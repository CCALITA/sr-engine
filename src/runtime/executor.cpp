#include "runtime/executor.hpp"

#include "engine/graph_store.hpp"

#include <atomic>
#include <cassert>
#include <exception>
#include <format>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <exec/any_sender_of.hpp>
#include <exec/static_thread_pool.hpp>
#include <stdexec/execution.hpp>

namespace sr::engine {
namespace {

namespace ex = stdexec;

template <class... Ts>
using AnySender = exec::any_receiver_ref<ex::completion_signatures<Ts...>>::template any_sender<>;

using NodeSender = AnySender<ex::set_value_t(), ex::set_error_t(EngineError),
                             ex::set_error_t(std::exception_ptr), ex::set_stopped_t()>;
using NodeSenderFactory = std::function<NodeSender()>;

struct SenderRunState;

struct get_run_state_t {
  using is_forwarding_query = std::true_type;

  template <class Env>
  auto operator()(const Env &env) const noexcept
      -> decltype(tag_invoke(*this, env)) {
    return tag_invoke(*this, env);
  }
};

inline constexpr get_run_state_t get_run_state{};

struct RunEnv {
  SenderRunState *state = nullptr;

  friend auto tag_invoke(get_run_state_t, const RunEnv &env) noexcept
      -> SenderRunState * {
    return env.state;
  }
};

auto reset_slots(const ExecPlan &plan, std::vector<ValueBox> &slots) -> void {
  slots.resize(plan.slots.size());
  for (std::size_t i = 0; i < plan.slots.size(); ++i) {
    slots[i].type = plan.slots[i].type;
    slots[i].storage.reset();
  }
}

auto collect_outputs(const ExecPlan &plan, const std::vector<ValueBox> &slots)
    -> Expected<ExecResult> {
  ExecResult result;
  for (const auto &output : plan.outputs) {
    const auto &slot = slots[static_cast<std::size_t>(output.slot_index)];
    if (!slot.has_value()) {
      return tl::unexpected(make_error(
          std::format("output slot not populated: {}", output.name)));
    }
    result.outputs.emplace(output.name, slot);
  }
  return result;
}

auto prepare_env_boxes(const ExecPlan &plan, const RequestContext &ctx,
                       std::vector<ValueBox> &boxes) -> Expected<void> {
  boxes.clear();
  boxes.resize(plan.env_requirements.size());
  for (std::size_t i = 0; i < plan.env_requirements.size(); ++i) {
    const auto &req = plan.env_requirements[i];
    auto it = ctx.env.find(req.key);
    if (it == ctx.env.end()) {
      return tl::unexpected(
          make_error(std::format("missing env value: {}", req.key)));
    }
    if (req.type && it->second.type != req.type) {
      return tl::unexpected(
          make_error(std::format("env type mismatch: {}", req.key)));
    }
    boxes[i] = it->second;
  }
  return {};
}

auto check_request_state(const RequestContext &ctx) -> Expected<void> {
  if (ctx.is_cancelled()) {
    return tl::unexpected(make_error("request cancelled"));
  }
  if (ctx.deadline_exceeded()) {
    return tl::unexpected(make_error("deadline exceeded"));
  }
  return {};
}

struct NodeBindings {
  std::size_t input_offset = 0;
  std::size_t input_count = 0;
  std::size_t output_offset = 0;
  std::size_t output_count = 0;
};

/// Per-run state allocated on each Executor::run call.
struct SenderRunState {
  const ExecPlan *plan = nullptr;
  RequestContext *ctx = nullptr;
  trace::TraceContext *trace = nullptr;
  exec::static_thread_pool *pool = nullptr;
  RequestContextView ctx_view;

  std::vector<ValueBox> slots;
  std::vector<NodeBindings> node_bindings;
  std::vector<ValueBox> env_boxes;
  std::vector<const ValueBox *> input_refs;
  std::vector<ValueBox *> output_ptrs;
  std::vector<trace::Tick> enqueue_ticks;

  std::atomic<bool> aborted{false};
  std::atomic<bool> has_error{false};
  std::mutex error_mutex;
  EngineError error;

  trace::TraceFlags trace_flags = 0;
  trace::TraceId trace_id = 0;
  trace::SpanId run_span = 0;
  trace::Tick run_start = 0;

  /// Prepare run state from a compiled plan and request context.
  auto prepare(const ExecPlan &plan_ref, RequestContext &ctx_ref,
               exec::static_thread_pool &pool_ref) -> Expected<void>;
  /// Record node enqueue timestamps for queue delay tracing.
  auto note_enqueue(int node_index) -> void;
  /// Execute a single node on a worker thread.
  template <typename StopToken>
  auto execute_node(int node_index, StopToken stop_token) -> Expected<void>;
  /// Record the first error and mark the run as aborted.
  auto record_error(std::string message) -> void;
};

auto SenderRunState::prepare(const ExecPlan &plan_ref, RequestContext &ctx_ref,
                             exec::static_thread_pool &pool_ref)
    -> Expected<void> {
  plan = &plan_ref;
  ctx = &ctx_ref;
  trace = &ctx_ref.trace;
  pool = &pool_ref;

  if (auto state = check_request_state(ctx_ref); !state) {
    return tl::unexpected(state.error());
  }
  if (auto env_result = prepare_env_boxes(plan_ref, ctx_ref, env_boxes);
      !env_result) {
    return tl::unexpected(env_result.error());
  }
  ctx_view = RequestContextView(ctx_ref);

  reset_slots(plan_ref, slots);

  const std::size_t node_count = plan_ref.nodes.size();
  if (node_count == 0) {
    return tl::unexpected(make_error("graph has no nodes"));
  }

  trace_flags = 0;
  trace_id = 0;
  run_span = 0;
  run_start = 0;
  enqueue_ticks.clear();
  if constexpr (trace::kTraceEnabled) {
    if (trace && trace->sink.enabled()) {
      trace->apply_sampler(plan_ref.name);
      trace_flags = trace->flags;
      if (trace_flags != 0) {
        trace_id = trace->trace_id;
        if (trace_id == 0) {
          trace_id = trace->new_span();
          trace->trace_id = trace_id;
        }
        run_span = trace->new_span();
        if (trace::has_flag(trace_flags, trace::TraceFlag::RunSpan)) {
          run_start = trace->now();
          trace::emit(trace->sink, trace::RunStart{trace_id, run_span,
                                                   plan_ref.name, run_start});
        }
        if (trace::has_flag(trace_flags, trace::TraceFlag::QueueDelay)) {
          enqueue_ticks.assign(node_count, 0);
        }
      }
    }
  }

  std::size_t total_inputs = 0;
  std::size_t total_outputs = 0;
  for (const auto &node : plan_ref.nodes) {
    total_inputs += node.inputs.size();
    total_outputs += node.outputs.size();
  }

  node_bindings.resize(node_count);
  input_refs.clear();
  output_ptrs.clear();
  input_refs.reserve(total_inputs);
  output_ptrs.reserve(total_outputs);
  for (std::size_t node_index = 0; node_index < node_count; ++node_index) {
    const auto &node = plan_ref.nodes[node_index];
    auto &runtime = node_bindings[node_index];
    runtime.input_offset = input_refs.size();
    runtime.input_count = node.inputs.size();
    runtime.output_offset = output_ptrs.size();
    runtime.output_count = node.outputs.size();

    for (const auto &binding : node.inputs) {
      switch (binding.kind) {
      case InputBindingKind::Slot: {
        input_refs.push_back(
            &slots[static_cast<std::size_t>(binding.slot_index)]);
        break;
      }
      case InputBindingKind::Const: {
        input_refs.push_back(
            &plan_ref
                 .const_slots[static_cast<std::size_t>(binding.const_index)]);
        break;
      }
      case InputBindingKind::Env: {
        std::size_t env_index = static_cast<std::size_t>(binding.env_index);
        assert(env_index < env_boxes.size());
        input_refs.push_back(&env_boxes[env_index]);
        break;
      }
      }
    }

    for (int slot_index : node.outputs) {
      output_ptrs.push_back(&slots[static_cast<std::size_t>(slot_index)]);
    }
  }

  aborted.store(false, std::memory_order_release);
  has_error.store(false, std::memory_order_release);
  error = EngineError{};
  return {};
}

auto SenderRunState::note_enqueue(int node_index) -> void {
  if constexpr (trace::kTraceEnabled) {
    if (!trace || trace_flags == 0) {
      return;
    }
    if (!trace::has_flag(trace_flags, trace::TraceFlag::QueueDelay)) {
      return;
    }
    if (node_index < 0 ||
        static_cast<std::size_t>(node_index) >= enqueue_ticks.size()) {
      return;
    }
    enqueue_ticks[static_cast<std::size_t>(node_index)] = trace->now();
  }
}

template <typename StopToken>
auto SenderRunState::execute_node(int node_index, StopToken stop_token)
    -> Expected<void> {
  auto &ctx_ref = ctx_view;
  const auto &node = plan->nodes[static_cast<std::size_t>(node_index)];
  const auto &runtime = node_bindings[static_cast<std::size_t>(node_index)];

  trace::SpanId span_id = 0;
  trace::Tick start_ts = 0;
  if constexpr (trace::kTraceEnabled) {
    if (trace && trace_flags != 0 && trace->sink.enabled()) {
      if (trace::has_flag(trace_flags, trace::TraceFlag::NodeSpan)) {
        span_id = trace->new_span();
        start_ts = trace->now();
        trace::emit(trace->sink,
                    trace::NodeStart{trace_id, span_id, run_span, node.id,
                                     node_index, start_ts});
      }
      if (trace::has_flag(trace_flags, trace::TraceFlag::QueueDelay) &&
          static_cast<std::size_t>(node_index) < enqueue_ticks.size()) {
        trace::Tick delay_start = start_ts ? start_ts : trace->now();
        trace::emit(trace->sink,
                    trace::QueueDelay{
                        trace_id, span_id, node.id, node_index,
                        enqueue_ticks[static_cast<std::size_t>(node_index)],
                        delay_start});
      }
    }
  }

  auto finish_trace = [this, &node, node_index, span_id, start_ts](
                          trace::SpanStatus status, std::string_view message) {
    if constexpr (trace::kTraceEnabled) {
      if (trace && trace_flags != 0 && trace->sink.enabled()) {
        if (trace::has_flag(trace_flags, trace::TraceFlag::NodeSpan)) {
          trace::Tick end_ts = trace->now();
          trace::Tick duration = 0;
          if (start_ts != 0 && end_ts >= start_ts) {
            duration = end_ts - start_ts;
          }
          trace::emit(trace->sink,
                      trace::NodeEnd{trace_id, span_id, node.id, node_index,
                                     end_ts, duration, status});
        }
        if (!message.empty() &&
            trace::has_flag(trace_flags, trace::TraceFlag::ErrorDetail)) {
          trace::emit(trace->sink, trace::NodeError{trace_id, span_id, node.id,
                                                    node_index, message});
        }
      }
    }
  };

  if (aborted.load(std::memory_order_acquire) || stop_token.stop_requested()) {
    finish_trace(trace::SpanStatus::Skipped, {});
    return {};
  }

  if (ctx_ref.is_cancelled()) {
    record_error("request cancelled");
    finish_trace(trace::SpanStatus::Cancelled, {});
    return tl::unexpected(make_error("request cancelled"));
  }

  if (ctx_ref.deadline_exceeded()) {
    record_error("deadline exceeded");
    finish_trace(trace::SpanStatus::Deadline, {});
    return tl::unexpected(make_error("deadline exceeded"));
  }

  auto input_view = InputValues(std::span<const ValueBox *const>(
      input_refs.data() + runtime.input_offset, runtime.input_count));
  auto output_view = OutputValues(std::span<ValueBox *>(
      output_ptrs.data() + runtime.output_offset, runtime.output_count));

  Expected<void> result{};
  try {
    result = node.kernel.compute(node.kernel.instance.get(), ctx_ref, input_view,
                                 output_view);
  } catch (const std::exception &ex) {
    result = tl::unexpected(
        make_error(std::format("kernel exception: {}", ex.what())));
  } catch (...) {
    result = tl::unexpected(make_error("kernel exception: unknown"));
  }

  if (!result) {
    std::string message = result.error().message;
    finish_trace(trace::SpanStatus::Error, message);
    record_error(message);
    return tl::unexpected(make_error(std::move(message)));
  }

  finish_trace(trace::SpanStatus::Ok, {});
  return {};
}

auto SenderRunState::record_error(std::string message) -> void {
  bool expected = false;
  if (has_error.compare_exchange_strong(expected, true,
                                        std::memory_order_acq_rel)) {
    std::lock_guard<std::mutex> lock(error_mutex);
    error = make_error(std::move(message));
  }
  aborted.store(true, std::memory_order_release);
}

auto build_dependency_sender(const ExecPlan &plan, int node_index,
                             const std::vector<NodeSenderFactory> &factories)
    -> Expected<NodeSender> {
  if (node_index < 0 ||
      static_cast<std::size_t>(node_index) >= plan.nodes.size()) {
    return tl::unexpected(make_error("node index out of range"));
  }
  if (plan.dependencies.size() != plan.nodes.size()) {
    return tl::unexpected(make_error("plan dependencies not built"));
  }
  NodeSender deps = ex::just();
  const auto &node_deps =
      plan.dependencies[static_cast<std::size_t>(node_index)];
  for (int producer : node_deps) {
    if (producer < 0 ||
        static_cast<std::size_t>(producer) >= plan.nodes.size()) {
      return tl::unexpected(make_error("dependency index out of range"));
    }
    if (!factories[static_cast<std::size_t>(producer)]) {
      return tl::unexpected(make_error("dependency sender not built"));
    }
    deps = NodeSender(
        ex::when_all(std::move(deps), factories[producer]()));
  }
  return deps;
}

struct PlanSenderTemplate {
  explicit PlanSenderTemplate(const ExecPlan &plan_ref) : plan(&plan_ref) {}

  auto make_sender() const -> Expected<NodeSender> {
    const std::size_t node_count = plan->nodes.size();
    if (plan->topo_order.size() != node_count) {
      return tl::unexpected(make_error("topo order mismatch"));
    }
    if (plan->dependencies.size() != node_count) {
      return tl::unexpected(make_error("plan dependencies not built"));
    }
    if (plan->dependents.size() != node_count) {
      return tl::unexpected(make_error("plan dependents not built"));
    }
    if (plan->sinks.empty()) {
      return tl::unexpected(make_error("plan sinks not built"));
    }

    std::vector<NodeSenderFactory> factories(node_count);
    std::vector<std::optional<NodeSender>> single_senders(node_count);

    for (int node_index : plan->topo_order) {
      auto deps_result =
          build_dependency_sender(*plan, node_index, factories);
      if (!deps_result) {
        return tl::unexpected(deps_result.error());
      }

      auto node_sender =
          ex::when_all(std::move(*deps_result), ex::read_env(get_run_state),
                       ex::get_stop_token()) |
          ex::let_value([node_index](SenderRunState *state, auto stop_token) {
            state->note_enqueue(node_index);
            auto work =
                ex::schedule(state->pool->get_scheduler()) |
                ex::then([state, node_index, stop_token]() {
                  return state->execute_node(node_index, stop_token);
                });
            return ex::let_value(std::move(work),
                                 [](Expected<void> result) -> NodeSender {
                                   if (result) {
                                     return NodeSender(ex::just());
                                   }
                                   return NodeSender(
                                       ex::just_error(result.error()));
                                 });
          });

      const std::size_t node_slot = static_cast<std::size_t>(node_index);
      const std::size_t use_count = plan->nodes[node_slot].sender_use_count;
      if (use_count == 0) {
        return tl::unexpected(make_error("node sender use count missing"));
      }
      if (use_count <= 1) {
        single_senders[node_slot] = std::move(node_sender);
        factories[node_slot] =
            [&single_senders, node_slot]() mutable -> NodeSender {
          auto &slot = single_senders[node_slot];
          if (!slot) {
            return NodeSender(
                ex::just_error(make_error("sender already consumed")));
          }
          NodeSender out = std::move(*slot);
          slot.reset();
          return out;
        };
      } else {
        auto shared = ex::split(std::move(node_sender));
        factories[node_slot] =
            [shared]() mutable -> NodeSender { return NodeSender(shared); };
      }
    }

    NodeSender all_nodes = ex::just();
    for (int node_index : plan->sinks) {
      const std::size_t node_slot = static_cast<std::size_t>(node_index);
      if (!factories[node_slot]) {
        return tl::unexpected(make_error("node sender not built"));
      }
      all_nodes = NodeSender(
          ex::when_all(std::move(all_nodes), factories[node_slot]()));
    }

    return all_nodes;
  }

  const ExecPlan *plan = nullptr;
};

auto build_sender_template(const ExecPlan &plan)
    -> Expected<std::shared_ptr<const PlanSenderTemplate>> {
  if (plan.nodes.empty()) {
    return tl::unexpected(make_error("plan has no nodes"));
  }
  if (plan.topo_order.size() != plan.nodes.size()) {
    return tl::unexpected(make_error("topo order mismatch"));
  }
  if (plan.dependencies.size() != plan.nodes.size()) {
    return tl::unexpected(make_error("plan dependencies not built"));
  }
  if (plan.dependents.size() != plan.nodes.size()) {
    return tl::unexpected(make_error("plan dependents not built"));
  }
  if (plan.sinks.empty()) {
    return tl::unexpected(make_error("plan sinks not built"));
  }
  for (const auto &node : plan.nodes) {
    if (node.sender_use_count == 0) {
      return tl::unexpected(make_error("node sender use count missing"));
    }
  }
  return std::make_shared<PlanSenderTemplate>(plan);
}

} // namespace

struct Executor::Pools {
  explicit Pools(int threads) : pool(static_cast<std::size_t>(threads)) {}
  exec::static_thread_pool pool;
};

Executor::Executor(ExecutorConfig config) {
  int compute_threads = config.compute_threads;
  if (compute_threads <= 0) {
    compute_threads = static_cast<int>(std::thread::hardware_concurrency());
    if (compute_threads <= 0) {
      compute_threads = 2;
    }
  }
  pools_ = std::make_shared<Pools>(compute_threads);
}

Executor::~Executor() = default;

auto Executor::run(const std::shared_ptr<const PlanSnapshot> &snapshot,
                   RequestContext &ctx) const -> Expected<ExecResult> {
  if (!snapshot) {
    return tl::unexpected(make_error("snapshot is null"));
  }
  const ExecPlan &plan = snapshot->plan;
  auto state = std::make_shared<SenderRunState>();
  if (auto prepared = state->prepare(plan, ctx, pools_->pool); !prepared) {
    return tl::unexpected(prepared.error());
  }

  auto emit_run_end = [state](trace::SpanStatus status) {
    if constexpr (trace::kTraceEnabled) {
      auto *trace_ctx = state->trace;
      if (!trace_ctx || state->trace_flags == 0) {
        return;
      }
      if (!trace::has_flag(state->trace_flags, trace::TraceFlag::RunSpan) ||
          state->run_span == 0) {
        return;
      }
      trace::Tick end_ts = trace_ctx->now();
      trace::Tick duration = 0;
      if (state->run_start != 0 && end_ts >= state->run_start) {
        duration = end_ts - state->run_start;
      }
      trace::emit(trace_ctx->sink,
                  trace::RunEnd{state->trace_id, state->run_span, end_ts,
                                duration, status});
    }
  };

  auto sender_template =
      snapshot->sender_template.load(std::memory_order_acquire);
  if (!sender_template) {
    auto built = build_sender_template(plan);
    if (!built) {
      emit_run_end(trace::SpanStatus::Error);
      return tl::unexpected(built.error());
    }
    std::shared_ptr<const PlanSenderTemplate> expected;
    if (!snapshot->sender_template.compare_exchange_strong(
            expected, *built, std::memory_order_acq_rel)) {
      sender_template =
          snapshot->sender_template.load(std::memory_order_acquire);
    } else {
      sender_template = std::move(*built);
    }
  }

  auto root_sender = sender_template->make_sender();
  if (!root_sender) {
    emit_run_end(trace::SpanStatus::Error);
    return tl::unexpected(root_sender.error());
  }

  auto run_sender =
      ex::write_env(std::move(*root_sender), RunEnv{state.get()});

  try {
    auto result = ex::sync_wait(std::move(run_sender));
    if (!result) {
      trace::SpanStatus status = trace::SpanStatus::Cancelled;
      if (ctx.deadline_exceeded()) {
        status = trace::SpanStatus::Deadline;
      }
      emit_run_end(status);
      return tl::unexpected(make_error("execution stopped"));
    }
  } catch (const EngineError &err) {
    state->record_error(err.message);
  } catch (const std::exception &ex) {
    state->record_error(
        std::format("executor exception: {}", ex.what()));
  } catch (...) {
    state->record_error("executor exception: unknown");
  }

  if (state->has_error.load(std::memory_order_acquire)) {
    trace::SpanStatus status = trace::SpanStatus::Error;
    if (ctx.is_cancelled()) {
      status = trace::SpanStatus::Cancelled;
    } else if (ctx.deadline_exceeded()) {
      status = trace::SpanStatus::Deadline;
    }
    emit_run_end(status);
    std::lock_guard<std::mutex> lock(state->error_mutex);
    return tl::unexpected(state->error);
  }

  emit_run_end(trace::SpanStatus::Ok);
  return collect_outputs(plan, state->slots);
}

} // namespace sr::engine
