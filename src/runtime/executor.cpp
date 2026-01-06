#include "runtime/executor.hpp"

#include <atomic>
#include <cassert>
#include <format>
#include <latch>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include <exec/static_thread_pool.hpp>
#include <stdexec/execution.hpp>

namespace sr::engine {
namespace {

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

// Per-run state allocated on each Executor::run call.
struct RuntimeContext : std::enable_shared_from_this<RuntimeContext> {
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
  std::vector<int> pending_counts;
  std::vector<int> scheduled;

  std::atomic<int> pending_nodes{0};
  std::atomic<bool> aborted{false};
  std::atomic<bool> has_error{false};
  std::mutex error_mutex;
  EngineError error;
  trace::TraceFlags trace_flags = 0;
  trace::TraceId trace_id = 0;
  trace::SpanId run_span = 0;
  trace::Tick run_start = 0;
  std::latch done_latch{1};

  auto prepare(const ExecPlan &plan_ref, RequestContext &ctx_ref,
               exec::static_thread_pool &pool_ref) -> Expected<void>;
  auto schedule_initial_nodes() -> void;
  auto schedule_node(int node_index) -> void;
  auto execute_node(int node_index) -> void;
  auto complete_node(int node_index) -> void;
  auto finish_node() -> void;
  auto record_error(std::string message) -> void;
};

auto RuntimeContext::prepare(const ExecPlan &plan_ref, RequestContext &ctx_ref,
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
        input_refs.push_back(&slots[static_cast<std::size_t>(binding.slot_index)]);
        break;
      }
      case InputBindingKind::Const: {
        input_refs.push_back(
            &plan_ref.const_slots[static_cast<std::size_t>(binding.const_index)]);
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

  pending_counts = plan_ref.pending_counts;
  scheduled.assign(node_count, 0);
  pending_nodes.store(static_cast<int>(node_count), std::memory_order_release);
  aborted.store(false, std::memory_order_release);
  has_error.store(false, std::memory_order_release);
  error = EngineError{};
  return {};
}

auto RuntimeContext::schedule_initial_nodes() -> void {
  const std::size_t node_count = plan->nodes.size();
  for (std::size_t node_index = 0; node_index < node_count; ++node_index) {
    if (std::atomic_ref<int>(pending_counts[node_index])
            .load(std::memory_order_relaxed) == 0) {
      schedule_node(static_cast<int>(node_index));
    }
  }
}

auto RuntimeContext::schedule_node(int node_index) -> void {
  if (!plan || !pool) {
    return;
  }
  if (node_index < 0 ||
      static_cast<std::size_t>(node_index) >= scheduled.size()) {
    return;
  }
  int expected = 0;
  if (!std::atomic_ref<int>(scheduled[static_cast<std::size_t>(node_index)])
           .compare_exchange_strong(expected, 1, std::memory_order_acq_rel)) {
    return;
  }
  if constexpr (trace::kTraceEnabled) {
    if (trace && trace_flags != 0 &&
        trace::has_flag(trace_flags, trace::TraceFlag::QueueDelay) &&
        node_index >= 0 &&
        static_cast<std::size_t>(node_index) < enqueue_ticks.size()) {
      enqueue_ticks[static_cast<std::size_t>(node_index)] = trace->now();
    }
  }

  auto scheduler = pool->get_scheduler();

  auto self = shared_from_this();
  auto task =
      stdexec::schedule(scheduler) |
      stdexec::then([self, node_index]() { self->execute_node(node_index); });
  stdexec::start_detached(std::move(task));
}

auto RuntimeContext::execute_node(int node_index) -> void {
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

  if (aborted.load(std::memory_order_acquire)) {
    finish_trace(trace::SpanStatus::Skipped, {});
    complete_node(node_index);
    return;
  }

  if (ctx_ref.is_cancelled()) {
    record_error("request cancelled");
    finish_trace(trace::SpanStatus::Cancelled, {});
    complete_node(node_index);
    return;
  }

  if (ctx_ref.deadline_exceeded()) {
    record_error("deadline exceeded");
    finish_trace(trace::SpanStatus::Deadline, {});
    complete_node(node_index);
    return;
  }

  auto input_view = InputValues(std::span<const ValueBox *const>(
      input_refs.data() + runtime.input_offset, runtime.input_count));
  auto output_view = OutputValues(std::span<ValueBox *>(
      output_ptrs.data() + runtime.output_offset, runtime.output_count));
  auto result = node.kernel.compute(node.kernel.instance.get(), ctx_ref,
                                    input_view, output_view);
  if (!result) {
    std::string message = result.error().message;
    finish_trace(trace::SpanStatus::Error, message);
    record_error(std::move(message));
    complete_node(node_index);
    return;
  }

  finish_trace(trace::SpanStatus::Ok, {});
  complete_node(node_index);
}

auto RuntimeContext::complete_node(int node_index) -> void {
  for (int dependent : plan->dependents[static_cast<std::size_t>(node_index)]) {
    if (std::atomic_ref<int>(
            pending_counts[static_cast<std::size_t>(dependent)])
            .fetch_sub(1, std::memory_order_acq_rel) == 1) {
      schedule_node(dependent);
    }
  }
  finish_node();
}

auto RuntimeContext::finish_node() -> void {
  if (pending_nodes.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    done_latch.count_down();
  }
}

auto RuntimeContext::record_error(std::string message) -> void {
  bool expected = false;
  if (has_error.compare_exchange_strong(expected, true,
                                        std::memory_order_acq_rel)) {
    std::lock_guard<std::mutex> lock(error_mutex);
    error = make_error(std::move(message));
  }
  aborted.store(true, std::memory_order_release);
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

auto Executor::run(const ExecPlan &plan, RequestContext &ctx) const
    -> Expected<ExecResult> {
  auto runtime = std::make_shared<RuntimeContext>();
  if (auto prepared = runtime->prepare(plan, ctx, pools_->pool); !prepared) {
    return tl::unexpected(prepared.error());
  }

  runtime->schedule_initial_nodes();
  runtime->done_latch.wait();

  auto emit_run_end = [runtime](trace::SpanStatus status) {
    if constexpr (trace::kTraceEnabled) {
      auto *trace_ctx = runtime->trace;
      if (!trace_ctx || runtime->trace_flags == 0) {
        return;
      }
      if (!trace::has_flag(runtime->trace_flags, trace::TraceFlag::RunSpan) ||
          runtime->run_span == 0) {
        return;
      }
      trace::Tick end_ts = trace_ctx->now();
      trace::Tick duration = 0;
      if (runtime->run_start != 0 && end_ts >= runtime->run_start) {
        duration = end_ts - runtime->run_start;
      }
      trace::emit(trace_ctx->sink,
                  trace::RunEnd{runtime->trace_id, runtime->run_span, end_ts,
                                duration, status});
    }
  };

  if (runtime->has_error.load(std::memory_order_acquire)) {
    trace::SpanStatus status = trace::SpanStatus::Error;
    if (ctx.is_cancelled()) {
      status = trace::SpanStatus::Cancelled;
    } else if (ctx.deadline_exceeded()) {
      status = trace::SpanStatus::Deadline;
    }
    emit_run_end(status);
    std::lock_guard<std::mutex> lock(runtime->error_mutex);
    return tl::unexpected(runtime->error);
  }

  emit_run_end(trace::SpanStatus::Ok);
  return collect_outputs(plan, runtime->slots);
}

} // namespace sr::engine
