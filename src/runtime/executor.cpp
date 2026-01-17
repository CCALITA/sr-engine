#include "runtime/executor.hpp"

#include <atomic>
#include <cassert>
#include <format>
#include <latch>
#include <memory>
#include <mutex>
#include <new>
#include <span>
#include <thread>
#include <utility>
#include <vector>

#include <exec/async_scope.hpp>
#include <exec/static_thread_pool.hpp>
#include <exec/task.hpp>
#include <stdexec/execution.hpp>

#include "runtime/frozen_env.hpp"

namespace sr::engine {
namespace {

auto reset_slots(const ExecPlan &plan, std::vector<ValueBox> &slots) -> void {
  slots.resize(plan.slots.size());
  for (std::size_t i = 0; i < plan.slots.size(); ++i) {
    slots[i].type_id = plan.slots[i].type_id;
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

#if defined(__cpp_lib_hardware_interference_size)
constexpr std::size_t kNodeStateAlignment =
    std::hardware_destructive_interference_size;
#else
constexpr std::size_t kNodeStateAlignment = 64;
#endif

// Per-node pending counter padded to reduce false sharing.
struct alignas(kNodeStateAlignment) NodeState {
  std::atomic<int> pending{0};

  NodeState() = default;
  NodeState(const NodeState &other) noexcept
      : pending(other.pending.load(std::memory_order_relaxed)) {}
  NodeState &operator=(const NodeState &other) noexcept {
    pending.store(other.pending.load(std::memory_order_relaxed),
                  std::memory_order_relaxed);
    return *this;
  }
  NodeState(NodeState &&other) noexcept
      : pending(other.pending.load(std::memory_order_relaxed)) {}
  NodeState &operator=(NodeState &&other) noexcept {
    pending.store(other.pending.load(std::memory_order_relaxed),
                  std::memory_order_relaxed);
    return *this;
  }
};

// Per-run state allocated on each Executor::run call.
struct DAGStates : std::enable_shared_from_this<DAGStates> {
  const ExecPlan *plan = nullptr;
  RequestContext *ctx = nullptr;
  trace::TraceContext *trace = nullptr;
  exec::static_thread_pool *pool = nullptr;
  const ExecutorConfig *config = nullptr;

  std::vector<ValueBox> slots;
  std::vector<NodeBindings> node_bindings;
  FrozenEnv frozen_env;
  std::vector<const ValueBox *> input_refs;
  std::vector<ValueBox *> output_ptrs;
  std::vector<trace::Tick> enqueue_ticks;
  std::vector<NodeState> node_states;

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
  exec::async_scope node_scope;

  auto prepare(const ExecPlan &plan_ref, RequestContext &ctx_ref,
               exec::static_thread_pool &pool_ref,
               const ExecutorConfig &config_ref) -> Expected<void>;
  auto schedule_initial_nodes() -> void;
  auto schedule_nodes(std::span<const int> node_indices) -> void;
  auto schedule_nodes(std::vector<int> node_indices) -> void;
  auto execute_node(int node_index, int inline_depth = 0) -> void;
  auto complete_node(int node_index, int inline_depth = 0) -> void;
  auto finish_node() -> void;
  auto record_error(std::string message) -> void;

  [[nodiscard]] auto env() const -> const FrozenEnv & { return frozen_env; }

  /// Check if a node should be executed inline on the current thread.
  auto should_inline(int node_index, int current_depth) const -> bool;
  /// Execute a node inline (tail-call style continuation).
  auto execute_node_inline(int node_index, int inline_depth, int parent_node = -1) -> void;

  template <class Range> auto schedule_nodes_impl(Range &&node_indices) -> void;
};

auto DAGStates::prepare(const ExecPlan &plan_ref, RequestContext &ctx_ref,
                        exec::static_thread_pool &pool_ref,
                        const ExecutorConfig &config_ref) -> Expected<void> {
  plan = &plan_ref;
  ctx = &ctx_ref;
  trace = &ctx_ref.trace;
  pool = &pool_ref;
  config = &config_ref;

  if (auto state = check_request_state(ctx_ref); !state) {
    return tl::unexpected(state.error());
  }

  frozen_env = prepare_env_from_context(ctx_ref);
  ctx_ref.frozen_env = &frozen_env;

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

  node_bindings.resize(node_count);
  input_refs.clear();
  output_ptrs.clear();
  input_refs.reserve(node_count * 2);
  output_ptrs.reserve(node_count * 2);
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
        const auto &req = plan_ref.env_requirements[static_cast<std::size_t>(binding.env_index)];
        const auto *value = frozen_env.find(std::string_view(req.key));
        if (!value) {
          return tl::unexpected(
              make_error(std::format("missing env value for key: {}", req.key)));
        }
        if (req.type_id != TypeId{} && value->type_id != req.type_id) {
          return tl::unexpected(
              make_error(std::format("env type mismatch for key: {}", req.key)));
        }
        input_refs.push_back(value);
        break;
      }
      }
    }

    for (int slot_index : node.outputs) {
      output_ptrs.push_back(&slots[static_cast<std::size_t>(slot_index)]);
    }
  }

  node_states.clear();
  node_states.resize(node_count);
  for (std::size_t node_index = 0; node_index < node_count; ++node_index) {
    node_states[node_index].pending.store(plan_ref.pending_counts[node_index],
                                          std::memory_order_relaxed);
  }
  pending_nodes.store(static_cast<int>(node_count), std::memory_order_release);
  aborted.store(false, std::memory_order_release);
  has_error.store(false, std::memory_order_release);
  return {};
}

template <class Range>
auto DAGStates::schedule_nodes_impl(Range &&node_indices) -> void {
  if (!plan || !pool) {
    return;
  }
  const std::size_t count = node_indices.size();
  if (count == 0) {
    return;
  }

  if constexpr (trace::kTraceEnabled) {
    if (trace && trace_flags != 0 &&
        trace::has_flag(trace_flags, trace::TraceFlag::QueueDelay) &&
        !enqueue_ticks.empty()) {
      trace::Tick enqueue_tick = trace->now();
      for (int node_index : node_indices) {
        assert(node_index >= 0);
        auto index = static_cast<std::size_t>(node_index);
        assert(index < enqueue_ticks.size());
        enqueue_ticks[index] = enqueue_tick;
      }
    }
  }

  auto self = shared_from_this();
  auto scheduler = pool->get_scheduler();
  for (int node_index : node_indices) {
    assert(node_index >= 0);
    assert(static_cast<std::size_t>(node_index) < plan->nodes.size());
    auto task =
        stdexec::schedule(scheduler) |
        stdexec::then([self, node_index]() { self->execute_node(node_index); });
    self->node_scope.spawn(std::move(task));
  }
}

auto DAGStates::schedule_initial_nodes() -> void {
  if (!plan) {
    return;
  }
  schedule_nodes(std::span<const int>(plan->initial_ready));
}

auto DAGStates::schedule_nodes(std::span<const int> node_indices) -> void {
  schedule_nodes_impl(node_indices);
}

auto DAGStates::schedule_nodes(std::vector<int> node_indices) -> void {
  schedule_nodes_impl(std::move(node_indices));
}

auto DAGStates::should_inline(int node_index, int current_depth) const -> bool {
  if (!config || !config->enable_inline_execution) {
    return false;
  }
  if (current_depth >= config->max_inline_depth) {
    return false;
  }
  
  const auto &node = plan->nodes[static_cast<std::size_t>(node_index)];
  
  // Check kernel trait NeverInline
  if (has_trait(node.kernel.traits, KernelTraits::NeverInline)) {
    return false;
  }
  
  // Check DSL-level opt-out
  if (!node.inline_eligible) {
    return false;
  }
  
  // Check kernel trait InlineHint (always inline if set)
  if (has_trait(node.kernel.traits, KernelTraits::InlineHint)) {
    return true;
  }
  
  // Use kernel's estimated cost if available
  std::uint64_t cost_ns = 0;
  if (node.kernel.estimated_cost_us > 0) {
    cost_ns = static_cast<std::uint64_t>(node.kernel.estimated_cost_us) * 1000;
  } else if (node.estimated_cost > 0) {
    cost_ns = static_cast<std::uint64_t>(node.estimated_cost) * 1000;
  }
  
  // If we have a cost estimate, check against threshold
  if (cost_ns > 0) {
    return cost_ns < config->inline_threshold_ns;
  }
  
  // No cost info available - default to not inlining
  return false;
}

auto DAGStates::execute_node_inline(int node_index, int inline_depth, int parent_node) -> void {
  // Emit inline trace event
  if constexpr (trace::kTraceEnabled) {
    if (trace && trace_flags != 0 &&
        trace::has_flag(trace_flags, trace::TraceFlag::InlineDetail)) {
      const auto &node = plan->nodes[static_cast<std::size_t>(node_index)];
      trace::emit(trace->sink, trace::NodeInlined{
        trace_id, trace->new_span(), node.id, node_index,
        parent_node, inline_depth
      });
    }
  }
  
  execute_node(node_index, inline_depth);
}

auto DAGStates::execute_node(int node_index, int inline_depth) -> void {
  auto &ctx_ref = *ctx;
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
      // Only emit QueueDelay for non-inlined nodes (inline_depth == 0)
      if (inline_depth == 0 &&
          trace::has_flag(trace_flags, trace::TraceFlag::QueueDelay) &&
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
    complete_node(node_index, inline_depth);
    return;
  }

  if (ctx_ref.is_cancelled()) {
    record_error("request cancelled");
    finish_trace(trace::SpanStatus::Cancelled, {});
    complete_node(node_index, inline_depth);
    return;
  }

  if (ctx_ref.deadline_exceeded()) {
    record_error("deadline exceeded");
    finish_trace(trace::SpanStatus::Deadline, {});
    complete_node(node_index, inline_depth);
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
    complete_node(node_index, inline_depth);
    return;
  }

  finish_trace(trace::SpanStatus::Ok, {});
  complete_node(node_index, inline_depth);
}

auto DAGStates::complete_node(int node_index, int inline_depth) -> void {
  const auto &dependents =
      plan->dependents[static_cast<std::size_t>(node_index)];
  
  // Collect ready nodes
  int ready_storage[8];
  int *ready_ptr = ready_storage;
  int ready_count = 0;
  if (dependents.size() > 8) {
    ready_ptr = new int[dependents.size()];
  }
  for (int dependent : dependents) {
    if (node_states[static_cast<std::size_t>(dependent)].pending.fetch_sub(
            1, std::memory_order_acq_rel) == 1) {
      ready_ptr[ready_count++] = dependent;
    }
  }
  
  if (ready_count == 0) {
    if (dependents.size() > 8) {
      delete[] ready_ptr;
    }
    finish_node();
    return;
  }
  
  // Inline execution optimization: if single ready node and cheap, inline it
  int inline_candidate = -1;
  if (ready_count == 1 && should_inline(ready_ptr[0], inline_depth)) {
    inline_candidate = ready_ptr[0];
    ready_count = 0;  // Don't schedule it
  } else if (ready_count > 1 && config && config->enable_inline_execution) {
    // Multiple ready nodes: maybe inline the first one
    if (should_inline(ready_ptr[0], inline_depth)) {
      inline_candidate = ready_ptr[0];
      // Shift array
      for (int i = 0; i < ready_count - 1; ++i) {
        ready_ptr[i] = ready_ptr[i + 1];
      }
      --ready_count;
    }
  }
  
  // Schedule remaining nodes via pool
  if (ready_count > 0) {
    schedule_nodes(std::span<int>(ready_ptr, static_cast<std::size_t>(ready_count)));
  }
  
  if (dependents.size() > 8) {
    delete[] ready_ptr;
  }
  
  finish_node();
  
  // Execute inline candidate last (after finish_node for correctness)
  if (inline_candidate >= 0) {
    execute_node_inline(inline_candidate, inline_depth + 1, node_index);
  }
}

auto DAGStates::finish_node() -> void {
  if (pending_nodes.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    done_latch.count_down();
  }
}

auto DAGStates::record_error(std::string message) -> void {
  bool expected = false;
  if (has_error.compare_exchange_strong(expected, true,
                                        std::memory_order_acq_rel)) {
    std::lock_guard<std::mutex> lock(error_mutex);
    error = make_error(std::move(message));
  }
  aborted.store(true, std::memory_order_release);
}

} // namespace (anonymous)

Executor::Executor(exec::static_thread_pool *pool) : pool_(pool), config_() {}

Executor::Executor(exec::static_thread_pool *pool, ExecutorConfig config)
    : pool_(pool), config_(std::move(config)) {}

Executor::~Executor() = default;

auto Executor::run_async(const ExecPlan &plan, RequestContext &ctx) const
    -> exec::task<Expected<ExecResult>> {
  if (!pool_) {
    co_return tl::unexpected(make_error("executor missing thread pool"));
  }
  auto runtime = std::make_shared<DAGStates>();
  if (auto prepared = runtime->prepare(plan, ctx, *pool_, config_); !prepared) {
    co_return tl::unexpected(prepared.error());
  }

  runtime->schedule_initial_nodes();
  co_await runtime->node_scope.on_empty();

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
    ctx.frozen_env = nullptr;
    trace::SpanStatus status = trace::SpanStatus::Error;
    if (ctx.is_cancelled()) {
      status = trace::SpanStatus::Cancelled;
    } else if (ctx.deadline_exceeded()) {
      status = trace::SpanStatus::Deadline;
    }
    emit_run_end(status);
    std::lock_guard<std::mutex> lock(runtime->error_mutex);
    co_return tl::unexpected(runtime->error);
  }

  ctx.frozen_env = nullptr;
  emit_run_end(trace::SpanStatus::Ok);
  co_return collect_outputs(plan, runtime->slots);
}

auto Executor::run(const ExecPlan &plan, RequestContext &ctx) const
    -> Expected<ExecResult> {
  auto task = run_async(plan, ctx);
  auto result = stdexec::sync_wait(std::move(task));
  if (!result) {
    return tl::unexpected(make_error("execution failed or cancelled"));
  }
  auto &maybe = std::get<0>(*result);
  if (!maybe) {
    return tl::unexpected(maybe.error());
  }
  return std::move(*maybe);
}

} // namespace sr::engine
