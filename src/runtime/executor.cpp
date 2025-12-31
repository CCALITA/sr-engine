#include "runtime/executor.hpp"

#include <atomic>
#include <condition_variable>
#include <deque>
#include <exception>
#include <format>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include <exec/static_thread_pool.hpp>
#include <stdexec/execution.hpp>

namespace sr::engine {
namespace {

auto reset_slots(const ExecPlan& plan, std::vector<ValueSlot>& slots) -> void {
  slots.resize(plan.slots.size());
  for (std::size_t i = 0; i < plan.slots.size(); ++i) {
    slots[i].type = plan.slots[i].type;
    slots[i].storage.reset();
  }
}

auto collect_outputs(const ExecPlan& plan, const std::vector<ValueSlot>& slots) -> Expected<ExecResult> {
  ExecResult result;
  for (const auto& [name, slot_index] : plan.output_slots) {
    const auto& slot = slots[static_cast<std::size_t>(slot_index)];
    if (!slot.has_value()) {
      return tl::unexpected(make_error(std::format("output slot not populated: {}", name)));
    }
    result.outputs.emplace(name, slot);
  }
  return result;
}

auto prepare_env_values(const ExecPlan& plan, const RequestContext& ctx, std::vector<const ValueSlot*>& values)
  -> Expected<void> {
  values.clear();
  values.reserve(plan.env_requirements.size());
  for (const auto& req : plan.env_requirements) {
    auto it = ctx.env.find(req.key);
    if (it == ctx.env.end()) {
      return tl::unexpected(make_error(std::format("missing env value: {}", req.key)));
    }
    if (req.type && it->second.type != req.type) {
      return tl::unexpected(make_error(std::format("env type mismatch: {}", req.key)));
    }
    values.push_back(&it->second);
  }
  return {};
}

auto check_request_state(const RequestContext& ctx) -> Expected<void> {
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

struct Dispatcher;

struct ExecutionState {
  const ExecPlan* plan = nullptr;
  RequestContext* ctx = nullptr;
  Dispatcher* dispatcher = nullptr;
  trace::TraceContext* trace = nullptr;

  std::vector<ValueSlot> slots;
  std::vector<NodeBindings> runtime_nodes;
  std::vector<const ValueSlot*> env_values;
  std::vector<const ValueSlot*> input_ptrs;
  std::vector<ValueSlot*> output_ptrs;
  std::vector<ValueSlot> missing_slots;
  std::vector<trace::Tick> enqueue_ticks;
  std::vector<int> pending;
  std::vector<int> scheduled;

  std::atomic<int> remaining{0};
  std::atomic<bool> aborted{false};
  std::atomic<bool> has_error{false};
  std::mutex error_mutex;
  EngineError error;
  trace::TraceFlags trace_flags = 0;
  trace::TraceId trace_id = 0;
  trace::SpanId run_span = 0;
  trace::Tick run_start = 0;

  auto prepare(const ExecPlan& plan_ref, RequestContext& ctx_ref, Dispatcher& dispatcher_ref) -> Expected<void>;
  auto schedule_initial_nodes() -> void;
  auto schedule_node(int node_index) -> void;
  auto execute_node(int node_index) -> void;
  auto complete_node(int node_index) -> void;
  auto finish_node() -> void;
  auto record_error(std::string message) -> void;
  auto wait_for_completion() -> void;
};

struct WorkItem {
  ExecutionState* state = nullptr;
  int node_index = -1;
};

struct WorkQueue {
  std::mutex mutex;
  std::condition_variable cv;
  std::deque<WorkItem> items;
  bool stopped = false;

  auto push(WorkItem item) -> void {
    std::lock_guard<std::mutex> lock(mutex);
    items.push_back(item);
    cv.notify_one();
  }

  auto pop(WorkItem& out) -> bool {
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait(lock, [&]() { return stopped || !items.empty(); });
    if (items.empty()) {
      return false;
    }
    out = items.front();
    items.pop_front();
    return true;
  }

  auto stop() -> void {
    {
      std::lock_guard<std::mutex> lock(mutex);
      if (stopped) {
        return;
      }
      stopped = true;
    }
    cv.notify_all();
  }
};

struct Dispatcher {
  WorkQueue compute_queue;
  WorkQueue io_queue;
  int compute_workers = 0;
  int io_workers = 0;
  std::atomic<int> alive{0};
  std::atomic<bool> started{false};

  Dispatcher(int compute_workers, int io_workers)
      : compute_workers(compute_workers), io_workers(io_workers) {}

  auto enqueue(TaskType type, WorkItem item) -> void {
    if (type == TaskType::Io) {
      io_queue.push(item);
    } else {
      compute_queue.push(item);
    }
  }

  auto start(exec::static_thread_pool& compute_pool, exec::static_thread_pool& io_pool) -> void {
    bool expected = false;
    if (!started.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
      return;
    }
    alive.store(compute_workers + io_workers, std::memory_order_release);

    auto spawn = [this](exec::static_thread_pool& pool, WorkQueue& queue, int count) {
      auto scheduler = pool.get_scheduler();
      for (int i = 0; i < count; ++i) {
        auto task = stdexec::schedule(scheduler)
          | stdexec::then([this, &queue]() { this->worker_loop(queue); });
        stdexec::start_detached(std::move(task));
      }
    };

    spawn(compute_pool, compute_queue, compute_workers);
    spawn(io_pool, io_queue, io_workers);
  }

  auto stop() -> void {
    if (!started.load(std::memory_order_acquire)) {
      return;
    }
    compute_queue.stop();
    io_queue.stop();
    int count = alive.load(std::memory_order_acquire);
    while (count != 0) {
      alive.wait(count, std::memory_order_relaxed);
      count = alive.load(std::memory_order_acquire);
    }
  }

 private:
  auto worker_loop(WorkQueue& queue) -> void {
    WorkItem item{};
    while (queue.pop(item)) {
      item.state->execute_node(item.node_index);
    }
    if (alive.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      alive.notify_all();
    }
  }
};

auto ExecutionState::prepare(const ExecPlan& plan_ref, RequestContext& ctx_ref, Dispatcher& dispatcher_ref)
  -> Expected<void> {
  plan = &plan_ref;
  ctx = &ctx_ref;
  dispatcher = &dispatcher_ref;
  trace = &ctx_ref.trace;

  if (auto state = check_request_state(ctx_ref); !state) {
    return tl::unexpected(state.error());
  }
  if (auto env_result = prepare_env_values(plan_ref, ctx_ref, env_values); !env_result) {
    return tl::unexpected(env_result.error());
  }

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
          trace::emit(trace->sink, trace::RunStart{trace_id, run_span, plan_ref.name, run_start});
        }
        if (trace::has_flag(trace_flags, trace::TraceFlag::QueueDelay)) {
          enqueue_ticks.assign(node_count, 0);
        }
      }
    }
  }

  std::size_t total_inputs = 0;
  std::size_t total_outputs = 0;
  std::size_t total_missing = 0;
  for (const auto& node : plan_ref.nodes) {
    total_inputs += node.inputs.size();
    total_outputs += node.outputs.size();
    for (const auto& binding : node.inputs) {
      if (binding.kind == InputBindingKind::Missing) {
        total_missing += 1;
      }
    }
  }

  runtime_nodes.resize(node_count);
  input_ptrs.clear();
  output_ptrs.clear();
  missing_slots.clear();
  input_ptrs.reserve(total_inputs);
  output_ptrs.reserve(total_outputs);
  missing_slots.reserve(total_missing);

  for (std::size_t node_index = 0; node_index < node_count; ++node_index) {
    const auto& node = plan_ref.nodes[node_index];
    auto& runtime = runtime_nodes[node_index];
    runtime.input_offset = input_ptrs.size();
    runtime.input_count = node.inputs.size();
    runtime.output_offset = output_ptrs.size();
    runtime.output_count = node.outputs.size();

    for (const auto& binding : node.inputs) {
      switch (binding.kind) {
        case InputBindingKind::Slot: {
          input_ptrs.push_back(&slots[static_cast<std::size_t>(binding.slot_index)]);
          break;
        }
        case InputBindingKind::Const: {
          input_ptrs.push_back(&plan_ref.const_slots[static_cast<std::size_t>(binding.const_index)]);
          break;
        }
        case InputBindingKind::Env: {
          input_ptrs.push_back(env_values[static_cast<std::size_t>(binding.env_index)]);
          break;
        }
        case InputBindingKind::Missing: {
          ValueSlot slot;
          slot.type = binding.expected_type;
          slot.storage.reset();
          missing_slots.push_back(std::move(slot));
          input_ptrs.push_back(&missing_slots.back());
          break;
        }
      }
    }

    for (int slot_index : node.outputs) {
      output_ptrs.push_back(&slots[static_cast<std::size_t>(slot_index)]);
    }
  }

  pending.resize(node_count);
  scheduled.resize(node_count);
  for (std::size_t node_index = 0; node_index < node_count; ++node_index) {
    pending[node_index] = plan_ref.pending_counts[node_index];
    scheduled[node_index] = 0;
  }

  remaining.store(static_cast<int>(node_count), std::memory_order_release);
  aborted.store(false, std::memory_order_release);
  has_error.store(false, std::memory_order_release);
  error = EngineError{};
  return {};
}

auto ExecutionState::schedule_initial_nodes() -> void {
  const std::size_t node_count = plan->nodes.size();
  for (std::size_t node_index = 0; node_index < node_count; ++node_index) {
    if (std::atomic_ref<int>(pending[node_index]).load(std::memory_order_relaxed) == 0) {
      schedule_node(static_cast<int>(node_index));
    }
  }
}

auto ExecutionState::schedule_node(int node_index) -> void {
  if (!dispatcher || !plan) {
    return;
  }
  if (std::atomic_ref<int>(scheduled[static_cast<std::size_t>(node_index)])
        .exchange(1, std::memory_order_acq_rel) != 0) {
    return;
  }
  if constexpr (trace::kTraceEnabled) {
    if (trace && trace_flags != 0 && trace::has_flag(trace_flags, trace::TraceFlag::QueueDelay) &&
        node_index >= 0 && static_cast<std::size_t>(node_index) < enqueue_ticks.size()) {
      enqueue_ticks[static_cast<std::size_t>(node_index)] = trace->now();
    }
  }
  const auto& node = plan->nodes[static_cast<std::size_t>(node_index)];
  dispatcher->enqueue(node.kernel.task_type, WorkItem{this, node_index});
}

auto ExecutionState::execute_node(int node_index) -> void {
  auto& ctx_ref = *ctx;
  const auto& node = plan->nodes[static_cast<std::size_t>(node_index)];
  const auto& runtime = runtime_nodes[static_cast<std::size_t>(node_index)];

  trace::SpanId span_id = 0;
  trace::Tick start_ts = 0;
  if constexpr (trace::kTraceEnabled) {
    if (trace && trace_flags != 0 && trace->sink.enabled()) {
      if (trace::has_flag(trace_flags, trace::TraceFlag::NodeSpan)) {
        span_id = trace->new_span();
        start_ts = trace->now();
        trace::emit(trace->sink,
                    trace::NodeStart{trace_id, span_id, run_span, node.id, node_index, node.kernel.task_type, start_ts});
      }
      if (trace::has_flag(trace_flags, trace::TraceFlag::QueueDelay) &&
          static_cast<std::size_t>(node_index) < enqueue_ticks.size()) {
        trace::Tick delay_start = start_ts ? start_ts : trace->now();
        trace::emit(trace->sink, trace::QueueDelay{trace_id, span_id, node.id, node_index,
                                                   enqueue_ticks[static_cast<std::size_t>(node_index)], delay_start});
      }
    }
  }

  auto finish_trace = [&](trace::SpanStatus status, std::string_view message) {
    if constexpr (trace::kTraceEnabled) {
      if (trace && trace_flags != 0 && trace->sink.enabled()) {
        if (trace::has_flag(trace_flags, trace::TraceFlag::NodeSpan)) {
          trace::Tick end_ts = trace->now();
          trace::Tick duration = 0;
          if (start_ts != 0 && end_ts >= start_ts) {
            duration = end_ts - start_ts;
          }
          trace::emit(trace->sink,
                      trace::NodeEnd{trace_id, span_id, node.id, node_index, end_ts, duration, status});
        }
        if (!message.empty() && trace::has_flag(trace_flags, trace::TraceFlag::ErrorDetail)) {
          trace::emit(trace->sink, trace::NodeError{trace_id, span_id, node.id, node_index, message});
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

  auto input_view = InputValues(std::span<const ValueSlot* const>(
    input_ptrs.data() + runtime.input_offset, runtime.input_count));
  auto output_view =
    OutputValues(std::span<ValueSlot*>(output_ptrs.data() + runtime.output_offset, runtime.output_count));
  try {
    auto result = node.kernel.compute(node.kernel.instance.get(), ctx_ref, input_view, output_view);
    if (!result) {
      std::string message = result.error().message;
      finish_trace(trace::SpanStatus::Error, message);
      record_error(std::move(message));
      complete_node(node_index);
      return;
    }
  } catch (const std::exception& ex) {
    std::string message = ex.what();
    finish_trace(trace::SpanStatus::Error, message);
    record_error(std::move(message));
    complete_node(node_index);
    return;
  } catch (...) {
    std::string message = "unknown exception";
    finish_trace(trace::SpanStatus::Error, message);
    record_error(std::move(message));
    complete_node(node_index);
    return;
  }

  finish_trace(trace::SpanStatus::Ok, {});
  complete_node(node_index);
}

auto ExecutionState::complete_node(int node_index) -> void {
  for (int dependent : plan->dependents[static_cast<std::size_t>(node_index)]) {
    if (std::atomic_ref<int>(pending[static_cast<std::size_t>(dependent)])
          .fetch_sub(1, std::memory_order_acq_rel) == 1) {
      schedule_node(dependent);
    }
  }
  finish_node();
}

auto ExecutionState::finish_node() -> void {
  if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    remaining.notify_one();
  }
}

auto ExecutionState::record_error(std::string message) -> void {
  bool expected = false;
  if (has_error.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
    std::lock_guard<std::mutex> lock(error_mutex);
    error = make_error(std::move(message));
  }
  aborted.store(true, std::memory_order_release);
}

auto ExecutionState::wait_for_completion() -> void {
  int count = remaining.load(std::memory_order_acquire);
  while (count != 0) {
    remaining.wait(count, std::memory_order_relaxed);
    count = remaining.load(std::memory_order_acquire);
  }
}

}  // namespace

struct Executor::Pools {
  Pools(int compute_threads, int io_threads)
      : compute_pool(static_cast<std::size_t>(compute_threads)),
        io_pool(static_cast<std::size_t>(io_threads)) {}
  exec::static_thread_pool compute_pool;
  exec::static_thread_pool io_pool;
};

struct Executor::BufferPool {
  std::mutex mutex;
  std::vector<std::unique_ptr<ExecutionState>> pool;

  auto acquire() -> std::unique_ptr<ExecutionState> {
    std::lock_guard<std::mutex> lock(mutex);
    if (!pool.empty()) {
      auto state = std::move(pool.back());
      pool.pop_back();
      return state;
    }
    return std::make_unique<ExecutionState>();
  }

  auto release(std::unique_ptr<ExecutionState> state) -> void {
    std::lock_guard<std::mutex> lock(mutex);
    pool.push_back(std::move(state));
  }
};

struct Executor::Dispatcher {
  std::shared_ptr<::sr::engine::Dispatcher> impl;

  ~Dispatcher() {
    if (impl) {
      impl->stop();
    }
  }
};

Executor::Executor(ExecutorConfig config) {
  int compute_threads = config.compute_threads;
  if (compute_threads <= 0) {
    compute_threads = static_cast<int>(std::thread::hardware_concurrency());
    if (compute_threads <= 0) {
      compute_threads = 4;
    }
  }
  int io_threads = config.io_threads;
  if (io_threads <= 0) {
    io_threads = 2;
  }
  pools_ = std::make_shared<Pools>(compute_threads, io_threads);
  buffers_ = std::make_shared<BufferPool>();
  dispatcher_ = std::make_shared<Dispatcher>();
  dispatcher_->impl = std::make_shared<::sr::engine::Dispatcher>(compute_threads, io_threads);
  dispatcher_->impl->start(pools_->compute_pool, pools_->io_pool);
}

Executor::~Executor() = default;

auto Executor::run(const ExecPlan& plan, RequestContext& ctx) const -> Expected<ExecResult> {
  struct BufferLease {
    std::shared_ptr<BufferPool> pool;
    std::unique_ptr<ExecutionState> state;

    ~BufferLease() {
      if (pool && state) {
        pool->release(std::move(state));
      }
    }
  };

  BufferLease lease{buffers_, buffers_->acquire()};
  auto& state = *lease.state;
  if (auto prepared = state.prepare(plan, ctx, *dispatcher_->impl); !prepared) {
    return tl::unexpected(prepared.error());
  }

  state.schedule_initial_nodes();
  state.wait_for_completion();

  auto emit_run_end = [&](trace::SpanStatus status) {
    if constexpr (trace::kTraceEnabled) {
      auto* trace_ctx = state.trace;
      if (!trace_ctx || state.trace_flags == 0) {
        return;
      }
      if (!trace::has_flag(state.trace_flags, trace::TraceFlag::RunSpan) || state.run_span == 0) {
        return;
      }
      trace::Tick end_ts = trace_ctx->now();
      trace::Tick duration = 0;
      if (state.run_start != 0 && end_ts >= state.run_start) {
        duration = end_ts - state.run_start;
      }
      trace::emit(trace_ctx->sink,
                  trace::RunEnd{state.trace_id, state.run_span, end_ts, duration, status});
    }
  };

  if (state.has_error.load(std::memory_order_acquire)) {
    trace::SpanStatus status = trace::SpanStatus::Error;
    if (ctx.is_cancelled()) {
      status = trace::SpanStatus::Cancelled;
    } else if (ctx.deadline_exceeded()) {
      status = trace::SpanStatus::Deadline;
    }
    emit_run_end(status);
    std::lock_guard<std::mutex> lock(state.error_mutex);
    return tl::unexpected(state.error);
  }

  emit_run_end(trace::SpanStatus::Ok);
  return collect_outputs(plan, state.slots);
}

}  // namespace sr::engine
