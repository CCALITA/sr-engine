#include "runtime/executor.hpp"

#include <atomic>
#include <condition_variable>
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

struct NodeRuntime {
  std::vector<const ValueSlot*> inputs;
  std::vector<ValueSlot*> outputs;
  std::vector<ValueSlot> missing_slots;
  InputValues input_view;
  OutputValues output_view;
};

struct ExecutionState {
  std::vector<ValueSlot> slots;
  std::vector<NodeRuntime> runtime_nodes;
  std::vector<const ValueSlot*> env_values;
  std::vector<int> pending;
  std::vector<int> scheduled;

  std::atomic<int> remaining{0};
  std::atomic<bool> aborted{false};
  std::atomic<bool> has_error{false};
  std::mutex error_mutex;
  EngineError error;

  std::mutex done_mutex;
  std::condition_variable done_cv;

  auto prepare(const ExecPlan& plan, RequestContext& ctx) -> Expected<void> {
    if (auto state = check_request_state(ctx); !state) {
      return tl::unexpected(state.error());
    }
    if (auto env_result = prepare_env_values(plan, ctx, env_values); !env_result) {
      return tl::unexpected(env_result.error());
    }

    reset_slots(plan, slots);

    const std::size_t node_count = plan.nodes.size();
    if (node_count == 0) {
      return tl::unexpected(make_error("graph has no nodes"));
    }

    runtime_nodes.resize(node_count);
    for (std::size_t node_index = 0; node_index < node_count; ++node_index) {
      const auto& node = plan.nodes[node_index];
      auto& runtime = runtime_nodes[node_index];
      runtime.inputs.clear();
      runtime.outputs.clear();
      runtime.inputs.reserve(node.inputs.size());
      runtime.outputs.reserve(node.outputs.size());

      std::size_t missing_count = 0;
      for (const auto& binding : node.inputs) {
        if (binding.kind == InputBindingKind::Missing) {
          missing_count += 1;
        }
      }
      runtime.missing_slots.clear();
      runtime.missing_slots.resize(missing_count);
      std::size_t missing_index = 0;

      for (const auto& binding : node.inputs) {
        switch (binding.kind) {
          case InputBindingKind::Slot: {
            runtime.inputs.push_back(&slots[static_cast<std::size_t>(binding.slot_index)]);
            break;
          }
          case InputBindingKind::Const: {
            runtime.inputs.push_back(&plan.const_slots[static_cast<std::size_t>(binding.const_index)]);
            break;
          }
          case InputBindingKind::Env: {
            runtime.inputs.push_back(env_values[static_cast<std::size_t>(binding.env_index)]);
            break;
          }
          case InputBindingKind::Missing: {
            auto& slot = runtime.missing_slots[missing_index++];
            slot.type = binding.expected_type;
            slot.storage.reset();
            runtime.inputs.push_back(&slot);
            break;
          }
        }
      }

      for (int slot_index : node.outputs) {
        runtime.outputs.push_back(&slots[static_cast<std::size_t>(slot_index)]);
      }

      runtime.input_view = InputValues(
        std::span<const ValueSlot* const>(runtime.inputs.data(), runtime.inputs.size()));
      runtime.output_view = OutputValues(std::span<ValueSlot*>(runtime.outputs.data(), runtime.outputs.size()));
    }

    pending.resize(node_count);
    scheduled.resize(node_count);
    for (std::size_t node_index = 0; node_index < node_count; ++node_index) {
      pending[node_index] = plan.pending_counts[node_index];
      scheduled[node_index] = 0;
    }

    remaining.store(static_cast<int>(node_count), std::memory_order_release);
    aborted.store(false, std::memory_order_release);
    has_error.store(false, std::memory_order_release);
    return {};
  }

  auto finish_node() -> void {
    if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      std::lock_guard<std::mutex> lock(done_mutex);
      done_cv.notify_all();
    }
  }

  auto record_error(std::string message) -> void {
    bool expected = false;
    if (has_error.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
      std::lock_guard<std::mutex> lock(error_mutex);
      error = make_error(std::move(message));
    }
    aborted.store(true, std::memory_order_release);
  }

  auto wait_for_completion() -> void {
    std::unique_lock<std::mutex> lock(done_mutex);
    done_cv.wait(lock, [&]() { return remaining.load(std::memory_order_acquire) == 0; });
  }
};

template <typename ComputeScheduler, typename IoScheduler>
class ExecutionDriver {
 public:
  ExecutionDriver(const ExecPlan& plan, RequestContext& ctx, ExecutionState& state, ComputeScheduler compute,
                  IoScheduler io)
      : plan_(plan), ctx_(ctx), state_(state), compute_(std::move(compute)), io_(std::move(io)) {}

  auto run() -> void {
    const std::size_t node_count = plan_.nodes.size();
    for (std::size_t node_index = 0; node_index < node_count; ++node_index) {
      if (std::atomic_ref<int>(state_.pending[node_index]).load(std::memory_order_relaxed) == 0) {
        schedule_node(static_cast<int>(node_index));
      }
    }
    state_.wait_for_completion();
  }

 private:
  auto schedule_node(int node_index) -> void {
    if (std::atomic_ref<int>(state_.scheduled[static_cast<std::size_t>(node_index)])
          .exchange(1, std::memory_order_acq_rel) != 0) {
      return;
    }
    auto& node = plan_.nodes[static_cast<std::size_t>(node_index)];
    auto scheduler = (node.kernel.task_type == TaskType::Io) ? io_ : compute_;
    auto task = stdexec::schedule(scheduler)
      | stdexec::then([this, node_index]() { this->execute_node(node_index); });

    stdexec::start_detached(std::move(task));
  }

  auto execute_node(int node_index) -> void {
    if (state_.aborted.load(std::memory_order_acquire)) {
      complete_node(node_index);
      return;
    }
    if (ctx_.is_cancelled()) {
      state_.record_error("request cancelled");
      complete_node(node_index);
      return;
    }
    if (ctx_.deadline_exceeded()) {
      state_.record_error("deadline exceeded");
      complete_node(node_index);
      return;
    }
    auto& runtime = state_.runtime_nodes[static_cast<std::size_t>(node_index)];
    const auto& node = plan_.nodes[static_cast<std::size_t>(node_index)];
    try {
      auto result = node.kernel.compute(node.kernel.instance.get(), ctx_, runtime.input_view, runtime.output_view);
      if (!result) {
        state_.record_error(result.error().message);
      }
    } catch (const std::exception& ex) {
      state_.record_error(ex.what());
    } catch (...) {
      state_.record_error("unknown exception");
    }
    complete_node(node_index);
  }

  auto complete_node(int node_index) -> void {
    for (int dependent : plan_.dependents[static_cast<std::size_t>(node_index)]) {
      if (std::atomic_ref<int>(state_.pending[static_cast<std::size_t>(dependent)])
            .fetch_sub(1, std::memory_order_acq_rel) == 1) {
        schedule_node(dependent);
      }
    }
    state_.finish_node();
  }

  const ExecPlan& plan_;
  RequestContext& ctx_;
  ExecutionState& state_;
  ComputeScheduler compute_;
  IoScheduler io_;
};

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
  if (auto prepared = state.prepare(plan, ctx); !prepared) {
    return tl::unexpected(prepared.error());
  }

  auto compute_scheduler = pools_->compute_pool.get_scheduler();
  auto io_scheduler = pools_->io_pool.get_scheduler();
  ExecutionDriver driver{plan, ctx, state, compute_scheduler, io_scheduler};
  driver.run();

  if (state.has_error.load(std::memory_order_acquire)) {
    std::lock_guard<std::mutex> lock(state.error_mutex);
    return tl::unexpected(state.error);
  }

  return collect_outputs(plan, state.slots);
}

}  // namespace sr::engine
