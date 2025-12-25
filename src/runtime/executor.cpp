#include "runtime/executor.hpp"

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>
#include <utility>

#include <exec/static_thread_pool.hpp>
#include <stdexec/execution.hpp>

namespace sr::engine {
namespace {

auto build_slots(const ExecPlan& plan) -> std::vector<ValueSlot> {
  std::vector<ValueSlot> slots;
  slots.reserve(plan.slots.size());
  for (const auto& spec : plan.slots) {
    ValueSlot slot;
    slot.type = spec.type;
    slots.push_back(std::move(slot));
  }
  return slots;
}

auto collect_outputs(const ExecPlan& plan, const std::vector<ValueSlot>& slots) -> Expected<ExecResult> {
  ExecResult result;
  for (const auto& [name, slot_index] : plan.output_slots) {
    const auto& slot = slots[static_cast<std::size_t>(slot_index)];
    if (!slot.has_value()) {
      return tl::unexpected(make_error("output slot not populated: " + name));
    }
    result.outputs.emplace(name, slot);
  }
  return result;
}

auto validate_env_bindings(const ExecPlan& plan, const RequestContext& ctx) -> Expected<void> {
  for (const auto& node : plan.nodes) {
    for (const auto& binding : node.inputs) {
      if (binding.kind != InputBindingKind::Env) {
        continue;
      }
      auto it = ctx.env.find(binding.env_key);
      if (it == ctx.env.end()) {
        return tl::unexpected(make_error("missing env value: " + binding.env_key));
      }
      if (binding.expected_type && it->second.type != binding.expected_type) {
        return tl::unexpected(make_error("env type mismatch: " + binding.env_key));
      }
    }
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

  NodeRuntime(std::vector<const ValueSlot*> inputs_in,
              std::vector<ValueSlot*> outputs_in,
              std::vector<ValueSlot> missing_in)
      : inputs(std::move(inputs_in)),
        outputs(std::move(outputs_in)),
        missing_slots(std::move(missing_in)),
        input_view(std::span<const ValueSlot* const>(inputs.data(), inputs.size())),
        output_view(std::span<ValueSlot*>(outputs.data(), outputs.size())) {}
};

}  // namespace

struct Executor::DataflowPool {
  explicit DataflowPool(int count) : pool(static_cast<std::size_t>(count)) {}
  exec::static_thread_pool pool;
};

Executor::Executor() = default;

Executor::Executor(int thread_count) {
  if (thread_count > 0) {
    pool_ = std::make_shared<DataflowPool>(thread_count);
  }
}

Executor::~Executor() = default;

auto Executor::run(const ExecPlan& plan, RequestContext& ctx) const -> Expected<ExecResult> {
  if (auto state = check_request_state(ctx); !state) {
    return tl::unexpected(state.error());
  }
  auto slots = build_slots(plan);

  for (int node_index : plan.topo_order) {
    if (auto state = check_request_state(ctx); !state) {
      return tl::unexpected(state.error());
    }
    const auto& node = plan.nodes[static_cast<std::size_t>(node_index)];
    std::vector<const ValueSlot*> inputs;
    inputs.reserve(node.inputs.size());
    std::size_t missing_count = 0;
    for (const auto& binding : node.inputs) {
      if (binding.kind == InputBindingKind::Missing) {
        missing_count += 1;
      }
    }
    std::vector<ValueSlot> missing_slots;
    missing_slots.reserve(missing_count);
    for (const auto& binding : node.inputs) {
      switch (binding.kind) {
        case InputBindingKind::Slot: {
          inputs.push_back(&slots[static_cast<std::size_t>(binding.slot_index)]);
          break;
        }
        case InputBindingKind::Const: {
          inputs.push_back(&plan.const_slots[static_cast<std::size_t>(binding.const_index)]);
          break;
        }
        case InputBindingKind::Env: {
          auto it = ctx.env.find(binding.env_key);
          if (it == ctx.env.end()) {
            return tl::unexpected(make_error("missing env value: " + binding.env_key));
          }
          if (binding.expected_type && it->second.type != binding.expected_type) {
            return tl::unexpected(make_error("env type mismatch: " + binding.env_key));
          }
          inputs.push_back(&it->second);
          break;
        }
        case InputBindingKind::Missing: {
          ValueSlot slot;
          slot.type = binding.expected_type;
          missing_slots.push_back(std::move(slot));
          inputs.push_back(&missing_slots.back());
          break;
        }
      }
    }

    std::vector<ValueSlot*> outputs;
    outputs.reserve(node.outputs.size());
    for (int slot_index : node.outputs) {
      outputs.push_back(&slots[static_cast<std::size_t>(slot_index)]);
    }

    InputValues input_view(std::span<const ValueSlot* const>(inputs.data(), inputs.size()));
    OutputValues output_view(std::span<ValueSlot*>(outputs.data(), outputs.size()));

    auto sender = node.kernel.exec(node.kernel.instance.get(), ctx, input_view, output_view);
    (void)stdexec::sync_wait(std::move(sender));
  }

  return collect_outputs(plan, slots);
}

auto Executor::run_dataflow(const ExecPlan& plan, RequestContext& ctx, int thread_count) const -> Expected<ExecResult> {
  if (auto env_check = validate_env_bindings(plan, ctx); !env_check) {
    return tl::unexpected(env_check.error());
  }
  if (auto state = check_request_state(ctx); !state) {
    return tl::unexpected(state.error());
  }

  auto slots = build_slots(plan);
  const std::size_t node_count = plan.nodes.size();
  if (node_count == 0) {
    return tl::unexpected(make_error("graph has no nodes"));
  }

  std::vector<NodeRuntime> runtime_nodes;
  runtime_nodes.reserve(node_count);
  for (std::size_t node_index = 0; node_index < node_count; ++node_index) {
    const auto& node = plan.nodes[node_index];
    std::vector<const ValueSlot*> inputs;
    inputs.reserve(node.inputs.size());
    std::vector<ValueSlot*> outputs;
    outputs.reserve(node.outputs.size());
    std::vector<ValueSlot> missing_slots;
    std::size_t missing_count = 0;
    for (const auto& binding : node.inputs) {
      if (binding.kind == InputBindingKind::Missing) {
        missing_count += 1;
      }
    }
    missing_slots.reserve(missing_count);

    for (const auto& binding : node.inputs) {
      switch (binding.kind) {
        case InputBindingKind::Slot: {
          inputs.push_back(&slots[static_cast<std::size_t>(binding.slot_index)]);
          break;
        }
        case InputBindingKind::Const: {
          inputs.push_back(&plan.const_slots[static_cast<std::size_t>(binding.const_index)]);
          break;
        }
        case InputBindingKind::Env: {
          inputs.push_back(&ctx.env.at(binding.env_key));
          break;
        }
        case InputBindingKind::Missing: {
          ValueSlot slot;
          slot.type = binding.expected_type;
          missing_slots.push_back(std::move(slot));
          inputs.push_back(&missing_slots.back());
          break;
        }
      }
    }

    for (int slot_index : node.outputs) {
      outputs.push_back(&slots[static_cast<std::size_t>(slot_index)]);
    }

    runtime_nodes.emplace_back(std::move(inputs), std::move(outputs), std::move(missing_slots));
  }

  std::vector<int> slot_producer(plan.slots.size(), -1);
  for (std::size_t node_index = 0; node_index < node_count; ++node_index) {
    const auto& node = plan.nodes[node_index];
    for (int slot_index : node.outputs) {
      slot_producer[static_cast<std::size_t>(slot_index)] = static_cast<int>(node_index);
    }
  }

  std::vector<std::vector<int>> dependents(node_count);
  std::vector<std::atomic<int>> pending(node_count);
  std::vector<int> seen(node_count, -1);
  int stamp = 0;

  for (std::size_t node_index = 0; node_index < node_count; ++node_index) {
    stamp += 1;
    int count = 0;
    const auto& node = plan.nodes[node_index];
    for (const auto& binding : node.inputs) {
      if (binding.kind != InputBindingKind::Slot) {
        continue;
      }
      int producer = slot_producer[static_cast<std::size_t>(binding.slot_index)];
      if (producer < 0) {
        return tl::unexpected(make_error("missing slot producer"));
      }
      if (producer == static_cast<int>(node_index)) {
        continue;
      }
      if (seen[static_cast<std::size_t>(producer)] == stamp) {
        continue;
      }
      seen[static_cast<std::size_t>(producer)] = stamp;
      dependents[static_cast<std::size_t>(producer)].push_back(static_cast<int>(node_index));
      count += 1;
    }
    pending[node_index].store(count, std::memory_order_relaxed);
  }

  exec::static_thread_pool* pool_ptr = nullptr;
  std::unique_ptr<exec::static_thread_pool> local_pool;
  if (pool_) {
    pool_ptr = &pool_->pool;
  } else {
    if (thread_count <= 0) {
      thread_count = static_cast<int>(std::thread::hardware_concurrency());
      if (thread_count <= 0) {
        thread_count = 4;
      }
    }
    local_pool = std::make_unique<exec::static_thread_pool>(static_cast<std::size_t>(thread_count));
    pool_ptr = local_pool.get();
  }
  auto scheduler = pool_ptr->get_scheduler();

  std::atomic<int> remaining(static_cast<int>(node_count));
  std::atomic<bool> aborted(false);
  std::atomic<bool> has_error(false);
  std::mutex error_mutex;
  EngineError error;

  std::mutex done_mutex;
  std::condition_variable done_cv;

  auto finish_node = [&]() {
    if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      std::lock_guard<std::mutex> lock(done_mutex);
      done_cv.notify_all();
    }
  };

  auto record_error = [&](std::string message) {
    bool expected = false;
    if (has_error.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
      std::lock_guard<std::mutex> lock(error_mutex);
      error = make_error(std::move(message));
    }
    aborted.store(true, std::memory_order_release);
  };

  std::function<void(int)> schedule_node;
  auto complete_node = [&](int node_index) {
    for (int dependent : dependents[static_cast<std::size_t>(node_index)]) {
      if (pending[static_cast<std::size_t>(dependent)].fetch_sub(1, std::memory_order_acq_rel) == 1) {
        schedule_node(dependent);
      }
    }
    finish_node();
  };

  schedule_node = [&](int node_index) {
    auto body = stdexec::schedule(scheduler)
      | stdexec::let_value([&, node_index]() -> ErasedSender {
          if (aborted.load(std::memory_order_acquire)) {
            return ErasedSender{stdexec::just()};
          }
          if (ctx.is_cancelled()) {
            record_error("request cancelled");
            return ErasedSender{stdexec::just()};
          }
          if (ctx.deadline_exceeded()) {
            record_error("deadline exceeded");
            return ErasedSender{stdexec::just()};
          }
          auto& runtime = runtime_nodes[static_cast<std::size_t>(node_index)];
          const auto& node = plan.nodes[static_cast<std::size_t>(node_index)];
          return node.kernel.exec(node.kernel.instance.get(), ctx, runtime.input_view, runtime.output_view);
        });

    auto on_success = stdexec::then(std::move(body), [&, node_index]() {
      complete_node(node_index);
    });

    auto on_error = stdexec::upon_error(std::move(on_success), [&, node_index](std::exception_ptr ep) {
      std::string message = "node failed";
      if (ep) {
        try {
          std::rethrow_exception(ep);
        } catch (const std::exception& ex) {
          message = ex.what();
        } catch (...) {
          message = "unknown exception";
        }
      }
      record_error(std::move(message));
      complete_node(node_index);
    });

    auto on_stopped = stdexec::upon_stopped(std::move(on_error), [&, node_index]() {
      record_error("execution stopped");
      complete_node(node_index);
    });

    stdexec::start_detached(std::move(on_stopped));
  };

  for (std::size_t node_index = 0; node_index < node_count; ++node_index) {
    if (pending[node_index].load(std::memory_order_relaxed) == 0) {
      schedule_node(static_cast<int>(node_index));
    }
  }

  std::unique_lock<std::mutex> lock(done_mutex);
  done_cv.wait(lock, [&]() { return remaining.load(std::memory_order_acquire) == 0; });

  if (has_error.load(std::memory_order_acquire)) {
    std::lock_guard<std::mutex> lock(error_mutex);
    return tl::unexpected(error);
  }

  return collect_outputs(plan, slots);
}

}  // namespace sr::engine
