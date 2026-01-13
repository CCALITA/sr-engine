#pragma once

#include <memory>
#include <unordered_map>

#include <exec/task.hpp>

#include "engine/error.hpp"
#include "engine/plan.hpp"
#include "engine/types.hpp"

namespace exec {
class static_thread_pool;
}

namespace sr::engine {

/// Execution result containing graph output slots keyed by output name.
struct ExecResult {
  std::unordered_map<std::string, ValueBox> outputs;
};

/// Thread pool sizing for the executor.
struct ExecutorConfig {
  /// Worker count for the shared pool (<=0 uses hardware concurrency).
  int compute_threads = 0;
};

/// Executes compiled plans using a shared worker pool.
class Executor {
public:
  /// Construct an executor with the provided thread pool.
  explicit Executor(exec::static_thread_pool *pool);
  /// Stops worker pools and releases resources.
  ~Executor();

  Executor(const Executor &) = default;
  Executor(Executor &&) noexcept = default;
  auto operator=(const Executor &) -> Executor & = default;
  auto operator=(Executor &&) noexcept -> Executor & = default;

  /// Execute a plan against a mutable request context.
  auto run(const ExecPlan &plan, RequestContext &ctx) const
      -> Expected<ExecResult>;

private:
  exec::static_thread_pool *pool_;
};

} // namespace sr::engine
