#pragma once

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

  // Inline execution settings
  bool enable_inline_execution = true;        ///< Allow inline execution of cheap nodes
  std::uint32_t inline_threshold_ns = 10'000; ///< Max estimated cost for inline (10us default)
  int max_inline_depth = 3;                   ///< Maximum inline recursion depth

  // Continuation optimization
  bool enable_continuation_chaining = true;   ///< Use single_continuation hints
};

/// Executes compiled plans using a shared worker pool.
class Executor {
public:
  /// Construct an executor with the provided thread pool and default config.
  explicit Executor(exec::static_thread_pool *pool);
  /// Construct an executor with explicit config.
  Executor(exec::static_thread_pool *pool, ExecutorConfig config);
  /// Stops worker pools and releases resources.
  ~Executor();

  Executor(const Executor &) = default;
  Executor(Executor &&) noexcept = default;
  auto operator=(const Executor &) -> Executor & = default;
  auto operator=(Executor &&) noexcept -> Executor & = default;

  /// Access the current configuration.
  auto config() const -> const ExecutorConfig & { return config_; }

  /// Execute a plan against a mutable request context.
  auto run(const ExecPlan &plan, RequestContext &ctx) const
      -> Expected<ExecResult>;

  /// Execute a plan asynchronously.
  auto run_async(const ExecPlan &plan, RequestContext &ctx) const
      -> exec::task<Expected<ExecResult>>;

private:
  exec::static_thread_pool *pool_;
  ExecutorConfig config_;
};

} // namespace sr::engine
