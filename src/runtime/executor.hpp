#pragma once

#include <memory>
#include <unordered_map>

#include "engine/error.hpp"
#include "engine/plan.hpp"
#include "engine/types.hpp"

namespace sr::engine {

/// Execution result containing graph output slots keyed by output name.
struct ExecResult {
  std::unordered_map<std::string, ValueSlot> outputs;
};

/// Thread pool sizing for the executor.
struct ExecutorConfig {
  /// Compute worker count (<=0 uses hardware concurrency, fallback to 4).
  int compute_threads = 0;
  /// IO worker count (<=0 defaults to 2).
  int io_threads = 0;
};

/// Executes compiled plans using compute and IO worker pools.
class Executor {
 public:
  /// Construct an executor with the provided thread configuration.
  explicit Executor(ExecutorConfig config = {});
  /// Stops worker pools and releases resources.
  ~Executor();

  Executor(const Executor&) = default;
  Executor(Executor&&) noexcept = default;
  auto operator=(const Executor&) -> Executor& = default;
  auto operator=(Executor&&) noexcept -> Executor& = default;

  /// Execute a plan against a mutable request context.
  auto run(const ExecPlan& plan, RequestContext& ctx) const -> Expected<ExecResult>;

 private:
  struct Pools;
  struct BufferPool;
  struct Dispatcher;
  std::shared_ptr<Pools> pools_;
  std::shared_ptr<BufferPool> buffers_;
  std::shared_ptr<Dispatcher> dispatcher_;
};

}  // namespace sr::engine
