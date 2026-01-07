#pragma once

#include <memory>
#include <unordered_map>

#include "engine/error.hpp"
#include "engine/graph_store.hpp"
#include "engine/types.hpp"

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

/// Executes compiled plans using a sender-based graph over a shared worker pool.
class Executor {
public:
  /// Construct an executor with the provided thread configuration.
  explicit Executor(ExecutorConfig config = {});
  /// Stops worker pools and releases resources.
  ~Executor();

  Executor(const Executor &) = default;
  Executor(Executor &&) noexcept = default;
  auto operator=(const Executor &) -> Executor & = default;
  auto operator=(Executor &&) noexcept -> Executor & = default;

  /// Execute a plan snapshot against a mutable request context.
  auto run(const std::shared_ptr<const PlanSnapshot> &snapshot,
           RequestContext &ctx) const
      -> Expected<ExecResult>;

private:
  struct Pools;
  std::shared_ptr<Pools> pools_;
};

} // namespace sr::engine
