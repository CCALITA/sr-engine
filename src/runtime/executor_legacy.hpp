#pragma once

#include <memory>

#include "runtime/executor.hpp"

namespace sr::engine {

/// Legacy executor using the original task-queue scheduling strategy.
class LegacyExecutor {
public:
  /// Construct an executor with the provided thread configuration.
  explicit LegacyExecutor(ExecutorConfig config = {});
  /// Stops worker pools and releases resources.
  ~LegacyExecutor();

  LegacyExecutor(const LegacyExecutor &) = default;
  LegacyExecutor(LegacyExecutor &&) noexcept = default;
  auto operator=(const LegacyExecutor &) -> LegacyExecutor & = default;
  auto operator=(LegacyExecutor &&) noexcept -> LegacyExecutor & = default;

  /// Execute a plan against a mutable request context.
  auto run(const ExecPlan &plan, RequestContext &ctx) const
      -> Expected<ExecResult>;

private:
  struct Pools;
  std::shared_ptr<Pools> pools_;
};

} // namespace sr::engine
