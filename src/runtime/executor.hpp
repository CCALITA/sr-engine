#pragma once

#include <memory>
#include <unordered_map>

#include "engine/error.hpp"
#include "engine/plan.hpp"
#include "engine/types.hpp"

namespace sr::engine {

struct ExecResult {
  std::unordered_map<std::string, ValueSlot> outputs;
};

class Executor {
 public:
  Executor();
  explicit Executor(int thread_count);
  ~Executor();

  Executor(const Executor&) = default;
  Executor(Executor&&) noexcept = default;
  auto operator=(const Executor&) -> Executor& = default;
  auto operator=(Executor&&) noexcept -> Executor& = default;

  auto run(const ExecPlan& plan, RequestContext& ctx) const -> Expected<ExecResult>;
  auto run_dataflow(const ExecPlan& plan, RequestContext& ctx, int thread_count = 0) const -> Expected<ExecResult>;

 private:
  struct DataflowPool;
  std::shared_ptr<DataflowPool> pool_;
};

}  // namespace sr::engine
