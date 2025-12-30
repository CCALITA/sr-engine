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

struct ExecutorConfig {
  int compute_threads = 0;
  int io_threads = 0;
};

class Executor {
 public:
  explicit Executor(ExecutorConfig config = {});
  ~Executor();

  Executor(const Executor&) = default;
  Executor(Executor&&) noexcept = default;
  auto operator=(const Executor&) -> Executor& = default;
  auto operator=(Executor&&) noexcept -> Executor& = default;

  auto run(const ExecPlan& plan, RequestContext& ctx) const -> Expected<ExecResult>;

 private:
  struct Pools;
  struct BufferPool;
  std::shared_ptr<Pools> pools_;
  std::shared_ptr<BufferPool> buffers_;
};

}  // namespace sr::engine
