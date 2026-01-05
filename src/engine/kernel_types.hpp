#pragma once

#include <memory>

#include "engine/error.hpp"
#include "engine/types.hpp"

namespace sr::engine {

/// Execution class for dispatching kernels to compute or IO workers.
enum class TaskType {
  Compute,
  Io,
};

/// Erased kernel instance plus signature and execution hook.
struct KernelHandle {
  Signature signature;
  std::shared_ptr<void> instance;
  Expected<void> (*compute)(void*, RequestContextView&, const InputValues&, OutputValues&);
  TaskType task_type = TaskType::Compute;
};

}  // namespace sr::engine
