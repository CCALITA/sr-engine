#pragma once

#include <memory>

#include "engine/error.hpp"
#include "engine/types.hpp"

namespace sr::engine {

/// Erased kernel instance plus signature and execution hook.
struct KernelHandle {
  Signature signature;
  std::shared_ptr<void> instance;
  Expected<void> (*compute)(void*, RequestContextView&, const InputValues&, OutputValues&);
};

}  // namespace sr::engine
