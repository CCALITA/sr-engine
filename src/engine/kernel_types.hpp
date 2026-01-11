#pragma once

#include <memory>

#include "engine/error.hpp"
#include "engine/types.hpp"

namespace sr::engine {

/// Erased kernel instance plus execution hook.
struct KernelHandle {
  std::shared_ptr<void> instance;
  Expected<void> (*compute)(void *, const RequestContext &, const InputValues &,
                            OutputValues &);
};

} // namespace sr::engine
