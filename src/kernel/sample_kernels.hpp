#pragma once

#include "engine/registry.hpp"

namespace sr::kernel {

/// Register builtin value types used by sample kernels.
auto register_builtin_types(sr::engine::TypeRegistry &registry) -> void;

/// Register sample kernels into a registry (for demos/tests).
auto register_sample_kernels(sr::engine::KernelRegistry& registry) -> void;

}  // namespace sr::kernel
