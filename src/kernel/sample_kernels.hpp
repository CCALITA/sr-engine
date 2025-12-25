#pragma once

#include "engine/registry.hpp"

namespace sr::kernel {

auto register_builtin_types() -> void;

auto register_sample_kernels(sr::engine::KernelRegistry& registry) -> void;

}  // namespace sr::kernel
