#pragma once

#include <cstdint>
#include <memory>

#include "engine/error.hpp"
#include "engine/types.hpp"

namespace sr::engine {

/// Kernel trait flags for optimization decisions.
enum class KernelTraits : std::uint32_t {
  None = 0,
  Pure = 1u << 0,           ///< No side effects, deterministic output
  Fusible = 1u << 1,        ///< Can be fused with adjacent pure nodes
  NoContext = 1u << 2,      ///< Doesn't use RequestContext
  InlineHint = 1u << 3,     ///< Hint that this kernel is cheap to inline
  NeverInline = 1u << 4,    ///< Never inline this kernel (I/O, long-running)
};

/// Combine kernel traits with bitwise OR.
constexpr auto operator|(KernelTraits lhs, KernelTraits rhs) -> KernelTraits {
  return static_cast<KernelTraits>(
      static_cast<std::uint32_t>(lhs) | static_cast<std::uint32_t>(rhs));
}

/// Check if a trait is set.
constexpr auto has_trait(KernelTraits traits, KernelTraits flag) -> bool {
  return (static_cast<std::uint32_t>(traits) & static_cast<std::uint32_t>(flag)) != 0;
}

/// Erased kernel instance plus execution hook.
struct KernelHandle {
  std::shared_ptr<void> instance;
  Expected<void> (*compute)(void *, RequestContext &, const InputValues &,
                            OutputValues &);
  KernelTraits traits = KernelTraits::None;  ///< Optimization traits
  std::uint32_t estimated_cost_us = 0;       ///< Estimated cost in microseconds
};

} // namespace sr::engine
