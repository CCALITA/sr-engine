#pragma once

#include <string>
#include <utility>

#include "reflection/expected.hpp"

namespace sr::engine {

/// Simple error payload used by Expected<T>.
struct EngineError {
  std::string message;
};

/// Expected type alias for engine operations.
template <typename T>
using Expected = tl::expected<T, EngineError>;

/// Helper to construct an EngineError.
inline auto make_error(std::string message) -> EngineError {
  return EngineError{std::move(message)};
}

}  // namespace sr::engine
