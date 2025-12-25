#pragma once

#include <string>
#include <utility>

#include "reflection/expected.hpp"

namespace sr::engine {

struct EngineError {
  std::string message;
};

template <typename T>
using Expected = tl::expected<T, EngineError>;

inline auto make_error(std::string message) -> EngineError {
  return EngineError{std::move(message)};
}

}  // namespace sr::engine
