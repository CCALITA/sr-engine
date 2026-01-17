#pragma once

#include <array>
#include <cstdint>
#include <string_view>

namespace sr::engine {

struct TypeDigest {
  std::array<std::uint8_t, 16> bytes{};
};

auto hash_type_bytes(std::string_view payload) -> TypeDigest;

} // namespace sr::engine
