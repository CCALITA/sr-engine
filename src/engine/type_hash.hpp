#pragma once

#include <array>
#include <cstdint>
#include <string_view>

namespace sr::engine {

/// 128-bit digest used for TypeId fingerprints.
struct TypeDigest {
  std::array<std::uint8_t, 16> bytes{};
};

/// Hash the canonical type encoding (truncated to 128 bits).
auto hash_type_bytes(std::string_view payload) -> TypeDigest;

} // namespace sr::engine
