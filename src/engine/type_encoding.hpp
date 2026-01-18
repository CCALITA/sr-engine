#pragma once

#include <cstdint>
#include <string_view>
#include <vector>

namespace sr::engine {

struct TypeEncoding {
  std::vector<std::uint8_t> bytes;
};

auto encode_primitive(std::string_view name) -> TypeEncoding;

} // namespace sr::engine
