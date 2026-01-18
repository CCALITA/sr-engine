#include "engine/type_encoding.hpp"

#include <array>
#include <limits>
#include <stdexcept>

namespace sr::engine {
namespace {

constexpr auto kPrimitiveKind = std::uint8_t{0x01};
constexpr auto kPrimitiveVersion = std::uint8_t{0x01};

auto write_u8(std::vector<std::uint8_t>& out, std::uint8_t value) -> void {
  out.push_back(value);
}

auto write_u32_le(std::vector<std::uint8_t>& out, std::uint32_t value) -> void {
  std::array<std::uint8_t, 4> bytes{
      static_cast<std::uint8_t>(value & 0xFFu),
      static_cast<std::uint8_t>((value >> 8) & 0xFFu),
      static_cast<std::uint8_t>((value >> 16) & 0xFFu),
      static_cast<std::uint8_t>((value >> 24) & 0xFFu)};
  out.insert(out.end(), bytes.begin(), bytes.end());
}

auto write_string(std::vector<std::uint8_t>& out, std::string_view value) -> void {
  if (value.size() > std::numeric_limits<std::uint32_t>::max()) {
    throw std::length_error("type encoding string too large");
  }
  const auto size = static_cast<std::uint32_t>(value.size());
  write_u32_le(out, size);
  out.insert(out.end(), value.begin(), value.end());
}

} // namespace

auto encode_primitive(std::string_view name) -> TypeEncoding {
  TypeEncoding encoding;
  write_u8(encoding.bytes, kPrimitiveKind);
  write_u8(encoding.bytes, kPrimitiveVersion);
  write_string(encoding.bytes, name);
  return encoding;
}

} // namespace sr::engine
