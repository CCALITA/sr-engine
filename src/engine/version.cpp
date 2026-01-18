#include "engine/version.hpp"

#include <charconv>
#include <vector>
#include <sstream>

namespace sr::engine {

auto Version::to_string() const -> std::string {
  return std::format("{}.{}.{}", major, minor, patch);
}

auto Version::parse(std::string_view str) -> Expected<Version> {
  if (str.empty()) {
    return tl::unexpected(make_error("version string cannot be empty"));
  }

  // Handle optional 'v' prefix
  if (str.starts_with('v') || str.starts_with('V')) {
    str.remove_prefix(1);
  }

  Version v;
  const char* ptr = str.data();
  const char* end = str.data() + str.size();

  // Helper to parse integer component
  auto parse_component = [&](int& out) -> bool {
    auto res = std::from_chars(ptr, end, out);
    if (res.ec != std::errc{}) {
      return false;
    }
    ptr = res.ptr;
    return true;
  };

  // Major
  if (!parse_component(v.major)) {
    return tl::unexpected(make_error(std::format("invalid major version in '{}'", str)));
  }

  // Minor (optional)
  if (ptr < end && *ptr == '.') {
    ptr++;
    if (!parse_component(v.minor)) {
       return tl::unexpected(make_error(std::format("invalid minor version in '{}'", str)));
    }
  }

  // Patch (optional)
  if (ptr < end && *ptr == '.') {
    ptr++;
    if (!parse_component(v.patch)) {
       return tl::unexpected(make_error(std::format("invalid patch version in '{}'", str)));
    }
  }

  // Ensure strict match (no trailing garbage)
  if (ptr != end) {
      return tl::unexpected(make_error(std::format("extra characters in version string '{}'", str)));
  }

  return v;
}

} // namespace sr::engine
