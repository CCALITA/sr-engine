#pragma once

#include <compare>
#include <string>
#include <string_view>
#include <format>
#include <functional>

#include "engine/error.hpp"

namespace sr::engine {

/// Semantic Versioning (SemVer) structure.
struct Version {
  int major = 0;
  int minor = 0;
  int patch = 0;

  Version() = default;
  Version(int major, int minor = 0, int patch = 0)
      : major(major), minor(minor), patch(patch) {}

  /// Default strict comparison.
  auto operator<=>(const Version&) const = default;

  /// Format as "major.minor.patch".
  auto to_string() const -> std::string;

  /// Parse from string (e.g. "1.0.1", "1.2", "1").
  /// Returns error if invalid format.
  static auto parse(std::string_view str) -> Expected<Version>;
};

} // namespace sr::engine

/// Hash specialization for Version.
template <>
struct std::hash<sr::engine::Version> {
  auto operator()(const sr::engine::Version& v) const -> std::size_t {
    std::size_t h1 = std::hash<int>{}(v.major);
    std::size_t h2 = std::hash<int>{}(v.minor);
    std::size_t h3 = std::hash<int>{}(v.patch);
    return h1 ^ (h2 << 1) ^ (h3 << 2);
  }
};

template <>
struct std::formatter<sr::engine::Version> : std::formatter<std::string> {
  auto format(const sr::engine::Version& v, std::format_context& ctx) const {
    return formatter<std::string>::format(v.to_string(), ctx);
  }
};
