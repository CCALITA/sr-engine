#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace sr::engine {

using TypeId = std::uint64_t;

struct TypeFingerprint {
  std::array<std::uint8_t, 16> bytes{};
};

struct TypeInfo {
  std::string name;
  TypeId id{};
  TypeFingerprint fingerprint{};
};

class TypeRegistry {
public:
  static auto create() -> std::shared_ptr<TypeRegistry>;

  auto intern_primitive(std::string_view name) -> TypeId;
  auto lookup(TypeId id) const -> const TypeInfo *;

private:
  struct Entry {
    TypeInfo info;
  };

  std::unordered_map<TypeId, std::size_t> id_to_index_;
  std::vector<Entry> entries_;
};

} // namespace sr::engine
