#include "engine/type_system.hpp"

#include "engine/type_encoding.hpp"
#include "engine/type_hash.hpp"

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string_view>

namespace sr::engine {
namespace {

auto to_payload(const TypeEncoding &encoding) -> std::string_view {
  return std::string_view(
      reinterpret_cast<const char *>(encoding.bytes.data()),
      encoding.bytes.size());
}

// TypeId uses the little-endian interpretation of the first 8 digest bytes.
auto digest_to_type_id(const TypeDigest &digest) -> TypeId {
  TypeId id = 0;
  for (std::size_t i = 0; i < sizeof(TypeId); ++i) {
    id |= static_cast<TypeId>(digest.bytes[i]) << (i * 8);
  }
  return id;
}

auto digest_to_fingerprint(const TypeDigest &digest) -> TypeFingerprint {
  TypeFingerprint fingerprint{};
  fingerprint.bytes = digest.bytes;
  return fingerprint;
}

auto build_primitive_info(std::string_view name) -> TypeInfo {
  const auto encoding = encode_primitive(name);
  const auto digest = hash_type_bytes(to_payload(encoding));

  TypeInfo info;
  info.name = std::string(name);
  info.id = digest_to_type_id(digest);
  info.fingerprint = digest_to_fingerprint(digest);
  return info;
}

} // namespace

auto TypeRegistry::create() -> std::shared_ptr<TypeRegistry> {
  return std::shared_ptr<TypeRegistry>(new TypeRegistry());
}

auto TypeRegistry::intern_primitive(std::string_view name) -> TypeId {
  auto info = build_primitive_info(name);
  const auto id = info.id;

  auto existing = id_to_index_.find(id);
  if (existing != id_to_index_.end()) {
    const auto &stored = entries_[existing->second].info;
    if (stored.fingerprint.bytes != info.fingerprint.bytes) {
      throw std::runtime_error("type id collision detected");
    }
    return id;
  }

  id_to_index_.emplace(id, entries_.size());
  entries_.push_back(Entry{std::move(info)});
  return id;
}

auto TypeRegistry::lookup(TypeId id) const -> const TypeInfo * {
  auto it = id_to_index_.find(id);
  if (it == id_to_index_.end()) {
    return nullptr;
  }
  return &entries_[it->second].info;
}

} // namespace sr::engine
