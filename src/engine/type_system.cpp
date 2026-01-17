#include "engine/type_system.hpp"

#include <array>
#include <cstring>

namespace sr::engine {
namespace {

constexpr auto fnv_offset = 14695981039346656037ULL;
constexpr auto fnv_prime = 1099511628211ULL;

auto hash_bytes(const std::uint8_t *data, std::size_t size) -> std::uint64_t {
  std::uint64_t hash = fnv_offset;
  for (std::size_t i = 0; i < size; ++i) {
    hash ^= data[i];
    hash *= fnv_prime;
  }
  return hash;
}

auto hash_string(std::string_view value) -> std::uint64_t {
  auto *data = reinterpret_cast<const std::uint8_t *>(value.data());
  return hash_bytes(data, value.size());
}

auto to_fingerprint(std::uint64_t high, std::uint64_t low) -> TypeFingerprint {
  TypeFingerprint fp{};
  for (int i = 0; i < 8; ++i) {
    fp.bytes[static_cast<std::size_t>(i)] =
        static_cast<std::uint8_t>((high >> (i * 8)) & 0xFF);
    fp.bytes[static_cast<std::size_t>(i + 8)] =
        static_cast<std::uint8_t>((low >> (i * 8)) & 0xFF);
  }
  return fp;
}

auto build_primitive_fingerprint(std::string_view name) -> TypeFingerprint {
  const auto name_hash = hash_string(name);
  const auto tag_hash = hash_string("prim");
  return to_fingerprint(tag_hash ^ name_hash, name_hash);
}

} // namespace

auto TypeRegistry::create() -> std::shared_ptr<TypeRegistry> {
  return std::shared_ptr<TypeRegistry>(new TypeRegistry());
}

auto TypeRegistry::intern_primitive(std::string_view name) -> TypeId {
  const auto fingerprint = build_primitive_fingerprint(name);
  std::uint64_t id = 0;
  std::memcpy(&id, fingerprint.bytes.data(), sizeof(TypeId));

  auto existing = id_to_index_.find(id);
  if (existing != id_to_index_.end()) {
    return id;
  }

  TypeInfo info;
  info.name = std::string(name);
  info.id = id;
  info.fingerprint = fingerprint;

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
