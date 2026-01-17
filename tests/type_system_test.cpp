#include "engine/type_encoding.hpp"
#include "engine/type_hash.hpp"
#include "engine/type_system.hpp"
#include "test_support.hpp"

#include <array>

auto test_stable_type_id_for_primitive() -> bool {
  auto registry = sr::engine::TypeRegistry::create();
  const auto id1 = registry->intern_primitive("i64");
  const auto id2 = registry->intern_primitive("i64");
  if (id1 != id2) {
    std::cerr << "expected stable TypeId for primitive\n";
    return false;
  }
  return true;
}

auto test_type_hash_stable() -> bool {
  const auto digest1 = sr::engine::hash_type_bytes("prim:i64");
  const auto digest2 = sr::engine::hash_type_bytes("prim:i64");
  if (digest1.bytes != digest2.bytes) {
    std::cerr << "expected stable hash for type bytes\n";
    return false;
  }
  return true;
}

auto test_typeid_from_encoding() -> bool {
  auto registry = sr::engine::TypeRegistry::create();
  const auto encoding = sr::engine::encode_primitive("i64");
  const auto payload = std::string_view(
      reinterpret_cast<const char *>(encoding.bytes.data()),
      encoding.bytes.size());
  const auto digest = sr::engine::hash_type_bytes(payload);

  std::uint64_t expected_id = 0;
  for (std::size_t i = 0; i < sizeof(std::uint64_t); ++i) {
    expected_id |= static_cast<std::uint64_t>(digest.bytes[i]) << (i * 8);
  }

  const auto id = registry->intern_primitive("i64");
  if (id == 0 || id != expected_id) {
    std::cerr << "expected TypeId derived from encoding\n";
    return false;
  }

  const auto *info = registry->lookup(id);
  if (!info) {
    std::cerr << "expected lookup for TypeId\n";
    return false;
  }
  if (info->fingerprint.bytes != digest.bytes) {
    std::cerr << "expected fingerprint to store digest\n";
    return false;
  }
  return true;
}

auto test_primitive_encoding_stable() -> bool {
  const auto bytes1 = sr::engine::encode_primitive("i64");
  const auto bytes2 = sr::engine::encode_primitive("i64");
  if (bytes1.bytes != bytes2.bytes) {
    std::cerr << "expected stable encoding for primitive\n";
    return false;
  }
  const std::array<std::uint8_t, 9> expected{
      0x01, 0x01, 0x03, 0x00, 0x00, 0x00, 0x69, 0x36, 0x34};
  if (bytes1.bytes != std::vector<std::uint8_t>(expected.begin(), expected.end())) {
    std::cerr << "unexpected primitive encoding bytes\n";
    return false;
  }
  return true;
}

int main() {
  auto passed = true;
  if (test_stable_type_id_for_primitive()) {
    std::cout << "[PASS] stable_type_id_for_primitive\n";
  } else {
    std::cout << "[FAIL] stable_type_id_for_primitive\n";
    passed = false;
  }
  if (test_type_hash_stable()) {
    std::cout << "[PASS] type_hash_stable\n";
  } else {
    std::cout << "[FAIL] type_hash_stable\n";
    passed = false;
  }
  if (test_primitive_encoding_stable()) {
    std::cout << "[PASS] primitive_encoding_stable\n";
  } else {
    std::cout << "[FAIL] primitive_encoding_stable\n";
    passed = false;
  }
  if (test_typeid_from_encoding()) {
    std::cout << "[PASS] typeid_from_encoding\n";
  } else {
    std::cout << "[FAIL] typeid_from_encoding\n";
    passed = false;
  }
  return passed ? 0 : 1;
}
