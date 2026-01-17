#include "engine/type_hash.hpp"
#include "engine/type_system.hpp"
#include "test_support.hpp"

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
  return passed ? 0 : 1;
}
