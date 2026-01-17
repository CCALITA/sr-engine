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

int main() {
  if (test_stable_type_id_for_primitive()) {
    std::cout << "[PASS] stable_type_id_for_primitive\n";
    return 0;
  }
  std::cout << "[FAIL] stable_type_id_for_primitive\n";
  return 1;
}
