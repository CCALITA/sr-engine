#include "engine/type_system.hpp"
#include <gtest/gtest.h>

TEST(TypeSystem, StableTypeIdForPrimitive) {
  auto registry = sr::engine::TypeRegistry::create();
  auto id1 = registry->intern_primitive("i64");
  auto id2 = registry->intern_primitive("i64");
  ASSERT_EQ(id1, id2);
}

TEST(TypeSystem, FunctionTypeStableId) {
  auto registry = sr::engine::TypeRegistry::create();
  auto i64 = registry->intern_primitive("i64");
  auto f64 = registry->intern_primitive("f64");
  
  std::vector<sr::engine::TypeId> inputs = {i64, f64};
  std::vector<sr::engine::TypeId> outputs = {i64};
  
  auto fn = registry->intern_function(inputs, outputs, {.noexcept_ = true});
  auto fn2 = registry->intern_function(inputs, outputs, {.noexcept_ = true});
  ASSERT_EQ(fn, fn2);
}
