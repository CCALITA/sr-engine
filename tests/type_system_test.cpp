#include "engine/type_system.hpp"
#include "engine/type_abi.h"
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

TEST(TypeSystem, ArrowSchemaStableId) {
  auto registry = sr::engine::TypeRegistry::create();
  auto i64 = registry->intern_primitive("i64");
  sr::engine::TypeRegistry::ArrowField id{"id", i64, false};
  sr::engine::TypeRegistry::ArrowField score{"score", i64, true};
  std::vector<sr::engine::TypeRegistry::ArrowField> fields = {id, score};
  auto schema = registry->intern_arrow_schema(fields);
  auto schema2 = registry->intern_arrow_schema(fields);
  ASSERT_EQ(schema, schema2);
}

TEST(TypeSystem, PluginTypeStableId) {
  auto registry = sr::engine::TypeRegistry::create();
  sr_type_descriptor desc{
    .kind = SR_TYPE_PLUGIN,
    .name = "my.plugin.type",
    .version = 1,
    .layout_size = 64,
    .layout_align = 8,
  };
  auto id1 = registry->intern_plugin(desc);
  auto id2 = registry->intern_plugin(desc);
  ASSERT_EQ(id1, id2);
}
