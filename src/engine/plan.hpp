#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "engine/dsl.hpp"
#include "engine/error.hpp"
#include "engine/registry.hpp"
#include "engine/types.hpp"

namespace sr::engine {

enum class InputBindingKind {
  Slot,
  Env,
  Const,
  Missing,
};

struct InputBinding {
  InputBindingKind kind = InputBindingKind::Slot;
  int slot_index = -1;
  int const_index = -1;
  std::string env_key;
  entt::meta_type expected_type{};
};

struct CompiledNode {
  std::string id;
  KernelHandle kernel;
  std::vector<InputBinding> inputs;
  std::vector<int> outputs;
};

struct SlotSpec {
  entt::meta_type type{};
};

struct ExecPlan {
  std::vector<CompiledNode> nodes;
  std::vector<int> topo_order;
  std::vector<SlotSpec> slots;
  std::vector<ValueSlot> const_slots;
  std::vector<int> slot_producer;
  std::vector<std::vector<int>> dependents;
  std::vector<int> pending_counts;
  std::unordered_map<std::string, int> output_slots;
};

struct CompileOptions {
  std::unordered_map<std::string, entt::meta_type> env_types;
};

auto compile_plan(const GraphDef& graph, const KernelRegistry& registry, const CompileOptions& options = {})
  -> Expected<ExecPlan>;

}  // namespace sr::engine
