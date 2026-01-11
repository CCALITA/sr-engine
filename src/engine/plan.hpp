#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "engine/dsl.hpp"
#include "engine/error.hpp"
#include "engine/registry.hpp"
#include "engine/types.hpp"

namespace sr::engine {

/// Internal binding source type for compiled inputs.
enum class InputBindingKind {
  Slot,
  Env,
  Const,
};

/// Resolved input binding for a compiled node.
struct InputBinding {
  InputBindingKind kind = InputBindingKind::Slot;
  int slot_index = -1;
  int const_index = -1;
  int env_index = -1;
};

/// Compiled node with resolved kernel and port bindings.
struct CompiledNode {
  std::string id;
  KernelHandle kernel;
  std::vector<InputBinding> inputs;
  std::vector<int> outputs;
};

/// Slot type description for execution storage.
struct SlotSpec {
  entt::meta_type type{};
};

/// Required env entry for plan execution.
struct EnvRequirement {
  std::string key;
  entt::meta_type type{};
};

/// Named graph output slot.
struct OutputSlot {
  std::string name;
  NameId name_id{};
  int slot_index = -1;
};

/// Compiled execution plan for a graph.
struct ExecPlan {
  std::string name;
  std::vector<CompiledNode> nodes;
  std::vector<int> topo_order;
  std::vector<SlotSpec> slots;
  std::vector<ValueBox> const_slots;
  std::vector<EnvRequirement> env_requirements;
  std::vector<int> slot_producer;
  std::vector<std::vector<int>> dependents;
  std::vector<int> pending_counts;
  std::vector<int> initial_ready;
  std::vector<OutputSlot> outputs;
};

/// Compile a parsed graph against a kernel registry.
auto compile_plan(const GraphDef& graph, const KernelRegistry& registry) -> Expected<ExecPlan>;

}  // namespace sr::engine
