#include "engine/plan.hpp"

#include <cstdint>
#include <queue>
#include <string_view>
#include <unordered_set>

namespace sr::engine {
namespace {

struct NodeBuild {
  std::string id;
  KernelHandle kernel;
  Signature signature;
  std::vector<InputBinding> inputs;
  std::vector<bool> input_bound;
  std::vector<int> outputs;
  std::unordered_map<std::string, int> output_port_map;
};

auto find_port_index(const std::vector<PortDesc>& ports, std::string_view name) -> int {
  for (std::size_t i = 0; i < ports.size(); ++i) {
    if (ports[i].name == name) {
      return static_cast<int>(i);
    }
  }
  return -1;
}

auto make_const_slot(const Json& value, entt::meta_type expected) -> Expected<ValueSlot> {
  if (!expected) {
    return tl::unexpected(make_error("const binding expects a registered type"));
  }

  ValueSlot slot;
  auto int_type = entt::resolve<int64_t>();
  auto double_type = entt::resolve<double>();
  auto bool_type = entt::resolve<bool>();
  auto string_type = entt::resolve<std::string>();

  if (expected == int_type) {
    if (!(value.is_number_integer() || value.is_number_unsigned())) {
      return tl::unexpected(make_error("const binding expects int64"));
    }
    slot.set<int64_t>(value.get<int64_t>());
    return slot;
  }

  if (expected == double_type) {
    if (!value.is_number()) {
      return tl::unexpected(make_error("const binding expects double"));
    }
    slot.set<double>(value.get<double>());
    return slot;
  }

  if (expected == bool_type) {
    if (!value.is_boolean()) {
      return tl::unexpected(make_error("const binding expects bool"));
    }
    slot.set<bool>(value.get<bool>());
    return slot;
  }

  if (expected == string_type) {
    if (!value.is_string()) {
      return tl::unexpected(make_error("const binding expects string"));
    }
    slot.set<std::string>(value.get<std::string>());
    return slot;
  }

  return tl::unexpected(make_error("const binding type is not supported"));
}

}  // namespace

auto compile_plan(const GraphDef& graph, const KernelRegistry& registry, const CompileOptions& options)
  -> Expected<ExecPlan> {
  if (graph.nodes.empty()) {
    return tl::unexpected(make_error("graph has no nodes"));
  }

  std::unordered_map<std::string, int> node_index;
  std::vector<NodeBuild> node_builds;
  node_builds.reserve(graph.nodes.size());
  std::vector<SlotSpec> slots;

  for (const auto& node_def : graph.nodes) {
    if (node_index.contains(node_def.id)) {
      return tl::unexpected(make_error("duplicate node id: " + node_def.id));
    }
    auto factory = registry.find(node_def.kernel);
    if (!factory) {
      return tl::unexpected(make_error("kernel not registered: " + node_def.kernel));
    }
    auto kernel = (*factory)(node_def.params);
    if (!kernel) {
      return tl::unexpected(make_error("kernel factory failed: " + node_def.kernel + ": " + kernel.error().message));
    }

    NodeBuild build;
    build.id = node_def.id;
    build.kernel = std::move(*kernel);
    build.signature = build.kernel.signature;

    for (const auto& in_port : build.signature.inputs) {
      if (!in_port.type) {
        return tl::unexpected(make_error("input port type missing for node: " + build.id));
      }
    }

    build.inputs.resize(build.signature.inputs.size());
    build.input_bound.assign(build.signature.inputs.size(), false);
    for (std::size_t i = 0; i < build.signature.inputs.size(); ++i) {
      build.inputs[i].expected_type = build.signature.inputs[i].type;
    }

    for (const auto& out_port : build.signature.outputs) {
      if (!out_port.type) {
        return tl::unexpected(make_error("output port type missing for node: " + build.id));
      }
      int slot_index = static_cast<int>(slots.size());
      slots.push_back(SlotSpec{out_port.type});
      build.outputs.push_back(slot_index);
      build.output_port_map.emplace(out_port.name, slot_index);
    }

    node_index.emplace(node_def.id, static_cast<int>(node_builds.size()));
    node_builds.push_back(std::move(build));
  }

  std::vector<std::vector<int>> edges(node_builds.size());
  std::vector<int> indegree(node_builds.size(), 0);
  std::vector<ValueSlot> const_slots;

  for (const auto& binding : graph.bindings) {
    auto to_it = node_index.find(binding.to_node);
    if (to_it == node_index.end()) {
      return tl::unexpected(make_error("binding to unknown node: " + binding.to_node));
    }
    auto& node = node_builds[to_it->second];
    int input_index = find_port_index(node.signature.inputs, binding.to_port);
    if (input_index < 0) {
      return tl::unexpected(make_error("binding to unknown input port: " + binding.to_node + "." + binding.to_port));
    }
    if (node.input_bound[static_cast<std::size_t>(input_index)]) {
      return tl::unexpected(make_error("input port bound twice: " + binding.to_node + "." + binding.to_port));
    }

    const auto& input_port = node.signature.inputs[static_cast<std::size_t>(input_index)];
    InputBinding input_binding;
    input_binding.expected_type = input_port.type;

    switch (binding.source.kind) {
      case BindingKind::NodePort: {
        auto from_it = node_index.find(binding.source.node);
        if (from_it == node_index.end()) {
          return tl::unexpected(make_error("binding from unknown node: " + binding.source.node));
        }
        auto& from_node = node_builds[from_it->second];
        auto out_it = from_node.output_port_map.find(binding.source.port);
        if (out_it == from_node.output_port_map.end()) {
          return tl::unexpected(
            make_error("binding from unknown output port: " + binding.source.node + "." + binding.source.port));
        }
        int slot_index = out_it->second;
        if (slots[static_cast<std::size_t>(slot_index)].type != input_port.type) {
          return tl::unexpected(make_error("type mismatch for binding: " + binding.to_node + "." + binding.to_port));
        }
        input_binding.kind = InputBindingKind::Slot;
        input_binding.slot_index = slot_index;
        edges[from_it->second].push_back(to_it->second);
        indegree[to_it->second] += 1;
        break;
      }
      case BindingKind::Env: {
        input_binding.kind = InputBindingKind::Env;
        input_binding.env_key = binding.source.env_key;
        if (auto env_it = options.env_types.find(binding.source.env_key); env_it != options.env_types.end()) {
          if (env_it->second != input_port.type) {
            return tl::unexpected(make_error("env type mismatch for binding: " + binding.to_node + "." + binding.to_port));
          }
        }
        break;
      }
      case BindingKind::Const: {
        auto slot = make_const_slot(binding.source.const_value, input_port.type);
        if (!slot) {
          return tl::unexpected(slot.error());
        }
        int const_index = static_cast<int>(const_slots.size());
        const_slots.push_back(std::move(*slot));
        input_binding.kind = InputBindingKind::Const;
        input_binding.const_index = const_index;
        break;
      }
    }

    node.inputs[static_cast<std::size_t>(input_index)] = std::move(input_binding);
    node.input_bound[static_cast<std::size_t>(input_index)] = true;
  }

  for (auto& node : node_builds) {
    for (std::size_t i = 0; i < node.signature.inputs.size(); ++i) {
      if (node.input_bound[i]) {
        continue;
      }
      if (node.signature.inputs[i].required) {
        return tl::unexpected(make_error("missing required input: " + node.id + "." + node.signature.inputs[i].name));
      }
      InputBinding missing_binding;
      missing_binding.kind = InputBindingKind::Missing;
      missing_binding.expected_type = node.signature.inputs[i].type;
      node.inputs[i] = std::move(missing_binding);
      node.input_bound[i] = true;
    }
  }

  std::queue<int> ready;
  for (std::size_t i = 0; i < indegree.size(); ++i) {
    if (indegree[i] == 0) {
      ready.push(static_cast<int>(i));
    }
  }

  std::vector<int> topo;
  topo.reserve(node_builds.size());
  while (!ready.empty()) {
    int node = ready.front();
    ready.pop();
    topo.push_back(node);
    for (int next : edges[static_cast<std::size_t>(node)]) {
      indegree[static_cast<std::size_t>(next)] -= 1;
      if (indegree[static_cast<std::size_t>(next)] == 0) {
        ready.push(next);
      }
    }
  }

  if (topo.size() != node_builds.size()) {
    return tl::unexpected(make_error("graph has cycles"));
  }

  ExecPlan plan;
  plan.slots = std::move(slots);
  plan.const_slots = std::move(const_slots);
  plan.topo_order = std::move(topo);

  plan.nodes.reserve(node_builds.size());
  for (auto& build : node_builds) {
    CompiledNode node;
    node.id = std::move(build.id);
    node.kernel = std::move(build.kernel);
    node.inputs = std::move(build.inputs);
    node.outputs = std::move(build.outputs);
    plan.nodes.push_back(std::move(node));
  }

  plan.slot_producer.assign(plan.slots.size(), -1);
  for (std::size_t node_index = 0; node_index < plan.nodes.size(); ++node_index) {
    const auto& node = plan.nodes[node_index];
    for (int slot_index : node.outputs) {
      plan.slot_producer[static_cast<std::size_t>(slot_index)] = static_cast<int>(node_index);
    }
  }

  plan.dependents.assign(plan.nodes.size(), {});
  plan.pending_counts.assign(plan.nodes.size(), 0);
  std::vector<int> seen(plan.nodes.size(), -1);
  int stamp = 0;
  for (std::size_t node_index = 0; node_index < plan.nodes.size(); ++node_index) {
    stamp += 1;
    int count = 0;
    const auto& node = plan.nodes[node_index];
    for (const auto& binding : node.inputs) {
      if (binding.kind != InputBindingKind::Slot) {
        continue;
      }
      int producer = plan.slot_producer[static_cast<std::size_t>(binding.slot_index)];
      if (producer < 0) {
        return tl::unexpected(make_error("missing slot producer"));
      }
      if (producer == static_cast<int>(node_index)) {
        continue;
      }
      if (seen[static_cast<std::size_t>(producer)] == stamp) {
        continue;
      }
      seen[static_cast<std::size_t>(producer)] = stamp;
      plan.dependents[static_cast<std::size_t>(producer)].push_back(static_cast<int>(node_index));
      count += 1;
    }
    plan.pending_counts[node_index] = count;
  }

  for (const auto& output : graph.outputs) {
    auto node_it = node_index.find(output.from_node);
    if (node_it == node_index.end()) {
      return tl::unexpected(make_error("output from unknown node: " + output.from_node));
    }
    auto& node = node_builds[static_cast<std::size_t>(node_it->second)];
    auto out_it = node.output_port_map.find(output.from_port);
    if (out_it == node.output_port_map.end()) {
      return tl::unexpected(
        make_error("output from unknown port: " + output.from_node + "." + output.from_port));
    }
    if (plan.output_slots.contains(output.as)) {
      return tl::unexpected(make_error("duplicate output name: " + output.as));
    }
    plan.output_slots.emplace(output.as, out_it->second);
  }

  return plan;
}

}  // namespace sr::engine
