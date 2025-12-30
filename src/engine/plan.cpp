#include "engine/plan.hpp"

#include <cstdint>
#include <format>
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
  std::unordered_map<std::string, int> input_port_map;
  std::vector<int> outputs;
  std::unordered_map<std::string, int> output_port_map;
};

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

auto apply_port_names(std::vector<PortDesc>& ports, const std::vector<std::string>& names,
                      std::string_view node_id, std::string_view label) -> Expected<void> {
  if (!names.empty()) {
    if (names.size() != ports.size()) {
      return tl::unexpected(make_error(std::format("{} name count mismatch for node: {}", label, node_id)));
    }
    for (std::size_t i = 0; i < ports.size(); ++i) {
      ports[i].name = names[i];
    }
  }

  std::unordered_set<std::string> seen;
  seen.reserve(ports.size());
  for (const auto& port : ports) {
    if (port.name.empty()) {
      return tl::unexpected(make_error(std::format("missing {} names for node: {}", label, node_id)));
    }
    if (!seen.insert(port.name).second) {
      return tl::unexpected(make_error(std::format("duplicate {} name for node: {}", label, node_id)));
    }
  }
  return {};
}

struct PlanBuilder {
  const GraphDef& graph;
  const KernelRegistry& registry;

  std::unordered_map<std::string, int> node_index;
  std::vector<NodeBuild> node_builds;
  std::vector<SlotSpec> slots;
  std::vector<std::vector<int>> edges;
  std::vector<int> indegree;
  std::vector<ValueSlot> const_slots;
  std::unordered_map<std::string, int> env_index;
  std::vector<EnvRequirement> env_requirements;

  auto build_nodes() -> Expected<void> {
    node_index.reserve(graph.nodes.size());
    node_builds.reserve(graph.nodes.size());

    for (const auto& node_def : graph.nodes) {
      if (node_index.contains(node_def.id)) {
        return tl::unexpected(make_error(std::format("duplicate node id: {}", node_def.id)));
      }
      auto factory = registry.find(node_def.kernel);
      if (!factory) {
        return tl::unexpected(make_error(std::format("kernel not registered: {}", node_def.kernel)));
      }
      auto kernel = (*factory)(node_def.params);
      if (!kernel) {
        return tl::unexpected(
          make_error(std::format("kernel factory failed: {}: {}", node_def.kernel, kernel.error().message)));
      }

      NodeBuild build;
      build.id = node_def.id;
      build.kernel = std::move(*kernel);
      build.signature = build.kernel.signature;

      if (auto name_result =
            apply_port_names(build.signature.inputs, node_def.input_names, build.id, "input");
          !name_result) {
        return tl::unexpected(name_result.error());
      }
      if (auto name_result =
            apply_port_names(build.signature.outputs, node_def.output_names, build.id, "output");
          !name_result) {
        return tl::unexpected(name_result.error());
      }

      build.inputs.resize(build.signature.inputs.size());
      build.input_bound.assign(build.signature.inputs.size(), false);
      build.input_port_map.reserve(build.signature.inputs.size());
      for (std::size_t i = 0; i < build.signature.inputs.size(); ++i) {
        const auto& in_port = build.signature.inputs[i];
        if (!in_port.type) {
          return tl::unexpected(make_error(std::format("input port type missing for node: {}", build.id)));
        }
        build.inputs[i].expected_type = in_port.type;
        build.input_port_map.emplace(in_port.name, static_cast<int>(i));
      }

      for (const auto& out_port : build.signature.outputs) {
        if (!out_port.type) {
          return tl::unexpected(make_error(std::format("output port type missing for node: {}", build.id)));
        }
        int slot_index = static_cast<int>(slots.size());
        slots.push_back(SlotSpec{out_port.type});
        build.outputs.push_back(slot_index);
        build.output_port_map.emplace(out_port.name, slot_index);
      }

      node_index.emplace(node_def.id, static_cast<int>(node_builds.size()));
      node_builds.push_back(std::move(build));
    }

    edges.assign(node_builds.size(), {});
    indegree.assign(node_builds.size(), 0);
    return {};
  }

  auto bind_inputs() -> Expected<void> {
    for (const auto& binding : graph.bindings) {
      auto to_it = node_index.find(binding.to_node);
      if (to_it == node_index.end()) {
        return tl::unexpected(make_error(std::format("binding to unknown node: {}", binding.to_node)));
      }
      auto& node = node_builds[to_it->second];
      auto input_it = node.input_port_map.find(binding.to_port);
      if (input_it == node.input_port_map.end()) {
        return tl::unexpected(
          make_error(std::format("binding to unknown input port: {}.{}", binding.to_node, binding.to_port)));
      }
      int input_index = input_it->second;
      if (node.input_bound[static_cast<std::size_t>(input_index)]) {
        return tl::unexpected(make_error(
          std::format("input port bound twice: {}.{}", binding.to_node, binding.to_port)));
      }

      const auto& input_port = node.signature.inputs[static_cast<std::size_t>(input_index)];
      InputBinding input_binding;
      input_binding.expected_type = input_port.type;

      switch (binding.source.kind) {
        case BindingKind::NodePort: {
          auto from_it = node_index.find(binding.source.node);
          if (from_it == node_index.end()) {
            return tl::unexpected(make_error(std::format("binding from unknown node: {}", binding.source.node)));
          }
          auto& from_node = node_builds[from_it->second];
          auto out_it = from_node.output_port_map.find(binding.source.port);
          if (out_it == from_node.output_port_map.end()) {
            return tl::unexpected(make_error(
              std::format("binding from unknown output port: {}.{}", binding.source.node, binding.source.port)));
          }
          int slot_index = out_it->second;
          if (slots[static_cast<std::size_t>(slot_index)].type != input_port.type) {
            return tl::unexpected(
              make_error(std::format("type mismatch for binding: {}.{}", binding.to_node, binding.to_port)));
          }
          input_binding.kind = InputBindingKind::Slot;
          input_binding.slot_index = slot_index;
          edges[from_it->second].push_back(to_it->second);
          indegree[to_it->second] += 1;
          break;
        }
        case BindingKind::Env: {
          input_binding.kind = InputBindingKind::Env;
          auto env_it = env_index.find(binding.source.env_key);
          int index = -1;
          if (env_it == env_index.end()) {
            index = static_cast<int>(env_requirements.size());
            env_index.emplace(binding.source.env_key, index);
            env_requirements.push_back(EnvRequirement{binding.source.env_key, input_port.type});
          } else {
            index = env_it->second;
            if (env_requirements[static_cast<std::size_t>(index)].type != input_port.type) {
              return tl::unexpected(
                make_error(std::format("env type mismatch for binding: {}.{}", binding.to_node, binding.to_port)));
            }
          }
          input_binding.env_index = index;
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
    return {};
  }

  auto fill_missing_inputs() -> Expected<void> {
    for (auto& node : node_builds) {
      for (std::size_t i = 0; i < node.signature.inputs.size(); ++i) {
        if (node.input_bound[i]) {
          continue;
        }
        if (node.signature.inputs[i].required) {
          return tl::unexpected(
            make_error(std::format("missing required input: {}.{}", node.id, node.signature.inputs[i].name)));
        }
        InputBinding missing_binding;
        missing_binding.kind = InputBindingKind::Missing;
        missing_binding.expected_type = node.signature.inputs[i].type;
        node.inputs[i] = std::move(missing_binding);
        node.input_bound[i] = true;
      }
    }
    return {};
  }

  auto topo_sort() -> Expected<std::vector<int>> {
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
    return topo;
  }

  auto bind_outputs(ExecPlan& plan) -> Expected<void> {
    for (const auto& output : graph.outputs) {
      auto node_it = node_index.find(output.from_node);
      if (node_it == node_index.end()) {
        return tl::unexpected(make_error(std::format("output from unknown node: {}", output.from_node)));
      }
      auto& node = node_builds[static_cast<std::size_t>(node_it->second)];
      auto out_it = node.output_port_map.find(output.from_port);
      if (out_it == node.output_port_map.end()) {
        return tl::unexpected(
          make_error(std::format("output from unknown port: {}.{}", output.from_node, output.from_port)));
      }
      if (plan.output_slots.contains(output.as)) {
        return tl::unexpected(make_error(std::format("duplicate output name: {}", output.as)));
      }
      plan.output_slots.emplace(output.as, out_it->second);
    }
    return {};
  }

  auto build_dependents(ExecPlan& plan) -> Expected<void> {
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
    return {};
  }

  auto build_plan(std::vector<int> topo) -> Expected<ExecPlan> {
    ExecPlan plan;
    plan.slots = std::move(slots);
    plan.const_slots = std::move(const_slots);
    plan.topo_order = std::move(topo);
    plan.env_requirements = std::move(env_requirements);

    if (auto outputs_result = bind_outputs(plan); !outputs_result) {
      return tl::unexpected(outputs_result.error());
    }

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

    if (auto dependents_result = build_dependents(plan); !dependents_result) {
      return tl::unexpected(dependents_result.error());
    }

    return plan;
  }
};

}  // namespace

auto compile_plan(const GraphDef& graph, const KernelRegistry& registry) -> Expected<ExecPlan> {
  if (graph.nodes.empty()) {
    return tl::unexpected(make_error("graph has no nodes"));
  }

  PlanBuilder builder{graph, registry};
  if (auto result = builder.build_nodes(); !result) {
    return tl::unexpected(result.error());
  }
  if (auto result = builder.bind_inputs(); !result) {
    return tl::unexpected(result.error());
  }
  if (auto result = builder.fill_missing_inputs(); !result) {
    return tl::unexpected(result.error());
  }
  auto topo = builder.topo_sort();
  if (!topo) {
    return tl::unexpected(topo.error());
  }
  return builder.build_plan(std::move(*topo));
}

}  // namespace sr::engine
