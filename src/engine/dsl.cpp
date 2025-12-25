#include "engine/dsl.hpp"

#include <string_view>
#include <unordered_set>

namespace sr::engine {
namespace {

auto parse_node_port(std::string_view value, std::string_view context)
  -> Expected<std::pair<std::string, std::string>> {
  auto dot = value.find('.');
  if (dot == std::string_view::npos || dot == 0 || dot + 1 >= value.size()) {
    return tl::unexpected(make_error(std::string(context) + ": expected node.port"));
  }
  std::string node(value.substr(0, dot));
  std::string port(value.substr(dot + 1));
  return std::make_pair(std::move(node), std::move(port));
}

auto get_string_field(const Json& obj, std::string_view field, std::string_view context)
  -> Expected<std::string> {
  auto it = obj.find(std::string(field));
  if (it == obj.end() || !it->is_string()) {
    return tl::unexpected(make_error(std::string(context) + ": missing or invalid field '" + std::string(field) + "'"));
  }
  return it->get<std::string>();
}

}  // namespace

auto parse_graph_json(const Json& json) -> Expected<GraphDef> {
  if (!json.is_object()) {
    return tl::unexpected(make_error("graph json must be an object"));
  }

  GraphDef graph;
  if (auto it = json.find("version"); it != json.end()) {
    if (!it->is_number_integer()) {
      return tl::unexpected(make_error("version must be an integer"));
    }
    graph.version = it->get<int>();
  }
  if (auto it = json.find("name"); it != json.end()) {
    if (!it->is_string()) {
      return tl::unexpected(make_error("name must be a string"));
    }
    graph.name = it->get<std::string>();
  }

  auto nodes_it = json.find("nodes");
  if (nodes_it == json.end() || !nodes_it->is_array()) {
    return tl::unexpected(make_error("nodes must be an array"));
  }
  std::unordered_set<std::string> node_ids;
  for (const auto& node_json : *nodes_it) {
    if (!node_json.is_object()) {
      return tl::unexpected(make_error("node entry must be an object"));
    }
    auto id = get_string_field(node_json, "id", "node");
    if (!id) {
      return tl::unexpected(id.error());
    }
    if (!node_ids.insert(*id).second) {
      return tl::unexpected(make_error("duplicate node id: " + *id));
    }
    auto kernel = get_string_field(node_json, "kernel", "node");
    if (!kernel) {
      return tl::unexpected(kernel.error());
    }
    NodeDef node;
    node.id = std::move(*id);
    node.kernel = std::move(*kernel);
    if (auto params_it = node_json.find("params"); params_it != node_json.end()) {
      node.params = *params_it;
    } else {
      node.params = Json::object();
    }
    graph.nodes.push_back(std::move(node));
  }

  auto bindings_it = json.find("bindings");
  if (bindings_it != json.end()) {
    if (!bindings_it->is_array()) {
      return tl::unexpected(make_error("bindings must be an array"));
    }
    for (const auto& binding_json : *bindings_it) {
      if (!binding_json.is_object()) {
        return tl::unexpected(make_error("binding entry must be an object"));
      }
      auto to_field = get_string_field(binding_json, "to", "binding");
      if (!to_field) {
        return tl::unexpected(to_field.error());
      }
      auto to_parts = parse_node_port(*to_field, "binding.to");
      if (!to_parts) {
        return tl::unexpected(to_parts.error());
      }
      auto from_it = binding_json.find("from");
      if (from_it == binding_json.end()) {
        return tl::unexpected(make_error("binding missing 'from'"));
      }

      BindingDef binding;
      binding.to_node = std::move(to_parts->first);
      binding.to_port = std::move(to_parts->second);

      if (from_it->is_string()) {
        auto from_value = from_it->get<std::string>();
        if (from_value.rfind("$req.", 0) == 0) {
          binding.source.kind = BindingKind::Env;
          binding.source.env_key = from_value.substr(5);
          if (binding.source.env_key.empty()) {
            return tl::unexpected(make_error("binding.from env key is empty"));
          }
        } else {
          auto from_parts = parse_node_port(from_value, "binding.from");
          if (!from_parts) {
            return tl::unexpected(from_parts.error());
          }
          binding.source.kind = BindingKind::NodePort;
          binding.source.node = std::move(from_parts->first);
          binding.source.port = std::move(from_parts->second);
        }
      } else {
        binding.source.kind = BindingKind::Const;
        binding.source.const_value = *from_it;
      }

      graph.bindings.push_back(std::move(binding));
    }
  }

  auto outputs_it = json.find("outputs");
  if (outputs_it != json.end()) {
    if (!outputs_it->is_array()) {
      return tl::unexpected(make_error("outputs must be an array"));
    }
    for (const auto& out_json : *outputs_it) {
      if (!out_json.is_object()) {
        return tl::unexpected(make_error("output entry must be an object"));
      }
      auto from_field = get_string_field(out_json, "from", "output");
      if (!from_field) {
        return tl::unexpected(from_field.error());
      }
      auto as_field = get_string_field(out_json, "as", "output");
      if (!as_field) {
        return tl::unexpected(as_field.error());
      }
      auto from_parts = parse_node_port(*from_field, "output.from");
      if (!from_parts) {
        return tl::unexpected(from_parts.error());
      }
      OutputDef output;
      output.from_node = std::move(from_parts->first);
      output.from_port = std::move(from_parts->second);
      output.as = std::move(*as_field);
      graph.outputs.push_back(std::move(output));
    }
  }

  return graph;
}

}  // namespace sr::engine
