#include "engine/dsl.hpp"
#include "common/logging/log.hpp"

#include <algorithm>
#include <format>
#include <initializer_list>
#include <string_view>
#include <unordered_set>

namespace sr::engine {
namespace {

/// ASCII letter check for identifier validation.
constexpr auto is_alpha(char c) -> bool {
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

/// ASCII digit check for identifier validation.
constexpr auto is_digit(char c) -> bool {
  return c >= '0' && c <= '9';
}

/// Valid identifier start: letter or underscore.
constexpr auto is_ident_start(char c) -> bool {
  return is_alpha(c) || c == '_';
}

/// Valid identifier character: letter, digit, or underscore.
constexpr auto is_ident_char(char c) -> bool {
  return is_ident_start(c) || is_digit(c);
}

/// Parse a single identifier from value at pos.
auto parse_identifier(std::string_view value, std::size_t& pos, std::string_view label,
                      std::string_view context) -> Expected<std::string_view> {
  if (pos >= value.size() || !is_ident_start(value[pos])) {
    return tl::unexpected(make_error(std::format("{}: invalid {} in '{}'", context, label, value)));
  }
  std::size_t start = pos;
  pos += 1;
  while (pos < value.size() && is_ident_char(value[pos])) {
    pos += 1;
  }
  return value.substr(start, pos - start);
}

/// Require that the full value is a valid identifier.
auto validate_identifier(std::string_view value, std::string_view label, std::string_view context)
  -> Expected<void> {
  std::size_t pos = 0;
  auto id = parse_identifier(value, pos, label, context);
  if (!id) {
    return tl::unexpected(id.error());
  }
  if (pos != value.size()) {
    return tl::unexpected(make_error(std::format("{}: invalid {} '{}'", context, label, value)));
  }
  return {};
}

/// Reject unknown fields to catch typos early.
auto ensure_allowed_fields(const Json& obj, std::initializer_list<std::string_view> allowed,
                           std::string_view context) -> Expected<void> {
  for (auto it = obj.begin(); it != obj.end(); ++it) {
    std::string_view key = it.key();
    if (std::find(allowed.begin(), allowed.end(), key) == allowed.end()) {
      return tl::unexpected(make_error(std::format("{}: unknown field '{}'", context, key)));
    }
  }
  return {};
}

/// Parse and validate node.port references.
auto parse_node_port(std::string_view value, std::string_view context)
  -> Expected<std::pair<std::string, std::string>> {
  std::size_t pos = 0;
  auto node = parse_identifier(value, pos, "node id", context);
  if (!node) {
    return tl::unexpected(node.error());
  }
  if (pos >= value.size() || value[pos] != '.') {
    return tl::unexpected(make_error(std::format("{}: expected node.port, got '{}'", context, value)));
  }
  pos += 1;
  auto port = parse_identifier(value, pos, "port id", context);
  if (!port) {
    return tl::unexpected(port.error());
  }
  if (pos != value.size()) {
    return tl::unexpected(make_error(std::format("{}: invalid node.port '{}'", context, value)));
  }
  return std::make_pair(std::string(*node), std::string(*port));
}

auto get_string_field(const Json& obj, std::string_view field, std::string_view context)
  -> Expected<std::string> {
  auto it = obj.find(std::string(field));
  if (it == obj.end() || !it->is_string()) {
    return tl::unexpected(make_error(std::format("{}: missing or invalid field '{}'", context, field)));
  }
  return it->get<std::string>();
}

auto get_string_array_field(const Json& obj, std::string_view field, std::string_view context)
  -> Expected<std::vector<std::string>> {
  auto it = obj.find(std::string(field));
  if (it == obj.end()) {
    return std::vector<std::string>{};
  }
  if (!it->is_array()) {
    return tl::unexpected(make_error(std::format("{}: field '{}' must be an array", context, field)));
  }
  std::vector<std::string> result;
  result.reserve(it->size());
  for (std::size_t index = 0; index < it->size(); ++index) {
    const auto& entry = (*it)[index];
    if (!entry.is_string()) {
      return tl::unexpected(
        make_error(std::format("{}: field '{}' entry {} must be a string", context, field, index)));
    }
    result.push_back(entry.get<std::string>());
  }
  return result;
}

auto get_identifier_array_field(const Json& obj, std::string_view field, std::string_view label,
                                std::string_view context) -> Expected<std::vector<std::string>> {
  auto values = get_string_array_field(obj, field, context);
  if (!values) {
    return tl::unexpected(values.error());
  }
  for (std::size_t i = 0; i < values->size(); ++i) {
    auto item_context = std::format("{}.{:s}[{}]", context, field, i);
    if (auto ok = validate_identifier((*values)[i], label, item_context); !ok) {
      return tl::unexpected(ok.error());
    }
  }
  return values;
}

}  // namespace

auto parse_graph_json(const Json& json) -> Expected<GraphDef> {
  sr::log::info("Parsing graph JSON");

  if (!json.is_object()) {
    return tl::unexpected(make_error("graph json must be an object"));
  }
  if (auto ok = ensure_allowed_fields(json, {"version", "name", "nodes", "bindings", "outputs"}, "graph");
      !ok) {
    return tl::unexpected(ok.error());
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
  for (std::size_t index = 0; index < nodes_it->size(); ++index) {
    const auto& node_json = (*nodes_it)[index];
    if (!node_json.is_object()) {
      return tl::unexpected(make_error("node entry must be an object"));
    }
    auto node_context = std::format("nodes[{}]", index);
    if (auto ok = ensure_allowed_fields(node_json, {"id", "kernel", "params", "inputs", "outputs"},
                                        node_context);
        !ok) {
      return tl::unexpected(ok.error());
    }
    auto id = get_string_field(node_json, "id", node_context);
    if (!id) {
      return tl::unexpected(id.error());
    }
    if (auto ok = validate_identifier(*id, "node id", node_context); !ok) {
      return tl::unexpected(ok.error());
    }
    if (!node_ids.insert(*id).second) {
      return tl::unexpected(make_error(std::format("duplicate node id: {}", *id)));
    }
    auto kernel = get_string_field(node_json, "kernel", node_context);
    if (!kernel) {
      return tl::unexpected(kernel.error());
    }
    NodeDef node;
    node.id = std::move(*id);
    node.kernel = std::move(*kernel);
    if (auto params_it = node_json.find("params"); params_it != node_json.end()) {
      if (!params_it->is_object()) {
        return tl::unexpected(make_error(std::format("{}: field 'params' must be an object", node_context)));
      }
      node.params = *params_it;
    } else {
      node.params = Json::object();
    }
    auto inputs = get_identifier_array_field(node_json, "inputs", "input name", node_context);
    if (!inputs) {
      return tl::unexpected(inputs.error());
    }
    node.input_names = std::move(*inputs);
    auto outputs = get_identifier_array_field(node_json, "outputs", "output name", node_context);
    if (!outputs) {
      return tl::unexpected(outputs.error());
    }
    node.output_names = std::move(*outputs);
    graph.nodes.push_back(std::move(node));
  }

  auto bindings_it = json.find("bindings");
  if (bindings_it != json.end()) {
    if (!bindings_it->is_array()) {
      return tl::unexpected(make_error("bindings must be an array"));
    }
    for (std::size_t index = 0; index < bindings_it->size(); ++index) {
      const auto& binding_json = (*bindings_it)[index];
      if (!binding_json.is_object()) {
        return tl::unexpected(make_error("binding entry must be an object"));
      }
      auto binding_context = std::format("bindings[{}]", index);
      if (auto ok = ensure_allowed_fields(binding_json, {"to", "from"}, binding_context); !ok) {
        return tl::unexpected(ok.error());
      }
      auto to_field = get_string_field(binding_json, "to", binding_context);
      if (!to_field) {
        return tl::unexpected(to_field.error());
      }
      auto to_parts = parse_node_port(*to_field, std::format("{}.to", binding_context));
      if (!to_parts) {
        return tl::unexpected(to_parts.error());
      }
      auto from_it = binding_json.find("from");
      if (from_it == binding_json.end()) {
        return tl::unexpected(make_error(std::format("{}: missing field 'from'", binding_context)));
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
            return tl::unexpected(
              make_error(std::format("{}.from: env key is empty", binding_context)));
          }
        } else {
          auto from_parts = parse_node_port(from_value, std::format("{}.from", binding_context));
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
    for (std::size_t index = 0; index < outputs_it->size(); ++index) {
      const auto& out_json = (*outputs_it)[index];
      if (!out_json.is_object()) {
        return tl::unexpected(make_error("output entry must be an object"));
      }
      auto output_context = std::format("outputs[{}]", index);
      if (auto ok = ensure_allowed_fields(out_json, {"from", "as"}, output_context); !ok) {
        return tl::unexpected(ok.error());
      }
      auto from_field = get_string_field(out_json, "from", output_context);
      if (!from_field) {
        return tl::unexpected(from_field.error());
      }
      auto as_field = get_string_field(out_json, "as", output_context);
      if (!as_field) {
        return tl::unexpected(as_field.error());
      }
      auto from_parts = parse_node_port(*from_field, std::format("{}.from", output_context));
      if (!from_parts) {
        return tl::unexpected(from_parts.error());
      }
      if (auto ok = validate_identifier(*as_field, "output name", output_context); !ok) {
        return tl::unexpected(ok.error());
      }
      OutputDef output;
      output.from_node = std::move(from_parts->first);
      output.from_port = std::move(from_parts->second);
      output.as = std::move(*as_field);
      graph.outputs.push_back(std::move(output));
    }
  }

  sr::log::info("Parsed graph completed");
  return graph;
}

}  // namespace sr::engine
