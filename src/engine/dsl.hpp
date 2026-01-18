#pragma once

#include <string>
#include <vector>

#include "engine/error.hpp"
#include "engine/types.hpp"
#include "engine/version.hpp"

namespace sr::engine {

/// Input binding source kinds from the DSL.
enum class BindingKind {
  NodePort,
  Env,
  Const,
};

/// Source description for an input binding.
struct BindingSource {
  BindingKind kind = BindingKind::NodePort;
  std::string node;
  std::string port;
  std::string env_key;
  Json const_value;
};

/// Node definition in a graph DSL.
struct NodeDef {
  std::string id;
  std::string kernel;
  Json params;
  std::vector<std::string> input_names;
  std::vector<std::string> output_names;
};

/// Binding from a source into a node input port.
struct BindingDef {
  std::string to_node;
  std::string to_port;
  BindingSource source;
};

/// Graph output mapping (node port -> output name).
struct OutputDef {
  std::string from_node;
  std::string from_port;
  std::string as;
};

/// Parsed graph definition used for compilation.
struct GraphDef {
  Version version{1, 0, 0};
  std::string name;
  std::vector<NodeDef> nodes;
  std::vector<BindingDef> bindings;
  std::vector<OutputDef> outputs;
};

/// Parse a JSON DSL object into a graph definition.
auto parse_graph_json(const Json& json) -> Expected<GraphDef>;

}  // namespace sr::engine
