#pragma once

#include <string>
#include <vector>

#include "engine/error.hpp"
#include "engine/types.hpp"

namespace sr::engine {

enum class BindingKind {
  NodePort,
  Env,
  Const,
};

struct BindingSource {
  BindingKind kind = BindingKind::NodePort;
  std::string node;
  std::string port;
  std::string env_key;
  Json const_value;
};

struct NodeDef {
  std::string id;
  std::string kernel;
  Json params;
};

struct BindingDef {
  std::string to_node;
  std::string to_port;
  BindingSource source;
};

struct OutputDef {
  std::string from_node;
  std::string from_port;
  std::string as;
};

struct GraphDef {
  int version = 1;
  std::string name;
  std::vector<NodeDef> nodes;
  std::vector<BindingDef> bindings;
  std::vector<OutputDef> outputs;
};

auto parse_graph_json(const Json& json) -> Expected<GraphDef>;

}  // namespace sr::engine
