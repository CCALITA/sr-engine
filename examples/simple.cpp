#include <cstdint>
#include <exception>
#include <iostream>

#include "engine/dsl.hpp"
#include "engine/plan.hpp"
#include "kernel/sample_kernels.hpp"
#include "runtime/executor.hpp"

int main() {
  sr::kernel::register_builtin_types();

  sr::engine::KernelRegistry registry;
  sr::kernel::register_sample_kernels(registry);

  const char* dsl = R"JSON(
  {
    "version": 1,
    "name": "demo",
    "nodes": [
      { "id": "sum", "kernel": "add", "inputs": ["a", "b"], "outputs": ["sum"] },
      { "id": "scale", "kernel": "mul", "params": { "factor": 10 }, "inputs": ["value"], "outputs": ["product"] },
      { "id": "fmt", "kernel": "format", "params": { "prefix": "result=" }, "inputs": ["value"], "outputs": ["text"] }
    ],
    "bindings": [
      { "to": "sum.a", "from": "$req.x" },
      { "to": "sum.b", "from": 5 },
      { "to": "scale.value", "from": "sum.sum" },
      { "to": "fmt.value", "from": "scale.product" }
    ],
    "outputs": [
      { "from": "fmt.text", "as": "output" }
    ]
  }
  )JSON";

  sr::engine::Json json;
  try {
    json = sr::engine::Json::parse(dsl);
  } catch (const std::exception& ex) {
    std::cerr << "Failed to parse DSL: " << ex.what() << "\n";
    return 1;
  }

  auto graph = sr::engine::parse_graph_json(json);
  if (!graph) {
    std::cerr << "Parse error: " << graph.error().message << "\n";
    return 1;
  }

  auto plan = sr::engine::compile_plan(*graph, registry);
  if (!plan) {
    std::cerr << "Compile error: " << plan.error().message << "\n";
    return 1;
  }

  sr::engine::RequestContext ctx;
  ctx.set_env<int64_t>("x", 7);

  sr::engine::Executor executor;
  auto result = executor.run(*plan, ctx);
  if (!result) {
    std::cerr << "Run error: " << result.error().message << "\n";
    return 1;
  }

  const auto& output = result->outputs.at("output").get<std::string>();
  std::cout << output << "\n";

  return 0;
}
