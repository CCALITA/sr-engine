#include <cstdint>
#include <exception>
#include <format>
#include <iostream>
#include <mutex>

#include "engine/dsl.hpp"
#include "engine/plan.hpp"
#include "engine/trace.hpp"
#include "kernel/sample_kernels.hpp"
#include "runtime/executor.hpp"

namespace {

struct StdoutTraceSink {
  std::mutex mutex;

  void on_run_start(const sr::engine::trace::RunStart& event) {
    std::lock_guard<std::mutex> lock(mutex);
    std::cout << std::format("[trace] run_start trace_id={} plan={}\n", event.trace_id, event.plan_name);
  }

  void on_run_end(const sr::engine::trace::RunEnd& event) {
    std::lock_guard<std::mutex> lock(mutex);
    std::cout << std::format("[trace] run_end trace_id={} status={} duration_ns={}\n",
                             event.trace_id,
                             static_cast<int>(event.status),
                             event.duration);
  }

  void on_node_end(const sr::engine::trace::NodeEnd& event) {
    std::lock_guard<std::mutex> lock(mutex);
    std::cout << std::format("[trace] node_end node={} status={} duration_ns={}\n",
                             event.node_id,
                             static_cast<int>(event.status),
                             event.duration);
  }
};

}  // namespace

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
  StdoutTraceSink trace_sink;
  ctx.trace.sink = sr::engine::trace::make_sink(trace_sink);
  ctx.trace.flags = sr::engine::trace::to_flags(sr::engine::trace::TraceFlag::RunSpan) |
                    sr::engine::trace::to_flags(sr::engine::trace::TraceFlag::NodeSpan);
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
