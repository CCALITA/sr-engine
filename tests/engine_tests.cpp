#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "engine/dsl.hpp"
#include "engine/plan.hpp"
#include "engine/types.hpp"
#include "kernel/sample_kernels.hpp"
#include "runtime/executor.hpp"

namespace {

struct TestStats {
  int passed = 0;
  int failed = 0;
};

auto parse_graph(const char* dsl, sr::engine::GraphDef& out, std::string& error) -> bool {
  try {
    auto json = sr::engine::Json::parse(dsl);
    auto graph = sr::engine::parse_graph_json(json);
    if (!graph) {
      error = graph.error().message;
      return false;
    }
    out = std::move(*graph);
    return true;
  } catch (const std::exception& ex) {
    error = ex.what();
    return false;
  }
}

auto make_registry() -> sr::engine::KernelRegistry {
  sr::engine::KernelRegistry registry;
  sr::kernel::register_sample_kernels(registry);
  return registry;
}

auto run_test(const char* name, const std::function<bool()>& test, TestStats& stats) -> void {
  if (test()) {
    std::cout << "[PASS] " << name << "\n";
    stats.passed += 1;
  } else {
    std::cout << "[FAIL] " << name << "\n";
    stats.failed += 1;
  }
}

auto test_basic_pipeline() -> bool {
  const char* dsl = R"JSON(
  {
    "version": 1,
    "name": "basic",
    "nodes": [
      { "id": "a", "kernel": "const_i64", "params": { "value": 7 }, "inputs": [], "outputs": ["value"] },
      { "id": "b", "kernel": "const_i64", "params": { "value": 5 }, "inputs": [], "outputs": ["value"] },
      { "id": "sum", "kernel": "add", "inputs": ["a", "b"], "outputs": ["sum"] },
      { "id": "scale", "kernel": "mul", "params": { "factor": 10 }, "inputs": ["value"], "outputs": ["product"] },
      { "id": "fmt", "kernel": "format", "params": { "prefix": "sum=" }, "inputs": ["value"], "outputs": ["text"] }
    ],
    "bindings": [
      { "to": "sum.a", "from": "a.value" },
      { "to": "sum.b", "from": "b.value" },
      { "to": "scale.value", "from": "sum.sum" },
      { "to": "fmt.value", "from": "scale.product" }
    ],
    "outputs": [
      { "from": "fmt.text", "as": "out" }
    ]
  }
  )JSON";

  sr::engine::GraphDef graph;
  std::string error;
  if (!parse_graph(dsl, graph, error)) {
    std::cerr << "parse error: " << error << "\n";
    return false;
  }

  auto registry = make_registry();
  auto plan = sr::engine::compile_plan(graph, registry);
  if (!plan) {
    std::cerr << "compile error: " << plan.error().message << "\n";
    return false;
  }

  sr::engine::Executor executor;
  sr::engine::RequestContext ctx;
  auto result = executor.run(*plan, ctx);
  if (!result) {
    std::cerr << "run error: " << result.error().message << "\n";
    return false;
  }

  auto it = result->outputs.find("out");
  if (it == result->outputs.end()) {
    std::cerr << "missing output\n";
    return false;
  }

  const auto& text = it->second.get<std::string>();
  if (text != "sum=120") {
    std::cerr << "unexpected output: " << text << "\n";
    return false;
  }
  return true;
}

auto test_optional_input_coalesce() -> bool {
  const char* dsl = R"JSON(
  {
    "version": 1,
    "name": "coalesce",
    "nodes": [
      { "id": "fallback", "kernel": "const_i64", "params": { "value": 42 }, "inputs": [], "outputs": ["value"] },
      { "id": "pick", "kernel": "coalesce_i64", "inputs": ["value", "fallback"], "outputs": ["value"] }
    ],
    "bindings": [
      { "to": "pick.fallback", "from": "fallback.value" }
    ],
    "outputs": [
      { "from": "pick.value", "as": "out" }
    ]
  }
  )JSON";

  sr::engine::GraphDef graph;
  std::string error;
  if (!parse_graph(dsl, graph, error)) {
    std::cerr << "parse error: " << error << "\n";
    return false;
  }

  auto registry = make_registry();
  auto plan = sr::engine::compile_plan(graph, registry);
  if (!plan) {
    std::cerr << "compile error: " << plan.error().message << "\n";
    return false;
  }

  sr::engine::Executor executor;
  sr::engine::RequestContext ctx;
  auto result = executor.run(*plan, ctx);
  if (!result) {
    std::cerr << "run error: " << result.error().message << "\n";
    return false;
  }

  const auto& value = result->outputs.at("out").get<int64_t>();
  return value == 42;
}

auto test_env_binding() -> bool {
  const char* dsl = R"JSON(
  {
    "version": 1,
    "name": "env",
    "nodes": [
      { "id": "sum", "kernel": "add", "inputs": ["a", "b"], "outputs": ["sum"] }
    ],
    "bindings": [
      { "to": "sum.a", "from": "$req.x" },
      { "to": "sum.b", "from": 5 }
    ],
    "outputs": [
      { "from": "sum.sum", "as": "out" }
    ]
  }
  )JSON";

  sr::engine::GraphDef graph;
  std::string error;
  if (!parse_graph(dsl, graph, error)) {
    std::cerr << "parse error: " << error << "\n";
    return false;
  }

  auto registry = make_registry();
  auto plan = sr::engine::compile_plan(graph, registry);
  if (!plan) {
    std::cerr << "compile error: " << plan.error().message << "\n";
    return false;
  }

  sr::engine::Executor executor;
  sr::engine::RequestContext ctx;
  ctx.set_env<int64_t>("x", 8);
  auto result = executor.run(*plan, ctx);
  if (!result) {
    std::cerr << "run error: " << result.error().message << "\n";
    return false;
  }

  const auto& value = result->outputs.at("out").get<int64_t>();
  return value == 13;
}

auto test_missing_required_input() -> bool {
  const char* dsl = R"JSON(
  {
    "version": 1,
    "name": "missing_input",
    "nodes": [
      { "id": "a", "kernel": "const_i64", "params": { "value": 1 }, "inputs": [], "outputs": ["value"] },
      { "id": "sum", "kernel": "add", "inputs": ["a", "b"], "outputs": ["sum"] }
    ],
    "bindings": [
      { "to": "sum.a", "from": "a.value" }
    ],
    "outputs": [
      { "from": "sum.sum", "as": "out" }
    ]
  }
  )JSON";

  sr::engine::GraphDef graph;
  std::string error;
  if (!parse_graph(dsl, graph, error)) {
    return false;
  }

  auto registry = make_registry();
  auto plan = sr::engine::compile_plan(graph, registry);
  return !plan;
}

auto test_type_mismatch() -> bool {
  const char* dsl = R"JSON(
  {
    "version": 1,
    "name": "type_mismatch",
    "nodes": [
      { "id": "text", "kernel": "const_str", "params": { "value": "oops" }, "inputs": [], "outputs": ["value"] },
      { "id": "sum", "kernel": "add", "inputs": ["a", "b"], "outputs": ["sum"] }
    ],
    "bindings": [
      { "to": "sum.a", "from": "text.value" },
      { "to": "sum.b", "from": 1 }
    ],
    "outputs": [
      { "from": "sum.sum", "as": "out" }
    ]
  }
  )JSON";

  sr::engine::GraphDef graph;
  std::string error;
  if (!parse_graph(dsl, graph, error)) {
    return false;
  }

  auto registry = make_registry();
  auto plan = sr::engine::compile_plan(graph, registry);
  return !plan;
}

auto test_cycle_detection() -> bool {
  const char* dsl = R"JSON(
  {
    "version": 1,
    "name": "cycle",
    "nodes": [
      { "id": "a", "kernel": "identity_i64", "inputs": ["value"], "outputs": ["value"] },
      { "id": "b", "kernel": "identity_i64", "inputs": ["value"], "outputs": ["value"] }
    ],
    "bindings": [
      { "to": "a.value", "from": "b.value" },
      { "to": "b.value", "from": "a.value" }
    ],
    "outputs": [
      { "from": "a.value", "as": "out" }
    ]
  }
  )JSON";

  sr::engine::GraphDef graph;
  std::string error;
  if (!parse_graph(dsl, graph, error)) {
    return false;
  }

  auto registry = make_registry();
  auto plan = sr::engine::compile_plan(graph, registry);
  return !plan;
}

auto test_duplicate_output_name() -> bool {
  const char* dsl = R"JSON(
  {
    "version": 1,
    "name": "dup_output",
    "nodes": [
      { "id": "a", "kernel": "const_i64", "params": { "value": 1 }, "inputs": [], "outputs": ["value"] }
    ],
    "outputs": [
      { "from": "a.value", "as": "dup" },
      { "from": "a.value", "as": "dup" }
    ]
  }
  )JSON";

  sr::engine::GraphDef graph;
  std::string error;
  if (!parse_graph(dsl, graph, error)) {
    return false;
  }

  auto registry = make_registry();
  auto plan = sr::engine::compile_plan(graph, registry);
  return !plan;
}

auto test_env_type_mismatch() -> bool {
  const char* dsl = R"JSON(
  {
    "version": 1,
    "name": "env_type_mismatch",
    "nodes": [
      { "id": "sum", "kernel": "add", "inputs": ["a", "b"], "outputs": ["sum"] }
    ],
    "bindings": [
      { "to": "sum.a", "from": "$req.x" },
      { "to": "sum.b", "from": 1 }
    ],
    "outputs": [
      { "from": "sum.sum", "as": "out" }
    ]
  }
  )JSON";

  sr::engine::GraphDef graph;
  std::string error;
  if (!parse_graph(dsl, graph, error)) {
    return false;
  }

  auto registry = make_registry();
  auto plan = sr::engine::compile_plan(graph, registry);
  if (!plan) {
    return false;
  }

  sr::engine::Executor executor;
  sr::engine::RequestContext ctx;
  ctx.set_env<std::string>("x", "bad-type");
  auto result = executor.run(*plan, ctx);
  return !result;
}

auto test_dynamic_port_names() -> bool {
  const char* dsl = R"JSON(
  {
    "version": 1,
    "name": "dynamic_ports",
    "nodes": [
      { "id": "sum", "kernel": "sum_dyn", "inputs": ["x", "y"], "outputs": ["z"] }
    ],
    "bindings": [
      { "to": "sum.x", "from": 4 },
      { "to": "sum.y", "from": 5 }
    ],
    "outputs": [
      { "from": "sum.z", "as": "out" }
    ]
  }
  )JSON";

  sr::engine::GraphDef graph;
  std::string error;
  if (!parse_graph(dsl, graph, error)) {
    std::cerr << "parse error: " << error << "\n";
    return false;
  }

  sr::engine::KernelRegistry registry;
  registry.register_kernel("sum_dyn", [](int64_t x, int64_t y) { return x + y; });
  auto plan = sr::engine::compile_plan(graph, registry);
  if (!plan) {
    std::cerr << "compile error: " << plan.error().message << "\n";
    return false;
  }

  sr::engine::Executor executor;
  sr::engine::RequestContext ctx;
  auto result = executor.run(*plan, ctx);
  if (!result) {
    std::cerr << "run error: " << result.error().message << "\n";
    return false;
  }

  const auto& value = result->outputs.at("out").get<int64_t>();
  return value == 9;
}

auto test_dynamic_ports_missing_names() -> bool {
  const char* dsl = R"JSON(
  {
    "version": 1,
    "name": "dynamic_ports_missing",
    "nodes": [
      { "id": "sum", "kernel": "sum_dyn" }
    ],
    "bindings": [
      { "to": "sum.a", "from": 1 },
      { "to": "sum.b", "from": 2 }
    ],
    "outputs": [
      { "from": "sum.sum", "as": "out" }
    ]
  }
  )JSON";

  sr::engine::GraphDef graph;
  std::string error;
  if (!parse_graph(dsl, graph, error)) {
    std::cerr << "parse error: " << error << "\n";
    return false;
  }

  sr::engine::KernelRegistry registry;
  registry.register_kernel("sum_dyn", [](int64_t x, int64_t y) { return x + y; });
  auto plan = sr::engine::compile_plan(graph, registry);
  return !plan;
}

auto test_dataflow_fanout_join() -> bool {
  const char* dsl = R"JSON(
  {
    "version": 1,
    "name": "dataflow_fanout_join",
    "nodes": [
      { "id": "src", "kernel": "const_i64", "params": { "value": 7 }, "inputs": [], "outputs": ["value"] },
      { "id": "fan", "kernel": "fanout_i64", "inputs": ["value"], "outputs": ["left", "right"] },
      { "id": "left", "kernel": "mul", "params": { "factor": 2 }, "inputs": ["value"], "outputs": ["product"] },
      { "id": "join", "kernel": "add", "inputs": ["a", "b"], "outputs": ["sum"] }
    ],
    "bindings": [
      { "to": "fan.value", "from": "src.value" },
      { "to": "left.value", "from": "fan.left" },
      { "to": "join.a", "from": "left.product" },
      { "to": "join.b", "from": "fan.right" }
    ],
    "outputs": [
      { "from": "join.sum", "as": "out" }
    ]
  }
  )JSON";

  sr::engine::GraphDef graph;
  std::string error;
  if (!parse_graph(dsl, graph, error)) {
    std::cerr << "parse error: " << error << "\n";
    return false;
  }

  auto registry = make_registry();
  auto plan = sr::engine::compile_plan(graph, registry);
  if (!plan) {
    std::cerr << "compile error: " << plan.error().message << "\n";
    return false;
  }

  sr::engine::ExecutorConfig config;
  config.compute_threads = 4;
  sr::engine::Executor executor(config);
  sr::engine::RequestContext ctx;
  auto result = executor.run(*plan, ctx);
  if (!result) {
    std::cerr << "run error: " << result.error().message << "\n";
    return false;
  }

  const auto& value = result->outputs.at("out").get<int64_t>();
  return value == 21;
}

auto test_dataflow_parallel_runs() -> bool {
  const char* dsl = R"JSON(
  {
    "version": 1,
    "name": "multi_thread",
    "nodes": [
      { "id": "sum", "kernel": "add", "inputs": ["a", "b"], "outputs": ["sum"] },
      { "id": "scale", "kernel": "mul", "params": { "factor": 2 }, "inputs": ["value"], "outputs": ["product"] }
    ],
    "bindings": [
      { "to": "sum.a", "from": "$req.x" },
      { "to": "sum.b", "from": 3 },
      { "to": "scale.value", "from": "sum.sum" }
    ],
    "outputs": [
      { "from": "scale.product", "as": "out" }
    ]
  }
  )JSON";

  sr::engine::GraphDef graph;
  std::string error;
  if (!parse_graph(dsl, graph, error)) {
    std::cerr << "parse error: " << error << "\n";
    return false;
  }

  auto registry = make_registry();
  auto plan = sr::engine::compile_plan(graph, registry);
  if (!plan) {
    std::cerr << "compile error: " << plan.error().message << "\n";
    return false;
  }

  constexpr int kThreads = 6;
  constexpr int kIterations = 30;
  std::atomic<int> failures{0};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);

  const sr::engine::ExecPlan& compiled = *plan;
  sr::engine::ExecutorConfig config;
  config.compute_threads = 2;
  sr::engine::Executor executor(config);

  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&, i]() {
      uint64_t seed = 0x9e3779b97f4a7c15ULL + static_cast<uint64_t>(i);
      for (int iter = 0; iter < kIterations; ++iter) {
        seed = seed * 6364136223846793005ULL + 1;
        int64_t x = static_cast<int64_t>(seed % 1000);
        sr::engine::RequestContext ctx;
        ctx.set_env<int64_t>("x", x);
        auto result = executor.run(compiled, ctx);
        if (!result) {
          failures.fetch_add(1);
          return;
        }
        const auto& value = result->outputs.at("out").get<int64_t>();
        const int64_t expected = (x + 3) * 2;
        if (value != expected) {
          failures.fetch_add(1);
          return;
        }
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  return failures.load() == 0;
}

auto test_dataflow_mixed_schedulers() -> bool {
  struct ThreadRecord {
    std::mutex mutex;
    std::thread::id compute_id;
    std::thread::id io_id;
    bool compute_set = false;
    bool io_set = false;
  };

  const char* dsl = R"JSON(
  {
    "version": 1,
    "name": "mixed_schedulers",
    "nodes": [
      { "id": "src", "kernel": "const_i64", "params": { "value": 9 }, "inputs": [], "outputs": ["value"] },
      { "id": "cpu", "kernel": "compute_recorder", "inputs": ["value"], "outputs": ["value"] },
      { "id": "io", "kernel": "io_recorder", "inputs": ["value"], "outputs": ["value"] }
    ],
    "bindings": [
      { "to": "cpu.value", "from": "src.value" },
      { "to": "io.value", "from": "src.value" }
    ],
    "outputs": [
      { "from": "cpu.value", "as": "cpu_out" },
      { "from": "io.value", "as": "io_out" }
    ]
  }
  )JSON";

  sr::engine::GraphDef graph;
  std::string error;
  if (!parse_graph(dsl, graph, error)) {
    std::cerr << "parse error: " << error << "\n";
    return false;
  }

  sr::engine::KernelRegistry registry;
  sr::kernel::register_sample_kernels(registry);

  auto record = std::make_shared<ThreadRecord>();

  registry.register_kernel(
      "compute_recorder",
      [record](int64_t value) {
        {
          std::lock_guard<std::mutex> lock(record->mutex);
          record->compute_id = std::this_thread::get_id();
          record->compute_set = true;
        }
        return value;
      },
      sr::engine::TaskType::Compute);

  registry.register_kernel(
      "io_recorder",
      [record](int64_t value) {
        {
          std::lock_guard<std::mutex> lock(record->mutex);
          record->io_id = std::this_thread::get_id();
          record->io_set = true;
        }
        return value;
      },
      sr::engine::TaskType::Io);

  auto plan = sr::engine::compile_plan(graph, registry);
  if (!plan) {
    std::cerr << "compile error: " << plan.error().message << "\n";
    return false;
  }

  sr::engine::ExecutorConfig config;
  config.compute_threads = 1;
  config.io_threads = 1;
  sr::engine::Executor executor(config);
  sr::engine::RequestContext ctx;
  auto result = executor.run(*plan, ctx);
  if (!result) {
    std::cerr << "run error: " << result.error().message << "\n";
    return false;
  }

  if (result->outputs.at("cpu_out").get<int64_t>() != 9) {
    return false;
  }
  if (result->outputs.at("io_out").get<int64_t>() != 9) {
    return false;
  }

  std::thread::id compute_id;
  std::thread::id io_id;
  bool compute_set = false;
  bool io_set = false;
  {
    std::lock_guard<std::mutex> lock(record->mutex);
    compute_id = record->compute_id;
    io_id = record->io_id;
    compute_set = record->compute_set;
    io_set = record->io_set;
  }

  if (!compute_set || !io_set) {
    return false;
  }
  return compute_id != io_id;
}

auto test_dataflow_cancelled_request() -> bool {
  const char* dsl = R"JSON(
  {
    "version": 1,
    "name": "cancelled",
    "nodes": [
      { "id": "sum", "kernel": "add", "inputs": ["a", "b"], "outputs": ["sum"] }
    ],
    "bindings": [
      { "to": "sum.a", "from": 1 },
      { "to": "sum.b", "from": 2 }
    ],
    "outputs": [
      { "from": "sum.sum", "as": "out" }
    ]
  }
  )JSON";

  sr::engine::GraphDef graph;
  std::string error;
  if (!parse_graph(dsl, graph, error)) {
    return false;
  }

  auto registry = make_registry();
  auto plan = sr::engine::compile_plan(graph, registry);
  if (!plan) {
    return false;
  }

  sr::engine::ExecutorConfig config;
  config.compute_threads = 2;
  sr::engine::Executor executor(config);
  sr::engine::RequestContext ctx;
  ctx.cancel();
  auto result = executor.run(*plan, ctx);
  return !result;
}

auto test_dataflow_deadline_exceeded() -> bool {
  const char* dsl = R"JSON(
  {
    "version": 1,
    "name": "deadline",
    "nodes": [
      { "id": "sum", "kernel": "add", "inputs": ["a", "b"], "outputs": ["sum"] }
    ],
    "bindings": [
      { "to": "sum.a", "from": 1 },
      { "to": "sum.b", "from": 2 }
    ],
    "outputs": [
      { "from": "sum.sum", "as": "out" }
    ]
  }
  )JSON";

  sr::engine::GraphDef graph;
  std::string error;
  if (!parse_graph(dsl, graph, error)) {
    return false;
  }

  auto registry = make_registry();
  auto plan = sr::engine::compile_plan(graph, registry);
  if (!plan) {
    return false;
  }

  sr::engine::ExecutorConfig config;
  config.compute_threads = 2;
  sr::engine::Executor executor(config);
  sr::engine::RequestContext ctx;
  ctx.deadline = std::chrono::steady_clock::now() - std::chrono::milliseconds(1);
  auto result = executor.run(*plan, ctx);
  return !result;
}

}  // namespace

int main() {
  sr::kernel::register_builtin_types();

  TestStats stats;
  run_test("basic_pipeline", test_basic_pipeline, stats);
  run_test("optional_input_coalesce", test_optional_input_coalesce, stats);
  run_test("env_binding", test_env_binding, stats);
  run_test("missing_required_input", test_missing_required_input, stats);
  run_test("type_mismatch", test_type_mismatch, stats);
  run_test("cycle_detection", test_cycle_detection, stats);
  run_test("duplicate_output_name", test_duplicate_output_name, stats);
  run_test("env_type_mismatch", test_env_type_mismatch, stats);
  run_test("dynamic_port_names", test_dynamic_port_names, stats);
  run_test("dynamic_ports_missing_names", test_dynamic_ports_missing_names, stats);
  run_test("dataflow_fanout_join", test_dataflow_fanout_join, stats);
  run_test("dataflow_parallel_runs", test_dataflow_parallel_runs, stats);
  run_test("dataflow_mixed_schedulers", test_dataflow_mixed_schedulers, stats);
  run_test("dataflow_cancelled_request", test_dataflow_cancelled_request, stats);
  run_test("dataflow_deadline_exceeded", test_dataflow_deadline_exceeded, stats);

  std::cout << "Passed: " << stats.passed << ", Failed: " << stats.failed << "\n";
  return stats.failed == 0 ? 0 : 1;
}
