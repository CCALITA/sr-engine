#include <cstdint>
#include <exception>
#include <format>
#include <iostream>
#include <mutex>
#include <string>

#include "engine/trace.hpp"
#include "kernel/sample_kernels.hpp"
#include "runtime/runtime.hpp"

namespace {

/// Trace sink that prints run and node summaries to stdout.
struct StdoutTraceSink {
  std::mutex mutex;

  void on_run_start(const sr::engine::trace::RunStart &event) {
    std::lock_guard<std::mutex> lock(mutex);
    std::cout << std::format("[trace] run_start trace_id={} plan={}\n",
                             event.trace_id, event.plan_name);
  }

  void on_run_end(const sr::engine::trace::RunEnd &event) {
    std::lock_guard<std::mutex> lock(mutex);
    std::cout << std::format(
        "[trace] run_end trace_id={} status={} duration_ns={}\n",
        event.trace_id, static_cast<int>(event.status), event.duration);
  }

  void on_node_end(const sr::engine::trace::NodeEnd &event) {
    std::lock_guard<std::mutex> lock(mutex);
    std::cout << std::format(
        "[trace] node_end node={} status={} duration_ns={}\n", event.node_id,
        static_cast<int>(event.status), event.duration);
  }
};

}  // namespace

int main() {
  static sr::engine::TypeRegistry type_registry;
  sr::engine::Runtime runtime;
  sr::kernel::register_builtin_types(type_registry);
  sr::kernel::register_sample_kernels(runtime.registry());

  const char *dsl = R"JSON(
  {
    "version": 1,
    "name": "complex_demo",
    "nodes": [
      { "id": "sum", "kernel": "add", "inputs": ["a", "b"], "outputs": ["sum"] },
      { "id": "scaled", "kernel": "mul", "params": { "factor": 3 }, "inputs": ["value"], "outputs": ["product"] },
      { "id": "bonus", "kernel": "add", "inputs": ["a", "b"], "outputs": ["sum"] },
      { "id": "penalty", "kernel": "sub_i64", "inputs": ["a", "b"], "outputs": ["diff"] },
      { "id": "is_large", "kernel": "gt_i64", "inputs": ["a", "b"], "outputs": ["flag"] },
      { "id": "chosen", "kernel": "if_else_i64", "inputs": ["cond", "then", "else"], "outputs": ["value"] },
      { "id": "fan", "kernel": "fanout_i64", "inputs": ["value"], "outputs": ["left", "right"] },
      { "id": "left_mul", "kernel": "mul", "params": { "factor": 2 }, "inputs": ["value"], "outputs": ["product"] },
      { "id": "right_mul", "kernel": "mul", "params": { "factor": 4 }, "inputs": ["value"], "outputs": ["product"] },
      { "id": "final", "kernel": "add", "inputs": ["a", "b"], "outputs": ["sum"] },
      { "id": "text", "kernel": "format", "params": { "prefix": "final=" }, "inputs": ["value"], "outputs": ["text"] },
      { "id": "suffix", "kernel": "const_str", "params": { "value": " units" }, "inputs": [], "outputs": ["value"] },
      { "id": "message", "kernel": "concat_str", "params": { "sep": "" }, "inputs": ["a", "b"], "outputs": ["text"] }
    ],
    "bindings": [
      { "to": "sum.a", "from": "$req.x" },
      { "to": "sum.b", "from": 5 },
      { "to": "scaled.value", "from": "sum.sum" },
      { "to": "bonus.a", "from": "scaled.product" },
      { "to": "bonus.b", "from": 10 },
      { "to": "penalty.a", "from": "sum.sum" },
      { "to": "penalty.b", "from": 2 },
      { "to": "is_large.a", "from": "sum.sum" },
      { "to": "is_large.b", "from": "$req.threshold" },
      { "to": "chosen.cond", "from": "is_large.flag" },
      { "to": "chosen.then", "from": "bonus.sum" },
      { "to": "chosen.else", "from": "penalty.diff" },
      { "to": "fan.value", "from": "chosen.value" },
      { "to": "left_mul.value", "from": "fan.left" },
      { "to": "right_mul.value", "from": "fan.right" },
      { "to": "final.a", "from": "left_mul.product" },
      { "to": "final.b", "from": "right_mul.product" },
      { "to": "text.value", "from": "final.sum" },
      { "to": "message.a", "from": "text.text" },
      { "to": "message.b", "from": "suffix.value" }
    ],
    "outputs": [
      { "from": "message.text", "as": "message" },
      { "from": "final.sum", "as": "value" }
    ]
  }
  )JSON";

  sr::engine::StageOptions stage_options;
  stage_options.source = "examples/complex.cpp";
  stage_options.publish = true;
  auto snapshot = runtime.stage_dsl(dsl, stage_options);
  if (!snapshot) {
    std::cerr << "Stage error: " << snapshot.error().message << "\n";
    return 1;
  }

  StdoutTraceSink trace_sink;
  auto sink = sr::engine::trace::make_sink(trace_sink);

  auto run_case = [&](int64_t x, int64_t threshold) -> bool {
    sr::engine::RequestContext ctx;
    ctx.set_env<int64_t>("x", x);
    ctx.set_env<int64_t>("threshold", threshold);
    ctx.trace.sink = sink;
    ctx.trace.flags =
        sr::engine::trace::to_flags(sr::engine::trace::TraceFlag::RunSpan) |
        sr::engine::trace::to_flags(sr::engine::trace::TraceFlag::NodeSpan);

    auto result = runtime.run("complex_demo", ctx);
    if (!result) {
      std::cerr << "Run error: " << result.error().message << "\n";
      return false;
    }

    const auto &message = result->outputs.at("message").get<std::string>();
    const auto &value = result->outputs.at("value").get<int64_t>();
    std::cout << std::format("x={} threshold={} value={} message={}\n", x,
                             threshold, value, message);
    return true;
  };

  if (!run_case(7, 20)) {
    return 1;
  }
  if (!run_case(7, 10)) {
    return 1;
  }

  return 0;
}
