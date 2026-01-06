#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <format>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "engine/dsl.hpp"
#include "engine/error.hpp"
#include "engine/graph_store.hpp"
#include "engine/plan.hpp"
#include "engine/trace.hpp"
#include "engine/types.hpp"
#include "kernel/sample_kernels.hpp"
#include "runtime/executor.hpp"
#include "runtime/runtime.hpp"

/// Simple stats holder for the ad-hoc test runner.
struct TestStats {
  int passed = 0;
  int failed = 0;
};

/// Wait until predicate returns true or the timeout expires.
inline auto wait_for_condition(const std::function<bool()> &predicate,
                               std::chrono::milliseconds timeout) -> bool {
  auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return false;
}

/// Write text to a file and bump the timestamp by the supplied duration.
inline auto write_text_file(const std::filesystem::path &path,
                            const std::string &content,
                            std::chrono::seconds bump) -> bool {
  std::ofstream file(path, std::ios::trunc);
  if (!file) {
    return false;
  }
  file << content;
  file.close();
  std::error_code ec;
  auto stamp = std::filesystem::file_time_type::clock::now() + bump;
  std::filesystem::last_write_time(path, stamp, ec);
  return !ec;
}

/// Build a small DSL string for daemon polling tests.
inline auto make_daemon_dsl(int value) -> std::string {
  return std::format(R"JSON(
  {{
    "version": 1,
    "name": "daemon_graph",
    "nodes": [
      {{ "id": "a", "kernel": "const_i64", "params": {{ "value": {} }}, "inputs": [], "outputs": ["value"] }}
    ],
    "outputs": [
      {{ "from": "a.value", "as": "out" }}
    ]
  }}
  )JSON",
                     value);
}

/// Parse a DSL string to a GraphDef for tests.
inline auto parse_graph(const char *dsl, sr::engine::GraphDef &out,
                        std::string &error) -> bool {
  try {
    auto json = sr::engine::Json::parse(dsl);
    auto graph = sr::engine::parse_graph_json(json);
    if (!graph) {
      error = graph.error().message;
      return false;
    }
    out = std::move(*graph);
    return true;
  } catch (const std::exception &ex) {
    error = ex.what();
    return false;
  }
}

/// Build a registry with the default sample kernels.
inline auto make_registry() -> sr::engine::KernelRegistry {
  sr::engine::KernelRegistry registry;
  sr::kernel::register_sample_kernels(registry);
  return registry;
}

/// Read an int64 param with a fallback for test kernels.
inline auto get_int_param(const sr::engine::Json &params, const char *key,
                          int64_t fallback) -> int64_t {
  if (!params.is_object()) {
    return fallback;
  }
  auto it = params.find(key);
  if (it != params.end() && it->is_number_integer()) {
    return it->get<int64_t>();
  }
  return fallback;
}

/// Register extra kernels used only by concurrency tests.
inline auto register_concurrency_kernels(sr::engine::KernelRegistry &registry)
  -> void {
  registry.register_kernel_with_params(
      "sleep_i64", [](const sr::engine::Json &params) {
        const auto ms = get_int_param(params, "ms", 0);
        return [ms](int64_t value) noexcept {
          if (ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(ms));
          }
          return value;
        };
      });

  registry.register_kernel_with_params(
      "fail_mod_i64", [](const sr::engine::Json &params) {
        const auto mod = get_int_param(params, "mod", 2);
        return [mod](int64_t value) noexcept -> sr::engine::Expected<int64_t> {
          if (mod != 0 && (value % mod) == 0) {
            return tl::unexpected(sr::engine::make_error("fail_mod_i64"));
          }
          return value;
        };
      });
}

/// Configure a runtime with sample + concurrency test kernels.
inline auto configure_runtime(sr::engine::Runtime &runtime) -> void {
  sr::kernel::register_sample_kernels(runtime.registry());
  register_concurrency_kernels(runtime.registry());
}

/// Simple deterministic RNG step for concurrency tests.
inline auto next_seed(uint64_t &seed) -> uint64_t {
  seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
  return seed;
}

/// Run a synchronized parallel loop and return false on the first failure.
template <typename Fn>
auto run_concurrent(int threads, int iterations, Fn &&fn) -> bool {
  if (threads <= 0 || iterations <= 0) {
    return true;
  }

  std::barrier start_gate(threads);
  std::atomic<bool> abort{false};
  std::atomic<int> failures{0};
  std::vector<std::thread> workers;
  workers.reserve(static_cast<std::size_t>(threads));

  for (int i = 0; i < threads; ++i) {
    workers.emplace_back([&, i]() {
      start_gate.arrive_and_wait();
      for (int iter = 0; iter < iterations; ++iter) {
        if (abort.load(std::memory_order_acquire)) {
          break;
        }
        if (!fn(i, iter)) {
          failures.fetch_add(1, std::memory_order_relaxed);
          abort.store(true, std::memory_order_release);
          break;
        }
      }
    });
  }

  for (auto &worker : workers) {
    worker.join();
  }

  return failures.load(std::memory_order_relaxed) == 0;
}

/// Check whether the stress suite is enabled via SR_ENGINE_STRESS.
inline auto stress_enabled() -> bool {
  const char *flag = std::getenv("SR_ENGINE_STRESS");
  return flag && *flag != '\0' && std::string(flag) != "0";
}

/// Build a simple env -> add -> optional sleep graph JSON.
inline auto make_env_add_graph_json(std::string_view name, int64_t addend,
                                    int64_t sleep_ms) -> sr::engine::Json {
  sr::engine::Json graph = sr::engine::Json::object();
  graph["version"] = 1;
  graph["name"] = std::string(name);
  graph["nodes"] = sr::engine::Json::array();
  graph["bindings"] = sr::engine::Json::array();
  graph["outputs"] = sr::engine::Json::array();

  graph["nodes"].push_back(
      {{"id", "sum"},
       {"kernel", "add"},
       {"inputs", sr::engine::Json::array({"a", "b"})},
       {"outputs", sr::engine::Json::array({"sum"})}});

  graph["bindings"].push_back({{"to", "sum.a"}, {"from", "$req.x"}});
  graph["bindings"].push_back({{"to", "sum.b"}, {"from", addend}});

  if (sleep_ms > 0) {
    graph["nodes"].push_back(
        {{"id", "delay"},
         {"kernel", "sleep_i64"},
         {"params", {{"ms", sleep_ms}}},
         {"inputs", sr::engine::Json::array({"value"})},
         {"outputs", sr::engine::Json::array({"value"})}});
    graph["bindings"].push_back({{"to", "delay.value"}, {"from", "sum.sum"}});
    graph["outputs"].push_back({{"from", "delay.value"}, {"as", "out"}});
  } else {
    graph["outputs"].push_back({{"from", "sum.sum"}, {"as", "out"}});
  }

  return graph;
}

/// Build a chain graph that forwards env x through N sleep nodes.
inline auto make_chain_graph_json(std::string_view name, int length,
                                  int64_t sleep_ms) -> sr::engine::Json {
  sr::engine::Json graph = sr::engine::Json::object();
  graph["version"] = 1;
  graph["name"] = std::string(name);
  graph["nodes"] = sr::engine::Json::array();
  graph["bindings"] = sr::engine::Json::array();
  graph["outputs"] = sr::engine::Json::array();

  graph["nodes"].push_back(
      {{"id", "seed"},
       {"kernel", "identity_i64"},
       {"inputs", sr::engine::Json::array({"value"})},
       {"outputs", sr::engine::Json::array({"value"})}});
  graph["bindings"].push_back({{"to", "seed.value"}, {"from", "$req.x"}});

  std::string prev_id = "seed";
  for (int i = 0; i < length; ++i) {
    std::string node_id = std::format("n{}", i);
    graph["nodes"].push_back(
        {{"id", node_id},
         {"kernel", "sleep_i64"},
         {"params", {{"ms", sleep_ms}}},
         {"inputs", sr::engine::Json::array({"value"})},
         {"outputs", sr::engine::Json::array({"value"})}});
    graph["bindings"].push_back(
        {{"to", node_id + ".value"}, {"from", prev_id + ".value"}});
    prev_id = std::move(node_id);
  }

  graph["outputs"].push_back({{"from", prev_id + ".value"}, {"as", "out"}});
  return graph;
}

/// Build a minimal const graph for publish/run contention tests.
inline auto make_const_graph_def(std::string_view name, int version,
                                 int64_t value) -> sr::engine::GraphDef {
  sr::engine::GraphDef graph;
  graph.name = std::string(name);
  graph.version = version;

  sr::engine::NodeDef node;
  node.id = "a";
  node.kernel = "const_i64";
  node.params = sr::engine::Json{{"value", value}};
  node.input_names = {};
  node.output_names = {"value"};
  graph.nodes.push_back(std::move(node));

  sr::engine::OutputDef output;
  output.from_node = "a";
  output.from_port = "value";
  output.as = "out";
  graph.outputs.push_back(std::move(output));

  return graph;
}
