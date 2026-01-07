#include <algorithm>
#include <atomic>
#include <barrier>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <format>
#include <iostream>
#include <mutex>
#include <numeric>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "engine/dsl.hpp"
#include "engine/plan.hpp"
#include "kernel/sample_kernels.hpp"
#include "runtime/executor.hpp"
#include "runtime/executor_legacy.hpp"

namespace sr::bench {

using sr::engine::BindingDef;
using sr::engine::BindingKind;
using sr::engine::BindingSource;
using sr::engine::ExecPlan;
using sr::engine::Executor;
using sr::engine::ExecutorConfig;
using sr::engine::GraphDef;
using sr::engine::Json;
using sr::engine::KernelRegistry;
using sr::engine::LegacyExecutor;
using sr::engine::NodeDef;
using sr::engine::OutputDef;
using sr::engine::RequestContext;
using sr::engine::Expected;

/// Benchmark configuration parsed from CLI arguments.
struct BenchmarkConfig {
  int runs = 2000;
  int warmup = 200;
  int concurrency = 1;
  int threads = 0;
  int chain_length = 64;
  int diamond_levels = 6;
  int64_t addend = 1;
  int64_t seed = 7;
  bool verify = false;
  std::string shape = "chain";
  std::string executor = "both";
};

/// Basic stats computed from a vector of durations.
struct BenchStats {
  int64_t min_ns = 0;
  int64_t max_ns = 0;
  double mean_ns = 0.0;
  int64_t p50_ns = 0;
  int64_t p95_ns = 0;
  int64_t p99_ns = 0;
  double throughput = 0.0;
  std::chrono::nanoseconds wall{};
};

/// Parse an integer from a string_view, returning false on failure.
inline auto parse_int(std::string_view value, int &out) -> bool {
  int parsed = 0;
  auto result = std::from_chars(value.data(), value.data() + value.size(), parsed);
  if (result.ec != std::errc{} || result.ptr != value.data() + value.size()) {
    return false;
  }
  out = parsed;
  return true;
}

/// Parse a 64-bit integer from a string_view, returning false on failure.
inline auto parse_int64(std::string_view value, int64_t &out) -> bool {
  int64_t parsed = 0;
  auto result = std::from_chars(value.data(), value.data() + value.size(), parsed);
  if (result.ec != std::errc{} || result.ptr != value.data() + value.size()) {
    return false;
  }
  out = parsed;
  return true;
}

/// Print CLI usage information.
auto print_usage(std::string_view exe) -> void {
  std::cout << std::format(
      "Usage: {} [options]\n"
      "Options:\n"
      "  --shape=chain|diamond      Graph shape (default: chain)\n"
      "  --runs=N                   Timed runs (default: 2000)\n"
      "  --warmup=N                 Warmup runs (default: 200)\n"
      "  --concurrency=N            Parallel run threads (default: 1)\n"
      "  --threads=N                Executor compute threads (default: 0 -> hw)\n"
      "  --chain-length=N           Nodes in chain (default: 64)\n"
      "  --diamond-levels=N          Diamond stages (default: 6)\n"
      "  --addend=N                 Constant addend for chain (default: 1)\n"
      "  --seed=N                   Env seed value (default: 7)\n"
      "  --executor=sender|legacy|both  Which executor to run (default: both)\n"
      "  --verify=0|1               Validate output each run (default: 0)\n"
      "  --help                     Show help\n",
      exe);
}

/// Parse CLI arguments into a BenchmarkConfig.
auto parse_args(int argc, char **argv) -> Expected<BenchmarkConfig> {
  BenchmarkConfig config;
  for (int i = 1; i < argc; ++i) {
    std::string_view arg(argv[i]);
    if (arg == "--help" || arg == "-h") {
      return tl::unexpected(sr::engine::make_error("help"));
    }
    if (!arg.starts_with("--")) {
      return tl::unexpected(sr::engine::make_error("invalid argument"));
    }
    auto eq = arg.find('=');
    std::string_view key = arg.substr(2, eq == std::string_view::npos ? arg.size() - 2 : eq - 2);
    std::string_view value;
    if (eq == std::string_view::npos) {
      if (i + 1 >= argc) {
        return tl::unexpected(sr::engine::make_error("missing value"));
      }
      value = std::string_view(argv[++i]);
    } else {
      value = arg.substr(eq + 1);
    }

    if (key == "shape") {
      config.shape = std::string(value);
    } else if (key == "runs") {
      if (!parse_int(value, config.runs)) {
        return tl::unexpected(sr::engine::make_error("invalid runs"));
      }
    } else if (key == "warmup") {
      if (!parse_int(value, config.warmup)) {
        return tl::unexpected(sr::engine::make_error("invalid warmup"));
      }
    } else if (key == "concurrency") {
      if (!parse_int(value, config.concurrency)) {
        return tl::unexpected(sr::engine::make_error("invalid concurrency"));
      }
    } else if (key == "threads") {
      if (!parse_int(value, config.threads)) {
        return tl::unexpected(sr::engine::make_error("invalid threads"));
      }
    } else if (key == "chain-length") {
      if (!parse_int(value, config.chain_length)) {
        return tl::unexpected(sr::engine::make_error("invalid chain length"));
      }
    } else if (key == "diamond-levels") {
      if (!parse_int(value, config.diamond_levels)) {
        return tl::unexpected(sr::engine::make_error("invalid diamond levels"));
      }
    } else if (key == "addend") {
      if (!parse_int64(value, config.addend)) {
        return tl::unexpected(sr::engine::make_error("invalid addend"));
      }
    } else if (key == "seed") {
      if (!parse_int64(value, config.seed)) {
        return tl::unexpected(sr::engine::make_error("invalid seed"));
      }
    } else if (key == "executor") {
      config.executor = std::string(value);
    } else if (key == "verify") {
      int flag = 0;
      if (!parse_int(value, flag)) {
        return tl::unexpected(sr::engine::make_error("invalid verify"));
      }
      config.verify = (flag != 0);
    } else {
      return tl::unexpected(sr::engine::make_error("unknown flag"));
    }
  }

  if (config.runs <= 0 || config.warmup < 0) {
    return tl::unexpected(sr::engine::make_error("runs/warmup must be positive"));
  }
  if (config.concurrency <= 0) {
    return tl::unexpected(sr::engine::make_error("concurrency must be positive"));
  }
  if (config.chain_length <= 0 || config.diamond_levels <= 0) {
    return tl::unexpected(sr::engine::make_error("shape size must be positive"));
  }

  return config;
}

/// Add a node definition to a GraphDef.
auto add_node(GraphDef &graph, std::string id, std::string kernel, Json params,
              std::vector<std::string> inputs,
              std::vector<std::string> outputs) -> void {
  NodeDef node;
  node.id = std::move(id);
  node.kernel = std::move(kernel);
  node.params = std::move(params);
  node.input_names = std::move(inputs);
  node.output_names = std::move(outputs);
  graph.nodes.push_back(std::move(node));
}

/// Bind an env entry to a node input.
auto bind_env(GraphDef &graph, std::string to_node, std::string to_port,
              std::string env_key) -> void {
  BindingDef binding;
  binding.to_node = std::move(to_node);
  binding.to_port = std::move(to_port);
  binding.source.kind = BindingKind::Env;
  binding.source.env_key = std::move(env_key);
  graph.bindings.push_back(std::move(binding));
}

/// Bind a const value to a node input.
auto bind_const(GraphDef &graph, std::string to_node, std::string to_port,
                Json value) -> void {
  BindingDef binding;
  binding.to_node = std::move(to_node);
  binding.to_port = std::move(to_port);
  binding.source.kind = BindingKind::Const;
  binding.source.const_value = std::move(value);
  graph.bindings.push_back(std::move(binding));
}

/// Bind a node output to a node input.
auto bind_node(GraphDef &graph, std::string to_node, std::string to_port,
               std::string from_node, std::string from_port) -> void {
  BindingDef binding;
  binding.to_node = std::move(to_node);
  binding.to_port = std::move(to_port);
  binding.source.kind = BindingKind::NodePort;
  binding.source.node = std::move(from_node);
  binding.source.port = std::move(from_port);
  graph.bindings.push_back(std::move(binding));
}

/// Register a graph output mapping.
auto add_output(GraphDef &graph, std::string from_node, std::string from_port,
                std::string as) -> void {
  OutputDef output;
  output.from_node = std::move(from_node);
  output.from_port = std::move(from_port);
  output.as = std::move(as);
  graph.outputs.push_back(std::move(output));
}

/// Build a linear chain of add nodes.
auto make_chain_graph(std::string_view name, int length, int64_t addend) -> GraphDef {
  GraphDef graph;
  graph.version = 1;
  graph.name = std::string(name);

  for (int i = 0; i < length; ++i) {
    add_node(graph,
             std::format("n{}", i),
             "add",
             Json::object(),
             {"a", "b"},
             {"sum"});
    if (i == 0) {
      bind_env(graph, std::format("n{}", i), "a", "x");
      bind_const(graph, std::format("n{}", i), "b", addend);
    } else {
      bind_node(graph, std::format("n{}", i), "a", std::format("n{}", i - 1), "sum");
      bind_const(graph, std::format("n{}", i), "b", addend);
    }
  }

  add_output(graph, std::format("n{}", length - 1), "sum", "out");
  return graph;
}

/// Build a diamond chain: fanout -> 2x mul -> add, repeated per level.
auto make_diamond_graph(std::string_view name, int levels) -> GraphDef {
  GraphDef graph;
  graph.version = 1;
  graph.name = std::string(name);

  std::string last_node;
  for (int i = 0; i < levels; ++i) {
    std::string fan = std::format("fan_{}", i);
    std::string left = std::format("left_{}", i);
    std::string right = std::format("right_{}", i);
    std::string join = std::format("join_{}", i);

    add_node(graph, fan, "fanout_i64", Json::object(), {"value"}, {"left", "right"});
    add_node(graph, left, "mul", Json{{"factor", i + 2}}, {"value"}, {"value"});
    add_node(graph, right, "mul", Json{{"factor", i + 3}}, {"value"}, {"value"});
    add_node(graph, join, "add", Json::object(), {"a", "b"}, {"sum"});

    if (i == 0) {
      bind_env(graph, fan, "value", "x");
    } else {
      bind_node(graph, fan, "value", last_node, "sum");
    }
    bind_node(graph, left, "value", fan, "left");
    bind_node(graph, right, "value", fan, "right");
    bind_node(graph, join, "a", left, "value");
    bind_node(graph, join, "b", right, "value");

    last_node = join;
  }

  add_output(graph, last_node, "sum", "out");
  return graph;
}

/// Build a graph per the benchmark configuration.
auto build_graph(const BenchmarkConfig &config) -> Expected<GraphDef> {
  if (config.shape == "chain") {
    return make_chain_graph("bench_chain", config.chain_length, config.addend);
  }
  if (config.shape == "diamond") {
    return make_diamond_graph("bench_diamond", config.diamond_levels);
  }
  return tl::unexpected(sr::engine::make_error("unknown shape"));
}

/// Compute percentile statistics from a duration vector.
auto compute_stats(std::vector<int64_t> samples,
                   std::chrono::nanoseconds wall) -> BenchStats {
  BenchStats stats;
  if (samples.empty()) {
    return stats;
  }
  std::sort(samples.begin(), samples.end());
  stats.min_ns = samples.front();
  stats.max_ns = samples.back();
  stats.mean_ns = static_cast<double>(
      std::accumulate(samples.begin(), samples.end(), int64_t{0})) /
      static_cast<double>(samples.size());
  auto idx = [&](double p) -> std::size_t {
    auto pos = static_cast<std::size_t>(p * static_cast<double>(samples.size() - 1));
    return std::min(pos, samples.size() - 1);
  };
  stats.p50_ns = samples[idx(0.50)];
  stats.p95_ns = samples[idx(0.95)];
  stats.p99_ns = samples[idx(0.99)];
  stats.wall = wall;
  if (wall.count() > 0) {
    stats.throughput = static_cast<double>(samples.size()) /
                       (static_cast<double>(wall.count()) / 1e9);
  }
  return stats;
}

/// Run a benchmark using a provided executor type.
template <typename ExecT>
auto run_benchmark(const ExecPlan &plan, const BenchmarkConfig &config,
                   ExecT &executor) -> Expected<BenchStats> {
  const int total_runs = config.runs;
  const int warmup_runs = config.warmup;
  const int concurrency = config.concurrency;

  if (concurrency <= 1) {
    std::vector<int64_t> samples;
    samples.reserve(static_cast<std::size_t>(total_runs));
    for (int i = 0; i < warmup_runs; ++i) {
      RequestContext ctx;
      ctx.set_env<int64_t>("x", config.seed + i);
      auto result = executor.run(plan, ctx);
      if (!result) {
        return tl::unexpected(result.error());
      }
      if (config.verify) {
        (void)result->outputs.at("out").template get<int64_t>();
      }
    }

    auto wall_start = std::chrono::steady_clock::now();
    for (int i = 0; i < total_runs; ++i) {
      RequestContext ctx;
      ctx.set_env<int64_t>("x", config.seed + i);
      auto start = std::chrono::steady_clock::now();
      auto result = executor.run(plan, ctx);
      auto end = std::chrono::steady_clock::now();
      if (!result) {
        return tl::unexpected(result.error());
      }
      if (config.verify) {
        (void)result->outputs.at("out").template get<int64_t>();
      }
      samples.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
    }
    auto wall_end = std::chrono::steady_clock::now();
    return compute_stats(std::move(samples),
                         std::chrono::duration_cast<std::chrono::nanoseconds>(wall_end - wall_start));
  }

  const int base_runs = total_runs / concurrency;
  const int extra_runs = total_runs % concurrency;
  std::vector<std::vector<int64_t>> thread_samples(concurrency);
  std::vector<std::thread> threads;
  threads.reserve(static_cast<std::size_t>(concurrency));
  std::barrier start_gate(concurrency + 1);
  std::barrier finish_gate(concurrency + 1);
  std::mutex error_mutex;
  std::optional<sr::engine::EngineError> error;
  std::atomic<bool> abort{false};

  for (int t = 0; t < concurrency; ++t) {
    thread_samples[t].reserve(static_cast<std::size_t>(base_runs + (t < extra_runs ? 1 : 0)));
    threads.emplace_back([&, t]() {
      const int runs_for_thread = base_runs + (t < extra_runs ? 1 : 0);
      for (int i = 0; i < warmup_runs; ++i) {
        if (abort.load(std::memory_order_acquire)) {
          break;
        }
        RequestContext ctx;
        ctx.set_env<int64_t>("x", config.seed + i + t * 13);
        auto result = executor.run(plan, ctx);
        if (!result) {
          std::lock_guard<std::mutex> lock(error_mutex);
          if (!error) {
            error = result.error();
          }
          abort.store(true, std::memory_order_release);
          break;
        }
      }
      start_gate.arrive_and_wait();
      if (abort.load(std::memory_order_acquire)) {
        finish_gate.arrive_and_wait();
        return;
      }
      for (int i = 0; i < runs_for_thread; ++i) {
        if (abort.load(std::memory_order_acquire)) {
          break;
        }
        RequestContext ctx;
        ctx.set_env<int64_t>("x", config.seed + i + t * 13);
        auto start = std::chrono::steady_clock::now();
        auto result = executor.run(plan, ctx);
        auto end = std::chrono::steady_clock::now();
        if (!result) {
          std::lock_guard<std::mutex> lock(error_mutex);
          if (!error) {
            error = result.error();
          }
          abort.store(true, std::memory_order_release);
          break;
        }
        if (config.verify) {
          (void)result->outputs.at("out").template get<int64_t>();
        }
        thread_samples[t].push_back(
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
      }
      finish_gate.arrive_and_wait();
    });
  }

  start_gate.arrive_and_wait();
  auto wall_start = std::chrono::steady_clock::now();
  finish_gate.arrive_and_wait();
  auto wall_end = std::chrono::steady_clock::now();
  for (auto &thread : threads) {
    thread.join();
  }

  if (error) {
    return tl::unexpected(*error);
  }

  std::vector<int64_t> merged;
  for (auto &bucket : thread_samples) {
    merged.insert(merged.end(), bucket.begin(), bucket.end());
  }
  return compute_stats(std::move(merged),
                       std::chrono::duration_cast<std::chrono::nanoseconds>(wall_end - wall_start));
}

/// Print benchmark stats.
auto print_stats(std::string_view label, const BenchStats &stats) -> void {
  std::cout << std::format(
      "{}\n  min:  {} ns\n  p50:  {} ns\n  p95:  {} ns\n  p99:  {} ns\n  max:  {} ns\n  mean: {:.2f} ns\n  wall: {} ms\n  throughput: {:.2f} runs/s\n",
      label,
      stats.min_ns,
      stats.p50_ns,
      stats.p95_ns,
      stats.p99_ns,
      stats.max_ns,
      stats.mean_ns,
      std::chrono::duration_cast<std::chrono::milliseconds>(stats.wall).count(),
      stats.throughput);
}

}  // namespace sr::bench

int main(int argc, char **argv) {
  using sr::bench::BenchmarkConfig;
  auto config_result = sr::bench::parse_args(argc, argv);
  if (!config_result) {
    if (config_result.error().message == "help") {
      sr::bench::print_usage(argv[0]);
      return 0;
    }
    std::cerr << "Argument error: " << config_result.error().message << "\n";
    sr::bench::print_usage(argv[0]);
    return 1;
  }
  BenchmarkConfig config = std::move(*config_result);

  sr::kernel::register_builtin_types();
  sr::engine::KernelRegistry registry;
  sr::kernel::register_sample_kernels(registry);

  auto graph_result = sr::bench::build_graph(config);
  if (!graph_result) {
    std::cerr << "Graph error: " << graph_result.error().message << "\n";
    return 1;
  }

  auto plan = sr::engine::compile_plan(*graph_result, registry);
  if (!plan) {
    std::cerr << "Compile error: " << plan.error().message << "\n";
    return 1;
  }

  sr::engine::ExecutorConfig exec_config;
  exec_config.compute_threads = config.threads;

  if (config.executor == "sender" || config.executor == "both") {
    sr::engine::Executor executor(exec_config);
    auto result = sr::bench::run_benchmark(*plan, config, executor);
    if (!result) {
      std::cerr << "Sender executor error: " << result.error().message << "\n";
      return 1;
    }
    sr::bench::print_stats("Sender Executor", *result);
  }

  if (config.executor == "legacy" || config.executor == "both") {
    sr::engine::LegacyExecutor executor(exec_config);
    auto result = sr::bench::run_benchmark(*plan, config, executor);
    if (!result) {
      std::cerr << "Legacy executor error: " << result.error().message << "\n";
      return 1;
    }
    sr::bench::print_stats("Legacy Executor", *result);
  }

  return 0;
}
