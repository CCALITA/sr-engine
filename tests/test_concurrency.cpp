#include "test_support.hpp"

auto test_runtime_concurrent_same_graph() -> bool {
  sr::engine::RuntimeConfig config;
  config.executor.compute_threads = 2;
  sr::engine::Runtime runtime(config);
  configure_runtime(runtime);

  auto counter = std::make_shared<std::atomic<int>>(0);
  runtime.registry().register_kernel(
      "count_i64", [counter](int64_t value) noexcept {
        counter->fetch_add(1, std::memory_order_relaxed);
        return value;
      });

  sr::engine::Json graph = sr::engine::Json::object();
  graph["version"] = 1;
  graph["name"] = "rt_same_graph";
  graph["nodes"] = sr::engine::Json::array();
  graph["bindings"] = sr::engine::Json::array();
  graph["outputs"] = sr::engine::Json::array();
  graph["nodes"].push_back(
      {{"id", "sum"},
       {"kernel", "add"},
       {"inputs", sr::engine::Json::array({"a", "b"})},
       {"outputs", sr::engine::Json::array({"sum"})}});
  graph["nodes"].push_back(
      {{"id", "count"},
       {"kernel", "count_i64"},
       {"inputs", sr::engine::Json::array({"value"})},
       {"outputs", sr::engine::Json::array({"value"})}});
  graph["bindings"].push_back({{"to", "sum.a"}, {"from", "$req.x"}});
  graph["bindings"].push_back({{"to", "sum.b"}, {"from", 5}});
  graph["bindings"].push_back({{"to", "count.value"}, {"from", "sum.sum"}});
  graph["outputs"].push_back({{"from", "count.value"}, {"as", "out"}});

  sr::engine::StageOptions options;
  options.publish = true;
  auto snapshot = runtime.stage_json(graph, options);
  if (!snapshot) {
    std::cerr << "stage error: " << snapshot.error().message << "\n";
    return false;
  }

  constexpr int kThreads = 6;
  constexpr int kIterations = 40;
  std::atomic<int> successes{0};
  std::atomic<bool> error_logged{false};
  std::mutex error_mutex;
  std::string error_message;
  std::vector<uint64_t> seeds(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    seeds[i] = 0x9e3779b97f4a7c15ULL ^ static_cast<uint64_t>(i);
  }

  auto ok = run_concurrent(kThreads, kIterations, [&](int thread_index, int) {
    auto &seed = seeds[thread_index];
    int64_t x = static_cast<int64_t>(next_seed(seed) % 1000);
    sr::engine::RequestContext ctx;
    ctx.set_env<int64_t>("x", x);
    auto result = runtime.run("rt_same_graph", ctx);
    if (!result) {
      if (!error_logged.exchange(true, std::memory_order_acq_rel)) {
        std::lock_guard<std::mutex> lock(error_mutex);
        error_message = std::format("run error: {}", result.error().message);
      }
      return false;
    }
    const auto &value = result->outputs.at("out").get<int64_t>();
    if (value != x + 5) {
      if (!error_logged.exchange(true, std::memory_order_acq_rel)) {
        std::lock_guard<std::mutex> lock(error_mutex);
        error_message = std::format("value mismatch: got {} expected {}", value,
                                    x + 5);
      }
      return false;
    }
    successes.fetch_add(1, std::memory_order_relaxed);
    return true;
  });

  if (!ok) {
    if (!error_message.empty()) {
      std::cerr << "[runtime_concurrent_same_graph] " << error_message << "\n";
    }
    return false;
  }
  const int expected = kThreads * kIterations;
  if (successes.load(std::memory_order_relaxed) != expected) {
    std::cerr << "[runtime_concurrent_same_graph] success mismatch: "
              << successes.load(std::memory_order_relaxed) << " expected "
              << expected << "\n";
    return false;
  }
  const int count = counter->load(std::memory_order_relaxed);
  if (count != expected) {
    std::cerr << "[runtime_concurrent_same_graph] count mismatch: " << count
              << " expected " << expected << "\n";
    return false;
  }
  return true;
}

auto test_runtime_concurrent_multi_graph() -> bool {
  sr::engine::RuntimeConfig config;
  config.executor.compute_threads = 3;
  sr::engine::Runtime runtime(config);
  configure_runtime(runtime);

  constexpr std::array<std::string_view, 3> names = {"rt_multi_a", "rt_multi_b",
                                                     "rt_multi_c"};
  constexpr std::array<int64_t, 3> addends = {1, 2, 3};

  sr::engine::StageOptions options;
  options.publish = true;
  for (std::size_t i = 0; i < names.size(); ++i) {
    auto graph = make_env_add_graph_json(names[i], addends[i], 0);
    auto snapshot = runtime.stage_json(graph, options);
    if (!snapshot) {
      std::cerr << "stage error: " << snapshot.error().message << "\n";
      return false;
    }
  }

  constexpr int kThreads = 5;
  constexpr int kIterations = 35;
  std::vector<uint64_t> seeds(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    seeds[i] = 0x243f6a8885a308d3ULL ^ static_cast<uint64_t>(i);
  }

  return run_concurrent(kThreads, kIterations, [&](int thread_index, int) {
    auto &seed = seeds[thread_index];
    auto pick = static_cast<std::size_t>(next_seed(seed) % names.size());
    int64_t x = static_cast<int64_t>(next_seed(seed) % 1000);
    sr::engine::RequestContext ctx;
    ctx.set_env<int64_t>("x", x);
    auto result = runtime.run(names[pick], ctx);
    if (!result) {
      return false;
    }
    const auto &value = result->outputs.at("out").get<int64_t>();
    return value == x + addends[pick];
  });
}

auto test_runtime_concurrent_error_isolation() -> bool {
  sr::engine::RuntimeConfig config;
  config.executor.compute_threads = 2;
  sr::engine::Runtime runtime(config);
  configure_runtime(runtime);

  sr::engine::Json graph = sr::engine::Json::object();
  graph["version"] = 1;
  graph["name"] = "rt_fail_mod";
  graph["nodes"] = sr::engine::Json::array();
  graph["bindings"] = sr::engine::Json::array();
  graph["outputs"] = sr::engine::Json::array();
  graph["nodes"].push_back(
      {{"id", "guard"},
       {"kernel", "fail_mod_i64"},
       {"params", {{"mod", 7}}},
       {"inputs", sr::engine::Json::array({"value"})},
       {"outputs", sr::engine::Json::array({"value"})}});
  graph["bindings"].push_back({{"to", "guard.value"}, {"from", "$req.x"}});
  graph["outputs"].push_back({{"from", "guard.value"}, {"as", "out"}});

  sr::engine::StageOptions options;
  options.publish = true;
  auto snapshot = runtime.stage_json(graph, options);
  if (!snapshot) {
    std::cerr << "stage error: " << snapshot.error().message << "\n";
    return false;
  }

  constexpr int kThreads = 6;
  constexpr int kIterations = 40;
  std::vector<uint64_t> seeds(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    seeds[i] = 0x5851f42d4c957f2dULL ^ static_cast<uint64_t>(i);
  }

  return run_concurrent(kThreads, kIterations, [&](int thread_index, int) {
    auto &seed = seeds[thread_index];
    int64_t x = static_cast<int64_t>(next_seed(seed) % 500);
    bool should_fail = (x % 7) == 0;
    sr::engine::RequestContext ctx;
    ctx.set_env<int64_t>("x", x);
    auto result = runtime.run("rt_fail_mod", ctx);
    if (should_fail) {
      return !result;
    }
    if (!result) {
      return false;
    }
    return result->outputs.at("out").get<int64_t>() == x;
  });
}

auto test_runtime_concurrent_cancel_deadline() -> bool {
  sr::engine::RuntimeConfig config;
  config.executor.compute_threads = 2;
  sr::engine::Runtime runtime(config);
  configure_runtime(runtime);

  sr::engine::StageOptions options;
  options.publish = true;
  auto graph = make_env_add_graph_json("rt_cancel_deadline", 4, 0);
  auto snapshot = runtime.stage_json(graph, options);
  if (!snapshot) {
    std::cerr << "stage error: " << snapshot.error().message << "\n";
    return false;
  }

  constexpr int kThreads = 4;
  constexpr int kIterations = 45;
  std::vector<uint64_t> seeds(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    seeds[i] = 0x14057b7ef767814fULL ^ static_cast<uint64_t>(i);
  }

  return run_concurrent(kThreads, kIterations, [&](int thread_index, int iter) {
    auto &seed = seeds[thread_index];
    int64_t x = static_cast<int64_t>(next_seed(seed) % 1000);
    int mode = (thread_index + iter) % 6;
    sr::engine::RequestContext ctx;
    ctx.set_env<int64_t>("x", x);
    if (mode == 0) {
      ctx.cancel();
    } else if (mode == 1) {
      ctx.deadline =
          std::chrono::steady_clock::now() - std::chrono::milliseconds(1);
    }
    auto result = runtime.run("rt_cancel_deadline", ctx);
    if (mode == 0 || mode == 1) {
      return !result;
    }
    if (!result) {
      return false;
    }
    return result->outputs.at("out").get<int64_t>() == x + 4;
  });
}

auto test_runtime_concurrent_stage_publish() -> bool {
  sr::engine::RuntimeConfig config;
  config.executor.compute_threads = 2;
  sr::engine::Runtime runtime(config);
  configure_runtime(runtime);

  constexpr std::string_view name = "rt_publish_graph";
  constexpr std::array<int64_t, 3> values = {10, 20, 30};

  sr::engine::StageOptions options;
  options.publish = true;
  options.allow_replace = true;
  for (int version = 1; version <= 3; ++version) {
    auto snapshot = runtime.stage_graph(
        make_const_graph_def(name, version, values[version - 1]), options);
    if (!snapshot) {
      std::cerr << "stage error: " << snapshot.error().message << "\n";
      return false;
    }
  }

  constexpr int kThreads = 4;
  constexpr int kIterations = 50;
  constexpr int kPublishes = 60;
  std::atomic<int> failures{0};
  std::barrier start_gate(kThreads + 1);

  auto runner = [&](int index) {
    start_gate.arrive_and_wait();
    uint64_t seed = 0x6a09e667f3bcc909ULL ^ static_cast<uint64_t>(index);
    for (int iter = 0; iter < kIterations; ++iter) {
      sr::engine::RequestContext ctx;
      auto result = runtime.run(name, ctx);
      if (!result) {
        failures.fetch_add(1, std::memory_order_relaxed);
        return;
      }
      const auto &value = result->outputs.at("out").get<int64_t>();
      if (std::find(values.begin(), values.end(), value) == values.end()) {
        failures.fetch_add(1, std::memory_order_relaxed);
        return;
      }
      if ((next_seed(seed) & 3ULL) == 0) {
        std::this_thread::yield();
      }
    }
  };

  auto publisher = [&]() {
    start_gate.arrive_and_wait();
    for (int iter = 0; iter < kPublishes; ++iter) {
      int version = (iter % 3) + 1;
      auto snapshot = runtime.stage_graph(
          make_const_graph_def(name, version, values[version - 1]), options);
      if (!snapshot) {
        failures.fetch_add(1, std::memory_order_relaxed);
        return;
      }
    }
  };

  std::thread publish_thread(publisher);
  std::vector<std::thread> runners;
  runners.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    runners.emplace_back(runner, i);
  }
  for (auto &thread : runners) {
    thread.join();
  }
  publish_thread.join();

  return failures.load(std::memory_order_relaxed) == 0;
}

auto test_runtime_concurrency_stress() -> bool {
  if (!stress_enabled()) {
    std::cout << "[SKIP] runtime_concurrency_stress (set SR_ENGINE_STRESS=1)\n";
    return true;
  }

  sr::engine::RuntimeConfig config;
  config.executor.compute_threads = 4;
  sr::engine::Runtime runtime(config);
  configure_runtime(runtime);

  const int chain_length = 24;
  const int64_t sleep_ms = 1;
  auto graph = make_chain_graph_json("rt_stress_chain", chain_length, sleep_ms);

  sr::engine::StageOptions options;
  options.publish = true;
  auto snapshot = runtime.stage_json(graph, options);
  if (!snapshot) {
    std::cerr << "stage error: " << snapshot.error().message << "\n";
    return false;
  }

  unsigned int hw = std::thread::hardware_concurrency();
  int threads = hw > 0 ? static_cast<int>(std::min(hw, 8U)) : 4;
  constexpr int kIterations = 20;

  std::vector<uint64_t> seeds(threads);
  for (int i = 0; i < threads; ++i) {
    seeds[i] = 0x9e3779b97f4a7c15ULL + static_cast<uint64_t>(i);
  }

  return run_concurrent(threads, kIterations, [&](int thread_index, int) {
    auto &seed = seeds[thread_index];
    int64_t x = static_cast<int64_t>(next_seed(seed) % 10000);
    sr::engine::RequestContext ctx;
    ctx.set_env<int64_t>("x", x);
    auto result = runtime.run("rt_stress_chain", ctx);
    if (!result) {
      return false;
    }
    return result->outputs.at("out").get<int64_t>() == x;
  });
}
