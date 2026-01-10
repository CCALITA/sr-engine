#include "test_support.hpp"

auto test_graph_store_versioning() -> bool {
  const char *dsl_v1 = R"JSON(
  {
    "version": 1,
    "name": "versioned_graph",
    "nodes": [
      { "id": "a", "kernel": "const_i64", "params": { "value": 2 }, "inputs": [], "outputs": ["value"] }
    ],
    "outputs": [
      { "from": "a.value", "as": "out" }
    ]
  }
  )JSON";

  const char *dsl_v2 = R"JSON(
  {
    "version": 2,
    "name": "versioned_graph",
    "nodes": [
      { "id": "a", "kernel": "const_i64", "params": { "value": 3 }, "inputs": [], "outputs": ["value"] }
    ],
    "outputs": [
      { "from": "a.value", "as": "out" }
    ]
  }
  )JSON";

  sr::engine::GraphDef graph_v1;
  sr::engine::GraphDef graph_v2;
  std::string error;
  if (!parse_graph(dsl_v1, graph_v1, error)) {
    std::cerr << "parse error v1: " << error << "\n";
    return false;
  }
  if (!parse_graph(dsl_v2, graph_v2, error)) {
    std::cerr << "parse error v2: " << error << "\n";
    return false;
  }

  auto registry = make_registry();
  sr::engine::GraphStore store;
  auto snap_v1 = store.stage(graph_v1, registry,
                             sr::engine::StageOptions{.publish = true});
  if (!snap_v1) {
    std::cerr << "stage v1 error: " << snap_v1.error().message << "\n";
    return false;
  }
  auto snap_v2 = store.stage(graph_v2, registry);
  if (!snap_v2) {
    std::cerr << "stage v2 error: " << snap_v2.error().message << "\n";
    return false;
  }

  if (!store.publish("versioned_graph", 2)) {
    return false;
  }

  auto active = store.resolve("versioned_graph");
  if (!active || active->key.version != 2) {
    return false;
  }

  auto resolved_v1 = store.resolve("versioned_graph", 1);
  if (!resolved_v1 || resolved_v1->key.version != 1) {
    return false;
  }

  sr::engine::GraphStoreConfig config;
  config.allow_rollback = false;
  sr::engine::GraphStore no_rollback_store(config);
  auto snap_v1b = no_rollback_store.stage(
      graph_v1, registry, sr::engine::StageOptions{.publish = true});
  if (!snap_v1b) {
    return false;
  }
  auto snap_v2b = no_rollback_store.stage(
      graph_v2, registry, sr::engine::StageOptions{.publish = true});
  if (!snap_v2b) {
    return false;
  }
  if (no_rollback_store.publish("versioned_graph", 1)) {
    return false;
  }

  return true;
}

auto test_runtime_hot_swap() -> bool {
  const char *dsl_v1 = R"JSON(
  {
    "version": 1,
    "name": "hotswap_graph",
    "nodes": [
      { "id": "a", "kernel": "const_i64", "params": { "value": 2 }, "inputs": [], "outputs": ["value"] }
    ],
    "outputs": [
      { "from": "a.value", "as": "out" }
    ]
  }
  )JSON";

  const char *dsl_v2 = R"JSON(
  {
    "version": 2,
    "name": "hotswap_graph",
    "nodes": [
      { "id": "a", "kernel": "const_i64", "params": { "value": 3 }, "inputs": [], "outputs": ["value"] }
    ],
    "outputs": [
      { "from": "a.value", "as": "out" }
    ]
  }
  )JSON";

  sr::engine::Runtime runtime;
  sr::kernel::register_sample_kernels(runtime.registry());

  sr::engine::StageOptions options;
  options.publish = true;

  auto snap_v1 = runtime.stage_dsl(dsl_v1, options);
  if (!snap_v1) {
    return false;
  }

  sr::engine::RequestContext ctx;
  auto result_v1 = runtime.run("hotswap_graph", ctx);
  if (!result_v1) {
    return false;
  }
  if (result_v1->outputs.at("out").get<int64_t>() != 2) {
    return false;
  }

  auto snap_v2 = runtime.stage_dsl(dsl_v2, options);
  if (!snap_v2) {
    return false;
  }

  auto result_v2 = runtime.run("hotswap_graph", ctx);
  if (!result_v2) {
    return false;
  }
  if (result_v2->outputs.at("out").get<int64_t>() != 3) {
    return false;
  }

  auto result_old = runtime.run("hotswap_graph", 1, ctx);
  if (!result_old) {
    return false;
  }
  return result_old->outputs.at("out").get<int64_t>() == 2;
}

auto test_runtime_hot_swap_concurrent() -> bool {
  const char *dsl_v1 = R"JSON(
  {
    "version": 1,
    "name": "hotswap_graph_mt",
    "nodes": [
      { "id": "a", "kernel": "const_i64", "params": { "value": 2 }, "inputs": [], "outputs": ["value"] }
    ],
    "outputs": [
      { "from": "a.value", "as": "out" }
    ]
  }
  )JSON";

  const char *dsl_v2 = R"JSON(
  {
    "version": 2,
    "name": "hotswap_graph_mt",
    "nodes": [
      { "id": "a", "kernel": "const_i64", "params": { "value": 3 }, "inputs": [], "outputs": ["value"] }
    ],
    "outputs": [
      { "from": "a.value", "as": "out" }
    ]
  }
  )JSON";

  sr::engine::Runtime runtime;
  sr::kernel::register_sample_kernels(runtime.registry());

  sr::engine::StageOptions options;
  options.publish = true;

  auto snap_v1 = runtime.stage_dsl(dsl_v1, options);
  if (!snap_v1) {
    return false;
  }
  auto snap_v2 = runtime.stage_dsl(dsl_v2, options);
  if (!snap_v2) {
    return false;
  }

  std::atomic<int> failures{0};
  std::atomic<int> runs{0};

  auto runner = [&]() {
    for (int i = 0; i < 200; ++i) {
      sr::engine::RequestContext ctx;
      auto result = runtime.run("hotswap_graph_mt", ctx);
      if (!result) {
        failures.fetch_add(1);
        return;
      }
      auto value = result->outputs.at("out").get<int64_t>();
      if (value != 2 && value != 3) {
        failures.fetch_add(1);
        return;
      }
      runs.fetch_add(1);
    }
  };

  auto publisher = [&]() {
    for (int i = 0; i < 200; ++i) {
      int version = (i % 2 == 0) ? 1 : 2;
      auto published = runtime.publish("hotswap_graph_mt", version);
      if (!published) {
        failures.fetch_add(1);
        return;
      }
    }
  };

  std::thread publisher_thread(publisher);
  std::vector<std::thread> runners;
  for (int i = 0; i < 4; ++i) {
    runners.emplace_back(runner);
  }
  for (auto &thread : runners) {
    thread.join();
  }
  publisher_thread.join();

  return failures.load() == 0 && runs.load() > 0;
}

auto test_runtime_daemon_polling() -> bool {
  auto unique_suffix = std::to_string(
      std::chrono::steady_clock::now().time_since_epoch().count());
  auto temp_root = std::filesystem::temp_directory_path() /
                   std::format("sr_engine_daemon_{}", unique_suffix);
  std::error_code fs_error;
  std::filesystem::create_directories(temp_root, fs_error);
  if (fs_error) {
    std::cerr << "mkdir error: " << fs_error.message() << "\n";
    return false;
  }

  auto file_path = temp_root / "graph.json";
  auto dsl_v1 = make_daemon_dsl(7);
  if (!write_text_file(file_path, dsl_v1, std::chrono::seconds(0))) {
    std::cerr << "write error\n";
    std::filesystem::remove_all(temp_root, fs_error);
    return false;
  }

  bool ok = true;
  {
    sr::engine::RuntimeConfig config;
    config.graph_root = temp_root;
    config.graph_poll_interval = std::chrono::milliseconds(25);

    config.graph_allow_replace = true;

    sr::engine::Runtime runtime(config);
    sr::kernel::register_sample_kernels(runtime.registry());

    auto loaded = wait_for_condition(
        [&] { return runtime.resolve("daemon_graph") != nullptr; },
        std::chrono::seconds(2));
    if (!loaded) {
      std::cerr << "daemon did not load initial graph\n";
      ok = false;
    }

    sr::engine::RequestContext ctx;
    auto result = runtime.run("daemon_graph", ctx);
    if (!result || result->outputs.at("out").get<int64_t>() != 7) {
      std::cerr << "unexpected daemon output\n";
      ok = false;
    }

    auto dsl_v2 = make_daemon_dsl(9);
    if (!write_text_file(file_path, dsl_v2, std::chrono::seconds(2))) {
      std::cerr << "write error v2\n";
      ok = false;
    }

    auto updated = wait_for_condition(
        [&] {
          sr::engine::RequestContext inner_ctx;
          auto next = runtime.run("daemon_graph", inner_ctx);
          return next && next->outputs.at("out").get<int64_t>() == 9;
        },
        std::chrono::seconds(2));
    if (!updated) {
      std::cerr << "daemon did not refresh graph\n";
      ok = false;
    }
  }

  std::filesystem::remove_all(temp_root, fs_error);
  return ok;
}
