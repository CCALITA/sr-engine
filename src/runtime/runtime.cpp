#include "runtime/runtime.hpp"
#include "common/logging/log.hpp"

#include <atomic>
#include <condition_variable>
#include <format>
#include <fstream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include <filesystem>

#include <exec/async_scope.hpp>
#include <exec/static_thread_pool.hpp>
#include <exec/timed_thread_scheduler.hpp>
#include <stdexec/execution.hpp>

#include "engine/error.hpp"
#include "kernel/rpc_kernels.hpp"
#include "runtime/runtime.hpp"
#include "runtime/serve/common.hpp"
#include "runtime/serve/grpc.hpp"
#include "runtime/serve/ipc.hpp"
#include "runtime/serve/rpc_env.hpp"
#ifdef SR_ENGINE_WITH_ARROW_FLIGHT
#include "runtime/serve/flight.hpp"
#endif

namespace sr::engine {
namespace {

struct GraphFileState {
  std::filesystem::file_time_type last_write;
  bool had_error = false;
};

auto path_matches_extension(const std::filesystem::path &path,
                            const std::string &extension) -> bool {
  if (extension.empty()) {
    return true;
  }
  return path.extension() == extension;
}

class GraphDaemon;

} // namespace

class Runtime::GraphDaemon {
public:
  GraphDaemon(Runtime &runtime, std::filesystem::path root,
              std::chrono::milliseconds poll_interval, std::string extension,
              bool recursive, bool allow_replace)
      : runtime_(runtime), root_(std::move(root)),
        poll_interval_(poll_interval), extension_(std::move(extension)),
        recursive_(recursive), allow_replace_(allow_replace),
        scheduler_context_(std::make_unique<exec::timed_thread_context>()),
        scheduler_(scheduler_context_->get_scheduler()), scope_() {
    schedule_scan();
  }

  ~GraphDaemon() {
    running_ = false;
    stdexec::sync_wait(scope_.on_empty());
  }

 private:
  auto log_error(std::string message) -> void {
    sr::log::error(message.c_str());
  }

  auto scan_once() -> void {
    if (root_.empty()) {
      log_error("root path is empty");
      return;
    }

    std::error_code fs_error;
    if (!std::filesystem::exists(root_, fs_error)) {
      log_error(std::format("root missing: {}", root_.string()));
      return;
    }
    if (!std::filesystem::is_directory(root_, fs_error)) {
      log_error(std::format("root is not a directory: {}", root_.string()));
      return;
    }

    std::unordered_set<std::string> seen;
    auto handle_entry = [this,
                         &seen](const std::filesystem::directory_entry &entry) {
      std::error_code entry_error;
      if (!entry.is_regular_file(entry_error)) {
        return;
      }
      const auto &path = entry.path();
      if (!path_matches_extension(path, extension_)) {
        return;
      }
      auto path_string = path.string();
      seen.insert(path_string);
      auto last_write = std::filesystem::last_write_time(path, entry_error);
      if (entry_error) {
        log_error(std::format("stat failed for {}: {}", path_string,
                              entry_error.message()));
        return;
      }
      auto it = files_.find(path_string);
      bool should_stage = it == files_.end() ||
                          it->second.last_write != last_write ||
                          it->second.had_error;
      if (!should_stage) {
        return;
      }
      StageOptions options;
      options.source = path_string;
      options.allow_replace = allow_replace_;
      auto staged = runtime_.stage_file(path_string, options);
      GraphFileState state{last_write, !staged};
      files_[path_string] = state;
      if (!staged) {
        log_error(std::format("stage failed for {}: {}", path_string,
                              staged.error().message));
      }
    };

    if (recursive_) {
      std::error_code iter_error;
      for (std::filesystem::recursive_directory_iterator it(root_, iter_error),
           end;
           it != end && !iter_error; it.increment(iter_error)) {
        handle_entry(*it);
      }
      if (iter_error) {
        log_error(std::format("scan failed for {}: {}", root_.string(),
                              iter_error.message()));
      }
    } else {
      std::error_code iter_error;
      for (std::filesystem::directory_iterator it(root_, iter_error), end;
           it != end && !iter_error; it.increment(iter_error)) {
        handle_entry(*it);
      }
      if (iter_error) {
        log_error(std::format("scan failed for {}: {}", root_.string(),
                              iter_error.message()));
      }
    }

    for (auto it = files_.begin(); it != files_.end();) {
      if (seen.find(it->first) == seen.end()) {
        it = files_.erase(it);
      } else {
        ++it;
      }
    }
  }

  auto schedule_scan() -> void {
    if (!running_.load(std::memory_order_relaxed)) {
      return;
    }
    auto sender = exec::schedule_after(scheduler_, poll_interval_) |
                  stdexec::then([this] {
                    if (running_.load(std::memory_order_relaxed)) {
                      scan_once();
                      schedule_scan();
                    }
                  });
    scope_.spawn(std::move(sender));
  }

  Runtime &runtime_;
  std::filesystem::path root_;
  std::chrono::milliseconds poll_interval_;
  std::string extension_;
  bool recursive_ = false;
  bool allow_replace_ = true;
  std::unique_ptr<exec::timed_thread_context> scheduler_context_;
  exec::timed_thread_scheduler scheduler_;
  std::atomic<bool> running_{true};
  exec::async_scope scope_;
  std::unordered_map<std::string, GraphFileState> files_;
};

Runtime::Runtime(RuntimeConfig config)
    : thread_pool_(config.executor.compute_threads > 0
                       ? config.executor.compute_threads
                       : (std::thread::hardware_concurrency() > 0
                              ? std::thread::hardware_concurrency()
                              : 1)),
      serve_pool_(config.executor.compute_threads > 0
                      ? config.executor.compute_threads
                      : (std::thread::hardware_concurrency() > 0
                             ? std::thread::hardware_concurrency()
                             : 1)),
       store_(config.store), executor_(&thread_pool_) {
  sr::log::init();
  
  sr::log::info("Initializing sr-engine runtime");
  
  if (config.graph_root && !config.graph_root->empty()) {
    sr::log::info("Starting graph daemon watching '{}'", config.graph_root->string());
    daemon_ = std::make_unique<GraphDaemon>(
        *this, *config.graph_root, config.graph_poll_interval,
        config.graph_extension, config.graph_recursive,
        config.graph_allow_replace);
  } else {
    sr::log::info("Runtime initialized without graph daemon");
  }
}

Runtime::~Runtime() = default;

auto Runtime::thread_pool() const -> exec::static_thread_pool & {
  return thread_pool_;
}

auto Runtime::serve_pool() const -> exec::static_thread_pool & {
  return serve_pool_;
}

auto Runtime::registry() -> KernelRegistry & { return registry_; }

auto Runtime::registry() const -> const KernelRegistry & { return registry_; }

auto Runtime::stage_graph(const GraphDef &graph, const StageOptions &options)
    -> Expected<std::shared_ptr<const PlanSnapshot>> {
  return store_.stage(graph, registry_, options);
}

auto Runtime::stage_json(const Json &json, const StageOptions &options)
    -> Expected<std::shared_ptr<const PlanSnapshot>> {
  auto graph = parse_graph_json(json);
  if (!graph) {
    return tl::unexpected(graph.error());
  }
  return stage_graph(*graph, options);
}

auto Runtime::stage_dsl(std::string_view dsl, const StageOptions &options)
    -> Expected<std::shared_ptr<const PlanSnapshot>> {
  Json json;
  try {
    json = Json::parse(dsl);
  } catch (const std::exception &ex) {
    return tl::unexpected(
        make_error(std::format("dsl parse error: {}", ex.what())));
  }
  return stage_json(json, options);
}

auto Runtime::stage_file(std::string_view path, const StageOptions &options)
    -> Expected<std::shared_ptr<const PlanSnapshot>> {
  std::ifstream file{std::string(path)};
  if (!file) {
    return tl::unexpected(
        make_error(std::format("failed to open dsl file: {}", path)));
  }
  std::ostringstream buffer;
  buffer << file.rdbuf();
  return stage_dsl(buffer.str(), options);
}

auto Runtime::publish(std::string_view name, int version,
                      PublishOptions options)
    -> Expected<std::shared_ptr<const PlanSnapshot>> {
  return store_.publish(name, version, options);
}

auto Runtime::resolve(std::string_view name) const
    -> std::shared_ptr<const PlanSnapshot> {
  return store_.resolve(name);
}

auto Runtime::resolve(std::string_view name, int version) const
    -> std::shared_ptr<const PlanSnapshot> {
  return store_.resolve(name, version);
}

auto Runtime::active_version(std::string_view name) const
    -> std::optional<int> {
  return store_.active_version(name);
}

auto Runtime::list_versions(std::string_view name) const -> std::vector<int> {
  return store_.list_versions(name);
}

auto Runtime::evict(std::string_view name, int version) -> bool {
  return store_.evict(name, version);
}

auto Runtime::run(std::string_view name, RequestContext &ctx) const
    -> Expected<ExecResult> {
  auto snapshot = store_.resolve(name);
  if (!snapshot) {
    auto versions = store_.list_versions(name);
    if (!versions.empty()) {
      return tl::unexpected(
          make_error(std::format("no active version for graph: {}", name)));
    }
    return tl::unexpected(make_error(std::format("graph not found: {}", name)));
  }
  return run(snapshot, ctx);
}

auto Runtime::run(std::string_view name, int version, RequestContext &ctx) const
    -> Expected<ExecResult> {
  auto snapshot = store_.resolve(name, version);
  if (!snapshot) {
    return tl::unexpected(make_error(
        std::format("graph version not found: {} v{}", name, version)));
  }
  return run(snapshot, ctx);
}

auto Runtime::run(const std::shared_ptr<const PlanSnapshot> &snapshot,
                  RequestContext &ctx) const -> Expected<ExecResult> {
  if (!snapshot) {
    return tl::unexpected(make_error("snapshot is null"));
  }
  return executor_.run(snapshot->plan, ctx);
}

auto Runtime::run_async(const std::shared_ptr<const PlanSnapshot> &snapshot,
                        RequestContext &ctx) const
    -> exec::task<Expected<ExecResult>> {
  if (!snapshot) {
    co_return tl::unexpected(make_error("snapshot is null"));
  }
  co_return co_await executor_.run_async(snapshot->plan, ctx);
}

auto Runtime::serve(ServeEndpointConfig config)
    -> Expected<std::unique_ptr<ServeHost>> {
  ServeLayerConfig layer;
  layer.endpoints.push_back(std::move(config));
  return ServeHost::create(*this, std::move(layer));
}

auto Runtime::serve(ServeLayerConfig config)
    -> Expected<std::unique_ptr<ServeHost>> {
  return ServeHost::create(*this, std::move(config));
}

} // namespace sr::engine
