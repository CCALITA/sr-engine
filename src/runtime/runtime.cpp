#include "runtime/runtime.hpp"

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

} // namespace

class Runtime::GraphDaemon {
public:
  GraphDaemon(Runtime &runtime, std::filesystem::path root,
              std::chrono::milliseconds poll_interval, std::string extension,
              bool recursive, bool allow_replace)
      : runtime_(runtime), root_(std::move(root)),
        poll_interval_(poll_interval), extension_(std::move(extension)),
        recursive_(recursive), allow_replace_(allow_replace) {
    start();
  }

  ~GraphDaemon() { stop(); }

private:
  auto start() -> void {
    if (running_) {
      // WTH?
      return;
    }
    running_ = true;
    worker_ = std::thread([this] { run(); });
  }

  auto stop() -> void {
    {
      std::lock_guard lock(mutex_);
      if (!running_) {
        return;
      }
      stop_requested_ = true;
    }
    wakeup_.notify_all();
    if (worker_.joinable()) {
      worker_.join();
    }
    running_ = false;
  }

  auto run() -> void {
    while (true) {
      scan_once();
      std::unique_lock lock(mutex_);
      if (stop_requested_) {
        break;
      }
      if (poll_interval_.count() <= 0) {
        wakeup_.wait(lock, [this] { return stop_requested_; });
      } else {
        wakeup_.wait_for(lock, poll_interval_,
                         [this] { return stop_requested_; });
      }
      if (stop_requested_) {
        break;
      }
    }
  }

  auto scan_once() -> void {
    if (root_.empty()) {
      set_error("graph daemon root path is empty");
      return;
    }

    std::error_code fs_error;
    if (!std::filesystem::exists(root_, fs_error)) {
      set_error(std::format("graph daemon root missing: {}", root_.string()));
      return;
    }
    if (!std::filesystem::is_directory(root_, fs_error)) {
      set_error(std::format("graph daemon root is not a directory: {}",
                            root_.string()));
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
        set_error(std::format("stat failed for {}: {}", path_string,
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
        set_error(std::format("stage failed for {}: {}", path_string,
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
        set_error(std::format("scan failed for {}: {}", root_.string(),
                              iter_error.message()));
      }
    } else {
      std::error_code iter_error;
      for (std::filesystem::directory_iterator it(root_, iter_error), end;
           it != end && !iter_error; it.increment(iter_error)) {
        handle_entry(*it);
      }
      if (iter_error) {
        set_error(std::format("scan failed for {}: {}", root_.string(),
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

  auto set_error(std::string message) -> void {
    std::lock_guard lock(mutex_);
    last_error_ = std::move(message);
  }

  Runtime &runtime_;
  std::filesystem::path root_;
  std::chrono::milliseconds poll_interval_;
  std::string extension_;
  bool recursive_ = false;
  bool allow_replace_ = true;
  std::atomic<bool> running_{false};
  bool stop_requested_ = false;
  std::thread worker_;
  std::condition_variable wakeup_;
  std::mutex mutex_;
  std::unordered_map<std::string, GraphFileState> files_;
  std::optional<std::string> last_error_;
};

Runtime::Runtime(RuntimeConfig config)
    : store_(config.store), executor_(config.executor) {
  if (config.graph_root && !config.graph_root->empty()) {
    daemon_ = std::make_unique<GraphDaemon>(
        *this, *config.graph_root, config.graph_poll_interval,
        config.graph_extension, config.graph_recursive,
        config.graph_allow_replace);
  }
}

Runtime::~Runtime() = default;

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

auto Runtime::serve(ServeConfig config)
    -> Expected<std::unique_ptr<ServeHost>> {
  return ServeHost::create(*this, std::move(config));
}

} // namespace sr::engine
