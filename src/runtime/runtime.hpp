#pragma once

#include <chrono>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <exec/static_thread_pool.hpp>
#include <exec/task.hpp>

#include "engine/dsl.hpp"
#include "engine/error.hpp"
#include "engine/graph_store.hpp"
#include "engine/registry.hpp"
#include "engine/version.hpp"
#include "runtime/executor.hpp"
#include "runtime/serve.hpp"

namespace sr::engine {

/// Configuration for runtime execution and optional DSL hot-swap polling.
struct RuntimeConfig {
  /// Executor thread configuration.
  ExecutorConfig executor;
  /// Graph store retention/publish policy.
  GraphStoreConfig store;
  /// Root directory to watch for graph definition files (disabled when unset).
  std::optional<std::filesystem::path> graph_root;
  /// Polling interval for the background graph daemon.
  std::chrono::milliseconds graph_poll_interval{std::chrono::seconds(60)};
  /// File extension filter (empty means all files).
  std::string graph_extension = ".json";
  /// When true, scan subdirectories under graph_root.
  bool graph_recursive = false;
  /// When true, allow replacing an existing version with a new hash.
  bool graph_allow_replace = true;
};

/// High-level facade that owns the registry, graph store, and executor.
class Runtime {
public:
  /// Construct with runtime config (spawns graph daemon when graph_root is
  /// set).
  explicit Runtime(RuntimeConfig config = {});
  /// Stops background daemon and releases resources.
  ~Runtime();

  /// Access the kernel registry to register kernel factories.
  auto registry() -> KernelRegistry &;
  /// Const access to the kernel registry.
  auto registry() const -> const KernelRegistry &;

  /// Stage a parsed graph and optionally publish it.
  auto stage_graph(const GraphDef &graph, const StageOptions &options = {})
      -> Expected<std::shared_ptr<const PlanSnapshot>>;
  /// Stage a graph from JSON and optionally publish it.
  auto stage_json(const Json &json, const StageOptions &options = {})
      -> Expected<std::shared_ptr<const PlanSnapshot>>;
  /// Stage a graph from DSL text and optionally publish it.
  auto stage_dsl(std::string_view dsl, const StageOptions &options = {})
      -> Expected<std::shared_ptr<const PlanSnapshot>>;
  /// Stage a graph by reading a DSL file from disk.
  auto stage_file(std::string_view path, const StageOptions &options = {})
      -> Expected<std::shared_ptr<const PlanSnapshot>>;

  /// Publish a specific graph version as active.
  auto publish(std::string_view name, Version version, PublishOptions options = {})
      -> Expected<std::shared_ptr<const PlanSnapshot>>;
  /// Resolve the active snapshot for a graph name.
  auto resolve(std::string_view name) const
      -> std::shared_ptr<const PlanSnapshot>;
  /// Resolve a specific graph snapshot version.
  auto resolve(std::string_view name, Version version) const
      -> std::shared_ptr<const PlanSnapshot>;
  /// Return the active version number for a graph name (if any).
  auto active_version(std::string_view name) const -> std::optional<Version>;
  /// List all stored versions for a graph name.
  auto list_versions(std::string_view name) const -> std::vector<Version>;
  /// Remove a non-active version from the store.
  auto evict(std::string_view name, Version version) -> bool;

  /// Execute the active graph version by name.
  auto run(std::string_view name, RequestContext &ctx) const
      -> Expected<ExecResult>;
  /// Execute a specific graph version by name.
  auto run(std::string_view name, Version version, RequestContext &ctx) const
      -> Expected<ExecResult>;
  /// Execute a previously staged snapshot.
  auto run(const std::shared_ptr<const PlanSnapshot> &snapshot,
           RequestContext &ctx) const -> Expected<ExecResult>;

  /// Execute a previously staged snapshot asynchronously.
  auto run_async(const std::shared_ptr<const PlanSnapshot> &snapshot,
                 RequestContext &ctx) const -> exec::task<Expected<ExecResult>>;

  /// Start a serve host for a single endpoint.
  auto serve(ServeEndpointConfig config)
      -> Expected<std::unique_ptr<ServeHost>>;
  /// Start a serve host for multiple endpoints.
  auto serve(ServeLayerConfig config) -> Expected<std::unique_ptr<ServeHost>>;

  /// Access the shared thread pool.
  auto thread_pool() const -> exec::static_thread_pool &;
  /// Access the serve thread pool.
  auto serve_pool() const -> exec::static_thread_pool &;

private:
  class GraphDaemon;

  mutable exec::static_thread_pool thread_pool_;
  mutable exec::static_thread_pool serve_pool_;
  KernelRegistry registry_;
  GraphStore store_;
  Executor executor_;
  std::unique_ptr<GraphDaemon> daemon_;
};

} // namespace sr::engine
