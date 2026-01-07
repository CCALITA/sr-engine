#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "engine/dsl.hpp"
#include "engine/error.hpp"
#include "engine/plan.hpp"
#include "engine/registry.hpp"

namespace sr::engine {

struct PlanSenderTemplate;

/// Unique graph identifier (name + version).
struct GraphKey {
  std::string name;
  int version = 0;
};

/// Immutable compiled plan snapshot stored in the graph store.
struct PlanSnapshot {
  GraphKey key;
  std::string hash;
  ExecPlan plan;
  std::string registry_fingerprint;
  std::string source;
  std::chrono::system_clock::time_point compiled_at;
  mutable std::atomic<std::shared_ptr<const PlanSenderTemplate>> sender_template{nullptr};
};

/// Options for staging a graph into the store.
struct StageOptions {
  /// Optional source description (file path, etc).
  std::string source;
  /// Registry fingerprint used during compilation.
  std::string registry_fingerprint;
  /// Optional content hash; computed when empty.
  std::string hash;
  /// When true, publish the staged version as active.
  bool publish = false;
  /// When true, replace an existing version with a new hash.
  bool allow_replace = false;
};

/// Options for publishing a specific graph version.
struct PublishOptions {
  /// When false, disallow publishing older versions.
  bool allow_rollback = true;
};

/// Retention and rollback policy for stored graphs.
struct GraphStoreConfig {
  /// Max versions retained per graph name (0 = unlimited).
  std::size_t max_versions = 0;
  /// Allow publishing older versions as active.
  bool allow_rollback = true;
};

/// Stores compiled graph plans and tracks active versions.
class GraphStore {
 public:
  /// Construct with retention policy.
  explicit GraphStore(GraphStoreConfig config = {});

  /// Compile a graph, store it, and optionally publish it.
  auto stage(const GraphDef& graph, const KernelRegistry& registry, const StageOptions& options = {})
    -> Expected<std::shared_ptr<const PlanSnapshot>>;
  /// Publish a stored version as the active one.
  auto publish(std::string_view name, int version, PublishOptions options = {})
    -> Expected<std::shared_ptr<const PlanSnapshot>>;

  /// Resolve the active version snapshot by name.
  auto resolve(std::string_view name) const -> std::shared_ptr<const PlanSnapshot>;
  /// Resolve a specific stored version by name.
  auto resolve(std::string_view name, int version) const -> std::shared_ptr<const PlanSnapshot>;
  /// Return the active version number (if any).
  auto active_version(std::string_view name) const -> std::optional<int>;
  /// List all stored versions for a graph name.
  auto list_versions(std::string_view name) const -> std::vector<int>;
  /// Remove a non-active version from the store.
  auto evict(std::string_view name, int version) -> bool;

 private:
  struct Entry {
    std::unordered_map<int, std::shared_ptr<const PlanSnapshot>> versions;
    std::shared_ptr<const PlanSnapshot> active;
    int active_version = -1;
  };

  auto publish_locked(Entry& entry, const std::shared_ptr<const PlanSnapshot>& snapshot, PublishOptions options)
    -> Expected<void>;
  auto enforce_retention(Entry& entry) -> void;

  GraphStoreConfig config_;
  mutable std::shared_mutex mutex_;
  std::unordered_map<std::string, Entry> entries_;
};

}  // namespace sr::engine
