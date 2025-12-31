#pragma once

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

struct GraphKey {
  std::string name;
  int version = 0;
};

struct PlanSnapshot {
  GraphKey key;
  std::string hash;
  ExecPlan plan;
  std::string registry_fingerprint;
  std::string source;
  std::chrono::system_clock::time_point compiled_at;
};

struct StageOptions {
  std::string source;
  std::string registry_fingerprint;
  std::string hash;
  bool publish = false;
  bool allow_replace = false;
};

struct PublishOptions {
  bool allow_rollback = true;
};

struct GraphStoreConfig {
  std::size_t max_versions = 0;
  bool allow_rollback = true;
};

class GraphStore {
 public:
  explicit GraphStore(GraphStoreConfig config = {});

  auto stage(const GraphDef& graph, const KernelRegistry& registry, const StageOptions& options = {})
    -> Expected<std::shared_ptr<const PlanSnapshot>>;
  auto publish(std::string_view name, int version, PublishOptions options = {})
    -> Expected<std::shared_ptr<const PlanSnapshot>>;

  auto resolve(std::string_view name) const -> std::shared_ptr<const PlanSnapshot>;
  auto resolve(std::string_view name, int version) const -> std::shared_ptr<const PlanSnapshot>;
  auto active_version(std::string_view name) const -> std::optional<int>;
  auto list_versions(std::string_view name) const -> std::vector<int>;
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
