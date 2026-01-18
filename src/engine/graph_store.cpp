#include "engine/graph_store.hpp"

#include <algorithm>
#include <atomic>
#include <format>
#include <utility>

namespace sr::engine {
namespace {

struct HashBuilder {
  std::uint64_t value = 1469598103934665603ULL;

  void add_byte(unsigned char byte) {
    value ^= byte;
    value *= 1099511628211ULL;
  }

  void add(std::string_view text) {
    for (unsigned char byte : text) {
      add_byte(byte);
    }
    add_byte(0xff);
  }

  void add_int(std::int64_t number) {
    add(std::to_string(number));
  }

  void add_version(const Version& v) {
    add(v.to_string());
  }

  void add_json(const Json& json) {
    add(json.dump());
  }

  auto finish() const -> std::string {
    return std::format("{:016x}", value);
  }
};

auto hash_graph(const GraphDef& graph) -> std::string {
  HashBuilder builder;
  builder.add("graph");
  builder.add(graph.name);
  builder.add_version(graph.version);

  builder.add("nodes");
  builder.add_int(static_cast<std::int64_t>(graph.nodes.size()));
  for (const auto& node : graph.nodes) {
    builder.add(node.id);
    builder.add(node.kernel);
    builder.add_json(node.params);
    builder.add_int(static_cast<std::int64_t>(node.input_names.size()));
    for (const auto& input : node.input_names) {
      builder.add(input);
    }
    builder.add_int(static_cast<std::int64_t>(node.output_names.size()));
    for (const auto& output : node.output_names) {
      builder.add(output);
    }
  }

  builder.add("bindings");
  builder.add_int(static_cast<std::int64_t>(graph.bindings.size()));
  for (const auto& binding : graph.bindings) {
    builder.add(binding.to_node);
    builder.add(binding.to_port);
    switch (binding.source.kind) {
      case BindingKind::NodePort:
        builder.add("node");
        builder.add(binding.source.node);
        builder.add(binding.source.port);
        break;
      case BindingKind::Env:
        builder.add("env");
        builder.add(binding.source.env_key);
        break;
      case BindingKind::Const:
        builder.add("const");
        builder.add_json(binding.source.const_value);
        break;
    }
  }

  builder.add("outputs");
  builder.add_int(static_cast<std::int64_t>(graph.outputs.size()));
  for (const auto& output : graph.outputs) {
    builder.add(output.from_node);
    builder.add(output.from_port);
    builder.add(output.as);
  }

  return builder.finish();
}

}  // namespace

GraphStore::GraphStore(GraphStoreConfig config) : config_(config) {}

auto GraphStore::stage(const GraphDef& graph, const KernelRegistry& registry, const StageOptions& options)
  -> Expected<std::shared_ptr<const PlanSnapshot>> {
  if (graph.name.empty()) {
    return tl::unexpected(make_error("graph name is required"));
  }
  // version is always valid struct

  auto plan = compile_plan(graph, registry);
  if (!plan) {
    return tl::unexpected(plan.error());
  }

  std::string resolved_hash = options.hash.empty() ? hash_graph(graph) : options.hash;
  auto snapshot = std::make_shared<PlanSnapshot>();
  snapshot->key = GraphKey{graph.name, graph.version};
  snapshot->hash = std::move(resolved_hash);
  snapshot->plan = std::move(*plan);
  snapshot->registry_fingerprint = options.registry_fingerprint;
  snapshot->source = options.source;
  snapshot->compiled_at = std::chrono::system_clock::now();

  std::unique_lock lock(mutex_);
  auto& entry = entries_[graph.name];
  auto existing = entry.versions.find(graph.version);
  if (existing != entry.versions.end()) {
    if (existing->second->hash == snapshot->hash) {
      if (options.publish) {
        auto publish_result = publish_locked(entry, existing->second, PublishOptions{});
        if (!publish_result) {
          return tl::unexpected(publish_result.error());
        }
      }
      return existing->second;
    }
    if (!options.allow_replace) {
      return tl::unexpected(make_error(
        std::format("graph version already exists with different hash: {} v{}", graph.name, graph.version.to_string())));
    }
  }

  entry.versions[graph.version] = snapshot;
  enforce_retention(entry);

  if (options.publish) {
    auto publish_result = publish_locked(entry, snapshot, PublishOptions{});
    if (!publish_result) {
      return tl::unexpected(publish_result.error());
    }
  }

  return snapshot;
}

auto GraphStore::publish(std::string_view name, Version version, PublishOptions options)
  -> Expected<std::shared_ptr<const PlanSnapshot>> {
  std::unique_lock lock(mutex_);
  auto it = entries_.find(std::string(name));
  if (it == entries_.end()) {
    return tl::unexpected(make_error(std::format("graph not found: {}", name)));
  }
  auto& entry = it->second;
  auto snapshot_it = entry.versions.find(version);
  if (snapshot_it == entry.versions.end()) {
    return tl::unexpected(make_error(std::format("graph version not found: {} v{}", name, version.to_string())));
  }
  auto& snapshot = snapshot_it->second;
  auto result = publish_locked(entry, snapshot, options);
  if (!result) {
    return tl::unexpected(result.error());
  }
  return snapshot;
}

auto GraphStore::resolve(std::string_view name) const -> std::shared_ptr<const PlanSnapshot> {
  std::shared_lock lock(mutex_);
  auto it = entries_.find(std::string(name));
  if (it == entries_.end()) {
    return {};
  }
  return std::atomic_load_explicit(&it->second.active, std::memory_order_acquire);
}

auto GraphStore::resolve(std::string_view name, Version version) const -> std::shared_ptr<const PlanSnapshot> {
  std::shared_lock lock(mutex_);
  auto it = entries_.find(std::string(name));
  if (it == entries_.end()) {
    return {};
  }
  auto snapshot_it = it->second.versions.find(version);
  if (snapshot_it == it->second.versions.end()) {
    return {};
  }
  return snapshot_it->second;
}

auto GraphStore::active_version(std::string_view name) const -> std::optional<Version> {
  auto snapshot = resolve(name);
  if (!snapshot) {
    return std::nullopt;
  }
  return snapshot->key.version;
}

auto GraphStore::list_versions(std::string_view name) const -> std::vector<Version> {
  std::vector<Version> versions;
  std::shared_lock lock(mutex_);
  auto it = entries_.find(std::string(name));
  if (it == entries_.end()) {
    return versions;
  }
  versions.reserve(it->second.versions.size());
  for (const auto& [version, _] : it->second.versions) {
    versions.push_back(version);
  }
  std::sort(versions.begin(), versions.end());
  return versions;
}

auto GraphStore::evict(std::string_view name, Version version) -> bool {
  std::unique_lock lock(mutex_);
  auto it = entries_.find(std::string(name));
  if (it == entries_.end()) {
    return false;
  }
  auto& entry = it->second;
  if (entry.active_version && *entry.active_version == version) {
    return false;
  }
  auto erased = entry.versions.erase(version);
  if (entry.versions.empty()) {
    entries_.erase(it);
  }
  return erased > 0;
}

auto GraphStore::publish_locked(Entry& entry, const std::shared_ptr<const PlanSnapshot>& snapshot, PublishOptions options)
  -> Expected<void> {
  const Version& next_version = snapshot->key.version;
  bool allow_rollback = config_.allow_rollback && options.allow_rollback;
  if (!allow_rollback && entry.active_version && next_version < *entry.active_version) {
    return tl::unexpected(make_error(std::format("rollback disabled: active v{} > v{}", entry.active_version->to_string(),
                                                 next_version.to_string())));
  }
  entry.active_version = next_version;
  std::atomic_store_explicit(&entry.active, snapshot, std::memory_order_release);
  return {};
}

auto GraphStore::enforce_retention(Entry& entry) -> void {
  if (config_.max_versions == 0) {
    return;
  }
  if (entry.versions.size() <= config_.max_versions) {
    return;
  }
  std::vector<Version> versions;
  versions.reserve(entry.versions.size());
  for (const auto& [version, _] : entry.versions) {
    versions.push_back(version);
  }
  std::sort(versions.begin(), versions.end());
  for (const auto& version : versions) {
    if (entry.versions.size() <= config_.max_versions) {
      break;
    }
    if (entry.active_version && version == *entry.active_version) {
      continue;
    }
    entry.versions.erase(version);
  }
}

}  // namespace sr::engine
