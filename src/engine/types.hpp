#pragma once

#include <atomic>
#include <cassert>
#include <chrono>
#include <exec/any_sender_of.hpp>
#include <format>
#include <memory>
#include <span>
#include <stdexec/execution.hpp>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "engine/error.hpp"
#include "engine/trace.hpp"
#include "reflection/entt.hpp"
#include "reflection/json.hpp"

namespace sr::engine {

using Json = nlohmann::json;
namespace ex = stdexec;

enum class Cardinality {
  Single,
  Multi,
};

struct PortDesc {
  std::string name;
  entt::meta_type type;
  bool required = true;
  Cardinality cardinality = Cardinality::Single;
};

struct Signature {
  std::vector<PortDesc> inputs;
  std::vector<PortDesc> outputs;
};

struct ValueSlot {
  entt::meta_type type{};
  std::shared_ptr<void> storage{};

  auto has_value() const -> bool { return static_cast<bool>(storage); }

  template <typename T> auto set(T value) -> void {
    auto meta = entt::resolve<T>();
    assert(meta && "Type must be registered before storing");
    type = meta;
    storage = std::make_shared<T>(std::move(value));
  }

  template <typename T> auto get() -> T & {
    auto meta = entt::resolve<T>();
    assert(meta && "Type must be registered before reading");
    assert(type == meta && "ValueSlot type mismatch");
    return *static_cast<T *>(storage.get());
  }

  template <typename T> auto get() const -> const T & {
    auto meta = entt::resolve<T>();
    assert(meta && "Type must be registered before reading");
    assert(type == meta && "ValueSlot type mismatch");
    return *static_cast<const T *>(storage.get());
  }
};

namespace detail {

struct EnvSlot {
  entt::meta_type type{};
  std::shared_ptr<const ValueSlot> value;
};

inline auto load_env_slot(const EnvSlot &slot) -> std::shared_ptr<const ValueSlot> {
  return std::atomic_load_explicit(&slot.value, std::memory_order_acquire);
}

inline auto store_env_slot(EnvSlot &slot, std::shared_ptr<const ValueSlot> value) -> void {
  std::atomic_store_explicit(&slot.value, std::move(value), std::memory_order_release);
}

struct InputRef {
  const ValueSlot *slot = nullptr;
  const EnvSlot *env = nullptr;
  std::shared_ptr<const ValueSlot> *cache = nullptr;
};

} // namespace detail
class InputValues;
class OutputValues {
  friend InputValues;

public:
  OutputValues() : slots_() {}
  explicit OutputValues(std::span<ValueSlot *> slots) : slots_(slots) {}

  auto size() const -> std::size_t { return slots_.size(); }

  auto slot(std::size_t index) -> ValueSlot & {
    assert(index < slots_.size());
    return *slots_[index];
  }

  template <typename T> auto set(std::size_t index, T value) -> void {
    assert(index < slots_.size());
    slots_[index]->set<T>(std::move(value));
  }

private:
  std::span<ValueSlot *> slots_;
};
class InputValues {
public:
  InputValues() : refs_() {}
  explicit InputValues(std::span<const detail::InputRef> refs) : refs_(refs) {}

  auto size() const -> std::size_t { return refs_.size(); }

  auto slot(std::size_t index) const -> const ValueSlot & {
    return resolve(index);
  }

  template <typename T> auto get(std::size_t index) const -> const T & {
    return resolve(index).get<T>();
  }

private:
  auto resolve(std::size_t index) const -> const ValueSlot & {
    assert(index < refs_.size());
    const auto &ref = refs_[index];
    if (ref.slot) {
      return *ref.slot;
    }
    if (ref.env) {
      assert(ref.cache && "env input requires cache slot");
      if (!*ref.cache) {
        auto loaded = detail::load_env_slot(*ref.env);
        assert(loaded && "env slot missing value");
        *ref.cache = std::move(loaded);
      }
      return **ref.cache;
    }
    assert(false && "invalid input reference");
    static ValueSlot empty;
    return empty;
  }

  std::span<const detail::InputRef> refs_;
};

// using rt_sender = stdexec::any_sender_of<ex::set_value_t(),
// ex::set_stopped_t(),
//  ex::set_error_t(std::exception_ptr)>;

struct RequestContext {
  std::unordered_map<std::string, ValueSlot> env;
  std::chrono::steady_clock::time_point deadline{
      std::chrono::steady_clock::time_point::max()};
  std::atomic<bool> cancelled{false};
  trace::TraceContext trace;

  template <typename T> auto set_env(std::string key, T value) -> void {
    ValueSlot slot;
    slot.set<T>(std::move(value));
    env.emplace(std::move(key), std::move(slot));
  }

  auto cancel() -> void { cancelled.store(true, std::memory_order_release); }

  auto is_cancelled() const -> bool {
    return cancelled.load(std::memory_order_acquire);
  }

  auto deadline_exceeded() const -> bool {
    return std::chrono::steady_clock::now() > deadline;
  }

  auto should_stop() const -> bool {
    return is_cancelled() || deadline_exceeded();
  }
};

class RequestContextView {
public:
  RequestContextView() = default;
  RequestContextView(RequestContext &base, std::span<detail::EnvSlot> env_slots,
                     const std::unordered_map<std::string, int> &env_index)
      : base_(&base), env_slots_(env_slots), env_index_(&env_index) {}

  template <typename T>
  auto set_env(std::string_view key, T value) -> Expected<void> {
    if (!env_index_) {
      return tl::unexpected(make_error("env index not available"));
    }
    auto it = env_index_->find(std::string(key));
    if (it == env_index_->end()) {
      return tl::unexpected(make_error(std::format("env key not declared: {}", key)));
    }
    int index = it->second;
    if (index < 0 || static_cast<std::size_t>(index) >= env_slots_.size()) {
      return tl::unexpected(make_error(std::format("env index out of range: {}", key)));
    }
    auto meta = entt::resolve<std::remove_cvref_t<T>>();
    if (!meta) {
      return tl::unexpected(make_error(std::format("env type not registered: {}", key)));
    }
    auto expected = env_slots_[static_cast<std::size_t>(index)].type;
    if (expected && expected != meta) {
      return tl::unexpected(make_error(std::format("env type mismatch: {}", key)));
    }
    auto slot = std::make_shared<ValueSlot>();
    slot->type = meta;
    slot->storage = std::make_shared<std::remove_cvref_t<T>>(std::move(value));
    detail::store_env_slot(env_slots_[static_cast<std::size_t>(index)], std::move(slot));
    return {};
  }

  auto cancel() -> void { base_->cancel(); }

  auto is_cancelled() const -> bool { return base_->is_cancelled(); }

  auto deadline_exceeded() const -> bool { return base_->deadline_exceeded(); }

  auto should_stop() const -> bool { return base_->should_stop(); }

  auto trace() -> trace::TraceContext & { return base_->trace; }

  auto trace() const -> const trace::TraceContext & { return base_->trace; }

private:
  RequestContext *base_ = nullptr;
  std::span<detail::EnvSlot> env_slots_{};
  const std::unordered_map<std::string, int> *env_index_ = nullptr;
};

template <typename T> inline auto register_type(const char *name) -> void {
  entt::meta<T>().type(entt::hashed_string{name});
}

} // namespace sr::engine
