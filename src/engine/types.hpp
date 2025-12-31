#pragma once

#include <atomic>
#include <cassert>
#include <chrono>
#include <exec/any_sender_of.hpp>
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
  InputValues() : slots_() {}
  explicit InputValues(std::span<const ValueSlot *const> slots)
      : slots_(slots) {}

  auto size() const -> std::size_t { return slots_.size(); }

  auto slot(std::size_t index) const -> const ValueSlot & {
    assert(index < slots_.size());
    return *slots_[index];
  }

  template <typename T> auto get(std::size_t index) const -> const T & {
    assert(index < slots_.size());
    return slots_[index]->get<T>();
  }

private:
  std::span<const ValueSlot *const> slots_;
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

template <typename T> inline auto register_type(const char *name) -> void {
  entt::meta<T>().type(entt::hashed_string{name});
}

} // namespace sr::engine
