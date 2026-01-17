#pragma once

#include <atomic>
#include <cassert>
#include <chrono>
#include <concepts>
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
#include "engine/type_system.hpp"
#include "reflection/entt.hpp"
#include "reflection/json.hpp"

namespace sr::engine {
class FrozenEnv;

/// JSON type used by the DSL and kernel parameters.
using Json = nlohmann::json;
namespace ex = stdexec;

/// Hashed identifier for names (ports, outputs).
using NameId = entt::id_type;

/// Hash a runtime name into an entt id.
inline constexpr auto hash_name(std::string_view name) -> NameId {
  return entt::hashed_string::value(name.data(), name.size());
}

/// Port cardinality constraint for compiled signatures.
enum class Cardinality {
  Single,
  Multi,
};

/// Describes a single input/output port type.
struct PortDesc {
  NameId name_id{};
  entt::meta_type type;
  bool required = true;
  Cardinality cardinality = Cardinality::Single;
};

/// Kernel signature extracted from callable types.
struct Signature {
  std::vector<PortDesc> inputs;
  std::vector<PortDesc> outputs;
};

/// Type-erased box holding a registered value instance.
struct ValueBox {
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
    assert(type == meta && "ValueBox type mismatch");
    return *static_cast<T *>(storage.get());
  }

  template <typename T> auto get() const -> const T & {
    auto meta = entt::resolve<T>();
    assert(meta && "Type must be registered before reading");
    assert(type == meta && "ValueBox type mismatch");
    return *static_cast<const T *>(storage.get());
  }
};

class InputValues;

/// Writable view over a kernel's output slots.
class OutputValues {
  friend InputValues;

public:
  OutputValues() : slots_() {}
  explicit OutputValues(std::span<ValueBox *> slots) : slots_(slots) {}

  auto size() const -> std::size_t { return slots_.size(); }

  auto slot(std::size_t index) -> ValueBox & {
    assert(index < slots_.size());
    return *slots_[index];
  }

  template <typename T> auto set(std::size_t index, T value) -> void {
    assert(index < slots_.size());
    slots_[index]->set<T>(std::move(value));
  }

private:
  std::span<ValueBox *> slots_;
};

/// Read-only view over a kernel's input slots.
class InputValues {
public:
  InputValues() : refs_() {}
  explicit InputValues(std::span<const ValueBox *const> refs) : refs_(refs) {}

  auto size() const -> std::size_t { return refs_.size(); }

  auto slot(std::size_t index) const -> const ValueBox & {
    assert(index < refs_.size());
    const auto *slot = refs_[index];
    if (slot) {
      return *slot;
    }
    assert(false && "invalid input reference");
    static ValueBox empty;
    return empty;
  }

  template <typename T> auto get(std::size_t index) const -> const T & {
    return slot(index).get<T>();
  }

private:
  std::span<const ValueBox *const> refs_;
};
/// Per-request mutable state (env values, cancellation, tracing).
struct RequestContext {
  std::unordered_map<std::string, ValueBox> env;
  std::chrono::steady_clock::time_point deadline{
      std::chrono::steady_clock::time_point::max()};
  std::atomic<bool> cancelled{false};
  trace::TraceContext trace;
  const class FrozenEnv *frozen_env = nullptr;

  template <typename T> auto set_env(std::string key, T value) -> void {
    ValueBox slot;
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

/// Register a C++ type for use in signatures and value slots.

template <typename T>
  requires std::semiregular<T>
inline auto register_type(const char *name) -> void {

  entt::meta<T>().type(entt::hashed_string{name});
}

} // namespace sr::engine
