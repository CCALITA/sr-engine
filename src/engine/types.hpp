#pragma once

#include <atomic>
#include <cassert>
#include <chrono>
#include <concepts>
#include <cstdint>
#include <exec/any_sender_of.hpp>
#include <format>
#include <memory>
#include <span>
#include <stdexec/execution.hpp>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <variant>
#include <vector>

#include "engine/error.hpp"
#include "engine/trace.hpp"
#include "engine/type_names.hpp"
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
  TypeId type_id{};
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
  struct InlineBool {
    bool value{};
  };
  struct InlineChar {
    char value{};
  };
  struct InlineSignedChar {
    signed char value{};
  };
  struct InlineUnsignedChar {
    unsigned char value{};
  };
  struct InlineShort {
    short value{};
  };
  struct InlineUnsignedShort {
    unsigned short value{};
  };
  struct InlineInt {
    int value{};
  };
  struct InlineUnsignedInt {
    unsigned int value{};
  };
  struct InlineLong {
    long value{};
  };
  struct InlineUnsignedLong {
    unsigned long value{};
  };
  struct InlineLongLong {
    long long value{};
  };
  struct InlineUnsignedLongLong {
    unsigned long long value{};
  };
  struct InlineInt8 {
    std::int8_t value{};
  };
  struct InlineUInt8 {
    std::uint8_t value{};
  };
  struct InlineInt16 {
    std::int16_t value{};
  };
  struct InlineUInt16 {
    std::uint16_t value{};
  };
  struct InlineInt32 {
    std::int32_t value{};
  };
  struct InlineUInt32 {
    std::uint32_t value{};
  };
  struct InlineInt64 {
    std::int64_t value{};
  };
  struct InlineUInt64 {
    std::uint64_t value{};
  };
  struct InlineFloat {
    float value{};
  };
  struct InlineDouble {
    double value{};
  };
  struct InlineLongDouble {
    long double value{};
  };
  struct InlineString {
    std::string value;
  };
  struct InlineTimePoint {
    std::chrono::steady_clock::time_point value{};
  };

  using InlineStorage = std::variant<
      std::monostate, InlineBool, InlineChar, InlineSignedChar,
      InlineUnsignedChar, InlineShort, InlineUnsignedShort, InlineInt,
      InlineUnsignedInt, InlineLong, InlineUnsignedLong, InlineLongLong,
      InlineUnsignedLongLong, InlineInt8, InlineUInt8, InlineInt16, InlineUInt16,
      InlineInt32, InlineUInt32, InlineInt64, InlineUInt64, InlineFloat,
      InlineDouble, InlineLongDouble, InlineString, InlineTimePoint,
      std::shared_ptr<void>>;

  TypeId type_id{};
  InlineStorage storage{};

  auto has_value() const -> bool {
    if (std::holds_alternative<std::monostate>(storage)) {
      return false;
    }
    if (std::holds_alternative<std::shared_ptr<void>>(storage)) {
      return static_cast<bool>(std::get<std::shared_ptr<void>>(storage));
    }
    return true;
  }

  template <typename Base> static constexpr bool is_inline_type() {
    using T = std::remove_cvref_t<Base>;
    return std::is_same_v<T, bool> || std::is_same_v<T, char> ||
           std::is_same_v<T, signed char> || std::is_same_v<T, unsigned char> ||
           std::is_same_v<T, std::int8_t> || std::is_same_v<T, std::uint8_t> ||
           std::is_same_v<T, short> || std::is_same_v<T, unsigned short> ||
           std::is_same_v<T, std::int16_t> || std::is_same_v<T, std::uint16_t> ||
           std::is_same_v<T, int> || std::is_same_v<T, unsigned int> ||
           std::is_same_v<T, std::int32_t> || std::is_same_v<T, std::uint32_t> ||
           std::is_same_v<T, long> || std::is_same_v<T, unsigned long> ||
           std::is_same_v<T, long long> ||
           std::is_same_v<T, unsigned long long> ||
           std::is_same_v<T, std::int64_t> || std::is_same_v<T, std::uint64_t> ||
           std::is_same_v<T, float> || std::is_same_v<T, double> ||
           std::is_same_v<T, long double> || std::is_same_v<T, std::string> ||
           std::is_same_v<T, std::chrono::steady_clock::time_point>;
  }

  template <typename Base>
  static auto store_inline(Base value) -> InlineStorage {
    using T = std::remove_cvref_t<Base>;
    if constexpr (std::is_same_v<T, bool>) {
      return InlineBool{value};
    } else if constexpr (std::is_same_v<T, char>) {
      return InlineChar{value};
    } else if constexpr (std::is_same_v<T, signed char>) {
      return InlineSignedChar{value};
    } else if constexpr (std::is_same_v<T, unsigned char>) {
      return InlineUnsignedChar{value};
    } else if constexpr (std::is_same_v<T, std::int8_t>) {
      return InlineInt8{value};
    } else if constexpr (std::is_same_v<T, std::uint8_t>) {
      return InlineUInt8{value};
    } else if constexpr (std::is_same_v<T, short>) {
      return InlineShort{value};
    } else if constexpr (std::is_same_v<T, unsigned short>) {
      return InlineUnsignedShort{value};
    } else if constexpr (std::is_same_v<T, std::int16_t>) {
      return InlineInt16{value};
    } else if constexpr (std::is_same_v<T, std::uint16_t>) {
      return InlineUInt16{value};
    } else if constexpr (std::is_same_v<T, int>) {
      return InlineInt{value};
    } else if constexpr (std::is_same_v<T, unsigned int>) {
      return InlineUnsignedInt{value};
    } else if constexpr (std::is_same_v<T, std::int32_t>) {
      return InlineInt32{value};
    } else if constexpr (std::is_same_v<T, std::uint32_t>) {
      return InlineUInt32{value};
    } else if constexpr (std::is_same_v<T, long>) {
      return InlineLong{value};
    } else if constexpr (std::is_same_v<T, unsigned long>) {
      return InlineUnsignedLong{value};
    } else if constexpr (std::is_same_v<T, long long>) {
      return InlineLongLong{value};
    } else if constexpr (std::is_same_v<T, unsigned long long>) {
      return InlineUnsignedLongLong{value};
    } else if constexpr (std::is_same_v<T, std::int64_t>) {
      return InlineInt64{value};
    } else if constexpr (std::is_same_v<T, std::uint64_t>) {
      return InlineUInt64{value};
    } else if constexpr (std::is_same_v<T, float>) {
      return InlineFloat{value};
    } else if constexpr (std::is_same_v<T, double>) {
      return InlineDouble{value};
    } else if constexpr (std::is_same_v<T, long double>) {
      return InlineLongDouble{value};
    } else if constexpr (std::is_same_v<T, std::string>) {
      return InlineString{std::move(value)};
    } else if constexpr (std::is_same_v<T, std::chrono::steady_clock::time_point>) {
      return InlineTimePoint{value};
    } else {
      return InlineStorage{std::monostate{}};
    }
  }

    template <typename Base>
  static auto read_inline(InlineStorage &storage) -> Base & {
    using T = std::remove_cvref_t<Base>;
    if constexpr (std::is_same_v<T, bool>) {
      return std::get<InlineBool>(storage).value;
    } else if constexpr (std::is_same_v<T, char>) {
      return std::get<InlineChar>(storage).value;
    } else if constexpr (std::is_same_v<T, signed char>) {
      return std::get<InlineSignedChar>(storage).value;
    } else if constexpr (std::is_same_v<T, unsigned char>) {
      return std::get<InlineUnsignedChar>(storage).value;
    } else if constexpr (std::is_same_v<T, short>) {
      return std::get<InlineShort>(storage).value;
    } else if constexpr (std::is_same_v<T, unsigned short>) {
      return std::get<InlineUnsignedShort>(storage).value;
    } else if constexpr (std::is_same_v<T, int>) {
      return std::get<InlineInt>(storage).value;
    } else if constexpr (std::is_same_v<T, unsigned int>) {
      return std::get<InlineUnsignedInt>(storage).value;
    } else if constexpr (std::is_same_v<T, long>) {
      return std::get<InlineLong>(storage).value;
    } else if constexpr (std::is_same_v<T, unsigned long>) {
      return std::get<InlineUnsignedLong>(storage).value;
    } else if constexpr (std::is_same_v<T, long long>) {
      return std::get<InlineLongLong>(storage).value;
    } else if constexpr (std::is_same_v<T, unsigned long long>) {
      return std::get<InlineUnsignedLongLong>(storage).value;
    } else if constexpr (std::is_same_v<T, std::int8_t>) {
      return std::get<InlineInt8>(storage).value;
    } else if constexpr (std::is_same_v<T, std::uint8_t>) {
      return std::get<InlineUInt8>(storage).value;
    } else if constexpr (std::is_same_v<T, std::int16_t>) {
      return std::get<InlineInt16>(storage).value;
    } else if constexpr (std::is_same_v<T, std::uint16_t>) {
      return std::get<InlineUInt16>(storage).value;
    } else if constexpr (std::is_same_v<T, std::int32_t>) {
      return std::get<InlineInt32>(storage).value;
    } else if constexpr (std::is_same_v<T, std::uint32_t>) {
      return std::get<InlineUInt32>(storage).value;
    } else if constexpr (std::is_same_v<T, std::int64_t>) {
      return std::get<InlineInt64>(storage).value;
    } else if constexpr (std::is_same_v<T, std::uint64_t>) {
      return std::get<InlineUInt64>(storage).value;
    } else if constexpr (std::is_same_v<T, float>) {
      return std::get<InlineFloat>(storage).value;
    } else if constexpr (std::is_same_v<T, double>) {
      return std::get<InlineDouble>(storage).value;
    } else if constexpr (std::is_same_v<T, long double>) {
      return std::get<InlineLongDouble>(storage).value;
    } else if constexpr (std::is_same_v<T, std::string>) {
      return std::get<InlineString>(storage).value;
    } else {
      return std::get<InlineTimePoint>(storage).value;
    }
  }

  template <typename Base>
  static auto read_inline(const InlineStorage &storage) -> const Base & {
    using T = std::remove_cvref_t<Base>;
    if constexpr (std::is_same_v<T, bool>) {
      return std::get<InlineBool>(storage).value;
    } else if constexpr (std::is_same_v<T, char>) {
      return std::get<InlineChar>(storage).value;
    } else if constexpr (std::is_same_v<T, signed char>) {
      return std::get<InlineSignedChar>(storage).value;
    } else if constexpr (std::is_same_v<T, unsigned char>) {
      return std::get<InlineUnsignedChar>(storage).value;
    } else if constexpr (std::is_same_v<T, short>) {
      return std::get<InlineShort>(storage).value;
    } else if constexpr (std::is_same_v<T, unsigned short>) {
      return std::get<InlineUnsignedShort>(storage).value;
    } else if constexpr (std::is_same_v<T, int>) {
      return std::get<InlineInt>(storage).value;
    } else if constexpr (std::is_same_v<T, unsigned int>) {
      return std::get<InlineUnsignedInt>(storage).value;
    } else if constexpr (std::is_same_v<T, long>) {
      return std::get<InlineLong>(storage).value;
    } else if constexpr (std::is_same_v<T, unsigned long>) {
      return std::get<InlineUnsignedLong>(storage).value;
    } else if constexpr (std::is_same_v<T, long long>) {
      return std::get<InlineLongLong>(storage).value;
    } else if constexpr (std::is_same_v<T, unsigned long long>) {
      return std::get<InlineUnsignedLongLong>(storage).value;
    } else if constexpr (std::is_same_v<T, std::int8_t>) {
      return std::get<InlineInt8>(storage).value;
    } else if constexpr (std::is_same_v<T, std::uint8_t>) {
      return std::get<InlineUInt8>(storage).value;
    } else if constexpr (std::is_same_v<T, std::int16_t>) {
      return std::get<InlineInt16>(storage).value;
    } else if constexpr (std::is_same_v<T, std::uint16_t>) {
      return std::get<InlineUInt16>(storage).value;
    } else if constexpr (std::is_same_v<T, std::int32_t>) {
      return std::get<InlineInt32>(storage).value;
    } else if constexpr (std::is_same_v<T, std::uint32_t>) {
      return std::get<InlineUInt32>(storage).value;
    } else if constexpr (std::is_same_v<T, std::int64_t>) {
      return std::get<InlineInt64>(storage).value;
    } else if constexpr (std::is_same_v<T, std::uint64_t>) {
      return std::get<InlineUInt64>(storage).value;
    } else if constexpr (std::is_same_v<T, float>) {
      return std::get<InlineFloat>(storage).value;
    } else if constexpr (std::is_same_v<T, double>) {
      return std::get<InlineDouble>(storage).value;
    } else if constexpr (std::is_same_v<T, long double>) {
      return std::get<InlineLongDouble>(storage).value;
    } else if constexpr (std::is_same_v<T, std::string>) {
      return std::get<InlineString>(storage).value;
    } else {
      return std::get<InlineTimePoint>(storage).value;
    }
  }

  template <typename T> auto set(T value) -> void {
    using Base = std::remove_cvref_t<T>;
    const auto expected = TypeName<Base>::id();
    assert(expected != TypeId{} && "Type must be registered before storing");
    type_id = expected;
    if constexpr (std::is_same_v<Base, Json>) {
      storage = std::make_shared<Json>(std::move(value));
    } else if constexpr (is_inline_type<Base>()) {
      storage = store_inline(std::move(value));
    } else {
      storage = std::make_shared<Base>(std::move(value));
    }
  }

  template <typename T> auto get() -> T & {
    using Base = std::remove_cvref_t<T>;
    const auto expected = TypeName<Base>::id();
    assert(expected != TypeId{} && "Type must be registered before reading");
    assert(type_id == expected && "ValueBox type mismatch");
    if constexpr (std::is_same_v<Base, Json>) {
      return *static_cast<T *>(std::get<std::shared_ptr<void>>(storage).get());
    } else if constexpr (is_inline_type<Base>()) {
      return read_inline<Base>(storage);
    } else {
      return *static_cast<T *>(std::get<std::shared_ptr<void>>(storage).get());
    }
  }

  template <typename T> auto get() const -> const T & {
    using Base = std::remove_cvref_t<T>;
    const auto expected = TypeName<Base>::id();
    assert(expected != TypeId{} && "Type must be registered before reading");
    assert(type_id == expected && "ValueBox type mismatch");
    if constexpr (std::is_same_v<Base, Json>) {
      return *static_cast<const T *>(
          std::get<std::shared_ptr<void>>(storage).get());
    } else if constexpr (is_inline_type<Base>()) {
      return read_inline<Base>(storage);
    } else {
      return *static_cast<const T *>(
          std::get<std::shared_ptr<void>>(storage).get());
    }
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


} // namespace sr::engine
