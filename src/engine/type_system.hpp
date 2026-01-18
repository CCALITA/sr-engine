#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>

namespace sr::engine {

using TypeId = std::uint64_t;

struct TypeFingerprint {
  std::array<std::uint8_t, 16> bytes{};
};

struct TypeInfo {
  std::string name;
  TypeId id;
  TypeFingerprint fp;
};

class TypeRegistry {
public:
  struct FunctionAttrs {
    bool noexcept_ = false;
    bool async = false;
    bool has_ctx = false;
  };

  static auto create() -> std::shared_ptr<TypeRegistry>;

  virtual ~TypeRegistry() = default;

  virtual auto intern_primitive(std::string_view name) -> TypeId = 0;
  virtual auto intern_function(std::span<const TypeId> inputs,
                               std::span<const TypeId> outputs,
                               FunctionAttrs attrs) -> TypeId = 0;
  virtual auto lookup(TypeId id) const -> const TypeInfo * = 0;
};

} // namespace sr::engine
