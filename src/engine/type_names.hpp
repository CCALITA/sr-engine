#pragma once

#include "engine/type_system.hpp"

#include <stdexcept>

namespace sr::engine {

/// Bind a human-readable name to a C++ type.
template <typename T>
struct TypeName {
  static auto set(const char *value) -> void {
    auto &storage = name();
    if (!storage.empty() && storage != value) {
      throw std::runtime_error("type name already registered");
    }
    storage = value;
  }
  static auto get() -> const std::string & { return name(); }

private:
  static auto name() -> std::string & {
    static std::string storage;
    return storage;
  }
};

/// Register a C++ type name with the runtime registry.
template <typename T>
auto register_type(TypeRegistry &registry, const char *name) -> TypeId {
  TypeName<T>::set(name);
  return registry.intern_primitive(name);
}

} // namespace sr::engine
