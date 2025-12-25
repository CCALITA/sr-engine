#include "engine/registry.hpp"

namespace sr::engine {

auto KernelRegistry::register_kernel(std::string name, FactoryFn factory) -> void {
  factories_[std::move(name)] = std::move(factory);
}

auto KernelRegistry::find(std::string_view name) const -> const FactoryFn* {
  auto it = factories_.find(std::string(name));
  if (it == factories_.end()) {
    return nullptr;
  }
  return &it->second;
}

}  // namespace sr::engine
