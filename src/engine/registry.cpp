#include "engine/registry.hpp"

namespace sr::engine {

KernelRegistry::KernelRegistry(KernelRegistry&& other) noexcept {
  std::unique_lock<std::shared_mutex> lock(other.mutex_);
  factories_ = std::move(other.factories_);
}

auto KernelRegistry::operator=(KernelRegistry&& other) noexcept -> KernelRegistry& {
  if (this == &other) {
    return *this;
  }
  std::scoped_lock lock(mutex_, other.mutex_);
  factories_ = std::move(other.factories_);
  return *this;
}

auto KernelRegistry::register_factory(std::string name, FactoryFn factory) -> void {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  factories_[std::move(name)] = std::make_shared<FactoryFn>(std::move(factory));
}

auto KernelRegistry::find(std::string_view name) const -> std::shared_ptr<const FactoryFn> {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = factories_.find(std::string(name));
  if (it == factories_.end()) {
    return {};
  }
  return it->second;
}

}  // namespace sr::engine
