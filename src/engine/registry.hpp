#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "engine/kernel_adapt.hpp"
#include "engine/kernel_types.hpp"

namespace sr::engine {

/// Registry of kernel factories used during graph compilation.
class KernelRegistry {
 public:
  /// Factory returns a fully bound kernel handle from JSON params.
  using FactoryFn = std::function<Expected<KernelHandle>(const Json& params)>;

  KernelRegistry() = default;
  KernelRegistry(const KernelRegistry&) = delete;
  auto operator=(const KernelRegistry&) -> KernelRegistry& = delete;
  KernelRegistry(KernelRegistry&& other) noexcept;
  auto operator=(KernelRegistry&& other) noexcept -> KernelRegistry&;

  /// Register a raw factory for a kernel name.
  auto register_factory(std::string name, FactoryFn factory) -> void;
  /// Register a stateless kernel callable (inputs/outputs come from the DSL).
  template <detail::KernelFn Fn>
  auto register_kernel(std::string name, Fn fn, TaskType task_type = TaskType::Compute) -> void {
    auto inputs = std::vector<std::string>{};
    auto outputs = std::vector<std::string>{};
    auto handle = detail::make_kernel_handle_from_fn(std::move(fn), inputs, outputs, task_type);
    register_factory(std::move(name),
                     [handle = std::move(handle)](const Json&) mutable -> Expected<KernelHandle> {
                       return handle;
                     });
  }

  /// Register a kernel factory that builds a callable from JSON params.
  template <detail::KernelFactory Factory>
  auto register_kernel_with_params(std::string name, Factory factory,
                                   TaskType task_type = TaskType::Compute) -> void {
    auto inputs = std::vector<std::string>{};
    auto outputs = std::vector<std::string>{};
    register_factory(
      std::move(name),
      [factory = std::move(factory), inputs = std::move(inputs), outputs = std::move(outputs),
       task_type](const Json& params) mutable -> Expected<KernelHandle> {
        using FactoryResult = decltype(factory(params));
        if constexpr (detail::is_expected_v<FactoryResult>) {
          auto fn_result = factory(params);
          if (!fn_result) {
            return tl::unexpected(fn_result.error());
          }
          return detail::make_kernel_handle_from_fn(std::move(*fn_result), inputs, outputs, task_type);
        } else {
          auto fn = factory(params);
          return detail::make_kernel_handle_from_fn(std::move(fn), inputs, outputs, task_type);
        }
      });
  }
  /// Lookup a factory by kernel name (nullptr when missing).
  auto find(std::string_view name) const -> std::shared_ptr<const FactoryFn>;

 private:
  mutable std::shared_mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<FactoryFn>> factories_;
};

}  // namespace sr::engine
