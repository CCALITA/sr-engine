#pragma once

#include <functional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "engine/kernel_adapt.hpp"
#include "engine/kernel_types.hpp"

namespace sr::engine {

class KernelRegistry {
 public:
  using FactoryFn = std::function<Expected<KernelHandle>(const Json& params)>;

  auto register_factory(std::string name, FactoryFn factory) -> void;
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
  auto find(std::string_view name) const -> const FactoryFn*;

 private:
  std::unordered_map<std::string, FactoryFn> factories_;
};

}  // namespace sr::engine
