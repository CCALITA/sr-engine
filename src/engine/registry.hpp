#pragma once

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "engine/kernel_adapt.hpp"
#include "engine/kernel_types.hpp"

namespace sr::engine {

/// Signature plus erased kernel handle produced by a registry factory.
struct KernelSpec {
  Signature signature;
  KernelHandle handle;
};

namespace detail {

/// Build a kernel spec from a callable and optional port names.
template <typename Fn>
auto make_kernel_spec_from_fn(Fn fn, const std::vector<std::string>& input_names,
                              const std::vector<std::string>& output_names)
    -> Expected<KernelSpec> {
  auto signature_result = build_signature<Fn>(input_names, output_names);
  if (!signature_result) {
    return tl::unexpected(signature_result.error());
  }
  KernelSpec spec;
  spec.signature = std::move(*signature_result);
  spec.handle = make_kernel_handle_from_fn(std::move(fn));
  return spec;
}

} // namespace detail

/// Registry of kernel factories used during graph compilation.
class KernelRegistry {
 public:
  /// Factory returns a fully bound kernel spec from JSON params.
  class Factory {
   public:
    Factory() = default;

    template <typename Fn>
      requires(std::is_invocable_r_v<Expected<KernelSpec>, Fn, const Json&> &&
               !std::is_same_v<std::decay_t<Fn>, Factory>)
    Factory(Fn fn) {
      using FnT = std::decay_t<Fn>;
      auto holder = std::make_shared<FnT>(std::move(fn));
      state_ = std::move(holder);
      invoke_ = [](void* ptr, const Json& params) -> Expected<KernelSpec> {
        return (*static_cast<FnT*>(ptr))(params);
      };
    }

    auto operator()(const Json& params) const -> Expected<KernelSpec> {
      return invoke_(state_.get(), params);
    }

   private:
    using InvokeFn = Expected<KernelSpec> (*)(void*, const Json&);
    std::shared_ptr<void> state_;
    InvokeFn invoke_ = nullptr;
  };

  KernelRegistry() = default;
  KernelRegistry(const KernelRegistry&) = delete;
  auto operator=(const KernelRegistry&) -> KernelRegistry& = delete;
  KernelRegistry(KernelRegistry&& other) noexcept;
  auto operator=(KernelRegistry&& other) noexcept -> KernelRegistry&;

  /// Register a raw factory for a kernel name.
  auto register_factory(std::string name, Factory factory) -> void;
  /// Register a stateless kernel callable (must be noexcept; inputs/outputs come from the DSL).
  template <detail::KernelFn Fn>
  auto register_kernel(std::string name, Fn fn) -> void {
    auto inputs = std::vector<std::string>{};
    auto outputs = std::vector<std::string>{};
    auto spec =
        detail::make_kernel_spec_from_fn(std::move(fn), inputs, outputs);
    register_factory(std::move(name),
                     [spec = std::move(spec)](const Json&) mutable -> Expected<KernelSpec> {
                       return spec;
                     });
  }

  /// Register a kernel factory that builds a callable from JSON params (callable must be noexcept).
  template <detail::KernelFactory Factory>
  auto register_kernel_with_params(std::string name, Factory factory) -> void {
    auto inputs = std::vector<std::string>{};
    auto outputs = std::vector<std::string>{};
    register_factory(
      std::move(name),
      [factory = std::move(factory), inputs = std::move(inputs),
       outputs = std::move(outputs)](const Json& params) mutable -> Expected<KernelSpec> {
        using FactoryResult = decltype(factory(params));
        if constexpr (detail::is_expected_v<FactoryResult>) {
          auto fn_result = factory(params);
          if (!fn_result) {
            return tl::unexpected(fn_result.error());
          }
          return detail::make_kernel_spec_from_fn(std::move(*fn_result), inputs,
                                                  outputs);
        } else {
          auto fn = factory(params);
          return detail::make_kernel_spec_from_fn(std::move(fn), inputs, outputs);
        }
      });
  }
  /// Lookup a factory by kernel name (nullptr when missing).
  auto find(std::string_view name) const -> std::shared_ptr<const Factory>;

 private:
  mutable std::shared_mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<Factory>> factories_;
};

}  // namespace sr::engine
