#pragma once

#include <functional>
#include <exception>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <exec/any_sender_of.hpp>
#include <stdexec/execution.hpp>

#include "engine/error.hpp"
#include "engine/types.hpp"

namespace sr::engine::dag {

struct get_signature_t {
  template <class K>
  auto operator()(const K& kernel) const noexcept -> decltype(tag_invoke(*this, kernel)) {
    return tag_invoke(*this, kernel);
  }
};

struct execute_t {
  template <class K>
  auto operator()(K& kernel, RequestContext& ctx, const InputValues& inputs, OutputValues& outputs) const
    -> decltype(tag_invoke(*this, kernel, ctx, inputs, outputs)) {
    return tag_invoke(*this, kernel, ctx, inputs, outputs);
  }
};

inline constexpr get_signature_t get_signature{};
inline constexpr execute_t execute{};

}  // namespace sr::engine::dag

namespace sr::engine {

using ErasedSender = exec::any_receiver_ref<stdexec::completion_signatures<
  stdexec::set_value_t(),
  stdexec::set_error_t(std::exception_ptr),
  stdexec::set_stopped_t()>>::any_sender<>;

struct KernelHandle {
  Signature signature;
  std::shared_ptr<void> instance;
  ErasedSender (*exec)(void*, RequestContext&, const InputValues&, OutputValues&);
};

template <typename K>
auto exec_adapter(void* ptr, RequestContext& ctx, const InputValues& inputs, OutputValues& outputs) -> ErasedSender {
  auto& kernel = *static_cast<K*>(ptr);
  auto sender = dag::execute(kernel, ctx, inputs, outputs);
  return ErasedSender{std::move(sender)};
}

template <typename K>
auto make_kernel_handle(K kernel) -> KernelHandle {
  KernelHandle handle;
  handle.signature = dag::get_signature(kernel);
  handle.instance = std::make_shared<K>(std::move(kernel));
  handle.exec = &exec_adapter<K>;
  return handle;
}

class KernelRegistry {
 public:
  using FactoryFn = std::function<Expected<KernelHandle>(const Json& params)>;

  auto register_kernel(std::string name, FactoryFn factory) -> void;
  auto find(std::string_view name) const -> const FactoryFn*;

 private:
  std::unordered_map<std::string, FactoryFn> factories_;
};

}  // namespace sr::engine
