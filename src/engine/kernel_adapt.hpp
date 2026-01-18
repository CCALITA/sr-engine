#pragma once

#include <concepts>
#include <format>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "engine/error.hpp"
#include "engine/kernel_types.hpp"
#include "engine/types.hpp"

namespace sr::engine::detail {

template <typename T> struct function_traits;

template <typename R, typename... Args> struct function_traits<R(Args...)> {
  using result_type = R;
  using args_tuple = std::tuple<Args...>;
};

template <typename R, typename... Args>
struct function_traits<R (*)(Args...)> : function_traits<R(Args...)> {};

template <typename C, typename R, typename... Args>
struct function_traits<R (C::*)(Args...)> : function_traits<R(Args...)> {};

template <typename C, typename R, typename... Args>
struct function_traits<R (C::*)(Args...) const> : function_traits<R(Args...)> {
};

template <typename R, typename... Args>
struct function_traits<R(Args...) noexcept> : function_traits<R(Args...)> {};

template <typename R, typename... Args>
struct function_traits<R (*)(Args...) noexcept> : function_traits<R(Args...)> {
};

template <typename C, typename R, typename... Args>
struct function_traits<R (C::*)(Args...) noexcept>
    : function_traits<R(Args...)> {};

template <typename C, typename R, typename... Args>
struct function_traits<R (C::*)(Args...) const noexcept>
    : function_traits<R(Args...)> {};

template <typename T, typename = void> struct callable_traits;

template <typename T>
struct callable_traits<
    T, std::enable_if_t<
           std::is_function_v<std::remove_pointer_t<std::remove_cvref_t<T>>> ||
           std::is_member_function_pointer_v<std::remove_cvref_t<T>>>>
    : function_traits<std::remove_cvref_t<T>> {};

template <typename T>
struct callable_traits<T, std::void_t<decltype(&T::operator())>>
    : callable_traits<decltype(&T::operator())> {};

template <typename T> struct expected_traits {
  using value_type = T;
  static constexpr bool is_expected = false;
};

template <typename T> struct expected_traits<Expected<T>> {
  using value_type = T;
  static constexpr bool is_expected = true;
};

template <typename T>
inline constexpr bool is_expected_v =
    expected_traits<std::remove_cvref_t<T>>::is_expected;

template <template <class...> class Template, class T>
struct is_specialization_of : std::false_type {};

template <template <class...> class Template, class... Args>
struct is_specialization_of<Template, Template<Args...>> : std::true_type {};

template <template <class...> class Template, class T>
inline constexpr bool is_specialization_of_v =
    is_specialization_of<Template, std::remove_cvref_t<T>>::value;

template <template <class...> class Template, class T>
concept specialization_of = is_specialization_of_v<Template, T>;

template <typename T>
concept is_optional = specialization_of<std::optional, T>;

template <typename T>
concept is_reference_wrapper = specialization_of<std::reference_wrapper, T>;

template <typename T, typename = void>
struct is_tuple_like : std::false_type {};

template <typename T>
struct is_tuple_like<T, std::void_t<decltype(std::tuple_size<T>::value)>>
    : std::true_type {};

template <typename T>
inline constexpr bool is_tuple_like_v =
    is_tuple_like<std::remove_cvref_t<T>>::value;

template <typename T>
using port_type_t = std::remove_cvref_t<std::unwrap_reference_t<T>>;

template <typename T> struct optional_value {
  using type = std::remove_cvref_t<T>;
};

template <typename T>
  requires is_optional<T>
struct optional_value<T> {
  using type = typename std::remove_cvref_t<T>::value_type;
};

template <typename T> using optional_value_t = typename optional_value<T>::type;

template <typename T>
using output_port_type_t = port_type_t<optional_value_t<T>>;

template <typename T> struct input_arg_traits {
  static constexpr bool optional = is_optional<T>;
  using port_type = port_type_t<optional_value_t<T>>;
};

template <typename T>
using output_tuple_t = std::conditional_t<
    std::is_same_v<std::remove_cvref_t<T>, void>, std::tuple<>,
    std::conditional_t<is_tuple_like_v<T>, std::remove_cvref_t<T>,
                       std::tuple<std::remove_cvref_t<T>>>>;

template <typename T>
using call_arg_type_t =
    std::conditional_t<is_optional<T>, std::remove_cvref_t<T>, T>;

template <typename T>
inline constexpr bool is_mutable_lref_v =
    std::is_lvalue_reference_v<T> &&
    !std::is_const_v<std::remove_reference_t<T>>;

template <typename T>
inline constexpr bool is_request_context_v =
    std::is_same_v<std::remove_cvref_t<T>, RequestContext>;

template <typename Tuple> struct tuple_tail;

template <typename T, typename... Rest>
struct tuple_tail<std::tuple<T, Rest...>> {
  using type = std::tuple<Rest...>;
};

template <> struct tuple_tail<std::tuple<>> {
  using type = std::tuple<>;
};

template <typename Tuple> struct first_is_context : std::false_type {};

template <typename T, typename... Rest>
struct first_is_context<std::tuple<T, Rest...>>
    : std::bool_constant<is_request_context_v<T>> {};

template <typename Fn>
concept KernelCallable = requires {
  typename callable_traits<Fn>::result_type;
  typename callable_traits<Fn>::args_tuple;
};

template <typename Fn> struct kernel_traits {
  using callable = callable_traits<Fn>;
  using args_tuple = typename callable::args_tuple;
  static constexpr bool has_ctx = first_is_context<args_tuple>::value;
  using input_tuple =
      std::conditional_t<has_ctx, typename tuple_tail<args_tuple>::type,
                         args_tuple>;
  using result_type = typename callable::result_type;
  using value_type = typename expected_traits<result_type>::value_type;
  using output_tuple = output_tuple_t<value_type>;
  static constexpr std::size_t input_count = std::tuple_size_v<input_tuple>;
  static constexpr std::size_t output_count = std::tuple_size_v<output_tuple>;
};

template <typename T>
inline constexpr bool is_valid_input_arg_v =
    !is_request_context_v<std::remove_cvref_t<T>> &&
    !std::is_rvalue_reference_v<T> && !is_mutable_lref_v<T>;

template <typename T>
inline constexpr bool is_valid_output_arg_v =
    !std::is_reference_v<T> && !is_reference_wrapper<T>;

template <typename Tuple, std::size_t... I>
constexpr bool input_tuple_ok(std::index_sequence<I...>) {
  return (is_valid_input_arg_v<std::tuple_element_t<I, Tuple>> && ...);
}

template <typename Tuple> constexpr bool input_tuple_ok() {
  return input_tuple_ok<Tuple>(
      std::make_index_sequence<std::tuple_size_v<Tuple>>{});
}

template <typename Tuple, std::size_t... I>
constexpr bool output_tuple_ok(std::index_sequence<I...>) {
  return (is_valid_output_arg_v<std::tuple_element_t<I, Tuple>> && ...);
}

template <typename Tuple> constexpr bool output_tuple_ok() {
  return output_tuple_ok<Tuple>(
      std::make_index_sequence<std::tuple_size_v<Tuple>>{});
}

template <typename Fn> constexpr bool kernel_inputs_ok() {
  return input_tuple_ok<typename kernel_traits<Fn>::input_tuple>();
}

template <typename Fn> constexpr bool kernel_outputs_ok() {
  return output_tuple_ok<typename kernel_traits<Fn>::output_tuple>();
}

template <typename Fn, typename Tuple, std::size_t... I>
constexpr bool kernel_nothrow_invocable(std::index_sequence<I...>) {
  if constexpr (kernel_traits<Fn>::has_ctx) {
    return std::is_nothrow_invocable_v<
        Fn, RequestContext &,
        call_arg_type_t<std::tuple_element_t<I, Tuple>>...>;
  } else {
    return std::is_nothrow_invocable_v<
        Fn, call_arg_type_t<std::tuple_element_t<I, Tuple>>...>;
  }
}

template <typename Fn> constexpr bool kernel_nothrow_ok() {
  using input_tuple = typename kernel_traits<Fn>::input_tuple;
  return kernel_nothrow_invocable<Fn, input_tuple>(
      std::make_index_sequence<std::tuple_size_v<input_tuple>>{});
}

template <typename Fn>
concept KernelFn = KernelCallable<Fn> && kernel_inputs_ok<Fn>() &&
                   kernel_outputs_ok<Fn>() && kernel_nothrow_ok<Fn>();

template <typename Factory>
using factory_result_t =
    decltype(std::declval<Factory &>()(std::declval<const Json &>()));

template <typename Factory>
using factory_functor_t = std::conditional_t<
    is_expected_v<factory_result_t<Factory>>,
    typename expected_traits<factory_result_t<Factory>>::value_type,
    factory_result_t<Factory>>;

template <typename Factory>
concept KernelFactory = std::invocable<Factory, const Json &> &&
                        KernelFn<factory_functor_t<Factory>>;

template <typename Arg>
auto load_input(const InputValues &inputs, std::size_t index)
    -> call_arg_type_t<Arg> {
  static_assert(!is_mutable_lref_v<Arg>,
                "Kernel inputs must be values or const references.");
  if constexpr (is_optional<Arg>) {
    using Opt = std::remove_cvref_t<Arg>;
    using Inner = typename Opt::value_type;
    const auto &slot = inputs.slot(index);
    if (!slot.has_value()) {
      return Opt{};
    }
    if constexpr (is_reference_wrapper<Inner>) {
      using Wrapped = typename Inner::type;
      static_assert(std::is_const_v<std::remove_reference_t<Wrapped>>,
                    "Optional reference inputs must use "
                    "std::reference_wrapper<const T>.");
      using Base = std::remove_cv_t<std::remove_reference_t<Wrapped>>;
      return Opt{std::cref(slot.get<Base>())};
    } else {
      using Base = std::remove_cv_t<std::remove_reference_t<Inner>>;
      return Opt{slot.get<Base>()};
    }
  } else if constexpr (std::is_reference_v<Arg>) {
    using Base = std::remove_cv_t<std::remove_reference_t<Arg>>;
    return inputs.get<Base>(index);
  } else {
    using Base = std::remove_cvref_t<Arg>;
    return Base(inputs.get<Base>(index));
  }
}

template <typename Tuple, std::size_t... I>
auto read_inputs(const InputValues &inputs, std::index_sequence<I...>)
    -> std::tuple<call_arg_type_t<std::tuple_element_t<I, Tuple>>...> {
  using Result = std::tuple<call_arg_type_t<std::tuple_element_t<I, Tuple>>...>;
  return Result(load_input<std::tuple_element_t<I, Tuple>>(inputs, I)...);
}

template <typename Tuple>
auto read_inputs(const InputValues &inputs) -> decltype(auto) {
  return read_inputs<Tuple>(
      inputs, std::make_index_sequence<std::tuple_size_v<Tuple>>{});
}

template <typename Tuple, std::size_t... I>
auto write_tuple_outputs(OutputValues &outputs, Tuple &&tuple,
                         std::index_sequence<I...>) -> void {
  (
      [&] {
        using Arg = std::tuple_element_t<I, std::remove_cvref_t<Tuple>>;
        auto &&value = std::get<I>(std::forward<Tuple>(tuple));
        if constexpr (is_optional<Arg>) {
          if (!value) {
            return;
          }
          outputs.set<output_port_type_t<Arg>>(I, *value);
        } else {
          outputs.set<output_port_type_t<Arg>>(
              I, std::forward<decltype(value)>(value));
        }
      }(),
      ...);
}

template <typename R>
auto write_outputs(OutputValues &outputs, R &&value) -> Expected<void> {
  using Raw = std::remove_cvref_t<R>;
  if constexpr (std::is_same_v<Raw, void>) {
    return {};
  } else if constexpr (is_tuple_like_v<Raw>) {
    write_tuple_outputs(outputs, std::forward<R>(value),
                        std::make_index_sequence<std::tuple_size_v<Raw>>{});
    return {};
  } else if constexpr (is_optional<Raw>) {
    if (!value) {
      return {};
    }
    outputs.set<output_port_type_t<Raw>>(0, *value);
    return {};
  } else {
    outputs.set<output_port_type_t<Raw>>(0, std::forward<R>(value));
    return {};
  }
}

template <typename R>
auto handle_return(OutputValues &outputs, R &&value) -> Expected<void> {
  using Raw = std::remove_cvref_t<R>;
  if constexpr (is_expected_v<Raw>) {
    if (!value) {
      return tl::unexpected(value.error());
    }
    if constexpr (std::is_same_v<typename expected_traits<Raw>::value_type,
                                 void>) {
      return {};
    } else {
      return write_outputs(outputs, std::move(*value));
    }
  } else {
    return write_outputs(outputs, std::forward<R>(value));
  }
}

template <bool HasCtx, typename Fn, typename Tuple>
auto apply_kernel(Fn &fn, RequestContext &ctx, Tuple &&args) -> decltype(auto) {
  if constexpr (HasCtx) {
    return std::apply(
        [&fn, &ctx](auto &&...arg) -> decltype(auto) {
          return std::invoke(fn, ctx, std::forward<decltype(arg)>(arg)...);
        },
        std::forward<Tuple>(args));
  }
  return std::apply(
      [&fn](auto &&...arg) -> decltype(auto) {
        return std::invoke(fn, std::forward<decltype(arg)>(arg)...);
      },
      std::forward<Tuple>(args));
}

template <typename Fn>
auto invoke_kernel(Fn &fn, RequestContext &ctx, const InputValues &inputs,
                   OutputValues &outputs) -> Expected<void> {
  using traits = kernel_traits<Fn>;
  using input_tuple = typename traits::input_tuple;

  auto args = read_inputs<input_tuple>(inputs);
  using result_type = typename traits::result_type;
  if constexpr (std::is_same_v<result_type, void>) {
    apply_kernel<traits::has_ctx>(fn, ctx, std::move(args));
    return {};
  } else {
    auto result = apply_kernel<traits::has_ctx>(fn, ctx, std::move(args));
    return handle_return(outputs, std::move(result));
  }
}

template <typename T>
struct TypeNameTrait {
  static constexpr std::string_view name() { return "unknown"; }
};

template <> struct TypeNameTrait<int64_t> { static constexpr std::string_view name() { return "i64"; } };
template <> struct TypeNameTrait<double> { static constexpr std::string_view name() { return "f64"; } };
template <> struct TypeNameTrait<bool> { static constexpr std::string_view name() { return "bool"; } };
template <> struct TypeNameTrait<std::string> { static constexpr std::string_view name() { return "string"; } };

template <typename Arg>
auto append_input(std::vector<PortDesc> &inputs, std::string_view name, sr::engine::TypeRegistry& registry)
    -> Expected<void> {
  using traits = input_arg_traits<Arg>;
  using port_type = typename traits::port_type;
  
  std::string_view type_name = TypeNameTrait<port_type>::name();
  
  // Try entt if unknown (if available and reliable)
  // For now rely on trait.
  
  sr::engine::TypeId type_id = registry.intern_primitive(type_name);

  PortDesc port;
  port.name_id = name.empty() ? NameId{} : hash_name(name);
  port.type_id = type_id;
  port.meta_type = entt::resolve<port_type>();
  port.required = !traits::optional;
  inputs.push_back(std::move(port));
  return {};
}

template <typename Arg>
auto append_output(std::vector<PortDesc> &outputs, std::string_view name, sr::engine::TypeRegistry& registry)
    -> Expected<void> {
  using port_type = output_port_type_t<Arg>;
  
  std::string_view type_name = TypeNameTrait<port_type>::name();

  sr::engine::TypeId type_id = registry.intern_primitive(type_name);

  PortDesc port;
  port.name_id = name.empty() ? NameId{} : hash_name(name);
  port.type_id = type_id;
  port.meta_type = entt::resolve<port_type>();
  port.required = true;
  outputs.push_back(std::move(port));
  return {};
}

template <typename Tuple, typename Fn, std::size_t... I>
auto for_each_index(Fn &&fn, std::index_sequence<I...>) -> Expected<void> {
  Expected<void> result{};
  ((result =
        result ? std::forward<Fn>(fn)(std::integral_constant<std::size_t, I>{})
               : result),
   ...);
  return result;
}

template <typename Tuple>
auto append_inputs(std::vector<PortDesc> &inputs,
                   const std::vector<std::string> &names,
                   sr::engine::TypeRegistry& registry) -> Expected<void> {
  auto add = [&inputs, &names, &registry](auto index_c) -> Expected<void> {
    constexpr std::size_t index = decltype(index_c)::value;
    return append_input<std::tuple_element_t<index, Tuple>>(inputs,
                                                            names[index], registry);
  };
  return for_each_index<Tuple>(
      add, std::make_index_sequence<std::tuple_size_v<Tuple>>{});
}

template <typename Tuple>
auto append_outputs(std::vector<PortDesc> &outputs,
                    const std::vector<std::string> &names,
                    sr::engine::TypeRegistry& registry) -> Expected<void> {
  auto add = [&outputs, &names, &registry](auto index_c) -> Expected<void> {
    constexpr std::size_t index = decltype(index_c)::value;
    return append_output<std::tuple_element_t<index, Tuple>>(outputs,
                                                             names[index], registry);
  };
  return for_each_index<Tuple>(
      add, std::make_index_sequence<std::tuple_size_v<Tuple>>{});
}

template <typename Fn>
auto build_signature(const std::vector<std::string> &input_names,
                     const std::vector<std::string> &output_names,
                     sr::engine::TypeRegistry& registry)
    -> Expected<Signature> {
  using traits = kernel_traits<Fn>;
  using input_tuple = typename traits::input_tuple;
  using output_tuple = typename traits::output_tuple;
  constexpr std::size_t input_count = traits::input_count;
  constexpr std::size_t output_count = traits::output_count;

  if (!input_names.empty() && input_names.size() != input_count) {
    return tl::unexpected(make_error("input count mismatch"));
  }
  if (!output_names.empty() && output_names.size() != output_count) {
    return tl::unexpected(make_error("output count mismatch"));
  }

  std::vector<std::string> resolved_inputs = input_names;
  std::vector<std::string> resolved_outputs = output_names;
  if (resolved_inputs.empty() && input_count > 0) {
    resolved_inputs.assign(input_count, "");
  }
  if (resolved_outputs.empty() && output_count > 0) {
    resolved_outputs.assign(output_count, "");
  }

  Signature signature;
  signature.inputs.reserve(input_count);
  signature.outputs.reserve(output_count);

  auto input_result =
      append_inputs<input_tuple>(signature.inputs, resolved_inputs, registry);
  if (!input_result) {
    return tl::unexpected(input_result.error());
  }

  auto output_result =
      append_outputs<output_tuple>(signature.outputs, resolved_outputs, registry);
  if (!output_result) {
    return tl::unexpected(output_result.error());
  }

  return signature;
}

template <typename Fn>
auto build_signature_with_types(sr::engine::TypeRegistry& registry) -> Expected<Signature> {
    return build_signature<Fn>({}, {}, registry);
}

template <typename Fn>
auto compute_from_fn(void *ptr, RequestContext &ctx, const InputValues &inputs,
                     OutputValues &outputs) -> Expected<void> {
  auto &fn = *static_cast<Fn *>(ptr);
  return invoke_kernel(fn, ctx, inputs, outputs);
}

template <typename Fn> auto make_kernel_handle_from_fn(Fn fn) -> KernelHandle {
  KernelHandle handle;

  handle.instance = std::make_shared<Fn>(std::move(fn));
  handle.compute = &compute_from_fn<Fn>;
  return handle;
}

} // namespace sr::engine::detail
namespace sr::engine {
template <typename Initiator, typename Canceller, typename... ResultTypes>
class AsyncAdapter {
  Initiator init_;
  Canceller cancel_;

public:
  using completion_signatures =
      ex::completion_signatures<ex::set_value_t(ResultTypes...),
                                ex::set_error_t(std::exception_ptr),
                                ex::set_stopped_t()>;

  AsyncAdapter(Initiator init, Canceller cancel)
      : init_(std::move(init)), cancel_(std::move(cancel)) {}

  template <typename Receiver> auto connect(Receiver r) const {
    return OperationState<Receiver>(std::move(r), init_, cancel_);
  }

private:
  template <typename Receiver> struct OperationState {
    Receiver receiver_;
    Initiator init_;
    Canceller cancel_;

    using NativeHandle = void *;
    NativeHandle native_handle_ = nullptr;

    using StopToken = ex::stop_token_of_t<ex::env_of_t<Receiver>>;
    using StopCallback =
        typename StopToken::template callback_type<std::function<void()>>;

    std::optional<StopCallback> stop_cb_;
    std::atomic<bool> finished_{false};

    OperationState(Receiver r, Initiator init, Canceller cancel)
        : receiver_(std::move(r)), init_(std::move(init)),
          cancel_(std::move(cancel)) {}

    OperationState(OperationState &&) = delete;

    void start() noexcept {
      try {

        auto env = ex::get_env(receiver_);
        auto st = ex::get_stop_token(env);

        if (st.stop_requested()) {
          ex::set_stopped(std::move(receiver_));
          return;
        }

        stop_cb_.emplace(st, [this] { this->request_stop(); });

        // 发起调用
        native_handle_ = init_(this, &OperationState::trampoline);

      } catch (...) {
        ex::set_error(std::move(receiver_), std::current_exception());
      }
    }

    void request_stop() {
      if (!finished_ && native_handle_) {
        cancel_(native_handle_);
      }
    }

    static void trampoline(void *context, ResultTypes... args) {
      auto *self = static_cast<OperationState *>(context);
      self->on_complete(std::forward<ResultTypes>(args)...);
    }

    void on_complete(ResultTypes... args) {
      if (finished_.exchange(true))
        return;
      stop_cb_.reset();

      ex::set_value(std::move(receiver_), std::forward<ResultTypes>(args)...);
    }
  };
};

template <typename... ResultTypes, typename Init, typename Cancel>
auto make_async_adapter(Init &&init, Cancel &&cancel) {
  return AsyncAdapter<std::decay_t<Init>, std::decay_t<Cancel>, ResultTypes...>(
      std::forward<Init>(init), std::forward<Cancel>(cancel));
}
} // namespace sr::engine