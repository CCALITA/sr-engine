#pragma once

#include <concepts>
#include <format>
#include <functional>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "engine/error.hpp"
#include "engine/kernel_types.hpp"
#include "engine/types.hpp"

namespace sr::engine::detail {

template <typename T>
struct function_traits;

template <typename R, typename... Args>
struct function_traits<R(Args...)> {
  using result_type = R;
  using args_tuple = std::tuple<Args...>;
};

template <typename R, typename... Args>
struct function_traits<R (*)(Args...)> : function_traits<R(Args...)> {};

template <typename C, typename R, typename... Args>
struct function_traits<R (C::*)(Args...)> : function_traits<R(Args...)> {};

template <typename C, typename R, typename... Args>
struct function_traits<R (C::*)(Args...) const> : function_traits<R(Args...)> {};

template <typename T, typename = void>
struct callable_traits;

template <typename R, typename... Args>
struct callable_traits<R(Args...), void> : function_traits<R(Args...)> {};

template <typename R, typename... Args>
struct callable_traits<R (*)(Args...), void> : function_traits<R(Args...)> {};

template <typename C, typename R, typename... Args>
struct callable_traits<R (C::*)(Args...), void> : function_traits<R(Args...)> {};

template <typename C, typename R, typename... Args>
struct callable_traits<R (C::*)(Args...) const, void> : function_traits<R(Args...)> {};

template <typename T>
struct callable_traits<T, std::void_t<decltype(&T::operator())>> : callable_traits<decltype(&T::operator())> {};

template <typename T>
struct expected_traits {
  using value_type = T;
  static constexpr bool is_expected = false;
};

template <typename T>
struct expected_traits<Expected<T>> {
  using value_type = T;
  static constexpr bool is_expected = true;
};

template <typename T>
inline constexpr bool is_expected_v = expected_traits<std::remove_cvref_t<T>>::is_expected;

template <typename T>
struct is_optional : std::false_type {};

template <typename T>
struct is_optional<std::optional<T>> : std::true_type {};

template <typename T>
inline constexpr bool is_optional_v = is_optional<std::remove_cvref_t<T>>::value;

template <typename T>
struct is_reference_wrapper : std::false_type {};

template <typename T>
struct is_reference_wrapper<std::reference_wrapper<T>> : std::true_type {};

template <typename T>
struct unwrap_reference_wrapper {
  using type = T;
};

template <typename T>
struct unwrap_reference_wrapper<std::reference_wrapper<T>> {
  using type = T;
};

template <typename T>
using port_type_t = std::remove_cvref_t<typename unwrap_reference_wrapper<T>::type>;

template <typename T>
struct input_arg_traits {
  static constexpr bool optional = false;
  using port_type = port_type_t<T>;
};

template <typename T>
struct input_arg_traits<std::optional<T>> {
  static constexpr bool optional = true;
  using port_type = port_type_t<T>;
};

template <typename T>
struct input_arg_traits<const std::optional<T>&> : input_arg_traits<std::optional<T>> {};

template <typename T>
struct input_arg_traits<std::optional<T>&> : input_arg_traits<std::optional<T>> {};

template <typename T>
struct input_arg_traits<std::optional<T>&&> : input_arg_traits<std::optional<T>> {};

template <typename T>
struct output_tuple {
  using type = std::tuple<T>;
};

template <>
struct output_tuple<void> {
  using type = std::tuple<>;
};

template <typename... Ts>
struct output_tuple<std::tuple<Ts...>> {
  using type = std::tuple<Ts...>;
};

template <typename A, typename B>
struct output_tuple<std::pair<A, B>> {
  using type = std::tuple<A, B>;
};

template <typename T>
struct call_arg_type {
  using type = std::conditional_t<is_optional_v<T>, std::remove_cvref_t<T>, T>;
};

template <typename T>
using call_arg_type_t = typename call_arg_type<T>::type;

template <typename T>
inline constexpr bool is_mutable_lref_v =
  std::is_lvalue_reference_v<T> && !std::is_const_v<std::remove_reference_t<T>>;

template <typename T>
inline constexpr bool is_request_context_v = std::is_same_v<std::remove_cvref_t<T>, RequestContextView>;

template <typename T>
inline constexpr bool is_request_context_base_v = std::is_same_v<std::remove_cvref_t<T>, RequestContext>;

template <typename Tuple>
struct tuple_tail;

template <typename T, typename... Rest>
struct tuple_tail<std::tuple<T, Rest...>> {
  using type = std::tuple<Rest...>;
};

template <>
struct tuple_tail<std::tuple<>> {
  using type = std::tuple<>;
};

template <typename Tuple>
struct first_is_context : std::false_type {};

template <typename T, typename... Rest>
struct first_is_context<std::tuple<T, Rest...>> : std::bool_constant<is_request_context_v<T>> {};

template <typename Fn>
concept KernelCallable = requires {
  typename callable_traits<Fn>::result_type;
  typename callable_traits<Fn>::args_tuple;
};

template <typename T>
inline constexpr bool is_valid_input_arg_v =
  !is_request_context_v<std::remove_cvref_t<T>> &&
  !is_request_context_base_v<std::remove_cvref_t<T>> &&
  !std::is_rvalue_reference_v<T> &&
  !is_mutable_lref_v<T>;

template <typename T>
inline constexpr bool is_valid_output_arg_v =
  !std::is_reference_v<T> && !is_optional_v<T> && !is_reference_wrapper<T>::value;

template <typename Tuple, std::size_t... I>
constexpr bool input_tuple_ok(std::index_sequence<I...>) {
  return (is_valid_input_arg_v<std::tuple_element_t<I, Tuple>> && ...);
}

template <typename Tuple>
constexpr bool input_tuple_ok() {
  return input_tuple_ok<Tuple>(std::make_index_sequence<std::tuple_size_v<Tuple>>{});
}

template <typename Tuple, std::size_t... I>
constexpr bool output_tuple_ok(std::index_sequence<I...>) {
  return (is_valid_output_arg_v<std::tuple_element_t<I, Tuple>> && ...);
}

template <typename Tuple>
constexpr bool output_tuple_ok() {
  return output_tuple_ok<Tuple>(std::make_index_sequence<std::tuple_size_v<Tuple>>{});
}

template <typename Fn>
constexpr bool kernel_inputs_ok() {
  using traits = callable_traits<Fn>;
  using args_tuple = typename traits::args_tuple;
  constexpr bool has_ctx = first_is_context<args_tuple>::value;
  using input_tuple = std::conditional_t<has_ctx, typename tuple_tail<args_tuple>::type, args_tuple>;
  return input_tuple_ok<input_tuple>();
}

template <typename Fn>
constexpr bool kernel_outputs_ok() {
  using traits = callable_traits<Fn>;
  using result_type = typename traits::result_type;
  using value_type = typename expected_traits<result_type>::value_type;
  using output_tuple = typename output_tuple<value_type>::type;
  return output_tuple_ok<output_tuple>();
}

template <typename Fn>
concept KernelFn =
  KernelCallable<Fn> && kernel_inputs_ok<Fn>() && kernel_outputs_ok<Fn>();

template <typename Factory>
using factory_result_t = decltype(std::declval<Factory&>()(std::declval<const Json&>()));

template <typename Factory>
using factory_functor_t = std::conditional_t<
  is_expected_v<factory_result_t<Factory>>,
  typename expected_traits<factory_result_t<Factory>>::value_type,
  factory_result_t<Factory>>;

template <typename Factory>
concept KernelFactory =
  std::invocable<Factory, const Json&> && KernelFn<factory_functor_t<Factory>>;

template <typename Arg>
auto load_input(const InputValues& inputs, std::size_t index) -> call_arg_type_t<Arg> {
  static_assert(!is_mutable_lref_v<Arg>, "Kernel inputs must be values or const references.");
  if constexpr (is_optional_v<Arg>) {
    using Opt = std::remove_cvref_t<Arg>;
    using Inner = typename Opt::value_type;
    const auto& slot = inputs.slot(index);
    if (!slot.has_value()) {
      return Opt{};
    }
    if constexpr (is_reference_wrapper<Inner>::value) {
      using Wrapped = typename Inner::type;
      static_assert(std::is_const_v<std::remove_reference_t<Wrapped>>,
                    "Optional reference inputs must use std::reference_wrapper<const T>.");
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
auto read_inputs(const InputValues& inputs, std::index_sequence<I...>)
  -> std::tuple<call_arg_type_t<std::tuple_element_t<I, Tuple>>...> {
  using Result = std::tuple<call_arg_type_t<std::tuple_element_t<I, Tuple>>...>;
  return Result(load_input<std::tuple_element_t<I, Tuple>>(inputs, I)...);
}

template <typename Tuple>
auto read_inputs(const InputValues& inputs) -> decltype(auto) {
  return read_inputs<Tuple>(inputs, std::make_index_sequence<std::tuple_size_v<Tuple>>{});
}

template <typename Tuple, std::size_t... I>
auto write_tuple_outputs(OutputValues& outputs, Tuple&& tuple, std::index_sequence<I...>) -> void {
  (outputs.set<port_type_t<std::tuple_element_t<I, std::remove_reference_t<Tuple>>>>(
     I, std::get<I>(std::forward<Tuple>(tuple))),
   ...);
}

template <typename T, typename = void>
struct is_tuple_like : std::false_type {};

template <typename T>
struct is_tuple_like<T, std::void_t<decltype(std::tuple_size<T>::value)>> : std::true_type {};

template <typename R>
auto write_outputs(OutputValues& outputs, R&& value) -> Expected<void> {
  using Raw = std::remove_cvref_t<R>;
  if constexpr (std::is_same_v<Raw, void>) {
    return {};
  } else if constexpr (is_tuple_like<Raw>::value) {
    write_tuple_outputs(outputs, std::forward<R>(value), std::make_index_sequence<std::tuple_size_v<Raw>>{});
    return {};
  } else {
    outputs.set<port_type_t<Raw>>(0, std::forward<R>(value));
    return {};
  }
}

template <typename R>
auto handle_return(OutputValues& outputs, R&& value) -> Expected<void> {
  using Raw = std::remove_cvref_t<R>;
  if constexpr (is_expected_v<Raw>) {
    if (!value) {
      return tl::unexpected(value.error());
    }
    if constexpr (std::is_same_v<typename expected_traits<Raw>::value_type, void>) {
      return {};
    } else {
      return write_outputs(outputs, std::move(*value));
    }
  } else {
    return write_outputs(outputs, std::forward<R>(value));
  }
}

template <typename Fn>
auto invoke_kernel(Fn& fn, RequestContextView& ctx, const InputValues& inputs, OutputValues& outputs)
  -> Expected<void> {
  using traits = callable_traits<Fn>;
  using args_tuple = typename traits::args_tuple;
  constexpr bool has_ctx = first_is_context<args_tuple>::value;
  using input_tuple = std::conditional_t<has_ctx, typename tuple_tail<args_tuple>::type, args_tuple>;

  auto args = read_inputs<input_tuple>(inputs);
  if constexpr (has_ctx) {
    using result_type = typename traits::result_type;
    if constexpr (std::is_same_v<result_type, void>) {
      std::apply([&](auto&&... arg) { fn(ctx, std::forward<decltype(arg)>(arg)...); }, std::move(args));
      return {};
    } else {
      auto result = std::apply(
        [&](auto&&... arg) { return fn(ctx, std::forward<decltype(arg)>(arg)...); }, std::move(args));
      return handle_return(outputs, std::move(result));
    }
  } else {
    using result_type = typename traits::result_type;
    if constexpr (std::is_same_v<result_type, void>) {
      std::apply([&](auto&&... arg) { fn(std::forward<decltype(arg)>(arg)...); }, std::move(args));
      return {};
    } else {
      auto result = std::apply(
        [&](auto&&... arg) { return fn(std::forward<decltype(arg)>(arg)...); }, std::move(args));
      return handle_return(outputs, std::move(result));
    }
  }
}

template <typename Arg>
auto append_input(std::vector<PortDesc>& inputs, const std::string& name) -> Expected<void> {
  using traits = input_arg_traits<Arg>;
  using port_type = typename traits::port_type;
  auto meta = entt::resolve<port_type>();
  if (!meta) {
    return tl::unexpected(make_error(std::format("unregistered input type for port: {}", name)));
  }
  PortDesc port;
  port.name = name;
  port.type = meta;
  port.required = !traits::optional;
  inputs.push_back(std::move(port));
  return {};
}

template <typename Arg>
auto append_output(std::vector<PortDesc>& outputs, const std::string& name) -> Expected<void> {
  using port_type = port_type_t<Arg>;
  auto meta = entt::resolve<port_type>();
  if (!meta) {
    return tl::unexpected(make_error(std::format("unregistered output type for port: {}", name)));
  }
  PortDesc port;
  port.name = name;
  port.type = meta;
  port.required = true;
  outputs.push_back(std::move(port));
  return {};
}

template <typename Tuple, std::size_t Index = 0>
auto append_inputs(std::vector<PortDesc>& inputs, const std::vector<std::string>& names) -> Expected<void> {
  if constexpr (Index == std::tuple_size_v<Tuple>) {
    return {};
  } else {
    auto result = append_input<std::tuple_element_t<Index, Tuple>>(inputs, names[Index]);
    if (!result) {
      return result;
    }
    return append_inputs<Tuple, Index + 1>(inputs, names);
  }
}

template <typename Tuple, std::size_t Index = 0>
auto append_outputs(std::vector<PortDesc>& outputs, const std::vector<std::string>& names) -> Expected<void> {
  if constexpr (Index == std::tuple_size_v<Tuple>) {
    return {};
  } else {
    auto result = append_output<std::tuple_element_t<Index, Tuple>>(outputs, names[Index]);
    if (!result) {
      return result;
    }
    return append_outputs<Tuple, Index + 1>(outputs, names);
  }
}

template <typename Fn>
auto build_signature(const std::vector<std::string>& input_names, const std::vector<std::string>& output_names)
  -> Expected<Signature> {
  using traits = callable_traits<Fn>;
  using args_tuple = typename traits::args_tuple;
  constexpr bool has_ctx = first_is_context<args_tuple>::value;
  using input_tuple = std::conditional_t<has_ctx, typename tuple_tail<args_tuple>::type, args_tuple>;
  constexpr std::size_t input_count = std::tuple_size_v<input_tuple>;

  using result_type = typename traits::result_type;
  using value_type = typename expected_traits<result_type>::value_type;
  using output_tuple = typename output_tuple<value_type>::type;
  constexpr std::size_t output_count = std::tuple_size_v<output_tuple>;

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

  auto input_result = append_inputs<input_tuple>(signature.inputs, resolved_inputs);
  if (!input_result) {
    return tl::unexpected(input_result.error());
  }

  auto output_result = append_outputs<output_tuple>(signature.outputs, resolved_outputs);
  if (!output_result) {
    return tl::unexpected(output_result.error());
  }

  return signature;
}

template <typename Fn>
auto compute_from_fn(void* ptr, RequestContextView& ctx, const InputValues& inputs, OutputValues& outputs)
  -> Expected<void> {
  auto& fn = *static_cast<Fn*>(ptr);
  return invoke_kernel(fn, ctx, inputs, outputs);
}

template <typename Fn>
auto make_kernel_handle_from_fn(Fn fn, const std::vector<std::string>& input_names,
                                const std::vector<std::string>& output_names, TaskType task_type)
  -> Expected<KernelHandle> {
  auto signature_result = build_signature<Fn>(input_names, output_names);
  if (!signature_result) {
    return tl::unexpected(signature_result.error());
  }
  KernelHandle handle;
  handle.signature = std::move(*signature_result);
  handle.instance = std::make_shared<Fn>(std::move(fn));
  handle.compute = &compute_from_fn<Fn>;
  handle.task_type = task_type;
  return handle;
}

}  // namespace sr::engine::detail
