#include "kernel/sample_kernels.hpp"

#include "engine/type_names.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <tuple>
#include <utility>

namespace sr::kernel {
namespace {

using sr::engine::KernelRegistry;

auto get_int_param(const sr::engine::Json &params, const char *key,
                   int64_t fallback) -> int64_t {
  if (!params.is_object()) {
    return fallback;
  }
  auto it = params.find(key);
  if (it != params.end() && it->is_number_integer()) {
    return it->get<int64_t>();
  }
  return fallback;
}

auto get_bool_param(const sr::engine::Json &params, const char *key,
                    bool fallback) -> bool {
  if (!params.is_object()) {
    return fallback;
  }
  auto it = params.find(key);
  if (it != params.end() && it->is_boolean()) {
    return it->get<bool>();
  }
  return fallback;
}

auto get_string_param(const sr::engine::Json &params, const char *key,
                      std::string fallback) -> std::string {
  if (!params.is_object()) {
    return fallback;
  }
  auto it = params.find(key);
  if (it != params.end() && it->is_string()) {
    return it->get<std::string>();
  }
  return fallback;
}

} // namespace

auto register_builtin_types(sr::engine::TypeRegistry &registry) -> void {
  sr::engine::register_type<int64_t>(registry, "int64");
  sr::engine::register_type<double>(registry, "double");
  sr::engine::register_type<bool>(registry, "bool");
  sr::engine::register_type<std::string>(registry, "string");
}

auto register_sample_kernels(KernelRegistry &registry) -> void {
  registry.register_kernel("add", [](int64_t a, int64_t b) noexcept { return a + b; });

  registry.register_kernel("sub_i64", [](int64_t a, int64_t b) noexcept { return a - b; });

  registry.register_kernel("negate_i64", [](int64_t value) noexcept { return -value; });

  registry.register_kernel(
      "div_i64",
      [](int64_t numerator, int64_t denominator) noexcept {
        return denominator == 0 ? int64_t{0} : (numerator / denominator);
      });

  registry.register_kernel("gt_i64", [](int64_t a, int64_t b) noexcept { return a > b; });

  registry.register_kernel("eq_i64", [](int64_t a, int64_t b) noexcept { return a == b; });

  registry.register_kernel(
      "eq_str", [](const std::string& a, const std::string& b) noexcept {
        return a == b;
      });

  registry.register_kernel(
      "starts_with_str",
      [](const std::string& text, const std::string& prefix) noexcept {
        return text.rfind(prefix, 0) == 0;
      });

  registry.register_kernel("not_bool", [](bool value) noexcept { return !value; });

  registry.register_kernel("and_bool", [](bool a, bool b) noexcept { return a && b; });

  registry.register_kernel("or_bool", [](bool a, bool b) noexcept { return a || b; });

  registry.register_kernel("identity_i64", [](int64_t value) noexcept { return value; });

  registry.register_kernel("identity_str", [](const std::string& text) noexcept { return text; });

  registry.register_kernel(
      "len_str",
      [](const std::string& text) noexcept { return static_cast<int64_t>(text.size()); });

  registry.register_kernel("to_string", [](int64_t value) noexcept { return std::to_string(value); });

  registry.register_kernel("fanout_i64", [](int64_t value) noexcept {
    return std::make_tuple(value, value);
  });

  registry.register_kernel("sink_i64", [](int64_t) noexcept {});

  registry.register_kernel("sink_str", [](const std::string&) noexcept {});

  registry.register_kernel("if_else_i64",
                           [](bool cond, int64_t then_value, int64_t else_value) noexcept {
    return cond ? then_value : else_value;
  });

  registry.register_kernel(
      "if_else_str",
      [](bool cond, const std::string& then_value,
         const std::string& else_value) noexcept { return cond ? then_value : else_value; });

  registry.register_kernel(
      "coalesce_i64", [](std::optional<int64_t> value, int64_t fallback) noexcept {
        return value ? *value : fallback;
      });

  registry.register_kernel(
      "coalesce_str", [](std::optional<std::string> text, const std::string& fallback) noexcept {
        return text ? *text : fallback;
      });

  registry.register_kernel_with_params(
      "mul", [](const sr::engine::Json& params) {
        const auto factor = get_int_param(params, "factor", 1);
        return [factor](int64_t value) noexcept { return value * factor; };
      });

  registry.register_kernel_with_params(
      "const_i64", [](const sr::engine::Json& params) {
        const auto value = get_int_param(params, "value", 0);
        return [value]() noexcept { return value; };
      });

  registry.register_kernel_with_params(
      "const_bool", [](const sr::engine::Json& params) {
        const auto value = get_bool_param(params, "value", false);
        return [value]() noexcept { return value; };
      });

  registry.register_kernel_with_params(
      "const_str", [](const sr::engine::Json& params) {
        const auto value = get_string_param(params, "value", "");
        return [value]() noexcept { return value; };
      });

  registry.register_kernel_with_params(
      "concat_str", [](const sr::engine::Json& params) {
        const auto sep = get_string_param(params, "sep", "");
        return [sep](const std::string& a, const std::string& b) noexcept {
          return a + sep + b;
        };
      });

  registry.register_kernel_with_params(
      "format", [](const sr::engine::Json& params) {
        const auto prefix = get_string_param(params, "prefix", "");
        return [prefix](int64_t value) noexcept {
          return prefix + std::to_string(value);
        };
      });
}

} // namespace sr::kernel
