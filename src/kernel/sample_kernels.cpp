#include "kernel/sample_kernels.hpp"

#include <cstdint>
#include <string>
#include <utility>

#include <stdexec/execution.hpp>

#include "engine/error.hpp"
#include "engine/types.hpp"

namespace sr::kernel {
namespace {

auto get_int_param(const sr::engine::Json& params, const char* key, int64_t fallback) -> int64_t {
  if (!params.is_object()) {
    return fallback;
  }
  auto it = params.find(key);
  if (it != params.end() && it->is_number_integer()) {
    return it->get<int64_t>();
  }
  return fallback;
}

auto get_bool_param(const sr::engine::Json& params, const char* key, bool fallback) -> bool {
  if (!params.is_object()) {
    return fallback;
  }
  auto it = params.find(key);
  if (it != params.end() && it->is_boolean()) {
    return it->get<bool>();
  }
  return fallback;
}

auto get_string_param(const sr::engine::Json& params, const char* key, std::string fallback) -> std::string {
  if (!params.is_object()) {
    return fallback;
  }
  auto it = params.find(key);
  if (it != params.end() && it->is_string()) {
    return it->get<std::string>();
  }
  return fallback;
}

template <typename K>
auto register_simple(sr::engine::KernelRegistry& registry, const char* name) -> void {
  registry.register_kernel(name, [](const sr::engine::Json&) -> sr::engine::Expected<sr::engine::KernelHandle> {
    return sr::engine::make_kernel_handle(K{});
  });
}

struct ConstI64Kernel {
  int64_t value = 0;

  friend auto tag_invoke(sr::engine::dag::get_signature_t, const ConstI64Kernel&) -> sr::engine::Signature {
    return sr::engine::Signature{{}, {sr::engine::PortDesc{"value", entt::resolve<int64_t>(), true}}};
  }

  friend auto tag_invoke(sr::engine::dag::execute_t, ConstI64Kernel& self, sr::engine::RequestContext&,
                         const sr::engine::InputValues&, sr::engine::OutputValues& outputs) {
    return stdexec::just() | stdexec::then([&outputs, value = self.value]() mutable {
             outputs.set<int64_t>(0, value);
           });
  }
};

struct ConstBoolKernel {
  bool value = false;

  friend auto tag_invoke(sr::engine::dag::get_signature_t, const ConstBoolKernel&) -> sr::engine::Signature {
    return sr::engine::Signature{{}, {sr::engine::PortDesc{"value", entt::resolve<bool>(), true}}};
  }

  friend auto tag_invoke(sr::engine::dag::execute_t, ConstBoolKernel& self, sr::engine::RequestContext&,
                         const sr::engine::InputValues&, sr::engine::OutputValues& outputs) {
    return stdexec::just() | stdexec::then([&outputs, value = self.value]() mutable {
             outputs.set<bool>(0, value);
           });
  }
};

struct ConstStringKernel {
  std::string value;

  friend auto tag_invoke(sr::engine::dag::get_signature_t, const ConstStringKernel&) -> sr::engine::Signature {
    return sr::engine::Signature{{}, {sr::engine::PortDesc{"value", entt::resolve<std::string>(), true}}};
  }

  friend auto tag_invoke(sr::engine::dag::execute_t, ConstStringKernel& self, sr::engine::RequestContext&,
                         const sr::engine::InputValues&, sr::engine::OutputValues& outputs) {
    return stdexec::just() | stdexec::then([&outputs, value = self.value]() mutable {
             outputs.set<std::string>(0, std::move(value));
           });
  }
};

struct IdentityI64Kernel {
  friend auto tag_invoke(sr::engine::dag::get_signature_t, const IdentityI64Kernel&) -> sr::engine::Signature {
    return sr::engine::Signature{
      {sr::engine::PortDesc{"value", entt::resolve<int64_t>(), true}},
      {sr::engine::PortDesc{"value", entt::resolve<int64_t>(), true}}};
  }

  friend auto tag_invoke(sr::engine::dag::execute_t, IdentityI64Kernel&, sr::engine::RequestContext&,
                         const sr::engine::InputValues& inputs, sr::engine::OutputValues& outputs) {
    auto value = inputs.get<int64_t>(0);
    return stdexec::just() | stdexec::then([&outputs, value]() mutable {
             outputs.set<int64_t>(0, value);
           });
  }
};

struct IdentityStringKernel {
  friend auto tag_invoke(sr::engine::dag::get_signature_t, const IdentityStringKernel&) -> sr::engine::Signature {
    return sr::engine::Signature{
      {sr::engine::PortDesc{"text", entt::resolve<std::string>(), true}},
      {sr::engine::PortDesc{"text", entt::resolve<std::string>(), true}}};
  }

  friend auto tag_invoke(sr::engine::dag::execute_t, IdentityStringKernel&, sr::engine::RequestContext&,
                         const sr::engine::InputValues& inputs, sr::engine::OutputValues& outputs) {
    auto text = inputs.get<std::string>(0);
    return stdexec::just() | stdexec::then([&outputs, text = std::move(text)]() mutable {
             outputs.set<std::string>(0, std::move(text));
           });
  }
};

struct NegateI64Kernel {
  friend auto tag_invoke(sr::engine::dag::get_signature_t, const NegateI64Kernel&) -> sr::engine::Signature {
    return sr::engine::Signature{
      {sr::engine::PortDesc{"value", entt::resolve<int64_t>(), true}},
      {sr::engine::PortDesc{"value", entt::resolve<int64_t>(), true}}};
  }

  friend auto tag_invoke(sr::engine::dag::execute_t, NegateI64Kernel&, sr::engine::RequestContext&,
                         const sr::engine::InputValues& inputs, sr::engine::OutputValues& outputs) {
    auto value = inputs.get<int64_t>(0);
    return stdexec::just() | stdexec::then([&outputs, value]() mutable {
             outputs.set<int64_t>(0, -value);
           });
  }
};

struct AddKernel {
  friend auto tag_invoke(sr::engine::dag::get_signature_t, const AddKernel&) -> sr::engine::Signature {
    return sr::engine::Signature{
      {sr::engine::PortDesc{"a", entt::resolve<int64_t>(), true},
       sr::engine::PortDesc{"b", entt::resolve<int64_t>(), true}},
      {sr::engine::PortDesc{"sum", entt::resolve<int64_t>(), true}}};
  }

  friend auto tag_invoke(sr::engine::dag::execute_t, AddKernel&, sr::engine::RequestContext&,
                         const sr::engine::InputValues& inputs, sr::engine::OutputValues& outputs) {
    auto a = inputs.get<int64_t>(0);
    auto b = inputs.get<int64_t>(1);
    return stdexec::just() | stdexec::then([&outputs, sum = a + b]() mutable {
             outputs.set<int64_t>(0, sum);
           });
  }
};

struct SubKernel {
  friend auto tag_invoke(sr::engine::dag::get_signature_t, const SubKernel&) -> sr::engine::Signature {
    return sr::engine::Signature{
      {sr::engine::PortDesc{"a", entt::resolve<int64_t>(), true},
       sr::engine::PortDesc{"b", entt::resolve<int64_t>(), true}},
      {sr::engine::PortDesc{"diff", entt::resolve<int64_t>(), true}}};
  }

  friend auto tag_invoke(sr::engine::dag::execute_t, SubKernel&, sr::engine::RequestContext&,
                         const sr::engine::InputValues& inputs, sr::engine::OutputValues& outputs) {
    auto a = inputs.get<int64_t>(0);
    auto b = inputs.get<int64_t>(1);
    return stdexec::just() | stdexec::then([&outputs, diff = a - b]() mutable {
             outputs.set<int64_t>(0, diff);
           });
  }
};

struct MulKernel {
  int64_t factor = 1;

  friend auto tag_invoke(sr::engine::dag::get_signature_t, const MulKernel&) -> sr::engine::Signature {
    return sr::engine::Signature{
      {sr::engine::PortDesc{"value", entt::resolve<int64_t>(), true}},
      {sr::engine::PortDesc{"product", entt::resolve<int64_t>(), true}}};
  }

  friend auto tag_invoke(sr::engine::dag::execute_t, MulKernel& self, sr::engine::RequestContext&,
                         const sr::engine::InputValues& inputs, sr::engine::OutputValues& outputs) {
    auto value = inputs.get<int64_t>(0);
    return stdexec::just() | stdexec::then([&outputs, product = value * self.factor]() mutable {
             outputs.set<int64_t>(0, product);
           });
  }
};

struct DivKernel {
  friend auto tag_invoke(sr::engine::dag::get_signature_t, const DivKernel&) -> sr::engine::Signature {
    return sr::engine::Signature{
      {sr::engine::PortDesc{"numerator", entt::resolve<int64_t>(), true},
       sr::engine::PortDesc{"denominator", entt::resolve<int64_t>(), true}},
      {sr::engine::PortDesc{"quot", entt::resolve<int64_t>(), true}}};
  }

  friend auto tag_invoke(sr::engine::dag::execute_t, DivKernel&, sr::engine::RequestContext&,
                         const sr::engine::InputValues& inputs, sr::engine::OutputValues& outputs) {
    auto num = inputs.get<int64_t>(0);
    auto den = inputs.get<int64_t>(1);
    auto quot = (den == 0) ? int64_t{0} : (num / den);
    return stdexec::just() | stdexec::then([&outputs, quot]() mutable {
             outputs.set<int64_t>(0, quot);
           });
  }
};

struct GreaterKernel {
  friend auto tag_invoke(sr::engine::dag::get_signature_t, const GreaterKernel&) -> sr::engine::Signature {
    return sr::engine::Signature{
      {sr::engine::PortDesc{"a", entt::resolve<int64_t>(), true},
       sr::engine::PortDesc{"b", entt::resolve<int64_t>(), true}},
      {sr::engine::PortDesc{"result", entt::resolve<bool>(), true}}};
  }

  friend auto tag_invoke(sr::engine::dag::execute_t, GreaterKernel&, sr::engine::RequestContext&,
                         const sr::engine::InputValues& inputs, sr::engine::OutputValues& outputs) {
    auto a = inputs.get<int64_t>(0);
    auto b = inputs.get<int64_t>(1);
    return stdexec::just() | stdexec::then([&outputs, result = (a > b)]() mutable {
             outputs.set<bool>(0, result);
           });
  }
};

struct EqualKernel {
  friend auto tag_invoke(sr::engine::dag::get_signature_t, const EqualKernel&) -> sr::engine::Signature {
    return sr::engine::Signature{
      {sr::engine::PortDesc{"a", entt::resolve<int64_t>(), true},
       sr::engine::PortDesc{"b", entt::resolve<int64_t>(), true}},
      {sr::engine::PortDesc{"result", entt::resolve<bool>(), true}}};
  }

  friend auto tag_invoke(sr::engine::dag::execute_t, EqualKernel&, sr::engine::RequestContext&,
                         const sr::engine::InputValues& inputs, sr::engine::OutputValues& outputs) {
    auto a = inputs.get<int64_t>(0);
    auto b = inputs.get<int64_t>(1);
    return stdexec::just() | stdexec::then([&outputs, result = (a == b)]() mutable {
             outputs.set<bool>(0, result);
           });
  }
};

struct NotKernel {
  friend auto tag_invoke(sr::engine::dag::get_signature_t, const NotKernel&) -> sr::engine::Signature {
    return sr::engine::Signature{
      {sr::engine::PortDesc{"value", entt::resolve<bool>(), true}},
      {sr::engine::PortDesc{"value", entt::resolve<bool>(), true}}};
  }

  friend auto tag_invoke(sr::engine::dag::execute_t, NotKernel&, sr::engine::RequestContext&,
                         const sr::engine::InputValues& inputs, sr::engine::OutputValues& outputs) {
    auto value = inputs.get<bool>(0);
    return stdexec::just() | stdexec::then([&outputs, value]() mutable {
             outputs.set<bool>(0, !value);
           });
  }
};

struct AndKernel {
  friend auto tag_invoke(sr::engine::dag::get_signature_t, const AndKernel&) -> sr::engine::Signature {
    return sr::engine::Signature{
      {sr::engine::PortDesc{"a", entt::resolve<bool>(), true},
       sr::engine::PortDesc{"b", entt::resolve<bool>(), true}},
      {sr::engine::PortDesc{"value", entt::resolve<bool>(), true}}};
  }

  friend auto tag_invoke(sr::engine::dag::execute_t, AndKernel&, sr::engine::RequestContext&,
                         const sr::engine::InputValues& inputs, sr::engine::OutputValues& outputs) {
    auto a = inputs.get<bool>(0);
    auto b = inputs.get<bool>(1);
    return stdexec::just() | stdexec::then([&outputs, value = (a && b)]() mutable {
             outputs.set<bool>(0, value);
           });
  }
};

struct OrKernel {
  friend auto tag_invoke(sr::engine::dag::get_signature_t, const OrKernel&) -> sr::engine::Signature {
    return sr::engine::Signature{
      {sr::engine::PortDesc{"a", entt::resolve<bool>(), true},
       sr::engine::PortDesc{"b", entt::resolve<bool>(), true}},
      {sr::engine::PortDesc{"value", entt::resolve<bool>(), true}}};
  }

  friend auto tag_invoke(sr::engine::dag::execute_t, OrKernel&, sr::engine::RequestContext&,
                         const sr::engine::InputValues& inputs, sr::engine::OutputValues& outputs) {
    auto a = inputs.get<bool>(0);
    auto b = inputs.get<bool>(1);
    return stdexec::just() | stdexec::then([&outputs, value = (a || b)]() mutable {
             outputs.set<bool>(0, value);
           });
  }
};

struct IfElseI64Kernel {
  friend auto tag_invoke(sr::engine::dag::get_signature_t, const IfElseI64Kernel&) -> sr::engine::Signature {
    return sr::engine::Signature{
      {sr::engine::PortDesc{"cond", entt::resolve<bool>(), true},
       sr::engine::PortDesc{"then", entt::resolve<int64_t>(), true},
       sr::engine::PortDesc{"else", entt::resolve<int64_t>(), true}},
      {sr::engine::PortDesc{"value", entt::resolve<int64_t>(), true}}};
  }

  friend auto tag_invoke(sr::engine::dag::execute_t, IfElseI64Kernel&, sr::engine::RequestContext&,
                         const sr::engine::InputValues& inputs, sr::engine::OutputValues& outputs) {
    auto cond = inputs.get<bool>(0);
    auto then_value = inputs.get<int64_t>(1);
    auto else_value = inputs.get<int64_t>(2);
    auto value = cond ? then_value : else_value;
    return stdexec::just() | stdexec::then([&outputs, value]() mutable {
             outputs.set<int64_t>(0, value);
           });
  }
};

struct IfElseStringKernel {
  friend auto tag_invoke(sr::engine::dag::get_signature_t, const IfElseStringKernel&) -> sr::engine::Signature {
    return sr::engine::Signature{
      {sr::engine::PortDesc{"cond", entt::resolve<bool>(), true},
       sr::engine::PortDesc{"then", entt::resolve<std::string>(), true},
       sr::engine::PortDesc{"else", entt::resolve<std::string>(), true}},
      {sr::engine::PortDesc{"text", entt::resolve<std::string>(), true}}};
  }

  friend auto tag_invoke(sr::engine::dag::execute_t, IfElseStringKernel&, sr::engine::RequestContext&,
                         const sr::engine::InputValues& inputs, sr::engine::OutputValues& outputs) {
    auto cond = inputs.get<bool>(0);
    auto then_value = inputs.get<std::string>(1);
    auto else_value = inputs.get<std::string>(2);
    auto text = cond ? std::move(then_value) : std::move(else_value);
    return stdexec::just() | stdexec::then([&outputs, text = std::move(text)]() mutable {
             outputs.set<std::string>(0, std::move(text));
           });
  }
};

struct CoalesceI64Kernel {
  friend auto tag_invoke(sr::engine::dag::get_signature_t, const CoalesceI64Kernel&) -> sr::engine::Signature {
    return sr::engine::Signature{
      {sr::engine::PortDesc{"value", entt::resolve<int64_t>(), false},
       sr::engine::PortDesc{"fallback", entt::resolve<int64_t>(), true}},
      {sr::engine::PortDesc{"value", entt::resolve<int64_t>(), true}}};
  }

  friend auto tag_invoke(sr::engine::dag::execute_t, CoalesceI64Kernel&, sr::engine::RequestContext&,
                         const sr::engine::InputValues& inputs, sr::engine::OutputValues& outputs) {
    const auto& primary = inputs.slot(0);
    auto value = primary.has_value() ? primary.get<int64_t>() : inputs.get<int64_t>(1);
    return stdexec::just() | stdexec::then([&outputs, value]() mutable {
             outputs.set<int64_t>(0, value);
           });
  }
};

struct CoalesceStringKernel {
  friend auto tag_invoke(sr::engine::dag::get_signature_t, const CoalesceStringKernel&) -> sr::engine::Signature {
    return sr::engine::Signature{
      {sr::engine::PortDesc{"text", entt::resolve<std::string>(), false},
       sr::engine::PortDesc{"fallback", entt::resolve<std::string>(), true}},
      {sr::engine::PortDesc{"text", entt::resolve<std::string>(), true}}};
  }

  friend auto tag_invoke(sr::engine::dag::execute_t, CoalesceStringKernel&, sr::engine::RequestContext&,
                         const sr::engine::InputValues& inputs, sr::engine::OutputValues& outputs) {
    const auto& primary = inputs.slot(0);
    auto text = primary.has_value() ? primary.get<std::string>() : inputs.get<std::string>(1);
    return stdexec::just() | stdexec::then([&outputs, text = std::move(text)]() mutable {
             outputs.set<std::string>(0, std::move(text));
           });
  }
};

struct ConcatKernel {
  std::string sep;

  friend auto tag_invoke(sr::engine::dag::get_signature_t, const ConcatKernel&) -> sr::engine::Signature {
    return sr::engine::Signature{
      {sr::engine::PortDesc{"a", entt::resolve<std::string>(), true},
       sr::engine::PortDesc{"b", entt::resolve<std::string>(), true}},
      {sr::engine::PortDesc{"text", entt::resolve<std::string>(), true}}};
  }

  friend auto tag_invoke(sr::engine::dag::execute_t, ConcatKernel& self, sr::engine::RequestContext&,
                         const sr::engine::InputValues& inputs, sr::engine::OutputValues& outputs) {
    auto a = inputs.get<std::string>(0);
    auto b = inputs.get<std::string>(1);
    auto text = a + self.sep + b;
    return stdexec::just() | stdexec::then([&outputs, text = std::move(text)]() mutable {
             outputs.set<std::string>(0, std::move(text));
           });
  }
};

struct LengthKernel {
  friend auto tag_invoke(sr::engine::dag::get_signature_t, const LengthKernel&) -> sr::engine::Signature {
    return sr::engine::Signature{
      {sr::engine::PortDesc{"text", entt::resolve<std::string>(), true}},
      {sr::engine::PortDesc{"length", entt::resolve<int64_t>(), true}}};
  }

  friend auto tag_invoke(sr::engine::dag::execute_t, LengthKernel&, sr::engine::RequestContext&,
                         const sr::engine::InputValues& inputs, sr::engine::OutputValues& outputs) {
    auto text = inputs.get<std::string>(0);
    return stdexec::just() | stdexec::then([&outputs, length = static_cast<int64_t>(text.size())]() mutable {
             outputs.set<int64_t>(0, length);
           });
  }
};

struct ToStringKernel {
  friend auto tag_invoke(sr::engine::dag::get_signature_t, const ToStringKernel&) -> sr::engine::Signature {
    return sr::engine::Signature{
      {sr::engine::PortDesc{"value", entt::resolve<int64_t>(), true}},
      {sr::engine::PortDesc{"text", entt::resolve<std::string>(), true}}};
  }

  friend auto tag_invoke(sr::engine::dag::execute_t, ToStringKernel&, sr::engine::RequestContext&,
                         const sr::engine::InputValues& inputs, sr::engine::OutputValues& outputs) {
    auto value = inputs.get<int64_t>(0);
    auto text = std::to_string(value);
    return stdexec::just() | stdexec::then([&outputs, text = std::move(text)]() mutable {
             outputs.set<std::string>(0, std::move(text));
           });
  }
};

struct FanoutI64Kernel {
  friend auto tag_invoke(sr::engine::dag::get_signature_t, const FanoutI64Kernel&) -> sr::engine::Signature {
    return sr::engine::Signature{
      {sr::engine::PortDesc{"value", entt::resolve<int64_t>(), true}},
      {sr::engine::PortDesc{"left", entt::resolve<int64_t>(), true},
       sr::engine::PortDesc{"right", entt::resolve<int64_t>(), true}}};
  }

  friend auto tag_invoke(sr::engine::dag::execute_t, FanoutI64Kernel&, sr::engine::RequestContext&,
                         const sr::engine::InputValues& inputs, sr::engine::OutputValues& outputs) {
    auto value = inputs.get<int64_t>(0);
    return stdexec::just() | stdexec::then([&outputs, value]() mutable {
             outputs.set<int64_t>(0, value);
             outputs.set<int64_t>(1, value);
           });
  }
};

struct SinkI64Kernel {
  friend auto tag_invoke(sr::engine::dag::get_signature_t, const SinkI64Kernel&) -> sr::engine::Signature {
    return sr::engine::Signature{
      {sr::engine::PortDesc{"value", entt::resolve<int64_t>(), true}},
      {}};
  }

  friend auto tag_invoke(sr::engine::dag::execute_t, SinkI64Kernel&, sr::engine::RequestContext&,
                         const sr::engine::InputValues&, sr::engine::OutputValues&) {
    return stdexec::just();
  }
};

struct SinkStringKernel {
  friend auto tag_invoke(sr::engine::dag::get_signature_t, const SinkStringKernel&) -> sr::engine::Signature {
    return sr::engine::Signature{
      {sr::engine::PortDesc{"text", entt::resolve<std::string>(), true}},
      {}};
  }

  friend auto tag_invoke(sr::engine::dag::execute_t, SinkStringKernel&, sr::engine::RequestContext&,
                         const sr::engine::InputValues&, sr::engine::OutputValues&) {
    return stdexec::just();
  }
};

struct FormatKernel {
  std::string prefix;

  friend auto tag_invoke(sr::engine::dag::get_signature_t, const FormatKernel&) -> sr::engine::Signature {
    return sr::engine::Signature{
      {sr::engine::PortDesc{"value", entt::resolve<int64_t>(), true}},
      {sr::engine::PortDesc{"text", entt::resolve<std::string>(), true}}};
  }

  friend auto tag_invoke(sr::engine::dag::execute_t, FormatKernel& self, sr::engine::RequestContext&,
                         const sr::engine::InputValues& inputs, sr::engine::OutputValues& outputs) {
    auto value = inputs.get<int64_t>(0);
    return stdexec::just() | stdexec::then([
      &outputs,
      text = self.prefix + std::to_string(value)
    ]() mutable {
             outputs.set<std::string>(0, std::move(text));
           });
  }
};

}  // namespace

auto register_builtin_types() -> void {
  sr::engine::register_type<int64_t>("int64");
  sr::engine::register_type<double>("double");
  sr::engine::register_type<bool>("bool");
  sr::engine::register_type<std::string>("string");
}

auto register_sample_kernels(sr::engine::KernelRegistry& registry) -> void {
  register_simple<AddKernel>(registry, "add");
  register_simple<SubKernel>(registry, "sub_i64");
  register_simple<NegateI64Kernel>(registry, "negate_i64");
  register_simple<DivKernel>(registry, "div_i64");
  register_simple<GreaterKernel>(registry, "gt_i64");
  register_simple<EqualKernel>(registry, "eq_i64");

  register_simple<NotKernel>(registry, "not_bool");
  register_simple<AndKernel>(registry, "and_bool");
  register_simple<OrKernel>(registry, "or_bool");

  register_simple<IdentityI64Kernel>(registry, "identity_i64");
  register_simple<IdentityStringKernel>(registry, "identity_str");
  register_simple<LengthKernel>(registry, "len_str");
  register_simple<ToStringKernel>(registry, "to_string");
  register_simple<FanoutI64Kernel>(registry, "fanout_i64");
  register_simple<SinkI64Kernel>(registry, "sink_i64");
  register_simple<SinkStringKernel>(registry, "sink_str");

  register_simple<IfElseI64Kernel>(registry, "if_else_i64");
  register_simple<IfElseStringKernel>(registry, "if_else_str");
  register_simple<CoalesceI64Kernel>(registry, "coalesce_i64");
  register_simple<CoalesceStringKernel>(registry, "coalesce_str");

  registry.register_kernel("mul", [](const sr::engine::Json& params) -> sr::engine::Expected<sr::engine::KernelHandle> {
    MulKernel kernel;
    kernel.factor = get_int_param(params, "factor", 1);
    return sr::engine::make_kernel_handle(std::move(kernel));
  });

  registry.register_kernel("const_i64", [](const sr::engine::Json& params) -> sr::engine::Expected<sr::engine::KernelHandle> {
    ConstI64Kernel kernel;
    kernel.value = get_int_param(params, "value", 0);
    return sr::engine::make_kernel_handle(std::move(kernel));
  });

  registry.register_kernel("const_bool", [](const sr::engine::Json& params) -> sr::engine::Expected<sr::engine::KernelHandle> {
    ConstBoolKernel kernel;
    kernel.value = get_bool_param(params, "value", false);
    return sr::engine::make_kernel_handle(std::move(kernel));
  });

  registry.register_kernel("const_str", [](const sr::engine::Json& params) -> sr::engine::Expected<sr::engine::KernelHandle> {
    ConstStringKernel kernel;
    kernel.value = get_string_param(params, "value", "");
    return sr::engine::make_kernel_handle(std::move(kernel));
  });

  registry.register_kernel("concat_str", [](const sr::engine::Json& params) -> sr::engine::Expected<sr::engine::KernelHandle> {
    ConcatKernel kernel;
    kernel.sep = get_string_param(params, "sep", "");
    return sr::engine::make_kernel_handle(std::move(kernel));
  });

  registry.register_kernel("format", [](const sr::engine::Json& params) -> sr::engine::Expected<sr::engine::KernelHandle> {
    FormatKernel kernel;
    kernel.prefix = get_string_param(params, "prefix", "");
    return sr::engine::make_kernel_handle(std::move(kernel));
  });
}

}  // namespace sr::kernel
