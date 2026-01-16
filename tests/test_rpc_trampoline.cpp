#include "test_support.hpp"

#include "kernel/rpc_trampoline.hpp"
#include "kernel/rpc_kernels.hpp"

#include <chrono>
#include <string>
#include <thread>

namespace {

auto test_trampoline_config_defaults() -> bool {
  sr::kernel::rpc::RuntimeConfig config;

  if (config.transport_type != "grpc") return false;
  if (config.serializer_type != "json") return false;
  if (!config.middleware_types.empty()) return false;
  if (config.target != "") return false;
  if (config.use_tls != false) return false;
  if (config.timeout.count() != 5000) return false;
  if (config.retry_attempts != 3) return false;
  if (config.retry_base_delay.count() != 100) return false;

  return true;
}

auto test_trampoline_config_custom() -> bool {
  sr::kernel::rpc::RuntimeConfig config;
  config.transport_type = "http";
  config.serializer_type = "flatbuffer";
  config.middleware_types = {"retry", "timeout"};
  config.target = "localhost:8080";
  config.use_tls = true;
  config.timeout = std::chrono::seconds(10);
  config.retry_attempts = 5;
  config.retry_base_delay = std::chrono::milliseconds(200);

  if (config.transport_type != "http") return false;
  if (config.serializer_type != "flatbuffer") return false;
  if (config.middleware_types.size() != 2) return false;
  if (config.target != "localhost:8080") return false;
  if (config.use_tls != true) return false;
  if (config.timeout.count() != 10000) return false;
  if (config.retry_attempts != 5) return false;
  if (config.retry_base_delay.count() != 200) return false;

  return true;
}

auto test_trampoline_reconfigure() -> bool {
  sr::kernel::rpc::RuntimeConfig config1;
  config1.target = "localhost:50051";

  sr::kernel::rpc::RpcTrampoline tramp(config1);

  if (tramp.config().target != "localhost:50051") return false;

  sr::kernel::rpc::RuntimeConfig config2;
  config2.target = "localhost:8080";

  auto result = tramp.reconfigure(config2);
  if (!result) return false;

  if (tramp.config().target != "localhost:8080") return false;

  return true;
}

auto test_trampoline_registry_singleton() -> bool {
  auto& reg1 = sr::kernel::rpc::TrampolineRegistry::instance();
  auto& reg2 = sr::kernel::rpc::TrampolineRegistry::instance();

  if (&reg1 != &reg2) return false;

  return true;
}

auto test_trampoline_registry_list_transports() -> bool {
  auto& reg = sr::kernel::rpc::TrampolineRegistry::instance();
  auto transports = reg.list_transports();

  if (transports.empty()) return false;

  return true;
}

auto test_trampoline_registry_list_serializers() -> bool {
  auto& reg = sr::kernel::rpc::TrampolineRegistry::instance();
  auto serializers = reg.list_serializers();

  if (serializers.empty()) return false;

  return true;
}

auto test_trampoline_make() -> bool {
  sr::kernel::rpc::RuntimeConfig config;
  config.transport_type = "grpc";
  config.target = "localhost:50051";

  auto trampoline = sr::kernel::rpc::make_trampoline(config);
  if (!trampoline) return false;

  return true;
}

auto test_trampoline_uninitialized_execute() -> bool {
  sr::kernel::rpc::RuntimeConfig config;
  config.transport_type = "grpc";
  config.target = "localhost:50051";

  sr::kernel::rpc::RpcTrampoline tramp(config);

  sr::engine::Json request = {{"method", "test"}};

  auto result = tramp.execute(request);
  if (result) return false;

  return true;
}

auto test_trampoline_invalid_transport() -> bool {
  sr::kernel::rpc::RuntimeConfig config;
  config.transport_type = "invalid_transport";
  config.target = "localhost:50051";

  auto trampoline = sr::kernel::rpc::make_trampoline(config);
  if (trampoline) return false;

  return true;
}

auto test_trampoline_invalid_serializer() -> bool {
  sr::kernel::rpc::RuntimeConfig config;
  config.transport_type = "grpc";
  config.serializer_type = "invalid_serializer";
  config.target = "localhost:50051";

  auto trampoline = sr::kernel::rpc::make_trampoline(config);
  if (trampoline) return false;

  return true;
}

} // namespace

auto test_rpc_trampoline_config() -> bool {
  if (!test_trampoline_config_defaults()) return false;
  if (!test_trampoline_config_custom()) return false;
  return true;
}

auto test_rpc_trampoline_reconfigure() -> bool {
  if (!test_trampoline_reconfigure()) return false;
  return true;
}

auto test_rpc_trampoline_registry() -> bool {
  if (!test_trampoline_registry_singleton()) return false;
  if (!test_trampoline_registry_list_transports()) return false;
  if (!test_trampoline_registry_list_serializers()) return false;
  return true;
}

auto test_rpc_trampoline_make() -> bool {
  if (!test_trampoline_make()) return false;
  return true;
}

auto test_rpc_trampoline_errors() -> bool {
  if (!test_trampoline_uninitialized_execute()) return false;
  if (!test_trampoline_invalid_transport()) return false;
  if (!test_trampoline_invalid_serializer()) return false;
  return true;
}
