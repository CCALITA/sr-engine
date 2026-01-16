#include "test_support.hpp"

#include "kernel/rpc_policies.hpp"
#include "kernel/rpc_serializer.hpp"
#include "kernel/rpc_generic_kernel.hpp"
#include "kernel/rpc_kernels.hpp"

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace {

auto test_concept_transport_grpc() -> bool {
  constexpr bool satisfies = sr::kernel::rpc::Transport<sr::kernel::rpc::GrpcTransport>;
  static_assert(satisfies, "GrpcTransport must satisfy Transport concept");
  return true;
}

auto test_concept_serializer_json() -> bool {
  constexpr bool satisfies = sr::kernel::rpc::Serializer<sr::kernel::rpc::JsonSerializer>;
  static_assert(satisfies, "JsonSerializer must satisfy Serializer concept");
  return true;
}

auto test_concept_middleware_retry() -> bool {
  using Retry = sr::kernel::rpc::RetryPolicy<void>;
  constexpr bool satisfies = sr::kernel::rpc::Middleware<Retry>;
  static_assert(satisfies, "RetryPolicy must satisfy Middleware concept");
  return true;
}

auto test_concept_middleware_chain_empty() -> bool {
  using Chain = sr::kernel::rpc::MiddlewareChain<>;
  constexpr bool satisfies = sr::kernel::rpc::Middleware<Chain>;
  static_assert(satisfies, "Empty MiddlewareChain must satisfy Middleware concept");
  return true;
}

auto test_concept_middleware_chain_single() -> bool {
  using Chain = sr::kernel::rpc::MiddlewareChain<sr::kernel::rpc::RetryPolicy<void>>;
  constexpr bool satisfies = sr::kernel::rpc::Middleware<Chain>;
  static_assert(satisfies, "Single MiddlewareChain must satisfy Middleware concept");
  return true;
}

auto test_concept_middleware_chain_multiple() -> bool {
  using Chain = sr::kernel::rpc::MiddlewareChain<
      sr::kernel::rpc::RetryPolicy<void>,
      sr::kernel::rpc::TimeoutPolicy<void>>;
  constexpr bool satisfies = sr::kernel::rpc::Middleware<Chain>;
  static_assert(satisfies, "Multi MiddlewareChain must satisfy Middleware concept");
  return true;
}

auto test_envelope_creation() -> bool {
  sr::kernel::rpc::Envelope env;
  env.method = "TestMethod";
  env.payload = std::vector<std::byte>{std::byte{1}, std::byte{2}, std::byte{3}};
  env.metadata.entries.push_back({"key", "value"});

  if (env.method != "TestMethod") {
    return false;
  }
  if (env.payload.size() != 3) {
    return false;
  }
  if (env.metadata.entries.size() != 1) {
    return false;
  }
  if (env.metadata.entries[0].key != "key") {
    return false;
  }
  return true;
}

auto test_retry_policy_defaults() -> bool {
  sr::kernel::rpc::RetryPolicy<void> retry;

  if (retry.max_attempts != 3) {
    return false;
  }
  if (retry.base_delay.count() != 100) {
    return false;
  }
  if (retry.max_delay_factor != 4.0) {
    return false;
  }
  return true;
}

auto test_retry_policy_custom() -> bool {
  sr::kernel::rpc::RetryPolicy<void> retry;
  retry.max_attempts = 5;
  retry.base_delay = std::chrono::milliseconds(200);
  retry.max_delay_factor = 8.0;

  if (retry.max_attempts != 5) {
    return false;
  }
  if (retry.base_delay.count() != 200) {
    return false;
  }
  if (retry.max_delay_factor != 8.0) {
    return false;
  }
  return true;
}

auto test_circuit_breaker_policy_defaults() -> bool {
  sr::kernel::rpc::CircuitBreakerPolicy<void> cb;

  if (cb.failure_threshold != 5) {
    return false;
  }
  if (cb.reset_timeout.count() != 30000) {
    return false;
  }
  return true;
}

auto test_timeout_policy_defaults() -> bool {
  sr::kernel::rpc::TimeoutPolicy<void> timeout;

  if (timeout.timeout.count() != 5000) {
    return false;
  }
  return true;
}

auto test_timeout_policy_custom() -> bool {
  sr::kernel::rpc::TimeoutPolicy<void> timeout;
  timeout.timeout = std::chrono::milliseconds(10000);

  if (timeout.timeout.count() != 10000) {
    return false;
  }
  return true;
}

auto test_middleware_chain_empty_before_send() -> bool {
  using Chain = sr::kernel::rpc::MiddlewareChain<>;

  sr::kernel::rpc::Envelope env;
  env.method = "test";

  auto result = Chain::before_send(env);
  if (!result) {
    return false;
  }
  if (result->method != "test") {
    return false;
  }
  return true;
}

auto test_middleware_chain_empty_after_receive() -> bool {
  using Chain = sr::kernel::rpc::MiddlewareChain<>;

  sr::kernel::rpc::Envelope env;
  env.method = "test";
  env.payload = std::vector<std::byte>{std::byte{1}};

  auto result = Chain::after_receive(env);
  if (!result) {
    return false;
  }
  if (result->payload.size() != 1) {
    return false;
  }
  return true;
}

auto test_middleware_chain_name() -> bool {
  using EmptyChain = sr::kernel::rpc::MiddlewareChain<>;
  if (EmptyChain::name() != "none") {
    return false;
  }

  using RetryChain = sr::kernel::rpc::MiddlewareChain<sr::kernel::rpc::RetryPolicy<void>>;
  if (RetryChain::name() != "retry") {
    return false;
  }

  return true;
}

auto test_round_robin_load_balancer() -> bool {
  sr::kernel::rpc::RoundRobinLoadBalancer lb;
  lb.release(nullptr, true);
  lb.add(nullptr);
  lb.remove(nullptr);
  return true;
}

auto test_json_serializer_name() -> bool {
  sr::kernel::rpc::JsonSerializer ser;

  if (ser.get_name() != "json") {
    return false;
  }
  if (ser.content_type() != "application/json") {
    return false;
  }
  return true;
}

auto test_json_serializer_supports_type() -> bool {
  sr::kernel::rpc::JsonSerializer ser;

  if (!ser.supports_type("json")) return false;
  if (!ser.supports_type("string")) return false;
  if (!ser.supports_type("int")) return false;
  if (!ser.supports_type("double")) return false;
  if (!ser.supports_type("bool")) return false;
  if (!ser.supports_type("array")) return false;
  if (!ser.supports_type("object")) return false;
  if (ser.supports_type("flatbuffer")) return false;
  if (ser.supports_type("unknown")) return false;

  return true;
}

auto test_json_serializer_roundtrip() -> bool {
  sr::kernel::rpc::JsonSerializer ser;

  sr::engine::Json input = sr::engine::Json::object();
  input["key"] = "value";
  input["number"] = 42;
  input["array"] = sr::engine::Json::array({1, 2, 3});

  auto serialized = ser.serialize(input);
  if (!serialized) {
    return false;
  }

  auto deserialized = ser.deserialize(*serialized);
  if (!deserialized) {
    return false;
  }

  if ((*deserialized)["key"] != "value") return false;
  if ((*deserialized)["number"] != 42) return false;

  return true;
}

auto test_flatbuffer_serializer_stub() -> bool {
  sr::kernel::rpc::FlatBufferSerializer ser;

  if (ser.get_name() != "flatbuffer") {
    return false;
  }
  if (ser.content_type() != "application/flatbuffers") {
    return false;
  }
  if (!ser.supports_type("flatbuffer")) {
    return false;
  }
  if (ser.supports_type("json")) {
    return false;
  }

  auto result = ser.serialize(sr::engine::Json::object());
  if (result) {
    return false;
  }

  return true;
}

} // namespace

auto test_rpc_policies_concepts() -> bool {
  if (!test_concept_transport_grpc()) return false;
  if (!test_concept_serializer_json()) return false;
  if (!test_concept_middleware_retry()) return false;
  if (!test_concept_middleware_chain_empty()) return false;
  if (!test_concept_middleware_chain_single()) return false;
  if (!test_concept_middleware_chain_multiple()) return false;
  return true;
}

auto test_rpc_envelope() -> bool {
  if (!test_envelope_creation()) return false;
  return true;
}

auto test_rpc_serializers() -> bool {
  if (!test_json_serializer_name()) return false;
  if (!test_json_serializer_supports_type()) return false;
  if (!test_json_serializer_roundtrip()) return false;
  if (!test_flatbuffer_serializer_stub()) return false;
  return true;
}
