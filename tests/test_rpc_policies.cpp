#include "test_support.hpp"

#include "kernel/rpc_policies.hpp"
#include "kernel/rpc_kernels.hpp"

#include <chrono>
#include <cmath>
#include <cstdint>
#include <vector>

namespace {

auto test_retry_policy_is_retryable_codes() -> bool {
  sr::kernel::rpc::RetryPolicy<void> retry;
  retry.retryable_codes = {
      grpc::StatusCode::UNAVAILABLE,
      grpc::StatusCode::DEADLINE_EXCEEDED,
      grpc::StatusCode::INTERNAL,
  };

  auto is_retryable = [&retry](grpc::StatusCode code) -> bool {
    for (const auto& c : retry.retryable_codes) {
      if (c == code) return true;
    }
    return false;
  };

  if (!is_retryable(grpc::StatusCode::UNAVAILABLE)) return false;
  if (!is_retryable(grpc::StatusCode::DEADLINE_EXCEEDED)) return false;
  if (!is_retryable(grpc::StatusCode::INTERNAL)) return false;
  if (is_retryable(grpc::StatusCode::OK)) return false;
  if (is_retryable(grpc::StatusCode::INVALID_ARGUMENT)) return false;

  return true;
}

auto test_retry_backoff_calculation() -> bool {
  sr::kernel::rpc::RetryPolicy<void> retry;
  retry.base_delay = std::chrono::milliseconds(100);
  retry.max_delay_factor = 4.0;

  auto calculate_backoff = [&retry](std::uint32_t attempt) -> std::chrono::milliseconds {
    double factor = std::min(std::pow(2.0, static_cast<double>(attempt)),
                             retry.max_delay_factor);
    auto delay_ms = static_cast<std::int64_t>(retry.base_delay.count() * factor);
    return std::chrono::milliseconds(delay_ms);
  };

  auto b0 = calculate_backoff(0);
  if (b0.count() != 100) return false;

  auto b1 = calculate_backoff(1);
  if (b1.count() != 200) return false;

  auto b2 = calculate_backoff(2);
  if (b2.count() != 400) return false;

  auto b3 = calculate_backoff(3);
  if (b3.count() != 400) return false;

  auto b10 = calculate_backoff(10);
  if (b10.count() != 400) return false;

  return true;
}

auto test_retry_with_attempts() -> bool {
  sr::kernel::rpc::RetryPolicy<void> retry;
  retry.max_attempts = 3;

  if (retry.max_attempts != 3) return false;

  retry.max_attempts = 5;
  if (retry.max_attempts != 5) return false;

  return true;
}

auto test_retry_with_custom_delay() -> bool {
  sr::kernel::rpc::RetryPolicy<void> retry;
  retry.base_delay = std::chrono::milliseconds(250);

  if (retry.base_delay.count() != 250) return false;

  return true;
}

auto test_circuit_breaker_with_threshold() -> bool {
  sr::kernel::rpc::CircuitBreakerPolicy<void> cb;
  cb.failure_threshold = 10;

  if (cb.failure_threshold != 10) return false;

  return true;
}

auto test_circuit_breaker_with_reset_timeout() -> bool {
  sr::kernel::rpc::CircuitBreakerPolicy<void> cb;
  cb.reset_timeout = std::chrono::seconds(60);

  if (cb.reset_timeout.count() != 60000) return false;

  return true;
}

auto test_timeout_with_custom_duration() -> bool {
  sr::kernel::rpc::TimeoutPolicy<void> timeout;
  timeout.timeout = std::chrono::seconds(30);

  if (timeout.timeout.count() != 30000) return false;

  return true;
}

auto test_middleware_chain_single_passthrough() -> bool {
  struct Passthrough {
    static constexpr std::string_view name() { return "passthrough"; }
    auto before_send(sr::kernel::rpc::Envelope e) -> sr::engine::Expected<sr::kernel::rpc::Envelope> { return e; }
    auto after_receive(sr::kernel::rpc::Envelope e) -> sr::engine::Expected<sr::kernel::rpc::Envelope> { return e; }
  };

  using Chain = sr::kernel::rpc::MiddlewareChain<Passthrough>;

  sr::kernel::rpc::Envelope env;
  env.method = "test";
  env.payload = std::vector<std::byte>{std::byte{1}, std::byte{2}};

  auto before = Chain::before_send(env);
  if (!before) return false;
  if (before->method != "test") return false;
  if (before->payload.size() != 2) return false;

  auto after = Chain::after_receive(env);
  if (!after) return false;

  return true;
}

auto test_middleware_chain_double_passthrough() -> bool {
  struct First {
    static constexpr std::string_view name() { return "first"; }
    auto before_send(sr::kernel::rpc::Envelope e) -> sr::engine::Expected<sr::kernel::rpc::Envelope> { return e; }
    auto after_receive(sr::kernel::rpc::Envelope e) -> sr::engine::Expected<sr::kernel::rpc::Envelope> { return e; }
  };

  struct Second {
    static constexpr std::string_view name() { return "second"; }
    auto before_send(sr::kernel::rpc::Envelope e) -> sr::engine::Expected<sr::kernel::rpc::Envelope> { return e; }
    auto after_receive(sr::kernel::rpc::Envelope e) -> sr::engine::Expected<sr::kernel::rpc::Envelope> { return e; }
  };

  using Chain = sr::kernel::rpc::MiddlewareChain<First, Second>;

  sr::kernel::rpc::Envelope env;
  env.method = "test";

  auto before = Chain::before_send(env);
  if (!before) return false;
  if (Chain::name() != "first") return false;

  return true;
}

auto test_middleware_chain_modifies_request() -> bool {
  struct AddHeader {
    static constexpr std::string_view name() { return "add_header"; }
    auto before_send(sr::kernel::rpc::Envelope e) -> sr::engine::Expected<sr::kernel::rpc::Envelope> {
      e.metadata.entries.push_back({"X-Test", "true"});
      return e;
    }
    auto after_receive(sr::kernel::rpc::Envelope e) -> sr::engine::Expected<sr::kernel::rpc::Envelope> { return e; }
  };

  using Chain = sr::kernel::rpc::MiddlewareChain<AddHeader>;

  sr::kernel::rpc::Envelope env;
  env.method = "test";

  auto result = Chain::before_send(env);
  if (!result) return false;
  if (result->metadata.entries.empty()) return false;
  if (result->metadata.entries[0].key != "X-Test") return false;
  if (result->metadata.entries[0].value != "true") return false;

  return true;
}

auto test_middleware_chain_modifies_response() -> bool {
  struct AddMetadata {
    static constexpr std::string_view name() { return "add_metadata"; }
    auto before_send(sr::kernel::rpc::Envelope e) -> sr::engine::Expected<sr::kernel::rpc::Envelope> { return e; }
    auto after_receive(sr::kernel::rpc::Envelope e) -> sr::engine::Expected<sr::kernel::rpc::Envelope> {
      e.metadata.entries.push_back({"X-Latency-Ms", "100"});
      return e;
    }
  };

  using Chain = sr::kernel::rpc::MiddlewareChain<AddMetadata>;

  sr::kernel::rpc::Envelope env;
  env.method = "test";

  auto result = Chain::after_receive(env);
  if (!result) return false;
  if (result->metadata.entries.empty()) return false;
  if (result->metadata.entries[0].key != "X-Latency-Ms") return false;

  return true;
}

auto test_middleware_chain_error_propagation() -> bool {
  struct FailingMiddleware {
    static constexpr std::string_view name() { return "failing"; }
    auto before_send(sr::kernel::rpc::Envelope) -> sr::engine::Expected<sr::kernel::rpc::Envelope> {
      return tl::unexpected(sr::engine::make_error("middleware error"));
    }
    auto after_receive(sr::kernel::rpc::Envelope e) -> sr::engine::Expected<sr::kernel::rpc::Envelope> { return e; }
  };

  using Chain = sr::kernel::rpc::MiddlewareChain<FailingMiddleware>;

  sr::kernel::rpc::Envelope env;
  env.method = "test";

  auto result = Chain::before_send(env);
  if (result) return false;

  return true;
}

} // namespace

auto test_rpc_retry_policy() -> bool {
  if (!test_retry_policy_is_retryable_codes()) return false;
  if (!test_retry_backoff_calculation()) return false;
  if (!test_retry_with_attempts()) return false;
  if (!test_retry_with_custom_delay()) return false;
  return true;
}

auto test_rpc_circuit_breaker_policy() -> bool {
  if (!test_circuit_breaker_with_threshold()) return false;
  if (!test_circuit_breaker_with_reset_timeout()) return false;
  return true;
}

auto test_rpc_timeout_policy() -> bool {
  if (!test_timeout_with_custom_duration()) return false;
  return true;
}

auto test_rpc_middleware_chain() -> bool {
  if (!test_middleware_chain_single_passthrough()) return false;
  if (!test_middleware_chain_double_passthrough()) return false;
  if (!test_middleware_chain_modifies_request()) return false;
  if (!test_middleware_chain_modifies_response()) return false;
  if (!test_middleware_chain_error_propagation()) return false;
  return true;
}
