#ifndef SR_RPC_POLICIES_HPP
#define SR_RPC_POLICIES_HPP

#include "kernel/rpc_kernels.hpp"

#include "engine/error.hpp"
#include "engine/types.hpp"

#include <concepts>
#include <functional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <vector>

namespace sr::kernel::rpc {

struct Envelope {
  std::string method;
  std::vector<std::byte> payload;
  RpcMetadata metadata;
};

template <typename T>
concept Transport = requires(T& t, const Envelope& e, std::vector<Envelope> ev) {
  { t.send(e) } -> std::same_as<engine::Expected<Envelope>>;
  { t.send_batch(ev) } -> std::same_as<engine::Expected<std::vector<Envelope>>>;
  { t.connected() } -> std::same_as<bool>;
  { t.health_check() } -> std::same_as<engine::Expected<bool>>;
};

template <typename T>
concept Serializer = requires(T& s, const engine::Json& j, const std::vector<std::byte>& b) {
  { s.get_name() } -> std::same_as<std::string_view>;
  { s.content_type() } -> std::same_as<std::string_view>;
  { s.serialize(j) } -> std::same_as<engine::Expected<std::vector<std::byte>>>;
  { s.deserialize(b) } -> std::same_as<engine::Expected<engine::Json>>;
};

template <typename T>
concept Middleware = requires(T& m, const Envelope& e) {
  { m.name() } -> std::same_as<std::string_view>;
  { m.before_send(e) } -> std::same_as<engine::Expected<Envelope>>;
  { m.after_receive(e) } -> std::same_as<engine::Expected<Envelope>>;
};

template <typename T>
concept LoadBalancer = requires(T& lb) {
  { lb.select() } -> std::convertible_to<void*>;
  { lb.release(std::declval<void*>(), true) } -> std::same_as<void>;
  { lb.add(std::declval<void*>()) } -> std::same_as<void>;
  { lb.remove(std::declval<void*>()) } -> std::same_as<void>;
};

template <typename... Middlewares>
struct MiddlewareChain;

template <>
struct MiddlewareChain<> {
  static auto before_send(Envelope e) -> engine::Expected<Envelope> { return e; }
  static auto after_receive(Envelope e) -> engine::Expected<Envelope> { return e; }
  static auto name() -> std::string_view { return "none"; }
};

template <typename First, typename... Rest>
struct MiddlewareChain<First, Rest...> : First, MiddlewareChain<Rest...> {
  using First::before_send;
  using First::after_receive;

  static auto before_send(Envelope e) -> engine::Expected<Envelope> {
    auto modified = First{}.before_send(e);
    if (!modified) return tl::unexpected(modified.error());
    return MiddlewareChain<Rest...>::before_send(*modified);
  }

  static auto after_receive(Envelope e) -> engine::Expected<Envelope> {
    auto modified = First{}.after_receive(e);
    if (!modified) return tl::unexpected(modified.error());
    return MiddlewareChain<Rest...>::after_receive(*modified);
  }

  static auto name() -> std::string_view { return First{}.name(); }
};

template <typename T>
struct RetryPolicy {
  static constexpr std::string_view name() { return "retry"; }

  std::uint32_t max_attempts = 3;
  std::chrono::milliseconds base_delay{100};
  double max_delay_factor = 4.0;
  std::vector<grpc::StatusCode> retryable_codes = {
      grpc::StatusCode::UNAVAILABLE,
      grpc::StatusCode::DEADLINE_EXCEEDED,
      grpc::StatusCode::INTERNAL,
  };

  auto before_send(Envelope e) -> engine::Expected<Envelope> { return e; }

  auto after_receive(Envelope e) -> engine::Expected<Envelope> { return e; }
};

template <typename T>
struct CircuitBreakerPolicy {
  static constexpr std::string_view name() { return "circuit_breaker"; }

  std::uint32_t failure_threshold = 5;
  std::chrono::milliseconds reset_timeout{30000};

  auto before_send(Envelope e) -> engine::Expected<Envelope> { return e; }

  auto after_receive(Envelope e) -> engine::Expected<Envelope> { return e; }
};

template <typename T>
struct TimeoutPolicy {
  static constexpr std::string_view name() { return "timeout"; }

  std::chrono::milliseconds timeout{5000};

  auto before_send(Envelope e) -> engine::Expected<Envelope> { return e; }

  auto after_receive(Envelope e) -> engine::Expected<Envelope> { return e; }
};

struct RoundRobinLoadBalancer {
  template <Transport T>
  auto select() -> T* {
    return nullptr;
  }

  auto release(void*, bool) -> void {}
  auto add(void*) -> void {}
  auto remove(void*) -> void {}
};

} // namespace sr::kernel::rpc

#endif // SR_RPC_POLICIES_HPP
