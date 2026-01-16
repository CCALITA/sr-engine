#ifndef SR_RPC_TRAMPOLINE_HPP
#define SR_RPC_TRAMPOLINE_HPP

#include "kernel/rpc_policies.hpp"
#include "kernel/rpc_generic_kernel.hpp"
#include "kernel/rpc_serializer.hpp"
#include "kernel/rpc_kernels.hpp"

#include "engine/error.hpp"
#include "engine/types.hpp"

#include <concepts>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <variant>

namespace sr::kernel::rpc {

struct RuntimeConfig {
  std::string transport_type = "grpc";
  std::string serializer_type = "json";
  std::vector<std::string> middleware_types;
  std::string target;
  bool use_tls = false;
  std::chrono::milliseconds timeout{5000};
  std::uint32_t retry_attempts = 3;
  std::chrono::milliseconds retry_base_delay{100};
};

namespace detail {

template <typename T>
concept TypeErasedCompatible = requires(T& t) {
  { t.send(std::declval<const Envelope&>()) } -> std::same_as<engine::Expected<Envelope>>;
  { t.connected() } -> std::same_as<bool>;
};

struct AnyTransport {
  std::function<engine::Expected<Envelope>(const Envelope&)> send;
  std::function<engine::Expected<std::vector<Envelope>>(const std::vector<Envelope>&)> send_batch;
  std::function<bool()> connected;
  std::function<engine::Expected<bool>()> health_check;
  std::type_index type_index;

  template <typename T>
  requires TypeErasedCompatible<T>
  AnyTransport(T&& impl) : type_index(typeid(T)) {
    send = [&impl](const Envelope& e) { return impl.send(e); };
    send_batch = [&impl](const std::vector<Envelope>& ev) { return impl.send_batch(ev); };
    connected = [&impl]() { return impl.connected(); };
    health_check = [&impl]() { return impl.health_check(); };
  }

  auto operator()(const Envelope& e) -> engine::Expected<Envelope> { return send(e); }
};

struct AnySerializer {
  std::function<std::string_view()> get_name;
  std::function<std::string_view()> content_type;
  std::function<engine::Expected<std::vector<std::byte>>(const engine::Json&)> serialize;
  std::function<engine::Expected<engine::Json>(const std::vector<std::byte>&)> deserialize;
  std::function<bool(const std::string&)> supports_type;
  std::type_index type_index;

  template <typename T>
  requires Serializer<T>
  AnySerializer(T&& impl) : type_index(typeid(T)) {
    get_name = [&impl]() { return impl.get_name(); };
    content_type = [&impl]() { return impl.content_type(); };
    serialize = [&impl](const engine::Json& j) { return impl.serialize(j); };
    deserialize = [&impl](const std::vector<std::byte>& b) { return impl.deserialize(b); };
    supports_type = [&impl](const std::string& s) { return impl.supports_type(s); };
  }
};

} // namespace detail

class RpcTrampoline {
public:
  explicit RpcTrampoline(RuntimeConfig config) : config_(std::move(config)) {
    initialize();
  }

  auto reconfigure(RuntimeConfig new_config) -> engine::Expected<void> {
    config_ = std::move(new_config);
    return initialize();
  }

  auto execute(const engine::Json& request) -> engine::Expected<engine::Json> {
    if (!transport_ || !serializer_) {
      return tl::unexpected(engine::make_error("trampoline not initialized"));
    }

    auto serialized = serializer_->serialize(request);
    if (!serialized) {
      return tl::unexpected(serialized.error());
    }

    Envelope env;
    env.method = config_.target;
    env.payload = std::move(*serialized);

    auto response = (*transport_)(env);
    if (!response) {
      return tl::unexpected(response.error());
    }

    auto deserialized = serializer_->deserialize(response->payload);
    if (!deserialized) {
      return tl::unexpected(deserialized.error());
    }

    return *deserialized;
  }

  auto execute_batch(const std::vector<engine::Json>& requests)
      -> engine::Expected<std::vector<engine::Json>> {
    std::vector<engine::Expected<engine::Json>> results;
    results.reserve(requests.size());

    for (const auto& req : requests) {
      results.push_back(execute(req));
    }

    std::vector<engine::Json> responses;
    responses.reserve(requests.size());

    for (auto& result : results) {
      if (!result) {
        return tl::unexpected(result.error());
      }
      responses.push_back(std::move(*result));
    }

    return responses;
  }

  auto connected() const -> bool {
    return transport_ && transport_->connected();
  }

  auto health_check() -> engine::Expected<bool> {
    return transport_ ? transport_->health_check()
                      : tl::unexpected(engine::make_error("transport not initialized"));
  }

  auto config() const -> const RuntimeConfig& { return config_; }

  auto transport_type() const -> std::string_view {
    return transport_ ? transport_->type_index.name() : "none";
  }

  auto serializer_type() const -> std::string_view {
    return serializer_ ? serializer_->type_index.name() : "none";
  }

private:
  auto initialize() -> engine::Expected<void> {
    transport_.reset();
    serializer_.reset();

    auto transport = create_transport();
    if (!transport) {
      return tl::unexpected(transport.error());
    }
    transport_ = std::move(*transport);

    auto serializer = create_serializer();
    if (!serializer) {
      return tl::unexpected(serializer.error());
    }
    serializer_ = std::move(*serializer);

    return {};
  }

  auto create_transport() -> engine::Expected<detail::AnyTransport> {
    if (config_.transport_type == "grpc") {
      auto creds = config_.use_tls
                       ? std::shared_ptr<grpc::ChannelCredentials>(
                             grpc::SslCredentials(grpc::SslCredentialsOptions()))
                       : std::shared_ptr<grpc::ChannelCredentials>(
                             grpc::InsecureChannelCredentials());
      auto channel = grpc::CreateChannel(config_.target, creds);
      auto stub = std::make_shared<grpc::GenericStub>(std::move(channel));
      return detail::AnyTransport(GrpcTransport{std::move(stub)});
    }
    return tl::unexpected(
        engine::make_error(std::format("unknown transport: {}", config_.transport_type)));
  }

  auto create_serializer() -> engine::Expected<detail::AnySerializer> {
    if (config_.serializer_type == "json") {
      return detail::AnySerializer(JsonSerializer{});
    }
    return tl::unexpected(
        engine::make_error(std::format("unknown serializer: {}", config_.serializer_type)));
  }

  RuntimeConfig config_;
  std::optional<detail::AnyTransport> transport_;
  std::optional<detail::AnySerializer> serializer_;
};

class TrampolineRegistry {
public:
  using Factory = std::function<engine::Expected<std::unique_ptr<RpcTrampoline>>(const RuntimeConfig&)>;

  static TrampolineRegistry& instance() {
    static TrampolineRegistry reg;
    return reg;
  }

  auto register_transport(const std::string& name, Factory factory) -> void {
    transport_factories_[name] = std::move(factory);
  }

  auto register_serializer(const std::string& name, Factory factory) -> void {
    serializer_factories_[name] = std::move(factory);
  }

  auto create(const RuntimeConfig& config) -> engine::Expected<std::unique_ptr<RpcTrampoline>> {
    return std::make_unique<RpcTrampoline>(config);
  }

  auto list_transports() const -> std::vector<std::string> {
    std::vector<std::string> names;
    for (const auto& [name, _] : transport_factories_) {
      names.push_back(name);
    }
    return names;
  }

  auto list_serializers() const -> std::vector<std::string> {
    std::vector<std::string> names;
    for (const auto& [name, _] : serializer_factories_) {
      names.push_back(name);
    }
    return names;
  }

private:
  TrampolineRegistry() = default;

  std::unordered_map<std::string, Factory> transport_factories_;
  std::unordered_map<std::string, Factory> serializer_factories_;
};

inline auto make_trampoline(const RuntimeConfig& config)
    -> engine::Expected<std::unique_ptr<RpcTrampoline>> {
  return TrampolineRegistry::instance().create(config);
}

} // namespace sr::kernel::rpc

#endif // SR_RPC_TRAMPOLINE_HPP
