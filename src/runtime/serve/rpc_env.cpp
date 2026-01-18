#include "runtime/serve/rpc_env.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <format>
#include <string>
#include <string_view>

#include "runtime/serve/grpc.hpp"
#include "runtime/serve/ipc.hpp"
#include "engine/type_names.hpp"

namespace sr::engine::serve {
namespace {

constexpr std::string_view kRpcCallKey = "rpc.call";
constexpr std::string_view kRpcMethodKey = "rpc.method";
constexpr std::string_view kRpcPayloadKey = "rpc.payload";
constexpr std::string_view kRpcMetadataKey = "rpc.metadata";
constexpr std::string_view kRpcPeerKey = "rpc.peer";
constexpr std::string_view kRpcDeadlineKey = "rpc.deadline_ms";

auto ascii_iequals(std::string_view lhs, std::string_view rhs) -> bool {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (std::size_t i = 0; i < lhs.size(); ++i) {
    const auto a = static_cast<unsigned char>(lhs[i]);
    const auto b = static_cast<unsigned char>(rhs[i]);
    if (std::tolower(a) != std::tolower(b)) {
      return false;
    }
  }
  return true;
}

auto to_string_view(const grpc::string_ref &ref) -> std::string_view {
  return std::string_view(ref.data(), ref.size());
}

auto remaining_deadline_ms(const RequestContext &ctx) -> std::int64_t {
  if (ctx.deadline == std::chrono::steady_clock::time_point::max()) {
    return -1;
  }
  const auto now = std::chrono::steady_clock::now();
  if (ctx.deadline <= now) {
    return 0;
  }
  const auto remaining =
      std::chrono::duration_cast<std::chrono::milliseconds>(ctx.deadline -
                                                            now);
  return remaining.count();
}

auto validate_type(const EnvRequirement &req, TypeId expected,
                   std::string_view key) -> Expected<void> {
  if (expected == TypeId{}) {
    return tl::unexpected(make_error(
        std::format("missing type registration for env key: {}", key)));
  }
  if (req.type_id != TypeId{} && req.type_id != expected) {
    return tl::unexpected(make_error(
        std::format("env type mismatch for key: {}", key)));
  }
  return {};
}

} // namespace

auto analyze_rpc_env(const ExecPlan &plan) -> Expected<RpcEnvBindings> {
  RpcEnvBindings bindings;
  for (const auto &req : plan.env_requirements) {
    if (req.key == kRpcCallKey) {
      if (auto ok =
              validate_type(req, TypeName<kernel::rpc::RpcServerCall>::id(),
                            req.key);
          !ok) {
        return tl::unexpected(ok.error());
      }
      bindings.call = true;
      continue;
    }
    if (req.key == kRpcMethodKey) {
      if (auto ok =
              validate_type(req, TypeName<std::string>::id(), req.key);
          !ok) {
        return tl::unexpected(ok.error());
      }
      bindings.method = true;
      continue;
    }
    if (req.key == kRpcPayloadKey) {
      if (auto ok =
              validate_type(req, TypeName<grpc::ByteBuffer>::id(), req.key);
          !ok) {
        return tl::unexpected(ok.error());
      }
      bindings.payload = true;
      continue;
    }
    if (req.key == kRpcMetadataKey) {
      if (auto ok =
              validate_type(req, TypeName<kernel::rpc::RpcMetadata>::id(),
                            req.key);
          !ok) {
        return tl::unexpected(ok.error());
      }
      bindings.metadata = true;
      continue;
    }
    if (req.key == kRpcPeerKey) {
      if (auto ok =
              validate_type(req, TypeName<std::string>::id(), req.key);
          !ok) {
        return tl::unexpected(ok.error());
      }
      bindings.peer = true;
      continue;
    }
    if (req.key == kRpcDeadlineKey) {
      if (auto ok =
              validate_type(req, TypeName<std::int64_t>::id(), req.key);
          !ok) {
        return tl::unexpected(ok.error());
      }
      bindings.deadline_ms = true;
      continue;
    }
    return tl::unexpected(make_error(
        std::format("unsupported serve env key for rpc transport: {}",
                    req.key)));
  }
  return bindings;
}

auto populate_grpc_env(RequestContext &ctx, GrpcEnvelope &env,
                       const RpcEnvBindings &bindings) -> Expected<void> {
  if (bindings.call) {
    kernel::rpc::RpcServerCall call;
    call.method = env.method;
    call.request = env.payload;
    call.metadata = env.metadata;
    call.responder = env.responder;
    ctx.set_env(std::string(kRpcCallKey), std::move(call));
  }
  if (bindings.method) {
    ctx.set_env(std::string(kRpcMethodKey), env.method);
  }
  if (bindings.payload) {
    ctx.set_env(std::string(kRpcPayloadKey), env.payload);
  }
  if (bindings.metadata) {
    ctx.set_env(std::string(kRpcMetadataKey), env.metadata);
  }
  if (bindings.peer) {
    const auto peer =
        env.context ? std::string(env.context->peer()) : std::string();
    ctx.set_env(std::string(kRpcPeerKey), peer);
  }
  if (bindings.deadline_ms) {
    ctx.set_env(std::string(kRpcDeadlineKey), remaining_deadline_ms(ctx));
  }
  return {};
}

auto populate_ipc_env(RequestContext &ctx, IpcEnvelope &env,
                      const RpcEnvBindings &bindings) -> Expected<void> {
  if (bindings.call) {
    kernel::rpc::RpcServerCall call;
    call.method = env.method;
    call.request = env.payload;
    call.metadata = env.metadata;
    call.responder = env.responder;
    ctx.set_env(std::string(kRpcCallKey), std::move(call));
  }
  if (bindings.method) {
    ctx.set_env(std::string(kRpcMethodKey), env.method);
  }
  if (bindings.payload) {
    ctx.set_env(std::string(kRpcPayloadKey), env.payload);
  }
  if (bindings.metadata) {
    ctx.set_env(std::string(kRpcMetadataKey), env.metadata);
  }
  if (bindings.peer) {
    ctx.set_env(std::string(kRpcPeerKey), env.peer);
  }
  if (bindings.deadline_ms) {
    ctx.set_env(std::string(kRpcDeadlineKey), remaining_deadline_ms(ctx));
  }
  return {};
}

auto find_grpc_metadata_value(const grpc::ServerContext &ctx,
                              std::string_view key)
    -> std::optional<std::string_view> {
  if (key.empty()) {
    return std::nullopt;
  }
  const auto &metadata = ctx.client_metadata();
  for (const auto &entry : metadata) {
    const auto entry_key = to_string_view(entry.first);
    if (ascii_iequals(entry_key, key)) {
      return to_string_view(entry.second);
    }
  }
  return std::nullopt;
}

auto find_ipc_metadata_value(const kernel::rpc::RpcMetadata &metadata,
                             std::string_view key)
    -> std::optional<std::string_view> {
  if (key.empty()) {
    return std::nullopt;
  }
  for (const auto &entry : metadata.entries) {
    if (entry.key == key) {
      return entry.value;
    }
  }
  return std::nullopt;
}

} // namespace sr::engine::serve
