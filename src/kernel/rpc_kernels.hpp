#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "engine/registry.hpp"

#include <grpcpp/generic/generic_stub.h>
#include <grpcpp/grpcpp.h>

namespace sr::kernel::rpc {

/// Key/value metadata entry used for RPC headers and trailers.
struct RpcMetadataEntry {
  std::string key;
  std::string value;
};

/// Collection of RPC metadata entries (order-preserving, allows duplicates).
struct RpcMetadata {
  std::vector<RpcMetadataEntry> entries;
};

/// Status payload for RPC responses.
struct RpcStatus {
  grpc::StatusCode code = grpc::StatusCode::OK;
  std::string message;
  std::string details;
};

/// RPC response payload plus status and trailing metadata.
struct RpcResponse {
  grpc::ByteBuffer payload;
  RpcStatus status;
  RpcMetadata trailing_metadata;
};

/// Type-erased response sink used by rpc_server_output.
struct RpcResponder {
  using Callback = std::function<sr::engine::Expected<void>(RpcResponse)>;
  using AttachCallback = std::function<void(void*)>;
  using SentCallback = std::function<bool()>;

  Callback send;
  AttachCallback attach_request_state;
  SentCallback sent;

  auto operator()(RpcResponse response) const
      -> sr::engine::Expected<void> {
    return send(response);
  }

  auto operator()(RpcResponse response, grpc::StatusCode code,
                  std::string message) const -> sr::engine::Expected<void> {
    RpcResponse resp = std::move(response);
    resp.status.code = code;
    resp.status.message = std::move(message);
    return send(resp);
  }

  explicit operator bool() const { return static_cast<bool>(send); }
};

/// Per-request server call handle injected via env binding.
struct RpcServerCall {
  std::string method;
  grpc::ByteBuffer request;
  RpcMetadata metadata;
  RpcResponder responder;
};

} // namespace sr::kernel::rpc

namespace sr::kernel {

/// Register gRPC value types used by rpc kernels.
auto register_rpc_types(sr::engine::TypeRegistry &registry) -> void;

/// Register rpc input/output/codec kernels into a registry.
auto register_rpc_kernels(sr::engine::KernelRegistry &registry) -> void;
} // namespace sr::kernel
