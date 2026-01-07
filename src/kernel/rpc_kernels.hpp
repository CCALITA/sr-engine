#pragma once

#include <memory>
#include <string>
#include <vector>

#include "engine/registry.hpp"

#if SR_ENGINE_WITH_GRPC
#include <grpcpp/generic/generic_stub.h>
#include <grpcpp/grpcpp.h>
#endif

namespace sr::kernel::rpc {

#if SR_ENGINE_WITH_GRPC

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

/// Response sink interface used by rpc_server_output.
class RpcResponder {
public:
  virtual ~RpcResponder() = default;
  /// Send a response back to the transport; implementers must be noexcept.
  virtual auto send(RpcResponse response) noexcept
      -> sr::engine::Expected<void> = 0;
};

/// Per-request server call handle injected via env binding.
struct RpcServerCall {
  std::string method;
  grpc::ByteBuffer request;
  RpcMetadata metadata;
  std::shared_ptr<RpcResponder> responder;
};

#endif // SR_ENGINE_WITH_GRPC

} // namespace sr::kernel::rpc

namespace sr::kernel {

#if SR_ENGINE_WITH_GRPC

/// Register gRPC value types used by rpc kernels.
auto register_rpc_types() -> void;

/// Register rpc input/output/codec kernels into a registry.
auto register_rpc_kernels(sr::engine::KernelRegistry &registry) -> void;

#else

/// No-op stubs when gRPC is disabled.
inline auto register_rpc_types() -> void {}
inline auto register_rpc_kernels(sr::engine::KernelRegistry &) -> void {}

#endif

} // namespace sr::kernel
