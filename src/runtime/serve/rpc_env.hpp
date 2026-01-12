#pragma once

#include <optional>
#include <string_view>

#include <grpcpp/grpcpp.h>

#include "engine/error.hpp"
#include "engine/plan.hpp"
#include "engine/types.hpp"
#include "kernel/rpc_kernels.hpp"

namespace sr::engine::serve {

struct GrpcEnvelope;
struct IpcEnvelope;

/// Resolved env requirements for rpc-based transports.
struct RpcEnvBindings {
  bool call = false;
  bool method = false;
  bool payload = false;
  bool metadata = false;
  bool peer = false;
  bool deadline_ms = false;
};

/// Inspect an ExecPlan and return the required rpc env bindings.
auto analyze_rpc_env(const ExecPlan &plan) -> Expected<RpcEnvBindings>;

/// Populate RequestContext.env for a gRPC request.
auto populate_grpc_env(RequestContext &ctx, GrpcEnvelope &env,
                       const RpcEnvBindings &bindings) -> Expected<void>;

/// Populate RequestContext.env for an IPC request.
auto populate_ipc_env(RequestContext &ctx, IpcEnvelope &env,
                      const RpcEnvBindings &bindings) -> Expected<void>;

/// Find a metadata value on a gRPC context.
auto find_grpc_metadata_value(const grpc::ServerContext &ctx,
                              std::string_view key)
    -> std::optional<std::string_view>;

/// Find a metadata value on an IPC request.
auto find_ipc_metadata_value(const sr::kernel::rpc::RpcMetadata &metadata,
                             std::string_view key)
    -> std::optional<std::string_view>;

} // namespace sr::engine::serve
