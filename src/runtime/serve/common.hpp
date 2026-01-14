#pragma once

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <utility>

#include <grpcpp/grpcpp.h>

#include "engine/error.hpp"
#include "engine/types.hpp"
#include "kernel/rpc_kernels.hpp"

namespace sr::engine::serve {

/// Per-request state held while a serve request executes.
struct RequestState {
  RequestContext ctx;
};

/// Helper to ignore transport send results.
inline auto ignore_send(const Expected<void> &) -> void {}
inline auto ignore_send(Expected<void> &&) -> void {}

/// Build a minimal error response for RPC transports.
inline auto make_error_response(grpc::StatusCode code, std::string message,
                                std::string details = {})
    -> sr::kernel::rpc::RpcResponse {
  sr::kernel::rpc::RpcResponse response;
  response.status.code = code;
  response.status.message = std::move(message);
  response.status.details = std::move(details);
  return response;
}

} // namespace sr::engine::serve
