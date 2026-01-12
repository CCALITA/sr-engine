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

/// Bounded multi-producer queue used by serve endpoints.
template <typename T>
class RequestQueue {
public:
  explicit RequestQueue(std::size_t capacity) : capacity_(capacity) {}

  auto push(T value) -> bool {
    std::unique_lock<std::mutex> lock(mutex_);
    if (closed_ || (capacity_ > 0 && queue_.size() >= capacity_)) {
      return false;
    }
    queue_.push_back(std::move(value));
    cv_.notify_one();
    return true;
  }

  auto pop(T &out) -> bool {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return closed_ || !queue_.empty(); });
    if (queue_.empty()) {
      return false;
    }
    out = std::move(queue_.front());
    queue_.pop_front();
    return true;
  }

  auto close() -> void {
    std::unique_lock<std::mutex> lock(mutex_);
    closed_ = true;
    cv_.notify_all();
  }

private:
  std::size_t capacity_ = 0;
  std::deque<T> queue_;
  bool closed_ = false;
  std::mutex mutex_;
  std::condition_variable cv_;
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
