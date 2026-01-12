#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/generic/async_generic_service.h>
#include <grpcpp/grpcpp.h>

#include "engine/error.hpp"
#include "kernel/rpc_kernels.hpp"
#include "runtime/serve/common.hpp"

namespace sr::engine::serve {

/// In-flight gRPC request envelope handed to the serve layer.
struct GrpcEnvelope {
  grpc::GenericServerContext *context = nullptr;
  std::string method;
  grpc::ByteBuffer payload;
  kernel::rpc::RpcMetadata metadata;
  std::shared_ptr<class GrpcResponder> responder;
};

/// Response sink for gRPC requests.
class GrpcResponder final : public kernel::rpc::RpcResponder {
public:
  explicit GrpcResponder(std::shared_ptr<class GrpcCall> call);

  auto send(kernel::rpc::RpcResponse response) noexcept
      -> Expected<void> override;

  auto attach_request_state(const std::shared_ptr<RequestState> &state)
      -> void;

  auto sent() const -> bool { return sent_.load(std::memory_order_acquire); }

private:
  std::shared_ptr<class GrpcCall> call_;
  std::atomic<bool> sent_{false};
};

/// Async gRPC unary server transport.
class GrpcServer {
public:
  using RequestCallback = std::function<void(GrpcEnvelope &&)>;

  GrpcServer(RequestCallback callback, std::string address, int io_threads);
  ~GrpcServer();

  GrpcServer(const GrpcServer &) = delete;
  GrpcServer &operator=(const GrpcServer &) = delete;

  auto start() -> Expected<void>;
  auto shutdown(std::chrono::milliseconds timeout) -> void;
  auto port() const -> int { return port_; }

private:
  friend class GrpcCall;

  auto start_call(grpc::ServerCompletionQueue *cq) -> void;
  auto worker_loop(grpc::ServerCompletionQueue *cq) -> void;

  RequestCallback callback_;
  std::string address_;
  int io_threads_ = 1;
  grpc::AsyncGenericService service_;
  std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> cqs_;
  std::unique_ptr<grpc::Server> server_;
  std::vector<std::thread> workers_;
  std::atomic<bool> running_{false};
  int port_ = 0;
};

} // namespace sr::engine::serve
