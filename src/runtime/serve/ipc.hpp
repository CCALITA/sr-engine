#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "engine/error.hpp"
#include "kernel/rpc_kernels.hpp"

namespace sr::engine::serve {

/// Response sink for IPC requests.
class IpcResponder final {
public:
  explicit IpcResponder(int fd);
  ~IpcResponder();

  IpcResponder(const IpcResponder &) = delete;
  IpcResponder &operator=(const IpcResponder &) = delete;

  auto send(kernel::rpc::RpcResponse response) noexcept -> Expected<void>;

  auto sent() const -> bool { return sent_.load(std::memory_order_acquire); }

private:
  auto close_fd() -> void;

  int fd_ = -1;
  std::atomic<bool> sent_{false};
  std::mutex send_mutex_;
};

inline auto to_rpc_responder(const std::shared_ptr<IpcResponder>& responder)
    -> kernel::rpc::RpcResponder {
  return kernel::rpc::RpcResponder{
      .send = [responder](kernel::rpc::RpcResponse resp) -> sr::engine::Expected<void> {
          return responder->send(std::move(resp));
      },
      .attach_request_state = nullptr,
      .sent = [responder]() -> bool {
          return responder->sent();
      }
  };
}

/// In-flight IPC request envelope handed to the serve layer.
struct IpcEnvelope {
  std::string method;
  grpc::ByteBuffer payload;
  kernel::rpc::RpcMetadata metadata;
  kernel::rpc::RpcResponder responder;
  std::shared_ptr<IpcResponder> responder_ptr;
  std::string peer;
};

/// Unix domain socket transport for unary IPC requests.
class IpcServer {
public:
  using RequestCallback = std::function<void(IpcEnvelope &&)>;

  IpcServer(RequestCallback callback, std::string path, int io_threads,
            int backlog, std::size_t max_message_bytes, bool remove_existing);
  ~IpcServer();

  IpcServer(const IpcServer &) = delete;
  IpcServer &operator=(const IpcServer &) = delete;

  auto start() -> Expected<void>;
  auto shutdown(std::chrono::milliseconds timeout) -> void;
  auto port() const -> int { return 0; }

private:
  auto accept_loop() -> void;
  auto worker_loop() -> void;
  auto enqueue_socket(int fd) -> void;
  auto handle_connection(int fd) -> void;

  RequestCallback callback_;
  std::string path_;
  int io_threads_ = 1;
  int backlog_ = 128;
  std::size_t max_message_bytes_ = 0;
  bool remove_existing_ = true;

  std::atomic<bool> running_{false};
  std::atomic<bool> stopping_{false};
  int listen_fd_ = -1;

  std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::deque<int> socket_queue_;
  std::thread accept_thread_;
  std::vector<std::thread> workers_;
};

} // namespace sr::engine::serve
