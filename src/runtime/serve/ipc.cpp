#include "runtime/serve/ipc.hpp"

#include <arpa/inet.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <format>
#include <string>
#include <string_view>
#include <vector>

#include "runtime/serve/common.hpp"

namespace sr::engine::serve {
namespace {

constexpr std::size_t kU32Size = sizeof(std::uint32_t);

auto read_exact(int fd, void *buffer, std::size_t size) -> bool {
  auto *out = static_cast<unsigned char *>(buffer);
  std::size_t offset = 0;
  while (offset < size) {
    const auto n = ::read(fd, out + offset, size - offset);
    if (n == 0) {
      return false;
    }
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    offset += static_cast<std::size_t>(n);
  }
  return true;
}

auto write_all(int fd, const void *buffer, std::size_t size) -> bool {
  const auto *data = static_cast<const unsigned char *>(buffer);
  std::size_t offset = 0;
  while (offset < size) {
    const auto n = ::write(fd, data + offset, size - offset);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    offset += static_cast<std::size_t>(n);
  }
  return true;
}

auto read_u32_be(int fd, std::uint32_t &value) -> bool {
  std::uint32_t raw = 0;
  if (!read_exact(fd, &raw, kU32Size)) {
    return false;
  }
  value = ntohl(raw);
  return true;
}

auto write_u32_be(int fd, std::uint32_t value) -> bool {
  const auto raw = htonl(value);
  return write_all(fd, &raw, kU32Size);
}

auto byte_buffer_to_string(const grpc::ByteBuffer &buffer) -> std::string {
  std::vector<grpc::Slice> slices;
  buffer.Dump(&slices);
  std::size_t total = 0;
  for (const auto &slice : slices) {
    total += slice.size();
  }
  std::string data;
  data.reserve(total);
  for (const auto &slice : slices) {
    const auto *ptr = reinterpret_cast<const char *>(slice.begin());
    data.append(ptr, slice.size());
  }
  return data;
}

auto string_to_byte_buffer(std::string_view data) -> grpc::ByteBuffer {
  grpc::Slice slice(data.data(), data.size());
  return grpc::ByteBuffer(&slice, 1);
}

auto read_string(int fd, std::size_t max_bytes) -> Expected<std::string> {
  std::uint32_t length = 0;
  if (!read_u32_be(fd, length)) {
    return tl::unexpected(make_error("ipc read failed"));
  }
  if (max_bytes > 0 && length > max_bytes) {
    return tl::unexpected(make_error("ipc field too large"));
  }
  std::string data;
  data.resize(length);
  if (length > 0 && !read_exact(fd, data.data(), length)) {
    return tl::unexpected(make_error("ipc read failed"));
  }
  return data;
}

auto send_ipc_response(int fd, kernel::rpc::RpcResponse response) -> bool {
  const auto payload = byte_buffer_to_string(response.payload);
  const auto &status = response.status;

  if (!write_u32_be(fd, static_cast<std::uint32_t>(status.code))) {
    return false;
  }
  if (!write_u32_be(fd, static_cast<std::uint32_t>(status.message.size())) ||
      !write_all(fd, status.message.data(), status.message.size())) {
    return false;
  }
  if (!write_u32_be(fd, static_cast<std::uint32_t>(status.details.size())) ||
      !write_all(fd, status.details.data(), status.details.size())) {
    return false;
  }

  const auto &meta = response.trailing_metadata.entries;
  if (!write_u32_be(fd, static_cast<std::uint32_t>(meta.size()))) {
    return false;
  }
  for (const auto &entry : meta) {
    if (!write_u32_be(fd, static_cast<std::uint32_t>(entry.key.size())) ||
        !write_all(fd, entry.key.data(), entry.key.size())) {
      return false;
    }
    if (!write_u32_be(fd, static_cast<std::uint32_t>(entry.value.size())) ||
        !write_all(fd, entry.value.data(), entry.value.size())) {
      return false;
    }
  }

  if (!write_u32_be(fd, static_cast<std::uint32_t>(payload.size())) ||
      !write_all(fd, payload.data(), payload.size())) {
    return false;
  }
  return true;
}

auto build_peer_string(int fd) -> std::string {
  sockaddr_un addr{};
  socklen_t len = sizeof(addr);
  if (::getpeername(fd, reinterpret_cast<sockaddr *>(&addr), &len) == 0 &&
      addr.sun_path[0] != '\0') {
    return std::string("unix:") + addr.sun_path;
  }
  return "unix:peer";
}

} // namespace

IpcResponder::IpcResponder(int fd) : fd_(fd) {}

IpcResponder::~IpcResponder() { close_fd(); }

auto IpcResponder::close_fd() -> void {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

auto IpcResponder::send(kernel::rpc::RpcResponse response) noexcept
    -> Expected<void> {
  std::lock_guard<std::mutex> lock(send_mutex_);
  if (fd_ < 0) {
    return tl::unexpected(make_error("ipc responder missing socket"));
  }
  if (sent_.exchange(true, std::memory_order_acq_rel)) {
    return tl::unexpected(make_error("ipc response already sent"));
  }
  if (!send_ipc_response(fd_, std::move(response))) {
    close_fd();
    return tl::unexpected(make_error("ipc response write failed"));
  }
  close_fd();
  return {};
}

IpcServer::IpcServer(RequestCallback callback, std::string path,
                     int io_threads, int backlog, std::size_t max_message_bytes,
                     bool remove_existing)
    : callback_(std::move(callback)), path_(std::move(path)),
      io_threads_(io_threads <= 0 ? 1 : io_threads), backlog_(backlog),
      max_message_bytes_(max_message_bytes),
      remove_existing_(remove_existing) {}

IpcServer::~IpcServer() { shutdown(std::chrono::milliseconds(0)); }

auto IpcServer::start() -> Expected<void> {
  if (running_.load(std::memory_order_acquire)) {
    return {};
  }
  if (path_.empty()) {
    return tl::unexpected(make_error("ipc socket path is empty"));
  }
  if (remove_existing_) {
    ::unlink(path_.c_str());
  }

  listen_fd_ = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (listen_fd_ < 0) {
    return tl::unexpected(make_error("ipc socket create failed"));
  }

  sockaddr_un addr{};
  addr.sun_family = AF_UNIX;
  if (path_.size() >= sizeof(addr.sun_path)) {
    ::close(listen_fd_);
    listen_fd_ = -1;
    return tl::unexpected(make_error("ipc socket path too long"));
  }
  std::snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", path_.c_str());
  if (::bind(listen_fd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) !=
      0) {
    ::close(listen_fd_);
    listen_fd_ = -1;
    return tl::unexpected(make_error("ipc socket bind failed"));
  }
  if (::listen(listen_fd_, backlog_) != 0) {
    ::close(listen_fd_);
    listen_fd_ = -1;
    return tl::unexpected(make_error("ipc socket listen failed"));
  }

  running_.store(true, std::memory_order_release);
  stopping_.store(false, std::memory_order_release);
  workers_.reserve(static_cast<std::size_t>(io_threads_));
  for (int i = 0; i < io_threads_; ++i) {
    workers_.emplace_back([this] { worker_loop(); });
  }
  accept_thread_ = std::thread([this] { accept_loop(); });
  return {};
}

auto IpcServer::shutdown(std::chrono::milliseconds timeout) -> void {
  (void)timeout;
  if (!running_.exchange(false, std::memory_order_acq_rel)) {
    return;
  }
  stopping_.store(true, std::memory_order_release);
  if (listen_fd_ >= 0) {
    ::shutdown(listen_fd_, SHUT_RDWR);
    ::close(listen_fd_);
    listen_fd_ = -1;
  }
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    queue_cv_.notify_all();
  }
  if (accept_thread_.joinable()) {
    accept_thread_.join();
  }
  for (auto &worker : workers_) {
    if (worker.joinable()) {
      worker.join();
    }
  }
  workers_.clear();
  if (remove_existing_) {
    ::unlink(path_.c_str());
  }
}

auto IpcServer::accept_loop() -> void {
  while (!stopping_.load(std::memory_order_acquire)) {
    const int fd = ::accept(listen_fd_, nullptr, nullptr);
    if (fd < 0) {
      if (errno == EINTR) {
        continue;
      }
      break;
    }
    enqueue_socket(fd);
  }
}

auto IpcServer::enqueue_socket(int fd) -> void {
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    socket_queue_.push_back(fd);
  }
  queue_cv_.notify_one();
}

auto IpcServer::worker_loop() -> void {
  while (true) {
    int fd = -1;
    {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      queue_cv_.wait(lock, [this] {
        return stopping_.load(std::memory_order_acquire) ||
               !socket_queue_.empty();
      });
      if (socket_queue_.empty()) {
        if (stopping_.load(std::memory_order_acquire)) {
          return;
        }
        continue;
      }
      fd = socket_queue_.front();
      socket_queue_.pop_front();
    }
    if (fd >= 0) {
      handle_connection(fd);
    }
  }
}

auto IpcServer::handle_connection(int fd) -> void {
  auto responder = std::make_shared<IpcResponder>(fd);
  auto method_result = read_string(fd, max_message_bytes_);
  if (!method_result) {
    ignore_send(responder->send(make_error_response(
        grpc::StatusCode::INVALID_ARGUMENT, method_result.error().message)));
    return;
  }

  std::uint32_t meta_count = 0;
  if (!read_u32_be(fd, meta_count)) {
    ignore_send(responder->send(make_error_response(
        grpc::StatusCode::INVALID_ARGUMENT, "ipc metadata read failed")));
    return;
  }
  kernel::rpc::RpcMetadata metadata;
  metadata.entries.reserve(meta_count);
  for (std::uint32_t i = 0; i < meta_count; ++i) {
    auto key_result = read_string(fd, max_message_bytes_);
    if (!key_result) {
      ignore_send(responder->send(make_error_response(
          grpc::StatusCode::INVALID_ARGUMENT, key_result.error().message)));
      return;
    }
    auto value_result = read_string(fd, max_message_bytes_);
    if (!value_result) {
      ignore_send(responder->send(make_error_response(
          grpc::StatusCode::INVALID_ARGUMENT, value_result.error().message)));
      return;
    }
    metadata.entries.push_back(
        {std::move(*key_result), std::move(*value_result)});
  }

  auto payload_result = read_string(fd, max_message_bytes_);
  if (!payload_result) {
    ignore_send(responder->send(make_error_response(
        grpc::StatusCode::INVALID_ARGUMENT, payload_result.error().message)));
    return;
  }

  IpcEnvelope env;
  env.method = std::move(*method_result);
  env.metadata = std::move(metadata);
  env.payload = string_to_byte_buffer(*payload_result);
  env.responder = std::move(responder);
  env.peer = build_peer_string(fd);

  if (callback_) {
    callback_(std::move(env));
  }
}

} // namespace sr::engine::serve
