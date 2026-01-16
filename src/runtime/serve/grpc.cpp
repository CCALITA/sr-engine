#include "runtime/serve/grpc.hpp"

#include <condition_variable>
#include <format>
#include <mutex>
#include <thread>

namespace sr::engine::serve {
namespace {

auto metadata_from_context(const grpc::ServerContext &ctx)
    -> kernel::rpc::RpcMetadata {
  kernel::rpc::RpcMetadata metadata;
  const auto &src = ctx.client_metadata();
  metadata.entries.reserve(src.size());
  for (const auto &entry : src) {
    const auto key = std::string(entry.first.data(), entry.first.size());
    const auto value = std::string(entry.second.data(), entry.second.size());
    metadata.entries.push_back({key, value});
  }
  return metadata;
}

auto to_grpc_status(const kernel::rpc::RpcStatus &status) -> grpc::Status {
  return grpc::Status(status.code, status.message, status.details);
}

} // namespace

class GrpcCall : public std::enable_shared_from_this<GrpcCall> {
public:
  enum class Op { Request, Read, Write, Done };

  struct Tag {
    GrpcCall *call = nullptr;
    Op op = Op::Request;
  };

  GrpcCall(GrpcServer *server, grpc::ServerCompletionQueue *cq)
      : server_(server), cq_(cq), stream_(&context_) {}

  auto start() -> void {
    self_ = shared_from_this();
    add_op();
    server_->service_.RequestCall(&context_, &stream_, cq_, cq_, &request_tag_);
  }

  auto proceed(Op op, bool ok) -> void {
    switch (op) {
    case Op::Request:
      handle_request(ok);
      return;
    case Op::Read:
      handle_read(ok);
      return;
    case Op::Write:
      handle_write(ok);
      return;
    case Op::Done:
      handle_done(ok);
      return;
    }
  }

  auto send_response(kernel::rpc::RpcResponse response) -> Expected<void> {
    std::unique_lock<std::mutex> lock(send_mutex_);
    if (done_.load(std::memory_order_acquire)) {
      return tl::unexpected(make_error("grpc call already finished"));
    }
    if (send_started_) {
      return tl::unexpected(make_error("grpc response already sent"));
    }
    send_started_ = true;
    response_payload_ = std::move(response.payload);
    response_status_ = to_grpc_status(response.status);
    for (const auto &entry : response.trailing_metadata.entries) {
      context_.AddTrailingMetadata(entry.key, entry.value);
    }
    add_op();
    stream_.WriteAndFinish(response_payload_, grpc::WriteOptions{},
                           response_status_, &write_tag_);
    return {};
  }

  auto attach_request_state(const std::shared_ptr<RequestState> &state)
      -> void {
    request_state_ = state;
  }

private:
  auto add_op() -> void { pending_ops_.fetch_add(1, std::memory_order_relaxed); }

  auto complete_op() -> void {
    if (pending_ops_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      self_.reset();
    }
  }

  auto handle_request(bool ok) -> void {
    if (!ok) {
      complete_op();
      return;
    }
    server_->start_call(cq_);
    metadata_ = metadata_from_context(context_);
    method_ = context_.method();

    add_op();
    stream_.Read(&request_, &read_tag_);
    add_op();
    context_.AsyncNotifyWhenDone(&done_tag_);
    complete_op();
  }

  auto handle_read(bool ok) -> void {
    if (!ok) {
      complete_op();
      return;
    }
    auto responder = std::make_shared<GrpcResponder>(shared_from_this());
    GrpcEnvelope env;
    env.context = &context_;
    env.method = method_;
    env.payload = request_;
    env.metadata = metadata_;
    env.responder = to_rpc_responder(responder);
    if (server_->callback_) {
      server_->callback_(std::move(env));
    }
    complete_op();
  }

  auto handle_write(bool ok) -> void {
    (void)ok;
    complete_op();
  }

  auto handle_done(bool ok) -> void {
    (void)ok;
    done_.store(true, std::memory_order_release);
    if (context_.IsCancelled()) {
      if (auto state = request_state_.lock()) {
        state->ctx.cancel();
      }
    }
    complete_op();
  }

  GrpcServer *server_ = nullptr;
  grpc::ServerCompletionQueue *cq_ = nullptr;
  grpc::GenericServerContext context_;
  grpc::GenericServerAsyncReaderWriter stream_;
  grpc::ByteBuffer request_;
  grpc::ByteBuffer response_payload_;
  grpc::Status response_status_;
  kernel::rpc::RpcMetadata metadata_;
  std::string method_;
  std::weak_ptr<RequestState> request_state_;

  std::mutex send_mutex_;
  bool send_started_ = false;

  std::atomic<bool> done_{false};
  std::atomic<int> pending_ops_{0};
  std::shared_ptr<GrpcCall> self_;

  Tag request_tag_{this, Op::Request};
  Tag read_tag_{this, Op::Read};
  Tag write_tag_{this, Op::Write};
  Tag done_tag_{this, Op::Done};
};

GrpcResponder::GrpcResponder(std::shared_ptr<GrpcCall> call)
    : call_(std::move(call)) {}

auto GrpcResponder::send(kernel::rpc::RpcResponse response) noexcept
    -> Expected<void> {
  if (!call_) {
    return tl::unexpected(make_error("grpc responder missing call state"));
  }
  if (sent_.exchange(true, std::memory_order_acq_rel)) {
    return tl::unexpected(make_error("grpc response already sent"));
  }
  return call_->send_response(std::move(response));
}

auto GrpcResponder::attach_request_state(
    const std::shared_ptr<RequestState> &state) -> void {
  if (call_) {
    call_->attach_request_state(state);
  }
}

GrpcServer::GrpcServer(RequestCallback callback, std::string address,
                       int io_threads)
    : callback_(std::move(callback)), address_(std::move(address)),
      io_threads_(io_threads <= 0 ? 1 : io_threads) {}

GrpcServer::~GrpcServer() { shutdown(std::chrono::milliseconds(0)); }

auto GrpcServer::start() -> Expected<void> {
  if (running_.load(std::memory_order_acquire)) {
    return {};
  }

  grpc::ServerBuilder builder;
  builder.RegisterAsyncGenericService(&service_);
  builder.AddListeningPort(address_, grpc::InsecureServerCredentials(), &port_);
  cqs_.reserve(static_cast<std::size_t>(io_threads_));
  for (int i = 0; i < io_threads_; ++i) {
    cqs_.push_back(builder.AddCompletionQueue());
  }
  server_ = builder.BuildAndStart();
  if (!server_) {
    return tl::unexpected(make_error("grpc server failed to start"));
  }
  running_.store(true, std::memory_order_release);
  for (const auto &cq : cqs_) {
    start_call(cq.get());
  }
  workers_.reserve(cqs_.size());
  for (const auto &cq : cqs_) {
    workers_.emplace_back([this, cq_ptr = cq.get()] { worker_loop(cq_ptr); });
  }
  return {};
}

auto GrpcServer::shutdown(std::chrono::milliseconds timeout) -> void {
  if (!running_.exchange(false, std::memory_order_acq_rel)) {
    return;
  }
  if (server_) {
    if (timeout.count() > 0) {
      const auto deadline =
          std::chrono::system_clock::now() + timeout;
      server_->Shutdown(deadline);
    } else {
      server_->Shutdown();
    }
  }
  for (const auto &cq : cqs_) {
    cq->Shutdown();
  }
  for (auto &worker : workers_) {
    if (worker.joinable()) {
      worker.join();
    }
  }
  workers_.clear();
  cqs_.clear();
  server_.reset();
}

auto GrpcServer::start_call(grpc::ServerCompletionQueue *cq) -> void {
  auto call = std::make_shared<GrpcCall>(this, cq);
  call->start();
}

auto GrpcServer::worker_loop(grpc::ServerCompletionQueue *cq) -> void {
  void *tag = nullptr;
  bool ok = false;
  while (cq->Next(&tag, &ok)) {
    auto *op = static_cast<GrpcCall::Tag *>(tag);
    if (op && op->call) {
      op->call->proceed(op->op, ok);
    }
  }
}

} // namespace sr::engine::serve
