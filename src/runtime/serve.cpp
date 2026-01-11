#include "runtime/serve.hpp"

#include <atomic>
#include <condition_variable>
#include <deque>
#include <format>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <semaphore>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <exec/async_scope.hpp>
#include <exec/static_thread_pool.hpp>
#include <stdexec/execution.hpp>

#include <grpcpp/generic/async_generic_service.h>
#include <grpcpp/grpcpp.h>

#ifdef SR_ENGINE_WITH_ARROW_FLIGHT
#include <arrow/api.h>
#include <arrow/flight/api.h>
#endif

#include "engine/error.hpp"
#include "engine/types.hpp"
#include "kernel/rpc_kernels.hpp"
#ifdef SR_ENGINE_WITH_ARROW_FLIGHT
#include "kernel/flight_kernels.hpp"
#endif
#include "runtime/runtime.hpp"

namespace sr::engine {
namespace {

using sr::kernel::rpc::RpcMetadata;
using sr::kernel::rpc::RpcResponder;
using sr::kernel::rpc::RpcResponse;
using sr::kernel::rpc::RpcServerCall;

struct RequestState {
  RequestContext ctx;
};

/// Thread-safe bounded queue for incoming requests.
template <typename Envelope> class RequestQueue {
public:
  explicit RequestQueue(std::size_t capacity) : capacity_(capacity) {}

  auto push(Envelope &&env) -> bool {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_ || (capacity_ > 0 && queue_.size() >= capacity_)) {
      return false;
    }
    queue_.push_back(std::move(env));
    wakeup_.notify_one();
    return true;
  }

  auto pop(Envelope &out) -> bool {
    std::unique_lock<std::mutex> lock(mutex_);
    wakeup_.wait(lock, [this] { return closed_ || !queue_.empty(); });
    if (queue_.empty()) {
      return false;
    }
    out = std::move(queue_.front());
    queue_.pop_front();
    return true;
  }

  auto close() -> void {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      closed_ = true;
    }
    wakeup_.notify_all();
  }

  auto size() const -> std::size_t {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.size();
  }

private:
  std::size_t capacity_ = 0;
  mutable std::mutex mutex_;
  std::condition_variable wakeup_;
  std::deque<Envelope> queue_;
  bool closed_ = false;
};

constexpr std::string_view kRpcCallKey = "rpc.call";
constexpr std::string_view kRpcMethodKey = "rpc.method";
constexpr std::string_view kRpcPayloadKey = "rpc.payload";
constexpr std::string_view kRpcMetadataKey = "rpc.metadata";
constexpr std::string_view kRpcPeerKey = "rpc.peer";
constexpr std::string_view kRpcDeadlineKey = "rpc.deadline_ms";

/// Flags describing which rpc.* env entries are required by a plan.
struct RpcEnvBindings {
  bool call = false;
  bool method = false;
  bool payload = false;
  bool metadata = false;
  bool peer = false;
  bool deadline_ms = false;
  std::size_t total = 0;
};

/// Extract supported rpc.* env requirements from a compiled plan.
auto analyze_rpc_env(const ExecPlan &plan) -> Expected<RpcEnvBindings> {
  RpcEnvBindings bindings;
  bindings.total = plan.env_requirements.size();
  for (const auto &req : plan.env_requirements) {
    if (req.key == kRpcCallKey) {
      bindings.call = true;
      continue;
    }
    if (req.key == kRpcMethodKey) {
      bindings.method = true;
      continue;
    }
    if (req.key == kRpcPayloadKey) {
      bindings.payload = true;
      continue;
    }
    if (req.key == kRpcMetadataKey) {
      bindings.metadata = true;
      continue;
    }
    if (req.key == kRpcPeerKey) {
      bindings.peer = true;
      continue;
    }
    if (req.key == kRpcDeadlineKey) {
      bindings.deadline_ms = true;
      continue;
    }
    return tl::unexpected(
        make_error(std::format("unsupported serve env key: {}", req.key)));
  }
  return bindings;
}

auto register_serve_types() -> void {
  sr::engine::register_type<int64_t>("int64");
  sr::engine::register_type<std::string>("string");
  sr::kernel::register_rpc_types();
#ifdef SR_ENGINE_WITH_ARROW_FLIGHT
  sr::kernel::register_flight_types();
#endif
}

auto to_steady_deadline(
    std::chrono::system_clock::time_point deadline,
    const std::optional<std::chrono::milliseconds> &fallback)
    -> std::chrono::steady_clock::time_point {
  if (deadline == std::chrono::system_clock::time_point::max()) {
    if (!fallback) {
      return std::chrono::steady_clock::time_point::max();
    }
    return std::chrono::steady_clock::now() + *fallback;
  }
  const auto now_sys = std::chrono::system_clock::now();
  const auto now_steady = std::chrono::steady_clock::now();
  return now_steady + (deadline - now_sys);
}

auto deadline_remaining_ms(std::chrono::steady_clock::time_point deadline)
    -> int64_t {
  if (deadline == std::chrono::steady_clock::time_point::max()) {
    return -1;
  }
  const auto now = std::chrono::steady_clock::now();
  if (deadline <= now) {
    return 0;
  }
  const auto remaining = deadline - now;
  return std::chrono::duration_cast<std::chrono::milliseconds>(remaining)
      .count();
}

auto metadata_from_context(const grpc::GenericServerContext &ctx)
    -> RpcMetadata {
  RpcMetadata metadata;
  const auto &client_metadata = ctx.client_metadata();
  metadata.entries.reserve(client_metadata.size());
  for (const auto &entry : client_metadata) {
    const auto key = entry.first;
    const auto value = entry.second;
    metadata.entries.push_back({std::string(key.data(), key.size()),
                                std::string(value.data(), value.size())});
  }
  return metadata;
}

auto apply_trailing_metadata(grpc::GenericServerContext &ctx,
                             const RpcMetadata &metadata) -> void {
  for (const auto &entry : metadata.entries) {
    ctx.AddTrailingMetadata(entry.key, entry.value);
  }
}

auto make_error_response(grpc::StatusCode code, std::string message)
    -> RpcResponse {
  RpcResponse response;
  response.status.code = code;
  response.status.message = std::move(message);
  return response;
}

auto ignore_send(Expected<void> result) -> void { (void)result; }

auto status_from_context(const RequestContext &ctx, grpc::StatusCode fallback)
    -> grpc::StatusCode {
  if (ctx.is_cancelled()) {
    return grpc::StatusCode::CANCELLED;
  }
  if (ctx.deadline_exceeded()) {
    return grpc::StatusCode::DEADLINE_EXCEEDED;
  }
  return fallback;
}

class GrpcCall;

class GrpcResponder final : public RpcResponder {
public:
  explicit GrpcResponder(GrpcCall *call) : call_(call) {}

  auto send(RpcResponse response) noexcept -> Expected<void> override;

  auto sent() const -> bool;

  auto attach_request_state(std::weak_ptr<RequestState> state) -> void;

private:
  GrpcCall *call_ = nullptr;
};

struct GrpcEnvelope {
  grpc::ByteBuffer payload;
  const grpc::GenericServerContext *context = nullptr;
  std::shared_ptr<GrpcResponder> responder;
};

/// Populate RequestContext env with only the rpc.* values required by the plan.
auto populate_grpc_env(RequestContext &ctx, GrpcEnvelope &env,
                       const RpcEnvBindings &bindings) -> Expected<void> {
  if (bindings.total > 0) {
    ctx.env.reserve(bindings.total);
  }

  const auto &context = *env.context;
  std::optional<std::string> method;
  if (bindings.call || bindings.method) {
    method.emplace(context.method());
  }

  std::optional<RpcMetadata> metadata;
  if (bindings.call || bindings.metadata) {
    metadata.emplace(metadata_from_context(context));
  }

  std::optional<grpc::ByteBuffer> payload_copy;
  if (bindings.call && bindings.payload) {
    payload_copy.emplace(env.payload);
  }

  if (bindings.call) {
    RpcServerCall call;
    if (method) {
      call.method = bindings.method ? *method : std::move(*method);
    }
    call.request = std::move(env.payload);
    if (metadata) {
      call.metadata = bindings.metadata ? *metadata : std::move(*metadata);
    }
    call.responder = env.responder;
    ctx.set_env(std::string(kRpcCallKey), std::move(call));
  }

  if (bindings.method && method) {
    ctx.set_env(std::string(kRpcMethodKey),
                bindings.call ? *method : std::move(*method));
  }

  if (bindings.payload) {
    if (bindings.call) {
      ctx.set_env(std::string(kRpcPayloadKey), std::move(*payload_copy));
    } else {
      ctx.set_env(std::string(kRpcPayloadKey), std::move(env.payload));
    }
  }

  if (bindings.metadata && metadata) {
    ctx.set_env(std::string(kRpcMetadataKey),
                bindings.call ? *metadata : std::move(*metadata));
  }

  if (bindings.peer) {
    ctx.set_env(std::string(kRpcPeerKey), context.peer());
  }

  if (bindings.deadline_ms) {
    ctx.set_env(std::string(kRpcDeadlineKey),
                deadline_remaining_ms(ctx.deadline));
  }

  return {};
}

struct GrpcTag {
  enum class Kind { Call, Done };
  GrpcCall *call = nullptr;
  Kind kind = Kind::Call;
};

class GrpcServer;

/// Per-RPC state machine that owns the async read/write lifecycle.
class GrpcCall {
public:
  GrpcCall(GrpcServer &server, grpc::AsyncGenericService &service,
           grpc::ServerCompletionQueue &cq)
      : server_(server), service_(service), cq_(cq), stream_(&ctx_),
        call_tag_{this, GrpcTag::Kind::Call},
        done_tag_{this, GrpcTag::Kind::Done} {}

  auto start() -> void;
  auto proceed(bool ok, GrpcTag::Kind kind) -> void;

  auto send_response(RpcResponse response) -> Expected<void>;

  auto responded() const -> bool { return responded_.load(); }

  auto attach_request_state(std::weak_ptr<RequestState> state) -> void;

private:
  enum class State { Create, Request, Read, Process, Finish };

  auto handle_done() -> void;
  auto build_envelope() -> GrpcEnvelope;

  GrpcServer &server_;
  grpc::AsyncGenericService &service_;
  grpc::ServerCompletionQueue &cq_;
  grpc::GenericServerContext ctx_;
  grpc::GenericServerAsyncReaderWriter stream_;
  grpc::ByteBuffer request_;
  grpc::ByteBuffer response_payload_;
  grpc::Status response_status_;
  GrpcTag call_tag_;
  GrpcTag done_tag_;
  std::atomic<State> state_{State::Create};
  std::atomic<bool> responded_{false};
  std::atomic<bool> cancelled_{false};
  std::mutex state_mutex_;
  std::weak_ptr<RequestState> request_state_;
};

/// Async generic gRPC server that emits unary request envelopes.
class GrpcServer {
public:
  using RequestCallback = std::function<void(GrpcEnvelope &&)>;

  explicit GrpcServer(RequestCallback on_request, std::string address,
                      int io_threads)
      : on_request_(std::move(on_request)), address_(std::move(address)),
        io_threads_(io_threads) {}

  auto start() -> Expected<void> {
    if (running_.load()) {
      return tl::unexpected(make_error("grpc server already started"));
    }
    grpc::ServerBuilder builder;
    builder.RegisterAsyncGenericService(&service_);
    builder.AddListeningPort(address_, grpc::InsecureServerCredentials(),
                             &port_);
    const int thread_count = io_threads_ > 0 ? io_threads_ : 1;
    cqs_.reserve(static_cast<std::size_t>(thread_count));
    for (int i = 0; i < thread_count; ++i) {
      cqs_.push_back(builder.AddCompletionQueue());
    }
    server_ = builder.BuildAndStart();
    if (!server_) {
      return tl::unexpected(make_error("failed to start grpc server"));
    }
    workers_.reserve(cqs_.size());
    for (auto &cq : cqs_) {
      auto *call = new GrpcCall(*this, service_, *cq);
      call->start();
      workers_.emplace_back([this, cq_ptr = cq.get()] { run_cq(*cq_ptr); });
    }
    running_.store(true, std::memory_order_release);
    return {};
  }

  auto shutdown(std::chrono::milliseconds timeout) -> void {
    if (!server_) {
      return;
    }
    const auto deadline = std::chrono::system_clock::now() + timeout;
    server_->Shutdown(deadline);
    for (auto &cq : cqs_) {
      cq->Shutdown();
    }
    for (auto &worker : workers_) {
      if (worker.joinable()) {
        worker.join();
      }
    }
    server_.reset();
    running_.store(false, std::memory_order_release);
  }

  auto port() const -> int { return port_; }

  auto on_request(GrpcEnvelope &&env) -> void { on_request_(std::move(env)); }

private:
  auto run_cq(grpc::ServerCompletionQueue &cq) -> void {
    void *tag = nullptr;
    bool ok = false;
    while (cq.Next(&tag, &ok)) {
      auto *event = static_cast<GrpcTag *>(tag);
      event->call->proceed(ok, event->kind);
    }
  }

  RequestCallback on_request_;
  std::string address_;
  int io_threads_ = 1;
  grpc::AsyncGenericService service_;
  std::unique_ptr<grpc::Server> server_;
  std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> cqs_;
  std::vector<std::thread> workers_;
  std::atomic<bool> running_{false};
  int port_ = 0;
};

auto GrpcResponder::send(RpcResponse response) noexcept -> Expected<void> {
  if (!call_) {
    return tl::unexpected(make_error("grpc responder missing call"));
  }
  return call_->send_response(std::move(response));
}

auto GrpcResponder::sent() const -> bool { return call_ && call_->responded(); }

auto GrpcResponder::attach_request_state(std::weak_ptr<RequestState> state)
    -> void {
  if (call_) {
    call_->attach_request_state(std::move(state));
  }
}

auto GrpcCall::start() -> void {
  state_.store(State::Request, std::memory_order_release);
  service_.RequestCall(&ctx_, &stream_, &cq_, &cq_, &call_tag_);
}

auto GrpcCall::proceed(bool ok, GrpcTag::Kind kind) -> void {
  if (kind == GrpcTag::Kind::Done) {
    handle_done();
    return;
  }

  const auto state = state_.load(std::memory_order_acquire);
  if (state == State::Request) {
    if (!ok) {
      delete this;
      return;
    }
    auto *next = new GrpcCall(server_, service_, cq_);
    next->start();
    state_.store(State::Read, std::memory_order_release);
    ctx_.AsyncNotifyWhenDone(&done_tag_);
    stream_.Read(&request_, &call_tag_);
    return;
  }
  if (state == State::Read) {
    if (!ok) {
      delete this;
      return;
    }
    state_.store(State::Process, std::memory_order_release);
    server_.on_request(build_envelope());
    return;
  }
  if (state == State::Finish) {
    delete this;
    return;
  }
}

auto GrpcCall::send_response(RpcResponse response) -> Expected<void> {
  bool expected = false;
  if (!responded_.compare_exchange_strong(expected, true,
                                          std::memory_order_acq_rel)) {
    return tl::unexpected(make_error("grpc response already sent"));
  }
  apply_trailing_metadata(ctx_, response.trailing_metadata);
  response_payload_ = std::move(response.payload);
  response_status_ =
      grpc::Status(static_cast<grpc::StatusCode>(response.status.code),
                   response.status.message, response.status.details);
  state_.store(State::Finish, std::memory_order_release);
  stream_.WriteAndFinish(response_payload_, grpc::WriteOptions{},
                         response_status_, &call_tag_);
  return {};
}

auto GrpcCall::attach_request_state(std::weak_ptr<RequestState> state) -> void {
  std::lock_guard<std::mutex> lock(state_mutex_);
  request_state_ = std::move(state);
  if (cancelled_.load(std::memory_order_acquire)) {
    if (auto locked = request_state_.lock()) {
      locked->ctx.cancel();
    }
  }
}

auto GrpcCall::handle_done() -> void {
  const bool cancelled = ctx_.IsCancelled();
  cancelled_.store(cancelled, std::memory_order_release);
  if (!cancelled) {
    return;
  }
  std::shared_ptr<RequestState> state;
  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    state = request_state_.lock();
  }
  if (state) {
    state->ctx.cancel();
  }
}

auto GrpcCall::build_envelope() -> GrpcEnvelope {
  GrpcEnvelope env;
  env.payload = std::move(request_);
  env.context = &ctx_;
  env.responder = std::make_shared<GrpcResponder>(this);
  return env;
}

#ifdef SR_ENGINE_WITH_ARROW_FLIGHT
using sr::kernel::flight::FlightCallKind;
using sr::kernel::flight::FlightResponder;
using sr::kernel::flight::FlightServerCall;

constexpr std::string_view kFlightCallKey = "flight.call";
constexpr std::string_view kFlightKindKey = "flight.kind";
constexpr std::string_view kFlightActionKey = "flight.action";
constexpr std::string_view kFlightTicketKey = "flight.ticket";
constexpr std::string_view kFlightDescriptorKey = "flight.descriptor";
constexpr std::string_view kFlightReaderKey = "flight.reader";
constexpr std::string_view kFlightWriterKey = "flight.writer";
constexpr std::string_view kFlightMetadataWriterKey = "flight.metadata_writer";
constexpr std::string_view kFlightPeerKey = "flight.peer";
constexpr std::string_view kFlightDeadlineKey = "flight.deadline_ms";

struct FlightResponseSlot;

using FlightResponse = std::variant<std::vector<arrow::flight::Result>,
                                    std::shared_ptr<arrow::RecordBatchReader>>;

/// Shared response slot for Flight calls.
struct FlightResponseSlot {
  std::mutex mutex;
  std::condition_variable wakeup;
  bool ready = false;
  arrow::Status status = arrow::Status::OK();
  std::optional<FlightResponse> response;

  auto set_response(FlightResponse value) -> void {
    {
      std::lock_guard<std::mutex> lock(mutex);
      if (ready) {
        return;
      }
      response = std::move(value);
      status = arrow::Status::OK();
      ready = true;
    }
    wakeup.notify_one();
  }

  auto set_status(arrow::Status value) -> void {
    {
      std::lock_guard<std::mutex> lock(mutex);
      if (ready) {
        return;
      }
      status = std::move(value);
      ready = true;
    }
    wakeup.notify_one();
  }

  auto ensure_ok() -> void {
    {
      std::lock_guard<std::mutex> lock(mutex);
      if (ready) {
        return;
      }
      status = arrow::Status::OK();
      ready = true;
    }
    wakeup.notify_one();
  }

  auto wait() -> std::pair<arrow::Status, std::optional<FlightResponse>> {
    std::unique_lock<std::mutex> lock(mutex);
    wakeup.wait(lock, [this] { return ready; });
    return {status, response};
  }
};

class FlightResponderImpl final : public FlightResponder {
public:
  FlightResponderImpl(std::shared_ptr<FlightResponseSlot> slot,
                      FlightCallKind kind)
      : slot_(std::move(slot)), kind_(kind) {}

  auto send_action_results(std::vector<arrow::flight::Result> results) noexcept
      -> Expected<void> override {
    if (kind_ != FlightCallKind::DoAction) {
      return tl::unexpected(make_error("flight responder expects action"));
    }
    if (!slot_) {
      return tl::unexpected(make_error("flight responder missing slot"));
    }
    bool expected = false;
    if (!sent_.compare_exchange_strong(expected, true,
                                       std::memory_order_acq_rel)) {
      return tl::unexpected(make_error("flight response already sent"));
    }
    slot_->set_response(std::move(results));
    return {};
  }

  auto
  send_record_batches(std::shared_ptr<arrow::RecordBatchReader> reader) noexcept
      -> Expected<void> override {
    if (kind_ != FlightCallKind::DoGet) {
      return tl::unexpected(make_error("flight responder expects do_get"));
    }
    if (!slot_) {
      return tl::unexpected(make_error("flight responder missing slot"));
    }
    bool expected = false;
    if (!sent_.compare_exchange_strong(expected, true,
                                       std::memory_order_acq_rel)) {
      return tl::unexpected(make_error("flight response already sent"));
    }
    slot_->set_response(std::move(reader));
    return {};
  }

  auto sent() const -> bool { return sent_.load(std::memory_order_acquire); }

private:
  std::shared_ptr<FlightResponseSlot> slot_;
  FlightCallKind kind_;
  std::atomic<bool> sent_{false};
};

struct FlightEnvelope {
  FlightCallKind kind = FlightCallKind::DoAction;
  const arrow::flight::ServerCallContext *context = nullptr;
  std::optional<arrow::flight::Action> action;
  std::optional<arrow::flight::Ticket> ticket;
  std::optional<arrow::flight::FlightDescriptor> descriptor;
  std::shared_ptr<arrow::flight::FlightMessageReader> reader;
  std::shared_ptr<arrow::flight::FlightMessageWriter> writer;
  std::shared_ptr<arrow::flight::FlightMetadataWriter> metadata_writer;
  std::shared_ptr<FlightResponderImpl> responder;
  std::shared_ptr<FlightResponseSlot> response;
};

/// Flags describing which flight.* env entries are required by a plan.
struct FlightEnvBindings {
  bool call = false;
  bool kind = false;
  bool action = false;
  bool ticket = false;
  bool descriptor = false;
  bool reader = false;
  bool writer = false;
  bool metadata_writer = false;
  bool peer = false;
  bool deadline_ms = false;
  std::size_t total = 0;
};

/// Extract supported flight.* env requirements from a compiled plan.
auto analyze_flight_env(const ExecPlan &plan) -> Expected<FlightEnvBindings> {
  FlightEnvBindings bindings;
  bindings.total = plan.env_requirements.size();
  for (const auto &req : plan.env_requirements) {
    if (req.key == kFlightCallKey) {
      bindings.call = true;
      continue;
    }
    if (req.key == kFlightKindKey) {
      bindings.kind = true;
      continue;
    }
    if (req.key == kFlightActionKey) {
      bindings.action = true;
      continue;
    }
    if (req.key == kFlightTicketKey) {
      bindings.ticket = true;
      continue;
    }
    if (req.key == kFlightDescriptorKey) {
      bindings.descriptor = true;
      continue;
    }
    if (req.key == kFlightReaderKey) {
      bindings.reader = true;
      continue;
    }
    if (req.key == kFlightWriterKey) {
      bindings.writer = true;
      continue;
    }
    if (req.key == kFlightMetadataWriterKey) {
      bindings.metadata_writer = true;
      continue;
    }
    if (req.key == kFlightPeerKey) {
      bindings.peer = true;
      continue;
    }
    if (req.key == kFlightDeadlineKey) {
      bindings.deadline_ms = true;
      continue;
    }
    return tl::unexpected(
        make_error(std::format("unsupported serve env key: {}", req.key)));
  }
  return bindings;
}

auto flight_deadline(const arrow::flight::ServerCallContext &ctx,
                     const std::optional<std::chrono::milliseconds> &fallback)
    -> std::chrono::steady_clock::time_point {
  (void)ctx;
  if (fallback) {
    return std::chrono::steady_clock::now() + *fallback;
  }
  return std::chrono::steady_clock::time_point::max();
}

auto flight_peer(const arrow::flight::ServerCallContext &ctx) -> std::string {
  return std::string(ctx.peer());
}

auto flight_cancelled(const arrow::flight::ServerCallContext &ctx) -> bool {
  return ctx.is_cancelled();
}

auto flight_descriptor_from_reader(
    const std::shared_ptr<arrow::flight::FlightMessageReader> &reader)
    -> std::optional<arrow::flight::FlightDescriptor> {
  if (!reader) {
    return std::nullopt;
  }
  return reader->descriptor();
}

/// Populate RequestContext env with flight.* values required by the plan.
auto populate_flight_env(RequestContext &ctx, FlightEnvelope &env,
                         const FlightEnvBindings &bindings) -> Expected<void> {
  if (bindings.total > 0) {
    ctx.env.reserve(bindings.total);
  }

  if (!env.context) {
    return tl::unexpected(make_error("flight context missing"));
  }
  const auto &context = *env.context;

  if (bindings.call) {
    FlightServerCall call;
    call.kind = env.kind;
    call.action = env.action;
    call.ticket = env.ticket;
    call.descriptor = env.descriptor;
    call.reader = env.reader;
    call.writer = env.writer;
    call.metadata_writer = env.metadata_writer;
    call.responder = env.responder;
    ctx.set_env(std::string(kFlightCallKey), std::move(call));
  }

  if (bindings.kind) {
    ctx.set_env(std::string(kFlightKindKey), env.kind);
  }

  if (bindings.action) {
    if (!env.action) {
      return tl::unexpected(make_error("flight action missing"));
    }
    ctx.set_env(std::string(kFlightActionKey), *env.action);
  }

  if (bindings.ticket) {
    if (!env.ticket) {
      return tl::unexpected(make_error("flight ticket missing"));
    }
    ctx.set_env(std::string(kFlightTicketKey), *env.ticket);
  }

  if (bindings.descriptor) {
    if (!env.descriptor) {
      return tl::unexpected(make_error("flight descriptor missing"));
    }
    ctx.set_env(std::string(kFlightDescriptorKey), *env.descriptor);
  }

  if (bindings.reader) {
    if (!env.reader) {
      return tl::unexpected(make_error("flight reader missing"));
    }
    ctx.set_env(std::string(kFlightReaderKey), env.reader);
  }

  if (bindings.writer) {
    if (!env.writer) {
      return tl::unexpected(make_error("flight writer missing"));
    }
    ctx.set_env(std::string(kFlightWriterKey), env.writer);
  }

  if (bindings.metadata_writer) {
    if (!env.metadata_writer) {
      return tl::unexpected(make_error("flight metadata writer missing"));
    }
    ctx.set_env(std::string(kFlightMetadataWriterKey), env.metadata_writer);
  }

  if (bindings.peer) {
    ctx.set_env(std::string(kFlightPeerKey), flight_peer(context));
  }

  if (bindings.deadline_ms) {
    ctx.set_env(std::string(kFlightDeadlineKey),
                deadline_remaining_ms(ctx.deadline));
  }

  return {};
}

auto map_grpc_status_to_arrow(grpc::StatusCode code, std::string message)
    -> arrow::Status {
  switch (code) {
  case grpc::StatusCode::CANCELLED:
    return arrow::Status::Cancelled(std::move(message));
  case grpc::StatusCode::DEADLINE_EXCEEDED:
    return arrow::Status::Invalid(std::move(message));
  case grpc::StatusCode::NOT_FOUND:
    return arrow::Status::KeyError(std::move(message));
  case grpc::StatusCode::RESOURCE_EXHAUSTED:
    return arrow::Status::CapacityError(std::move(message));
  case grpc::StatusCode::FAILED_PRECONDITION:
    return arrow::Status::Invalid(std::move(message));
  case grpc::StatusCode::UNIMPLEMENTED:
    return arrow::Status::NotImplemented(std::move(message));
  case grpc::StatusCode::UNAVAILABLE:
    return arrow::Status::IOError(std::move(message));
  case grpc::StatusCode::ALREADY_EXISTS:
    return arrow::Status::AlreadyExists(std::move(message));
  default:
    return arrow::Status::UnknownError(std::move(message));
  }
}

auto make_flight_options(const arrow::flight::Location &location)
    -> arrow::Result<arrow::flight::FlightServerOptions> {
  return arrow::flight::FlightServerOptions(location);
}

class VectorResultStream final : public arrow::flight::ResultStream {
public:
  explicit VectorResultStream(std::vector<arrow::flight::Result> results)
      : results_(std::move(results)) {}

  auto Next() -> arrow::Result<std::unique_ptr<arrow::flight::Result>> override {
    if (index_ >= results_.size()) {
      return std::unique_ptr<arrow::flight::Result>{};
    }
    auto result =
        std::make_unique<arrow::flight::Result>(std::move(results_[index_]));
    ++index_;
    return result;
  }

private:
  std::vector<arrow::flight::Result> results_;
  std::size_t index_ = 0;
};

class FlightServer final : public arrow::flight::FlightServerBase {
public:
  using RequestCallback = std::function<void(FlightEnvelope &&)>;

  explicit FlightServer(RequestCallback on_request, std::string location,
                        int io_threads)
      : on_request_(std::move(on_request)), location_(std::move(location)),
        io_threads_(io_threads) {}

  auto start() -> Expected<void> {
    if (running_.load()) {
      return tl::unexpected(make_error("flight server already started"));
    }
    auto location = arrow::flight::Location::Parse(location_);
    if (!location.ok()) {
      return tl::unexpected(
          make_error(std::format("invalid flight location: {}", location_)));
    }
    auto options_result = make_flight_options(*location);
    if (!options_result.ok()) {
      return tl::unexpected(make_error(options_result.status().ToString()));
    }
    options_ = std::move(*options_result);
    auto status = Init(*options_);
    if (!status.ok()) {
      return tl::unexpected(make_error(status.ToString()));
    }
    worker_ = std::thread([this] { (void)Serve(); });
    running_.store(true, std::memory_order_release);
    return {};
  }

  auto shutdown(std::chrono::milliseconds) -> void {
    if (!running_.load(std::memory_order_acquire)) {
      return;
    }
    (void)Shutdown();
    if (worker_.joinable()) {
      worker_.join();
    }
    running_.store(false, std::memory_order_release);
  }

  auto port() const -> int {
    return static_cast<int>(FlightServerBase::port());
  }

  auto DoAction(const arrow::flight::ServerCallContext &context,
                const arrow::flight::Action &action,
                std::unique_ptr<arrow::flight::ResultStream> *result)
      -> arrow::Status override {
    auto slot = std::make_shared<FlightResponseSlot>();
    FlightEnvelope env;
    env.kind = FlightCallKind::DoAction;
    env.context = &context;
    env.action = action;
    env.response = slot;
    env.responder = std::make_shared<FlightResponderImpl>(slot, env.kind);
    on_request_(std::move(env));
    auto [status, response] = slot->wait();
    if (!status.ok()) {
      return status;
    }
    if (!response) {
      return arrow::Status::Invalid("flight response missing for action");
    }
    auto *results = std::get_if<std::vector<arrow::flight::Result>>(&*response);
    if (!results) {
      return arrow::Status::Invalid("unexpected flight action response");
    }
    *result = std::make_unique<VectorResultStream>(std::move(*results));
    return arrow::Status::OK();
  }

  auto DoGet(const arrow::flight::ServerCallContext &context,
             const arrow::flight::Ticket &ticket,
             std::unique_ptr<arrow::flight::FlightDataStream> *stream)
      -> arrow::Status override {
    auto slot = std::make_shared<FlightResponseSlot>();
    FlightEnvelope env;
    env.kind = FlightCallKind::DoGet;
    env.context = &context;
    env.ticket = ticket;
    env.response = slot;
    env.responder = std::make_shared<FlightResponderImpl>(slot, env.kind);
    on_request_(std::move(env));
    auto [status, response] = slot->wait();
    if (!status.ok()) {
      return status;
    }
    if (!response) {
      return arrow::Status::Invalid("flight response missing for do_get");
    }
    auto *reader =
        std::get_if<std::shared_ptr<arrow::RecordBatchReader>>(&*response);
    if (!reader || !*reader) {
      return arrow::Status::Invalid("unexpected flight do_get response");
    }
    *stream =
        std::make_unique<arrow::flight::RecordBatchStream>(std::move(*reader));
    return arrow::Status::OK();
  }

  auto DoPut(const arrow::flight::ServerCallContext &context,
             std::unique_ptr<arrow::flight::FlightMessageReader> reader,
             std::unique_ptr<arrow::flight::FlightMetadataWriter> writer)
      -> arrow::Status override {
    auto slot = std::make_shared<FlightResponseSlot>();
    FlightEnvelope env;
    env.kind = FlightCallKind::DoPut;
    env.context = &context;
    env.reader =
        std::shared_ptr<arrow::flight::FlightMessageReader>(std::move(reader));
    env.descriptor = flight_descriptor_from_reader(env.reader);
    env.metadata_writer =
        std::shared_ptr<arrow::flight::FlightMetadataWriter>(std::move(writer));
    env.response = slot;
    on_request_(std::move(env));
    return slot->wait().first;
  }

  auto DoExchange(const arrow::flight::ServerCallContext &context,
                  std::unique_ptr<arrow::flight::FlightMessageReader> reader,
                  std::unique_ptr<arrow::flight::FlightMessageWriter> writer)
      -> arrow::Status override {
    auto slot = std::make_shared<FlightResponseSlot>();
    FlightEnvelope env;
    env.kind = FlightCallKind::DoExchange;
    env.context = &context;
    env.reader =
        std::shared_ptr<arrow::flight::FlightMessageReader>(std::move(reader));
    env.descriptor = flight_descriptor_from_reader(env.reader);
    env.writer =
        std::shared_ptr<arrow::flight::FlightMessageWriter>(std::move(writer));
    env.response = slot;
    on_request_(std::move(env));
    return slot->wait().first;
  }

  auto ListActions(const arrow::flight::ServerCallContext &,
                   std::vector<arrow::flight::ActionType> *)
      -> arrow::Status override {
    return arrow::Status::NotImplemented("list_actions not wired");
  }

  auto GetFlightInfo(const arrow::flight::ServerCallContext &,
                     const arrow::flight::FlightDescriptor &,
                     std::unique_ptr<arrow::flight::FlightInfo> *)
      -> arrow::Status override {
    return arrow::Status::NotImplemented("get_flight_info not wired");
  }

  auto GetSchema(const arrow::flight::ServerCallContext &,
                 const arrow::flight::FlightDescriptor &,
                 std::unique_ptr<arrow::flight::SchemaResult> *)
      -> arrow::Status override {
    return arrow::Status::NotImplemented("get_schema not wired");
  }

private:
  RequestCallback on_request_;
  std::string location_;
  int io_threads_ = 1;
  std::optional<arrow::flight::FlightServerOptions> options_;
  std::thread worker_;
  std::atomic<bool> running_{false};
};
#endif

/// Base interface for typed serve endpoints.
class ServeEndpointBase {
public:
  virtual ~ServeEndpointBase() = default;
  virtual auto start() -> Expected<void> = 0;
  virtual auto shutdown() -> void = 0;
  virtual auto wait() -> void = 0;
  virtual auto stats() const -> ServeStats = 0;
  virtual auto port() const -> int = 0;
  virtual auto name() const -> std::string_view = 0;
  virtual auto transport() const -> std::string_view = 0;
  virtual auto running() const -> bool = 0;
};

template <typename Traits>
class ServeEndpoint final : public ServeEndpointBase {
public:
  using Envelope = typename Traits::Envelope;
  using EnvBindings = typename Traits::EnvBindings;
  using Transport = typename Traits::Transport;

  ServeEndpoint(Runtime &runtime_ref, ServeEndpointConfig config_ref)
      : runtime(&runtime_ref), config(std::move(config_ref)),
        request_pool(resolve_request_threads(config)),
        inflight_sem(resolve_inflight_capacity(config)),
        use_inflight(config.max_inflight > 0),
        transport_(Traits::make_transport(
            config, [this](Envelope &&env) { enqueue(std::move(env)); })) {}

  auto start() -> Expected<void> override {
    if (config.graph_name.empty()) {
      return tl::unexpected(make_error("serve config missing graph_name"));
    }
    if (auto ok = Traits::validate_transport(config); !ok) {
      return tl::unexpected(ok.error());
    }
    register_serve_types();
    if (config.graph_version) {
      pinned_snapshot =
          runtime->resolve(config.graph_name, *config.graph_version);
      if (!pinned_snapshot) {
        return tl::unexpected(
            make_error(std::format("graph version not found: {} v{}",
                                   config.graph_name, *config.graph_version)));
      }
      auto bindings = Traits::analyze_env(pinned_snapshot->plan);
      if (!bindings) {
        return tl::unexpected(bindings.error());
      }
      pinned_env = std::move(*bindings);
    }

    if (auto ok = transport_->start(); !ok) {
      return tl::unexpected(ok.error());
    }

    if (config.queue_capacity > 0) {
      queue = std::make_unique<RequestQueue<Envelope>>(config.queue_capacity);
      const int threads =
          config.dispatch_threads > 0 ? config.dispatch_threads : 1;
      dispatch_threads.reserve(static_cast<std::size_t>(threads));
      for (int i = 0; i < threads; ++i) {
        dispatch_threads.emplace_back([this] { dispatch_loop(); });
      }
    }

    running_.store(true, std::memory_order_release);
    return {};
  }

  auto shutdown() -> void override {
    if (stopping.exchange(true, std::memory_order_acq_rel)) {
      return;
    }
    transport_->shutdown(config.shutdown_timeout);
    if (queue) {
      queue->close();
    }
    for (auto &thread : dispatch_threads) {
      if (thread.joinable()) {
        thread.join();
      }
    }
    scope.request_stop();
    running_.store(false, std::memory_order_release);
  }

  auto wait() -> void override { stdexec::sync_wait(scope.on_empty()); }

  auto stats() const -> ServeStats override {
    ServeStats snapshot;
    snapshot.accepted = accepted.load(std::memory_order_relaxed);
    snapshot.rejected = rejected.load(std::memory_order_relaxed);
    snapshot.completed = completed.load(std::memory_order_relaxed);
    snapshot.failed = failed.load(std::memory_order_relaxed);
    snapshot.inflight = inflight.load(std::memory_order_relaxed);
    snapshot.queued = queued.load(std::memory_order_relaxed);
    return snapshot;
  }

  auto port() const -> int override {
    return transport_ ? transport_->port() : 0;
  }

  auto name() const -> std::string_view override { return config.name; }

  auto transport() const -> std::string_view override {
    return Traits::transport_name();
  }

  auto running() const -> bool override {
    return running_.load(std::memory_order_acquire);
  }

private:
  struct InflightPermit {
    ServeEndpoint *state = nullptr;
    bool acquired = false;

    InflightPermit() = default;
    InflightPermit(ServeEndpoint *state_in, bool acquired_in)
        : state(state_in), acquired(acquired_in) {}

    InflightPermit(InflightPermit &&other) noexcept
        : state(other.state), acquired(other.acquired) {
      other.acquired = false;
    }

    auto operator=(InflightPermit &&other) noexcept -> InflightPermit & {
      if (this == &other) {
        return *this;
      }
      release();
      state = other.state;
      acquired = other.acquired;
      other.acquired = false;
      return *this;
    }

    ~InflightPermit() { release(); }

    auto release() -> void {
      if (acquired && state) {
        state->release_inflight();
        acquired = false;
      }
    }
  };

  /// RAII helper that finalizes per-request stats on exit.
  struct RequestOutcome {
    ServeEndpoint *state = nullptr;
    bool failed = false;

    explicit RequestOutcome(ServeEndpoint &state_in) : state(&state_in) {}
    RequestOutcome(const RequestOutcome &) = delete;
    RequestOutcome &operator=(const RequestOutcome &) = delete;

    ~RequestOutcome() {
      if (!state) {
        return;
      }
      if (failed) {
        state->failed.fetch_add(1, std::memory_order_relaxed);
      }
      state->completed.fetch_add(1, std::memory_order_relaxed);
    }

    auto mark_failed() -> void { failed = true; }
  };

  auto enqueue(Envelope env) -> void {
    if (stopping.load(std::memory_order_acquire)) {
      Traits::reject(env, grpc::StatusCode::UNAVAILABLE,
                     "server shutting down");
      return;
    }
    if (queue) {
      if (!queue->push(std::move(env))) {
        Traits::reject(env, grpc::StatusCode::RESOURCE_EXHAUSTED,
                       "request queue full");
        return;
      }
      accepted.fetch_add(1, std::memory_order_relaxed);
      queued.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    auto permit = try_acquire_inflight();
    if (!permit) {
      Traits::reject(env, grpc::StatusCode::RESOURCE_EXHAUSTED,
                     "request capacity exceeded");
      return;
    }
    accepted.fetch_add(1, std::memory_order_relaxed);
    schedule(std::move(env), std::move(*permit));
  }

  auto dispatch_loop() -> void {
    Envelope env;
    while (queue && queue->pop(env)) {
      queued.fetch_sub(1, std::memory_order_relaxed);
      auto permit = acquire_inflight();
      schedule(std::move(env), std::move(permit));
    }
  }

  auto schedule(Envelope env, InflightPermit permit) -> void {
    auto scheduler = request_pool.get_scheduler();
    auto sender = stdexec::schedule(scheduler) |
                  stdexec::then([this, env = std::move(env),
                                 permit = std::move(permit)]() mutable {
                    handle_request(std::move(env), std::move(permit));
                  });
    scope.spawn(std::move(sender));
  }

  auto handle_request(Envelope env, [[maybe_unused]] InflightPermit permit)
      -> void {
    RequestOutcome outcome{*this};
    auto fail = [&](grpc::StatusCode code, std::string message) -> void {
      Traits::reject(env, code, std::move(message));
      outcome.mark_failed();
    };

    if (Traits::requires_response(env) && !Traits::responder_available(env)) {
      fail(grpc::StatusCode::INTERNAL, "request responder missing");
      return;
    }

    auto request_state = std::make_shared<RequestState>();
    Traits::attach_request_state(env, request_state);

    auto &ctx = request_state->ctx;
    ctx.trace.sink = config.trace_sink;
    ctx.trace.sampler = config.trace_sampler;
    ctx.trace.clock = config.trace_clock;
    ctx.trace.flags = config.trace_flags;
    ctx.trace.trace_id = request_id.fetch_add(1, std::memory_order_relaxed);
    ctx.trace.next_span.store(1, std::memory_order_relaxed);

    ctx.deadline = Traits::deadline(env, config.default_deadline);
    Traits::prime_cancellation(env, ctx);

    if (ctx.should_stop()) {
      fail(status_from_context(ctx, grpc::StatusCode::CANCELLED),
           "request cancelled");
      return;
    }

    std::shared_ptr<const PlanSnapshot> snapshot =
        pinned_snapshot ? pinned_snapshot : runtime->resolve(config.graph_name);
    if (!snapshot) {
      fail(grpc::StatusCode::NOT_FOUND,
           std::format("graph not found: {}", config.graph_name));
      return;
    }

    EnvBindings bindings;
    if (pinned_env) {
      bindings = *pinned_env;
    } else {
      auto bindings_result = Traits::analyze_env(snapshot->plan);
      if (!bindings_result) {
        fail(grpc::StatusCode::FAILED_PRECONDITION,
             bindings_result.error().message);
        return;
      }
      bindings = std::move(*bindings_result);
    }

    auto env_result = Traits::populate_env(ctx, env, bindings);
    if (!env_result) {
      fail(grpc::StatusCode::FAILED_PRECONDITION, env_result.error().message);
      return;
    }

    auto result = runtime->run(snapshot, ctx);
    if (!result) {
      fail(status_from_context(ctx, grpc::StatusCode::INTERNAL),
           result.error().message);
      return;
    }

    if (!Traits::requires_response(env)) {
      Traits::complete(env);
      return;
    }

    if (!Traits::response_sent(env)) {
      const auto status =
          status_from_context(ctx, grpc::StatusCode::FAILED_PRECONDITION);
      const auto message = ctx.should_stop() ? "request cancelled"
                                             : "response not sent by graph";
      fail(status, message);
      return;
    }
  }

  auto acquire_inflight() -> InflightPermit {
    if (use_inflight) {
      inflight_sem.acquire();
    }
    inflight.fetch_add(1, std::memory_order_relaxed);
    return {this, true};
  }

  auto try_acquire_inflight() -> std::optional<InflightPermit> {
    if (use_inflight && !inflight_sem.try_acquire()) {
      return std::nullopt;
    }
    inflight.fetch_add(1, std::memory_order_relaxed);
    return InflightPermit{this, true};
  }

  auto release_inflight() -> void {
    inflight.fetch_sub(1, std::memory_order_relaxed);
    if (use_inflight) {
      inflight_sem.release();
    }
  }

  static auto resolve_request_threads(const ServeEndpointConfig &config)
      -> int {
    if (config.request_threads > 0) {
      return config.request_threads;
    }
    const auto hw = std::thread::hardware_concurrency();
    return hw > 0 ? static_cast<int>(hw) : 1;
  }

  static auto resolve_inflight_capacity(const ServeEndpointConfig &config)
      -> int {
    if (config.max_inflight <= 0) {
      return 0;
    }
    return config.max_inflight;
  }

  Runtime *runtime = nullptr;
  ServeEndpointConfig config;
  exec::static_thread_pool request_pool;
  exec::async_scope scope;
  std::unique_ptr<RequestQueue<Envelope>> queue;
  std::vector<std::thread> dispatch_threads;
  std::unique_ptr<Transport> transport_;
  std::atomic<bool> running_{false};
  std::atomic<bool> stopping{false};
  std::shared_ptr<const PlanSnapshot> pinned_snapshot;
  std::optional<EnvBindings> pinned_env;
  std::atomic<std::uint64_t> request_id{1};
  std::atomic<std::uint64_t> accepted{0};
  std::atomic<std::uint64_t> rejected{0};
  std::atomic<std::uint64_t> completed{0};
  std::atomic<std::uint64_t> failed{0};
  std::atomic<std::uint64_t> inflight{0};
  std::atomic<std::uint64_t> queued{0};
  std::counting_semaphore<std::numeric_limits<int>::max()> inflight_sem;
  bool use_inflight = false;
};

struct GrpcTraits {
  using Envelope = GrpcEnvelope;
  using EnvBindings = RpcEnvBindings;
  using Transport = GrpcServer;

  static auto transport_name() -> std::string_view { return "grpc_unary"; }

  static auto validate_transport(const ServeEndpointConfig &config)
      -> Expected<void> {
    const auto &grpc_config = std::get<GrpcServeConfig>(config.transport);
    if (grpc_config.address.empty()) {
      return tl::unexpected(make_error("serve config missing address"));
    }
    return {};
  }

  static auto make_transport(const ServeEndpointConfig &config,
                             Transport::RequestCallback callback)
      -> std::unique_ptr<Transport> {
    const auto &grpc_config = std::get<GrpcServeConfig>(config.transport);
    return std::make_unique<Transport>(std::move(callback), grpc_config.address,
                                       grpc_config.io_threads);
  }

  static auto analyze_env(const ExecPlan &plan) -> Expected<EnvBindings> {
    return analyze_rpc_env(plan);
  }

  static auto populate_env(RequestContext &ctx, Envelope &env,
                           const EnvBindings &bindings) -> Expected<void> {
    return populate_grpc_env(ctx, env, bindings);
  }

  static auto deadline(const Envelope &env,
                       const std::optional<std::chrono::milliseconds> &fallback)
      -> std::chrono::steady_clock::time_point {
    if (!env.context) {
      return std::chrono::steady_clock::time_point::max();
    }
    return to_steady_deadline(env.context->deadline(), fallback);
  }

  static auto prime_cancellation(Envelope &env, RequestContext &ctx) -> void {
    if (env.context && env.context->IsCancelled()) {
      ctx.cancel();
    }
  }

  static auto attach_request_state(Envelope &env,
                                   const std::shared_ptr<RequestState> &state)
      -> void {
    if (env.responder) {
      env.responder->attach_request_state(state);
    }
  }

  static auto requires_response(const Envelope &) -> bool { return true; }

  static auto responder_available(const Envelope &env) -> bool {
    return static_cast<bool>(env.responder);
  }

  static auto response_sent(const Envelope &env) -> bool {
    return env.responder && env.responder->sent();
  }

  static auto complete(Envelope &) -> void {}

  static auto reject(Envelope &env, grpc::StatusCode code, std::string message)
      -> void {
    if (env.responder) {
      ignore_send(
          env.responder->send(make_error_response(code, std::move(message))));
    }
  }
};

#ifdef SR_ENGINE_WITH_ARROW_FLIGHT
struct FlightTraits {
  using Envelope = FlightEnvelope;
  using EnvBindings = FlightEnvBindings;
  using Transport = FlightServer;

  static auto transport_name() -> std::string_view { return "arrow_flight"; }

  static auto validate_transport(const ServeEndpointConfig &config)
      -> Expected<void> {
    const auto &flight_config = std::get<FlightServeConfig>(config.transport);
    if (flight_config.location.empty()) {
      return tl::unexpected(make_error("serve config missing flight location"));
    }
    return {};
  }

  static auto make_transport(const ServeEndpointConfig &config,
                             Transport::RequestCallback callback)
      -> std::unique_ptr<Transport> {
    const auto &flight_config = std::get<FlightServeConfig>(config.transport);
    return std::make_unique<Transport>(
        std::move(callback), flight_config.location, flight_config.io_threads);
  }

  static auto analyze_env(const ExecPlan &plan) -> Expected<EnvBindings> {
    return analyze_flight_env(plan);
  }

  static auto populate_env(RequestContext &ctx, Envelope &env,
                           const EnvBindings &bindings) -> Expected<void> {
    return populate_flight_env(ctx, env, bindings);
  }

  static auto deadline(const Envelope &env,
                       const std::optional<std::chrono::milliseconds> &fallback)
      -> std::chrono::steady_clock::time_point {
    if (!env.context) {
      return std::chrono::steady_clock::time_point::max();
    }
    return flight_deadline(*env.context, fallback);
  }

  static auto prime_cancellation(Envelope &env, RequestContext &ctx) -> void {
    if (env.context && flight_cancelled(*env.context)) {
      ctx.cancel();
    }
  }

  static auto attach_request_state(Envelope &,
                                   const std::shared_ptr<RequestState> &)
      -> void {}

  static auto requires_response(const Envelope &env) -> bool {
    return env.kind == FlightCallKind::DoAction ||
           env.kind == FlightCallKind::DoGet;
  }

  static auto responder_available(const Envelope &env) -> bool {
    return static_cast<bool>(env.responder) || !requires_response(env);
  }

  static auto response_sent(const Envelope &env) -> bool {
    return env.responder && env.responder->sent();
  }

  static auto complete(Envelope &env) -> void {
    if (env.response) {
      env.response->ensure_ok();
    }
  }

  static auto reject(Envelope &env, grpc::StatusCode code, std::string message)
      -> void {
    if (env.response) {
      env.response->set_status(
          map_grpc_status_to_arrow(code, std::move(message)));
    }
  }
};
#endif

auto normalize_endpoint_names(ServeLayerConfig &config) -> void {
  std::unordered_set<std::string> seen;
  int index = 1;
  for (auto &endpoint : config.endpoints) {
    std::string base = endpoint.name;
    if (base.empty()) {
      base = endpoint.graph_name.empty() ? std::format("endpoint-{}", index++)
                                         : endpoint.graph_name;
    }
    std::string name = base;
    int suffix = 2;
    while (seen.contains(name)) {
      name = std::format("{}-{}", base, suffix++);
    }
    seen.insert(name);
    endpoint.name = std::move(name);
  }
}

} // namespace

struct ServeHost::State {
  explicit State(Runtime &runtime_ref, ServeLayerConfig config_ref)
      : runtime(&runtime_ref), config(std::move(config_ref)) {}

  auto start() -> Expected<void> {
    if (config.endpoints.empty()) {
      return tl::unexpected(make_error("serve config missing endpoints"));
    }
    normalize_endpoint_names(config);

    endpoints.reserve(config.endpoints.size());
    for (auto &endpoint_config : config.endpoints) {
      auto endpoint = make_endpoint(std::move(endpoint_config));
      if (!endpoint) {
        return tl::unexpected(endpoint.error());
      }
      endpoints.push_back(std::move(*endpoint));
    }

    for (auto &endpoint : endpoints) {
      if (auto ok = endpoint->start(); !ok) {
        shutdown();
        return tl::unexpected(ok.error());
      }
    }
    return {};
  }

  auto shutdown() -> void {
    for (auto &endpoint : endpoints) {
      endpoint->shutdown();
    }
  }

  auto wait() -> void {
    for (auto &endpoint : endpoints) {
      endpoint->wait();
    }
  }

  auto stats() const -> std::vector<ServeEndpointSnapshot> {
    std::vector<ServeEndpointSnapshot> out;
    out.reserve(endpoints.size());
    for (const auto &endpoint : endpoints) {
      ServeEndpointSnapshot snapshot;
      snapshot.name = std::string(endpoint->name());
      snapshot.transport = std::string(endpoint->transport());
      snapshot.port = endpoint->port();
      snapshot.running = endpoint->running();
      snapshot.stats = endpoint->stats();
      out.push_back(std::move(snapshot));
    }
    return out;
  }

  auto running_all() const -> bool {
    if (endpoints.empty()) {
      return false;
    }
    for (const auto &endpoint : endpoints) {
      if (!endpoint->running()) {
        return false;
      }
    }
    return true;
  }

  auto make_endpoint(ServeEndpointConfig config)
      -> Expected<std::unique_ptr<ServeEndpointBase>> {
    if (std::holds_alternative<GrpcServeConfig>(config.transport)) {
      return std::unique_ptr<ServeEndpointBase>(
          new ServeEndpoint<GrpcTraits>(*runtime, std::move(config)));
    }
#ifdef SR_ENGINE_WITH_ARROW_FLIGHT
    if (std::holds_alternative<FlightServeConfig>(config.transport)) {
      return std::unique_ptr<ServeEndpointBase>(
          new ServeEndpoint<FlightTraits>(*runtime, std::move(config)));
    }
#else
    if (std::holds_alternative<FlightServeConfig>(config.transport)) {
      return tl::unexpected(make_error("arrow flight support is disabled"));
    }
#endif
    return tl::unexpected(make_error("unsupported serve transport"));
  }

  Runtime *runtime = nullptr;
  ServeLayerConfig config;
  std::vector<std::unique_ptr<ServeEndpointBase>> endpoints;
};

} // namespace sr::engine

namespace sr::engine {

ServeHost::ServeHost(Runtime &runtime, ServeLayerConfig config)
    : state_(std::make_unique<State>(runtime, std::move(config))) {}

ServeHost::~ServeHost() {
  shutdown();
  wait();
}

auto ServeHost::create(Runtime &runtime, ServeLayerConfig config)
    -> Expected<std::unique_ptr<ServeHost>> {
  auto host =
      std::unique_ptr<ServeHost>(new ServeHost(runtime, std::move(config)));
  if (auto ok = host->start(); !ok) {
    return tl::unexpected(ok.error());
  }
  return host;
}

auto ServeHost::start() -> Expected<void> {
  return state_ ? state_->start()
                : tl::unexpected(make_error("serve missing state"));
}

auto ServeHost::shutdown() -> void {
  if (state_) {
    state_->shutdown();
  }
}

auto ServeHost::wait() -> void {
  if (state_) {
    state_->wait();
  }
}

auto ServeHost::stats() const -> std::vector<ServeEndpointSnapshot> {
  if (!state_) {
    return {};
  }
  return state_->stats();
}

auto ServeHost::running() const -> bool {
  return state_ && state_->running_all();
}

} // namespace sr::engine
