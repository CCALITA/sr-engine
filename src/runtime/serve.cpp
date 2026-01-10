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
#include <utility>
#include <vector>

#include <exec/async_scope.hpp>
#include <exec/static_thread_pool.hpp>
#include <stdexec/execution.hpp>

#include <grpcpp/generic/async_generic_service.h>
#include <grpcpp/grpcpp.h>

#include "engine/error.hpp"
#include "engine/types.hpp"
#include "kernel/rpc_kernels.hpp"
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

class GrpcResponder;

struct RequestEnvelope {
  grpc::ByteBuffer payload;
  const grpc::GenericServerContext *context = nullptr;
  std::shared_ptr<GrpcResponder> responder;
};

/// Thread-safe bounded queue for incoming RPC requests.
class RequestQueue {
public:
  explicit RequestQueue(std::size_t capacity) : capacity_(capacity) {}

  auto push(RequestEnvelope &&env) -> bool {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_ || (capacity_ > 0 && queue_.size() >= capacity_)) {
      return false;
    }
    queue_.push_back(std::move(env));
    wakeup_.notify_one();
    return true;
  }

  auto pop(RequestEnvelope &out) -> bool {
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
  std::deque<RequestEnvelope> queue_;
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
    return tl::unexpected(make_error(
        std::format("unsupported serve env key: {}", req.key)));
  }
  return bindings;
}

auto register_serve_types() -> void {
  sr::engine::register_type<int64_t>("int64");
  sr::engine::register_type<std::string>("string");
  sr::kernel::register_rpc_types();
}

auto to_steady_deadline(std::chrono::system_clock::time_point deadline,
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
    metadata.entries.push_back(
        {std::string(key.data(), key.size()),
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

auto ignore_send(Expected<void> result) -> void {
  (void)result;
}

auto status_from_context(const RequestContext &ctx,
                         grpc::StatusCode fallback) -> grpc::StatusCode {
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

/// Populate RequestContext env with only the rpc.* values required by the plan.
auto populate_request_env(RequestContext &ctx, RequestEnvelope &env,
                          const RpcEnvBindings &bindings) -> void {
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
  auto build_envelope() -> RequestEnvelope;

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
  using RequestCallback = std::function<void(RequestEnvelope &&)>;

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
    const auto deadline =
        std::chrono::system_clock::now() + timeout;
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

  auto on_request(RequestEnvelope &&env) -> void {
    on_request_(std::move(env));
  }

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

auto GrpcResponder::sent() const -> bool {
  return call_ && call_->responded();
}

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
  response_status_ = grpc::Status(
      static_cast<grpc::StatusCode>(response.status.code),
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

auto GrpcCall::build_envelope() -> RequestEnvelope {
  RequestEnvelope env;
  env.payload = std::move(request_);
  env.context = &ctx_;
  env.responder = std::make_shared<GrpcResponder>(this);
  return env;
}

} // namespace

struct ServeHost::State {
  struct InflightPermit {
    State *state = nullptr;
    bool acquired = false;

    InflightPermit() = default;
    InflightPermit(State *state_in, bool acquired_in)
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
    State *state = nullptr;
    bool failed = false;

    explicit RequestOutcome(State &state_in) : state(&state_in) {}
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

  explicit State(Runtime &runtime_ref, ServeConfig config_ref)
      : runtime(&runtime_ref), config(std::move(config_ref)),
        request_pool(resolve_request_threads(config)),
        inflight_sem(resolve_inflight_capacity(config)),
        use_inflight(config.max_inflight > 0),
        server(std::make_unique<GrpcServer>(
            [this](RequestEnvelope &&env) { enqueue(std::move(env)); },
            config.address, config.io_threads)) {}

  auto start() -> Expected<void> {
    if (config.graph_name.empty()) {
      return tl::unexpected(make_error("serve config missing graph_name"));
    }
    if (config.address.empty()) {
      return tl::unexpected(make_error("serve config missing address"));
    }
    register_serve_types();
    if (config.graph_version) {
      pinned_snapshot = runtime->resolve(config.graph_name, *config.graph_version);
      if (!pinned_snapshot) {
        return tl::unexpected(make_error(std::format(
            "graph version not found: {} v{}", config.graph_name,
            *config.graph_version)));
      }
      auto bindings = analyze_rpc_env(pinned_snapshot->plan);
      if (!bindings) {
        return tl::unexpected(bindings.error());
      }
      pinned_env = std::move(*bindings);
    }

    if (auto ok = server->start(); !ok) {
      return tl::unexpected(ok.error());
    }

    if (config.queue_capacity > 0) {
      queue = std::make_unique<RequestQueue>(config.queue_capacity);
      const int threads = config.dispatch_threads > 0 ? config.dispatch_threads : 1;
      dispatch_threads.reserve(static_cast<std::size_t>(threads));
      for (int i = 0; i < threads; ++i) {
        dispatch_threads.emplace_back([this] { dispatch_loop(); });
      }
    }

    running.store(true, std::memory_order_release);
    return {};
  }

  auto shutdown() -> void {
    if (stopping.exchange(true, std::memory_order_acq_rel)) {
      return;
    }
    server->shutdown(config.shutdown_timeout);
    if (queue) {
      queue->close();
    }
    for (auto &thread : dispatch_threads) {
      if (thread.joinable()) {
        thread.join();
      }
    }
    scope.request_stop();
  }

  auto wait() -> void {
    if (!running.load(std::memory_order_acquire)) {
      return;
    }
    stdexec::sync_wait(scope.on_empty());
  }

  auto stats() const -> ServeStats {
    ServeStats snapshot;
    snapshot.accepted = accepted.load(std::memory_order_relaxed);
    snapshot.rejected = rejected.load(std::memory_order_relaxed);
    snapshot.completed = completed.load(std::memory_order_relaxed);
    snapshot.failed = failed.load(std::memory_order_relaxed);
    snapshot.inflight = inflight.load(std::memory_order_relaxed);
    snapshot.queued = queued.load(std::memory_order_relaxed);
    return snapshot;
  }

  auto port() const -> int { return server ? server->port() : 0; }

  auto is_running() const -> bool {
    return running.load(std::memory_order_acquire);
  }

  auto enqueue(RequestEnvelope env) -> void {
    if (stopping.load(std::memory_order_acquire)) {
      reject(std::move(env), grpc::StatusCode::UNAVAILABLE,
             "server shutting down");
      return;
    }
    if (queue) {
      if (!queue->push(std::move(env))) {
        reject(std::move(env), grpc::StatusCode::RESOURCE_EXHAUSTED,
               "request queue full");
        return;
      }
      accepted.fetch_add(1, std::memory_order_relaxed);
      queued.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    auto permit = try_acquire_inflight();
    if (!permit) {
      reject(std::move(env), grpc::StatusCode::RESOURCE_EXHAUSTED,
             "request capacity exceeded");
      return;
    }
    accepted.fetch_add(1, std::memory_order_relaxed);
    schedule(std::move(env), std::move(*permit));
  }

  auto dispatch_loop() -> void {
    RequestEnvelope env;
    while (queue && queue->pop(env)) {
      queued.fetch_sub(1, std::memory_order_relaxed);
      auto permit = acquire_inflight();
      schedule(std::move(env), std::move(permit));
    }
  }

  auto schedule(RequestEnvelope env, InflightPermit permit) -> void {
    auto scheduler = request_pool.get_scheduler();
    auto sender = stdexec::schedule(scheduler) |
                  stdexec::then([this, env = std::move(env),
                                 permit = std::move(permit)]() mutable {
                    handle_request(std::move(env), std::move(permit));
                  });
    scope.spawn(std::move(sender));
  }

  auto handle_request(RequestEnvelope env,
                      [[maybe_unused]] InflightPermit permit) -> void {
    auto responder = env.responder;
    RequestOutcome outcome{*this};
    auto fail = [&](grpc::StatusCode code, std::string message) -> void {
      if (responder && !responder->sent()) {
        ignore_send(responder->send(
            make_error_response(code, std::move(message))));
      }
      outcome.mark_failed();
    };

    if (!responder || !env.context) {
      fail(grpc::StatusCode::INTERNAL, "grpc responder missing");
      return;
    }

    auto request_state = std::make_shared<RequestState>();
    responder->attach_request_state(request_state);

    auto &ctx = request_state->ctx;
    ctx.trace.sink = config.trace_sink;
    ctx.trace.sampler = config.trace_sampler;
    ctx.trace.clock = config.trace_clock;
    ctx.trace.flags = config.trace_flags;
    ctx.trace.trace_id = request_id.fetch_add(1, std::memory_order_relaxed);
    ctx.trace.next_span.store(1, std::memory_order_relaxed);

    ctx.deadline =
        to_steady_deadline(env.context->deadline(), config.default_deadline);

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

    RpcEnvBindings bindings;
    if (pinned_env) {
      bindings = *pinned_env;
    } else {
      auto bindings_result = analyze_rpc_env(snapshot->plan);
      if (!bindings_result) {
        fail(grpc::StatusCode::FAILED_PRECONDITION,
             bindings_result.error().message);
        return;
      }
      bindings = std::move(*bindings_result);
    }
    populate_request_env(ctx, env, bindings);

    auto result = runtime->run(snapshot, ctx);
    if (!result) {
      fail(status_from_context(ctx, grpc::StatusCode::INTERNAL),
           result.error().message);
      return;
    }

    if (!responder->sent()) {
      const auto status =
          status_from_context(ctx, grpc::StatusCode::FAILED_PRECONDITION);
      const auto message = ctx.should_stop() ? "request cancelled"
                                             : "rpc response not sent by graph";
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

  auto reject(RequestEnvelope env, grpc::StatusCode code,
              std::string message) -> void {
    rejected.fetch_add(1, std::memory_order_relaxed);
    if (env.responder) {
      ignore_send(env.responder->send(make_error_response(code, std::move(message))));
    }
  }

  static auto resolve_request_threads(const ServeConfig &config) -> int {
    if (config.request_threads > 0) {
      return config.request_threads;
    }
    const auto hw = std::thread::hardware_concurrency();
    return hw > 0 ? static_cast<int>(hw) : 1;
  }

  static auto resolve_inflight_capacity(const ServeConfig &config) -> int {
    if (config.max_inflight <= 0) {
      return 0;
    }
    return config.max_inflight;
  }

  Runtime *runtime = nullptr;
  ServeConfig config;
  exec::static_thread_pool request_pool;
  exec::async_scope scope;
  std::unique_ptr<RequestQueue> queue;
  std::vector<std::thread> dispatch_threads;
  std::unique_ptr<GrpcServer> server;
  std::atomic<bool> running{false};
  std::atomic<bool> stopping{false};
  std::shared_ptr<const PlanSnapshot> pinned_snapshot;
  std::optional<RpcEnvBindings> pinned_env;
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

} // namespace sr::engine

namespace sr::engine {

ServeHost::ServeHost(Runtime &runtime, ServeConfig config)
    : state_(std::make_unique<State>(runtime, std::move(config))) {}

ServeHost::~ServeHost() {
  shutdown();
  wait();
}

auto ServeHost::create(Runtime &runtime, ServeConfig config)
    -> Expected<std::unique_ptr<ServeHost>> {
  auto host =
      std::unique_ptr<ServeHost>(new ServeHost(runtime, std::move(config)));
  if (auto ok = host->start(); !ok) {
    return tl::unexpected(ok.error());
  }
  return host;
}

auto ServeHost::start() -> Expected<void> {
  return state_ ? state_->start() : tl::unexpected(make_error("serve missing state"));
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

auto ServeHost::stats() const -> ServeStats {
  if (!state_) {
    return {};
  }
  return state_->stats();
}

auto ServeHost::port() const -> int {
  return state_ ? state_->port() : 0;
}

auto ServeHost::running() const -> bool {
  return state_ && state_->is_running();
}

} // namespace sr::engine
