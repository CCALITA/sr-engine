#include "runtime/serve/serve.hpp"

#include <atomic>
#include <charconv>
#include <chrono>
#include <exception>
#include <format>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <semaphore>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <exec/async_scope.hpp>
#include <exec/static_thread_pool.hpp>
#include <exec/task.hpp>
#include <stdexec/execution.hpp>

#include "engine/error.hpp"
#include "engine/version.hpp"
#include "kernel/rpc_kernels.hpp"
#include "runtime/runtime.hpp"
#include "runtime/serve/common.hpp"
#include "runtime/serve/grpc.hpp"
#include "runtime/serve/ipc.hpp"
#include "runtime/serve/rpc_env.hpp"
#ifdef SR_ENGINE_WITH_ARROW_FLIGHT
#include "runtime/serve/flight.hpp"
#endif

namespace sr::engine::serve::detail {

struct GraphSelection final {
  std::string name;
  std::optional<Version> version;
};

struct GraphKeyHash final {
  auto operator()(const GraphKey &key) const noexcept -> std::size_t {
    const auto h1 = std::hash<std::string>{}(key.name);
    const auto h2 = std::hash<Version>{}(key.version);
    return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
  }
};

struct GraphKeyEqual final {
  auto operator()(const GraphKey &lhs, const GraphKey &rhs) const noexcept
      -> bool {
    return lhs.version == rhs.version && lhs.name == rhs.name;
  }
};

[[nodiscard]]
auto parse_graph_version(std::string_view value) -> Expected<Version> {
  return Version::parse(value);
}

[[nodiscard]]
auto parse_graph_selection(std::optional<std::string_view> name_value,
                           std::optional<std::string_view> version_value,
                           std::string_view missing_message)
    -> Expected<GraphSelection> {
  if (!name_value || name_value->empty()) {
    return tl::unexpected(make_error(std::string(missing_message)));
  }
  GraphSelection selection;
  selection.name.assign(*name_value);
  if (version_value) {
    if (version_value->empty()) {
      return tl::unexpected(make_error("graph version metadata empty"));
    }
    auto parsed = parse_graph_version(*version_value);
    if (!parsed) {
      return tl::unexpected(parsed.error());
    }
    selection.version = *parsed;
  }
  return selection;
}

auto register_serve_types() -> void {
  sr::kernel::register_rpc_types();
#ifdef SR_ENGINE_WITH_ARROW_FLIGHT
  sr::kernel::register_flight_types();
#endif
}

[[nodiscard]]
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

[[nodiscard]]
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
        inflight_sem(resolve_inflight_capacity(config)),
        use_inflight(config.max_inflight > 0) {
    if (config.request_threads > 0) {
      owned_pool =
          std::make_unique<exec::static_thread_pool>(config.request_threads);
      pool_ = owned_pool.get();
    } else {
      pool_ = &runtime->serve_pool();
    }
    transport_ = Traits::make_transport(
        config, [this](Envelope &&env) { enqueue(std::move(env)); });
  }

  auto start() -> Expected<void> override {
    if (config.graph.metadata.name_header.empty()) {
      return tl::unexpected(
          make_error("serve config missing graph name metadata key"));
    }
    if (auto ok = Traits::validate_transport(config); !ok) {
      return tl::unexpected(ok.error());
    }
    register_serve_types();

    if (auto ok = transport_->start(); !ok) {
      return tl::unexpected(ok.error());
    }

    running_.store(true, std::memory_order_release);
    return {};
  }

  auto shutdown() -> void override {
    if (stopping.exchange(true, std::memory_order_acq_rel)) {
      return;
    }
    transport_->shutdown(config.shutdown_timeout);
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

  auto select_graph(Envelope &env) -> Expected<GraphSelection> {
    return Traits::select_graph(config, env);
  }

  auto resolve_snapshot(const GraphSelection &selection)
      -> Expected<std::shared_ptr<const PlanSnapshot>> {
    if (selection.version) {
      auto snapshot = runtime->resolve(selection.name, *selection.version);
      if (!snapshot) {
        return tl::unexpected(
            make_error(std::format("graph version not found: {} v{}",
                                   selection.name, selection.version->to_string())));
      }
      return snapshot;;
    }
    auto snapshot = runtime->resolve(selection.name);
    if (!snapshot) {
      auto versions = runtime->list_versions(selection.name);
      if (!versions.empty()) {
        return tl::unexpected(make_error(
            std::format("no active version for graph: {}", selection.name)));
      }
      return tl::unexpected(
          make_error(std::format("graph not found: {}", selection.name)));
    }
    return snapshot;
  }

  auto resolve_env_bindings(const PlanSnapshot &snapshot)
      -> Expected<EnvBindings> {
    auto it = env_cache.find(snapshot.key);
    if (it != env_cache.end()) {
      return it->second;
    }
    auto bindings = Traits::analyze_env(snapshot.plan);
    if (!bindings) {
      return tl::unexpected(bindings.error());
    }
    std::unique_lock<std::shared_mutex> lock(env_mutex);
    auto [inserted_it, inserted] = env_cache.emplace(snapshot.key, *bindings);
    if (!inserted) {
      return inserted_it->second;
    }
    return *bindings;
  }

  auto enqueue(Envelope env) -> void {
    if (stopping.load(std::memory_order_acquire)) {
      Traits::reject(env, grpc::StatusCode::UNAVAILABLE,
                     "server shutting down");
      return;
    }
    auto permit = try_acquire_inflight();
    if (!permit) {
      Traits::reject(env, grpc::StatusCode::RESOURCE_EXHAUSTED,
                     "request capacity exceeded");
      rejected.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    accepted.fetch_add(1, std::memory_order_relaxed);
    schedule(std::move(env), std::move(*permit));
  }

  auto schedule(Envelope env, InflightPermit permit) -> void {
    auto scheduler = pool_->get_scheduler();
    scope.spawn(stdexec::on(
        scheduler, handle_request(std::move(env), std::move(permit))));
  }

  auto handle_request(Envelope env, InflightPermit permit) -> exec::task<void> {
    RequestOutcome outcome{*this};
    auto fail = [&](grpc::StatusCode code, std::string message) -> void {
      Traits::reject(env, code, std::move(message));
      outcome.mark_failed();
    };

    try {
      if (Traits::requires_response(env) && !Traits::responder_available(env)) {
        fail(grpc::StatusCode::INTERNAL, "request responder missing");
        co_return;
      }

      auto request_state = std::make_shared<RequestState>();
      Traits::attach_request_state(env, request_state);

      auto &ctx = request_state->ctx;
      ctx.trace.sink = config.trace_sink;
      ctx.trace.sampler = config.trace_sampler;
      ctx.trace.clock = config.trace_clock;
      ctx.trace.flags = config.trace_flags;
      if (config.trace_flags != 0) {
        ctx.trace.trace_id = request_id.fetch_add(1, std::memory_order_relaxed);
        ctx.trace.next_span.store(1, std::memory_order_relaxed);
      }

      ctx.deadline = Traits::deadline(env, config.default_deadline);
      Traits::prime_cancellation(env, ctx);

      if (ctx.should_stop()) {
        fail(status_from_context(ctx, grpc::StatusCode::CANCELLED),
             "request cancelled");
        co_return;
      }

      auto selection = select_graph(env);
      if (!selection) {
        fail(grpc::StatusCode::FAILED_PRECONDITION, selection.error().message);
        co_return;
      }

      auto snapshot_result = resolve_snapshot(*selection);
      if (!snapshot_result) {
        fail(grpc::StatusCode::NOT_FOUND, snapshot_result.error().message);
        co_return;
      }
      auto snapshot = std::move(*snapshot_result);

      auto bindings_result = resolve_env_bindings(*snapshot);
      if (!bindings_result) {
        fail(grpc::StatusCode::FAILED_PRECONDITION,
             bindings_result.error().message);
        co_return;
      }

      auto env_result = Traits::populate_env(ctx, env, *bindings_result);
      if (!env_result) {
        fail(grpc::StatusCode::FAILED_PRECONDITION, env_result.error().message);
        co_return;
      }

      auto result = co_await runtime->run_async(snapshot, ctx);
      if (!result) {
        fail(status_from_context(ctx, grpc::StatusCode::INTERNAL),
             result.error().message);
        co_return;
      }

      if (!Traits::requires_response(env)) {
        Traits::complete(env);
        co_return;
      }

      if (!Traits::response_sent(env)) {
        const auto status =
            status_from_context(ctx, grpc::StatusCode::FAILED_PRECONDITION);
        const auto message = ctx.should_stop() ? "request cancelled"
                                               : "response not sent by graph";
        fail(status, message);
        co_return;
      }
    } catch (const std::exception &e) {
      fail(grpc::StatusCode::INTERNAL,
           std::format("internal error: {}", e.what()));
    } catch (...) {
      fail(grpc::StatusCode::INTERNAL, "unknown internal error");
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

  static auto resolve_inflight_capacity(const ServeEndpointConfig &config)
      -> int {
    if (config.max_inflight <= 0) {
      return 0;
    }
    return config.max_inflight;
  }

  Runtime *runtime = nullptr;
  ServeEndpointConfig config;
  exec::static_thread_pool *pool_ = nullptr;
  std::unique_ptr<exec::static_thread_pool> owned_pool;
  exec::async_scope scope;
  std::unique_ptr<Transport> transport_;
  std::atomic<bool> running_{false};
  std::atomic<bool> stopping{false};
  std::shared_mutex env_mutex;
  std::unordered_map<GraphKey, EnvBindings, GraphKeyHash, GraphKeyEqual>
      env_cache;
  std::atomic<std::uint64_t> request_id{1};
  std::atomic<std::uint64_t> accepted{0};
  std::atomic<std::uint64_t> rejected{0};
  std::atomic<std::uint64_t> completed{0};
  std::atomic<std::uint64_t> failed{0};
  std::atomic<std::uint64_t> inflight{0};
  std::counting_semaphore<std::numeric_limits<int>::max()> inflight_sem;
  bool use_inflight = false;
};

struct GrpcTraits final {
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

  [[nodiscard]]
  static auto analyze_env(const ExecPlan &plan) -> Expected<EnvBindings> {
    return analyze_rpc_env(plan);
  }

  [[nodiscard]]
  static auto populate_env(RequestContext &ctx, Envelope &env,
                           const EnvBindings &bindings) -> Expected<void> {
    return populate_grpc_env(ctx, env, bindings);
  }

  [[nodiscard]]
  static auto select_graph(const ServeEndpointConfig &config,
                           const Envelope &env) -> Expected<GraphSelection> {
    if (!env.context) {
      return tl::unexpected(make_error("rpc context missing"));
    }
    const auto &metadata = config.graph.metadata;
    auto name_value =
        find_grpc_metadata_value(*env.context, metadata.name_header);
    std::optional<std::string_view> version_value;
    if (!metadata.version_header.empty()) {
      version_value =
          find_grpc_metadata_value(*env.context, metadata.version_header);
    }
    return parse_graph_selection(name_value, version_value,
                                 "request missing graph name metadata");
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
    if (env.responder.attach_request_state) {
      env.responder.attach_request_state(state.get());
    }
  }

  static auto requires_response(const Envelope &) -> bool { return true; }

  static auto responder_available(const Envelope &env) -> bool {
    return static_cast<bool>(env.responder);
  }

  static auto response_sent(const Envelope &env) -> bool {
    return static_cast<bool>(env.responder) && env.responder.sent &&
           env.responder.sent();
  }

  static auto complete(Envelope &) -> void {}

  static auto reject(Envelope &env, grpc::StatusCode code, std::string message)
      -> void {
    if (env.responder) {
      ignore_send(
          env.responder.send(make_error_response(code, std::move(message))));
    }
  }
};

namespace detail {

template <typename Envelope, typename EnvBindings>
struct RpcTraitsBase {
  using Bindings = EnvBindings;

  static auto analyze_env(const ExecPlan &plan) -> Expected<Bindings> {
    return analyze_rpc_env(plan);
  }

  static auto requires_response(const Envelope &) -> bool { return true; }

  static auto responder_available(const Envelope &env) -> bool {
    return static_cast<bool>(env.responder);
  }

  static auto response_sent(const Envelope &env) -> bool {
    return static_cast<bool>(env.responder) && env.responder.sent &&
           env.responder.sent();
  }

  static auto complete(Envelope &) -> void {}

  static auto reject(Envelope &env, grpc::StatusCode code, std::string message)
      -> void {
    if (env.responder) {
      ignore_send(
          env.responder.send(make_error_response(code, std::move(message))));
    }
  }

  static auto attach_request_state(Envelope &,
                                   const std::shared_ptr<RequestState> &)
      -> void {}
};

} // namespace detail

struct IpcTraits final {
  using Envelope = IpcEnvelope;
  using EnvBindings = RpcEnvBindings;
  using Transport = IpcServer;

  static auto transport_name() -> std::string_view { return "ipc_unix"; }

  static auto validate_transport(const ServeEndpointConfig &config)
      -> Expected<void> {
    const auto &ipc_config = std::get<IpcServeConfig>(config.transport);
    if (ipc_config.path.empty()) {
      return tl::unexpected(make_error("serve config missing ipc path"));
    }
    if (ipc_config.max_message_bytes == 0) {
      return tl::unexpected(make_error("ipc max_message_bytes must be > 0"));
    }
    return {};
  }

  static auto make_transport(const ServeEndpointConfig &config,
                             Transport::RequestCallback callback)
      -> std::unique_ptr<Transport> {
    const auto &ipc_config = std::get<IpcServeConfig>(config.transport);
    return std::make_unique<Transport>(
        std::move(callback), ipc_config.path, ipc_config.io_threads,
        ipc_config.backlog, ipc_config.max_message_bytes,
        ipc_config.remove_existing);
  }

  [[nodiscard]]
  static auto analyze_env(const ExecPlan &plan) -> Expected<EnvBindings> {
    return analyze_rpc_env(plan);
  }

  [[nodiscard]]
  static auto populate_env(RequestContext &ctx, Envelope &env,
                           const EnvBindings &bindings) -> Expected<void> {
    return populate_ipc_env(ctx, env, bindings);
  }

  [[nodiscard]]
  static auto select_graph(const ServeEndpointConfig &config,
                           const Envelope &env) -> Expected<GraphSelection> {
    const auto &metadata = config.graph.metadata;
    auto name_value =
        find_ipc_metadata_value(env.metadata, metadata.name_header);
    std::optional<std::string_view> version_value;
    if (!metadata.version_header.empty()) {
      version_value =
          find_ipc_metadata_value(env.metadata, metadata.version_header);
    }
    return parse_graph_selection(name_value, version_value,
                                 "request missing graph name metadata");
  }

  [[nodiscard]]
  static auto deadline(const Envelope &,
                       const std::optional<std::chrono::milliseconds> &fallback)
      -> std::chrono::steady_clock::time_point {
    if (!fallback) {
      return std::chrono::steady_clock::time_point::max();
    }
    return std::chrono::steady_clock::now() + *fallback;
  }

  static auto prime_cancellation(Envelope &, RequestContext &) -> void {}

  static auto attach_request_state(Envelope &,
                                   const std::shared_ptr<RequestState> &)
      -> void {}

  static auto requires_response(const Envelope &) -> bool { return true; }

  static auto responder_available(const Envelope &env) -> bool {
    return static_cast<bool>(env.responder);
  }

  static auto response_sent(const Envelope &env) -> bool {
    return static_cast<bool>(env.responder) && env.responder.sent &&
           env.responder.sent();
  }

  static auto complete(Envelope &) -> void {}

  static auto reject(Envelope &env, grpc::StatusCode code, std::string message)
      -> void {
    if (env.responder) {
      ignore_send(
          env.responder.send(make_error_response(code, std::move(message))));
    }
  }
};

#ifdef SR_ENGINE_WITH_ARROW_FLIGHT
struct FlightTraits final {
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

  [[nodiscard]]
  static auto analyze_env(const ExecPlan &plan) -> Expected<EnvBindings> {
    return analyze_flight_env(plan);
  }

  [[nodiscard]]
  static auto populate_env(RequestContext &ctx, Envelope &env,
                           const EnvBindings &bindings) -> Expected<void> {
    return populate_flight_env(ctx, env, bindings);
  }

  [[nodiscard]]
  static auto select_graph(const ServeEndpointConfig &config,
                           const Envelope &env) -> Expected<GraphSelection> {
    if (!env.context) {
      return tl::unexpected(make_error("flight context missing"));
    }
    const auto &metadata = config.graph.metadata;
    auto name_value =
        find_flight_header_value(*env.context, metadata.name_header);
    std::optional<std::string_view> version_value;
    if (!metadata.version_header.empty()) {
      version_value =
          find_flight_header_value(*env.context, metadata.version_header);
    }
    return parse_graph_selection(name_value, version_value,
                                 "request missing graph name metadata");
  }

  [[nodiscard]]
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
    return env.kind == kernel::flight::FlightCallKind::DoAction ||
           env.kind == kernel::flight::FlightCallKind::DoGet;
  }

  static auto responder_available(const Envelope &env) -> bool {
    return static_cast<bool>(env.responder) || !requires_response(env);
  }

  static auto response_sent(const Envelope &env) -> bool {
    return static_cast<bool>(env.responder) && env.responder->sent() &&
           env.responder->sent();
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
      base = std::format("endpoint-{}", index++);
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

} // namespace sr::engine::serve::detail

namespace sr::engine {

struct ServeHost::State final {
  explicit State(Runtime &runtime_ref, ServeLayerConfig config_ref)
      : runtime(&runtime_ref), config(std::move(config_ref)) {}

  auto start() -> Expected<void> {
    if (config.endpoints.empty()) {
      return tl::unexpected(make_error("serve config missing endpoints"));
    }
    serve::detail::normalize_endpoint_names(config);

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
      -> Expected<std::unique_ptr<serve::detail::ServeEndpointBase>> {
    if (std::holds_alternative<GrpcServeConfig>(config.transport)) {
      return std::unique_ptr<serve::detail::ServeEndpointBase>(
          new serve::detail::ServeEndpoint<serve::detail::GrpcTraits>(
              *runtime, std::move(config)));
    }
    if (std::holds_alternative<IpcServeConfig>(config.transport)) {
      return std::unique_ptr<serve::detail::ServeEndpointBase>(
          new serve::detail::ServeEndpoint<serve::detail::IpcTraits>(
              *runtime, std::move(config)));
    }
#ifdef SR_ENGINE_WITH_ARROW_FLIGHT
    if (std::holds_alternative<FlightServeConfig>(config.transport)) {
      return std::unique_ptr<serve::detail::ServeEndpointBase>(
          new serve::detail::ServeEndpoint<serve::detail::FlightTraits>(
              *runtime, std::move(config)));
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
  std::vector<std::unique_ptr<serve::detail::ServeEndpointBase>> endpoints;
};

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
