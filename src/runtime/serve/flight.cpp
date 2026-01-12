#include "runtime/serve/flight.hpp"

#ifdef SR_ENGINE_WITH_ARROW_FLIGHT

#include <cctype>
#include <cstdint>
#include <format>
#include <string>
#include <string_view>
#include <thread>

#include "engine/plan.hpp"
#include "engine/types.hpp"

namespace sr::engine::serve {
namespace {

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

auto ascii_iequals(std::string_view lhs, std::string_view rhs) -> bool {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (std::size_t i = 0; i < lhs.size(); ++i) {
    const auto a = static_cast<unsigned char>(lhs[i]);
    const auto b = static_cast<unsigned char>(rhs[i]);
    if (std::tolower(a) != std::tolower(b)) {
      return false;
    }
  }
  return true;
}

template <typename T>
concept HasDeadline = requires(const T &value) { value.deadline(); };

template <typename T>
auto flight_deadline_impl(const T &ctx,
                          const std::optional<std::chrono::milliseconds> &fallback)
    -> std::chrono::steady_clock::time_point {
  if constexpr (HasDeadline<T>) {
    const auto deadline = ctx.deadline();
    if (deadline == std::chrono::system_clock::time_point::max()) {
      if (!fallback) {
        return std::chrono::steady_clock::time_point::max();
      }
      return std::chrono::steady_clock::now() + *fallback;
    }
    const auto now_sys = std::chrono::system_clock::now();
    const auto now_steady = std::chrono::steady_clock::now();
    return now_steady + (deadline - now_sys);
  } else {
    if (!fallback) {
      return std::chrono::steady_clock::time_point::max();
    }
    return std::chrono::steady_clock::now() + *fallback;
  }
}

auto remaining_deadline_ms(const RequestContext &ctx) -> std::int64_t {
  if (ctx.deadline == std::chrono::steady_clock::time_point::max()) {
    return -1;
  }
  const auto now = std::chrono::steady_clock::now();
  if (ctx.deadline <= now) {
    return 0;
  }
  const auto remaining =
      std::chrono::duration_cast<std::chrono::milliseconds>(ctx.deadline -
                                                            now);
  return remaining.count();
}

auto validate_type(const EnvRequirement &req, entt::meta_type expected,
                   std::string_view key) -> Expected<void> {
  if (!expected) {
    return tl::unexpected(make_error(
        std::format("missing type registration for env key: {}", key)));
  }
  if (req.type && req.type != expected) {
    return tl::unexpected(make_error(
        std::format("env type mismatch for key: {}", key)));
  }
  return {};
}

} // namespace

auto FlightResponseState::send_action_results(
    std::vector<arrow::flight::Result> results) noexcept -> Expected<void> {
  std::unique_lock<std::mutex> lock(mutex_);
  if (done_) {
    return tl::unexpected(make_error("flight response already completed"));
  }
  results_ = std::move(results);
  status_ = arrow::Status::OK();
  done_ = true;
  sent_.store(true, std::memory_order_release);
  cv_.notify_all();
  return {};
}

auto FlightResponseState::send_record_batches(
    std::shared_ptr<arrow::RecordBatchReader> reader) noexcept
    -> Expected<void> {
  std::unique_lock<std::mutex> lock(mutex_);
  if (done_) {
    return tl::unexpected(make_error("flight response already completed"));
  }
  if (!reader) {
    return tl::unexpected(make_error("flight record batch reader missing"));
  }
  reader_ = std::move(reader);
  status_ = arrow::Status::OK();
  done_ = true;
  sent_.store(true, std::memory_order_release);
  cv_.notify_all();
  return {};
}

auto FlightResponseState::set_status(arrow::Status status) -> void {
  std::unique_lock<std::mutex> lock(mutex_);
  if (done_) {
    return;
  }
  status_ = std::move(status);
  done_ = true;
  cv_.notify_all();
}

auto FlightResponseState::ensure_ok() -> void {
  std::unique_lock<std::mutex> lock(mutex_);
  if (!done_) {
    status_ = arrow::Status::OK();
    done_ = true;
    cv_.notify_all();
  }
}

auto FlightResponseState::wait() -> arrow::Status {
  std::unique_lock<std::mutex> lock(mutex_);
  cv_.wait(lock, [this] { return done_; });
  return status_;
}

auto FlightResponseState::action_results() const
    -> const std::vector<arrow::flight::Result> & {
  return results_;
}

auto FlightResponseState::record_reader() const
    -> const std::shared_ptr<arrow::RecordBatchReader> & {
  return reader_;
}

auto analyze_flight_env(const ExecPlan &plan) -> Expected<FlightEnvBindings> {
  FlightEnvBindings bindings;
  for (const auto &req : plan.env_requirements) {
    if (req.key == kFlightCallKey) {
      if (auto ok =
              validate_type(req, entt::resolve<kernel::flight::FlightServerCall>(),
                            req.key);
          !ok) {
        return tl::unexpected(ok.error());
      }
      bindings.call = true;
      continue;
    }
    if (req.key == kFlightKindKey) {
      if (auto ok = validate_type(
              req, entt::resolve<kernel::flight::FlightCallKind>(), req.key);
          !ok) {
        return tl::unexpected(ok.error());
      }
      bindings.kind = true;
      continue;
    }
    if (req.key == kFlightActionKey) {
      if (auto ok = validate_type(
              req, entt::resolve<std::optional<arrow::flight::Action>>(),
              req.key);
          !ok) {
        return tl::unexpected(ok.error());
      }
      bindings.action = true;
      continue;
    }
    if (req.key == kFlightTicketKey) {
      if (auto ok = validate_type(
              req, entt::resolve<std::optional<arrow::flight::Ticket>>(),
              req.key);
          !ok) {
        return tl::unexpected(ok.error());
      }
      bindings.ticket = true;
      continue;
    }
    if (req.key == kFlightDescriptorKey) {
      if (auto ok = validate_type(
              req,
              entt::resolve<std::optional<arrow::flight::FlightDescriptor>>(),
              req.key);
          !ok) {
        return tl::unexpected(ok.error());
      }
      bindings.descriptor = true;
      continue;
    }
    if (req.key == kFlightReaderKey) {
      if (auto ok = validate_type(
              req, entt::resolve<std::shared_ptr<arrow::flight::FlightMessageReader>>(),
              req.key);
          !ok) {
        return tl::unexpected(ok.error());
      }
      bindings.reader = true;
      continue;
    }
    if (req.key == kFlightWriterKey) {
      if (auto ok = validate_type(
              req, entt::resolve<std::shared_ptr<arrow::flight::FlightMessageWriter>>(),
              req.key);
          !ok) {
        return tl::unexpected(ok.error());
      }
      bindings.writer = true;
      continue;
    }
    if (req.key == kFlightMetadataWriterKey) {
      if (auto ok = validate_type(
              req,
              entt::resolve<std::shared_ptr<arrow::flight::FlightMetadataWriter>>(),
              req.key);
          !ok) {
        return tl::unexpected(ok.error());
      }
      bindings.metadata_writer = true;
      continue;
    }
    if (req.key == kFlightPeerKey) {
      if (auto ok =
              validate_type(req, entt::resolve<std::string>(), req.key);
          !ok) {
        return tl::unexpected(ok.error());
      }
      bindings.peer = true;
      continue;
    }
    if (req.key == kFlightDeadlineKey) {
      if (auto ok = validate_type(req, entt::resolve<std::int64_t>(), req.key);
          !ok) {
        return tl::unexpected(ok.error());
      }
      bindings.deadline_ms = true;
      continue;
    }
    return tl::unexpected(make_error(
        std::format("unsupported serve env key for flight transport: {}",
                    req.key)));
  }
  return bindings;
}

auto populate_flight_env(RequestContext &ctx, FlightEnvelope &env,
                         const FlightEnvBindings &bindings) -> Expected<void> {
  if (bindings.call) {
    kernel::flight::FlightServerCall call;
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
      return tl::unexpected(make_error("flight action missing for call"));
    }
    ctx.set_env(std::string(kFlightActionKey), env.action);
  }
  if (bindings.ticket) {
    if (!env.ticket) {
      return tl::unexpected(make_error("flight ticket missing for call"));
    }
    ctx.set_env(std::string(kFlightTicketKey), env.ticket);
  }
  if (bindings.descriptor) {
    if (!env.descriptor) {
      return tl::unexpected(make_error("flight descriptor missing for call"));
    }
    ctx.set_env(std::string(kFlightDescriptorKey), env.descriptor);
  }
  if (bindings.reader) {
    if (!env.reader) {
      return tl::unexpected(make_error("flight reader missing for call"));
    }
    ctx.set_env(std::string(kFlightReaderKey), env.reader);
  }
  if (bindings.writer) {
    if (!env.writer) {
      return tl::unexpected(make_error("flight writer missing for call"));
    }
    ctx.set_env(std::string(kFlightWriterKey), env.writer);
  }
  if (bindings.metadata_writer) {
    if (!env.metadata_writer) {
      return tl::unexpected(
          make_error("flight metadata writer missing for call"));
    }
    ctx.set_env(std::string(kFlightMetadataWriterKey), env.metadata_writer);
  }
  if (bindings.peer) {
    const auto peer =
        env.context ? std::string(env.context->peer()) : std::string();
    ctx.set_env(std::string(kFlightPeerKey), peer);
  }
  if (bindings.deadline_ms) {
    ctx.set_env(std::string(kFlightDeadlineKey), remaining_deadline_ms(ctx));
  }
  return {};
}

auto find_flight_header_value(const arrow::flight::ServerCallContext &ctx,
                              std::string_view key)
    -> std::optional<std::string_view> {
  if (key.empty()) {
    return std::nullopt;
  }
  const auto &headers = ctx.incoming_headers();
  for (const auto &entry : headers) {
    if (ascii_iequals(entry.first, key)) {
      return entry.second;
    }
  }
  return std::nullopt;
}

auto flight_deadline(const arrow::flight::ServerCallContext &ctx,
                     const std::optional<std::chrono::milliseconds> &fallback)
    -> std::chrono::steady_clock::time_point {
  return flight_deadline_impl(ctx, fallback);
}

auto flight_cancelled(const arrow::flight::ServerCallContext &ctx) -> bool {
  return ctx.is_cancelled();
}

auto map_grpc_status_to_arrow(grpc::StatusCode code, std::string message)
    -> arrow::Status {
  switch (code) {
  case grpc::StatusCode::OK:
    return arrow::Status::OK();
  case grpc::StatusCode::CANCELLED:
    return arrow::Status::Cancelled(std::move(message));
  case grpc::StatusCode::INVALID_ARGUMENT:
    return arrow::Status::Invalid(std::move(message));
  case grpc::StatusCode::NOT_FOUND:
    return arrow::Status::KeyError(std::move(message));
  case grpc::StatusCode::ALREADY_EXISTS:
    return arrow::Status::AlreadyExists(std::move(message));
  case grpc::StatusCode::RESOURCE_EXHAUSTED:
    return arrow::Status::CapacityError(std::move(message));
  case grpc::StatusCode::OUT_OF_RANGE:
    return arrow::Status::IndexError(std::move(message));
  case grpc::StatusCode::UNIMPLEMENTED:
    return arrow::Status::NotImplemented(std::move(message));
  case grpc::StatusCode::FAILED_PRECONDITION:
  case grpc::StatusCode::ABORTED:
    return arrow::Status::Invalid(std::move(message));
  case grpc::StatusCode::DEADLINE_EXCEEDED:
  case grpc::StatusCode::UNAVAILABLE:
  case grpc::StatusCode::INTERNAL:
  case grpc::StatusCode::DATA_LOSS:
  case grpc::StatusCode::PERMISSION_DENIED:
  case grpc::StatusCode::UNAUTHENTICATED:
  default:
    return arrow::Status::IOError(std::move(message));
  }
}

class FlightServer::Impl {
public:
  class CallbackServer : public arrow::flight::FlightServerBase {
  public:
    explicit CallbackServer(RequestCallback callback)
        : callback_(std::move(callback)) {}

    auto DoAction(const arrow::flight::ServerCallContext &context,
                  const arrow::flight::Action &action,
                  std::unique_ptr<arrow::flight::ResultStream> *result)
        -> arrow::Status override {
      auto response = std::make_shared<FlightResponseState>();
      FlightEnvelope env;
      env.context = &context;
      env.kind = kernel::flight::FlightCallKind::DoAction;
      env.action = action;
      env.responder = response;
      env.response = response;

      if (callback_) {
        callback_(std::move(env));
      } else {
        response->set_status(
            arrow::Status::Invalid("flight callback missing"));
      }

      const auto status = response->wait();
      if (!status.ok()) {
        return status;
      }
      auto results = response->action_results();
      *result = std::make_unique<arrow::flight::SimpleResultStream>(
          std::move(results));
      return arrow::Status::OK();
    }

    auto DoGet(const arrow::flight::ServerCallContext &context,
               const arrow::flight::Ticket &ticket,
               std::unique_ptr<arrow::flight::FlightDataStream> *stream)
        -> arrow::Status override {
      auto response = std::make_shared<FlightResponseState>();
      FlightEnvelope env;
      env.context = &context;
      env.kind = kernel::flight::FlightCallKind::DoGet;
      env.ticket = ticket;
      env.responder = response;
      env.response = response;

      if (callback_) {
        callback_(std::move(env));
      } else {
        response->set_status(
            arrow::Status::Invalid("flight callback missing"));
      }

      const auto status = response->wait();
      if (!status.ok()) {
        return status;
      }
      const auto &reader = response->record_reader();
      if (!reader) {
        return arrow::Status::Invalid("flight record reader missing");
      }
      if (stream) {
        *stream = std::make_unique<arrow::flight::RecordBatchStream>(reader);
      }
      return arrow::Status::OK();
    }

    auto DoPut(const arrow::flight::ServerCallContext &context,
               std::unique_ptr<arrow::flight::FlightMessageReader> reader,
               std::unique_ptr<arrow::flight::FlightMetadataWriter> writer)
        -> arrow::Status override {
      auto response = std::make_shared<FlightResponseState>();
      FlightEnvelope env;
      env.context = &context;
      env.kind = kernel::flight::FlightCallKind::DoPut;
      env.reader =
          std::shared_ptr<arrow::flight::FlightMessageReader>(std::move(reader));
      if (env.reader) {
        env.descriptor = env.reader->descriptor();
      }
      env.metadata_writer =
          std::shared_ptr<arrow::flight::FlightMetadataWriter>(std::move(writer));
      env.response = response;

      if (callback_) {
        callback_(std::move(env));
      } else {
        response->set_status(
            arrow::Status::Invalid("flight callback missing"));
      }

      return response->wait();
    }

    auto DoExchange(
        const arrow::flight::ServerCallContext &context,
        std::unique_ptr<arrow::flight::FlightMessageReader> reader,
        std::unique_ptr<arrow::flight::FlightMessageWriter> writer)
        -> arrow::Status override {
      auto response = std::make_shared<FlightResponseState>();
      FlightEnvelope env;
      env.context = &context;
      env.kind = kernel::flight::FlightCallKind::DoExchange;
      env.reader =
          std::shared_ptr<arrow::flight::FlightMessageReader>(std::move(reader));
      if (env.reader) {
        env.descriptor = env.reader->descriptor();
      }
      env.writer =
          std::shared_ptr<arrow::flight::FlightMessageWriter>(std::move(writer));
      env.response = response;

      if (callback_) {
        callback_(std::move(env));
      } else {
        response->set_status(
            arrow::Status::Invalid("flight callback missing"));
      }

      return response->wait();
    }

  private:
    RequestCallback callback_;
  };

  Impl(RequestCallback callback, std::string location, int io_threads)
      : callback_(std::move(callback)), location_(std::move(location)),
        io_threads_(io_threads <= 0 ? 1 : io_threads) {}

  auto start() -> Expected<void> {
    if (running_) {
      return {};
    }
    auto location_result = arrow::flight::Location::Parse(location_);
    if (!location_result.ok()) {
      return tl::unexpected(make_error(location_result.status().ToString()));
    }
    arrow::flight::FlightServerOptions options(*location_result);
    server_ = std::make_unique<CallbackServer>(callback_);
    auto status = server_->Init(options);
    if (!status.ok()) {
      return tl::unexpected(make_error(status.ToString()));
    }
    running_ = true;
    worker_ = std::thread([this]() {
      const auto status = server_->Serve();
      if (!status.ok()) {
        std::lock_guard<std::mutex> lock(mutex_);
        last_status_ = status;
      }
    });
    return {};
  }

  auto shutdown(std::chrono::milliseconds timeout) -> void {
    (void)timeout;
    if (!running_) {
      return;
    }
    running_ = false;
    if (server_) {
      (void)server_->Shutdown();
    }
    if (worker_.joinable()) {
      worker_.join();
    }
    server_.reset();
  }

  auto port() const -> int { return server_ ? server_->port() : 0; }

private:
  RequestCallback callback_;
  std::string location_;
  int io_threads_ = 1;
  std::unique_ptr<CallbackServer> server_;
  std::thread worker_;
  bool running_ = false;
  mutable std::mutex mutex_;
  std::optional<arrow::Status> last_status_;
};

FlightServer::FlightServer(RequestCallback callback, std::string location,
                           int io_threads)
    : impl_(std::make_unique<Impl>(std::move(callback), std::move(location),
                                   io_threads)) {}

FlightServer::~FlightServer() = default;

auto FlightServer::start() -> Expected<void> { return impl_->start(); }

auto FlightServer::shutdown(std::chrono::milliseconds timeout) -> void {
  impl_->shutdown(timeout);
}

auto FlightServer::port() const -> int { return impl_->port(); }

} // namespace sr::engine::serve

#endif // SR_ENGINE_WITH_ARROW_FLIGHT
