#pragma once

#ifdef SR_ENGINE_WITH_ARROW_FLIGHT

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <arrow/flight/api.h>
#include <grpcpp/grpcpp.h>

#include "engine/error.hpp"
#include "kernel/flight_kernels.hpp"

#include "engine/type_system.hpp"

namespace sr::engine {
struct ExecPlan;
struct RequestContext;
} // namespace sr::engine

namespace sr::engine::serve {

/// In-flight Flight request envelope handed to the serve layer.
struct FlightEnvelope {
  const arrow::flight::ServerCallContext *context = nullptr;
  kernel::flight::FlightCallKind kind = kernel::flight::FlightCallKind::DoAction;
  std::optional<arrow::flight::Action> action;
  std::optional<arrow::flight::Ticket> ticket;
  std::optional<arrow::flight::FlightDescriptor> descriptor;
  std::shared_ptr<arrow::flight::FlightMessageReader> reader;
  std::shared_ptr<arrow::flight::FlightMessageWriter> writer;
  std::shared_ptr<arrow::flight::FlightMetadataWriter> metadata_writer;
  std::shared_ptr<class FlightResponseState> responder;
  std::shared_ptr<class FlightResponseState> response;
};

/// Resolved env requirements for Flight transports.
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
};

/// Response sink and completion state for Flight calls.
class FlightResponseState final : public kernel::flight::FlightResponder {
public:
  auto send_action_results(std::vector<arrow::flight::Result> results) noexcept
      -> Expected<void> override;
  auto send_record_batches(
      std::shared_ptr<arrow::RecordBatchReader> reader) noexcept
      -> Expected<void> override;

  auto sent() const -> bool { return sent_.load(std::memory_order_acquire); }
  auto set_status(arrow::Status status) -> void;
  auto ensure_ok() -> void;

  auto wait() -> arrow::Status;
  auto action_results() const -> const std::vector<arrow::flight::Result> &;
  auto record_reader() const -> const std::shared_ptr<arrow::RecordBatchReader> &;

private:
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  bool done_ = false;
  arrow::Status status_{arrow::Status::OK()};
  std::vector<arrow::flight::Result> results_;
  std::shared_ptr<arrow::RecordBatchReader> reader_;
  std::atomic<bool> sent_{false};
};

/// Inspect an ExecPlan and return the required flight env bindings.
auto analyze_flight_env(const ExecPlan &plan, TypeRegistry& registry) -> Expected<FlightEnvBindings>;

/// Populate RequestContext.env for a Flight request.
auto populate_flight_env(RequestContext &ctx, FlightEnvelope &env,
                         const FlightEnvBindings &bindings) -> Expected<void>;

/// Find a metadata value on a Flight call context.
auto find_flight_header_value(const arrow::flight::ServerCallContext &ctx,
                              std::string_view key)
    -> std::optional<std::string_view>;

/// Convert a Flight deadline to a steady clock deadline.
auto flight_deadline(const arrow::flight::ServerCallContext &ctx,
                     const std::optional<std::chrono::milliseconds> &fallback)
    -> std::chrono::steady_clock::time_point;

/// Check for cancellation on a Flight call.
auto flight_cancelled(const arrow::flight::ServerCallContext &ctx) -> bool;

/// Map a gRPC status into an Arrow status.
auto map_grpc_status_to_arrow(grpc::StatusCode code, std::string message)
    -> arrow::Status;

/// Arrow Flight transport adapter.
class FlightServer {
public:
  using RequestCallback = std::function<void(FlightEnvelope &&)>;

  FlightServer(RequestCallback callback, std::string location, int io_threads);
  ~FlightServer();

  FlightServer(const FlightServer &) = delete;
  FlightServer &operator=(const FlightServer &) = delete;

  auto start() -> Expected<void>;
  auto shutdown(std::chrono::milliseconds timeout) -> void;
  auto port() const -> int;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace sr::engine::serve

#endif // SR_ENGINE_WITH_ARROW_FLIGHT
