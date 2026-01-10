#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "engine/error.hpp"
#include "engine/trace.hpp"

namespace sr::engine {

class Runtime;

/// Live stats snapshot for the serve layer.
struct ServeStats {
  std::uint64_t accepted = 0;
  std::uint64_t rejected = 0;
  std::uint64_t completed = 0;
  std::uint64_t failed = 0;
  std::uint64_t inflight = 0;
  std::uint64_t queued = 0;
};

/// Configuration for the unary gRPC serve layer.
struct ServeConfig {
  /// Graph name to execute for each request.
  std::string graph_name;
  /// Optional pinned version (unset uses the active version).
  std::optional<int> graph_version;
  /// gRPC listen address.
  std::string address = "0.0.0.0:50051";
  /// gRPC CQ worker count (<=0 uses 1).
  int io_threads = 1;
  /// Queue dispatcher threads (<=0 uses 1).
  int dispatch_threads = 1;
  /// Request scheduler threads (<=0 uses hardware concurrency).
  int request_threads = 0;
  /// Max in-flight requests (<=0 is unbounded).
  int max_inflight = 1024;
  /// Pending queue capacity (0 disables the queue).
  std::size_t queue_capacity = 1024;
  /// Default request timeout when gRPC deadlines are unset.
  std::optional<std::chrono::milliseconds> default_deadline;
  /// Graceful shutdown timeout for draining requests.
  std::chrono::milliseconds shutdown_timeout{std::chrono::seconds(5)};
  /// Trace sink wired into per-request contexts.
  trace::TraceSinkRef trace_sink;
  /// Optional trace sampler used per request.
  trace::TraceSampler trace_sampler;
  /// Clock used for trace timestamps.
  trace::TraceClock trace_clock{&trace::steady_tick};
  /// Default trace flags when sampling is disabled.
  trace::TraceFlags trace_flags =
      trace::to_flags(trace::TraceFlag::RunSpan) |
      trace::to_flags(trace::TraceFlag::NodeSpan);
};

/// Long-lived gRPC server that runs a graph per unary request.
class ServeHost {
public:
  /// Create and start a ServeHost bound to the provided runtime.
  /// The runtime must outlive the ServeHost instance.
  static auto create(Runtime& runtime, ServeConfig config)
      -> Expected<std::unique_ptr<ServeHost>>;

  ServeHost(const ServeHost&) = delete;
  ServeHost& operator=(const ServeHost&) = delete;
  ServeHost(ServeHost&&) = delete;
  ServeHost& operator=(ServeHost&&) = delete;

  /// Stop accepting new requests and drain outstanding work.
  auto shutdown() -> void;
  /// Block until all in-flight requests have completed.
  auto wait() -> void;
  /// Return a snapshot of current serve-layer counters.
  auto stats() const -> ServeStats;
  /// Return the bound server port (0 if not started).
  auto port() const -> int;
  /// Return true while the server is running.
  auto running() const -> bool;

  ~ServeHost();

private:
  explicit ServeHost(Runtime& runtime, ServeConfig config);
  auto start() -> Expected<void>;

  struct State;
  std::unique_ptr<State> state_;
};

}  // namespace sr::engine
