#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string_view>

namespace sr::engine {

enum class TaskType : int;

namespace trace {

/// Trace identifiers and timestamp tick types.
using TraceId = std::uint64_t;
using SpanId = std::uint64_t;
using Tick = std::uint64_t;

/// Span completion status used for run/node events.
enum class SpanStatus : std::uint8_t {
  Ok,
  Error,
  Cancelled,
  Deadline,
  Skipped,
};

/// Feature flags enabling optional trace events.
enum class TraceFlag : std::uint32_t {
  RunSpan = 1u << 0,
  NodeSpan = 1u << 1,
  QueueDelay = 1u << 2,
  ErrorDetail = 1u << 3,
  ValueSizes = 1u << 4,
};

using TraceFlags = std::uint32_t;

constexpr auto to_flags(TraceFlag flag) -> TraceFlags {
  return static_cast<TraceFlags>(flag);
}

constexpr auto has_flag(TraceFlags flags, TraceFlag flag) -> bool {
  return (flags & to_flags(flag)) != 0;
}

/// Monotonic timestamp in nanoseconds since steady clock epoch.
inline auto steady_tick() -> Tick {
  using clock = std::chrono::steady_clock;
  return static_cast<Tick>(
    std::chrono::duration_cast<std::chrono::nanoseconds>(clock::now().time_since_epoch()).count());
}

/// Run-level start event.
struct RunStart {
  TraceId trace_id = 0;
  SpanId span_id = 0;
  std::string_view plan_name;
  Tick ts = 0;
};

/// Run-level completion event.
struct RunEnd {
  TraceId trace_id = 0;
  SpanId span_id = 0;
  Tick ts = 0;
  Tick duration = 0;
  SpanStatus status = SpanStatus::Ok;
};

/// Node execution start event.
struct NodeStart {
  TraceId trace_id = 0;
  SpanId span_id = 0;
  SpanId parent_span_id = 0;
  std::string_view node_id;
  int node_index = -1;
  TaskType task_type = TaskType{};
  Tick ts = 0;
};

/// Node execution completion event.
struct NodeEnd {
  TraceId trace_id = 0;
  SpanId span_id = 0;
  std::string_view node_id;
  int node_index = -1;
  Tick ts = 0;
  Tick duration = 0;
  SpanStatus status = SpanStatus::Ok;
};

/// Node execution error event.
struct NodeError {
  TraceId trace_id = 0;
  SpanId span_id = 0;
  std::string_view node_id;
  int node_index = -1;
  std::string_view message;
};

/// Queue wait measurement for a node dispatch.
struct QueueDelay {
  TraceId trace_id = 0;
  SpanId span_id = 0;
  std::string_view node_id;
  int node_index = -1;
  Tick enqueue_ts = 0;
  Tick start_ts = 0;
};

/// Clock provider for tracing.
struct TraceClock {
  Tick (*now)() = nullptr;
};

/// Sampler callback for enabling trace flags per run.
struct TraceSampler {
  void* user = nullptr;
  TraceFlags (*decide)(void*, std::string_view, TraceId) = nullptr;
};

/// Type-erased sink that receives tracing callbacks.
struct TraceSinkRef {
  void* self = nullptr;
  void (*run_start)(void*, const RunStart&) = nullptr;
  void (*run_end)(void*, const RunEnd&) = nullptr;
  void (*node_start)(void*, const NodeStart&) = nullptr;
  void (*node_end)(void*, const NodeEnd&) = nullptr;
  void (*node_error)(void*, const NodeError&) = nullptr;
  void (*queue_delay)(void*, const QueueDelay&) = nullptr;

  /// Returns true when any callback is installed.
  auto enabled() const -> bool {
    return run_start || run_end || node_start || node_end || node_error || queue_delay;
  }
};

namespace detail {

template <typename Sink>
constexpr bool has_run_start = requires(Sink& sink, const RunStart& event) { sink.on_run_start(event); };

template <typename Sink>
constexpr bool has_run_end = requires(Sink& sink, const RunEnd& event) { sink.on_run_end(event); };

template <typename Sink>
constexpr bool has_node_start = requires(Sink& sink, const NodeStart& event) { sink.on_node_start(event); };

template <typename Sink>
constexpr bool has_node_end = requires(Sink& sink, const NodeEnd& event) { sink.on_node_end(event); };

template <typename Sink>
constexpr bool has_node_error = requires(Sink& sink, const NodeError& event) { sink.on_node_error(event); };

template <typename Sink>
constexpr bool has_queue_delay = requires(Sink& sink, const QueueDelay& event) { sink.on_queue_delay(event); };

template <typename Sink>
auto bind_run_start(void (**slot)(void*, const RunStart&)) -> void {
  if constexpr (has_run_start<Sink>) {
    *slot = [](void* self, const RunStart& event) {
      static_cast<Sink*>(self)->on_run_start(event);
    };
  } else {
    *slot = nullptr;
  }
}

template <typename Sink>
auto bind_run_end(void (**slot)(void*, const RunEnd&)) -> void {
  if constexpr (has_run_end<Sink>) {
    *slot = [](void* self, const RunEnd& event) {
      static_cast<Sink*>(self)->on_run_end(event);
    };
  } else {
    *slot = nullptr;
  }
}

template <typename Sink>
auto bind_node_start(void (**slot)(void*, const NodeStart&)) -> void {
  if constexpr (has_node_start<Sink>) {
    *slot = [](void* self, const NodeStart& event) {
      static_cast<Sink*>(self)->on_node_start(event);
    };
  } else {
    *slot = nullptr;
  }
}

template <typename Sink>
auto bind_node_end(void (**slot)(void*, const NodeEnd&)) -> void {
  if constexpr (has_node_end<Sink>) {
    *slot = [](void* self, const NodeEnd& event) {
      static_cast<Sink*>(self)->on_node_end(event);
    };
  } else {
    *slot = nullptr;
  }
}

template <typename Sink>
auto bind_node_error(void (**slot)(void*, const NodeError&)) -> void {
  if constexpr (has_node_error<Sink>) {
    *slot = [](void* self, const NodeError& event) {
      static_cast<Sink*>(self)->on_node_error(event);
    };
  } else {
    *slot = nullptr;
  }
}

template <typename Sink>
auto bind_queue_delay(void (**slot)(void*, const QueueDelay&)) -> void {
  if constexpr (has_queue_delay<Sink>) {
    *slot = [](void* self, const QueueDelay& event) {
      static_cast<Sink*>(self)->on_queue_delay(event);
    };
  } else {
    *slot = nullptr;
  }
}

}  // namespace detail

/// Create a TraceSinkRef from a sink object with on_* methods.
template <typename Sink>
auto make_sink(Sink& sink) -> TraceSinkRef {
  TraceSinkRef ref;
  ref.self = &sink;
  detail::bind_run_start<Sink>(&ref.run_start);
  detail::bind_run_end<Sink>(&ref.run_end);
  detail::bind_node_start<Sink>(&ref.node_start);
  detail::bind_node_end<Sink>(&ref.node_end);
  detail::bind_node_error<Sink>(&ref.node_error);
  detail::bind_queue_delay<Sink>(&ref.queue_delay);
  return ref;
}

/// Per-request tracing configuration and state.
struct TraceContext {
  TraceSinkRef sink;
  TraceClock clock{&steady_tick};
  TraceSampler sampler;
  TraceFlags flags = 0;
  TraceId trace_id = 0;
  std::atomic<SpanId> next_span{1};

  /// Returns true when tracing is enabled and flags are set.
  auto enabled() const -> bool {
    return sink.enabled() && flags != 0;
  }

  auto apply_sampler(std::string_view plan_name) -> void {
    if (sampler.decide) {
      flags = sampler.decide(sampler.user, plan_name, trace_id);
    }
  }

  auto now() const -> Tick {
    return clock.now ? clock.now() : 0;
  }

  auto new_span() -> SpanId {
    return next_span.fetch_add(1, std::memory_order_relaxed);
  }
};

#if defined(SR_TRACE_DISABLED)
inline constexpr bool kTraceEnabled = false;
#else
inline constexpr bool kTraceEnabled = true;
#endif

/// Emit a RunStart event when a callback is installed.
inline auto emit(TraceSinkRef sink, const RunStart& event) -> void {
  if (sink.run_start) {
    sink.run_start(sink.self, event);
  }
}

/// Emit a RunEnd event when a callback is installed.
inline auto emit(TraceSinkRef sink, const RunEnd& event) -> void {
  if (sink.run_end) {
    sink.run_end(sink.self, event);
  }
}

/// Emit a NodeStart event when a callback is installed.
inline auto emit(TraceSinkRef sink, const NodeStart& event) -> void {
  if (sink.node_start) {
    sink.node_start(sink.self, event);
  }
}

/// Emit a NodeEnd event when a callback is installed.
inline auto emit(TraceSinkRef sink, const NodeEnd& event) -> void {
  if (sink.node_end) {
    sink.node_end(sink.self, event);
  }
}

/// Emit a NodeError event when a callback is installed.
inline auto emit(TraceSinkRef sink, const NodeError& event) -> void {
  if (sink.node_error) {
    sink.node_error(sink.self, event);
  }
}

/// Emit a QueueDelay event when a callback is installed.
inline auto emit(TraceSinkRef sink, const QueueDelay& event) -> void {
  if (sink.queue_delay) {
    sink.queue_delay(sink.self, event);
  }
}

}  // namespace trace
}  // namespace sr::engine
