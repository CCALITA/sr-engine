# Tracing Guide

## Overview
Tracing is an optional, per-request feature that emits run and node events from the runtime. It is designed for C++20, zero allocations in hot paths, and no backend dependency.

When enabled, `RequestContext::trace` drives event emission from worker threads. Tracing is off by default; define `SR_TRACE_DISABLED` to compile out trace paths entirely.

## Quick Start
```cpp
#include "engine/trace.hpp"

struct MySink {
  void on_run_start(const sr::engine::trace::RunStart& e) {}
  void on_node_start(const sr::engine::trace::NodeStart& e) {}
  void on_node_end(const sr::engine::trace::NodeEnd& e) {}
};

sr::engine::RequestContext ctx;
MySink sink;
ctx.trace.sink = sr::engine::trace::make_sink(sink);
ctx.trace.flags = sr::engine::trace::to_flags(sr::engine::trace::TraceFlag::RunSpan) |
                  sr::engine::trace::to_flags(sr::engine::trace::TraceFlag::NodeSpan);
ctx.trace.trace_id = 1234; // optional
```

## Event Model
Events are POD with `std::string_view` fields that are valid only during callback execution:
- `RunStart/RunEnd`: per execution.
- `NodeStart/NodeEnd`: per node execution.
- `NodeError`: optional error detail.
- `QueueDelay`: enqueue time vs start time.

## Concurrency and Lifetime Rules
- Callbacks execute on worker threads. Sinks must be thread-safe.
- Sinks must outlive the `Executor::run` call. The engine stores only a raw pointer.
- Keep callbacks non-blocking. Heavy I/O or coarse locks will stall workers.
- If `QueueDelay` is enabled without `NodeSpan`, `span_id` is zero; sinks should handle that.

## Performance Notes
- Each node span allocates a span id via an atomic counter. For extreme throughput, consider sampling or batching in the sink.
- Use `TraceSampler` to decide flags once per run and avoid per-node decisions.
