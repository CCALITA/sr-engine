# Improvement Plan for SR-Engine

Following the recent threading model optimizations (unified thread pool, non-blocking dispatch), the following areas are identified for further improvement.

## 1. Asynchronous gRPC Integration (High Priority)

**Goal:** Eliminate blocking threads in the gRPC transport layer (`GrpcServer`).

**Current State:**
- `GrpcServer` uses a blocking `worker_loop` that calls `cq_->Next()`.
- Requires one dedicated thread per Completion Queue.
- Handlers use a callback/tag state machine (`GrpcCall`).

**Proposal:**
- Integrate `thirdparty/asio-grpc` (agrpc).
- Drive gRPC `CompletionQueue`s using `asio::io_context` or directly via `stdexec` schedulers if supported.
- Rewrite request handlers as C++20 coroutines (`exec::task`) that `co_await` gRPC events (read, write, finish).
- **Benefit:** Removes the last major source of thread blocking, allowing the server to scale with the thread pool rather than `io_threads` config.

## 2. Enhanced Observability

**Goal:** Improve visibility into the async execution engine.

**Proposal:**
- **Full Distributed Tracing:** Update `TraceContext` and `RunStart` events to properly support and record `parent_span_id` extracted from W3C headers, enabling full trace reconstruction.
- **Metrics:** Add counters and gauges for:
    - `serve_pool` and `compute_pool` utilization.
    - `pending_requests_` depth and wait times.
    - Graph execution latency histograms.
- **Exporters:** Implement a concrete `TraceSink` that exports to OpenTelemetry/Jaeger (leveraging `api_c.h` structs or a client library).

## 3. Advanced Flow Control

**Goal:** Dynamic overload protection.

**Proposal:**
- Implement **Adaptive Concurrency Control** (e.g., Gradient algorithm) to adjust `max_inflight` dynamically based on observed latency and error rates.
- Add **Request Prioritization** to the `pending_requests_` queue (e.g., priority queue based on deadlines or metadata).

## 4. Kernel Library Expansion

**Goal:** Broaden the engine's capability.

**Proposal:**
- Add standard kernels for:
    - **HTTP Client:** Non-blocking HTTP calls.
    - **Key-Value Store:** Async get/put.
    - **Tensor Operations:** Basic math or integration with a tensor library.

## 5. Modernize Scheduler Composition

**Goal:** Simplify `ServeEndpoint` internals.

**Proposal:**
- Refactor the manual `pending_requests_` + `try_schedule` pump in `ServeEndpoint` using `stdexec`'s high-level composition primitives (e.g., `sequence_senders` or custom `sender` adaptors) to model the "acquire permit -> execute -> release" flow declaratively.
