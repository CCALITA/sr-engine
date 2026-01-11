# Serve Layer (Multi-Transport)

This document describes the production-grade serve layer that hosts one or more
server endpoints (gRPC unary or Arrow Flight) and routes each request into the
DAG runtime. The serve layer is transport-focused (accept, backpressure,
deadlines) and keeps per-request execution as a normal `ExecPlan` run.

## Goals

- Unary gRPC, Unix IPC, and Arrow Flight (DoAction/DoGet/DoPut/DoExchange).
- Method routing happens inside the graph (no external per-method routing).
- No TLS/authn in the serve layer for now.
- High-performance, low-latency, built on C++20 and stdexec sender/receiver.
- Hot-swap friendly: requests run against immutable plan snapshots.

## Architecture

**ServeHost**
- Owns one or more endpoints with dedicated transport adapters.
- Resolves the active graph version per request (or uses a pinned version).
- Builds a `RequestContext` and executes `Executor::run` on a request pool.
- The owning `Runtime` must outlive the host.

**gRPC Transport Adapter**
- Accepts unary calls via `grpc::AsyncGenericService`.
- Converts client metadata + payload into a request envelope.
- Provides a `RpcResponder` implementation that writes the response.

**IPC Transport Adapter**
- Accepts unary calls over a Unix domain socket.
- Converts the IPC message into a request envelope.
- Provides a `RpcResponder` implementation that writes the response.

**Arrow Flight Transport Adapter**
- Accepts Flight calls via `arrow::flight::FlightServerBase`.
- Converts Flight call state into a request envelope.
- Provides a `FlightResponder` implementation for `DoAction`/`DoGet`.

## Implementation Layout

- `src/runtime/serve/serve.cpp`: endpoint template, host wiring, transport traits.
- `src/runtime/serve/common.hpp`: shared queue/env helpers and response helpers.
- `src/runtime/serve/rpc_env.cpp` + `src/runtime/serve/rpc_env.hpp`: rpc env bindings.
- `src/runtime/serve/grpc.cpp` + `src/runtime/serve/grpc.hpp`: gRPC transport adapter.
- `src/runtime/serve/ipc.cpp` + `src/runtime/serve/ipc.hpp`: IPC transport adapter.
- `src/runtime/serve/flight.cpp` + `src/runtime/serve/flight.hpp`: Flight transport adapter.
- `src/runtime/serve/serve.hpp`: public serve API (included by `src/runtime/serve.hpp`).

**Request Pipeline (sender/receiver)**
- `accept → enqueue → schedule(request_pool) → run ExecPlan → respond`.
- Cancellation and deadlines propagate into `RequestContext`.
- Bounded queue + max inflight limit for backpressure.

## Request Lifecycle

1. Transport thread accepts a call and builds an envelope.
2. The serve layer enqueues the request (or rejects when full).
3. A dispatcher acquires an inflight permit and schedules the request.
4. A new `RequestContext` is built (env, deadline, trace).
5. `Executor::run` executes the graph once for the request.
6. The graph responds via a transport-specific output kernel.

## Request Env Keys

The serve layer injects env entries when the graph binds them. Only `rpc.*` and
`flight.*` keys are supported. IPC uses the same `rpc.*` keys as gRPC.

## Graph Selection Metadata

Each request must include the graph name metadata header (default
`sr-graph-name`). Optionally include `sr-graph-version` to target a specific
version. These keys are configurable via `ServeEndpointConfig::graph.metadata`.

### gRPC keys

- `rpc.call`: `sr::kernel::rpc::RpcServerCall`
- `rpc.method`: `std::string`
- `rpc.payload`: `grpc::ByteBuffer`
- `rpc.metadata`: `sr::kernel::rpc::RpcMetadata`
- `rpc.peer`: `std::string`
- `rpc.deadline_ms`: `int64_t` (milliseconds remaining, `-1` if unset)

### Flight keys

- `flight.call`: `sr::kernel::flight::FlightServerCall`
- `flight.kind`: `sr::kernel::flight::FlightCallKind`
- `flight.action`: `arrow::flight::Action` (DoAction only)
- `flight.ticket`: `arrow::flight::Ticket` (DoGet only)
- `flight.descriptor`: `arrow::flight::FlightDescriptor` (when available)
- `flight.reader`: `std::shared_ptr<arrow::flight::FlightMessageReader>`
- `flight.writer`: `std::shared_ptr<arrow::flight::FlightMessageWriter>`
- `flight.metadata_writer`: `std::shared_ptr<arrow::flight::FlightMetadataWriter>`
- `flight.peer`: `std::string`
- `flight.deadline_ms`: `int64_t` (milliseconds remaining, `-1` if unset)

Arrow Flight support requires building with `SR_ENGINE_ENABLE_ARROW_FLIGHT=ON`.

If a bound Flight key is missing for the current call kind, the serve layer
returns a `FAILED_PRECONDITION` error.

Graphs should bind inputs from `$req.<key>`. Example:

```
{ "id": "in", "kernel": "rpc_server_input",
  "inputs": ["call"], "outputs": ["method", "payload", "meta"] }
```

Bind `call` from `$req.rpc.call`.

## Response Path

- **gRPC/IPC:** use `rpc_server_output` to send responses.
- **Flight DoAction:** use `flight_action_output` to send results.
- **Flight DoGet:** use `flight_get_output` to send a `RecordBatchReader`.
- **Flight DoPut/DoExchange:** read/write from `flight.reader`/`flight.writer` and
  optionally emit app metadata through `flight.metadata_writer`.

The serve layer verifies that required responses were sent; otherwise it replies
with an error status.

See `docs/flight_kernels.md` for Flight kernel details.

## IPC Wire Format

IPC requests and responses are length-prefixed with 32-bit big-endian sizes.
All strings are UTF-8 encoded.

**Request**
- `u32`: method length
- `bytes`: method
- `u32`: metadata entry count
- per entry: `u32` key length, `bytes` key, `u32` value length, `bytes` value
- `u32`: payload length
- `bytes`: payload (opaque)

**Response**
- `u32`: status code (`grpc::StatusCode`)
- `u32`: status message length, `bytes`: status message
- `u32`: status details length, `bytes`: status details
- `u32`: trailing metadata entry count
- per entry: `u32` key length, `bytes` key, `u32` value length, `bytes` value
- `u32`: payload length
- `bytes`: payload (opaque)

## Backpressure

- `queue_capacity`: bounded request queue depth; overflow returns
  `RESOURCE_EXHAUSTED`.
- `max_inflight`: hard limit on in-flight requests.
  Set `queue_capacity` to `0` to disable queuing.

## Cancellation & Deadlines

- gRPC cancellation triggers `RequestContext::cancel()`.
- Flight cancellation is best-effort (checked when available).
- `RequestContext::deadline` mirrors transport deadlines or a default timeout.
- The executor checks cancellation/deadline before running.

## Hot-Swap

- Requests resolve plan snapshots at submission time.
- When `graph_version` is set, serving is pinned to that version.
- When unset, the active version is used, enabling live swaps.

## Observability

- Per-request tracing is supported via `RequestContext.trace`.
- Serve-level stats expose per-endpoint accepted/rejected/completed counters and
  inflight depth (see `ServeHost::stats()`).

## Example DSL Sketch (gRPC)

```
{
  "nodes": [
    { "id": "in", "kernel": "rpc_server_input",
      "inputs": ["call"], "outputs": ["method", "payload", "meta"] },
    { "id": "logic", "kernel": "handle_rpc",
      "inputs": ["method", "payload", "meta"], "outputs": ["payload"] },
    { "id": "out", "kernel": "rpc_server_output",
      "inputs": ["call", "payload"], "outputs": [] }
  ],
  "bindings": [
    { "to": "in.call", "from": "$req.rpc.call" },
    { "to": "logic.method", "from": "in.method" },
    { "to": "logic.payload", "from": "in.payload" },
    { "to": "logic.meta", "from": "in.meta" },
    { "to": "out.call", "from": "$req.rpc.call" },
    { "to": "out.payload", "from": "logic.payload" }
  ]
}
```

## Example DSL Sketch (Flight DoAction)

```
{
  "nodes": [
    { "id": "in", "kernel": "flight_server_input",
      "inputs": ["call"], "outputs": ["kind", "action", "ticket", "descriptor", "reader", "writer", "metadata_writer"] },
    { "id": "act", "kernel": "handle_action",
      "inputs": ["action"], "outputs": ["results"] },
    { "id": "out", "kernel": "flight_action_output",
      "inputs": ["call", "results"], "outputs": [] }
  ],
  "bindings": [
    { "to": "in.call", "from": "$req.flight.call" },
    { "to": "act.action", "from": "in.action" },
    { "to": "out.call", "from": "$req.flight.call" },
    { "to": "out.results", "from": "act.results" }
  ]
}
```
