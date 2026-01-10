# Serve Layer (gRPC Unary)

This document describes the production-grade serve layer that hosts a gRPC
unary server and routes each request into the DAG runtime. The serve layer is
transport-focused (accept, backpressure, deadlines) and keeps per-request
execution as a normal `ExecPlan` run.

## Goals

- Unary gRPC only (streaming is out of scope for v1).
- Method routing happens inside the graph (no external per-method routing).
- No TLS/authn in the serve layer for now.
- High-performance, low-latency, built on C++20 and stdexec sender/receiver.
- Hot-swap friendly: requests run against immutable plan snapshots.

## Architecture

**ServeHost**
- Owns the gRPC server, request queue, inflight limiter, and request scheduler.
- Resolves the active graph version per request (or uses a pinned version).
- Builds a `RequestContext` and executes `Executor::run` on a request pool.
- The owning `Runtime` must outlive the host.

**gRPC Transport Adapter**
- Accepts unary calls via `grpc::AsyncGenericService`.
- Converts client metadata + payload into a `RequestEnvelope`.
- Provides a `RpcResponder` implementation that writes the response.

**Request Pipeline (sender/receiver)**
- `accept → enqueue → schedule(request_pool) → run ExecPlan → respond`.
- Cancellation and deadlines propagate into `RequestContext`.
- Bounded queue + max inflight limit for backpressure.

## Request Lifecycle

1. gRPC CQ thread accepts a call and reads the request payload.
2. The serve layer enqueues the request (or rejects when full).
3. A dispatcher acquires an inflight permit and schedules the request.
4. A new `RequestContext` is built (env, deadline, trace).
5. `Executor::run` executes the graph once for the request.
6. The graph responds via `rpc_server_output` (or an error fallback).

## Request Env Keys

The serve layer injects the following env entries when the graph binds them
(only `rpc.*` keys are supported):

- `rpc.call`: `sr::kernel::rpc::RpcServerCall`
- `rpc.method`: `std::string`
- `rpc.payload`: `grpc::ByteBuffer`
- `rpc.metadata`: `sr::kernel::rpc::RpcMetadata`
- `rpc.peer`: `std::string`
- `rpc.deadline_ms`: `int64_t` (milliseconds remaining, `-1` if unset)

Bindings to other `$req.*` keys are not supported by the serve layer.

Graphs should bind inputs from `$req.<key>`. Example:

```
{ "id": "in", "kernel": "rpc_server_input",
  "inputs": ["call"], "outputs": ["method", "payload", "meta"] }
```

Bind `call` from `$req.rpc.call`.

## Response Path

Use `rpc_server_output` to send responses. The serve layer verifies that a
response was sent; otherwise it replies with an error status.

## Backpressure

- `queue_capacity`: bounded request queue depth; overflow returns
  `RESOURCE_EXHAUSTED`.
- `max_inflight`: hard limit on in-flight requests.
  Set `queue_capacity` to `0` to disable queuing.

## Cancellation & Deadlines

- gRPC cancellation triggers `RequestContext::cancel()`.
- `RequestContext::deadline` mirrors the gRPC deadline or a default timeout.
- The executor checks cancellation/deadline before running.

## Hot-Swap

- Requests resolve plan snapshots at submission time.
- When `graph_version` is set, serving is pinned to that version.
- When unset, the active version is used, enabling live swaps.

## Observability

- Per-request tracing is supported via `RequestContext.trace`.
- Serve-level stats expose accepted/rejected/completed counters and inflight
  depth (see `ServeHost::stats()`).

## Example DSL Sketch

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
