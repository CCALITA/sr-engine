# Optimization Plan for Serve Layer

This document outlines a plan to optimize the threading model of the `sr-engine` serve layer, specifically targeting `src/runtime/serve/serve.cpp` and `src/runtime/serve/grpc.cpp`.

## Current State

1.  **`GrpcServer::worker_loop`**:
    -   Uses blocking `grpc::ServerCompletionQueue::Next()`.
    -   Requires one dedicated thread per completion queue (`io_threads`).
    -   This is standard gRPC C++ async API usage, but consumes threads just for polling.

2.  **`ServeEndpoint::dispatch_loop`**:
    -   Used when `queue_capacity > 0`.
    -   Uses blocking `RequestQueue::pop()` (std::mutex + std::condition_variable).
    -   Requires dedicated `dispatch_threads`.
    -   Blocks threads waiting for requests, and then blocks waiting for `inflight_sem` permit.

## Optimization Opportunities

We can leverage C++20 coroutines and `stdexec` (specifically `exec::task`) to reduce thread blocking and context switching.

### 1. Remove `dispatch_threads` (High Impact, Low Effort)

The `ServeEndpoint` intermediate queue and dispatch threads can be replaced by an asynchronous flow.

**Proposal:**
-   If `queue_capacity > 0`, instead of blocking threads, use an asynchronous queue (e.g., implemented via `stdexec` senders or by spawning tasks that await availability).
-   If backpressure/queueing is not strictly required at the *endpoint* level (relying instead on the global thread pool queue), we can remove `RequestQueue` entirely.
-   If queueing is required (to limit active requests while holding pending ones):
    -   Implement an `AsyncSemaphore` using `stdexec` (a sender that completes when a permit is acquired).
    -   The `enqueue` operation becomes a non-blocking submission that chains: `acquire_permit() | then(execute_request)`.
    -   This removes the need for `dispatch_threads`.

### 2. Optimize `GrpcServer` (High Impact, High Effort)

To remove the blocking `worker_loop`, we need to integrate gRPC with `stdexec` or `asio`.

**Proposal:**
-   Utilize `thirdparty/asio-grpc` (which is present in the codebase).
-   `asio-grpc` allows running gRPC completion queues on `asio::io_context`.
-   We can then run the `io_context` on the runtime's `static_thread_pool` (if compatible) or a minimal poller thread.
-   More importantly, it allows writing request handlers as coroutines:
    ```cpp
    exec::task<void> handle_rpc(agrpc::GrpcContext& grpc_context) {
        // ...
        co_await agrpc::request(..., grpc_context);
        // ...
        co_await agrpc::finish(..., grpc_context);
    }
    ```
-   This eliminates manual tag management (`GrpcCall` state machine) and blocking loops.

## Implementation Steps

1.  **Refactor `ServeEndpoint`**:
    -   Remove `RequestQueue` and `dispatch_threads`.
    -   Implement an async flow control mechanism (Async Semaphore) if `max_inflight` limiting with queueing is required.
    -   If `queue_capacity` behavior (reject if full) is sufficient, `try_acquire` is enough, and we can drop the separate dispatch threads immediately.

2.  **Refactor `GrpcServer`**:
    -   Replace `grpc::ServerCompletionQueue` loop with `agrpc::GrpcContext` (from `asio-grpc`).
    -   Rewrite `GrpcCall` to use `exec::task` and `co_await`.

## Conclusion

By adopting `exec::task` and potentially `asio-grpc`, we can transition `sr-engine` to a fully asynchronous, coroutine-based architecture, minimizing thread usage and blocking operations.
