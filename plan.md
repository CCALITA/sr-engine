# Queue-based Worker Dispatch Plan

## Goals
- Replace per-node `schedule + start_detached` with long-lived worker loops to cut dispatch overhead.
- Preserve completion-driven semantics: schedule a node when its pending count hits zero.
- Keep compute/IO separation and avoid thundering herd wakeups.
- Maintain existing cancellation/deadline/error behavior.

## Design Overview
### Work Queues
- One `WorkQueue` per pool (compute/io): `std::deque<WorkItem>` + `std::mutex` + `std::condition_variable`.
- `push(item)` enqueues under lock, then `notify_one` to wake a single worker.
- `pop()` waits until `stopped || !items.empty()`, then pops under lock.
- `stop` flips a flag under lock and `notify_all` on shutdown (rare path).

### Worker Loop
- Each worker blocks on its queue and exits when `stop` is set.
- Before executing, check `aborted`/RequestContext state; if aborted, skip work and only finalize.
- Execute node, then for each dependent `pending.fetch_sub(1)`; when it hits zero, enqueue to the correct queue.
- Use `std::atomic_ref<int>` on `std::vector<int>` for `pending` and a `scheduled` guard to avoid duplicate dispatch.

### Completion + Shutdown
- `remaining` is an `std::atomic<int>`; main thread waits via `remaining.wait()` until it reaches zero.
- Workers are persistent across runs; shutdown happens in `Executor::~Executor`.
- On shutdown, set `stop` on both queues and `notify_all` to drain workers.
- Track `workers_alive` with `atomic_ref` + `wait()` to ensure worker loops drain before returning.

## Plan
1) Add `WorkQueue` and `WorkerGroup` helpers in `src/runtime/executor.cpp`, scoped to the executor runtime.
2) Replace `schedule_node/start_detached` with queue-based dispatch, remove `scheduled` bookkeeping, and reuse existing `pending` logic.
3) Extend tests to stress queue dispatch: multi-branch DAG, mixed compute/io, and repeated runs for race safety.

## Optional Follow-up: Scheduler Encapsulation
- Introduce a custom scheduler that enqueues into the dispatcher work queues.
- `schedule()` would produce a sender that enqueues a `WorkItem` and completes via `set_value` on the worker thread.
- Benefits: integration with stdexec pipelines and uniform scheduling APIs.
- Tradeoff: adds per-node op-state allocation/dispatch overhead vs direct queue calls.
