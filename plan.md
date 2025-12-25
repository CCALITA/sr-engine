# Dataflow Scheduling Plan

## Goals
- Add a dataflow execution path that maximizes intra-DAG concurrency while preserving dependency correctness.
- Keep the existing sequential `Executor::run` intact and add an opt-in dataflow API.
- Add stress tests to validate correctness under concurrent execution.

## Steps
1. **Dataflow API surface**
   - Add `Executor::run_dataflow(...)` with a configurable thread count.
   - Keep current `Executor::run(...)` as sequential baseline.

2. **Dependency analysis & scheduling**
   - Build a slot->producer map and node dependency lists from the compiled plan.
   - Maintain per-node pending dependency counters and a dependents list.
   - Use a thread pool to schedule ready nodes; decrement counters and enqueue dependents when they become ready.

3. **Execution safety**
   - Allocate per-run slot storage as today; ensure each node writes only its outputs.
   - Use atomic counters + condition variable to wait for completion.
   - Collect the first runtime error (if any) and return it after all nodes finish.

4. **Tests & validation**
   - Add a dataflow test path that runs a branching DAG with shared producers.
   - Extend multithreaded stress to cover dataflow execution and repeated iterations.
