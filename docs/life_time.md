# Lifetime and Ownership

This document summarizes which objects are shared across runs versus per-run, and the concurrency implications.

## Shared Across Runs
- `ExecPlan`: nodes, topology, bindings, dependents, pending counts, slot specs, outputs, and const values.
- `KernelHandle` per node, including `kernel.compute` and `kernel.instance` (`std::shared_ptr<void>`).
- `Executor` resources: thread pool and scheduler.

These objects are immutable once compiled. Kernel instances are shared across runs, so they must be thread-safe or immutable.

## Per Run (Executor::run)
- `RunStates`: per-run slots, env snapshot boxes, input pointer table, pending counts, and trace state.
- `InputValues` / `OutputValues`: lightweight views created per node execution.
- `ExecResult`: map of graph outputs to `ValueBox` instances for the run.

Env values are read from `RequestContext.env` at run start and snapshotted into per-run `ValueBox` storage. Kernels do not mutate env during execution.

## Concurrency Implications
- Shared kernel instances must not keep per-run mutable state unless they synchronize internally.
- Per-run data (`RunStates`, slot boxes, env snapshot) is isolated and safe to use concurrently between runs.
- If you need per-run kernel state, switch to a factory that creates a fresh instance per execution.
