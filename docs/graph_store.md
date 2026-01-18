# Graph Store (Versioned Plan Snapshots)

## Goal
Support multiple DSL graphs compiled into immutable plan snapshots, with versioned updates and safe hot‑swap. Reads should be fast and lock‑free on the active plan; updates must not interrupt in‑flight runs.

## Core Objects
- `GraphKey { name, version }` where `version` is a Semantic Version (e.g., 1.0.1).
- `PlanSnapshot { GraphKey key, ExecPlan plan, hash, metadata }`
- `GraphStore` maps graph name → versioned snapshots and tracks an active version.

## Semantics
- **Immutable snapshots**: once published, a snapshot never mutates.
- **Versioned updates**: `publish(name, version)` flips the active snapshot.
- **Safe rollback**: publishing an older version is allowed if configured.
- **Conflict rules**: same `name+version` with different hash is rejected unless forced.
- **Semantic Versioning**: version strings are parsed as SemVer (major.minor.patch) and sorted accordingly (1.2 < 1.10).

## Concurrency Model
- Active snapshot is stored with atomic load/store on `shared_ptr`.
- Lookups acquire a shared lock only to find the graph entry; plan access is lock‑free.
- In‑flight runs hold a `shared_ptr`, so old snapshots stay alive after updates.

## Identity & Validation
- Snapshot hash is computed from canonicalized graph structure (name, version, nodes, bindings, outputs, params).
- Optional registry fingerprint can be recorded for debugging.
- No‑op update: same version + same hash returns existing snapshot.

## Lifecycle Policy
- Configurable `max_versions` per graph.
- Evict non‑active versions on overflow (lowest version first by default).
- Explicit `evict(name, version)` for manual GC.

## Usage Flow
```
stage(graph) -> snapshot
publish(name, version)
resolve(name[, version]) -> snapshot
executor.run(snapshot->plan, ctx)
```

## Runtime Integration
Use `Runtime` when you want to stage DSL and run the active graph in one place.
```
sr::engine::Runtime runtime;
sr::kernel::register_sample_kernels(runtime.registry());

runtime.stage_dsl(dsl_v1, {.publish = true});
runtime.run("demo", ctx);

runtime.stage_dsl(dsl_v2, {.publish = true});
runtime.run("demo", ctx); // new active version
```

## Thread Safety
- `GraphStore` supports concurrent `stage/publish/resolve`; active snapshots are lock-free to read.
- `KernelRegistry` lookups are safe while registering, but new registrations do not affect existing snapshots.
- In-flight runs pin a snapshot, so hot-swaps never mutate a running plan.

## Guarantees
- Reads are consistent per run (snapshot pinned by shared_ptr).
- Updates never affect in‑flight runs.
- Last‑writer‑wins on `publish` when multiple updates race.
