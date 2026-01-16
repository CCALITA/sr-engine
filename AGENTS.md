# Repository Guidelines

## Architecture Overview
- The engine loads a JSON DSL, parses it into a `GraphDef`, resolves bindings/types, and compiles to an `ExecPlan`. The runtime executes the plan in topological order. Kernels are registered via `KernelRegistry`; signatures are inferred from callable argument/return types and port names come from the DSL. `Runtime` wraps `KernelRegistry`, `GraphStore`, and `Executor` to support hot-swapped DSL flows (see `docs/runtime.md`). `RequestContext` carries a `TraceContext` that can emit per-run and per-node events.


## Build, Test, and Development Commands
- `cmake -S . -B build`: configure the CMake build.
- `cmake --build build -j 4`: build the library and example binary.
- `./build/sr_engine_example`: run the sample DAG execution.
- `scripts/build_and_test.sh`: reserved for future automation.

## Coding Style & Naming Conventions
- DO NOT THINK OF MINIMAL CHANGE, ALWAYS implement PERFECT and FANCY as possible.
- prefer C++20 feature, prefer functional programming style.
- no need of backwards compatibility, free to refact and change existing API.
- standard comments on implemented function and class.

## Testing Guidelines
There is no test framework wired yet. Add tests under a new `tests/` directory using `*_test.cpp` naming, and register them in `CMakeLists.txt`. Focus on DSL parsing errors, compile-time validation, kernel execution, and output propagation.

## Tracing
- Tracing is optional and lives in `RequestContext::trace`. When enabled, the runtime emits `RunStart/RunEnd`, `NodeStart/NodeEnd`, `NodeError`, and `QueueDelay` events from worker threads.
- Implement a sink with `on_run_start/on_node_start/...` and wrap it via `trace::make_sink` (see `src/engine/trace.hpp`). Sinks must be thread-safe; `string_view` fields are only valid during callbacks.
- Define `SR_TRACE_DISABLED` to compile out tracing paths entirely.

## Commit & Pull Request Guidelines
Use short, imperative commit subjects (e.g., “Add plan validation”). For PRs, include a concise description, the commands run, and any user-visible behavior changes (CLI output or API changes).
