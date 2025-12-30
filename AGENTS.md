# Repository Guidelines

## Project Structure & Module Organization
- `src/engine/`: JSON DSL parsing, graph validation, plan compilation, and kernel registry/adapters.
- `src/runtime/`: execution runtime (`Executor`) and request-context handling.
- `src/kernel/`: built-in/sample kernels plus type registration.
- `src/reflection/`: vendored headers (ENTT, JSON, expected).
- `examples/`: runnable demos (e.g., `examples/simple.cpp`).
- `thirdparty/`: external dependencies (stdexec).
- `scripts/`: helper scripts (currently minimal).
- `build/`: generated build output (do not commit).

## Architecture Overview
- The engine loads a JSON DSL, parses it into a `GraphDef`, resolves bindings/types, and compiles to an `ExecPlan`. The runtime executes the plan in topological order. Kernels are registered via `KernelRegistry`; signatures are inferred from callable argument/return types and port names come from the DSL.


## Build, Test, and Development Commands
- `cmake -S . -B build`: configure the CMake build.
- `cmake --build build -j 4`: build the library and example binary.
- `./build/sr_engine_example`: run the sample DAG execution.
- `scripts/build_and_test.sh`: reserved for future automation.

## Coding Style & Naming Conventions
- DO NOT THINK OF MINIMAL CHANGE, ALWAYS implement PERFECT and FANCY as possible.
- C++20 codebase; keep headers in `.hpp` and sources in `.cpp`.
- Use 2-space indentation and same-line braces to match existing files.
- be aggressive to refact and optimize existing API.
- make detailed comments on every function and class.
- Order includes as: standard library, third-party, then project headers.

## Testing Guidelines
There is no test framework wired yet. Add tests under a new `tests/` directory using `*_test.cpp` naming, and register them in `CMakeLists.txt`. Focus on DSL parsing errors, compile-time validation, kernel execution, and output propagation.

## Commit & Pull Request Guidelines
No git history is available in this repository. Use short, imperative commit subjects (e.g., “Add plan validation”). For PRs, include a concise description, the commands run, and any user-visible behavior changes (CLI output or API changes).
