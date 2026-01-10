# Runtime API

`Runtime` wraps `KernelRegistry`, `GraphStore`, and `Executor` to provide end-to-end DSL hot-swap.

## Typical Flow
```
sr::engine::Runtime runtime;
sr::kernel::register_sample_kernels(runtime.registry());

runtime.stage_dsl(dsl_v1, {.publish = true});
runtime.run("demo", ctx);

runtime.stage_dsl(dsl_v2, {.publish = true});
runtime.run("demo", ctx);
```

## CLI Demo
Build and run the hot-swap example:
```
cmake --build build -j 4
./build/sr_engine_hot_swap path/to/graph.json --poll-ms=200 --runs=10 --env x=7
```
On Linux, the demo uses stdexec's `io_uring_context` to drive timer-based polling; on other platforms it falls back to sleep-based polling.

## Staging Helpers
- `stage_dsl(text)` parses and stages a JSON DSL string.
- `stage_json(json)` stages a parsed JSON document.
- `stage_file(path)` reads a DSL file and stages it.
- `stage_graph(graph)` stages a `GraphDef` directly.

## Background Graph Daemon
Set `RuntimeConfig.graph_root` to enable a polling daemon that scans a directory for DSL files and keeps snapshots up to date.
It stages any `graph_extension` matches (default `.json`) and publishes by default.
If staging fails (missing kernels or invalid DSL), it retries on the next poll interval.

Example:
```
sr::engine::RuntimeConfig config;
config.graph_root = "graphs/";
config.graph_poll_interval = std::chrono::seconds(60);
config.graph_extension = ".json";
config.graph_recursive = false;

config.graph_allow_replace = true;
sr::engine::Runtime runtime(config);
```

Notes:
- Deleting a file stops tracking it but does not evict existing snapshots.
- Keep the poll interval conservative for small directories (30–60s is typical).
- Register kernels early; if compilation fails, the daemon retries each interval.
- Kernel callables must be `noexcept`; use `Expected` for recoverable errors.
- `RequestContext.env` is snapshotted per run; kernels cannot mutate env during execution.
- All node inputs must be bound in the DSL; missing inputs are compile errors.

## Serve Layer (Multi-Transport)

Use `ServeHost` to run one or more serve endpoints (gRPC unary or Arrow Flight)
that execute a graph per request:

```
sr::engine::Runtime runtime;
sr::kernel::register_sample_kernels(runtime.registry());
sr::kernel::register_rpc_kernels(runtime.registry());

sr::engine::ServeEndpointConfig endpoint;
endpoint.graph_name = "rpc_graph";
sr::engine::GrpcServeConfig grpc;
grpc.address = "0.0.0.0:50051";
endpoint.transport = grpc;

auto host = runtime.serve(endpoint);
```

To run multiple endpoints:

```
sr::engine::ServeLayerConfig layer;
layer.endpoints.push_back(endpoint);
layer.endpoints.push_back(another_endpoint);

auto host = runtime.serve(layer);
```

Arrow Flight endpoints require `SR_ENGINE_ENABLE_ARROW_FLIGHT=ON` and a Flight
config:

```
sr::engine::ServeEndpointConfig flight_endpoint;
flight_endpoint.graph_name = "flight_graph";
sr::engine::FlightServeConfig flight;
flight.location = "grpc+tcp://0.0.0.0:8815";
flight_endpoint.transport = flight;

sr::kernel::register_flight_kernels(runtime.registry());
auto host = runtime.serve(flight_endpoint);
```

See `docs/serve_layer.md` for request env keys, transport details, backpressure,
and lifecycle details.

## Thread Safety Contract
- `GraphStore` is safe to call from multiple threads.
- `KernelRegistry` lookups are safe during registration, but snapshots are immutable once compiled.
- Hot-swap is completion-driven: publish a new version and future `run(name)` calls pick it up.
