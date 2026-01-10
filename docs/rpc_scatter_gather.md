# RPC Scatter/Gather (Real-World Test)

This note documents the scatter/gather RPC pipeline and the live gRPC
integration test added to the engine.

## What Changed

New RPC-aware kernels and a real-world test demonstrate distributed-style
arithmetic:

- `scatter_i64`: split a value into N shards and encode metadata into
  `grpc::ByteBuffer` payloads.
- `gather_i64_sum`: validate shard metadata and sum values back together.
- `rpc_unary_call_batch`: send each shard payload to a real gRPC server and
  collect responses.
- `flatbuffer_echo_batch`: local echo variant for non-network runs.

The test spins up a generic gRPC server (callee) and uses the engine as a
caller to validate round-trip behavior and status reporting.

## Pipeline Sketch

```
scatter_i64 -> rpc_unary_call_batch -> gather_i64_sum
```

Each shard payload carries:

```
uint32 part_index
uint32 part_count
int64  value
```

## Example DSL (Caller)

```
{
  "version": 1,
  "name": "rpc_scatter_gather_remote",
  "nodes": [
    { "id": "scatter", "kernel": "scatter_i64",
      "params": { "parts": 4 }, "inputs": ["value"], "outputs": ["payloads"] },
    { "id": "rpc", "kernel": "rpc_unary_call_batch",
      "params": { "target": "127.0.0.1:PORT",
                  "method": "/sr.engine.Shard/Scale",
                  "timeout_ms": 2000,
                  "wait_for_ready": true },
      "inputs": ["payloads"], "outputs": ["payloads"] },
    { "id": "gather", "kernel": "gather_i64_sum",
      "inputs": ["payloads"], "outputs": ["sum"] }
  ],
  "bindings": [
    { "to": "scatter.value", "from": "$req.x" },
    { "to": "rpc.payloads", "from": "scatter.payloads" },
    { "to": "gather.payloads", "from": "rpc.payloads" }
  ],
  "outputs": [
    { "from": "gather.sum", "as": "out" }
  ]
}
```

The test expects `out == x * multiplier`.

## Real-World Test

`tests/rpc_test.cpp` 

- Starts a gRPC async generic server.
- The server decodes shard payloads and scales each shard value.
- The client pipeline calls the server for each shard and gathers the result.
- Prints a status line: `[rpc] status=OK calls=4 target=...`.

## Insufficient / Limitations

- Shard payload format is custom and binary; it is not FlatBuffers or protobuf.
- `rpc_unary_call_batch` is sequential; no concurrency or pipelining yet.
- The gRPC server is a test-only generic handler; it is not a production
  service implementation or discovery setup.
- `rpc_unary_call(_batch)` uses insecure channel credentials only.
- Error handling is minimal: a single bad shard fails the entire gather.
