# RPC Kernels (gRPC Generic API)

This module provides core RPC nodes built on `grpc::GenericStub` and
`grpc::ByteBuffer`. It is required by the serve layer and assumes gRPC is
available at build time.

## Build

gRPC is required by default:

```
cmake -S . -B build
```

## Types

`sr::kernel::rpc::RpcServerCall` carries per-request data injected via
`RequestContext.env` (see `docs/serve_layer.md`):

- `method`: full RPC method string.
- `request`: `grpc::ByteBuffer` with the request payload.
- `metadata`: request metadata entries.
- `responder`: user-supplied `RpcResponder` to send responses.

## Kernels

### `rpc_server_input`

Signature:

- inputs: `RpcServerCall`
- outputs: `std::string` (method), `grpc::ByteBuffer` (payload),
  `RpcMetadata` (metadata)

Example DSL:

```
{ "id": "in", "kernel": "rpc_server_input",
  "inputs": ["call"], "outputs": ["method", "payload", "meta"] }
```

Bind the `call` input from `$req.rpc.call`.

### `rpc_server_output`

Signature:

- inputs: `RpcServerCall`, `grpc::ByteBuffer`
- outputs: none

Params:

- `status_code` (int, default 0)
- `status_message` (string, default "")
- `status_details` (string, default "")

Example DSL:

```
{ "id": "out", "kernel": "rpc_server_output",
  "params": { "status_code": 0 },
  "inputs": ["call", "payload"], "outputs": [] }
```

### `flatbuffer_echo`

Signature:

- inputs: `grpc::ByteBuffer`
- outputs: `grpc::ByteBuffer`

Performs a deep copy round-trip to emulate a codec boundary.

### `flatbuffer_echo_batch`

Signature:

- inputs: `std::vector<grpc::ByteBuffer>`
- outputs: `std::vector<grpc::ByteBuffer>`

Batched echo variant for scatter/gather pipelines.

### `scatter_i64`

Signature:

- inputs: `int64`
- outputs: `std::vector<grpc::ByteBuffer>`

Params:

- `parts` (int, default 2)

Splits a value across `parts` and encodes each shard as a ByteBuffer with part
index/count metadata.

Example DSL:

```
{ "id": "scatter", "kernel": "scatter_i64",
  "params": { "parts": 4 },
  "inputs": ["value"], "outputs": ["payloads"] }
```

### `gather_i64_sum`

Signature:

- inputs: `std::vector<grpc::ByteBuffer>`
- outputs: `int64`

Decodes shard metadata, validates indices/count, and sums shard values.

### `rpc_unary_call`

Signature:

- inputs: `grpc::ByteBuffer`
- outputs: `grpc::ByteBuffer`

Params:

- `target` (string, required)
- `method` (string, required)
- `timeout_ms` (int, optional)
- `wait_for_ready` (bool, optional)
- `authority` (string, optional)
- `metadata` (object or array, optional)

Non-OK gRPC status is surfaced as an engine error.

Note: `rpc_unary_call` currently uses `grpc::InsecureChannelCredentials`.

### `rpc_unary_call_batch`

Signature:

- inputs: `std::vector<grpc::ByteBuffer>`
- outputs: `std::vector<grpc::ByteBuffer>`

Params: same as `rpc_unary_call`.

Calls the remote unary method once per payload and returns responses in order.

See `docs/rpc_scatter_gather.md` for the scatter/gather pipeline and
real-world test notes.
