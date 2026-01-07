# RPC Kernels (gRPC Generic API)

This module provides core RPC nodes built on `grpc::GenericStub` and
`grpc::ByteBuffer`. It is optional and compiles only when gRPC is available.

## Build

Enable in CMake (default ON):

```
cmake -S . -B build -DSR_ENGINE_ENABLE_GRPC=ON
```

If gRPC is not found, the kernels are disabled and `SR_ENGINE_WITH_GRPC` is set
to 0.

## Types

`sr::kernel::rpc::RpcServerCall` carries per-request data injected via
`RequestContext.env`:

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

Bind the `call` input from `$req.call`.

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
