# brpc Transport Design

## Overview

Add brpc as a bidirectional transport layer for sr-engine, enabling both server-side request handling and client-side RPC calls from kernels.

## Requirements

- **Scope:** Both server and client transport
- **Protocol:** baidu_std (brpc native binary protocol)
- **Threading:** bthread internally, bridge to stdexec pool for execution
- **Resilience:** Full (connection pool, retry, circuit breaker, rate limiting)
- **Tracing:** Full integration with RequestContext/TraceContext

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         ServeHost                                │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌──────────────┐  │
│  │GrpcServer  │ │IpcServer   │ │FlightServer│ │ BrpcServer   │  │
│  └─────┬──────┘ └─────┬──────┘ └─────┬──────┘ └──────┬───────┘  │
│        └──────────────┴──────────────┴───────────────┘           │
│                              ▼                                   │
│                  RequestCallback → Executor::run                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                     RPC Kernel Layer                             │
│  ┌──────────────┐ ┌──────────────┐ ┌────────────────────────┐   │
│  │GrpcTransport │ │FlightTransport│ │    BrpcTransport      │   │
│  └──────┬───────┘ └──────┬───────┘ └───────────┬────────────┘   │
│         └────────────────┴─────────────────────┘                 │
│                              ▼                                   │
│              Transport concept → Middleware → Serializer         │
└─────────────────────────────────────────────────────────────────┘
```

## New Files

| File | Purpose |
|------|---------|
| `src/runtime/serve/brpc.hpp` | BrpcServer declaration |
| `src/runtime/serve/brpc.cpp` | BrpcServer implementation |
| `src/kernel/rpc_transport_brpc.hpp` | BrpcTransport declaration |
| `src/kernel/rpc_transport_brpc.cpp` | BrpcTransport implementation |
| `include/sr/brpc_config.hpp` | Configuration structures |
| `tests/brpc_transport_test.cpp` | Client unit tests |
| `tests/brpc_server_test.cpp` | Server unit tests |
| `tests/brpc_integration_test.cpp` | End-to-end tests |
| `tests/brpc_resilience_test.cpp` | Resilience feature tests |

## Component Details

### BrpcServeConfig

```cpp
struct BrpcServeConfig {
  std::string address = "0.0.0.0:8000";
  int io_thread_num = 4;
  int max_concurrency = 0;  // 0 = unlimited
  std::chrono::milliseconds idle_timeout{60000};
  bool enable_ssl = false;
  std::string ssl_cert_path;
  std::string ssl_key_path;
};
```

### BrpcServer

```cpp
class BrpcServer {
public:
  using RequestCallback = std::function<void(BrpcEnvelope&&)>;
  
  explicit BrpcServer(BrpcServeConfig config, RequestCallback cb);
  
  auto start() -> Expected<void>;
  auto shutdown(std::chrono::milliseconds timeout) -> void;
  
private:
  brpc::Server server_;
  std::unique_ptr<BrpcServiceImpl> service_;
};
```

**Request Flow:**
1. brpc receives request on bthread
2. `BrpcServiceImpl::Process()` extracts method/payload into `BrpcEnvelope`
3. Injects trace context from brpc metadata
4. Dispatches via `stdexec::execute(pool_, [cb, env]{ cb(std::move(env)); })`
5. Response written back via `brpc::Controller::response_attachment()`

### BrpcClientConfig

```cpp
struct BrpcClientConfig {
  std::string server_address;
  std::chrono::milliseconds timeout{3000};
  int max_retry = 3;
  std::chrono::milliseconds retry_backoff_base{100};
  size_t connection_pool_size = 8;
  bool enable_circuit_breaker = true;
  double circuit_breaker_error_rate = 0.5;
};
```

### BrpcTransport

```cpp
class BrpcTransport {
public:
  explicit BrpcTransport(BrpcClientConfig config);
  
  // Transport concept implementation
  auto send(const Envelope& request) -> Expected<Envelope>;
  auto send_batch(const std::vector<Envelope>& requests) 
      -> Expected<std::vector<Envelope>>;
  auto connected() const -> bool;
  auto health_check() -> Expected<bool>;
  
private:
  brpc::Channel channel_;
  BrpcClientConfig config_;
  std::atomic<bool> circuit_open_{false};
};
```

## Error Handling

```cpp
enum class BrpcError {
  ConnectionFailed,
  Timeout,
  ServiceUnavailable,
  CircuitOpen,
  SerializationError,
  ServerError,
};
```

## Trace Integration

**Server-side:** Extract trace context from `brpc::Controller::request_attachment()`, propagate to `RequestContext`.

**Client-side:** Inject trace context into outgoing requests, emit `TraceEvent::RpcComplete` on response.

## CMake Integration

```cmake
option(SR_ENGINE_ENABLE_BRPC "Enable brpc transport" OFF)

if(SR_ENGINE_ENABLE_BRPC)
  find_package(brpc REQUIRED)
  target_sources(sr_engine PRIVATE
    src/runtime/serve/brpc.cpp
    src/kernel/rpc_transport_brpc.cpp
  )
  target_link_libraries(sr_engine PRIVATE brpc::brpc)
  target_compile_definitions(sr_engine PUBLIC SR_ENGINE_HAS_BRPC=1)
endif()
```

## DSL Configuration Example

```json
{
  "serve": {
    "transports": [
      { "type": "brpc", "address": "0.0.0.0:8000", "io_threads": 4 }
    ]
  },
  "nodes": [
    {
      "id": "remote_call",
      "kernel": "rpc_call",
      "params": {
        "transport": "brpc",
        "address": "remote-service:8000",
        "method": "compute"
      }
    }
  ]
}
```

## Testing Strategy

1. **Unit tests:** Connection failures, serialization, circuit breaker logic
2. **Integration tests:** Full server-client round-trip
3. **Resilience tests:** Retry behavior, timeout handling, circuit breaker transitions

## Implementation Order

1. CMake setup with brpc dependency
2. BrpcServeConfig and BrpcClientConfig structures
3. BrpcTransport client implementation
4. BrpcServer implementation
5. ServeHost integration (add to variant)
6. TrampolineRegistry integration
7. Unit tests
8. Integration tests
9. Documentation
