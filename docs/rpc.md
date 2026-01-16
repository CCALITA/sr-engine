┌──────────────────────────────────────────────────────────────────┐
│                     Graph DSL Engine                              │
│  (nodes = kernels, bindings = edges, runtime = executor)          │
├──────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Kernel Interface (what all kernels implement):                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  auto operator()(Input) -> Expected<Output>             │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                 │
├──────────────────────────────────────────────────────────────────┤
│                     RPC Plugin Layer                              │
│                                                                  │
│  ┌─────────────────┐   ┌─────────────────┐   ┌───────────────┐  │
│  │  Transport I/F  │   │ Serializer I/F  │   │Middleware I/F │  │
│  │  (send, batch)  │   │ (serialize,     │   │ (before/after │  │
│  │                 │   │  deserialize)   │   │  send)        │  │
│  └────────┬────────┘   └────────┬────────┘   └───────┬───────┘  │
│           │                     │                    │          │
│  ┌────────▼────────┐   ┌────────▼────────┐   ┌───────▼───────┐  │
│  │ GrpcTransport   │   │ ProtobufSer     │   │ RetryMw       │  │
│  │ HttpJsonTrans   │   │ FlatBufferSer   │   │ CircuitBreaker│  │
│  │ WebSocketTrans  │   │ JsonSer         │   │ MetricsMw     │  │
│  │ InMemoryTrans   │   │ CBORSer         │   │ CacheMw       │  │
│  └─────────────────┘   └─────────────────┘   └───────────────┘  │
│                                                                 │
├──────────────────────────────────────────────────────────────────┤
│                     Composable Kernels                            │
│                                                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  GenericRpcKernel - adapts to transport/serializer config │  │
│  ├───────────────────────────────────────────────────────────┤  │
│  │  ScatteringKernel - splits input to multiple targets       │  │
│  │  GatheringKernel  - collects multiple outputs              │  │
│  │  ParallelBatchKernel - concurrent RPC execution            │  │
│  │  RetryKernel      - wraps any kernel with retry logic      │  │
│  │  CacheKernel      - wraps any kernel with caching          │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
└──────────────────────────────────────────────────────────────────┘