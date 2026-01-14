#include <benchmark/benchmark.h>
#include <grpcpp/grpcpp.h>
#include <thread>
#include <vector>
#include <chrono>
#include <atomic>
#include <future>

#include "runtime/runtime.hpp"
#include "kernel/rpc_kernels.hpp"
#include "kernel/sample_kernels.hpp"

namespace {

auto make_byte_buffer(std::string_view data) -> grpc::ByteBuffer {
  grpc::Slice slice(data.data(), data.size());
  return grpc::ByteBuffer(&slice, 1);
}

class ServeBenchmark : public benchmark::Fixture {
public:
  void SetUp(const benchmark::State& state) override {
    sr::kernel::register_sample_kernels(runtime_.registry());
    sr::kernel::register_rpc_kernels(runtime_.registry());

    const char *dsl = R"JSON(
    {
      "version": 1,
      "name": "bench_echo",
      "nodes": [
        { "id": "in", "kernel": "rpc_server_input", "inputs": ["call"], "outputs": ["method", "payload", "metadata"] },
        { "id": "out", "kernel": "rpc_server_output", "inputs": ["call", "payload"], "outputs": [] }
      ],
      "bindings": [
        { "to": "in.call", "from": "$req.rpc.call" },
        { "to": "out.call", "from": "$req.rpc.call" },
        { "to": "out.payload", "from": "in.payload" }
      ],
      "outputs": []
    }
    )JSON";

    sr::engine::StageOptions options;
    options.publish = true;
    auto snapshot = runtime_.stage_dsl(dsl, options);
    if (!snapshot) {
      throw std::runtime_error(snapshot.error().message);
    }

    sr::engine::ServeEndpointConfig config;
    sr::engine::GrpcServeConfig grpc_config;
    grpc_config.address = "127.0.0.1:0";
    grpc_config.io_threads = 1;
    config.transport = grpc_config;
    config.request_threads = 0; // Use shared pool
    config.max_inflight = 4096;

    host_ = runtime_.serve(config);
    if (!host_) {
      throw std::runtime_error(host_.error().message);
    }

    auto snapshots = (*host_)->stats();
    if (snapshots.empty()) {
       throw std::runtime_error("no endpoints");
    }
    port_ = snapshots.front().port;
    
    channel_ = grpc::CreateChannel(std::format("127.0.0.1:{}", port_),
                                     grpc::InsecureChannelCredentials());
  }

  void TearDown(const benchmark::State& state) override {
    channel_.reset();
    (*host_)->shutdown();
    (*host_)->wait();
  }

  sr::engine::Runtime runtime_;
  sr::engine::Expected<std::unique_ptr<sr::engine::ServeHost>> host_ = tl::unexpected(sr::engine::make_error("init"));
  int port_ = 0;
  std::shared_ptr<grpc::Channel> channel_;
};

BENCHMARK_DEFINE_F(ServeBenchmark, UnaryEcho)(benchmark::State& state) {
  grpc::GenericStub stub(channel_);
  const auto request = make_byte_buffer("bench");
  
  for (auto _ : state) {
    grpc::ClientContext context;
    context.set_wait_for_ready(true);
    context.AddMetadata("sr-graph-name", "bench_echo");
    grpc::ByteBuffer response;
    auto promise = std::make_shared<std::promise<grpc::Status>>();
    auto future = promise->get_future();
    stub.UnaryCall(&context, "/sr.engine.Echo/Unary", grpc::StubOptions{}, &request, &response,
                   [promise](grpc::Status status) {
                     promise->set_value(std::move(status));
                   });
    if (future.wait_for(std::chrono::seconds(5)) == std::future_status::timeout) {
      context.TryCancel();
      state.SkipWithError("RPC timed out");
      break;
    }
    auto status = future.get();
    if (!status.ok()) {
      state.SkipWithError(status.error_message().c_str());
    }
  }
}

BENCHMARK_REGISTER_F(ServeBenchmark, UnaryEcho)->Threads(1)->UseRealTime();

} // namespace

BENCHMARK_MAIN();