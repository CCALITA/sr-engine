#include <benchmark/benchmark.h>
#include <grpcpp/grpcpp.h>
#include <thread>
#include <vector>
#include <chrono>
#include <atomic>
#include <future>
#include <iostream>
#include <mutex>

#include "runtime/runtime.hpp"
#include "kernel/rpc_kernels.hpp"
#include "kernel/sample_kernels.hpp"

namespace {

auto make_byte_buffer(std::string_view data) -> grpc::ByteBuffer {
  grpc::Slice slice(data.data(), data.size());
  return grpc::ByteBuffer(&slice, 1);
}

auto byte_buffer_to_string(const grpc::ByteBuffer &buffer) -> std::string {
  std::vector<grpc::Slice> slices;
  buffer.Dump(&slices);
  std::size_t total = 0;
  for (const auto &slice : slices) {
    total += slice.size();
  }
  std::string data;
  data.reserve(total);
  for (const auto &slice : slices) {
    const auto *ptr = reinterpret_cast<const char *>(slice.begin());
    data.append(ptr, slice.size());
  }
  return data;
}

class ServeBenchmark : public benchmark::Fixture {
public:
  void SetUp(const benchmark::State& state) override {
    if (!initialized_) {
      std::call_once(init_flag_, []() {
        initialize_benchmark();
      });
    }

    if (!channel_) {
      throw std::runtime_error("benchmark not initialized");
    }
  }

  void TearDown(const benchmark::State& state) override {
  }

  static auto initialize_benchmark() -> void {
    sr::kernel::register_builtin_types();
    sr::kernel::register_sample_kernels(get_runtime().registry());
    sr::kernel::register_rpc_kernels(get_runtime().registry());

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
    auto snapshot = get_runtime().stage_dsl(dsl, options);
    if (!snapshot) {
      throw std::runtime_error(snapshot.error().message);
    }

    sr::engine::ServeEndpointConfig config;
    sr::engine::GrpcServeConfig grpc_config;
    grpc_config.address = "127.0.0.1:0";
    grpc_config.io_threads = 4;
    config.transport = grpc_config;
    config.request_threads = 8;
    config.max_inflight = 4096;

    auto serve_result = get_runtime().serve(config);
    if (!serve_result) {
      throw std::runtime_error(serve_result.error().message);
    }
    host_ = std::move(*serve_result);

    auto snapshots = host_->stats();
    if (snapshots.empty()) {
       throw std::runtime_error("no endpoints");
    }
    port_ = snapshots.front().port;

    channel_ = grpc::CreateChannel(std::format("127.0.0.1:{}", port_),
                                     grpc::InsecureChannelCredentials());

    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(5);
    if (!channel_->WaitForConnected(deadline)) {
      throw std::runtime_error("channel failed to connect");
    }

    initialized_ = true;
  }

  static sr::engine::Runtime& get_runtime() {
    static sr::engine::Runtime runtime;
    return runtime;
  }

  static std::once_flag init_flag_;
  static std::unique_ptr<sr::engine::ServeHost> host_;
  static std::uint16_t port_;
  static std::shared_ptr<grpc::Channel> channel_;
  static bool initialized_;
};

std::once_flag ServeBenchmark::init_flag_;
std::unique_ptr<sr::engine::ServeHost> ServeBenchmark::host_;
std::uint16_t ServeBenchmark::port_ = 0;
std::shared_ptr<grpc::Channel> ServeBenchmark::channel_;
bool ServeBenchmark::initialized_ = false;

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
    if (future.wait_for(std::chrono::seconds(10)) == std::future_status::timeout) {
      context.TryCancel();
      state.SkipWithError("RPC timed out");
      break;
    }
    auto status = future.get();
    if (!status.ok()) {
      state.SkipWithError(status.error_message().c_str());
    }
    const auto response_str = byte_buffer_to_string(response);
    if (response_str != "bench") {
      state.SkipWithError("unexpected response");
    }
  }
}

BENCHMARK_REGISTER_F(ServeBenchmark, UnaryEcho)->Iterations(10000)->UseRealTime()->Repetitions(3);

BENCHMARK_DEFINE_F(ServeBenchmark, MultiThreadedEcho)(benchmark::State& state) {
  const int num_threads = state.threads();
  grpc::GenericStub stub(channel_);
  const auto request = make_byte_buffer("bench_multi");

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
    if (future.wait_for(std::chrono::seconds(10)) == std::future_status::timeout) {
      context.TryCancel();
      state.SkipWithError("RPC timed out");
      break;
    }
    auto status = future.get();
    if (!status.ok()) {
      state.SkipWithError(status.error_message().c_str());
    }
    const auto response_str = byte_buffer_to_string(response);
    if (response_str != "bench_multi") {
      state.SkipWithError("unexpected response");
    }
  }
}

BENCHMARK_REGISTER_F(ServeBenchmark, MultiThreadedEcho)
    ->Threads(2)->UseRealTime()->Repetitions(3);
BENCHMARK_REGISTER_F(ServeBenchmark, MultiThreadedEcho)
    ->Threads(4)->UseRealTime()->Repetitions(3);
BENCHMARK_REGISTER_F(ServeBenchmark, MultiThreadedEcho)
    ->Threads(8)->UseRealTime()->Repetitions(3);

BENCHMARK_DEFINE_F(ServeBenchmark, HighThroughputEcho)(benchmark::State& state) {
  grpc::GenericStub stub(channel_);
  const auto request = make_byte_buffer("bench");
  const int batch_size = state.range(0);

  for (auto _ : state) {
    std::vector<std::shared_ptr<std::promise<grpc::Status>>> promises;
    std::vector<std::future<grpc::Status>> futures;
    promises.reserve(static_cast<std::size_t>(batch_size));
    futures.reserve(static_cast<std::size_t>(batch_size));

    for (int i = 0; i < batch_size; ++i) {
      grpc::ClientContext context;
      context.set_wait_for_ready(true);
      context.AddMetadata("sr-graph-name", "bench_echo");
      grpc::ByteBuffer response;
      auto promise = std::make_shared<std::promise<grpc::Status>>();
      futures.push_back(promise->get_future());
      promises.push_back(promise);
      stub.UnaryCall(&context, "/sr.engine.Echo/Unary", grpc::StubOptions{}, &request, &response,
                     [promise](grpc::Status status) {
                       promise->set_value(std::move(status));
                     });
    }

    for (auto &future : futures) {
      if (future.wait_for(std::chrono::seconds(2)) == std::future_status::timeout) {
        state.SkipWithError("RPC timed out");
        break;
      }
    }
  }
}

BENCHMARK_REGISTER_F(ServeBenchmark, HighThroughputEcho)
    ->Range(1, 16)->UseRealTime()->Repetitions(3);

} // namespace

BENCHMARK_MAIN();