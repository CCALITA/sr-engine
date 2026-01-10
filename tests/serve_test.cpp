#include "test_support.hpp"

#include "kernel/rpc_kernels.hpp"

#include <grpcpp/generic/generic_stub.h>
#include <grpcpp/grpcpp.h>

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <format>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

/// Build a ByteBuffer from a string view.
auto make_byte_buffer(std::string_view data) -> grpc::ByteBuffer {
  grpc::Slice slice(data.data(), data.size());
  return grpc::ByteBuffer(&slice, 1);
}

/// Convert a ByteBuffer into a contiguous string.
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

/// Result container for a unary gRPC call.
struct UnaryCallResult {
  grpc::ByteBuffer response;
  grpc::Status status;
};

/// Perform a blocking unary call through a generic stub.
auto perform_unary_call(grpc::GenericStub &stub, std::string_view method,
                        const grpc::ByteBuffer &request,
                        std::chrono::milliseconds timeout)
    -> UnaryCallResult {
  grpc::ClientContext context;
  context.set_wait_for_ready(true);
  context.set_deadline(std::chrono::system_clock::now() + timeout);

  grpc::ByteBuffer response;
  grpc::Status status;
  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;

  const std::string method_str(method);
  stub.UnaryCall(&context, method_str, grpc::StubOptions{}, &request, &response,
                 [&mutex, &cv, &done, &status](grpc::Status call_status) {
                   std::lock_guard<std::mutex> lock(mutex);
                   status = std::move(call_status);
                   done = true;
                   cv.notify_one();
                 });

  std::unique_lock<std::mutex> lock(mutex);
  cv.wait(lock, [&done] { return done; });

  return {std::move(response), std::move(status)};
}

} // namespace

/// Validate serve configuration errors for missing required fields.
auto test_serve_config_validation() -> bool {
  sr::engine::Runtime runtime;
  sr::kernel::register_sample_kernels(runtime.registry());
  sr::kernel::register_rpc_kernels(runtime.registry());

  sr::engine::ServeConfig config;
  config.graph_name = "";
  config.address = "127.0.0.1:0";
  auto host = runtime.serve(config);
  if (host) {
    std::cerr << "expected missing graph_name error\n";
    return false;
  }
  if (host.error().message.find("graph_name") == std::string::npos) {
    std::cerr << "unexpected error: " << host.error().message << "\n";
    return false;
  }

  config.graph_name = "missing_graph";
  config.address = "";
  auto host_bad_address = runtime.serve(config);
  if (host_bad_address) {
    std::cerr << "expected missing address error\n";
    return false;
  }
  if (host_bad_address.error().message.find("address") == std::string::npos) {
    std::cerr << "unexpected error: " << host_bad_address.error().message
              << "\n";
    return false;
  }

  return true;
}

/// Verify ServeHost echoes payloads via rpc_server_output.
auto test_serve_unary_echo() -> bool {
  const char *dsl = R"JSON(
  {
    "version": 1,
    "name": "serve_echo",
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

  sr::engine::Runtime runtime;
  sr::kernel::register_sample_kernels(runtime.registry());
  sr::kernel::register_rpc_kernels(runtime.registry());

  sr::engine::StageOptions options;
  options.publish = true;
  auto snapshot = runtime.stage_dsl(dsl, options);
  if (!snapshot) {
    std::cerr << "stage error: " << snapshot.error().message << "\n";
    return false;
  }

  sr::engine::ServeConfig config;
  config.graph_name = "serve_echo";
  config.address = "127.0.0.1:0";
  config.queue_capacity = 0;
  config.request_threads = 1;
  config.io_threads = 1;
  config.max_inflight = 4;

  auto host = runtime.serve(config);
  if (!host) {
    std::cerr << "serve error: " << host.error().message << "\n";
    return false;
  }

  const int port = (*host)->port();
  if (port <= 0) {
    std::cerr << "invalid serve port\n";
    return false;
  }

  auto channel = grpc::CreateChannel(
      std::format("127.0.0.1:{}", port),
      grpc::InsecureChannelCredentials());
  grpc::GenericStub stub(channel);

  const auto request = make_byte_buffer("hello");
  auto result = perform_unary_call(
      stub, "/sr.engine.Echo/Unary", request, std::chrono::seconds(2));
  if (!result.status.ok()) {
    std::cerr << "rpc error: " << result.status.error_message() << " ("
              << static_cast<int>(result.status.error_code()) << ")\n";
    return false;
  }
  const auto response = byte_buffer_to_string(result.response);
  if (response != "hello") {
    std::cerr << "unexpected response: " << response << "\n";
    return false;
  }

  (*host)->wait();
  auto stats = (*host)->stats();
  if (stats.accepted != 1 || stats.completed != 1 || stats.failed != 0 ||
      stats.rejected != 0 || stats.inflight != 0 || stats.queued != 0) {
    std::cerr << "unexpected stats\n";
    return false;
  }

  (*host)->shutdown();
  (*host)->wait();
  return true;
}

/// Verify ServeHost returns NOT_FOUND when the graph is missing.
auto test_serve_missing_graph() -> bool {
  sr::engine::Runtime runtime;
  sr::kernel::register_sample_kernels(runtime.registry());
  sr::kernel::register_rpc_kernels(runtime.registry());

  sr::engine::ServeConfig config;
  config.graph_name = "missing_graph";
  config.address = "127.0.0.1:0";
  config.queue_capacity = 0;
  config.request_threads = 1;
  config.io_threads = 1;
  config.max_inflight = 2;

  auto host = runtime.serve(config);
  if (!host) {
    std::cerr << "serve error: " << host.error().message << "\n";
    return false;
  }

  const int port = (*host)->port();
  if (port <= 0) {
    std::cerr << "invalid serve port\n";
    return false;
  }

  auto channel = grpc::CreateChannel(
      std::format("127.0.0.1:{}", port),
      grpc::InsecureChannelCredentials());
  grpc::GenericStub stub(channel);

  auto result = perform_unary_call(
      stub, "/sr.engine.Missing/Unary", make_byte_buffer("ping"),
      std::chrono::seconds(2));
  if (result.status.error_code() != grpc::StatusCode::NOT_FOUND) {
    std::cerr << "expected NOT_FOUND, got "
              << static_cast<int>(result.status.error_code()) << "\n";
    return false;
  }

  (*host)->wait();
  auto stats = (*host)->stats();
  if (stats.accepted != 1 || stats.completed != 1 || stats.failed != 1 ||
      stats.rejected != 0 || stats.inflight != 0 || stats.queued != 0) {
    std::cerr << "unexpected stats for missing graph\n";
    return false;
  }

  (*host)->shutdown();
  (*host)->wait();
  return true;
}
