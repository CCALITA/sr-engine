#include "test_support.hpp"

#include "kernel/rpc_kernels.hpp"

#include <grpcpp/generic/generic_stub.h>
#include <grpcpp/grpcpp.h>

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <format>
#include <mutex>
#include <optional>
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

using MetadataPairs = std::vector<std::pair<std::string, std::string>>;

/// Perform a blocking unary call through a generic stub.
auto perform_unary_call(grpc::GenericStub &stub, std::string_view method,
                        const grpc::ByteBuffer &request,
                        std::chrono::milliseconds timeout,
                        const MetadataPairs &metadata = {})
    -> UnaryCallResult {
  grpc::ClientContext context;
  context.set_wait_for_ready(true);
  context.set_deadline(std::chrono::system_clock::now() + timeout);
  for (const auto &entry : metadata) {
    context.AddMetadata(entry.first, entry.second);
  }

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

auto endpoint_snapshot(const sr::engine::ServeHost &host)
    -> std::optional<sr::engine::ServeEndpointSnapshot> {
  auto snapshots = host.stats();
  if (snapshots.empty()) {
    return std::nullopt;
  }
  return snapshots.front();
}

/// Validate serve configuration errors for missing required fields.
auto test_serve_config_validation() -> bool {
  sr::engine::Runtime runtime;
  sr::kernel::register_sample_kernels(runtime.registry());
  sr::kernel::register_rpc_kernels(runtime.registry());

  sr::engine::ServeEndpointConfig config;
  config.graph.metadata.name_header.clear();
  sr::engine::GrpcServeConfig grpc_config;
  grpc_config.address = "127.0.0.1:0";
  config.transport = grpc_config;
  auto host = runtime.serve(config);
  if (host) {
    std::cerr << "expected missing graph metadata key error\n";
    return false;
  }
  if (host.error().message.find("metadata") == std::string::npos) {
    std::cerr << "unexpected error: " << host.error().message << "\n";
    return false;
  }

  sr::engine::ServeEndpointConfig bad_address;
  sr::engine::GrpcServeConfig missing_address;
  missing_address.address = "";
  bad_address.transport = missing_address;
  auto host_bad_address = runtime.serve(bad_address);
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

  sr::engine::ServeEndpointConfig config;
  sr::engine::GrpcServeConfig grpc_config;
  grpc_config.address = "127.0.0.1:0";
  grpc_config.io_threads = 1;
  config.transport = grpc_config;
  config.queue_capacity = 0;
  config.request_threads = 1;
  config.max_inflight = 4;

  auto host = runtime.serve(config);
  if (!host) {
    std::cerr << "serve error: " << host.error().message << "\n";
    return false;
  }

  auto snapshot2 = endpoint_snapshot(*(*host));
  if (!snapshot2) {
    std::cerr << "missing endpoint snapshot\n";
    return false;
  }
  const int port = snapshot2->port;
  if (port <= 0) {
    std::cerr << "invalid serve port\n";
    return false;
  }

  auto channel = grpc::CreateChannel(std::format("127.0.0.1:{}", port),
                                     grpc::InsecureChannelCredentials());
  grpc::GenericStub stub(channel);

  const auto request = make_byte_buffer("hello");
  MetadataPairs metadata{{"sr-graph-name", "serve_echo"}};
  auto result = perform_unary_call(stub, "/sr.engine.Echo/Unary", request,
                                   std::chrono::seconds(2), metadata);
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
  auto snapshot1 = endpoint_snapshot(*(*host));
  if (!snapshot1) {
    std::cerr << "missing endpoint snapshot\n";
    return false;
  }
  const auto stats = snapshot1->stats;
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

  sr::engine::ServeEndpointConfig config;
  sr::engine::GrpcServeConfig grpc_config;
  grpc_config.address = "127.0.0.1:0";
  grpc_config.io_threads = 1;
  config.transport = grpc_config;
  config.queue_capacity = 0;
  config.request_threads = 1;
  config.max_inflight = 2;

  auto host = runtime.serve(config);
  if (!host) {
    std::cerr << "serve error: " << host.error().message << "\n";
    return false;
  }

  auto snapshot = endpoint_snapshot(*(*host));
  if (!snapshot) {
    std::cerr << "missing endpoint snapshot\n";
    return false;
  }
  const int port = snapshot->port;
  if (port <= 0) {
    std::cerr << "invalid serve port\n";
    return false;
  }

  auto channel = grpc::CreateChannel(std::format("127.0.0.1:{}", port),
                                     grpc::InsecureChannelCredentials());
  grpc::GenericStub stub(channel);

  auto result =
      perform_unary_call(stub, "/sr.engine.Missing/Unary",
                         make_byte_buffer("ping"), std::chrono::seconds(2),
                         {{"sr-graph-name", "missing_graph"}});
  if (result.status.error_code() != grpc::StatusCode::NOT_FOUND) {
    std::cerr << "expected NOT_FOUND, got "
              << static_cast<int>(result.status.error_code()) << "\n";
    return false;
  }

  (*host)->wait();
  snapshot = endpoint_snapshot(*(*host));
  if (!snapshot) {
    std::cerr << "missing endpoint snapshot\n";
    return false;
  }
  const auto stats = snapshot->stats;
  if (stats.accepted != 1 || stats.completed != 1 || stats.failed != 1 ||
      stats.rejected != 0 || stats.inflight != 0 || stats.queued != 0) {
    std::cerr << "unexpected stats for missing graph\n";
    return false;
  }

  (*host)->shutdown();
  (*host)->wait();
  return true;
}

/// Verify multiple gRPC endpoints can run concurrently.
auto test_serve_multi_endpoint() -> bool {
  const char *dsl = R"JSON(
  {
    "version": 1,
    "name": "serve_echo_multi",
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

  sr::engine::ServeEndpointConfig endpoint_a;
  sr::engine::GrpcServeConfig grpc_a;
  grpc_a.address = "127.0.0.1:0";
  grpc_a.io_threads = 1;
  endpoint_a.transport = grpc_a;
  endpoint_a.queue_capacity = 0;
  endpoint_a.request_threads = 1;

  sr::engine::ServeEndpointConfig endpoint_b;
  sr::engine::GrpcServeConfig grpc_b;
  grpc_b.address = "127.0.0.1:0";
  grpc_b.io_threads = 1;
  endpoint_b.transport = grpc_b;
  endpoint_b.queue_capacity = 0;
  endpoint_b.request_threads = 1;

  sr::engine::ServeLayerConfig layer;
  layer.endpoints.push_back(endpoint_a);
  layer.endpoints.push_back(endpoint_b);

  auto host = runtime.serve(layer);
  if (!host) {
    std::cerr << "serve error: " << host.error().message << "\n";
    return false;
  }

  auto snapshots = (*host)->stats();
  if (snapshots.size() != 2) {
    std::cerr << "expected 2 endpoints, got " << snapshots.size() << "\n";
    return false;
  }

  std::vector<int> ports;
  ports.reserve(snapshots.size());
  for (const auto &snapshot : snapshots) {
    if (snapshot.port <= 0) {
      std::cerr << "invalid endpoint port\n";
      return false;
    }
    ports.push_back(snapshot.port);
  }

  const auto request = make_byte_buffer("hello");
  for (int port : ports) {
    auto channel = grpc::CreateChannel(std::format("127.0.0.1:{}", port),
                                       grpc::InsecureChannelCredentials());
    grpc::GenericStub stub(channel);

    auto result = perform_unary_call(stub, "/sr.engine.Echo/Unary", request,
                                     std::chrono::seconds(2),
                                     {{"sr-graph-name", "serve_echo_multi"}});
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
  }

  (*host)->wait();
  snapshots = (*host)->stats();
  for (const auto &snapshot : snapshots) {
    const auto &stats = snapshot.stats;
    if (stats.accepted != 1 || stats.completed != 1 || stats.failed != 0 ||
        stats.rejected != 0 || stats.inflight != 0 || stats.queued != 0) {
      std::cerr << "unexpected stats for endpoint " << snapshot.name << "\n";
      return false;
    }
  }

  (*host)->shutdown();
  (*host)->wait();
  return true;
}
