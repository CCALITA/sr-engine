#include "test_support.hpp"

#include "kernel/rpc_kernels.hpp"

#include <grpcpp/grpcpp.h>

#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

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
    const auto *ptr = static_cast<const char *>(slice.begin());
    data.append(ptr, ptr + slice.size());
  }
  return data;
}

struct CaptureResponder final : sr::kernel::rpc::RpcResponder {
  std::mutex mutex;
  std::optional<sr::kernel::rpc::RpcResponse> last;

  /// Capture responses from rpc_server_output.
  auto send(sr::kernel::rpc::RpcResponse response) noexcept
      -> sr::engine::Expected<void> override {
    std::lock_guard<std::mutex> lock(mutex);
    last = std::move(response);
    return {};
  }
};

auto make_rpc_registry() -> sr::engine::KernelRegistry {
  sr::engine::KernelRegistry registry;
  sr::kernel::register_sample_kernels(registry);
  sr::kernel::register_rpc_kernels(registry);
  return registry;
}

} // namespace

auto test_rpc_flatbuffer_echo() -> bool {
  const char *dsl = R"JSON(
  {
    "version": 1,
    "name": "rpc_flatbuffer_echo",
    "nodes": [
      { "id": "echo", "kernel": "flatbuffer_echo", "inputs": ["payload"], "outputs": ["payload"] }
    ],
    "bindings": [
      { "to": "echo.payload", "from": "$req.payload" }
    ],
    "outputs": [
      { "from": "echo.payload", "as": "out" }
    ]
  }
  )JSON";

  sr::engine::GraphDef graph;
  std::string error;
  if (!parse_graph(dsl, graph, error)) {
    std::cerr << "parse error: " << error << "\n";
    return false;
  }

  auto registry = make_rpc_registry();
  auto plan = sr::engine::compile_plan(graph, registry);
  if (!plan) {
    std::cerr << "compile error: " << plan.error().message << "\n";
    return false;
  }

  sr::engine::Executor executor;
  sr::engine::RequestContext ctx;
  ctx.set_env<grpc::ByteBuffer>("payload", make_byte_buffer("hello"));

  auto result = executor.run(*plan, ctx);
  if (!result) {
    std::cerr << "run error: " << result.error().message << "\n";
    return false;
  }

  const auto &payload = result->outputs.at("out").get<grpc::ByteBuffer>();
  return byte_buffer_to_string(payload) == "hello";
}

auto test_rpc_server_input() -> bool {
  const char *dsl = R"JSON(
  {
    "version": 1,
    "name": "rpc_server_input",
    "nodes": [
      { "id": "in", "kernel": "rpc_server_input", "inputs": ["call"], "outputs": ["method", "payload", "metadata"] }
    ],
    "bindings": [
      { "to": "in.call", "from": "$req.call" }
    ],
    "outputs": [
      { "from": "in.method", "as": "method" },
      { "from": "in.payload", "as": "payload" }
    ]
  }
  )JSON";

  sr::engine::GraphDef graph;
  std::string error;
  if (!parse_graph(dsl, graph, error)) {
    std::cerr << "parse error: " << error << "\n";
    return false;
  }

  auto registry = make_rpc_registry();
  auto plan = sr::engine::compile_plan(graph, registry);
  if (!plan) {
    std::cerr << "compile error: " << plan.error().message << "\n";
    return false;
  }

  sr::engine::RequestContext ctx;
  sr::kernel::rpc::RpcServerCall call;
  call.method = "Echo";
  call.request = make_byte_buffer("ping");
  call.metadata.entries.push_back({"x-id", "42"});
  ctx.set_env<sr::kernel::rpc::RpcServerCall>("call", std::move(call));

  sr::engine::Executor executor;
  auto result = executor.run(*plan, ctx);
  if (!result) {
    std::cerr << "run error: " << result.error().message << "\n";
    return false;
  }

  const auto &method = result->outputs.at("method").get<std::string>();
  if (method != "Echo") {
    return false;
  }
  const auto &payload = result->outputs.at("payload").get<grpc::ByteBuffer>();
  return byte_buffer_to_string(payload) == "ping";
}

auto test_rpc_server_output() -> bool {
  const char *dsl = R"JSON(
  {
    "version": 1,
    "name": "rpc_server_output",
    "nodes": [
      { "id": "out", "kernel": "rpc_server_output", "inputs": ["call", "payload"], "outputs": [] }
    ],
    "bindings": [
      { "to": "out.call", "from": "$req.call" },
      { "to": "out.payload", "from": "$req.payload" }
    ],
    "outputs": []
  }
  )JSON";

  sr::engine::GraphDef graph;
  std::string error;
  if (!parse_graph(dsl, graph, error)) {
    std::cerr << "parse error: " << error << "\n";
    return false;
  }

  auto registry = make_rpc_registry();
  auto plan = sr::engine::compile_plan(graph, registry);
  if (!plan) {
    std::cerr << "compile error: " << plan.error().message << "\n";
    return false;
  }

  auto responder = std::make_shared<CaptureResponder>();
  sr::kernel::rpc::RpcServerCall call;
  call.method = "Echo";
  call.request = make_byte_buffer("request");
  call.responder = responder;

  sr::engine::RequestContext ctx;
  ctx.set_env<sr::kernel::rpc::RpcServerCall>("call", std::move(call));
  ctx.set_env<grpc::ByteBuffer>("payload", make_byte_buffer("response"));

  sr::engine::Executor executor;
  auto result = executor.run(*plan, ctx);
  if (!result) {
    std::cerr << "run error: " << result.error().message << "\n";
    return false;
  }

  std::lock_guard<std::mutex> lock(responder->mutex);
  if (!responder->last) {
    return false;
  }
  const auto &payload = responder->last->payload;
  if (byte_buffer_to_string(payload) != "response") {
    return false;
  }
  return responder->last->status.code == grpc::StatusCode::OK;
}
