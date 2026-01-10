#include "test_support.hpp"

#include "kernel/rpc_kernels.hpp"

#include <grpcpp/generic/async_generic_service.h>
#include <grpcpp/grpcpp.h>

#include <arpa/inet.h>
#include <bit>
#include <cstdint>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <optional>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
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
    const auto *ptr = reinterpret_cast<const char *>(slice.begin());
    data.append(ptr, slice.size());
  }
  return data;
}

constexpr std::size_t kShardHeaderSize =
    sizeof(std::uint32_t) * 2 + sizeof(std::uint64_t);

auto append_u32_le(std::string &out, std::uint32_t value) -> void {
  for (int i = 0; i < 4; ++i) {
    out.push_back(static_cast<char>(value & 0xFFu));
    value >>= 8;
  }
}

auto append_u64_le(std::string &out, std::uint64_t value) -> void {
  for (int i = 0; i < 8; ++i) {
    out.push_back(static_cast<char>(value & 0xFFu));
    value >>= 8;
  }
}

auto read_u32_le(const unsigned char *data) -> std::uint32_t {
  std::uint32_t value = 0;
  for (int i = 3; i >= 0; --i) {
    value = (value << 8) | data[i];
  }
  return value;
}

auto read_u64_le(const unsigned char *data) -> std::uint64_t {
  std::uint64_t value = 0;
  for (int i = 7; i >= 0; --i) {
    value = (value << 8) | data[i];
  }
  return value;
}

auto encode_shard_payload(std::uint32_t part_index, std::uint32_t part_count,
                          std::int64_t value) -> grpc::ByteBuffer {
  std::string data;
  data.reserve(kShardHeaderSize);
  append_u32_le(data, part_index);
  append_u32_le(data, part_count);
  append_u64_le(data, std::bit_cast<std::uint64_t>(value));
  return make_byte_buffer(data);
}

struct DecodedShard {
  std::uint32_t index = 0;
  std::uint32_t count = 0;
  std::int64_t value = 0;
};

auto decode_shard_payload(const grpc::ByteBuffer &buffer)
    -> sr::engine::Expected<DecodedShard> {
  const auto data = byte_buffer_to_string(buffer);
  if (data.size() < kShardHeaderSize) {
    return tl::unexpected(sr::engine::make_error("shard payload too small"));
  }
  const auto *bytes = reinterpret_cast<const unsigned char *>(data.data());
  DecodedShard decoded;
  decoded.index = read_u32_le(bytes);
  decoded.count = read_u32_le(bytes + sizeof(std::uint32_t));
  decoded.value = std::bit_cast<std::int64_t>(
      read_u64_le(bytes + sizeof(std::uint32_t) * 2));
  return decoded;
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

auto reserve_ephemeral_port() -> int {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return 0;
  }
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  if (::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
    ::close(fd);
    return 0;
  }
  socklen_t len = sizeof(addr);
  if (::getsockname(fd, reinterpret_cast<sockaddr *>(&addr), &len) != 0) {
    ::close(fd);
    return 0;
  }
  const int port = static_cast<int>(ntohs(addr.sin_port));
  ::close(fd);
  return port;
}

class ShardUnaryServer {
public:
  explicit ShardUnaryServer(std::int64_t multiplier) : multiplier_(multiplier) {
    grpc::ServerBuilder builder;
    builder.RegisterAsyncGenericService(&service_);
    const int reserved_port = reserve_ephemeral_port();
    if (reserved_port <= 0) {
      return;
    }
    const std::string address =
        std::string("127.0.0.1:") + std::to_string(reserved_port);
    builder.AddListeningPort(address, grpc::InsecureServerCredentials(),
                             &port_);
    cq_ = builder.AddCompletionQueue();
    server_ = builder.BuildAndStart();
    if (server_) {
      worker_ = std::thread([this]() { HandleRpcs(); });
    } else if (cq_) {
      cq_->Shutdown();
    }
  }

  ~ShardUnaryServer() { Shutdown(); }

  auto port() const -> int { return port_; }

  auto Shutdown() -> void {
    if (server_) {
      server_->Shutdown();
    }
    if (cq_) {
      cq_->Shutdown();
    }
    if (worker_.joinable()) {
      worker_.join();
    }
    server_.reset();
  }

private:
  class CallData {
  public:
    CallData(grpc::AsyncGenericService *service,
             grpc::ServerCompletionQueue *cq, std::int64_t multiplier)
        : service_(service), cq_(cq), stream_(&ctx_), multiplier_(multiplier) {
      Proceed(true);
    }

    auto Proceed(bool ok) -> void {
      if (state_ == State::Create) {
        state_ = State::Request;
        service_->RequestCall(&ctx_, &stream_, cq_, cq_, this);
        return;
      }
      if (!ok) {
        delete this;
        return;
      }
      if (state_ == State::Request) {
        new CallData(service_, cq_, multiplier_);
        state_ = State::Read;
        stream_.Read(&request_, this);
        return;
      }
      if (state_ == State::Read) {
        auto decoded = decode_shard_payload(request_);
        grpc::Status status;
        if (!decoded) {
          status = grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                                decoded.error().message);
        } else {
          response_ = encode_shard_payload(decoded->index, decoded->count,
                                           decoded->value * multiplier_);
          status = grpc::Status::OK;
        }
        state_ = State::Write;
        stream_.WriteAndFinish(response_, grpc::WriteOptions{}, status, this);
        return;
      }
      delete this;
    }

  private:
    enum class State { Create, Request, Read, Write };

    grpc::AsyncGenericService *service_ = nullptr;
    grpc::ServerCompletionQueue *cq_ = nullptr;
    grpc::GenericServerContext ctx_;
    grpc::GenericServerAsyncReaderWriter stream_;
    grpc::ByteBuffer request_;
    grpc::ByteBuffer response_;
    std::int64_t multiplier_ = 1;
    State state_ = State::Create;
  };

  auto HandleRpcs() -> void {
    new CallData(&service_, cq_.get(), multiplier_);
    void *tag = nullptr;
    bool ok = false;
    while (cq_->Next(&tag, &ok)) {
      static_cast<CallData *>(tag)->Proceed(ok);
    }
  }

  grpc::AsyncGenericService service_;
  std::unique_ptr<grpc::ServerCompletionQueue> cq_;
  std::unique_ptr<grpc::Server> server_;
  std::thread worker_;
  int port_ = 0;
  std::int64_t multiplier_ = 1;
};

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
