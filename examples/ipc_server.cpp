#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <format>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "kernel/rpc_kernels.hpp"
#include "kernel/sample_kernels.hpp"
#include "runtime/runtime.hpp"
#include "runtime/serve.hpp"

namespace {

constexpr std::string_view kDefaultSocketPath = "/tmp/sr-ipc-demo.sock";
constexpr std::string_view kDemoGraphName = "ipc_concat_example";

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

auto string_to_byte_buffer(std::string_view data) -> grpc::ByteBuffer {
  grpc::Slice slice(data.data(), data.size());
  return grpc::ByteBuffer(&slice, 1);
}

auto register_demo_kernels(sr::engine::KernelRegistry& registry) -> void {
  registry.register_kernel("bytes_to_str", [](const grpc::ByteBuffer& buffer) noexcept {
     return byte_buffer_to_string(buffer);
  });
  registry.register_kernel("str_to_bytes", [](const std::string& str) noexcept {
     return string_to_byte_buffer(str);
  });
}

struct DemoOptions {
  std::string socket_path = std::string(kDefaultSocketPath);
  bool serve_only = false;
};

auto print_usage(std::string_view program) -> void {
  std::cout << std::format(
      "Usage: {} [--socket <path>] [--serve-only]\n"
      "Defaults:\n"
      "  --socket {}",
      program,
      kDefaultSocketPath);
}

auto parse_args(int argc, char **argv) -> std::optional<DemoOptions> {
  DemoOptions options;
  for (int index = 1; index < argc; ++index) {
    std::string_view arg(argv[index]);
    if (arg == "--help" || arg == "-h") {
      print_usage(argv[0]);
      return std::nullopt;
    }
    if (arg == "--serve-only") {
      options.serve_only = true;
      continue;
    }
    auto next_value = [&](std::string_view flag) -> std::optional<std::string> {
      if (index + 1 >= argc) {
        std::cerr << "Missing value for " << flag << "\n";
        print_usage(argv[0]);
        return std::nullopt;
      }
      ++index;
      return std::string(argv[index]);
    };
    if (arg == "--socket") {
      auto value = next_value(arg);
      if (!value) {
        return std::nullopt;
      }
      options.socket_path = std::move(*value);
      continue;
    }
    std::cerr << "Unknown argument: " << arg << "\n";
    print_usage(argv[0]);
    return std::nullopt;
  }
  return options;
}

namespace client {

auto read_exact(int fd, void *buffer, std::size_t size) -> bool {
  auto *out = static_cast<unsigned char *>(buffer);
  std::size_t offset = 0;
  while (offset < size) {
    const auto n = ::read(fd, out + offset, size - offset);
    if (n == 0) {
      return false;
    }
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    offset += static_cast<std::size_t>(n);
  }
  return true;
}

auto write_all(int fd, const void *buffer, std::size_t size) -> bool {
  const auto *data = static_cast<const unsigned char *>(buffer);
  std::size_t offset = 0;
  while (offset < size) {
    const auto n = ::write(fd, data + offset, size - offset);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    offset += static_cast<std::size_t>(n);
  }
  return true;
}

auto read_u32_be(int fd, std::uint32_t &value) -> bool {
  std::uint32_t raw = 0;
  if (!read_exact(fd, &raw, sizeof(std::uint32_t))) {
    return false;
  }
  value = ntohl(raw);
  return true;
}

auto write_u32_be(int fd, std::uint32_t value) -> bool {
  const auto raw = htonl(value);
  return write_all(fd, &raw, sizeof(std::uint32_t));
}

auto read_string(int fd, std::string &value) -> bool {
  std::uint32_t len = 0;
  if (!read_u32_be(fd, len)) {
    return false;
  }
  value.resize(len);
  return read_exact(fd, value.data(), len);
}

auto write_string(int fd, std::string_view value) -> bool {
  if (!write_u32_be(fd, static_cast<uint32_t>(value.size()))) {
    return false;
  }
  return write_all(fd, value.data(), value.size());
}

auto run_demo_client(const std::string& socket_path) -> bool {
  int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) {
    std::cerr << "Client Error: failed to create socket\n";
    return false;
  }

  sockaddr_un addr{};
  addr.sun_family = AF_UNIX;
  std::snprintf(addr.sun_path, sizeof(addr.sun_path), "%s",
                socket_path.c_str());

  if (::connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
    std::cerr << "Client Error: failed to connect to socket\n";
    ::close(fd);
    return false;
  }

  // Method name
  if (!write_string(fd, "some_method")) {
    std::cerr << "Client Error: failed to write method\n";
    ::close(fd);
    return false;
  }

  // Metadata: graph name
  if (!write_u32_be(fd, 1)) { // 1 metadata entry
    std::cerr << "Client Error: failed to write metadata count\n";
    ::close(fd);
    return false;
  }
  if (!write_string(fd, "sr-graph-name") ||
      !write_string(fd, kDemoGraphName)) {
    std::cerr << "Client Error: failed to write metadata\n";
    ::close(fd);
    return false;
  }

  // Payload
  if (!write_string(fd, "hello")) {
    std::cerr << "Client Error: failed to write payload\n";
    ::close(fd);
    return false;
  }

  std::uint32_t status_code;
  if (!read_u32_be(fd, status_code)) {
    std::cerr << "Client Error: failed to read status code\n";
    ::close(fd);
    return false;
  }

  std::string status_message, status_details, payload;
  if (!read_string(fd, status_message) ||
      !read_string(fd, status_details)) {
    std::cerr << "Client Error: failed to read status message/details\n";
    ::close(fd);
    return false;
  }

  std::uint32_t trailing_meta_count;
  if (!read_u32_be(fd, trailing_meta_count)) {
    std::cerr << "Client Error: failed to read trailing metadata count\n";
    ::close(fd);
    return false;
  }

  for (uint32_t i = 0; i < trailing_meta_count; ++i) {
    std::string key, value;
    if (!read_string(fd, key) || !read_string(fd, value)) {
      std::cerr << "Client Error: failed to read trailing metadata\n";
      ::close(fd);
      return false;
    }
  }

  if (!read_string(fd, payload)) {
    std::cerr << "Client Error: failed to read payload\n";
    ::close(fd);
    return false;
  }

  ::close(fd);

  if (status_code != 0) {
    std::cerr << std::format("Server Error: code={}, message='{}'\n",
                             status_code, status_message);
    return false;
  }

  std::cout << "Server response: " << payload << "\n";
  return true;
}

} // namespace client
} // namespace

int main(int argc, char **argv) {
  auto options = parse_args(argc, argv);
  if (!options) {
    return 1;
  }

  static sr::engine::TypeRegistry type_registry;
  sr::engine::Runtime runtime;
  sr::kernel::register_builtin_types(type_registry);
  sr::kernel::register_sample_kernels(runtime.registry());
  sr::kernel::register_rpc_kernels(runtime.registry());
  register_demo_kernels(runtime.registry());

  const char *dsl = R"JSON(
  {
    "version": 1,
    "name": "ipc_concat_example",
    "nodes": [
      { "id": "in_str", "kernel": "bytes_to_str", "inputs": ["in"], "outputs": ["text"] },
      { "id": "c1", "kernel": "const_str", "params": { "value": " world" }, "outputs": ["text"] },
      { "id": "cat", "kernel": "concat_str", "inputs": ["a", "b"], "outputs": ["result"] },
      { "id": "out_bytes", "kernel": "str_to_bytes", "inputs": ["in"], "outputs": ["bytes"] },
      { "id": "reply", "kernel": "rpc_server_output", "params": {}, "inputs": ["call", "payload"]}
    ],
    "bindings": [
      { "to": "in_str.in", "from": "$req.rpc.payload" },
      { "to": "cat.a", "from": "in_str.text" },
      { "to": "cat.b", "from": "c1.text" },
      { "to": "out_bytes.in", "from": "cat.result"},
      { "to": "reply.payload", "from": "out_bytes.bytes" },
      { "to": "reply.call", "from": "$req.rpc.call" }
    ],
    "outputs": []
  }
  )JSON";

  sr::engine::StageOptions stage_options;
  stage_options.source = "examples/ipc_server.cpp";
  stage_options.publish = true;
  auto snapshot = runtime.stage_dsl(dsl, stage_options);
  if (!snapshot) {
    std::cerr << "Stage error: " << snapshot.error().message << "\n";
    return 1;
  }

  sr::engine::ServeEndpointConfig endpoint;
  endpoint.name = std::string(kDemoGraphName);
  sr::engine::IpcServeConfig ipc;
  ipc.path = options->socket_path;
  ipc.io_threads = 1;
  ipc.remove_existing = true;
  endpoint.transport = ipc;
  endpoint.request_threads = 1;
  endpoint.max_inflight = 8;
  endpoint.graph.metadata.name_header = "sr-graph-name";

  auto host = runtime.serve(endpoint);
  if (!host) {
    std::cerr << "Serve error: " << host.error().message << "\n";
    return 1;
  }

  std::cout << "IPC server listening on " << options->socket_path << "\n";

  if (options->serve_only) {
    std::cout << "Press Enter to stop the server.\n";
    std::string line;
    std::getline(std::cin, line);
  } else {
    // Give server a moment to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    bool client_ok = client::run_demo_client(options->socket_path);
    if (!client_ok) {
       std::cerr << "Client demo failed\n";
       (*host)->shutdown();
       (*host)->wait();
       return 1;
    }
  }

  (*host)->shutdown();
  (*host)->wait();

  return 0;
}
