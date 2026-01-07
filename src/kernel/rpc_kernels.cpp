#include "kernel/rpc_kernels.hpp"

#if SR_ENGINE_WITH_GRPC

#include <chrono>
#include <cstdint>
#include <format>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

namespace sr::kernel {
namespace {

using sr::engine::Expected;
using sr::engine::Json;
using sr::engine::KernelRegistry;

auto get_int_param(const Json &params, const char *key, int64_t fallback)
    -> int64_t {
  if (!params.is_object()) {
    return fallback;
  }
  auto it = params.find(key);
  if (it != params.end() && it->is_number_integer()) {
    return it->get<int64_t>();
  }
  return fallback;
}

auto get_bool_param(const Json &params, const char *key, bool fallback)
    -> bool {
  if (!params.is_object()) {
    return fallback;
  }
  auto it = params.find(key);
  if (it != params.end() && it->is_boolean()) {
    return it->get<bool>();
  }
  return fallback;
}

auto get_string_param(const Json &params, const char *key, std::string fallback)
    -> std::string {
  if (!params.is_object()) {
    return fallback;
  }
  auto it = params.find(key);
  if (it != params.end() && it->is_string()) {
    return it->get<std::string>();
  }
  return fallback;
}

auto get_metadata_param(const Json &params, const char *key)
    -> rpc::RpcMetadata {
  rpc::RpcMetadata metadata;
  if (!params.is_object()) {
    return metadata;
  }
  auto it = params.find(key);
  if (it == params.end()) {
    return metadata;
  }
  const auto &value = *it;
  if (value.is_object()) {
    for (const auto &entry : value.items()) {
      metadata.entries.push_back(
          {entry.key(), entry.value().is_string()
                            ? entry.value().get<std::string>()
                            : entry.value().dump()});
    }
  } else if (value.is_array()) {
    for (const auto &entry : value) {
      if (!entry.is_object()) {
        continue;
      }
      auto key_it = entry.find("key");
      auto value_it = entry.find("value");
      if (key_it == entry.end() || value_it == entry.end()) {
        continue;
      }
      if (!key_it->is_string()) {
        continue;
      }
      metadata.entries.push_back(
          {key_it->get<std::string>(), value_it->is_string()
                                           ? value_it->get<std::string>()
                                           : value_it->dump()});
    }
  }
  return metadata;
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

auto string_to_byte_buffer(std::string_view data) -> grpc::ByteBuffer {
  grpc::Slice slice(data.data(), data.size());
  return grpc::ByteBuffer(&slice, 1);
}

auto clone_byte_buffer(const grpc::ByteBuffer &buffer) -> grpc::ByteBuffer {
  return string_to_byte_buffer(byte_buffer_to_string(buffer));
}

struct ServerOutputKernel {
  rpc::RpcStatus status;

  /// Send a response using the responder stored on the server call.
  auto operator()(const rpc::RpcServerCall &call,
                  const grpc::ByteBuffer &payload) const noexcept
      -> Expected<void> {
    if (!call.responder) {
      return tl::unexpected(
          sr::engine::make_error("rpc responder missing on server call"));
    }
    rpc::RpcResponse response;
    response.payload = clone_byte_buffer(payload);
    response.status = status;
    return call.responder->send(std::move(response));
  }
};

struct UnaryCallKernel {
  std::shared_ptr<grpc::GenericStub> stub;
  std::string method;
  rpc::RpcMetadata metadata;
  std::chrono::milliseconds timeout{0};
  bool wait_for_ready = false;
  std::string authority;

  /// Invoke a unary RPC using grpc::GenericStub.
  auto operator()(const grpc::ByteBuffer &request) const noexcept
      -> Expected<grpc::ByteBuffer> {
    grpc::ClientContext context;
    if (!authority.empty()) {
      context.set_authority(authority);
    }
    context.set_wait_for_ready(wait_for_ready);
    if (timeout.count() > 0) {
      context.set_deadline(std::chrono::system_clock::now() + timeout);
    }
    for (const auto &entry : metadata.entries) {
      context.AddMetadata(entry.key, entry.value);
    }
    grpc::ByteBuffer response;
    const auto status = stub->UnaryCall(&context, method, request, &response);
    if (!status.ok()) {
      return tl::unexpected(sr::engine::make_error(
          std::format("rpc call failed: {} ({})", status.error_message(),
                      static_cast<int>(status.error_code()))));
    }
    return response;
  }
};

} // namespace

auto register_rpc_types() -> void {
  sr::engine::register_type<grpc::ByteBuffer>("grpc_byte_buffer");
  sr::engine::register_type<rpc::RpcMetadata>("rpc_metadata");
  sr::engine::register_type<rpc::RpcServerCall>("rpc_server_call");
}

auto register_rpc_kernels(KernelRegistry &registry) -> void {
  register_rpc_types();

  registry.register_kernel(
      "rpc_server_input", [](const rpc::RpcServerCall &call) noexcept {
        return std::make_tuple(call.method, clone_byte_buffer(call.request),
                               call.metadata);
      });

  registry.register_kernel_with_params(
      "rpc_server_output",
      [](const Json &params) -> Expected<ServerOutputKernel> {
        rpc::RpcStatus status;
        const auto code = get_int_param(params, "status_code", 0);
        status.code = static_cast<grpc::StatusCode>(code);
        status.message = get_string_param(params, "status_message", "");
        status.details = get_string_param(params, "status_details", "");

        return ServerOutputKernel{status};
      });

  registry.register_kernel("flatbuffer_echo",
                           [](const grpc::ByteBuffer &payload) noexcept {
                             return clone_byte_buffer(payload);
                           });

  registry.register_kernel_with_params(
      "rpc_unary_call", [](const Json &params) -> Expected<UnaryCallKernel> {
        if (!params.is_object()) {
          return tl::unexpected(
              sr::engine::make_error("rpc_unary_call expects params object"));
        }
        const auto target = get_string_param(params, "target", "");
        const auto method = get_string_param(params, "method", "");
        if (target.empty()) {
          return tl::unexpected(
              sr::engine::make_error("rpc_unary_call missing target"));
        }
        if (method.empty()) {
          return tl::unexpected(
              sr::engine::make_error("rpc_unary_call missing method"));
        }
        const auto timeout_ms = get_int_param(params, "timeout_ms", 0);
        const auto wait_for_ready =
            get_bool_param(params, "wait_for_ready", false);
        const auto authority = get_string_param(params, "authority", "");
        const auto metadata = get_metadata_param(params, "metadata");

        auto channel =
            grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
        UnaryCallKernel kernel;
        kernel.stub = std::make_shared<grpc::GenericStub>(std::move(channel));
        kernel.method = method;
        kernel.metadata = metadata;
        kernel.timeout = std::chrono::milliseconds(timeout_ms);
        kernel.wait_for_ready = wait_for_ready;
        kernel.authority = authority;
        return kernel;
      });
}

} // namespace sr::kernel

#endif // SR_ENGINE_WITH_GRPC
