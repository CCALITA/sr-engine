#include "kernel/rpc_kernels.hpp"

#if SR_ENGINE_WITH_GRPC

#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <condition_variable>
#include <cstdlib>
#include <format>
#include <mutex>
#include <optional>
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
    const auto *ptr = reinterpret_cast<const char *>(slice.begin());
    data.append(ptr, slice.size());
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

constexpr std::size_t kShardHeaderSize =
    sizeof(std::uint32_t) * 2 + sizeof(std::uint64_t);

/// Append a little-endian 32-bit value to a byte string.
auto append_u32_le(std::string &out, std::uint32_t value) -> void {
  for (int i = 0; i < 4; ++i) {
    out.push_back(static_cast<char>(value & 0xFFu));
    value >>= 8;
  }
}

/// Append a little-endian 64-bit value to a byte string.
auto append_u64_le(std::string &out, std::uint64_t value) -> void {
  for (int i = 0; i < 8; ++i) {
    out.push_back(static_cast<char>(value & 0xFFu));
    value >>= 8;
  }
}

/// Read a 32-bit little-endian value from raw bytes.
auto read_u32_le(const unsigned char *data) -> std::uint32_t {
  std::uint32_t value = 0;
  for (int i = 3; i >= 0; --i) {
    value = (value << 8) | data[i];
  }
  return value;
}

/// Read a 64-bit little-endian value from raw bytes.
auto read_u64_le(const unsigned char *data) -> std::uint64_t {
  std::uint64_t value = 0;
  for (int i = 7; i >= 0; --i) {
    value = (value << 8) | data[i];
  }
  return value;
}

/// Encode a shard header into a ByteBuffer payload.
auto encode_shard_payload(std::uint32_t part_index,
                          std::uint32_t part_count, std::int64_t value)
    -> grpc::ByteBuffer {
  std::string data;
  data.reserve(kShardHeaderSize);
  append_u32_le(data, part_index);
  append_u32_le(data, part_count);
  append_u64_le(data, std::bit_cast<std::uint64_t>(value));
  return string_to_byte_buffer(data);
}

/// Decoded shard metadata extracted from an RPC payload.
struct DecodedShard {
  std::uint32_t index = 0;
  std::uint32_t count = 0;
  std::int64_t value = 0;
};

/// Decode a shard header from a ByteBuffer payload.
auto decode_shard_payload(const grpc::ByteBuffer &buffer)
    -> Expected<DecodedShard> {
  const auto data = byte_buffer_to_string(buffer);
  if (data.size() < kShardHeaderSize) {
    return tl::unexpected(
        sr::engine::make_error("shard payload too small"));
  }
  const auto *bytes =
      reinterpret_cast<const unsigned char *>(data.data());
  DecodedShard decoded;
  decoded.index = read_u32_le(bytes);
  decoded.count = read_u32_le(bytes + sizeof(std::uint32_t));
  decoded.value = std::bit_cast<std::int64_t>(
      read_u64_le(bytes + sizeof(std::uint32_t) * 2));
  return decoded;
}

/// Evenly split an int64 across a fixed number of parts.
auto split_i64_even(std::int64_t value, std::uint32_t parts)
    -> std::vector<std::int64_t> {
  std::vector<std::int64_t> out;
  out.resize(parts);
  if (parts == 0) {
    return out;
  }
  const auto divisor = static_cast<std::int64_t>(parts);
  const auto div = std::div(value, divisor);
  const auto base = div.quot;
  const auto rem = div.rem;
  const auto rem_count =
      static_cast<std::uint32_t>(rem >= 0 ? rem : -rem);
  const auto sign = rem >= 0 ? 1 : -1;
  for (std::uint32_t i = 0; i < parts; ++i) {
    out[i] = base + (i < rem_count ? sign : 0);
  }
  return out;
}

/// Perform a unary RPC call using grpc::GenericStub.
auto perform_unary_call(grpc::GenericStub &stub,
                        const std::string &method,
                        const rpc::RpcMetadata &metadata,
                        std::chrono::milliseconds timeout, bool wait_for_ready,
                        const std::string &authority,
                        const grpc::ByteBuffer &request)
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
  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;
  grpc::Status status;

  stub.UnaryCall(&context, method, grpc::StubOptions{}, &request, &response,
                 [&mutex, &cv, &done, &status](grpc::Status call_status) {
                   std::lock_guard<std::mutex> lock(mutex);
                   status = std::move(call_status);
                   done = true;
                   cv.notify_one();
                 });

  {
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait(lock, [&done]() { return done; });
  }

  if (!status.ok()) {
    return tl::unexpected(sr::engine::make_error(std::format(
        "rpc call failed: {} ({})", status.error_message(),
        static_cast<int>(status.error_code()))));
  }

  return response;
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
    if (!stub) {
      return tl::unexpected(
          sr::engine::make_error("rpc_unary_call missing stub"));
    }
    return perform_unary_call(*stub, method, metadata, timeout,
                              wait_for_ready, authority, request);
  }
};

struct BatchUnaryCallKernel {
  std::shared_ptr<grpc::GenericStub> stub;
  std::string method;
  rpc::RpcMetadata metadata;
  std::chrono::milliseconds timeout{0};
  bool wait_for_ready = false;
  std::string authority;

  /// Invoke a unary RPC for each payload in the batch.
  auto operator()(const std::vector<grpc::ByteBuffer> &requests) const noexcept
      -> Expected<std::vector<grpc::ByteBuffer>> {
    if (!stub) {
      return tl::unexpected(
          sr::engine::make_error("rpc_unary_call_batch missing stub"));
    }
    std::vector<grpc::ByteBuffer> responses;
    responses.reserve(requests.size());
    for (const auto &request : requests) {
      auto response = perform_unary_call(*stub, method, metadata, timeout,
                                         wait_for_ready, authority, request);
      if (!response) {
        return tl::unexpected(response.error());
      }
      responses.push_back(std::move(*response));
    }
    return responses;
  }
};

} // namespace

auto register_rpc_types() -> void {
  sr::engine::register_type<grpc::ByteBuffer>("grpc_byte_buffer");
  sr::engine::register_type<std::vector<grpc::ByteBuffer>>(
      "grpc_byte_buffer_vec");
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

  registry.register_kernel(
      "flatbuffer_echo_batch",
      [](const std::vector<grpc::ByteBuffer> &payloads) noexcept {
        std::vector<grpc::ByteBuffer> out;
        out.reserve(payloads.size());
        for (const auto &payload : payloads) {
          out.push_back(clone_byte_buffer(payload));
        }
        return out;
      });

  registry.register_kernel_with_params(
      "scatter_i64", [](const Json &params) {
        const auto parts = get_int_param(params, "parts", 2);
        return [parts](std::int64_t value) noexcept
                   -> Expected<std::vector<grpc::ByteBuffer>> {
          if (parts <= 0) {
            return tl::unexpected(sr::engine::make_error(
                "scatter_i64 requires parts > 0"));
          }
          const auto count = static_cast<std::uint32_t>(parts);
          const auto splits = split_i64_even(value, count);
          std::vector<grpc::ByteBuffer> shards;
          shards.reserve(count);
          for (std::uint32_t i = 0; i < count; ++i) {
            shards.push_back(encode_shard_payload(i, count, splits[i]));
          }
          return shards;
        };
      });

  registry.register_kernel(
      "gather_i64_sum",
      [](const std::vector<grpc::ByteBuffer> &payloads) noexcept
          -> Expected<std::int64_t> {
        if (payloads.empty()) {
          return tl::unexpected(sr::engine::make_error(
              "gather_i64_sum requires at least one payload"));
        }
        std::optional<std::uint32_t> expected_count;
        std::vector<bool> seen;
        std::int64_t sum = 0;
        for (const auto &payload : payloads) {
          auto decoded = decode_shard_payload(payload);
          if (!decoded) {
            return tl::unexpected(decoded.error());
          }
          if (!expected_count) {
            if (decoded->count == 0) {
              return tl::unexpected(sr::engine::make_error(
                  "gather_i64_sum shard count is zero"));
            }
            expected_count = decoded->count;
            seen.assign(*expected_count, false);
          }
          if (decoded->count != *expected_count) {
            return tl::unexpected(sr::engine::make_error(
                "gather_i64_sum shard count mismatch"));
          }
          if (decoded->index >= *expected_count) {
            return tl::unexpected(sr::engine::make_error(
                "gather_i64_sum shard index out of range"));
          }
          if (seen[decoded->index]) {
            return tl::unexpected(sr::engine::make_error(
                "gather_i64_sum duplicate shard index"));
          }
          seen[decoded->index] = true;
          sum += decoded->value;
        }
        if (!expected_count || payloads.size() != *expected_count) {
          return tl::unexpected(sr::engine::make_error(
              "gather_i64_sum shard count does not match payloads"));
        }
        for (bool present : seen) {
          if (!present) {
            return tl::unexpected(sr::engine::make_error(
                "gather_i64_sum missing shard"));
          }
        }
        return sum;
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

  registry.register_kernel_with_params(
      "rpc_unary_call_batch",
      [](const Json &params) -> Expected<BatchUnaryCallKernel> {
        if (!params.is_object()) {
          return tl::unexpected(sr::engine::make_error(
              "rpc_unary_call_batch expects params object"));
        }
        const auto target = get_string_param(params, "target", "");
        const auto method = get_string_param(params, "method", "");
        if (target.empty()) {
          return tl::unexpected(sr::engine::make_error(
              "rpc_unary_call_batch missing target"));
        }
        if (method.empty()) {
          return tl::unexpected(sr::engine::make_error(
              "rpc_unary_call_batch missing method"));
        }
        const auto timeout_ms = get_int_param(params, "timeout_ms", 0);
        const auto wait_for_ready =
            get_bool_param(params, "wait_for_ready", false);
        const auto authority = get_string_param(params, "authority", "");
        const auto metadata = get_metadata_param(params, "metadata");

        auto channel =
            grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
        BatchUnaryCallKernel kernel;
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
