#include "kernel/rpc_kernels.hpp"
#include "engine/error.hpp"
#include "engine/type_names.hpp"

#include <algorithm>
#include <atomic>
#include <bit>
#include <chrono>
#include <condition_variable>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <format>
#include <future>
#include <latch>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include <exec/async_scope.hpp>
#include <exec/static_thread_pool.hpp>
#include <stdexec/execution.hpp>

namespace sr::kernel {
namespace {

std::unique_ptr<exec::static_thread_pool> g_rpc_thread_pool;
std::once_flag g_rpc_pool_init;

auto get_rpc_thread_pool() -> exec::static_thread_pool & {
  std::call_once(g_rpc_pool_init, []() {
    g_rpc_thread_pool = std::make_unique<exec::static_thread_pool>(
        std::max(2u, std::thread::hardware_concurrency()));
  });
  return *g_rpc_thread_pool;
}

auto get_rpc_scheduler() {
  return get_rpc_thread_pool().get_scheduler();
}

template <typename F>
auto parallel_for(std::size_t count, F &&func) -> void {
  if (count == 0) {
    return;
  }

  auto scheduler = get_rpc_scheduler();

  stdexec::sender auto sender =
      stdexec::on(scheduler, stdexec::just()) |
      stdexec::bulk(stdexec::par, count, [&func](std::size_t index) {
        func(index);
      });

  stdexec::sync_wait(std::move(sender));
}

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

auto get_bool_param(const Json &params, const char *key, bool fallback) -> bool {
  if (!params.is_object()) {
    return fallback;
  }
  auto it = params.find(key);
  if (it != params.end() && it->is_boolean()) {
    return it->get<bool>();
  }
  return fallback;
}

auto get_string_param(const Json &params, const char *key,
                      std::string fallback) -> std::string {
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
  return string_to_byte_buffer(data);
}

struct DecodedShard {
  std::uint32_t index = 0;
  std::uint32_t count = 0;
  std::int64_t value = 0;
};

auto decode_shard_payload(const grpc::ByteBuffer &buffer)
    -> Expected<DecodedShard> {
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
  const auto rem_count = static_cast<std::uint32_t>(rem >= 0 ? rem : -rem);
  const auto sign = rem >= 0 ? 1 : -1;
  for (std::uint32_t i = 0; i < parts; ++i) {
    out[i] = base + (i < rem_count ? sign : 0);
  }
  return out;
}

struct RetryConfig {
  std::uint32_t max_attempts = 3;
  std::chrono::milliseconds base_delay = std::chrono::milliseconds(100);
  double max_delay_factor = 4.0;
  std::vector<grpc::StatusCode> retryable_codes = {
      grpc::StatusCode::UNAVAILABLE,
      grpc::StatusCode::DEADLINE_EXCEEDED,
      grpc::StatusCode::INTERNAL,
  };
};

struct RpcMetrics {
  std::atomic<std::uint64_t> total_requests{0};
  std::atomic<std::uint64_t> successful_requests{0};
  std::atomic<std::uint64_t> failed_requests{0};
  std::atomic<std::uint64_t> retries_total{0};
  std::atomic<std::uint64_t> retries_success{0};
  std::atomic<std::chrono::microseconds::rep> total_latency_us{0};

  auto record_request(std::chrono::microseconds latency, bool success,
                      std::uint32_t retries) -> void {
    ++total_requests;
    if (success) {
      ++successful_requests;
    } else {
      ++failed_requests;
    }
    retries_total += retries;
    if (success && retries > 0) {
      ++retries_success;
    }
    total_latency_us.fetch_add(latency.count(), std::memory_order_relaxed);
  }

  auto average_latency_ms() const -> double {
    auto total = total_requests.load(std::memory_order_acquire);
    if (total == 0) {
      return 0.0;
    }
    auto latency_us =
        total_latency_us.load(std::memory_order_acquire);
    return static_cast<double>(latency_us) / 1000.0 /
           static_cast<double>(total);
  }

  auto success_rate() const -> double {
    auto total = total_requests.load(std::memory_order_acquire);
    if (total == 0) {
      return 1.0;
    }
    auto success =
        successful_requests.load(std::memory_order_acquire);
    return static_cast<double>(success) / static_cast<double>(total);
  }
};

struct KeepaliveConfig {
  std::chrono::seconds keepalive_time = std::chrono::seconds(30);
  std::chrono::seconds keepalive_timeout = std::chrono::seconds(10);
  std::uint32_t keepalive_permit_without_calls = 1;
  std::uint32_t max_pings_without_data = 2;
};

auto build_channel_args(const KeepaliveConfig &keepalive)
    -> grpc::ChannelArguments {
  grpc::ChannelArguments args;
  args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS,
              static_cast<int>(keepalive.keepalive_time.count() * 1000));
  args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS,
              static_cast<int>(keepalive.keepalive_timeout.count() * 1000));
  args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS,
              static_cast<int>(keepalive.keepalive_permit_without_calls));
  return args;
}

auto perform_health_check(grpc::GenericStub &stub, const std::string &service)
    -> Expected<bool> {
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(5));

  grpc::ByteBuffer request;
  grpc::ByteBuffer response;
  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;
  grpc::Status status;

  auto method = std::format("/{}/HealthCheck", service);
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
        "health check failed: {} ({})", status.error_message(),
        static_cast<int>(status.error_code()))));
  }

  return true;
}

auto is_retryable(const RetryConfig &config, grpc::StatusCode code) -> bool {
  return std::find(config.retryable_codes.begin(), config.retryable_codes.end(),
                   code) != config.retryable_codes.end();
}

auto calculate_backoff(const RetryConfig &config, std::uint32_t attempt)
    -> std::chrono::milliseconds {
  double factor = std::min(std::pow(2.0, static_cast<double>(attempt)),
                           config.max_delay_factor);
  auto delay_ms = static_cast<std::int64_t>(config.base_delay.count() * factor);
  return std::chrono::milliseconds(delay_ms);
}

auto perform_unary_call(grpc::GenericStub &stub, const std::string &method,
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
    return tl::unexpected(sr::engine::make_error(
        std::format("rpc call failed: {} ({})", status.error_message(),
                    static_cast<int>(status.error_code()))));
  }

  return response;
}

auto perform_unary_call_with_retry(
    grpc::GenericStub &stub, const std::string &method,
    const rpc::RpcMetadata &metadata, std::chrono::milliseconds timeout,
    bool wait_for_ready, const std::string &authority,
    const grpc::ByteBuffer &request, const RetryConfig &retry)
    -> Expected<grpc::ByteBuffer> {
  std::uint32_t attempt = 0;

  while (attempt < retry.max_attempts) {
    auto result = perform_unary_call(stub, method, metadata, timeout,
                                     wait_for_ready, authority, request);

    if (result) {
      return result;
    }

    if (attempt + 1 < retry.max_attempts) {
      grpc::StatusCode code = static_cast<grpc::StatusCode>(0);
      auto status = result.error().message;
      if (!status.empty()) {
        auto code_start = status.rfind("(");
        auto code_end = status.rfind(")");
        if (code_start != std::string::npos && code_end != std::string::npos &&
            code_end > code_start + 1) {
          auto code_str = status.substr(code_start + 1, code_end - code_start - 1);
          code = static_cast<grpc::StatusCode>(std::stoi(code_str));
        }
      }

      if (is_retryable(retry, code)) {
        auto backoff = calculate_backoff(retry, attempt);
        std::this_thread::sleep_for(backoff);
        ++attempt;
        continue;
      }
    }

    return tl::unexpected(result.error());
  }

  return tl::unexpected(
      sr::engine::make_error("rpc call failed: max attempts exceeded"));
}

struct ServerOutputKernel {
  rpc::RpcStatus status;

  auto operator()(const rpc::RpcServerCall &call,
                  const grpc::ByteBuffer &payload) const noexcept
      -> Expected<void> {
    if (!static_cast<bool>(call.responder)) {
      return tl::unexpected(
          sr::engine::make_error("rpc responder missing on server call"));
    }
    rpc::RpcResponse response;
    response.payload = clone_byte_buffer(payload);
    response.status = status;
    return call.responder.send(std::move(response));
  }
};

struct UnaryCallKernel {
  std::shared_ptr<grpc::GenericStub> stub;
  std::string method;
  rpc::RpcMetadata metadata;
  std::chrono::milliseconds timeout{0};
  bool wait_for_ready = false;
  std::string authority;
  RetryConfig retry;

  auto operator()(const grpc::ByteBuffer &request) const noexcept
      -> Expected<grpc::ByteBuffer> {
    if (!stub) {
      return tl::unexpected(
          sr::engine::make_error("rpc_unary_call missing stub"));
    }
    return perform_unary_call_with_retry(*stub, method, metadata, timeout,
                                         wait_for_ready, authority, request,
                                         retry);
  }
};

struct BatchUnaryCallKernel {
  std::shared_ptr<grpc::GenericStub> stub;
  std::string method;
  rpc::RpcMetadata metadata;
  std::chrono::milliseconds timeout{0};
  bool wait_for_ready = false;
  std::string authority;
  RetryConfig retry;
  bool parallel = false;

  auto operator()(const std::vector<grpc::ByteBuffer> &requests) const noexcept
      -> Expected<std::vector<grpc::ByteBuffer>> {
    if (!stub) {
      return tl::unexpected(
          sr::engine::make_error("rpc_unary_call_batch missing stub"));
    }

    if (!parallel || requests.size() <= 1) {
      std::vector<grpc::ByteBuffer> responses;
      responses.reserve(requests.size());
      for (const auto &request : requests) {
        auto result = perform_unary_call_with_retry(
            *stub, method, metadata, timeout, wait_for_ready, authority,
            request, retry);
        if (!result) {
          return tl::unexpected(result.error());
        }
        responses.push_back(std::move(*result));
      }
      return responses;
    }

    const auto n = requests.size();
    std::vector<Expected<grpc::ByteBuffer>> results(n);
    std::atomic<bool> has_error{false};

    parallel_for(n, [this, &requests, &results, &has_error, n](std::size_t index) {
      if (index >= n) {
        return;
      }
      results[index] = perform_unary_call_with_retry(
          *stub, method, metadata, timeout, wait_for_ready, authority,
          requests[index], retry);
      if (!results[index]) {
        has_error.store(true, std::memory_order_release);
      }
    });

    if (has_error.load(std::memory_order_acquire)) {
      for (const auto &result : results) {
        if (!result) {
          return tl::unexpected(result.error());
        }
      }
    }

    std::vector<grpc::ByteBuffer> responses;
    responses.reserve(n);
    for (auto &result : results) {
      responses.push_back(std::move(*result));
    }

    return responses;
  }
};

struct HealthCheckKernel {
  std::shared_ptr<grpc::GenericStub> stub;
  std::string service;

  auto operator()() const noexcept -> Expected<bool> {
    if (!stub) {
      return tl::unexpected(
          sr::engine::make_error("rpc_health_check missing stub"));
    }
    return perform_health_check(*stub, service);
  }
};

struct MetricsKernel {
  std::shared_ptr<RpcMetrics> metrics;

  auto operator()() const noexcept -> Json {
    Json result = Json::object();
    result["total_requests"] = metrics->total_requests.load(std::memory_order_acquire);
    result["successful_requests"] = metrics->successful_requests.load(std::memory_order_acquire);
    result["failed_requests"] = metrics->failed_requests.load(std::memory_order_acquire);
    result["retries_total"] = metrics->retries_total.load(std::memory_order_acquire);
    result["retries_success"] = metrics->retries_success.load(std::memory_order_acquire);
    result["average_latency_ms"] = metrics->average_latency_ms();
    result["success_rate"] = metrics->success_rate();
    return result;
  }
};

} // namespace

auto register_rpc_types(sr::engine::TypeRegistry &registry) -> void {
  sr::engine::register_type<grpc::ByteBuffer>(registry, "grpc_byte_buffer");
  sr::engine::register_type<std::vector<grpc::ByteBuffer>>(
      registry, "grpc_byte_buffer_vec");
  sr::engine::register_type<rpc::RpcMetadata>(registry, "rpc_metadata");
  sr::engine::register_type<rpc::RpcServerCall>(registry, "rpc_server_call");
  sr::engine::register_type<Json>(registry, "json");
  sr::engine::register_type<std::shared_ptr<RpcMetrics>>(registry, "rpc_metrics");
}

auto register_rpc_kernels(KernelRegistry &registry) -> void {
  register_rpc_types(*registry.type_registry());

  registry.register_kernel(
      "rpc_server_input", [](const rpc::RpcServerCall &call) noexcept {
        return std::tuple(call.method, clone_byte_buffer(call.request),
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

  registry.register_kernel_with_params("scatter_i64", [](const Json &params) {
    const auto parts = get_int_param(params, "parts", 2);
    return [parts](std::int64_t value) noexcept
               -> Expected<std::vector<grpc::ByteBuffer>> {
      if (parts <= 0) {
        return tl::unexpected(
            sr::engine::make_error("scatter_i64 requires parts > 0"));
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
              return tl::unexpected(
                  sr::engine::make_error("gather_i64_sum shard count is zero"));
            }
            expected_count = decoded->count;
            seen.assign(*expected_count, false);
          }
          if (decoded->count != *expected_count) {
            return tl::unexpected(
                sr::engine::make_error("gather_i64_sum shard count mismatch"));
          }
          if (decoded->index >= *expected_count) {
            return tl::unexpected(sr::engine::make_error(
                "gather_i64_sum shard index out of range"));
          }
          if (seen[decoded->index]) {
            return tl::unexpected(
                sr::engine::make_error("gather_i64_sum duplicate shard index"));
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
            return tl::unexpected(
                sr::engine::make_error("gather_i64_sum missing shard"));
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

        auto creds = grpc::InsecureChannelCredentials();
        const auto use_tls = get_bool_param(params, "use_tls", false);
        if (use_tls) {
          creds = grpc::SslCredentials(grpc::SslCredentialsOptions());
        }

        auto channel = grpc::CreateChannel(target, std::move(creds));
        UnaryCallKernel kernel;
        kernel.stub = std::make_shared<grpc::GenericStub>(std::move(channel));
        kernel.method = method;
        kernel.metadata = metadata;
        kernel.timeout = std::chrono::milliseconds(timeout_ms);
        kernel.wait_for_ready = wait_for_ready;
        kernel.authority = authority;
        kernel.retry.max_attempts =
            static_cast<std::uint32_t>(get_int_param(params, "retry_attempts", 3));
        kernel.retry.base_delay = std::chrono::milliseconds(
            get_int_param(params, "retry_base_delay_ms", 100));

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
          return tl::unexpected(
              sr::engine::make_error("rpc_unary_call_batch missing target"));
        }
        if (method.empty()) {
          return tl::unexpected(
              sr::engine::make_error("rpc_unary_call_batch missing method"));
        }
        const auto timeout_ms = get_int_param(params, "timeout_ms", 0);
        const auto wait_for_ready =
            get_bool_param(params, "wait_for_ready", false);
        const auto authority = get_string_param(params, "authority", "");
        const auto metadata = get_metadata_param(params, "metadata");
        const auto parallel =
            get_bool_param(params, "parallel_batch", false);

        auto creds = grpc::InsecureChannelCredentials();
        const auto use_tls = get_bool_param(params, "use_tls", false);
        if (use_tls) {
          creds = grpc::SslCredentials(grpc::SslCredentialsOptions());
        }

        auto channel = grpc::CreateChannel(target, std::move(creds));
        BatchUnaryCallKernel kernel;
        kernel.stub = std::make_shared<grpc::GenericStub>(std::move(channel));
        kernel.method = method;
        kernel.metadata = metadata;
        kernel.timeout = std::chrono::milliseconds(timeout_ms);
        kernel.wait_for_ready = wait_for_ready;
        kernel.authority = authority;
        kernel.parallel = parallel;
        kernel.retry.max_attempts =
            static_cast<std::uint32_t>(get_int_param(params, "retry_attempts", 3));
        kernel.retry.base_delay = std::chrono::milliseconds(
            get_int_param(params, "retry_base_delay_ms", 100));

        return kernel;
      });

  registry.register_kernel_with_params(
      "rpc_health_check",
      [](const Json &params) -> Expected<HealthCheckKernel> {
        if (!params.is_object()) {
          return tl::unexpected(
              sr::engine::make_error("rpc_health_check expects params object"));
        }
        const auto target = get_string_param(params, "target", "");
        const auto service = get_string_param(params, "service", "");
        if (target.empty()) {
          return tl::unexpected(
              sr::engine::make_error("rpc_health_check missing target"));
        }

        auto creds = grpc::InsecureChannelCredentials();
        const auto use_tls = get_bool_param(params, "use_tls", false);
        if (use_tls) {
          creds = grpc::SslCredentials(grpc::SslCredentialsOptions());
        }

        auto channel = grpc::CreateChannel(target, std::move(creds));
        HealthCheckKernel kernel;
        kernel.stub = std::make_shared<grpc::GenericStub>(std::move(channel));
        kernel.service = service;
        return kernel;
      });

  registry.register_kernel_with_params(
      "rpc_metrics",
      [](const Json &params) -> Expected<MetricsKernel> {
        auto metrics = std::make_shared<RpcMetrics>();
        return MetricsKernel{.metrics = std::move(metrics)};
      });
}

} // namespace sr::kernel
