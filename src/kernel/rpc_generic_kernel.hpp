#ifndef SR_RPC_GENERIC_KERNEL_HPP
#define SR_RPC_GENERIC_KERNEL_HPP

#include "kernel/rpc_policies.hpp"
#include "kernel/rpc_kernels.hpp"

#include "engine/error.hpp"
#include "engine/types.hpp"

#include <atomic>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <tuple>
#include <type_traits>
#include <vector>

namespace sr::kernel::rpc {

struct GrpcTransport {
  std::shared_ptr<grpc::GenericStub> stub;

  auto send(const Envelope& request) -> engine::Expected<Envelope> {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));

    for (const auto& entry : request.metadata.entries) {
      context.AddMetadata(entry.key, entry.value);
    }

    auto request_buf = vector_to_byte_buffer(request.payload);
    grpc::ByteBuffer response_buf;

    std::mutex mutex;
    std::condition_variable cv;
    bool done = false;
    grpc::Status status;

    stub->UnaryCall(&context, request.method, grpc::StubOptions{}, &request_buf,
                    &response_buf, [&mutex, &cv, &done, &status](grpc::Status s) {
                      std::lock_guard<std::mutex> lock(mutex);
                      status = std::move(s);
                      done = true;
                      cv.notify_one();
                    });

    {
      std::unique_lock<std::mutex> lock(mutex);
      cv.wait(lock, [&done]() { return done; });
    }

    if (!status.ok()) {
      return tl::unexpected(engine::make_error(
          std::format("gRPC call failed: {} ({})", status.error_message(),
                      static_cast<int>(status.error_code()))));
    }

    Envelope response;
    response.method = request.method;
    response.payload = byte_buffer_to_vector(response_buf);
    return response;
  }

  auto send_batch(const std::vector<Envelope>& requests)
      -> engine::Expected<std::vector<Envelope>> {
    std::vector<engine::Expected<Envelope>> results;
    results.reserve(requests.size());

    for (const auto& request : requests) {
      results.push_back(send(request));
    }

    std::vector<Envelope> responses;
    responses.reserve(requests.size());

    for (auto& result : results) {
      if (!result) {
        return tl::unexpected(result.error());
      }
      responses.push_back(std::move(*result));
    }

    return responses;
  }

  auto connected() const -> bool { return true; }

  auto health_check() -> engine::Expected<bool> {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

    grpc::ByteBuffer request_buf;
    grpc::ByteBuffer response_buf;
    grpc::Status status;

    stub->UnaryCall(&context, "/grpc.health.v1.Health/Check",
                    grpc::StubOptions{}, &request_buf, &response_buf,
                    [&status](grpc::Status s) { status = std::move(s); });

    if (!status.ok()) {
      return tl::unexpected(engine::make_error(
          std::format("health check failed: {} ({})", status.error_message(),
                      static_cast<int>(status.error_code()))));
    }

    return true;
  }

private:
  static auto vector_to_byte_buffer(const std::vector<std::byte>& data) -> grpc::ByteBuffer {
    grpc::Slice slice(reinterpret_cast<const char*>(data.data()), data.size());
    return grpc::ByteBuffer(&slice, 1);
  }

  static auto byte_buffer_to_vector(const grpc::ByteBuffer& buffer) -> std::vector<std::byte> {
    std::vector<std::byte> result;
    std::vector<grpc::Slice> slices;
    buffer.Dump(&slices);
    result.reserve(buffer.Length());
    for (const auto& slice : slices) {
      const auto* data = reinterpret_cast<const std::byte*>(slice.begin());
      result.insert(result.end(), data, data + slice.size());
    }
    return result;
  }
};

template <Transport TransportT, Serializer SerializerT, typename... MiddlewareTs>
requires(Middleware<MiddlewareTs> && ...)
struct GenericRpcKernel {
  std::unique_ptr<TransportT> transport;
  SerializerT serializer;
  MiddlewareChain<MiddlewareTs...> middleware;
  std::string method;

  template <typename Input>
  auto operator()(const Input& input) -> engine::Expected<engine::Json> {
    auto serialized = serializer.serialize(input);
    if (!serialized) {
      return tl::unexpected(serialized.error());
    }

    Envelope request;
    request.method = method;
    request.payload = std::move(*serialized);

    auto modified = middleware.before_send(request);
    if (!modified) {
      return tl::unexpected(modified.error());
    }

    auto response = transport->send(*modified);
    if (!response) {
      return tl::unexpected(response.error());
    }

    auto after = middleware.after_receive(*response);
    if (!after) {
      return tl::unexpected(after.error());
    }

    auto deserialized = serializer.deserialize(after->payload);
    if (!deserialized) {
      return tl::unexpected(deserialized.error());
    }

    return *deserialized;
  }

  template <typename Input>
  auto operator()(const std::vector<Input>& inputs)
      -> engine::Expected<std::vector<engine::Json>> {
    std::vector<engine::Expected<engine::Json>> results;
    results.reserve(inputs.size());

    for (const auto& input : inputs) {
      results.push_back((*this)(input));
    }

    std::vector<engine::Json> responses;
    responses.reserve(inputs.size());

    for (auto& result : results) {
      if (!result) {
        return tl::unexpected(result.error());
      }
      responses.push_back(std::move(*result));
    }

    return responses;
  }
};

} // namespace sr::kernel::rpc

#endif // SR_RPC_GENERIC_KERNEL_HPP
