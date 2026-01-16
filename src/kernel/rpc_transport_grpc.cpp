#include "kernel/rpc_policies.hpp"
#include "kernel/rpc_kernels.hpp"

#include "engine/error.hpp"
#include "engine/types.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <stdexec/execution.hpp>

namespace sr::kernel::rpc {

namespace {

auto byte_buffer_to_vector(const grpc::ByteBuffer& buffer) -> std::vector<std::byte> {
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

auto vector_to_byte_buffer(const std::vector<std::byte>& data) -> grpc::ByteBuffer {
  grpc::Slice slice(reinterpret_cast<const char*>(data.data()), data.size());
  return grpc::ByteBuffer(&slice, 1);
}

} // namespace

struct GrpcTransport {
  std::shared_ptr<grpc::GenericStub> stub;

  auto send(const Envelope& request) -> engine::Expected<Envelope> {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));

    for (const auto& entry : request.metadata.entries) {
      context.AddMetadata(entry.key, entry.value);
    }

    grpc::ByteBuffer request_buf = vector_to_byte_buffer(request.payload);
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
};

class GrpcTransportFactory {
public:
  static auto create(const std::string& target, bool use_tls)
      -> engine::Expected<std::unique_ptr<GrpcTransport>> {
    std::shared_ptr<grpc::ChannelCredentials> creds =
        use_tls ? grpc::SslCredentials(grpc::SslCredentialsOptions())
                : grpc::InsecureChannelCredentials();

    auto channel = grpc::CreateChannel(target, creds);
    auto stub = std::make_shared<grpc::GenericStub>(std::move(channel));

    return std::make_unique<GrpcTransport>(GrpcTransport{std::move(stub)});
  }
};

} // namespace sr::kernel::rpc
