#pragma once

// #ifdef SR_ENGINE_WITH_ARROW_FLIGHT

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/flight/api.h>

#include "engine/error.hpp"
#include "engine/registry.hpp"

namespace sr::kernel::flight {

/// Arrow Flight call kinds supported by the serve layer.
enum class FlightCallKind {
  DoAction,
  DoGet,
  DoPut,
  DoExchange,
};

/// Response sink interface used by flight output kernels.
class FlightResponder {
public:
  virtual ~FlightResponder() = default;
  /// Send action results back to the transport.
  virtual auto
  send_action_results(std::vector<arrow::flight::Result> results) noexcept
      -> sr::engine::Expected<void> = 0;
  /// Send record batches back to the transport.
  virtual auto
  send_record_batches(std::shared_ptr<arrow::RecordBatchReader> reader) noexcept
      -> sr::engine::Expected<void> = 0;
};

/// Per-request Flight call handle injected via env binding.
struct FlightServerCall {
  FlightCallKind kind = FlightCallKind::DoAction;
  std::optional<arrow::flight::Action> action;
  std::optional<arrow::flight::Ticket> ticket;
  std::optional<arrow::flight::FlightDescriptor> descriptor;
  std::shared_ptr<arrow::flight::FlightMessageReader> reader;
  std::shared_ptr<arrow::flight::FlightMessageWriter> writer;
  std::shared_ptr<arrow::flight::FlightMetadataWriter> metadata_writer;
  std::shared_ptr<FlightResponder> responder;
};

} // namespace sr::kernel::flight

namespace sr::kernel {

/// Register Arrow Flight value types used by flight kernels.
auto register_flight_types() -> void;

/// Register Flight input/output kernels into a registry.
auto register_flight_kernels(sr::engine::KernelRegistry &registry) -> void;

} // namespace sr::kernel

#endif // SR_ENGINE_WITH_ARROW_FLIGHT
