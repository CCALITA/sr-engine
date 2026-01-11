#include "kernel/flight_kernels.hpp"

#ifdef SR_ENGINE_WITH_ARROW_FLIGHT

#include <tuple>

#include "engine/error.hpp"

namespace sr::kernel {
namespace {

using sr::engine::Expected;
using sr::engine::KernelRegistry;

} // namespace

auto register_flight_types() -> void {
  sr::engine::register_type<flight::FlightCallKind>("flight_call_kind");
  sr::engine::register_type<arrow::flight::Action>("flight_action_value");
  sr::engine::register_type<std::optional<arrow::flight::Action>>(
      "flight_action");
  sr::engine::register_type<arrow::flight::Ticket>("flight_ticket_value");
  sr::engine::register_type<std::optional<arrow::flight::Ticket>>(
      "flight_ticket");
  sr::engine::register_type<arrow::flight::FlightDescriptor>(
      "flight_descriptor_value");
  sr::engine::register_type<std::optional<arrow::flight::FlightDescriptor>>(
      "flight_descriptor");
  sr::engine::register_type<
      std::shared_ptr<arrow::flight::FlightMessageReader>>("flight_reader");
  sr::engine::register_type<
      std::shared_ptr<arrow::flight::FlightMessageWriter>>("flight_writer");
  sr::engine::register_type<
      std::shared_ptr<arrow::flight::FlightMetadataWriter>>(
      "flight_metadata_writer");
  sr::engine::register_type<std::shared_ptr<arrow::RecordBatchReader>>(
      "record_batch_reader");
  sr::engine::register_type<std::vector<arrow::flight::Result>>(
      "flight_results");
  sr::engine::register_type<flight::FlightServerCall>("flight_server_call");
}

auto register_flight_kernels(KernelRegistry &registry) -> void {
  register_flight_types();

  registry.register_kernel(
      "flight_server_input", [](const flight::FlightServerCall &call) noexcept {
        return std::make_tuple(call.kind, call.action, call.ticket,
                               call.descriptor, call.reader, call.writer,
                               call.metadata_writer);
      });

  registry.register_kernel(
      "flight_action_output",
      [](const flight::FlightServerCall &call,
         const std::vector<arrow::flight::Result> &results) noexcept
          -> Expected<void> {
        if (call.kind != flight::FlightCallKind::DoAction) {
          return tl::unexpected(sr::engine::make_error(
              "flight_action_output used for non-action call"));
        }
        if (!call.responder) {
          return tl::unexpected(
              sr::engine::make_error("flight responder missing"));
        }
        return call.responder->send_action_results(results);
      });

  registry.register_kernel(
      "flight_get_output",
      [](const flight::FlightServerCall &call,
         const std::shared_ptr<arrow::RecordBatchReader> &reader) noexcept
          -> Expected<void> {
        if (call.kind != flight::FlightCallKind::DoGet) {
          return tl::unexpected(sr::engine::make_error(
              "flight_get_output used for non-do_get call"));
        }
        if (!call.responder) {
          return tl::unexpected(
              sr::engine::make_error("flight responder missing"));
        }
        if (!reader) {
          return tl::unexpected(
              sr::engine::make_error("flight reader missing"));
        }
        return call.responder->send_record_batches(reader);
      });
}

} // namespace sr::kernel

#endif // SR_ENGINE_WITH_ARROW_FLIGHT
