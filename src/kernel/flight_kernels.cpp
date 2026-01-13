#include "kernel/flight_kernels.hpp"

#ifdef SR_ENGINE_WITH_ARROW_FLIGHT

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <format>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include "engine/error.hpp"

namespace sr::kernel {
namespace {

using sr::engine::Expected;
using sr::engine::KernelRegistry;

/// Computed totals for an invoice payload.
struct InvoiceTotals {
  std::int64_t item_count = 0;
  std::int64_t quantity_total = 0;
  double subtotal = 0.0;
  double discount_total = 0.0;
  double tax_total = 0.0;
  double total = 0.0;
};

/// Resolved field positions for invoice inputs.
struct InvoiceFieldIndices {
  int quantity = -1;
  int unit_price = -1;
  int discount = -1;
  int tax_rate = -1;
};

/// Round money values to two decimal places.
auto round_money(double value) -> double {
  return std::round(value * 100.0) / 100.0;
}

/// Normalize a percentage value into a [0,1] fraction.
auto normalize_rate(double rate) -> Expected<double> {
  if (rate < 0.0) {
    return tl::unexpected(
        sr::engine::make_error("tax_rate must be non-negative"));
  }
  if (rate > 1.0) {
    rate /= 100.0;
  }
  if (rate > 1.0) {
    return tl::unexpected(sr::engine::make_error("tax_rate exceeds 100%"));
  }
  return rate;
}

/// Resolve a required field index by name.
auto find_field_index(const std::shared_ptr<arrow::Schema> &schema,
                      std::initializer_list<std::string_view> names,
                      std::string_view label) -> Expected<int> {
  if (!schema) {
    return tl::unexpected(sr::engine::make_error("invoice schema missing"));
  }
  for (auto name : names) {
    const int index = schema->GetFieldIndex(std::string(name));
    if (index >= 0) {
      return index;
    }
  }
  return tl::unexpected(sr::engine::make_error(
      std::format("invoice missing required column: {}", label)));
}

/// Resolve an optional field index by name.
auto optional_field_index(const std::shared_ptr<arrow::Schema> &schema,
                          std::initializer_list<std::string_view> names)
    -> int {
  if (!schema) {
    return -1;
  }
  for (auto name : names) {
    const int index = schema->GetFieldIndex(std::string(name));
    if (index >= 0) {
      return index;
    }
  }
  return -1;
}

/// Read an int64 value from an Arrow array.
auto fetch_int64(const std::shared_ptr<arrow::Array> &array, int64_t row,
                 std::string_view label) -> Expected<std::int64_t> {
  if (!array) {
    return tl::unexpected(sr::engine::make_error(
        std::format("invoice {} column missing", label)));
  }
  if (array->IsNull(row)) {
    return tl::unexpected(
        sr::engine::make_error(std::format("invoice {} value is null", label)));
  }
  switch (array->type_id()) {
  case arrow::Type::INT64:
    return std::static_pointer_cast<arrow::Int64Array>(array)->Value(row);
  case arrow::Type::INT32:
    return static_cast<std::int64_t>(
        std::static_pointer_cast<arrow::Int32Array>(array)->Value(row));
  default:
    return tl::unexpected(sr::engine::make_error(
        std::format("invoice {} column must be int32 or int64 (got {})", label,
                    array->type()->ToString())));
  }
}

/// Read a double value from an Arrow array.
auto fetch_double(const std::shared_ptr<arrow::Array> &array, int64_t row,
                  std::string_view label) -> Expected<double> {
  if (!array) {
    return tl::unexpected(sr::engine::make_error(
        std::format("invoice {} column missing", label)));
  }
  if (array->IsNull(row)) {
    return tl::unexpected(
        sr::engine::make_error(std::format("invoice {} value is null", label)));
  }
  switch (array->type_id()) {
  case arrow::Type::DOUBLE:
    return std::static_pointer_cast<arrow::DoubleArray>(array)->Value(row);
  case arrow::Type::FLOAT:
    return static_cast<double>(
        std::static_pointer_cast<arrow::FloatArray>(array)->Value(row));
  case arrow::Type::INT64:
    return static_cast<double>(
        std::static_pointer_cast<arrow::Int64Array>(array)->Value(row));
  case arrow::Type::INT32:
    return static_cast<double>(
        std::static_pointer_cast<arrow::Int32Array>(array)->Value(row));
  default:
    return tl::unexpected(sr::engine::make_error(
        std::format("invoice {} column must be numeric (got {})", label,
                    array->type()->ToString())));
  }
}

/// Resolve input fields for invoice calculations.
auto resolve_invoice_fields(const std::shared_ptr<arrow::Schema> &schema)
    -> Expected<InvoiceFieldIndices> {
  InvoiceFieldIndices fields;
  auto quantity = find_field_index(schema, {"quantity", "qty"}, "quantity");
  if (!quantity) {
    return tl::unexpected(quantity.error());
  }
  fields.quantity = *quantity;
  auto unit_price =
      find_field_index(schema, {"unit_price", "unitPrice"}, "unit_price");
  if (!unit_price) {
    return tl::unexpected(unit_price.error());
  }
  fields.unit_price = *unit_price;
  fields.discount = optional_field_index(schema, {"discount", "line_discount"});
  fields.tax_rate = optional_field_index(schema, {"tax_rate", "taxRate"});
  return fields;
}

/// Accumulate invoice totals from a record batch.
auto accumulate_invoice_totals(const arrow::RecordBatch &batch,
                               const InvoiceFieldIndices &fields,
                               InvoiceTotals &totals) -> Expected<void> {
  const auto qty_array = batch.column(fields.quantity);
  const auto price_array = batch.column(fields.unit_price);
  const auto discount_array =
      fields.discount >= 0 ? batch.column(fields.discount) : nullptr;
  const auto tax_array =
      fields.tax_rate >= 0 ? batch.column(fields.tax_rate) : nullptr;

  for (int64_t row = 0; row < batch.num_rows(); ++row) {
    auto qty_value = fetch_int64(qty_array, row, "quantity");
    if (!qty_value) {
      return tl::unexpected(qty_value.error());
    }
    if (*qty_value <= 0) {
      return tl::unexpected(
          sr::engine::make_error("invoice quantity must be positive"));
    }
    auto price_value = fetch_double(price_array, row, "unit_price");
    if (!price_value) {
      return tl::unexpected(price_value.error());
    }
    if (*price_value < 0.0) {
      return tl::unexpected(
          sr::engine::make_error("invoice unit_price must be non-negative"));
    }
    double discount_value = 0.0;
    if (discount_array) {
      auto discount = fetch_double(discount_array, row, "discount");
      if (!discount) {
        return tl::unexpected(discount.error());
      }
      if (*discount < 0.0) {
        return tl::unexpected(
            sr::engine::make_error("invoice discount must be non-negative"));
      }
      discount_value = *discount;
    }
    double tax_rate_value = 0.0;
    if (tax_array) {
      auto tax_rate = fetch_double(tax_array, row, "tax_rate");
      if (!tax_rate) {
        return tl::unexpected(tax_rate.error());
      }
      auto normalized = normalize_rate(*tax_rate);
      if (!normalized) {
        return tl::unexpected(normalized.error());
      }
      tax_rate_value = *normalized;
    }

    const double line_subtotal = static_cast<double>(*qty_value) * *price_value;
    const double line_discount = std::min(discount_value, line_subtotal);
    const double taxable = line_subtotal - line_discount;
    const double line_tax = taxable * tax_rate_value;

    totals.item_count += 1;
    totals.quantity_total += *qty_value;
    totals.subtotal += line_subtotal;
    totals.discount_total += line_discount;
    totals.tax_total += line_tax;
  }
  return {};
}

/// Build a single-row RecordBatch with invoice totals.
auto make_totals_batch(const InvoiceTotals &totals)
    -> Expected<std::shared_ptr<arrow::RecordBatch>> {
  auto schema = arrow::schema({
      arrow::field("item_count", arrow::int64()),
      arrow::field("quantity_total", arrow::int64()),
      arrow::field("subtotal", arrow::float64()),
      arrow::field("discount_total", arrow::float64()),
      arrow::field("tax_total", arrow::float64()),
      arrow::field("total", arrow::float64()),
  });

  arrow::Int64Builder item_count_builder;
  arrow::Int64Builder quantity_builder;
  arrow::DoubleBuilder subtotal_builder;
  arrow::DoubleBuilder discount_builder;
  arrow::DoubleBuilder tax_builder;
  arrow::DoubleBuilder total_builder;

  auto append = [](auto &builder, auto value) -> Expected<void> {
    auto status = builder.Append(value);
    if (!status.ok()) {
      return tl::unexpected(sr::engine::make_error(status.ToString()));
    }
    return {};
  };

  if (auto ok = append(item_count_builder, totals.item_count); !ok) {
    return tl::unexpected(ok.error());
  }
  if (auto ok = append(quantity_builder, totals.quantity_total); !ok) {
    return tl::unexpected(ok.error());
  }
  if (auto ok = append(subtotal_builder, totals.subtotal); !ok) {
    return tl::unexpected(ok.error());
  }
  if (auto ok = append(discount_builder, totals.discount_total); !ok) {
    return tl::unexpected(ok.error());
  }
  if (auto ok = append(tax_builder, totals.tax_total); !ok) {
    return tl::unexpected(ok.error());
  }
  if (auto ok = append(total_builder, totals.total); !ok) {
    return tl::unexpected(ok.error());
  }

  std::shared_ptr<arrow::Array> item_count;
  std::shared_ptr<arrow::Array> quantity_total;
  std::shared_ptr<arrow::Array> subtotal;
  std::shared_ptr<arrow::Array> discount_total;
  std::shared_ptr<arrow::Array> tax_total;
  std::shared_ptr<arrow::Array> total;

  if (auto status = item_count_builder.Finish(&item_count); !status.ok()) {
    return tl::unexpected(sr::engine::make_error(status.ToString()));
  }
  if (auto status = quantity_builder.Finish(&quantity_total); !status.ok()) {
    return tl::unexpected(sr::engine::make_error(status.ToString()));
  }
  if (auto status = subtotal_builder.Finish(&subtotal); !status.ok()) {
    return tl::unexpected(sr::engine::make_error(status.ToString()));
  }
  if (auto status = discount_builder.Finish(&discount_total); !status.ok()) {
    return tl::unexpected(sr::engine::make_error(status.ToString()));
  }
  if (auto status = tax_builder.Finish(&tax_total); !status.ok()) {
    return tl::unexpected(sr::engine::make_error(status.ToString()));
  }
  if (auto status = total_builder.Finish(&total); !status.ok()) {
    return tl::unexpected(sr::engine::make_error(status.ToString()));
  }

  return arrow::RecordBatch::Make(
      schema, 1,
      {item_count, quantity_total, subtotal, discount_total, tax_total, total});
}

/// Flight kernel: read invoice line items and return totals via DoExchange.
auto flight_invoice_exchange(const flight::FlightServerCall &call) noexcept
    -> Expected<void> {
  if (call.kind != flight::FlightCallKind::DoExchange) {
    return tl::unexpected(
        sr::engine::make_error("flight_invoice_exchange expects do_exchange"));
  }
  if (!call.reader) {
    return tl::unexpected(sr::engine::make_error("flight reader missing"));
  }
  if (!call.writer) {
    return tl::unexpected(sr::engine::make_error("flight writer missing"));
  }

  auto schema_result = call.reader->GetSchema();
  if (!schema_result.ok()) {
    return tl::unexpected(
        sr::engine::make_error(schema_result.status().ToString()));
  }
  auto fields_result = resolve_invoice_fields(*schema_result);
  if (!fields_result) {
    return tl::unexpected(fields_result.error());
  }
  const auto fields = *fields_result;

  InvoiceTotals totals;
  while (true) {
    auto chunk_result = call.reader->Next();
    if (!chunk_result.ok()) {
      return tl::unexpected(
          sr::engine::make_error(chunk_result.status().ToString()));
    }
    auto chunk = std::move(*chunk_result);
    if (!chunk.data) {
      break;
    }
    if (chunk.data->num_rows() == 0) {
      continue;
    }
    if (auto ok = accumulate_invoice_totals(*chunk.data, fields, totals); !ok) {
      return tl::unexpected(ok.error());
    }
  }

  if (totals.item_count == 0) {
    return tl::unexpected(
        sr::engine::make_error("invoice payload contains no rows"));
  }

  totals.subtotal = round_money(totals.subtotal);
  totals.discount_total = round_money(totals.discount_total);
  totals.tax_total = round_money(totals.tax_total);
  totals.total =
      round_money((totals.subtotal - totals.discount_total) + totals.tax_total);

  auto result_batch = make_totals_batch(totals);
  if (!result_batch) {
    return tl::unexpected(result_batch.error());
  }

  auto status = call.writer->Begin((*result_batch)->schema());
  if (!status.ok()) {
    return tl::unexpected(sr::engine::make_error(status.ToString()));
  }
  status = call.writer->WriteRecordBatch(*(*result_batch));
  if (!status.ok()) {
    return tl::unexpected(sr::engine::make_error(status.ToString()));
  }
  status = call.writer->Close();
  if (!status.ok()) {
    return tl::unexpected(sr::engine::make_error(status.ToString()));
  }
  return {};
}

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

  registry.register_kernel("flight_invoice_exchange", flight_invoice_exchange);

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
