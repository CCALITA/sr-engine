#include <arrow/api.h>
#include <arrow/flight/api.h>

#include <cerrno>
#include <cstdlib>
#include <cstdint>
#include <format>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "kernel/flight_kernels.hpp"
#include "kernel/sample_kernels.hpp"
#include "runtime/runtime.hpp"
#include "runtime/serve.hpp"

namespace {

/// CLI options for the Flight demo server.
struct DemoOptions {
  std::string location = "grpc+tcp://127.0.0.1:0";
  double tax_rate = 0.0825;
  double discount = 1.5;
  bool serve_only = false;
};

constexpr std::string_view kDemoGraphName = "flight_invoice_exchange";

/// Demo line item for the invoice payload.
struct InvoiceLine {
  std::string sku;
  std::int64_t quantity = 0;
  double unit_price = 0.0;
  double discount = 0.0;
  double tax_rate = 0.0;
};

/// Print CLI usage information.
auto print_usage(std::string_view program) -> void {
  std::cout << std::format(
      "Usage: {} [--location <uri>] [--tax-rate <rate>] "
      "[--discount <amount>] [--serve-only]\n"
      "Defaults:\n"
      "  --location grpc+tcp://127.0.0.1:0\n"
      "  --tax-rate 0.0825\n"
      "  --discount 1.5\n"
      "Payload schema: sku (string), quantity (int64), "
      "unit_price (float64), discount (float64), tax_rate (float64)\n"
      "Tax rate accepts fraction (0.0825) or percent (8.25).\n",
      program);
}

/// Parse a double value from CLI.
auto parse_double(std::string_view value) -> std::optional<double> {
  std::string temp(value);
  char *end = nullptr;
  errno = 0;
  const double parsed = std::strtod(temp.c_str(), &end);
  if (end == temp.c_str() || *end != '\0' || errno == ERANGE) {
    return std::nullopt;
  }
  return parsed;
}

/// Parse CLI arguments into demo options.
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
    if (arg == "--location") {
      auto value = next_value(arg);
      if (!value) {
        return std::nullopt;
      }
      options.location = std::move(*value);
      continue;
    }
    if (arg == "--tax-rate") {
      auto value = next_value(arg);
      if (!value) {
        return std::nullopt;
      }
      auto parsed = parse_double(*value);
      if (!parsed) {
        std::cerr << "Invalid value for " << arg << "\n";
        print_usage(argv[0]);
        return std::nullopt;
      }
      if (*parsed < 0.0) {
        std::cerr << "Tax rate must be non-negative\n";
        return std::nullopt;
      }
      options.tax_rate = *parsed;
      continue;
    }
    if (arg == "--discount") {
      auto value = next_value(arg);
      if (!value) {
        return std::nullopt;
      }
      auto parsed = parse_double(*value);
      if (!parsed) {
        std::cerr << "Invalid value for " << arg << "\n";
        print_usage(argv[0]);
        return std::nullopt;
      }
      if (*parsed < 0.0) {
        std::cerr << "Discount must be non-negative\n";
        return std::nullopt;
      }
      options.discount = *parsed;
      continue;
    }
    std::cerr << "Unknown argument: " << arg << "\n";
    print_usage(argv[0]);
    return std::nullopt;
  }
  return options;
}

/// Build a demo invoice payload with Arrow arrays.
auto build_invoice_batch(const std::vector<InvoiceLine> &lines)
    -> arrow::Result<std::shared_ptr<arrow::RecordBatch>> {
  if (lines.empty()) {
    return arrow::Status::Invalid("invoice payload missing line items");
  }
  auto schema = arrow::schema({
      arrow::field("sku", arrow::utf8()),
      arrow::field("quantity", arrow::int64()),
      arrow::field("unit_price", arrow::float64()),
      arrow::field("discount", arrow::float64()),
      arrow::field("tax_rate", arrow::float64()),
  });

  arrow::StringBuilder sku_builder;
  arrow::Int64Builder quantity_builder;
  arrow::DoubleBuilder price_builder;
  arrow::DoubleBuilder discount_builder;
  arrow::DoubleBuilder tax_rate_builder;

  for (const auto &line : lines) {
    if (auto status = sku_builder.Append(line.sku); !status.ok()) {
      return status;
    }
    if (auto status = quantity_builder.Append(line.quantity); !status.ok()) {
      return status;
    }
    if (auto status = price_builder.Append(line.unit_price); !status.ok()) {
      return status;
    }
    if (auto status = discount_builder.Append(line.discount); !status.ok()) {
      return status;
    }
    if (auto status = tax_rate_builder.Append(line.tax_rate); !status.ok()) {
      return status;
    }
  }

  std::shared_ptr<arrow::Array> sku;
  std::shared_ptr<arrow::Array> quantity;
  std::shared_ptr<arrow::Array> unit_price;
  std::shared_ptr<arrow::Array> discount;
  std::shared_ptr<arrow::Array> tax_rate;

  if (auto status = sku_builder.Finish(&sku); !status.ok()) {
    return status;
  }
  if (auto status = quantity_builder.Finish(&quantity); !status.ok()) {
    return status;
  }
  if (auto status = price_builder.Finish(&unit_price); !status.ok()) {
    return status;
  }
  if (auto status = discount_builder.Finish(&discount); !status.ok()) {
    return status;
  }
  if (auto status = tax_rate_builder.Finish(&tax_rate); !status.ok()) {
    return status;
  }

  return arrow::RecordBatch::Make(
      schema, static_cast<int64_t>(lines.size()),
      {sku, quantity, unit_price, discount, tax_rate});
}

/// Build demo invoice line items.
auto build_invoice_lines(const DemoOptions &options)
    -> std::vector<InvoiceLine> {
  std::vector<InvoiceLine> lines;
  lines.push_back({"SKU-1001", 2, 19.99, 0.0, options.tax_rate});
  lines.push_back({"SKU-2207", 1, 49.5, options.discount, options.tax_rate});
  lines.push_back({"SKU-4033", 5, 4.25, 0.0, options.tax_rate});
  return lines;
}

/// Connect a Flight client for the demo call.
auto connect_flight_client(const arrow::flight::Location &location)
    -> arrow::Result<std::unique_ptr<arrow::flight::FlightClient>> {
  return arrow::flight::FlightClient::Connect(location);
}

/// Open a DoExchange stream for invoice calculation.
auto open_exchange_stream(arrow::flight::FlightClient &client,
                          std::string_view graph_name)
    -> arrow::Result<arrow::flight::FlightClient::DoExchangeResult> {
  arrow::flight::FlightCallOptions options;
  options.headers.emplace_back("sr-graph-name", std::string(graph_name));
  auto descriptor = arrow::flight::FlightDescriptor::Command(
      std::string(sr::kernel::flight::kInvoiceDescriptorCommand));
  return client.DoExchange(options, descriptor);
}

/// Write the invoice batch and close the client stream.
auto send_invoice(arrow::flight::FlightStreamWriter &writer,
                  const std::shared_ptr<arrow::RecordBatch> &batch)
    -> arrow::Status {
  if (!batch) {
    return arrow::Status::Invalid("invoice batch missing");
  }
  auto status = writer.Begin(batch->schema());
  if (!status.ok()) {
    return status;
  }
  status = writer.WriteRecordBatch(*batch);
  if (!status.ok()) {
    return status;
  }
  return writer.DoneWriting();
}

/// Read the first non-empty totals batch from the server.
auto read_totals_batch(arrow::flight::FlightStreamReader &reader)
    -> arrow::Result<std::shared_ptr<arrow::RecordBatch>> {
  while (true) {
    auto chunk_result = reader.Next();
    if (!chunk_result.ok()) {
      return chunk_result.status();
    }
    auto chunk = std::move(*chunk_result);
    if (!chunk.data) {
      break;
    }
    if (chunk.data->num_rows() > 0) {
      return chunk.data;
    }
  }
  return arrow::Status::Invalid("invoice response missing");
}

/// Read a single int64 value from a totals column.
auto read_int64_value(const std::shared_ptr<arrow::Array> &array,
                      std::string_view name)
    -> arrow::Result<std::int64_t> {
  if (!array) {
    return arrow::Status::Invalid(
        std::format("missing totals column {}", name));
  }
  auto typed = std::dynamic_pointer_cast<arrow::Int64Array>(array);
  if (!typed) {
    return arrow::Status::Invalid(
        std::format("totals column {} is not int64", name));
  }
  if (typed->length() == 0 || typed->IsNull(0)) {
    return arrow::Status::Invalid(
        std::format("totals column {} is empty", name));
  }
  return typed->Value(0);
}

/// Read a single double value from a totals column.
auto read_double_value(const std::shared_ptr<arrow::Array> &array,
                       std::string_view name)
    -> arrow::Result<double> {
  if (!array) {
    return arrow::Status::Invalid(
        std::format("missing totals column {}", name));
  }
  auto typed = std::dynamic_pointer_cast<arrow::DoubleArray>(array);
  if (!typed) {
    return arrow::Status::Invalid(
        std::format("totals column {} is not float64", name));
  }
  if (typed->length() == 0 || typed->IsNull(0)) {
    return arrow::Status::Invalid(
        std::format("totals column {} is empty", name));
  }
  return typed->Value(0);
}

/// Print the invoice totals batch.
auto print_totals(const arrow::RecordBatch &batch) -> arrow::Status {
  auto item_count =
      read_int64_value(batch.GetColumnByName("item_count"), "item_count");
  if (!item_count.ok()) {
    return item_count.status();
  }
  auto quantity_total = read_int64_value(
      batch.GetColumnByName("quantity_total"), "quantity_total");
  if (!quantity_total.ok()) {
    return quantity_total.status();
  }
  auto subtotal =
      read_double_value(batch.GetColumnByName("subtotal"), "subtotal");
  if (!subtotal.ok()) {
    return subtotal.status();
  }
  auto discount = read_double_value(
      batch.GetColumnByName("discount_total"), "discount_total");
  if (!discount.ok()) {
    return discount.status();
  }
  auto tax = read_double_value(batch.GetColumnByName("tax_total"), "tax_total");
  if (!tax.ok()) {
    return tax.status();
  }
  auto total = read_double_value(batch.GetColumnByName("total"), "total");
  if (!total.ok()) {
    return total.status();
  }

  std::cout << std::format(
      "Invoice totals: items={}, quantity={}, subtotal=${:.2f}, "
      "discount=${:.2f}, tax=${:.2f}, total=${:.2f}\n",
      *item_count, *quantity_total, *subtotal, *discount, *tax, *total);
  return arrow::Status::OK();
}

/// Execute a demo DoExchange call against the local server.
auto run_demo_call(int port, const DemoOptions &options) -> arrow::Status {
  auto location_result = arrow::flight::Location::Parse(
      std::format("grpc+tcp://127.0.0.1:{}", port));
  if (!location_result.ok()) {
    return location_result.status();
  }
  auto client_result = connect_flight_client(*location_result);
  if (!client_result.ok()) {
    return client_result.status();
  }
  auto exchange_result = open_exchange_stream(**client_result, kDemoGraphName);
  if (!exchange_result.ok()) {
    return exchange_result.status();
  }
  auto lines = build_invoice_lines(options);
  auto batch_result = build_invoice_batch(lines);
  if (!batch_result.ok()) {
    return batch_result.status();
  }
  auto status = send_invoice(*exchange_result->writer, *batch_result);
  if (!status.ok()) {
    return status;
  }
  auto totals_result = read_totals_batch(*exchange_result->reader);
  if (!totals_result.ok()) {
    return totals_result.status();
  }
  return print_totals(**totals_result);
}

} // namespace

int main(int argc, char **argv) {
  auto options = parse_args(argc, argv);
  if (!options) {
    return 1;
  }

  static sr::engine::TypeRegistry type_registry;
  sr::engine::Runtime runtime;
  sr::kernel::register_builtin_types(type_registry);
  sr::kernel::register_flight_kernels(runtime.registry());

  const char *dsl = R"JSON(
  {
    "version": 1,
    "name": "flight_invoice_exchange",
    "nodes": [
      { "id": "calc", "kernel": "flight_invoice_exchange",
        "inputs": ["call"], "outputs": [] }
    ],
    "bindings": [
      { "to": "calc.call", "from": "$req.flight.call" }
    ],
    "outputs": []
  }
  )JSON";

  sr::engine::StageOptions stage_options;
  stage_options.source = "examples/arrow_server.cpp";
  stage_options.publish = true;
  auto snapshot = runtime.stage_dsl(dsl, stage_options);
  if (!snapshot) {
    std::cerr << "Stage error: " << snapshot.error().message << "\n";
    return 1;
  }

  sr::engine::ServeEndpointConfig endpoint;
  endpoint.name = std::string(kDemoGraphName);
  sr::engine::FlightServeConfig flight;
  flight.location = options->location;
  flight.io_threads = 1;
  endpoint.transport = flight;
  endpoint.request_threads = 1;
  endpoint.max_inflight = 8;

  auto host = runtime.serve(endpoint);
  if (!host) {
    std::cerr << "Serve error: " << host.error().message << "\n";
    return 1;
  }

  auto snapshots = (*host)->stats();
  if (snapshots.empty()) {
    std::cerr << "Serve error: missing endpoint stats\n";
    return 1;
  }
  const auto port = snapshots.front().port;
  if (port <= 0) {
    std::cerr << "Serve error: invalid port\n";
    return 1;
  }
  std::cout << std::format("Flight server listening on port {}\n", port);

  if (options->serve_only) {
    std::cout << "Press Enter to stop the server.\n";
    std::string line;
    std::getline(std::cin, line);
  } else {
    auto status = run_demo_call(port, *options);
    if (!status.ok()) {
      std::cerr << "Client error: " << status.ToString() << "\n";
    }
  }

  (*host)->shutdown();
  (*host)->wait();

  return 0;
}
