#include <arrow/api.h>
#include <arrow/flight/api.h>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <format>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "engine/error.hpp"
#include "kernel/flight_kernels.hpp"
#include "kernel/sample_kernels.hpp"
#include "runtime/runtime.hpp"
#include "runtime/serve.hpp"

namespace {

/// CLI options for the Flight demo server.
struct DemoOptions {
  std::string location = "grpc+tcp://127.0.0.1:0";
  std::string action_type = "echo";
  std::string action_body = "hello from sr-engine";
  bool serve_only = false;
};

/// Print CLI usage information.
auto print_usage(std::string_view program) -> void {
  std::cout << std::format(
      "Usage: {} [--location <uri>] [--action-type <type>] "
      "[--action-body <body>] [--serve-only]\n"
      "Defaults:\n"
      "  --location grpc+tcp://127.0.0.1:0\n"
      "  --action-type echo\n"
      "  --action-body \"hello from sr-engine\"\n"
      "Action types: echo, upper, len\n",
      program);
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
    if (arg == "--action-type") {
      auto value = next_value(arg);
      if (!value) {
        return std::nullopt;
      }
      options.action_type = std::move(*value);
      continue;
    }
    if (arg == "--action-body") {
      auto value = next_value(arg);
      if (!value) {
        return std::nullopt;
      }
      options.action_body = std::move(*value);
      continue;
    }
    std::cerr << "Unknown argument: " << arg << "\n";
    print_usage(argv[0]);
    return std::nullopt;
  }
  return options;
}

/// Build an Arrow buffer from a payload string.
auto make_buffer(std::string_view payload)
    -> arrow::Result<std::shared_ptr<arrow::Buffer>> {
  arrow::BufferBuilder builder;
  const auto *data = reinterpret_cast<const std::uint8_t *>(payload.data());
  auto status = builder.Append(data, static_cast<int64_t>(payload.size()));
  if (!status.ok()) {
    return status;
  }
  std::shared_ptr<arrow::Buffer> buffer;
  status = builder.Finish(&buffer);
  if (!status.ok()) {
    return status;
  }
  return buffer;
}

/// Convert an Arrow buffer into a string payload.
auto buffer_to_string(const std::shared_ptr<arrow::Buffer> &buffer)
    -> std::string {
  if (!buffer || buffer->size() == 0) {
    return {};
  }
  const auto *data = reinterpret_cast<const char *>(buffer->data());
  return std::string(data, static_cast<std::size_t>(buffer->size()));
}

/// Build a Flight action from CLI options.
auto make_action(const DemoOptions &options)
    -> arrow::Result<arrow::flight::Action> {
  auto buffer_result = make_buffer(options.action_body);
  if (!buffer_result.ok()) {
    return buffer_result.status();
  }
  arrow::flight::Action action;
  action.type = options.action_type;
  action.body = std::move(*buffer_result);
  return action;
}

/// Create a Flight result with the given payload.
auto make_result(std::string_view payload)
    -> arrow::Result<arrow::flight::Result> {
  auto buffer_result = make_buffer(payload);
  if (!buffer_result.ok()) {
    return buffer_result.status();
  }
  arrow::flight::Result result;
  result.body = std::move(*buffer_result);
  return result;
}

/// Convert a string to uppercase for the demo actions.
auto to_upper(std::string value) -> std::string {
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char ch) { return static_cast<char>(std::toupper(ch)); });
  return value;
}

/// Evaluate the demo action and return a payload string.
auto action_payload(std::string_view action_type, std::string payload)
    -> sr::engine::Expected<std::string> {
  if (action_type == "echo") {
    return payload;
  }
  if (action_type == "upper") {
    return to_upper(std::move(payload));
  }
  if (action_type == "len") {
    return std::to_string(payload.size());
  }
  return tl::unexpected(sr::engine::make_error(
      std::format("unsupported action type: {}", action_type)));
}

/// Flight kernel: transform a Flight action into a result payload.
auto action_to_results(const std::optional<arrow::flight::Action> &action) noexcept
    -> sr::engine::Expected<std::vector<arrow::flight::Result>> {
  if (!action) {
    return tl::unexpected(sr::engine::make_error("missing flight action"));
  }
  auto payload =
      action_payload(action->type, buffer_to_string(action->body));
  if (!payload) {
    return tl::unexpected(payload.error());
  }
  auto result = make_result(*payload);
  if (!result.ok()) {
    return tl::unexpected(
        sr::engine::make_error(result.status().ToString()));
  }
  std::vector<arrow::flight::Result> results;
  results.push_back(std::move(*result));
  return results;
}

/// Connect a Flight client for the demo call.
auto connect_flight_client(const arrow::flight::Location &location)
    -> arrow::Result<std::unique_ptr<arrow::flight::FlightClient>> {
  return arrow::flight::FlightClient::Connect(location);
}

/// Open a ResultStream for a DoAction call.
auto open_action_stream(arrow::flight::FlightClient &client,
                        const arrow::flight::Action &action)
    -> arrow::Result<std::unique_ptr<arrow::flight::ResultStream>> {
  return client.DoAction(action);
}

/// Collect all results from a ResultStream.
auto collect_results(std::unique_ptr<arrow::flight::ResultStream> stream)
    -> arrow::Result<std::vector<arrow::flight::Result>> {
  if (!stream) {
    return arrow::Status::Invalid("missing result stream");
  }
  std::vector<arrow::flight::Result> results;
  while (true) {
    auto next = stream->Next();
    if (!next.ok()) {
      return next.status();
    }
    auto result = std::move(*next);
    if (!result) {
      break;
    }
    results.push_back(std::move(*result));
  }
  return results;
}

/// Execute a demo DoAction call against the local server.
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
  auto action_result = make_action(options);
  if (!action_result.ok()) {
    return action_result.status();
  }
  auto stream_result = open_action_stream(**client_result, *action_result);
  if (!stream_result.ok()) {
    return stream_result.status();
  }
  auto results_result = collect_results(std::move(*stream_result));
  if (!results_result.ok()) {
    return results_result.status();
  }
  if (results_result->empty()) {
    std::cout << "Action returned no results.\n";
    return arrow::Status::OK();
  }
  for (std::size_t i = 0; i < results_result->size(); ++i) {
    const auto &result = (*results_result)[i];
    std::cout << std::format("Result[{}]: {}\n", i,
                             buffer_to_string(result.body));
  }
  return arrow::Status::OK();
}

} // namespace

int main(int argc, char **argv) {
  auto options = parse_args(argc, argv);
  if (!options) {
    return 1;
  }

  sr::kernel::register_builtin_types();

  sr::engine::Runtime runtime;
  sr::kernel::register_flight_kernels(runtime.registry());
  runtime.registry().register_kernel("flight_action_demo", action_to_results);

  const char *dsl = R"JSON(
  {
    "version": 1,
    "name": "flight_action_demo",
    "nodes": [
      { "id": "in", "kernel": "flight_server_input",
        "inputs": ["call"],
        "outputs": ["kind", "action", "ticket", "descriptor", "reader", "writer", "metadata_writer"] },
      { "id": "act", "kernel": "flight_action_demo",
        "inputs": ["action"], "outputs": ["results"] },
      { "id": "out", "kernel": "flight_action_output",
        "inputs": ["call", "results"], "outputs": [] }
    ],
    "bindings": [
      { "to": "in.call", "from": "$req.flight.call" },
      { "to": "act.action", "from": "in.action" },
      { "to": "out.call", "from": "$req.flight.call" },
      { "to": "out.results", "from": "act.results" }
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
  endpoint.name = "flight_action_demo";
  endpoint.graph_name = "flight_action_demo";
  sr::engine::FlightServeConfig flight;
  flight.location = options->location;
  flight.io_threads = 1;
  endpoint.transport = flight;
  endpoint.queue_capacity = 0;
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
