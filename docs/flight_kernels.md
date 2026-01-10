# Flight Kernels

This document describes the built-in Arrow Flight kernels and value types.
These kernels are available when the engine is built with
`SR_ENGINE_ENABLE_ARROW_FLIGHT=ON`.

Register them with:

```
sr::kernel::register_flight_kernels(runtime.registry());
```

## Value Types

The serve layer injects Flight values into `RequestContext.env` (see
`docs/serve_layer.md`). The Flight kernel types include:

- `flight_server_call`: `sr::kernel::flight::FlightServerCall`
- `flight_call_kind`: `sr::kernel::flight::FlightCallKind`
- `flight_action`: `std::optional<arrow::flight::Action>`
- `flight_ticket`: `std::optional<arrow::flight::Ticket>`
- `flight_descriptor`: `std::optional<arrow::flight::FlightDescriptor>`
- `flight_reader`: `std::shared_ptr<arrow::flight::FlightMessageReader>`
- `flight_writer`: `std::shared_ptr<arrow::flight::FlightMessageWriter>`
- `flight_metadata_writer`: `std::shared_ptr<arrow::flight::FlightMetadataWriter>`
- `record_batch_reader`: `std::shared_ptr<arrow::RecordBatchReader>`
- `flight_results`: `std::vector<arrow::flight::Result>`

## Kernels

### `flight_server_input`

Input: `FlightServerCall`

Outputs:
- `kind`: `FlightCallKind`
- `action`: `std::optional<Action>`
- `ticket`: `std::optional<Ticket>`
- `descriptor`: `std::optional<FlightDescriptor>`
- `reader`: `std::shared_ptr<FlightMessageReader>`
- `writer`: `std::shared_ptr<FlightMessageWriter>`
- `metadata_writer`: `std::shared_ptr<FlightMetadataWriter>`

### `flight_action_output`

Inputs:
- `call`: `FlightServerCall`
- `results`: `std::vector<arrow::flight::Result>`

Sends action results back to a Flight `DoAction` call.

### `flight_get_output`

Inputs:
- `call`: `FlightServerCall`
- `reader`: `std::shared_ptr<arrow::RecordBatchReader>`

Sends a record batch stream back to a Flight `DoGet` call.
