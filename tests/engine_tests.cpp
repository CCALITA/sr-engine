#include "test_support.hpp"

/// Execute a single test and update the stats counters.
auto run_test(const char *name, const std::function<bool()> &test,
              TestStats &stats) -> void {
  if (test()) {
    std::cout << "[PASS] " << name << "\n";
    stats.passed += 1;
  } else {
    std::cout << "[FAIL] " << name << "\n";
    stats.failed += 1;
  }
}

auto test_basic_pipeline() -> bool;
auto test_missing_optional_input() -> bool;
auto test_env_binding() -> bool;
auto test_dsl_invalid_node_port() -> bool;
auto test_dsl_params_must_be_object() -> bool;
auto test_graph_store_versioning() -> bool;
auto test_runtime_hot_swap() -> bool;
auto test_runtime_hot_swap_concurrent() -> bool;
auto test_runtime_daemon_polling() -> bool;
auto test_missing_required_input() -> bool;
auto test_type_mismatch() -> bool;
auto test_cycle_detection() -> bool;
auto test_duplicate_output_name() -> bool;
auto test_env_type_mismatch() -> bool;
auto test_dynamic_port_names() -> bool;
auto test_dynamic_ports_missing_names() -> bool;
auto test_dataflow_fanout_join() -> bool;
auto test_dataflow_parallel_runs() -> bool;
auto test_trace_parallel_runs() -> bool;
auto test_runtime_concurrent_same_graph() -> bool;
auto test_runtime_concurrent_multi_graph() -> bool;
auto test_runtime_concurrent_multi_graph_req_bind() -> bool;
auto test_runtime_concurrent_error_isolation() -> bool;
auto test_runtime_concurrent_cancel_deadline() -> bool;
auto test_runtime_concurrent_stage_publish() -> bool;
auto test_runtime_concurrency_stress() -> bool;
auto test_dataflow_task_types() -> bool;
auto test_dataflow_cancelled_request() -> bool;
auto test_dataflow_deadline_exceeded() -> bool;
auto test_rpc_flatbuffer_echo() -> bool;
auto test_rpc_server_input() -> bool;
auto test_rpc_server_output() -> bool;
auto test_serve_config_validation() -> bool;
auto test_serve_unary_echo() -> bool;
auto test_serve_missing_graph() -> bool;
auto test_serve_multi_endpoint() -> bool;
auto test_serve_concurrent_stress() -> bool;

int main() {
  sr::kernel::register_builtin_types();

  TestStats stats;
  run_test("basic_pipeline", test_basic_pipeline, stats);
  run_test("missing_optional_input", test_missing_optional_input, stats);
  run_test("env_binding", test_env_binding, stats);
  run_test("dsl_invalid_node_port", test_dsl_invalid_node_port, stats);
  run_test("dsl_params_must_be_object", test_dsl_params_must_be_object, stats);

  run_test("graph_store_versioning", test_graph_store_versioning, stats);
  run_test("runtime_hot_swap", test_runtime_hot_swap, stats);
  run_test("runtime_hot_swap_concurrent", test_runtime_hot_swap_concurrent,
           stats);
  run_test("runtime_daemon_polling", test_runtime_daemon_polling, stats);
  run_test("missing_required_input", test_missing_required_input, stats);
  run_test("type_mismatch", test_type_mismatch, stats);
  run_test("cycle_detection", test_cycle_detection, stats);
  run_test("duplicate_output_name", test_duplicate_output_name, stats);
  run_test("env_type_mismatch", test_env_type_mismatch, stats);
  run_test("dynamic_port_names", test_dynamic_port_names, stats);
  run_test("dynamic_ports_missing_names", test_dynamic_ports_missing_names,
           stats);
  run_test("dataflow_fanout_join", test_dataflow_fanout_join, stats);
  run_test("dataflow_parallel_runs", test_dataflow_parallel_runs, stats);
  run_test("trace_parallel_runs", test_trace_parallel_runs, stats);
  run_test("runtime_concurrent_same_graph", test_runtime_concurrent_same_graph,
           stats);
  run_test("runtime_concurrent_multi_graph",
           test_runtime_concurrent_multi_graph, stats);
  run_test("runtime_concurrent_multi_graph_req_bind",
           test_runtime_concurrent_multi_graph_req_bind, stats);
  run_test("runtime_concurrent_error_isolation",
           test_runtime_concurrent_error_isolation, stats);
  run_test("runtime_concurrent_cancel_deadline",
           test_runtime_concurrent_cancel_deadline, stats);
  run_test("runtime_concurrent_stage_publish",
           test_runtime_concurrent_stage_publish, stats);
  run_test("runtime_concurrency_stress", test_runtime_concurrency_stress,
           stats);
  run_test("dataflow_task_types", test_dataflow_task_types, stats);
  run_test("dataflow_cancelled_request", test_dataflow_cancelled_request,
           stats);
  run_test("dataflow_deadline_exceeded", test_dataflow_deadline_exceeded,
           stats);
  run_test("rpc_flatbuffer_echo", test_rpc_flatbuffer_echo, stats);
  run_test("rpc_server_input", test_rpc_server_input, stats);
  run_test("rpc_server_output", test_rpc_server_output, stats);
  run_test("serve_config_validation", test_serve_config_validation, stats);
  run_test("serve_unary_echo", test_serve_unary_echo, stats);
  run_test("serve_missing_graph", test_serve_missing_graph, stats);
  run_test("serve_multi_endpoint", test_serve_multi_endpoint, stats);
  run_test("serve_concurrent_stress", test_serve_concurrent_stress, stats);

  std::cout << "Passed: " << stats.passed << ", Failed: " << stats.failed
            << "\n";
  return stats.failed == 0 ? 0 : 1;
}
