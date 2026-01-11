cmake_minimum_required(VERSION 3.20)

project(sr_engine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if (APPLE AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  set(SR_ENGINE_GCC_QUICK_EXIT_HEADER
      "${CMAKE_CURRENT_SOURCE_DIR}/src/compat/quick_exit_compat.hpp")
  add_compile_options(
    $<$<COMPILE_LANGUAGE:CXX>:-include>
    $<$<COMPILE_LANGUAGE:CXX>:${SR_ENGINE_GCC_QUICK_EXIT_HEADER}>
    $<$<COMPILE_LANGUAGE:C>:-include>
    $<$<COMPILE_LANGUAGE:C>:stdbool.h>
  )
endif()

add_library(sr_engine
  src/compat/quick_exit_compat.cpp
  src/engine/dsl.cpp
  src/engine/graph_store.cpp
  src/engine/plan.cpp
  src/engine/registry.cpp
  src/runtime/executor.cpp
  src/runtime/runtime.cpp
  src/runtime/serve/serve.cpp
  src/runtime/serve/grpc.cpp
  src/runtime/serve/ipc.cpp
  src/runtime/serve/rpc_env.cpp
  src/kernel/sample_kernels.cpp
  src/kernel/rpc_kernels.cpp
)

target_include_directories(sr_engine PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/stdexec/include
)


option(SR_ENGINE_ENABLE_ARROW_FLIGHT "Enable Arrow Flight serve support." ON)

find_package(gRPC CONFIG REQUIRED)
target_link_libraries(sr_engine PUBLIC gRPC::grpc++ gRPC::grpc)

if (SR_ENGINE_ENABLE_ARROW_FLIGHT)
  find_package(ArrowFlight CONFIG REQUIRED)
  target_sources(sr_engine PRIVATE src/kernel/flight_kernels.cpp)
  target_sources(sr_engine PRIVATE src/runtime/serve/flight.cpp)
  if (TARGET ArrowFlight::arrow_flight_shared)
    target_link_libraries(sr_engine PUBLIC ArrowFlight::arrow_flight_shared)
  elseif (TARGET ArrowFlight::arrow_flight_static)
    target_link_libraries(sr_engine PUBLIC ArrowFlight::arrow_flight_static)
  else()
    message(FATAL_ERROR "ArrowFlight target not found (expected arrow_flight_shared or arrow_flight_static)")
  endif()
  target_compile_definitions(sr_engine PUBLIC SR_ENGINE_WITH_ARROW_FLIGHT=1)
endif()


add_executable(sr_engine_example
  examples/simple.cpp
)

target_link_libraries(sr_engine_example PRIVATE sr_engine)

add_executable(sr_engine_hot_swap
  examples/hot_swap.cpp
)

target_link_libraries(sr_engine_hot_swap PRIVATE sr_engine)

add_executable(sr_engine_complex_example
  examples/complex.cpp
)

target_link_libraries(sr_engine_complex_example PRIVATE sr_engine)

if (SR_ENGINE_ENABLE_ARROW_FLIGHT)
  add_executable(sr_engine_arrow_server
    examples/arrow_server.cpp
  )

  target_link_libraries(sr_engine_arrow_server PRIVATE sr_engine)
endif()

enable_testing()

add_executable(sr_engine_tests
  tests/engine_tests.cpp
  tests/test_concurrency.cpp
  tests/test_core.cpp
  tests/test_runtime.cpp
  tests/test_trace.cpp
  tests/rpc_test.cpp
  tests/serve_test.cpp
)

target_link_libraries(sr_engine_tests PRIVATE sr_engine)

if (TARGET absl::log)
  target_link_libraries(sr_engine_tests PRIVATE absl::log)
endif()

add_test(NAME sr_engine_tests COMMAND sr_engine_tests)
