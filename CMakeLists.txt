cmake_minimum_required(VERSION 3.20)

if (APPLE AND NOT DEFINED CMAKE_CXX_COMPILER)
  if (DEFINED ENV{CXX} AND "$ENV{CXX}" MATCHES "g\\+\\+|gcc")
    message(STATUS "Detected GCC in CXX; forcing clang on macOS")
    set(CMAKE_C_COMPILER "/usr/bin/clang" CACHE FILEPATH "C compiler" FORCE)
    set(CMAKE_CXX_COMPILER "/usr/bin/clang++" CACHE FILEPATH "C++ compiler" FORCE)
  endif()
endif()

project(sr_engine LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_compile_options(-O3 -fPIC -g -Wno-error)

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
  src/common/logging/flags.cpp
  src/common/logging/log.cpp
  src/engine/dsl.cpp
  src/engine/fusion.cpp
  src/engine/graph_store.cpp
  src/engine/plan.cpp
  src/engine/registry.cpp
  src/engine/type_encoding.cpp
  src/engine/type_hash.cpp
  src/engine/type_system.cpp
  src/engine/version.cpp
  src/runtime/executor.cpp
  src/runtime/runtime.cpp
  src/runtime/serve/serve.cpp
  src/runtime/serve/grpc.cpp
  src/runtime/serve/ipc.cpp
  src/runtime/serve/rpc_env.cpp
  src/kernel/sample_kernels.cpp
  src/kernel/rpc_kernels.cpp
  src/kernel/rpc_transport_grpc.cpp
  thirdparty/blake3/blake3.c
)

target_include_directories(sr_engine PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/blake3
  ${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/stdexec/include
)

add_subdirectory(thirdparty/spdlog)
target_link_libraries(sr_engine PUBLIC spdlog_header_only)

option(SR_ENGINE_ENABLE_ARROW_FLIGHT "Enable Arrow Flight serve support." ON)

find_package(gflags CONFIG)
if(TARGET gflags::gflags)
  target_link_libraries(sr_engine PUBLIC gflags::gflags)
else()
  find_library(gflags_lib NAMES gflags PATHS /opt/homebrew/lib /usr/local/lib NO_DEFAULT_PATH)
  if(NOT gflags_lib)
    message(FATAL_ERROR "gflags library not found")
  endif()
  target_link_libraries(sr_engine PUBLIC "${gflags_lib}")
endif()

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

add_executable(sr_engine_ipc_server
  examples/ipc_server.cpp
)

target_link_libraries(sr_engine_ipc_server PRIVATE sr_engine)

add_executable(sr_engine_sr_ipc_server
  examples/sr_ipc_server.cpp
  examples/mock_api_c.cpp
)

target_link_libraries(sr_engine_sr_ipc_server PRIVATE sr_engine)

if (SR_ENGINE_ENABLE_ARROW_FLIGHT)
  add_executable(sr_engine_arrow_server
    examples/arrow_server.cpp
  )

  target_link_libraries(sr_engine_arrow_server PRIVATE sr_engine)
endif()

find_package(benchmark REQUIRED)

add_executable(sr_engine_benchmarks
  benchmarks/serve_benchmark.cpp
)
target_link_libraries(sr_engine_benchmarks PRIVATE sr_engine benchmark::benchmark)

enable_testing()

add_executable(sr_engine_tests
  tests/engine_tests.cpp
  tests/test_concurrency.cpp
  tests/test_core.cpp
  tests/test_runtime.cpp
  tests/test_trace.cpp
  tests/rpc_test.cpp
  tests/serve_test.cpp
  tests/test_rpc_abstractions.cpp
  tests/test_rpc_policies.cpp
  tests/test_rpc_trampoline.cpp
)

target_link_libraries(sr_engine_tests PRIVATE sr_engine)

if (TARGET absl::log)
  target_link_libraries(sr_engine_tests PRIVATE absl::log)
endif()

add_test(NAME sr_engine_tests COMMAND sr_engine_tests)

add_executable(type_system_test
  tests/type_system_test.cpp
)

target_link_libraries(type_system_test PRIVATE sr_engine)

add_test(NAME type_system_test COMMAND type_system_test)
