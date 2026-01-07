cmake_minimum_required(VERSION 3.20)

project(sr_engine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_library(sr_engine
  src/engine/dsl.cpp
  src/engine/graph_store.cpp
  src/engine/plan.cpp
  src/engine/registry.cpp
  src/runtime/executor.cpp
  src/runtime/runtime.cpp
  src/kernel/sample_kernels.cpp
  src/kernel/rpc_kernels.cpp
)

target_include_directories(sr_engine PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/stdexec/include
)

option(SR_ENGINE_ENABLE_GRPC "Enable gRPC-based kernels." ON)

if (SR_ENGINE_ENABLE_GRPC)
  find_package(gRPC CONFIG QUIET)
  if (gRPC_FOUND)
    target_link_libraries(sr_engine PUBLIC gRPC::grpc++ gRPC::grpc)
    target_compile_definitions(sr_engine PUBLIC SR_ENGINE_WITH_GRPC=1)
  else()
    message(STATUS "gRPC not found; RPC kernels disabled.")
    target_compile_definitions(sr_engine PUBLIC SR_ENGINE_WITH_GRPC=0)
  endif()
else()
  target_compile_definitions(sr_engine PUBLIC SR_ENGINE_WITH_GRPC=0)
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

enable_testing()

add_executable(sr_engine_tests
  tests/engine_tests.cpp
  tests/test_concurrency.cpp
  tests/test_core.cpp
  tests/test_runtime.cpp
  tests/test_trace.cpp
  tests/rpc_test.cpp
)

target_link_libraries(sr_engine_tests PRIVATE sr_engine)

add_test(NAME sr_engine_tests COMMAND sr_engine_tests)
