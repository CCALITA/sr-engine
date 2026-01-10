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
  src/runtime/serve.cpp
  src/kernel/sample_kernels.cpp
  src/kernel/rpc_kernels.cpp
)

target_include_directories(sr_engine PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/stdexec/include
)

option(SR_ENGINE_ENABLE_GRPC "Enable gRPC-based kernels." ON)
if (NOT SR_ENGINE_ENABLE_GRPC)
  message(FATAL_ERROR "SR_ENGINE_ENABLE_GRPC=OFF is not supported; gRPC is required.")
endif()

set(SR_ENGINE_BUNDLED_GRPC_DIR
  "${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/grpc")
if (NOT EXISTS "${SR_ENGINE_BUNDLED_GRPC_DIR}/CMakeLists.txt")
  message(FATAL_ERROR "Bundled gRPC not found at ${SR_ENGINE_BUNDLED_GRPC_DIR}")
endif()

set(ABSL_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
set(ABSL_PROPAGATE_CXX_STD ON CACHE BOOL "" FORCE)
set(gRPC_INSTALL OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_CODEGEN OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_GRPC_CPP_PLUGIN OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_GRPC_CSHARP_PLUGIN OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_GRPC_CSHARP_EXT OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_GRPC_NODE_PLUGIN OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_GRPC_PHP_PLUGIN OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_GRPC_PYTHON_PLUGIN OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_GRPC_RUBY_PLUGIN OFF CACHE BOOL "" FORCE)
set(gRPC_ABSL_PROVIDER "module" CACHE STRING "" FORCE)
set(gRPC_CARES_PROVIDER "module" CACHE STRING "" FORCE)
set(gRPC_PROTOBUF_PROVIDER "module" CACHE STRING "" FORCE)
set(gRPC_RE2_PROVIDER "module" CACHE STRING "" FORCE)
set(gRPC_SSL_PROVIDER "module" CACHE STRING "" FORCE)
set(gRPC_ZLIB_PROVIDER "module" CACHE STRING "" FORCE)
set(protobuf_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(protobuf_BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(protobuf_INSTALL OFF CACHE BOOL "" FORCE)
set(utf8_range_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
set(OPENSSL_NO_ASM ON CACHE BOOL "" FORCE)

add_subdirectory(${SR_ENGINE_BUNDLED_GRPC_DIR} ${CMAKE_CURRENT_BINARY_DIR}/grpc)
if (NOT TARGET gRPC::grpc++)
  add_library(gRPC::grpc++ ALIAS grpc++)
endif()
if (NOT TARGET gRPC::grpc)
  add_library(gRPC::grpc ALIAS grpc)
endif()
target_link_libraries(sr_engine PUBLIC gRPC::grpc++ gRPC::grpc)
target_compile_definitions(sr_engine PUBLIC SR_ENGINE_WITH_GRPC=1)


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
  tests/serve_test.cpp
)

target_link_libraries(sr_engine_tests PRIVATE sr_engine)

if (TARGET absl::log)
  target_link_libraries(sr_engine_tests PRIVATE absl::log)
endif()

add_test(NAME sr_engine_tests COMMAND sr_engine_tests)
