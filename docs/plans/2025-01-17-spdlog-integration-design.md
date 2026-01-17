# spdlog Integration Design

**Date:** 2025-01-17
**Status:** Design Complete

## Overview

Integrate spdlog (header-only) as the default logging utility for the sr-engine project with OpenTelemetry-style trace correlation and gflags-based configuration.

## Architecture

### Core Infrastructure

- **Location:** `src/common/logging/`
- **Components:**
  - `log.hpp` - Public API header with `sr::log` namespace
  - `log.cpp` - Implementation including `TraceCorrelatingSink`
  - `flags.cpp` - gflags definitions for configuration

### Logger Design

- **Single global logger** for simplicity (no module-specific loggers)
- **Thread-safe** via spdlog async sink
- **Hierarchical naming** available but not used (e.g., `engine.dsl`)
- **Async logging** for performance in multi-threaded runtime

### Trace Correlation

**OpenTelemetry-style correlation** via custom `TraceCorrelatingSink`:
- Automatically injects `trace_id` and `span_id` from `RequestContext::trace`
- Transparent to application code - no API changes needed
- Works with existing `TraceContext` infrastructure
- Zero overhead when `SR_TRACE_DISABLED` is defined
- Thread-safe - each worker thread's `RequestContext` is thread-local

### Log Output

- **Async file sink** with rotation
- **Pluggable design** - easy to add console or other sinks
- **Structured logging** support for rich metadata

## API Design

### Namespace API (C++20 Functional Style)

```cpp
namespace sr::log {
  void init(const std::string& level, const std::string& file_path, 
            size_t max_size, size_t max_files);
  
  template <typename... Args>
  void trace(std::string_view fmt, Args&&... args);
  
  template <typename... Args>
  void debug(std::string_view fmt, Args&&... args);
  
  template <typename... Args>
  void info(std::string_view fmt, Args&&... args);
  
  template <typename... Args>
  void warn(std::string_view fmt, Args&&... args);
  
  template <typename... Args>
  void error(std::string_view fmt, Args&&... args);
  
  template <typename... Args>
  void critical(std::string_view fmt, Args&&... args);
  
  // Structured logging
  template <typename... Args>
  void info(std::string_view event, 
            std::unordered_map<std::string, std::string> fields);
}
```

### Usage Examples

```cpp
// Basic logging
sr::log::info("Parsed {} nodes", count);
sr::log::error("Failed to connect: {}", error);

// Structured logging
sr::log::info("node_execution", {
  {"node_id", "transform"},
  {"duration_ms", "123"},
  {"status", "success"}
});
```

## Build System Integration

### Dependencies

- **spdlog:** Git submodule in `thirdparty/spdlog` (header-only branch)
- **gflags:** System dependency via `find_package(gflags REQUIRED)`

### CMake Configuration

```cmake
# spdlog submodule
add_subdirectory(thirdparty/spdlog)

# gflags
find_package(gflags REQUIRED)

# Logging interface library
add_library(sr_logging INTERFACE)
target_link_libraries(sr_logging INTERFACE 
    spdlog::spdlog_header_only
    gflags::gflags
)
target_include_directories(sr_logging INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/src)

# Link to targets that need logging
target_link_libraries(sr_engine PUBLIC sr_logging)
target_link_libraries(sr_engine_example PRIVATE sr_logging)
```

### gflags Configuration

```cpp
// src/common/logging/flags.cpp
DEFINE_string(log_level, "info", "Minimum log level (trace, debug, info, warn, error, critical, off)");
DEFINE_string(log_file, "sr_engine.log", "Log file path");
DEFINE_int32(log_max_size, 10485760, "Max log file size in bytes before rotation");
DEFINE_int32(log_max_files, 3, "Number of rotated log files to keep");
```

### Usage

```bash
# Command line
./sr_engine_example --log_level=debug --log_file=debug.log

# Environment variables
SR_LOG_LEVEL=debug SR_LOG_FILE=debug.log ./sr_engine_example

# Programmatic
FLAGS_log_level = "debug";
```

## Log Levels

Standard 5-level hierarchy:
- `trace` - Most detailed
- `debug` - Debugging information
- `info` - General informational messages
- `warn` - Warning conditions
- `error` - Error conditions
- `critical` - Critical conditions
- `off` - Disable logging

## Data Flow

1. Application calls `sr::log::info("message", data)`
2. Message formatted using `std::format`
3. Log record passes through `TraceCorrelatingSink`
4. Sink checks `RequestContext::current()` for active trace context
5. If trace exists, adds `trace_id` and `span_id` fields
6. Record queued to async sink
7. Worker thread writes to file (non-blocking for caller)

## Initialization

- `sr::log::init()` called by `Runtime::initialize()` automatically
- Examples and tests can call directly if needed
- Reads gflags values for configuration
- Creates async sink with file rotation
- Registers `TraceCorrelatingSink`

## Log Format Example

```
2025-01-17 22:15:30.123 [INFO] [engine.dsl] Parsed 15 nodes trace_id=abc123 span_id=def456
```

## Integration with Existing Components

### Tracing
- **Separate systems:** Logging and tracing remain independent
- **Correlation only:** Logs carry trace IDs but don't depend on tracing infrastructure
- **Optional:** Zero overhead when tracing disabled

### RequestContext
- No changes needed to existing `RequestContext` or `TraceContext`
- Sink reads existing data via `RequestContext::current()`
- Works naturally with multi-threaded execution

## Implementation Plan

1. Add spdlog git submodule to `thirdparty/spdlog`
2. Add gflags dependency to CMakeLists.txt
3. Create `src/common/logging/` directory
4. Implement `src/common/logging/log.hpp` with public API
5. Implement `src/common/logging/log.cpp` with `TraceCorrelatingSink`
6. Implement `src/common/logging/flags.cpp` with gflags
7. Create `src/common/logging/CMakeLists.txt`
8. Update root `CMakeLists.txt` to include logging subdirectory and link targets
9. Add logging calls throughout codebase:
   - `src/engine/` - DSL parsing, compilation, error handling
   - `src/runtime/` - Execution, scheduling, RPC serving
   - `src/kernel/` - Kernel registration, execution
   - `examples/` - Demonstration logging
10. Update documentation in AGENTS.md

## Success Criteria

- [ ] spdlog integrated as header-only library
- [ ] gflags-based configuration working
- [ ] Trace correlation working with RequestContext
- [ ] Async file logging with rotation
- [ ] Logging added to all major components
- [ ] Project builds successfully
- [ ] Examples and tests demonstrate logging
- [ ] Documentation updated
