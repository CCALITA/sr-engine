# Executor Scheduling Optimization Design

**Date**: 2026-01-16  
**Status**: Draft  
**Scope**: Throughput-focused scheduling optimizations for mixed-latency DAGs

## Summary

Refine the executor scheduling to improve throughput for medium-scale graphs (50-500 nodes) with mixed latency workloads. The design leverages the existing `stdexec::static_thread_pool` work-stealing infrastructure while adding inline execution for cheap nodes and continuation chaining for linear paths.

## Goals

1. **Reduce unnecessary thread hops** for short-running nodes via inline execution
2. **Optimize linear execution chains** with continuation chaining
3. **Maintain correct tracing** with inline execution awareness
4. **Preserve cancellation semantics** through inline execution paths

## Non-Goals

- Pluggable scheduling policy abstraction (keeping it simple)
- NUMA-aware scheduling (not needed for current scale)
- Priority queues (work-stealing handles load balancing)

## Background

### Current Implementation

The executor uses `stdexec::static_thread_pool` which provides:
- Per-thread local deques (LIFO for producer, FIFO for stealers)
- Work-stealing when a thread's queue is empty
- Efficient `spawn()` that pushes to local queue

When a node completes, it spawns all ready dependents individually. Each spawn involves queue operations and potential atomic contention.

### Opportunity

For mixed-latency workloads:
- Some nodes complete in microseconds (lookups, transforms)
- Others take milliseconds+ (I/O, compute-heavy)

Short nodes pay unnecessary overhead from queue round-trips. By inlining their execution on the completing thread, we can:
- Eliminate queue push/pop operations
- Maintain cache locality on slot data
- Reduce atomic operations

## Design

### 1. Extended CompiledNode

Add scheduling metadata to compiled nodes:

```cpp
// In plan.hpp
struct CompiledNode {
  std::string id;
  KernelHandle kernel;
  std::vector<InputBinding> inputs;
  std::vector<int> outputs;
  
  // Scheduling optimization hints
  int single_continuation = -1;     // If exactly one dependent, cache index
  bool inline_eligible = true;      // Can be inlined (DSL can disable)
  std::uint32_t estimated_cost = 0; // Microseconds estimate (0 = unknown)
};
```

### 2. ExecutorConfig Extensions

Add tuning parameters:

```cpp
// In executor.hpp
struct ExecutorConfig {
  int compute_threads = 0;
  
  // Inline execution settings
  bool enable_inline_execution = true;
  std::uint32_t inline_threshold_ns = 10'000;  // 10 microseconds
  int max_inline_depth = 3;                     // Prevent stack overflow
  
  // Continuation optimization
  bool enable_continuation_chaining = true;
};
```

### 3. DAGStates Enhancements

Add inline execution support:

```cpp
// In executor.cpp
struct DAGStates : std::enable_shared_from_this<DAGStates> {
  // ... existing fields ...
  
  // Configuration
  const ExecutorConfig* config = nullptr;
  
  // Runtime cost tracking (optional, for adaptive decisions)
  struct NodeMetrics {
    std::atomic<std::uint64_t> total_duration_ns{0};
    std::atomic<std::uint32_t> execution_count{0};
  };
  std::vector<NodeMetrics> node_metrics;
  
  // Methods
  auto should_inline(int node_index, int current_depth) const -> bool;
  auto execute_node_inline(int node_index, int inline_depth) -> void;
  auto complete_node_with_inline(int node_index, int inline_depth) -> void;
};
```

### 4. Inline Decision Logic

```cpp
auto DAGStates::should_inline(int node_index, int current_depth) const -> bool {
  if (!config || !config->enable_inline_execution) return false;
  if (current_depth >= config->max_inline_depth) return false;
  
  const auto& node = plan->nodes[static_cast<std::size_t>(node_index)];
  
  // DSL can disable inline for specific nodes
  if (!node.inline_eligible) return false;
  
  // Use runtime metrics if available
  if (!node_metrics.empty()) {
    const auto& m = node_metrics[static_cast<std::size_t>(node_index)];
    auto count = m.execution_count.load(std::memory_order_relaxed);
    if (count > 0) {
      auto avg_ns = m.total_duration_ns.load(std::memory_order_relaxed) / count;
      return avg_ns < config->inline_threshold_ns;
    }
  }
  
  // Fall back to compile-time estimate
  return node.estimated_cost > 0 && 
         node.estimated_cost * 1000 < config->inline_threshold_ns;
}
```

### 5. Execution Flow with Inline Support

```cpp
auto DAGStates::complete_node_with_inline(int node_index, int inline_depth) -> void {
  const auto& dependents = plan->dependents[static_cast<std::size_t>(node_index)];
  
  // Collect ready nodes
  int ready_storage[8];
  int* ready_ptr = ready_storage;
  int ready_count = 0;
  if (dependents.size() > 8) {
    ready_ptr = new int[dependents.size()];
  }
  
  for (int dep : dependents) {
    if (node_states[static_cast<std::size_t>(dep)].pending.fetch_sub(
            1, std::memory_order_acq_rel) == 1) {
      ready_ptr[ready_count++] = dep;
    }
  }
  
  if (ready_count == 0) {
    if (dependents.size() > 8) delete[] ready_ptr;
    finish_node();
    return;
  }
  
  // Optimization: single ready node that's cheap -> inline it
  if (ready_count == 1 && should_inline(ready_ptr[0], inline_depth)) {
    int inline_node = ready_ptr[0];
    if (dependents.size() > 8) delete[] ready_ptr;
    finish_node();
    execute_node_inline(inline_node, inline_depth + 1);
    return;
  }
  
  // Multiple ready nodes: schedule all but potentially inline first
  int inline_candidate = -1;
  std::span<int> to_schedule(ready_ptr, static_cast<std::size_t>(ready_count));
  
  if (config && config->enable_inline_execution &&
      ready_count > 0 && should_inline(ready_ptr[0], inline_depth)) {
    inline_candidate = ready_ptr[0];
    to_schedule = to_schedule.subspan(1);
  }
  
  // Schedule remaining via pool
  if (!to_schedule.empty()) {
    schedule_nodes(to_schedule);
  }
  
  if (dependents.size() > 8) delete[] ready_ptr;
  finish_node();
  
  // Execute inline candidate last (after finish_node for correctness)
  if (inline_candidate >= 0) {
    execute_node_inline(inline_candidate, inline_depth + 1);
  }
}
```

### 6. Continuation Chaining

For linear chains detected at compile time:

```cpp
auto DAGStates::execute_node_impl(int node_index, int inline_depth) -> void {
  // ... existing execution logic ...
  
  // After successful kernel completion:
  const auto& node = plan->nodes[static_cast<std::size_t>(node_index)];
  
  // Continuation chaining for single-dependent paths
  if (config && config->enable_continuation_chaining &&
      node.single_continuation >= 0) {
    
    // Decrement the single dependent's counter
    int cont = node.single_continuation;
    if (node_states[static_cast<std::size_t>(cont)].pending.fetch_sub(
            1, std::memory_order_acq_rel) == 1) {
      // Dependent is ready, decide inline vs schedule
      if (should_inline(cont, inline_depth)) {
        finish_node();
        execute_node_inline(cont, inline_depth + 1);
        return;
      }
      // Schedule it
      schedule_nodes(std::span<const int>(&cont, 1));
    }
    finish_node();
    return;
  }
  
  // Normal completion path
  complete_node_with_inline(node_index, inline_depth);
}
```

### 7. Tracing Integration

#### 7.1 New Trace Event

```cpp
// In trace.hpp
struct NodeInlined {
  TraceId trace_id = 0;
  SpanId span_id = 0;
  std::string_view node_id;
  int node_index = -1;
  int parent_node_index = -1;
  int inline_depth = 0;
};

enum class TraceFlag : std::uint32_t {
  // ... existing flags ...
  InlineDetail = 1u << 5,
};
```

#### 7.2 Queue Delay Handling

Skip `QueueDelay` events for inlined nodes since they never entered the queue:

```cpp
// In execute_node tracing section:
if (trace::has_flag(trace_flags, trace::TraceFlag::QueueDelay)) {
  if (inline_depth == 0 &&  // Only for scheduled (not inlined) nodes
      static_cast<std::size_t>(node_index) < enqueue_ticks.size()) {
    trace::emit(trace->sink, trace::QueueDelay{...});
  }
}
```

### 8. Compile-Time Annotations

During `compile_plan()`:

```cpp
// Detect single continuations
for (std::size_t i = 0; i < plan.nodes.size(); ++i) {
  const auto& deps = plan.dependents[i];
  if (deps.size() == 1) {
    plan.nodes[i].single_continuation = deps[0];
  }
}

// Parse DSL scheduling hints
if (node_def.contains("scheduling")) {
  const auto& sched = node_def["scheduling"];
  if (sched.contains("inline")) {
    plan.nodes[i].inline_eligible = sched["inline"].get<bool>();
  }
  if (sched.contains("cost_estimate_us")) {
    plan.nodes[i].estimated_cost = sched["cost_estimate_us"].get<uint32_t>();
  }
}
```

## DSL Syntax

Optional scheduling hints in node definitions:

```json
{
  "id": "fast_transform",
  "kernel": "transform",
  "inputs": ["data"],
  "outputs": ["result"],
  "scheduling": {
    "inline": true,
    "cost_estimate_us": 5
  }
}
```

```json
{
  "id": "slow_io",
  "kernel": "http_fetch",
  "inputs": ["url"],
  "outputs": ["response"],
  "scheduling": {
    "inline": false
  }
}
```

## Safety Considerations

### Stack Overflow Prevention

The `max_inline_depth` limit (default 3) prevents deep inline chains from overflowing the stack. Each level adds ~200-500 bytes of stack frame, so depth 3 is safe for typical 1MB+ thread stacks.

### Cancellation Propagation

Inline execution paths check cancellation at node entry, same as scheduled execution:

```cpp
if (aborted.load(std::memory_order_acquire)) {
  finish_trace(trace::SpanStatus::Skipped, {});
  complete_node(...);
  return;
}
```

### Error Propagation

Errors in inlined nodes use the same `record_error()` path, setting `aborted = true` to prevent further execution.

## Testing Strategy

1. **Unit tests**: Verify inline eligibility logic with various configurations
2. **Integration tests**: DAGs with linear chains verify continuation chaining
3. **Stress tests**: High fan-out/fan-in patterns verify no stack overflow
4. **Benchmarks**: Compare throughput with/without inline execution

## Implementation Order

1. Add `single_continuation` and `inline_eligible` to `CompiledNode`
2. Extend `ExecutorConfig` with inline settings
3. Implement `should_inline()` in `DAGStates`
4. Add `execute_node_inline()` with tracing
5. Refactor `complete_node()` to `complete_node_with_inline()`
6. Add continuation chaining in `execute_node_impl()`
7. Update `compile_plan()` to populate scheduling hints
8. Add DSL parsing for scheduling section
9. Add tracing events and sink bindings
10. Write tests and benchmarks

## Metrics to Track

- **Inline execution rate**: % of nodes executed inline vs scheduled
- **Queue delay reduction**: Average queue delay before/after
- **Throughput improvement**: Graphs/second for representative workloads
- **Latency distribution**: P50/P99 for mixed-latency DAGs
