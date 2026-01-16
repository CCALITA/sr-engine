# Node Fusion Optimization Design

**Date**: 2026-01-16  
**Status**: Draft  
**Depends On**: `2026-01-16-executor-scheduling-design.md` (inline execution)

## Summary

Compile-time node fusion optimization that merges linear chains of pure nodes into single composite kernels, eliminating intermediate slots, const lookups, and scheduling overhead. Integrates with runtime inline execution for maximum throughput.

## Goals

1. **Eliminate intermediate allocations** - Fused nodes don't create intermediate `ValueBox` instances
2. **Reduce scheduling overhead** - Fewer nodes means fewer scheduling decisions
3. **Enable aggressive inlining** - Fused nodes are excellent inline candidates
4. **Preserve tracing capability** - Original node structure visible for debugging
5. **Handle all binding types** - Slot, const, and env bindings work correctly

## Non-Goals

- Multi-input fusion (e.g., joining two branches)
- Runtime fusion decisions
- Kernel code generation/JIT

## Background

### Current Execution Model

Each node in the compiled plan:
1. Reads inputs from slots, consts, or env
2. Executes kernel
3. Writes outputs to slots
4. Signals dependents

For a chain like `A → B → C`:
- 3 scheduling events
- 3 intermediate slot allocations  
- 3 kernel invocations with type-erased I/O

### Opportunity

For linear chains of pure (side-effect-free) nodes, we can:
- Merge into single kernel call
- Pass intermediate values directly (stack or register)
- Execute as single scheduling unit

## Design

### 1. Fusion Detection

#### 1.1 Fusibility Criteria

A node is fusible if:

```cpp
auto is_node_fusible(const CompiledNode& node) -> bool {
  // Must have exactly one output
  if (node.outputs.size() != 1) return false;
  
  // Count slot inputs (data flow)
  int slot_inputs = 0;
  for (const auto& input : node.inputs) {
    if (input.kind == InputBindingKind::Slot) slot_inputs++;
  }
  
  // At most one slot input (linear data flow)
  // Const and Env inputs are allowed (handled specially)
  return slot_inputs <= 1;
}
```

#### 1.2 Chain Detection Algorithm

```cpp
auto find_fusion_chains(const ExecPlan& plan, const FusionOptions& options)
    -> std::vector<FusionChain> {
  std::vector<FusionChain> chains;
  std::vector<bool> visited(plan.nodes.size(), false);
  
  for (int start : plan.topo_order) {
    if (visited[start]) continue;
    if (!is_node_fusible(plan.nodes[start])) {
      visited[start] = true;
      continue;
    }
    
    // Build chain forward
    std::vector<int> chain = {start};
    visited[start] = true;
    int current = start;
    
    while (chain.size() < options.max_chain_length) {
      const auto& deps = plan.dependents[current];
      if (deps.size() != 1) break;  // Must have single dependent
      
      int next = deps[0];
      if (visited[next]) break;
      if (!is_node_fusible(plan.nodes[next])) break;
      if (!is_only_consumer(plan, next, current)) break;
      if (has_external_output(plan, current)) break;
      
      chain.push_back(next);
      visited[next] = true;
      current = next;
    }
    
    if (chain.size() >= 2) {
      chains.push_back({chain, /* head/tail slots */});
    }
  }
  
  return chains;
}
```

### 2. Fused Kernel Structure

```cpp
struct FusedKernel {
  struct Stage {
    KernelHandle handle;
    entt::meta_type output_type;
    
    // Captured const inputs (baked at compile time)
    struct CapturedConst {
      std::size_t input_index;
      ValueBox value;
    };
    std::vector<CapturedConst> captured_consts;
    
    // Env bindings (looked up at runtime)
    struct EnvBinding {
      std::size_t input_index;
      std::string key;
      entt::meta_type type;
    };
    std::vector<EnvBinding> env_bindings;
  };
  
  std::vector<Stage> stages;
  
  static auto compute(void* self, RequestContext& ctx,
                      const InputValues& inputs, 
                      OutputValues& outputs) -> Expected<void>;
};
```

### 3. Fused Kernel Execution

```cpp
auto FusedKernel::compute(void* self, RequestContext& ctx,
                          const InputValues& inputs, 
                          OutputValues& outputs) -> Expected<void> {
  auto* fused = static_cast<FusedKernel*>(self);
  
  ValueBox intermediate;
  std::vector<const ValueBox*> merged_refs;
  std::vector<ValueBox> env_boxes;  // Temp storage for env lookups
  
  for (std::size_t s = 0; s < fused->stages.size(); ++s) {
    const auto& stage = fused->stages[s];
    
    // Determine data input source
    std::span<const ValueBox* const> data_in;
    if (s == 0) {
      // External inputs
      data_in = /* from inputs */;
    } else {
      // Previous stage output
      data_in = std::span(&intermediate, 1);
    }
    
    // Build merged input refs
    //   1. Data inputs (from previous stage or external)
    //   2. Captured consts (compile-time values)
    //   3. Env values (runtime lookup)
    auto build_result = build_merged_inputs(stage, data_in, ctx,
                                            merged_refs, env_boxes);
    if (!build_result) return tl::unexpected(build_result.error());
    
    // Determine output target
    OutputValues stage_out = (s == fused->stages.size() - 1)
        ? outputs  // Final stage writes to external outputs
        : OutputValues(std::span(&intermediate, 1));
    
    // Execute stage
    InputValues stage_in(merged_refs);
    auto result = stage.handle.compute(
        stage.handle.instance.get(), ctx, stage_in, stage_out);
    if (!result) return result;
  }
  
  return {};
}
```

### 4. Binding Type Handling

#### 4.1 Slot Bindings

- **External slots**: First stage input and last stage output remain as slot bindings
- **Internal slots**: Eliminated - intermediate values passed directly

#### 4.2 Const Bindings

- **Captured at compile time**: Const values copied into `FusedKernel::Stage::captured_consts`
- **Original const_slots may be eliminated**: If only used by fused nodes

```cpp
// During fusion:
for (const auto& input : node.inputs) {
  if (input.kind == InputBindingKind::Const) {
    stage.captured_consts.push_back({
      input_index,
      plan.const_slots[input.const_index]  // Copy value
    });
  }
}
```

#### 4.3 Env Bindings

- **NOT captured**: Env values are per-request, looked up at runtime
- **Key and type stored**: For runtime lookup and validation

```cpp
// During fusion:
for (const auto& input : node.inputs) {
  if (input.kind == InputBindingKind::Env) {
    const auto& req = plan.env_requirements[input.env_index];
    stage.env_bindings.push_back({input_index, req.key, req.type});
  }
}

// During compute:
for (const auto& eb : stage.env_bindings) {
  auto it = ctx.env.find(eb.key);
  if (it == ctx.env.end()) {
    return tl::unexpected(make_error("missing env: " + eb.key));
  }
  env_boxes.push_back(it->second);
}
```

### 5. Slot Elimination

After fusion, eliminate slots only used internally:

```cpp
auto find_eliminable_slots(const ExecPlan& plan,
                           const std::vector<FusionChain>& chains,
                           const std::vector<bool>& eliminated_nodes)
    -> std::vector<bool> {
  std::vector<bool> eliminable(plan.slots.size(), false);
  
  for (const auto& chain : chains) {
    // Intermediate slots (outputs of non-tail nodes in chain)
    for (std::size_t i = 0; i < chain.node_indices.size() - 1; ++i) {
      int node_idx = chain.node_indices[i];
      for (int slot : plan.nodes[node_idx].outputs) {
        // Check no external consumers
        if (is_only_internal_use(plan, slot, chain, eliminated_nodes)) {
          eliminable[slot] = true;
        }
      }
    }
  }
  
  return eliminable;
}

auto compact_slots(ExecPlan& plan, const std::vector<bool>& eliminable) -> void {
  // Build old→new mapping
  std::vector<int> old_to_new(plan.slots.size(), -1);
  int new_idx = 0;
  for (std::size_t i = 0; i < plan.slots.size(); ++i) {
    if (!eliminable[i]) old_to_new[i] = new_idx++;
  }
  
  // Compact slots vector
  // Remap all slot references in nodes, outputs, slot_producer
}
```

### 6. Const Slot Elimination

Eliminate const slots that are only used by fused nodes (captured):

```cpp
auto find_eliminable_const_slots(const ExecPlan& plan,
                                  const std::vector<bool>& eliminated_nodes)
    -> std::vector<bool> {
  std::vector<bool> eliminable(plan.const_slots.size(), true);
  
  // Mark consts still referenced by non-eliminated nodes
  for (std::size_t n = 0; n < plan.nodes.size(); ++n) {
    if (eliminated_nodes[n]) continue;
    
    for (const auto& input : plan.nodes[n].inputs) {
      if (input.kind == InputBindingKind::Const) {
        eliminable[input.const_index] = false;
      }
    }
  }
  
  return eliminable;
}
```

### 7. Plan Rewriting

Complete fusion pass:

```cpp
auto apply_fusion_pass(ExecPlan& plan, const FusionOptions& options)
    -> Expected<void> {
  if (!options.enabled) return {};
  
  // Phase 1: Detect fusion chains
  auto chains = find_fusion_chains(plan, options);
  if (chains.empty()) return {};
  
  // Phase 2: Build fused kernels, mark eliminated nodes
  std::vector<bool> eliminated_nodes(plan.nodes.size(), false);
  for (const auto& chain : chains) {
    auto fused_kernel = build_fused_kernel(plan, chain);
    replace_head_with_fused(plan, chain, fused_kernel);
    mark_eliminated(chain, eliminated_nodes);
  }
  
  // Phase 3: Find eliminable slots
  auto eliminable_slots = find_eliminable_slots(plan, chains, eliminated_nodes);
  auto eliminable_consts = find_eliminable_const_slots(plan, eliminated_nodes);
  
  // Phase 4: Compact everything
  compact_nodes(plan, eliminated_nodes);
  compact_slots(plan, eliminable_slots);
  compact_const_slots(plan, eliminable_consts);
  
  // Phase 5: Rebuild dependent structures
  rebuild_dependents(plan);
  
  // Phase 6: Populate scheduling hints
  populate_inline_hints(plan);
  populate_continuation_hints(plan);
  
  return {};
}
```

### 8. Extended CompiledNode

```cpp
struct CompiledNode {
  std::string id;
  KernelHandle kernel;
  std::vector<InputBinding> inputs;
  std::vector<int> outputs;
  
  // Scheduling hints (from inline execution design)
  int single_continuation = -1;
  bool inline_eligible = true;
  std::uint32_t estimated_cost = 0;  // Microseconds
  
  // Fusion metadata
  bool is_fused = false;
  std::vector<std::string> fused_node_ids;  // Original node IDs for tracing
};
```

### 9. Compile Options

```cpp
struct FusionOptions {
  bool enabled = true;
  int max_chain_length = 8;
  bool preserve_trace_nodes = true;  // Don't fuse nodes with trace breakpoints
};

struct CompileOptions {
  FusionOptions fusion;
  bool enable_inline_hints = true;
  bool enable_continuation_hints = true;
};

auto compile_plan(const GraphDef& graph, const KernelRegistry& registry,
                  const CompileOptions& options = {}) -> Expected<ExecPlan>;
```

### 10. Integration with Inline Execution

Fused nodes are ideal inline candidates:

```cpp
// During fusion:
fused_node.inline_eligible = true;

// Cost estimation: sum of stage costs
fused_node.estimated_cost = 0;
for (int idx : chain) {
  fused_node.estimated_cost += plan.nodes[idx].estimated_cost;
}
fused_node.estimated_cost += chain.size();  // Overhead per stage

// At runtime, inline decision considers fused cost
auto should_inline(int node_idx, int depth) -> bool {
  const auto& node = plan.nodes[node_idx];
  if (!node.inline_eligible) return false;
  if (depth >= config.max_inline_depth) return false;
  
  // Fused nodes count as 1 inline level
  return node.estimated_cost * 1000 < config.inline_threshold_ns;
}
```

### 11. Tracing Support

```cpp
// Trace event for fused node execution
struct FusedNodeStart {
  TraceId trace_id;
  SpanId span_id;
  std::string_view fused_id;
  std::span<const std::string> original_ids;
  int stage_count;
};

struct FusedNodeEnd {
  TraceId trace_id;
  SpanId span_id;
  std::string_view fused_id;
  Tick duration;
  SpanStatus status;
};

// Optional: per-stage timing (for debugging)
struct FusedStageEnd {
  TraceId trace_id;
  SpanId parent_span;
  std::string_view original_id;
  int stage_index;
  Tick duration;
};
```

### 12. Error Handling

When a fused kernel fails:

```cpp
// In FusedKernel::compute:
auto result = stage.handle.compute(...);
if (!result) {
  // Enhance error with stage info
  auto error = result.error();
  error.message = std::format("[stage {} ({})]: {}", 
      stage_idx, fused->stages[stage_idx].original_id, error.message);
  return tl::unexpected(error);
}
```

## Memory Impact

```
Before Fusion (5 nodes, A→B→C→D→E linear):
  plan.slots: 5 slots
  plan.const_slots: 3 consts
  plan.nodes: 5 nodes
  Runtime: 5 ValueBox allocations, 5 scheduling events

After Fusion:
  plan.slots: 1 slot (final output only)
  plan.const_slots: 0 (all captured)
  plan.nodes: 1 fused node
  Runtime: 1 ValueBox allocation, 1 scheduling event
  
Savings:
  - 4 slots eliminated
  - 3 const_slots eliminated
  - 4 scheduling events eliminated
  - Better cache locality (stages execute contiguously)
```

## Testing Strategy

1. **Unit tests**:
   - Fusion detection for various topologies
   - Slot/const elimination logic
   - Plan compaction correctness

2. **Integration tests**:
   - Fused execution produces same results as unfused
   - Env bindings work correctly through fused nodes
   - Error propagation with stage info

3. **Benchmarks**:
   - Measure throughput improvement
   - Memory allocation reduction
   - Cache performance

## Implementation Order

1. Add fusion metadata to `CompiledNode`
2. Implement fusion chain detection
3. Build `FusedKernel` with const capture
4. Add env binding handling
5. Implement slot elimination
6. Implement const slot elimination
7. Integrate with `compile_plan`
8. Add tracing support
9. Add `FusionOptions` to `CompileOptions`
10. Write tests

## Future Work

- **Multi-input fusion**: Join nodes like `(A, B) → C`
- **Conditional fusion**: Handle branches within fusible regions
- **Type-specialized fusion**: Generate monomorphized kernels
- **Kernel purity tracking**: Register-level kernel trait system
