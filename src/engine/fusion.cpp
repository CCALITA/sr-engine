#include "engine/fusion.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <format>
#include <unordered_set>

#include "engine/plan.hpp"
#include "runtime/frozen_env.hpp"

namespace sr::engine {
namespace {

/// Check if a node is eligible for fusion based on its structure.
auto is_node_fusible(const CompiledNode& node) -> bool {
  // Must have exactly one output for simple linear fusion
  if (node.outputs.size() != 1) {
    return false;
  }
  
  // Count slot inputs (data flow inputs)
  int slot_input_count = 0;
  for (const auto& input : node.inputs) {
    if (input.kind == InputBindingKind::Slot) {
      ++slot_input_count;
    }
  }
  
  // At most one slot input (linear data flow)
  // Const and Env inputs are fine - they're handled specially
  if (slot_input_count > 1) {
    return false;
  }
  
  // Check kernel is marked as fusible (or at least not marked NeverInline)
  if (has_trait(node.kernel.traits, KernelTraits::NeverInline)) {
    return false;
  }
  
  return true;
}

/// Check if a node's output slot is used outside the fusion chain.
auto has_external_consumers(const ExecPlan& plan, int node_index,
                            const std::unordered_set<int>& chain_set,
                            const std::vector<bool>& eliminated) -> bool {
  // Check dependents
  for (int dep : plan.dependents[static_cast<std::size_t>(node_index)]) {
    if (eliminated[static_cast<std::size_t>(dep)]) {
      continue;
    }
    if (!chain_set.contains(dep)) {
      return true;
    }
  }
  
  // Check if this node produces a graph output
  for (const auto& output : plan.outputs) {
    for (int slot : plan.nodes[static_cast<std::size_t>(node_index)].outputs) {
      if (output.slot_index == slot) {
        // This is a graph output - only OK if it's the tail of the chain
        // We'll check this separately
      }
    }
  }
  
  return false;
}

/// Check if a slot is a graph output.
auto is_graph_output(const ExecPlan& plan, int slot_index) -> bool {
  for (const auto& output : plan.outputs) {
    if (output.slot_index == slot_index) {
      return true;
    }
  }
  return false;
}

/// Build a fusion chain starting from a given node, extending forward.
auto build_chain_forward(const ExecPlan& plan, int start,
                         std::vector<bool>& visited,
                         const FusionOptions& options) -> FusionChain {
  FusionChain chain;
  chain.node_indices.push_back(start);
  visited[static_cast<std::size_t>(start)] = true;
  
  int current = start;
  
  while (chain.node_indices.size() < static_cast<std::size_t>(options.max_chain_length)) {
    const auto& deps = plan.dependents[static_cast<std::size_t>(current)];
    
    // Must have exactly one dependent
    if (deps.size() != 1) {
      break;
    }
    
    int next = deps[0];
    if (visited[static_cast<std::size_t>(next)]) {
      break;
    }
    
    const auto& next_node = plan.nodes[static_cast<std::size_t>(next)];
    
    // Next node must be fusible
    if (!is_node_fusible(next_node)) {
      break;
    }
    
    // Next node must have exactly one slot input (from current)
    int slot_inputs = 0;
    bool input_from_current = false;
    for (const auto& input : next_node.inputs) {
      if (input.kind == InputBindingKind::Slot) {
        ++slot_inputs;
        // Check this slot is produced by current node
        const auto& current_node = plan.nodes[static_cast<std::size_t>(current)];
        for (int out_slot : current_node.outputs) {
          if (input.slot_index == out_slot) {
            input_from_current = true;
          }
        }
      }
    }
    
    if (slot_inputs != 1 || !input_from_current) {
      break;
    }
    
    // Check current node has no external consumers (besides next)
    std::unordered_set<int> temp_chain_set(chain.node_indices.begin(),
                                           chain.node_indices.end());
    temp_chain_set.insert(next);
    std::vector<bool> no_eliminated(plan.nodes.size(), false);
    if (has_external_consumers(plan, current, temp_chain_set, no_eliminated)) {
      break;
    }
    
    // Check current node's output is not a graph output (unless it's the last)
    const auto& current_node = plan.nodes[static_cast<std::size_t>(current)];
    for (int slot : current_node.outputs) {
      if (is_graph_output(plan, slot)) {
        // Can't fuse past a graph output
        goto done;
      }
    }
    
    chain.node_indices.push_back(next);
    visited[static_cast<std::size_t>(next)] = true;
    current = next;
  }
  
done:
  // Record head input and tail output slots
  if (!chain.node_indices.empty()) {
    int head = chain.node_indices.front();
    int tail = chain.node_indices.back();
    
    const auto& head_node = plan.nodes[static_cast<std::size_t>(head)];
    const auto& tail_node = plan.nodes[static_cast<std::size_t>(tail)];
    
    // Find the slot input for head
    for (const auto& input : head_node.inputs) {
      if (input.kind == InputBindingKind::Slot) {
        chain.head_input_slot = input.slot_index;
        break;
      }
    }
    
    // Record tail output
    if (!tail_node.outputs.empty()) {
      chain.tail_output_slot = tail_node.outputs[0];
    }
  }
  
  return chain;
}

/// Build the merged input array for a fused stage.
auto build_stage_inputs(const FusedStage& stage,
                        std::span<const ValueBox* const> data_inputs,
                        RequestContext& ctx,
                        std::vector<const ValueBox*>& merged_refs,
                        std::vector<ValueBox>& env_boxes) -> Expected<void> {
  // Lookup env values from frozen_env
  env_boxes.clear();
  env_boxes.reserve(stage.env_bindings.size());
  const FrozenEnv* frozen = ctx.frozen_env;
  for (const auto& eb : stage.env_bindings) {
    const ValueBox* value = nullptr;
    if (frozen) {
      value = frozen->find(eb.key);
    }
    if (!value) {
      return tl::unexpected(
          make_error(std::format("missing env value in fused kernel: {}", eb.key)));
    }
    if (eb.type_id != TypeId{} && value->type_id != eb.type_id) {
      return tl::unexpected(
          make_error(std::format("env type mismatch in fused kernel: {}", eb.key)));
    }
    env_boxes.push_back(*value);
  }
  
  // Calculate total input count
  std::size_t total = data_inputs.size() + stage.consts.size() + stage.env_bindings.size();
  merged_refs.clear();
  merged_refs.resize(total, nullptr);
  
  // Fill in captured consts
  for (const auto& cc : stage.consts) {
    if (cc.input_index < total) {
      merged_refs[cc.input_index] = &cc.value;
    }
  }
  
  // Fill in env values
  for (std::size_t i = 0; i < stage.env_bindings.size(); ++i) {
    std::size_t idx = stage.env_bindings[i].input_index;
    if (idx < total) {
      merged_refs[idx] = &env_boxes[i];
    }
  }
  
  // Fill remaining slots with data inputs
  std::size_t data_idx = 0;
  for (std::size_t i = 0; i < total && data_idx < data_inputs.size(); ++i) {
    if (merged_refs[i] == nullptr) {
      merged_refs[i] = data_inputs[data_idx++];
    }
  }
  
  return {};
}

}  // namespace

/// Helper to create InputValues from a vector of pointers.
inline auto make_input_values(const std::vector<const ValueBox*>& refs) -> InputValues {
  return InputValues{std::span<const ValueBox* const>{refs.data(), refs.size()}};
}

/// Helper to create OutputValues from a single ValueBox pointer.
inline auto make_single_output(ValueBox* box) -> std::pair<std::array<ValueBox*, 1>, OutputValues> {
  std::array<ValueBox*, 1> arr = {box};
  return {arr, OutputValues{std::span<ValueBox*>{arr.data(), arr.size()}}};
}

auto FusedKernel::compute(void* self, RequestContext& ctx,
                          const InputValues& inputs,
                          OutputValues& outputs) -> Expected<void> {
  auto* fused = static_cast<FusedKernel*>(self);
  
  if (fused->stages.empty()) {
    return tl::unexpected(make_error("fused kernel has no stages"));
  }
  
  // Helper to build input refs from external inputs
  auto collect_external_inputs = [&inputs]() {
    std::vector<const ValueBox*> ext;
    ext.reserve(inputs.size());
    for (std::size_t i = 0; i < inputs.size(); ++i) {
      ext.push_back(&inputs.slot(i));
    }
    return ext;
  };
  
  // Single stage: just delegate
  if (fused->stages.size() == 1) {
    std::vector<const ValueBox*> merged_refs;
    std::vector<ValueBox> env_boxes;
    auto ext = collect_external_inputs();
    
    auto build_result = build_stage_inputs(fused->stages[0], ext, ctx,
                                           merged_refs, env_boxes);
    if (!build_result) {
      return tl::unexpected(build_result.error());
    }
    
    auto stage_in = make_input_values(merged_refs);
    return fused->stages[0].handle.compute(
        fused->stages[0].handle.instance.get(), ctx, stage_in, outputs);
  }
  
  // Multi-stage execution
  ValueBox intermediate;
  std::vector<const ValueBox*> merged_refs;
  std::vector<ValueBox> env_boxes;
  
  // First stage: external inputs -> intermediate
  {
    auto ext = collect_external_inputs();
    
    auto build_result = build_stage_inputs(fused->stages[0], ext, ctx,
                                           merged_refs, env_boxes);
    if (!build_result) {
      return tl::unexpected(build_result.error());
    }
    
    auto stage_in = make_input_values(merged_refs);
    ValueBox* out_ptr = &intermediate;
    std::array<ValueBox*, 1> out_arr = {out_ptr};
    OutputValues stage_out{std::span<ValueBox*>{out_arr}};
    
    auto result = fused->stages[0].handle.compute(
        fused->stages[0].handle.instance.get(), ctx, stage_in, stage_out);
    if (!result) {
      return tl::unexpected(make_error(
          std::format("[fused stage 0 ({})]: {}", 
                      fused->stages[0].original_id, result.error().message)));
    }
  }
  
  // Middle stages: intermediate -> intermediate
  for (std::size_t s = 1; s < fused->stages.size() - 1; ++s) {
    std::vector<const ValueBox*> data_in = {&intermediate};
    
    auto build_result = build_stage_inputs(fused->stages[s], data_in, ctx,
                                           merged_refs, env_boxes);
    if (!build_result) {
      return tl::unexpected(build_result.error());
    }
    
    ValueBox next_intermediate;
    next_intermediate.type_id = fused->stages[s].output_type_id;
    
    auto stage_in = make_input_values(merged_refs);
    ValueBox* out_ptr = &next_intermediate;
    std::array<ValueBox*, 1> out_arr = {out_ptr};
    OutputValues stage_out{std::span<ValueBox*>{out_arr}};
    
    auto result = fused->stages[s].handle.compute(
        fused->stages[s].handle.instance.get(), ctx, stage_in, stage_out);
    if (!result) {
      return tl::unexpected(make_error(
          std::format("[fused stage {} ({})]: {}",
                      s, fused->stages[s].original_id, result.error().message)));
    }
    
    intermediate = std::move(next_intermediate);
  }
  
  // Last stage: intermediate -> external outputs
  {
    std::vector<const ValueBox*> data_in = {&intermediate};
    
    auto build_result = build_stage_inputs(fused->stages.back(), data_in, ctx,
                                           merged_refs, env_boxes);
    if (!build_result) {
      return tl::unexpected(build_result.error());
    }
    
    auto stage_in = make_input_values(merged_refs);
    
    auto result = fused->stages.back().handle.compute(
        fused->stages.back().handle.instance.get(), ctx, stage_in, outputs);
    if (!result) {
      return tl::unexpected(make_error(
          std::format("[fused stage {} ({})]: {}",
                      fused->stages.size() - 1, 
                      fused->stages.back().original_id, 
                      result.error().message)));
    }
  }
  
  return {};
}

auto find_fusion_chains(const ExecPlan& plan, const FusionOptions& options)
    -> std::vector<FusionChain> {
  if (!options.enabled) {
    return {};
  }
  
  std::vector<FusionChain> chains;
  std::vector<bool> visited(plan.nodes.size(), false);
  
  // Process nodes in topological order
  for (int node_idx : plan.topo_order) {
    if (visited[static_cast<std::size_t>(node_idx)]) {
      continue;
    }
    
    const auto& node = plan.nodes[static_cast<std::size_t>(node_idx)];
    if (!is_node_fusible(node)) {
      visited[static_cast<std::size_t>(node_idx)] = true;
      continue;
    }
    
    auto chain = build_chain_forward(plan, node_idx, visited, options);
    
    // Only keep chains with 2+ nodes
    if (chain.node_indices.size() >= 2) {
      chains.push_back(std::move(chain));
    }
  }
  
  return chains;
}

auto apply_fusion_pass(ExecPlan& plan, const FusionOptions& options)
    -> Expected<void> {
  if (!options.enabled || options.preserve_trace_nodes) {
    return {};
  }
  
  auto chains = find_fusion_chains(plan, options);
  if (chains.empty()) {
    return {};
  }
  
  // Track eliminated nodes
  std::vector<bool> eliminated(plan.nodes.size(), false);
  
  for (const auto& chain : chains) {
    if (chain.node_indices.size() < 2) {
      continue;
    }
    
    int head_idx = chain.node_indices.front();
    int tail_idx = chain.node_indices.back();
    
    // Build the fused kernel
    auto fused = std::make_shared<FusedKernel>();
    std::vector<std::string> original_ids;
    std::uint32_t total_cost = 0;
    
    for (int idx : chain.node_indices) {
      const auto& node = plan.nodes[static_cast<std::size_t>(idx)];
      
      FusedStage stage;
      stage.handle = node.kernel;
      stage.original_id = node.id;
      
      // Get output type from slot spec
      if (!node.outputs.empty()) {
        int slot = node.outputs[0];
        stage.output_type_id = plan.slots[static_cast<std::size_t>(slot)].type_id;
      }
      
      // Capture const inputs
      for (std::size_t i = 0; i < node.inputs.size(); ++i) {
        const auto& input = node.inputs[i];
        if (input.kind == InputBindingKind::Const) {
          stage.consts.push_back({
            i,
            plan.const_slots[static_cast<std::size_t>(input.const_index)]
          });
        } else if (input.kind == InputBindingKind::Env) {
          const auto& req = plan.env_requirements[
              static_cast<std::size_t>(input.env_index)];
          stage.env_bindings.push_back({i, req.key, req.type_id});
        }
      }
      
      fused->stages.push_back(std::move(stage));
      original_ids.push_back(node.id);
      total_cost += node.estimated_cost;
    }
    
    // Get head and tail nodes
    auto& head_node = plan.nodes[static_cast<std::size_t>(head_idx)];
    const auto& tail_node = plan.nodes[static_cast<std::size_t>(tail_idx)];
    
    // Create the fused node (modify head in place)
    head_node.id = head_node.id + "_fused";
    head_node.kernel = KernelHandle{
      fused,
      &FusedKernel::compute,
      KernelTraits::Pure | KernelTraits::Fusible,
      total_cost
    };
    // Keep head's inputs, take tail's outputs
    head_node.outputs = tail_node.outputs;
    head_node.inline_eligible = true;
    head_node.estimated_cost = total_cost + static_cast<std::uint32_t>(chain.node_indices.size());
    head_node.is_fused = true;
    head_node.fused_node_ids = std::move(original_ids);
    
    // Mark other nodes as eliminated
    for (std::size_t i = 1; i < chain.node_indices.size(); ++i) {
      eliminated[static_cast<std::size_t>(chain.node_indices[i])] = true;
    }
    
    // Update slot_producer for tail's outputs
    for (int slot : head_node.outputs) {
      plan.slot_producer[static_cast<std::size_t>(slot)] = head_idx;
    }
    
    // Update dependents: fused node inherits tail's dependents
    plan.dependents[static_cast<std::size_t>(head_idx)] =
        plan.dependents[static_cast<std::size_t>(tail_idx)];
    
    // Clear eliminated nodes' dependents
    for (std::size_t i = 1; i < chain.node_indices.size(); ++i) {
      plan.dependents[static_cast<std::size_t>(chain.node_indices[i])].clear();
    }
  }
  
  // TODO: Compact eliminated nodes and slots for memory savings
  // For now, we leave them in place (they won't be executed)
  
  // Update pending counts
  plan.pending_counts.assign(plan.nodes.size(), 0);
  for (std::size_t i = 0; i < plan.dependents.size(); ++i) {
    if (eliminated[i]) continue;
    for (int dep : plan.dependents[i]) {
      if (!eliminated[static_cast<std::size_t>(dep)]) {
        plan.pending_counts[static_cast<std::size_t>(dep)]++;
      }
    }
  }
  
  // Update initial_ready
  plan.initial_ready.clear();
  for (std::size_t i = 0; i < plan.nodes.size(); ++i) {
    if (eliminated[i]) continue;
    if (plan.pending_counts[i] == 0) {
      plan.initial_ready.push_back(static_cast<int>(i));
    }
  }
  
  // Update continuation hints
  for (std::size_t i = 0; i < plan.nodes.size(); ++i) {
    if (eliminated[i]) {
      plan.nodes[i].single_continuation = -1;
      continue;
    }
    // Filter out eliminated dependents
    std::vector<int> active_deps;
    for (int dep : plan.dependents[i]) {
      if (!eliminated[static_cast<std::size_t>(dep)]) {
        active_deps.push_back(dep);
      }
    }
    plan.nodes[i].single_continuation = (active_deps.size() == 1) ? active_deps[0] : -1;
  }
  
  return {};
}

}  // namespace sr::engine
