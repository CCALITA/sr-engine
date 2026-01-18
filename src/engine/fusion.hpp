#pragma once

#include <span>
#include <string>
#include <vector>

#include "engine/error.hpp"
#include "engine/plan.hpp"

namespace sr::engine {

/// Result of fusion analysis for a chain of nodes.
struct FusionChain {
  std::vector<int> node_indices;  ///< Nodes in topological order
  int head_input_slot = -1;       ///< External input slot to chain
  int tail_output_slot = -1;      ///< External output slot from chain
};

/// Captured const input for a fused stage.
struct CapturedConst {
  std::size_t input_index;  ///< Which input position receives this const
  ValueBox value;           ///< The const value
};

/// Captured env binding for a fused stage.
struct CapturedEnv {
  std::size_t input_index;   ///< Which input position receives this env
  std::string key;           ///< Env key to look up at runtime
  TypeId type_id;            ///< Expected type
  entt::meta_type meta_type; ///< Expected meta type
};

/// A single stage within a fused kernel.
struct FusedStage {
  KernelHandle handle;                    ///< Original kernel
  TypeId output_type;                     ///< Output type for intermediate
  std::string original_id;                ///< Original node ID (for tracing)
  std::vector<CapturedConst> consts;      ///< Captured const inputs
  std::vector<CapturedEnv> env_bindings;  ///< Env bindings to look up at runtime
};

/// Fused kernel that chains multiple kernels sequentially.
struct FusedKernel {
  std::vector<FusedStage> stages;
  
  /// Type-erased compute function for fused execution.
  static auto compute(void* self, RequestContext& ctx,
                      const InputValues& inputs,
                      OutputValues& outputs) -> Expected<void>;
};

/// Analyze a plan and find all fusible chains.
auto find_fusion_chains(const ExecPlan& plan, const FusionOptions& options)
    -> std::vector<FusionChain>;

/// Apply the fusion pass to a plan, rewriting it in place.
auto apply_fusion_pass(ExecPlan& plan, const FusionOptions& options)
    -> Expected<void>;

}  // namespace sr::engine
