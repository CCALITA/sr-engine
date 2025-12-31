  - ExecPlan itself: nodes, topology, bindings, dependents, pending_counts, slot specs, output_slots, and const_slots.
  - KernelHandle per node, including kernel.compute and kernel.instance (std::shared_ptr<void>). This means kernel instances are
    shared across runs; they must be thread‑safe or immutable.
  - Executor resources: thread pools, dispatcher queues/workers, and the buffer pool.

    offsets, pending/scheduled, and error/remaining flags.
  - InputValues/OutputValues views are created on the fly per node execution.
  - ExecResult outputs map.

  Concurrency implication: because kernel.instance is shared, any mutable kernel state must be synchronized or moved into
  RequestContext/per‑run state. If you need per‑run kernel state, we should switch to cloning kernel instances per execution
  (e.g., via factory or cloneable handle).