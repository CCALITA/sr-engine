# TypeId Registry + ValueBox Migration Design

## Goal
Replace entt-based type metadata and register_type usage across the engine with TypeId-driven registration, and redesign ValueBox to use an inline sum type plus a ref path for large/custom values.

## Context
The engine now has a canonical TypeId system based on encoded payloads and BLAKE3 hashing. Current runtime structures still rely on entt::meta_type and a shared_ptr<void> ValueBox. The next phase removes entt from engine/runtime and uses TypeId for signatures, plan validation, and ValueBox storage while keeping entt only in reflection helpers.

## Architecture
Runtime owns a TypeRegistry instance and passes it into KernelRegistry and compile-time validation. PortDesc, SlotSpec, EnvRequirement, and ValueBox store TypeId instead of entt::meta_type. Registration becomes a template helper that maps T to a name and registers it explicitly against the runtime registry. ValueBox becomes a tagged sum type for inline values and a shared_ptr path for large/custom types.

## Components
- TypeRegistry (runtime-owned): canonical TypeId mapping and lookup.
- TypeName<T> trait + register_type(TypeRegistry&, const char*): explicit registration for types used by kernels and runtime env.
- PortDesc/Signature/SlotSpec/EnvRequirement: switch to TypeId.
- ValueBox: stores TypeId + std::variant of inline values or shared_ptr<void>.
- Validation: plan compilation and runtime checks compare TypeId only.

## Data Flow
1. Runtime initialization registers core types via register_type(registry, name).
2. KernelRegistry derives signatures using TypeId from the registry.
3. DSL compile resolves declared types to TypeId and validates against kernel signatures.
4. Execution stores ValueBox slots with TypeId + inline/ref storage and validates reads by TypeId.

## ValueBox Storage Strategy
Inline values: int64_t, double, bool, std::string, std::chrono::steady_clock::time_point, std::monostate. Json and non-core types use the shared_ptr path. Templated set/get map T to a TypeId and validate stored ids without calling into the registry during hot paths.

## Performance and Concurrency
TypeId comparisons are integer-only. Registration occurs during runtime setup; after that the registry is read-mostly. ValueBox uses no shared mutable global state and remains safe under per-run slot ownership. Optionally, the registry can be frozen after setup to prevent late registrations.

## Error Handling
- TypeId mismatches return structured compile errors during plan compile.
- ValueBox get() asserts or returns error when TypeId does not match expected T.
- Registry detects hash collisions and fails fast.

## Testing
- Update tests to cover TypeId on PortDesc/SlotSpec/EnvRequirement.
- Add ValueBox tests for inline and ref storage, plus TypeId validation.
- Ensure compile-time validation catches TypeId mismatches.

## Migration Notes
- Remove entt::meta_type from engine/runtime headers.
- Keep entt in reflection helpers only.
- Update kernel modules to register types via TypeRegistry and TypeName<T>.
- Update all plan/executor paths to use TypeId.
