# TypeId ValueBox Migration Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace entt-based types and register_type usage across the engine with TypeId-based registration and a redesigned ValueBox.

**Architecture:** Runtime owns a TypeRegistry passed into KernelRegistry/compile. Port descriptors, slot specs, and env requirements carry TypeId. ValueBox stores TypeId plus a sum-type inline storage (core types) or shared_ptr for large/custom values. entt remains only in reflection helpers.

**Tech Stack:** C++20, existing engine runtime, canonical TypeId system.

### Task 1: Introduce TypeName mapping + register_type helper

**Files:**
- Create: `src/engine/type_names.hpp`
- Modify: `src/kernel/sample_kernels.cpp`
- Modify: `src/kernel/flight_kernels.cpp`
- Modify: `src/kernel/rpc_kernels.cpp`
- Test: `tests/type_system_test.cpp`

**Step 1: Write the failing test**

```cpp
// tests/type_system_test.cpp
struct ExampleType {};

auto test_register_type_mapping() -> bool {
  sr::engine::TypeRegistry registry;
  const auto id = sr::engine::register_type<ExampleType>(registry, "example_type");
  return id == registry.intern_primitive("example_type");
}
```

**Step 2: Run test to verify it fails**

Run: `cmake -S . -B build && cmake --build build -j 4 --target type_system_test && ctest --test-dir build -R type_system_test`
Expected: FAIL due to missing register_type helper.

**Step 3: Write minimal implementation**

```cpp
// src/engine/type_names.hpp
namespace sr::engine {

template <typename T>
struct TypeName;

template <typename T>
auto register_type(TypeRegistry &registry, const char *name) -> TypeId {
  TypeName<T>::set(name);
  return registry.intern_primitive(name);
}

}
```

**Step 4: Run test to verify it passes**

Run: `cmake -S . -B build && cmake --build build -j 4 --target type_system_test && ctest --test-dir build -R type_system_test`
Expected: PASS for `register_type_mapping`.

**Step 5: Commit**

```bash
git add src/engine/type_names.hpp tests/type_system_test.cpp src/kernel/sample_kernels.cpp src/kernel/flight_kernels.cpp src/kernel/rpc_kernels.cpp
git commit -m "feat: add type name registry"
```

### Task 2: Replace entt meta usage in PortDesc/Plan types

**Files:**
- Modify: `src/engine/types.hpp`
- Modify: `src/engine/plan.hpp`
- Modify: `src/engine/plan.cpp`
- Modify: `src/runtime/serve/rpc_env.cpp`
- Modify: `src/runtime/serve/flight.cpp`
- Test: `tests/engine_tests.cpp`

**Step 1: Write the failing test**

```cpp
// tests/engine_tests.cpp
auto test_plan_slot_uses_typeid() -> bool {
  sr::engine::SlotSpec slot{};
  return slot.type_id != 0 || slot.type_id == 0;
}
```

**Step 2: Run test to verify it fails**

Run: `cmake -S . -B build && cmake --build build -j 4 --target sr_engine_tests && ctest --test-dir build -R engine_tests`
Expected: FAIL due to missing type_id.

**Step 3: Write minimal implementation**

```cpp
// src/engine/plan.hpp
struct SlotSpec {
  TypeId type_id{};
};

struct EnvRequirement {
  std::string key;
  TypeId type_id{};
};
```

**Step 4: Run test to verify it passes**

Run: `cmake -S . -B build && cmake --build build -j 4 --target sr_engine_tests && ctest --test-dir build -R engine_tests`
Expected: PASS for plan type id test.

**Step 5: Commit**

```bash
git add src/engine/types.hpp src/engine/plan.hpp src/engine/plan.cpp src/runtime/serve/rpc_env.cpp src/runtime/serve/flight.cpp tests/engine_tests.cpp
git commit -m "refactor: replace entt meta with type ids"
```

### Task 3: Redesign ValueBox storage

**Files:**
- Modify: `src/engine/types.hpp`
- Modify: `src/runtime/executor.cpp`
- Modify: `src/runtime/executor.hpp`
- Modify: `src/runtime/frozen_env.hpp`
- Test: `tests/engine_tests.cpp`

**Step 1: Write the failing test**

```cpp
// tests/engine_tests.cpp
auto test_valuebox_inline_storage() -> bool {
  sr::engine::ValueBox box;
  box.set<int64_t>(42);
  return box.get<int64_t>() == 42;
}
```

**Step 2: Run test to verify it fails**

Run: `cmake -S . -B build && cmake --build build -j 4 --target sr_engine_tests && ctest --test-dir build -R engine_tests`
Expected: FAIL due to ValueBox signature changes.

**Step 3: Write minimal implementation**

```cpp
// src/engine/types.hpp
using InlineStorage = std::variant<std::monostate, int64_t, double, bool, std::string,
                                   std::chrono::steady_clock::time_point, std::shared_ptr<void>>;
struct ValueBox {
  TypeId type_id{};
  InlineStorage storage;
  template <typename T> void set(T value);
  template <typename T> T &get();
};
```

**Step 4: Run test to verify it passes**

Run: `cmake -S . -B build && cmake --build build -j 4 --target sr_engine_tests && ctest --test-dir build -R engine_tests`
Expected: PASS for ValueBox inline storage.

**Step 5: Commit**

```bash
git add src/engine/types.hpp src/runtime/executor.cpp src/runtime/executor.hpp src/runtime/frozen_env.hpp tests/engine_tests.cpp
git commit -m "refactor: redesign valuebox storage"
```

### Task 4: Update kernel signature and registry usage

**Files:**
- Modify: `src/engine/registry.hpp`
- Modify: `src/engine/registry.cpp`
- Modify: `src/kernel/sample_kernels.cpp`
- Modify: `src/kernel/flight_kernels.cpp`
- Modify: `src/kernel/rpc_kernels.cpp`
- Test: `tests/engine_tests.cpp`

**Step 1: Write the failing test**

```cpp
// tests/engine_tests.cpp
auto test_kernel_signature_uses_typeid() -> bool {
  sr::engine::Signature sig;
  return sig.inputs.empty() || sig.inputs[0].type_id == 0;
}
```

**Step 2: Run test to verify it fails**

Run: `cmake -S . -B build && cmake --build build -j 4 --target sr_engine_tests && ctest --test-dir build -R engine_tests`
Expected: FAIL due to Signature type updates.

**Step 3: Write minimal implementation**

```cpp
// src/engine/types.hpp
struct PortDesc {
  NameId name_id{};
  TypeId type_id{};
  bool required = true;
  Cardinality cardinality = Cardinality::Single;
};
```

**Step 4: Run test to verify it passes**

Run: `cmake -S . -B build && cmake --build build -j 4 --target sr_engine_tests && ctest --test-dir build -R engine_tests`
Expected: PASS for signature type id.

**Step 5: Commit**

```bash
git add src/engine/registry.hpp src/engine/registry.cpp src/engine/types.hpp src/kernel/sample_kernels.cpp src/kernel/flight_kernels.cpp src/kernel/rpc_kernels.cpp tests/engine_tests.cpp
git commit -m "refactor: migrate signatures to type ids"
```
