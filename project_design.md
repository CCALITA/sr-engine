Dynamic DAG Engine（工业级在线低延迟计算服务）架构设计（Markdown）

核心技术栈：C++20 / ENTT / stdexec Sender-Receiver(P2300) / nlohmann/json
核心定位：在线推荐 / 检索 / 风控 / 策略等系统的通用“计算编排 + 低延迟执行”框架
编排方式：port-bind 风格 JSON DSL → resolve → 可执行计划（ExecPlan）
执行方式：以 stdexec 调度 + completion-driven 运行时为核心抽象，kernel 通过 callable 适配接入

目录

1. 背景与目标

2. 总体架构

3. 核心概念与数据模型

4. JSON DSL 设计（port-bind）

5. Resolve/Compile：从 DSL 到 ExecPlan

6. Kernel 插件模型：ENTT meta + Callable + Registry

7. 执行模型：stdexec 组合、调度、取消与超时

8. 并发与性能：低延迟关键路径

9. 观测性与工程化能力

10. 典型业务形态模板

11. 测试、验证与演进

12. 附录：代码骨架与示例

1. 背景与目标
1.1 业务背景（推荐/检索/风控/策略常见形态）

这些系统普遍具备：

多阶段流水线：召回/检索 → 粗排 → 精排 → 混排 → 规则/风控 → 输出

强异构 I/O：KV/Embedding/ANN/FeatureStore/模型推理/外部服务

高并发低延迟：P99/P999 约束、尾延迟治理

可配置化：快速实验、A/B、灰度、策略热更新

可观测可回放：线上问题定位、链路追踪、可复现

1.2 设计目标

功能目标

用 JSON DSL 描述 DAG：节点（kernel）+ 端口（port）+ 边（binding）+ 常量/环境变量/条件分支

Resolve 阶段完成：schema 校验、类型校验、拓扑排序、资源需求分析、编译为 ExecPlan

Exec 阶段完成：基于 stdexec 的组合执行、并发调度、取消/超时、错误传播、指标埋点

性能目标（方向性）

单请求执行开销尽量“接近手写 pipeline”

解析/resolve 放到配置加载期，请求期只做轻量执行

尽量少的锁、少的堆分配、少的虚调用（热路径）

工程目标

Kernel 插件可独立编译/注册/灰度

DAG 引擎与业务 kernel 解耦：通过 KernelRegistry + callable 签名反射实现可组合的 API

可测试：每个 kernel、每个子图、完整图均可离线跑

1.3 非目标

不做通用分布式 DAG 调度器（非 Airflow/Spark）

不在引擎层强绑定具体 RPC/存储实现（通过 adapter）

2. 总体架构
2.1 模块视图
flowchart LR
  A[JSON DSL] --> B[Parser nlohmann/json]
  B --> C[Resolver/Validator]
  C --> D[Compiler -> ExecPlan]
  D --> E[Runtime Executor]
  E --> F[stdexec 调度执行]
  E --> G[Observability: tracing/metrics/log]
  E --> H[Resource: pools/cache/threads]
  I[Kernel Library] --> C
  I --> D
  I --> E
  J[Admin/Hot Reload] --> C

2.2 核心思想：ENTT 负责“图的结构与解析”，stdexec 负责“执行的组合”

ENTT registry：作为 resolve/compile 的“图数据容器”

node/edge/port/type 等都作为 component 存放

resolve 阶段在 registry 上做规则校验与编译

stdexec sender/receiver：作为 runtime 的调度/任务编排基础

每个节点在选定 scheduler 上 schedule -> then 执行 callable

完成后以 completion-driven 方式触发下游（依赖计数归零即调度）

Kernel 通过 callable 适配：输入参数与返回值推导 Signature，端口名由 DSL 指定

3. 核心概念与数据模型
3.1 概念表
概念	含义	关键点
Node	DAG 节点，一个 kernel 实例	可带参数、可多实例
Port	输入/输出端口	强类型/弱类型、单值/多值
Binding（Edge）	端口连接关系	src.out -> dst.in
Env Binding	从请求上下文/环境取值	"$req.uid" 等
Const Binding	常量注入	JSON literal
ExecPlan	可执行计划	请求期零解析、少分配
RequestContext	单请求上下文	TraceContext、deadline、缓存、arena
3.2 ENTT registry 组件建议（resolve/compile 用）

NodeComponent { std::string kernel_name; json params; }

PortTableComponent { vector<InPort>, vector<OutPort> }

EdgeComponent { entt::entity from_node; int from_port; entt::entity to_node; int to_port; }

TypeComponent { type_id / meta_type / enum TypeTag }

TopoComponent { int topo_order; }

CompiledNodeComponent { function pointers / vtable / indices }

PlanStorageComponent { contiguous arrays for runtime }

经验：resolve 阶段可以“随意用 string/map”，compile 阶段要把一切压到整数索引 + contiguous array。

4. JSON DSL 设计（port-bind）
4.1 DSL 顶层结构（建议）
{
  "version": 1,
  "name": "recall_rank_pipeline",
  "nodes": [
    { "id": "recall", "kernel": "ann_recall", "params": { "topk": 300 }, "inputs": ["query", "uid"], "outputs": ["items"] },
    { "id": "feat",   "kernel": "feature_fetch", "params": { "timeout_ms": 8 }, "inputs": ["items"], "outputs": ["items"] },
    { "id": "rank",   "kernel": "ltr_rank", "params": { "model": "rank_v7" }, "inputs": ["items", "user"], "outputs": ["scored_items"] }
  ],
  "bindings": [
    { "to": "recall.query", "from": "$req.query" },
    { "to": "recall.uid",   "from": "$req.uid" },

    { "to": "feat.items",   "from": "recall.items" },
    { "to": "rank.items",   "from": "feat.items" },
    { "to": "rank.user",    "from": "$req.user_profile" }
  ],
  "outputs": [
    { "from": "rank.scored_items", "as": "result" }
  ]
}

4.2 Port-Bind 规则（工业落地建议：严格、可诊断）

端口名唯一且显式：引用必须是 node.port

输入端口必须满足：每个 required input 必须被绑定（from/env/const）

类型可校验：允许 Exact / Convertible / Any 三种模式（见 5.2）

多重绑定策略明确：同一输入端口是否允许多条 binding（一般禁止；除非声明为 multi 聚合端口）

4.3 条件分支/开关（常见 A/B、风控 gating）

推荐把条件做成显式 kernel（更可观测），例如：

{ "id": "gate", "kernel": "if_else", "inputs": ["cond", "then", "else"], "outputs": ["value"] }


bindings：

gate.cond <- $req.ab.bucket("exp1")

gate.then <- rank_v7.scored_items

gate.else <- rank_v6.scored_items

5. Resolve/Compile：从 DSL 到 ExecPlan
5.1 Pipeline

Parse：nlohmann/json → AST

Build IR：AST → ENTT registry（nodes/ports/edges）

Resolve：kernel 查找、port schema 注入、参数解码

Validate：拓扑、缺参、类型、环检测、策略检查

Compile：生成 ExecPlan（紧凑布局 + 索引化 + 预绑定）

Publish：原子替换 plan（读写隔离，支持热更新）

flowchart LR
  DSL --> Parse --> IR[ENTT registry IR]
  IR --> Resolve --> Validate --> Compile[ExecPlan]
  Compile --> Runtime

5.2 类型系统策略（强类型与工程可落地的折中）

三层类型：

Static：C++ 类型（如 std::vector<Item>）

Meta：ENTT meta_type（用于运行期反射/校验/构造）

Wire：DSL 声明类型（可选），用于跨语言/配置提示

连接校验模式：

Exact：meta_type 完全一致

Convertible：允许注册转换（例如 int -> int64, string -> enum）

Any：允许 type-erasure（但会引入运行期开销，建议仅边界/调试用）

经验：在线系统建议默认 Exact，对少数端口允许 Convertible，Any 只给“边缘适配层”。

6. Kernel 插件模型：ENTT meta + Callable + Registry
6.1 Kernel 需要提供什么

引擎希望以统一方式获得：

端口签名（inputs/outputs + 类型 + required/multi）：由 callable 参数/返回值推导

执行入口：直接调用 callable，可选 RequestContext 作为首参

运行属性：TaskType（Compute/Io）与可选参数工厂

6.2 用 Callable + Registry 设计“最小侵入”接口

示意（概念代码）：

sr::engine::KernelRegistry registry;
registry.register_kernel("add", [](int64_t a, int64_t b) { return a + b; });
registry.register_kernel_with_params("mul", [](const Json& params) {
  auto factor = params.at("factor").get<int64_t>();
  return [factor](int64_t v) { return v * factor; };
});
registry.register_kernel(
  "format",
  [](const std::string& prefix, int64_t value) -> std::string {
    return std::format("{}{}", prefix, value);
  },
  TaskType::Io);

输入/输出端口名由 DSL 的 inputs/outputs 提供；引擎在 compile 时校验端口数量和类型。

6.3 ENTT meta 注册（类型注册）

引擎通过 register_type<T>("name") 注册可用数据类型

compile 阶段用 meta_type 做类型校验与运行期存储

7. 执行模型：stdexec 组合、调度、取消与超时
7.1 ExecPlan 的 runtime 形态（关键）

请求期不再使用 string 查找、不再遍历 JSON。建议：

节点数组：nodes[i] 包含

kernel instance（或轻量 handle）

输入 slot index 列表、输出 slot index 列表

执行函数指针：Expected<void> (*compute)(void*, RequestContext&, InputValues, OutputValues)

slot 存储：arena 分配的 ValueSlot[]（按类型对齐/或 type-erasure）

拓扑顺序：topo[] 或 dependents/pending_counts（用于 completion-driven 调度）

7.2 DAG 调度策略（completion-driven）

每个节点维护 pending 计数，计数归零时立刻调度

节点完成后递减 dependents 的 pending，触发新的可运行节点

调度时按 TaskType 选择计算池或 IO 池，使用 schedule + then + start_detached

7.3 取消与超时（必须一等公民）

RequestContext 含 deadline、stop_source/stop_token

每个 kernel 执行必须尊重 stop_token（或至少在 IO 边界处检查）

引擎在最外层提供：

每个节点执行前检查 RequestContext 的取消/超时状态

发生取消/超时时停止调度并返回错误

8. 并发与性能：低延迟关键路径
8.1 关键路径原则（强建议）

Resolve/Compile 离线化：请求期不要碰 JSON/string/map

索引化：所有 node/port 在 compile 后都是 int index

内存策略：RequestContext 持有 arena（bump allocator），slot 与中间对象尽量在 arena 上构造

减少虚调用：compile 成函数指针或小 vtable；热点可用 CRTP/模板内联（对少量核心 kernel）

I/O 与 CPU 分池：避免 IO 回调阻塞 CPU 线程

尾延迟治理：超时、降级、fallback、缓存（per-request / shared）

8.2 并发安全模型

ExecPlan 只读共享（多线程安全）

RequestContext 每请求独享

Kernel 实例建议：

无状态：共享单例（最优）

只读状态：共享（模型参数、字典）

有状态/缓存：放到 ctx（每请求）或使用分片并发结构（sharded cache）

8.3 常见优化点

slot 采用 “typed slot + index” 而不是 std::any

常量/小对象走 SBO（small buffer）

预构建拓扑 level，减少 runtime 调度开销

热点 kernel 支持批量接口（micro-batch），引擎可在同层合并执行

9. 观测性与工程化能力
9.1 Trace / Span

TraceContext 位于 RequestContext（每请求），默认禁用、零开销；启用时由 TraceSampler 一次性决定 TraceFlags。

TraceSinkRef 采用函数指针擦除（无虚表/无分配），sink 可选择实现 on_run_start/on_node_start/on_node_end/on_node_error/on_queue_delay。

事件模型（POD，string_view 仅在回调期间有效）：
RunStart/RunEnd、NodeStart/NodeEnd、NodeError、QueueDelay。

字段包含 trace_id、span_id、node_id、node_index、task_type、ticks（TraceClock 输出）。

集成点：Executor::run 发 RunStart/RunEnd；schedule_node 记录 enqueue_tick；execute_node 发 NodeStart/NodeEnd/NodeError。

线程安全：事件在 worker 线程发出，TraceSink 必须自行保证并发安全。

编译期开关：定义 SR_TRACE_DISABLED 可完全剔除 tracing 代码路径。

支持把“binding 解析结果”打印成可读拓扑，便于定位配置错误

9.2 Metrics

per-node：QPS、P50/P90/P99、error rate、timeout rate

per-plan：编译耗时、版本号、热更新次数、回滚次数

资源：线程池队列长度、arena 使用量、cache hit rate

9.3 日志与回放

支持把 RequestContext 的关键输入（脱敏）与 DAG version 记录，用于离线 replay

失败时输出：缺失端口/类型不匹配/环信息/节点参数错误的精确诊断

10. 典型业务形态模板
10.1 推荐：召回 + 特征 + 排序 + 混排 + 规则

并行召回：多路 recall（ANN/倒排/协同）→ merge

feature fetch 并行化（按域拆分）

rank 支持多模型 A/B

post rule（风控/多样性/去重）作为显式节点

10.2 检索：query rewrite + 多路检索 + rerank

rewrite（轻模型）与检索并行

rerank 与 snippet 生成分层执行（超时可降级）

10.3 风控/策略：规则图 + 外部信号

大量轻量规则节点：要求极低 overhead

外部依赖强超时与 fallback：timeout -> default 模式

11. 测试、验证与演进
11.1 测试金字塔

单 kernel：输入→输出（包含超时/取消）

子图：固定上下文的 DAG 片段回归

全图：金丝雀配置 + replay 流量

Fuzz：对 DSL 随机生成 bindings，验证 resolver 诊断稳定性

11.2 配置演进

DSL version + 兼容策略（向后兼容优先）

ExecPlan 带 hash（node+params+bindings），便于缓存与回滚

热更新：双缓冲（old plan 继续服务，new plan 编译成功再切）

12. 附录：代码骨架与示例
12.1 ExecPlan 结构（示意）
struct ValueSlot {
  entt::meta_type type;
  void* ptr;            // 指向 arena 内对象
};

struct CompiledNode {
  // 输入输出 slot 索引
  std::span<const int> in_slots;
  std::span<const int> out_slots;

  // 运行入口：直接 compute 写入 OutputValues
  Expected<void> (*compute)(void* kernel_instance, RequestContext&, const InputValues&, OutputValues&);
  void* kernel_instance;
};

struct ExecPlan {
  std::vector<CompiledNode> nodes;
  std::vector<int> topo;           // 或 levels
  // 共享资源引用：线程池、模型、字典等（只读）
};

12.2 Port 签名表示（建议）
enum class Cardinality { Single, Multi };
struct PortDesc {
  std::string_view name;
  entt::meta_type type;
  bool required;
  Cardinality card;
};
struct Signature {
  std::vector<PortDesc> inputs;
  std::vector<PortDesc> outputs;
};
