#include "test_support.hpp"

auto test_trace_parallel_runs() -> bool {
  if constexpr (!sr::engine::trace::kTraceEnabled) {
    return true;
  }

  auto fail = [](const std::string &message) -> bool {
    std::cerr << "[trace_parallel_runs] " << message << "\n";
    return false;
  };

  struct TraceEvent {
    enum class Kind { RunStart, RunEnd, NodeStart, NodeEnd };
    Kind kind = Kind::RunStart;
    sr::engine::trace::TraceId trace_id = 0;
    sr::engine::trace::SpanId span_id = 0;
    int node_index = -1;
    std::size_t index = 0;
  };

  struct TraceCollector {
    std::mutex mutex;
    std::vector<TraceEvent> events;

    void on_run_start(const sr::engine::trace::RunStart &event) {
      push(TraceEvent::Kind::RunStart, event.trace_id, event.span_id, -1);
    }

    void on_run_end(const sr::engine::trace::RunEnd &event) {
      push(TraceEvent::Kind::RunEnd, event.trace_id, event.span_id, -1);
    }

    void on_node_start(const sr::engine::trace::NodeStart &event) {
      push(TraceEvent::Kind::NodeStart, event.trace_id, event.span_id,
           event.node_index);
    }

    void on_node_end(const sr::engine::trace::NodeEnd &event) {
      push(TraceEvent::Kind::NodeEnd, event.trace_id, event.span_id,
           event.node_index);
    }

    auto snapshot() -> std::vector<TraceEvent> {
      std::lock_guard<std::mutex> lock(mutex);
      return events;
    }

  private:
    void push(TraceEvent::Kind kind, sr::engine::trace::TraceId trace_id,
              sr::engine::trace::SpanId span_id, int node_index) {
      std::lock_guard<std::mutex> lock(mutex);
      std::size_t index = events.size();
      events.push_back(TraceEvent{kind, trace_id, span_id, node_index, index});
    }
  };

  struct SpanKey {
    sr::engine::trace::TraceId trace_id = 0;
    sr::engine::trace::SpanId span_id = 0;
    auto operator==(const SpanKey &other) const -> bool {
      return trace_id == other.trace_id && span_id == other.span_id;
    }
  };

  struct SpanKeyHash {
    auto operator()(const SpanKey &key) const -> std::size_t {
      std::size_t seed = std::hash<sr::engine::trace::TraceId>{}(key.trace_id);
      return seed ^ (std::hash<sr::engine::trace::SpanId>{}(key.span_id) << 1);
    }
  };

  const char *dsl = R"JSON(
  {
    "version": 1,
    "name": "trace_parallel",
    "nodes": [
      { "id": "sum", "kernel": "add", "inputs": ["a", "b"], "outputs": ["sum"] },
      { "id": "scale", "kernel": "mul", "params": { "factor": 4 }, "inputs": ["value"], "outputs": ["product"] }
    ],
    "bindings": [
      { "to": "sum.a", "from": "$req.x" },
      { "to": "sum.b", "from": 1 },
      { "to": "scale.value", "from": "sum.sum" }
    ],
    "outputs": [
      { "from": "scale.product", "as": "out" }
    ]
  }
  )JSON";

  sr::engine::GraphDef graph;
  std::string error;
  if (!parse_graph(dsl, graph, error)) {
    return fail(std::format("parse error: {}", error));
  }

  auto registry = make_registry();
  auto plan = sr::engine::compile_plan(graph, registry);
  if (!plan) {
    return fail(std::format("compile error: {}", plan.error().message));
  }

  TraceCollector collector;
  auto sink = sr::engine::trace::make_sink(collector);
  const std::size_t node_count = plan->nodes.size();

  constexpr int kThreads = 4;
  constexpr int kIterations = 25;
  std::atomic<int> failures{0};
  std::atomic<sr::engine::trace::TraceId> next_trace_id{1};

  exec::static_thread_pool pool(2);
  sr::engine::Executor executor(&pool);
  const sr::engine::ExecPlan &compiled = *plan;

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&, i]() {
      uint64_t seed = 0x517cc1b727220a95ULL + static_cast<uint64_t>(i);
      for (int iter = 0; iter < kIterations; ++iter) {
        seed = seed * 2862933555777941757ULL + 3037000493ULL;
        int64_t x = static_cast<int64_t>(seed % 1000);
        sr::engine::RequestContext ctx;
        ctx.set_env<int64_t>("x", x);
        ctx.trace.sink = sink;
        ctx.trace.flags =
            sr::engine::trace::to_flags(sr::engine::trace::TraceFlag::RunSpan) |
            sr::engine::trace::to_flags(sr::engine::trace::TraceFlag::NodeSpan);
        ctx.trace.trace_id = next_trace_id.fetch_add(1);
        ctx.trace.next_span.store(1, std::memory_order_relaxed);
        auto result = executor.run(compiled, ctx);
        if (!result) {
          failures.fetch_add(1);
          return;
        }
        const auto &value = result->outputs.at("out").get<int64_t>();
        const int64_t expected = (x + 1) * 4;
        if (value != expected) {
          failures.fetch_add(1);
          return;
        }
      }
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  if (failures.load() != 0) {
    return fail("run failures detected");
  }

  const auto events = collector.snapshot();
  if (events.empty()) {
    return fail("no trace events captured");
  }

  struct RunInfo {
    bool start = false;
    bool end = false;
    std::size_t start_index = 0;
    std::size_t end_index = 0;
    int node_starts = 0;
    int node_ends = 0;
  };

  struct SpanInfo {
    bool start = false;
    bool end = false;
    std::size_t start_index = 0;
    std::size_t end_index = 0;
    int node_index = -1;
  };

  std::unordered_map<sr::engine::trace::TraceId, RunInfo> runs;
  std::unordered_map<SpanKey, SpanInfo, SpanKeyHash> spans;

  for (const auto &event : events) {
    auto &run = runs[event.trace_id];
    switch (event.kind) {
    case TraceEvent::Kind::RunStart: {
      if (run.start) {
        return fail(
            std::format("duplicate run_start for trace_id {}", event.trace_id));
      }
      run.start = true;
      run.start_index = event.index;
      break;
    }
    case TraceEvent::Kind::RunEnd: {
      if (run.end) {
        return fail(
            std::format("duplicate run_end for trace_id {}", event.trace_id));
      }
      run.end = true;
      run.end_index = event.index;
      break;
    }
    case TraceEvent::Kind::NodeStart: {
      run.node_starts += 1;
      if (event.node_index < 0 ||
          static_cast<std::size_t>(event.node_index) >= node_count) {
        return fail(std::format("invalid node_index {} for trace_id {}",
                                event.node_index, event.trace_id));
      }
      SpanKey key{event.trace_id, event.span_id};
      auto &span = spans[key];
      if (span.start) {
        return fail(
            std::format("duplicate node_start span {} for trace_id {} (prev "
                        "index {} node {}, new index {} node {})",
                        event.span_id, event.trace_id, span.start_index,
                        span.node_index, event.index, event.node_index));
      }
      span.start = true;
      span.start_index = event.index;
      span.node_index = event.node_index;
      break;
    }
    case TraceEvent::Kind::NodeEnd: {
      run.node_ends += 1;
      SpanKey key{event.trace_id, event.span_id};
      auto &span = spans[key];
      if (!span.start || span.end) {
        return fail(std::format(
            "node_end without matching start span {} for trace_id {}",
            event.span_id, event.trace_id));
      }
      if (span.node_index != event.node_index) {
        return fail(std::format("node_end mismatch span {} for trace_id {}",
                                event.span_id, event.trace_id));
      }
      span.end = true;
      span.end_index = event.index;
      break;
    }
    }
  }

  const int expected_runs = kThreads * kIterations;
  if (static_cast<int>(runs.size()) != expected_runs) {
    return fail(
        std::format("expected {} runs, got {}", expected_runs, runs.size()));
  }

  for (const auto &[trace_id, run] : runs) {
    if (!run.start || !run.end) {
      return fail(
          std::format("missing run_start/run_end for trace_id {}", trace_id));
    }
    if (run.start_index >= run.end_index) {
      return fail(
          std::format("run_start after run_end for trace_id {}", trace_id));
    }
    if (run.node_starts != static_cast<int>(node_count) ||
        run.node_ends != static_cast<int>(node_count)) {
      return fail(
          std::format("trace_id {} node count mismatch starts={} ends={}",
                      trace_id, run.node_starts, run.node_ends));
    }
  }

  for (const auto &[key, span] : spans) {
    if (!span.start || !span.end) {
      return fail(std::format("incomplete span {} for trace_id {}", key.span_id,
                              key.trace_id));
    }
    if (span.start_index >= span.end_index) {
      return fail(std::format("span start after end {} for trace_id {}",
                              key.span_id, key.trace_id));
    }
  }

  return true;
}
