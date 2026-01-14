#include <arpa/inet.h>
#include <chrono>
#include <cstring>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "engine/registry.hpp"
#include "kernel/rpc_kernels.hpp"
#include "kernel/sample_kernels.hpp"
#include "runtime/runtime.hpp"
#include "runtime/serve.hpp"

// Include the C API. Using relative path for simplicity.
#include "../thirdparty/sr-proto/api_c.h"

namespace {

constexpr const char *kSocketPath = "/tmp/sr-ipc.sock";
constexpr const char *kGraphName = "milvus_sr_ipc_handler";

// --- Serialization Helpers ---

struct ByteBuffer {
  std::vector<char> data;

  void append_u32(uint32_t val) {
    uint32_t net_val = htonl(val);
    const char *p = reinterpret_cast<const char *>(&net_val);
    data.insert(data.end(), p, p + 4);
  }

  void append_u64(uint64_t val) {
    uint32_t high = htonl(val >> 32);
    uint32_t low = htonl(val & 0xFFFFFFFF);
    const char *ph = reinterpret_cast<const char *>(&high);
    const char *pl = reinterpret_cast<const char *>(&low);
    data.insert(data.end(), ph, ph + 4);
    data.insert(data.end(), pl, pl + 4);
  }

  void append_string(const std::string &str) {
    append_u32(static_cast<uint32_t>(str.size()));
    data.insert(data.end(), str.begin(), str.end());
  }

  void append_bytes(const void *ptr, size_t size) {
    append_u32(static_cast<uint32_t>(size));
    const char *p = reinterpret_cast<const char *>(ptr);
    data.insert(data.end(), p, p + size);
  }

  // Helper to prepare the final IPC response payload:
  // [app_code][msg_len][msg][data...]
  static grpc::ByteBuffer
  make_response(int32_t code, const std::string &msg,
                const std::vector<char> &output_data = {}) {
    ByteBuffer buf;
    buf.append_u32(static_cast<uint32_t>(code));
    buf.append_string(msg);
    if (!output_data.empty()) {
      buf.data.insert(buf.data.end(), output_data.begin(), output_data.end());
    }

    grpc::Slice slice(buf.data.data(), buf.data.size());
    return grpc::ByteBuffer(&slice, 1);
  }
};

class BinaryReader {
  const char *ptr_;
  size_t remaining_;

public:
  BinaryReader(const void *data, size_t size)
      : ptr_(static_cast<const char *>(data)), remaining_(size) {}

  bool read_u32(uint32_t &out) {
    if (remaining_ < 4)
      return false;
    uint32_t net_val;
    std::memcpy(&net_val, ptr_, 4);
    out = ntohl(net_val);
    ptr_ += 4;
    remaining_ -= 4;
    return true;
  }

  bool read_u64(uint64_t &out) {
    if (remaining_ < 8)
      return false;
    uint32_t high, low;
    std::memcpy(&high, ptr_, 4);
    std::memcpy(&low, ptr_ + 4, 4);
    out = (static_cast<uint64_t>(ntohl(high)) << 32) | ntohl(low);
    ptr_ += 8;
    remaining_ -= 8;
    return true;
  }

  bool read_string(std::string &out) {
    uint32_t len;
    if (!read_u32(len))
      return false;
    if (remaining_ < len)
      return false;
    out.assign(ptr_, len);
    ptr_ += len;
    remaining_ -= len;
    return true;
  }

  bool read_long_array(std::vector<int64_t> &out) {
    uint32_t len;
    if (!read_u32(len))
      return false;
    out.resize(len);
    for (uint32_t i = 0; i < len; ++i) {
      uint64_t val;
      if (!read_u64(val))
        return false;
      out[i] = static_cast<int64_t>(val);
    }
    return true;
  }
};

// Helper to convert grpc::ByteBuffer to string for reader
std::string byte_buffer_to_string(const grpc::ByteBuffer &buffer) {
  std::vector<grpc::Slice> slices;
  buffer.Dump(&slices);
  std::string data;
  for (const auto &slice : slices) {
    data.append(reinterpret_cast<const char *>(slice.begin()), slice.size());
  }
  return data;
}

// --- Dispatcher Kernel ---

using HandlerFunc = std::function<grpc::ByteBuffer(BinaryReader &)>;

std::map<std::string, HandlerFunc> handlers;

void register_handlers() {
  // handlers["CreateQueryContext"] = [](BinaryReader& reader) ->
  // grpc::ByteBuffer {
  //     std::string config_path, context_name;
  //     if (!reader.read_string(config_path) ||
  //     !reader.read_string(context_name)) {
  //         return ByteBuffer::make_response(-1, "Invalid arguments");
  //     }
  //     CQueryContext ctx = nullptr;
  //     CStatus status = CreateQueryContext(config_path.c_str(),
  //     context_name.c_str(), &ctx);

  //     ByteBuffer resp;
  //     resp.append_u64(reinterpret_cast<uint64_t>(ctx));
  //     return ByteBuffer::make_response(status.error_code, status.error_msg ?
  //     status.error_msg : "", resp.data);
  // };

  // handlers["CreateQueryContextV2"] = [](BinaryReader& reader) ->
  // grpc::ByteBuffer {
  //     std::string config, ctx_name, coll_name;
  //     if (!reader.read_string(config) || !reader.read_string(ctx_name) ||
  //     !reader.read_string(coll_name)) {
  //          return ByteBuffer::make_response(-1, "Invalid arguments");
  //     }
  //     CQueryContext ctx = nullptr;
  //     CStatus status = CreateQueryContextV2(config.c_str(), ctx_name.c_str(),
  //     coll_name.c_str(), &ctx);

  //     ByteBuffer resp;
  //     resp.append_u64(reinterpret_cast<uint64_t>(ctx));
  //     return ByteBuffer::make_response(status.error_code, status.error_msg ?
  //     status.error_msg : "", resp.data);
  // };

  handlers["InitContext"] = [](BinaryReader &reader) -> grpc::ByteBuffer {
    std::string config, ctx_name;
    if (!reader.read_string(config) || !reader.read_string(ctx_name)) {
      return ByteBuffer::make_response(-1, "Invalid arguments");
    }
    CStatus status = InitContext(config.c_str(), ctx_name.c_str());
    return ByteBuffer::make_response(status.error_code,
                                     status.error_msg ? status.error_msg : "");
  };

  handlers["GetCollCtxPrtStat"] = [](BinaryReader &reader) -> grpc::ByteBuffer {
    std::string ctx_name;
    uint64_t part_id;
    if (!reader.read_string(ctx_name) || !reader.read_u64(part_id)) {
      return ByteBuffer::make_response(-1, "Invalid arguments");
    }
    CCtxIndexStatus status =
        GetCollCtxPrtStat(ctx_name.c_str(), static_cast<int64_t>(part_id));

    ByteBuffer resp;
    resp.append_u32(static_cast<uint32_t>(status));
    return ByteBuffer::make_response(0, "", resp.data);
  };

  handlers["GetCollPrtStat"] = [](BinaryReader &reader) -> grpc::ByteBuffer {
    std::string ctx_name, coll_name;
    uint64_t part_id;
    if (!reader.read_string(ctx_name) || !reader.read_string(coll_name) ||
        !reader.read_u64(part_id)) {
      return ByteBuffer::make_response(-1, "Invalid arguments");
    }
    CIndexStatus status = GetCollPrtStat(ctx_name.c_str(), coll_name.c_str(),
                                         static_cast<int64_t>(part_id));

    ByteBuffer resp;
    resp.append_u32(static_cast<uint32_t>(status));
    return ByteBuffer::make_response(0, "", resp.data);
  };

  handlers["GetQueryCtxStat"] = [](BinaryReader &reader) -> grpc::ByteBuffer {
    uint64_t ptr_val;
    if (!reader.read_u64(ptr_val)) {
      return ByteBuffer::make_response(-1, "Invalid arguments");
    }
    CIndexStatus status =
        GetQueryCtxStat(reinterpret_cast<CQueryContext>(ptr_val));

    ByteBuffer resp;
    resp.append_u32(static_cast<uint32_t>(status));
    return ByteBuffer::make_response(0, "", resp.data);
  };

  handlers["ApplyConfig"] = [](BinaryReader &reader) -> grpc::ByteBuffer {
    std::string name, config;
    if (!reader.read_string(name) || !reader.read_string(config)) {
      return ByteBuffer::make_response(-1, "Invalid arguments");
    }
    CStatus status = ApplyConfig(name.c_str(), config.c_str());
    return ByteBuffer::make_response(status.error_code,
                                     status.error_msg ? status.error_msg : "");
  };

  handlers["AssignPartitions"] = [](BinaryReader &reader) -> grpc::ByteBuffer {
    std::string ctx_name;
    std::vector<int64_t> parts;
    if (!reader.read_string(ctx_name) || !reader.read_long_array(parts)) {
      return ByteBuffer::make_response(-1, "Invalid arguments");
    }
    CStatus status =
        AssignPartitions(ctx_name.c_str(), parts.data(), parts.size());
    return ByteBuffer::make_response(status.error_code,
                                     status.error_msg ? status.error_msg : "");
  };

  handlers["GetAssignedPrts"] = [](BinaryReader &reader) -> grpc::ByteBuffer {
    std::string ctx_name, coll_name;
    if (!reader.read_string(ctx_name) || !reader.read_string(coll_name)) {
      return ByteBuffer::make_response(-1, "Invalid arguments");
    }
    int64_t *partitions = nullptr;
    int64_t count = 0;
    CStatus status = GetAssignedPrts(ctx_name.c_str(), coll_name.c_str(),
                                     &partitions, &count);

    if (status.error_code != 0) {
      return ByteBuffer::make_response(
          status.error_code, status.error_msg ? status.error_msg : "");
    }

    ByteBuffer resp;
    resp.append_u32(static_cast<uint32_t>(count));
    for (int64_t i = 0; i < count; ++i) {
      resp.append_u64(static_cast<uint64_t>(partitions[i]));
    }
    DeletePartitionIds(partitions);

    return ByteBuffer::make_response(0, "", resp.data);
  };

  handlers["GetShardNum"] = [](BinaryReader &reader) -> grpc::ByteBuffer {
    std::string ctx_name, coll_name;
    if (!reader.read_string(ctx_name) || !reader.read_string(coll_name)) {
      return ByteBuffer::make_response(-1, "Invalid arguments");
    }
    int64_t shard_num = 0;
    CStatus status =
        GetShardNum(ctx_name.c_str(), coll_name.c_str(), &shard_num);

    if (status.error_code != 0) {
      return ByteBuffer::make_response(
          status.error_code, status.error_msg ? status.error_msg : "");
    }

    ByteBuffer resp;
    resp.append_u64(static_cast<uint64_t>(shard_num));
    return ByteBuffer::make_response(0, "", resp.data);
  };

  handlers["GetSnapshot"] = [](BinaryReader &reader) -> grpc::ByteBuffer {
    uint32_t level;
    if (!reader.read_u32(level)) {
      return ByteBuffer::make_response(-1, "Invalid arguments");
    }
    CSnapshot snapshot;
    CStatus status = GetSnapshot(&snapshot, static_cast<int>(level));

    if (status.error_code != 0) {
      return ByteBuffer::make_response(
          status.error_code, status.error_msg ? status.error_msg : "");
    }

    ByteBuffer resp;
    resp.append_bytes(snapshot.data, snapshot.length);
    DeleteSnapshot(&snapshot);

    return ByteBuffer::make_response(0, "", resp.data);
  };

  handlers["UpdateCollections"] = [](BinaryReader &reader) -> grpc::ByteBuffer {
    std::string ctx_name, config;
    if (!reader.read_string(ctx_name) || !reader.read_string(config)) {
      return ByteBuffer::make_response(-1, "Invalid arguments");
    }
    CStatus status = UpdateCollections(ctx_name.c_str(), config.c_str());
    return ByteBuffer::make_response(status.error_code,
                                     status.error_msg ? status.error_msg : "");
  };

  handlers["LoadIndex"] = [](BinaryReader &reader) -> grpc::ByteBuffer {
    std::string ctx_name, coll_name, config;
    if (!reader.read_string(ctx_name) || !reader.read_string(coll_name) ||
        !reader.read_string(config)) {
      return ByteBuffer::make_response(-1, "Invalid arguments");
    }
    CStatus status =
        LoadIndex(ctx_name.c_str(), coll_name.c_str(), config.c_str());
    return ByteBuffer::make_response(status.error_code,
                                     status.error_msg ? status.error_msg : "");
  };

  handlers["DeleteCollectionIndex"] =
      [](BinaryReader &reader) -> grpc::ByteBuffer {
    std::string ctx_name, coll_name, config;
    if (!reader.read_string(ctx_name) || !reader.read_string(coll_name) ||
        !reader.read_string(config)) {
      return ByteBuffer::make_response(-1, "Invalid arguments");
    }
    CStatus status = DeleteCollectionIndex(ctx_name.c_str(), coll_name.c_str(),
                                           config.c_str());
    return ByteBuffer::make_response(status.error_code,
                                     status.error_msg ? status.error_msg : "");
  };

  handlers["GetCollInfo"] = [](BinaryReader &reader) -> grpc::ByteBuffer {
    std::string ctx_name;
    if (!reader.read_string(ctx_name)) {
      return ByteBuffer::make_response(-1, "Invalid arguments");
    }
    uint32_t count = 0;
    if (!reader.read_u32(count)) {
      return ByteBuffer::make_response(-1, "Invalid arguments");
    }

    std::vector<std::string> names_storage(count);
    std::vector<const char *> names_ptr(count);
    for (uint32_t i = 0; i < count; ++i) {
      if (!reader.read_string(names_storage[i])) {
        return ByteBuffer::make_response(-1, "Invalid arguments");
      }
      names_ptr[i] = names_storage[i].c_str();
    }

    CSnapshot snapshot;
    CStatus status = GetCollInfo(ctx_name.c_str(), names_ptr.data(),
                                 static_cast<int>(count), &snapshot);

    if (status.error_code != 0) {
      return ByteBuffer::make_response(
          status.error_code, status.error_msg ? status.error_msg : "");
    }

    ByteBuffer resp;
    resp.append_bytes(snapshot.data, snapshot.length);
    DeleteSnapshot(&snapshot);
    return ByteBuffer::make_response(0, "", resp.data);
  };

  handlers["ReloadFlags"] = [](BinaryReader &reader) -> grpc::ByteBuffer {
    std::string config;
    if (!reader.read_string(config)) {
      return ByteBuffer::make_response(-1, "Invalid arguments");
    }
    CStatus status = ReloadFlags(config.c_str());
    return ByteBuffer::make_response(status.error_code,
                                     status.error_msg ? status.error_msg : "");
  };

  handlers["UnloadCollections"] = [](BinaryReader &reader) -> grpc::ByteBuffer {
    std::string ctx_name;
    if (!reader.read_string(ctx_name)) {
      return ByteBuffer::make_response(-1, "Invalid arguments");
    }
    uint32_t count = 0;
    if (!reader.read_u32(count)) {
      return ByteBuffer::make_response(-1, "Invalid arguments");
    }

    std::vector<std::string> names_storage(count);
    std::vector<const char *> names_ptr(count);
    for (uint32_t i = 0; i < count; ++i) {
      if (!reader.read_string(names_storage[i])) {
        return ByteBuffer::make_response(-1, "Invalid arguments");
      }
      names_ptr[i] = names_storage[i].c_str();
    }

    CStatus status = UnloadCollections(ctx_name.c_str(), names_ptr.data(),
                                       static_cast<int>(count));
    return ByteBuffer::make_response(status.error_code,
                                     status.error_msg ? status.error_msg : "");
  };
}

} // namespace

int main(int argc, char **argv) {
  register_handlers();

  sr::engine::Runtime runtime;
  sr::kernel::register_builtin_types();
  sr::kernel::register_rpc_kernels(runtime.registry());

  // Register our dispatcher kernel
  runtime.registry().register_kernel(
      "sr_api_dispatcher",
      [](const std::string &method, const grpc::ByteBuffer &payload) noexcept
          -> sr::engine::Expected<grpc::ByteBuffer> {
        try {
          auto it = handlers.find(method);
          if (it == handlers.end()) {
            return ByteBuffer::make_response(-1, "Unknown method: " + method);
          }
          std::string data = byte_buffer_to_string(payload);
          BinaryReader reader(data.data(), data.size());
          return it->second(reader);
        } catch (const std::exception &e) {
          return ByteBuffer::make_response(
              -1, std::string("Dispatcher error: ") + e.what());
        } catch (...) {
          return ByteBuffer::make_response(-1, "Dispatcher error: unknown");
        }
      });

  const char *dsl = R"JSON(
    {
      "version": 1,
      "name": "milvus_sr_ipc_handler",
      "nodes": [
        { "id": "dispatch", "kernel": "sr_api_dispatcher", "inputs": ["method", "payload"], "outputs": ["result"] },
        { "id": "reply", "kernel": "rpc_server_output", "params": {}, "inputs": ["call", "payload"]}
      ],
      "bindings": [
        { "to": "dispatch.method", "from": "$req.rpc.method" },
        { "to": "dispatch.payload", "from": "$req.rpc.payload" },
        { "to": "reply.payload", "from": "dispatch.result" },
        { "to": "reply.call", "from": "$req.rpc.call" }
      ],
      "outputs": []
    }
    )JSON";

  sr::engine::StageOptions stage_options;
  stage_options.source = "examples/sr_ipc_server.cpp";
  stage_options.publish = true;
  auto snapshot = runtime.stage_dsl(dsl, stage_options);
  if (!snapshot) {
    std::cerr << "Stage error: " << snapshot.error().message << "\n";
    return 1;
  }

  sr::engine::ServeEndpointConfig endpoint;
  endpoint.name = std::string(kGraphName);
  sr::engine::IpcServeConfig ipc;
  ipc.path = kSocketPath;
  ipc.io_threads = 2; // Slight increase for potential concurrency
  ipc.remove_existing = true;
  endpoint.transport = ipc;
  endpoint.request_threads = 4;
  endpoint.max_inflight = 16;
  endpoint.graph.metadata.name_header = "sr-graph-name";

  auto host = runtime.serve(endpoint);
  if (!host) {
    std::cerr << "Serve error: " << host.error().message << "\n";
    return 1;
  }

  std::cout << "SR IPC server listening on " << kSocketPath << "\n";
  std::cout << "Press Enter to stop the server.\n";
  std::string line;
  std::getline(std::cin, line);

  (*host)->shutdown();
  (*host)->wait();

  return 0;
}
