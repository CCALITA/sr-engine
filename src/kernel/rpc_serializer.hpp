#ifndef SR_RPC_SERIALIZER_HPP
#define SR_RPC_SERIALIZER_HPP

#include "kernel/rpc_kernels.hpp"

#include "engine/error.hpp"
#include "engine/types.hpp"

#include <cstddef>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

namespace sr::kernel::rpc {

struct JsonSerializer {
  auto get_name() const -> std::string_view { return "json"; }

  auto content_type() const -> std::string_view { return "application/json"; }

  auto serialize(const engine::Json& data) -> engine::Expected<std::vector<std::byte>> {
    try {
      auto str = data.dump();
      std::vector<std::byte> result(str.size());
      std::memcpy(result.data(), str.data(), str.size());
      return result;
    } catch (const std::exception& e) {
      return tl::unexpected(engine::make_error(
          std::format("JSON serialization failed: {}", e.what())));
    }
  }

  auto deserialize(const std::vector<std::byte>& bytes) -> engine::Expected<engine::Json> {
    try {
      auto str = std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
      return engine::Json::parse(str);
    } catch (const std::exception& e) {
      return tl::unexpected(engine::make_error(
          std::format("JSON deserialization failed: {}", e.what())));
    }
  }

  auto supports_type(const std::string& type_name) const -> bool {
    return type_name == "json" || type_name == "Json" ||
           type_name == "string" || type_name == "int" ||
           type_name == "double" || type_name == "bool" ||
           type_name == "array" || type_name == "object";
  }
};

struct FlatBufferSerializer {
  auto get_name() const -> std::string_view { return "flatbuffer"; }

  auto content_type() const -> std::string_view { return "application/flatbuffers"; }

  auto serialize(const engine::Json& data) -> engine::Expected<std::vector<std::byte>> {
    return tl::unexpected(engine::make_error(
        "FlatBuffer serialization not yet implemented"));
  }

  auto deserialize(const std::vector<std::byte>& bytes) -> engine::Expected<engine::Json> {
    return tl::unexpected(engine::make_error(
        "FlatBuffer deserialization not yet implemented"));
  }

  auto supports_type(const std::string& type_name) const -> bool {
    return type_name == "flatbuffer";
  }
};

} // namespace sr::kernel::rpc

#endif // SR_RPC_SERIALIZER_HPP
