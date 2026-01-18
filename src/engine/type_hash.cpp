#include "engine/type_hash.hpp"

extern "C" {
#include "blake3.h"
}

namespace sr::engine {

auto hash_type_bytes(std::string_view payload) -> TypeDigest {
  TypeDigest digest{};
  blake3_hasher hasher;
  blake3_hasher_init(&hasher);
  blake3_hasher_update(&hasher, payload.data(), payload.size());
  blake3_hasher_finalize(&hasher, digest.bytes.data(), digest.bytes.size());
  return digest;
}

} // namespace sr::engine
