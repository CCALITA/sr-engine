#include "engine/type_system.hpp"
#include <sstream>
#include <unordered_map>
#include <vector>

namespace sr::engine {

namespace {

auto hash_canonical(std::string_view kind, std::string_view name) -> TypeFingerprint {
    TypeFingerprint fp;
    // Simple mixing for now
    std::size_t h = 0;
    for (char c : kind) h = h * 31 + c;
    for (char c : name) h = h * 31 + c;
    
    // Spread to bytes
    for (int i = 0; i < 16; ++i) {
        fp.bytes[i] = (h >> (i % 8)) & 0xFF;
        if (i % 2 == 0) h = h * 3; 
    }
    return fp;
}

auto truncate64(const TypeFingerprint& fp) -> TypeId {
    TypeId id = 0;
    for (int i = 0; i < 8; ++i) {
        id |= (static_cast<TypeId>(fp.bytes[i]) << (i * 8));
    }
    return id;
}

class TypeRegistryImpl : public TypeRegistry {
public:
    auto intern_primitive(std::string_view name) -> TypeId override {
        auto fp = hash_canonical("prim", name);
        auto id = truncate64(fp);
        
        if (id_map_.find(id) == id_map_.end()) {
            TypeInfo info;
            info.name = std::string(name);
            info.id = id;
            info.fp = fp;
            id_map_[id] = std::move(info);
        }
        return id;
    }

    auto intern_function(std::span<const TypeId> inputs,
                         std::span<const TypeId> outputs,
                         FunctionAttrs attrs) -> TypeId override {
        std::stringstream ss;
        ss << "fn";
        if (attrs.noexcept_) ss << ".noexcept";
        if (attrs.async) ss << ".async";
        if (attrs.has_ctx) ss << ".ctx";
        
        ss << "(";
        for (auto id : inputs) ss << id << ",";
        ss << ")->(";
        for (auto id : outputs) ss << id << ",";
        ss << ")";
        
        std::string key = ss.str();
        auto fp = hash_canonical("func", key);
        auto id = truncate64(fp);
        
        if (id_map_.find(id) == id_map_.end()) {
            TypeInfo info;
            info.name = key;
            info.id = id;
            info.fp = fp;
            id_map_[id] = std::move(info);
        }
        return id;
    }

    auto lookup(TypeId id) const -> const TypeInfo * override {
        auto it = id_map_.find(id);
        if (it != id_map_.end()) {
            return &it->second;
        }
        return nullptr;
    }

private:
    std::unordered_map<TypeId, TypeInfo> id_map_;
};

} // namespace

auto TypeRegistry::create() -> std::shared_ptr<TypeRegistry> {
    return std::make_shared<TypeRegistryImpl>();
}

} // namespace sr::engine
