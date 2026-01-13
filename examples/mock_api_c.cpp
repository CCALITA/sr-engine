#include <iostream>
#include <vector>
#include <cstddef>
#include <cstdint>
#include "../thirdparty/sr-proto/api_c.h"

extern "C" {

// Mocks for linking

CStatus CreateQueryContext(const char *config_path, const char *context_name,
                           CQueryContext *query_ctx) {
    std::cout << "[Mock] CreateQueryContext: config=" << config_path << ", ctx=" << context_name << "\n";
    *query_ctx = (void*)0x1001;
    return {0, nullptr};
}

CStatus CreateQueryContextV2(const char *config_path, const char *context_name,
                             const char *collection_name,
                             CQueryContext *query_ctx) {
    std::cout << "[Mock] CreateQueryContextV2: config=" << config_path << ", ctx=" << context_name << ", coll=" << collection_name << "\n";
    *query_ctx = (void*)0x1002;
    return {0, nullptr};
}

CStatus InitContext(const char *config_json_str, const char *context_name) {
    std::cout << "[Mock] InitContext: ctx=" << context_name << "\n";
    return {0, nullptr};
}

CCtxIndexStatus GetCollCtxPrtStat(const char *context_name,
                                  const int64_t partition_id) {
    return CtxIndexReady;
}

CIndexStatus GetCollPrtStat(const char *context_name,
                            const char *collection_name,
                            const int64_t partition_id) {
    return Ready;
}

CIndexStatus GetQueryCtxStat(CQueryContext query_ctx) {
    return Ready;
}

CStatus ApplyConfig(const char *context_name, const char *config_json_str) {
    std::cout << "[Mock] ApplyConfig: ctx=" << context_name << "\n";
    return {0, nullptr};
}

CStatus AssignPartitions(const char *context_name, const int64_t *partitions,
                         const int64_t size) {
    std::cout << "[Mock] AssignPartitions: ctx=" << context_name << ", count=" << size << "\n";
    return {0, nullptr};
}

CStatus GetAssignedPrts(const char *context_name, const char *collection_name,
                        int64_t **partitions, int64_t *partition_num) {
    std::cout << "[Mock] GetAssignedPrts: ctx=" << context_name << "\n";
    *partition_num = 2;
    *partitions = new int64_t[2];
    (*partitions)[0] = 100;
    (*partitions)[1] = 200;
    return {0, nullptr};
}

void DeletePartitionIds(int64_t *partitions) {
    delete[] partitions;
}

CStatus GetShardNum(const char *context_name, const char *collection_name,
                    int64_t *shard_num) {
    *shard_num = 4;
    return {0, nullptr};
}

CStatus GetSnapshot(CSnapshot *c_snapshot, int level) {
    static char dummy[] = "dummy_snapshot_data";
    c_snapshot->data = (uint8_t*)dummy;
    c_snapshot->length = sizeof(dummy);
    return {0, nullptr};
}

void DeleteSnapshot(CSnapshot *coll_stat) {
    // No-op for static dummy
}

CStatus UpdateCollections(const char *context_name,
                          const char *config_json_str) {
    std::cout << "[Mock] UpdateCollections: ctx=" << context_name << "\n";
    return {0, nullptr};
}

CStatus LoadIndex(const char *context_name, const char *collection_name,
                  const char *index_config_str) {
    std::cout << "[Mock] LoadIndex: ctx=" << context_name << "\n";
    return {0, nullptr};
}

CStatus DeleteCollectionIndex(const char *context_name,
                              const char *collection_name,
                              const char *del_config_str) {
    std::cout << "[Mock] DeleteCollectionIndex: ctx=" << context_name << "\n";
    return {0, nullptr};
}

CStatus GetCollInfo(const char *context_name, const char **collection_names,
                    int coll_num, CSnapshot *c_snapshot) {
    std::cout << "[Mock] GetCollInfo: ctx=" << context_name << ", count=" << coll_num << "\n";
    static char dummy[] = "dummy_coll_info";
    c_snapshot->data = (uint8_t*)dummy;
    c_snapshot->length = sizeof(dummy);
    return {0, nullptr};
}

CStatus ReloadFlags(const char *gflags_json_str) {
    std::cout << "[Mock] ReloadFlags\n";
    return {0, nullptr};
}

CStatus UnloadCollections(const char *context_name,
                          const char **collection_names, int coll_num) {
    std::cout << "[Mock] UnloadCollections: ctx=" << context_name << ", count=" << coll_num << "\n";
    return {0, nullptr};
}

} // extern "C"
