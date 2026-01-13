#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include "type_c.h"
#include <cstddef>
#include <stdint.h>

typedef void *CFileManagerContext;
typedef void *CLoadFieldDataInfo;
typedef void *CFieldData;

typedef void *CQueryContext;
typedef struct {
  const char *data;
  int64_t length;
} CString;

typedef struct {
  uint8_t *data;
  int64_t length;
} CBytes;

// FB raw buffer
typedef struct {
  uint8_t *buff;
  int64_t buff_size;
  uint8_t *fb_data;
  int64_t fb_length;
  int64_t offset;
} CFBuffer;

typedef CString CQueryPlan;
typedef CBytes CQueryResult;
typedef CBytes CSnapshot;
typedef CString CQueryVec;
typedef CFBuffer CQueryFbResult;

typedef struct {
  char **files;
  size_t file_num;
  int64_t field_id;
  int64_t row_count;
} CUploadInfo;

enum CIndexStatus { Init = 0, Loading = 1, Ready = 2 };

enum CCtxIndexStatus { CtxIndexInit = 0, CtxIndexReady = 1 };

CStatus CreateFileManagerContext(CStorageConfig c_storage_config,
                                 int index_type, int64_t build_id,
                                 int64_t index_version, int64_t segment_id,
                                 int64_t field_id, int64_t collection_id,
                                 int64_t partition_id,
                                 CFileManagerContext *file_mgr_ctx);

void DeleteFileManagerContext(CFileManagerContext context);

CStatus UploadRawData(CFileManagerContext file_manager_context,
                      CFieldData field_data, const char *field_name,
                      enum CDataType field_type, CUploadInfo *upload_info,
                      int64_t max_length = 0);

CStatus BuildAndUploadIndex(CFileManagerContext file_manager_context,
                            CFieldData field_data, const char *field_name,
                            enum CDataType field_type, CUploadInfo *upload_info,
                            enum CDataType element_type = CDataType::None,
                            int64_t max_length = 0);

CStatus BuildAndUploadFullTypeIndex(
    CFileManagerContext file_manager_context, CFieldData field_data,
    const char *field_name, enum CDataType field_type, CUploadInfo *upload_info,
    enum CDataType element_type = CDataType::None, int64_t max_length = 0,
    // vector index build config, must has index_type
    const char *build_config = nullptr);

CStatus UploadInvertedIndex(CFileManagerContext file_manager_context,
                            const char *inverted_blob,
                            int64_t inverted_blob_len, const char *field_name,
                            enum CDataType field_type, CUploadInfo *upload_info,
                            enum CDataType element_type = CDataType::None);

void DeleteCUploadInfo(CUploadInfo *upload_info);

/**
 * @brief 反序列化 FieldData PB
 *
 * @param field_data_blob
 * @param length
 * @return CFieldData
 */
CFieldData DeserializeFieldDataPB(const char *field_data_blob, int64_t length);

void DeleteFieldData(CFieldData field_data);

// Create QueryContext loading specified index,
// for test and index validation.
CStatus CreateQueryContextVal(CStorageConfig c_storage_config, int64_t build_id,
                              const char *context_name,
                              const int64_t *partitions, const int64_t size,
                              const char *meta_file_name, bool async,
                              CQueryContext *query_ctx);

CStatus CreateQueryContext(const char *config_path, const char *context_name,
                           CQueryContext *query_ctx);

CStatus CreateQueryContextV2(const char *config_path, const char *context_name,
                             const char *collection_name,
                             CQueryContext *query_ctx);

CStatus InitContext(const char *config_json_str, const char *context_name);

CCtxIndexStatus GetCollCtxPrtStat(const char *context_name,
                                  const int64_t partition_id);

CIndexStatus GetCollPrtStat(const char *context_name,
                            const char *collection_name,
                            const int64_t partition_id);

// v1
CIndexStatus GetQueryCtxStat(CQueryContext query_ctx);

CStatus AssignPartitions(const char *context_name, const int64_t *partitions,
                         const int64_t size);

CStatus SqlQuery(const char *context_name, const char *collection_name,
                 const char *exprsql, void *jni_env, void *jni_param,
                 CQueryResult *result, const int64_t *partitions,
                 const int64_t partition_num);

CStatus SqlQueryWithJsonParams(const char *context_name,
                               const char *collection_name, const char *exprsql,
                               const char *json_param, CQueryResult *result,
                               const int64_t *partitions,
                               const int64_t partition_num);

CStatus QueryV2(CQueryContext query_ctx, CQueryPlan *plan, CQueryResult *result,
                const int64_t *partitions, const int64_t size);

// deprecated. exclusive API to v1. use QueryV2 or QueryByName instead
CStatus Query(CQueryContext query_ctx, CQueryPlan *plan, CQueryResult *result);

CStatus QueryByName(const char *context_name, const char *collection_name,
                    CQueryPlan *plan, CQueryResult *result,
                    const int64_t *partitions, const int64_t size);

CStatus QueryWithFbRes(const char *context_name, const char *collection_name,
                       CQueryPlan *plan, CQueryFbResult *result,
                       const int64_t *partitions, const int64_t size);

CStatus SearchByPb(const char *context_name, const char *collection_name,
                   CQueryPlan *plan, CQueryVec *query_vec, CQueryResult *result,
                   const int64_t *partitions, int64_t partition_num);

CStatus SearchByFloatArray(const char *context_name,
                           const char *collection_name, CQueryPlan *plan,
                           float *query_vec, int64_t query_vec_len,
                           CQueryResult *result, const int64_t *partitions,
                           int64_t partition_num);

CStatus SqlSearchByFloatArray(const char *context_name,
                              const char *collection_name, const char *exprsql,
                              void *jni_env, void *jni_param, float *query_vec,
                              int64_t query_vec_len, CQueryResult *result,
                              const int64_t *partitions, int64_t partition_num);

CStatus ApplyConfig(const char *context_name, const char *config_json_str);

// deprecated, use DeleteCollContext instead
void DeleteQueryContext(const char *context_name);

void DeleteCollContext(const char *context_name);

// deprecated, use DeleteQueryContext instead
void DeleteQueryContextVal(const char *context_name);

void DeleteQueryResult(CQueryResult *result);

void DeleteQueryFbResult(CQueryFbResult *result);

CStatus UpdateCollections(const char *context_name,
                          const char *config_json_str);

CStatus GetAssignedPrts(const char *context_name, const char *collection_name,
                        int64_t **partitions, int64_t *partition_num);

void DeletePartitionIds(int64_t *partitions);

CStatus GetShardNum(const char *context_name, const char *collection_name,
                    int64_t *shard_num);

CStatus GetSnapshot(CSnapshot *c_snapshot, int level);

void DeleteSnapshot(CSnapshot *coll_stat);

CStatus ReloadFlags(const char *gflags_json_str);

CStatus LoadIndex(const char *context_name, const char *collection_name,
                  const char *index_config_str);

CStatus DeleteCollectionIndex(const char *context_name,
                              const char *collection_name,
                              const char *del_config_str);

CStatus GetCollInfo(const char *context_name, const char **collection_names,
                    int coll_num, CSnapshot *c_snapshot);

CStatus UnloadCollections(const char *context_name,
                          const char **collection_names, int coll_num);

#ifdef __cplusplus
};
#endif
