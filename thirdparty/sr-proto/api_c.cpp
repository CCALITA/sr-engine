#include "api_c.h"
#include "CollectionContext.h"
#include "Common.h"
#include "IndexMgr.h"
#include "IndexScanner.h"
#include "Status.h"
#include "config/ConfigKnowhere.h"
#include "index/ArrayInvertedIndex.h"
#include "index/IndexFactory.h"
#include "index/Utils.h"
#include "parser/parser_api.h"
#include "storage/Util.h"
#include <fstream>
#include <google/protobuf/util/json_util.h>

using namespace milvus;
using namespace sr_core;

void
GetStorageConfig(const CStorageConfig &c_storage_config, storage::StorageConfig &storage_config) {
    storage_config.address = std::string(c_storage_config.address);
    storage_config.bucket_name = std::string(c_storage_config.bucket_name);
    storage_config.access_key_id = std::string(c_storage_config.access_key_id);
    storage_config.access_key_value = std::string(c_storage_config.access_key_value);
    storage_config.root_path = std::string(c_storage_config.root_path);
    storage_config.storage_type = std::string(c_storage_config.storage_type);
    storage_config.cloud_provider = std::string(c_storage_config.cloud_provider);
    storage_config.iam_endpoint = std::string(c_storage_config.iam_endpoint);
    storage_config.log_level = std::string(c_storage_config.log_level);
    storage_config.useSSL = c_storage_config.useSSL;
    storage_config.useIAM = c_storage_config.useIAM;
    storage_config.useVirtualHost = c_storage_config.useVirtualHost;
    storage_config.region = c_storage_config.region;
    storage_config.requestTimeoutMs = c_storage_config.requestTimeoutMs;
}

void
GetStorageConfig(const CStorageConfig &c_storage_config, milvus::Config &storage_config) {
    storage_config["address"] = c_storage_config.address;
    storage_config["bucket_name"] = c_storage_config.bucket_name;
    storage_config["access_key_id"] = c_storage_config.access_key_id;
    storage_config["access_key_value"] = c_storage_config.access_key_value;
    storage_config["root_path"] = c_storage_config.root_path;
    storage_config["storage_type"] = c_storage_config.storage_type;
    storage_config["cloud_provider"] = c_storage_config.cloud_provider;
    storage_config["iam_endpoint"] = c_storage_config.iam_endpoint;
    storage_config["log_level"] = c_storage_config.log_level;
    storage_config["useSSL"] = c_storage_config.useSSL;
    storage_config["useIAM"] = c_storage_config.useIAM;
    storage_config["useVirtualHost"] = c_storage_config.useVirtualHost;
    storage_config["region"] = c_storage_config.region;
    storage_config["requestTimeoutMs"] = c_storage_config.requestTimeoutMs;
}

CStatus
CreateFileManagerContext(CStorageConfig c_storage_config,
                         int index_type,
                         int64_t build_id,
                         int64_t index_version,
                         int64_t segment_id,
                         int64_t field_id,
                         int64_t collection_id,
                         int64_t partition_id,
                         CFileManagerContext *file_mgr_ctx) {
    auto status = CStatus();
    try {
        LOG_SEGCORE_INFO_ << "Start to create file manager context(field id: " << field_id << ")";
        int min_limit = storage::IndexingTypeMin;
        int max_limit = storage::IndexingTypeMax;
        AssertInfo(index_type >= min_limit && index_type <= max_limit,
                   "index_type should be in [{}, {}], but got {}", min_limit, max_limit,
                   index_type);

        storage::StorageConfig storage_config;
        GetStorageConfig(c_storage_config, storage_config);
        // TODO(zhanghe): use a common instance
        auto chunk_manager = storage::CreateChunkManager(storage_config);

        storage::FieldDataMeta field_meta{collection_id, partition_id, segment_id, field_id};
        storage::IndexMeta index_meta{static_cast<storage::FieldIndexingType>(index_type),
                                      segment_id, field_id, build_id, index_version};

        auto file_manager_context =
            std::make_unique<storage::FileManagerContext>(field_meta, index_meta, chunk_manager);
        *file_mgr_ctx = reinterpret_cast<void *>(file_manager_context.release());
        LOG_SEGCORE_INFO_ << "Finish to create file manager context(field id: " << field_id << ")";

        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception &e) {
        LOG_SEGCORE_ERROR_ << "Failed to create file manager context(field id: " << field_id
                           << "): " << e.what();
        status.error_code = UnexpectedError;
        // status.error_msg = strdup(e.what());
    }
    return status;
}

void
DeleteFileManagerContext(CFileManagerContext context) {
    auto ctx = reinterpret_cast<storage::FileManagerContext *>(context);
    delete ctx;
}

CStatus
UploadRawData(CFileManagerContext file_manager_context,
              CFieldData field_data,
              const char *field_name,
              enum CDataType field_type,
              CUploadInfo *upload_info,
              int64_t max_length) {
    auto status = CStatus();
    try {
        auto file_mgr_ctx = reinterpret_cast<storage::FileManagerContext *>(file_manager_context);
        int64_t field_id = file_mgr_ctx->fieldDataMeta.field_id;
        FieldMeta field_meta{FieldName(field_name), FieldId(field_id), DataType(field_type)};

        LOG_SEGCORE_INFO_ << "Start uploading raw data. field: " << field_name;
        auto field_data_p = storage::CreateFieldDataWapperFromDataArray(
            reinterpret_cast<proto::schema::FieldData *>(field_data), field_meta);
        auto load_info = ExportFieldData(field_data_p, file_mgr_ctx->indexMeta,
                                         file_mgr_ctx->fieldDataMeta, *file_mgr_ctx);

        auto field_binlog_info = load_info.field_infos[field_id];
        upload_info->field_id = field_id;
        upload_info->file_num = field_binlog_info.insert_files.size();
        upload_info->files = new char *[upload_info->file_num];
        upload_info->row_count = field_binlog_info.row_count;
        int i = 0;
        for (auto &file : field_binlog_info.insert_files) {
            size_t len = file.length() + 1;
            upload_info->files[i] = new char[len];
            strncpy(upload_info->files[i], file.c_str(), len);
            i++;
        }
        LOG_SEGCORE_INFO_ << "Finish uploading raw data. field: " << field_name
                          << ", row count: " << upload_info->row_count;

        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception &e) {
        LOG_SEGCORE_ERROR_ << "Failed to upload raw data(field name: " << field_name
                           << "): " << e.what();
        status.error_code = UnexpectedError;
        // status.error_msg = strdup(e.what());
    }
    return status;
}

void
get_index_upload_info(const BinarySet &storage_info,
                      CUploadInfo *upload_info,
                      int64_t field_id,
                      int64_t row_count) {
    upload_info->file_num = storage_info.binary_map_.size();
    upload_info->files = new char *[upload_info->file_num];
    upload_info->field_id = field_id;
    upload_info->row_count = row_count;
    size_t i = 0;
    for (auto iter = storage_info.binary_map_.begin(); iter != storage_info.binary_map_.end();
         iter++, i++) {
        // std::cout << "storage key: " << iter->first << " val sz: ["<< iter->second->size<< "]" <<
        // std::endl;
        size_t len = iter->first.length() + 1;
        upload_info->files[i] = new char[len];
        strncpy(upload_info->files[i], iter->first.c_str(), len);
    }
}

CStatus
BuildAndUploadIndex(CFileManagerContext file_manager_context,
                    CFieldData field_data,
                    const char *field_name,
                    enum CDataType field_type,
                    CUploadInfo *upload_info,
                    enum CDataType element_type,
                    int64_t max_length) {
    LOG_WARN("Depracated. Use BuildAndUploadFullTypeIndex instead.");
    auto status = CStatus();
    try {
        auto data_type = DataType(field_type);
        index::CreateIndexInfo create_index_info;
        create_index_info.field_type = data_type;
        auto file_mgr_ctx = reinterpret_cast<storage::FileManagerContext *>(file_manager_context);
        int64_t field_id = file_mgr_ctx->indexMeta.field_id;
        std::string index_type;
        auto index_meta_type = file_mgr_ctx->indexMeta.index_type;
        switch (index_meta_type) {
        case storage::VecIndex: // vector
            index_type = INDEX_VECTOR;
            break;
        case storage::ScalarIndex: // sort
            index_type = INDEX_SORT;
            break;
        case storage::InvertedIndex: // inverted
            index_type = INDEX_INVERTED;
            break;
        case storage::BitmapIndex: // bitmap
            index_type = INDEX_BITMAP;
            break;
        case storage::TokenTrieIndex: // trie
            index_type = INDEX_TOKEN_TRIE;
            break;
        default:
            PanicInfo(Unsupported,
                      fmt::format("unsupported index type: {}", std::to_string(index_meta_type)));
        }

        // Used to differentiate between inverted and sort index.
        if (std::find(valid_index_type.begin(), valid_index_type.end(), index_type) ==
            valid_index_type.end()) {
            PanicInfo(DataTypeInvalid, fmt::format("Invalid index type: {}", index_type));
        }
        create_index_info.index_type = index_type;
        create_index_info.element_type = DataType(element_type);
        auto index =
            index::IndexFactory::GetInstance().CreateIndex(create_index_info, *file_mgr_ctx);

        LOG_SEGCORE_INFO_ << "Start building index. field: " << field_name;
        auto starttime = std::chrono::system_clock::now();
        switch (index_meta_type) {
        case storage::VecIndex: { // vector
            AssertInfo(datatype_is_vector(data_type),
                       "field_type is not vector, but index_type is VecIndex.");
            index->Build();
            break;
        }
        case storage::ScalarIndex: { // sort
            // 如果是array, 默认是inverted index
            AssertInfo(!datatype_is_array(data_type), "Array field only support inverted index.");

            FieldMeta field_meta{FieldName(field_name), FieldId(field_id), DataType(field_type)};
            auto field_data_p = storage::CreateFieldDataWapperFromDataArray(
                reinterpret_cast<proto::schema::FieldData *>(field_data), field_meta);
            if (datatype_is_string(data_type)) { // str-sort
                dynamic_cast<milvus::index::StringIndexMarisa *>(index.get())
                    ->Build(field_data_p->get_num_rows(),
                            reinterpret_cast<const std::string *>(field_data_p->Data()));
            } else { // scalar-sort
                index->BuildWithRawData(field_data_p->get_num_rows(), field_data_p->Data());
            }
            break;
        }
        case storage::InvertedIndex:  // inverted
        case storage::BitmapIndex:    // bitmap
        case storage::TokenTrieIndex: // trie
            index->Build(reinterpret_cast<proto::schema::FieldData *>(field_data));
            break;
        default:
            PanicInfo(Unsupported, "unsupported index type");
        }
        std::chrono::duration<double> time = std::chrono::system_clock::now() - starttime;
        LOG_SEGCORE_INFO_ << "Finish building index. field: " << field_name
                          << ", index type: " << index_type << ", time(sec): " << time.count();

        LOG_SEGCORE_INFO_ << "Start uploading index. field: " << field_name;
        auto storage_info = index->Upload();
        get_index_upload_info(storage_info, upload_info, field_id, index->Count());
        LOG_SEGCORE_INFO_ << "Finish uploading index. field: " << field_name
                          << ", row count: " << upload_info->row_count
                          << ", file num: " << upload_info->file_num;

        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception &e) {
        LOG_SEGCORE_ERROR_ << "Failed to build and upload index(field name: " << field_name
                           << "): " << e.what();
        status.error_code = UnexpectedError;
        // status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
BuildAndUploadFullTypeIndex(CFileManagerContext file_manager_context,
                            CFieldData field_data,
                            const char *field_name,
                            enum CDataType field_type,
                            CUploadInfo *upload_info,
                            enum CDataType element_type,
                            int64_t max_length,
                            const char *build_config) {
    auto status = CStatus();
    try {
        auto data_type = DataType(field_type);
        index::CreateIndexInfo create_index_info;
        create_index_info.field_type = data_type;
        auto file_mgr_ctx = reinterpret_cast<storage::FileManagerContext *>(file_manager_context);
        int64_t field_id = file_mgr_ctx->indexMeta.field_id;
        std::string index_type;
        milvus::Config bconfig = {};
        auto index_meta_type = file_mgr_ctx->indexMeta.index_type;
        switch (index_meta_type) {
        case storage::VecIndex: // vector
            AssertInfo(data_type == DataType::VECTOR_FLOAT,
                       "vector index only support float vector.");
            AssertInfo(build_config, "Build Vecindex, params build_config not be nullptr");
            bconfig = milvus::Config::parse(build_config);
            AssertInfo(bconfig.contains(knowhere::meta::INDEX_TYPE) &&
                           bconfig[knowhere::meta::INDEX_TYPE].is_string(),
                       "Build Vecindex, params build_config must has index_type");
            AssertInfo(bconfig.contains(knowhere::meta::METRIC_TYPE) &&
                           bconfig[knowhere::meta::METRIC_TYPE].is_string(),
                       "Build Vecindex, params build_config must has metric_type");
            index_type =
                static_cast<std::string>(bconfig[knowhere::meta::INDEX_TYPE].get<std::string>());
            create_index_info.metric_type =
                static_cast<std::string>(bconfig[knowhere::meta::METRIC_TYPE].get<std::string>());
            if (bconfig.contains(knowhere::meta::NUM_BUILD_THREAD)) {
                milvus::config::KnowhereInitBuildThreadPool(
                    static_cast<int>(bconfig[knowhere::meta::NUM_BUILD_THREAD].get<int>()));
            }
            break;
        case storage::ScalarIndex: // sort
            LOG_WARN("index type 'ScalarIndex' is deprecated, use 'StlSortIndex' or 'TrieIndex' "
                     "instead.");
            // 如果是array, 默认是inverted index
            AssertInfo(!datatype_is_array(data_type), "Array field only support inverted index.");
            index_type = INDEX_SORT;
            break;
        case storage::StlSortIndex: // stl sort
            AssertInfo(!datatype_is_array(data_type), "Array field only support inverted index.");
            index_type = INDEX_STL_SORT;
            break;
        case storage::TrieIndex: // marisa trie
            AssertInfo(datatype_is_string(data_type), "TrieIndex only support STRING or VARCHAR");
            index_type = INDEX_MARISA_TRIE;
            break;
        case storage::InvertedIndex: // inverted
            index_type = INDEX_INVERTED;
            break;
        case storage::BitmapIndex: // bitmap
            index_type = INDEX_BITMAP;
            break;
        case storage::TokenTrieIndex: // token trie
            index_type = INDEX_TOKEN_TRIE;
            break;
        case storage::ACAutomationIndex: // ac automation
            AssertInfo(
                datatype_is_string(data_type) ||
                    (datatype_is_array(data_type) && datatype_is_string(DataType(element_type))),
                "ACAutomationIndex only support STRING/VARCHAR/ARRAY(STRING/VARCHAR)");
            index_type = INDEX_AC_AUTOMATION;
            break;
        default:
            PanicInfo(Unsupported,
                      fmt::format("unsupported index type: {}", std::to_string(index_meta_type)));
        }

        // Used to differentiate between inverted and sort index.
        if (std::find(valid_index_type.begin(), valid_index_type.end(), index_type) ==
            valid_index_type.end()) {
            PanicInfo(DataTypeInvalid, fmt::format("Invalid index type: {}", index_type));
        }
        create_index_info.index_type = index_type;
        create_index_info.element_type = DataType(element_type);
        create_index_info.index_engine_version =
            knowhere::Version::GetCurrentVersion().VersionNumber();

        auto index =
            index::IndexFactory::GetInstance().CreateIndex(create_index_info, *file_mgr_ctx);

        LOG_SEGCORE_INFO_ << "Start building index. field: " << field_name;
        auto starttime = std::chrono::system_clock::now();
        switch (index_meta_type) {
        case storage::VecIndex: { // vector
            LOG_SEGCORE_INFO_ << "Start vec index";
            auto vec_data = reinterpret_cast<proto::schema::FieldData *>(field_data);
            auto raw_data = vec_data->vectors().float_vector().data().data();
            auto dim = vec_data->vectors().dim();
            auto sz = vec_data->vectors().float_vector().data_size();
            AssertInfo(sz % dim == 0,
                       fmt::format("file size {} is not divisible by dim {}", sz, dim));
            auto ds = knowhere::GenDataSet(sz / dim, dim, raw_data);
            index->BuildWithDataset(ds, bconfig);
            break;
        }
        case storage::ScalarIndex: { // sort, deprecated
            FieldMeta field_meta{FieldName(field_name), FieldId(field_id), DataType(field_type)};
            auto field_data_p = storage::CreateFieldDataWapperFromDataArray(
                reinterpret_cast<proto::schema::FieldData *>(field_data), field_meta);
            if (datatype_is_string(data_type)) { // str-sort
                dynamic_cast<milvus::index::StringIndexMarisa *>(index.get())
                    ->Build(field_data_p->get_num_rows(),
                            reinterpret_cast<const std::string *>(field_data_p->Data()));
            } else { // scalar-sort
                index->BuildWithRawData(field_data_p->get_num_rows(), field_data_p->Data());
            }
            break;
        }
        case storage::StlSortIndex: { // stl sort
            FieldMeta field_meta{FieldName(field_name), FieldId(field_id), DataType(field_type)};
            auto field_data_p = storage::CreateFieldDataWapperFromDataArray(
                reinterpret_cast<proto::schema::FieldData *>(field_data), field_meta);
            if (datatype_is_string(data_type)) { // str sort
                dynamic_cast<milvus::index::StringIndexSort *>(index.get())
                    ->Build(field_data_p->get_num_rows(),
                            reinterpret_cast<const std::string *>(field_data_p->Data()));
            } else { // numeric sort
                index->BuildWithRawData(field_data_p->get_num_rows(), field_data_p->Data());
            }
            break;
        }
        case storage::TrieIndex: { // str marisa trie
            FieldMeta field_meta{FieldName(field_name), FieldId(field_id), DataType(field_type)};
            auto field_data_p = storage::CreateFieldDataWapperFromDataArray(
                reinterpret_cast<proto::schema::FieldData *>(field_data), field_meta);
            dynamic_cast<milvus::index::StringIndexMarisa *>(index.get())
                ->Build(field_data_p->get_num_rows(),
                        reinterpret_cast<const std::string *>(field_data_p->Data()));
            break;
        }
        case storage::InvertedIndex:     // inverted
        case storage::BitmapIndex:       // bitmap
        case storage::TokenTrieIndex:    // token trie
        case storage::ACAutomationIndex: // ac
            index->Build(reinterpret_cast<proto::schema::FieldData *>(field_data));
            break;
        default:
            PanicInfo(Unsupported, "unsupported index type");
        }
        std::chrono::duration<double> time = std::chrono::system_clock::now() - starttime;
        LOG_SEGCORE_INFO_ << "Finish building index. field: " << field_name
                          << ", index type: " << index_type << ", time(sec): " << time.count();

        LOG_SEGCORE_INFO_ << "Start uploading index. field: " << field_name;
        auto storage_info = index->Upload();
        get_index_upload_info(storage_info, upload_info, field_id, index->Count());
        LOG_SEGCORE_INFO_ << "Finish uploading index. field: " << field_name
                          << ", row count: " << upload_info->row_count
                          << ", file num: " << upload_info->file_num;

        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception &e) {
        LOG_SEGCORE_ERROR_ << "Failed to build and upload index(field name: " << field_name
                           << "): " << e.what();
        status.error_code = UnexpectedError;
        // status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
UploadInvertedIndex(CFileManagerContext file_manager_context,
                    const char *inverted_blob,
                    int64_t inverted_blob_len,
                    const char *field_name,
                    enum CDataType field_type,
                    CUploadInfo *upload_info,
                    enum CDataType element_type) {
    auto status = CStatus();
    try {
        index::CreateIndexInfo create_index_info;
        create_index_info.field_type = DataType(field_type);
        create_index_info.index_type = INDEX_INVERTED;
        create_index_info.element_type = DataType(element_type);
        auto file_mgr_ctx = reinterpret_cast<storage::FileManagerContext *>(file_manager_context);
        auto index =
            index::IndexFactory::GetInstance().CreateIndex(create_index_info, *file_mgr_ctx);

        proto::schema::InvertedData inverted_data;
        inverted_data.ParseFromArray(inverted_blob, inverted_blob_len);

        LOG_SEGCORE_INFO_ << "Start building and uploading index. field: " << field_name;
        index->Build(&inverted_data);
        auto storage_info = index->Upload();
        get_index_upload_info(storage_info, upload_info, file_mgr_ctx->indexMeta.field_id,
                              index->Count());
        LOG_SEGCORE_INFO_ << "Finish building and uploading index. field: " << field_name
                          << ", row count: " << upload_info->row_count
                          << ", file num: " << upload_info->file_num;

        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception &e) {
        LOG_SEGCORE_ERROR_ << "Failed to upload inverted index(field name: " << field_name
                           << "): " << e.what();
        status.error_code = UnexpectedError;
        // status.error_msg = strdup(e.what());
    }
    return status;
}

void
DeleteCUploadInfo(CUploadInfo *upload_info) {
    if (upload_info == nullptr)
        return;

    for (size_t i = 0; i < upload_info->file_num; ++i) {
        delete[] upload_info->files[i];
    }
    delete[] upload_info->files;
}

CFieldData
DeserializeFieldDataPB(const char *field_data_blob, int64_t length) {
    auto field_data = std::make_unique<proto::schema::FieldData>();
    field_data->ParseFromArray(field_data_blob, length);
    return reinterpret_cast<void *>(field_data.release());
}

void
DeleteFieldData(CFieldData field_data) {
    auto fd = reinterpret_cast<proto::schema::FieldData *>(field_data);
    delete fd;
}

CStatus
GetQueryCollections(const char *context_name,
                    std::shared_ptr<sr_core::app::QueryCollections> &coll_indexes_out) {
    auto coll_ctx_mgr = app::IndexManagerSingleton::GetInstance().GetCollectionsCtxManager();
    if (!coll_ctx_mgr) {
        return milvus::FailureCStatus(UnexpectedError, "collection context manager is null");
    }
    auto coll_indexes = coll_ctx_mgr->GetCollectionIndexs(context_name);
    if (!coll_indexes) {
        LOG_ERROR("Collection ctx '{}' not found", context_name);
        return milvus::FailureCStatus(app::ErrCollectionNotFound, "Collection ctx not found");
    }
    coll_indexes_out = coll_indexes;
    return milvus::SuccessCStatus();
}

bool
Ok(CStatus status) {
    return status.error_code == ErrorCode::Success;
}

CStatus
GetCollectionIndex(const char *context_name,
                   const char *collection_name,
                   std::shared_ptr<sr_core::app::CollectionIndex> &indexptr_out) {
    std::shared_ptr<sr_core::app::QueryCollections> query_colls;
    auto status = GetQueryCollections(context_name, query_colls);
    if (!Ok(status)) {
        return status;
    }
    auto indexptr = query_colls->GetCollIndex(collection_name);
    if (!indexptr) {
        LOG_ERROR("Collection index '{}' not found", collection_name);
        return milvus::FailureCStatus(app::ErrIndexNotFound, "Collection index not found");
    }
    indexptr_out = indexptr;
    return milvus::SuccessCStatus();
}

CStatus
CreateQueryContextVal(CStorageConfig c_storage_config,
                      int64_t build_id,
                      const char *context_name,
                      const int64_t *partitions,
                      const int64_t size,
                      const char *meta_file_name,
                      bool async,
                      CQueryContext *query_ctx) {
    try {
        milvus::Config storage_config;
        GetStorageConfig(c_storage_config, storage_config);

        milvus::Config coll_config;
        coll_config[context_name] = {
            {STORAGE, storage_config},
            {TABLETYPE, NORMAL},
            {TABLENAME, context_name},
            {BASE_META_FILE, app::GetBaseMetaFilePath(c_storage_config.root_path, build_id)},
            {INDEXFILES_POLL_INTERVAL, 0}};
        if (meta_file_name) {
            coll_config[context_name][BASE_META_FILE] =
                app::GetBaseMetaFilePath(c_storage_config.root_path, build_id, meta_file_name);
        } else {
            coll_config[context_name][BASE_META_FILE] =
                app::GetBaseMetaFilePath(c_storage_config.root_path, build_id);
        }
        coll_config[context_name][PARTITIONS] = std::vector<int64_t>(partitions, partitions + size);
        milvus::Config load_config;
        load_config[LOAD_CONFIG] = coll_config;

        auto collection_name = context_name;
        app::IndexManagerSingleton::GetInstance().Init(c_storage_config.root_path);
        auto coll_ctx_mgr = app::IndexManagerSingleton::GetInstance().GetCollectionsCtxManager();
        coll_ctx_mgr->Init({});
        app::Status result = coll_ctx_mgr->LoadCollections(context_name, load_config);
        if (result.IsErr()) {
            LOG_ERROR("Query Context {} load failed: {} detail: {}", context_name, result.Msg(),
                      result.Detail());
            return milvus::FailureCStatus(result.Code(), result.Msg());
        }
        auto coll_indexs = coll_ctx_mgr->GetCollectionIndexs(context_name);
        if (!coll_indexs) {
            LOG_ERROR("Collection {} not found", context_name);
            return milvus::FailureCStatus(app::ErrCollectionNotFound, "Collection not found");
        }

        auto indexptr = coll_indexs->GetCollIndex(collection_name);
        if (!indexptr) {
            LOG_ERROR("Collection index {} not found", collection_name);
            return milvus::FailureCStatus(app::ErrIndexNotFound, "Collection index not found");
        }

        auto index_scanner = coll_indexs->GetIndexScanner(collection_name);
        if (!index_scanner) {
            // TODO(zhanghe): release context
            LOG_ERROR("index scanner of collection '{}' not found", collection_name);
            return milvus::FailureCStatus(app::ErrIndexNotFound, "index scanner not found");
        }

        if (!async) {
            while (true) {
                auto index_status = index_scanner->GetWarmupIndexState();
                switch (index_status) {
                case app::WarmupIndexState::Successed: {
                    *query_ctx = indexptr.get();
                    LOG_INFO("Finish creating qr ctx for validation");
                    return milvus::SuccessCStatus();
                }
                case app::WarmupIndexState::Failed:
                    return milvus::FailureCStatus(app::ErrCollectionLoaded, "failed to load index");
                case app::WarmupIndexState::Loading:
                    LOG_INFO("index loading");
                    break;
                case app::WarmupIndexState::Empty:
                    LOG_INFO("index empty");
                    break;
                default:
                    return milvus::FailureCStatus(app::ErrCollectionLoaded, "failed to load index");
                }
                std::this_thread::sleep_for(std::chrono::seconds(5));
            }
        } else {
            *query_ctx = indexptr.get();
            LOG_INFO("Finish creating qr ctx for validation");
        }
    } catch (std::exception &e) {
        LOG_SEGCORE_ERROR_ << "Failed to create query context(build_id: " << build_id
                           << "): " << e.what();
        return milvus::FailureCStatus(app::ErrCollectionLoaded, "failed to load index");
        // status.error_msg = strdup(e.what());
    }
    return milvus::SuccessCStatus();
}

CStatus
InitContext(const char *config_json_str, const char *context_name_ptr) {
    static bool already_init = false;
    try {
        if (!already_init) {
            config::KnowhereInitImpl(nullptr);
            gflags::SetCommandLineOption("alsologtostderr", "true");
        }

        std::string context_name(context_name_ptr);
        milvus::Config global_config = milvus::Config::parse(config_json_str);
        if (!global_config.contains(context_name)) {
            LOG_ERROR("Config file has no query context name '{}'", context_name);
            return CStatus{ConfigInvalid, "Config file has no index name."};
        }

        if (!already_init) {
            std::string flagfile;
            if (global_config.contains(FLAGFILE)) {
                flagfile = global_config[FLAGFILE];
            } else if (global_config[context_name].contains(FLAGFILE)) {
                flagfile = global_config[context_name][FLAGFILE];
            }
            if (!flagfile.empty()) {
                gflags::SetCommandLineOption("flagfile", flagfile.c_str());
            }
            milvus::config::KnowhereInitSearchThreadPool(getSearchMaxThreads());
            milvus::config::KnowhereInitBuildThreadPool(1);
            already_init = true;
        }

        auto coll_ctx_config = global_config[context_name];
        LOG_INFO("Loading coll ctx. name: '{}', configs: {}", context_name, coll_ctx_config.dump());

        if (!coll_ctx_config.contains(LOAD_CONFIG)) {
            return CStatus{ConfigInvalid, "config error, load config not set."};
        }

        app::IndexManagerSingleton::GetInstance().Init("");
        auto coll_ctx_mgr = app::IndexManagerSingleton::GetInstance().GetCollectionsCtxManager();
        coll_ctx_mgr->Init({});
        app::Status result = coll_ctx_mgr->LoadCollections(context_name, coll_ctx_config);
        if (result.IsErr()) {
            LOG_ERROR("Query Context {} load failed: {} detail: {}", context_name, result.Msg(),
                      result.Detail());
            return milvus::FailureCStatus(result.Code(), result.Msg());
        }
    } catch (std::exception &e) {
        LOG_SEGCORE_ERROR_ << "Failed to load query context. " << e.what();
        return milvus::FailureCStatus(&e);
    }

    return milvus::SuccessCStatus();
}

CStatus
CreateQueryContextV2(const char *config_path,
                     const char *context_name,
                     const char *collection_name,
                     CQueryContext *query_ctx) {
    try {
        config::KnowhereInitImpl(nullptr);
        gflags::SetCommandLineOption("alsologtostderr", "true");
        std::ifstream config_file(config_path);
        if (!config_file.is_open()) {
            LOG_SEGCORE_ERROR_ << "Failed to open " << config_path;
            return CStatus{FileOpenFailed, "Failed to open config file."};
        }
        std::stringstream buffer;
        buffer << config_file.rdbuf();

        auto status = InitContext(buffer.str().c_str(), context_name);
        if (!Ok(status)) {
            return status;
        }

        std::shared_ptr<sr_core::app::CollectionIndex> indexptr;
        status = GetCollectionIndex(context_name, collection_name, indexptr);
        if (!Ok(status)) {
            return status;
        }
        *query_ctx = indexptr.get();
    } catch (std::exception &e) {
        LOG_SEGCORE_ERROR_ << "Failed to create query context. " << e.what();
        return milvus::FailureCStatus(&e);
    }
    return milvus::SuccessCStatus();
}

CStatus
CreateQueryContext(const char *config_path, const char *context_name, CQueryContext *query_ctx) {
    return CreateQueryContextV2(config_path, context_name, context_name, query_ctx);
}

CCtxIndexStatus
GetCollCtxPrtStat(const char *context_name, const int64_t partition_id) {
    app::ContextIndexState index_state;
    try {
        auto coll_ctx_mgr = app::IndexManagerSingleton::GetInstance().GetCollectionsCtxManager();
        if (!coll_ctx_mgr) {
            LOG_ERROR("coll_ctx_mgr is null");
            return CCtxIndexStatus::CtxIndexInit;
        }
        auto coll_indexs = coll_ctx_mgr->GetCollectionIndexs(context_name);
        if (!coll_indexs) {
            LOG_ERROR("Collection '{}' not found", context_name);
            return CCtxIndexStatus::CtxIndexInit;
        }
        auto state = coll_indexs->GetCollIndexState(partition_id, &index_state);
        if (state.IsErr()) {
            LOG_ERROR("get state of '{}' failed: {}", context_name, state.Msg());
            return CCtxIndexStatus::CtxIndexInit;
        }
    } catch (std::exception &e) {
        LOG_ERROR("failed to get ctx stat: {}", e.what());
        return CCtxIndexStatus::CtxIndexInit;
    }
    return static_cast<CCtxIndexStatus>(index_state);
}

CIndexStatus
GetCollPrtStat(const char *context_name, const char *collection_name, const int64_t partition_id) {
    auto coll_ctx_mgr = app::IndexManagerSingleton::GetInstance().GetCollectionsCtxManager();
    if (!coll_ctx_mgr) {
        LOG_ERROR("coll_ctx_mgr is null");
        return CIndexStatus::Init;
    }
    auto coll_indexs = coll_ctx_mgr->GetCollectionIndexs(context_name);
    if (!coll_indexs) {
        LOG_ERROR("Collection '{}' not found", context_name);
        return CIndexStatus::Init;
    }
    app::IndexState index_state;
    auto state = coll_indexs->GetCollIndexState(collection_name, partition_id, &index_state);
    if (state.IsErr()) {
        LOG_ERROR("[{}][{}][{}]get state failed: {}", context_name, collection_name, partition_id,
                  state.Msg());
        return CIndexStatus::Init;
    }
    return static_cast<CIndexStatus>(index_state);
}

CIndexStatus
GetQueryCtxStat(CQueryContext query_ctx) {
    try {
        auto indexptr = reinterpret_cast<app::CollectionIndex *>(query_ctx);
        if (!indexptr) {
            LOG_ERROR("invalid indexptr");
            return CIndexStatus::Init;
        }
        auto prt_indexes = indexptr->GetAllIndexs();
        if (prt_indexes.empty()) {
            LOG_WARN("Context[{}] has no prt loaded yet", indexptr->GetContextName());
            return CIndexStatus::Init;
        }
        for (auto &index : prt_indexes) {
            auto state = index->GetIndexState();
            if (state != sr_core::app::IndexState::Ready) {
                LOG_WARN("Context[{}], Coll[{}], Prt[{}] not ready, current state {}",
                         index->GetContextName(), index->GetCollectionName(),
                         index->GetPartitionId(), static_cast<int64_t>(state));
                return CIndexStatus::Init;
            }
        }
    } catch (std::exception &e) {
        LOG_ERROR("failed to get coll stat: {}", e.what());
        return CIndexStatus::Init;
    }
    return CIndexStatus::Ready;
}

CStatus
AssignPartitions(const char *context_name, const int64_t *partitions, const int64_t size) {
    try {
        auto coll_ctx_mgr = app::IndexManagerSingleton::GetInstance().GetCollectionsCtxManager();
        if (!coll_ctx_mgr) {
            return milvus::FailureCStatus(UnexpectedError, "collection context manager is null");
        }
        auto coll_indexs = coll_ctx_mgr->GetCollectionIndexs(context_name);
        if (!coll_indexs) {
            LOG_ERROR("Collection '{}' not found", context_name);
            return milvus::FailureCStatus(app::ErrCollectionNotFound, "Collection not found");
        }

        std::unordered_set<int64_t> partitions_set(partitions, partitions + size);
        auto status = coll_indexs->AssignPartitions(partitions_set);
        if (status.IsErr()) {
            LOG_SEGCORE_ERROR_ << status.Detail();
            return milvus::FailureCStatus(status.Code(), status.Msg());
        }
    } catch (std::exception &e) {
        LOG_ERROR("failed to assign prts: {}", e.what());
        return milvus::FailureCStatus(UnexpectedError, "failed to assign prts");
    }
    return milvus::SuccessCStatus();
}

CStatus
Query(CQueryContext query_ctx, CQueryPlan *plan, CQueryResult *result) {
    // LOG_WARN("Depracated. Use QueryByName instead.");
    auto status = CStatus();
    auto retrieve_data_ptr = std::make_unique<milvus::proto::segcore::RetrieveData>();
    auto retrieve_status = retrieve_data_ptr->mutable_status();
    auto set_status = [&](int code, const char *msg) {
        retrieve_status->set_code(code);
        retrieve_status->set_reason(msg);
        status.error_code = code;
        status.error_msg = msg;
    };
    try {
        auto indexptr = reinterpret_cast<app::CollectionIndex *>(query_ctx);
        if (!indexptr) {
            set_status(UnexpectedError, "invalid indexptr");
        } else {
            auto retrieve_results_ptr = retrieve_data_ptr->mutable_results();
            auto retrieve_status =
                indexptr->Retrieve(std::string(plan->data, plan->length), retrieve_results_ptr);
            if (retrieve_status.IsErr()) {
                LOG_SEGCORE_ERROR_ << retrieve_status.Detail();
                set_status(retrieve_status.Code(), "retrieve error");
            }

            size_t size = retrieve_data_ptr->ByteSizeLong();
            std::unique_ptr<uint8_t[]> data(new uint8_t[size]);
            if (!retrieve_data_ptr->SerializeToArray(data.get(), size)) {
                set_status(ProtoConversionError, "Failed to serialize retrieve result.");
            }
            result->data = data.release();
            result->length = size;

            set_status(Success, "");
        }
    } catch (std::exception &e) {
        LOG_SEGCORE_ERROR_ << "Failed to query. " << e.what();
        set_status(UnexpectedError, "query execption occured");
    }
    return status;
}

CStatus
QueryV2(CQueryContext query_ctx,
        CQueryPlan *plan,
        CQueryResult *result,
        const int64_t *partitions,
        int64_t partition_num) {
    // LOG_WARN("Depracated. Use QueryByName instead.");
    auto status = CStatus();
    try {
        auto indexptr = reinterpret_cast<app::CollectionIndex *>(query_ctx);
        if (!indexptr) {
            return CStatus{UnexpectedError, "Query context is null."};
        } else {
            // LOG_SEGCORE_INFO_ << "Query start";
            // auto start = std::chrono::steady_clock::now();
            auto retrieve_results = std::make_unique<milvus::proto::segcore::RetrieveResults>();
            auto retrieve_status = sr_core::app::Status::Ok();
            if (partition_num == 0) {
                retrieve_status = indexptr->Retrieve(std::string(plan->data, plan->length),
                                                     retrieve_results.get());
            } else {
                std::unordered_set<int64_t> partitions_set(partitions, partitions + partition_num);
                retrieve_status = indexptr->Retrieve(std::string(plan->data, plan->length),
                                                     partitions_set, retrieve_results.get());
            }
            if (retrieve_status.IsErr()) {
                LOG_SEGCORE_ERROR_ << retrieve_status.Detail();
                return milvus::FailureCStatus(retrieve_status.Code(), retrieve_status.Msg());
            }
            // LOG_SEGCORE_INFO_ << "Query end, cost "
            //     <<
            //     std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now()
            //     - start).count()
            //     << "ms";

            size_t size = retrieve_results->ByteSizeLong();
            std::unique_ptr<uint8_t[]> data(new uint8_t[size]);
            if (!retrieve_results->SerializeToArray(data.get(), size)) {
                return CStatus{ProtoConversionError, "Failed to serialize retrieve result."};
            }
            result->data = data.release();
            result->length = size;

            status.error_code = Success;
            status.error_msg = "";
        }
    } catch (std::exception &e) {
        LOG_SEGCORE_ERROR_ << "Failed to query. " << e.what();
        status.error_code = UnexpectedError;
        // status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
QueryByName(const char *context_name,
            const char *collection_name,
            CQueryPlan *plan,
            CQueryResult *result,
            const int64_t *partitions,
            int64_t partition_num) {
    auto status = CStatus();
    try {
        std::shared_ptr<sr_core::app::CollectionIndex> indexptr;
        status = GetCollectionIndex(context_name, collection_name, indexptr);
        if (!Ok(status)) {
            return status;
        }
        status = QueryV2(reinterpret_cast<CQueryContext>(indexptr.get()), plan, result, partitions,
                         partition_num);
        if (!Ok(status)) {
            return status;
        }
    } catch (std::exception &e) {
        LOG_SEGCORE_ERROR_ << "Failed to query. " << e.what();
        status.error_code = UnexpectedError;
        // status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
QueryWithFbRes(const char *context_name,
               const char *collection_name,
               CQueryPlan *plan,
               CQueryFbResult *result,
               const int64_t *partitions,
               int64_t partition_num) {
    auto status = CStatus();
    try {
        std::shared_ptr<sr_core::app::CollectionIndex> indexptr;
        status = GetCollectionIndex(context_name, collection_name, indexptr);
        if (!Ok(status)) {
            return status;
        }

        auto retrieve_results = std::make_unique<milvus::proto::segcore::RetrieveResults>();
        auto retrieve_status = sr_core::app::Status::Ok();
        if (partition_num == 0) {
            retrieve_status =
                indexptr->Retrieve(std::string(plan->data, plan->length), retrieve_results.get());
        } else {
            std::unordered_set<int64_t> partitions_set(partitions, partitions + partition_num);
            retrieve_status = indexptr->Retrieve(std::string(plan->data, plan->length),
                                                 partitions_set, retrieve_results.get());
        }
        if (retrieve_status.IsErr()) {
            LOG_SEGCORE_ERROR_ << retrieve_status.Detail();
            return milvus::FailureCStatus(retrieve_status.Code(), retrieve_status.Msg());
        }

        PlanResultsFbWrapper fb_result_wrapper;
        fb_result_wrapper.CreateRetrieveResultsFlatBuffer(*retrieve_results);
        size_t size = 0, offset = 0;
        result->buff = fb_result_wrapper.ReleaseRawBuffer(size, offset);
        result->buff_size = size;
        result->fb_data = result->buff + offset;
        result->fb_length = size - offset;
        result->offset = offset;
        if (result->fb_data == NULL || result->fb_length <= 0) {
            LOG_SEGCORE_ERROR_ << "QueryWithFbRes failed, fb sz " << result->fb_length;
            status.error_code = FbQueryResInvalid;
        }
    } catch (std::exception &e) {
        LOG_SEGCORE_ERROR_ << "Failed to query. " << e.what();
        status.error_code = UnexpectedError;
        // status.error_msg = strdup(e.what());
    }
    return status;
}

// using
CStatus
SqlQuery(const char *context_name,
         const char *collection_name,
         const char *exprsql,
         void *jni_env,
         void *jni_param,
         CQueryResult *result,
         const int64_t *partitions,
         const int64_t partition_num) {
    auto status = CStatus();
    try {
        std::shared_ptr<sr_core::app::CollectionIndex> indexptr;
        status = GetCollectionIndex(context_name, collection_name, indexptr);
        if (!Ok(status)) {
            return status;
        }
        AssertInfo(indexptr, "Query context is null.");

        auto paln_node = milvus::Parser(exprsql, jni_env, jni_param);

        auto retrieve_results = std::make_unique<milvus::proto::segcore::RetrieveResults>();
        auto retrieve_status = sr_core::app::Status::Ok();
        std::unordered_set<int64_t> partitions_set;
        for (int64_t i = 0; i < partition_num; ++i) {
            partitions_set.insert(partitions[i]);
        }
        retrieve_status = indexptr->Retrieve(*paln_node, partitions_set, retrieve_results.get());

        if (retrieve_status.IsErr()) {
            LOG_SEGCORE_ERROR_ << retrieve_status.Detail();
            return milvus::FailureCStatus(retrieve_status.Code(), retrieve_status.Msg());
        }

        size_t size = retrieve_results->ByteSizeLong();
        std::unique_ptr<uint8_t[]> data(new uint8_t[size]);
        if (!retrieve_results->SerializeToArray(data.get(), size)) {
            return CStatus{ProtoConversionError, "Failed to serialize retrieve result."};
        }
        result->data = data.release();
        result->length = size;

        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception &e) {
        LOG_SEGCORE_ERROR_ << "Failed to query. " << e.what();
        status.error_code = UnexpectedError;
    }
    return status;
}

CStatus
SqlQueryWithJsonParams(const char *context_name,
                       const char *collection_name,
                       const char *exprsql,
                       const char *json_param,
                       CQueryResult *result,
                       const int64_t *partitions,
                       const int64_t partition_num) {
    auto status = CStatus();
    try {
        std::shared_ptr<sr_core::app::CollectionIndex> indexptr;
        status = GetCollectionIndex(context_name, collection_name, indexptr);
        if (!Ok(status)) {
            return status;
        }
        AssertInfo(indexptr, "Query context is null.");

        auto paln_node = milvus::Parser(exprsql, json_param);

        auto retrieve_results = std::make_unique<milvus::proto::segcore::RetrieveResults>();
        auto retrieve_status = sr_core::app::Status::Ok();
        std::unordered_set<int64_t> partitions_set;
        for (int64_t i = 0; i < partition_num; ++i) {
            partitions_set.insert(partitions[i]);
        }
        retrieve_status = indexptr->Retrieve(*paln_node, partitions_set, retrieve_results.get());

        if (retrieve_status.IsErr()) {
            LOG_SEGCORE_ERROR_ << retrieve_status.Detail();
            return milvus::FailureCStatus(retrieve_status.Code(), retrieve_status.Msg());
        }

        size_t size = retrieve_results->ByteSizeLong();
        std::unique_ptr<uint8_t[]> data(new uint8_t[size]);
        if (!retrieve_results->SerializeToArray(data.get(), size)) {
            return CStatus{ProtoConversionError, "Failed to serialize retrieve result."};
        }
        result->data = data.release();
        result->length = size;

        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception &e) {
        LOG_SEGCORE_ERROR_ << "Failed to query. " << e.what();
        status.error_code = UnexpectedError;
    }
    return status;
}

CStatus
SearchByFloatArray(const char *context_name,
                   const char *collection_name,
                   CQueryPlan *plan,
                   float *query_arr,
                   int64_t query_arr_len,
                   CQueryResult *result,
                   const int64_t *partitions,
                   int64_t partition_num) {
    auto status = CStatus();
    try {
        std::shared_ptr<sr_core::app::CollectionIndex> indexptr;
        status = GetCollectionIndex(context_name, collection_name, indexptr);
        if (!Ok(status)) {
            return status;
        }
        if (!indexptr) {
            return CStatus{UnexpectedError, "Query context is null."};
        } else {
            auto search_results = std::make_unique<milvus::proto::schema::SearchResultData>();
            auto app_status = sr_core::app::Status::Ok();
            app::QueryVectors query_vec{reinterpret_cast<char *>(query_arr), query_arr_len,
                                        app::QueryVectors::Mode::FloatArray};
            if (partition_num == 0) {
                app_status = indexptr->Search(std::string(plan->data, plan->length), query_vec,
                                              search_results.get());
            } else {
                std::unordered_set<int64_t> partitions_set(partitions, partitions + partition_num);
                app_status = indexptr->Search(std::string(plan->data, plan->length), query_vec,
                                              partitions_set, search_results.get());
            }
            if (app_status.IsErr()) {
                LOG_SEGCORE_ERROR_ << app_status.Detail();
                return milvus::FailureCStatus(RetrieveError, app_status.Msg());
            }
            size_t size = search_results->ByteSizeLong();
            std::unique_ptr<uint8_t[]> data(new uint8_t[size]);
            if (!search_results->SerializeToArray(data.get(), size)) {
                return CStatus{ProtoConversionError, "Failed to serialize search result."};
            }
            result->data = data.release();
            result->length = size;
            status.error_code = Success;
            status.error_msg = "";
        }
    } catch (std::exception &e) {
        LOG_SEGCORE_ERROR_ << "Failed to search. " << e.what();
        status.error_code = UnexpectedError;
        // status.error_msg = strdup(e.what());
    }
    return status;
}

// using
CStatus
SqlSearchByFloatArray(const char *context_name,
                      const char *collection_name,
                      const char *exprsql,
                      void *jni_env,
                      void *jni_param,
                      float *query_arr,
                      int64_t query_arr_len,
                      CQueryResult *result,
                      const int64_t *partitions,
                      int64_t partition_num) {
    auto status = CStatus();
    try {
        std::shared_ptr<sr_core::app::CollectionIndex> indexptr;
        status = GetCollectionIndex(context_name, collection_name, indexptr);
        if (!Ok(status)) {
            return status;
        }
        if (!indexptr) {
            return CStatus{UnexpectedError, "Query context is null."};
        } else {
            auto search_results = std::make_unique<milvus::proto::schema::SearchResultData>();
            auto app_status = sr_core::app::Status::Ok();
            app::QueryVectors query_vec{reinterpret_cast<char *>(query_arr), query_arr_len,
                                        app::QueryVectors::Mode::FloatArray};
            auto paln_node = milvus::Parser(exprsql, jni_env, jni_param);
            std::unordered_set<int64_t> partitions_set;

            if (partition_num == 0) {
                app_status =
                    indexptr->Search(*paln_node, query_vec, partitions_set, search_results.get());
            } else {
                partitions_set =
                    std::unordered_set<int64_t>(partitions, partitions + partition_num);
                app_status =
                    indexptr->Search(*paln_node, query_vec, partitions_set, search_results.get());
            }
            if (app_status.IsErr()) {
                LOG_SEGCORE_ERROR_ << app_status.Detail();
                return milvus::FailureCStatus(RetrieveError, app_status.Msg());
            }
            size_t size = search_results->ByteSizeLong();
            std::unique_ptr<uint8_t[]> data(new uint8_t[size]);
            if (!search_results->SerializeToArray(data.get(), size)) {
                return CStatus{ProtoConversionError, "Failed to serialize search result."};
            }
            result->data = data.release();
            result->length = size;
            status.error_code = Success;
            status.error_msg = "";
        }
    } catch (std::exception &e) {
        LOG_SEGCORE_ERROR_ << "Failed to search. " << e.what();
        status.error_code = UnexpectedError;
        // status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
SearchByPb(const char *context_name,
           const char *collection_name,
           CQueryPlan *plan,
           CQueryVec *cquery_vec,
           CQueryResult *result,
           const int64_t *partitions,
           int64_t partition_num) {
    auto status = CStatus();
    try {
        std::shared_ptr<sr_core::app::CollectionIndex> indexptr;
        status = GetCollectionIndex(context_name, collection_name, indexptr);
        if (!Ok(status)) {
            return status;
        }
        if (!indexptr) {
            return CStatus{UnexpectedError, "Query context is null."};
        } else {
            auto search_results = std::make_unique<milvus::proto::schema::SearchResultData>();
            auto app_status = sr_core::app::Status::Ok();
            app::QueryVectors query_vec{const_cast<char *>(cquery_vec->data), cquery_vec->length,
                                        app::QueryVectors::Mode::Pb};
            if (partition_num == 0) {
                app_status = indexptr->Search(std::string(plan->data, plan->length), query_vec,
                                              search_results.get());
            } else {
                std::unordered_set<int64_t> partitions_set(partitions, partitions + partition_num);
                app_status = indexptr->Search(std::string(plan->data, plan->length), query_vec,
                                              partitions_set, search_results.get());
            }
            if (app_status.IsErr()) {
                LOG_SEGCORE_ERROR_ << app_status.Detail();
                return milvus::FailureCStatus(RetrieveError, app_status.Msg());
            }

            size_t size = search_results->ByteSizeLong();
            std::unique_ptr<uint8_t[]> data(new uint8_t[size]);
            if (!search_results->SerializeToArray(data.get(), size)) {
                return CStatus{ProtoConversionError, "Failed to serialize search result."};
            }
            result->data = data.release();
            result->length = size;

            status.error_code = Success;
            status.error_msg = "";
        }
    } catch (std::exception &e) {
        LOG_SEGCORE_ERROR_ << "Failed to search. " << e.what();
        status.error_code = UnexpectedError;
        // status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
ApplyConfig(const char *context_name, const char *config_json_str) {
    LOG_INFO("apply config on query context '{}', config: {}", context_name, config_json_str);
    try {
        auto coll_ctx_mgr = app::IndexManagerSingleton::GetInstance().GetCollectionsCtxManager();
        if (!coll_ctx_mgr) {
            return milvus::FailureCStatus(UnexpectedError, "collection context manager is null");
        }
        auto coll_indexs = coll_ctx_mgr->GetCollectionIndexs(context_name);
        if (!coll_indexs) {
            LOG_ERROR("Collection ctx '{}' not found", context_name);
            return milvus::FailureCStatus(app::ErrCollectionNotFound, "Collection ctx not found");
        }

        milvus::Config config = milvus::Config::parse(config_json_str);

        auto status = coll_indexs->ApplyConfig(config);
        if (status.IsErr()) {
            LOG_SEGCORE_ERROR_ << status.Detail();
            return milvus::FailureCStatus(status.Code(), status.Msg());
        }

        LOG_SEGCORE_INFO_ << "apply config finish.";
        return milvus::SuccessCStatus();
    } catch (std::exception &e) {
        LOG_SEGCORE_ERROR_ << "apply config failed. " << e.what();
        return CStatus{UnexpectedError, "apply config failed."};
    }
}

// deprecated, use DeleteCollContext instead
void
DeleteQueryContext(const char *context_name) {
    DeleteCollContext(context_name);
}

void
DeleteCollContext(const char *context_name) {
    auto coll_ctx_mgr = app::IndexManagerSingleton::GetInstance().GetCollectionsCtxManager();
    if (!coll_ctx_mgr) {
        LOG_ERROR("collection context manager not init");
        return;
    }
    auto status = coll_ctx_mgr->ReleaseCollContext(context_name);
    if (status.IsErr()) {
        LOG_ERROR("Failed to release coll context {}, err: {}", std::string(context_name),
                  status.Detail());
    }
}

// deprecated, use DeleteCollContext instead
void
DeleteQueryContextVal(const char *context_name) {
    DeleteCollContext(context_name);
}

void
DeleteQueryResult(CQueryResult *result) {
    delete[] result->data;
    result->data = nullptr;
    result->length = 0;
}

void
DeleteQueryFbResult(CQueryFbResult *result) {
    delete[] result->buff;
}

CStatus
UpdateCollections(const char *context_name, const char *config_json_str) {
    LOG_INFO("update collections of query context '{}', config: {}", context_name, config_json_str);
    try {
        std::shared_ptr<sr_core::app::QueryCollections> coll_indexs;
        auto c_status = GetQueryCollections(context_name, coll_indexs);
        if (!Ok(c_status)) {
            return c_status;
        }

        milvus::Config update_config = milvus::Config::parse(config_json_str);
        auto status = coll_indexs->UpdateCollections(update_config);
        if (status.IsErr()) {
            LOG_ERROR("update collections failed. msg: {}", status.Detail());
            return milvus::FailureCStatus(status.Code(), status.Msg());
        }

        LOG_SEGCORE_INFO_ << "update collections finish.";
        return milvus::SuccessCStatus();
    } catch (std::exception &e) {
        LOG_SEGCORE_ERROR_ << "update collections failed. msg: " << e.what();
        return CStatus{UnexpectedError, "update collections failed."};
    }
}

CStatus
GetAssignedPrts(const char *context_name,
                const char *collection_name,
                int64_t **partitions,
                int64_t *partition_num) {
    try {
        auto coll_ctx_mgr = app::IndexManagerSingleton::GetInstance().GetCollectionsCtxManager();
        if (!coll_ctx_mgr) {
            return milvus::FailureCStatus(UnexpectedError, "collection context manager is null");
        }
        auto coll_indexes = coll_ctx_mgr->GetCollectionIndexs(context_name);
        if (!coll_indexes) {
            LOG_ERROR("Context '{}' not found", context_name);
            return milvus::FailureCStatus(app::ErrCollectionNotFound, "Context not found");
        }

        std::string coll_name_str(collection_name);
        if (coll_name_str.empty()) {
            auto coll_names = coll_indexes->GetCollNames();
            if (coll_names.empty()) {
                LOG_ERROR("Context '{}' has no collections", context_name);
                return milvus::FailureCStatus(app::ErrIndexNotFound, "Empty collections");
            }
            coll_name_str = coll_names.at(0);
            LOG_INFO("get prts of coll '{}'", coll_name_str);
        }

        auto indexptr = coll_indexes->GetCollIndex(coll_name_str);
        if (!indexptr) {
            LOG_ERROR("Collection index '{}' not found", coll_name_str);
            return milvus::FailureCStatus(app::ErrIndexNotFound, "Collection index not found");
        }

        auto prt_ids = indexptr->GetAssignedPartitions();
        *partition_num = prt_ids.size();
        *partitions = new int64_t[*partition_num]();
        std::copy(prt_ids.begin(), prt_ids.end(), *partitions);
    } catch (std::exception &e) {
        LOG_ERROR("failed to get prts: {}", e.what());
        return milvus::FailureCStatus(UnexpectedError, "failed to get prts");
    }
    return milvus::SuccessCStatus();
}

void
DeletePartitionIds(int64_t *partitions) {
    delete[] partitions;
}

CStatus
GetShardNum(const char *context_name, const char *collection_name, int64_t *shard_num) {
    try {
        auto coll_ctx_mgr = app::IndexManagerSingleton::GetInstance().GetCollectionsCtxManager();
        if (!coll_ctx_mgr) {
            return milvus::FailureCStatus(UnexpectedError, "collection context manager is null");
        }
        auto coll_indexes = coll_ctx_mgr->GetCollectionIndexs(context_name);
        if (!coll_indexes) {
            LOG_ERROR("Context '{}' not found", context_name);
            return milvus::FailureCStatus(app::ErrCollectionNotFound, "Context not found");
        }

        std::string coll_name_str(collection_name);
        if (coll_name_str.empty()) {
            auto coll_names = coll_indexes->GetCollNames();
            if (coll_names.empty()) {
                LOG_ERROR("Context '{}' has no collections", context_name);
                return milvus::FailureCStatus(app::ErrIndexNotFound, "Empty collections");
            }
            coll_name_str = coll_names.at(0);
            LOG_INFO("get shard num of coll '{}'", coll_name_str);
        }

        auto indexptr = coll_indexes->GetCollIndex(coll_name_str);
        if (!indexptr) {
            LOG_ERROR("Collection index '{}' not found", coll_name_str);
            return milvus::FailureCStatus(app::ErrIndexNotFound, "Collection index not found");
        }

        *shard_num = indexptr->GetTotalNumPartitions();
    } catch (std::exception &e) {
        LOG_ERROR("failed to get shard num: {}", e.what());
        return milvus::FailureCStatus(UnexpectedError, "failed to get shard num");
    }
    return milvus::SuccessCStatus();
}

CStatus
GetSnapshot(CSnapshot *c_snapshot, int level) {
    auto coll_ctx_mgr = app::IndexManagerSingleton::GetInstance().GetCollectionsCtxManager();
    if (!coll_ctx_mgr) {
        return milvus::FailureCStatus(UnexpectedError, "collection context manager is null");
    }

    auto snapshot = std::make_unique<milvus::proto::sr::CollectionsMetaSnapshot>();
    coll_ctx_mgr->GetSnapshot(snapshot.get(), level);

    size_t size = snapshot->ByteSizeLong();
    std::unique_ptr<uint8_t[]> data(new uint8_t[size]);
    if (!snapshot->SerializeToArray(data.get(), size)) {
        return CStatus{ProtoConversionError, "Failed to serialize retrieve result."};
    }
    c_snapshot->data = data.release();
    c_snapshot->length = size;

    return milvus::SuccessCStatus();
}

void
DeleteSnapshot(CSnapshot *c_snapshot) {
    delete[] c_snapshot->data;
    c_snapshot->data = nullptr;
    c_snapshot->length = 0;
}

CStatus
LoadIndex(const char *context_name, const char *collection_name, const char *index_config_str) {
    LOG_INFO("[Ctx:{}][Coll:{}]LoadIndex, config: {}", context_name, collection_name,
             index_config_str);
    try {
        std::shared_ptr<sr_core::app::CollectionIndex> indexptr;
        auto status = GetCollectionIndex(context_name, collection_name, indexptr);
        if (!Ok(status)) {
            return status;
        }

        auto index_config = milvus::Config::parse(index_config_str);

        auto partitions_config =
            milvus::index::GetValueFromConfig<milvus::Config>(index_config, PARTITIONS);
        if (!partitions_config.has_value()) {
            return milvus::FailureCStatus(app::ErrParameterMissing, "partitions not set");
        }

        for (const auto &prt_load_config : partitions_config.value()) {
            auto partition =
                milvus::index::GetValueFromConfig<int64_t>(prt_load_config, "partition");
            if (!partition.has_value()) {
                return milvus::FailureCStatus(app::ErrParameterMissing, "partition not set");
            }
            auto is_base = prt_load_config.value("is_base", true);
            std::string msg_header =
                fmt::format("[Ctx:{}][Coll:{}][Prt:{}][{}]", context_name, collection_name,
                            partition.value(), is_base ? "Base" : "Incr");

            if (!prt_load_config.contains("versions")) {
                LOG_ERROR("{}versions not set", msg_header);
                return milvus::FailureCStatus(app::ErrParameterMissing, "versions not set");
            }
            auto versions = prt_load_config.at("versions").get<std::vector<milvus::Timestamp>>();

            auto config = prt_load_config.value(STORAGE, milvus::Config());
            storage::StorageConfig storage_config;
            sr_core::app::from_json_to_storage(storage_config, config);
            auto chunk_manager = storage::CreateChunkManager(storage_config);

            auto load_strategy =
                milvus::index::GetValueFromConfig<std::string>(prt_load_config, LOAD_STRATEGY);

            for (int i = 0; i < versions.size(); i++) {
                std::filesystem::path base_meta =
                    std::filesystem::path(chunk_manager->GetRootPath()) /
                    std::to_string(versions[i]) / BASE_INDEX_META_FILE;

                milvus::Config load_config;
                load_config[BASE_META_FILE] = base_meta.string();
                load_config[TABLETYPE] = "normal";
                load_config[PARTITIONS] = {partition.value()};

                if (load_strategy.has_value()) {
                    load_config[LOAD_STRATEGY] = load_strategy.value();
                }

                auto res = indexptr->LoadIndex(load_config, chunk_manager, is_base);
                if (res.IsErr()) {
                    LOG_ERROR("{}failed to load version {}, msg: {}", msg_header, versions[i],
                              res.Detail());
                    // return milvus::FailureCStatus(res.Code(), res.Msg());
                } else {
                    LOG_INFO("{}Finish loading version {}", msg_header, versions[i]);
                }
            }
        }
    } catch (std::exception &e) {
        LOG_SEGCORE_ERROR_ << "LoadIndex failed. exception: " << e.what();
        return milvus::FailureCStatus(UnexpectedError, "exception occured in LoadIndex");
    }
    return milvus::SuccessCStatus();
}

CStatus
DeleteCollectionIndex(const char *context_name,
                      const char *collection_name,
                      const char *del_config_str) {
    LOG_INFO("[Ctx:{}][Coll:{}]DeleteCollectionIndex, config: {}", context_name, collection_name,
             del_config_str);
    try {
        std::shared_ptr<sr_core::app::CollectionIndex> indexptr;
        auto status = GetCollectionIndex(context_name, collection_name, indexptr);
        if (!Ok(status)) {
            return status;
        }
        auto del_config = milvus::Config::parse(del_config_str);

        auto del_partitions_config =
            milvus::index::GetValueFromConfig<milvus::Config>(del_config, PARTITIONS);
        if (!del_partitions_config.has_value()) {
            return milvus::FailureCStatus(app::ErrParameterMissing, "partitions not set");
        }

        for (const auto &prt_del_config : del_partitions_config.value()) {
            app::DeleteRange del_range;
            auto del_range_config =
                milvus::index::GetValueFromConfig<std::string>(prt_del_config, "range");
            if (del_range_config.has_value()) {
                if (del_range_config.value() == ONLY_BASE) {
                    del_range = app::DeleteRange::OnlyBase;
                } else if (del_range_config.value() == ONLY_INCR) {
                    del_range = app::DeleteRange::OnlyIncr;
                } else if (del_range_config.value() == BASE_AND_INCR) {
                    del_range = app::DeleteRange::BaseAndIncr;
                } else {
                    LOG_ERROR("del_range is invalid: {}, Context {}, Coll {}",
                              del_range_config.value(), context_name, collection_name);
                    return milvus::FailureCStatus(app::ErrParameterInvalid, "del_range is invalid");
                }
            }

            std::unordered_set<app::IndexVersion> versions;
            if (prt_del_config.contains("versions")) {
                versions =
                    prt_del_config.at("versions").get<std::unordered_set<app::IndexVersion>>();
            }
            auto partition =
                milvus::index::GetValueFromConfig<int64_t>(prt_del_config, "partition");
            if (!partition.has_value()) {
                return milvus::FailureCStatus(app::ErrParameterMissing, "partition not set");
            }

            auto res = indexptr->DeleteIndex(partition.value(), versions, del_range);
            if (res.IsErr()) {
                LOG_WARN("delete versions failed: msg: {}", res.Detail());
            }
        }
    } catch (std::exception &e) {
        LOG_ERROR("Context {}, Coll {}, exception occured when deleting versions: {}", context_name,
                  collection_name, e.what());
        return milvus::FailureCStatus(UnexpectedError, "delete versions failed");
    }
    return milvus::SuccessCStatus();
}

CStatus
GetCollInfo(const char *context_name,
            const char **collection_names,
            int coll_num,
            CSnapshot *c_snapshot) {
    try {
        auto snapshot = std::make_unique<milvus::proto::sr::CollContextInfo>();
        for (int i = 0; i < coll_num; i++) {
            std::shared_ptr<sr_core::app::CollectionIndex> indexptr;
            auto status = GetCollectionIndex(context_name, collection_names[i], indexptr);
            if (!Ok(status)) {
                LOG_ERROR("GetCollectionIndex failed. Context {}, Coll {}, msg: {}", context_name,
                          collection_names[i], status.error_msg);
                continue;
            }
            auto coll_info = std::make_unique<milvus::proto::sr::CollectionInfo>();
            indexptr->GetSnapshot(coll_info.get(), SNAPSHOT_QUERY_CONTEXT_LEVEL);
            snapshot->mutable_collections()->AddAllocated(coll_info.release());
        }

        size_t size = snapshot->ByteSizeLong();
        std::unique_ptr<uint8_t[]> data(new uint8_t[size]);
        if (!snapshot->SerializeToArray(data.get(), size)) {
            return CStatus{ProtoConversionError, "Failed to serialize retrieve result."};
        }
        c_snapshot->data = data.release();
        c_snapshot->length = size;
    } catch (std::exception &e) {
        LOG_ERROR("GetCollStat failed: Context {}, msg: {}", context_name, e.what());
        return milvus::FailureCStatus(UnexpectedError, "GetCollStat failed");
    }
    return milvus::SuccessCStatus();
}

CStatus
ReloadFlags(const char *gflags_json_str) {
    // TODO(zhanghe): support more flags
    static std::unordered_set<std::string> allowed_flags{"verbose",
                                                         "snapshot_coll_schema",
                                                         "join_num_per_batch",
                                                         "join_sortingbuff_size",
                                                         "reduce_parallel",
                                                         "query_verbose_logging_interval",
                                                         "query_max_threads",
                                                         "thread_pool_tasks_coefficient"};
    LOG_INFO("reload flags: {}", gflags_json_str);
    try {
        milvus::Config gflags_config = milvus::Config::parse(gflags_json_str);
        for (auto &[key, value] : gflags_config.items()) {
            if (allowed_flags.find(key) == allowed_flags.end()) {
                LOG_ERROR("reload of flag '{}' is currently not allowed.", key);
                continue;
            }
            auto value_str = value.template get<std::string>();
            auto result = gflags::SetCommandLineOption(key.c_str(), value_str.c_str());
            if (result.empty()) {
                LOG_ERROR("fail to set '{}'", key);
            } else {
                LOG_INFO(result);
                if (key == "query_max_threads") {
                    milvus::ThreadPools::SetThreadPoolCoefficient(
                        milvus::ThreadPoolPriority::PRIORITY_LEVEL_0, getQueryMaxThreads());
                } else if (key == "thread_pool_tasks_coefficient") {
                    milvus::ThreadPools::SetTasksPoolCoefficient(
                        milvus::ThreadPoolPriority::PRIORITY_LEVEL_0, getTasksPoolCoefficient());
                }
            }
        }
        return milvus::SuccessCStatus();
    } catch (std::exception &e) {
        LOG_ERROR("fail to reload flags. msg: {}", e.what());
        return CStatus{UnexpectedError, "fail to reload flags."};
    }
}

CStatus
UnloadCollections(const char *context_name, const char **collection_names, int coll_num) {
    LOG_INFO("unload collections of context '{}'", context_name);
    try {
        std::shared_ptr<sr_core::app::QueryCollections> coll_indexs;
        auto c_status = GetQueryCollections(context_name, coll_indexs);
        if (!Ok(c_status)) {
            return c_status;
        }
        for (int i = 0; i < coll_num; i++) {
            auto coll_name = collection_names[i];
            LOG_INFO("Unloading collection '{}'", coll_name);
            auto status = coll_indexs->UnloadCollection(coll_name);
            if (status.IsErr()) {
                LOG_WARN("Failed to unload collection '{}', msg: {}", coll_name, status.Detail());
            } else {
                LOG_INFO("Finish unloading collection '{}'", coll_name);
            }
        }
    } catch (std::exception &e) {
        LOG_SEGCORE_ERROR_ << "unload collections failed. msg: " << e.what();
        return CStatus{UnexpectedError, "unload collections failed."};
    }
    return milvus::SuccessCStatus();
}
