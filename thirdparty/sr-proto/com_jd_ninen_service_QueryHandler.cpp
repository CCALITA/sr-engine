#include "com_jd_ninen_service_QueryHandler.h"
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <cstring>
#include <string>
#include <vector>
#include <memory>
#include "log/Log.h"

// Define ErrorCode if not available (assuming Success is 0)
namespace milvus {
namespace segcore {
    // Basic definitions if headers are missing/changed
}
}
// Assuming ErrorCode is available via other headers or we define a mock
enum class ErrorCode {
    Success = 0,
    UnexpectedError = 1
};

// RAII for JNI String
class JStringHandleScope {
  public:
    JStringHandleScope(JNIEnv *env, jstring &jinput_str) : env_(env), jinput_str_(jinput_str) {
        if (jinput_str == nullptr) {
            return;
        }
        output_str_ = env->GetStringUTFChars(jinput_str, JNI_FALSE);
        len_ = env->GetStringUTFLength(jinput_str);
    }
    ~JStringHandleScope() {
        if (output_str_ != nullptr) {
            env_->ReleaseStringUTFChars(jinput_str_, output_str_);
        }
    }
    const char *GetStr() { return output_str_ ? output_str_ : ""; }
    jsize GetLen() { return len_; }

  private:
    JNIEnv *env_;
    jstring &jinput_str_;
    const char *output_str_ = nullptr;
    jsize len_ = 0;
};

namespace ipc {

constexpr const char* kSocketPath = "/tmp/milvus-sr-ipc.sock";
constexpr const char* kGraphName = "milvus_sr_ipc_handler";

struct ByteBuffer {
    std::vector<char> data;
    
    void append_u32(uint32_t val) {
        uint32_t net_val = htonl(val);
        const char* p = reinterpret_cast<const char*>(&net_val);
        data.insert(data.end(), p, p + 4);
    }
    
    void append_u64(uint64_t val) {
        // Simple manual pack for u64 (network byte order)
        uint32_t high = htonl(val >> 32);
        uint32_t low = htonl(val & 0xFFFFFFFF);
        const char* ph = reinterpret_cast<const char*>(&high);
        const char* pl = reinterpret_cast<const char*>(&low);
        data.insert(data.end(), ph, ph + 4);
        data.insert(data.end(), pl, pl + 4);
    }

    void append_string(const std::string& str) {
        append_u32(static_cast<uint32_t>(str.size()));
        data.insert(data.end(), str.begin(), str.end());
    }
    
    void append_bytes(const void* ptr, size_t size) {
        append_u32(static_cast<uint32_t>(size));
        const char* p = reinterpret_cast<const char*>(ptr);
        data.insert(data.end(), p, p + size);
    }
    
    void append_long_array(const jlong* arr, jsize len) {
        append_u32(static_cast<uint32_t>(len));
        for (int i = 0; i < len; ++i) {
            append_u64(static_cast<uint64_t>(arr[i]));
        }
    }
};

class BinaryReader {
    const char* ptr_;
    size_t remaining_;
public:
    BinaryReader(const std::vector<char>& data) : ptr_(data.data()), remaining_(data.size()) {}
    
    bool read_u32(uint32_t& out) {
        if (remaining_ < 4) return false;
        uint32_t net_val;
        std::memcpy(&net_val, ptr_, 4);
        out = ntohl(net_val);
        ptr_ += 4;
        remaining_ -= 4;
        return true;
    }
    
    bool read_u64(uint64_t& out) {
        if (remaining_ < 8) return false;
        uint32_t high, low;
        std::memcpy(&high, ptr_, 4);
        std::memcpy(&low, ptr_ + 4, 4);
        out = (static_cast<uint64_t>(ntohl(high)) << 32) | ntohl(low);
        ptr_ += 8;
        remaining_ -= 8;
        return true;
    }
    
    bool read_string(std::string& out) {
        uint32_t len;
        if (!read_u32(len)) return false;
        if (remaining_ < len) return false;
        out.assign(ptr_, len);
        ptr_ += len;
        remaining_ -= len;
        return true;
    }

    bool read_bytes(std::vector<char>& out) {
        uint32_t len;
        if (!read_u32(len)) return false;
        if (remaining_ < len) return false;
        out.assign(ptr_, ptr_ + len);
        ptr_ += len;
        remaining_ -= len;
        return true;
    }

    const char* current() const { return ptr_; }
    size_t remaining() const { return remaining_; }
};

bool read_exact(int fd, void *buffer, size_t size) {
  auto *out = static_cast<unsigned char *>(buffer);
  size_t offset = 0;
  while (offset < size) {
    const auto n = ::read(fd, out + offset, size - offset);
    if (n <= 0) return false;
    offset += static_cast<size_t>(n);
  }
  return true;
}

bool write_all(int fd, const void *buffer, size_t size) {
  const auto *data = static_cast<const unsigned char *>(buffer);
  size_t offset = 0;
  while (offset < size) {
    const auto n = ::write(fd, data + offset, size - offset);
    if (n < 0) return false;
    offset += static_cast<size_t>(n);
  }
  return true;
}

bool read_u32_be(int fd, uint32_t &value) {
  uint32_t raw = 0;
  if (!read_exact(fd, &raw, 4)) return false;
  value = ntohl(raw);
  return true;
}

bool write_u32_be(int fd, uint32_t value) {
  const auto raw = htonl(value);
  return write_all(fd, &raw, 4);
}

bool read_string(int fd, std::string &value) {
  uint32_t len = 0;
  if (!read_u32_be(fd, len)) return false;
  value.resize(len);
  return read_exact(fd, value.data(), len);
}

bool write_string(int fd, const std::string& value) {
  if (!write_u32_be(fd, static_cast<uint32_t>(value.size()))) return false;
  return write_all(fd, value.data(), value.size());
}

struct IpcResponse {
    int32_t error_code;
    std::string error_msg;
    std::vector<char> payload;
};

IpcResponse Call(const std::string& method, const ByteBuffer& args) {
    int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) return { -1, "Socket creation failed", {} };

    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, kSocketPath, sizeof(addr.sun_path) - 1);

    if (::connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        return { -1, "Connection failed: " + std::string(kSocketPath), {} };
    }

    // Write Method
    if (!write_string(fd, method)) { ::close(fd); return { -1, "Write method failed", {} }; }

    // Write Metadata (Graph Name)
    if (!write_u32_be(fd, 1)) { ::close(fd); return { -1, "Write meta count failed", {} }; }
    if (!write_string(fd, "sr-graph-name") || !write_string(fd, kGraphName)) {
        ::close(fd); return { -1, "Write meta failed", {} };
    }

    // Write Payload
    if (!write_u32_be(fd, args.data.size()) || !write_all(fd, args.data.data(), args.data.size())) {
        ::close(fd); return { -1, "Write payload failed", {} };
    }

    // Read Response
    uint32_t status_code;
    if (!read_u32_be(fd, status_code)) { ::close(fd); return { -1, "Read status failed", {} }; }
    
    std::string status_msg, status_details;
    if (!read_string(fd, status_msg) || !read_string(fd, status_details)) {
        ::close(fd); return { -1, "Read status msg failed", {} };
    }

    uint32_t meta_count;
    if (!read_u32_be(fd, meta_count)) { ::close(fd); return { -1, "Read meta count failed", {} }; }
    for(uint32_t i=0; i<meta_count; ++i) {
        std::string k, v;
        read_string(fd, k); read_string(fd, v);
    }

    std::string resp_payload_str;
    if (!read_string(fd, resp_payload_str)) { ::close(fd); return { -1, "Read payload failed", {} }; }
    
    ::close(fd);
    
    std::vector<char> resp_payload(resp_payload_str.begin(), resp_payload_str.end());
    
    // Parse our application-level payload: [i32 error_code] [u32 msg_len] [msg] [outputs...]
    BinaryReader reader(resp_payload);
    uint32_t app_err_code_u32 = 0;
    if (!reader.read_u32(app_err_code_u32)) return { -1, "Malformed response payload", {} };
    
    std::string app_err_msg;
    reader.read_string(app_err_msg);
    
    std::vector<char> output_data;
    if (reader.remaining() > 0) {
        output_data.assign(reader.current(), reader.current() + reader.remaining());
    }
    
    return { static_cast<int32_t>(app_err_code_u32), app_err_msg, output_data };
}

} // namespace ipc

// JNI Implementations

extern "C" {

JNIEXPORT jlong JNICALL
Java_com_jd_ninen_service_QueryHandler_createQueryCtx__Ljava_lang_String_2Ljava_lang_String_2(
    JNIEnv *env, jobject jobj, jstring jconfig_path, jstring jname) {
    JStringHandleScope jconfig_path_scope(env, jconfig_path);
    JStringHandleScope jname_scope(env, jname);
    
    ipc::ByteBuffer args;
    args.append_string(jconfig_path_scope.GetStr());
    args.append_string(jname_scope.GetStr());

    auto resp = ipc::Call("CreateQueryContext", args);
    if (resp.error_code != 0) {
        // Log error
        std::cerr << "CreateQueryContext failed: " << resp.error_msg << std::endl;
        return 0;
    }
    
    ipc::BinaryReader reader(resp.payload);
    uint64_t ptr_val = 0;
    reader.read_u64(ptr_val);
    return static_cast<jlong>(ptr_val);
}

JNIEXPORT jlong JNICALL
Java_com_jd_ninen_service_QueryHandler_createQueryCtx__Ljava_lang_String_2Ljava_lang_String_2Ljava_lang_String_2(
    JNIEnv *env, jobject jobj, jstring jconfig_path, jstring jcontext_name, jstring jcollection_name) {
    JStringHandleScope s1(env, jconfig_path);
    JStringHandleScope s2(env, jcontext_name);
    JStringHandleScope s3(env, jcollection_name);

    ipc::ByteBuffer args;
    args.append_string(s1.GetStr());
    args.append_string(s2.GetStr());
    args.append_string(s3.GetStr());

    auto resp = ipc::Call("CreateQueryContextV2", args);
    if (resp.error_code != 0) {
        std::cerr << "CreateQueryContextV2 failed: " << resp.error_msg << std::endl;
        return 0;
    }

    ipc::BinaryReader reader(resp.payload);
    uint64_t ptr_val = 0;
    reader.read_u64(ptr_val);
    return static_cast<jlong>(ptr_val);
}

JNIEXPORT jboolean JNICALL
Java_com_jd_ninen_service_QueryHandler_initContext(JNIEnv *env, jobject jobj, jstring jconfig, jstring jcontext_name) {
    JStringHandleScope s1(env, jconfig);
    JStringHandleScope s2(env, jcontext_name);

    ipc::ByteBuffer args;
    args.append_string(s1.GetStr());
    args.append_string(s2.GetStr());

    auto resp = ipc::Call("InitContext", args);
    return resp.error_code == 0 ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT jint JNICALL
Java_com_jd_ninen_service_QueryHandler_getQueryCtxStat__Ljava_lang_String_2J(JNIEnv *env, jobject jobj, jstring jcontext_name, jlong partition_id) {
    JStringHandleScope s1(env, jcontext_name);
    ipc::ByteBuffer args;
    args.append_string(s1.GetStr());
    args.append_u64(static_cast<uint64_t>(partition_id));

    auto resp = ipc::Call("GetCollCtxPrtStat", args); // Assuming this maps to deprecated one too or logic is similar
    if (resp.error_code != 0) return 0;
    
    ipc::BinaryReader reader(resp.payload);
    uint32_t status = 0;
    reader.read_u32(status);
    return (status == 1) ? 1 : 0; // CtxIndexReady = 1
}

JNIEXPORT jint JNICALL
Java_com_jd_ninen_service_QueryHandler_getCollCtxPrtStat(JNIEnv *env, jobject jobj, jstring jcontext_name, jlong partition_id) {
    JStringHandleScope s1(env, jcontext_name);
    ipc::ByteBuffer args;
    args.append_string(s1.GetStr());
    args.append_u64(static_cast<uint64_t>(partition_id));

    auto resp = ipc::Call("GetCollCtxPrtStat", args);
    if (resp.error_code != 0) return 0;

    ipc::BinaryReader reader(resp.payload);
    uint32_t status = 0;
    reader.read_u32(status);
    return (status == 1) ? 1 : 0;
}

JNIEXPORT jint JNICALL
Java_com_jd_ninen_service_QueryHandler_getCollPrtStat(JNIEnv *env, jobject jobj, jstring jcontext_name, jstring jcollection_name, jlong partition_id) {
    JStringHandleScope s1(env, jcontext_name);
    JStringHandleScope s2(env, jcollection_name);
    ipc::ByteBuffer args;
    args.append_string(s1.GetStr());
    args.append_string(s2.GetStr());
    args.append_u64(static_cast<uint64_t>(partition_id));

    auto resp = ipc::Call("GetCollPrtStat", args);
    if (resp.error_code != 0) return 0;

    ipc::BinaryReader reader(resp.payload);
    uint32_t status = 0;
    reader.read_u32(status);
    return (status == 2) ? 1 : 0; // Ready = 2
}

JNIEXPORT jint JNICALL
Java_com_jd_ninen_service_QueryHandler_getQueryCtxStat__J(JNIEnv *env, jobject jobj, jlong jquery_ctx_ptr) {
    ipc::ByteBuffer args;
    args.append_u64(static_cast<uint64_t>(jquery_ctx_ptr));

    auto resp = ipc::Call("GetQueryCtxStat", args);
    if (resp.error_code != 0) return 0;

    ipc::BinaryReader reader(resp.payload);
    uint32_t status = 0;
    reader.read_u32(status);
    return (status == 2) ? 1 : 0; // Ready = 2
}

JNIEXPORT jboolean JNICALL
Java_com_jd_ninen_service_QueryHandler_applyConfig(JNIEnv *env, jobject jobj, jstring jindex_name, jstring jconfig_json_str) {
    JStringHandleScope s1(env, jindex_name);
    JStringHandleScope s2(env, jconfig_json_str);
    ipc::ByteBuffer args;
    args.append_string(s1.GetStr());
    args.append_string(s2.GetStr());

    auto resp = ipc::Call("ApplyConfig", args);
    return resp.error_code == 0 ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT jboolean JNICALL
Java_com_jd_ninen_service_QueryHandler_assignPartitions(JNIEnv *env, jobject jobj, jstring jcontext_name, jlongArray jpartitions) {
    JStringHandleScope s1(env, jcontext_name);
    jlong *partitions = env->GetLongArrayElements(jpartitions, NULL);
    jsize partition_num = env->GetArrayLength(jpartitions);

    ipc::ByteBuffer args;
    args.append_string(s1.GetStr());
    args.append_long_array(partitions, partition_num);

    env->ReleaseLongArrayElements(jpartitions, partitions, 0);

    auto resp = ipc::Call("AssignPartitions", args);
    return resp.error_code == 0 ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT jlongArray JNICALL
Java_com_jd_ninen_service_QueryHandler_getAssignedPrts(JNIEnv *env, jobject jobj, jstring jcontext_name, jstring jcollection_name) {
    JStringHandleScope s1(env, jcontext_name);
    JStringHandleScope s2(env, jcollection_name);
    ipc::ByteBuffer args;
    args.append_string(s1.GetStr());
    args.append_string(s2.GetStr());

    auto resp = ipc::Call("GetAssignedPrts", args);
    if (resp.error_code != 0) return nullptr;

    ipc::BinaryReader reader(resp.payload);
    uint32_t count = 0;
    if (!reader.read_u32(count)) return nullptr;
    
    jlongArray jarr = env->NewLongArray(count);
    if (count > 0) {
        std::vector<jlong> buffer(count);
        for(uint32_t i=0; i<count; ++i) {
            uint64_t val;
            reader.read_u64(val);
            buffer[i] = static_cast<jlong>(val);
        }
        env->SetLongArrayRegion(jarr, 0, count, buffer.data());
    }
    return jarr;
}

JNIEXPORT jlong JNICALL
Java_com_jd_ninen_service_QueryHandler_getShardNum(JNIEnv *env, jobject jobj, jstring jcontext_name, jstring jcollection_name) {
    JStringHandleScope s1(env, jcontext_name);
    JStringHandleScope s2(env, jcollection_name);
    ipc::ByteBuffer args;
    args.append_string(s1.GetStr());
    args.append_string(s2.GetStr());

    auto resp = ipc::Call("GetShardNum", args);
    if (resp.error_code != 0) return -1;
    
    ipc::BinaryReader reader(resp.payload);
    uint64_t val = 0;
    reader.read_u64(val);
    return static_cast<jlong>(val);
}

JNIEXPORT jbyteArray JNICALL
Java_com_jd_ninen_service_QueryHandler_getSnapshot(JNIEnv *env, jobject jobj, jint jlevel) {
    ipc::ByteBuffer args;
    args.append_u32(static_cast<uint32_t>(jlevel));

    auto resp = ipc::Call("GetSnapshot", args);
    if (resp.error_code != 0) return nullptr;

    ipc::BinaryReader reader(resp.payload);
    std::vector<char> snapshot_data;
    reader.read_bytes(snapshot_data);

    jbyteArray jarr = env->NewByteArray(snapshot_data.size());
    env->SetByteArrayRegion(jarr, 0, snapshot_data.size(), reinterpret_cast<const signed char*>(snapshot_data.data()));
    return jarr;
}

JNIEXPORT jboolean JNICALL
Java_com_jd_ninen_service_QueryHandler_updateCollections(JNIEnv *env, jobject jobj, jstring jcontext_name, jstring jconfig) {
    JStringHandleScope s1(env, jcontext_name);
    JStringHandleScope s2(env, jconfig);
    ipc::ByteBuffer args;
    args.append_string(s1.GetStr());
    args.append_string(s2.GetStr());

    auto resp = ipc::Call("UpdateCollections", args);
    return resp.error_code == 0 ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT jint JNICALL
Java_com_jd_ninen_service_QueryHandler_loadCollIndex(JNIEnv *env, jobject jobj, jstring jcontext_name, jstring jcollection_name, jstring jconfig) {
    JStringHandleScope s1(env, jcontext_name);
    JStringHandleScope s2(env, jcollection_name);
    JStringHandleScope s3(env, jconfig);
    ipc::ByteBuffer args;
    args.append_string(s1.GetStr());
    args.append_string(s2.GetStr());
    args.append_string(s3.GetStr());

    auto resp = ipc::Call("LoadIndex", args);
    return static_cast<jint>(resp.error_code);
}

JNIEXPORT jint JNICALL
Java_com_jd_ninen_service_QueryHandler_deleteCollIndex(JNIEnv *env, jobject jobj, jstring jcontext_name, jstring jcollection_name, jstring jconfig) {
    JStringHandleScope s1(env, jcontext_name);
    JStringHandleScope s2(env, jcollection_name);
    JStringHandleScope s3(env, jconfig);
    ipc::ByteBuffer args;
    args.append_string(s1.GetStr());
    args.append_string(s2.GetStr());
    args.append_string(s3.GetStr());

    auto resp = ipc::Call("DeleteCollectionIndex", args);
    return static_cast<jint>(resp.error_code);
}

JNIEXPORT jbyteArray JNICALL
Java_com_jd_ninen_service_QueryHandler_getCollInfo(JNIEnv *env, jobject jobj, jstring jcontext_name, jobjectArray jcollection_names) {
    JStringHandleScope s1(env, jcontext_name);
    jsize length = env->GetArrayLength(jcollection_names);
    
    ipc::ByteBuffer args;
    args.append_string(s1.GetStr());
    args.append_u32(static_cast<uint32_t>(length));
    
    for (int i = 0; i < length; i++) {
        jstring coll_name = (jstring)env->GetObjectArrayElement(jcollection_names, i);
        const char *raw_str = env->GetStringUTFChars(coll_name, nullptr);
        args.append_string(raw_str);
        env->ReleaseStringUTFChars(coll_name, raw_str);
        env->DeleteLocalRef(coll_name);
    }

    auto resp = ipc::Call("GetCollInfo", args);
    if (resp.error_code != 0) return nullptr;

    ipc::BinaryReader reader(resp.payload);
    std::vector<char> data;
    reader.read_bytes(data);

    jbyteArray jarr = env->NewByteArray(data.size());
    env->SetByteArrayRegion(jarr, 0, data.size(), reinterpret_cast<const signed char*>(data.data()));
    return jarr;
}

JNIEXPORT jboolean JNICALL
Java_com_jd_ninen_service_QueryHandler_reloadFlags(JNIEnv *env, jobject jobj, jstring jconfig) {
    JStringHandleScope s1(env, jconfig);
    ipc::ByteBuffer args;
    args.append_string(s1.GetStr());

    auto resp = ipc::Call("ReloadFlags", args);
    return resp.error_code == 0 ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT jint JNICALL
Java_com_jd_ninen_service_QueryHandler_unloadCollections(JNIEnv *env, jobject jobj, jstring jcontext_name, jobjectArray jcollection_names) {
    JStringHandleScope s1(env, jcontext_name);
    jsize length = env->GetArrayLength(jcollection_names);

    ipc::ByteBuffer args;
    args.append_string(s1.GetStr());
    args.append_u32(static_cast<uint32_t>(length));
    
    for (int i = 0; i < length; i++) {
        jstring coll_name = (jstring)env->GetObjectArrayElement(jcollection_names, i);
        const char *raw_str = env->GetStringUTFChars(coll_name, nullptr);
        args.append_string(raw_str);
        env->ReleaseStringUTFChars(coll_name, raw_str);
        env->DeleteLocalRef(coll_name);
    }

    auto resp = ipc::Call("UnloadCollections", args);
    return static_cast<jint>(resp.error_code);
}

} // extern "C"