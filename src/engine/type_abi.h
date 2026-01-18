#pragma once

#include <cstdint>

#ifdef __cplusplus
extern "C" {
#endif

#define SR_TYPE_PLUGIN 1000

typedef struct sr_type_descriptor {
    uint32_t kind;
    const char* name;
    uint32_t version;
    uint64_t layout_size;
    uint64_t layout_align;
} sr_type_descriptor;

#ifdef __cplusplus
}
#endif
