#ifndef PRDB_QUERY_ABI_V1_H
#define PRDB_QUERY_ABI_V1_H

#include <stdint.h>

#define PRDB_QUERY_ABI_VERSION UINT32_C(1)
#define PRDB_QUERY_DESCRIPTOR_SYMBOL "prdb_query_descriptor_v1"
#define PRDB_QUERY_ENTRY_SYMBOL "prdb_query_main_v1"

typedef struct prdb_host_api_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
} prdb_host_api_v1;

typedef struct prdb_invocation_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    const uint8_t *parameters;
    uint64_t parameters_len;
} prdb_invocation_v1;

typedef struct prdb_query_descriptor_v1_record {
    uint32_t struct_size;
    uint32_t abi_version;
    uint32_t host_api_size;
    uint32_t invocation_size;
} prdb_query_descriptor_v1_record;

typedef int32_t (*prdb_query_main_v1_fn)(const prdb_host_api_v1 *host,
                                         const prdb_invocation_v1 *invocation);

extern const prdb_query_descriptor_v1_record prdb_query_descriptor_v1;
int32_t prdb_query_main_v1(const prdb_host_api_v1 *host,
                           const prdb_invocation_v1 *invocation);

#endif /* PRDB_QUERY_ABI_V1_H */
