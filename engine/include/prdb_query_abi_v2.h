#ifndef PRDB_QUERY_ABI_V2_H
#define PRDB_QUERY_ABI_V2_H

#include <stdint.h>

#define PRDB_QUERY_ABI_V2_VERSION UINT32_C(2)
#define PRDB_QUERY_DESCRIPTOR_V2_SYMBOL "prdb_query_descriptor_v2"
#define PRDB_QUERY_ENTRY_V2_SYMBOL "prdb_query_main_v2"
#define PRDB_QUERY_MAX_IPC_REQUEST_BYTES UINT64_C(1048576)
#define PRDB_QUERY_MAX_IPC_RESPONSE_BYTES UINT64_C(1048576)
#define PRDB_QUERY_MAX_RESULT_CHUNK_BYTES UINT64_C(16384)
#define PRDB_QUERY_MAX_TOTAL_RESULT_BYTES UINT64_C(67108864)

/* Operation authority is fixed by the provisioned channel. The storage or
 * planner endpoint still validates each operation and payload independently. */
typedef int32_t (*prdb_query_ipc_request_v2_fn)(
    void *context, uint16_t operation, const uint8_t *request,
    uint64_t request_len, uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len);

/* Writes provisional application bytes. It cannot finalize or abort storage. */
typedef int32_t (*prdb_query_write_result_v2_fn)(
    void *context, const uint8_t *bytes, uint64_t bytes_len);

typedef struct prdb_host_api_v2 {
    uint32_t struct_size;
    uint32_t abi_version;
    void *context;
    prdb_query_ipc_request_v2_fn executor_request;
    prdb_query_ipc_request_v2_fn planner_request;
    prdb_query_write_result_v2_fn write_result;
} prdb_host_api_v2;

typedef struct prdb_invocation_v2 {
    uint32_t struct_size;
    uint32_t abi_version;
    const uint8_t *parameters;
    uint64_t parameters_len;
} prdb_invocation_v2;

typedef struct prdb_query_descriptor_v2_record {
    uint32_t struct_size;
    uint32_t abi_version;
    uint32_t host_api_size;
    uint32_t invocation_size;
} prdb_query_descriptor_v2_record;

typedef int32_t (*prdb_query_main_v2_fn)(const prdb_host_api_v2 *host,
                                         const prdb_invocation_v2 *invocation);

extern const prdb_query_descriptor_v2_record prdb_query_descriptor_v2;
int32_t prdb_query_main_v2(const prdb_host_api_v2 *host,
                           const prdb_invocation_v2 *invocation);

#endif /* PRDB_QUERY_ABI_V2_H */
