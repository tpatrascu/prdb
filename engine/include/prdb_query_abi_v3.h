#ifndef PRDB_QUERY_ABI_V3_H
#define PRDB_QUERY_ABI_V3_H

#include "prdb_query_abi_v2.h"

/* This callback latches a statement failure in the trusted executor. It does
 * not expose terminal transaction authority to query code; after the entry
 * point returns, the executor exits unsuccessfully and the supervisor aborts. */
typedef int32_t (*prdb_query_statement_failure_v3_fn)(
    void *context, uint32_t failure_code);

typedef struct prdb_host_api_v3 {
    uint32_t struct_size;
    uint32_t abi_version;
    void *context;
    prdb_query_ipc_request_v2_fn executor_request;
    prdb_query_ipc_request_v2_fn planner_request;
    prdb_query_write_result_v2_fn write_result;
    prdb_query_statement_failure_v3_fn statement_failure;
} prdb_host_api_v3;

typedef prdb_invocation_v2 prdb_invocation_v3;

typedef struct prdb_query_descriptor_v3_record {
    uint32_t struct_size;
    uint32_t abi_version;
    uint32_t host_api_size;
    uint32_t invocation_size;
} prdb_query_descriptor_v3_record;

typedef int32_t (*prdb_query_main_v3_fn)(const prdb_host_api_v3 *host,
                                         const prdb_invocation_v3 *invocation);

extern const prdb_query_descriptor_v3_record prdb_query_descriptor_v3;
int32_t prdb_query_main_v3(const prdb_host_api_v3 *host,
                           const prdb_invocation_v3 *invocation);

#endif
