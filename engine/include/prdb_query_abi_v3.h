#ifndef PRDB_QUERY_ABI_V3_H
#define PRDB_QUERY_ABI_V3_H

#include "prdb_query_abi_v2.h"

/* This callback latches a statement failure in the trusted executor. It does
 * not expose terminal transaction authority to query code; after the entry
 * point returns, the executor exits unsuccessfully and the supervisor aborts. */
typedef int32_t (*prdb_query_statement_failure_v3_fn)(
    void *context, uint32_t failure_code);

#include "prdb_query_abi_records_v3.h"

typedef int32_t (*prdb_query_main_v3_fn)(const prdb_host_api_v3 *host,
                                         const prdb_invocation_v3 *invocation);

extern const prdb_query_descriptor_v3_record prdb_query_descriptor_v3;
int32_t prdb_query_main_v3(const prdb_host_api_v3 *host,
                           const prdb_invocation_v3 *invocation);

#include "prdb_query_abi_layout_assertions.h"

#endif
