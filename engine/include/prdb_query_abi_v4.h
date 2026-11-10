#ifndef PRDB_QUERY_ABI_V4_H
#define PRDB_QUERY_ABI_V4_H

#include "prdb_query_abi_v2.h"

typedef int32_t (*prdb_query_statement_failure_v3_fn)(
    void *context, uint32_t failure_code);

#include "prdb_query_abi_records_v3.h"

/* This callback submits the canonical bounded SELECT v1 payload to the
 * optimizer process. The provisioned channel fixes the operation to
 * OptimizeSelect and grants no storage authority. */
typedef int32_t (*prdb_query_optimizer_request_v4_fn)(
    void *context, const uint8_t *request, uint64_t request_len,
    uint8_t *response, uint64_t response_capacity, uint64_t *response_len);

#include "prdb_query_abi_records_v4.h"

typedef int32_t (*prdb_query_main_v4_fn)(const prdb_host_api_v4 *host,
                                         const prdb_invocation_v4 *invocation);

extern const prdb_query_descriptor_v4_record prdb_query_descriptor_v4;
int32_t prdb_query_main_v4(const prdb_host_api_v4 *host,
                           const prdb_invocation_v4 *invocation);

#endif /* PRDB_QUERY_ABI_V4_H */
