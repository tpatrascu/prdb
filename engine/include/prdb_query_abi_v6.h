#ifndef PRDB_QUERY_ABI_V6_H
#define PRDB_QUERY_ABI_V6_H

#include "prdb_query_abi_v5.h"

/* ABI v6 adds one bounded callback over the supervisor-provisioned opaque
 * spill channel. Requests have no filesystem or invocation identity fields. */
typedef int32_t (*prdb_query_spill_request_v1_fn)(
    void *context, uint16_t operation, const uint8_t *request,
    uint64_t request_len, uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len);

#include "prdb_query_abi_records_v6.h"

typedef int32_t (*prdb_query_main_v6_fn)(const prdb_host_api_v6 *host,
                                         const prdb_invocation_v6 *invocation);

extern const prdb_query_descriptor_v6_record prdb_query_descriptor_v6;
int32_t prdb_query_main_v6(const prdb_host_api_v6 *host,
                           const prdb_invocation_v6 *invocation);

#endif /* PRDB_QUERY_ABI_V6_H */
