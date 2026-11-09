#ifndef PRDB_QUERY_ABI_V2_H
#define PRDB_QUERY_ABI_V2_H

#include "prdb_query_abi_v1.h"

enum prdb_query_callback_status_v2 {
    PRDB_QUERY_CALLBACK_OK = 0,
    PRDB_QUERY_CALLBACK_INVALID_ARGUMENT = 1,
    PRDB_QUERY_CALLBACK_LIMIT_EXCEEDED = 2,
    PRDB_QUERY_CALLBACK_IO_ERROR = 3,
    PRDB_QUERY_CALLBACK_PROTOCOL_ERROR = 4,
    PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL = 5,
    PRDB_QUERY_CALLBACK_INTERNAL_ERROR = 6
};

/* Operation authority is fixed by the provisioned channel. The storage or
 * planner endpoint still validates each operation and payload independently. */
typedef int32_t (*prdb_query_ipc_request_v2_fn)(
    void *context, uint16_t operation, const uint8_t *request,
    uint64_t request_len, uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len);

/* On success response_len is the complete response size. On RESPONSE_TOO_SMALL,
 * response_len reports the required capacity and response bytes are untouched. */

/* Writes provisional application bytes. It cannot finalize or abort storage. */
typedef int32_t (*prdb_query_write_result_v2_fn)(
    void *context, const uint8_t *bytes, uint64_t bytes_len);

#include "prdb_query_abi_records_v2.h"

typedef int32_t (*prdb_query_main_v2_fn)(const prdb_host_api_v2 *host,
                                         const prdb_invocation_v2 *invocation);

extern const prdb_query_descriptor_v2_record prdb_query_descriptor_v2;
int32_t prdb_query_main_v2(const prdb_host_api_v2 *host,
                           const prdb_invocation_v2 *invocation);

#endif /* PRDB_QUERY_ABI_V2_H */
