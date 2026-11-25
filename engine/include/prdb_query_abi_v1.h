#ifndef PRDB_QUERY_ABI_V1_H
#define PRDB_QUERY_ABI_V1_H

#include <stdint.h>
#include "prdb_query_abi_constants.h"

enum prdb_query_callback_status_v1 {
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
typedef int32_t (*prdb_query_ipc_request_v1_fn)(
    void *context, uint16_t operation, const uint8_t *request,
    uint64_t request_len, uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len);

/* On success response_len is the complete response size. On RESPONSE_TOO_SMALL,
 * response_len reports the required capacity and response bytes are untouched. */

/* Writes provisional application bytes. It cannot finalize or abort storage. */
typedef int32_t (*prdb_query_write_result_v1_fn)(
    void *context, const uint8_t *bytes, uint64_t bytes_len);

/* This callback latches a statement failure in the trusted executor. It does
 * not expose terminal transaction authority to query code; after the entry
 * point returns, the executor exits unsuccessfully and the supervisor aborts. */
typedef int32_t (*prdb_query_statement_failure_v1_fn)(
    void *context, uint32_t failure_code);

/* Bounded optimizer callback whose operation is selected by the SDK request
 * path. The host still applies the provisioned role allowlist; this callback
 * grants no storage authority. */
typedef int32_t (*prdb_query_optimizer_request_v1_fn)(
    void *context, uint16_t operation, const uint8_t *request,
    uint64_t request_len, uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len);

/* Bounded callback over the supervisor-provisioned opaque spill channel.
 * Requests have no filesystem or invocation identity fields. */
typedef int32_t (*prdb_query_spill_request_v1_fn)(
    void *context, uint16_t operation, const uint8_t *request,
    uint64_t request_len, uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len);

#include "prdb_query_abi_records_v1.h"

typedef int32_t (*prdb_query_main_v1_fn)(const prdb_host_api_v1 *host,
                                         const prdb_invocation_v1 *invocation);

extern const prdb_query_descriptor_v1_record prdb_query_descriptor_v1;
int32_t prdb_query_main_v1(const prdb_host_api_v1 *host,
                           const prdb_invocation_v1 *invocation);

#endif /* PRDB_QUERY_ABI_V1_H */
