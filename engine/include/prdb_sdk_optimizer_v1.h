#ifndef PRDB_SDK_OPTIMIZER_V1_H
#define PRDB_SDK_OPTIMIZER_V1_H

#include "prdb_query_abi_v4.h"
#include "prdb_sdk_sql_v2.h"

#define PRDB_SDK_OPTIMIZER_ABI_V1_VERSION UINT32_C(1)
#define PRDB_SDK_OPTIMIZER_RESPONSE_V1_VERSION UINT16_C(1)
#define PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES UINT32_C(65536)
#define PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES UINT32_C(65536)

#define PRDB_SDK_OPTIMIZER_STATUS_SUCCESS UINT32_C(0)
#define PRDB_SDK_OPTIMIZER_STATUS_INVALID_REQUEST UINT32_C(1)
#define PRDB_SDK_OPTIMIZER_STATUS_UNAVAILABLE UINT32_C(2)
#define PRDB_SDK_OPTIMIZER_STATUS_REJECTED UINT32_C(3)
#define PRDB_SDK_OPTIMIZER_STATUS_INTERNAL UINT32_C(4)

#define PRDB_SDK_OPTIMIZER_RESULT_OK INT32_C(0)
#define PRDB_SDK_OPTIMIZER_RESULT_INVALID_ARGUMENT INT32_C(1)
#define PRDB_SDK_OPTIMIZER_RESULT_LIMIT_EXCEEDED INT32_C(2)
#define PRDB_SDK_OPTIMIZER_RESULT_CALLBACK_ERROR INT32_C(3)
#define PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE INT32_C(4)
#define PRDB_SDK_OPTIMIZER_RESULT_RESPONSE_TOO_SMALL INT32_C(5)

#define PRDB_SDK_OPTIMIZER_ENCODE_OK INT32_C(0)
#define PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT INT32_C(1)
#define PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED INT32_C(2)
#define PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED INT32_C(3)
#define PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL INT32_C(4)

/* The plan bytes borrow the response buffer supplied to submit_select_v1 and
 * remain valid only while that buffer is unchanged and alive. The SDK checks
 * the response envelope here; plan structure validation belongs to the
 * independent plan decoder before execution. */
typedef struct prdb_sdk_optimizer_response_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    uint32_t status;
    const uint8_t *plan;
    uint32_t plan_len;
} prdb_sdk_optimizer_response_v1;

/* Encode an explicit-column, single-relation SELECT AST as optimizer request
 * v1. This first encoder supports no WHERE tree; predicates remain on the SQL
 * executor path until request v2 carries positional parameters. Schema is
 * supplied explicitly by the caller and is never inferred by this helper.
 * On ENCODE_BUFFER_TOO_SMALL, request_len receives the required byte count
 * and the request buffer is unchanged. */
int32_t prdb_sdk_optimizer_encode_select_v1(
    const prdb_sdk_select_v2 *statement,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity, uint64_t *request_len);

int32_t prdb_sdk_optimizer_decode_response_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_response_v1 *out, uint32_t out_capacity);

/* Sends one already encoded structured SELECT v1 request. The callback uses
 * only the fixed optimizer submission operation. `out->plan` borrows from
 * `response`; caller storage must not overlap the output record. */
int32_t prdb_sdk_optimizer_submit_select_v1(
    const prdb_host_api_v4 *host,
    const uint8_t *request, uint64_t request_len,
    uint8_t *response, uint64_t response_capacity, uint64_t *response_len,
    prdb_sdk_optimizer_response_v1 *out, uint32_t out_capacity);

#endif /* PRDB_SDK_OPTIMIZER_V1_H */
