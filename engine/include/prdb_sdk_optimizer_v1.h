#ifndef PRDB_SDK_OPTIMIZER_V1_H
#define PRDB_SDK_OPTIMIZER_V1_H

#include "prdb_query_abi_v1.h"
#include "prdb_sdk_sql_dml_v1.h"
#include "prdb_sdk_sql_v1.h"

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
 * v1. This compatibility form supports no WHERE tree. */
int32_t prdb_sdk_optimizer_encode_select_v1(
    const prdb_sdk_select_v1 *statement,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity, uint64_t *request_len);

/* Encode the bounded unfiltered SQL v3 SELECT with one source-column ORDER BY. */
int32_t prdb_sdk_optimizer_encode_ordered_select_v1(
    const prdb_sdk_select_v1 *statement,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity, uint64_t *request_len);

/* Encode the same request with bounded parameterized comparisons and Boolean
 * predicates. The current subset supports Boolean, INTEGER, TEXT/VARCHAR, and
 * NULL parameters; IS [NOT] NULL, SELECT *, and other value types return
 * UNSUPPORTED. */
int32_t prdb_sdk_optimizer_encode_select_with_parameters_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity, uint64_t *request_len);

/* Encode a parsed UPDATE AST as a bounded optimizer request. UPDATE requires
 * WHERE and supports parameter or NULL assignments. */
int32_t prdb_sdk_optimizer_encode_update_v1(
    const prdb_sdk_update_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity, uint64_t *request_len);

/* Encode the alias-aware unfiltered INNER JOIN AST as optimizer request
 * statement 4, preserving projection sides, composite keys, and LIMIT. */
int32_t prdb_sdk_optimizer_encode_join_select_v1(
    const prdb_sdk_select_v1 *statement,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity, uint64_t *request_len);

/* Encode the filtered v20 INNER JOIN AST as statement 12. The request carries
 * the side-qualified WHERE column and bounded parameter index or NULL test. */
int32_t prdb_sdk_optimizer_encode_filtered_join_select_v1(
    const prdb_sdk_select_v1 *statement,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity, uint64_t *request_len);

/* Encode the bounded single-key COUNT(*) GROUP BY AST as statement 5. The
 * request subset rejects WHERE, HAVING, ORDER BY, and composite keys. */
int32_t prdb_sdk_optimizer_encode_group_count_v1(
    const prdb_sdk_select_v1 *statement,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity, uint64_t *request_len);

int32_t prdb_sdk_optimizer_encode_ordered_group_count_v1(
    const prdb_sdk_select_v1 *statement,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity, uint64_t *request_len);

/* Encode the bounded two-to-eight-key COUNT(*) v13 AST as statement 6.
 * WHERE, HAVING, and ORDER BY remain unsupported. */
int32_t prdb_sdk_optimizer_encode_composite_group_count_v1(
    const prdb_sdk_select_v1 *statement,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity, uint64_t *request_len);

/* Encode composite GROUP BY with one COUNT/MIN/MAX/SUM/AVG aggregate as
 * statement 13. WHERE, HAVING, and ORDER BY remain unsupported. */
int32_t prdb_sdk_optimizer_encode_composite_group_aggregate_v1(
    const prdb_sdk_select_v1 *statement,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity, uint64_t *request_len);

/* Encode filtered GROUP BY with one aggregate as statement 15. The v24
 * optimizer bridge may supply one key; v13 SQL parsing remains composite-only.
 * Parameters follow the v2 comparison encoding. HAVING and ORDER BY remain
 * outside this request subset. */
int32_t prdb_sdk_optimizer_encode_filtered_composite_group_aggregate_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity, uint64_t *request_len);

/* Encode filtered grouped aggregation with v15 ORDER BY as statement 16. */
int32_t prdb_sdk_optimizer_encode_ordered_filtered_composite_group_aggregate_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity, uint64_t *request_len);

int32_t prdb_sdk_optimizer_encode_ordered_composite_group_count_v1(
    const prdb_sdk_select_v1 *statement,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity, uint64_t *request_len);

/* Encode composite GROUP BY with an aggregate and v15 ORDER BY as
 * statement 14. HAVING remains outside this request subset. */
int32_t prdb_sdk_optimizer_encode_ordered_composite_group_aggregate_v1(
    const prdb_sdk_select_v1 *statement,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity, uint64_t *request_len);

int32_t prdb_sdk_optimizer_encode_group_aggregate_v1(
    const prdb_sdk_select_v1 *statement,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity, uint64_t *request_len);

int32_t prdb_sdk_optimizer_encode_ordered_group_aggregate_v1(
    const prdb_sdk_select_v1 *statement,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity, uint64_t *request_len);

int32_t prdb_sdk_optimizer_decode_response_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_response_v1 *out, uint32_t out_capacity);

/* Sends one already encoded structured SELECT v1 request. The callback uses
 * only the fixed optimizer submission operation. `out->plan` borrows from
 * `response`; caller storage must not overlap the output record. */
int32_t prdb_sdk_optimizer_submit_select_v1(
    const prdb_host_api_v1 *host,
    const uint8_t *request, uint64_t request_len,
    uint8_t *response, uint64_t response_capacity, uint64_t *response_len,
    prdb_sdk_optimizer_response_v1 *out, uint32_t out_capacity);

/* Submit one encoded UPDATE request on the operation-selected ABI v5 callback. */
int32_t prdb_sdk_optimizer_submit_update_v1(
    const prdb_host_api_v1 *host,
    const uint8_t *request, uint64_t request_len,
    uint8_t *response, uint64_t response_capacity, uint64_t *response_len,
    prdb_sdk_optimizer_response_v1 *out, uint32_t out_capacity);

/* Submit one encoded join SELECT request using the role-authorized v5
 * operation-selected optimizer callback. */
int32_t prdb_sdk_optimizer_submit_join_select_v1(
    const prdb_host_api_v1 *host,
    const uint8_t *request, uint64_t request_len,
    uint8_t *response, uint64_t response_capacity, uint64_t *response_len,
    prdb_sdk_optimizer_response_v1 *out, uint32_t out_capacity);

int32_t prdb_sdk_optimizer_submit_group_count_v1(
    const prdb_host_api_v1 *host,
    const uint8_t *request, uint64_t request_len,
    uint8_t *response, uint64_t response_capacity, uint64_t *response_len,
    prdb_sdk_optimizer_response_v1 *out, uint32_t out_capacity);



/* Encode a parsed SQL DELETE with its mandatory bounded WHERE tree as
 * optimizer request v1. Predicate parameters use the same supported scalar
 * subset as the parameterized SELECT encoder. */
int32_t prdb_sdk_optimizer_encode_delete_v1(
    const prdb_sdk_delete_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity, uint64_t *request_len);

/* Submit an encoded DELETE request on ABI v5's operation-aware optimizer
 * callback. The request uses only OptimizeDelete and grants no storage access. */
int32_t prdb_sdk_optimizer_submit_delete_v1(
    const prdb_host_api_v1 *host,
    const uint8_t *request, uint64_t request_len,
    uint8_t *response, uint64_t response_capacity, uint64_t *response_len,
    prdb_sdk_optimizer_response_v1 *out, uint32_t out_capacity);

#endif /* PRDB_SDK_OPTIMIZER_V1_H */
