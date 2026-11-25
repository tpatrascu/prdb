#ifndef PRDB_SDK_EXECUTOR_V1_H
#define PRDB_SDK_EXECUTOR_V1_H

#include "prdb_query_abi_v2.h"
#include "prdb_query_abi_constants.h"
#include "prdb_sdk_batch_v1.h"
#include "prdb_sdk_planner_v1.h"

#define PRDB_SDK_EXECUTOR_ABI_V1_VERSION UINT32_C(1)
#define PRDB_SDK_EXECUTOR_OP_RESOLVE_RELATION PRDB_IPC_OP_RESOLVE_RELATION
#define PRDB_SDK_EXECUTOR_OP_RESOLVE_PLAN_RELATION PRDB_IPC_OP_RESOLVE_PLAN_RELATION
#define PRDB_SDK_EXECUTOR_OP_OPEN_SCAN PRDB_IPC_OP_OPEN_SCAN
#define PRDB_SDK_EXECUTOR_OP_NEXT_BATCH PRDB_IPC_OP_NEXT_BATCH
#define PRDB_SDK_EXECUTOR_OP_CLOSE_CURSOR PRDB_IPC_OP_CLOSE_CURSOR
#define PRDB_SDK_EXECUTOR_OP_STAGE_INSERT PRDB_IPC_OP_STAGE_INSERT
#define PRDB_SDK_EXECUTOR_OP_STAGE_INSERTS PRDB_IPC_OP_STAGE_INSERTS
#define PRDB_SDK_EXECUTOR_OP_STAGE_UPDATES PRDB_IPC_OP_STAGE_UPDATES
#define PRDB_SDK_EXECUTOR_OP_STAGE_DELETES PRDB_IPC_OP_STAGE_DELETES
#define PRDB_SDK_EXECUTOR_OP_STAGE_UPDATE PRDB_IPC_OP_STAGE_UPDATE
#define PRDB_SDK_EXECUTOR_OP_STAGE_DELETE PRDB_IPC_OP_STAGE_DELETE
#define PRDB_SDK_EXECUTOR_OP_PROBE_UNIQUE_INDEX PRDB_IPC_OP_PROBE_UNIQUE_INDEX
#define PRDB_SDK_EXECUTOR_OP_SCAN_INDEX_RANGE PRDB_IPC_OP_SCAN_INDEX_RANGE
#define PRDB_SDK_EXECUTOR_OP_SCAN_INDEX_PREFIX PRDB_IPC_OP_SCAN_INDEX_PREFIX
#define PRDB_SDK_EXECUTOR_OP_SCAN_INDEX_RANGES PRDB_IPC_OP_SCAN_INDEX_RANGES
#define PRDB_SDK_EXECUTOR_OP_CHECK_CANCELLATION PRDB_IPC_OP_EXECUTOR_CHECK_CANCELLATION
#define PRDB_SDK_EXECUTOR_OP_GET_WORKLOAD_BUDGETS PRDB_QUERY_HOST_OP_GET_WORKLOAD_BUDGETS
#define PRDB_SDK_EXECUTOR_MAX_COLUMN_IDS UINT16_C(256)

typedef struct prdb_sdk_workload_budgets_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    uint32_t max_index_probe_candidates;
} prdb_sdk_workload_budgets_v1;

/* Reads a trusted, invocation-pinned workload budget from the executor host.
 * The operation is answered locally by the trusted executor and is not sent
 * to storage IPC. */
int32_t prdb_sdk_executor_get_workload_budgets_v1(
    const prdb_host_api_v2 *host, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len,
    prdb_sdk_workload_budgets_v1 *out, uint32_t out_capacity);

#define PRDB_SDK_EXECUTOR_OK INT32_C(0)
#define PRDB_SDK_EXECUTOR_UNAVAILABLE INT32_C(1)
#define PRDB_SDK_EXECUTOR_INVALID_ARGUMENT INT32_C(2)
#define PRDB_SDK_EXECUTOR_INVALID_PAYLOAD INT32_C(3)
#define PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL INT32_C(4)

/* The returned description borrows the executor response. Its handle has
 * kind 6 and is unrelated to the planner handle. */
typedef struct prdb_sdk_executor_relation_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    uint8_t relation_id[16];
    uint8_t executor_handle[16];
    const uint8_t *name;
    uint32_t name_len;
    uint32_t column_count;
} prdb_sdk_executor_relation_v1;

#define PRDB_SDK_EXECUTOR_INSERT_VALUE UINT32_C(0)
#define PRDB_SDK_EXECUTOR_INSERT_NULL UINT32_C(1)
#define PRDB_SDK_EXECUTOR_INSERT_DEFAULT UINT32_C(2)

typedef struct prdb_sdk_executor_insert_value_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    uint32_t state;
    prdb_sdk_value_v1 value;
} prdb_sdk_executor_insert_value_v1;

typedef struct prdb_sdk_executor_update_patch_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    uint32_t ordinal;
    prdb_sdk_value_v1 value;
} prdb_sdk_executor_update_patch_v1;

typedef struct prdb_sdk_executor_index_probe_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    const prdb_sdk_value_v1 *values;
    uint8_t value_count;
} prdb_sdk_executor_index_probe_v1;

/* Resolve requires an explicit schema because InvocationV2 has no default
 * schema field. Request and response buffers are caller-owned and bounded. */
int32_t prdb_sdk_executor_resolve_relation_v1(
    const prdb_host_api_v2 *host,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *relation, uint32_t relation_len,
    uint8_t *response, uint64_t response_capacity, uint64_t *response_len,
    prdb_sdk_executor_relation_v1 *out, uint32_t out_capacity,
    prdb_sdk_sql_column_shape_v1 *columns, uint32_t column_capacity);

int32_t prdb_sdk_executor_open_full_scan_v1(
    const prdb_host_api_v2 *host, const uint8_t executor_handle[16],
    uint8_t *response, uint64_t response_capacity, uint64_t *response_len,
    uint8_t cursor_out[16]);

/* Executes a typed leading-key range request. A bound is either a non-NULL
 * VALUE or absent; result column IDs are packed 16-byte identifiers. The
 * decoded rows borrow response and intentionally contain no row handles. */
int32_t prdb_sdk_executor_scan_index_range_v1(
    const prdb_host_api_v2 *host, const uint8_t executor_handle[16],
    const uint8_t index_id[16], const prdb_sdk_value_v1 *lower_bound,
    uint8_t lower_inclusive, const prdb_sdk_value_v1 *upper_bound,
    uint8_t upper_inclusive, const uint8_t *column_ids,
    uint16_t column_count, uint8_t *request, uint64_t request_capacity,
    uint8_t *response, uint64_t response_capacity, uint64_t *response_len,
    prdb_sdk_index_range_v1 *range_out, uint32_t range_out_capacity);

/* Probes a composite index using two to eight non-NULL leading key values. */
int32_t prdb_sdk_executor_scan_index_prefix_v1(
    const prdb_host_api_v2 *host, const uint8_t executor_handle[16],
    const uint8_t index_id[16], const prdb_sdk_value_v1 *values,
    uint8_t value_count, const uint8_t *column_ids, uint16_t column_count,
    uint8_t *request, uint64_t request_capacity, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len,
    prdb_sdk_index_range_v1 *range_out, uint32_t range_out_capacity);

/* Runs 1..128 independent non-NULL point or leading-prefix probes in one
 * frame. Result groups remain in probe order and borrow the response bytes. */
int32_t prdb_sdk_executor_scan_index_ranges_v1(
    const prdb_host_api_v2 *host, const uint8_t executor_handle[16],
    const uint8_t index_id[16],
    const prdb_sdk_executor_index_probe_v1 *probes, uint16_t probe_count,
    const uint8_t *column_ids, uint16_t column_count,
    uint8_t *request, uint64_t request_capacity, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len,
    prdb_sdk_index_ranges_v1 *ranges_out, uint32_t ranges_out_capacity);

int32_t prdb_sdk_executor_next_batch_v1(
    const prdb_host_api_v2 *host, const uint8_t cursor[16],
    uint16_t max_candidates, uint32_t max_bytes,
    uint8_t *response, uint64_t response_capacity, uint64_t *response_len,
    prdb_sdk_batch_v1 *batch, uint32_t batch_out_capacity);

int32_t prdb_sdk_executor_close_cursor_v1(
    const prdb_host_api_v2 *host, const uint8_t cursor[16],
    uint8_t *response, uint64_t response_capacity, uint64_t *response_len);

/* Encodes the caller's complete schema-order insert shape into request before
 * submitting it. VALUE/NULL/DEFAULT use the public state constants below. */
int32_t prdb_sdk_executor_stage_insert_v1(
    const prdb_host_api_v2 *host, const uint8_t relation_handle[16],
    const prdb_sdk_executor_insert_value_v1 *values, uint16_t value_count,
    uint8_t *request, uint64_t request_capacity, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len,
    uint8_t row_handle_out[16]);

/* Patches accept VALUE/NULL only. Duplicate and out-of-range ordinals are
 * rejected before the host callback is invoked. */
int32_t prdb_sdk_executor_stage_update_v1(
    const prdb_host_api_v2 *host, const uint8_t row_handle[16],
    const prdb_sdk_executor_update_patch_v1 *patches, uint16_t patch_count,
    uint8_t *request, uint64_t request_capacity, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len);

/* Stages deletion of a connection-local row handle. Storage rechecks its
 * invocation, snapshot, grant, policy and row-version authority. */
int32_t prdb_sdk_executor_stage_delete_v1(
    const prdb_host_api_v2 *host, const uint8_t row_handle[16],
    uint8_t *response, uint64_t response_capacity, uint64_t *response_len);

/* Batch mutations use a version/count header followed by length-delimited
 * scalar mutation records. The caller chooses bounded chunks that fit the
 * 1 MiB IPC frame; the storage handler aborts the invocation on any member
 * failure. Insert values are row-major; update patches apply to each handle. */
int32_t prdb_sdk_executor_stage_inserts_v1(
    const prdb_host_api_v2 *host, const uint8_t relation_handle[16],
    const prdb_sdk_executor_insert_value_v1 *values, uint16_t row_count,
    uint16_t value_count, uint8_t *request, uint64_t request_capacity,
    uint8_t *response, uint64_t response_capacity, uint64_t *response_len,
    uint8_t *row_handles_out, uint64_t row_handles_capacity);
int32_t prdb_sdk_executor_stage_updates_v1(
    const prdb_host_api_v2 *host, const uint8_t *row_handles,
    uint16_t row_count, const prdb_sdk_executor_update_patch_v1 *patches,
    uint16_t patch_count, uint8_t *request, uint64_t request_capacity,
    uint8_t *response, uint64_t response_capacity, uint64_t *response_len);
int32_t prdb_sdk_executor_stage_deletes_v1(
    const prdb_host_api_v2 *host, const uint8_t *row_handles,
    uint16_t row_count, uint8_t *request, uint64_t request_capacity,
    uint8_t *response, uint64_t response_capacity, uint64_t *response_len);

/* Returns the invocation's current cancellation or deadline state in
 * cancelled_out (0 or 1). Query runners should poll between bounded work
 * batches and stop promptly when the result is 1. */
int32_t prdb_sdk_executor_check_cancelled_v1(
    const prdb_host_api_v2 *host, int32_t *cancelled_out);

#endif
