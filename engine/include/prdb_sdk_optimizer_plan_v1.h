#ifndef PRDB_SDK_OPTIMIZER_PLAN_V1_H
#define PRDB_SDK_OPTIMIZER_PLAN_V1_H

#include "prdb_sdk_optimizer_v1.h"
#include "prdb_sdk_executor_v1.h"

#define PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION UINT32_C(1)
#define PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS UINT32_C(256)
#define PRDB_SDK_OPTIMIZER_PLAN_MAX_PROJECTIONS UINT32_C(128)
#define PRDB_SDK_OPTIMIZER_PLAN_MAX_NEXT_REQUESTS UINT32_C(125)

#define PRDB_SDK_OPTIMIZER_PLAN_OK INT32_C(0)
#define PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT INT32_C(1)
#define PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD INT32_C(2)
#define PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED INT32_C(3)
#define PRDB_SDK_OPTIMIZER_PLAN_UNAVAILABLE INT32_C(4)
#define PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH INT32_C(5)
#define PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR INT32_C(6)
#define PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL INT32_C(7)
#define PRDB_SDK_OPTIMIZER_PLAN_ROW_CALLBACK_ERROR INT32_C(8)
#define PRDB_SDK_OPTIMIZER_PLAN_CANCELLED INT32_C(9)
#define PRDB_SDK_OPTIMIZER_PLAN_REQUEST_LIMIT INT32_C(10)
#define PRDB_SDK_OPTIMIZER_PLAN_FILTER_ERROR INT32_C(11)

/* Plan names borrow the encoded payload. Column arrays are owned by this
 * output record and contain stable catalog IDs, never executor handles. */
typedef struct prdb_sdk_optimizer_plan_column_v1 {
    uint8_t column_id[16];
    uint32_t ordinal;
    uint8_t type_code;
    uint8_t type_parameters[4];
    uint8_t nullable;
} prdb_sdk_optimizer_plan_column_v1;

typedef struct prdb_sdk_optimizer_plan_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    const uint8_t *schema_name;
    uint32_t schema_name_len;
    uint8_t schema_id[16];
    uint64_t schema_version;
    const uint8_t *relation_name;
    uint32_t relation_name_len;
    uint8_t relation_id[16];
    uint64_t table_epoch;
    uint32_t scan_column_count;
    prdb_sdk_optimizer_plan_column_v1
        scan_columns[PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS];
    uint32_t projection_count;
    prdb_sdk_optimizer_plan_column_v1
        projections[PRDB_SDK_OPTIMIZER_PLAN_MAX_PROJECTIONS];
    uint32_t has_limit;
    uint32_t limit;
} prdb_sdk_optimizer_plan_v1;

/* A validated filter subtree borrows its canonical prefix bytes from the
 * plan payload. It is absent when the plan has no Filter node. */
typedef struct prdb_sdk_optimizer_filter_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    uint32_t present;
    const uint8_t *prefix;
    uint32_t prefix_len;
} prdb_sdk_optimizer_filter_v1;

typedef struct prdb_sdk_optimizer_scan_state_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    uint32_t projection_count;
    uint32_t has_limit;
    uint32_t limit;
    uint32_t delivered_rows;
    uint32_t done;
    prdb_sdk_optimizer_plan_column_v1
        projections[PRDB_SDK_OPTIMIZER_PLAN_MAX_PROJECTIONS];
} prdb_sdk_optimizer_scan_state_v1;

typedef int32_t (*prdb_sdk_optimizer_projected_row_v1_fn)(
    void *context, const prdb_sdk_optimizer_plan_column_v1 *projections,
    const prdb_sdk_value_v1 *values, uint32_t projection_count);

/* Independently decodes the canonical plan v1 payload for the initial
 * Scan -> Project -> optional Limit subset. Filter plans return UNSUPPORTED;
 * use decode_plan_with_filter_v1 for the complete pipeline. Names borrow the
 * payload; the output record must not overlap it. */
int32_t prdb_sdk_optimizer_decode_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_plan_v1 *out, uint32_t out_capacity);

/* Decode all plan-v1 pipelines, independently validating a Filter node's
 * bounded prefix tree, scanned column references, scalar compatibility, and
 * Boolean root. `filter_out` is written only after the whole plan succeeds. */
int32_t prdb_sdk_optimizer_decode_plan_with_filter_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_plan_v1 *out, uint32_t out_capacity,
    prdb_sdk_optimizer_filter_v1 *filter_out, uint32_t filter_out_capacity);

/* Re-resolve a validated plan's qualified relation through executor storage
 * and require its stable identity and scanned column definitions to match.
 * The response buffer remains caller-owned. No executor handle is returned. */
int32_t prdb_sdk_optimizer_validate_executor_relation_v1(
    const prdb_host_api_v2 *host, const uint8_t *plan_payload,
    uint64_t plan_payload_len, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len);

/* First validates plan metadata as above, then obtains a regular executor
 * relation handle. Relation/name outputs are valid only on success; column
 * shapes describe the full authorized relation and borrow response names. */
int32_t prdb_sdk_optimizer_acquire_plan_relation_v1(
    const prdb_host_api_v2 *host, const uint8_t *plan_payload,
    uint64_t plan_payload_len, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_capacity);

/* Acquire the checked relation capability and open its full scan. A zero
 * LIMIT returns success with scan_opened_out=0 and a zero cursor. Relation
 * names and column metadata borrow relation_response; cursor bytes are copied. */
int32_t prdb_sdk_optimizer_open_plan_scan_v1(
    const prdb_host_api_v2 *host, const uint8_t *plan_payload,
    uint64_t plan_payload_len, uint8_t *relation_response,
    uint64_t relation_response_capacity, uint64_t *relation_response_len,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint64_t *scan_response_len, prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_capacity,
    uint8_t cursor_out[16], uint32_t *scan_opened_out);

/* Initialize and consume the initial unfiltered scan pipeline. Each callback
 * receives projection-ordered values borrowing the current immutable batch. */
int32_t prdb_sdk_optimizer_scan_state_init_v1(
    const uint8_t *plan_payload, uint64_t plan_payload_len,
    prdb_sdk_optimizer_scan_state_v1 *state_out,
    uint32_t state_out_capacity);
int32_t prdb_sdk_optimizer_consume_scan_batch_v1(
    prdb_sdk_optimizer_scan_state_v1 *state,
    const prdb_sdk_batch_v1 *batch, uint32_t relation_column_count,
    prdb_sdk_value_v1 *source_cells, uint32_t source_cell_capacity,
    prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out);

/* Consume a batch using a fully validated plan. Filter predicates are
 * evaluated against the scanned row before projection; only TRUE rows emit. */
int32_t prdb_sdk_optimizer_consume_plan_batch_v1(
    const uint8_t *plan_payload, uint64_t plan_payload_len,
    prdb_sdk_optimizer_scan_state_v1 *state,
    const prdb_sdk_batch_v1 *batch, uint32_t relation_column_count,
    prdb_sdk_value_v1 *source_cells, uint32_t source_cell_capacity,
    prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out);

/* Executes a bounded scan: validate/acquire/open, poll
 * cancellation between pages, project values, apply LIMIT, and close an early
 * cursor. Filter evaluation errors fail the statement. */
int32_t prdb_sdk_optimizer_execute_unfiltered_plan_v1(
    const prdb_host_api_v2 *host, const uint8_t *plan_payload,
    uint64_t plan_payload_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *relation_response,
    uint64_t relation_response_capacity, uint8_t *scan_response,
    uint64_t scan_response_capacity, uint8_t *batch_response,
    uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v2 *relation_columns,
    uint32_t relation_column_capacity, prdb_sdk_value_v1 *source_cells,
    uint32_t source_cell_capacity, prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out);

/* Submit an already parsed explicit-projection SELECT AST to the isolated
 * optimizer and execute its returned unfiltered plan through the executor.
 * ENCODE_UNSUPPORTED leaves the statement unlatchable so callers can use the
 * ordinary SQL path; execution errors latch statement failure through ABI v3. */
int32_t prdb_sdk_optimizer_execute_select_ast_v1(
    const prdb_host_api_v4 *host, const prdb_sdk_select_v2 *statement,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v2 *relation_columns,
    uint32_t relation_column_capacity, prdb_sdk_value_v1 *source_cells,
    uint32_t source_cell_capacity, prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out);

/* Parameterized counterpart that submits and validates supported WHERE
 * predicates before executing a filtered plan. */
int32_t prdb_sdk_optimizer_execute_select_ast_with_parameters_v1(
    const prdb_host_api_v4 *host, const prdb_sdk_select_v2 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v2 *relation_columns,
    uint32_t relation_column_capacity, prdb_sdk_value_v1 *source_cells,
    uint32_t source_cell_capacity, prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out);

/* Execute the supported explicit-projection SELECT subset and emit one SQLR
 * v1 record per row through the host's provisional result callback. */
int32_t prdb_sdk_optimizer_execute_select_ast_to_result_v1(
    const prdb_host_api_v4 *host, const prdb_sdk_select_v2 *statement,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v2 *relation_columns,
    uint32_t relation_column_capacity, prdb_sdk_value_v1 *source_cells,
    uint32_t source_cell_capacity, prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity, uint8_t *result_record_buffer,
    uint64_t result_record_capacity, uint32_t *delivered_rows_out);

int32_t prdb_sdk_optimizer_execute_select_ast_with_parameters_to_result_v1(
    const prdb_host_api_v4 *host, const prdb_sdk_select_v2 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v2 *relation_columns,
    uint32_t relation_column_capacity, prdb_sdk_value_v1 *source_cells,
    uint32_t source_cell_capacity, prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity, uint8_t *result_record_buffer,
    uint64_t result_record_capacity, uint32_t *delivered_rows_out);

/* Verify that a returned plan represents the submitted SQL relation,
 * projection names/order, and LIMIT using executor-authorized column names. */
int32_t prdb_sdk_optimizer_validate_select_plan_v1(
    const prdb_host_api_v2 *host, const prdb_sdk_select_v2 *statement,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len,
    uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len);

/* Validate a filtered returned plan against the submitted AST and supplied
 * parameters as well as executor-authorized relation metadata. */
int32_t prdb_sdk_optimizer_validate_select_plan_with_parameters_v1(
    const prdb_host_api_v2 *host, const prdb_sdk_select_v2 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len,
    uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len);

#endif /* PRDB_SDK_OPTIMIZER_PLAN_V1_H */
