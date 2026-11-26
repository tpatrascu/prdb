#ifndef PRDB_SDK_OPTIMIZER_PLAN_V1_H
#define PRDB_SDK_OPTIMIZER_PLAN_V1_H

#include "prdb_sdk_optimizer_v1.h"
#include "prdb_sdk_sql_v1.h"
#include "prdb_sdk_executor_v1.h"
#include "prdb_sdk_sql_dml_v1.h"
#include "prdb_query_abi_v1.h"

#define PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION UINT32_C(1)
#define PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS UINT32_C(256)
#define PRDB_SDK_OPTIMIZER_PLAN_MAX_PROJECTIONS UINT32_C(128)
#define PRDB_SDK_OPTIMIZER_PLAN_MAX_NEXT_REQUESTS UINT32_C(125)
#define PRDB_SDK_OPTIMIZER_PLAN_MAX_DELETE_TARGETS UINT32_C(128)
#define PRDB_SDK_OPTIMIZER_PLAN_MAX_UPDATE_ASSIGNMENTS UINT32_C(64)
#define PRDB_SDK_OPTIMIZER_PLAN_KIND_JOIN_SELECT UINT32_C(4)
#define PRDB_SDK_OPTIMIZER_PLAN_KIND_FILTERED_JOIN_SELECT UINT32_C(12)
#define PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_NESTED_LOOP_JOIN UINT32_C(17)
#define PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_PREFIX_NESTED_LOOP_JOIN UINT32_C(18)
#define PRDB_SDK_OPTIMIZER_PLAN_MAX_PARAMETER_INDEX UINT32_C(256)

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

typedef struct prdb_sdk_optimizer_join_relation_v1 {
    const uint8_t *relation_name;
    uint32_t relation_name_len;
    uint8_t relation_id[16];
    uint64_t table_epoch;
    uint32_t scan_column_count;
    prdb_sdk_optimizer_plan_column_v1
        scan_columns[PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS];
} prdb_sdk_optimizer_join_relation_v1;

typedef struct prdb_sdk_optimizer_join_projection_v1 {
    uint32_t relation_side;
    prdb_sdk_optimizer_plan_column_v1 column;
} prdb_sdk_optimizer_join_projection_v1;

typedef struct prdb_sdk_optimizer_join_key_v1 {
    prdb_sdk_optimizer_plan_column_v1 left_column;
    prdb_sdk_optimizer_plan_column_v1 right_column;
} prdb_sdk_optimizer_join_key_v1;

typedef struct prdb_sdk_optimizer_join_plan_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    const uint8_t *schema_name;
    uint32_t schema_name_len;
    uint8_t schema_id[16];
    uint64_t schema_version;
    prdb_sdk_optimizer_join_relation_v1 left_relation;
    prdb_sdk_optimizer_join_relation_v1 right_relation;
    uint32_t projection_count;
    prdb_sdk_optimizer_join_projection_v1
        projections[PRDB_SDK_OPTIMIZER_PLAN_MAX_PROJECTIONS];
    uint32_t join_key_count;
    prdb_sdk_optimizer_join_key_v1 join_keys[8];
    uint32_t has_limit;
    uint32_t limit;
} prdb_sdk_optimizer_join_plan_v1;

typedef struct prdb_sdk_optimizer_join_filter_v1 {
    uint32_t relation_side;
    prdb_sdk_optimizer_plan_column_v1 column;
    uint32_t expression_kind;
    uint32_t comparison;
    uint32_t parameter_index;
} prdb_sdk_optimizer_join_filter_v1;

typedef struct prdb_sdk_optimizer_index_join_plan_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_optimizer_join_plan_v1 join;
    uint32_t has_filter;
    prdb_sdk_optimizer_join_filter_v1 filter;
    uint32_t outer_side;
    uint32_t inner_side;
    uint8_t index_id[16];
    prdb_sdk_optimizer_plan_column_v1 key_column;
} prdb_sdk_optimizer_index_join_plan_v1;

typedef struct prdb_sdk_optimizer_index_prefix_join_plan_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_optimizer_join_plan_v1 join;
    uint32_t has_filter;
    prdb_sdk_optimizer_join_filter_v1 filter;
    uint32_t outer_side;
    uint32_t inner_side;
    uint8_t index_id[16];
    uint32_t prefix_column_count;
    prdb_sdk_optimizer_plan_column_v1 prefix_columns[8];
} prdb_sdk_optimizer_index_prefix_join_plan_v1;


typedef int32_t (*prdb_sdk_optimizer_projected_row_v1_fn)(
    void *context, const prdb_sdk_optimizer_plan_column_v1 *projections,
    const prdb_sdk_value_v1 *values, uint32_t projection_count);


/* --- Unified Composable Physical Plan Tree --- */

#define PRDB_SDK_OPTIMIZER_MAX_PHYSICAL_NODES UINT32_C(64)

typedef struct prdb_sdk_optimizer_physical_node_v1 {
    uint8_t op_tag; /* PRDB_OPTIMIZER_PHYSICAL_OP_* */
    uint8_t join_type;
    uint8_t allow_spill;
    uint8_t descending;
    uint16_t relation_index;
    uint16_t left_child;
    uint16_t right_child;
    const uint8_t *relation_name;
    uint32_t relation_name_len;
    uint8_t relation_id[16];
    uint64_t table_epoch;
    uint8_t index_id[16];
    uint32_t scan_column_count;
    prdb_sdk_optimizer_plan_column_v1
        scan_columns[PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS];
    uint32_t key_count;
    prdb_sdk_optimizer_plan_column_v1 key_columns[8];
    prdb_sdk_optimizer_plan_column_v1 right_key_columns[8];
    uint32_t has_limit;
    uint32_t limit;
    uint32_t has_predicate;
    const uint8_t *predicate_prefix;
    uint32_t predicate_prefix_len;
    uint32_t has_lower_bound;
    uint32_t lower_inclusive;
    const uint8_t *lower_bound_prefix;
    uint32_t lower_bound_prefix_len;
    uint32_t has_upper_bound;
    uint32_t upper_inclusive;
    const uint8_t *upper_bound_prefix;
    uint32_t upper_bound_prefix_len;
    const uint8_t *key_value_prefixes[8];
    uint32_t key_value_prefix_lens[8];
} prdb_sdk_optimizer_physical_node_v1;

typedef struct prdb_sdk_optimizer_unified_plan_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    const uint8_t *schema_name;
    uint32_t schema_name_len;
    uint8_t schema_id[16];
    uint64_t schema_version;
    uint32_t node_count;
    uint16_t root_index;
    prdb_sdk_optimizer_physical_node_v1
        nodes[PRDB_SDK_OPTIMIZER_MAX_PHYSICAL_NODES];
} prdb_sdk_optimizer_unified_plan_v1;

int32_t prdb_sdk_optimizer_decode_unified_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_unified_plan_v1 *out, uint32_t out_capacity);

/* Re-resolve every relation in the unified physical plan through executor
 * storage and verify column IDs, ordinals, scalar types, and nullability. */
int32_t prdb_sdk_optimizer_validate_unified_plan_v1(
    const prdb_host_api_v1 *host,
    const prdb_sdk_optimizer_unified_plan_v1 *plan,
    uint8_t *response, uint64_t response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out, uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *columns_buf, uint32_t columns_capacity);

/* Adaptive spill decision helper: returns 1 if the operator should spill
 * (either plan->nodes[idx].allow_spill is set or runtime rows/bytes exceed
 * the trusted workload budget). */
int32_t prdb_sdk_optimizer_should_spill_unified_node_v1(
    const prdb_sdk_optimizer_physical_node_v1 *node,
    const prdb_sdk_workload_budgets_v1 *budgets,
    uint64_t observed_rows, uint64_t observed_bytes);

/* Validate and execute a unified physical plan tree (single-table scan/index
 * pipelines, Filter, Project, Limit, EmptyResult) and emit provisional SQLR v1
 * rows through `host->write_result`. */
int32_t prdb_sdk_optimizer_execute_unified_plan_v1(
    const prdb_host_api_v1 *host,
    const prdb_sdk_optimizer_unified_plan_v1 *plan,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out, uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns, uint32_t relation_column_capacity,
    prdb_sdk_value_v1 *source_cells, uint32_t source_cell_capacity,
    prdb_sdk_value_v1 *projected_values, uint32_t projected_value_capacity,
    uint8_t *result_record_buffer, uint64_t result_record_capacity,
    uint32_t *delivered_rows_out);

/* Lower a parsed single-table `prdb_sdk_select_v1` AST + parameters into a
 * unified logical request, submit it to `prdb-optimizer`, validate the
 * returned unified physical plan, and execute it. */
int32_t prdb_sdk_optimizer_execute_select_unified_v1(
    const prdb_host_api_v1 *host, const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out, uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns, uint32_t relation_column_capacity,
    prdb_sdk_value_v1 *source_cells, uint32_t source_cell_capacity,
    prdb_sdk_value_v1 *projected_values, uint32_t projected_value_capacity,
    uint8_t *result_record_buffer, uint64_t result_record_capacity,
    uint32_t *delivered_rows_out);

/* Lower a parsed single-table `prdb_sdk_select_v1` (optionally ordered, with
 * optional parameters) into a unified logical request, submit it to
 * `prdb-optimizer`, validate the returned unified physical plan, and deliver
 * projected rows to `on_row`. */
int32_t prdb_sdk_optimizer_execute_select_unified_cb_v1(
    const prdb_host_api_v1 *host, const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out, uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns, uint32_t relation_column_capacity,
    prdb_sdk_value_v1 *source_cells, uint32_t source_cell_capacity,
    prdb_sdk_value_v1 *projected_values, uint32_t projected_value_capacity,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out);

/* Lower a parsed `prdb_sdk_update_v1` + parameters into a unified logical
 * request, submit it to `prdb-optimizer`, validate the returned plan, scan
 * matching row handles, and stage executor updates. */
int32_t prdb_sdk_optimizer_execute_update_unified_v1(
    const prdb_host_api_v1 *host, const prdb_sdk_update_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out, uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns, uint32_t relation_column_capacity,
    prdb_sdk_value_v1 *source_cells, uint32_t source_cell_capacity,
    uint8_t *target_row_handles, uint32_t target_row_handle_capacity,
    uint32_t *updated_rows_out);

/* Lower a parsed `prdb_sdk_delete_v1` + parameters into a unified logical
 * request, submit it to `prdb-optimizer`, validate the returned plan, scan
 * matching row handles, and stage executor deletes. */
int32_t prdb_sdk_optimizer_execute_delete_unified_v1(
    const prdb_host_api_v1 *host, const prdb_sdk_delete_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out, uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns, uint32_t relation_column_capacity,
    prdb_sdk_value_v1 *source_cells, uint32_t source_cell_capacity,
    uint8_t *target_row_handles, uint32_t target_row_handle_capacity,
    uint32_t *deleted_rows_out);

/* Lower a parsed `prdb_sdk_join_select_v1` + parameters into a unified
 * logical request, submit it to `prdb-optimizer`, decode the unified plan,
 * and re-validate both left and right relations via executor storage. */
int32_t prdb_sdk_optimizer_plan_join_unified_v1(
    const prdb_host_api_v1 *host, const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *left_relation_response, uint64_t left_relation_response_capacity,
    uint8_t *right_relation_response, uint64_t right_relation_response_capacity,
    prdb_sdk_executor_relation_v1 *left_relation_out,
    prdb_sdk_executor_relation_v1 *right_relation_out,
    prdb_sdk_sql_column_shape_v1 *left_columns, uint32_t left_column_capacity,
    prdb_sdk_sql_column_shape_v1 *right_columns, uint32_t right_column_capacity,
    prdb_sdk_optimizer_unified_plan_v1 *plan_out);

/* Lower a parsed grouped/aggregated `prdb_sdk_select_v1` + parameters into a
 * unified logical request, submit it to `prdb-optimizer`, decode the unified
 * plan, and re-validate its relation via executor storage. */
int32_t prdb_sdk_optimizer_plan_group_unified_v1(
    const prdb_host_api_v1 *host, const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    prdb_sdk_sql_column_shape_v1 *relation_columns, uint32_t relation_column_capacity,
    prdb_sdk_optimizer_unified_plan_v1 *plan_out);

#endif /* PRDB_SDK_OPTIMIZER_PLAN_V1_H */
