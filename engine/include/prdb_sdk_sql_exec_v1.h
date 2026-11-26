#ifndef PRDB_SDK_SQL_EXEC_V1_H
#define PRDB_SDK_SQL_EXEC_V1_H

#include "prdb_query_abi_v1.h"
#include "prdb_sdk_executor_v1.h"
#include "prdb_sdk_optimizer_v1.h"
#include "prdb_sdk_planner_v1.h"
#include "prdb_sdk_sql_dml_v1.h"
#include "prdb_sdk_sql_group_v1.h"
#include "prdb_sdk_sql_join_v1.h"
#include "prdb_sdk_sql_order_v1.h"
#include "prdb_sdk_sql_spill_group_v1.h"
#include "prdb_sdk_sql_v1.h"

#define PRDB_SDK_SQL_EXEC_ABI_V1_VERSION UINT32_C(1)
#define PRDB_SDK_SQL_EXEC_MAX_NEXT_REQUESTS UINT32_C(253)
#define PRDB_SDK_SQL_EXEC_SELECT_MAX_SCAN_PAGES UINT32_C(126)
#define PRDB_SDK_SQL_EXEC_MAX_MATERIALIZED_BYTES UINT64_C(1048576)
#define PRDB_SDK_SQL_EXEC_MAX_GROUP_KEY_BYTES UINT64_C(1048576)
#define PRDB_SDK_SQL_EXEC_JOIN_MAX_ROWS PRDB_SDK_SQL_JOIN_MAX_ROWS

#define PRDB_SDK_SQL_EXEC_OK INT32_C(0)
#define PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT INT32_C(1)
#define PRDB_SDK_SQL_EXEC_PLANNER_ERROR INT32_C(2)
#define PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR INT32_C(3)
#define PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH INT32_C(4)
#define PRDB_SDK_SQL_EXEC_BIND_ERROR INT32_C(5)
#define PRDB_SDK_SQL_EXEC_EVALUATION_ERROR INT32_C(6)
#define PRDB_SDK_SQL_EXEC_RESULT_ERROR INT32_C(7)
#define PRDB_SDK_SQL_EXEC_REQUEST_LIMIT INT32_C(8)
#define PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT INT32_C(9)
#define PRDB_SDK_SQL_EXEC_UNSUPPORTED INT32_C(100)
#define PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS UINT32_C(128)

typedef int32_t (*prdb_sdk_sql_result_row_v1_fn)(
    void *context, const prdb_sdk_row_v1 *row,
    const prdb_sdk_select_binding_v1 *binding);

typedef int32_t (*prdb_sdk_sql_group_row_v1_fn)(
    void *context, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v1 *projection_binding);

typedef struct prdb_sdk_sql_join_materialized_row_v1 {
    prdb_sdk_value_v1 join_keys[PRDB_SDK_SQL_MAX_JOIN_KEYS];
    prdb_sdk_value_v1 projections[PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS];
} prdb_sdk_sql_join_materialized_row_v1;

typedef int32_t (*prdb_sdk_sql_join_result_row_v1_fn)(
    void *context, const prdb_sdk_value_v1 *projected_values,
    uint32_t projection_count, const prdb_sdk_select_binding_v1 *binding);

/* Row SELECT */
int32_t prdb_sdk_sql_execute_select_v1(
    const prdb_host_api_v1 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, prdb_sdk_sql_result_row_v1_fn on_row,
    void *result_context);

int32_t prdb_sdk_sql_execute_select_typed_v1(
    const prdb_host_api_v1 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters,
    const prdb_sdk_sql_parameter_metadata_v1 *parameter_metadata,
    uint32_t parameter_count, uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, prdb_sdk_sql_result_row_v1_fn on_row,
    void *result_context);

int32_t prdb_sdk_sql_execute_select_typed_ordered_v1(
    const prdb_host_api_v1 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters,
    const prdb_sdk_sql_parameter_metadata_v1 *parameter_metadata,
    uint32_t parameter_count, uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *materialized_rows,
    uint64_t materialized_rows_capacity,
    prdb_sdk_sql_result_row_v1_fn on_row, void *result_context);


int32_t prdb_sdk_sql_execute_select_ordered_v1(
    const prdb_host_api_v1 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *materialized_rows,
    uint64_t materialized_rows_capacity,
    prdb_sdk_sql_result_row_v1_fn on_row, void *result_context);

int32_t prdb_sdk_sql_execute_select_spill_v1(
    const prdb_host_api_v1 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const prdb_sdk_sql_parameter_metadata_v1 *parameter_metadata,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, prdb_sdk_sql_result_row_v1_fn on_row,
    void *result_context);

/* DML */
int32_t prdb_sdk_sql_execute_insert_v1(
    const prdb_host_api_v1 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *insert_request, uint64_t insert_request_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity,
    prdb_sdk_executor_insert_value_v1 *insert_values,
    uint32_t insert_value_capacity, uint8_t row_handle_out[16]);

int32_t prdb_sdk_sql_execute_insert_batch_v1(
    const prdb_host_api_v1 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *insert_request, uint64_t insert_request_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity,
    prdb_sdk_executor_insert_value_v1 *insert_values,
    uint32_t insert_value_capacity, uint32_t *inserted_count_out);

int32_t prdb_sdk_sql_execute_update_v1(
    const prdb_host_api_v1 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    uint8_t *update_request, uint64_t update_request_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_select_binding_v1 *binding,
    uint32_t binding_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *target_handles,
    uint32_t target_capacity, prdb_sdk_executor_update_patch_v1 *patches,
    uint32_t patch_capacity, uint32_t *updated_count_out);

int32_t prdb_sdk_sql_execute_delete_v1(
    const prdb_host_api_v1 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_select_binding_v1 *binding,
    uint32_t binding_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *target_handles,
    uint32_t target_capacity, uint32_t *deleted_count_out);

/* GROUP BY & Aggregates */
int32_t prdb_sdk_sql_execute_group_v1(
    const prdb_host_api_v1 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *materialized_bytes,
    uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_group_row_v1_fn on_group, void *result_context);

int32_t prdb_sdk_sql_execute_group_typed_v1(
    const prdb_host_api_v1 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters,
    const prdb_sdk_sql_parameter_metadata_v1 *parameter_metadata,
    uint32_t parameter_count, uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *materialized_bytes,
    uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_group_row_v1_fn on_group, void *result_context);

int32_t prdb_sdk_sql_execute_group_having_optimizer_v1(
    const prdb_host_api_v1 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters,
    const prdb_sdk_sql_parameter_metadata_v1 *parameter_metadata,
    uint32_t parameter_count, uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *optimizer_request, uint64_t optimizer_request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *materialized_bytes,
    uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_group_row_v1_fn on_group, void *result_context);

int32_t prdb_sdk_sql_execute_group_count_spill_v1(
    const prdb_host_api_v1 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, prdb_sdk_sql_group_row_v1_fn on_group,
    void *result_context);

int32_t prdb_sdk_sql_execute_group_count_optimizer_v1(
    const prdb_host_api_v1 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *optimizer_request, uint64_t optimizer_request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *group_key_bytes,
    uint64_t group_key_bytes_capacity, prdb_sdk_sql_group_row_v1_fn on_group,
    void *result_context);

int32_t prdb_sdk_sql_execute_group_aggregate_optimizer_v1(
    const prdb_host_api_v1 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *optimizer_request, uint64_t optimizer_request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *materialized_bytes,
    uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_group_row_v1_fn on_group, void *result_context);

int32_t prdb_sdk_sql_execute_group_count_ordered_optimizer_v1(
    const prdb_host_api_v1 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *optimizer_request, uint64_t optimizer_request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *materialized_bytes,
    uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_group_row_v1_fn on_group, void *result_context);

int32_t prdb_sdk_sql_execute_group_aggregate_ordered_optimizer_v1(
    const prdb_host_api_v1 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *optimizer_request, uint64_t optimizer_request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *materialized_bytes,
    uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_group_row_v1_fn on_group, void *result_context);

int32_t prdb_sdk_sql_execute_composite_count_optimizer_v1(
    const prdb_host_api_v1 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *optimizer_request, uint64_t optimizer_request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *materialized_bytes,
    uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_group_row_v1_fn on_group, void *result_context);

int32_t prdb_sdk_sql_execute_group_composite_aggregate_optimizer_v1(
    const prdb_host_api_v1 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *optimizer_request, uint64_t optimizer_request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *materialized_bytes,
    uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_group_row_v1_fn on_group, void *result_context);

int32_t prdb_sdk_sql_execute_filtered_group_composite_aggregate_optimizer_v1(
    const prdb_host_api_v1 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *optimizer_request, uint64_t optimizer_request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *materialized_bytes,
    uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_group_row_v1_fn on_group, void *result_context);

int32_t prdb_sdk_sql_execute_ordered_composite_count_optimizer_v1(
    const prdb_host_api_v1 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *optimizer_request, uint64_t optimizer_request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *materialized_bytes,
    uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_group_row_v1_fn on_group, void *result_context);

int32_t prdb_sdk_sql_execute_ordered_composite_aggregate_optimizer_v1(
    const prdb_host_api_v1 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *optimizer_request, uint64_t optimizer_request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *materialized_bytes,
    uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_group_row_v1_fn on_group, void *result_context);

/* INNER JOIN */
int32_t prdb_sdk_sql_execute_inner_join_v1(
    const prdb_host_api_v1 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *left_planner_response, uint64_t left_planner_response_capacity,
    uint8_t *right_planner_response, uint64_t right_planner_response_capacity,
    uint8_t *left_executor_response, uint64_t left_executor_response_capacity,
    uint8_t *right_executor_response, uint64_t right_executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v1 *left_planner_columns,
    uint32_t left_planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *right_planner_columns,
    uint32_t right_planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *left_executor_columns,
    uint32_t left_executor_column_capacity,
    prdb_sdk_sql_column_shape_v1 *right_executor_columns,
    uint32_t right_executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity,
    prdb_sdk_sql_join_materialized_row_v1 *left_rows,
    uint32_t left_row_capacity,
    prdb_sdk_sql_join_materialized_row_v1 *right_rows,
    uint32_t right_row_capacity, uint8_t *materialized_bytes,
    uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_join_result_row_v1_fn on_row, void *result_context);

int32_t prdb_sdk_sql_execute_inner_join_typed_v1(
    const prdb_host_api_v1 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters,
    const prdb_sdk_sql_parameter_metadata_v1 *metadata,
    uint32_t parameter_count, uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *left_planner_response, uint64_t left_planner_response_capacity,
    uint8_t *right_planner_response, uint64_t right_planner_response_capacity,
    uint8_t *left_executor_response, uint64_t left_executor_response_capacity,
    uint8_t *right_executor_response, uint64_t right_executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v1 *left_planner_columns,
    uint32_t left_planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *right_planner_columns,
    uint32_t right_planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *left_executor_columns,
    uint32_t left_executor_column_capacity,
    prdb_sdk_sql_column_shape_v1 *right_executor_columns,
    uint32_t right_executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity,
    prdb_sdk_sql_join_materialized_row_v1 *left_rows,
    uint32_t left_row_capacity,
    prdb_sdk_sql_join_materialized_row_v1 *right_rows,
    uint32_t right_row_capacity, uint8_t *materialized_bytes,
    uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_join_result_row_v1_fn on_row, void *result_context);

int32_t prdb_sdk_sql_execute_inner_join_typed_spill_v1(
    const prdb_host_api_v1 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters,
    const prdb_sdk_sql_parameter_metadata_v1 *metadata,
    uint32_t parameter_count, uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *left_planner_response, uint64_t left_planner_response_capacity,
    uint8_t *right_planner_response, uint64_t right_planner_response_capacity,
    uint8_t *left_executor_response, uint64_t left_executor_response_capacity,
    uint8_t *right_executor_response, uint64_t right_executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v1 *left_planner_columns,
    uint32_t left_planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *right_planner_columns,
    uint32_t right_planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *left_executor_columns,
    uint32_t left_executor_column_capacity,
    prdb_sdk_sql_column_shape_v1 *right_executor_columns,
    uint32_t right_executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity,
    prdb_sdk_sql_join_materialized_row_v1 *left_rows,
    uint32_t left_row_capacity,
    prdb_sdk_sql_join_materialized_row_v1 *right_rows,
    uint32_t right_row_capacity, uint8_t *materialized_bytes,
    uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_join_result_row_v1_fn on_row, void *result_context);

int32_t prdb_sdk_sql_execute_inner_join_optimizer_v1(
    const prdb_host_api_v1 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *optimizer_request, uint64_t optimizer_request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *left_planner_response, uint64_t left_planner_response_capacity,
    uint8_t *right_planner_response, uint64_t right_planner_response_capacity,
    uint8_t *left_executor_response, uint64_t left_executor_response_capacity,
    uint8_t *right_executor_response, uint64_t right_executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v1 *left_planner_columns,
    uint32_t left_planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *right_planner_columns,
    uint32_t right_planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *left_executor_columns,
    uint32_t left_executor_column_capacity,
    prdb_sdk_sql_column_shape_v1 *right_executor_columns,
    uint32_t right_executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity,
    prdb_sdk_sql_join_materialized_row_v1 *left_rows,
    uint32_t left_row_capacity,
    prdb_sdk_sql_join_materialized_row_v1 *right_rows,
    uint32_t right_row_capacity, uint8_t *materialized_bytes,
    uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_join_result_row_v1_fn on_row, void *result_context);

#endif /* PRDB_SDK_SQL_EXEC_V1_H */
