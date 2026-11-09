#ifndef PRDB_SDK_SQL_EXEC_V12_H
#define PRDB_SDK_SQL_EXEC_V12_H

#include "prdb_sdk_sql_exec_v11.h"
#include "prdb_sdk_sql_v12.h"

typedef int32_t (*prdb_sdk_sql_group_row_v12_fn)(
    void *context, const prdb_sdk_value_v1 *group_key,
    const prdb_sdk_value_v1 *aggregate_value, uint64_t matching_row_count,
    const prdb_sdk_select_binding_v12 *binding);

int32_t prdb_sdk_sql_execute_group_v12(
    const prdb_host_api_v3 *host,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v2 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v2 *executor_columns,
    uint32_t executor_column_capacity,
    prdb_sdk_value_v1 *cell_scratch, uint32_t cell_scratch_capacity,
    uint8_t *materialized_bytes, uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_group_row_v12_fn on_group, void *result_context);

#endif /* PRDB_SDK_SQL_EXEC_V12_H */
