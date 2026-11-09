#ifndef PRDB_SDK_SQL_EXEC_V3_H
#define PRDB_SDK_SQL_EXEC_V3_H

#include "prdb_sdk_sql_exec_v2.h"
#include "prdb_sdk_sql_v3.h"

#define PRDB_SDK_SQL_EXEC_V3_MAX_MATERIALIZED_BYTES UINT64_C(1048576)

/* Executes a v3 SELECT. Without ORDER BY it has v2 streaming behavior. With
 * ORDER BY it buffers at most 128 matching rows in caller-owned storage,
 * closes the scan, then emits rows in stable sort order and applies LIMIT.
 * materialized_rows must not overlap other inputs or scratch and is needed
 * only when ORDER BY is present. */
int32_t prdb_sdk_sql_execute_select_v3(
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
    uint8_t *materialized_rows, uint64_t materialized_rows_capacity,
    prdb_sdk_sql_result_row_v2_fn on_row, void *result_context);

#endif /* PRDB_SDK_SQL_EXEC_V3_H */
