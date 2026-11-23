#ifndef PRDB_SDK_SQL_EXEC_V25_H
#define PRDB_SDK_SQL_EXEC_V25_H

#include "prdb_sdk_sql_exec_v24.h"
#include "prdb_sdk_sql_spill_group_v1.h"
#include "prdb_query_abi_v6.h"

#define PRDB_SDK_SQL_EXEC_V25_UNSUPPORTED INT32_C(100)

/* ABI v6 spill runner for unordered COUNT(*) GROUP BY queries without WHERE
 * or HAVING. Other grouped shapes remain on the bounded v24 runner. */
int32_t prdb_sdk_sql_execute_group_count_spill_v25(
    const prdb_host_api_v6 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v2 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v2 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity,
    prdb_sdk_sql_group_row_v24_fn on_group, void *result_context);

#endif /* PRDB_SDK_SQL_EXEC_V25_H */
