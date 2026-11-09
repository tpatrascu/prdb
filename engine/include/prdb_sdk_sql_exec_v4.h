#ifndef PRDB_SDK_SQL_EXEC_V4_H
#define PRDB_SDK_SQL_EXEC_V4_H

#include "prdb_sdk_sql_exec_v2.h"
#include "prdb_sdk_sql_v4.h"
#include "prdb_sdk_sql_group_v1.h"

#define PRDB_SDK_SQL_EXEC_V4_MAX_GROUP_KEY_BYTES UINT64_C(1048576)

/* The group key borrows the bounded caller buffer and remains valid only
 * during this callback. The callback receives first-seen groups after the
 * authorized scan is closed; count is COUNT(*) over matching visible rows. */
typedef int32_t (*prdb_sdk_sql_group_count_row_v4_fn)(
    void *context, const prdb_sdk_value_v1 *group_key, uint64_t count,
    const prdb_sdk_sql_bound_column_v2 *group_by_column);

/* Executes one v4 grouped COUNT(*) SELECT. It retains no more than 128
 * matching group-key values and 1 MiB of key bytes. No group callback occurs
 * if scanning, filtering, or grouping fails. LIMIT applies to output groups. */
int32_t prdb_sdk_sql_execute_group_count_v4(
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
    uint8_t *group_key_bytes, uint64_t group_key_bytes_capacity,
    prdb_sdk_sql_group_count_row_v4_fn on_group, void *result_context);

#endif /* PRDB_SDK_SQL_EXEC_V4_H */
