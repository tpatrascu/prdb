#ifndef PRDB_SDK_SQL_EXEC_V2_H
#define PRDB_SDK_SQL_EXEC_V2_H

#include "prdb_sdk_executor_v1.h"
#include "prdb_query_abi_v3.h"

#define PRDB_SDK_SQL_EXEC_V2_ABI_VERSION UINT32_C(1)
#define PRDB_SDK_SQL_EXEC_MAX_NEXT_REQUESTS UINT32_C(253)

#define PRDB_SDK_SQL_EXEC_OK INT32_C(0)
#define PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT INT32_C(1)
#define PRDB_SDK_SQL_EXEC_PLANNER_ERROR INT32_C(2)
#define PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR INT32_C(3)
#define PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH INT32_C(4)
#define PRDB_SDK_SQL_EXEC_BIND_ERROR INT32_C(5)
#define PRDB_SDK_SQL_EXEC_EVALUATION_ERROR INT32_C(6)
#define PRDB_SDK_SQL_EXEC_RESULT_ERROR INT32_C(7)
#define PRDB_SDK_SQL_EXEC_REQUEST_LIMIT INT32_C(8)

/* The row and all its cell bytes borrow batch_response and are valid only
 * until the callback returns. The binding maps the SQL projection to source
 * row ordinals; consumers read only those ordinals unless SELECT * was used.
 * Any nonzero API result fails the statement; query entry code must propagate
 * it as worker failure so the supervisor aborts the invocation. */
typedef int32_t (*prdb_sdk_sql_result_row_v2_fn)(
    void *context, const prdb_sdk_row_v1 *row,
    const prdb_sdk_select_binding_v2 *binding);

/* Executes a read-only SELECT as authorized full-shape pages. The caller
 * supplies three bounded response buffers and shape/evaluation scratch. The
 * schema must be the invocation's admitted schema for this unqualified SQL
 * relation. No native allocation is performed. */
int32_t prdb_sdk_sql_execute_select_v2(
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
    prdb_sdk_sql_result_row_v2_fn on_row, void *result_context);

#endif
