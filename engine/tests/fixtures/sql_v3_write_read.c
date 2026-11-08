#include "prdb_sdk_sql_exec_v2.h"

#include <string.h>

const prdb_query_descriptor_v3_record prdb_query_descriptor_v3 = {
    sizeof(prdb_query_descriptor_v3_record), PRDB_QUERY_ABI_V3_VERSION,
    sizeof(prdb_host_api_v3), sizeof(prdb_invocation_v3)};

typedef struct result_context {
    const prdb_host_api_v3 *host;
    uint32_t row_count;
} result_context;

static int32_t verify_inserted_null(void *opaque,
                                    const prdb_sdk_row_v1 *row,
                                    const prdb_sdk_select_binding_v2 *binding) {
    static const uint8_t result[] = "read-after-insert";
    result_context *context = (result_context *)opaque;
    prdb_sdk_cell_iterator_v1 cells = {0};
    prdb_sdk_value_v1 value = {0};

    (void)binding;
    if (context == NULL || row == NULL || row->cell_count != 1 ||
        prdb_sdk_row_cells_begin_v1(row, &cells, sizeof(cells)) !=
            PRDB_SDK_BATCH_OK ||
        prdb_sdk_cell_next_v1(&cells, &value, sizeof(value)) !=
            PRDB_SDK_BATCH_OK ||
        value.state != PRDB_SDK_VALUE_STATE_NULL ||
        cells.cells_remaining != 0) {
        return 1;
    }
    ++context->row_count;
    return context->host->write_result(context->host->context, result,
                                       sizeof(result) - 1);
}

int32_t prdb_query_main_v3(const prdb_host_api_v3 *host,
                           const prdb_invocation_v3 *invocation) {
    static const uint8_t insert_sql[] = "INSERT INTO records VALUES (NULL)";
    static const uint8_t select_sql[] =
        "SELECT id FROM records WHERE id IS NULL";
    static const uint8_t schema[] = "sql_roundtrip";
    uint8_t planner_response[1024] = {0};
    uint8_t executor_response[1024] = {0};
    uint8_t batch_response[4096] = {0};
    uint8_t insert_request[256] = {0};
    uint8_t inserted_handle[16] = {0};
    prdb_sdk_sql_column_shape_v2 planner_columns[1] = {{0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[1] = {{0}};
    prdb_sdk_executor_insert_value_v1 insert_values[1] = {{0}};
    prdb_sdk_select_binding_v2 binding = {0};
    prdb_sdk_value_v1 cell_scratch[1] = {{0}};
    result_context results = {host, 0};
    int32_t status;

    if (host == NULL || invocation == NULL ||
        host->struct_size < sizeof(*host) ||
        host->abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->executor_request == NULL || host->planner_request == NULL ||
        host->write_result == NULL || host->statement_failure == NULL) {
        return 1;
    }

    status = prdb_sdk_sql_execute_insert_v1(
        host, schema, sizeof(schema) - 1, insert_sql, sizeof(insert_sql) - 1,
        NULL, 0, planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), insert_request, sizeof(insert_request),
        planner_columns, 1, executor_columns, 1, insert_values, 1,
        inserted_handle);
    if (status != PRDB_SDK_SQL_EXEC_OK) return status;

    status = prdb_sdk_sql_execute_select_v2(
        host, schema, sizeof(schema) - 1, select_sql, sizeof(select_sql) - 1,
        NULL, 0, 16, sizeof(batch_response), planner_response,
        sizeof(planner_response), executor_response, sizeof(executor_response),
        batch_response, sizeof(batch_response), planner_columns, 1,
        executor_columns, 1, cell_scratch, 1, verify_inserted_null, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 1) return 1;
    return 0;
}
