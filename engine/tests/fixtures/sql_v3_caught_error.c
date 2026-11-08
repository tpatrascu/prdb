#include "prdb_sdk_sql_exec_v2.h"

#include <string.h>

const prdb_query_descriptor_v3_record prdb_query_descriptor_v3 = {
    sizeof(prdb_query_descriptor_v3_record), PRDB_QUERY_ABI_V3_VERSION,
    sizeof(prdb_host_api_v3), sizeof(prdb_invocation_v3)};

static int32_t discard_row(void *context, const prdb_sdk_row_v1 *row,
                           const prdb_sdk_select_binding_v2 *binding) {
    (void)context;
    (void)row;
    (void)binding;
    return 0;
}

int32_t prdb_query_main_v3(const prdb_host_api_v3 *host,
                           const prdb_invocation_v3 *invocation) {
    static const uint8_t partial_output[] = "partial SQL output";
    static const uint8_t schema[] = "public";
    static const uint8_t invalid_sql[] = "DELETE FROM records";
    uint8_t planner_response[256];
    uint8_t executor_response[256];
    uint8_t batch_response[8];
    prdb_sdk_sql_column_shape_v2 planner_columns[1];
    prdb_sdk_sql_column_shape_v2 executor_columns[1];
    prdb_sdk_value_v1 cell_scratch[1];

    (void)invocation;
    if (host == NULL || host->struct_size < sizeof(*host) ||
        host->abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->write_result == NULL) {
        return 1;
    }
    if (host->write_result(host->context, partial_output,
                           sizeof(partial_output) - 1) != 0) {
        return 1;
    }

    /* Intentionally catch the SQL API error and return success. The v3 host
     * latch must still make the executor process fail after this entrypoint. */
    (void)prdb_sdk_sql_execute_select_v2(
        host, schema, sizeof(schema) - 1, invalid_sql, sizeof(invalid_sql) - 1,
        NULL, 0, 1, sizeof(batch_response), planner_response,
        sizeof(planner_response), executor_response, sizeof(executor_response),
        batch_response, sizeof(batch_response), planner_columns, 1,
        executor_columns, 1, cell_scratch, 1, discard_row, NULL);
    return 0;
}
