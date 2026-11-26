#include "prdb_sdk_sql_exec_v1.h"
#include "prdb_sdk_optimizer_plan_v1.h"

#include <string.h>

const prdb_query_descriptor_v1_record prdb_query_descriptor_v1 = {
    sizeof(prdb_query_descriptor_v1_record), PRDB_QUERY_ABI_VERSION,
    sizeof(prdb_host_api_v1), sizeof(prdb_invocation_v1)};

typedef struct optimized_metadata_result {
    const prdb_host_api_v1 *host;
    const uint8_t *expected;
    uint64_t expected_len;
    uint32_t rows;
} optimized_metadata_result;

static int32_t write_optimized_metadata_row(
    void *opaque, const prdb_sdk_optimizer_plan_column_v1 *projections,
    const prdb_sdk_value_v1 *values, uint32_t projection_count) {
    optimized_metadata_result *result = (optimized_metadata_result *)opaque;
    if (result == NULL || projections == NULL || values == NULL ||
        projection_count != 1 ||
        projections[0].type_code != PRDB_SDK_VALUE_TYPE_TEXT ||
        values[0].state != PRDB_SDK_VALUE_STATE_VALUE ||
        values[0].type_code != PRDB_SDK_VALUE_TYPE_TEXT ||
        values[0].data == NULL || values[0].data_len != result->expected_len ||
        memcmp(values[0].data, result->expected,
               (size_t)result->expected_len) != 0) {
        return 1;
    }
    ++result->rows;
    return result->host->write_result(result->host->context,
                                         values[0].data, values[0].data_len);
}

int32_t prdb_query_main_v1(const prdb_host_api_v1 *host,
                           const prdb_invocation_v1 *invocation) {
    static const uint8_t schema[] = "information_schema";
    static const uint8_t sql[] =
        "SELECT schema_name FROM schemata WHERE schema_name = $1 LIMIT 1";
    static const uint8_t expected[] = "empty_roundtrip";
    static const uint8_t parameter_bytes[] = "empty_roundtrip";
    prdb_sdk_select_v1 statement = {0};
    prdb_sdk_value_v1 parameter = {0};
    uint8_t request[4096] = {0};
    uint8_t optimizer_response[4096] = {0};
    uint8_t relation_response[4096] = {0};
    uint8_t scan_response[4096] = {0};
    uint8_t batch_response[4096] = {0};
    prdb_sdk_executor_relation_v1 relation = {0};
    prdb_sdk_sql_column_shape_v1 columns[16] = {{0}};
    prdb_sdk_value_v1 source_cells[16] = {{0}};
    prdb_sdk_value_v1 projected_values[16] = {{0}};
    optimized_metadata_result result = {
        host, expected, sizeof(expected) - 1, 0};
    uint32_t delivered_rows = 0;
    uint32_t parse_error_offset = 0;

    if (host == NULL || invocation == NULL ||
        host->struct_size < sizeof(*host) ||
        host->abi_version != PRDB_QUERY_ABI_VERSION ||
        host->optimizer_request == NULL || host->write_result == NULL ||
        prdb_sdk_sql_parse_select_v1(sql, sizeof(sql) - 1, &statement,
                                     sizeof(statement),
                                     &parse_error_offset) != PRDB_SDK_SQL_OK) {
        return 1;
    }
    parameter.struct_size = sizeof(parameter);
    parameter.abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
    parameter.state = PRDB_SDK_VALUE_STATE_VALUE;
    parameter.type_code = PRDB_SDK_VALUE_TYPE_TEXT;
    parameter.data = parameter_bytes;
    parameter.data_len = sizeof(parameter_bytes) - 1;

    int32_t status = prdb_sdk_optimizer_execute_select_unified_cb_v1(
        host, &statement, &parameter, 1, schema, sizeof(schema) - 1,
        request, sizeof(request), optimizer_response,
        sizeof(optimizer_response), relation_response,
        sizeof(relation_response), scan_response, sizeof(scan_response),
        batch_response, sizeof(batch_response), &relation, sizeof(relation),
        columns, 16, source_cells, 16, projected_values, 16,
        write_optimized_metadata_row, &result, &delivered_rows);
    return status == PRDB_SDK_OPTIMIZER_PLAN_OK && delivered_rows == 1 &&
                   result.rows == 1
               ? 0
               : 1;
}
