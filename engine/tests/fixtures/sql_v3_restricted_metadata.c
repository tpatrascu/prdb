#include "prdb_sdk_sql_exec_v2.h"

#include <string.h>

const prdb_query_descriptor_v3_record prdb_query_descriptor_v3 = {
    sizeof(prdb_query_descriptor_v3_record), PRDB_QUERY_ABI_V3_VERSION,
    sizeof(prdb_host_api_v3), sizeof(prdb_invocation_v3)};

typedef struct metadata_result {
    const prdb_host_api_v3 *host;
    const uint8_t *expected;
    uint64_t expected_len;
    uint32_t expected_ordinal;
    uint32_t rows;
} metadata_result;

static int32_t verify_metadata_row(
    void *opaque, const prdb_sdk_row_v1 *row,
    const prdb_sdk_select_binding_v2 *binding) {
    metadata_result *result = (metadata_result *)opaque;
    prdb_sdk_cell_iterator_v1 cells = {0};
    prdb_sdk_value_v1 value = {0};

    if (result == NULL || row == NULL || binding == NULL ||
        binding->projection_count != 1 ||
        binding->projection_ordinals[0] != result->expected_ordinal ||
        row->cell_count <= result->expected_ordinal ||
        prdb_sdk_row_cells_begin_v1(row, &cells, sizeof(cells)) !=
            PRDB_SDK_BATCH_OK) {
        return 1;
    }
    for (uint32_t index = 0; index <= result->expected_ordinal; ++index) {
        if (prdb_sdk_cell_next_v1(&cells, &value, sizeof(value)) !=
            PRDB_SDK_BATCH_OK) {
            return 1;
        }
    }
    if (result->expected_len == 0 ||
        value.state != PRDB_SDK_VALUE_STATE_VALUE ||
        value.type_code != PRDB_SDK_VALUE_TYPE_TEXT ||
        value.data_len != result->expected_len ||
        memcmp(value.data, result->expected,
               (size_t)result->expected_len) != 0) {
        return 1;
    }
    ++result->rows;
    return result->host->write_result(result->host->context, value.data,
                                      value.data_len);
}

static int32_t run_select(
    const prdb_host_api_v3 *host, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint32_t column_count, uint32_t projection_ordinal,
    const uint8_t *expected, uint64_t expected_len) {
    uint8_t planner_response[1024] = {0};
    uint8_t executor_response[1024] = {0};
    uint8_t batch_response[4096] = {0};
    prdb_sdk_sql_column_shape_v2 planner_columns[9] = {{0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[9] = {{0}};
    prdb_sdk_value_v1 cell_scratch[9] = {{0}};
    metadata_result result = {0};
    static const uint8_t schema[] = "information_schema";

    result.host = host;
    result.expected = expected;
    result.expected_len = expected_len;
    result.expected_ordinal = projection_ordinal;
    int32_t status = prdb_sdk_sql_execute_select_v2(
        host, schema, sizeof(schema) - 1, sql, sql_len, parameters,
        parameter_count, 16, sizeof(batch_response), planner_response,
        sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, column_count, executor_columns, column_count,
        cell_scratch, column_count, verify_metadata_row, &result);
    if (status != PRDB_SDK_SQL_EXEC_OK) {
        return 1;
    }
    return result.rows == (expected_len == 0 ? 0u : 1u) ? 0 : 1;
}

int32_t prdb_query_main_v3(const prdb_host_api_v3 *host,
                           const prdb_invocation_v3 *invocation) {
    static const uint8_t schemata_sql[] =
        "SELECT schema_name FROM schemata WHERE schema_name = $1 LIMIT 1";
    static const uint8_t tables_sql[] =
        "SELECT table_name FROM tables WHERE table_schema = $1 AND table_name = $2 LIMIT 1";
    static const uint8_t columns_sql[] =
        "SELECT column_name FROM columns WHERE table_schema = $1 AND table_name = $2 LIMIT 1";
    static const uint8_t empty_schema[] = "empty_roundtrip";
    static const uint8_t hidden_schema[] = "sql_roundtrip";
    static const uint8_t expected_schema[] = "empty_roundtrip";
    static const uint8_t table_name[] = "records";
    prdb_sdk_value_v1 schema_parameter = {0};
    prdb_sdk_value_v1 table_parameter = {0};
    prdb_sdk_value_v1 parameters[2] = {{0}};

    if (host == NULL || invocation == NULL ||
        host->struct_size < sizeof(*host) ||
        host->abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->executor_request == NULL || host->planner_request == NULL ||
        host->write_result == NULL || host->statement_failure == NULL) {
        return 1;
    }
    schema_parameter.struct_size = sizeof(schema_parameter);
    schema_parameter.abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
    schema_parameter.state = PRDB_SDK_VALUE_STATE_VALUE;
    schema_parameter.type_code = PRDB_SDK_VALUE_TYPE_TEXT;
    schema_parameter.data = empty_schema;
    schema_parameter.data_len = sizeof(empty_schema) - 1;

    table_parameter.struct_size = sizeof(table_parameter);
    table_parameter.abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
    table_parameter.state = PRDB_SDK_VALUE_STATE_VALUE;
    table_parameter.type_code = PRDB_SDK_VALUE_TYPE_TEXT;
    table_parameter.data = table_name;
    table_parameter.data_len = sizeof(table_name) - 1;

    if (run_select(host, schemata_sql, sizeof(schemata_sql) - 1,
                   &schema_parameter, 1, 1, 0, expected_schema,
                   sizeof(expected_schema) - 1) != 0) {
        return 1;
    }

    schema_parameter.data = hidden_schema;
    schema_parameter.data_len = sizeof(hidden_schema) - 1;
    if (run_select(host, schemata_sql, sizeof(schemata_sql) - 1,
                   &schema_parameter, 1, 1, 0, NULL, 0) != 0) {
        return 1;
    }

    parameters[0] = schema_parameter;
    parameters[1] = table_parameter;
    if (run_select(host, tables_sql, sizeof(tables_sql) - 1, parameters, 2,
                   3, 1, NULL, 0) != 0) {
        return 1;
    }
    if (run_select(host, columns_sql, sizeof(columns_sql) - 1, parameters, 2,
                   9, 2, NULL, 0) != 0) {
        return 1;
    }
    return 0;
}
