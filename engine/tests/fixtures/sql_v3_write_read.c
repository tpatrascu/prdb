#include "prdb_sdk_sql_exec_v2.h"

#include <string.h>

const prdb_query_descriptor_v3_record prdb_query_descriptor_v3 = {
    sizeof(prdb_query_descriptor_v3_record), PRDB_QUERY_ABI_V3_VERSION,
    sizeof(prdb_host_api_v3), sizeof(prdb_invocation_v3)};

typedef struct result_context {
    const prdb_host_api_v3 *host;
    uint32_t expected_state;
    const uint8_t *expected_value;
    uint64_t expected_value_len;
    const uint8_t *marker;
    uint64_t marker_len;
    uint32_t row_count;
    uint32_t expected_ordinal;
    uint32_t failure_code;
} result_context;

static int32_t verify_selected_value(void *opaque, const prdb_sdk_row_v1 *row,
                                     const prdb_sdk_select_binding_v2 *binding) {
    result_context *context = (result_context *)opaque;
    prdb_sdk_cell_iterator_v1 cells = {0};
    prdb_sdk_value_v1 value = {0};

    (void)binding;
    if (context == NULL || row == NULL || row->cell_count != 1 ||
        prdb_sdk_row_cells_begin_v1(row, &cells, sizeof(cells)) !=
            PRDB_SDK_BATCH_OK ||
        prdb_sdk_cell_next_v1(&cells, &value, sizeof(value)) !=
            PRDB_SDK_BATCH_OK ||
        value.state != context->expected_state ||
        cells.cells_remaining != 0) {
        return 1;
    }
    if (value.state == PRDB_SDK_VALUE_STATE_VALUE &&
        (value.type_code != PRDB_SDK_VALUE_TYPE_INTEGER ||
         value.data_len != context->expected_value_len ||
         memcmp(value.data, context->expected_value,
                (size_t)context->expected_value_len) != 0)) {
        return 1;
    }
    ++context->row_count;
    if (context->marker != NULL) {
        return context->host->write_result(context->host->context,
                                           context->marker,
                                           context->marker_len);
    }
    return 0;
}

static int32_t verify_information_schema_name(
    void *opaque, const prdb_sdk_row_v1 *row,
    const prdb_sdk_select_binding_v2 *binding) {
    result_context *context = (result_context *)opaque;
    prdb_sdk_cell_iterator_v1 cells = {0};
    prdb_sdk_value_v1 value = {0};

    if (context == NULL || row == NULL || binding == NULL ||
        binding->projection_count != 1 ||
        binding->projection_ordinals[0] != context->expected_ordinal ||
        row->cell_count <= context->expected_ordinal ||
        prdb_sdk_row_cells_begin_v1(row, &cells, sizeof(cells)) !=
            PRDB_SDK_BATCH_OK) {
        if (context != NULL) context->failure_code = 1;
        return 1;
    }
    for (uint32_t index = 0; index <= context->expected_ordinal; ++index) {
        if (prdb_sdk_cell_next_v1(&cells, &value, sizeof(value)) !=
            PRDB_SDK_BATCH_OK) {
            context->failure_code = 2;
            return 1;
        }
    }
    if (
        value.state != PRDB_SDK_VALUE_STATE_VALUE ||
        value.type_code != PRDB_SDK_VALUE_TYPE_TEXT ||
        value.data_len != context->expected_value_len ||
        memcmp(value.data, context->expected_value,
               (size_t)context->expected_value_len) != 0) {
        context->failure_code = 3;
        return 1;
    }
    ++context->row_count;
    if (context->marker != NULL) {
        return context->host->write_result(context->host->context,
                                           context->marker,
                                           context->marker_len);
    }
    return 0;
}

static int32_t report_failure(const prdb_host_api_v3 *host,
                              const uint8_t *message, uint64_t length) {
    (void)host->write_result(host->context, message, length);
    return 1;
}

int32_t prdb_query_main_v3(const prdb_host_api_v3 *host,
                           const prdb_invocation_v3 *invocation) {
    static const uint8_t insert_sql[] = "INSERT INTO records VALUES (NULL)";
    static const uint8_t select_null_sql[] =
        "SELECT id FROM records WHERE id IS NULL";
    static const uint8_t select_schemata_sql[] =
        "SELECT schema_name FROM schemata WHERE schema_name = $1 LIMIT 1";
    static const uint8_t select_tables_sql[] =
        "SELECT table_name FROM tables WHERE table_schema = $1 LIMIT 1";
    static const uint8_t select_columns_sql[] =
        "SELECT column_name FROM columns WHERE table_schema = $1 LIMIT 1";
    static const uint8_t update_sql[] =
        "UPDATE records SET id = $1 WHERE id IS NULL";
    static const uint8_t select_value_sql[] =
        "SELECT id FROM records WHERE id = $1";
    static const uint8_t delete_sql[] = "DELETE FROM records WHERE id = $1";
    static const uint8_t schema[] = "sql_roundtrip";
    static const uint8_t information_schema[] = "information_schema";
    static const uint8_t first_information_schema_table[] = "columns";
    static const uint8_t first_information_schema_column[] = "table_schema";
    static const uint8_t integer_bytes[] = {0, 0, 0, 42};
    static const uint8_t inserted_marker[] = "insert-visible";
    static const uint8_t metadata_marker[] = "metadata-visible";
    static const uint8_t tables_marker[] = "metadata-tables-visible";
    static const uint8_t columns_marker[] = "metadata-columns-visible";
    static const uint8_t updated_marker[] = "update-visible";
    uint8_t planner_response[1024] = {0};
    uint8_t executor_response[1024] = {0};
    uint8_t batch_response[4096] = {0};
    uint8_t insert_request[256] = {0};
    uint8_t update_request[256] = {0};
    uint8_t inserted_handle[16] = {0};
    uint8_t update_targets[PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS * 16] = {0};
    prdb_sdk_sql_column_shape_v2 planner_columns[9] = {{0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[9] = {{0}};
    prdb_sdk_executor_insert_value_v1 insert_values[1] = {{0}};
    prdb_sdk_executor_update_patch_v1 update_patches[1] = {{0}};
    prdb_sdk_select_binding_v2 binding = {0};
    prdb_sdk_value_v1 cell_scratch[9] = {{0}};
    prdb_sdk_value_v1 parameter = {0};
    prdb_sdk_value_v1 metadata_parameter = {0};
    result_context results = {0};
    uint32_t updated_count = 0;
    uint32_t deleted_count = 0;
    int32_t status;

    if (host == NULL || invocation == NULL ||
        host->struct_size < sizeof(*host) ||
        host->abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->executor_request == NULL || host->planner_request == NULL ||
        host->write_result == NULL || host->statement_failure == NULL) {
        return 1;
    }
    results.host = host;
    parameter.struct_size = sizeof(parameter);
    parameter.abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
    parameter.state = PRDB_SDK_VALUE_STATE_VALUE;
    parameter.type_code = PRDB_SDK_VALUE_TYPE_INTEGER;
    parameter.data = integer_bytes;
    parameter.data_len = sizeof(integer_bytes);
    metadata_parameter.struct_size = sizeof(metadata_parameter);
    metadata_parameter.abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
    metadata_parameter.state = PRDB_SDK_VALUE_STATE_VALUE;
    metadata_parameter.type_code = PRDB_SDK_VALUE_TYPE_TEXT;
    metadata_parameter.data = information_schema;
    metadata_parameter.data_len = sizeof(information_schema) - 1;

    status = prdb_sdk_sql_execute_insert_v1(
        host, schema, sizeof(schema) - 1, insert_sql, sizeof(insert_sql) - 1,
        NULL, 0, planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), insert_request, sizeof(insert_request),
        planner_columns, 1, executor_columns, 1, insert_values, 1,
        inserted_handle);
    if (status != PRDB_SDK_SQL_EXEC_OK) {
        static const uint8_t planner_message[] = "fixture-error-insert-planner";
        static const uint8_t executor_message[] = "fixture-error-insert-executor";
        static const uint8_t shape_message[] = "fixture-error-insert-shape";
        static const uint8_t bind_message[] = "fixture-error-insert-bind";
        if (status == PRDB_SDK_SQL_EXEC_PLANNER_ERROR) {
            return report_failure(host, planner_message,
                                  sizeof(planner_message) - 1);
        }
        if (status == PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR) {
            return report_failure(host, executor_message,
                                  sizeof(executor_message) - 1);
        }
        if (status == PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH) {
            return report_failure(host, shape_message,
                                  sizeof(shape_message) - 1);
        }
        return report_failure(host, bind_message, sizeof(bind_message) - 1);
    }

    results.expected_state = PRDB_SDK_VALUE_STATE_NULL;
    results.marker = inserted_marker;
    results.marker_len = sizeof(inserted_marker) - 1;
    status = prdb_sdk_sql_execute_select_v2(
        host, schema, sizeof(schema) - 1, select_null_sql,
        sizeof(select_null_sql) - 1,
        NULL, 0, 16, sizeof(batch_response), planner_response,
        sizeof(planner_response), executor_response, sizeof(executor_response),
        batch_response, sizeof(batch_response), planner_columns, 1,
        executor_columns, 1, cell_scratch, 1, verify_selected_value, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 1) {
        static const uint8_t message[] = "fixture-error-null-select";
        return report_failure(host, message, sizeof(message) - 1);
    }

    results.expected_state = PRDB_SDK_VALUE_STATE_VALUE;
    results.expected_value = information_schema;
    results.expected_value_len = sizeof(information_schema) - 1;
    results.marker = metadata_marker;
    results.marker_len = sizeof(metadata_marker) - 1;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_schemata_sql, sizeof(select_schemata_sql) - 1,
        &metadata_parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 1, executor_columns, 1,
        cell_scratch, 1, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 1) {
        static const uint8_t message[] = "fixture-error-schemata-select";
        return report_failure(host, message, sizeof(message) - 1);
    }

    results.expected_value = first_information_schema_table;
    results.expected_value_len = sizeof(first_information_schema_table) - 1;
    results.expected_ordinal = 1;
    results.marker = tables_marker;
    results.marker_len = sizeof(tables_marker) - 1;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_tables_sql, sizeof(select_tables_sql) - 1,
        &metadata_parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK) {
        static const uint8_t planner_message[] = "fixture-error-tables-planner";
        static const uint8_t executor_message[] = "fixture-error-tables-executor";
        static const uint8_t shape_message[] = "fixture-error-tables-shape";
        static const uint8_t bind_message[] = "fixture-error-tables-bind";
        static const uint8_t evaluation_message[] = "fixture-error-tables-evaluation";
        static const uint8_t result_message[] = "fixture-error-tables-result";
        static const uint8_t request_message[] = "fixture-error-tables-request";
        static const uint8_t materialization_message[] =
            "fixture-error-tables-materialization";
        if (status == PRDB_SDK_SQL_EXEC_PLANNER_ERROR) {
            return report_failure(host, planner_message, sizeof(planner_message) - 1);
        }
        if (status == PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR) {
            return report_failure(host, executor_message, sizeof(executor_message) - 1);
        }
        if (status == PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH) {
            return report_failure(host, shape_message, sizeof(shape_message) - 1);
        }
        if (status == PRDB_SDK_SQL_EXEC_BIND_ERROR) {
            return report_failure(host, bind_message, sizeof(bind_message) - 1);
        }
        if (status == PRDB_SDK_SQL_EXEC_EVALUATION_ERROR) {
            return report_failure(host, evaluation_message, sizeof(evaluation_message) - 1);
        }
        if (status == PRDB_SDK_SQL_EXEC_RESULT_ERROR) {
            return report_failure(host, result_message, sizeof(result_message) - 1);
        }
        if (status == PRDB_SDK_SQL_EXEC_REQUEST_LIMIT) {
            return report_failure(host, request_message, sizeof(request_message) - 1);
        }
        if (status == PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT) {
            return report_failure(host, materialization_message,
                                  sizeof(materialization_message) - 1);
        }
        static const uint8_t message[] = "fixture-error-tables-status";
        return report_failure(host, message, sizeof(message) - 1);
    }
    if (results.row_count != 1) {
        static const uint8_t message[] = "fixture-error-tables-count";
        return report_failure(host, message, sizeof(message) - 1);
    }

    results.expected_value = first_information_schema_column;
    results.expected_value_len = sizeof(first_information_schema_column) - 1;
    results.expected_ordinal = 2;
    results.marker = columns_marker;
    results.marker_len = sizeof(columns_marker) - 1;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_columns_sql, sizeof(select_columns_sql) - 1,
        &metadata_parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 1) {
        static const uint8_t header_message[] = "fixture-error-columns-header";
        static const uint8_t cells_message[] = "fixture-error-columns-cells";
        static const uint8_t value_message[] = "fixture-error-columns-value";
        static const uint8_t status_message[] = "fixture-error-columns-status";
        static const uint8_t count_message[] = "fixture-error-columns-count";
        if (results.failure_code == 1) {
            return report_failure(host, header_message, sizeof(header_message) - 1);
        }
        if (results.failure_code == 2) {
            return report_failure(host, cells_message, sizeof(cells_message) - 1);
        }
        if (results.failure_code == 3) {
            return report_failure(host, value_message, sizeof(value_message) - 1);
        }
        if (status != PRDB_SDK_SQL_EXEC_OK) {
            return report_failure(host, status_message, sizeof(status_message) - 1);
        }
        return report_failure(host, count_message, sizeof(count_message) - 1);
    }

    status = prdb_sdk_sql_execute_update_v1(
        host, schema, sizeof(schema) - 1, update_sql, sizeof(update_sql) - 1,
        &parameter, 1, 16, sizeof(batch_response), planner_response,
        sizeof(planner_response), executor_response, sizeof(executor_response),
        batch_response, sizeof(batch_response), update_request,
        sizeof(update_request), planner_columns, 1, executor_columns, 1,
        &binding, sizeof(binding), cell_scratch, 1, update_targets,
        PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS, update_patches, 1,
        &updated_count);
    if (status != PRDB_SDK_SQL_EXEC_OK || updated_count != 1) {
        static const uint8_t message[] = "fixture-error-update";
        return report_failure(host, message, sizeof(message) - 1);
    }

    results.expected_state = PRDB_SDK_VALUE_STATE_VALUE;
    results.expected_value = integer_bytes;
    results.expected_value_len = sizeof(integer_bytes);
    results.marker = updated_marker;
    results.marker_len = sizeof(updated_marker) - 1;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, schema, sizeof(schema) - 1, select_value_sql,
        sizeof(select_value_sql) - 1, &parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 1, executor_columns, 1,
        cell_scratch, 1, verify_selected_value, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 1) {
        static const uint8_t message[] = "fixture-error-value-select";
        return report_failure(host, message, sizeof(message) - 1);
    }

    status = prdb_sdk_sql_execute_delete_v1(
        host, schema, sizeof(schema) - 1, delete_sql, sizeof(delete_sql) - 1,
        &parameter, 1, 16, sizeof(batch_response), planner_response,
        sizeof(planner_response), executor_response, sizeof(executor_response),
        batch_response, sizeof(batch_response), planner_columns, 1,
        executor_columns, 1, &binding, sizeof(binding), cell_scratch, 1,
        update_targets, PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS, &deleted_count);
    if (status != PRDB_SDK_SQL_EXEC_OK || deleted_count != 1) {
        static const uint8_t message[] = "fixture-error-delete";
        return report_failure(host, message, sizeof(message) - 1);
    }

    results.marker = NULL;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, schema, sizeof(schema) - 1, select_value_sql,
        sizeof(select_value_sql) - 1, &parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 1, executor_columns, 1,
        cell_scratch, 1, verify_selected_value, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 0) {
        static const uint8_t message[] = "fixture-error-final-select";
        return report_failure(host, message, sizeof(message) - 1);
    }
    return 0;
}
