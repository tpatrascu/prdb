#include "prdb_sdk_sql_exec_v2.h"
#include "prdb_sdk_sql_exec_v5.h"
#include "prdb_sdk_sql_exec_v6.h"
#include "prdb_sdk_sql_exec_v7.h"
#include "prdb_sdk_sql_exec_v8.h"
#include "prdb_sdk_sql_exec_v10.h"
#include "prdb_sdk_sql_exec_v11.h"
#include "prdb_sdk_sql_exec_v12.h"
#include "prdb_sdk_sql_exec_v13.h"
#include "prdb_sdk_sql_exec_v15.h"
#include "prdb_sdk_sql_exec_v16.h"
#include "prdb_sdk_sql_result_v1.h"

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

typedef struct grouped_result_context {
    const prdb_host_api_v3 *host;
    const prdb_sdk_sql_column_shape_v2 *group_column;
    const prdb_sdk_sql_column_shape_v2 *aggregate_column;
    uint8_t *output;
    uint64_t output_capacity;
    uint64_t output_len;
    uint32_t row_count;
} grouped_result_context;

typedef struct joined_result_context {
    const prdb_host_api_v3 *host;
    const prdb_sdk_sql_column_shape_v2 *left_columns;
    const prdb_sdk_sql_column_shape_v2 *right_columns;
    uint8_t *output;
    uint64_t output_capacity;
    uint64_t output_len;
    uint32_t row_count;
} joined_result_context;

typedef struct composite_grouped_result_context {
    const prdb_host_api_v3 *host;
    const prdb_sdk_sql_column_shape_v2 *columns;
    uint32_t column_count;
    uint8_t *output;
    uint64_t output_capacity;
    uint64_t output_len;
    uint32_t row_count;
} composite_grouped_result_context;

static int32_t write_grouped_composite_count(
    void *opaque, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v13 *binding) {
    composite_grouped_result_context *context =
        (composite_grouped_result_context *)opaque;
    if (context == NULL || binding == NULL || aggregate_value != NULL ||
        binding->base.aggregate_kind !=
            PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR ||
        group_key_count != 2 || binding->group_key_count != 2 ||
        matching_row_count == 0) {
        return 1;
    }
    int32_t status = prdb_sdk_sql_write_group_composite_result_v1(
        context->host, binding, context->columns, context->column_count,
        group_keys, group_key_count, NULL, matching_row_count,
        context->output, context->output_capacity, &context->output_len);
    if (status != PRDB_SDK_SQL_RESULT_OK) return 1;
    ++context->row_count;
    return 0;
}

static int32_t write_grouped_composite_count_v15(
    void *opaque, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v15 *binding) {
    if (binding == NULL || binding->order_count != 3 ||
        binding->order_items[0].item.kind !=
            PRDB_SDK_SQL_V10_ORDER_GROUP_KEY ||
        binding->order_items[0].item.group_key_index != 1 ||
        binding->order_items[1].item.kind !=
            PRDB_SDK_SQL_V10_ORDER_AGGREGATE ||
        binding->order_items[2].item.kind !=
            PRDB_SDK_SQL_V10_ORDER_GROUP_KEY ||
        binding->order_items[2].item.group_key_index != 0) {
        return 1;
    }
    return write_grouped_composite_count(
        opaque, group_keys, group_key_count, aggregate_value,
        matching_row_count, &binding->base.base);
}

static int32_t write_joined_row(void *opaque,
                                const prdb_sdk_value_v1 *projected_values,
                                uint32_t projection_count,
                                const prdb_sdk_select_binding_v8 *binding) {
    joined_result_context *context = (joined_result_context *)opaque;
    if (context == NULL || binding == NULL || projection_count != 2 ||
        binding->projection_count != 2) {
        return 1;
    }
    int32_t status = prdb_sdk_sql_write_join_result_v1(
        context->host, binding, context->left_columns, 1,
        context->right_columns, 2, projected_values, projection_count,
        context->output, context->output_capacity, &context->output_len);
    if (status != PRDB_SDK_SQL_RESULT_OK) return 1;
    ++context->row_count;
    return 0;
}

static int32_t write_composite_joined_row(
    void *opaque, const prdb_sdk_value_v1 *projected_values,
    uint32_t projection_count, const prdb_sdk_select_binding_v16 *binding) {
    joined_result_context *context = (joined_result_context *)opaque;
    if (context == NULL || binding == NULL ||
        binding->join_key_count != 2 || projection_count != 4 ||
        binding->base.projection_count != 4) {
        return 1;
    }
    int32_t status = prdb_sdk_sql_write_join_result_v1(
        context->host, &binding->base, context->left_columns, 2,
        context->right_columns, 2, projected_values, projection_count,
        context->output, context->output_capacity, &context->output_len);
    if (status != PRDB_SDK_SQL_RESULT_OK) return 1;
    ++context->row_count;
    return 0;
}

static int32_t write_grouped_min(void *opaque,
                                 const prdb_sdk_value_v1 *group_key,
                                 const prdb_sdk_value_v1 *aggregate_value,
                                 uint64_t matching_row_count,
                                 const prdb_sdk_select_binding_v5 *binding) {
    grouped_result_context *context = (grouped_result_context *)opaque;
    if (context == NULL || binding == NULL ||
        binding->aggregate_kind != PRDB_SDK_SQL_V5_AGGREGATE_MIN ||
        matching_row_count != 1) {
        return 1;
    }
    int32_t status = prdb_sdk_sql_write_group_minmax_result_v1(
        context->host, context->group_column, group_key,
        context->aggregate_column, aggregate_value, binding->aggregate_kind,
        context->output, context->output_capacity, &context->output_len);
    if (status != PRDB_SDK_SQL_RESULT_OK) return 1;
    ++context->row_count;
    return 0;
}

static int32_t write_grouped_sum(void *opaque,
                                 const prdb_sdk_value_v1 *group_key,
                                 const prdb_sdk_value_v1 *aggregate_value,
                                 uint64_t matching_row_count,
                                 const prdb_sdk_select_binding_v6 *binding) {
    grouped_result_context *context = (grouped_result_context *)opaque;
    if (context == NULL || binding == NULL ||
        binding->aggregate_kind != PRDB_SDK_SQL_V6_AGGREGATE_SUM ||
        matching_row_count != 1) {
        return 1;
    }
    int32_t status = prdb_sdk_sql_write_group_sum_result_v1(
        context->host, context->group_column, group_key,
        context->aggregate_column, aggregate_value, context->output,
        context->output_capacity, &context->output_len);
    if (status != PRDB_SDK_SQL_RESULT_OK) return 1;
    ++context->row_count;
    return 0;
}

static int32_t write_grouped_avg(void *opaque,
                                 const prdb_sdk_value_v1 *group_key,
                                 const prdb_sdk_value_v1 *aggregate_value,
                                 uint64_t matching_row_count,
                                 const prdb_sdk_select_binding_v7 *binding) {
    grouped_result_context *context = (grouped_result_context *)opaque;
    if (context == NULL || binding == NULL ||
        binding->aggregate_kind != PRDB_SDK_SQL_V7_AGGREGATE_AVG ||
        matching_row_count != 1) {
        return 1;
    }
    int32_t status = prdb_sdk_sql_write_group_avg_result_v1(
        context->host, context->group_column, group_key,
        context->aggregate_column, aggregate_value, context->output,
        context->output_capacity, &context->output_len);
    if (status != PRDB_SDK_SQL_RESULT_OK) return 1;
    ++context->row_count;
    return 0;
}

static int32_t write_grouped_ordered_count(
    void *opaque, const prdb_sdk_value_v1 *group_key,
    const prdb_sdk_value_v1 *aggregate_value, uint64_t matching_row_count,
    const prdb_sdk_select_binding_v12 *binding) {
    grouped_result_context *context = (grouped_result_context *)opaque;
    if (context == NULL || binding == NULL || aggregate_value != NULL ||
        binding->base.base.base.base.aggregate_kind !=
            PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR ||
        binding->base.base.has_order_by != 1 ||
        binding->order_count != 2 ||
        binding->order_items[0].item.kind !=
            PRDB_SDK_SQL_V10_ORDER_AGGREGATE ||
        binding->order_items[0].item.direction !=
            PRDB_SDK_SQL_V10_ORDER_DESCENDING ||
        binding->order_items[0].item.null_order !=
            PRDB_SDK_SQL_V11_NULLS_LAST ||
        binding->order_items[1].item.kind !=
            PRDB_SDK_SQL_V10_ORDER_GROUP_KEY ||
        binding->order_items[1].item.direction !=
            PRDB_SDK_SQL_V10_ORDER_ASCENDING ||
        binding->order_items[1].item.null_order !=
            PRDB_SDK_SQL_V11_NULLS_LAST ||
        matching_row_count != 1) {
        return 1;
    }
    int32_t status = prdb_sdk_sql_write_group_count_result_v1(
        context->host, context->group_column, group_key, matching_row_count,
        context->output, context->output_capacity, &context->output_len);
    if (status != PRDB_SDK_SQL_RESULT_OK) return 1;
    ++context->row_count;
    return 0;
}

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
        (context->expected_value != NULL &&
         (value.data_len != context->expected_value_len ||
          memcmp(value.data, context->expected_value,
                 (size_t)context->expected_value_len) != 0))) {
        context->failure_code = 3;
        return 1;
    }
    ++context->row_count;
    if (context->marker != NULL) {
        return context->host->write_result(context->host->context,
                                           context->marker,
                                           context->marker_len);
    }
    return context->host->write_result(context->host->context, value.data,
                                       value.data_len);
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
    static const uint8_t select_records_columns_sql[] =
        "SELECT column_name FROM columns WHERE table_schema = $1 AND "
        "table_name = $2 LIMIT 1";
    static const uint8_t select_private_table_sql[] =
        "SELECT table_name FROM tables WHERE table_schema = $1 AND table_name = $2 LIMIT 1";
    static const uint8_t select_private_columns_sql[] =
        "SELECT column_name FROM columns WHERE table_schema = $1 AND table_name = $2 LIMIT 1";
    static const uint8_t update_sql[] =
        "UPDATE records SET id = $1 WHERE id IS NULL";
    static const uint8_t select_value_sql[] =
        "SELECT id FROM records WHERE id = $1";
    static const uint8_t select_grouped_min_sql[] =
        "SELECT id, MIN(id) FROM records GROUP BY id";
    static const uint8_t select_grouped_sum_sql[] =
        "SELECT id, SUM(id) FROM records GROUP BY id";
    static const uint8_t select_grouped_avg_sql[] =
        "SELECT id, AVG(id) FROM records GROUP BY id";
    static const uint8_t select_grouped_order_sql[] =
        "SELECT id, COUNT(*) FROM records GROUP BY id "
        "ORDER BY COUNT(*) DESC NULLS LAST, id ASC NULLS LAST LIMIT 1";
    static const uint8_t select_composite_group_sql[] =
        "SELECT left_key, right_key, COUNT(*) FROM grouped_records "
        "GROUP BY left_key, right_key LIMIT 2";
    static const uint8_t select_composite_order_sql[] =
        "SELECT left_key, right_key, COUNT(*) FROM grouped_records "
        "GROUP BY left_key, right_key HAVING COUNT(*) >= $1 "
        "ORDER BY right_key DESC NULLS LAST, COUNT(*) DESC, "
        "left_key ASC NULLS LAST LIMIT 2";
    static const uint8_t select_join_sql[] =
        "SELECT records.id, linked_records.id FROM records INNER JOIN "
        "linked_records ON records.id = linked_records.id LIMIT 1";
    static const uint8_t select_composite_join_sql[] =
        "SELECT grouped_records.left_key, grouped_records.right_key, "
        "linked_records.id, linked_records.match_key FROM grouped_records "
        "INNER JOIN linked_records ON grouped_records.left_key = "
        "linked_records.id AND grouped_records.right_key = "
        "linked_records.match_key LIMIT 2";
    static const uint8_t delete_sql[] = "DELETE FROM records WHERE id = $1";
    static const uint8_t schema[] = "sql_roundtrip";
    static const uint8_t empty_schema[] = "empty_roundtrip";
    static const uint8_t information_schema[] = "information_schema";
    static const uint8_t expected_user_column[] = "id";
    static const uint8_t records_table_name[] = "records";
    static const uint8_t private_table_name[] = "private_records";
    static const uint8_t missing_table_name[] = "missing_records";
    static const uint8_t hidden_schema_name[] = "hidden_roundtrip";
    static const uint8_t missing_schema_name[] = "missing_roundtrip";
    static const uint8_t first_information_schema_table[] = "columns";
    static const uint8_t first_information_schema_column[] = "table_schema";
    static const uint8_t integer_bytes[] = {0, 0, 0, 42};
    static const uint8_t inserted_marker[] = "insert-visible";
    static const uint8_t updated_marker[] = "update-visible";
    uint8_t planner_response[1024] = {0};
    uint8_t executor_response[1024] = {0};
    uint8_t batch_response[4096] = {0};
    uint8_t grouped_materialized[128] = {0};
    uint8_t grouped_result[256] = {0};
    uint8_t composite_grouped_result[256] = {0};
    uint8_t join_result[128] = {0};
    uint8_t insert_request[256] = {0};
    uint8_t update_request[256] = {0};
    uint8_t inserted_handle[16] = {0};
    uint8_t update_targets[PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS * 16] = {0};
    prdb_sdk_sql_column_shape_v2 planner_columns[9] = {{0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[9] = {{0}};
    uint8_t join_left_planner_response[1024] = {0};
    uint8_t join_right_planner_response[1024] = {0};
    uint8_t join_left_executor_response[1024] = {0};
    uint8_t join_right_executor_response[1024] = {0};
    prdb_sdk_sql_column_shape_v2 join_left_planner_columns[9] = {{0}};
    prdb_sdk_sql_column_shape_v2 join_right_planner_columns[9] = {{0}};
    prdb_sdk_sql_column_shape_v2 join_left_executor_columns[9] = {{0}};
    prdb_sdk_sql_column_shape_v2 join_right_executor_columns[9] = {{0}};
    static prdb_sdk_sql_join_materialized_row_v8 join_left_rows[128];
    static prdb_sdk_sql_join_materialized_row_v8 join_right_rows[128];
    static uint8_t join_materialized_bytes[
        PRDB_SDK_SQL_EXEC_V8_MAX_MATERIALIZED_BYTES];
    uint8_t composite_join_left_planner_response[1024] = {0};
    uint8_t composite_join_right_planner_response[1024] = {0};
    uint8_t composite_join_left_executor_response[1024] = {0};
    uint8_t composite_join_right_executor_response[1024] = {0};
    prdb_sdk_sql_column_shape_v2 composite_join_left_planner_columns[9] = {{0}};
    prdb_sdk_sql_column_shape_v2 composite_join_right_planner_columns[9] = {{0}};
    prdb_sdk_sql_column_shape_v2 composite_join_left_executor_columns[9] = {{0}};
    prdb_sdk_sql_column_shape_v2 composite_join_right_executor_columns[9] = {{0}};
    static prdb_sdk_sql_join_materialized_row_v16 composite_join_left_rows[128];
    static prdb_sdk_sql_join_materialized_row_v16 composite_join_right_rows[128];
    static uint8_t composite_join_materialized_bytes[
        PRDB_SDK_SQL_EXEC_V8_MAX_MATERIALIZED_BYTES];
    prdb_sdk_executor_insert_value_v1 insert_values[1] = {{0}};
    prdb_sdk_executor_update_patch_v1 update_patches[1] = {{0}};
    prdb_sdk_select_binding_v2 binding = {0};
    prdb_sdk_value_v1 cell_scratch[9] = {{0}};
    prdb_sdk_value_v1 parameter = {0};
    prdb_sdk_value_v1 metadata_parameter = {0};
    prdb_sdk_value_v1 records_table_parameter = {0};
    prdb_sdk_value_v1 records_metadata_parameters[2] = {{0}};
    prdb_sdk_value_v1 private_table_parameter = {0};
    prdb_sdk_value_v1 private_metadata_parameters[2] = {{0}};
    static const uint8_t count_parameter_bytes[8] = {0, 0, 0, 0, 0, 0, 0, 1};
    prdb_sdk_value_v1 count_parameter = {
        sizeof(count_parameter), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BIGINT,
        count_parameter_bytes, sizeof(count_parameter_bytes)};
    result_context results = {0};
    static const uint8_t id_column_name[] = "id";
    prdb_sdk_sql_column_shape_v2 grouped_column = {
        id_column_name, sizeof(id_column_name) - 1, 0,
        PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0};
    grouped_result_context grouped_results = {
        host, &grouped_column, &grouped_column, grouped_result,
        sizeof(grouped_result), 0, 0};
    composite_grouped_result_context composite_grouped_results = {
        host, executor_columns, 2, composite_grouped_result,
        sizeof(composite_grouped_result), 0, 0};
    joined_result_context joined_results = {
        host, join_left_executor_columns, join_right_executor_columns,
        join_result, sizeof(join_result), 0, 0};
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
    records_table_parameter.struct_size = sizeof(records_table_parameter);
    records_table_parameter.abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
    records_table_parameter.state = PRDB_SDK_VALUE_STATE_VALUE;
    records_table_parameter.type_code = PRDB_SDK_VALUE_TYPE_TEXT;
    records_table_parameter.data = records_table_name;
    records_table_parameter.data_len = sizeof(records_table_name) - 1;
    private_table_parameter.struct_size = sizeof(private_table_parameter);
    private_table_parameter.abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
    private_table_parameter.state = PRDB_SDK_VALUE_STATE_VALUE;
    private_table_parameter.type_code = PRDB_SDK_VALUE_TYPE_TEXT;
    private_table_parameter.data = private_table_name;
    private_table_parameter.data_len = sizeof(private_table_name) - 1;

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
    results.marker = NULL;
    results.marker_len = 0;
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
    results.marker = NULL;
    results.marker_len = 0;
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
    results.marker = NULL;
    results.marker_len = 0;
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

    metadata_parameter.data = schema;
    metadata_parameter.data_len = sizeof(schema) - 1;
    records_metadata_parameters[0] = metadata_parameter;
    records_metadata_parameters[1] = records_table_parameter;
    results.expected_value = schema;
    results.expected_value_len = sizeof(schema) - 1;
    results.expected_ordinal = 0;
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
        static const uint8_t message[] = "fixture-error-user-schema-select";
        return report_failure(host, message, sizeof(message) - 1);
    }

    results.expected_value = NULL;
    results.expected_value_len = 0;
    results.expected_ordinal = 1;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_tables_sql, sizeof(select_tables_sql) - 1,
        &metadata_parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 1) {
        static const uint8_t message[] = "fixture-error-user-table-select";
        return report_failure(host, message, sizeof(message) - 1);
    }

    results.expected_value = expected_user_column;
    results.expected_value_len = sizeof(expected_user_column) - 1;
    results.expected_ordinal = 2;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_records_columns_sql, sizeof(select_records_columns_sql) - 1,
        records_metadata_parameters, 2, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 1) {
        static const uint8_t message[] = "fixture-error-user-column-select";
        return report_failure(host, message, sizeof(message) - 1);
    }

    metadata_parameter.data = empty_schema;
    metadata_parameter.data_len = sizeof(empty_schema) - 1;
    results.expected_value = empty_schema;
    results.expected_value_len = sizeof(empty_schema) - 1;
    results.expected_ordinal = 0;
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
        static const uint8_t message[] = "fixture-error-empty-schema-select";
        return report_failure(host, message, sizeof(message) - 1);
    }

    results.expected_value = NULL;
    results.expected_value_len = 0;
    results.expected_ordinal = 1;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_tables_sql, sizeof(select_tables_sql) - 1,
        &metadata_parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 0) {
        static const uint8_t message[] = "fixture-error-empty-schema-tables";
        return report_failure(host, message, sizeof(message) - 1);
    }

    results.expected_ordinal = 2;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_columns_sql, sizeof(select_columns_sql) - 1,
        &metadata_parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 0) {
        static const uint8_t message[] = "fixture-error-empty-schema-columns";
        return report_failure(host, message, sizeof(message) - 1);
    }

    metadata_parameter.data = schema;
    metadata_parameter.data_len = sizeof(schema) - 1;
    private_metadata_parameters[0] = metadata_parameter;
    private_metadata_parameters[1] = private_table_parameter;
    results.expected_value = private_table_name;
    results.expected_value_len = sizeof(private_table_name) - 1;
    results.expected_ordinal = 1;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_private_table_sql, sizeof(select_private_table_sql) - 1,
        private_metadata_parameters, 2, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 0) {
        static const uint8_t message[] = "fixture-error-private-table-leaked";
        return report_failure(host, message, sizeof(message) - 1);
    }

    results.expected_ordinal = 2;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_private_columns_sql, sizeof(select_private_columns_sql) - 1,
        private_metadata_parameters, 2, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 0) {
        static const uint8_t message[] = "fixture-error-private-columns-leaked";
        return report_failure(host, message, sizeof(message) - 1);
    }

    private_table_parameter.data = missing_table_name;
    private_table_parameter.data_len = sizeof(missing_table_name) - 1;
    private_metadata_parameters[1] = private_table_parameter;
    results.expected_value = missing_table_name;
    results.expected_value_len = sizeof(missing_table_name) - 1;
    results.expected_ordinal = 1;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_private_table_sql, sizeof(select_private_table_sql) - 1,
        private_metadata_parameters, 2, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 0) {
        static const uint8_t message[] = "fixture-error-missing-table-result";
        return report_failure(host, message, sizeof(message) - 1);
    }

    results.expected_ordinal = 2;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_private_columns_sql, sizeof(select_private_columns_sql) - 1,
        private_metadata_parameters, 2, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 0) {
        static const uint8_t message[] = "fixture-error-missing-columns-result";
        return report_failure(host, message, sizeof(message) - 1);
    }

    metadata_parameter.data = hidden_schema_name;
    metadata_parameter.data_len = sizeof(hidden_schema_name) - 1;
    results.expected_value = NULL;
    results.expected_value_len = 0;
    results.expected_ordinal = 0;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_schemata_sql, sizeof(select_schemata_sql) - 1,
        &metadata_parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 1, executor_columns, 1,
        cell_scratch, 1, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 0) {
        static const uint8_t message[] = "fixture-error-hidden-schema-visible";
        return report_failure(host, message, sizeof(message) - 1);
    }

    results.expected_ordinal = 1;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_tables_sql, sizeof(select_tables_sql) - 1,
        &metadata_parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 0) {
        static const uint8_t message[] = "fixture-error-hidden-schema-tables-visible";
        return report_failure(host, message, sizeof(message) - 1);
    }

    results.expected_ordinal = 2;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_columns_sql, sizeof(select_columns_sql) - 1,
        &metadata_parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 0) {
        static const uint8_t message[] = "fixture-error-hidden-schema-columns-visible";
        return report_failure(host, message, sizeof(message) - 1);
    }

    metadata_parameter.data = missing_schema_name;
    metadata_parameter.data_len = sizeof(missing_schema_name) - 1;
    results.expected_ordinal = 0;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_schemata_sql, sizeof(select_schemata_sql) - 1,
        &metadata_parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 1, executor_columns, 1,
        cell_scratch, 1, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 0) {
        static const uint8_t message[] = "fixture-error-missing-schema-result";
        return report_failure(host, message, sizeof(message) - 1);
    }

    results.expected_ordinal = 1;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_tables_sql, sizeof(select_tables_sql) - 1,
        &metadata_parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 0) {
        static const uint8_t message[] = "fixture-error-missing-schema-tables-result";
        return report_failure(host, message, sizeof(message) - 1);
    }

    results.expected_ordinal = 2;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_columns_sql, sizeof(select_columns_sql) - 1,
        &metadata_parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 0) {
        static const uint8_t message[] = "fixture-error-missing-schema-columns-result";
        return report_failure(host, message, sizeof(message) - 1);
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

    status = prdb_sdk_sql_execute_group_v5(
        host, schema, sizeof(schema) - 1, select_grouped_min_sql,
        sizeof(select_grouped_min_sql) - 1, NULL, 0, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 1, executor_columns, 1,
        cell_scratch, 1, grouped_materialized, sizeof(grouped_materialized),
        write_grouped_min, &grouped_results);
    if (status != PRDB_SDK_SQL_EXEC_OK || grouped_results.row_count != 1) {
        static const uint8_t message[] = "fixture-error-grouped-min";
        return report_failure(host, message, sizeof(message) - 1);
    }

    composite_grouped_results.row_count = 0;
    composite_grouped_results.output_len = 0;
    status = prdb_sdk_sql_execute_group_v13(
        host, schema, sizeof(schema) - 1, select_composite_group_sql,
        sizeof(select_composite_group_sql) - 1, NULL, 0, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, grouped_materialized, sizeof(grouped_materialized),
        write_grouped_composite_count, &composite_grouped_results);
    if (status != PRDB_SDK_SQL_EXEC_OK ||
        composite_grouped_results.row_count != 2) {
        static const uint8_t message[] = "fixture-error-composite-group";
        return report_failure(host, message, sizeof(message) - 1);
    }

    composite_grouped_results.row_count = 0;
    composite_grouped_results.output_len = 0;
    status = prdb_sdk_sql_execute_group_v15(
        host, schema, sizeof(schema) - 1, select_composite_order_sql,
        sizeof(select_composite_order_sql) - 1, &count_parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, grouped_materialized, sizeof(grouped_materialized),
        write_grouped_composite_count_v15, &composite_grouped_results);
    if (status != PRDB_SDK_SQL_EXEC_OK ||
        composite_grouped_results.row_count != 2) {
        static const uint8_t message[] = "fixture-error-composite-order";
        return report_failure(host, message, sizeof(message) - 1);
    }

    grouped_results.row_count = 0;
    grouped_results.output_len = 0;
    status = prdb_sdk_sql_execute_group_v6(
        host, schema, sizeof(schema) - 1, select_grouped_sum_sql,
        sizeof(select_grouped_sum_sql) - 1, NULL, 0, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 1, executor_columns, 1,
        cell_scratch, 1, grouped_materialized, sizeof(grouped_materialized),
        write_grouped_sum, &grouped_results);
    if (status != PRDB_SDK_SQL_EXEC_OK || grouped_results.row_count != 1) {
        static const uint8_t message[] = "fixture-error-grouped-sum";
        return report_failure(host, message, sizeof(message) - 1);
    }

    grouped_results.row_count = 0;
    grouped_results.output_len = 0;
    status = prdb_sdk_sql_execute_group_v7(
        host, schema, sizeof(schema) - 1, select_grouped_avg_sql,
        sizeof(select_grouped_avg_sql) - 1, NULL, 0, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 1, executor_columns, 1,
        cell_scratch, 1, grouped_materialized, sizeof(grouped_materialized),
        write_grouped_avg, &grouped_results);
    if (status != PRDB_SDK_SQL_EXEC_OK || grouped_results.row_count != 1) {
        static const uint8_t message[] = "fixture-error-grouped-avg";
        return report_failure(host, message, sizeof(message) - 1);
    }

    grouped_results.row_count = 0;
    grouped_results.output_len = 0;
    status = prdb_sdk_sql_execute_group_v12(
        host, schema, sizeof(schema) - 1, select_grouped_order_sql,
        sizeof(select_grouped_order_sql) - 1, NULL, 0, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 1, executor_columns, 1,
        cell_scratch, 1, grouped_materialized, sizeof(grouped_materialized),
        write_grouped_ordered_count, &grouped_results);
    if (status != PRDB_SDK_SQL_EXEC_OK || grouped_results.row_count != 1) {
        static const uint8_t message[] = "fixture-error-grouped-order";
        return report_failure(host, message, sizeof(message) - 1);
    }

    status = prdb_sdk_sql_execute_inner_join_v8(
        host, schema, sizeof(schema) - 1, select_join_sql,
        sizeof(select_join_sql) - 1, 16, sizeof(batch_response),
        join_left_planner_response, sizeof(join_left_planner_response),
        join_right_planner_response, sizeof(join_right_planner_response),
        join_left_executor_response, sizeof(join_left_executor_response),
        join_right_executor_response, sizeof(join_right_executor_response),
        batch_response, sizeof(batch_response), join_left_planner_columns, 9,
        join_right_planner_columns, 9, join_left_executor_columns, 9,
        join_right_executor_columns, 9, cell_scratch, 9, join_left_rows, 128,
        join_right_rows, 128, join_materialized_bytes,
        sizeof(join_materialized_bytes), write_joined_row, &joined_results);
    if (status != PRDB_SDK_SQL_EXEC_OK || joined_results.row_count != 1) {
        static const uint8_t message[] = "fixture-error-inner-join";
        return report_failure(host, message, sizeof(message) - 1);
    }

    joined_results.row_count = 0;
    joined_results.output_len = 0;
    joined_results.left_columns = composite_join_left_executor_columns;
    joined_results.right_columns = composite_join_right_executor_columns;
    status = prdb_sdk_sql_execute_inner_join_v16(
        host, schema, sizeof(schema) - 1, select_composite_join_sql,
        sizeof(select_composite_join_sql) - 1, 16, sizeof(batch_response),
        composite_join_left_planner_response,
        sizeof(composite_join_left_planner_response),
        composite_join_right_planner_response,
        sizeof(composite_join_right_planner_response),
        composite_join_left_executor_response,
        sizeof(composite_join_left_executor_response),
        composite_join_right_executor_response,
        sizeof(composite_join_right_executor_response), batch_response,
        sizeof(batch_response), composite_join_left_planner_columns, 9,
        composite_join_right_planner_columns, 9,
        composite_join_left_executor_columns, 9,
        composite_join_right_executor_columns, 9, cell_scratch, 9,
        composite_join_left_rows, 128, composite_join_right_rows, 128,
        composite_join_materialized_bytes,
        sizeof(composite_join_materialized_bytes),
        write_composite_joined_row, &joined_results);
    if (status != PRDB_SDK_SQL_EXEC_OK || joined_results.row_count != 2) {
        static const uint8_t message[] = "fixture-error-composite-inner-join";
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
