#include "prdb_sdk_sql_exec_v2.h"

#include <string.h>

static int32_t fail_statement(const prdb_host_api_v3 *host, int32_t status) {
    if (status != PRDB_SDK_SQL_EXEC_OK) {
        (void)host->statement_failure(host->context,
                                      PRDB_QUERY_STATEMENT_FAILURE_SQL);
    }
    return status;
}

static int same_relation_shape(const prdb_sdk_planner_relation_v1 *planned,
                               const prdb_sdk_executor_relation_v1 *resolved,
                               const prdb_sdk_sql_column_shape_v2 *planned_columns,
                               const prdb_sdk_sql_column_shape_v2 *resolved_columns) {
    if (memcmp(planned->relation_id, resolved->relation_id, 16) != 0 ||
        planned->column_count != resolved->column_count ||
        planned->name_len != resolved->name_len ||
        memcmp(planned->name, resolved->name, planned->name_len) != 0) return 0;
    for (uint32_t index = 0; index < planned->column_count; ++index) {
        const prdb_sdk_sql_column_shape_v2 *a = &planned_columns[index];
        const prdb_sdk_sql_column_shape_v2 *b = &resolved_columns[index];
        if (a->ordinal != b->ordinal || a->type_code != b->type_code ||
            a->decimal_precision != b->decimal_precision ||
            a->decimal_scale != b->decimal_scale || a->name_len != b->name_len ||
            memcmp(a->name, b->name, a->name_len) != 0) return 0;
    }
    return 1;
}

int32_t prdb_sdk_sql_execute_select_v2(
    const prdb_host_api_v3 *host_v3, const uint8_t *schema, uint32_t schema_len,
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
    prdb_sdk_sql_result_row_v2_fn on_row, void *result_context) {
    prdb_sdk_select_v2 statement = {0};
    prdb_sdk_planner_relation_v1 planned = {0};
    prdb_sdk_executor_relation_v1 resolved = {0};
    prdb_sdk_select_binding_v2 binding = {0};
    prdb_sdk_batch_v1 batch = {0};
    prdb_sdk_row_iterator_v1 rows = {0};
    prdb_sdk_row_v1 row = {0};
    uint8_t cursor[16] = {0};
    uint64_t planner_response_len = 0;
    uint64_t executor_response_len = 0;
    uint64_t batch_response_len = 0;
    uint32_t page_count = 0;
    uint32_t delivered = 0;
    int cursor_open = 0;
    int32_t status;
    int32_t final_status = PRDB_SDK_SQL_EXEC_OK;
    prdb_host_api_v2 base_host;
    const prdb_host_api_v2 *host;

    if (host_v3 == NULL || host_v3->struct_size < sizeof(*host_v3) ||
        host_v3->abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host_v3->statement_failure == NULL ||
        host_v3->executor_request == NULL || host_v3->planner_request == NULL) {
        return PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT;
    }
    base_host = (prdb_host_api_v2){
        sizeof(prdb_host_api_v2), PRDB_QUERY_ABI_V2_VERSION, host_v3->context,
        host_v3->executor_request, host_v3->planner_request,
        host_v3->write_result};
    host = &base_host;
    if (schema == NULL || sql == NULL || planner_response == NULL ||
        executor_response == NULL || batch_response == NULL || on_row == NULL ||
        planner_columns == NULL || executor_columns == NULL ||
        cell_scratch == NULL || planner_column_capacity == 0 ||
        executor_column_capacity == 0 || cell_scratch_capacity == 0 ||
        planner_response_capacity == 0 || executor_response_capacity == 0 ||
        batch_response_capacity < max_batch_bytes ||
        max_candidates == 0 || max_candidates > PRDB_SDK_BATCH_MAX_ROWS ||
        max_batch_bytes < 8 || max_batch_bytes > PRDB_SDK_BATCH_MAX_BYTES ||
        (parameter_count != 0 && parameters == NULL)) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT);
    }
    status = prdb_sdk_sql_parse_select_v2(sql, sql_len, &statement,
                                          sizeof(statement), NULL);
    if (status != PRDB_SDK_SQL_OK) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    }

    status = prdb_sdk_planner_describe_relation_v1(
        host, schema, schema_len, statement.sql + statement.relation.offset,
        statement.relation.length, planner_response, planner_response_capacity,
        &planner_response_len, &planned, sizeof(planned), planner_columns,
        planner_column_capacity);
    if (status != PRDB_SDK_PLANNER_REQUEST_OK) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_PLANNER_ERROR);
    }
    status = prdb_sdk_executor_resolve_relation_v1(
        host, schema, schema_len, statement.sql + statement.relation.offset,
        statement.relation.length, executor_response,
        executor_response_capacity, &executor_response_len, &resolved,
        sizeof(resolved), executor_columns, executor_column_capacity);
    if (status != PRDB_SDK_EXECUTOR_OK) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    }
    if (!same_relation_shape(&planned, &resolved, planner_columns,
                             executor_columns)) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH);
    }
    if (cell_scratch_capacity < resolved.column_count) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT);
    }
    status = prdb_sdk_sql_bind_select_v2(
        &statement, resolved.name, resolved.name_len, executor_columns,
        resolved.column_count, &binding, sizeof(binding));
    if (status != PRDB_SDK_SQL_BIND_OK) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    }
    for (uint32_t index = 0; index < statement.predicate_count; ++index) {
        const prdb_sdk_predicate_node_v2 *node = &statement.predicates[index];
        if (node->node_kind == PRDB_SDK_SQL_PREDICATE_COMPARE) {
            const prdb_sdk_sql_bound_column_v2 *column =
                &binding.predicate_columns[index];
            prdb_sdk_select_v1 parameter_statement = {0};
            prdb_sdk_value_v1 bound_parameter = {0};
            parameter_statement.struct_size = sizeof(parameter_statement);
            parameter_statement.abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
            parameter_statement.sql = sql;
            parameter_statement.sql_len = sql_len;
            parameter_statement.has_where = 1;
            parameter_statement.parameter_index = node->parameter_index;
            status = prdb_sdk_sql_bind_select_parameter_v1(
                &parameter_statement, parameters, parameter_count,
                column->type_code, column->decimal_precision,
                column->decimal_scale, &bound_parameter,
                sizeof(bound_parameter));
            if (status != PRDB_SDK_SQL_EVAL_OK) {
                return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
            }
        }
    }
    if (statement.has_limit && statement.limit == 0) return PRDB_SDK_SQL_EXEC_OK;

    status = prdb_sdk_executor_open_full_scan_v1(
        host, resolved.executor_handle, executor_response,
        executor_response_capacity, &executor_response_len, cursor);
    if (status != PRDB_SDK_EXECUTOR_OK) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    }
    cursor_open = 1;

    for (;;) {
        if (page_count >= PRDB_SDK_SQL_EXEC_MAX_NEXT_REQUESTS) {
            final_status = PRDB_SDK_SQL_EXEC_REQUEST_LIMIT;
            break;
        }
        status = prdb_sdk_executor_next_batch_v1(
            host, cursor, max_candidates, max_batch_bytes, batch_response,
            batch_response_capacity, &batch_response_len, &batch, sizeof(batch));
        if (status != PRDB_SDK_EXECUTOR_OK) {
            final_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
            break;
        }
        ++page_count;
        if (batch.finished) {
            cursor_open = 0;
        } else {
            memcpy(cursor, batch.continuation, sizeof(cursor));
        }
        status = prdb_sdk_batch_rows_begin_v1(&batch, &rows, sizeof(rows));
        if (status != PRDB_SDK_BATCH_OK) {
            final_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
            break;
        }
        while (rows.rows_remaining != 0) {
            uint32_t truth = PRDB_SDK_SQL_TRUTH_TRUE;
            status = prdb_sdk_batch_row_next_v1(&rows, &row, sizeof(row));
            if (status != PRDB_SDK_BATCH_OK) {
                final_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
                break;
            }
            if (statement.predicate_count != 0) {
                status = prdb_sdk_sql_eval_bound_select_row_v2(
                    &statement, &binding, parameters, parameter_count, &row,
                    cell_scratch, cell_scratch_capacity, &truth);
                if (status != PRDB_SDK_SQL_EVAL_OK) {
                    final_status = PRDB_SDK_SQL_EXEC_EVALUATION_ERROR;
                    break;
                }
            }
            if (truth == PRDB_SDK_SQL_TRUTH_TRUE) {
                if (on_row(result_context, &row, &binding) != 0) {
                    final_status = PRDB_SDK_SQL_EXEC_RESULT_ERROR;
                    break;
                }
                ++delivered;
                if (statement.has_limit && delivered >= statement.limit) break;
            }
        }
        if (final_status != PRDB_SDK_SQL_EXEC_OK) break;
        if (statement.has_limit && delivered >= statement.limit) break;
        if (batch.finished) break;
    }

    if (cursor_open) {
        status = prdb_sdk_executor_close_cursor_v1(
            host, cursor, executor_response, executor_response_capacity,
            &executor_response_len);
        if (status != PRDB_SDK_EXECUTOR_OK && final_status == PRDB_SDK_SQL_EXEC_OK) {
            final_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
        }
    }
    return fail_statement(host_v3, final_status);
}

int32_t prdb_sdk_sql_execute_insert_v1(
    const prdb_host_api_v3 *host_v3, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *insert_request, uint64_t insert_request_capacity,
    prdb_sdk_sql_column_shape_v2 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v2 *executor_columns,
    uint32_t executor_column_capacity,
    prdb_sdk_executor_insert_value_v1 *insert_values,
    uint32_t insert_value_capacity, uint8_t row_handle_out[16]) {
    prdb_sdk_insert_v1 statement = {0};
    prdb_sdk_planner_relation_v1 planned = {0};
    prdb_sdk_executor_relation_v1 resolved = {0};
    uint64_t planner_response_len = 0;
    uint64_t executor_response_len = 0;
    int32_t status;
    prdb_host_api_v2 base_host;
    const prdb_host_api_v2 *host;

    if (host_v3 == NULL || host_v3->struct_size < sizeof(*host_v3) ||
        host_v3->abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host_v3->statement_failure == NULL ||
        host_v3->executor_request == NULL || host_v3->planner_request == NULL) {
        return PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT;
    }
    base_host = (prdb_host_api_v2){
        sizeof(prdb_host_api_v2), PRDB_QUERY_ABI_V2_VERSION, host_v3->context,
        host_v3->executor_request, host_v3->planner_request,
        host_v3->write_result};
    host = &base_host;
    if (schema == NULL || sql == NULL || planner_response == NULL ||
        executor_response == NULL || insert_request == NULL ||
        planner_columns == NULL || executor_columns == NULL ||
        insert_values == NULL || row_handle_out == NULL ||
        parameter_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        (parameter_count != 0 && parameters == NULL) ||
        planner_column_capacity == 0 || executor_column_capacity == 0 ||
        planner_response_capacity == 0 || executor_response_capacity == 0 ||
        insert_request_capacity == 0 ||
        insert_request_capacity > PRDB_QUERY_MAX_IPC_REQUEST_BYTES) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT);
    }
    status = prdb_sdk_sql_parse_insert_v1(sql, sql_len, &statement,
                                          sizeof(statement), NULL);
    if (status != PRDB_SDK_SQL_OK) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    }
    status = prdb_sdk_planner_describe_relation_v1(
        host, schema, schema_len, sql + statement.relation.offset,
        statement.relation.length, planner_response, planner_response_capacity,
        &planner_response_len, &planned, sizeof(planned), planner_columns,
        planner_column_capacity);
    if (status != PRDB_SDK_PLANNER_REQUEST_OK) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_PLANNER_ERROR);
    }
    status = prdb_sdk_executor_resolve_relation_v1(
        host, schema, schema_len, sql + statement.relation.offset,
        statement.relation.length, executor_response,
        executor_response_capacity, &executor_response_len, &resolved,
        sizeof(resolved), executor_columns, executor_column_capacity);
    if (status != PRDB_SDK_EXECUTOR_OK) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    }
    if (!same_relation_shape(&planned, &resolved, planner_columns,
                             executor_columns)) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH);
    }
    if (statement.value_count != resolved.column_count ||
        resolved.column_count > PRDB_SDK_SQL_MAX_INSERT_VALUES ||
        resolved.column_count > insert_value_capacity) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    }
    for (uint32_t index = 0; index < statement.value_count; ++index) {
        const prdb_sdk_sql_insert_cell_v1 *cell = &statement.values[index];
        prdb_sdk_executor_insert_value_v1 *input = &insert_values[index];
        input->struct_size = sizeof(*input);
        input->abi_version = PRDB_SDK_EXECUTOR_ABI_V1_VERSION;
        input->state = cell->kind;
        if (cell->kind == PRDB_SDK_SQL_INSERT_PARAMETER) {
            status = prdb_sdk_sql_bind_parameter_v1(
                cell->parameter_index, parameters, parameter_count,
                executor_columns[index].type_code,
                executor_columns[index].decimal_precision,
                executor_columns[index].decimal_scale, &input->value,
                sizeof(input->value));
            if (status != PRDB_SDK_SQL_EVAL_OK) {
                return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
            }
            input->state = input->value.state == PRDB_SDK_VALUE_STATE_NULL
                               ? PRDB_SDK_EXECUTOR_INSERT_NULL
                               : PRDB_SDK_EXECUTOR_INSERT_VALUE;
        } else if (cell->kind == PRDB_SDK_SQL_INSERT_NULL) {
            input->state = PRDB_SDK_EXECUTOR_INSERT_NULL;
            input->value = (prdb_sdk_value_v1){
                sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
                PRDB_SDK_VALUE_STATE_NULL, 0, NULL, 0};
        } else if (cell->kind == PRDB_SDK_SQL_INSERT_DEFAULT) {
            input->state = PRDB_SDK_EXECUTOR_INSERT_DEFAULT;
        } else {
            return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
        }
    }
    status = prdb_sdk_executor_stage_insert_v1(
        host, resolved.executor_handle, insert_values,
        (uint16_t)statement.value_count, insert_request,
        insert_request_capacity, executor_response,
        executor_response_capacity, &executor_response_len, row_handle_out);
    if (status != PRDB_SDK_EXECUTOR_OK) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    }
    return PRDB_SDK_SQL_EXEC_OK;
}
