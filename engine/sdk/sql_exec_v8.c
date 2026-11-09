#include "prdb_sdk_sql_exec_v8.h"

#include <string.h>

#define SQL_EXEC_V8_MAX_NEXT_REQUESTS UINT32_C(252)

static int32_t fail_statement_v8(const prdb_host_api_v3 *host,
                                 int32_t status) {
    if (status != PRDB_SDK_SQL_EXEC_OK) {
        (void)host->statement_failure(host->context,
                                      PRDB_QUERY_STATEMENT_FAILURE_SQL);
    }
    return status;
}

static int same_relation_shape_v8(
    const prdb_sdk_planner_relation_v1 *planned,
    const prdb_sdk_executor_relation_v1 *resolved,
    const prdb_sdk_sql_column_shape_v2 *planned_columns,
    const prdb_sdk_sql_column_shape_v2 *resolved_columns) {
    if (memcmp(planned->relation_id, resolved->relation_id, 16) != 0 ||
        planned->column_count != resolved->column_count ||
        planned->name_len != resolved->name_len ||
        memcmp(planned->name, resolved->name, planned->name_len) != 0) {
        return 0;
    }
    for (uint32_t index = 0; index < planned->column_count; ++index) {
        const prdb_sdk_sql_column_shape_v2 *a = &planned_columns[index];
        const prdb_sdk_sql_column_shape_v2 *b = &resolved_columns[index];
        if (a->ordinal != b->ordinal || a->type_code != b->type_code ||
            a->decimal_precision != b->decimal_precision ||
            a->decimal_scale != b->decimal_scale || a->name_len != b->name_len ||
            memcmp(a->name, b->name, a->name_len) != 0) {
            return 0;
        }
    }
    return 1;
}

static int32_t copy_value_v8(const prdb_sdk_value_v1 *source,
                             prdb_sdk_value_v1 *destination,
                             uint8_t *materialized_bytes,
                             uint64_t materialized_capacity,
                             uint64_t *materialized_used) {
    int32_t status = prdb_sdk_value_validate_v1(source);
    if (status == PRDB_SDK_VALUE_INVALID_ARGUMENT) {
        return PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT;
    }
    if (status != PRDB_SDK_VALUE_VALID) {
        return PRDB_SDK_SQL_EXEC_EVALUATION_ERROR;
    }
    *destination = *source;
    if (source->state != PRDB_SDK_VALUE_STATE_VALUE) {
        destination->data = NULL;
        destination->data_len = 0;
        return PRDB_SDK_SQL_EXEC_OK;
    }
    if (source->data_len > materialized_capacity - *materialized_used) {
        return PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT;
    }
    if (source->data_len != 0) {
        memcpy(materialized_bytes + *materialized_used, source->data,
               (size_t)source->data_len);
    }
    destination->data = materialized_bytes + *materialized_used;
    *materialized_used += source->data_len;
    return PRDB_SDK_SQL_EXEC_OK;
}

static int32_t materialize_relation_v8(
    const prdb_host_api_v2 *host,
    const prdb_sdk_executor_relation_v1 *relation,
    const prdb_sdk_select_binding_v8 *binding, uint32_t relation_side,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_value_v1 *cell_scratch, uint32_t cell_scratch_capacity,
    prdb_sdk_sql_join_materialized_row_v8 *rows, uint32_t row_capacity,
    uint32_t *row_count_out, uint8_t *materialized_bytes,
    uint64_t materialized_bytes_capacity, uint64_t *materialized_used) {
    uint8_t cursor[16] = {0};
    uint8_t close_response[8] = {0};
    uint64_t executor_response_len = 0, batch_response_len = 0;
    uint64_t close_response_len = 0;
    uint32_t page_count = 0, row_count = 0;
    int cursor_open = 0;
    int32_t status = prdb_sdk_executor_open_full_scan_v1(
        host, relation->executor_handle, executor_response,
        executor_response_capacity, &executor_response_len, cursor);
    int32_t final_status = PRDB_SDK_SQL_EXEC_OK;

    if (status != PRDB_SDK_EXECUTOR_OK) {
        return PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
    }
    cursor_open = 1;
    for (;;) {
        prdb_sdk_batch_v1 batch = {0};
        prdb_sdk_row_iterator_v1 row_iterator = {0};
        if (page_count >= SQL_EXEC_V8_MAX_NEXT_REQUESTS) {
            final_status = PRDB_SDK_SQL_EXEC_REQUEST_LIMIT;
            break;
        }
        status = prdb_sdk_executor_next_batch_v1(
            host, cursor, max_candidates, max_batch_bytes, batch_response,
            batch_response_capacity, &batch_response_len, &batch,
            sizeof(batch));
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
        status = prdb_sdk_batch_rows_begin_v1(&batch, &row_iterator,
                                              sizeof(row_iterator));
        if (status != PRDB_SDK_BATCH_OK) {
            final_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
            break;
        }
        while (row_iterator.rows_remaining != 0) {
            prdb_sdk_row_v1 row = {0};
            prdb_sdk_cell_iterator_v1 cell_iterator = {0};
            prdb_sdk_sql_join_materialized_row_v8 *materialized_row;
            const prdb_sdk_sql_join_projection_binding_v8 *join_key_binding =
                relation_side == PRDB_SDK_SQL_V8_LEFT
                    ? &binding->left_join_key
                    : &binding->right_join_key;

            if (row_count >= row_capacity) {
                final_status = PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT;
                break;
            }
            status = prdb_sdk_batch_row_next_v1(
                &row_iterator, &row, sizeof(row));
            if (status != PRDB_SDK_BATCH_OK ||
                row.cell_count != relation->column_count) {
                final_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
                break;
            }
            if (prdb_sdk_row_cells_begin_v1(&row, &cell_iterator,
                                            sizeof(cell_iterator)) !=
                PRDB_SDK_BATCH_OK) {
                final_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
                break;
            }
            for (uint32_t ordinal = 0; ordinal < relation->column_count;
                 ++ordinal) {
                if (prdb_sdk_cell_next_v1(
                        &cell_iterator, &cell_scratch[ordinal],
                        sizeof(cell_scratch[ordinal])) != PRDB_SDK_BATCH_OK) {
                    final_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
                    break;
                }
            }
            if (final_status != PRDB_SDK_SQL_EXEC_OK) break;

            materialized_row = &rows[row_count];
            memset(materialized_row, 0, sizeof(*materialized_row));
            status = copy_value_v8(
                &cell_scratch[join_key_binding->column.ordinal],
                &materialized_row->join_key, materialized_bytes,
                materialized_bytes_capacity, materialized_used);
            if (status != PRDB_SDK_SQL_EXEC_OK) {
                final_status = status;
                break;
            }
            for (uint32_t projection = 0;
                 projection < binding->projection_count; ++projection) {
                const prdb_sdk_sql_join_projection_binding_v8 *projected =
                    &binding->projections[projection];
                if (projected->relation_side != relation_side) continue;
                status = copy_value_v8(
                    &cell_scratch[projected->column.ordinal],
                    &materialized_row->projections[projection],
                    materialized_bytes, materialized_bytes_capacity,
                    materialized_used);
                if (status != PRDB_SDK_SQL_EXEC_OK) {
                    final_status = status;
                    break;
                }
            }
            if (final_status != PRDB_SDK_SQL_EXEC_OK) break;
            ++row_count;
        }
        if (final_status != PRDB_SDK_SQL_EXEC_OK || batch.finished) break;
    }

    if (cursor_open) {
        status = prdb_sdk_executor_close_cursor_v1(
            host, cursor, close_response, sizeof(close_response),
            &close_response_len);
        if (status != PRDB_SDK_EXECUTOR_OK &&
            final_status == PRDB_SDK_SQL_EXEC_OK) {
            final_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
        }
    }
    if (final_status == PRDB_SDK_SQL_EXEC_OK) *row_count_out = row_count;
    return final_status;
}

int32_t prdb_sdk_sql_execute_inner_join_v8(
    const prdb_host_api_v3 *host_v3,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *sql, uint64_t sql_len,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *left_planner_response, uint64_t left_planner_response_capacity,
    uint8_t *right_planner_response, uint64_t right_planner_response_capacity,
    uint8_t *left_executor_response, uint64_t left_executor_response_capacity,
    uint8_t *right_executor_response, uint64_t right_executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v2 *left_planner_columns,
    uint32_t left_planner_column_capacity,
    prdb_sdk_sql_column_shape_v2 *right_planner_columns,
    uint32_t right_planner_column_capacity,
    prdb_sdk_sql_column_shape_v2 *left_executor_columns,
    uint32_t left_executor_column_capacity,
    prdb_sdk_sql_column_shape_v2 *right_executor_columns,
    uint32_t right_executor_column_capacity,
    prdb_sdk_value_v1 *cell_scratch, uint32_t cell_scratch_capacity,
    prdb_sdk_sql_join_materialized_row_v8 *left_rows,
    uint32_t left_row_capacity,
    prdb_sdk_sql_join_materialized_row_v8 *right_rows,
    uint32_t right_row_capacity,
    uint8_t *materialized_bytes, uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_join_result_row_v8_fn on_row, void *result_context) {
    prdb_sdk_select_v8 statement = {0};
    prdb_sdk_select_binding_v8 binding = {0};
    prdb_sdk_planner_relation_v1 planned_left = {0}, planned_right = {0};
    prdb_sdk_executor_relation_v1 resolved_left = {0}, resolved_right = {0};
    prdb_sdk_value_v1 left_keys[PRDB_SDK_SQL_JOIN_MAX_ROWS] = {{0}};
    prdb_sdk_value_v1 right_keys[PRDB_SDK_SQL_JOIN_MAX_ROWS] = {{0}};
    prdb_sdk_sql_join_pair_v1 pairs[PRDB_SDK_SQL_JOIN_MAX_PAIRS];
    prdb_sdk_value_v1 projected_values[PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS];
    uint32_t left_row_count = 0, right_row_count = 0, pair_count = 0;
    uint64_t left_planner_response_len = 0, right_planner_response_len = 0;
    uint64_t left_executor_response_len = 0, right_executor_response_len = 0;
    uint64_t materialized_used = 0;
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
    if (schema == NULL || sql == NULL || left_planner_response == NULL ||
        right_planner_response == NULL || left_executor_response == NULL ||
        right_executor_response == NULL || batch_response == NULL ||
        left_planner_columns == NULL || right_planner_columns == NULL ||
        left_executor_columns == NULL || right_executor_columns == NULL ||
        cell_scratch == NULL || left_rows == NULL || right_rows == NULL ||
        materialized_bytes == NULL || on_row == NULL ||
        left_planner_column_capacity == 0 ||
        right_planner_column_capacity == 0 ||
        left_executor_column_capacity == 0 ||
        right_executor_column_capacity == 0 ||
        left_planner_response_capacity == 0 ||
        right_planner_response_capacity == 0 ||
        left_executor_response_capacity == 0 ||
        right_executor_response_capacity == 0 ||
        batch_response_capacity < max_batch_bytes || max_candidates == 0 ||
        max_candidates > PRDB_SDK_BATCH_MAX_ROWS || max_batch_bytes < 8 ||
        max_batch_bytes > PRDB_SDK_BATCH_MAX_BYTES ||
        cell_scratch_capacity == 0 ||
        left_row_capacity == 0 ||
        left_row_capacity > PRDB_SDK_SQL_EXEC_V8_MAX_ROWS ||
        right_row_capacity == 0 ||
        right_row_capacity > PRDB_SDK_SQL_EXEC_V8_MAX_ROWS ||
        materialized_bytes_capacity >
            PRDB_SDK_SQL_EXEC_V8_MAX_MATERIALIZED_BYTES) {
        return fail_statement_v8(host_v3,
                                 PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT);
    }
    status = prdb_sdk_sql_parse_select_v8(
        sql, sql_len, &statement, sizeof(statement), NULL);
    if (status != PRDB_SDK_SQL_OK) {
        return fail_statement_v8(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    }
    status = prdb_sdk_planner_describe_relation_v1(
        host, schema, schema_len,
        sql + statement.left_relation.offset, statement.left_relation.length,
        left_planner_response, left_planner_response_capacity,
        &left_planner_response_len, &planned_left, sizeof(planned_left),
        left_planner_columns, left_planner_column_capacity);
    if (status != PRDB_SDK_PLANNER_REQUEST_OK) {
        return fail_statement_v8(host_v3, PRDB_SDK_SQL_EXEC_PLANNER_ERROR);
    }
    status = prdb_sdk_planner_describe_relation_v1(
        host, schema, schema_len,
        sql + statement.right_relation.offset, statement.right_relation.length,
        right_planner_response, right_planner_response_capacity,
        &right_planner_response_len, &planned_right, sizeof(planned_right),
        right_planner_columns, right_planner_column_capacity);
    if (status != PRDB_SDK_PLANNER_REQUEST_OK) {
        return fail_statement_v8(host_v3, PRDB_SDK_SQL_EXEC_PLANNER_ERROR);
    }
    status = prdb_sdk_executor_resolve_relation_v1(
        host, schema, schema_len,
        sql + statement.left_relation.offset, statement.left_relation.length,
        left_executor_response, left_executor_response_capacity,
        &left_executor_response_len, &resolved_left, sizeof(resolved_left),
        left_executor_columns, left_executor_column_capacity);
    if (status != PRDB_SDK_EXECUTOR_OK) {
        return fail_statement_v8(host_v3, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    }
    status = prdb_sdk_executor_resolve_relation_v1(
        host, schema, schema_len,
        sql + statement.right_relation.offset, statement.right_relation.length,
        right_executor_response, right_executor_response_capacity,
        &right_executor_response_len, &resolved_right, sizeof(resolved_right),
        right_executor_columns, right_executor_column_capacity);
    if (status != PRDB_SDK_EXECUTOR_OK) {
        return fail_statement_v8(host_v3, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    }
    if (!same_relation_shape_v8(&planned_left, &resolved_left,
                                left_planner_columns,
                                left_executor_columns) ||
        !same_relation_shape_v8(&planned_right, &resolved_right,
                                right_planner_columns,
                                right_executor_columns) ||
        memcmp(resolved_left.relation_id, resolved_right.relation_id, 16) ==
            0) {
        return fail_statement_v8(host_v3,
                                 PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH);
    }
    if (cell_scratch_capacity < resolved_left.column_count ||
        cell_scratch_capacity < resolved_right.column_count) {
        return fail_statement_v8(host_v3,
                                 PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT);
    }
    status = prdb_sdk_sql_bind_select_v8(
        &statement, resolved_left.name, resolved_left.name_len,
        left_executor_columns, resolved_left.column_count,
        resolved_right.name, resolved_right.name_len,
        right_executor_columns, resolved_right.column_count, &binding,
        sizeof(binding));
    if (status != PRDB_SDK_SQL_BIND_OK) {
        return fail_statement_v8(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    }
    if (statement.has_limit != 0 && statement.limit == 0) {
        return PRDB_SDK_SQL_EXEC_OK;
    }

    status = materialize_relation_v8(
        host, &resolved_left, &binding,
        PRDB_SDK_SQL_V8_LEFT, max_candidates, max_batch_bytes,
        left_executor_response, left_executor_response_capacity,
        batch_response, batch_response_capacity, cell_scratch,
        cell_scratch_capacity, left_rows, left_row_capacity, &left_row_count,
        materialized_bytes, materialized_bytes_capacity, &materialized_used);
    if (status != PRDB_SDK_SQL_EXEC_OK) {
        return fail_statement_v8(host_v3, status);
    }
    status = materialize_relation_v8(
        host, &resolved_right, &binding,
        PRDB_SDK_SQL_V8_RIGHT, max_candidates, max_batch_bytes,
        right_executor_response, right_executor_response_capacity,
        batch_response, batch_response_capacity, cell_scratch,
        cell_scratch_capacity, right_rows, right_row_capacity,
        &right_row_count, materialized_bytes, materialized_bytes_capacity,
        &materialized_used);
    if (status != PRDB_SDK_SQL_EXEC_OK) {
        return fail_statement_v8(host_v3, status);
    }
    for (uint32_t index = 0; index < left_row_count; ++index) {
        left_keys[index] = left_rows[index].join_key;
    }
    for (uint32_t index = 0; index < right_row_count; ++index) {
        right_keys[index] = right_rows[index].join_key;
    }
    status = prdb_sdk_sql_inner_join_pairs_v1(
        left_keys, left_row_count,
        binding.left_join_key.column.decimal_scale, right_keys,
        right_row_count, binding.right_join_key.column.decimal_scale,
        binding.left_join_key.column.type_code, pairs,
        PRDB_SDK_SQL_JOIN_MAX_PAIRS, &pair_count);
    if (status != PRDB_SDK_SQL_JOIN_OK) {
        return fail_statement_v8(host_v3,
                                 PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
    }
    for (uint32_t pair_index = 0; pair_index < pair_count; ++pair_index) {
        const prdb_sdk_sql_join_pair_v1 *pair = &pairs[pair_index];
        for (uint32_t projection = 0;
             projection < binding.projection_count; ++projection) {
            const prdb_sdk_sql_join_projection_binding_v8 *projection_binding =
                &binding.projections[projection];
            projected_values[projection] =
                projection_binding->relation_side == PRDB_SDK_SQL_V8_LEFT
                    ? left_rows[pair->left_row].projections[projection]
                    : right_rows[pair->right_row].projections[projection];
        }
        if (on_row(result_context, projected_values, binding.projection_count,
                   &binding) != PRDB_QUERY_CALLBACK_OK) {
            return fail_statement_v8(host_v3,
                                     PRDB_SDK_SQL_EXEC_RESULT_ERROR);
        }
        if (statement.has_limit != 0 && pair_index + 1 >= statement.limit) {
            break;
        }
    }
    return PRDB_SDK_SQL_EXEC_OK;
}
