#include "prdb_sdk_sql_exec_v19.h"
#include "prdb_sdk_sql_exec_v20.h"
#include "prdb_sdk_optimizer_plan_v1.h"
#include "prdb_sdk_optimizer_v1.h"
#include "sql_parameter_internal.h"

#include <string.h>

/* Each page uses one cancellation check and one NEXT request. V19 reserves
 * 121 pages (242 calls), six resolve/open/close calls, and eight post-scan
 * cancellation checks. The optimizer-gated v19 path re-resolves both relations
 * before execution, so its scan ceiling is 120 pages. V20's independent
 * planner/executor preflight adds four requests, so its scan ceiling is 119
 * pages under the same 256-call cap. */
#define SQL_EXEC_V19_MAX_SCAN_PAGES UINT32_C(121)
#define SQL_EXEC_V19_OPTIMIZER_MAX_SCAN_PAGES UINT32_C(120)
#define SQL_EXEC_V20_MAX_SCAN_PAGES UINT32_C(119)
#define SQL_EXEC_V20_OPTIMIZER_MAX_SCAN_PAGES UINT32_C(117)
#define SQL_EXEC_V19_JOIN_POLL_INTERVAL UINT32_C(2048)

static int32_t fail_statement_v19(const prdb_host_api_v3 *host,
                                 int32_t status) {
    if (status != PRDB_SDK_SQL_EXEC_OK) {
        (void)host->statement_failure(host->context,
                                      PRDB_QUERY_STATEMENT_FAILURE_SQL);
    }
    return status;
}

static int32_t check_cancelled_v19(const prdb_host_api_v2 *host) {
    int32_t cancelled = 0;
    int32_t status = prdb_sdk_executor_check_cancelled_v1(host, &cancelled);
    return status == PRDB_SDK_EXECUTOR_OK && !cancelled
               ? PRDB_SDK_SQL_EXEC_OK
               : PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
}

static int same_relation_shape_v19(
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

static int32_t copy_value_v19(const prdb_sdk_value_v1 *source,
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

static int32_t materialize_relation_v19(
    const prdb_host_api_v2 *host,
    const prdb_sdk_executor_relation_v1 *relation,
    const prdb_sdk_select_binding_v16 *binding, uint32_t relation_side,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_value_v1 *cell_scratch, uint32_t cell_scratch_capacity,
    prdb_sdk_sql_join_materialized_row_v16 *rows, uint32_t row_capacity,
    uint32_t *row_count_out, uint32_t *total_pages,
    uint32_t max_scan_pages,
    uint8_t *materialized_bytes,
    uint64_t materialized_bytes_capacity, uint64_t *materialized_used) {
    uint8_t cursor[16] = {0};
    uint8_t close_response[8] = {0};
    uint64_t executor_response_len = 0, batch_response_len = 0;
    uint64_t close_response_len = 0;
    uint32_t row_count = 0;
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
        if (*total_pages >= max_scan_pages) {
            final_status = PRDB_SDK_SQL_EXEC_REQUEST_LIMIT;
            break;
        }
        status = check_cancelled_v19(host);
        if (status != PRDB_SDK_SQL_EXEC_OK) {
            final_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
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
        ++*total_pages;
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
            prdb_sdk_sql_join_materialized_row_v16 *materialized_row;

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
            for (uint32_t key = 0; key < binding->join_key_count; ++key) {
                const prdb_sdk_sql_join_key_binding_v16 *key_binding =
                    &binding->join_keys[key];
                uint32_t ordinal =
                    relation_side == PRDB_SDK_SQL_V8_LEFT
                        ? key_binding->left.ordinal
                        : key_binding->right.ordinal;
                status = copy_value_v19(
                    &cell_scratch[ordinal], &materialized_row->join_keys[key],
                    materialized_bytes, materialized_bytes_capacity,
                    materialized_used);
                if (status != PRDB_SDK_SQL_EXEC_OK) {
                    final_status = status;
                    break;
                }
            }
            if (final_status != PRDB_SDK_SQL_EXEC_OK) break;
            for (uint32_t projection = 0;
                 projection < binding->base.projection_count; ++projection) {
                const prdb_sdk_sql_join_projection_binding_v8 *projected =
                    &binding->base.projections[projection];
                if (projected->relation_side != relation_side) continue;
                status = copy_value_v19(
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

static int32_t execute_inner_join_v19_with_page_limit(
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
    prdb_sdk_sql_join_materialized_row_v16 *left_rows,
    uint32_t left_row_capacity,
    prdb_sdk_sql_join_materialized_row_v16 *right_rows,
    uint32_t right_row_capacity,
    uint8_t *materialized_bytes, uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_join_result_row_v19_fn on_row, void *result_context,
    uint32_t max_scan_pages) {
    prdb_sdk_select_v19 statement = {0};
    prdb_sdk_select_binding_v19 binding = {0};
    prdb_sdk_planner_relation_v1 planned_left = {0}, planned_right = {0};
    prdb_sdk_executor_relation_v1 resolved_left = {0}, resolved_right = {0};
    prdb_sdk_value_v1 left_keys[PRDB_SDK_SQL_JOIN_MAX_ROWS *
                                 PRDB_SDK_SQL_V16_MAX_JOIN_KEYS] = {{0}};
    prdb_sdk_value_v1 right_keys[PRDB_SDK_SQL_JOIN_MAX_ROWS *
                                  PRDB_SDK_SQL_V16_MAX_JOIN_KEYS] = {{0}};
    prdb_sdk_sql_join_key_shape_v2 join_shapes[
        PRDB_SDK_SQL_V16_MAX_JOIN_KEYS] = {{0}};
    prdb_sdk_sql_join_pair_v1 pairs[PRDB_SDK_SQL_JOIN_MAX_PAIRS];
    prdb_sdk_value_v1 projected_values[PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS];
    uint32_t left_row_count = 0, right_row_count = 0, pair_count = 0;
    uint32_t total_pages = 0;
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
        return fail_statement_v19(host_v3,
                                 PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT);
    }
    status = prdb_sdk_sql_parse_select_v19(
        sql, sql_len, &statement, sizeof(statement), NULL);
    if (status != PRDB_SDK_SQL_OK) {
        return fail_statement_v19(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    }
    status = prdb_sdk_planner_describe_relation_v1(
        host, schema, schema_len,
        sql + statement.base.base.left_relation.offset,
        statement.base.base.left_relation.length,
        left_planner_response, left_planner_response_capacity,
        &left_planner_response_len, &planned_left, sizeof(planned_left),
        left_planner_columns, left_planner_column_capacity);
    if (status != PRDB_SDK_PLANNER_REQUEST_OK) {
        return fail_statement_v19(host_v3, PRDB_SDK_SQL_EXEC_PLANNER_ERROR);
    }
    status = prdb_sdk_planner_describe_relation_v1(
        host, schema, schema_len,
        sql + statement.base.base.right_relation.offset,
        statement.base.base.right_relation.length,
        right_planner_response, right_planner_response_capacity,
        &right_planner_response_len, &planned_right, sizeof(planned_right),
        right_planner_columns, right_planner_column_capacity);
    if (status != PRDB_SDK_PLANNER_REQUEST_OK) {
        return fail_statement_v19(host_v3, PRDB_SDK_SQL_EXEC_PLANNER_ERROR);
    }
    status = prdb_sdk_executor_resolve_relation_v1(
        host, schema, schema_len,
        sql + statement.base.base.left_relation.offset,
        statement.base.base.left_relation.length,
        left_executor_response, left_executor_response_capacity,
        &left_executor_response_len, &resolved_left, sizeof(resolved_left),
        left_executor_columns, left_executor_column_capacity);
    if (status != PRDB_SDK_EXECUTOR_OK) {
        return fail_statement_v19(host_v3, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    }
    status = prdb_sdk_executor_resolve_relation_v1(
        host, schema, schema_len,
        sql + statement.base.base.right_relation.offset,
        statement.base.base.right_relation.length,
        right_executor_response, right_executor_response_capacity,
        &right_executor_response_len, &resolved_right, sizeof(resolved_right),
        right_executor_columns, right_executor_column_capacity);
    if (status != PRDB_SDK_EXECUTOR_OK) {
        return fail_statement_v19(host_v3, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    }
    if (!same_relation_shape_v19(&planned_left, &resolved_left,
                                left_planner_columns,
                                left_executor_columns) ||
        !same_relation_shape_v19(&planned_right, &resolved_right,
                                right_planner_columns,
                                right_executor_columns)) {
        return fail_statement_v19(host_v3,
                                 PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH);
    }
    if (cell_scratch_capacity < resolved_left.column_count ||
        cell_scratch_capacity < resolved_right.column_count) {
        return fail_statement_v19(host_v3,
                                 PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT);
    }
    status = prdb_sdk_sql_bind_select_v19(
        &statement, resolved_left.name, resolved_left.name_len,
        left_executor_columns, resolved_left.column_count,
        resolved_right.name, resolved_right.name_len,
        right_executor_columns, resolved_right.column_count, &binding,
        sizeof(binding));
    if (status != PRDB_SDK_SQL_BIND_OK) {
        return fail_statement_v19(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    }
    if (statement.base.base.has_limit != 0 && statement.base.base.limit == 0) {
        return PRDB_SDK_SQL_EXEC_OK;
    }

    status = materialize_relation_v19(
        host, &resolved_left, &binding.base,
        PRDB_SDK_SQL_V8_LEFT, max_candidates, max_batch_bytes,
        left_executor_response, left_executor_response_capacity,
        batch_response, batch_response_capacity, cell_scratch,
        cell_scratch_capacity, left_rows, left_row_capacity, &left_row_count,
        &total_pages, max_scan_pages, materialized_bytes,
        materialized_bytes_capacity,
        &materialized_used);
    if (status != PRDB_SDK_SQL_EXEC_OK) {
        return fail_statement_v19(host_v3, status);
    }
    status = materialize_relation_v19(
        host, &resolved_right, &binding.base,
        PRDB_SDK_SQL_V8_RIGHT, max_candidates, max_batch_bytes,
        right_executor_response, right_executor_response_capacity,
        batch_response, batch_response_capacity, cell_scratch,
        cell_scratch_capacity, right_rows, right_row_capacity,
        &right_row_count, &total_pages, max_scan_pages, materialized_bytes,
        materialized_bytes_capacity,
        &materialized_used);
    if (status != PRDB_SDK_SQL_EXEC_OK) {
        return fail_statement_v19(host_v3, status);
    }
    for (uint32_t index = 0; index < left_row_count; ++index) {
        for (uint32_t key = 0; key < binding.base.join_key_count; ++key) {
            left_keys[index * binding.base.join_key_count + key] =
                left_rows[index].join_keys[key];
        }
    }
    for (uint32_t index = 0; index < right_row_count; ++index) {
        for (uint32_t key = 0; key < binding.base.join_key_count; ++key) {
            right_keys[index * binding.base.join_key_count + key] =
                right_rows[index].join_keys[key];
        }
    }
    for (uint32_t key = 0; key < binding.base.join_key_count; ++key) {
        join_shapes[key] = (prdb_sdk_sql_join_key_shape_v2){
            sizeof(prdb_sdk_sql_join_key_shape_v2),
            PRDB_SDK_SQL_JOIN_ABI_V2_VERSION,
            binding.base.join_keys[key].left.type_code,
            binding.base.join_keys[key].left.decimal_scale,
            {0, 0, 0}};
    }
    status = prdb_sdk_sql_inner_join_pairs_composite_v2(
        left_keys, left_row_count, right_keys, right_row_count, join_shapes,
        binding.base.join_key_count, pairs,
        PRDB_SDK_SQL_JOIN_MAX_PAIRS, &pair_count);
    if (status != PRDB_SDK_SQL_JOIN_OK) {
        return fail_statement_v19(host_v3,
                                 PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
    }
    status = check_cancelled_v19(host);
    if (status != PRDB_SDK_SQL_EXEC_OK) {
        return fail_statement_v19(host_v3, status);
    }
    for (uint32_t pair_index = 0; pair_index < pair_count; ++pair_index) {
        if (pair_index != 0 &&
            pair_index % SQL_EXEC_V19_JOIN_POLL_INTERVAL == 0) {
            status = check_cancelled_v19(host);
            if (status != PRDB_SDK_SQL_EXEC_OK) {
                return fail_statement_v19(host_v3, status);
            }
        }
        const prdb_sdk_sql_join_pair_v1 *pair = &pairs[pair_index];
        for (uint32_t projection = 0;
             projection < binding.base.base.projection_count; ++projection) {
            const prdb_sdk_sql_join_projection_binding_v8 *projection_binding =
                &binding.base.base.projections[projection];
            projected_values[projection] =
                projection_binding->relation_side == PRDB_SDK_SQL_V8_LEFT
                    ? left_rows[pair->left_row].projections[projection]
                    : right_rows[pair->right_row].projections[projection];
        }
        if (on_row(result_context, projected_values,
                   binding.base.base.projection_count,
                   &binding) != PRDB_QUERY_CALLBACK_OK) {
            return fail_statement_v19(host_v3,
                                     PRDB_SDK_SQL_EXEC_RESULT_ERROR);
        }
        if (statement.base.base.has_limit != 0 &&
            pair_index + 1 >= statement.base.base.limit) {
            break;
        }
    }
    return PRDB_SDK_SQL_EXEC_OK;
}

int32_t prdb_sdk_sql_execute_inner_join_v19(
    const prdb_host_api_v3 *host, const uint8_t *schema, uint32_t schema_len,
    const uint8_t *sql, uint64_t sql_len, uint16_t max_candidates,
    uint32_t max_batch_bytes,
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
    prdb_sdk_sql_join_materialized_row_v16 *left_rows,
    uint32_t left_row_capacity,
    prdb_sdk_sql_join_materialized_row_v16 *right_rows,
    uint32_t right_row_capacity,
    uint8_t *materialized_bytes, uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_join_result_row_v19_fn on_row, void *result_context) {
    return execute_inner_join_v19_with_page_limit(
        host, schema, schema_len, sql, sql_len, max_candidates,
        max_batch_bytes, left_planner_response,
        left_planner_response_capacity, right_planner_response,
        right_planner_response_capacity, left_executor_response,
        left_executor_response_capacity, right_executor_response,
        right_executor_response_capacity, batch_response,
        batch_response_capacity, left_planner_columns,
        left_planner_column_capacity, right_planner_columns,
        right_planner_column_capacity, left_executor_columns,
        left_executor_column_capacity, right_executor_columns,
        right_executor_column_capacity, cell_scratch,
        cell_scratch_capacity, left_rows, left_row_capacity, right_rows,
        right_row_capacity, materialized_bytes,
        materialized_bytes_capacity, on_row, result_context,
        SQL_EXEC_V19_MAX_SCAN_PAGES);
}

typedef struct join_v20_callback_context {
    const prdb_sdk_select_v20 *statement;
    const prdb_sdk_select_binding_v20 *binding;
    const prdb_sdk_value_v1 *parameters;
    uint32_t parameter_count;
    prdb_sdk_sql_join_result_row_v20_fn on_row;
    void *result_context;
    uint32_t emitted_rows;
    int32_t callback_failure;
} join_v20_callback_context;

static int32_t join_v20_filter_and_emit(
    void *context, const prdb_sdk_value_v1 *projected_values,
    uint32_t projection_count, const prdb_sdk_select_binding_v19 *binding) {
    join_v20_callback_context *state = context;
    prdb_sdk_select_binding_v20 callback_binding = *state->binding;
    uint32_t truth = PRDB_SDK_SQL_TRUTH_TRUE;
    int32_t status;

    if (projection_count < state->binding->base.base.base.projection_count) {
        state->callback_failure = PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH;
        return PRDB_QUERY_CALLBACK_INTERNAL_ERROR;
    }
    if (state->statement->base.base.base.has_limit != 0 &&
        state->emitted_rows >= state->statement->base.base.base.limit) {
        return PRDB_QUERY_CALLBACK_OK;
    }
    if (state->statement->has_where == 1) {
        const prdb_sdk_value_v1 *parameter = NULL;
        prdb_sdk_select_v1 predicate = {0};
        if (state->statement->parameter_index != 0) {
            if (state->statement->parameter_index > state->parameter_count ||
                state->parameters == NULL) {
                state->callback_failure = PRDB_SDK_SQL_EXEC_EVALUATION_ERROR;
                return PRDB_QUERY_CALLBACK_INTERNAL_ERROR;
            }
            parameter = &state->parameters[
                state->statement->parameter_index - 1];
        }
        predicate.struct_size = sizeof(predicate);
        predicate.abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
        predicate.sql = state->statement->base.base.base.sql;
        predicate.sql_len = state->statement->base.base.base.sql_len;
        predicate.has_where = 1;
        predicate.comparison = state->statement->comparison;
        predicate.parameter_index = state->statement->parameter_index;
        status = prdb_sdk_sql_eval_select_predicate_v1(
            &predicate,
            &projected_values[state->binding->where_projection_index],
            parameter, state->binding->where_column.type_code,
            state->binding->where_column.decimal_scale, &truth);
        if (status != PRDB_SDK_SQL_EVAL_OK) {
            state->callback_failure = PRDB_SDK_SQL_EXEC_EVALUATION_ERROR;
            return PRDB_QUERY_CALLBACK_INTERNAL_ERROR;
        }
        if (truth != PRDB_SDK_SQL_TRUTH_TRUE) {
            return PRDB_QUERY_CALLBACK_OK;
        }
    }
    callback_binding.base = *binding;
    callback_binding.base.base.base.projection_count =
        state->binding->output_projection_count;
    status = state->on_row(
        state->result_context, projected_values,
        state->binding->output_projection_count, &callback_binding);
    if (status != PRDB_QUERY_CALLBACK_OK) {
        state->callback_failure = PRDB_SDK_SQL_EXEC_RESULT_ERROR;
        return status;
    }
    ++state->emitted_rows;
    return PRDB_QUERY_CALLBACK_OK;
}

static int32_t execute_inner_join_v20_with_page_limit(
    const prdb_host_api_v3 *host,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters,
    const prdb_sdk_sql_parameter_metadata_v1 *metadata, uint32_t parameter_count,
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
    prdb_sdk_sql_join_materialized_row_v16 *left_rows,
    uint32_t left_row_capacity,
    prdb_sdk_sql_join_materialized_row_v16 *right_rows,
    uint32_t right_row_capacity,
    uint8_t *materialized_bytes, uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_join_result_row_v20_fn on_row, void *result_context,
    uint32_t scan_page_limit) {
    prdb_sdk_select_v20 statement = {0};
    prdb_sdk_select_binding_v20 binding = {0};
    prdb_sdk_select_v1 predicate = {0};
    prdb_sdk_value_v1 bound_parameter = {0};
    prdb_sdk_planner_relation_v1 planned_left = {0}, planned_right = {0};
    prdb_sdk_executor_relation_v1 resolved_left = {0}, resolved_right = {0};
    uint8_t synthetic_sql[PRDB_SDK_SQL_MAX_QUERY_BYTES];
    uint64_t synthetic_len = 0;
    uint64_t response_len = 0;
    uint32_t error_offset = 0;
    int32_t status;
    join_v20_callback_context callback = {0};
    prdb_host_api_v2 base_host;

    if (host == NULL) {
        return PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT;
    }
    if (host->struct_size < sizeof(*host) ||
        host->abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->executor_request == NULL || host->planner_request == NULL ||
        host->statement_failure == NULL || schema == NULL || sql == NULL ||
        on_row == NULL || (parameter_count != 0 && parameters == NULL)) {
        return fail_statement_v19(host, PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT);
    }
    base_host = (prdb_host_api_v2){
        sizeof(base_host), PRDB_QUERY_ABI_V2_VERSION, host->context,
        host->executor_request, host->planner_request, host->write_result};
    status = prdb_sdk_sql_parse_select_v20(
        sql, sql_len, &statement, sizeof(statement), &error_offset);
    if (status != PRDB_SDK_SQL_OK) {
        return fail_statement_v19(host, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    }
    status = prdb_sdk_planner_describe_relation_v1(
        &base_host, schema, schema_len,
        sql + statement.base.base.base.left_relation.offset,
        statement.base.base.base.left_relation.length,
        left_planner_response, left_planner_response_capacity, &response_len,
        &planned_left, sizeof(planned_left), left_planner_columns,
        left_planner_column_capacity);
    if (status != PRDB_SDK_PLANNER_REQUEST_OK) {
        return fail_statement_v19(host, PRDB_SDK_SQL_EXEC_PLANNER_ERROR);
    }
    status = prdb_sdk_planner_describe_relation_v1(
        &base_host, schema, schema_len,
        sql + statement.base.base.base.right_relation.offset,
        statement.base.base.base.right_relation.length,
        right_planner_response, right_planner_response_capacity, &response_len,
        &planned_right, sizeof(planned_right), right_planner_columns,
        right_planner_column_capacity);
    if (status != PRDB_SDK_PLANNER_REQUEST_OK) {
        return fail_statement_v19(host, PRDB_SDK_SQL_EXEC_PLANNER_ERROR);
    }
    status = prdb_sdk_executor_resolve_relation_v1(
        &base_host, schema, schema_len,
        sql + statement.base.base.base.left_relation.offset,
        statement.base.base.base.left_relation.length,
        left_executor_response, left_executor_response_capacity, &response_len,
        &resolved_left, sizeof(resolved_left), left_executor_columns,
        left_executor_column_capacity);
    if (status != PRDB_SDK_EXECUTOR_OK) {
        return fail_statement_v19(host, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    }
    status = prdb_sdk_executor_resolve_relation_v1(
        &base_host, schema, schema_len,
        sql + statement.base.base.base.right_relation.offset,
        statement.base.base.base.right_relation.length,
        right_executor_response, right_executor_response_capacity, &response_len,
        &resolved_right, sizeof(resolved_right), right_executor_columns,
        right_executor_column_capacity);
    if (status != PRDB_SDK_EXECUTOR_OK) {
        return fail_statement_v19(host, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    }
    if (!same_relation_shape_v19(&planned_left, &resolved_left,
                                left_planner_columns,
                                left_executor_columns) ||
        !same_relation_shape_v19(&planned_right, &resolved_right,
                                right_planner_columns,
                                right_executor_columns)) {
        return fail_statement_v19(host, PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH);
    }
    status = prdb_sdk_sql_bind_select_v20(
        &statement,
        resolved_left.name, resolved_left.name_len,
        left_executor_columns, resolved_left.column_count,
        resolved_right.name, resolved_right.name_len,
        right_executor_columns, resolved_right.column_count,
        &binding, sizeof(binding));
    if (status != PRDB_SDK_SQL_BIND_OK) {
        return fail_statement_v19(host, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    }
    if (statement.has_where == 1 && statement.parameter_index != 0) {
        status = bind_sql_parameter(
            statement.parameter_index, parameters, metadata, parameter_count,
            binding.where_column.type_code,
            binding.where_column.decimal_precision,
            binding.where_column.decimal_scale, &bound_parameter);
        if (status != PRDB_SDK_SQL_EVAL_OK) {
            return fail_statement_v19(host,
                                     PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
        }
    }
    if (statement.base.base.base.has_limit != 0 &&
        statement.base.base.base.limit == 0) {
        return PRDB_SDK_SQL_EXEC_OK;
    }

    if (statement.has_where == 1) {
        uint64_t used = 0;
        uint32_t filter_index = binding.where_projection_index;
        uint32_t output_count = binding.output_projection_count;
        uint32_t from_offset = statement.from_keyword_offset;
        uint32_t where_offset = statement.where_keyword_offset;
        uint32_t filter_relation_end = statement.where_column.relation.offset +
                                       statement.where_column.relation.length;
        uint32_t filter_column_end = statement.where_column.column.offset +
                                     statement.where_column.column.length;
#define APPEND_SQL_V20(ptr, len)                                               \
        do {                                                                   \
            uint64_t append_length = (uint64_t)(len);                          \
            if (append_length > sizeof(synthetic_sql) - used) {                \
                return fail_statement_v19(                                     \
                    host, PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT);             \
            }                                                                  \
            if (append_length != 0) {                                          \
                memcpy(synthetic_sql + used, (ptr), (size_t)append_length);    \
            }                                                                  \
            used += append_length;                                             \
        } while (0)
        if (filter_index > output_count || from_offset > where_offset ||
            where_offset > sql_len || filter_column_end > sql_len ||
            filter_relation_end > statement.where_column.column.offset) {
            return fail_statement_v19(host, PRDB_SDK_SQL_EXEC_BIND_ERROR);
        }
        APPEND_SQL_V20(sql, from_offset);
        if (filter_index == output_count) {
            static const uint8_t comma_space[] = ", ";
            static const uint8_t dot[] = ".";
            static const uint8_t space[] = " ";
            APPEND_SQL_V20(comma_space, sizeof(comma_space) - 1);
            APPEND_SQL_V20(sql + statement.where_column.relation.offset,
                           statement.where_column.relation.length);
            APPEND_SQL_V20(dot, sizeof(dot) - 1);
            APPEND_SQL_V20(sql + statement.where_column.column.offset,
                           statement.where_column.column.length);
            APPEND_SQL_V20(space, sizeof(space) - 1);
            ++output_count;
        }
        APPEND_SQL_V20(sql + from_offset, where_offset - from_offset);
#undef APPEND_SQL_V20
        if (used == 0 || used >= sizeof(synthetic_sql)) {
            return fail_statement_v19(host,
                                     PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT);
        }
        synthetic_sql[used] = ';';
        synthetic_len = used + 1;
    }

    callback.statement = &statement;
    callback.binding = &binding;
    callback.parameters = parameters;
    callback.parameter_count = parameter_count;
    callback.on_row = on_row;
    callback.result_context = result_context;
    status = execute_inner_join_v19_with_page_limit(
        host, schema, schema_len,
        statement.has_where == 1 ? synthetic_sql : sql,
        statement.has_where == 1 ? synthetic_len : sql_len,
        max_candidates, max_batch_bytes,
        left_planner_response, left_planner_response_capacity,
        right_planner_response, right_planner_response_capacity,
        left_executor_response, left_executor_response_capacity,
        right_executor_response, right_executor_response_capacity,
        batch_response, batch_response_capacity,
        left_planner_columns, left_planner_column_capacity,
        right_planner_columns, right_planner_column_capacity,
        left_executor_columns, left_executor_column_capacity,
        right_executor_columns, right_executor_column_capacity,
        cell_scratch, cell_scratch_capacity,
        left_rows, left_row_capacity, right_rows, right_row_capacity,
        materialized_bytes, materialized_bytes_capacity,
        join_v20_filter_and_emit, &callback, scan_page_limit);
    if (status == PRDB_SDK_SQL_EXEC_RESULT_ERROR &&
        callback.callback_failure != 0) {
        return fail_statement_v19(host, callback.callback_failure);
    }
    return status;
}

int32_t prdb_sdk_sql_execute_inner_join_v20(
    const prdb_host_api_v3 *host,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
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
    prdb_sdk_sql_join_materialized_row_v16 *left_rows,
    uint32_t left_row_capacity,
    prdb_sdk_sql_join_materialized_row_v16 *right_rows,
    uint32_t right_row_capacity,
    uint8_t *materialized_bytes, uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_join_result_row_v20_fn on_row, void *result_context) {
    return execute_inner_join_v20_with_page_limit(
        host, schema, schema_len, sql, sql_len, parameters, NULL, parameter_count,
        max_candidates, max_batch_bytes, left_planner_response,
        left_planner_response_capacity, right_planner_response,
        right_planner_response_capacity, left_executor_response,
        left_executor_response_capacity, right_executor_response,
        right_executor_response_capacity, batch_response,
        batch_response_capacity, left_planner_columns,
        left_planner_column_capacity, right_planner_columns,
        right_planner_column_capacity, left_executor_columns,
        left_executor_column_capacity, right_executor_columns,
        right_executor_column_capacity, cell_scratch, cell_scratch_capacity,
        left_rows, left_row_capacity, right_rows, right_row_capacity,
        materialized_bytes, materialized_bytes_capacity, on_row,
        result_context, SQL_EXEC_V20_MAX_SCAN_PAGES);
}

int32_t prdb_sdk_sql_execute_inner_join_typed_v20(
    const prdb_host_api_v3 *host,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters,
    const prdb_sdk_sql_parameter_metadata_v1 *metadata, uint32_t parameter_count,
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
    prdb_sdk_sql_join_materialized_row_v16 *left_rows,
    uint32_t left_row_capacity,
    prdb_sdk_sql_join_materialized_row_v16 *right_rows,
    uint32_t right_row_capacity,
    uint8_t *materialized_bytes, uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_join_result_row_v20_fn on_row, void *result_context) {
    return execute_inner_join_v20_with_page_limit(
        host, schema, schema_len, sql, sql_len, parameters, metadata, parameter_count,
        max_candidates, max_batch_bytes, left_planner_response,
        left_planner_response_capacity, right_planner_response,
        right_planner_response_capacity, left_executor_response,
        left_executor_response_capacity, right_executor_response,
        right_executor_response_capacity, batch_response,
        batch_response_capacity, left_planner_columns,
        left_planner_column_capacity, right_planner_columns,
        right_planner_column_capacity, left_executor_columns,
        left_executor_column_capacity, right_executor_columns,
        right_executor_column_capacity, cell_scratch, cell_scratch_capacity,
        left_rows, left_row_capacity, right_rows, right_row_capacity,
        materialized_bytes, materialized_bytes_capacity, on_row,
        result_context, SQL_EXEC_V20_MAX_SCAN_PAGES);
}

int32_t prdb_sdk_sql_execute_inner_join_optimizer_v1(
    const prdb_host_api_v5 *host,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *sql, uint64_t sql_len,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *optimizer_request, uint64_t optimizer_request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
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
    prdb_sdk_sql_join_materialized_row_v16 *left_rows,
    uint32_t left_row_capacity,
    prdb_sdk_sql_join_materialized_row_v16 *right_rows,
    uint32_t right_row_capacity,
    uint8_t *materialized_bytes, uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_join_result_row_v19_fn on_row, void *result_context) {
    prdb_sdk_select_v19 statement = {0};
    prdb_sdk_select_binding_v19 binding = {0};
    prdb_sdk_planner_relation_v1 planned_left = {0};
    prdb_sdk_planner_relation_v1 planned_right = {0};
    prdb_sdk_optimizer_response_v1 optimizer_result = {0};
    prdb_host_api_v2 base_host;
    uint64_t request_len = 0;
    uint64_t optimizer_response_len = 0;
    uint64_t planner_response_len = 0;
    uint64_t identity_response_len = 0;
    uint32_t error_offset = 0;
    int32_t status;

    if (host == NULL) return PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT;
    if (host->v4.v3.struct_size != sizeof(prdb_host_api_v3) ||
        host->v4.v3.abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->v4.v3.context == NULL ||
        host->v4.v3.statement_failure == NULL)
        return PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT;
    if (host->v4.v3.struct_size != sizeof(prdb_host_api_v3) ||
        host->v4.v3.abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->v4.v3.context == NULL || host->v4.v3.executor_request == NULL ||
        host->v4.v3.planner_request == NULL ||
        host->v4.v3.statement_failure == NULL || host->optimizer_request == NULL ||
        schema == NULL || sql == NULL || optimizer_request == NULL ||
        optimizer_response == NULL || left_planner_response == NULL ||
        right_planner_response == NULL || left_executor_response == NULL ||
        right_executor_response == NULL || batch_response == NULL ||
        left_planner_columns == NULL || right_planner_columns == NULL ||
        left_executor_columns == NULL || right_executor_columns == NULL ||
        cell_scratch == NULL || left_rows == NULL || right_rows == NULL ||
        materialized_bytes == NULL || on_row == NULL) {
        return fail_statement_v19(&host->v4.v3,
                                  PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT);
    }
    base_host = (prdb_host_api_v2){
        sizeof(base_host), PRDB_QUERY_ABI_V2_VERSION, host->v4.v3.context,
        host->v4.v3.executor_request, host->v4.v3.planner_request,
        host->v4.v3.write_result};
    status = prdb_sdk_sql_parse_select_v19(
        sql, sql_len, &statement, sizeof(statement), &error_offset);
    if (status != PRDB_SDK_SQL_OK)
        return fail_statement_v19(&host->v4.v3,
                                  PRDB_SDK_SQL_EXEC_BIND_ERROR);
    status = prdb_sdk_optimizer_encode_join_select_v1(
        &statement, schema, schema_len, optimizer_request,
        optimizer_request_capacity, &request_len);
    if (status != PRDB_SDK_OPTIMIZER_ENCODE_OK)
        return fail_statement_v19(&host->v4.v3,
                                  PRDB_SDK_SQL_EXEC_BIND_ERROR);
    status = prdb_sdk_optimizer_submit_join_select_v1(
        host, optimizer_request, request_len, optimizer_response,
        optimizer_response_capacity, &optimizer_response_len,
        &optimizer_result, sizeof(optimizer_result));
    if (status != PRDB_SDK_OPTIMIZER_RESULT_OK ||
        optimizer_result.status != PRDB_SDK_OPTIMIZER_STATUS_SUCCESS ||
        optimizer_result.plan == NULL)
        return fail_statement_v19(&host->v4.v3,
                                  PRDB_SDK_SQL_EXEC_PLANNER_ERROR);

    status = prdb_sdk_planner_describe_relation_v1(
        &base_host, schema, schema_len,
        sql + statement.base.base.left_relation.offset,
        statement.base.base.left_relation.length, left_planner_response,
        left_planner_response_capacity, &planner_response_len, &planned_left,
        sizeof(planned_left), left_planner_columns,
        left_planner_column_capacity);
    if (status != PRDB_SDK_PLANNER_REQUEST_OK)
        return fail_statement_v19(&host->v4.v3,
                                  PRDB_SDK_SQL_EXEC_PLANNER_ERROR);
    status = prdb_sdk_planner_describe_relation_v1(
        &base_host, schema, schema_len,
        sql + statement.base.base.right_relation.offset,
        statement.base.base.right_relation.length, right_planner_response,
        right_planner_response_capacity, &planner_response_len, &planned_right,
        sizeof(planned_right), right_planner_columns,
        right_planner_column_capacity);
    if (status != PRDB_SDK_PLANNER_REQUEST_OK)
        return fail_statement_v19(&host->v4.v3,
                                  PRDB_SDK_SQL_EXEC_PLANNER_ERROR);
    status = prdb_sdk_sql_bind_select_v19(
        &statement, planned_left.name, planned_left.name_len,
        left_planner_columns, planned_left.column_count, planned_right.name,
        planned_right.name_len, right_planner_columns,
        planned_right.column_count, &binding, sizeof(binding));
    if (status != PRDB_SDK_SQL_BIND_OK)
        return fail_statement_v19(&host->v4.v3,
                                  PRDB_SDK_SQL_EXEC_BIND_ERROR);
    status = prdb_sdk_optimizer_match_join_plan_v19_v1(
        &statement, &binding, schema, schema_len, optimizer_result.plan,
        optimizer_result.plan_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK)
        return fail_statement_v19(&host->v4.v3,
                                  PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH);
    status = prdb_sdk_optimizer_validate_executor_join_relations_v1(
        &base_host, optimizer_result.plan, optimizer_result.plan_len,
        left_executor_response, left_executor_response_capacity,
        &identity_response_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK)
        return fail_statement_v19(&host->v4.v3,
                                  PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH);

    return execute_inner_join_v19_with_page_limit(
        &host->v4.v3, schema, schema_len, sql, sql_len, max_candidates,
        max_batch_bytes, left_planner_response,
        left_planner_response_capacity, right_planner_response,
        right_planner_response_capacity, left_executor_response,
        left_executor_response_capacity, right_executor_response,
        right_executor_response_capacity, batch_response,
        batch_response_capacity, left_planner_columns,
        left_planner_column_capacity, right_planner_columns,
        right_planner_column_capacity, left_executor_columns,
        left_executor_column_capacity, right_executor_columns,
        right_executor_column_capacity, cell_scratch, cell_scratch_capacity,
        left_rows, left_row_capacity, right_rows, right_row_capacity,
        materialized_bytes, materialized_bytes_capacity, on_row,
        result_context, SQL_EXEC_V19_OPTIMIZER_MAX_SCAN_PAGES);
}

int32_t prdb_sdk_sql_execute_inner_join_optimizer_v20(
    const prdb_host_api_v5 *host,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *optimizer_request, uint64_t optimizer_request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
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
    prdb_sdk_sql_join_materialized_row_v16 *left_rows,
    uint32_t left_row_capacity,
    prdb_sdk_sql_join_materialized_row_v16 *right_rows,
    uint32_t right_row_capacity,
    uint8_t *materialized_bytes, uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_join_result_row_v20_fn on_row, void *result_context) {
    prdb_sdk_select_v20 statement = {0};
    prdb_sdk_select_binding_v20 binding = {0};
    prdb_sdk_planner_relation_v1 planned_left = {0};
    prdb_sdk_planner_relation_v1 planned_right = {0};
    prdb_sdk_optimizer_response_v1 optimizer_result = {0};
    prdb_host_api_v2 base_host;
    uint64_t request_len = 0;
    uint64_t optimizer_response_len = 0;
    uint64_t planner_response_len = 0;
    uint64_t identity_response_len = 0;
    uint32_t error_offset = 0;
    int32_t status;

    if (host == NULL) return PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT;
    if (host->v4.v3.struct_size != sizeof(prdb_host_api_v3) ||
        host->v4.v3.abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->v4.v3.context == NULL ||
        host->v4.v3.statement_failure == NULL)
        return PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT;
    if (host->v4.v3.executor_request == NULL ||
        host->v4.v3.planner_request == NULL || host->optimizer_request == NULL ||
        schema == NULL || sql == NULL || optimizer_request == NULL ||
        optimizer_response == NULL || left_planner_response == NULL ||
        right_planner_response == NULL || left_executor_response == NULL ||
        right_executor_response == NULL || batch_response == NULL ||
        left_planner_columns == NULL || right_planner_columns == NULL ||
        left_executor_columns == NULL || right_executor_columns == NULL ||
        cell_scratch == NULL || left_rows == NULL || right_rows == NULL ||
        materialized_bytes == NULL || on_row == NULL)
        return fail_statement_v19(&host->v4.v3,
                                  PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT);
    base_host = (prdb_host_api_v2){
        sizeof(base_host), PRDB_QUERY_ABI_V2_VERSION, host->v4.v3.context,
        host->v4.v3.executor_request, host->v4.v3.planner_request,
        host->v4.v3.write_result};
    status = prdb_sdk_sql_parse_select_v20(
        sql, sql_len, &statement, sizeof(statement), &error_offset);
    if (status != PRDB_SDK_SQL_OK || statement.has_where != 1)
        return fail_statement_v19(&host->v4.v3,
                                  PRDB_SDK_SQL_EXEC_BIND_ERROR);
    status = prdb_sdk_optimizer_encode_filtered_join_select_v1(
        &statement, schema, schema_len, optimizer_request,
        optimizer_request_capacity, &request_len);
    if (status != PRDB_SDK_OPTIMIZER_ENCODE_OK)
        return fail_statement_v19(&host->v4.v3,
                                  PRDB_SDK_SQL_EXEC_BIND_ERROR);
    status = prdb_sdk_optimizer_submit_join_select_v1(
        host, optimizer_request, request_len, optimizer_response,
        optimizer_response_capacity, &optimizer_response_len,
        &optimizer_result, sizeof(optimizer_result));
    if (status != PRDB_SDK_OPTIMIZER_RESULT_OK ||
        optimizer_result.status != PRDB_SDK_OPTIMIZER_STATUS_SUCCESS ||
        optimizer_result.plan == NULL)
        return fail_statement_v19(&host->v4.v3,
                                  PRDB_SDK_SQL_EXEC_PLANNER_ERROR);

    status = prdb_sdk_planner_describe_relation_v1(
        &base_host, schema, schema_len,
        sql + statement.base.base.base.left_relation.offset,
        statement.base.base.base.left_relation.length, left_planner_response,
        left_planner_response_capacity, &planner_response_len, &planned_left,
        sizeof(planned_left), left_planner_columns,
        left_planner_column_capacity);
    if (status != PRDB_SDK_PLANNER_REQUEST_OK)
        return fail_statement_v19(&host->v4.v3,
                                  PRDB_SDK_SQL_EXEC_PLANNER_ERROR);
    status = prdb_sdk_planner_describe_relation_v1(
        &base_host, schema, schema_len,
        sql + statement.base.base.base.right_relation.offset,
        statement.base.base.base.right_relation.length,
        right_planner_response, right_planner_response_capacity,
        &planner_response_len, &planned_right, sizeof(planned_right),
        right_planner_columns, right_planner_column_capacity);
    if (status != PRDB_SDK_PLANNER_REQUEST_OK)
        return fail_statement_v19(&host->v4.v3,
                                  PRDB_SDK_SQL_EXEC_PLANNER_ERROR);
    status = prdb_sdk_sql_bind_select_v20(
        &statement, planned_left.name, planned_left.name_len,
        left_planner_columns, planned_left.column_count, planned_right.name,
        planned_right.name_len, right_planner_columns,
        planned_right.column_count, &binding, sizeof(binding));
    if (status != PRDB_SDK_SQL_BIND_OK)
        return fail_statement_v19(&host->v4.v3,
                                  PRDB_SDK_SQL_EXEC_BIND_ERROR);
    status = prdb_sdk_optimizer_match_filtered_join_plan_v20_v1(
        &statement, &binding, parameters, parameter_count, schema, schema_len,
        optimizer_result.plan, optimizer_result.plan_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK)
        return fail_statement_v19(&host->v4.v3,
                                  PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH);
    status = prdb_sdk_optimizer_validate_executor_filtered_join_relations_v1(
        &base_host, optimizer_result.plan, optimizer_result.plan_len,
        left_executor_response, left_executor_response_capacity,
        &identity_response_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK)
        return fail_statement_v19(&host->v4.v3,
                                  PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH);
    return execute_inner_join_v20_with_page_limit(
        &host->v4.v3, schema, schema_len, sql, sql_len, parameters, NULL,
        parameter_count, max_candidates, max_batch_bytes,
        left_planner_response, left_planner_response_capacity,
        right_planner_response, right_planner_response_capacity,
        left_executor_response, left_executor_response_capacity,
        right_executor_response, right_executor_response_capacity,
        batch_response, batch_response_capacity, left_planner_columns,
        left_planner_column_capacity, right_planner_columns,
        right_planner_column_capacity, left_executor_columns,
        left_executor_column_capacity, right_executor_columns,
        right_executor_column_capacity, cell_scratch, cell_scratch_capacity,
        left_rows, left_row_capacity, right_rows, right_row_capacity,
        materialized_bytes, materialized_bytes_capacity, on_row,
        result_context, SQL_EXEC_V20_OPTIMIZER_MAX_SCAN_PAGES);
}
