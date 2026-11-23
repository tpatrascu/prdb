#include "prdb_sdk_sql_exec_v19.h"
#include "prdb_sdk_sql_exec_v20.h"
#include "prdb_sdk_sql_spill_join_v1.h"
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
#define SQL_EXEC_V20_SPILL_MAX_SCAN_PAGES UINT32_C(112)
#define SQL_EXEC_V20_OPTIMIZER_MAX_SCAN_PAGES UINT32_C(117)
#define SQL_EXEC_V19_JOIN_POLL_INTERVAL UINT32_C(2048)
#define SQL_EXEC_V19_INDEX_OUTER_MAX_ROWS UINT32_C(128)
#define SQL_EXEC_V19_INDEX_CANDIDATE_LIMIT UINT32_C(16384)
#define SQL_EXEC_V19_INDEX_MAX_SCAN_PAGES UINT32_C(48)
#define SQL_EXEC_V19_INDEX_REQUEST_BYTES UINT32_C(16384)

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

static int32_t execute_index_nested_loop_v19(
    const prdb_host_api_v3 *host_v3, const prdb_host_api_v2 *host,
    const prdb_sdk_select_binding_v19 *binding,
    const prdb_sdk_optimizer_index_join_plan_v1 *plan,
    const prdb_sdk_executor_relation_v1 *left_relation,
    const prdb_sdk_executor_relation_v1 *right_relation,
    const prdb_sdk_sql_join_materialized_row_v16 *outer_rows,
    uint32_t outer_count, uint8_t *inner_response,
    uint64_t inner_response_capacity, uint8_t *materialized_bytes,
    uint64_t materialized_capacity, uint64_t *materialized_used,
    prdb_sdk_value_v1 *cell_scratch, uint32_t cell_scratch_capacity,
    prdb_sdk_sql_join_result_row_v19_fn on_row, void *result_context,
    uint32_t has_limit, uint32_t limit) {
    uint8_t request[SQL_EXEC_V19_INDEX_REQUEST_BYTES];
    uint8_t packed_ids[PRDB_SDK_EXECUTOR_MAX_COLUMN_IDS * 16];
    uint32_t inner_side = plan->inner_side;
    uint32_t outer_side = plan->outer_side;
    const prdb_sdk_executor_relation_v1 *inner_relation =
        inner_side == PRDB_SDK_SQL_V8_LEFT ? left_relation : right_relation;
    const prdb_sdk_optimizer_join_relation_v1 *inner_shape =
        inner_side == PRDB_SDK_SQL_V8_LEFT ? &plan->join.left_relation
                                           : &plan->join.right_relation;
    uint32_t key_index = UINT32_MAX;
    uint32_t emitted = 0, candidates = 0;
    int32_t status;

    if (binding->base.join_key_count == 0 ||
        inner_shape->scan_column_count > PRDB_SDK_EXECUTOR_MAX_COLUMN_IDS ||
        inner_shape->scan_column_count == 0 || outer_count >
            SQL_EXEC_V19_INDEX_OUTER_MAX_ROWS) {
        return fail_statement_v19(host_v3, PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH);
    }
    for (uint32_t key = 0; key < binding->base.join_key_count; ++key) {
        const prdb_sdk_sql_bound_column_v2 *inner_column =
            inner_side == PRDB_SDK_SQL_V8_LEFT
                ? &binding->base.join_keys[key].left : &binding->base.join_keys[key].right;
        if (inner_column->ordinal == plan->key_column.ordinal &&
            inner_column->type_code == plan->key_column.type_code) {
            key_index = key;
            break;
        }
    }
    if (key_index == UINT32_MAX) {
        return fail_statement_v19(host_v3, PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH);
    }
    for (uint32_t column = 0; column < inner_shape->scan_column_count; ++column) {
        if (inner_shape->scan_columns[column].ordinal >= cell_scratch_capacity) {
            return fail_statement_v19(host_v3, PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH);
        }
        memcpy(packed_ids + column * 16,
               inner_shape->scan_columns[column].column_id, 16);
    }
    for (uint32_t outer_index = 0; outer_index < outer_count; ++outer_index) {
        const prdb_sdk_value_v1 *probe = &outer_rows[outer_index].join_keys[key_index];
        prdb_sdk_index_range_v1 range = {0};
        prdb_sdk_index_range_row_iterator_v1 rows = {0};
        uint64_t response_len = 0;
        if ((outer_index % 16) == 0) {
            status = check_cancelled_v19(host);
            if (status != PRDB_SDK_SQL_EXEC_OK)
                return fail_statement_v19(host_v3, status);
        }
        if (probe->state == PRDB_SDK_VALUE_STATE_NULL) continue;
        if (probe->state != PRDB_SDK_VALUE_STATE_VALUE ||
            probe->type_code != plan->key_column.type_code ||
            probe->data_len > PRDB_SDK_VALUE_MAX_BYTES)
            return fail_statement_v19(host_v3, PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
        status = prdb_sdk_executor_scan_index_range_v1(
            host, inner_relation->executor_handle, plan->index_id, probe, 1,
            probe, 1, packed_ids, (uint16_t)inner_shape->scan_column_count,
            request, sizeof(request), inner_response, inner_response_capacity,
            &response_len, &range, sizeof(range));
        if (status != PRDB_SDK_EXECUTOR_OK)
            return fail_statement_v19(host_v3,
                status == PRDB_SDK_EXECUTOR_UNAVAILABLE
                    ? PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT
                    : PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
        if (prdb_sdk_index_range_rows_begin_v1(&range, &rows, sizeof(rows)) !=
            PRDB_SDK_BATCH_OK)
            return fail_statement_v19(host_v3, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
        while (rows.rows_remaining != 0) {
            prdb_sdk_index_range_row_v1 row = {0};
            prdb_sdk_cell_iterator_v1 cells = {0};
            prdb_sdk_value_v1 left_keys[8] = {{0}}, right_keys[8] = {{0}};
            prdb_sdk_value_v1 inner_keys[8] = {{0}};
            prdb_sdk_value_v1 projected[PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS] = {{0}};
            uint32_t matches = 1;
            if (++candidates > SQL_EXEC_V19_INDEX_CANDIDATE_LIMIT)
                return fail_statement_v19(host_v3, PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT);
            if ((candidates % 1024) == 0) {
                status = check_cancelled_v19(host);
                if (status != PRDB_SDK_SQL_EXEC_OK)
                    return fail_statement_v19(host_v3, status);
            }
            if (prdb_sdk_index_range_row_next_v1(&rows, &row, sizeof(row)) !=
                    PRDB_SDK_BATCH_OK || row.cell_count != inner_shape->scan_column_count ||
                prdb_sdk_index_range_row_cells_begin_v1(&row, &cells, sizeof(cells)) !=
                    PRDB_SDK_BATCH_OK)
                return fail_statement_v19(host_v3, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
            for (uint32_t i = 0; i < row.cell_count; ++i) {
                prdb_sdk_value_v1 value = {0};
                const prdb_sdk_optimizer_plan_column_v1 *column = &inner_shape->scan_columns[i];
                if (prdb_sdk_cell_next_v1(&cells, &value, sizeof(value)) != PRDB_SDK_BATCH_OK ||
                    value.type_code != column->type_code || column->ordinal >= cell_scratch_capacity)
                    return fail_statement_v19(host_v3, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
                cell_scratch[column->ordinal] = value;
            }
            for (uint32_t key = 0; key < binding->base.join_key_count; ++key) {
                const prdb_sdk_sql_join_key_binding_v16 *key_binding = &binding->base.join_keys[key];
                const prdb_sdk_value_v1 *outer_key = &outer_rows[outer_index].join_keys[key];
                const prdb_sdk_sql_bound_column_v2 *inner_key =
                    inner_side == PRDB_SDK_SQL_V8_LEFT ? &key_binding->left : &key_binding->right;
                const prdb_sdk_value_v1 *inner_value = &cell_scratch[inner_key->ordinal];
                uint32_t truth = PRDB_SDK_SQL_TRUTH_UNKNOWN;
                status = copy_value_v19(inner_value, &inner_keys[key],
                    materialized_bytes, materialized_capacity, materialized_used);
                if (status != PRDB_SDK_SQL_EXEC_OK)
                    return fail_statement_v19(host_v3, status);
                if (outer_side == PRDB_SDK_SQL_V8_LEFT) {
                    left_keys[key] = *outer_key; right_keys[key] = inner_keys[key];
                } else { left_keys[key] = inner_keys[key]; right_keys[key] = *outer_key; }
                status = prdb_sdk_sql_compare_v1(&left_keys[key], &right_keys[key],
                    PRDB_SDK_SQL_COMPARE_EQUAL, key_binding->left.decimal_scale,
                    key_binding->right.decimal_scale, &truth);
                if (status != PRDB_SDK_SQL_EVAL_OK)
                    return fail_statement_v19(host_v3, PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
                if (truth != PRDB_SDK_SQL_TRUTH_TRUE) { matches = 0; break; }
            }
            if (!matches) continue;
            for (uint32_t projection = 0; projection < binding->base.base.projection_count; ++projection) {
                const prdb_sdk_sql_join_projection_binding_v8 *p = &binding->base.base.projections[projection];
                if (p->relation_side == outer_side) {
                    projected[projection] = outer_rows[outer_index].projections[projection];
                } else {
                    status = copy_value_v19(&cell_scratch[p->column.ordinal],
                        &projected[projection], materialized_bytes,
                        materialized_capacity, materialized_used);
                    if (status != PRDB_SDK_SQL_EXEC_OK)
                        return fail_statement_v19(host_v3, status);
                }
            }
            if (on_row(result_context, projected, binding->base.base.projection_count, binding) !=
                PRDB_QUERY_CALLBACK_OK)
                return fail_statement_v19(host_v3, PRDB_SDK_SQL_EXEC_RESULT_ERROR);
            if (plan->has_filter == 0 && ++emitted >= limit && has_limit)
                return PRDB_SDK_SQL_EXEC_OK;
        }
    }
    return PRDB_SDK_SQL_EXEC_OK;
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
    uint32_t max_scan_pages,
    const prdb_sdk_optimizer_index_join_plan_v1 *index_plan) {
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

    if (index_plan != NULL) {
        const uint32_t outer_side = index_plan->outer_side;
        const prdb_sdk_executor_relation_v1 *outer_relation =
            outer_side == PRDB_SDK_SQL_V8_LEFT ? &resolved_left : &resolved_right;
        prdb_sdk_sql_join_materialized_row_v16 *outer_rows =
            outer_side == PRDB_SDK_SQL_V8_LEFT ? left_rows : right_rows;
        uint32_t outer_row_capacity = outer_side == PRDB_SDK_SQL_V8_LEFT
            ? left_row_capacity : right_row_capacity;
        uint32_t outer_row_count = 0;
        if (outer_row_capacity > SQL_EXEC_V19_INDEX_OUTER_MAX_ROWS)
            outer_row_capacity = SQL_EXEC_V19_INDEX_OUTER_MAX_ROWS;
        status = materialize_relation_v19(
            host, outer_relation, &binding.base, outer_side, max_candidates,
            max_batch_bytes,
            outer_side == PRDB_SDK_SQL_V8_LEFT ? left_executor_response : right_executor_response,
            outer_side == PRDB_SDK_SQL_V8_LEFT ? left_executor_response_capacity : right_executor_response_capacity,
            batch_response, batch_response_capacity, cell_scratch,
            cell_scratch_capacity, outer_rows, outer_row_capacity,
            &outer_row_count, &total_pages, SQL_EXEC_V19_INDEX_MAX_SCAN_PAGES,
            materialized_bytes, materialized_bytes_capacity, &materialized_used);
        if (status != PRDB_SDK_SQL_EXEC_OK)
            return fail_statement_v19(host_v3, status);
        return execute_index_nested_loop_v19(
            host_v3, host, &binding, index_plan, &resolved_left,
            &resolved_right, outer_rows, outer_row_count,
            index_plan->inner_side == PRDB_SDK_SQL_V8_LEFT
                ? left_executor_response : right_executor_response,
            index_plan->inner_side == PRDB_SDK_SQL_V8_LEFT
                ? left_executor_response_capacity : right_executor_response_capacity,
            materialized_bytes, materialized_bytes_capacity, &materialized_used,
            cell_scratch, cell_scratch_capacity, on_row, result_context,
            statement.base.base.has_limit, statement.base.base.limit);
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
        SQL_EXEC_V19_MAX_SCAN_PAGES, NULL);
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

#define SQL_SPILL_JOIN_RECORD_BYTES PRDB_SDK_SQL_SPILL_JOIN_MAX_RECORD_BYTES

typedef struct sql_spill_join_scan_v21 {
    const prdb_sdk_executor_relation_v1 *relation;
    const prdb_sdk_select_binding_v19 *binding;
    const prdb_host_api_v2 *host;
    uint32_t relation_side;
    uint16_t max_candidates;
    uint32_t max_batch_bytes;
    uint8_t *executor_response;
    uint64_t executor_response_capacity;
    uint8_t *batch_response;
    uint64_t batch_response_capacity;
    prdb_sdk_value_v1 *cell_scratch;
    uint32_t cell_scratch_capacity;
    uint32_t *total_pages;
    uint32_t max_scan_pages;
    uint8_t cursor[16];
    uint8_t cursor_open;
    uint8_t source_started;
    uint8_t source_finished;
    uint8_t batch_ready;
    prdb_sdk_batch_v1 batch;
    prdb_sdk_row_iterator_v1 row_iterator;
    int32_t failure_status;
} sql_spill_join_scan_v21;

typedef struct sql_spill_join_compare_v21 {
    const prdb_sdk_select_binding_v19 *binding;
} sql_spill_join_compare_v21;

typedef struct sql_spill_join_output_v21 {
    join_v20_callback_context *callback;
    const prdb_sdk_select_binding_v19 *binding;
} sql_spill_join_output_v21;

static int32_t join_v20_filter_and_emit(
    void *context, const prdb_sdk_value_v1 *projected_values,
    uint32_t projection_count, const prdb_sdk_select_binding_v19 *binding);

static void spill_write_u32_v21(uint8_t *bytes, uint32_t value) {
    bytes[0] = (uint8_t)(value >> 24);
    bytes[1] = (uint8_t)(value >> 16);
    bytes[2] = (uint8_t)(value >> 8);
    bytes[3] = (uint8_t)value;
}

static uint32_t spill_read_u32_v21(const uint8_t *bytes) {
    return ((uint32_t)bytes[0] << 24) | ((uint32_t)bytes[1] << 16) |
           ((uint32_t)bytes[2] << 8) | bytes[3];
}

static int32_t spill_encode_value_v21(const prdb_sdk_value_v1 *value,
                                      uint8_t *record, uint32_t *offset) {
    uint64_t data_len = value->state == PRDB_SDK_VALUE_STATE_VALUE
                            ? value->data_len
                            : 0;
    if (prdb_sdk_value_validate_v1(value) != PRDB_SDK_VALUE_VALID ||
        *offset > SQL_SPILL_JOIN_RECORD_BYTES - 12 ||
        data_len > SQL_SPILL_JOIN_RECORD_BYTES - *offset - 12) {
        return PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT;
    }
    spill_write_u32_v21(record + *offset, value->state);
    spill_write_u32_v21(record + *offset + 4, value->type_code);
    spill_write_u32_v21(record + *offset + 8, (uint32_t)data_len);
    *offset += 12;
    if (data_len != 0) {
        memcpy(record + *offset, value->data, (size_t)data_len);
        *offset += (uint32_t)data_len;
    }
    return PRDB_SDK_SQL_EXEC_OK;
}

static int32_t spill_decode_value_v21(const uint8_t *record,
                                      uint32_t record_bytes,
                                      uint32_t *offset,
                                      prdb_sdk_value_v1 *value) {
    uint32_t state, type, data_len;
    if (*offset > record_bytes || record_bytes - *offset < 12) {
        return PRDB_SDK_SQL_EXEC_EVALUATION_ERROR;
    }
    state = spill_read_u32_v21(record + *offset);
    type = spill_read_u32_v21(record + *offset + 4);
    data_len = spill_read_u32_v21(record + *offset + 8);
    *offset += 12;
    if (data_len > record_bytes - *offset ||
        (state != PRDB_SDK_VALUE_STATE_VALUE && data_len != 0)) {
        return PRDB_SDK_SQL_EXEC_EVALUATION_ERROR;
    }
    *value = (prdb_sdk_value_v1){
        sizeof(*value), PRDB_SDK_VALUE_ABI_V1_VERSION, state, type,
        data_len == 0 ? NULL : record + *offset, data_len};
    *offset += data_len;
    return prdb_sdk_value_validate_v1(value) == PRDB_SDK_VALUE_VALID
               ? PRDB_SDK_SQL_EXEC_OK
               : PRDB_SDK_SQL_EXEC_EVALUATION_ERROR;
}

static int32_t spill_decode_row_v21(
    const uint8_t *record, uint32_t record_bytes,
    const prdb_sdk_select_binding_v19 *binding,
    prdb_sdk_sql_join_materialized_row_v16 *row) {
    uint32_t offset = 8;
    uint32_t key_count = record[0];
    uint32_t projection_count = record[1];
    if (record_bytes != SQL_SPILL_JOIN_RECORD_BYTES ||
        key_count != binding->base.join_key_count ||
        projection_count != binding->base.base.projection_count ||
        record[2] != 0 || record[3] != 0 || record[4] != 0 ||
        record[5] != 0 || record[6] != 0 || record[7] != 0) {
        return PRDB_SDK_SQL_EXEC_EVALUATION_ERROR;
    }
    memset(row, 0, sizeof(*row));
    for (uint32_t key = 0; key < key_count; ++key) {
        int32_t status = spill_decode_value_v21(
            record, record_bytes, &offset, &row->join_keys[key]);
        if (status != PRDB_SDK_SQL_EXEC_OK) return status;
    }
    for (uint32_t projection = 0; projection < projection_count; ++projection) {
        int32_t status = spill_decode_value_v21(
            record, record_bytes, &offset, &row->projections[projection]);
        if (status != PRDB_SDK_SQL_EXEC_OK) return status;
    }
    return PRDB_SDK_SQL_EXEC_OK;
}

static int32_t spill_join_open_scan_v21(sql_spill_join_scan_v21 *scan) {
    uint64_t response_len = 0;
    int32_t status = prdb_sdk_executor_open_full_scan_v1(
        scan->host, scan->relation->executor_handle,
        scan->executor_response, scan->executor_response_capacity,
        &response_len, scan->cursor);
    if (status != PRDB_SDK_EXECUTOR_OK) {
        scan->failure_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
        return -1;
    }
    scan->cursor_open = 1;
    scan->source_started = 1;
    return 0;
}

static int32_t spill_join_next_batch_v21(sql_spill_join_scan_v21 *scan) {
    uint64_t response_len = 0;
    int32_t status;
    if (*scan->total_pages >= scan->max_scan_pages) {
        scan->failure_status = PRDB_SDK_SQL_EXEC_REQUEST_LIMIT;
        return -1;
    }
    status = check_cancelled_v19(scan->host);
    if (status != PRDB_SDK_SQL_EXEC_OK) {
        scan->failure_status = status;
        return -1;
    }
    status = prdb_sdk_executor_next_batch_v1(
        scan->host, scan->cursor, scan->max_candidates,
        scan->max_batch_bytes, scan->batch_response,
        scan->batch_response_capacity, &response_len, &scan->batch,
        sizeof(scan->batch));
    if (status != PRDB_SDK_EXECUTOR_OK) {
        scan->failure_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
        return -1;
    }
    ++*scan->total_pages;
    scan->source_finished = scan->batch.finished;
    if (scan->batch.finished) {
        scan->cursor_open = 0;
    } else {
        memcpy(scan->cursor, scan->batch.continuation,
               sizeof(scan->cursor));
    }
    status = prdb_sdk_batch_rows_begin_v1(&scan->batch, &scan->row_iterator,
                                         sizeof(scan->row_iterator));
    if (status != PRDB_SDK_BATCH_OK) {
        scan->failure_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
        return -1;
    }
    scan->batch_ready = 1;
    return 0;
}

static int32_t spill_join_source_v21(void *opaque, uint8_t *record,
                                     uint32_t capacity,
                                     uint8_t *has_record_out) {
    sql_spill_join_scan_v21 *scan = (sql_spill_join_scan_v21 *)opaque;
    uint32_t offset;
    if (scan == NULL || record == NULL || has_record_out == NULL ||
        capacity != SQL_SPILL_JOIN_RECORD_BYTES) {
        return -1;
    }
    *has_record_out = 0;
    if (!scan->source_started) {
        if (spill_join_open_scan_v21(scan) != 0) return -1;
    }
    for (;;) {
        prdb_sdk_row_v1 row = {0};
        prdb_sdk_cell_iterator_v1 cell_iterator = {0};
        if (!scan->batch_ready || scan->row_iterator.rows_remaining == 0) {
            scan->batch_ready = 0;
            if (scan->source_finished) return 0;
            if (!scan->cursor_open && !scan->source_finished) {
                scan->failure_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
                return -1;
            }
            if (spill_join_next_batch_v21(scan) != 0) return -1;
            if (scan->row_iterator.rows_remaining == 0) {
                continue;
            }
        }
        if (prdb_sdk_batch_row_next_v1(&scan->row_iterator, &row,
                                       sizeof(row)) != PRDB_SDK_BATCH_OK ||
            row.cell_count != scan->relation->column_count ||
            prdb_sdk_row_cells_begin_v1(&row, &cell_iterator,
                                        sizeof(cell_iterator)) !=
                PRDB_SDK_BATCH_OK) {
            scan->failure_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
            return -1;
        }
        for (uint32_t ordinal = 0; ordinal < scan->relation->column_count;
             ++ordinal) {
            if (prdb_sdk_cell_next_v1(
                    &cell_iterator, &scan->cell_scratch[ordinal],
                    sizeof(scan->cell_scratch[ordinal])) != PRDB_SDK_BATCH_OK) {
                scan->failure_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
                return -1;
            }
        }
        memset(record, 0, capacity);
        record[0] = (uint8_t)scan->binding->base.join_key_count;
        record[1] = (uint8_t)scan->binding->base.base.projection_count;
        offset = 8;
        for (uint32_t key = 0; key < scan->binding->base.join_key_count;
             ++key) {
            const prdb_sdk_sql_join_key_binding_v16 *key_binding =
                &scan->binding->base.join_keys[key];
            uint32_t ordinal = scan->relation_side == PRDB_SDK_SQL_V8_LEFT
                                   ? key_binding->left.ordinal
                                   : key_binding->right.ordinal;
            const prdb_sdk_value_v1 *value = &scan->cell_scratch[ordinal];
            if (value->state == PRDB_SDK_VALUE_STATE_REDACTED) {
                scan->failure_status = PRDB_SDK_SQL_EXEC_EVALUATION_ERROR;
                return -1;
            }
            scan->failure_status = spill_encode_value_v21(value, record,
                                                          &offset);
            if (scan->failure_status != PRDB_SDK_SQL_EXEC_OK) return -1;
        }
        for (uint32_t projection = 0;
             projection < scan->binding->base.base.projection_count;
             ++projection) {
            const prdb_sdk_sql_join_projection_binding_v8 *projection_binding =
                &scan->binding->base.base.projections[projection];
            prdb_sdk_value_v1 null_value = {0};
            const prdb_sdk_value_v1 *value;
            if (projection_binding->relation_side == scan->relation_side) {
                value = &scan->cell_scratch[
                    projection_binding->column.ordinal];
            } else {
                null_value = (prdb_sdk_value_v1){
                    sizeof(null_value), PRDB_SDK_VALUE_ABI_V1_VERSION,
                    PRDB_SDK_VALUE_STATE_NULL, 0, NULL, 0};
                value = &null_value;
            }
            scan->failure_status = spill_encode_value_v21(value, record,
                                                          &offset);
            if (scan->failure_status != PRDB_SDK_SQL_EXEC_OK) return -1;
        }
        *has_record_out = 1;
        return 0;
    }
}

static int32_t spill_join_hash_v21(void *opaque, const uint8_t *record,
                                   uint32_t record_bytes,
                                   uint64_t *hash_out) {
    uint32_t key_count = record[0], offset = 8;
    uint64_t hash = UINT64_C(1469598103934665603);
    (void)opaque;
    if (record_bytes != SQL_SPILL_JOIN_RECORD_BYTES || key_count == 0 ||
        key_count > PRDB_SDK_SQL_V16_MAX_JOIN_KEYS) {
        return -1;
    }
    for (uint32_t key = 0; key < key_count; ++key) {
        uint32_t state, type, data_len;
        if (offset > record_bytes || record_bytes - offset < 12) return -1;
        state = spill_read_u32_v21(record + offset);
        type = spill_read_u32_v21(record + offset + 4);
        data_len = spill_read_u32_v21(record + offset + 8);
        offset += 12;
        if (data_len > record_bytes - offset) return -1;
        for (uint32_t byte = 0; byte < 4; ++byte) {
            hash ^= (uint8_t)(state >> ((3 - byte) * 8));
            hash *= UINT64_C(1099511628211);
            hash ^= (uint8_t)(type >> ((3 - byte) * 8));
            hash *= UINT64_C(1099511628211);
        }
        for (uint32_t byte = 0; byte < 4; ++byte) {
            hash ^= (uint8_t)(data_len >> ((3 - byte) * 8));
            hash *= UINT64_C(1099511628211);
        }
        for (uint32_t byte = 0; byte < data_len; ++byte) {
            hash ^= record[offset + byte];
            hash *= UINT64_C(1099511628211);
        }
        offset += data_len;
    }
    *hash_out = hash;
    return 0;
}

static int32_t spill_join_equal_v21(void *opaque, const uint8_t *left,
                                    const uint8_t *right,
                                    uint32_t record_bytes,
                                    uint8_t *equal_out) {
    const sql_spill_join_compare_v21 *state = opaque;
    prdb_sdk_sql_join_materialized_row_v16 left_row = {0}, right_row = {0};
    int32_t status;
    *equal_out = 0;
    status = spill_decode_row_v21(left, record_bytes, state->binding,
                                  &left_row);
    if (status != PRDB_SDK_SQL_EXEC_OK) return -1;
    status = spill_decode_row_v21(right, record_bytes, state->binding,
                                  &right_row);
    if (status != PRDB_SDK_SQL_EXEC_OK) return -1;
    for (uint32_t key = 0; key < state->binding->base.join_key_count; ++key) {
        uint32_t truth = PRDB_SDK_SQL_TRUTH_UNKNOWN;
        const prdb_sdk_sql_join_key_binding_v16 *key_binding =
            &state->binding->base.join_keys[key];
        status = prdb_sdk_sql_compare_v1(
            &left_row.join_keys[key], &right_row.join_keys[key],
            PRDB_SDK_SQL_COMPARE_EQUAL, key_binding->left.decimal_scale,
            key_binding->right.decimal_scale, &truth);
        if (status != PRDB_SDK_SQL_EVAL_OK) return -1;
        if (truth != PRDB_SDK_SQL_TRUTH_TRUE) return 0;
    }
    *equal_out = 1;
    return 0;
}

static int32_t spill_join_output_v21(void *opaque, const uint8_t *left,
                                    const uint8_t *right,
                                    uint32_t record_bytes) {
    sql_spill_join_output_v21 *state = opaque;
    prdb_sdk_sql_join_materialized_row_v16 left_row = {0}, right_row = {0};
    prdb_sdk_value_v1 projected[PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS] = {{0}};
    uint32_t projection_count =
        state->binding->base.base.projection_count;
    int32_t status = spill_decode_row_v21(left, record_bytes, state->binding,
                                          &left_row);
    if (status != PRDB_SDK_SQL_EXEC_OK) {
        state->callback->callback_failure = status;
        return -1;
    }
    status = spill_decode_row_v21(right, record_bytes, state->binding,
                                  &right_row);
    if (status != PRDB_SDK_SQL_EXEC_OK) {
        state->callback->callback_failure = status;
        return -1;
    }
    for (uint32_t projection = 0; projection < projection_count; ++projection) {
        const prdb_sdk_sql_join_projection_binding_v8 *binding =
            &state->binding->base.base.projections[projection];
        projected[projection] =
            binding->relation_side == PRDB_SDK_SQL_V8_LEFT
                ? left_row.projections[projection]
                : right_row.projections[projection];
    }
    return join_v20_filter_and_emit(state->callback, projected,
                                    projection_count, state->binding);
}

static int32_t spill_join_cancel_v21(void *opaque) {
    const prdb_host_api_v2 *host = opaque;
    return check_cancelled_v19(host) == PRDB_SDK_SQL_EXEC_OK ? 0 : -1;
}

static int32_t spill_join_close_scan_v21(sql_spill_join_scan_v21 *scan) {
    uint8_t response[8] = {0};
    uint64_t response_len = 0;
    if (!scan->cursor_open) return PRDB_SDK_SQL_EXEC_OK;
    scan->cursor_open = 0;
    return prdb_sdk_executor_close_cursor_v1(
               scan->host, scan->cursor, response, sizeof(response),
               &response_len) == PRDB_SDK_EXECUTOR_OK
               ? PRDB_SDK_SQL_EXEC_OK
               : PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
}

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

static int32_t execute_spill_join_v21(
    const prdb_host_api_v3 *host_v3, const prdb_host_api_v6 *spill_host,
    const prdb_host_api_v2 *host_v2,
    const prdb_sdk_executor_relation_v1 *left_relation,
    const prdb_sdk_executor_relation_v1 *right_relation,
    const prdb_sdk_select_binding_v19 *binding,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *left_executor_response,
    uint64_t left_executor_response_capacity,
    uint8_t *right_executor_response,
    uint64_t right_executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_value_v1 *cell_scratch, uint32_t cell_scratch_capacity,
    uint32_t max_scan_pages, join_v20_callback_context *callback) {
    sql_spill_join_scan_v21 left_scan = {0}, right_scan = {0};
    sql_spill_join_compare_v21 compare = {binding};
    sql_spill_join_output_v21 output = {callback, binding};
    int32_t spill_status, status = PRDB_SDK_SQL_EXEC_OK;
    uint32_t total_pages = 0;
    prdb_host_api_v2 cancel_host = *host_v2;

    if (spill_host == NULL || spill_host->spill_request == NULL ||
        &spill_host->v5.v4.v3 != host_v3 || binding == NULL ||
        callback == NULL) {
        return fail_statement_v19(host_v3,
                                 PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT);
    }
    left_scan.relation = left_relation;
    left_scan.binding = binding;
    left_scan.host = host_v2;
    left_scan.relation_side = PRDB_SDK_SQL_V8_LEFT;
    left_scan.max_candidates = max_candidates;
    left_scan.max_batch_bytes = max_batch_bytes;
    left_scan.executor_response = left_executor_response;
    left_scan.executor_response_capacity = left_executor_response_capacity;
    left_scan.batch_response = batch_response;
    left_scan.batch_response_capacity = batch_response_capacity;
    left_scan.cell_scratch = cell_scratch;
    left_scan.cell_scratch_capacity = cell_scratch_capacity;
    left_scan.total_pages = &total_pages;
    left_scan.max_scan_pages = max_scan_pages;
    right_scan = left_scan;
    right_scan.relation = right_relation;
    right_scan.relation_side = PRDB_SDK_SQL_V8_RIGHT;
    right_scan.executor_response = right_executor_response;
    right_scan.executor_response_capacity = right_executor_response_capacity;

    spill_status = prdb_sdk_sql_spill_hash_join_v1(
        spill_host, SQL_SPILL_JOIN_RECORD_BYTES,
        spill_join_source_v21, &left_scan,
        spill_join_source_v21, &right_scan,
        spill_join_hash_v21, NULL,
        spill_join_equal_v21, &compare,
        spill_join_output_v21, &output,
        spill_join_cancel_v21, &cancel_host);
    if (spill_status != PRDB_SDK_SQL_SPILL_JOIN_OK) {
        if (left_scan.failure_status != 0) {
            status = left_scan.failure_status;
        } else if (right_scan.failure_status != 0) {
            status = right_scan.failure_status;
        } else if (callback->callback_failure != 0) {
            status = callback->callback_failure;
        } else if (spill_status == PRDB_SDK_SQL_SPILL_JOIN_INVALID_ARGUMENT) {
            status = PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT;
        } else if (spill_status == PRDB_SDK_SQL_SPILL_JOIN_MEMORY_ERROR) {
            status = PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT;
        } else {
            status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
        }
    }
    if (spill_join_close_scan_v21(&left_scan) != PRDB_SDK_SQL_EXEC_OK &&
        status == PRDB_SDK_SQL_EXEC_OK) {
        status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
    }
    if (spill_join_close_scan_v21(&right_scan) != PRDB_SDK_SQL_EXEC_OK &&
        status == PRDB_SDK_SQL_EXEC_OK) {
        status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
    }
    return status == PRDB_SDK_SQL_EXEC_OK
               ? status
               : fail_statement_v19(host_v3, status);
}

static int32_t execute_inner_join_v20_with_page_limit(
    const prdb_host_api_v3 *host,
    const prdb_host_api_v6 *spill_host,
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
    uint32_t scan_page_limit,
    const prdb_sdk_optimizer_index_join_plan_v1 *index_plan) {
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
    if (spill_host != NULL) {
        prdb_sdk_select_v19 spill_statement = {0};
        prdb_sdk_select_binding_v19 spill_binding = {0};
        const uint8_t *execution_sql =
            statement.has_where == 1 ? synthetic_sql : sql;
        uint64_t execution_sql_len =
            statement.has_where == 1 ? synthetic_len : sql_len;
        status = prdb_sdk_sql_parse_select_v19(
            execution_sql, execution_sql_len, &spill_statement,
            sizeof(spill_statement), NULL);
        if (status != PRDB_SDK_SQL_OK) {
            return fail_statement_v19(host, PRDB_SDK_SQL_EXEC_BIND_ERROR);
        }
        status = prdb_sdk_sql_bind_select_v19(
            &spill_statement, resolved_left.name, resolved_left.name_len,
            left_executor_columns, resolved_left.column_count,
            resolved_right.name, resolved_right.name_len,
            right_executor_columns, resolved_right.column_count,
            &spill_binding, sizeof(spill_binding));
        if (status != PRDB_SDK_SQL_BIND_OK) {
            return fail_statement_v19(host, PRDB_SDK_SQL_EXEC_BIND_ERROR);
        }
        status = execute_spill_join_v21(
            host, spill_host, &base_host, &resolved_left, &resolved_right,
            &spill_binding, max_candidates, max_batch_bytes,
            left_executor_response, left_executor_response_capacity,
            right_executor_response, right_executor_response_capacity,
            batch_response, batch_response_capacity, cell_scratch,
            cell_scratch_capacity, scan_page_limit, &callback);
    } else {
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
            join_v20_filter_and_emit, &callback, scan_page_limit, index_plan);
    }
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
        host, NULL, schema, schema_len, sql, sql_len, parameters, NULL, parameter_count,
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
        result_context, SQL_EXEC_V20_MAX_SCAN_PAGES, NULL);
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
        host, NULL, schema, schema_len, sql, sql_len, parameters, metadata, parameter_count,
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
        result_context, SQL_EXEC_V20_MAX_SCAN_PAGES, NULL);
}

int32_t prdb_sdk_sql_execute_inner_join_typed_spill_v21(
    const prdb_host_api_v6 *host,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters,
    const prdb_sdk_sql_parameter_metadata_v1 *metadata,
    uint32_t parameter_count,
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
    if (host == NULL || host->spill_request == NULL ||
        host->v5.v4.v3.struct_size < sizeof(prdb_host_api_v3) ||
        host->v5.v4.v3.abi_version != PRDB_QUERY_ABI_V3_VERSION) {
        if (host != NULL)
            return fail_statement_v19(&host->v5.v4.v3,
                                      PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT);
        return PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT;
    }
    return execute_inner_join_v20_with_page_limit(
        &host->v5.v4.v3, host, schema, schema_len, sql, sql_len, parameters,
        metadata, parameter_count, max_candidates, max_batch_bytes,
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
        result_context, SQL_EXEC_V20_SPILL_MAX_SCAN_PAGES, NULL);
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
    prdb_sdk_optimizer_index_join_plan_v1 index_plan = {0};
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
    if (optimizer_result.plan[2] ==
        PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_NESTED_LOOP_JOIN) {
        status = prdb_sdk_optimizer_decode_index_join_plan_v1(
            optimizer_result.plan, optimizer_result.plan_len,
            &index_plan, sizeof(index_plan));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK || index_plan.has_filter != 0)
            return fail_statement_v19(&host->v4.v3,
                                      PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH);
    }

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
        result_context, SQL_EXEC_V19_OPTIMIZER_MAX_SCAN_PAGES,
        optimizer_result.plan_len >= 3 && optimizer_result.plan[2] ==
            PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_NESTED_LOOP_JOIN
            ? &index_plan : NULL);
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
    prdb_sdk_optimizer_index_join_plan_v1 index_plan = {0};
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
    if (optimizer_result.plan[2] ==
        PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_NESTED_LOOP_JOIN) {
        status = prdb_sdk_optimizer_decode_index_join_plan_v1(
            optimizer_result.plan, optimizer_result.plan_len,
            &index_plan, sizeof(index_plan));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK || index_plan.has_filter != 1)
            return fail_statement_v19(&host->v4.v3,
                                      PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH);
    }
    return execute_inner_join_v20_with_page_limit(
        &host->v4.v3, NULL, schema, schema_len, sql, sql_len, parameters, NULL,
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
        result_context, SQL_EXEC_V20_OPTIMIZER_MAX_SCAN_PAGES,
        optimizer_result.plan_len >= 3 && optimizer_result.plan[2] ==
            PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_NESTED_LOOP_JOIN
            ? &index_plan : NULL);
}
