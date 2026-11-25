#include "prdb_sdk_sql_exec_v1.h"

#include <stdlib.h>
#include <string.h>

#define GROUP_SPILL_RECORD_BYTES UINT32_C(1024)
#define GROUP_SPILL_MAX_SCAN_PAGES UINT32_C(112)
#define GROUP_SPILL_MEMORY_BYTES UINT32_C(65536)

typedef struct group_spill_context {
    const prdb_host_api_v6 *host;
    const prdb_host_api_v2 *executor_host;
    const prdb_sdk_select_v1 *statement;
    const prdb_sdk_select_binding_v1 *binding;
    const prdb_sdk_executor_relation_v1 *relation;
    prdb_sdk_sql_column_shape_v1 *columns;
    prdb_sdk_value_v1 *cells;
    uint32_t cell_capacity;
    uint16_t max_candidates;
    uint32_t max_batch_bytes;
    uint8_t *response;
    uint64_t response_capacity;
    uint8_t *batch_bytes;
    uint64_t batch_capacity;
    uint64_t batch_len;
    prdb_sdk_batch_v1 batch;
    prdb_sdk_row_iterator_v1 rows;
    uint8_t cursor[16];
    uint8_t cursor_open;
    uint8_t finished;
    uint32_t pages;
    uint64_t consumed_groups;
    prdb_sdk_sql_group_row_v1_fn on_group;
    void *result_context;
    int32_t source_status;
} group_spill_context;

static int32_t group_spill_fail(const prdb_host_api_v6 *host, int32_t status) {
    if (status != 0 && host != NULL && host->v5.v4.v3.statement_failure != NULL)
        (void)host->v5.v4.v3.statement_failure(
            host->v5.v4.v3.context, PRDB_QUERY_STATEMENT_FAILURE_SQL);
    return status;
}

static int same_shape(const prdb_sdk_planner_relation_v1 *planned,
                      const prdb_sdk_executor_relation_v1 *resolved,
                      const prdb_sdk_sql_column_shape_v1 *left,
                      const prdb_sdk_sql_column_shape_v1 *right) {
    if (memcmp(planned->relation_id, resolved->relation_id, 16) != 0 ||
        planned->column_count != resolved->column_count ||
        planned->name_len != resolved->name_len ||
        memcmp(planned->name, resolved->name, planned->name_len) != 0)
        return 0;
    for (uint32_t i = 0; i < planned->column_count; ++i)
        if (left[i].ordinal != right[i].ordinal ||
            left[i].type_code != right[i].type_code ||
            left[i].decimal_precision != right[i].decimal_precision ||
            left[i].decimal_scale != right[i].decimal_scale ||
            left[i].name_len != right[i].name_len ||
            memcmp(left[i].name, right[i].name, left[i].name_len) != 0)
            return 0;
    return 1;
}

static void write_u16(uint8_t *out, uint16_t value) {
    out[0] = (uint8_t)(value >> 8);
    out[1] = (uint8_t)value;
}

static uint16_t read_u16(const uint8_t *in) {
    return (uint16_t)(((uint16_t)in[0] << 8) | in[1]);
}

static int32_t group_spill_check_cancelled(group_spill_context *state) {
    int32_t cancelled = 0;
    int32_t status = prdb_sdk_executor_check_cancelled_v1(
        state->executor_host, &cancelled);
    if (status != PRDB_SDK_EXECUTOR_OK || cancelled) {
        state->source_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
        return -1;
    }
    return 0;
}

static int32_t group_spill_source(void *opaque, uint8_t *record,
                                  uint32_t capacity, uint32_t *record_len,
                                  uint8_t *has_row) {
    group_spill_context *state = (group_spill_context *)opaque;
    const prdb_sdk_select_binding_v1 *groups =
        state->binding;
    uint32_t key_count = groups->group_key_count;
    if (capacity < GROUP_SPILL_RECORD_BYTES) return -1;
    *record_len = 0;
    *has_row = 0;
    for (;;) {
        if (state->finished && state->rows.rows_remaining == 0) {
            if (group_spill_check_cancelled(state) != 0) return -1;
            return 0;
        }
        if (state->rows.rows_remaining == 0) {
            int32_t status;
            if (state->pages >= GROUP_SPILL_MAX_SCAN_PAGES) {
                state->source_status = PRDB_SDK_SQL_EXEC_REQUEST_LIMIT;
                return -1;
            }
            if (group_spill_check_cancelled(state) != 0) return -1;
            status = prdb_sdk_executor_next_batch_v1(
                state->executor_host, state->cursor,
                state->max_candidates, state->max_batch_bytes,
                state->batch_bytes, state->batch_capacity, &state->batch_len,
                &state->batch, sizeof(state->batch));
            if (status != PRDB_SDK_EXECUTOR_OK) {
                state->source_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
                return -1;
            }
            ++state->pages;
            if (state->batch.finished) {
                state->cursor_open = 0;
                state->finished = 1;
            } else {
                memcpy(state->cursor, state->batch.continuation, 16);
            }
            status = prdb_sdk_batch_rows_begin_v1(
                &state->batch, &state->rows, sizeof(state->rows));
            if (status != PRDB_SDK_BATCH_OK) {
                state->source_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
                return -1;
            }
            if (state->rows.rows_remaining == 0 && state->finished) return 0;
            continue;
        }

        prdb_sdk_row_v1 row = {0};
        prdb_sdk_cell_iterator_v1 cells = {0};
        int32_t status = prdb_sdk_batch_row_next_v1(
            &state->rows, &row, sizeof(row));
        if (status != PRDB_SDK_BATCH_OK ||
            row.cell_count != state->relation->column_count ||
            prdb_sdk_row_cells_begin_v1(&row, &cells, sizeof(cells)) !=
                PRDB_SDK_BATCH_OK) {
            state->source_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
            return -1;
        }
        for (uint32_t i = 0; i < row.cell_count; ++i) {
            if (prdb_sdk_cell_next_v1(&cells, &state->cells[i],
                                      sizeof(state->cells[i])) !=
                PRDB_SDK_BATCH_OK) {
                state->source_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
                return -1;
            }
        }
        memset(record, 0, GROUP_SPILL_RECORD_BYTES);
        uint32_t offset = 0;
        for (uint32_t key = 0; key < key_count; ++key) {
            const prdb_sdk_value_v1 *value =
                &state->cells[groups->group_keys[key].ordinal];
            if ((value->state != PRDB_SDK_VALUE_STATE_VALUE &&
                 value->state != PRDB_SDK_VALUE_STATE_NULL) ||
                offset + 3 > capacity) {
                state->source_status = PRDB_SDK_SQL_EXEC_EVALUATION_ERROR;
                return -1;
            }
            record[offset++] = (uint8_t)value->state;
            if (value->state == PRDB_SDK_VALUE_STATE_VALUE) {
                if (value->data_len > UINT16_MAX ||
                    value->data_len > capacity - offset - 2 ||
                    (value->data_len != 0 && value->data == NULL)) {
                    state->source_status = PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT;
                    return -1;
                }
                write_u16(record + offset, (uint16_t)value->data_len);
                offset += 2;
                memcpy(record + offset, value->data, value->data_len);
                offset += value->data_len;
            } else {
                write_u16(record + offset, 0);
                offset += 2;
            }
        }
        *record_len = GROUP_SPILL_RECORD_BYTES;
        *has_row = 1;
        return 0;
    }
}

static int32_t decode_key(const group_spill_context *state,
                          const uint8_t *record, uint32_t key,
                          uint32_t *offset, prdb_sdk_value_v1 *value) {
    uint8_t value_state = record[(*offset)++];
    uint16_t length = read_u16(record + *offset);
    *offset += 2;
    if ((value_state != PRDB_SDK_VALUE_STATE_VALUE &&
         value_state != PRDB_SDK_VALUE_STATE_NULL) ||
        (value_state != PRDB_SDK_VALUE_STATE_VALUE && length != 0) ||
        length > GROUP_SPILL_RECORD_BYTES - *offset)
        return -1;
    *value = (prdb_sdk_value_v1){
        sizeof(*value), PRDB_SDK_VALUE_ABI_V1_VERSION, value_state,
        value_state == PRDB_SDK_VALUE_STATE_VALUE
            ? state->binding->group_keys[key].type_code
            : 0,
        value_state == PRDB_SDK_VALUE_STATE_VALUE ? record + *offset : NULL,
        length};
    *offset += length;
    return 0;
}

static int32_t group_spill_compare(void *opaque, const uint8_t *left,
                                   const uint8_t *right, int32_t *ordering) {
    const group_spill_context *state = (const group_spill_context *)opaque;
    const prdb_sdk_select_binding_v1 *groups =
        state->binding;
    uint32_t left_offset = 0, right_offset = 0;
    for (uint32_t key = 0; key < groups->group_key_count; ++key) {
        prdb_sdk_value_v1 a = {0}, b = {0};
        uint32_t truth = PRDB_SDK_SQL_TRUTH_FALSE;
        if (decode_key(state, left, key, &left_offset, &a) != 0 ||
            decode_key(state, right, key, &right_offset, &b) != 0)
            return -1;
        if (a.state == PRDB_SDK_VALUE_STATE_NULL ||
            b.state == PRDB_SDK_VALUE_STATE_NULL) {
            if (a.state != b.state) {
                *ordering = a.state == PRDB_SDK_VALUE_STATE_NULL ? -1 : 1;
                return 0;
            }
            continue;
        }
        int32_t status = prdb_sdk_sql_compare_v1(
            &a, &b, PRDB_SDK_SQL_COMPARE_LESS,
            groups->group_keys[key].decimal_scale,
            groups->group_keys[key].decimal_scale, &truth);
        if (status != PRDB_SDK_SQL_EVAL_OK) return -1;
        if (truth == PRDB_SDK_SQL_TRUTH_TRUE) { *ordering = -1; return 0; }
        status = prdb_sdk_sql_compare_v1(
            &a, &b, PRDB_SDK_SQL_COMPARE_GREATER,
            groups->group_keys[key].decimal_scale,
            groups->group_keys[key].decimal_scale, &truth);
        if (status != PRDB_SDK_SQL_EVAL_OK) return -1;
        if (truth == PRDB_SDK_SQL_TRUTH_TRUE) { *ordering = 1; return 0; }
    }
    *ordering = 0;
    return 0;
}

static int32_t group_spill_output(void *opaque, const uint8_t *record,
                                  uint32_t record_len, uint64_t row_count) {
    group_spill_context *state = (group_spill_context *)opaque;
    const prdb_sdk_select_binding_v1 *groups =
        state->binding;
    prdb_sdk_value_v1 values[PRDB_SDK_SQL_MAX_GROUP_KEYS] = {{0}};
    uint32_t offset = 0;
    if (record_len != GROUP_SPILL_RECORD_BYTES) return -1;
    if ((state->consumed_groups & UINT64_C(2047)) == 0 &&
        group_spill_check_cancelled(state) != 0)
        return -1;
    if (state->statement->has_limit &&
        state->consumed_groups >=
            state->statement->limit) {
        state->consumed_groups++;
        return 0;
    }
    for (uint32_t key = 0; key < groups->group_key_count; ++key)
        if (decode_key(state, record, key, &offset, &values[key]) != 0)
            return -1;
    state->consumed_groups++;
    if (state->on_group(
        state->result_context, values, groups->group_key_count, NULL, row_count,
        state->binding) != 0) {
        state->source_status = PRDB_SDK_SQL_EXEC_RESULT_ERROR;
        return -1;
    }
    return 0;
}

int32_t prdb_sdk_sql_execute_group_count_spill_v1(
    const prdb_host_api_v6 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity,
    prdb_sdk_sql_group_row_v1_fn on_group, void *result_context) {
    const prdb_host_api_v3 *host_v3;
    prdb_sdk_select_v1 statement = {0};
    prdb_sdk_select_binding_v1 binding = {0};
    prdb_sdk_planner_relation_v1 planned = {0};
    prdb_sdk_executor_relation_v1 resolved = {0};
    group_spill_context state = {0};
    prdb_host_api_v2 base_host;
    uint64_t planner_len = 0, executor_len = 0, close_len = 0;
    uint8_t close_response[8] = {0};
    int32_t status;
    if (host == NULL || host->spill_request == NULL ||
        host->v5.v4.v3.struct_size < sizeof(host->v5.v4.v3) ||
        host->v5.v4.v3.abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->v5.v4.v3.statement_failure == NULL)
        return PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT;
    host_v3 = &host->v5.v4.v3;
    base_host = (prdb_host_api_v2){
        sizeof(prdb_host_api_v2), PRDB_QUERY_ABI_V2_VERSION,
        host_v3->context, host_v3->executor_request, host_v3->planner_request,
        host_v3->write_result};
    if (schema == NULL || sql == NULL || on_group == NULL || max_candidates == 0 ||
        max_candidates > PRDB_SDK_BATCH_MAX_ROWS || max_batch_bytes < 8 ||
        max_batch_bytes > PRDB_SDK_BATCH_MAX_BYTES ||
        batch_response_capacity < max_batch_bytes || planner_response == NULL ||
        executor_response == NULL || batch_response == NULL ||
        planner_columns == NULL || executor_columns == NULL ||
        cell_scratch == NULL || planner_column_capacity == 0 ||
        executor_column_capacity == 0)
        return group_spill_fail(host, PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT);
    status = prdb_sdk_sql_parse_select_v1(sql, sql_len, &statement,
                                            sizeof(statement), NULL);
    if (status != PRDB_SDK_SQL_OK)
        return group_spill_fail(host, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    const prdb_sdk_select_v1 *parsed = &statement;
    if (parsed->aggregate_kind != PRDB_SDK_SQL_AGGREGATE_COUNT_STAR ||
        parsed->predicate_count != 0 ||
        statement.having_node_count != 0 ||
        statement.order_count != 0)
        return PRDB_SDK_SQL_EXEC_UNSUPPORTED;
    status = prdb_sdk_planner_describe_relation_v1(
        &base_host, schema, schema_len,
        sql + parsed->relation.offset,
        parsed->relation.length, planner_response,
        planner_response_capacity, &planner_len, &planned, sizeof(planned),
        planner_columns, planner_column_capacity);
    if (status != PRDB_SDK_PLANNER_REQUEST_OK)
        return group_spill_fail(host, PRDB_SDK_SQL_EXEC_PLANNER_ERROR);
    status = prdb_sdk_executor_resolve_relation_v1(
        &base_host, schema, schema_len, sql + parsed->relation.offset,
        parsed->relation.length, executor_response,
        executor_response_capacity, &executor_len, &resolved, sizeof(resolved),
        executor_columns, executor_column_capacity);
    if (status != PRDB_SDK_EXECUTOR_OK)
        return group_spill_fail(host, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    if (!same_shape(&planned, &resolved, planner_columns, executor_columns))
        return group_spill_fail(host, PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH);
    if (cell_scratch_capacity < resolved.column_count)
        return group_spill_fail(host, PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT);
    status = prdb_sdk_sql_bind_select_v1(
        &statement, resolved.name, resolved.name_len, executor_columns,
        resolved.column_count, &binding, sizeof(binding));
    if (status != PRDB_SDK_SQL_BIND_OK)
        return group_spill_fail(host, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    const prdb_sdk_select_binding_v1 *groups =
        &binding;
    if (groups->group_key_count == 0 || groups->group_key_count > 8)
        return group_spill_fail(host, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    if (statement.has_limit &&
        statement.limit == 0)
        return PRDB_SDK_SQL_EXEC_OK;
    state.host = host;
    state.executor_host = &base_host;
    state.statement = &statement;
    state.binding = &binding;
    state.relation = &resolved;
    state.columns = executor_columns;
    state.cells = cell_scratch;
    state.cell_capacity = cell_scratch_capacity;
    state.max_candidates = max_candidates;
    state.max_batch_bytes = max_batch_bytes;
    state.response = executor_response;
    state.response_capacity = executor_response_capacity;
    state.batch_bytes = batch_response;
    state.batch_capacity = batch_response_capacity;
    state.on_group = on_group;
    state.result_context = result_context;
    status = prdb_sdk_executor_open_full_scan_v1(
        &base_host, resolved.executor_handle, executor_response,
        executor_response_capacity, &executor_len, state.cursor);
    if (status != PRDB_SDK_EXECUTOR_OK)
        return group_spill_fail(host, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    state.cursor_open = 1;
    status = prdb_sdk_sql_spill_group_count_v1(
        host, &state, GROUP_SPILL_RECORD_BYTES, GROUP_SPILL_MEMORY_BYTES,
        group_spill_source, group_spill_compare, group_spill_output);
    if (state.cursor_open) {
        int32_t close_status = prdb_sdk_executor_close_cursor_v1(
            &base_host, state.cursor, close_response, sizeof(close_response),
            &close_len);
        if (close_status != PRDB_SDK_EXECUTOR_OK && status == 0) status = -1;
    }
    if (state.source_status != 0)
        return group_spill_fail(host, state.source_status);
    if (status != 0)
        return group_spill_fail(host, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    int32_t cancelled = 0;
    if (prdb_sdk_executor_check_cancelled_v1(&base_host, &cancelled) !=
            PRDB_SDK_EXECUTOR_OK || cancelled)
        return group_spill_fail(host, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    return PRDB_SDK_SQL_EXEC_OK;
}
