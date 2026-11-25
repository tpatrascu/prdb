#include "prdb_sdk_sql_exec_v1.h"
#include "prdb_sdk_sql_v1.h"
#include "prdb_sdk_sql_dml_v1.h"
#include "prdb_sdk_sql_order_v1.h"
#include "prdb_sdk_sql_spill_sort_v1.h"
#include "sql_parameter_internal.h"

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
                               const prdb_sdk_sql_column_shape_v1 *planned_columns,
                               const prdb_sdk_sql_column_shape_v1 *resolved_columns) {
    if (memcmp(planned->relation_id, resolved->relation_id, 16) != 0 ||
        planned->column_count != resolved->column_count ||
        planned->name_len != resolved->name_len ||
        memcmp(planned->name, resolved->name, planned->name_len) != 0) return 0;
    for (uint32_t index = 0; index < planned->column_count; ++index) {
        const prdb_sdk_sql_column_shape_v1 *a = &planned_columns[index];
        const prdb_sdk_sql_column_shape_v1 *b = &resolved_columns[index];
        if (a->ordinal != b->ordinal || a->type_code != b->type_code ||
            a->decimal_precision != b->decimal_precision ||
            a->decimal_scale != b->decimal_scale || a->name_len != b->name_len ||
            memcmp(a->name, b->name, a->name_len) != 0) return 0;
    }
    return 1;
}

static int span_matches_name(const uint8_t *sql,
                             prdb_sdk_sql_span_v1 span,
                             const uint8_t *name, uint32_t name_len) {
    if (name == NULL || span.length != name_len) return 0;
    for (uint32_t index = 0; index < span.length; ++index) {
        uint8_t left = sql[span.offset + index];
        uint8_t right = name[index];
        if (left >= 'A' && left <= 'Z') left = (uint8_t)(left - 'A' + 'a');
        if (right >= 'A' && right <= 'Z') right = (uint8_t)(right - 'A' + 'a');
        if (left != right) return 0;
    }
    return 1;
}

#define SQL_SPILL_SORT_ROW_BYTES UINT32_C(1024)

typedef struct sql_spill_sort_context {
    const prdb_host_api_v6 *host;
    prdb_sdk_spill_stream_v1 input;
    uint64_t source_offset;
    uint64_t record_count;
    uint32_t order_ordinal;
    uint32_t order_type;
    uint8_t decimal_scale;
    uint8_t descending;
    uint8_t has_limit;
    uint64_t limit;
    uint64_t emitted;
    prdb_sdk_select_binding_v1 binding;
    prdb_sdk_sql_result_row_v1_fn on_row;
    void *result_context;
} sql_spill_sort_context;

static uint32_t spill_read_u32(const uint8_t *bytes) {
    return ((uint32_t)bytes[0] << 24) | ((uint32_t)bytes[1] << 16) |
           ((uint32_t)bytes[2] << 8) | bytes[3];
}

static void spill_write_u32(uint8_t *bytes, uint32_t value) {
    bytes[0] = (uint8_t)(value >> 24);
    bytes[1] = (uint8_t)(value >> 16);
    bytes[2] = (uint8_t)(value >> 8);
    bytes[3] = (uint8_t)value;
}

static int32_t parse_spilled_row(const uint8_t *record,
                                 sql_spill_sort_context *context,
                                 uint8_t wire[SQL_SPILL_SORT_ROW_BYTES + 5],
                                 prdb_sdk_row_v1 *row_out,
                                 prdb_sdk_value_v1 *key_out) {
    prdb_sdk_batch_v1 batch = {0};
    prdb_sdk_row_iterator_v1 rows = {0};
    prdb_sdk_cell_iterator_v1 cells = {0};
    uint32_t row_bytes = spill_read_u32(record);
    if (row_bytes == 0 || row_bytes > SQL_SPILL_SORT_ROW_BYTES - 4)
        return -1;
    wire[0] = 0;
    wire[1] = (uint8_t)PRDB_SDK_BATCH_WIRE_V1_VERSION;
    wire[2] = 1;
    wire[3] = 0;
    wire[4] = 1;
    memcpy(wire + 5, record + 4, row_bytes);
    if (prdb_sdk_batch_open_v1(&batch, sizeof(batch), wire, row_bytes + 5) !=
            PRDB_SDK_BATCH_OK ||
        prdb_sdk_batch_rows_begin_v1(&batch, &rows, sizeof(rows)) !=
            PRDB_SDK_BATCH_OK ||
        prdb_sdk_batch_row_next_v1(&rows, row_out, sizeof(*row_out)) !=
            PRDB_SDK_BATCH_OK ||
        row_out->cell_count <= context->order_ordinal ||
        prdb_sdk_row_cells_begin_v1(row_out, &cells, sizeof(cells)) !=
            PRDB_SDK_BATCH_OK) {
        return -1;
    }
    for (uint32_t ordinal = 0; ordinal <= context->order_ordinal; ++ordinal) {
        if (prdb_sdk_cell_next_v1(&cells, key_out, sizeof(*key_out)) !=
            PRDB_SDK_BATCH_OK)
            return -1;
    }
    return 0;
}

static int32_t spill_sort_source(void *opaque, uint8_t *record,
                                 uint32_t capacity, uint32_t *row_len,
                                 uint8_t *has_row) {
    sql_spill_sort_context *context = (sql_spill_sort_context *)opaque;
    uint64_t read_bytes = 0;
    if (capacity < SQL_SPILL_SORT_ROW_BYTES || row_len == NULL ||
        has_row == NULL)
        return -1;
    *row_len = 0;
    *has_row = 0;
    if (context->source_offset == context->record_count *
                                      SQL_SPILL_SORT_ROW_BYTES)
        return 0;
    if (context->source_offset > context->record_count *
                                    SQL_SPILL_SORT_ROW_BYTES ||
        prdb_sdk_spill_read_at_v1(
            context->host, &context->input, context->source_offset, record,
            SQL_SPILL_SORT_ROW_BYTES, &read_bytes) != 0 ||
        read_bytes != SQL_SPILL_SORT_ROW_BYTES)
        return -1;
    context->source_offset += SQL_SPILL_SORT_ROW_BYTES;
    *row_len = SQL_SPILL_SORT_ROW_BYTES;
    *has_row = 1;
    return 0;
}

static int32_t spill_sort_compare(void *opaque, const uint8_t *left_record,
                                  const uint8_t *right_record,
                                  int32_t *ordering) {
    sql_spill_sort_context *context = (sql_spill_sort_context *)opaque;
    uint8_t left_wire[SQL_SPILL_SORT_ROW_BYTES + 5] = {0};
    uint8_t right_wire[SQL_SPILL_SORT_ROW_BYTES + 5] = {0};
    prdb_sdk_row_v1 left_row = {0}, right_row = {0};
    prdb_sdk_value_v1 left_key = {0}, right_key = {0};
    uint32_t indices[2] = {0};
    prdb_sdk_value_v1 values[2];
    if (ordering == NULL ||
        parse_spilled_row(left_record, context, left_wire, &left_row,
                          &left_key) != 0 ||
        parse_spilled_row(right_record, context, right_wire, &right_row,
                          &right_key) != 0)
        return -1;
    values[0] = left_key;
    values[1] = right_key;
    prdb_sdk_sql_order_key_v1 sort_key = {
        values,
        context->order_type,
        context->decimal_scale,
        (uint8_t)(context->descending ? PRDB_SDK_SQL_ORDER_DESCENDING
                                      : PRDB_SDK_SQL_ORDER_ASCENDING),
        PRDB_SDK_SQL_ORDER_NULLS_FIRST,
        0,
    };
    int32_t status =
        prdb_sdk_sql_order_row_indices_v1(&sort_key, 1, 2, indices, 2);
    if (status != PRDB_SDK_SQL_EVAL_OK) return status;
    if (values[0].state == PRDB_SDK_VALUE_STATE_NULL &&
        values[1].state == PRDB_SDK_VALUE_STATE_NULL) {
        *ordering = 0;
        return 0;
    }
    if (values[0].state == PRDB_SDK_VALUE_STATE_NULL ||
        values[1].state == PRDB_SDK_VALUE_STATE_NULL) {
        *ordering = indices[0] == 0 ? -1 : 1;
        return 0;
    }
    uint32_t equal = 0;
    status = prdb_sdk_sql_compare_v1(
        &values[0], &values[1], PRDB_SDK_SQL_COMPARE_EQUAL,
        context->decimal_scale, context->decimal_scale, &equal);
    if (status != PRDB_SDK_SQL_EVAL_OK) return status;
    *ordering = equal == PRDB_SDK_SQL_TRUTH_TRUE
                    ? 0
                    : (indices[0] == 0 ? -1 : 1);
    return 0;
}

static int32_t spill_sort_output(void *opaque, const uint8_t *record,
                                 uint32_t row_len) {
    sql_spill_sort_context *context = (sql_spill_sort_context *)opaque;
    uint8_t wire[SQL_SPILL_SORT_ROW_BYTES + 5] = {0};
    prdb_sdk_row_v1 row = {0};
    prdb_sdk_value_v1 key = {0};
    if (row_len != SQL_SPILL_SORT_ROW_BYTES ||
        parse_spilled_row(record, context, wire, &row, &key) != 0)
        return -1;
    if (context->has_limit && context->emitted >= context->limit) return 0;
    ++context->emitted;
    return context->on_row(context->result_context, &row,
                           &context->binding);
}

static int32_t execute_select_internal(
    const prdb_host_api_v3 *host_v3, const uint8_t *schema, uint32_t schema_len,
    const prdb_host_api_v6 *spill_host,
    const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters,
    const prdb_sdk_sql_parameter_metadata_v1 *parameter_metadata,
    uint32_t parameter_count,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity,
    prdb_sdk_value_v1 *cell_scratch, uint32_t cell_scratch_capacity,
    uint8_t *materialized_rows, uint64_t materialized_rows_capacity,
    uint32_t parser_version, prdb_sdk_sql_result_row_v1_fn on_row,
    void *result_context) {
    prdb_sdk_select_v1 statement = {0};
    prdb_sdk_select_v1 statement_v26 = {0};
    prdb_sdk_select_v1 statement_v3 = {0};
    prdb_sdk_planner_relation_v1 planned = {0};
    prdb_sdk_executor_relation_v1 resolved = {0};
    prdb_sdk_select_binding_v1 binding = {0};
    prdb_sdk_select_binding_v1 binding_v3 = {0};
    prdb_sdk_batch_v1 batch = {0};
    prdb_sdk_row_iterator_v1 rows = {0};
    prdb_sdk_row_v1 row = {0};
    uint8_t cursor[16] = {0};
    uint64_t planner_response_len = 0;
    uint64_t executor_response_len = 0;
    uint64_t batch_response_len = 0;
    uint32_t page_count = 0;
    uint32_t delivered = 0;
    uint32_t materialized_count = 0;
    uint32_t materialized_offset = 5;
    prdb_sdk_spill_stream_v1 spill_input = {0};
    sql_spill_sort_context spill_sort = {0};
    uint64_t spill_record_count = 0;
    uint32_t order_indices[PRDB_SDK_SQL_ORDER_MAX_ROWS];
    prdb_sdk_row_v1 order_rows[PRDB_SDK_SQL_ORDER_MAX_ROWS];
    prdb_sdk_value_v1 order_keys[PRDB_SDK_SQL_ORDER_MAX_ROWS];
    uint32_t has_order_by = 0;
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
    if (parser_version == PRDB_SDK_SQL_ABI_V1_VERSION) {
        status = prdb_sdk_sql_parse_select_v1(sql, sql_len, &statement,
                                              sizeof(statement), NULL);
        if (status == PRDB_SDK_SQL_OK) {
            has_order_by = statement.has_order_by;
            if (has_order_by != 0 && spill_host == NULL &&
                (materialized_rows == NULL ||
                 materialized_rows_capacity < 5 ||
                 materialized_rows_capacity >
                     PRDB_SDK_SQL_EXEC_MAX_MATERIALIZED_BYTES)) {
                return fail_statement(host_v3,
                                      PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT);
            }
        }
    } else {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT);
    }
    if (status != PRDB_SDK_SQL_OK) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    }
    if (parameter_metadata != NULL) {
        uint8_t parameter_seen[UINT16_MAX / 8 + 1] = {0};
        uint32_t maximum_parameter_index = 0;
        for (uint32_t index = 0; index < statement.predicate_count; ++index) {
            const prdb_sdk_predicate_node_v1 *node = &statement.predicates[index];
            if (node->node_kind == PRDB_SDK_SQL_PREDICATE_COMPARE) {
                uint32_t parameter_index = node->parameter_index;
                if (parameter_index == 0 || parameter_index > parameter_count) {
                    return fail_statement(host_v3,
                                          PRDB_SDK_SQL_EXEC_BIND_ERROR);
                }
                parameter_seen[(parameter_index - 1) / 8] |=
                    (uint8_t)(1u << ((parameter_index - 1) % 8));
                if (parameter_index > maximum_parameter_index) {
                    maximum_parameter_index = parameter_index;
                }
            }
        }
        if (maximum_parameter_index != parameter_count) {
            return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
        }
        for (uint32_t index = 0; index < parameter_count; ++index) {
            if ((parameter_seen[index / 8] & (uint8_t)(1u << (index % 8))) ==
                0) {
                return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
            }
        }
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
    status = prdb_sdk_sql_bind_select_v1(
        &statement, resolved.name, resolved.name_len, executor_columns,
        resolved.column_count, &binding, sizeof(binding));
    (void)statement_v3;
    (void)statement_v26;
    (void)binding_v3;
    if (status != PRDB_SDK_SQL_BIND_OK) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    }
    for (uint32_t index = 0; index < statement.predicate_count; ++index) {
        const prdb_sdk_predicate_node_v1 *node = &statement.predicates[index];
        if (node->node_kind == PRDB_SDK_SQL_PREDICATE_COMPARE) {
            const prdb_sdk_sql_bound_column_v1 *column =
                &binding.predicate_columns[index];
            prdb_sdk_value_v1 bound_parameter = {0};
            status = bind_sql_parameter(
                node->parameter_index, parameters, parameter_metadata,
                parameter_count, column->type_code, column->decimal_precision,
                column->decimal_scale, &bound_parameter);
            if (status != PRDB_SDK_SQL_EVAL_OK) {
                return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
            }
        }
    }
    if (statement.has_limit && statement.limit == 0) return PRDB_SDK_SQL_EXEC_OK;

    if (has_order_by != 0 && spill_host != NULL &&
        prdb_sdk_spill_create_v1(spill_host, &spill_input) != 0) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    }

    status = prdb_sdk_executor_open_full_scan_v1(
        host, resolved.executor_handle, executor_response,
        executor_response_capacity, &executor_response_len, cursor);
    if (status != PRDB_SDK_EXECUTOR_OK) {
        if (spill_input.valid == 1)
            (void)prdb_sdk_spill_destroy_v1(spill_host, &spill_input);
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    }
    cursor_open = 1;

    for (;;) {
        if (page_count >= PRDB_SDK_SQL_EXEC_SELECT_MAX_SCAN_PAGES) {
            final_status = PRDB_SDK_SQL_EXEC_REQUEST_LIMIT;
            break;
        }
        int32_t cancelled = 0;
        status = prdb_sdk_executor_check_cancelled_v1(host, &cancelled);
        if (status != PRDB_SDK_EXECUTOR_OK || cancelled) {
            final_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
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
                status = prdb_sdk_sql_eval_bound_select_row_v1(
                    &statement, &binding, parameters, parameter_count, &row,
                    cell_scratch, cell_scratch_capacity, &truth);
                if (status != PRDB_SDK_SQL_EVAL_OK) {
                    final_status = PRDB_SDK_SQL_EXEC_EVALUATION_ERROR;
                    break;
                }
            }
            if (truth == PRDB_SDK_SQL_TRUTH_TRUE) {
                if (has_order_by != 0) {
                    uint32_t row_start;
                    uint32_t row_length;
                    if (row.cells_offset < 18 || rows.offset < row.cells_offset) {
                        final_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
                        break;
                    }
                    row_start = row.cells_offset - 18;
                    row_length = rows.offset - row_start;
                    if (spill_host != NULL) {
                        uint8_t record[SQL_SPILL_SORT_ROW_BYTES] = {0};
                        if (row_length > SQL_SPILL_SORT_ROW_BYTES - 4) {
                            final_status =
                                PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT;
                            break;
                        }
                        spill_write_u32(record, row_length);
                        memcpy(record + 4, batch_response + row_start,
                               row_length);
                        if (prdb_sdk_spill_append_v1(
                                spill_host, &spill_input, record,
                                sizeof(record)) != 0) {
                            final_status =
                                PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
                            break;
                        }
                        ++spill_record_count;
                    } else {
                        if (materialized_count >=
                            PRDB_SDK_SQL_ORDER_MAX_ROWS) {
                            final_status =
                                PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT;
                            break;
                        }
                        if ((uint64_t)row_length >
                            materialized_rows_capacity - materialized_offset) {
                            final_status =
                                PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT;
                            break;
                        }
                        memcpy(materialized_rows + materialized_offset,
                               batch_response + row_start, row_length);
                        materialized_offset += row_length;
                        ++materialized_count;
                    }
                } else {
                    if (on_row(result_context, &row, &binding) != 0) {
                        final_status = PRDB_SDK_SQL_EXEC_RESULT_ERROR;
                        break;
                    }
                    ++delivered;
                    if (statement.has_limit &&
                        delivered >= statement.limit) {
                        break;
                    }
                }
            }
        }
        if (final_status != PRDB_SDK_SQL_EXEC_OK) break;
        if (has_order_by == 0 && statement.has_limit &&
            delivered >= statement.limit) {
            break;
        }
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
    if (final_status == PRDB_SDK_SQL_EXEC_OK) {
        int32_t cancelled = 0;
        status = prdb_sdk_executor_check_cancelled_v1(host, &cancelled);
        if (status != PRDB_SDK_EXECUTOR_OK || cancelled) {
            final_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
        }
    }
    if (final_status == PRDB_SDK_SQL_EXEC_OK && has_order_by != 0 &&
        spill_host != NULL) {
        int32_t spill_status = prdb_sdk_spill_seal_v1(spill_host, &spill_input);
        if (spill_status != 0) {
            final_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
        } else {
            spill_sort.host = spill_host;
            spill_sort.input = spill_input;
            spill_sort.record_count = spill_record_count;
            spill_sort.order_ordinal = binding.order_by_column.ordinal;
            spill_sort.order_type = binding.order_by_column.type_code;
            spill_sort.decimal_scale =
                binding.order_by_column.decimal_scale;
            spill_sort.descending = statement.order_descending;
            spill_sort.has_limit = statement.has_limit;
            spill_sort.limit = statement.limit;
            spill_sort.binding = binding;
            spill_sort.on_row = on_row;
            spill_sort.result_context = result_context;
            spill_status = prdb_sdk_sql_spill_sort_v1(
                spill_host, &spill_sort, SQL_SPILL_SORT_ROW_BYTES,
                UINT32_C(65536), spill_sort_source, spill_sort_compare,
                spill_sort_output);
            if (spill_status != 0) {
                final_status = PRDB_SDK_SQL_EXEC_EVALUATION_ERROR;
            }
        }
    } else if (final_status == PRDB_SDK_SQL_EXEC_OK && has_order_by != 0) {
        prdb_sdk_batch_v1 materialized_batch = {0};
        prdb_sdk_row_iterator_v1 materialized_iterator = {0};
        uint32_t emitted = 0;

        materialized_rows[0] = 0;
        materialized_rows[1] =
            (uint8_t)PRDB_SDK_BATCH_WIRE_V1_VERSION;
        materialized_rows[2] = 1;
        materialized_rows[3] = (uint8_t)(materialized_count >> 8);
        materialized_rows[4] = (uint8_t)materialized_count;
        status = prdb_sdk_batch_open_v1(
            &materialized_batch, sizeof(materialized_batch), materialized_rows,
            materialized_offset);
        if (status != PRDB_SDK_BATCH_OK ||
            prdb_sdk_batch_rows_begin_v1(
                &materialized_batch, &materialized_iterator,
                sizeof(materialized_iterator)) != PRDB_SDK_BATCH_OK) {
            final_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
        }
        for (uint32_t index = 0;
             final_status == PRDB_SDK_SQL_EXEC_OK &&
             index < materialized_count;
             ++index) {
            prdb_sdk_row_v1 materialized_row = {0};
            prdb_sdk_cell_iterator_v1 cells = {0};
            uint32_t key_found = 0;
            if (prdb_sdk_batch_row_next_v1(
                    &materialized_iterator, &materialized_row,
                    sizeof(materialized_row)) != PRDB_SDK_BATCH_OK ||
                materialized_row.cell_count != resolved.column_count ||
                prdb_sdk_row_cells_begin_v1(
                    &materialized_row, &cells, sizeof(cells)) !=
                    PRDB_SDK_BATCH_OK) {
                final_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
                break;
            }
            for (uint32_t ordinal = 0; ordinal < resolved.column_count;
                 ++ordinal) {
                if (prdb_sdk_cell_next_v1(
                        &cells, &cell_scratch[ordinal],
                        sizeof(cell_scratch[ordinal])) != PRDB_SDK_BATCH_OK) {
                    final_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
                    break;
                }
                if (ordinal == binding.order_by_column.ordinal) {
                    order_keys[index] = cell_scratch[ordinal];
                    key_found = 1;
                }
            }
            if (final_status == PRDB_SDK_SQL_EXEC_OK && key_found == 0) {
                final_status = PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH;
            }
            order_rows[index] = materialized_row;
        }
        if (final_status == PRDB_SDK_SQL_EXEC_OK) {
            prdb_sdk_sql_order_key_v1 sort_key = {
                order_keys,
                binding.order_by_column.type_code,
                binding.order_by_column.decimal_scale,
                (uint8_t)statement.order_descending,
                PRDB_SDK_SQL_ORDER_NULLS_FIRST,
                0,
            };
            status = prdb_sdk_sql_order_row_indices_v1(
                &sort_key, 1, materialized_count, order_indices,
                PRDB_SDK_SQL_ORDER_MAX_ROWS);
            if (status != PRDB_SDK_SQL_ORDER_OK) {
                final_status = PRDB_SDK_SQL_EXEC_EVALUATION_ERROR;
            }
        }
        while (final_status == PRDB_SDK_SQL_EXEC_OK &&
               emitted < materialized_count &&
               (!statement.has_limit || emitted < statement.limit)) {
            if (on_row(result_context, &order_rows[order_indices[emitted]],
                       &binding) != 0) {
                final_status = PRDB_SDK_SQL_EXEC_RESULT_ERROR;
                break;
            }
            ++emitted;
        }
    }
    if (spill_input.valid == 1 &&
        prdb_sdk_spill_destroy_v1(spill_host, &spill_input) != 0 &&
        final_status == PRDB_SDK_SQL_EXEC_OK) {
        final_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
    }
    return fail_statement(host_v3, final_status);
}

int32_t prdb_sdk_sql_execute_select_v1(
    const prdb_host_api_v3 *host, const uint8_t *schema, uint32_t schema_len,
    const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity,
    prdb_sdk_value_v1 *cell_scratch, uint32_t cell_scratch_capacity,
    prdb_sdk_sql_result_row_v1_fn on_row, void *result_context) {
    return execute_select_internal(
        host, schema, schema_len, NULL, sql, sql_len, parameters, NULL, parameter_count,
        max_candidates, max_batch_bytes, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns,
        executor_column_capacity, cell_scratch, cell_scratch_capacity, NULL, 0,
        PRDB_SDK_SQL_ABI_V1_VERSION, on_row, result_context);
}

int32_t prdb_sdk_sql_execute_select_ordered_v1(
    const prdb_host_api_v3 *host, const uint8_t *schema, uint32_t schema_len,
    const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity,
    prdb_sdk_value_v1 *cell_scratch, uint32_t cell_scratch_capacity,
    uint8_t *materialized_rows, uint64_t materialized_rows_capacity,
    prdb_sdk_sql_result_row_v1_fn on_row, void *result_context) {
    return execute_select_internal(
        host, schema, schema_len, NULL, sql, sql_len, parameters, NULL, parameter_count,
        max_candidates, max_batch_bytes, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns,
        executor_column_capacity, cell_scratch, cell_scratch_capacity,
        materialized_rows, materialized_rows_capacity,
        PRDB_SDK_SQL_ABI_V1_VERSION, on_row, result_context);
}

int32_t prdb_sdk_sql_execute_select_spill_v1(
    const prdb_host_api_v6 *host, const uint8_t *schema, uint32_t schema_len,
    const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const prdb_sdk_sql_parameter_metadata_v1 *parameter_metadata,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity,
    prdb_sdk_value_v1 *cell_scratch, uint32_t cell_scratch_capacity,
    prdb_sdk_sql_result_row_v1_fn on_row, void *result_context) {
    if (host == NULL ||
        host->v5.v4.v3.struct_size < sizeof(prdb_host_api_v3) ||
        host->v5.v4.v3.abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->spill_request == NULL)
        return PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT;
    return execute_select_internal(
        &host->v5.v4.v3, schema, schema_len, host, sql, sql_len, parameters,
        parameter_metadata, parameter_count, max_candidates, max_batch_bytes,
        planner_response, planner_response_capacity, executor_response,
        executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns,
        executor_column_capacity, cell_scratch, cell_scratch_capacity, NULL,
        0, PRDB_SDK_SQL_ABI_V1_VERSION, on_row, result_context);
}

int32_t prdb_sdk_sql_execute_select_typed_v1(
    const prdb_host_api_v3 *host, const uint8_t *schema, uint32_t schema_len,
    const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters,
    const prdb_sdk_sql_parameter_metadata_v1 *parameter_metadata,
    uint32_t parameter_count, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *planner_response,
    uint64_t planner_response_capacity, uint8_t *executor_response,
    uint64_t executor_response_capacity, uint8_t *batch_response,
    uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, prdb_sdk_sql_result_row_v1_fn on_row,
    void *result_context) {
    if (host == NULL || host->struct_size < sizeof(*host) ||
        host->abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->statement_failure == NULL) {
        return PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT;
    }
    if (parameter_count != 0 && parameter_metadata == NULL) {
        return fail_statement(host, PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT);
    }
    return execute_select_internal(
        host, schema, schema_len, NULL, sql, sql_len, parameters, parameter_metadata,
        parameter_count, max_candidates, max_batch_bytes, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns,
        executor_column_capacity, cell_scratch, cell_scratch_capacity, NULL, 0,
        PRDB_SDK_SQL_ABI_V1_VERSION, on_row, result_context);
}

int32_t prdb_sdk_sql_execute_insert_v1(
    const prdb_host_api_v3 *host_v3, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *insert_request, uint64_t insert_request_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
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

static uint64_t insert_batch_row_record_bytes(
    const prdb_sdk_executor_insert_value_v1 *values, uint32_t value_count) {
    uint64_t bytes = PRDB_EXECUTOR_STAGE_INSERT_REQUEST_HEADER_BYTES;
    for (uint32_t i = 0; i < value_count; ++i) {
        uint64_t cell_bytes = PRDB_MUTATION_CELL_TAG_RECORD_HEADER_BYTES;
        if (values[i].state == PRDB_SDK_EXECUTOR_INSERT_VALUE) {
            if (values[i].value.data_len > UINT64_MAX - cell_bytes -
                    PRDB_SCALAR_VALUE_RECORD_HEADER_BYTES) return UINT64_MAX;
            cell_bytes += PRDB_SCALAR_VALUE_RECORD_HEADER_BYTES +
                          values[i].value.data_len;
        }
        if (bytes > UINT64_MAX - cell_bytes) return UINT64_MAX;
        bytes += cell_bytes;
    }
    return bytes;
}

static uint32_t insert_batch_count(
    const prdb_sdk_executor_insert_value_v1 *values, uint32_t row_count,
    uint32_t value_count, uint64_t frame_capacity) {
    if (frame_capacity < 8) return 0;
    uint32_t batches = 0;
    uint32_t first = 0;
    while (first < row_count) {
        uint64_t bytes = 4;
        uint32_t count = 0;
        while (first + count < row_count && count < 128) {
            uint64_t record_bytes = insert_batch_row_record_bytes(
                values + (uint64_t)(first + count) * value_count, value_count);
            if (record_bytes == UINT64_MAX || record_bytes > UINT32_MAX ||
                record_bytes > frame_capacity - bytes - 4) break;
            bytes += 4 + record_bytes;
            ++count;
        }
        if (count == 0) return 0;
        first += count;
        ++batches;
    }
    return batches;
}

int32_t prdb_sdk_sql_execute_insert_batch_v1(
    const prdb_host_api_v3 *host_v3, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *insert_request, uint64_t insert_request_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity,
    prdb_sdk_executor_insert_value_v1 *insert_values,
    uint32_t insert_value_capacity, uint32_t *inserted_count_out) {
    prdb_sdk_insert_v1 statement_v3 = {0};
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
        insert_values == NULL || inserted_count_out == NULL ||
        parameter_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        (parameter_count != 0 && parameters == NULL) ||
        planner_column_capacity == 0 || executor_column_capacity == 0 ||
        planner_response_capacity == 0 || executor_response_capacity == 0 ||
        insert_request_capacity == 0 ||
        insert_request_capacity > PRDB_QUERY_MAX_IPC_REQUEST_BYTES) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT);
    }
    status = prdb_sdk_sql_parse_insert_v1(sql, sql_len, &statement_v3,
                                          sizeof(statement_v3), NULL);
    if (status != PRDB_SDK_SQL_OK) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    }
    prdb_sdk_insert_v1 *statement = &statement_v3;
    status = prdb_sdk_planner_describe_relation_v1(
        host, schema, schema_len,
        sql + statement->relation.offset,
        statement->relation.length, planner_response,
        planner_response_capacity, &planner_response_len, &planned,
        sizeof(planned), planner_columns, planner_column_capacity);
    if (status != PRDB_SDK_PLANNER_REQUEST_OK) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_PLANNER_ERROR);
    }
    status = prdb_sdk_executor_resolve_relation_v1(
        host, schema, schema_len,
        sql + statement->relation.offset,
        statement->relation.length, executor_response,
        executor_response_capacity, &executor_response_len, &resolved,
        sizeof(resolved), executor_columns, executor_column_capacity);
    if (status != PRDB_SDK_EXECUTOR_OK) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    }
    if (!same_relation_shape(&planned, &resolved, planner_columns,
                             executor_columns)) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH);
    }
    uint64_t required_insert_cells = (uint64_t)statement->row_count *
                                     resolved.column_count;
    if (statement->row_count == 0 || statement->values_per_row == 0 ||
        (statement_v3.column_count == 0 &&
         statement->values_per_row != resolved.column_count) ||
        (statement_v3.column_count != 0 &&
         statement_v3.column_count != statement->values_per_row) ||
        statement_v3.column_count > resolved.column_count ||
        resolved.column_count > PRDB_SDK_SQL_MAX_INSERT_VALUES ||
        required_insert_cells > insert_value_capacity) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    }

    uint32_t target_ordinals[PRDB_SDK_SQL_MAX_INSERT_VALUES];
    for (uint32_t source = 0; source < statement->values_per_row; ++source) {
        uint32_t target = source;
        if (statement_v3.column_count != 0) {
            target = resolved.column_count;
            for (uint32_t ordinal = 0; ordinal < resolved.column_count; ++ordinal) {
                if (span_matches_name(sql, statement_v3.columns[source],
                                      executor_columns[ordinal].name,
                                      executor_columns[ordinal].name_len)) {
                    target = ordinal;
                    break;
                }
            }
            if (target == resolved.column_count) {
                return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
            }
            for (uint32_t previous = 0; previous < source; ++previous) {
                if (target_ordinals[previous] == target) {
                    return fail_statement(host_v3,
                                          PRDB_SDK_SQL_EXEC_BIND_ERROR);
                }
            }
        }
        target_ordinals[source] = target;
    }

    for (uint32_t row = 0; row < statement->row_count; ++row) {
        uint32_t first_cell = row * statement->values_per_row;
        prdb_sdk_executor_insert_value_v1 *row_values =
            insert_values + (uint64_t)row * resolved.column_count;
        for (uint32_t ordinal = 0; ordinal < resolved.column_count; ++ordinal) {
            row_values[ordinal].struct_size = sizeof(row_values[ordinal]);
            row_values[ordinal].abi_version = PRDB_SDK_EXECUTOR_ABI_V1_VERSION;
            row_values[ordinal].state = PRDB_SDK_EXECUTOR_INSERT_DEFAULT;
            row_values[ordinal].value = (prdb_sdk_value_v1){0};
        }
        for (uint32_t column = 0; column < statement->values_per_row; ++column) {
            const prdb_sdk_sql_insert_cell_v1 *cell =
                &statement->values[first_cell + column];
            uint32_t target = target_ordinals[column];
            prdb_sdk_executor_insert_value_v1 *input = &row_values[target];
            input->struct_size = sizeof(*input);
            input->abi_version = PRDB_SDK_EXECUTOR_ABI_V1_VERSION;
            input->state = cell->kind;
            if (cell->kind == PRDB_SDK_SQL_INSERT_PARAMETER) {
                status = prdb_sdk_sql_bind_parameter_v1(
                    cell->parameter_index, parameters, parameter_count,
                    executor_columns[target].type_code,
                    executor_columns[target].decimal_precision,
                    executor_columns[target].decimal_scale, &input->value,
                    sizeof(input->value));
                if (status != PRDB_SDK_SQL_EVAL_OK) {
                    return fail_statement(host_v3,
                                          PRDB_SDK_SQL_EXEC_BIND_ERROR);
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
                return fail_statement(host_v3,
                                      PRDB_SDK_SQL_EXEC_BIND_ERROR);
            }
        }
    }
    uint64_t frame_capacity = insert_request_capacity <
            PRDB_QUERY_MAX_IPC_REQUEST_BYTES
        ? insert_request_capacity : PRDB_QUERY_MAX_IPC_REQUEST_BYTES;
    uint32_t batches = insert_batch_count(insert_values, statement->row_count,
                                           resolved.column_count, frame_capacity);
    if (batches == 0 || 2U + batches * 2U > 256U) {
        return fail_statement(host_v3, batches == 0
            ? PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR : PRDB_SDK_SQL_EXEC_REQUEST_LIMIT);
    }
    uint8_t row_handles[128 * 16];
    for (uint32_t first = 0; first < statement->row_count;) {
        uint64_t bytes = 4;
        uint32_t count = 0;
        while (first + count < statement->row_count && count < 128) {
            uint64_t record_bytes = insert_batch_row_record_bytes(
                insert_values + (uint64_t)(first + count) * resolved.column_count,
                resolved.column_count);
            if (record_bytes == UINT64_MAX || record_bytes > UINT32_MAX ||
                record_bytes > frame_capacity - bytes - 4) break;
            bytes += 4 + record_bytes;
            ++count;
        }
        int32_t cancelled = 0;
        status = prdb_sdk_executor_check_cancelled_v1(host, &cancelled);
        if (status != PRDB_SDK_EXECUTOR_OK || cancelled) {
            return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
        }
        status = prdb_sdk_executor_stage_inserts_v1(
            host, resolved.executor_handle,
            insert_values + (uint64_t)first * resolved.column_count,
            (uint16_t)count, (uint16_t)resolved.column_count, insert_request,
            insert_request_capacity, executor_response,
            executor_response_capacity, &executor_response_len, row_handles,
            sizeof(row_handles));
        if (status != PRDB_SDK_EXECUTOR_OK) {
            return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
        }
        first += count;
    }
    *inserted_count_out = statement->row_count;
    return PRDB_SDK_SQL_EXEC_OK;
}

int32_t prdb_sdk_sql_execute_update_v1(
    const prdb_host_api_v3 *host_v3, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    uint8_t *update_request, uint64_t update_request_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity,
    prdb_sdk_select_binding_v1 *binding, uint32_t binding_capacity,
    prdb_sdk_value_v1 *cell_scratch, uint32_t cell_scratch_capacity,
    uint8_t *target_handles, uint32_t target_capacity,
    prdb_sdk_executor_update_patch_v1 *patches, uint32_t patch_capacity,
    uint32_t *updated_count_out) {
    prdb_sdk_update_v1 statement = {0};
    prdb_sdk_planner_relation_v1 planned = {0};
    prdb_sdk_executor_relation_v1 resolved = {0};
    prdb_sdk_batch_v1 batch = {0};
    prdb_sdk_row_iterator_v1 rows = {0};
    prdb_sdk_row_v1 row = {0};
    uint8_t cursor[16] = {0};
    uint64_t planner_response_len = 0;
    uint64_t executor_response_len = 0;
    uint64_t batch_response_len = 0;
    uint32_t target_count = 0;
    uint32_t page_count = 0;
    uint32_t close_count = 0;
    int cursor_open = 0;
    int32_t final_status = PRDB_SDK_SQL_EXEC_OK;
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
        executor_response == NULL || batch_response == NULL ||
        update_request == NULL || planner_columns == NULL ||
        executor_columns == NULL || binding == NULL || cell_scratch == NULL ||
        target_handles == NULL || patches == NULL || updated_count_out == NULL ||
        parameter_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        (parameter_count != 0 && parameters == NULL) ||
        max_candidates == 0 || max_candidates > PRDB_SDK_BATCH_MAX_ROWS ||
        max_batch_bytes < 8 || max_batch_bytes > PRDB_SDK_BATCH_MAX_BYTES ||
        batch_response_capacity < max_batch_bytes ||
        planner_response_capacity == 0 || executor_response_capacity == 0 ||
        update_request_capacity == 0 ||
        update_request_capacity > PRDB_QUERY_MAX_IPC_REQUEST_BYTES ||
        planner_column_capacity == 0 || executor_column_capacity == 0 ||
        binding_capacity < sizeof(*binding) || cell_scratch_capacity == 0 ||
        target_capacity == 0 ||
        target_capacity > PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT);
    }
    status = prdb_sdk_sql_parse_update_v1(sql, sql_len, &statement,
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
    if (resolved.column_count > PRDB_SDK_SQL_MAX_RELATION_COLUMNS ||
        resolved.column_count > cell_scratch_capacity ||
        statement.assignment_count == 0 ||
        statement.assignment_count > patch_capacity) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    }
    status = prdb_sdk_sql_bind_select_v1(
        &statement.predicate_statement, resolved.name, resolved.name_len,
        executor_columns, resolved.column_count, binding, binding_capacity);
    if (status != PRDB_SDK_SQL_BIND_OK) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    }
    for (uint32_t index = 0; index < statement.assignment_count; ++index) {
        uint32_t ordinal = binding->projection_ordinals[index];
        prdb_sdk_sql_update_assignment_v1 *assignment =
            &statement.assignments[index];
        if (ordinal >= resolved.column_count) {
            return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
        }
        for (uint32_t prior = 0; prior < index; ++prior) {
            if (binding->projection_ordinals[prior] == ordinal) {
                return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
            }
        }
        patches[index].struct_size = sizeof(patches[index]);
        patches[index].abi_version = PRDB_SDK_EXECUTOR_ABI_V1_VERSION;
        patches[index].ordinal = ordinal;
        if (assignment->kind == PRDB_SDK_SQL_UPDATE_PARAMETER) {
            status = prdb_sdk_sql_bind_parameter_v1(
                assignment->parameter_index, parameters, parameter_count,
                executor_columns[ordinal].type_code,
                executor_columns[ordinal].decimal_precision,
                executor_columns[ordinal].decimal_scale,
                &patches[index].value, sizeof(patches[index].value));
            if (status != PRDB_SDK_SQL_EVAL_OK) {
                return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
            }
        } else if (assignment->kind == PRDB_SDK_SQL_UPDATE_NULL) {
            patches[index].value = (prdb_sdk_value_v1){
                sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
                PRDB_SDK_VALUE_STATE_NULL, 0, NULL, 0};
        } else {
            return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
        }
    }
    for (uint32_t index = 0;
         index < statement.predicate_statement.predicate_count; ++index) {
        const prdb_sdk_predicate_node_v1 *node =
            &statement.predicate_statement.predicates[index];
        if (node->node_kind == PRDB_SDK_SQL_PREDICATE_COMPARE) {
            const prdb_sdk_sql_bound_column_v1 *column =
                &binding->predicate_columns[index];
            prdb_sdk_value_v1 bound_parameter = {0};
            status = prdb_sdk_sql_bind_parameter_v1(
                node->parameter_index, parameters, parameter_count,
                column->type_code, column->decimal_precision,
                column->decimal_scale, &bound_parameter,
                sizeof(bound_parameter));
            if (status != PRDB_SDK_SQL_EVAL_OK) {
                return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
            }
        }
    }
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
        int32_t cancelled = 0;
        status = prdb_sdk_executor_check_cancelled_v1(host, &cancelled);
        if (status != PRDB_SDK_EXECUTOR_OK || cancelled) {
            final_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
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
            uint32_t truth = PRDB_SDK_SQL_TRUTH_UNKNOWN;
            status = prdb_sdk_batch_row_next_v1(&rows, &row, sizeof(row));
            if (status != PRDB_SDK_BATCH_OK) {
                final_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
                break;
            }
            status = prdb_sdk_sql_eval_bound_select_row_v1(
                &statement.predicate_statement, binding, parameters,
                parameter_count, &row, cell_scratch,
                cell_scratch_capacity, &truth);
            if (status != PRDB_SDK_SQL_EVAL_OK) {
                final_status = PRDB_SDK_SQL_EXEC_EVALUATION_ERROR;
                break;
            }
            if (truth == PRDB_SDK_SQL_TRUTH_TRUE) {
                if (target_count >= target_capacity) {
                    final_status = PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT;
                    break;
                }
                memcpy(target_handles + (uint64_t)target_count * 16,
                       row.handle, 16);
                ++target_count;
            }
        }
        if (final_status != PRDB_SDK_SQL_EXEC_OK || batch.finished) break;
    }
    if (cursor_open) {
        status = prdb_sdk_executor_close_cursor_v1(
            host, cursor, executor_response, executor_response_capacity,
            &executor_response_len);
        ++close_count;
        if (status != PRDB_SDK_EXECUTOR_OK &&
            final_status == PRDB_SDK_SQL_EXEC_OK) {
            final_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
        }
    }
    if (final_status != PRDB_SDK_SQL_EXEC_OK) {
        return fail_statement(host_v3, final_status);
    }
    uint64_t update_record_bytes = PRDB_EXECUTOR_STAGE_UPDATE_REQUEST_HEADER_BYTES;
    for (uint32_t patch_index = 0; patch_index < statement.assignment_count;
         ++patch_index) {
        const prdb_sdk_executor_update_patch_v1 *patch = &patches[patch_index];
        uint64_t cell_bytes = PRDB_PATCH_ORDINAL_RECORD_HEADER_BYTES +
                              PRDB_MUTATION_CELL_TAG_RECORD_HEADER_BYTES;
        if (patch->value.state == PRDB_SDK_VALUE_STATE_VALUE) {
            if (patch->value.data_len > UINT64_MAX - cell_bytes -
                    PRDB_SCALAR_VALUE_RECORD_HEADER_BYTES) {
                return fail_statement(host_v3,
                                      PRDB_SDK_SQL_EXEC_REQUEST_LIMIT);
            }
            cell_bytes += PRDB_SCALAR_VALUE_RECORD_HEADER_BYTES +
                          patch->value.data_len;
        }
        if (update_record_bytes > UINT64_MAX - cell_bytes) {
            return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_REQUEST_LIMIT);
        }
        update_record_bytes += cell_bytes;
    }
    uint64_t update_frame_capacity = update_request_capacity <
            PRDB_QUERY_MAX_IPC_REQUEST_BYTES
        ? update_request_capacity : PRDB_QUERY_MAX_IPC_REQUEST_BYTES;
    if (update_record_bytes > UINT32_MAX || update_frame_capacity <= 4 ||
        update_record_bytes + 4 > update_frame_capacity) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    }
    uint32_t rows_per_update_batch = (uint32_t)((update_frame_capacity - 4) /
        (update_record_bytes + 4));
    if (rows_per_update_batch == 0) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    }
    uint32_t update_batch_count = target_count == 0 ? 0 :
        (target_count + rows_per_update_batch - 1) / rows_per_update_batch;
    if (2U + page_count + close_count + update_batch_count +
            (update_batch_count != 0) > 256U) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_REQUEST_LIMIT);
    }
    if (update_batch_count != 0) {
        int32_t cancelled = 0;
        status = prdb_sdk_executor_check_cancelled_v1(host, &cancelled);
        if (status != PRDB_SDK_EXECUTOR_OK || cancelled) {
            return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
        }
    }
    for (uint32_t first = 0; first < target_count;) {
        uint32_t count = target_count - first;
        if (count > rows_per_update_batch) count = rows_per_update_batch;
        status = prdb_sdk_executor_stage_updates_v1(
            host, target_handles + (uint64_t)first * 16, (uint16_t)count,
            patches, (uint16_t)statement.assignment_count, update_request,
            update_request_capacity, executor_response,
            executor_response_capacity, &executor_response_len);
        if (status != PRDB_SDK_EXECUTOR_OK) {
            return fail_statement(host_v3,
                                  PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
        }
        first += count;
    }
    *updated_count_out = target_count;
    return PRDB_SDK_SQL_EXEC_OK;
}

int32_t prdb_sdk_sql_execute_delete_v1(
    const prdb_host_api_v3 *host_v3, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v1 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v1 *executor_columns,
    uint32_t executor_column_capacity,
    prdb_sdk_select_binding_v1 *binding, uint32_t binding_capacity,
    prdb_sdk_value_v1 *cell_scratch, uint32_t cell_scratch_capacity,
    uint8_t *target_handles, uint32_t target_capacity,
    uint32_t *deleted_count_out) {
    prdb_sdk_delete_v1 statement = {0};
    prdb_sdk_planner_relation_v1 planned = {0};
    prdb_sdk_executor_relation_v1 resolved = {0};
    prdb_sdk_batch_v1 batch = {0};
    prdb_sdk_row_iterator_v1 rows = {0};
    prdb_sdk_row_v1 row = {0};
    uint8_t cursor[16] = {0};
    uint64_t planner_response_len = 0;
    uint64_t executor_response_len = 0;
    uint64_t batch_response_len = 0;
    uint32_t target_count = 0;
    uint32_t page_count = 0;
    uint32_t close_count = 0;
    int cursor_open = 0;
    int32_t final_status = PRDB_SDK_SQL_EXEC_OK;
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
        executor_response == NULL || batch_response == NULL ||
        planner_columns == NULL || executor_columns == NULL || binding == NULL ||
        cell_scratch == NULL ||
        target_handles == NULL || deleted_count_out == NULL ||
        parameter_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        (parameter_count != 0 && parameters == NULL) ||
        max_candidates == 0 || max_candidates > PRDB_SDK_BATCH_MAX_ROWS ||
        max_batch_bytes < 8 || max_batch_bytes > PRDB_SDK_BATCH_MAX_BYTES ||
        batch_response_capacity < max_batch_bytes ||
        planner_response_capacity == 0 || executor_response_capacity == 0 ||
        planner_column_capacity == 0 || executor_column_capacity == 0 ||
        binding_capacity < sizeof(*binding) || cell_scratch_capacity == 0 ||
        target_capacity == 0 ||
        target_capacity > PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT);
    }
    status = prdb_sdk_sql_parse_delete_v1(sql, sql_len, &statement,
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
    if (resolved.column_count > PRDB_SDK_SQL_MAX_RELATION_COLUMNS ||
        resolved.column_count > cell_scratch_capacity) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    }
    status = prdb_sdk_sql_bind_select_v1(
        &statement.predicate_statement, resolved.name, resolved.name_len,
        executor_columns, resolved.column_count, binding, binding_capacity);
    if (status != PRDB_SDK_SQL_BIND_OK) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    }
    for (uint32_t index = 0;
         index < statement.predicate_statement.predicate_count; ++index) {
        const prdb_sdk_predicate_node_v1 *node =
            &statement.predicate_statement.predicates[index];
        if (node->node_kind == PRDB_SDK_SQL_PREDICATE_COMPARE) {
            const prdb_sdk_sql_bound_column_v1 *column =
                &binding->predicate_columns[index];
            prdb_sdk_value_v1 bound_parameter = {0};
            status = prdb_sdk_sql_bind_parameter_v1(
                node->parameter_index, parameters, parameter_count,
                column->type_code, column->decimal_precision,
                column->decimal_scale, &bound_parameter,
                sizeof(bound_parameter));
            if (status != PRDB_SDK_SQL_EVAL_OK) {
                return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
            }
        }
    }
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
        int32_t cancelled = 0;
        status = prdb_sdk_executor_check_cancelled_v1(host, &cancelled);
        if (status != PRDB_SDK_EXECUTOR_OK || cancelled) {
            final_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
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
            uint32_t truth = PRDB_SDK_SQL_TRUTH_UNKNOWN;
            status = prdb_sdk_batch_row_next_v1(&rows, &row, sizeof(row));
            if (status != PRDB_SDK_BATCH_OK) {
                final_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
                break;
            }
            status = prdb_sdk_sql_eval_bound_select_row_v1(
                &statement.predicate_statement, binding, parameters,
                parameter_count, &row, cell_scratch,
                cell_scratch_capacity, &truth);
            if (status != PRDB_SDK_SQL_EVAL_OK) {
                final_status = PRDB_SDK_SQL_EXEC_EVALUATION_ERROR;
                break;
            }
            if (truth == PRDB_SDK_SQL_TRUTH_TRUE) {
                if (target_count >= target_capacity) {
                    final_status = PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT;
                    break;
                }
                memcpy(target_handles + (uint64_t)target_count * 16,
                       row.handle, 16);
                ++target_count;
            }
        }
        if (final_status != PRDB_SDK_SQL_EXEC_OK || batch.finished) break;
    }
    if (cursor_open) {
        status = prdb_sdk_executor_close_cursor_v1(
            host, cursor, executor_response, executor_response_capacity,
            &executor_response_len);
        ++close_count;
        if (status != PRDB_SDK_EXECUTOR_OK &&
            final_status == PRDB_SDK_SQL_EXEC_OK) {
            final_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
        }
    }
    if (final_status != PRDB_SDK_SQL_EXEC_OK) {
        return fail_statement(host_v3, final_status);
    }
    uint64_t delete_frame_capacity = batch_response_capacity <
            PRDB_QUERY_MAX_IPC_REQUEST_BYTES
        ? batch_response_capacity : PRDB_QUERY_MAX_IPC_REQUEST_BYTES;
    if (delete_frame_capacity < 26) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    }
    uint32_t rows_per_delete_batch = (uint32_t)((delete_frame_capacity - 4) / 22);
    uint32_t delete_batch_count = target_count == 0 ? 0 :
        (target_count + rows_per_delete_batch - 1) / rows_per_delete_batch;
    if (2U + page_count + close_count + delete_batch_count +
            (delete_batch_count != 0) > 256U) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_REQUEST_LIMIT);
    }
    if (delete_batch_count != 0) {
        int32_t cancelled = 0;
        status = prdb_sdk_executor_check_cancelled_v1(host, &cancelled);
        if (status != PRDB_SDK_EXECUTOR_OK || cancelled) {
            return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
        }
    }
    for (uint32_t first = 0; first < target_count;) {
        uint32_t count = target_count - first;
        if (count > rows_per_delete_batch) count = rows_per_delete_batch;
        status = prdb_sdk_executor_stage_deletes_v1(
            host, target_handles + (uint64_t)first * 16, (uint16_t)count,
            batch_response, batch_response_capacity, executor_response,
            executor_response_capacity, &executor_response_len);
        if (status != PRDB_SDK_EXECUTOR_OK) {
            return fail_statement(host_v3,
                                  PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
        }
        first += count;
    }
    *deleted_count_out = target_count;
    return PRDB_SDK_SQL_EXEC_OK;
}
