#include "prdb_sdk_sql_exec_v7.h"
#include "prdb_sdk_sql_exec_v9.h"
#include "prdb_sdk_sql_exec_v10.h"
#include "prdb_sdk_sql_exec_v11.h"
#include "prdb_sdk_sql_exec_v12.h"
#include "prdb_sdk_sql_exec_v13.h"
#include "prdb_sdk_sql_v9.h"
#include "prdb_sdk_sql_v10.h"
#include "prdb_sdk_sql_order_v2.h"
#include "prdb_sdk_sql_order_v3.h"

#include <string.h>

#define SQL_EXEC_V7_MAX_NEXT_REQUESTS UINT32_C(252)

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

static int values_equal(const prdb_sdk_value_v1 *left,
                        const prdb_sdk_value_v1 *right,
                        uint8_t decimal_scale) {
    uint32_t truth = PRDB_SDK_SQL_TRUTH_FALSE;
    if (left->state == PRDB_SDK_VALUE_STATE_NULL &&
        right->state == PRDB_SDK_VALUE_STATE_NULL) return 1;
    if (left->state != right->state ||
        left->state != PRDB_SDK_VALUE_STATE_VALUE) return 0;
    return prdb_sdk_sql_compare_v1(
               left, right, PRDB_SDK_SQL_COMPARE_EQUAL, decimal_scale,
               decimal_scale, &truth) == PRDB_SDK_SQL_EVAL_OK &&
           truth == PRDB_SDK_SQL_TRUTH_TRUE;
}

static int32_t execute_group_internal(
    const prdb_host_api_v3 *host_v3,
    const uint8_t *schema, uint32_t schema_len,
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
    uint8_t *materialized_bytes, uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_group_row_v7_fn on_group,
    prdb_sdk_sql_group_row_v9_fn on_group_v9,
    prdb_sdk_sql_group_row_v10_fn on_group_v10,
    prdb_sdk_sql_group_row_v11_fn on_group_v11,
    prdb_sdk_sql_group_row_v12_fn on_group_v12,
    prdb_sdk_sql_group_row_v13_fn on_group_v13, void *result_context,
    int enable_having, int enable_ordering, int enable_null_placement,
    int enable_multi_order, int enable_composite_grouping) {
    prdb_sdk_select_v7 statement = {0};
    prdb_sdk_select_binding_v7 binding = {0};
    prdb_sdk_select_v9 statement_v9 = {0};
    prdb_sdk_select_binding_v9 binding_v9 = {0};
    prdb_sdk_select_v10 statement_v10 = {0};
    prdb_sdk_select_binding_v10 binding_v10 = {0};
    prdb_sdk_select_v11 statement_v11 = {0};
    prdb_sdk_select_binding_v11 binding_v11 = {0};
    prdb_sdk_select_v12 statement_v12 = {0};
    prdb_sdk_select_binding_v12 binding_v12 = {0};
    prdb_sdk_select_v13 statement_v13 = {0};
    prdb_sdk_select_binding_v13 binding_v13 = {0};
    prdb_sdk_value_v1 having_parameter = {0};
    prdb_sdk_planner_relation_v1 planned = {0};
    prdb_sdk_executor_relation_v1 resolved = {0};
    prdb_sdk_batch_v1 batch = {0};
    prdb_sdk_row_iterator_v1 rows = {0};
    prdb_sdk_row_v1 row = {0};
    prdb_sdk_value_v1 keys[PRDB_SDK_SQL_GROUP_MAX_ROWS];
    prdb_sdk_value_v1 composite_keys[
        PRDB_SDK_SQL_GROUP_MAX_ROWS * PRDB_SDK_SQL_GROUP_MAX_COLUMNS];
    uint32_t composite_types[PRDB_SDK_SQL_GROUP_MAX_COLUMNS] = {0};
    uint8_t composite_scales[PRDB_SDK_SQL_GROUP_MAX_COLUMNS] = {0};
    uint32_t composite_key_count = 1;
    prdb_sdk_value_v1 operands[PRDB_SDK_SQL_GROUP_MAX_ROWS];
    prdb_sdk_value_v1 aggregate_results[PRDB_SDK_SQL_GROUP_MAX_ROWS];
    uint8_t having_match[PRDB_SDK_SQL_GROUP_MAX_ROWS] = {0};
    uint8_t aggregate_result_bytes[PRDB_SDK_SQL_GROUP_MAX_ROWS][16] = {{0}};
    uint8_t aggregate_result_scales[PRDB_SDK_SQL_GROUP_MAX_ROWS] = {0};
    prdb_sdk_value_v1 group_operands[PRDB_SDK_SQL_GROUP_MAX_ROWS];
    prdb_sdk_sql_count_group_v1 groups[PRDB_SDK_SQL_GROUP_MAX_ROWS];
    uint32_t group_count = 0, matching_row_count = 0, page_count = 0;
    uint64_t materialized_used = 0;
    uint64_t planner_response_len = 0, executor_response_len = 0;
    uint64_t batch_response_len = 0;
    uint8_t cursor[16] = {0}, close_response[8] = {0};
    uint64_t close_response_len = 0;
    int cursor_open = 0;
    int32_t status, final_status = PRDB_SDK_SQL_EXEC_OK;
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
        planner_columns == NULL || executor_columns == NULL ||
        cell_scratch == NULL || materialized_bytes == NULL ||
        (on_group == NULL && on_group_v9 == NULL && on_group_v10 == NULL &&
         on_group_v11 == NULL && on_group_v12 == NULL &&
         on_group_v13 == NULL) ||
        planner_column_capacity == 0 || executor_column_capacity == 0 ||
        cell_scratch_capacity == 0 || planner_response_capacity == 0 ||
        executor_response_capacity == 0 ||
        batch_response_capacity < max_batch_bytes || max_candidates == 0 ||
        max_candidates > PRDB_SDK_BATCH_MAX_ROWS || max_batch_bytes < 8 ||
        max_batch_bytes > PRDB_SDK_BATCH_MAX_BYTES ||
            materialized_bytes_capacity >
            PRDB_SDK_SQL_EXEC_V7_MAX_MATERIALIZED_BYTES ||
        (parameter_count != 0 && parameters == NULL)) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT);
    }

    if (enable_composite_grouping) {
        status = prdb_sdk_sql_parse_select_v13(
            sql, sql_len, &statement_v13, sizeof(statement_v13), NULL);
        statement = statement_v13.base;
    } else if (enable_multi_order) {
        status = prdb_sdk_sql_parse_select_v12(
            sql, sql_len, &statement_v12, sizeof(statement_v12), NULL);
        statement_v11 = statement_v12.base;
        statement_v10 = statement_v11.base;
        statement_v9 = statement_v10.base;
        statement = statement_v9.base;
    } else if (enable_null_placement) {
        status = prdb_sdk_sql_parse_select_v11(
            sql, sql_len, &statement_v11, sizeof(statement_v11), NULL);
        statement_v10 = statement_v11.base;
        statement_v9 = statement_v10.base;
        statement = statement_v9.base;
    } else if (enable_ordering) {
        status = prdb_sdk_sql_parse_select_v10(sql, sql_len, &statement_v10,
                                               sizeof(statement_v10), NULL);
        statement_v9 = statement_v10.base;
        statement = statement_v9.base;
    } else if (enable_having) {
        status = prdb_sdk_sql_parse_select_v9(sql, sql_len, &statement_v9,
                                              sizeof(statement_v9), NULL);
        statement = statement_v9.base;
    } else {
        status = prdb_sdk_sql_parse_select_v7(sql, sql_len, &statement,
                                              sizeof(statement), NULL);
    }
    if (status != PRDB_SDK_SQL_OK) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    }
    status = prdb_sdk_planner_describe_relation_v1(
        host, schema, schema_len, statement.base.sql +
            statement.base.relation.offset,
        statement.base.relation.length, planner_response,
        planner_response_capacity, &planner_response_len, &planned,
        sizeof(planned), planner_columns, planner_column_capacity);
    if (status != PRDB_SDK_PLANNER_REQUEST_OK) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_PLANNER_ERROR);
    }
    status = prdb_sdk_executor_resolve_relation_v1(
        host, schema, schema_len, statement.base.sql +
            statement.base.relation.offset,
        statement.base.relation.length, executor_response,
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
    if (enable_composite_grouping) {
        status = prdb_sdk_sql_bind_select_v13(
            &statement_v13, resolved.name, resolved.name_len, executor_columns,
            resolved.column_count, &binding_v13, sizeof(binding_v13));
        binding = binding_v13.base;
        composite_key_count = binding_v13.group_key_count;
        for (uint32_t index = 0; index < binding_v13.group_key_count; ++index) {
            composite_types[index] = binding_v13.group_keys[index].type_code;
            composite_scales[index] =
                binding_v13.group_keys[index].decimal_scale;
        }
    } else if (enable_multi_order) {
        status = prdb_sdk_sql_bind_select_v12(
            &statement_v12, resolved.name, resolved.name_len, executor_columns,
            resolved.column_count, &binding_v12, sizeof(binding_v12));
        binding_v11 = binding_v12.base;
        binding_v10 = binding_v11.base;
        binding_v9 = binding_v10.base;
        binding = binding_v9.base;
    } else if (enable_null_placement) {
        status = prdb_sdk_sql_bind_select_v11(
            &statement_v11, resolved.name, resolved.name_len, executor_columns,
            resolved.column_count, &binding_v11, sizeof(binding_v11));
        binding_v10 = binding_v11.base;
        binding_v9 = binding_v10.base;
        binding = binding_v9.base;
    } else if (enable_ordering) {
        status = prdb_sdk_sql_bind_select_v10(
            &statement_v10, resolved.name, resolved.name_len, executor_columns,
            resolved.column_count, &binding_v10, sizeof(binding_v10));
        binding_v9 = binding_v10.base;
        binding = binding_v9.base;
    } else if (enable_having) {
        status = prdb_sdk_sql_bind_select_v9(
            &statement_v9, resolved.name, resolved.name_len, executor_columns,
            resolved.column_count, &binding_v9, sizeof(binding_v9));
        binding = binding_v9.base;
    } else {
        status = prdb_sdk_sql_bind_select_v7(
            &statement, resolved.name, resolved.name_len, executor_columns,
            resolved.column_count, &binding, sizeof(binding));
    }
    if (status != PRDB_SDK_SQL_BIND_OK) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    }
    if (enable_having && binding_v9.has_having) {
        status = prdb_sdk_sql_bind_parameter_v1(
            binding_v9.having_parameter_index, parameters, parameter_count,
            binding_v9.having_type_code,
            binding_v9.having_decimal_precision,
            binding_v9.having_decimal_scale, &having_parameter,
            sizeof(having_parameter));
        if (status != PRDB_SDK_SQL_EVAL_OK) {
            return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
        }
    }
    for (uint32_t index = 0; index < statement.base.predicate_count; ++index) {
        const prdb_sdk_predicate_node_v2 *node =
            &statement.base.predicates[index];
        if (node->node_kind == PRDB_SDK_SQL_PREDICATE_COMPARE) {
            const prdb_sdk_sql_bound_column_v2 *column =
                &binding.base.predicate_columns[index];
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
    if (statement.base.has_limit && statement.base.limit == 0) {
        return PRDB_SDK_SQL_EXEC_OK;
    }

    status = prdb_sdk_executor_open_full_scan_v1(
        host, resolved.executor_handle, executor_response,
        executor_response_capacity, &executor_response_len, cursor);
    if (status != PRDB_SDK_EXECUTOR_OK) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    }
    cursor_open = 1;
    for (;;) {
        if (page_count >= SQL_EXEC_V7_MAX_NEXT_REQUESTS) {
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
        if (batch.finished) cursor_open = 0;
        else memcpy(cursor, batch.continuation, sizeof(cursor));
        status = prdb_sdk_batch_rows_begin_v1(&batch, &rows, sizeof(rows));
        if (status != PRDB_SDK_BATCH_OK) {
            final_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
            break;
        }
        while (rows.rows_remaining != 0) {
            uint32_t truth = PRDB_SDK_SQL_TRUTH_TRUE;
            prdb_sdk_cell_iterator_v1 cells = {0};
            status = prdb_sdk_batch_row_next_v1(&rows, &row, sizeof(row));
            if (status != PRDB_SDK_BATCH_OK ||
                row.cell_count != resolved.column_count) {
                final_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
                break;
            }
            if (statement.base.predicate_count != 0) {
                status = prdb_sdk_sql_eval_bound_select_row_v2(
                    &statement.base, &binding.base, parameters,
                    parameter_count, &row, cell_scratch,
                    cell_scratch_capacity, &truth);
                if (status != PRDB_SDK_SQL_EVAL_OK) {
                    final_status = PRDB_SDK_SQL_EXEC_EVALUATION_ERROR;
                    break;
                }
            }
            if (truth != PRDB_SDK_SQL_TRUTH_TRUE) continue;
            if (matching_row_count >= PRDB_SDK_SQL_GROUP_MAX_ROWS) {
                final_status = PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT;
                break;
            }
            if (prdb_sdk_row_cells_begin_v1(&row, &cells, sizeof(cells)) !=
                PRDB_SDK_BATCH_OK) {
                final_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
                break;
            }
            for (uint32_t ordinal = 0; ordinal < resolved.column_count;
                 ++ordinal) {
                if (prdb_sdk_cell_next_v1(&cells, &cell_scratch[ordinal],
                                          sizeof(cell_scratch[ordinal])) !=
                    PRDB_SDK_BATCH_OK) {
                    final_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
                    break;
                }
            }
            if (final_status != PRDB_SDK_SQL_EXEC_OK) break;
            uint32_t key_count = enable_composite_grouping
                                     ? binding_v13.group_key_count
                                     : 1;
            for (uint32_t key = 0; key < key_count; ++key) {
                const uint32_t ordinal = enable_composite_grouping
                                             ? binding_v13.group_keys[key].ordinal
                                             : binding.group_by_column.ordinal;
                composite_keys[matching_row_count * composite_key_count + key] =
                    cell_scratch[ordinal];
                if (key == 0) keys[matching_row_count] =
                    composite_keys[matching_row_count * composite_key_count +
                                   key];
            }
            operands[matching_row_count] =
                binding.aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR
                    ? (prdb_sdk_value_v1){0}
                    : cell_scratch[binding.aggregate_column.ordinal];
            for (uint32_t value_index = 0;
                 value_index < key_count +
                                   (binding.aggregate_kind ==
                                            PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR
                                        ? 0
                                        : 1);
                 ++value_index) {
                prdb_sdk_value_v1 *value =
                    value_index < key_count
                        ? &composite_keys[matching_row_count *
                                              composite_key_count +
                                          value_index]
                        : &operands[matching_row_count];
                if (value->state == PRDB_SDK_VALUE_STATE_VALUE) {
                    uint64_t length = value->data_len;
                    if (length > materialized_bytes_capacity - materialized_used) {
                        final_status = PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT;
                        break;
                    }
                    if (length != 0) {
                        memcpy(materialized_bytes + materialized_used,
                               value->data, (size_t)length);
                    }
                    value->data = materialized_bytes + materialized_used;
                    materialized_used += length;
                }
            }
            if (final_status != PRDB_SDK_SQL_EXEC_OK) break;
            keys[matching_row_count] =
                composite_keys[matching_row_count * composite_key_count];
            ++matching_row_count;
        }
        if (final_status != PRDB_SDK_SQL_EXEC_OK || batch.finished) break;
    }
    if (cursor_open) {
        status = prdb_sdk_executor_close_cursor_v1(
            host, cursor, close_response, sizeof(close_response),
            &close_response_len);
        cursor_open = 0;
        if (status != PRDB_SDK_EXECUTOR_OK &&
            final_status == PRDB_SDK_SQL_EXEC_OK) {
            final_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
        }
    }
    if (final_status != PRDB_SDK_SQL_EXEC_OK) {
        return fail_statement(host_v3, final_status);
    }
    if (enable_composite_grouping) {
        status = prdb_sdk_sql_group_count_composite_v1(
            composite_keys, matching_row_count,
            binding_v13.group_key_count, composite_types, composite_scales,
            groups, PRDB_SDK_SQL_GROUP_MAX_ROWS, &group_count);
    } else {
        status = prdb_sdk_sql_group_count_v1(
            keys, matching_row_count, binding.group_by_column.type_code,
            binding.group_by_column.decimal_scale, groups,
            PRDB_SDK_SQL_GROUP_MAX_ROWS, &group_count);
    }
    if (status != PRDB_SDK_SQL_GROUP_OK) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
    }
    if (binding.aggregate_kind != PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
        for (uint32_t group = 0; group < group_count; ++group) {
            uint32_t operand_count = 0;
            const prdb_sdk_value_v1 *representative =
                &keys[groups[group].representative_row];
            for (uint32_t row_index = 0; row_index < matching_row_count;
                 ++row_index) {
                int same_group = 0;
                if (enable_composite_grouping) {
                    same_group = 1;
                    for (uint32_t key = 0;
                         key < binding_v13.group_key_count; ++key) {
                        if (!values_equal(
                                &composite_keys[
                                    groups[group].representative_row *
                                        composite_key_count +
                                    key],
                                &composite_keys[row_index *
                                                    composite_key_count + key],
                                composite_scales[key])) {
                            same_group = 0;
                            break;
                        }
                    }
                } else {
                    same_group = values_equal(
                        representative, &keys[row_index],
                        binding.group_by_column.decimal_scale);
                }
                if (same_group) {
                    group_operands[operand_count++] = operands[row_index];
                }
            }
            if (binding.aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_SUM) {
                status = prdb_sdk_sql_sum_v1(
                    group_operands, operand_count,
                    binding.aggregate_column.type_code,
                    binding.aggregate_column.decimal_precision,
                    binding.aggregate_column.decimal_scale,
                    &aggregate_results[group], sizeof(aggregate_results[group]),
                    aggregate_result_bytes[group],
                    sizeof(aggregate_result_bytes[group]));
            } else if (binding.aggregate_kind ==
                       PRDB_SDK_SQL_V7_AGGREGATE_AVG) {
                status = prdb_sdk_sql_avg_v1(
                    group_operands, operand_count,
                    binding.aggregate_column.type_code,
                    binding.aggregate_column.decimal_precision,
                    binding.aggregate_column.decimal_scale,
                    &aggregate_results[group], sizeof(aggregate_results[group]),
                    aggregate_result_bytes[group],
                    sizeof(aggregate_result_bytes[group]),
                    &aggregate_result_scales[group]);
            } else {
                uint32_t operation = binding.aggregate_kind ==
                                             PRDB_SDK_SQL_V7_AGGREGATE_MIN
                                         ? PRDB_SDK_SQL_MINMAX_MIN
                                         : PRDB_SDK_SQL_MINMAX_MAX;
                status = prdb_sdk_sql_minmax_v1(
                    group_operands, operand_count,
                    binding.aggregate_column.type_code,
                    binding.aggregate_column.decimal_scale, operation,
                    &aggregate_results[group],
                    sizeof(aggregate_results[group]));
            }
            if (status != PRDB_SDK_SQL_EVAL_OK) {
                return fail_statement(host_v3,
                                      PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
            }
        }
    }
    if (enable_having && binding_v9.has_having) {
        for (uint32_t group = 0; group < group_count; ++group) {
            uint8_t count_bytes[8];
            prdb_sdk_value_v1 count_value = {0};
            const prdb_sdk_value_v1 *aggregate =
                binding.aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR
                    ? NULL
                    : &aggregate_results[group];
            const prdb_sdk_value_v1 *comparison_value = aggregate;
            uint32_t truth = PRDB_SDK_SQL_TRUTH_FALSE;
            if (binding.aggregate_kind ==
                PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
                uint64_t count = groups[group].count;
                for (uint32_t byte = 0; byte < 8; ++byte) {
                    count_bytes[7 - byte] = (uint8_t)(count & UINT64_C(0xff));
                    count >>= 8;
                }
                count_value.struct_size = sizeof(count_value);
                count_value.abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
                count_value.type_code = PRDB_SDK_VALUE_TYPE_BIGINT;
                count_value.state = PRDB_SDK_VALUE_STATE_VALUE;
                count_value.data = count_bytes;
                count_value.data_len = sizeof(count_bytes);
                comparison_value = &count_value;
            }
            status = prdb_sdk_sql_compare_v1(
                comparison_value, &having_parameter,
                binding_v9.having_comparison,
                binding_v9.having_decimal_scale,
                binding_v9.having_decimal_scale, &truth);
            if (status != PRDB_SDK_SQL_EVAL_OK) {
                return fail_statement(host_v3,
                                      PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
            }
            having_match[group] = (uint8_t)(truth == PRDB_SDK_SQL_TRUTH_TRUE);
        }
    }
    {
        uint32_t output_group_indices[PRDB_SDK_SQL_GROUP_MAX_ROWS];
        uint32_t output_group_count = 0;
        if (enable_ordering && statement_v10.has_order_by) {
            prdb_sdk_value_v1 sort_keys[PRDB_SDK_SQL_GROUP_MAX_ROWS];
            uint8_t count_bytes[PRDB_SDK_SQL_GROUP_MAX_ROWS][8];
            uint32_t group_for_sort_key[PRDB_SDK_SQL_GROUP_MAX_ROWS];
            uint32_t sorted_indices[PRDB_SDK_SQL_GROUP_MAX_ROWS];
            uint32_t sort_key_count = 0;
            for (uint32_t group = 0; group < group_count; ++group) {
                if (binding_v10.base.has_having && !having_match[group]) {
                    continue;
                }
                group_for_sort_key[sort_key_count] = group;
                if (statement_v10.order_kind ==
                    PRDB_SDK_SQL_V10_ORDER_GROUP_KEY) {
                    sort_keys[sort_key_count] =
                        keys[groups[group].representative_row];
                } else if (binding.aggregate_kind ==
                           PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
                    uint64_t count = groups[group].count;
                    for (uint32_t byte = 0; byte < 8; ++byte) {
                        count_bytes[sort_key_count][7 - byte] =
                            (uint8_t)(count & UINT64_C(0xff));
                        count >>= 8;
                    }
                    sort_keys[sort_key_count].struct_size =
                        sizeof(sort_keys[sort_key_count]);
                    sort_keys[sort_key_count].abi_version =
                        PRDB_SDK_VALUE_ABI_V1_VERSION;
                    sort_keys[sort_key_count].type_code =
                        PRDB_SDK_VALUE_TYPE_BIGINT;
                    sort_keys[sort_key_count].state =
                        PRDB_SDK_VALUE_STATE_VALUE;
                    sort_keys[sort_key_count].data =
                        count_bytes[sort_key_count];
                    sort_keys[sort_key_count].data_len = 8;
                } else {
                    sort_keys[sort_key_count] = aggregate_results[group];
                }
                ++sort_key_count;
            }
            if (enable_multi_order) {
                prdb_sdk_value_v1 order_values[
                    PRDB_SDK_SQL_V12_MAX_ORDER_KEYS]
                    [PRDB_SDK_SQL_GROUP_MAX_ROWS];
                uint8_t order_count_bytes[
                    PRDB_SDK_SQL_V12_MAX_ORDER_KEYS]
                    [PRDB_SDK_SQL_GROUP_MAX_ROWS][8];
                prdb_sdk_sql_order_key_v3 order_keys[
                    PRDB_SDK_SQL_V12_MAX_ORDER_KEYS] = {{0}};
                for (uint32_t key = 0; key < binding_v12.order_count; ++key) {
                    const prdb_sdk_sql_bound_order_item_v12 *bound =
                        &binding_v12.order_items[key];
                    order_keys[key].values = order_values[key];
                    order_keys[key].declared_type = bound->type_code;
                    order_keys[key].decimal_scale = bound->decimal_scale;
                    order_keys[key].direction =
                        (uint8_t)bound->item.direction;
                    order_keys[key].null_order =
                        (uint8_t)(bound->item.null_order ==
                                          PRDB_SDK_SQL_V11_NULL_ORDER_DEFAULT
                                      ? PRDB_SDK_SQL_ORDER_NULLS_FIRST
                                      : bound->item.null_order);
                    for (uint32_t sort_index = 0;
                         sort_index < sort_key_count; ++sort_index) {
                        uint32_t group = group_for_sort_key[sort_index];
                        if (bound->item.kind ==
                            PRDB_SDK_SQL_V10_ORDER_GROUP_KEY) {
                            order_values[key][sort_index] =
                                keys[groups[group].representative_row];
                        } else if (binding.aggregate_kind ==
                                   PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
                            uint64_t count = groups[group].count;
                            for (uint32_t byte = 0; byte < 8; ++byte) {
                                order_count_bytes[key][sort_index][7 - byte] =
                                    (uint8_t)(count & UINT64_C(0xff));
                                count >>= 8;
                            }
                            order_values[key][sort_index].struct_size =
                                sizeof(order_values[key][sort_index]);
                            order_values[key][sort_index].abi_version =
                                PRDB_SDK_VALUE_ABI_V1_VERSION;
                            order_values[key][sort_index].type_code =
                                PRDB_SDK_VALUE_TYPE_BIGINT;
                            order_values[key][sort_index].state =
                                PRDB_SDK_VALUE_STATE_VALUE;
                            order_values[key][sort_index].data =
                                order_count_bytes[key][sort_index];
                            order_values[key][sort_index].data_len = 8;
                        } else {
                            order_values[key][sort_index] =
                                aggregate_results[group];
                        }
                    }
                }
                status = prdb_sdk_sql_order_row_indices_v3(
                    order_keys, binding_v12.order_count, sort_key_count,
                    sorted_indices, PRDB_SDK_SQL_GROUP_MAX_ROWS);
            } else if (enable_null_placement) {
                uint32_t null_order =
                    binding_v11.null_order ==
                            PRDB_SDK_SQL_V11_NULL_ORDER_DEFAULT
                        ? PRDB_SDK_SQL_ORDER_NULLS_FIRST
                        : binding_v11.null_order;
                status = prdb_sdk_sql_order_row_indices_v2(
                    sort_keys, sort_key_count, binding_v10.order_type_code,
                    binding_v10.order_decimal_scale,
                    statement_v10.order_direction, null_order,
                    sorted_indices, PRDB_SDK_SQL_GROUP_MAX_ROWS);
            } else {
                status = prdb_sdk_sql_order_row_indices_v1(
                    sort_keys, sort_key_count, binding_v10.order_type_code,
                    binding_v10.order_decimal_scale,
                    statement_v10.order_direction, sorted_indices,
                    PRDB_SDK_SQL_GROUP_MAX_ROWS);
            }
            if (status != PRDB_SDK_SQL_ORDER_OK) {
                return fail_statement(host_v3,
                                      PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
            }
            for (uint32_t index = 0; index < sort_key_count; ++index) {
                output_group_indices[output_group_count++] =
                    group_for_sort_key[sorted_indices[index]];
            }
        } else {
            for (uint32_t group = 0; group < group_count; ++group) {
                if (enable_having && binding_v9.has_having &&
                    !having_match[group]) {
                    continue;
                }
                output_group_indices[output_group_count++] = group;
            }
        }
        for (uint32_t output_index = 0; output_index < output_group_count;
             ++output_index) {
            uint32_t group = output_group_indices[output_index];
            const prdb_sdk_value_v1 *aggregate =
                binding.aggregate_kind ==
                        PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR
                    ? NULL
                    : &aggregate_results[group];
            if (statement.base.has_limit &&
                output_index >= statement.base.limit) {
                break;
            }
            if (enable_composite_grouping && on_group_v13 != NULL) {
                status = on_group_v13(
                    result_context,
                    &composite_keys[groups[group].representative_row *
                                    composite_key_count],
                    binding_v13.group_key_count, aggregate,
                    groups[group].count, &binding_v13);
            } else if (enable_multi_order && on_group_v12 != NULL) {
                status = on_group_v12(
                    result_context, &keys[groups[group].representative_row],
                    aggregate, groups[group].count, &binding_v12);
            } else if (enable_null_placement && on_group_v11 != NULL) {
                status = on_group_v11(
                    result_context, &keys[groups[group].representative_row],
                    aggregate, groups[group].count, &binding_v11);
            } else if (enable_ordering && on_group_v10 != NULL) {
                status = on_group_v10(
                    result_context, &keys[groups[group].representative_row],
                    aggregate, groups[group].count, &binding_v10);
            } else if (enable_having && on_group_v9 != NULL) {
                status = on_group_v9(
                    result_context, &keys[groups[group].representative_row],
                    aggregate, groups[group].count, &binding_v9);
            } else {
                status = on_group(
                    result_context, &keys[groups[group].representative_row],
                    aggregate, groups[group].count, &binding);
            }
            if (status != 0) {
                return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_RESULT_ERROR);
            }
        }
    }
    return PRDB_SDK_SQL_EXEC_OK;
}

int32_t prdb_sdk_sql_execute_group_v7(
    const prdb_host_api_v3 *host_v3, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v2 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v2 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *materialized_bytes,
    uint64_t materialized_bytes_capacity, prdb_sdk_sql_group_row_v7_fn on_group,
    void *result_context) {
    return execute_group_internal(
        host_v3, schema, schema_len, sql, sql_len, parameters, parameter_count,
        max_candidates, max_batch_bytes, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns,
        executor_column_capacity, cell_scratch, cell_scratch_capacity,
        materialized_bytes, materialized_bytes_capacity, on_group, NULL, NULL,
        NULL, NULL, NULL, result_context, 0, 0, 0, 0, 0);
}

int32_t prdb_sdk_sql_execute_group_v9(
    const prdb_host_api_v3 *host_v3, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v2 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v2 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *materialized_bytes,
    uint64_t materialized_bytes_capacity, prdb_sdk_sql_group_row_v9_fn on_group,
    void *result_context) {
    return execute_group_internal(
        host_v3, schema, schema_len, sql, sql_len, parameters, parameter_count,
        max_candidates, max_batch_bytes, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns,
        executor_column_capacity, cell_scratch, cell_scratch_capacity,
        materialized_bytes, materialized_bytes_capacity, NULL, on_group, NULL,
        NULL, NULL, NULL, result_context, 1, 0, 0, 0, 0);
}

int32_t prdb_sdk_sql_execute_group_v10(
    const prdb_host_api_v3 *host_v3, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v2 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v2 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *materialized_bytes,
    uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_group_row_v10_fn on_group, void *result_context) {
    return execute_group_internal(
        host_v3, schema, schema_len, sql, sql_len, parameters, parameter_count,
        max_candidates, max_batch_bytes, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns,
        executor_column_capacity, cell_scratch, cell_scratch_capacity,
        materialized_bytes, materialized_bytes_capacity, NULL, NULL, on_group,
        NULL, NULL, NULL, result_context, 1, 1, 0, 0, 0);
}

int32_t prdb_sdk_sql_execute_group_v11(
    const prdb_host_api_v3 *host_v3, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v2 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v2 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *materialized_bytes,
    uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_group_row_v11_fn on_group, void *result_context) {
    return execute_group_internal(
        host_v3, schema, schema_len, sql, sql_len, parameters, parameter_count,
        max_candidates, max_batch_bytes, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns,
        executor_column_capacity, cell_scratch, cell_scratch_capacity,
        materialized_bytes, materialized_bytes_capacity, NULL, NULL, NULL,
        on_group, NULL, NULL, result_context, 1, 1, 1, 0, 0);
}

int32_t prdb_sdk_sql_execute_group_v12(
    const prdb_host_api_v3 *host_v3, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v2 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v2 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *materialized_bytes,
    uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_group_row_v12_fn on_group, void *result_context) {
    return execute_group_internal(
        host_v3, schema, schema_len, sql, sql_len, parameters, parameter_count,
        max_candidates, max_batch_bytes, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns,
        executor_column_capacity, cell_scratch, cell_scratch_capacity,
        materialized_bytes, materialized_bytes_capacity, NULL, NULL, NULL,
        NULL, on_group, NULL, result_context, 1, 1, 1, 1, 0);
}

int32_t prdb_sdk_sql_execute_group_v13(
    const prdb_host_api_v3 *host_v3, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v2 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v2 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *materialized_bytes,
    uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_group_row_v13_fn on_group, void *result_context) {
    return execute_group_internal(
        host_v3, schema, schema_len, sql, sql_len, parameters, parameter_count,
        max_candidates, max_batch_bytes, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns,
        executor_column_capacity, cell_scratch, cell_scratch_capacity,
        materialized_bytes, materialized_bytes_capacity, NULL, NULL, NULL,
        NULL, NULL, on_group, result_context, 0, 0, 0, 0, 1);
}
