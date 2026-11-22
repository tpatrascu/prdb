#include "prdb_sdk_sql_exec_v7.h"
#include "prdb_sdk_sql_exec_v9.h"
#include "prdb_sdk_sql_exec_v10.h"
#include "prdb_sdk_sql_exec_v11.h"
#include "prdb_sdk_sql_exec_v12.h"
#include "prdb_sdk_sql_exec_v13.h"
#include "prdb_sdk_sql_exec_v14.h"
#include "prdb_sdk_sql_exec_v15.h"
#include "prdb_sdk_sql_exec_v17.h"
#include "prdb_sdk_sql_exec_v18.h"
#include "prdb_sdk_sql_exec_v21.h"
#include "prdb_sdk_sql_exec_v22.h"
#include "prdb_sdk_sql_exec_v23.h"
#include "prdb_sdk_sql_exec_v24.h"
#include "prdb_sdk_sql_v22.h"
#include "prdb_sdk_optimizer_plan_v1.h"
#include "prdb_sdk_planner_v1.h"
#include "prdb_sdk_sql_v14.h"
#include "prdb_sdk_sql_v17.h"
#include "prdb_sdk_sql_v18.h"
#include "prdb_sdk_sql_v9.h"
#include "prdb_sdk_sql_v10.h"
#include "prdb_sdk_sql_order_v2.h"
#include "prdb_sdk_sql_order_v3.h"
#include "sql_parameter_internal.h"

#include <string.h>

#define SQL_EXEC_V7_MAX_SCAN_PAGES UINT32_C(124)
#define SQL_EXEC_V7_GROUPS_PER_CANCEL_CHECK UINT32_C(32)

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

static int32_t evaluate_having_node_v18(
    const prdb_sdk_select_binding_v18 *binding, uint32_t index,
    const prdb_sdk_value_v1 *aggregate,
    const prdb_sdk_value_v1 *count_value,
    const prdb_sdk_value_v1 *operands, uint64_t count_mask,
    uint32_t depth, uint32_t *truth_out) {
    const prdb_sdk_sql_having_node_v18 *node;
    uint32_t left_truth, right_truth;
    int32_t status;
    if (depth > PRDB_SDK_SQL_MAX_PREDICATE_NODES ||
        index >= binding->having_node_count)
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    node = &binding->having_nodes[index];
    if (node->node_kind == PRDB_SDK_SQL_PREDICATE_NOT) {
        status = evaluate_having_node_v18(
            binding, node->left_child, aggregate, count_value, operands,
            count_mask, depth + 1,
            &left_truth);
        if (status != PRDB_SDK_SQL_EVAL_OK) return status;
        return prdb_sdk_sql_truth_not_v1(left_truth, truth_out);
    }
    if (node->node_kind == PRDB_SDK_SQL_PREDICATE_AND ||
        node->node_kind == PRDB_SDK_SQL_PREDICATE_OR) {
        status = evaluate_having_node_v18(
            binding, node->left_child, aggregate, count_value, operands,
            count_mask, depth + 1,
            &left_truth);
        if (status != PRDB_SDK_SQL_EVAL_OK) return status;
        if (node->node_kind == PRDB_SDK_SQL_PREDICATE_AND &&
            left_truth == PRDB_SDK_SQL_TRUTH_FALSE) {
            *truth_out = PRDB_SDK_SQL_TRUTH_FALSE;
            return PRDB_SDK_SQL_EVAL_OK;
        }
        if (node->node_kind == PRDB_SDK_SQL_PREDICATE_OR &&
            left_truth == PRDB_SDK_SQL_TRUTH_TRUE) {
            *truth_out = PRDB_SDK_SQL_TRUTH_TRUE;
            return PRDB_SDK_SQL_EVAL_OK;
        }
        status = evaluate_having_node_v18(
            binding, node->right_child, aggregate, count_value, operands,
            count_mask, depth + 1,
            &right_truth);
        if (status != PRDB_SDK_SQL_EVAL_OK) return status;
        return node->node_kind == PRDB_SDK_SQL_PREDICATE_AND
                   ? prdb_sdk_sql_truth_and_v1(left_truth, right_truth,
                                               truth_out)
                   : prdb_sdk_sql_truth_or_v1(left_truth, right_truth,
                                              truth_out);
    }
    if (node->node_kind == PRDB_SDK_SQL_PREDICATE_IS_NULL ||
        node->node_kind == PRDB_SDK_SQL_PREDICATE_IS_NOT_NULL) {
        const prdb_sdk_value_v1 *value =
            (count_mask & (UINT64_C(1) << index)) ? count_value : aggregate;
        status = prdb_sdk_sql_value_is_null_v1(value, truth_out);
        if (status != PRDB_SDK_SQL_EVAL_OK ||
            node->node_kind == PRDB_SDK_SQL_PREDICATE_IS_NULL)
            return status;
        return prdb_sdk_sql_truth_not_v1(*truth_out, truth_out);
    }
    if ((count_mask & (UINT64_C(1) << index)) != 0)
        return prdb_sdk_sql_compare_v1(
            count_value, &operands[index], node->comparison, 0, 0, truth_out);
    return prdb_sdk_sql_compare_v1(aggregate, &operands[index],
                                   node->comparison,
                                   binding->having_decimal_scale,
                                   binding->having_decimal_scale, truth_out);
}

static int valid_group_host(const prdb_host_api_v3 *host) {
    return host != NULL && host->struct_size >= sizeof(*host) &&
           host->abi_version == PRDB_QUERY_ABI_V3_VERSION &&
           host->statement_failure != NULL &&
           host->executor_request != NULL && host->planner_request != NULL;
}

static int32_t execute_group_internal(
    const prdb_host_api_v3 *host_v3,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters,
    const prdb_sdk_sql_parameter_metadata_v1 *parameter_metadata,
    uint32_t parameter_count, uint16_t max_candidates, uint32_t max_batch_bytes,
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
    prdb_sdk_sql_group_row_v13_fn on_group_v13,
    prdb_sdk_sql_group_row_v14_fn on_group_v14,
    prdb_sdk_sql_group_row_v15_fn on_group_v15,
    void *result_context,
    int enable_having, int enable_ordering, int enable_null_placement,
    int enable_multi_order, int enable_composite_grouping,
    int enable_composite_having, int enable_composite_order,
    int enable_literal_having,
    prdb_sdk_sql_group_row_v17_fn on_group_v17,
    int enable_boolean_having,
    prdb_sdk_sql_group_row_v18_fn on_group_v18,
    uint32_t numeric_having_version,
    prdb_sdk_sql_group_row_v21_fn on_group_v21) {
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
    prdb_sdk_select_v14 statement_v14 = {0};
    prdb_sdk_select_binding_v14 binding_v14 = {0};
    prdb_sdk_select_v15 statement_v15 = {0};
    prdb_sdk_select_binding_v15 binding_v15 = {0};
    prdb_sdk_select_v17 statement_v17 = {0};
    prdb_sdk_select_binding_v17 binding_v17 = {0};
    prdb_sdk_select_v18 statement_v18 = {0};
    prdb_sdk_select_binding_v18 binding_v18 = {0};
    prdb_sdk_select_v21 statement_v21 = {0};
    prdb_sdk_select_binding_v21 binding_v21 = {0};
    prdb_sdk_select_v23 statement_v23 = {0};
    prdb_sdk_select_binding_v23 binding_v23 = {0};
    prdb_sdk_select_v22 statement_v22 = {0};
    prdb_sdk_select_binding_v22 binding_v22 = {0};
    uint64_t having_count_mask = 0;
    prdb_sdk_value_v1 having_parameter = {0};
    prdb_sdk_value_v1 having_operands[
        PRDB_SDK_SQL_V18_MAX_HAVING_NODES] = {{0}};
    uint8_t having_operand_bytes[
        PRDB_SDK_SQL_V18_MAX_HAVING_NODES][8] = {{0}};
    uint8_t having_decimal_literal_bytes[
        PRDB_SDK_SQL_V18_MAX_HAVING_NODES][16] = {{0}};
    uint8_t having_literal_bytes[8] = {0};
    uint32_t having_parameter_index = 0, having_type_code = 0;
    uint32_t having_comparison = 0;
    uint8_t having_decimal_precision = 0, having_decimal_scale = 0;
    int has_having = 0, has_having_literal = 0;
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

    if (!valid_group_host(host_v3)) return PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT;
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
         on_group_v13 == NULL && on_group_v14 == NULL &&
         on_group_v15 == NULL && on_group_v17 == NULL &&
         on_group_v18 == NULL && on_group_v21 == NULL) ||
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

    if (numeric_having_version >= PRDB_SDK_SQL_ABI_V22_VERSION) {
        if (numeric_having_version >= PRDB_SDK_SQL_ABI_V23_VERSION) {
            status = numeric_having_version == PRDB_SDK_SQL_ABI_V24_VERSION
                ? prdb_sdk_sql_parse_select_v24(
                    sql, sql_len, &statement_v23, sizeof(statement_v23), NULL)
                : prdb_sdk_sql_parse_select_v23(
                    sql, sql_len, &statement_v23, sizeof(statement_v23), NULL);
            statement_v21 = statement_v23.base;
        } else {
            status = prdb_sdk_sql_parse_select_v22(
                sql, sql_len, &statement_v22, sizeof(statement_v22), NULL);
            statement_v21 = statement_v22.base;
        }
        statement_v18 = statement_v21.base;
        statement_v15 = statement_v18.base;
        statement_v14 = statement_v15.base;
        statement_v13 = statement_v14.base;
        statement = statement_v13.base;
    } else if (numeric_having_version) {
        status = prdb_sdk_sql_parse_select_v21(
            sql, sql_len, &statement_v21, sizeof(statement_v21), NULL);
        statement_v18 = statement_v21.base;
        statement_v15 = statement_v18.base;
        statement_v14 = statement_v15.base;
        statement_v13 = statement_v14.base;
        statement = statement_v13.base;
    } else if (enable_boolean_having) {
        status = prdb_sdk_sql_parse_select_v18(
            sql, sql_len, &statement_v18, sizeof(statement_v18), NULL);
        statement_v15 = statement_v18.base;
        statement_v14 = statement_v15.base;
        statement_v13 = statement_v14.base;
        statement = statement_v13.base;
    } else if (enable_literal_having) {
        status = prdb_sdk_sql_parse_select_v17(
            sql, sql_len, &statement_v17, sizeof(statement_v17), NULL);
        statement_v15 = statement_v17.base;
        statement_v14 = statement_v15.base;
        statement_v13 = statement_v14.base;
        statement = statement_v13.base;
    } else if (enable_composite_order) {
        status = prdb_sdk_sql_parse_select_v15(
            sql, sql_len, &statement_v15, sizeof(statement_v15), NULL);
        statement_v14 = statement_v15.base;
        statement_v13 = statement_v14.base;
        statement = statement_v13.base;
    } else if (enable_composite_having) {
        status = prdb_sdk_sql_parse_select_v14(
            sql, sql_len, &statement_v14, sizeof(statement_v14), NULL);
        statement_v13 = statement_v14.base;
        statement = statement_v13.base;
    } else if (enable_composite_grouping) {
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
    if (numeric_having_version >= PRDB_SDK_SQL_ABI_V22_VERSION) {
        if (numeric_having_version >= PRDB_SDK_SQL_ABI_V23_VERSION) {
            status = numeric_having_version == PRDB_SDK_SQL_ABI_V24_VERSION
                ? prdb_sdk_sql_bind_select_v24(
                    &statement_v23, resolved.name, resolved.name_len,
                    executor_columns, resolved.column_count, &binding_v23,
                    sizeof(binding_v23))
                : prdb_sdk_sql_bind_select_v23(
                    &statement_v23, resolved.name, resolved.name_len,
                    executor_columns, resolved.column_count, &binding_v23,
                    sizeof(binding_v23));
            binding_v21 = binding_v23.base;
            having_count_mask = binding_v23.having_count_mask;
        } else {
            status = prdb_sdk_sql_bind_select_v22(
                &statement_v22, resolved.name, resolved.name_len,
                executor_columns, resolved.column_count, &binding_v22,
                sizeof(binding_v22));
            binding_v21 = binding_v22.base;
            having_count_mask = binding_v22.having_count_mask;
        }
        binding_v18 = binding_v21.base;
        binding_v15 = binding_v18.base;
        binding_v14 = binding_v15.base;
        binding_v13 = binding_v14.base;
        binding = binding_v13.base;
        composite_key_count = binding_v13.group_key_count;
        has_having = binding_v18.having_node_count != 0;
        having_type_code = binding_v18.having_type_code;
        having_decimal_precision = binding_v18.having_decimal_precision;
        having_decimal_scale = binding_v18.having_decimal_scale;
        for (uint32_t index = 0; index < binding_v13.group_key_count; ++index) {
            composite_types[index] = binding_v13.group_keys[index].type_code;
            composite_scales[index] = binding_v13.group_keys[index].decimal_scale;
        }
    } else if (numeric_having_version) {
        status = prdb_sdk_sql_bind_select_v21(
            &statement_v21, resolved.name, resolved.name_len, executor_columns,
            resolved.column_count, &binding_v21, sizeof(binding_v21));
        binding_v18 = binding_v21.base;
        binding_v15 = binding_v18.base;
        binding_v14 = binding_v15.base;
        binding_v13 = binding_v14.base;
        binding = binding_v13.base;
        composite_key_count = binding_v13.group_key_count;
        has_having = binding_v18.having_node_count != 0;
        having_type_code = binding_v18.having_type_code;
        having_decimal_precision = binding_v18.having_decimal_precision;
        having_decimal_scale = binding_v18.having_decimal_scale;
        for (uint32_t index = 0; index < binding_v13.group_key_count; ++index) {
            composite_types[index] = binding_v13.group_keys[index].type_code;
            composite_scales[index] = binding_v13.group_keys[index].decimal_scale;
        }
    } else if (enable_boolean_having) {
        status = prdb_sdk_sql_bind_select_v18(
            &statement_v18, resolved.name, resolved.name_len,
            executor_columns, resolved.column_count, &binding_v18,
            sizeof(binding_v18));
        binding_v15 = binding_v18.base;
        binding_v14 = binding_v15.base;
        binding_v13 = binding_v14.base;
        binding = binding_v13.base;
        composite_key_count = binding_v13.group_key_count;
        has_having = binding_v18.having_node_count != 0;
        having_type_code = binding_v18.having_type_code;
        having_decimal_precision = binding_v18.having_decimal_precision;
        having_decimal_scale = binding_v18.having_decimal_scale;
        for (uint32_t index = 0; index < binding_v13.group_key_count; ++index) {
            composite_types[index] = binding_v13.group_keys[index].type_code;
            composite_scales[index] =
                binding_v13.group_keys[index].decimal_scale;
        }
    } else if (enable_literal_having) {
        status = prdb_sdk_sql_bind_select_v17(
            &statement_v17, resolved.name, resolved.name_len,
            executor_columns, resolved.column_count, &binding_v17,
            sizeof(binding_v17));
        binding_v15 = binding_v17.base;
        binding_v14 = binding_v15.base;
        binding_v13 = binding_v14.base;
        binding = binding_v13.base;
        composite_key_count = binding_v13.group_key_count;
        has_having = binding_v14.has_having;
        having_parameter_index = binding_v14.having_parameter_index;
        having_comparison = binding_v14.having_comparison;
        having_type_code = binding_v14.having_type_code;
        having_decimal_precision = binding_v14.having_decimal_precision;
        having_decimal_scale = binding_v14.having_decimal_scale;
        has_having_literal = binding_v17.has_having_literal;
        for (uint32_t index = 0; index < binding_v13.group_key_count; ++index) {
            composite_types[index] = binding_v13.group_keys[index].type_code;
            composite_scales[index] =
                binding_v13.group_keys[index].decimal_scale;
        }
    } else if (enable_composite_order) {
        status = prdb_sdk_sql_bind_select_v15(
            &statement_v15, resolved.name, resolved.name_len, executor_columns,
            resolved.column_count, &binding_v15, sizeof(binding_v15));
        binding_v14 = binding_v15.base;
        binding_v13 = binding_v14.base;
        binding = binding_v13.base;
        composite_key_count = binding_v13.group_key_count;
        has_having = binding_v14.has_having;
        having_parameter_index = binding_v14.having_parameter_index;
        having_comparison = binding_v14.having_comparison;
        having_type_code = binding_v14.having_type_code;
        having_decimal_precision = binding_v14.having_decimal_precision;
        having_decimal_scale = binding_v14.having_decimal_scale;
        for (uint32_t index = 0; index < binding_v13.group_key_count; ++index) {
            composite_types[index] = binding_v13.group_keys[index].type_code;
            composite_scales[index] =
                binding_v13.group_keys[index].decimal_scale;
        }
    } else if (enable_composite_having) {
        status = prdb_sdk_sql_bind_select_v14(
            &statement_v14, resolved.name, resolved.name_len, executor_columns,
            resolved.column_count, &binding_v14, sizeof(binding_v14));
        binding_v13 = binding_v14.base;
        binding = binding_v13.base;
        composite_key_count = binding_v13.group_key_count;
        has_having = binding_v14.has_having;
        having_parameter_index = binding_v14.having_parameter_index;
        having_comparison = binding_v14.having_comparison;
        having_type_code = binding_v14.having_type_code;
        having_decimal_precision = binding_v14.having_decimal_precision;
        having_decimal_scale = binding_v14.having_decimal_scale;
        for (uint32_t index = 0; index < binding_v13.group_key_count; ++index) {
            composite_types[index] = binding_v13.group_keys[index].type_code;
            composite_scales[index] =
                binding_v13.group_keys[index].decimal_scale;
        }
    } else if (enable_composite_grouping) {
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
    if (enable_multi_order) {
        binding_v9 = binding_v12.base.base.base;
    } else if (enable_null_placement) {
        binding_v9 = binding_v11.base.base;
    } else if (enable_ordering) {
        binding_v9 = binding_v10.base;
    }
    if (enable_having) {
        has_having = binding_v9.has_having;
        having_parameter_index = binding_v9.having_parameter_index;
        having_comparison = binding_v9.having_comparison;
        having_type_code = binding_v9.having_type_code;
        having_decimal_precision = binding_v9.having_decimal_precision;
        having_decimal_scale = binding_v9.having_decimal_scale;
    }
    if (status != PRDB_SDK_SQL_BIND_OK) {
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    }
    if (numeric_having_version && has_having) {
        for (uint32_t index = 0; index < binding_v21.base.having_node_count;
             ++index) {
            const prdb_sdk_sql_having_node_v21 *node =
                &binding_v21.having_nodes[index];
            if (node->base.node_kind != PRDB_SDK_SQL_PREDICATE_COMPARE) continue;
            if (node->base.operand_kind ==
                PRDB_SDK_SQL_V18_OPERAND_PARAMETER) {
                status = bind_sql_parameter(
                    node->base.parameter_index, parameters, parameter_metadata,
                    parameter_count,
                    (having_count_mask & (UINT64_C(1) << index))
                        ? PRDB_SDK_VALUE_TYPE_BIGINT
                        : binding_v21.base.having_type_code,
                    (having_count_mask & (UINT64_C(1) << index))
                        ? 0
                        : binding_v21.base.having_decimal_precision,
                    (having_count_mask & (UINT64_C(1) << index))
                        ? 0
                        : binding_v21.base.having_decimal_scale,
                    &having_operands[index]);
                if (status != PRDB_SDK_SQL_EVAL_OK)
                    return fail_statement(host_v3,
                                          PRDB_SDK_SQL_EXEC_BIND_ERROR);
            } else if (node->base.operand_kind ==
                       PRDB_SDK_SQL_V21_OPERAND_NUMERIC_LITERAL) {
                uint32_t operand_type =
                    (having_count_mask & (UINT64_C(1) << index))
                        ? PRDB_SDK_VALUE_TYPE_BIGINT
                        : binding_v21.base.having_type_code;
                uint32_t data_len = operand_type ==
                                            PRDB_SDK_VALUE_TYPE_DECIMAL
                                        ? 16
                                        : 8;
                const uint8_t *coefficient = node->coefficient;
                memcpy(having_decimal_literal_bytes[index],
                       coefficient + (16 - data_len), data_len);
                having_operands[index].struct_size =
                    sizeof(having_operands[index]);
                having_operands[index].abi_version =
                    PRDB_SDK_VALUE_ABI_V1_VERSION;
                having_operands[index].state = PRDB_SDK_VALUE_STATE_VALUE;
                having_operands[index].type_code = operand_type;
                having_operands[index].data =
                    having_decimal_literal_bytes[index];
                having_operands[index].data_len = data_len;
            } else {
                return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
            }
        }
    } else if (enable_boolean_having && has_having) {
        for (uint32_t index = 0; index < binding_v18.having_node_count;
             ++index) {
            const prdb_sdk_sql_having_node_v18 *node =
                &binding_v18.having_nodes[index];
            if (node->node_kind != PRDB_SDK_SQL_PREDICATE_COMPARE) continue;
            if (node->operand_kind == PRDB_SDK_SQL_V18_OPERAND_PARAMETER) {
                status = bind_sql_parameter(
                    node->parameter_index, parameters, parameter_metadata,
                    parameter_count,
                    binding_v18.having_type_code,
                    binding_v18.having_decimal_precision,
                    binding_v18.having_decimal_scale, &having_operands[index]);
                if (status != PRDB_SDK_SQL_EVAL_OK)
                    return fail_statement(host_v3,
                                          PRDB_SDK_SQL_EXEC_BIND_ERROR);
            } else {
                uint64_t literal = node->literal;
                for (uint32_t byte = 0; byte < sizeof(having_operand_bytes[index]);
                     ++byte) {
                    having_operand_bytes[index]
                                        [sizeof(having_operand_bytes[index]) -
                                         1 - byte] =
                        (uint8_t)(literal & UINT64_C(0xff));
                    literal >>= 8;
                }
                having_operands[index].struct_size =
                    sizeof(having_operands[index]);
                having_operands[index].abi_version =
                    PRDB_SDK_VALUE_ABI_V1_VERSION;
                having_operands[index].state = PRDB_SDK_VALUE_STATE_VALUE;
                having_operands[index].type_code = PRDB_SDK_VALUE_TYPE_BIGINT;
                having_operands[index].data = having_operand_bytes[index];
                having_operands[index].data_len =
                    sizeof(having_operand_bytes[index]);
            }
        }
    } else if ((enable_having || enable_composite_having ||
                enable_composite_order || enable_literal_having) &&
               has_having) {
        if (has_having_literal) {
            uint64_t literal = binding_v17.having_literal;
            for (uint32_t index = 0; index < sizeof(having_literal_bytes);
                 ++index) {
                having_literal_bytes[sizeof(having_literal_bytes) - 1 - index] =
                    (uint8_t)(literal & UINT64_C(0xff));
                literal >>= 8;
            }
            having_parameter.struct_size = sizeof(having_parameter);
            having_parameter.abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
            having_parameter.state = PRDB_SDK_VALUE_STATE_VALUE;
            having_parameter.type_code = PRDB_SDK_VALUE_TYPE_BIGINT;
            having_parameter.data = having_literal_bytes;
            having_parameter.data_len = sizeof(having_literal_bytes);
        } else {
            status = bind_sql_parameter(
                having_parameter_index, parameters, parameter_metadata,
                parameter_count,
                having_type_code, having_decimal_precision, having_decimal_scale,
                &having_parameter);
            if (status != PRDB_SDK_SQL_EVAL_OK) {
                return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
            }
        }
    }
    for (uint32_t index = 0; index < statement.base.predicate_count; ++index) {
        const prdb_sdk_predicate_node_v2 *node =
            &statement.base.predicates[index];
        if (node->node_kind == PRDB_SDK_SQL_PREDICATE_COMPARE) {
            const prdb_sdk_sql_bound_column_v2 *column =
                &binding.base.predicate_columns[index];
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
        if (page_count >= SQL_EXEC_V7_MAX_SCAN_PAGES) {
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
    if (final_status == PRDB_SDK_SQL_EXEC_OK) {
        int32_t cancelled = 0;
        status = prdb_sdk_executor_check_cancelled_v1(host, &cancelled);
        if (status != PRDB_SDK_EXECUTOR_OK || cancelled)
            final_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
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
    if ((enable_having || enable_composite_having || enable_composite_order ||
         enable_literal_having || enable_boolean_having) && has_having) {
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
                    PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR ||
                having_count_mask != 0) {
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
            if (enable_boolean_having) {
                status = evaluate_having_node_v18(
                    &binding_v18, binding_v18.having_root,
                    aggregate == NULL ? &count_value : aggregate,
                    &count_value, having_operands, having_count_mask, 0,
                    &truth);
            } else {
                status = prdb_sdk_sql_compare_v1(
                    comparison_value, &having_parameter,
                    having_comparison, having_decimal_scale,
                    having_decimal_scale, &truth);
            }
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
        if (enable_composite_order && statement_v15.order_count != 0) {
            prdb_sdk_value_v1 order_values[
                PRDB_SDK_SQL_V15_MAX_ORDER_KEYS]
                [PRDB_SDK_SQL_GROUP_MAX_ROWS];
            uint8_t count_bytes[PRDB_SDK_SQL_V15_MAX_ORDER_KEYS]
                               [PRDB_SDK_SQL_GROUP_MAX_ROWS][8];
            uint32_t group_for_sort_key[PRDB_SDK_SQL_GROUP_MAX_ROWS];
            uint32_t sorted_indices[PRDB_SDK_SQL_GROUP_MAX_ROWS];
            prdb_sdk_sql_order_key_v3 order_keys[
                PRDB_SDK_SQL_V15_MAX_ORDER_KEYS] = {{0}};
            uint32_t sort_key_count = 0;
            for (uint32_t group = 0; group < group_count; ++group) {
                if (has_having && !having_match[group]) continue;
                group_for_sort_key[sort_key_count++] = group;
            }
            for (uint32_t key = 0; key < binding_v15.order_count; ++key) {
                const prdb_sdk_sql_bound_order_item_v15 *bound =
                    &binding_v15.order_items[key];
                order_keys[key].values = order_values[key];
                order_keys[key].declared_type = bound->type_code;
                order_keys[key].decimal_scale = bound->decimal_scale;
                order_keys[key].direction = (uint8_t)bound->item.direction;
                order_keys[key].null_order =
                    (uint8_t)(bound->item.null_order ==
                                      PRDB_SDK_SQL_V11_NULL_ORDER_DEFAULT
                                  ? PRDB_SDK_SQL_ORDER_NULLS_FIRST
                                  : bound->item.null_order);
                for (uint32_t sort_index = 0; sort_index < sort_key_count;
                     ++sort_index) {
                    uint32_t group = group_for_sort_key[sort_index];
                    if (bound->item.kind ==
                        PRDB_SDK_SQL_V10_ORDER_GROUP_KEY) {
                        order_values[key][sort_index] = composite_keys[
                            groups[group].representative_row *
                                composite_key_count +
                            bound->item.group_key_index];
                    } else if (binding.aggregate_kind ==
                               PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
                        uint64_t count = groups[group].count;
                        for (uint32_t byte = 0; byte < 8; ++byte) {
                            count_bytes[key][sort_index][7 - byte] =
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
                            count_bytes[key][sort_index];
                        order_values[key][sort_index].data_len = 8;
                    } else {
                        order_values[key][sort_index] =
                            aggregate_results[group];
                    }
                }
            }
            status = prdb_sdk_sql_order_row_indices_v3(
                order_keys, binding_v15.order_count, sort_key_count,
                sorted_indices, PRDB_SDK_SQL_GROUP_MAX_ROWS);
            if (status != PRDB_SDK_SQL_ORDER_OK) {
                return fail_statement(host_v3,
                                      PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
            }
            for (uint32_t index = 0; index < sort_key_count; ++index) {
                output_group_indices[output_group_count++] =
                    group_for_sort_key[sorted_indices[index]];
            }
        } else if (enable_ordering && statement_v10.has_order_by) {
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
                if ((enable_having || enable_composite_having ||
                     enable_composite_order || enable_literal_having ||
                     enable_boolean_having) && has_having &&
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
            if (output_index % SQL_EXEC_V7_GROUPS_PER_CANCEL_CHECK == 0) {
                int32_t cancelled = 0;
                status = prdb_sdk_executor_check_cancelled_v1(host,
                                                              &cancelled);
                if (status != PRDB_SDK_EXECUTOR_OK || cancelled) {
                    return fail_statement(host_v3,
                                          PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
                }
            }
            if (numeric_having_version && on_group_v21 != NULL) {
                status = on_group_v21(
                    result_context,
                    &composite_keys[groups[group].representative_row *
                                    composite_key_count],
                    binding_v13.group_key_count, aggregate,
                    groups[group].count, &binding_v21);
            } else if (enable_boolean_having && on_group_v18 != NULL) {
                status = on_group_v18(
                    result_context,
                    &composite_keys[groups[group].representative_row *
                                    composite_key_count],
                    binding_v13.group_key_count, aggregate,
                    groups[group].count, &binding_v18);
            } else if (enable_literal_having && on_group_v17 != NULL) {
                status = on_group_v17(
                    result_context,
                    &composite_keys[groups[group].representative_row *
                                    composite_key_count],
                    binding_v13.group_key_count, aggregate,
                    groups[group].count, &binding_v17);
            } else if (enable_composite_order && on_group_v15 != NULL) {
                status = on_group_v15(
                    result_context,
                    &composite_keys[groups[group].representative_row *
                                    composite_key_count],
                    binding_v13.group_key_count, aggregate,
                    groups[group].count, &binding_v15);
            } else if (enable_composite_having && on_group_v14 != NULL) {
                status = on_group_v14(
                    result_context,
                    &composite_keys[groups[group].representative_row *
                                    composite_key_count],
                    binding_v13.group_key_count, aggregate,
                    groups[group].count, &binding_v14);
            } else if (enable_composite_grouping && on_group_v13 != NULL) {
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
        host_v3, schema, schema_len, sql, sql_len, parameters, NULL, parameter_count,
        max_candidates, max_batch_bytes, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns,
        executor_column_capacity, cell_scratch, cell_scratch_capacity,
        materialized_bytes, materialized_bytes_capacity, on_group, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, result_context, 0, 0, 0, 0, 0, 0, 0,
        0, NULL, 0, NULL, 0, NULL);
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
        host_v3, schema, schema_len, sql, sql_len, parameters, NULL, parameter_count,
        max_candidates, max_batch_bytes, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns,
        executor_column_capacity, cell_scratch, cell_scratch_capacity,
        materialized_bytes, materialized_bytes_capacity, NULL, on_group, NULL,
        NULL, NULL, NULL, NULL, NULL, result_context, 1, 0, 0, 0, 0, 0, 0,
        0, NULL, 0, NULL, 0, NULL);
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
        host_v3, schema, schema_len, sql, sql_len, parameters, NULL, parameter_count,
        max_candidates, max_batch_bytes, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns,
        executor_column_capacity, cell_scratch, cell_scratch_capacity,
        materialized_bytes, materialized_bytes_capacity, NULL, NULL, on_group,
        NULL, NULL, NULL, NULL, NULL, result_context, 1, 1, 0, 0, 0, 0, 0,
        0, NULL, 0, NULL, 0, NULL);
}

int32_t prdb_sdk_sql_execute_group_typed_v10(
    const prdb_host_api_v3 *host_v3, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters,
    const prdb_sdk_sql_parameter_metadata_v1 *parameter_metadata,
    uint32_t parameter_count,
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
    if (!valid_group_host(host_v3)) return PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT;
    if (parameter_count != 0 && parameter_metadata == NULL)
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT);
    return execute_group_internal(
        host_v3, schema, schema_len, sql, sql_len, parameters,
        parameter_metadata, parameter_count,
        max_candidates, max_batch_bytes, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns,
        executor_column_capacity, cell_scratch, cell_scratch_capacity,
        materialized_bytes, materialized_bytes_capacity, NULL, NULL, on_group,
        NULL, NULL, NULL, NULL, NULL, result_context, 1, 1, 0, 0, 0, 0, 0,
        0, NULL, 0, NULL, 0, NULL);
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
        host_v3, schema, schema_len, sql, sql_len, parameters, NULL, parameter_count,
        max_candidates, max_batch_bytes, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns,
        executor_column_capacity, cell_scratch, cell_scratch_capacity,
        materialized_bytes, materialized_bytes_capacity, NULL, NULL, NULL,
        on_group, NULL, NULL, NULL, NULL, result_context, 1, 1, 1, 0, 0, 0,
        0, 0, NULL, 0, NULL, 0, NULL);
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
        host_v3, schema, schema_len, sql, sql_len, parameters, NULL, parameter_count,
        max_candidates, max_batch_bytes, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns,
        executor_column_capacity, cell_scratch, cell_scratch_capacity,
        materialized_bytes, materialized_bytes_capacity, NULL, NULL, NULL,
        NULL, on_group, NULL, NULL, NULL, result_context, 1, 1, 1, 1, 0, 0,
        0, 0, NULL, 0, NULL, 0, NULL);
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
        host_v3, schema, schema_len, sql, sql_len, parameters, NULL, parameter_count,
        max_candidates, max_batch_bytes, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns,
        executor_column_capacity, cell_scratch, cell_scratch_capacity,
        materialized_bytes, materialized_bytes_capacity, NULL, NULL, NULL,
        NULL, NULL, on_group, NULL, NULL, result_context, 0, 0, 0, 0, 1, 0,
        0, 0, NULL, 0, NULL, 0, NULL);
}

int32_t prdb_sdk_sql_execute_composite_count_optimizer_v1(
    const prdb_host_api_v5 *host,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *sql, uint64_t sql_len,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *optimizer_request, uint64_t optimizer_request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *planner_response, uint64_t planner_response_capacity,
    uint8_t *executor_response, uint64_t executor_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v2 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v2 *executor_columns,
    uint32_t executor_column_capacity,
    prdb_sdk_value_v1 *cell_scratch, uint32_t cell_scratch_capacity,
    uint8_t *materialized_bytes, uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_group_row_v13_fn on_group, void *result_context) {
    prdb_sdk_select_v13 statement = {0};
    prdb_sdk_select_binding_v13 binding = {0};
    prdb_sdk_planner_relation_v1 planned = {0};
    prdb_sdk_optimizer_response_v1 optimizer = {0};
    prdb_host_api_v2 host_v2;
    uint64_t planner_response_len = 0;
    uint64_t optimizer_request_len = 0;
    uint64_t optimizer_response_len = 0;
    uint64_t executor_response_len = 0;
    int32_t status;

    if (host == NULL || host->v4.v3.struct_size < sizeof(host->v4.v3) ||
        host->v4.v3.abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->v4.v3.statement_failure == NULL ||
        host->v4.v3.planner_request == NULL ||
        host->v4.v3.executor_request == NULL ||
        host->optimizer_request == NULL || schema == NULL || sql == NULL ||
        optimizer_request == NULL || optimizer_response == NULL ||
        planner_response == NULL || executor_response == NULL ||
        planner_columns == NULL || optimizer_request_capacity == 0 ||
        optimizer_response_capacity == 0 || planner_response_capacity == 0 ||
        executor_response_capacity == 0) {
        if (host != NULL && host->v4.v3.statement_failure != NULL)
            (void)host->v4.v3.statement_failure(
                host->v4.v3.context, PRDB_QUERY_STATEMENT_FAILURE_SQL);
        return PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT;
    }
    host_v2 = (prdb_host_api_v2){
        sizeof(prdb_host_api_v2), PRDB_QUERY_ABI_V2_VERSION,
        host->v4.v3.context, host->v4.v3.executor_request,
        host->v4.v3.planner_request, host->v4.v3.write_result};
    status = prdb_sdk_sql_parse_select_v13(sql, sql_len, &statement,
                                           sizeof(statement), NULL);
    if (status != PRDB_SDK_SQL_OK)
        return fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    status = prdb_sdk_planner_describe_relation_v1(
        &host_v2, schema, schema_len,
        statement.base.base.sql + statement.base.base.relation.offset,
        statement.base.base.relation.length, planner_response,
        planner_response_capacity, &planner_response_len, &planned,
        sizeof(planned), planner_columns, planner_column_capacity);
    if (status != PRDB_SDK_PLANNER_REQUEST_OK)
        return fail_statement(&host->v4.v3,
                              PRDB_SDK_SQL_EXEC_PLANNER_ERROR);
    status = prdb_sdk_sql_bind_select_v13(
        &statement, planned.name, planned.name_len, planner_columns,
        planned.column_count, &binding, sizeof(binding));
    if (status != PRDB_SDK_SQL_BIND_OK)
        return fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    status = prdb_sdk_optimizer_encode_composite_group_count_v1(
        &statement, schema, schema_len, optimizer_request,
        optimizer_request_capacity, &optimizer_request_len);
    if (status != PRDB_SDK_OPTIMIZER_ENCODE_OK)
        return fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    status = prdb_sdk_optimizer_submit_group_count_v1(
        host, optimizer_request, optimizer_request_len, optimizer_response,
        optimizer_response_capacity, &optimizer_response_len, &optimizer,
        sizeof(optimizer));
    if (status != PRDB_SDK_OPTIMIZER_RESULT_OK)
        return fail_statement(&host->v4.v3,
                              PRDB_SDK_SQL_EXEC_PLANNER_ERROR);
    status = prdb_sdk_optimizer_match_composite_group_count_plan_v1(
        &statement, &binding, schema, schema_len, optimizer.plan,
        optimizer.plan_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK)
        return fail_statement(&host->v4.v3,
                              PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH);
    status =
        prdb_sdk_optimizer_validate_executor_composite_group_count_relation_v1(
            &host_v2, optimizer.plan, optimizer.plan_len, executor_response,
            executor_response_capacity, &executor_response_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK)
        return fail_statement(&host->v4.v3,
                              PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    return prdb_sdk_sql_execute_group_v13(
        &host->v4.v3, schema, schema_len, sql, sql_len, NULL, 0,
        max_candidates, max_batch_bytes, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response,
        batch_response_capacity, planner_columns, planner_column_capacity,
        executor_columns, executor_column_capacity, cell_scratch,
        cell_scratch_capacity, materialized_bytes,
        materialized_bytes_capacity, on_group, result_context);
}

static int32_t execute_group_composite_aggregate_optimizer_internal(
    const prdb_host_api_v5 *host, const uint8_t *schema, uint32_t schema_len,
    const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint32_t filtered, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *optimizer_request,
    uint64_t optimizer_request_capacity, uint8_t *optimizer_response,
    uint64_t optimizer_response_capacity, uint8_t *planner_response,
    uint64_t planner_response_capacity, uint8_t *executor_response,
    uint64_t executor_response_capacity, uint8_t *batch_response,
    uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v2 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v2 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *materialized_bytes,
    uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_group_row_v13_fn on_group, void *result_context) {
    prdb_sdk_select_v13 statement = {0};
    prdb_sdk_select_binding_v13 binding = {0};
    prdb_sdk_planner_relation_v1 planned = {0};
    prdb_sdk_optimizer_response_v1 optimizer = {0};
    prdb_host_api_v2 host_v2;
    uint64_t planner_response_len = 0, request_len = 0, response_len = 0;
    uint64_t executor_response_len = 0;
    int32_t status;
    if (host == NULL || host->v4.v3.struct_size < sizeof(host->v4.v3) ||
        host->v4.v3.abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->v4.v3.statement_failure == NULL ||
        host->v4.v3.planner_request == NULL ||
        host->v4.v3.executor_request == NULL || host->optimizer_request == NULL ||
        schema == NULL || sql == NULL || optimizer_request == NULL ||
        optimizer_response == NULL || planner_response == NULL ||
        executor_response == NULL || planner_columns == NULL ||
        optimizer_request_capacity == 0 || optimizer_response_capacity == 0 ||
        planner_response_capacity == 0 || executor_response_capacity == 0)
        return host != NULL && host->v4.v3.statement_failure != NULL
                   ? fail_statement(&host->v4.v3,
                                   PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT)
                   : PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT;
    host_v2 = (prdb_host_api_v2){sizeof(prdb_host_api_v2),
                                 PRDB_QUERY_ABI_V2_VERSION,
                                 host->v4.v3.context,
                                 host->v4.v3.executor_request,
                                 host->v4.v3.planner_request,
                                 host->v4.v3.write_result};
    status = prdb_sdk_sql_parse_select_v13(sql, sql_len, &statement,
                                           sizeof(statement), NULL);
    if (status != PRDB_SDK_SQL_OK)
        return fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    status = prdb_sdk_planner_describe_relation_v1(
        &host_v2, schema, schema_len,
        statement.base.base.sql + statement.base.base.relation.offset,
        statement.base.base.relation.length, planner_response,
        planner_response_capacity, &planner_response_len, &planned,
        sizeof(planned), planner_columns, planner_column_capacity);
    if (status != PRDB_SDK_PLANNER_REQUEST_OK)
        return fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_PLANNER_ERROR);
    status = prdb_sdk_sql_bind_select_v13(
        &statement, planned.name, planned.name_len, planner_columns,
        planned.column_count, &binding, sizeof(binding));
    if (status != PRDB_SDK_SQL_BIND_OK)
        return fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    if (filtered) {
        status = prdb_sdk_optimizer_encode_filtered_composite_group_aggregate_v1(
            &statement, parameters, parameter_count, schema, schema_len,
            optimizer_request, optimizer_request_capacity, &request_len);
    } else {
        status = prdb_sdk_optimizer_encode_composite_group_aggregate_v1(
            &statement, schema, schema_len, optimizer_request,
            optimizer_request_capacity, &request_len);
    }
    if (status != PRDB_SDK_OPTIMIZER_ENCODE_OK)
        return fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    status = prdb_sdk_optimizer_submit_group_count_v1(
        host, optimizer_request, request_len, optimizer_response,
        optimizer_response_capacity, &response_len, &optimizer,
        sizeof(optimizer));
    if (status != PRDB_SDK_OPTIMIZER_RESULT_OK)
        return fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_PLANNER_ERROR);
    if (filtered) {
        status = prdb_sdk_optimizer_match_filtered_composite_group_aggregate_plan_v1(
            &statement, &binding, parameters, parameter_count, schema,
            schema_len, optimizer.plan, optimizer.plan_len);
    } else {
        status = prdb_sdk_optimizer_match_composite_group_aggregate_plan_v1(
            &statement, &binding, schema, schema_len, optimizer.plan,
            optimizer.plan_len);
    }
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK)
        return fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH);
    status = prdb_sdk_optimizer_validate_executor_relation_v1(
        &host_v2, optimizer.plan, optimizer.plan_len, executor_response,
        executor_response_capacity, &executor_response_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK)
        return fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    return prdb_sdk_sql_execute_group_v13(
        &host->v4.v3, schema, schema_len, sql, sql_len, parameters,
        parameter_count,
        max_candidates, max_batch_bytes, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns,
        executor_column_capacity, cell_scratch, cell_scratch_capacity,
        materialized_bytes, materialized_bytes_capacity, on_group,
        result_context);
}

int32_t prdb_sdk_sql_execute_group_composite_aggregate_optimizer_v1(
    const prdb_host_api_v5 *host, const uint8_t *schema, uint32_t schema_len,
    const uint8_t *sql, uint64_t sql_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *optimizer_request,
    uint64_t optimizer_request_capacity, uint8_t *optimizer_response,
    uint64_t optimizer_response_capacity, uint8_t *planner_response,
    uint64_t planner_response_capacity, uint8_t *executor_response,
    uint64_t executor_response_capacity, uint8_t *batch_response,
    uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v2 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v2 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *materialized_bytes,
    uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_group_row_v13_fn on_group, void *result_context) {
    return execute_group_composite_aggregate_optimizer_internal(
        host, schema, schema_len, sql, sql_len, NULL, 0, 0, max_candidates,
        max_batch_bytes, optimizer_request, optimizer_request_capacity,
        optimizer_response, optimizer_response_capacity, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns,
        executor_column_capacity, cell_scratch, cell_scratch_capacity,
        materialized_bytes, materialized_bytes_capacity, on_group,
        result_context);
}

int32_t prdb_sdk_sql_execute_filtered_group_composite_aggregate_optimizer_v1(
    const prdb_host_api_v5 *host, const uint8_t *schema, uint32_t schema_len,
    const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint16_t max_candidates, uint32_t max_batch_bytes,
    uint8_t *optimizer_request, uint64_t optimizer_request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
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
    return execute_group_composite_aggregate_optimizer_internal(
        host, schema, schema_len, sql, sql_len, parameters, parameter_count, 1,
        max_candidates, max_batch_bytes, optimizer_request,
        optimizer_request_capacity, optimizer_response,
        optimizer_response_capacity, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns,
        executor_column_capacity, cell_scratch, cell_scratch_capacity,
        materialized_bytes, materialized_bytes_capacity, on_group,
        result_context);
}

int32_t prdb_sdk_sql_execute_ordered_composite_count_optimizer_v1(
    const prdb_host_api_v5 *host, const uint8_t *schema, uint32_t schema_len,
    const uint8_t *sql, uint64_t sql_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *optimizer_request,
    uint64_t optimizer_request_capacity, uint8_t *optimizer_response,
    uint64_t optimizer_response_capacity, uint8_t *planner_response,
    uint64_t planner_response_capacity, uint8_t *executor_response,
    uint64_t executor_response_capacity, uint8_t *batch_response,
    uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v2 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v2 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *materialized_bytes,
    uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_group_row_v15_fn on_group, void *result_context) {
    prdb_sdk_select_v15 statement = {0};
    prdb_sdk_select_binding_v15 binding = {0};
    prdb_sdk_planner_relation_v1 planned = {0};
    prdb_sdk_optimizer_response_v1 optimizer = {0};
    prdb_host_api_v2 host_v2;
    uint64_t planner_response_len = 0, request_len = 0, response_len = 0, executor_response_len = 0;
    int32_t status;
    if (host == NULL || host->v4.v3.struct_size < sizeof(host->v4.v3) || host->v4.v3.abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->v4.v3.statement_failure == NULL || host->v4.v3.planner_request == NULL || host->v4.v3.executor_request == NULL || host->optimizer_request == NULL ||
        schema == NULL || sql == NULL || optimizer_request == NULL || optimizer_response == NULL || planner_response == NULL || executor_response == NULL || planner_columns == NULL ||
        optimizer_request_capacity == 0 || optimizer_response_capacity == 0 || planner_response_capacity == 0 || executor_response_capacity == 0)
        return host != NULL && host->v4.v3.statement_failure != NULL ? fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT) : PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT;
    host_v2 = (prdb_host_api_v2){sizeof(prdb_host_api_v2), PRDB_QUERY_ABI_V2_VERSION, host->v4.v3.context, host->v4.v3.executor_request, host->v4.v3.planner_request, host->v4.v3.write_result};
    status = prdb_sdk_sql_parse_select_v15(sql, sql_len, &statement, sizeof(statement), NULL);
    if (status != PRDB_SDK_SQL_OK) return fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    status = prdb_sdk_planner_describe_relation_v1(&host_v2, schema, schema_len,
        statement.base.base.base.base.sql + statement.base.base.base.base.relation.offset,
        statement.base.base.base.base.relation.length, planner_response, planner_response_capacity,
        &planner_response_len, &planned, sizeof(planned), planner_columns, planner_column_capacity);
    if (status != PRDB_SDK_PLANNER_REQUEST_OK) return fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_PLANNER_ERROR);
    status = prdb_sdk_sql_bind_select_v15(&statement, planned.name, planned.name_len, planner_columns, planned.column_count, &binding, sizeof(binding));
    if (status != PRDB_SDK_SQL_BIND_OK) return fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    status = prdb_sdk_optimizer_encode_ordered_composite_group_count_v1(&statement, schema, schema_len, optimizer_request, optimizer_request_capacity, &request_len);
    if (status != PRDB_SDK_OPTIMIZER_ENCODE_OK) return fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    status = prdb_sdk_optimizer_submit_group_count_v1(host, optimizer_request, request_len, optimizer_response, optimizer_response_capacity, &response_len, &optimizer, sizeof(optimizer));
    if (status != PRDB_SDK_OPTIMIZER_RESULT_OK) return fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_PLANNER_ERROR);
    status = prdb_sdk_optimizer_match_ordered_composite_group_count_plan_v1(&statement, &binding, schema, schema_len, optimizer.plan, optimizer.plan_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH);
    status = prdb_sdk_optimizer_validate_executor_relation_v1(&host_v2, optimizer.plan, optimizer.plan_len, executor_response, executor_response_capacity, &executor_response_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    return prdb_sdk_sql_execute_group_v15(&host->v4.v3, schema, schema_len, sql, sql_len, NULL, 0, max_candidates, max_batch_bytes,
        planner_response, planner_response_capacity, executor_response, executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns, executor_column_capacity, cell_scratch, cell_scratch_capacity,
        materialized_bytes, materialized_bytes_capacity, on_group, result_context);
}

int32_t prdb_sdk_sql_execute_ordered_composite_aggregate_optimizer_v1(
    const prdb_host_api_v5 *host, const uint8_t *schema, uint32_t schema_len,
    const uint8_t *sql, uint64_t sql_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *optimizer_request,
    uint64_t optimizer_request_capacity, uint8_t *optimizer_response,
    uint64_t optimizer_response_capacity, uint8_t *planner_response,
    uint64_t planner_response_capacity, uint8_t *executor_response,
    uint64_t executor_response_capacity, uint8_t *batch_response,
    uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v2 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v2 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *materialized_bytes,
    uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_group_row_v15_fn on_group, void *result_context) {
    prdb_sdk_select_v15 statement = {0};
    prdb_sdk_select_binding_v15 binding = {0};
    prdb_sdk_planner_relation_v1 planned = {0};
    prdb_sdk_optimizer_response_v1 optimizer = {0};
    prdb_host_api_v2 host_v2;
    uint64_t planner_response_len = 0, request_len = 0, response_len = 0;
    uint64_t executor_response_len = 0;
    int32_t status;
    if (host == NULL || host->v4.v3.struct_size < sizeof(host->v4.v3) ||
        host->v4.v3.abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->v4.v3.statement_failure == NULL ||
        host->v4.v3.planner_request == NULL ||
        host->v4.v3.executor_request == NULL || host->optimizer_request == NULL ||
        schema == NULL || sql == NULL || optimizer_request == NULL ||
        optimizer_response == NULL || planner_response == NULL ||
        executor_response == NULL || planner_columns == NULL ||
        optimizer_request_capacity == 0 || optimizer_response_capacity == 0 ||
        planner_response_capacity == 0 || executor_response_capacity == 0)
        return host != NULL && host->v4.v3.statement_failure != NULL
                   ? fail_statement(&host->v4.v3,
                                   PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT)
                   : PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT;
    host_v2 = (prdb_host_api_v2){
        sizeof(prdb_host_api_v2), PRDB_QUERY_ABI_V2_VERSION,
        host->v4.v3.context, host->v4.v3.executor_request,
        host->v4.v3.planner_request, host->v4.v3.write_result};
    status = prdb_sdk_sql_parse_select_v15(
        sql, sql_len, &statement, sizeof(statement), NULL);
    if (status != PRDB_SDK_SQL_OK)
        return fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    const prdb_sdk_select_v2 *base = &statement.base.base.base.base;
    status = prdb_sdk_planner_describe_relation_v1(
        &host_v2, schema, schema_len,
        base->sql + base->relation.offset, base->relation.length,
        planner_response, planner_response_capacity, &planner_response_len,
        &planned, sizeof(planned), planner_columns, planner_column_capacity);
    if (status != PRDB_SDK_PLANNER_REQUEST_OK)
        return fail_statement(&host->v4.v3,
                              PRDB_SDK_SQL_EXEC_PLANNER_ERROR);
    status = prdb_sdk_sql_bind_select_v15(
        &statement, planned.name, planned.name_len, planner_columns,
        planned.column_count, &binding, sizeof(binding));
    if (status != PRDB_SDK_SQL_BIND_OK)
        return fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    status = prdb_sdk_optimizer_encode_ordered_composite_group_aggregate_v1(
        &statement, schema, schema_len, optimizer_request,
        optimizer_request_capacity, &request_len);
    if (status != PRDB_SDK_OPTIMIZER_ENCODE_OK)
        return fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    status = prdb_sdk_optimizer_submit_group_count_v1(
        host, optimizer_request, request_len, optimizer_response,
        optimizer_response_capacity, &response_len, &optimizer,
        sizeof(optimizer));
    if (status != PRDB_SDK_OPTIMIZER_RESULT_OK)
        return fail_statement(&host->v4.v3,
                              PRDB_SDK_SQL_EXEC_PLANNER_ERROR);
    status = prdb_sdk_optimizer_match_ordered_composite_group_aggregate_plan_v1(
        &statement, &binding, schema, schema_len, optimizer.plan,
        optimizer.plan_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK)
        return fail_statement(&host->v4.v3,
                              PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH);
    status = prdb_sdk_optimizer_validate_executor_relation_v1(
        &host_v2, optimizer.plan, optimizer.plan_len, executor_response,
        executor_response_capacity, &executor_response_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK)
        return fail_statement(&host->v4.v3,
                              PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    return prdb_sdk_sql_execute_group_v15(
        &host->v4.v3, schema, schema_len, sql, sql_len, NULL, 0,
        max_candidates, max_batch_bytes, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response,
        batch_response_capacity, planner_columns, planner_column_capacity,
        executor_columns, executor_column_capacity, cell_scratch,
        cell_scratch_capacity, materialized_bytes,
        materialized_bytes_capacity, on_group, result_context);
}

int32_t prdb_sdk_sql_execute_group_aggregate_optimizer_v1(
    const prdb_host_api_v5 *host, const uint8_t *schema, uint32_t schema_len,
    const uint8_t *sql, uint64_t sql_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *optimizer_request,
    uint64_t optimizer_request_capacity, uint8_t *optimizer_response,
    uint64_t optimizer_response_capacity, uint8_t *planner_response,
    uint64_t planner_response_capacity, uint8_t *executor_response,
    uint64_t executor_response_capacity, uint8_t *batch_response,
    uint64_t batch_response_capacity, prdb_sdk_sql_column_shape_v2 *planner_columns,
    uint32_t planner_column_capacity, prdb_sdk_sql_column_shape_v2 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *materialized_bytes,
    uint64_t materialized_bytes_capacity, prdb_sdk_sql_group_row_v7_fn on_group,
    void *result_context) {
    prdb_sdk_select_v7 statement = {0};
    prdb_sdk_select_binding_v7 binding = {0};
    prdb_sdk_planner_relation_v1 planned = {0};
    prdb_sdk_optimizer_response_v1 optimizer = {0};
    prdb_host_api_v2 host_v2;
    uint64_t planner_response_len = 0, request_len = 0, response_len = 0, executor_response_len = 0;
    int32_t status;
    if (host == NULL || host->v4.v3.struct_size < sizeof(host->v4.v3) || host->v4.v3.abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->v4.v3.statement_failure == NULL || host->v4.v3.planner_request == NULL || host->v4.v3.executor_request == NULL || host->optimizer_request == NULL ||
        schema == NULL || sql == NULL || optimizer_request == NULL || optimizer_response == NULL || planner_response == NULL || executor_response == NULL || planner_columns == NULL ||
        optimizer_request_capacity == 0 || optimizer_response_capacity == 0 || planner_response_capacity == 0 || executor_response_capacity == 0)
        return host != NULL && host->v4.v3.statement_failure != NULL ? fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT) : PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT;
    host_v2 = (prdb_host_api_v2){sizeof(prdb_host_api_v2), PRDB_QUERY_ABI_V2_VERSION, host->v4.v3.context, host->v4.v3.executor_request, host->v4.v3.planner_request, host->v4.v3.write_result};
    status = prdb_sdk_sql_parse_select_v7(sql, sql_len, &statement, sizeof(statement), NULL);
    if (status != PRDB_SDK_SQL_OK) return fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    status = prdb_sdk_planner_describe_relation_v1(&host_v2, schema, schema_len,
        statement.base.sql + statement.base.relation.offset, statement.base.relation.length,
        planner_response, planner_response_capacity, &planner_response_len, &planned, sizeof(planned),
        planner_columns, planner_column_capacity);
    if (status != PRDB_SDK_PLANNER_REQUEST_OK) return fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_PLANNER_ERROR);
    status = prdb_sdk_sql_bind_select_v7(&statement, planned.name, planned.name_len, planner_columns, planned.column_count, &binding, sizeof(binding));
    if (status != PRDB_SDK_SQL_BIND_OK) return fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    status = prdb_sdk_optimizer_encode_group_aggregate_v1(&statement, schema, schema_len, optimizer_request, optimizer_request_capacity, &request_len);
    if (status != PRDB_SDK_OPTIMIZER_ENCODE_OK) return fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    status = prdb_sdk_optimizer_submit_group_count_v1(host, optimizer_request, request_len, optimizer_response, optimizer_response_capacity, &response_len, &optimizer, sizeof(optimizer));
    if (status != PRDB_SDK_OPTIMIZER_RESULT_OK) return fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_PLANNER_ERROR);
    status = prdb_sdk_optimizer_match_group_aggregate_plan_v1(&statement, &binding, schema, schema_len, optimizer.plan, optimizer.plan_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH);
    status = prdb_sdk_optimizer_validate_executor_relation_v1(&host_v2, optimizer.plan, optimizer.plan_len, executor_response, executor_response_capacity, &executor_response_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    return prdb_sdk_sql_execute_group_v7(&host->v4.v3, schema, schema_len, sql, sql_len, NULL, 0, max_candidates, max_batch_bytes,
        planner_response, planner_response_capacity, executor_response, executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns, executor_column_capacity, cell_scratch, cell_scratch_capacity,
        materialized_bytes, materialized_bytes_capacity, on_group, result_context);
}

int32_t prdb_sdk_sql_execute_group_aggregate_ordered_optimizer_v1(
    const prdb_host_api_v5 *host, const uint8_t *schema, uint32_t schema_len,
    const uint8_t *sql, uint64_t sql_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *optimizer_request,
    uint64_t optimizer_request_capacity, uint8_t *optimizer_response,
    uint64_t optimizer_response_capacity, uint8_t *planner_response,
    uint64_t planner_response_capacity, uint8_t *executor_response,
    uint64_t executor_response_capacity, uint8_t *batch_response,
    uint64_t batch_response_capacity,
    prdb_sdk_sql_column_shape_v2 *planner_columns,
    uint32_t planner_column_capacity,
    prdb_sdk_sql_column_shape_v2 *executor_columns,
    uint32_t executor_column_capacity, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *materialized_bytes,
    uint64_t materialized_bytes_capacity,
    prdb_sdk_sql_group_row_v12_fn on_group, void *result_context) {
    prdb_sdk_select_v12 statement = {0};
    prdb_sdk_select_binding_v12 binding = {0};
    prdb_sdk_planner_relation_v1 planned = {0};
    prdb_sdk_optimizer_response_v1 optimizer = {0};
    prdb_host_api_v2 host_v2;
    uint64_t planner_response_len = 0, request_len = 0, response_len = 0;
    uint64_t executor_response_len = 0;
    int32_t status;
    if (host == NULL || host->v4.v3.struct_size < sizeof(host->v4.v3) ||
        host->v4.v3.abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->v4.v3.statement_failure == NULL ||
        host->v4.v3.planner_request == NULL ||
        host->v4.v3.executor_request == NULL || host->optimizer_request == NULL ||
        schema == NULL || sql == NULL || optimizer_request == NULL ||
        optimizer_response == NULL || planner_response == NULL ||
        executor_response == NULL || planner_columns == NULL ||
        optimizer_request_capacity == 0 || optimizer_response_capacity == 0 ||
        planner_response_capacity == 0 || executor_response_capacity == 0)
        return host != NULL && host->v4.v3.statement_failure != NULL
                   ? fail_statement(&host->v4.v3,
                                   PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT)
                   : PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT;
    host_v2 = (prdb_host_api_v2){
        sizeof(prdb_host_api_v2), PRDB_QUERY_ABI_V2_VERSION,
        host->v4.v3.context, host->v4.v3.executor_request,
        host->v4.v3.planner_request, host->v4.v3.write_result};
    status = prdb_sdk_sql_parse_select_v12(sql, sql_len, &statement,
                                           sizeof(statement), NULL);
    if (status != PRDB_SDK_SQL_OK || statement.order_count == 0 ||
        statement.base.base.base.has_having != 0 ||
        statement.base.base.base.base.aggregate_kind <
            PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR ||
        statement.base.base.base.base.aggregate_kind >
            PRDB_SDK_SQL_V7_AGGREGATE_AVG ||
        statement.base.base.base.base.has_group_by == 0 ||
        statement.base.base.base.base.base.predicate_count != 0)
        return fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    prdb_sdk_select_v7 *statement_v7 = &statement.base.base.base.base;
    prdb_sdk_select_v2 *base = &statement_v7->base;
    status = prdb_sdk_planner_describe_relation_v1(
        &host_v2, schema, schema_len,
        base->sql + base->relation.offset, base->relation.length,
        planner_response, planner_response_capacity, &planner_response_len,
        &planned, sizeof(planned), planner_columns,
        planner_column_capacity);
    if (status != PRDB_SDK_PLANNER_REQUEST_OK)
        return fail_statement(&host->v4.v3,
                              PRDB_SDK_SQL_EXEC_PLANNER_ERROR);
    status = prdb_sdk_sql_bind_select_v12(
        &statement, planned.name, planned.name_len, planner_columns,
        planned.column_count, &binding, sizeof(binding));
    if (status != PRDB_SDK_SQL_BIND_OK)
        return fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    status = prdb_sdk_optimizer_encode_ordered_group_aggregate_v1(
        &statement, schema, schema_len, optimizer_request,
        optimizer_request_capacity, &request_len);
    if (status != PRDB_SDK_OPTIMIZER_ENCODE_OK)
        return fail_statement(&host->v4.v3, PRDB_SDK_SQL_EXEC_BIND_ERROR);
    status = prdb_sdk_optimizer_submit_group_count_v1(
        host, optimizer_request, request_len, optimizer_response,
        optimizer_response_capacity, &response_len, &optimizer,
        sizeof(optimizer));
    if (status != PRDB_SDK_OPTIMIZER_RESULT_OK)
        return fail_statement(&host->v4.v3,
                              PRDB_SDK_SQL_EXEC_PLANNER_ERROR);
    status = prdb_sdk_optimizer_match_ordered_group_aggregate_plan_v1(
        &statement, &binding, schema, schema_len, optimizer.plan,
        optimizer.plan_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK)
        return fail_statement(&host->v4.v3,
                              PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH);
    status = prdb_sdk_optimizer_validate_executor_relation_v1(
        &host_v2, optimizer.plan, optimizer.plan_len, executor_response,
        executor_response_capacity, &executor_response_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK)
        return fail_statement(&host->v4.v3,
                              PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    return prdb_sdk_sql_execute_group_v12(
        &host->v4.v3, schema, schema_len, sql, sql_len, NULL, 0,
        max_candidates, max_batch_bytes, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response,
        batch_response_capacity, planner_columns, planner_column_capacity,
        executor_columns, executor_column_capacity, cell_scratch,
        cell_scratch_capacity, materialized_bytes,
        materialized_bytes_capacity, on_group, result_context);
}

int32_t prdb_sdk_sql_execute_group_v14(
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
    prdb_sdk_sql_group_row_v14_fn on_group,
    void *result_context) {
    return execute_group_internal(
        host_v3, schema, schema_len, sql, sql_len, parameters, NULL, parameter_count,
        max_candidates, max_batch_bytes, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns,
        executor_column_capacity, cell_scratch, cell_scratch_capacity,
        materialized_bytes, materialized_bytes_capacity, NULL, NULL, NULL,
        NULL, NULL, NULL, on_group, NULL, result_context, 0, 0, 0, 0, 1, 1,
        0, 0, NULL, 0, NULL, 0, NULL);
}

int32_t prdb_sdk_sql_execute_group_v15(
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
    prdb_sdk_sql_group_row_v15_fn on_group,
    void *result_context) {
    return execute_group_internal(
        host_v3, schema, schema_len, sql, sql_len, parameters, NULL, parameter_count,
        max_candidates, max_batch_bytes, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns,
        executor_column_capacity, cell_scratch, cell_scratch_capacity,
        materialized_bytes, materialized_bytes_capacity, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, on_group, result_context, 0, 0, 0, 0, 1, 1,
        1, 0, NULL, 0, NULL, 0, NULL);
}

int32_t prdb_sdk_sql_execute_group_v17(
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
    prdb_sdk_sql_group_row_v17_fn on_group, void *result_context) {
    return execute_group_internal(
        host_v3, schema, schema_len, sql, sql_len, parameters, NULL, parameter_count,
        max_candidates, max_batch_bytes, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns,
        executor_column_capacity, cell_scratch, cell_scratch_capacity,
        materialized_bytes, materialized_bytes_capacity, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, result_context, 0, 0, 0, 0, 1, 1,
        1, 1, on_group, 0, NULL, 0, NULL);
}

int32_t prdb_sdk_sql_execute_group_v18(
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
    prdb_sdk_sql_group_row_v18_fn on_group, void *result_context) {
    return execute_group_internal(
        host_v3, schema, schema_len, sql, sql_len, parameters, NULL, parameter_count,
        max_candidates, max_batch_bytes, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns,
        executor_column_capacity, cell_scratch, cell_scratch_capacity,
        materialized_bytes, materialized_bytes_capacity, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, result_context, 0, 0, 0, 0, 1, 1,
        1, 0, NULL, 1, on_group, 0, NULL);
}

int32_t prdb_sdk_sql_execute_group_v21(
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
    prdb_sdk_sql_group_row_v21_fn on_group, void *result_context) {
    return execute_group_internal(
        host_v3, schema, schema_len, sql, sql_len, parameters, NULL, parameter_count,
        max_candidates, max_batch_bytes, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns,
        executor_column_capacity, cell_scratch, cell_scratch_capacity,
        materialized_bytes, materialized_bytes_capacity, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, result_context, 0, 0, 0, 0, 1, 1, 1,
        0, NULL, 1, NULL, PRDB_SDK_SQL_ABI_V21_VERSION, on_group);
}

int32_t prdb_sdk_sql_execute_group_v22(
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
    prdb_sdk_sql_group_row_v22_fn on_group, void *result_context) {
    return execute_group_internal(
        host_v3, schema, schema_len, sql, sql_len, parameters, NULL, parameter_count,
        max_candidates, max_batch_bytes, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns,
        executor_column_capacity, cell_scratch, cell_scratch_capacity,
        materialized_bytes, materialized_bytes_capacity, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, result_context, 0, 0, 0, 0, 1, 1, 1,
        0, NULL, 1, NULL, PRDB_SDK_SQL_ABI_V22_VERSION, on_group);
}

int32_t prdb_sdk_sql_execute_group_v23(
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
    prdb_sdk_sql_group_row_v23_fn on_group, void *result_context) {
    return execute_group_internal(
        host_v3, schema, schema_len, sql, sql_len, parameters, NULL, parameter_count,
        max_candidates, max_batch_bytes, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns,
        executor_column_capacity, cell_scratch, cell_scratch_capacity,
        materialized_bytes, materialized_bytes_capacity, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, result_context, 0, 0, 0, 0, 1, 1, 1,
        0, NULL, 1, NULL, PRDB_SDK_SQL_ABI_V23_VERSION, on_group);
}

int32_t prdb_sdk_sql_execute_group_v24(
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
    prdb_sdk_sql_group_row_v24_fn on_group, void *result_context) {
    return execute_group_internal(
        host_v3, schema, schema_len, sql, sql_len, parameters, NULL, parameter_count,
        max_candidates, max_batch_bytes, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns,
        executor_column_capacity, cell_scratch, cell_scratch_capacity,
        materialized_bytes, materialized_bytes_capacity, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, result_context, 0, 0, 0, 0, 1, 1, 1,
        0, NULL, 1, NULL, PRDB_SDK_SQL_ABI_V24_VERSION, on_group);
}

int32_t prdb_sdk_sql_execute_group_typed_v24(
    const prdb_host_api_v3 *host_v3, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters,
    const prdb_sdk_sql_parameter_metadata_v1 *parameter_metadata,
    uint32_t parameter_count,
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
    prdb_sdk_sql_group_row_v24_fn on_group, void *result_context) {
    if (!valid_group_host(host_v3)) return PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT;
    if (parameter_count != 0 && parameter_metadata == NULL)
        return fail_statement(host_v3, PRDB_SDK_SQL_EXEC_INVALID_ARGUMENT);
    return execute_group_internal(
        host_v3, schema, schema_len, sql, sql_len, parameters,
        parameter_metadata, parameter_count,
        max_candidates, max_batch_bytes, planner_response,
        planner_response_capacity, executor_response,
        executor_response_capacity, batch_response, batch_response_capacity,
        planner_columns, planner_column_capacity, executor_columns,
        executor_column_capacity, cell_scratch, cell_scratch_capacity,
        materialized_bytes, materialized_bytes_capacity, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, result_context, 0, 0, 0, 0, 1, 1, 1,
        0, NULL, 1, NULL, PRDB_SDK_SQL_ABI_V24_VERSION, on_group);
}
