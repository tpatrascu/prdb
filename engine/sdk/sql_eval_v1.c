#include "sql_internal.h"

PRDB_SDK_INTERNAL int prdb_sdk_internal_decimal_fits_precision(const uint8_t *coefficient,
                                  uint8_t precision) {
    uint8_t magnitude[16];
    uint8_t limit[16] = {0};
    int negative = (coefficient[0] & 0x80) != 0;
    uint16_t carry;

    if (negative) {
        carry = 1;
        for (size_t index = 16; index > 0; --index) {
            uint16_t value = (uint16_t)(uint8_t)~coefficient[index - 1] + carry;
            magnitude[index - 1] = (uint8_t)value;
            carry = value >> 8;
        }
    } else {
        memcpy(magnitude, coefficient, sizeof(magnitude));
    }

    limit[15] = 1;
    for (uint8_t digit = 0; digit < precision; ++digit) {
        carry = 0;
        for (size_t index = 16; index > 0; --index) {
            uint16_t value = (uint16_t)limit[index - 1] * 10 + carry;
            limit[index - 1] = (uint8_t)value;
            carry = value >> 8;
        }
        if (carry != 0) {
            return 0;
        }
    }
    carry = 1;
    for (size_t index = 16; index > 0 && carry != 0; --index) {
        if (limit[index - 1] == 0) {
            limit[index - 1] = 0xff;
        } else {
            --limit[index - 1];
            carry = 0;
        }
    }
    for (size_t index = 0; index < sizeof(magnitude); ++index) {
        if (magnitude[index] < limit[index]) {
            return 1;
        }
        if (magnitude[index] > limit[index]) {
            return 0;
        }
    }
    return 1;
}

int32_t prdb_sdk_sql_bind_parameter_v1(
    uint32_t parameter_index, const prdb_sdk_value_v1 *parameters,
    uint32_t parameter_count,
    uint32_t expected_type, uint8_t expected_decimal_precision,
    uint8_t expected_decimal_scale, prdb_sdk_value_v1 *out,
    uint32_t out_capacity) {
    const prdb_sdk_value_v1 *parameter;
    int32_t status;

    if (out == NULL || out_capacity < sizeof(*out) ||
        parameter_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        (parameter_count != 0 && parameters == NULL) ||
        parameter_index == 0 ||
        parameter_index > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        expected_type < PRDB_SDK_VALUE_TYPE_BOOLEAN ||
        expected_type > PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ ||
        (expected_type == PRDB_SDK_VALUE_TYPE_DECIMAL &&
         (expected_decimal_precision == 0 || expected_decimal_precision > 38 ||
          expected_decimal_scale > expected_decimal_precision)) ||
        (expected_type != PRDB_SDK_VALUE_TYPE_DECIMAL &&
         (expected_decimal_precision != 0 || expected_decimal_scale != 0))) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (parameter_index > parameter_count) {
        return PRDB_SDK_SQL_EVAL_PARAMETER_MISSING;
    }
    parameter = &parameters[parameter_index - 1];
    status = prdb_sdk_value_validate_v1(parameter);
    if (status == PRDB_SDK_VALUE_INVALID_ARGUMENT) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (status != PRDB_SDK_VALUE_VALID) {
        return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
    }
    if (parameter->state == PRDB_SDK_VALUE_STATE_REDACTED) {
        return PRDB_SDK_SQL_EVAL_REDACTED_INPUT;
    }
    if (parameter->state == PRDB_SDK_VALUE_STATE_VALUE &&
        parameter->type_code != expected_type) {
        return PRDB_SDK_SQL_EVAL_TYPE_MISMATCH;
    }
    if (parameter->state == PRDB_SDK_VALUE_STATE_VALUE &&
        expected_type == PRDB_SDK_VALUE_TYPE_DECIMAL &&
        !prdb_sdk_internal_decimal_fits_precision(parameter->data,
                                expected_decimal_precision)) {
        return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
    }
    *out = *parameter;
    return PRDB_SDK_SQL_EVAL_OK;
}

static int32_t validate_eval_value(const prdb_sdk_value_v1 *value) {
    int32_t status = prdb_sdk_value_validate_v1(value);
    if (status == PRDB_SDK_VALUE_INVALID_ARGUMENT) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (status != PRDB_SDK_VALUE_VALID) {
        return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
    }
    if (value->state == PRDB_SDK_VALUE_STATE_REDACTED) {
        return PRDB_SDK_SQL_EVAL_REDACTED_INPUT;
    }
    return PRDB_SDK_SQL_EVAL_OK;
}

static int compare_bytes(const uint8_t *left, uint64_t left_len,
                         const uint8_t *right, uint64_t right_len) {
    uint64_t common_len = left_len < right_len ? left_len : right_len;
    int result = common_len == 0 ? 0 : memcmp(left, right, (size_t)common_len);
    if (result != 0) {
        return result < 0 ? -1 : 1;
    }
    if (left_len == right_len) {
        return 0;
    }
    return left_len < right_len ? -1 : 1;
}

static int compare_signed_bytes(const uint8_t *left, const uint8_t *right,
                                size_t length) {
    int left_negative = (left[0] & 0x80) != 0;
    int right_negative = (right[0] & 0x80) != 0;
    if (left_negative != right_negative) {
        return left_negative ? -1 : 1;
    }
    return compare_bytes(left, length, right, length);
}

static int32_t compare_value_payloads(const prdb_sdk_value_v1 *left,
                                      const prdb_sdk_value_v1 *right,
                                      uint8_t left_decimal_scale,
                                      uint8_t right_decimal_scale,
                                      int *ordering) {
    switch (left->type_code) {
    case PRDB_SDK_VALUE_TYPE_BOOLEAN:
        *ordering = (left->data[0] > right->data[0]) -
                    (left->data[0] < right->data[0]);
        break;
    case PRDB_SDK_VALUE_TYPE_SMALLINT:
        *ordering = compare_signed_bytes(left->data, right->data, 2);
        break;
    case PRDB_SDK_VALUE_TYPE_INTEGER:
    case PRDB_SDK_VALUE_TYPE_DATE:
        *ordering = compare_signed_bytes(left->data, right->data, 4);
        break;
    case PRDB_SDK_VALUE_TYPE_BIGINT:
    case PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ:
        *ordering = compare_signed_bytes(left->data, right->data, 8);
        break;
    case PRDB_SDK_VALUE_TYPE_DECIMAL:
        if (left_decimal_scale > 38 || right_decimal_scale > 38) {
            return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
        }
        if (left_decimal_scale != right_decimal_scale) {
            return PRDB_SDK_SQL_EVAL_TYPE_MISMATCH;
        }
        *ordering = compare_signed_bytes(left->data, right->data, 16);
        break;
    case PRDB_SDK_VALUE_TYPE_TEXT:
    case PRDB_SDK_VALUE_TYPE_VARCHAR:
    case PRDB_SDK_VALUE_TYPE_BYTEA:
    case PRDB_SDK_VALUE_TYPE_UUID:
        *ordering = compare_bytes(left->data, left->data_len, right->data,
                                  right->data_len);
        break;
    default:
        return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
    }
    return PRDB_SDK_SQL_EVAL_OK;
}

int32_t prdb_sdk_sql_compare_v1(const prdb_sdk_value_v1 *left,
                                const prdb_sdk_value_v1 *right,
                                uint32_t comparison, uint8_t left_decimal_scale,
                                uint8_t right_decimal_scale,
                                uint32_t *truth_out) {
    int32_t status;
    int ordering;

    if (truth_out == NULL) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (comparison < PRDB_SDK_SQL_COMPARE_EQUAL ||
        comparison > PRDB_SDK_SQL_COMPARE_GREATER_EQUAL) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    status = validate_eval_value(left);
    if (status != PRDB_SDK_SQL_EVAL_OK) {
        return status;
    }
    status = validate_eval_value(right);
    if (status != PRDB_SDK_SQL_EVAL_OK) {
        return status;
    }
    if (left->state == PRDB_SDK_VALUE_STATE_NULL ||
        right->state == PRDB_SDK_VALUE_STATE_NULL) {
        *truth_out = PRDB_SDK_SQL_TRUTH_UNKNOWN;
        return PRDB_SDK_SQL_EVAL_OK;
    }
    if (left->type_code != right->type_code) {
        return PRDB_SDK_SQL_EVAL_TYPE_MISMATCH;
    }
    status = compare_value_payloads(left, right, left_decimal_scale,
                                     right_decimal_scale, &ordering);
    if (status != PRDB_SDK_SQL_EVAL_OK) {
        return status;
    }
    switch (comparison) {
    case PRDB_SDK_SQL_COMPARE_EQUAL:
        *truth_out = ordering == 0 ? PRDB_SDK_SQL_TRUTH_TRUE
                                   : PRDB_SDK_SQL_TRUTH_FALSE;
        break;
    case PRDB_SDK_SQL_COMPARE_NOT_EQUAL:
        *truth_out = ordering != 0 ? PRDB_SDK_SQL_TRUTH_TRUE
                                   : PRDB_SDK_SQL_TRUTH_FALSE;
        break;
    case PRDB_SDK_SQL_COMPARE_LESS:
        *truth_out = ordering < 0 ? PRDB_SDK_SQL_TRUTH_TRUE
                                  : PRDB_SDK_SQL_TRUTH_FALSE;
        break;
    case PRDB_SDK_SQL_COMPARE_LESS_EQUAL:
        *truth_out = ordering <= 0 ? PRDB_SDK_SQL_TRUTH_TRUE
                                   : PRDB_SDK_SQL_TRUTH_FALSE;
        break;
    case PRDB_SDK_SQL_COMPARE_GREATER:
        *truth_out = ordering > 0 ? PRDB_SDK_SQL_TRUTH_TRUE
                                  : PRDB_SDK_SQL_TRUTH_FALSE;
        break;
    case PRDB_SDK_SQL_COMPARE_GREATER_EQUAL:
        *truth_out = ordering >= 0 ? PRDB_SDK_SQL_TRUTH_TRUE
                                   : PRDB_SDK_SQL_TRUTH_FALSE;
        break;
    default:
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    return PRDB_SDK_SQL_EVAL_OK;
}

int32_t prdb_sdk_sql_value_is_null_v1(const prdb_sdk_value_v1 *value,
                                      uint32_t *truth_out) {
    int32_t status;
    if (truth_out == NULL) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    status = validate_eval_value(value);
    if (status != PRDB_SDK_SQL_EVAL_OK) {
        return status;
    }
    *truth_out = value->state == PRDB_SDK_VALUE_STATE_NULL
                     ? PRDB_SDK_SQL_TRUTH_TRUE
                     : PRDB_SDK_SQL_TRUTH_FALSE;
    return PRDB_SDK_SQL_EVAL_OK;
}

static int32_t evaluate_predicate_node_v2(
    const prdb_sdk_select_v1 *statement, uint32_t index,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    prdb_sdk_sql_resolve_column_v1 resolve_column, void *resolver_context,
    const prdb_sdk_select_binding_v1 *binding,
    prdb_sdk_sql_resolve_bound_column_v1 resolve_bound_column,
    uint32_t depth, uint32_t *truth_out) {
    const prdb_sdk_predicate_node_v1 *node = &statement->predicates[index];
    uint32_t left_truth;
    uint32_t right_truth;
    int32_t status;

    if (depth > PRDB_SDK_SQL_MAX_PREDICATE_NODES) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (node->node_kind == PRDB_SDK_SQL_PREDICATE_NOT) {
        status = evaluate_predicate_node_v2(
            statement, node->left_child, parameters, parameter_count,
            resolve_column, resolver_context, binding, resolve_bound_column,
            depth + 1, &left_truth);
        if (status != PRDB_SDK_SQL_EVAL_OK) {
            return status;
        }
        return prdb_sdk_sql_truth_not_v1(left_truth, truth_out);
    }
    if (node->node_kind == PRDB_SDK_SQL_PREDICATE_AND ||
        node->node_kind == PRDB_SDK_SQL_PREDICATE_OR) {
        status = evaluate_predicate_node_v2(
            statement, node->left_child, parameters, parameter_count,
            resolve_column, resolver_context, binding, resolve_bound_column,
            depth + 1, &left_truth);
        if (status != PRDB_SDK_SQL_EVAL_OK) {
            return status;
        }
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
        status = evaluate_predicate_node_v2(
            statement, node->right_child, parameters, parameter_count,
            resolve_column, resolver_context, binding, resolve_bound_column,
            depth + 1, &right_truth);
        if (status != PRDB_SDK_SQL_EVAL_OK) {
            return status;
        }
        return node->node_kind == PRDB_SDK_SQL_PREDICATE_AND
                   ? prdb_sdk_sql_truth_and_v1(left_truth, right_truth,
                                               truth_out)
                   : prdb_sdk_sql_truth_or_v1(left_truth, right_truth,
                                              truth_out);
    }

    {
        prdb_sdk_select_v1 leaf = {0};
        const prdb_sdk_value_v1 *column_value = NULL;
        prdb_sdk_value_v1 bound_parameter;
        const prdb_sdk_value_v1 *predicate_parameter = NULL;
        uint32_t expected_type = 0;
        uint8_t precision = 0;
        uint8_t scale = 0;

        if (node->node_kind == PRDB_SDK_SQL_PREDICATE_COMPARE &&
            node->parameter_index > parameter_count) {
            return PRDB_SDK_SQL_EVAL_PARAMETER_MISSING;
        }
        if (binding == NULL) {
            status = resolve_column(resolver_context, statement->sql,
                                    statement->sql_len, node->column,
                                    &column_value, &expected_type, &precision,
                                    &scale);
        } else {
            const prdb_sdk_sql_bound_column_v1 *column =
                &binding->predicate_columns[index];
            expected_type = column->type_code;
            precision = column->decimal_precision;
            scale = column->decimal_scale;
            status = resolve_bound_column(resolver_context, column->ordinal,
                                          &column_value);
        }
        if (status != PRDB_SDK_SQL_EVAL_OK) {
            return status;
        }
        if (column_value == NULL ||
            expected_type < PRDB_SDK_VALUE_TYPE_BOOLEAN ||
            expected_type > PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ ||
            (expected_type == PRDB_SDK_VALUE_TYPE_DECIMAL
                 ? precision == 0 || precision > 38 || scale > precision
                 : precision != 0 || scale != 0)) {
            return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
        }
        status = validate_eval_value(column_value);
        if (status != PRDB_SDK_SQL_EVAL_OK) {
            return status;
        }
        if (column_value->state == PRDB_SDK_VALUE_STATE_VALUE &&
            column_value->type_code != expected_type) {
            return PRDB_SDK_SQL_EVAL_TYPE_MISMATCH;
        }
        if (node->node_kind != PRDB_SDK_SQL_PREDICATE_COMPARE) {
            if (node->parameter_index != 0) {
                return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
            }
            status = prdb_sdk_sql_value_is_null_v1(column_value, truth_out);
            if (status != PRDB_SDK_SQL_EVAL_OK) {
                return status;
            }
            if (node->node_kind == PRDB_SDK_SQL_PREDICATE_IS_NOT_NULL) {
                *truth_out = *truth_out == PRDB_SDK_SQL_TRUTH_TRUE
                                 ? PRDB_SDK_SQL_TRUTH_FALSE
                                 : PRDB_SDK_SQL_TRUTH_TRUE;
            }
            return PRDB_SDK_SQL_EVAL_OK;
        }
        if (node->comparison < PRDB_SDK_SQL_COMPARE_EQUAL ||
            node->comparison > PRDB_SDK_SQL_COMPARE_GREATER_EQUAL ||
            node->parameter_index == 0) {
            return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
        }
        status = prdb_sdk_sql_bind_parameter_v1(
            node->parameter_index, parameters, parameter_count, expected_type,
            precision, scale, &bound_parameter, sizeof(bound_parameter));
        if (status != PRDB_SDK_SQL_EVAL_OK) {
            return status;
        }
        return prdb_sdk_sql_compare_v1(column_value, &bound_parameter,
                                       node->comparison, scale, scale,
                                       truth_out);
    }
}

int32_t prdb_sdk_sql_eval_select_predicate_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    prdb_sdk_sql_resolve_column_v1 resolve_column, void *resolver_context,
    uint32_t *truth_out) {
    int32_t status;
    uint32_t truth;

    if (statement == NULL || truth_out == NULL ||
        statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->sql == NULL || statement->sql_len == 0 ||
        statement->sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        parameter_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        (parameter_count != 0 && parameters == NULL)) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (statement->predicate_count == 0) {
        if (statement->predicate_root != 0) {
            return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
        }
        *truth_out = PRDB_SDK_SQL_TRUTH_TRUE;
        return PRDB_SDK_SQL_EVAL_OK;
    }
    if (resolve_column == NULL) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    status = prdb_sdk_internal_validate_predicate_tree_v1(statement);
    if (status != PRDB_SDK_SQL_EVAL_OK) {
        return status;
    }
    status = evaluate_predicate_node_v2(
        statement, statement->predicate_root, parameters, parameter_count,
        resolve_column, resolver_context, NULL, NULL, 0, &truth);
    if (status == PRDB_SDK_SQL_EVAL_OK) {
        *truth_out = truth;
    }
    return status;
}

static int valid_bound_column_type_v2(
    const prdb_sdk_sql_bound_column_v1 *column) {
    return column->type_code >= PRDB_SDK_VALUE_TYPE_BOOLEAN &&
           column->type_code <= PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ &&
           (column->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL
                ? column->decimal_precision != 0 &&
                      column->decimal_precision <= 38 &&
                      column->decimal_scale <= column->decimal_precision
                : column->decimal_precision == 0 &&
                      column->decimal_scale == 0);
}

int32_t prdb_sdk_sql_eval_bound_select_predicate_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    prdb_sdk_sql_resolve_bound_column_v1 resolve_column,
    void *resolver_context, uint32_t *truth_out) {
    int32_t status;
    uint32_t truth;

    if (statement == NULL || binding == NULL || truth_out == NULL ||
        statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->sql == NULL || statement->sql_len == 0 ||
        statement->sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        statement->projection_star > 1 ||
        (statement->projection_star == 1
             ? statement->projection_count != 0
             : statement->projection_count == 0 ||
                   statement->projection_count >
                       PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS) ||
        binding->struct_size < sizeof(*binding) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        binding->projection_star != statement->projection_star ||
        binding->projection_count != statement->projection_count ||
        binding->relation_column_count == 0 ||
        binding->relation_column_count >
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS ||
        binding->predicate_count != statement->predicate_count ||
        binding->projection_count > PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS ||
        binding->predicate_count > PRDB_SDK_SQL_MAX_PREDICATE_NODES ||
        parameter_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        (parameter_count != 0 && parameters == NULL)) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (statement->predicate_count == 0) {
        if (statement->predicate_root != 0) {
            return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
        }
        *truth_out = PRDB_SDK_SQL_TRUTH_TRUE;
        return PRDB_SDK_SQL_EVAL_OK;
    }
    if (resolve_column == NULL) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    status = prdb_sdk_internal_validate_predicate_tree_v1(statement);
    if (status != PRDB_SDK_SQL_EVAL_OK) {
        return status;
    }
    for (uint32_t index = 0; index < statement->predicate_count; ++index) {
        uint32_t kind = statement->predicates[index].node_kind;
        if ((kind == PRDB_SDK_SQL_PREDICATE_COMPARE ||
             kind == PRDB_SDK_SQL_PREDICATE_IS_NULL ||
             kind == PRDB_SDK_SQL_PREDICATE_IS_NOT_NULL) &&
            (!valid_bound_column_type_v2(&binding->predicate_columns[index]) ||
             binding->predicate_columns[index].ordinal >=
                 binding->relation_column_count)) {
            return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
        }
    }
    if (binding->projection_star == 0) {
        for (uint32_t index = 0; index < binding->projection_count; ++index) {
            if (binding->projection_ordinals[index] >=
                binding->relation_column_count) {
                return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
            }
        }
    }
    status = evaluate_predicate_node_v2(
        statement, statement->predicate_root, parameters, parameter_count,
        NULL, resolver_context, binding, resolve_column, 0, &truth);
    if (status == PRDB_SDK_SQL_EVAL_OK) {
        *truth_out = truth;
    }
    return status;
}

typedef struct bound_row_resolver_context_v2 {
    const prdb_sdk_value_v1 *cells;
    uint32_t cell_count;
} bound_row_resolver_context_v2;

static int32_t resolve_bound_row_column_v2(
    void *opaque, uint32_t ordinal, const prdb_sdk_value_v1 **value_out) {
    bound_row_resolver_context_v2 *context = opaque;
    if (context == NULL || value_out == NULL || ordinal >= context->cell_count) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    *value_out = &context->cells[ordinal];
    return PRDB_SDK_SQL_EVAL_OK;
}

int32_t prdb_sdk_sql_eval_bound_select_row_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const prdb_sdk_row_v1 *row, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint32_t *truth_out) {
    bound_row_resolver_context_v2 context;
    prdb_sdk_cell_iterator_v1 iterator = {0};
    int32_t status;

    if (statement == NULL || binding == NULL || row == NULL ||
        truth_out == NULL ||
        row->struct_size < sizeof(*row) ||
        row->abi_version != PRDB_SDK_BATCH_ABI_V1_VERSION ||
        row->cell_count != binding->relation_column_count ||
        (row->payload_len != 0 && row->payload == NULL) ||
        (row->cell_count != 0 && cell_scratch == NULL) ||
        cell_scratch_capacity < row->cell_count) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    status = prdb_sdk_row_cells_begin_v1(row, &iterator, sizeof(iterator));
    if (status != PRDB_SDK_BATCH_OK) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    for (uint32_t index = 0; index < row->cell_count; ++index) {
        status = prdb_sdk_cell_next_v1(&iterator, &cell_scratch[index],
                                       sizeof(cell_scratch[index]));
        if (status != PRDB_SDK_BATCH_OK) {
            return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
        }
    }
    context.cells = cell_scratch;
    context.cell_count = row->cell_count;
    return prdb_sdk_sql_eval_bound_select_predicate_v1(
        statement, binding, parameters, parameter_count,
        resolve_bound_row_column_v2, &context, truth_out);
}

int32_t prdb_sdk_sql_value_truth_v1(const prdb_sdk_value_v1 *value,
                                    uint32_t *truth_out) {
    int32_t status;
    if (truth_out == NULL) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    status = validate_eval_value(value);
    if (status != PRDB_SDK_SQL_EVAL_OK) {
        return status;
    }
    if (value->state == PRDB_SDK_VALUE_STATE_NULL) {
        *truth_out = PRDB_SDK_SQL_TRUTH_UNKNOWN;
        return PRDB_SDK_SQL_EVAL_OK;
    }
    if (value->type_code != PRDB_SDK_VALUE_TYPE_BOOLEAN) {
        return PRDB_SDK_SQL_EVAL_TYPE_MISMATCH;
    }
    *truth_out = value->data[0] == 0 ? PRDB_SDK_SQL_TRUTH_FALSE
                                     : PRDB_SDK_SQL_TRUTH_TRUE;
    return PRDB_SDK_SQL_EVAL_OK;
}

static int valid_truth(uint32_t value) {
    return value == PRDB_SDK_SQL_TRUTH_UNKNOWN ||
           value == PRDB_SDK_SQL_TRUTH_FALSE ||
           value == PRDB_SDK_SQL_TRUTH_TRUE;
}

int32_t prdb_sdk_sql_truth_not_v1(uint32_t operand, uint32_t *truth_out) {
    if (truth_out == NULL || !valid_truth(operand)) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    *truth_out = operand == PRDB_SDK_SQL_TRUTH_UNKNOWN
                     ? PRDB_SDK_SQL_TRUTH_UNKNOWN
                     : operand == PRDB_SDK_SQL_TRUTH_TRUE
                           ? PRDB_SDK_SQL_TRUTH_FALSE
                           : PRDB_SDK_SQL_TRUTH_TRUE;
    return PRDB_SDK_SQL_EVAL_OK;
}

int32_t prdb_sdk_sql_truth_and_v1(uint32_t left, uint32_t right,
                                  uint32_t *truth_out) {
    if (truth_out == NULL || !valid_truth(left) || !valid_truth(right)) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (left == PRDB_SDK_SQL_TRUTH_FALSE ||
        right == PRDB_SDK_SQL_TRUTH_FALSE) {
        *truth_out = PRDB_SDK_SQL_TRUTH_FALSE;
    } else if (left == PRDB_SDK_SQL_TRUTH_UNKNOWN ||
               right == PRDB_SDK_SQL_TRUTH_UNKNOWN) {
        *truth_out = PRDB_SDK_SQL_TRUTH_UNKNOWN;
    } else {
        *truth_out = PRDB_SDK_SQL_TRUTH_TRUE;
    }
    return PRDB_SDK_SQL_EVAL_OK;
}

int32_t prdb_sdk_sql_truth_or_v1(uint32_t left, uint32_t right,
                                 uint32_t *truth_out) {
    if (truth_out == NULL || !valid_truth(left) || !valid_truth(right)) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (left == PRDB_SDK_SQL_TRUTH_TRUE ||
        right == PRDB_SDK_SQL_TRUTH_TRUE) {
        *truth_out = PRDB_SDK_SQL_TRUTH_TRUE;
    } else if (left == PRDB_SDK_SQL_TRUTH_UNKNOWN ||
               right == PRDB_SDK_SQL_TRUTH_UNKNOWN) {
        *truth_out = PRDB_SDK_SQL_TRUTH_UNKNOWN;
    } else {
        *truth_out = PRDB_SDK_SQL_TRUTH_FALSE;
    }
    return PRDB_SDK_SQL_EVAL_OK;
}
