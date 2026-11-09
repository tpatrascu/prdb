#include "prdb_sdk_sql_order_v3.h"

#include <string.h>

static int32_t validate_sort_key(const prdb_sdk_value_v1 *key,
                                 uint32_t declared_type,
                                 uint8_t decimal_scale) {
    int32_t status = prdb_sdk_value_validate_v1(key);
    if (status == PRDB_SDK_VALUE_INVALID_ARGUMENT) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (status != PRDB_SDK_VALUE_VALID) {
        return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
    }
    if (key->state == PRDB_SDK_VALUE_STATE_REDACTED) {
        return PRDB_SDK_SQL_EVAL_REDACTED_INPUT;
    }
    if (key->state == PRDB_SDK_VALUE_STATE_NULL) {
        return PRDB_SDK_SQL_EVAL_OK;
    }
    if (key->type_code != declared_type) {
        return PRDB_SDK_SQL_EVAL_TYPE_MISMATCH;
    }
    {
        uint32_t ignored_truth;
        status = prdb_sdk_sql_compare_v1(
            key, key, PRDB_SDK_SQL_COMPARE_EQUAL, decimal_scale,
            decimal_scale, &ignored_truth);
    }
    return status;
}

static int32_t key_precedes(const prdb_sdk_value_v1 *left,
                            const prdb_sdk_value_v1 *right,
                            uint32_t direction, uint32_t null_order,
                            uint8_t decimal_scale,
                            int *precedes_out) {
    uint32_t truth;
    int32_t status;

    if (left->state == PRDB_SDK_VALUE_STATE_NULL) {
        *precedes_out = right->state != PRDB_SDK_VALUE_STATE_NULL &&
                        null_order == PRDB_SDK_SQL_ORDER_NULLS_FIRST;
        return PRDB_SDK_SQL_EVAL_OK;
    }
    if (right->state == PRDB_SDK_VALUE_STATE_NULL) {
        *precedes_out = null_order == PRDB_SDK_SQL_ORDER_NULLS_LAST;
        return PRDB_SDK_SQL_EVAL_OK;
    }

    status = prdb_sdk_sql_compare_v1(
        left, right,
        direction == PRDB_SDK_SQL_ORDER_DESCENDING
            ? PRDB_SDK_SQL_COMPARE_GREATER
            : PRDB_SDK_SQL_COMPARE_LESS,
        decimal_scale, decimal_scale, &truth);
    if (status != PRDB_SDK_SQL_EVAL_OK) {
        return status;
    }
    *precedes_out = truth == PRDB_SDK_SQL_TRUTH_TRUE;
    return PRDB_SDK_SQL_EVAL_OK;
}

int32_t prdb_sdk_sql_order_row_indices_v2(
    const prdb_sdk_value_v1 *keys, uint32_t key_count,
    uint32_t declared_type, uint8_t decimal_scale, uint32_t direction,
    uint32_t null_order, uint32_t *row_indices_out,
    uint32_t row_indices_capacity) {
    uint32_t sorted_indices[PRDB_SDK_SQL_ORDER_MAX_ROWS];
    int32_t status;

    if (direction > PRDB_SDK_SQL_ORDER_DESCENDING ||
        (null_order != PRDB_SDK_SQL_ORDER_NULLS_FIRST &&
         null_order != PRDB_SDK_SQL_ORDER_NULLS_LAST) ||
        declared_type < PRDB_SDK_VALUE_TYPE_BOOLEAN ||
        declared_type > PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ ||
        (declared_type == PRDB_SDK_VALUE_TYPE_DECIMAL
             ? decimal_scale > 38
             : decimal_scale != 0) ||
        row_indices_capacity < key_count ||
        (key_count != 0 && (keys == NULL || row_indices_out == NULL))) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (key_count > PRDB_SDK_SQL_ORDER_MAX_ROWS) {
        return PRDB_SDK_SQL_ORDER_LIMIT_EXCEEDED;
    }
    if (key_count == 0) {
        return PRDB_SDK_SQL_ORDER_OK;
    }

    for (uint32_t index = 0; index < key_count; ++index) {
        status = validate_sort_key(&keys[index], declared_type,
                                   decimal_scale);
        if (status != PRDB_SDK_SQL_EVAL_OK) {
            return status;
        }
        sorted_indices[index] = index;
    }

    for (uint32_t index = 1; index < key_count; ++index) {
        uint32_t row_index = sorted_indices[index];
        uint32_t destination = index;
        while (destination != 0) {
            uint32_t previous_index = sorted_indices[destination - 1];
            int precedes;
            status = key_precedes(&keys[row_index], &keys[previous_index],
                                  direction, null_order, decimal_scale,
                                  &precedes);
            if (status != PRDB_SDK_SQL_EVAL_OK) {
                return status;
            }
            if (!precedes) {
                break;
            }
            sorted_indices[destination] = previous_index;
            --destination;
        }
        sorted_indices[destination] = row_index;
    }

    memcpy(row_indices_out, sorted_indices,
           (size_t)key_count * sizeof(*row_indices_out));
    return PRDB_SDK_SQL_ORDER_OK;
}

int32_t prdb_sdk_sql_order_row_indices_v1(
    const prdb_sdk_value_v1 *keys, uint32_t key_count,
    uint32_t declared_type, uint8_t decimal_scale, uint32_t direction,
    uint32_t *row_indices_out, uint32_t row_indices_capacity) {
    return prdb_sdk_sql_order_row_indices_v2(
        keys, key_count, declared_type, decimal_scale, direction,
        PRDB_SDK_SQL_ORDER_NULLS_FIRST, row_indices_out,
        row_indices_capacity);
}

static int32_t compare_order_tuple(const prdb_sdk_sql_order_key_v3 *keys,
                                   uint32_t key_count, uint32_t left_index,
                                   uint32_t right_index, int *comparison_out) {
    for (uint32_t key = 0; key < key_count; ++key) {
        int left_precedes = 0, right_precedes = 0;
        int32_t status = key_precedes(
            &keys[key].values[left_index], &keys[key].values[right_index],
            keys[key].direction, keys[key].null_order,
            keys[key].decimal_scale, &left_precedes);
        if (status != PRDB_SDK_SQL_EVAL_OK) return status;
        if (left_precedes) {
            *comparison_out = -1;
            return PRDB_SDK_SQL_EVAL_OK;
        }
        status = key_precedes(
            &keys[key].values[right_index], &keys[key].values[left_index],
            keys[key].direction, keys[key].null_order,
            keys[key].decimal_scale, &right_precedes);
        if (status != PRDB_SDK_SQL_EVAL_OK) return status;
        if (right_precedes) {
            *comparison_out = 1;
            return PRDB_SDK_SQL_EVAL_OK;
        }
    }
    *comparison_out = 0;
    return PRDB_SDK_SQL_EVAL_OK;
}

int32_t prdb_sdk_sql_order_row_indices_v3(
    const prdb_sdk_sql_order_key_v3 *keys, uint32_t key_count,
    uint32_t row_count, uint32_t *row_indices_out,
    uint32_t row_indices_capacity) {
    uint32_t sorted_indices[PRDB_SDK_SQL_ORDER_MAX_ROWS];
    int32_t status;

    if (key_count == 0 || key_count > PRDB_SDK_SQL_ORDER_MAX_KEYS ||
        keys == NULL || (row_count != 0 && row_indices_out == NULL) ||
        row_indices_capacity < row_count) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (row_count > PRDB_SDK_SQL_ORDER_MAX_ROWS) {
        return PRDB_SDK_SQL_ORDER_LIMIT_EXCEEDED;
    }
    for (uint32_t key = 0; key < key_count; ++key) {
        if ((row_count != 0 && keys[key].values == NULL) ||
            keys[key].declared_type < PRDB_SDK_VALUE_TYPE_BOOLEAN ||
            keys[key].declared_type > PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ ||
            (keys[key].declared_type == PRDB_SDK_VALUE_TYPE_DECIMAL
                 ? keys[key].decimal_scale > 38
                 : keys[key].decimal_scale != 0) ||
            keys[key].direction > PRDB_SDK_SQL_ORDER_DESCENDING ||
            (keys[key].null_order != PRDB_SDK_SQL_ORDER_NULLS_FIRST &&
             keys[key].null_order != PRDB_SDK_SQL_ORDER_NULLS_LAST) ||
            keys[key].reserved != 0) {
            return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
        }
    }
    for (uint32_t row = 0; row < row_count; ++row) {
        for (uint32_t key = 0; key < key_count; ++key) {
            status = validate_sort_key(&keys[key].values[row],
                                       keys[key].declared_type,
                                       keys[key].decimal_scale);
            if (status != PRDB_SDK_SQL_EVAL_OK) return status;
        }
        sorted_indices[row] = row;
    }
    for (uint32_t row = 1; row < row_count; ++row) {
        uint32_t row_index = sorted_indices[row];
        uint32_t destination = row;
        while (destination != 0) {
            uint32_t previous_index = sorted_indices[destination - 1];
            int comparison = 0;
            status = compare_order_tuple(keys, key_count, row_index,
                                         previous_index, &comparison);
            if (status != PRDB_SDK_SQL_EVAL_OK) return status;
            if (comparison >= 0) break;
            sorted_indices[destination] = previous_index;
            --destination;
        }
        sorted_indices[destination] = row_index;
    }
    if (row_count != 0) {
        memcpy(row_indices_out, sorted_indices,
               (size_t)row_count * sizeof(*row_indices_out));
    }
    return PRDB_SDK_SQL_ORDER_OK;
}
