#include "prdb_sdk_sql_join_v2.h"

#include <stddef.h>

static int32_t validate_composite_key(
    const prdb_sdk_value_v1 *key,
    const prdb_sdk_sql_join_key_shape_v2 *shape) {
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
    if (key->type_code != shape->declared_type) {
        return PRDB_SDK_SQL_EVAL_TYPE_MISMATCH;
    }
    {
        uint32_t truth = PRDB_SDK_SQL_TRUTH_UNKNOWN;
        return prdb_sdk_sql_compare_v1(
            key, key, PRDB_SDK_SQL_COMPARE_EQUAL, shape->decimal_scale,
            shape->decimal_scale, &truth);
    }
}

static int32_t composite_rows_equal(
    const prdb_sdk_value_v1 *left_row,
    const prdb_sdk_value_v1 *right_row,
    const prdb_sdk_sql_join_key_shape_v2 *key_shapes, uint32_t key_count,
    int *equal_out) {
    *equal_out = 0;
    for (uint32_t key = 0; key < key_count; ++key) {
        const prdb_sdk_value_v1 *left = &left_row[key];
        const prdb_sdk_value_v1 *right = &right_row[key];
        uint32_t truth = PRDB_SDK_SQL_TRUTH_UNKNOWN;
        int32_t status;

        if (left->state == PRDB_SDK_VALUE_STATE_NULL ||
            right->state == PRDB_SDK_VALUE_STATE_NULL) {
            return PRDB_SDK_SQL_EVAL_OK;
        }
        status = prdb_sdk_sql_compare_v1(
            left, right, PRDB_SDK_SQL_COMPARE_EQUAL,
            key_shapes[key].decimal_scale, key_shapes[key].decimal_scale,
            &truth);
        if (status != PRDB_SDK_SQL_EVAL_OK) return status;
        if (truth != PRDB_SDK_SQL_TRUTH_TRUE) return PRDB_SDK_SQL_EVAL_OK;
    }
    *equal_out = 1;
    return PRDB_SDK_SQL_EVAL_OK;
}

static int32_t validate_key_shapes(
    const prdb_sdk_sql_join_key_shape_v2 *key_shapes, uint32_t key_count) {
    for (uint32_t key = 0; key < key_count; ++key) {
        const prdb_sdk_sql_join_key_shape_v2 *shape = &key_shapes[key];
        if (shape->struct_size != sizeof(*shape) ||
            shape->abi_version != PRDB_SDK_SQL_JOIN_ABI_V2_VERSION ||
            shape->declared_type < PRDB_SDK_VALUE_TYPE_BOOLEAN ||
            shape->declared_type > PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ ||
            (shape->declared_type == PRDB_SDK_VALUE_TYPE_DECIMAL
                 ? shape->decimal_scale > 38
                 : shape->decimal_scale != 0) ||
            shape->reserved[0] != 0 || shape->reserved[1] != 0 ||
            shape->reserved[2] != 0) {
            return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
        }
    }
    return PRDB_SDK_SQL_EVAL_OK;
}

int32_t prdb_sdk_sql_inner_join_pairs_composite_v2(
    const prdb_sdk_value_v1 *left_keys, uint32_t left_count,
    const prdb_sdk_value_v1 *right_keys, uint32_t right_count,
    const prdb_sdk_sql_join_key_shape_v2 *key_shapes, uint32_t key_count,
    prdb_sdk_sql_join_pair_v1 *pairs_out, uint32_t pairs_capacity,
    uint32_t *pair_count_out) {
    uint32_t pair_count = 0;
    int32_t status;

    if (pair_count_out == NULL || key_count < 2 ||
        key_count > PRDB_SDK_SQL_JOIN_MAX_KEYS || key_shapes == NULL ||
        (left_count != 0 && left_keys == NULL) ||
        (right_count != 0 && right_keys == NULL)) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (left_count > PRDB_SDK_SQL_JOIN_MAX_ROWS ||
        right_count > PRDB_SDK_SQL_JOIN_MAX_ROWS) {
        return PRDB_SDK_SQL_JOIN_INPUT_LIMIT_EXCEEDED;
    }
    status = validate_key_shapes(key_shapes, key_count);
    if (status != PRDB_SDK_SQL_EVAL_OK) return status;

    for (uint32_t row = 0; row < left_count; ++row) {
        for (uint32_t key = 0; key < key_count; ++key) {
            status = validate_composite_key(&left_keys[row * key_count + key],
                                            &key_shapes[key]);
            if (status != PRDB_SDK_SQL_EVAL_OK) return status;
        }
    }
    for (uint32_t row = 0; row < right_count; ++row) {
        for (uint32_t key = 0; key < key_count; ++key) {
            status = validate_composite_key(&right_keys[row * key_count + key],
                                            &key_shapes[key]);
            if (status != PRDB_SDK_SQL_EVAL_OK) return status;
        }
    }

    /* Preflight every tuple comparison and output capacity so caller state is
     * untouched on any validation, comparison, or bounds error. */
    for (uint32_t left_row = 0; left_row < left_count; ++left_row) {
        for (uint32_t right_row = 0; right_row < right_count; ++right_row) {
            int equal;
            status = composite_rows_equal(
                &left_keys[left_row * key_count],
                &right_keys[right_row * key_count], key_shapes, key_count,
                &equal);
            if (status != PRDB_SDK_SQL_EVAL_OK) return status;
            if (equal) ++pair_count;
        }
    }
    if (pairs_capacity < pair_count ||
        (pair_count != 0 && pairs_out == NULL)) {
        return PRDB_SDK_SQL_JOIN_OUTPUT_LIMIT_EXCEEDED;
    }

    uint32_t output_index = 0;
    for (uint32_t left_row = 0; left_row < left_count; ++left_row) {
        for (uint32_t right_row = 0; right_row < right_count; ++right_row) {
            int equal;
            status = composite_rows_equal(
                &left_keys[left_row * key_count],
                &right_keys[right_row * key_count], key_shapes, key_count,
                &equal);
            if (status != PRDB_SDK_SQL_EVAL_OK) return status;
            if (equal) {
                pairs_out[output_index++] =
                    (prdb_sdk_sql_join_pair_v1){left_row, right_row};
            }
        }
    }
    *pair_count_out = pair_count;
    return PRDB_SDK_SQL_JOIN_OK;
}
