#include "prdb_sdk_sql_join_v1.h"

#include <stddef.h>

static int32_t validate_join_key(const prdb_sdk_value_v1 *key,
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
        uint32_t truth = PRDB_SDK_SQL_TRUTH_UNKNOWN;
        return prdb_sdk_sql_compare_v1(
            key, key, PRDB_SDK_SQL_COMPARE_EQUAL, decimal_scale,
            decimal_scale, &truth);
    }
}

static int32_t join_keys_equal(const prdb_sdk_value_v1 *left,
                               const prdb_sdk_value_v1 *right,
                               uint8_t left_decimal_scale,
                               uint8_t right_decimal_scale, int *equal_out) {
    uint32_t truth = PRDB_SDK_SQL_TRUTH_UNKNOWN;
    int32_t status;

    *equal_out = 0;
    if (left->state == PRDB_SDK_VALUE_STATE_NULL ||
        right->state == PRDB_SDK_VALUE_STATE_NULL) {
        return PRDB_SDK_SQL_EVAL_OK;
    }
    status = prdb_sdk_sql_compare_v1(
        left, right, PRDB_SDK_SQL_COMPARE_EQUAL, left_decimal_scale,
        right_decimal_scale, &truth);
    if (status == PRDB_SDK_SQL_EVAL_OK) {
        *equal_out = truth == PRDB_SDK_SQL_TRUTH_TRUE;
    }
    return status;
}

int32_t prdb_sdk_sql_inner_join_pairs_v1(
    const prdb_sdk_value_v1 *left_keys, uint32_t left_count,
    uint8_t left_decimal_scale,
    const prdb_sdk_value_v1 *right_keys, uint32_t right_count,
    uint8_t right_decimal_scale, uint32_t declared_type,
    prdb_sdk_sql_join_pair_v1 *pairs_out, uint32_t pairs_capacity,
    uint32_t *pair_count_out) {
    uint32_t pair_count = 0;
    int32_t status;

    if (pair_count_out == NULL ||
        declared_type < PRDB_SDK_VALUE_TYPE_BOOLEAN ||
        declared_type > PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ ||
        (declared_type == PRDB_SDK_VALUE_TYPE_DECIMAL
             ? left_decimal_scale > 38 || right_decimal_scale > 38 ||
                   left_decimal_scale != right_decimal_scale
             : left_decimal_scale != 0 || right_decimal_scale != 0) ||
        (left_count != 0 && left_keys == NULL) ||
        (right_count != 0 && right_keys == NULL)) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (left_count > PRDB_SDK_SQL_JOIN_MAX_ROWS ||
        right_count > PRDB_SDK_SQL_JOIN_MAX_ROWS) {
        return PRDB_SDK_SQL_JOIN_INPUT_LIMIT_EXCEEDED;
    }

    for (uint32_t index = 0; index < left_count; ++index) {
        status = validate_join_key(&left_keys[index], declared_type,
                                   left_decimal_scale);
        if (status != PRDB_SDK_SQL_EVAL_OK) return status;
    }
    for (uint32_t index = 0; index < right_count; ++index) {
        status = validate_join_key(&right_keys[index], declared_type,
                                   right_decimal_scale);
        if (status != PRDB_SDK_SQL_EVAL_OK) return status;
    }

    /* Preflight every comparison before touching caller output. Besides
     * finding the exact result size, this proves that the second deterministic
     * pass cannot fail after it begins writing pairs. */
    for (uint32_t left_index = 0; left_index < left_count; ++left_index) {
        for (uint32_t right_index = 0; right_index < right_count;
             ++right_index) {
            int equal;
            status = join_keys_equal(&left_keys[left_index],
                                     &right_keys[right_index],
                                     left_decimal_scale,
                                     right_decimal_scale, &equal);
            if (status != PRDB_SDK_SQL_EVAL_OK) return status;
            if (equal) ++pair_count;
        }
    }
    if (pairs_capacity < pair_count ||
        (pair_count != 0 && pairs_out == NULL)) {
        return PRDB_SDK_SQL_JOIN_OUTPUT_LIMIT_EXCEEDED;
    }

    uint32_t output_index = 0;
    for (uint32_t left_index = 0; left_index < left_count; ++left_index) {
        for (uint32_t right_index = 0; right_index < right_count;
             ++right_index) {
            int equal;
            status = join_keys_equal(&left_keys[left_index],
                                     &right_keys[right_index],
                                     left_decimal_scale,
                                     right_decimal_scale, &equal);
            if (status != PRDB_SDK_SQL_EVAL_OK) return status;
            if (equal) {
                pairs_out[output_index++] =
                    (prdb_sdk_sql_join_pair_v1){left_index, right_index};
            }
        }
    }
    *pair_count_out = pair_count;
    return PRDB_SDK_SQL_JOIN_OK;
}
