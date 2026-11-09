#include "prdb_sdk_sql_join_v1.h"

#include <assert.h>
#include <string.h>

static prdb_sdk_value_v1 integer_value(uint8_t bytes[4], int32_t value) {
    uint32_t bits = (uint32_t)value;
    bytes[0] = (uint8_t)(bits >> 24);
    bytes[1] = (uint8_t)(bits >> 16);
    bytes[2] = (uint8_t)(bits >> 8);
    bytes[3] = (uint8_t)bits;
    return (prdb_sdk_value_v1){sizeof(prdb_sdk_value_v1),
                               PRDB_SDK_VALUE_ABI_V1_VERSION,
                               PRDB_SDK_VALUE_STATE_VALUE,
                               PRDB_SDK_VALUE_TYPE_INTEGER,
                               bytes, 4};
}

static prdb_sdk_value_v1 null_value(void) {
    return (prdb_sdk_value_v1){sizeof(prdb_sdk_value_v1),
                               PRDB_SDK_VALUE_ABI_V1_VERSION,
                               PRDB_SDK_VALUE_STATE_NULL, 0, NULL, 0};
}

int main(void) {
    uint8_t left_bytes[4][4];
    uint8_t right_bytes[4][4];
    prdb_sdk_value_v1 left[] = {
        integer_value(left_bytes[0], 1), integer_value(left_bytes[1], 2),
        null_value(), integer_value(left_bytes[3], 2)};
    prdb_sdk_value_v1 right[] = {
        integer_value(right_bytes[0], 2), integer_value(right_bytes[1], 1),
        null_value(), integer_value(right_bytes[3], 2)};
    prdb_sdk_sql_join_pair_v1 pairs[8] = {{0}};
    uint32_t pair_count = 999;

    assert(prdb_sdk_sql_inner_join_pairs_v1(
               left, 4, 0, right, 4, 0, PRDB_SDK_VALUE_TYPE_INTEGER,
               pairs, 8, &pair_count) == PRDB_SDK_SQL_JOIN_OK);
    assert(pair_count == 5);
    assert(pairs[0].left_row == 0 && pairs[0].right_row == 1);
    assert(pairs[1].left_row == 1 && pairs[1].right_row == 0);
    assert(pairs[2].left_row == 1 && pairs[2].right_row == 3);
    assert(pairs[3].left_row == 3 && pairs[3].right_row == 0);
    assert(pairs[4].left_row == 3 && pairs[4].right_row == 3);

    {
        prdb_sdk_sql_join_pair_v1 unchanged[4] = {
            {41, 42}, {43, 44}, {45, 46}, {47, 48}};
        uint32_t unchanged_count = 49;
        prdb_sdk_value_v1 invalid_right[4];
        uint8_t wrong_type_bytes[8] = {0};
        memcpy(invalid_right, right, sizeof(right));
        invalid_right[3] = (prdb_sdk_value_v1){
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_REDACTED, 0, NULL, 0};
        assert(prdb_sdk_sql_inner_join_pairs_v1(
                   left, 4, 0, invalid_right, 4, 0,
                   PRDB_SDK_VALUE_TYPE_INTEGER, unchanged, 4,
                   &unchanged_count) == PRDB_SDK_SQL_EVAL_REDACTED_INPUT);
        assert(unchanged_count == 49 && unchanged[0].left_row == 41 &&
               unchanged[3].right_row == 48);
        invalid_right[3] = (prdb_sdk_value_v1){
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BIGINT,
            wrong_type_bytes, sizeof(wrong_type_bytes)};
        assert(prdb_sdk_sql_inner_join_pairs_v1(
                   left, 4, 0, invalid_right, 4, 0,
                   PRDB_SDK_VALUE_TYPE_INTEGER, unchanged, 4,
                   &unchanged_count) == PRDB_SDK_SQL_EVAL_TYPE_MISMATCH);
        assert(unchanged_count == 49 && unchanged[0].left_row == 41);
        invalid_right[3] = (prdb_sdk_value_v1){
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
            right_bytes[3], 3};
        assert(prdb_sdk_sql_inner_join_pairs_v1(
                   left, 4, 0, invalid_right, 4, 0,
                   PRDB_SDK_VALUE_TYPE_INTEGER, unchanged, 4,
                   &unchanged_count) == PRDB_SDK_SQL_EVAL_INVALID_VALUE);
        assert(unchanged_count == 49 && unchanged[0].left_row == 41);
        assert(prdb_sdk_sql_inner_join_pairs_v1(
                   left, 4, 0, right, 4, 0, PRDB_SDK_VALUE_TYPE_INTEGER,
                   unchanged, 4, &unchanged_count) ==
               PRDB_SDK_SQL_JOIN_OUTPUT_LIMIT_EXCEEDED);
        assert(unchanged_count == 49 && unchanged[0].left_row == 41 &&
               unchanged[3].right_row == 48);
    }

    {
        prdb_sdk_value_v1 too_many[PRDB_SDK_SQL_JOIN_MAX_ROWS + 1] = {0};
        prdb_sdk_sql_join_pair_v1 unchanged = {51, 52};
        uint32_t unchanged_count = 53;
        assert(prdb_sdk_sql_inner_join_pairs_v1(
                   too_many, PRDB_SDK_SQL_JOIN_MAX_ROWS + 1, 0, NULL, 0, 0,
                   PRDB_SDK_VALUE_TYPE_INTEGER, &unchanged, 1,
                   &unchanged_count) ==
               PRDB_SDK_SQL_JOIN_INPUT_LIMIT_EXCEEDED);
        assert(unchanged_count == 53 && unchanged.left_row == 51 &&
               unchanged.right_row == 52);
    }

    {
        uint8_t repeated_bytes[4];
        prdb_sdk_value_v1 repeated = integer_value(repeated_bytes, 7);
        prdb_sdk_value_v1 left_max[PRDB_SDK_SQL_JOIN_MAX_ROWS];
        prdb_sdk_value_v1 right_max[PRDB_SDK_SQL_JOIN_MAX_ROWS];
        prdb_sdk_sql_join_pair_v1 all_pairs[PRDB_SDK_SQL_JOIN_MAX_PAIRS];
        uint32_t all_pair_count = 0;
        for (uint32_t index = 0; index < PRDB_SDK_SQL_JOIN_MAX_ROWS; ++index) {
            left_max[index] = repeated;
            right_max[index] = repeated;
        }
        assert(prdb_sdk_sql_inner_join_pairs_v1(
                   left_max, PRDB_SDK_SQL_JOIN_MAX_ROWS, 0, right_max,
                   PRDB_SDK_SQL_JOIN_MAX_ROWS, 0,
                   PRDB_SDK_VALUE_TYPE_INTEGER, all_pairs,
                   PRDB_SDK_SQL_JOIN_MAX_PAIRS,
                   &all_pair_count) == PRDB_SDK_SQL_JOIN_OK);
        assert(all_pair_count == PRDB_SDK_SQL_JOIN_MAX_PAIRS);
        assert(all_pairs[0].left_row == 0 && all_pairs[0].right_row == 0);
        assert(all_pairs[PRDB_SDK_SQL_JOIN_MAX_PAIRS - 1].left_row == 127 &&
               all_pairs[PRDB_SDK_SQL_JOIN_MAX_PAIRS - 1].right_row == 127);
    }

    {
        uint8_t left_decimal[16] = {0};
        uint8_t right_decimal[16] = {0};
        prdb_sdk_value_v1 left_decimal_key;
        prdb_sdk_value_v1 right_decimal_key;
        prdb_sdk_sql_join_pair_v1 decimal_pair[1];
        uint32_t decimal_pair_count = 0;
        left_decimal[15] = 10;
        right_decimal[15] = 10;
        left_decimal_key = (prdb_sdk_value_v1){
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_DECIMAL,
            left_decimal, sizeof(left_decimal)};
        right_decimal_key = (prdb_sdk_value_v1){
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_DECIMAL,
            right_decimal, sizeof(right_decimal)};
        assert(prdb_sdk_sql_inner_join_pairs_v1(
                   &left_decimal_key, 1, 1, &right_decimal_key, 1, 1,
                   PRDB_SDK_VALUE_TYPE_DECIMAL, decimal_pair, 1,
                   &decimal_pair_count) == PRDB_SDK_SQL_JOIN_OK);
        assert(decimal_pair_count == 1 && decimal_pair[0].left_row == 0 &&
               decimal_pair[0].right_row == 0);
        assert(prdb_sdk_sql_inner_join_pairs_v1(
                   &left_decimal_key, 1, 1, &right_decimal_key, 1, 2,
                   PRDB_SDK_VALUE_TYPE_DECIMAL, decimal_pair, 1,
                   &decimal_pair_count) == PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT);
        assert(decimal_pair_count == 1 && decimal_pair[0].left_row == 0);
    }

    return 0;
}
