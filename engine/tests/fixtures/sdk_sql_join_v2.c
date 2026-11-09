#include "prdb_sdk_sql_join_v2.h"

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
                               PRDB_SDK_VALUE_TYPE_INTEGER, bytes, 4};
}

static prdb_sdk_value_v1 null_value(void) {
    return (prdb_sdk_value_v1){sizeof(prdb_sdk_value_v1),
                               PRDB_SDK_VALUE_ABI_V1_VERSION,
                               PRDB_SDK_VALUE_STATE_NULL, 0, NULL, 0};
}

static prdb_sdk_value_v1 text_value(const uint8_t *bytes, uint64_t len) {
    return (prdb_sdk_value_v1){sizeof(prdb_sdk_value_v1),
                               PRDB_SDK_VALUE_ABI_V1_VERSION,
                               PRDB_SDK_VALUE_STATE_VALUE,
                               PRDB_SDK_VALUE_TYPE_TEXT, bytes, len};
}

static prdb_sdk_sql_join_key_shape_v2 integer_shape(void) {
    return (prdb_sdk_sql_join_key_shape_v2){
        sizeof(prdb_sdk_sql_join_key_shape_v2),
        PRDB_SDK_SQL_JOIN_ABI_V2_VERSION,
        PRDB_SDK_VALUE_TYPE_INTEGER,
        0,
        {0, 0, 0}};
}

int main(void) {
    uint8_t left_bytes[8][4];
    uint8_t right_bytes[8][4];
    prdb_sdk_sql_join_key_shape_v2 shapes[2] = {
        integer_shape(), integer_shape()};
    prdb_sdk_value_v1 left[] = {
        integer_value(left_bytes[0], 1), integer_value(left_bytes[1], 7),
        integer_value(left_bytes[2], 1), integer_value(left_bytes[3], 8),
        integer_value(left_bytes[4], 2), integer_value(left_bytes[5], 9),
        null_value(), integer_value(left_bytes[7], 1)};
    prdb_sdk_value_v1 right[] = {
        integer_value(right_bytes[0], 1), integer_value(right_bytes[1], 8),
        integer_value(right_bytes[2], 1), integer_value(right_bytes[3], 7),
        integer_value(right_bytes[4], 1), null_value(),
        null_value(), integer_value(right_bytes[7], 1)};
    prdb_sdk_sql_join_pair_v1 pairs[8] = {{0}};
    uint32_t pair_count = 999;

    assert(prdb_sdk_sql_inner_join_pairs_composite_v2(
               left, 4, right, 4, shapes, 2, pairs, 8, &pair_count) ==
           PRDB_SDK_SQL_JOIN_OK);
    assert(pair_count == 2);
    assert(pairs[0].left_row == 0 && pairs[0].right_row == 1);
    assert(pairs[1].left_row == 1 && pairs[1].right_row == 0);

    {
        prdb_sdk_sql_join_pair_v1 unchanged[2] = {{41, 42}, {43, 44}};
        uint32_t unchanged_count = 49;
        prdb_sdk_value_v1 invalid_right[8];
        uint8_t wrong_type_bytes[8] = {0};
        memcpy(invalid_right, right, sizeof(right));
        invalid_right[7] = (prdb_sdk_value_v1){
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_REDACTED, 0, NULL, 0};
        assert(prdb_sdk_sql_inner_join_pairs_composite_v2(
                   left, 4, invalid_right, 4, shapes, 2, unchanged, 2,
                   &unchanged_count) == PRDB_SDK_SQL_EVAL_REDACTED_INPUT);
        assert(unchanged_count == 49 && unchanged[0].left_row == 41 &&
               unchanged[1].right_row == 44);

        memcpy(invalid_right, right, sizeof(right));
        invalid_right[3] = (prdb_sdk_value_v1){
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BIGINT,
            wrong_type_bytes, sizeof(wrong_type_bytes)};
        assert(prdb_sdk_sql_inner_join_pairs_composite_v2(
                   left, 4, invalid_right, 4, shapes, 2, unchanged, 2,
                   &unchanged_count) == PRDB_SDK_SQL_EVAL_TYPE_MISMATCH);
        assert(unchanged_count == 49 && unchanged[0].left_row == 41);

        assert(prdb_sdk_sql_inner_join_pairs_composite_v2(
                   left, 4, right, 4, shapes, 2, unchanged, 1,
                   &unchanged_count) ==
               PRDB_SDK_SQL_JOIN_OUTPUT_LIMIT_EXCEEDED);
        assert(unchanged_count == 49 && unchanged[0].left_row == 41);
    }

    {
        static const uint8_t alpha[] = "alpha";
        static const uint8_t beta[] = "beta";
        prdb_sdk_value_v1 left_mixed[] = {
            integer_value(left_bytes[0], 3), text_value(alpha, 5),
            integer_value(left_bytes[1], 3), text_value(beta, 4)};
        prdb_sdk_value_v1 right_mixed[] = {
            integer_value(right_bytes[0], 3), text_value(beta, 4),
            integer_value(right_bytes[1], 3), text_value(alpha, 5)};
        prdb_sdk_sql_join_key_shape_v2 mixed_shapes[] = {
            integer_shape(),
            {sizeof(prdb_sdk_sql_join_key_shape_v2),
             PRDB_SDK_SQL_JOIN_ABI_V2_VERSION, PRDB_SDK_VALUE_TYPE_TEXT, 0,
             {0, 0, 0}}};
        prdb_sdk_sql_join_pair_v1 mixed_pairs[2] = {{0}};
        uint32_t mixed_count = 0;
        assert(prdb_sdk_sql_inner_join_pairs_composite_v2(
                   left_mixed, 2, right_mixed, 2, mixed_shapes, 2,
                   mixed_pairs, 2, &mixed_count) == PRDB_SDK_SQL_JOIN_OK);
        assert(mixed_count == 2);
        assert(mixed_pairs[0].left_row == 0 && mixed_pairs[0].right_row == 1);
        assert(mixed_pairs[1].left_row == 1 && mixed_pairs[1].right_row == 0);
    }

    {
        prdb_sdk_value_v1 all_null[] = {null_value(), null_value()};
        prdb_sdk_sql_join_pair_v1 unchanged = {51, 52};
        uint32_t unchanged_count = 53;
        assert(prdb_sdk_sql_inner_join_pairs_composite_v2(
                   all_null, 1, all_null, 1, shapes, 2, &unchanged, 1,
                   &unchanged_count) == PRDB_SDK_SQL_JOIN_OK);
        assert(unchanged_count == 0 && unchanged.left_row == 51 &&
               unchanged.right_row == 52);
        assert(prdb_sdk_sql_inner_join_pairs_composite_v2(
                   NULL, 0, NULL, 0, shapes, 1, &unchanged, 1,
                   &unchanged_count) == PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT);
        assert(unchanged_count == 0 && unchanged.left_row == 51);
    }

    {
        uint8_t left_decimal[16] = {0};
        uint8_t right_decimal[16] = {0};
        prdb_sdk_value_v1 decimal_left[] = {
            integer_value(left_bytes[0], 4),
            {sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
             PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_DECIMAL,
             left_decimal, sizeof(left_decimal)}};
        prdb_sdk_value_v1 decimal_right[] = {
            integer_value(right_bytes[0], 4),
            {sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
             PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_DECIMAL,
             right_decimal, sizeof(right_decimal)}};
        prdb_sdk_sql_join_key_shape_v2 decimal_shapes[] = {
            integer_shape(),
            {sizeof(prdb_sdk_sql_join_key_shape_v2),
             PRDB_SDK_SQL_JOIN_ABI_V2_VERSION, PRDB_SDK_VALUE_TYPE_DECIMAL,
             2, {0, 0, 0}}};
        prdb_sdk_sql_join_pair_v1 decimal_pair[1] = {{0}};
        uint32_t decimal_pair_count = 99;
        assert(prdb_sdk_sql_inner_join_pairs_composite_v2(
                   decimal_left, 1, decimal_right, 1, decimal_shapes, 2,
                   decimal_pair, 1, &decimal_pair_count) ==
               PRDB_SDK_SQL_JOIN_OK);
        assert(decimal_pair_count == 1 && decimal_pair[0].left_row == 0 &&
               decimal_pair[0].right_row == 0);
        decimal_shapes[1].reserved[0] = 1;
        assert(prdb_sdk_sql_inner_join_pairs_composite_v2(
                   decimal_left, 1, decimal_right, 1, decimal_shapes, 2,
                   decimal_pair, 1, &decimal_pair_count) ==
               PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT);
        assert(decimal_pair_count == 1 && decimal_pair[0].left_row == 0);
    }

    {
        uint8_t key_bytes[16][4];
        prdb_sdk_value_v1 left_eight[8];
        prdb_sdk_value_v1 right_eight[8];
        prdb_sdk_sql_join_key_shape_v2 eight_shapes[8];
        prdb_sdk_sql_join_pair_v1 eight_pairs[1] = {{0}};
        uint32_t eight_count = 99;
        for (uint32_t key = 0; key < 8; ++key) {
            eight_shapes[key] = integer_shape();
            left_eight[key] = integer_value(key_bytes[key], (int32_t)key);
            right_eight[key] = integer_value(key_bytes[key + 8], (int32_t)key);
        }
        assert(prdb_sdk_sql_inner_join_pairs_composite_v2(
                   left_eight, 1, right_eight, 1, eight_shapes, 8,
                   eight_pairs, 1, &eight_count) == PRDB_SDK_SQL_JOIN_OK);
        assert(eight_count == 1 && eight_pairs[0].left_row == 0 &&
               eight_pairs[0].right_row == 0);
        right_eight[7] = integer_value(key_bytes[15], 99);
        assert(prdb_sdk_sql_inner_join_pairs_composite_v2(
                   left_eight, 1, right_eight, 1, eight_shapes, 8,
                   eight_pairs, 1, &eight_count) == PRDB_SDK_SQL_JOIN_OK);
        assert(eight_count == 0 && eight_pairs[0].left_row == 0);
    }

    {
        prdb_sdk_value_v1 too_many[PRDB_SDK_SQL_JOIN_MAX_ROWS * 2 + 2] = {0};
        prdb_sdk_sql_join_pair_v1 unchanged = {61, 62};
        uint32_t unchanged_count = 63;
        assert(prdb_sdk_sql_inner_join_pairs_composite_v2(
                   too_many, PRDB_SDK_SQL_JOIN_MAX_ROWS + 1, NULL, 0,
                   shapes, 2, &unchanged, 1, &unchanged_count) ==
               PRDB_SDK_SQL_JOIN_INPUT_LIMIT_EXCEEDED);
        assert(unchanged_count == 63 && unchanged.left_row == 61 &&
               unchanged.right_row == 62);
    }

    return 0;
}
