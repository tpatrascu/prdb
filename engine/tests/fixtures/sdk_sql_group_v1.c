#include "prdb_sdk_sql_group_v1.h"

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
                               bytes,
                               4};
}

static prdb_sdk_value_v1 null_value(void) {
    return (prdb_sdk_value_v1){sizeof(prdb_sdk_value_v1),
                               PRDB_SDK_VALUE_ABI_V1_VERSION,
                               PRDB_SDK_VALUE_STATE_NULL,
                               0,
                               NULL,
                               0};
}

static prdb_sdk_value_v1 bigint_value(uint8_t bytes[8], int64_t value) {
    uint64_t bits = (uint64_t)value;
    for (uint32_t index = 0; index < 8; ++index) {
        bytes[7 - index] = (uint8_t)(bits >> (index * 8));
    }
    return (prdb_sdk_value_v1){sizeof(prdb_sdk_value_v1),
                               PRDB_SDK_VALUE_ABI_V1_VERSION,
                               PRDB_SDK_VALUE_STATE_VALUE,
                               PRDB_SDK_VALUE_TYPE_BIGINT,
                               bytes,
                               8};
}

int main(void) {
    uint8_t bytes[3][4];
    prdb_sdk_value_v1 keys[] = {
        integer_value(bytes[0], 8), integer_value(bytes[1], 3),
        integer_value(bytes[2], 8), null_value(), null_value()};
    prdb_sdk_sql_count_group_v1 groups[5] = {{0}};
    uint32_t group_count = 999;

    assert(prdb_sdk_sql_group_count_v1(
               keys, 5, PRDB_SDK_VALUE_TYPE_INTEGER, 0, groups, 5,
               &group_count) == PRDB_SDK_SQL_GROUP_OK);
    assert(group_count == 3);
    assert(groups[0].representative_row == 0 && groups[0].count == 2);
    assert(groups[1].representative_row == 1 && groups[1].count == 1);
    assert(groups[2].representative_row == 3 && groups[2].count == 2);

    {
        prdb_sdk_sql_count_group_v1 unchanged[2] = {
            {41, UINT64_C(42)}, {43, UINT64_C(44)}};
        uint32_t unchanged_count = 45;
        uint8_t bigint_bytes[8] = {0};
        prdb_sdk_value_v1 invalid_keys[2] = {keys[0], keys[1]};
        invalid_keys[1] = (prdb_sdk_value_v1){
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_REDACTED, 0, NULL, 0};
        assert(prdb_sdk_sql_group_count_v1(
                   invalid_keys, 2, PRDB_SDK_VALUE_TYPE_INTEGER, 0,
                   unchanged, 2, &unchanged_count) ==
               PRDB_SDK_SQL_EVAL_REDACTED_INPUT);
        assert(unchanged_count == 45 && unchanged[0].representative_row == 41 &&
               unchanged[0].count == 42 &&
               unchanged[1].representative_row == 43 &&
               unchanged[1].count == 44);

        invalid_keys[1] = (prdb_sdk_value_v1){
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BIGINT,
            bigint_bytes, sizeof(bigint_bytes)};
        assert(prdb_sdk_sql_group_count_v1(
                   invalid_keys, 2, PRDB_SDK_VALUE_TYPE_INTEGER, 0,
                   unchanged, 2, &unchanged_count) ==
               PRDB_SDK_SQL_EVAL_TYPE_MISMATCH);
        assert(unchanged_count == 45 && unchanged[0].representative_row == 41);

        invalid_keys[1] = (prdb_sdk_value_v1){
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
            bytes[1], 3};
        assert(prdb_sdk_sql_group_count_v1(
                   invalid_keys, 2, PRDB_SDK_VALUE_TYPE_INTEGER, 0,
                   unchanged, 2, &unchanged_count) ==
               PRDB_SDK_SQL_EVAL_INVALID_VALUE);
        assert(unchanged_count == 45 && unchanged[0].representative_row == 41);

        assert(prdb_sdk_sql_group_count_v1(
                   keys, 5, PRDB_SDK_VALUE_TYPE_INTEGER, 0, unchanged, 2,
                   &unchanged_count) == PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT);
        assert(unchanged_count == 45 && unchanged[0].representative_row == 41);
    }

    {
        prdb_sdk_value_v1 too_many[PRDB_SDK_SQL_GROUP_MAX_ROWS + 1] = {0};
        prdb_sdk_sql_count_group_v1 unchanged = {77, UINT64_C(78)};
        uint32_t unchanged_count = 79;
        assert(prdb_sdk_sql_group_count_v1(
                   too_many, PRDB_SDK_SQL_GROUP_MAX_ROWS + 1,
                   PRDB_SDK_VALUE_TYPE_INTEGER, 0, &unchanged, 1,
                   &unchanged_count) == PRDB_SDK_SQL_GROUP_LIMIT_EXCEEDED);
        assert(unchanged_count == 79 && unchanged.representative_row == 77 &&
               unchanged.count == 78);
    }

    group_count = 999;
    assert(prdb_sdk_sql_group_count_v1(
               NULL, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, NULL, 0,
               &group_count) == PRDB_SDK_SQL_GROUP_OK);
    assert(group_count == 0);

    {
        prdb_sdk_value_v1 result = {
            sizeof(result), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BIGINT,
            bytes[0], sizeof(bytes[0])};
        uint8_t bigint_bytes[8] = {0};
        int32_t result_number = 0;
        assert(prdb_sdk_sql_minmax_v1(
                   keys, 5, PRDB_SDK_VALUE_TYPE_INTEGER, 0,
                   PRDB_SDK_SQL_MINMAX_MIN, &result, sizeof(result)) ==
               PRDB_SDK_SQL_EVAL_OK);
        assert(prdb_sdk_value_get_integer_v1(&result, &result_number) ==
               PRDB_SDK_VALUE_ACCESS_OK);
        assert(result_number == 3);
        assert(prdb_sdk_sql_minmax_v1(
                   keys, 5, PRDB_SDK_VALUE_TYPE_INTEGER, 0,
                   PRDB_SDK_SQL_MINMAX_MAX, &result, sizeof(result)) ==
               PRDB_SDK_SQL_EVAL_OK);
        assert(prdb_sdk_value_get_integer_v1(&result, &result_number) ==
               PRDB_SDK_VALUE_ACCESS_OK);
        assert(result_number == 8);
        assert(prdb_sdk_sql_minmax_v1(
                   NULL, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0,
                   PRDB_SDK_SQL_MINMAX_MIN, &result, sizeof(result)) ==
               PRDB_SDK_SQL_EVAL_OK);
        assert(result.state == PRDB_SDK_VALUE_STATE_NULL &&
               result.type_code == 0 && result.data == NULL &&
               result.data_len == 0);
        assert(prdb_sdk_sql_minmax_v1(
                   &keys[3], 2, PRDB_SDK_VALUE_TYPE_INTEGER, 0,
                   PRDB_SDK_SQL_MINMAX_MAX, &result, sizeof(result)) ==
               PRDB_SDK_SQL_EVAL_OK);
        assert(result.state == PRDB_SDK_VALUE_STATE_NULL);

        result = (prdb_sdk_value_v1){
            sizeof(result), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
            bytes[0], sizeof(bytes[0])};
        prdb_sdk_value_v1 invalid_values[2] = {0};
        invalid_values[0] = keys[0];
        invalid_values[1] = (prdb_sdk_value_v1){
            sizeof(result), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_REDACTED, 0, NULL, 0};
        assert(prdb_sdk_sql_minmax_v1(
                   invalid_values, 2, PRDB_SDK_VALUE_TYPE_INTEGER, 0,
                   PRDB_SDK_SQL_MINMAX_MIN, &result, sizeof(result)) ==
               PRDB_SDK_SQL_EVAL_REDACTED_INPUT);
        assert(prdb_sdk_value_get_integer_v1(&result, &result_number) ==
                   PRDB_SDK_VALUE_ACCESS_OK &&
               result_number == 8);
        invalid_values[1] = (prdb_sdk_value_v1){
            sizeof(result), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BIGINT,
            bigint_bytes, sizeof(bigint_bytes)};
        assert(prdb_sdk_sql_minmax_v1(
                   invalid_values, 2, PRDB_SDK_VALUE_TYPE_INTEGER, 0,
                   PRDB_SDK_SQL_MINMAX_MAX, &result, sizeof(result)) ==
               PRDB_SDK_SQL_EVAL_TYPE_MISMATCH);
        assert(prdb_sdk_value_get_integer_v1(&result, &result_number) ==
                   PRDB_SDK_VALUE_ACCESS_OK &&
               result_number == 8);
        invalid_values[1] = (prdb_sdk_value_v1){
            sizeof(result), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
            bytes[1], 3};
        assert(prdb_sdk_sql_minmax_v1(
                   invalid_values, 2, PRDB_SDK_VALUE_TYPE_INTEGER, 0,
                   PRDB_SDK_SQL_MINMAX_MIN, &result, sizeof(result)) ==
               PRDB_SDK_SQL_EVAL_INVALID_VALUE);
        assert(prdb_sdk_value_get_integer_v1(&result, &result_number) ==
                   PRDB_SDK_VALUE_ACCESS_OK &&
               result_number == 8);
        assert(prdb_sdk_sql_minmax_v1(
                   keys, 5, PRDB_SDK_VALUE_TYPE_INTEGER, 0,
                   PRDB_SDK_SQL_MINMAX_MIN, &result, 1) ==
               PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT);
        assert(prdb_sdk_value_get_integer_v1(&result, &result_number) ==
                   PRDB_SDK_VALUE_ACCESS_OK &&
               result_number == 8);
        {
            prdb_sdk_value_v1 too_many[PRDB_SDK_SQL_GROUP_MAX_ROWS + 1] = {0};
            assert(prdb_sdk_sql_minmax_v1(
                       too_many, PRDB_SDK_SQL_GROUP_MAX_ROWS + 1,
                       PRDB_SDK_VALUE_TYPE_INTEGER, 0,
                       PRDB_SDK_SQL_MINMAX_MAX, &result, sizeof(result)) ==
                   PRDB_SDK_SQL_GROUP_LIMIT_EXCEEDED);
            assert(prdb_sdk_value_get_integer_v1(&result, &result_number) ==
                       PRDB_SDK_VALUE_ACCESS_OK &&
                   result_number == 8);
        }
    }

    {
        uint8_t decimal_bytes[2][16] = {{0}};
        prdb_sdk_value_v1 decimal_values[2] = {{0}};
        prdb_sdk_value_v1 decimal_min = {0};
        decimal_bytes[0][15] = 0xB6;
        decimal_bytes[1][15] = 0x78;
        decimal_bytes[1][14] = 0x03;
        decimal_values[0] = (prdb_sdk_value_v1){
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_DECIMAL,
            decimal_bytes[0], sizeof(decimal_bytes[0])};
        decimal_values[1] = (prdb_sdk_value_v1){
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_DECIMAL,
            decimal_bytes[1], sizeof(decimal_bytes[1])};
        assert(prdb_sdk_sql_minmax_v1(
                   decimal_values, 2, PRDB_SDK_VALUE_TYPE_DECIMAL, 2,
                   PRDB_SDK_SQL_MINMAX_MIN, &decimal_min,
                   sizeof(decimal_min)) == PRDB_SDK_SQL_EVAL_OK);
        assert(decimal_min.data == decimal_bytes[0]);
    }
    {
        uint8_t integer_bytes[3][8] = {{0}};
        uint8_t result_bytes[16];
        prdb_sdk_value_v1 sum_values[3] = {
            bigint_value(integer_bytes[0], 10),
            bigint_value(integer_bytes[1], -3), null_value()};
        prdb_sdk_value_v1 sum = {0};
        int64_t sum_number = 0;

        memset(result_bytes, 0xA5, sizeof(result_bytes));
        assert(prdb_sdk_sql_sum_v1(
                   sum_values, 3, PRDB_SDK_VALUE_TYPE_BIGINT, 0, 0, &sum,
                   sizeof(sum), result_bytes, sizeof(result_bytes)) ==
               PRDB_SDK_SQL_EVAL_OK);
        assert(sum.type_code == PRDB_SDK_VALUE_TYPE_BIGINT &&
               sum.data == result_bytes && sum.data_len == 8);
        assert(prdb_sdk_value_get_bigint_v1(&sum, &sum_number) ==
               PRDB_SDK_VALUE_ACCESS_OK);
        assert(sum_number == 7);

        {
            uint8_t cancellation_bytes[3][8] = {{0}};
            prdb_sdk_value_v1 cancellation_values[3] = {
                bigint_value(cancellation_bytes[0], INT64_MAX),
                bigint_value(cancellation_bytes[1], 1),
                bigint_value(cancellation_bytes[2], -1)};
            assert(prdb_sdk_sql_sum_v1(
                       cancellation_values, 3, PRDB_SDK_VALUE_TYPE_BIGINT, 0,
                       0, &sum, sizeof(sum), result_bytes,
                       sizeof(result_bytes)) == PRDB_SDK_SQL_EVAL_OK);
            assert(prdb_sdk_value_get_bigint_v1(&sum, &sum_number) ==
                   PRDB_SDK_VALUE_ACCESS_OK);
            assert(sum_number == INT64_MAX);
        }

        {
            uint8_t overflow_bytes[2][8] = {{0}};
            prdb_sdk_value_v1 overflow_values[2] = {
                bigint_value(overflow_bytes[0], INT64_MAX),
                bigint_value(overflow_bytes[1], 1)};
            prdb_sdk_value_v1 unchanged_sum = sum;
            uint8_t before[16];
            memcpy(before, result_bytes, sizeof(before));
            assert(prdb_sdk_sql_sum_v1(
                       overflow_values, 2, PRDB_SDK_VALUE_TYPE_BIGINT, 0, 0,
                       &unchanged_sum, sizeof(unchanged_sum), result_bytes,
                       sizeof(result_bytes)) ==
                   PRDB_SDK_SQL_EVAL_NUMERIC_OVERFLOW);
            assert(unchanged_sum.data == sum.data &&
                   unchanged_sum.data_len == sum.data_len &&
                   memcmp(result_bytes, before, sizeof(before)) == 0);
        }

        {
            uint8_t decimal_bytes[2][16] = {{0}};
            prdb_sdk_value_v1 decimal_values[2] = {{0}};
            prdb_sdk_value_v1 decimal_sum = {0};
            decimal_bytes[0][15] = 123;
            memset(decimal_bytes[1], 0xFF, sizeof(decimal_bytes[1]));
            decimal_bytes[1][15] = 0xE9;
            decimal_values[0] = (prdb_sdk_value_v1){
                sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
                PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_DECIMAL,
                decimal_bytes[0], sizeof(decimal_bytes[0])};
            decimal_values[1] = (prdb_sdk_value_v1){
                sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
                PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_DECIMAL,
                decimal_bytes[1], sizeof(decimal_bytes[1])};
            assert(prdb_sdk_sql_sum_v1(
                       decimal_values, 2, PRDB_SDK_VALUE_TYPE_DECIMAL, 5, 2,
                       &decimal_sum, sizeof(decimal_sum), result_bytes,
                       sizeof(result_bytes)) == PRDB_SDK_SQL_EVAL_OK);
            assert(decimal_sum.type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
                   decimal_sum.data_len == 16 &&
                   decimal_sum.data == result_bytes &&
                   result_bytes[15] == 100);
        }

        {
            uint8_t decimal_bytes[3][16] = {{0}};
            prdb_sdk_value_v1 decimal_values[3] = {{0}};
            prdb_sdk_value_v1 decimal_sum = {0};
            static const uint8_t max_decimal_coefficient[16] = {
                0x4B, 0x3B, 0x4C, 0xA8, 0x5A, 0x86, 0xC4, 0x7A,
                0x09, 0x8A, 0x22, 0x3F, 0xFF, 0xFF, 0xFF, 0xFF};
            for (uint32_t index = 0; index < 3; ++index) {
                memcpy(decimal_bytes[index], max_decimal_coefficient,
                       sizeof(max_decimal_coefficient));
                decimal_values[index] = (prdb_sdk_value_v1){
                    sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
                    PRDB_SDK_VALUE_STATE_VALUE,
                    PRDB_SDK_VALUE_TYPE_DECIMAL, decimal_bytes[index], 16};
            }
            assert(prdb_sdk_sql_sum_v1(
                       decimal_values, 3, PRDB_SDK_VALUE_TYPE_DECIMAL, 38, 0,
                       &decimal_sum, sizeof(decimal_sum), result_bytes,
                       sizeof(result_bytes)) ==
                   PRDB_SDK_SQL_EVAL_NUMERIC_OVERFLOW);
        }

        assert(prdb_sdk_sql_sum_v1(
                   NULL, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, &sum,
                   sizeof(sum), NULL, 0) == PRDB_SDK_SQL_EVAL_OK);
        assert(sum.state == PRDB_SDK_VALUE_STATE_NULL && sum.type_code == 0 &&
               sum.data == NULL && sum.data_len == 0);
    }
    return 0;
}
