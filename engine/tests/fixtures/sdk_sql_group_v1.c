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
    return 0;
}
