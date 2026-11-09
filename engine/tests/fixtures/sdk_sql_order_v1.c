#include "prdb_sdk_sql_order_v3.h"

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
    uint8_t data[4][4];
    prdb_sdk_value_v1 keys[] = {
        integer_value(data[0], 4), integer_value(data[1], 1), null_value(),
        integer_value(data[2], 2), integer_value(data[3], 1)};
    uint32_t order[5] = {99, 99, 99, 99, 99};
    static const uint32_t ascending[] = {2, 1, 4, 3, 0};
    static const uint32_t descending[] = {2, 0, 3, 1, 4};
    static const uint32_t nulls_last[] = {1, 4, 3, 0, 2};
    static const uint32_t descending_nulls_last[] = {0, 3, 1, 4, 2};

    assert(prdb_sdk_sql_order_row_indices_v1(
               keys, 5, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, order, 5) ==
           PRDB_SDK_SQL_ORDER_OK);
    assert(memcmp(order, ascending, sizeof(ascending)) == 0);

    assert(prdb_sdk_sql_order_row_indices_v1(
               keys, 5, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 1, order, 5) ==
           PRDB_SDK_SQL_ORDER_OK);
    assert(memcmp(order, descending, sizeof(descending)) == 0);

    assert(prdb_sdk_sql_order_row_indices_v2(
               keys, 5, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0,
               PRDB_SDK_SQL_ORDER_NULLS_LAST, order, 5) ==
           PRDB_SDK_SQL_ORDER_OK);
    assert(memcmp(order, nulls_last, sizeof(nulls_last)) == 0);
    assert(prdb_sdk_sql_order_row_indices_v2(
               keys, 5, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 1,
               PRDB_SDK_SQL_ORDER_NULLS_LAST, order, 5) ==
           PRDB_SDK_SQL_ORDER_OK);
    assert(memcmp(order, descending_nulls_last,
                  sizeof(descending_nulls_last)) == 0);

    {
        uint8_t primary_bytes[5][4], secondary_bytes[5][4];
        const int32_t primary_values[] = {1, 1, 1, 2, 2};
        const int32_t secondary_values[] = {5, 2, 0, 1, 3};
        prdb_sdk_value_v1 primary[5], secondary[5];
        for (uint32_t index = 0; index < 5; ++index) {
            primary[index] = integer_value(primary_bytes[index],
                                           primary_values[index]);
            secondary[index] = integer_value(secondary_bytes[index],
                                             secondary_values[index]);
        }
        secondary[2] = null_value();
        prdb_sdk_sql_order_key_v3 keys_v3[2] = {
            {primary, PRDB_SDK_VALUE_TYPE_INTEGER, 0,
             PRDB_SDK_SQL_ORDER_ASCENDING, PRDB_SDK_SQL_ORDER_NULLS_FIRST,
             0},
            {secondary, PRDB_SDK_VALUE_TYPE_INTEGER, 0,
             PRDB_SDK_SQL_ORDER_DESCENDING, PRDB_SDK_SQL_ORDER_NULLS_LAST,
             0}};
        static const uint32_t composite[] = {0, 1, 2, 4, 3};
        assert(prdb_sdk_sql_order_row_indices_v3(
                   keys_v3, 2, 5, order, 5) == PRDB_SDK_SQL_ORDER_OK);
        assert(memcmp(order, composite, sizeof(composite)) == 0);
        secondary[1] = (prdb_sdk_value_v1){
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_REDACTED, 0, NULL, 0};
        uint32_t unchanged[5] = {31, 32, 33, 34, 35};
        assert(prdb_sdk_sql_order_row_indices_v3(
                   keys_v3, 2, 5, unchanged, 5) ==
               PRDB_SDK_SQL_EVAL_REDACTED_INPUT);
        assert(unchanged[0] == 31 && unchanged[4] == 35);
        keys_v3[1].values = NULL;
        assert(prdb_sdk_sql_order_row_indices_v3(
                   keys_v3, 2, 0, NULL, 0) == PRDB_SDK_SQL_ORDER_OK);
    }

    {
        uint8_t bytes[PRDB_SDK_SQL_ORDER_MAX_KEYS][5][4];
        prdb_sdk_value_v1 values[PRDB_SDK_SQL_ORDER_MAX_KEYS][5];
        prdb_sdk_sql_order_key_v3 keys_v3[PRDB_SDK_SQL_ORDER_MAX_KEYS];
        const int32_t last_key[] = {3, 1, 4, 0, 2};
        static const uint32_t expected[] = {3, 1, 4, 0, 2};
        for (uint32_t key = 0; key < PRDB_SDK_SQL_ORDER_MAX_KEYS; ++key) {
            keys_v3[key] = (prdb_sdk_sql_order_key_v3){
                values[key], PRDB_SDK_VALUE_TYPE_INTEGER, 0,
                PRDB_SDK_SQL_ORDER_ASCENDING, PRDB_SDK_SQL_ORDER_NULLS_FIRST,
                0};
            for (uint32_t row = 0; row < 5; ++row) {
                values[key][row] = integer_value(
                    bytes[key][row], key + 1 == PRDB_SDK_SQL_ORDER_MAX_KEYS
                                         ? last_key[row]
                                         : 7);
            }
        }
        assert(PRDB_SDK_SQL_ORDER_MAX_KEYS == 9);
        assert(prdb_sdk_sql_order_row_indices_v3(
                   keys_v3, PRDB_SDK_SQL_ORDER_MAX_KEYS, 5, order, 5) ==
               PRDB_SDK_SQL_ORDER_OK);
        assert(memcmp(order, expected, sizeof(expected)) == 0);
    }

    {
        prdb_sdk_value_v1 invalid_keys[2] = {keys[0], keys[1]};
        uint32_t unchanged[2] = {41, 42};
        uint8_t bigint_bytes[8] = {0};
        prdb_sdk_value_v1 bigint = {sizeof(bigint),
                                    PRDB_SDK_VALUE_ABI_V1_VERSION,
                                    PRDB_SDK_VALUE_STATE_VALUE,
                                    PRDB_SDK_VALUE_TYPE_BIGINT,
                                    bigint_bytes,
                                    sizeof(bigint_bytes)};
        invalid_keys[1] = bigint;
        assert(prdb_sdk_sql_order_row_indices_v1(
                   invalid_keys, 2, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0,
                   unchanged, 2) == PRDB_SDK_SQL_EVAL_TYPE_MISMATCH);
        assert(unchanged[0] == 41 && unchanged[1] == 42);

        invalid_keys[1] = (prdb_sdk_value_v1){
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_REDACTED, 0, NULL, 0};
        assert(prdb_sdk_sql_order_row_indices_v1(
                   invalid_keys, 2, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0,
                   unchanged, 2) == PRDB_SDK_SQL_EVAL_REDACTED_INPUT);
        assert(unchanged[0] == 41 && unchanged[1] == 42);

        assert(prdb_sdk_sql_order_row_indices_v1(
                   keys, 5, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, unchanged,
                   2) == PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT);
        assert(unchanged[0] == 41 && unchanged[1] == 42);
        assert(prdb_sdk_sql_order_row_indices_v1(
                   keys, 2, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 2, unchanged,
                   2) == PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT);
        assert(unchanged[0] == 41 && unchanged[1] == 42);
    }

    {
        prdb_sdk_value_v1 too_many[PRDB_SDK_SQL_ORDER_MAX_ROWS + 1] = {0};
        uint32_t too_many_order[PRDB_SDK_SQL_ORDER_MAX_ROWS + 1] = {0};
        assert(prdb_sdk_sql_order_row_indices_v1(
                   too_many, PRDB_SDK_SQL_ORDER_MAX_ROWS + 1,
                   PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, too_many_order,
                   PRDB_SDK_SQL_ORDER_MAX_ROWS + 1) ==
               PRDB_SDK_SQL_ORDER_LIMIT_EXCEEDED);
    }

    assert(prdb_sdk_sql_order_row_indices_v1(
               NULL, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, NULL, 0) ==
           PRDB_SDK_SQL_ORDER_OK);
    return 0;
}
