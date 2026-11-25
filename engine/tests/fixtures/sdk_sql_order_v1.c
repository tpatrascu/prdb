#include "prdb_sdk_sql_order_v1.h"

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


static int32_t test_order_v2(
    const prdb_sdk_value_v1 *keys, uint32_t key_count,
    uint32_t declared_type, uint8_t decimal_scale, uint32_t direction,
    uint32_t null_order, uint32_t *row_indices_out,
    uint32_t row_indices_capacity) {
    prdb_sdk_sql_order_key_v1 key = {
        keys, declared_type, decimal_scale, (uint8_t)direction,
        (uint8_t)null_order, 0};
    return prdb_sdk_sql_order_row_indices_v1(&key, 1, key_count,
                                             row_indices_out,
                                             row_indices_capacity);
}

static int32_t test_order_v1(
    const prdb_sdk_value_v1 *keys, uint32_t key_count,
    uint32_t declared_type, uint8_t decimal_scale, uint32_t direction,
    uint32_t *row_indices_out, uint32_t row_indices_capacity) {
    return test_order_v2(keys, key_count, declared_type, decimal_scale,
                         direction, PRDB_SDK_SQL_ORDER_NULLS_FIRST,
                         row_indices_out, row_indices_capacity);
}

static void assert_scalar_order(uint32_t type, uint8_t decimal_scale,
                                const uint8_t *lower, uint32_t lower_len,
                                const uint8_t *higher, uint32_t higher_len) {
    prdb_sdk_value_v1 values[2] = {
        {sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
         PRDB_SDK_VALUE_STATE_VALUE, type, (uint8_t *)higher, higher_len},
        {sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
         PRDB_SDK_VALUE_STATE_VALUE, type, (uint8_t *)lower, lower_len}};
    uint32_t order[2] = {UINT32_MAX, UINT32_MAX};
    static const uint32_t ascending[] = {1, 0};
    static const uint32_t descending[] = {0, 1};

    assert(test_order_v1(
               values, 2, type, decimal_scale, 0, order, 2) ==
           PRDB_SDK_SQL_ORDER_OK);
    assert(memcmp(order, ascending, sizeof(ascending)) == 0);
    assert(test_order_v1(
               values, 2, type, decimal_scale, 1, order, 2) ==
           PRDB_SDK_SQL_ORDER_OK);
    assert(memcmp(order, descending, sizeof(descending)) == 0);
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
    static const uint8_t boolean_lower[] = {0};
    static const uint8_t boolean_higher[] = {1};
    static const uint8_t smallint_lower[] = {0x80, 0};
    static const uint8_t smallint_higher[] = {0x7F, 0xFF};
    static const uint8_t integer_lower[] = {0x80, 0, 0, 0};
    static const uint8_t integer_higher[] = {0x7F, 0xFF, 0xFF, 0xFF};
    static const uint8_t bigint_lower[] = {0x80, 0, 0, 0, 0, 0, 0, 0};
    static const uint8_t bigint_higher[] = {0x7F, 0xFF, 0xFF, 0xFF,
                                             0xFF, 0xFF, 0xFF, 0xFF};
    static const uint8_t decimal_lower[] = {0xFF, 0xFF, 0xFF, 0xFF,
                                             0xFF, 0xFF, 0xFF, 0xFF,
                                             0xFF, 0xFF, 0xFF, 0xFF,
                                             0xFF, 0xFF, 0xFF, 0x9C};
    static const uint8_t decimal_higher[] = {0, 0, 0, 0, 0, 0, 0, 0,
                                              0, 0, 0, 0, 0, 0, 0, 0x64};
    static const uint8_t text_lower[] = {'a'};
    static const uint8_t text_higher[] = {'b'};
    static const uint8_t varchar_lower[] = {0xC3, 0xA9};
    static const uint8_t varchar_higher[] = {0xE2, 0x82, 0xAC};
    static const uint8_t bytea_lower[] = {0, 0xFF};
    static const uint8_t bytea_higher[] = {1, 0};
    static const uint8_t uuid_lower[] = {0, 1, 2, 3, 4, 5, 6, 7,
                                          8, 9, 10, 11, 12, 13, 14, 15};
    static const uint8_t uuid_higher[] = {0, 1, 2, 3, 4, 5, 6, 7,
                                           8, 9, 10, 11, 12, 13, 14, 16};
    static const uint8_t date_lower[] = {0xFF, 0xFF, 0xFF, 0xFF};
    static const uint8_t date_higher[] = {0, 0, 0, 0};
    static const uint8_t timestamp_lower[] = {0xFF, 0xFF, 0xFF, 0xFF,
                                               0xFF, 0xFF, 0xFF, 0xFF};
    static const uint8_t timestamp_higher[] = {0, 0, 0, 0, 0, 0, 0, 0};

    assert_scalar_order(PRDB_SDK_VALUE_TYPE_BOOLEAN, 0, boolean_lower,
                        sizeof(boolean_lower), boolean_higher,
                        sizeof(boolean_higher));
    assert_scalar_order(PRDB_SDK_VALUE_TYPE_SMALLINT, 0, smallint_lower,
                        sizeof(smallint_lower), smallint_higher,
                        sizeof(smallint_higher));
    assert_scalar_order(PRDB_SDK_VALUE_TYPE_INTEGER, 0, integer_lower,
                        sizeof(integer_lower), integer_higher,
                        sizeof(integer_higher));
    assert_scalar_order(PRDB_SDK_VALUE_TYPE_BIGINT, 0, bigint_lower,
                        sizeof(bigint_lower), bigint_higher,
                        sizeof(bigint_higher));
    assert_scalar_order(PRDB_SDK_VALUE_TYPE_DECIMAL, 2, decimal_lower,
                        sizeof(decimal_lower), decimal_higher,
                        sizeof(decimal_higher));
    assert_scalar_order(PRDB_SDK_VALUE_TYPE_TEXT, 0, text_lower,
                        sizeof(text_lower), text_higher,
                        sizeof(text_higher));
    assert_scalar_order(PRDB_SDK_VALUE_TYPE_VARCHAR, 0, varchar_lower,
                        sizeof(varchar_lower), varchar_higher,
                        sizeof(varchar_higher));
    assert_scalar_order(PRDB_SDK_VALUE_TYPE_BYTEA, 0, bytea_lower,
                        sizeof(bytea_lower), bytea_higher,
                        sizeof(bytea_higher));
    assert_scalar_order(PRDB_SDK_VALUE_TYPE_UUID, 0, uuid_lower,
                        sizeof(uuid_lower), uuid_higher,
                        sizeof(uuid_higher));
    assert_scalar_order(PRDB_SDK_VALUE_TYPE_DATE, 0, date_lower,
                        sizeof(date_lower), date_higher,
                        sizeof(date_higher));
    assert_scalar_order(PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ, 0, timestamp_lower,
                        sizeof(timestamp_lower), timestamp_higher,
                        sizeof(timestamp_higher));

    uint8_t data[4][4];
    prdb_sdk_value_v1 keys[] = {
        integer_value(data[0], 4), integer_value(data[1], 1), null_value(),
        integer_value(data[2], 2), integer_value(data[3], 1)};
    uint32_t order[5] = {99, 99, 99, 99, 99};
    static const uint32_t ascending[] = {2, 1, 4, 3, 0};
    static const uint32_t descending[] = {2, 0, 3, 1, 4};
    static const uint32_t nulls_last[] = {1, 4, 3, 0, 2};
    static const uint32_t descending_nulls_last[] = {0, 3, 1, 4, 2};

    assert(test_order_v1(
               keys, 5, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, order, 5) ==
           PRDB_SDK_SQL_ORDER_OK);
    assert(memcmp(order, ascending, sizeof(ascending)) == 0);

    assert(test_order_v1(
               keys, 5, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 1, order, 5) ==
           PRDB_SDK_SQL_ORDER_OK);
    assert(memcmp(order, descending, sizeof(descending)) == 0);

    assert(test_order_v2(
               keys, 5, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0,
               PRDB_SDK_SQL_ORDER_NULLS_LAST, order, 5) ==
           PRDB_SDK_SQL_ORDER_OK);
    assert(memcmp(order, nulls_last, sizeof(nulls_last)) == 0);
    assert(test_order_v2(
               keys, 5, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 1,
               PRDB_SDK_SQL_ORDER_NULLS_LAST, order, 5) ==
           PRDB_SDK_SQL_ORDER_OK);
    assert(memcmp(order, descending_nulls_last,
                  sizeof(descending_nulls_last)) == 0);

    {
        uint8_t row_bytes[PRDB_SDK_SQL_ORDER_MAX_ROWS][4];
        prdb_sdk_value_v1 row_keys[PRDB_SDK_SQL_ORDER_MAX_ROWS];
        uint32_t row_order[PRDB_SDK_SQL_ORDER_MAX_ROWS];
        uint8_t seen[PRDB_SDK_SQL_ORDER_MAX_ROWS] = {0};
        for (uint32_t row = 0; row < PRDB_SDK_SQL_ORDER_MAX_ROWS; ++row) {
            row_keys[row] = integer_value(
                row_bytes[row], (int32_t)((PRDB_SDK_SQL_ORDER_MAX_ROWS - row) % 19));
        }
        assert(test_order_v1(
                   row_keys, PRDB_SDK_SQL_ORDER_MAX_ROWS,
                   PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, row_order,
                   PRDB_SDK_SQL_ORDER_MAX_ROWS) == PRDB_SDK_SQL_ORDER_OK);
        for (uint32_t position = 0;
             position < PRDB_SDK_SQL_ORDER_MAX_ROWS; ++position) {
            uint32_t row = row_order[position];
            assert(row < PRDB_SDK_SQL_ORDER_MAX_ROWS && seen[row] == 0);
            seen[row] = 1;
            if (position != 0) {
                uint32_t previous = row_order[position - 1];
                int32_t previous_value = (int32_t)(
                    (PRDB_SDK_SQL_ORDER_MAX_ROWS - previous) % 19);
                int32_t current_value = (int32_t)(
                    (PRDB_SDK_SQL_ORDER_MAX_ROWS - row) % 19);
                assert(previous_value <= current_value);
                if (previous_value == current_value) assert(previous < row);
            }
        }
        memset(seen, 0, sizeof(seen));
        assert(test_order_v1(
                   row_keys, PRDB_SDK_SQL_ORDER_MAX_ROWS,
                   PRDB_SDK_VALUE_TYPE_INTEGER, 0, 1, row_order,
                   PRDB_SDK_SQL_ORDER_MAX_ROWS) == PRDB_SDK_SQL_ORDER_OK);
        for (uint32_t position = 0;
             position < PRDB_SDK_SQL_ORDER_MAX_ROWS; ++position) {
            uint32_t row = row_order[position];
            assert(row < PRDB_SDK_SQL_ORDER_MAX_ROWS && seen[row] == 0);
            seen[row] = 1;
            if (position != 0) {
                uint32_t previous = row_order[position - 1];
                int32_t previous_value = (int32_t)(
                    (PRDB_SDK_SQL_ORDER_MAX_ROWS - previous) % 19);
                int32_t current_value = (int32_t)(
                    (PRDB_SDK_SQL_ORDER_MAX_ROWS - row) % 19);
                assert(previous_value >= current_value);
                if (previous_value == current_value) assert(previous < row);
            }
        }
    }

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
        prdb_sdk_sql_order_key_v1 keys_v3[2] = {
            {primary, PRDB_SDK_VALUE_TYPE_INTEGER, 0,
             PRDB_SDK_SQL_ORDER_ASCENDING, PRDB_SDK_SQL_ORDER_NULLS_FIRST,
             0},
            {secondary, PRDB_SDK_VALUE_TYPE_INTEGER, 0,
             PRDB_SDK_SQL_ORDER_DESCENDING, PRDB_SDK_SQL_ORDER_NULLS_LAST,
             0}};
        static const uint32_t composite[] = {0, 1, 2, 4, 3};
        assert(prdb_sdk_sql_order_row_indices_v1(
                   keys_v3, 2, 5, order, 5) == PRDB_SDK_SQL_ORDER_OK);
        assert(memcmp(order, composite, sizeof(composite)) == 0);
        secondary[1] = (prdb_sdk_value_v1){
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_REDACTED, 0, NULL, 0};
        uint32_t unchanged[5] = {31, 32, 33, 34, 35};
        assert(prdb_sdk_sql_order_row_indices_v1(
                   keys_v3, 2, 5, unchanged, 5) ==
               PRDB_SDK_SQL_EVAL_REDACTED_INPUT);
        assert(unchanged[0] == 31 && unchanged[4] == 35);
        keys_v3[1].values = NULL;
        assert(prdb_sdk_sql_order_row_indices_v1(
                   keys_v3, 2, 0, NULL, 0) == PRDB_SDK_SQL_ORDER_OK);
    }

    {
        uint8_t bytes[PRDB_SDK_SQL_ORDER_MAX_KEYS][5][4];
        prdb_sdk_value_v1 values[PRDB_SDK_SQL_ORDER_MAX_KEYS][5];
        prdb_sdk_sql_order_key_v1 keys_v3[PRDB_SDK_SQL_ORDER_MAX_KEYS];
        const int32_t last_key[] = {3, 1, 4, 0, 2};
        static const uint32_t expected[] = {3, 1, 4, 0, 2};
        for (uint32_t key = 0; key < PRDB_SDK_SQL_ORDER_MAX_KEYS; ++key) {
            keys_v3[key] = (prdb_sdk_sql_order_key_v1){
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
        assert(prdb_sdk_sql_order_row_indices_v1(
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
        assert(test_order_v1(
                   invalid_keys, 2, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0,
                   unchanged, 2) == PRDB_SDK_SQL_EVAL_TYPE_MISMATCH);
        assert(unchanged[0] == 41 && unchanged[1] == 42);

        invalid_keys[1] = (prdb_sdk_value_v1){
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_REDACTED, 0, NULL, 0};
        assert(test_order_v1(
                   invalid_keys, 2, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0,
                   unchanged, 2) == PRDB_SDK_SQL_EVAL_REDACTED_INPUT);
        assert(unchanged[0] == 41 && unchanged[1] == 42);

        assert(test_order_v1(
                   keys, 5, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, unchanged,
                   2) == PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT);
        assert(unchanged[0] == 41 && unchanged[1] == 42);
        assert(test_order_v1(
                   keys, 2, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 2, unchanged,
                   2) == PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT);
        assert(unchanged[0] == 41 && unchanged[1] == 42);
    }

    {
        prdb_sdk_value_v1 too_many[PRDB_SDK_SQL_ORDER_MAX_ROWS + 1] = {0};
        uint32_t too_many_order[PRDB_SDK_SQL_ORDER_MAX_ROWS + 1] = {0};
        assert(test_order_v1(
                   too_many, PRDB_SDK_SQL_ORDER_MAX_ROWS + 1,
                   PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, too_many_order,
                   PRDB_SDK_SQL_ORDER_MAX_ROWS + 1) ==
               PRDB_SDK_SQL_ORDER_LIMIT_EXCEEDED);
    }

    assert(test_order_v1(
               NULL, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, NULL, 0) ==
           PRDB_SDK_SQL_ORDER_OK);
    return 0;
}
