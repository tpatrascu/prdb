#include "prdb_sdk_sql_v1.h"
#include "prdb_sdk_value_v1.h"

#include <assert.h>
#include <stddef.h>

static prdb_sdk_value_v1 value(uint32_t state, uint32_t type,
                               const uint8_t *data, uint64_t length) {
    prdb_sdk_value_v1 result = {
        sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
        state, type, data, length};
    return result;
}

int main(void) {
    static const uint8_t positive_integer[] = {0, 0, 0, 2};
    static const uint8_t negative_integer[] = {0xff, 0xff, 0xff, 0xfd};
    static const uint8_t other_type[] = {0, 0, 0, 2, 0, 0, 0, 0};
    static const uint8_t decimal_coefficient[] = {
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12};
    static const uint8_t alpha[] = {'a'};
    static const uint8_t beta[] = {'b'};
    static const uint32_t truth_values[] = {
        PRDB_SDK_SQL_TRUTH_UNKNOWN, PRDB_SDK_SQL_TRUTH_FALSE,
        PRDB_SDK_SQL_TRUTH_TRUE};
    static const uint32_t and_table[3][3] = {
        {PRDB_SDK_SQL_TRUTH_UNKNOWN, PRDB_SDK_SQL_TRUTH_FALSE,
         PRDB_SDK_SQL_TRUTH_UNKNOWN},
        {PRDB_SDK_SQL_TRUTH_FALSE, PRDB_SDK_SQL_TRUTH_FALSE,
         PRDB_SDK_SQL_TRUTH_FALSE},
        {PRDB_SDK_SQL_TRUTH_UNKNOWN, PRDB_SDK_SQL_TRUTH_FALSE,
         PRDB_SDK_SQL_TRUTH_TRUE}};
    static const uint32_t or_table[3][3] = {
        {PRDB_SDK_SQL_TRUTH_UNKNOWN, PRDB_SDK_SQL_TRUTH_UNKNOWN,
         PRDB_SDK_SQL_TRUTH_TRUE},
        {PRDB_SDK_SQL_TRUTH_UNKNOWN, PRDB_SDK_SQL_TRUTH_FALSE,
         PRDB_SDK_SQL_TRUTH_TRUE},
        {PRDB_SDK_SQL_TRUTH_TRUE, PRDB_SDK_SQL_TRUTH_TRUE,
         PRDB_SDK_SQL_TRUTH_TRUE}};
    static const uint32_t not_table[] = {
        PRDB_SDK_SQL_TRUTH_UNKNOWN, PRDB_SDK_SQL_TRUTH_TRUE,
        PRDB_SDK_SQL_TRUTH_FALSE};
    static const uint32_t comparisons[] = {
        PRDB_SDK_SQL_COMPARE_EQUAL, PRDB_SDK_SQL_COMPARE_NOT_EQUAL,
        PRDB_SDK_SQL_COMPARE_LESS, PRDB_SDK_SQL_COMPARE_LESS_EQUAL,
        PRDB_SDK_SQL_COMPARE_GREATER, PRDB_SDK_SQL_COMPARE_GREATER_EQUAL};
    static const uint32_t two_greater_than_minus_three[] = {
        PRDB_SDK_SQL_TRUTH_FALSE, PRDB_SDK_SQL_TRUTH_TRUE,
        PRDB_SDK_SQL_TRUTH_FALSE, PRDB_SDK_SQL_TRUTH_FALSE,
        PRDB_SDK_SQL_TRUTH_TRUE, PRDB_SDK_SQL_TRUTH_TRUE};
    prdb_sdk_value_v1 two =
        value(PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
              positive_integer, sizeof(positive_integer));
    prdb_sdk_value_v1 minus_three =
        value(PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
              negative_integer, sizeof(negative_integer));
    prdb_sdk_value_v1 null_value =
        value(PRDB_SDK_VALUE_STATE_NULL, 0, 0, 0);
    prdb_sdk_value_v1 redacted =
        value(PRDB_SDK_VALUE_STATE_REDACTED, 0, 0, 0);
    prdb_sdk_value_v1 bigint =
        value(PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BIGINT,
              other_type, sizeof(other_type));
    prdb_sdk_value_v1 decimal =
        value(PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_DECIMAL,
              decimal_coefficient, sizeof(decimal_coefficient));
    prdb_sdk_value_v1 text_a =
        value(PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_TEXT, alpha,
              sizeof(alpha));
    prdb_sdk_value_v1 text_b =
        value(PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_TEXT, beta,
              sizeof(beta));
    uint32_t result = 99;

    assert(prdb_sdk_sql_compare_v1(&two, &minus_three,
                                   PRDB_SDK_SQL_COMPARE_GREATER, 0, 0,
                                   &result) == PRDB_SDK_SQL_EVAL_OK);
    assert(result == PRDB_SDK_SQL_TRUTH_TRUE);
    for (size_t index = 0; index < 6; ++index) {
        assert(prdb_sdk_sql_compare_v1(&two, &minus_three,
                                       comparisons[index], 0, 0,
                                       &result) == PRDB_SDK_SQL_EVAL_OK);
        assert(result == two_greater_than_minus_three[index]);
    }
    assert(prdb_sdk_sql_compare_v1(&two, &minus_three, 99, 0, 0, &result) ==
           PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT);
    assert(prdb_sdk_sql_compare_v1(&two, &null_value,
                                   PRDB_SDK_SQL_COMPARE_EQUAL, 0, 0,
                                   &result) == PRDB_SDK_SQL_EVAL_OK);
    assert(result == PRDB_SDK_SQL_TRUTH_UNKNOWN);
    assert(prdb_sdk_sql_compare_v1(&two, &redacted,
                                   PRDB_SDK_SQL_COMPARE_EQUAL, 0, 0,
                                   &result) ==
           PRDB_SDK_SQL_EVAL_REDACTED_INPUT);
    assert(prdb_sdk_sql_compare_v1(&two, &bigint,
                                   PRDB_SDK_SQL_COMPARE_EQUAL, 0, 0,
                                   &result) ==
           PRDB_SDK_SQL_EVAL_TYPE_MISMATCH);
    assert(prdb_sdk_sql_compare_v1(&text_a, &text_b,
                                   PRDB_SDK_SQL_COMPARE_LESS, 0, 0,
                                   &result) == PRDB_SDK_SQL_EVAL_OK);
    assert(result == PRDB_SDK_SQL_TRUTH_TRUE);
    assert(prdb_sdk_sql_compare_v1(&decimal, &decimal,
                                   PRDB_SDK_SQL_COMPARE_EQUAL, 2, 3,
                                   &result) ==
           PRDB_SDK_SQL_EVAL_TYPE_MISMATCH);
    assert(prdb_sdk_sql_compare_v1(&decimal, &decimal,
                                   PRDB_SDK_SQL_COMPARE_EQUAL, 2, 2,
                                   &result) == PRDB_SDK_SQL_EVAL_OK);
    assert(result == PRDB_SDK_SQL_TRUTH_TRUE);

    assert(prdb_sdk_sql_value_is_null_v1(&null_value, &result) ==
           PRDB_SDK_SQL_EVAL_OK);
    assert(result == PRDB_SDK_SQL_TRUTH_TRUE);
    assert(prdb_sdk_sql_value_is_null_v1(&two, &result) ==
           PRDB_SDK_SQL_EVAL_OK);
    assert(result == PRDB_SDK_SQL_TRUTH_FALSE);
    assert(prdb_sdk_sql_value_is_null_v1(&redacted, &result) ==
           PRDB_SDK_SQL_EVAL_REDACTED_INPUT);

    assert(prdb_sdk_sql_truth_and_v1(PRDB_SDK_SQL_TRUTH_UNKNOWN,
                                     PRDB_SDK_SQL_TRUTH_FALSE, &result) ==
           PRDB_SDK_SQL_EVAL_OK);
    assert(result == PRDB_SDK_SQL_TRUTH_FALSE);
    assert(prdb_sdk_sql_truth_or_v1(PRDB_SDK_SQL_TRUTH_UNKNOWN,
                                    PRDB_SDK_SQL_TRUTH_TRUE, &result) ==
           PRDB_SDK_SQL_EVAL_OK);
    assert(result == PRDB_SDK_SQL_TRUTH_TRUE);
    assert(prdb_sdk_sql_truth_not_v1(PRDB_SDK_SQL_TRUTH_UNKNOWN, &result) ==
           PRDB_SDK_SQL_EVAL_OK);
    assert(result == PRDB_SDK_SQL_TRUTH_UNKNOWN);
    assert(prdb_sdk_sql_truth_and_v1(99, PRDB_SDK_SQL_TRUTH_TRUE, &result) ==
           PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT);
    for (size_t left = 0; left < 3; ++left) {
        assert(prdb_sdk_sql_truth_not_v1(truth_values[left], &result) ==
               PRDB_SDK_SQL_EVAL_OK);
        assert(result == not_table[left]);
        for (size_t right = 0; right < 3; ++right) {
            assert(prdb_sdk_sql_truth_and_v1(truth_values[left],
                                            truth_values[right], &result) ==
                   PRDB_SDK_SQL_EVAL_OK);
            assert(result == and_table[left][right]);
            assert(prdb_sdk_sql_truth_or_v1(truth_values[left],
                                           truth_values[right], &result) ==
                   PRDB_SDK_SQL_EVAL_OK);
            assert(result == or_table[left][right]);
        }
    }
    return 0;
}
