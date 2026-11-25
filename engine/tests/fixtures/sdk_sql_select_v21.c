#include "prdb_sdk_sql_v1.h"

#include <assert.h>
#include <string.h>

static int coefficient_equals(const uint8_t actual[16], int64_t expected) {
    uint8_t encoded[16];
    uint64_t bits = (uint64_t)expected;
    memset(encoded, expected < 0 ? 0xff : 0, sizeof(encoded));
    for (uint32_t index = 0; index < 8; ++index) {
        encoded[15 - index] = (uint8_t)bits;
        bits >>= 8;
    }
    return memcmp(actual, encoded, sizeof(encoded)) == 0;
}

int main(void) {
    static const uint8_t relation[] = "records";
    static const uint8_t a[] = "a", b[] = "b", c[] = "c";
    static const uint8_t decimal_sql[] =
        "SELECT a, b, SUM(c) FROM records GROUP BY a, b "
        "HAVING (SUM(c) >= 1.25 OR SUM(c) = 1.2e0) AND "
        "NOT SUM(c) < - 1.20e1 ORDER BY a LIMIT 4";
    static const uint8_t exact_integer_sql[] =
        "SELECT a, b, SUM(c) FROM records GROUP BY a, b "
        "HAVING SUM(c) >= 1.000e0";
    static const uint8_t bigint_min_sql[] =
        "SELECT a, b, SUM(c) FROM records GROUP BY a, b "
        "HAVING SUM(c) >= -9223372036854775808";
    static const uint8_t bigint_max_sql[] =
        "SELECT a, b, SUM(c) FROM records GROUP BY a, b "
        "HAVING SUM(c) <= 9223372036854775807";
    static const uint8_t bigint_positive_overflow_sql[] =
        "SELECT a, b, SUM(c) FROM records GROUP BY a, b "
        "HAVING SUM(c) >= 9223372036854775808";
    static const uint8_t bigint_negative_overflow_sql[] =
        "SELECT a, b, SUM(c) FROM records GROUP BY a, b "
        "HAVING SUM(c) >= -9223372036854775809";
    static const uint8_t inexact_integer_sql[] =
        "SELECT a, b, SUM(c) FROM records GROUP BY a, b "
        "HAVING SUM(c) >= 1.25";
    static const uint8_t inexact_decimal_sql[] =
        "SELECT a, b, SUM(c) FROM records GROUP BY a, b "
        "HAVING SUM(c) >= 1.251";
    static const uint8_t decimal_overflow_sql[] =
        "SELECT a, b, SUM(c) FROM records GROUP BY a, b "
        "HAVING SUM(c) >= 1e36";
    static const uint8_t decimal_underflow_sql[] =
        "SELECT a, b, SUM(c) FROM records GROUP BY a, b "
        "HAVING SUM(c) >= 1e-100";
    static const uint8_t malformed_exponent_sql[] =
        "SELECT a, b, SUM(c) FROM records GROUP BY a, b "
        "HAVING SUM(c) >= 1e";
    prdb_sdk_sql_column_shape_v1 decimal_columns[3] = {
        {a, 1, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {b, 1, 1, PRDB_SDK_VALUE_TYPE_TEXT, 0, 0},
        {c, 1, 2, PRDB_SDK_VALUE_TYPE_DECIMAL, 10, 2}};
    prdb_sdk_sql_column_shape_v1 integer_columns[3] = {
        {a, 1, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {b, 1, 1, PRDB_SDK_VALUE_TYPE_TEXT, 0, 0},
        {c, 1, 2, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_select_v1 statement = {0};
    prdb_sdk_select_binding_v1 binding = {0};
    prdb_sdk_select_v1 tampered_statement;
    uint32_t error_offset = 0;

    assert(prdb_sdk_sql_parse_select_v1(
               decimal_sql, sizeof(decimal_sql) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V1_VERSION);
    assert(statement.having_node_count == 6);
    assert(statement.having_nodes[0].operand_kind ==
           PRDB_SDK_SQL_HAVING_OPERAND_NUMERIC_LITERAL);
    assert(statement.having_nodes[0].scale == 2);
    assert(coefficient_equals(statement.having_nodes[0].coefficient, 125));
    assert(statement.having_nodes[1].scale == 1);
    assert(coefficient_equals(statement.having_nodes[1].coefficient, 12));
    assert(statement.having_nodes[3].scale == 0);
    assert(coefficient_equals(statement.having_nodes[3].coefficient, -12));
    assert(prdb_sdk_sql_bind_select_v1(
               &statement, relation, sizeof(relation) - 1, decimal_columns, 3,
               &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.having_type_code == PRDB_SDK_VALUE_TYPE_DECIMAL);
    assert(binding.having_nodes[0].scale == 2);
    assert(coefficient_equals(binding.having_nodes[0].coefficient, 125));
    assert(binding.having_nodes[1].scale == 2);
    assert(coefficient_equals(binding.having_nodes[1].coefficient, 120));
    assert(binding.having_nodes[3].scale == 2);
    assert(coefficient_equals(binding.having_nodes[3].coefficient, -1200));
    tampered_statement = statement;
    tampered_statement.having_nodes[0].scale = 39;
    assert(prdb_sdk_sql_bind_select_v1(
               &tampered_statement, relation, sizeof(relation) - 1,
               decimal_columns, 3, &binding, sizeof(binding)) ==
           PRDB_SDK_SQL_BIND_INVALID_STATEMENT);

    assert(prdb_sdk_sql_parse_select_v1(
               exact_integer_sql, sizeof(exact_integer_sql) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_select_v1(
               &statement, relation, sizeof(relation) - 1, integer_columns, 3,
               &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.having_type_code == PRDB_SDK_VALUE_TYPE_BIGINT);
    assert(coefficient_equals(binding.having_nodes[0].coefficient, 1));
    assert(binding.having_nodes[0].scale == 0);

    assert(prdb_sdk_sql_parse_select_v1(
               bigint_min_sql, sizeof(bigint_min_sql) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(coefficient_equals(statement.having_nodes[0].coefficient,
                              INT64_MIN));
    assert(prdb_sdk_sql_bind_select_v1(
               &statement, relation, sizeof(relation) - 1, integer_columns, 3,
               &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(coefficient_equals(binding.having_nodes[0].coefficient,
                              INT64_MIN));
    assert(prdb_sdk_sql_parse_select_v1(
               bigint_max_sql, sizeof(bigint_max_sql) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_select_v1(
               &statement, relation, sizeof(relation) - 1, integer_columns, 3,
               &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(coefficient_equals(binding.having_nodes[0].coefficient,
                              INT64_MAX));
    assert(prdb_sdk_sql_parse_select_v1(
               bigint_positive_overflow_sql,
               sizeof(bigint_positive_overflow_sql) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_select_v1(
               &statement, relation, sizeof(relation) - 1, integer_columns, 3,
               &binding, sizeof(binding)) ==
           PRDB_SDK_SQL_BIND_LITERAL_TYPE_MISMATCH);
    assert(prdb_sdk_sql_parse_select_v1(
               bigint_negative_overflow_sql,
               sizeof(bigint_negative_overflow_sql) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_select_v1(
               &statement, relation, sizeof(relation) - 1, integer_columns, 3,
               &binding, sizeof(binding)) ==
           PRDB_SDK_SQL_BIND_LITERAL_TYPE_MISMATCH);

    assert(prdb_sdk_sql_parse_select_v1(
               inexact_integer_sql, sizeof(inexact_integer_sql) - 1,
               &statement, sizeof(statement), &error_offset) ==
           PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_select_v1(
               &statement, relation, sizeof(relation) - 1, integer_columns, 3,
               &binding, sizeof(binding)) ==
           PRDB_SDK_SQL_BIND_LITERAL_TYPE_MISMATCH);
    assert(prdb_sdk_sql_parse_select_v1(
               inexact_decimal_sql, sizeof(inexact_decimal_sql) - 1,
               &statement, sizeof(statement), &error_offset) ==
           PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_select_v1(
               &statement, relation, sizeof(relation) - 1, decimal_columns, 3,
               &binding, sizeof(binding)) ==
           PRDB_SDK_SQL_BIND_LITERAL_TYPE_MISMATCH);
    assert(prdb_sdk_sql_parse_select_v1(
               decimal_overflow_sql, sizeof(decimal_overflow_sql) - 1,
               &statement, sizeof(statement), &error_offset) ==
           PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_select_v1(
               &statement, relation, sizeof(relation) - 1, decimal_columns, 3,
               &binding, sizeof(binding)) ==
           PRDB_SDK_SQL_BIND_LITERAL_TYPE_MISMATCH);
    assert(prdb_sdk_sql_parse_select_v1(
               decimal_underflow_sql, sizeof(decimal_underflow_sql) - 1,
               &statement, sizeof(statement), &error_offset) ==
           PRDB_SDK_SQL_LIMIT_EXCEEDED);
    assert(prdb_sdk_sql_parse_select_v1(
               malformed_exponent_sql, sizeof(malformed_exponent_sql) - 1,
               &statement, sizeof(statement), &error_offset) ==
           PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(error_offset != 0);
    return 0;
}
