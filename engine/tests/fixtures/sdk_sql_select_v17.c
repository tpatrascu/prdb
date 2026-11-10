#include "prdb_sdk_sql_v17.h"
#include <assert.h>
#include <string.h>

int main(void) {
    static const uint8_t relation[] = "records";
    static const uint8_t a[] = "a", b[] = "b", c[] = "c";
    static const uint8_t literal_sql[] =
        "SELECT a, b, SUM(c) FROM records GROUP BY a, b "
        "HAVING SUM(c) >= 5 ORDER BY a LIMIT 4";
    static const uint8_t parameter_sql[] =
        "SELECT a, b, SUM(c) FROM records GROUP BY a, b "
        "HAVING SUM(c) >= $2 ORDER BY a LIMIT 4";
    static const uint8_t overflow_sql[] =
        "SELECT a, b, COUNT(*) FROM records GROUP BY a, b "
        "HAVING COUNT(*) > 9223372036854775808";
    static const uint8_t negative_sql[] =
        "SELECT a, b, COUNT(*) FROM records GROUP BY a, b "
        "HAVING COUNT(*) > -1";
    static const uint8_t mismatched_sql[] =
        "SELECT a, b, SUM(c) FROM records GROUP BY a, b "
        "HAVING COUNT(*) > 5";
    prdb_sdk_sql_column_shape_v2 integer_columns[3] = {
        {a, 1, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {b, 1, 1, PRDB_SDK_VALUE_TYPE_TEXT, 0, 0},
        {c, 1, 2, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_sql_column_shape_v2 decimal_columns[3] = {
        {a, 1, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {b, 1, 1, PRDB_SDK_VALUE_TYPE_TEXT, 0, 0},
        {c, 1, 2, PRDB_SDK_VALUE_TYPE_DECIMAL, 10, 2}};
    prdb_sdk_select_v17 statement = {0};
    prdb_sdk_select_binding_v17 binding = {0};
    prdb_sdk_select_binding_v17 previous;
    prdb_sdk_select_v15 old_statement = {0};
    uint32_t error_offset = 0;

    assert(prdb_sdk_sql_parse_select_v17(
               literal_sql, sizeof(literal_sql) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V17_VERSION);
    assert(statement.base.base.has_having == 1);
    assert(statement.has_having_literal == 1);
    assert(statement.having_literal == 5);
    assert(statement.base.order_count == 1);
    assert(prdb_sdk_sql_bind_select_v17(
               &statement, relation, sizeof(relation) - 1, integer_columns, 3,
               &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.has_having_literal == 1);
    assert(binding.having_literal == 5);
    assert(binding.base.base.having_type_code ==
           PRDB_SDK_VALUE_TYPE_BIGINT);

    assert(prdb_sdk_sql_parse_select_v17(
               parameter_sql, sizeof(parameter_sql) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.has_having_literal == 0);
    assert(statement.base.base.having_parameter_index == 2);
    assert(prdb_sdk_sql_bind_select_v17(
               &statement, relation, sizeof(relation) - 1, integer_columns, 3,
               &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.has_having_literal == 0);

    assert(prdb_sdk_sql_parse_select_v17(
               overflow_sql, sizeof(overflow_sql) - 1, &statement,
               sizeof(statement), &error_offset) ==
           PRDB_SDK_SQL_LIMIT_EXCEEDED);
    assert(error_offset != 0);
    assert(prdb_sdk_sql_parse_select_v17(
               negative_sql, sizeof(negative_sql) - 1, &statement,
               sizeof(statement), &error_offset) ==
           PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(prdb_sdk_sql_parse_select_v17(
               mismatched_sql, sizeof(mismatched_sql) - 1, &statement,
               sizeof(statement), &error_offset) ==
           PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(prdb_sdk_sql_parse_select_v15(
               literal_sql, sizeof(literal_sql) - 1, &old_statement,
               sizeof(old_statement), &error_offset) ==
           PRDB_SDK_SQL_SYNTAX_ERROR);

    assert(prdb_sdk_sql_parse_select_v17(
               literal_sql, sizeof(literal_sql) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    memset(&binding, 0xA5, sizeof(binding));
    previous = binding;
    assert(prdb_sdk_sql_bind_select_v17(
               &statement, relation, sizeof(relation) - 1, decimal_columns, 3,
               &binding, sizeof(binding)) ==
           PRDB_SDK_SQL_BIND_V17_LITERAL_TYPE_MISMATCH);
    assert(memcmp(&binding, &previous, sizeof(binding)) == 0);
    statement.having_literal = UINT64_C(0x8000000000000000);
    assert(prdb_sdk_sql_bind_select_v17(
        &statement, relation, sizeof(relation) - 1, integer_columns, 3,
        &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_INVALID_STATEMENT);
    assert(memcmp(&binding, &previous, sizeof(binding)) == 0);
    return 0;
}
