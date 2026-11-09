#include "prdb_sdk_sql_v14.h"

#include <assert.h>
#include <string.h>

int main(void) {
    static const uint8_t relation[] = "records";
    static const uint8_t key_a[] = "a", key_b[] = "b";
    static const uint8_t sum_c[] = "c";
    static const uint8_t sql[] =
        "SELECT a, b, SUM(c) FROM records WHERE a > $1 "
        "GROUP BY a, b HAVING SUM(c) >= $2 LIMIT 4";
    static const uint8_t bad_aggregate[] =
        "SELECT a, b, COUNT(*) FROM records GROUP BY a, b "
        "HAVING SUM(c) > $1";
    static const uint8_t misplaced_having[] =
        "SELECT a, b, COUNT(*) FROM records HAVING COUNT(*) > $1 "
        "GROUP BY a, b";
    prdb_sdk_sql_column_shape_v2 columns[3] = {
        {key_a, 1, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {key_b, 1, 1, PRDB_SDK_VALUE_TYPE_TEXT, 0, 0},
        {sum_c, 1, 2, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_select_v14 statement = {0};
    prdb_sdk_select_binding_v14 binding = {0};
    uint32_t error_offset = 0;

    assert(prdb_sdk_sql_parse_select_v14(sql, sizeof(sql) - 1, &statement,
                                         sizeof(statement), &error_offset) ==
           PRDB_SDK_SQL_OK);
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V14_VERSION);
    assert(statement.base.group_key_count == 2);
    assert(statement.base.base.base.predicate_count == 1);
    assert(statement.base.base.base.has_limit == 1);
    assert(statement.has_having == 1);
    assert(statement.having_comparison == PRDB_SDK_SQL_COMPARE_GREATER_EQUAL);
    assert(statement.having_parameter_index == 2);
    assert(prdb_sdk_sql_bind_select_v14(
               &statement, relation, sizeof(relation) - 1, columns, 3,
               &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.base.group_key_count == 2);
    assert(binding.having_type_code == PRDB_SDK_VALUE_TYPE_BIGINT);
    assert(binding.having_parameter_index == 2);

    assert(prdb_sdk_sql_parse_select_v14(
               bad_aggregate, sizeof(bad_aggregate) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(prdb_sdk_sql_parse_select_v14(
               misplaced_having, sizeof(misplaced_having) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(prdb_sdk_sql_parse_select_v14(
               (const uint8_t *)"SELECT a, b, COUNT(*) FROM records "
                                "GROUP BY a, b LIMIT 2",
               sizeof("SELECT a, b, COUNT(*) FROM records GROUP BY a, b LIMIT 2") - 1,
               &statement, sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.has_having == 0);
    assert(statement.base.base.base.has_limit == 1);
    statement.having_parameter_index = 1;
    prdb_sdk_select_binding_v14 previous = binding;
    assert(prdb_sdk_sql_bind_select_v14(
               &statement, relation, sizeof(relation) - 1, columns, 3,
               &binding, sizeof(binding)) ==
           PRDB_SDK_SQL_BIND_INVALID_STATEMENT);
    assert(memcmp(&binding, &previous, sizeof(binding)) == 0);
    return 0;
}
