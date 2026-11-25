#include "prdb_sdk_sql_v1.h"
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
    prdb_sdk_sql_column_shape_v1 integer_columns[3] = {
        {a, 1, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {b, 1, 1, PRDB_SDK_VALUE_TYPE_TEXT, 0, 0},
        {c, 1, 2, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_select_v1 statement = {0};
    prdb_sdk_select_binding_v1 binding = {0};
    prdb_sdk_select_binding_v1 previous;
    uint32_t error_offset = 0;

    assert(prdb_sdk_sql_parse_select_v1(
               literal_sql, sizeof(literal_sql) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V1_VERSION);
    assert(statement.having_node_count == 1);
    assert(statement.having_nodes[0].operand_kind ==
           PRDB_SDK_SQL_HAVING_OPERAND_NUMERIC_LITERAL);
    assert(statement.having_nodes[0].scale == 0);
    assert(statement.order_count == 1);
    assert(prdb_sdk_sql_bind_select_v1(
               &statement, relation, sizeof(relation) - 1, integer_columns, 3,
               &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.having_nodes[0].operand_kind ==
           PRDB_SDK_SQL_HAVING_OPERAND_NUMERIC_LITERAL);
    assert(binding.having_nodes[0].scale == 0);
    assert(binding.having_type_code ==
           PRDB_SDK_VALUE_TYPE_BIGINT);

    assert(prdb_sdk_sql_parse_select_v1(
               parameter_sql, sizeof(parameter_sql) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.has_having_literal == 0);
    assert(statement.having_nodes[0].parameter_index == 2);
    assert(prdb_sdk_sql_bind_select_v1(
               &statement, relation, sizeof(relation) - 1, integer_columns, 3,
               &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.has_having_literal == 0);

    assert(prdb_sdk_sql_parse_select_v1(
               literal_sql, sizeof(literal_sql) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    memset(&binding, 0xA5, sizeof(binding));
    previous = binding;
    statement.having_nodes[0].scale = 39;
    assert(prdb_sdk_sql_bind_select_v1(
        &statement, relation, sizeof(relation) - 1, integer_columns, 3,
        &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_INVALID_STATEMENT);
    assert(memcmp(&binding, &previous, sizeof(binding)) == 0);
    return 0;
}
