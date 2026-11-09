#include "prdb_sdk_sql_v15.h"
#include "prdb_sdk_sql_v14.h"

#include <assert.h>
#include <string.h>

int main(void) {
    static const uint8_t relation[] = "records";
    static const uint8_t a[] = "a", b[] = "b", c[] = "c";
    static const uint8_t sql[] =
        "SELECT a, b, SUM(c) FROM records GROUP BY a, b "
        "HAVING SUM(c) >= $1 ORDER BY b DESC NULLS LAST, "
        "SUM(c) ASC NULLS FIRST, a LIMIT 4";
    static const uint8_t duplicate[] =
        "SELECT a, b, COUNT(*) FROM records GROUP BY a, b ORDER BY a, a";
    static const uint8_t unselected[] =
        "SELECT a, b, COUNT(*) FROM records GROUP BY a, b ORDER BY c";
    static const uint8_t wrong_limit_order[] =
        "SELECT a, b, COUNT(*) FROM records GROUP BY a, b LIMIT 3 ORDER BY a";
    static const uint8_t eight_keys[] =
        "SELECT a, b, c, d, e, f, g, h, COUNT(*) FROM records "
        "GROUP BY a, b, c, d, e, f, g, h ORDER BY a, b, c, d, e, f, g, h, "
        "COUNT(*) DESC";
    static const uint8_t *names[8] = {a, b, c, (const uint8_t *)"d",
                                      (const uint8_t *)"e", (const uint8_t *)"f",
                                      (const uint8_t *)"g", (const uint8_t *)"h"};
    prdb_sdk_sql_column_shape_v2 columns[3] = {
        {a, 1, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {b, 1, 1, PRDB_SDK_VALUE_TYPE_TEXT, 0, 0},
        {c, 1, 2, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_sql_column_shape_v2 columns_eight[8];
    prdb_sdk_select_v15 statement = {0};
    prdb_sdk_select_binding_v15 binding = {0};
    prdb_sdk_select_v14 old_statement = {0};
    uint32_t error_offset = 0;

    for (uint32_t index = 0; index < 8; ++index) {
        columns_eight[index] = (prdb_sdk_sql_column_shape_v2){
            names[index], 1, index, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0};
    }
    assert(prdb_sdk_sql_parse_select_v15(sql, sizeof(sql) - 1, &statement,
                                         sizeof(statement), &error_offset) ==
           PRDB_SDK_SQL_OK);
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V15_VERSION);
    assert(statement.base.base.group_key_count == 2);
    assert(statement.base.has_having == 1);
    assert(statement.order_count == 3);
    assert(statement.order_items[0].kind == PRDB_SDK_SQL_V10_ORDER_GROUP_KEY);
    assert(statement.order_items[0].group_key_index == 1);
    assert(statement.order_items[0].direction ==
           PRDB_SDK_SQL_V10_ORDER_DESCENDING);
    assert(statement.order_items[0].null_order == PRDB_SDK_SQL_V11_NULLS_LAST);
    assert(statement.order_items[1].kind == PRDB_SDK_SQL_V10_ORDER_AGGREGATE);
    assert(statement.order_items[1].null_order == PRDB_SDK_SQL_V11_NULLS_FIRST);
    assert(prdb_sdk_sql_bind_select_v15(
               &statement, relation, sizeof(relation) - 1, columns, 3,
               &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.order_count == 3);
    assert(binding.order_items[0].type_code == PRDB_SDK_VALUE_TYPE_TEXT);
    assert(binding.order_items[1].type_code == PRDB_SDK_VALUE_TYPE_BIGINT);
    assert(binding.order_items[2].type_code == PRDB_SDK_VALUE_TYPE_INTEGER);

    assert(prdb_sdk_sql_parse_select_v15(
               eight_keys, sizeof(eight_keys) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.order_count == PRDB_SDK_SQL_V15_MAX_ORDER_KEYS);
    assert(prdb_sdk_sql_bind_select_v15(
               &statement, relation, sizeof(relation) - 1, columns_eight, 8,
               &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.order_items[8].type_code == PRDB_SDK_VALUE_TYPE_BIGINT);

    assert(prdb_sdk_sql_parse_select_v15(
               duplicate, sizeof(duplicate) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(prdb_sdk_sql_parse_select_v15(
               unselected, sizeof(unselected) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(prdb_sdk_sql_parse_select_v15(
               wrong_limit_order, sizeof(wrong_limit_order) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(prdb_sdk_sql_parse_select_v14(
               sql, sizeof(sql) - 1, &old_statement, sizeof(old_statement),
               &error_offset) == PRDB_SDK_SQL_SYNTAX_ERROR);

    assert(prdb_sdk_sql_parse_select_v15(sql, sizeof(sql) - 1, &statement,
                                         sizeof(statement), &error_offset) ==
           PRDB_SDK_SQL_OK);
    statement.order_items[1] = statement.order_items[0];
    prdb_sdk_select_binding_v15 previous = binding;
    assert(prdb_sdk_sql_bind_select_v15(
               &statement, relation, sizeof(relation) - 1, columns, 3,
               &binding, sizeof(binding)) ==
           PRDB_SDK_SQL_BIND_INVALID_STATEMENT);
    assert(memcmp(&binding, &previous, sizeof(binding)) == 0);
    return 0;
}
