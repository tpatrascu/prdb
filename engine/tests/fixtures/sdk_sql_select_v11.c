#include "prdb_sdk_sql_v1.h"

#include <assert.h>
#include <string.h>

int main(void) {
    static const uint8_t relation[] = "records";
    static const uint8_t sql_last[] =
        "SELECT k, MIN(v) FROM records GROUP BY k ORDER BY MIN(v) DESC NULLS LAST";
    static const uint8_t sql_first[] =
        "SELECT k, COUNT(*) FROM records GROUP BY k ORDER BY k NULLS FIRST LIMIT 2";
    static const uint8_t sql_default[] =
        "SELECT k, COUNT(*) FROM records GROUP BY k ORDER BY COUNT(*) DESC";
    static const uint8_t malformed[] =
        "SELECT k, COUNT(*) FROM records GROUP BY k ORDER BY k NULLS SIDEWAYS";
    static const uint8_t k[] = "k", v[] = "v";
    prdb_sdk_sql_column_shape_v1 columns[2] = {
        {k, 1, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {v, 1, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_select_v1 statement = {0};
    prdb_sdk_select_binding_v1 binding = {0};
    uint32_t error_offset = 0;

    assert(prdb_sdk_sql_parse_select_v1(
               sql_last, sizeof(sql_last) - 1, &statement, sizeof(statement),
               &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V1_VERSION);
    assert(statement.order_items[0].null_order == PRDB_SDK_SQL_NULLS_LAST);
    assert(statement.order_items[0].direction == PRDB_SDK_SQL_ORDER_DESCENDING);
    assert(prdb_sdk_sql_bind_select_v1(
               &statement, relation, sizeof(relation) - 1, columns, 2,
               &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.order_items[0].item.null_order == PRDB_SDK_SQL_NULLS_LAST);

    memset(&statement, 0, sizeof(statement));
    assert(prdb_sdk_sql_parse_select_v1(
               sql_first, sizeof(sql_first) - 1, &statement, sizeof(statement),
               &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.order_items[0].null_order == PRDB_SDK_SQL_NULLS_FIRST);
    assert(statement.has_limit == 1);

    memset(&statement, 0, sizeof(statement));
    assert(prdb_sdk_sql_parse_select_v1(
               sql_default, sizeof(sql_default) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.order_items[0].null_order ==
           PRDB_SDK_SQL_NULL_ORDER_DEFAULT);

    memset(&statement, 0xA5, sizeof(statement));
    prdb_sdk_select_v1 previous = statement;
    assert(prdb_sdk_sql_parse_select_v1(
               malformed, sizeof(malformed) - 1, &statement, sizeof(statement),
               &error_offset) == PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(memcmp(&statement, &previous, sizeof(statement)) == 0);
    return 0;
}
