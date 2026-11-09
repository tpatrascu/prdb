#include "prdb_sdk_sql_v12.h"

#include <assert.h>
#include <string.h>

int main(void) {
    static const uint8_t relation[] = "records";
    static const uint8_t key[] = "k", value[] = "v";
    static const uint8_t sql[] =
        "SELECT k, COUNT(*) FROM records GROUP BY k "
        "ORDER BY COUNT(*) DESC NULLS LAST, k ASC NULLS FIRST LIMIT 4";
    static const uint8_t duplicate[] =
        "SELECT k, COUNT(*) FROM records GROUP BY k ORDER BY k, k";
    static const uint8_t third_key[] =
        "SELECT k, COUNT(*) FROM records GROUP BY k ORDER BY k, COUNT(*), k";
    static const uint8_t malformed[] =
        "SELECT k, COUNT(*) FROM records GROUP BY k ORDER BY k, DESC";
    static const uint8_t no_order[] =
        "SELECT k, COUNT(*) FROM records GROUP BY k";
    prdb_sdk_sql_column_shape_v2 columns[2] = {
        {key, 1, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {value, 1, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_select_v12 statement = {0};
    prdb_sdk_select_binding_v12 binding = {0};
    uint32_t error_offset = 0;

    assert(prdb_sdk_sql_parse_select_v12(sql, sizeof(sql) - 1, &statement,
                                         sizeof(statement), &error_offset) ==
           PRDB_SDK_SQL_OK);
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V12_VERSION);
    assert(statement.order_count == 2);
    assert(statement.order_items[0].kind == PRDB_SDK_SQL_V10_ORDER_AGGREGATE);
    assert(statement.order_items[0].direction ==
           PRDB_SDK_SQL_V10_ORDER_DESCENDING);
    assert(statement.order_items[0].null_order == PRDB_SDK_SQL_V11_NULLS_LAST);
    assert(statement.order_items[1].kind == PRDB_SDK_SQL_V10_ORDER_GROUP_KEY);
    assert(statement.order_items[1].direction ==
           PRDB_SDK_SQL_V10_ORDER_ASCENDING);
    assert(statement.order_items[1].null_order == PRDB_SDK_SQL_V11_NULLS_FIRST);
    assert(statement.base.base.base.base.base.has_limit == 1);
    assert(prdb_sdk_sql_bind_select_v12(
               &statement, relation, sizeof(relation) - 1, columns, 2,
               &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.order_count == 2);
    assert(binding.order_items[0].type_code == PRDB_SDK_VALUE_TYPE_BIGINT);
    assert(binding.order_items[1].type_code == PRDB_SDK_VALUE_TYPE_INTEGER);

    memset(&statement, 0, sizeof(statement));
    assert(prdb_sdk_sql_parse_select_v12(
               no_order, sizeof(no_order) - 1, &statement, sizeof(statement),
               &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.order_count == 0);
    assert(prdb_sdk_sql_bind_select_v12(
               &statement, relation, sizeof(relation) - 1, columns, 2,
               &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.order_count == 0 && binding.base.base.has_order_by == 0);

    memset(&statement, 0xA5, sizeof(statement));
    prdb_sdk_select_v12 previous = statement;
    assert(prdb_sdk_sql_parse_select_v12(
               duplicate, sizeof(duplicate) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(memcmp(&statement, &previous, sizeof(statement)) == 0);
    assert(prdb_sdk_sql_parse_select_v12(
               third_key, sizeof(third_key) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(prdb_sdk_sql_parse_select_v12(
               malformed, sizeof(malformed) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_SYNTAX_ERROR);
    return 0;
}
