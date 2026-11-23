#include "prdb_sdk_sql_v25.h"

#include <assert.h>
#include <string.h>

int main(void) {
    static const uint8_t sql[] =
        "SELECT l.label, r.value FROM lefts AS l INNER JOIN rights AS r ON "
        "l.id = r.id AND l.region = r.region WHERE r.value >= $1 "
        "ORDER BY r.value DESC LIMIT 2;";
    static const uint8_t not_projected[] =
        "SELECT l.label FROM lefts AS l INNER JOIN rights AS r ON "
        "l.id = r.id AND l.region = r.region ORDER BY r.region";
    static const uint8_t no_order[] =
        "SELECT l.label FROM lefts AS l INNER JOIN rights AS r ON "
        "l.id = r.id AND l.region = r.region";
    static const uint8_t left_name[] = "lefts";
    static const uint8_t right_name[] = "rights";
    static const prdb_sdk_sql_column_shape_v2 left_columns[] = {
        {(const uint8_t *)"id", 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {(const uint8_t *)"region", 6, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {(const uint8_t *)"label", 5, 2, PRDB_SDK_VALUE_TYPE_TEXT, 0, 0}};
    static const prdb_sdk_sql_column_shape_v2 right_columns[] = {
        {(const uint8_t *)"id", 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {(const uint8_t *)"region", 6, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {(const uint8_t *)"value", 5, 2, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_select_v25 statement = {0};
    prdb_sdk_select_binding_v25 binding = {0};
    prdb_sdk_select_v20 legacy = {0};
    uint32_t error_offset = 0;

    assert(prdb_sdk_sql_parse_select_v25(
               sql, sizeof(sql) - 1, &statement, sizeof(statement),
               &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V25_VERSION &&
           statement.base.abi_version == PRDB_SDK_SQL_ABI_V20_VERSION &&
           statement.has_order_by == 1 && statement.order_descending == 1 &&
           statement.base.base.base.base.has_limit == 1 &&
           statement.base.base.base.base.limit == 2 &&
           memcmp(sql + statement.order_by_column.relation.offset, "r", 1) ==
               0 &&
           memcmp(sql + statement.order_by_column.column.offset, "value", 5) ==
               0);
    assert(prdb_sdk_sql_bind_select_v25(
               &statement, left_name, sizeof(left_name) - 1, left_columns, 3,
               right_name, sizeof(right_name) - 1, right_columns, 3, &binding,
               sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.abi_version == PRDB_SDK_SQL_ABI_V25_VERSION &&
           binding.base.output_projection_count == 2 &&
           binding.order_by_projection_index == 1 &&
           binding.order_relation_side == PRDB_SDK_SQL_V8_RIGHT &&
           binding.order_by_column.ordinal == 2 &&
           binding.order_by_column.type_code == PRDB_SDK_VALUE_TYPE_INTEGER);

    assert(prdb_sdk_sql_parse_select_v20(
               sql, sizeof(sql) - 1, &legacy, sizeof(legacy), NULL) ==
           PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(prdb_sdk_sql_parse_select_v25(
               no_order, sizeof(no_order) - 1, &statement, sizeof(statement),
               &error_offset) == PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(prdb_sdk_sql_parse_select_v25(
               not_projected, sizeof(not_projected) - 1, &statement,
               sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_select_v25(
               &statement, left_name, sizeof(left_name) - 1, left_columns, 3,
               right_name, sizeof(right_name) - 1, right_columns, 3, &binding,
               sizeof(binding)) == PRDB_SDK_SQL_BIND_COLUMN_NOT_FOUND);
    return 0;
}
