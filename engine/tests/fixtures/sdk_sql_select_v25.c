#include "prdb_sdk_sql_v1.h"

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
    static const uint8_t decimal_order[] =
        "SELECT l.id, r.amount FROM lefts AS l INNER JOIN rights AS r ON "
        "l.id = r.id AND l.region = r.region "
        "ORDER BY r.amount DESC LIMIT 6";
    static const uint8_t decimal_join_order[] =
        "SELECT a.left_key, b.decimal_value "
        "FROM decimal_grouped_records AS a INNER JOIN "
        "decimal_grouped_records AS b ON a.left_key = b.left_key AND "
        "a.right_key = b.right_key ORDER BY b.decimal_value DESC LIMIT 6";
    static const uint8_t filtered_decimal_join_order[] =
        "SELECT a.id, b.amount FROM lefts AS a INNER JOIN rights AS b ON "
        "a.id = b.id AND a.region = b.region WHERE b.amount IS NULL "
        "ORDER BY b.amount DESC LIMIT 2";
    static const uint8_t parameterized_decimal_join_order[] =
        "SELECT a.id, b.amount FROM lefts AS a INNER JOIN rights AS b ON "
        "a.id = b.id AND a.region = b.region WHERE a.id >= $1 "
        "ORDER BY b.amount DESC LIMIT 2";
    static const uint8_t left_name[] = "lefts";
    static const uint8_t right_name[] = "rights";
    static const prdb_sdk_sql_column_shape_v1 left_columns[] = {
        {(const uint8_t *)"id", 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {(const uint8_t *)"region", 6, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {(const uint8_t *)"label", 5, 2, PRDB_SDK_VALUE_TYPE_TEXT, 0, 0}};
    static const prdb_sdk_sql_column_shape_v1 right_columns[] = {
        {(const uint8_t *)"id", 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {(const uint8_t *)"region", 6, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {(const uint8_t *)"value", 5, 2, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    static const prdb_sdk_sql_column_shape_v1 decimal_right_columns[] = {
        {(const uint8_t *)"id", 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {(const uint8_t *)"region", 6, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {(const uint8_t *)"amount", 6, 2, PRDB_SDK_VALUE_TYPE_DECIMAL, 4, 2}};
    prdb_sdk_select_v1 statement = {0};
    prdb_sdk_select_binding_v1 binding = {0};
    uint32_t error_offset = 0;

    assert(prdb_sdk_sql_parse_select_v1(
               sql, sizeof(sql) - 1, &statement, sizeof(statement),
               &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V1_VERSION &&
           statement.abi_version == PRDB_SDK_SQL_ABI_V1_VERSION &&
           statement.join_has_order_by == 1 && statement.join_order_descending == 1 &&
           statement.has_limit == 1 &&
           statement.limit == 2 &&
           memcmp(sql + statement.join_order_by_column.relation.offset, "r", 1) ==
               0 &&
           memcmp(sql + statement.join_order_by_column.column.offset, "value", 5) ==
               0);
    assert(prdb_sdk_sql_bind_join_select_v1(
               &statement, left_name, sizeof(left_name) - 1, left_columns, 3,
               right_name, sizeof(right_name) - 1, right_columns, 3, &binding,
               sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.abi_version == PRDB_SDK_SQL_ABI_V1_VERSION &&
           binding.output_projection_count == 2 &&
           binding.order_by_projection_index == 1 &&
           binding.order_relation_side == PRDB_SDK_SQL_JOIN_RIGHT &&
           binding.join_order_by_column.ordinal == 2 &&
           binding.join_order_by_column.type_code == PRDB_SDK_VALUE_TYPE_INTEGER);
    memset(&statement, 0, sizeof(statement));
    memset(&binding, 0, sizeof(binding));
    assert(prdb_sdk_sql_parse_select_v1(
               decimal_order, sizeof(decimal_order) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_join_select_v1(
               &statement, left_name, sizeof(left_name) - 1, left_columns, 3,
               right_name, sizeof(right_name) - 1, decimal_right_columns, 3,
               &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.join_order_by_column.type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
           binding.join_order_by_column.decimal_precision == 4 &&
           binding.join_order_by_column.decimal_scale == 2 &&
           binding.order_by_projection_index == 1);
    memset(&statement, 0, sizeof(statement));
    assert(prdb_sdk_sql_parse_select_v1(
               decimal_join_order, sizeof(decimal_join_order) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    memset(&statement, 0, sizeof(statement));
    memset(&binding, 0, sizeof(binding));
    assert(prdb_sdk_sql_parse_select_v1(
               filtered_decimal_join_order,
               sizeof(filtered_decimal_join_order) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_join_select_v1(
               &statement, left_name, sizeof(left_name) - 1, left_columns, 3,
               right_name, sizeof(right_name) - 1, decimal_right_columns, 3,
               &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(statement.has_join_where == 1 &&
           binding.join_order_by_column.type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
           binding.order_by_projection_index == 1 &&
           binding.order_relation_side == PRDB_SDK_SQL_JOIN_RIGHT);
    memset(&statement, 0, sizeof(statement));
    memset(&binding, 0, sizeof(binding));
    assert(prdb_sdk_sql_parse_select_v1(
               parameterized_decimal_join_order,
               sizeof(parameterized_decimal_join_order) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.has_join_where == 1 && statement.join_where_parameter_index == 1);
    assert(prdb_sdk_sql_bind_join_select_v1(
               &statement, left_name, sizeof(left_name) - 1, left_columns, 3,
               right_name, sizeof(right_name) - 1, decimal_right_columns, 3,
               &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.where_relation_side == PRDB_SDK_SQL_JOIN_LEFT &&
           binding.order_relation_side == PRDB_SDK_SQL_JOIN_RIGHT &&
           binding.order_by_projection_index == 1);

    assert(prdb_sdk_sql_parse_select_v1(
               no_order, sizeof(no_order) - 1, &statement, sizeof(statement),
               &error_offset) == PRDB_SDK_SQL_OK &&
           statement.join_has_order_by == 0);
    assert(prdb_sdk_sql_parse_select_v1(
               not_projected, sizeof(not_projected) - 1, &statement,
               sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_join_select_v1(
               &statement, left_name, sizeof(left_name) - 1, left_columns, 3,
               right_name, sizeof(right_name) - 1, right_columns, 3, &binding,
               sizeof(binding)) == PRDB_SDK_SQL_BIND_COLUMN_NOT_FOUND);
    return 0;
}
