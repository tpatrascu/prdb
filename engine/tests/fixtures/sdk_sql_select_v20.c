#include "prdb_sdk_sql_v1.h"

#include <assert.h>
#include <string.h>

static prdb_sdk_sql_column_shape_v1 column(const char *name,
                                          uint32_t ordinal) {
    return (prdb_sdk_sql_column_shape_v1){
        (const uint8_t *)name, (uint32_t)strlen(name), ordinal,
        PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0};
}

int main(void) {
    static const uint8_t sql[] =
        "SELECT l.label, r.value FROM lefts AS l INNER JOIN rights AS r ON "
        "l.id = r.id AND l.region = r.region WHERE r.value >= $1 LIMIT 2";
    static const uint8_t invalid_parameter[] =
        "SELECT l.label FROM lefts AS l INNER JOIN rights AS r ON "
        "l.id = r.id AND l.region = r.region WHERE r.value = $0";
    static const uint8_t left_name[] = "lefts";
    static const uint8_t right_name[] = "rights";
    prdb_sdk_sql_column_shape_v1 left_columns[] = {
        column("id", 0), column("region", 1), column("label", 2)};
    prdb_sdk_sql_column_shape_v1 right_columns[] = {
        column("id", 0), column("region", 1), column("value", 2)};
    prdb_sdk_select_v1 statement = {0};
    prdb_sdk_select_binding_v1 binding = {0};
    uint32_t error_offset = 0;

    assert(prdb_sdk_sql_parse_select_v1(
               sql, sizeof(sql) - 1, &statement, sizeof(statement),
               &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V1_VERSION &&
           statement.abi_version == PRDB_SDK_SQL_ABI_V1_VERSION &&
           statement.has_join_where == 1 &&
           statement.join_where_comparison == PRDB_SDK_SQL_COMPARE_GREATER_EQUAL &&
           statement.join_where_parameter_index == 1 &&
           statement.has_limit == 1 &&
           statement.limit == 2);
    assert(prdb_sdk_sql_bind_join_select_v1(
               &statement, left_name, sizeof(left_name) - 1, left_columns, 3,
               right_name, sizeof(right_name) - 1, right_columns, 3, &binding,
               sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.abi_version == PRDB_SDK_SQL_ABI_V1_VERSION &&
           binding.output_projection_count == 2 &&
           binding.where_relation_side == PRDB_SDK_SQL_JOIN_RIGHT &&
           binding.where_column.ordinal == 2 &&
           binding.where_projection_index == 2 &&
           binding.join_projection_count == 3);

    statement.struct_size = 91;
    assert(prdb_sdk_sql_parse_select_v1(
               invalid_parameter, sizeof(invalid_parameter) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(statement.struct_size == 91 && error_offset != 0);
    return 0;
}
