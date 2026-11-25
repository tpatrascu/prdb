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
        "l.id = r.id AND l.region = r.region LIMIT 3";
    static const uint8_t duplicate_alias[] =
        "SELECT l.id FROM lefts AS l INNER JOIN rights AS L ON "
        "l.id = L.id AND l.region = L.region";
    static const uint8_t no_alias[] =
        "SELECT lefts.label, rights.value FROM lefts INNER JOIN rights ON "
        "lefts.id = rights.id AND lefts.region = rights.region";
    static const uint8_t missing_alias[] =
        "SELECT l.id FROM lefts AS INNER JOIN rights ON "
        "lefts.id = rights.id AND lefts.region = rights.region";
    static const uint8_t self_join_sql[] =
        "SELECT a.label, b.label FROM lefts AS a INNER JOIN lefts AS b ON "
        "a.id = b.id AND a.region = b.region";
    static const uint8_t left_name[] = "lefts";
    static const uint8_t right_name[] = "rights";
    prdb_sdk_select_v1 statement = {0};
    prdb_sdk_select_v1 no_alias_statement = {0};
    prdb_sdk_select_v1 self_join_statement = {0};
    prdb_sdk_select_v1 unchanged_statement = {0};
    prdb_sdk_select_binding_v1 binding = {0};
    prdb_sdk_select_binding_v1 unchanged_binding = {0};
    prdb_sdk_sql_column_shape_v1 left_columns[] = {
        column("id", 0), column("region", 1), column("label", 2)};
    prdb_sdk_sql_column_shape_v1 right_columns[] = {
        column("id", 0), column("region", 1), column("value", 2)};
    uint32_t error_offset = 0;

    assert(prdb_sdk_sql_parse_select_v1(
               sql, sizeof(sql) - 1, &statement, sizeof(statement),
               &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V1_VERSION &&
           statement.abi_version == PRDB_SDK_SQL_ABI_V1_VERSION &&
           statement.join_key_count == 2 &&
           statement.has_limit == 1 &&
           statement.limit == 3);
    assert(memcmp(sql + statement.left_alias.offset, "l", 1) == 0 &&
           memcmp(sql + statement.right_alias.offset, "r", 1) == 0);
    assert(prdb_sdk_sql_bind_join_select_v1(
               &statement, left_name, sizeof(left_name) - 1, left_columns, 3,
               right_name, sizeof(right_name) - 1, right_columns, 3, &binding,
               sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.abi_version == PRDB_SDK_SQL_ABI_V1_VERSION &&
           binding.join_key_count == 2 &&
           binding.join_projection_count == 2 &&
           binding.join_projections[0].relation_side ==
               PRDB_SDK_SQL_JOIN_LEFT &&
           binding.join_projections[0].column.ordinal == 2 &&
           binding.join_projections[1].relation_side ==
               PRDB_SDK_SQL_JOIN_RIGHT &&
           binding.join_projections[1].column.ordinal == 2 &&
           binding.join_keys[1].left.ordinal == 1 &&
           binding.join_keys[1].right.ordinal == 1);

    assert(prdb_sdk_sql_parse_select_v1(
               no_alias, sizeof(no_alias) - 1, &no_alias_statement,
               sizeof(no_alias_statement), NULL) == PRDB_SDK_SQL_OK);
    assert(no_alias_statement.left_alias.offset ==
               no_alias_statement.left_relation.offset &&
           no_alias_statement.left_alias.length ==
               no_alias_statement.left_relation.length &&
           no_alias_statement.right_alias.offset ==
               no_alias_statement.right_relation.offset &&
           no_alias_statement.right_alias.length ==
               no_alias_statement.right_relation.length);
    assert(prdb_sdk_sql_bind_join_select_v1(
               &no_alias_statement, left_name, sizeof(left_name) - 1,
               left_columns, 3, right_name, sizeof(right_name) - 1,
               right_columns, 3, &binding, sizeof(binding)) ==
           PRDB_SDK_SQL_BIND_OK);

    assert(prdb_sdk_sql_parse_select_v1(
               self_join_sql, sizeof(self_join_sql) - 1, &self_join_statement,
               sizeof(self_join_statement), NULL) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_join_select_v1(
               &self_join_statement, left_name, sizeof(left_name) - 1,
               left_columns, 3, left_name, sizeof(left_name) - 1,
               left_columns, 3, &binding, sizeof(binding)) ==
           PRDB_SDK_SQL_BIND_OK);
    assert(binding.join_projections[0].relation_side ==
               PRDB_SDK_SQL_JOIN_LEFT &&
           binding.join_projections[1].relation_side ==
               PRDB_SDK_SQL_JOIN_RIGHT &&
           binding.join_keys[0].left.ordinal == 0 &&
           binding.join_keys[0].right.ordinal == 0 &&
           binding.join_keys[1].left.ordinal == 1 &&
           binding.join_keys[1].right.ordinal == 1);

    unchanged_statement.struct_size = 73;
    assert(prdb_sdk_sql_parse_select_v1(
               duplicate_alias, sizeof(duplicate_alias) - 1,
               &unchanged_statement, sizeof(unchanged_statement), NULL) ==
           PRDB_SDK_SQL_OK);
    unchanged_binding.struct_size = 81;
    assert(prdb_sdk_sql_bind_join_select_v1(
               &unchanged_statement, left_name, sizeof(left_name) - 1,
               left_columns, 3, right_name, sizeof(right_name) - 1,
               right_columns, 3, &unchanged_binding,
               sizeof(unchanged_binding)) ==
           PRDB_SDK_SQL_BIND_INVALID_STATEMENT);
    assert(unchanged_binding.struct_size == 81);

    unchanged_statement.struct_size = 91;
    assert(prdb_sdk_sql_parse_select_v1(
               missing_alias, sizeof(missing_alias) - 1,
               &unchanged_statement, sizeof(unchanged_statement),
               &error_offset) == PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(unchanged_statement.struct_size == 91 && error_offset != 0);

    statement.left_alias.offset = UINT32_MAX;
    assert(prdb_sdk_sql_bind_join_select_v1(
               &statement, left_name, sizeof(left_name) - 1, left_columns, 3,
               right_name, sizeof(right_name) - 1, right_columns, 3,
               &unchanged_binding, sizeof(unchanged_binding)) ==
           PRDB_SDK_SQL_BIND_INVALID_STATEMENT);
    assert(unchanged_binding.struct_size == 81);
    return 0;
}
