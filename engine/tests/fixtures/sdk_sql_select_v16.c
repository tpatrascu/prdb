#include "prdb_sdk_sql_v16.h"
#include "prdb_sdk_sql_v15.h"
#include "prdb_sdk_sql_v8.h"

#include <assert.h>
#include <string.h>

static prdb_sdk_sql_column_shape_v2 column(const char *name,
                                          uint32_t ordinal,
                                          uint32_t type_code) {
    return (prdb_sdk_sql_column_shape_v2){
        (const uint8_t *)name, (uint32_t)strlen(name), ordinal, type_code, 0,
        0};
}

int main(void) {
    static const uint8_t sql[] =
        "SELECT lefts.label, rights.value FROM lefts INNER JOIN rights ON "
        "lefts.id = rights.id AND lefts.region = rights.region LIMIT 3";
    static const uint8_t single_key[] =
        "SELECT lefts.label FROM lefts INNER JOIN rights ON "
        "lefts.id = rights.id";
    static const uint8_t missing_rhs[] =
        "SELECT lefts.label FROM lefts INNER JOIN rights ON "
        "lefts.id = rights.id AND lefts.region = LIMIT 3";
    static const uint8_t eight_keys[] =
        "SELECT lefts.label FROM lefts INNER JOIN rights ON "
        "lefts.id = rights.id AND lefts.id = rights.id AND "
        "lefts.id = rights.id AND lefts.id = rights.id AND "
        "lefts.id = rights.id AND lefts.id = rights.id AND "
        "lefts.id = rights.id AND lefts.id = rights.id";
    static const uint8_t nine_keys[] =
        "SELECT lefts.label FROM lefts INNER JOIN rights ON "
        "lefts.id = rights.id AND lefts.id = rights.id AND "
        "lefts.id = rights.id AND lefts.id = rights.id AND "
        "lefts.id = rights.id AND lefts.id = rights.id AND "
        "lefts.id = rights.id AND lefts.id = rights.id AND "
        "lefts.id = rights.id";
    static const uint8_t left_name[] = "lefts";
    static const uint8_t right_name[] = "rights";
    static const uint8_t wrong_name[] = "other";
    prdb_sdk_select_v16 statement = {0};
    prdb_sdk_select_v16 unchanged_statement = {0};
    prdb_sdk_select_binding_v16 binding = {0};
    prdb_sdk_select_binding_v16 unchanged_binding = {0};
    prdb_sdk_sql_column_shape_v2 left_columns[] = {
        column("id", 0, PRDB_SDK_VALUE_TYPE_INTEGER),
        column("region", 1, PRDB_SDK_VALUE_TYPE_TEXT),
        column("label", 2, PRDB_SDK_VALUE_TYPE_TEXT)};
    prdb_sdk_sql_column_shape_v2 right_columns[] = {
        column("id", 0, PRDB_SDK_VALUE_TYPE_INTEGER),
        column("region", 1, PRDB_SDK_VALUE_TYPE_TEXT),
        column("value", 2, PRDB_SDK_VALUE_TYPE_TEXT)};
    uint32_t error_offset = 0;

    assert(prdb_sdk_sql_parse_select_v16(
               sql, sizeof(sql) - 1, &statement, sizeof(statement),
               &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.struct_size == sizeof(statement));
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V16_VERSION);
    assert(statement.join_key_count == 2);
    assert(statement.base.projection_count == 2 &&
           statement.base.has_limit == 1 && statement.base.limit == 3);
    assert(memcmp(sql + statement.left_join_keys[1].column.offset, "region",
                  6) == 0);
    assert(prdb_sdk_sql_bind_select_v16(
               &statement, left_name, sizeof(left_name) - 1, left_columns, 3,
               right_name, sizeof(right_name) - 1, right_columns, 3, &binding,
               sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.abi_version == PRDB_SDK_SQL_ABI_V16_VERSION &&
           binding.join_key_count == 2);
    assert(binding.base.projection_count == 2);
    assert(binding.join_keys[0].left.ordinal == 0 &&
           binding.join_keys[0].right.ordinal == 0 &&
           binding.join_keys[0].left.type_code ==
               PRDB_SDK_VALUE_TYPE_INTEGER);
    assert(binding.join_keys[1].left.ordinal == 1 &&
           binding.join_keys[1].right.ordinal == 1 &&
           binding.join_keys[1].left.type_code == PRDB_SDK_VALUE_TYPE_TEXT);
    {
        prdb_sdk_select_v16 eight_statement = {0};
        prdb_sdk_select_binding_v16 eight_binding = {0};
        assert(prdb_sdk_sql_parse_select_v16(
                   eight_keys, sizeof(eight_keys) - 1, &eight_statement,
                   sizeof(eight_statement), NULL) == PRDB_SDK_SQL_OK);
        assert(eight_statement.join_key_count == 8);
        assert(prdb_sdk_sql_bind_select_v16(
                   &eight_statement, left_name, sizeof(left_name) - 1,
                   left_columns, 3, right_name, sizeof(right_name) - 1,
                   right_columns, 3, &eight_binding, sizeof(eight_binding)) ==
               PRDB_SDK_SQL_BIND_OK);
        assert(eight_binding.join_key_count == 8 &&
               eight_binding.join_keys[7].left.ordinal == 0);
        assert(prdb_sdk_sql_parse_select_v16(
                   nine_keys, sizeof(nine_keys) - 1, &unchanged_statement,
                   sizeof(unchanged_statement), NULL) ==
               PRDB_SDK_SQL_LIMIT_EXCEEDED);
        assert(unchanged_statement.struct_size == 0);
    }

    unchanged_statement.struct_size = 71;
    assert(prdb_sdk_sql_parse_select_v16(
               single_key, sizeof(single_key) - 1, &unchanged_statement,
               sizeof(unchanged_statement), &error_offset) ==
           PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(unchanged_statement.struct_size == 71);
    assert(prdb_sdk_sql_parse_select_v16(
               missing_rhs, sizeof(missing_rhs) - 1, &unchanged_statement,
               sizeof(unchanged_statement), &error_offset) ==
           PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(unchanged_statement.struct_size == 71);

    unchanged_binding.struct_size = 81;
    assert(prdb_sdk_sql_bind_select_v16(
               &statement, wrong_name, sizeof(wrong_name) - 1, left_columns, 3,
               right_name, sizeof(right_name) - 1, right_columns, 3,
               &unchanged_binding, sizeof(unchanged_binding)) ==
           PRDB_SDK_SQL_BIND_RELATION_MISMATCH);
    assert(unchanged_binding.struct_size == 81);
    {
        prdb_sdk_select_v16 wrong_side = statement;
        wrong_side.left_join_keys[1].relation = statement.base.right_relation;
        assert(prdb_sdk_sql_bind_select_v16(
                   &wrong_side, left_name, sizeof(left_name) - 1,
                   left_columns, 3, right_name, sizeof(right_name) - 1,
                   right_columns, 3, &unchanged_binding,
                   sizeof(unchanged_binding)) ==
               PRDB_SDK_SQL_BIND_INVALID_STATEMENT);
        assert(unchanged_binding.struct_size == 81);
    }
    right_columns[1].type_code = PRDB_SDK_VALUE_TYPE_BOOLEAN;
    assert(prdb_sdk_sql_bind_select_v16(
               &statement, left_name, sizeof(left_name) - 1, left_columns, 3,
               right_name, sizeof(right_name) - 1, right_columns, 3,
               &unchanged_binding, sizeof(unchanged_binding)) ==
           PRDB_SDK_SQL_BIND_V16_JOIN_TYPE_MISMATCH);
    assert(unchanged_binding.struct_size == 81);

    {
        prdb_sdk_select_v8 old_statement = {0};
        prdb_sdk_select_v15 old_v15 = {0};
        assert(prdb_sdk_sql_parse_select_v8(
                   sql, sizeof(sql) - 1, &old_statement,
                   sizeof(old_statement), NULL) != PRDB_SDK_SQL_OK);
        assert(prdb_sdk_sql_parse_select_v15(
                   sql, sizeof(sql) - 1, &old_v15, sizeof(old_v15), NULL) !=
               PRDB_SDK_SQL_OK);
    }
    return 0;
}
