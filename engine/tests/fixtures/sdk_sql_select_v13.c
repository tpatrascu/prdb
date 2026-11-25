#include "prdb_sdk_sql_v1.h"

#include <assert.h>
#include <string.h>

int main(void) {
    static const uint8_t relation[] = "records";
    static const uint8_t key_a[] = "a", key_b[] = "b", key_c[] = "c";
    static const uint8_t key_d[] = "d", key_e[] = "e";
    static const uint8_t key_f[] = "f", key_g[] = "g", key_h[] = "h";
    static const uint8_t sql[] =
        "SELECT a, b, COUNT(*) FROM records WHERE c > $1 "
        "GROUP BY a, b LIMIT 7";
    static const uint8_t sql_eight[] =
        "SELECT a, b, c, d, e, f, g, h, AVG(c) FROM records "
        "GROUP BY a, b, c, d, e, f, g, h";
    static const uint8_t mismatch[] =
        "SELECT a, b, COUNT(*) FROM records GROUP BY b, a";
    static const uint8_t duplicate[] =
        "SELECT a, a, COUNT(*) FROM records GROUP BY a, a";
    static const uint8_t too_many[] =
        "SELECT a, b, c, d, e, f, g, h, i, COUNT(*) FROM records "
        "GROUP BY a, b, c, d, e, f, g, h, i";
    prdb_sdk_sql_column_shape_v1 columns[3] = {
        {key_a, 1, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {key_b, 1, 1, PRDB_SDK_VALUE_TYPE_TEXT, 0, 0},
        {key_c, 1, 2, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_sql_column_shape_v1 columns_eight[8] = {
        {key_a, 1, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {key_b, 1, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {key_c, 1, 2, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {key_d, 1, 3, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {key_e, 1, 4, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {key_f, 1, 5, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {key_g, 1, 6, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {key_h, 1, 7, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_select_v1 statement = {0};
    prdb_sdk_select_binding_v1 binding = {0};
    uint32_t error_offset = 0;

    assert(prdb_sdk_sql_parse_select_v1(sql, sizeof(sql) - 1, &statement,
                                         sizeof(statement), &error_offset) ==
           PRDB_SDK_SQL_OK);
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V1_VERSION);
    assert(statement.group_key_count == 2);
    assert(statement.projection_count == 1);
    assert(statement.predicate_count == 1);
    assert(statement.has_limit == 1);
    assert(statement.aggregate_kind ==
           PRDB_SDK_SQL_AGGREGATE_COUNT_STAR);
    assert(prdb_sdk_sql_bind_select_v1(
               &statement, relation, sizeof(relation) - 1, columns, 3,
               &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.group_key_count == 2);
    assert(binding.group_keys[0].ordinal == 0);
    assert(binding.group_keys[0].type_code == PRDB_SDK_VALUE_TYPE_INTEGER);
    assert(binding.group_keys[1].ordinal == 1);
    assert(binding.group_keys[1].type_code == PRDB_SDK_VALUE_TYPE_TEXT);

    assert(prdb_sdk_sql_parse_select_v1(
               sql_eight, sizeof(sql_eight) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.group_key_count == PRDB_SDK_SQL_MAX_GROUP_KEYS);
    assert(statement.aggregate_kind == PRDB_SDK_SQL_AGGREGATE_AVG);
    assert(prdb_sdk_sql_bind_select_v1(
               &statement, relation, sizeof(relation) - 1, columns_eight, 8,
               &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.group_key_count == 8);
    assert(binding.group_keys[7].ordinal == 7);

    memset(&statement, 0xA5, sizeof(statement));
    prdb_sdk_select_v1 previous = statement;
    assert(prdb_sdk_sql_parse_select_v1(
               mismatch, sizeof(mismatch) - 1, &statement, sizeof(statement),
               &error_offset) == PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(memcmp(&statement, &previous, sizeof(statement)) == 0);
    assert(prdb_sdk_sql_parse_select_v1(
               duplicate, sizeof(duplicate) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(prdb_sdk_sql_parse_select_v1(
               too_many, sizeof(too_many) - 1, &statement, sizeof(statement),
               &error_offset) == PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(prdb_sdk_sql_parse_select_v1(
               sql, sizeof(sql) - 1, &statement, sizeof(statement),
               &error_offset) == PRDB_SDK_SQL_OK);
    previous = statement;
    memset(&binding, 0x5A, sizeof(binding));
    prdb_sdk_select_binding_v1 previous_binding = binding;
    statement.group_keys[1] = statement.group_keys[0];
    assert(prdb_sdk_sql_bind_select_v1(
               &statement, relation, sizeof(relation) - 1, columns, 3,
               &binding, sizeof(binding)) ==
           PRDB_SDK_SQL_BIND_INVALID_STATEMENT);
    assert(memcmp(&binding, &previous_binding, sizeof(binding)) == 0);
    statement = previous;
    statement.projection_keys[1] = statement.projection_keys[0];
    assert(prdb_sdk_sql_bind_select_v1(
               &statement, relation, sizeof(relation) - 1, columns, 3,
               &binding, sizeof(binding)) ==
           PRDB_SDK_SQL_BIND_INVALID_STATEMENT);
    assert(memcmp(&binding, &previous_binding, sizeof(binding)) == 0);
    return 0;
}
