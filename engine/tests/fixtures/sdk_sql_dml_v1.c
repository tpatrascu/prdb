#include "prdb_sdk_sql_dml_v1.h"
#include "prdb_sdk_sql_dml_v2.h"

#include <assert.h>
#include <string.h>

int main(void) {
    static const uint8_t insert_sql[] =
        "INSERT INTO widgets VALUES ($2, DEFAULT, NULL);";
    static const uint8_t malformed_sql[] =
        "INSERT INTO widgets VALUES ($0)";
    static const uint8_t second_statement_sql[] =
        "INSERT INTO widgets VALUES ($1); INSERT INTO widgets VALUES ($2)";
    static const uint8_t unsupported_sql[] = "UPDATE widgets SET id = $1";
    static const uint8_t multi_insert_sql[] =
        "INSERT INTO widgets VALUES ($1, NULL), (DEFAULT, $3);";
    static const uint8_t mismatched_multi_insert_sql[] =
        "INSERT INTO widgets VALUES ($1, NULL), ($2);";
    static const uint8_t single_tuple_sql[] =
        "INSERT INTO widgets VALUES ($1, DEFAULT, NULL);";
    static const uint8_t insert_prefix[] = "INSERT INTO widgets VALUES ";
    static const uint8_t update_sql[] =
        "UPDATE widgets SET id = $1, label = NULL "
        "WHERE id <> $2 AND enabled IS NOT NULL;";
    static const uint8_t duplicate_set_sql[] =
        "UPDATE widgets SET id = $1, ID = NULL WHERE id = $2";
    static const uint8_t malformed_update_sql[] =
        "UPDATE widgets SET id = $0 WHERE id = $1";
    static const uint8_t delete_sql[] =
        "DELETE FROM widgets WHERE id = $1 AND enabled IS NULL;";
    static const uint8_t missing_delete_where_sql[] =
        "DELETE FROM widgets";
    prdb_sdk_insert_v1 insert = {0};
    prdb_sdk_insert_v2 insert_v2 = {0};
    prdb_sdk_update_v1 update = {0};
    prdb_sdk_delete_v1 delete_statement = {0};
    uint32_t error_offset = 0;

    assert(prdb_sdk_sql_parse_insert_v1(
               insert_sql, sizeof(insert_sql) - 1, &insert, sizeof(insert),
               &error_offset) == PRDB_SDK_SQL_OK);
    assert(insert.abi_version == PRDB_SDK_SQL_DML_ABI_V1_VERSION);
    assert(insert.relation.length == 7 &&
           memcmp(insert.sql + insert.relation.offset, "widgets", 7) == 0);
    assert(insert.value_count == 3);
    assert(insert.values[0].kind == PRDB_SDK_SQL_INSERT_PARAMETER &&
           insert.values[0].parameter_index == 2);
    assert(insert.values[1].kind == PRDB_SDK_SQL_INSERT_DEFAULT);
    assert(insert.values[2].kind == PRDB_SDK_SQL_INSERT_NULL);

    uint32_t saved_value_count = insert.value_count;
    uint32_t saved_first_parameter = insert.values[0].parameter_index;
    assert(prdb_sdk_sql_parse_insert_v1(
               malformed_sql, sizeof(malformed_sql) - 1, &insert,
               sizeof(insert), &error_offset) == PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(insert.value_count == saved_value_count &&
           insert.values[0].parameter_index == saved_first_parameter);
    assert(prdb_sdk_sql_parse_insert_v1(
               second_statement_sql, sizeof(second_statement_sql) - 1,
               &insert, sizeof(insert), &error_offset) ==
           PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(prdb_sdk_sql_parse_insert_v1(
               unsupported_sql, sizeof(unsupported_sql) - 1, &insert,
               sizeof(insert), &error_offset) ==
           PRDB_SDK_SQL_UNSUPPORTED_STATEMENT);

    assert(prdb_sdk_sql_parse_insert_v2(
               multi_insert_sql, sizeof(multi_insert_sql) - 1, &insert_v2,
               sizeof(insert_v2), &error_offset) == PRDB_SDK_SQL_OK);
    assert(insert_v2.abi_version == PRDB_SDK_SQL_DML_ABI_V2_VERSION);
    assert(insert_v2.base.abi_version == PRDB_SDK_SQL_DML_ABI_V1_VERSION);
    assert(insert_v2.row_count == 2 && insert_v2.values_per_row == 2);
    assert(insert_v2.base.value_count == 4);
    assert(insert_v2.base.values[0].kind == PRDB_SDK_SQL_INSERT_PARAMETER &&
           insert_v2.base.values[0].parameter_index == 1);
    assert(insert_v2.base.values[1].kind == PRDB_SDK_SQL_INSERT_NULL);
    assert(insert_v2.base.values[2].kind == PRDB_SDK_SQL_INSERT_DEFAULT);
    assert(insert_v2.base.values[3].kind == PRDB_SDK_SQL_INSERT_PARAMETER &&
           insert_v2.base.values[3].parameter_index == 3);
    uint32_t saved_row_count = insert_v2.row_count;
    assert(prdb_sdk_sql_parse_insert_v2(
               mismatched_multi_insert_sql,
               sizeof(mismatched_multi_insert_sql) - 1, &insert_v2,
               sizeof(insert_v2), &error_offset) == PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(insert_v2.row_count == saved_row_count);
    assert(prdb_sdk_sql_parse_insert_v1(
               multi_insert_sql, sizeof(multi_insert_sql) - 1, &insert,
               sizeof(insert), &error_offset) == PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(prdb_sdk_sql_parse_insert_v2(
               single_tuple_sql, sizeof(single_tuple_sql) - 1, &insert_v2,
               sizeof(insert_v2), &error_offset) == PRDB_SDK_SQL_OK);
    assert(insert_v2.row_count == 1 && insert_v2.values_per_row == 3);
    uint8_t too_many_rows[2048] = {0};
    uint32_t too_many_rows_len = sizeof(insert_prefix) - 1;
    memcpy(too_many_rows, insert_prefix, too_many_rows_len);
    for (uint32_t row = 0; row < PRDB_SDK_SQL_MAX_INSERT_ROWS_V2 + 1; ++row) {
        if (row != 0) too_many_rows[too_many_rows_len++] = ',';
        memcpy(too_many_rows + too_many_rows_len, "(NULL)", 6);
        too_many_rows_len += 6;
    }
    too_many_rows[too_many_rows_len++] = ';';
    uint32_t completed_row_count = insert_v2.row_count;
    assert(prdb_sdk_sql_parse_insert_v2(
               too_many_rows, too_many_rows_len, &insert_v2,
               sizeof(insert_v2), &error_offset) ==
           PRDB_SDK_SQL_LIMIT_EXCEEDED);
    assert(insert_v2.row_count == completed_row_count);

    assert(prdb_sdk_sql_parse_update_v1(
               update_sql, sizeof(update_sql) - 1, &update, sizeof(update),
               &error_offset) == PRDB_SDK_SQL_OK);
    assert(update.abi_version == PRDB_SDK_SQL_DML_ABI_V1_VERSION);
    assert(update.assignment_count == 2);
    assert(update.assignments[0].column.length == 2 &&
           memcmp(update.sql + update.assignments[0].column.offset, "id", 2) == 0);
    assert(update.assignments[0].kind == PRDB_SDK_SQL_UPDATE_PARAMETER &&
           update.assignments[0].parameter_index == 1);
    assert(update.assignments[1].kind == PRDB_SDK_SQL_UPDATE_NULL);
    assert(update.predicate_statement.projection_count == 2 &&
           update.predicate_statement.projection[1].length == 5);
    assert(update.predicate_statement.predicate_count == 3);
    uint32_t saved_assignments = update.assignment_count;
    assert(prdb_sdk_sql_parse_update_v1(
               duplicate_set_sql, sizeof(duplicate_set_sql) - 1, &update,
               sizeof(update), &error_offset) == PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(update.assignment_count == saved_assignments);
    assert(prdb_sdk_sql_parse_update_v1(
               malformed_update_sql, sizeof(malformed_update_sql) - 1,
               &update, sizeof(update), &error_offset) ==
           PRDB_SDK_SQL_SYNTAX_ERROR);

    assert(prdb_sdk_sql_parse_delete_v1(
               delete_sql, sizeof(delete_sql) - 1, &delete_statement,
               sizeof(delete_statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(delete_statement.abi_version == PRDB_SDK_SQL_DML_ABI_V1_VERSION);
    assert(delete_statement.predicate_statement.projection_star == 1);
    assert(delete_statement.predicate_statement.predicate_count == 3);
    assert(prdb_sdk_sql_parse_delete_v1(
               missing_delete_where_sql, sizeof(missing_delete_where_sql) - 1,
               &delete_statement, sizeof(delete_statement), &error_offset) ==
           PRDB_SDK_SQL_SYNTAX_ERROR);
    return 0;
}
