#include "prdb_sdk_sql_v1.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

int main(void) {
    static const uint8_t query[] =
        "SELECT name,department_id FROM employees "
        "WHERE department_id >= $2 LIMIT 100;";
    static const uint8_t star_query[] = "select * from Employees limit 0";
    static const uint8_t is_null_query[] =
        "SELECT nickname FROM people WHERE nickname IS NULL";
    static const uint8_t is_not_null_query[] =
        "SELECT nickname FROM people WHERE nickname IS NOT NULL";
    static const uint8_t malformed_is_null_query[] =
        "SELECT nickname FROM people WHERE nickname IS MAYBE";
    static const uint8_t unsupported[] = "DELETE FROM employees";
    static const uint8_t malformed[] = "SELECT FROM employees";
    static const uint8_t empty_query[] = "";
    static const uint8_t oversized_limit[] =
        "SELECT id FROM employees LIMIT 1000001";
    static const uint8_t zero_parameter[] =
        "SELECT id FROM employees WHERE id = $0";
    static const uint8_t too_large_parameter[] =
        "SELECT id FROM employees WHERE id = $65536";
    static const uint8_t second_statement[] =
        "SELECT id FROM employees; SELECT id FROM employees";
    static uint8_t too_long[PRDB_SDK_SQL_MAX_QUERY_BYTES + 1];
    uint8_t too_many_columns[1024];
    size_t too_many_length = 0;
    prdb_sdk_select_v1 statement;
    uint32_t error_offset = 0;

    assert(prdb_sdk_sql_parse_select_v1(
               query, sizeof(query) - 1, &statement, sizeof(statement),
               &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V1_VERSION);
    assert(statement.projection_count == 2);
    assert(statement.projection_star == 0);
    assert(statement.projection[0].length == 4);
    assert(memcmp(statement.sql + statement.projection[0].offset, "name", 4) ==
           0);
    assert(statement.relation.length == 9);
    assert(memcmp(statement.sql + statement.relation.offset, "employees", 9) ==
           0);
    assert(statement.has_where == 1);
    assert(statement.comparison == PRDB_SDK_SQL_COMPARE_GREATER_EQUAL);
    assert(statement.parameter_index == 2);
    assert(statement.has_limit == 1 && statement.limit == 100);

    assert(prdb_sdk_sql_parse_select_v1(
               star_query, sizeof(star_query) - 1, &statement, sizeof(statement),
               &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.projection_star == 1 && statement.projection_count == 0);
    assert(statement.has_where == 0);
    assert(statement.has_limit == 1 && statement.limit == 0);
    assert(statement.relation.length == 9);
    assert(memcmp(statement.sql + statement.relation.offset, "Employees", 9) ==
           0);

    assert(prdb_sdk_sql_parse_select_v1(
               is_null_query, sizeof(is_null_query) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.has_where == 1);
    assert(statement.comparison == PRDB_SDK_SQL_COMPARE_IS_NULL);
    assert(statement.parameter_index == 0);
    assert(memcmp(statement.sql + statement.where_column.offset, "nickname",
                  8) == 0);
    assert(prdb_sdk_sql_parse_select_v1(
               is_not_null_query, sizeof(is_not_null_query) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.comparison == PRDB_SDK_SQL_COMPARE_IS_NOT_NULL);
    assert(statement.parameter_index == 0);
    assert(prdb_sdk_sql_parse_select_v1(
               malformed_is_null_query, sizeof(malformed_is_null_query) - 1,
               &statement, sizeof(statement), &error_offset) ==
           PRDB_SDK_SQL_SYNTAX_ERROR);

    assert(prdb_sdk_sql_parse_select_v1(
               unsupported, sizeof(unsupported) - 1, &statement,
               sizeof(statement), &error_offset) ==
           PRDB_SDK_SQL_UNSUPPORTED_STATEMENT);
    assert(prdb_sdk_sql_parse_select_v1(
               malformed, sizeof(malformed) - 1, &statement, sizeof(statement),
               &error_offset) == PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(prdb_sdk_sql_parse_select_v1(
               empty_query, 0, &statement, sizeof(statement), &error_offset) ==
           PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(prdb_sdk_sql_parse_select_v1(
               oversized_limit, sizeof(oversized_limit) - 1, &statement,
               sizeof(statement), &error_offset) ==
           PRDB_SDK_SQL_LIMIT_EXCEEDED);
    assert(prdb_sdk_sql_parse_select_v1(
               zero_parameter, sizeof(zero_parameter) - 1, &statement,
               sizeof(statement), &error_offset) ==
           PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(prdb_sdk_sql_parse_select_v1(
               too_large_parameter, sizeof(too_large_parameter) - 1,
               &statement, sizeof(statement), &error_offset) ==
           PRDB_SDK_SQL_LIMIT_EXCEEDED);
    assert(prdb_sdk_sql_parse_select_v1(
               second_statement, sizeof(second_statement) - 1, &statement,
               sizeof(statement), &error_offset) ==
           PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(prdb_sdk_sql_parse_select_v1(
               too_long, sizeof(too_long), &statement, sizeof(statement),
               &error_offset) == PRDB_SDK_SQL_LIMIT_EXCEEDED);

    too_many_length += (size_t)snprintf((char *)too_many_columns + too_many_length,
                                        sizeof(too_many_columns) - too_many_length,
                                        "SELECT ");
    for (size_t index = 0; index < PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS + 1;
         ++index) {
        too_many_length += (size_t)snprintf(
            (char *)too_many_columns + too_many_length,
            sizeof(too_many_columns) - too_many_length, "%sx%zu",
            index == 0 ? "" : ",", index);
    }
    too_many_length += (size_t)snprintf(
        (char *)too_many_columns + too_many_length,
        sizeof(too_many_columns) - too_many_length, " FROM t");
    assert(prdb_sdk_sql_parse_select_v1(
               too_many_columns, too_many_length, &statement,
               sizeof(statement), &error_offset) ==
           PRDB_SDK_SQL_LIMIT_EXCEEDED);
    return 0;
}
