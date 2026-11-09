#include "prdb_sdk_sql_v5.h"

#include <assert.h>
#include <string.h>

int main(void) {
    static const uint8_t relation_name[] = "employees";
    static const uint8_t department[] = "department";
    static const uint8_t salary[] = "salary";
    static const uint8_t active[] = "active";
    static const prdb_sdk_sql_column_shape_v2 columns[] = {
        {department, sizeof(department) - 1, 0, PRDB_SDK_VALUE_TYPE_TEXT, 0, 0},
        {salary, sizeof(salary) - 1, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 10, 2},
        {active, sizeof(active) - 1, 2, PRDB_SDK_VALUE_TYPE_BOOLEAN, 0, 0}};
    static const uint8_t min_query[] =
        "SELECT department, MIN(salary) FROM employees "
        "WHERE active = $1 GROUP BY department LIMIT 7;";
    static const uint8_t max_query[] =
        "SELECT department, MAX(salary) FROM employees GROUP BY department";
    static const uint8_t count_query[] =
        "SELECT department, COUNT(*) FROM employees GROUP BY department";
    static const uint8_t *invalid_queries[] = {
        (const uint8_t *)"SELECT department, SUM(salary) FROM employees GROUP BY department",
        (const uint8_t *)"SELECT department, MIN(*) FROM employees GROUP BY department",
        (const uint8_t *)"SELECT department, COUNT(salary) FROM employees GROUP BY department",
        (const uint8_t *)"SELECT department, MIN(salary, active) FROM employees GROUP BY department",
        (const uint8_t *)"SELECT department, MIN(salary) FROM employees GROUP BY active",
        (const uint8_t *)"SELECT department, MAX(salary) FROM employees GROUP BY department LIMIT 1 GROUP BY department"};
    static const uint64_t invalid_lengths[] = {
        sizeof("SELECT department, SUM(salary) FROM employees GROUP BY department") - 1,
        sizeof("SELECT department, MIN(*) FROM employees GROUP BY department") - 1,
        sizeof("SELECT department, COUNT(salary) FROM employees GROUP BY department") - 1,
        sizeof("SELECT department, MIN(salary, active) FROM employees GROUP BY department") - 1,
        sizeof("SELECT department, MIN(salary) FROM employees GROUP BY active") - 1,
        sizeof("SELECT department, MAX(salary) FROM employees GROUP BY department LIMIT 1 GROUP BY department") - 1};
    prdb_sdk_select_v5 statement = {0};
    prdb_sdk_select_binding_v5 binding = {0};
    uint32_t error_offset = 0;

    assert(prdb_sdk_sql_parse_select_v5(
               min_query, sizeof(min_query) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V5_VERSION);
    assert(statement.aggregate_kind == PRDB_SDK_SQL_V5_AGGREGATE_MIN);
    assert(statement.base.projection_count == 1 &&
           statement.base.predicate_count == 1 &&
           statement.base.has_limit == 1 && statement.base.limit == 7);
    assert(statement.aggregate_column.length == sizeof(salary) - 1);
    assert(memcmp(min_query + statement.aggregate_column.offset, salary,
                  sizeof(salary) - 1) == 0);
    assert(prdb_sdk_sql_bind_select_v5(
               &statement, relation_name, sizeof(relation_name) - 1,
               columns, 3, &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.group_by_column.ordinal == 0 &&
           binding.aggregate_column.ordinal == 1 &&
           binding.aggregate_column.type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
           binding.aggregate_column.decimal_precision == 10 &&
           binding.aggregate_column.decimal_scale == 2);

    assert(prdb_sdk_sql_parse_select_v5(
               max_query, sizeof(max_query) - 1, &statement,
               sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    assert(statement.aggregate_kind == PRDB_SDK_SQL_V5_AGGREGATE_MAX);
    assert(prdb_sdk_sql_bind_select_v5(
               &statement, relation_name, sizeof(relation_name) - 1,
               columns, 3, &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.aggregate_column.ordinal == 1);

    assert(prdb_sdk_sql_parse_select_v5(
               count_query, sizeof(count_query) - 1, &statement,
               sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    assert(statement.aggregate_kind ==
               PRDB_SDK_SQL_V5_AGGREGATE_COUNT_STAR &&
           statement.aggregate_column.length == 0);
    assert(prdb_sdk_sql_bind_select_v5(
               &statement, relation_name, sizeof(relation_name) - 1,
               columns, 3, &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.aggregate_kind ==
               PRDB_SDK_SQL_V5_AGGREGATE_COUNT_STAR &&
           binding.aggregate_column.type_code == 0);

    for (uint32_t index = 0;
         index < sizeof(invalid_queries) / sizeof(invalid_queries[0]);
         ++index) {
        prdb_sdk_select_v5 unchanged = {0};
        unchanged.struct_size = UINT32_C(0x12345678);
        assert(prdb_sdk_sql_parse_select_v5(
                   invalid_queries[index], invalid_lengths[index], &unchanged,
                   sizeof(unchanged), &error_offset) != PRDB_SDK_SQL_OK);
        assert(unchanged.struct_size == UINT32_C(0x12345678));
    }

    {
        prdb_sdk_select_v5 missing = {0};
        prdb_sdk_select_binding_v5 unchanged = {0};
        static const uint8_t missing_query[] =
            "SELECT department, MIN(missing) FROM employees GROUP BY department";
        unchanged.struct_size = UINT32_C(0x87654321);
        assert(prdb_sdk_sql_parse_select_v5(
                   missing_query, sizeof(missing_query) - 1, &missing,
                   sizeof(missing), NULL) == PRDB_SDK_SQL_OK);
        assert(prdb_sdk_sql_bind_select_v5(
                   &missing, relation_name, sizeof(relation_name) - 1,
                   columns, 3, &unchanged, sizeof(unchanged)) ==
               PRDB_SDK_SQL_BIND_COLUMN_NOT_FOUND);
        assert(unchanged.struct_size == UINT32_C(0x87654321));
    }

    {
        prdb_sdk_select_v5 malformed = {0};
        prdb_sdk_select_binding_v5 unchanged = {0};
        assert(prdb_sdk_sql_parse_select_v5(
                   min_query, sizeof(min_query) - 1, &malformed,
                   sizeof(malformed), NULL) == PRDB_SDK_SQL_OK);
        malformed.aggregate_column.length = 0;
        unchanged.struct_size = UINT32_C(0x87654321);
        assert(prdb_sdk_sql_bind_select_v5(
                   &malformed, relation_name, sizeof(relation_name) - 1,
                   columns, 3, &unchanged, sizeof(unchanged)) ==
               PRDB_SDK_SQL_BIND_INVALID_STATEMENT);
        assert(unchanged.struct_size == UINT32_C(0x87654321));
    }
    return 0;
}
