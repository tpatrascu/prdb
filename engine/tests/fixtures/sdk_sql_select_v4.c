#include "prdb_sdk_sql_v4.h"

#include <assert.h>
#include <string.h>

int main(void) {
    static const uint8_t query[] =
        "SELECT Department, COUNT(*) FROM Employees "
        "WHERE active = $1 GROUP BY department LIMIT 5;";
    static const uint8_t relation_name[] = "employees";
    static const uint8_t department[] = "department";
    static const uint8_t active[] = "active";
    static const uint8_t missing_columns_key[] = "missing";
    static const prdb_sdk_sql_column_shape_v2 columns[] = {
        {department, sizeof(department) - 1, 0, PRDB_SDK_VALUE_TYPE_TEXT, 0, 0},
        {active, sizeof(active) - 1, 1, PRDB_SDK_VALUE_TYPE_BOOLEAN, 0, 0}};
    static const prdb_sdk_sql_column_shape_v2 missing_key_columns[] = {
        {missing_columns_key, sizeof(missing_columns_key) - 1, 0,
         PRDB_SDK_VALUE_TYPE_TEXT, 0, 0},
        {active, sizeof(active) - 1, 1, PRDB_SDK_VALUE_TYPE_BOOLEAN, 0, 0}};
    static const uint8_t *invalid_queries[] = {
        (const uint8_t *)"SELECT Department, COUNT(salary) FROM Employees GROUP BY Department",
        (const uint8_t *)"SELECT Department, COUNT(*) FROM Employees",
        (const uint8_t *)"SELECT Department, COUNT(*) FROM Employees GROUP BY active",
        (const uint8_t *)"SELECT Department, COUNT(*) FROM Employees GROUP BY Department, active",
        (const uint8_t *)"SELECT Department, COUNT(*) FROM Employees LIMIT 5 GROUP BY Department",
        (const uint8_t *)"SELECT COUNT(*) FROM Employees GROUP BY Department",
        (const uint8_t *)"SELECT Department, COUNT(*) FROM Employees GROUP BY Department; SELECT Department, COUNT(*) FROM Employees GROUP BY Department"};
    static const uint64_t invalid_lengths[] = {
        sizeof("SELECT Department, COUNT(salary) FROM Employees GROUP BY Department") - 1,
        sizeof("SELECT Department, COUNT(*) FROM Employees") - 1,
        sizeof("SELECT Department, COUNT(*) FROM Employees GROUP BY active") - 1,
        sizeof("SELECT Department, COUNT(*) FROM Employees GROUP BY Department, active") - 1,
        sizeof("SELECT Department, COUNT(*) FROM Employees LIMIT 5 GROUP BY Department") - 1,
        sizeof("SELECT COUNT(*) FROM Employees GROUP BY Department") - 1,
        sizeof("SELECT Department, COUNT(*) FROM Employees GROUP BY Department; SELECT Department, COUNT(*) FROM Employees GROUP BY Department") - 1};
    prdb_sdk_select_v4 statement = {0};
    prdb_sdk_select_binding_v4 binding = {0};
    uint32_t error_offset = 0;

    assert(prdb_sdk_sql_parse_select_v4(query, sizeof(query) - 1, &statement,
                                        sizeof(statement), &error_offset) ==
           PRDB_SDK_SQL_OK);
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V4_VERSION);
    assert(statement.has_group_by == 1);
    assert(statement.aggregate_kind ==
           PRDB_SDK_SQL_AGGREGATE_COUNT_STAR);
    assert(statement.base.projection_count == 1);
    assert(statement.base.predicate_count == 1);
    assert(statement.base.has_limit == 1 && statement.base.limit == 5);
    assert(statement.group_by_column.length == sizeof(department) - 1);
    assert(memcmp(query + statement.group_by_column.offset, department,
                  sizeof(department) - 1) == 0);
    assert(memcmp(query + statement.base.projection[0].offset,
                  "Department", sizeof("Department") - 1) == 0);

    assert(prdb_sdk_sql_bind_select_v4(
               &statement, relation_name, sizeof(relation_name) - 1, columns,
               2, &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.abi_version == PRDB_SDK_SQL_ABI_V4_VERSION);
    assert(binding.base.projection_count == 1);
    assert(binding.base.projection_ordinals[0] == 0);
    assert(binding.has_group_by == 1);
    assert(binding.group_by_column.ordinal == 0);
    assert(binding.group_by_column.type_code == PRDB_SDK_VALUE_TYPE_TEXT);
    assert(binding.aggregate_kind ==
           PRDB_SDK_SQL_AGGREGATE_COUNT_STAR);

    for (uint32_t index = 0;
         index < sizeof(invalid_queries) / sizeof(invalid_queries[0]);
         ++index) {
        prdb_sdk_select_v4 unchanged = {0};
        unchanged.struct_size = UINT32_C(0x12345678);
        assert(prdb_sdk_sql_parse_select_v4(
                   invalid_queries[index], invalid_lengths[index], &unchanged,
                   sizeof(unchanged), &error_offset) != PRDB_SDK_SQL_OK);
        assert(unchanged.struct_size == UINT32_C(0x12345678));
    }

    {
        prdb_sdk_select_binding_v4 unchanged = {0};
        unchanged.struct_size = UINT32_C(0x87654321);
        assert(prdb_sdk_sql_bind_select_v4(
                   &statement, relation_name, sizeof(relation_name) - 1,
                   missing_key_columns, 2, &unchanged,
                   sizeof(unchanged)) == PRDB_SDK_SQL_BIND_COLUMN_NOT_FOUND);
        assert(unchanged.struct_size == UINT32_C(0x87654321));
        {
            prdb_sdk_select_v4 malformed = statement;
            malformed.aggregate_kind = 0;
            assert(prdb_sdk_sql_bind_select_v4(
                       &malformed, relation_name, sizeof(relation_name) - 1,
                       columns, 2, &unchanged, sizeof(unchanged)) ==
                   PRDB_SDK_SQL_BIND_INVALID_STATEMENT);
            assert(unchanged.struct_size == UINT32_C(0x87654321));
        }
    }
    return 0;
}
