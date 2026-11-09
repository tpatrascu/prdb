#include "prdb_sdk_sql_v3.h"

#include <assert.h>
#include <string.h>

int main(void) {
    static const uint8_t query[] =
        "SELECT Name FROM Employees WHERE salary >= $1 "
        "ORDER BY salary DESC LIMIT 3;";
    static const uint8_t default_ascending[] =
        "select Name from Employees order by salary";
    static const uint8_t no_order[] = "SELECT Name FROM Employees LIMIT 2";
    static const uint8_t relation_name[] = "employees";
    static const uint8_t name[] = "name";
    static const uint8_t salary[] = "salary";
    static const prdb_sdk_sql_column_shape_v2 name_only_columns[] = {
        {name, sizeof(name) - 1, 0, PRDB_SDK_VALUE_TYPE_TEXT, 0, 0}};
    static const uint8_t *invalid_queries[] = {
        (const uint8_t *)"SELECT Name FROM Employees ORDER BY salary, Name",
        (const uint8_t *)"SELECT Name FROM Employees ORDER BY salary + 1",
        (const uint8_t *)"SELECT Name FROM Employees LIMIT 3 ORDER BY salary",
        (const uint8_t *)"SELECT Name FROM Employees ORDER BY",
        (const uint8_t *)"SELECT Name FROM Employees ORDER BY salary ORDER BY Name",
        (const uint8_t *)"SELECT Name FROM Employees ORDER BY salary DESC ASC",
        (const uint8_t *)"SELECT Name FROM Employees ORDER BY salary; SELECT Name FROM Employees"};
    static const uint64_t invalid_lengths[] = {
        sizeof("SELECT Name FROM Employees ORDER BY salary, Name") - 1,
        sizeof("SELECT Name FROM Employees ORDER BY salary + 1") - 1,
        sizeof("SELECT Name FROM Employees LIMIT 3 ORDER BY salary") - 1,
        sizeof("SELECT Name FROM Employees ORDER BY") - 1,
        sizeof("SELECT Name FROM Employees ORDER BY salary ORDER BY Name") - 1,
        sizeof("SELECT Name FROM Employees ORDER BY salary DESC ASC") - 1,
        sizeof("SELECT Name FROM Employees ORDER BY salary; SELECT Name FROM Employees") - 1};
    static const prdb_sdk_sql_column_shape_v2 columns[] = {
        {name, sizeof(name) - 1, 0, PRDB_SDK_VALUE_TYPE_TEXT, 0, 0},
        {salary, sizeof(salary) - 1, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 10, 2}};
    prdb_sdk_select_v3 statement = {0};
    prdb_sdk_select_v3 ascending = {0};
    prdb_sdk_select_v3 plain = {0};
    prdb_sdk_select_binding_v3 binding = {0};
    prdb_sdk_select_binding_v3 unchanged_binding = {0};
    uint32_t error_offset = 0;

    assert(prdb_sdk_sql_parse_select_v3(query, sizeof(query) - 1, &statement,
                                        sizeof(statement), &error_offset) ==
           PRDB_SDK_SQL_OK);
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V3_VERSION);
    assert(statement.base.sql_len == sizeof(query) - 1);
    assert(statement.base.projection_count == 1);
    assert(statement.base.predicate_count == 1);
    assert(statement.has_order_by == 1);
    assert(statement.order_descending == PRDB_SDK_SQL_ORDER_DESCENDING);
    assert(statement.base.has_limit == 1 && statement.base.limit == 3);
    assert(statement.order_by_column.length == sizeof(salary) - 1);
    assert(memcmp(query + statement.order_by_column.offset, salary,
                  sizeof(salary) - 1) == 0);

    assert(prdb_sdk_sql_bind_select_v3(
               &statement, relation_name, sizeof(relation_name) - 1, columns,
               2, &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.has_order_by == 1);
    assert(binding.order_by_column.ordinal == 1);
    assert(binding.order_by_column.type_code == PRDB_SDK_VALUE_TYPE_DECIMAL);
    assert(binding.order_by_column.decimal_precision == 10);
    assert(binding.order_by_column.decimal_scale == 2);

    unchanged_binding.struct_size = UINT32_C(0x12345678);
    assert(prdb_sdk_sql_bind_select_v3(
               &statement, relation_name, sizeof(relation_name) - 1,
               name_only_columns, 1, &unchanged_binding,
               sizeof(unchanged_binding)) ==
           PRDB_SDK_SQL_BIND_COLUMN_NOT_FOUND);
    assert(unchanged_binding.struct_size == UINT32_C(0x12345678));
    {
        prdb_sdk_select_v3 malformed = statement;
        malformed.has_order_by = 2;
        assert(prdb_sdk_sql_bind_select_v3(
                   &malformed, relation_name, sizeof(relation_name) - 1,
                   columns, 2, &unchanged_binding,
                   sizeof(unchanged_binding)) ==
               PRDB_SDK_SQL_BIND_INVALID_STATEMENT);
        assert(unchanged_binding.struct_size == UINT32_C(0x12345678));
    }

    assert(prdb_sdk_sql_parse_select_v3(
               default_ascending, sizeof(default_ascending) - 1, &ascending,
               sizeof(ascending), &error_offset) == PRDB_SDK_SQL_OK);
    assert(ascending.has_order_by == 1 && ascending.order_descending == 0);
    assert(prdb_sdk_sql_parse_select_v3(no_order, sizeof(no_order) - 1, &plain,
                                        sizeof(plain), &error_offset) ==
           PRDB_SDK_SQL_OK);
    assert(plain.has_order_by == 0 && plain.base.has_limit == 1);

    for (uint32_t index = 0;
         index < sizeof(invalid_queries) / sizeof(invalid_queries[0]);
         ++index) {
        prdb_sdk_select_v3 unchanged = {0};
        unchanged.struct_size = UINT32_C(0x12345678);
        assert(prdb_sdk_sql_parse_select_v3(
                   invalid_queries[index], invalid_lengths[index], &unchanged,
                   sizeof(unchanged), &error_offset) != PRDB_SDK_SQL_OK);
        assert(unchanged.struct_size == UINT32_C(0x12345678));
    }
    return 0;
}
