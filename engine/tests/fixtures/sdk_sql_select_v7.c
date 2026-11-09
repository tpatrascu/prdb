#include "prdb_sdk_sql_v6.h"
#include "prdb_sdk_sql_v7.h"

#include <assert.h>
#include <stddef.h>

int main(void) {
    static const uint8_t relation[] = "employees";
    static const uint8_t department[] = "department";
    static const uint8_t amount[] = "amount";
    static const uint8_t active[] = "active";
    static const prdb_sdk_sql_column_shape_v2 columns[] = {
        {department, 10, 0, PRDB_SDK_VALUE_TYPE_TEXT, 0, 0},
        {amount, 6, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 12, 3},
        {active, 6, 2, PRDB_SDK_VALUE_TYPE_BOOLEAN, 0, 0}};
    static const uint8_t avg_query[] =
        "SELECT department, AVG(amount) FROM employees "
        "WHERE active = $1 GROUP BY department LIMIT 4";
    static const uint8_t invalid_type_query[] =
        "SELECT department, AVG(active) FROM employees GROUP BY department";
    prdb_sdk_select_v7 statement = {0};
    prdb_sdk_select_binding_v7 binding = {0};

    assert(prdb_sdk_sql_parse_select_v7(
               avg_query, sizeof(avg_query) - 1, &statement,
               sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V7_VERSION &&
           statement.aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_AVG &&
           statement.base.predicate_count == 1 &&
           statement.base.has_limit == 1 && statement.base.limit == 4);
    assert(prdb_sdk_sql_bind_select_v7(
               &statement, relation, sizeof(relation) - 1, columns, 3,
               &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.abi_version == PRDB_SDK_SQL_ABI_V7_VERSION &&
           binding.aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_AVG &&
           binding.group_by_column.ordinal == 0 &&
           binding.aggregate_column.ordinal == 1 &&
           binding.aggregate_column.type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
           binding.aggregate_column.decimal_precision == 12 &&
           binding.aggregate_column.decimal_scale == 3);

    {
        prdb_sdk_select_v6 frozen = {0};
        assert(prdb_sdk_sql_parse_select_v6(
                   avg_query, sizeof(avg_query) - 1, &frozen,
                   sizeof(frozen), NULL) != PRDB_SDK_SQL_OK);
    }

    {
        prdb_sdk_select_v7 invalid = {0};
        prdb_sdk_select_binding_v7 unchanged = {0};
        unchanged.struct_size = UINT32_C(0x12345678);
        assert(prdb_sdk_sql_parse_select_v7(
                   invalid_type_query, sizeof(invalid_type_query) - 1,
                   &invalid, sizeof(invalid), NULL) == PRDB_SDK_SQL_OK);
        assert(prdb_sdk_sql_bind_select_v7(
                   &invalid, relation, sizeof(relation) - 1, columns, 3,
                   &unchanged, sizeof(unchanged)) ==
               PRDB_SDK_SQL_BIND_V7_INVALID_AGGREGATE_TYPE);
        assert(unchanged.struct_size == UINT32_C(0x12345678));
    }
    return 0;
}
