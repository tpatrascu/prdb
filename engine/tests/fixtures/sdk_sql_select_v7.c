#include "prdb_sdk_sql_v1.h"

#include <assert.h>
#include <stddef.h>

int main(void) {
    static const uint8_t relation[] = "employees";
    static const uint8_t department[] = "department";
    static const uint8_t amount[] = "amount";
    static const uint8_t active[] = "active";
    static const prdb_sdk_sql_column_shape_v1 columns[] = {
        {department, 10, 0, PRDB_SDK_VALUE_TYPE_TEXT, 0, 0},
        {amount, 6, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 12, 3},
        {active, 6, 2, PRDB_SDK_VALUE_TYPE_BOOLEAN, 0, 0}};
    static const uint8_t avg_query[] =
        "SELECT department, AVG(amount) FROM employees "
        "WHERE active = $1 GROUP BY department LIMIT 4";
    static const uint8_t invalid_type_query[] =
        "SELECT department, AVG(active) FROM employees GROUP BY department";
    prdb_sdk_select_v1 statement = {0};
    prdb_sdk_select_binding_v1 binding = {0};

    assert(prdb_sdk_sql_parse_select_v1(
               avg_query, sizeof(avg_query) - 1, &statement,
               sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V1_VERSION &&
           statement.aggregate_kind == PRDB_SDK_SQL_AGGREGATE_AVG &&
           statement.predicate_count == 1 &&
           statement.has_limit == 1 && statement.limit == 4);
    assert(prdb_sdk_sql_bind_select_v1(
               &statement, relation, sizeof(relation) - 1, columns, 3,
               &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.abi_version == PRDB_SDK_SQL_ABI_V1_VERSION &&
           binding.aggregate_kind == PRDB_SDK_SQL_AGGREGATE_AVG &&
           binding.group_by_column.ordinal == 0 &&
           binding.aggregate_column.ordinal == 1 &&
           binding.aggregate_column.type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
           binding.aggregate_column.decimal_precision == 12 &&
           binding.aggregate_column.decimal_scale == 3);

    {
        prdb_sdk_select_v1 invalid = {0};
        prdb_sdk_select_binding_v1 unchanged = {0};
        unchanged.struct_size = UINT32_C(0x12345678);
        assert(prdb_sdk_sql_parse_select_v1(
                   invalid_type_query, sizeof(invalid_type_query) - 1,
                   &invalid, sizeof(invalid), NULL) == PRDB_SDK_SQL_OK);
        assert(prdb_sdk_sql_bind_select_v1(
                   &invalid, relation, sizeof(relation) - 1, columns, 3,
                   &unchanged, sizeof(unchanged)) ==
               PRDB_SDK_SQL_BIND_INVALID_AGGREGATE_TYPE);
        assert(unchanged.struct_size == UINT32_C(0x12345678));
    }
    return 0;
}
