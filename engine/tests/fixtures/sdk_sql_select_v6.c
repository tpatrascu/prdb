#include "prdb_sdk_sql_v5.h"
#include "prdb_sdk_sql_v6.h"

#include <assert.h>
#include <string.h>

int main(void) {
    static const uint8_t relation[] = "employees";
    static const uint8_t department[] = "department";
    static const uint8_t amount[] = "amount";
    static const uint8_t active[] = "active";
    static const prdb_sdk_sql_column_shape_v2 columns[] = {
        {department, 10, 0, PRDB_SDK_VALUE_TYPE_TEXT, 0, 0},
        {amount, 6, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 12, 3},
        {active, 6, 2, PRDB_SDK_VALUE_TYPE_BOOLEAN, 0, 0}};
    static const uint8_t sum_query[] =
        "SELECT department, SUM(amount) FROM employees "
        "WHERE active = $1 GROUP BY department LIMIT 4";
    static const uint8_t bad_type_query[] =
        "SELECT department, SUM(active) FROM employees GROUP BY department";
    prdb_sdk_select_v6 statement = {0};
    prdb_sdk_select_binding_v6 binding = {0};
    uint32_t error_offset = 0;

    assert(prdb_sdk_sql_parse_select_v6(
               sum_query, sizeof(sum_query) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V6_VERSION &&
           statement.aggregate_kind == PRDB_SDK_SQL_V6_AGGREGATE_SUM &&
           statement.base.predicate_count == 1 &&
           statement.base.has_limit == 1 && statement.base.limit == 4);
    assert(prdb_sdk_sql_bind_select_v6(
               &statement, relation, sizeof(relation) - 1, columns, 3,
               &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.abi_version == PRDB_SDK_SQL_ABI_V6_VERSION &&
           binding.aggregate_kind == PRDB_SDK_SQL_V6_AGGREGATE_SUM &&
           binding.group_by_column.ordinal == 0 &&
           binding.aggregate_column.ordinal == 1 &&
           binding.aggregate_column.type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
           binding.aggregate_column.decimal_precision == 12 &&
           binding.aggregate_column.decimal_scale == 3);

    {
        prdb_sdk_select_v5 frozen = {0};
        assert(prdb_sdk_sql_parse_select_v5(
                   sum_query, sizeof(sum_query) - 1, &frozen,
                   sizeof(frozen), NULL) != PRDB_SDK_SQL_OK);
    }

    {
        prdb_sdk_select_v6 bad_type = {0};
        prdb_sdk_select_binding_v6 unchanged = {0};
        unchanged.struct_size = UINT32_C(0x12345678);
        assert(prdb_sdk_sql_parse_select_v6(
                   bad_type_query, sizeof(bad_type_query) - 1, &bad_type,
                   sizeof(bad_type), NULL) == PRDB_SDK_SQL_OK);
        assert(prdb_sdk_sql_bind_select_v6(
                   &bad_type, relation, sizeof(relation) - 1, columns, 3,
                   &unchanged, sizeof(unchanged)) ==
               PRDB_SDK_SQL_BIND_V6_INVALID_AGGREGATE_TYPE);
        assert(unchanged.struct_size == UINT32_C(0x12345678));
    }

    {
        prdb_sdk_select_v6 malformed = statement;
        prdb_sdk_select_binding_v6 unchanged = {0};
        malformed.aggregate_column.length = 0;
        unchanged.struct_size = UINT32_C(0x87654321);
        assert(prdb_sdk_sql_bind_select_v6(
                   &malformed, relation, sizeof(relation) - 1, columns, 3,
                   &unchanged, sizeof(unchanged)) ==
               PRDB_SDK_SQL_BIND_INVALID_STATEMENT);
        assert(unchanged.struct_size == UINT32_C(0x87654321));
    }
    return 0;
}
