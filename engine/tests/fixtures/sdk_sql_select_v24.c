#include "prdb_sdk_sql_v1.h"

#include <assert.h>
#include <stddef.h>

int main(void) {
    static const uint8_t sql[] =
        "SELECT id, SUM(amount) FROM records WHERE id >= $1 GROUP BY id "
        "HAVING SUM(amount) IS NULL OR COUNT(*) >= 2 "
        "ORDER BY SUM(amount) DESC NULLS LAST, id ASC LIMIT 2";
    static const uint8_t relation[] = "records";
    static const prdb_sdk_sql_column_shape_v1 columns[] = {
        {(const uint8_t *)"id", 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {(const uint8_t *)"amount", 6, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 8, 2}};
    prdb_sdk_select_v1 statement = {0};
    prdb_sdk_select_binding_v1 binding = {0};

    assert(prdb_sdk_sql_parse_select_v1(
        sql, sizeof(sql) - 1, &statement, sizeof(statement), NULL) ==
        PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_select_v1(
        &statement, relation, sizeof(relation) - 1, columns, 2, &binding,
        sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V1_VERSION);
    assert(binding.abi_version == PRDB_SDK_SQL_ABI_V1_VERSION);
    assert(binding.group_key_count == 1);
    assert(binding.order_count == 2);
    assert(binding.having_count_mask == (UINT64_C(1) << 1));
    static const uint8_t decimal_sql[] =
        "SELECT right_key, SUM(decimal_value) FROM decimal_grouped_records "
        "WHERE (decimal_value IS NULL OR decimal_value >= $1) GROUP BY right_key "
        "HAVING SUM(decimal_value) IS NULL OR COUNT(*) >= 2 "
        "ORDER BY SUM(decimal_value) DESC NULLS LAST, right_key LIMIT 1";
    static const uint8_t decimal_relation[] = "decimal_grouped_records";
    static const prdb_sdk_sql_column_shape_v1 decimal_columns[] = {
        {(const uint8_t *)"left_key", 8, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {(const uint8_t *)"right_key", 9, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {(const uint8_t *)"decimal_value", 13, 2, PRDB_SDK_VALUE_TYPE_DECIMAL, 4, 2}};
    assert(prdb_sdk_sql_parse_select_v1(
        decimal_sql, sizeof(decimal_sql) - 1, &statement, sizeof(statement),
        NULL) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_select_v1(
        &statement, decimal_relation, sizeof(decimal_relation) - 1,
        decimal_columns, 3, &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    {
        static const uint8_t count_column_sql[] =
            "SELECT id, COUNT(score) FROM records GROUP BY id "
            "HAVING COUNT(score) >= $1 "
            "ORDER BY COUNT(score) DESC LIMIT 2";
        static const prdb_sdk_sql_column_shape_v1 count_columns[] = {
            {(const uint8_t *)"id", 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
            {(const uint8_t *)"score", 5, 1, PRDB_SDK_VALUE_TYPE_BOOLEAN, 0, 0}};
        assert(prdb_sdk_sql_parse_select_v1(
                   count_column_sql, sizeof(count_column_sql) - 1,
                   &statement, sizeof(statement), NULL) ==
               PRDB_SDK_SQL_OK);
        assert(statement.aggregate_kind ==
               PRDB_SDK_SQL_AGGREGATE_COUNT_COLUMN);
        assert(prdb_sdk_sql_bind_select_v1(
                   &statement, relation, sizeof(relation) - 1, count_columns,
                   2, &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
        assert(binding.aggregate_column.ordinal ==
               1);
        assert(binding.having_type_code ==
               PRDB_SDK_VALUE_TYPE_BIGINT);
        assert(binding.order_count == 1);
        assert(binding.order_items[0].type_code ==
               PRDB_SDK_VALUE_TYPE_BIGINT);
    }
    return 0;
}
