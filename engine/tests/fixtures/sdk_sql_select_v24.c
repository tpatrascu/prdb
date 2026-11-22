#include "prdb_sdk_sql_v24.h"

#include <assert.h>
#include <stddef.h>

int main(void) {
    static const uint8_t sql[] =
        "SELECT id, SUM(amount) FROM records WHERE id >= $1 GROUP BY id "
        "HAVING SUM(amount) IS NULL OR COUNT(*) >= 2 "
        "ORDER BY SUM(amount) DESC NULLS LAST, id ASC LIMIT 2";
    static const uint8_t relation[] = "records";
    static const prdb_sdk_sql_column_shape_v2 columns[] = {
        {(const uint8_t *)"id", 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {(const uint8_t *)"amount", 6, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 8, 2}};
    prdb_sdk_select_v24 statement = {0};
    prdb_sdk_select_binding_v24 binding = {0};
    prdb_sdk_select_v23 legacy = {0};

    assert(prdb_sdk_sql_parse_select_v24(
        sql, sizeof(sql) - 1, &statement, sizeof(statement), NULL) ==
        PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_select_v24(
        &statement, relation, sizeof(relation) - 1, columns, 2, &binding,
        sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V24_VERSION);
    assert(binding.abi_version == PRDB_SDK_SQL_ABI_V24_VERSION);
    assert(binding.base.base.base.base.base.group_key_count == 1);
    assert(binding.base.base.base.order_count == 2);
    assert(binding.having_count_mask == (UINT64_C(1) << 1));
    assert(prdb_sdk_sql_parse_select_v23(
        sql, sizeof(sql) - 1, &legacy, sizeof(legacy), NULL) ==
        PRDB_SDK_SQL_SYNTAX_ERROR);
    legacy = statement;
    legacy.abi_version = PRDB_SDK_SQL_ABI_V23_VERSION;
    assert(prdb_sdk_sql_bind_select_v23(
        &legacy, relation, sizeof(relation) - 1, columns, 2, &binding,
        sizeof(binding)) == PRDB_SDK_SQL_BIND_INVALID_STATEMENT);
    static const uint8_t decimal_sql[] =
        "SELECT right_key, SUM(decimal_value) FROM decimal_grouped_records "
        "WHERE (decimal_value IS NULL OR decimal_value >= $1) GROUP BY right_key "
        "HAVING SUM(decimal_value) IS NULL OR COUNT(*) >= 2 "
        "ORDER BY SUM(decimal_value) DESC NULLS LAST, right_key LIMIT 1";
    static const uint8_t decimal_relation[] = "decimal_grouped_records";
    static const prdb_sdk_sql_column_shape_v2 decimal_columns[] = {
        {(const uint8_t *)"left_key", 8, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {(const uint8_t *)"right_key", 9, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {(const uint8_t *)"decimal_value", 13, 2, PRDB_SDK_VALUE_TYPE_DECIMAL, 4, 2}};
    assert(prdb_sdk_sql_parse_select_v24(
        decimal_sql, sizeof(decimal_sql) - 1, &statement, sizeof(statement),
        NULL) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_select_v24(
        &statement, decimal_relation, sizeof(decimal_relation) - 1,
        decimal_columns, 3, &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    return 0;
}
