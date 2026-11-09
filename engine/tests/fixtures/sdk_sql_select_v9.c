#include "prdb_sdk_sql_v9.h"

#include <assert.h>
#include <string.h>

int main(void) {
    static const uint8_t key_name[] = "k";
    static const uint8_t amount_name[] = "amount";
    static const uint8_t relation_name[] = "records";
    static const uint8_t count_sql[] =
        "SELECT k, COUNT(*) FROM records GROUP BY k "
        "HAVING COUNT(*) >= $2 LIMIT 3";
    static const uint8_t sum_sql[] =
        "SELECT k, SUM(amount) FROM records GROUP BY k "
        "HAVING SUM(amount) <> $1";
    static const uint8_t avg_sql[] =
        "SELECT k, AVG(amount) FROM records GROUP BY k "
        "HAVING AVG(amount) < $1";
    static const uint8_t mismatch_sql[] =
        "SELECT k, COUNT(*) FROM records GROUP BY k "
        "HAVING SUM(k) > $1";
    static const uint8_t missing_parameter_sql[] =
        "SELECT k, COUNT(*) FROM records GROUP BY k HAVING COUNT(*) >= 0";
    static const uint8_t zero_parameter_sql[] =
        "SELECT k, COUNT(*) FROM records GROUP BY k HAVING COUNT(*) >= $0";
    static const uint8_t unsupported_operator_sql[] =
        "SELECT k, COUNT(*) FROM records GROUP BY k "
        "HAVING COUNT(*) IS $1";
    prdb_sdk_sql_column_shape_v2 columns[2] = {
        {key_name, sizeof(key_name) - 1, 0,
         PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {amount_name, sizeof(amount_name) - 1, 1,
         PRDB_SDK_VALUE_TYPE_DECIMAL, 8, 2}};
    prdb_sdk_select_v9 statement = {0};
    prdb_sdk_select_binding_v9 binding = {0};
    uint32_t error_offset = 0;

    assert(prdb_sdk_sql_parse_select_v9(
               count_sql, sizeof(count_sql) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.struct_size == sizeof(statement));
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V9_VERSION);
    assert(statement.base.aggregate_kind ==
           PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR);
    assert(statement.base.base.has_limit == 1 &&
           statement.base.base.limit == 3);
    assert(statement.has_having == 1);
    assert(statement.having_comparison == PRDB_SDK_SQL_COMPARE_GREATER_EQUAL);
    assert(statement.having_parameter_index == 2);
    assert(prdb_sdk_sql_bind_select_v9(
               &statement, relation_name, sizeof(relation_name) - 1, columns,
               2, &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.having_type_code == PRDB_SDK_VALUE_TYPE_BIGINT);
    assert(binding.having_decimal_precision == 0 &&
           binding.having_decimal_scale == 0);

    memset(&statement, 0, sizeof(statement));
    assert(prdb_sdk_sql_parse_select_v9(
               sum_sql, sizeof(sum_sql) - 1, &statement, sizeof(statement),
               &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.base.aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_SUM);
    assert(statement.having_comparison == PRDB_SDK_SQL_COMPARE_NOT_EQUAL);
    assert(statement.having_parameter_index == 1);
    assert(prdb_sdk_sql_bind_select_v9(
               &statement, relation_name, sizeof(relation_name) - 1, columns,
               2, &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.having_type_code == PRDB_SDK_VALUE_TYPE_DECIMAL);
    assert(binding.having_decimal_precision == 38 &&
           binding.having_decimal_scale == 2);

    memset(&statement, 0, sizeof(statement));
    assert(prdb_sdk_sql_parse_select_v9(
               avg_sql, sizeof(avg_sql) - 1, &statement, sizeof(statement),
               &error_offset) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_select_v9(
               &statement, relation_name, sizeof(relation_name) - 1, columns,
               2, &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.having_type_code == PRDB_SDK_VALUE_TYPE_DECIMAL);
    assert(binding.having_decimal_precision == 38 &&
           binding.having_decimal_scale == 8);

    memset(&statement, 0xA5, sizeof(statement));
    prdb_sdk_select_v9 previous = statement;
    assert(prdb_sdk_sql_parse_select_v9(
               mismatch_sql, sizeof(mismatch_sql) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(memcmp(&statement, &previous, sizeof(statement)) == 0);
    assert(prdb_sdk_sql_parse_select_v9(
               missing_parameter_sql, sizeof(missing_parameter_sql) - 1,
               &statement, sizeof(statement), &error_offset) ==
           PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(prdb_sdk_sql_parse_select_v9(
               zero_parameter_sql, sizeof(zero_parameter_sql) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(prdb_sdk_sql_parse_select_v9(
               unsupported_operator_sql, sizeof(unsupported_operator_sql) - 1,
               &statement, sizeof(statement), &error_offset) ==
           PRDB_SDK_SQL_SYNTAX_ERROR);
    return 0;
}
