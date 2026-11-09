#include "prdb_sdk_sql_v10.h"

#include <assert.h>
#include <string.h>

int main(void) {
    static const uint8_t key_name[] = "k";
    static const uint8_t amount_name[] = "amount";
    static const uint8_t relation_name[] = "records";
    static const uint8_t key_sql[] =
        "SELECT k, COUNT(*) FROM records GROUP BY k "
        "HAVING COUNT(*) >= $1 ORDER BY k DESC LIMIT 3";
    static const uint8_t aggregate_sql[] =
        "SELECT k, SUM(amount) FROM records GROUP BY k "
        "ORDER BY SUM(amount) ASC";
    static const uint8_t average_sql[] =
        "SELECT k, AVG(amount) FROM records GROUP BY k "
        "HAVING AVG(amount) > $1 ORDER BY AVG(amount) DESC";
    static const uint8_t mismatch_sql[] =
        "SELECT k, SUM(amount) FROM records GROUP BY k "
        "ORDER BY MAX(amount)";
    static const uint8_t arbitrary_sql[] =
        "SELECT k, COUNT(*) FROM records GROUP BY k ORDER BY amount";
    static const uint8_t missing_direction_sql[] =
        "SELECT k, COUNT(*) FROM records GROUP BY k ORDER BY k SIDEWAYS";
    prdb_sdk_sql_column_shape_v2 columns[2] = {
        {key_name, sizeof(key_name) - 1, 0,
         PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {amount_name, sizeof(amount_name) - 1, 1,
         PRDB_SDK_VALUE_TYPE_DECIMAL, 8, 2}};
    prdb_sdk_select_v10 statement = {0};
    prdb_sdk_select_binding_v10 binding = {0};
    uint32_t error_offset = 0;

    assert(prdb_sdk_sql_parse_select_v10(
               key_sql, sizeof(key_sql) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V10_VERSION);
    assert(statement.base.has_having == 1);
    assert(statement.has_order_by == 1);
    assert(statement.order_kind == PRDB_SDK_SQL_V10_ORDER_GROUP_KEY);
    assert(statement.order_direction == PRDB_SDK_SQL_V10_ORDER_DESCENDING);
    assert(statement.base.base.base.has_limit == 1 &&
           statement.base.base.base.limit == 3);
    assert(prdb_sdk_sql_bind_select_v10(
               &statement, relation_name, sizeof(relation_name) - 1, columns,
               2, &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.order_type_code == PRDB_SDK_VALUE_TYPE_INTEGER);
    assert(binding.has_order_by == 1 &&
           binding.order_kind == PRDB_SDK_SQL_V10_ORDER_GROUP_KEY &&
           binding.order_direction == PRDB_SDK_SQL_V10_ORDER_DESCENDING);
    assert(binding.order_decimal_precision == 0 &&
           binding.order_decimal_scale == 0);

    memset(&statement, 0, sizeof(statement));
    assert(prdb_sdk_sql_parse_select_v10(
               aggregate_sql, sizeof(aggregate_sql) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.order_kind == PRDB_SDK_SQL_V10_ORDER_AGGREGATE);
    assert(statement.order_direction == PRDB_SDK_SQL_V10_ORDER_ASCENDING);
    assert(prdb_sdk_sql_bind_select_v10(
               &statement, relation_name, sizeof(relation_name) - 1, columns,
               2, &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.order_type_code == PRDB_SDK_VALUE_TYPE_DECIMAL);
    assert(binding.order_kind == PRDB_SDK_SQL_V10_ORDER_AGGREGATE &&
           binding.order_direction == PRDB_SDK_SQL_V10_ORDER_ASCENDING);
    assert(binding.order_decimal_precision == 38 &&
           binding.order_decimal_scale == 2);

    memset(&statement, 0, sizeof(statement));
    assert(prdb_sdk_sql_parse_select_v10(
               average_sql, sizeof(average_sql) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_select_v10(
               &statement, relation_name, sizeof(relation_name) - 1, columns,
               2, &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.order_type_code == PRDB_SDK_VALUE_TYPE_DECIMAL);
    assert(binding.order_decimal_precision == 38 &&
           binding.order_decimal_scale == 8);

    memset(&statement, 0xA5, sizeof(statement));
    prdb_sdk_select_v10 previous = statement;
    assert(prdb_sdk_sql_parse_select_v10(
               mismatch_sql, sizeof(mismatch_sql) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(memcmp(&statement, &previous, sizeof(statement)) == 0);
    assert(prdb_sdk_sql_parse_select_v10(
               arbitrary_sql, sizeof(arbitrary_sql) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(prdb_sdk_sql_parse_select_v10(
               missing_direction_sql, sizeof(missing_direction_sql) - 1,
               &statement, sizeof(statement), &error_offset) ==
           PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(prdb_sdk_sql_parse_select_v9(
               key_sql, sizeof(key_sql) - 1, &statement.base,
               sizeof(statement.base), &error_offset) ==
           PRDB_SDK_SQL_SYNTAX_ERROR);
    return 0;
}
