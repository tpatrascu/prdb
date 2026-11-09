#include "prdb_sdk_sql_v11.h"

#include <assert.h>
#include <string.h>

int main(void) {
    static const uint8_t relation[] = "records";
    static const uint8_t sql_last[] =
        "SELECT k, MIN(v) FROM records GROUP BY k ORDER BY MIN(v) DESC NULLS LAST";
    static const uint8_t sql_first[] =
        "SELECT k, COUNT(*) FROM records GROUP BY k ORDER BY k NULLS FIRST LIMIT 2";
    static const uint8_t sql_default[] =
        "SELECT k, COUNT(*) FROM records GROUP BY k ORDER BY COUNT(*) DESC";
    static const uint8_t malformed[] =
        "SELECT k, COUNT(*) FROM records GROUP BY k ORDER BY k NULLS SIDEWAYS";
    static const uint8_t v10_reject[] =
        "SELECT k, COUNT(*) FROM records GROUP BY k ORDER BY k NULLS LAST";
    static const uint8_t k[] = "k", v[] = "v";
    prdb_sdk_sql_column_shape_v2 columns[2] = {
        {k, 1, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {v, 1, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_select_v11 statement = {0};
    prdb_sdk_select_binding_v11 binding = {0};
    uint32_t error_offset = 0;

    assert(prdb_sdk_sql_parse_select_v11(
               sql_last, sizeof(sql_last) - 1, &statement, sizeof(statement),
               &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V11_VERSION);
    assert(statement.null_order == PRDB_SDK_SQL_V11_NULLS_LAST);
    assert(statement.base.order_direction == PRDB_SDK_SQL_V10_ORDER_DESCENDING);
    assert(prdb_sdk_sql_bind_select_v11(
               &statement, relation, sizeof(relation) - 1, columns, 2,
               &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.null_order == PRDB_SDK_SQL_V11_NULLS_LAST);

    memset(&statement, 0, sizeof(statement));
    assert(prdb_sdk_sql_parse_select_v11(
               sql_first, sizeof(sql_first) - 1, &statement, sizeof(statement),
               &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.null_order == PRDB_SDK_SQL_V11_NULLS_FIRST);
    assert(statement.base.base.base.base.has_limit == 1);

    memset(&statement, 0, sizeof(statement));
    assert(prdb_sdk_sql_parse_select_v11(
               sql_default, sizeof(sql_default) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.null_order == PRDB_SDK_SQL_V11_NULL_ORDER_DEFAULT);

    memset(&statement, 0xA5, sizeof(statement));
    prdb_sdk_select_v11 previous = statement;
    assert(prdb_sdk_sql_parse_select_v11(
               malformed, sizeof(malformed) - 1, &statement, sizeof(statement),
               &error_offset) == PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(memcmp(&statement, &previous, sizeof(statement)) == 0);
    prdb_sdk_select_v10 frozen = {0};
    assert(prdb_sdk_sql_parse_select_v10(
               v10_reject, sizeof(v10_reject) - 1, &frozen, sizeof(frozen),
               &error_offset) == PRDB_SDK_SQL_SYNTAX_ERROR);
    return 0;
}
