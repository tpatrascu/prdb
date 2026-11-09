#include "prdb_sdk_sql_v8.h"
#include "prdb_sdk_sql_v7.h"

#include <assert.h>
#include <string.h>

static prdb_sdk_sql_column_shape_v2 column(const char *name,
                                          uint32_t ordinal,
                                          uint32_t type_code,
                                          uint8_t precision,
                                          uint8_t scale) {
    return (prdb_sdk_sql_column_shape_v2){
        (const uint8_t *)name, (uint32_t)strlen(name), ordinal, type_code,
        precision, scale};
}

int main(void) {
    static const uint8_t sql[] =
        "SELECT accounts.name, orders.amount FROM accounts INNER JOIN "
        "orders ON accounts.account_id = orders.account_id LIMIT 4;";
    static const uint8_t missing_on[] =
        "SELECT accounts.name FROM accounts INNER JOIN orders "
        "accounts.account_id = orders.account_id";
    static const uint8_t unqualified[] =
        "SELECT name FROM accounts INNER JOIN orders ON "
        "accounts.account_id = orders.account_id";
    static const uint8_t accounts_name[] = "accounts";
    static const uint8_t orders_name[] = "orders";
    static const uint8_t wrong_name[] = "other";
    prdb_sdk_select_v8 statement = {0};
    prdb_sdk_select_v8 unchanged_statement = {0};
    prdb_sdk_select_binding_v8 binding = {0};
    prdb_sdk_select_binding_v8 unchanged_binding = {0};
    prdb_sdk_sql_column_shape_v2 account_columns[] = {
        column("account_id", 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0),
        column("name", 1, PRDB_SDK_VALUE_TYPE_TEXT, 0, 0)};
    prdb_sdk_sql_column_shape_v2 order_columns[] = {
        column("account_id", 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0),
        column("amount", 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 10, 2)};
    uint32_t error_offset = 0;

    assert(prdb_sdk_sql_parse_select_v8(
               sql, sizeof(sql) - 1, &statement, sizeof(statement),
               &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V8_VERSION);
    assert(statement.projection_count == 2 && statement.has_limit == 1 &&
           statement.limit == 4);
    assert(memcmp(sql + statement.left_relation.offset, "accounts", 8) == 0);
    assert(memcmp(sql + statement.right_relation.offset, "orders", 6) == 0);
    assert(memcmp(sql + statement.left_join_key.column.offset,
                  "account_id", 10) == 0);
    assert(prdb_sdk_sql_bind_select_v8(
               &statement, accounts_name, sizeof(accounts_name) - 1,
               account_columns, 2, orders_name, sizeof(orders_name) - 1,
               order_columns, 2, &binding, sizeof(binding)) ==
           PRDB_SDK_SQL_BIND_OK);
    assert(binding.projection_count == 2);
    assert(binding.projections[0].relation_side == PRDB_SDK_SQL_V8_LEFT &&
           binding.projections[0].column.ordinal == 1 &&
           binding.projections[0].column.type_code ==
               PRDB_SDK_VALUE_TYPE_TEXT);
    assert(binding.projections[1].relation_side == PRDB_SDK_SQL_V8_RIGHT &&
           binding.projections[1].column.ordinal == 1 &&
           binding.projections[1].column.type_code ==
               PRDB_SDK_VALUE_TYPE_DECIMAL);
    assert(binding.left_join_key.column.ordinal == 0 &&
           binding.right_join_key.column.ordinal == 0);

    unchanged_statement.struct_size = 77;
    assert(prdb_sdk_sql_parse_select_v8(
               missing_on, sizeof(missing_on) - 1, &unchanged_statement,
               sizeof(unchanged_statement), &error_offset) ==
           PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(unchanged_statement.struct_size == 77);
    assert(prdb_sdk_sql_parse_select_v8(
               unqualified, sizeof(unqualified) - 1, &unchanged_statement,
               sizeof(unchanged_statement), &error_offset) ==
           PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(unchanged_statement.struct_size == 77);

    unchanged_binding.struct_size = 88;
    assert(prdb_sdk_sql_bind_select_v8(
               &statement, wrong_name, sizeof(wrong_name) - 1,
               account_columns, 2, orders_name, sizeof(orders_name) - 1,
               order_columns, 2, &unchanged_binding,
               sizeof(unchanged_binding)) ==
           PRDB_SDK_SQL_BIND_RELATION_MISMATCH);
    assert(unchanged_binding.struct_size == 88);
    order_columns[0].type_code = PRDB_SDK_VALUE_TYPE_BIGINT;
    assert(prdb_sdk_sql_bind_select_v8(
               &statement, accounts_name, sizeof(accounts_name) - 1,
               account_columns, 2, orders_name, sizeof(orders_name) - 1,
               order_columns, 2, &unchanged_binding,
               sizeof(unchanged_binding)) ==
           PRDB_SDK_SQL_BIND_V8_JOIN_TYPE_MISMATCH);
    assert(unchanged_binding.struct_size == 88);

    {
        prdb_sdk_select_v7 old_statement = {0};
        assert(prdb_sdk_sql_parse_select_v7(
                   sql, sizeof(sql) - 1, &old_statement,
                   sizeof(old_statement), NULL) != PRDB_SDK_SQL_OK);
    }
    return 0;
}
