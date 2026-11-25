#include "prdb_sdk_sql_v1.h"

#include <assert.h>
#include <string.h>

static prdb_sdk_sql_column_shape_v1 column(const char *name,
                                          uint32_t ordinal,
                                          uint32_t type_code,
                                          uint8_t precision,
                                          uint8_t scale) {
    return (prdb_sdk_sql_column_shape_v1){
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
    prdb_sdk_select_v1 statement = {0};
    prdb_sdk_select_v1 unchanged_statement = {0};
    prdb_sdk_select_binding_v1 binding = {0};
    prdb_sdk_select_binding_v1 unchanged_binding = {0};
    prdb_sdk_sql_column_shape_v1 account_columns[] = {
        column("account_id", 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0),
        column("name", 1, PRDB_SDK_VALUE_TYPE_TEXT, 0, 0)};
    prdb_sdk_sql_column_shape_v1 order_columns[] = {
        column("account_id", 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0),
        column("amount", 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 10, 2)};
    uint32_t error_offset = 0;

    assert(prdb_sdk_sql_parse_select_v1(
               sql, sizeof(sql) - 1, &statement, sizeof(statement),
               &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V1_VERSION);
    assert(statement.join_projection_count == 2 && statement.has_limit == 1 &&
           statement.limit == 4);
    assert(memcmp(sql + statement.left_relation.offset, "accounts", 8) == 0);
    assert(memcmp(sql + statement.right_relation.offset, "orders", 6) == 0);
    assert(memcmp(sql + statement.left_join_keys[0].column.offset,
                  "account_id", 10) == 0);
    assert(prdb_sdk_sql_bind_join_select_v1(
               &statement, accounts_name, sizeof(accounts_name) - 1,
               account_columns, 2, orders_name, sizeof(orders_name) - 1,
               order_columns, 2, &binding, sizeof(binding)) ==
           PRDB_SDK_SQL_BIND_OK);
    assert(binding.join_projection_count == 2);
    assert(binding.join_projections[0].relation_side == PRDB_SDK_SQL_JOIN_LEFT &&
           binding.join_projections[0].column.ordinal == 1 &&
           binding.join_projections[0].column.type_code ==
               PRDB_SDK_VALUE_TYPE_TEXT);
    assert(binding.join_projections[1].relation_side == PRDB_SDK_SQL_JOIN_RIGHT &&
           binding.join_projections[1].column.ordinal == 1 &&
           binding.join_projections[1].column.type_code ==
               PRDB_SDK_VALUE_TYPE_DECIMAL);
    assert(binding.join_keys[0].left.ordinal == 0 &&
           binding.join_keys[0].right.ordinal == 0);

    unchanged_statement.struct_size = 77;
    assert(prdb_sdk_sql_parse_select_v1(
               missing_on, sizeof(missing_on) - 1, &unchanged_statement,
               sizeof(unchanged_statement), &error_offset) ==
           PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(unchanged_statement.struct_size == 77);
    assert(prdb_sdk_sql_parse_select_v1(
               unqualified, sizeof(unqualified) - 1, &unchanged_statement,
               sizeof(unchanged_statement), &error_offset) ==
           PRDB_SDK_SQL_SYNTAX_ERROR);
    assert(unchanged_statement.struct_size == 77);

    unchanged_binding.struct_size = 88;
    assert(prdb_sdk_sql_bind_join_select_v1(
               &statement, wrong_name, sizeof(wrong_name) - 1,
               account_columns, 2, orders_name, sizeof(orders_name) - 1,
               order_columns, 2, &unchanged_binding,
               sizeof(unchanged_binding)) ==
           PRDB_SDK_SQL_BIND_RELATION_MISMATCH);
    assert(unchanged_binding.struct_size == 88);
    order_columns[0].type_code = PRDB_SDK_VALUE_TYPE_BIGINT;
    assert(prdb_sdk_sql_bind_join_select_v1(
               &statement, accounts_name, sizeof(accounts_name) - 1,
               account_columns, 2, orders_name, sizeof(orders_name) - 1,
               order_columns, 2, &unchanged_binding,
               sizeof(unchanged_binding)) ==
           PRDB_SDK_SQL_BIND_JOIN_TYPE_MISMATCH);
    assert(unchanged_binding.struct_size == 88);
    return 0;
}
