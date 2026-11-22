#include "prdb_sdk_sql_v23.h"
#include <assert.h>
#include <string.h>

static void parse_and_bind(const char *sql, uint32_t type,
                           prdb_sdk_select_v23 *statement,
                           prdb_sdk_select_binding_v23 *binding) {
    static const uint8_t relation[] = "records";
    prdb_sdk_sql_column_shape_v2 columns[3] = {
        {(const uint8_t *)"department", 10, 0, PRDB_SDK_VALUE_TYPE_TEXT, 0, 0},
        {(const uint8_t *)"region", 6, 1, PRDB_SDK_VALUE_TYPE_TEXT, 0, 0},
        {(const uint8_t *)"amount", 6, 2, type, 0, 0}};
    if (type == PRDB_SDK_VALUE_TYPE_DECIMAL) {
        columns[2].decimal_precision = 10;
        columns[2].decimal_scale = 2;
    }
    assert(prdb_sdk_sql_parse_select_v23(
        (const uint8_t *)sql, strlen(sql), statement, sizeof(*statement),
        NULL) == PRDB_SDK_SQL_OK);
    assert(statement->abi_version == PRDB_SDK_SQL_ABI_V23_VERSION);
    assert(prdb_sdk_sql_bind_select_v23(
        statement, relation, sizeof(relation) - 1, columns, 3,
        binding, sizeof(*binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V23_VERSION);
}

int main(void) {
    static const char *prefix =
        "SELECT department, region, SUM(amount) FROM records "
        "GROUP BY department, region HAVING SUM(amount) IS NULL";
    prdb_sdk_select_v23 statement = {0}, saved;
    prdb_sdk_select_binding_v23 binding = {0};
    prdb_sdk_select_v22 legacy = {0};
    parse_and_bind(prefix, PRDB_SDK_VALUE_TYPE_DECIMAL, &statement, &binding);
    assert(statement.base.having_nodes[0].base.node_kind ==
           PRDB_SDK_SQL_PREDICATE_IS_NULL);
    assert(statement.having_count_mask == 0);
    assert(binding.base.having_nodes[0].base.operand_kind == 0);
    assert(prdb_sdk_sql_parse_select_v22(
        (const uint8_t *)prefix, strlen(prefix), &legacy, sizeof(legacy),
        NULL) == PRDB_SDK_SQL_SYNTAX_ERROR);
    /* Earlier binders also reject forged NULL-test nodes. */
    legacy.struct_size = sizeof(legacy);
    legacy.abi_version = PRDB_SDK_SQL_ABI_V22_VERSION;
    legacy.base = statement.base;
    prdb_sdk_sql_column_shape_v2 columns[3] = {
        {(const uint8_t *)"department", 10, 0, PRDB_SDK_VALUE_TYPE_TEXT, 0, 0},
        {(const uint8_t *)"region", 6, 1, PRDB_SDK_VALUE_TYPE_TEXT, 0, 0},
        {(const uint8_t *)"amount", 6, 2, PRDB_SDK_VALUE_TYPE_DECIMAL, 10, 2}};
    prdb_sdk_select_binding_v22 old_binding = {0};
    assert(prdb_sdk_sql_bind_select_v22(
        &legacy, (const uint8_t *)"records", 7, columns, 3, &old_binding,
        sizeof(old_binding)) == PRDB_SDK_SQL_BIND_INVALID_STATEMENT);
    saved = statement;
    statement.base.having_nodes[0].base.parameter_index = 1;
    assert(prdb_sdk_sql_bind_select_v23(
        &statement, (const uint8_t *)"records", 7, columns, 3, &binding,
        sizeof(binding)) == PRDB_SDK_SQL_BIND_INVALID_STATEMENT);
    statement = saved;
    statement.base.base.having_nodes[0].comparison = 1;
    statement.base.having_nodes[0].base.comparison = 1;
    assert(prdb_sdk_sql_bind_select_v23(
        &statement, (const uint8_t *)"records", 7, columns, 3, &binding,
        sizeof(binding)) == PRDB_SDK_SQL_BIND_INVALID_STATEMENT);
    statement = saved;
    statement.base.base.having_node_count = 65;
    assert(prdb_sdk_sql_bind_select_v23(
        &statement, (const uint8_t *)"records", 7, columns, 3, &binding,
        sizeof(binding)) == PRDB_SDK_SQL_BIND_INVALID_STATEMENT);
    parse_and_bind(
        "SELECT department, region, MIN(amount) FROM records "
        "GROUP BY department, region HAVING NOT MIN(amount) IS NULL "
        "AND (COUNT(*) IS NOT NULL OR COUNT(*) >= 2) "
        "ORDER BY department LIMIT 1",
        PRDB_SDK_VALUE_TYPE_TEXT, &statement, &binding);
    assert(statement.base.base.having_node_count == 6);
    assert(statement.having_count_mask == ((UINT64_C(1) << 2) |
                                          (UINT64_C(1) << 3)));
    assert(binding.base.having_nodes[2].base.node_kind ==
           PRDB_SDK_SQL_PREDICATE_IS_NOT_NULL);
    assert(binding.base.having_nodes[3].scale == 0);
    parse_and_bind(
        "SELECT department, region, COUNT(*) FROM records "
        "GROUP BY department, region HAVING COUNT(*) IS NULL",
        PRDB_SDK_VALUE_TYPE_INTEGER, &statement, &binding);
    assert(statement.having_count_mask == 0);
    const char *invalid[] = {
        "SELECT department, region, SUM(amount) FROM records GROUP BY department, region HAVING SUM(amount) IS",
        "SELECT department, region, SUM(amount) FROM records GROUP BY department, region HAVING SUM(amount) IS NOT",
        "SELECT department, region, SUM(amount) FROM records GROUP BY department, region HAVING SUM(amount) IS TRUE",
        "SELECT department, region, SUM(amount) FROM records GROUP BY department, region HAVING SUM(amount) IS NULL $1",
        "SELECT department, region, SUM(amount) FROM records GROUP BY department, region HAVING MAX(amount) IS NULL"};
    for (uint32_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); ++i) {
        statement = saved;
        uint32_t offset = 0;
        assert(prdb_sdk_sql_parse_select_v23(
            (const uint8_t *)invalid[i], strlen(invalid[i]), &statement,
            sizeof(statement), &offset) == PRDB_SDK_SQL_SYNTAX_ERROR);
        assert(offset > 0 && offset <= strlen(invalid[i]));
        assert(memcmp(&statement, &saved, sizeof(saved)) == 0);
    }
    return 0;
}
