#include "prdb_sdk_sql_v26.h"

#include <assert.h>
#include <stdint.h>
#include <string.h>

static uint32_t make_in_query(uint8_t *out, uint32_t count) {
    static const uint8_t prefix[] = "SELECT id FROM widgets WHERE id IN (";
    uint32_t offset = sizeof(prefix) - 1;
    memcpy(out, prefix, offset);
    for (uint32_t i = 0; i < count; ++i) {
        if (i != 0) out[offset++] = ',';
        out[offset++] = '$';
        out[offset++] = '1';
    }
    out[offset++] = ')';
    return offset;
}

int main(void) {
    static const uint8_t query[] =
        "SELECT id FROM customers WHERE id IN ($1, $2, $3) LIMIT 10";
    static const uint8_t empty_list[] =
        "SELECT id FROM customers WHERE id IN ()";
    static const uint8_t literal_list[] =
        "SELECT id FROM customers WHERE id IN (1, $2)";
    static const uint8_t not_in[] =
        "SELECT id FROM customers WHERE id NOT IN ($1, $2)";
    uint8_t many_values[256];
    uint32_t many_values_len;
    prdb_sdk_select_v26 statement = {0};
    prdb_sdk_select_v2 legacy = {0};

    assert(prdb_sdk_sql_parse_select_v26(query, sizeof(query) - 1,
               &statement, sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    assert(statement.struct_size == sizeof(statement));
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V26_VERSION);
    assert(statement.base.predicate_count == 5);
    assert(statement.base.predicate_root == 4);
    assert(statement.base.predicates[0].node_kind == PRDB_SDK_SQL_PREDICATE_COMPARE);
    assert(statement.base.predicates[0].parameter_index == 1);
    assert(statement.base.predicates[1].node_kind == PRDB_SDK_SQL_PREDICATE_COMPARE);
    assert(statement.base.predicates[1].parameter_index == 2);
    assert(statement.base.predicates[2].node_kind == PRDB_SDK_SQL_PREDICATE_OR);
    assert(statement.base.predicates[3].parameter_index == 3);
    assert(statement.base.predicates[4].node_kind == PRDB_SDK_SQL_PREDICATE_OR);
    assert(statement.base.has_limit == 1 && statement.base.limit == 10);

    assert(prdb_sdk_sql_parse_select_v2(query, sizeof(query) - 1,
               &legacy, sizeof(legacy), NULL) != PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_parse_select_v26(empty_list, sizeof(empty_list) - 1,
               &statement, sizeof(statement), NULL) != PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_parse_select_v26(literal_list, sizeof(literal_list) - 1,
               &statement, sizeof(statement), NULL) != PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_parse_select_v26(not_in, sizeof(not_in) - 1,
               &statement, sizeof(statement), NULL) != PRDB_SDK_SQL_OK);
    many_values_len = make_in_query(many_values, PRDB_SDK_SQL_V26_MAX_IN_VALUES);
    assert(prdb_sdk_sql_parse_select_v26(many_values, many_values_len,
               &statement, sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    assert(statement.base.predicate_count == 63);
    many_values_len = make_in_query(many_values,
                                    PRDB_SDK_SQL_V26_MAX_IN_VALUES + 1);
    assert(prdb_sdk_sql_parse_select_v26(many_values, many_values_len,
               &statement, sizeof(statement), NULL) ==
           PRDB_SDK_SQL_LIMIT_EXCEEDED);
    return 0;
}
