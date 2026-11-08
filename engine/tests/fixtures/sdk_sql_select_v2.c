#include "prdb_sdk_sql_v2.h"

#include <assert.h>
#include <stddef.h>
#include <string.h>

int main(void) {
    static const uint8_t query[] =
        "SELECT name FROM employees WHERE department_id = $1 AND "
        "(salary >= $2 OR active IS NULL) LIMIT 25";
    static const uint8_t malformed[] =
        "SELECT id FROM t WHERE id = $1 AND";
    static const uint8_t not_query[] =
        "SELECT id FROM t WHERE NOT active IS NULL";
    static const uint8_t precedence_query[] =
        "SELECT id FROM t WHERE a = $1 OR b = $2 AND c = $3";
    static const uint8_t is_not_null_query[] =
        "SELECT id FROM t WHERE active IS NOT NULL";
    static const uint8_t prefix[] = "SELECT id FROM t WHERE ";
    static const uint8_t term[] = "id = $1";
    static const uint8_t and_separator[] = " AND ";
    static const uint8_t not_keyword[] = "NOT ";
    static const uint8_t simple_term[] = "id = $1";
    uint8_t too_many_nodes[1024];
    uint8_t at_depth_limit[512];
    uint8_t too_deep[512];
    size_t too_many_nodes_len = 0;
    size_t at_depth_limit_len = 0;
    size_t too_deep_len = 0;
    uint32_t error_offset;
    prdb_sdk_select_v2 statement = {0};

    assert(prdb_sdk_sql_parse_select_v2(
               query, sizeof(query) - 1, &statement, sizeof(statement), NULL) ==
           PRDB_SDK_SQL_OK);
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V2_VERSION);
    assert(statement.projection_count == 1);
    assert(statement.relation.length == 9);
    assert(statement.has_limit == 1 && statement.limit == 25);
    assert(statement.predicate_count == 5);
    assert(statement.predicate_root == 4);
    assert(statement.predicates[0].node_kind == PRDB_SDK_SQL_PREDICATE_COMPARE);
    assert(statement.predicates[0].comparison == PRDB_SDK_SQL_COMPARE_EQUAL);
    assert(statement.predicates[0].parameter_index == 1);
    assert(memcmp(statement.sql + statement.predicates[0].column.offset,
                  "department_id", 13) == 0);
    assert(statement.predicates[1].node_kind == PRDB_SDK_SQL_PREDICATE_COMPARE);
    assert(statement.predicates[1].comparison ==
           PRDB_SDK_SQL_COMPARE_GREATER_EQUAL);
    assert(statement.predicates[1].parameter_index == 2);
    assert(statement.predicates[2].node_kind == PRDB_SDK_SQL_PREDICATE_IS_NULL);
    assert(statement.predicates[2].parameter_index == 0);
    assert(statement.predicates[3].node_kind == PRDB_SDK_SQL_PREDICATE_OR);
    assert(statement.predicates[3].left_child == 1);
    assert(statement.predicates[3].right_child == 2);
    assert(statement.predicates[4].node_kind == PRDB_SDK_SQL_PREDICATE_AND);
    assert(statement.predicates[4].left_child == 0);
    assert(statement.predicates[4].right_child == 3);

    assert(prdb_sdk_sql_parse_select_v2(
               not_query, sizeof(not_query) - 1, &statement,
               sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    assert(statement.predicate_count == 2);
    assert(statement.predicate_root == 1);
    assert(statement.predicates[0].node_kind ==
           PRDB_SDK_SQL_PREDICATE_IS_NULL);
    assert(statement.predicates[1].node_kind == PRDB_SDK_SQL_PREDICATE_NOT);
    assert(statement.predicates[1].left_child == 0);

    assert(prdb_sdk_sql_parse_select_v2(
               precedence_query, sizeof(precedence_query) - 1, &statement,
               sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    assert(statement.predicate_root == 4);
    assert(statement.predicates[3].node_kind == PRDB_SDK_SQL_PREDICATE_AND);
    assert(statement.predicates[4].node_kind == PRDB_SDK_SQL_PREDICATE_OR);
    assert(statement.predicates[4].left_child == 0);
    assert(statement.predicates[4].right_child == 3);

    assert(prdb_sdk_sql_parse_select_v2(
               is_not_null_query, sizeof(is_not_null_query) - 1, &statement,
               sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    assert(statement.predicate_count == 1);
    assert(statement.predicates[0].node_kind ==
           PRDB_SDK_SQL_PREDICATE_IS_NOT_NULL);

    assert(prdb_sdk_sql_parse_select_v2(
               malformed, sizeof(malformed) - 1, &statement,
               sizeof(statement), &error_offset) ==
           PRDB_SDK_SQL_SYNTAX_ERROR);

    memcpy(at_depth_limit, prefix, sizeof(prefix) - 1);
    at_depth_limit_len = sizeof(prefix) - 1;
    for (uint32_t index = 0; index < PRDB_SDK_SQL_MAX_EXPRESSION_DEPTH;
         ++index) {
        memcpy(at_depth_limit + at_depth_limit_len, not_keyword,
               sizeof(not_keyword) - 1);
        at_depth_limit_len += sizeof(not_keyword) - 1;
    }
    memcpy(at_depth_limit + at_depth_limit_len, simple_term,
           sizeof(simple_term) - 1);
    at_depth_limit_len += sizeof(simple_term) - 1;
    assert(prdb_sdk_sql_parse_select_v2(
               at_depth_limit, at_depth_limit_len, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);

    memcpy(too_deep, prefix, sizeof(prefix) - 1);
    too_deep_len = sizeof(prefix) - 1;
    for (uint32_t index = 0; index <= PRDB_SDK_SQL_MAX_EXPRESSION_DEPTH;
         ++index) {
        memcpy(too_deep + too_deep_len, not_keyword, sizeof(not_keyword) - 1);
        too_deep_len += sizeof(not_keyword) - 1;
    }
    memcpy(too_deep + too_deep_len, simple_term, sizeof(simple_term) - 1);
    too_deep_len += sizeof(simple_term) - 1;
    assert(prdb_sdk_sql_parse_select_v2(
               too_deep, too_deep_len, &statement, sizeof(statement),
               &error_offset) == PRDB_SDK_SQL_LIMIT_EXCEEDED);

    memcpy(too_many_nodes, prefix, sizeof(prefix) - 1);
    too_many_nodes_len = sizeof(prefix) - 1;
    for (uint32_t index = 0; index < PRDB_SDK_SQL_MAX_PREDICATE_NODES / 2 + 1;
         ++index) {
        if (index != 0) {
            memcpy(too_many_nodes + too_many_nodes_len, and_separator,
                   sizeof(and_separator) - 1);
            too_many_nodes_len += sizeof(and_separator) - 1;
        }
        memcpy(too_many_nodes + too_many_nodes_len, term, sizeof(term) - 1);
        too_many_nodes_len += sizeof(term) - 1;
    }
    assert(prdb_sdk_sql_parse_select_v2(
               too_many_nodes, too_many_nodes_len, &statement,
               sizeof(statement), &error_offset) ==
           PRDB_SDK_SQL_LIMIT_EXCEEDED);
    return 0;
}
