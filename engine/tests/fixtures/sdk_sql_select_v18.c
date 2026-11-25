#include "prdb_sdk_sql_v1.h"

#include <assert.h>
#include <string.h>

int main(void) {
    static const uint8_t relation[] = "records";
    static const uint8_t a[] = "a", b[] = "b", c[] = "c";
    static const uint8_t compound_sql[] =
        "SELECT a, b, SUM(c) FROM records GROUP BY a, b "
        "HAVING (SUM(c) >= 5 OR SUM(c) = $2) AND NOT SUM(c) < 20 "
        "ORDER BY a LIMIT 4";
    static const uint8_t signed_min_sql[] =
        "SELECT a, b, SUM(c) FROM records GROUP BY a, b "
        "HAVING SUM(c) >= -9223372036854775808";
    prdb_sdk_sql_column_shape_v1 columns[3] = {
        {a, 1, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {b, 1, 1, PRDB_SDK_VALUE_TYPE_TEXT, 0, 0},
        {c, 1, 2, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_select_v1 statement = {0};
    prdb_sdk_select_binding_v1 binding = {0};
    prdb_sdk_select_binding_v1 previous;
    uint8_t boundary_sql[2048];
    uint32_t boundary_len;
    uint32_t error_offset = 0;

    assert(prdb_sdk_sql_parse_select_v1(
               compound_sql, sizeof(compound_sql) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V1_VERSION);
    assert(statement.having_node_count == 6 && statement.having_root == 5);
    assert(statement.having_nodes[2].node_kind ==
           PRDB_SDK_SQL_PREDICATE_OR);
    assert(statement.having_nodes[4].node_kind ==
           PRDB_SDK_SQL_PREDICATE_NOT);
    assert(statement.having_nodes[5].node_kind ==
           PRDB_SDK_SQL_PREDICATE_AND);
    assert(statement.order_count == 1);
    assert(prdb_sdk_sql_bind_select_v1(
               &statement, relation, sizeof(relation) - 1, columns, 3,
               &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.having_node_count == 6 && binding.having_root == 5);
    assert(binding.having_type_code == PRDB_SDK_VALUE_TYPE_BIGINT);
    assert(binding.having_nodes[0].operand_kind ==
           PRDB_SDK_SQL_HAVING_OPERAND_NUMERIC_LITERAL);
    assert(binding.having_nodes[0].scale == 0);
    assert(binding.having_nodes[1].operand_kind ==
           PRDB_SDK_SQL_HAVING_OPERAND_PARAMETER);
    assert(binding.having_nodes[1].parameter_index == 2);
    assert(binding.has_having == 0);

    assert(prdb_sdk_sql_parse_select_v1(
               signed_min_sql, sizeof(signed_min_sql) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(statement.having_nodes[0].operand_kind ==
           PRDB_SDK_SQL_HAVING_OPERAND_NUMERIC_LITERAL);
    assert(prdb_sdk_sql_bind_select_v1(
               &statement, relation, sizeof(relation) - 1, columns, 3,
               &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(binding.having_nodes[0].operand_kind ==
           PRDB_SDK_SQL_HAVING_OPERAND_NUMERIC_LITERAL);

    {
        static const uint8_t prefix[] =
            "SELECT a, b, SUM(c) FROM records GROUP BY a, b HAVING ";
        static const uint8_t leaf[] = "SUM(c) > 1";
        boundary_len = sizeof(prefix) - 1;
        memcpy(boundary_sql, prefix, boundary_len);
        for (uint32_t index = 0; index < 32; ++index) {
            if (index != 0) {
                memcpy(boundary_sql + boundary_len, " OR ", 4);
                boundary_len += 4;
            }
            memcpy(boundary_sql + boundary_len, leaf, sizeof(leaf) - 1);
            boundary_len += sizeof(leaf) - 1;
        }
        assert(prdb_sdk_sql_parse_select_v1(
                   boundary_sql, boundary_len, &statement,
                   sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
        assert(statement.having_node_count == 63);

        memcpy(boundary_sql + boundary_len, " OR ", 4);
        boundary_len += 4;
        memcpy(boundary_sql + boundary_len, leaf, sizeof(leaf) - 1);
        boundary_len += sizeof(leaf) - 1;
        assert(prdb_sdk_sql_parse_select_v1(
                   boundary_sql, boundary_len, &statement,
                   sizeof(statement), &error_offset) ==
               PRDB_SDK_SQL_LIMIT_EXCEEDED);

        boundary_len = sizeof(prefix) - 1;
        memcpy(boundary_sql, prefix, boundary_len);
        memset(boundary_sql + boundary_len, '(', 33);
        boundary_len += 33;
        memcpy(boundary_sql + boundary_len, leaf, sizeof(leaf) - 1);
        boundary_len += sizeof(leaf) - 1;
        memset(boundary_sql + boundary_len, ')', 33);
        boundary_len += 33;
        assert(prdb_sdk_sql_parse_select_v1(
                   boundary_sql, boundary_len, &statement,
                   sizeof(statement), &error_offset) ==
               PRDB_SDK_SQL_LIMIT_EXCEEDED);
    }

    assert(prdb_sdk_sql_parse_select_v1(
               compound_sql, sizeof(compound_sql) - 1, &statement,
               sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
    memset(&binding, 0xA5, sizeof(binding));
    previous = binding;

    statement.having_nodes[0].operand_kind = PRDB_SDK_SQL_HAVING_OPERAND_PARAMETER;
    statement.having_nodes[0].parameter_index = 0;
    statement.having_nodes[0].literal = 0;
    assert(prdb_sdk_sql_bind_select_v1(
               &statement, relation, sizeof(relation) - 1, columns, 3,
               &binding, sizeof(binding)) ==
           PRDB_SDK_SQL_BIND_INVALID_STATEMENT);
    assert(memcmp(&binding, &previous, sizeof(binding)) == 0);

    {
        static const uint32_t inputs[] = {
            PRDB_SDK_SQL_TRUTH_TRUE, PRDB_SDK_SQL_TRUTH_FALSE,
            PRDB_SDK_SQL_TRUTH_UNKNOWN};
        static const uint32_t and_expected[3][3] = {
            {PRDB_SDK_SQL_TRUTH_TRUE, PRDB_SDK_SQL_TRUTH_FALSE,
             PRDB_SDK_SQL_TRUTH_UNKNOWN},
            {PRDB_SDK_SQL_TRUTH_FALSE, PRDB_SDK_SQL_TRUTH_FALSE,
             PRDB_SDK_SQL_TRUTH_FALSE},
            {PRDB_SDK_SQL_TRUTH_UNKNOWN, PRDB_SDK_SQL_TRUTH_FALSE,
             PRDB_SDK_SQL_TRUTH_UNKNOWN}};
        static const uint32_t or_expected[3][3] = {
            {PRDB_SDK_SQL_TRUTH_TRUE, PRDB_SDK_SQL_TRUTH_TRUE,
             PRDB_SDK_SQL_TRUTH_TRUE},
            {PRDB_SDK_SQL_TRUTH_TRUE, PRDB_SDK_SQL_TRUTH_FALSE,
             PRDB_SDK_SQL_TRUTH_UNKNOWN},
            {PRDB_SDK_SQL_TRUTH_TRUE, PRDB_SDK_SQL_TRUTH_UNKNOWN,
             PRDB_SDK_SQL_TRUTH_UNKNOWN}};
        static const uint32_t not_expected[] = {
            PRDB_SDK_SQL_TRUTH_FALSE, PRDB_SDK_SQL_TRUTH_TRUE,
            PRDB_SDK_SQL_TRUTH_UNKNOWN};
        for (uint32_t left = 0; left < 3; ++left) {
            uint32_t truth = UINT32_MAX;
            assert(prdb_sdk_sql_truth_not_v1(inputs[left], &truth) ==
                   PRDB_SDK_SQL_EVAL_OK);
            assert(truth == not_expected[left]);
            for (uint32_t right = 0; right < 3; ++right) {
                truth = UINT32_MAX;
                assert(prdb_sdk_sql_truth_and_v1(
                           inputs[left], inputs[right], &truth) ==
                       PRDB_SDK_SQL_EVAL_OK);
                assert(truth == and_expected[left][right]);
                truth = UINT32_MAX;
                assert(prdb_sdk_sql_truth_or_v1(
                           inputs[left], inputs[right], &truth) ==
                       PRDB_SDK_SQL_EVAL_OK);
                assert(truth == or_expected[left][right]);
            }
        }
    }
    return 0;
}
