#include "prdb_sdk_sql_v2.h"

#include <assert.h>
#include <stddef.h>
#include <string.h>

typedef struct resolver_context {
    const prdb_sdk_value_v1 *id;
    const prdb_sdk_value_v1 *salary;
    const prdb_sdk_value_v1 *active;
    uint32_t calls;
} resolver_context;

typedef struct bound_resolver_context {
    const prdb_sdk_value_v1 *cells[2];
    uint32_t calls;
    uint32_t last_ordinal;
} bound_resolver_context;

static prdb_sdk_value_v1 make_value(uint32_t state, uint32_t type,
                                    const uint8_t *data, uint64_t data_len) {
    prdb_sdk_value_v1 value = {sizeof(value), PRDB_SDK_VALUE_ABI_V1_VERSION,
                               state, type, data, data_len};
    return value;
}

static int32_t resolve_test_column(
    void *opaque, const uint8_t *sql, uint64_t sql_len,
    prdb_sdk_sql_span_v1 column, const prdb_sdk_value_v1 **value_out,
    uint32_t *type_out, uint8_t *precision_out, uint8_t *scale_out) {
    resolver_context *context = opaque;
    const uint8_t *name;

    ++context->calls;
    if (column.offset > sql_len || column.length > sql_len - column.offset) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    name = sql + column.offset;
    *precision_out = 0;
    *scale_out = 0;
    if (column.length == 2 && memcmp(name, "id", 2) == 0) {
        *value_out = context->id;
        *type_out = PRDB_SDK_VALUE_TYPE_INTEGER;
        return PRDB_SDK_SQL_EVAL_OK;
    }
    if (column.length == 6 && memcmp(name, "salary", 6) == 0) {
        *value_out = context->salary;
        *type_out = PRDB_SDK_VALUE_TYPE_INTEGER;
        return PRDB_SDK_SQL_EVAL_OK;
    }
    if (column.length == 6 && memcmp(name, "active", 6) == 0) {
        *value_out = context->active;
        *type_out = PRDB_SDK_VALUE_TYPE_BOOLEAN;
        return PRDB_SDK_SQL_EVAL_OK;
    }
    return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
}

static int32_t resolve_bound_test_column(
    void *opaque, uint32_t ordinal, const prdb_sdk_value_v1 **value_out) {
    bound_resolver_context *context = opaque;
    ++context->calls;
    context->last_ordinal = ordinal;
    if (ordinal >= 2) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    *value_out = context->cells[ordinal];
    return PRDB_SDK_SQL_EVAL_OK;
}

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
    static const uint8_t eval_query[] =
        "SELECT id FROM t WHERE id = $1 AND "
        "(salary >= $2 OR active IS NULL)";
    static const uint8_t unknown_query[] =
        "SELECT id FROM t WHERE salary >= $2 OR active IS NULL";
    static const uint8_t unknown_and_false_query[] =
        "SELECT id FROM t WHERE salary >= $2 AND active = $3";
    static const uint8_t no_predicate_query[] = "SELECT id FROM t";
    static const uint8_t bind_query[] =
        "SELECT Name FROM Employees WHERE salary >= $1 AND active IS NULL";
    static const uint8_t bound_eval_query[] =
        "SELECT id FROM t WHERE id = $1 AND active IS NULL";
    static const uint8_t missing_column_query[] =
        "SELECT secret FROM employees";
    static const uint8_t star_query[] = "SELECT * FROM employees";
    static const uint8_t relation_name[] = "employees";
    static const uint8_t column_name[] = "name";
    static const uint8_t salary_name[] = "salary";
    static const uint8_t active_name[] = "active";
    static const uint8_t id_name[] = "id";
    static const uint8_t secret_name[] = "secret";
    static const prdb_sdk_sql_column_shape_v2 columns[] = {
        {column_name, sizeof(column_name) - 1, 0, PRDB_SDK_VALUE_TYPE_TEXT,
         0, 0},
        {salary_name, sizeof(salary_name) - 1, 1,
         PRDB_SDK_VALUE_TYPE_DECIMAL, 10, 2},
        {active_name, sizeof(active_name) - 1, 2,
         PRDB_SDK_VALUE_TYPE_BOOLEAN, 0, 0}};
    static const prdb_sdk_sql_column_shape_v2 bound_eval_columns[] = {
        {id_name, sizeof(id_name) - 1, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {active_name, sizeof(active_name) - 1, 1,
         PRDB_SDK_VALUE_TYPE_BOOLEAN, 0, 0}};
    static const prdb_sdk_sql_column_shape_v2 ambiguous_columns[] = {
        {column_name, sizeof(column_name) - 1, 0, PRDB_SDK_VALUE_TYPE_TEXT,
         0, 0},
        {salary_name, sizeof(salary_name) - 1, 1,
         PRDB_SDK_VALUE_TYPE_DECIMAL, 10, 2},
        {salary_name, sizeof(salary_name) - 1, 2,
         PRDB_SDK_VALUE_TYPE_DECIMAL, 10, 2},
        {active_name, sizeof(active_name) - 1, 3,
         PRDB_SDK_VALUE_TYPE_BOOLEAN, 0, 0}};
    static const prdb_sdk_sql_column_shape_v2 invalid_columns[] = {
        {column_name, sizeof(column_name) - 1, 0, PRDB_SDK_VALUE_TYPE_TEXT,
         0, 0},
        {salary_name, sizeof(salary_name) - 1, 1,
         PRDB_SDK_VALUE_TYPE_DECIMAL, 0, 2},
        {active_name, sizeof(active_name) - 1, 2,
         PRDB_SDK_VALUE_TYPE_BOOLEAN, 0, 0}};
    static const uint8_t positive_integer[] = {0, 0, 0, 2};
    static const uint8_t negative_integer[] = {0xff, 0xff, 0xff, 0xfd};
    static const uint8_t false_boolean[] = {0};
    static const uint8_t true_boolean[] = {1};
    uint8_t too_many_nodes[1024];
    uint8_t at_depth_limit[512];
    uint8_t too_deep[512];
    size_t too_many_nodes_len = 0;
    size_t at_depth_limit_len = 0;
    size_t too_deep_len = 0;
    uint32_t error_offset;
    prdb_sdk_select_v2 statement = {0};
    prdb_sdk_select_v2 unknown_statement = {0};
    prdb_sdk_select_v2 no_predicate_statement = {0};
    prdb_sdk_select_v2 bind_statement = {0};
    prdb_sdk_select_v2 missing_column_statement = {0};
    prdb_sdk_select_v2 star_statement = {0};
    prdb_sdk_select_v2 bound_eval_statement = {0};
    prdb_sdk_select_binding_v2 binding = {0};
    prdb_sdk_select_binding_v2 bound_eval_binding = {0};
    prdb_sdk_value_v1 two = make_value(
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
        positive_integer, sizeof(positive_integer));
    prdb_sdk_value_v1 minus_three = make_value(
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
        negative_integer, sizeof(negative_integer));
    prdb_sdk_value_v1 null_value = make_value(PRDB_SDK_VALUE_STATE_NULL, 0,
                                              NULL, 0);
    prdb_sdk_value_v1 redacted = make_value(
        PRDB_SDK_VALUE_STATE_REDACTED, 0, NULL, 0);
    prdb_sdk_value_v1 false_value = make_value(
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BOOLEAN,
        false_boolean, sizeof(false_boolean));
    prdb_sdk_value_v1 true_value = make_value(
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BOOLEAN, true_boolean,
        sizeof(true_boolean));
    prdb_sdk_value_v1 parameters[] = {two, two};
    prdb_sdk_value_v1 three_parameters[] = {two, two, true_value};
    resolver_context context = {&minus_three, &redacted, &redacted, 0};
    bound_resolver_context bound_context = {{&two, &null_value}, 0, 0};
    uint32_t truth;

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
               bind_query, sizeof(bind_query) - 1, &bind_statement,
               sizeof(bind_statement), NULL) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_select_v2(
               &bind_statement, relation_name, sizeof(relation_name) - 1,
               columns, 3, &binding, sizeof(binding)) ==
           PRDB_SDK_SQL_BIND_OK);
    assert(binding.abi_version == PRDB_SDK_SQL_ABI_V2_VERSION);
    assert(binding.projection_count == 1);
    assert(binding.projection_ordinals[0] == 0);
    assert(binding.predicate_count == bind_statement.predicate_count);
    assert(binding.predicate_columns[0].ordinal == 1);
    assert(binding.predicate_columns[0].type_code ==
           PRDB_SDK_VALUE_TYPE_DECIMAL);
    assert(binding.predicate_columns[0].decimal_precision == 10);
    assert(binding.predicate_columns[0].decimal_scale == 2);
    assert(binding.predicate_columns[1].ordinal == 2);
    assert(binding.predicate_columns[1].type_code ==
           PRDB_SDK_VALUE_TYPE_BOOLEAN);
    assert(binding.predicate_columns[2].type_code == 0);

    assert(prdb_sdk_sql_bind_select_v2(
               &bind_statement, relation_name, sizeof(relation_name) - 1,
               ambiguous_columns, 4, &binding, sizeof(binding)) ==
           PRDB_SDK_SQL_BIND_AMBIGUOUS_COLUMN);
    assert(binding.projection_count == 1);
    assert(prdb_sdk_sql_bind_select_v2(
               &bind_statement, relation_name, sizeof(relation_name) - 1,
               invalid_columns, 3, &binding, sizeof(binding)) ==
           PRDB_SDK_SQL_BIND_INVALID_SCHEMA);
    assert(prdb_sdk_sql_bind_select_v2(
               &bind_statement, (const uint8_t *)"other", 5, columns, 3,
               &binding, sizeof(binding)) ==
           PRDB_SDK_SQL_BIND_RELATION_MISMATCH);
    assert(prdb_sdk_sql_parse_select_v2(
               missing_column_query, sizeof(missing_column_query) - 1,
               &missing_column_statement, sizeof(missing_column_statement),
               NULL) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_select_v2(
               &missing_column_statement, relation_name,
               sizeof(relation_name) - 1, columns, 3, &binding,
               sizeof(binding)) == PRDB_SDK_SQL_BIND_COLUMN_NOT_FOUND);
    assert(prdb_sdk_sql_parse_select_v2(
               star_query, sizeof(star_query) - 1, &star_statement,
               sizeof(star_statement), NULL) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_select_v2(
               &star_statement, relation_name, sizeof(relation_name) - 1,
               columns, 3, &binding, sizeof(binding)) ==
           PRDB_SDK_SQL_BIND_OK);
    assert(binding.projection_star == 1 && binding.projection_count == 0);
    assert(prdb_sdk_sql_parse_select_v2(
               bound_eval_query, sizeof(bound_eval_query) - 1,
               &bound_eval_statement, sizeof(bound_eval_statement), NULL) ==
           PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_select_v2(
               &bound_eval_statement, (const uint8_t *)"t", 1,
               bound_eval_columns, 2, &bound_eval_binding,
               sizeof(bound_eval_binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(prdb_sdk_sql_eval_bound_select_predicate_v2(
               &bound_eval_statement, &bound_eval_binding, parameters, 1,
               resolve_bound_test_column, &bound_context, &truth) ==
           PRDB_SDK_SQL_EVAL_OK);
    assert(truth == PRDB_SDK_SQL_TRUTH_TRUE);
    assert(bound_context.calls == 2 && bound_context.last_ordinal == 1);
    bound_context = (bound_resolver_context){{&minus_three, &redacted}, 0, 0};
    assert(prdb_sdk_sql_eval_bound_select_predicate_v2(
               &bound_eval_statement, &bound_eval_binding, parameters, 1,
               resolve_bound_test_column, &bound_context, &truth) ==
           PRDB_SDK_SQL_EVAL_OK);
    assert(truth == PRDB_SDK_SQL_TRUTH_FALSE);
    assert(bound_context.calls == 1 && bound_context.last_ordinal == 0);
    {
        prdb_sdk_select_binding_v2 malformed_binding = bound_eval_binding;
        malformed_binding.predicate_columns[0].type_code = 0;
        bound_context.calls = 0;
        truth = PRDB_SDK_SQL_TRUTH_UNKNOWN;
        assert(prdb_sdk_sql_eval_bound_select_predicate_v2(
                   &bound_eval_statement, &malformed_binding, parameters, 1,
                   resolve_bound_test_column, &bound_context, &truth) ==
               PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT);
        assert(bound_context.calls == 0);
        assert(truth == PRDB_SDK_SQL_TRUTH_UNKNOWN);
    }
    {
        prdb_sdk_select_binding_v2 invalid_ordinal_binding =
            bound_eval_binding;
        invalid_ordinal_binding.predicate_columns[0].ordinal = 2;
        bound_context.calls = 0;
        truth = PRDB_SDK_SQL_TRUTH_UNKNOWN;
        assert(prdb_sdk_sql_eval_bound_select_predicate_v2(
                   &bound_eval_statement, &invalid_ordinal_binding, parameters,
                   1, resolve_bound_test_column, &bound_context, &truth) ==
               PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT);
        assert(bound_context.calls == 1 && bound_context.last_ordinal == 2);
        assert(truth == PRDB_SDK_SQL_TRUTH_UNKNOWN);
    }

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

    assert(prdb_sdk_sql_parse_select_v2(
               eval_query, sizeof(eval_query) - 1, &statement,
               sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_eval_select_predicate_v2(
               &statement, parameters, 2, resolve_test_column, &context,
               &truth) == PRDB_SDK_SQL_EVAL_OK);
    assert(truth == PRDB_SDK_SQL_TRUTH_FALSE);
    assert(context.calls == 1);

    context = (resolver_context){&two, &two, &redacted, 0};
    assert(prdb_sdk_sql_eval_select_predicate_v2(
               &statement, parameters, 2, resolve_test_column, &context,
               &truth) == PRDB_SDK_SQL_EVAL_OK);
    assert(truth == PRDB_SDK_SQL_TRUTH_TRUE);
    assert(context.calls == 2);

    context = (resolver_context){&two, &minus_three, &redacted, 0};
    assert(prdb_sdk_sql_eval_select_predicate_v2(
               &statement, parameters, 2, resolve_test_column, &context,
               &truth) == PRDB_SDK_SQL_EVAL_REDACTED_INPUT);
    assert(context.calls == 3);

    context = (resolver_context){&two, &two, &false_value, 0};
    assert(prdb_sdk_sql_parse_select_v2(
               not_query, sizeof(not_query) - 1, &statement,
               sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_eval_select_predicate_v2(
               &statement, parameters, 2, resolve_test_column, &context,
               &truth) == PRDB_SDK_SQL_EVAL_OK);
    assert(truth == PRDB_SDK_SQL_TRUTH_TRUE);
    assert(context.calls == 1);

    context = (resolver_context){&two, &null_value, &false_value, 0};
    assert(prdb_sdk_sql_parse_select_v2(
               unknown_query, sizeof(unknown_query) - 1, &unknown_statement,
               sizeof(unknown_statement), NULL) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_eval_select_predicate_v2(
               &unknown_statement, parameters, 2, resolve_test_column,
               &context, &truth) == PRDB_SDK_SQL_EVAL_OK);
    assert(truth == PRDB_SDK_SQL_TRUTH_UNKNOWN);
    assert(context.calls == 2);

    context = (resolver_context){&two, &null_value, &false_value, 0};
    assert(prdb_sdk_sql_parse_select_v2(
               unknown_and_false_query, sizeof(unknown_and_false_query) - 1,
               &unknown_statement, sizeof(unknown_statement), NULL) ==
           PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_eval_select_predicate_v2(
               &unknown_statement, three_parameters, 3,
               resolve_test_column, &context, &truth) == PRDB_SDK_SQL_EVAL_OK);
    assert(truth == PRDB_SDK_SQL_TRUTH_FALSE);
    assert(context.calls == 2);

    context = (resolver_context){&two, &two, &redacted, 0};
    assert(prdb_sdk_sql_parse_select_v2(
               eval_query, sizeof(eval_query) - 1, &statement,
               sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_eval_select_predicate_v2(
               &statement, parameters, 1, resolve_test_column, &context,
               &truth) == PRDB_SDK_SQL_EVAL_PARAMETER_MISSING);
    assert(context.calls == 1);

    assert(prdb_sdk_sql_parse_select_v2(
               no_predicate_query, sizeof(no_predicate_query) - 1,
               &no_predicate_statement, sizeof(no_predicate_statement), NULL) ==
           PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_eval_select_predicate_v2(
               &no_predicate_statement, NULL, 0, NULL, NULL, &truth) ==
           PRDB_SDK_SQL_EVAL_OK);
    assert(truth == PRDB_SDK_SQL_TRUTH_TRUE);

    statement.predicates[statement.predicate_root].left_child =
        statement.predicate_root;
    context.calls = 0;
    assert(prdb_sdk_sql_eval_select_predicate_v2(
               &statement, parameters, 2, resolve_test_column, &context,
               &truth) == PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT);
    assert(context.calls == 0);
    return 0;
}
