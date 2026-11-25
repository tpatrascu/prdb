#include "sql_internal.h"

static int32_t parse_nonnegative_bigint_literal(parser *state, uint64_t *out) {
    uint64_t value = 0;
    if (state->current.kind != TOKEN_NUMBER) {
        state->error_offset = state->current.offset;
        return PRDB_SDK_SQL_SYNTAX_ERROR;
    }
    for (uint32_t index = 0; index < state->current.length; ++index) {
        uint8_t byte = state->sql[state->current.offset + index];
        uint32_t digit;
        if (byte < '0' || byte > '9') {
            state->error_offset = state->current.offset + index;
            return PRDB_SDK_SQL_SYNTAX_ERROR;
        }
        digit = (uint32_t)(byte - '0');
        if (value > (UINT64_C(0x7fffffffffffffff) - digit) / 10) {
            state->error_offset = state->current.offset;
            return PRDB_SDK_SQL_LIMIT_EXCEEDED;
        }
        value = value * 10 + digit;
    }
    *out = value;
    return PRDB_SDK_SQL_OK;
}

static int32_t parse_signed_bigint_literal_v18(parser *state, uint64_t *out) {
    uint64_t magnitude = 0;
    uint64_t limit;
    uint32_t start, sign_offset = state->current.offset;
    int negative = state->current.kind == TOKEN_MINUS;
    int32_t status;

    if (negative) {
        status = advance(state);
        if (status != PRDB_SDK_SQL_OK) return status;
    }
    if (state->current.kind != TOKEN_NUMBER) {
        state->error_offset = negative ? sign_offset : state->current.offset;
        return PRDB_SDK_SQL_SYNTAX_ERROR;
    }
    start = state->current.offset;
    limit = negative ? UINT64_C(0x8000000000000000)
                     : UINT64_C(0x7fffffffffffffff);
    for (uint32_t index = 0; index < state->current.length; ++index) {
        uint8_t byte = state->sql[start + index];
        uint32_t digit;
        if (byte < '0' || byte > '9') {
            state->error_offset = start + index;
            return PRDB_SDK_SQL_SYNTAX_ERROR;
        }
        digit = (uint32_t)(byte - '0');
        if (magnitude > (limit - digit) / 10) {
            state->error_offset = negative ? sign_offset : start;
            return PRDB_SDK_SQL_LIMIT_EXCEEDED;
        }
        magnitude = magnitude * 10 + digit;
    }
    *out = negative ? UINT64_C(0) - magnitude : magnitude;
    return PRDB_SDK_SQL_OK;
}

PRDB_SDK_INTERNAL int32_t prdb_sdk_sql_parse_select_v17(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v1 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out) {
    uint8_t base_sql[PRDB_SDK_SQL_MAX_QUERY_BYTES];
    parser scan, having;
    prdb_sdk_select_v1 result = {0};
    token having_token = {TOKEN_END, 0, 0};
    uint32_t group_by_end = 0, clause_end;
    int saw_group = 0;
    int32_t status;

    if (error_offset_out != NULL) *error_offset_out = 0;
    if (sql == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_INVALID_ARGUMENT;
    }
    if (sql_len == 0) return PRDB_SDK_SQL_SYNTAX_ERROR;
    if (sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES) {
        if (error_offset_out != NULL) {
            *error_offset_out = (uint32_t)PRDB_SDK_SQL_MAX_QUERY_BYTES;
        }
        return PRDB_SDK_SQL_LIMIT_EXCEEDED;
    }

    scan = (parser){sql, (uint32_t)sql_len, 0, {TOKEN_END, 0, 0}, 0,
                    PRDB_SDK_SQL_OK, 0};
    status = advance(&scan);
    if (status != PRDB_SDK_SQL_OK) goto failed_v17;
    while (scan.current.kind != TOKEN_END &&
           scan.current.kind != TOKEN_SEMICOLON) {
        if (token_is_keyword(&scan, "GROUP")) {
            saw_group = 1;
        } else if (saw_group && token_is_keyword(&scan, "BY")) {
            group_by_end = scan.current.offset + scan.current.length;
            saw_group = 0;
        } else {
            saw_group = 0;
        }
        if (token_is_keyword(&scan, "HAVING")) {
            if (having_token.kind != TOKEN_END) {
                status = PRDB_SDK_SQL_SYNTAX_ERROR;
                scan.error_offset = scan.current.offset;
                goto failed_v17;
            }
            having_token = scan.current;
        }
        status = advance(&scan);
        if (status != PRDB_SDK_SQL_OK) goto failed_v17;
    }

    if (having_token.kind == TOKEN_END) {
        status = prdb_sdk_sql_parse_select_v15(
            sql, sql_len, &result, sizeof(result), error_offset_out);
        if (status != PRDB_SDK_SQL_OK) return status;
        result.sql = sql;
    } else {
        if (group_by_end == 0 || having_token.offset < group_by_end) {
            status = PRDB_SDK_SQL_SYNTAX_ERROR;
            scan.error_offset = having_token.offset;
            goto failed_v17;
        }
        memcpy(base_sql, sql, (size_t)sql_len);
        clause_end = (uint32_t)sql_len;
        having = (parser){sql, (uint32_t)sql_len,
                          having_token.offset + having_token.length,
                          {TOKEN_END, 0, 0}, 0, PRDB_SDK_SQL_OK, 0};
        status = advance(&having);
        if (status != PRDB_SDK_SQL_OK) goto failed_having_v17;
        while (having.current.kind != TOKEN_END &&
               having.current.kind != TOKEN_SEMICOLON) {
            if (token_is_keyword(&having, "ORDER") ||
                token_is_keyword(&having, "LIMIT")) {
                clause_end = having.current.offset;
                break;
            }
            status = advance(&having);
            if (status != PRDB_SDK_SQL_OK) goto failed_having_v17;
        }
        if (clause_end < having_token.offset) {
            status = PRDB_SDK_SQL_SYNTAX_ERROR;
            having.error_offset = having_token.offset;
            goto failed_having_v17;
        }
        for (uint32_t index = having_token.offset; index < clause_end; ++index) {
            base_sql[index] = ' ';
        }
        status = prdb_sdk_sql_parse_select_v15(
            base_sql, sql_len, &result, sizeof(result),
            error_offset_out);
        if (status != PRDB_SDK_SQL_OK) return status;
        result.sql = sql;

        having = (parser){sql, (uint32_t)sql_len,
                          having_token.offset + having_token.length,
                          {TOKEN_END, 0, 0}, 0, PRDB_SDK_SQL_OK, 0};
        status = advance(&having);
        if (status != PRDB_SDK_SQL_OK) goto failed_having_v17;
        {
            prdb_sdk_select_v1 *grouped =
                &result;
            uint32_t parsed_kind;
            if (token_is_keyword(&having, "COUNT")) {
                parsed_kind = PRDB_SDK_SQL_AGGREGATE_COUNT_STAR;
            } else if (token_is_keyword(&having, "MIN")) {
                parsed_kind = PRDB_SDK_SQL_AGGREGATE_MIN;
            } else if (token_is_keyword(&having, "MAX")) {
                parsed_kind = PRDB_SDK_SQL_AGGREGATE_MAX;
            } else if (token_is_keyword(&having, "SUM")) {
                parsed_kind = PRDB_SDK_SQL_AGGREGATE_SUM;
            } else if (token_is_keyword(&having, "AVG")) {
                parsed_kind = PRDB_SDK_SQL_AGGREGATE_AVG;
            } else {
                status = PRDB_SDK_SQL_SYNTAX_ERROR;
                having.error_offset = having.current.offset;
                goto failed_having_v17;
            }
            if (parsed_kind != grouped->aggregate_kind) {
                status = PRDB_SDK_SQL_SYNTAX_ERROR;
                having.error_offset = having.current.offset;
                goto failed_having_v17;
            }
            status = advance(&having);
            if (status != PRDB_SDK_SQL_OK ||
                having.current.kind != TOKEN_LEFT_PAREN) {
                status = status == PRDB_SDK_SQL_OK
                             ? PRDB_SDK_SQL_SYNTAX_ERROR
                             : status;
                having.error_offset = having.current.offset;
                goto failed_having_v17;
            }
            status = advance(&having);
            if (status != PRDB_SDK_SQL_OK) goto failed_having_v17;
            if (parsed_kind == PRDB_SDK_SQL_AGGREGATE_COUNT_STAR) {
                if (having.current.kind != TOKEN_STAR) {
                    status = PRDB_SDK_SQL_SYNTAX_ERROR;
                    having.error_offset = having.current.offset;
                    goto failed_having_v17;
                }
            } else if (having.current.kind != TOKEN_IDENTIFIER ||
                       !prdb_sdk_internal_span_matches_name(
                           grouped,
                           grouped->aggregate_column,
                           sql + having.current.offset,
                           having.current.length)) {
                status = PRDB_SDK_SQL_SYNTAX_ERROR;
                having.error_offset = having.current.offset;
                goto failed_having_v17;
            }
            status = advance(&having);
            if (status != PRDB_SDK_SQL_OK ||
                having.current.kind != TOKEN_RIGHT_PAREN) {
                status = status == PRDB_SDK_SQL_OK
                             ? PRDB_SDK_SQL_SYNTAX_ERROR
                             : status;
                having.error_offset = having.current.offset;
                goto failed_having_v17;
            }
            status = advance(&having);
            if (status != PRDB_SDK_SQL_OK) goto failed_having_v17;
            status = comparison_code(having.current.kind,
                                     &result.having_comparison);
            if (status != PRDB_SDK_SQL_OK) {
                having.error_offset = having.current.offset;
                goto failed_having_v17;
            }
            status = advance(&having);
            if (status != PRDB_SDK_SQL_OK) goto failed_having_v17;
            if (having.current.kind == TOKEN_PARAMETER) {
                status = parse_unsigned(
                    &having, having.current,
                    PRDB_SDK_SQL_MAX_PARAMETER_INDEX, 0,
                    &result.having_parameter_index);
                if (status != PRDB_SDK_SQL_OK) goto failed_having_v17;
            } else {
                status = parse_nonnegative_bigint_literal(
                    &having, &result.having_literal);
                if (status != PRDB_SDK_SQL_OK) goto failed_having_v17;
                result.has_having_literal = 1;
                /* The frozen v15 binder derives the selected aggregate type
                 * through its existing parameter binding path. V17 replaces
                 * this placeholder operand in the v17 runner. */
                result.having_parameter_index = 1;
            }
            result.has_having = 1;
            status = advance(&having);
            if (status != PRDB_SDK_SQL_OK) goto failed_having_v17;
            if (!token_is_keyword(&having, "ORDER") &&
                !token_is_keyword(&having, "LIMIT") &&
                having.current.kind != TOKEN_SEMICOLON &&
                having.current.kind != TOKEN_END) {
                status = PRDB_SDK_SQL_SYNTAX_ERROR;
                having.error_offset = having.current.offset;
                goto failed_having_v17;
            }
        }
    }
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
    *out = result;
    return PRDB_SDK_SQL_OK;

failed_having_v17:
    if (error_offset_out != NULL) *error_offset_out = having.error_offset;
    return fail_at(&having, status);
failed_v17:
    if (error_offset_out != NULL) *error_offset_out = scan.error_offset;
    return fail_at(&scan, status);
}

PRDB_SDK_INTERNAL int32_t prdb_sdk_sql_bind_select_v17(
    const prdb_sdk_select_v1 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v1 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v1 *out, uint32_t out_capacity) {
    prdb_sdk_select_binding_v1 result = {0};
    int32_t status;

    if (statement == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_BIND_INVALID_ARGUMENT;
    }
    if (statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->has_having_literal > 1 ||
        statement->having_literal > UINT64_C(0x7fffffffffffffff) ||
        (statement->has_having_literal == 0 &&
         statement->having_literal != 0) ||
        (statement->has_having_literal == 1 &&
         (statement->has_having != 1 ||
          statement->having_parameter_index != 1))) {
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    }
    status = prdb_sdk_sql_bind_select_v15(
        statement, relation_name, relation_name_len, columns,
        column_count, &result, sizeof(result));
    if (status != PRDB_SDK_SQL_BIND_OK) return status;
    if (statement->has_having_literal &&
        result.having_type_code != PRDB_SDK_VALUE_TYPE_BIGINT) {
        return PRDB_SDK_SQL_BIND_LITERAL_TYPE_MISMATCH;
    }
    result.has_having_literal = statement->has_having_literal;
    result.having_literal = statement->having_literal;
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
    *out = result;
    return PRDB_SDK_SQL_BIND_OK;
}

static int32_t add_having_node_v18(
    parser *state, prdb_sdk_select_v1 *statement,
    prdb_sdk_sql_having_node_v1 node, uint32_t *index_out) {
    if (statement->having_node_count >=
        PRDB_SDK_SQL_MAX_HAVING_NODES) {
        state->error_offset = state->current.offset;
        return PRDB_SDK_SQL_LIMIT_EXCEEDED;
    }
    *index_out = statement->having_node_count;
    statement->having_nodes[statement->having_node_count++] = node;
    return PRDB_SDK_SQL_OK;
}

static int32_t parse_having_or_v18(parser *state,
                                  prdb_sdk_select_v1 *statement,
                                  uint32_t depth, uint32_t *index_out);

static int32_t parse_having_primary_v18(parser *state,
                                        prdb_sdk_select_v1 *statement,
                                        uint32_t depth,
                                        uint32_t *index_out) {
    prdb_sdk_sql_having_node_v1 node = {0};
    prdb_sdk_select_v1 *grouped = statement;
    int is_count = token_is_keyword(state, "COUNT");
    int32_t status;

    if (depth > PRDB_SDK_SQL_MAX_EXPRESSION_DEPTH) {
        state->error_offset = state->current.offset;
        return PRDB_SDK_SQL_LIMIT_EXCEEDED;
    }
    if (state->current.kind == TOKEN_LEFT_PAREN) {
        status = advance(state);
        if (status != PRDB_SDK_SQL_OK) return status;
        status = parse_having_or_v18(state, statement, depth + 1, index_out);
        if (status != PRDB_SDK_SQL_OK) return status;
        if (state->current.kind != TOKEN_RIGHT_PAREN) {
            state->error_offset = state->current.offset;
            return PRDB_SDK_SQL_SYNTAX_ERROR;
        }
        return advance(state);
    }

    if (is_count) {
        if (grouped->aggregate_kind !=
                PRDB_SDK_SQL_AGGREGATE_COUNT_STAR &&
            !(state->having_features & HAVING_HIDDEN_COUNT)) {
            state->error_offset = state->current.offset;
            return PRDB_SDK_SQL_SYNTAX_ERROR;
        }
    } else if (token_is_keyword(state, "MIN") ||
               token_is_keyword(state, "MAX") ||
               token_is_keyword(state, "SUM") ||
               token_is_keyword(state, "AVG")) {
        uint32_t parsed_kind = token_is_keyword(state, "MIN")
                                   ? PRDB_SDK_SQL_AGGREGATE_MIN
                               : token_is_keyword(state, "MAX")
                                   ? PRDB_SDK_SQL_AGGREGATE_MAX
                               : token_is_keyword(state, "SUM")
                                   ? PRDB_SDK_SQL_AGGREGATE_SUM
                                   : PRDB_SDK_SQL_AGGREGATE_AVG;
        if (grouped->aggregate_kind != parsed_kind) {
            state->error_offset = state->current.offset;
            return PRDB_SDK_SQL_SYNTAX_ERROR;
        }
    } else {
        state->error_offset = state->current.offset;
        return PRDB_SDK_SQL_SYNTAX_ERROR;
    }
    status = advance(state);
    if (status != PRDB_SDK_SQL_OK ||
        state->current.kind != TOKEN_LEFT_PAREN) {
        state->error_offset = state->current.offset;
        return status == PRDB_SDK_SQL_OK ? PRDB_SDK_SQL_SYNTAX_ERROR : status;
    }
    status = advance(state);
    if (status != PRDB_SDK_SQL_OK) return status;
    if (is_count) {
        if (state->current.kind != TOKEN_STAR) {
            state->error_offset = state->current.offset;
            return PRDB_SDK_SQL_SYNTAX_ERROR;
        }
    } else if (state->current.kind != TOKEN_IDENTIFIER ||
               !prdb_sdk_internal_span_matches_name(grouped, grouped->aggregate_column,
                                  state->sql + state->current.offset,
                                  state->current.length)) {
        state->error_offset = state->current.offset;
        return PRDB_SDK_SQL_SYNTAX_ERROR;
    }
    status = advance(state);
    if (status != PRDB_SDK_SQL_OK ||
        state->current.kind != TOKEN_RIGHT_PAREN) {
        state->error_offset = state->current.offset;
        return status == PRDB_SDK_SQL_OK ? PRDB_SDK_SQL_SYNTAX_ERROR : status;
    }
    status = advance(state);
    if (status != PRDB_SDK_SQL_OK) return status;
    if ((state->having_features & HAVING_NULL_TESTS) && token_is_keyword(state, "IS")) {
        node.node_kind = PRDB_SDK_SQL_PREDICATE_IS_NULL;
        status = advance(state);
        if (status != PRDB_SDK_SQL_OK) return status;
        if (token_is_keyword(state, "NOT")) {
            node.node_kind = PRDB_SDK_SQL_PREDICATE_IS_NOT_NULL;
            status = advance(state);
            if (status != PRDB_SDK_SQL_OK) return status;
        }
        if (!token_is_keyword(state, "NULL")) {
            state->error_offset = state->current.offset;
            return PRDB_SDK_SQL_SYNTAX_ERROR;
        }
        status = advance(state);
        if (status != PRDB_SDK_SQL_OK) return status;
        return add_having_node_v18(state, statement, node, index_out);
    }
    node.node_kind = PRDB_SDK_SQL_PREDICATE_COMPARE;
    status = comparison_code(state->current.kind, &node.comparison);
    if (status != PRDB_SDK_SQL_OK) {
        state->error_offset = state->current.offset;
        return status;
    }
    status = advance(state);
    if (status != PRDB_SDK_SQL_OK) return status;
    if (state->current.kind == TOKEN_PARAMETER) {
        node.operand_kind = PRDB_SDK_SQL_HAVING_OPERAND_PARAMETER;
        status = parse_unsigned(state, state->current,
                                PRDB_SDK_SQL_MAX_PARAMETER_INDEX, 0,
                                &node.parameter_index);
    } else {
        node.operand_kind = PRDB_SDK_SQL_HAVING_OPERAND_BIGINT_LITERAL;
        status = parse_signed_bigint_literal_v18(state, &node.literal);
    }
    if (status != PRDB_SDK_SQL_OK) return status;
    status = advance(state);
    if (status != PRDB_SDK_SQL_OK) return status;
    return add_having_node_v18(state, statement, node, index_out);
}

static int32_t parse_having_unary_v18(parser *state,
                                     prdb_sdk_select_v1 *statement,
                                     uint32_t depth, uint32_t *index_out) {
    prdb_sdk_sql_having_node_v1 node = {0};
    int32_t status;
    if (depth > PRDB_SDK_SQL_MAX_EXPRESSION_DEPTH) {
        state->error_offset = state->current.offset;
        return PRDB_SDK_SQL_LIMIT_EXCEEDED;
    }
    if (!token_is_keyword(state, "NOT")) {
        return parse_having_primary_v18(state, statement, depth, index_out);
    }
    node.node_kind = PRDB_SDK_SQL_PREDICATE_NOT;
    status = advance(state);
    if (status != PRDB_SDK_SQL_OK) return status;
    status = parse_having_unary_v18(state, statement, depth + 1,
                                    &node.left_child);
    if (status != PRDB_SDK_SQL_OK) return status;
    return add_having_node_v18(state, statement, node, index_out);
}

static int32_t parse_having_and_v18(parser *state,
                                   prdb_sdk_select_v1 *statement,
                                   uint32_t depth, uint32_t *index_out) {
    int32_t status = parse_having_unary_v18(state, statement, depth,
                                            index_out);
    if (status != PRDB_SDK_SQL_OK) return status;
    while (token_is_keyword(state, "AND")) {
        prdb_sdk_sql_having_node_v1 node = {0};
        node.node_kind = PRDB_SDK_SQL_PREDICATE_AND;
        node.left_child = *index_out;
        status = advance(state);
        if (status != PRDB_SDK_SQL_OK) return status;
        status = parse_having_unary_v18(state, statement, depth,
                                        &node.right_child);
        if (status != PRDB_SDK_SQL_OK) return status;
        status = add_having_node_v18(state, statement, node, index_out);
        if (status != PRDB_SDK_SQL_OK) return status;
    }
    return PRDB_SDK_SQL_OK;
}

static int32_t parse_having_or_v18(parser *state,
                                  prdb_sdk_select_v1 *statement,
                                  uint32_t depth, uint32_t *index_out) {
    int32_t status = parse_having_and_v18(state, statement, depth,
                                          index_out);
    if (status != PRDB_SDK_SQL_OK) return status;
    while (token_is_keyword(state, "OR")) {
        prdb_sdk_sql_having_node_v1 node = {0};
        node.node_kind = PRDB_SDK_SQL_PREDICATE_OR;
        node.left_child = *index_out;
        status = advance(state);
        if (status != PRDB_SDK_SQL_OK) return status;
        status = parse_having_and_v18(state, statement, depth,
                                      &node.right_child);
        if (status != PRDB_SDK_SQL_OK) return status;
        status = add_having_node_v18(state, statement, node, index_out);
        if (status != PRDB_SDK_SQL_OK) return status;
    }
    return PRDB_SDK_SQL_OK;
}

static int32_t parse_select_v18_mode(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_select_v1 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out,
                                     int having_features) {
    uint8_t base_sql[PRDB_SDK_SQL_MAX_QUERY_BYTES];
    parser scan, having;
    prdb_sdk_select_v1 result = {0};
    token having_token = {TOKEN_END, 0, 0};
    uint32_t group_by_end = 0, clause_end, root = 0;
    uint32_t min_group_keys = (having_features & GROUP_SINGLE_KEY) ? 1 : 2;
    int saw_group = 0;
    int32_t status;

    if (error_offset_out != NULL) *error_offset_out = 0;
    if (sql == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_INVALID_ARGUMENT;
    }
    if (sql_len == 0) return PRDB_SDK_SQL_SYNTAX_ERROR;
    if (sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES) {
        if (error_offset_out != NULL)
            *error_offset_out = (uint32_t)PRDB_SDK_SQL_MAX_QUERY_BYTES;
        return PRDB_SDK_SQL_LIMIT_EXCEEDED;
    }
    scan = (parser){sql, (uint32_t)sql_len, 0, {TOKEN_END, 0, 0}, 0,
                    PRDB_SDK_SQL_OK, 0};
    status = advance(&scan);
    if (status != PRDB_SDK_SQL_OK) goto failed_v18;
    while (scan.current.kind != TOKEN_END &&
           scan.current.kind != TOKEN_SEMICOLON) {
        if (token_is_keyword(&scan, "GROUP")) {
            saw_group = 1;
        } else if (saw_group && token_is_keyword(&scan, "BY")) {
            group_by_end = scan.current.offset + scan.current.length;
            saw_group = 0;
        } else {
            saw_group = 0;
        }
        if (token_is_keyword(&scan, "HAVING")) {
            if (having_token.kind != TOKEN_END) {
                status = PRDB_SDK_SQL_SYNTAX_ERROR;
                scan.error_offset = scan.current.offset;
                goto failed_v18;
            }
            having_token = scan.current;
        }
        status = advance(&scan);
        if (status != PRDB_SDK_SQL_OK) goto failed_v18;
    }
    if (having_token.kind == TOKEN_END) {
        status = prdb_sdk_internal_parse_select_v15_mode(
            sql, sql_len, &result, sizeof(result), error_offset_out,
            min_group_keys);
        if (status != PRDB_SDK_SQL_OK) return status;
        result.sql = sql;
    } else {
        if (group_by_end == 0 || having_token.offset < group_by_end) {
            status = PRDB_SDK_SQL_SYNTAX_ERROR;
            scan.error_offset = having_token.offset;
            goto failed_v18;
        }
        memcpy(base_sql, sql, (size_t)sql_len);
        clause_end = (uint32_t)sql_len;
        having = (parser){sql, (uint32_t)sql_len,
                          having_token.offset + having_token.length,
                          {TOKEN_END, 0, 0}, 0, PRDB_SDK_SQL_OK,
                          having_features};
        status = advance(&having);
        if (status != PRDB_SDK_SQL_OK) goto failed_having_v18;
        while (having.current.kind != TOKEN_END &&
               having.current.kind != TOKEN_SEMICOLON) {
            if (token_is_keyword(&having, "ORDER") ||
                token_is_keyword(&having, "LIMIT")) {
                clause_end = having.current.offset;
                break;
            }
            status = advance(&having);
            if (status != PRDB_SDK_SQL_OK) goto failed_having_v18;
        }
        for (uint32_t index = having_token.offset; index < clause_end; ++index)
            base_sql[index] = ' ';
        status = prdb_sdk_internal_parse_select_v15_mode(
            base_sql, sql_len, &result, sizeof(result),
            error_offset_out, min_group_keys);
        if (status != PRDB_SDK_SQL_OK) return status;
        result.sql = sql;
        having = (parser){sql, (uint32_t)sql_len,
                          having_token.offset + having_token.length,
                          {TOKEN_END, 0, 0}, 0, PRDB_SDK_SQL_OK,
                          having_features};
        status = advance(&having);
        if (status != PRDB_SDK_SQL_OK) goto failed_having_v18;
        status = parse_having_or_v18(&having, &result, 0, &root);
        if (status != PRDB_SDK_SQL_OK) goto failed_having_v18;
        if (!token_is_keyword(&having, "ORDER") &&
            !token_is_keyword(&having, "LIMIT") &&
            having.current.kind != TOKEN_SEMICOLON &&
            having.current.kind != TOKEN_END) {
            status = PRDB_SDK_SQL_SYNTAX_ERROR;
            having.error_offset = having.current.offset;
            goto failed_having_v18;
        }
        result.having_root = root;
    }
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
    *out = result;
    return PRDB_SDK_SQL_OK;

failed_having_v18:
    if (error_offset_out != NULL) *error_offset_out = having.error_offset;
    return fail_at(&having, status);
failed_v18:
    if (error_offset_out != NULL) *error_offset_out = scan.error_offset;
    return fail_at(&scan, status);
}

PRDB_SDK_INTERNAL int32_t prdb_sdk_sql_parse_select_v18(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v1 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out) {
    return parse_select_v18_mode(sql, sql_len, out, out_capacity,
                                 error_offset_out, 0);
}

static int validate_having_node_v18(
    const prdb_sdk_select_v1 *statement, uint32_t index, uint8_t *visited,
    int allow_null_tests) {
    const prdb_sdk_sql_having_node_v1 *node;
    if (index >= statement->having_node_count || visited[index]) return 0;
    visited[index] = 1;
    node = &statement->having_nodes[index];
    if (node->node_kind == PRDB_SDK_SQL_PREDICATE_NOT) {
        if (node->right_child != 0 || node->comparison != 0 ||
            node->operand_kind != 0 || node->parameter_index != 0 ||
            node->literal != 0 || node->left_child >= index) return 0;
        return validate_having_node_v18(statement, node->left_child, visited,
                                         allow_null_tests);
    }
    if (node->node_kind == PRDB_SDK_SQL_PREDICATE_AND ||
        node->node_kind == PRDB_SDK_SQL_PREDICATE_OR) {
        if (node->comparison != 0 || node->operand_kind != 0 ||
            node->parameter_index != 0 || node->literal != 0 ||
            node->left_child >= index || node->right_child >= index) return 0;
        return validate_having_node_v18(statement, node->left_child, visited,
                                         allow_null_tests) &&
               validate_having_node_v18(statement, node->right_child, visited,
                                         allow_null_tests);
    }
    if (allow_null_tests &&
        (node->node_kind == PRDB_SDK_SQL_PREDICATE_IS_NULL ||
         node->node_kind == PRDB_SDK_SQL_PREDICATE_IS_NOT_NULL)) {
        return node->left_child == 0 && node->right_child == 0 &&
               node->comparison == 0 && node->operand_kind == 0 &&
               node->parameter_index == 0 && node->literal == 0;
    }
    if (node->node_kind != PRDB_SDK_SQL_PREDICATE_COMPARE ||
        node->comparison < PRDB_SDK_SQL_COMPARE_EQUAL ||
        node->comparison > PRDB_SDK_SQL_COMPARE_GREATER_EQUAL ||
        node->left_child != 0 || node->right_child != 0) return 0;
    if (node->operand_kind == PRDB_SDK_SQL_HAVING_OPERAND_PARAMETER) {
        return node->parameter_index > 0 &&
               node->parameter_index <= PRDB_SDK_SQL_MAX_PARAMETER_INDEX &&
               node->literal == 0;
    }
    if (node->operand_kind == PRDB_SDK_SQL_HAVING_OPERAND_BIGINT_LITERAL) {
        /* The literal field is the two's-complement BIGINT bit pattern. */
        return node->parameter_index == 0;
    }
    return 0;
}

static int32_t bind_select_v18_mode(
    const prdb_sdk_select_v1 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v1 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v1 *out, uint32_t out_capacity,
    int allow_null_tests, uint32_t min_group_keys) {
    prdb_sdk_select_binding_v1 result = {0};
    prdb_sdk_select_v1 base;
    uint8_t visited[PRDB_SDK_SQL_MAX_HAVING_NODES] = {0};
    int32_t status;
    if (statement == NULL || out == NULL || out_capacity < sizeof(*out))
        return PRDB_SDK_SQL_BIND_INVALID_ARGUMENT;
    if (statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->having_node_count > PRDB_SDK_SQL_MAX_HAVING_NODES ||
        (statement->having_node_count == 0 && statement->having_root != 0) ||
        (statement->having_node_count != 0 &&
         statement->having_root >= statement->having_node_count) ||
        statement->has_having != 0 ||
        statement->having_comparison != 0 ||
        statement->having_parameter_index != 0)
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    for (uint32_t index = statement->having_node_count;
         index < PRDB_SDK_SQL_MAX_HAVING_NODES; ++index) {
        const prdb_sdk_sql_having_node_v1 zero = {0};
        if (memcmp(&statement->having_nodes[index], &zero, sizeof(zero)) != 0)
            return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    }
    if (statement->having_node_count != 0) {
        if (!validate_having_node_v18(statement, statement->having_root,
                                      visited, allow_null_tests))
            return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
        for (uint32_t index = 0; index < statement->having_node_count; ++index)
            if (!visited[index]) return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    }
    base = (*statement);
    if (statement->having_node_count != 0) {
        /* Ask the frozen v15 binder to derive aggregate comparison metadata;
         * v18's AST remains the only authoritative HAVING representation. */
        base.has_having = 1;
        base.having_comparison = PRDB_SDK_SQL_COMPARE_EQUAL;
        base.having_parameter_index = 1;
    }
    status = prdb_sdk_internal_bind_select_v15_mode(
        &base, relation_name, relation_name_len, columns, column_count,
        &result, sizeof(result), min_group_keys);
    if (status != PRDB_SDK_SQL_BIND_OK) return status;
    if (statement->having_node_count != 0) {
        for (uint32_t index = 0; index < statement->having_node_count; ++index) {
            if (statement->having_nodes[index].node_kind ==
                    PRDB_SDK_SQL_PREDICATE_COMPARE &&
                statement->having_nodes[index].operand_kind ==
                    PRDB_SDK_SQL_HAVING_OPERAND_BIGINT_LITERAL &&
                result.having_type_code !=
                    PRDB_SDK_VALUE_TYPE_BIGINT)
                return PRDB_SDK_SQL_BIND_LITERAL_TYPE_MISMATCH;
        }
    }
    result.has_having = 0;
    result.having_comparison = 0;
    result.having_parameter_index = 0;
    result.having_node_count = statement->having_node_count;
    result.having_root = statement->having_root;
    result.having_type_code = result.having_type_code;
    result.having_decimal_precision =
        result.having_decimal_precision;
    result.having_decimal_scale = result.having_decimal_scale;
    memcpy(result.having_nodes, statement->having_nodes,
           sizeof(result.having_nodes));
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
    *out = result;
    return PRDB_SDK_SQL_BIND_OK;
}

PRDB_SDK_INTERNAL int32_t prdb_sdk_sql_bind_select_v18(
    const prdb_sdk_select_v1 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v1 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v1 *out, uint32_t out_capacity) {
    return bind_select_v18_mode(statement, relation_name, relation_name_len,
                                columns, column_count, out, out_capacity, 0, 2);
}

typedef struct numeric_literal_v21 {
    uint8_t coefficient[16];
    uint8_t scale;
    uint32_t offset;
} numeric_literal_v21;

static int decimal_multiply_add_digit_v21(uint8_t magnitude[16],
                                          uint8_t digit) {
    uint16_t carry = digit;
    for (size_t index = 16; index > 0; --index) {
        uint16_t value = (uint16_t)magnitude[index - 1] * 10 + carry;
        magnitude[index - 1] = (uint8_t)value;
        carry = value >> 8;
    }
    return carry == 0;
}

static int32_t parse_numeric_literal_v21(const uint8_t *sql, uint32_t start,
                                         uint32_t end,
                                         numeric_literal_v21 *out) {
    uint8_t digits[PRDB_SDK_SQL_MAX_QUERY_BYTES];
    uint8_t magnitude[16] = {0};
    uint32_t position = start, digit_count = 0, fraction_digits = 0;
    uint32_t leading_zeroes = 0, exponent_offset = 0;
    int exponent = 0, exponent_negative = 0, negative = 0, saw_dot = 0;
    int saw_exponent = 0;
    int64_t scale;

    if (out == NULL || start >= end || end > PRDB_SDK_SQL_MAX_QUERY_BYTES)
        return PRDB_SDK_SQL_SYNTAX_ERROR;
    if (sql[position] == '-' || sql[position] == '+') {
        negative = sql[position] == '-';
        ++position;
        while (position < end && is_space(sql[position])) ++position;
    }
    while (position < end) {
        uint8_t byte = sql[position];
        if (byte >= '0' && byte <= '9') {
            if (digit_count >= sizeof(digits))
                return PRDB_SDK_SQL_LIMIT_EXCEEDED;
            digits[digit_count++] = (uint8_t)(byte - '0');
            if (saw_dot) ++fraction_digits;
            ++position;
            continue;
        }
        if (byte == '.' && !saw_dot && !saw_exponent) {
            saw_dot = 1;
            ++position;
            continue;
        }
        if ((byte == 'e' || byte == 'E') && !saw_exponent) {
            saw_exponent = 1;
            exponent_offset = position++;
            if (position < end && (sql[position] == '+' || sql[position] == '-')) {
                exponent_negative = sql[position] == '-';
                ++position;
            }
            if (position == end) return PRDB_SDK_SQL_SYNTAX_ERROR;
            for (; position < end; ++position) {
                uint8_t exponent_byte = sql[position];
                uint32_t digit;
                if (exponent_byte < '0' || exponent_byte > '9')
                    return PRDB_SDK_SQL_SYNTAX_ERROR;
                digit = (uint32_t)(exponent_byte - '0');
                if (exponent > 1000 ||
                    (exponent == 1000 && digit != 0))
                    return PRDB_SDK_SQL_LIMIT_EXCEEDED;
                exponent = exponent * 10 + (int)digit;
            }
            break;
        }
        return PRDB_SDK_SQL_SYNTAX_ERROR;
    }
    if (digit_count == 0) return PRDB_SDK_SQL_SYNTAX_ERROR;
    if (exponent_negative) exponent = -exponent;
    scale = (int64_t)fraction_digits - exponent;
    while (leading_zeroes < digit_count && digits[leading_zeroes] == 0)
        ++leading_zeroes;
    if (leading_zeroes == digit_count) {
        memset(out->coefficient, 0, sizeof(out->coefficient));
        out->scale = 0;
        out->offset = start;
        return PRDB_SDK_SQL_OK;
    }
    if (scale < 0) {
        uint64_t appended = (uint64_t)(-scale);
        if (appended > 38 || digit_count - leading_zeroes + appended > 38)
            return PRDB_SDK_SQL_LIMIT_EXCEEDED;
        if ((uint64_t)digit_count + appended > sizeof(digits))
            return PRDB_SDK_SQL_LIMIT_EXCEEDED;
        while (appended-- != 0) digits[digit_count++] = 0;
        scale = 0;
    }
    while (scale > 0 && digit_count > leading_zeroes &&
           digits[digit_count - 1] == 0) {
        --digit_count;
        --scale;
    }
    if (scale > 38 || digit_count - leading_zeroes > 38)
        return PRDB_SDK_SQL_LIMIT_EXCEEDED;
    for (uint32_t index = leading_zeroes; index < digit_count; ++index) {
        if (!decimal_multiply_add_digit_v21(magnitude, digits[index]))
            return PRDB_SDK_SQL_LIMIT_EXCEEDED;
    }
    if (!prdb_sdk_internal_decimal_fits_precision(magnitude, 38))
        return PRDB_SDK_SQL_LIMIT_EXCEEDED;
    if (negative) {
        uint16_t carry = 1;
        for (size_t index = sizeof(magnitude); index > 0; --index) {
            uint16_t value = (uint16_t)(uint8_t)~magnitude[index - 1] + carry;
            magnitude[index - 1] = (uint8_t)value;
            carry = value >> 8;
        }
    }
    memcpy(out->coefficient, magnitude, sizeof(out->coefficient));
    out->scale = (uint8_t)scale;
    out->offset = start;
    (void)exponent_offset;
    return PRDB_SDK_SQL_OK;
}

static int32_t scan_and_mask_having_literals_v21(
    const uint8_t *sql, uint32_t sql_len, uint8_t *masked_sql,
    numeric_literal_v21 *literals, uint32_t *literal_count,
    uint32_t *error_offset) {
    parser scan = {sql, sql_len, 0, {TOKEN_END, 0, 0}, 0,
                   PRDB_SDK_SQL_OK, 0};
    int in_having = 0;
    token previous = {TOKEN_END, 0, 0};
    int32_t status = advance(&scan);
    *literal_count = 0;
    if (status != PRDB_SDK_SQL_OK) {
        *error_offset = scan.error_offset;
        return status;
    }
    while (scan.current.kind != TOKEN_END &&
           scan.current.kind != TOKEN_SEMICOLON) {
        if (token_is_keyword(&scan, "HAVING")) in_having = 1;
        else if (in_having &&
                 (token_is_keyword(&scan, "ORDER") ||
                  token_is_keyword(&scan, "LIMIT")))
            in_having = 0;
        if (in_having && scan.current.kind == TOKEN_NUMBER) {
            uint32_t start = scan.current.offset;
            uint32_t end = start + scan.current.length;
            int is_negative = previous.kind == TOKEN_MINUS;
            int is_positive = previous.kind == TOKEN_PLUS;
            if ((is_negative || is_positive) && previous.offset < start) {
                uint32_t gap = start - (previous.offset + previous.length);
                int only_space = 1;
                for (uint32_t index = 0; index < gap; ++index)
                    if (!is_space(sql[previous.offset + previous.length + index]))
                        only_space = 0;
                if (only_space) start = previous.offset;
            }
            if (*literal_count >= PRDB_SDK_SQL_MAX_HAVING_NODES) {
                *error_offset = start;
                return PRDB_SDK_SQL_LIMIT_EXCEEDED;
            }
            status = parse_numeric_literal_v21(sql, start, end,
                                               &literals[*literal_count]);
            if (status != PRDB_SDK_SQL_OK) {
                *error_offset = start;
                return status;
            }
            ++*literal_count;
            masked_sql[start] = '0';
            memset(masked_sql + start + 1, ' ', end - start - 1);
        }
        previous = scan.current;
        status = advance(&scan);
        if (status != PRDB_SDK_SQL_OK) {
            *error_offset = scan.error_offset;
            return status;
        }
    }
    return PRDB_SDK_SQL_OK;
}

static int32_t parse_select_v21_mode(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_select_v1 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out,
                                     int having_features) {
    uint8_t masked_sql[PRDB_SDK_SQL_MAX_QUERY_BYTES];
    numeric_literal_v21 literals[PRDB_SDK_SQL_MAX_HAVING_NODES];
    prdb_sdk_select_v1 result = {0};
    uint32_t literal_count = 0, literal_index = 0, error_offset = 0;
    int32_t status;

    if (error_offset_out != NULL) *error_offset_out = 0;
    if (sql == NULL || out == NULL || out_capacity < sizeof(*out))
        return PRDB_SDK_SQL_INVALID_ARGUMENT;
    if (sql_len == 0) return PRDB_SDK_SQL_SYNTAX_ERROR;
    if (sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES) {
        if (error_offset_out != NULL)
            *error_offset_out = (uint32_t)PRDB_SDK_SQL_MAX_QUERY_BYTES;
        return PRDB_SDK_SQL_LIMIT_EXCEEDED;
    }
    memcpy(masked_sql, sql, (size_t)sql_len);
    status = scan_and_mask_having_literals_v21(
        sql, (uint32_t)sql_len, masked_sql, literals, &literal_count,
        &error_offset);
    if (status != PRDB_SDK_SQL_OK) {
        if (error_offset_out != NULL) *error_offset_out = error_offset;
        return status;
    }
    status = parse_select_v18_mode(masked_sql, sql_len, &result,
                                   sizeof(result), &error_offset,
                                   having_features);
    if (status != PRDB_SDK_SQL_OK) {
        if (error_offset_out != NULL) *error_offset_out = error_offset;
        return status;
    }
    result.sql = sql;
    for (uint32_t index = 0; index < result.having_node_count; ++index) {
        prdb_sdk_sql_having_node_v1 *base_node =
            &result.having_nodes[index];
        prdb_sdk_sql_having_node_v1 *node = &result.having_nodes[index];
        (*node) = *base_node;
        if (base_node->node_kind == PRDB_SDK_SQL_PREDICATE_COMPARE &&
            base_node->operand_kind == PRDB_SDK_SQL_HAVING_OPERAND_BIGINT_LITERAL) {
            if (literal_index >= literal_count) {
                if (error_offset_out != NULL)
                    *error_offset_out = literals[0].offset;
                return PRDB_SDK_SQL_SYNTAX_ERROR;
            }
            node->operand_kind =
                PRDB_SDK_SQL_HAVING_OPERAND_NUMERIC_LITERAL;
            node->literal = 0;
            node->parameter_index = 0;
            memcpy(node->coefficient, literals[literal_index].coefficient,
                   sizeof(node->coefficient));
            node->scale = literals[literal_index].scale;
            ++literal_index;
        }
    }
    if (literal_index != literal_count) {
        if (error_offset_out != NULL)
            *error_offset_out = literals[literal_index].offset;
        return PRDB_SDK_SQL_SYNTAX_ERROR;
    }
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
    *out = result;
    return PRDB_SDK_SQL_OK;
}

PRDB_SDK_INTERNAL int32_t prdb_sdk_sql_parse_select_v21(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v1 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out) {
    return parse_select_v21_mode(sql, sql_len, out, out_capacity,
                                 error_offset_out, 0);
}

static void decimal_magnitude_v21(const uint8_t coefficient[16],
                                  uint8_t magnitude[16], int *negative) {
    *negative = (coefficient[0] & 0x80) != 0;
    memcpy(magnitude, coefficient, 16);
    if (*negative) {
        uint16_t carry = 1;
        for (size_t index = 16; index > 0; --index) {
            uint16_t value = (uint16_t)(uint8_t)~magnitude[index - 1] + carry;
            magnitude[index - 1] = (uint8_t)value;
            carry = value >> 8;
        }
    }
}

static int bytes_are_zero(const uint8_t *bytes, size_t length) {
    for (size_t index = 0; index < length; ++index)
        if (bytes[index] != 0) return 0;
    return 1;
}

static int numeric_literal_is_canonical_v21(
    const prdb_sdk_sql_having_node_v1 *node) {
    uint8_t magnitude[16];
    int negative;
    uint16_t remainder = 0;
    int is_zero = 1;
    decimal_magnitude_v21(node->coefficient, magnitude, &negative);
    (void)negative;
    if (node->scale > 38 || !prdb_sdk_internal_decimal_fits_precision(node->coefficient, 38))
        return 0;
    for (size_t index = 0; index < sizeof(magnitude); ++index) {
        if (magnitude[index] != 0) is_zero = 0;
        uint16_t value = (uint16_t)remainder * 256 + magnitude[index];
        remainder = (uint16_t)(value % 10);
    }
    return is_zero ? node->scale == 0
                   : node->scale == 0 || remainder != 0;
}

static int32_t rescale_numeric_literal_v21(
    const prdb_sdk_sql_having_node_v1 *node, uint32_t target_type,
    uint8_t target_scale, uint8_t coefficient_out[16]) {
    uint8_t magnitude[16];
    uint32_t source_scale = node->scale;
    int negative;
    decimal_magnitude_v21(node->coefficient, magnitude, &negative);
    if (target_type != PRDB_SDK_VALUE_TYPE_BIGINT &&
        target_type != PRDB_SDK_VALUE_TYPE_DECIMAL)
        return 0;
    if (target_type == PRDB_SDK_VALUE_TYPE_BIGINT) target_scale = 0;
    if (source_scale < target_scale) {
        for (uint32_t step = source_scale; step < target_scale; ++step) {
            uint16_t carry = 0;
            for (size_t index = sizeof(magnitude); index > 0; --index) {
                uint16_t value = (uint16_t)magnitude[index - 1] * 10 + carry;
                magnitude[index - 1] = (uint8_t)value;
                carry = value >> 8;
            }
            if (carry != 0) return 0;
        }
    } else {
        for (uint32_t step = target_scale; step < source_scale; ++step) {
            uint16_t remainder = 0;
            for (size_t index = 0; index < sizeof(magnitude); ++index) {
                uint16_t value = (uint16_t)remainder * 256 + magnitude[index];
                magnitude[index] = (uint8_t)(value / 10);
                remainder = (uint16_t)(value % 10);
            }
            if (remainder != 0) return 0;
        }
    }
    if (negative) {
        uint16_t carry = 1;
        for (size_t index = sizeof(magnitude); index > 0; --index) {
            uint16_t value = (uint16_t)(uint8_t)~magnitude[index - 1] + carry;
            magnitude[index - 1] = (uint8_t)value;
            carry = value >> 8;
        }
    }
    if (target_type == PRDB_SDK_VALUE_TYPE_BIGINT) {
        uint8_t fill = negative ? 0xff : 0;
        for (size_t index = 0; index < 8; ++index)
            if (magnitude[index] != fill) return 0;
        if (((magnitude[8] & 0x80) != 0) != negative) return 0;
    } else if (!prdb_sdk_internal_decimal_fits_precision(magnitude, 38)) {
        return 0;
    }
    memcpy(coefficient_out, magnitude, 16);
    return 1;
}

static int32_t bind_select_v21_mode(
    const prdb_sdk_select_v1 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v1 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v1 *out, uint32_t out_capacity,
    uint64_t having_count_mask, int allow_null_tests, uint32_t min_group_keys) {
    prdb_sdk_select_v1 base_statement;
    prdb_sdk_select_binding_v1 result = {0};
    int32_t status;
    if (statement == NULL || out == NULL || out_capacity < sizeof(*out))
        return PRDB_SDK_SQL_BIND_INVALID_ARGUMENT;
    if (statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION)
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    base_statement = (*statement);
    for (uint32_t index = 0; index < PRDB_SDK_SQL_MAX_HAVING_NODES;
         ++index) {
        const prdb_sdk_sql_having_node_v1 *node =
            &statement->having_nodes[index];
        if (index >= statement->having_node_count) {
            const prdb_sdk_sql_having_node_v1 zero = {0};
            if (memcmp(node, &zero, sizeof(zero)) != 0)
                return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
            continue;
        }
        if (node->node_kind != statement->having_nodes[index].node_kind ||
            node->left_child != statement->having_nodes[index].left_child ||
            node->right_child != statement->having_nodes[index].right_child ||
            node->comparison != statement->having_nodes[index].comparison ||
            node->operand_kind !=
                (statement->having_nodes[index].node_kind ==
                         PRDB_SDK_SQL_PREDICATE_COMPARE &&
                         statement->having_nodes[index].operand_kind ==
                             PRDB_SDK_SQL_HAVING_OPERAND_BIGINT_LITERAL
                     ? PRDB_SDK_SQL_HAVING_OPERAND_NUMERIC_LITERAL
                     : statement->having_nodes[index].operand_kind) ||
            (node->operand_kind == PRDB_SDK_SQL_HAVING_OPERAND_NUMERIC_LITERAL &&
             (node->parameter_index != 0 || node->literal != 0 ||
              !numeric_literal_is_canonical_v21(node))) ||
            (node->operand_kind !=
                 PRDB_SDK_SQL_HAVING_OPERAND_NUMERIC_LITERAL &&
             (node->scale != 0 ||
              !bytes_are_zero(node->coefficient,
                              sizeof(node->coefficient))))
            ) return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
        for (size_t reserved = 0; reserved < sizeof(node->reserved); ++reserved)
            if (node->reserved[reserved] != 0)
                return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
        base_statement.having_nodes[index] = (*node);
        if (node->operand_kind == PRDB_SDK_SQL_HAVING_OPERAND_NUMERIC_LITERAL) {
            base_statement.having_nodes[index].operand_kind =
                PRDB_SDK_SQL_HAVING_OPERAND_PARAMETER;
            base_statement.having_nodes[index].parameter_index = 1;
        }
    }
    status = bind_select_v18_mode(
        &base_statement, relation_name, relation_name_len, columns, column_count,
        &result, sizeof(result), allow_null_tests, min_group_keys);
    if (status != PRDB_SDK_SQL_BIND_OK) return status;
    for (uint32_t index = 0; index < statement->having_node_count; ++index) {
        const prdb_sdk_sql_having_node_v1 *input =
            &statement->having_nodes[index];
        prdb_sdk_sql_having_node_v1 *bound = &result.having_nodes[index];
        (*bound) = result.having_nodes[index];
        if (input->node_kind == PRDB_SDK_SQL_PREDICATE_COMPARE &&
            input->operand_kind == PRDB_SDK_SQL_HAVING_OPERAND_NUMERIC_LITERAL) {
            uint32_t type = (having_count_mask & (UINT64_C(1) << index))
                                ? PRDB_SDK_VALUE_TYPE_BIGINT
                                : result.having_type_code;
            uint8_t scale = type == PRDB_SDK_VALUE_TYPE_DECIMAL
                                ? result.having_decimal_scale
                                : 0;
            if (!rescale_numeric_literal_v21(input, type, scale,
                                             bound->coefficient))
                return PRDB_SDK_SQL_BIND_LITERAL_TYPE_MISMATCH;
            bound->operand_kind = PRDB_SDK_SQL_HAVING_OPERAND_NUMERIC_LITERAL;
            bound->parameter_index = 0;
            bound->literal = 0;
            bound->scale = scale;
        } else {
            memcpy(bound->coefficient, input->coefficient,
                   sizeof(bound->coefficient));
            bound->scale = input->scale;
        }
    }
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
    *out = result;
    return PRDB_SDK_SQL_BIND_OK;
}

PRDB_SDK_INTERNAL int32_t prdb_sdk_sql_bind_select_v21(
    const prdb_sdk_select_v1 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v1 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v1 *out, uint32_t out_capacity) {
    return bind_select_v21_mode(statement, relation_name, relation_name_len,
                                columns, column_count, out, out_capacity, 0, 0, 2);
}

static int32_t parse_select_v22_mode(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v1 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out,
                                      int having_features) {
    prdb_sdk_select_v1 result = {0};
    parser scan;
    uint64_t count_ordinals = 0;
    uint32_t comparison_ordinal = 0;
    int in_having = 0;
    int32_t status;
    if (error_offset_out != NULL) *error_offset_out = 0;
    if (sql == NULL || out == NULL || out_capacity < sizeof(*out))
        return PRDB_SDK_SQL_INVALID_ARGUMENT;
    status = parse_select_v21_mode(sql, sql_len, &result,
                                   sizeof(result), error_offset_out,
                                   having_features);
    if (status != PRDB_SDK_SQL_OK) return status;
    scan = (parser){sql, (uint32_t)sql_len, 0, {TOKEN_END, 0, 0}, 0,
                    PRDB_SDK_SQL_OK, 0};
    status = advance(&scan);
    while (status == PRDB_SDK_SQL_OK && scan.current.kind != TOKEN_END &&
           scan.current.kind != TOKEN_SEMICOLON) {
        if (token_is_keyword(&scan, "HAVING")) in_having = 1;
        else if (in_having && (token_is_keyword(&scan, "ORDER") ||
                               token_is_keyword(&scan, "LIMIT")))
            in_having = 0;
        if (in_having &&
            (token_is_keyword(&scan, "COUNT") ||
             token_is_keyword(&scan, "MIN") ||
             token_is_keyword(&scan, "MAX") ||
             token_is_keyword(&scan, "SUM") ||
             token_is_keyword(&scan, "AVG"))) {
            parser look = scan;
            int is_count = token_is_keyword(&scan, "COUNT");
            if (advance(&look) == PRDB_SDK_SQL_OK &&
                look.current.kind == TOKEN_LEFT_PAREN &&
                advance(&look) == PRDB_SDK_SQL_OK &&
                (look.current.kind == TOKEN_STAR ||
                 look.current.kind == TOKEN_IDENTIFIER) &&
                advance(&look) == PRDB_SDK_SQL_OK &&
                look.current.kind == TOKEN_RIGHT_PAREN &&
                advance(&look) == PRDB_SDK_SQL_OK &&
                (look.current.kind == TOKEN_EQUAL ||
                 look.current.kind == TOKEN_NOT_EQUAL ||
                 look.current.kind == TOKEN_LESS ||
                 look.current.kind == TOKEN_LESS_EQUAL ||
                 look.current.kind == TOKEN_GREATER ||
                 look.current.kind == TOKEN_GREATER_EQUAL ||
                 ((having_features & HAVING_NULL_TESTS) && token_is_keyword(&look, "IS")))) {
                if (is_count && comparison_ordinal < 64)
                    count_ordinals |= UINT64_C(1) << comparison_ordinal;
                ++comparison_ordinal;
                scan = look;
            }
        }
        status = advance(&scan);
    }
    if (status != PRDB_SDK_SQL_OK) {
        if (error_offset_out != NULL) *error_offset_out = scan.error_offset;
        return status;
    }
    comparison_ordinal = 0;
    for (uint32_t index = 0; index < result.having_node_count;
         ++index) {
        uint32_t kind = result.having_nodes[index].node_kind;
        if (kind != PRDB_SDK_SQL_PREDICATE_COMPARE &&
            kind != PRDB_SDK_SQL_PREDICATE_IS_NULL &&
            kind != PRDB_SDK_SQL_PREDICATE_IS_NOT_NULL) continue;
        if (comparison_ordinal < 64 &&
            (count_ordinals & (UINT64_C(1) << comparison_ordinal)) != 0 &&
            result.aggregate_kind !=
                PRDB_SDK_SQL_AGGREGATE_COUNT_STAR)
            result.having_count_mask |= UINT64_C(1) << index;
        ++comparison_ordinal;
    }
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
    *out = result;
    return PRDB_SDK_SQL_OK;
}

static int32_t bind_select_v22_mode(
    const prdb_sdk_select_v1 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v1 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v1 *out, uint32_t out_capacity,
    int allow_null_tests, uint32_t min_group_keys) {
    prdb_sdk_select_binding_v1 result = {0};
    uint64_t valid_mask = 0;
    int32_t status;
    if (statement == NULL || out == NULL || out_capacity < sizeof(*out))
        return PRDB_SDK_SQL_BIND_INVALID_ARGUMENT;
    if (statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION)
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    if (statement->having_node_count >
        PRDB_SDK_SQL_MAX_HAVING_NODES)
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    for (uint32_t index = 0; index < statement->having_node_count;
         ++index) {
        uint32_t kind = statement->having_nodes[index].node_kind;
        if (kind == PRDB_SDK_SQL_PREDICATE_COMPARE ||
            (allow_null_tests &&
             (kind == PRDB_SDK_SQL_PREDICATE_IS_NULL ||
              kind == PRDB_SDK_SQL_PREDICATE_IS_NOT_NULL)))
            valid_mask |= UINT64_C(1) << index;
    }
    if ((statement->having_count_mask & ~valid_mask) != 0)
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    status = bind_select_v21_mode(statement, relation_name,
                                  relation_name_len, columns, column_count,
                                  &result, sizeof(result),
                                  statement->having_count_mask, allow_null_tests,
                                  min_group_keys);
    if (status != PRDB_SDK_SQL_BIND_OK) return status;
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
    result.having_count_mask = statement->having_count_mask;
    *out = result;
    return PRDB_SDK_SQL_BIND_OK;
}

PRDB_SDK_INTERNAL int32_t prdb_sdk_sql_parse_select_v22(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v1 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out) {
    return parse_select_v22_mode(sql, sql_len, out, out_capacity,
                                  error_offset_out, HAVING_HIDDEN_COUNT);
}

PRDB_SDK_INTERNAL int32_t prdb_sdk_sql_bind_select_v22(
    const prdb_sdk_select_v1 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v1 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v1 *out, uint32_t out_capacity) {
    return bind_select_v22_mode(statement, relation_name, relation_name_len,
                                columns, column_count, out, out_capacity, 0, 2);
}

static int32_t parse_select_v23_mode(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v1 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out,
                                      uint32_t abi_version) {
    prdb_sdk_select_v1 result = {0};
    if (error_offset_out != NULL) *error_offset_out = 0;
    if (out == NULL || out_capacity < sizeof(*out))
        return PRDB_SDK_SQL_INVALID_ARGUMENT;
    int32_t status = parse_select_v22_mode(
        sql, sql_len, &result, sizeof(result), error_offset_out,
        HAVING_HIDDEN_COUNT | HAVING_NULL_TESTS |
            (abi_version == PRDB_SDK_SQL_ABI_V1_VERSION ? GROUP_SINGLE_KEY : 0));
    if (status == PRDB_SDK_SQL_OK) {
        out->struct_size = sizeof(*out);
        out->abi_version = abi_version;
        (*out) = result;
        out->having_count_mask = result.having_count_mask;
    }
    return status;
}

static int32_t bind_select_v23_mode(
    const prdb_sdk_select_v1 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v1 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v1 *out, uint32_t out_capacity,
    uint32_t abi_version) {
    prdb_sdk_select_v1 input = {0};
    prdb_sdk_select_binding_v1 result = {0};
    if (statement == NULL || out == NULL || out_capacity < sizeof(*out))
        return PRDB_SDK_SQL_BIND_INVALID_ARGUMENT;
    if (statement->struct_size < sizeof(*statement) ||
        statement->abi_version != abi_version)
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    input.struct_size = sizeof(input);
    input.abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
    input = (*statement);
    input.having_count_mask = statement->having_count_mask;
    int32_t status = bind_select_v22_mode(
        &input, relation_name, relation_name_len, columns, column_count,
        &result, sizeof(result), 1,
        abi_version == PRDB_SDK_SQL_ABI_V1_VERSION ? 1 : 2);
    if (status != PRDB_SDK_SQL_BIND_OK) return status;
    out->struct_size = sizeof(*out);
    out->abi_version = abi_version;
    (*out) = result;
    out->having_count_mask = result.having_count_mask;
    return PRDB_SDK_SQL_BIND_OK;
}

PRDB_SDK_INTERNAL int32_t prdb_sdk_sql_parse_select_v23(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v1 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out) {
    return parse_select_v23_mode(sql, sql_len, out, out_capacity,
                                  error_offset_out, PRDB_SDK_SQL_ABI_V1_VERSION);
}

PRDB_SDK_INTERNAL int32_t prdb_sdk_sql_bind_select_v23(
    const prdb_sdk_select_v1 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v1 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v1 *out, uint32_t out_capacity) {
    return bind_select_v23_mode(statement, relation_name, relation_name_len,
                                columns, column_count, out, out_capacity,
                                PRDB_SDK_SQL_ABI_V1_VERSION);
}

PRDB_SDK_INTERNAL int32_t prdb_sdk_sql_parse_select_v24(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v1 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out) {
    uint8_t rewritten[PRDB_SDK_SQL_MAX_QUERY_BYTES];
    parser scan;
    int32_t status;
    prdb_sdk_sql_span_v1 selected_argument = {0};

    if (error_offset_out != NULL) *error_offset_out = 0;
    if (sql == NULL || out == NULL || out_capacity < sizeof(*out))
        return PRDB_SDK_SQL_INVALID_ARGUMENT;
    if (sql_len == 0) return PRDB_SDK_SQL_SYNTAX_ERROR;
    if (sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES)
        return PRDB_SDK_SQL_LIMIT_EXCEEDED;
    memcpy(rewritten, sql, (size_t)sql_len);
    scan = (parser){sql, (uint32_t)sql_len, 0, {TOKEN_END, 0, 0}, 0,
                    PRDB_SDK_SQL_OK, 0};
    status = advance(&scan);
    while (status == PRDB_SDK_SQL_OK && scan.current.kind != TOKEN_END &&
           scan.current.kind != TOKEN_SEMICOLON) {
        if (token_is_keyword(&scan, "COUNT")) {
            parser look = scan;
            if (advance(&look) == PRDB_SDK_SQL_OK &&
                look.current.kind == TOKEN_LEFT_PAREN &&
                advance(&look) == PRDB_SDK_SQL_OK &&
                look.current.kind == TOKEN_IDENTIFIER) {
                token argument = look.current;
                if (advance(&look) == PRDB_SDK_SQL_OK &&
                    look.current.kind == TOKEN_RIGHT_PAREN) {
                    rewritten[scan.current.offset] = 'M';
                    rewritten[scan.current.offset + 1] = 'I';
                    rewritten[scan.current.offset + 2] = 'N';
                    rewritten[scan.current.offset + 3] = ' ';
                    rewritten[scan.current.offset + 4] = ' ';
                    if (selected_argument.offset == 0) {
                        selected_argument.offset = argument.offset;
                        selected_argument.length = argument.length;
                    }
                }
            }
        }
        status = advance(&scan);
    }
    if (status != PRDB_SDK_SQL_OK) {
        if (error_offset_out != NULL) *error_offset_out = scan.error_offset;
        return status;
    }
    status = parse_select_v23_mode(rewritten, sql_len, out, out_capacity,
                                   error_offset_out,
                                   PRDB_SDK_SQL_ABI_V1_VERSION);
    if (status != PRDB_SDK_SQL_OK) return status;
    {
        prdb_sdk_select_v1 *grouped =
            out;
        if (selected_argument.length != 0 &&
            grouped->aggregate_column.offset == selected_argument.offset &&
            grouped->aggregate_column.length == selected_argument.length) {
            grouped->aggregate_kind =
                PRDB_SDK_SQL_AGGREGATE_COUNT_COLUMN;
        }
        /* The parser used a stack copy to preserve the frozen earlier APIs. */
        grouped->sql = sql;
    }
    return PRDB_SDK_SQL_OK;
}

PRDB_SDK_INTERNAL int32_t prdb_sdk_sql_bind_select_v24(
    const prdb_sdk_select_v1 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v1 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v1 *out, uint32_t out_capacity) {
    prdb_sdk_select_v1 compatible = {0};
    prdb_sdk_select_binding_v1 result = {0};
    prdb_sdk_select_v1 *grouped;
    prdb_sdk_select_binding_v1 *bound_grouped;
    int32_t status;
    if (statement == NULL || out == NULL || out_capacity < sizeof(*out))
        return PRDB_SDK_SQL_BIND_INVALID_ARGUMENT;
    if (statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION)
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    compatible = *statement;
    grouped = &compatible;
    if (grouped->aggregate_kind !=
        PRDB_SDK_SQL_AGGREGATE_COUNT_COLUMN) {
        return bind_select_v23_mode(
            statement, relation_name, relation_name_len, columns,
            column_count, out, out_capacity, PRDB_SDK_SQL_ABI_V1_VERSION);
    }
    grouped->aggregate_kind = PRDB_SDK_SQL_AGGREGATE_MIN;
    status = bind_select_v23_mode(
        &compatible, relation_name, relation_name_len, columns, column_count,
        &result, sizeof(result), PRDB_SDK_SQL_ABI_V1_VERSION);
    if (status == PRDB_SDK_SQL_BIND_INVALID_AGGREGATE_TYPE) {
        grouped->aggregate_kind = PRDB_SDK_SQL_AGGREGATE_COUNT_STAR;
        status = bind_select_v23_mode(
            &compatible, relation_name, relation_name_len, columns,
            column_count, &result, sizeof(result),
            PRDB_SDK_SQL_ABI_V1_VERSION);
        if (status == PRDB_SDK_SQL_BIND_OK) {
            uint32_t ordinal = 0;
            status = prdb_sdk_internal_find_column_shape_v1(
                grouped, grouped->aggregate_column, columns,
                column_count, &ordinal);
            if (status == PRDB_SDK_SQL_BIND_OK) {
                uint32_t index = 0;
                prdb_sdk_sql_bound_column_v1 *aggregate =
                    &result.aggregate_column;
                while (index < column_count &&
                       columns[index].ordinal != ordinal) ++index;
                if (index == column_count) {
                    status = PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
                } else {
                    aggregate->ordinal = ordinal;
                    aggregate->type_code = columns[index].type_code;
                    aggregate->decimal_precision =
                        columns[index].decimal_precision;
                    aggregate->decimal_scale = columns[index].decimal_scale;
                }
            }
        }
    }
    if (status != PRDB_SDK_SQL_BIND_OK) return status;
    bound_grouped = &result;
    bound_grouped->aggregate_kind =
        PRDB_SDK_SQL_AGGREGATE_COUNT_COLUMN;
    if (statement->having_node_count != 0)
        result.having_type_code = PRDB_SDK_VALUE_TYPE_BIGINT;
    {
        prdb_sdk_select_binding_v1 *ordered = &result;
        for (uint32_t index = 0; index < ordered->order_count; ++index) {
            if (ordered->order_items[index].item.kind ==
                PRDB_SDK_SQL_ORDER_AGGREGATE) {
                ordered->order_items[index].type_code =
                    PRDB_SDK_VALUE_TYPE_BIGINT;
                ordered->order_items[index].decimal_precision = 0;
                ordered->order_items[index].decimal_scale = 0;
            }
        }
    }
    *out = result;
    return PRDB_SDK_SQL_BIND_OK;
}
