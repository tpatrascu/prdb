#include "sql_internal.h"

static int32_t add_predicate_node(parser *state, prdb_sdk_select_v2 *statement,
                                  prdb_sdk_predicate_node_v2 node,
                                  uint32_t *index_out) {
    if (statement->predicate_count >= PRDB_SDK_SQL_MAX_PREDICATE_NODES) {
        state->error_offset = state->current.offset;
        return PRDB_SDK_SQL_LIMIT_EXCEEDED;
    }
    *index_out = statement->predicate_count;
    statement->predicates[statement->predicate_count++] = node;
    return PRDB_SDK_SQL_OK;
}


static int32_t parse_predicate_primary_v2(parser *state,
                                          prdb_sdk_select_v2 *statement,
                                          uint32_t depth,
                                          uint32_t *index_out) {
    prdb_sdk_predicate_node_v2 node = {0};
    int32_t status;

    if (depth > PRDB_SDK_SQL_MAX_EXPRESSION_DEPTH) {
        state->error_offset = state->current.offset;
        return PRDB_SDK_SQL_LIMIT_EXCEEDED;
    }
    if (state->current.kind == TOKEN_LEFT_PAREN) {
        if (advance(state) != PRDB_SDK_SQL_OK) {
            return state->error;
        }
        status = prdb_sdk_internal_parse_predicate_or_v2(state, statement, depth + 1,
                                       index_out);
        if (status != PRDB_SDK_SQL_OK) {
            return status;
        }
        if (state->current.kind != TOKEN_RIGHT_PAREN) {
            state->error_offset = state->current.offset;
            return PRDB_SDK_SQL_SYNTAX_ERROR;
        }
        return advance(state);
    }

    status = expect_identifier(state, &node.column);
    if (status != PRDB_SDK_SQL_OK) {
        return status;
    }
    if ((state->having_features & PARSER_ALLOW_IN) != 0 &&
        token_is_keyword(state, "IN")) {
        uint32_t root = 0;
        uint32_t value_count = 0;
        if (advance(state) != PRDB_SDK_SQL_OK) return state->error;
        if (state->current.kind != TOKEN_LEFT_PAREN) {
            state->error_offset = state->current.offset;
            return PRDB_SDK_SQL_SYNTAX_ERROR;
        }
        if (advance(state) != PRDB_SDK_SQL_OK) return state->error;
        for (;;) {
            prdb_sdk_predicate_node_v2 equality = {0};
            uint32_t leaf;
            if (state->current.kind != TOKEN_PARAMETER) {
                state->error_offset = state->current.offset;
                return PRDB_SDK_SQL_SYNTAX_ERROR;
            }
            if (++value_count > PRDB_SDK_SQL_V26_MAX_IN_VALUES) {
                state->error_offset = state->current.offset;
                return PRDB_SDK_SQL_LIMIT_EXCEEDED;
            }
            equality.node_kind = PRDB_SDK_SQL_PREDICATE_COMPARE;
            equality.comparison = PRDB_SDK_SQL_COMPARE_EQUAL;
            equality.column = node.column;
            status = parse_unsigned(state, state->current,
                                    PRDB_SDK_SQL_MAX_PARAMETER_INDEX, 0,
                                    &equality.parameter_index);
            if (status != PRDB_SDK_SQL_OK) return status;
            status = add_predicate_node(state, statement, equality, &leaf);
            if (status != PRDB_SDK_SQL_OK) return status;
            if (value_count == 1) {
                root = leaf;
            } else {
                prdb_sdk_predicate_node_v2 disjunction = {0};
                disjunction.node_kind = PRDB_SDK_SQL_PREDICATE_OR;
                disjunction.left_child = root;
                disjunction.right_child = leaf;
                status = add_predicate_node(state, statement, disjunction, &root);
                if (status != PRDB_SDK_SQL_OK) return status;
            }
            if (advance(state) != PRDB_SDK_SQL_OK) return state->error;
            if (state->current.kind != TOKEN_COMMA) break;
            if (advance(state) != PRDB_SDK_SQL_OK) return state->error;
        }
        if (state->current.kind != TOKEN_RIGHT_PAREN) {
            state->error_offset = state->current.offset;
            return PRDB_SDK_SQL_SYNTAX_ERROR;
        }
        if (advance(state) != PRDB_SDK_SQL_OK) return state->error;
        *index_out = root;
        return PRDB_SDK_SQL_OK;
    }
    if (token_is_keyword(state, "IS")) {
        if (advance(state) != PRDB_SDK_SQL_OK) {
            return state->error;
        }
        if (token_is_keyword(state, "NOT")) {
            node.node_kind = PRDB_SDK_SQL_PREDICATE_IS_NOT_NULL;
            if (advance(state) != PRDB_SDK_SQL_OK) {
                return state->error;
            }
        } else {
            node.node_kind = PRDB_SDK_SQL_PREDICATE_IS_NULL;
        }
        if (!token_is_keyword(state, "NULL")) {
            state->error_offset = state->current.offset;
            return PRDB_SDK_SQL_SYNTAX_ERROR;
        }
        if (advance(state) != PRDB_SDK_SQL_OK) {
            return state->error;
        }
        return add_predicate_node(state, statement, node, index_out);
    }

    node.node_kind = PRDB_SDK_SQL_PREDICATE_COMPARE;
    status = comparison_code(state->current.kind, &node.comparison);
    if (status != PRDB_SDK_SQL_OK) {
        state->error_offset = state->current.offset;
        return status;
    }
    if (advance(state) != PRDB_SDK_SQL_OK) {
        return state->error;
    }
    if (state->current.kind != TOKEN_PARAMETER) {
        state->error_offset = state->current.offset;
        return PRDB_SDK_SQL_SYNTAX_ERROR;
    }
    status = parse_unsigned(state, state->current,
                            PRDB_SDK_SQL_MAX_PARAMETER_INDEX, 0,
                            &node.parameter_index);
    if (status != PRDB_SDK_SQL_OK) {
        return status;
    }
    if (advance(state) != PRDB_SDK_SQL_OK) {
        return state->error;
    }
    return add_predicate_node(state, statement, node, index_out);
}

static int32_t parse_predicate_unary_v2(parser *state,
                                        prdb_sdk_select_v2 *statement,
                                        uint32_t depth,
                                        uint32_t *index_out) {
    prdb_sdk_predicate_node_v2 node = {0};
    int32_t status;

    if (depth > PRDB_SDK_SQL_MAX_EXPRESSION_DEPTH) {
        state->error_offset = state->current.offset;
        return PRDB_SDK_SQL_LIMIT_EXCEEDED;
    }
    if (!token_is_keyword(state, "NOT")) {
        return parse_predicate_primary_v2(state, statement, depth, index_out);
    }
    node.node_kind = PRDB_SDK_SQL_PREDICATE_NOT;
    if (advance(state) != PRDB_SDK_SQL_OK) {
        return state->error;
    }
    status = parse_predicate_unary_v2(state, statement, depth + 1,
                                      &node.left_child);
    if (status != PRDB_SDK_SQL_OK) {
        return status;
    }
    return add_predicate_node(state, statement, node, index_out);
}

static int32_t parse_predicate_and_v2(parser *state,
                                      prdb_sdk_select_v2 *statement,
                                      uint32_t depth, uint32_t *index_out) {
    int32_t status = parse_predicate_unary_v2(state, statement, depth,
                                              index_out);
    if (status != PRDB_SDK_SQL_OK) {
        return status;
    }
    while (token_is_keyword(state, "AND")) {
        prdb_sdk_predicate_node_v2 node = {0};
        node.node_kind = PRDB_SDK_SQL_PREDICATE_AND;
        node.left_child = *index_out;
        if (advance(state) != PRDB_SDK_SQL_OK) {
            return state->error;
        }
        status = parse_predicate_unary_v2(state, statement, depth,
                                          &node.right_child);
        if (status != PRDB_SDK_SQL_OK) {
            return status;
        }
        status = add_predicate_node(state, statement, node, index_out);
        if (status != PRDB_SDK_SQL_OK) {
            return status;
        }
    }
    return PRDB_SDK_SQL_OK;
}

PRDB_SDK_INTERNAL int32_t prdb_sdk_internal_parse_predicate_or_v2(parser *state,
                                     prdb_sdk_select_v2 *statement,
                                     uint32_t depth, uint32_t *index_out) {
    int32_t status = parse_predicate_and_v2(state, statement, depth,
                                            index_out);
    if (status != PRDB_SDK_SQL_OK) {
        return status;
    }
    while (token_is_keyword(state, "OR")) {
        prdb_sdk_predicate_node_v2 node = {0};
        node.node_kind = PRDB_SDK_SQL_PREDICATE_OR;
        node.left_child = *index_out;
        if (advance(state) != PRDB_SDK_SQL_OK) {
            return state->error;
        }
        status = parse_predicate_and_v2(state, statement, depth,
                                        &node.right_child);
        if (status != PRDB_SDK_SQL_OK) {
            return status;
        }
        status = add_predicate_node(state, statement, node, index_out);
        if (status != PRDB_SDK_SQL_OK) {
            return status;
        }
    }
    return PRDB_SDK_SQL_OK;
}

int32_t prdb_sdk_sql_parse_select_v1(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_select_v1 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out) {
    parser state;
    prdb_sdk_select_v1 result;
    int32_t status;

    if (error_offset_out != NULL) {
        *error_offset_out = 0;
    }
    if (sql == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_INVALID_ARGUMENT;
    }
    if (sql_len == 0) {
        return PRDB_SDK_SQL_SYNTAX_ERROR;
    }
    if (sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES) {
        if (error_offset_out != NULL) {
            *error_offset_out = (uint32_t)PRDB_SDK_SQL_MAX_QUERY_BYTES;
        }
        return PRDB_SDK_SQL_LIMIT_EXCEEDED;
    }
    state = (parser){sql, (uint32_t)sql_len, 0, {TOKEN_END, 0, 0}, 0,
                     PRDB_SDK_SQL_OK, 0};
    result = (prdb_sdk_select_v1){0};
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
    result.sql = sql;
    result.sql_len = sql_len;

    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) {
        goto failed;
    }
    if (!token_is_keyword(&state, "SELECT")) {
        if (state.current.kind == TOKEN_IDENTIFIER) {
            status = PRDB_SDK_SQL_UNSUPPORTED_STATEMENT;
            state.error_offset = state.current.offset;
        } else {
            status = PRDB_SDK_SQL_SYNTAX_ERROR;
            state.error_offset = state.current.offset;
        }
        goto failed;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed;
    }
    if (state.current.kind == TOKEN_STAR) {
        result.projection_star = 1;
        if (advance(&state) != PRDB_SDK_SQL_OK) {
            status = state.error;
            goto failed;
        }
    } else {
        for (;;) {
            if (result.projection_count >=
                PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS) {
                status = PRDB_SDK_SQL_LIMIT_EXCEEDED;
                state.error_offset = state.current.offset;
                goto failed;
            }
            status = expect_identifier(
                &state, &result.projection[result.projection_count]);
            if (status != PRDB_SDK_SQL_OK) {
                goto failed;
            }
            ++result.projection_count;
            if (state.current.kind != TOKEN_COMMA) {
                break;
            }
            if (advance(&state) != PRDB_SDK_SQL_OK) {
                status = state.error;
                goto failed;
            }
        }
    }
    if (!token_is_keyword(&state, "FROM")) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed;
    }
    status = expect_identifier(&state, &result.relation);
    if (status != PRDB_SDK_SQL_OK) {
        goto failed;
    }
    if (token_is_keyword(&state, "WHERE")) {
        uint32_t parameter_index;
        result.has_where = 1;
        if (advance(&state) != PRDB_SDK_SQL_OK) {
            status = state.error;
            goto failed;
        }
        status = expect_identifier(&state, &result.where_column);
        if (status != PRDB_SDK_SQL_OK) {
            goto failed;
        }
        if (token_is_keyword(&state, "IS")) {
            uint32_t is_not_null = 0;
            if (advance(&state) != PRDB_SDK_SQL_OK) {
                status = state.error;
                goto failed;
            }
            if (token_is_keyword(&state, "NOT")) {
                is_not_null = 1;
                if (advance(&state) != PRDB_SDK_SQL_OK) {
                    status = state.error;
                    goto failed;
                }
            }
            if (!token_is_keyword(&state, "NULL")) {
                status = PRDB_SDK_SQL_SYNTAX_ERROR;
                state.error_offset = state.current.offset;
                goto failed;
            }
            result.comparison = is_not_null
                                    ? PRDB_SDK_SQL_COMPARE_IS_NOT_NULL
                                    : PRDB_SDK_SQL_COMPARE_IS_NULL;
            if (advance(&state) != PRDB_SDK_SQL_OK) {
                status = state.error;
                goto failed;
            }
        } else {
            status = comparison_code(state.current.kind, &result.comparison);
            if (status != PRDB_SDK_SQL_OK) {
                state.error_offset = state.current.offset;
                goto failed;
            }
            if (advance(&state) != PRDB_SDK_SQL_OK) {
                status = state.error;
                goto failed;
            }
            if (state.current.kind != TOKEN_PARAMETER) {
                status = PRDB_SDK_SQL_SYNTAX_ERROR;
                state.error_offset = state.current.offset;
                goto failed;
            }
            status = parse_unsigned(&state, state.current,
                                    PRDB_SDK_SQL_MAX_PARAMETER_INDEX, 0,
                                    &parameter_index);
            if (status != PRDB_SDK_SQL_OK) {
                goto failed;
            }
            result.parameter_index = parameter_index;
            if (advance(&state) != PRDB_SDK_SQL_OK) {
                status = state.error;
                goto failed;
            }
        }
    }
    if (token_is_keyword(&state, "LIMIT")) {
        token limit_token;
        result.has_limit = 1;
        if (advance(&state) != PRDB_SDK_SQL_OK) {
            status = state.error;
            goto failed;
        }
        limit_token = state.current;
        status = parse_unsigned(&state, limit_token, PRDB_SDK_SQL_MAX_LIMIT,
                                1, &result.limit);
        if (status != PRDB_SDK_SQL_OK) {
            goto failed;
        }
        if (advance(&state) != PRDB_SDK_SQL_OK) {
            status = state.error;
            goto failed;
        }
    }
    if (state.current.kind == TOKEN_SEMICOLON &&
        advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed;
    }
    if (state.current.kind != TOKEN_END) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed;
    }
    *out = result;
    return PRDB_SDK_SQL_OK;

failed:
    if (error_offset_out != NULL) {
        *error_offset_out = state.error_offset;
    }
    return fail_at(&state, status);
}

static int32_t parse_select_v2_internal(const uint8_t *sql, uint64_t sql_len,
                                        prdb_sdk_select_v2 *out,
                                        uint32_t out_capacity,
                                        uint32_t *error_offset_out,
                                        int allow_in) {
    parser state;
    prdb_sdk_select_v2 result;
    int32_t status;

    if (error_offset_out != NULL) {
        *error_offset_out = 0;
    }
    if (sql == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_INVALID_ARGUMENT;
    }
    if (sql_len == 0) {
        return PRDB_SDK_SQL_SYNTAX_ERROR;
    }
    if (sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES) {
        if (error_offset_out != NULL) {
            *error_offset_out = (uint32_t)PRDB_SDK_SQL_MAX_QUERY_BYTES;
        }
        return PRDB_SDK_SQL_LIMIT_EXCEEDED;
    }
    state = (parser){sql, (uint32_t)sql_len, 0, {TOKEN_END, 0, 0}, 0,
                     PRDB_SDK_SQL_OK, allow_in ? PARSER_ALLOW_IN : 0};
    result = (prdb_sdk_select_v2){0};
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V2_VERSION;
    result.sql = sql;
    result.sql_len = sql_len;

    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) {
        goto failed_v2;
    }
    if (!token_is_keyword(&state, "SELECT")) {
        status = state.current.kind == TOKEN_IDENTIFIER
                     ? PRDB_SDK_SQL_UNSUPPORTED_STATEMENT
                     : PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_v2;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_v2;
    }
    if (state.current.kind == TOKEN_STAR) {
        result.projection_star = 1;
        if (advance(&state) != PRDB_SDK_SQL_OK) {
            status = state.error;
            goto failed_v2;
        }
    } else {
        for (;;) {
            if (result.projection_count >=
                PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS) {
                status = PRDB_SDK_SQL_LIMIT_EXCEEDED;
                state.error_offset = state.current.offset;
                goto failed_v2;
            }
            status = expect_identifier(
                &state, &result.projection[result.projection_count]);
            if (status != PRDB_SDK_SQL_OK) {
                goto failed_v2;
            }
            ++result.projection_count;
            if (state.current.kind != TOKEN_COMMA) {
                break;
            }
            if (advance(&state) != PRDB_SDK_SQL_OK) {
                status = state.error;
                goto failed_v2;
            }
        }
    }
    if (!token_is_keyword(&state, "FROM")) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_v2;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_v2;
    }
    status = expect_identifier(&state, &result.relation);
    if (status != PRDB_SDK_SQL_OK) {
        goto failed_v2;
    }
    if (token_is_keyword(&state, "WHERE")) {
        if (advance(&state) != PRDB_SDK_SQL_OK) {
            status = state.error;
            goto failed_v2;
        }
        status = prdb_sdk_internal_parse_predicate_or_v2(&state, &result, 0,
                                       &result.predicate_root);
        if (status != PRDB_SDK_SQL_OK) {
            goto failed_v2;
        }
    }
    if (token_is_keyword(&state, "LIMIT")) {
        token limit_token;
        result.has_limit = 1;
        if (advance(&state) != PRDB_SDK_SQL_OK) {
            status = state.error;
            goto failed_v2;
        }
        limit_token = state.current;
        status = parse_unsigned(&state, limit_token, PRDB_SDK_SQL_MAX_LIMIT,
                                1, &result.limit);
        if (status != PRDB_SDK_SQL_OK) {
            goto failed_v2;
        }
        if (advance(&state) != PRDB_SDK_SQL_OK) {
            status = state.error;
            goto failed_v2;
        }
    }
    if (state.current.kind == TOKEN_SEMICOLON &&
        advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_v2;
    }
    if (state.current.kind != TOKEN_END) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_v2;
    }
    *out = result;
    return PRDB_SDK_SQL_OK;

failed_v2:
    if (error_offset_out != NULL) {
        *error_offset_out = state.error_offset;
    }
    return fail_at(&state, status);
}

int32_t prdb_sdk_sql_parse_select_v2(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_select_v2 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out) {
    return parse_select_v2_internal(sql, sql_len, out, out_capacity,
                                    error_offset_out, 0);
}

int32_t prdb_sdk_sql_parse_select_v26(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v26 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out) {
    prdb_sdk_select_v26 result = {0};
    int32_t status;
    if (out == NULL || out_capacity < sizeof(*out))
        return PRDB_SDK_SQL_INVALID_ARGUMENT;
    status = parse_select_v2_internal(sql, sql_len, &result.base,
                                      sizeof(result.base), error_offset_out, 1);
    if (status != PRDB_SDK_SQL_OK) return status;
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V26_VERSION;
    *out = result;
    return PRDB_SDK_SQL_OK;
}
PRDB_SDK_INTERNAL int32_t prdb_sdk_internal_validate_predicate_tree_v2(
    const prdb_sdk_select_v2 *statement) {
    uint8_t reachable[PRDB_SDK_SQL_MAX_PREDICATE_NODES] = {0};
    uint32_t pending[PRDB_SDK_SQL_MAX_PREDICATE_NODES];
    uint32_t pending_count = 0;

    if (statement->predicate_count > PRDB_SDK_SQL_MAX_PREDICATE_NODES ||
        statement->predicate_root >= statement->predicate_count) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (statement->predicate_root != statement->predicate_count - 1) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    for (uint32_t index = 0; index < statement->predicate_count; ++index) {
        const prdb_sdk_predicate_node_v2 *node =
            &statement->predicates[index];
        int span_valid = node->column.length != 0 &&
                         node->column.offset <= statement->sql_len &&
                         node->column.length <=
                             statement->sql_len - node->column.offset;

        switch (node->node_kind) {
        case PRDB_SDK_SQL_PREDICATE_COMPARE:
            if (!span_valid || node->left_child != 0 ||
                node->right_child != 0 ||
                node->comparison < PRDB_SDK_SQL_COMPARE_EQUAL ||
                node->comparison > PRDB_SDK_SQL_COMPARE_GREATER_EQUAL ||
                node->parameter_index == 0 ||
                node->parameter_index > PRDB_SDK_SQL_MAX_PARAMETER_INDEX) {
                return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
            }
            break;
        case PRDB_SDK_SQL_PREDICATE_IS_NULL:
        case PRDB_SDK_SQL_PREDICATE_IS_NOT_NULL:
            if (!span_valid || node->left_child != 0 ||
                node->right_child != 0 || node->comparison != 0 ||
                node->parameter_index != 0) {
                return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
            }
            break;
        case PRDB_SDK_SQL_PREDICATE_NOT:
            if (node->left_child >= index || node->right_child != 0 ||
                node->comparison != 0 || node->column.offset != 0 ||
                node->column.length != 0 || node->parameter_index != 0) {
                return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
            }
            break;
        case PRDB_SDK_SQL_PREDICATE_AND:
        case PRDB_SDK_SQL_PREDICATE_OR:
            if (node->left_child >= index || node->right_child >= index ||
                node->comparison != 0 || node->column.offset != 0 ||
                node->column.length != 0 || node->parameter_index != 0) {
                return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
            }
            break;
        default:
            return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
        }
    }

    reachable[statement->predicate_root] = 1;
    pending[pending_count++] = statement->predicate_root;
    while (pending_count != 0) {
        uint32_t index = pending[--pending_count];
        const prdb_sdk_predicate_node_v2 *node;

        node = &statement->predicates[index];
        if ((node->node_kind == PRDB_SDK_SQL_PREDICATE_NOT ||
             node->node_kind == PRDB_SDK_SQL_PREDICATE_AND ||
             node->node_kind == PRDB_SDK_SQL_PREDICATE_OR) &&
            !reachable[node->left_child]) {
            reachable[node->left_child] = 1;
            pending[pending_count++] = node->left_child;
        }
        if ((node->node_kind == PRDB_SDK_SQL_PREDICATE_AND ||
             node->node_kind == PRDB_SDK_SQL_PREDICATE_OR) &&
            !reachable[node->right_child]) {
            reachable[node->right_child] = 1;
            pending[pending_count++] = node->right_child;
        }
    }
    for (uint32_t index = 0; index < statement->predicate_count; ++index) {
        if (!reachable[index]) {
            return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
        }
    }
    return PRDB_SDK_SQL_EVAL_OK;
}

PRDB_SDK_INTERNAL uint8_t prdb_sdk_internal_fold_identifier_byte(uint8_t byte) {
    return byte >= 'A' && byte <= 'Z' ? (uint8_t)(byte + ('a' - 'A')) : byte;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_valid_identifier_bytes(const uint8_t *name, uint32_t name_len) {
    if (name == NULL || name_len == 0 ||
        name_len > PRDB_SDK_SQL_MAX_IDENTIFIER_BYTES ||
        !is_identifier_start(name[0])) {
        return 0;
    }
    for (uint32_t index = 1; index < name_len; ++index) {
        if (!is_identifier_continue(name[index])) {
            return 0;
        }
    }
    return 1;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_span_matches_name(const prdb_sdk_select_v2 *statement,
                             prdb_sdk_sql_span_v1 span, const uint8_t *name,
                             uint32_t name_len) {
    if (span.length != name_len || span.offset > statement->sql_len ||
        span.length > statement->sql_len - span.offset) {
        return 0;
    }
    for (uint32_t index = 0; index < name_len; ++index) {
        if (prdb_sdk_internal_fold_identifier_byte(statement->sql[span.offset + index]) !=
            prdb_sdk_internal_fold_identifier_byte(name[index])) {
            return 0;
        }
    }
    return 1;
}

PRDB_SDK_INTERNAL int32_t prdb_sdk_internal_validate_column_shape_v2(
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count) {
    if (column_count == 0 ||
        column_count > PRDB_SDK_SQL_MAX_RELATION_COLUMNS ||
        (column_count != 0 && columns == NULL)) {
        return PRDB_SDK_SQL_BIND_INVALID_ARGUMENT;
    }
    for (uint32_t index = 0; index < column_count; ++index) {
        const prdb_sdk_sql_column_shape_v2 *column = &columns[index];
        if (!prdb_sdk_internal_valid_identifier_bytes(column->name, column->name_len) ||
            column->ordinal != index ||
            column->type_code < PRDB_SDK_VALUE_TYPE_BOOLEAN ||
            column->type_code > PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ ||
            (column->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL
                 ? column->decimal_precision == 0 ||
                       column->decimal_precision > 38 ||
                       column->decimal_scale > column->decimal_precision
                 : column->decimal_precision != 0 ||
                       column->decimal_scale != 0)) {
            return PRDB_SDK_SQL_BIND_INVALID_SCHEMA;
        }
    }
    return PRDB_SDK_SQL_BIND_OK;
}

PRDB_SDK_INTERNAL int32_t prdb_sdk_internal_find_column_shape_v2(
    const prdb_sdk_select_v2 *statement, prdb_sdk_sql_span_v1 span,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    uint32_t *ordinal_out) {
    uint32_t found = UINT32_MAX;
    for (uint32_t index = 0; index < column_count; ++index) {
        if (!prdb_sdk_internal_span_matches_name(statement, span, columns[index].name,
                              columns[index].name_len)) {
            continue;
        }
        if (found != UINT32_MAX) {
            return PRDB_SDK_SQL_BIND_AMBIGUOUS_COLUMN;
        }
        found = index;
    }
    if (found == UINT32_MAX) {
        return PRDB_SDK_SQL_BIND_COLUMN_NOT_FOUND;
    }
    *ordinal_out = columns[found].ordinal;
    return PRDB_SDK_SQL_BIND_OK;
}

int32_t prdb_sdk_sql_bind_select_v2(
    const prdb_sdk_select_v2 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v2 *out, uint32_t out_capacity) {
    prdb_sdk_select_binding_v2 result = {0};
    int32_t status;

    if (statement == NULL || relation_name == NULL || out == NULL ||
        out_capacity < sizeof(*out) ||
        !prdb_sdk_internal_valid_identifier_bytes(relation_name, relation_name_len)) {
        return PRDB_SDK_SQL_BIND_INVALID_ARGUMENT;
    }
    if (statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V2_VERSION ||
        statement->sql == NULL || statement->sql_len == 0 ||
        statement->sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        statement->relation.length == 0 ||
        statement->relation.offset > statement->sql_len ||
        statement->relation.length >
            statement->sql_len - statement->relation.offset ||
        statement->projection_star > 1 ||
        (statement->projection_star == 1
             ? statement->projection_count != 0
             : statement->projection_count == 0 ||
                   statement->projection_count >
                       PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS) ||
        (statement->predicate_count == 0
             ? statement->predicate_root != 0
             : statement->predicate_count >
                       PRDB_SDK_SQL_MAX_PREDICATE_NODES ||
                   statement->predicate_root !=
                       statement->predicate_count - 1)) {
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    }
    if (!prdb_sdk_internal_span_matches_name(statement, statement->relation, relation_name,
                           relation_name_len)) {
        return PRDB_SDK_SQL_BIND_RELATION_MISMATCH;
    }
    for (uint32_t index = 0; index < statement->projection_count; ++index) {
        prdb_sdk_sql_span_v1 span = statement->projection[index];
        if (span.length == 0 || span.offset > statement->sql_len ||
            span.length > statement->sql_len - span.offset) {
            return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
        }
    }
    status = prdb_sdk_internal_validate_column_shape_v2(columns, column_count);
    if (status != PRDB_SDK_SQL_BIND_OK) {
        return status;
    }
    if (statement->predicate_count != 0 &&
        prdb_sdk_internal_validate_predicate_tree_v2(statement) != PRDB_SDK_SQL_EVAL_OK) {
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    }

    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V2_VERSION;
    result.projection_star = statement->projection_star;
    result.projection_count = statement->projection_count;
    result.relation_column_count = column_count;
    result.predicate_count = statement->predicate_count;
    if (statement->projection_star == 0) {
        for (uint32_t index = 0; index < statement->projection_count; ++index) {
            status = prdb_sdk_internal_find_column_shape_v2(
                statement, statement->projection[index], columns,
                column_count, &result.projection_ordinals[index]);
            if (status != PRDB_SDK_SQL_BIND_OK) {
                return status;
            }
        }
    }
    for (uint32_t index = 0; index < statement->predicate_count; ++index) {
        const prdb_sdk_predicate_node_v2 *node =
            &statement->predicates[index];
        uint32_t ordinal;
        if (node->node_kind != PRDB_SDK_SQL_PREDICATE_COMPARE &&
            node->node_kind != PRDB_SDK_SQL_PREDICATE_IS_NULL &&
            node->node_kind != PRDB_SDK_SQL_PREDICATE_IS_NOT_NULL) {
            continue;
        }
        status = prdb_sdk_internal_find_column_shape_v2(statement, node->column, columns,
                                      column_count, &ordinal);
        if (status != PRDB_SDK_SQL_BIND_OK) {
            return status;
        }
        result.predicate_columns[index] =
            (prdb_sdk_sql_bound_column_v2){
                ordinal, columns[ordinal].type_code,
                columns[ordinal].decimal_precision,
                columns[ordinal].decimal_scale};
    }
    *out = result;
    return PRDB_SDK_SQL_BIND_OK;
}

int32_t prdb_sdk_sql_parse_select_v3(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_select_v3 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out) {
    parser scan;
    parser suffix;
    prdb_sdk_select_v3 result = {0};
    uint32_t order_offset = 0;
    uint32_t by_end = 0;
    int found_order = 0;
    int32_t status;

    if (error_offset_out != NULL) {
        *error_offset_out = 0;
    }
    if (sql == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_INVALID_ARGUMENT;
    }
    if (sql_len == 0 || sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES) {
        return prdb_sdk_sql_parse_select_v2(
            sql, sql_len, &result.base, sizeof(result.base), error_offset_out);
    }

    scan = (parser){sql, (uint32_t)sql_len, 0, {TOKEN_END, 0, 0}, 0,
                    PRDB_SDK_SQL_OK, 0};
    status = advance(&scan);
    while (status == PRDB_SDK_SQL_OK && scan.current.kind != TOKEN_END) {
        if (token_is_keyword(&scan, "ORDER")) {
            parser lookahead = scan;
            if (advance(&lookahead) == PRDB_SDK_SQL_OK &&
                token_is_keyword(&lookahead, "BY")) {
                order_offset = scan.current.offset;
                by_end = lookahead.current.offset + lookahead.current.length;
                found_order = 1;
                break;
            }
        }
        status = advance(&scan);
    }

    if (!found_order) {
        status = prdb_sdk_sql_parse_select_v2(
            sql, sql_len, &result.base, sizeof(result.base), error_offset_out);
        if (status != PRDB_SDK_SQL_OK) {
            return status;
        }
        result.struct_size = sizeof(result);
        result.abi_version = PRDB_SDK_SQL_ABI_V3_VERSION;
        *out = result;
        return PRDB_SDK_SQL_OK;
    }

    status = prdb_sdk_sql_parse_select_v2(
        sql, order_offset, &result.base, sizeof(result.base), error_offset_out);
    if (status != PRDB_SDK_SQL_OK) {
        return status;
    }
    if (result.base.has_limit != 0) {
        if (error_offset_out != NULL) {
            *error_offset_out = order_offset;
        }
        return PRDB_SDK_SQL_SYNTAX_ERROR;
    }
    result.base.sql_len = sql_len;

    suffix = (parser){sql, (uint32_t)sql_len, by_end, {TOKEN_END, 0, 0}, 0,
                      PRDB_SDK_SQL_OK, 0};
    status = advance(&suffix);
    if (status != PRDB_SDK_SQL_OK) {
        goto failed_v3;
    }
    if (suffix.current.kind != TOKEN_IDENTIFIER ||
        token_is_keyword(&suffix, "ASC") ||
        token_is_keyword(&suffix, "DESC") ||
        token_is_keyword(&suffix, "LIMIT")) {
        suffix.error_offset = suffix.current.offset;
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        goto failed_v3;
    }
    result.order_by_column.offset = suffix.current.offset;
    result.order_by_column.length = suffix.current.length;
    status = advance(&suffix);
    if (status != PRDB_SDK_SQL_OK) {
        goto failed_v3;
    }
    if (token_is_keyword(&suffix, "ASC") ||
        token_is_keyword(&suffix, "DESC")) {
        result.order_descending = token_is_keyword(&suffix, "DESC")
                                      ? PRDB_SDK_SQL_ORDER_DESCENDING
                                      : PRDB_SDK_SQL_ORDER_ASCENDING;
        status = advance(&suffix);
        if (status != PRDB_SDK_SQL_OK) {
            goto failed_v3;
        }
    }
    result.has_order_by = 1;
    if (token_is_keyword(&suffix, "LIMIT")) {
        token limit_token;
        result.base.has_limit = 1;
        status = advance(&suffix);
        if (status != PRDB_SDK_SQL_OK) {
            goto failed_v3;
        }
        limit_token = suffix.current;
        status = parse_unsigned(&suffix, limit_token, PRDB_SDK_SQL_MAX_LIMIT,
                                1, &result.base.limit);
        if (status != PRDB_SDK_SQL_OK) {
            goto failed_v3;
        }
        status = advance(&suffix);
        if (status != PRDB_SDK_SQL_OK) {
            goto failed_v3;
        }
    }
    if (suffix.current.kind == TOKEN_SEMICOLON) {
        status = advance(&suffix);
        if (status != PRDB_SDK_SQL_OK) {
            goto failed_v3;
        }
    }
    if (suffix.current.kind != TOKEN_END) {
        suffix.error_offset = suffix.current.offset;
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        goto failed_v3;
    }
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V3_VERSION;
    *out = result;
    return PRDB_SDK_SQL_OK;

failed_v3:
    if (error_offset_out != NULL) {
        *error_offset_out = suffix.error_offset;
    }
    return fail_at(&suffix, status);
}

int32_t prdb_sdk_sql_bind_select_v3(
    const prdb_sdk_select_v3 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v3 *out, uint32_t out_capacity) {
    prdb_sdk_select_binding_v3 result = {0};
    uint32_t ordinal;
    int32_t status;

    if (statement == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_BIND_INVALID_ARGUMENT;
    }
    if (statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V3_VERSION ||
        statement->has_order_by > 1 || statement->order_descending > 1 ||
        (statement->has_order_by == 0 &&
         (statement->order_by_column.offset != 0 ||
          statement->order_by_column.length != 0 ||
          statement->order_descending != 0)) ||
        (statement->has_order_by == 1 &&
         (statement->order_by_column.length == 0 ||
          statement->order_by_column.offset > statement->base.sql_len ||
          statement->order_by_column.length >
              statement->base.sql_len - statement->order_by_column.offset))) {
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    }
    status = prdb_sdk_sql_bind_select_v2(
        &statement->base, relation_name, relation_name_len, columns,
        column_count, &result.base, sizeof(result.base));
    if (status != PRDB_SDK_SQL_BIND_OK) {
        return status;
    }
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V3_VERSION;
    result.has_order_by = statement->has_order_by;
    if (statement->has_order_by != 0) {
        status = prdb_sdk_internal_find_column_shape_v2(
            &statement->base, statement->order_by_column, columns,
            column_count, &ordinal);
        if (status != PRDB_SDK_SQL_BIND_OK) {
            return status;
        }
        result.order_by_column.ordinal = ordinal;
        result.order_by_column.type_code = columns[ordinal].type_code;
        result.order_by_column.decimal_precision =
            columns[ordinal].decimal_precision;
        result.order_by_column.decimal_scale = columns[ordinal].decimal_scale;
    }
    *out = result;
    return PRDB_SDK_SQL_BIND_OK;
}

int32_t prdb_sdk_sql_parse_select_v4(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_select_v4 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out) {
    parser state;
    prdb_sdk_select_v4 result = {0};
    int32_t status;

    if (error_offset_out != NULL) {
        *error_offset_out = 0;
    }
    if (sql == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_INVALID_ARGUMENT;
    }
    if (sql_len == 0) {
        return PRDB_SDK_SQL_SYNTAX_ERROR;
    }
    if (sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES) {
        if (error_offset_out != NULL) {
            *error_offset_out = (uint32_t)PRDB_SDK_SQL_MAX_QUERY_BYTES;
        }
        return PRDB_SDK_SQL_LIMIT_EXCEEDED;
    }
    state = (parser){sql, (uint32_t)sql_len, 0, {TOKEN_END, 0, 0}, 0,
                     PRDB_SDK_SQL_OK, 0};
    result.base.struct_size = sizeof(result.base);
    result.base.abi_version = PRDB_SDK_SQL_ABI_V2_VERSION;
    result.base.sql = sql;
    result.base.sql_len = sql_len;

    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) {
        goto failed_v4;
    }
    if (!token_is_keyword(&state, "SELECT")) {
        status = state.current.kind == TOKEN_IDENTIFIER
                     ? PRDB_SDK_SQL_UNSUPPORTED_STATEMENT
                     : PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_v4;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_v4;
    }
    status = expect_identifier(&state, &result.base.projection[0]);
    if (status != PRDB_SDK_SQL_OK) {
        goto failed_v4;
    }
    result.base.projection_count = 1;
    if (state.current.kind != TOKEN_COMMA || advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error == PRDB_SDK_SQL_OK
                     ? PRDB_SDK_SQL_SYNTAX_ERROR
                     : state.error;
        if (state.error == PRDB_SDK_SQL_OK) {
            state.error_offset = state.current.offset;
        }
        goto failed_v4;
    }
    if (!token_is_keyword(&state, "COUNT") ||
        advance(&state) != PRDB_SDK_SQL_OK ||
        state.current.kind != TOKEN_LEFT_PAREN ||
        advance(&state) != PRDB_SDK_SQL_OK ||
        state.current.kind != TOKEN_STAR ||
        advance(&state) != PRDB_SDK_SQL_OK ||
        state.current.kind != TOKEN_RIGHT_PAREN ||
        advance(&state) != PRDB_SDK_SQL_OK) {
        if (state.error != PRDB_SDK_SQL_OK) {
            status = state.error;
        } else {
            status = PRDB_SDK_SQL_SYNTAX_ERROR;
            state.error_offset = state.current.offset;
        }
        goto failed_v4;
    }
    if (!token_is_keyword(&state, "FROM")) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_v4;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_v4;
    }
    status = expect_identifier(&state, &result.base.relation);
    if (status != PRDB_SDK_SQL_OK) {
        goto failed_v4;
    }
    if (token_is_keyword(&state, "WHERE")) {
        if (advance(&state) != PRDB_SDK_SQL_OK) {
            status = state.error;
            goto failed_v4;
        }
        status = prdb_sdk_internal_parse_predicate_or_v2(
            &state, &result.base, 0, &result.base.predicate_root);
        if (status != PRDB_SDK_SQL_OK) {
            goto failed_v4;
        }
    }
    if (!token_is_keyword(&state, "GROUP")) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_v4;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK ||
        !token_is_keyword(&state, "BY") ||
        advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error == PRDB_SDK_SQL_OK
                     ? PRDB_SDK_SQL_SYNTAX_ERROR
                     : state.error;
        if (state.error == PRDB_SDK_SQL_OK) {
            state.error_offset = state.current.offset;
        }
        goto failed_v4;
    }
    status = expect_identifier(&state, &result.group_by_column);
    if (status != PRDB_SDK_SQL_OK) {
        goto failed_v4;
    }
    if (!prdb_sdk_internal_span_matches_name(
            &result.base, result.group_by_column,
            sql + result.base.projection[0].offset,
            result.base.projection[0].length)) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = result.group_by_column.offset;
        goto failed_v4;
    }
    result.has_group_by = 1;
    result.aggregate_kind = PRDB_SDK_SQL_AGGREGATE_COUNT_STAR;
    if (token_is_keyword(&state, "LIMIT")) {
        token limit_token;
        result.base.has_limit = 1;
        if (advance(&state) != PRDB_SDK_SQL_OK) {
            status = state.error;
            goto failed_v4;
        }
        limit_token = state.current;
        status = parse_unsigned(&state, limit_token, PRDB_SDK_SQL_MAX_LIMIT,
                                1, &result.base.limit);
        if (status != PRDB_SDK_SQL_OK) {
            goto failed_v4;
        }
        if (advance(&state) != PRDB_SDK_SQL_OK) {
            status = state.error;
            goto failed_v4;
        }
    }
    if (state.current.kind == TOKEN_SEMICOLON &&
        advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_v4;
    }
    if (state.current.kind != TOKEN_END) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_v4;
    }
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V4_VERSION;
    *out = result;
    return PRDB_SDK_SQL_OK;

failed_v4:
    if (error_offset_out != NULL) {
        *error_offset_out = state.error_offset;
    }
    return fail_at(&state, status);
}

int32_t prdb_sdk_sql_bind_select_v4(
    const prdb_sdk_select_v4 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v4 *out, uint32_t out_capacity) {
    prdb_sdk_select_binding_v4 result = {0};
    uint32_t ordinal;
    int32_t status;

    if (statement == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_BIND_INVALID_ARGUMENT;
    }
    if (statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V4_VERSION ||
        statement->has_group_by != 1 ||
        statement->aggregate_kind != PRDB_SDK_SQL_AGGREGATE_COUNT_STAR ||
        statement->base.projection_star != 0 ||
        statement->base.projection_count != 1 ||
        statement->base.has_limit > 1 ||
        (statement->base.has_limit == 0 && statement->base.limit != 0) ||
        statement->base.sql == NULL || statement->base.sql_len == 0 ||
        statement->group_by_column.length == 0 ||
        statement->group_by_column.offset > statement->base.sql_len ||
        statement->group_by_column.length >
            statement->base.sql_len - statement->group_by_column.offset ||
        statement->base.projection[0].length == 0 ||
        statement->base.projection[0].offset > statement->base.sql_len ||
        statement->base.projection[0].length >
            statement->base.sql_len - statement->base.projection[0].offset ||
        !prdb_sdk_internal_span_matches_name(
            &statement->base, statement->group_by_column,
            statement->base.sql + statement->base.projection[0].offset,
            statement->base.projection[0].length)) {
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    }
    status = prdb_sdk_sql_bind_select_v2(
        &statement->base, relation_name, relation_name_len, columns,
        column_count, &result.base, sizeof(result.base));
    if (status != PRDB_SDK_SQL_BIND_OK) {
        return status;
    }
    ordinal = result.base.projection_ordinals[0];
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V4_VERSION;
    result.has_group_by = 1;
    result.group_by_column.ordinal = ordinal;
    result.group_by_column.type_code = columns[ordinal].type_code;
    result.group_by_column.decimal_precision =
        columns[ordinal].decimal_precision;
    result.group_by_column.decimal_scale = columns[ordinal].decimal_scale;
    result.aggregate_kind = PRDB_SDK_SQL_AGGREGATE_COUNT_STAR;
    *out = result;
    return PRDB_SDK_SQL_BIND_OK;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_spans_equal_ascii_case(
    const uint8_t *sql, prdb_sdk_sql_span_v1 left, prdb_sdk_sql_span_v1 right) {
    if (left.length != right.length) {
        return 0;
    }
    for (uint32_t i = 0; i < left.length; ++i) {
        if (prdb_sdk_internal_fold_identifier_byte(sql[left.offset + i]) !=
            prdb_sdk_internal_fold_identifier_byte(sql[right.offset + i])) {
            return 0;
        }
    }
    return 1;
}
