#include "sql_internal.h"

static int32_t parse_qualified_column_v8(
    parser *state, prdb_sdk_sql_qualified_column_v1 *out) {
    int32_t status = expect_identifier(state, &out->relation);
    if (status != PRDB_SDK_SQL_OK) return status;
    if (state->current.kind != TOKEN_DOT) {
        state->error_offset = state->current.offset;
        return fail_at(state, PRDB_SDK_SQL_SYNTAX_ERROR);
    }
    status = advance(state);
    if (status != PRDB_SDK_SQL_OK) return status;
    return expect_identifier(state, &out->column);
}

PRDB_SDK_INTERNAL int32_t prdb_sdk_sql_parse_select_v8(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_select_v1 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out) {
    parser state;
    prdb_sdk_select_v1 result = {0};
    int32_t status;
    uint32_t parsed_limit = 0;

    if (error_offset_out != NULL) *error_offset_out = 0;
    if (sql == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_INVALID_ARGUMENT;
    }
    if (sql_len == 0 || sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES) {
        if (sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES &&
            error_offset_out != NULL) {
            *error_offset_out = (uint32_t)PRDB_SDK_SQL_MAX_QUERY_BYTES;
        }
        return sql_len == 0 ? PRDB_SDK_SQL_SYNTAX_ERROR
                            : PRDB_SDK_SQL_LIMIT_EXCEEDED;
    }
    state = (parser){sql, (uint32_t)sql_len, 0, {TOKEN_END, 0, 0}, 0,
                     PRDB_SDK_SQL_OK, 0};
    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v8;
    if (!token_is_keyword(&state, "SELECT")) {
        status = state.current.kind == TOKEN_IDENTIFIER
                     ? PRDB_SDK_SQL_UNSUPPORTED_STATEMENT
                     : PRDB_SDK_SQL_SYNTAX_ERROR;
        goto failed_join_select_v8;
    }
    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v8;
    for (;;) {
        if (result.join_projection_count >= PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS) {
            state.error_offset = state.current.offset;
            status = PRDB_SDK_SQL_LIMIT_EXCEEDED;
            goto failed_join_select_v8;
        }
        status = parse_qualified_column_v8(
            &state, &result.join_projections[result.join_projection_count]);
        if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v8;
        ++result.join_projection_count;
        if (state.current.kind != TOKEN_COMMA) break;
        status = advance(&state);
        if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v8;
    }
    if (!token_is_keyword(&state, "FROM")) {
        state.error_offset = state.current.offset;
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        goto failed_join_select_v8;
    }
    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v8;
    status = expect_identifier(&state, &result.left_relation);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v8;
    if (!token_is_keyword(&state, "INNER")) {
        state.error_offset = state.current.offset;
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        goto failed_join_select_v8;
    }
    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v8;
    if (!token_is_keyword(&state, "JOIN")) {
        state.error_offset = state.current.offset;
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        goto failed_join_select_v8;
    }
    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v8;
    status = expect_identifier(&state, &result.right_relation);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v8;
    if (!token_is_keyword(&state, "ON")) {
        state.error_offset = state.current.offset;
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        goto failed_join_select_v8;
    }
    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v8;
    status = parse_qualified_column_v8(&state, &result.left_join_keys[0]);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v8;
    if (state.current.kind != TOKEN_EQUAL) {
        state.error_offset = state.current.offset;
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        goto failed_join_select_v8;
    }
    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v8;
    status = parse_qualified_column_v8(&state, &result.right_join_keys[0]);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v8;
    if (token_is_keyword(&state, "LIMIT")) {
        token limit_token;
        result.has_limit = 1;
        status = advance(&state);
        if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v8;
        limit_token = state.current;
        status = parse_unsigned(&state, limit_token, PRDB_SDK_SQL_MAX_LIMIT,
                                1, &parsed_limit);
        if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v8;
        result.limit = parsed_limit;
        status = advance(&state);
        if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v8;
    }
    if (state.current.kind == TOKEN_SEMICOLON) {
        status = advance(&state);
        if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v8;
    }
    if (state.current.kind != TOKEN_END) {
        state.error_offset = state.current.offset;
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        goto failed_join_select_v8;
    }
    result.left_alias = result.left_relation;
    result.right_alias = result.right_relation;
    result.join_key_count = 1;
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
    result.sql = sql;
    result.sql_len = sql_len;
    *out = result;
    return PRDB_SDK_SQL_OK;

failed_join_select_v8:
    if (error_offset_out != NULL) {
        *error_offset_out = state.error_offset <= state.length
                                ? state.error_offset
                                : state.length;
    }
    return status;
}

static int32_t resolve_qualified_column_v8(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_sql_qualified_column_v1 *reference,
    const uint8_t *left_relation_name, uint32_t left_relation_name_len,
    const prdb_sdk_sql_column_shape_v1 *left_columns,
    uint32_t left_column_count,
    const uint8_t *right_relation_name, uint32_t right_relation_name_len,
    const prdb_sdk_sql_column_shape_v1 *right_columns,
    uint32_t right_column_count, uint32_t *side_out,
    prdb_sdk_sql_bound_column_v1 *column_out) {
    prdb_sdk_select_v1 name_statement = {0};
    const prdb_sdk_sql_column_shape_v1 *columns;
    uint32_t column_count;
    uint32_t ordinal;
    uint32_t side;
    int32_t status;

    name_statement.sql = statement->sql;
    name_statement.sql_len = statement->sql_len;
    if (prdb_sdk_internal_span_matches_name(&name_statement, reference->relation,
                          left_relation_name, left_relation_name_len)) {
        side = PRDB_SDK_SQL_JOIN_LEFT;
        columns = left_columns;
        column_count = left_column_count;
    } else if (prdb_sdk_internal_span_matches_name(&name_statement, reference->relation,
                                 right_relation_name,
                                 right_relation_name_len)) {
        side = PRDB_SDK_SQL_JOIN_RIGHT;
        columns = right_columns;
        column_count = right_column_count;
    } else {
        return PRDB_SDK_SQL_BIND_RELATION_MISMATCH;
    }
    status = prdb_sdk_internal_find_column_shape_v1(&name_statement, reference->column,
                                  columns, column_count, &ordinal);
    if (status != PRDB_SDK_SQL_BIND_OK) return status;
    *side_out = side;
    *column_out = (prdb_sdk_sql_bound_column_v1){
        ordinal, columns[ordinal].type_code,
        columns[ordinal].decimal_precision, columns[ordinal].decimal_scale};
    return PRDB_SDK_SQL_BIND_OK;
}

PRDB_SDK_INTERNAL int32_t prdb_sdk_sql_bind_select_v8(
    const prdb_sdk_select_v1 *statement,
    const uint8_t *left_relation_name, uint32_t left_relation_name_len,
    const prdb_sdk_sql_column_shape_v1 *left_columns,
    uint32_t left_column_count,
    const uint8_t *right_relation_name, uint32_t right_relation_name_len,
    const prdb_sdk_sql_column_shape_v1 *right_columns,
    uint32_t right_column_count, prdb_sdk_select_binding_v1 *out,
    uint32_t out_capacity) {
    prdb_sdk_select_binding_v1 result = {0};
    prdb_sdk_select_v1 name_statement = {0};
    uint32_t side;
    int32_t status;

    if (statement == NULL || left_relation_name == NULL ||
        right_relation_name == NULL || out == NULL ||
        out_capacity < sizeof(*out) ||
        !prdb_sdk_internal_valid_identifier_bytes(left_relation_name, left_relation_name_len) ||
        !prdb_sdk_internal_valid_identifier_bytes(right_relation_name,
                                right_relation_name_len)) {
        return PRDB_SDK_SQL_BIND_INVALID_ARGUMENT;
    }
    if (statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->sql == NULL || statement->sql_len == 0 ||
        statement->sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        statement->join_projection_count == 0 ||
        statement->join_projection_count > PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS ||
        statement->has_limit > 1 ||
        (statement->has_limit == 0 && statement->limit != 0)) {
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    }
    if (statement->left_relation.offset > statement->sql_len ||
        statement->left_relation.length >
            statement->sql_len - statement->left_relation.offset ||
        statement->right_relation.offset > statement->sql_len ||
        statement->right_relation.length >
            statement->sql_len - statement->right_relation.offset ||
        statement->left_join_keys[0].relation.offset > statement->sql_len ||
        statement->left_join_keys[0].relation.length >
            statement->sql_len - statement->left_join_keys[0].relation.offset ||
        statement->left_join_keys[0].column.offset > statement->sql_len ||
        statement->left_join_keys[0].column.length >
            statement->sql_len - statement->left_join_keys[0].column.offset ||
        statement->right_join_keys[0].relation.offset > statement->sql_len ||
        statement->right_join_keys[0].relation.length >
            statement->sql_len - statement->right_join_keys[0].relation.offset ||
        statement->right_join_keys[0].column.offset > statement->sql_len ||
        statement->right_join_keys[0].column.length >
            statement->sql_len - statement->right_join_keys[0].column.offset) {
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    }

    name_statement.sql = statement->sql;
    name_statement.sql_len = statement->sql_len;
    if (!prdb_sdk_internal_span_matches_name(&name_statement, statement->left_relation,
                           left_relation_name, left_relation_name_len) ||
        !prdb_sdk_internal_span_matches_name(&name_statement, statement->right_relation,
                           right_relation_name, right_relation_name_len)) {
        return PRDB_SDK_SQL_BIND_RELATION_MISMATCH;
    }
    if (prdb_sdk_internal_span_matches_name(&name_statement, statement->left_relation,
                          right_relation_name, right_relation_name_len) &&
        prdb_sdk_internal_span_matches_name(&name_statement, statement->right_relation,
                          left_relation_name, left_relation_name_len)) {
        return PRDB_SDK_SQL_BIND_AMBIGUOUS_COLUMN;
    }
    status = prdb_sdk_internal_validate_column_shape_v1(left_columns, left_column_count);
    if (status != PRDB_SDK_SQL_BIND_OK) return status;
    status = prdb_sdk_internal_validate_column_shape_v1(right_columns, right_column_count);
    if (status != PRDB_SDK_SQL_BIND_OK) return status;

    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
    result.left_relation_column_count = left_column_count;
    result.right_relation_column_count = right_column_count;
    result.join_projection_count = statement->join_projection_count;
    result.output_projection_count = statement->join_projection_count;
    for (uint32_t index = 0; index < statement->join_projection_count; ++index) {
        status = resolve_qualified_column_v8(
            statement, &statement->join_projections[index], left_relation_name,
            left_relation_name_len, left_columns, left_column_count,
            right_relation_name, right_relation_name_len, right_columns,
            right_column_count, &side, &result.join_projections[index].column);
        if (status != PRDB_SDK_SQL_BIND_OK) return status;
        result.join_projections[index].relation_side = side;
    }
    status = resolve_qualified_column_v8(
        statement, &statement->left_join_keys[0], left_relation_name,
        left_relation_name_len, left_columns, left_column_count,
        right_relation_name, right_relation_name_len, right_columns,
        right_column_count, &side, &result.join_keys[0].left);
    if (status != PRDB_SDK_SQL_BIND_OK) return status;
    if (side != PRDB_SDK_SQL_JOIN_LEFT) {
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    }
    status = resolve_qualified_column_v8(
        statement, &statement->right_join_keys[0], left_relation_name,
        left_relation_name_len, left_columns, left_column_count,
        right_relation_name, right_relation_name_len, right_columns,
        right_column_count, &side, &result.join_keys[0].right);
    if (status != PRDB_SDK_SQL_BIND_OK) return status;
    if (side != PRDB_SDK_SQL_JOIN_RIGHT) {
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    }
    result.join_key_count = 1;
    if (result.join_keys[0].left.type_code !=
            result.join_keys[0].right.type_code ||
        result.join_keys[0].left.decimal_scale !=
            result.join_keys[0].right.decimal_scale) {
        return PRDB_SDK_SQL_BIND_JOIN_TYPE_MISMATCH;
    }
    *out = result;
    return PRDB_SDK_SQL_BIND_OK;
}

PRDB_SDK_INTERNAL int32_t prdb_sdk_sql_parse_select_v16(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v1 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out) {
    parser state;
    prdb_sdk_select_v1 result = {0};
    int32_t status;
    uint32_t parsed_limit = 0;

    if (error_offset_out != NULL) *error_offset_out = 0;
    if (sql == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_INVALID_ARGUMENT;
    }
    if (sql_len == 0 || sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES) {
        if (sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES &&
            error_offset_out != NULL) {
            *error_offset_out = (uint32_t)PRDB_SDK_SQL_MAX_QUERY_BYTES;
        }
        return sql_len == 0 ? PRDB_SDK_SQL_SYNTAX_ERROR
                            : PRDB_SDK_SQL_LIMIT_EXCEEDED;
    }
    state = (parser){sql, (uint32_t)sql_len, 0, {TOKEN_END, 0, 0}, 0,
                     PRDB_SDK_SQL_OK, 0};
    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v16;
    if (!token_is_keyword(&state, "SELECT")) {
        status = state.current.kind == TOKEN_IDENTIFIER
                     ? PRDB_SDK_SQL_UNSUPPORTED_STATEMENT
                     : PRDB_SDK_SQL_SYNTAX_ERROR;
        goto failed_join_select_v16;
    }
    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v16;
    for (;;) {
        if (result.join_projection_count >=
            PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS) {
            state.error_offset = state.current.offset;
            status = PRDB_SDK_SQL_LIMIT_EXCEEDED;
            goto failed_join_select_v16;
        }
        status = parse_qualified_column_v8(
            &state, &result.join_projections[result.join_projection_count]);
        if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v16;
        ++result.join_projection_count;
        if (state.current.kind != TOKEN_COMMA) break;
        status = advance(&state);
        if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v16;
    }
    if (!token_is_keyword(&state, "FROM")) {
        state.error_offset = state.current.offset;
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        goto failed_join_select_v16;
    }
    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v16;
    status = expect_identifier(&state, &result.left_relation);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v16;
    if (!token_is_keyword(&state, "INNER")) {
        state.error_offset = state.current.offset;
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        goto failed_join_select_v16;
    }
    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v16;
    if (!token_is_keyword(&state, "JOIN")) {
        state.error_offset = state.current.offset;
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        goto failed_join_select_v16;
    }
    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v16;
    status = expect_identifier(&state, &result.right_relation);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v16;
    if (!token_is_keyword(&state, "ON")) {
        state.error_offset = state.current.offset;
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        goto failed_join_select_v16;
    }
    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v16;
    for (;;) {
        uint32_t index = result.join_key_count;
        if (index >= PRDB_SDK_SQL_MAX_JOIN_KEYS) {
            state.error_offset = state.current.offset;
            status = PRDB_SDK_SQL_LIMIT_EXCEEDED;
            goto failed_join_select_v16;
        }
        status = parse_qualified_column_v8(&state,
                                           &result.left_join_keys[index]);
        if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v16;
        if (state.current.kind != TOKEN_EQUAL) {
            state.error_offset = state.current.offset;
            status = PRDB_SDK_SQL_SYNTAX_ERROR;
            goto failed_join_select_v16;
        }
        status = advance(&state);
        if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v16;
        status = parse_qualified_column_v8(&state,
                                           &result.right_join_keys[index]);
        if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v16;
        ++result.join_key_count;
        if (!token_is_keyword(&state, "AND")) break;
        status = advance(&state);
        if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v16;
    }
    if (result.join_key_count < 2) {
        state.error_offset = state.current.offset;
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        goto failed_join_select_v16;
    }
    result.left_join_keys[0] = result.left_join_keys[0];
    result.right_join_keys[0] = result.right_join_keys[0];
    if (token_is_keyword(&state, "LIMIT")) {
        token limit_token;
        result.has_limit = 1;
        status = advance(&state);
        if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v16;
        limit_token = state.current;
        status = parse_unsigned(&state, limit_token, PRDB_SDK_SQL_MAX_LIMIT,
                                1, &parsed_limit);
        if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v16;
        result.limit = parsed_limit;
        status = advance(&state);
        if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v16;
    }
    if (state.current.kind == TOKEN_SEMICOLON) {
        status = advance(&state);
        if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v16;
    }
    if (state.current.kind != TOKEN_END) {
        state.error_offset = state.current.offset;
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        goto failed_join_select_v16;
    }
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
    result.sql = sql;
    result.sql_len = sql_len;
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
    *out = result;
    return PRDB_SDK_SQL_OK;

failed_join_select_v16:
    if (error_offset_out != NULL) {
        *error_offset_out = state.error_offset <= state.length
                                ? state.error_offset
                                : state.length;
    }
    return status;
}

PRDB_SDK_INTERNAL int32_t prdb_sdk_sql_bind_select_v16(
    const prdb_sdk_select_v1 *statement,
    const uint8_t *left_relation_name, uint32_t left_relation_name_len,
    const prdb_sdk_sql_column_shape_v1 *left_columns,
    uint32_t left_column_count,
    const uint8_t *right_relation_name, uint32_t right_relation_name_len,
    const prdb_sdk_sql_column_shape_v1 *right_columns,
    uint32_t right_column_count, prdb_sdk_select_binding_v1 *out,
    uint32_t out_capacity) {
    prdb_sdk_select_binding_v1 result = {0};
    int32_t status;

    if (statement == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_BIND_INVALID_ARGUMENT;
    }
    if (statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->join_key_count < 2 ||
        statement->join_key_count > PRDB_SDK_SQL_MAX_JOIN_KEYS ||
        statement->sql == NULL || statement->sql_len == 0 ||
        statement->left_join_keys[0].relation.offset !=
            statement->left_join_keys[0].relation.offset ||
        statement->left_join_keys[0].relation.length !=
            statement->left_join_keys[0].relation.length ||
        statement->left_join_keys[0].column.offset !=
            statement->left_join_keys[0].column.offset ||
        statement->left_join_keys[0].column.length !=
            statement->left_join_keys[0].column.length ||
        statement->right_join_keys[0].relation.offset !=
            statement->right_join_keys[0].relation.offset ||
        statement->right_join_keys[0].relation.length !=
            statement->right_join_keys[0].relation.length ||
        statement->right_join_keys[0].column.offset !=
            statement->right_join_keys[0].column.offset ||
        statement->right_join_keys[0].column.length !=
            statement->right_join_keys[0].column.length) {
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    }

    status = prdb_sdk_sql_bind_select_v8(
        statement, left_relation_name, left_relation_name_len,
        left_columns, left_column_count, right_relation_name,
        right_relation_name_len, right_columns, right_column_count,
        &result, sizeof(result));
    if (status != PRDB_SDK_SQL_BIND_OK) return status;

    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
    result.join_key_count = statement->join_key_count;
    for (uint32_t index = 0; index < statement->join_key_count; ++index) {
        uint32_t left_side;
        uint32_t right_side;
        prdb_sdk_sql_bound_column_v1 left_bound;
        prdb_sdk_sql_bound_column_v1 right_bound;
        status = resolve_qualified_column_v8(
            statement, &statement->left_join_keys[index],
            left_relation_name, left_relation_name_len, left_columns,
            left_column_count, right_relation_name, right_relation_name_len,
            right_columns, right_column_count, &left_side, &left_bound);
        if (status != PRDB_SDK_SQL_BIND_OK) return status;
        status = resolve_qualified_column_v8(
            statement, &statement->right_join_keys[index],
            left_relation_name, left_relation_name_len, left_columns,
            left_column_count, right_relation_name, right_relation_name_len,
            right_columns, right_column_count, &right_side, &right_bound);
        if (status != PRDB_SDK_SQL_BIND_OK) return status;
        if (left_side != PRDB_SDK_SQL_JOIN_LEFT ||
            right_side != PRDB_SDK_SQL_JOIN_RIGHT) {
            return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
        }
        if (left_bound.type_code != right_bound.type_code ||
            left_bound.decimal_scale != right_bound.decimal_scale) {
            return PRDB_SDK_SQL_BIND_JOIN_TYPE_MISMATCH;
        }
        result.join_keys[index] =
            (prdb_sdk_sql_join_key_binding_v1){left_bound, right_bound};
    }
    *out = result;
    return PRDB_SDK_SQL_BIND_OK;
}

static int32_t parse_optional_relation_alias(
    parser *state, prdb_sdk_sql_span_v1 relation,
    prdb_sdk_sql_span_v1 *alias_out) {
    int32_t status;
    *alias_out = relation;
    if (!token_is_keyword(state, "AS")) return PRDB_SDK_SQL_OK;
    status = advance(state);
    if (status != PRDB_SDK_SQL_OK) return status;
    if (token_is_keyword(state, "INNER") ||
        token_is_keyword(state, "JOIN") || token_is_keyword(state, "ON") ||
        token_is_keyword(state, "LIMIT") || token_is_keyword(state, "AS")) {
        state->error_offset = state->current.offset;
        return PRDB_SDK_SQL_SYNTAX_ERROR;
    }
    return expect_identifier(state, alias_out);
}

PRDB_SDK_INTERNAL int32_t prdb_sdk_sql_parse_select_v19(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v1 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out) {
    parser state;
    prdb_sdk_select_v1 result = {0};
    int32_t status;
    uint32_t parsed_limit = 0;

    if (error_offset_out != NULL) *error_offset_out = 0;
    if (sql == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_INVALID_ARGUMENT;
    }
    if (sql_len == 0 || sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES) {
        if (sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES &&
            error_offset_out != NULL) {
            *error_offset_out = (uint32_t)PRDB_SDK_SQL_MAX_QUERY_BYTES;
        }
        return sql_len == 0 ? PRDB_SDK_SQL_SYNTAX_ERROR
                            : PRDB_SDK_SQL_LIMIT_EXCEEDED;
    }
    state = (parser){sql, (uint32_t)sql_len, 0, {TOKEN_END, 0, 0}, 0,
                     PRDB_SDK_SQL_OK, 0};
    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v19;
    if (!token_is_keyword(&state, "SELECT")) {
        status = state.current.kind == TOKEN_IDENTIFIER
                     ? PRDB_SDK_SQL_UNSUPPORTED_STATEMENT
                     : PRDB_SDK_SQL_SYNTAX_ERROR;
        goto failed_join_select_v19;
    }
    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v19;
    for (;;) {
        if (result.join_projection_count >=
            PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS) {
            state.error_offset = state.current.offset;
            status = PRDB_SDK_SQL_LIMIT_EXCEEDED;
            goto failed_join_select_v19;
        }
        status = parse_qualified_column_v8(
            &state,
            &result.join_projections[
                result.join_projection_count]);
        if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v19;
        ++result.join_projection_count;
        if (state.current.kind != TOKEN_COMMA) break;
        status = advance(&state);
        if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v19;
    }
    if (!token_is_keyword(&state, "FROM")) {
        state.error_offset = state.current.offset;
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        goto failed_join_select_v19;
    }
    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v19;
    status = expect_identifier(&state, &result.left_relation);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v19;
    status = parse_optional_relation_alias(
        &state, result.left_relation, &result.left_alias);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v19;
    if (!token_is_keyword(&state, "INNER")) {
        state.error_offset = state.current.offset;
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        goto failed_join_select_v19;
    }
    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v19;
    if (!token_is_keyword(&state, "JOIN")) {
        state.error_offset = state.current.offset;
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        goto failed_join_select_v19;
    }
    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v19;
    status = expect_identifier(&state, &result.right_relation);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v19;
    status = parse_optional_relation_alias(
        &state, result.right_relation, &result.right_alias);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v19;
    if (!token_is_keyword(&state, "ON")) {
        state.error_offset = state.current.offset;
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        goto failed_join_select_v19;
    }
    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v19;
    for (;;) {
        uint32_t index = result.join_key_count;
        if (index >= PRDB_SDK_SQL_MAX_JOIN_KEYS) {
            state.error_offset = state.current.offset;
            status = PRDB_SDK_SQL_LIMIT_EXCEEDED;
            goto failed_join_select_v19;
        }
        status = parse_qualified_column_v8(
            &state, &result.left_join_keys[index]);
        if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v19;
        if (state.current.kind != TOKEN_EQUAL) {
            state.error_offset = state.current.offset;
            status = PRDB_SDK_SQL_SYNTAX_ERROR;
            goto failed_join_select_v19;
        }
        status = advance(&state);
        if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v19;
        status = parse_qualified_column_v8(
            &state, &result.right_join_keys[index]);
        if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v19;
        ++result.join_key_count;
        if (!token_is_keyword(&state, "AND")) break;
        status = advance(&state);
        if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v19;
    }
    if (result.join_key_count < 2) {
        state.error_offset = state.current.offset;
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        goto failed_join_select_v19;
    }
    result.left_join_keys[0] = result.left_join_keys[0];
    result.right_join_keys[0] = result.right_join_keys[0];
    if (token_is_keyword(&state, "LIMIT")) {
        token limit_token;
        result.has_limit = 1;
        status = advance(&state);
        if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v19;
        limit_token = state.current;
        status = parse_unsigned(&state, limit_token, PRDB_SDK_SQL_MAX_LIMIT,
                                1, &parsed_limit);
        if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v19;
        result.limit = parsed_limit;
        status = advance(&state);
        if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v19;
    }
    if (state.current.kind == TOKEN_SEMICOLON) {
        status = advance(&state);
        if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v19;
    }
    if (state.current.kind != TOKEN_END) {
        state.error_offset = state.current.offset;
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        goto failed_join_select_v19;
    }
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
    result.sql = sql;
    result.sql_len = sql_len;
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
    *out = result;
    return PRDB_SDK_SQL_OK;

failed_join_select_v19:
    if (error_offset_out != NULL) {
        *error_offset_out = state.error_offset <= state.length
                                ? state.error_offset
                                : state.length;
    }
    return status;
}

static int rebase_v19_relation(
    const prdb_sdk_select_v1 *statement,
    prdb_sdk_sql_qualified_column_v1 *reference) {
    if (prdb_sdk_internal_spans_equal_ascii_case(statement->sql,
                               reference->relation, statement->left_alias)) {
        return 1;
    }
    if (prdb_sdk_internal_spans_equal_ascii_case(statement->sql,
                               reference->relation, statement->right_alias)) {
        return 1;
    }
    return 0;
}

static int v19_span_in_sql(const uint8_t *sql, uint64_t sql_len,
                           prdb_sdk_sql_span_v1 span) {
    return sql != NULL && span.length != 0 && span.offset <= sql_len &&
           span.length <= sql_len - span.offset;
}

PRDB_SDK_INTERNAL int32_t prdb_sdk_sql_bind_select_v19(
    const prdb_sdk_select_v1 *statement,
    const uint8_t *left_relation_name, uint32_t left_relation_name_len,
    const prdb_sdk_sql_column_shape_v1 *left_columns,
    uint32_t left_column_count,
    const uint8_t *right_relation_name, uint32_t right_relation_name_len,
    const prdb_sdk_sql_column_shape_v1 *right_columns,
    uint32_t right_column_count, prdb_sdk_select_binding_v1 *out,
    uint32_t out_capacity) {
    prdb_sdk_select_v1 checked;
    prdb_sdk_select_binding_v1 result = {0};
    prdb_sdk_select_v1 relation_names = {0};
    int32_t status;

    if (statement == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_BIND_INVALID_ARGUMENT;
    }
    if (left_relation_name == NULL || right_relation_name == NULL ||
        !prdb_sdk_internal_valid_identifier_bytes(left_relation_name,
                                left_relation_name_len) ||
        !prdb_sdk_internal_valid_identifier_bytes(right_relation_name,
                                right_relation_name_len)) {
        return PRDB_SDK_SQL_BIND_INVALID_ARGUMENT;
    }
    if (statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->sql == NULL ||
        statement->sql_len == 0 ||
        statement->sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        statement->join_key_count < 2 ||
        statement->join_key_count > PRDB_SDK_SQL_MAX_JOIN_KEYS ||
        statement->join_projection_count == 0 ||
        statement->join_projection_count >
            PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS ||
        !v19_span_in_sql(statement->sql,
                         statement->sql_len,
                         statement->left_relation) ||
        !v19_span_in_sql(statement->sql,
                         statement->sql_len,
                         statement->right_relation) ||
        !v19_span_in_sql(statement->sql,
                         statement->sql_len,
                         statement->left_alias) ||
        !v19_span_in_sql(statement->sql,
                         statement->sql_len,
                         statement->right_alias) ||
        prdb_sdk_internal_spans_equal_ascii_case(statement->sql,
                               statement->left_alias, statement->right_alias)) {
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    }
    if (!prdb_sdk_internal_valid_identifier_bytes(
            statement->sql + statement->left_alias.offset,
            statement->left_alias.length) ||
        !prdb_sdk_internal_valid_identifier_bytes(
            statement->sql + statement->right_alias.offset,
            statement->right_alias.length)) {
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    }
    relation_names.sql = statement->sql;
    relation_names.sql_len = statement->sql_len;
    if (!prdb_sdk_internal_span_matches_name(&relation_names,
                           statement->left_relation,
                           left_relation_name, left_relation_name_len) ||
        !prdb_sdk_internal_span_matches_name(&relation_names,
                           statement->right_relation,
                           right_relation_name, right_relation_name_len)) {
        return PRDB_SDK_SQL_BIND_RELATION_MISMATCH;
    }
    checked = *statement;
    for (uint32_t index = 0;
         index < checked.join_projection_count; ++index) {
        if (!v19_span_in_sql(checked.sql,
                             checked.sql_len,
                             checked.join_projections[index].relation) ||
            !v19_span_in_sql(checked.sql,
                             checked.sql_len,
                             checked.join_projections[index].column)) {
            return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
        }
        if (!rebase_v19_relation(&checked,
                                 &checked.join_projections[index])) {
            return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
        }
    }
    for (uint32_t index = 0; index < checked.join_key_count; ++index) {
        if (!v19_span_in_sql(checked.sql,
                             checked.sql_len,
                             checked.left_join_keys[index].relation) ||
            !v19_span_in_sql(checked.sql,
                             checked.sql_len,
                             checked.left_join_keys[index].column) ||
            !v19_span_in_sql(checked.sql,
                             checked.sql_len,
                             checked.right_join_keys[index].relation) ||
            !v19_span_in_sql(checked.sql,
                             checked.sql_len,
                             checked.right_join_keys[index].column)) {
            return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
        }
        if (!rebase_v19_relation(&checked,
                                 &checked.left_join_keys[index]) ||
            !rebase_v19_relation(&checked,
                                 &checked.right_join_keys[index])) {
            return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
        }
    }
    checked.left_relation = checked.left_alias;
    checked.right_relation = checked.right_alias;
    status = prdb_sdk_sql_bind_select_v16(
        &checked,
        checked.sql + checked.left_alias.offset,
        checked.left_alias.length, left_columns, left_column_count,
        checked.sql + checked.right_alias.offset,
        checked.right_alias.length, right_columns, right_column_count,
        &result, sizeof(result));
    if (status != PRDB_SDK_SQL_BIND_OK) return status;
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
    *out = result;
    return PRDB_SDK_SQL_BIND_OK;
}

PRDB_SDK_INTERNAL int32_t prdb_sdk_sql_parse_select_v20(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v1 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out) {
    parser scan;
    uint32_t candidate_count = 0;
    int32_t status;

    if (error_offset_out != NULL) *error_offset_out = 0;
    if (sql == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_INVALID_ARGUMENT;
    }
    if (sql_len == 0 || sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES) {
        if (sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES &&
            error_offset_out != NULL) {
            *error_offset_out = (uint32_t)PRDB_SDK_SQL_MAX_QUERY_BYTES;
        }
        return sql_len == 0 ? PRDB_SDK_SQL_SYNTAX_ERROR
                            : PRDB_SDK_SQL_LIMIT_EXCEEDED;
    }

    /* First preserve the common no-WHERE form exactly through the frozen v19
     * parser. A WHERE clause is recognized only at a boundary where the
     * complete preceding text is itself a valid v19 join statement. */
    {
        prdb_sdk_select_v1 result = {0};
        uint32_t v19_error = 0;
        status = prdb_sdk_sql_parse_select_v19(
            sql, sql_len, &result, sizeof(result), &v19_error);
        if (status == PRDB_SDK_SQL_OK) {
            result.struct_size = sizeof(result);
            result.abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
            *out = result;
            return PRDB_SDK_SQL_OK;
        }
        if (status == PRDB_SDK_SQL_LIMIT_EXCEEDED) {
            if (error_offset_out != NULL) *error_offset_out = v19_error;
            return PRDB_SDK_SQL_LIMIT_EXCEEDED;
        }
    }

    scan = (parser){sql, (uint32_t)sql_len, 0, {TOKEN_END, 0, 0}, 0,
                    PRDB_SDK_SQL_OK, 0};
    status = advance(&scan);
    if (status != PRDB_SDK_SQL_OK) goto failed_v20;
    while (scan.current.kind != TOKEN_END) {
        uint32_t where_offset = scan.current.offset;
        if (token_is_keyword(&scan, "WHERE")) {
            prdb_sdk_select_v1 result = {0};
            prdb_sdk_select_v1 prefix = {0};
            parser predicate;
            uint32_t parameter_index = 0;
            int32_t prefix_status;

            if (++candidate_count > 128) {
                scan.error_offset = where_offset;
                status = PRDB_SDK_SQL_LIMIT_EXCEEDED;
                goto failed_v20;
            }
            prefix_status = prdb_sdk_sql_parse_select_v19(
                sql, where_offset, &prefix, sizeof(prefix), NULL);
            if (prefix_status == PRDB_SDK_SQL_OK) {
                result = prefix;
                result.sql_len = sql_len;
                result.has_join_where = 1;
                result.where_keyword_offset = where_offset;
                result.from_keyword_offset = UINT32_MAX;

                predicate = (parser){
                    sql, (uint32_t)sql_len,
                    where_offset + scan.current.length,
                    {TOKEN_END, 0, 0}, 0, PRDB_SDK_SQL_OK, 0};
                status = advance(&predicate);
                if (status != PRDB_SDK_SQL_OK) goto failed_predicate_v20;
                status = parse_qualified_column_v8(
                    &predicate, &result.join_where_column);
                if (status != PRDB_SDK_SQL_OK) goto failed_predicate_v20;
                if (token_is_keyword(&predicate, "IS")) {
                    uint32_t is_not_null = 0;
                    status = advance(&predicate);
                    if (status != PRDB_SDK_SQL_OK) goto failed_predicate_v20;
                    if (token_is_keyword(&predicate, "NOT")) {
                        is_not_null = 1;
                        status = advance(&predicate);
                        if (status != PRDB_SDK_SQL_OK) goto failed_predicate_v20;
                    }
                    if (!token_is_keyword(&predicate, "NULL")) {
                        predicate.error_offset = predicate.current.offset;
                        status = PRDB_SDK_SQL_SYNTAX_ERROR;
                        goto failed_predicate_v20;
                    }
                    result.join_where_comparison = is_not_null
                                            ? PRDB_SDK_SQL_COMPARE_IS_NOT_NULL
                                            : PRDB_SDK_SQL_COMPARE_IS_NULL;
                    status = advance(&predicate);
                    if (status != PRDB_SDK_SQL_OK) goto failed_predicate_v20;
                } else {
                    status = comparison_code(predicate.current.kind,
                                             &result.join_where_comparison);
                    if (status != PRDB_SDK_SQL_OK) {
                        predicate.error_offset = predicate.current.offset;
                        goto failed_predicate_v20;
                    }
                    status = advance(&predicate);
                    if (status != PRDB_SDK_SQL_OK) goto failed_predicate_v20;
                    if (predicate.current.kind != TOKEN_PARAMETER) {
                        predicate.error_offset = predicate.current.offset;
                        status = PRDB_SDK_SQL_SYNTAX_ERROR;
                        goto failed_predicate_v20;
                    }
                    status = parse_unsigned(
                        &predicate, predicate.current,
                        PRDB_SDK_SQL_MAX_PARAMETER_INDEX, 0,
                        &parameter_index);
                    if (status != PRDB_SDK_SQL_OK) goto failed_predicate_v20;
                    result.join_where_parameter_index = parameter_index;
                    status = advance(&predicate);
                    if (status != PRDB_SDK_SQL_OK) goto failed_predicate_v20;
                }
                result.where_clause_end = predicate.position;

                /* Find the structural FROM token directly preceding the
                 * already parsed relation span. */
                {
                    parser from_scan = {sql, (uint32_t)sql_len, 0,
                                        {TOKEN_END, 0, 0}, 0,
                                        PRDB_SDK_SQL_OK, 0};
                    status = advance(&from_scan);
                    if (status != PRDB_SDK_SQL_OK) goto failed_predicate_v20;
                    while (from_scan.current.kind != TOKEN_END) {
                        uint32_t offset = from_scan.current.offset;
                        if (token_is_keyword(&from_scan, "FROM")) {
                            status = advance(&from_scan);
                            if (status != PRDB_SDK_SQL_OK) goto failed_predicate_v20;
                            if (from_scan.current.offset ==
                                result.left_relation.offset) {
                                result.from_keyword_offset = offset;
                                break;
                            }
                            continue;
                        }
                        status = advance(&from_scan);
                        if (status != PRDB_SDK_SQL_OK) goto failed_predicate_v20;
                    }
                }
                if (result.from_keyword_offset == UINT32_MAX) {
                    predicate.error_offset = result.left_relation.offset;
                    status = PRDB_SDK_SQL_SYNTAX_ERROR;
                    goto failed_predicate_v20;
                }

                if (token_is_keyword(&predicate, "LIMIT")) {
                    token limit_token;
                    result.has_limit = 1;
                    status = advance(&predicate);
                    if (status != PRDB_SDK_SQL_OK) goto failed_predicate_v20;
                    limit_token = predicate.current;
                    status = parse_unsigned(
                        &predicate, limit_token, PRDB_SDK_SQL_MAX_LIMIT, 1,
                        &result.limit);
                    if (status != PRDB_SDK_SQL_OK) goto failed_predicate_v20;
                    status = advance(&predicate);
                    if (status != PRDB_SDK_SQL_OK) goto failed_predicate_v20;
                }
                if (predicate.current.kind == TOKEN_SEMICOLON) {
                    status = advance(&predicate);
                    if (status != PRDB_SDK_SQL_OK) goto failed_predicate_v20;
                }
                if (predicate.current.kind != TOKEN_END) {
                    predicate.error_offset = predicate.current.offset;
                    status = PRDB_SDK_SQL_SYNTAX_ERROR;
                    goto failed_predicate_v20;
                }
                result.struct_size = sizeof(result);
                result.abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
                *out = result;
                return PRDB_SDK_SQL_OK;

failed_predicate_v20:
                scan.error_offset = predicate.error_offset;
                goto failed_v20;
            }
        }
        status = advance(&scan);
        if (status != PRDB_SDK_SQL_OK) goto failed_v20;
    }
    status = PRDB_SDK_SQL_SYNTAX_ERROR;
    scan.error_offset = (uint32_t)sql_len;

failed_v20:
    if (error_offset_out != NULL) {
        *error_offset_out = scan.error_offset <= scan.length
                                ? scan.error_offset
                                : scan.length;
    }
    return status;
}

PRDB_SDK_INTERNAL int32_t prdb_sdk_sql_bind_select_v20(
    const prdb_sdk_select_v1 *statement,
    const uint8_t *left_relation_name, uint32_t left_relation_name_len,
    const prdb_sdk_sql_column_shape_v1 *left_columns,
    uint32_t left_column_count,
    const uint8_t *right_relation_name, uint32_t right_relation_name_len,
    const prdb_sdk_sql_column_shape_v1 *right_columns,
    uint32_t right_column_count, prdb_sdk_select_binding_v1 *out,
    uint32_t out_capacity) {
    prdb_sdk_select_binding_v1 result = {0};
    prdb_sdk_select_v1 checked;
    int32_t status;

    if (statement == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_BIND_INVALID_ARGUMENT;
    }
    if (statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->has_join_where > 1) {
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    }
    checked = (*statement);
    status = prdb_sdk_sql_bind_select_v19(
        &checked, left_relation_name, left_relation_name_len, left_columns,
        left_column_count, right_relation_name, right_relation_name_len,
        right_columns, right_column_count, &result, sizeof(result));
    if (status != PRDB_SDK_SQL_BIND_OK) return status;
    result.output_projection_count = result.join_projection_count;
    if (statement->has_join_where == 1) {
        prdb_sdk_select_v1 alias_checked = (*statement);
        uint32_t side = UINT32_MAX;
        prdb_sdk_sql_bound_column_v1 column = {0};
        prdb_sdk_sql_qualified_column_v1 where_column =
            statement->join_where_column;
        if (statement->where_keyword_offset >=
                statement->sql_len ||
            statement->where_clause_end <= statement->where_keyword_offset ||
            statement->where_clause_end >
                statement->sql_len ||
            !rebase_v19_relation(&alias_checked, &where_column)) {
            return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
        }
        alias_checked.left_relation = alias_checked.left_alias;
        alias_checked.right_relation = alias_checked.right_alias;
        status = resolve_qualified_column_v8(
            &alias_checked, &where_column,
            alias_checked.sql + alias_checked.left_alias.offset,
            alias_checked.left_alias.length, left_columns, left_column_count,
            alias_checked.sql + alias_checked.right_alias.offset,
            alias_checked.right_alias.length, right_columns,
            right_column_count, &side, &column);
        if (status != PRDB_SDK_SQL_BIND_OK) return status;
        if (result.output_projection_count >=
            PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS) {
            return PRDB_SDK_SQL_LIMIT_EXCEEDED;
        }
        result.where_relation_side = side;
        result.where_projection_index = result.output_projection_count;
        result.where_column = column;
        result.join_projections[result.output_projection_count] =
            (prdb_sdk_sql_join_projection_binding_v1){side, column};
        ++result.join_projection_count;
    } else if (statement->join_where_comparison != 0 || statement->join_where_parameter_index != 0) {
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    }
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
    *out = result;
    return PRDB_SDK_SQL_BIND_OK;
}

PRDB_SDK_INTERNAL int32_t prdb_sdk_sql_parse_select_v25(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v1 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out) {
    parser scan;
    int32_t status;

    if (error_offset_out != NULL) *error_offset_out = 0;
    if (sql == NULL || out == NULL || out_capacity < sizeof(*out))
        return PRDB_SDK_SQL_INVALID_ARGUMENT;
    if (sql_len == 0) return PRDB_SDK_SQL_SYNTAX_ERROR;
    if (sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES) {
        if (error_offset_out != NULL)
            *error_offset_out = PRDB_SDK_SQL_MAX_QUERY_BYTES;
        return PRDB_SDK_SQL_LIMIT_EXCEEDED;
    }

    scan = (parser){sql, (uint32_t)sql_len, 0, {TOKEN_END, 0, 0}, 0,
                    PRDB_SDK_SQL_OK, 0};
    status = advance(&scan);
    if (status != PRDB_SDK_SQL_OK) goto failed_v25;
    while (scan.current.kind != TOKEN_END &&
           scan.current.kind != TOKEN_SEMICOLON) {
        if (token_is_keyword(&scan, "ORDER")) {
            prdb_sdk_select_v1 result = {0};
            parser clause = scan;
            uint32_t order_offset = scan.current.offset;

            status = prdb_sdk_sql_parse_select_v20(
                sql, order_offset, &result, sizeof(result), NULL);
            if (status != PRDB_SDK_SQL_OK) goto failed_v25;
            status = advance(&clause);
            if (status != PRDB_SDK_SQL_OK) goto failed_clause_v25;
            if (!token_is_keyword(&clause, "BY")) {
                clause.error_offset = clause.current.offset;
                status = PRDB_SDK_SQL_SYNTAX_ERROR;
                goto failed_clause_v25;
            }
            status = advance(&clause);
            if (status != PRDB_SDK_SQL_OK) goto failed_clause_v25;
            status = parse_qualified_column_v8(
                &clause, &result.join_order_by_column);
            if (status != PRDB_SDK_SQL_OK) goto failed_clause_v25;
            if (token_is_keyword(&clause, "DESC")) {
                result.join_order_descending = 1;
                status = advance(&clause);
                if (status != PRDB_SDK_SQL_OK) goto failed_clause_v25;
            } else if (token_is_keyword(&clause, "ASC")) {
                status = advance(&clause);
                if (status != PRDB_SDK_SQL_OK) goto failed_clause_v25;
            }
            if (token_is_keyword(&clause, "LIMIT")) {
                token limit_token;
                result.has_limit = 1;
                status = advance(&clause);
                if (status != PRDB_SDK_SQL_OK) goto failed_clause_v25;
                limit_token = clause.current;
                status = parse_unsigned(
                    &clause, limit_token, PRDB_SDK_SQL_MAX_LIMIT, 1,
                    &result.limit);
                if (status != PRDB_SDK_SQL_OK) goto failed_clause_v25;
                status = advance(&clause);
                if (status != PRDB_SDK_SQL_OK) goto failed_clause_v25;
            }
            if (clause.current.kind == TOKEN_SEMICOLON) {
                status = advance(&clause);
                if (status != PRDB_SDK_SQL_OK) goto failed_clause_v25;
            }
            if (clause.current.kind != TOKEN_END) {
                clause.error_offset = clause.current.offset;
                status = PRDB_SDK_SQL_SYNTAX_ERROR;
                goto failed_clause_v25;
            }
            result.sql_len = sql_len;
            result.order_keyword_offset = order_offset;
            result.join_has_order_by = 1;
            result.struct_size = sizeof(result);
            result.abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
            *out = result;
            return PRDB_SDK_SQL_OK;

failed_clause_v25:
            scan.error_offset = clause.error_offset;
            goto failed_v25;
        }
        status = advance(&scan);
        if (status != PRDB_SDK_SQL_OK) goto failed_v25;
    }
    return prdb_sdk_sql_parse_select_v20(sql, sql_len, out, out_capacity,
                                         error_offset_out);

failed_v25:
    if (error_offset_out != NULL)
        *error_offset_out = scan.error_offset <= scan.length
                                ? scan.error_offset
                                : scan.length;
    return status;
}

PRDB_SDK_INTERNAL int32_t prdb_sdk_sql_bind_select_v25(
    const prdb_sdk_select_v1 *statement,
    const uint8_t *left_relation_name, uint32_t left_relation_name_len,
    const prdb_sdk_sql_column_shape_v1 *left_columns,
    uint32_t left_column_count, const uint8_t *right_relation_name,
    uint32_t right_relation_name_len,
    const prdb_sdk_sql_column_shape_v1 *right_columns,
    uint32_t right_column_count, prdb_sdk_select_binding_v1 *out,
    uint32_t out_capacity) {
    prdb_sdk_select_binding_v1 result = {0};
    prdb_sdk_select_v1 alias_checked;
    prdb_sdk_sql_qualified_column_v1 order_column;
    uint32_t side = UINT32_MAX;
    int32_t status;

    if (statement == NULL || out == NULL || out_capacity < sizeof(*out))
        return PRDB_SDK_SQL_BIND_INVALID_ARGUMENT;
    if (statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->join_has_order_by > 1 || statement->join_order_descending > 1)
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    if (statement->join_has_order_by == 0) {
        return prdb_sdk_sql_bind_select_v20(
            statement, left_relation_name, left_relation_name_len,
            left_columns, left_column_count, right_relation_name,
            right_relation_name_len, right_columns, right_column_count, out,
            out_capacity);
    }
    alias_checked = (*statement);
    order_column = statement->join_order_by_column;
    if (!v19_span_in_sql(alias_checked.sql,
                         alias_checked.sql_len,
                         order_column.relation) ||
        !v19_span_in_sql(alias_checked.sql,
                         alias_checked.sql_len,
                         order_column.column) ||
        !rebase_v19_relation(&alias_checked, &order_column))
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    status = prdb_sdk_sql_bind_select_v20(
        statement, left_relation_name, left_relation_name_len,
        left_columns, left_column_count, right_relation_name,
        right_relation_name_len, right_columns, right_column_count,
        &result, sizeof(result));
    if (status != PRDB_SDK_SQL_BIND_OK) return status;
    alias_checked.left_relation = alias_checked.left_alias;
    alias_checked.right_relation = alias_checked.right_alias;
    status = resolve_qualified_column_v8(
        &alias_checked, &order_column,
        alias_checked.sql + alias_checked.left_alias.offset,
        alias_checked.left_alias.length, left_columns, left_column_count,
        alias_checked.sql + alias_checked.right_alias.offset,
        alias_checked.right_alias.length, right_columns, right_column_count,
        &side, &result.join_order_by_column);
    if (status != PRDB_SDK_SQL_BIND_OK) return status;

    for (uint32_t index = 0; index < result.output_projection_count;
         ++index) {
        const prdb_sdk_sql_join_projection_binding_v1 *projection =
            &result.join_projections[index];
        if (projection->relation_side == side &&
            projection->column.ordinal == result.join_order_by_column.ordinal) {
            result.order_by_projection_index = index;
            result.order_relation_side = side;
            result.struct_size = sizeof(result);
            result.abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
            *out = result;
            return PRDB_SDK_SQL_BIND_OK;
        }
    }
    return PRDB_SDK_SQL_BIND_COLUMN_NOT_FOUND;
}
