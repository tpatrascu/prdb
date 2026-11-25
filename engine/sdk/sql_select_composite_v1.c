#include "sql_internal.h"

static int32_t parse_select_v13_mode(const uint8_t *sql, uint64_t sql_len,
                                    prdb_sdk_select_v13 *out,
                                    uint32_t out_capacity,
                                    uint32_t *error_offset_out,
                                    uint32_t min_group_keys) {
    parser state;
    prdb_sdk_select_v13 result = {0};
    prdb_sdk_sql_span_v1 selected_keys[PRDB_SDK_SQL_V13_MAX_GROUP_KEYS] =
        {{0}};
    int aggregate_ready = 0;
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
    state = (parser){sql, (uint32_t)sql_len, 0, {TOKEN_END, 0, 0}, 0,
                     PRDB_SDK_SQL_OK, 0};
    result.base.struct_size = sizeof(result.base);
    result.base.abi_version = PRDB_SDK_SQL_ABI_V7_VERSION;
    result.base.base.struct_size = sizeof(result.base.base);
    result.base.base.abi_version = PRDB_SDK_SQL_ABI_V2_VERSION;
    result.base.base.sql = sql;
    result.base.base.sql_len = sql_len;

    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) goto failed_v13;
    if (!token_is_keyword(&state, "SELECT")) {
        status = state.current.kind == TOKEN_IDENTIFIER
                     ? PRDB_SDK_SQL_UNSUPPORTED_STATEMENT
                     : PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_v13;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_v13;
    }
    for (;;) {
        if (result.group_key_count >= PRDB_SDK_SQL_V13_MAX_GROUP_KEYS) {
            status = PRDB_SDK_SQL_SYNTAX_ERROR;
            state.error_offset = state.current.offset;
            goto failed_v13;
        }
        status = expect_identifier(&state,
                                   &selected_keys[result.group_key_count]);
        if (status != PRDB_SDK_SQL_OK) goto failed_v13;
        for (uint32_t index = 0; index < result.group_key_count; ++index) {
            if (prdb_sdk_internal_span_matches_name(&result.base.base,
                                  selected_keys[result.group_key_count],
                                  sql + selected_keys[index].offset,
                                  selected_keys[index].length)) {
                status = PRDB_SDK_SQL_SYNTAX_ERROR;
                state.error_offset = selected_keys[result.group_key_count].offset;
                goto failed_v13;
            }
        }
        ++result.group_key_count;
        if (state.current.kind != TOKEN_COMMA) break;
        if (advance(&state) != PRDB_SDK_SQL_OK) {
            status = state.error;
            goto failed_v13;
        }
        if (token_is_keyword(&state, "COUNT") ||
            token_is_keyword(&state, "MIN") ||
            token_is_keyword(&state, "MAX") ||
            token_is_keyword(&state, "SUM") ||
            token_is_keyword(&state, "AVG")) {
            aggregate_ready = 1;
            break;
        }
    }
    if (result.group_key_count < min_group_keys) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_v13;
    }
    if (!aggregate_ready && state.current.kind != TOKEN_COMMA) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_v13;
    }
    result.base.base.projection_count = 1;
    result.base.base.projection[0] = selected_keys[0];
    if (!aggregate_ready && advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_v13;
    }
    if (token_is_keyword(&state, "COUNT")) {
        result.base.aggregate_kind = PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR;
    } else if (token_is_keyword(&state, "MIN")) {
        result.base.aggregate_kind = PRDB_SDK_SQL_V7_AGGREGATE_MIN;
    } else if (token_is_keyword(&state, "MAX")) {
        result.base.aggregate_kind = PRDB_SDK_SQL_V7_AGGREGATE_MAX;
    } else if (token_is_keyword(&state, "SUM")) {
        result.base.aggregate_kind = PRDB_SDK_SQL_V7_AGGREGATE_SUM;
    } else if (token_is_keyword(&state, "AVG")) {
        result.base.aggregate_kind = PRDB_SDK_SQL_V7_AGGREGATE_AVG;
    } else {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_v13;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK ||
        state.current.kind != TOKEN_LEFT_PAREN) {
        status = state.error == PRDB_SDK_SQL_OK
                     ? PRDB_SDK_SQL_SYNTAX_ERROR
                     : state.error;
        if (state.error == PRDB_SDK_SQL_OK) {
            state.error_offset = state.current.offset;
        }
        goto failed_v13;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_v13;
    }
    if (result.base.aggregate_kind ==
        PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
        if (state.current.kind != TOKEN_STAR) {
            status = PRDB_SDK_SQL_SYNTAX_ERROR;
            state.error_offset = state.current.offset;
            goto failed_v13;
        }
        if (advance(&state) != PRDB_SDK_SQL_OK) {
            status = state.error;
            goto failed_v13;
        }
    } else {
        status = expect_identifier(&state, &result.base.aggregate_column);
        if (status != PRDB_SDK_SQL_OK) goto failed_v13;
    }
    if (state.current.kind != TOKEN_RIGHT_PAREN) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_v13;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_v13;
    }
    if (!token_is_keyword(&state, "FROM")) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_v13;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_v13;
    }
    status = expect_identifier(&state, &result.base.base.relation);
    if (status != PRDB_SDK_SQL_OK) goto failed_v13;
    if (token_is_keyword(&state, "WHERE")) {
        if (advance(&state) != PRDB_SDK_SQL_OK) {
            status = state.error;
            goto failed_v13;
        }
        status = prdb_sdk_internal_parse_predicate_or_v2(
            &state, &result.base.base, 0, &result.base.base.predicate_root);
        if (status != PRDB_SDK_SQL_OK) goto failed_v13;
    }
    if (!token_is_keyword(&state, "GROUP")) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_v13;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK ||
        !token_is_keyword(&state, "BY")) {
        status = state.error == PRDB_SDK_SQL_OK
                     ? PRDB_SDK_SQL_SYNTAX_ERROR
                     : state.error;
        if (state.error == PRDB_SDK_SQL_OK) {
            state.error_offset = state.current.offset;
        }
        goto failed_v13;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_v13;
    }
    result.base.has_group_by = 1;
    for (uint32_t index = 0; index < result.group_key_count; ++index) {
        status = expect_identifier(&state, &result.group_keys[index]);
        if (status != PRDB_SDK_SQL_OK) goto failed_v13;
        if (!prdb_sdk_internal_span_matches_name(&result.base.base, result.group_keys[index],
                               sql + selected_keys[index].offset,
                               selected_keys[index].length)) {
            status = PRDB_SDK_SQL_SYNTAX_ERROR;
            state.error_offset = result.group_keys[index].offset;
            goto failed_v13;
        }
        result.projection_keys[index] = selected_keys[index];
        if (index == 0) {
            result.base.group_by_column = result.group_keys[index];
        }
        if (index + 1 < result.group_key_count) {
            if (state.current.kind != TOKEN_COMMA) {
                status = PRDB_SDK_SQL_SYNTAX_ERROR;
                state.error_offset = state.current.offset;
                goto failed_v13;
            }
            if (advance(&state) != PRDB_SDK_SQL_OK) {
                status = state.error;
                goto failed_v13;
            }
        }
    }
    if (token_is_keyword(&state, "LIMIT")) {
        token limit_token;
        result.base.base.has_limit = 1;
        if (advance(&state) != PRDB_SDK_SQL_OK) {
            status = state.error;
            goto failed_v13;
        }
        limit_token = state.current;
        status = parse_unsigned(&state, limit_token, PRDB_SDK_SQL_MAX_LIMIT,
                                1, &result.base.base.limit);
        if (status != PRDB_SDK_SQL_OK) goto failed_v13;
        if (advance(&state) != PRDB_SDK_SQL_OK) {
            status = state.error;
            goto failed_v13;
        }
    }
    if (state.current.kind == TOKEN_SEMICOLON &&
        advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_v13;
    }
    if (state.current.kind != TOKEN_END) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_v13;
    }
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V13_VERSION;
    *out = result;
    return PRDB_SDK_SQL_OK;

failed_v13:
    if (error_offset_out != NULL) *error_offset_out = state.error_offset;
    return fail_at(&state, status);
}

int32_t prdb_sdk_sql_parse_select_v13(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v13 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out) {
    return parse_select_v13_mode(sql, sql_len, out, out_capacity,
                                  error_offset_out, 2);
}

static int32_t bind_select_v13_mode(
    const prdb_sdk_select_v13 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v13 *out, uint32_t out_capacity,
    uint32_t min_group_keys) {
    prdb_sdk_select_binding_v13 result = {0};
    int32_t status;

    if (statement == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_BIND_INVALID_ARGUMENT;
    }
    if (statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V13_VERSION ||
        statement->group_key_count < min_group_keys ||
        statement->group_key_count > PRDB_SDK_SQL_V13_MAX_GROUP_KEYS ||
        statement->base.base.projection_count != 1 ||
        statement->base.base.projection_star != 0 ||
        statement->base.has_group_by != 1) {
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    }
    status = prdb_sdk_sql_bind_select_v7(
        &statement->base, relation_name, relation_name_len, columns,
        column_count, &result.base, sizeof(result.base));
    if (status != PRDB_SDK_SQL_BIND_OK) return status;
    for (uint32_t index = 0; index < statement->group_key_count; ++index) {
        const prdb_sdk_sql_span_v1 projection =
            statement->projection_keys[index];
        const prdb_sdk_sql_span_v1 key = statement->group_keys[index];
        uint32_t ordinal;
        uint32_t shape_index;

        if (projection.length == 0 || key.length == 0 ||
            projection.offset > statement->base.base.sql_len ||
            projection.length >
                statement->base.base.sql_len - projection.offset ||
            key.offset > statement->base.base.sql_len ||
            key.length > statement->base.base.sql_len - key.offset ||
            !prdb_sdk_internal_span_matches_name(&statement->base.base, projection,
                               statement->base.base.sql + key.offset,
                               key.length)) {
            return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
        }
        status = prdb_sdk_internal_find_column_shape_v2(&statement->base.base, key, columns,
                                      column_count, &ordinal);
        if (status != PRDB_SDK_SQL_BIND_OK) return status;
        for (uint32_t prior = 0; prior < index; ++prior) {
            if (result.group_keys[prior].ordinal == ordinal) {
                return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
            }
        }
        for (shape_index = 0; shape_index < column_count; ++shape_index) {
            if (columns[shape_index].ordinal == ordinal) break;
        }
        if (shape_index == column_count) {
            return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
        }
        result.group_keys[index].ordinal = ordinal;
        result.group_keys[index].type_code = columns[shape_index].type_code;
        result.group_keys[index].decimal_precision =
            columns[shape_index].decimal_precision;
        result.group_keys[index].decimal_scale =
            columns[shape_index].decimal_scale;
    }
    if (result.group_keys[0].ordinal != result.base.group_by_column.ordinal ||
        statement->group_keys[0].offset !=
            statement->base.group_by_column.offset ||
        statement->group_keys[0].length !=
            statement->base.group_by_column.length ||
        statement->projection_keys[0].offset !=
            statement->base.base.projection[0].offset ||
        statement->projection_keys[0].length !=
            statement->base.base.projection[0].length) {
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    }
    for (uint32_t index = statement->group_key_count;
         index < PRDB_SDK_SQL_V13_MAX_GROUP_KEYS; ++index) {
        const prdb_sdk_sql_span_v1 zero = {0};
        if (memcmp(&statement->projection_keys[index], &zero,
                   sizeof(zero)) != 0 ||
            memcmp(&statement->group_keys[index], &zero, sizeof(zero)) != 0) {
            return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
        }
    }
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V13_VERSION;
    result.group_key_count = statement->group_key_count;
    *out = result;
    return PRDB_SDK_SQL_BIND_OK;
}

int32_t prdb_sdk_sql_bind_select_v13(
    const prdb_sdk_select_v13 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v13 *out, uint32_t out_capacity) {
    return bind_select_v13_mode(statement, relation_name, relation_name_len,
                                columns, column_count, out, out_capacity, 2);
}

static int32_t parse_select_v14_mode(const uint8_t *sql, uint64_t sql_len,
                                    prdb_sdk_select_v14 *out,
                                    uint32_t out_capacity,
                                    uint32_t *error_offset_out,
                                    uint32_t min_group_keys) {
    uint8_t base_sql[PRDB_SDK_SQL_MAX_QUERY_BYTES];
    parser scan, having;
    prdb_sdk_select_v14 result = {0};
    token having_token = {TOKEN_END, 0, 0};
    uint32_t parameter_index = 0, comparison = 0, group_by_end = 0;
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
    memcpy(base_sql, sql, (size_t)sql_len);
    scan = (parser){sql, (uint32_t)sql_len, 0, {TOKEN_END, 0, 0}, 0,
                    PRDB_SDK_SQL_OK, 0};
    status = advance(&scan);
    if (status != PRDB_SDK_SQL_OK) goto failed_v14;
    while (scan.current.kind != TOKEN_END &&
           scan.current.kind != TOKEN_SEMICOLON) {
        if (token_is_keyword(&scan, "GROUP")) {
            saw_group = 1;
        } else if (saw_group && token_is_keyword(&scan, "BY")) {
            group_by_end = scan.current.offset + scan.current.length;
            saw_group = 0;
        } else if (!token_is_keyword(&scan, "GROUP")) {
            saw_group = 0;
        }
        if (token_is_keyword(&scan, "HAVING")) {
            if (having_token.kind != TOKEN_END) {
                status = PRDB_SDK_SQL_SYNTAX_ERROR;
                scan.error_offset = scan.current.offset;
                goto failed_v14;
            }
            having_token = scan.current;
        }
        status = advance(&scan);
        if (status != PRDB_SDK_SQL_OK) goto failed_v14;
    }
    if (having_token.kind != TOKEN_END) {
        uint32_t clause_end = (uint32_t)sql_len;
        if (group_by_end == 0 || having_token.offset < group_by_end) {
            status = PRDB_SDK_SQL_SYNTAX_ERROR;
            scan.error_offset = having_token.offset;
            goto failed_v14;
        }
        for (uint32_t index = having_token.offset; index < clause_end; ++index) {
            base_sql[index] = ' ';
        }
        /* Keep a trailing LIMIT and optional semicolon in the v13 parse. */
        scan = (parser){sql, (uint32_t)sql_len,
                        having_token.offset + having_token.length,
                        {TOKEN_END, 0, 0}, 0, PRDB_SDK_SQL_OK, 0};
        status = advance(&scan);
        if (status != PRDB_SDK_SQL_OK) goto failed_v14;
        while (scan.current.kind != TOKEN_END &&
               scan.current.kind != TOKEN_SEMICOLON) {
            if (token_is_keyword(&scan, "LIMIT")) {
                clause_end = scan.current.offset;
                break;
            }
            status = advance(&scan);
            if (status != PRDB_SDK_SQL_OK) goto failed_v14;
        }
        for (uint32_t index = clause_end; index < (uint32_t)sql_len; ++index) {
            base_sql[index] = sql[index];
        }
        status = parse_select_v13_mode(
            base_sql, sql_len, &result.base, sizeof(result.base),
            error_offset_out, min_group_keys);
        if (status != PRDB_SDK_SQL_OK) return status;
        result.base.base.base.sql = sql;
        result.has_having = 1;
        having = (parser){sql, (uint32_t)sql_len,
                          having_token.offset + having_token.length,
                          {TOKEN_END, 0, 0}, 0, PRDB_SDK_SQL_OK, 0};
        status = advance(&having);
        if (status != PRDB_SDK_SQL_OK) goto failed_having_v14;
        if (token_is_keyword(&having, "COUNT")) {
            if (result.base.base.aggregate_kind !=
                PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
                status = PRDB_SDK_SQL_SYNTAX_ERROR;
                having.error_offset = having.current.offset;
                goto failed_having_v14;
            }
        } else if (token_is_keyword(&having, "MIN") ||
                   token_is_keyword(&having, "MAX") ||
                   token_is_keyword(&having, "SUM") ||
                   token_is_keyword(&having, "AVG")) {
            uint32_t kind = token_is_keyword(&having, "MIN")
                                ? PRDB_SDK_SQL_V7_AGGREGATE_MIN
                            : token_is_keyword(&having, "MAX")
                                ? PRDB_SDK_SQL_V7_AGGREGATE_MAX
                            : token_is_keyword(&having, "SUM")
                                ? PRDB_SDK_SQL_V7_AGGREGATE_SUM
                                : PRDB_SDK_SQL_V7_AGGREGATE_AVG;
            if (result.base.base.aggregate_kind != kind) {
                status = PRDB_SDK_SQL_SYNTAX_ERROR;
                having.error_offset = having.current.offset;
                goto failed_having_v14;
            }
        } else {
            status = PRDB_SDK_SQL_SYNTAX_ERROR;
            having.error_offset = having.current.offset;
            goto failed_having_v14;
        }
        status = advance(&having);
        if (status != PRDB_SDK_SQL_OK ||
            having.current.kind != TOKEN_LEFT_PAREN) {
            status = status == PRDB_SDK_SQL_OK ? PRDB_SDK_SQL_SYNTAX_ERROR
                                                : status;
            having.error_offset = having.current.offset;
            goto failed_having_v14;
        }
        status = advance(&having);
        if (status != PRDB_SDK_SQL_OK) goto failed_having_v14;
        if (result.base.base.aggregate_kind ==
            PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
            if (having.current.kind != TOKEN_STAR) {
                status = PRDB_SDK_SQL_SYNTAX_ERROR;
                having.error_offset = having.current.offset;
                goto failed_having_v14;
            }
        } else if (having.current.kind != TOKEN_IDENTIFIER ||
                   !prdb_sdk_internal_span_matches_name(&result.base.base.base,
                                      result.base.base.aggregate_column,
                                      sql + having.current.offset,
                                      having.current.length)) {
            status = PRDB_SDK_SQL_SYNTAX_ERROR;
            having.error_offset = having.current.offset;
            goto failed_having_v14;
        }
        status = advance(&having);
        if (status != PRDB_SDK_SQL_OK ||
            having.current.kind != TOKEN_RIGHT_PAREN) {
            status = status == PRDB_SDK_SQL_OK ? PRDB_SDK_SQL_SYNTAX_ERROR
                                                : status;
            having.error_offset = having.current.offset;
            goto failed_having_v14;
        }
        status = advance(&having);
        if (status != PRDB_SDK_SQL_OK) goto failed_having_v14;
        status = comparison_code(having.current.kind, &comparison);
        if (status != PRDB_SDK_SQL_OK) {
            having.error_offset = having.current.offset;
            goto failed_having_v14;
        }
        result.having_comparison = comparison;
        status = advance(&having);
        if (status != PRDB_SDK_SQL_OK ||
            having.current.kind != TOKEN_PARAMETER) {
            status = status == PRDB_SDK_SQL_OK ? PRDB_SDK_SQL_SYNTAX_ERROR
                                                : status;
            having.error_offset = having.current.offset;
            goto failed_having_v14;
        }
        status = parse_unsigned(&having, having.current,
                                PRDB_SDK_SQL_MAX_PARAMETER_INDEX, 0,
                                &parameter_index);
        if (status != PRDB_SDK_SQL_OK) goto failed_having_v14;
        result.having_parameter_index = parameter_index;
        status = advance(&having);
        if (status != PRDB_SDK_SQL_OK) goto failed_having_v14;
        if (!token_is_keyword(&having, "LIMIT") &&
            having.current.kind != TOKEN_SEMICOLON &&
            having.current.kind != TOKEN_END) {
            status = PRDB_SDK_SQL_SYNTAX_ERROR;
            having.error_offset = having.current.offset;
            goto failed_having_v14;
        }
    }
    if (having_token.kind == TOKEN_END) {
        status = parse_select_v13_mode(
            sql, sql_len, &result.base, sizeof(result.base), error_offset_out,
            min_group_keys);
        if (status != PRDB_SDK_SQL_OK) return status;
    }
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V14_VERSION;
    *out = result;
    return PRDB_SDK_SQL_OK;

failed_having_v14:
    if (error_offset_out != NULL) *error_offset_out = having.error_offset;
    return fail_at(&having, status);
failed_v14:
    if (error_offset_out != NULL) *error_offset_out = scan.error_offset;
    return fail_at(&scan, status);
}

int32_t prdb_sdk_sql_parse_select_v14(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v14 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out) {
    return parse_select_v14_mode(sql, sql_len, out, out_capacity,
                                  error_offset_out, 2);
}

static int32_t bind_select_v14_mode(
    const prdb_sdk_select_v14 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v14 *out, uint32_t out_capacity,
    uint32_t min_group_keys) {
    prdb_sdk_select_binding_v14 result = {0};
    const prdb_sdk_sql_bound_column_v2 *aggregate;
    uint32_t kind;
    int32_t status;

    if (statement == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_BIND_INVALID_ARGUMENT;
    }
    if (statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V14_VERSION ||
        statement->has_having > 1 ||
        (statement->has_having == 0 &&
         (statement->having_comparison != 0 ||
          statement->having_parameter_index != 0)) ||
        (statement->has_having == 1 &&
         (statement->having_comparison < PRDB_SDK_SQL_COMPARE_EQUAL ||
          statement->having_comparison >
              PRDB_SDK_SQL_COMPARE_GREATER_EQUAL ||
          statement->having_parameter_index == 0 ||
          statement->having_parameter_index >
              PRDB_SDK_SQL_MAX_PARAMETER_INDEX))) {
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    }
    status = bind_select_v13_mode(
        &statement->base, relation_name, relation_name_len, columns,
        column_count, &result.base, sizeof(result.base), min_group_keys);
    if (status != PRDB_SDK_SQL_BIND_OK) return status;
    result.has_having = statement->has_having;
    result.having_comparison = statement->having_comparison;
    result.having_parameter_index = statement->having_parameter_index;
    aggregate = &result.base.base.aggregate_column;
    kind = result.base.base.aggregate_kind;
    if (result.has_having) {
        if (kind == PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
            result.having_type_code = PRDB_SDK_VALUE_TYPE_BIGINT;
        } else if (kind == PRDB_SDK_SQL_V7_AGGREGATE_MIN ||
                   kind == PRDB_SDK_SQL_V7_AGGREGATE_MAX) {
            result.having_type_code = aggregate->type_code;
            result.having_decimal_precision = aggregate->decimal_precision;
            result.having_decimal_scale = aggregate->decimal_scale;
        } else if (kind == PRDB_SDK_SQL_V7_AGGREGATE_SUM) {
            result.having_type_code = aggregate->type_code ==
                                              PRDB_SDK_VALUE_TYPE_DECIMAL
                                          ? PRDB_SDK_VALUE_TYPE_DECIMAL
                                          : PRDB_SDK_VALUE_TYPE_BIGINT;
            if (aggregate->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL) {
                result.having_decimal_precision = 38;
                result.having_decimal_scale = aggregate->decimal_scale;
            }
        } else if (kind == PRDB_SDK_SQL_V7_AGGREGATE_AVG) {
            uint32_t integer_digits =
                aggregate->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL
                    ? aggregate->decimal_precision - aggregate->decimal_scale
                    : 0;
            uint32_t desired_scale = aggregate->type_code ==
                                             PRDB_SDK_VALUE_TYPE_DECIMAL
                                         ? aggregate->decimal_scale + 6
                                         : 6;
            uint32_t available_scale = 38 - integer_digits;
            if (desired_scale > available_scale) desired_scale = available_scale;
            result.having_type_code = PRDB_SDK_VALUE_TYPE_DECIMAL;
            result.having_decimal_precision = 38;
            result.having_decimal_scale = (uint8_t)desired_scale;
        }
    }
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V14_VERSION;
    *out = result;
    return PRDB_SDK_SQL_BIND_OK;
}

int32_t prdb_sdk_sql_bind_select_v14(
    const prdb_sdk_select_v14 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v14 *out, uint32_t out_capacity) {
    return bind_select_v14_mode(statement, relation_name, relation_name_len,
                                columns, column_count, out, out_capacity, 2);
}

static int32_t parse_order_item_v15(parser *state,
                                   const prdb_sdk_select_v14 *base,
                                   prdb_sdk_sql_order_item_v15 *item) {
    const prdb_sdk_select_v13 *grouped = &base->base;
    uint32_t aggregate_kind = grouped->base.aggregate_kind;
    int32_t status;

    if (token_is_keyword(state, "COUNT") || token_is_keyword(state, "MIN") ||
        token_is_keyword(state, "MAX") || token_is_keyword(state, "SUM") ||
        token_is_keyword(state, "AVG")) {
        uint32_t parsed_kind = token_is_keyword(state, "COUNT")
                                   ? PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR
                               : token_is_keyword(state, "MIN")
                                   ? PRDB_SDK_SQL_V7_AGGREGATE_MIN
                               : token_is_keyword(state, "MAX")
                                   ? PRDB_SDK_SQL_V7_AGGREGATE_MAX
                               : token_is_keyword(state, "SUM")
                                   ? PRDB_SDK_SQL_V7_AGGREGATE_SUM
                                   : PRDB_SDK_SQL_V7_AGGREGATE_AVG;
        if (parsed_kind != aggregate_kind) {
            state->error_offset = state->current.offset;
            return PRDB_SDK_SQL_SYNTAX_ERROR;
        }
        item->kind = PRDB_SDK_SQL_V10_ORDER_AGGREGATE;
        if (advance(state) != PRDB_SDK_SQL_OK ||
            state->current.kind != TOKEN_LEFT_PAREN) {
            state->error_offset = state->current.offset;
            return state->error == PRDB_SDK_SQL_OK
                       ? PRDB_SDK_SQL_SYNTAX_ERROR
                       : state->error;
        }
        status = advance(state);
        if (status != PRDB_SDK_SQL_OK) return status;
        if (aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
            if (state->current.kind != TOKEN_STAR) {
                state->error_offset = state->current.offset;
                return PRDB_SDK_SQL_SYNTAX_ERROR;
            }
        } else if (state->current.kind != TOKEN_IDENTIFIER ||
                   !prdb_sdk_internal_span_matches_name(&grouped->base.base,
                                      grouped->base.aggregate_column,
                                      state->sql + state->current.offset,
                                      state->current.length)) {
            state->error_offset = state->current.offset;
            return PRDB_SDK_SQL_SYNTAX_ERROR;
        }
        status = advance(state);
        if (status != PRDB_SDK_SQL_OK ||
            state->current.kind != TOKEN_RIGHT_PAREN) {
            state->error_offset = state->current.offset;
            return state->error == PRDB_SDK_SQL_OK
                       ? PRDB_SDK_SQL_SYNTAX_ERROR
                       : state->error;
        }
        return advance(state);
    }
    if (state->current.kind != TOKEN_IDENTIFIER) {
        state->error_offset = state->current.offset;
        return PRDB_SDK_SQL_SYNTAX_ERROR;
    }
    for (uint32_t index = 0; index < grouped->group_key_count; ++index) {
        if (prdb_sdk_internal_span_matches_name(&grouped->base.base,
                              grouped->projection_keys[index],
                              state->sql + state->current.offset,
                              state->current.length)) {
            item->kind = PRDB_SDK_SQL_V10_ORDER_GROUP_KEY;
            item->group_key_index = index;
            return advance(state);
        }
    }
    state->error_offset = state->current.offset;
    return PRDB_SDK_SQL_SYNTAX_ERROR;
}

PRDB_SDK_INTERNAL int32_t prdb_sdk_internal_parse_select_v15_mode(const uint8_t *sql, uint64_t sql_len,
                                    prdb_sdk_select_v15 *out,
                                    uint32_t out_capacity,
                                    uint32_t *error_offset_out,
                                    uint32_t min_group_keys) {
    uint8_t base_sql[PRDB_SDK_SQL_MAX_QUERY_BYTES];
    parser scan, order;
    prdb_sdk_select_v15 result = {0};
    uint32_t order_start = 0, group_by_end = 0;
    uint32_t having_offset = UINT32_MAX, limit_offset = UINT32_MAX;
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
    memcpy(base_sql, sql, (size_t)sql_len);
    scan = (parser){sql, (uint32_t)sql_len, 0, {TOKEN_END, 0, 0}, 0,
                    PRDB_SDK_SQL_OK, 0};
    status = advance(&scan);
    if (status != PRDB_SDK_SQL_OK) goto failed_v15;
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
        if (token_is_keyword(&scan, "HAVING") &&
            having_offset == UINT32_MAX) {
            having_offset = scan.current.offset;
        }
        if (token_is_keyword(&scan, "LIMIT") &&
            limit_offset == UINT32_MAX) {
            limit_offset = scan.current.offset;
        }
        if (token_is_keyword(&scan, "ORDER")) {
            parser lookahead = scan;
            status = advance(&lookahead);
            if (status != PRDB_SDK_SQL_OK) goto failed_v15;
            if (token_is_keyword(&lookahead, "BY")) {
                if (order_start != 0) {
                    status = PRDB_SDK_SQL_SYNTAX_ERROR;
                    scan.error_offset = scan.current.offset;
                    goto failed_v15;
                }
                order_start = scan.current.offset;
            }
        }
        status = advance(&scan);
        if (status != PRDB_SDK_SQL_OK) goto failed_v15;
    }
    if (order_start != 0) {
        uint32_t by_end, clause_end = (uint32_t)sql_len;
        if (group_by_end == 0 || order_start < group_by_end ||
            (having_offset != UINT32_MAX && order_start < having_offset) ||
            (limit_offset != UINT32_MAX && order_start > limit_offset)) {
            status = PRDB_SDK_SQL_SYNTAX_ERROR;
            scan.error_offset = order_start;
            goto failed_v15;
        }
        scan = (parser){sql, (uint32_t)sql_len, order_start,
                        {TOKEN_END, 0, 0}, 0, PRDB_SDK_SQL_OK, 0};
        status = advance(&scan);
        if (status != PRDB_SDK_SQL_OK ||
            !token_is_keyword(&scan, "ORDER")) {
            status = status == PRDB_SDK_SQL_OK ? PRDB_SDK_SQL_SYNTAX_ERROR
                                               : status;
            goto failed_v15;
        }
        status = advance(&scan);
        if (status != PRDB_SDK_SQL_OK || !token_is_keyword(&scan, "BY")) {
            status = status == PRDB_SDK_SQL_OK ? PRDB_SDK_SQL_SYNTAX_ERROR
                                               : status;
            goto failed_v15;
        }
        by_end = scan.current.offset + scan.current.length;
        status = advance(&scan);
        if (status != PRDB_SDK_SQL_OK) goto failed_v15;
        while (scan.current.kind != TOKEN_END &&
               scan.current.kind != TOKEN_SEMICOLON) {
            if (token_is_keyword(&scan, "LIMIT")) {
                clause_end = scan.current.offset;
                break;
            }
            status = advance(&scan);
            if (status != PRDB_SDK_SQL_OK) goto failed_v15;
        }
        for (uint32_t index = order_start; index < clause_end; ++index) {
            base_sql[index] = ' ';
        }
        for (uint32_t index = clause_end; index < (uint32_t)sql_len; ++index) {
            base_sql[index] = sql[index];
        }
        status = parse_select_v14_mode(
            base_sql, sql_len, &result.base, sizeof(result.base),
            error_offset_out, min_group_keys);
        if (status != PRDB_SDK_SQL_OK) return status;
        result.base.base.base.base.sql = sql;
        order = (parser){sql, (uint32_t)sql_len, by_end,
                         {TOKEN_END, 0, 0}, 0, PRDB_SDK_SQL_OK, 0};
        status = advance(&order);
        if (status != PRDB_SDK_SQL_OK) goto failed_order_v15;
        for (;;) {
            prdb_sdk_sql_order_item_v15 *item;
            if (result.order_count >= PRDB_SDK_SQL_V15_MAX_ORDER_KEYS) {
                status = PRDB_SDK_SQL_LIMIT_EXCEEDED;
                order.error_offset = order.current.offset;
                goto failed_order_v15;
            }
            item = &result.order_items[result.order_count];
            status = parse_order_item_v15(&order, &result.base, item);
            if (status != PRDB_SDK_SQL_OK) goto failed_order_v15;
            if (token_is_keyword(&order, "ASC")) {
                item->direction = PRDB_SDK_SQL_V10_ORDER_ASCENDING;
                status = advance(&order);
            } else if (token_is_keyword(&order, "DESC")) {
                item->direction = PRDB_SDK_SQL_V10_ORDER_DESCENDING;
                status = advance(&order);
            }
            if (status != PRDB_SDK_SQL_OK) goto failed_order_v15;
            if (token_is_keyword(&order, "NULLS")) {
                status = advance(&order);
                if (status != PRDB_SDK_SQL_OK) goto failed_order_v15;
                if (token_is_keyword(&order, "FIRST")) {
                    item->null_order = PRDB_SDK_SQL_V11_NULLS_FIRST;
                } else if (token_is_keyword(&order, "LAST")) {
                    item->null_order = PRDB_SDK_SQL_V11_NULLS_LAST;
                } else {
                    status = PRDB_SDK_SQL_SYNTAX_ERROR;
                    order.error_offset = order.current.offset;
                    goto failed_order_v15;
                }
                status = advance(&order);
                if (status != PRDB_SDK_SQL_OK) goto failed_order_v15;
            } else {
                item->null_order = PRDB_SDK_SQL_V11_NULL_ORDER_DEFAULT;
            }
            for (uint32_t prior = 0; prior < result.order_count; ++prior) {
                const prdb_sdk_sql_order_item_v15 *previous =
                    &result.order_items[prior];
                if (item->kind == previous->kind &&
                    (item->kind == PRDB_SDK_SQL_V10_ORDER_AGGREGATE ||
                     item->group_key_index == previous->group_key_index)) {
                    status = PRDB_SDK_SQL_SYNTAX_ERROR;
                    order.error_offset = order.current.offset;
                    goto failed_order_v15;
                }
            }
            ++result.order_count;
            if (order.current.kind != TOKEN_COMMA) break;
            status = advance(&order);
            if (status != PRDB_SDK_SQL_OK) goto failed_order_v15;
        }
        if (!token_is_keyword(&order, "LIMIT") &&
            order.current.kind != TOKEN_SEMICOLON &&
            order.current.kind != TOKEN_END) {
            status = PRDB_SDK_SQL_SYNTAX_ERROR;
            order.error_offset = order.current.offset;
            goto failed_order_v15;
        }
    } else {
        status = parse_select_v14_mode(
            sql, sql_len, &result.base, sizeof(result.base), error_offset_out,
            min_group_keys);
        if (status != PRDB_SDK_SQL_OK) return status;
        result.base.base.base.base.sql = sql;
    }
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V15_VERSION;
    *out = result;
    return PRDB_SDK_SQL_OK;

failed_order_v15:
    if (error_offset_out != NULL) *error_offset_out = order.error_offset;
    return fail_at(&order, status);
failed_v15:
    if (error_offset_out != NULL) *error_offset_out = scan.error_offset;
    return fail_at(&scan, status);
}

int32_t prdb_sdk_sql_parse_select_v15(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v15 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out) {
    return prdb_sdk_internal_parse_select_v15_mode(sql, sql_len, out, out_capacity,
                                  error_offset_out, 2);
}

PRDB_SDK_INTERNAL int32_t prdb_sdk_internal_bind_select_v15_mode(
    const prdb_sdk_select_v15 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v15 *out, uint32_t out_capacity,
    uint32_t min_group_keys) {
    prdb_sdk_select_binding_v15 result = {0};
    int32_t status;

    if (statement == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_BIND_INVALID_ARGUMENT;
    }
    if (statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V15_VERSION ||
        statement->order_count > PRDB_SDK_SQL_V15_MAX_ORDER_KEYS) {
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    }
    status = bind_select_v14_mode(
        &statement->base, relation_name, relation_name_len, columns,
        column_count, &result.base, sizeof(result.base), min_group_keys);
    if (status != PRDB_SDK_SQL_BIND_OK) return status;
    for (uint32_t index = 0; index < statement->order_count; ++index) {
        const prdb_sdk_sql_order_item_v15 *item = &statement->order_items[index];
        prdb_sdk_sql_bound_order_item_v15 *bound = &result.order_items[index];
        uint32_t kind = result.base.base.base.aggregate_kind;
        const prdb_sdk_sql_bound_column_v2 *aggregate =
            &result.base.base.base.aggregate_column;
        if (item->direction > PRDB_SDK_SQL_V10_ORDER_DESCENDING ||
            item->null_order > PRDB_SDK_SQL_V11_NULLS_LAST ||
            (item->kind == PRDB_SDK_SQL_V10_ORDER_GROUP_KEY
                 ? item->group_key_index >= result.base.base.group_key_count
                 : item->kind != PRDB_SDK_SQL_V10_ORDER_AGGREGATE ||
                       item->group_key_index != 0)) {
            return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
        }
        for (uint32_t prior = 0; prior < index; ++prior) {
            const prdb_sdk_sql_order_item_v15 *previous =
                &statement->order_items[prior];
            if (item->kind == previous->kind &&
                (item->kind == PRDB_SDK_SQL_V10_ORDER_AGGREGATE ||
                 item->group_key_index == previous->group_key_index)) {
                return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
            }
        }
        bound->item = *item;
        if (item->kind == PRDB_SDK_SQL_V10_ORDER_GROUP_KEY) {
            const prdb_sdk_sql_bound_column_v2 *group_key =
                &result.base.base.group_keys[item->group_key_index];
            bound->type_code = group_key->type_code;
            bound->decimal_precision = group_key->decimal_precision;
            bound->decimal_scale = group_key->decimal_scale;
        } else if (kind == PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
            bound->type_code = PRDB_SDK_VALUE_TYPE_BIGINT;
        } else if (kind == PRDB_SDK_SQL_V7_AGGREGATE_MIN ||
                   kind == PRDB_SDK_SQL_V7_AGGREGATE_MAX) {
            bound->type_code = aggregate->type_code;
            bound->decimal_precision = aggregate->decimal_precision;
            bound->decimal_scale = aggregate->decimal_scale;
        } else if (kind == PRDB_SDK_SQL_V7_AGGREGATE_SUM) {
            bound->type_code = aggregate->type_code ==
                                       PRDB_SDK_VALUE_TYPE_DECIMAL
                                   ? PRDB_SDK_VALUE_TYPE_DECIMAL
                                   : PRDB_SDK_VALUE_TYPE_BIGINT;
            if (aggregate->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL) {
                bound->decimal_precision = 38;
                bound->decimal_scale = aggregate->decimal_scale;
            }
        } else {
            uint32_t integer_digits =
                aggregate->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL
                    ? aggregate->decimal_precision - aggregate->decimal_scale
                    : 0;
            uint32_t desired_scale = aggregate->type_code ==
                                             PRDB_SDK_VALUE_TYPE_DECIMAL
                                         ? aggregate->decimal_scale + 6
                                         : 6;
            uint32_t available_scale = 38 - integer_digits;
            if (desired_scale > available_scale) desired_scale = available_scale;
            bound->type_code = PRDB_SDK_VALUE_TYPE_DECIMAL;
            bound->decimal_precision = 38;
            bound->decimal_scale = (uint8_t)desired_scale;
        }
    }
    for (uint32_t index = statement->order_count;
         index < PRDB_SDK_SQL_V15_MAX_ORDER_KEYS; ++index) {
        const prdb_sdk_sql_order_item_v15 zero = {0};
        if (memcmp(&statement->order_items[index], &zero, sizeof(zero)) != 0) {
            return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
        }
    }
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V15_VERSION;
    result.order_count = statement->order_count;
    *out = result;
    return PRDB_SDK_SQL_BIND_OK;
}

int32_t prdb_sdk_sql_bind_select_v15(
    const prdb_sdk_select_v15 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v15 *out, uint32_t out_capacity) {
    return prdb_sdk_internal_bind_select_v15_mode(statement, relation_name, relation_name_len,
                                columns, column_count, out, out_capacity, 2);
}
