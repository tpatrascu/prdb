#include "sql_internal.h"

static int32_t parse_group_order_item(
    parser *state, const prdb_sdk_select_v5 *statement,
    int allow_null_placement, prdb_sdk_sql_order_item_v12 *item) {
    prdb_sdk_sql_span_v1 order_column = {0};
    int aggregate_matches = 0;
    int32_t status;

    memset(item, 0, sizeof(*item));
    item->direction = PRDB_SDK_SQL_V10_ORDER_ASCENDING;
    if (state->current.kind == TOKEN_IDENTIFIER &&
        prdb_sdk_internal_span_matches_name(&statement->base, statement->group_by_column,
                          state->sql + state->current.offset,
                          state->current.length)) {
        item->kind = PRDB_SDK_SQL_V10_ORDER_GROUP_KEY;
        if (advance(state) != PRDB_SDK_SQL_OK) return state->error;
    } else {
        if (state->current.kind == TOKEN_IDENTIFIER) {
            aggregate_matches =
                (statement->aggregate_kind ==
                     PRDB_SDK_SQL_V5_AGGREGATE_COUNT_STAR &&
                 token_is_keyword(state, "COUNT")) ||
                (statement->aggregate_kind ==
                     PRDB_SDK_SQL_V5_AGGREGATE_MIN &&
                 token_is_keyword(state, "MIN")) ||
                (statement->aggregate_kind ==
                     PRDB_SDK_SQL_V5_AGGREGATE_MAX &&
                 token_is_keyword(state, "MAX")) ||
                (statement->aggregate_kind ==
                     PRDB_SDK_SQL_V6_AGGREGATE_SUM &&
                 token_is_keyword(state, "SUM")) ||
                (statement->aggregate_kind ==
                     PRDB_SDK_SQL_V7_AGGREGATE_AVG &&
                 token_is_keyword(state, "AVG"));
        }
        if (!aggregate_matches) {
            state->error_offset = state->current.offset;
            return PRDB_SDK_SQL_SYNTAX_ERROR;
        }
        if (advance(state) != PRDB_SDK_SQL_OK ||
            state->current.kind != TOKEN_LEFT_PAREN) {
            if (state->error == PRDB_SDK_SQL_OK) {
                state->error_offset = state->current.offset;
                return PRDB_SDK_SQL_SYNTAX_ERROR;
            }
            return state->error;
        }
        if (advance(state) != PRDB_SDK_SQL_OK) return state->error;
        if (statement->aggregate_kind == PRDB_SDK_SQL_V5_AGGREGATE_COUNT_STAR) {
            if (state->current.kind != TOKEN_STAR) {
                state->error_offset = state->current.offset;
                return PRDB_SDK_SQL_SYNTAX_ERROR;
            }
            if (advance(state) != PRDB_SDK_SQL_OK) return state->error;
        } else {
            status = expect_identifier(state, &order_column);
            if (status != PRDB_SDK_SQL_OK) return status;
            if (!prdb_sdk_internal_span_matches_name(&statement->base, order_column,
                                   state->sql +
                                       statement->aggregate_column.offset,
                                   statement->aggregate_column.length)) {
                state->error_offset = order_column.offset;
                return PRDB_SDK_SQL_SYNTAX_ERROR;
            }
        }
        if (state->current.kind != TOKEN_RIGHT_PAREN) {
            state->error_offset = state->current.offset;
            return PRDB_SDK_SQL_SYNTAX_ERROR;
        }
        if (advance(state) != PRDB_SDK_SQL_OK) return state->error;
        item->kind = PRDB_SDK_SQL_V10_ORDER_AGGREGATE;
    }

    if (token_is_keyword(state, "ASC")) {
        item->direction = PRDB_SDK_SQL_V10_ORDER_ASCENDING;
        if (advance(state) != PRDB_SDK_SQL_OK) return state->error;
    } else if (token_is_keyword(state, "DESC")) {
        item->direction = PRDB_SDK_SQL_V10_ORDER_DESCENDING;
        if (advance(state) != PRDB_SDK_SQL_OK) return state->error;
    }
    if (token_is_keyword(state, "NULLS")) {
        if (!allow_null_placement || advance(state) != PRDB_SDK_SQL_OK) {
            if (state->error == PRDB_SDK_SQL_OK) {
                state->error_offset = state->current.offset;
                return PRDB_SDK_SQL_SYNTAX_ERROR;
            }
            return state->error;
        }
        if (token_is_keyword(state, "FIRST")) {
            item->null_order = PRDB_SDK_SQL_V11_NULLS_FIRST;
        } else if (token_is_keyword(state, "LAST")) {
            item->null_order = PRDB_SDK_SQL_V11_NULLS_LAST;
        } else {
            state->error_offset = state->current.offset;
            return PRDB_SDK_SQL_SYNTAX_ERROR;
        }
        if (advance(state) != PRDB_SDK_SQL_OK) return state->error;
    }
    return PRDB_SDK_SQL_OK;
}

static int32_t parse_grouped_select_v5_internal(
    const uint8_t *sql, uint64_t sql_len, prdb_sdk_select_v5 *out,
    uint32_t out_capacity, uint32_t *error_offset_out, int allow_sum,
    int allow_avg, int allow_having, int allow_group_order,
    uint32_t *has_having_out, uint32_t *having_comparison_out,
    uint32_t *having_parameter_index_out, uint32_t *has_order_by_out,
    uint32_t *order_kind_out, uint32_t *order_direction_out,
    int allow_null_placement, uint32_t *null_order_out,
    uint32_t max_order_items, uint32_t *order_count_out,
    prdb_sdk_sql_order_item_v12 *order_items_out) {
    parser state;
    prdb_sdk_select_v5 result = {0};
    int32_t status;

    if (error_offset_out != NULL) *error_offset_out = 0;
    if (has_having_out != NULL) *has_having_out = 0;
    if (having_comparison_out != NULL) *having_comparison_out = 0;
    if (having_parameter_index_out != NULL) *having_parameter_index_out = 0;
    if (has_order_by_out != NULL) *has_order_by_out = 0;
    if (order_kind_out != NULL) *order_kind_out = 0;
    if (order_direction_out != NULL) *order_direction_out = 0;
    if (null_order_out != NULL) *null_order_out = 0;
    if (order_count_out != NULL) *order_count_out = 0;
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
    result.base.abi_version = PRDB_SDK_SQL_ABI_V2_VERSION;
    result.base.sql = sql;
    result.base.sql_len = sql_len;

    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) goto failed_v5;
    if (!token_is_keyword(&state, "SELECT")) {
        status = state.current.kind == TOKEN_IDENTIFIER
                     ? PRDB_SDK_SQL_UNSUPPORTED_STATEMENT
                     : PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_v5;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_v5;
    }
    status = expect_identifier(&state, &result.base.projection[0]);
    if (status != PRDB_SDK_SQL_OK) goto failed_v5;
    result.base.projection_count = 1;
    if (state.current.kind != TOKEN_COMMA) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_v5;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_v5;
    }
    if (token_is_keyword(&state, "COUNT")) {
        result.aggregate_kind = PRDB_SDK_SQL_V5_AGGREGATE_COUNT_STAR;
    } else if (token_is_keyword(&state, "MIN")) {
        result.aggregate_kind = PRDB_SDK_SQL_V5_AGGREGATE_MIN;
    } else if (token_is_keyword(&state, "MAX")) {
        result.aggregate_kind = PRDB_SDK_SQL_V5_AGGREGATE_MAX;
    } else if (allow_sum && token_is_keyword(&state, "SUM")) {
        result.aggregate_kind = PRDB_SDK_SQL_V6_AGGREGATE_SUM;
    } else if (allow_avg && token_is_keyword(&state, "AVG")) {
        result.aggregate_kind = PRDB_SDK_SQL_V7_AGGREGATE_AVG;
    } else {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_v5;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK ||
        state.current.kind != TOKEN_LEFT_PAREN) {
        status = state.error == PRDB_SDK_SQL_OK
                     ? PRDB_SDK_SQL_SYNTAX_ERROR
                     : state.error;
        if (state.error == PRDB_SDK_SQL_OK) {
            state.error_offset = state.current.offset;
        }
        goto failed_v5;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_v5;
    }
    if (result.aggregate_kind == PRDB_SDK_SQL_V5_AGGREGATE_COUNT_STAR) {
        if (state.current.kind != TOKEN_STAR) {
            status = PRDB_SDK_SQL_SYNTAX_ERROR;
            state.error_offset = state.current.offset;
            goto failed_v5;
        }
        if (advance(&state) != PRDB_SDK_SQL_OK) {
            status = state.error;
            goto failed_v5;
        }
    } else {
        status = expect_identifier(&state, &result.aggregate_column);
        if (status != PRDB_SDK_SQL_OK) goto failed_v5;
    }
    if (state.current.kind != TOKEN_RIGHT_PAREN) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_v5;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_v5;
    }
    if (!token_is_keyword(&state, "FROM")) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_v5;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_v5;
    }
    status = expect_identifier(&state, &result.base.relation);
    if (status != PRDB_SDK_SQL_OK) goto failed_v5;
    if (token_is_keyword(&state, "WHERE")) {
        if (advance(&state) != PRDB_SDK_SQL_OK) {
            status = state.error;
            goto failed_v5;
        }
        status = prdb_sdk_internal_parse_predicate_or_v2(
            &state, &result.base, 0, &result.base.predicate_root);
        if (status != PRDB_SDK_SQL_OK) goto failed_v5;
    }
    if (!token_is_keyword(&state, "GROUP")) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_v5;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK ||
        !token_is_keyword(&state, "BY")) {
        status = state.error == PRDB_SDK_SQL_OK
                     ? PRDB_SDK_SQL_SYNTAX_ERROR
                     : state.error;
        if (state.error == PRDB_SDK_SQL_OK) {
            state.error_offset = state.current.offset;
        }
        goto failed_v5;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_v5;
    }
    status = expect_identifier(&state, &result.group_by_column);
    if (status != PRDB_SDK_SQL_OK) goto failed_v5;
    if (!prdb_sdk_internal_span_matches_name(
            &result.base, result.group_by_column,
            sql + result.base.projection[0].offset,
            result.base.projection[0].length)) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = result.group_by_column.offset;
        goto failed_v5;
    }
    result.has_group_by = 1;
    if (token_is_keyword(&state, "HAVING")) {
        uint32_t comparison;
        uint32_t parameter_index;
        prdb_sdk_sql_span_v1 having_column = {0};
        int aggregate_matches = 0;

        if (!allow_having) {
            status = PRDB_SDK_SQL_SYNTAX_ERROR;
            state.error_offset = state.current.offset;
            goto failed_v5;
        }
        if (advance(&state) != PRDB_SDK_SQL_OK) {
            status = state.error;
            goto failed_v5;
        }
        if (state.current.kind == TOKEN_IDENTIFIER) {
            aggregate_matches =
                (result.aggregate_kind ==
                     PRDB_SDK_SQL_V5_AGGREGATE_COUNT_STAR &&
                 token_is_keyword(&state, "COUNT")) ||
                (result.aggregate_kind == PRDB_SDK_SQL_V5_AGGREGATE_MIN &&
                 token_is_keyword(&state, "MIN")) ||
                (result.aggregate_kind == PRDB_SDK_SQL_V5_AGGREGATE_MAX &&
                 token_is_keyword(&state, "MAX")) ||
                (result.aggregate_kind == PRDB_SDK_SQL_V6_AGGREGATE_SUM &&
                 token_is_keyword(&state, "SUM")) ||
                (result.aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_AVG &&
                 token_is_keyword(&state, "AVG"));
        }
        if (!aggregate_matches) {
            status = PRDB_SDK_SQL_SYNTAX_ERROR;
            state.error_offset = state.current.offset;
            goto failed_v5;
        }
        if (advance(&state) != PRDB_SDK_SQL_OK ||
            state.current.kind != TOKEN_LEFT_PAREN) {
            status = state.error == PRDB_SDK_SQL_OK
                         ? PRDB_SDK_SQL_SYNTAX_ERROR
                         : state.error;
            if (state.error == PRDB_SDK_SQL_OK) {
                state.error_offset = state.current.offset;
            }
            goto failed_v5;
        }
        if (advance(&state) != PRDB_SDK_SQL_OK) {
            status = state.error;
            goto failed_v5;
        }
        if (result.aggregate_kind ==
            PRDB_SDK_SQL_V5_AGGREGATE_COUNT_STAR) {
            if (state.current.kind != TOKEN_STAR) {
                status = PRDB_SDK_SQL_SYNTAX_ERROR;
                state.error_offset = state.current.offset;
                goto failed_v5;
            }
            if (advance(&state) != PRDB_SDK_SQL_OK) {
                status = state.error;
                goto failed_v5;
            }
        } else {
            status = expect_identifier(&state, &having_column);
            if (status != PRDB_SDK_SQL_OK) goto failed_v5;
            if (!prdb_sdk_internal_span_matches_name(
                    &result.base, having_column,
                    sql + result.aggregate_column.offset,
                    result.aggregate_column.length)) {
                status = PRDB_SDK_SQL_SYNTAX_ERROR;
                state.error_offset = having_column.offset;
                goto failed_v5;
            }
        }
        if (state.current.kind != TOKEN_RIGHT_PAREN) {
            status = PRDB_SDK_SQL_SYNTAX_ERROR;
            state.error_offset = state.current.offset;
            goto failed_v5;
        }
        if (advance(&state) != PRDB_SDK_SQL_OK) {
            status = state.error;
            goto failed_v5;
        }
        switch (state.current.kind) {
        case TOKEN_EQUAL:
            comparison = PRDB_SDK_SQL_COMPARE_EQUAL;
            break;
        case TOKEN_NOT_EQUAL:
            comparison = PRDB_SDK_SQL_COMPARE_NOT_EQUAL;
            break;
        case TOKEN_LESS:
            comparison = PRDB_SDK_SQL_COMPARE_LESS;
            break;
        case TOKEN_LESS_EQUAL:
            comparison = PRDB_SDK_SQL_COMPARE_LESS_EQUAL;
            break;
        case TOKEN_GREATER:
            comparison = PRDB_SDK_SQL_COMPARE_GREATER;
            break;
        case TOKEN_GREATER_EQUAL:
            comparison = PRDB_SDK_SQL_COMPARE_GREATER_EQUAL;
            break;
        default:
            status = PRDB_SDK_SQL_SYNTAX_ERROR;
            state.error_offset = state.current.offset;
            goto failed_v5;
        }
        if (advance(&state) != PRDB_SDK_SQL_OK ||
            state.current.kind != TOKEN_PARAMETER) {
            status = state.error == PRDB_SDK_SQL_OK
                         ? PRDB_SDK_SQL_SYNTAX_ERROR
                         : state.error;
            if (state.error == PRDB_SDK_SQL_OK) {
                state.error_offset = state.current.offset;
            }
            goto failed_v5;
        }
        status = parse_unsigned(&state, state.current,
                                PRDB_SDK_SQL_MAX_PARAMETER_INDEX, 0,
                                &parameter_index);
        if (status != PRDB_SDK_SQL_OK) goto failed_v5;
        if (advance(&state) != PRDB_SDK_SQL_OK) {
            status = state.error;
            goto failed_v5;
        }
        if (has_having_out != NULL) *has_having_out = 1;
        if (having_comparison_out != NULL) {
            *having_comparison_out = comparison;
        }
        if (having_parameter_index_out != NULL) {
            *having_parameter_index_out = parameter_index;
        }
    }
    if (token_is_keyword(&state, "ORDER")) {
        uint32_t order_count = 0;

        if (!allow_group_order) {
            status = PRDB_SDK_SQL_SYNTAX_ERROR;
            state.error_offset = state.current.offset;
            goto failed_v5;
        }
        if (advance(&state) != PRDB_SDK_SQL_OK ||
            !token_is_keyword(&state, "BY")) {
            status = state.error == PRDB_SDK_SQL_OK
                         ? PRDB_SDK_SQL_SYNTAX_ERROR
                         : state.error;
            if (state.error == PRDB_SDK_SQL_OK) {
                state.error_offset = state.current.offset;
            }
            goto failed_v5;
        }
        if (advance(&state) != PRDB_SDK_SQL_OK) {
            status = state.error;
            goto failed_v5;
        }
        for (;;) {
            prdb_sdk_sql_order_item_v12 item = {0};
            uint32_t item_offset = state.current.offset;
            status = parse_group_order_item(&state, &result,
                                            allow_null_placement, &item);
            if (status != PRDB_SDK_SQL_OK) goto failed_v5;
            if (order_count != 0 && order_items_out != NULL &&
                order_items_out[0].kind == item.kind) {
                status = PRDB_SDK_SQL_SYNTAX_ERROR;
                state.error_offset = item_offset;
                goto failed_v5;
            }
            if (order_count >= max_order_items) {
                status = PRDB_SDK_SQL_SYNTAX_ERROR;
                state.error_offset = state.current.offset;
                goto failed_v5;
            }
            if (order_items_out != NULL) {
                order_items_out[order_count] = item;
            }
            if (order_count == 0) {
                if (order_kind_out != NULL) *order_kind_out = item.kind;
                if (order_direction_out != NULL) {
                    *order_direction_out = item.direction;
                }
                if (null_order_out != NULL) {
                    *null_order_out = item.null_order;
                }
            }
            ++order_count;
            if (state.current.kind != TOKEN_COMMA) break;
            if (order_count >= max_order_items) {
                status = PRDB_SDK_SQL_SYNTAX_ERROR;
                state.error_offset = state.current.offset;
                goto failed_v5;
            }
            if (advance(&state) != PRDB_SDK_SQL_OK) {
                status = state.error;
                goto failed_v5;
            }
        }
        if (has_order_by_out != NULL) *has_order_by_out = 1;
        if (order_count_out != NULL) *order_count_out = order_count;
    }
    if (token_is_keyword(&state, "LIMIT")) {
        token limit_token;
        result.base.has_limit = 1;
        if (advance(&state) != PRDB_SDK_SQL_OK) {
            status = state.error;
            goto failed_v5;
        }
        limit_token = state.current;
        status = parse_unsigned(&state, limit_token, PRDB_SDK_SQL_MAX_LIMIT,
                                1, &result.base.limit);
        if (status != PRDB_SDK_SQL_OK) goto failed_v5;
        if (advance(&state) != PRDB_SDK_SQL_OK) {
            status = state.error;
            goto failed_v5;
        }
    }
    if (state.current.kind == TOKEN_SEMICOLON &&
        advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_v5;
    }
    if (state.current.kind != TOKEN_END) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_v5;
    }
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V5_VERSION;
    *out = result;
    return PRDB_SDK_SQL_OK;

failed_v5:
    if (error_offset_out != NULL) *error_offset_out = state.error_offset;
    return fail_at(&state, status);
}

int32_t prdb_sdk_sql_parse_select_v5(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_select_v5 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out) {
    return parse_grouped_select_v5_internal(sql, sql_len, out, out_capacity,
                                            error_offset_out, 0, 0, 0, 0,
                                            NULL, NULL, NULL, NULL, NULL,
                                            NULL, 0, NULL, 1, NULL, NULL);
}

int32_t prdb_sdk_sql_parse_select_v6(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_select_v6 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out) {
    prdb_sdk_select_v5 parsed = {0};
    prdb_sdk_select_v6 result = {0};
    int32_t status;

    if (out == NULL || out_capacity < sizeof(*out)) {
        if (error_offset_out != NULL) *error_offset_out = 0;
        return PRDB_SDK_SQL_INVALID_ARGUMENT;
    }
    status = parse_grouped_select_v5_internal(
        sql, sql_len, &parsed, sizeof(parsed), error_offset_out, 1, 0, 0, 0,
        NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, 1, NULL, NULL);
    if (status != PRDB_SDK_SQL_OK) return status;
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V6_VERSION;
    result.base = parsed.base;
    result.has_group_by = parsed.has_group_by;
    result.group_by_column = parsed.group_by_column;
    result.aggregate_kind = parsed.aggregate_kind;
    result.aggregate_column = parsed.aggregate_column;
    *out = result;
    return PRDB_SDK_SQL_OK;
}

int32_t prdb_sdk_sql_bind_select_v6(
    const prdb_sdk_select_v6 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v6 *out, uint32_t out_capacity) {
    prdb_sdk_select_v5 compatible_statement = {0};
    prdb_sdk_select_binding_v5 compatible_binding = {0};
    prdb_sdk_select_binding_v6 result = {0};
    uint32_t aggregate_kind;
    int32_t status;

    if (statement == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_BIND_INVALID_ARGUMENT;
    }
    if (statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V6_VERSION ||
        (statement->aggregate_kind !=
             PRDB_SDK_SQL_V6_AGGREGATE_COUNT_STAR &&
         statement->aggregate_kind != PRDB_SDK_SQL_V6_AGGREGATE_MIN &&
         statement->aggregate_kind != PRDB_SDK_SQL_V6_AGGREGATE_MAX &&
         statement->aggregate_kind != PRDB_SDK_SQL_V6_AGGREGATE_SUM)) {
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    }
    aggregate_kind = statement->aggregate_kind;
    compatible_statement.struct_size = sizeof(compatible_statement);
    compatible_statement.abi_version = PRDB_SDK_SQL_ABI_V5_VERSION;
    compatible_statement.base = statement->base;
    compatible_statement.has_group_by = statement->has_group_by;
    compatible_statement.group_by_column = statement->group_by_column;
    compatible_statement.aggregate_kind =
        aggregate_kind == PRDB_SDK_SQL_V6_AGGREGATE_SUM
            ? PRDB_SDK_SQL_V5_AGGREGATE_MAX
            : aggregate_kind;
    compatible_statement.aggregate_column = statement->aggregate_column;
    status = prdb_sdk_sql_bind_select_v5(
        &compatible_statement, relation_name, relation_name_len, columns,
        column_count, &compatible_binding, sizeof(compatible_binding));
    if (status != PRDB_SDK_SQL_BIND_OK) return status;
    if (aggregate_kind == PRDB_SDK_SQL_V6_AGGREGATE_SUM &&
        compatible_binding.aggregate_column.type_code !=
            PRDB_SDK_VALUE_TYPE_SMALLINT &&
        compatible_binding.aggregate_column.type_code !=
            PRDB_SDK_VALUE_TYPE_INTEGER &&
        compatible_binding.aggregate_column.type_code !=
            PRDB_SDK_VALUE_TYPE_BIGINT &&
        compatible_binding.aggregate_column.type_code !=
            PRDB_SDK_VALUE_TYPE_DECIMAL) {
        return PRDB_SDK_SQL_BIND_V6_INVALID_AGGREGATE_TYPE;
    }
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V6_VERSION;
    result.base = compatible_binding.base;
    result.has_group_by = compatible_binding.has_group_by;
    result.group_by_column = compatible_binding.group_by_column;
    result.aggregate_kind = aggregate_kind;
    result.aggregate_column = compatible_binding.aggregate_column;
    *out = result;
    return PRDB_SDK_SQL_BIND_OK;
}

int32_t prdb_sdk_sql_parse_select_v7(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_select_v7 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out) {
    prdb_sdk_select_v5 parsed = {0};
    prdb_sdk_select_v7 result = {0};
    int32_t status;

    if (out == NULL || out_capacity < sizeof(*out)) {
        if (error_offset_out != NULL) *error_offset_out = 0;
        return PRDB_SDK_SQL_INVALID_ARGUMENT;
    }
    status = parse_grouped_select_v5_internal(
        sql, sql_len, &parsed, sizeof(parsed), error_offset_out, 1, 1, 0, 0,
        NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, 1, NULL, NULL);
    if (status != PRDB_SDK_SQL_OK) return status;
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V7_VERSION;
    result.base = parsed.base;
    result.has_group_by = parsed.has_group_by;
    result.group_by_column = parsed.group_by_column;
    result.aggregate_kind = parsed.aggregate_kind;
    result.aggregate_column = parsed.aggregate_column;
    *out = result;
    return PRDB_SDK_SQL_OK;
}

int32_t prdb_sdk_sql_bind_select_v7(
    const prdb_sdk_select_v7 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v7 *out, uint32_t out_capacity) {
    prdb_sdk_select_v6 compatible_statement = {0};
    prdb_sdk_select_binding_v6 compatible_binding = {0};
    prdb_sdk_select_binding_v7 result = {0};
    uint32_t aggregate_kind;
    int32_t status;

    if (statement == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_BIND_INVALID_ARGUMENT;
    }
    if (statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V7_VERSION ||
        (statement->aggregate_kind < PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR ||
         statement->aggregate_kind > PRDB_SDK_SQL_V7_AGGREGATE_AVG)) {
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    }
    aggregate_kind = statement->aggregate_kind;
    compatible_statement.struct_size = sizeof(compatible_statement);
    compatible_statement.abi_version = PRDB_SDK_SQL_ABI_V6_VERSION;
    compatible_statement.base = statement->base;
    compatible_statement.has_group_by = statement->has_group_by;
    compatible_statement.group_by_column = statement->group_by_column;
    compatible_statement.aggregate_kind =
        aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_AVG
            ? PRDB_SDK_SQL_V6_AGGREGATE_SUM
            : aggregate_kind;
    compatible_statement.aggregate_column = statement->aggregate_column;
    status = prdb_sdk_sql_bind_select_v6(
        &compatible_statement, relation_name, relation_name_len, columns,
        column_count, &compatible_binding, sizeof(compatible_binding));
    if (status == PRDB_SDK_SQL_BIND_V6_INVALID_AGGREGATE_TYPE) {
        return PRDB_SDK_SQL_BIND_V7_INVALID_AGGREGATE_TYPE;
    }
    if (status != PRDB_SDK_SQL_BIND_OK) return status;
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V7_VERSION;
    result.base = compatible_binding.base;
    result.has_group_by = compatible_binding.has_group_by;
    result.group_by_column = compatible_binding.group_by_column;
    result.aggregate_kind = aggregate_kind;
    result.aggregate_column = compatible_binding.aggregate_column;
    *out = result;
    return PRDB_SDK_SQL_BIND_OK;
}

int32_t prdb_sdk_sql_parse_select_v9(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_select_v9 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out) {
    prdb_sdk_select_v5 parsed = {0};
    prdb_sdk_select_v9 result = {0};
    uint32_t has_having = 0, having_comparison = 0;
    uint32_t having_parameter_index = 0;
    int32_t status;

    if (error_offset_out != NULL) *error_offset_out = 0;
    if (sql == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_INVALID_ARGUMENT;
    }
    status = parse_grouped_select_v5_internal(
        sql, sql_len, &parsed, sizeof(parsed), error_offset_out, 1, 1, 1, 0,
        &has_having, &having_comparison, &having_parameter_index, NULL, NULL,
        NULL, 0, NULL, 1, NULL, NULL);
    if (status != PRDB_SDK_SQL_OK) return status;
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V9_VERSION;
    result.base.struct_size = sizeof(result.base);
    result.base.abi_version = PRDB_SDK_SQL_ABI_V7_VERSION;
    result.base.base = parsed.base;
    result.base.has_group_by = parsed.has_group_by;
    result.base.group_by_column = parsed.group_by_column;
    result.base.aggregate_kind = parsed.aggregate_kind;
    result.base.aggregate_column = parsed.aggregate_column;
    result.has_having = has_having;
    result.having_comparison = having_comparison;
    result.having_parameter_index = having_parameter_index;
    *out = result;
    return PRDB_SDK_SQL_OK;
}

int32_t prdb_sdk_sql_bind_select_v9(
    const prdb_sdk_select_v9 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v9 *out, uint32_t out_capacity) {
    prdb_sdk_select_binding_v9 result = {0};
    int32_t status;

    if (statement == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_BIND_INVALID_ARGUMENT;
    }
    if (statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V9_VERSION ||
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
    status = prdb_sdk_sql_bind_select_v7(
        &statement->base, relation_name, relation_name_len, columns,
        column_count, &result.base, sizeof(result.base));
    if (status != PRDB_SDK_SQL_BIND_OK) return status;
    result.has_having = statement->has_having;
    result.having_comparison = statement->having_comparison;
    result.having_parameter_index = statement->having_parameter_index;
    if (statement->has_having == 1) {
        const prdb_sdk_sql_bound_column_v2 *aggregate =
            &result.base.aggregate_column;
        uint32_t kind = result.base.aggregate_kind;

        if (kind == PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
            result.having_type_code = PRDB_SDK_VALUE_TYPE_BIGINT;
        } else if (kind == PRDB_SDK_SQL_V7_AGGREGATE_MIN ||
                   kind == PRDB_SDK_SQL_V7_AGGREGATE_MAX) {
            result.having_type_code = aggregate->type_code;
            result.having_decimal_precision = aggregate->decimal_precision;
            result.having_decimal_scale = aggregate->decimal_scale;
        } else if (kind == PRDB_SDK_SQL_V7_AGGREGATE_SUM) {
            if (aggregate->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL) {
                result.having_type_code = PRDB_SDK_VALUE_TYPE_DECIMAL;
                result.having_decimal_precision = 38;
                result.having_decimal_scale = aggregate->decimal_scale;
            } else {
                result.having_type_code = PRDB_SDK_VALUE_TYPE_BIGINT;
            }
        } else if (kind == PRDB_SDK_SQL_V7_AGGREGATE_AVG) {
            uint32_t integer_digits =
                aggregate->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL
                    ? aggregate->decimal_precision - aggregate->decimal_scale
                    : 0;
            uint32_t desired_scale =
                aggregate->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL
                    ? aggregate->decimal_scale + 6
                    : 6;
            uint32_t available_scale = 38 - integer_digits;
            if (desired_scale < 6) desired_scale = 6;
            if (desired_scale > available_scale) {
                desired_scale = available_scale;
            }
            result.having_type_code = PRDB_SDK_VALUE_TYPE_DECIMAL;
            result.having_decimal_precision = 38;
            result.having_decimal_scale = (uint8_t)desired_scale;
        } else {
            return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
        }
    }
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V9_VERSION;
    *out = result;
    return PRDB_SDK_SQL_BIND_OK;
}

int32_t prdb_sdk_sql_parse_select_v10(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v10 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out) {
    prdb_sdk_select_v5 parsed = {0};
    prdb_sdk_select_v10 result = {0};
    uint32_t has_having = 0, having_comparison = 0;
    uint32_t having_parameter_index = 0;
    uint32_t has_order_by = 0, order_kind = 0, order_direction = 0;
    int32_t status;

    if (error_offset_out != NULL) *error_offset_out = 0;
    if (sql == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_INVALID_ARGUMENT;
    }
    status = parse_grouped_select_v5_internal(
        sql, sql_len, &parsed, sizeof(parsed), error_offset_out, 1, 1, 1, 1,
        &has_having, &having_comparison, &having_parameter_index,
        &has_order_by, &order_kind, &order_direction, 0, NULL, 1, NULL, NULL);
    if (status != PRDB_SDK_SQL_OK) return status;
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V10_VERSION;
    result.base.struct_size = sizeof(result.base);
    result.base.abi_version = PRDB_SDK_SQL_ABI_V9_VERSION;
    result.base.base.struct_size = sizeof(result.base.base);
    result.base.base.abi_version = PRDB_SDK_SQL_ABI_V7_VERSION;
    result.base.base.base = parsed.base;
    result.base.base.has_group_by = parsed.has_group_by;
    result.base.base.group_by_column = parsed.group_by_column;
    result.base.base.aggregate_kind = parsed.aggregate_kind;
    result.base.base.aggregate_column = parsed.aggregate_column;
    result.base.has_having = has_having;
    result.base.having_comparison = having_comparison;
    result.base.having_parameter_index = having_parameter_index;
    result.has_order_by = has_order_by;
    result.order_kind = order_kind;
    result.order_direction = order_direction;
    *out = result;
    return PRDB_SDK_SQL_OK;
}

int32_t prdb_sdk_sql_bind_select_v10(
    const prdb_sdk_select_v10 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v10 *out, uint32_t out_capacity) {
    prdb_sdk_select_binding_v10 result = {0};
    int32_t status;

    if (statement == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_BIND_INVALID_ARGUMENT;
    }
    if (statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V10_VERSION ||
        statement->has_order_by > 1 ||
        (statement->has_order_by == 0 &&
         (statement->order_kind != 0 || statement->order_direction != 0)) ||
        (statement->has_order_by == 1 &&
         (statement->order_kind < PRDB_SDK_SQL_V10_ORDER_GROUP_KEY ||
          statement->order_kind > PRDB_SDK_SQL_V10_ORDER_AGGREGATE ||
          statement->order_direction >
              PRDB_SDK_SQL_V10_ORDER_DESCENDING))) {
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    }
    status = prdb_sdk_sql_bind_select_v9(
        &statement->base, relation_name, relation_name_len, columns,
        column_count, &result.base, sizeof(result.base));
    if (status != PRDB_SDK_SQL_BIND_OK) return status;
    result.has_order_by = statement->has_order_by;
    result.order_kind = statement->order_kind;
    result.order_direction = statement->order_direction;
    if (statement->has_order_by) {
        const prdb_sdk_sql_bound_column_v2 *aggregate =
            &result.base.base.aggregate_column;
        uint32_t kind = result.base.base.aggregate_kind;
        if (statement->order_kind == PRDB_SDK_SQL_V10_ORDER_GROUP_KEY) {
            result.order_type_code =
                result.base.base.group_by_column.type_code;
            result.order_decimal_precision =
                result.base.base.group_by_column.decimal_precision;
            result.order_decimal_scale =
                result.base.base.group_by_column.decimal_scale;
        } else if (kind == PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
            result.order_type_code = PRDB_SDK_VALUE_TYPE_BIGINT;
        } else if (kind == PRDB_SDK_SQL_V7_AGGREGATE_MIN ||
                   kind == PRDB_SDK_SQL_V7_AGGREGATE_MAX) {
            result.order_type_code = aggregate->type_code;
            result.order_decimal_precision = aggregate->decimal_precision;
            result.order_decimal_scale = aggregate->decimal_scale;
        } else if (kind == PRDB_SDK_SQL_V7_AGGREGATE_SUM) {
            if (aggregate->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL) {
                result.order_type_code = PRDB_SDK_VALUE_TYPE_DECIMAL;
                result.order_decimal_precision = 38;
                result.order_decimal_scale = aggregate->decimal_scale;
            } else {
                result.order_type_code = PRDB_SDK_VALUE_TYPE_BIGINT;
            }
        } else if (kind == PRDB_SDK_SQL_V7_AGGREGATE_AVG) {
            uint32_t integer_digits =
                aggregate->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL
                    ? aggregate->decimal_precision - aggregate->decimal_scale
                    : 0;
            uint32_t desired_scale =
                aggregate->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL
                    ? aggregate->decimal_scale + 6
                    : 6;
            uint32_t available_scale = 38 - integer_digits;
            if (desired_scale < 6) desired_scale = 6;
            if (desired_scale > available_scale) {
                desired_scale = available_scale;
            }
            result.order_type_code = PRDB_SDK_VALUE_TYPE_DECIMAL;
            result.order_decimal_precision = 38;
            result.order_decimal_scale = (uint8_t)desired_scale;
        } else {
            return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
        }
    }
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V10_VERSION;
    *out = result;
    return PRDB_SDK_SQL_BIND_OK;
}

int32_t prdb_sdk_sql_parse_select_v11(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v11 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out) {
    prdb_sdk_select_v5 parsed = {0};
    prdb_sdk_select_v11 result = {0};
    uint32_t has_having = 0, having_comparison = 0;
    uint32_t having_parameter_index = 0, has_order_by = 0;
    uint32_t order_kind = 0, order_direction = 0, null_order = 0;
    int32_t status;

    if (error_offset_out != NULL) *error_offset_out = 0;
    if (sql == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_INVALID_ARGUMENT;
    }
    status = parse_grouped_select_v5_internal(
        sql, sql_len, &parsed, sizeof(parsed), error_offset_out, 1, 1, 1, 1,
        &has_having, &having_comparison, &having_parameter_index,
        &has_order_by, &order_kind, &order_direction, 1, &null_order, 1,
        NULL, NULL);
    if (status != PRDB_SDK_SQL_OK) return status;

    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V11_VERSION;
    result.base.struct_size = sizeof(result.base);
    result.base.abi_version = PRDB_SDK_SQL_ABI_V10_VERSION;
    result.base.base.struct_size = sizeof(result.base.base);
    result.base.base.abi_version = PRDB_SDK_SQL_ABI_V9_VERSION;
    result.base.base.base.struct_size = sizeof(result.base.base.base);
    result.base.base.base.abi_version = PRDB_SDK_SQL_ABI_V7_VERSION;
    result.base.base.base.base = parsed.base;
    result.base.base.base.has_group_by = parsed.has_group_by;
    result.base.base.base.group_by_column = parsed.group_by_column;
    result.base.base.base.aggregate_kind = parsed.aggregate_kind;
    result.base.base.base.aggregate_column = parsed.aggregate_column;
    result.base.base.has_having = has_having;
    result.base.base.having_comparison = having_comparison;
    result.base.base.having_parameter_index = having_parameter_index;
    result.base.has_order_by = has_order_by;
    result.base.order_kind = order_kind;
    result.base.order_direction = order_direction;
    result.null_order = null_order;
    *out = result;
    return PRDB_SDK_SQL_OK;
}

int32_t prdb_sdk_sql_bind_select_v11(
    const prdb_sdk_select_v11 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v11 *out, uint32_t out_capacity) {
    prdb_sdk_select_binding_v11 result = {0};
    int32_t status;

    if (statement == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_BIND_INVALID_ARGUMENT;
    }
    if (statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V11_VERSION ||
        statement->null_order > PRDB_SDK_SQL_V11_NULLS_LAST ||
        (statement->null_order != PRDB_SDK_SQL_V11_NULL_ORDER_DEFAULT &&
         statement->base.has_order_by == 0)) {
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    }
    status = prdb_sdk_sql_bind_select_v10(
        &statement->base, relation_name, relation_name_len, columns,
        column_count, &result.base, sizeof(result.base));
    if (status != PRDB_SDK_SQL_BIND_OK) return status;
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V11_VERSION;
    result.null_order = statement->null_order;
    *out = result;
    return PRDB_SDK_SQL_BIND_OK;
}

int32_t prdb_sdk_sql_parse_select_v12(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v12 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out) {
    prdb_sdk_select_v5 parsed = {0};
    prdb_sdk_select_v12 result = {0};
    uint32_t has_having = 0, having_comparison = 0;
    uint32_t having_parameter_index = 0, has_order_by = 0;
    uint32_t order_count = 0;
    prdb_sdk_sql_order_item_v12 order_items[PRDB_SDK_SQL_V12_MAX_ORDER_KEYS] =
        {{0}};
    int32_t status;

    if (error_offset_out != NULL) *error_offset_out = 0;
    if (sql == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_INVALID_ARGUMENT;
    }
    status = parse_grouped_select_v5_internal(
        sql, sql_len, &parsed, sizeof(parsed), error_offset_out, 1, 1, 1, 1,
        &has_having, &having_comparison, &having_parameter_index,
        &has_order_by, NULL, NULL, 1, NULL,
        PRDB_SDK_SQL_V12_MAX_ORDER_KEYS, &order_count, order_items);
    if (status != PRDB_SDK_SQL_OK) return status;
    if (order_count > PRDB_SDK_SQL_V12_MAX_ORDER_KEYS ||
        (has_order_by != (order_count != 0))) {
        if (error_offset_out != NULL) *error_offset_out = (uint32_t)sql_len;
        return PRDB_SDK_SQL_SYNTAX_ERROR;
    }

    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V12_VERSION;
    result.base.struct_size = sizeof(result.base);
    result.base.abi_version = PRDB_SDK_SQL_ABI_V11_VERSION;
    result.base.base.struct_size = sizeof(result.base.base);
    result.base.base.abi_version = PRDB_SDK_SQL_ABI_V10_VERSION;
    result.base.base.base.struct_size = sizeof(result.base.base.base);
    result.base.base.base.abi_version = PRDB_SDK_SQL_ABI_V9_VERSION;
    result.base.base.base.base.struct_size =
        sizeof(result.base.base.base.base);
    result.base.base.base.base.abi_version = PRDB_SDK_SQL_ABI_V7_VERSION;
    result.base.base.base.base.base = parsed.base;
    result.base.base.base.base.has_group_by = parsed.has_group_by;
    result.base.base.base.base.group_by_column = parsed.group_by_column;
    result.base.base.base.base.aggregate_kind = parsed.aggregate_kind;
    result.base.base.base.base.aggregate_column = parsed.aggregate_column;
    result.base.base.base.has_having = has_having;
    result.base.base.base.having_comparison = having_comparison;
    result.base.base.base.having_parameter_index = having_parameter_index;
    result.base.base.has_order_by = has_order_by;
    if (has_order_by) {
        result.base.base.order_kind = order_items[0].kind;
        result.base.base.order_direction = order_items[0].direction;
        result.base.null_order = order_items[0].null_order;
    }
    result.order_count = order_count;
    for (uint32_t index = 0; index < order_count; ++index) {
        result.order_items[index] = order_items[index];
    }
    *out = result;
    return PRDB_SDK_SQL_OK;
}

int32_t prdb_sdk_sql_bind_select_v12(
    const prdb_sdk_select_v12 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v12 *out, uint32_t out_capacity) {
    prdb_sdk_select_binding_v12 result = {0};
    prdb_sdk_select_binding_v11 first_binding = {0};
    int32_t status;

    if (statement == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_BIND_INVALID_ARGUMENT;
    }
    if (statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V12_VERSION ||
        statement->order_count > PRDB_SDK_SQL_V12_MAX_ORDER_KEYS ||
        statement->base.base.has_order_by > 1 ||
        (statement->order_count != 0) !=
            (statement->base.base.has_order_by != 0)) {
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    }
    if (statement->order_count == 0) {
        status = prdb_sdk_sql_bind_select_v11(
            &statement->base, relation_name, relation_name_len, columns,
            column_count, &first_binding, sizeof(first_binding));
        if (status != PRDB_SDK_SQL_BIND_OK) return status;
    }
    for (uint32_t index = 0; index < statement->order_count; ++index) {
        const prdb_sdk_sql_order_item_v12 *item =
            &statement->order_items[index];
        prdb_sdk_select_v11 item_statement = statement->base;
        prdb_sdk_select_binding_v11 item_binding = {0};
        if (item->kind < PRDB_SDK_SQL_V10_ORDER_GROUP_KEY ||
            item->kind > PRDB_SDK_SQL_V10_ORDER_AGGREGATE ||
            item->direction > PRDB_SDK_SQL_V10_ORDER_DESCENDING ||
            item->null_order > PRDB_SDK_SQL_V11_NULLS_LAST ||
            (index != 0 &&
             item->kind == statement->order_items[index - 1].kind)) {
            return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
        }
        item_statement.base.order_kind = item->kind;
        item_statement.base.order_direction = item->direction;
        item_statement.null_order = item->null_order;
        status = prdb_sdk_sql_bind_select_v11(
            &item_statement, relation_name, relation_name_len, columns,
            column_count, &item_binding, sizeof(item_binding));
        if (status != PRDB_SDK_SQL_BIND_OK) return status;
        if (index == 0) first_binding = item_binding;
        result.order_items[index].item = *item;
        result.order_items[index].type_code = item_binding.base.order_type_code;
        result.order_items[index].decimal_precision =
            item_binding.base.order_decimal_precision;
        result.order_items[index].decimal_scale =
            item_binding.base.order_decimal_scale;
    }
    for (uint32_t index = statement->order_count;
         index < PRDB_SDK_SQL_V12_MAX_ORDER_KEYS; ++index) {
        const prdb_sdk_sql_order_item_v12 zero = {0};
        if (memcmp(&statement->order_items[index], &zero, sizeof(zero)) != 0) {
            return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
        }
    }
    if (statement->order_count != 0 &&
        (statement->base.base.order_kind !=
             statement->order_items[0].kind ||
         statement->base.base.order_direction !=
             statement->order_items[0].direction ||
         statement->base.null_order != statement->order_items[0].null_order)) {
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    }
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V12_VERSION;
    result.base = first_binding;
    result.order_count = statement->order_count;
    *out = result;
    return PRDB_SDK_SQL_BIND_OK;
}
int32_t prdb_sdk_sql_bind_select_v5(
    const prdb_sdk_select_v5 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v5 *out, uint32_t out_capacity) {
    prdb_sdk_select_binding_v5 result = {0};
    uint32_t ordinal;
    int32_t status;

    if (statement == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_BIND_INVALID_ARGUMENT;
    }
    if (statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V5_VERSION ||
        statement->has_group_by != 1 ||
        (statement->aggregate_kind !=
             PRDB_SDK_SQL_V5_AGGREGATE_COUNT_STAR &&
         statement->aggregate_kind != PRDB_SDK_SQL_V5_AGGREGATE_MIN &&
         statement->aggregate_kind != PRDB_SDK_SQL_V5_AGGREGATE_MAX) ||
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
            statement->base.projection[0].length) ||
        (statement->aggregate_kind ==
                 PRDB_SDK_SQL_V5_AGGREGATE_COUNT_STAR
             ? statement->aggregate_column.length != 0
             : statement->aggregate_column.length == 0 ||
                   statement->aggregate_column.offset >
                       statement->base.sql_len ||
                   statement->aggregate_column.length >
                       statement->base.sql_len -
                           statement->aggregate_column.offset ||
                   !prdb_sdk_internal_valid_identifier_bytes(
                       statement->base.sql +
                           statement->aggregate_column.offset,
                       statement->aggregate_column.length))) {
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    }
    status = prdb_sdk_sql_bind_select_v2(
        &statement->base, relation_name, relation_name_len, columns,
        column_count, &result.base, sizeof(result.base));
    if (status != PRDB_SDK_SQL_BIND_OK) return status;
    ordinal = result.base.projection_ordinals[0];
    result.group_by_column.ordinal = ordinal;
    result.group_by_column.type_code = columns[ordinal].type_code;
    result.group_by_column.decimal_precision =
        columns[ordinal].decimal_precision;
    result.group_by_column.decimal_scale = columns[ordinal].decimal_scale;
    if (statement->aggregate_kind !=
        PRDB_SDK_SQL_V5_AGGREGATE_COUNT_STAR) {
        status = prdb_sdk_internal_find_column_shape_v2(
            &statement->base, statement->aggregate_column, columns,
            column_count, &ordinal);
        if (status != PRDB_SDK_SQL_BIND_OK) return status;
        result.aggregate_column.ordinal = ordinal;
        result.aggregate_column.type_code = columns[ordinal].type_code;
        result.aggregate_column.decimal_precision =
            columns[ordinal].decimal_precision;
        result.aggregate_column.decimal_scale = columns[ordinal].decimal_scale;
    }
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V5_VERSION;
    result.has_group_by = 1;
    result.aggregate_kind = statement->aggregate_kind;
    *out = result;
    return PRDB_SDK_SQL_BIND_OK;
}
