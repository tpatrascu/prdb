#include "sql_internal.h"

int32_t prdb_sdk_sql_parse_insert_v1(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_insert_v1 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out) {
    parser state;
    prdb_sdk_insert_v1 result = {0};
    int32_t status;

    if (error_offset_out != NULL) *error_offset_out = 0;
    if (sql == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_INVALID_ARGUMENT;
    }
    if (sql_len == 0 || sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES) {
        if (sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES && error_offset_out != NULL) {
            *error_offset_out = (uint32_t)PRDB_SDK_SQL_MAX_QUERY_BYTES;
        }
        return sql_len == 0 ? PRDB_SDK_SQL_SYNTAX_ERROR
                            : PRDB_SDK_SQL_LIMIT_EXCEEDED;
    }
    state = (parser){sql, (uint32_t)sql_len, 0, {TOKEN_END, 0, 0}, 0,
                     PRDB_SDK_SQL_OK, 0};
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_DML_ABI_V1_VERSION;
    result.sql = sql;
    result.sql_len = sql_len;

    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) goto failed_insert;
    if (!token_is_keyword(&state, "INSERT")) {
        status = state.current.kind == TOKEN_IDENTIFIER
                     ? PRDB_SDK_SQL_UNSUPPORTED_STATEMENT
                     : PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_insert;
    }
    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) goto failed_insert;
    if (!token_is_keyword(&state, "INTO")) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_insert;
    }
    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) goto failed_insert;
    status = expect_identifier(&state, &result.relation);
    if (status != PRDB_SDK_SQL_OK) goto failed_insert;
    if (state.current.kind == TOKEN_LEFT_PAREN) {
        status = advance(&state);
        if (status != PRDB_SDK_SQL_OK) goto failed_insert;
        for (;;) {
            prdb_sdk_sql_span_v1 column = {0};
            if (result.column_count >= PRDB_SDK_SQL_MAX_INSERT_COLUMNS) {
                status = PRDB_SDK_SQL_LIMIT_EXCEEDED;
                state.error_offset = state.current.offset;
                goto failed_insert;
            }
            status = expect_identifier(&state, &column);
            if (status != PRDB_SDK_SQL_OK) goto failed_insert;
            for (uint32_t previous = 0; previous < result.column_count; ++previous) {
                if (prdb_sdk_internal_spans_equal_ascii_case(sql, result.columns[previous], column)) {
                    status = PRDB_SDK_SQL_SYNTAX_ERROR;
                    state.error_offset = column.offset;
                    goto failed_insert;
                }
            }
            result.columns[result.column_count++] = column;
            if (state.current.kind != TOKEN_COMMA) break;
            status = advance(&state);
            if (status != PRDB_SDK_SQL_OK) goto failed_insert;
        }
        if (state.current.kind != TOKEN_RIGHT_PAREN) {
            status = PRDB_SDK_SQL_SYNTAX_ERROR;
            state.error_offset = state.current.offset;
            goto failed_insert;
        }
        status = advance(&state);
        if (status != PRDB_SDK_SQL_OK) goto failed_insert;
    }
    if (!token_is_keyword(&state, "VALUES")) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_insert;
    }
    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) goto failed_insert;

    for (;;) {
        uint32_t row_start = result.value_count;
        if (result.row_count >= PRDB_SDK_SQL_MAX_INSERT_ROWS) {
            status = PRDB_SDK_SQL_LIMIT_EXCEEDED;
            state.error_offset = state.current.offset;
            goto failed_insert;
        }
        if (state.current.kind != TOKEN_LEFT_PAREN) {
            status = PRDB_SDK_SQL_SYNTAX_ERROR;
            state.error_offset = state.current.offset;
            goto failed_insert;
        }
        status = advance(&state);
        if (status != PRDB_SDK_SQL_OK) goto failed_insert;
        for (;;) {
            prdb_sdk_sql_insert_cell_v1 *cell;
            if (result.value_count >= PRDB_SDK_SQL_MAX_INSERT_VALUES) {
                status = PRDB_SDK_SQL_LIMIT_EXCEEDED;
                state.error_offset = state.current.offset;
                goto failed_insert;
            }
            cell = &result.values[result.value_count];
            if (state.current.kind == TOKEN_PARAMETER) {
                status = parse_unsigned(&state, state.current,
                                        PRDB_SDK_SQL_MAX_PARAMETER_INDEX, 0,
                                        &cell->parameter_index);
                if (status != PRDB_SDK_SQL_OK) goto failed_insert;
                cell->kind = PRDB_SDK_SQL_INSERT_PARAMETER;
            } else if (token_is_keyword(&state, "NULL")) {
                cell->kind = PRDB_SDK_SQL_INSERT_NULL;
            } else if (token_is_keyword(&state, "DEFAULT")) {
                cell->kind = PRDB_SDK_SQL_INSERT_DEFAULT;
            } else {
                status = PRDB_SDK_SQL_SYNTAX_ERROR;
                state.error_offset = state.current.offset;
                goto failed_insert;
            }
            ++result.value_count;
            status = advance(&state);
            if (status != PRDB_SDK_SQL_OK) goto failed_insert;
            if (state.current.kind != TOKEN_COMMA) break;
            status = advance(&state);
            if (status != PRDB_SDK_SQL_OK) goto failed_insert;
        }
        if (state.current.kind != TOKEN_RIGHT_PAREN) {
            status = PRDB_SDK_SQL_SYNTAX_ERROR;
            state.error_offset = state.current.offset;
            goto failed_insert;
        }
        uint32_t row_width = result.value_count - row_start;
        if (row_width == 0 ||
            (result.row_count != 0 && row_width != result.values_per_row)) {
            status = PRDB_SDK_SQL_SYNTAX_ERROR;
            state.error_offset = state.current.offset;
            goto failed_insert;
        }
        if (result.row_count == 0) result.values_per_row = row_width;
        ++result.row_count;
        status = advance(&state);
        if (status != PRDB_SDK_SQL_OK) goto failed_insert;
        if (state.current.kind != TOKEN_COMMA) break;
        status = advance(&state);
        if (status != PRDB_SDK_SQL_OK) goto failed_insert;
    }
    if (state.current.kind == TOKEN_SEMICOLON) {
        status = advance(&state);
        if (status != PRDB_SDK_SQL_OK) goto failed_insert;
    }
    if (state.current.kind != TOKEN_END) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_insert;
    }
    *out = result;
    return PRDB_SDK_SQL_OK;

failed_insert:
    if (error_offset_out != NULL) *error_offset_out = state.error_offset;
    return fail_at(&state, status);
}

int32_t prdb_sdk_sql_parse_update_v1(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_update_v1 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out) {
    parser state;
    prdb_sdk_update_v1 result;
    prdb_sdk_select_v1 *predicate;
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
    result = (prdb_sdk_update_v1){0};
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_DML_ABI_V1_VERSION;
    result.sql = sql;
    result.sql_len = sql_len;
    predicate = &result.predicate_statement;
    predicate->struct_size = sizeof(*predicate);
    predicate->abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
    predicate->sql = sql;
    predicate->sql_len = sql_len;

    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) goto failed_update;
    if (!token_is_keyword(&state, "UPDATE")) {
        status = state.current.kind == TOKEN_IDENTIFIER
                     ? PRDB_SDK_SQL_UNSUPPORTED_STATEMENT
                     : PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_update;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_update;
    }
    status = expect_identifier(&state, &result.relation);
    if (status != PRDB_SDK_SQL_OK) goto failed_update;
    predicate->relation = result.relation;
    if (!token_is_keyword(&state, "SET")) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_update;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_update;
    }

    for (;;) {
        prdb_sdk_sql_update_assignment_v1 *assignment;
        if (result.assignment_count >= PRDB_SDK_SQL_MAX_UPDATE_ASSIGNMENTS) {
            status = PRDB_SDK_SQL_LIMIT_EXCEEDED;
            state.error_offset = state.current.offset;
            goto failed_update;
        }
        assignment = &result.assignments[result.assignment_count];
        status = expect_identifier(&state, &assignment->column);
        if (status != PRDB_SDK_SQL_OK) goto failed_update;
        for (uint32_t previous = 0; previous < result.assignment_count;
             ++previous) {
            if (prdb_sdk_internal_spans_equal_ascii_case(sql, assignment->column,
                                       result.assignments[previous].column)) {
                status = PRDB_SDK_SQL_SYNTAX_ERROR;
                state.error_offset = assignment->column.offset;
                goto failed_update;
            }
        }
        if (state.current.kind != TOKEN_EQUAL) {
            status = PRDB_SDK_SQL_SYNTAX_ERROR;
            state.error_offset = state.current.offset;
            goto failed_update;
        }
        if (advance(&state) != PRDB_SDK_SQL_OK) {
            status = state.error;
            goto failed_update;
        }
        if (state.current.kind == TOKEN_PARAMETER) {
            assignment->kind = PRDB_SDK_SQL_UPDATE_PARAMETER;
            status = parse_unsigned(&state, state.current,
                                    PRDB_SDK_SQL_MAX_PARAMETER_INDEX, 0,
                                    &assignment->parameter_index);
            if (status != PRDB_SDK_SQL_OK) goto failed_update;
        } else if (token_is_keyword(&state, "NULL")) {
            assignment->kind = PRDB_SDK_SQL_UPDATE_NULL;
        } else {
            status = PRDB_SDK_SQL_SYNTAX_ERROR;
            state.error_offset = state.current.offset;
            goto failed_update;
        }
        predicate->projection[result.assignment_count] = assignment->column;
        ++result.assignment_count;
        ++predicate->projection_count;
        if (advance(&state) != PRDB_SDK_SQL_OK) {
            status = state.error;
            goto failed_update;
        }
        if (state.current.kind != TOKEN_COMMA) break;
        if (advance(&state) != PRDB_SDK_SQL_OK) {
            status = state.error;
            goto failed_update;
        }
    }
    if (!token_is_keyword(&state, "WHERE")) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_update;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_update;
    }
    status = prdb_sdk_internal_parse_predicate_or_v1(&state, predicate, 0,
                                   &predicate->predicate_root);
    if (status != PRDB_SDK_SQL_OK) goto failed_update;
    if (state.current.kind == TOKEN_SEMICOLON &&
        advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_update;
    }
    if (state.current.kind != TOKEN_END) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_update;
    }
    *out = result;
    return PRDB_SDK_SQL_OK;

failed_update:
    if (error_offset_out != NULL) *error_offset_out = state.error_offset;
    return fail_at(&state, status);
}

int32_t prdb_sdk_sql_parse_delete_v1(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_delete_v1 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out) {
    parser state;
    prdb_sdk_delete_v1 result;
    prdb_sdk_select_v1 *predicate;
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
    result = (prdb_sdk_delete_v1){0};
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_DML_ABI_V1_VERSION;
    result.sql = sql;
    result.sql_len = sql_len;
    predicate = &result.predicate_statement;
    predicate->struct_size = sizeof(*predicate);
    predicate->abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
    predicate->sql = sql;
    predicate->sql_len = sql_len;
    predicate->projection_star = 1;

    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) goto failed_delete;
    if (!token_is_keyword(&state, "DELETE")) {
        status = state.current.kind == TOKEN_IDENTIFIER
                     ? PRDB_SDK_SQL_UNSUPPORTED_STATEMENT
                     : PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_delete;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_delete;
    }
    if (!token_is_keyword(&state, "FROM")) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_delete;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_delete;
    }
    status = expect_identifier(&state, &result.relation);
    if (status != PRDB_SDK_SQL_OK) goto failed_delete;
    predicate->relation = result.relation;
    if (!token_is_keyword(&state, "WHERE")) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_delete;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_delete;
    }
    status = prdb_sdk_internal_parse_predicate_or_v1(&state, predicate, 0,
                                   &predicate->predicate_root);
    if (status != PRDB_SDK_SQL_OK) goto failed_delete;
    if (state.current.kind == TOKEN_SEMICOLON &&
        advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_delete;
    }
    if (state.current.kind != TOKEN_END) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_delete;
    }
    *out = result;
    return PRDB_SDK_SQL_OK;

failed_delete:
    if (error_offset_out != NULL) *error_offset_out = state.error_offset;
    return fail_at(&state, status);
}
