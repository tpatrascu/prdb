#include "prdb_sdk_sql_v1.h"

#include <stddef.h>
#include <string.h>

typedef enum token_kind {
    TOKEN_END,
    TOKEN_IDENTIFIER,
    TOKEN_NUMBER,
    TOKEN_PARAMETER,
    TOKEN_STAR,
    TOKEN_COMMA,
    TOKEN_SEMICOLON,
    TOKEN_EQUAL,
    TOKEN_NOT_EQUAL,
    TOKEN_LESS,
    TOKEN_LESS_EQUAL,
    TOKEN_GREATER,
    TOKEN_GREATER_EQUAL
} token_kind;

typedef struct token {
    token_kind kind;
    uint32_t offset;
    uint32_t length;
} token;

typedef struct parser {
    const uint8_t *sql;
    uint32_t length;
    uint32_t position;
    token current;
    uint32_t error_offset;
    int32_t error;
} parser;

static int is_space(uint8_t byte) {
    return byte == ' ' || byte == '\t' || byte == '\r' || byte == '\n';
}

static int is_identifier_start(uint8_t byte) {
    return (byte >= 'A' && byte <= 'Z') ||
           (byte >= 'a' && byte <= 'z') || byte == '_';
}

static int is_identifier_continue(uint8_t byte) {
    return is_identifier_start(byte) || (byte >= '0' && byte <= '9');
}

static int32_t next_token(parser *state) {
    uint32_t start;
    uint8_t byte;

    while (state->position < state->length &&
           is_space(state->sql[state->position])) {
        ++state->position;
    }
    start = state->position;
    if (start == state->length) {
        state->current = (token){TOKEN_END, start, 0};
        return PRDB_SDK_SQL_OK;
    }
    byte = state->sql[state->position++];
    if (is_identifier_start(byte)) {
        while (state->position < state->length &&
               is_identifier_continue(state->sql[state->position])) {
            ++state->position;
        }
        if (state->position - start > PRDB_SDK_SQL_MAX_IDENTIFIER_BYTES) {
            state->error_offset = start;
            return PRDB_SDK_SQL_LIMIT_EXCEEDED;
        }
        state->current = (token){TOKEN_IDENTIFIER, start,
                                 state->position - start};
        return PRDB_SDK_SQL_OK;
    }
    if (byte >= '0' && byte <= '9') {
        while (state->position < state->length &&
               state->sql[state->position] >= '0' &&
               state->sql[state->position] <= '9') {
            ++state->position;
        }
        state->current = (token){TOKEN_NUMBER, start,
                                 state->position - start};
        return PRDB_SDK_SQL_OK;
    }
    if (byte == '$') {
        uint32_t parameter_start = state->position;
        while (state->position < state->length &&
               state->sql[state->position] >= '0' &&
               state->sql[state->position] <= '9') {
            ++state->position;
        }
        if (state->position == parameter_start) {
            state->error_offset = start;
            return PRDB_SDK_SQL_SYNTAX_ERROR;
        }
        state->current = (token){TOKEN_PARAMETER, parameter_start,
                                 state->position - parameter_start};
        return PRDB_SDK_SQL_OK;
    }
    switch (byte) {
    case '*':
        state->current = (token){TOKEN_STAR, start, 1};
        return PRDB_SDK_SQL_OK;
    case ',':
        state->current = (token){TOKEN_COMMA, start, 1};
        return PRDB_SDK_SQL_OK;
    case ';':
        state->current = (token){TOKEN_SEMICOLON, start, 1};
        return PRDB_SDK_SQL_OK;
    case '=':
        state->current = (token){TOKEN_EQUAL, start, 1};
        return PRDB_SDK_SQL_OK;
    case '!':
        if (state->position < state->length &&
            state->sql[state->position] == '=') {
            ++state->position;
            state->current = (token){TOKEN_NOT_EQUAL, start, 2};
            return PRDB_SDK_SQL_OK;
        }
        break;
    case '<':
        if (state->position < state->length &&
            state->sql[state->position] == '=') {
            ++state->position;
            state->current = (token){TOKEN_LESS_EQUAL, start, 2};
        } else if (state->position < state->length &&
                   state->sql[state->position] == '>') {
            ++state->position;
            state->current = (token){TOKEN_NOT_EQUAL, start, 2};
        } else {
            state->current = (token){TOKEN_LESS, start, 1};
        }
        return PRDB_SDK_SQL_OK;
    case '>':
        if (state->position < state->length &&
            state->sql[state->position] == '=') {
            ++state->position;
            state->current = (token){TOKEN_GREATER_EQUAL, start, 2};
        } else {
            state->current = (token){TOKEN_GREATER, start, 1};
        }
        return PRDB_SDK_SQL_OK;
    default:
        break;
    }
    state->error_offset = start;
    return PRDB_SDK_SQL_SYNTAX_ERROR;
}

static int token_is_keyword(const parser *state, const char *keyword) {
    size_t length = strlen(keyword);
    if (state->current.kind != TOKEN_IDENTIFIER ||
        state->current.length != length) {
        return 0;
    }
    for (size_t index = 0; index < length; ++index) {
        uint8_t byte = state->sql[state->current.offset + index];
        if (byte >= 'a' && byte <= 'z') {
            byte = (uint8_t)(byte - 'a' + 'A');
        }
        if (byte != (uint8_t)keyword[index]) {
            return 0;
        }
    }
    return 1;
}

static int32_t advance(parser *state) {
    int32_t status = next_token(state);
    if (status != PRDB_SDK_SQL_OK) {
        state->error = status;
    }
    return status;
}

static int32_t parse_unsigned(parser *state, token token, uint32_t maximum,
                              int allow_zero, uint32_t *out) {
    uint32_t value = 0;
    if (token.kind != TOKEN_NUMBER && token.kind != TOKEN_PARAMETER) {
        state->error_offset = token.offset;
        return PRDB_SDK_SQL_SYNTAX_ERROR;
    }
    for (uint32_t index = 0; index < token.length; ++index) {
        uint32_t digit = (uint32_t)(state->sql[token.offset + index] - '0');
        if (value > (maximum - digit) / 10) {
            state->error_offset = token.offset;
            return PRDB_SDK_SQL_LIMIT_EXCEEDED;
        }
        value = value * 10 + digit;
    }
    if ((!allow_zero && value == 0) || value > maximum) {
        state->error_offset = token.offset;
        return value > maximum ? PRDB_SDK_SQL_LIMIT_EXCEEDED
                               : PRDB_SDK_SQL_SYNTAX_ERROR;
    }
    *out = value;
    return PRDB_SDK_SQL_OK;
}

static int32_t expect_identifier(parser *state, prdb_sdk_sql_span_v1 *out) {
    if (state->current.kind != TOKEN_IDENTIFIER ||
        token_is_keyword(state, "SELECT") || token_is_keyword(state, "FROM") ||
        token_is_keyword(state, "WHERE") || token_is_keyword(state, "LIMIT")) {
        state->error_offset = state->current.offset;
        return PRDB_SDK_SQL_SYNTAX_ERROR;
    }
    out->offset = state->current.offset;
    out->length = state->current.length;
    return advance(state);
}

static int32_t comparison_code(token_kind kind, uint32_t *out) {
    switch (kind) {
    case TOKEN_EQUAL:
        *out = PRDB_SDK_SQL_COMPARE_EQUAL;
        return PRDB_SDK_SQL_OK;
    case TOKEN_NOT_EQUAL:
        *out = PRDB_SDK_SQL_COMPARE_NOT_EQUAL;
        return PRDB_SDK_SQL_OK;
    case TOKEN_LESS:
        *out = PRDB_SDK_SQL_COMPARE_LESS;
        return PRDB_SDK_SQL_OK;
    case TOKEN_LESS_EQUAL:
        *out = PRDB_SDK_SQL_COMPARE_LESS_EQUAL;
        return PRDB_SDK_SQL_OK;
    case TOKEN_GREATER:
        *out = PRDB_SDK_SQL_COMPARE_GREATER;
        return PRDB_SDK_SQL_OK;
    case TOKEN_GREATER_EQUAL:
        *out = PRDB_SDK_SQL_COMPARE_GREATER_EQUAL;
        return PRDB_SDK_SQL_OK;
    default:
        return PRDB_SDK_SQL_SYNTAX_ERROR;
    }
}

static int32_t fail_at(parser *state, int32_t error) {
    state->error = error;
    if (state->error_offset > state->length) {
        state->error_offset = state->length;
    }
    return error;
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
                     PRDB_SDK_SQL_OK};
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
