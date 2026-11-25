#ifndef PRDB_SDK_SQL_INTERNAL_H
#define PRDB_SDK_SQL_INTERNAL_H

#include "prdb_sdk_sql_v1.h"
#include "prdb_sdk_sql_v2.h"
#include "prdb_sdk_sql_v3.h"
#include "prdb_sdk_sql_v4.h"
#include "prdb_sdk_sql_v5.h"
#include "prdb_sdk_sql_v6.h"
#include "prdb_sdk_sql_v7.h"
#include "prdb_sdk_sql_v8.h"
#include "prdb_sdk_sql_v9.h"
#include "prdb_sdk_sql_v10.h"
#include "prdb_sdk_sql_v11.h"
#include "prdb_sdk_sql_v12.h"
#include "prdb_sdk_sql_v13.h"
#include "prdb_sdk_sql_v14.h"
#include "prdb_sdk_sql_v15.h"
#include "prdb_sdk_sql_v16.h"
#include "prdb_sdk_sql_v17.h"
#include "prdb_sdk_sql_v18.h"
#include "prdb_sdk_sql_v19.h"
#include "prdb_sdk_sql_v20.h"
#include "prdb_sdk_sql_v21.h"
#include "prdb_sdk_sql_v22.h"
#include "prdb_sdk_sql_v23.h"
#include "prdb_sdk_sql_v24.h"
#include "prdb_sdk_sql_v25.h"
#include "prdb_sdk_sql_v26.h"
#include "prdb_sdk_sql_dml_v1.h"
#include "prdb_sdk_sql_dml_v2.h"
#include "prdb_sdk_sql_dml_v3.h"

#include <stddef.h>
#include <string.h>


#ifndef PRDB_SDK_INTERNAL
#define PRDB_SDK_INTERNAL __attribute__((visibility("hidden")))
#endif

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
    TOKEN_GREATER_EQUAL,
    TOKEN_MINUS,
    TOKEN_PLUS,
    TOKEN_LEFT_PAREN,
    TOKEN_RIGHT_PAREN,
    TOKEN_DOT
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
    int having_features;
} parser;

#define HAVING_HIDDEN_COUNT 1
#define HAVING_NULL_TESTS 2
#define GROUP_SINGLE_KEY 4
#define PARSER_ALLOW_IN 8

static inline int is_space(uint8_t byte) {
    return byte == ' ' || byte == '\t' || byte == '\r' || byte == '\n';
}

static inline int is_identifier_start(uint8_t byte) {
    return (byte >= 'A' && byte <= 'Z') ||
           (byte >= 'a' && byte <= 'z') || byte == '_';
}

static inline int is_identifier_continue(uint8_t byte) {
    return is_identifier_start(byte) || (byte >= '0' && byte <= '9');
}

static inline int32_t next_token(parser *state) {
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
        if (state->position < state->length &&
            state->sql[state->position] == '.') {
            ++state->position;
            while (state->position < state->length &&
                   state->sql[state->position] >= '0' &&
                   state->sql[state->position] <= '9') {
                ++state->position;
            }
        }
        if (state->position < state->length &&
            (state->sql[state->position] == 'e' ||
             state->sql[state->position] == 'E')) {
            uint32_t exponent_offset = state->position++;
            if (state->position < state->length &&
                (state->sql[state->position] == '+' ||
                 state->sql[state->position] == '-')) {
                ++state->position;
            }
            uint32_t exponent_digits = state->position;
            while (state->position < state->length &&
                   state->sql[state->position] >= '0' &&
                   state->sql[state->position] <= '9') {
                ++state->position;
            }
            if (state->position == exponent_digits) {
                state->error_offset = exponent_offset;
                return PRDB_SDK_SQL_SYNTAX_ERROR;
            }
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
    case '(':
        state->current = (token){TOKEN_LEFT_PAREN, start, 1};
        return PRDB_SDK_SQL_OK;
    case ')':
        state->current = (token){TOKEN_RIGHT_PAREN, start, 1};
        return PRDB_SDK_SQL_OK;
    case '.':
        state->current = (token){TOKEN_DOT, start, 1};
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
    case '-':
        state->current = (token){TOKEN_MINUS, start, 1};
        return PRDB_SDK_SQL_OK;
    case '+':
        state->current = (token){TOKEN_PLUS, start, 1};
        return PRDB_SDK_SQL_OK;
    default:
        break;
    }
    state->error_offset = start;
    return PRDB_SDK_SQL_SYNTAX_ERROR;
}

static inline int token_is_keyword(const parser *state, const char *keyword) {
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

static inline int32_t advance(parser *state) {
    int32_t status = next_token(state);
    if (status != PRDB_SDK_SQL_OK) {
        state->error = status;
    }
    return status;
}

static inline int32_t parse_unsigned(parser *state, token token, uint32_t maximum,
                              int allow_zero, uint32_t *out) {
    uint32_t value = 0;
    if (token.kind != TOKEN_NUMBER && token.kind != TOKEN_PARAMETER) {
        state->error_offset = token.offset;
        return PRDB_SDK_SQL_SYNTAX_ERROR;
    }
    for (uint32_t index = 0; index < token.length; ++index) {
        uint8_t byte = state->sql[token.offset + index];
        uint32_t digit;
        if (byte < '0' || byte > '9') {
            state->error_offset = token.offset + index;
            return PRDB_SDK_SQL_SYNTAX_ERROR;
        }
        digit = (uint32_t)(byte - '0');
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

static inline int32_t expect_identifier(parser *state, prdb_sdk_sql_span_v1 *out) {
    if (state->current.kind != TOKEN_IDENTIFIER ||
        token_is_keyword(state, "SELECT") || token_is_keyword(state, "FROM") ||
        token_is_keyword(state, "WHERE") || token_is_keyword(state, "LIMIT") ||
        token_is_keyword(state, "IS") || token_is_keyword(state, "NOT") ||
        token_is_keyword(state, "NULL") || token_is_keyword(state, "AND") ||
        token_is_keyword(state, "OR") || token_is_keyword(state, "INSERT") ||
        token_is_keyword(state, "INTO") || token_is_keyword(state, "VALUES") ||
        token_is_keyword(state, "DEFAULT") || token_is_keyword(state, "UPDATE") ||
        token_is_keyword(state, "DELETE") || token_is_keyword(state, "SET")) {
        state->error_offset = state->current.offset;
        return PRDB_SDK_SQL_SYNTAX_ERROR;
    }
    out->offset = state->current.offset;
    out->length = state->current.length;
    return advance(state);
}

static inline int32_t comparison_code(token_kind kind, uint32_t *out) {
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

static inline int32_t fail_at(parser *state, int32_t error) {
    state->error = error;
    if (state->error_offset > state->length) {
        state->error_offset = state->length;
    }
    return error;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_spans_equal_ascii_case(const uint8_t *sql, prdb_sdk_sql_span_v1 left, prdb_sdk_sql_span_v1 right);
PRDB_SDK_INTERNAL int32_t prdb_sdk_internal_parse_predicate_or_v2(parser *state, prdb_sdk_select_v2 *statement, uint32_t depth, uint32_t *node_index_out);
PRDB_SDK_INTERNAL int prdb_sdk_internal_decimal_fits_precision(const uint8_t *coefficient, uint8_t precision);
PRDB_SDK_INTERNAL int32_t prdb_sdk_internal_validate_predicate_tree_v2(const prdb_sdk_select_v2 *statement);
PRDB_SDK_INTERNAL uint8_t prdb_sdk_internal_fold_identifier_byte(uint8_t byte);
PRDB_SDK_INTERNAL int prdb_sdk_internal_valid_identifier_bytes(const uint8_t *name, uint32_t name_len);
PRDB_SDK_INTERNAL int prdb_sdk_internal_span_matches_name(const prdb_sdk_select_v2 *statement, prdb_sdk_sql_span_v1 span, const uint8_t *name, uint32_t name_len);
PRDB_SDK_INTERNAL int32_t prdb_sdk_internal_validate_column_shape_v2(const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count);
PRDB_SDK_INTERNAL int32_t prdb_sdk_internal_find_column_shape_v2(const prdb_sdk_select_v2 *statement, prdb_sdk_sql_span_v1 span, const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count, uint32_t *index_out);
PRDB_SDK_INTERNAL int32_t prdb_sdk_internal_parse_select_v15_mode(const uint8_t *sql, uint64_t sql_len, prdb_sdk_select_v15 *out, uint32_t out_capacity, uint32_t *error_offset_out, uint32_t min_group_keys);
PRDB_SDK_INTERNAL int32_t prdb_sdk_internal_bind_select_v15_mode(const prdb_sdk_select_v15 *statement, const uint8_t *relation_name, uint32_t relation_name_len, const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count, prdb_sdk_select_binding_v15 *out, uint32_t out_capacity, uint32_t min_group_keys);

#endif
