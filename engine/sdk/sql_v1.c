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
#include "prdb_sdk_sql_dml_v1.h"

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
    TOKEN_GREATER_EQUAL,
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

int32_t prdb_sdk_sql_parse_insert_v1(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_insert_v1 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out) {
    parser state;
    prdb_sdk_insert_v1 result;
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
                     PRDB_SDK_SQL_OK};
    result = (prdb_sdk_insert_v1){0};
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
    if (advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_insert;
    }
    if (!token_is_keyword(&state, "INTO")) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_insert;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_insert;
    }
    status = expect_identifier(&state, &result.relation);
    if (status != PRDB_SDK_SQL_OK) goto failed_insert;
    if (!token_is_keyword(&state, "VALUES")) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_insert;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_insert;
    }
    if (state.current.kind != TOKEN_LEFT_PAREN) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_insert;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_insert;
    }
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
        if (advance(&state) != PRDB_SDK_SQL_OK) {
            status = state.error;
            goto failed_insert;
        }
        if (state.current.kind != TOKEN_COMMA) break;
        if (advance(&state) != PRDB_SDK_SQL_OK) {
            status = state.error;
            goto failed_insert;
        }
    }
    if (state.current.kind != TOKEN_RIGHT_PAREN) {
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        state.error_offset = state.current.offset;
        goto failed_insert;
    }
    if (advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_insert;
    }
    if (state.current.kind == TOKEN_SEMICOLON &&
        advance(&state) != PRDB_SDK_SQL_OK) {
        status = state.error;
        goto failed_insert;
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

static int spans_equal_ascii_case(const uint8_t *sql,
                                  prdb_sdk_sql_span_v1 left,
                                  prdb_sdk_sql_span_v1 right) {
    if (left.length != right.length) return 0;
    for (uint32_t index = 0; index < left.length; ++index) {
        uint8_t a = sql[left.offset + index];
        uint8_t b = sql[right.offset + index];
        if (a >= 'A' && a <= 'Z') a = (uint8_t)(a - 'A' + 'a');
        if (b >= 'A' && b <= 'Z') b = (uint8_t)(b - 'A' + 'a');
        if (a != b) return 0;
    }
    return 1;
}

static int32_t parse_predicate_or_v2(parser *state,
                                     prdb_sdk_select_v2 *statement,
                                     uint32_t depth, uint32_t *index_out);

int32_t prdb_sdk_sql_parse_update_v1(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_update_v1 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out) {
    parser state;
    prdb_sdk_update_v1 result;
    prdb_sdk_select_v2 *predicate;
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
                     PRDB_SDK_SQL_OK};
    result = (prdb_sdk_update_v1){0};
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_DML_ABI_V1_VERSION;
    result.sql = sql;
    result.sql_len = sql_len;
    predicate = &result.predicate_statement;
    predicate->struct_size = sizeof(*predicate);
    predicate->abi_version = PRDB_SDK_SQL_ABI_V2_VERSION;
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
            if (spans_equal_ascii_case(sql, assignment->column,
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
    status = parse_predicate_or_v2(&state, predicate, 0,
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
    prdb_sdk_select_v2 *predicate;
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
                     PRDB_SDK_SQL_OK};
    result = (prdb_sdk_delete_v1){0};
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_DML_ABI_V1_VERSION;
    result.sql = sql;
    result.sql_len = sql_len;
    predicate = &result.predicate_statement;
    predicate->struct_size = sizeof(*predicate);
    predicate->abi_version = PRDB_SDK_SQL_ABI_V2_VERSION;
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
    status = parse_predicate_or_v2(&state, predicate, 0,
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

static int32_t parse_predicate_or_v2(parser *state,
                                     prdb_sdk_select_v2 *statement,
                                     uint32_t depth, uint32_t *index_out);

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
        status = parse_predicate_or_v2(state, statement, depth + 1,
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

static int32_t parse_predicate_or_v2(parser *state,
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

int32_t prdb_sdk_sql_parse_select_v2(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_select_v2 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out) {
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
                     PRDB_SDK_SQL_OK};
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
        status = parse_predicate_or_v2(&state, &result, 0,
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

static int decimal_fits_precision(const uint8_t *coefficient,
                                  uint8_t precision) {
    uint8_t magnitude[16];
    uint8_t limit[16] = {0};
    int negative = (coefficient[0] & 0x80) != 0;
    uint16_t carry;

    if (negative) {
        carry = 1;
        for (size_t index = 16; index > 0; --index) {
            uint16_t value = (uint16_t)(uint8_t)~coefficient[index - 1] + carry;
            magnitude[index - 1] = (uint8_t)value;
            carry = value >> 8;
        }
    } else {
        memcpy(magnitude, coefficient, sizeof(magnitude));
    }

    limit[15] = 1;
    for (uint8_t digit = 0; digit < precision; ++digit) {
        carry = 0;
        for (size_t index = 16; index > 0; --index) {
            uint16_t value = (uint16_t)limit[index - 1] * 10 + carry;
            limit[index - 1] = (uint8_t)value;
            carry = value >> 8;
        }
        if (carry != 0) {
            return 0;
        }
    }
    carry = 1;
    for (size_t index = 16; index > 0 && carry != 0; --index) {
        if (limit[index - 1] == 0) {
            limit[index - 1] = 0xff;
        } else {
            --limit[index - 1];
            carry = 0;
        }
    }
    for (size_t index = 0; index < sizeof(magnitude); ++index) {
        if (magnitude[index] < limit[index]) {
            return 1;
        }
        if (magnitude[index] > limit[index]) {
            return 0;
        }
    }
    return 1;
}

int32_t prdb_sdk_sql_bind_parameter_v1(
    uint32_t parameter_index, const prdb_sdk_value_v1 *parameters,
    uint32_t parameter_count,
    uint32_t expected_type, uint8_t expected_decimal_precision,
    uint8_t expected_decimal_scale, prdb_sdk_value_v1 *out,
    uint32_t out_capacity) {
    const prdb_sdk_value_v1 *parameter;
    int32_t status;

    if (out == NULL || out_capacity < sizeof(*out) ||
        parameter_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        (parameter_count != 0 && parameters == NULL) ||
        parameter_index == 0 ||
        parameter_index > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        expected_type < PRDB_SDK_VALUE_TYPE_BOOLEAN ||
        expected_type > PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ ||
        (expected_type == PRDB_SDK_VALUE_TYPE_DECIMAL &&
         (expected_decimal_precision == 0 || expected_decimal_precision > 38 ||
          expected_decimal_scale > expected_decimal_precision)) ||
        (expected_type != PRDB_SDK_VALUE_TYPE_DECIMAL &&
         (expected_decimal_precision != 0 || expected_decimal_scale != 0))) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (parameter_index > parameter_count) {
        return PRDB_SDK_SQL_EVAL_PARAMETER_MISSING;
    }
    parameter = &parameters[parameter_index - 1];
    status = prdb_sdk_value_validate_v1(parameter);
    if (status == PRDB_SDK_VALUE_INVALID_ARGUMENT) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (status != PRDB_SDK_VALUE_VALID) {
        return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
    }
    if (parameter->state == PRDB_SDK_VALUE_STATE_REDACTED) {
        return PRDB_SDK_SQL_EVAL_REDACTED_INPUT;
    }
    if (parameter->state == PRDB_SDK_VALUE_STATE_VALUE &&
        parameter->type_code != expected_type) {
        return PRDB_SDK_SQL_EVAL_TYPE_MISMATCH;
    }
    if (parameter->state == PRDB_SDK_VALUE_STATE_VALUE &&
        expected_type == PRDB_SDK_VALUE_TYPE_DECIMAL &&
        !decimal_fits_precision(parameter->data,
                                expected_decimal_precision)) {
        return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
    }
    *out = *parameter;
    return PRDB_SDK_SQL_EVAL_OK;
}

int32_t prdb_sdk_sql_bind_select_parameter_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint32_t expected_type, uint8_t expected_decimal_precision,
    uint8_t expected_decimal_scale, prdb_sdk_value_v1 *out,
    uint32_t out_capacity) {
    if (statement == NULL || statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->has_where != 1 || statement->parameter_index == 0) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    return prdb_sdk_sql_bind_parameter_v1(
        statement->parameter_index, parameters, parameter_count, expected_type,
        expected_decimal_precision, expected_decimal_scale, out, out_capacity);
}

static int32_t validate_eval_value(const prdb_sdk_value_v1 *value) {
    int32_t status = prdb_sdk_value_validate_v1(value);
    if (status == PRDB_SDK_VALUE_INVALID_ARGUMENT) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (status != PRDB_SDK_VALUE_VALID) {
        return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
    }
    if (value->state == PRDB_SDK_VALUE_STATE_REDACTED) {
        return PRDB_SDK_SQL_EVAL_REDACTED_INPUT;
    }
    return PRDB_SDK_SQL_EVAL_OK;
}

static int compare_bytes(const uint8_t *left, uint64_t left_len,
                         const uint8_t *right, uint64_t right_len) {
    uint64_t common_len = left_len < right_len ? left_len : right_len;
    int result = common_len == 0 ? 0 : memcmp(left, right, (size_t)common_len);
    if (result != 0) {
        return result < 0 ? -1 : 1;
    }
    if (left_len == right_len) {
        return 0;
    }
    return left_len < right_len ? -1 : 1;
}

static int compare_signed_bytes(const uint8_t *left, const uint8_t *right,
                                size_t length) {
    int left_negative = (left[0] & 0x80) != 0;
    int right_negative = (right[0] & 0x80) != 0;
    if (left_negative != right_negative) {
        return left_negative ? -1 : 1;
    }
    return compare_bytes(left, length, right, length);
}

static int32_t compare_value_payloads(const prdb_sdk_value_v1 *left,
                                      const prdb_sdk_value_v1 *right,
                                      uint8_t left_decimal_scale,
                                      uint8_t right_decimal_scale,
                                      int *ordering) {
    switch (left->type_code) {
    case PRDB_SDK_VALUE_TYPE_BOOLEAN:
        *ordering = (left->data[0] > right->data[0]) -
                    (left->data[0] < right->data[0]);
        break;
    case PRDB_SDK_VALUE_TYPE_SMALLINT:
        *ordering = compare_signed_bytes(left->data, right->data, 2);
        break;
    case PRDB_SDK_VALUE_TYPE_INTEGER:
    case PRDB_SDK_VALUE_TYPE_DATE:
        *ordering = compare_signed_bytes(left->data, right->data, 4);
        break;
    case PRDB_SDK_VALUE_TYPE_BIGINT:
    case PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ:
        *ordering = compare_signed_bytes(left->data, right->data, 8);
        break;
    case PRDB_SDK_VALUE_TYPE_DECIMAL:
        if (left_decimal_scale > 38 || right_decimal_scale > 38) {
            return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
        }
        if (left_decimal_scale != right_decimal_scale) {
            return PRDB_SDK_SQL_EVAL_TYPE_MISMATCH;
        }
        *ordering = compare_signed_bytes(left->data, right->data, 16);
        break;
    case PRDB_SDK_VALUE_TYPE_TEXT:
    case PRDB_SDK_VALUE_TYPE_VARCHAR:
    case PRDB_SDK_VALUE_TYPE_BYTEA:
    case PRDB_SDK_VALUE_TYPE_UUID:
        *ordering = compare_bytes(left->data, left->data_len, right->data,
                                  right->data_len);
        break;
    default:
        return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
    }
    return PRDB_SDK_SQL_EVAL_OK;
}

int32_t prdb_sdk_sql_compare_v1(const prdb_sdk_value_v1 *left,
                                const prdb_sdk_value_v1 *right,
                                uint32_t comparison, uint8_t left_decimal_scale,
                                uint8_t right_decimal_scale,
                                uint32_t *truth_out) {
    int32_t status;
    int ordering;

    if (truth_out == NULL) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (comparison < PRDB_SDK_SQL_COMPARE_EQUAL ||
        comparison > PRDB_SDK_SQL_COMPARE_GREATER_EQUAL) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    status = validate_eval_value(left);
    if (status != PRDB_SDK_SQL_EVAL_OK) {
        return status;
    }
    status = validate_eval_value(right);
    if (status != PRDB_SDK_SQL_EVAL_OK) {
        return status;
    }
    if (left->state == PRDB_SDK_VALUE_STATE_NULL ||
        right->state == PRDB_SDK_VALUE_STATE_NULL) {
        *truth_out = PRDB_SDK_SQL_TRUTH_UNKNOWN;
        return PRDB_SDK_SQL_EVAL_OK;
    }
    if (left->type_code != right->type_code) {
        return PRDB_SDK_SQL_EVAL_TYPE_MISMATCH;
    }
    status = compare_value_payloads(left, right, left_decimal_scale,
                                     right_decimal_scale, &ordering);
    if (status != PRDB_SDK_SQL_EVAL_OK) {
        return status;
    }
    switch (comparison) {
    case PRDB_SDK_SQL_COMPARE_EQUAL:
        *truth_out = ordering == 0 ? PRDB_SDK_SQL_TRUTH_TRUE
                                   : PRDB_SDK_SQL_TRUTH_FALSE;
        break;
    case PRDB_SDK_SQL_COMPARE_NOT_EQUAL:
        *truth_out = ordering != 0 ? PRDB_SDK_SQL_TRUTH_TRUE
                                   : PRDB_SDK_SQL_TRUTH_FALSE;
        break;
    case PRDB_SDK_SQL_COMPARE_LESS:
        *truth_out = ordering < 0 ? PRDB_SDK_SQL_TRUTH_TRUE
                                  : PRDB_SDK_SQL_TRUTH_FALSE;
        break;
    case PRDB_SDK_SQL_COMPARE_LESS_EQUAL:
        *truth_out = ordering <= 0 ? PRDB_SDK_SQL_TRUTH_TRUE
                                   : PRDB_SDK_SQL_TRUTH_FALSE;
        break;
    case PRDB_SDK_SQL_COMPARE_GREATER:
        *truth_out = ordering > 0 ? PRDB_SDK_SQL_TRUTH_TRUE
                                  : PRDB_SDK_SQL_TRUTH_FALSE;
        break;
    case PRDB_SDK_SQL_COMPARE_GREATER_EQUAL:
        *truth_out = ordering >= 0 ? PRDB_SDK_SQL_TRUTH_TRUE
                                   : PRDB_SDK_SQL_TRUTH_FALSE;
        break;
    default:
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    return PRDB_SDK_SQL_EVAL_OK;
}

int32_t prdb_sdk_sql_value_is_null_v1(const prdb_sdk_value_v1 *value,
                                      uint32_t *truth_out) {
    int32_t status;
    if (truth_out == NULL) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    status = validate_eval_value(value);
    if (status != PRDB_SDK_SQL_EVAL_OK) {
        return status;
    }
    *truth_out = value->state == PRDB_SDK_VALUE_STATE_NULL
                     ? PRDB_SDK_SQL_TRUTH_TRUE
                     : PRDB_SDK_SQL_TRUTH_FALSE;
    return PRDB_SDK_SQL_EVAL_OK;
}

int32_t prdb_sdk_sql_eval_select_predicate_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *column_value,
    const prdb_sdk_value_v1 *parameter_value, uint32_t expected_type,
    uint8_t decimal_scale, uint32_t *truth_out) {
    int32_t status;

    if (statement == NULL || truth_out == NULL ||
        statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->has_where != 1 ||
        expected_type < PRDB_SDK_VALUE_TYPE_BOOLEAN ||
        expected_type > PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ ||
        (expected_type == PRDB_SDK_VALUE_TYPE_DECIMAL
             ? decimal_scale > 38
             : decimal_scale != 0)) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    status = validate_eval_value(column_value);
    if (status != PRDB_SDK_SQL_EVAL_OK) {
        return status;
    }
    if (column_value->state == PRDB_SDK_VALUE_STATE_VALUE &&
        column_value->type_code != expected_type) {
        return PRDB_SDK_SQL_EVAL_TYPE_MISMATCH;
    }

    if (statement->comparison == PRDB_SDK_SQL_COMPARE_IS_NULL ||
        statement->comparison == PRDB_SDK_SQL_COMPARE_IS_NOT_NULL) {
        if (parameter_value != NULL || statement->parameter_index != 0) {
            return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
        }
        status = prdb_sdk_sql_value_is_null_v1(column_value, truth_out);
        if (status != PRDB_SDK_SQL_EVAL_OK) {
            return status;
        }
        if (statement->comparison == PRDB_SDK_SQL_COMPARE_IS_NOT_NULL) {
            *truth_out = *truth_out == PRDB_SDK_SQL_TRUTH_TRUE
                             ? PRDB_SDK_SQL_TRUTH_FALSE
                             : PRDB_SDK_SQL_TRUTH_TRUE;
        }
        return PRDB_SDK_SQL_EVAL_OK;
    }
    if (statement->comparison < PRDB_SDK_SQL_COMPARE_EQUAL ||
        statement->comparison > PRDB_SDK_SQL_COMPARE_GREATER_EQUAL ||
        parameter_value == NULL || statement->parameter_index == 0) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    status = validate_eval_value(parameter_value);
    if (status != PRDB_SDK_SQL_EVAL_OK) {
        return status;
    }
    if (parameter_value->state == PRDB_SDK_VALUE_STATE_VALUE &&
        parameter_value->type_code != expected_type) {
        return PRDB_SDK_SQL_EVAL_TYPE_MISMATCH;
    }
    return prdb_sdk_sql_compare_v1(column_value, parameter_value,
                                   statement->comparison, decimal_scale,
                                   decimal_scale, truth_out);
}

static int32_t validate_predicate_tree_v2(
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

static uint8_t fold_identifier_byte(uint8_t byte) {
    return byte >= 'A' && byte <= 'Z' ? (uint8_t)(byte + ('a' - 'A')) : byte;
}

static int valid_identifier_bytes(const uint8_t *name, uint32_t name_len) {
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

static int span_matches_name(const prdb_sdk_select_v2 *statement,
                             prdb_sdk_sql_span_v1 span, const uint8_t *name,
                             uint32_t name_len) {
    if (span.length != name_len || span.offset > statement->sql_len ||
        span.length > statement->sql_len - span.offset) {
        return 0;
    }
    for (uint32_t index = 0; index < name_len; ++index) {
        if (fold_identifier_byte(statement->sql[span.offset + index]) !=
            fold_identifier_byte(name[index])) {
            return 0;
        }
    }
    return 1;
}

static int32_t validate_column_shape_v2(
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count) {
    if (column_count == 0 ||
        column_count > PRDB_SDK_SQL_MAX_RELATION_COLUMNS ||
        (column_count != 0 && columns == NULL)) {
        return PRDB_SDK_SQL_BIND_INVALID_ARGUMENT;
    }
    for (uint32_t index = 0; index < column_count; ++index) {
        const prdb_sdk_sql_column_shape_v2 *column = &columns[index];
        if (!valid_identifier_bytes(column->name, column->name_len) ||
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

static int32_t find_column_shape_v2(
    const prdb_sdk_select_v2 *statement, prdb_sdk_sql_span_v1 span,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    uint32_t *ordinal_out) {
    uint32_t found = UINT32_MAX;
    for (uint32_t index = 0; index < column_count; ++index) {
        if (!span_matches_name(statement, span, columns[index].name,
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
        !valid_identifier_bytes(relation_name, relation_name_len)) {
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
    if (!span_matches_name(statement, statement->relation, relation_name,
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
    status = validate_column_shape_v2(columns, column_count);
    if (status != PRDB_SDK_SQL_BIND_OK) {
        return status;
    }
    if (statement->predicate_count != 0 &&
        validate_predicate_tree_v2(statement) != PRDB_SDK_SQL_EVAL_OK) {
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
            status = find_column_shape_v2(
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
        status = find_column_shape_v2(statement, node->column, columns,
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
                    PRDB_SDK_SQL_OK};
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
                      PRDB_SDK_SQL_OK};
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
        status = find_column_shape_v2(
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
                     PRDB_SDK_SQL_OK};
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
        status = parse_predicate_or_v2(
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
    if (!span_matches_name(
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
        !span_matches_name(
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

static int32_t parse_group_order_item(
    parser *state, const prdb_sdk_select_v5 *statement,
    int allow_null_placement, prdb_sdk_sql_order_item_v12 *item) {
    prdb_sdk_sql_span_v1 order_column = {0};
    int aggregate_matches = 0;
    int32_t status;

    memset(item, 0, sizeof(*item));
    item->direction = PRDB_SDK_SQL_V10_ORDER_ASCENDING;
    if (state->current.kind == TOKEN_IDENTIFIER &&
        span_matches_name(&statement->base, statement->group_by_column,
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
            if (!span_matches_name(&statement->base, order_column,
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
                     PRDB_SDK_SQL_OK};
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
        status = parse_predicate_or_v2(
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
    if (!span_matches_name(
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
            if (!span_matches_name(
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

int32_t prdb_sdk_sql_parse_select_v13(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v13 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out) {
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
                     PRDB_SDK_SQL_OK};
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
            if (span_matches_name(&result.base.base,
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
    if (result.group_key_count < 2) {
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
        status = parse_predicate_or_v2(
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
        if (!span_matches_name(&result.base.base, result.group_keys[index],
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

int32_t prdb_sdk_sql_bind_select_v13(
    const prdb_sdk_select_v13 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v13 *out, uint32_t out_capacity) {
    prdb_sdk_select_binding_v13 result = {0};
    int32_t status;

    if (statement == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_SQL_BIND_INVALID_ARGUMENT;
    }
    if (statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V13_VERSION ||
        statement->group_key_count < 2 ||
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
            !span_matches_name(&statement->base.base, projection,
                               statement->base.base.sql + key.offset,
                               key.length)) {
            return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
        }
        status = find_column_shape_v2(&statement->base.base, key, columns,
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

int32_t prdb_sdk_sql_parse_select_v14(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v14 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out) {
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
                    PRDB_SDK_SQL_OK};
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
                        {TOKEN_END, 0, 0}, 0, PRDB_SDK_SQL_OK};
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
        status = prdb_sdk_sql_parse_select_v13(
            base_sql, sql_len, &result.base, sizeof(result.base),
            error_offset_out);
        if (status != PRDB_SDK_SQL_OK) return status;
        result.base.base.base.sql = sql;
        result.has_having = 1;
        having = (parser){sql, (uint32_t)sql_len,
                          having_token.offset + having_token.length,
                          {TOKEN_END, 0, 0}, 0, PRDB_SDK_SQL_OK};
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
                   !span_matches_name(&result.base.base.base,
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
        status = prdb_sdk_sql_parse_select_v13(
            sql, sql_len, &result.base, sizeof(result.base), error_offset_out);
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

int32_t prdb_sdk_sql_bind_select_v14(
    const prdb_sdk_select_v14 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v14 *out, uint32_t out_capacity) {
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
    status = prdb_sdk_sql_bind_select_v13(
        &statement->base, relation_name, relation_name_len, columns,
        column_count, &result.base, sizeof(result.base));
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
                   !span_matches_name(&grouped->base.base,
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
        if (span_matches_name(&grouped->base.base,
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

int32_t prdb_sdk_sql_parse_select_v15(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v15 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out) {
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
                    PRDB_SDK_SQL_OK};
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
                        {TOKEN_END, 0, 0}, 0, PRDB_SDK_SQL_OK};
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
        status = prdb_sdk_sql_parse_select_v14(
            base_sql, sql_len, &result.base, sizeof(result.base),
            error_offset_out);
        if (status != PRDB_SDK_SQL_OK) return status;
        result.base.base.base.base.sql = sql;
        order = (parser){sql, (uint32_t)sql_len, by_end,
                         {TOKEN_END, 0, 0}, 0, PRDB_SDK_SQL_OK};
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
        status = prdb_sdk_sql_parse_select_v14(
            sql, sql_len, &result.base, sizeof(result.base), error_offset_out);
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

int32_t prdb_sdk_sql_bind_select_v15(
    const prdb_sdk_select_v15 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v15 *out, uint32_t out_capacity) {
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
    status = prdb_sdk_sql_bind_select_v14(
        &statement->base, relation_name, relation_name_len, columns,
        column_count, &result.base, sizeof(result.base));
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
        !span_matches_name(
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
                   !valid_identifier_bytes(
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
        status = find_column_shape_v2(
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

static int32_t evaluate_predicate_node_v2(
    const prdb_sdk_select_v2 *statement, uint32_t index,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    prdb_sdk_sql_resolve_column_v2 resolve_column, void *resolver_context,
    const prdb_sdk_select_binding_v2 *binding,
    prdb_sdk_sql_resolve_bound_column_v2 resolve_bound_column,
    uint32_t depth, uint32_t *truth_out) {
    const prdb_sdk_predicate_node_v2 *node = &statement->predicates[index];
    uint32_t left_truth;
    uint32_t right_truth;
    int32_t status;

    if (depth > PRDB_SDK_SQL_MAX_PREDICATE_NODES) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (node->node_kind == PRDB_SDK_SQL_PREDICATE_NOT) {
        status = evaluate_predicate_node_v2(
            statement, node->left_child, parameters, parameter_count,
            resolve_column, resolver_context, binding, resolve_bound_column,
            depth + 1, &left_truth);
        if (status != PRDB_SDK_SQL_EVAL_OK) {
            return status;
        }
        return prdb_sdk_sql_truth_not_v1(left_truth, truth_out);
    }
    if (node->node_kind == PRDB_SDK_SQL_PREDICATE_AND ||
        node->node_kind == PRDB_SDK_SQL_PREDICATE_OR) {
        status = evaluate_predicate_node_v2(
            statement, node->left_child, parameters, parameter_count,
            resolve_column, resolver_context, binding, resolve_bound_column,
            depth + 1, &left_truth);
        if (status != PRDB_SDK_SQL_EVAL_OK) {
            return status;
        }
        if (node->node_kind == PRDB_SDK_SQL_PREDICATE_AND &&
            left_truth == PRDB_SDK_SQL_TRUTH_FALSE) {
            *truth_out = PRDB_SDK_SQL_TRUTH_FALSE;
            return PRDB_SDK_SQL_EVAL_OK;
        }
        if (node->node_kind == PRDB_SDK_SQL_PREDICATE_OR &&
            left_truth == PRDB_SDK_SQL_TRUTH_TRUE) {
            *truth_out = PRDB_SDK_SQL_TRUTH_TRUE;
            return PRDB_SDK_SQL_EVAL_OK;
        }
        status = evaluate_predicate_node_v2(
            statement, node->right_child, parameters, parameter_count,
            resolve_column, resolver_context, binding, resolve_bound_column,
            depth + 1, &right_truth);
        if (status != PRDB_SDK_SQL_EVAL_OK) {
            return status;
        }
        return node->node_kind == PRDB_SDK_SQL_PREDICATE_AND
                   ? prdb_sdk_sql_truth_and_v1(left_truth, right_truth,
                                               truth_out)
                   : prdb_sdk_sql_truth_or_v1(left_truth, right_truth,
                                              truth_out);
    }

    {
        prdb_sdk_select_v1 leaf = {0};
        const prdb_sdk_value_v1 *column_value = NULL;
        prdb_sdk_value_v1 bound_parameter;
        const prdb_sdk_value_v1 *predicate_parameter = NULL;
        uint32_t expected_type = 0;
        uint8_t precision = 0;
        uint8_t scale = 0;

        if (node->node_kind == PRDB_SDK_SQL_PREDICATE_COMPARE &&
            node->parameter_index > parameter_count) {
            return PRDB_SDK_SQL_EVAL_PARAMETER_MISSING;
        }
        if (binding == NULL) {
            status = resolve_column(resolver_context, statement->sql,
                                    statement->sql_len, node->column,
                                    &column_value, &expected_type, &precision,
                                    &scale);
        } else {
            const prdb_sdk_sql_bound_column_v2 *column =
                &binding->predicate_columns[index];
            expected_type = column->type_code;
            precision = column->decimal_precision;
            scale = column->decimal_scale;
            status = resolve_bound_column(resolver_context, column->ordinal,
                                          &column_value);
        }
        if (status != PRDB_SDK_SQL_EVAL_OK) {
            return status;
        }
        if (column_value == NULL ||
            expected_type < PRDB_SDK_VALUE_TYPE_BOOLEAN ||
            expected_type > PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ ||
            (expected_type == PRDB_SDK_VALUE_TYPE_DECIMAL
                 ? precision == 0 || precision > 38 || scale > precision
                 : precision != 0 || scale != 0)) {
            return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
        }
        leaf.struct_size = sizeof(leaf);
        leaf.abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
        leaf.sql = statement->sql;
        leaf.sql_len = statement->sql_len;
        leaf.has_where = 1;
        leaf.where_column = node->column;
        leaf.parameter_index = node->parameter_index;
        if (node->node_kind == PRDB_SDK_SQL_PREDICATE_COMPARE) {
            leaf.comparison = node->comparison;
            status = prdb_sdk_sql_bind_select_parameter_v1(
                &leaf, parameters, parameter_count, expected_type, precision,
                scale, &bound_parameter, sizeof(bound_parameter));
            if (status != PRDB_SDK_SQL_EVAL_OK) {
                return status;
            }
            predicate_parameter = &bound_parameter;
        } else {
            leaf.comparison =
                node->node_kind == PRDB_SDK_SQL_PREDICATE_IS_NULL
                    ? PRDB_SDK_SQL_COMPARE_IS_NULL
                    : PRDB_SDK_SQL_COMPARE_IS_NOT_NULL;
        }
        return prdb_sdk_sql_eval_select_predicate_v1(
            &leaf, column_value, predicate_parameter, expected_type, scale,
            truth_out);
    }
}

int32_t prdb_sdk_sql_eval_select_predicate_v2(
    const prdb_sdk_select_v2 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    prdb_sdk_sql_resolve_column_v2 resolve_column, void *resolver_context,
    uint32_t *truth_out) {
    int32_t status;
    uint32_t truth;

    if (statement == NULL || truth_out == NULL ||
        statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V2_VERSION ||
        statement->sql == NULL || statement->sql_len == 0 ||
        statement->sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        parameter_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        (parameter_count != 0 && parameters == NULL)) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (statement->predicate_count == 0) {
        if (statement->predicate_root != 0) {
            return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
        }
        *truth_out = PRDB_SDK_SQL_TRUTH_TRUE;
        return PRDB_SDK_SQL_EVAL_OK;
    }
    if (resolve_column == NULL) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    status = validate_predicate_tree_v2(statement);
    if (status != PRDB_SDK_SQL_EVAL_OK) {
        return status;
    }
    status = evaluate_predicate_node_v2(
        statement, statement->predicate_root, parameters, parameter_count,
        resolve_column, resolver_context, NULL, NULL, 0, &truth);
    if (status == PRDB_SDK_SQL_EVAL_OK) {
        *truth_out = truth;
    }
    return status;
}

static int valid_bound_column_type_v2(
    const prdb_sdk_sql_bound_column_v2 *column) {
    return column->type_code >= PRDB_SDK_VALUE_TYPE_BOOLEAN &&
           column->type_code <= PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ &&
           (column->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL
                ? column->decimal_precision != 0 &&
                      column->decimal_precision <= 38 &&
                      column->decimal_scale <= column->decimal_precision
                : column->decimal_precision == 0 &&
                      column->decimal_scale == 0);
}

int32_t prdb_sdk_sql_eval_bound_select_predicate_v2(
    const prdb_sdk_select_v2 *statement,
    const prdb_sdk_select_binding_v2 *binding,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    prdb_sdk_sql_resolve_bound_column_v2 resolve_column,
    void *resolver_context, uint32_t *truth_out) {
    int32_t status;
    uint32_t truth;

    if (statement == NULL || binding == NULL || truth_out == NULL ||
        statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V2_VERSION ||
        statement->sql == NULL || statement->sql_len == 0 ||
        statement->sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        statement->projection_star > 1 ||
        (statement->projection_star == 1
             ? statement->projection_count != 0
             : statement->projection_count == 0 ||
                   statement->projection_count >
                       PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS) ||
        binding->struct_size < sizeof(*binding) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V2_VERSION ||
        binding->projection_star != statement->projection_star ||
        binding->projection_count != statement->projection_count ||
        binding->relation_column_count == 0 ||
        binding->relation_column_count >
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS ||
        binding->predicate_count != statement->predicate_count ||
        binding->projection_count > PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS ||
        binding->predicate_count > PRDB_SDK_SQL_MAX_PREDICATE_NODES ||
        parameter_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        (parameter_count != 0 && parameters == NULL)) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (statement->predicate_count == 0) {
        if (statement->predicate_root != 0) {
            return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
        }
        *truth_out = PRDB_SDK_SQL_TRUTH_TRUE;
        return PRDB_SDK_SQL_EVAL_OK;
    }
    if (resolve_column == NULL) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    status = validate_predicate_tree_v2(statement);
    if (status != PRDB_SDK_SQL_EVAL_OK) {
        return status;
    }
    for (uint32_t index = 0; index < statement->predicate_count; ++index) {
        uint32_t kind = statement->predicates[index].node_kind;
        if ((kind == PRDB_SDK_SQL_PREDICATE_COMPARE ||
             kind == PRDB_SDK_SQL_PREDICATE_IS_NULL ||
             kind == PRDB_SDK_SQL_PREDICATE_IS_NOT_NULL) &&
            (!valid_bound_column_type_v2(&binding->predicate_columns[index]) ||
             binding->predicate_columns[index].ordinal >=
                 binding->relation_column_count)) {
            return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
        }
    }
    if (binding->projection_star == 0) {
        for (uint32_t index = 0; index < binding->projection_count; ++index) {
            if (binding->projection_ordinals[index] >=
                binding->relation_column_count) {
                return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
            }
        }
    }
    status = evaluate_predicate_node_v2(
        statement, statement->predicate_root, parameters, parameter_count,
        NULL, resolver_context, binding, resolve_column, 0, &truth);
    if (status == PRDB_SDK_SQL_EVAL_OK) {
        *truth_out = truth;
    }
    return status;
}

typedef struct bound_row_resolver_context_v2 {
    const prdb_sdk_value_v1 *cells;
    uint32_t cell_count;
} bound_row_resolver_context_v2;

static int32_t resolve_bound_row_column_v2(
    void *opaque, uint32_t ordinal, const prdb_sdk_value_v1 **value_out) {
    bound_row_resolver_context_v2 *context = opaque;
    if (context == NULL || value_out == NULL || ordinal >= context->cell_count) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    *value_out = &context->cells[ordinal];
    return PRDB_SDK_SQL_EVAL_OK;
}

int32_t prdb_sdk_sql_eval_bound_select_row_v2(
    const prdb_sdk_select_v2 *statement,
    const prdb_sdk_select_binding_v2 *binding,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const prdb_sdk_row_v1 *row, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint32_t *truth_out) {
    bound_row_resolver_context_v2 context;
    prdb_sdk_cell_iterator_v1 iterator = {0};
    int32_t status;

    if (statement == NULL || binding == NULL || row == NULL ||
        truth_out == NULL ||
        row->struct_size < sizeof(*row) ||
        row->abi_version != PRDB_SDK_BATCH_ABI_V1_VERSION ||
        row->cell_count != binding->relation_column_count ||
        (row->payload_len != 0 && row->payload == NULL) ||
        (row->cell_count != 0 && cell_scratch == NULL) ||
        cell_scratch_capacity < row->cell_count) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    status = prdb_sdk_row_cells_begin_v1(row, &iterator, sizeof(iterator));
    if (status != PRDB_SDK_BATCH_OK) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    for (uint32_t index = 0; index < row->cell_count; ++index) {
        status = prdb_sdk_cell_next_v1(&iterator, &cell_scratch[index],
                                       sizeof(cell_scratch[index]));
        if (status != PRDB_SDK_BATCH_OK) {
            return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
        }
    }
    context.cells = cell_scratch;
    context.cell_count = row->cell_count;
    return prdb_sdk_sql_eval_bound_select_predicate_v2(
        statement, binding, parameters, parameter_count,
        resolve_bound_row_column_v2, &context, truth_out);
}

int32_t prdb_sdk_sql_value_truth_v1(const prdb_sdk_value_v1 *value,
                                    uint32_t *truth_out) {
    int32_t status;
    if (truth_out == NULL) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    status = validate_eval_value(value);
    if (status != PRDB_SDK_SQL_EVAL_OK) {
        return status;
    }
    if (value->state == PRDB_SDK_VALUE_STATE_NULL) {
        *truth_out = PRDB_SDK_SQL_TRUTH_UNKNOWN;
        return PRDB_SDK_SQL_EVAL_OK;
    }
    if (value->type_code != PRDB_SDK_VALUE_TYPE_BOOLEAN) {
        return PRDB_SDK_SQL_EVAL_TYPE_MISMATCH;
    }
    *truth_out = value->data[0] == 0 ? PRDB_SDK_SQL_TRUTH_FALSE
                                     : PRDB_SDK_SQL_TRUTH_TRUE;
    return PRDB_SDK_SQL_EVAL_OK;
}

static int valid_truth(uint32_t value) {
    return value == PRDB_SDK_SQL_TRUTH_UNKNOWN ||
           value == PRDB_SDK_SQL_TRUTH_FALSE ||
           value == PRDB_SDK_SQL_TRUTH_TRUE;
}

int32_t prdb_sdk_sql_truth_not_v1(uint32_t operand, uint32_t *truth_out) {
    if (truth_out == NULL || !valid_truth(operand)) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    *truth_out = operand == PRDB_SDK_SQL_TRUTH_UNKNOWN
                     ? PRDB_SDK_SQL_TRUTH_UNKNOWN
                     : operand == PRDB_SDK_SQL_TRUTH_TRUE
                           ? PRDB_SDK_SQL_TRUTH_FALSE
                           : PRDB_SDK_SQL_TRUTH_TRUE;
    return PRDB_SDK_SQL_EVAL_OK;
}

int32_t prdb_sdk_sql_truth_and_v1(uint32_t left, uint32_t right,
                                  uint32_t *truth_out) {
    if (truth_out == NULL || !valid_truth(left) || !valid_truth(right)) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (left == PRDB_SDK_SQL_TRUTH_FALSE ||
        right == PRDB_SDK_SQL_TRUTH_FALSE) {
        *truth_out = PRDB_SDK_SQL_TRUTH_FALSE;
    } else if (left == PRDB_SDK_SQL_TRUTH_UNKNOWN ||
               right == PRDB_SDK_SQL_TRUTH_UNKNOWN) {
        *truth_out = PRDB_SDK_SQL_TRUTH_UNKNOWN;
    } else {
        *truth_out = PRDB_SDK_SQL_TRUTH_TRUE;
    }
    return PRDB_SDK_SQL_EVAL_OK;
}

int32_t prdb_sdk_sql_truth_or_v1(uint32_t left, uint32_t right,
                                 uint32_t *truth_out) {
    if (truth_out == NULL || !valid_truth(left) || !valid_truth(right)) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (left == PRDB_SDK_SQL_TRUTH_TRUE ||
        right == PRDB_SDK_SQL_TRUTH_TRUE) {
        *truth_out = PRDB_SDK_SQL_TRUTH_TRUE;
    } else if (left == PRDB_SDK_SQL_TRUTH_UNKNOWN ||
               right == PRDB_SDK_SQL_TRUTH_UNKNOWN) {
        *truth_out = PRDB_SDK_SQL_TRUTH_UNKNOWN;
    } else {
        *truth_out = PRDB_SDK_SQL_TRUTH_FALSE;
    }
    return PRDB_SDK_SQL_EVAL_OK;
}

static int32_t parse_qualified_column_v8(
    parser *state, prdb_sdk_sql_qualified_column_v8 *out) {
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

int32_t prdb_sdk_sql_parse_select_v8(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_select_v8 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out) {
    parser state;
    prdb_sdk_select_v8 result = {0};
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
                     PRDB_SDK_SQL_OK};
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
        if (result.projection_count >= PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS) {
            state.error_offset = state.current.offset;
            status = PRDB_SDK_SQL_LIMIT_EXCEEDED;
            goto failed_join_select_v8;
        }
        status = parse_qualified_column_v8(
            &state, &result.projections[result.projection_count]);
        if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v8;
        ++result.projection_count;
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
    status = parse_qualified_column_v8(&state, &result.left_join_key);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v8;
    if (state.current.kind != TOKEN_EQUAL) {
        state.error_offset = state.current.offset;
        status = PRDB_SDK_SQL_SYNTAX_ERROR;
        goto failed_join_select_v8;
    }
    status = advance(&state);
    if (status != PRDB_SDK_SQL_OK) goto failed_join_select_v8;
    status = parse_qualified_column_v8(&state, &result.right_join_key);
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
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V8_VERSION;
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
    const prdb_sdk_select_v8 *statement,
    const prdb_sdk_sql_qualified_column_v8 *reference,
    const uint8_t *left_relation_name, uint32_t left_relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *left_columns,
    uint32_t left_column_count,
    const uint8_t *right_relation_name, uint32_t right_relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *right_columns,
    uint32_t right_column_count, uint32_t *side_out,
    prdb_sdk_sql_bound_column_v2 *column_out) {
    prdb_sdk_select_v2 name_statement = {0};
    const prdb_sdk_sql_column_shape_v2 *columns;
    uint32_t column_count;
    uint32_t ordinal;
    uint32_t side;
    int32_t status;

    name_statement.sql = statement->sql;
    name_statement.sql_len = statement->sql_len;
    if (span_matches_name(&name_statement, reference->relation,
                          left_relation_name, left_relation_name_len)) {
        side = PRDB_SDK_SQL_V8_LEFT;
        columns = left_columns;
        column_count = left_column_count;
    } else if (span_matches_name(&name_statement, reference->relation,
                                 right_relation_name,
                                 right_relation_name_len)) {
        side = PRDB_SDK_SQL_V8_RIGHT;
        columns = right_columns;
        column_count = right_column_count;
    } else {
        return PRDB_SDK_SQL_BIND_RELATION_MISMATCH;
    }
    status = find_column_shape_v2(&name_statement, reference->column,
                                  columns, column_count, &ordinal);
    if (status != PRDB_SDK_SQL_BIND_OK) return status;
    *side_out = side;
    *column_out = (prdb_sdk_sql_bound_column_v2){
        ordinal, columns[ordinal].type_code,
        columns[ordinal].decimal_precision, columns[ordinal].decimal_scale};
    return PRDB_SDK_SQL_BIND_OK;
}

int32_t prdb_sdk_sql_bind_select_v8(
    const prdb_sdk_select_v8 *statement,
    const uint8_t *left_relation_name, uint32_t left_relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *left_columns,
    uint32_t left_column_count,
    const uint8_t *right_relation_name, uint32_t right_relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *right_columns,
    uint32_t right_column_count, prdb_sdk_select_binding_v8 *out,
    uint32_t out_capacity) {
    prdb_sdk_select_binding_v8 result = {0};
    prdb_sdk_select_v2 name_statement = {0};
    uint32_t side;
    int32_t status;

    if (statement == NULL || left_relation_name == NULL ||
        right_relation_name == NULL || out == NULL ||
        out_capacity < sizeof(*out) ||
        !valid_identifier_bytes(left_relation_name, left_relation_name_len) ||
        !valid_identifier_bytes(right_relation_name,
                                right_relation_name_len)) {
        return PRDB_SDK_SQL_BIND_INVALID_ARGUMENT;
    }
    if (statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V8_VERSION ||
        statement->sql == NULL || statement->sql_len == 0 ||
        statement->sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        statement->projection_count == 0 ||
        statement->projection_count > PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS ||
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
        statement->left_join_key.relation.offset > statement->sql_len ||
        statement->left_join_key.relation.length >
            statement->sql_len - statement->left_join_key.relation.offset ||
        statement->left_join_key.column.offset > statement->sql_len ||
        statement->left_join_key.column.length >
            statement->sql_len - statement->left_join_key.column.offset ||
        statement->right_join_key.relation.offset > statement->sql_len ||
        statement->right_join_key.relation.length >
            statement->sql_len - statement->right_join_key.relation.offset ||
        statement->right_join_key.column.offset > statement->sql_len ||
        statement->right_join_key.column.length >
            statement->sql_len - statement->right_join_key.column.offset) {
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    }

    name_statement.sql = statement->sql;
    name_statement.sql_len = statement->sql_len;
    if (!span_matches_name(&name_statement, statement->left_relation,
                           left_relation_name, left_relation_name_len) ||
        !span_matches_name(&name_statement, statement->right_relation,
                           right_relation_name, right_relation_name_len)) {
        return PRDB_SDK_SQL_BIND_RELATION_MISMATCH;
    }
    if (span_matches_name(&name_statement, statement->left_relation,
                          right_relation_name, right_relation_name_len) &&
        span_matches_name(&name_statement, statement->right_relation,
                          left_relation_name, left_relation_name_len)) {
        return PRDB_SDK_SQL_BIND_AMBIGUOUS_COLUMN;
    }
    status = validate_column_shape_v2(left_columns, left_column_count);
    if (status != PRDB_SDK_SQL_BIND_OK) return status;
    status = validate_column_shape_v2(right_columns, right_column_count);
    if (status != PRDB_SDK_SQL_BIND_OK) return status;

    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_SQL_ABI_V8_VERSION;
    result.left_relation_column_count = left_column_count;
    result.right_relation_column_count = right_column_count;
    result.projection_count = statement->projection_count;
    for (uint32_t index = 0; index < statement->projection_count; ++index) {
        status = resolve_qualified_column_v8(
            statement, &statement->projections[index], left_relation_name,
            left_relation_name_len, left_columns, left_column_count,
            right_relation_name, right_relation_name_len, right_columns,
            right_column_count, &side, &result.projections[index].column);
        if (status != PRDB_SDK_SQL_BIND_OK) return status;
        result.projections[index].relation_side = side;
    }
    status = resolve_qualified_column_v8(
        statement, &statement->left_join_key, left_relation_name,
        left_relation_name_len, left_columns, left_column_count,
        right_relation_name, right_relation_name_len, right_columns,
        right_column_count, &side, &result.left_join_key.column);
    if (status != PRDB_SDK_SQL_BIND_OK) return status;
    if (side != PRDB_SDK_SQL_V8_LEFT) {
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    }
    result.left_join_key.relation_side = side;
    status = resolve_qualified_column_v8(
        statement, &statement->right_join_key, left_relation_name,
        left_relation_name_len, left_columns, left_column_count,
        right_relation_name, right_relation_name_len, right_columns,
        right_column_count, &side, &result.right_join_key.column);
    if (status != PRDB_SDK_SQL_BIND_OK) return status;
    if (side != PRDB_SDK_SQL_V8_RIGHT) {
        return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
    }
    result.right_join_key.relation_side = side;
    if (result.left_join_key.column.type_code !=
            result.right_join_key.column.type_code ||
        result.left_join_key.column.decimal_scale !=
            result.right_join_key.column.decimal_scale) {
        return PRDB_SDK_SQL_BIND_V8_JOIN_TYPE_MISMATCH;
    }
    *out = result;
    return PRDB_SDK_SQL_BIND_OK;
}
