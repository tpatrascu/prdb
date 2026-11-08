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

int32_t prdb_sdk_sql_bind_select_parameter_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint32_t expected_type, uint8_t expected_decimal_precision,
    uint8_t expected_decimal_scale, prdb_sdk_value_v1 *out,
    uint32_t out_capacity) {
    const prdb_sdk_value_v1 *parameter;
    int32_t status;

    if (statement == NULL || out == NULL || out_capacity < sizeof(*out) ||
        parameter_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        (parameter_count != 0 && parameters == NULL) ||
        statement->struct_size < sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->has_where != 1 || statement->parameter_index == 0 ||
        statement->parameter_index > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        expected_type < PRDB_SDK_VALUE_TYPE_BOOLEAN ||
        expected_type > PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ ||
        (expected_type == PRDB_SDK_VALUE_TYPE_DECIMAL &&
         (expected_decimal_precision == 0 || expected_decimal_precision > 38 ||
          expected_decimal_scale > expected_decimal_precision)) ||
        (expected_type != PRDB_SDK_VALUE_TYPE_DECIMAL &&
         (expected_decimal_precision != 0 || expected_decimal_scale != 0))) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (statement->parameter_index > parameter_count) {
        return PRDB_SDK_SQL_EVAL_PARAMETER_MISSING;
    }
    parameter = &parameters[statement->parameter_index - 1];
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
