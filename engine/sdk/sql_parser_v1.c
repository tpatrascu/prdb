#include "sql_ast_internal.h"

#include <stdlib.h>
#include <string.h>

#define ARENA_CHUNK_DEFAULT 16384
#define MAX_PARSER_DEPTH 64

PRDB_SDK_INTERNAL void prdb_sql_arena_init(prdb_sql_arena *arena,
                                           size_t max_bytes) {
    arena->head = NULL;
    arena->total_allocated = 0;
    arena->max_bytes = max_bytes == 0 ? (size_t)(16 * 1024 * 1024) : max_bytes;
}

PRDB_SDK_INTERNAL void *prdb_sql_arena_alloc(prdb_sql_arena *arena,
                                             size_t size, size_t align) {
    size_t aligned_used;
    size_t chunk_cap;
    prdb_sql_arena_chunk *chunk;

    if (arena == NULL) return NULL;
    if (size == 0) size = 1;
    if (align == 0) align = 8;
    if (arena->head != NULL) {
        aligned_used = (arena->head->used + (align - 1)) & ~(align - 1);
        if (aligned_used <= arena->head->capacity &&
            size <= arena->head->capacity - aligned_used) {
            void *ptr = arena->head->data + aligned_used;
            arena->head->used = aligned_used + size;
            memset(ptr, 0, size);
            return ptr;
        }
    }
    chunk_cap = size + align + ARENA_CHUNK_DEFAULT;
    if (chunk_cap < size) return NULL;
    if (arena->total_allocated > arena->max_bytes ||
        chunk_cap > arena->max_bytes - arena->total_allocated) {
        return NULL;
    }
    chunk = (prdb_sql_arena_chunk *)malloc(sizeof(prdb_sql_arena_chunk) +
                                           chunk_cap);
    if (chunk == NULL) return NULL;
    chunk->next = arena->head;
    chunk->capacity = chunk_cap;
    chunk->used = size;
    arena->head = chunk;
    arena->total_allocated += chunk_cap;
    memset(chunk->data, 0, size);
    return chunk->data;
}

PRDB_SDK_INTERNAL char *prdb_sql_arena_strdup(prdb_sql_arena *arena,
                                              const uint8_t *bytes,
                                              size_t len) {
    char *copy = (char *)prdb_sql_arena_alloc(arena, len + 1, 1);
    if (copy == NULL) return NULL;
    if (len != 0 && bytes != NULL) memcpy(copy, bytes, len);
    copy[len] = '\0';
    return copy;
}

PRDB_SDK_INTERNAL void prdb_sql_arena_free(prdb_sql_arena *arena) {
    prdb_sql_arena_chunk *cur;
    if (arena == NULL) return;
    cur = arena->head;
    while (cur != NULL) {
        prdb_sql_arena_chunk *next = cur->next;
        free(cur);
        cur = next;
    }
    arena->head = NULL;
    arena->total_allocated = 0;
}

/* --- Lexer --- */

typedef enum tok_kind {
    TOK_EOF = 0,
    TOK_IDENT,
    TOK_QUOTED_IDENT,
    TOK_STRING,
    TOK_NUMBER,
    TOK_PARAM,
    TOK_STAR,
    TOK_PLUS,
    TOK_MINUS,
    TOK_SLASH,
    TOK_PERCENT,
    TOK_EQ,
    TOK_NE,
    TOK_LT,
    TOK_LE,
    TOK_GT,
    TOK_GE,
    TOK_LPAREN,
    TOK_RPAREN,
    TOK_COMMA,
    TOK_DOT,
    TOK_SEMI
} tok_kind;

typedef struct tok {
    tok_kind kind;
    uint32_t offset;
    uint32_t length;
} tok;

typedef struct sql_parser {
    prdb_sql_arena *arena;
    const uint8_t *sql;
    uint32_t len;
    uint32_t pos;
    tok cur;
    int32_t err;
    uint32_t err_off;
} sql_parser;

static int is_ws(uint8_t b) {
    return b == ' ' || b == '\t' || b == '\r' || b == '\n';
}

static int is_ident_start(uint8_t b) {
    return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || b == '_';
}

static int is_ident_cont(uint8_t b) {
    return is_ident_start(b) || (b >= '0' && b <= '9');
}

static int ascii_ieq(const uint8_t *a, uint32_t alen, const char *b) {
    size_t blen = strlen(b);
    if (alen != blen) return 0;
    for (size_t i = 0; i < blen; ++i) {
        uint8_t ca = a[i];
        uint8_t cb = (uint8_t)b[i];
        if (ca >= 'A' && ca <= 'Z') ca = (uint8_t)(ca + ('a' - 'A'));
        if (cb >= 'A' && cb <= 'Z') cb = (uint8_t)(cb + ('a' - 'A'));
        if (ca != cb) return 0;
    }
    return 1;
}

static int tok_is_kw(const sql_parser *p, const char *kw) {
    return p->cur.kind == TOK_IDENT &&
           ascii_ieq(p->sql + p->cur.offset, p->cur.length, kw);
}

static int32_t next_tok(sql_parser *p) {
    for (;;) {
        while (p->pos < p->len && is_ws(p->sql[p->pos])) {
            ++p->pos;
        }
        if (p->pos + 1 < p->len && p->sql[p->pos] == '-' &&
            p->sql[p->pos + 1] == '-') {
            p->pos += 2;
            while (p->pos < p->len && p->sql[p->pos] != '\n') ++p->pos;
            continue;
        }
        if (p->pos + 1 < p->len && p->sql[p->pos] == '/' &&
            p->sql[p->pos + 1] == '*') {
            uint32_t start = p->pos;
            p->pos += 2;
            while (p->pos + 1 < p->len &&
                   !(p->sql[p->pos] == '*' && p->sql[p->pos + 1] == '/')) {
                ++p->pos;
            }
            if (p->pos + 1 >= p->len) {
                p->err = PRDB_SDK_SQL_SYNTAX_ERROR;
                p->err_off = start;
                return p->err;
            }
            p->pos += 2;
            continue;
        }
        break;
    }

    uint32_t start = p->pos;
    if (start >= p->len) {
        p->cur = (tok){TOK_EOF, start, 0};
        return PRDB_SDK_SQL_OK;
    }
    uint8_t b = p->sql[p->pos++];

    if (is_ident_start(b)) {
        while (p->pos < p->len && is_ident_cont(p->sql[p->pos])) ++p->pos;
        if (p->pos - start > PRDB_SDK_SQL_MAX_IDENTIFIER_BYTES) {
            p->err = PRDB_SDK_SQL_LIMIT_EXCEEDED;
            p->err_off = start;
            return p->err;
        }
        p->cur = (tok){TOK_IDENT, start, p->pos - start};
        return PRDB_SDK_SQL_OK;
    }
    if (b == '"') {
        while (p->pos < p->len) {
            if (p->sql[p->pos] == '"') {
                if (p->pos + 1 < p->len && p->sql[p->pos + 1] == '"') {
                    p->pos += 2;
                    continue;
                }
                ++p->pos;
                p->cur = (tok){TOK_QUOTED_IDENT, start, p->pos - start};
                return PRDB_SDK_SQL_OK;
            }
            ++p->pos;
        }
        p->err = PRDB_SDK_SQL_SYNTAX_ERROR;
        p->err_off = start;
        return p->err;
    }
    if (b == '\'') {
        while (p->pos < p->len) {
            if (p->sql[p->pos] == '\'') {
                if (p->pos + 1 < p->len && p->sql[p->pos + 1] == '\'') {
                    p->pos += 2;
                    continue;
                }
                ++p->pos;
                p->cur = (tok){TOK_STRING, start, p->pos - start};
                return PRDB_SDK_SQL_OK;
            }
            ++p->pos;
        }
        p->err = PRDB_SDK_SQL_SYNTAX_ERROR;
        p->err_off = start;
        return p->err;
    }
    if ((b >= '0' && b <= '9') ||
        (b == '.' && p->pos < p->len && p->sql[p->pos] >= '0' &&
         p->sql[p->pos] <= '9')) {
        if (b != '.') {
            while (p->pos < p->len && p->sql[p->pos] >= '0' &&
                   p->sql[p->pos] <= '9') {
                ++p->pos;
            }
            if (p->pos < p->len && p->sql[p->pos] == '.') {
                ++p->pos;
                while (p->pos < p->len && p->sql[p->pos] >= '0' &&
                       p->sql[p->pos] <= '9') {
                    ++p->pos;
                }
            }
        } else {
            while (p->pos < p->len && p->sql[p->pos] >= '0' &&
                   p->sql[p->pos] <= '9') {
                ++p->pos;
            }
        }
        if (p->pos < p->len &&
            (p->sql[p->pos] == 'e' || p->sql[p->pos] == 'E')) {
            uint32_t epos = p->pos++;
            if (p->pos < p->len &&
                (p->sql[p->pos] == '+' || p->sql[p->pos] == '-')) {
                ++p->pos;
            }
            uint32_t dstart = p->pos;
            while (p->pos < p->len && p->sql[p->pos] >= '0' &&
                   p->sql[p->pos] <= '9') {
                ++p->pos;
            }
            if (p->pos == dstart) {
                p->err = PRDB_SDK_SQL_SYNTAX_ERROR;
                p->err_off = epos;
                return p->err;
            }
        }
        p->cur = (tok){TOK_NUMBER, start, p->pos - start};
        return PRDB_SDK_SQL_OK;
    }
    if (b == '$') {
        uint32_t dstart = p->pos;
        while (p->pos < p->len && p->sql[p->pos] >= '0' &&
               p->sql[p->pos] <= '9') {
            ++p->pos;
        }
        if (p->pos == dstart) {
            p->err = PRDB_SDK_SQL_SYNTAX_ERROR;
            p->err_off = start;
            return p->err;
        }
        p->cur = (tok){TOK_PARAM, start, p->pos - start};
        return PRDB_SDK_SQL_OK;
    }
    switch (b) {
        case '*': p->cur = (tok){TOK_STAR, start, 1}; return PRDB_SDK_SQL_OK;
        case '+': p->cur = (tok){TOK_PLUS, start, 1}; return PRDB_SDK_SQL_OK;
        case '-': p->cur = (tok){TOK_MINUS, start, 1}; return PRDB_SDK_SQL_OK;
        case '/': p->cur = (tok){TOK_SLASH, start, 1}; return PRDB_SDK_SQL_OK;
        case '%': p->cur = (tok){TOK_PERCENT, start, 1}; return PRDB_SDK_SQL_OK;
        case '(': p->cur = (tok){TOK_LPAREN, start, 1}; return PRDB_SDK_SQL_OK;
        case ')': p->cur = (tok){TOK_RPAREN, start, 1}; return PRDB_SDK_SQL_OK;
        case ',': p->cur = (tok){TOK_COMMA, start, 1}; return PRDB_SDK_SQL_OK;
        case '.': p->cur = (tok){TOK_DOT, start, 1}; return PRDB_SDK_SQL_OK;
        case ';': p->cur = (tok){TOK_SEMI, start, 1}; return PRDB_SDK_SQL_OK;
        case '=': p->cur = (tok){TOK_EQ, start, 1}; return PRDB_SDK_SQL_OK;
        case '!':
            if (p->pos < p->len && p->sql[p->pos] == '=') {
                ++p->pos;
                p->cur = (tok){TOK_NE, start, 2};
                return PRDB_SDK_SQL_OK;
            }
            break;
        case '<':
            if (p->pos < p->len && p->sql[p->pos] == '=') {
                ++p->pos;
                p->cur = (tok){TOK_LE, start, 2};
                return PRDB_SDK_SQL_OK;
            }
            if (p->pos < p->len && p->sql[p->pos] == '>') {
                ++p->pos;
                p->cur = (tok){TOK_NE, start, 2};
                return PRDB_SDK_SQL_OK;
            }
            p->cur = (tok){TOK_LT, start, 1};
            return PRDB_SDK_SQL_OK;
        case '>':
            if (p->pos < p->len && p->sql[p->pos] == '=') {
                ++p->pos;
                p->cur = (tok){TOK_GE, start, 2};
                return PRDB_SDK_SQL_OK;
            }
            p->cur = (tok){TOK_GT, start, 1};
            return PRDB_SDK_SQL_OK;
        default: break;
    }
    p->err = PRDB_SDK_SQL_SYNTAX_ERROR;
    p->err_off = start;
    return p->err;
}

static int32_t syntax_err(sql_parser *p) {
    p->err = PRDB_SDK_SQL_SYNTAX_ERROR;
    p->err_off = p->cur.offset;
    return p->err;
}

static int32_t expect_tok(sql_parser *p, tok_kind kind) {
    if (p->cur.kind != kind) return syntax_err(p);
    return next_tok(p);
}

static int32_t expect_kw(sql_parser *p, const char *kw) {
    if (!tok_is_kw(p, kw)) return syntax_err(p);
    return next_tok(p);
}

static int32_t parse_ident_bytes(sql_parser *p, const uint8_t **out,
                                 uint32_t *out_len) {
    if (p->cur.kind == TOK_IDENT) {
        uint8_t *buf = (uint8_t *)prdb_sql_arena_alloc(p->arena, p->cur.length + 1, 1);
        if (buf == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
        for (uint32_t i = 0; i < p->cur.length; ++i) {
            uint8_t c = p->sql[p->cur.offset + i];
            if (c >= 'A' && c <= 'Z') c = (uint8_t)(c + ('a' - 'A'));
            buf[i] = c;
        }
        buf[p->cur.length] = '\0';
        *out = buf;
        *out_len = p->cur.length;
        return next_tok(p);
    }
    if (p->cur.kind == TOK_QUOTED_IDENT) {
        const uint8_t *raw = p->sql + p->cur.offset + 1;
        uint32_t raw_len = p->cur.length - 2;
        uint8_t *buf = (uint8_t *)prdb_sql_arena_alloc(p->arena, raw_len + 1, 1);
        uint32_t j = 0;
        if (buf == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
        for (uint32_t i = 0; i < raw_len; ++i) {
            if (raw[i] == '"' && i + 1 < raw_len && raw[i + 1] == '"') {
                buf[j++] = '"';
                ++i;
            } else {
                buf[j++] = raw[i];
            }
        }
        buf[j] = '\0';
        *out = buf;
        *out_len = j;
        return next_tok(p);
    }
    return syntax_err(p);
}

static int32_t unescape_string(sql_parser *p, tok t, const uint8_t **out,
                               uint32_t *out_len) {
    const uint8_t *raw = p->sql + t.offset + 1;
    uint32_t raw_len = t.length - 2;
    uint8_t *buf = (uint8_t *)prdb_sql_arena_alloc(p->arena, raw_len + 1, 1);
    uint32_t j = 0;
    if (buf == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
    for (uint32_t i = 0; i < raw_len; ++i) {
        if (raw[i] == '\'' && i + 1 < raw_len && raw[i + 1] == '\'') {
            buf[j++] = '\'';
            ++i;
        } else {
            buf[j++] = raw[i];
        }
    }
    buf[j] = '\0';
    *out = buf;
    *out_len = j;
    return PRDB_SDK_SQL_OK;
}

/* Date helper: days since 1970-01-01 */
static int parse_two_digits(const uint8_t *s, int *out) {
    if (s[0] < '0' || s[0] > '9' || s[1] < '0' || s[1] > '9') return 0;
    *out = (s[0] - '0') * 10 + (s[1] - '0');
    return 1;
}

static int parse_four_digits(const uint8_t *s, int *out) {
    int hi, lo;
    if (!parse_two_digits(s, &hi) || !parse_two_digits(s + 2, &lo)) return 0;
    *out = hi * 100 + lo;
    return 1;
}

static int32_t days_from_civil(int64_t y, unsigned m, unsigned d) {
    y -= m <= 2;
    const int64_t era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = (unsigned)(y - era * 400);
    const unsigned doy = (153 * (m + (m > 2 ? (unsigned)-3 : 9)) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return (int32_t)(era * 146097 + (int64_t)doe - 719468);
}

static int parse_date_bytes(const uint8_t *s, uint32_t len, int32_t *days_out) {
    int y, m, d;
    if (len != 10 || s[4] != '-' || s[7] != '-') return 0;
    if (!parse_four_digits(s, &y) || !parse_two_digits(s + 5, &m) ||
        !parse_two_digits(s + 8, &d)) {
        return 0;
    }
    if (m < 1 || m > 12 || d < 1 || d > 31) return 0;
    int32_t days = days_from_civil(y, (unsigned)m, (unsigned)d);
    if (days < PRDB_SDK_VALUE_DATE_MIN_DAYS || days > PRDB_SDK_VALUE_DATE_MAX_DAYS) {
        return 0;
    }
    *days_out = days;
    return 1;
}

static int parse_timestamp_bytes(const uint8_t *s, uint32_t len,
                                 int64_t *micros_out) {
    int32_t days;
    int hh = 0, mm = 0, ss = 0;
    int64_t frac = 0;
    uint32_t pos = 10;
    if (len < 10 || !parse_date_bytes(s, 10, &days)) return 0;
    if (pos < len) {
        if (s[pos] != ' ' && s[pos] != 'T' && s[pos] != 't') return 0;
        ++pos;
        if (pos + 8 > len || s[pos + 2] != ':' || s[pos + 5] != ':') return 0;
        if (!parse_two_digits(s + pos, &hh) ||
            !parse_two_digits(s + pos + 3, &mm) ||
            !parse_two_digits(s + pos + 6, &ss)) {
            return 0;
        }
        if (hh > 23 || mm > 59 || ss > 59) return 0;
        pos += 8;
        if (pos < len && s[pos] == '.') {
            ++pos;
            int64_t scale = 100000;
            if (pos >= len || s[pos] < '0' || s[pos] > '9') return 0;
            while (pos < len && s[pos] >= '0' && s[pos] <= '9') {
                if (scale > 0) {
                    frac += (int64_t)(s[pos] - '0') * scale;
                    scale /= 10;
                }
                ++pos;
            }
        }
        if (pos < len) {
            if (s[pos] == 'Z' || s[pos] == 'z') {
                ++pos;
            } else if (s[pos] == '+' || s[pos] == '-') {
                int sign = s[pos] == '+' ? 1 : -1;
                int tzh = 0, tzm = 0;
                ++pos;
                if (pos + 2 > len || !parse_two_digits(s + pos, &tzh)) return 0;
                pos += 2;
                if (pos < len && s[pos] == ':') ++pos;
                if (pos + 2 <= len && s[pos] >= '0' && s[pos] <= '9') {
                    if (!parse_two_digits(s + pos, &tzm)) return 0;
                    pos += 2;
                }
                if (tzh > 23 || tzm > 59) return 0;
                frac -= (int64_t)sign * ((int64_t)tzh * 3600 + tzm * 60) * 1000000LL;
            }
        }
        if (pos != len) return 0;
    }
    int64_t total = (int64_t)days * 86400000000LL +
                    ((int64_t)hh * 3600 + mm * 60 + ss) * 1000000LL + frac;
    if (total < PRDB_SDK_VALUE_TIMESTAMPTZ_MIN_MICROS ||
        total > PRDB_SDK_VALUE_TIMESTAMPTZ_MAX_MICROS) {
        return 0;
    }
    *micros_out = total;
    return 1;
}

/* Parse unsigned/decimal number token into int64 or 128-bit decimal. */
static int parse_number_expr(sql_parser *p, tok t, prdb_sql_expr *expr) {
    const uint8_t *s = p->sql + t.offset;
    uint32_t len = t.length;
    uint32_t dot_pos = UINT32_MAX;
    uint32_t e_pos = UINT32_MAX;
    for (uint32_t i = 0; i < len; ++i) {
        if (s[i] == '.' && dot_pos == UINT32_MAX) dot_pos = i;
        else if ((s[i] == 'e' || s[i] == 'E') && e_pos == UINT32_MAX) e_pos = i;
    }
    if (dot_pos == UINT32_MAX && e_pos == UINT32_MAX) {
        uint64_t v = 0;
        int overflow = 0;
        for (uint32_t i = 0; i < len; ++i) {
            uint64_t d = (uint64_t)(s[i] - '0');
            if (v > (UINT64_MAX - d) / 10) {
                overflow = 1;
                break;
            }
            v = v * 10 + d;
        }
        if (!overflow && v <= (uint64_t)INT64_MAX) {
            expr->kind = PRDB_SQL_EXPR_INT;
            expr->u.int_val = (int64_t)v;
            return 1;
        }
    }
    /* Parse as DECIMAL(p, s) into 128-bit big-endian */
    uint8_t mag[16] = {0};
    uint32_t mant_end = e_pos == UINT32_MAX ? len : e_pos;
    uint32_t digits = 0;
    int32_t frac_digits = 0;
    int seen_dot = 0;
    for (uint32_t i = 0; i < mant_end; ++i) {
        if (s[i] == '.') {
            seen_dot = 1;
            continue;
        }
        uint16_t carry = (uint16_t)(s[i] - '0');
        for (int k = 15; k >= 0; --k) {
            uint16_t cur = (uint16_t)mag[k] * 10 + carry;
            mag[k] = (uint8_t)(cur & 0xff);
            carry = cur >> 8;
        }
        if (carry != 0) return 0;
        ++digits;
        if (seen_dot) ++frac_digits;
    }
    if (digits == 0) return 0;
    int32_t exp_val = 0;
    if (e_pos != UINT32_MAX) {
        uint32_t i = e_pos + 1;
        int sign = 1;
        if (s[i] == '+' || s[i] == '-') {
            if (s[i] == '-') sign = -1;
            ++i;
        }
        while (i < len) {
            exp_val = exp_val * 10 + (s[i] - '0');
            if (exp_val > 100) return 0;
            ++i;
        }
        exp_val *= sign;
    }
    int32_t scale = frac_digits - exp_val;
    while (scale < 0) {
        uint16_t carry = 0;
        for (int k = 15; k >= 0; --k) {
            uint16_t cur = (uint16_t)mag[k] * 10 + carry;
            mag[k] = (uint8_t)(cur & 0xff);
            carry = cur >> 8;
        }
        if (carry != 0) return 0;
        ++scale;
        ++digits;
    }
    if (scale > 38) return 0;
    if (mag[0] & 0x80) return 0;
    uint8_t prec = (uint8_t)(digits > 38 ? 38 : (digits == 0 ? 1 : digits));
    if (prec < (uint8_t)scale) prec = (uint8_t)scale;
    if (prec == 0) prec = 1;
    expr->kind = PRDB_SQL_EXPR_DECIMAL;
    memcpy(expr->u.dec_val.bytes, mag, 16);
    expr->u.dec_val.precision = prec;
    expr->u.dec_val.scale = (uint8_t)scale;
    return 1;
}

static prdb_sql_expr *new_expr(sql_parser *p, prdb_sql_expr_kind kind) {
    prdb_sql_expr *e = (prdb_sql_expr *)prdb_sql_arena_alloc(
        p->arena, sizeof(prdb_sql_expr), 8);
    if (e == NULL) {
        p->err = PRDB_SDK_SQL_LIMIT_EXCEEDED;
        p->err_off = p->cur.offset;
        return NULL;
    }
    e->kind = kind;
    return e;
}

static int32_t parse_select_stmt(sql_parser *p, uint32_t depth,
                                 prdb_sql_select_ast **out);
static int32_t parse_expr(sql_parser *p, uint32_t depth, prdb_sql_expr **out);

static int is_select_terminator(const sql_parser *p) {
    return p->cur.kind == TOK_EOF || p->cur.kind == TOK_SEMI ||
           p->cur.kind == TOK_RPAREN || p->cur.kind == TOK_COMMA ||
           tok_is_kw(p, "FROM") || tok_is_kw(p, "WHERE") ||
           tok_is_kw(p, "GROUP") || tok_is_kw(p, "HAVING") ||
           tok_is_kw(p, "ORDER") || tok_is_kw(p, "LIMIT") ||
           tok_is_kw(p, "OFFSET") || tok_is_kw(p, "ON") ||
           tok_is_kw(p, "INNER") || tok_is_kw(p, "LEFT") ||
           tok_is_kw(p, "CROSS") || tok_is_kw(p, "JOIN") ||
           tok_is_kw(p, "SET") || tok_is_kw(p, "VALUES");
}

static int32_t parse_primary(sql_parser *p, uint32_t depth,
                             prdb_sql_expr **out) {
    if (depth > MAX_PARSER_DEPTH) {
        p->err = PRDB_SDK_SQL_LIMIT_EXCEEDED;
        p->err_off = p->cur.offset;
        return p->err;
    }
    if (p->cur.kind == TOK_LPAREN) {
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        if (tok_is_kw(p, "SELECT") || tok_is_kw(p, "WITH")) {
            prdb_sql_select_ast *sub = NULL;
            if (parse_select_stmt(p, depth + 1, &sub) != PRDB_SDK_SQL_OK)
                return p->err;
            if (expect_tok(p, TOK_RPAREN) != PRDB_SDK_SQL_OK) return p->err;
            prdb_sql_expr *e = new_expr(p, PRDB_SQL_EXPR_SUBQUERY);
            if (e == NULL) return p->err;
            e->u.subquery.subq_kind = PRDB_SQL_SUBQ_SCALAR;
            e->u.subquery.subquery = sub;
            *out = e;
            return PRDB_SDK_SQL_OK;
        }
        prdb_sql_expr *inner = NULL;
        if (parse_expr(p, depth + 1, &inner) != PRDB_SDK_SQL_OK) return p->err;
        if (expect_tok(p, TOK_RPAREN) != PRDB_SDK_SQL_OK) return p->err;
        *out = inner;
        return PRDB_SDK_SQL_OK;
    }
    if (tok_is_kw(p, "NULL")) {
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        *out = new_expr(p, PRDB_SQL_EXPR_NULL);
        return *out == NULL ? p->err : PRDB_SDK_SQL_OK;
    }
    if (tok_is_kw(p, "TRUE") || tok_is_kw(p, "FALSE")) {
        uint8_t val = tok_is_kw(p, "TRUE") ? 1 : 0;
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        prdb_sql_expr *e = new_expr(p, PRDB_SQL_EXPR_BOOL);
        if (e == NULL) return p->err;
        e->u.bool_val = val;
        *out = e;
        return PRDB_SDK_SQL_OK;
    }
    if (p->cur.kind == TOK_PARAM) {
        uint64_t idx = 0;
        for (uint32_t i = 1; i < p->cur.length; ++i) {
            idx = idx * 10 + (p->sql[p->cur.offset + i] - '0');
            if (idx > PRDB_SDK_SQL_MAX_PARAMETER_INDEX) {
                p->err = PRDB_SDK_SQL_LIMIT_EXCEEDED;
                p->err_off = p->cur.offset;
                return p->err;
            }
        }
        if (idx == 0) return syntax_err(p);
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        prdb_sql_expr *e = new_expr(p, PRDB_SQL_EXPR_PARAMETER);
        if (e == NULL) return p->err;
        e->u.param_index = (uint32_t)idx;
        *out = e;
        return PRDB_SDK_SQL_OK;
    }
    if (p->cur.kind == TOK_NUMBER) {
        prdb_sql_expr *e = new_expr(p, PRDB_SQL_EXPR_INT);
        if (e == NULL) return p->err;
        if (!parse_number_expr(p, p->cur, e)) return syntax_err(p);
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        *out = e;
        return PRDB_SDK_SQL_OK;
    }
    if (p->cur.kind == TOK_STRING) {
        const uint8_t *s = NULL;
        uint32_t slen = 0;
        if (unescape_string(p, p->cur, &s, &slen) != PRDB_SDK_SQL_OK)
            return p->err;
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        prdb_sql_expr *e = new_expr(p, PRDB_SQL_EXPR_STRING);
        if (e == NULL) return p->err;
        e->u.str_val.data = s;
        e->u.str_val.len = slen;
        *out = e;
        return PRDB_SDK_SQL_OK;
    }
    if (tok_is_kw(p, "DATE")) {
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        if (p->cur.kind != TOK_STRING) return syntax_err(p);
        const uint8_t *s = NULL;
        uint32_t slen = 0;
        int32_t days = 0;
        if (unescape_string(p, p->cur, &s, &slen) != PRDB_SDK_SQL_OK)
            return p->err;
        if (!parse_date_bytes(s, slen, &days)) return syntax_err(p);
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        prdb_sql_expr *e = new_expr(p, PRDB_SQL_EXPR_DATE);
        if (e == NULL) return p->err;
        e->u.date_days = days;
        *out = e;
        return PRDB_SDK_SQL_OK;
    }
    if (tok_is_kw(p, "TIMESTAMP") || tok_is_kw(p, "TIMESTAMPTZ")) {
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        if (p->cur.kind != TOK_STRING) return syntax_err(p);
        const uint8_t *s = NULL;
        uint32_t slen = 0;
        int64_t micros = 0;
        if (unescape_string(p, p->cur, &s, &slen) != PRDB_SDK_SQL_OK)
            return p->err;
        if (!parse_timestamp_bytes(s, slen, &micros)) return syntax_err(p);
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        prdb_sql_expr *e = new_expr(p, PRDB_SQL_EXPR_TIMESTAMP);
        if (e == NULL) return p->err;
        e->u.ts_micros = micros;
        *out = e;
        return PRDB_SDK_SQL_OK;
    }
    if (tok_is_kw(p, "INTERVAL")) {
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        if (p->cur.kind != TOK_STRING) return syntax_err(p);
        const uint8_t *s = NULL;
        uint32_t slen = 0;
        if (unescape_string(p, p->cur, &s, &slen) != PRDB_SDK_SQL_OK)
            return p->err;
        int64_t amt = 0;
        int sign = 1;
        uint32_t i = 0;
        if (i < slen && s[i] == '-') { sign = -1; ++i; }
        else if (i < slen && s[i] == '+') { ++i; }
        if (i >= slen) return syntax_err(p);
        while (i < slen) {
            if (s[i] < '0' || s[i] > '9') return syntax_err(p);
            amt = amt * 10 + (s[i] - '0');
            ++i;
        }
        amt *= sign;
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        prdb_sql_interval_unit unit;
        if (tok_is_kw(p, "DAY") || tok_is_kw(p, "DAYS"))
            unit = PRDB_SQL_INTERVAL_DAY;
        else if (tok_is_kw(p, "MONTH") || tok_is_kw(p, "MONTHS"))
            unit = PRDB_SQL_INTERVAL_MONTH;
        else if (tok_is_kw(p, "YEAR") || tok_is_kw(p, "YEARS"))
            unit = PRDB_SQL_INTERVAL_YEAR;
        else
            return syntax_err(p);
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        prdb_sql_expr *e = new_expr(p, PRDB_SQL_EXPR_INTERVAL);
        if (e == NULL) return p->err;
        e->u.interval_val.amount = amt;
        e->u.interval_val.unit = unit;
        *out = e;
        return PRDB_SDK_SQL_OK;
    }
    if (tok_is_kw(p, "EXISTS")) {
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        if (expect_tok(p, TOK_LPAREN) != PRDB_SDK_SQL_OK) return p->err;
        prdb_sql_select_ast *sub = NULL;
        if (parse_select_stmt(p, depth + 1, &sub) != PRDB_SDK_SQL_OK)
            return p->err;
        if (expect_tok(p, TOK_RPAREN) != PRDB_SDK_SQL_OK) return p->err;
        prdb_sql_expr *e = new_expr(p, PRDB_SQL_EXPR_SUBQUERY);
        if (e == NULL) return p->err;
        e->u.subquery.subq_kind = PRDB_SQL_SUBQ_EXISTS;
        e->u.subquery.subquery = sub;
        *out = e;
        return PRDB_SDK_SQL_OK;
    }
    if (tok_is_kw(p, "CASE")) {
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        prdb_sql_expr *operand = NULL;
        if (!tok_is_kw(p, "WHEN")) {
            if (parse_expr(p, depth + 1, &operand) != PRDB_SDK_SQL_OK)
                return p->err;
        }
        uint32_t cap = 8, count = 0;
        prdb_sql_expr **whens = (prdb_sql_expr **)prdb_sql_arena_alloc(
            p->arena, cap * sizeof(*whens), 8);
        prdb_sql_expr **thens = (prdb_sql_expr **)prdb_sql_arena_alloc(
            p->arena, cap * sizeof(*thens), 8);
        if (whens == NULL || thens == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
        while (tok_is_kw(p, "WHEN")) {
            if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
            prdb_sql_expr *w = NULL, *t = NULL;
            if (parse_expr(p, depth + 1, &w) != PRDB_SDK_SQL_OK) return p->err;
            if (expect_kw(p, "THEN") != PRDB_SDK_SQL_OK) return p->err;
            if (parse_expr(p, depth + 1, &t) != PRDB_SDK_SQL_OK) return p->err;
            if (count == cap) {
                uint32_t ncap = cap * 2;
                prdb_sql_expr **nw = (prdb_sql_expr **)prdb_sql_arena_alloc(
                    p->arena, ncap * sizeof(*nw), 8);
                prdb_sql_expr **nt = (prdb_sql_expr **)prdb_sql_arena_alloc(
                    p->arena, ncap * sizeof(*nt), 8);
                if (nw == NULL || nt == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
                memcpy(nw, whens, count * sizeof(*nw));
                memcpy(nt, thens, count * sizeof(*nt));
                whens = nw;
                thens = nt;
                cap = ncap;
            }
            whens[count] = w;
            thens[count] = t;
            ++count;
        }
        if (count == 0) return syntax_err(p);
        prdb_sql_expr *else_e = NULL;
        if (tok_is_kw(p, "ELSE")) {
            if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
            if (parse_expr(p, depth + 1, &else_e) != PRDB_SDK_SQL_OK)
                return p->err;
        }
        if (expect_kw(p, "END") != PRDB_SDK_SQL_OK) return p->err;
        prdb_sql_expr *e = new_expr(p, PRDB_SQL_EXPR_CASE);
        if (e == NULL) return p->err;
        e->u.case_expr.operand = operand;
        e->u.case_expr.when_exprs = whens;
        e->u.case_expr.then_exprs = thens;
        e->u.case_expr.branch_count = count;
        e->u.case_expr.else_expr = else_e;
        *out = e;
        return PRDB_SDK_SQL_OK;
    }
    if (tok_is_kw(p, "EXTRACT")) {
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        if (expect_tok(p, TOK_LPAREN) != PRDB_SDK_SQL_OK) return p->err;
        prdb_sql_interval_unit field;
        if (tok_is_kw(p, "YEAR")) field = PRDB_SQL_INTERVAL_YEAR;
        else if (tok_is_kw(p, "MONTH")) field = PRDB_SQL_INTERVAL_MONTH;
        else if (tok_is_kw(p, "DAY")) field = PRDB_SQL_INTERVAL_DAY;
        else return syntax_err(p);
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        if (expect_kw(p, "FROM") != PRDB_SDK_SQL_OK) return p->err;
        prdb_sql_expr *arg = NULL;
        if (parse_expr(p, depth + 1, &arg) != PRDB_SDK_SQL_OK) return p->err;
        if (expect_tok(p, TOK_RPAREN) != PRDB_SDK_SQL_OK) return p->err;
        prdb_sql_expr *e = new_expr(p, PRDB_SQL_EXPR_EXTRACT);
        if (e == NULL) return p->err;
        e->u.extract.field = field;
        e->u.extract.arg = arg;
        *out = e;
        return PRDB_SDK_SQL_OK;
    }
    if (tok_is_kw(p, "SUBSTRING") || tok_is_kw(p, "SUBSTR")) {
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        if (expect_tok(p, TOK_LPAREN) != PRDB_SDK_SQL_OK) return p->err;
        prdb_sql_expr *str = NULL, *start = NULL, *slen = NULL;
        if (parse_expr(p, depth + 1, &str) != PRDB_SDK_SQL_OK) return p->err;
        if (tok_is_kw(p, "FROM")) {
            if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
            if (parse_expr(p, depth + 1, &start) != PRDB_SDK_SQL_OK)
                return p->err;
            if (tok_is_kw(p, "FOR")) {
                if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
                if (parse_expr(p, depth + 1, &slen) != PRDB_SDK_SQL_OK)
                    return p->err;
            }
        } else {
            if (expect_tok(p, TOK_COMMA) != PRDB_SDK_SQL_OK) return p->err;
            if (parse_expr(p, depth + 1, &start) != PRDB_SDK_SQL_OK)
                return p->err;
            if (p->cur.kind == TOK_COMMA) {
                if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
                if (parse_expr(p, depth + 1, &slen) != PRDB_SDK_SQL_OK)
                    return p->err;
            }
        }
        if (expect_tok(p, TOK_RPAREN) != PRDB_SDK_SQL_OK) return p->err;
        prdb_sql_expr *e = new_expr(p, PRDB_SQL_EXPR_SUBSTRING);
        if (e == NULL) return p->err;
        e->u.substring.str = str;
        e->u.substring.start = start;
        e->u.substring.len = slen;
        *out = e;
        return PRDB_SDK_SQL_OK;
    }
    if (tok_is_kw(p, "COALESCE")) {
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        if (expect_tok(p, TOK_LPAREN) != PRDB_SDK_SQL_OK) return p->err;
        uint32_t cap = 4, count = 0;
        prdb_sql_expr **args = (prdb_sql_expr **)prdb_sql_arena_alloc(
            p->arena, cap * sizeof(*args), 8);
        if (args == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
        for (;;) {
            prdb_sql_expr *a = NULL;
            if (parse_expr(p, depth + 1, &a) != PRDB_SDK_SQL_OK) return p->err;
            if (count == cap) {
                uint32_t ncap = cap * 2;
                prdb_sql_expr **na = (prdb_sql_expr **)prdb_sql_arena_alloc(
                    p->arena, ncap * sizeof(*na), 8);
                if (na == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
                memcpy(na, args, count * sizeof(*na));
                args = na;
                cap = ncap;
            }
            args[count++] = a;
            if (p->cur.kind != TOK_COMMA) break;
            if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        }
        if (expect_tok(p, TOK_RPAREN) != PRDB_SDK_SQL_OK) return p->err;
        prdb_sql_expr *e = new_expr(p, PRDB_SQL_EXPR_COALESCE);
        if (e == NULL) return p->err;
        e->u.coalesce.args = args;
        e->u.coalesce.arg_count = count;
        *out = e;
        return PRDB_SDK_SQL_OK;
    }
    if (tok_is_kw(p, "COUNT") || tok_is_kw(p, "SUM") ||
        tok_is_kw(p, "AVG") || tok_is_kw(p, "MIN") || tok_is_kw(p, "MAX")) {
        uint32_t agg_kind = 0;
        if (tok_is_kw(p, "COUNT")) agg_kind = PRDB_SDK_SQL_AGGREGATE_COUNT_COLUMN;
        else if (tok_is_kw(p, "SUM")) agg_kind = PRDB_SDK_SQL_AGGREGATE_SUM;
        else if (tok_is_kw(p, "AVG")) agg_kind = PRDB_SDK_SQL_AGGREGATE_AVG;
        else if (tok_is_kw(p, "MIN")) agg_kind = PRDB_SDK_SQL_AGGREGATE_MIN;
        else agg_kind = PRDB_SDK_SQL_AGGREGATE_MAX;
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        if (expect_tok(p, TOK_LPAREN) != PRDB_SDK_SQL_OK) return p->err;
        uint8_t distinct = 0;
        prdb_sql_expr *arg = NULL;
        if (p->cur.kind == TOK_STAR) {
            if (agg_kind != PRDB_SDK_SQL_AGGREGATE_COUNT_COLUMN)
                return syntax_err(p);
            agg_kind = PRDB_SDK_SQL_AGGREGATE_COUNT_STAR;
            if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        } else {
            if (tok_is_kw(p, "DISTINCT")) {
                distinct = 1;
                if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
            }
            if (parse_expr(p, depth + 1, &arg) != PRDB_SDK_SQL_OK)
                return p->err;
        }
        if (expect_tok(p, TOK_RPAREN) != PRDB_SDK_SQL_OK) return p->err;
        prdb_sql_expr *e = new_expr(p, PRDB_SQL_EXPR_AGGREGATE);
        if (e == NULL) return p->err;
        e->u.agg.agg_kind = agg_kind;
        e->u.agg.distinct = distinct;
        e->u.agg.arg = arg;
        *out = e;
        return PRDB_SDK_SQL_OK;
    }
    if (p->cur.kind == TOK_IDENT || p->cur.kind == TOK_QUOTED_IDENT) {
        const uint8_t *id1 = NULL, *id2 = NULL;
        uint32_t len1 = 0, len2 = 0;
        if (parse_ident_bytes(p, &id1, &len1) != PRDB_SDK_SQL_OK)
            return p->err;
        if (p->cur.kind == TOK_DOT) {
            if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
            if (parse_ident_bytes(p, &id2, &len2) != PRDB_SDK_SQL_OK)
                return p->err;
        }
        prdb_sql_expr *e = new_expr(p, PRDB_SQL_EXPR_COLUMN);
        if (e == NULL) return p->err;
        if (id2 != NULL) {
            e->u.col.table_name = id1;
            e->u.col.table_name_len = len1;
            e->u.col.col_name = id2;
            e->u.col.col_name_len = len2;
        } else {
            e->u.col.col_name = id1;
            e->u.col.col_name_len = len1;
        }
        *out = e;
        return PRDB_SDK_SQL_OK;
    }
    return syntax_err(p);
}

static int32_t parse_unary(sql_parser *p, uint32_t depth, prdb_sql_expr **out) {
    if (p->cur.kind == TOK_MINUS || p->cur.kind == TOK_PLUS) {
        prdb_sql_unary_op op = p->cur.kind == TOK_MINUS ? PRDB_SQL_UOP_NEG
                                                        : PRDB_SQL_UOP_POS;
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        prdb_sql_expr *operand = NULL;
        if (parse_unary(p, depth + 1, &operand) != PRDB_SDK_SQL_OK)
            return p->err;
        if (op == PRDB_SQL_UOP_POS) {
            *out = operand;
            return PRDB_SDK_SQL_OK;
        }
        prdb_sql_expr *e = new_expr(p, PRDB_SQL_EXPR_UNARY);
        if (e == NULL) return p->err;
        e->u.unary.op = op;
        e->u.unary.operand = operand;
        *out = e;
        return PRDB_SDK_SQL_OK;
    }
    return parse_primary(p, depth, out);
}

static int32_t parse_mul(sql_parser *p, uint32_t depth, prdb_sql_expr **out) {
    if (parse_unary(p, depth, out) != PRDB_SDK_SQL_OK) return p->err;
    while (p->cur.kind == TOK_STAR || p->cur.kind == TOK_SLASH ||
           p->cur.kind == TOK_PERCENT) {
        prdb_sql_bin_op op = p->cur.kind == TOK_STAR ? PRDB_SQL_BOP_MUL
                             : p->cur.kind == TOK_SLASH ? PRDB_SQL_BOP_DIV
                                                        : PRDB_SQL_BOP_MOD;
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        prdb_sql_expr *rhs = NULL;
        if (parse_unary(p, depth, &rhs) != PRDB_SDK_SQL_OK) return p->err;
        prdb_sql_expr *e = new_expr(p, PRDB_SQL_EXPR_BINARY);
        if (e == NULL) return p->err;
        e->u.binary.op = op;
        e->u.binary.left = *out;
        e->u.binary.right = rhs;
        *out = e;
    }
    return PRDB_SDK_SQL_OK;
}

static int32_t parse_add(sql_parser *p, uint32_t depth, prdb_sql_expr **out) {
    if (parse_mul(p, depth, out) != PRDB_SDK_SQL_OK) return p->err;
    while (p->cur.kind == TOK_PLUS || p->cur.kind == TOK_MINUS) {
        prdb_sql_bin_op op =
            p->cur.kind == TOK_PLUS ? PRDB_SQL_BOP_ADD : PRDB_SQL_BOP_SUB;
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        prdb_sql_expr *rhs = NULL;
        if (parse_mul(p, depth, &rhs) != PRDB_SDK_SQL_OK) return p->err;
        prdb_sql_expr *e = new_expr(p, PRDB_SQL_EXPR_BINARY);
        if (e == NULL) return p->err;
        e->u.binary.op = op;
        e->u.binary.left = *out;
        e->u.binary.right = rhs;
        *out = e;
    }
    return PRDB_SDK_SQL_OK;
}

static int32_t parse_predicate(sql_parser *p, uint32_t depth,
                               prdb_sql_expr **out) {
    if (parse_add(p, depth, out) != PRDB_SDK_SQL_OK) return p->err;

    for (;;) {
        if (tok_is_kw(p, "IS")) {
            uint8_t is_not = 0;
            if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
            if (tok_is_kw(p, "NOT")) {
                is_not = 1;
                if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
            }
            if (expect_kw(p, "NULL") != PRDB_SDK_SQL_OK) return p->err;
            prdb_sql_expr *e = new_expr(p, PRDB_SQL_EXPR_UNARY);
            if (e == NULL) return p->err;
            e->u.unary.op =
                is_not ? PRDB_SQL_UOP_IS_NOT_NULL : PRDB_SQL_UOP_IS_NULL;
            e->u.unary.operand = *out;
            *out = e;
            continue;
        }
        uint8_t negated = 0;
        if (tok_is_kw(p, "NOT")) {
            negated = 1;
            if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
            if (!tok_is_kw(p, "IN") && !tok_is_kw(p, "BETWEEN") &&
                !tok_is_kw(p, "LIKE")) {
                return syntax_err(p);
            }
        }
        if (tok_is_kw(p, "BETWEEN")) {
            if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
            prdb_sql_expr *low = NULL, *high = NULL;
            if (parse_add(p, depth, &low) != PRDB_SDK_SQL_OK) return p->err;
            if (expect_kw(p, "AND") != PRDB_SDK_SQL_OK) return p->err;
            if (parse_add(p, depth, &high) != PRDB_SDK_SQL_OK) return p->err;
            prdb_sql_expr *e = new_expr(p, PRDB_SQL_EXPR_BETWEEN);
            if (e == NULL) return p->err;
            e->u.between.expr = *out;
            e->u.between.low = low;
            e->u.between.high = high;
            e->u.between.negated = negated;
            *out = e;
            continue;
        }
        if (tok_is_kw(p, "LIKE")) {
            if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
            prdb_sql_expr *pat = NULL;
            if (parse_add(p, depth, &pat) != PRDB_SDK_SQL_OK) return p->err;
            prdb_sql_expr *e = new_expr(p, PRDB_SQL_EXPR_BINARY);
            if (e == NULL) return p->err;
            e->u.binary.op =
                negated ? PRDB_SQL_BOP_NOT_LIKE : PRDB_SQL_BOP_LIKE;
            e->u.binary.left = *out;
            e->u.binary.right = pat;
            *out = e;
            continue;
        }
        if (tok_is_kw(p, "IN")) {
            if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
            if (expect_tok(p, TOK_LPAREN) != PRDB_SDK_SQL_OK) return p->err;
            if (tok_is_kw(p, "SELECT") || tok_is_kw(p, "WITH")) {
                prdb_sql_select_ast *sub = NULL;
                if (parse_select_stmt(p, depth + 1, &sub) != PRDB_SDK_SQL_OK)
                    return p->err;
                if (expect_tok(p, TOK_RPAREN) != PRDB_SDK_SQL_OK) return p->err;
                prdb_sql_expr *e = new_expr(p, PRDB_SQL_EXPR_SUBQUERY);
                if (e == NULL) return p->err;
                e->u.subquery.subq_kind =
                    negated ? PRDB_SQL_SUBQ_NOT_IN : PRDB_SQL_SUBQ_IN;
                e->u.subquery.lhs = *out;
                e->u.subquery.subquery = sub;
                *out = e;
                continue;
            }
            uint32_t cap = 8, count = 0;
            prdb_sql_expr **items = (prdb_sql_expr **)prdb_sql_arena_alloc(
                p->arena, cap * sizeof(*items), 8);
            if (items == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
            for (;;) {
                prdb_sql_expr *item = NULL;
                if (parse_expr(p, depth + 1, &item) != PRDB_SDK_SQL_OK)
                    return p->err;
                if (count == cap) {
                    uint32_t ncap = cap * 2;
                    prdb_sql_expr **ni = (prdb_sql_expr **)prdb_sql_arena_alloc(
                        p->arena, ncap * sizeof(*ni), 8);
                    if (ni == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
                    memcpy(ni, items, count * sizeof(*ni));
                    items = ni;
                    cap = ncap;
                }
                items[count++] = item;
                if (p->cur.kind != TOK_COMMA) break;
                if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
            }
            if (expect_tok(p, TOK_RPAREN) != PRDB_SDK_SQL_OK) return p->err;
            prdb_sql_expr *e = new_expr(p, PRDB_SQL_EXPR_IN_LIST);
            if (e == NULL) return p->err;
            e->u.in_list.expr = *out;
            e->u.in_list.items = items;
            e->u.in_list.item_count = count;
            e->u.in_list.negated = negated;
            *out = e;
            continue;
        }
        if (negated) return syntax_err(p);
        if (p->cur.kind == TOK_EQ || p->cur.kind == TOK_NE ||
            p->cur.kind == TOK_LT || p->cur.kind == TOK_LE ||
            p->cur.kind == TOK_GT || p->cur.kind == TOK_GE) {
            prdb_sql_bin_op op;
            switch (p->cur.kind) {
                case TOK_EQ: op = PRDB_SQL_BOP_EQ; break;
                case TOK_NE: op = PRDB_SQL_BOP_NE; break;
                case TOK_LT: op = PRDB_SQL_BOP_LT; break;
                case TOK_LE: op = PRDB_SQL_BOP_LE; break;
                case TOK_GT: op = PRDB_SQL_BOP_GT; break;
                default: op = PRDB_SQL_BOP_GE; break;
            }
            if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
            prdb_sql_expr *rhs = NULL;
            if (parse_add(p, depth, &rhs) != PRDB_SDK_SQL_OK) return p->err;
            prdb_sql_expr *e = new_expr(p, PRDB_SQL_EXPR_BINARY);
            if (e == NULL) return p->err;
            e->u.binary.op = op;
            e->u.binary.left = *out;
            e->u.binary.right = rhs;
            *out = e;
            continue;
        }
        break;
    }
    return PRDB_SDK_SQL_OK;
}

static int32_t parse_not(sql_parser *p, uint32_t depth, prdb_sql_expr **out) {
    if (tok_is_kw(p, "NOT")) {
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        if (tok_is_kw(p, "EXISTS")) {
            prdb_sql_expr *sub = NULL;
            if (parse_primary(p, depth + 1, &sub) != PRDB_SDK_SQL_OK)
                return p->err;
            sub->u.subquery.subq_kind = PRDB_SQL_SUBQ_NOT_EXISTS;
            *out = sub;
            return PRDB_SDK_SQL_OK;
        }
        prdb_sql_expr *operand = NULL;
        if (parse_not(p, depth + 1, &operand) != PRDB_SDK_SQL_OK)
            return p->err;
        prdb_sql_expr *e = new_expr(p, PRDB_SQL_EXPR_UNARY);
        if (e == NULL) return p->err;
        e->u.unary.op = PRDB_SQL_UOP_NOT;
        e->u.unary.operand = operand;
        *out = e;
        return PRDB_SDK_SQL_OK;
    }
    return parse_predicate(p, depth, out);
}

static int32_t parse_and(sql_parser *p, uint32_t depth, prdb_sql_expr **out) {
    if (parse_not(p, depth, out) != PRDB_SDK_SQL_OK) return p->err;
    while (tok_is_kw(p, "AND")) {
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        prdb_sql_expr *rhs = NULL;
        if (parse_not(p, depth, &rhs) != PRDB_SDK_SQL_OK) return p->err;
        prdb_sql_expr *e = new_expr(p, PRDB_SQL_EXPR_BINARY);
        if (e == NULL) return p->err;
        e->u.binary.op = PRDB_SQL_BOP_AND;
        e->u.binary.left = *out;
        e->u.binary.right = rhs;
        *out = e;
    }
    return PRDB_SDK_SQL_OK;
}

static int32_t parse_expr(sql_parser *p, uint32_t depth, prdb_sql_expr **out) {
    if (parse_and(p, depth, out) != PRDB_SDK_SQL_OK) return p->err;
    while (tok_is_kw(p, "OR")) {
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        prdb_sql_expr *rhs = NULL;
        if (parse_and(p, depth, &rhs) != PRDB_SDK_SQL_OK) return p->err;
        prdb_sql_expr *e = new_expr(p, PRDB_SQL_EXPR_BINARY);
        if (e == NULL) return p->err;
        e->u.binary.op = PRDB_SQL_BOP_OR;
        e->u.binary.left = *out;
        e->u.binary.right = rhs;
        *out = e;
    }
    return PRDB_SDK_SQL_OK;
}

static int is_alias_start(const sql_parser *p) {
    return !is_select_terminator(p) &&
           (tok_is_kw(p, "AS") || p->cur.kind == TOK_IDENT ||
            p->cur.kind == TOK_QUOTED_IDENT);
}

static int32_t parse_table_factor(sql_parser *p, uint32_t depth,
                                  prdb_sql_table_ref **out) {
    prdb_sql_table_ref *tref = (prdb_sql_table_ref *)prdb_sql_arena_alloc(
        p->arena, sizeof(prdb_sql_table_ref), 8);
    if (tref == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
    if (p->cur.kind == TOK_LPAREN) {
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        prdb_sql_select_ast *sub = NULL;
        if (parse_select_stmt(p, depth + 1, &sub) != PRDB_SDK_SQL_OK)
            return p->err;
        if (expect_tok(p, TOK_RPAREN) != PRDB_SDK_SQL_OK) return p->err;
        tref->kind = PRDB_SQL_TREF_SUBQUERY;
        tref->u.subq.subquery = sub;
        if (tok_is_kw(p, "AS")) {
            if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        }
        if (p->cur.kind == TOK_IDENT || p->cur.kind == TOK_QUOTED_IDENT) {
            if (parse_ident_bytes(p, &tref->u.subq.alias,
                                  &tref->u.subq.alias_len) != PRDB_SDK_SQL_OK)
                return p->err;
        } else {
            return syntax_err(p);
        }
        if (p->cur.kind == TOK_LPAREN) {
            uint32_t cap = 8, count = 0;
            const uint8_t **cols = (const uint8_t **)prdb_sql_arena_alloc(
                p->arena, cap * sizeof(*cols), 8);
            uint32_t *lens = (uint32_t *)prdb_sql_arena_alloc(
                p->arena, cap * sizeof(*lens), 4);
            if (cols == NULL || lens == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
            if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
            for (;;) {
                if (count == cap) {
                    uint32_t ncap = cap * 2;
                    const uint8_t **nc = (const uint8_t **)prdb_sql_arena_alloc(
                        p->arena, ncap * sizeof(*nc), 8);
                    uint32_t *nl = (uint32_t *)prdb_sql_arena_alloc(
                        p->arena, ncap * sizeof(*nl), 4);
                    if (nc == NULL || nl == NULL)
                        return PRDB_SDK_SQL_LIMIT_EXCEEDED;
                    memcpy(nc, cols, count * sizeof(*nc));
                    memcpy(nl, lens, count * sizeof(*nl));
                    cols = nc;
                    lens = nl;
                    cap = ncap;
                }
                if (parse_ident_bytes(p, &cols[count], &lens[count]) !=
                    PRDB_SDK_SQL_OK)
                    return p->err;
                ++count;
                if (p->cur.kind != TOK_COMMA) break;
                if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
            }
            if (expect_tok(p, TOK_RPAREN) != PRDB_SDK_SQL_OK) return p->err;
            tref->u.subq.col_aliases = cols;
            tref->u.subq.col_alias_lens = lens;
            tref->u.subq.col_alias_count = count;
        }
        *out = tref;
        return PRDB_SDK_SQL_OK;
    }

    const uint8_t *id1 = NULL, *id2 = NULL;
    uint32_t len1 = 0, len2 = 0;
    if (parse_ident_bytes(p, &id1, &len1) != PRDB_SDK_SQL_OK) return p->err;
    if (p->cur.kind == TOK_DOT) {
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        if (parse_ident_bytes(p, &id2, &len2) != PRDB_SDK_SQL_OK) return p->err;
    }
    tref->kind = PRDB_SQL_TREF_BASE;
    if (id2 != NULL) {
        tref->u.base.schema = id1;
        tref->u.base.schema_len = len1;
        tref->u.base.name = id2;
        tref->u.base.name_len = len2;
    } else {
        tref->u.base.name = id1;
        tref->u.base.name_len = len1;
    }
    if (tok_is_kw(p, "AS")) {
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        if (parse_ident_bytes(p, &tref->u.base.alias,
                              &tref->u.base.alias_len) != PRDB_SDK_SQL_OK)
            return p->err;
    } else if ((p->cur.kind == TOK_IDENT || p->cur.kind == TOK_QUOTED_IDENT) &&
               !is_select_terminator(p)) {
        if (parse_ident_bytes(p, &tref->u.base.alias,
                              &tref->u.base.alias_len) != PRDB_SDK_SQL_OK)
            return p->err;
    }
    *out = tref;
    return PRDB_SDK_SQL_OK;
}

static int32_t parse_from_clause(sql_parser *p, uint32_t depth,
                                 prdb_sql_table_ref **out) {
    if (parse_table_factor(p, depth, out) != PRDB_SDK_SQL_OK) return p->err;
    for (;;) {
        prdb_sql_join_kind jkind = 0;
        if (p->cur.kind == TOK_COMMA) {
            jkind = PRDB_SQL_JOIN_CROSS;
            if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        } else if (tok_is_kw(p, "JOIN")) {
            jkind = PRDB_SQL_JOIN_INNER;
            if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        } else if (tok_is_kw(p, "INNER")) {
            jkind = PRDB_SQL_JOIN_INNER;
            if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
            if (expect_kw(p, "JOIN") != PRDB_SDK_SQL_OK) return p->err;
        } else if (tok_is_kw(p, "CROSS")) {
            jkind = PRDB_SQL_JOIN_CROSS;
            if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
            if (expect_kw(p, "JOIN") != PRDB_SDK_SQL_OK) return p->err;
        } else if (tok_is_kw(p, "LEFT")) {
            jkind = PRDB_SQL_JOIN_LEFT_OUTER;
            if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
            if (tok_is_kw(p, "OUTER")) {
                if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
            }
            if (expect_kw(p, "JOIN") != PRDB_SDK_SQL_OK) return p->err;
        } else {
            break;
        }
        prdb_sql_table_ref *rhs = NULL;
        if (parse_table_factor(p, depth, &rhs) != PRDB_SDK_SQL_OK) return p->err;
        prdb_sql_expr *on_cond = NULL;
        if (jkind != PRDB_SQL_JOIN_CROSS) {
            if (expect_kw(p, "ON") != PRDB_SDK_SQL_OK) return p->err;
            if (parse_expr(p, depth + 1, &on_cond) != PRDB_SDK_SQL_OK)
                return p->err;
        }
        prdb_sql_table_ref *j = (prdb_sql_table_ref *)prdb_sql_arena_alloc(
            p->arena, sizeof(prdb_sql_table_ref), 8);
        if (j == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
        j->kind = PRDB_SQL_TREF_JOIN;
        j->u.join.join_kind = jkind;
        j->u.join.left = *out;
        j->u.join.right = rhs;
        j->u.join.on_cond = on_cond;
        *out = j;
    }
    return PRDB_SDK_SQL_OK;
}

static int32_t parse_u64_tok(sql_parser *p, uint64_t *out) {
    if (p->cur.kind != TOK_NUMBER) return syntax_err(p);
    uint64_t v = 0;
    for (uint32_t i = 0; i < p->cur.length; ++i) {
        uint8_t c = p->sql[p->cur.offset + i];
        if (c < '0' || c > '9') return syntax_err(p);
        v = v * 10 + (uint64_t)(c - '0');
    }
    *out = v;
    return next_tok(p);
}

static int32_t parse_select_stmt(sql_parser *p, uint32_t depth,
                                 prdb_sql_select_ast **out) {
    if (depth > MAX_PARSER_DEPTH) {
        p->err = PRDB_SDK_SQL_LIMIT_EXCEEDED;
        p->err_off = p->cur.offset;
        return p->err;
    }
    prdb_sql_select_ast *sel = (prdb_sql_select_ast *)prdb_sql_arena_alloc(
        p->arena, sizeof(prdb_sql_select_ast), 8);
    if (sel == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;

    if (tok_is_kw(p, "WITH")) {
        uint32_t cap = 4, count = 0;
        prdb_sql_cte *ctes = (prdb_sql_cte *)prdb_sql_arena_alloc(
            p->arena, cap * sizeof(*ctes), 8);
        if (ctes == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        for (;;) {
            if (count == cap) {
                uint32_t ncap = cap * 2;
                prdb_sql_cte *nc = (prdb_sql_cte *)prdb_sql_arena_alloc(
                    p->arena, ncap * sizeof(*nc), 8);
                if (nc == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
                memcpy(nc, ctes, count * sizeof(*nc));
                ctes = nc;
                cap = ncap;
            }
            prdb_sql_cte *cte = &ctes[count++];
            if (parse_ident_bytes(p, &cte->name, &cte->name_len) !=
                PRDB_SDK_SQL_OK)
                return p->err;
            if (p->cur.kind == TOK_LPAREN) {
                uint32_t ccap = 8, ccnt = 0;
                const uint8_t **cnames = (const uint8_t **)prdb_sql_arena_alloc(
                    p->arena, ccap * sizeof(*cnames), 8);
                uint32_t *clens = (uint32_t *)prdb_sql_arena_alloc(
                    p->arena, ccap * sizeof(*clens), 4);
                if (cnames == NULL || clens == NULL)
                    return PRDB_SDK_SQL_LIMIT_EXCEEDED;
                if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
                for (;;) {
                    if (ccnt == ccap) {
                        uint32_t ncap = ccap * 2;
                        const uint8_t **ncn =
                            (const uint8_t **)prdb_sql_arena_alloc(
                                p->arena, ncap * sizeof(*ncn), 8);
                        uint32_t *ncl = (uint32_t *)prdb_sql_arena_alloc(
                            p->arena, ncap * sizeof(*ncl), 4);
                        if (ncn == NULL || ncl == NULL)
                            return PRDB_SDK_SQL_LIMIT_EXCEEDED;
                        memcpy(ncn, cnames, ccnt * sizeof(*ncn));
                        memcpy(ncl, clens, ccnt * sizeof(*ncl));
                        cnames = ncn;
                        clens = ncl;
                        ccap = ncap;
                    }
                    if (parse_ident_bytes(p, &cnames[ccnt], &clens[ccnt]) !=
                        PRDB_SDK_SQL_OK)
                        return p->err;
                    ++ccnt;
                    if (p->cur.kind != TOK_COMMA) break;
                    if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
                }
                if (expect_tok(p, TOK_RPAREN) != PRDB_SDK_SQL_OK) return p->err;
                cte->col_names = cnames;
                cte->col_name_lens = clens;
                cte->col_name_count = ccnt;
            }
            if (expect_kw(p, "AS") != PRDB_SDK_SQL_OK) return p->err;
            if (expect_tok(p, TOK_LPAREN) != PRDB_SDK_SQL_OK) return p->err;
            if (parse_select_stmt(p, depth + 1, &cte->query) != PRDB_SDK_SQL_OK)
                return p->err;
            if (expect_tok(p, TOK_RPAREN) != PRDB_SDK_SQL_OK) return p->err;
            if (p->cur.kind != TOK_COMMA) break;
            if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        }
        sel->ctes = ctes;
        sel->cte_count = count;
    }

    if (expect_kw(p, "SELECT") != PRDB_SDK_SQL_OK) return p->err;
    if (tok_is_kw(p, "DISTINCT")) {
        sel->distinct = 1;
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
    } else if (tok_is_kw(p, "ALL")) {
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
    }

    {
        uint32_t cap = 16, count = 0;
        prdb_sql_select_item *items =
            (prdb_sql_select_item *)prdb_sql_arena_alloc(
                p->arena, cap * sizeof(*items), 8);
        if (items == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
        for (;;) {
            if (count == cap) {
                uint32_t ncap = cap * 2;
                prdb_sql_select_item *ni =
                    (prdb_sql_select_item *)prdb_sql_arena_alloc(
                        p->arena, ncap * sizeof(*ni), 8);
                if (ni == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
                memcpy(ni, items, count * sizeof(*ni));
                items = ni;
                cap = ncap;
            }
            prdb_sql_select_item *it = &items[count++];
            if (p->cur.kind == TOK_STAR) {
                it->is_star = 1;
                if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
            } else {
                if (parse_expr(p, depth + 1, &it->expr) != PRDB_SDK_SQL_OK)
                    return p->err;
                /* Check for table.* where parser parsed table as column?
                 * Handle in parser: if expr is COLUMN without table_name and
                 * next token is '.' followed by '*', convert to star_table. */
                if (tok_is_kw(p, "AS")) {
                    if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
                    if (parse_ident_bytes(p, &it->alias, &it->alias_len) !=
                        PRDB_SDK_SQL_OK)
                        return p->err;
                } else if (is_alias_start(p)) {
                    if (parse_ident_bytes(p, &it->alias, &it->alias_len) !=
                        PRDB_SDK_SQL_OK)
                        return p->err;
                }
            }
            if (p->cur.kind != TOK_COMMA) break;
            if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        }
        sel->items = items;
        sel->item_count = count;
    }

    if (tok_is_kw(p, "FROM")) {
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        if (parse_from_clause(p, depth + 1, &sel->from) != PRDB_SDK_SQL_OK)
            return p->err;
    }
    if (tok_is_kw(p, "WHERE")) {
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        if (parse_expr(p, depth + 1, &sel->where_expr) != PRDB_SDK_SQL_OK)
            return p->err;
    }
    if (tok_is_kw(p, "GROUP")) {
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        if (expect_kw(p, "BY") != PRDB_SDK_SQL_OK) return p->err;
        uint32_t cap = 8, count = 0;
        prdb_sql_expr **gb = (prdb_sql_expr **)prdb_sql_arena_alloc(
            p->arena, cap * sizeof(*gb), 8);
        if (gb == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
        for (;;) {
            if (count == cap) {
                uint32_t ncap = cap * 2;
                prdb_sql_expr **ng = (prdb_sql_expr **)prdb_sql_arena_alloc(
                    p->arena, ncap * sizeof(*ng), 8);
                if (ng == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
                memcpy(ng, gb, count * sizeof(*ng));
                gb = ng;
                cap = ncap;
            }
            if (parse_expr(p, depth + 1, &gb[count++]) != PRDB_SDK_SQL_OK)
                return p->err;
            if (p->cur.kind != TOK_COMMA) break;
            if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        }
        sel->group_by = gb;
        sel->group_by_count = count;
    }
    if (tok_is_kw(p, "HAVING")) {
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        if (parse_expr(p, depth + 1, &sel->having_expr) != PRDB_SDK_SQL_OK)
            return p->err;
    }
    if (tok_is_kw(p, "ORDER")) {
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        if (expect_kw(p, "BY") != PRDB_SDK_SQL_OK) return p->err;
        uint32_t cap = 8, count = 0;
        prdb_sql_order_spec *ob = (prdb_sql_order_spec *)prdb_sql_arena_alloc(
            p->arena, cap * sizeof(*ob), 8);
        if (ob == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
        for (;;) {
            if (count == cap) {
                uint32_t ncap = cap * 2;
                prdb_sql_order_spec *no =
                    (prdb_sql_order_spec *)prdb_sql_arena_alloc(
                        p->arena, ncap * sizeof(*no), 8);
                if (no == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
                memcpy(no, ob, count * sizeof(*no));
                ob = no;
                cap = ncap;
            }
            prdb_sql_order_spec *os = &ob[count++];
            if (parse_expr(p, depth + 1, &os->expr) != PRDB_SDK_SQL_OK)
                return p->err;
            if (tok_is_kw(p, "DESC")) {
                os->descending = 1;
                if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
            } else if (tok_is_kw(p, "ASC")) {
                if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
            }
            if (tok_is_kw(p, "NULLS")) {
                if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
                if (tok_is_kw(p, "FIRST")) {
                    os->nulls_first = 1;
                } else if (tok_is_kw(p, "LAST")) {
                    os->nulls_last = 1;
                } else {
                    return syntax_err(p);
                }
                if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
            }
            if (p->cur.kind != TOK_COMMA) break;
            if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        }
        sel->order_by = ob;
        sel->order_by_count = count;
    }
    if (tok_is_kw(p, "LIMIT")) {
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        sel->has_limit = 1;
        if (parse_u64_tok(p, &sel->limit) != PRDB_SDK_SQL_OK) return p->err;
    }
    if (tok_is_kw(p, "OFFSET")) {
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        sel->has_offset = 1;
        if (parse_u64_tok(p, &sel->offset) != PRDB_SDK_SQL_OK) return p->err;
    }
    *out = sel;
    return PRDB_SDK_SQL_OK;
}

static int32_t parse_rel_name(sql_parser *p, const uint8_t **schema,
                              uint32_t *schema_len, const uint8_t **rel,
                              uint32_t *rel_len) {
    const uint8_t *id1 = NULL, *id2 = NULL;
    uint32_t l1 = 0, l2 = 0;
    if (parse_ident_bytes(p, &id1, &l1) != PRDB_SDK_SQL_OK) return p->err;
    if (p->cur.kind == TOK_DOT) {
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        if (parse_ident_bytes(p, &id2, &l2) != PRDB_SDK_SQL_OK) return p->err;
        *schema = id1;
        *schema_len = l1;
        *rel = id2;
        *rel_len = l2;
    } else {
        *schema = NULL;
        *schema_len = 0;
        *rel = id1;
        *rel_len = l1;
    }
    return PRDB_SDK_SQL_OK;
}

static int32_t parse_insert_stmt(sql_parser *p, prdb_sql_insert_ast **out) {
    prdb_sql_insert_ast *ins = (prdb_sql_insert_ast *)prdb_sql_arena_alloc(
        p->arena, sizeof(*ins), 8);
    if (ins == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
    if (expect_kw(p, "INSERT") != PRDB_SDK_SQL_OK) return p->err;
    if (expect_kw(p, "INTO") != PRDB_SDK_SQL_OK) return p->err;
    if (parse_rel_name(p, &ins->schema, &ins->schema_len, &ins->relation,
                       &ins->relation_len) != PRDB_SDK_SQL_OK)
        return p->err;
    if (p->cur.kind == TOK_LPAREN) {
        uint32_t cap = 16, count = 0;
        const uint8_t **cols = (const uint8_t **)prdb_sql_arena_alloc(
            p->arena, cap * sizeof(*cols), 8);
        uint32_t *lens = (uint32_t *)prdb_sql_arena_alloc(
            p->arena, cap * sizeof(*lens), 4);
        if (cols == NULL || lens == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        for (;;) {
            if (count == cap) {
                uint32_t ncap = cap * 2;
                const uint8_t **nc = (const uint8_t **)prdb_sql_arena_alloc(
                    p->arena, ncap * sizeof(*nc), 8);
                uint32_t *nl = (uint32_t *)prdb_sql_arena_alloc(
                    p->arena, ncap * sizeof(*nl), 4);
                if (nc == NULL || nl == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
                memcpy(nc, cols, count * sizeof(*nc));
                memcpy(nl, lens, count * sizeof(*nl));
                cols = nc;
                lens = nl;
                cap = ncap;
            }
            if (parse_ident_bytes(p, &cols[count], &lens[count]) !=
                PRDB_SDK_SQL_OK)
                return p->err;
            ++count;
            if (p->cur.kind != TOK_COMMA) break;
            if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        }
        if (expect_tok(p, TOK_RPAREN) != PRDB_SDK_SQL_OK) return p->err;
        ins->columns = cols;
        ins->column_lens = lens;
        ins->column_count = count;
    }
    if (expect_kw(p, "VALUES") != PRDB_SDK_SQL_OK) return p->err;
    uint32_t rcap = 8, rcount = 0;
    prdb_sql_expr ***rows = (prdb_sql_expr ***)prdb_sql_arena_alloc(
        p->arena, rcap * sizeof(*rows), 8);
    if (rows == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
    for (;;) {
        if (rcount == rcap) {
            uint32_t ncap = rcap * 2;
            prdb_sql_expr ***nr = (prdb_sql_expr ***)prdb_sql_arena_alloc(
                p->arena, ncap * sizeof(*nr), 8);
            if (nr == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
            memcpy(nr, rows, rcount * sizeof(*nr));
            rows = nr;
            rcap = ncap;
        }
        if (expect_tok(p, TOK_LPAREN) != PRDB_SDK_SQL_OK) return p->err;
        uint32_t vcap = 16, vcount = 0;
        prdb_sql_expr **row = (prdb_sql_expr **)prdb_sql_arena_alloc(
            p->arena, vcap * sizeof(*row), 8);
        if (row == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
        for (;;) {
            if (vcount == vcap) {
                uint32_t ncap = vcap * 2;
                prdb_sql_expr **nrow = (prdb_sql_expr **)prdb_sql_arena_alloc(
                    p->arena, ncap * sizeof(*nrow), 8);
                if (nrow == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
                memcpy(nrow, row, vcount * sizeof(*nrow));
                row = nrow;
                vcap = ncap;
            }
            if (tok_is_kw(p, "DEFAULT")) {
                row[vcount++] = NULL;
                if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
            } else {
                if (parse_expr(p, 0, &row[vcount++]) != PRDB_SDK_SQL_OK)
                    return p->err;
            }
            if (p->cur.kind != TOK_COMMA) break;
            if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        }
        if (expect_tok(p, TOK_RPAREN) != PRDB_SDK_SQL_OK) return p->err;
        if (rcount == 0) {
            ins->cols_per_row = vcount;
        } else if (vcount != ins->cols_per_row) {
            return syntax_err(p);
        }
        rows[rcount++] = row;
        if (p->cur.kind != TOK_COMMA) break;
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
    }
    ins->rows = rows;
    ins->row_count = rcount;
    *out = ins;
    return PRDB_SDK_SQL_OK;
}

static int32_t parse_update_stmt(sql_parser *p, prdb_sql_update_ast **out) {
    prdb_sql_update_ast *upd = (prdb_sql_update_ast *)prdb_sql_arena_alloc(
        p->arena, sizeof(*upd), 8);
    if (upd == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
    if (expect_kw(p, "UPDATE") != PRDB_SDK_SQL_OK) return p->err;
    if (parse_rel_name(p, &upd->schema, &upd->schema_len, &upd->relation,
                       &upd->relation_len) != PRDB_SDK_SQL_OK)
        return p->err;
    if (expect_kw(p, "SET") != PRDB_SDK_SQL_OK) return p->err;
    uint32_t cap = 8, count = 0;
    prdb_sql_update_assign *as = (prdb_sql_update_assign *)prdb_sql_arena_alloc(
        p->arena, cap * sizeof(*as), 8);
    if (as == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
    for (;;) {
        if (count == cap) {
            uint32_t ncap = cap * 2;
            prdb_sql_update_assign *na =
                (prdb_sql_update_assign *)prdb_sql_arena_alloc(
                    p->arena, ncap * sizeof(*na), 8);
            if (na == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
            memcpy(na, as, count * sizeof(*na));
            as = na;
            cap = ncap;
        }
        if (parse_ident_bytes(p, &as[count].column, &as[count].column_len) !=
            PRDB_SDK_SQL_OK)
            return p->err;
        if (expect_tok(p, TOK_EQ) != PRDB_SDK_SQL_OK) return p->err;
        if (parse_expr(p, 0, &as[count].expr) != PRDB_SDK_SQL_OK) return p->err;
        ++count;
        if (p->cur.kind != TOK_COMMA) break;
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
    }
    upd->assignments = as;
    upd->assignment_count = count;
    if (tok_is_kw(p, "WHERE")) {
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        if (parse_expr(p, 0, &upd->where_expr) != PRDB_SDK_SQL_OK)
            return p->err;
    }
    *out = upd;
    return PRDB_SDK_SQL_OK;
}

static int32_t parse_delete_stmt(sql_parser *p, prdb_sql_delete_ast **out) {
    prdb_sql_delete_ast *del = (prdb_sql_delete_ast *)prdb_sql_arena_alloc(
        p->arena, sizeof(*del), 8);
    if (del == NULL) return PRDB_SDK_SQL_LIMIT_EXCEEDED;
    if (expect_kw(p, "DELETE") != PRDB_SDK_SQL_OK) return p->err;
    if (expect_kw(p, "FROM") != PRDB_SDK_SQL_OK) return p->err;
    if (parse_rel_name(p, &del->schema, &del->schema_len, &del->relation,
                       &del->relation_len) != PRDB_SDK_SQL_OK)
        return p->err;
    if (tok_is_kw(p, "WHERE")) {
        if (next_tok(p) != PRDB_SDK_SQL_OK) return p->err;
        if (parse_expr(p, 0, &del->where_expr) != PRDB_SDK_SQL_OK)
            return p->err;
    }
    *out = del;
    return PRDB_SDK_SQL_OK;
}

PRDB_SDK_INTERNAL int32_t prdb_sql_parse_statement_v1(
    prdb_sql_arena *arena, const uint8_t *sql, uint64_t sql_len,
    prdb_sql_stmt_ast *out, uint32_t *error_offset_out) {
    sql_parser p = {0};
    int32_t status;

    if (error_offset_out != NULL) *error_offset_out = 0;
    if (arena == NULL || sql == NULL || out == NULL || sql_len == 0 ||
        sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES) {
        return sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES
                   ? PRDB_SDK_SQL_LIMIT_EXCEEDED
                   : PRDB_SDK_SQL_INVALID_ARGUMENT;
    }
    memset(out, 0, sizeof(*out));
    p.arena = arena;
    p.sql = sql;
    p.len = (uint32_t)sql_len;
    status = next_tok(&p);
    if (status != PRDB_SDK_SQL_OK) goto fail;

    if (tok_is_kw(&p, "SELECT") || tok_is_kw(&p, "WITH")) {
        out->kind = PRDB_SQL_STMT_SELECT;
        status = parse_select_stmt(&p, 0, &out->u.select);
    } else if (tok_is_kw(&p, "INSERT")) {
        out->kind = PRDB_SQL_STMT_INSERT;
        status = parse_insert_stmt(&p, &out->u.insert);
    } else if (tok_is_kw(&p, "UPDATE")) {
        out->kind = PRDB_SQL_STMT_UPDATE;
        status = parse_update_stmt(&p, &out->u.update);
    } else if (tok_is_kw(&p, "DELETE")) {
        out->kind = PRDB_SQL_STMT_DELETE;
        status = parse_delete_stmt(&p, &out->u.del);
    } else {
        status = PRDB_SDK_SQL_UNSUPPORTED_STATEMENT;
        p.err_off = p.cur.offset;
    }
    if (status != PRDB_SDK_SQL_OK) goto fail;
    if (p.cur.kind == TOK_SEMI) {
        status = next_tok(&p);
        if (status != PRDB_SDK_SQL_OK) goto fail;
    }
    if (p.cur.kind != TOK_EOF) {
        status = syntax_err(&p);
        goto fail;
    }
    return PRDB_SDK_SQL_OK;

fail:
    if (error_offset_out != NULL) *error_offset_out = p.err_off;
    return status;
}
