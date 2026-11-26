#include "sql_ast_internal.h"

#include <stdlib.h>
#include <string.h>

/* --- Owned value helpers --- */

PRDB_SDK_INTERNAL void prdb_sql_owned_val_free(prdb_sql_owned_val *v) {
    if (v != NULL && v->heap_buf != NULL) {
        free(v->heap_buf);
        v->heap_buf = NULL;
    }
}

static void init_val_header(prdb_sdk_value_v1 *v, uint32_t state,
                            uint32_t type_code, const uint8_t *data,
                            uint64_t data_len) {
    v->struct_size = sizeof(*v);
    v->abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
    v->state = state;
    v->type_code = type_code;
    v->data = data;
    v->data_len = data_len;
}

static void set_null(prdb_sql_owned_val *out) {
    prdb_sql_owned_val_free(out);
    init_val_header(&out->val, PRDB_SDK_VALUE_STATE_NULL, 0, NULL, 0);
}

static void set_bool(prdb_sql_owned_val *out, uint8_t b) {
    prdb_sql_owned_val_free(out);
    out->inline_buf[0] = b ? 1 : 0;
    init_val_header(&out->val, PRDB_SDK_VALUE_STATE_VALUE,
                    PRDB_SDK_VALUE_TYPE_BOOLEAN, out->inline_buf, 1);
}

static void set_i64_typed(prdb_sql_owned_val *out, uint32_t type_code,
                          int64_t v) {
    prdb_sql_owned_val_free(out);
    uint64_t u = (uint64_t)v;
    if (type_code == PRDB_SDK_VALUE_TYPE_SMALLINT) {
        uint16_t u16 = (uint16_t)(int16_t)v;
        out->inline_buf[0] = (uint8_t)(u16 >> 8);
        out->inline_buf[1] = (uint8_t)(u16 & 0xff);
        init_val_header(&out->val, PRDB_SDK_VALUE_STATE_VALUE, type_code,
                        out->inline_buf, 2);
    } else if (type_code == PRDB_SDK_VALUE_TYPE_INTEGER ||
               type_code == PRDB_SDK_VALUE_TYPE_DATE) {
        uint32_t u32 = (uint32_t)(int32_t)v;
        out->inline_buf[0] = (uint8_t)(u32 >> 24);
        out->inline_buf[1] = (uint8_t)(u32 >> 16);
        out->inline_buf[2] = (uint8_t)(u32 >> 8);
        out->inline_buf[3] = (uint8_t)(u32 & 0xff);
        init_val_header(&out->val, PRDB_SDK_VALUE_STATE_VALUE, type_code,
                        out->inline_buf, 4);
    } else {
        for (int i = 0; i < 8; ++i) {
            out->inline_buf[i] = (uint8_t)(u >> (56 - i * 8));
        }
        init_val_header(&out->val, PRDB_SDK_VALUE_STATE_VALUE, type_code,
                        out->inline_buf, 8);
    }
}

static void set_decimal(prdb_sql_owned_val *out, const uint8_t bytes[16]) {
    prdb_sql_owned_val_free(out);
    memcpy(out->inline_buf, bytes, 16);
    init_val_header(&out->val, PRDB_SDK_VALUE_STATE_VALUE,
                    PRDB_SDK_VALUE_TYPE_DECIMAL, out->inline_buf, 16);
}

static int32_t set_bytes_copy(prdb_sql_owned_val *out, uint32_t type_code,
                              const uint8_t *data, uint64_t len) {
    prdb_sql_owned_val_free(out);
    if (len <= sizeof(out->inline_buf)) {
        if (len != 0 && data != NULL) memcpy(out->inline_buf, data, len);
        init_val_header(&out->val, PRDB_SDK_VALUE_STATE_VALUE, type_code,
                        out->inline_buf, len);
        return PRDB_SDK_SQL_EVAL_OK;
    }
    out->heap_buf = (uint8_t *)malloc((size_t)len);
    if (out->heap_buf == NULL) return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    memcpy(out->heap_buf, data, (size_t)len);
    init_val_header(&out->val, PRDB_SDK_VALUE_STATE_VALUE, type_code,
                    out->heap_buf, len);
    return PRDB_SDK_SQL_EVAL_OK;
}

static int32_t copy_value_to_owned(const prdb_sdk_value_v1 *src,
                                   prdb_sql_owned_val *out) {
    if (src->state == PRDB_SDK_VALUE_STATE_REDACTED) {
        return PRDB_SDK_SQL_EVAL_REDACTED_INPUT;
    }
    if (src->state == PRDB_SDK_VALUE_STATE_NULL) {
        set_null(out);
        return PRDB_SDK_SQL_EVAL_OK;
    }
    return set_bytes_copy(out, src->type_code, src->data, src->data_len);
}

/* --- 128-bit big-endian signed/unsigned arithmetic helpers --- */

static int i128_is_neg(const uint8_t a[16]) { return (a[0] & 0x80) != 0; }

static void i128_neg(const uint8_t a[16], uint8_t out[16]) {
    uint16_t carry = 1;
    for (int i = 15; i >= 0; --i) {
        uint16_t v = (uint16_t)(uint8_t)(~a[i]) + carry;
        out[i] = (uint8_t)(v & 0xff);
        carry = v >> 8;
    }
}

static void i128_from_i64(int64_t v, uint8_t out[16]) {
    uint8_t fill = v < 0 ? 0xff : 0x00;
    memset(out, fill, 8);
    uint64_t u = (uint64_t)v;
    for (int i = 0; i < 8; ++i) {
        out[8 + i] = (uint8_t)(u >> (56 - i * 8));
    }
}

static int u128_cmp(const uint8_t a[16], const uint8_t b[16]) {
    for (int i = 0; i < 16; ++i) {
        if (a[i] < b[i]) return -1;
        if (a[i] > b[i]) return 1;
    }
    return 0;
}

static int u128_add(const uint8_t a[16], const uint8_t b[16], uint8_t out[16]) {
    uint16_t carry = 0;
    for (int i = 15; i >= 0; --i) {
        uint16_t s = (uint16_t)a[i] + b[i] + carry;
        out[i] = (uint8_t)(s & 0xff);
        carry = s >> 8;
    }
    return carry == 0;
}

static int u128_sub(const uint8_t a[16], const uint8_t b[16], uint8_t out[16]) {
    int16_t borrow = 0;
    for (int i = 15; i >= 0; --i) {
        int16_t d = (int16_t)a[i] - (int16_t)b[i] - borrow;
        if (d < 0) {
            d += 256;
            borrow = 1;
        } else {
            borrow = 0;
        }
        out[i] = (uint8_t)d;
    }
    return borrow == 0;
}

static int u128_mul_u32(const uint8_t a[16], uint32_t m, uint8_t out[16]) {
    uint64_t carry = 0;
    for (int i = 15; i >= 0; --i) {
        uint64_t p = (uint64_t)a[i] * m + carry;
        out[i] = (uint8_t)(p & 0xff);
        carry = p >> 8;
    }
    return carry == 0;
}

static int u128_scale_up(uint8_t a[16], uint32_t delta) {
    for (uint32_t i = 0; i < delta; ++i) {
        if (!u128_mul_u32(a, 10, a)) return 0;
    }
    return 1;
}

static int i128_add_checked(const uint8_t a[16], const uint8_t b[16],
                            uint8_t out[16]) {
    u128_add(a, b, out);
    int sa = i128_is_neg(a);
    int sb = i128_is_neg(b);
    int so = i128_is_neg(out);
    if (sa == sb && so != sa) return 0;
    return 1;
}

static int i128_sub_checked(const uint8_t a[16], const uint8_t b[16],
                            uint8_t out[16]) {
    uint8_t nb[16];
    /* Check b == INT128_MIN */
    if (b[0] == 0x80) {
        int all_zero = 1;
        for (int i = 1; i < 16; ++i) {
            if (b[i] != 0) { all_zero = 0; break; }
        }
        if (all_zero) return 0;
    }
    i128_neg(b, nb);
    return i128_add_checked(a, nb, out);
}

/* 256-bit unsigned multiply of two 128-bit unsigned numbers */
static void u128_mul_u256(const uint8_t a[16], const uint8_t b[16],
                          uint8_t out[32]) {
    memset(out, 0, 32);
    for (int i = 15; i >= 0; --i) {
        uint32_t carry = 0;
        for (int j = 15; j >= 0; --j) {
            uint32_t cur = (uint32_t)out[i + j + 1] +
                           (uint32_t)a[i] * (uint32_t)b[j] + carry;
            out[i + j + 1] = (uint8_t)(cur & 0xff);
            carry = cur >> 8;
        }
        out[i] = (uint8_t)(out[i] + carry);
    }
}

/* Divide 256-bit unsigned by 128-bit unsigned; returns 128-bit quotient + rem */
static int u256_div_u128(const uint8_t num[32], const uint8_t den[16],
                         uint8_t quot[16], uint8_t rem[16]) {
    uint8_t q[32] = {0};
    uint8_t r[16] = {0};
    int den_zero = 1;
    for (int i = 0; i < 16; ++i) {
        if (den[i] != 0) { den_zero = 0; break; }
    }
    if (den_zero) return 0;
    for (int bit = 0; bit < 256; ++bit) {
        uint8_t carry = (uint8_t)((num[bit / 8] >> (7 - (bit % 8))) & 1);
        uint8_t overflow = (r[0] & 0x80) != 0;
        for (int i = 15; i >= 0; --i) {
            uint8_t next_c = (r[i] >> 7) & 1;
            r[i] = (uint8_t)((r[i] << 1) | carry);
            carry = next_c;
        }
        if (overflow || u128_cmp(r, den) >= 0) {
            u128_sub(r, den, r);
            q[bit / 8] |= (uint8_t)(1 << (7 - (bit % 8)));
        }
    }
    for (int i = 0; i < 16; ++i) {
        if (q[i] != 0) return 0; /* overflow > 128 bits */
    }
    memcpy(quot, q + 16, 16);
    memcpy(rem, r, 16);
    return 1;
}

static int u256_div_u32(uint8_t num[32], uint32_t den) {
    uint64_t rem = 0;
    for (int i = 0; i < 32; ++i) {
        uint64_t cur = (rem << 8) | num[i];
        num[i] = (uint8_t)(cur / den);
        rem = cur % den;
    }
    return (int)rem;
}

static int i128_mul_checked(const uint8_t a[16], const uint8_t b[16],
                            uint8_t out[16]) {
    uint8_t ua[16], ub[16], prod[32];
    int neg = i128_is_neg(a) ^ i128_is_neg(b);
    if (i128_is_neg(a)) i128_neg(a, ua); else memcpy(ua, a, 16);
    if (i128_is_neg(b)) i128_neg(b, ub); else memcpy(ub, b, 16);
    u128_mul_u256(ua, ub, prod);
    for (int i = 0; i < 16; ++i) {
        if (prod[i] != 0) return 0;
    }
    if (prod[16] & 0x80) return 0;
    if (neg) i128_neg(prod + 16, out);
    else memcpy(out, prod + 16, 16);
    return 1;
}

/* --- Date / calendar helpers --- */

static void civil_from_days(int32_t z, int64_t *y_out, unsigned *m_out,
                            unsigned *d_out) {
    int64_t zz = (int64_t)z + 719468;
    int64_t era = (zz >= 0 ? zz : zz - 146096) / 146097;
    unsigned doe = (unsigned)(zz - era * 146097);
    unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    int64_t y = (int64_t)yoe + era * 400;
    unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    unsigned mp = (5 * doy + 2) / 153;
    unsigned d = doy - (153 * mp + 2) / 5 + 1;
    unsigned m = mp + (mp < 10 ? 3 : (unsigned)-9);
    y += (m <= 2);
    *y_out = y;
    *m_out = m;
    *d_out = d;
}

static int32_t days_from_civil_eval(int64_t y, unsigned m, unsigned d) {
    y -= m <= 2;
    int64_t era = (y >= 0 ? y : y - 399) / 400;
    unsigned yoe = (unsigned)(y - era * 400);
    unsigned doy = (153 * (m + (m > 2 ? (unsigned)-3 : 9)) + 2) / 5 + d - 1;
    unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return (int32_t)(era * 146097 + (int64_t)doe - 719468);
}

static unsigned days_in_month(int64_t y, unsigned m) {
    static const unsigned dim[12] = {31, 28, 31, 30, 31, 30,
                                     31, 31, 30, 31, 30, 31};
    if (m == 2 && ((y % 4 == 0 && y % 100 != 0) || (y % 400 == 0))) {
        return 29;
    }
    return dim[m - 1];
}

/* --- LIKE matching (UTF-8 / byte aware, % and _) --- */

static int like_match(const uint8_t *str, uint64_t slen, const uint8_t *pat,
                      uint64_t plen) {
    uint64_t si = 0, pi = 0;
    uint64_t star_p = UINT64_MAX, star_s = 0;
    while (si < slen) {
        if (pi < plen && pat[pi] == '%') {
            star_p = pi++;
            star_s = si;
        } else if (pi < plen && pat[pi] == '_') {
            /* Advance one UTF-8 scalar */
            uint8_t b = str[si++];
            if ((b & 0x80) != 0) {
                while (si < slen && (str[si] & 0xc0) == 0x80) ++si;
            }
            ++pi;
        } else if (pi < plen && pat[pi] == str[si]) {
            ++pi;
            ++si;
        } else if (star_p != UINT64_MAX) {
            pi = star_p + 1;
            uint8_t b = str[star_s++];
            if ((b & 0x80) != 0) {
                while (star_s < slen && (str[star_s] & 0xc0) == 0x80) ++star_s;
            }
            si = star_s;
        } else {
            return 0;
        }
    }
    while (pi < plen && pat[pi] == '%') ++pi;
    return pi == plen;
}

/* --- Numeric extraction & promotion --- */

static int get_i64_value(const prdb_sdk_value_v1 *v, int64_t *out) {
    int16_t s16;
    int32_t s32;
    int64_t s64;
    switch (v->type_code) {
        case PRDB_SDK_VALUE_TYPE_SMALLINT:
            if (prdb_sdk_value_get_smallint_v1(v, &s16) != PRDB_SDK_VALUE_ACCESS_OK)
                return 0;
            *out = s16;
            return 1;
        case PRDB_SDK_VALUE_TYPE_INTEGER:
            if (prdb_sdk_value_get_integer_v1(v, &s32) != PRDB_SDK_VALUE_ACCESS_OK)
                return 0;
            *out = s32;
            return 1;
        case PRDB_SDK_VALUE_TYPE_BIGINT:
            if (prdb_sdk_value_get_bigint_v1(v, &s64) != PRDB_SDK_VALUE_ACCESS_OK)
                return 0;
            *out = s64;
            return 1;
        default: return 0;
    }
}

static int is_int_type(uint32_t t) {
    return t == PRDB_SDK_VALUE_TYPE_SMALLINT ||
           t == PRDB_SDK_VALUE_TYPE_INTEGER ||
           t == PRDB_SDK_VALUE_TYPE_BIGINT;
}

static int to_decimal_with_scale(const prdb_sdk_value_v1 *v, uint8_t src_scale,
                                 uint8_t dst_scale, uint8_t out[16]) {
    uint8_t tmp[16];
    if (is_int_type(v->type_code)) {
        int64_t iv = 0;
        if (!get_i64_value(v, &iv)) return 0;
        i128_from_i64(iv, tmp);
        src_scale = 0;
    } else if (v->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL && v->data_len == 16) {
        memcpy(tmp, v->data, 16);
    } else {
        return 0;
    }
    if (dst_scale == src_scale) {
        memcpy(out, tmp, 16);
        return 1;
    }
    if (dst_scale < src_scale) return 0;
    int neg = i128_is_neg(tmp);
    uint8_t mag[16];
    if (neg) i128_neg(tmp, mag); else memcpy(mag, tmp, 16);
    if (!u128_scale_up(mag, (uint32_t)(dst_scale - src_scale))) return 0;
    if (mag[0] & 0x80) return 0;
    if (neg) i128_neg(mag, out); else memcpy(out, mag, 16);
    return 1;
}

static uint8_t expr_scale(const prdb_sql_expr *e) {
    if (e->kind == PRDB_SQL_EXPR_DECIMAL) return e->u.dec_val.scale;
    if (e->is_bound && e->bound_type == PRDB_SDK_VALUE_TYPE_DECIMAL)
        return e->bound_scale;
    return 0;
}

static int32_t eval_compare(const prdb_sql_expr *left_e,
                            const prdb_sql_owned_val *l,
                            const prdb_sql_expr *right_e,
                            const prdb_sql_owned_val *r, uint32_t cmp_op,
                            uint32_t *truth_out) {
    if (l->val.state == PRDB_SDK_VALUE_STATE_REDACTED ||
        r->val.state == PRDB_SDK_VALUE_STATE_REDACTED) {
        return PRDB_SDK_SQL_EVAL_REDACTED_INPUT;
    }
    if (l->val.state == PRDB_SDK_VALUE_STATE_NULL ||
        r->val.state == PRDB_SDK_VALUE_STATE_NULL) {
        *truth_out = PRDB_SDK_SQL_TRUTH_UNKNOWN;
        return PRDB_SDK_SQL_EVAL_OK;
    }
    /* Handle numeric cross-type comparison (int vs int, decimal vs int/decimal) */
    if ((is_int_type(l->val.type_code) ||
         l->val.type_code == PRDB_SDK_VALUE_TYPE_DECIMAL) &&
        (is_int_type(r->val.type_code) ||
         r->val.type_code == PRDB_SDK_VALUE_TYPE_DECIMAL)) {
        if (is_int_type(l->val.type_code) && is_int_type(r->val.type_code)) {
            int64_t lv = 0, rv = 0;
            get_i64_value(&l->val, &lv);
            get_i64_value(&r->val, &rv);
            int cmp = (lv < rv) ? -1 : (lv > rv ? 1 : 0);
            int match = 0;
            switch (cmp_op) {
                case PRDB_SDK_SQL_COMPARE_EQUAL: match = cmp == 0; break;
                case PRDB_SDK_SQL_COMPARE_NOT_EQUAL: match = cmp != 0; break;
                case PRDB_SDK_SQL_COMPARE_LESS: match = cmp < 0; break;
                case PRDB_SDK_SQL_COMPARE_LESS_EQUAL: match = cmp <= 0; break;
                case PRDB_SDK_SQL_COMPARE_GREATER: match = cmp > 0; break;
                case PRDB_SDK_SQL_COMPARE_GREATER_EQUAL: match = cmp >= 0; break;
                default: return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
            }
            *truth_out = match ? PRDB_SDK_SQL_TRUTH_TRUE : PRDB_SDK_SQL_TRUTH_FALSE;
            return PRDB_SDK_SQL_EVAL_OK;
        }
        uint8_t ls = expr_scale(left_e);
        uint8_t rs = expr_scale(right_e);
        uint8_t target = ls > rs ? ls : rs;
        uint8_t lb[16], rb[16];
        if (!to_decimal_with_scale(&l->val, ls, target, lb) ||
            !to_decimal_with_scale(&r->val, rs, target, rb)) {
            return PRDB_SDK_SQL_EVAL_NUMERIC_OVERFLOW;
        }
        prdb_sdk_value_v1 lv = l->val, rv = r->val;
        lv.type_code = PRDB_SDK_VALUE_TYPE_DECIMAL;
        lv.data = lb;
        lv.data_len = 16;
        rv.type_code = PRDB_SDK_VALUE_TYPE_DECIMAL;
        rv.data = rb;
        rv.data_len = 16;
        return prdb_sdk_sql_compare_v1(&lv, &rv, cmp_op, target, target,
                                       truth_out);
    }
    /* Allow TEXT vs VARCHAR comparison */
    if ((l->val.type_code == PRDB_SDK_VALUE_TYPE_TEXT ||
         l->val.type_code == PRDB_SDK_VALUE_TYPE_VARCHAR) &&
        (r->val.type_code == PRDB_SDK_VALUE_TYPE_TEXT ||
         r->val.type_code == PRDB_SDK_VALUE_TYPE_VARCHAR)) {
        prdb_sdk_value_v1 lv = l->val, rv = r->val;
        lv.type_code = PRDB_SDK_VALUE_TYPE_TEXT;
        rv.type_code = PRDB_SDK_VALUE_TYPE_TEXT;
        return prdb_sdk_sql_compare_v1(&lv, &rv, cmp_op, 0, 0, truth_out);
    }
    return prdb_sdk_sql_compare_v1(&l->val, &r->val, cmp_op, 0, 0, truth_out);
}

static uint32_t binop_to_cmp(prdb_sql_bin_op op) {
    switch (op) {
        case PRDB_SQL_BOP_EQ: return PRDB_SDK_SQL_COMPARE_EQUAL;
        case PRDB_SQL_BOP_NE: return PRDB_SDK_SQL_COMPARE_NOT_EQUAL;
        case PRDB_SQL_BOP_LT: return PRDB_SDK_SQL_COMPARE_LESS;
        case PRDB_SQL_BOP_LE: return PRDB_SDK_SQL_COMPARE_LESS_EQUAL;
        case PRDB_SQL_BOP_GT: return PRDB_SDK_SQL_COMPARE_GREATER;
        default: return PRDB_SDK_SQL_COMPARE_GREATER_EQUAL;
    }
}

PRDB_SDK_INTERNAL int32_t prdb_sql_eval_expr_v1(
    const prdb_sql_expr *expr, const prdb_sql_eval_row_set *ctx,
    prdb_sql_owned_val *out) {
    int32_t status;
    if (expr == NULL || ctx == NULL || out == NULL) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    switch (expr->kind) {
        case PRDB_SQL_EXPR_NULL:
            set_null(out);
            return PRDB_SDK_SQL_EVAL_OK;
        case PRDB_SQL_EXPR_BOOL:
            set_bool(out, expr->u.bool_val);
            return PRDB_SDK_SQL_EVAL_OK;
        case PRDB_SQL_EXPR_INT:
            set_i64_typed(out,
                          expr->is_bound && is_int_type(expr->bound_type)
                              ? expr->bound_type
                              : PRDB_SDK_VALUE_TYPE_BIGINT,
                          expr->u.int_val);
            return PRDB_SDK_SQL_EVAL_OK;
        case PRDB_SQL_EXPR_DECIMAL:
            set_decimal(out, expr->u.dec_val.bytes);
            return PRDB_SDK_SQL_EVAL_OK;
        case PRDB_SQL_EXPR_STRING:
            return set_bytes_copy(out, PRDB_SDK_VALUE_TYPE_TEXT,
                                  expr->u.str_val.data, expr->u.str_val.len);
        case PRDB_SQL_EXPR_DATE:
            set_i64_typed(out, PRDB_SDK_VALUE_TYPE_DATE, expr->u.date_days);
            return PRDB_SDK_SQL_EVAL_OK;
        case PRDB_SQL_EXPR_TIMESTAMP:
            set_i64_typed(out, PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ,
                          expr->u.ts_micros);
            return PRDB_SDK_SQL_EVAL_OK;
        case PRDB_SQL_EXPR_PARAMETER: {
            if (expr->u.param_index == 0 ||
                expr->u.param_index > ctx->param_count || ctx->params == NULL) {
                return PRDB_SDK_SQL_EVAL_PARAMETER_MISSING;
            }
            return copy_value_to_owned(&ctx->params[expr->u.param_index - 1],
                                       out);
        }
        case PRDB_SQL_EXPR_COLUMN: {
            if (expr->u.col.scope_depth >= ctx->scope_count) {
                return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
            }
            const prdb_sql_eval_scope *sc =
                &ctx->scopes[expr->u.col.scope_depth];
            if (expr->u.col.col_ordinal >= sc->column_count) {
                return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
            }
            return copy_value_to_owned(&sc->values[expr->u.col.col_ordinal],
                                       out);
        }
        case PRDB_SQL_EXPR_AGGREGATE: {
            if (expr->u.agg.bound_agg_index >= ctx->agg_count ||
                ctx->agg_values == NULL) {
                return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
            }
            return copy_value_to_owned(
                &ctx->agg_values[expr->u.agg.bound_agg_index], out);
        }
        case PRDB_SQL_EXPR_SUBQUERY: {
            if (ctx->subq_eval == NULL) {
                return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
            }
            return ctx->subq_eval(ctx->subq_ctx, expr, ctx, out);
        }
        case PRDB_SQL_EXPR_UNARY: {
            prdb_sql_owned_val opv = {0};
            if (expr->u.unary.op == PRDB_SQL_UOP_NOT ||
                expr->u.unary.op == PRDB_SQL_UOP_IS_NULL ||
                expr->u.unary.op == PRDB_SQL_UOP_IS_NOT_NULL) {
                uint32_t truth = 0;
                status = prdb_sql_eval_truth_v1(expr, ctx, &truth);
                if (status != PRDB_SDK_SQL_EVAL_OK) return status;
                if (truth == PRDB_SDK_SQL_TRUTH_UNKNOWN) set_null(out);
                else set_bool(out, truth == PRDB_SDK_SQL_TRUTH_TRUE);
                return PRDB_SDK_SQL_EVAL_OK;
            }
            status = prdb_sql_eval_expr_v1(expr->u.unary.operand, ctx, &opv);
            if (status != PRDB_SDK_SQL_EVAL_OK) {
                prdb_sql_owned_val_free(&opv);
                return status;
            }
            if (opv.val.state == PRDB_SDK_VALUE_STATE_NULL) {
                set_null(out);
                prdb_sql_owned_val_free(&opv);
                return PRDB_SDK_SQL_EVAL_OK;
            }
            if (is_int_type(opv.val.type_code)) {
                int64_t iv = 0;
                get_i64_value(&opv.val, &iv);
                if (iv == INT64_MIN) {
                    prdb_sql_owned_val_free(&opv);
                    return PRDB_SDK_SQL_EVAL_NUMERIC_OVERFLOW;
                }
                set_i64_typed(out, opv.val.type_code, -iv);
                prdb_sql_owned_val_free(&opv);
                return PRDB_SDK_SQL_EVAL_OK;
            }
            if (opv.val.type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
                opv.val.data_len == 16) {
                uint8_t nb[16];
                i128_neg(opv.val.data, nb);
                set_decimal(out, nb);
                prdb_sql_owned_val_free(&opv);
                return PRDB_SDK_SQL_EVAL_OK;
            }
            prdb_sql_owned_val_free(&opv);
            return PRDB_SDK_SQL_EVAL_TYPE_MISMATCH;
        }
        case PRDB_SQL_EXPR_BINARY: {
            prdb_sql_bin_op op = expr->u.binary.op;
            if (op == PRDB_SQL_BOP_AND || op == PRDB_SQL_BOP_OR ||
                op == PRDB_SQL_BOP_EQ || op == PRDB_SQL_BOP_NE ||
                op == PRDB_SQL_BOP_LT || op == PRDB_SQL_BOP_LE ||
                op == PRDB_SQL_BOP_GT || op == PRDB_SQL_BOP_GE ||
                op == PRDB_SQL_BOP_LIKE || op == PRDB_SQL_BOP_NOT_LIKE) {
                uint32_t truth = 0;
                status = prdb_sql_eval_truth_v1(expr, ctx, &truth);
                if (status != PRDB_SDK_SQL_EVAL_OK) return status;
                if (truth == PRDB_SDK_SQL_TRUTH_UNKNOWN) set_null(out);
                else set_bool(out, truth == PRDB_SDK_SQL_TRUTH_TRUE);
                return PRDB_SDK_SQL_EVAL_OK;
            }
            /* Date +/- Interval */
            if ((op == PRDB_SQL_BOP_ADD || op == PRDB_SQL_BOP_SUB) &&
                expr->u.binary.right->kind == PRDB_SQL_EXPR_INTERVAL) {
                prdb_sql_owned_val lv = {0};
                status = prdb_sql_eval_expr_v1(expr->u.binary.left, ctx, &lv);
                if (status != PRDB_SDK_SQL_EVAL_OK) {
                    prdb_sql_owned_val_free(&lv);
                    return status;
                }
                if (lv.val.state == PRDB_SDK_VALUE_STATE_NULL) {
                    set_null(out);
                    prdb_sql_owned_val_free(&lv);
                    return PRDB_SDK_SQL_EVAL_OK;
                }
                if (lv.val.type_code != PRDB_SDK_VALUE_TYPE_DATE) {
                    prdb_sql_owned_val_free(&lv);
                    return PRDB_SDK_SQL_EVAL_TYPE_MISMATCH;
                }
                int32_t days = 0;
                prdb_sdk_value_get_date_v1(&lv.val, &days);
                prdb_sql_owned_val_free(&lv);
                int64_t amt = expr->u.binary.right->u.interval_val.amount;
                if (op == PRDB_SQL_BOP_SUB) amt = -amt;
                prdb_sql_interval_unit unit =
                    expr->u.binary.right->u.interval_val.unit;
                int32_t new_days = days;
                if (unit == PRDB_SQL_INTERVAL_DAY) {
                    int64_t nd = (int64_t)days + amt;
                    if (nd < PRDB_SDK_VALUE_DATE_MIN_DAYS ||
                        nd > PRDB_SDK_VALUE_DATE_MAX_DAYS)
                        return PRDB_SDK_SQL_EVAL_NUMERIC_OVERFLOW;
                    new_days = (int32_t)nd;
                } else {
                    int64_t y;
                    unsigned m, d;
                    civil_from_days(days, &y, &m, &d);
                    int64_t total_m =
                        y * 12 + (int64_t)(m - 1) +
                        (unit == PRDB_SQL_INTERVAL_YEAR ? amt * 12 : amt);
                    int64_t ny = total_m >= 0 ? total_m / 12
                                              : (total_m - 11) / 12;
                    unsigned nm = (unsigned)(total_m - ny * 12) + 1;
                    unsigned max_d = days_in_month(ny, nm);
                    unsigned nd = d > max_d ? max_d : d;
                    new_days = days_from_civil_eval(ny, nm, nd);
                    if (new_days < PRDB_SDK_VALUE_DATE_MIN_DAYS ||
                        new_days > PRDB_SDK_VALUE_DATE_MAX_DAYS)
                        return PRDB_SDK_SQL_EVAL_NUMERIC_OVERFLOW;
                }
                set_i64_typed(out, PRDB_SDK_VALUE_TYPE_DATE, new_days);
                return PRDB_SDK_SQL_EVAL_OK;
            }
            /* Arithmetic */
            prdb_sql_owned_val lv = {0}, rv = {0};
            status = prdb_sql_eval_expr_v1(expr->u.binary.left, ctx, &lv);
            if (status != PRDB_SDK_SQL_EVAL_OK) {
                prdb_sql_owned_val_free(&lv);
                return status;
            }
            status = prdb_sql_eval_expr_v1(expr->u.binary.right, ctx, &rv);
            if (status != PRDB_SDK_SQL_EVAL_OK) {
                prdb_sql_owned_val_free(&lv);
                prdb_sql_owned_val_free(&rv);
                return status;
            }
            if (lv.val.state == PRDB_SDK_VALUE_STATE_NULL ||
                rv.val.state == PRDB_SDK_VALUE_STATE_NULL) {
                set_null(out);
                prdb_sql_owned_val_free(&lv);
                prdb_sql_owned_val_free(&rv);
                return PRDB_SDK_SQL_EVAL_OK;
            }
            if (is_int_type(lv.val.type_code) && is_int_type(rv.val.type_code)) {
                int64_t a = 0, b = 0, res = 0;
                get_i64_value(&lv.val, &a);
                get_i64_value(&rv.val, &b);
                uint32_t res_type =
                    (lv.val.type_code == PRDB_SDK_VALUE_TYPE_BIGINT ||
                     rv.val.type_code == PRDB_SDK_VALUE_TYPE_BIGINT)
                        ? PRDB_SDK_VALUE_TYPE_BIGINT
                        : PRDB_SDK_VALUE_TYPE_INTEGER;
                if (op == PRDB_SQL_BOP_ADD) {
                    if ((b > 0 && a > INT64_MAX - b) ||
                        (b < 0 && a < INT64_MIN - b)) {
                        status = PRDB_SDK_SQL_EVAL_NUMERIC_OVERFLOW;
                    } else res = a + b;
                } else if (op == PRDB_SQL_BOP_SUB) {
                    if ((b < 0 && a > INT64_MAX + b) ||
                        (b > 0 && a < INT64_MIN + b)) {
                        status = PRDB_SDK_SQL_EVAL_NUMERIC_OVERFLOW;
                    } else res = a - b;
                } else if (op == PRDB_SQL_BOP_MUL) {
                    if (a != 0 && b != 0 &&
                        ((a == -1 && b == INT64_MIN) ||
                         (b == -1 && a == INT64_MIN) ||
                         (a > 0 && b > 0 && a > INT64_MAX / b) ||
                         (a < 0 && b < 0 && a < INT64_MAX / b) ||
                         (a > 0 && b < 0 && b < INT64_MIN / a) ||
                         (a < 0 && b > 0 && a < INT64_MIN / b))) {
                        status = PRDB_SDK_SQL_EVAL_NUMERIC_OVERFLOW;
                    } else res = a * b;
                } else if (op == PRDB_SQL_BOP_DIV) {
                    if (b == 0 || (a == INT64_MIN && b == -1)) {
                        status = PRDB_SDK_SQL_EVAL_NUMERIC_OVERFLOW;
                    } else res = a / b;
                } else if (op == PRDB_SQL_BOP_MOD) {
                    if (b == 0 || (a == INT64_MIN && b == -1)) {
                        status = PRDB_SDK_SQL_EVAL_NUMERIC_OVERFLOW;
                    } else res = a % b;
                } else {
                    status = PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
                }
                prdb_sql_owned_val_free(&lv);
                prdb_sql_owned_val_free(&rv);
                if (status != PRDB_SDK_SQL_EVAL_OK) return status;
                if (res_type == PRDB_SDK_VALUE_TYPE_INTEGER &&
                    (res < INT32_MIN || res > INT32_MAX)) {
                    res_type = PRDB_SDK_VALUE_TYPE_BIGINT;
                }
                set_i64_typed(out, res_type, res);
                return PRDB_SDK_SQL_EVAL_OK;
            }
            /* Decimal arithmetic */
            if ((is_int_type(lv.val.type_code) ||
                 lv.val.type_code == PRDB_SDK_VALUE_TYPE_DECIMAL) &&
                (is_int_type(rv.val.type_code) ||
                 rv.val.type_code == PRDB_SDK_VALUE_TYPE_DECIMAL)) {
                uint8_t ls = expr_scale(expr->u.binary.left);
                uint8_t rs = expr_scale(expr->u.binary.right);
                uint8_t lb[16], rb[16], res[16];
                if (op == PRDB_SQL_BOP_ADD || op == PRDB_SQL_BOP_SUB) {
                    uint8_t target = ls > rs ? ls : rs;
                    if (!to_decimal_with_scale(&lv.val, ls, target, lb) ||
                        !to_decimal_with_scale(&rv.val, rs, target, rb)) {
                        prdb_sql_owned_val_free(&lv);
                        prdb_sql_owned_val_free(&rv);
                        return PRDB_SDK_SQL_EVAL_NUMERIC_OVERFLOW;
                    }
                    int ok = op == PRDB_SQL_BOP_ADD ? i128_add_checked(lb, rb, res)
                                                    : i128_sub_checked(lb, rb, res);
                    prdb_sql_owned_val_free(&lv);
                    prdb_sql_owned_val_free(&rv);
                    if (!ok) return PRDB_SDK_SQL_EVAL_NUMERIC_OVERFLOW;
                    set_decimal(out, res);
                    return PRDB_SDK_SQL_EVAL_OK;
                }
                if (op == PRDB_SQL_BOP_MUL) {
                    if (!to_decimal_with_scale(&lv.val, ls, ls, lb) ||
                        !to_decimal_with_scale(&rv.val, rs, rs, rb) ||
                        !i128_mul_checked(lb, rb, res)) {
                        prdb_sql_owned_val_free(&lv);
                        prdb_sql_owned_val_free(&rv);
                        return PRDB_SDK_SQL_EVAL_NUMERIC_OVERFLOW;
                    }
                    prdb_sql_owned_val_free(&lv);
                    prdb_sql_owned_val_free(&rv);
                    set_decimal(out, res);
                    return PRDB_SDK_SQL_EVAL_OK;
                }
                if (op == PRDB_SQL_BOP_DIV) {
                    uint8_t target = expr->is_bound &&
                                     expr->bound_type == PRDB_SDK_VALUE_TYPE_DECIMAL
                                         ? expr->bound_scale
                                         : 6;
                    if (!to_decimal_with_scale(&lv.val, ls, ls, lb) ||
                        !to_decimal_with_scale(&rv.val, rs, rs, rb)) {
                        prdb_sql_owned_val_free(&lv);
                        prdb_sql_owned_val_free(&rv);
                        return PRDB_SDK_SQL_EVAL_NUMERIC_OVERFLOW;
                    }
                    int neg = i128_is_neg(lb) ^ i128_is_neg(rb);
                    uint8_t ulb[16], urb[16];
                    if (i128_is_neg(lb)) i128_neg(lb, ulb); else memcpy(ulb, lb, 16);
                    if (i128_is_neg(rb)) i128_neg(rb, urb); else memcpy(urb, rb, 16);
                    uint8_t num256[32] = {0};
                    memcpy(num256 + 16, ulb, 16);
                    int32_t scale_mul = (int32_t)target + (int32_t)rs - (int32_t)ls;
                    while (scale_mul > 0) {
                        uint32_t carry = 0;
                        for (int k = 31; k >= 0; --k) {
                            uint32_t cur = (uint32_t)num256[k] * 10 + carry;
                            num256[k] = (uint8_t)(cur & 0xff);
                            carry = cur >> 8;
                        }
                        --scale_mul;
                    }
                    while (scale_mul < 0) {
                        u256_div_u32(num256, 10);
                        ++scale_mul;
                    }
                    uint8_t quot[16], rem[16];
                    int ok = u256_div_u128(num256, urb, quot, rem);
                    prdb_sql_owned_val_free(&lv);
                    prdb_sql_owned_val_free(&rv);
                    if (!ok || (quot[0] & 0x80)) {
                        return PRDB_SDK_SQL_EVAL_NUMERIC_OVERFLOW;
                    }
                    if (neg) i128_neg(quot, res); else memcpy(res, quot, 16);
                    set_decimal(out, res);
                    return PRDB_SDK_SQL_EVAL_OK;
                }
            }
            prdb_sql_owned_val_free(&lv);
            prdb_sql_owned_val_free(&rv);
            return PRDB_SDK_SQL_EVAL_TYPE_MISMATCH;
        }
        case PRDB_SQL_EXPR_BETWEEN:
        case PRDB_SQL_EXPR_IN_LIST: {
            uint32_t truth = 0;
            status = prdb_sql_eval_truth_v1(expr, ctx, &truth);
            if (status != PRDB_SDK_SQL_EVAL_OK) return status;
            if (truth == PRDB_SDK_SQL_TRUTH_UNKNOWN) set_null(out);
            else set_bool(out, truth == PRDB_SDK_SQL_TRUTH_TRUE);
            return PRDB_SDK_SQL_EVAL_OK;
        }
        case PRDB_SQL_EXPR_CASE: {
            prdb_sql_owned_val opv = {0};
            if (expr->u.case_expr.operand != NULL) {
                status = prdb_sql_eval_expr_v1(expr->u.case_expr.operand, ctx,
                                               &opv);
                if (status != PRDB_SDK_SQL_EVAL_OK) {
                    prdb_sql_owned_val_free(&opv);
                    return status;
                }
            }
            for (uint32_t i = 0; i < expr->u.case_expr.branch_count; ++i) {
                if (expr->u.case_expr.operand == NULL) {
                    uint32_t truth = 0;
                    status = prdb_sql_eval_truth_v1(
                        expr->u.case_expr.when_exprs[i], ctx, &truth);
                    if (status != PRDB_SDK_SQL_EVAL_OK) return status;
                    if (truth == PRDB_SDK_SQL_TRUTH_TRUE) {
                        return prdb_sql_eval_expr_v1(
                            expr->u.case_expr.then_exprs[i], ctx, out);
                    }
                } else {
                    prdb_sql_owned_val wv = {0};
                    uint32_t truth = 0;
                    status = prdb_sql_eval_expr_v1(
                        expr->u.case_expr.when_exprs[i], ctx, &wv);
                    if (status != PRDB_SDK_SQL_EVAL_OK) {
                        prdb_sql_owned_val_free(&opv);
                        prdb_sql_owned_val_free(&wv);
                        return status;
                    }
                    status = eval_compare(expr->u.case_expr.operand, &opv,
                                          expr->u.case_expr.when_exprs[i], &wv,
                                          PRDB_SDK_SQL_COMPARE_EQUAL, &truth);
                    prdb_sql_owned_val_free(&wv);
                    if (status != PRDB_SDK_SQL_EVAL_OK) {
                        prdb_sql_owned_val_free(&opv);
                        return status;
                    }
                    if (truth == PRDB_SDK_SQL_TRUTH_TRUE) {
                        prdb_sql_owned_val_free(&opv);
                        return prdb_sql_eval_expr_v1(
                            expr->u.case_expr.then_exprs[i], ctx, out);
                    }
                }
            }
            prdb_sql_owned_val_free(&opv);
            if (expr->u.case_expr.else_expr != NULL) {
                return prdb_sql_eval_expr_v1(expr->u.case_expr.else_expr, ctx,
                                             out);
            }
            set_null(out);
            return PRDB_SDK_SQL_EVAL_OK;
        }
        case PRDB_SQL_EXPR_EXTRACT: {
            prdb_sql_owned_val av = {0};
            status = prdb_sql_eval_expr_v1(expr->u.extract.arg, ctx, &av);
            if (status != PRDB_SDK_SQL_EVAL_OK) {
                prdb_sql_owned_val_free(&av);
                return status;
            }
            if (av.val.state == PRDB_SDK_VALUE_STATE_NULL) {
                set_null(out);
                prdb_sql_owned_val_free(&av);
                return PRDB_SDK_SQL_EVAL_OK;
            }
            int32_t days = 0;
            if (av.val.type_code == PRDB_SDK_VALUE_TYPE_DATE) {
                prdb_sdk_value_get_date_v1(&av.val, &days);
            } else if (av.val.type_code == PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ) {
                int64_t us = 0;
                prdb_sdk_value_get_timestamptz_v1(&av.val, &us);
                days = (int32_t)(us >= 0 ? us / 86400000000LL
                                         : (us - 86399999999LL) / 86400000000LL);
            } else {
                prdb_sql_owned_val_free(&av);
                return PRDB_SDK_SQL_EVAL_TYPE_MISMATCH;
            }
            prdb_sql_owned_val_free(&av);
            int64_t y;
            unsigned m, d;
            civil_from_days(days, &y, &m, &d);
            int64_t res = expr->u.extract.field == PRDB_SQL_INTERVAL_YEAR ? y
                          : expr->u.extract.field == PRDB_SQL_INTERVAL_MONTH
                              ? (int64_t)m
                              : (int64_t)d;
            set_i64_typed(out, PRDB_SDK_VALUE_TYPE_INTEGER, res);
            return PRDB_SDK_SQL_EVAL_OK;
        }
        case PRDB_SQL_EXPR_SUBSTRING: {
            prdb_sql_owned_val sv = {0}, stv = {0}, lenv = {0};
            status = prdb_sql_eval_expr_v1(expr->u.substring.str, ctx, &sv);
            if (status != PRDB_SDK_SQL_EVAL_OK) {
                prdb_sql_owned_val_free(&sv);
                return status;
            }
            status = prdb_sql_eval_expr_v1(expr->u.substring.start, ctx, &stv);
            if (status != PRDB_SDK_SQL_EVAL_OK) {
                prdb_sql_owned_val_free(&sv);
                prdb_sql_owned_val_free(&stv);
                return status;
            }
            if (expr->u.substring.len != NULL) {
                status =
                    prdb_sql_eval_expr_v1(expr->u.substring.len, ctx, &lenv);
                if (status != PRDB_SDK_SQL_EVAL_OK) {
                    prdb_sql_owned_val_free(&sv);
                    prdb_sql_owned_val_free(&stv);
                    prdb_sql_owned_val_free(&lenv);
                    return status;
                }
            }
            if (sv.val.state == PRDB_SDK_VALUE_STATE_NULL ||
                stv.val.state == PRDB_SDK_VALUE_STATE_NULL ||
                (expr->u.substring.len != NULL &&
                 lenv.val.state == PRDB_SDK_VALUE_STATE_NULL)) {
                set_null(out);
                prdb_sql_owned_val_free(&sv);
                prdb_sql_owned_val_free(&stv);
                prdb_sql_owned_val_free(&lenv);
                return PRDB_SDK_SQL_EVAL_OK;
            }
            if ((sv.val.type_code != PRDB_SDK_VALUE_TYPE_TEXT &&
                 sv.val.type_code != PRDB_SDK_VALUE_TYPE_VARCHAR) ||
                !is_int_type(stv.val.type_code) ||
                (expr->u.substring.len != NULL &&
                 !is_int_type(lenv.val.type_code))) {
                prdb_sql_owned_val_free(&sv);
                prdb_sql_owned_val_free(&stv);
                prdb_sql_owned_val_free(&lenv);
                return PRDB_SDK_SQL_EVAL_TYPE_MISMATCH;
            }
            int64_t start_pos = 1, want_len = INT64_MAX;
            get_i64_value(&stv.val, &start_pos);
            if (expr->u.substring.len != NULL) {
                get_i64_value(&lenv.val, &want_len);
            }
            if (want_len < 0) want_len = 0;
            /* 1-based UTF-8 char slicing */
            const uint8_t *s = sv.val.data;
            uint64_t slen = sv.val.data_len;
            uint64_t byte_start = 0, byte_end = 0;
            int64_t char_idx = 1;
            uint64_t i = 0;
            while (i < slen && char_idx < start_pos) {
                ++i;
                while (i < slen && (s[i] & 0xc0) == 0x80) ++i;
                ++char_idx;
            }
            byte_start = i;
            int64_t taken = 0;
            if (start_pos < 1) {
                int64_t skip = 1 - start_pos;
                if (want_len <= skip) want_len = 0;
                else want_len -= skip;
            }
            while (i < slen && taken < want_len) {
                ++i;
                while (i < slen && (s[i] & 0xc0) == 0x80) ++i;
                ++taken;
            }
            byte_end = i;
            status = set_bytes_copy(out, PRDB_SDK_VALUE_TYPE_TEXT,
                                    s + byte_start, byte_end - byte_start);
            prdb_sql_owned_val_free(&sv);
            prdb_sql_owned_val_free(&stv);
            prdb_sql_owned_val_free(&lenv);
            return status;
        }
        case PRDB_SQL_EXPR_COALESCE: {
            for (uint32_t i = 0; i < expr->u.coalesce.arg_count; ++i) {
                prdb_sql_owned_val v = {0};
                status =
                    prdb_sql_eval_expr_v1(expr->u.coalesce.args[i], ctx, &v);
                if (status != PRDB_SDK_SQL_EVAL_OK) {
                    prdb_sql_owned_val_free(&v);
                    return status;
                }
                if (v.val.state == PRDB_SDK_VALUE_STATE_VALUE) {
                    status = copy_value_to_owned(&v.val, out);
                    prdb_sql_owned_val_free(&v);
                    return status;
                }
                prdb_sql_owned_val_free(&v);
            }
            set_null(out);
            return PRDB_SDK_SQL_EVAL_OK;
        }
        default:
            return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
}

PRDB_SDK_INTERNAL int32_t prdb_sql_eval_truth_v1(
    const prdb_sql_expr *expr, const prdb_sql_eval_row_set *ctx,
    uint32_t *truth_out) {
    int32_t status;
    if (expr == NULL || ctx == NULL || truth_out == NULL) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (expr->kind == PRDB_SQL_EXPR_UNARY) {
        if (expr->u.unary.op == PRDB_SQL_UOP_NOT) {
            uint32_t t = 0;
            status = prdb_sql_eval_truth_v1(expr->u.unary.operand, ctx, &t);
            if (status != PRDB_SDK_SQL_EVAL_OK) return status;
            return prdb_sdk_sql_truth_not_v1(t, truth_out);
        }
        if (expr->u.unary.op == PRDB_SQL_UOP_IS_NULL ||
            expr->u.unary.op == PRDB_SQL_UOP_IS_NOT_NULL) {
            prdb_sql_owned_val v = {0};
            status = prdb_sql_eval_expr_v1(expr->u.unary.operand, ctx, &v);
            if (status != PRDB_SDK_SQL_EVAL_OK) {
                prdb_sql_owned_val_free(&v);
                return status;
            }
            uint32_t is_null = 0;
            status = prdb_sdk_sql_value_is_null_v1(&v.val, &is_null);
            prdb_sql_owned_val_free(&v);
            if (status != PRDB_SDK_SQL_EVAL_OK) return status;
            if (expr->u.unary.op == PRDB_SQL_UOP_IS_NOT_NULL) {
                return prdb_sdk_sql_truth_not_v1(is_null, truth_out);
            }
            *truth_out = is_null;
            return PRDB_SDK_SQL_EVAL_OK;
        }
    }
    if (expr->kind == PRDB_SQL_EXPR_BINARY) {
        prdb_sql_bin_op op = expr->u.binary.op;
        if (op == PRDB_SQL_BOP_AND || op == PRDB_SQL_BOP_OR) {
            uint32_t lt = 0, rt = 0;
            status = prdb_sql_eval_truth_v1(expr->u.binary.left, ctx, &lt);
            if (status != PRDB_SDK_SQL_EVAL_OK) return status;
            if (op == PRDB_SQL_BOP_AND && lt == PRDB_SDK_SQL_TRUTH_FALSE) {
                *truth_out = PRDB_SDK_SQL_TRUTH_FALSE;
                return PRDB_SDK_SQL_EVAL_OK;
            }
            if (op == PRDB_SQL_BOP_OR && lt == PRDB_SDK_SQL_TRUTH_TRUE) {
                *truth_out = PRDB_SDK_SQL_TRUTH_TRUE;
                return PRDB_SDK_SQL_EVAL_OK;
            }
            status = prdb_sql_eval_truth_v1(expr->u.binary.right, ctx, &rt);
            if (status != PRDB_SDK_SQL_EVAL_OK) return status;
            return op == PRDB_SQL_BOP_AND
                       ? prdb_sdk_sql_truth_and_v1(lt, rt, truth_out)
                       : prdb_sdk_sql_truth_or_v1(lt, rt, truth_out);
        }
        if (op == PRDB_SQL_BOP_LIKE || op == PRDB_SQL_BOP_NOT_LIKE) {
            prdb_sql_owned_val lv = {0}, rv = {0};
            status = prdb_sql_eval_expr_v1(expr->u.binary.left, ctx, &lv);
            if (status != PRDB_SDK_SQL_EVAL_OK) {
                prdb_sql_owned_val_free(&lv);
                return status;
            }
            status = prdb_sql_eval_expr_v1(expr->u.binary.right, ctx, &rv);
            if (status != PRDB_SDK_SQL_EVAL_OK) {
                prdb_sql_owned_val_free(&lv);
                prdb_sql_owned_val_free(&rv);
                return status;
            }
            if (lv.val.state == PRDB_SDK_VALUE_STATE_NULL ||
                rv.val.state == PRDB_SDK_VALUE_STATE_NULL) {
                *truth_out = PRDB_SDK_SQL_TRUTH_UNKNOWN;
                prdb_sql_owned_val_free(&lv);
                prdb_sql_owned_val_free(&rv);
                return PRDB_SDK_SQL_EVAL_OK;
            }
            if ((lv.val.type_code != PRDB_SDK_VALUE_TYPE_TEXT &&
                 lv.val.type_code != PRDB_SDK_VALUE_TYPE_VARCHAR) ||
                (rv.val.type_code != PRDB_SDK_VALUE_TYPE_TEXT &&
                 rv.val.type_code != PRDB_SDK_VALUE_TYPE_VARCHAR)) {
                prdb_sql_owned_val_free(&lv);
                prdb_sql_owned_val_free(&rv);
                return PRDB_SDK_SQL_EVAL_TYPE_MISMATCH;
            }
            int matched = like_match(lv.val.data, lv.val.data_len, rv.val.data,
                                     rv.val.data_len);
            if (op == PRDB_SQL_BOP_NOT_LIKE) matched = !matched;
            *truth_out =
                matched ? PRDB_SDK_SQL_TRUTH_TRUE : PRDB_SDK_SQL_TRUTH_FALSE;
            prdb_sql_owned_val_free(&lv);
            prdb_sql_owned_val_free(&rv);
            return PRDB_SDK_SQL_EVAL_OK;
        }
        if (op == PRDB_SQL_BOP_EQ || op == PRDB_SQL_BOP_NE ||
            op == PRDB_SQL_BOP_LT || op == PRDB_SQL_BOP_LE ||
            op == PRDB_SQL_BOP_GT || op == PRDB_SQL_BOP_GE) {
            prdb_sql_owned_val lv = {0}, rv = {0};
            status = prdb_sql_eval_expr_v1(expr->u.binary.left, ctx, &lv);
            if (status != PRDB_SDK_SQL_EVAL_OK) {
                prdb_sql_owned_val_free(&lv);
                return status;
            }
            status = prdb_sql_eval_expr_v1(expr->u.binary.right, ctx, &rv);
            if (status != PRDB_SDK_SQL_EVAL_OK) {
                prdb_sql_owned_val_free(&lv);
                prdb_sql_owned_val_free(&rv);
                return status;
            }
            status = eval_compare(expr->u.binary.left, &lv,
                                  expr->u.binary.right, &rv, op == PRDB_SQL_BOP_EQ ? PRDB_SDK_SQL_COMPARE_EQUAL : binop_to_cmp(op),
                                  truth_out);
            prdb_sql_owned_val_free(&lv);
            prdb_sql_owned_val_free(&rv);
            return status;
        }
    }
    if (expr->kind == PRDB_SQL_EXPR_BETWEEN) {
        prdb_sql_owned_val ev = {0}, lv = {0}, hv = {0};
        uint32_t t1 = 0, t2 = 0, tand = 0;
        status = prdb_sql_eval_expr_v1(expr->u.between.expr, ctx, &ev);
        if (status != PRDB_SDK_SQL_EVAL_OK) { prdb_sql_owned_val_free(&ev); return status; }
        status = prdb_sql_eval_expr_v1(expr->u.between.low, ctx, &lv);
        if (status != PRDB_SDK_SQL_EVAL_OK) { prdb_sql_owned_val_free(&ev); prdb_sql_owned_val_free(&lv); return status; }
        status = prdb_sql_eval_expr_v1(expr->u.between.high, ctx, &hv);
        if (status != PRDB_SDK_SQL_EVAL_OK) { prdb_sql_owned_val_free(&ev); prdb_sql_owned_val_free(&lv); prdb_sql_owned_val_free(&hv); return status; }
        status = eval_compare(expr->u.between.expr, &ev, expr->u.between.low,
                              &lv, PRDB_SDK_SQL_COMPARE_GREATER_EQUAL, &t1);
        if (status == PRDB_SDK_SQL_EVAL_OK) {
            status = eval_compare(expr->u.between.expr, &ev,
                                  expr->u.between.high, &hv,
                                  PRDB_SDK_SQL_COMPARE_LESS_EQUAL, &t2);
        }
        prdb_sql_owned_val_free(&ev);
        prdb_sql_owned_val_free(&lv);
        prdb_sql_owned_val_free(&hv);
        if (status != PRDB_SDK_SQL_EVAL_OK) return status;
        status = prdb_sdk_sql_truth_and_v1(t1, t2, &tand);
        if (status != PRDB_SDK_SQL_EVAL_OK) return status;
        return expr->u.between.negated ? prdb_sdk_sql_truth_not_v1(tand, truth_out)
                                       : (*truth_out = tand, PRDB_SDK_SQL_EVAL_OK);
    }
    if (expr->kind == PRDB_SQL_EXPR_IN_LIST) {
        prdb_sql_owned_val ev = {0};
        status = prdb_sql_eval_expr_v1(expr->u.in_list.expr, ctx, &ev);
        if (status != PRDB_SDK_SQL_EVAL_OK) {
            prdb_sql_owned_val_free(&ev);
            return status;
        }
        uint32_t acc = PRDB_SDK_SQL_TRUTH_FALSE;
        for (uint32_t i = 0; i < expr->u.in_list.item_count; ++i) {
            prdb_sql_owned_val iv = {0};
            uint32_t eq = 0;
            status =
                prdb_sql_eval_expr_v1(expr->u.in_list.items[i], ctx, &iv);
            if (status != PRDB_SDK_SQL_EVAL_OK) {
                prdb_sql_owned_val_free(&ev);
                prdb_sql_owned_val_free(&iv);
                return status;
            }
            status = eval_compare(expr->u.in_list.expr, &ev,
                                  expr->u.in_list.items[i], &iv,
                                  PRDB_SDK_SQL_COMPARE_EQUAL, &eq);
            prdb_sql_owned_val_free(&iv);
            if (status != PRDB_SDK_SQL_EVAL_OK) {
                prdb_sql_owned_val_free(&ev);
                return status;
            }
            status = prdb_sdk_sql_truth_or_v1(acc, eq, &acc);
            if (status != PRDB_SDK_SQL_EVAL_OK) {
                prdb_sql_owned_val_free(&ev);
                return status;
            }
            if (acc == PRDB_SDK_SQL_TRUTH_TRUE) break;
        }
        prdb_sql_owned_val_free(&ev);
        return expr->u.in_list.negated
                   ? prdb_sdk_sql_truth_not_v1(acc, truth_out)
                   : (*truth_out = acc, PRDB_SDK_SQL_EVAL_OK);
    }
    /* Fallback: evaluate expression as boolean */
    prdb_sql_owned_val v = {0};
    status = prdb_sql_eval_expr_v1(expr, ctx, &v);
    if (status != PRDB_SDK_SQL_EVAL_OK) {
        prdb_sql_owned_val_free(&v);
        return status;
    }
    status = prdb_sdk_sql_value_truth_v1(&v.val, truth_out);
    prdb_sql_owned_val_free(&v);
    return status;
}
