#include "sql_ast_internal.h"
#include "prdb_sdk_sql_result_v1.h"
#include "prdb_sdk_sql_dispatch_v1.h"
#include "prdb_sdk_planner_v1.h"
#include "prdb_sdk_batch_v1.h"

#include <stdlib.h>
#include <string.h>

#define EXEC_RESP_BYTES UINT64_C(1048576)
#define EXEC_REQ_BYTES PRDB_QUERY_MAX_IPC_REQUEST_BYTES

/* --- Portable 128-bit big-endian signed helpers (no __int128 in TinyCC) --- */

static void be128_neg(const uint8_t a[16], uint8_t out[16]) {
    uint16_t carry = 1;
    for (int i = 15; i >= 0; --i) {
        uint16_t v = (uint16_t)(uint8_t)(~a[i]) + carry;
        out[i] = (uint8_t)(v & 0xff);
        carry = v >> 8;
    }
}

static void be128_from_i64(int64_t v, uint8_t out[16]) {
    uint8_t fill = v < 0 ? 0xff : 0x00;
    memset(out, fill, 8);
    uint64_t u = (uint64_t)v;
    for (int i = 0; i < 8; ++i) out[8 + i] = (uint8_t)(u >> (56 - i * 8));
}

static void be128_add(uint8_t acc[16], const uint8_t b[16]) {
    uint16_t carry = 0;
    for (int i = 15; i >= 0; --i) {
        uint16_t s = (uint16_t)acc[i] + b[i] + carry;
        acc[i] = (uint8_t)(s & 0xff);
        carry = s >> 8;
    }
}

static void be128_mul_u32_signed(uint8_t a[16], uint32_t m) {
    int neg = (a[0] & 0x80) != 0;
    uint8_t mag[16];
    if (neg) be128_neg(a, mag); else memcpy(mag, a, 16);
    uint64_t carry = 0;
    for (int i = 15; i >= 0; --i) {
        uint64_t p = (uint64_t)mag[i] * m + carry;
        mag[i] = (uint8_t)(p & 0xff);
        carry = p >> 8;
    }
    if (neg) be128_neg(mag, a); else memcpy(a, mag, 16);
}

static void be128_div_u32_signed(uint8_t a[16], uint32_t d) {
    int neg = (a[0] & 0x80) != 0;
    uint8_t mag[16];
    if (neg) be128_neg(a, mag); else memcpy(mag, a, 16);
    uint64_t rem = 0;
    for (int i = 0; i < 16; ++i) {
        uint64_t cur = (rem << 8) | mag[i];
        mag[i] = (uint8_t)(cur / d);
        rem = cur % d;
    }
    if (neg) be128_neg(mag, a); else memcpy(a, mag, 16);
}

/* --- Materialized row-table representation --- */

typedef struct mat_row {
    prdb_sdk_value_v1 *cells;
    uint8_t row_handle[16];
    uint8_t has_handle;
} mat_row;

typedef struct mat_table {
    prdb_sdk_sql_column_shape_v1 *cols;
    uint32_t col_count;
    mat_row *rows;
    uint32_t row_count;
    uint32_t row_cap;
    prdb_sql_arena arena; /* owns copied cell byte payloads & col names */
} mat_table;

static void mat_table_init(mat_table *t, size_t max_bytes) {
    memset(t, 0, sizeof(*t));
    prdb_sql_arena_init(&t->arena, max_bytes);
}

static void mat_table_free(mat_table *t) {
    if (t == NULL) return;
    free(t->cols);
    free(t->rows);
    prdb_sql_arena_free(&t->arena);
    memset(t, 0, sizeof(*t));
}

static int mat_table_append_row(mat_table *t, const prdb_sdk_value_v1 *vals,
                                const uint8_t *handle) {
    if (t->row_count == t->row_cap) {
        uint32_t ncap = t->row_cap == 0 ? 64 : t->row_cap * 2;
        mat_row *nr = (mat_row *)realloc(t->rows, ncap * sizeof(mat_row));
        if (nr == NULL) return 0;
        t->rows = nr;
        t->row_cap = ncap;
    }
    prdb_sdk_value_v1 *cells = (prdb_sdk_value_v1 *)prdb_sql_arena_alloc(
        &t->arena,
        t->col_count == 0 ? sizeof(prdb_sdk_value_v1)
                          : t->col_count * sizeof(prdb_sdk_value_v1),
        8);
    if (cells == NULL) return 0;
    for (uint32_t c = 0; c < t->col_count; ++c) {
        cells[c] = vals[c];
        if (vals[c].state == PRDB_SDK_VALUE_STATE_VALUE &&
            vals[c].data_len > 0 && vals[c].data != NULL) {
            uint8_t *buf = (uint8_t *)prdb_sql_arena_alloc(
                &t->arena, (size_t)vals[c].data_len, 1);
            if (buf == NULL) return 0;
            memcpy(buf, vals[c].data, (size_t)vals[c].data_len);
            cells[c].data = buf;
        }
    }
    mat_row *r = &t->rows[t->row_count++];
    r->cells = cells;
    if (handle != NULL) {
        memcpy(r->row_handle, handle, 16);
        r->has_handle = 1;
    } else {
        r->has_handle = 0;
    }
    return 1;
}

typedef struct cte_entry {
    const uint8_t *name;
    uint32_t name_len;
    mat_table data;
} cte_entry;

typedef struct exec_ctx {
    const prdb_host_api_v1 *host;
    const uint8_t *schema;
    uint32_t schema_len;
    const prdb_sdk_value_v1 *params;
    uint32_t param_count;
    const prdb_sdk_sql_parameter_metadata_v1 *param_meta;
    prdb_sdk_workload_budgets_v1 budgets;
    uint8_t *resp_buf;
    uint8_t *batch_buf;
    uint8_t *req_buf;
    uint8_t *result_buf;
    cte_entry *ctes;
    uint32_t cte_count;
} exec_ctx;

static int bytes_ieq(const uint8_t *a, uint32_t alen, const uint8_t *b,
                     uint32_t blen) {
    if (alen != blen) return 0;
    for (uint32_t i = 0; i < alen; ++i) {
        uint8_t ca = a[i], cb = b[i];
        if (ca >= 'A' && ca <= 'Z') ca = (uint8_t)(ca + ('a' - 'A'));
        if (cb >= 'A' && cb <= 'Z') cb = (uint8_t)(cb + ('a' - 'A'));
        if (ca != cb) return 0;
    }
    return 1;
}

/* --- Subquery cache --- */

typedef struct subq_cache_entry {
    const prdb_sql_select_ast *ast;
    mat_table result;
} subq_cache_entry;

typedef struct subq_runtime {
    exec_ctx *ec;
    subq_cache_entry *cache;
    uint32_t cache_count;
    uint32_t cache_cap;
} subq_runtime;

static int expr_has_outer_ref(const prdb_sql_expr *e);

static int select_has_outer_ref(const prdb_sql_select_ast *s) {
    if (s == NULL) return 0;
    for (uint32_t i = 0; i < s->item_count; ++i) {
        if (!s->items[i].is_star && expr_has_outer_ref(s->items[i].expr))
            return 1;
    }
    if (expr_has_outer_ref(s->where_expr)) return 1;
    for (uint32_t i = 0; i < s->group_by_count; ++i) {
        if (expr_has_outer_ref(s->group_by[i])) return 1;
    }
    if (expr_has_outer_ref(s->having_expr)) return 1;
    for (uint32_t i = 0; i < s->order_by_count; ++i) {
        if (expr_has_outer_ref(s->order_by[i].expr)) return 1;
    }
    return 0;
}

static int expr_has_outer_ref(const prdb_sql_expr *e) {
    if (e == NULL) return 0;
    switch (e->kind) {
        case PRDB_SQL_EXPR_COLUMN:
            return e->u.col.scope_depth > 0;
        case PRDB_SQL_EXPR_UNARY:
            return expr_has_outer_ref(e->u.unary.operand);
        case PRDB_SQL_EXPR_BINARY:
            return expr_has_outer_ref(e->u.binary.left) ||
                   expr_has_outer_ref(e->u.binary.right);
        case PRDB_SQL_EXPR_BETWEEN:
            return expr_has_outer_ref(e->u.between.expr) ||
                   expr_has_outer_ref(e->u.between.low) ||
                   expr_has_outer_ref(e->u.between.high);
        case PRDB_SQL_EXPR_IN_LIST:
            if (expr_has_outer_ref(e->u.in_list.expr)) return 1;
            for (uint32_t i = 0; i < e->u.in_list.item_count; ++i) {
                if (expr_has_outer_ref(e->u.in_list.items[i])) return 1;
            }
            return 0;
        case PRDB_SQL_EXPR_CASE:
            if (expr_has_outer_ref(e->u.case_expr.operand)) return 1;
            for (uint32_t i = 0; i < e->u.case_expr.branch_count; ++i) {
                if (expr_has_outer_ref(e->u.case_expr.when_exprs[i]) ||
                    expr_has_outer_ref(e->u.case_expr.then_exprs[i]))
                    return 1;
            }
            return expr_has_outer_ref(e->u.case_expr.else_expr);
        case PRDB_SQL_EXPR_EXTRACT:
            return expr_has_outer_ref(e->u.extract.arg);
        case PRDB_SQL_EXPR_SUBSTRING:
            return expr_has_outer_ref(e->u.substring.str) ||
                   expr_has_outer_ref(e->u.substring.start) ||
                   expr_has_outer_ref(e->u.substring.len);
        case PRDB_SQL_EXPR_COALESCE:
            for (uint32_t i = 0; i < e->u.coalesce.arg_count; ++i) {
                if (expr_has_outer_ref(e->u.coalesce.args[i])) return 1;
            }
            return 0;
        case PRDB_SQL_EXPR_AGGREGATE:
            return expr_has_outer_ref(e->u.agg.arg);
        case PRDB_SQL_EXPR_SUBQUERY:
            return expr_has_outer_ref(e->u.subquery.lhs) ||
                   select_has_outer_ref(e->u.subquery.subquery);
        default:
            return 0;
    }
}

/* --- Forward declarations --- */
static int32_t exec_select_to_table(exec_ctx *ec, prdb_sql_select_ast *sel,
                                    const prdb_sql_eval_scope *outer_scopes,
                                    uint32_t outer_scope_count,
                                    mat_table *out_table);

/* --- Table scan helper --- */

static int32_t scan_base_relation(exec_ctx *ec, const uint8_t *schema,
                                  uint32_t schema_len, const uint8_t *rel_name,
                                  uint32_t rel_len, uint8_t keep_handles,
                                  mat_table *out) {
    prdb_sdk_executor_relation_v1 rel = {0};
    prdb_sdk_sql_column_shape_v1 *cols =
        (prdb_sdk_sql_column_shape_v1 *)calloc(
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, sizeof(*cols));
    prdb_sdk_value_v1 *cells = (prdb_sdk_value_v1 *)calloc(
        PRDB_SDK_SQL_MAX_RELATION_COLUMNS, sizeof(*cells));
    uint64_t resp_len = 0;
    int32_t status;
    uint8_t cursor[16] = {0};
    uint8_t cursor_open = 0;

    if (cols == NULL || cells == NULL) {
        free(cols);
        free(cells);
        return PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
    }
    status = prdb_sdk_executor_resolve_relation_v1(
        ec->host, schema, schema_len, rel_name, rel_len, ec->resp_buf,
        EXEC_RESP_BYTES, &resp_len, &rel, sizeof(rel), cols,
        PRDB_SDK_SQL_MAX_RELATION_COLUMNS);
    if (status != PRDB_SDK_EXECUTOR_OK) {
        free(cols);
        free(cells);
        return PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
    }
    out->col_count = rel.column_count;
    out->cols = (prdb_sdk_sql_column_shape_v1 *)calloc(
        rel.column_count == 0 ? 1 : rel.column_count,
        sizeof(prdb_sdk_sql_column_shape_v1));
    if (out->cols == NULL) {
        free(cols);
        free(cells);
        return PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
    }
    for (uint32_t c = 0; c < rel.column_count; ++c) {
        out->cols[c] = cols[c];
        out->cols[c].name = (const uint8_t *)prdb_sql_arena_strdup(
            &out->arena, cols[c].name, cols[c].name_len);
        if (out->cols[c].name == NULL) {
            free(cols);
            free(cells);
            return PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
        }
    }

    status = prdb_sdk_executor_open_full_scan_v1(
        ec->host, rel.executor_handle, ec->resp_buf, EXEC_RESP_BYTES, &resp_len,
        cursor);
    if (status != PRDB_SDK_EXECUTOR_OK) {
        free(cols);
        free(cells);
        return PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
    }
    cursor_open = 1;

    uint32_t max_pages = ec->budgets.sql_exec_max_scan_pages;
    if (max_pages == 0) max_pages = 1000000;
    for (uint32_t page = 0; page < max_pages; ++page) {
        int32_t cancelled = 0;
        status = prdb_sdk_executor_check_cancelled_v1(ec->host, &cancelled);
        if (status != PRDB_SDK_EXECUTOR_OK || cancelled != 0) {
            status = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
            goto fail;
        }
        prdb_sdk_batch_v1 batch = {0};
        status = prdb_sdk_executor_next_batch_v1(
            ec->host, cursor, PRDB_SDK_BATCH_MAX_ROWS,
            (uint32_t)EXEC_RESP_BYTES, ec->batch_buf, EXEC_RESP_BYTES,
            &resp_len, &batch, sizeof(batch));
        if (status != PRDB_SDK_EXECUTOR_OK) {
            status = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
            goto fail;
        }
        prdb_sdk_row_iterator_v1 riter = {0};
        if (prdb_sdk_batch_rows_begin_v1(&batch, &riter, sizeof(riter)) !=
            PRDB_SDK_BATCH_OK) {
            status = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
            goto fail;
        }
        for (uint32_t r = 0; r < batch.row_count; ++r) {
            prdb_sdk_row_v1 row = {0};
            if (prdb_sdk_batch_row_next_v1(&riter, &row, sizeof(row)) !=
                PRDB_SDK_BATCH_OK) {
                status = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
                goto fail;
            }
            if (row.cell_count != rel.column_count) {
                status = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
                goto fail;
            }
            prdb_sdk_cell_iterator_v1 citer = {0};
            if (prdb_sdk_row_cells_begin_v1(&row, &citer, sizeof(citer)) !=
                PRDB_SDK_BATCH_OK) {
                status = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
                goto fail;
            }
            for (uint32_t c = 0; c < rel.column_count; ++c) {
                if (prdb_sdk_cell_next_v1(&citer, &cells[c],
                                          sizeof(cells[c])) !=
                    PRDB_SDK_BATCH_OK) {
                    status = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
                    goto fail;
                }
            }
            if (!mat_table_append_row(
                    out, cells,
                    keep_handles && row.handle != NULL ? row.handle : NULL)) {
                status = PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
                goto fail;
            }
        }
        if (batch.finished || batch.continuation == NULL) {
            cursor_open = 0;
            break;
        }
        memcpy(cursor, batch.continuation, 16);
    }

    if (cursor_open) {
        prdb_sdk_executor_close_cursor_v1(ec->host, cursor, ec->resp_buf,
                                          EXEC_RESP_BYTES, &resp_len);
    }
    free(cols);
    free(cells);
    return PRDB_SDK_SQL_DISPATCH_OK;

fail:
    if (cursor_open) {
        prdb_sdk_executor_close_cursor_v1(ec->host, cursor, ec->resp_buf,
                                          EXEC_RESP_BYTES, &resp_len);
    }
    free(cols);
    free(cells);
    return status;
}

/* --- Scope & Column Binding --- */

typedef struct table_slice {
    const uint8_t *name;
    uint32_t name_len;
    const uint8_t *alias;
    uint32_t alias_len;
    uint32_t col_offset;
    uint32_t col_count;
} table_slice;

typedef struct bind_scope {
    const prdb_sdk_sql_column_shape_v1 *cols;
    uint32_t col_count;
    const table_slice *tables;
    uint32_t table_count;
} bind_scope;

typedef struct agg_collector {
    prdb_sql_expr **aggs;
    uint32_t count;
    uint32_t cap;
} agg_collector;

typedef struct bind_env {
    const bind_scope *scopes; /* scopes[0] = current, 1.. = outer */
    uint32_t scope_count;
    const prdb_sql_select_item *proj_items;
    uint32_t proj_count;
    agg_collector *aggs;
    uint8_t allow_agg;
} bind_env;

static int32_t infer_expr_type(prdb_sql_expr *e, const bind_env *env,
                               uint32_t *type_out, uint8_t *prec_out,
                               uint8_t *scale_out);

static int32_t bind_expr(prdb_sql_expr *e, bind_env *env) {
    int32_t st;
    if (e == NULL) return PRDB_SDK_SQL_BIND_OK;
    switch (e->kind) {
        case PRDB_SQL_EXPR_NULL:
        case PRDB_SQL_EXPR_BOOL:
        case PRDB_SQL_EXPR_INT:
        case PRDB_SQL_EXPR_DECIMAL:
        case PRDB_SQL_EXPR_STRING:
        case PRDB_SQL_EXPR_DATE:
        case PRDB_SQL_EXPR_TIMESTAMP:
        case PRDB_SQL_EXPR_INTERVAL:
        case PRDB_SQL_EXPR_PARAMETER:
            return PRDB_SDK_SQL_BIND_OK;
        case PRDB_SQL_EXPR_COLUMN: {
            int found = 0;
            for (uint32_t s = 0; s < env->scope_count; ++s) {
                const bind_scope *bs = &env->scopes[s];
                uint32_t hits = 0;
                uint32_t hit_ord = 0;
                if (bs->table_count == 0 && e->u.col.table_name == NULL) {
                    for (uint32_t c = 0; c < bs->col_count; ++c) {
                        if (bytes_ieq(e->u.col.col_name, e->u.col.col_name_len,
                                      bs->cols[c].name, bs->cols[c].name_len)) {
                            ++hits;
                            hit_ord = c;
                        }
                    }
                } else {
                    for (uint32_t t = 0; t < bs->table_count; ++t) {
                        const table_slice *ts = &bs->tables[t];
                        if (e->u.col.table_name != NULL) {
                            int match_t =
                                (ts->alias != NULL &&
                                 bytes_ieq(e->u.col.table_name,
                                           e->u.col.table_name_len, ts->alias,
                                           ts->alias_len)) ||
                                (ts->alias == NULL && ts->name != NULL &&
                                 bytes_ieq(e->u.col.table_name,
                                           e->u.col.table_name_len, ts->name,
                                           ts->name_len));
                            if (!match_t) continue;
                        }
                        for (uint32_t c = 0; c < ts->col_count; ++c) {
                            uint32_t ord = ts->col_offset + c;
                            if (bytes_ieq(e->u.col.col_name,
                                          e->u.col.col_name_len,
                                          bs->cols[ord].name,
                                          bs->cols[ord].name_len)) {
                                ++hits;
                                hit_ord = ord;
                            }
                        }
                    }
                }
                if (hits > 1) return PRDB_SDK_SQL_BIND_AMBIGUOUS_COLUMN;
                if (hits == 1) {
                    e->u.col.scope_depth = s;
                    e->u.col.col_ordinal = hit_ord;
                    e->bound_type = bs->cols[hit_ord].type_code;
                    e->bound_precision = bs->cols[hit_ord].decimal_precision;
                    e->bound_scale = bs->cols[hit_ord].decimal_scale;
                    e->is_bound = 1;
                    found = 1;
                    break;
                }
            }
            if (!found) return PRDB_SDK_SQL_BIND_COLUMN_NOT_FOUND;
            return PRDB_SDK_SQL_BIND_OK;
        }
        case PRDB_SQL_EXPR_UNARY:
            st = bind_expr(e->u.unary.operand, env);
            if (st != PRDB_SDK_SQL_BIND_OK) return st;
            return infer_expr_type(e, env, &e->bound_type, &e->bound_precision,
                                   &e->bound_scale);
        case PRDB_SQL_EXPR_BINARY:
            st = bind_expr(e->u.binary.left, env);
            if (st != PRDB_SDK_SQL_BIND_OK) return st;
            st = bind_expr(e->u.binary.right, env);
            if (st != PRDB_SDK_SQL_BIND_OK) return st;
            return infer_expr_type(e, env, &e->bound_type, &e->bound_precision,
                                   &e->bound_scale);
        case PRDB_SQL_EXPR_BETWEEN:
            st = bind_expr(e->u.between.expr, env);
            if (st != PRDB_SDK_SQL_BIND_OK) return st;
            st = bind_expr(e->u.between.low, env);
            if (st != PRDB_SDK_SQL_BIND_OK) return st;
            st = bind_expr(e->u.between.high, env);
            if (st != PRDB_SDK_SQL_BIND_OK) return st;
            e->bound_type = PRDB_SDK_VALUE_TYPE_BOOLEAN;
            e->is_bound = 1;
            return PRDB_SDK_SQL_BIND_OK;
        case PRDB_SQL_EXPR_IN_LIST:
            st = bind_expr(e->u.in_list.expr, env);
            if (st != PRDB_SDK_SQL_BIND_OK) return st;
            for (uint32_t i = 0; i < e->u.in_list.item_count; ++i) {
                st = bind_expr(e->u.in_list.items[i], env);
                if (st != PRDB_SDK_SQL_BIND_OK) return st;
            }
            e->bound_type = PRDB_SDK_VALUE_TYPE_BOOLEAN;
            e->is_bound = 1;
            return PRDB_SDK_SQL_BIND_OK;
        case PRDB_SQL_EXPR_CASE:
            if (e->u.case_expr.operand != NULL) {
                st = bind_expr(e->u.case_expr.operand, env);
                if (st != PRDB_SDK_SQL_BIND_OK) return st;
            }
            for (uint32_t i = 0; i < e->u.case_expr.branch_count; ++i) {
                st = bind_expr(e->u.case_expr.when_exprs[i], env);
                if (st != PRDB_SDK_SQL_BIND_OK) return st;
                st = bind_expr(e->u.case_expr.then_exprs[i], env);
                if (st != PRDB_SDK_SQL_BIND_OK) return st;
            }
            if (e->u.case_expr.else_expr != NULL) {
                st = bind_expr(e->u.case_expr.else_expr, env);
                if (st != PRDB_SDK_SQL_BIND_OK) return st;
            }
            return infer_expr_type(e, env, &e->bound_type, &e->bound_precision,
                                   &e->bound_scale);
        case PRDB_SQL_EXPR_EXTRACT:
            st = bind_expr(e->u.extract.arg, env);
            if (st != PRDB_SDK_SQL_BIND_OK) return st;
            e->bound_type = PRDB_SDK_VALUE_TYPE_INTEGER;
            e->is_bound = 1;
            return PRDB_SDK_SQL_BIND_OK;
        case PRDB_SQL_EXPR_SUBSTRING:
            st = bind_expr(e->u.substring.str, env);
            if (st != PRDB_SDK_SQL_BIND_OK) return st;
            st = bind_expr(e->u.substring.start, env);
            if (st != PRDB_SDK_SQL_BIND_OK) return st;
            if (e->u.substring.len != NULL) {
                st = bind_expr(e->u.substring.len, env);
                if (st != PRDB_SDK_SQL_BIND_OK) return st;
            }
            e->bound_type = PRDB_SDK_VALUE_TYPE_TEXT;
            e->is_bound = 1;
            return PRDB_SDK_SQL_BIND_OK;
        case PRDB_SQL_EXPR_COALESCE:
            for (uint32_t i = 0; i < e->u.coalesce.arg_count; ++i) {
                st = bind_expr(e->u.coalesce.args[i], env);
                if (st != PRDB_SDK_SQL_BIND_OK) return st;
            }
            return infer_expr_type(e, env, &e->bound_type, &e->bound_precision,
                                   &e->bound_scale);
        case PRDB_SQL_EXPR_AGGREGATE: {
            if (!env->allow_agg || env->aggs == NULL)
                return PRDB_SDK_SQL_BIND_INVALID_STATEMENT;
            if (e->u.agg.arg != NULL) {
                uint8_t prev = env->allow_agg;
                env->allow_agg = 0;
                st = bind_expr(e->u.agg.arg, env);
                env->allow_agg = prev;
                if (st != PRDB_SDK_SQL_BIND_OK) return st;
            }
            if (env->aggs->count == env->aggs->cap) {
                uint32_t ncap = env->aggs->cap == 0 ? 8 : env->aggs->cap * 2;
                prdb_sql_expr **na = (prdb_sql_expr **)realloc(
                    env->aggs->aggs, ncap * sizeof(*na));
                if (na == NULL) return PRDB_SDK_SQL_BIND_INVALID_ARGUMENT;
                env->aggs->aggs = na;
                env->aggs->cap = ncap;
            }
            e->u.agg.bound_agg_index = env->aggs->count;
            env->aggs->aggs[env->aggs->count++] = e;
            return infer_expr_type(e, env, &e->bound_type, &e->bound_precision,
                                   &e->bound_scale);
        }
        case PRDB_SQL_EXPR_SUBQUERY:
            if (e->u.subquery.lhs != NULL) {
                st = bind_expr(e->u.subquery.lhs, env);
                if (st != PRDB_SDK_SQL_BIND_OK) return st;
            }
            /* Subquery itself is bound/executed when evaluated or pre-bound */
            e->bound_type =
                e->u.subquery.subq_kind == PRDB_SQL_SUBQ_SCALAR
                    ? PRDB_SDK_VALUE_TYPE_DECIMAL /* refined at runtime */
                    : PRDB_SDK_VALUE_TYPE_BOOLEAN;
            e->is_bound = 1;
            return PRDB_SDK_SQL_BIND_OK;
    }
    return PRDB_SDK_SQL_BIND_OK;
}

static int32_t infer_expr_type(prdb_sql_expr *e, const bind_env *env,
                               uint32_t *type_out, uint8_t *prec_out,
                               uint8_t *scale_out) {
    (void)env;
    *prec_out = 0;
    *scale_out = 0;
    if (e == NULL) { *type_out = 0; return PRDB_SDK_SQL_BIND_OK; }
    switch (e->kind) {
        case PRDB_SQL_EXPR_NULL: *type_out = 0; break;
        case PRDB_SQL_EXPR_BOOL: *type_out = PRDB_SDK_VALUE_TYPE_BOOLEAN; break;
        case PRDB_SQL_EXPR_INT: *type_out = PRDB_SDK_VALUE_TYPE_BIGINT; break;
        case PRDB_SQL_EXPR_DECIMAL:
            *type_out = PRDB_SDK_VALUE_TYPE_DECIMAL;
            *prec_out = e->u.dec_val.precision;
            *scale_out = e->u.dec_val.scale;
            break;
        case PRDB_SQL_EXPR_STRING: *type_out = PRDB_SDK_VALUE_TYPE_TEXT; break;
        case PRDB_SQL_EXPR_DATE: *type_out = PRDB_SDK_VALUE_TYPE_DATE; break;
        case PRDB_SQL_EXPR_TIMESTAMP:
            *type_out = PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ;
            break;
        case PRDB_SQL_EXPR_COLUMN:
            *type_out = e->bound_type;
            *prec_out = e->bound_precision;
            *scale_out = e->bound_scale;
            break;
        case PRDB_SQL_EXPR_UNARY:
            if (e->u.unary.op == PRDB_SQL_UOP_NOT ||
                e->u.unary.op == PRDB_SQL_UOP_IS_NULL ||
                e->u.unary.op == PRDB_SQL_UOP_IS_NOT_NULL) {
                *type_out = PRDB_SDK_VALUE_TYPE_BOOLEAN;
            } else {
                uint32_t t = 0;
                uint8_t p = 0, s = 0;
                infer_expr_type(e->u.unary.operand, env, &t, &p, &s);
                *type_out = t;
                *prec_out = p;
                *scale_out = s;
            }
            break;
        case PRDB_SQL_EXPR_BINARY: {
            prdb_sql_bin_op op = e->u.binary.op;
            if (op == PRDB_SQL_BOP_AND || op == PRDB_SQL_BOP_OR ||
                op == PRDB_SQL_BOP_EQ || op == PRDB_SQL_BOP_NE ||
                op == PRDB_SQL_BOP_LT || op == PRDB_SQL_BOP_LE ||
                op == PRDB_SQL_BOP_GT || op == PRDB_SQL_BOP_GE ||
                op == PRDB_SQL_BOP_LIKE || op == PRDB_SQL_BOP_NOT_LIKE) {
                *type_out = PRDB_SDK_VALUE_TYPE_BOOLEAN;
                break;
            }
            if ((op == PRDB_SQL_BOP_ADD || op == PRDB_SQL_BOP_SUB) &&
                e->u.binary.right->kind == PRDB_SQL_EXPR_INTERVAL) {
                *type_out = PRDB_SDK_VALUE_TYPE_DATE;
                break;
            }
            uint32_t lt = 0, rt = 0;
            uint8_t lp = 0, ls = 0, rp = 0, rs = 0;
            infer_expr_type(e->u.binary.left, env, &lt, &lp, &ls);
            infer_expr_type(e->u.binary.right, env, &rt, &rp, &rs);
            if (lt == PRDB_SDK_VALUE_TYPE_DECIMAL ||
                rt == PRDB_SDK_VALUE_TYPE_DECIMAL) {
                *type_out = PRDB_SDK_VALUE_TYPE_DECIMAL;
                *prec_out = 38;
                if (op == PRDB_SQL_BOP_MUL) {
                    uint8_t sum_s = (uint8_t)(ls + rs);
                    *scale_out = sum_s > 38 ? 38 : sum_s;
                } else if (op == PRDB_SQL_BOP_DIV) {
                    *scale_out = 6;
                } else {
                    *scale_out = ls > rs ? ls : rs;
                }
            } else {
                *type_out = PRDB_SDK_VALUE_TYPE_BIGINT;
            }
            break;
        }
        case PRDB_SQL_EXPR_CASE: {
            for (uint32_t i = 0; i < e->u.case_expr.branch_count; ++i) {
                uint32_t tt = 0;
                uint8_t tp = 0, ts = 0;
                infer_expr_type(e->u.case_expr.then_exprs[i], env, &tt, &tp,
                                &ts);
                if (tt != 0) {
                    *type_out = tt;
                    *prec_out = tp;
                    *scale_out = ts;
                    break;
                }
            }
            if (*type_out == 0 && e->u.case_expr.else_expr != NULL) {
                infer_expr_type(e->u.case_expr.else_expr, env, type_out,
                                prec_out, scale_out);
            }
            break;
        }
        case PRDB_SQL_EXPR_EXTRACT:
            *type_out = PRDB_SDK_VALUE_TYPE_INTEGER;
            break;
        case PRDB_SQL_EXPR_SUBSTRING:
            *type_out = PRDB_SDK_VALUE_TYPE_TEXT;
            break;
        case PRDB_SQL_EXPR_COALESCE:
            for (uint32_t i = 0; i < e->u.coalesce.arg_count; ++i) {
                uint32_t tt = 0;
                uint8_t tp = 0, ts = 0;
                infer_expr_type(e->u.coalesce.args[i], env, &tt, &tp, &ts);
                if (tt != 0) {
                    *type_out = tt;
                    *prec_out = tp;
                    *scale_out = ts;
                    break;
                }
            }
            break;
        case PRDB_SQL_EXPR_AGGREGATE: {
            uint32_t ak = e->u.agg.agg_kind;
            if (ak == PRDB_SDK_SQL_AGGREGATE_COUNT_STAR ||
                ak == PRDB_SDK_SQL_AGGREGATE_COUNT_COLUMN) {
                *type_out = PRDB_SDK_VALUE_TYPE_BIGINT;
            } else if (ak == PRDB_SDK_SQL_AGGREGATE_AVG) {
                uint32_t at = 0;
                uint8_t ap = 0, as = 0;
                infer_expr_type(e->u.agg.arg, env, &at, &ap, &as);
                *type_out = PRDB_SDK_VALUE_TYPE_DECIMAL;
                *prec_out = 38;
                if (at == PRDB_SDK_VALUE_TYPE_DECIMAL) {
                    uint8_t r = as + 6;
                    if (r < 6) r = 6;
                    if (ap >= as && r > (uint8_t)(38 - (ap - as)))
                        r = (uint8_t)(38 - (ap - as));
                    *scale_out = r;
                } else {
                    *scale_out = 6;
                }
            } else if (ak == PRDB_SDK_SQL_AGGREGATE_SUM) {
                uint32_t at = 0;
                uint8_t ap = 0, as = 0;
                infer_expr_type(e->u.agg.arg, env, &at, &ap, &as);
                if (at == PRDB_SDK_VALUE_TYPE_DECIMAL) {
                    *type_out = PRDB_SDK_VALUE_TYPE_DECIMAL;
                    *prec_out = 38;
                    *scale_out = as;
                } else {
                    *type_out = PRDB_SDK_VALUE_TYPE_BIGINT;
                }
            } else {
                infer_expr_type(e->u.agg.arg, env, type_out, prec_out,
                                scale_out);
            }
            break;
        }
        default:
            *type_out = e->bound_type;
            *prec_out = e->bound_precision;
            *scale_out = e->bound_scale;
            break;
    }
    e->bound_type = *type_out;
    e->bound_precision = *prec_out;
    e->bound_scale = *scale_out;
    e->is_bound = 1;
    return PRDB_SDK_SQL_BIND_OK;
}

/* --- Hash & Equality for Join / Group / Distinct --- */

static uint64_t hash_value(const prdb_sdk_value_v1 *v, uint8_t dec_scale) {
    (void)dec_scale;
    uint64_t h = 14695981039346656037ULL;
    if (v->state == PRDB_SDK_VALUE_STATE_NULL) return h ^ 0x9e3779b97f4a7c15ULL;
    uint32_t tc = v->type_code;
    if (tc == PRDB_SDK_VALUE_TYPE_SMALLINT ||
        tc == PRDB_SDK_VALUE_TYPE_INTEGER || tc == PRDB_SDK_VALUE_TYPE_BIGINT) {
        int64_t iv = 0;
        if (tc == PRDB_SDK_VALUE_TYPE_SMALLINT) {
            int16_t s16 = 0;
            prdb_sdk_value_get_smallint_v1(v, &s16);
            iv = s16;
        } else if (tc == PRDB_SDK_VALUE_TYPE_INTEGER) {
            int32_t s32 = 0;
            prdb_sdk_value_get_integer_v1(v, &s32);
            iv = s32;
        } else {
            prdb_sdk_value_get_bigint_v1(v, &iv);
        }
        uint64_t u = (uint64_t)iv;
        for (int i = 0; i < 8; ++i) {
            h ^= (uint8_t)(u >> (i * 8));
            h *= 1099511628211ULL;
        }
        return h;
    }
    for (uint64_t i = 0; i < v->data_len; ++i) {
        h ^= v->data[i];
        h *= 1099511628211ULL;
    }
    return h;
}

static int values_equal_for_key(const prdb_sdk_value_v1 *a,
                                const prdb_sdk_value_v1 *b, uint8_t scale,
                                int null_eq) {
    if (a->state == PRDB_SDK_VALUE_STATE_NULL ||
        b->state == PRDB_SDK_VALUE_STATE_NULL) {
        return null_eq && a->state == b->state;
    }
    if ((a->type_code == PRDB_SDK_VALUE_TYPE_SMALLINT ||
         a->type_code == PRDB_SDK_VALUE_TYPE_INTEGER ||
         a->type_code == PRDB_SDK_VALUE_TYPE_BIGINT) &&
        (b->type_code == PRDB_SDK_VALUE_TYPE_SMALLINT ||
         b->type_code == PRDB_SDK_VALUE_TYPE_INTEGER ||
         b->type_code == PRDB_SDK_VALUE_TYPE_BIGINT)) {
        int64_t ia = 0, ib = 0;
        if (a->type_code == PRDB_SDK_VALUE_TYPE_SMALLINT) {
            int16_t x = 0; prdb_sdk_value_get_smallint_v1(a, &x); ia = x;
        } else if (a->type_code == PRDB_SDK_VALUE_TYPE_INTEGER) {
            int32_t x = 0; prdb_sdk_value_get_integer_v1(a, &x); ia = x;
        } else {
            prdb_sdk_value_get_bigint_v1(a, &ia);
        }
        if (b->type_code == PRDB_SDK_VALUE_TYPE_SMALLINT) {
            int16_t x = 0; prdb_sdk_value_get_smallint_v1(b, &x); ib = x;
        } else if (b->type_code == PRDB_SDK_VALUE_TYPE_INTEGER) {
            int32_t x = 0; prdb_sdk_value_get_integer_v1(b, &x); ib = x;
        } else {
            prdb_sdk_value_get_bigint_v1(b, &ib);
        }
        return ia == ib;
    }
    prdb_sdk_value_v1 va = *a, vb = *b;
    if ((va.type_code == PRDB_SDK_VALUE_TYPE_TEXT ||
         va.type_code == PRDB_SDK_VALUE_TYPE_VARCHAR) &&
        (vb.type_code == PRDB_SDK_VALUE_TYPE_TEXT ||
         vb.type_code == PRDB_SDK_VALUE_TYPE_VARCHAR)) {
        va.type_code = PRDB_SDK_VALUE_TYPE_TEXT;
        vb.type_code = PRDB_SDK_VALUE_TYPE_TEXT;
    }
    uint32_t truth = 0;
    if (prdb_sdk_sql_compare_v1(&va, &vb, PRDB_SDK_SQL_COMPARE_EQUAL, scale,
                                scale, &truth) != PRDB_SDK_SQL_EVAL_OK) {
        return 0;
    }
    return truth == PRDB_SDK_SQL_TRUTH_TRUE;
}

/* --- Join Execution --- */

static void collect_conjuncts(prdb_sql_expr *e, prdb_sql_expr ***out,
                              uint32_t *count, uint32_t *cap) {
    if (e == NULL) return;
    if (e->kind == PRDB_SQL_EXPR_BINARY && e->u.binary.op == PRDB_SQL_BOP_AND) {
        collect_conjuncts(e->u.binary.left, out, count, cap);
        collect_conjuncts(e->u.binary.right, out, count, cap);
        return;
    }
    if (*count == *cap) {
        uint32_t ncap = *cap == 0 ? 8 : *cap * 2;
        prdb_sql_expr **na =
            (prdb_sql_expr **)realloc(*out, ncap * sizeof(*na));
        if (na == NULL) return;
        *out = na;
        *cap = ncap;
    }
    (*out)[(*count)++] = e;
}

static int expr_side_mask(const prdb_sql_expr *e, uint32_t left_cols) {
    if (e == NULL) return 0;
    switch (e->kind) {
        case PRDB_SQL_EXPR_COLUMN:
            if (e->u.col.scope_depth > 0) return 0;
            return e->u.col.col_ordinal < left_cols ? 1 : 2;
        case PRDB_SQL_EXPR_UNARY:
            return expr_side_mask(e->u.unary.operand, left_cols);
        case PRDB_SQL_EXPR_BINARY:
            return expr_side_mask(e->u.binary.left, left_cols) |
                   expr_side_mask(e->u.binary.right, left_cols);
        case PRDB_SQL_EXPR_BETWEEN:
            return expr_side_mask(e->u.between.expr, left_cols) |
                   expr_side_mask(e->u.between.low, left_cols) |
                   expr_side_mask(e->u.between.high, left_cols);
        case PRDB_SQL_EXPR_CASE: {
            int m = expr_side_mask(e->u.case_expr.operand, left_cols);
            for (uint32_t i = 0; i < e->u.case_expr.branch_count; ++i) {
                m |= expr_side_mask(e->u.case_expr.when_exprs[i], left_cols);
                m |= expr_side_mask(e->u.case_expr.then_exprs[i], left_cols);
            }
            return m | expr_side_mask(e->u.case_expr.else_expr, left_cols);
        }
        default:
            return 3;
    }
}

static int32_t eval_subquery_cb(void *vctx, const prdb_sql_expr *subq_expr,
                                const prdb_sql_eval_row_set *outer_ctx,
                                prdb_sql_owned_val *out) {
    subq_runtime *rt = (subq_runtime *)vctx;
    const prdb_sql_select_ast *sub = subq_expr->u.subquery.subquery;
    mat_table *res = NULL;
    mat_table local_res;
    int cached = 0;

    if (!select_has_outer_ref(sub)) {
        for (uint32_t i = 0; i < rt->cache_count; ++i) {
            if (rt->cache[i].ast == sub) {
                res = &rt->cache[i].result;
                cached = 1;
                break;
            }
        }
    }
    if (!cached) {
        mat_table_init(&local_res, 16 * 1024 * 1024);
        if (exec_select_to_table(rt->ec, (prdb_sql_select_ast *)sub,
                                 outer_ctx->scopes, outer_ctx->scope_count,
                                 &local_res) != PRDB_SDK_SQL_DISPATCH_OK) {
            mat_table_free(&local_res);
            return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
        }
        if (!select_has_outer_ref(sub)) {
            if (rt->cache_count == rt->cache_cap) {
                uint32_t ncap = rt->cache_cap == 0 ? 8 : rt->cache_cap * 2;
                subq_cache_entry *nc = (subq_cache_entry *)realloc(
                    rt->cache, ncap * sizeof(*nc));
                if (nc == NULL) {
                    mat_table_free(&local_res);
                    return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
                }
                rt->cache = nc;
                rt->cache_cap = ncap;
            }
            subq_cache_entry *ce = &rt->cache[rt->cache_count++];
            ce->ast = sub;
            ce->result = local_res;
            res = &ce->result;
            cached = 1;
        } else {
            res = &local_res;
        }
    }

    int32_t status = PRDB_SDK_SQL_EVAL_OK;
    prdb_sql_subquery_kind sk = subq_expr->u.subquery.subq_kind;
    if (sk == PRDB_SQL_SUBQ_EXISTS || sk == PRDB_SQL_SUBQ_NOT_EXISTS) {
        uint8_t b = res->row_count > 0 ? 1 : 0;
        if (sk == PRDB_SQL_SUBQ_NOT_EXISTS) b = !b;
        prdb_sql_owned_val_free(out);
        out->inline_buf[0] = b;
        out->val = (prdb_sdk_value_v1){
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BOOLEAN,
            out->inline_buf, 1};
    } else if (sk == PRDB_SQL_SUBQ_SCALAR) {
        if (res->row_count == 0) {
            prdb_sql_owned_val_free(out);
            out->val = (prdb_sdk_value_v1){
                sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
                PRDB_SDK_VALUE_STATE_NULL, 0, NULL, 0};
        } else if (res->row_count > 1 || res->col_count == 0) {
            status = PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
        } else {
            const prdb_sdk_value_v1 *v = &res->rows[0].cells[0];
            ((prdb_sql_expr *)subq_expr)->bound_type = res->cols[0].type_code;
            ((prdb_sql_expr *)subq_expr)->bound_scale = res->cols[0].decimal_scale;
            if (v->state == PRDB_SDK_VALUE_STATE_NULL) {
                prdb_sql_owned_val_free(out);
                out->val = *v;
            } else {
                prdb_sql_owned_val_free(out);
                if (v->data_len <= sizeof(out->inline_buf)) {
                    memcpy(out->inline_buf, v->data, (size_t)v->data_len);
                    out->val = *v;
                    out->val.data = out->inline_buf;
                } else {
                    out->heap_buf = (uint8_t *)malloc((size_t)v->data_len);
                    if (out->heap_buf == NULL) {
                        status = PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
                    } else {
                        memcpy(out->heap_buf, v->data, (size_t)v->data_len);
                        out->val = *v;
                        out->val.data = out->heap_buf;
                    }
                }
            }
        }
    } else {
        /* IN / NOT IN */
        prdb_sql_owned_val lv = {0};
        status = prdb_sql_eval_expr_v1(subq_expr->u.subquery.lhs, outer_ctx, &lv);
        if (status == PRDB_SDK_SQL_EVAL_OK) {
            if (lv.val.state == PRDB_SDK_VALUE_STATE_NULL) {
                prdb_sql_owned_val_free(out);
                out->val = (prdb_sdk_value_v1){
                    sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
                    PRDB_SDK_VALUE_STATE_NULL, 0, NULL, 0};
            } else {
                uint8_t matched = 0, saw_null = 0;
                uint8_t sc = res->col_count > 0 ? res->cols[0].decimal_scale : 0;
                for (uint32_t r = 0; r < res->row_count; ++r) {
                    const prdb_sdk_value_v1 *rv = &res->rows[r].cells[0];
                    if (rv->state == PRDB_SDK_VALUE_STATE_NULL) {
                        saw_null = 1;
                    } else if (values_equal_for_key(&lv.val, rv, sc, 0)) {
                        matched = 1;
                        break;
                    }
                }
                prdb_sql_owned_val_free(out);
                if (!matched && saw_null) {
                    out->val = (prdb_sdk_value_v1){
                        sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
                        PRDB_SDK_VALUE_STATE_NULL, 0, NULL, 0};
                } else {
                    uint8_t ans = sk == PRDB_SQL_SUBQ_NOT_IN ? !matched : matched;
                    out->inline_buf[0] = ans;
                    out->val = (prdb_sdk_value_v1){
                        sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
                        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BOOLEAN,
                        out->inline_buf, 1};
                }
            }
        }
        prdb_sql_owned_val_free(&lv);
    }

    if (!cached) mat_table_free(&local_res);
    return status;
}

static int32_t materialize_from_ref(exec_ctx *ec, const prdb_sql_table_ref *ref,
                                    const prdb_sql_eval_scope *outer_scopes,
                                    uint32_t outer_scope_count,
                                    mat_table *out_table,
                                    table_slice **slices_out,
                                    uint32_t *slice_count_out) {
    if (ref->kind == PRDB_SQL_TREF_BASE) {
        /* Check CTEs first (only if no explicit schema) */
        if (ref->u.base.schema == NULL) {
            for (uint32_t i = 0; i < ec->cte_count; ++i) {
                if (bytes_ieq(ref->u.base.name, ref->u.base.name_len,
                              ec->ctes[i].name, ec->ctes[i].name_len)) {
                    const mat_table *ct = &ec->ctes[i].data;
                    out_table->col_count = ct->col_count;
                    out_table->cols = (prdb_sdk_sql_column_shape_v1 *)calloc(
                        ct->col_count == 0 ? 1 : ct->col_count,
                        sizeof(prdb_sdk_sql_column_shape_v1));
                    if (out_table->cols == NULL)
                        return PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
                    for (uint32_t c = 0; c < ct->col_count; ++c) {
                        out_table->cols[c] = ct->cols[c];
                        out_table->cols[c].name =
                            (const uint8_t *)prdb_sql_arena_strdup(
                                &out_table->arena, ct->cols[c].name,
                                ct->cols[c].name_len);
                    }
                    for (uint32_t r = 0; r < ct->row_count; ++r) {
                        if (!mat_table_append_row(out_table, ct->rows[r].cells,
                                                  NULL))
                            return PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
                    }
                    table_slice *sl = (table_slice *)calloc(1, sizeof(*sl));
                    if (sl == NULL) return PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
                    sl->name = ref->u.base.name;
                    sl->name_len = ref->u.base.name_len;
                    sl->alias = ref->u.base.alias;
                    sl->alias_len = ref->u.base.alias_len;
                    sl->col_offset = 0;
                    sl->col_count = out_table->col_count;
                    *slices_out = sl;
                    *slice_count_out = 1;
                    return PRDB_SDK_SQL_DISPATCH_OK;
                }
            }
        }
        const uint8_t *sc =
            ref->u.base.schema != NULL ? ref->u.base.schema : ec->schema;
        uint32_t sc_len =
            ref->u.base.schema != NULL ? ref->u.base.schema_len : ec->schema_len;
        int32_t st = scan_base_relation(ec, sc, sc_len, ref->u.base.name,
                                        ref->u.base.name_len, 0, out_table);
        if (st != PRDB_SDK_SQL_DISPATCH_OK) return st;
        table_slice *sl = (table_slice *)calloc(1, sizeof(*sl));
        if (sl == NULL) return PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
        sl->name = ref->u.base.name;
        sl->name_len = ref->u.base.name_len;
        sl->alias = ref->u.base.alias;
        sl->alias_len = ref->u.base.alias_len;
        sl->col_offset = 0;
        sl->col_count = out_table->col_count;
        *slices_out = sl;
        *slice_count_out = 1;
        return PRDB_SDK_SQL_DISPATCH_OK;
    }

    if (ref->kind == PRDB_SQL_TREF_SUBQUERY) {
        int32_t st = exec_select_to_table(ec, ref->u.subq.subquery,
                                          outer_scopes, outer_scope_count,
                                          out_table);
        if (st != PRDB_SDK_SQL_DISPATCH_OK) return st;
        for (uint32_t c = 0;
             c < ref->u.subq.col_alias_count && c < out_table->col_count; ++c) {
            out_table->cols[c].name = (const uint8_t *)prdb_sql_arena_strdup(
                &out_table->arena, ref->u.subq.col_aliases[c],
                ref->u.subq.col_alias_lens[c]);
            out_table->cols[c].name_len = ref->u.subq.col_alias_lens[c];
        }
        table_slice *sl = (table_slice *)calloc(1, sizeof(*sl));
        if (sl == NULL) return PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
        sl->alias = ref->u.subq.alias;
        sl->alias_len = ref->u.subq.alias_len;
        sl->col_offset = 0;
        sl->col_count = out_table->col_count;
        *slices_out = sl;
        *slice_count_out = 1;
        return PRDB_SDK_SQL_DISPATCH_OK;
    }

    /* Join node */
    mat_table left_t, right_t;
    mat_table_init(&left_t, 64 * 1024 * 1024);
    mat_table_init(&right_t, 64 * 1024 * 1024);
    table_slice *l_slices = NULL, *r_slices = NULL;
    uint32_t l_scnt = 0, r_scnt = 0;
    int32_t st = materialize_from_ref(ec, ref->u.join.left, outer_scopes,
                                      outer_scope_count, &left_t, &l_slices,
                                      &l_scnt);
    if (st != PRDB_SDK_SQL_DISPATCH_OK) {
        mat_table_free(&left_t);
        mat_table_free(&right_t);
        free(l_slices);
        return st;
    }
    st = materialize_from_ref(ec, ref->u.join.right, outer_scopes,
                              outer_scope_count, &right_t, &r_slices, &r_scnt);
    if (st != PRDB_SDK_SQL_DISPATCH_OK) {
        mat_table_free(&left_t);
        mat_table_free(&right_t);
        free(l_slices);
        free(r_slices);
        return st;
    }

    uint32_t total_cols = left_t.col_count + right_t.col_count;
    out_table->col_count = total_cols;
    out_table->cols = (prdb_sdk_sql_column_shape_v1 *)calloc(
        total_cols == 0 ? 1 : total_cols, sizeof(prdb_sdk_sql_column_shape_v1));
    table_slice *all_slices =
        (table_slice *)calloc(l_scnt + r_scnt, sizeof(table_slice));
    if (out_table->cols == NULL || all_slices == NULL) {
        mat_table_free(&left_t);
        mat_table_free(&right_t);
        free(l_slices);
        free(r_slices);
        free(all_slices);
        return PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
    }
    for (uint32_t c = 0; c < left_t.col_count; ++c) {
        out_table->cols[c] = left_t.cols[c];
        out_table->cols[c].name = (const uint8_t *)prdb_sql_arena_strdup(
            &out_table->arena, left_t.cols[c].name, left_t.cols[c].name_len);
    }
    for (uint32_t c = 0; c < right_t.col_count; ++c) {
        out_table->cols[left_t.col_count + c] = right_t.cols[c];
        out_table->cols[left_t.col_count + c].name =
            (const uint8_t *)prdb_sql_arena_strdup(
                &out_table->arena, right_t.cols[c].name,
                right_t.cols[c].name_len);
    }
    for (uint32_t i = 0; i < l_scnt; ++i) all_slices[i] = l_slices[i];
    for (uint32_t i = 0; i < r_scnt; ++i) {
        all_slices[l_scnt + i] = r_slices[i];
        all_slices[l_scnt + i].col_offset += left_t.col_count;
    }
    free(l_slices);
    free(r_slices);
    *slices_out = all_slices;
    *slice_count_out = l_scnt + r_scnt;

    /* Bind ON condition against combined scope */
    bind_scope *scopes =
        (bind_scope *)calloc(1 + outer_scope_count, sizeof(bind_scope));
    if (scopes == NULL) {
        mat_table_free(&left_t);
        mat_table_free(&right_t);
        return PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
    }
    scopes[0] = (bind_scope){out_table->cols, total_cols, all_slices,
                             l_scnt + r_scnt};
    for (uint32_t i = 0; i < outer_scope_count; ++i) {
        scopes[1 + i] =
            (bind_scope){outer_scopes[i].columns, outer_scopes[i].column_count,
                         NULL, 0};
    }
    bind_env benv = {scopes, 1 + outer_scope_count, NULL, 0, NULL, 0};
    if (ref->u.join.on_cond != NULL) {
        if (bind_expr(ref->u.join.on_cond, &benv) != PRDB_SDK_SQL_BIND_OK) {
            free(scopes);
            mat_table_free(&left_t);
            mat_table_free(&right_t);
            return PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
        }
    }
    free(scopes);

    /* Extract equijoin keys from ON condition */
    prdb_sql_expr **conjs = NULL;
    uint32_t conj_cnt = 0, conj_cap = 0;
    collect_conjuncts(ref->u.join.on_cond, &conjs, &conj_cnt, &conj_cap);
    prdb_sql_expr *eq_left = NULL, *eq_right = NULL;
    for (uint32_t i = 0; i < conj_cnt; ++i) {
        prdb_sql_expr *c = conjs[i];
        if (c->kind == PRDB_SQL_EXPR_BINARY &&
            c->u.binary.op == PRDB_SQL_BOP_EQ) {
            int lm = expr_side_mask(c->u.binary.left, left_t.col_count);
            int rm = expr_side_mask(c->u.binary.right, left_t.col_count);
            if (lm == 1 && rm == 2) {
                eq_left = c->u.binary.left;
                eq_right = c->u.binary.right;
                break;
            } else if (lm == 2 && rm == 1) {
                eq_left = c->u.binary.right;
                eq_right = c->u.binary.left;
                break;
            }
        }
    }
    free(conjs);

    prdb_sdk_value_v1 *combined = (prdb_sdk_value_v1 *)calloc(
        total_cols == 0 ? 1 : total_cols, sizeof(prdb_sdk_value_v1));
    prdb_sql_eval_scope *eval_scopes = (prdb_sql_eval_scope *)calloc(
        1 + outer_scope_count, sizeof(prdb_sql_eval_scope));
    if (combined == NULL || eval_scopes == NULL) {
        free(combined);
        free(eval_scopes);
        mat_table_free(&left_t);
        mat_table_free(&right_t);
        return PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
    }
    eval_scopes[0] = (prdb_sql_eval_scope){out_table->cols, combined, total_cols};
    for (uint32_t i = 0; i < outer_scope_count; ++i)
        eval_scopes[1 + i] = outer_scopes[i];

    subq_runtime sub_rt = {ec, NULL, 0, 0};
    prdb_sql_eval_row_set rctx = {
        eval_scopes, 1 + outer_scope_count, ec->params, ec->param_count,
        ec->param_meta, NULL, 0, eval_subquery_cb, &sub_rt};

    if (eq_left != NULL && eq_right != NULL) {
        /* Build hash table on right_t */
        uint32_t nbuckets = 16;
        while (nbuckets < right_t.row_count * 2) nbuckets *= 2;
        uint32_t *buckets = (uint32_t *)malloc(nbuckets * sizeof(uint32_t));
        uint32_t *next_idx = (uint32_t *)malloc(
            (right_t.row_count == 0 ? 1 : right_t.row_count) * sizeof(uint32_t));
        prdb_sql_owned_val *rkeys = (prdb_sql_owned_val *)calloc(
            right_t.row_count == 0 ? 1 : right_t.row_count,
            sizeof(prdb_sql_owned_val));
        if (buckets == NULL || next_idx == NULL || rkeys == NULL) {
            free(buckets); free(next_idx); free(rkeys);
            free(combined); free(eval_scopes);
            mat_table_free(&left_t); mat_table_free(&right_t);
            return PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
        }
        memset(buckets, 0xff, nbuckets * sizeof(uint32_t));
        for (uint32_t r = 0; r < right_t.row_count; ++r) {
            if (right_t.col_count > 0) {
                memcpy(combined + left_t.col_count, right_t.rows[r].cells,
                       right_t.col_count * sizeof(prdb_sdk_value_v1));
            }
            if (prdb_sql_eval_expr_v1(eq_right, &rctx, &rkeys[r]) ==
                    PRDB_SDK_SQL_EVAL_OK &&
                rkeys[r].val.state == PRDB_SDK_VALUE_STATE_VALUE) {
                uint64_t h = hash_value(&rkeys[r].val, eq_right->bound_scale);
                uint32_t b = (uint32_t)(h & (nbuckets - 1));
                next_idx[r] = buckets[b];
                buckets[b] = r;
            } else {
                next_idx[r] = UINT32_MAX;
            }
        }
        uint8_t sc = eq_left->bound_scale > eq_right->bound_scale
                         ? eq_left->bound_scale
                         : eq_right->bound_scale;
        for (uint32_t l = 0; l < left_t.row_count; ++l) {
            if (left_t.col_count > 0) {
                memcpy(combined, left_t.rows[l].cells,
                       left_t.col_count * sizeof(prdb_sdk_value_v1));
            }
            prdb_sql_owned_val lk = {0};
            uint8_t matched_any = 0;
            if (prdb_sql_eval_expr_v1(eq_left, &rctx, &lk) ==
                    PRDB_SDK_SQL_EVAL_OK &&
                lk.val.state == PRDB_SDK_VALUE_STATE_VALUE) {
                uint64_t h = hash_value(&lk.val, eq_left->bound_scale);
                uint32_t b = (uint32_t)(h & (nbuckets - 1));
                for (uint32_t r = buckets[b]; r != UINT32_MAX; r = next_idx[r]) {
                    if (!values_equal_for_key(&lk.val, &rkeys[r].val, sc, 0))
                        continue;
                    if (right_t.col_count > 0) {
                        memcpy(combined + left_t.col_count,
                               right_t.rows[r].cells,
                               right_t.col_count * sizeof(prdb_sdk_value_v1));
                    }
                    uint32_t truth = PRDB_SDK_SQL_TRUTH_TRUE;
                    if (ref->u.join.on_cond != NULL) {
                        if (prdb_sql_eval_truth_v1(ref->u.join.on_cond, &rctx,
                                                   &truth) !=
                            PRDB_SDK_SQL_EVAL_OK) {
                            prdb_sql_owned_val_free(&lk);
                            for (uint32_t k = 0; k < right_t.row_count; ++k)
                                prdb_sql_owned_val_free(&rkeys[k]);
                            free(buckets); free(next_idx); free(rkeys);
                            free(combined); free(eval_scopes);
                            mat_table_free(&left_t); mat_table_free(&right_t);
                            return PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
                        }
                    }
                    if (truth == PRDB_SDK_SQL_TRUTH_TRUE) {
                        matched_any = 1;
                        if (!mat_table_append_row(out_table, combined, NULL)) {
                            prdb_sql_owned_val_free(&lk);
                            for (uint32_t k = 0; k < right_t.row_count; ++k)
                                prdb_sql_owned_val_free(&rkeys[k]);
                            free(buckets); free(next_idx); free(rkeys);
                            free(combined); free(eval_scopes);
                            mat_table_free(&left_t); mat_table_free(&right_t);
                            return PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
                        }
                    }
                }
            }
            prdb_sql_owned_val_free(&lk);
            if (!matched_any &&
                ref->u.join.join_kind == PRDB_SQL_JOIN_LEFT_OUTER) {
                for (uint32_t c = 0; c < right_t.col_count; ++c) {
                    combined[left_t.col_count + c] = (prdb_sdk_value_v1){
                        sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
                        PRDB_SDK_VALUE_STATE_NULL, 0, NULL, 0};
                }
                if (!mat_table_append_row(out_table, combined, NULL)) {
                    for (uint32_t k = 0; k < right_t.row_count; ++k)
                        prdb_sql_owned_val_free(&rkeys[k]);
                    free(buckets); free(next_idx); free(rkeys);
                    free(combined); free(eval_scopes);
                    mat_table_free(&left_t); mat_table_free(&right_t);
                    return PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
                }
            }
        }
        for (uint32_t k = 0; k < right_t.row_count; ++k)
            prdb_sql_owned_val_free(&rkeys[k]);
        free(buckets);
        free(next_idx);
        free(rkeys);
    } else {
        /* Nested loop join */
        for (uint32_t l = 0; l < left_t.row_count; ++l) {
            if (left_t.col_count > 0) {
                memcpy(combined, left_t.rows[l].cells,
                       left_t.col_count * sizeof(prdb_sdk_value_v1));
            }
            uint8_t matched_any = 0;
            for (uint32_t r = 0; r < right_t.row_count; ++r) {
                if (right_t.col_count > 0) {
                    memcpy(combined + left_t.col_count, right_t.rows[r].cells,
                           right_t.col_count * sizeof(prdb_sdk_value_v1));
                }
                uint32_t truth = PRDB_SDK_SQL_TRUTH_TRUE;
                if (ref->u.join.on_cond != NULL) {
                    if (prdb_sql_eval_truth_v1(ref->u.join.on_cond, &rctx,
                                               &truth) != PRDB_SDK_SQL_EVAL_OK) {
                        free(combined); free(eval_scopes);
                        mat_table_free(&left_t); mat_table_free(&right_t);
                        return PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
                    }
                }
                if (truth == PRDB_SDK_SQL_TRUTH_TRUE) {
                    matched_any = 1;
                    if (!mat_table_append_row(out_table, combined, NULL)) {
                        free(combined); free(eval_scopes);
                        mat_table_free(&left_t); mat_table_free(&right_t);
                        return PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
                    }
                }
            }
            if (!matched_any &&
                ref->u.join.join_kind == PRDB_SQL_JOIN_LEFT_OUTER) {
                for (uint32_t c = 0; c < right_t.col_count; ++c) {
                    combined[left_t.col_count + c] = (prdb_sdk_value_v1){
                        sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
                        PRDB_SDK_VALUE_STATE_NULL, 0, NULL, 0};
                }
                if (!mat_table_append_row(out_table, combined, NULL)) {
                    free(combined); free(eval_scopes);
                    mat_table_free(&left_t); mat_table_free(&right_t);
                    return PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
                }
            }
        }
    }

    for (uint32_t i = 0; i < sub_rt.cache_count; ++i)
        mat_table_free(&sub_rt.cache[i].result);
    free(sub_rt.cache);
    free(combined);
    free(eval_scopes);
    mat_table_free(&left_t);
    mat_table_free(&right_t);
    return PRDB_SDK_SQL_DISPATCH_OK;
}

/* --- Sort Comparator --- */

typedef struct sort_row_item {
    uint32_t row_idx;
    prdb_sql_owned_val *keys;
} sort_row_item;

static const prdb_sql_order_spec *g_sort_specs = NULL;
static uint32_t g_sort_spec_count = 0;

static int cmp_sort_items(const void *va, const void *vb) {
    const sort_row_item *a = (const sort_row_item *)va;
    const sort_row_item *b = (const sort_row_item *)vb;
    for (uint32_t i = 0; i < g_sort_spec_count; ++i) {
        const prdb_sdk_value_v1 *ka = &a->keys[i].val;
        const prdb_sdk_value_v1 *kb = &b->keys[i].val;
        int a_null = ka->state == PRDB_SDK_VALUE_STATE_NULL;
        int b_null = kb->state == PRDB_SDK_VALUE_STATE_NULL;
        if (a_null && b_null) continue;
        if (a_null || b_null) {
            int nulls_first = g_sort_specs[i].nulls_first
                                  ? 1
                                  : (g_sort_specs[i].nulls_last
                                         ? 0
                                         : (g_sort_specs[i].descending ? 1 : 0));
            return (a_null ? -1 : 1) * (nulls_first ? 1 : -1);
        }
        uint8_t sc = g_sort_specs[i].expr->bound_scale;
        int cmp = 0;
        if (values_equal_for_key(ka, kb, sc, 0)) continue;
        prdb_sdk_value_v1 l = *ka, r = *kb;
        if ((l.type_code == PRDB_SDK_VALUE_TYPE_TEXT ||
             l.type_code == PRDB_SDK_VALUE_TYPE_VARCHAR) &&
            (r.type_code == PRDB_SDK_VALUE_TYPE_TEXT ||
             r.type_code == PRDB_SDK_VALUE_TYPE_VARCHAR)) {
            l.type_code = PRDB_SDK_VALUE_TYPE_TEXT;
            r.type_code = PRDB_SDK_VALUE_TYPE_TEXT;
        }
        uint32_t lt = 0;
        if (prdb_sdk_sql_compare_v1(&l, &r, PRDB_SDK_SQL_COMPARE_LESS, sc, sc,
                                    &lt) == PRDB_SDK_SQL_EVAL_OK) {
            cmp = (lt == PRDB_SDK_SQL_TRUTH_TRUE) ? -1 : 1;
        }
        if (cmp != 0) {
            return g_sort_specs[i].descending ? -cmp : cmp;
        }
    }
    return a->row_idx < b->row_idx ? -1 : (a->row_idx > b->row_idx ? 1 : 0);
}

/* --- SELECT execution --- */

static int32_t exec_select_to_table(exec_ctx *ec, prdb_sql_select_ast *sel,
                                    const prdb_sql_eval_scope *outer_scopes,
                                    uint32_t outer_scope_count,
                                    mat_table *out_table) {
    uint32_t saved_cte_count = ec->cte_count;
    int32_t st = PRDB_SDK_SQL_DISPATCH_OK;

    if (sel->cte_count > 0) {
        cte_entry *nc = (cte_entry *)realloc(
            ec->ctes, (ec->cte_count + sel->cte_count) * sizeof(cte_entry));
        if (nc == NULL) return PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
        ec->ctes = nc;
        for (uint32_t i = 0; i < sel->cte_count; ++i) {
            cte_entry *ce = &ec->ctes[ec->cte_count];
            ce->name = sel->ctes[i].name;
            ce->name_len = sel->ctes[i].name_len;
            mat_table_init(&ce->data, 64 * 1024 * 1024);
            st = exec_select_to_table(ec, sel->ctes[i].query, outer_scopes,
                                      outer_scope_count, &ce->data);
            if (st != PRDB_SDK_SQL_DISPATCH_OK) goto cleanup_ctes;
            for (uint32_t c = 0;
                 c < sel->ctes[i].col_name_count && c < ce->data.col_count;
                 ++c) {
                ce->data.cols[c].name = (const uint8_t *)prdb_sql_arena_strdup(
                    &ce->data.arena, sel->ctes[i].col_names[c],
                    sel->ctes[i].col_name_lens[c]);
                ce->data.cols[c].name_len = sel->ctes[i].col_name_lens[c];
            }
            ++ec->cte_count;
        }
    }

    mat_table from_t;
    mat_table_init(&from_t, 64 * 1024 * 1024);
    table_slice *slices = NULL;
    uint32_t slice_cnt = 0;

    if (sel->from != NULL) {
        st = materialize_from_ref(ec, sel->from, outer_scopes,
                                  outer_scope_count, &from_t, &slices,
                                  &slice_cnt);
        if (st != PRDB_SDK_SQL_DISPATCH_OK) {
            mat_table_free(&from_t);
            free(slices);
            goto cleanup_ctes;
        }
    } else {
        /* SELECT without FROM: 1 empty row */
        uint8_t dummy = 0;
        mat_table_append_row(&from_t, (const prdb_sdk_value_v1 *)&dummy, NULL);
    }

    /* Expand * items */
    uint32_t exp_cap = sel->item_count + from_t.col_count;
    prdb_sql_select_item *proj = (prdb_sql_select_item *)calloc(
        exp_cap == 0 ? 1 : exp_cap, sizeof(prdb_sql_select_item));
    uint32_t proj_cnt = 0;
    if (proj == NULL) {
        mat_table_free(&from_t);
        free(slices);
        st = PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
        goto cleanup_ctes;
    }
    for (uint32_t i = 0; i < sel->item_count; ++i) {
        if (sel->items[i].is_star) {
            for (uint32_t c = 0; c < from_t.col_count; ++c) {
                prdb_sql_expr *ce = (prdb_sql_expr *)prdb_sql_arena_alloc(
                    &out_table->arena, sizeof(prdb_sql_expr), 8);
                if (ce == NULL) {
                    free(proj); mat_table_free(&from_t); free(slices);
                    st = PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
                    goto cleanup_ctes;
                }
                ce->kind = PRDB_SQL_EXPR_COLUMN;
                ce->u.col.col_name = from_t.cols[c].name;
                ce->u.col.col_name_len = from_t.cols[c].name_len;
                ce->u.col.scope_depth = 0;
                ce->u.col.col_ordinal = c;
                ce->bound_type = from_t.cols[c].type_code;
                ce->bound_precision = from_t.cols[c].decimal_precision;
                ce->bound_scale = from_t.cols[c].decimal_scale;
                ce->is_bound = 1;
                proj[proj_cnt].expr = ce;
                proj[proj_cnt].alias = from_t.cols[c].name;
                proj[proj_cnt].alias_len = from_t.cols[c].name_len;
                ++proj_cnt;
            }
        } else {
            proj[proj_cnt++] = sel->items[i];
        }
    }

    /* Bind expressions */
    bind_scope *bscopes =
        (bind_scope *)calloc(1 + outer_scope_count, sizeof(bind_scope));
    agg_collector aggs = {0};
    if (bscopes == NULL) {
        free(proj); mat_table_free(&from_t); free(slices);
        st = PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
        goto cleanup_ctes;
    }
    bscopes[0] = (bind_scope){from_t.cols, from_t.col_count, slices, slice_cnt};
    for (uint32_t i = 0; i < outer_scope_count; ++i) {
        bscopes[1 + i] = (bind_scope){
            outer_scopes[i].columns, outer_scopes[i].column_count, NULL, 0};
    }
    bind_env benv = {bscopes, 1 + outer_scope_count, proj, proj_cnt, &aggs, 0};

    if (bind_expr(sel->where_expr, &benv) != PRDB_SDK_SQL_BIND_OK) {
        st = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
        goto cleanup_bind;
    }
    for (uint32_t i = 0; i < sel->group_by_count; ++i) {
        if (bind_expr(sel->group_by[i], &benv) != PRDB_SDK_SQL_BIND_OK) {
            st = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
            goto cleanup_bind;
        }
    }
    benv.allow_agg = 1;
    for (uint32_t i = 0; i < proj_cnt; ++i) {
        if (bind_expr(proj[i].expr, &benv) != PRDB_SDK_SQL_BIND_OK) {
            st = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
            goto cleanup_bind;
        }
    }
    if (bind_expr(sel->having_expr, &benv) != PRDB_SDK_SQL_BIND_OK) {
        st = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
        goto cleanup_bind;
    }
    for (uint32_t i = 0; i < sel->order_by_count; ++i) {
        prdb_sql_expr *oe = sel->order_by[i].expr;
        /* Check if oe is an unqualified identifier matching a projection alias */
        if (oe->kind == PRDB_SQL_EXPR_COLUMN && oe->u.col.table_name == NULL) {
            int matched_alias = 0;
            for (uint32_t p = 0; p < proj_cnt; ++p) {
                if (proj[p].alias != NULL &&
                    bytes_ieq(oe->u.col.col_name, oe->u.col.col_name_len,
                              proj[p].alias, proj[p].alias_len)) {
                    sel->order_by[i].expr = proj[p].expr;
                    matched_alias = 1;
                    break;
                }
            }
            if (matched_alias) continue;
        }
        if (bind_expr(oe, &benv) != PRDB_SDK_SQL_BIND_OK) {
            st = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
            goto cleanup_bind;
        }
    }

    /* Filter rows via WHERE */
    uint32_t *filtered_rows = (uint32_t *)malloc(
        (from_t.row_count == 0 ? 1 : from_t.row_count) * sizeof(uint32_t));
    uint32_t filtered_cnt = 0;
    prdb_sql_eval_scope *eval_scopes = (prdb_sql_eval_scope *)calloc(
        1 + outer_scope_count, sizeof(prdb_sql_eval_scope));
    if (filtered_rows == NULL || eval_scopes == NULL) {
        free(filtered_rows); free(eval_scopes);
        st = PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
        goto cleanup_bind;
    }
    for (uint32_t i = 0; i < outer_scope_count; ++i)
        eval_scopes[1 + i] = outer_scopes[i];
    subq_runtime sub_rt = {ec, NULL, 0, 0};

    for (uint32_t r = 0; r < from_t.row_count; ++r) {
        if (sel->where_expr != NULL) {
            eval_scopes[0] = (prdb_sql_eval_scope){
                from_t.cols, from_t.rows[r].cells, from_t.col_count};
            prdb_sql_eval_row_set rctx = {
                eval_scopes, 1 + outer_scope_count, ec->params, ec->param_count,
                ec->param_meta, NULL, 0, eval_subquery_cb, &sub_rt};
            uint32_t truth = 0;
            if (prdb_sql_eval_truth_v1(sel->where_expr, &rctx, &truth) !=
                PRDB_SDK_SQL_EVAL_OK) {
                st = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
                goto cleanup_eval;
            }
            if (truth != PRDB_SDK_SQL_TRUTH_TRUE) continue;
        }
        filtered_rows[filtered_cnt++] = r;
    }

    /* Set up output table columns */
    out_table->col_count = proj_cnt;
    out_table->cols = (prdb_sdk_sql_column_shape_v1 *)calloc(
        proj_cnt == 0 ? 1 : proj_cnt, sizeof(prdb_sdk_sql_column_shape_v1));
    if (out_table->cols == NULL) {
        st = PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
        goto cleanup_eval;
    }
    for (uint32_t p = 0; p < proj_cnt; ++p) {
        uint8_t name_buf[32];
        const uint8_t *cname = proj[p].alias;
        uint32_t clen = proj[p].alias_len;
        if (cname == NULL && proj[p].expr->kind == PRDB_SQL_EXPR_COLUMN) {
            cname = proj[p].expr->u.col.col_name;
            clen = proj[p].expr->u.col.col_name_len;
        }
        if (cname == NULL) {
            int n = 0;
            uint32_t v = p + 1;
            name_buf[n++] = 'c';
            name_buf[n++] = 'o';
            name_buf[n++] = 'l';
            name_buf[n++] = '_';
            uint8_t tmp[12];
            int tc = 0;
            do { tmp[tc++] = (uint8_t)('0' + (v % 10)); v /= 10; } while (v);
            while (tc > 0) name_buf[n++] = tmp[--tc];
            cname = name_buf;
            clen = (uint32_t)n;
        }
        out_table->cols[p].name = (const uint8_t *)prdb_sql_arena_strdup(
            &out_table->arena, cname, clen);
        out_table->cols[p].name_len = clen;
        out_table->cols[p].ordinal = p;
        out_table->cols[p].type_code =
            proj[p].expr->bound_type == 0 ? PRDB_SDK_VALUE_TYPE_BIGINT
                                          : proj[p].expr->bound_type;
        out_table->cols[p].decimal_precision = proj[p].expr->bound_precision;
        out_table->cols[p].decimal_scale = proj[p].expr->bound_scale;
    }

    /* Grouping / Aggregation or Row Projection */
    int is_grouped = (sel->group_by_count > 0 || aggs.count > 0);
    mat_table pre_sort_t;
    mat_table_init(&pre_sort_t, 64 * 1024 * 1024);
    pre_sort_t.col_count = out_table->col_count;
    pre_sort_t.cols = (prdb_sdk_sql_column_shape_v1 *)calloc(
        out_table->col_count == 0 ? 1 : out_table->col_count,
        sizeof(prdb_sdk_sql_column_shape_v1));
    if (pre_sort_t.cols == NULL) {
        st = PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
        goto cleanup_eval;
    }
    memcpy(pre_sort_t.cols, out_table->cols,
           out_table->col_count * sizeof(prdb_sdk_sql_column_shape_v1));

    /* Also track order-by keys alongside pre_sort_t rows */
    sort_row_item *sort_items = NULL;
    uint32_t sort_cap = 0;

    if (!is_grouped) {
        prdb_sdk_value_v1 *pvals = (prdb_sdk_value_v1 *)calloc(
            proj_cnt == 0 ? 1 : proj_cnt, sizeof(prdb_sdk_value_v1));
        prdb_sql_owned_val *povals = (prdb_sql_owned_val *)calloc(
            proj_cnt == 0 ? 1 : proj_cnt, sizeof(prdb_sql_owned_val));
        if (pvals == NULL || povals == NULL) {
            free(pvals); free(povals);
            mat_table_free(&pre_sort_t);
            st = PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
            goto cleanup_eval;
        }
        for (uint32_t i = 0; i < filtered_cnt; ++i) {
            uint32_t r = filtered_rows[i];
            eval_scopes[0] = (prdb_sql_eval_scope){
                from_t.cols, from_t.rows[r].cells, from_t.col_count};
            prdb_sql_eval_row_set rctx = {
                eval_scopes, 1 + outer_scope_count, ec->params, ec->param_count,
                ec->param_meta, NULL, 0, eval_subquery_cb, &sub_rt};
            for (uint32_t p = 0; p < proj_cnt; ++p) {
                if (prdb_sql_eval_expr_v1(proj[p].expr, &rctx, &povals[p]) !=
                    PRDB_SDK_SQL_EVAL_OK) {
                    for (uint32_t k = 0; k <= p; ++k)
                        prdb_sql_owned_val_free(&povals[k]);
                    free(pvals); free(povals);
                    mat_table_free(&pre_sort_t);
                    st = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
                    goto cleanup_eval;
                }
                pvals[p] = povals[p].val;
                if (pvals[p].state == PRDB_SDK_VALUE_STATE_VALUE &&
                    out_table->cols[p].type_code != pvals[p].type_code) {
                    out_table->cols[p].type_code = pvals[p].type_code;
                }
            }
            mat_table_append_row(&pre_sort_t, pvals, NULL);
            for (uint32_t p = 0; p < proj_cnt; ++p)
                prdb_sql_owned_val_free(&povals[p]);

            if (sel->order_by_count > 0) {
                if (pre_sort_t.row_count > sort_cap) {
                    uint32_t ncap = sort_cap == 0 ? 64 : sort_cap * 2;
                    sort_row_item *ns = (sort_row_item *)realloc(
                        sort_items, ncap * sizeof(sort_row_item));
                    if (ns == NULL) {
                        free(pvals); free(povals);
                        mat_table_free(&pre_sort_t);
                        st = PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
                        goto cleanup_eval;
                    }
                    sort_items = ns;
                    sort_cap = ncap;
                }
                sort_row_item *si = &sort_items[pre_sort_t.row_count - 1];
                si->row_idx = pre_sort_t.row_count - 1;
                si->keys = (prdb_sql_owned_val *)calloc(
                    sel->order_by_count, sizeof(prdb_sql_owned_val));
                for (uint32_t k = 0; k < sel->order_by_count; ++k) {
                    prdb_sql_eval_expr_v1(sel->order_by[k].expr, &rctx,
                                          &si->keys[k]);
                }
            }
        }
        free(pvals);
        free(povals);
    } else {
        /* Grouped / Aggregate execution */
        typedef struct group_bucket {
            prdb_sql_owned_val *gkeys;
            uint32_t *row_indices;
            uint32_t row_cnt;
            uint32_t row_cap;
            uint64_t hash;
        } group_bucket;

        uint32_t gb_cnt = sel->group_by_count;
        uint32_t g_cnt = 0, g_cap = 0, nbuckets = 16;
        group_bucket *groups = NULL;
        uint32_t *ht = (uint32_t *)malloc(nbuckets * sizeof(uint32_t));
        uint32_t *next_g = NULL;
        if (ht == NULL) {
            mat_table_free(&pre_sort_t);
            st = PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
            goto cleanup_eval;
        }
        memset(ht, 0xff, nbuckets * sizeof(uint32_t));

        for (uint32_t i = 0; i < filtered_cnt; ++i) {
            uint32_t r = filtered_rows[i];
            eval_scopes[0] = (prdb_sql_eval_scope){
                from_t.cols, from_t.rows[r].cells, from_t.col_count};
            prdb_sql_eval_row_set rctx = {
                eval_scopes, 1 + outer_scope_count, ec->params, ec->param_count,
                ec->param_meta, NULL, 0, eval_subquery_cb, &sub_rt};
            prdb_sql_owned_val *gkeys =
                gb_cnt == 0 ? NULL
                            : (prdb_sql_owned_val *)calloc(
                                  gb_cnt, sizeof(prdb_sql_owned_val));
            uint64_t h = 0;
            for (uint32_t g = 0; g < gb_cnt; ++g) {
                if (prdb_sql_eval_expr_v1(sel->group_by[g], &rctx, &gkeys[g]) !=
                    PRDB_SDK_SQL_EVAL_OK) {
                    for (uint32_t k = 0; k <= g; ++k)
                        prdb_sql_owned_val_free(&gkeys[k]);
                    free(gkeys);
                    st = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
                    break;
                }
                h ^= hash_value(&gkeys[g].val, sel->group_by[g]->bound_scale) +
                     0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
            }
            if (st != PRDB_SDK_SQL_DISPATCH_OK) {
                free(ht); free(next_g); free(groups);
                mat_table_free(&pre_sort_t);
                goto cleanup_eval;
            }
            uint32_t b = (uint32_t)(h & (nbuckets - 1));
            uint32_t found_g = UINT32_MAX;
            for (uint32_t g = ht[b]; g != UINT32_MAX; g = next_g[g]) {
                if (groups[g].hash != h) continue;
                int eq = 1;
                for (uint32_t k = 0; k < gb_cnt; ++k) {
                    if (!values_equal_for_key(
                            &gkeys[k].val, &groups[g].gkeys[k].val,
                            sel->group_by[k]->bound_scale, 1)) {
                        eq = 0;
                        break;
                    }
                }
                if (eq) { found_g = g; break; }
            }
            if (found_g != UINT32_MAX) {
                for (uint32_t k = 0; k < gb_cnt; ++k)
                    prdb_sql_owned_val_free(&gkeys[k]);
                free(gkeys);
            } else {
                if (g_cnt == g_cap) {
                    uint32_t ncap = g_cap == 0 ? 16 : g_cap * 2;
                    groups = (group_bucket *)realloc(
                        groups, ncap * sizeof(group_bucket));
                    next_g = (uint32_t *)realloc(next_g, ncap * sizeof(uint32_t));
                    g_cap = ncap;
                    if (g_cnt * 2 >= nbuckets) {
                        nbuckets *= 2;
                        ht = (uint32_t *)realloc(ht, nbuckets * sizeof(uint32_t));
                        memset(ht, 0xff, nbuckets * sizeof(uint32_t));
                        for (uint32_t k = 0; k < g_cnt; ++k) {
                            uint32_t kb = (uint32_t)(groups[k].hash & (nbuckets - 1));
                            next_g[k] = ht[kb];
                            ht[kb] = k;
                        }
                        b = (uint32_t)(h & (nbuckets - 1));
                    }
                }
                found_g = g_cnt++;
                groups[found_g] = (group_bucket){gkeys, NULL, 0, 0, h};
                next_g[found_g] = ht[b];
                ht[b] = found_g;
            }
            group_bucket *gb = &groups[found_g];
            if (gb->row_cnt == gb->row_cap) {
                uint32_t nrc = gb->row_cap == 0 ? 16 : gb->row_cap * 2;
                gb->row_indices = (uint32_t *)realloc(
                    gb->row_indices, nrc * sizeof(uint32_t));
                gb->row_cap = nrc;
            }
            gb->row_indices[gb->row_cnt++] = r;
        }
        /* Scalar aggregate on 0 input rows produces 1 empty group */
        if (g_cnt == 0 && gb_cnt == 0) {
            groups = (group_bucket *)calloc(1, sizeof(group_bucket));
            g_cnt = 1;
        }

        prdb_sdk_value_v1 *agg_vals = (prdb_sdk_value_v1 *)calloc(
            aggs.count == 0 ? 1 : aggs.count, sizeof(prdb_sdk_value_v1));
        prdb_sql_owned_val *agg_ovals = (prdb_sql_owned_val *)calloc(
            aggs.count == 0 ? 1 : aggs.count, sizeof(prdb_sql_owned_val));
        prdb_sdk_value_v1 *pvals = (prdb_sdk_value_v1 *)calloc(
            proj_cnt == 0 ? 1 : proj_cnt, sizeof(prdb_sdk_value_v1));
        prdb_sql_owned_val *povals = (prdb_sql_owned_val *)calloc(
            proj_cnt == 0 ? 1 : proj_cnt, sizeof(prdb_sql_owned_val));

        for (uint32_t g = 0; g < g_cnt; ++g) {
            group_bucket *gb = &groups[g];
            /* Compute each aggregate for this group */
            for (uint32_t a = 0; a < aggs.count; ++a) {
                prdb_sql_expr *ae = aggs.aggs[a];
                uint32_t ak = ae->u.agg.agg_kind;
                if (ak == PRDB_SDK_SQL_AGGREGATE_COUNT_STAR) {
                    int64_t cnt = (int64_t)gb->row_cnt;
                    agg_ovals[a].inline_buf[0] = (uint8_t)((uint64_t)cnt >> 56);
                    for (int k = 0; k < 8; ++k)
                        agg_ovals[a].inline_buf[k] =
                            (uint8_t)((uint64_t)cnt >> (56 - k * 8));
                    agg_ovals[a].val = (prdb_sdk_value_v1){
                        sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
                        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BIGINT,
                        agg_ovals[a].inline_buf, 8};
                    agg_vals[a] = agg_ovals[a].val;
                    continue;
                }
                /* Evaluate argument across group rows */
                prdb_sql_owned_val *vals = (prdb_sql_owned_val *)calloc(
                    gb->row_cnt == 0 ? 1 : gb->row_cnt,
                    sizeof(prdb_sql_owned_val));
                uint32_t non_null = 0;
                for (uint32_t i = 0; i < gb->row_cnt; ++i) {
                    uint32_t r = gb->row_indices[i];
                    eval_scopes[0] = (prdb_sql_eval_scope){
                        from_t.cols, from_t.rows[r].cells, from_t.col_count};
                    prdb_sql_eval_row_set rctx = {
                        eval_scopes, 1 + outer_scope_count, ec->params,
                        ec->param_count, ec->param_meta, NULL, 0,
                        eval_subquery_cb, &sub_rt};
                    prdb_sql_owned_val v = {0};
                    if (prdb_sql_eval_expr_v1(ae->u.agg.arg, &rctx, &v) !=
                        PRDB_SDK_SQL_EVAL_OK) {
                        prdb_sql_owned_val_free(&v);
                        st = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
                        break;
                    }
                    if (v.val.state == PRDB_SDK_VALUE_STATE_NULL) {
                        prdb_sql_owned_val_free(&v);
                        continue;
                    }
                    if (ae->u.agg.distinct) {
                        int dup = 0;
                        uint8_t sc = ae->u.agg.arg->bound_scale;
                        for (uint32_t k = 0; k < non_null; ++k) {
                            if (values_equal_for_key(&v.val, &vals[k].val, sc,
                                                     0)) {
                                dup = 1;
                                break;
                            }
                        }
                        if (dup) {
                            prdb_sql_owned_val_free(&v);
                            continue;
                        }
                    }
                    vals[non_null++] = v;
                }
                if (st != PRDB_SDK_SQL_DISPATCH_OK) {
                    for (uint32_t k = 0; k < non_null; ++k)
                        prdb_sql_owned_val_free(&vals[k]);
                    free(vals);
                    break;
                }
                if (ak == PRDB_SDK_SQL_AGGREGATE_COUNT_COLUMN) {
                    int64_t cnt = (int64_t)non_null;
                    for (int k = 0; k < 8; ++k)
                        agg_ovals[a].inline_buf[k] =
                            (uint8_t)((uint64_t)cnt >> (56 - k * 8));
                    agg_ovals[a].val = (prdb_sdk_value_v1){
                        sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
                        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BIGINT,
                        agg_ovals[a].inline_buf, 8};
                } else if (non_null == 0) {
                    agg_ovals[a].val = (prdb_sdk_value_v1){
                        sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
                        PRDB_SDK_VALUE_STATE_NULL, 0, NULL, 0};
                } else if (ak == PRDB_SDK_SQL_AGGREGATE_MIN ||
                           ak == PRDB_SDK_SQL_AGGREGATE_MAX) {
                    uint32_t best = 0;
                    uint8_t sc = ae->u.agg.arg->bound_scale;
                    uint32_t cmp_op = ak == PRDB_SDK_SQL_AGGREGATE_MIN
                                          ? PRDB_SDK_SQL_COMPARE_LESS
                                          : PRDB_SDK_SQL_COMPARE_GREATER;
                    for (uint32_t k = 1; k < non_null; ++k) {
                        uint32_t tr = 0;
                        prdb_sdk_sql_compare_v1(&vals[k].val, &vals[best].val,
                                                cmp_op, sc, sc, &tr);
                        if (tr == PRDB_SDK_SQL_TRUTH_TRUE) best = k;
                    }
                    agg_ovals[a] = vals[best];
                    vals[best].heap_buf = NULL;
                    if (agg_ovals[a].val.data == vals[best].inline_buf)
                        agg_ovals[a].val.data = agg_ovals[a].inline_buf;
                } else {
                    /* SUM or AVG: accumulate in chunks of 128 or direct */
                    uint32_t at = ae->u.agg.arg->bound_type;
                    uint8_t as = ae->u.agg.arg->bound_scale;
                    if (at != PRDB_SDK_VALUE_TYPE_DECIMAL) {
                        int64_t sum = 0;
                        for (uint32_t k = 0; k < non_null; ++k) {
                            int64_t iv = 0;
                            if (vals[k].val.type_code ==
                                PRDB_SDK_VALUE_TYPE_SMALLINT) {
                                int16_t x = 0;
                                prdb_sdk_value_get_smallint_v1(&vals[k].val, &x);
                                iv = x;
                            } else if (vals[k].val.type_code ==
                                       PRDB_SDK_VALUE_TYPE_INTEGER) {
                                int32_t x = 0;
                                prdb_sdk_value_get_integer_v1(&vals[k].val, &x);
                                iv = x;
                            } else {
                                prdb_sdk_value_get_bigint_v1(&vals[k].val, &iv);
                            }
                            sum += iv;
                        }
                        if (ak == PRDB_SDK_SQL_AGGREGATE_SUM) {
                            for (int k = 0; k < 8; ++k)
                                agg_ovals[a].inline_buf[k] =
                                    (uint8_t)((uint64_t)sum >> (56 - k * 8));
                            agg_ovals[a].val = (prdb_sdk_value_v1){
                                sizeof(prdb_sdk_value_v1),
                                PRDB_SDK_VALUE_ABI_V1_VERSION,
                                PRDB_SDK_VALUE_STATE_VALUE,
                                PRDB_SDK_VALUE_TYPE_BIGINT,
                                agg_ovals[a].inline_buf, 8};
                        } else {
                            /* AVG of int -> DECIMAL(38,6): (sum * 10^6) / non_null */
                            uint8_t *b = agg_ovals[a].inline_buf;
                            be128_from_i64(sum, b);
                            be128_mul_u32_signed(b, 1000000U);
                            be128_div_u32_signed(b, non_null);
                            agg_ovals[a].val = (prdb_sdk_value_v1){
                                sizeof(prdb_sdk_value_v1),
                                PRDB_SDK_VALUE_ABI_V1_VERSION,
                                PRDB_SDK_VALUE_STATE_VALUE,
                                PRDB_SDK_VALUE_TYPE_DECIMAL, b, 16};
                        }
                    } else {
                        uint8_t sum[16] = {0};
                        for (uint32_t k = 0; k < non_null; ++k) {
                            be128_add(sum, vals[k].val.data);
                        }
                        if (ak == PRDB_SDK_SQL_AGGREGATE_SUM) {
                            uint8_t *b = agg_ovals[a].inline_buf;
                            memcpy(b, sum, 16);
                            agg_ovals[a].val = (prdb_sdk_value_v1){
                                sizeof(prdb_sdk_value_v1),
                                PRDB_SDK_VALUE_ABI_V1_VERSION,
                                PRDB_SDK_VALUE_STATE_VALUE,
                                PRDB_SDK_VALUE_TYPE_DECIMAL, b, 16};
                        } else {
                            uint8_t rscale = ae->bound_scale;
                            uint8_t *b = agg_ovals[a].inline_buf;
                            memcpy(b, sum, 16);
                            for (uint8_t s = as; s < rscale; ++s)
                                be128_mul_u32_signed(b, 10U);
                            be128_div_u32_signed(b, non_null);
                            agg_ovals[a].val = (prdb_sdk_value_v1){
                                sizeof(prdb_sdk_value_v1),
                                PRDB_SDK_VALUE_ABI_V1_VERSION,
                                PRDB_SDK_VALUE_STATE_VALUE,
                                PRDB_SDK_VALUE_TYPE_DECIMAL, b, 16};
                        }
                    }
                }
                for (uint32_t k = 0; k < non_null; ++k)
                    prdb_sql_owned_val_free(&vals[k]);
                free(vals);
                agg_vals[a] = agg_ovals[a].val;
            }
            if (st != PRDB_SDK_SQL_DISPATCH_OK) break;

            uint32_t rep_r = gb->row_cnt > 0 ? gb->row_indices[0] : 0;
            eval_scopes[0] = (prdb_sql_eval_scope){
                from_t.cols,
                gb->row_cnt > 0 ? from_t.rows[rep_r].cells : NULL,
                gb->row_cnt > 0 ? from_t.col_count : 0};
            prdb_sql_eval_row_set rctx = {
                eval_scopes, 1 + outer_scope_count, ec->params, ec->param_count,
                ec->param_meta, agg_vals, aggs.count, eval_subquery_cb, &sub_rt};

            if (sel->having_expr != NULL) {
                uint32_t htruth = 0;
                if (prdb_sql_eval_truth_v1(sel->having_expr, &rctx, &htruth) !=
                    PRDB_SDK_SQL_EVAL_OK) {
                    st = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
                    break;
                }
                if (htruth != PRDB_SDK_SQL_TRUTH_TRUE) {
                    for (uint32_t a = 0; a < aggs.count; ++a)
                        prdb_sql_owned_val_free(&agg_ovals[a]);
                    continue;
                }
            }
            for (uint32_t p = 0; p < proj_cnt; ++p) {
                if (prdb_sql_eval_expr_v1(proj[p].expr, &rctx, &povals[p]) !=
                    PRDB_SDK_SQL_EVAL_OK) {
                    st = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
                    break;
                }
                pvals[p] = povals[p].val;
            }
            if (st == PRDB_SDK_SQL_DISPATCH_OK) {
                mat_table_append_row(&pre_sort_t, pvals, NULL);
                if (sel->order_by_count > 0) {
                    if (pre_sort_t.row_count > sort_cap) {
                        uint32_t ncap = sort_cap == 0 ? 64 : sort_cap * 2;
                        sort_items = (sort_row_item *)realloc(
                            sort_items, ncap * sizeof(sort_row_item));
                        sort_cap = ncap;
                    }
                    sort_row_item *si = &sort_items[pre_sort_t.row_count - 1];
                    si->row_idx = pre_sort_t.row_count - 1;
                    si->keys = (prdb_sql_owned_val *)calloc(
                        sel->order_by_count, sizeof(prdb_sql_owned_val));
                    for (uint32_t k = 0; k < sel->order_by_count; ++k) {
                        prdb_sql_eval_expr_v1(sel->order_by[k].expr, &rctx,
                                              &si->keys[k]);
                    }
                }
            }
            for (uint32_t p = 0; p < proj_cnt; ++p)
                prdb_sql_owned_val_free(&povals[p]);
            for (uint32_t a = 0; a < aggs.count; ++a)
                prdb_sql_owned_val_free(&agg_ovals[a]);
            if (st != PRDB_SDK_SQL_DISPATCH_OK) break;
        }
        for (uint32_t g = 0; g < g_cnt; ++g) {
            for (uint32_t k = 0; k < gb_cnt; ++k)
                prdb_sql_owned_val_free(&groups[g].gkeys[k]);
            free(groups[g].gkeys);
            free(groups[g].row_indices);
        }
        free(groups);
        free(ht);
        free(next_g);
        free(agg_vals);
        free(agg_ovals);
        free(pvals);
        free(povals);
    }

    if (st == PRDB_SDK_SQL_DISPATCH_OK) {
        if (sel->order_by_count > 0 && pre_sort_t.row_count > 1) {
            g_sort_specs = sel->order_by;
            g_sort_spec_count = sel->order_by_count;
            qsort(sort_items, pre_sort_t.row_count, sizeof(sort_row_item),
                  cmp_sort_items);
        }
        uint64_t start_row = sel->has_offset ? sel->offset : 0;
        uint64_t end_row = pre_sort_t.row_count;
        if (sel->has_limit && start_row + sel->limit < end_row) {
            end_row = start_row + sel->limit;
        }
        for (uint64_t i = start_row; i < end_row; ++i) {
            uint32_t r = sel->order_by_count > 0 ? sort_items[i].row_idx
                                                 : (uint32_t)i;
            if (sel->distinct) {
                int dup = 0;
                for (uint32_t prev = 0; prev < out_table->row_count; ++prev) {
                    int all_eq = 1;
                    for (uint32_t c = 0; c < proj_cnt; ++c) {
                        if (!values_equal_for_key(
                                &pre_sort_t.rows[r].cells[c],
                                &out_table->rows[prev].cells[c],
                                out_table->cols[c].decimal_scale, 1)) {
                            all_eq = 0;
                            break;
                        }
                    }
                    if (all_eq) { dup = 1; break; }
                }
                if (dup) continue;
            }
            mat_table_append_row(out_table, pre_sort_t.rows[r].cells, NULL);
        }
    }

    if (sort_items != NULL) {
        for (uint32_t i = 0; i < pre_sort_t.row_count; ++i) {
            for (uint32_t k = 0; k < sel->order_by_count; ++k)
                prdb_sql_owned_val_free(&sort_items[i].keys[k]);
            free(sort_items[i].keys);
        }
        free(sort_items);
    }
    mat_table_free(&pre_sort_t);

cleanup_eval:
    for (uint32_t i = 0; i < sub_rt.cache_count; ++i)
        mat_table_free(&sub_rt.cache[i].result);
    free(sub_rt.cache);
    free(filtered_rows);
    free(eval_scopes);
cleanup_bind:
    free(bscopes);
    free(aggs.aggs);
    free(proj);
    mat_table_free(&from_t);
    free(slices);
cleanup_ctes:
    while (ec->cte_count > saved_cte_count) {
        --ec->cte_count;
        mat_table_free(&ec->ctes[ec->cte_count].data);
    }
    return st;
}

/* --- DML Execution (INSERT / UPDATE / DELETE) --- */

static int32_t eval_to_storage_val(const prdb_sql_expr *expr,
                                   const prdb_sql_eval_row_set *ctx,
                                   uint32_t target_type, uint8_t target_prec,
                                   uint8_t target_scale,
                                   prdb_sql_owned_val *out) {
    int32_t st = prdb_sql_eval_expr_v1(expr, ctx, out);
    if (st != PRDB_SDK_SQL_EVAL_OK) return st;
    if (out->val.state == PRDB_SDK_VALUE_STATE_NULL) return PRDB_SDK_SQL_EVAL_OK;
    /* Coerce integer literal/expr to target int or decimal if compatible */
    if ((target_type == PRDB_SDK_VALUE_TYPE_SMALLINT ||
         target_type == PRDB_SDK_VALUE_TYPE_INTEGER ||
         target_type == PRDB_SDK_VALUE_TYPE_BIGINT) &&
        (out->val.type_code == PRDB_SDK_VALUE_TYPE_SMALLINT ||
         out->val.type_code == PRDB_SDK_VALUE_TYPE_INTEGER ||
         out->val.type_code == PRDB_SDK_VALUE_TYPE_BIGINT)) {
        int64_t iv = 0;
        if (out->val.type_code == PRDB_SDK_VALUE_TYPE_SMALLINT) {
            int16_t x = 0; prdb_sdk_value_get_smallint_v1(&out->val, &x); iv = x;
        } else if (out->val.type_code == PRDB_SDK_VALUE_TYPE_INTEGER) {
            int32_t x = 0; prdb_sdk_value_get_integer_v1(&out->val, &x); iv = x;
        } else {
            prdb_sdk_value_get_bigint_v1(&out->val, &iv);
        }
        prdb_sql_owned_val_free(out);
        if (target_type == PRDB_SDK_VALUE_TYPE_SMALLINT) {
            if (iv < INT16_MIN || iv > INT16_MAX)
                return PRDB_SDK_SQL_EVAL_NUMERIC_OVERFLOW;
            uint16_t u = (uint16_t)(int16_t)iv;
            out->inline_buf[0] = (uint8_t)(u >> 8);
            out->inline_buf[1] = (uint8_t)(u & 0xff);
            out->val = (prdb_sdk_value_v1){sizeof(prdb_sdk_value_v1),
                                           PRDB_SDK_VALUE_ABI_V1_VERSION,
                                           PRDB_SDK_VALUE_STATE_VALUE,
                                           target_type, out->inline_buf, 2};
        } else if (target_type == PRDB_SDK_VALUE_TYPE_INTEGER) {
            if (iv < INT32_MIN || iv > INT32_MAX)
                return PRDB_SDK_SQL_EVAL_NUMERIC_OVERFLOW;
            uint32_t u = (uint32_t)(int32_t)iv;
            out->inline_buf[0] = (uint8_t)(u >> 24);
            out->inline_buf[1] = (uint8_t)(u >> 16);
            out->inline_buf[2] = (uint8_t)(u >> 8);
            out->inline_buf[3] = (uint8_t)(u & 0xff);
            out->val = (prdb_sdk_value_v1){sizeof(prdb_sdk_value_v1),
                                           PRDB_SDK_VALUE_ABI_V1_VERSION,
                                           PRDB_SDK_VALUE_STATE_VALUE,
                                           target_type, out->inline_buf, 4};
        } else {
            for (int k = 0; k < 8; ++k)
                out->inline_buf[k] = (uint8_t)((uint64_t)iv >> (56 - k * 8));
            out->val = (prdb_sdk_value_v1){sizeof(prdb_sdk_value_v1),
                                           PRDB_SDK_VALUE_ABI_V1_VERSION,
                                           PRDB_SDK_VALUE_STATE_VALUE,
                                           target_type, out->inline_buf, 8};
        }
        return PRDB_SDK_SQL_EVAL_OK;
    }
    if (target_type == PRDB_SDK_VALUE_TYPE_DECIMAL) {
        uint8_t v128[16] = {0};
        uint8_t src_scale = 0;
        if (out->val.type_code == PRDB_SDK_VALUE_TYPE_SMALLINT) {
            int16_t x = 0; prdb_sdk_value_get_smallint_v1(&out->val, &x);
            be128_from_i64(x, v128);
        } else if (out->val.type_code == PRDB_SDK_VALUE_TYPE_INTEGER) {
            int32_t x = 0; prdb_sdk_value_get_integer_v1(&out->val, &x);
            be128_from_i64(x, v128);
        } else if (out->val.type_code == PRDB_SDK_VALUE_TYPE_BIGINT) {
            int64_t x = 0; prdb_sdk_value_get_bigint_v1(&out->val, &x);
            be128_from_i64(x, v128);
        } else if (out->val.type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
                   out->val.data_len == 16) {
            memcpy(v128, out->val.data, 16);
            src_scale = expr->kind == PRDB_SQL_EXPR_DECIMAL ? expr->u.dec_val.scale
                        : (expr->is_bound ? expr->bound_scale : target_scale);
        } else {
            return PRDB_SDK_SQL_EVAL_TYPE_MISMATCH;
        }
        while (src_scale < target_scale) { be128_mul_u32_signed(v128, 10U); ++src_scale; }
        while (src_scale > target_scale) { be128_div_u32_signed(v128, 10U); --src_scale; }
        (void)target_prec;
        prdb_sql_owned_val_free(out);
        memcpy(out->inline_buf, v128, 16);
        out->val = (prdb_sdk_value_v1){sizeof(prdb_sdk_value_v1),
                                       PRDB_SDK_VALUE_ABI_V1_VERSION,
                                       PRDB_SDK_VALUE_STATE_VALUE,
                                       PRDB_SDK_VALUE_TYPE_DECIMAL,
                                       out->inline_buf, 16};
        return PRDB_SDK_SQL_EVAL_OK;
    }
    if (target_type == PRDB_SDK_VALUE_TYPE_VARCHAR &&
        out->val.type_code == PRDB_SDK_VALUE_TYPE_TEXT) {
        out->val.type_code = PRDB_SDK_VALUE_TYPE_VARCHAR;
        return PRDB_SDK_SQL_EVAL_OK;
    }
    if (out->val.type_code != target_type) {
        return PRDB_SDK_SQL_EVAL_TYPE_MISMATCH;
    }
    return PRDB_SDK_SQL_EVAL_OK;
}

static int32_t exec_insert_unified(exec_ctx *ec,
                                   const prdb_sql_insert_ast *ins) {
    prdb_sdk_executor_relation_v1 rel = {0};
    prdb_sdk_sql_column_shape_v1 *cols =
        (prdb_sdk_sql_column_shape_v1 *)calloc(
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, sizeof(*cols));
    uint64_t resp_len = 0;
    const uint8_t *sc = ins->schema != NULL ? ins->schema : ec->schema;
    uint32_t sc_len = ins->schema != NULL ? ins->schema_len : ec->schema_len;

    if (cols == NULL) return PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
    if (prdb_sdk_executor_resolve_relation_v1(
            ec->host, sc, sc_len, ins->relation, ins->relation_len,
            ec->resp_buf, EXEC_RESP_BYTES, &resp_len, &rel, sizeof(rel), cols,
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS) != PRDB_SDK_EXECUTOR_OK) {
        free(cols);
        return PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
    }
    uint32_t ncols = rel.column_count;
    uint32_t *col_map = (uint32_t *)calloc(ins->cols_per_row, sizeof(uint32_t));
    prdb_sdk_executor_insert_value_v1 *ivals =
        (prdb_sdk_executor_insert_value_v1 *)calloc(
            (size_t)ins->row_count * ncols, sizeof(*ivals));
    prdb_sql_owned_val *ovals = (prdb_sql_owned_val *)calloc(
        (size_t)ins->row_count * ncols, sizeof(*ovals));
    if (col_map == NULL || ivals == NULL || ovals == NULL) {
        free(cols); free(col_map); free(ivals); free(ovals);
        return PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
    }
    if (ins->column_count > 0) {
        if (ins->column_count != ins->cols_per_row) {
            free(cols); free(col_map); free(ivals); free(ovals);
            return PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
        }
        for (uint32_t i = 0; i < ins->column_count; ++i) {
            uint32_t found = UINT32_MAX;
            for (uint32_t c = 0; c < ncols; ++c) {
                if (bytes_ieq(ins->columns[i], ins->column_lens[i],
                              cols[c].name, cols[c].name_len)) {
                    found = c;
                    break;
                }
            }
            if (found == UINT32_MAX) {
                free(cols); free(col_map); free(ivals); free(ovals);
                return PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
            }
            col_map[i] = found;
        }
    } else {
        if (ins->cols_per_row != ncols) {
            free(cols); free(col_map); free(ivals); free(ovals);
            return PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
        }
        for (uint32_t c = 0; c < ncols; ++c) col_map[c] = c;
    }

    prdb_sql_eval_row_set rctx = {NULL, 0, ec->params, ec->param_count,
                                  ec->param_meta, NULL, 0, NULL, NULL};
    int32_t st = PRDB_SDK_SQL_DISPATCH_OK;
    for (uint32_t r = 0; r < ins->row_count; ++r) {
        for (uint32_t c = 0; c < ncols; ++c) {
            ivals[r * ncols + c].struct_size = sizeof(ivals[0]);
            ivals[r * ncols + c].abi_version = PRDB_SDK_EXECUTOR_ABI_V1_VERSION;
            ivals[r * ncols + c].state = PRDB_SDK_EXECUTOR_INSERT_DEFAULT;
        }
        for (uint32_t i = 0; i < ins->cols_per_row; ++i) {
            uint32_t c = col_map[i];
            const prdb_sql_expr *e = ins->rows[r][i];
            if (e == NULL) {
                ivals[r * ncols + c].state = PRDB_SDK_EXECUTOR_INSERT_DEFAULT;
            } else {
                if (eval_to_storage_val(
                        e, &rctx, cols[c].type_code, cols[c].decimal_precision,
                        cols[c].decimal_scale, &ovals[r * ncols + c]) !=
                    PRDB_SDK_SQL_EVAL_OK) {
                    st = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
                    break;
                }
                if (ovals[r * ncols + c].val.state ==
                    PRDB_SDK_VALUE_STATE_NULL) {
                    ivals[r * ncols + c].state = PRDB_SDK_EXECUTOR_INSERT_NULL;
                } else {
                    ivals[r * ncols + c].state = PRDB_SDK_EXECUTOR_INSERT_VALUE;
                    ivals[r * ncols + c].value = ovals[r * ncols + c].val;
                }
            }
        }
        if (st != PRDB_SDK_SQL_DISPATCH_OK) break;
    }
    if (st == PRDB_SDK_SQL_DISPATCH_OK) {
        uint8_t *handles = (uint8_t *)calloc(ins->row_count, 16);
        if (handles == NULL ||
            prdb_sdk_executor_stage_inserts_v1(
                ec->host, rel.executor_handle, ivals, (uint16_t)ins->row_count,
                (uint16_t)ncols, ec->req_buf, EXEC_REQ_BYTES, ec->resp_buf,
                EXEC_RESP_BYTES, &resp_len, handles,
                (uint64_t)ins->row_count * 16) != PRDB_SDK_EXECUTOR_OK) {
            st = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
        }
        free(handles);
    }

    for (size_t i = 0; i < (size_t)ins->row_count * ncols; ++i)
        prdb_sql_owned_val_free(&ovals[i]);
    free(ovals);
    free(ivals);
    free(col_map);
    free(cols);
    return st;
}

static int32_t exec_update_unified(exec_ctx *ec,
                                   const prdb_sql_update_ast *upd) {
    const uint8_t *sc = upd->schema != NULL ? upd->schema : ec->schema;
    uint32_t sc_len = upd->schema != NULL ? upd->schema_len : ec->schema_len;
    mat_table t;
    mat_table_init(&t, 64 * 1024 * 1024);
    int32_t st = scan_base_relation(ec, sc, sc_len, upd->relation,
                                    upd->relation_len, 1, &t);
    if (st != PRDB_SDK_SQL_DISPATCH_OK) {
        mat_table_free(&t);
        return st;
    }

    table_slice ts = {upd->relation, upd->relation_len, NULL, 0, 0, t.col_count};
    bind_scope bscope = {t.cols, t.col_count, &ts, 1};
    bind_env benv = {&bscope, 1, NULL, 0, NULL, 0};
    if (bind_expr(upd->where_expr, &benv) != PRDB_SDK_SQL_BIND_OK) {
        mat_table_free(&t);
        return PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
    }
    uint32_t *assign_ords =
        (uint32_t *)calloc(upd->assignment_count, sizeof(uint32_t));
    if (assign_ords == NULL) {
        mat_table_free(&t);
        return PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
    }
    for (uint32_t i = 0; i < upd->assignment_count; ++i) {
        uint32_t ord = UINT32_MAX;
        for (uint32_t c = 0; c < t.col_count; ++c) {
            if (bytes_ieq(upd->assignments[i].column,
                          upd->assignments[i].column_len, t.cols[c].name,
                          t.cols[c].name_len)) {
                ord = c;
                break;
            }
        }
        if (ord == UINT32_MAX ||
            bind_expr(upd->assignments[i].expr, &benv) != PRDB_SDK_SQL_BIND_OK) {
            free(assign_ords);
            mat_table_free(&t);
            return PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
        }
        assign_ords[i] = ord;
    }

    prdb_sdk_executor_relation_v1 rel = {0};
    prdb_sdk_sql_column_shape_v1 *cols =
        (prdb_sdk_sql_column_shape_v1 *)calloc(
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, sizeof(*cols));
    uint64_t resp_len = 0;
    if (cols == NULL ||
        prdb_sdk_executor_resolve_relation_v1(
            ec->host, sc, sc_len, upd->relation, upd->relation_len,
            ec->resp_buf, EXEC_RESP_BYTES, &resp_len, &rel, sizeof(rel), cols,
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS) != PRDB_SDK_EXECUTOR_OK) {
        free(cols);
        free(assign_ords);
        mat_table_free(&t);
        return PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
    }
    free(cols);

    prdb_sdk_executor_update_patch_v1 *patches =
        (prdb_sdk_executor_update_patch_v1 *)calloc(
            upd->assignment_count, sizeof(*patches));
    prdb_sql_owned_val *ovals = (prdb_sql_owned_val *)calloc(
        upd->assignment_count, sizeof(*ovals));
    if (patches == NULL || ovals == NULL) {
        free(patches); free(ovals); free(assign_ords);
        mat_table_free(&t);
        return PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
    }

    for (uint32_t r = 0; r < t.row_count; ++r) {
        prdb_sql_eval_scope escope = {t.cols, t.rows[r].cells, t.col_count};
        prdb_sql_eval_row_set rctx = {&escope, 1, ec->params, ec->param_count,
                                      ec->param_meta, NULL, 0, NULL, NULL};
        if (upd->where_expr != NULL) {
            uint32_t truth = 0;
            if (prdb_sql_eval_truth_v1(upd->where_expr, &rctx, &truth) !=
                PRDB_SDK_SQL_EVAL_OK) {
                st = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
                break;
            }
            if (truth != PRDB_SDK_SQL_TRUTH_TRUE) continue;
        }
        if (!t.rows[r].has_handle) {
            st = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
            break;
        }
        for (uint32_t a = 0; a < upd->assignment_count; ++a) {
            uint32_t ord = assign_ords[a];
            if (eval_to_storage_val(upd->assignments[a].expr, &rctx,
                                    t.cols[ord].type_code,
                                    t.cols[ord].decimal_precision,
                                    t.cols[ord].decimal_scale,
                                    &ovals[a]) != PRDB_SDK_SQL_EVAL_OK) {
                st = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
                break;
            }
            patches[a].struct_size = sizeof(patches[0]);
            patches[a].abi_version = PRDB_SDK_EXECUTOR_ABI_V1_VERSION;
            patches[a].ordinal = ord;
            patches[a].value = ovals[a].val;
        }
        if (st == PRDB_SDK_SQL_DISPATCH_OK) {
            if (prdb_sdk_executor_stage_updates_v1(
                    ec->host, t.rows[r].row_handle, 1, patches,
                    (uint16_t)upd->assignment_count, ec->req_buf,
                    EXEC_REQ_BYTES, ec->resp_buf, EXEC_RESP_BYTES,
                    &resp_len) != PRDB_SDK_EXECUTOR_OK) {
                st = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
            }
        }
        for (uint32_t a = 0; a < upd->assignment_count; ++a)
            prdb_sql_owned_val_free(&ovals[a]);
        if (st != PRDB_SDK_SQL_DISPATCH_OK) break;
    }

    free(ovals);
    free(patches);
    free(assign_ords);
    mat_table_free(&t);
    return st;
}

static int32_t exec_delete_unified(exec_ctx *ec,
                                   const prdb_sql_delete_ast *del) {
    const uint8_t *sc = del->schema != NULL ? del->schema : ec->schema;
    uint32_t sc_len = del->schema != NULL ? del->schema_len : ec->schema_len;
    mat_table t;
    mat_table_init(&t, 64 * 1024 * 1024);
    int32_t st = scan_base_relation(ec, sc, sc_len, del->relation,
                                    del->relation_len, 1, &t);
    if (st != PRDB_SDK_SQL_DISPATCH_OK) {
        mat_table_free(&t);
        return st;
    }

    table_slice ts = {del->relation, del->relation_len, NULL, 0, 0, t.col_count};
    bind_scope bscope = {t.cols, t.col_count, &ts, 1};
    bind_env benv = {&bscope, 1, NULL, 0, NULL, 0};
    if (bind_expr(del->where_expr, &benv) != PRDB_SDK_SQL_BIND_OK) {
        mat_table_free(&t);
        return PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
    }

    uint8_t *handles = (uint8_t *)malloc(
        (t.row_count == 0 ? 1 : t.row_count) * 16);
    uint32_t hcount = 0;
    if (handles == NULL) {
        mat_table_free(&t);
        return PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
    }
    for (uint32_t r = 0; r < t.row_count; ++r) {
        if (del->where_expr != NULL) {
            prdb_sql_eval_scope escope = {t.cols, t.rows[r].cells, t.col_count};
            prdb_sql_eval_row_set rctx = {
                &escope, 1, ec->params, ec->param_count, ec->param_meta, NULL,
                0, NULL, NULL};
            uint32_t truth = 0;
            if (prdb_sql_eval_truth_v1(del->where_expr, &rctx, &truth) !=
                PRDB_SDK_SQL_EVAL_OK) {
                free(handles);
                mat_table_free(&t);
                return PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
            }
            if (truth != PRDB_SDK_SQL_TRUTH_TRUE) continue;
        }
        if (!t.rows[r].has_handle) {
            free(handles);
            mat_table_free(&t);
            return PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
        }
        memcpy(handles + (size_t)hcount * 16, t.rows[r].row_handle, 16);
        ++hcount;
    }
    if (hcount > 0) {
        uint64_t resp_len = 0;
        if (prdb_sdk_executor_stage_deletes_v1(
                ec->host, handles, (uint16_t)hcount, ec->req_buf,
                EXEC_REQ_BYTES, ec->resp_buf, EXEC_RESP_BYTES, &resp_len) !=
            PRDB_SDK_EXECUTOR_OK) {
            st = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
        }
    }
    free(handles);
    mat_table_free(&t);
    return st;
}

/* --- Top-level entry point --- */

PRDB_SDK_INTERNAL int32_t prdb_sql_execute_unified_v1(
    const prdb_host_api_v1 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const uint8_t *parameters, uint64_t parameters_len) {
    prdb_sql_arena ast_arena;
    prdb_sql_stmt_ast stmt = {0};
    exec_ctx ec = {0};
    int32_t st = PRDB_SDK_SQL_DISPATCH_OK;

    if (host == NULL || host->struct_size < sizeof(*host) ||
        host->abi_version != PRDB_QUERY_ABI_VERSION ||
        host->statement_failure == NULL || schema == NULL || schema_len == 0 ||
        schema_len > 128 || sql == NULL || sql_len == 0 ||
        sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        parameters_len > PRDB_SDK_SQL_DISPATCH_MAX_PARAMETER_BYTES ||
        (parameters_len != 0 && parameters == NULL)) {
        if (host != NULL && host->statement_failure != NULL) {
            host->statement_failure(host->context,
                                    PRDB_SDK_SQL_DISPATCH_INVALID_ARGUMENT);
        }
        return PRDB_SDK_SQL_DISPATCH_INVALID_ARGUMENT;
    }

    prdb_sql_arena_init(&ast_arena, 16 * 1024 * 1024);
    if (prdb_sql_parse_statement_v1(&ast_arena, sql, sql_len, &stmt, NULL) !=
        PRDB_SDK_SQL_OK) {
        prdb_sql_arena_free(&ast_arena);
        host->statement_failure(host->context,
                                PRDB_SDK_SQL_DISPATCH_INVALID_ARGUMENT);
        return PRDB_SDK_SQL_DISPATCH_INVALID_ARGUMENT;
    }

    /* Decode parameters if present */
    uint32_t declared_count = 0, param_count = 0;
    prdb_sdk_value_v1 *values = NULL;
    uint8_t (*payloads)[16] = NULL;
    prdb_sdk_sql_parameter_metadata_v1 *meta = NULL;
    if (parameters_len >= 8 && parameters != NULL &&
        memcmp(parameters, "PSP1", 4) == 0) {
        declared_count = (uint32_t)(((uint16_t)parameters[4] << 8) | parameters[5]);
    }
    if (declared_count > 0) {
        values = (prdb_sdk_value_v1 *)calloc(declared_count, sizeof(*values));
        payloads = (uint8_t (*)[16])calloc(declared_count, sizeof(*payloads));
        meta = (prdb_sdk_sql_parameter_metadata_v1 *)calloc(declared_count,
                                                            sizeof(*meta));
        if (values == NULL || payloads == NULL || meta == NULL ||
            prdb_sdk_sql_decode_parameters_v1(
                parameters, parameters_len, values, declared_count, meta,
                declared_count, &param_count) != PRDB_SDK_SQL_DISPATCH_OK) {
            free(values); free(payloads); free(meta);
            prdb_sql_arena_free(&ast_arena);
            host->statement_failure(host->context,
                                    PRDB_SDK_SQL_DISPATCH_INVALID_PARAMETERS);
            return PRDB_SDK_SQL_DISPATCH_INVALID_PARAMETERS;
        }
        /* Normalize little-endian PSP1 fixed-width payloads to big-endian */
        for (uint32_t i = 0; i < param_count; ++i) {
            if (values[i].state != PRDB_SDK_VALUE_STATE_VALUE) continue;
            uint32_t tc = values[i].type_code;
            if ((tc == PRDB_SDK_VALUE_TYPE_SMALLINT && values[i].data_len == 2) ||
                ((tc == PRDB_SDK_VALUE_TYPE_INTEGER ||
                  tc == PRDB_SDK_VALUE_TYPE_DATE) &&
                 values[i].data_len == 4) ||
                ((tc == PRDB_SDK_VALUE_TYPE_BIGINT ||
                  tc == PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ) &&
                 values[i].data_len == 8) ||
                (tc == PRDB_SDK_VALUE_TYPE_DECIMAL && values[i].data_len == 16)) {
                for (uint64_t b = 0; b < values[i].data_len; ++b) {
                    payloads[i][b] = values[i].data[values[i].data_len - 1 - b];
                }
                values[i].data = payloads[i];
            }
        }
    }

    ec.host = host;
    ec.schema = schema;
    ec.schema_len = schema_len;
    ec.params = values;
    ec.param_count = param_count;
    ec.param_meta = meta;
    ec.resp_buf = (uint8_t *)malloc(EXEC_RESP_BYTES);
    ec.batch_buf = (uint8_t *)malloc(EXEC_RESP_BYTES);
    ec.req_buf = (uint8_t *)malloc(EXEC_REQ_BYTES);
    ec.result_buf = (uint8_t *)malloc(PRDB_SDK_SQL_RESULT_MAX_BYTES);
    if (ec.resp_buf == NULL || ec.batch_buf == NULL || ec.req_buf == NULL ||
        ec.result_buf == NULL) {
        st = PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
        goto cleanup;
    }

    uint8_t bresp[PRDB_QUERY_WORKLOAD_BUDGETS_WIRE_BYTES] = {0};
    uint64_t brlen = 0;
    if (prdb_sdk_executor_get_workload_budgets_v1(
            host, bresp, sizeof(bresp), &brlen, &ec.budgets,
            sizeof(ec.budgets)) != PRDB_SDK_EXECUTOR_OK) {
        ec.budgets.sql_exec_max_scan_pages = 1000000;
    }

    if (stmt.kind == PRDB_SQL_STMT_SELECT) {
        mat_table out_t;
        mat_table_init(&out_t, 64 * 1024 * 1024);
        st = exec_select_to_table(&ec, stmt.u.select, NULL, 0, &out_t);
        if (st == PRDB_SDK_SQL_DISPATCH_OK) {
            uint32_t *ordinals = (uint32_t *)calloc(
                out_t.col_count == 0 ? 1 : out_t.col_count, sizeof(uint32_t));
            if (ordinals == NULL) {
                st = PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
            } else {
                for (uint32_t c = 0; c < out_t.col_count; ++c) ordinals[c] = c;
                for (uint32_t r = 0; r < out_t.row_count; ++r) {
                    uint64_t olen = 0;
                    if (prdb_sdk_sql_write_projected_result_v1(
                            host, out_t.cols, out_t.col_count, ordinals,
                            out_t.rows[r].cells, out_t.col_count, ec.result_buf,
                            PRDB_SDK_SQL_RESULT_MAX_BYTES, &olen) !=
                        PRDB_SDK_SQL_RESULT_OK) {
                        st = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
                        break;
                    }
                }
                free(ordinals);
            }
        }
        mat_table_free(&out_t);
    } else if (stmt.kind == PRDB_SQL_STMT_INSERT) {
        st = exec_insert_unified(&ec, stmt.u.insert);
    } else if (stmt.kind == PRDB_SQL_STMT_UPDATE) {
        st = exec_update_unified(&ec, stmt.u.update);
    } else if (stmt.kind == PRDB_SQL_STMT_DELETE) {
        st = exec_delete_unified(&ec, stmt.u.del);
    }

cleanup:
    free(ec.ctes);
    free(ec.resp_buf);
    free(ec.batch_buf);
    free(ec.req_buf);
    free(ec.result_buf);
    free(values);
    free(payloads);
    free(meta);
    prdb_sql_arena_free(&ast_arena);
    if (st != PRDB_SDK_SQL_DISPATCH_OK) {
        host->statement_failure(host->context, (uint32_t)st);
    }
    return st;
}
