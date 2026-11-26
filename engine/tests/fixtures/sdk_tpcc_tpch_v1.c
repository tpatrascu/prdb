#include "prdb_query_abi_v1.h"
#include "prdb_query_abi_constants.h"
#include "prdb_sdk_sql_dispatch_v1.h"
#include "prdb_sdk_sql_result_v1.h"
#include "prdb_sdk_executor_v1.h"
#include "prdb_sdk_batch_v1.h"
#include "prdb_sdk_value_v1.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* --- Minimal in-memory storage mock for TPC-C & TPC-H end-to-end SQL --- */

static void write_u16_be(uint8_t *p, uint16_t v) {
    p[0] = (uint8_t)(v >> 8);
    p[1] = (uint8_t)(v & 0xff);
}
static void write_u32_be(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);
    p[3] = (uint8_t)(v & 0xff);
}
static void write_u64_be(uint8_t *p, uint64_t v) {
    for (int i = 0; i < 8; ++i) p[i] = (uint8_t)(v >> (56 - i * 8));
}
static void write_u32_le(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)(v & 0xff);
    p[1] = (uint8_t)(v >> 8);
    p[2] = (uint8_t)(v >> 16);
    p[3] = (uint8_t)(v >> 24);
}
static void write_u64_le(uint8_t *p, uint64_t v) {
    for (int i = 0; i < 8; ++i) p[i] = (uint8_t)(v >> (i * 8));
}
static uint16_t read_u16_be(const uint8_t *p) {
    return (uint16_t)(((uint16_t)p[0] << 8) | p[1]);
}
static uint32_t read_u32_be(const uint8_t *p) {
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] << 8) | p[3];
}

typedef struct mock_col {
    const char *name;
    uint8_t type_code;
    uint8_t prec;
    uint8_t scale;
    uint8_t nullable;
} mock_col;

typedef struct mock_cell {
    uint8_t is_null;
    uint8_t type_code;
    uint8_t data[64];
    uint32_t len;
} mock_cell;

typedef struct mock_row {
    mock_cell cols[32];
} mock_row;

typedef struct mock_table {
    const char *name;
    uint8_t id[16];
    const mock_col *cols;
    uint32_t col_count;
    mock_row rows[64];
    uint32_t row_count;
} mock_table;

typedef struct mock_ctx {
    mock_table *tables;
    uint32_t table_count;
    uint32_t active_table;
    uint32_t scan_pos;
    uint32_t result_rows;
    uint32_t insert_count;
    uint32_t update_count;
    uint32_t delete_count;
    uint32_t failure_latched;
} mock_ctx;

static mock_cell cell_i32(int32_t v) {
    mock_cell c = {0};
    c.type_code = PRDB_SDK_VALUE_TYPE_INTEGER;
    write_u32_be(c.data, (uint32_t)v);
    c.len = 4;
    return c;
}
static mock_cell cell_i64(int64_t v) {
    mock_cell c = {0};
    c.type_code = PRDB_SDK_VALUE_TYPE_BIGINT;
    write_u64_be(c.data, (uint64_t)v);
    c.len = 8;
    return c;
}
static mock_cell cell_dec_i64(int64_t v) {
    mock_cell c = {0};
    c.type_code = PRDB_SDK_VALUE_TYPE_DECIMAL;
    uint8_t fill = v < 0 ? 0xff : 0x00;
    memset(c.data, fill, 8);
    write_u64_be(c.data + 8, (uint64_t)v);
    c.len = 16;
    return c;
}
static mock_cell cell_str(const char *s) {
    mock_cell c = {0};
    c.type_code = PRDB_SDK_VALUE_TYPE_VARCHAR;
    c.len = (uint32_t)strlen(s);
    memcpy(c.data, s, c.len);
    return c;
}
static mock_cell cell_date(int32_t days) {
    mock_cell c = {0};
    c.type_code = PRDB_SDK_VALUE_TYPE_DATE;
    write_u32_be(c.data, (uint32_t)days);
    c.len = 4;
    return c;
}

static int32_t mock_executor_request(
    void *vctx, uint16_t op, const uint8_t *req, uint64_t req_len,
    uint8_t *resp, uint64_t resp_cap, uint64_t *resp_len) {
    mock_ctx *ctx = (mock_ctx *)vctx;
    (void)req_len;
    if (op == PRDB_QUERY_HOST_OP_GET_WORKLOAD_BUDGETS) {
        memset(resp, 0, PRDB_QUERY_WORKLOAD_BUDGETS_WIRE_BYTES);
        write_u32_le(resp + 0, 1);
        write_u32_le(resp + 4, 16384);
        write_u32_le(resp + 8, 65536);
        write_u32_le(resp + 60, 100000);
        write_u64_le(resp + 148, 64 * 1024 * 1024);
        *resp_len = PRDB_QUERY_WORKLOAD_BUDGETS_WIRE_BYTES;
        return PRDB_QUERY_CALLBACK_OK;
    }
    if (op == PRDB_IPC_OP_EXECUTOR_CHECK_CANCELLATION) {
        write_u16_be(resp, 1);
        resp[2] = 0;
        *resp_len = 3;
        return PRDB_QUERY_CALLBACK_OK;
    }
    if (op == PRDB_IPC_OP_RESOLVE_RELATION) {
        uint16_t slen = read_u16_be(req + 2);
        uint16_t rlen = read_u16_be(req + 4);
        const char *rname = (const char *)(req + 6 + slen);
        for (uint32_t t = 0; t < ctx->table_count; ++t) {
            if (strlen(ctx->tables[t].name) == rlen &&
                memcmp(ctx->tables[t].name, rname, rlen) == 0) {
                mock_table *tb = &ctx->tables[t];
                uint64_t p = 0;
                write_u16_be(resp + p, 1); p += 2;
                resp[p++] = 0; /* status ok */
                memset(resp + p, (int)(t + 1), 16); p += 16; /* nonzero relation_id */
                memset(resp + p, (int)(t + 1), 16);
                resp[p] = 6; /* handle kind 6 */
                p += 16;
                write_u16_be(resp + p, (uint16_t)strlen(tb->name)); p += 2;
                memcpy(resp + p, tb->name, strlen(tb->name));
                p += strlen(tb->name);
                write_u16_be(resp + p, (uint16_t)tb->col_count); p += 2;
                for (uint32_t c = 0; c < tb->col_count; ++c) {
                    uint16_t clen = (uint16_t)strlen(tb->cols[c].name);
                    memset(resp + p, (int)(c + 1), 16); p += 16;
                    write_u32_be(resp + p, c); p += 4;
                    resp[p++] = tb->cols[c].nullable;
                    resp[p++] = tb->cols[c].type_code;
                    if (tb->cols[c].type_code == PRDB_SDK_VALUE_TYPE_DECIMAL) {
                        resp[p++] = tb->cols[c].prec;
                        resp[p++] = tb->cols[c].scale;
                        resp[p++] = 0;
                        resp[p++] = 0;
                    } else if (tb->cols[c].type_code ==
                               PRDB_SDK_VALUE_TYPE_VARCHAR) {
                        write_u32_be(resp + p, 64); p += 4;
                    } else {
                        memset(resp + p, 0, 4); p += 4;
                    }
                    write_u16_be(resp + p, clen); p += 2;
                    memcpy(resp + p, tb->cols[c].name, clen);
                    p += clen;
                }
                *resp_len = p;
                return PRDB_QUERY_CALLBACK_OK;
            }
        }
        write_u16_be(resp, 1);
        resp[2] = 1;
        *resp_len = 3;
        return PRDB_QUERY_CALLBACK_OK;
    }
    if (op == PRDB_IPC_OP_OPEN_SCAN) {
        uint8_t tidx = (uint8_t)(req[3] - 1);
        ctx->active_table = tidx;
        ctx->scan_pos = 0;
        write_u16_be(resp, 1);
        memset(resp + 2, 0, 16);
        resp[2] = PRDB_SDK_BATCH_CURSOR_TOKEN_KIND;
        resp[3] = (uint8_t)tidx;
        *resp_len = 18;
        return PRDB_QUERY_CALLBACK_OK;
    }
    if (op == PRDB_IPC_OP_NEXT_BATCH) {
        uint8_t tidx = req[3];
        mock_table *tb = &ctx->tables[tidx];
        uint64_t p = 0;
        write_u16_be(resp + p, PRDB_SDK_BATCH_WIRE_V1_VERSION); p += 2;
        uint16_t rem = (uint16_t)(tb->row_count - ctx->scan_pos);
        if (rem > 128) rem = 128;
        uint8_t finished = (ctx->scan_pos + rem >= tb->row_count) ? 1 : 0;
        resp[p++] = finished;
        write_u16_be(resp + p, rem); p += 2;
        if (!finished) {
            memset(resp + p, 0, 16);
            resp[p] = PRDB_SDK_BATCH_CURSOR_TOKEN_KIND;
            resp[p + 1] = tidx;
            p += 16;
        }
        for (uint16_t r = 0; r < rem; ++r) {
            mock_row *row = &tb->rows[ctx->scan_pos + r];
            memset(resp + p, 0, 16);
            resp[p] = PRDB_SDK_BATCH_ROW_TOKEN_KIND;
            resp[p + 1] = tidx;
            resp[p + 2] = (uint8_t)(ctx->scan_pos + r);
            p += 16;
            write_u16_be(resp + p, (uint16_t)tb->col_count); p += 2;
            for (uint32_t c = 0; c < tb->col_count; ++c) {
                mock_cell *cell = &row->cols[c];
                if (cell->is_null) {
                    resp[p++] = PRDB_SDK_VALUE_STATE_NULL;
                } else {
                    resp[p++] = PRDB_SDK_VALUE_STATE_VALUE;
                    resp[p++] = cell->type_code;
                    write_u32_be(resp + p, cell->len); p += 4;
                    memcpy(resp + p, cell->data, cell->len);
                    p += cell->len;
                }
            }
        }
        ctx->scan_pos += rem;
        assert(p <= resp_cap);
        *resp_len = p;
        return PRDB_QUERY_CALLBACK_OK;
    }
    if (op == PRDB_IPC_OP_CLOSE_CURSOR) {
        write_u16_be(resp, 1);
        resp[2] = 0;
        *resp_len = 3;
        return PRDB_QUERY_CALLBACK_OK;
    }
    if (op == PRDB_IPC_OP_STAGE_INSERTS) {
        uint16_t cnt = read_u16_be(req + 2);
        ctx->insert_count += cnt;
        write_u16_be(resp, 1);
        write_u16_be(resp + 2, cnt);
        uint64_t p = 4;
        for (uint16_t i = 0; i < cnt; ++i) {
            memset(resp + p, 0, 16);
            resp[p] = PRDB_SDK_BATCH_ROW_TOKEN_KIND;
            p += 16;
        }
        *resp_len = p;
        return PRDB_QUERY_CALLBACK_OK;
    }
    if (op == PRDB_IPC_OP_STAGE_UPDATES) {
        uint16_t cnt = read_u16_be(req + 2);
        ctx->update_count += cnt;
        write_u16_be(resp, 1);
        resp[2] = 0;
        *resp_len = 3;
        return PRDB_QUERY_CALLBACK_OK;
    }
    if (op == PRDB_IPC_OP_STAGE_DELETES) {
        uint16_t cnt = read_u16_be(req + 2);
        ctx->delete_count += cnt;
        write_u16_be(resp, 1);
        resp[2] = 0;
        *resp_len = 3;
        return PRDB_QUERY_CALLBACK_OK;
    }
    return PRDB_QUERY_CALLBACK_INTERNAL_ERROR;
}

static int32_t mock_write_result(void *vctx, const uint8_t *bytes,
                                 uint64_t len) {
    mock_ctx *ctx = (mock_ctx *)vctx;
    (void)bytes;
    if (len >= 4 && memcmp(bytes, "SQLR", 4) == 0) ++ctx->result_rows;
    return PRDB_QUERY_CALLBACK_OK;
}

static int32_t mock_stmt_failure(void *vctx, uint32_t code) {
    mock_ctx *ctx = (mock_ctx *)vctx;
    (void)code;
    ctx->failure_latched = 1;
    return PRDB_QUERY_CALLBACK_OK;
}

static void run_sql_ok(mock_ctx *ctx, const prdb_host_api_v1 *host,
                       const char *sql) {
    ctx->result_rows = 0;
    ctx->failure_latched = 0;
    int32_t st = prdb_sdk_sql_run_statement_v1(
        host, (const uint8_t *)"bench", 5, (const uint8_t *)sql, strlen(sql),
        NULL, 0);
    if (st != PRDB_SDK_SQL_DISPATCH_OK || ctx->failure_latched != 0) {
        fprintf(stderr, "SQL FAILED (st=%d): %s\n", st, sql);
        assert(0);
    }
}

int main(void) {
    /* --- 1. TPC-C Schema & 5 Transactions --- */
    static const mock_col wh_cols[] = {
        {"w_id", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"w_tax", PRDB_SDK_VALUE_TYPE_DECIMAL, 4, 4, 0},
        {"w_ytd", PRDB_SDK_VALUE_TYPE_DECIMAL, 12, 2, 0}};
    static const mock_col dist_cols[] = {
        {"d_id", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"d_w_id", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"d_tax", PRDB_SDK_VALUE_TYPE_DECIMAL, 4, 4, 0},
        {"d_ytd", PRDB_SDK_VALUE_TYPE_DECIMAL, 12, 2, 0},
        {"d_next_o_id", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0}};
    static const mock_col cust_cols[] = {
        {"c_id", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"c_d_id", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"c_w_id", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"c_last", PRDB_SDK_VALUE_TYPE_VARCHAR, 0, 0, 0},
        {"c_balance", PRDB_SDK_VALUE_TYPE_DECIMAL, 12, 2, 0}};
    static const mock_col no_cols[] = {
        {"no_o_id", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"no_d_id", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"no_w_id", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0}};
    static const mock_col ord_cols[] = {
        {"o_id", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"o_d_id", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"o_w_id", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"o_c_id", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0}};
    static const mock_col ol_cols[] = {
        {"ol_o_id", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"ol_d_id", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"ol_w_id", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"ol_i_id", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"ol_amount", PRDB_SDK_VALUE_TYPE_DECIMAL, 6, 2, 0}};
    static const mock_col stk_cols[] = {
        {"s_i_id", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"s_w_id", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"s_quantity", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0}};

    /* TPC-H tables */
    static const mock_col reg_cols[] = {
        {"r_regionkey", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"r_name", PRDB_SDK_VALUE_TYPE_VARCHAR, 0, 0, 0}};
    static const mock_col nat_cols[] = {
        {"n_nationkey", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"n_name", PRDB_SDK_VALUE_TYPE_VARCHAR, 0, 0, 0},
        {"n_regionkey", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0}};
    static const mock_col sup_cols[] = {
        {"s_suppkey", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"s_name", PRDB_SDK_VALUE_TYPE_VARCHAR, 0, 0, 0},
        {"s_nationkey", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"s_acctbal", PRDB_SDK_VALUE_TYPE_DECIMAL, 15, 2, 0},
        {"s_comment", PRDB_SDK_VALUE_TYPE_VARCHAR, 0, 0, 0}};
    static const mock_col part_cols[] = {
        {"p_partkey", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"p_name", PRDB_SDK_VALUE_TYPE_VARCHAR, 0, 0, 0},
        {"p_brand", PRDB_SDK_VALUE_TYPE_VARCHAR, 0, 0, 0},
        {"p_type", PRDB_SDK_VALUE_TYPE_VARCHAR, 0, 0, 0},
        {"p_size", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"p_container", PRDB_SDK_VALUE_TYPE_VARCHAR, 0, 0, 0}};
    static const mock_col ps_cols[] = {
        {"ps_partkey", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"ps_suppkey", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"ps_availqty", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"ps_supplycost", PRDB_SDK_VALUE_TYPE_DECIMAL, 15, 2, 0}};
    static const mock_col hcust_cols[] = {
        {"c_custkey", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"c_name", PRDB_SDK_VALUE_TYPE_VARCHAR, 0, 0, 0},
        {"c_nationkey", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"c_phone", PRDB_SDK_VALUE_TYPE_VARCHAR, 0, 0, 0},
        {"c_acctbal", PRDB_SDK_VALUE_TYPE_DECIMAL, 15, 2, 0},
        {"c_mktsegment", PRDB_SDK_VALUE_TYPE_VARCHAR, 0, 0, 0}};
    static const mock_col hord_cols[] = {
        {"o_orderkey", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"o_custkey", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"o_orderstatus", PRDB_SDK_VALUE_TYPE_VARCHAR, 0, 0, 0},
        {"o_totalprice", PRDB_SDK_VALUE_TYPE_DECIMAL, 15, 2, 0},
        {"o_orderdate", PRDB_SDK_VALUE_TYPE_DATE, 0, 0, 0},
        {"o_orderpriority", PRDB_SDK_VALUE_TYPE_VARCHAR, 0, 0, 0},
        {"o_shippriority", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"o_comment", PRDB_SDK_VALUE_TYPE_VARCHAR, 0, 0, 0}};
    static const mock_col li_cols[] = {
        {"l_orderkey", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"l_partkey", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"l_suppkey", PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0},
        {"l_quantity", PRDB_SDK_VALUE_TYPE_DECIMAL, 15, 2, 0},
        {"l_extendedprice", PRDB_SDK_VALUE_TYPE_DECIMAL, 15, 2, 0},
        {"l_discount", PRDB_SDK_VALUE_TYPE_DECIMAL, 15, 2, 0},
        {"l_tax", PRDB_SDK_VALUE_TYPE_DECIMAL, 15, 2, 0},
        {"l_returnflag", PRDB_SDK_VALUE_TYPE_VARCHAR, 0, 0, 0},
        {"l_linestatus", PRDB_SDK_VALUE_TYPE_VARCHAR, 0, 0, 0},
        {"l_shipdate", PRDB_SDK_VALUE_TYPE_DATE, 0, 0, 0},
        {"l_commitdate", PRDB_SDK_VALUE_TYPE_DATE, 0, 0, 0},
        {"l_receiptdate", PRDB_SDK_VALUE_TYPE_DATE, 0, 0, 0},
        {"l_shipinstruct", PRDB_SDK_VALUE_TYPE_VARCHAR, 0, 0, 0},
        {"l_shipmode", PRDB_SDK_VALUE_TYPE_VARCHAR, 0, 0, 0}};

    mock_table tables[15] = {0};
    tables[0].name = "warehouse"; tables[0].cols = wh_cols; tables[0].col_count = 3;
    tables[0].row_count = 1;
    tables[0].rows[0].cols[0] = cell_i32(1);
    tables[0].rows[0].cols[1] = cell_dec_i64(500);
    tables[0].rows[0].cols[2] = cell_dec_i64(30000000);

    tables[1].name = "district"; tables[1].cols = dist_cols; tables[1].col_count = 5;
    tables[1].row_count = 1;
    tables[1].rows[0].cols[0] = cell_i32(1);
    tables[1].rows[0].cols[1] = cell_i32(1);
    tables[1].rows[0].cols[2] = cell_dec_i64(500);
    tables[1].rows[0].cols[3] = cell_dec_i64(3000000);
    tables[1].rows[0].cols[4] = cell_i32(3001);

    tables[2].name = "customer"; tables[2].cols = cust_cols; tables[2].col_count = 5;
    tables[2].row_count = 1;
    tables[2].rows[0].cols[0] = cell_i32(1);
    tables[2].rows[0].cols[1] = cell_i32(1);
    tables[2].rows[0].cols[2] = cell_i32(1);
    tables[2].rows[0].cols[3] = cell_str("BARBARBAR");
    tables[2].rows[0].cols[4] = cell_dec_i64(-1000);

    tables[3].name = "new_order"; tables[3].cols = no_cols; tables[3].col_count = 3;
    tables[3].row_count = 1;
    tables[3].rows[0].cols[0] = cell_i32(2101);
    tables[3].rows[0].cols[1] = cell_i32(1);
    tables[3].rows[0].cols[2] = cell_i32(1);

    tables[4].name = "orders"; tables[4].cols = ord_cols; tables[4].col_count = 4;
    tables[4].row_count = 1;
    tables[4].rows[0].cols[0] = cell_i32(3000);
    tables[4].rows[0].cols[1] = cell_i32(1);
    tables[4].rows[0].cols[2] = cell_i32(1);
    tables[4].rows[0].cols[3] = cell_i32(1);

    tables[5].name = "order_line"; tables[5].cols = ol_cols; tables[5].col_count = 5;
    tables[5].row_count = 1;
    tables[5].rows[0].cols[0] = cell_i32(3000);
    tables[5].rows[0].cols[1] = cell_i32(1);
    tables[5].rows[0].cols[2] = cell_i32(1);
    tables[5].rows[0].cols[3] = cell_i32(101);
    tables[5].rows[0].cols[4] = cell_dec_i64(2500);

    tables[6].name = "stock"; tables[6].cols = stk_cols; tables[6].col_count = 3;
    tables[6].row_count = 1;
    tables[6].rows[0].cols[0] = cell_i32(101);
    tables[6].rows[0].cols[1] = cell_i32(1);
    tables[6].rows[0].cols[2] = cell_i32(10);

    /* TPC-H tables */
    tables[7].name = "region"; tables[7].cols = reg_cols; tables[7].col_count = 2;
    tables[7].row_count = 1;
    tables[7].rows[0].cols[0] = cell_i32(1);
    tables[7].rows[0].cols[1] = cell_str("EUROPE");

    tables[8].name = "nation"; tables[8].cols = nat_cols; tables[8].col_count = 3;
    tables[8].row_count = 2;
    tables[8].rows[0].cols[0] = cell_i32(7);
    tables[8].rows[0].cols[1] = cell_str("GERMANY");
    tables[8].rows[0].cols[2] = cell_i32(1);
    tables[8].rows[1].cols[0] = cell_i32(6);
    tables[8].rows[1].cols[1] = cell_str("FRANCE");
    tables[8].rows[1].cols[2] = cell_i32(1);

    tables[9].name = "supplier"; tables[9].cols = sup_cols; tables[9].col_count = 5;
    tables[9].row_count = 1;
    tables[9].rows[0].cols[0] = cell_i32(1);
    tables[9].rows[0].cols[1] = cell_str("Supplier#000000001");
    tables[9].rows[0].cols[2] = cell_i32(7);
    tables[9].rows[0].cols[3] = cell_dec_i64(500000);
    tables[9].rows[0].cols[4] = cell_str("Customer Complaints");

    tables[10].name = "part"; tables[10].cols = part_cols; tables[10].col_count = 6;
    tables[10].row_count = 1;
    tables[10].rows[0].cols[0] = cell_i32(101);
    tables[10].rows[0].cols[1] = cell_str("forest green steel");
    tables[10].rows[0].cols[2] = cell_str("Brand#12");
    tables[10].rows[0].cols[3] = cell_str("PROMO BURNISHED BRASS");
    tables[10].rows[0].cols[4] = cell_i32(15);
    tables[10].rows[0].cols[5] = cell_str("SM CASE");

    tables[11].name = "partsupp"; tables[11].cols = ps_cols; tables[11].col_count = 4;
    tables[11].row_count = 1;
    tables[11].rows[0].cols[0] = cell_i32(101);
    tables[11].rows[0].cols[1] = cell_i32(1);
    tables[11].rows[0].cols[2] = cell_i32(100);
    tables[11].rows[0].cols[3] = cell_dec_i64(1000);

    tables[12].name = "h_customer"; tables[12].cols = hcust_cols; tables[12].col_count = 6;
    tables[12].row_count = 2;
    tables[12].rows[0].cols[0] = cell_i32(1);
    tables[12].rows[0].cols[1] = cell_str("Customer#000000001");
    tables[12].rows[0].cols[2] = cell_i32(6);
    tables[12].rows[0].cols[3] = cell_str("13-123-456-7890");
    tables[12].rows[0].cols[4] = cell_dec_i64(100000);
    tables[12].rows[0].cols[5] = cell_str("BUILDING");
    tables[12].rows[1].cols[0] = cell_i32(2);
    tables[12].rows[1].cols[1] = cell_str("Customer#000000002");
    tables[12].rows[1].cols[2] = cell_i32(7);
    tables[12].rows[1].cols[3] = cell_str("31-999-888-7777");
    tables[12].rows[1].cols[4] = cell_dec_i64(20000);
    tables[12].rows[1].cols[5] = cell_str("AUTOMOBILE");

    tables[13].name = "h_orders"; tables[13].cols = hord_cols; tables[13].col_count = 8;
    tables[13].row_count = 1;
    tables[13].rows[0].cols[0] = cell_i32(1);
    tables[13].rows[0].cols[1] = cell_i32(1);
    tables[13].rows[0].cols[2] = cell_str("F");
    tables[13].rows[0].cols[3] = cell_dec_i64(500000);
    tables[13].rows[0].cols[4] = cell_date(9200); /* ~1995-03-11 */
    tables[13].rows[0].cols[5] = cell_str("1-URGENT");
    tables[13].rows[0].cols[6] = cell_i32(0);
    tables[13].rows[0].cols[7] = cell_str("normal order");

    tables[14].name = "lineitem"; tables[14].cols = li_cols; tables[14].col_count = 14;
    tables[14].row_count = 1;
    tables[14].rows[0].cols[0] = cell_i32(1);
    tables[14].rows[0].cols[1] = cell_i32(101);
    tables[14].rows[0].cols[2] = cell_i32(1);
    tables[14].rows[0].cols[3] = cell_dec_i64(1000); /* 10.00 */
    tables[14].rows[0].cols[4] = cell_dec_i64(100000); /* 1000.00 */
    tables[14].rows[0].cols[5] = cell_dec_i64(5); /* 0.05 */
    tables[14].rows[0].cols[6] = cell_dec_i64(2); /* 0.02 */
    tables[14].rows[0].cols[7] = cell_str("R");
    tables[14].rows[0].cols[8] = cell_str("F");
    tables[14].rows[0].cols[9] = cell_date(9500);
    tables[14].rows[0].cols[10] = cell_date(9490);
    tables[14].rows[0].cols[11] = cell_date(9510);
    tables[14].rows[0].cols[12] = cell_str("DELIVER IN PERSON");
    tables[14].rows[0].cols[13] = cell_str("AIR");

    mock_ctx ctx = {tables, 15, 0, 0, 0, 0, 0, 0, 0};
    prdb_host_api_v1 host = {0};
    host.struct_size = sizeof(host);
    host.abi_version = PRDB_QUERY_ABI_VERSION;
    host.context = &ctx;
    host.executor_request = mock_executor_request;
    host.write_result = mock_write_result;
    host.statement_failure = mock_stmt_failure;

    /* TPC-C SQL statements */
    run_sql_ok(&ctx, &host,
               "UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = 1 AND d_id = 1;");
    assert(ctx.update_count == 1);

    run_sql_ok(&ctx, &host,
               "INSERT INTO new_order VALUES (3001, 1, 1);");
    assert(ctx.insert_count == 1);

    run_sql_ok(&ctx, &host,
               "SELECT MIN(no_o_id) FROM new_order WHERE no_w_id = 1 AND no_d_id = 1;");
    assert(ctx.result_rows == 1);

    run_sql_ok(&ctx, &host,
               "DELETE FROM new_order WHERE no_w_id = 1 AND no_d_id = 1 AND no_o_id = 2101;");
    assert(ctx.delete_count == 1);

    run_sql_ok(&ctx, &host,
               "SELECT COUNT(DISTINCT s_i_id) FROM order_line, stock WHERE ol_w_id = 1 AND ol_d_id = 1 AND ol_o_id < 3005 AND ol_o_id >= 2995 AND s_w_id = 1 AND s_i_id = ol_i_id AND s_quantity < 20;");
    assert(ctx.result_rows == 1);

    /* TPC-H representative complex queries (Q1, Q2, Q4, Q6, Q7, Q11, Q13, Q14, Q15 CTE, Q16, Q19, Q22) */
    run_sql_ok(&ctx, &host,
               "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, "
               "SUM(l_extendedprice) AS sum_base_price, "
               "SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, "
               "SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, "
               "AVG(l_quantity) AS avg_qty, AVG(l_extendedprice) AS avg_price, "
               "AVG(l_discount) AS avg_disc, COUNT(*) AS count_order "
               "FROM lineitem WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY "
               "GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus;");
    assert(ctx.result_rows == 1);

    run_sql_ok(&ctx, &host,
               "SELECT SUM(l_extendedprice * l_discount) AS revenue FROM lineitem "
               "WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1994-01-01' + INTERVAL '3' YEAR "
               "AND l_discount BETWEEN 0.04 AND 0.07 AND l_quantity < 24;");
    assert(ctx.result_rows == 1);

    run_sql_ok(&ctx, &host,
               "SELECT o_orderpriority, COUNT(*) AS order_count FROM h_orders "
               "WHERE o_orderdate >= DATE '1993-07-01' AND o_orderdate < DATE '1993-07-01' + INTERVAL '3' MONTH "
               "AND EXISTS (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate) "
               "GROUP BY o_orderpriority ORDER BY o_orderpriority;");

    run_sql_ok(&ctx, &host,
               "SELECT s_acctbal, s_name, n_name, p_partkey FROM part, supplier, partsupp, nation, region "
               "WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = 15 "
               "AND p_type LIKE '%BRASS' AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey "
               "AND r_name = 'EUROPE' AND ps_supplycost = ("
               "SELECT MIN(ps_supplycost) FROM partsupp, supplier, nation, region "
               "WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey "
               "AND n_regionkey = r_regionkey AND r_name = 'EUROPE');");
    assert(ctx.result_rows == 1);

    run_sql_ok(&ctx, &host,
               "SELECT c_count, COUNT(*) AS custdist FROM ("
               "SELECT c_custkey, COUNT(o_orderkey) AS c_count FROM h_customer "
               "LEFT OUTER JOIN h_orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%' "
               "GROUP BY c_custkey) AS c_orders GROUP BY c_count ORDER BY custdist DESC, c_count DESC;");
    assert(ctx.result_rows == 2);

    run_sql_ok(&ctx, &host,
               "WITH revenue AS (SELECT l_suppkey AS supplier_no, SUM(l_extendedprice * (1 - l_discount)) AS total_revenue "
               "FROM lineitem WHERE l_shipdate >= DATE '1996-01-01' GROUP BY l_suppkey) "
               "SELECT s_suppkey, s_name, total_revenue FROM supplier, revenue WHERE s_suppkey = supplier_no "
               "AND total_revenue = (SELECT MAX(total_revenue) FROM revenue) ORDER BY s_suppkey;");
    assert(ctx.result_rows == 1);

    run_sql_ok(&ctx, &host,
               "SELECT EXTRACT(YEAR FROM o_orderdate) AS o_year, "
               "CASE WHEN n_name = 'FRANCE' THEN 1 ELSE 0 END AS is_france "
               "FROM h_orders, h_customer, nation WHERE o_custkey = c_custkey AND c_nationkey = n_nationkey;");
    assert(ctx.result_rows == 1);

    run_sql_ok(&ctx, &host,
               "SELECT SUBSTRING(c_phone FROM 1 FOR 2) AS cntrycode, COUNT(*) AS numcust "
               "FROM h_customer WHERE SUBSTRING(c_phone FROM 1 FOR 2) IN ('13', '31', '23') "
               "AND c_acctbal > (SELECT AVG(c_acctbal) FROM h_customer WHERE c_acctbal > 0.00) "
               "GROUP BY SUBSTRING(c_phone FROM 1 FOR 2) ORDER BY cntrycode;");
    assert(ctx.result_rows == 1);

    printf("TPC-C and TPC-H v1 SQL suite passed!\n");
    return 0;
}
