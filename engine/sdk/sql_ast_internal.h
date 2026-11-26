#ifndef PRDB_SDK_SQL_AST_INTERNAL_H
#define PRDB_SDK_SQL_AST_INTERNAL_H

#include "prdb_sdk_sql_v1.h"
#include "prdb_sdk_executor_v1.h"
#include "prdb_sdk_value_v1.h"

#include <stddef.h>
#include <stdint.h>

#ifndef PRDB_SDK_INTERNAL
#define PRDB_SDK_INTERNAL __attribute__((visibility("hidden")))
#endif

typedef struct prdb_sql_arena_chunk {
    struct prdb_sql_arena_chunk *next;
    size_t capacity;
    size_t used;
    uint8_t data[];
} prdb_sql_arena_chunk;

typedef struct prdb_sql_arena {
    prdb_sql_arena_chunk *head;
    size_t total_allocated;
    size_t max_bytes;
} prdb_sql_arena;

PRDB_SDK_INTERNAL void prdb_sql_arena_init(prdb_sql_arena *arena,
                                           size_t max_bytes);
PRDB_SDK_INTERNAL void *prdb_sql_arena_alloc(prdb_sql_arena *arena,
                                             size_t size, size_t align);
PRDB_SDK_INTERNAL char *prdb_sql_arena_strdup(prdb_sql_arena *arena,
                                              const uint8_t *bytes,
                                              size_t len);
PRDB_SDK_INTERNAL void prdb_sql_arena_free(prdb_sql_arena *arena);

typedef enum prdb_sql_expr_kind {
    PRDB_SQL_EXPR_NULL = 1,
    PRDB_SQL_EXPR_BOOL,
    PRDB_SQL_EXPR_INT,
    PRDB_SQL_EXPR_DECIMAL,
    PRDB_SQL_EXPR_STRING,
    PRDB_SQL_EXPR_DATE,
    PRDB_SQL_EXPR_TIMESTAMP,
    PRDB_SQL_EXPR_INTERVAL,
    PRDB_SQL_EXPR_PARAMETER,
    PRDB_SQL_EXPR_COLUMN,
    PRDB_SQL_EXPR_UNARY,
    PRDB_SQL_EXPR_BINARY,
    PRDB_SQL_EXPR_BETWEEN,
    PRDB_SQL_EXPR_IN_LIST,
    PRDB_SQL_EXPR_CASE,
    PRDB_SQL_EXPR_EXTRACT,
    PRDB_SQL_EXPR_SUBSTRING,
    PRDB_SQL_EXPR_COALESCE,
    PRDB_SQL_EXPR_AGGREGATE,
    PRDB_SQL_EXPR_SUBQUERY
} prdb_sql_expr_kind;

typedef enum prdb_sql_unary_op {
    PRDB_SQL_UOP_NEG = 1,
    PRDB_SQL_UOP_POS,
    PRDB_SQL_UOP_NOT,
    PRDB_SQL_UOP_IS_NULL,
    PRDB_SQL_UOP_IS_NOT_NULL
} prdb_sql_unary_op;

typedef enum prdb_sql_bin_op {
    PRDB_SQL_BOP_ADD = 1,
    PRDB_SQL_BOP_SUB,
    PRDB_SQL_BOP_MUL,
    PRDB_SQL_BOP_DIV,
    PRDB_SQL_BOP_MOD,
    PRDB_SQL_BOP_EQ,
    PRDB_SQL_BOP_NE,
    PRDB_SQL_BOP_LT,
    PRDB_SQL_BOP_LE,
    PRDB_SQL_BOP_GT,
    PRDB_SQL_BOP_GE,
    PRDB_SQL_BOP_AND,
    PRDB_SQL_BOP_OR,
    PRDB_SQL_BOP_LIKE,
    PRDB_SQL_BOP_NOT_LIKE
} prdb_sql_bin_op;

typedef enum prdb_sql_interval_unit {
    PRDB_SQL_INTERVAL_DAY = 1,
    PRDB_SQL_INTERVAL_MONTH,
    PRDB_SQL_INTERVAL_YEAR
} prdb_sql_interval_unit;

typedef enum prdb_sql_subquery_kind {
    PRDB_SQL_SUBQ_SCALAR = 1,
    PRDB_SQL_SUBQ_EXISTS,
    PRDB_SQL_SUBQ_NOT_EXISTS,
    PRDB_SQL_SUBQ_IN,
    PRDB_SQL_SUBQ_NOT_IN
} prdb_sql_subquery_kind;

typedef struct prdb_sql_expr prdb_sql_expr;
typedef struct prdb_sql_select_ast prdb_sql_select_ast;

typedef struct prdb_sql_decimal_lit {
    uint8_t bytes[16]; /* signed big-endian two's complement */
    uint8_t precision;
    uint8_t scale;
} prdb_sql_decimal_lit;

struct prdb_sql_expr {
    prdb_sql_expr_kind kind;
    /* Bound type info (0 if unbound or NULL) */
    uint32_t bound_type;
    uint8_t bound_precision;
    uint8_t bound_scale;
    uint8_t is_bound;

    union {
        uint8_t bool_val;
        int64_t int_val;
        prdb_sql_decimal_lit dec_val;
        struct {
            const uint8_t *data;
            uint32_t len;
        } str_val;
        int32_t date_days;
        int64_t ts_micros;
        struct {
            int64_t amount;
            prdb_sql_interval_unit unit;
        } interval_val;
        uint32_t param_index;
        struct {
            const uint8_t *table_name;
            uint32_t table_name_len;
            const uint8_t *col_name;
            uint32_t col_name_len;
            /* Bound coordinates */
            uint32_t scope_depth; /* 0 = current query, >0 = outer query */
            uint32_t table_index;
            uint32_t col_ordinal;
        } col;
        struct {
            prdb_sql_unary_op op;
            prdb_sql_expr *operand;
        } unary;
        struct {
            prdb_sql_bin_op op;
            prdb_sql_expr *left;
            prdb_sql_expr *right;
        } binary;
        struct {
            prdb_sql_expr *expr;
            prdb_sql_expr *low;
            prdb_sql_expr *high;
            uint8_t negated;
        } between;
        struct {
            prdb_sql_expr *expr;
            prdb_sql_expr **items;
            uint32_t item_count;
            uint8_t negated;
        } in_list;
        struct {
            prdb_sql_expr *operand; /* NULL for searched CASE */
            prdb_sql_expr **when_exprs;
            prdb_sql_expr **then_exprs;
            uint32_t branch_count;
            prdb_sql_expr *else_expr; /* NULL if omitted */
        } case_expr;
        struct {
            prdb_sql_interval_unit field;
            prdb_sql_expr *arg;
        } extract;
        struct {
            prdb_sql_expr *str;
            prdb_sql_expr *start;
            prdb_sql_expr *len; /* NULL if omitted */
        } substring;
        struct {
            prdb_sql_expr **args;
            uint32_t arg_count;
        } coalesce;
        struct {
            uint32_t agg_kind; /* PRDB_SDK_SQL_AGGREGATE_* */
            uint8_t distinct;
            prdb_sql_expr *arg; /* NULL for COUNT(*) */
            uint32_t bound_agg_index;
        } agg;
        struct {
            prdb_sql_subquery_kind subq_kind;
            prdb_sql_expr *lhs; /* for IN / NOT IN */
            prdb_sql_select_ast *subquery;
        } subquery;
    } u;
};

typedef struct prdb_sql_select_item {
    prdb_sql_expr *expr;
    const uint8_t *alias;
    uint32_t alias_len;
    uint8_t is_star;
    const uint8_t *star_table;
    uint32_t star_table_len;
} prdb_sql_select_item;

typedef enum prdb_sql_table_ref_kind {
    PRDB_SQL_TREF_BASE = 1,
    PRDB_SQL_TREF_SUBQUERY,
    PRDB_SQL_TREF_JOIN
} prdb_sql_table_ref_kind;

typedef enum prdb_sql_join_kind {
    PRDB_SQL_JOIN_INNER = 1,
    PRDB_SQL_JOIN_LEFT_OUTER,
    PRDB_SQL_JOIN_CROSS
} prdb_sql_join_kind;

typedef struct prdb_sql_table_ref {
    prdb_sql_table_ref_kind kind;
    union {
        struct {
            const uint8_t *schema;
            uint32_t schema_len;
            const uint8_t *name;
            uint32_t name_len;
            const uint8_t *alias;
            uint32_t alias_len;
        } base;
        struct {
            prdb_sql_select_ast *subquery;
            const uint8_t *alias;
            uint32_t alias_len;
            const uint8_t **col_aliases;
            uint32_t *col_alias_lens;
            uint32_t col_alias_count;
        } subq;
        struct {
            prdb_sql_join_kind join_kind;
            struct prdb_sql_table_ref *left;
            struct prdb_sql_table_ref *right;
            prdb_sql_expr *on_cond;
        } join;
    } u;
} prdb_sql_table_ref;

typedef struct prdb_sql_cte {
    const uint8_t *name;
    uint32_t name_len;
    const uint8_t **col_names;
    uint32_t *col_name_lens;
    uint32_t col_name_count;
    prdb_sql_select_ast *query;
} prdb_sql_cte;

typedef struct prdb_sql_order_spec {
    prdb_sql_expr *expr;
    uint8_t descending;
    uint8_t nulls_first;
    uint8_t nulls_last;
} prdb_sql_order_spec;

struct prdb_sql_select_ast {
    prdb_sql_cte *ctes;
    uint32_t cte_count;
    uint8_t distinct;
    prdb_sql_select_item *items;
    uint32_t item_count;
    prdb_sql_table_ref *from;
    prdb_sql_expr *where_expr;
    prdb_sql_expr **group_by;
    uint32_t group_by_count;
    prdb_sql_expr *having_expr;
    prdb_sql_order_spec *order_by;
    uint32_t order_by_count;
    uint8_t has_limit;
    uint64_t limit;
    uint8_t has_offset;
    uint64_t offset;
};

typedef struct prdb_sql_insert_ast {
    const uint8_t *schema;
    uint32_t schema_len;
    const uint8_t *relation;
    uint32_t relation_len;
    const uint8_t **columns;
    uint32_t *column_lens;
    uint32_t column_count;
    prdb_sql_expr ***rows; /* rows[r][c], NULL expr pointer = DEFAULT */
    uint32_t row_count;
    uint32_t cols_per_row;
} prdb_sql_insert_ast;

typedef struct prdb_sql_update_assign {
    const uint8_t *column;
    uint32_t column_len;
    prdb_sql_expr *expr;
} prdb_sql_update_assign;

typedef struct prdb_sql_update_ast {
    const uint8_t *schema;
    uint32_t schema_len;
    const uint8_t *relation;
    uint32_t relation_len;
    prdb_sql_update_assign *assignments;
    uint32_t assignment_count;
    prdb_sql_expr *where_expr;
} prdb_sql_update_ast;

typedef struct prdb_sql_delete_ast {
    const uint8_t *schema;
    uint32_t schema_len;
    const uint8_t *relation;
    uint32_t relation_len;
    prdb_sql_expr *where_expr;
} prdb_sql_delete_ast;

typedef enum prdb_sql_stmt_kind {
    PRDB_SQL_STMT_SELECT = 1,
    PRDB_SQL_STMT_INSERT,
    PRDB_SQL_STMT_UPDATE,
    PRDB_SQL_STMT_DELETE
} prdb_sql_stmt_kind;

typedef struct prdb_sql_stmt_ast {
    prdb_sql_stmt_kind kind;
    union {
        prdb_sql_select_ast *select;
        prdb_sql_insert_ast *insert;
        prdb_sql_update_ast *update;
        prdb_sql_delete_ast *del;
    } u;
} prdb_sql_stmt_ast;

/* Parse any SQL statement (SELECT/WITH, INSERT, UPDATE, DELETE) into arena. */
PRDB_SDK_INTERNAL int32_t prdb_sql_parse_statement_v1(
    prdb_sql_arena *arena, const uint8_t *sql, uint64_t sql_len,
    prdb_sql_stmt_ast *out, uint32_t *error_offset_out);

/* Evaluate a bound scalar expression against row scopes. */
typedef struct prdb_sql_eval_scope {
    const prdb_sdk_sql_column_shape_v1 *columns;
    const prdb_sdk_value_v1 *values;
    uint32_t column_count;
} prdb_sql_eval_scope;

typedef struct prdb_sql_eval_row_set prdb_sql_eval_row_set;
typedef struct prdb_sql_owned_val prdb_sql_owned_val;

typedef int32_t (*prdb_sql_subquery_eval_fn)(
    void *subq_ctx, const prdb_sql_expr *subq_expr,
    const prdb_sql_eval_row_set *outer_ctx, prdb_sql_owned_val *out);

struct prdb_sql_eval_row_set {
    const prdb_sql_eval_scope *scopes; /* scopes[0] = current, 1.. = outer */
    uint32_t scope_count;
    const prdb_sdk_value_v1 *params;
    uint32_t param_count;
    const prdb_sdk_sql_parameter_metadata_v1 *param_meta;
    const prdb_sdk_value_v1 *agg_values; /* precomputed aggregate values */
    uint32_t agg_count;
    prdb_sql_subquery_eval_fn subq_eval;
    void *subq_ctx;
};

/* Owned value buffer for expression evaluation results. */
typedef struct prdb_sql_owned_val {
    prdb_sdk_value_v1 val;
    uint8_t inline_buf[32];
    uint8_t *heap_buf; /* NULL if inline_buf or borrowed */
} prdb_sql_owned_val;

PRDB_SDK_INTERNAL void prdb_sql_owned_val_free(prdb_sql_owned_val *v);

PRDB_SDK_INTERNAL int32_t prdb_sql_eval_expr_v1(
    const prdb_sql_expr *expr, const prdb_sql_eval_row_set *ctx,
    prdb_sql_owned_val *out);

PRDB_SDK_INTERNAL int32_t prdb_sql_eval_truth_v1(
    const prdb_sql_expr *expr, const prdb_sql_eval_row_set *ctx,
    uint32_t *truth_out);

/* Unified execution entry point used by v1 dispatchers. */
PRDB_SDK_INTERNAL int32_t prdb_sql_execute_unified_v1(
    const prdb_host_api_v1 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const uint8_t *parameters, uint64_t parameters_len);

#endif /* PRDB_SDK_SQL_AST_INTERNAL_H */
