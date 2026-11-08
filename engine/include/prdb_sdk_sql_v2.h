#ifndef PRDB_SDK_SQL_V2_H
#define PRDB_SDK_SQL_V2_H

#include "prdb_sdk_sql_v1.h"

#define PRDB_SDK_SQL_ABI_V2_VERSION UINT32_C(2)
#define PRDB_SDK_SQL_MAX_PREDICATE_NODES UINT32_C(64)
#define PRDB_SDK_SQL_MAX_EXPRESSION_DEPTH UINT32_C(32)
#define PRDB_SDK_SQL_MAX_RELATION_COLUMNS UINT32_C(4096)

#define PRDB_SDK_SQL_BIND_OK INT32_C(0)
#define PRDB_SDK_SQL_BIND_INVALID_ARGUMENT INT32_C(1)
#define PRDB_SDK_SQL_BIND_INVALID_STATEMENT INT32_C(2)
#define PRDB_SDK_SQL_BIND_RELATION_MISMATCH INT32_C(3)
#define PRDB_SDK_SQL_BIND_COLUMN_NOT_FOUND INT32_C(4)
#define PRDB_SDK_SQL_BIND_AMBIGUOUS_COLUMN INT32_C(5)
#define PRDB_SDK_SQL_BIND_INVALID_SCHEMA INT32_C(6)

#define PRDB_SDK_SQL_PREDICATE_COMPARE UINT32_C(1)
#define PRDB_SDK_SQL_PREDICATE_IS_NULL UINT32_C(2)
#define PRDB_SDK_SQL_PREDICATE_IS_NOT_NULL UINT32_C(3)
#define PRDB_SDK_SQL_PREDICATE_AND UINT32_C(4)
#define PRDB_SDK_SQL_PREDICATE_OR UINT32_C(5)
#define PRDB_SDK_SQL_PREDICATE_NOT UINT32_C(6)

typedef struct prdb_sdk_predicate_node_v2 {
    uint32_t node_kind;
    uint32_t left_child;
    uint32_t right_child;
    uint32_t comparison;
    prdb_sdk_sql_span_v1 column;
    uint32_t parameter_index;
} prdb_sdk_predicate_node_v2;

/* Borrowed column metadata from one authorized relation description. */
typedef struct prdb_sdk_sql_column_shape_v2 {
    const uint8_t *name;
    uint32_t name_len;
    uint32_t ordinal;
    uint32_t type_code;
    uint8_t decimal_precision;
    uint8_t decimal_scale;
} prdb_sdk_sql_column_shape_v2;

typedef struct prdb_sdk_sql_bound_column_v2 {
    uint32_t ordinal;
    uint32_t type_code;
    uint8_t decimal_precision;
    uint8_t decimal_scale;
} prdb_sdk_sql_bound_column_v2;

typedef struct prdb_sdk_select_v2 {
    uint32_t struct_size;
    uint32_t abi_version;
    const uint8_t *sql;
    uint64_t sql_len;
    prdb_sdk_sql_span_v1 relation;
    prdb_sdk_sql_span_v1 projection[PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS];
    uint32_t projection_count;
    uint32_t projection_star;
    uint32_t predicate_count;
    uint32_t predicate_root;
    prdb_sdk_predicate_node_v2
        predicates[PRDB_SDK_SQL_MAX_PREDICATE_NODES];
    uint32_t has_limit;
    uint32_t limit;
} prdb_sdk_select_v2;

typedef struct prdb_sdk_select_binding_v2 {
    uint32_t struct_size;
    uint32_t abi_version;
    uint32_t projection_star;
    uint32_t projection_count;
    uint32_t projection_ordinals[PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS];
    uint32_t predicate_count;
    prdb_sdk_sql_bound_column_v2
        predicate_columns[PRDB_SDK_SQL_MAX_PREDICATE_NODES];
} prdb_sdk_select_binding_v2;

/* Resolves one predicate column to its already-authorized row-cell view and
 * pinned schema type metadata. Return a PRDB_SDK_SQL_EVAL_* status. */
typedef int32_t (*prdb_sdk_sql_resolve_column_v2)(
    void *context, const uint8_t *sql, uint64_t sql_len,
    prdb_sdk_sql_span_v1 column, const prdb_sdk_value_v1 **value_out,
    uint32_t *type_out, uint8_t *decimal_precision_out,
    uint8_t *decimal_scale_out);

/* Resolves a bound schema ordinal in one already-authorized sanitized row. */
typedef int32_t (*prdb_sdk_sql_resolve_bound_column_v2)(
    void *context, uint32_t ordinal, const prdb_sdk_value_v1 **value_out);

/* Version 2 preserves v1 and adds a bounded Boolean predicate tree. */
int32_t prdb_sdk_sql_parse_select_v2(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_select_v2 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out);
/* Binds names against one previously authorized relation description. The
 * result maps to ordinals and schema types; it is not a storage capability. */
int32_t prdb_sdk_sql_bind_select_v2(
    const prdb_sdk_select_v2 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v2 *out, uint32_t out_capacity);
/* Evaluates only reached leaves; resolver results must be sanitized views. */
int32_t prdb_sdk_sql_eval_select_predicate_v2(
    const prdb_sdk_select_v2 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    prdb_sdk_sql_resolve_column_v2 resolve_column, void *resolver_context,
    uint32_t *truth_out);
/* Uses binding ordinals and type metadata so callbacks can read row cells by
 * schema position without reparsing names. */
int32_t prdb_sdk_sql_eval_bound_select_predicate_v2(
    const prdb_sdk_select_v2 *statement,
    const prdb_sdk_select_binding_v2 *binding,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    prdb_sdk_sql_resolve_bound_column_v2 resolve_column,
    void *resolver_context, uint32_t *truth_out);

#endif /* PRDB_SDK_SQL_V2_H */
