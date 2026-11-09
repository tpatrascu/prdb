#ifndef PRDB_SDK_SQL_V8_H
#define PRDB_SDK_SQL_V8_H

#include "prdb_sdk_sql_v2.h"

#define PRDB_SDK_SQL_ABI_V8_VERSION UINT32_C(8)
#define PRDB_SDK_SQL_V8_LEFT UINT32_C(0)
#define PRDB_SDK_SQL_V8_RIGHT UINT32_C(1)

#define PRDB_SDK_SQL_BIND_V8_JOIN_TYPE_MISMATCH INT32_C(7)

typedef struct prdb_sdk_sql_qualified_column_v8 {
    prdb_sdk_sql_span_v1 relation;
    prdb_sdk_sql_span_v1 column;
} prdb_sdk_sql_qualified_column_v8;

typedef struct prdb_sdk_select_v8 {
    uint32_t struct_size;
    uint32_t abi_version;
    const uint8_t *sql;
    uint64_t sql_len;
    prdb_sdk_sql_span_v1 left_relation;
    prdb_sdk_sql_span_v1 right_relation;
    prdb_sdk_sql_qualified_column_v8
        projections[PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS];
    uint32_t projection_count;
    prdb_sdk_sql_qualified_column_v8 left_join_key;
    prdb_sdk_sql_qualified_column_v8 right_join_key;
    uint32_t has_limit;
    uint32_t limit;
} prdb_sdk_select_v8;

typedef struct prdb_sdk_sql_join_projection_binding_v8 {
    uint32_t relation_side;
    prdb_sdk_sql_bound_column_v2 column;
} prdb_sdk_sql_join_projection_binding_v8;

typedef struct prdb_sdk_select_binding_v8 {
    uint32_t struct_size;
    uint32_t abi_version;
    uint32_t left_relation_column_count;
    uint32_t right_relation_column_count;
    uint32_t projection_count;
    prdb_sdk_sql_join_projection_binding_v8
        projections[PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS];
    prdb_sdk_sql_join_projection_binding_v8 left_join_key;
    prdb_sdk_sql_join_projection_binding_v8 right_join_key;
} prdb_sdk_select_binding_v8;

/* Parses SELECT qualified-columns FROM relation INNER JOIN relation ON
 * qualified-column = qualified-column, with optional LIMIT and semicolon.
 * Relation aliases, WHERE, outer joins, and multiple join predicates are not
 * part of this first bounded form. */
int32_t prdb_sdk_sql_parse_select_v8(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_select_v8 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out);

/* Binds both relations against their separately authorized shapes. Join keys
 * must have the same type and Decimal scale. Qualified projections retain
 * the source side and schema ordinal. */
int32_t prdb_sdk_sql_bind_select_v8(
    const prdb_sdk_select_v8 *statement,
    const uint8_t *left_relation_name, uint32_t left_relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *left_columns,
    uint32_t left_column_count,
    const uint8_t *right_relation_name, uint32_t right_relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *right_columns,
    uint32_t right_column_count, prdb_sdk_select_binding_v8 *out,
    uint32_t out_capacity);

#endif /* PRDB_SDK_SQL_V8_H */
