#ifndef PRDB_SDK_SQL_V5_H
#define PRDB_SDK_SQL_V5_H

#include "prdb_sdk_sql_v2.h"

#define PRDB_SDK_SQL_ABI_V5_VERSION UINT32_C(5)
#define PRDB_SDK_SQL_V5_AGGREGATE_COUNT_STAR UINT32_C(1)
#define PRDB_SDK_SQL_V5_AGGREGATE_MIN UINT32_C(2)
#define PRDB_SDK_SQL_V5_AGGREGATE_MAX UINT32_C(3)

typedef struct prdb_sdk_select_v5 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_v2 base;
    uint32_t has_group_by;
    prdb_sdk_sql_span_v1 group_by_column;
    uint32_t aggregate_kind;
    prdb_sdk_sql_span_v1 aggregate_column;
} prdb_sdk_select_v5;

typedef struct prdb_sdk_select_binding_v5 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_binding_v2 base;
    uint32_t has_group_by;
    prdb_sdk_sql_bound_column_v2 group_by_column;
    uint32_t aggregate_kind;
    prdb_sdk_sql_bound_column_v2 aggregate_column;
} prdb_sdk_select_binding_v5;

/* Parses one key projection with COUNT(*), MIN(column), or MAX(column), then
 * FROM, optional WHERE, GROUP BY the same key, and optional final LIMIT. */
int32_t prdb_sdk_sql_parse_select_v5(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_select_v5 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out);

/* Binds both the group key and optional MIN/MAX argument against the same
 * authorized relation shape. COUNT(*) has no aggregate-column binding. */
int32_t prdb_sdk_sql_bind_select_v5(
    const prdb_sdk_select_v5 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v5 *out, uint32_t out_capacity);

#endif /* PRDB_SDK_SQL_V5_H */
