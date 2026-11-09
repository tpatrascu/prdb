#ifndef PRDB_SDK_SQL_V4_H
#define PRDB_SDK_SQL_V4_H

#include "prdb_sdk_sql_v2.h"

#define PRDB_SDK_SQL_ABI_V4_VERSION UINT32_C(4)
#define PRDB_SDK_SQL_AGGREGATE_COUNT_STAR UINT32_C(1)

typedef struct prdb_sdk_select_v4 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_v2 base;
    uint32_t has_group_by;
    prdb_sdk_sql_span_v1 group_by_column;
    uint32_t aggregate_kind;
} prdb_sdk_select_v4;

typedef struct prdb_sdk_select_binding_v4 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_binding_v2 base;
    uint32_t has_group_by;
    prdb_sdk_sql_bound_column_v2 group_by_column;
    uint32_t aggregate_kind;
} prdb_sdk_select_binding_v4;

/* Parses SELECT key, COUNT(*) FROM relation [WHERE ...] GROUP BY key
 * [LIMIT n]. V1-v3 SELECT records remain unchanged. */
int32_t prdb_sdk_sql_parse_select_v4(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_select_v4 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out);
/* Binds the sole grouping key and projection against one authorized relation
 * shape; no scan or aggregate execution capability is returned. */
int32_t prdb_sdk_sql_bind_select_v4(
    const prdb_sdk_select_v4 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v4 *out, uint32_t out_capacity);

#endif /* PRDB_SDK_SQL_V4_H */
