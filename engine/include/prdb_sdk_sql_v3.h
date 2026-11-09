#ifndef PRDB_SDK_SQL_V3_H
#define PRDB_SDK_SQL_V3_H

#include "prdb_sdk_sql_v2.h"
#include "prdb_sdk_sql_order_v1.h"

#define PRDB_SDK_SQL_ABI_V3_VERSION UINT32_C(3)

typedef struct prdb_sdk_select_v3 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_v2 base;
    uint32_t has_order_by;
    prdb_sdk_sql_span_v1 order_by_column;
    uint32_t order_descending;
} prdb_sdk_select_v3;

typedef struct prdb_sdk_select_binding_v3 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_binding_v2 base;
    uint32_t has_order_by;
    prdb_sdk_sql_bound_column_v2 order_by_column;
} prdb_sdk_select_binding_v3;

/* Version 3 preserves the v1/v2 layouts and adds one source-column ORDER BY
 * key. ASC is the default. LIMIT follows ORDER BY when both are present. */
int32_t prdb_sdk_sql_parse_select_v3(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_select_v3 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out);
/* Binds the order key against the full authorized relation shape; an
 * unprojected source column is permitted. */
int32_t prdb_sdk_sql_bind_select_v3(
    const prdb_sdk_select_v3 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v3 *out, uint32_t out_capacity);

#endif /* PRDB_SDK_SQL_V3_H */
