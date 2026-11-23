#ifndef PRDB_SDK_SQL_V25_H
#define PRDB_SDK_SQL_V25_H

#include "prdb_sdk_sql_v20.h"

#define PRDB_SDK_SQL_ABI_V25_VERSION UINT32_C(25)

/* V25 adds one qualified ORDER BY column to the aliased composite JOIN.
 * The order key must be included in the SELECT projection. */
typedef struct prdb_sdk_select_v25 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_v20 base;
    uint32_t order_keyword_offset;
    uint32_t has_order_by;
    prdb_sdk_sql_qualified_column_v8 order_by_column;
    uint32_t order_descending;
} prdb_sdk_select_v25;

typedef struct prdb_sdk_select_binding_v25 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_binding_v20 base;
    uint32_t order_by_projection_index;
    uint32_t order_relation_side;
    prdb_sdk_sql_bound_column_v2 order_by_column;
} prdb_sdk_select_binding_v25;

int32_t prdb_sdk_sql_parse_select_v25(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v25 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out);

int32_t prdb_sdk_sql_bind_select_v25(
    const prdb_sdk_select_v25 *statement,
    const uint8_t *left_relation_name, uint32_t left_relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *left_columns,
    uint32_t left_column_count, const uint8_t *right_relation_name,
    uint32_t right_relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *right_columns,
    uint32_t right_column_count, prdb_sdk_select_binding_v25 *out,
    uint32_t out_capacity);

#endif /* PRDB_SDK_SQL_V25_H */
