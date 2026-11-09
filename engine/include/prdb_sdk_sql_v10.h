#ifndef PRDB_SDK_SQL_V10_H
#define PRDB_SDK_SQL_V10_H

#include "prdb_sdk_sql_v9.h"

#define PRDB_SDK_SQL_ABI_V10_VERSION UINT32_C(10)
#define PRDB_SDK_SQL_V10_ORDER_GROUP_KEY UINT32_C(1)
#define PRDB_SDK_SQL_V10_ORDER_AGGREGATE UINT32_C(2)
#define PRDB_SDK_SQL_V10_ORDER_ASCENDING UINT32_C(0)
#define PRDB_SDK_SQL_V10_ORDER_DESCENDING UINT32_C(1)

typedef struct prdb_sdk_select_v10 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_v9 base;
    uint32_t has_order_by;
    uint32_t order_kind;
    uint32_t order_direction;
} prdb_sdk_select_v10;

typedef struct prdb_sdk_select_binding_v10 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_binding_v9 base;
    uint32_t has_order_by;
    uint32_t order_kind;
    uint32_t order_direction;
    uint32_t order_type_code;
    uint8_t order_decimal_precision;
    uint8_t order_decimal_scale;
} prdb_sdk_select_binding_v10;

/* Parses grouped SELECT with optional HAVING, one ORDER BY group key or the
 * selected aggregate, optional ASC/DESC, and final LIMIT. */
int32_t prdb_sdk_sql_parse_select_v10(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v10 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out);

/* Binds the selected sort expression to the group-key or aggregate result
 * type, preserving Decimal precision and scale. */
int32_t prdb_sdk_sql_bind_select_v10(
    const prdb_sdk_select_v10 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v10 *out, uint32_t out_capacity);

#endif /* PRDB_SDK_SQL_V10_H */
