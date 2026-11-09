#ifndef PRDB_SDK_SQL_V12_H
#define PRDB_SDK_SQL_V12_H

#include "prdb_sdk_sql_v11.h"

#define PRDB_SDK_SQL_ABI_V12_VERSION UINT32_C(12)
#define PRDB_SDK_SQL_V12_MAX_ORDER_KEYS UINT32_C(2)

typedef struct prdb_sdk_sql_order_item_v12 {
    uint32_t kind;
    uint32_t direction;
    uint32_t null_order;
} prdb_sdk_sql_order_item_v12;

typedef struct prdb_sdk_select_v12 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_v11 base;
    uint32_t order_count;
    prdb_sdk_sql_order_item_v12 order_items[PRDB_SDK_SQL_V12_MAX_ORDER_KEYS];
} prdb_sdk_select_v12;

typedef struct prdb_sdk_sql_bound_order_item_v12 {
    prdb_sdk_sql_order_item_v12 item;
    uint32_t type_code;
    uint8_t decimal_precision;
    uint8_t decimal_scale;
} prdb_sdk_sql_bound_order_item_v12;

typedef struct prdb_sdk_select_binding_v12 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_binding_v11 base;
    uint32_t order_count;
    prdb_sdk_sql_bound_order_item_v12 order_items[PRDB_SDK_SQL_V12_MAX_ORDER_KEYS];
} prdb_sdk_select_binding_v12;

/* Retains v11's optional ORDER BY and supports a lexicographic list of the
 * selected group key and aggregate, each with independent direction/NULLs. */
int32_t prdb_sdk_sql_parse_select_v12(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v12 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out);
int32_t prdb_sdk_sql_bind_select_v12(
    const prdb_sdk_select_v12 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v12 *out, uint32_t out_capacity);

#endif /* PRDB_SDK_SQL_V12_H */
