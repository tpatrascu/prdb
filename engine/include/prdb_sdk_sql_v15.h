#ifndef PRDB_SDK_SQL_V15_H
#define PRDB_SDK_SQL_V15_H

#include "prdb_sdk_sql_v14.h"
#include "prdb_sdk_sql_v10.h"
#include "prdb_sdk_sql_v11.h"

#define PRDB_SDK_SQL_ABI_V15_VERSION UINT32_C(15)
#define PRDB_SDK_SQL_V15_MAX_ORDER_KEYS UINT32_C(9)

typedef struct prdb_sdk_sql_order_item_v15 {
    uint32_t kind;
    uint32_t group_key_index;
    uint32_t direction;
    uint32_t null_order;
} prdb_sdk_sql_order_item_v15;

typedef struct prdb_sdk_select_v15 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_v14 base;
    uint32_t order_count;
    prdb_sdk_sql_order_item_v15 order_items[PRDB_SDK_SQL_V15_MAX_ORDER_KEYS];
} prdb_sdk_select_v15;

typedef struct prdb_sdk_sql_bound_order_item_v15 {
    prdb_sdk_sql_order_item_v15 item;
    uint32_t type_code;
    uint8_t decimal_precision;
    uint8_t decimal_scale;
} prdb_sdk_sql_bound_order_item_v15;

typedef struct prdb_sdk_select_binding_v15 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_binding_v14 base;
    uint32_t order_count;
    prdb_sdk_sql_bound_order_item_v15
        order_items[PRDB_SDK_SQL_V15_MAX_ORDER_KEYS];
} prdb_sdk_select_binding_v15;

/* V15 adds up to nine distinct ORDER BY outputs: any selected group keys and
 * the selected aggregate, with independent direction and NULL placement. */
int32_t prdb_sdk_sql_parse_select_v15(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v15 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out);
int32_t prdb_sdk_sql_bind_select_v15(
    const prdb_sdk_select_v15 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v15 *out, uint32_t out_capacity);

#endif /* PRDB_SDK_SQL_V15_H */
