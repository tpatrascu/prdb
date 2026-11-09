#ifndef PRDB_SDK_SQL_ORDER_V3_H
#define PRDB_SDK_SQL_ORDER_V3_H

#include "prdb_sdk_sql_order_v2.h"

#define PRDB_SDK_SQL_ORDER_MAX_KEYS UINT32_C(2)

typedef struct prdb_sdk_sql_order_key_v3 {
    const prdb_sdk_value_v1 *values;
    uint32_t declared_type;
    uint8_t decimal_scale;
    uint8_t direction;
    uint8_t null_order;
    uint8_t reserved;
} prdb_sdk_sql_order_key_v3;

/* Stable lexicographic ordering for up to two authorized keys and 128 rows.
 * No index output is written if any key or comparison is invalid. */
int32_t prdb_sdk_sql_order_row_indices_v3(
    const prdb_sdk_sql_order_key_v3 *keys, uint32_t key_count,
    uint32_t row_count, uint32_t *row_indices_out,
    uint32_t row_indices_capacity);

#endif /* PRDB_SDK_SQL_ORDER_V3_H */
