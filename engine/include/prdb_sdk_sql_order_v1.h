#ifndef PRDB_SDK_SQL_ORDER_V1_H
#define PRDB_SDK_SQL_ORDER_V1_H

#include "prdb_sdk_sql_v1.h"

#define PRDB_SDK_SQL_ORDER_MAX_ROWS UINT32_C(128)
#define PRDB_SDK_SQL_ORDER_MAX_KEYS UINT32_C(9)
#define PRDB_SDK_SQL_ORDER_ASCENDING UINT32_C(0)
#define PRDB_SDK_SQL_ORDER_DESCENDING UINT32_C(1)
#define PRDB_SDK_SQL_ORDER_NULLS_FIRST UINT32_C(1)
#define PRDB_SDK_SQL_ORDER_NULLS_LAST UINT32_C(2)

#define PRDB_SDK_SQL_ORDER_OK INT32_C(0)
#define PRDB_SDK_SQL_ORDER_LIMIT_EXCEEDED INT32_C(6)

typedef struct prdb_sdk_sql_order_key_v1 {
    const prdb_sdk_value_v1 *values;
    uint32_t declared_type;
    uint8_t decimal_scale;
    uint8_t direction;
    uint8_t null_order;
    uint8_t reserved;
} prdb_sdk_sql_order_key_v1;

/* Stable lexicographic ordering for up to nine authorized keys and 128 rows.
 * No index output is written if any key or comparison is invalid. */
int32_t prdb_sdk_sql_order_row_indices_v1(
    const prdb_sdk_sql_order_key_v1 *keys, uint32_t key_count,
    uint32_t row_count, uint32_t *row_indices_out,
    uint32_t row_indices_capacity);

#endif /* PRDB_SDK_SQL_ORDER_V1_H */
