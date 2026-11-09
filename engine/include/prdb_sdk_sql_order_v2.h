#ifndef PRDB_SDK_SQL_ORDER_V2_H
#define PRDB_SDK_SQL_ORDER_V2_H

#include "prdb_sdk_sql_order_v1.h"

#define PRDB_SDK_SQL_ORDER_NULLS_FIRST UINT32_C(1)
#define PRDB_SDK_SQL_ORDER_NULLS_LAST UINT32_C(2)

/* Stable bounded sort extension with explicit NULL placement. */
int32_t prdb_sdk_sql_order_row_indices_v2(
    const prdb_sdk_value_v1 *keys, uint32_t key_count,
    uint32_t declared_type, uint8_t decimal_scale, uint32_t direction,
    uint32_t null_order, uint32_t *row_indices_out,
    uint32_t row_indices_capacity);

#endif /* PRDB_SDK_SQL_ORDER_V2_H */
