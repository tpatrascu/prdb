#ifndef PRDB_SDK_SQL_ORDER_V1_H
#define PRDB_SDK_SQL_ORDER_V1_H

#include "prdb_sdk_sql_v1.h"

#define PRDB_SDK_SQL_ORDER_MAX_ROWS UINT32_C(128)
#define PRDB_SDK_SQL_ORDER_ASCENDING UINT32_C(0)
#define PRDB_SDK_SQL_ORDER_DESCENDING UINT32_C(1)

#define PRDB_SDK_SQL_ORDER_OK INT32_C(0)
#define PRDB_SDK_SQL_ORDER_LIMIT_EXCEEDED INT32_C(6)

/* Produces a stable permutation for one declared sort key. NULL sorts first
 * in both directions; REDACTED is an error. Equal keys keep input order.
 * The caller must have selected only authorized values and keep them alive
 * for the duration of this call. No output index is written on error. */
int32_t prdb_sdk_sql_order_row_indices_v1(
    const prdb_sdk_value_v1 *keys, uint32_t key_count,
    uint32_t declared_type, uint8_t decimal_scale, uint32_t direction,
    uint32_t *row_indices_out, uint32_t row_indices_capacity);

#endif /* PRDB_SDK_SQL_ORDER_V1_H */
