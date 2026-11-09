#ifndef PRDB_SDK_SQL_GROUP_V1_H
#define PRDB_SDK_SQL_GROUP_V1_H

#include "prdb_sdk_sql_v1.h"

#define PRDB_SDK_SQL_GROUP_MAX_ROWS UINT32_C(128)
#define PRDB_SDK_SQL_GROUP_OK INT32_C(0)
#define PRDB_SDK_SQL_GROUP_LIMIT_EXCEEDED INT32_C(6)

#define PRDB_SDK_SQL_MINMAX_MIN UINT32_C(1)
#define PRDB_SDK_SQL_MINMAX_MAX UINT32_C(2)

/* SUM widens SMALLINT/INTEGER/BIGINT to BIGINT and returns DECIMAL(38,s)
 * for DECIMAL(p,s). NULLs are ignored; empty/all-NULL returns canonical NULL.
 * The numeric bytes are written to caller storage only on success. */
int32_t prdb_sdk_sql_sum_v1(
    const prdb_sdk_value_v1 *values, uint32_t value_count,
    uint32_t declared_type, uint8_t decimal_precision, uint8_t decimal_scale,
    prdb_sdk_value_v1 *value_out, uint32_t out_capacity,
    uint8_t *value_bytes_out, uint64_t value_bytes_capacity);

/* AVG returns DECIMAL(38,result_scale), where integer input uses scale 6 and
 * Decimal input uses min(max(6, scale + 6), 38 - (precision - scale)). The
 * exact average is rounded to nearest, ties to even. result_scale_out and
 * the value/byte outputs are written only on success. */
int32_t prdb_sdk_sql_avg_v1(
    const prdb_sdk_value_v1 *values, uint32_t value_count,
    uint32_t declared_type, uint8_t decimal_precision, uint8_t decimal_scale,
    prdb_sdk_value_v1 *value_out, uint32_t out_capacity,
    uint8_t *value_bytes_out, uint64_t value_bytes_capacity,
    uint8_t *result_scale_out);

typedef struct prdb_sdk_sql_count_group_v1 {
    uint32_t representative_row;
    uint64_t count;
} prdb_sdk_sql_count_group_v1;

/* Groups one authorized scalar key and counts every input row, including rows
 * whose key is NULL. Groups are emitted in first-seen order. REDACTED or
 * malformed keys fail without writing groups_out or group_count_out. */
int32_t prdb_sdk_sql_group_count_v1(
    const prdb_sdk_value_v1 *keys, uint32_t key_count,
    uint32_t declared_type, uint8_t decimal_scale,
    prdb_sdk_sql_count_group_v1 *groups_out, uint32_t groups_capacity,
    uint32_t *group_count_out);

/* Computes MIN or MAX over at most 128 authorized typed values. SQL NULLs
 * are ignored; empty/all-NULL input returns a canonical NULL view. REDACTED
 * and invalid operands fail without changing value_out. The selected VALUE
 * borrows input bytes and ties retain the first matching input representation. */
int32_t prdb_sdk_sql_minmax_v1(
    const prdb_sdk_value_v1 *values, uint32_t value_count,
    uint32_t declared_type, uint8_t decimal_scale, uint32_t operation,
    prdb_sdk_value_v1 *value_out, uint32_t out_capacity);

#endif /* PRDB_SDK_SQL_GROUP_V1_H */
