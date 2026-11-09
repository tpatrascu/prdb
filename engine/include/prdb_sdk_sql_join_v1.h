#ifndef PRDB_SDK_SQL_JOIN_V1_H
#define PRDB_SDK_SQL_JOIN_V1_H

#include "prdb_sdk_sql_v1.h"

#define PRDB_SDK_SQL_JOIN_MAX_ROWS UINT32_C(128)
#define PRDB_SDK_SQL_JOIN_MAX_PAIRS \
    (PRDB_SDK_SQL_JOIN_MAX_ROWS * PRDB_SDK_SQL_JOIN_MAX_ROWS)

#define PRDB_SDK_SQL_JOIN_OK PRDB_SDK_SQL_EVAL_OK
#define PRDB_SDK_SQL_JOIN_OUTPUT_LIMIT_EXCEEDED INT32_C(7)
#define PRDB_SDK_SQL_JOIN_INPUT_LIMIT_EXCEEDED INT32_C(8)

typedef struct prdb_sdk_sql_join_pair_v1 {
    uint32_t left_row;
    uint32_t right_row;
} prdb_sdk_sql_join_pair_v1;

/* Computes stable left-major inner equijoin pairs for already-authorized key
 * arrays. Both sides must use the same declared type and, for Decimal keys,
 * the same declared scale. NULL never matches NULL. REDACTED, malformed, or
 * mismatched values fail. At most 128 keys per side and 16,384 output pairs
 * are considered. No output is changed on failure. */
int32_t prdb_sdk_sql_inner_join_pairs_v1(
    const prdb_sdk_value_v1 *left_keys, uint32_t left_count,
    uint8_t left_decimal_scale,
    const prdb_sdk_value_v1 *right_keys, uint32_t right_count,
    uint8_t right_decimal_scale, uint32_t declared_type,
    prdb_sdk_sql_join_pair_v1 *pairs_out, uint32_t pairs_capacity,
    uint32_t *pair_count_out);

#endif /* PRDB_SDK_SQL_JOIN_V1_H */
