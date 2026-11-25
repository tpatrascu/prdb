#ifndef PRDB_SDK_SQL_JOIN_V1_H
#define PRDB_SDK_SQL_JOIN_V1_H

#include "prdb_sdk_sql_v1.h"

#define PRDB_SDK_SQL_JOIN_ABI_V1_VERSION UINT32_C(1)
#define PRDB_SDK_SQL_JOIN_MAX_ROWS UINT32_C(128)
#define PRDB_SDK_SQL_JOIN_MAX_KEYS UINT32_C(8)
#define PRDB_SDK_SQL_JOIN_MAX_PAIRS \
    (PRDB_SDK_SQL_JOIN_MAX_ROWS * PRDB_SDK_SQL_JOIN_MAX_ROWS)

#define PRDB_SDK_SQL_JOIN_OK PRDB_SDK_SQL_EVAL_OK
#define PRDB_SDK_SQL_JOIN_OUTPUT_LIMIT_EXCEEDED INT32_C(7)
#define PRDB_SDK_SQL_JOIN_INPUT_LIMIT_EXCEEDED INT32_C(8)
#define PRDB_SDK_SQL_JOIN_CALLBACK_ERROR INT32_C(9)

typedef struct prdb_sdk_sql_join_pair_v1 {
    uint32_t left_row;
    uint32_t right_row;
} prdb_sdk_sql_join_pair_v1;

typedef struct prdb_sdk_sql_join_key_shape_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    uint32_t declared_type;
    uint8_t decimal_scale;
    uint8_t reserved[3];
} prdb_sdk_sql_join_key_shape_v1;

typedef int32_t (*prdb_sdk_sql_join_pair_visitor_v1_fn)(
    void *context, const prdb_sdk_sql_join_pair_v1 *pair);

/* Computes stable left-major inner equijoin pairs for already-authorized key
 * arrays. */
int32_t prdb_sdk_sql_inner_join_pairs_v1(
    const prdb_sdk_value_v1 *left_keys, uint32_t left_count,
    uint8_t left_decimal_scale,
    const prdb_sdk_value_v1 *right_keys, uint32_t right_count,
    uint8_t right_decimal_scale, uint32_t declared_type,
    prdb_sdk_sql_join_pair_v1 *pairs_out, uint32_t pairs_capacity,
    uint32_t *pair_count_out);

/* Computes stable left-major composite INNER JOIN pairs from row-major key
 * arrays. */
int32_t prdb_sdk_sql_inner_join_pairs_composite_v1(
    const prdb_sdk_value_v1 *left_keys, uint32_t left_count,
    const prdb_sdk_value_v1 *right_keys, uint32_t right_count,
    const prdb_sdk_sql_join_key_shape_v1 *key_shapes, uint32_t key_count,
    prdb_sdk_sql_join_pair_v1 *pairs_out, uint32_t pairs_capacity,
    uint32_t *pair_count_out);

/* Visits stable left-major join pairs without materializing the pair array. */
int32_t prdb_sdk_sql_inner_join_visit_v1(
    const prdb_sdk_value_v1 *left_keys, uint32_t left_count,
    uint8_t left_decimal_scale,
    const prdb_sdk_value_v1 *right_keys, uint32_t right_count,
    uint8_t right_decimal_scale, uint32_t declared_type,
    prdb_sdk_sql_join_pair_visitor_v1_fn visitor, void *visitor_context,
    uint32_t *pair_count_out);

#endif /* PRDB_SDK_SQL_JOIN_V1_H */
