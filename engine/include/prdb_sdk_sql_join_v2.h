#ifndef PRDB_SDK_SQL_JOIN_V2_H
#define PRDB_SDK_SQL_JOIN_V2_H

#include "prdb_sdk_sql_join_v1.h"

#define PRDB_SDK_SQL_JOIN_ABI_V2_VERSION UINT32_C(2)
#define PRDB_SDK_SQL_JOIN_MAX_KEYS UINT32_C(8)

typedef struct prdb_sdk_sql_join_key_shape_v2 {
    uint32_t struct_size;
    uint32_t abi_version;
    uint32_t declared_type;
    uint8_t decimal_scale;
    uint8_t reserved[3];
} prdb_sdk_sql_join_key_shape_v2;

/* Computes stable left-major composite INNER JOIN pairs from row-major key
 * arrays. Each row contains key_count values, and each key has one matching
 * shape on both sides. A row pair matches only when every equality is TRUE;
 * therefore any NULL component prevents the match. REDACTED, malformed, or
 * mismatched cells fail. At most 128 rows per side, eight keys, and 16,384
 * output pairs are considered. All inputs are validated before output is
 * written; caller buffers must be disjoint. */
int32_t prdb_sdk_sql_inner_join_pairs_composite_v2(
    const prdb_sdk_value_v1 *left_keys, uint32_t left_count,
    const prdb_sdk_value_v1 *right_keys, uint32_t right_count,
    const prdb_sdk_sql_join_key_shape_v2 *key_shapes, uint32_t key_count,
    prdb_sdk_sql_join_pair_v1 *pairs_out, uint32_t pairs_capacity,
    uint32_t *pair_count_out);

#endif /* PRDB_SDK_SQL_JOIN_V2_H */
