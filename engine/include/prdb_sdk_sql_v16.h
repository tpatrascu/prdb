#ifndef PRDB_SDK_SQL_V16_H
#define PRDB_SDK_SQL_V16_H

#include "prdb_sdk_sql_v8.h"

#define PRDB_SDK_SQL_ABI_V16_VERSION UINT32_C(16)
#define PRDB_SDK_SQL_V16_MAX_JOIN_KEYS UINT32_C(8)
#define PRDB_SDK_SQL_BIND_V16_JOIN_TYPE_MISMATCH INT32_C(8)

typedef struct prdb_sdk_select_v16 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_v8 base;
    uint32_t join_key_count;
    prdb_sdk_sql_qualified_column_v8
        left_join_keys[PRDB_SDK_SQL_V16_MAX_JOIN_KEYS];
    prdb_sdk_sql_qualified_column_v8
        right_join_keys[PRDB_SDK_SQL_V16_MAX_JOIN_KEYS];
} prdb_sdk_select_v16;

typedef struct prdb_sdk_sql_join_key_binding_v16 {
    prdb_sdk_sql_bound_column_v2 left;
    prdb_sdk_sql_bound_column_v2 right;
} prdb_sdk_sql_join_key_binding_v16;

typedef struct prdb_sdk_select_binding_v16 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_binding_v8 base;
    uint32_t join_key_count;
    prdb_sdk_sql_join_key_binding_v16
        join_keys[PRDB_SDK_SQL_V16_MAX_JOIN_KEYS];
} prdb_sdk_select_binding_v16;

/* Parses qualified projections and two to eight qualified equality
 * predicates joined by AND:
 * `... ON left.k1 = right.k1 AND left.k2 = right.k2 ... [LIMIT n]`.
 * The first predicate is also preserved in the embedded v8 record. */
int32_t prdb_sdk_sql_parse_select_v16(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v16 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out);

/* Binds projections through the frozen v8 binder and every key pair against
 * the independently authorized relation shapes. Each pair must be
 * left-qualified then right-qualified with the same scalar type and Decimal
 * scale. */
int32_t prdb_sdk_sql_bind_select_v16(
    const prdb_sdk_select_v16 *statement,
    const uint8_t *left_relation_name, uint32_t left_relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *left_columns,
    uint32_t left_column_count,
    const uint8_t *right_relation_name, uint32_t right_relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *right_columns,
    uint32_t right_column_count, prdb_sdk_select_binding_v16 *out,
    uint32_t out_capacity);

#endif /* PRDB_SDK_SQL_V16_H */
