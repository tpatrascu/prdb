#ifndef PRDB_SDK_SQL_V13_H
#define PRDB_SDK_SQL_V13_H

#include "prdb_sdk_sql_v7.h"

#define PRDB_SDK_SQL_ABI_V13_VERSION UINT32_C(13)
#define PRDB_SDK_SQL_V13_MAX_GROUP_KEYS UINT32_C(8)

/* V13 adds multi-column GROUP BY with matching key projections. HAVING and
 * ORDER BY are not accepted by this initial grammar. */
typedef struct prdb_sdk_select_v13 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_v7 base;
    uint32_t group_key_count;
    prdb_sdk_sql_span_v1 projection_keys[PRDB_SDK_SQL_V13_MAX_GROUP_KEYS];
    prdb_sdk_sql_span_v1 group_keys[PRDB_SDK_SQL_V13_MAX_GROUP_KEYS];
} prdb_sdk_select_v13;

typedef struct prdb_sdk_select_binding_v13 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_binding_v7 base;
    uint32_t group_key_count;
    prdb_sdk_sql_bound_column_v2
        group_keys[PRDB_SDK_SQL_V13_MAX_GROUP_KEYS];
} prdb_sdk_select_binding_v13;

int32_t prdb_sdk_sql_parse_select_v13(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v13 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out);
int32_t prdb_sdk_sql_bind_select_v13(
    const prdb_sdk_select_v13 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v13 *out, uint32_t out_capacity);

#endif /* PRDB_SDK_SQL_V13_H */
