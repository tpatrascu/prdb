#ifndef PRDB_SDK_SQL_V23_H
#define PRDB_SDK_SQL_V23_H

#include "prdb_sdk_sql_v22.h"

#define PRDB_SDK_SQL_ABI_V23_VERSION UINT32_C(23)

/* V23 additionally permits IS_NULL / IS_NOT_NULL HAVING leaf nodes.
 * Bit i marks any leaf whose left operand is hidden COUNT(*). */
typedef struct prdb_sdk_select_v23 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_v21 base;
    uint64_t having_count_mask;
} prdb_sdk_select_v23;

typedef struct prdb_sdk_select_binding_v23 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_binding_v21 base;
    uint64_t having_count_mask;
} prdb_sdk_select_binding_v23;

int32_t prdb_sdk_sql_parse_select_v23(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v23 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out);

int32_t prdb_sdk_sql_bind_select_v23(
    const prdb_sdk_select_v23 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v23 *out, uint32_t out_capacity);

#endif /* PRDB_SDK_SQL_V23_H */
