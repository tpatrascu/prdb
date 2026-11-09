#ifndef PRDB_SDK_SQL_V11_H
#define PRDB_SDK_SQL_V11_H

#include "prdb_sdk_sql_v10.h"

#define PRDB_SDK_SQL_ABI_V11_VERSION UINT32_C(11)
#define PRDB_SDK_SQL_V11_NULL_ORDER_DEFAULT UINT32_C(0)
#define PRDB_SDK_SQL_V11_NULLS_FIRST UINT32_C(1)
#define PRDB_SDK_SQL_V11_NULLS_LAST UINT32_C(2)

typedef struct prdb_sdk_select_v11 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_v10 base;
    uint32_t null_order;
} prdb_sdk_select_v11;

typedef struct prdb_sdk_select_binding_v11 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_binding_v10 base;
    uint32_t null_order;
} prdb_sdk_select_binding_v11;

/* Adds optional NULLS FIRST/LAST to the v10 grouped ORDER BY grammar. When
 * omitted, NULLS FIRST preserves the established grouped ordering behavior. */
int32_t prdb_sdk_sql_parse_select_v11(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v11 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out);

int32_t prdb_sdk_sql_bind_select_v11(
    const prdb_sdk_select_v11 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v11 *out, uint32_t out_capacity);

#endif /* PRDB_SDK_SQL_V11_H */
