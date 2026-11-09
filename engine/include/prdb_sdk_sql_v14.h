#ifndef PRDB_SDK_SQL_V14_H
#define PRDB_SDK_SQL_V14_H

#include "prdb_sdk_sql_v13.h"

#define PRDB_SDK_SQL_ABI_V14_VERSION UINT32_C(14)

/* V14 adds one aggregate HAVING comparison to the composite v13 grouping
 * grammar. The embedded v13 AST and binding layouts remain frozen. */
typedef struct prdb_sdk_select_v14 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_v13 base;
    uint32_t has_having;
    uint32_t having_comparison;
    uint32_t having_parameter_index;
} prdb_sdk_select_v14;

typedef struct prdb_sdk_select_binding_v14 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_binding_v13 base;
    uint32_t has_having;
    uint32_t having_comparison;
    uint32_t having_parameter_index;
    uint32_t having_type_code;
    uint8_t having_decimal_precision;
    uint8_t having_decimal_scale;
} prdb_sdk_select_binding_v14;

int32_t prdb_sdk_sql_parse_select_v14(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v14 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out);
int32_t prdb_sdk_sql_bind_select_v14(
    const prdb_sdk_select_v14 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v14 *out, uint32_t out_capacity);

#endif /* PRDB_SDK_SQL_V14_H */
