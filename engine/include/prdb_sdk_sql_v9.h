#ifndef PRDB_SDK_SQL_V9_H
#define PRDB_SDK_SQL_V9_H

#include "prdb_sdk_sql_v7.h"

#define PRDB_SDK_SQL_ABI_V9_VERSION UINT32_C(9)

typedef struct prdb_sdk_select_v9 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_v7 base;
    uint32_t has_having;
    uint32_t having_comparison;
    uint32_t having_parameter_index;
} prdb_sdk_select_v9;

typedef struct prdb_sdk_select_binding_v9 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_binding_v7 base;
    uint32_t has_having;
    uint32_t having_comparison;
    uint32_t having_parameter_index;
    uint32_t having_type_code;
    uint8_t having_decimal_precision;
    uint8_t having_decimal_scale;
} prdb_sdk_select_binding_v9;

/* Parses grouped SELECT with optional HAVING <same selected aggregate>
 * <comparison> $n and final LIMIT. WHERE parameters are evaluated before
 * grouping; HAVING parameters compare against the aggregate result type. */
int32_t prdb_sdk_sql_parse_select_v9(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_select_v9 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out);

/* Binds the grouped query and derives the aggregate result type used to
 * validate the HAVING parameter: COUNT -> BIGINT, widened SUM, derived AVG,
 * or the declared MIN/MAX argument type. */
int32_t prdb_sdk_sql_bind_select_v9(
    const prdb_sdk_select_v9 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v9 *out, uint32_t out_capacity);

#endif /* PRDB_SDK_SQL_V9_H */
