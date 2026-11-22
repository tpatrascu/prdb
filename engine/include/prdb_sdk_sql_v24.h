#ifndef PRDB_SDK_SQL_V24_H
#define PRDB_SDK_SQL_V24_H

#include "prdb_sdk_sql_v23.h"

#define PRDB_SDK_SQL_ABI_V24_VERSION UINT32_C(24)

/* V24 uses the v23 layouts and supports one to eight grouping keys. */
typedef prdb_sdk_select_v23 prdb_sdk_select_v24;
typedef prdb_sdk_select_binding_v23 prdb_sdk_select_binding_v24;

int32_t prdb_sdk_sql_parse_select_v24(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v24 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out);

int32_t prdb_sdk_sql_bind_select_v24(
    const prdb_sdk_select_v24 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v24 *out, uint32_t out_capacity);

#endif /* PRDB_SDK_SQL_V24_H */
