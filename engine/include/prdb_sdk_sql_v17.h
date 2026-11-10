#ifndef PRDB_SDK_SQL_V17_H
#define PRDB_SDK_SQL_V17_H

#include "prdb_sdk_sql_v15.h"

#define PRDB_SDK_SQL_ABI_V17_VERSION UINT32_C(17)
#define PRDB_SDK_SQL_BIND_V17_LITERAL_TYPE_MISMATCH INT32_C(9)

/* V17 adds a nonnegative BIGINT literal as the right operand of its one
 * selected aggregate HAVING comparison. Earlier statement and binding layouts
 * stay frozen. A zero has_having_literal means the v15 parameter form is used. */
typedef struct prdb_sdk_select_v17 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_v15 base;
    uint32_t has_having_literal;
    uint64_t having_literal;
} prdb_sdk_select_v17;

typedef struct prdb_sdk_select_binding_v17 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_binding_v15 base;
    uint32_t has_having_literal;
    uint64_t having_literal;
} prdb_sdk_select_binding_v17;

int32_t prdb_sdk_sql_parse_select_v17(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v17 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out);
int32_t prdb_sdk_sql_bind_select_v17(
    const prdb_sdk_select_v17 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v17 *out, uint32_t out_capacity);

#endif /* PRDB_SDK_SQL_V17_H */
