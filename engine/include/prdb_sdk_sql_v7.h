#ifndef PRDB_SDK_SQL_V7_H
#define PRDB_SDK_SQL_V7_H

#include "prdb_sdk_sql_v2.h"

#define PRDB_SDK_SQL_ABI_V7_VERSION UINT32_C(7)
#define PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR UINT32_C(1)
#define PRDB_SDK_SQL_V7_AGGREGATE_MIN UINT32_C(2)
#define PRDB_SDK_SQL_V7_AGGREGATE_MAX UINT32_C(3)
#define PRDB_SDK_SQL_V7_AGGREGATE_SUM UINT32_C(4)
#define PRDB_SDK_SQL_V7_AGGREGATE_AVG UINT32_C(5)

#define PRDB_SDK_SQL_BIND_V7_INVALID_AGGREGATE_TYPE INT32_C(7)

typedef struct prdb_sdk_select_v7 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_v2 base;
    uint32_t has_group_by;
    prdb_sdk_sql_span_v1 group_by_column;
    uint32_t aggregate_kind;
    prdb_sdk_sql_span_v1 aggregate_column;
} prdb_sdk_select_v7;

typedef struct prdb_sdk_select_binding_v7 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_binding_v2 base;
    uint32_t has_group_by;
    prdb_sdk_sql_bound_column_v2 group_by_column;
    uint32_t aggregate_kind;
    prdb_sdk_sql_bound_column_v2 aggregate_column;
} prdb_sdk_select_binding_v7;

/* Parses grouped COUNT(*), MIN(column), MAX(column), SUM(column), or
 * AVG(column), with one key projection, optional WHERE, and final LIMIT. */
int32_t prdb_sdk_sql_parse_select_v7(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_select_v7 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out);

/* Binds grouped keys and aggregate arguments. SUM/AVG accept only SMALLINT,
 * INTEGER, BIGINT, and DECIMAL columns. */
int32_t prdb_sdk_sql_bind_select_v7(
    const prdb_sdk_select_v7 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v7 *out, uint32_t out_capacity);

#endif /* PRDB_SDK_SQL_V7_H */
