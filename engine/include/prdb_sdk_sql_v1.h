#ifndef PRDB_SDK_SQL_V1_H
#define PRDB_SDK_SQL_V1_H

#include <stdint.h>

#define PRDB_SDK_SQL_ABI_V1_VERSION UINT32_C(1)
#define PRDB_SDK_SQL_MAX_QUERY_BYTES UINT64_C(65536)
#define PRDB_SDK_SQL_MAX_IDENTIFIER_BYTES UINT32_C(128)
#define PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS UINT32_C(64)
#define PRDB_SDK_SQL_MAX_PARAMETER_INDEX UINT32_C(65535)
#define PRDB_SDK_SQL_MAX_LIMIT UINT32_C(1000000)

#define PRDB_SDK_SQL_OK INT32_C(0)
#define PRDB_SDK_SQL_UNSUPPORTED_STATEMENT INT32_C(1)
#define PRDB_SDK_SQL_SYNTAX_ERROR INT32_C(2)
#define PRDB_SDK_SQL_LIMIT_EXCEEDED INT32_C(3)
#define PRDB_SDK_SQL_INVALID_ARGUMENT INT32_C(4)

#define PRDB_SDK_SQL_COMPARE_EQUAL UINT32_C(1)
#define PRDB_SDK_SQL_COMPARE_NOT_EQUAL UINT32_C(2)
#define PRDB_SDK_SQL_COMPARE_LESS UINT32_C(3)
#define PRDB_SDK_SQL_COMPARE_LESS_EQUAL UINT32_C(4)
#define PRDB_SDK_SQL_COMPARE_GREATER UINT32_C(5)
#define PRDB_SDK_SQL_COMPARE_GREATER_EQUAL UINT32_C(6)

typedef struct prdb_sdk_sql_span_v1 {
    uint32_t offset;
    uint32_t length;
} prdb_sdk_sql_span_v1;

typedef struct prdb_sdk_select_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    const uint8_t *sql;
    uint64_t sql_len;
    prdb_sdk_sql_span_v1 relation;
    prdb_sdk_sql_span_v1 projection[PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS];
    uint32_t projection_count;
    uint32_t projection_star;
    uint32_t has_where;
    prdb_sdk_sql_span_v1 where_column;
    uint32_t comparison;
    uint32_t parameter_index;
    uint32_t has_limit;
    uint32_t limit;
} prdb_sdk_select_v1;

/* The output borrows the immutable SQL input and is written only on success. */
int32_t prdb_sdk_sql_parse_select_v1(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_select_v1 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out);

#endif /* PRDB_SDK_SQL_V1_H */
