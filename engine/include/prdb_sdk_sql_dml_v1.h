#ifndef PRDB_SDK_SQL_DML_V1_H
#define PRDB_SDK_SQL_DML_V1_H

#include "prdb_sdk_sql_v1.h"

#define PRDB_SDK_SQL_DML_ABI_V1_VERSION UINT32_C(1)
#define PRDB_SDK_SQL_MAX_INSERT_VALUES UINT32_C(4096)

#define PRDB_SDK_SQL_INSERT_PARAMETER UINT32_C(0)
#define PRDB_SDK_SQL_INSERT_NULL UINT32_C(1)
#define PRDB_SDK_SQL_INSERT_DEFAULT UINT32_C(2)

typedef struct prdb_sdk_sql_insert_cell_v1 {
    uint32_t kind;
    uint32_t parameter_index;
} prdb_sdk_sql_insert_cell_v1;

typedef struct prdb_sdk_insert_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    const uint8_t *sql;
    uint64_t sql_len;
    prdb_sdk_sql_span_v1 relation;
    uint32_t value_count;
    prdb_sdk_sql_insert_cell_v1 values[PRDB_SDK_SQL_MAX_INSERT_VALUES];
} prdb_sdk_insert_v1;

int32_t prdb_sdk_sql_parse_insert_v1(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_insert_v1 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out);

#endif
