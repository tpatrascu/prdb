#ifndef PRDB_SDK_SQL_DML_V1_H
#define PRDB_SDK_SQL_DML_V1_H

#include "prdb_sdk_sql_v1.h"
#include "prdb_sdk_sql_v2.h"

#define PRDB_SDK_SQL_DML_ABI_V1_VERSION UINT32_C(1)
#define PRDB_SDK_SQL_MAX_INSERT_VALUES UINT32_C(4096)
#define PRDB_SDK_SQL_MAX_UPDATE_ASSIGNMENTS UINT32_C(64)

#define PRDB_SDK_SQL_INSERT_PARAMETER UINT32_C(0)
#define PRDB_SDK_SQL_INSERT_NULL UINT32_C(1)
#define PRDB_SDK_SQL_INSERT_DEFAULT UINT32_C(2)
#define PRDB_SDK_SQL_UPDATE_PARAMETER UINT32_C(0)
#define PRDB_SDK_SQL_UPDATE_NULL UINT32_C(1)

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

typedef struct prdb_sdk_sql_update_assignment_v1 {
    prdb_sdk_sql_span_v1 column;
    uint32_t kind;
    uint32_t parameter_index;
} prdb_sdk_sql_update_assignment_v1;

typedef struct prdb_sdk_update_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    const uint8_t *sql;
    uint64_t sql_len;
    prdb_sdk_sql_span_v1 relation;
    uint32_t assignment_count;
    prdb_sdk_sql_update_assignment_v1
        assignments[PRDB_SDK_SQL_MAX_UPDATE_ASSIGNMENTS];
    prdb_sdk_select_v2 predicate_statement;
} prdb_sdk_update_v1;

typedef struct prdb_sdk_delete_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    const uint8_t *sql;
    uint64_t sql_len;
    prdb_sdk_sql_span_v1 relation;
    prdb_sdk_select_v2 predicate_statement;
} prdb_sdk_delete_v1;

int32_t prdb_sdk_sql_parse_insert_v1(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_insert_v1 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out);
int32_t prdb_sdk_sql_parse_update_v1(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_update_v1 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out);
int32_t prdb_sdk_sql_parse_delete_v1(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_delete_v1 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out);

#endif
