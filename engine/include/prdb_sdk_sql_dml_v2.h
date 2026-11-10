#ifndef PRDB_SDK_SQL_DML_V2_H
#define PRDB_SDK_SQL_DML_V2_H

#include "prdb_sdk_sql_dml_v1.h"

#define PRDB_SDK_SQL_DML_ABI_V2_VERSION UINT32_C(2)
#define PRDB_SDK_SQL_MAX_INSERT_ROWS_V2 UINT32_C(127)

typedef struct prdb_sdk_insert_v2 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_insert_v1 base;
    uint32_t row_count;
    uint32_t values_per_row;
} prdb_sdk_insert_v2;

/* Parses one to 127 positional VALUES tuples. Every tuple has the same
 * positive number of cells, with at most 4096 cells total. Cells retain the
 * v1 parameter, NULL, and DEFAULT forms. V1 remains single-row-only. */
int32_t prdb_sdk_sql_parse_insert_v2(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_insert_v2 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out);

#endif /* PRDB_SDK_SQL_DML_V2_H */
