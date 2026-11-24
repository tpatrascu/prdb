#ifndef PRDB_SDK_SQL_V26_H
#define PRDB_SDK_SQL_V26_H

#include "prdb_sdk_sql_v2.h"

#define PRDB_SDK_SQL_ABI_V26_VERSION UINT32_C(26)
#define PRDB_SDK_SQL_V26_MAX_IN_VALUES UINT32_C(32)

/* V26 adds a parameter-list IN predicate to single-relation SELECT. The
 * parser lowers IN to the existing Boolean predicate tree, preserving the
 * frozen v2 AST and optimizer request formats. */
typedef struct prdb_sdk_select_v26 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_v2 base;
} prdb_sdk_select_v26;

int32_t prdb_sdk_sql_parse_select_v26(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v26 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out);

#endif /* PRDB_SDK_SQL_V26_H */
