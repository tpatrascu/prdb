#ifndef PRDB_SDK_SQL_V2_H
#define PRDB_SDK_SQL_V2_H

#include "prdb_sdk_sql_v1.h"

#define PRDB_SDK_SQL_ABI_V2_VERSION UINT32_C(2)
#define PRDB_SDK_SQL_MAX_PREDICATE_NODES UINT32_C(64)
#define PRDB_SDK_SQL_MAX_EXPRESSION_DEPTH UINT32_C(32)

#define PRDB_SDK_SQL_PREDICATE_COMPARE UINT32_C(1)
#define PRDB_SDK_SQL_PREDICATE_IS_NULL UINT32_C(2)
#define PRDB_SDK_SQL_PREDICATE_IS_NOT_NULL UINT32_C(3)
#define PRDB_SDK_SQL_PREDICATE_AND UINT32_C(4)
#define PRDB_SDK_SQL_PREDICATE_OR UINT32_C(5)
#define PRDB_SDK_SQL_PREDICATE_NOT UINT32_C(6)

typedef struct prdb_sdk_predicate_node_v2 {
    uint32_t node_kind;
    uint32_t left_child;
    uint32_t right_child;
    uint32_t comparison;
    prdb_sdk_sql_span_v1 column;
    uint32_t parameter_index;
} prdb_sdk_predicate_node_v2;

typedef struct prdb_sdk_select_v2 {
    uint32_t struct_size;
    uint32_t abi_version;
    const uint8_t *sql;
    uint64_t sql_len;
    prdb_sdk_sql_span_v1 relation;
    prdb_sdk_sql_span_v1 projection[PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS];
    uint32_t projection_count;
    uint32_t projection_star;
    uint32_t predicate_count;
    uint32_t predicate_root;
    prdb_sdk_predicate_node_v2
        predicates[PRDB_SDK_SQL_MAX_PREDICATE_NODES];
    uint32_t has_limit;
    uint32_t limit;
} prdb_sdk_select_v2;

/* Version 2 preserves v1 and adds a bounded Boolean predicate tree. */
int32_t prdb_sdk_sql_parse_select_v2(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_select_v2 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out);

#endif /* PRDB_SDK_SQL_V2_H */
