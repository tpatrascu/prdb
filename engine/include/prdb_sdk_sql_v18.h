#ifndef PRDB_SDK_SQL_V18_H
#define PRDB_SDK_SQL_V18_H

#include "prdb_sdk_sql_v17.h"
#include "prdb_sdk_sql_v2.h"

#define PRDB_SDK_SQL_ABI_V18_VERSION UINT32_C(18)
#define PRDB_SDK_SQL_V18_MAX_HAVING_NODES PRDB_SDK_SQL_MAX_PREDICATE_NODES
#define PRDB_SDK_SQL_BIND_V18_LITERAL_TYPE_MISMATCH \
    PRDB_SDK_SQL_BIND_V17_LITERAL_TYPE_MISMATCH
#define PRDB_SDK_SQL_V18_OPERAND_PARAMETER UINT32_C(1)
#define PRDB_SDK_SQL_V18_OPERAND_BIGINT_LITERAL UINT32_C(2)

typedef struct prdb_sdk_sql_having_node_v18 {
    uint32_t node_kind;
    uint32_t left_child;
    uint32_t right_child;
    uint32_t comparison;
    uint32_t operand_kind;
    uint32_t parameter_index;
    uint64_t literal;
} prdb_sdk_sql_having_node_v18;

typedef struct prdb_sdk_select_v18 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_v15 base;
    uint32_t having_node_count;
    uint32_t having_root;
    prdb_sdk_sql_having_node_v18
        having_nodes[PRDB_SDK_SQL_V18_MAX_HAVING_NODES];
} prdb_sdk_select_v18;

typedef struct prdb_sdk_select_binding_v18 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_select_binding_v15 base;
    uint32_t having_node_count;
    uint32_t having_root;
    uint32_t having_type_code;
    uint8_t having_decimal_precision;
    uint8_t having_decimal_scale;
    prdb_sdk_sql_having_node_v18
        having_nodes[PRDB_SDK_SQL_V18_MAX_HAVING_NODES];
} prdb_sdk_select_binding_v18;

int32_t prdb_sdk_sql_parse_select_v18(const uint8_t *sql, uint64_t sql_len,
                                      prdb_sdk_select_v18 *out,
                                      uint32_t out_capacity,
                                      uint32_t *error_offset_out);
int32_t prdb_sdk_sql_bind_select_v18(
    const prdb_sdk_select_v18 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v18 *out, uint32_t out_capacity);

#endif /* PRDB_SDK_SQL_V18_H */
