#include "prdb_sdk_sql_v1.h"

#include <string.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    prdb_sdk_sql_column_shape_v1 columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 10, 2}};
    prdb_sdk_select_v1 statement;
    prdb_sdk_select_binding_v1 binding;
    uint32_t error_offset = 0;
    int32_t status;

    if (size > PRDB_SDK_SQL_MAX_QUERY_BYTES) return 0;
    memset(&statement, 0xa5, sizeof(statement));
    status = prdb_sdk_sql_parse_select_v1(
        data, size, &statement, sizeof(statement), &error_offset);
    if (status != PRDB_SDK_SQL_OK) {
        const uint8_t *bytes = (const uint8_t *)&statement;
        for (size_t index = 0; index < sizeof(statement); ++index)
            if (bytes[index] != 0xa5) __builtin_trap();
        return 0;
    }
    if (statement.struct_size != sizeof(statement) ||
        statement.abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement.having_node_count >
            PRDB_SDK_SQL_MAX_HAVING_NODES ||
        (statement.having_node_count == 0 &&
         statement.having_root != 0) ||
        (statement.having_node_count != 0 &&
         statement.having_root >=
             statement.having_node_count))
        __builtin_trap();
    uint64_t comparison_mask = 0;
    for (uint32_t index = 0; index < statement.having_node_count;
         ++index) {
        uint32_t kind = statement.having_nodes[index].node_kind;
        if (kind == PRDB_SDK_SQL_PREDICATE_COMPARE ||
            kind == PRDB_SDK_SQL_PREDICATE_IS_NULL ||
            kind == PRDB_SDK_SQL_PREDICATE_IS_NOT_NULL)
            comparison_mask |= UINT64_C(1) << index;
    }
    if ((statement.having_count_mask & ~comparison_mask) != 0)
        __builtin_trap();

    memset(&binding, 0x5a, sizeof(binding));
    status = prdb_sdk_sql_bind_select_v1(
        &statement,
        data + statement.relation.offset,
        statement.relation.length, columns, 2,
        &binding, sizeof(binding));
    if (status != PRDB_SDK_SQL_BIND_OK) {
        const uint8_t *bytes = (const uint8_t *)&binding;
        for (size_t index = 0; index < sizeof(binding); ++index)
            if (bytes[index] != 0x5a) __builtin_trap();
        return 0;
    }
    if (binding.struct_size != sizeof(binding) ||
        binding.abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        binding.having_node_count !=
            statement.having_node_count ||
        binding.having_count_mask != statement.having_count_mask)
        __builtin_trap();
    for (uint32_t index = 0; index < binding.having_node_count;
         ++index) {
        const prdb_sdk_sql_having_node_v1 *node =
            &binding.having_nodes[index];
        if (node->operand_kind !=
            PRDB_SDK_SQL_HAVING_OPERAND_NUMERIC_LITERAL)
            continue;
        if (node->node_kind != PRDB_SDK_SQL_PREDICATE_COMPARE ||
            node->parameter_index != 0 || node->literal != 0)
            __builtin_trap();
        if ((binding.having_count_mask & (UINT64_C(1) << index)) != 0) {
            uint8_t extension = (node->coefficient[8] & 0x80) != 0 ? 0xff : 0;
            if (node->scale != 0) __builtin_trap();
            for (uint32_t byte = 0; byte < 8; ++byte)
                if (node->coefficient[byte] != extension) __builtin_trap();
        } else if (binding.having_type_code ==
                   PRDB_SDK_VALUE_TYPE_DECIMAL) {
            if (node->scale != binding.having_decimal_scale)
                __builtin_trap();
        } else {
            __builtin_trap();
        }
    }
    return 0;
}
