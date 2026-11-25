#include "prdb_sdk_sql_v1.h"

#include <string.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    static const uint8_t bigint_boundary_seed[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING SUM(score) >= -9223372036854775808 AND "
        "SUM(score) <= 9223372036854775807\n";
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    prdb_sdk_sql_column_shape_v1 decimal_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 10, 2}};
    prdb_sdk_sql_column_shape_v1 integer_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_select_v1 statement;
    prdb_sdk_select_binding_v1 binding;
    uint32_t error_offset = 0;
    int32_t status;
    int is_bigint_boundary_seed =
        size == sizeof(bigint_boundary_seed) - 1 &&
        memcmp(data, bigint_boundary_seed, sizeof(bigint_boundary_seed) - 1) ==
            0;

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
         statement.having_root >= statement.having_node_count))
        __builtin_trap();
    const prdb_sdk_sql_column_shape_v1 *shapes[] = {
        decimal_columns, integer_columns};
    const uint32_t expected_having_types[] = {
        PRDB_SDK_VALUE_TYPE_DECIMAL, PRDB_SDK_VALUE_TYPE_BIGINT};
    /* Exercise both numeric result paths; a Decimal-only schema leaves the
     * BIGINT sign-extension and range checks unreachable under fuzzing. */
    for (size_t shape_index = 0;
         shape_index < sizeof(shapes) / sizeof(shapes[0]); ++shape_index) {
        memset(&binding, 0x5a, sizeof(binding));
        status = prdb_sdk_sql_bind_select_v1(
            &statement,
            data + statement.relation.offset,
            statement.relation.length,
            shapes[shape_index], 2, &binding, sizeof(binding));
        if (status != PRDB_SDK_SQL_BIND_OK) {
            const uint8_t *bytes = (const uint8_t *)&binding;
            for (size_t index = 0; index < sizeof(binding); ++index)
                if (bytes[index] != 0x5a) __builtin_trap();
            if (is_bigint_boundary_seed && shape_index == 1)
                __builtin_trap();
            continue;
        }
        if (binding.struct_size != sizeof(binding) ||
            binding.abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
            binding.having_node_count !=
                statement.having_node_count)
            __builtin_trap();
        if (binding.having_node_count != 0 &&
            binding.having_type_code != expected_having_types[shape_index])
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
            if (binding.having_type_code ==
                PRDB_SDK_VALUE_TYPE_DECIMAL) {
                if (binding.having_decimal_precision == 0 ||
                    binding.having_decimal_precision > 38 ||
                    binding.having_decimal_scale >
                        binding.having_decimal_precision ||
                    node->scale != binding.having_decimal_scale)
                    __builtin_trap();
            } else if (binding.having_type_code ==
                       PRDB_SDK_VALUE_TYPE_BIGINT) {
                uint8_t extension =
                    (node->coefficient[8] & 0x80) != 0 ? 0xff : 0;
                if (node->scale != 0) __builtin_trap();
                for (uint32_t byte = 0; byte < 8; ++byte)
                    if (node->coefficient[byte] != extension)
                        __builtin_trap();
            } else {
                __builtin_trap();
            }
        }
    }
    return 0;
}
