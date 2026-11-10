#include "prdb_sdk_sql_v18.h"

#include <assert.h>
#include <stdint.h>
#include <stddef.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    static const uint8_t relation[] = "records";
    static const uint8_t key_name[] = "key";
    static const uint8_t value_name[] = "value";
    static const prdb_sdk_sql_column_shape_v2 columns[] = {
        {key_name, 3, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {value_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_select_v18 statement = {0};
    prdb_sdk_select_binding_v18 binding = {0};
    uint32_t error_offset = 0;
    int32_t status;

    if (data == NULL || size > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        size > UINT32_MAX) {
        return 0;
    }
    status = prdb_sdk_sql_parse_select_v18(
        data, (uint64_t)size, &statement, sizeof(statement), &error_offset);
    if (status != PRDB_SDK_SQL_OK) {
        return 0;
    }

    assert(statement.struct_size == sizeof(statement));
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V18_VERSION);
    assert(statement.having_node_count <=
           PRDB_SDK_SQL_V18_MAX_HAVING_NODES);
    if (statement.having_node_count != 0) {
        assert(statement.having_root < statement.having_node_count);
    }

    status = prdb_sdk_sql_bind_select_v18(
        &statement, relation, sizeof(relation) - 1, columns,
        (uint32_t)(sizeof(columns) / sizeof(columns[0])), &binding,
        sizeof(binding));
    if (status == PRDB_SDK_SQL_BIND_OK) {
        assert(binding.struct_size == sizeof(binding));
        assert(binding.abi_version == PRDB_SDK_SQL_ABI_V18_VERSION);
        assert(binding.having_node_count == statement.having_node_count);
        if (binding.having_node_count != 0) {
            assert(binding.having_root < binding.having_node_count);
        }
    }
    return 0;
}
