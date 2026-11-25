#include "prdb_sdk_sql_v1.h"

#include <string.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    static const prdb_sdk_sql_column_shape_v1 columns[] = {
        {(const uint8_t *)"id", 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {(const uint8_t *)"score", 5, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 10, 2}};
    prdb_sdk_select_v1 statement;
    prdb_sdk_select_binding_v1 binding;
    uint32_t offset = 0;

    if (size > PRDB_SDK_SQL_MAX_QUERY_BYTES) return 0;
    memset(&statement, 0xa5, sizeof(statement));
    if (prdb_sdk_sql_parse_select_v1(
            data, size, &statement, sizeof(statement), &offset) !=
            PRDB_SDK_SQL_OK) {
        const uint8_t *bytes = (const uint8_t *)&statement;
        for (size_t i = 0; i < sizeof(statement); ++i)
            if (bytes[i] != 0xa5) __builtin_trap();
        return 0;
    }
    const prdb_sdk_select_v1 *grouped = &statement;
    const prdb_sdk_sql_span_v1 relation = grouped->relation;
    if (statement.abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        grouped->group_key_count == 0 ||
        grouped->group_key_count > PRDB_SDK_SQL_MAX_GROUP_KEYS ||
        relation.offset > size || relation.length > size - relation.offset)
        __builtin_trap();
    if (grouped->group_key_count == 1) {
        prdb_sdk_select_v1 legacy;
        if (prdb_sdk_sql_parse_select_v1(
                data, size, &legacy, sizeof(legacy), NULL) == PRDB_SDK_SQL_OK)
            __builtin_trap();
    }
    memset(&binding, 0x5a, sizeof(binding));
    if (prdb_sdk_sql_bind_select_v1(
            &statement, data + relation.offset, relation.length, columns, 2,
            &binding, sizeof(binding)) != PRDB_SDK_SQL_BIND_OK) {
        const uint8_t *bytes = (const uint8_t *)&binding;
        for (size_t i = 0; i < sizeof(binding); ++i)
            if (bytes[i] != 0x5a) __builtin_trap();
        return 0;
    }
    if (binding.abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        binding.group_key_count !=
            grouped->group_key_count ||
        binding.having_count_mask != statement.having_count_mask)
        __builtin_trap();
    return 0;
}
