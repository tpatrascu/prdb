#include "prdb_sdk_sql_v1.h"

#include <assert.h>
#include <stddef.h>
#include <stdint.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    static const uint8_t left_name[] = "lefts";
    static const uint8_t right_name[] = "rights";
    static const uint8_t id_name[] = "id";
    static const uint8_t region_name[] = "region";
    static const uint8_t label_name[] = "label";
    static const uint8_t value_name[] = "value";
    static const prdb_sdk_sql_column_shape_v1 left_columns[] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {region_name, 6, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {label_name, 5, 2, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    static const prdb_sdk_sql_column_shape_v1 right_columns[] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {region_name, 6, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {value_name, 5, 2, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_select_v1 statement = {0};
    prdb_sdk_select_binding_v1 binding = {0};
    uint32_t error_offset = 0;
    int32_t status;

    if (data == NULL || size > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        size > UINT32_MAX) {
        return 0;
    }
    status = prdb_sdk_sql_parse_select_v1(
        data, (uint64_t)size, &statement, sizeof(statement), &error_offset);
    if (status != PRDB_SDK_SQL_OK) {
        assert(error_offset <= size);
        return 0;
    }

    assert(statement.struct_size == sizeof(statement));
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V1_VERSION);
    assert(statement.struct_size == sizeof(statement));
    assert(statement.abi_version == PRDB_SDK_SQL_ABI_V1_VERSION);
    assert(statement.join_key_count >= 1);
    assert(statement.join_key_count <=
           PRDB_SDK_SQL_MAX_JOIN_KEYS);
    assert(statement.join_projection_count > 0);
    assert(statement.join_projection_count <=
           PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS);
    assert(statement.left_alias.length != 0);
    assert(statement.left_alias.offset <= size);
    assert(statement.left_alias.length <= size - statement.left_alias.offset);
    assert(statement.right_alias.length != 0);
    assert(statement.right_alias.offset <= size);
    assert(statement.right_alias.length <= size - statement.right_alias.offset);
    if (statement.has_join_where != 0) {
        assert(statement.has_join_where == 1);
        assert(statement.where_keyword_offset < size);
        assert(statement.from_keyword_offset <
               statement.where_keyword_offset);
        assert(statement.where_clause_end > statement.where_keyword_offset);
        assert(statement.where_clause_end <= size);
        assert(statement.join_where_column.relation.offset <= size);
        assert(statement.join_where_column.relation.length != 0);
        assert(statement.join_where_column.relation.length <=
               size - statement.join_where_column.relation.offset);
        assert(statement.join_where_column.column.offset <= size);
        assert(statement.join_where_column.column.length != 0);
        assert(statement.join_where_column.column.length <=
               size - statement.join_where_column.column.offset);
        assert(statement.join_where_comparison >= PRDB_SDK_SQL_COMPARE_EQUAL);
        assert(statement.join_where_comparison <= PRDB_SDK_SQL_COMPARE_IS_NOT_NULL);
        if (statement.join_where_comparison == PRDB_SDK_SQL_COMPARE_IS_NULL ||
            statement.join_where_comparison == PRDB_SDK_SQL_COMPARE_IS_NOT_NULL) {
            assert(statement.join_where_parameter_index == 0);
        } else {
            assert(statement.join_where_parameter_index > 0);
            assert(statement.join_where_parameter_index <= UINT16_MAX);
        }
    } else {
        assert(statement.join_where_comparison == 0);
        assert(statement.join_where_parameter_index == 0);
        assert(statement.where_keyword_offset == 0);
        assert(statement.where_clause_end == 0);
    }

    status = prdb_sdk_sql_bind_join_select_v1(
        &statement, left_name, sizeof(left_name) - 1, left_columns,
        (uint32_t)(sizeof(left_columns) / sizeof(left_columns[0])), right_name,
        sizeof(right_name) - 1, right_columns,
        (uint32_t)(sizeof(right_columns) / sizeof(right_columns[0])), &binding,
        sizeof(binding));
    if (status == PRDB_SDK_SQL_BIND_OK) {
        assert(binding.struct_size == sizeof(binding));
        assert(binding.abi_version == PRDB_SDK_SQL_ABI_V1_VERSION);
        assert(binding.join_key_count ==
               statement.join_key_count);
        assert(binding.output_projection_count ==
               statement.join_projection_count);
        assert(binding.join_projection_count ==
               binding.output_projection_count + statement.has_join_where);
        if (statement.has_join_where != 0) {
            assert(binding.where_projection_index ==
                   binding.output_projection_count);
            assert(binding.where_relation_side <= 1);
            if (binding.where_relation_side == 0) {
                assert(binding.where_column.ordinal <
                       sizeof(left_columns) / sizeof(left_columns[0]));
                assert(binding.where_column.type_code ==
                       left_columns[binding.where_column.ordinal].type_code);
            } else {
                assert(binding.where_column.ordinal <
                       sizeof(right_columns) / sizeof(right_columns[0]));
                assert(binding.where_column.type_code ==
                       right_columns[binding.where_column.ordinal].type_code);
            }
        }
    }
    return 0;
}
