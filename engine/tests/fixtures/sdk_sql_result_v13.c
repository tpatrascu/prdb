#include "prdb_sdk_sql_result_v1.h"

#include <assert.h>
#include <string.h>

typedef struct capture {
    uint8_t bytes[128];
    uint64_t length;
    uint32_t calls;
} capture;

static int32_t write_result(void *context, const uint8_t *bytes,
                            uint64_t length) {
    capture *result = (capture *)context;
    assert(length <= sizeof(result->bytes) - result->length);
    memcpy(result->bytes + result->length, bytes, (size_t)length);
    result->length += length;
    ++result->calls;
    return PRDB_QUERY_CALLBACK_OK;
}

int main(void) {
    static const uint8_t id_name[] = "id", score_name[] = "score";
    static const uint8_t integer_one[] = {0, 0, 0, 1};
    static const uint8_t integer_nine[] = {0, 0, 0, 9};
    static const uint8_t integer_five[] = {0, 0, 0, 5};
    static const uint8_t bigint_eighteen[] = {0, 0, 0, 0, 0, 0, 0, 18};
    static const uint8_t decimal_average[16] = {
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 18, 168, 128};
    static const uint8_t expected[] = {
        'S', 'Q', 'L', 'R', 0, 1, 0, 3,
        0, 2, 'i', 'd', 0, 0, 0, PRDB_SDK_VALUE_TYPE_INTEGER,
        0, 0, PRDB_SDK_VALUE_STATE_VALUE, 0, 0, 0, 4, 0, 0, 0, 1,
        0, 5, 's', 'c', 'o', 'r', 'e', 0, 0, 0,
        PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, PRDB_SDK_VALUE_STATE_VALUE,
        0, 0, 0, 4, 0, 0, 0, 9,
        0, 5, 'c', 'o', 'u', 'n', 't', 0, 0, 0,
        PRDB_SDK_VALUE_TYPE_BIGINT, 0, 0, PRDB_SDK_VALUE_STATE_VALUE,
        0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 2};
    prdb_sdk_sql_column_shape_v2 columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_select_binding_v13 binding = {0};
    prdb_sdk_value_v1 keys[2] = {
        {sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
         PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
         integer_one, sizeof(integer_one)},
        {sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
         PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
         integer_nine, sizeof(integer_nine)}};
    uint8_t output[128] = {0};
    uint64_t output_len = 0;
    capture result = {{0}, 0, 0};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             &result, NULL, NULL, write_result, NULL};

    binding.struct_size = sizeof(binding);
    binding.abi_version = PRDB_SDK_SQL_ABI_V13_VERSION;
    binding.group_key_count = 2;
    binding.group_keys[0] = (prdb_sdk_sql_bound_column_v2){
        0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0};
    binding.group_keys[1] = (prdb_sdk_sql_bound_column_v2){
        1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0};
    binding.base.struct_size = sizeof(binding.base);
    binding.base.abi_version = PRDB_SDK_SQL_ABI_V7_VERSION;
    binding.base.has_group_by = 1;
    binding.base.aggregate_kind = PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR;

    assert(prdb_sdk_sql_encode_group_composite_result_v1(
               &binding, columns, 2, keys, 2, NULL, 2, output,
               sizeof(output), &output_len) == PRDB_SDK_SQL_RESULT_OK);
    assert(output_len == sizeof(expected));
    assert(memcmp(output, expected, sizeof(expected)) == 0);
    assert(prdb_sdk_sql_write_group_composite_result_v1(
               &host, &binding, columns, 2, keys, 2, NULL, 2, output,
               sizeof(output), &output_len) == PRDB_SDK_SQL_RESULT_OK);
    assert(result.calls == 1 && result.length == sizeof(expected));
    assert(memcmp(result.bytes, expected, sizeof(expected)) == 0);

    prdb_sdk_value_v1 aggregate = {
        sizeof(aggregate), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
        integer_five, sizeof(integer_five)};
    binding.base.aggregate_column =
        (prdb_sdk_sql_bound_column_v2){1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0};
    binding.base.aggregate_kind = PRDB_SDK_SQL_V7_AGGREGATE_MIN;
    assert(prdb_sdk_sql_encode_group_composite_result_v1(
               &binding, columns, 2, keys, 2, &aggregate, 2, output,
               sizeof(output), &output_len) == PRDB_SDK_SQL_RESULT_OK);
    assert(output_len == 69 && output[49] == 0 && output[50] == 3 &&
           memcmp(output + 51, "min", 3) == 0 && output[54] == 0 &&
           output[57] == PRDB_SDK_VALUE_TYPE_INTEGER);

    binding.base.aggregate_kind = PRDB_SDK_SQL_V7_AGGREGATE_MAX;
    assert(prdb_sdk_sql_encode_group_composite_result_v1(
               &binding, columns, 2, keys, 2, &aggregate, 2, output,
               sizeof(output), &output_len) == PRDB_SDK_SQL_RESULT_OK);
    assert(output_len == 69 && memcmp(output + 51, "max", 3) == 0);

    aggregate.type_code = PRDB_SDK_VALUE_TYPE_BIGINT;
    aggregate.data = bigint_eighteen;
    aggregate.data_len = sizeof(bigint_eighteen);
    binding.base.aggregate_kind = PRDB_SDK_SQL_V7_AGGREGATE_SUM;
    assert(prdb_sdk_sql_encode_group_composite_result_v1(
               &binding, columns, 2, keys, 2, &aggregate, 2, output,
               sizeof(output), &output_len) == PRDB_SDK_SQL_RESULT_OK);
    assert(output_len == 73 && memcmp(output + 51, "sum", 3) == 0 &&
           output[57] == PRDB_SDK_VALUE_TYPE_BIGINT);

    aggregate.type_code = PRDB_SDK_VALUE_TYPE_DECIMAL;
    aggregate.data = decimal_average;
    aggregate.data_len = sizeof(decimal_average);
    binding.base.aggregate_kind = PRDB_SDK_SQL_V7_AGGREGATE_AVG;
    assert(prdb_sdk_sql_encode_group_composite_result_v1(
               &binding, columns, 2, keys, 2, &aggregate, 2, output,
               sizeof(output), &output_len) == PRDB_SDK_SQL_RESULT_OK);
    assert(output_len == 81 && memcmp(output + 51, "avg", 3) == 0 &&
           output[57] == PRDB_SDK_VALUE_TYPE_DECIMAL && output[58] == 38 &&
           output[59] == 6);

    binding.base.aggregate_kind = PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR;
    binding.base.aggregate_column = (prdb_sdk_sql_bound_column_v2){0};
    binding.group_keys[1].ordinal = 0;
    output_len = 9;
    assert(prdb_sdk_sql_encode_group_composite_result_v1(
               &binding, columns, 2, keys, 2, NULL, 2, output,
               sizeof(output), &output_len) ==
           PRDB_SDK_SQL_RESULT_INVALID_BINDING);
    assert(output_len == 0);
    return 0;
}
