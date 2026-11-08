#include "prdb_sdk_sql_result_v1.h"

#include <assert.h>
#include <string.h>

static void put_u16(uint8_t *bytes, uint32_t offset, uint16_t value) {
    bytes[offset] = (uint8_t)(value >> 8);
    bytes[offset + 1] = (uint8_t)value;
}

static void put_u32(uint8_t *bytes, uint32_t offset, uint32_t value) {
    bytes[offset] = (uint8_t)(value >> 24);
    bytes[offset + 1] = (uint8_t)(value >> 16);
    bytes[offset + 2] = (uint8_t)(value >> 8);
    bytes[offset + 3] = (uint8_t)value;
}

int main(void) {
    static const uint8_t id_name[] = "id";
    static const uint8_t hidden_name[] = "hidden";
    uint8_t batch_bytes[34] = {0};
    uint8_t output[256] = {0};
    uint8_t too_small[8] = {0};
    uint64_t output_len = 99;
    prdb_sdk_batch_v1 batch = {0};
    prdb_sdk_row_iterator_v1 rows = {0};
    prdb_sdk_row_v1 row = {0};
    prdb_sdk_sql_column_shape_v2 columns[2] = {
        {id_name, sizeof(id_name) - 1, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {hidden_name, sizeof(hidden_name) - 1, 1,
         PRDB_SDK_VALUE_TYPE_TEXT, 0, 0}};
    prdb_sdk_select_binding_v2 binding = {0};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    static const uint8_t expected[] = {
        'S', 'Q', 'L', 'R', 0, 1, 0, 2,
        0, 6, 'h', 'i', 'd', 'd', 'e', 'n',
        0, 0, 0, 6, 0, 0, PRDB_SDK_VALUE_STATE_REDACTED, 0, 0, 0, 0,
        0, 2, 'i', 'd', 0, 0, 0, 3, 0, 0, PRDB_SDK_VALUE_STATE_VALUE,
        0, 0, 0, 4, 0, 0, 0, 42};

    put_u16(batch_bytes, 0, PRDB_SDK_BATCH_WIRE_V1_VERSION);
    batch_bytes[2] = 1;
    put_u16(batch_bytes, 3, 1);
    batch_bytes[5] = PRDB_SDK_BATCH_ROW_TOKEN_KIND;
    put_u16(batch_bytes, 21, 2);
    batch_bytes[23] = PRDB_SDK_VALUE_STATE_VALUE;
    batch_bytes[24] = PRDB_SDK_VALUE_TYPE_INTEGER;
    put_u32(batch_bytes, 25, 4);
    put_u32(batch_bytes, 29, 42);
    batch_bytes[33] = PRDB_SDK_VALUE_STATE_REDACTED;
    assert(prdb_sdk_batch_open_v1(&batch, sizeof(batch), batch_bytes,
                                  sizeof(batch_bytes)) == PRDB_SDK_BATCH_OK);
    assert(prdb_sdk_batch_rows_begin_v1(&batch, &rows, sizeof(rows)) ==
           PRDB_SDK_BATCH_OK);
    assert(prdb_sdk_batch_row_next_v1(&rows, &row, sizeof(row)) ==
           PRDB_SDK_BATCH_OK);

    binding.struct_size = sizeof(binding);
    binding.abi_version = PRDB_SDK_SQL_ABI_V2_VERSION;
    binding.projection_count = 2;
    binding.projection_ordinals[0] = 1;
    binding.projection_ordinals[1] = 0;
    binding.relation_column_count = 2;
    assert(prdb_sdk_sql_encode_result_row_v1(
               &binding, columns, 2, &row, cell_scratch, 2, output,
               sizeof(output), &output_len) == PRDB_SDK_SQL_RESULT_OK);
    assert(output_len == sizeof(expected));
    assert(memcmp(output, expected, sizeof(expected)) == 0);

    output_len = 99;
    assert(prdb_sdk_sql_encode_result_row_v1(
               &binding, columns, 2, &row, cell_scratch, 2, too_small,
               sizeof(too_small), &output_len) ==
           PRDB_SDK_SQL_RESULT_LIMIT_EXCEEDED);
    assert(output_len == 0);

    binding.projection_star = 1;
    binding.projection_count = 0;
    output_len = 0;
    assert(prdb_sdk_sql_encode_result_row_v1(
               &binding, columns, 2, &row, cell_scratch, 2, output,
               sizeof(output), &output_len) == PRDB_SDK_SQL_RESULT_OK);
    assert(output[6] == 0 && output[7] == 2);
    assert(output[8] == 0 && output[9] == 2 && output[10] == 'i');

    batch_bytes[33] = PRDB_SDK_VALUE_STATE_NULL;
    assert(prdb_sdk_batch_open_v1(&batch, sizeof(batch), batch_bytes,
                                  sizeof(batch_bytes)) == PRDB_SDK_BATCH_OK);
    assert(prdb_sdk_batch_rows_begin_v1(&batch, &rows, sizeof(rows)) ==
           PRDB_SDK_BATCH_OK);
    assert(prdb_sdk_batch_row_next_v1(&rows, &row, sizeof(row)) ==
           PRDB_SDK_BATCH_OK);
    binding.projection_star = 0;
    binding.projection_count = 2;
    binding.projection_ordinals[0] = 1;
    binding.projection_ordinals[1] = 0;
    assert(prdb_sdk_sql_encode_result_row_v1(
               &binding, columns, 2, &row, cell_scratch, 2, output,
               sizeof(output), &output_len) == PRDB_SDK_SQL_RESULT_OK);
    assert(output[22] == PRDB_SDK_VALUE_STATE_NULL);
    assert(output[16] == 0 && output[17] == 0 && output[18] == 0 &&
           output[19] == PRDB_SDK_VALUE_TYPE_TEXT);
    assert(output[23] == 0 && output[24] == 0 && output[25] == 0 &&
           output[26] == 0);

    binding.projection_ordinals[0] = 0;
    binding.projection_ordinals[1] = 0;
    assert(prdb_sdk_sql_encode_result_row_v1(
               &binding, columns, 2, &row, cell_scratch, 2, output,
               sizeof(output), &output_len) == PRDB_SDK_SQL_RESULT_OK);
    assert(output[8] == 0 && output[9] == 2 && output[10] == 'i');
    assert(output[27] == 0 && output[28] == 2 && output[29] == 'i');

    binding.projection_ordinals[0] = 2;
    output_len = 99;
    assert(prdb_sdk_sql_encode_result_row_v1(
               &binding, columns, 2, &row, cell_scratch, 2, output,
               sizeof(output), &output_len) ==
           PRDB_SDK_SQL_RESULT_INVALID_BINDING);
    assert(output_len == 0);

    binding.projection_ordinals[0] = 1;
    binding.projection_ordinals[1] = 0;
    columns[1].type_code = PRDB_SDK_VALUE_TYPE_DECIMAL;
    columns[1].decimal_precision = 10;
    columns[1].decimal_scale = 2;
    assert(prdb_sdk_sql_encode_result_row_v1(
               &binding, columns, 2, &row, cell_scratch, 2, output,
               sizeof(output), &output_len) == PRDB_SDK_SQL_RESULT_OK);
    assert(output[16] == 0 && output[17] == 0 && output[18] == 0 &&
           output[19] == PRDB_SDK_VALUE_TYPE_DECIMAL);
    assert(output[20] == 10 && output[21] == 2 &&
           output[22] == PRDB_SDK_VALUE_STATE_NULL);

    columns[1].type_code = PRDB_SDK_VALUE_TYPE_TEXT;
    columns[1].decimal_precision = 0;
    columns[1].decimal_scale = 0;
    columns[0].type_code = PRDB_SDK_VALUE_TYPE_BIGINT;
    output_len = 99;
    assert(prdb_sdk_sql_encode_result_row_v1(
               &binding, columns, 2, &row, cell_scratch, 2, output,
               sizeof(output), &output_len) == PRDB_SDK_SQL_RESULT_INVALID_ROW);
    assert(output_len == 0);
    return 0;
}
