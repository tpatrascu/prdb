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

typedef struct result_capture {
    uint8_t bytes[256];
    uint64_t length;
    uint32_t calls;
    uint32_t fail;
} result_capture;

static int32_t write_result(void *context, const uint8_t *bytes,
                            uint64_t length) {
    result_capture *capture = (result_capture *)context;
    assert(length <= PRDB_QUERY_MAX_RESULT_CHUNK_BYTES);
    if (capture->fail) return PRDB_QUERY_CALLBACK_IO_ERROR;
    assert(length <= sizeof(capture->bytes) - capture->length);
    memcpy(capture->bytes + capture->length, bytes, (size_t)length);
    capture->length += length;
    ++capture->calls;
    return PRDB_QUERY_CALLBACK_OK;
}

typedef struct chunk_counter {
    uint64_t total;
    uint64_t last_chunk;
    uint32_t calls;
    uint32_t fail_on_call;
} chunk_counter;

static int32_t count_result_chunk(void *context, const uint8_t *bytes,
                                  uint64_t length) {
    chunk_counter *counter = (chunk_counter *)context;
    assert(bytes != NULL && length > 0);
    assert(length <= PRDB_QUERY_MAX_RESULT_CHUNK_BYTES);
    if (counter->fail_on_call != 0 &&
        counter->calls + 1 == counter->fail_on_call) {
        return PRDB_QUERY_CALLBACK_IO_ERROR;
    }
    counter->total += length;
    counter->last_chunk = length;
    ++counter->calls;
    return PRDB_QUERY_CALLBACK_OK;
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
    result_capture capture = {{0}, 0, 0, 0};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             &capture, NULL, NULL, write_result, NULL};
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
    assert(prdb_sdk_sql_write_result_row_v1(
               &host, &binding, columns, 2, &row, cell_scratch, 2, output,
               sizeof(output), &output_len) == PRDB_SDK_SQL_RESULT_OK);
    assert(capture.calls == 1 && capture.length == sizeof(expected));
    assert(memcmp(capture.bytes, expected, sizeof(expected)) == 0);
    capture.fail = 1;
    capture.calls = 0;
    capture.length = 0;
    assert(prdb_sdk_sql_write_result_row_v1(
               &host, &binding, columns, 2, &row, cell_scratch, 2, output,
               sizeof(output), &output_len) ==
           PRDB_SDK_SQL_RESULT_WRITE_FAILED);
    assert(capture.calls == 0 && capture.length == 0 && output_len == 0);
    capture.fail = 0;

    {
        enum { LARGE_TEXT_BYTES = 17000 };
        static const uint8_t text_name[] = "text";
        uint8_t large_batch[29 + LARGE_TEXT_BYTES] = {0};
        uint8_t large_output[18000] = {0};
        prdb_sdk_sql_column_shape_v2 text_column = {
            text_name, sizeof(text_name) - 1, 0, PRDB_SDK_VALUE_TYPE_TEXT, 0, 0};
        prdb_sdk_select_binding_v2 text_binding = {0};
        chunk_counter counter = {0};
        prdb_host_api_v3 chunk_host = {sizeof(chunk_host),
                                       PRDB_QUERY_ABI_V3_VERSION, &counter,
                                       NULL, NULL, count_result_chunk, NULL};
        prdb_sdk_batch_v1 text_batch = {0};
        prdb_sdk_row_iterator_v1 text_rows = {0};
        prdb_sdk_row_v1 text_row = {0};
        prdb_sdk_value_v1 text_scratch[1] = {{0}};
        uint64_t text_output_len = 0;

        put_u16(large_batch, 0, PRDB_SDK_BATCH_WIRE_V1_VERSION);
        large_batch[2] = 1;
        put_u16(large_batch, 3, 1);
        large_batch[5] = PRDB_SDK_BATCH_ROW_TOKEN_KIND;
        put_u16(large_batch, 21, 1);
        large_batch[23] = PRDB_SDK_VALUE_STATE_VALUE;
        large_batch[24] = PRDB_SDK_VALUE_TYPE_TEXT;
        put_u32(large_batch, 25, LARGE_TEXT_BYTES);
        memset(large_batch + 29, 'x', LARGE_TEXT_BYTES);
        assert(prdb_sdk_batch_open_v1(&text_batch, sizeof(text_batch),
                                      large_batch, sizeof(large_batch)) ==
               PRDB_SDK_BATCH_OK);
        assert(prdb_sdk_batch_rows_begin_v1(&text_batch, &text_rows,
                                            sizeof(text_rows)) ==
               PRDB_SDK_BATCH_OK);
        assert(prdb_sdk_batch_row_next_v1(&text_rows, &text_row,
                                          sizeof(text_row)) ==
               PRDB_SDK_BATCH_OK);
        text_binding.struct_size = sizeof(text_binding);
        text_binding.abi_version = PRDB_SDK_SQL_ABI_V2_VERSION;
        text_binding.projection_count = 1;
        text_binding.projection_ordinals[0] = 0;
        text_binding.relation_column_count = 1;
        assert(prdb_sdk_sql_write_result_row_v1(
                   &chunk_host, &text_binding, &text_column, 1, &text_row,
                   text_scratch, 1, large_output, sizeof(large_output),
                   &text_output_len) == PRDB_SDK_SQL_RESULT_OK);
        assert(text_output_len == 17025);
        assert(counter.calls == 2 && counter.total == text_output_len);
        assert(counter.last_chunk ==
               text_output_len - PRDB_QUERY_MAX_RESULT_CHUNK_BYTES);
        counter.calls = 0;
        counter.total = 0;
        counter.last_chunk = 0;
        counter.fail_on_call = 2;
        assert(prdb_sdk_sql_write_result_row_v1(
                   &chunk_host, &text_binding, &text_column, 1, &text_row,
                   text_scratch, 1, large_output, sizeof(large_output),
                   &text_output_len) == PRDB_SDK_SQL_RESULT_WRITE_FAILED);
        assert(text_output_len == 0 && counter.calls == 1);
        assert(counter.total == PRDB_QUERY_MAX_RESULT_CHUNK_BYTES);
    }

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

    {
        static const uint8_t count_name[] = "id";
        static const uint8_t count_key_bytes[] = {0, 0, 0, 42};
        static const uint8_t expected_group[] = {
            'S', 'Q', 'L', 'R', 0, 1, 0, 2,
            0, 2, 'i', 'd',
            0, 0, 0, PRDB_SDK_VALUE_TYPE_INTEGER,
            0, 0, PRDB_SDK_VALUE_STATE_VALUE, 0, 0, 0, 4,
            0, 0, 0, 42,
            0, 5, 'c', 'o', 'u', 'n', 't',
            0, 0, 0, PRDB_SDK_VALUE_TYPE_BIGINT,
            0, 0, PRDB_SDK_VALUE_STATE_VALUE, 0, 0, 0, 8,
            0, 0, 0, 0, 0, 0, 0, 2};
        prdb_sdk_sql_column_shape_v2 group_column = {
            count_name, sizeof(count_name) - 1, 0,
            PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0};
        prdb_sdk_value_v1 group_key = {
            sizeof(group_key), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
            count_key_bytes, sizeof(count_key_bytes)};

        output_len = 99;
        assert(prdb_sdk_sql_encode_group_count_result_v1(
                   &group_column, &group_key, 2, output, sizeof(output),
                   &output_len) == PRDB_SDK_SQL_RESULT_OK);
        assert(output_len == sizeof(expected_group));
        assert(memcmp(output, expected_group, sizeof(expected_group)) == 0);
        capture.calls = 0;
        capture.length = 0;
        assert(prdb_sdk_sql_write_group_count_result_v1(
                   &host, &group_column, &group_key, 2, output,
                   sizeof(output), &output_len) == PRDB_SDK_SQL_RESULT_OK);
        assert(capture.calls == 1 && capture.length == sizeof(expected_group));
        assert(memcmp(capture.bytes, expected_group,
                      sizeof(expected_group)) == 0);
        capture.fail = 1;
        capture.calls = 0;
        capture.length = 0;
        assert(prdb_sdk_sql_write_group_count_result_v1(
                   &host, &group_column, &group_key, 2, output,
                   sizeof(output), &output_len) ==
               PRDB_SDK_SQL_RESULT_WRITE_FAILED);
        assert(capture.calls == 0 && capture.length == 0 && output_len == 0);
        capture.fail = 0;
        output_len = 99;
        assert(prdb_sdk_sql_encode_group_count_result_v1(
                   &group_column, &group_key, 2, too_small,
                   sizeof(too_small), &output_len) ==
               PRDB_SDK_SQL_RESULT_LIMIT_EXCEEDED);
        assert(output_len == 0);
        assert(prdb_sdk_sql_encode_group_count_result_v1(
                   &group_column, &group_key, 0, output, sizeof(output),
                   &output_len) == PRDB_SDK_SQL_RESULT_INVALID_ROW);
        assert(output_len == 0);
        group_key = (prdb_sdk_value_v1){
            sizeof(group_key), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_NULL, 0, NULL, 0};
        assert(prdb_sdk_sql_encode_group_count_result_v1(
                   &group_column, &group_key, 2, output, sizeof(output),
                   &output_len) == PRDB_SDK_SQL_RESULT_OK);
        assert(output[18] == PRDB_SDK_VALUE_STATE_NULL &&
               output[19] == 0 && output[20] == 0 && output[21] == 0 &&
               output[22] == 0);
        group_key = (prdb_sdk_value_v1){
            sizeof(group_key), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_REDACTED, 0, NULL, 0};
        assert(prdb_sdk_sql_encode_group_count_result_v1(
                   &group_column, &group_key, 2, output, sizeof(output),
                   &output_len) == PRDB_SDK_SQL_RESULT_OK);
        assert(output[18] == PRDB_SDK_VALUE_STATE_REDACTED &&
               output[19] == 0 && output[20] == 0 && output[21] == 0 &&
               output[22] == 0);
    }

    {
        enum { GROUP_TEXT_BYTES = 17000 };
        static const uint8_t label_name[] = "label";
        uint8_t group_text[GROUP_TEXT_BYTES];
        uint8_t grouped_output[18000] = {0};
        prdb_sdk_sql_column_shape_v2 label_column = {
            label_name, sizeof(label_name) - 1, 0,
            PRDB_SDK_VALUE_TYPE_TEXT, 0, 0};
        prdb_sdk_value_v1 label_key = {
            sizeof(label_key), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_TEXT,
            group_text, sizeof(group_text)};
        chunk_counter counter = {0};
        prdb_host_api_v3 chunk_host = {sizeof(chunk_host),
                                       PRDB_QUERY_ABI_V3_VERSION, &counter,
                                       NULL, NULL, count_result_chunk, NULL};
        uint64_t grouped_output_len = 0;

        memset(group_text, 'x', sizeof(group_text));
        assert(prdb_sdk_sql_write_group_count_result_v1(
                   &chunk_host, &label_column, &label_key, 2, grouped_output,
                   sizeof(grouped_output), &grouped_output_len) ==
               PRDB_SDK_SQL_RESULT_OK);
        assert(grouped_output_len == 17052 && counter.calls == 2 &&
               counter.total == grouped_output_len &&
               counter.last_chunk ==
                   grouped_output_len - PRDB_QUERY_MAX_RESULT_CHUNK_BYTES);
        counter.calls = 0;
        counter.total = 0;
        counter.last_chunk = 0;
        counter.fail_on_call = 2;
        assert(prdb_sdk_sql_write_group_count_result_v1(
                   &chunk_host, &label_column, &label_key, 2, grouped_output,
                   sizeof(grouped_output), &grouped_output_len) ==
               PRDB_SDK_SQL_RESULT_WRITE_FAILED);
        assert(grouped_output_len == 0 && counter.calls == 1 &&
               counter.total == PRDB_QUERY_MAX_RESULT_CHUNK_BYTES);
    }
    {
        static const uint8_t score_name[] = "score";
        static const uint8_t key_bytes[] = {0, 0, 0, 42};
        static const uint8_t min_bytes[] = {0, 0, 0, 3};
        static const uint8_t expected_min[] = {
            'S', 'Q', 'L', 'R', 0, 1, 0, 2,
            0, 2, 'i', 'd', 0, 0, 0, PRDB_SDK_VALUE_TYPE_INTEGER,
            0, 0, PRDB_SDK_VALUE_STATE_VALUE, 0, 0, 0, 4,
            0, 0, 0, 42,
            0, 3, 'm', 'i', 'n', 0, 0, 0, PRDB_SDK_VALUE_TYPE_INTEGER,
            0, 0, PRDB_SDK_VALUE_STATE_VALUE, 0, 0, 0, 4,
            0, 0, 0, 3};
        prdb_sdk_sql_column_shape_v2 group_column = {
            id_name, sizeof(id_name) - 1, 0,
            PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0};
        prdb_sdk_sql_column_shape_v2 aggregate_column = {
            score_name, sizeof(score_name) - 1, 1,
            PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0};
        prdb_sdk_value_v1 group_key = {
            sizeof(group_key), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
            key_bytes, sizeof(key_bytes)};
        prdb_sdk_value_v1 aggregate_value = {
            sizeof(aggregate_value), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
            min_bytes, sizeof(min_bytes)};

        assert(prdb_sdk_sql_encode_group_minmax_result_v1(
                   &group_column, &group_key, &aggregate_column,
                   &aggregate_value, PRDB_SDK_SQL_V5_AGGREGATE_MIN, output,
                   sizeof(output), &output_len) == PRDB_SDK_SQL_RESULT_OK);
        assert(output_len == sizeof(expected_min));
        assert(memcmp(output, expected_min, sizeof(expected_min)) == 0);
        {
            static const uint8_t sum_bytes[] = {0, 0, 0, 0, 0, 0, 0, 12};
            prdb_sdk_value_v1 sum_value = {
                sizeof(sum_value), PRDB_SDK_VALUE_ABI_V1_VERSION,
                PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BIGINT,
                sum_bytes, sizeof(sum_bytes)};
            assert(prdb_sdk_sql_encode_group_sum_result_v1(
                       &group_column, &group_key, &aggregate_column,
                       &sum_value, output, sizeof(output), &output_len) ==
                   PRDB_SDK_SQL_RESULT_OK);
            assert(output[27] == 0 && output[28] == 3 &&
                   memcmp(output + 29, "sum", 3) == 0 &&
                   output[35] == PRDB_SDK_VALUE_TYPE_BIGINT &&
                   output[36] == 0 && output[37] == 0 &&
                   output[38] == PRDB_SDK_VALUE_STATE_VALUE &&
                   output[42] == 8 && output_len == 51);
            capture.calls = 0;
            capture.length = 0;
            assert(prdb_sdk_sql_write_group_sum_result_v1(
                       &host, &group_column, &group_key, &aggregate_column,
                       &sum_value, output, sizeof(output), &output_len) ==
                   PRDB_SDK_SQL_RESULT_OK);
            assert(capture.calls == 1 && capture.length == output_len &&
                   memcmp(capture.bytes, output, output_len) == 0);
        }
        capture.calls = 0;
        capture.length = 0;
        assert(prdb_sdk_sql_write_group_minmax_result_v1(
                   &host, &group_column, &group_key, &aggregate_column,
                   &aggregate_value, PRDB_SDK_SQL_V5_AGGREGATE_MIN, output,
                   sizeof(output), &output_len) == PRDB_SDK_SQL_RESULT_OK);
        assert(capture.calls == 1 && capture.length == sizeof(expected_min));
        assert(memcmp(capture.bytes, expected_min, sizeof(expected_min)) == 0);
        assert(prdb_sdk_sql_encode_group_minmax_result_v1(
                   &group_column, &group_key, &aggregate_column,
                   &aggregate_value, PRDB_SDK_SQL_V5_AGGREGATE_MAX, output,
                   sizeof(output), &output_len) == PRDB_SDK_SQL_RESULT_OK);
        assert(output[27] == 0 && output[28] == 3 && output[29] == 'm' &&
               output[30] == 'a' && output[31] == 'x' &&
               output[43] == 0 && output[44] == 0 && output[45] == 0 &&
               output[46] == 3);
        capture.fail = 1;
        capture.calls = 0;
        capture.length = 0;
        assert(prdb_sdk_sql_write_group_minmax_result_v1(
                   &host, &group_column, &group_key, &aggregate_column,
                   &aggregate_value, PRDB_SDK_SQL_V5_AGGREGATE_MAX, output,
                   sizeof(output), &output_len) ==
               PRDB_SDK_SQL_RESULT_WRITE_FAILED);
        assert(capture.calls == 0 && capture.length == 0 && output_len == 0);
        capture.fail = 0;

        aggregate_value = (prdb_sdk_value_v1){
            sizeof(aggregate_value), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_NULL, 0, NULL, 0};
        aggregate_column.type_code = PRDB_SDK_VALUE_TYPE_DECIMAL;
        aggregate_column.decimal_precision = 10;
        aggregate_column.decimal_scale = 2;
        assert(prdb_sdk_sql_encode_group_minmax_result_v1(
                   &group_column, &group_key, &aggregate_column,
                   &aggregate_value, PRDB_SDK_SQL_V5_AGGREGATE_MAX, output,
                   sizeof(output), &output_len) == PRDB_SDK_SQL_RESULT_OK);
        assert(output[27] == 0 && output[28] == 3 && output[29] == 'm' &&
               output[30] == 'a' && output[31] == 'x');
        assert(output[35] == PRDB_SDK_VALUE_TYPE_DECIMAL &&
               output[36] == 10 && output[37] == 2 &&
               output[38] == PRDB_SDK_VALUE_STATE_NULL &&
               output[39] == 0 && output[40] == 0 && output[41] == 0 &&
               output[42] == 0);
        assert(prdb_sdk_sql_encode_group_sum_result_v1(
                   &group_column, &group_key, &aggregate_column,
                   &aggregate_value, output, sizeof(output), &output_len) ==
               PRDB_SDK_SQL_RESULT_OK);
        assert(output[35] == PRDB_SDK_VALUE_TYPE_DECIMAL &&
               output[36] == 38 && output[37] == 2 &&
               output[38] == PRDB_SDK_VALUE_STATE_NULL);
        {
            uint8_t decimal_sum_bytes[16] = {0};
            prdb_sdk_value_v1 decimal_sum = {0};
            decimal_sum_bytes[15] = 123;
            decimal_sum = (prdb_sdk_value_v1){
                sizeof(decimal_sum), PRDB_SDK_VALUE_ABI_V1_VERSION,
                PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_DECIMAL,
                decimal_sum_bytes, sizeof(decimal_sum_bytes)};
            assert(prdb_sdk_sql_encode_group_sum_result_v1(
                       &group_column, &group_key, &aggregate_column,
                       &decimal_sum, output, sizeof(output), &output_len) ==
                   PRDB_SDK_SQL_RESULT_OK);
            assert(output_len == 59 &&
                   output[35] == PRDB_SDK_VALUE_TYPE_DECIMAL &&
                   output[36] == 38 && output[37] == 2 &&
                   output[38] == PRDB_SDK_VALUE_STATE_VALUE &&
                   output[42] == 16 && output[43] == 0 &&
                   output[57] == 0 && output[58] == 123);
        }

        aggregate_value.state = PRDB_SDK_VALUE_STATE_REDACTED;
        assert(prdb_sdk_sql_encode_group_minmax_result_v1(
                   &group_column, &group_key, &aggregate_column,
                   &aggregate_value, PRDB_SDK_SQL_V5_AGGREGATE_MIN, output,
                   sizeof(output), &output_len) == PRDB_SDK_SQL_RESULT_INVALID_ROW);
        assert(output_len == 0);
        assert(prdb_sdk_sql_encode_group_sum_result_v1(
                   &group_column, &group_key, &aggregate_column,
                   &aggregate_value, output, sizeof(output), &output_len) ==
               PRDB_SDK_SQL_RESULT_INVALID_ROW);
        assert(output_len == 0);

        {
            uint8_t average_bytes[16] = {0};
            prdb_sdk_value_v1 average = {0};
            average_bytes[13] = 0x16;
            average_bytes[14] = 0xE3;
            average_bytes[15] = 0x60;
            average = (prdb_sdk_value_v1){
                sizeof(average), PRDB_SDK_VALUE_ABI_V1_VERSION,
                PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_DECIMAL,
                average_bytes, sizeof(average_bytes)};
            aggregate_column.type_code = PRDB_SDK_VALUE_TYPE_INTEGER;
            aggregate_column.decimal_precision = 0;
            aggregate_column.decimal_scale = 0;
            assert(prdb_sdk_sql_encode_group_avg_result_v1(
                       &group_column, &group_key, &aggregate_column,
                       &average, output, sizeof(output), &output_len) ==
                   PRDB_SDK_SQL_RESULT_OK);
            assert(output_len == 59 && output[27] == 0 && output[28] == 3 &&
                   memcmp(output + 29, "avg", 3) == 0 &&
                   output[35] == PRDB_SDK_VALUE_TYPE_DECIMAL &&
                   output[36] == 38 && output[37] == 6 &&
                   output[38] == PRDB_SDK_VALUE_STATE_VALUE &&
                   output[42] == 16 && output[58] == 0x60);
            capture.calls = 0;
            capture.length = 0;
            assert(prdb_sdk_sql_write_group_avg_result_v1(
                       &host, &group_column, &group_key, &aggregate_column,
                       &average, output, sizeof(output), &output_len) ==
                   PRDB_SDK_SQL_RESULT_OK);
            assert(capture.calls == 1 && capture.length == output_len &&
                   memcmp(capture.bytes, output, output_len) == 0);
            average.state = PRDB_SDK_VALUE_STATE_NULL;
            average.type_code = 0;
            average.data = NULL;
            average.data_len = 0;
            aggregate_column.type_code = PRDB_SDK_VALUE_TYPE_DECIMAL;
            aggregate_column.decimal_precision = 10;
            aggregate_column.decimal_scale = 2;
            assert(prdb_sdk_sql_encode_group_avg_result_v1(
                       &group_column, &group_key, &aggregate_column,
                       &average, output, sizeof(output), &output_len) ==
                   PRDB_SDK_SQL_RESULT_OK);
            assert(output[35] == PRDB_SDK_VALUE_TYPE_DECIMAL &&
                   output[36] == 38 && output[37] == 8 &&
                   output[38] == PRDB_SDK_VALUE_STATE_NULL);
        }
    }
    return 0;
}
