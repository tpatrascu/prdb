#include "prdb_sdk_executor_v1.h"

#include <assert.h>
#include <string.h>

#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
_Static_assert(PRDB_SDK_EXECUTOR_OP_RESOLVE_RELATION == UINT16_C(0x1101), "resolve operation tag");
_Static_assert(PRDB_SDK_EXECUTOR_OP_OPEN_SCAN == UINT16_C(0x1102), "open scan operation tag");
_Static_assert(PRDB_SDK_EXECUTOR_OP_NEXT_BATCH == UINT16_C(0x1103), "next batch operation tag");
_Static_assert(PRDB_SDK_EXECUTOR_OP_CLOSE_CURSOR == UINT16_C(0x1104), "close cursor operation tag");
_Static_assert(PRDB_SDK_EXECUTOR_OP_STAGE_INSERT == UINT16_C(0x1110), "insert operation tag");
_Static_assert(PRDB_SDK_EXECUTOR_OP_STAGE_UPDATE == UINT16_C(0x1111), "update operation tag");
_Static_assert(PRDB_SDK_EXECUTOR_OP_STAGE_DELETE == UINT16_C(0x1112), "delete operation tag");
_Static_assert(PRDB_SDK_EXECUTOR_OP_CHECK_CANCELLATION == UINT16_C(0x1113), "cancellation operation tag");
_Static_assert(PRDB_SDK_EXECUTOR_OP_SCAN_INDEX_RANGE == UINT16_C(0x1115), "index range operation tag");
_Static_assert(PRDB_SDK_EXECUTOR_OP_SCAN_INDEX_PREFIX == UINT16_C(0x1116), "index prefix operation tag");
#endif

typedef struct mock_host {
    uint16_t expected_operation;
    const uint8_t *expected_request;
    uint64_t expected_request_len;
    const uint8_t *reply;
    uint64_t reply_len;
    uint32_t calls;
} mock_host;

static int32_t mock_request(void *context, uint16_t operation,
                            const uint8_t *request, uint64_t request_len,
                            uint8_t *response, uint64_t capacity,
                            uint64_t *response_len) {
    mock_host *mock = (mock_host *)context;
    assert(operation == mock->expected_operation);
    assert(request_len == mock->expected_request_len);
    assert(memcmp(request, mock->expected_request, request_len) == 0);
    assert(mock->reply_len <= capacity);
    memcpy(response, mock->reply, mock->reply_len);
    *response_len = mock->reply_len;
    ++mock->calls;
    return PRDB_QUERY_CALLBACK_OK;
}

static void put_u16(uint8_t *p, uint32_t *offset, uint16_t value) {
    p[(*offset)++] = (uint8_t)(value >> 8);
    p[(*offset)++] = (uint8_t)value;
}

static void put_u32(uint8_t *p, uint32_t *offset, uint32_t value) {
    p[(*offset)++] = (uint8_t)(value >> 24);
    p[(*offset)++] = (uint8_t)(value >> 16);
    p[(*offset)++] = (uint8_t)(value >> 8);
    p[(*offset)++] = (uint8_t)value;
}

int main(void) {
    uint8_t resolve_reply[128] = {0};
    uint32_t n = 0;
    put_u16(resolve_reply, &n, 1);
    resolve_reply[n++] = 0;
    memset(resolve_reply + n, 0x11, 16); n += 16;
    resolve_reply[n++] = 6;
    memset(resolve_reply + n, 0x22, 15); n += 15;
    put_u16(resolve_reply, &n, 7);
    memcpy(resolve_reply + n, "widgets", 7); n += 7;
    put_u16(resolve_reply, &n, 1);
    memset(resolve_reply + n, 0x33, 16); n += 16;
    put_u32(resolve_reply, &n, 0);
    resolve_reply[n++] = 1;
    resolve_reply[n++] = PRDB_SDK_VALUE_TYPE_INTEGER;
    resolve_reply[n++] = 0;
    resolve_reply[n++] = 0;
    resolve_reply[n++] = 0;
    resolve_reply[n++] = 0;
    put_u16(resolve_reply, &n, 2);
    memcpy(resolve_reply + n, "id", 2); n += 2;

    const uint8_t schema[] = "app";
    const uint8_t relation[] = "widgets";
    uint8_t resolve_request[16] = {0, 1, 0, 3, 0, 7};
    memcpy(resolve_request + 6, schema, 3);
    memcpy(resolve_request + 9, relation, 7);
    mock_host mock = {PRDB_SDK_EXECUTOR_OP_RESOLVE_RELATION, resolve_request,
                      sizeof(resolve_request), resolve_reply, n, 0};
    prdb_host_api_v1 host = {sizeof(host), PRDB_QUERY_ABI_VERSION,
                             &mock, mock_request, mock_request, NULL};
    uint8_t response[256] = {0};
    uint64_t response_len = 0;
    prdb_sdk_executor_relation_v1 resolved = {0};
    prdb_sdk_sql_column_shape_v1 columns[1] = {{0}};
    assert(prdb_sdk_executor_resolve_relation_v1(
               &host, schema, 3, relation, 7, response, sizeof(response),
               &response_len, &resolved, sizeof(resolved), columns, 1) ==
           PRDB_SDK_EXECUTOR_OK);
    assert(mock.calls == 1 && response_len == n);
    assert(resolved.abi_version == PRDB_SDK_EXECUTOR_ABI_V1_VERSION);
    assert(resolved.executor_handle[0] == 6 && resolved.column_count == 1);
    assert(columns[0].type_code == PRDB_SDK_VALUE_TYPE_INTEGER);
    assert(columns[0].ordinal == 0 && columns[0].name_len == 2);

    uint8_t open_request[20] = {0, 1, 6};
    memset(open_request + 3, 0x22, 15);
    uint8_t open_reply[18] = {0, 1, 7};
    memset(open_reply + 3, 0x44, 15);
    mock.expected_operation = PRDB_SDK_EXECUTOR_OP_OPEN_SCAN;
    mock.expected_request = open_request;
    mock.expected_request_len = sizeof(open_request);
    mock.reply = open_reply;
    mock.reply_len = sizeof(open_reply);
    uint8_t cursor[16];
    assert(prdb_sdk_executor_open_full_scan_v1(
               &host, resolved.executor_handle, response, sizeof(response),
               &response_len, cursor) == PRDB_SDK_EXECUTOR_OK);
    assert(cursor[0] == 7 && mock.calls == 2);

    uint8_t next_request[24] = {0, 1, 7};
    memset(next_request + 3, 0x44, 15);
    next_request[19] = 1;
    next_request[20] = 0;
    next_request[21] = 0;
    next_request[22] = 4;
    next_request[23] = 0;
    uint8_t batch_reply[5] = {0, 1, 1, 0, 0};
    mock.expected_operation = PRDB_SDK_EXECUTOR_OP_NEXT_BATCH;
    mock.expected_request = next_request;
    mock.expected_request_len = sizeof(next_request);
    mock.reply = batch_reply;
    mock.reply_len = sizeof(batch_reply);
    prdb_sdk_batch_v1 batch = {0};
    assert(prdb_sdk_executor_next_batch_v1(
               &host, cursor, 1, 1024, response, sizeof(response),
               &response_len, &batch, sizeof(batch)) == PRDB_SDK_EXECUTOR_OK);
    assert(batch.finished == 1 && batch.row_count == 0 && mock.calls == 3);

    {
        uint8_t over_capacity_reply[41] = {0, 1, 1, 0, 2};
        uint32_t offset = 5;
        prdb_sdk_batch_v1 unchanged_batch;
        memcpy(&unchanged_batch, &batch, sizeof(batch));
        for (uint32_t row = 0; row < 2; ++row) {
            over_capacity_reply[offset++] = PRDB_SDK_BATCH_ROW_TOKEN_KIND;
            memset(over_capacity_reply + offset, (int)(0x31 + row), 15);
            offset += 15;
            put_u16(over_capacity_reply, &offset, 0);
        }
        assert(offset == sizeof(over_capacity_reply));
        mock.reply = over_capacity_reply;
        mock.reply_len = sizeof(over_capacity_reply);
        assert(prdb_sdk_executor_next_batch_v1(
                   &host, cursor, 1, 1024, response, sizeof(response),
                   &response_len, &batch, sizeof(batch)) ==
               PRDB_SDK_EXECUTOR_INVALID_PAYLOAD);
        assert(memcmp(&batch, &unchanged_batch, sizeof(batch)) == 0);

        next_request[19] = 2;
        next_request[20] = 0;
        next_request[21] = 0;
        next_request[22] = 0;
        next_request[23] = 40;
        assert(prdb_sdk_executor_next_batch_v1(
                   &host, cursor, 2, 40, response, sizeof(response),
                   &response_len, &batch, sizeof(batch)) ==
               PRDB_SDK_EXECUTOR_INVALID_PAYLOAD);
        assert(memcmp(&batch, &unchanged_batch, sizeof(batch)) == 0);

        next_request[19] = 1;
        next_request[20] = 0;
        next_request[21] = 0;
        next_request[22] = 4;
        next_request[23] = 0;
        mock.reply = batch_reply;
        mock.reply_len = sizeof(batch_reply);
    }

    uint8_t close_request[18] = {0, 1, 7};
    memcpy(close_request + 2, cursor, 16);
    uint8_t close_reply[3] = {0, 1, 0};
    mock.expected_operation = PRDB_SDK_EXECUTOR_OP_CLOSE_CURSOR;
    mock.expected_request = close_request;
    mock.expected_request_len = sizeof(close_request);
    mock.reply = close_reply;
    mock.reply_len = sizeof(close_reply);
    assert(prdb_sdk_executor_close_cursor_v1(
               &host, cursor, response, sizeof(response), &response_len) ==
           PRDB_SDK_EXECUTOR_OK);
    assert(mock.calls == 6);
    uint8_t legacy_close_reply[4] = {0, 1, 0, 0};
    mock.reply = legacy_close_reply;
    mock.reply_len = sizeof(legacy_close_reply);
    assert(prdb_sdk_executor_close_cursor_v1(
               &host, cursor, response, sizeof(response), &response_len) ==
           PRDB_SDK_EXECUTOR_INVALID_PAYLOAD);
    assert(mock.calls == 7);

    uint8_t row_handle[16] = {8};
    memset(row_handle + 1, 0x55, 15);
    uint8_t delete_request[18] = {0, 1};
    memcpy(delete_request + 2, row_handle, 16);
    uint8_t mutation_reply[3] = {0, 1, 0};
    mock.expected_operation = PRDB_SDK_EXECUTOR_OP_STAGE_DELETE;
    mock.expected_request = delete_request;
    mock.expected_request_len = sizeof(delete_request);
    mock.reply = mutation_reply;
    mock.reply_len = sizeof(mutation_reply);
    assert(prdb_sdk_executor_stage_delete_v1(
               &host, row_handle, response, sizeof(response), &response_len) ==
           PRDB_SDK_EXECUTOR_OK);
    assert(response_len == sizeof(mutation_reply) && mock.calls == 8);
    mutation_reply[2] = 1;
    assert(prdb_sdk_executor_stage_delete_v1(
               &host, row_handle, response, sizeof(response), &response_len) ==
           PRDB_SDK_EXECUTOR_INVALID_PAYLOAD);
    assert(mock.calls == 9);
    row_handle[0] = 7;
    assert(prdb_sdk_executor_stage_delete_v1(
               &host, row_handle, response, sizeof(response), &response_len) ==
           PRDB_SDK_EXECUTOR_INVALID_ARGUMENT);
    assert(mock.calls == 9);

    uint8_t relation_handle[16] = {6};
    memset(relation_handle + 1, 0x22, 15);
    uint8_t expected_insert_request[32] = {0, 1};
    memcpy(expected_insert_request + 2, relation_handle, 16);
    expected_insert_request[18] = 0;
    expected_insert_request[19] = 3;
    expected_insert_request[20] = 0;
    expected_insert_request[21] = PRDB_SDK_VALUE_TYPE_INTEGER;
    expected_insert_request[25] = 4;
    expected_insert_request[26] = 0;
    expected_insert_request[27] = 0;
    expected_insert_request[28] = 0;
    expected_insert_request[29] = 42;
    expected_insert_request[30] = 1;
    expected_insert_request[31] = 2;
    prdb_sdk_executor_insert_value_v1 insert_values[3] = {{0}};
    for (uint32_t i = 0; i < 3; ++i) {
        insert_values[i].struct_size = sizeof(insert_values[i]);
        insert_values[i].abi_version = PRDB_SDK_EXECUTOR_ABI_V1_VERSION;
    }
    insert_values[0].state = PRDB_SDK_EXECUTOR_INSERT_VALUE;
    insert_values[0].value.struct_size = sizeof(prdb_sdk_value_v1);
    insert_values[0].value.abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
    insert_values[0].value.state = PRDB_SDK_VALUE_STATE_VALUE;
    insert_values[0].value.type_code = PRDB_SDK_VALUE_TYPE_INTEGER;
    const uint8_t integer_42[] = {0, 0, 0, 42};
    insert_values[0].value.data = integer_42;
    insert_values[0].value.data_len = sizeof(integer_42);
    insert_values[1].state = PRDB_SDK_EXECUTOR_INSERT_NULL;
    insert_values[1].value.struct_size = sizeof(prdb_sdk_value_v1);
    insert_values[1].value.abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
    insert_values[1].value.state = PRDB_SDK_VALUE_STATE_NULL;
    insert_values[2].state = PRDB_SDK_EXECUTOR_INSERT_DEFAULT;

    uint8_t insert_reply[18] = {0, 1, 8};
    memset(insert_reply + 3, 0x66, 15);
    mock.expected_operation = PRDB_SDK_EXECUTOR_OP_STAGE_INSERT;
    mock.expected_request = expected_insert_request;
    mock.expected_request_len = sizeof(expected_insert_request);
    mock.reply = insert_reply;
    mock.reply_len = sizeof(insert_reply);
    uint8_t inserted_handle[16] = {0};
    uint8_t insert_request[32] = {0};
    assert(prdb_sdk_executor_stage_insert_v1(
               &host, relation_handle, insert_values, 3, insert_request,
               sizeof(insert_request) - 1, response, sizeof(response),
               &response_len, inserted_handle) ==
           PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL);
    assert(mock.calls == 9);
    assert(prdb_sdk_executor_stage_insert_v1(
               &host, relation_handle, insert_values, 3, insert_request,
               sizeof(insert_request), response, sizeof(response),
               &response_len, inserted_handle) == PRDB_SDK_EXECUTOR_OK);
    assert(response_len == sizeof(insert_reply) && inserted_handle[0] == 8 &&
           mock.calls == 10);
    uint8_t valid_inserted_handle[16];
    memcpy(valid_inserted_handle, inserted_handle, sizeof(valid_inserted_handle));
    memset(inserted_handle, 0xaa, sizeof(inserted_handle));
    insert_reply[2] = 7;
    assert(prdb_sdk_executor_stage_insert_v1(
               &host, relation_handle, insert_values, 3, insert_request,
               sizeof(insert_request), response, sizeof(response),
               &response_len, inserted_handle) ==
           PRDB_SDK_EXECUTOR_INVALID_PAYLOAD);
    assert(inserted_handle[0] == 0xaa && mock.calls == 11);

    uint8_t expected_update_request[39] = {0, 1};
    memcpy(expected_update_request + 2, valid_inserted_handle, 16);
    expected_update_request[18] = 0;
    expected_update_request[19] = 2;
    expected_update_request[23] = 1;
    expected_update_request[24] = 1;
    expected_update_request[25] = 0;
    expected_update_request[26] = 0;
    expected_update_request[27] = 0;
    expected_update_request[28] = 3;
    expected_update_request[29] = 0;
    expected_update_request[30] = PRDB_SDK_VALUE_TYPE_INTEGER;
    expected_update_request[34] = 4;
    expected_update_request[35] = 0;
    expected_update_request[36] = 0;
    expected_update_request[37] = 0;
    expected_update_request[38] = 99;
    prdb_sdk_executor_update_patch_v1 patches[2] = {{0}};
    for (uint32_t i = 0; i < 2; ++i) {
        patches[i].struct_size = sizeof(patches[i]);
        patches[i].abi_version = PRDB_SDK_EXECUTOR_ABI_V1_VERSION;
    }
    patches[0].ordinal = 1;
    patches[0].value.struct_size = sizeof(prdb_sdk_value_v1);
    patches[0].value.abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
    patches[0].value.state = PRDB_SDK_VALUE_STATE_NULL;
    patches[1].ordinal = 3;
    patches[1].value.struct_size = sizeof(prdb_sdk_value_v1);
    patches[1].value.abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
    patches[1].value.state = PRDB_SDK_VALUE_STATE_VALUE;
    patches[1].value.type_code = PRDB_SDK_VALUE_TYPE_INTEGER;
    const uint8_t integer_99[] = {0, 0, 0, 99};
    patches[1].value.data = integer_99;
    patches[1].value.data_len = sizeof(integer_99);
    uint8_t update_request[39] = {0};
    mock.expected_operation = PRDB_SDK_EXECUTOR_OP_STAGE_UPDATE;
    mock.expected_request = expected_update_request;
    mock.expected_request_len = sizeof(expected_update_request);
    mock.reply = mutation_reply;
    mutation_reply[2] = 0;
    mock.reply_len = sizeof(mutation_reply);
    assert(prdb_sdk_executor_stage_update_v1(
               &host, valid_inserted_handle, patches, 2, update_request,
               sizeof(update_request), response, sizeof(response),
               &response_len) == PRDB_SDK_EXECUTOR_OK);
    assert(mock.calls == 12);
    mutation_reply[2] = 1;
    assert(prdb_sdk_executor_stage_update_v1(
               &host, valid_inserted_handle, patches, 2, update_request,
               sizeof(update_request), response, sizeof(response),
               &response_len) == PRDB_SDK_EXECUTOR_INVALID_PAYLOAD);
    assert(mock.calls == 13);
    mutation_reply[2] = 0;
    patches[1].ordinal = 1;
    assert(prdb_sdk_executor_stage_update_v1(
               &host, valid_inserted_handle, patches, 2, update_request,
               sizeof(update_request), response, sizeof(response),
               &response_len) == PRDB_SDK_EXECUTOR_INVALID_ARGUMENT);
    assert(mock.calls == 13);
    patches[1].ordinal = PRDB_SDK_BATCH_MAX_CELLS_PER_ROW;
    assert(prdb_sdk_executor_stage_update_v1(
               &host, valid_inserted_handle, patches, 2, update_request,
               sizeof(update_request), response, sizeof(response),
               &response_len) == PRDB_SDK_EXECUTOR_INVALID_ARGUMENT);
    assert(mock.calls == 13);

    assert(prdb_sdk_executor_open_full_scan_v1(
               &host, cursor, response, sizeof(response), &response_len,
               cursor) == PRDB_SDK_EXECUTOR_INVALID_ARGUMENT);

    const uint8_t empty_request = 0;
    uint8_t cancellation_reply[PRDB_EXECUTOR_CANCELLATION_CHECK_RESPONSE_HEADER_BYTES] = {0, 1, 0};
    mock.expected_operation = PRDB_SDK_EXECUTOR_OP_CHECK_CANCELLATION;
    mock.expected_request = &empty_request;
    mock.expected_request_len = 0;
    mock.reply = cancellation_reply;
    mock.reply_len = sizeof(cancellation_reply);
    int32_t cancelled = 77;
    assert(prdb_sdk_executor_check_cancelled_v1(&host, &cancelled) ==
           PRDB_SDK_EXECUTOR_OK);
    assert(cancelled == 0 && mock.calls == 14);
    cancellation_reply[PRDB_EXECUTOR_CANCELLATION_CHECK_RESPONSE_CANCELLED_OFFSET] = 1;
    assert(prdb_sdk_executor_check_cancelled_v1(&host, &cancelled) ==
           PRDB_SDK_EXECUTOR_OK);
    assert(cancelled == 1 && mock.calls == 15);
    cancellation_reply[PRDB_EXECUTOR_CANCELLATION_CHECK_RESPONSE_CANCELLED_OFFSET] = 2;
    cancelled = 77;
    assert(prdb_sdk_executor_check_cancelled_v1(&host, &cancelled) ==
           PRDB_SDK_EXECUTOR_INVALID_PAYLOAD);
    assert(cancelled == 77 && mock.calls == 16);

    {
        const uint8_t integer_42[] = {0, 0, 0, 42};
        uint8_t key_id[16];
        uint8_t expected_range_request[63] = {0};
        uint8_t range_request[63] = {0};
        uint8_t range_reply[] = {1, 1, 0, 0};
        uint8_t unavailable_reply[] = {1, 0};
        uint8_t malformed_reply[] = {1, 1, 0, 0, 0};
        uint32_t offset = 0;
        uint8_t selected_columns[16];
        prdb_sdk_value_v1 lower = {0};
        prdb_sdk_index_range_v1 range = {0};
        prdb_sdk_index_range_v1 unchanged_range;

        memset(key_id, 0x99, sizeof(key_id));
        memset(selected_columns, 0x33, sizeof(selected_columns));
        put_u16(expected_range_request, &offset, 1);
        memcpy(expected_range_request + offset, resolved.executor_handle, 16);
        offset += 16;
        memcpy(expected_range_request + offset, key_id, 16);
        offset += 16;
        expected_range_request[offset++] = 1;
        expected_range_request[offset++] = PRDB_SDK_VALUE_TYPE_INTEGER;
        put_u32(expected_range_request, &offset, 4);
        memcpy(expected_range_request + offset, integer_42, sizeof(integer_42));
        offset += sizeof(integer_42);
        expected_range_request[offset++] = 0;
        put_u16(expected_range_request, &offset, 1);
        memcpy(expected_range_request + offset, selected_columns, 16);
        offset += 16;
        assert(offset == sizeof(expected_range_request));

        lower.struct_size = sizeof(lower);
        lower.abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
        lower.state = PRDB_SDK_VALUE_STATE_VALUE;
        lower.type_code = PRDB_SDK_VALUE_TYPE_INTEGER;
        lower.data = integer_42;
        lower.data_len = sizeof(integer_42);
        mock.expected_operation = PRDB_SDK_EXECUTOR_OP_SCAN_INDEX_RANGE;
        mock.expected_request = expected_range_request;
        mock.expected_request_len = sizeof(expected_range_request);
        mock.reply = range_reply;
        mock.reply_len = sizeof(range_reply);
        assert(prdb_sdk_executor_scan_index_range_v1(
                   &host, resolved.executor_handle, key_id, &lower, 1, NULL,
                   0, selected_columns, 1,
                   range_request, sizeof(range_request), response,
                   sizeof(response), &response_len, &range, sizeof(range)) ==
               PRDB_SDK_EXECUTOR_OK);
        assert(range.row_count == 0 && mock.calls == 17);
        memcpy(&unchanged_range, &range, sizeof(range));
        mock.reply = unavailable_reply;
        mock.reply_len = sizeof(unavailable_reply);
        assert(prdb_sdk_executor_scan_index_range_v1(
                   &host, resolved.executor_handle, key_id, &lower, 1, NULL,
                   0, selected_columns, 1,
                   range_request, sizeof(range_request), response,
                   sizeof(response), &response_len, &range, sizeof(range)) ==
               PRDB_SDK_EXECUTOR_UNAVAILABLE);
        assert(memcmp(&range, &unchanged_range, sizeof(range)) == 0 &&
               mock.calls == 18);
        mock.reply = malformed_reply;
        mock.reply_len = sizeof(malformed_reply);
        assert(prdb_sdk_executor_scan_index_range_v1(
                   &host, resolved.executor_handle, key_id, &lower, 1, NULL,
                   0, selected_columns, 1, range_request,
                   sizeof(range_request), response, sizeof(response),
                   &response_len, &range, sizeof(range)) ==
               PRDB_SDK_EXECUTOR_INVALID_PAYLOAD);
        assert(memcmp(&range, &unchanged_range, sizeof(range)) == 0 &&
               mock.calls == 19);
        assert(prdb_sdk_executor_scan_index_range_v1(
                   &host, resolved.executor_handle, key_id, NULL, 0, NULL, 0,
                   selected_columns, 1, range_request, sizeof(range_request),
                   response, sizeof(response), &response_len, &range,
                   sizeof(range)) == PRDB_SDK_EXECUTOR_INVALID_ARGUMENT);
        assert(mock.calls == 19);
        assert(prdb_sdk_executor_scan_index_range_v1(
                   &host, resolved.executor_handle, key_id, &lower, 1, NULL,
                   0, selected_columns, 1, range_request,
                   sizeof(expected_range_request) - 1, response,
                   sizeof(response), &response_len, &range, sizeof(range)) ==
               PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL);
        assert(mock.calls == 19);
    }
    {
        const uint8_t first_value[] = {0, 0, 0, 4};
        const uint8_t second_value[] = {0, 0, 0, 9};
        uint8_t key_id[16], selected_columns[16], prefix_request[128];
        uint8_t expected[71] = {0}, empty_reply[] = {1, 1, 0, 0};
        uint32_t offset = 0;
        prdb_sdk_value_v1 values[2] = {{0}};
        prdb_sdk_index_range_v1 range = {0};
        memset(key_id, 0x77, sizeof(key_id));
        memset(selected_columns, 0x55, sizeof(selected_columns));
        put_u16(expected, &offset, 1);
        memcpy(expected + offset, resolved.executor_handle, 16); offset += 16;
        memcpy(expected + offset, key_id, 16); offset += 16;
        expected[offset++] = 2;
        for (uint32_t i = 0; i < 2; ++i) {
            expected[offset++] = PRDB_SDK_VALUE_TYPE_INTEGER;
            put_u32(expected, &offset, 4);
            memcpy(expected + offset, i == 0 ? first_value : second_value, 4);
            offset += 4;
            values[i].struct_size = sizeof(values[i]);
            values[i].abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
            values[i].state = PRDB_SDK_VALUE_STATE_VALUE;
            values[i].type_code = PRDB_SDK_VALUE_TYPE_INTEGER;
            values[i].data = i == 0 ? first_value : second_value;
            values[i].data_len = 4;
        }
        expected[offset++] = 0;
        expected[offset++] = 1;
        memcpy(expected + offset, selected_columns, 16); offset += 16;
        assert(offset == sizeof(expected));
        mock.expected_operation = PRDB_SDK_EXECUTOR_OP_SCAN_INDEX_PREFIX;
        mock.expected_request = expected;
        mock.expected_request_len = sizeof(expected);
        mock.reply = empty_reply;
        mock.reply_len = sizeof(empty_reply);
        assert(prdb_sdk_executor_scan_index_prefix_v1(
                   &host, resolved.executor_handle, key_id, values, 2,
                   selected_columns, 1, prefix_request, sizeof(prefix_request),
                   response, sizeof(response), &response_len, &range,
                   sizeof(range)) == PRDB_SDK_EXECUTOR_OK);
        assert(range.row_count == 0 && mock.calls == 20);
        values[1].state = PRDB_SDK_VALUE_STATE_NULL;
        assert(prdb_sdk_executor_scan_index_prefix_v1(
                   &host, resolved.executor_handle, key_id, values, 2,
                   selected_columns, 1, prefix_request, sizeof(prefix_request),
                   response, sizeof(response), &response_len, &range,
                   sizeof(range)) == PRDB_SDK_EXECUTOR_INVALID_ARGUMENT);
        assert(mock.calls == 20);
    }
    {
        const uint8_t empty_request[] = {0};
        const uint8_t budget_reply[12] = {
            'P', 'Q', 'B', '1', 0, PRDB_QUERY_WORKLOAD_BUDGETS_VERSION,
            0, 12, 0, 0, 1, 0};
        prdb_sdk_workload_budgets_v1 budgets = {0};
        mock.expected_operation = PRDB_SDK_EXECUTOR_OP_GET_WORKLOAD_BUDGETS;
        mock.expected_request = empty_request;
        mock.expected_request_len = 0;
        mock.reply = budget_reply;
        mock.reply_len = sizeof(budget_reply);
        assert(prdb_sdk_executor_get_workload_budgets_v1(
                   &host, response, sizeof(response), &response_len, &budgets,
                   sizeof(budgets)) == PRDB_SDK_EXECUTOR_OK);
        assert(budgets.abi_version == PRDB_QUERY_WORKLOAD_BUDGETS_VERSION);
        assert(budgets.max_index_probe_candidates == 256);
        assert(mock.calls == 21);
    }
    {
        enum { ROW_COUNT = 1025 };
        uint8_t range_payload[4 + ROW_COUNT * 2] = {1, 1, 0, 0};
        prdb_sdk_index_range_v1 range = {0};
        range_payload[2] = (uint8_t)(ROW_COUNT >> 8);
        range_payload[3] = (uint8_t)ROW_COUNT;
        for (uint32_t row = 0; row < ROW_COUNT; ++row) {
            range_payload[4 + row * 2] = 0;
            range_payload[5 + row * 2] = 0;
        }
        assert(prdb_sdk_index_range_open_v1(
                   &range, sizeof(range), range_payload,
                   sizeof(range_payload)) == PRDB_SDK_BATCH_OK);
        assert(range.row_count == ROW_COUNT);
    }
    return 0;
}
