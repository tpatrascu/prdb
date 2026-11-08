#include "prdb_sdk_executor_v1.h"

#include <assert.h>
#include <string.h>

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
    prdb_host_api_v2 host = {sizeof(host), PRDB_QUERY_ABI_V2_VERSION,
                             &mock, mock_request, mock_request, NULL};
    uint8_t response[256] = {0};
    uint64_t response_len = 0;
    prdb_sdk_executor_relation_v1 resolved = {0};
    prdb_sdk_sql_column_shape_v2 columns[1] = {{0}};
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

    uint8_t close_request[18] = {0, 1, 7};
    memcpy(close_request + 2, cursor, 16);
    uint8_t close_reply[4] = {0, 1, 0, 0};
    mock.expected_operation = PRDB_SDK_EXECUTOR_OP_CLOSE_CURSOR;
    mock.expected_request = close_request;
    mock.expected_request_len = sizeof(close_request);
    mock.reply = close_reply;
    mock.reply_len = sizeof(close_reply);
    assert(prdb_sdk_executor_close_cursor_v1(
               &host, cursor, response, sizeof(response), &response_len) ==
           PRDB_SDK_EXECUTOR_OK);
    assert(mock.calls == 4);
    assert(prdb_sdk_executor_open_full_scan_v1(
               &host, cursor, response, sizeof(response), &response_len,
               cursor) == PRDB_SDK_EXECUTOR_INVALID_ARGUMENT);
    return 0;
}
