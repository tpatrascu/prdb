#include "prdb_sdk_sql_exec_v2.h"

#include <assert.h>
#include <string.h>

typedef struct mock_host {
    uint8_t planner[128];
    uint32_t planner_len;
    uint8_t executor[128];
    uint32_t executor_len;
    uint8_t first_batch[128];
    uint32_t first_batch_len;
    uint8_t second_batch[128];
    uint32_t second_batch_len;
    uint32_t next_calls;
    uint32_t planner_calls;
    uint32_t executor_calls;
    uint32_t result_calls;
    uint32_t failure_calls;
} mock_host;

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

static void put_name(uint8_t *p, uint32_t *offset, const char *name) {
    uint16_t length = (uint16_t)strlen(name);
    put_u16(p, offset, length);
    memcpy(p + *offset, name, length);
    *offset += length;
}

static uint32_t make_relation(uint8_t *p, uint8_t handle_kind,
                              uint8_t handle_fill) {
    uint32_t offset = 0;
    put_u16(p, &offset, 1);
    p[offset++] = 0;
    memset(p + offset, 0x11, 16);
    offset += 16;
    p[offset++] = handle_kind;
    memset(p + offset, handle_fill, 15);
    offset += 15;
    put_name(p, &offset, "schemata");
    put_u16(p, &offset, 1);
    memset(p + offset, 0x22, 16);
    offset += 16;
    put_u32(p, &offset, 0);
    p[offset++] = 0;
    p[offset++] = PRDB_SDK_VALUE_TYPE_TEXT;
    p[offset++] = 0;
    p[offset++] = 0;
    p[offset++] = 0;
    p[offset++] = 0;
    put_name(p, &offset, "schema_name");
    return offset;
}

static uint32_t make_text_batch(uint8_t *p, uint8_t row_fill,
                                const char *value, uint8_t next_fill) {
    uint32_t offset = 0;
    put_u16(p, &offset, 1);
    p[offset++] = 0;
    p[offset++] = PRDB_SDK_BATCH_CURSOR_TOKEN_KIND;
    memset(p + offset, next_fill, 15);
    offset += 15;
    put_u16(p, &offset, 1);
    p[offset++] = PRDB_SDK_BATCH_ROW_TOKEN_KIND;
    memset(p + offset, row_fill, 15);
    offset += 15;
    put_u16(p, &offset, 1);
    p[offset++] = PRDB_SDK_VALUE_STATE_VALUE;
    p[offset++] = PRDB_SDK_VALUE_TYPE_TEXT;
    put_u32(p, &offset, (uint32_t)strlen(value));
    memcpy(p + offset, value, strlen(value));
    offset += (uint32_t)strlen(value);
    return offset;
}

static int32_t planner_request(void *context, uint16_t operation,
                               const uint8_t *request, uint64_t request_len,
                               uint8_t *response, uint64_t capacity,
                               uint64_t *response_len) {
    static const uint8_t expected[] = {
        0, 1, 0, 18, 0, 8,
        'i', 'n', 'f', 'o', 'r', 'm', 'a', 't', 'i', 'o', 'n', '_',
        's', 'c', 'h', 'e', 'm', 'a', 's', 'c', 'h', 'e', 'm', 'a', 't', 'a'};
    mock_host *mock = (mock_host *)context;
    assert(operation == PRDB_SDK_PLANNER_OP_DESCRIBE_RELATION);
    assert(request_len == sizeof(expected));
    assert(memcmp(request, expected, sizeof(expected)) == 0);
    assert(mock->planner_len <= capacity);
    memcpy(response, mock->planner, mock->planner_len);
    *response_len = mock->planner_len;
    ++mock->planner_calls;
    return PRDB_QUERY_CALLBACK_OK;
}

static int32_t executor_request(void *context, uint16_t operation,
                                const uint8_t *request, uint64_t request_len,
                                uint8_t *response, uint64_t capacity,
                                uint64_t *response_len) {
    static const uint8_t resolve[] = {
        0, 1, 0, 18, 0, 8,
        'i', 'n', 'f', 'o', 'r', 'm', 'a', 't', 'i', 'o', 'n', '_',
        's', 'c', 'h', 'e', 'm', 'a', 's', 'c', 'h', 'e', 'm', 'a', 't', 'a'};
    static const uint8_t open[] = {
        0, 1, 6,
        0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33,
        0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33,
        0, 0};
    static const uint8_t open_reply[] = {
        0, 1, 7,
        0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
        0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44};
    static const uint8_t next_first[] = {
        0, 1, 7,
        0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
        0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
        0, 1, 0, 0, 4, 0};
    static const uint8_t next_second[] = {
        0, 1, 7,
        0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
        0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
        0, 1, 0, 0, 4, 0};
    static const uint8_t close[] = {
        0, 1, 7,
        0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
        0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66};
    static const uint8_t close_reply[] = {0, 1, 0};
    mock_host *mock = (mock_host *)context;
    const uint8_t *source = NULL;
    uint32_t source_len = 0;
    if (operation == PRDB_SDK_EXECUTOR_OP_RESOLVE_RELATION) {
        assert(request_len == sizeof(resolve));
        assert(memcmp(request, resolve, sizeof(resolve)) == 0);
        source = mock->executor;
        source_len = mock->executor_len;
    } else if (operation == PRDB_SDK_EXECUTOR_OP_OPEN_SCAN) {
        assert(request_len == sizeof(open));
        assert(memcmp(request, open, sizeof(open)) == 0);
        source = open_reply;
        source_len = sizeof(open_reply);
    } else if (operation == PRDB_SDK_EXECUTOR_OP_NEXT_BATCH) {
        if (mock->next_calls == 0) {
            assert(request_len == sizeof(next_first));
            assert(memcmp(request, next_first, sizeof(next_first)) == 0);
            source = mock->first_batch;
            source_len = mock->first_batch_len;
        } else {
            assert(mock->next_calls == 1);
            assert(request_len == sizeof(next_second));
            assert(memcmp(request, next_second, sizeof(next_second)) == 0);
            source = mock->second_batch;
            source_len = mock->second_batch_len;
        }
        ++mock->next_calls;
    } else if (operation == PRDB_SDK_EXECUTOR_OP_CLOSE_CURSOR) {
        assert(request_len == sizeof(close));
        assert(memcmp(request, close, sizeof(close)) == 0);
        source = close_reply;
        source_len = sizeof(close_reply);
    } else {
        assert(!"unexpected executor operation");
    }
    assert(source_len <= capacity);
    memcpy(response, source, source_len);
    *response_len = source_len;
    ++mock->executor_calls;
    return PRDB_QUERY_CALLBACK_OK;
}

static int32_t statement_failure(void *context, uint32_t failure_code) {
    mock_host *mock = (mock_host *)context;
    assert(failure_code == PRDB_QUERY_STATEMENT_FAILURE_SQL);
    ++mock->failure_calls;
    return PRDB_QUERY_CALLBACK_OK;
}

static int32_t on_row(void *context, const prdb_sdk_row_v1 *row,
                      const prdb_sdk_select_binding_v2 *binding) {
    mock_host *mock = (mock_host *)context;
    prdb_sdk_cell_iterator_v1 cells = {0};
    prdb_sdk_value_v1 value = {0};
    assert(binding->projection_count == 1);
    assert(binding->projection_ordinals[0] == 0);
    assert(row->cell_count == 1);
    assert(prdb_sdk_row_cells_begin_v1(row, &cells, sizeof(cells)) ==
           PRDB_SDK_BATCH_OK);
    assert(prdb_sdk_cell_next_v1(&cells, &value, sizeof(value)) ==
           PRDB_SDK_BATCH_OK);
    assert(value.state == PRDB_SDK_VALUE_STATE_VALUE);
    assert(value.type_code == PRDB_SDK_VALUE_TYPE_TEXT);
    assert(value.data_len == 3 && memcmp(value.data, "app", 3) == 0);
    ++mock->result_calls;
    return 0;
}

int main(void) {
    static const uint8_t schema[] = "information_schema";
    static const uint8_t sql[] =
        "SELECT schema_name FROM schemata WHERE schema_name = $1 LIMIT 1";
    static const uint8_t parameter_bytes[] = "app";
    mock_host mock = {0};
    mock.planner_len = make_relation(mock.planner,
                                     PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x22);
    mock.executor_len = make_relation(mock.executor, 6, 0x33);
    mock.first_batch_len = make_text_batch(
        mock.first_batch, 0x11, "information_schema", 0x55);
    mock.second_batch_len = make_text_batch(mock.second_batch, 0x12, "app", 0x66);
    prdb_host_api_v3 host = {
        sizeof(host), PRDB_QUERY_ABI_V3_VERSION, &mock,
        executor_request, planner_request, NULL, statement_failure};
    prdb_sdk_value_v1 parameter = {
        sizeof(parameter), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_TEXT,
        parameter_bytes, sizeof(parameter_bytes) - 1};
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[1024] = {0};
    prdb_sdk_sql_column_shape_v2 planner_columns[1] = {{0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[1] = {{0}};
    prdb_sdk_value_v1 cell_scratch[1] = {{0}};

    assert(prdb_sdk_sql_execute_select_v2(
               &host, schema, sizeof(schema) - 1, sql, sizeof(sql) - 1,
               &parameter, 1, 1, 1024, planner_response,
               sizeof(planner_response), executor_response,
               sizeof(executor_response), batch_response,
               sizeof(batch_response), planner_columns, 1, executor_columns,
               1, cell_scratch, 1, on_row, &mock) == PRDB_SDK_SQL_EXEC_OK);
    assert(mock.planner_calls == 1);
    assert(mock.executor_calls == 5);
    assert(mock.next_calls == 2);
    assert(mock.result_calls == 1);
    assert(mock.failure_calls == 0);
    return 0;
}
