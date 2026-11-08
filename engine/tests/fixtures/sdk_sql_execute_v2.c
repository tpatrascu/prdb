#include "prdb_sdk_sql_exec_v2.h"
#include "prdb_sdk_sql_result_v1.h"

#include <assert.h>
#include <string.h>

typedef struct mock_host {
    const uint8_t *planner;
    uint64_t planner_len;
    const uint8_t *executor;
    uint64_t executor_len;
    const uint8_t *batch;
    uint64_t batch_len;
    uint32_t planner_calls;
    uint32_t executor_calls;
    uint32_t statement_failed;
} mock_host;

static int32_t statement_failure(void *context, uint32_t failure_code) {
    mock_host *mock = (mock_host *)context;
    assert(failure_code == PRDB_QUERY_STATEMENT_FAILURE_SQL);
    mock->statement_failed = 1;
    return PRDB_QUERY_CALLBACK_OK;
}

static int32_t planner_request(void *context, uint16_t operation,
                               const uint8_t *request, uint64_t request_len,
                               uint8_t *response, uint64_t capacity,
                               uint64_t *response_len) {
    static const uint8_t expected[] = {
        0, 1, 0, 3, 0, 7, 'a', 'p', 'p', 'w', 'i', 'd', 'g', 'e', 't', 's'};
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
    mock_host *mock = (mock_host *)context;
    if (operation == PRDB_SDK_EXECUTOR_OP_RESOLVE_RELATION) {
        static const uint8_t expected[] = {
            0, 1, 0, 3, 0, 7, 'a', 'p', 'p', 'w', 'i', 'd', 'g', 'e', 't', 's'};
        assert(request_len == sizeof(expected));
        assert(memcmp(request, expected, sizeof(expected)) == 0);
        assert(mock->executor_len <= capacity);
        memcpy(response, mock->executor, mock->executor_len);
        *response_len = mock->executor_len;
    } else if (operation == PRDB_SDK_EXECUTOR_OP_OPEN_SCAN) {
        static const uint8_t expected[20] = {
            0, 1, 6, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22,
            0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0, 0};
        static const uint8_t reply[18] = {
            0, 1, 7, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
            0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44};
        assert(request_len == sizeof(expected));
        assert(memcmp(request, expected, sizeof(expected)) == 0);
        assert(sizeof(reply) <= capacity);
        memcpy(response, reply, sizeof(reply));
        *response_len = sizeof(reply);
    } else if (operation == PRDB_SDK_EXECUTOR_OP_NEXT_BATCH) {
        static const uint8_t expected[24] = {
            0, 1, 7, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
            0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
            0, 1, 0, 0, 4, 0};
        assert(request_len == sizeof(expected));
        assert(memcmp(request, expected, sizeof(expected)) == 0);
        assert(mock->batch_len <= capacity);
        memcpy(response, mock->batch, mock->batch_len);
        *response_len = mock->batch_len;
    } else if (operation == PRDB_SDK_EXECUTOR_OP_CLOSE_CURSOR) {
        static const uint8_t expected[] = {
            0, 1, 7, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
            0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55};
        static const uint8_t reply[] = {0, 1, 0, 0};
        assert(request_len == sizeof(expected));
        assert(memcmp(request, expected, sizeof(expected)) == 0);
        assert(sizeof(reply) <= capacity);
        memcpy(response, reply, sizeof(reply));
        *response_len = sizeof(reply);
    } else {
        assert(!"unexpected executor operation");
    }
    ++mock->executor_calls;
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
    memset(p + offset, 0x11, 16); offset += 16;
    p[offset++] = handle_kind;
    memset(p + offset, handle_fill, 15); offset += 15;
    put_name(p, &offset, "widgets");
    put_u16(p, &offset, 1);
    memset(p + offset, 0x33, 16); offset += 16;
    put_u32(p, &offset, 0);
    p[offset++] = 1;
    p[offset++] = PRDB_SDK_VALUE_TYPE_INTEGER;
    p[offset++] = 0; p[offset++] = 0; p[offset++] = 0; p[offset++] = 0;
    put_name(p, &offset, "id");
    return offset;
}

typedef struct result_state {
    uint32_t calls;
    uint32_t fail;
    const prdb_sdk_sql_column_shape_v2 *columns;
    uint8_t encoded[128];
    uint64_t encoded_len;
    prdb_sdk_value_v1 cell_scratch[1];
} result_state;

static int32_t on_row(void *context, const prdb_sdk_row_v1 *row,
                      const prdb_sdk_select_binding_v2 *binding) {
    result_state *result = (result_state *)context;
    assert(binding->projection_count == 1 && binding->projection_ordinals[0] == 0);
    assert(prdb_sdk_sql_encode_result_row_v1(
               binding, result->columns, 1, row, result->cell_scratch, 1,
               result->encoded, sizeof(result->encoded),
               &result->encoded_len) == PRDB_SDK_SQL_RESULT_OK);
    ++result->calls;
    return result->fail ? 1 : 0;
}

int main(void) {
    static const uint8_t sql[] = "SELECT id FROM widgets WHERE id >= $1 LIMIT 1";
    static const uint8_t zero_limit_sql[] = "SELECT id FROM widgets LIMIT 0";
    static const uint8_t schema[] = "app";
    static const uint8_t parameter_bytes[] = {0, 0, 0, 4};
    uint8_t planner_payload[256] = {0};
    uint8_t executor_payload[256] = {0};
    uint8_t batch_payload[] = {
        0, 1, 0,
        7, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
        0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
        0, 1,
        8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
        0, 1,
        0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0, 4, 0, 0, 0, 5};
    uint32_t planner_len = make_relation(planner_payload,
                                         PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99);
    uint32_t executor_len = make_relation(executor_payload, 6, 0x22);
    mock_host mock = {planner_payload, planner_len, executor_payload,
                      executor_len, batch_payload, sizeof(batch_payload), 0, 0, 0};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             &mock, executor_request, planner_request, NULL,
                             statement_failure};
    prdb_sdk_value_v1 parameter = {sizeof(parameter),
                                   PRDB_SDK_VALUE_ABI_V1_VERSION,
                                   PRDB_SDK_VALUE_STATE_VALUE,
                                   PRDB_SDK_VALUE_TYPE_INTEGER,
                                   parameter_bytes, sizeof(parameter_bytes)};
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[1024] = {0};
    prdb_sdk_sql_column_shape_v2 planner_columns[1] = {{0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[1] = {{0}};
    prdb_sdk_value_v1 cell_scratch[1] = {{0}};
    result_state result = {0};
    result.columns = executor_columns;
    assert(sizeof(prdb_host_api_v3) == 48);
    assert(sizeof(prdb_query_descriptor_v3_record) == 16);
    assert(sizeof(prdb_invocation_v3) == 24);
    assert(prdb_sdk_sql_execute_select_v2(
               &host, schema, sizeof(schema) - 1, sql, sizeof(sql) - 1,
               &parameter, 1, 1, 1024, planner_response,
               sizeof(planner_response), executor_response,
               sizeof(executor_response), batch_response,
               sizeof(batch_response), planner_columns, 1, executor_columns,
               1, cell_scratch, 1, on_row, &result) ==
           PRDB_SDK_SQL_EXEC_OK);
    assert(mock.planner_calls == 1 && mock.executor_calls == 4);
    assert(result.calls == 1);
    {
        static const uint8_t expected_result[] = {
            'S', 'Q', 'L', 'R', 0, 1, 0, 1,
            0, 2, 'i', 'd', 0, 0, 0, 3, 0, 0, 0,
            0, 0, 0, 4, 0, 0, 0, 5};
        assert(result.encoded_len == sizeof(expected_result));
        assert(memcmp(result.encoded, expected_result,
                      sizeof(expected_result)) == 0);
    }
    assert(mock.statement_failed == 0);

    mock.executor_calls = 0;
    result.calls = 0;
    result.fail = 1;
    assert(prdb_sdk_sql_execute_select_v2(
               &host, schema, sizeof(schema) - 1, sql, sizeof(sql) - 1,
               &parameter, 1, 1, 1024, planner_response,
               sizeof(planner_response), executor_response,
               sizeof(executor_response), batch_response,
               sizeof(batch_response), planner_columns, 1, executor_columns,
               1, cell_scratch, 1, on_row, &result) ==
           PRDB_SDK_SQL_EXEC_RESULT_ERROR);
    assert(mock.executor_calls == 4 && result.calls == 1);
    assert(mock.statement_failed == 1);

    mock.executor_calls = 0;
    mock.statement_failed = 0;
    result.calls = 0;
    result.fail = 0;
    assert(prdb_sdk_sql_execute_select_v2(
               &host, schema, sizeof(schema) - 1, zero_limit_sql,
               sizeof(zero_limit_sql) - 1, NULL, 0, 1, 1024,
               planner_response, sizeof(planner_response), executor_response,
               sizeof(executor_response), batch_response,
               sizeof(batch_response), planner_columns, 1, executor_columns,
               1, cell_scratch, 1, on_row, &result) ==
           PRDB_SDK_SQL_EXEC_OK);
    assert(mock.executor_calls == 1 && result.calls == 0);
    assert(mock.statement_failed == 0);

    parameter.type_code = PRDB_SDK_VALUE_TYPE_BIGINT;
    mock.executor_calls = 0;
    result.calls = 0;
    result.fail = 0;
    assert(prdb_sdk_sql_execute_select_v2(
               &host, schema, sizeof(schema) - 1, sql, sizeof(sql) - 1,
               &parameter, 1, 1, 1024, planner_response,
               sizeof(planner_response), executor_response,
               sizeof(executor_response), batch_response,
               sizeof(batch_response), planner_columns, 1, executor_columns,
               1, cell_scratch, 1, on_row, &result) ==
           PRDB_SDK_SQL_EXEC_BIND_ERROR);
    assert(mock.executor_calls == 1 && result.calls == 0);
    assert(mock.statement_failed == 1);
    return 0;
}
