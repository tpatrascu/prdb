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
        static const uint8_t expected_two_candidates[24] = {
            0, 1, 7, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
            0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
            0, 2, 0, 0, 4, 0};
        static const uint8_t continuation_request[24] = {
            0, 1, 7, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
            0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
            0, 1, 0, 0, 4, 0};
        static const uint8_t finished_reply[5] = {0, 1, 1, 0, 0};
        assert(request_len == sizeof(expected));
        if (mock->executor_calls >= 3) {
            assert(memcmp(request, continuation_request,
                          sizeof(continuation_request)) == 0);
            assert(sizeof(finished_reply) <= capacity);
            memcpy(response, finished_reply, sizeof(finished_reply));
            *response_len = sizeof(finished_reply);
        } else {
            const uint8_t *expected_request =
                mock->batch_len > 60 ? expected_two_candidates : expected;
            assert(memcmp(request, expected_request, sizeof(expected)) == 0);
            assert(mock->batch_len <= capacity);
            memcpy(response, mock->batch, mock->batch_len);
            *response_len = mock->batch_len;
        }
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
    } else if (operation == PRDB_SDK_EXECUTOR_OP_STAGE_UPDATE) {
        static const uint8_t expected[] = {
            0, 1, 8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
            0, 1, 0, 0, 0, 0, 0, PRDB_SDK_VALUE_TYPE_INTEGER,
            0, 0, 0, 4, 0, 0, 0, 4};
        static const uint8_t reply[] = {0, 1, 0};
        assert(request_len == sizeof(expected));
        assert(memcmp(request, expected, sizeof(expected)) == 0);
        assert(sizeof(reply) <= capacity);
        memcpy(response, reply, sizeof(reply));
        *response_len = sizeof(reply);
    } else if (operation == PRDB_SDK_EXECUTOR_OP_STAGE_INSERT) {
        static const uint8_t expected[] = {
            0, 1, 6, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22,
            0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22,
            0, 3, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0, 4,
            0, 0, 0, 42, 1, 2};
        static const uint8_t reply[18] = {
            0, 1, 8, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
            0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66};
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

static uint32_t make_relation_three(uint8_t *p, uint8_t handle_kind,
                                    uint8_t handle_fill) {
    static const char *names[] = {"id", "label", "payload"};
    static const uint8_t types[] = {PRDB_SDK_VALUE_TYPE_INTEGER,
                                    PRDB_SDK_VALUE_TYPE_TEXT,
                                    PRDB_SDK_VALUE_TYPE_BYTEA};
    uint32_t offset = 0;
    put_u16(p, &offset, 1);
    p[offset++] = 0;
    memset(p + offset, 0x11, 16); offset += 16;
    p[offset++] = handle_kind;
    memset(p + offset, handle_fill, 15); offset += 15;
    put_name(p, &offset, "widgets");
    put_u16(p, &offset, 3);
    for (uint32_t index = 0; index < 3; ++index) {
        memset(p + offset, (int)(0x33 + index), 16); offset += 16;
        put_u32(p, &offset, index);
        p[offset++] = 1;
        p[offset++] = types[index];
        p[offset++] = 0; p[offset++] = 0; p[offset++] = 0; p[offset++] = 0;
        put_name(p, &offset, names[index]);
    }
    return offset;
}

static uint32_t make_two_matching_rows(uint8_t *p) {
    uint32_t offset = 0;
    put_u16(p, &offset, 1);
    p[offset++] = 0;
    p[offset++] = 7;
    memset(p + offset, 0x55, 15); offset += 15;
    put_u16(p, &offset, 2);
    for (uint8_t row = 0; row < 2; ++row) {
        p[offset++] = 8;
        memset(p + offset, (int)(row + 1), 15); offset += 15;
        put_u16(p, &offset, 1);
        p[offset++] = 0;
        p[offset++] = PRDB_SDK_VALUE_TYPE_INTEGER;
        put_u32(p, &offset, 4);
        put_u32(p, &offset, 5);
    }
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
    static const uint8_t insert_sql[] =
        "INSERT INTO widgets VALUES ($1, NULL, DEFAULT)";
    static const uint8_t update_sql[] =
        "UPDATE widgets SET id = $1 WHERE id >= $1";
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
    const uint8_t insert_parameter_bytes[] = {0, 0, 0, 42};
    prdb_sdk_value_v1 insert_parameter = {
        sizeof(insert_parameter), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
        insert_parameter_bytes, sizeof(insert_parameter_bytes)};
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[1024] = {0};
    prdb_sdk_sql_column_shape_v2 planner_columns[3] = {{0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[3] = {{0}};
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

    mock.planner_calls = 0;
    mock.executor_calls = 0;
    mock.statement_failed = 0;
    mock.planner_len = make_relation_three(
        planner_payload, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99);
    mock.executor_len = make_relation_three(executor_payload, 6, 0x22);
    uint8_t insert_request[64] = {0};
    uint8_t inserted_row_handle[16] = {0};
    prdb_sdk_executor_insert_value_v1 insert_values[3] = {{0}};
    assert(prdb_sdk_sql_execute_insert_v1(
               &host, schema, sizeof(schema) - 1, insert_sql,
               sizeof(insert_sql) - 1, &insert_parameter, 1,
               planner_response, sizeof(planner_response), executor_response,
               sizeof(executor_response), insert_request,
               sizeof(insert_request), planner_columns, 3, executor_columns,
               3, insert_values, 3, inserted_row_handle) ==
           PRDB_SDK_SQL_EXEC_OK);
    assert(mock.planner_calls == 1 && mock.executor_calls == 2);
    assert(inserted_row_handle[0] == 8 && mock.statement_failed == 0);

    insert_parameter.type_code = PRDB_SDK_VALUE_TYPE_BIGINT;
    mock.planner_calls = 0;
    mock.executor_calls = 0;
    mock.statement_failed = 0;
    assert(prdb_sdk_sql_execute_insert_v1(
               &host, schema, sizeof(schema) - 1, insert_sql,
               sizeof(insert_sql) - 1, &insert_parameter, 1,
               planner_response, sizeof(planner_response), executor_response,
               sizeof(executor_response), insert_request,
               sizeof(insert_request), planner_columns, 3, executor_columns,
               3, insert_values, 3, inserted_row_handle) ==
           PRDB_SDK_SQL_EXEC_BIND_ERROR);
    assert(mock.planner_calls == 1 && mock.executor_calls == 1);
    assert(mock.statement_failed == 1);

    planner_len = make_relation(planner_payload,
                                PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99);
    executor_len = make_relation(executor_payload, 6, 0x22);
    mock.planner_len = planner_len;
    mock.executor_len = executor_len;
    mock.planner_calls = 0;
    mock.executor_calls = 0;
    mock.statement_failed = 0;
    parameter.type_code = PRDB_SDK_VALUE_TYPE_INTEGER;
    uint8_t update_request[64] = {0};
    uint8_t update_targets[16 * PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS] = {0};
    prdb_sdk_select_binding_v2 update_binding = {0};
    prdb_sdk_value_v1 update_cell_scratch[1] = {{0}};
    prdb_sdk_executor_update_patch_v1 update_patches[1] = {{0}};
    uint32_t updated_count = 0;
    assert(prdb_sdk_sql_execute_update_v1(
               &host, schema, sizeof(schema) - 1, update_sql,
               sizeof(update_sql) - 1, &parameter, 1, 1, 1024,
               planner_response, sizeof(planner_response), executor_response,
               sizeof(executor_response), batch_response,
               sizeof(batch_response), update_request, sizeof(update_request),
               planner_columns, 1, executor_columns, 1, &update_binding,
               sizeof(update_binding), update_cell_scratch, 1, update_targets,
               PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS, update_patches, 1,
               &updated_count) == PRDB_SDK_SQL_EXEC_OK);
    assert(updated_count == 1 && mock.planner_calls == 1 &&
           mock.executor_calls == 5 && mock.statement_failed == 0);

    uint8_t two_rows_batch[128] = {0};
    uint32_t two_rows_batch_len = make_two_matching_rows(two_rows_batch);
    mock.batch = two_rows_batch;
    mock.batch_len = two_rows_batch_len;
    mock.planner_calls = 0;
    mock.executor_calls = 0;
    mock.statement_failed = 0;
    updated_count = 777;
    assert(prdb_sdk_sql_execute_update_v1(
               &host, schema, sizeof(schema) - 1, update_sql,
               sizeof(update_sql) - 1, &parameter, 1, 2, 1024,
               planner_response, sizeof(planner_response), executor_response,
               sizeof(executor_response), batch_response,
               sizeof(batch_response), update_request, sizeof(update_request),
               planner_columns, 1, executor_columns, 1, &update_binding,
               sizeof(update_binding), update_cell_scratch, 1, update_targets,
               1, update_patches, 1, &updated_count) ==
           PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT);
    assert(updated_count == 777 && mock.planner_calls == 1 &&
           mock.executor_calls == 4 && mock.statement_failed == 1);
    return 0;
}
