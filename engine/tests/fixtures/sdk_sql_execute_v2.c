#include "prdb_sdk_sql_exec_v2.h"
#include "prdb_sdk_sql_exec_v3.h"
#include "prdb_sdk_sql_exec_v4.h"
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
    uint32_t accept_multi_insert;
    uint32_t multi_insert_calls;
    uint32_t fail_multi_insert_at;
    uint32_t cancellation_checks;
    uint32_t cancel_after_checks;
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
    if (operation == PRDB_SDK_EXECUTOR_OP_CHECK_CANCELLATION) {
        uint8_t reply[] = {0, 1, 0};
        assert(request_len == 0 && sizeof(reply) <= capacity);
        ++mock->cancellation_checks;
        if (mock->cancel_after_checks != 0 &&
            mock->cancellation_checks >= mock->cancel_after_checks) {
            reply[2] = 1;
        }
        memcpy(response, reply, sizeof(reply));
        *response_len = sizeof(reply);
    } else if (operation == PRDB_SDK_EXECUTOR_OP_RESOLVE_RELATION) {
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
        static const uint8_t continuation_two_candidates[24] = {
            0, 1, 7, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
            0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
            0, 2, 0, 0, 4, 0};
        static const uint8_t finished_reply[5] = {0, 1, 1, 0, 0};
        assert(request_len == sizeof(expected));
        if (mock->executor_calls >= 3) {
            const uint8_t *expected_continuation =
                mock->batch_len > 60 ? continuation_two_candidates
                                     : continuation_request;
            assert(memcmp(request, expected_continuation,
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
        static const uint8_t reply[] = {0, 1, 0};
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
    } else if (operation == PRDB_SDK_EXECUTOR_OP_STAGE_DELETE) {
        static const uint8_t expected[] = {
            0, 1, 8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
        static const uint8_t reply[] = {0, 1, 0};
        assert(request_len == sizeof(expected));
        assert(memcmp(request, expected, sizeof(expected)) == 0);
        assert(sizeof(reply) <= capacity);
        memcpy(response, reply, sizeof(reply));
        *response_len = sizeof(reply);
    } else if (operation == PRDB_SDK_EXECUTOR_OP_STAGE_INSERT) {
        if (mock->accept_multi_insert) {
            static const uint8_t reply[18] = {
                0, 1, 8, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
                0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66};
            static const uint8_t expected_handle[18] = {
                0, 1, 6, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22,
                0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22};
            uint32_t offset = 20;
            assert(request_len == 30);
            assert(memcmp(request, expected_handle, sizeof(expected_handle)) == 0);
            assert(request[18] == 0 && request[19] == 1);
            uint8_t expected_value = (uint8_t)(42 + mock->multi_insert_calls);
            assert(request[offset++] == PRDB_SDK_EXECUTOR_INSERT_VALUE);
            assert(request[offset++] == PRDB_SDK_VALUE_TYPE_INTEGER);
            assert(request[offset++] == 0 && request[offset++] == 0 &&
                   request[offset++] == 0 && request[offset++] == 4);
            assert(request[offset++] == 0 && request[offset++] == 0 &&
                   request[offset++] == 0 && request[offset++] == expected_value);
            assert(offset == request_len);
            ++mock->multi_insert_calls;
            if (mock->fail_multi_insert_at == mock->multi_insert_calls) {
                *response_len = 0;
            } else {
                assert(sizeof(reply) <= capacity);
                memcpy(response, reply, sizeof(reply));
                *response_len = sizeof(reply);
            }
        } else {
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
        }
    } else {
        assert(!"unexpected executor operation");
    }
    if (operation != PRDB_SDK_EXECUTOR_OP_CHECK_CANCELLATION) {
        ++mock->executor_calls;
    }
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

static uint32_t make_two_sort_rows(uint8_t *p, uint32_t first,
                                   uint32_t second) {
    uint32_t offset = 0;
    put_u16(p, &offset, 1);
    p[offset++] = 0;
    p[offset++] = 7;
    memset(p + offset, 0x55, 15); offset += 15;
    put_u16(p, &offset, 2);
    for (uint32_t value_index = 0; value_index < 2; ++value_index) {
        uint32_t value = value_index == 0 ? first : second;
        p[offset++] = 8;
        memset(p + offset, (int)(value_index + 1), 15); offset += 15;
        put_u16(p, &offset, 1);
        p[offset++] = 0;
        p[offset++] = PRDB_SDK_VALUE_TYPE_INTEGER;
        put_u32(p, &offset, 4);
        put_u32(p, &offset, value);
    }
    return offset;
}

typedef struct result_state {
    uint32_t calls;
    uint32_t fail;
    uint8_t last_value;
    const prdb_sdk_sql_column_shape_v2 *columns;
    uint8_t encoded[128];
    uint64_t encoded_len;
    prdb_sdk_value_v1 cell_scratch[1];
} result_state;

typedef struct group_result_state {
    uint32_t calls;
    uint32_t keys[4];
    uint64_t counts[4];
    mock_host *mock;
    const prdb_sdk_sql_column_shape_v2 *columns;
    uint8_t encoded[128];
    uint64_t encoded_len;
} group_result_state;

static int32_t on_group(void *context, const prdb_sdk_value_v1 *key,
                        uint64_t count,
                        const prdb_sdk_sql_bound_column_v2 *column) {
    group_result_state *result = (group_result_state *)context;
    int32_t value = 0;
    assert(result->mock != NULL && result->mock->executor_calls == 4);
    assert(result->calls < 4);
    assert(column->ordinal == 0 &&
           column->type_code == PRDB_SDK_VALUE_TYPE_INTEGER);
    assert(prdb_sdk_value_get_integer_v1(key, &value) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(prdb_sdk_sql_encode_group_count_result_v1(
               &result->columns[column->ordinal], key, count,
               result->encoded, sizeof(result->encoded),
               &result->encoded_len) == PRDB_SDK_SQL_RESULT_OK);
    result->keys[result->calls] = (uint32_t)value;
    result->counts[result->calls] = count;
    ++result->calls;
    return 0;
}

static int32_t on_row(void *context, const prdb_sdk_row_v1 *row,
                      const prdb_sdk_select_binding_v2 *binding) {
    result_state *result = (result_state *)context;
    assert(binding->projection_count == 1 && binding->projection_ordinals[0] == 0);
    assert(prdb_sdk_sql_encode_result_row_v1(
               binding, result->columns, 1, row, result->cell_scratch, 1,
               result->encoded, sizeof(result->encoded),
               &result->encoded_len) == PRDB_SDK_SQL_RESULT_OK);
    result->last_value = result->encoded[result->encoded_len - 1];
    ++result->calls;
    return result->fail ? 1 : 0;
}

int main(void) {
    static const uint8_t sql[] = "SELECT id FROM widgets WHERE id >= $1 LIMIT 1";
    static const uint8_t zero_limit_sql[] = "SELECT id FROM widgets LIMIT 0";
    static const uint8_t sort_limit_sql[] =
        "SELECT id FROM widgets ORDER BY id DESC LIMIT 1";
    static const uint8_t sort_all_sql[] =
        "SELECT id FROM widgets ORDER BY id DESC";
    static const uint8_t group_sql[] =
        "SELECT id, COUNT(*) FROM widgets GROUP BY id";
    static const uint8_t group_limit_sql[] =
        "SELECT id, COUNT(*) FROM widgets GROUP BY id LIMIT 1";
    static const uint8_t group_filter_sql[] =
        "SELECT id, COUNT(*) FROM widgets WHERE id >= $1 GROUP BY id";
    static const uint8_t insert_sql[] =
        "INSERT INTO widgets VALUES ($1, NULL, DEFAULT)";
    static const uint8_t multi_insert_sql[] =
        "INSERT INTO widgets VALUES ($1), ($2)";
    static const uint8_t update_sql[] =
        "UPDATE widgets SET id = $1 WHERE id >= $1";
    static const uint8_t delete_sql[] = "DELETE FROM widgets WHERE id >= $1";
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
                      executor_len, batch_payload, sizeof(batch_payload),
                      0, 0, 0, 0, 0, 0, 0, 0};
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
    static const uint8_t multi_parameter_bytes[2][4] = {
        {0, 0, 0, 42}, {0, 0, 0, 43}};
    prdb_sdk_value_v1 multi_parameters[2] = {{0}};
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[1024] = {0};
    uint8_t materialized_rows[256] = {0};
    prdb_sdk_sql_column_shape_v2 planner_columns[3] = {{0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[3] = {{0}};
    prdb_sdk_value_v1 cell_scratch[1] = {{0}};
    result_state result = {0};
    result.columns = executor_columns;
    for (uint32_t index = 0; index < 2; ++index) {
        multi_parameters[index] = (prdb_sdk_value_v1){
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
            multi_parameter_bytes[index], sizeof(multi_parameter_bytes[index])};
    }
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

    {
        uint8_t sort_batch[128] = {0};
        uint32_t sort_batch_len = make_two_sort_rows(sort_batch, 3, 5);
        mock.batch = sort_batch;
        mock.batch_len = sort_batch_len;
        mock.executor_calls = 0;
        result.calls = 0;
        result.last_value = 0;
        mock.statement_failed = 0;
        assert(prdb_sdk_sql_execute_select_v3(
                   &host, schema, sizeof(schema) - 1, sort_limit_sql,
                   sizeof(sort_limit_sql) - 1, NULL, 0, 2, 1024,
                   planner_response, sizeof(planner_response),
                   executor_response, sizeof(executor_response),
                   batch_response, sizeof(batch_response), planner_columns, 1,
                   executor_columns, 1, cell_scratch, 1, materialized_rows,
                   sizeof(materialized_rows), on_row, &result) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(result.calls == 1 && result.last_value == 5);
        assert(mock.executor_calls == 4 && mock.statement_failed == 0);

        mock.executor_calls = 0;
        result.calls = 0;
        result.last_value = 0;
        mock.statement_failed = 0;
        assert(prdb_sdk_sql_execute_select_v3(
                   &host, schema, sizeof(schema) - 1, sort_all_sql,
                   sizeof(sort_all_sql) - 1, NULL, 0, 2, 1024,
                   planner_response, sizeof(planner_response),
                   executor_response, sizeof(executor_response),
                   batch_response, sizeof(batch_response), planner_columns, 1,
                   executor_columns, 1, cell_scratch, 1, materialized_rows,
                   sizeof(materialized_rows), on_row, &result) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(result.calls == 2 && result.last_value == 3);
        assert(mock.executor_calls == 4 && mock.statement_failed == 0);

        mock.executor_calls = 0;
        result.calls = 0;
        result.last_value = 0;
        mock.statement_failed = 0;
        assert(prdb_sdk_sql_execute_select_v3(
                   &host, schema, sizeof(schema) - 1, sort_limit_sql,
                   sizeof(sort_limit_sql) - 1, NULL, 0, 2, 1024,
                   planner_response, sizeof(planner_response),
                   executor_response, sizeof(executor_response),
                   batch_response, sizeof(batch_response), planner_columns, 1,
                   executor_columns, 1, cell_scratch, 1, materialized_rows, 40,
                   on_row, &result) ==
               PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT);
        assert(result.calls == 0 && mock.executor_calls == 4);
        assert(mock.statement_failed == 1);
        mock.batch = batch_payload;
        mock.batch_len = sizeof(batch_payload);
    }

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

    mock.accept_multi_insert = 1;
    mock.planner_len = make_relation(
        planner_payload, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99);
    mock.executor_len = make_relation(executor_payload, 6, 0x22);
    mock.planner_calls = 0;
    mock.executor_calls = 0;
    mock.multi_insert_calls = 0;
    mock.cancellation_checks = 0;
    mock.cancel_after_checks = 0;
    mock.fail_multi_insert_at = 0;
    mock.statement_failed = 0;
    uint32_t inserted_count = UINT32_C(0xfeed);
    int32_t multi_status = prdb_sdk_sql_execute_insert_v2(
               &host, schema, sizeof(schema) - 1, multi_insert_sql,
               sizeof(multi_insert_sql) - 1, multi_parameters, 2,
               planner_response, sizeof(planner_response), executor_response,
               sizeof(executor_response), insert_request,
               sizeof(insert_request), planner_columns, 3, executor_columns,
               3, insert_values, 3, &inserted_count);
    assert(multi_status == PRDB_SDK_SQL_EXEC_OK);
    assert(inserted_count == 2 && mock.multi_insert_calls == 2);
    assert(mock.cancellation_checks == 2 && mock.executor_calls == 3);
    assert(mock.planner_calls == 1 && mock.statement_failed == 0);

    mock.planner_calls = 0;
    mock.executor_calls = 0;
    mock.multi_insert_calls = 0;
    mock.cancellation_checks = 0;
    mock.fail_multi_insert_at = 2;
    mock.statement_failed = 0;
    inserted_count = UINT32_C(0xbeef);
    assert(prdb_sdk_sql_execute_insert_v2(
               &host, schema, sizeof(schema) - 1, multi_insert_sql,
               sizeof(multi_insert_sql) - 1, multi_parameters, 2,
               planner_response, sizeof(planner_response), executor_response,
               sizeof(executor_response), insert_request,
               sizeof(insert_request), planner_columns, 3, executor_columns,
               3, insert_values, 3, &inserted_count) ==
           PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    assert(inserted_count == UINT32_C(0xbeef));
    assert(mock.multi_insert_calls == 2 && mock.statement_failed == 1);

    mock.planner_calls = 0;
    mock.executor_calls = 0;
    mock.multi_insert_calls = 0;
    mock.cancellation_checks = 0;
    mock.fail_multi_insert_at = 0;
    mock.cancel_after_checks = 2;
    mock.statement_failed = 0;
    inserted_count = UINT32_C(0xcafe);
    assert(prdb_sdk_sql_execute_insert_v2(
               &host, schema, sizeof(schema) - 1, multi_insert_sql,
               sizeof(multi_insert_sql) - 1, multi_parameters, 2,
               planner_response, sizeof(planner_response), executor_response,
               sizeof(executor_response), insert_request,
               sizeof(insert_request), planner_columns, 3, executor_columns,
               3, insert_values, 3, &inserted_count) ==
           PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    assert(inserted_count == UINT32_C(0xcafe));
    assert(mock.multi_insert_calls == 1 && mock.statement_failed == 1);
    mock.accept_multi_insert = 0;
    mock.cancel_after_checks = 0;
    mock.planner_len = make_relation_three(
        planner_payload, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99);
    mock.executor_len = make_relation_three(executor_payload, 6, 0x22);

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

    mock.batch = batch_payload;
    mock.batch_len = sizeof(batch_payload);
    mock.planner_calls = 0;
    mock.executor_calls = 0;
    mock.statement_failed = 0;
    uint32_t deleted_count = 0;
    assert(prdb_sdk_sql_execute_delete_v1(
               &host, schema, sizeof(schema) - 1, delete_sql,
               sizeof(delete_sql) - 1, &parameter, 1, 1, 1024,
               planner_response, sizeof(planner_response), executor_response,
               sizeof(executor_response), batch_response,
               sizeof(batch_response), planner_columns, 1, executor_columns,
               1, &update_binding,
               sizeof(update_binding), update_cell_scratch, 1, update_targets,
               PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS, &deleted_count) ==
           PRDB_SDK_SQL_EXEC_OK);
    assert(deleted_count == 1 && mock.planner_calls == 1 &&
           mock.executor_calls == 5 && mock.statement_failed == 0);

    mock.batch = two_rows_batch;
    mock.batch_len = two_rows_batch_len;
    mock.planner_calls = 0;
    mock.executor_calls = 0;
    mock.statement_failed = 0;
    deleted_count = 888;
    assert(prdb_sdk_sql_execute_delete_v1(
               &host, schema, sizeof(schema) - 1, delete_sql,
               sizeof(delete_sql) - 1, &parameter, 1, 2, 1024,
               planner_response, sizeof(planner_response), executor_response,
               sizeof(executor_response), batch_response,
               sizeof(batch_response), planner_columns, 1, executor_columns,
               1, &update_binding,
               sizeof(update_binding), update_cell_scratch, 1, update_targets,
               1, &deleted_count) == PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT);
    assert(deleted_count == 888 && mock.planner_calls == 1 &&
           mock.executor_calls == 4 && mock.statement_failed == 1);

    {
        uint8_t group_batch[128] = {0};
        uint32_t group_batch_len = make_two_sort_rows(group_batch, 3, 5);
        uint8_t group_key_bytes[16] = {0};
        group_result_state groups = {0};
        groups.mock = &mock;
        groups.columns = executor_columns;
        mock.batch = group_batch;
        mock.batch_len = group_batch_len;
        mock.planner_calls = 0;
        mock.executor_calls = 0;
        mock.statement_failed = 0;
        assert(prdb_sdk_sql_execute_group_count_v4(
                   &host, schema, sizeof(schema) - 1, group_sql,
                   sizeof(group_sql) - 1, NULL, 0, 2, 1024,
                   planner_response, sizeof(planner_response),
                   executor_response, sizeof(executor_response),
                   batch_response, sizeof(batch_response), planner_columns, 1,
                   executor_columns, 1, cell_scratch, 1, group_key_bytes,
                   sizeof(group_key_bytes), on_group, &groups) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(groups.calls == 2 && groups.keys[0] == 3 &&
               groups.counts[0] == 1 && groups.keys[1] == 5 &&
               groups.counts[1] == 1);
        assert(groups.encoded_len == 53 && groups.encoded[52] == 1);
        assert(mock.executor_calls == 4 && mock.statement_failed == 0);

        group_batch_len = make_two_sort_rows(group_batch, 3, 3);
        mock.batch_len = group_batch_len;
        groups = (group_result_state){0};
        groups.mock = &mock;
        groups.columns = executor_columns;
        mock.executor_calls = 0;
        assert(prdb_sdk_sql_execute_group_count_v4(
                   &host, schema, sizeof(schema) - 1, group_sql,
                   sizeof(group_sql) - 1, NULL, 0, 2, 1024,
                   planner_response, sizeof(planner_response),
                   executor_response, sizeof(executor_response),
                   batch_response, sizeof(batch_response), planner_columns, 1,
                   executor_columns, 1, cell_scratch, 1, group_key_bytes,
                   sizeof(group_key_bytes), on_group, &groups) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(groups.calls == 1 && groups.keys[0] == 3 &&
               groups.counts[0] == 2 && mock.statement_failed == 0);
        assert(groups.encoded_len == 53 && groups.encoded[52] == 2);

        group_batch_len = make_two_sort_rows(group_batch, 3, 5);
        mock.batch_len = group_batch_len;
        groups = (group_result_state){0};
        groups.mock = &mock;
        groups.columns = executor_columns;
        mock.executor_calls = 0;
        assert(prdb_sdk_sql_execute_group_count_v4(
                   &host, schema, sizeof(schema) - 1, group_filter_sql,
                   sizeof(group_filter_sql) - 1, &parameter, 1, 2, 1024,
                   planner_response, sizeof(planner_response),
                   executor_response, sizeof(executor_response),
                   batch_response, sizeof(batch_response), planner_columns, 1,
                   executor_columns, 1, cell_scratch, 1, group_key_bytes,
                   sizeof(group_key_bytes), on_group, &groups) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(groups.calls == 1 && groups.keys[0] == 5 &&
               groups.counts[0] == 1 && mock.statement_failed == 0);

        groups = (group_result_state){0};
        groups.mock = &mock;
        groups.columns = executor_columns;
        mock.executor_calls = 0;
        assert(prdb_sdk_sql_execute_group_count_v4(
                   &host, schema, sizeof(schema) - 1, group_limit_sql,
                   sizeof(group_limit_sql) - 1, NULL, 0, 2, 1024,
                   planner_response, sizeof(planner_response),
                   executor_response, sizeof(executor_response),
                   batch_response, sizeof(batch_response), planner_columns, 1,
                   executor_columns, 1, cell_scratch, 1, group_key_bytes,
                   sizeof(group_key_bytes), on_group, &groups) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(groups.calls == 1 && groups.keys[0] == 3 &&
               groups.counts[0] == 1 && mock.statement_failed == 0);

        groups = (group_result_state){0};
        groups.mock = &mock;
        groups.columns = executor_columns;
        mock.executor_calls = 0;
        mock.statement_failed = 0;
        assert(prdb_sdk_sql_execute_group_count_v4(
                   &host, schema, sizeof(schema) - 1, group_sql,
                   sizeof(group_sql) - 1, NULL, 0, 2, 1024,
                   planner_response, sizeof(planner_response),
                   executor_response, sizeof(executor_response),
                   batch_response, sizeof(batch_response), planner_columns, 1,
                   executor_columns, 1, cell_scratch, 1, group_key_bytes,
                   2, on_group, &groups) ==
               PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT);
        assert(groups.calls == 0 && mock.executor_calls == 4 &&
               mock.statement_failed == 1);
    }
    return 0;
}
