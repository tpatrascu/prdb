#include "prdb_sdk_sql_exec_v8.h"
#include "prdb_sdk_sql_exec_v16.h"

#include <assert.h>
#include <string.h>

typedef struct mock_state {
    uint8_t planner[2][256];
    uint32_t planner_len[2];
    uint8_t executor[2][256];
    uint32_t executor_len[2];
    uint8_t batch[2][512];
    uint32_t batch_len[2];
    uint32_t planner_calls;
    uint32_t resolve_calls;
    uint32_t open_calls;
    uint32_t check_cancel_calls;
    uint32_t cancel_after_check;
    uint32_t close_calls;
    uint32_t executor_requests;
    uint32_t empty_pages[2];
    uint32_t next_calls[2];
    uint32_t scan_finished[2];
    uint32_t statement_failed;
} mock_state;

typedef struct row_capture {
    mock_state *mock;
    uint32_t calls;
    int32_t left_values[8];
    int32_t right_values[8];
    int32_t callback_status;
} row_capture;

static void put_u16(uint8_t *bytes, uint32_t *offset, uint16_t value) {
    bytes[(*offset)++] = (uint8_t)(value >> 8);
    bytes[(*offset)++] = (uint8_t)value;
}

static void put_u32(uint8_t *bytes, uint32_t *offset, uint32_t value) {
    bytes[(*offset)++] = (uint8_t)(value >> 24);
    bytes[(*offset)++] = (uint8_t)(value >> 16);
    bytes[(*offset)++] = (uint8_t)(value >> 8);
    bytes[(*offset)++] = (uint8_t)value;
}

static void put_name(uint8_t *bytes, uint32_t *offset, const char *name) {
    uint16_t length = (uint16_t)strlen(name);
    put_u16(bytes, offset, length);
    memcpy(bytes + *offset, name, length);
    *offset += length;
}

static uint32_t relation_index(const uint8_t *request, uint64_t request_len) {
    uint16_t schema_len;
    uint16_t relation_len;
    assert(request_len >= 6);
    assert(request[0] == 0 && request[1] == 1);
    schema_len = (uint16_t)(((uint16_t)request[2] << 8) | request[3]);
    relation_len = (uint16_t)(((uint16_t)request[4] << 8) | request[5]);
    assert(request_len == (uint64_t)(6 + schema_len + relation_len));
    assert(schema_len == 3 && memcmp(request + 6, "app", 3) == 0);
    if (relation_len == 8 &&
        memcmp(request + 6 + schema_len, "accounts", 8) == 0) {
        return 0;
    }
    assert(relation_len == 6 &&
           memcmp(request + 6 + schema_len, "orders", 6) == 0);
    return 1;
}

static uint32_t make_relation(uint8_t *bytes, uint32_t index,
                              uint8_t handle_kind) {
    const char *relation_name = index == 0 ? "accounts" : "orders";
    const uint8_t id_fill = (uint8_t)(0x11 + index);
    const uint8_t handle_fill = (uint8_t)(0x21 + index);
    uint32_t offset = 0;
    put_u16(bytes, &offset, 1);
    bytes[offset++] = 0;
    memset(bytes + offset, id_fill, 16); offset += 16;
    bytes[offset++] = handle_kind;
    memset(bytes + offset, handle_fill, 15); offset += 15;
    put_name(bytes, &offset, relation_name);
    put_u16(bytes, &offset, 2);
    for (uint32_t ordinal = 0; ordinal < 2; ++ordinal) {
        memset(bytes + offset, (int)(0x31 + ordinal), 16); offset += 16;
        put_u32(bytes, &offset, ordinal);
        bytes[offset++] = 1;
        bytes[offset++] = PRDB_SDK_VALUE_TYPE_INTEGER;
        bytes[offset++] = 0;
        bytes[offset++] = 0;
        bytes[offset++] = 0;
        bytes[offset++] = 0;
        put_name(bytes, &offset,
                 ordinal == 0 ? "account_id" : "value");
    }
    return offset;
}

static void append_integer_cell(uint8_t *bytes, uint32_t *offset,
                                int32_t value) {
    bytes[(*offset)++] = PRDB_SDK_VALUE_STATE_VALUE;
    bytes[(*offset)++] = PRDB_SDK_VALUE_TYPE_INTEGER;
    put_u32(bytes, offset, 4);
    put_u32(bytes, offset, (uint32_t)value);
}

static uint32_t make_batch(uint8_t *bytes, const int32_t *keys,
                           const uint8_t *key_states,
                           const int32_t *values, uint16_t row_count) {
    uint32_t offset = 0;
    put_u16(bytes, &offset, 1);
    bytes[offset++] = 1;
    put_u16(bytes, &offset, row_count);
    for (uint32_t row = 0; row < row_count; ++row) {
        bytes[offset++] = PRDB_SDK_BATCH_ROW_TOKEN_KIND;
        memset(bytes + offset, (int)(row + 1), 15); offset += 15;
        put_u16(bytes, &offset, 2);
        if (key_states[row] == PRDB_SDK_VALUE_STATE_VALUE) {
            append_integer_cell(bytes, &offset, keys[row]);
        } else {
            bytes[offset++] = key_states[row];
        }
        append_integer_cell(bytes, &offset, values[row]);
    }
    return offset;
}

static int32_t statement_failure(void *context, uint32_t failure_code) {
    mock_state *mock = (mock_state *)context;
    assert(failure_code == PRDB_QUERY_STATEMENT_FAILURE_SQL);
    mock->statement_failed = 1;
    return PRDB_QUERY_CALLBACK_OK;
}

static int32_t planner_request(void *context, uint16_t operation,
                               const uint8_t *request, uint64_t request_len,
                               uint8_t *response, uint64_t capacity,
                               uint64_t *response_len) {
    mock_state *mock = (mock_state *)context;
    uint32_t index;
    assert(operation == PRDB_SDK_PLANNER_OP_DESCRIBE_RELATION);
    index = relation_index(request, request_len);
    assert(mock->planner_len[index] <= capacity);
    memcpy(response, mock->planner[index], mock->planner_len[index]);
    *response_len = mock->planner_len[index];
    ++mock->planner_calls;
    return PRDB_QUERY_CALLBACK_OK;
}

static int32_t executor_request(void *context, uint16_t operation,
                                const uint8_t *request, uint64_t request_len,
                                uint8_t *response, uint64_t capacity,
                                uint64_t *response_len) {
    mock_state *mock = (mock_state *)context;
    ++mock->executor_requests;
    assert(mock->executor_requests <= 256);
    if (operation == PRDB_SDK_EXECUTOR_OP_CHECK_CANCELLATION) {
        uint8_t reply[] = {0, 1, 0};
        assert(request_len == 0 && sizeof(reply) <= capacity);
        ++mock->check_cancel_calls;
        if (mock->cancel_after_check != 0 &&
            mock->check_cancel_calls >= mock->cancel_after_check) {
            reply[2] = 1;
        }
        memcpy(response, reply, sizeof(reply));
        *response_len = sizeof(reply);
    } else if (operation == PRDB_SDK_EXECUTOR_OP_RESOLVE_RELATION) {
        uint32_t index = relation_index(request, request_len);
        assert(mock->executor_len[index] <= capacity);
        memcpy(response, mock->executor[index], mock->executor_len[index]);
        *response_len = mock->executor_len[index];
        ++mock->resolve_calls;
    } else if (operation == PRDB_SDK_EXECUTOR_OP_OPEN_SCAN) {
        uint32_t index;
        assert(request_len == 20 && request[0] == 0 && request[1] == 1 &&
               request[2] == 6);
        index = request[3] == 0x21 ? 0 : 1;
        assert(request[3] == (uint8_t)(0x21 + index));
        response[0] = 0; response[1] = 1; response[2] = 7;
        memset(response + 3, index == 0 ? 0x44 : 0x55, 15);
        *response_len = 18;
        ++mock->open_calls;
    } else if (operation == PRDB_SDK_EXECUTOR_OP_NEXT_BATCH) {
        uint32_t index;
        assert(request_len == 24 && request[0] == 0 && request[1] == 1 &&
               request[2] == 7);
        assert(mock->open_calls == 1 || mock->open_calls == 2);
        index = mock->open_calls - 1;
        if (mock->empty_pages[index] != 0) {
            uint32_t offset = 0;
            put_u16(response, &offset, 1);
            response[offset++] = 0;
            response[offset++] = PRDB_SDK_BATCH_CURSOR_TOKEN_KIND;
            memset(response + offset, (int)(0x61 + index), 15);
            offset += 15;
            put_u16(response, &offset, 0);
            *response_len = offset;
            --mock->empty_pages[index];
        } else {
            assert(mock->batch_len[index] <= capacity);
            memcpy(response, mock->batch[index], mock->batch_len[index]);
            *response_len = mock->batch_len[index];
            mock->scan_finished[index] = 1;
        }
        ++mock->next_calls[index];
    } else if (operation == PRDB_SDK_EXECUTOR_OP_CLOSE_CURSOR) {
        static const uint8_t reply[] = {0, 1, 0};
        assert(request_len == 18 && request[0] == 0 && request[1] == 1 &&
               sizeof(reply) <= capacity);
        memcpy(response, reply, sizeof(reply));
        *response_len = sizeof(reply);
        ++mock->close_calls;
    } else {
        assert(!"unexpected executor operation");
    }
    return PRDB_QUERY_CALLBACK_OK;
}

static int32_t on_row(void *context,
                      const prdb_sdk_value_v1 *projected_values,
                      uint32_t projection_count,
                      const prdb_sdk_select_binding_v8 *binding) {
    row_capture *capture = (row_capture *)context;
    assert(capture->mock->scan_finished[0] == 1 &&
           capture->mock->scan_finished[1] == 1);
    assert(binding->projection_count == 2 && projection_count == 2);
    assert(capture->calls < 8);
    assert(prdb_sdk_value_get_integer_v1(&projected_values[0],
                                         &capture->left_values[capture->calls]) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(prdb_sdk_value_get_integer_v1(&projected_values[1],
                                         &capture->right_values[capture->calls]) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    ++capture->calls;
    return capture->callback_status;
}

static int32_t on_row_v16(void *context,
                          const prdb_sdk_value_v1 *projected_values,
                          uint32_t projection_count,
                          const prdb_sdk_select_binding_v16 *binding) {
    row_capture *capture = (row_capture *)context;
    assert(capture->mock->scan_finished[0] == 1 &&
           capture->mock->scan_finished[1] == 1);
    assert(binding->base.projection_count == 2 &&
           (binding->join_key_count == 2 || binding->join_key_count == 8) &&
           projection_count == 2);
    assert(capture->calls < 8);
    assert(prdb_sdk_value_get_integer_v1(
               &projected_values[0], &capture->left_values[capture->calls]) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(prdb_sdk_value_get_integer_v1(
               &projected_values[1], &capture->right_values[capture->calls]) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    ++capture->calls;
    return capture->callback_status;
}

static int32_t run_query(mock_state *mock, row_capture *capture,
                         const uint8_t *sql, uint64_t sql_len,
                         uint64_t materialized_capacity) {
    static const uint8_t schema[] = "app";
    uint8_t left_planner_response[256] = {0};
    uint8_t right_planner_response[256] = {0};
    uint8_t left_executor_response[256] = {0};
    uint8_t right_executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[1024] = {0};
    uint8_t integer_names[2][11] = {{0}};
    prdb_sdk_sql_column_shape_v2 left_planner_columns[2] = {{0}};
    prdb_sdk_sql_column_shape_v2 right_planner_columns[2] = {{0}};
    prdb_sdk_sql_column_shape_v2 left_executor_columns[2] = {{0}};
    prdb_sdk_sql_column_shape_v2 right_executor_columns[2] = {{0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_sdk_sql_join_materialized_row_v8 left_rows[
        PRDB_SDK_SQL_EXEC_V8_MAX_ROWS] = {{{0}}};
    prdb_sdk_sql_join_materialized_row_v8 right_rows[
        PRDB_SDK_SQL_EXEC_V8_MAX_ROWS] = {{{0}}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    uint32_t name_len = (uint32_t)strlen("account_id");
    memcpy(integer_names[0], "account_id", name_len);
    memcpy(integer_names[1], "value", 5);
    for (uint32_t index = 0; index < 2; ++index) {
        const char *name = index == 0 ? "account_id" : "value";
        uint32_t len = (uint32_t)strlen(name);
        left_planner_columns[index] = (prdb_sdk_sql_column_shape_v2){
            integer_names[index], len, index, PRDB_SDK_VALUE_TYPE_INTEGER, 0,
            0};
        right_planner_columns[index] = left_planner_columns[index];
        left_executor_columns[index] = left_planner_columns[index];
        right_executor_columns[index] = left_planner_columns[index];
    }
    return prdb_sdk_sql_execute_inner_join_v8(
        &host, schema, sizeof(schema) - 1, sql, sql_len, 4, 512,
        left_planner_response, sizeof(left_planner_response),
        right_planner_response, sizeof(right_planner_response),
        left_executor_response, sizeof(left_executor_response),
        right_executor_response, sizeof(right_executor_response),
        batch_response, sizeof(batch_response), left_planner_columns, 2,
        right_planner_columns, 2, left_executor_columns, 2,
        right_executor_columns, 2, cell_scratch, 2, left_rows,
        PRDB_SDK_SQL_EXEC_V8_MAX_ROWS, right_rows,
        PRDB_SDK_SQL_EXEC_V8_MAX_ROWS, materialized_bytes,
        materialized_capacity, on_row, capture);
}

static int32_t run_query_v16(mock_state *mock, row_capture *capture,
                             const uint8_t *sql, uint64_t sql_len,
                             uint64_t materialized_capacity) {
    static const uint8_t schema[] = "app";
    uint8_t left_planner_response[256] = {0};
    uint8_t right_planner_response[256] = {0};
    uint8_t left_executor_response[256] = {0};
    uint8_t right_executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[1024] = {0};
    uint8_t integer_names[2][11] = {{0}};
    prdb_sdk_sql_column_shape_v2 left_planner_columns[2] = {{0}};
    prdb_sdk_sql_column_shape_v2 right_planner_columns[2] = {{0}};
    prdb_sdk_sql_column_shape_v2 left_executor_columns[2] = {{0}};
    prdb_sdk_sql_column_shape_v2 right_executor_columns[2] = {{0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_sdk_sql_join_materialized_row_v16 left_rows[
        PRDB_SDK_SQL_EXEC_V8_MAX_ROWS] = {{{0}}};
    prdb_sdk_sql_join_materialized_row_v16 right_rows[
        PRDB_SDK_SQL_EXEC_V8_MAX_ROWS] = {{{0}}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    memcpy(integer_names[0], "account_id", sizeof("account_id") - 1);
    memcpy(integer_names[1], "value", sizeof("value") - 1);
    for (uint32_t index = 0; index < 2; ++index) {
        const char *name = index == 0 ? "account_id" : "value";
        uint32_t len = (uint32_t)strlen(name);
        left_planner_columns[index] = (prdb_sdk_sql_column_shape_v2){
            integer_names[index], len, index, PRDB_SDK_VALUE_TYPE_INTEGER, 0,
            0};
        right_planner_columns[index] = left_planner_columns[index];
        left_executor_columns[index] = left_planner_columns[index];
        right_executor_columns[index] = left_planner_columns[index];
    }
    return prdb_sdk_sql_execute_inner_join_v16(
        &host, schema, sizeof(schema) - 1, sql, sql_len, 4, 512,
        left_planner_response, sizeof(left_planner_response),
        right_planner_response, sizeof(right_planner_response),
        left_executor_response, sizeof(left_executor_response),
        right_executor_response, sizeof(right_executor_response),
        batch_response, sizeof(batch_response), left_planner_columns, 2,
        right_planner_columns, 2, left_executor_columns, 2,
        right_executor_columns, 2, cell_scratch, 2, left_rows,
        PRDB_SDK_SQL_EXEC_V8_MAX_ROWS, right_rows,
        PRDB_SDK_SQL_EXEC_V8_MAX_ROWS, materialized_bytes,
        materialized_capacity, on_row_v16, capture);
}

static void reset(mock_state *mock, row_capture *capture) {
    mock->planner_calls = 0;
    mock->resolve_calls = 0;
    mock->open_calls = 0;
    mock->check_cancel_calls = 0;
    mock->cancel_after_check = 0;
    mock->close_calls = 0;
    mock->executor_requests = 0;
    mock->empty_pages[0] = mock->empty_pages[1] = 0;
    mock->next_calls[0] = mock->next_calls[1] = 0;
    mock->scan_finished[0] = mock->scan_finished[1] = 0;
    mock->statement_failed = 0;
    capture->calls = 0;
    capture->callback_status = PRDB_QUERY_CALLBACK_OK;
}

int main(void) {
    static const uint8_t sql[] =
        "SELECT accounts.value, orders.value FROM accounts INNER JOIN "
        "orders ON accounts.account_id = orders.account_id";
    static const uint8_t limit_sql[] =
        "SELECT accounts.value, orders.value FROM accounts INNER JOIN "
        "orders ON accounts.account_id = orders.account_id LIMIT 1";
    static const uint8_t limit_zero_sql[] =
        "SELECT accounts.value, orders.value FROM accounts INNER JOIN "
        "orders ON accounts.account_id = orders.account_id LIMIT 0";
    static const uint8_t redacted_sql[] =
        "SELECT accounts.value, orders.value FROM accounts INNER JOIN "
        "orders ON accounts.account_id = orders.account_id";
    static const uint8_t composite_sql[] =
        "SELECT accounts.value, orders.value FROM accounts INNER JOIN "
        "orders ON accounts.account_id = orders.account_id AND "
        "accounts.value = orders.value";
    static const uint8_t composite_limit_sql[] =
        "SELECT accounts.value, orders.value FROM accounts INNER JOIN "
        "orders ON accounts.account_id = orders.account_id AND "
        "accounts.value = orders.value LIMIT 1";
    static const uint8_t composite_max_keys_sql[] =
        "SELECT accounts.value, orders.value FROM accounts INNER JOIN "
        "orders ON accounts.account_id = orders.account_id AND "
        "accounts.value = orders.value AND "
        "accounts.account_id = orders.account_id AND "
        "accounts.value = orders.value AND "
        "accounts.account_id = orders.account_id AND "
        "accounts.value = orders.value AND "
        "accounts.account_id = orders.account_id AND "
        "accounts.value = orders.value";
    const int32_t left_keys[] = {1, 2, 3};
    const uint8_t left_states[] = {
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_STATE_VALUE,
        PRDB_SDK_VALUE_STATE_NULL};
    const int32_t left_values[] = {101, 102, 103};
    const int32_t right_keys[] = {2, 1, 4};
    const uint8_t right_states[] = {
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_STATE_VALUE,
        PRDB_SDK_VALUE_STATE_NULL};
    const int32_t right_values[] = {202, 201, 204};
    mock_state mock = {0};
    row_capture capture = {0};

    for (uint32_t index = 0; index < 2; ++index) {
        mock.planner_len[index] =
            make_relation(mock.planner[index], index,
                          PRDB_SDK_PLANNER_TABLE_HANDLE_KIND);
        mock.executor_len[index] =
            make_relation(mock.executor[index], index, 6);
    }
    mock.batch_len[0] = make_batch(mock.batch[0], left_keys, left_states,
                                   left_values, 3);
    mock.batch_len[1] = make_batch(mock.batch[1], right_keys, right_states,
                                   right_values, 3);
    capture.mock = &mock;

    assert(run_query(&mock, &capture, sql, sizeof(sql) - 1, 1024) ==
           PRDB_SDK_SQL_EXEC_OK);
    assert(mock.planner_calls == 2 && mock.resolve_calls == 2 &&
           mock.open_calls == 2 && mock.next_calls[0] == 1 &&
           mock.next_calls[1] == 1 && mock.statement_failed == 0);
    assert(capture.calls == 2 && capture.left_values[0] == 101 &&
           capture.right_values[0] == 201 && capture.left_values[1] == 102 &&
           capture.right_values[1] == 202);

    reset(&mock, &capture);
    assert(run_query(&mock, &capture, limit_sql, sizeof(limit_sql) - 1, 1024) ==
           PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 1 && capture.left_values[0] == 101 &&
           capture.right_values[0] == 201);

    reset(&mock, &capture);
    assert(run_query(&mock, &capture, limit_zero_sql,
                     sizeof(limit_zero_sql) - 1, 1024) ==
           PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 0 && mock.planner_calls == 2 &&
           mock.resolve_calls == 2 && mock.open_calls == 0 &&
           mock.statement_failed == 0);

    {
        const uint8_t redacted_states[] = {
            PRDB_SDK_VALUE_STATE_REDACTED, PRDB_SDK_VALUE_STATE_VALUE,
            PRDB_SDK_VALUE_STATE_NULL};
        mock.batch_len[0] = make_batch(mock.batch[0], left_keys,
                                       redacted_states, left_values, 3);
        reset(&mock, &capture);
        assert(run_query(&mock, &capture, redacted_sql,
                         sizeof(redacted_sql) - 1, 1024) ==
               PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
        assert(capture.calls == 0 && mock.statement_failed == 1);
        mock.batch_len[0] = make_batch(mock.batch[0], left_keys, left_states,
                                       left_values, 3);
    }

    reset(&mock, &capture);
    assert(run_query(&mock, &capture, sql, sizeof(sql) - 1, 4) ==
           PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT);
    assert(capture.calls == 0 && mock.statement_failed == 1);

    reset(&mock, &capture);
    capture.callback_status = PRDB_QUERY_CALLBACK_INTERNAL_ERROR;
    assert(run_query(&mock, &capture, sql, sizeof(sql) - 1, 1024) ==
           PRDB_SDK_SQL_EXEC_RESULT_ERROR);
    assert(capture.calls == 1 && mock.statement_failed == 1);

    {
        const int32_t composite_left_values[] = {10, 20, 30};
        const int32_t composite_right_values[] = {20, 10, 30};
        mock.batch_len[0] = make_batch(mock.batch[0], left_keys, left_states,
                                       composite_left_values, 3);
        mock.batch_len[1] = make_batch(mock.batch[1], right_keys, right_states,
                                       composite_right_values, 3);
        reset(&mock, &capture);
        assert(run_query_v16(&mock, &capture, composite_sql,
                             sizeof(composite_sql) - 1, 1024) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(mock.planner_calls == 2 && mock.resolve_calls == 2 &&
               mock.open_calls == 2 && mock.next_calls[0] == 1 &&
               mock.next_calls[1] == 1 && mock.check_cancel_calls == 3 &&
               mock.statement_failed == 0);
        assert(capture.calls == 2 && capture.left_values[0] == 10 &&
               capture.right_values[0] == 10 && capture.left_values[1] == 20 &&
               capture.right_values[1] == 20);

        reset(&mock, &capture);
        mock.cancel_after_check = 3;
        assert(run_query_v16(&mock, &capture, composite_sql,
                             sizeof(composite_sql) - 1, 1024) ==
               PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
        assert(capture.calls == 0 && mock.statement_failed == 1 &&
               mock.check_cancel_calls == 3);

        reset(&mock, &capture);
        mock.empty_pages[0] = 121;
        assert(run_query_v16(&mock, &capture, composite_sql,
                             sizeof(composite_sql) - 1, 1024) ==
               PRDB_SDK_SQL_EXEC_REQUEST_LIMIT);
        assert(mock.next_calls[0] == 121 && mock.next_calls[1] == 0 &&
               mock.close_calls == 1 && mock.executor_requests == 246 &&
               capture.calls == 0 && mock.statement_failed == 1);

        reset(&mock, &capture);
        assert(run_query_v16(&mock, &capture, composite_max_keys_sql,
                             sizeof(composite_max_keys_sql) - 1, 1024) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && capture.left_values[0] == 10 &&
               capture.right_values[0] == 10 && capture.left_values[1] == 20 &&
               capture.right_values[1] == 20);

        reset(&mock, &capture);
        assert(run_query_v16(&mock, &capture, composite_limit_sql,
                             sizeof(composite_limit_sql) - 1, 1024) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 1 && capture.left_values[0] == 10 &&
               capture.right_values[0] == 10);

        reset(&mock, &capture);
        assert(run_query_v16(&mock, &capture, composite_sql,
                             sizeof(composite_sql) - 1, 4) ==
               PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT);
        assert(capture.calls == 0 && mock.statement_failed == 1);

        reset(&mock, &capture);
        capture.callback_status = PRDB_QUERY_CALLBACK_INTERNAL_ERROR;
        assert(run_query_v16(&mock, &capture, composite_sql,
                             sizeof(composite_sql) - 1, 1024) ==
               PRDB_SDK_SQL_EXEC_RESULT_ERROR);
        assert(capture.calls == 1 && mock.statement_failed == 1);
    }
    return 0;
}
