#include "prdb_sdk_sql_exec_v10.h"
#include "prdb_sdk_sql_exec_v11.h"
#include "prdb_sdk_sql_exec_v12.h"
#include "prdb_sdk_sql_exec_v13.h"
#include "prdb_sdk_sql_exec_v14.h"
#include "prdb_sdk_sql_exec_v15.h"

#include <assert.h>
#include <string.h>

typedef struct mock_state {
    uint8_t planner[256];
    uint32_t planner_len;
    uint8_t executor[256];
    uint32_t executor_len;
    uint8_t batch[512];
    uint32_t batch_len;
    uint32_t batch_finished;
    uint32_t planner_calls;
    uint32_t executor_calls;
    uint32_t next_calls;
    uint32_t statement_failed;
    uint32_t cancelled;
} mock_state;

typedef struct group_capture {
    mock_state *mock;
    uint32_t calls;
    int32_t keys[4];
    int32_t second_keys[4];
    int32_t aggregates[4];
    uint32_t aggregate_states[4];
    uint64_t row_counts[4];
} group_capture;

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
    static const char *names[] = {"id", "score"};
    uint32_t offset = 0;
    put_u16(p, &offset, 1);
    p[offset++] = 0;
    memset(p + offset, 0x11, 16); offset += 16;
    p[offset++] = handle_kind;
    memset(p + offset, handle_fill, 15); offset += 15;
    put_name(p, &offset, "widgets");
    put_u16(p, &offset, 2);
    for (uint32_t index = 0; index < 2; ++index) {
        memset(p + offset, (int)(0x33 + index), 16); offset += 16;
        put_u32(p, &offset, index);
        p[offset++] = 1;
        p[offset++] = PRDB_SDK_VALUE_TYPE_INTEGER;
        p[offset++] = 0;
        p[offset++] = 0;
        p[offset++] = 0;
        p[offset++] = 0;
        put_name(p, &offset, names[index]);
    }
    return offset;
}

static void append_integer_cell(uint8_t *p, uint32_t *offset, int32_t value) {
    uint32_t bits = (uint32_t)value;
    p[(*offset)++] = PRDB_SDK_VALUE_STATE_VALUE;
    p[(*offset)++] = PRDB_SDK_VALUE_TYPE_INTEGER;
    put_u32(p, offset, 4);
    put_u32(p, offset, bits);
}

static uint32_t make_rows(uint8_t *p, uint32_t finished,
                          const int32_t *keys, const int32_t *scores,
                          const uint8_t *score_states, uint16_t row_count) {
    uint32_t offset = 0;
    put_u16(p, &offset, 1);
    p[offset++] = (uint8_t)finished;
    if (!finished) {
        p[offset++] = 7;
        memset(p + offset, 0x55, 15); offset += 15;
    }
    put_u16(p, &offset, row_count);
    for (uint32_t index = 0; index < row_count; ++index) {
        p[offset++] = 8;
        memset(p + offset, (int)(index + 1), 15); offset += 15;
        put_u16(p, &offset, 2);
        append_integer_cell(p, &offset, keys[index]);
        if (score_states[index] == PRDB_SDK_VALUE_STATE_VALUE) {
            append_integer_cell(p, &offset, scores[index]);
        } else {
            p[offset++] = score_states[index];
        }
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
    static const uint8_t expected[] = {
        0, 1, 0, 3, 0, 7, 'a', 'p', 'p', 'w', 'i', 'd', 'g', 'e', 't', 's'};
    mock_state *mock = (mock_state *)context;
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
    mock_state *mock = (mock_state *)context;
    if (operation == PRDB_SDK_EXECUTOR_OP_CHECK_CANCELLATION) {
        const uint8_t reply[] = {0, 1, (uint8_t)mock->cancelled};
        assert(request_len == 0 && sizeof(reply) <= capacity);
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
        static const uint8_t finished_reply[5] = {0, 1, 1, 0, 0};
        static const uint8_t initial_cursor_tail[16] = {
            0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
            0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0};
        static const uint8_t continued_cursor_tail[16] = {
            0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
            0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0};
        assert(request_len == 24);
        assert(request[0] == 0 && request[1] == 1 && request[2] == 7);
        if (mock->next_calls == 0) {
            assert(memcmp(request + 3, initial_cursor_tail, 16) == 0);
            assert(mock->batch_len <= capacity);
            memcpy(response, mock->batch, mock->batch_len);
            *response_len = mock->batch_len;
        } else {
            assert(memcmp(request + 3, continued_cursor_tail, 16) == 0);
            assert(sizeof(finished_reply) <= capacity);
            memcpy(response, finished_reply, sizeof(finished_reply));
            *response_len = sizeof(finished_reply);
        }
        ++mock->next_calls;
    } else if (operation == PRDB_SDK_EXECUTOR_OP_CLOSE_CURSOR) {
        static const uint8_t reply[] = {0, 1, 0};
        static const uint8_t continued_cursor_tail[15] = {
            0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
            0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55};
        static const uint8_t initial_cursor_tail[15] = {
            0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
            0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44};
        assert(request_len == 18);
        assert(request[0] == 0 && request[1] == 1 && request[2] == 7);
        assert(memcmp(request + 3,
                      request[3] == 0x44 ? initial_cursor_tail
                                          : continued_cursor_tail,
                      15) == 0);
        assert(sizeof(reply) <= capacity);
        memcpy(response, reply, sizeof(reply));
        *response_len = sizeof(reply);
    } else {
        assert(!"unexpected executor operation");
    }
    if (operation != PRDB_SDK_EXECUTOR_OP_CHECK_CANCELLATION) {
        ++mock->executor_calls;
    }
    return PRDB_QUERY_CALLBACK_OK;
}

static int32_t on_group(void *context, const prdb_sdk_value_v1 *group_key,
                        const prdb_sdk_value_v1 *aggregate_value,
                        uint64_t matching_row_count,
                        const prdb_sdk_select_binding_v10 *binding) {
    group_capture *capture = (group_capture *)context;
    int32_t key = 0;
    int32_t aggregate = 0;
    assert(capture->mock->batch_finished != 0);
    assert(capture->mock->executor_calls == 3);
    assert(capture->calls < 4);
    assert(binding->base.base.aggregate_kind ==
               PRDB_SDK_SQL_V7_AGGREGATE_MIN ||
           binding->base.base.aggregate_kind ==
               PRDB_SDK_SQL_V7_AGGREGATE_MAX ||
           binding->base.base.aggregate_kind ==
               PRDB_SDK_SQL_V7_AGGREGATE_SUM ||
           binding->base.base.aggregate_kind ==
               PRDB_SDK_SQL_V7_AGGREGATE_AVG ||
           binding->base.base.aggregate_kind ==
               PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR);
    assert(prdb_sdk_value_get_integer_v1(group_key, &key) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    if (binding->base.base.aggregate_kind ==
        PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
        assert(aggregate_value == NULL);
        aggregate = (int32_t)matching_row_count;
        capture->aggregate_states[capture->calls] =
            PRDB_SDK_VALUE_STATE_VALUE;
    } else {
        assert(aggregate_value != NULL);
        capture->aggregate_states[capture->calls] = aggregate_value->state;
        if (aggregate_value->state == PRDB_SDK_VALUE_STATE_VALUE) {
            if (binding->base.base.aggregate_kind ==
                PRDB_SDK_SQL_V7_AGGREGATE_SUM) {
                int64_t sum = 0;
                assert(prdb_sdk_value_get_bigint_v1(aggregate_value, &sum) ==
                       PRDB_SDK_VALUE_ACCESS_OK);
                aggregate = (int32_t)sum;
                assert(sum == aggregate);
            } else if (binding->base.base.aggregate_kind ==
                       PRDB_SDK_SQL_V7_AGGREGATE_AVG) {
                uint32_t coefficient = 0;
                assert(aggregate_value->type_code ==
                           PRDB_SDK_VALUE_TYPE_DECIMAL &&
                       aggregate_value->data_len == 16);
                for (uint32_t index = 0; index < 12; ++index) {
                    assert(aggregate_value->data[index] == 0);
                }
                for (uint32_t index = 12; index < 16; ++index) {
                    coefficient =
                        (coefficient << 8) | aggregate_value->data[index];
                }
                aggregate = (int32_t)coefficient;
            } else {
                assert(prdb_sdk_value_get_integer_v1(aggregate_value,
                                                     &aggregate) ==
                       PRDB_SDK_VALUE_ACCESS_OK);
            }
        } else {
            assert(aggregate_value->state == PRDB_SDK_VALUE_STATE_NULL);
        }
    }
    capture->keys[capture->calls] = key;
    capture->aggregates[capture->calls] = aggregate;
    capture->row_counts[capture->calls] = matching_row_count;
    ++capture->calls;
    return 0;
}

static int32_t run_query(mock_state *mock, const uint8_t *sql,
                         uint64_t sql_len,
                         const prdb_sdk_value_v1 *parameters,
                         uint32_t parameter_count, uint64_t arena_capacity,
                         group_capture *capture) {
    static const uint8_t schema[] = "app";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[64] = {0};
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v10(
        &host, schema, sizeof(schema) - 1, sql, sql_len, parameters,
        parameter_count, 3, 512, planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 2, executor_columns, 2,
        cell_scratch, 2, materialized_bytes, arena_capacity, on_group, capture);
}

static int32_t on_group_v11(void *context,
                            const prdb_sdk_value_v1 *group_key,
                            const prdb_sdk_value_v1 *aggregate_value,
                            uint64_t matching_row_count,
                            const prdb_sdk_select_binding_v11 *binding) {
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V11_VERSION);
    return on_group(context, group_key, aggregate_value, matching_row_count,
                    &binding->base);
}

static int32_t run_query_v11(mock_state *mock, const uint8_t *sql,
                             uint64_t sql_len, uint64_t arena_capacity,
                             group_capture *capture) {
    static const uint8_t schema[] = "app";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[64] = {0};
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v11(
        &host, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0, 3, 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, arena_capacity, on_group_v11, capture);
}

static int32_t on_group_v12(void *context,
                            const prdb_sdk_value_v1 *group_key,
                            const prdb_sdk_value_v1 *aggregate_value,
                            uint64_t matching_row_count,
                            const prdb_sdk_select_binding_v12 *binding) {
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V12_VERSION);
    assert(binding->order_count <= PRDB_SDK_SQL_V12_MAX_ORDER_KEYS);
    if (binding->order_count == 2) {
        assert(binding->order_items[0].type_code == PRDB_SDK_VALUE_TYPE_BIGINT);
        assert(binding->order_items[1].type_code == PRDB_SDK_VALUE_TYPE_INTEGER);
    }
    return on_group(context, group_key, aggregate_value, matching_row_count,
                    &binding->base.base);
}

static int32_t run_query_v12(mock_state *mock, const uint8_t *sql,
                             uint64_t sql_len, uint64_t arena_capacity,
                             group_capture *capture) {
    static const uint8_t schema[] = "app";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[64] = {0};
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v12(
        &host, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0, 3, 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, arena_capacity, on_group_v12, capture);
}

static int32_t on_group_v13(void *context,
                            const prdb_sdk_value_v1 *group_keys,
                            uint32_t group_key_count,
                            const prdb_sdk_value_v1 *aggregate_value,
                            uint64_t matching_row_count,
                            const prdb_sdk_select_binding_v13 *binding) {
    group_capture *capture = (group_capture *)context;
    int32_t first = 0, second = 0;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V13_VERSION);
    assert(group_key_count == 2 && binding->group_key_count == 2);
    assert(capture->mock->batch_finished != 0);
    assert(capture->mock->executor_calls == 3);
    assert(capture->calls < 4);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &first) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[1], &second) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(binding->base.aggregate_kind ==
               PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR ||
           binding->base.aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_SUM);
    if (binding->base.aggregate_kind ==
        PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
        assert(aggregate_value == NULL);
        capture->aggregates[capture->calls] = (int32_t)matching_row_count;
    } else {
        int64_t sum = 0;
        assert(aggregate_value != NULL &&
               aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_BIGINT);
        assert(prdb_sdk_value_get_bigint_v1(aggregate_value, &sum) ==
               PRDB_SDK_VALUE_ACCESS_OK);
        capture->aggregates[capture->calls] = (int32_t)sum;
        assert((int64_t)capture->aggregates[capture->calls] == sum);
    }
    capture->keys[capture->calls] = first;
    capture->second_keys[capture->calls] = second;
    capture->row_counts[capture->calls] = matching_row_count;
    ++capture->calls;
    return 0;
}

static int32_t run_query_v13(mock_state *mock, const uint8_t *sql,
                             uint64_t sql_len, uint64_t arena_capacity,
                             group_capture *capture) {
    static const uint8_t schema[] = "app";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[64] = {0};
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v13(
        &host, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0, 3, 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, arena_capacity, on_group_v13, capture);
}

static int32_t on_group_v14(void *context,
                            const prdb_sdk_value_v1 *group_keys,
                            uint32_t group_key_count,
                            const prdb_sdk_value_v1 *aggregate_value,
                            uint64_t matching_row_count,
                            const prdb_sdk_select_binding_v14 *binding) {
    group_capture *capture = (group_capture *)context;
    int32_t key = 0;
    int64_t sum = 0;
    (void)matching_row_count;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V14_VERSION);
    assert(binding->base.group_key_count == 2 && group_key_count == 2);
    assert(binding->has_having == 1 &&
           binding->having_parameter_index == 1);
    assert(capture->mock->batch_finished != 0);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &key) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(prdb_sdk_value_get_bigint_v1(aggregate_value, &sum) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    capture->keys[capture->calls] = key;
    capture->aggregates[capture->calls] = (int32_t)sum;
    ++capture->calls;
    return 0;
}

static int32_t run_query_v14(mock_state *mock, const uint8_t *sql,
                             uint64_t sql_len,
                             const prdb_sdk_value_v1 *parameter,
                             group_capture *capture) {
    static const uint8_t schema[] = "app";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[64] = {0};
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v14(
        &host, schema, sizeof(schema) - 1, sql, sql_len, parameter, 1, 3, 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes), on_group_v14, capture);
}

static int32_t on_group_v15(void *context,
                            const prdb_sdk_value_v1 *group_keys,
                            uint32_t group_key_count,
                            const prdb_sdk_value_v1 *aggregate_value,
                            uint64_t matching_row_count,
                            const prdb_sdk_select_binding_v15 *binding) {
    group_capture *capture = (group_capture *)context;
    int32_t first = 0, second = 0;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V15_VERSION);
    assert(binding->order_count == 3);
    assert(binding->order_items[0].type_code == PRDB_SDK_VALUE_TYPE_INTEGER);
    assert(binding->order_items[1].type_code == PRDB_SDK_VALUE_TYPE_BIGINT);
    assert(binding->order_items[2].type_code == PRDB_SDK_VALUE_TYPE_INTEGER);
    assert(group_key_count == 2 && aggregate_value == NULL);
    assert(capture->mock->batch_finished != 0);
    assert(capture->mock->executor_calls == 3);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &first) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[1], &second) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    capture->keys[capture->calls] = first;
    capture->second_keys[capture->calls] = second;
    capture->row_counts[capture->calls] = matching_row_count;
    ++capture->calls;
    return 0;
}

static int32_t run_query_v15(mock_state *mock, const uint8_t *sql,
                             uint64_t sql_len,
                             const prdb_sdk_value_v1 *parameter,
                             group_capture *capture) {
    static const uint8_t schema[] = "app";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[64] = {0};
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v15(
        &host, schema, sizeof(schema) - 1, sql, sql_len, parameter, 1, 3, 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes), on_group_v15, capture);
}

static void reset_scan(mock_state *mock, group_capture *capture,
                       uint32_t finished) {
    mock->executor_calls = 0;
    mock->next_calls = 0;
    mock->statement_failed = 0;
    mock->cancelled = 0;
    mock->batch_finished = finished;
    capture->calls = 0;
}

int main(void) {
    static const uint8_t min_sql[] =
        "SELECT id, MIN(score) FROM widgets GROUP BY id";
    static const uint8_t max_sql[] =
        "SELECT id, MAX(score) FROM widgets GROUP BY id";
    static const uint8_t sum_sql[] =
        "SELECT id, SUM(score) FROM widgets GROUP BY id";
    static const uint8_t avg_sql[] =
        "SELECT id, AVG(score) FROM widgets GROUP BY id";
    static const uint8_t limit_sql[] =
        "SELECT id, MIN(score) FROM widgets GROUP BY id LIMIT 1";
    static const uint8_t where_sql[] =
        "SELECT id, MIN(score) FROM widgets WHERE score >= $1 GROUP BY id";
    static const uint8_t having_sql[] =
        "SELECT id, COUNT(*) FROM widgets GROUP BY id "
        "HAVING COUNT(*) = $1 LIMIT 1";
    static const uint8_t ordered_having_sql[] =
        "SELECT id, COUNT(*) FROM widgets GROUP BY id "
        "HAVING COUNT(*) >= $1 ORDER BY COUNT(*) ASC LIMIT 1";
    static const uint8_t ordered_key_sql[] =
        "SELECT id, COUNT(*) FROM widgets GROUP BY id "
        "ORDER BY id DESC LIMIT 1";
    static const uint8_t ordered_aggregate_sql[] =
        "SELECT id, MIN(score) FROM widgets GROUP BY id "
        "ORDER BY MIN(score) DESC LIMIT 1";
    static const uint8_t nulls_last_sql[] =
        "SELECT id, MIN(score) FROM widgets GROUP BY id "
        "ORDER BY MIN(score) ASC NULLS LAST";
    static const uint8_t default_null_order_sql[] =
        "SELECT id, MIN(score) FROM widgets GROUP BY id "
        "ORDER BY MIN(score) DESC";
    static const uint8_t composite_order_sql[] =
        "SELECT id, COUNT(*) FROM widgets GROUP BY id "
        "ORDER BY COUNT(*) ASC, id DESC NULLS LAST LIMIT 2";
    static const uint8_t v12_no_order_sql[] =
        "SELECT id, COUNT(*) FROM widgets GROUP BY id";
    static const uint8_t v13_composite_sql[] =
        "SELECT id, score, COUNT(*) FROM widgets GROUP BY id, score";
    static const uint8_t v13_sum_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score";
    static const uint8_t v14_having_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING SUM(score) >= $1";
    static const uint8_t v15_order_sql[] =
        "SELECT id, score, COUNT(*) FROM widgets GROUP BY id, score "
        "HAVING COUNT(*) >= $1 ORDER BY score DESC NULLS LAST, "
        "COUNT(*) DESC, id ASC NULLS LAST LIMIT 3";
    static const uint8_t parameter_bytes[] = {0, 0, 0, 5};
    static const uint8_t having_parameter_bytes[] = {
        0, 0, 0, 0, 0, 0, 0, 1};
    static const uint8_t v14_having_parameter_bytes[] = {
        0, 0, 0, 0, 0, 0, 0, 6};
    const int32_t keys[] = {1, 1, 2};
    const int32_t scores[] = {9, 3, 5};
    const uint8_t value_states[] = {
        PRDB_SDK_VALUE_STATE_VALUE,
        PRDB_SDK_VALUE_STATE_VALUE,
        PRDB_SDK_VALUE_STATE_VALUE};
    mock_state mock = {0};
    group_capture capture = {0};
    prdb_sdk_value_v1 parameter = {
        sizeof(parameter), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
        parameter_bytes, sizeof(parameter_bytes)};
    prdb_sdk_value_v1 having_parameter = {
        sizeof(having_parameter), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BIGINT,
        having_parameter_bytes, sizeof(having_parameter_bytes)};
    prdb_sdk_value_v1 v14_having_parameter = {
        sizeof(v14_having_parameter), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BIGINT,
        v14_having_parameter_bytes, sizeof(v14_having_parameter_bytes)};
    prdb_sdk_value_v1 v15_having_parameter = {
        sizeof(v15_having_parameter), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BIGINT,
        having_parameter_bytes, sizeof(having_parameter_bytes)};
    mock.planner_len = make_relation(mock.planner,
                                     PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99);
    mock.executor_len = make_relation(mock.executor, 6, 0x22);
    mock.batch_len = make_rows(mock.batch, 1, keys, scores, value_states, 3);
    mock.batch_finished = 1;
    capture.mock = &mock;

    assert(run_query(&mock, min_sql, sizeof(min_sql) - 1, NULL, 0, 64,
                     &capture) == PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 2 && capture.keys[0] == 1 &&
           capture.aggregates[0] == 3 && capture.row_counts[0] == 2 &&
           capture.keys[1] == 2 && capture.aggregates[1] == 5 &&
           capture.row_counts[1] == 1 && mock.statement_failed == 0);

    reset_scan(&mock, &capture, 1);
    assert(run_query(&mock, max_sql, sizeof(max_sql) - 1, NULL, 0, 64,
                     &capture) == PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 2 && capture.aggregates[0] == 9 &&
           capture.aggregates[1] == 5);

    reset_scan(&mock, &capture, 1);
    assert(run_query(&mock, sum_sql, sizeof(sum_sql) - 1, NULL, 0, 64,
                     &capture) == PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 2 && capture.keys[0] == 1 &&
           capture.aggregates[0] == 12 && capture.row_counts[0] == 2 &&
           capture.keys[1] == 2 && capture.aggregates[1] == 5 &&
           capture.row_counts[1] == 1);

    reset_scan(&mock, &capture, 1);
    assert(run_query(&mock, avg_sql, sizeof(avg_sql) - 1, NULL, 0, 64,
                     &capture) == PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 2 && capture.keys[0] == 1 &&
           capture.aggregates[0] == 6000000 && capture.row_counts[0] == 2 &&
           capture.keys[1] == 2 && capture.aggregates[1] == 5000000 &&
           capture.row_counts[1] == 1);

    reset_scan(&mock, &capture, 1);
    assert(run_query(&mock, limit_sql, sizeof(limit_sql) - 1, NULL, 0, 64,
                     &capture) == PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 1 && capture.keys[0] == 1 &&
           capture.aggregates[0] == 3);

    reset_scan(&mock, &capture, 1);
    assert(run_query(&mock, where_sql, sizeof(where_sql) - 1, &parameter, 1,
                     64, &capture) == PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 2 && capture.keys[0] == 1 &&
           capture.aggregates[0] == 9 && capture.keys[1] == 2 &&
           capture.aggregates[1] == 5);

    reset_scan(&mock, &capture, 1);
    assert(run_query(&mock, having_sql, sizeof(having_sql) - 1,
                     &having_parameter, 1, 64, &capture) ==
           PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 1 && capture.keys[0] == 2 &&
           capture.aggregates[0] == 1);

    reset_scan(&mock, &capture, 1);
    assert(run_query(&mock, ordered_aggregate_sql,
                     sizeof(ordered_aggregate_sql) - 1, NULL, 0, 64,
                     &capture) == PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 1 && capture.keys[0] == 2 &&
           capture.aggregates[0] == 5);

    {
        const uint8_t null_then_value[] = {
            PRDB_SDK_VALUE_STATE_NULL,
            PRDB_SDK_VALUE_STATE_NULL,
            PRDB_SDK_VALUE_STATE_VALUE};
        mock.batch_len = make_rows(mock.batch, 1, keys, scores,
                                   null_then_value, 3);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v11(&mock, nulls_last_sql,
                             sizeof(nulls_last_sql) - 1, 64, &capture) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && capture.keys[0] == 2 &&
               capture.aggregates[0] == 5 && capture.keys[1] == 1 &&
               capture.aggregate_states[1] == PRDB_SDK_VALUE_STATE_NULL &&
               mock.statement_failed == 0);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v11(&mock, default_null_order_sql,
                             sizeof(default_null_order_sql) - 1, 64,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && capture.keys[0] == 1 &&
               capture.aggregate_states[0] == PRDB_SDK_VALUE_STATE_NULL &&
               capture.keys[1] == 2 && capture.aggregates[1] == 5 &&
               mock.statement_failed == 0);
        mock.batch_len = make_rows(mock.batch, 1, keys, scores,
                                   value_states, 3);
    }

    reset_scan(&mock, &capture, 1);
    assert(run_query_v12(&mock, v12_no_order_sql,
                         sizeof(v12_no_order_sql) - 1, 64, &capture) ==
           PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 2 && capture.keys[0] == 1 &&
           capture.keys[1] == 2 && capture.row_counts[0] == 2 &&
           capture.row_counts[1] == 1 && mock.statement_failed == 0);

    {
        const int32_t tuple_ids[] = {1, 1, 2};
        const int32_t tuple_scores[] = {9, 9, 5};
        mock.batch_len = make_rows(mock.batch, 1, tuple_ids, tuple_scores,
                                   value_states, 3);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v13(&mock, v13_composite_sql,
                             sizeof(v13_composite_sql) - 1, 64,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && capture.keys[0] == 1 &&
               capture.second_keys[0] == 9 && capture.row_counts[0] == 2 &&
               capture.keys[1] == 2 && capture.second_keys[1] == 5 &&
               capture.row_counts[1] == 1 && mock.statement_failed == 0);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v13(&mock, v13_sum_sql,
                             sizeof(v13_sum_sql) - 1, 64,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && capture.aggregates[0] == 18 &&
               capture.aggregates[1] == 5 && mock.statement_failed == 0);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v14(&mock, v14_having_sql,
                             sizeof(v14_having_sql) - 1,
                             &v14_having_parameter, &capture) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 1 && capture.keys[0] == 1 &&
               capture.aggregates[0] == 18 && mock.statement_failed == 0);
        mock.batch_len = make_rows(mock.batch, 1, keys, scores,
                                   value_states, 3);
    }

    {
        const int32_t tuple_ids[] = {2, 1, 1, 0};
        const int32_t tuple_scores[] = {9, 9, 9, 9};
        const uint8_t tuple_states[] = {
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_STATE_VALUE,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_STATE_VALUE};
        const int32_t sorted_ids[] = {1, 0, 2};
        mock.batch_len = make_rows(mock.batch, 1, tuple_ids, tuple_scores,
                                   tuple_states, 4);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v15(&mock, v15_order_sql,
                             sizeof(v15_order_sql) - 1,
                             &v15_having_parameter, &capture) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 3 && mock.statement_failed == 0);
        for (uint32_t index = 0; index < 3; ++index) {
            assert(capture.keys[index] == sorted_ids[index]);
            assert(capture.second_keys[index] == 9);
        }
        assert(capture.row_counts[0] == 2 && capture.row_counts[1] == 1 &&
               capture.row_counts[2] == 1);
        mock.batch_len = make_rows(mock.batch, 1, keys, scores,
                                   value_states, 3);
    }

    {
        const int32_t tied_group_keys[] = {1, 2, 3};
        mock.batch_len = make_rows(mock.batch, 1, tied_group_keys, scores,
                                   value_states, 3);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v12(&mock, composite_order_sql,
                             sizeof(composite_order_sql) - 1, 64,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && capture.keys[0] == 3 &&
               capture.keys[1] == 2 && capture.row_counts[0] == 1 &&
               capture.row_counts[1] == 1 && mock.statement_failed == 0);
        mock.batch_len = make_rows(mock.batch, 1, keys, scores,
                                   value_states, 3);
    }

    reset_scan(&mock, &capture, 1);
    assert(run_query(&mock, ordered_having_sql,
                     sizeof(ordered_having_sql) - 1, &having_parameter, 1, 64,
                     &capture) == PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 1 && capture.keys[0] == 2 &&
           capture.aggregates[0] == 1);

    reset_scan(&mock, &capture, 1);
    assert(run_query(&mock, ordered_key_sql, sizeof(ordered_key_sql) - 1,
                     NULL, 0, 64, &capture) == PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 1 && capture.keys[0] == 2 &&
           capture.aggregates[0] == 1);

    {
        prdb_sdk_value_v1 null_parameter = {
            sizeof(null_parameter), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_NULL, 0, NULL, 0};
        reset_scan(&mock, &capture, 1);
        assert(run_query(&mock, having_sql, sizeof(having_sql) - 1,
                         &null_parameter, 1, 64, &capture) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 0 && mock.next_calls == 1 &&
               mock.statement_failed == 0);
    }

    {
        prdb_sdk_value_v1 invalid_parameter = parameter;
        reset_scan(&mock, &capture, 1);
        assert(run_query(&mock, having_sql, sizeof(having_sql) - 1,
                         &invalid_parameter, 1, 64, &capture) ==
               PRDB_SDK_SQL_EXEC_BIND_ERROR);
        assert(capture.calls == 0 && mock.next_calls == 0 &&
               mock.statement_failed == 1);
        invalid_parameter.state = PRDB_SDK_VALUE_STATE_REDACTED;
        invalid_parameter.type_code = 0;
        invalid_parameter.data = NULL;
        invalid_parameter.data_len = 0;
        reset_scan(&mock, &capture, 1);
        assert(run_query(&mock, having_sql, sizeof(having_sql) - 1,
                         &invalid_parameter, 1, 64, &capture) ==
               PRDB_SDK_SQL_EXEC_BIND_ERROR);
        assert(capture.calls == 0 && mock.next_calls == 0 &&
               mock.statement_failed == 1);
    }

    {
        const uint8_t null_states[] = {
            PRDB_SDK_VALUE_STATE_NULL,
            PRDB_SDK_VALUE_STATE_NULL,
            PRDB_SDK_VALUE_STATE_NULL};
        mock.batch_len = make_rows(mock.batch, 1, keys, scores, null_states, 3);
        reset_scan(&mock, &capture, 1);
        assert(run_query(&mock, min_sql, sizeof(min_sql) - 1, NULL, 0, 64,
                         &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 &&
               capture.aggregate_states[0] == PRDB_SDK_VALUE_STATE_NULL &&
               capture.aggregate_states[1] == PRDB_SDK_VALUE_STATE_NULL);
        {
            const uint8_t partly_null_states[] = {
                PRDB_SDK_VALUE_STATE_NULL,
                PRDB_SDK_VALUE_STATE_NULL,
                PRDB_SDK_VALUE_STATE_VALUE};
            mock.batch_len = make_rows(mock.batch, 1, keys, scores,
                                       partly_null_states, 3);
            reset_scan(&mock, &capture, 1);
            assert(run_query(&mock, ordered_aggregate_sql,
                             sizeof(ordered_aggregate_sql) - 1, NULL, 0, 64,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 1 && capture.keys[0] == 1 &&
                   capture.aggregate_states[0] ==
                       PRDB_SDK_VALUE_STATE_NULL);
            mock.batch_len = make_rows(mock.batch, 1, keys, scores,
                                       null_states, 3);
        }
        reset_scan(&mock, &capture, 1);
        assert(run_query(&mock, avg_sql, sizeof(avg_sql) - 1, NULL, 0, 64,
                         &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 &&
               capture.aggregate_states[0] == PRDB_SDK_VALUE_STATE_NULL &&
               capture.aggregate_states[1] == PRDB_SDK_VALUE_STATE_NULL);
        reset_scan(&mock, &capture, 1);
        assert(run_query(&mock, sum_sql, sizeof(sum_sql) - 1, NULL, 0, 64,
                         &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 &&
               capture.aggregate_states[0] == PRDB_SDK_VALUE_STATE_NULL &&
               capture.aggregate_states[1] == PRDB_SDK_VALUE_STATE_NULL);
    }

    {
        const uint8_t redacted_states[] = {
            PRDB_SDK_VALUE_STATE_VALUE,
            PRDB_SDK_VALUE_STATE_REDACTED,
            PRDB_SDK_VALUE_STATE_VALUE};
        mock.batch_len = make_rows(mock.batch, 0, keys, scores,
                                   redacted_states, 3);
        reset_scan(&mock, &capture, 0);
        assert(run_query(&mock, min_sql, sizeof(min_sql) - 1, NULL, 0, 64,
                         &capture) ==
               PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
        assert(capture.calls == 0 && mock.executor_calls == 4 &&
               mock.statement_failed == 1);
        reset_scan(&mock, &capture, 0);
        assert(run_query(&mock, avg_sql, sizeof(avg_sql) - 1, NULL, 0, 64,
                         &capture) ==
               PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
        assert(capture.calls == 0 && mock.executor_calls == 4 &&
               mock.statement_failed == 1);
        reset_scan(&mock, &capture, 0);
        assert(run_query(&mock, sum_sql, sizeof(sum_sql) - 1, NULL, 0, 64,
                         &capture) ==
               PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
        assert(capture.calls == 0 && mock.executor_calls == 4 &&
               mock.statement_failed == 1);
    }

    mock.batch_len = make_rows(mock.batch, 0, keys, scores, value_states, 3);
    reset_scan(&mock, &capture, 0);
    assert(run_query(&mock, min_sql, sizeof(min_sql) - 1, NULL, 0, 4,
                     &capture) ==
           PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT);
    assert(capture.calls == 0 && mock.executor_calls == 4 &&
           mock.statement_failed == 1);

    reset_scan(&mock, &capture, 0);
    mock.cancelled = 1;
    assert(run_query(&mock, min_sql, sizeof(min_sql) - 1, NULL, 0, 64,
                     &capture) == PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    assert(capture.calls == 0 && mock.next_calls == 0 &&
           mock.executor_calls == 3 && mock.statement_failed == 1);
    return 0;
}
