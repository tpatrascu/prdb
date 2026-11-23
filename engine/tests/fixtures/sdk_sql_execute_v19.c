#include "prdb_sdk_sql_exec_v8.h"
#include "prdb_sdk_sql_exec_v16.h"
#include "prdb_sdk_sql_exec_v19.h"
#include "prdb_sdk_sql_exec_v20.h"
#include "prdb_sdk_sql_v24.h"
#include "prdb_sdk_sql_dispatch_v1.h"
#include "prdb_sdk_spill_v1.h"
#include "prdb_sdk_optimizer_v1.h"
#include "prdb_sdk_optimizer_plan_v1.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define JOIN_V16_STRESS_ROWS 47
#define JOIN_V16_ROW_LIMIT PRDB_SDK_SQL_EXEC_V8_MAX_ROWS

typedef struct mock_state {
    uint8_t planner[2][256];
    uint32_t planner_len[2];
    uint8_t executor[2][256];
    uint32_t executor_len[2];
    uint8_t plan_relation[2][512];
    uint32_t plan_relation_len[2];
    uint8_t optimizer_response[2048];
    uint32_t optimizer_response_len;
    uint32_t optimizer_calls;
    uint8_t batch[2][8192];
    uint32_t batch_len[2];
    uint8_t paged_batch[2][2][8192];
    uint32_t paged_batch_len[2][2];
    uint32_t paged_batch_count[2];
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
    uint32_t range_calls;
    uint16_t range_row_count;
    int32_t range_value_base;
    uint8_t repeat_range_values;
    uint32_t statement_failed;
    struct spill_mock_stream {
        uint8_t handle[16];
        uint8_t *bytes;
        uint64_t length;
        uint8_t live;
        uint8_t sealed;
    } spill_streams[16];
    uint64_t spill_stream_id;
    uint32_t spill_append_calls;
    uint32_t spill_fail_append_at;
    uint32_t result_write_calls;
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

static void put_u64(uint8_t *bytes, uint32_t *offset, uint64_t value) {
    put_u32(bytes, offset, (uint32_t)(value >> 32));
    put_u32(bytes, offset, (uint32_t)value);
}

static uint64_t get_u64_be(const uint8_t *bytes) {
    uint64_t value = 0;
    for (uint32_t index = 0; index < 8; ++index)
        value = (value << 8) | bytes[index];
    return value;
}

static uint32_t get_u32_be(const uint8_t *bytes) {
    return ((uint32_t)bytes[0] << 24) | ((uint32_t)bytes[1] << 16) |
           ((uint32_t)bytes[2] << 8) | bytes[3];
}

static void put_u64_be(uint8_t *bytes, uint64_t value) {
    for (uint32_t index = 0; index < 8; ++index)
        bytes[index] = (uint8_t)(value >> (56 - index * 8));
}

static int32_t spill_request(void *context, uint16_t operation,
                             const uint8_t *request, uint64_t request_len,
                             uint8_t *response, uint64_t response_capacity,
                             uint64_t *response_len) {
    mock_state *mock = (mock_state *)context;
    struct spill_mock_stream *stream = NULL;
    *response_len = 0;
    if (operation == PRDB_SPILL_OP_CREATE) {
        assert(request_len == 0 && response_capacity == 16);
        for (uint32_t index = 0; index < 16; ++index) {
            if (mock->spill_streams[index].live) continue;
            stream = &mock->spill_streams[index];
            memset(stream, 0, sizeof(*stream));
            stream->bytes = malloc(UINT64_C(1048576));
            if (stream->bytes == NULL) return -1;
            stream->live = 1;
            put_u64_be(stream->handle, ++mock->spill_stream_id);
            memcpy(response, stream->handle, 16);
            *response_len = 16;
            return 0;
        }
        return -1;
    }
    assert(request_len >= 16);
    for (uint32_t index = 0; index < 16; ++index) {
        if (mock->spill_streams[index].live &&
            memcmp(mock->spill_streams[index].handle, request, 16) == 0) {
            stream = &mock->spill_streams[index];
            break;
        }
    }
    if (stream == NULL) return -1;
    if (operation == PRDB_SPILL_OP_APPEND) {
        uint64_t count = request_len - 16;
        if (++mock->spill_append_calls == mock->spill_fail_append_at ||
            stream->sealed || count > UINT64_C(1048576) - stream->length)
            return -1;
        memcpy(stream->bytes + stream->length, request + 16, (size_t)count);
        stream->length += count;
    } else if (operation == PRDB_SPILL_OP_SEAL) {
        assert(request_len == 16);
        stream->sealed = 1;
    } else if (operation == PRDB_SPILL_OP_READ_AT) {
        uint64_t offset;
        uint32_t capacity, count;
        assert(request_len == 28 && stream->sealed);
        offset = get_u64_be(request + 16);
        capacity = get_u32_be(request + 24);
        if (offset > stream->length || capacity > response_capacity)
            return -1;
        count = (uint32_t)(stream->length - offset);
        if (count > capacity) count = capacity;
        memcpy(response, stream->bytes + offset, count);
        *response_len = count;
    } else if (operation == PRDB_SPILL_OP_DESTROY) {
        assert(request_len == 16);
        free(stream->bytes);
        memset(stream, 0, sizeof(*stream));
    } else {
        return -1;
    }
    return 0;
}

static void put_name(uint8_t *bytes, uint32_t *offset, const char *name) {
    uint16_t length = (uint16_t)strlen(name);
    put_u16(bytes, offset, length);
    memcpy(bytes + *offset, name, length);
    *offset += length;
}

static void put_plan_name(uint8_t *bytes, uint32_t *offset, const char *name) {
    uint8_t length = (uint8_t)strlen(name);
    bytes[(*offset)++] = length;
    memcpy(bytes + *offset, name, length);
    *offset += length;
}

static uint32_t put_plan_column(uint8_t *bytes, uint32_t *offset,
                                uint8_t id, uint32_t ordinal,
                                uint8_t nullable) {
    memset(bytes + *offset, id, 16);
    *offset += 16;
    put_u32(bytes, offset, ordinal);
    bytes[(*offset)++] = 3; /* INTEGER */
    memset(bytes + *offset, 0, 4);
    *offset += 4;
    bytes[(*offset)++] = nullable;
    return *offset;
}

static uint32_t build_optimizer_join_plan(uint8_t *bytes) {
    uint32_t offset = 0;
    bytes[offset++] = 0;
    bytes[offset++] = 1;
    bytes[offset++] = 4;
    bytes[offset++] = 0;
    put_plan_name(bytes, &offset, "app");
    memset(bytes + offset, 0x61, 16);
    offset += 16;
    put_u64(bytes, &offset, 1);
    for (uint32_t side = 0; side < 2; ++side) {
        put_plan_name(bytes, &offset, side == 0 ? "accounts" : "orders");
        memset(bytes + offset, (int)(0x11 + side), 16);
        offset += 16;
        put_u64(bytes, &offset, 1);
    }
    for (uint32_t side = 0; side < 2; ++side) {
        put_u16(bytes, &offset, 2);
        put_plan_column(bytes, &offset, 0x31, 0, 1);
        put_plan_column(bytes, &offset, 0x32, 1, 1);
    }
    put_u16(bytes, &offset, 2);
    bytes[offset++] = 0;
    put_plan_column(bytes, &offset, 0x32, 1, 1);
    bytes[offset++] = 1;
    put_plan_column(bytes, &offset, 0x32, 1, 1);
    bytes[offset++] = 2;
    put_plan_column(bytes, &offset, 0x31, 0, 1);
    put_plan_column(bytes, &offset, 0x31, 0, 1);
    put_plan_column(bytes, &offset, 0x32, 1, 1);
    put_plan_column(bytes, &offset, 0x32, 1, 1);
    bytes[offset++] = 0;
    return offset;
}

static uint32_t build_optimizer_filtered_join_plan(uint8_t *bytes) {
    uint32_t offset = build_optimizer_join_plan(bytes);
    bytes[2] = PRDB_OPTIMIZER_FILTERED_JOIN_SELECT_STATEMENT;
    bytes[offset - 1] = 1;
    put_u32(bytes, &offset, 1);
    bytes[offset++] = 1;
    put_plan_column(bytes, &offset, 0x32, 1, 1);
    bytes[offset++] = 0;
    bytes[offset++] = 5;
    put_u32(bytes, &offset, 1);
    return offset;
}

static uint32_t build_optimizer_index_join_plan(uint8_t *bytes) {
    uint32_t offset = build_optimizer_join_plan(bytes);
    bytes[2] = PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_NESTED_LOOP_JOIN;
    bytes[offset++] = 0; /* no filter */
    bytes[offset++] = 0; /* outer left */
    bytes[offset++] = 1; /* inner right */
    memset(bytes + offset, 0x71, 16);
    offset += 16;
    put_plan_column(bytes, &offset, 0x31, 0, 1);
    return offset;
}

static void set_join_id_columns_decimal(uint8_t *bytes, uint32_t length) {
    for (uint32_t offset = 0; offset + 25 <= length; ++offset) {
        uint32_t byte = 0;
        while (byte < 16 && bytes[offset + byte] == 0x31) ++byte;
        if (byte == 16 && bytes[offset + 20] == 3) {
            bytes[offset + 20] = 5; /* DECIMAL */
            bytes[offset + 21] = 12;
            bytes[offset + 22] = 2;
            bytes[offset + 23] = 0;
            bytes[offset + 24] = 0;
        }
    }
}

static uint32_t build_optimizer_filtered_index_join_plan(uint8_t *bytes) {
    uint32_t offset = build_optimizer_join_plan(bytes);
    bytes[2] = PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_NESTED_LOOP_JOIN;
    bytes[offset - 1] = 1; /* LIMIT 1 */
    put_u32(bytes, &offset, 1);
    bytes[offset++] = 1; /* filter present */
    bytes[offset++] = 1; /* right side */
    put_plan_column(bytes, &offset, 0x32, 1, 1);
    bytes[offset++] = 0; /* parameter comparison */
    bytes[offset++] = 5; /* greater-or-equal */
    put_u32(bytes, &offset, 1);
    bytes[offset++] = 0; /* outer left */
    bytes[offset++] = 1; /* inner right */
    memset(bytes + offset, 0x71, 16);
    offset += 16;
    put_plan_column(bytes, &offset, 0x31, 0, 1);
    return offset;
}

static uint32_t build_plan_relation(uint8_t *bytes, uint32_t index) {
    uint32_t offset = 0;
    const char *name = index == 0 ? "accounts" : "orders";
    bytes[offset++] = 0;
    bytes[offset++] = 1;
    bytes[offset++] = 0;
    memset(bytes + offset, 0x61, 16);
    offset += 16;
    put_u64(bytes, &offset, 1);
    put_u64(bytes, &offset, 1);
    memset(bytes + offset, (int)(0x11 + index), 16);
    offset += 16;
    put_name(bytes, &offset, name);
    put_u16(bytes, &offset, 2);
    for (uint32_t ordinal = 0; ordinal < 2; ++ordinal) {
        memset(bytes + offset, (int)(0x31 + ordinal), 16);
        offset += 16;
        put_u32(bytes, &offset, ordinal);
        bytes[offset++] = 1;
        bytes[offset++] = PRDB_SDK_VALUE_TYPE_INTEGER;
        memset(bytes + offset, 0, 4);
        offset += 4;
        put_name(bytes, &offset, ordinal == 0 ? "account_id" : "value");
    }
    return offset;
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

static uint32_t make_batch_with_column_states(
    uint8_t *bytes, const int32_t *keys, const uint8_t *key_states,
    const int32_t *values, const uint8_t *value_states, uint16_t row_count) {
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
        if (value_states[row] == PRDB_SDK_VALUE_STATE_VALUE) {
            append_integer_cell(bytes, &offset, values[row]);
        } else {
            bytes[offset++] = value_states[row];
        }
    }
    return offset;
}

static uint32_t make_batch_with_value_state(
    uint8_t *bytes, const int32_t *keys, const uint8_t *key_states,
    const int32_t *values, uint8_t value_state, uint16_t row_count) {
    uint8_t value_states[JOIN_V16_ROW_LIMIT + 1];
    assert(row_count <= JOIN_V16_ROW_LIMIT + 1);
    for (uint16_t row = 0; row < row_count; ++row) {
        value_states[row] = value_state;
    }
    return make_batch_with_column_states(bytes, keys, key_states, values,
                                         value_states, row_count);
}

static uint32_t make_batch(uint8_t *bytes, const int32_t *keys,
                           const uint8_t *key_states,
                           const int32_t *values, uint16_t row_count) {
    return make_batch_with_value_state(
        bytes, keys, key_states, values, PRDB_SDK_VALUE_STATE_VALUE,
        row_count);
}

static uint32_t make_continuation_batch(uint8_t *bytes, uint32_t length) {
    assert(length >= 5 && bytes[2] == 1);
    memmove(bytes + 19, bytes + 3, length - 3);
    bytes[2] = 0;
    bytes[3] = PRDB_SDK_BATCH_CURSOR_TOKEN_KIND;
    memset(bytes + 4, 0x7a, 15);
    return length + 16;
}

static int32_t statement_failure(void *context, uint32_t failure_code) {
    mock_state *mock = (mock_state *)context;
    assert(failure_code == PRDB_QUERY_STATEMENT_FAILURE_SQL);
    mock->statement_failed = 1;
    return PRDB_QUERY_CALLBACK_OK;
}

static int32_t write_result(void *context, const uint8_t *bytes,
                            uint64_t bytes_len) {
    mock_state *mock = (mock_state *)context;
    assert(bytes != NULL && bytes_len != 0);
    ++mock->result_write_calls;
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

static int32_t optimizer_request_v4(void *context, const uint8_t *request,
                                    uint64_t request_len, uint8_t *response,
                                    uint64_t capacity,
                                    uint64_t *response_len) {
    (void)context;
    (void)request;
    (void)request_len;
    (void)response;
    (void)capacity;
    (void)response_len;
    return PRDB_QUERY_CALLBACK_INTERNAL_ERROR;
}

static int32_t optimizer_request_v5(void *context, uint16_t operation,
                                   const uint8_t *request,
                                   uint64_t request_len, uint8_t *response,
                                   uint64_t capacity,
                                   uint64_t *response_len) {
    mock_state *mock = (mock_state *)context;
    assert(operation == PRDB_IPC_OP_OPTIMIZE_JOIN_SELECT);
    assert(request_len >= 4 && request[0] == 0 && request[1] == 1 &&
           (request[2] == PRDB_OPTIMIZER_JOIN_SELECT_STATEMENT ||
            request[2] == PRDB_OPTIMIZER_FILTERED_JOIN_SELECT_STATEMENT) &&
           request[3] == 0);
    ++mock->optimizer_calls;
    if (mock->optimizer_response_len > capacity) {
        *response_len = mock->optimizer_response_len;
        return PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL;
    }
    memcpy(response, mock->optimizer_response, mock->optimizer_response_len);
    *response_len = mock->optimizer_response_len;
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
    } else if (operation == PRDB_SDK_EXECUTOR_OP_RESOLVE_PLAN_RELATION) {
        uint32_t index = relation_index(request, request_len);
        assert(mock->plan_relation_len[index] <= capacity);
        memcpy(response, mock->plan_relation[index],
               mock->plan_relation_len[index]);
        *response_len = mock->plan_relation_len[index];
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
            if (mock->paged_batch_count[index] != 0) {
                uint32_t page = mock->next_calls[index];
                assert(page < mock->paged_batch_count[index]);
                assert(mock->paged_batch_len[index][page] <= capacity);
                memcpy(response, mock->paged_batch[index][page],
                       mock->paged_batch_len[index][page]);
                *response_len = mock->paged_batch_len[index][page];
                mock->scan_finished[index] =
                    page + 1 == mock->paged_batch_count[index];
            } else {
                assert(mock->batch_len[index] <= capacity);
                memcpy(response, mock->batch[index], mock->batch_len[index]);
                *response_len = mock->batch_len[index];
                mock->scan_finished[index] = 1;
            }
        }
        ++mock->next_calls[index];
    } else if (operation == PRDB_SDK_EXECUTOR_OP_SCAN_INDEX_RANGE) {
        uint32_t offset = 0;
        uint32_t row_count = mock->range_row_count == 0 ? 1 : mock->range_row_count;
        int32_t value_base = mock->range_value_base == 0 ? 101 : mock->range_value_base;
        assert(request_len >= 34 && request[2] == 6 && request[3] == 0x22);
        assert(memcmp(request + 18, (uint8_t[16]){
                   0x71,0x71,0x71,0x71,0x71,0x71,0x71,0x71,
                   0x71,0x71,0x71,0x71,0x71,0x71,0x71,0x71}, 16) == 0);
        assert(mock->open_calls == 1);
        response[offset++] = 1; /* range response version */
        response[offset++] = 1; /* available */
        put_u16(response, &offset, (uint16_t)row_count);
        for (uint32_t row = 0; row < row_count; ++row) {
            put_u16(response, &offset, 2);
            append_integer_cell(response, &offset, 1);
            append_integer_cell(response, &offset,
                value_base + (mock->repeat_range_values ? 0 : (int32_t)row));
        }
        assert(offset <= capacity);
        *response_len = offset;
        mock->scan_finished[1] = 1;
        ++mock->range_calls;
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

static int32_t on_row_v19(void *context,
                          const prdb_sdk_value_v1 *projected_values,
                          uint32_t projection_count,
                          const prdb_sdk_select_binding_v19 *binding) {
    row_capture *capture = (row_capture *)context;
    assert(capture->mock->scan_finished[0] == 1 &&
           capture->mock->scan_finished[1] == 1);
    assert(binding->base.base.projection_count == 2 &&
           (binding->base.join_key_count == 2 || binding->base.join_key_count == 8) &&
           projection_count == 2);
    if (capture->calls < 8) {
        assert(prdb_sdk_value_get_integer_v1(
                   &projected_values[0],
                   &capture->left_values[capture->calls]) ==
               PRDB_SDK_VALUE_ACCESS_OK);
        assert(prdb_sdk_value_get_integer_v1(
                   &projected_values[1],
                   &capture->right_values[capture->calls]) ==
               PRDB_SDK_VALUE_ACCESS_OK);
    }
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

static int32_t run_query_v19(mock_state *mock, row_capture *capture,
                             const uint8_t *sql, uint64_t sql_len,
                             uint64_t materialized_capacity) {
    static const uint8_t schema[] = "app";
    uint8_t left_planner_response[256] = {0};
    uint8_t right_planner_response[256] = {0};
    uint8_t left_executor_response[256] = {0};
    uint8_t right_executor_response[256] = {0};
    uint8_t batch_response[8192] = {0};
    uint8_t materialized_bytes[4096] = {0};
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
    return prdb_sdk_sql_execute_inner_join_v19(
        &host, schema, sizeof(schema) - 1, sql, sql_len,
        JOIN_V16_ROW_LIMIT, 8192,
        left_planner_response, sizeof(left_planner_response),
        right_planner_response, sizeof(right_planner_response),
        left_executor_response, sizeof(left_executor_response),
        right_executor_response, sizeof(right_executor_response),
        batch_response, sizeof(batch_response), left_planner_columns, 2,
        right_planner_columns, 2, left_executor_columns, 2,
        right_executor_columns, 2, cell_scratch, 2, left_rows,
        PRDB_SDK_SQL_EXEC_V8_MAX_ROWS, right_rows,
        PRDB_SDK_SQL_EXEC_V8_MAX_ROWS, materialized_bytes,
        materialized_capacity, on_row_v19, capture);
}

static int32_t run_query_optimizer_with_materialized_capacity(
    mock_state *mock, row_capture *capture, const uint8_t *sql,
    uint64_t sql_len, uint64_t materialized_capacity) {
    static const uint8_t schema[] = "app";
    uint8_t optimizer_request[512] = {0};
    uint8_t optimizer_response[2048] = {0};
    uint8_t left_planner_response[256] = {0};
    uint8_t right_planner_response[256] = {0};
    uint8_t left_executor_response[65536] = {0};
    uint8_t right_executor_response[65536] = {0};
    uint8_t batch_response[8192] = {0};
    uint8_t materialized_bytes[262144] = {0};
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
    prdb_host_api_v3 host3 = {sizeof(host3), PRDB_QUERY_ABI_V3_VERSION,
                              mock, executor_request, planner_request,
                              write_result,
                              statement_failure};
    prdb_host_api_v4 host4 = {host3, optimizer_request_v4};
    prdb_host_api_v5 host5 = {host4, optimizer_request_v5};
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
    return prdb_sdk_sql_execute_inner_join_optimizer_v1(
        &host5, schema, sizeof(schema) - 1, sql, sql_len,
        JOIN_V16_ROW_LIMIT, 8192, optimizer_request,
        sizeof(optimizer_request), optimizer_response,
        sizeof(optimizer_response), left_planner_response,
        sizeof(left_planner_response), right_planner_response,
        sizeof(right_planner_response), left_executor_response,
        sizeof(left_executor_response), right_executor_response,
        sizeof(right_executor_response), batch_response,
        sizeof(batch_response), left_planner_columns, 2,
        right_planner_columns, 2, left_executor_columns, 2,
        right_executor_columns, 2, cell_scratch, 2, left_rows,
        PRDB_SDK_SQL_EXEC_V8_MAX_ROWS, right_rows,
        PRDB_SDK_SQL_EXEC_V8_MAX_ROWS, materialized_bytes,
        materialized_capacity, on_row_v19, capture);
}

static int32_t run_query_optimizer(mock_state *mock, row_capture *capture,
                                   const uint8_t *sql, uint64_t sql_len) {
    return run_query_optimizer_with_materialized_capacity(
        mock, capture, sql, sql_len, UINT64_C(262144));
}

static int32_t on_row_v20(void *context,
                          const prdb_sdk_value_v1 *projected_values,
                          uint32_t projection_count,
                          const prdb_sdk_select_binding_v20 *binding) {
    prdb_sdk_select_binding_v19 old_binding = binding->base;
    return on_row_v19(context, projected_values, projection_count,
                      &old_binding);
}

static int32_t run_query_v20(mock_state *mock, row_capture *capture,
                             const uint8_t *sql, uint64_t sql_len,
                             uint64_t materialized_capacity,
                             const prdb_sdk_value_v1 *parameters,
                             uint32_t parameter_count) {
    static const uint8_t schema[] = "app";
    uint8_t left_planner_response[256] = {0};
    uint8_t right_planner_response[256] = {0};
    uint8_t left_executor_response[256] = {0};
    uint8_t right_executor_response[256] = {0};
    uint8_t batch_response[8192] = {0};
    uint8_t materialized_bytes[4096] = {0};
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
            integer_names[index], len, index, PRDB_SDK_VALUE_TYPE_INTEGER,
            0, 0};
        right_planner_columns[index] = left_planner_columns[index];
        left_executor_columns[index] = left_planner_columns[index];
        right_executor_columns[index] = left_planner_columns[index];
    }
    return prdb_sdk_sql_execute_inner_join_v20(
        &host, schema, sizeof(schema) - 1, sql, sql_len, parameters,
        parameter_count, JOIN_V16_ROW_LIMIT, 8192,
        left_planner_response, sizeof(left_planner_response),
        right_planner_response, sizeof(right_planner_response),
        left_executor_response, sizeof(left_executor_response),
        right_executor_response, sizeof(right_executor_response),
        batch_response, sizeof(batch_response), left_planner_columns, 2,
        right_planner_columns, 2, left_executor_columns, 2,
        right_executor_columns, 2, cell_scratch, 2, left_rows,
        PRDB_SDK_SQL_EXEC_V8_MAX_ROWS, right_rows,
        PRDB_SDK_SQL_EXEC_V8_MAX_ROWS, materialized_bytes,
        materialized_capacity, on_row_v20, capture);
}

static int32_t run_query_spill_v21(
    mock_state *mock, row_capture *capture, const uint8_t *sql,
    uint64_t sql_len, const prdb_sdk_value_v1 *parameters,
    uint32_t parameter_count) {
    static const uint8_t schema[] = "app";
    uint8_t left_planner_response[256] = {0};
    uint8_t right_planner_response[256] = {0};
    uint8_t left_executor_response[256] = {0};
    uint8_t right_executor_response[256] = {0};
    uint8_t batch_response[8192] = {0};
    uint8_t materialized_bytes[4096] = {0};
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
    prdb_host_api_v3 host3 = {sizeof(host3), PRDB_QUERY_ABI_V3_VERSION,
                              mock, executor_request, planner_request, NULL,
                              statement_failure};
    prdb_host_api_v6 host6 = {0};
    host6.v5.v4.v3 = host3;
    host6.spill_request = spill_request;
    memcpy(integer_names[0], "account_id", sizeof("account_id") - 1);
    memcpy(integer_names[1], "value", sizeof("value") - 1);
    for (uint32_t index = 0; index < 2; ++index) {
        const char *name = index == 0 ? "account_id" : "value";
        uint32_t len = (uint32_t)strlen(name);
        left_planner_columns[index] = (prdb_sdk_sql_column_shape_v2){
            integer_names[index], len, index, PRDB_SDK_VALUE_TYPE_INTEGER,
            0, 0};
        right_planner_columns[index] = left_planner_columns[index];
        left_executor_columns[index] = left_planner_columns[index];
        right_executor_columns[index] = left_planner_columns[index];
    }
    return prdb_sdk_sql_execute_inner_join_typed_spill_v21(
        &host6, schema, sizeof(schema) - 1, sql, sql_len, parameters, NULL,
        parameter_count, JOIN_V16_ROW_LIMIT, 8192,
        left_planner_response, sizeof(left_planner_response),
        right_planner_response, sizeof(right_planner_response),
        left_executor_response, sizeof(left_executor_response),
        right_executor_response, sizeof(right_executor_response),
        batch_response, sizeof(batch_response), left_planner_columns, 2,
        right_planner_columns, 2, left_executor_columns, 2,
        right_executor_columns, 2, cell_scratch, 2, left_rows,
        PRDB_SDK_SQL_EXEC_V8_MAX_ROWS, right_rows,
        PRDB_SDK_SQL_EXEC_V8_MAX_ROWS, materialized_bytes,
        sizeof(materialized_bytes), on_row_v20, capture);
}

static int32_t run_statement_spill_v21(mock_state *mock, const uint8_t *sql,
                                       uint64_t sql_len) {
    static const uint8_t schema[] = "app";
    prdb_host_api_v3 host3 = {sizeof(host3), PRDB_QUERY_ABI_V3_VERSION,
                              mock, executor_request, planner_request,
                              write_result, statement_failure};
    prdb_host_api_v6 host6 = {0};
    host6.v5.v4.v3 = host3;
    host6.spill_request = spill_request;
    return prdb_sdk_sql_run_statement_v2(
        &host6, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0);
}

static int32_t run_statement_no_spill_v6(mock_state *mock, const uint8_t *sql,
                                         uint64_t sql_len) {
    static const uint8_t schema[] = "app";
    prdb_host_api_v3 host3 = {sizeof(host3), PRDB_QUERY_ABI_V3_VERSION,
                              mock, executor_request, planner_request,
                              write_result, statement_failure};
    prdb_host_api_v6 host6 = {0};
    host6.v5.v4.v3 = host3;
    return prdb_sdk_sql_run_statement_v2(
        &host6, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0);
}

static int32_t run_query_optimizer_v20(
    mock_state *mock, row_capture *capture, const uint8_t *sql,
    uint64_t sql_len, const prdb_sdk_value_v1 *parameters,
    uint32_t parameter_count) {
    static const uint8_t schema[] = "app";
    uint8_t optimizer_request[512] = {0};
    uint8_t optimizer_response[2048] = {0};
    uint8_t left_planner_response[256] = {0};
    uint8_t right_planner_response[256] = {0};
    uint8_t left_executor_response[512] = {0};
    uint8_t right_executor_response[512] = {0};
    uint8_t batch_response[8192] = {0};
    uint8_t materialized_bytes[4096] = {0};
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
    prdb_host_api_v3 host3 = {sizeof(host3), PRDB_QUERY_ABI_V3_VERSION,
                              mock, executor_request, planner_request, NULL,
                              statement_failure};
    prdb_host_api_v4 host4 = {host3, optimizer_request_v4};
    prdb_host_api_v5 host5 = {host4, optimizer_request_v5};
    memcpy(integer_names[0], "account_id", sizeof("account_id") - 1);
    memcpy(integer_names[1], "value", sizeof("value") - 1);
    for (uint32_t index = 0; index < 2; ++index) {
        const char *name = index == 0 ? "account_id" : "value";
        uint32_t len = (uint32_t)strlen(name);
        left_planner_columns[index] = (prdb_sdk_sql_column_shape_v2){
            integer_names[index], len, index, PRDB_SDK_VALUE_TYPE_INTEGER,
            0, 0};
        right_planner_columns[index] = left_planner_columns[index];
        left_executor_columns[index] = left_planner_columns[index];
        right_executor_columns[index] = left_planner_columns[index];
    }
    return prdb_sdk_sql_execute_inner_join_optimizer_v20(
        &host5, schema, sizeof(schema) - 1, sql, sql_len, parameters,
        parameter_count, JOIN_V16_ROW_LIMIT, 8192, optimizer_request,
        sizeof(optimizer_request), optimizer_response,
        sizeof(optimizer_response), left_planner_response,
        sizeof(left_planner_response), right_planner_response,
        sizeof(right_planner_response), left_executor_response,
        sizeof(left_executor_response), right_executor_response,
        sizeof(right_executor_response), batch_response,
        sizeof(batch_response), left_planner_columns, 2,
        right_planner_columns, 2, left_executor_columns, 2,
        right_executor_columns, 2, cell_scratch, 2, left_rows,
        PRDB_SDK_SQL_EXEC_V8_MAX_ROWS, right_rows,
        PRDB_SDK_SQL_EXEC_V8_MAX_ROWS, materialized_bytes,
        sizeof(materialized_bytes), on_row_v20, capture);
}

static void reset(mock_state *mock, row_capture *capture) {
    mock->planner_calls = 0;
    mock->optimizer_calls = 0;
    mock->resolve_calls = 0;
    mock->open_calls = 0;
    mock->check_cancel_calls = 0;
    mock->cancel_after_check = 0;
    mock->close_calls = 0;
    mock->executor_requests = 0;
    mock->empty_pages[0] = mock->empty_pages[1] = 0;
    mock->paged_batch_count[0] = mock->paged_batch_count[1] = 0;
    mock->next_calls[0] = mock->next_calls[1] = 0;
    mock->scan_finished[0] = mock->scan_finished[1] = 0;
    mock->range_calls = 0;
    mock->range_row_count = 0;
    mock->range_value_base = 0;
    mock->repeat_range_values = 0;
    mock->statement_failed = 0;
    mock->spill_append_calls = 0;
    mock->spill_fail_append_at = 0;
    mock->result_write_calls = 0;
    capture->calls = 0;
    capture->callback_status = PRDB_QUERY_CALLBACK_OK;
}

static void install_optimizer_index_join_plan(mock_state *mock) {
    uint32_t plan_len = build_optimizer_index_join_plan(
        mock->optimizer_response + 7);
    uint32_t offset = 0;
    put_u16(mock->optimizer_response, &offset, 1);
    mock->optimizer_response[offset++] = PRDB_SDK_OPTIMIZER_STATUS_SUCCESS;
    put_u32(mock->optimizer_response, &offset, plan_len);
    offset += plan_len;
    mock->optimizer_response_len = offset;
}

static void install_optimizer_join_plan(mock_state *mock) {
    uint32_t plan_len = build_optimizer_join_plan(
        mock->optimizer_response + 7);
    uint32_t offset = 0;
    put_u16(mock->optimizer_response, &offset, 1);
    mock->optimizer_response[offset++] = PRDB_SDK_OPTIMIZER_STATUS_SUCCESS;
    put_u32(mock->optimizer_response, &offset, plan_len);
    offset += plan_len;
    mock->optimizer_response_len = offset;
}

int main(void) {
    static const uint8_t grouped_spill_sql[] =
        "SELECT account_id, COUNT(*) FROM accounts GROUP BY account_id";
    static const uint8_t grouped_spill_limit_sql[] =
        "SELECT account_id, COUNT(*) FROM accounts GROUP BY account_id LIMIT 2";
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
    static const uint8_t alias_sql[] =
        "SELECT a.value, o.value FROM accounts AS a INNER JOIN "
        "orders AS o ON a.account_id = o.account_id AND "
        "a.account_id = o.account_id";
    static const uint8_t self_join_sql[] =
        "SELECT a.value, b.value FROM accounts AS a INNER JOIN "
        "accounts AS b ON a.account_id = b.account_id AND "
        "a.account_id = b.account_id";
    static const uint8_t composite_sql[] =
        "SELECT accounts.value, orders.value FROM accounts INNER JOIN "
        "orders ON accounts.account_id = orders.account_id AND "
        "accounts.value = orders.value";
    static const uint8_t filtered_sql[] =
        "SELECT a.value, o.value FROM accounts AS a INNER JOIN "
        "orders AS o ON a.account_id = o.account_id AND "
        "a.account_id = o.account_id WHERE o.value >= $1 LIMIT 1";
    static const uint8_t optimizer_filtered_sql[] =
        "SELECT a.value, o.value FROM accounts AS a INNER JOIN "
        "orders AS o ON a.account_id = o.account_id AND "
        "a.value = o.value WHERE o.value >= $1 LIMIT 1";
    static const uint8_t redacted_filter_sql[] =
        "SELECT a.value, o.value FROM accounts AS a INNER JOIN "
        "orders AS o ON a.account_id = o.account_id AND "
        "a.account_id = o.account_id WHERE o.value >= $1";
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
    const int32_t matching_key[] = {2};
    const uint8_t matching_state[] = {PRDB_SDK_VALUE_STATE_VALUE};
    const int32_t matching_left_value[] = {102};
    const int32_t matching_right_value[] = {202};
    static const uint8_t threshold_bytes[] = {0, 0, 0, 202};
    const prdb_sdk_value_v1 threshold = {
        sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
        threshold_bytes, sizeof(threshold_bytes)};
    mock_state mock = {0};
    row_capture capture = {0};

    {
        uint8_t decimal_plan[4096] = {0};
        prdb_sdk_optimizer_index_join_plan_v1 decoded = {0};
        uint32_t decimal_plan_len =
            build_optimizer_index_join_plan(decimal_plan);
        set_join_id_columns_decimal(decimal_plan, decimal_plan_len);
        assert(prdb_sdk_optimizer_decode_index_join_plan_v1(
                   decimal_plan, decimal_plan_len, &decoded,
                   sizeof(decoded)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
        assert(decoded.key_column.type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
               decoded.key_column.type_parameters[0] == 12 &&
               decoded.key_column.type_parameters[1] == 2);
    }

    for (uint32_t index = 0; index < 2; ++index) {
        mock.planner_len[index] =
            make_relation(mock.planner[index], index,
                          PRDB_SDK_PLANNER_TABLE_HANDLE_KIND);
        mock.executor_len[index] =
            make_relation(mock.executor[index], index, 6);
        mock.plan_relation_len[index] =
            build_plan_relation(mock.plan_relation[index], index);
    }
    uint32_t optimizer_plan_len =
        build_optimizer_join_plan(mock.optimizer_response + 7);
    uint32_t optimizer_offset = 0;
    put_u16(mock.optimizer_response, &optimizer_offset, 1);
    mock.optimizer_response[optimizer_offset++] =
        PRDB_SDK_OPTIMIZER_STATUS_SUCCESS;
    put_u32(mock.optimizer_response, &optimizer_offset, optimizer_plan_len);
    optimizer_offset += optimizer_plan_len;
    mock.optimizer_response_len = optimizer_offset;
    mock.batch_len[0] = make_batch(mock.batch[0], left_keys, left_states,
                                   left_values, 3);
    mock.batch_len[1] = make_batch(mock.batch[1], right_keys, right_states,
                                   right_values, 3);
    capture.mock = &mock;

    {
        reset(&mock, &capture);
        assert(run_statement_no_spill_v6(
                   &mock, grouped_spill_sql,
                   sizeof(grouped_spill_sql) - 1) == PRDB_SDK_SQL_DISPATCH_OK);
        assert(mock.result_write_calls == 3 && mock.statement_failed == 0 &&
               mock.next_calls[0] == 1);
        reset(&mock, &capture);

        prdb_sdk_select_v24 grouped_statement = {0};
        assert(prdb_sdk_sql_parse_select_v24(
                   grouped_spill_sql, sizeof(grouped_spill_sql) - 1,
                   &grouped_statement, sizeof(grouped_statement), NULL) ==
               PRDB_SDK_SQL_OK);
        int32_t keys[JOIN_V16_ROW_LIMIT];
        int32_t values[JOIN_V16_ROW_LIMIT];
        uint8_t states[JOIN_V16_ROW_LIMIT];
        int32_t final_key = 129, final_value = 1;
        uint8_t final_state = PRDB_SDK_VALUE_STATE_VALUE;
        for (uint32_t index = 0; index < JOIN_V16_ROW_LIMIT; ++index) {
            keys[index] = (int32_t)(JOIN_V16_ROW_LIMIT - index);
            values[index] = 1;
            states[index] = PRDB_SDK_VALUE_STATE_VALUE;
        }
        mock.paged_batch_len[0][0] = make_continuation_batch(
            mock.paged_batch[0][0], make_batch(
                mock.paged_batch[0][0], keys, states, values,
                JOIN_V16_ROW_LIMIT));
        mock.paged_batch_len[0][1] = make_batch(
            mock.paged_batch[0][1], &final_key, &final_state, &final_value, 1);
        reset(&mock, &capture);
        mock.paged_batch_count[0] = 2;
        assert(run_statement_spill_v21(
                   &mock, grouped_spill_sql,
                   sizeof(grouped_spill_sql) - 1) == PRDB_SDK_SQL_DISPATCH_OK);
        assert(mock.result_write_calls == JOIN_V16_ROW_LIMIT + 1 &&
               mock.next_calls[0] == 2 && mock.statement_failed == 0);
        for (uint32_t stream = 0; stream < 16; ++stream)
            assert(mock.spill_streams[stream].live == 0);

        reset(&mock, &capture);
        mock.paged_batch_count[0] = 2;
        assert(run_statement_spill_v21(
                   &mock, grouped_spill_limit_sql,
                   sizeof(grouped_spill_limit_sql) - 1) ==
               PRDB_SDK_SQL_DISPATCH_OK);
        assert(mock.result_write_calls == 2 && mock.next_calls[0] == 2 &&
               mock.statement_failed == 0);
        for (uint32_t stream = 0; stream < 16; ++stream)
            assert(mock.spill_streams[stream].live == 0);

        {
            const int32_t null_keys[] = {0, 0, 5};
            const uint8_t null_states[] = {
                PRDB_SDK_VALUE_STATE_NULL, PRDB_SDK_VALUE_STATE_NULL,
                PRDB_SDK_VALUE_STATE_VALUE};
            const int32_t null_values[] = {0, 0, 1};
            mock.batch_len[0] = make_batch(
                mock.batch[0], null_keys, null_states, null_values, 3);
            reset(&mock, &capture);
            assert(run_statement_spill_v21(
                       &mock, grouped_spill_sql,
                       sizeof(grouped_spill_sql) - 1) ==
                   PRDB_SDK_SQL_DISPATCH_OK);
            assert(mock.result_write_calls == 2 && mock.statement_failed == 0);
            for (uint32_t stream = 0; stream < 16; ++stream)
                assert(mock.spill_streams[stream].live == 0);

            const uint8_t redacted_states[] = {
                PRDB_SDK_VALUE_STATE_REDACTED,
                PRDB_SDK_VALUE_STATE_VALUE,
                PRDB_SDK_VALUE_STATE_VALUE};
            mock.batch_len[0] = make_batch(
                mock.batch[0], null_keys, redacted_states, null_values, 3);
            reset(&mock, &capture);
            assert(run_statement_spill_v21(
                       &mock, grouped_spill_sql,
                       sizeof(grouped_spill_sql) - 1) ==
                   PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR);
            assert(mock.result_write_calls == 0 && mock.statement_failed == 1);
            for (uint32_t stream = 0; stream < 16; ++stream)
                assert(mock.spill_streams[stream].live == 0);
            mock.batch_len[0] = make_batch(
                mock.batch[0], left_keys, left_states, left_values, 3);
        }

        reset(&mock, &capture);
        mock.paged_batch_count[0] = 2;
        mock.spill_fail_append_at = 1;
        assert(run_statement_spill_v21(
                   &mock, grouped_spill_sql,
                   sizeof(grouped_spill_sql) - 1) ==
               PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR);
        assert(mock.result_write_calls == 0 && mock.statement_failed == 1);
        for (uint32_t stream = 0; stream < 16; ++stream)
            assert(mock.spill_streams[stream].live == 0);

        reset(&mock, &capture);
        mock.paged_batch_count[0] = 2;
        mock.cancel_after_check = 1;
        assert(run_statement_spill_v21(
                   &mock, grouped_spill_sql,
                   sizeof(grouped_spill_sql) - 1) ==
               PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR);
        assert(mock.result_write_calls == 0 && mock.statement_failed == 1);
        for (uint32_t stream = 0; stream < 16; ++stream)
            assert(mock.spill_streams[stream].live == 0);
    }
    mock.paged_batch_count[0] = 0;
    reset(&mock, &capture);

    assert(run_query(&mock, &capture, sql, sizeof(sql) - 1, 1024) ==
           PRDB_SDK_SQL_EXEC_OK);
    assert(mock.planner_calls == 2 && mock.resolve_calls == 2 &&
           mock.open_calls == 2 && mock.next_calls[0] == 1 &&
           mock.next_calls[1] == 1 && mock.statement_failed == 0);
    assert(capture.calls == 2 && capture.left_values[0] == 101 &&
           capture.right_values[0] == 201 && capture.left_values[1] == 102 &&
           capture.right_values[1] == 202);

    {
        const int32_t common_key[] = {5};
        const uint8_t value_state[] = {PRDB_SDK_VALUE_STATE_VALUE};
        const uint8_t null_value_state[] = {PRDB_SDK_VALUE_STATE_NULL};
        const int32_t left_value[] = {105};
        const int32_t right_value[] = {205};
        mock.batch_len[0] = make_batch_with_column_states(
            mock.batch[0], common_key, value_state, left_value,
            null_value_state, 1);
        mock.batch_len[1] = make_batch_with_column_states(
            mock.batch[1], common_key, value_state, right_value,
            null_value_state, 1);
        reset(&mock, &capture);
        assert(run_query_v19(&mock, &capture, composite_sql,
                             sizeof(composite_sql) - 1, 1024) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 0 && mock.planner_calls == 2 &&
               mock.resolve_calls == 2 && mock.open_calls == 2 &&
               mock.scan_finished[0] == 1 && mock.scan_finished[1] == 1 &&
               mock.statement_failed == 0);
        mock.batch_len[0] = make_batch(mock.batch[0], left_keys, left_states,
                                       left_values, 3);
        mock.batch_len[1] = make_batch(mock.batch[1], right_keys, right_states,
                                       right_values, 3);
    }

    {
        const int32_t outer_key[] = {1};
        const uint8_t outer_state[] = {PRDB_SDK_VALUE_STATE_VALUE};
        const int32_t outer_value[] = {101};
        uint32_t plan_len = build_optimizer_index_join_plan(
            mock.optimizer_response + 7);
        uint32_t response_offset = 0;
        put_u16(mock.optimizer_response, &response_offset, 1);
        mock.optimizer_response[response_offset++] =
            PRDB_SDK_OPTIMIZER_STATUS_SUCCESS;
        put_u32(mock.optimizer_response, &response_offset, plan_len);
        response_offset += plan_len;
        mock.optimizer_response_len = response_offset;
        mock.batch_len[0] = make_batch(mock.batch[0], outer_key, outer_state,
                                       outer_value, 1);
        reset(&mock, &capture);
        int32_t index_join_status = run_query_optimizer(
            &mock, &capture, composite_sql, sizeof(composite_sql) - 1);
        assert(index_join_status == PRDB_SDK_SQL_EXEC_OK);
        assert(mock.optimizer_calls == 1 && mock.open_calls == 1 &&
               mock.next_calls[0] == 1 && mock.range_calls == 1 &&
               mock.executor_requests < 256 &&
               capture.calls == 1 && capture.left_values[0] == 101 &&
               capture.right_values[0] == 101 && mock.statement_failed == 0);
        uint32_t restored_len = build_optimizer_join_plan(
            mock.optimizer_response + 7);
        response_offset = 0;
        put_u16(mock.optimizer_response, &response_offset, 1);
        mock.optimizer_response[response_offset++] =
            PRDB_SDK_OPTIMIZER_STATUS_SUCCESS;
        put_u32(mock.optimizer_response, &response_offset, restored_len);
        response_offset += restored_len;
        mock.optimizer_response_len = response_offset;
    }

    {
        const int32_t outer_key[] = {1};
        const uint8_t outer_state[] = {PRDB_SDK_VALUE_STATE_VALUE};
        const int32_t outer_value[] = {101};
        const int32_t duplicate_keys[] = {1, 1};
        const uint8_t duplicate_states[] = {
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_STATE_VALUE};
        const int32_t duplicate_values[] = {101, 101};
        mock.batch_len[0] = make_batch(mock.batch[0], outer_key, outer_state,
                                       outer_value, 1);
        mock.batch_len[1] = make_batch(mock.batch[1], duplicate_keys,
                                       duplicate_states, duplicate_values, 2);
        install_optimizer_join_plan(&mock);
        reset(&mock, &capture);
        assert(run_query_optimizer(&mock, &capture, composite_sql,
                                  sizeof(composite_sql) - 1) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && capture.left_values[0] == 101 &&
               capture.right_values[0] == 101 && capture.left_values[1] == 101 &&
               capture.right_values[1] == 101 && mock.statement_failed == 0);
        const int32_t hash_left[2] = {
            capture.left_values[0], capture.left_values[1]};
        const int32_t hash_right[2] = {
            capture.right_values[0], capture.right_values[1]};

        install_optimizer_index_join_plan(&mock);
        reset(&mock, &capture);
        mock.range_row_count = 2;
        mock.repeat_range_values = 1;
        assert(run_query_optimizer(&mock, &capture, composite_sql,
                                  sizeof(composite_sql) - 1) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(mock.range_calls == 1 && capture.calls == 2 &&
               capture.left_values[0] == 101 && capture.right_values[0] == 101 &&
               capture.left_values[1] == 101 && capture.right_values[1] == 101 &&
               capture.left_values[0] == hash_left[0] &&
               capture.left_values[1] == hash_left[1] &&
               capture.right_values[0] == hash_right[0] &&
               capture.right_values[1] == hash_right[1] &&
               mock.statement_failed == 0);

        const uint8_t null_state[] = {PRDB_SDK_VALUE_STATE_NULL};
        mock.batch_len[0] = make_batch(mock.batch[0], outer_key, null_state,
                                       outer_value, 1);
        install_optimizer_index_join_plan(&mock);
        reset(&mock, &capture);
        assert(run_query_optimizer(&mock, &capture, composite_sql,
                                  sizeof(composite_sql) - 1) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(mock.range_calls == 0 && capture.calls == 0 &&
               mock.statement_failed == 0);
    }

    {
        int32_t outer_keys[17];
        uint8_t outer_states[17];
        int32_t outer_values[17];
        for (uint32_t index = 0; index < 17; ++index) {
            outer_keys[index] = 1;
            outer_states[index] = PRDB_SDK_VALUE_STATE_VALUE;
            outer_values[index] = 101;
        }
        mock.batch_len[0] = make_batch(mock.batch[0], outer_keys, outer_states,
                                       outer_values, 17);
        install_optimizer_index_join_plan(&mock);
        reset(&mock, &capture);
        mock.cancel_after_check = 3;
        assert(run_query_optimizer(&mock, &capture, composite_sql,
                                  sizeof(composite_sql) - 1) ==
               PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
        assert(mock.range_calls == 16 && capture.calls == 16 &&
               mock.statement_failed == 1);
    }

    {
        const int32_t outer_key[] = {1};
        const uint8_t outer_state[] = {PRDB_SDK_VALUE_STATE_VALUE};
        const int32_t outer_value[] = {101};
        mock.batch_len[0] = make_batch(mock.batch[0], outer_key, outer_state,
                                       outer_value, 1);
        install_optimizer_index_join_plan(&mock);
        reset(&mock, &capture);
        assert(run_query_optimizer_with_materialized_capacity(
                   &mock, &capture, composite_sql,
                   sizeof(composite_sql) - 1, 12) ==
               PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT);
        assert(mock.range_calls == 1 && capture.calls == 0 &&
               mock.statement_failed == 1);
    }

    {
        int32_t outer_keys[17];
        int32_t outer_values[17];
        uint8_t outer_states[17];
        for (uint32_t index = 0; index < 17; ++index) {
            outer_keys[index] = 1;
            outer_values[index] = 101;
            outer_states[index] = PRDB_SDK_VALUE_STATE_VALUE;
        }
        mock.batch_len[0] = make_batch(mock.batch[0], outer_keys,
                                       outer_states, outer_values, 17);
        install_optimizer_index_join_plan(&mock);
        reset(&mock, &capture);
        mock.range_row_count = 1024;
        mock.repeat_range_values = 1;
        assert(run_query_optimizer(&mock, &capture, composite_sql,
                                  sizeof(composite_sql) - 1) ==
               PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT);
        assert(mock.range_calls == 17 && capture.calls == 16384 &&
               mock.statement_failed == 1);
        install_optimizer_join_plan(&mock);
    }

    {
        const int32_t pair_key[] = {1};
        const uint8_t pair_state[] = {PRDB_SDK_VALUE_STATE_VALUE};
        const int32_t pair_value[] = {101};
        mock.batch_len[0] = make_batch(mock.batch[0], pair_key, pair_state,
                                       pair_value, 1);
        mock.batch_len[1] = make_batch(mock.batch[1], pair_key, pair_state,
                                       pair_value, 1);
        reset(&mock, &capture);
        assert(run_query_optimizer(&mock, &capture, composite_sql,
                                   sizeof(composite_sql) - 1) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(mock.optimizer_calls == 1 && mock.planner_calls == 4 &&
               mock.resolve_calls == 2 && mock.open_calls == 2 &&
               capture.calls == 1 && capture.left_values[0] == 101 &&
               capture.right_values[0] == 101 &&
               mock.statement_failed == 0);
        mock.batch_len[0] = make_batch(mock.batch[0], left_keys, left_states,
                                       left_values, 3);
        mock.batch_len[1] = make_batch(mock.batch[1], right_keys, right_states,
                                       right_values, 3);
    }

    reset(&mock, &capture);
    assert(run_query_v19(&mock, &capture, alias_sql, sizeof(alias_sql) - 1,
                         1024) == PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 2 && capture.left_values[0] == 101 &&
           capture.right_values[0] == 201 && capture.left_values[1] == 102 &&
           capture.right_values[1] == 202 && mock.statement_failed == 0);

    reset(&mock, &capture);
    assert(run_query_v20(&mock, &capture, filtered_sql,
                         sizeof(filtered_sql) - 1, 1024, &threshold, 1) ==
           PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 1 && capture.left_values[0] == 102 &&
           capture.right_values[0] == 202 && mock.statement_failed == 0);

    uint32_t filtered_optimizer_plan_len =
        build_optimizer_filtered_join_plan(mock.optimizer_response + 7);
    optimizer_offset = 0;
    put_u16(mock.optimizer_response, &optimizer_offset, 1);
    mock.optimizer_response[optimizer_offset++] =
        PRDB_SDK_OPTIMIZER_STATUS_SUCCESS;
    put_u32(mock.optimizer_response, &optimizer_offset,
            filtered_optimizer_plan_len);
    optimizer_offset += filtered_optimizer_plan_len;
    mock.optimizer_response_len = optimizer_offset;
    reset(&mock, &capture);
    const int32_t optimizer_pair_key[] = {1};
    const uint8_t optimizer_pair_state[] = {PRDB_SDK_VALUE_STATE_VALUE};
    const int32_t optimizer_pair_value[] = {101};
    static const uint8_t optimizer_threshold_bytes[] = {0, 0, 0, 100};
    const prdb_sdk_value_v1 optimizer_threshold = {
        sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
        optimizer_threshold_bytes, sizeof(optimizer_threshold_bytes)};
    mock.batch_len[0] = make_batch(mock.batch[0], optimizer_pair_key,
                                   optimizer_pair_state, optimizer_pair_value,
                                   1);
    mock.batch_len[1] = make_batch(mock.batch[1], optimizer_pair_key,
                                   optimizer_pair_state, optimizer_pair_value,
                                   1);
    assert(run_query_optimizer_v20(
               &mock, &capture, optimizer_filtered_sql,
               sizeof(optimizer_filtered_sql) - 1, &optimizer_threshold, 1) ==
           PRDB_SDK_SQL_EXEC_OK);
    assert(mock.optimizer_calls == 1 && mock.planner_calls == 6 &&
           mock.resolve_calls == 4 && mock.open_calls == 2 &&
           mock.executor_requests <= 256 && capture.calls == 1 &&
           capture.left_values[0] == 101 && capture.right_values[0] == 101 &&
           mock.statement_failed == 0);

    {
        uint32_t index_filtered_len = build_optimizer_filtered_index_join_plan(
            mock.optimizer_response + 7);
        optimizer_offset = 0;
        put_u16(mock.optimizer_response, &optimizer_offset, 1);
        mock.optimizer_response[optimizer_offset++] =
            PRDB_SDK_OPTIMIZER_STATUS_SUCCESS;
        put_u32(mock.optimizer_response, &optimizer_offset, index_filtered_len);
        optimizer_offset += index_filtered_len;
        mock.optimizer_response_len = optimizer_offset;
        mock.batch_len[0] = make_batch(mock.batch[0], optimizer_pair_key,
                                       optimizer_pair_state,
                                       optimizer_pair_value, 1);
        mock.batch_len[1] = make_batch(mock.batch[1], optimizer_pair_key,
                                       optimizer_pair_state,
                                       optimizer_pair_value, 1);
        reset(&mock, &capture);
        int32_t filtered_index_status = run_query_optimizer_v20(
            &mock, &capture, optimizer_filtered_sql,
            sizeof(optimizer_filtered_sql) - 1, &optimizer_threshold, 1);
        assert(filtered_index_status == PRDB_SDK_SQL_EXEC_OK);
        assert(mock.range_calls == 1 && mock.open_calls == 1 &&
               capture.calls == 1 && capture.left_values[0] == 101 &&
               capture.right_values[0] == 101 && mock.statement_failed == 0);
        uint32_t restored_filtered_len = build_optimizer_filtered_join_plan(
            mock.optimizer_response + 7);
        optimizer_offset = 0;
        put_u16(mock.optimizer_response, &optimizer_offset, 1);
        mock.optimizer_response[optimizer_offset++] =
            PRDB_SDK_OPTIMIZER_STATUS_SUCCESS;
        put_u32(mock.optimizer_response, &optimizer_offset,
                restored_filtered_len);
        optimizer_offset += restored_filtered_len;
        mock.optimizer_response_len = optimizer_offset;
        mock.batch_len[0] = make_batch(mock.batch[0], left_keys, left_states,
                                       left_values, 3);
        mock.batch_len[1] = make_batch(mock.batch[1], right_keys, right_states,
                                       right_values, 3);
    }
    mock.batch_len[0] = make_batch(mock.batch[0], left_keys, left_states,
                                   left_values, 3);
    mock.batch_len[1] = make_batch(mock.batch[1], right_keys, right_states,
                                   right_values, 3);
    uint32_t restored_optimizer_plan_len =
        build_optimizer_join_plan(mock.optimizer_response + 7);
    optimizer_offset = 0;
    put_u16(mock.optimizer_response, &optimizer_offset, 1);
    mock.optimizer_response[optimizer_offset++] =
        PRDB_SDK_OPTIMIZER_STATUS_SUCCESS;
    put_u32(mock.optimizer_response, &optimizer_offset,
            restored_optimizer_plan_len);
    optimizer_offset += restored_optimizer_plan_len;
    mock.optimizer_response_len = optimizer_offset;

    mock.batch_len[0] = make_batch(mock.batch[0], matching_key,
                                   matching_state, matching_left_value, 1);
    mock.batch_len[1] = make_batch_with_value_state(
        mock.batch[1], matching_key, matching_state, matching_right_value,
        PRDB_SDK_VALUE_STATE_REDACTED, 1);
    reset(&mock, &capture);
    assert(run_query_v20(&mock, &capture, redacted_filter_sql,
                         sizeof(redacted_filter_sql) - 1, 1024, &threshold,
                         1) == PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
    assert(capture.calls == 0 && mock.statement_failed == 1);

    {
        const uint8_t value_state[] = {PRDB_SDK_VALUE_STATE_VALUE};
        const uint8_t null_value_state[] = {PRDB_SDK_VALUE_STATE_NULL};
        const int32_t left_value[] = {102};
        const int32_t right_value[] = {202};
        mock.batch_len[0] = make_batch_with_column_states(
            mock.batch[0], matching_key, matching_state, left_value,
            value_state, 1);
        mock.batch_len[1] = make_batch_with_column_states(
            mock.batch[1], matching_key, matching_state, right_value,
            null_value_state, 1);
        reset(&mock, &capture);
        assert(run_query_v20(&mock, &capture, filtered_sql,
                             sizeof(filtered_sql) - 1, 1024, &threshold, 1) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 0 && mock.open_calls == 2 &&
               mock.scan_finished[0] == 1 && mock.scan_finished[1] == 1 &&
               mock.statement_failed == 0);
    }
    mock.batch_len[0] = make_batch(mock.batch[0], left_keys, left_states,
                                   left_values, 3);
    mock.batch_len[1] = make_batch(mock.batch[1], right_keys, right_states,
                                   right_values, 3);

    {
        const int32_t self_keys[] = {1, 2, 3};
        const uint8_t self_states[] = {
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_STATE_VALUE,
            PRDB_SDK_VALUE_STATE_NULL};
        const int32_t self_values[] = {101, 102, 103};
        mock.batch_len[0] = make_batch(mock.batch[0], self_keys, self_states,
                                       self_values, 3);
        mock.batch_len[1] = make_batch(mock.batch[1], self_keys, self_states,
                                       self_values, 3);
        reset(&mock, &capture);
        assert(run_query_v19(&mock, &capture, self_join_sql,
                             sizeof(self_join_sql) - 1, 1024) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(mock.planner_calls == 2 && mock.resolve_calls == 2 &&
               mock.open_calls == 2 && mock.next_calls[0] == 1 &&
               mock.next_calls[1] == 1 && mock.statement_failed == 0);
        assert(capture.calls == 2 && capture.left_values[0] == 101 &&
               capture.right_values[0] == 101 &&
               capture.left_values[1] == 102 &&
               capture.right_values[1] == 102);
        mock.batch_len[0] = make_batch(mock.batch[0], left_keys, left_states,
                                       left_values, 3);
        mock.batch_len[1] = make_batch(mock.batch[1], right_keys, right_states,
                                       right_values, 3);
    }

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
        assert(run_query_v19(&mock, &capture, composite_sql,
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
        assert(run_query_v19(&mock, &capture, composite_sql,
                             sizeof(composite_sql) - 1, 1024) ==
               PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
        assert(capture.calls == 0 && mock.statement_failed == 1 &&
               mock.check_cancel_calls == 3);

        reset(&mock, &capture);
        mock.empty_pages[0] = 121;
        assert(run_query_v19(&mock, &capture, composite_sql,
                             sizeof(composite_sql) - 1, 1024) ==
               PRDB_SDK_SQL_EXEC_REQUEST_LIMIT);
        assert(mock.next_calls[0] == 121 && mock.next_calls[1] == 0 &&
               mock.close_calls == 1 && mock.executor_requests == 246 &&
               capture.calls == 0 && mock.statement_failed == 1);

        reset(&mock, &capture);
        assert(run_query_v19(&mock, &capture, composite_max_keys_sql,
                             sizeof(composite_max_keys_sql) - 1, 1024) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && capture.left_values[0] == 10 &&
               capture.right_values[0] == 10 && capture.left_values[1] == 20 &&
               capture.right_values[1] == 20);

        reset(&mock, &capture);
        assert(run_query_v19(&mock, &capture, composite_limit_sql,
                             sizeof(composite_limit_sql) - 1, 1024) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 1 && capture.left_values[0] == 10 &&
               capture.right_values[0] == 10);

        {
            int32_t boundary_keys[JOIN_V16_ROW_LIMIT];
            int32_t boundary_values[JOIN_V16_ROW_LIMIT];
            uint8_t boundary_states[JOIN_V16_ROW_LIMIT];
            for (uint32_t row = 0; row < JOIN_V16_ROW_LIMIT; ++row) {
                boundary_keys[row] = (int32_t)row;
                boundary_values[row] = (int32_t)(row * 2);
                boundary_states[row] = PRDB_SDK_VALUE_STATE_VALUE;
            }
            mock.batch_len[0] = make_batch(
                mock.batch[0], boundary_keys, boundary_states,
                boundary_values, JOIN_V16_ROW_LIMIT);
            mock.batch_len[1] = make_batch(
                mock.batch[1], boundary_keys, boundary_states,
                boundary_values, JOIN_V16_ROW_LIMIT);
            reset(&mock, &capture);
            assert(run_query_v19(&mock, &capture, composite_sql,
                                 sizeof(composite_sql) - 1, 4096) ==
                   PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == JOIN_V16_ROW_LIMIT &&
                   mock.executor_requests == 9 &&
                   mock.statement_failed == 0);

            {
                int32_t overflow_keys[JOIN_V16_ROW_LIMIT + 1];
                int32_t overflow_values[JOIN_V16_ROW_LIMIT + 1];
                uint8_t overflow_states[JOIN_V16_ROW_LIMIT + 1];
                for (uint32_t row = 0; row <= JOIN_V16_ROW_LIMIT; ++row) {
                    overflow_keys[row] = (int32_t)row;
                    overflow_values[row] = (int32_t)(row * 2);
                    overflow_states[row] = PRDB_SDK_VALUE_STATE_VALUE;
                }
                mock.batch_len[0] = make_batch(
                    mock.batch[0], overflow_keys, overflow_states,
                    overflow_values, JOIN_V16_ROW_LIMIT + 1);
                mock.batch_len[1] = make_batch(
                    mock.batch[1], boundary_keys, boundary_states,
                    boundary_values, JOIN_V16_ROW_LIMIT);
                reset(&mock, &capture);
                assert(run_query_v19(&mock, &capture, composite_sql,
                                     sizeof(composite_sql) - 1, 4096) ==
                       PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
                assert(capture.calls == 0 && mock.open_calls == 1 &&
                       mock.next_calls[0] == 1 && mock.next_calls[1] == 0 &&
                       mock.statement_failed == 1);
            }

            {
                int32_t first_keys[JOIN_V16_ROW_LIMIT];
                int32_t first_values[JOIN_V16_ROW_LIMIT];
                uint8_t first_states[JOIN_V16_ROW_LIMIT];
                int32_t final_key = 99, final_value = 900;
                uint8_t final_state = PRDB_SDK_VALUE_STATE_VALUE;
                int32_t right_key = 99, right_value = 900;
                uint8_t right_state = PRDB_SDK_VALUE_STATE_VALUE;
                for (uint32_t row = 0; row < JOIN_V16_ROW_LIMIT; ++row) {
                    first_keys[row] = 99;
                    first_values[row] = 900;
                    first_states[row] = PRDB_SDK_VALUE_STATE_VALUE;
                }
                mock.paged_batch_len[0][0] = make_continuation_batch(
                    mock.paged_batch[0][0],
                    make_batch(mock.paged_batch[0][0], first_keys,
                               first_states, first_values,
                               JOIN_V16_ROW_LIMIT));
                mock.paged_batch_len[0][1] = make_batch(
                    mock.paged_batch[0][1], &final_key, &final_state,
                    &final_value, 1);
                mock.paged_batch_count[0] = 2;
                mock.batch_len[1] = make_batch(
                    mock.batch[1], &right_key, &right_state, &right_value, 1);
                reset(&mock, &capture);
                mock.paged_batch_count[0] = 2;
                int32_t spill_status = run_query_spill_v21(
                    &mock, &capture, composite_sql,
                    sizeof(composite_sql) - 1, NULL, 0);
                assert(spill_status == PRDB_SDK_SQL_EXEC_OK);
                assert(capture.calls == JOIN_V16_ROW_LIMIT + 1 &&
                       mock.next_calls[0] == 2 && mock.next_calls[1] == 1 &&
                       mock.statement_failed == 0);
                for (uint32_t stream = 0; stream < 16; ++stream)
                    assert(mock.spill_streams[stream].live == 0);

                reset(&mock, &capture);
                mock.paged_batch_count[0] = 2;
                mock.spill_fail_append_at = 1;
                assert(run_query_spill_v21(
                           &mock, &capture, composite_sql,
                           sizeof(composite_sql) - 1, NULL, 0) ==
                       PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
                assert(capture.calls == 0 && mock.statement_failed == 1);
                for (uint32_t stream = 0; stream < 16; ++stream)
                    assert(mock.spill_streams[stream].live == 0);

                reset(&mock, &capture);
                mock.paged_batch_count[0] = 2;
                {
                    int32_t right_keys[JOIN_V16_ROW_LIMIT];
                    int32_t right_values[JOIN_V16_ROW_LIMIT];
                    uint8_t right_states[JOIN_V16_ROW_LIMIT];
                    for (uint32_t row = 0; row < JOIN_V16_ROW_LIMIT; ++row) {
                        right_keys[row] = 99;
                        right_values[row] = 900;
                        right_states[row] = PRDB_SDK_VALUE_STATE_VALUE;
                    }
                    mock.batch_len[1] = make_batch(
                        mock.batch[1], right_keys, right_states, right_values,
                        JOIN_V16_ROW_LIMIT);
                }
                mock.cancel_after_check = 9;
                int32_t cancel_status = run_query_spill_v21(
                    &mock, &capture, composite_sql,
                    sizeof(composite_sql) - 1, NULL, 0);
                assert(cancel_status == PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
                assert(capture.calls != 0 &&
                       capture.calls < (JOIN_V16_ROW_LIMIT + 1) *
                                           JOIN_V16_ROW_LIMIT &&
                       mock.statement_failed == 1);
                for (uint32_t stream = 0; stream < 16; ++stream)
                    assert(mock.spill_streams[stream].live == 0);

                {
                    int32_t right_key = 99, right_value = 900;
                    uint8_t right_state = PRDB_SDK_VALUE_STATE_VALUE;
                    mock.batch_len[1] = make_batch(
                        mock.batch[1], &right_key, &right_state, &right_value,
                        1);
                    reset(&mock, &capture);
                    mock.paged_batch_count[0] = 2;
                    assert(run_statement_spill_v21(
                               &mock, composite_sql,
                               sizeof(composite_sql) - 1) ==
                           PRDB_SDK_SQL_DISPATCH_OK);
                    assert(mock.result_write_calls == JOIN_V16_ROW_LIMIT + 1 &&
                           mock.statement_failed == 0);
                    for (uint32_t stream = 0; stream < 16; ++stream)
                        assert(mock.spill_streams[stream].live == 0);

                    reset(&mock, &capture);
                    mock.paged_batch_count[0] = 2;
                    assert(run_statement_spill_v21(
                               &mock, sql, sizeof(sql) - 1) ==
                           PRDB_SDK_SQL_DISPATCH_OK);
                    assert(mock.result_write_calls == JOIN_V16_ROW_LIMIT + 1 &&
                           mock.statement_failed == 0);
                    for (uint32_t stream = 0; stream < 16; ++stream)
                        assert(mock.spill_streams[stream].live == 0);
                }
            }
        }

        {
            int32_t stress_keys[JOIN_V16_STRESS_ROWS];
            int32_t stress_values[JOIN_V16_STRESS_ROWS];
            uint8_t stress_states[JOIN_V16_STRESS_ROWS];
            for (uint32_t row = 0; row < JOIN_V16_STRESS_ROWS; ++row) {
                stress_keys[row] = 1;
                stress_values[row] = 77;
                stress_states[row] = PRDB_SDK_VALUE_STATE_VALUE;
            }
            mock.batch_len[0] = make_batch(
                mock.batch[0], stress_keys, stress_states, stress_values,
                JOIN_V16_STRESS_ROWS);
            mock.batch_len[1] = make_batch(
                mock.batch[1], stress_keys, stress_states, stress_values,
                JOIN_V16_STRESS_ROWS);
            reset(&mock, &capture);
            mock.cancel_after_check = 4;
            assert(run_query_v19(&mock, &capture, composite_sql,
                                 sizeof(composite_sql) - 1, 2048) ==
                   PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
            assert(capture.calls == 2048 &&
                   mock.check_cancel_calls == 4 &&
                   mock.executor_requests == 10 &&
                   mock.statement_failed == 1);
        }
        mock.batch_len[0] = make_batch(mock.batch[0], left_keys, left_states,
                                       composite_left_values, 3);
        mock.batch_len[1] = make_batch(mock.batch[1], right_keys, right_states,
                                       composite_right_values, 3);

        reset(&mock, &capture);
        assert(run_query_v19(&mock, &capture, composite_sql,
                             sizeof(composite_sql) - 1, 4) ==
               PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT);
        assert(capture.calls == 0 && mock.statement_failed == 1);

        reset(&mock, &capture);
        capture.callback_status = PRDB_QUERY_CALLBACK_INTERNAL_ERROR;
        assert(run_query_v19(&mock, &capture, composite_sql,
                             sizeof(composite_sql) - 1, 1024) ==
               PRDB_SDK_SQL_EXEC_RESULT_ERROR);
        assert(capture.calls == 1 && mock.statement_failed == 1);
    }
    return 0;
}
