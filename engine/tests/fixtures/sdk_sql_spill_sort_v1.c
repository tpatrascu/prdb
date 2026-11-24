#include "prdb_sdk_sql_spill_sort_v1.h"
#include "prdb_sdk_sql_spill_group_v1.h"
#include "../../sdk/sql_spill_workspace_internal.h"

#include <assert.h>
#include <string.h>

typedef struct row { uint32_t key; uint32_t ordinal; } row;
typedef struct mock_stream {
    uint8_t handle[16];
    uint8_t bytes[8192];
    uint32_t length;
    uint8_t live;
    uint8_t sealed;
} mock_stream;
typedef struct fixture {
    uint32_t next;
    uint32_t outputs;
    uint32_t previous_key;
    uint32_t previous_ordinal;
    uint32_t groups;
    uint32_t previous_group_key;
    uint32_t fail_source_at;
    uint32_t fail_group_output_at;
    uint64_t next_stream_id;
    mock_stream streams[16];
} fixture;

static uint64_t read_u64(const uint8_t *bytes) {
    uint64_t value = 0;
    for (unsigned index = 0; index < 8; ++index)
        value = (value << 8) | bytes[index];
    return value;
}

static uint32_t read_u32(const uint8_t *bytes) {
    return ((uint32_t)bytes[0] << 24) | ((uint32_t)bytes[1] << 16) |
           ((uint32_t)bytes[2] << 8) | bytes[3];
}

static void write_u64(uint8_t *bytes, uint64_t value) {
    for (unsigned index = 0; index < 8; ++index)
        bytes[index] = (uint8_t)(value >> (56 - index * 8));
}

static int32_t mock_spill(void *context, uint16_t operation,
                            const uint8_t *request, uint64_t request_len,
                            uint8_t *response, uint64_t response_capacity,
                            uint64_t *response_len) {
    fixture *state = (fixture *)context;
    mock_stream *stream = NULL;
    *response_len = 0;
    if (operation == PRDB_SPILL_OP_CREATE) {
        assert(request_len == 0 && response_capacity == 16);
        for (unsigned index = 0; index < 16; ++index) {
            if (state->streams[index].live) continue;
            stream = &state->streams[index];
            memset(stream, 0, sizeof(*stream));
            stream->live = 1;
            write_u64(stream->handle, ++state->next_stream_id);
            memcpy(response, stream->handle, 16);
            *response_len = 16;
            return 0;
        }
        return -1;
    }
    assert(request_len >= 16);
    for (unsigned index = 0; index < 16; ++index)
        if (state->streams[index].live &&
            memcmp(state->streams[index].handle, request, 16) == 0)
            stream = &state->streams[index];
    if (stream == NULL) return -1;
    if (operation == PRDB_SPILL_OP_APPEND) {
        uint64_t count = request_len - 16;
        if (stream->sealed || count > sizeof(stream->bytes) - stream->length)
            return -1;
        memcpy(stream->bytes + stream->length, request + 16, (size_t)count);
        stream->length += (uint32_t)count;
    } else if (operation == PRDB_SPILL_OP_SEAL) {
        assert(request_len == 16);
        stream->sealed = 1;
    } else if (operation == PRDB_SPILL_OP_READ_AT) {
        assert(request_len == 28 && stream->sealed);
        uint64_t offset = read_u64(request + 16);
        uint32_t capacity = read_u32(request + 24);
        if (offset > stream->length || capacity > response_capacity) return -1;
        uint32_t count = stream->length - (uint32_t)offset;
        if (count > capacity) count = capacity;
        memcpy(response, stream->bytes + offset, count);
        *response_len = count;
    } else if (operation == PRDB_SPILL_OP_DESTROY) {
        assert(request_len == 16);
        memset(stream, 0, sizeof(*stream));
    } else {
        return -1;
    }
    return 0;
}

static int32_t source_rows(void *context, uint8_t *bytes, uint32_t capacity,
                           uint32_t *row_len, uint8_t *has_row) {
    fixture *state = (fixture *)context;
    row value;
    if (capacity < sizeof(value)) return -1;
    if (state->fail_source_at != 0 && state->next == state->fail_source_at)
        return -1;
    if (state->next == 257) { *has_row = 0; *row_len = 0; return 0; }
    value.key = (state->next * 37) % 19;
    value.ordinal = state->next++;
    memcpy(bytes, &value, sizeof(value));
    *row_len = sizeof(value);
    *has_row = 1;
    return 0;
}

static int32_t compare_rows(void *context, const uint8_t *left_bytes,
                            const uint8_t *right_bytes, int32_t *ordering) {
    row left, right;
    (void)context;
    memcpy(&left, left_bytes, sizeof(left));
    memcpy(&right, right_bytes, sizeof(right));
    *ordering = (left.key > right.key) - (left.key < right.key);
    return 0;
}

static int32_t output_rows(void *context, const uint8_t *bytes,
                           uint32_t row_len) {
    fixture *state = (fixture *)context;
    row value;
    assert(row_len == sizeof(value));
    memcpy(&value, bytes, sizeof(value));
    if (state->outputs != 0) {
        assert(state->previous_key <= value.key);
        if (state->previous_key == value.key)
            assert(state->previous_ordinal < value.ordinal);
    }
    state->previous_key = value.key;
    state->previous_ordinal = value.ordinal;
    state->outputs++;
    return 0;
}

static int32_t output_groups(void *context, const uint8_t *bytes,
                             uint32_t row_len, uint64_t count) {
    fixture *state = (fixture *)context;
    row value;
    if (state->fail_group_output_at != 0 &&
        state->groups == state->fail_group_output_at)
        return -1;
    assert(row_len == sizeof(value));
    memcpy(&value, bytes, sizeof(value));
    assert(value.key == state->groups);
    assert(count == (value.key == 0 || value.key >= 10 ? 14 : 13));
    state->previous_group_key = value.key;
    state->groups++;
    return 0;
}

int main(void) {
    prdb_host_api_v6 host = {0};
    fixture state = {0};
    prdb_sql_spill_heap_budget exhausted_budget = {
        PRDB_SDK_SQL_SPILL_SORT_MAX_WORKSPACE_BYTES};
    host.v5.v4.v3.context = &state;
    host.spill_request = mock_spill;
    assert(prdb_sdk_sql_spill_sort_with_budget_internal_v1(
               &host, &state, sizeof(row), 8 * (sizeof(row) + 8),
               source_rows, compare_rows, output_rows,
               &exhausted_budget) != 0);
    assert(state.next == 0 && state.outputs == 0);
    assert(prdb_sdk_sql_spill_sort_v1(
               &host, &state, sizeof(row), 8 * (sizeof(row) + 8),
               source_rows, compare_rows, output_rows) == 0);
    assert(state.outputs == 257);
    memset(&state, 0, sizeof(state));
    host.v5.v4.v3.context = &state;
    assert(prdb_sdk_sql_spill_group_count_v1(
               &host, &state, sizeof(row), 8 * (sizeof(row) + 8),
               source_rows, compare_rows, output_groups) == 0);
    assert(state.groups == 19 && state.previous_group_key == 18);
    memset(&state, 0, sizeof(state));
    host.v5.v4.v3.context = &state;
    state.fail_group_output_at = 3;
    assert(prdb_sdk_sql_spill_group_count_v1(
               &host, &state, sizeof(row), 8 * (sizeof(row) + 8),
               source_rows, compare_rows, output_groups) != 0);
    for (unsigned index = 0; index < 16; ++index)
        assert(state.streams[index].live == 0);
    memset(&state, 0, sizeof(state));
    host.v5.v4.v3.context = &state;
    state.fail_source_at = 31;
    assert(prdb_sdk_sql_spill_group_count_v1(
               &host, &state, sizeof(row), 8 * (sizeof(row) + 8),
               source_rows, compare_rows, output_groups) != 0);
    for (unsigned index = 0; index < 16; ++index)
        assert(state.streams[index].live == 0);
    return 0;
}
