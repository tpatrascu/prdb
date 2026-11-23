#include "prdb_sdk_sql_spill_join_v1.h"

#include <assert.h>
#include <string.h>

typedef struct row { uint32_t key; uint32_t ordinal; } row;
typedef struct row_source {
    row *rows;
    uint32_t count;
    uint32_t next;
} row_source;
typedef struct mock_stream {
    uint8_t handle[16];
    uint8_t bytes[8192];
    uint32_t length;
    uint8_t live;
    uint8_t sealed;
} mock_stream;
typedef struct fixture {
    row left[257];
    row right[257];
    uint32_t left_count;
    uint32_t right_count;
    uint64_t next_stream_id;
    uint32_t append_calls;
    uint32_t fail_append_at;
    uint32_t cancel_calls;
    uint32_t cancel_at;
    uint64_t output_count;
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
        if (++state->append_calls == state->fail_append_at || stream->sealed ||
            count > sizeof(stream->bytes) - stream->length)
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

static int32_t source_row(void *context, uint8_t *record, uint32_t capacity,
                          uint8_t *has_record_out) {
    row_source *source = (row_source *)context;
    assert(capacity == sizeof(row));
    if (source->next == source->count) {
        *has_record_out = 0;
        return 0;
    }
    memcpy(record, &source->rows[source->next++], sizeof(row));
    *has_record_out = 1;
    return 0;
}

static int32_t hash_row(void *context, const uint8_t *record,
                        uint32_t record_bytes, uint64_t *hash_out) {
    row value;
    (void)context;
    assert(record_bytes == sizeof(value));
    memcpy(&value, record, sizeof(value));
    *hash_out = value.key;
    return 0;
}

static int32_t equal_rows(void *context, const uint8_t *left_bytes,
                          const uint8_t *right_bytes, uint32_t record_bytes,
                          uint8_t *equal_out) {
    row left, right;
    (void)context;
    assert(record_bytes == sizeof(left));
    memcpy(&left, left_bytes, sizeof(left));
    memcpy(&right, right_bytes, sizeof(right));
    *equal_out = left.key == right.key;
    return 0;
}

static int32_t output_row(void *context, const uint8_t *left_bytes,
                          const uint8_t *right_bytes, uint32_t record_bytes) {
    fixture *state = (fixture *)context;
    row left, right;
    assert(record_bytes == sizeof(left));
    memcpy(&left, left_bytes, sizeof(left));
    memcpy(&right, right_bytes, sizeof(right));
    assert(left.key == right.key);
    ++state->output_count;
    return 0;
}

static int32_t check_cancelled(void *context) {
    fixture *state = (fixture *)context;
    ++state->cancel_calls;
    return state->cancel_at != 0 && state->cancel_calls >= state->cancel_at;
}

static void assert_no_live_streams(const fixture *state) {
    for (unsigned index = 0; index < 16; ++index)
        assert(state->streams[index].live == 0);
}

int main(void) {
    prdb_host_api_v6 host = {0};
    fixture state = {0};
    row_source left_source, right_source;
    host.v5.v4.v3.context = &state;
    host.spill_request = mock_spill;

    /* A single skewed partition crosses the 128-row block boundary and
     * produces the independently calculated 257 x 129 matches. */
    state.left_count = 257;
    state.right_count = 130;
    for (uint32_t index = 0; index < state.left_count; ++index)
        state.left[index] = (row){7, index};
    for (uint32_t index = 0; index < state.right_count; ++index)
        state.right[index] = (row){index == 129 ? 15u : 7u, index};
    left_source = (row_source){state.left, state.left_count, 0};
    right_source = (row_source){state.right, state.right_count, 0};
    assert(prdb_sdk_sql_spill_hash_join_v1(
               &host, sizeof(row), source_row, &left_source, source_row,
               &right_source, hash_row, NULL,
               equal_rows, NULL, output_row, &state, check_cancelled,
               &state) ==
           PRDB_SDK_SQL_SPILL_JOIN_OK);
    assert(state.output_count == UINT64_C(257) * 129);
    assert_no_live_streams(&state);

    /* Broker write failure aborts before results and still reclaims streams. */
    state.output_count = 0;
    state.append_calls = 0;
    state.cancel_calls = 0;
    state.fail_append_at = 3;
    left_source.next = right_source.next = 0;
    assert(prdb_sdk_sql_spill_hash_join_v1(
               &host, sizeof(row), source_row, &left_source, source_row,
               &right_source, hash_row, NULL,
               equal_rows, NULL, output_row, &state, check_cancelled,
               &state) ==
           PRDB_SDK_SQL_SPILL_JOIN_BROKER_ERROR);
    assert(state.output_count == 0);
    assert_no_live_streams(&state);

    state.fail_append_at = 0;
    state.cancel_calls = 0;
    state.cancel_at = 2;
    left_source.next = right_source.next = 0;
    assert(prdb_sdk_sql_spill_hash_join_v1(
               &host, sizeof(row), source_row, &left_source, source_row,
               &right_source, hash_row, NULL,
               equal_rows, NULL, output_row, &state, check_cancelled,
               &state) == PRDB_SDK_SQL_SPILL_JOIN_CANCELLED);
    assert(state.output_count != 0 &&
           state.output_count < UINT64_C(257) * 129);
    assert_no_live_streams(&state);

    /* Bound records and callback inputs before any stream is opened. */
    assert(prdb_sdk_sql_spill_hash_join_v1(
               &host, PRDB_SDK_SQL_SPILL_JOIN_MAX_RECORD_BYTES + 1,
               source_row, &left_source, source_row, &right_source,
               hash_row, NULL, equal_rows, NULL, output_row, &state,
               check_cancelled, &state) ==
           PRDB_SDK_SQL_SPILL_JOIN_INVALID_ARGUMENT);
    return 0;
}
