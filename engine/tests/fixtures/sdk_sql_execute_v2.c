#include "prdb_sdk_sql_exec_v1.h"
#include "prdb_sdk_sql_dispatch_v1.h"
#include "prdb_sdk_sql_result_v1.h"

#include <assert.h>
#include <stdlib.h>
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
    uint32_t check_column_list_insert;
    uint32_t column_list_insert_calls;
    uint32_t accept_multi_insert;
    uint32_t multi_insert_calls;
    uint32_t fail_multi_insert_at;
    uint32_t cancellation_checks;
    uint32_t cancel_after_checks;
    uint32_t cancel_after_group_callback;
    uint32_t stream_pages;
    uint32_t stream_next_calls;
    uint32_t executor_requests;
    uint16_t expected_max_candidates;
    uint32_t expected_batch_bytes;
    uint32_t stage_update_calls;
    uint32_t stage_delete_calls;
    uint32_t stage_handles_repeated;
    uint32_t require_finished_scan_before_stage;
    uint32_t scan_finished;
    struct {
        uint8_t handle[16];
        uint8_t *bytes;
        uint32_t capacity;
        uint32_t len;
        uint8_t live;
        uint8_t sealed;
    } spill[16];
    uint32_t spill_next;
    uint8_t result_bytes[32768];
    uint32_t result_len;
    uint32_t result_writes;
    uint32_t spill_append_calls;
    uint32_t fail_spill_append_at;
} mock_host;

static uint32_t get_u32_be(const uint8_t *bytes) {
    return ((uint32_t)bytes[0] << 24) | ((uint32_t)bytes[1] << 16) |
           ((uint32_t)bytes[2] << 8) | bytes[3];
}

static uint64_t get_u64_be(const uint8_t *bytes) {
    uint64_t value = 0;
    for (uint32_t index = 0; index < 8; ++index)
        value = (value << 8) | bytes[index];
    return value;
}

static int32_t write_result(void *context, const uint8_t *bytes,
                            uint64_t bytes_len) {
    mock_host *mock = (mock_host *)context;
    if (bytes_len > sizeof(mock->result_bytes) - mock->result_len)
        return PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL;
    memcpy(mock->result_bytes + mock->result_len, bytes, (size_t)bytes_len);
    mock->result_len += (uint32_t)bytes_len;
    ++mock->result_writes;
    return PRDB_QUERY_CALLBACK_OK;
}

static int32_t spill_request(void *context, uint16_t operation,
                             const uint8_t *request, uint64_t request_len,
                             uint8_t *response, uint64_t response_capacity,
                             uint64_t *response_len) {
    mock_host *mock = (mock_host *)context;
    uint32_t slot = 16;
    *response_len = 0;
    if (operation == PRDB_SPILL_OP_CREATE) {
        if (request_len != 0 || response_capacity < 16) return -1;
        for (slot = 0; slot < 16 && mock->spill[slot].live; ++slot) {}
        if (slot == 16) return -1;
        memset(&mock->spill[slot], 0, sizeof(mock->spill[slot]));
        mock->spill[slot].capacity = 1048576;
        mock->spill[slot].bytes = (uint8_t *)malloc(
            mock->spill[slot].capacity);
        if (mock->spill[slot].bytes == NULL) return -1;
        mock->spill[slot].live = 1;
        ++mock->spill_next;
        for (uint32_t index = 0; index < 16; ++index)
            mock->spill[slot].handle[index] =
                (uint8_t)(mock->spill_next >> ((index % 4) * 8));
        memcpy(response, mock->spill[slot].handle, 16);
        *response_len = 16;
        return 0;
    }
    if (request == NULL || request_len < 16) return -1;
    for (slot = 0; slot < 16; ++slot)
        if (mock->spill[slot].live &&
            memcmp(mock->spill[slot].handle, request, 16) == 0) break;
    if (slot == 16) return -1;
    if (operation == PRDB_SPILL_OP_APPEND) {
        ++mock->spill_append_calls;
        if (mock->fail_spill_append_at != 0 &&
            mock->spill_append_calls == mock->fail_spill_append_at)
            return PRDB_QUERY_CALLBACK_IO_ERROR;
        uint64_t len = request_len - 16;
        if (mock->spill[slot].sealed ||
            len > mock->spill[slot].capacity - mock->spill[slot].len)
            return -1;
        memcpy(mock->spill[slot].bytes + mock->spill[slot].len,
               request + 16, (size_t)len);
        mock->spill[slot].len += (uint32_t)len;
    } else if (operation == PRDB_SPILL_OP_SEAL) {
        if (request_len != 16) return -1;
        mock->spill[slot].sealed = 1;
    } else if (operation == PRDB_SPILL_OP_READ_AT) {
        if (request_len != 28 || !mock->spill[slot].sealed) return -1;
        uint64_t offset = get_u64_be(request + 16);
        uint32_t capacity = get_u32_be(request + 24);
        if (offset > mock->spill[slot].len || capacity > response_capacity)
            return -1;
        uint32_t len = mock->spill[slot].len - (uint32_t)offset;
        if (len > capacity) len = capacity;
        memcpy(response, mock->spill[slot].bytes + offset, len);
        *response_len = len;
    } else if (operation == PRDB_SPILL_OP_DESTROY) {
        if (request_len != 16) return -1;
        free(mock->spill[slot].bytes);
        memset(&mock->spill[slot], 0, sizeof(mock->spill[slot]));
    } else {
        return -1;
    }
    return 0;
}

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
    ++mock->executor_requests;
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
        static const uint8_t expected_three_candidates[24] = {
            0, 1, 7, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
            0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
            0, 3, 0, 0, 4, 0};
        static const uint8_t expected_max_candidates_request[24] = {
            0, 1, 7, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
            0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
            0, 128, 0, 0, 16, 0};
        static const uint8_t expected_max_candidates_1mib[24] = {
            0, 1, 7, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
            0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
            0, 128, 0, 16, 0, 0};
        static const uint8_t expected_many_candidates[24] = {
            0, 1, 7, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
            0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
            0, 40, 0, 0, 16, 0};
        static const uint8_t continuation_request[24] = {
            0, 1, 7, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
            0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
            0, 1, 0, 0, 4, 0};
        static const uint8_t continuation_two_candidates[24] = {
            0, 1, 7, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
            0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
            0, 2, 0, 0, 4, 0};
        static const uint8_t continuation_three_candidates[24] = {
            0, 1, 7, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
            0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
            0, 3, 0, 0, 4, 0};
        static const uint8_t continuation_max_candidates_request[24] = {
            0, 1, 7, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
            0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
            0, 128, 0, 0, 16, 0};
        static const uint8_t continuation_max_candidates_1mib[24] = {
            0, 1, 7, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
            0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
            0, 128, 0, 16, 0, 0};
        static const uint8_t continuation_many_candidates[24] = {
            0, 1, 7, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
            0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
            0, 40, 0, 0, 16, 0};
        static const uint8_t finished_reply[5] = {0, 1, 1, 0, 0};
        assert(request_len == sizeof(expected));
        if (mock->stream_pages != 0) {
            const uint8_t *first_request =
                mock->expected_max_candidates == 128
                    ? (mock->expected_batch_bytes == 1048576
                           ? expected_max_candidates_1mib
                           : expected_max_candidates_request)
                    : mock->expected_max_candidates == 3
                          ? expected_three_candidates
                          : mock->expected_max_candidates == 2
                                ? expected_two_candidates
                                : expected;
            const uint8_t *next_request =
                mock->expected_max_candidates == 128
                    ? (mock->expected_batch_bytes == 1048576
                           ? continuation_max_candidates_1mib
                           : continuation_max_candidates_request)
                    : mock->expected_max_candidates == 3
                          ? continuation_three_candidates
                          : mock->expected_max_candidates == 2
                                ? continuation_two_candidates
                                : continuation_request;
            const uint8_t *expected_request = mock->stream_next_calls == 0
                                                  ? first_request
                                                  : next_request;
            assert(memcmp(request, expected_request, sizeof(expected)) == 0);
            if (mock->stream_next_calls < mock->stream_pages) {
                assert(mock->batch_len <= capacity);
                memcpy(response, mock->batch, mock->batch_len);
                *response_len = mock->batch_len;
            } else {
                assert(sizeof(finished_reply) <= capacity);
                memcpy(response, finished_reply, sizeof(finished_reply));
                *response_len = sizeof(finished_reply);
                mock->scan_finished = 1;
            }
            ++mock->stream_next_calls;
        } else if (mock->executor_calls >= 3) {
            const uint8_t *expected_continuation =
                mock->expected_max_candidates == 3
                    ? continuation_three_candidates
                    : mock->expected_max_candidates == 128
                    ? continuation_max_candidates_request
                    : mock->expected_max_candidates == 40
                    ? continuation_many_candidates
                    : (mock->batch_len > 60 ? continuation_two_candidates
                                            : continuation_request);
            assert(memcmp(request, expected_continuation,
                          sizeof(continuation_request)) == 0);
            assert(sizeof(finished_reply) <= capacity);
            memcpy(response, finished_reply, sizeof(finished_reply));
            *response_len = sizeof(finished_reply);
            mock->scan_finished = 1;
        } else {
            const uint8_t *expected_request =
                mock->expected_max_candidates == 3
                    ? expected_three_candidates
                    : mock->expected_max_candidates == 128
                    ? expected_max_candidates_request
                    : mock->expected_max_candidates == 40
                    ? expected_many_candidates
                    : (mock->batch_len > 60 ? expected_two_candidates
                                            : expected);
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
    } else if (operation == PRDB_SDK_EXECUTOR_OP_STAGE_UPDATES) {
        uint8_t expected_row[] = {
            0, 1, 8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
            0, 1, 0, 0, 0, 0, 0, PRDB_SDK_VALUE_TYPE_INTEGER,
            0, 0, 0, 4, 0, 0, 0, 4};
        static const uint8_t reply[] = {0, 1, 0};
        assert(request_len >= 4 && request[0] == 0 && request[1] == 1);
        uint32_t count = ((uint32_t)request[2] << 8) | request[3];
        uint32_t offset = 4;
        assert(count > 0);
        for (uint32_t row = 0; row < count; ++row) {
            uint32_t length = ((uint32_t)request[offset] << 24) |
                ((uint32_t)request[offset + 1] << 16) |
                ((uint32_t)request[offset + 2] << 8) | request[offset + 3];
            offset += 4;
            assert(length == sizeof(expected_row));
            if (mock->stage_handles_repeated)
                memset(expected_row + 3, (int)(mock->stage_update_calls + 1), 15);
            assert(memcmp(request + offset, expected_row, length) == 0);
            ++mock->stage_update_calls;
            offset += length;
        }
        assert(offset == request_len && sizeof(reply) <= capacity);
        memcpy(response, reply, sizeof(reply));
        *response_len = sizeof(reply);
    } else if (operation == PRDB_SDK_EXECUTOR_OP_STAGE_UPDATE) {
        uint8_t expected[] = {
            0, 1, 8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
            0, 1, 0, 0, 0, 0, 0, PRDB_SDK_VALUE_TYPE_INTEGER,
            0, 0, 0, 4, 0, 0, 0, 4};
        static const uint8_t reply[] = {0, 1, 0};
        assert(!mock->require_finished_scan_before_stage || mock->scan_finished);
        if (mock->stage_handles_repeated) {
            memset(expected + 3, (int)(mock->stage_update_calls + 1), 15);
        }
        assert(request_len == sizeof(expected));
        assert(memcmp(request, expected, sizeof(expected)) == 0);
        assert(sizeof(reply) <= capacity);
        memcpy(response, reply, sizeof(reply));
        *response_len = sizeof(reply);
        ++mock->stage_update_calls;
    } else if (operation == PRDB_SDK_EXECUTOR_OP_STAGE_DELETES) {
        uint8_t expected_row[] = {0, 1, 8, 1, 2, 3, 4, 5, 6,
                                  7, 8, 9, 10, 11, 12, 13, 14, 15};
        static const uint8_t reply[] = {0, 1, 0};
        assert(request_len >= 4 && request[0] == 0 && request[1] == 1);
        uint32_t count = ((uint32_t)request[2] << 8) | request[3];
        uint32_t offset = 4;
        assert(count > 0);
        for (uint32_t row = 0; row < count; ++row) {
            uint32_t length = ((uint32_t)request[offset] << 24) |
                ((uint32_t)request[offset + 1] << 16) |
                ((uint32_t)request[offset + 2] << 8) | request[offset + 3];
            offset += 4;
            assert(length == sizeof(expected_row));
            if (mock->stage_handles_repeated)
                memset(expected_row + 3, (int)(mock->stage_delete_calls + 1), 15);
            assert(memcmp(request + offset, expected_row, length) == 0);
            ++mock->stage_delete_calls;
            offset += length;
        }
        assert(offset == request_len && sizeof(reply) <= capacity);
        memcpy(response, reply, sizeof(reply));
        *response_len = sizeof(reply);
    } else if (operation == PRDB_SDK_EXECUTOR_OP_STAGE_DELETE) {
        uint8_t expected[] = {0, 1, 8, 1, 2, 3, 4, 5, 6,
                              7, 8, 9, 10, 11, 12, 13, 14, 15};
        static const uint8_t reply[] = {0, 1, 0};
        assert(!mock->require_finished_scan_before_stage || mock->scan_finished);
        if (mock->stage_handles_repeated) {
            memset(expected + 3, (int)(mock->stage_delete_calls + 1), 15);
        }
        assert(request_len == sizeof(expected));
        assert(memcmp(request, expected, sizeof(expected)) == 0);
        assert(sizeof(reply) <= capacity);
        memcpy(response, reply, sizeof(reply));
        *response_len = sizeof(reply);
        ++mock->stage_delete_calls;
    } else if (operation == PRDB_SDK_EXECUTOR_OP_STAGE_INSERTS) {
        assert(request_len >= 4 && request[0] == 0 && request[1] == 1);
        uint32_t count = ((uint32_t)request[2] << 8) | request[3];
        uint32_t offset = 4;
        assert(count > 0 && count <= 128);
        if (mock->check_column_list_insert) {
            static const uint8_t expected_handle[18] = {
                0, 1, 6, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22,
                0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22};
            assert(count == 1);
            uint32_t length = ((uint32_t)request[offset] << 24) |
                ((uint32_t)request[offset + 1] << 16) |
                ((uint32_t)request[offset + 2] << 8) | request[offset + 3];
            offset += 4;
            assert(length == 39 && request_len == 47);
            assert(memcmp(request + offset, expected_handle, sizeof(expected_handle)) == 0);
            assert(request[offset + 18] == 0 && request[offset + 19] == 3);
            assert(request[offset + 20] == PRDB_SDK_EXECUTOR_INSERT_VALUE &&
                   request[offset + 21] == PRDB_SDK_VALUE_TYPE_INTEGER &&
                   request[offset + 25] == 4 && request[offset + 29] == 42);
            assert(request[offset + 30] == PRDB_SDK_EXECUTOR_INSERT_DEFAULT);
            assert(request[offset + 31] == PRDB_SDK_EXECUTOR_INSERT_VALUE &&
                   request[offset + 32] == PRDB_SDK_VALUE_TYPE_BYTEA &&
                   request[offset + 36] == 2 && request[offset + 37] == 0xaa &&
                   request[offset + 38] == 0xbb);
            assert(offset + length == request_len && 20 <= capacity);
            response[0] = 0; response[1] = 1; response[3] = 1;
            response[4] = 8; memset(response + 5, 0x66, 15);
            *response_len = 20;
            ++mock->column_list_insert_calls;
            goto stage_inserts_done;
        }
        for (uint32_t row = 0; row < count; ++row) {
            uint32_t length = ((uint32_t)request[offset] << 24) |
                ((uint32_t)request[offset + 1] << 16) |
                ((uint32_t)request[offset + 2] << 8) | request[offset + 3];
            offset += 4;
            assert(length == 30);
            assert(request[offset] == 0 && request[offset + 1] == 1 &&
                   request[offset + 2] == 6);
            assert(request[offset + 18] == 0 && request[offset + 19] == 1);
            uint8_t expected_value = (uint8_t)(42 + mock->multi_insert_calls);
            assert(request[offset + 20] == PRDB_SDK_EXECUTOR_INSERT_VALUE &&
                   request[offset + 21] == PRDB_SDK_VALUE_TYPE_INTEGER &&
                   request[offset + 25] == 4 && request[offset + 29] == expected_value);
            ++mock->multi_insert_calls;
            offset += length;
        }
        assert(offset == request_len);
        if (mock->fail_multi_insert_at != 0 &&
            mock->multi_insert_calls >= mock->fail_multi_insert_at) {
            *response_len = 0;
        } else {
            assert(4 + count * 16 <= capacity);
            response[0] = 0; response[1] = 1;
            response[2] = request[2]; response[3] = request[3];
            for (uint32_t row = 0; row < count; ++row) {
                response[4 + row * 16] = 8;
                memset(response + 5 + row * 16, 0x66, 15);
            }
            *response_len = 4 + count * 16;
        }
stage_inserts_done: ;
    } else if (operation == PRDB_SDK_EXECUTOR_OP_STAGE_INSERT) {
        if (mock->check_column_list_insert) {
            static const uint8_t expected_handle[18] = {
                0, 1, 6, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22,
                0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22};
            static const uint8_t reply[18] = {
                0, 1, 8, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
                0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66};
            uint32_t offset = 20;
            assert(request_len == 39);
            assert(memcmp(request, expected_handle, sizeof(expected_handle)) == 0);
            assert(request[18] == 0 && request[19] == 3);
            assert(request[offset++] == PRDB_SDK_EXECUTOR_INSERT_VALUE);
            assert(request[offset++] == PRDB_SDK_VALUE_TYPE_INTEGER);
            assert(request[offset++] == 0 && request[offset++] == 0 &&
                   request[offset++] == 0 && request[offset++] == 4);
            assert(request[offset++] == 0 && request[offset++] == 0 &&
                   request[offset++] == 0 && request[offset++] == 42);
            assert(request[offset++] == PRDB_SDK_EXECUTOR_INSERT_DEFAULT);
            assert(request[offset++] == PRDB_SDK_EXECUTOR_INSERT_VALUE);
            assert(request[offset++] == PRDB_SDK_VALUE_TYPE_BYTEA);
            assert(request[offset++] == 0 && request[offset++] == 0 &&
                   request[offset++] == 0 && request[offset++] == 2);
            assert(request[offset++] == 0xaa && request[offset++] == 0xbb);
            assert(offset == request_len);
            assert(sizeof(reply) <= capacity);
            memcpy(response, reply, sizeof(reply));
            *response_len = sizeof(reply);
            ++mock->column_list_insert_calls;
        } else if (mock->accept_multi_insert) {
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

static uint32_t make_matching_rows(uint8_t *p, uint16_t row_count) {
    uint32_t offset = 0;
    put_u16(p, &offset, 1);
    p[offset++] = 0;
    p[offset++] = 7;
    memset(p + offset, 0x55, 15);
    offset += 15;
    put_u16(p, &offset, row_count);
    for (uint16_t row = 0; row < row_count; ++row) {
        p[offset++] = 8;
        memset(p + offset, (int)(row + 1), 15);
        offset += 15;
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

static uint32_t make_many_sort_rows(uint8_t *p, uint16_t row_count) {
    uint32_t offset = 0;
    put_u16(p, &offset, 1);
    p[offset++] = 0;
    p[offset++] = 7;
    memset(p + offset, 0x55, 15); offset += 15;
    put_u16(p, &offset, row_count);
    for (uint32_t row = 0; row < row_count; ++row) {
        p[offset++] = 8;
        memset(p + offset, (int)(row + 1), 15); offset += 15;
        put_u16(p, &offset, 1);
        p[offset++] = 0;
        p[offset++] = PRDB_SDK_VALUE_TYPE_INTEGER;
        put_u32(p, &offset, 4);
        put_u32(p, &offset, row + 1);
    }
    return offset;
}

static uint32_t make_one_sort_row(uint8_t *p, uint32_t value) {
    uint32_t offset = 0;
    put_u16(p, &offset, 1);
    p[offset++] = 0;
    p[offset++] = 7;
    memset(p + offset, 0x55, 15); offset += 15;
    put_u16(p, &offset, 1);
    p[offset++] = 8;
    memset(p + offset, 1, 15); offset += 15;
    put_u16(p, &offset, 1);
    p[offset++] = 0;
    p[offset++] = PRDB_SDK_VALUE_TYPE_INTEGER;
    put_u32(p, &offset, 4);
    put_u32(p, &offset, value);
    return offset;
}

static uint32_t make_group_rows(uint8_t *p, const uint32_t *values,
                                uint16_t count) {
    uint32_t offset = 0;
    put_u16(p, &offset, 1);
    p[offset++] = 0;
    p[offset++] = 7;
    memset(p + offset, 0x55, 15);
    offset += 15;
    put_u16(p, &offset, count);
    for (uint32_t index = 0; index < count; ++index) {
        p[offset++] = 8;
        memset(p + offset, (int)(index + 1), 15);
        offset += 15;
        put_u16(p, &offset, 1);
        p[offset++] = 0;
        p[offset++] = PRDB_SDK_VALUE_TYPE_INTEGER;
        put_u32(p, &offset, 4);
        put_u32(p, &offset, values[index]);
    }
    return offset;
}

typedef struct result_state {
    uint32_t calls;
    uint32_t fail;
    uint32_t sort_validate;
    uint32_t sort_failed;
    int32_t sort_previous;
    uint8_t last_value;
    const prdb_sdk_sql_column_shape_v1 *columns;
    uint8_t encoded[128];
    uint64_t encoded_len;
    prdb_sdk_value_v1 cell_scratch[1];
} result_state;

typedef struct group_result_state {
    uint32_t calls;
    uint32_t keys[128];
    uint64_t counts[128];
    mock_host *mock;
    const prdb_sdk_sql_column_shape_v1 *columns;
    uint8_t encoded[128];
    uint64_t encoded_len;
} group_result_state;

static int32_t on_group(void *context, const prdb_sdk_value_v1 *key,
                        uint32_t key_count,
                        const prdb_sdk_value_v1 *aggregate, uint64_t count,
                        const prdb_sdk_select_binding_v1 *binding) {
    group_result_state *result = (group_result_state *)context;
    const prdb_sdk_sql_bound_column_v1 *column = &binding->group_by_column;
    (void)key_count;
    (void)aggregate;
    int32_t value = 0;
    assert(result->mock != NULL && result->mock->executor_calls == 4);
    assert(result->calls < 128);
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
    if (result->mock->cancel_after_group_callback == result->calls) {
        result->mock->cancel_after_checks =
            result->mock->cancellation_checks + 1;
    }
    return 0;
}

static int32_t on_row(void *context, const prdb_sdk_row_v1 *row,
                      const prdb_sdk_select_binding_v1 *binding) {
    result_state *result = (result_state *)context;
    if (result->sort_validate) {
        prdb_sdk_cell_iterator_v1 cells = {0};
        prdb_sdk_value_v1 value = {0};
        int32_t current = 0;
        assert(prdb_sdk_row_cells_begin_v1(row, &cells, sizeof(cells)) ==
               PRDB_SDK_BATCH_OK);
        assert(prdb_sdk_cell_next_v1(&cells, &value, sizeof(value)) ==
               PRDB_SDK_BATCH_OK);
        assert(prdb_sdk_value_get_integer_v1(&value, &current) ==
               PRDB_SDK_VALUE_ACCESS_OK);
        if (result->calls != 0 && result->sort_previous < current)
            result->sort_failed = 1;
        result->sort_previous = current;
    }
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
    static const uint8_t no_match_sql[] =
        "SELECT id FROM widgets WHERE id < $1";
    static const uint8_t in_sql[] =
        "SELECT id FROM widgets WHERE id IN ($1, $2) LIMIT 1";
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
    static const uint8_t group_no_match_sql[] =
        "SELECT id, COUNT(*) FROM widgets WHERE id < $1 GROUP BY id";
    static const uint8_t insert_sql[] =
        "INSERT INTO widgets VALUES ($1, NULL, DEFAULT)";
    static const uint8_t column_list_insert_sql[] =
        "INSERT INTO widgets (payload, ID) VALUES ($1, $2)";
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
    mock_host mock = {
        .planner = planner_payload,
        .planner_len = planner_len,
        .executor = executor_payload,
        .executor_len = executor_len,
        .batch = batch_payload,
        .batch_len = sizeof(batch_payload),
    };
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             &mock, executor_request, planner_request, NULL,
                             statement_failure};
    prdb_host_api_v6 spill_host = {0};
    prdb_sdk_value_v1 parameter = {sizeof(parameter),
                                   PRDB_SDK_VALUE_ABI_V1_VERSION,
                                   PRDB_SDK_VALUE_STATE_VALUE,
                                   PRDB_SDK_VALUE_TYPE_INTEGER,
                                   parameter_bytes, sizeof(parameter_bytes)};
    const uint8_t insert_parameter_bytes[] = {0, 0, 0, 42};
    const uint8_t column_list_payload_bytes[] = {0xaa, 0xbb};
    prdb_sdk_value_v1 insert_parameter = {
        sizeof(insert_parameter), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
        insert_parameter_bytes, sizeof(insert_parameter_bytes)};
    prdb_sdk_value_v1 column_list_parameters[2] = {
        {sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
         PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BYTEA,
         column_list_payload_bytes, sizeof(column_list_payload_bytes)},
        {sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
         PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
         insert_parameter_bytes, sizeof(insert_parameter_bytes)}};
    static const uint8_t multi_parameter_bytes[2][4] = {
        {0, 0, 0, 42}, {0, 0, 0, 43}};
    static const uint8_t in_parameter_bytes[2][4] = {
        {0, 0, 0, 4}, {0, 0, 0, 5}};
    prdb_sdk_value_v1 multi_parameters[2] = {{0}};
    prdb_sdk_value_v1 in_parameters[2] = {{0}};
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[4096] = {0};
    uint8_t materialized_rows[256] = {0};
    prdb_sdk_sql_column_shape_v1 planner_columns[3] = {{0}};
    prdb_sdk_sql_column_shape_v1 executor_columns[3] = {{0}};
    prdb_sdk_value_v1 cell_scratch[1] = {{0}};
    result_state result = {0};
    result.columns = executor_columns;
    for (uint32_t index = 0; index < 2; ++index) {
        multi_parameters[index] = (prdb_sdk_value_v1){
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
            multi_parameter_bytes[index], sizeof(multi_parameter_bytes[index])};
        in_parameters[index] = (prdb_sdk_value_v1){
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
            in_parameter_bytes[index], sizeof(in_parameter_bytes[index])};
    }
    assert(sizeof(prdb_host_api_v3) == 48);
    assert(sizeof(prdb_query_descriptor_v3_record) == 16);
    assert(sizeof(prdb_invocation_v3) == 24);
    spill_host.v5.v4.v3 = host;
    spill_host.v5.v4.v3.struct_size = sizeof(spill_host.v5.v4.v3);
    spill_host.v5.v4.v3.abi_version = PRDB_QUERY_ABI_V3_VERSION;
    spill_host.v5.v4.v3.write_result = write_result;
    spill_host.spill_request = spill_request;
    assert(prdb_sdk_sql_execute_select_v1(
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

    mock.stream_pages = 0;
    mock.stream_next_calls = 0;
    mock.executor_calls = 0;
    mock.executor_requests = 0;
    mock.cancellation_checks = 0;
    mock.scan_finished = 0;
    mock.statement_failed = 0;
    result.calls = 0;
    result.encoded_len = 0;
    assert(prdb_sdk_sql_execute_select_v1(
               &host, schema, sizeof(schema) - 1, in_sql, sizeof(in_sql) - 1,
               in_parameters, 2, 1, 1024, planner_response,
               sizeof(planner_response), executor_response,
               sizeof(executor_response), batch_response,
               sizeof(batch_response), planner_columns, 1, executor_columns,
               1, cell_scratch, 1, on_row, &result) == PRDB_SDK_SQL_EXEC_OK);
    assert(result.calls == 1 && mock.statement_failed == 0);

    in_parameters[1] = (prdb_sdk_value_v1){
        sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_NULL, 0,
        NULL, 0};
    mock.executor_calls = 0;
    mock.executor_requests = 0;
    mock.cancellation_checks = 0;
    mock.scan_finished = 0;
    result.calls = 0;
    result.encoded_len = 0;
    assert(prdb_sdk_sql_execute_select_v1(
               &host, schema, sizeof(schema) - 1, in_sql, sizeof(in_sql) - 1,
               in_parameters, 2, 1, 1024, planner_response,
               sizeof(planner_response), executor_response,
               sizeof(executor_response), batch_response,
               sizeof(batch_response), planner_columns, 1, executor_columns,
               1, cell_scratch, 1, on_row, &result) == PRDB_SDK_SQL_EXEC_OK);
    assert(result.calls == 0 && mock.statement_failed == 0);

    mock.stream_pages = 1;
    mock.stream_next_calls = 0;
    mock.executor_requests = 0;
    mock.executor_calls = 0;
    mock.cancellation_checks = 0;
    mock.cancel_after_checks = 3;
    mock.statement_failed = 0;
    result.calls = 0;
    result.fail = 0;
    assert(prdb_sdk_sql_execute_select_v1(
               &host, schema, sizeof(schema) - 1, no_match_sql,
               sizeof(no_match_sql) - 1, &parameter, 1, 1, 1024,
               planner_response, sizeof(planner_response), executor_response,
               sizeof(executor_response), batch_response,
               sizeof(batch_response), planner_columns, 1, executor_columns,
               1, cell_scratch, 1, on_row, &result) ==
           PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    assert(result.calls == 0 && mock.cancellation_checks == 3 &&
           mock.statement_failed == 1);

    mock.stream_pages = 126;
    mock.stream_next_calls = 0;
    mock.executor_requests = 0;
    mock.executor_calls = 0;
    mock.cancellation_checks = 0;
    mock.cancel_after_checks = 0;
    mock.statement_failed = 0;
    result.calls = 0;
    assert(prdb_sdk_sql_execute_select_v1(
               &host, schema, sizeof(schema) - 1, no_match_sql,
               sizeof(no_match_sql) - 1, &parameter, 1, 1, 1024,
               planner_response, sizeof(planner_response), executor_response,
               sizeof(executor_response), batch_response,
               sizeof(batch_response), planner_columns, 1, executor_columns,
               1, cell_scratch, 1, on_row, &result) ==
           PRDB_SDK_SQL_EXEC_REQUEST_LIMIT);
    assert(mock.stream_next_calls == 126 && mock.cancellation_checks == 126 &&
           mock.executor_requests == 255 && result.calls == 0 &&
           mock.statement_failed == 1);
    mock.stream_pages = 0;

    {
        uint8_t sort_batch[128] = {0};
        uint32_t sort_batch_len = make_two_sort_rows(sort_batch, 3, 5);
        mock.batch = sort_batch;
        mock.batch_len = sort_batch_len;
        mock.executor_calls = 0;
        result.calls = 0;
        result.last_value = 0;
        result.sort_validate = 1;
        result.sort_failed = 0;
        result.sort_previous = INT32_MAX;
        mock.statement_failed = 0;
        assert(prdb_sdk_sql_execute_select_ordered_v1(
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
        assert(prdb_sdk_sql_execute_select_ordered_v1(
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
        assert(prdb_sdk_sql_execute_select_ordered_v1(
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

    {
        uint8_t many_rows[4096] = {0};
        mock.batch = many_rows;
        mock.batch_len = make_many_sort_rows(many_rows, 128);
        mock.stream_pages = 5;
        mock.stream_next_calls = 0;
        mock.executor_calls = 0;
        mock.executor_requests = 0;
        mock.cancellation_checks = 0;
        mock.statement_failed = 0;
        mock.expected_max_candidates = 128;
        mock.expected_batch_bytes = 4096;
        result.calls = 0;
        result.last_value = 0;
        assert(prdb_sdk_sql_execute_select_spill_v1(
                   &spill_host, schema, sizeof(schema) - 1, sort_all_sql,
                   sizeof(sort_all_sql) - 1, NULL, 0, NULL, 128, 4096,
                   planner_response, sizeof(planner_response),
                   executor_response, sizeof(executor_response),
                   batch_response, sizeof(batch_response), planner_columns, 1,
                   executor_columns, 1, cell_scratch, 1, on_row, &result) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(result.calls == 640 && result.last_value == 1);
        assert(result.sort_failed == 0);
        assert(mock.statement_failed == 0 && mock.stream_next_calls == 6);
        for (uint32_t index = 0; index < 16; ++index)
            assert(mock.spill[index].live == 0);

        mock.stream_next_calls = 0;
        mock.executor_calls = 0;
        mock.executor_requests = 0;
        mock.cancellation_checks = 0;
        mock.statement_failed = 0;
        mock.spill_append_calls = 0;
        mock.fail_spill_append_at = 4;
        result.calls = 0;
        assert(prdb_sdk_sql_execute_select_spill_v1(
                   &spill_host, schema, sizeof(schema) - 1, sort_all_sql,
                   sizeof(sort_all_sql) - 1, NULL, 0, NULL, 128, 4096,
                   planner_response, sizeof(planner_response),
                   executor_response, sizeof(executor_response),
                   batch_response, sizeof(batch_response), planner_columns, 1,
                   executor_columns, 1, cell_scratch, 1, on_row, &result) ==
               PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
        assert(result.calls == 0 && mock.statement_failed == 1);
        mock.fail_spill_append_at = 0;
        for (uint32_t index = 0; index < 16; ++index)
            assert(mock.spill[index].live == 0);

        mock.stream_next_calls = 0;
        mock.executor_calls = 0;
        mock.executor_requests = 0;
        mock.cancellation_checks = 0;
        mock.statement_failed = 0;
        mock.result_len = 0;
        mock.result_writes = 0;
        mock.expected_batch_bytes = 1048576;
        assert(prdb_sdk_sql_run_statement_v2(
                   &spill_host, schema, sizeof(schema) - 1, sort_all_sql,
                   sizeof(sort_all_sql) - 1, NULL, 0) ==
               PRDB_SDK_SQL_DISPATCH_OK);
        assert(mock.result_writes == 640 && mock.result_len > 640);
        assert(mock.result_bytes[mock.result_len - 1] == 1);
        assert(mock.statement_failed == 0);
        for (uint32_t index = 0; index < 16; ++index)
            assert(mock.spill[index].live == 0);
        mock.expected_batch_bytes = 4096;

        mock.stream_next_calls = 0;
        mock.executor_calls = 0;
        mock.executor_requests = 0;
        mock.cancellation_checks = 0;
        mock.statement_failed = 0;
        result.calls = 0;
        result.last_value = 0;
        result.sort_previous = INT32_MAX;
        result.sort_failed = 0;
        assert(prdb_sdk_sql_execute_select_spill_v1(
                   &spill_host, schema, sizeof(schema) - 1, sort_limit_sql,
                   sizeof(sort_limit_sql) - 1, NULL, 0, NULL, 128, 4096,
                   planner_response, sizeof(planner_response),
                   executor_response, sizeof(executor_response),
                   batch_response, sizeof(batch_response), planner_columns, 1,
                   executor_columns, 1, cell_scratch, 1, on_row, &result) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(result.calls == 1 && result.last_value == 128);
        assert(result.sort_failed == 0 && mock.statement_failed == 0);
        for (uint32_t index = 0; index < 16; ++index)
            assert(mock.spill[index].live == 0);
        mock.stream_pages = 0;
        mock.expected_max_candidates = 0;
        mock.expected_batch_bytes = 0;
        result.sort_validate = 0;
        mock.batch = batch_payload;
        mock.batch_len = sizeof(batch_payload);
    }

    mock.executor_calls = 0;
    result.calls = 0;
    result.fail = 1;
    assert(prdb_sdk_sql_execute_select_v1(
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
    assert(prdb_sdk_sql_execute_select_v1(
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
    assert(prdb_sdk_sql_execute_select_v1(
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
    uint8_t insert_request[256] = {0};
    uint8_t inserted_row_handle[16] = {0};
    prdb_sdk_executor_insert_value_v1 insert_values[6] = {{0}};
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
    int32_t multi_status = prdb_sdk_sql_execute_insert_batch_v1(
               &host, schema, sizeof(schema) - 1, multi_insert_sql,
               sizeof(multi_insert_sql) - 1, multi_parameters, 2,
               planner_response, sizeof(planner_response), executor_response,
               sizeof(executor_response), insert_request,
               sizeof(insert_request), planner_columns, 3, executor_columns,
               3, insert_values, 6, &inserted_count);
    assert(multi_status == PRDB_SDK_SQL_EXEC_OK);
    assert(inserted_count == 2 && mock.multi_insert_calls == 2);
    assert(mock.cancellation_checks == 1);
    assert(mock.executor_calls == 2);
    assert(mock.planner_calls == 1 && mock.statement_failed == 0);

    mock.planner_calls = 0;
    mock.executor_calls = 0;
    mock.multi_insert_calls = 0;
    mock.cancellation_checks = 0;
    mock.fail_multi_insert_at = 2;
    mock.statement_failed = 0;
    inserted_count = UINT32_C(0xbeef);
    assert(prdb_sdk_sql_execute_insert_batch_v1(
               &host, schema, sizeof(schema) - 1, multi_insert_sql,
               sizeof(multi_insert_sql) - 1, multi_parameters, 2,
               planner_response, sizeof(planner_response), executor_response,
               sizeof(executor_response), insert_request,
               64, planner_columns, 3, executor_columns,
               3, insert_values, 6, &inserted_count) ==
           PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    assert(inserted_count == UINT32_C(0xbeef));
    assert(mock.multi_insert_calls == 2 && mock.statement_failed == 1);

    mock.planner_calls = 0;
    mock.executor_calls = 0;
    mock.multi_insert_calls = 0;
    mock.cancellation_checks = 0;
    mock.fail_multi_insert_at = 0;
    mock.cancel_after_checks = 1;
    mock.statement_failed = 0;
    inserted_count = UINT32_C(0xcafe);
    assert(prdb_sdk_sql_execute_insert_batch_v1(
               &host, schema, sizeof(schema) - 1, multi_insert_sql,
               sizeof(multi_insert_sql) - 1, multi_parameters, 2,
               planner_response, sizeof(planner_response), executor_response,
               sizeof(executor_response), insert_request,
               sizeof(insert_request), planner_columns, 3, executor_columns,
               3, insert_values, 6, &inserted_count) ==
           PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    assert(inserted_count == UINT32_C(0xcafe));
    assert(mock.multi_insert_calls == 0 && mock.statement_failed == 1);
    mock.accept_multi_insert = 0;
    mock.cancel_after_checks = 0;
    mock.planner_len = make_relation_three(
        planner_payload, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99);
    mock.executor_len = make_relation_three(executor_payload, 6, 0x22);

    mock.planner_calls = 0;
    mock.executor_calls = 0;
    mock.column_list_insert_calls = 0;
    mock.check_column_list_insert = 1;
    mock.statement_failed = 0;
    inserted_count = UINT32_C(0xfeed);
    assert(prdb_sdk_sql_execute_insert_batch_v1(
               &host, schema, sizeof(schema) - 1, column_list_insert_sql,
               sizeof(column_list_insert_sql) - 1, column_list_parameters, 2,
               planner_response, sizeof(planner_response), executor_response,
               sizeof(executor_response), insert_request,
               sizeof(insert_request), planner_columns, 3, executor_columns,
               3, insert_values, 6, &inserted_count) == PRDB_SDK_SQL_EXEC_OK);
    assert(inserted_count == 1 && mock.column_list_insert_calls == 1);
    assert(mock.planner_calls == 1 && mock.executor_calls == 2);
    assert(mock.statement_failed == 0);
    mock.check_column_list_insert = 0;

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
    prdb_sdk_select_binding_v1 update_binding = {0};
    prdb_sdk_value_v1 update_cell_scratch[1] = {{0}};
    prdb_sdk_executor_update_patch_v1 update_patches[1] = {{0}};
    uint8_t two_rows_batch[128] = {0};
    uint32_t two_rows_batch_len = make_two_matching_rows(two_rows_batch);
    uint8_t three_rows_batch[160] = {0};
    uint32_t three_rows_batch_len = make_matching_rows(three_rows_batch, 3);
    uint8_t max_rows_batch[4096] = {0};
    uint32_t max_rows_batch_len =
        make_matching_rows(max_rows_batch, PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS);
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

    mock.batch = two_rows_batch;
    mock.batch_len = two_rows_batch_len;
    mock.planner_calls = 0;
    mock.executor_calls = 0;
    mock.statement_failed = 0;
    mock.stage_update_calls = 0;
    mock.stage_handles_repeated = 1;
    mock.require_finished_scan_before_stage = 1;
    mock.scan_finished = 0;
    updated_count = 777;
    assert(prdb_sdk_sql_execute_update_v1(
               &host, schema, sizeof(schema) - 1, update_sql,
               sizeof(update_sql) - 1, &parameter, 1, 2, 1024,
               planner_response, sizeof(planner_response), executor_response,
               sizeof(executor_response), batch_response,
               sizeof(batch_response), update_request, sizeof(update_request),
               planner_columns, 1, executor_columns, 1, &update_binding,
               sizeof(update_binding), update_cell_scratch, 1, update_targets,
               2, update_patches, 1, &updated_count) ==
           PRDB_SDK_SQL_EXEC_OK);
    assert(updated_count == 2);
    assert(mock.stage_update_calls == 2);
    assert(mock.planner_calls == 1);
    assert(mock.executor_calls == 6);
    assert(mock.statement_failed == 0);

    mock.batch = three_rows_batch;
    mock.batch_len = three_rows_batch_len;
    mock.planner_calls = 0;
    mock.executor_calls = 0;
    mock.statement_failed = 0;
    mock.stage_update_calls = 0;
    mock.expected_max_candidates = 3;
    updated_count = 777;
    assert(prdb_sdk_sql_execute_update_v1(
               &host, schema, sizeof(schema) - 1, update_sql,
               sizeof(update_sql) - 1, &parameter, 1, 3, 1024,
               planner_response, sizeof(planner_response), executor_response,
               sizeof(executor_response), batch_response,
               sizeof(batch_response), update_request, sizeof(update_request),
               planner_columns, 1, executor_columns, 1, &update_binding,
               sizeof(update_binding), update_cell_scratch, 1, update_targets,
               3, update_patches, 1, &updated_count) ==
           PRDB_SDK_SQL_EXEC_OK);
    assert(updated_count == 3 && mock.stage_update_calls == 3 &&
           mock.planner_calls == 1 && mock.executor_calls == 7 &&
           mock.statement_failed == 0 && mock.scan_finished);

    mock.batch = max_rows_batch;
    mock.batch_len = max_rows_batch_len;
    mock.planner_calls = 0;
    mock.executor_calls = 0;
    mock.statement_failed = 0;
    mock.stage_update_calls = 0;
    mock.scan_finished = 0;
    mock.expected_max_candidates = PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS;
    updated_count = 777;
    assert(prdb_sdk_sql_execute_update_v1(
               &host, schema, sizeof(schema) - 1, update_sql,
               sizeof(update_sql) - 1, &parameter, 1,
               PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS, sizeof(batch_response),
               planner_response, sizeof(planner_response), executor_response,
               sizeof(executor_response), batch_response,
               sizeof(batch_response), update_request, sizeof(update_request),
               planner_columns, 1, executor_columns, 1, &update_binding,
               sizeof(update_binding), update_cell_scratch, 1, update_targets,
               PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS, update_patches, 1,
               &updated_count) == PRDB_SDK_SQL_EXEC_OK);
    assert(updated_count == PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS &&
           mock.stage_update_calls == PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS &&
           mock.planner_calls == 1 && mock.executor_calls == 132 &&
           mock.statement_failed == 0 && mock.scan_finished);
    mock.stage_handles_repeated = 0;
    mock.require_finished_scan_before_stage = 0;
    mock.expected_max_candidates = 0;

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
    mock.stage_delete_calls = 0;
    mock.stage_handles_repeated = 1;
    mock.require_finished_scan_before_stage = 1;
    mock.scan_finished = 0;
    deleted_count = 888;
    assert(prdb_sdk_sql_execute_delete_v1(
               &host, schema, sizeof(schema) - 1, delete_sql,
               sizeof(delete_sql) - 1, &parameter, 1, 2, 1024,
               planner_response, sizeof(planner_response), executor_response,
               sizeof(executor_response), batch_response,
               sizeof(batch_response), planner_columns, 1, executor_columns,
               1, &update_binding, sizeof(update_binding), update_cell_scratch,
               1, update_targets, 2, &deleted_count) == PRDB_SDK_SQL_EXEC_OK);
    assert(deleted_count == 2 && mock.stage_delete_calls == 2 &&
           mock.planner_calls == 1 && mock.executor_calls == 5 &&
           mock.statement_failed == 0);
    mock.stage_handles_repeated = 0;
    mock.require_finished_scan_before_stage = 0;

    mock.batch = three_rows_batch;
    mock.batch_len = three_rows_batch_len;
    mock.planner_calls = 0;
    mock.executor_calls = 0;
    mock.statement_failed = 0;
    mock.stage_delete_calls = 0;
    mock.stage_handles_repeated = 1;
    mock.require_finished_scan_before_stage = 1;
    mock.scan_finished = 0;
    mock.expected_max_candidates = 3;
    deleted_count = 888;
    assert(prdb_sdk_sql_execute_delete_v1(
               &host, schema, sizeof(schema) - 1, delete_sql,
               sizeof(delete_sql) - 1, &parameter, 1, 3, 1024,
               planner_response, sizeof(planner_response), executor_response,
               sizeof(executor_response), batch_response,
               sizeof(batch_response), planner_columns, 1, executor_columns,
               1, &update_binding, sizeof(update_binding), update_cell_scratch,
               1, update_targets, 3, &deleted_count) == PRDB_SDK_SQL_EXEC_OK);
    assert(deleted_count == 3 && mock.stage_delete_calls == 3 &&
           mock.planner_calls == 1 && mock.executor_calls == 5 &&
           mock.statement_failed == 0 && mock.scan_finished);

    mock.batch = max_rows_batch;
    mock.batch_len = max_rows_batch_len;
    mock.planner_calls = 0;
    mock.executor_calls = 0;
    mock.statement_failed = 0;
    mock.stage_delete_calls = 0;
    mock.scan_finished = 0;
    mock.expected_max_candidates = PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS;
    deleted_count = 888;
    assert(prdb_sdk_sql_execute_delete_v1(
               &host, schema, sizeof(schema) - 1, delete_sql,
               sizeof(delete_sql) - 1, &parameter, 1,
               PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS, sizeof(batch_response),
               planner_response, sizeof(planner_response), executor_response,
               sizeof(executor_response), batch_response,
               sizeof(batch_response), planner_columns, 1, executor_columns,
               1, &update_binding, sizeof(update_binding), update_cell_scratch,
               1, update_targets, PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS,
               &deleted_count) == PRDB_SDK_SQL_EXEC_OK);
    assert(deleted_count == PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS &&
           mock.stage_delete_calls == PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS &&
           mock.planner_calls == 1 && mock.executor_calls == 5 &&
           mock.statement_failed == 0 && mock.scan_finished);
    mock.stage_handles_repeated = 0;
    mock.require_finished_scan_before_stage = 0;
    mock.expected_max_candidates = 0;

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
        uint8_t group_batch[4096] = {0};
        uint32_t group_batch_len = make_two_sort_rows(group_batch, 3, 5);
        uint8_t group_key_bytes[1024] = {0};
        group_result_state groups = {0};
        groups.mock = &mock;
        groups.columns = executor_columns;
        mock.batch = group_batch;
        mock.batch_len = group_batch_len;
        mock.planner_calls = 0;
        mock.executor_calls = 0;
        mock.statement_failed = 0;
        assert(prdb_sdk_sql_execute_group_v1(
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
        assert(prdb_sdk_sql_execute_group_v1(
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
        assert(prdb_sdk_sql_execute_group_v1(
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
        assert(prdb_sdk_sql_execute_group_v1(
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
        assert(prdb_sdk_sql_execute_group_v1(
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

        mock.batch = group_batch;
        mock.batch_len = make_one_sort_row(group_batch, 3);
        mock.stream_pages = 128;
        mock.stream_next_calls = 0;
        mock.executor_requests = 0;
        mock.executor_calls = 0;
        mock.cancellation_checks = 0;
        mock.statement_failed = 0;
        groups = (group_result_state){0};
        groups.mock = &mock;
        groups.columns = executor_columns;
        int32_t budget_status = prdb_sdk_sql_execute_group_v1(
            &host, schema, sizeof(schema) - 1, group_sql,
            sizeof(group_sql) - 1, NULL, 0, 1, 1024, planner_response,
            sizeof(planner_response), executor_response,
            sizeof(executor_response), batch_response,
            sizeof(batch_response), planner_columns, 1, executor_columns, 1,
            cell_scratch, 1, group_key_bytes, sizeof(group_key_bytes),
            on_group, &groups);
        assert(budget_status == PRDB_SDK_SQL_EXEC_REQUEST_LIMIT);
        assert(groups.calls == 0 && mock.stream_next_calls == 124 &&
               mock.executor_requests == 251 && mock.statement_failed == 1);
        mock.stream_pages = 0;

        {
            const uint8_t no_match_bytes[] = {0, 0, 0, 0};
            prdb_sdk_value_v1 no_match_parameter = {
                sizeof(no_match_parameter), PRDB_SDK_VALUE_ABI_V1_VERSION,
                PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
                no_match_bytes, sizeof(no_match_bytes)};
            mock.batch = group_batch;
            mock.batch_len = make_two_sort_rows(group_batch, 3, 5);
            mock.stream_next_calls = 0;
            mock.executor_requests = 0;
            mock.executor_calls = 0;
            mock.cancellation_checks = 0;
            mock.cancel_after_checks = 0;
            mock.statement_failed = 0;
            groups = (group_result_state){0};
            groups.mock = &mock;
            groups.columns = executor_columns;
            assert(prdb_sdk_sql_execute_group_v1(
                       &host, schema, sizeof(schema) - 1, group_no_match_sql,
                       sizeof(group_no_match_sql) - 1, &no_match_parameter, 1,
                       2, 1024, planner_response, sizeof(planner_response),
                       executor_response, sizeof(executor_response),
                       batch_response, sizeof(batch_response), planner_columns,
                       1, executor_columns, 1, cell_scratch, 1, group_key_bytes,
                       sizeof(group_key_bytes), on_group, &groups) ==
                   PRDB_SDK_SQL_EXEC_OK);
            assert(groups.calls == 0 && mock.cancellation_checks == 3);

            mock.stream_next_calls = 0;
            mock.executor_requests = 0;
            mock.executor_calls = 0;
            mock.cancellation_checks = 0;
            mock.cancel_after_checks = 3;
            mock.statement_failed = 0;
            groups = (group_result_state){0};
            groups.mock = &mock;
            groups.columns = executor_columns;
            int32_t final_batch_cancel_status =
                prdb_sdk_sql_execute_group_v1(
                    &host, schema, sizeof(schema) - 1, group_no_match_sql,
                    sizeof(group_no_match_sql) - 1, &no_match_parameter, 1, 2,
                    1024, planner_response, sizeof(planner_response),
                    executor_response, sizeof(executor_response),
                    batch_response, sizeof(batch_response), planner_columns, 1,
                    executor_columns, 1, cell_scratch, 1, group_key_bytes,
                    sizeof(group_key_bytes), on_group, &groups);
            assert(final_batch_cancel_status ==
                   PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
            assert(groups.calls == 0 && mock.cancellation_checks == 3 &&
                   mock.statement_failed == 1);
            mock.cancel_after_checks = 0;
        }

        uint32_t many_values[40];
        for (uint32_t index = 0; index < 40; ++index) {
            many_values[index] = index + 3;
        }
        mock.batch = group_batch;
        mock.batch_len = make_group_rows(group_batch, many_values, 40);
        mock.expected_max_candidates = 40;
        mock.stream_next_calls = 0;
        mock.executor_requests = 0;
        mock.executor_calls = 0;
        mock.cancellation_checks = 0;
        mock.cancel_after_checks = 3;
        mock.statement_failed = 0;
        groups = (group_result_state){0};
        groups.mock = &mock;
        groups.columns = executor_columns;
        int32_t before_callback_status = prdb_sdk_sql_execute_group_v1(
            &host, schema, sizeof(schema) - 1, group_sql, sizeof(group_sql) - 1,
            NULL, 0, 40, 4096, planner_response, sizeof(planner_response),
            executor_response, sizeof(executor_response), batch_response,
            sizeof(batch_response), planner_columns, 1, executor_columns, 1,
            cell_scratch, 1, group_key_bytes, sizeof(group_key_bytes), on_group,
            &groups);
        assert(before_callback_status == PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
        assert(groups.calls == 0 && mock.cancellation_checks == 3 &&
               mock.statement_failed == 1);

        mock.stream_next_calls = 0;
        mock.executor_requests = 0;
        mock.executor_calls = 0;
        mock.cancellation_checks = 0;
        mock.cancel_after_checks = 0;
        mock.cancel_after_group_callback = 32;
        mock.statement_failed = 0;
        groups = (group_result_state){0};
        groups.mock = &mock;
        groups.columns = executor_columns;
        int32_t during_callbacks_status = prdb_sdk_sql_execute_group_v1(
            &host, schema, sizeof(schema) - 1, group_sql, sizeof(group_sql) - 1,
            NULL, 0, 40, 4096, planner_response, sizeof(planner_response),
            executor_response, sizeof(executor_response), batch_response,
            sizeof(batch_response), planner_columns, 1, executor_columns, 1,
            cell_scratch, 1, group_key_bytes, sizeof(group_key_bytes), on_group,
            &groups);
        assert(during_callbacks_status == PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
        assert(groups.calls == 32 && mock.cancellation_checks == 5 &&
               mock.statement_failed == 1);
        mock.cancel_after_group_callback = 0;
        mock.expected_max_candidates = 0;
    }
    return 0;
}
