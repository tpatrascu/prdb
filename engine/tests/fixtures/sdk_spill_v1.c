#include "prdb_sdk_spill_v1.h"

#include <assert.h>
#include <string.h>

typedef struct mock_state {
    uint16_t last_operation;
    uint8_t handle[16];
} mock_state;

static int32_t mock_spill(void *context, uint16_t operation,
                          const uint8_t *request, uint64_t request_len,
                          uint8_t *response, uint64_t response_capacity,
                          uint64_t *response_len) {
    mock_state *state = (mock_state *)context;
    state->last_operation = operation;
    *response_len = 0;
    if (operation == PRDB_SPILL_OP_CREATE) {
        assert(request == NULL && request_len == 0 && response_capacity == 16);
        memset(state->handle, 0x5a, sizeof(state->handle));
        memcpy(response, state->handle, 16);
        *response_len = 16;
    } else if (operation == PRDB_SPILL_OP_APPEND) {
        assert(request_len == 19 && memcmp(request, state->handle, 16) == 0);
        assert(memcmp(request + 16, "abc", 3) == 0);
    } else if (operation == PRDB_SPILL_OP_SEAL ||
               operation == PRDB_SPILL_OP_DESTROY) {
        assert(request_len == 16 && memcmp(request, state->handle, 16) == 0);
    } else if (operation == PRDB_SPILL_OP_READ_AT) {
        assert(request_len == 28 && memcmp(request, state->handle, 16) == 0);
        assert(memcmp(request + 16, "\0\0\0\0\0\0\0\x02\0\0\0\x08", 12) == 0);
        assert(response_capacity == 8);
        memcpy(response, "payload!", 8);
        *response_len = 8;
    } else {
        return -1;
    }
    return 0;
}

int main(void) {
    mock_state state = {0};
    prdb_host_api_v1 host = {0};
    host.context = &state;
    host.spill_request = mock_spill;
    prdb_sdk_spill_stream_v1 stream;
    uint8_t bytes[8] = {0};
    uint64_t bytes_len = 0;

    assert(prdb_sdk_spill_create_v1(&host, &stream) == 0);
    assert(stream.valid == 1 && state.last_operation == PRDB_SPILL_OP_CREATE);
    assert(prdb_sdk_spill_append_v1(&host, &stream, (const uint8_t *)"abc", 3) == 0);
    assert(state.last_operation == PRDB_SPILL_OP_APPEND);
    assert(prdb_sdk_spill_seal_v1(&host, &stream) == 0);
    assert(state.last_operation == PRDB_SPILL_OP_SEAL);
    assert(prdb_sdk_spill_read_at_v1(&host, &stream, 2, bytes, sizeof(bytes),
                                     &bytes_len) == 0);
    assert(bytes_len == 8 && memcmp(bytes, "payload!", 8) == 0);
    assert(prdb_sdk_spill_append_v1(
               &host, &stream, bytes,
               (uint64_t)PRDB_SDK_SPILL_MAX_IO_BYTES) != 0);
    assert(prdb_sdk_spill_destroy_v1(&host, &stream) == 0);
    assert(stream.valid == 0 && state.last_operation == PRDB_SPILL_OP_DESTROY);
    assert(prdb_sdk_spill_seal_v1(&host, &stream) != 0);
    return 0;
}
