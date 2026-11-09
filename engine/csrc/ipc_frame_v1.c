#include "prdb_ipc_frame_v1.h"

#include <string.h>

static uint16_t load_u16_be(const uint8_t *bytes) {
    return (uint16_t)(((uint16_t)bytes[0] << 8) | bytes[1]);
}

static uint32_t load_u32_be(const uint8_t *bytes) {
    return ((uint32_t)bytes[0] << 24) | ((uint32_t)bytes[1] << 16) |
           ((uint32_t)bytes[2] << 8) | (uint32_t)bytes[3];
}

static uint64_t load_u64_be(const uint8_t *bytes) {
    uint64_t value = 0;
    for (uint32_t index = 0; index < 8; ++index) {
        value = (value << 8) | bytes[index];
    }
    return value;
}

static void store_u16_be(uint8_t *bytes, uint16_t value) {
    bytes[0] = (uint8_t)(value >> 8);
    bytes[1] = (uint8_t)value;
}

static void store_u32_be(uint8_t *bytes, uint32_t value) {
    bytes[0] = (uint8_t)(value >> 24);
    bytes[1] = (uint8_t)(value >> 16);
    bytes[2] = (uint8_t)(value >> 8);
    bytes[3] = (uint8_t)value;
}

static void store_u64_be(uint8_t *bytes, uint64_t value) {
    for (uint32_t index = 0; index < 8; ++index) {
        bytes[index] = (uint8_t)(value >> (56 - index * 8));
    }
}

static int operation_is_known(uint16_t operation) {
    switch (operation) {
#define PRDB_IPC_OPERATION_CASE(operation_code) case operation_code:
#include "prdb_ipc_operation_cases_v1.h"
#undef PRDB_IPC_OPERATION_CASE
        return 1;
    default:
        return 0;
    }
}

int32_t prdb_ipc_frame_encode_v1(const prdb_ipc_frame_v1 *frame,
                                 uint8_t *output,
                                 uint64_t output_capacity,
                                 uint64_t *output_len) {
    if (frame == NULL || output_len == NULL || (output == NULL && output_capacity != 0)) {
        return PRDB_IPC_FRAME_INVALID_ARGUMENT;
    }
    *output_len = 0;
    if (frame->operation == 0) {
        return PRDB_IPC_FRAME_INVALID_OPERATION;
    }
    if (!operation_is_known(frame->operation)) {
        return PRDB_IPC_FRAME_UNKNOWN_OPERATION;
    }
    if (frame->request_id == 0) {
        return PRDB_IPC_FRAME_INVALID_REQUEST_ID;
    }
    if (frame->payload_len > PRDB_IPC_FRAME_MAX_PAYLOAD_BYTES) {
        return PRDB_IPC_FRAME_PAYLOAD_TOO_LARGE;
    }
    if (frame->payload_len != 0 && frame->payload == NULL) {
        return PRDB_IPC_FRAME_INVALID_ARGUMENT;
    }
    uint64_t required = PRDB_IPC_FRAME_HEADER_LEN + (uint64_t)frame->payload_len;
    *output_len = required;
    if (output == NULL || output_capacity < required) {
        return PRDB_IPC_FRAME_OUTPUT_TOO_SMALL;
    }

    if (frame->payload_len != 0) {
        memmove(output + PRDB_IPC_FRAME_HEADER_LEN, frame->payload, frame->payload_len);
    }
    store_u32_be(output + PRDB_IPC_FRAME_MAGIC_OFFSET, PRDB_IPC_FRAME_MAGIC);
    store_u16_be(output + PRDB_IPC_FRAME_VERSION_OFFSET, PRDB_IPC_FRAME_VERSION);
    store_u16_be(output + PRDB_IPC_FRAME_OPERATION_OFFSET, frame->operation);
    store_u64_be(output + PRDB_IPC_FRAME_REQUEST_ID_OFFSET, frame->request_id);
    store_u32_be(output + PRDB_IPC_FRAME_PAYLOAD_LEN_OFFSET, frame->payload_len);
    return PRDB_IPC_FRAME_OK;
}

int32_t prdb_ipc_frame_decode_v1(const uint8_t *input,
                                 uint64_t input_len,
                                 prdb_ipc_frame_v1 *frame,
                                 uint64_t *consumed_len) {
    if (input == NULL || frame == NULL || consumed_len == NULL) {
        return PRDB_IPC_FRAME_INVALID_ARGUMENT;
    }
    if (input_len < PRDB_IPC_FRAME_HEADER_LEN) {
        return PRDB_IPC_FRAME_TRUNCATED;
    }
    if (load_u32_be(input + PRDB_IPC_FRAME_MAGIC_OFFSET) != PRDB_IPC_FRAME_MAGIC) {
        return PRDB_IPC_FRAME_INVALID_MAGIC;
    }
    if (load_u16_be(input + PRDB_IPC_FRAME_VERSION_OFFSET) != PRDB_IPC_FRAME_VERSION) {
        return PRDB_IPC_FRAME_UNSUPPORTED_VERSION;
    }
    uint16_t operation = load_u16_be(input + PRDB_IPC_FRAME_OPERATION_OFFSET);
    if (operation == 0) {
        return PRDB_IPC_FRAME_INVALID_OPERATION;
    }
    if (!operation_is_known(operation)) {
        return PRDB_IPC_FRAME_UNKNOWN_OPERATION;
    }
    uint64_t request_id = load_u64_be(input + PRDB_IPC_FRAME_REQUEST_ID_OFFSET);
    if (request_id == 0) {
        return PRDB_IPC_FRAME_INVALID_REQUEST_ID;
    }
    uint32_t payload_len = load_u32_be(input + PRDB_IPC_FRAME_PAYLOAD_LEN_OFFSET);
    if (payload_len > PRDB_IPC_FRAME_MAX_PAYLOAD_BYTES) {
        return PRDB_IPC_FRAME_PAYLOAD_TOO_LARGE;
    }
    uint64_t total_len = PRDB_IPC_FRAME_HEADER_LEN + (uint64_t)payload_len;
    if (input_len < total_len) {
        return PRDB_IPC_FRAME_TRUNCATED;
    }

    prdb_ipc_frame_v1 decoded = {
        operation,
        request_id,
        input + PRDB_IPC_FRAME_HEADER_LEN,
        payload_len,
    };
    *frame = decoded;
    *consumed_len = total_len;
    return PRDB_IPC_FRAME_OK;
}
