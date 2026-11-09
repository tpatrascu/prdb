#include "prdb_ipc_frame_v1.h"

#include <assert.h>
#include <string.h>

static const uint8_t expected[] = {
    0x50, 0x52, 0x44, 0x49, 0x00, 0x01, 0x11, 0x01,
    0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
    0x00, 0x00, 0x00, 0x02, 0x6f, 0x6b,
};

static void test_exact_fixture_and_round_trip(void) {
    const uint8_t payload[] = {'o', 'k'};
    prdb_ipc_frame_v1 source = {PRDB_IPC_OP_RESOLVE_RELATION,
                                UINT64_C(0x0102030405060708), payload, 2};
    uint8_t encoded[sizeof(expected)];
    uint64_t output_len = 0;
    assert(prdb_ipc_frame_encode_v1(&source, encoded, sizeof(encoded),
                                    &output_len) == PRDB_IPC_FRAME_OK);
    assert(output_len == sizeof(expected));
    assert(memcmp(encoded, expected, sizeof(expected)) == 0);

    prdb_ipc_frame_v1 decoded = {0, 0, NULL, 0};
    uint64_t consumed = 0;
    assert(prdb_ipc_frame_decode_v1(expected, sizeof(expected), &decoded,
                                    &consumed) == PRDB_IPC_FRAME_OK);
    assert(consumed == sizeof(expected));
    assert(decoded.operation == PRDB_IPC_OP_RESOLVE_RELATION);
    assert(decoded.request_id == UINT64_C(0x0102030405060708));
    assert(decoded.payload_len == sizeof(payload));
    assert(decoded.payload == expected + PRDB_IPC_FRAME_HEADER_LEN);
    assert(memcmp(decoded.payload, payload, sizeof(payload)) == 0);
}

static void test_size_query_and_no_partial_output(void) {
    const uint8_t payload[] = {0xa1, 0xb2};
    prdb_ipc_frame_v1 frame = {PRDB_IPC_OP_RESOLVE_RELATION, 7, payload, 2};
    uint64_t output_len = 0;
    assert(prdb_ipc_frame_encode_v1(&frame, NULL, 0, &output_len) ==
           PRDB_IPC_FRAME_OUTPUT_TOO_SMALL);
    assert(output_len == sizeof(expected));

    uint8_t short_output[sizeof(expected)];
    memset(short_output, 0xa5, sizeof(short_output));
    output_len = 0;
    assert(prdb_ipc_frame_encode_v1(&frame, short_output,
                                    sizeof(short_output) - 1, &output_len) ==
           PRDB_IPC_FRAME_OUTPUT_TOO_SMALL);
    assert(output_len == sizeof(expected));
    for (size_t i = 0; i < sizeof(short_output); ++i) {
        assert(short_output[i] == 0xa5);
    }
}

static void test_encode_rejections_and_payload_boundary(void) {
    const uint8_t byte = 0x42;
    prdb_ipc_frame_v1 frame = {PRDB_IPC_OP_RESOLVE_RELATION, 1, &byte, 1};
    uint8_t output[sizeof(expected)];
    uint64_t output_len = 99;

    frame.operation = 0;
    assert(prdb_ipc_frame_encode_v1(&frame, output, sizeof(output),
                                    &output_len) == PRDB_IPC_FRAME_INVALID_OPERATION);
    assert(output_len == 0);
    frame.operation = UINT16_C(0x7fff);
    assert(prdb_ipc_frame_encode_v1(&frame, output, sizeof(output),
                                    &output_len) == PRDB_IPC_FRAME_UNKNOWN_OPERATION);
    frame.operation = PRDB_IPC_OP_RESOLVE_RELATION;
    frame.request_id = 0;
    assert(prdb_ipc_frame_encode_v1(&frame, output, sizeof(output),
                                    &output_len) == PRDB_IPC_FRAME_INVALID_REQUEST_ID);
    frame.request_id = 1;
    frame.payload = NULL;
    assert(prdb_ipc_frame_encode_v1(&frame, output, sizeof(output),
                                    &output_len) == PRDB_IPC_FRAME_INVALID_ARGUMENT);

    frame.payload = &byte;
    frame.payload_len = PRDB_IPC_FRAME_MAX_PAYLOAD_BYTES;
    assert(prdb_ipc_frame_encode_v1(&frame, NULL, 0, &output_len) ==
           PRDB_IPC_FRAME_OUTPUT_TOO_SMALL);
    assert(output_len == PRDB_IPC_FRAME_HEADER_LEN +
                             PRDB_IPC_FRAME_MAX_PAYLOAD_BYTES);
    frame.payload_len = PRDB_IPC_FRAME_MAX_PAYLOAD_BYTES + 1;
    assert(prdb_ipc_frame_encode_v1(&frame, NULL, 0, &output_len) ==
           PRDB_IPC_FRAME_PAYLOAD_TOO_LARGE);
    assert(output_len == 0);
}

static void test_decode_rejections_preserve_outputs(void) {
    uint8_t buffer[sizeof(expected)];
    prdb_ipc_frame_v1 decoded = {9, 10, (const uint8_t *)(uintptr_t)11, 12};
    const prdb_ipc_frame_v1 sentinel = decoded;
    uint64_t consumed = 13;

    memcpy(buffer, expected, sizeof(buffer));
    buffer[0] = 0;
    assert(prdb_ipc_frame_decode_v1(buffer, sizeof(buffer), &decoded,
                                    &consumed) == PRDB_IPC_FRAME_INVALID_MAGIC);
    assert(memcmp(&decoded, &sentinel, sizeof(decoded)) == 0 && consumed == 13);

    memcpy(buffer, expected, sizeof(buffer));
    buffer[5] = 2;
    assert(prdb_ipc_frame_decode_v1(buffer, sizeof(buffer), &decoded,
                                    &consumed) ==
           PRDB_IPC_FRAME_UNSUPPORTED_VERSION);

    memcpy(buffer, expected, sizeof(buffer));
    buffer[6] = 0;
    buffer[7] = 0;
    assert(prdb_ipc_frame_decode_v1(buffer, sizeof(buffer), &decoded,
                                    &consumed) == PRDB_IPC_FRAME_INVALID_OPERATION);

    memcpy(buffer, expected, sizeof(buffer));
    buffer[6] = 0x7f;
    buffer[7] = 0xff;
    assert(prdb_ipc_frame_decode_v1(buffer, sizeof(buffer), &decoded,
                                    &consumed) == PRDB_IPC_FRAME_UNKNOWN_OPERATION);

    memcpy(buffer, expected, sizeof(buffer));
    memset(buffer + 8, 0, 8);
    assert(prdb_ipc_frame_decode_v1(buffer, sizeof(buffer), &decoded,
                                    &consumed) == PRDB_IPC_FRAME_INVALID_REQUEST_ID);

    memcpy(buffer, expected, sizeof(buffer));
    buffer[16] = 0x00;
    buffer[17] = 0x10;
    buffer[18] = 0x00;
    buffer[19] = 0x01;
    assert(prdb_ipc_frame_decode_v1(buffer, sizeof(buffer), &decoded,
                                    &consumed) == PRDB_IPC_FRAME_PAYLOAD_TOO_LARGE);

    assert(prdb_ipc_frame_decode_v1(expected, sizeof(expected) - 1, &decoded,
                                    &consumed) == PRDB_IPC_FRAME_TRUNCATED);
}

static void test_decode_one_of_coalesced_frames_and_bad_arguments(void) {
    uint8_t coalesced[sizeof(expected) * 2];
    memcpy(coalesced, expected, sizeof(expected));
    memcpy(coalesced + sizeof(expected), expected, sizeof(expected));
    prdb_ipc_frame_v1 decoded = {0, 0, NULL, 0};
    uint64_t consumed = 0;
    assert(prdb_ipc_frame_decode_v1(coalesced, sizeof(coalesced), &decoded,
                                    &consumed) == PRDB_IPC_FRAME_OK);
    assert(consumed == sizeof(expected));
    assert(prdb_ipc_frame_decode_v1(coalesced + consumed,
                                    sizeof(coalesced) - consumed, &decoded,
                                    &consumed) == PRDB_IPC_FRAME_OK);
    assert(consumed == sizeof(expected));
    assert(prdb_ipc_frame_encode_v1(NULL, NULL, 0, &consumed) ==
           PRDB_IPC_FRAME_INVALID_ARGUMENT);
    assert(prdb_ipc_frame_decode_v1(NULL, sizeof(expected), &decoded,
                                    &consumed) == PRDB_IPC_FRAME_INVALID_ARGUMENT);
}

int main(void) {
    test_exact_fixture_and_round_trip();
    test_size_query_and_no_partial_output();
    test_encode_rejections_and_payload_boundary();
    test_decode_rejections_preserve_outputs();
    test_decode_one_of_coalesced_frames_and_bad_arguments();
    return 0;
}
