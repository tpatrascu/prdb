#include "prdb_query_abi_payload_codecs.h"

int main(void) {
    const uint8_t invocation_id[16] = {
        0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a,
        0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a,
    };
    const uint8_t expected[18] = {
        0x00, 0x01, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a,
        0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a,
    };
    const uint8_t cancel_id[16] = {
        0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
        0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
    };
    const uint8_t cancel_expected[20] = {
        0x00, 0x01, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
        0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x12, 0x34,
    };
    uint8_t payload[18] = {0};
    uint8_t decoded[16] = {0};
    uint8_t cancel_payload[20] = {0};
    uint8_t cancel_decoded[16] = {0};
    uint16_t decoded_reason = 0;

    prdb_encode_supervisor_invocation_control_request(payload, invocation_id);
    for (size_t i = 0; i < sizeof(expected); ++i) {
        if (payload[i] != expected[i]) {
            return 1;
        }
    }
    if (!prdb_decode_supervisor_invocation_control_request(
            payload, sizeof(payload), decoded)) {
        return 2;
    }
    for (size_t i = 0; i < sizeof(invocation_id); ++i) {
        if (decoded[i] != invocation_id[i]) {
            return 3;
        }
    }
    if (prdb_decode_supervisor_invocation_control_request(
            payload, sizeof(payload) - 1, decoded)) {
        return 4;
    }
    payload[1] = 2;
    if (prdb_decode_supervisor_invocation_control_request(
            payload, sizeof(payload), decoded)) {
        return 5;
    }

    prdb_encode_supervisor_admin_cancel_request(
        cancel_payload, cancel_id, 0x1234);
    for (size_t i = 0; i < sizeof(cancel_expected); ++i) {
        if (cancel_payload[i] != cancel_expected[i]) {
            return 6;
        }
    }
    if (!prdb_decode_supervisor_admin_cancel_request(
            cancel_payload, sizeof(cancel_payload), cancel_decoded,
            &decoded_reason)) {
        return 7;
    }
    if (decoded_reason != 0x1234) {
        return 8;
    }
    for (size_t i = 0; i < sizeof(cancel_id); ++i) {
        if (cancel_decoded[i] != cancel_id[i]) {
            return 9;
        }
    }
    return 0;
}
