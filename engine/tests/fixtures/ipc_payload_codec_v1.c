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
    const uint8_t simple_expected[3] = {0x00, 0x01, 0x03};
    const uint8_t terminal_cancel_expected[20] = {
        0x00, 0x01, 0x02, 0x01,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe,
    };
    const uint8_t terminal_status_expected[27] = {
        0x00, 0x01, 0x04,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe,
    };
    const uint8_t finalize_expected[11] = {
        0x00, 0x01, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
    };
    uint8_t payload[18] = {0};
    uint8_t decoded[16] = {0};
    uint8_t cancel_payload[20] = {0};
    uint8_t cancel_decoded[16] = {0};
    uint16_t decoded_reason = 0;
    uint8_t simple_payload[3] = {0};
    uint8_t decoded_status = 0;
    uint8_t terminal_cancel_payload[20] = {0};
    uint8_t terminal_status_payload[27] = {0};
    uint8_t decoded_outcome_status = 0;
    uint64_t decoded_commit_sequence = 0;
    int64_t decoded_retention_deadline = 0;
    int64_t decoded_observed_at = 0;
    uint8_t finalize_payload[11] = {0};
    uint64_t decoded_finalize_sequence = 0;
    uint8_t decoded_finalize_status = 0;
    const uint8_t abort_expected[3] = {0x00, 0x01, 0x00};
    uint8_t abort_payload[3] = {0};
    uint8_t decoded_abort_status = 0;

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
    prdb_encode_supervisor_cancel_result_simple(simple_payload, 3);
    for (size_t i = 0; i < sizeof(simple_expected); ++i) {
        if (simple_payload[i] != simple_expected[i]) {
            return 10;
        }
    }
    if (!prdb_decode_supervisor_cancel_result_simple(
            simple_payload, sizeof(simple_payload), &decoded_status) ||
        decoded_status != 3) {
        return 11;
    }
    prdb_encode_supervisor_cancel_result_terminal(
        terminal_cancel_payload, 2, 1, 7, -2);
    for (size_t i = 0; i < sizeof(terminal_cancel_expected); ++i) {
        if (terminal_cancel_payload[i] != terminal_cancel_expected[i]) {
            return 12;
        }
    }
    decoded_status = 0;
    decoded_outcome_status = 0;
    decoded_commit_sequence = 0;
    decoded_retention_deadline = 0;
    if (!prdb_decode_supervisor_cancel_result_terminal(
            terminal_cancel_payload, sizeof(terminal_cancel_payload),
            &decoded_status, &decoded_outcome_status,
            &decoded_commit_sequence, &decoded_retention_deadline) ||
        decoded_status != 2 || decoded_outcome_status != 1 ||
        decoded_commit_sequence != 7 || decoded_retention_deadline != -2) {
        return 13;
    }
    prdb_encode_supervisor_status_terminal_response(
        terminal_status_payload, 4, -1, 7, -2);
    for (size_t i = 0; i < sizeof(terminal_status_expected); ++i) {
        if (terminal_status_payload[i] != terminal_status_expected[i]) {
            return 14;
        }
    }
    decoded_status = 0;
    decoded_observed_at = 0;
    decoded_commit_sequence = 0;
    decoded_retention_deadline = 0;
    if (!prdb_decode_supervisor_status_terminal_response(
            terminal_status_payload, sizeof(terminal_status_payload),
            &decoded_status, &decoded_observed_at, &decoded_commit_sequence,
            &decoded_retention_deadline) ||
        decoded_status != 4 || decoded_observed_at != -1 ||
        decoded_commit_sequence != 7 || decoded_retention_deadline != -2) {
        return 15;
    }
    prdb_encode_supervisor_finalize_response(
        finalize_payload, 0, UINT64_C(0x0102030405060708));
    for (size_t i = 0; i < sizeof(finalize_expected); ++i) {
        if (finalize_payload[i] != finalize_expected[i]) {
            return 16;
        }
    }
    if (!prdb_decode_supervisor_finalize_response(
            finalize_payload, sizeof(finalize_payload),
            &decoded_finalize_status, &decoded_finalize_sequence) ||
        decoded_finalize_status != 0 ||
        decoded_finalize_sequence != UINT64_C(0x0102030405060708)) {
        return 17;
    }
    prdb_encode_supervisor_abort_response(abort_payload, 0);
    for (size_t i = 0; i < sizeof(abort_expected); ++i) {
        if (abort_payload[i] != abort_expected[i]) {
            return 18;
        }
    }
    if (!prdb_decode_supervisor_abort_response(
            abort_payload, sizeof(abort_payload), &decoded_abort_status) ||
        decoded_abort_status != 0) {
        return 19;
    }
    const uint8_t unknown_status_expected[3] = {0x00, 0x01, 0x00};
    uint8_t unknown_status_payload[3] = {0};
    uint8_t decoded_unknown_status = 0xff;
    prdb_encode_supervisor_status_unknown_response(unknown_status_payload, 0);
    for (size_t i = 0; i < sizeof(unknown_status_expected); ++i) {
        if (unknown_status_payload[i] != unknown_status_expected[i]) {
            return 20;
        }
    }
    if (!prdb_decode_supervisor_status_unknown_response(
            unknown_status_payload, sizeof(unknown_status_payload),
            &decoded_unknown_status) || decoded_unknown_status != 0) {
        return 21;
    }
    return 0;
}
