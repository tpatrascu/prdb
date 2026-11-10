#include "prdb_query_abi_payload_codecs.h"

#include <string.h>

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
    const uint8_t active_no_deadline_expected[21] = {
        0x00, 0x01, 0x01,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
        0x00, 0x01,
    };
    const uint8_t active_with_deadline_expected[29] = {
        0x00, 0x01, 0x02,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
        0x01,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe,
        0x00,
    };
    uint8_t active_no_deadline_payload[21] = {0};
    uint8_t active_with_deadline_payload[29] = {0};
    uint8_t decoded_active_status = 0;
    int64_t decoded_active_observed_at = 0;
    int64_t decoded_active_started_at = 0;
    uint8_t decoded_has_deadline = 0;
    int64_t decoded_active_deadline_at = 0;
    uint8_t decoded_active_cancellation_requested = 0;

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
    prdb_encode_supervisor_status_active_no_deadline_response(
        active_no_deadline_payload, 1, -1, 3, 0, 1);
    for (size_t i = 0; i < sizeof(active_no_deadline_expected); ++i) {
        if (active_no_deadline_payload[i] != active_no_deadline_expected[i]) {
            return 22;
        }
    }
    if (!prdb_decode_supervisor_status_active_no_deadline_response(
            active_no_deadline_payload, sizeof(active_no_deadline_payload),
            &decoded_active_status, &decoded_active_observed_at,
            &decoded_active_started_at, &decoded_has_deadline,
            &decoded_active_cancellation_requested) ||
        decoded_active_status != 1 || decoded_active_observed_at != -1 ||
        decoded_active_started_at != 3 || decoded_has_deadline != 0 ||
        decoded_active_cancellation_requested != 1) {
        return 23;
    }
    prdb_encode_supervisor_status_active_with_deadline_response(
        active_with_deadline_payload, 2, -1, 3, 1, -2, 0);
    for (size_t i = 0; i < sizeof(active_with_deadline_expected); ++i) {
        if (active_with_deadline_payload[i] != active_with_deadline_expected[i]) {
            return 24;
        }
    }
    if (!prdb_decode_supervisor_status_active_with_deadline_response(
            active_with_deadline_payload, sizeof(active_with_deadline_payload),
            &decoded_active_status, &decoded_active_observed_at,
            &decoded_active_started_at, &decoded_has_deadline,
            &decoded_active_deadline_at,
            &decoded_active_cancellation_requested) ||
        decoded_active_status != 2 || decoded_active_observed_at != -1 ||
        decoded_active_started_at != 3 || decoded_has_deadline != 1 ||
        decoded_active_deadline_at != -2 ||
        decoded_active_cancellation_requested != 0) {
        return 25;
    }
    const uint8_t optimizer_failure_expected[3] = {0x00, 0x01, 0x04};
    uint8_t optimizer_failure_payload[3] = {0};
    uint8_t decoded_optimizer_failure_status = 0;
    prdb_encode_optimizer_response_failure(optimizer_failure_payload, 4);
    for (size_t i = 0; i < sizeof(optimizer_failure_expected); ++i) {
        if (optimizer_failure_payload[i] != optimizer_failure_expected[i]) {
            return 26;
        }
    }
    if (!prdb_decode_optimizer_response_failure(
            optimizer_failure_payload, sizeof(optimizer_failure_payload),
            &decoded_optimizer_failure_status) ||
        decoded_optimizer_failure_status != 4) {
        return 27;
    }
    const uint8_t optimizer_success_expected[10] = {
        0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x03, 0xaa, 0xbb, 0xcc,
    };
    uint8_t optimizer_success_payload[10] = {0};
    uint32_t decoded_optimizer_plan_length = 0;
    prdb_encode_optimizer_response_success(
        optimizer_success_payload, 0, 3);
    optimizer_success_payload[7] = 0xaa;
    optimizer_success_payload[8] = 0xbb;
    optimizer_success_payload[9] = 0xcc;
    for (size_t i = 0; i < sizeof(optimizer_success_expected); ++i) {
        if (optimizer_success_payload[i] != optimizer_success_expected[i]) {
            return 49;
        }
    }
    if (!prdb_decode_optimizer_response_success(
            optimizer_success_payload, sizeof(optimizer_success_payload),
            &decoded_optimizer_failure_status, &decoded_optimizer_plan_length) ||
        decoded_optimizer_failure_status != 0 || decoded_optimizer_plan_length != 3) {
        return 50;
    }
    const uint8_t cancellation_check_expected[3] = {0x00, 0x01, 0x01};
    uint8_t cancellation_check_payload[3] = {0};
    uint8_t decoded_cancelled = 0;
    prdb_encode_executor_cancellation_check_response(cancellation_check_payload, 1);
    for (size_t i = 0; i < sizeof(cancellation_check_expected); ++i) {
        if (cancellation_check_payload[i] != cancellation_check_expected[i]) {
            return 28;
        }
    }
    if (!prdb_decode_executor_cancellation_check_response(
            cancellation_check_payload, sizeof(cancellation_check_payload),
            &decoded_cancelled) || decoded_cancelled != 1) {
        return 29;
    }
    const uint8_t mutation_ack_expected[3] = {0x00, 0x01, 0x00};
    uint8_t mutation_ack_payload[3] = {0};
    uint8_t decoded_mutation_ack_status = 0xff;
    prdb_encode_executor_mutation_acknowledgement(mutation_ack_payload, 0);
    for (size_t i = 0; i < sizeof(mutation_ack_expected); ++i) {
        if (mutation_ack_payload[i] != mutation_ack_expected[i]) {
            return 30;
        }
    }
    if (!prdb_decode_executor_mutation_acknowledgement(
            mutation_ack_payload, sizeof(mutation_ack_payload),
            &decoded_mutation_ack_status) || decoded_mutation_ack_status != 0) {
        return 31;
    }
    const uint8_t open_scan_response_expected[18] = {
        0x00, 0x01,
        0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a,
        0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a,
    };
    uint8_t open_scan_response_payload[18] = {0};
    uint8_t open_scan_response_cursor[16] = {0};
    for (size_t i = 0; i < sizeof(open_scan_response_cursor); ++i) {
        open_scan_response_cursor[i] = 0x5a;
    }
    uint8_t decoded_open_scan_response_cursor[16] = {0};
    prdb_encode_executor_open_scan_response(
        open_scan_response_payload, open_scan_response_cursor);
    for (size_t i = 0; i < sizeof(open_scan_response_expected); ++i) {
        if (open_scan_response_payload[i] != open_scan_response_expected[i]) {
            return 32;
        }
    }
    if (!prdb_decode_executor_open_scan_response(
            open_scan_response_payload, sizeof(open_scan_response_payload),
            decoded_open_scan_response_cursor)) {
        return 33;
    }
    for (size_t i = 0; i < sizeof(open_scan_response_cursor); ++i) {
        if (decoded_open_scan_response_cursor[i] != open_scan_response_cursor[i]) {
            return 34;
        }
    }
    const uint8_t stage_insert_response_expected[18] = {
        0x00, 0x01, 0x08,
        0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a,
        0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a,
    };
    uint8_t stage_insert_response_handle[16] = {0};
    stage_insert_response_handle[0] = 0x08;
    for (size_t i = 1; i < sizeof(stage_insert_response_handle); ++i) {
        stage_insert_response_handle[i] = 0x5a;
    }
    uint8_t stage_insert_response_payload[18] = {0};
    uint8_t decoded_stage_insert_response_handle[16] = {0};
    prdb_encode_executor_stage_insert_response(
        stage_insert_response_payload, stage_insert_response_handle);
    for (size_t i = 0; i < sizeof(stage_insert_response_expected); ++i) {
        if (stage_insert_response_payload[i] != stage_insert_response_expected[i]) {
            return 35;
        }
    }
    if (!prdb_decode_executor_stage_insert_response(
            stage_insert_response_payload, sizeof(stage_insert_response_payload),
            decoded_stage_insert_response_handle)) {
        return 36;
    }
    for (size_t i = 0; i < sizeof(stage_insert_response_handle); ++i) {
        if (decoded_stage_insert_response_handle[i] != stage_insert_response_handle[i]) {
            return 37;
        }
    }
    const uint8_t stage_delete_request_expected[18] = {
        0x00, 0x01, 0x08,
        0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a,
        0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a,
    };
    uint8_t stage_delete_request_payload[18] = {0};
    uint8_t decoded_stage_delete_request_handle[16] = {0};
    prdb_encode_executor_stage_delete_request(
        stage_delete_request_payload, stage_insert_response_handle);
    for (size_t i = 0; i < sizeof(stage_delete_request_expected); ++i) {
        if (stage_delete_request_payload[i] != stage_delete_request_expected[i]) {
            return 38;
        }
    }
    if (!prdb_decode_executor_stage_delete_request(
            stage_delete_request_payload, sizeof(stage_delete_request_payload),
            decoded_stage_delete_request_handle)) {
        return 39;
    }
    for (size_t i = 0; i < sizeof(stage_insert_response_handle); ++i) {
        if (decoded_stage_delete_request_handle[i] != stage_insert_response_handle[i]) {
            return 40;
        }
    }
    const uint8_t planner_unavailable_expected[3] = {0x00, 0x01, 0x01};
    uint8_t planner_unavailable_payload[3] = {0};
    uint8_t decoded_planner_unavailable_status = 0;
    prdb_encode_planner_describe_relation_unavailable_response(
        planner_unavailable_payload, 1);
    for (size_t i = 0; i < sizeof(planner_unavailable_expected); ++i) {
        if (planner_unavailable_payload[i] != planner_unavailable_expected[i]) {
            return 41;
        }
    }
    if (!prdb_decode_planner_describe_relation_unavailable_response(
            planner_unavailable_payload, sizeof(planner_unavailable_payload),
            &decoded_planner_unavailable_status) ||
        decoded_planner_unavailable_status != 1) {
        return 42;
    }
    uint8_t planner_plan_unavailable_payload[3] = {0};
    decoded_planner_unavailable_status = 0;
    prdb_encode_planner_describe_plan_relation_unavailable_response(
        planner_plan_unavailable_payload, 1);
    for (size_t i = 0; i < sizeof(planner_unavailable_expected); ++i) {
        if (planner_plan_unavailable_payload[i] != planner_unavailable_expected[i]) {
            return 43;
        }
    }
    if (!prdb_decode_planner_describe_plan_relation_unavailable_response(
            planner_plan_unavailable_payload,
            sizeof(planner_plan_unavailable_payload),
            &decoded_planner_unavailable_status) ||
        decoded_planner_unavailable_status != 1) {
        return 44;
    }
    uint8_t planner_relation_id[16];
    uint8_t planner_handle[16];
    uint8_t decoded_planner_relation_id[16] = {0};
    uint8_t decoded_planner_handle[16] = {0};
    for (size_t i = 0; i < 16; ++i) {
        planner_relation_id[i] = 0x11;
        planner_handle[i] = 0x22;
    }
    uint8_t planner_available_prefix[36] = {0};
    prdb_encode_planner_describe_relation_available_response(
        planner_available_prefix, 0, planner_relation_id, planner_handle);
    planner_available_prefix[35] = 0xaa;
    if (planner_available_prefix[0] != 0x00 || planner_available_prefix[1] != 0x01 ||
        planner_available_prefix[2] != 0x00 ||
        !prdb_decode_planner_describe_relation_available_response(
            planner_available_prefix, sizeof(planner_available_prefix),
            &decoded_planner_unavailable_status, decoded_planner_relation_id,
            decoded_planner_handle) || decoded_planner_unavailable_status != 0) {
        return 45;
    }
    for (size_t i = 0; i < 16; ++i) {
        if (decoded_planner_relation_id[i] != planner_relation_id[i] ||
            decoded_planner_handle[i] != planner_handle[i]) {
            return 46;
        }
    }
    uint8_t planner_schema_id[16];
    uint8_t planner_plan_relation_id[16];
    uint8_t decoded_planner_schema_id[16] = {0};
    uint8_t decoded_planner_plan_relation_id[16] = {0};
    for (size_t i = 0; i < 16; ++i) {
        planner_schema_id[i] = 0x33;
        planner_plan_relation_id[i] = 0x44;
    }
    uint8_t planner_plan_available_prefix[52] = {0};
    uint64_t decoded_schema_version = 0;
    uint64_t decoded_table_epoch = 0;
    prdb_encode_planner_describe_plan_relation_available_response(
        planner_plan_available_prefix, 0, planner_schema_id, 3, 4,
        planner_plan_relation_id);
    planner_plan_available_prefix[51] = 0xbb;
    if (planner_plan_available_prefix[0] != 0x00 ||
        planner_plan_available_prefix[1] != 0x01 ||
        planner_plan_available_prefix[2] != 0x00 ||
        !prdb_decode_planner_describe_plan_relation_available_response(
            planner_plan_available_prefix, sizeof(planner_plan_available_prefix),
            &decoded_planner_unavailable_status, decoded_planner_schema_id,
            &decoded_schema_version, &decoded_table_epoch,
            decoded_planner_plan_relation_id) ||
        decoded_planner_unavailable_status != 0 || decoded_schema_version != 3 ||
        decoded_table_epoch != 4) {
        return 47;
    }
    for (size_t i = 0; i < 16; ++i) {
        if (decoded_planner_schema_id[i] != planner_schema_id[i] ||
            decoded_planner_plan_relation_id[i] != planner_plan_relation_id[i]) {
            return 48;
        }
    }
    uint8_t stage_insert_relation[16];
    uint8_t decoded_stage_insert_relation[16] = {0};
    for (size_t i = 0; i < 16; ++i) {
        stage_insert_relation[i] = 0x55;
    }
    uint8_t stage_insert_prefix[21] = {0};
    uint16_t decoded_stage_insert_count = 0;
    prdb_encode_executor_stage_insert_request(
        stage_insert_prefix, stage_insert_relation, 1);
    stage_insert_prefix[20] = 0xee;
    if (stage_insert_prefix[0] != 0x00 || stage_insert_prefix[1] != 0x01 ||
        !prdb_decode_executor_stage_insert_request(
            stage_insert_prefix, sizeof(stage_insert_prefix),
            decoded_stage_insert_relation, &decoded_stage_insert_count) ||
        decoded_stage_insert_count != 1) {
        return 51;
    }
    for (size_t i = 0; i < 16; ++i) {
        if (decoded_stage_insert_relation[i] != stage_insert_relation[i]) {
            return 52;
        }
    }
    uint8_t stage_update_handle[16];
    uint8_t decoded_stage_update_handle[16] = {0};
    for (size_t i = 0; i < 16; ++i) {
        stage_update_handle[i] = 0x66;
    }
    uint8_t stage_update_prefix[21] = {0};
    uint16_t decoded_stage_update_count = 0;
    prdb_encode_executor_stage_update_request(
        stage_update_prefix, stage_update_handle, 1);
    stage_update_prefix[20] = 0xdd;
    if (stage_update_prefix[0] != 0x00 || stage_update_prefix[1] != 0x01 ||
        !prdb_decode_executor_stage_update_request(
            stage_update_prefix, sizeof(stage_update_prefix),
            decoded_stage_update_handle, &decoded_stage_update_count) ||
        decoded_stage_update_count != 1) {
        return 53;
    }
    for (size_t i = 0; i < 16; ++i) {
        if (decoded_stage_update_handle[i] != stage_update_handle[i]) {
            return 54;
        }
    }
    uint8_t estimate_handle[16];
    uint8_t estimate_column_id[16];
    uint8_t decoded_estimate_handle[16] = {0};
    uint8_t decoded_estimate_column_id[16] = {0};
    for (size_t i = 0; i < 16; ++i) {
        estimate_handle[i] = 0x77;
        estimate_column_id[i] = 0x88;
    }
    uint8_t estimate_request[36] = {0};
    uint16_t decoded_estimate_column_count = 0;
    prdb_encode_planner_estimate_scan_request(
        estimate_request, estimate_handle, 1);
    estimate_request[20] = 0x00;
    estimate_request[21] = 0x01;
    for (size_t i = 0; i < 16; ++i) {
        estimate_request[22 + i] = estimate_column_id[i];
    }
    if (estimate_request[0] != 0x00 || estimate_request[1] != 0x01 ||
        !prdb_decode_planner_estimate_scan_request(
            estimate_request, sizeof(estimate_request), decoded_estimate_handle,
            &decoded_estimate_column_count) || decoded_estimate_column_count != 1) {
        return 55;
    }
    for (size_t i = 0; i < 16; ++i) {
        if (decoded_estimate_handle[i] != estimate_handle[i]) {
            return 56;
        }
    }
    uint8_t open_scan_relation[16];
    uint8_t open_scan_column_id[16];
    uint8_t decoded_open_scan_relation[16] = {0};
    for (size_t i = 0; i < 16; ++i) {
        open_scan_relation[i] = 0x99;
        open_scan_column_id[i] = 0xaa;
    }
    uint8_t open_scan_request[36] = {0};
    uint16_t decoded_open_scan_column_count = 0;
    prdb_encode_executor_open_scan_request(open_scan_request, open_scan_relation, 1);
    for (size_t i = 0; i < 16; ++i) {
        open_scan_request[20 + i] = open_scan_column_id[i];
    }
    if (open_scan_request[0] != 0x00 || open_scan_request[1] != 0x01 ||
        !prdb_decode_executor_open_scan_request(
            open_scan_request, sizeof(open_scan_request), decoded_open_scan_relation,
            &decoded_open_scan_column_count) || decoded_open_scan_column_count != 1) {
        return 58;
    }
    for (size_t i = 0; i < 16; ++i) {
        if (decoded_open_scan_relation[i] != open_scan_relation[i]) {
            return 59;
        }
    }
    const uint8_t resolve_relation_expected[14] = {
        0x00, 0x01, 0x00, 0x03, 0x00, 0x05,
        'a', 'p', 'p', 'i', 't', 'e', 'm', 's',
    };
    uint8_t resolve_relation_payload[14] = {0};
    uint16_t decoded_schema_name_length = 0;
    uint16_t decoded_relation_name_length = 0;
    prdb_encode_executor_resolve_relation_request(
        resolve_relation_payload, 3, 5);
    memcpy(resolve_relation_payload + 6, "appitems", 8);
    for (size_t i = 0; i < sizeof(resolve_relation_expected); ++i) {
        if (resolve_relation_payload[i] != resolve_relation_expected[i]) {
            return 60;
        }
    }
    if (!prdb_decode_executor_resolve_relation_request(
            resolve_relation_payload, sizeof(resolve_relation_payload),
            &decoded_schema_name_length, &decoded_relation_name_length) ||
        decoded_schema_name_length != 3 || decoded_relation_name_length != 5) {
        return 61;
    }
    uint8_t estimate_response[37] = {0};
    uint8_t decoded_estimate_status = 0xff;
    uint64_t decoded_visible_rows = 0;
    uint16_t decoded_population_count = 0;
    prdb_encode_planner_estimate_scan_response(estimate_response, 0, 9, 1);
    estimate_response[13] = 0x00;
    estimate_response[20] = 0x01;
    estimate_response[28] = 0x02;
    estimate_response[36] = 0x03;
    if (estimate_response[0] != 0x00 || estimate_response[1] != 0x01 ||
        !prdb_decode_planner_estimate_scan_response(
            estimate_response, sizeof(estimate_response), &decoded_estimate_status,
            &decoded_visible_rows, &decoded_population_count) ||
        decoded_estimate_status != 0 || decoded_visible_rows != 9 ||
        decoded_population_count != 1) {
        return 57;
    }
    uint8_t finalize_request[2] = {0};
    uint8_t abort_request[2] = {0};
    prdb_encode_supervisor_finalize_request(finalize_request);
    prdb_encode_supervisor_abort_request(abort_request);
    if (finalize_request[0] != 0x00 || finalize_request[1] != 0x01 ||
        abort_request[0] != 0x00 || abort_request[1] != 0x01 ||
        !prdb_decode_supervisor_finalize_request(finalize_request,
                                                  sizeof(finalize_request)) ||
        !prdb_decode_supervisor_abort_request(abort_request,
                                               sizeof(abort_request))) {
        return 62;
    }
    finalize_request[1] = 0x02;
    if (prdb_decode_supervisor_finalize_request(finalize_request,
                                                 sizeof(finalize_request))) {
        return 63;
    }
    return 0;
}
