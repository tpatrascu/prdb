#include "prdb_sdk_optimizer_plan_v1.h"
#include "prdb_sdk_optimizer_v1.h"
#include "prdb_sdk_sql_exec_v1.h"
#include "prdb_sdk_sql_v1.h"
#include "prdb_query_abi_payload_codecs.h"

#include <assert.h>
#include <stdint.h>
#include <string.h>


#include "sdk_optimizer_plan_v1/mock_harness.h"

int main(void) {
    uint8_t payload[256] = {0};
    uint8_t null_filter_payload[512] = {0};
    uint8_t malformed[sizeof(payload)];
    uint8_t join_payload[1024] = {0};
    uint8_t join_malformed[sizeof(join_payload)];
    uint8_t filtered_join_payload[sizeof(join_payload)] = {0};
    uint8_t filtered_join_malformed[sizeof(filtered_join_payload)];
    uint8_t group_count_payload[256] = {0};
    uint8_t group_count_malformed[sizeof(group_count_payload)];
    uint8_t ordered_group_count_payload[256] = {0};
    uint8_t composite_group_count_payload[512] = {0};
    uint8_t composite_group_aggregate_payload[512] = {0};
    uint8_t filtered_composite_group_aggregate_payload[512] = {0};
    uint8_t ordered_composite_group_count_payload[512] = {0};
    uint8_t group_aggregate_payload[512] = {0};
    uint8_t ordered_group_aggregate_payload[512] = {0};
    uint8_t group_aggregate_malformed[512] = {0};
    uint8_t composite_group_count_malformed[
        sizeof(composite_group_count_payload)];
    uint8_t ordered_select_payload[512] = {0};
    uint8_t ordered_select_malformed[sizeof(ordered_select_payload)];
    uint32_t projected_id_offset;
    uint32_t project_node_offset;
    uint32_t predicate_offset;
    uint32_t null_predicate_offset;
    uint32_t payload_len = build_plan(payload, &projected_id_offset,
                                      &project_node_offset);
    static prdb_sdk_optimizer_plan_v1 decoded;
    static prdb_sdk_optimizer_filter_v1 filter;
    static prdb_sdk_optimizer_update_plan_v1 update_decoded;
    static prdb_sdk_optimizer_join_plan_v1 join_decoded;
    static prdb_sdk_optimizer_filtered_join_plan_v1 filtered_join_decoded;
    static prdb_sdk_optimizer_group_count_plan_v1 group_count_decoded;
    static prdb_sdk_optimizer_ordered_group_count_plan_v1
        ordered_group_count_decoded;
    static prdb_sdk_optimizer_composite_group_count_plan_v1
        composite_group_count_decoded;
    static prdb_sdk_optimizer_composite_group_aggregate_plan_v1
        composite_group_aggregate_decoded;
    static prdb_sdk_optimizer_filtered_composite_group_aggregate_plan_v1
        filtered_composite_group_aggregate_decoded;
    static prdb_sdk_optimizer_ordered_composite_group_count_plan_v1
        ordered_composite_group_count_decoded;
    static prdb_sdk_optimizer_group_aggregate_plan_v1 group_aggregate_decoded;
    static prdb_sdk_optimizer_ordered_group_aggregate_plan_v1
        ordered_group_aggregate_decoded;
    static prdb_sdk_optimizer_ordered_select_plan_v1 ordered_select_decoded;
    static prdb_sdk_select_v1 ordered_select_statement;
    static prdb_sdk_select_binding_v1 ordered_select_binding;
    static prdb_sdk_sql_column_shape_v1 ordered_select_columns[2];
    static prdb_sdk_select_v1 join_statement;
    static prdb_sdk_select_binding_v1 join_binding;
    static prdb_sdk_select_v1 filtered_join_statement;
    static prdb_sdk_select_binding_v1 filtered_join_binding;
    static prdb_sdk_value_v1 filtered_join_parameters[3];
    static uint8_t filtered_join_parameter_bytes[3][1] = {{1}, {0}, {1}};
    static prdb_sdk_select_v1 group_count_statement;
    static prdb_sdk_select_binding_v1 group_count_binding;
    static prdb_sdk_select_v1 ordered_group_count_statement;
    static prdb_sdk_select_binding_v1 ordered_group_count_binding;
    static prdb_sdk_sql_column_shape_v1 ordered_group_count_columns[1];
    static prdb_sdk_select_v1 composite_group_count_statement;
    static prdb_sdk_select_binding_v1 composite_group_count_binding;
    static prdb_sdk_select_v1 composite_group_aggregate_statement;
    static prdb_sdk_select_binding_v1 composite_group_aggregate_binding;
    static prdb_sdk_select_v1 count_column_statement;
    static prdb_sdk_select_binding_v1 count_column_binding;
    static prdb_sdk_sql_column_shape_v1 composite_group_aggregate_columns[3];
    static uint8_t count_column_optimizer_request[512];
    static uint64_t count_column_optimizer_request_len;
    static prdb_sdk_select_v1 ordered_composite_group_count_statement;
    static prdb_sdk_select_binding_v1 ordered_composite_group_count_binding;
    static prdb_sdk_sql_column_shape_v1 ordered_composite_group_count_columns[2];
    static prdb_sdk_select_v1 group_aggregate_statement;
    static prdb_sdk_select_binding_v1 group_aggregate_binding;
    static prdb_sdk_select_v1 ordered_group_aggregate_statement;
    static prdb_sdk_select_binding_v1 ordered_group_aggregate_binding;
    static prdb_sdk_sql_column_shape_v1 group_aggregate_columns[2];
    uint32_t ordered_select_order_key_offset;
    uint32_t ordered_select_payload_len = build_ordered_select_plan(
        ordered_select_payload, &ordered_select_order_key_offset);
    assert(prdb_sdk_optimizer_decode_ordered_select_plan_v1(
               ordered_select_payload, ordered_select_payload_len,
               &ordered_select_decoded, sizeof(ordered_select_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(ordered_select_decoded.plan.plan_kind == 7 &&
           ordered_select_decoded.plan.scan_column_count == 2 &&
           ordered_select_decoded.plan.projection_count == 1 &&
           ordered_select_decoded.plan.projections[0].ordinal == 0 &&
           ordered_select_decoded.order_key.ordinal == 1 &&
           ordered_select_decoded.descending == 1 &&
           ordered_select_decoded.plan.has_limit == 1 &&
           ordered_select_decoded.plan.limit == 5);
    static const uint8_t ordered_select_sql[] =
        "SELECT id FROM items ORDER BY label DESC LIMIT 5";
    assert(prdb_sdk_sql_parse_select_v1(
               ordered_select_sql, sizeof(ordered_select_sql) - 1,
               &ordered_select_statement, sizeof(ordered_select_statement),
               NULL) == PRDB_SDK_SQL_OK);
    ordered_select_columns[0] = (prdb_sdk_sql_column_shape_v1){
        (const uint8_t *)"id", 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0};
    ordered_select_columns[1] = (prdb_sdk_sql_column_shape_v1){
        (const uint8_t *)"label", 5, 1, PRDB_SDK_VALUE_TYPE_TEXT, 0, 0};
    assert(prdb_sdk_sql_bind_select_v1(
               &ordered_select_statement, (const uint8_t *)"items", 5,
               ordered_select_columns, 2, &ordered_select_binding,
               sizeof(ordered_select_binding)) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_optimizer_match_ordered_select_plan_v1(
               &ordered_select_statement, &ordered_select_binding,
               (const uint8_t *)"public", 6, ordered_select_payload,
               ordered_select_payload_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    ordered_select_statement.order_descending = 0;
    assert(prdb_sdk_optimizer_match_ordered_select_plan_v1(
               &ordered_select_statement, &ordered_select_binding,
               (const uint8_t *)"public", 6, ordered_select_payload,
               ordered_select_payload_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    ordered_select_statement.order_descending = 1;
    memcpy(ordered_select_malformed, ordered_select_payload,
           ordered_select_payload_len);
    ordered_select_malformed[ordered_select_order_key_offset] = 9;
    assert(prdb_sdk_optimizer_decode_ordered_select_plan_v1(
               ordered_select_malformed, ordered_select_payload_len,
               &ordered_select_decoded, sizeof(ordered_select_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    static prdb_sdk_sql_column_shape_v1 join_left_columns[3];
    static prdb_sdk_sql_column_shape_v1 join_right_columns[3];
    static resolve_context resolve = {0};
    static prdb_host_api_v1 host = {0};
    static prdb_host_api_v1 host_v4 = {0};
    static prdb_host_api_v1 host_v5 = {0};
    static prdb_sdk_executor_relation_v1 acquired_relation;
    static prdb_sdk_sql_column_shape_v1 acquired_columns[4];
    static prdb_sdk_optimizer_scan_state_v1 scan_state;
    static prdb_sdk_batch_v1 row_batch;
    static prdb_sdk_value_v1 source_cells[4];
    static prdb_sdk_value_v1 projected_values[1];
    static prdb_sdk_value_v1 delete_cell_scratch[4];
    static prdb_sdk_select_binding_v1 delete_binding;
    uint8_t row_batch_bytes[128];
    uint8_t batch_response[128];
    uint8_t result_record_buffer[256];
    uint8_t scan_response[18];
    uint32_t row_batch_len;
    uint32_t delivered_rows = 0;
    uint32_t total_rows = 0;
    uint8_t response[512];
    uint8_t request[512];
    uint8_t optimizer_response[512];
    uint8_t probe_plan[512];
    uint32_t probe_predicate_offset;
    uint32_t probe_plan_len;
    uint8_t delete_target_handles[
        PRDB_SDK_OPTIMIZER_PLAN_MAX_DELETE_TARGETS * 16];
    uint8_t sql[] = "SELECT c3 FROM items LIMIT 1";
    static prdb_sdk_select_v1 statement;
    static prdb_sdk_delete_v1 delete_statement;
    static prdb_sdk_update_v1 update_statement;
    uint64_t response_len = 0;
    uint64_t scan_response_len = 0;

    assert(prdb_sdk_optimizer_decode_plan_v1(
               payload, payload_len, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               payload, payload_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(filter.present == 0 && filter.prefix == NULL &&
           filter.prefix_len == 0);
    assert(decoded.struct_size == sizeof(decoded));
    assert(decoded.abi_version == PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION);
    assert(decoded.schema_name_len == 6);
    assert(memcmp(decoded.schema_name, "public", 6) == 0);
    assert(decoded.relation_name_len == 5);
    assert(memcmp(decoded.relation_name, "items", 5) == 0);
    assert(decoded.schema_version == 1 && decoded.table_epoch == 1);
    assert(decoded.scan_column_count == 1 && decoded.projection_count == 1);
    assert(decoded.scan_columns[0].ordinal == 3);
    assert(decoded.projections[0].type_code == 6);
    assert(decoded.has_limit == 1 && decoded.limit == 7);
    assert(prdb_sdk_optimizer_scan_state_init_v1(
               payload, payload_len, &scan_state, sizeof(scan_state)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    row_batch_len = build_row_batch(row_batch_bytes);
    assert(prdb_sdk_batch_open_v1(&row_batch, sizeof(row_batch), row_batch_bytes,
                                  row_batch_len) == PRDB_SDK_BATCH_OK);
    projected_rows_seen = 0;
    assert(prdb_sdk_optimizer_consume_scan_batch_v1(
               &scan_state, &row_batch, 4, source_cells, 4, projected_values,
               1, on_projected_row, NULL, &delivered_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(delivered_rows == 1 && projected_rows_seen == 1);
    uint8_t limit_one_plan[sizeof(payload)];
    memcpy(limit_one_plan, payload, payload_len);
    limit_one_plan[payload_len - 1] = 1;
    assert(prdb_sdk_optimizer_scan_state_init_v1(
               limit_one_plan, payload_len, &scan_state, sizeof(scan_state)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(prdb_sdk_optimizer_consume_scan_batch_v1(
               &scan_state, &row_batch, 4, source_cells, 4, projected_values,
               1, on_projected_row, NULL, &delivered_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(delivered_rows == 1 && scan_state.done == 1);
    assert(prdb_sdk_optimizer_consume_scan_batch_v1(
               &scan_state, &row_batch, 4, source_cells, 4, projected_values,
               1, on_projected_row, NULL, &delivered_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(delivered_rows == 0 && projected_rows_seen == 2);
    payload_len = build_filter_plan(payload, &predicate_offset);
    assert(prdb_sdk_optimizer_scan_state_init_v1(
               payload, payload_len, &scan_state, sizeof(scan_state)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    scan_state.projections[0].ordinal = 1;
    assert(prdb_sdk_optimizer_consume_plan_batch_v1(
               payload, payload_len, &scan_state, &row_batch, 4, source_cells,
               4, projected_values, 1, on_projected_row, NULL,
               &delivered_rows) == PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    scan_state.projections[0].ordinal = 0;
    projected_rows_seen = 0;
    assert(prdb_sdk_optimizer_consume_plan_batch_v1(
               payload, payload_len, &scan_state, &row_batch, 4, source_cells,
               4, projected_values, 1, on_projected_row, NULL,
               &delivered_rows) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(delivered_rows == 1 && projected_rows_seen == 1);
    row_batch_bytes[32] = 2;
    assert(prdb_sdk_batch_open_v1(&row_batch, sizeof(row_batch), row_batch_bytes,
                                  row_batch_len) == PRDB_SDK_BATCH_OK);
    assert(prdb_sdk_optimizer_scan_state_init_v1(
               payload, payload_len, &scan_state, sizeof(scan_state)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(prdb_sdk_optimizer_consume_plan_batch_v1(
               payload, payload_len, &scan_state, &row_batch, 4, source_cells,
               4, projected_values, 1, on_projected_row, NULL,
               &delivered_rows) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(delivered_rows == 0 && projected_rows_seen == 1);
    row_batch_bytes[32] = 1;
    assert(prdb_sdk_batch_open_v1(&row_batch, sizeof(row_batch), row_batch_bytes,
                                  row_batch_len) == PRDB_SDK_BATCH_OK);
    for (uint8_t predicate_tag = 0x40; predicate_tag <= 0x41;
         ++predicate_tag) {
        uint32_t null_filter_len = build_null_filter_plan(
            null_filter_payload, predicate_tag, &null_predicate_offset);
        assert(null_predicate_offset < null_filter_len);
        for (uint8_t state = PRDB_SDK_VALUE_STATE_VALUE;
             state <= PRDB_SDK_VALUE_STATE_REDACTED; ++state) {
            static const uint32_t expected_rows[2][3] = {
                {0, 1, 0}, /* IS NULL */
                {1, 0, 0}, /* IS NOT NULL */
            };
            uint32_t state_index = state == PRDB_SDK_VALUE_STATE_VALUE
                                       ? 0
                                       : state == PRDB_SDK_VALUE_STATE_NULL
                                             ? 1
                                             : 2;
            row_batch_len = build_cell_state_row_batch(row_batch_bytes,
                                                       state);
            assert(prdb_sdk_batch_open_v1(
                       &row_batch, sizeof(row_batch), row_batch_bytes,
                       row_batch_len) == PRDB_SDK_BATCH_OK);
            assert(prdb_sdk_optimizer_scan_state_init_v1(
                       null_filter_payload, null_filter_len, &scan_state,
                       sizeof(scan_state)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
            predicate_rows_seen = 0;
            int32_t filter_status = prdb_sdk_optimizer_consume_plan_batch_v1(
                null_filter_payload, null_filter_len, &scan_state, &row_batch,
                4, source_cells, 4, projected_values, 1,
                count_predicate_row, NULL, &delivered_rows);
            if (state == PRDB_SDK_VALUE_STATE_REDACTED) {
                assert(filter_status == PRDB_SDK_OPTIMIZER_PLAN_FILTER_ERROR);
                assert(delivered_rows == 0 && predicate_rows_seen == 0);
            } else {
                uint32_t tag_index = predicate_tag == 0x40 ? 0 : 1;
                assert(filter_status == PRDB_SDK_OPTIMIZER_PLAN_OK);
                assert(delivered_rows == expected_rows[tag_index][state_index]);
                assert(predicate_rows_seen == expected_rows[tag_index][state_index]);
            }
        }
    }
    row_batch_len = build_row_batch(row_batch_bytes);
    assert(prdb_sdk_batch_open_v1(&row_batch, sizeof(row_batch), row_batch_bytes,
                                  row_batch_len) == PRDB_SDK_BATCH_OK);
    payload_len = build_plan(payload, &projected_id_offset,
                              &project_node_offset);

    resolve.response_len = build_relation_response(resolve.response);
    resolve.executor_response_len = build_executor_relation_response(
        resolve.executor_response);
    resolve.scan_response[1] = 1;
    resolve.scan_response[2] = 7;
    resolve.batch_response_len = build_continuing_row_batch(
        resolve.batch_response);
    resolve.cancellation_response[1] = 1;
    resolve.close_response[1] = 1;
    host.struct_size = sizeof(host);
    host.abi_version = PRDB_QUERY_ABI_VERSION;
    host.context = &resolve;
    host.executor_request = resolve_plan_relation;
    {
        uint8_t empty_view_plan[512];
        prdb_sdk_optimizer_plan_v1 empty_decoded = {0};
        memcpy(empty_view_plan, payload, payload_len);
        empty_view_plan[payload_len] = 0xa3;
        resolve.calls = 0;
        resolve.cancellation_check_calls = 0;
        resolve.cancellation_response[2] = 0;
        projected_rows_seen = 0;
        total_rows = UINT32_MAX;
        assert(prdb_sdk_optimizer_execute_unfiltered_plan_v1(
                   &host, empty_view_plan, payload_len + 1, 2,
                   sizeof(batch_response), response, sizeof(response),
                   scan_response, sizeof(scan_response), batch_response,
                   sizeof(batch_response), &acquired_relation,
                   sizeof(acquired_relation), acquired_columns, 4, source_cells,
                   4, projected_values, 1, on_projected_row, NULL,
                   &total_rows) == PRDB_SDK_OPTIMIZER_PLAN_OK);
        assert(total_rows == 0 && projected_rows_seen == 0 &&
               resolve.calls == 3 && resolve.cancellation_check_calls == 1);
        resolve.calls = 0;
        resolve.cancellation_check_calls = 0;
        resolve.cancel_on_check_call = 1;
        assert(prdb_sdk_optimizer_execute_unfiltered_plan_v1(
                   &host, empty_view_plan, payload_len + 1, 2,
                   sizeof(batch_response), response, sizeof(response),
                   scan_response, sizeof(scan_response), batch_response,
                   sizeof(batch_response), &acquired_relation,
                   sizeof(acquired_relation), acquired_columns, 4, source_cells,
                   4, projected_values, 1, on_projected_row, NULL,
                   &total_rows) == PRDB_SDK_OPTIMIZER_PLAN_CANCELLED);
        assert(total_rows == 0 && projected_rows_seen == 0 &&
               resolve.calls == 3 && resolve.cancellation_check_calls == 1);
        resolve.cancel_on_check_call = 0;
        resolve.cancellation_response[2] = 0;
        empty_view_plan[payload_len + 1] = 0;
        assert(prdb_sdk_optimizer_decode_plan_v1(
                   empty_view_plan, payload_len + 2, &empty_decoded,
                   sizeof(empty_decoded)) ==
               PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    }
    uint8_t filtered_plan[512];
    uint8_t date_filter_plan[512];
    uint8_t uuid_filter_plan[512];
    uint8_t timestamptz_filter_plan[512];
    uint8_t bytea_filter_plan[512];
    uint8_t decimal_filter_plan[512];
    uint8_t probed_plan[512];
    uint8_t ranged_plan[512];
    uint8_t equality_ranged_plan[512];
    uint32_t filtered_plan_len = build_filter_plan(filtered_plan,
                                                   &predicate_offset);
    uint32_t date_filter_plan_len = build_date_filter_plan(date_filter_plan);
    prdb_sdk_optimizer_plan_v1 decoded_date_plan = {0};
    prdb_sdk_optimizer_filter_v1 decoded_date_filter = {0};
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               date_filter_plan, date_filter_plan_len, &decoded_date_plan,
               sizeof(decoded_date_plan), &decoded_date_filter,
               sizeof(decoded_date_filter)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(decoded_date_filter.present == 1 &&
           decoded_date_filter.prefix_len == 24 &&
           decoded_date_filter.prefix[0] == 0x20 &&
           decoded_date_filter.prefix[19] == 0x04);
    uint32_t uuid_filter_plan_len = build_uuid_filter_plan(uuid_filter_plan);
    prdb_sdk_optimizer_filter_v1 decoded_uuid_filter = {0};
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               uuid_filter_plan, uuid_filter_plan_len, &decoded_date_plan,
               sizeof(decoded_date_plan), &decoded_uuid_filter,
               sizeof(decoded_uuid_filter)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(decoded_uuid_filter.present == 1 &&
           decoded_uuid_filter.prefix_len == 36 &&
           decoded_uuid_filter.prefix[0] == 0x20 &&
           decoded_uuid_filter.prefix[19] == 0x05);
    uint32_t timestamptz_filter_plan_len =
        build_timestamptz_filter_plan(timestamptz_filter_plan);
    prdb_sdk_optimizer_filter_v1 decoded_timestamptz_filter = {0};
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               timestamptz_filter_plan, timestamptz_filter_plan_len,
               &decoded_date_plan, sizeof(decoded_date_plan),
               &decoded_timestamptz_filter,
               sizeof(decoded_timestamptz_filter)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(decoded_timestamptz_filter.present == 1 &&
           decoded_timestamptz_filter.prefix_len == 28 &&
           decoded_timestamptz_filter.prefix[0] == 0x20 &&
           decoded_timestamptz_filter.prefix[19] == 0x06);
    uint32_t bytea_filter_plan_len = build_bytea_filter_plan(bytea_filter_plan);
    prdb_sdk_optimizer_filter_v1 decoded_bytea_filter = {0};
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               bytea_filter_plan, bytea_filter_plan_len, &decoded_date_plan,
               sizeof(decoded_date_plan), &decoded_bytea_filter,
               sizeof(decoded_bytea_filter)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(decoded_bytea_filter.present == 1 &&
           decoded_bytea_filter.prefix_len == 25 &&
           decoded_bytea_filter.prefix[0] == 0x20 &&
           decoded_bytea_filter.prefix[19] == 0x07);
    uint32_t decimal_filter_plan_len =
        build_decimal_filter_plan(decimal_filter_plan);
    prdb_sdk_optimizer_filter_v1 decoded_decimal_filter = {0};
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               decimal_filter_plan, decimal_filter_plan_len, &decoded_date_plan,
               sizeof(decoded_date_plan), &decoded_decimal_filter,
               sizeof(decoded_decimal_filter)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(decoded_decimal_filter.present == 1 &&
           decoded_decimal_filter.prefix_len == 36 &&
           decoded_decimal_filter.prefix[0] == 0x20 &&
           decoded_decimal_filter.prefix[19] == 0x08);
    uint8_t sql_filtered[] = "SELECT c0 FROM items WHERE c0 = $1 LIMIT 1";
    uint8_t filter_parameter_bytes[4] = {0, 0, 0, 1};
    prdb_sdk_value_v1 filter_parameters[1] = {{
        sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
        filter_parameter_bytes, sizeof(filter_parameter_bytes)}};
    assert(prdb_sdk_sql_parse_select_v1(
               sql_filtered, sizeof(sql_filtered) - 1, &statement,
               sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    resolve.calls = 0;
    assert(prdb_sdk_optimizer_validate_select_plan_with_parameters_v1(
               &host, &statement, filter_parameters, 1,
               (const uint8_t *)"public", 6, filtered_plan,
               filtered_plan_len, response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.calls == 1);
    filter_parameter_bytes[3] = 2;
    assert(prdb_sdk_optimizer_validate_select_plan_with_parameters_v1(
               &host, &statement, filter_parameters, 1,
               (const uint8_t *)"public", 6, filtered_plan,
               filtered_plan_len, response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    filter_parameter_bytes[3] = 1;
    memcpy(probed_plan, filtered_plan, filtered_plan_len);
    uint32_t probed_plan_len = append_integer_point_hint(
        probed_plan, filtered_plan_len);
    resolve.probe_response[0] = 1;
    resolve.probe_response[1] = 2;
    memcpy(resolve.probe_response + 2, row_batch_bytes + 5,
           row_batch_len - 5);
    resolve.probe_response_len = 2 + row_batch_len - 5;
    memcpy(ranged_plan, filtered_plan, filtered_plan_len);
    ranged_plan[predicate_offset + 1] = 5; /* >= */
    uint32_t ranged_plan_len = append_integer_range_hint(
        ranged_plan, filtered_plan_len);
    memcpy(equality_ranged_plan, filtered_plan, filtered_plan_len);
    uint32_t equality_ranged_plan_len = append_integer_equality_range_hint(
        equality_ranged_plan, filtered_plan_len);
    prdb_sdk_optimizer_plan_v1 decoded_range_plan = {0};
    prdb_sdk_optimizer_filter_v1 decoded_range_filter = {0};
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               equality_ranged_plan, equality_ranged_plan_len,
               &decoded_range_plan, sizeof(decoded_range_plan),
               &decoded_range_filter, sizeof(decoded_range_filter)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(prdb_sdk_optimizer_validate_select_plan_with_parameters_v1(
               &host, &statement, filter_parameters, 1,
               (const uint8_t *)"public", 6, equality_ranged_plan,
               equality_ranged_plan_len, response, sizeof(response),
               &response_len) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               ranged_plan, ranged_plan_len, &decoded_range_plan,
               sizeof(decoded_range_plan), &decoded_range_filter,
               sizeof(decoded_range_filter)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    uint8_t malformed_range_plan[512];
    memcpy(malformed_range_plan, ranged_plan, ranged_plan_len);
    malformed_range_plan[filtered_plan_len + 43] = 3;
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               malformed_range_plan, ranged_plan_len, &decoded_range_plan,
               sizeof(decoded_range_plan), &decoded_range_filter,
               sizeof(decoded_range_filter)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    malformed_range_plan[filtered_plan_len + 43] = 1;
    malformed_range_plan[predicate_offset + 1] = 2; /* < contradicts >= */
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               malformed_range_plan, ranged_plan_len, &decoded_range_plan,
               sizeof(decoded_range_plan), &decoded_range_filter,
               sizeof(decoded_range_filter)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    uint8_t two_sided_range_plan[512];
    uint32_t two_sided_predicate_offset = 0;
    uint32_t two_sided_plan_len = build_two_sided_range_plan(
        two_sided_range_plan, &two_sided_predicate_offset);
    two_sided_plan_len = append_integer_two_sided_range_hint(
        two_sided_range_plan, two_sided_plan_len);
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               two_sided_range_plan, two_sided_plan_len,
               &decoded_range_plan, sizeof(decoded_range_plan),
               &decoded_range_filter, sizeof(decoded_range_filter)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    two_sided_range_plan[two_sided_predicate_offset + 40] = 11;
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               two_sided_range_plan, two_sided_plan_len,
               &decoded_range_plan, sizeof(decoded_range_plan),
               &decoded_range_filter, sizeof(decoded_range_filter)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    resolve.range_response[0] = 1;
    resolve.range_response[1] = 1;
    resolve.range_response[2] = 0;
    resolve.range_response[3] = 1;
    memcpy(resolve.range_response + 4, row_batch_bytes + 21,
           row_batch_len - 21);
    resolve.range_response_len = 4 + row_batch_len - 21;
    resolve.calls = 0;
    resolve.cancellation_check_calls = 0;
    projected_rows_seen = 0;
    total_rows = UINT32_MAX;
    assert(prdb_sdk_optimizer_execute_unfiltered_plan_v1(
               &host, ranged_plan, ranged_plan_len, 2,
               sizeof(batch_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL, &total_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(total_rows == 1 && projected_rows_seen == 1 &&
           resolve.calls == 4 && resolve.range_calls == 1);
    resolve.range_response[2] = 0;
    resolve.range_response[3] = 0;
    resolve.range_response_len = 4;
    resolve.calls = 0;
    resolve.cancellation_check_calls = 0;
    projected_rows_seen = 0;
    assert(prdb_sdk_optimizer_execute_unfiltered_plan_v1(
               &host, ranged_plan, ranged_plan_len, 2,
               sizeof(batch_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL, &total_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(total_rows == 0 && projected_rows_seen == 0 &&
           resolve.calls == 4 && resolve.range_calls == 2);
    resolve.range_response[1] = 0; /* unavailable requests a safe full scan */
    resolve.range_response_len = 2;
    memcpy(resolve.batch_response, row_batch_bytes, row_batch_len);
    resolve.batch_response_len = row_batch_len;
    resolve.calls = 0;
    resolve.cancellation_check_calls = 0;
    projected_rows_seen = 0;
    assert(prdb_sdk_optimizer_execute_unfiltered_plan_v1(
               &host, ranged_plan, ranged_plan_len, 2,
               sizeof(batch_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL, &total_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(total_rows == 1 && projected_rows_seen == 1 &&
           resolve.range_calls == 3 && resolve.calls == 6);
    resolve.range_response_too_small = 1;
    resolve.calls = 0;
    resolve.cancellation_check_calls = 0;
    projected_rows_seen = 0;
    assert(prdb_sdk_optimizer_execute_unfiltered_plan_v1(
               &host, ranged_plan, ranged_plan_len, 2,
               sizeof(batch_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL, &total_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(total_rows == 1 && projected_rows_seen == 1 &&
           resolve.range_calls == 4 && resolve.calls == 6);
    resolve.range_response_too_small = 0;
    resolve.range_response[0] = 1;
    resolve.range_response[1] = 1;
    resolve.range_response[2] = 0;
    resolve.range_response[3] = 1;
    memcpy(resolve.range_response + 4, row_batch_bytes + 21,
           row_batch_len - 21);
    resolve.range_response_len = 4 + row_batch_len - 21;
    resolve.calls = 0;
    resolve.cancellation_check_calls = 0;
    projected_rows_seen = 0;
    assert(prdb_sdk_optimizer_execute_unfiltered_plan_v1(
               &host, equality_ranged_plan, equality_ranged_plan_len, 2,
               sizeof(batch_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL, &total_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(total_rows == 1 && projected_rows_seen == 1 &&
           resolve.range_calls == 5 && resolve.calls == 4);
    resolve.batch_response_len = build_continuing_row_batch(
        resolve.batch_response);
    resolve.calls = 0;
    assert(prdb_sdk_optimizer_validate_select_plan_with_parameters_v1(
               &host, &statement, filter_parameters, 1,
               (const uint8_t *)"public", 6, probed_plan, probed_plan_len,
               response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.calls == 1);
    resolve.calls = 0;
    projected_rows_seen = 0;
    int32_t unique_probe_status = prdb_sdk_optimizer_execute_unfiltered_plan_v1(
               &host, probed_plan, probed_plan_len, 2,
               sizeof(batch_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL, &total_rows);
    assert(unique_probe_status == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(total_rows == 1 && projected_rows_seen == 1 &&
           resolve.calls == 4 && resolve.probe_calls == 1);
    resolve.probe_response[1] = 1;
    resolve.probe_response_len = 2;
    resolve.calls = 0;
    projected_rows_seen = 0;
    total_rows = UINT32_MAX;
    assert(prdb_sdk_optimizer_execute_unfiltered_plan_v1(
               &host, probed_plan, probed_plan_len, 2,
               sizeof(batch_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL, &total_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(total_rows == 0 && projected_rows_seen == 0 &&
           resolve.calls == 4 && resolve.probe_calls == 2);
    resolve.probe_response[1] = 0;
    resolve.calls = 0;
    projected_rows_seen = 0;
    assert(prdb_sdk_optimizer_execute_unfiltered_plan_v1(
               &host, probed_plan, probed_plan_len, 2,
               sizeof(batch_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL, &total_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(total_rows == 1 && projected_rows_seen == 1 &&
           resolve.calls == 7 && resolve.probe_calls == 3);
    resolve.probe_response[1] = 2;
    resolve.probe_response_len = sizeof(resolve.probe_response);
    resolve.calls = 0;
    projected_rows_seen = 0;
    assert(prdb_sdk_optimizer_execute_unfiltered_plan_v1(
               &host, probed_plan, probed_plan_len, 2,
               sizeof(batch_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL, &total_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(total_rows == 1 && projected_rows_seen == 1 &&
           resolve.calls == 7 && resolve.probe_calls == 4);
    resolve.probe_response_len = 2 + row_batch_len - 5;
    resolve.calls = 0;
    resolve.cancellation_check_calls = 0;
    resolve.cancel_on_check_call = 1;
    projected_rows_seen = 0;
    total_rows = UINT32_MAX;
    assert(prdb_sdk_optimizer_execute_unfiltered_plan_v1(
               &host, probed_plan, probed_plan_len, 2,
               sizeof(batch_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL, &total_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_CANCELLED);
    assert(total_rows == 0 && projected_rows_seen == 0 &&
           resolve.calls == 4 && resolve.probe_calls == 5);
    resolve.cancel_on_check_call = 0;
    resolve.cancellation_check_calls = 0;
    resolve.cancellation_response[2] = 0;
    assert(prdb_sdk_optimizer_validate_executor_relation_v1(
               &host, payload, payload_len, response, sizeof(response),
               &response_len) == PRDB_SDK_OPTIMIZER_PLAN_OK);

    resolve.response[34] = 2; /* A newer table epoch must reject this plan. */
    assert(prdb_sdk_optimizer_validate_executor_relation_v1(
               &host, payload, payload_len, response, sizeof(response),
               &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    resolve.response[34] = 1;
    resolve.response[150] = 4; /* Scanned column ID differs at ordinal 3. */
    assert(prdb_sdk_optimizer_validate_executor_relation_v1(
               &host, payload, payload_len, response, sizeof(response),
               &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    resolve.response[150] = 3;
    resolve.response[2] = 1;
    resolve.response_len = 3;
    assert(prdb_sdk_optimizer_validate_executor_relation_v1(
               &host, payload, payload_len, response, sizeof(response),
               &response_len) == PRDB_SDK_OPTIMIZER_PLAN_UNAVAILABLE);
    resolve.response[2] = 0;
    resolve.response_len = build_relation_response(resolve.response);
    assert(prdb_sdk_optimizer_validate_executor_relation_v1(
               &host, payload, payload_len, response, 3, &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL);

    memcpy(limit_one_plan, payload, payload_len);
    limit_one_plan[payload_len - 1] = 1;
    projected_rows_seen = 0;
    resolve.calls = 0;
    int32_t execute_status = prdb_sdk_optimizer_execute_unfiltered_plan_v1(
               &host, limit_one_plan, payload_len, 2, sizeof(batch_response), response,
               sizeof(response), scan_response, sizeof(scan_response),
               batch_response, sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL,
               &total_rows);
    assert(execute_status == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(total_rows == 1 && projected_rows_seen == 1 && resolve.calls == 6);

    assert(prdb_sdk_sql_parse_select_v1(
               sql, sizeof(sql) - 1, &statement, sizeof(statement), NULL) ==
           PRDB_SDK_SQL_OK);
    resolve.calls = 0;
    assert(prdb_sdk_optimizer_validate_select_plan_v1(
               &host, &statement, (const uint8_t *)"public", 6, limit_one_plan,
               payload_len, response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.calls == 1);
    uint8_t sql_mixed_case[] = "SELECT C3 FROM ITEMS LIMIT 1";
    assert(prdb_sdk_sql_parse_select_v1(
               sql_mixed_case, sizeof(sql_mixed_case) - 1, &statement,
               sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_optimizer_validate_select_plan_v1(
               &host, &statement, (const uint8_t *)"public", 6, limit_one_plan,
               payload_len, response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.calls == 2);
    uint8_t sql_wrong_projection[] = "SELECT c2 FROM items LIMIT 1";
    assert(prdb_sdk_sql_parse_select_v1(
               sql_wrong_projection, sizeof(sql_wrong_projection) - 1,
               &statement, sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    uint32_t calls_before_projection_mismatch = resolve.calls;
    assert(prdb_sdk_optimizer_validate_select_plan_v1(
               &host, &statement, (const uint8_t *)"public", 6, limit_one_plan,
               payload_len, response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    assert(resolve.calls == calls_before_projection_mismatch + 1);
    uint8_t sql_wrong_limit[] = "SELECT c3 FROM items LIMIT 2";
    assert(prdb_sdk_sql_parse_select_v1(
               sql_wrong_limit, sizeof(sql_wrong_limit) - 1, &statement,
               sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    calls_before_projection_mismatch = resolve.calls;
    assert(prdb_sdk_optimizer_validate_select_plan_v1(
               &host, &statement, (const uint8_t *)"public", 6, limit_one_plan,
               payload_len, response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    assert(resolve.calls == calls_before_projection_mismatch);
    assert(prdb_sdk_sql_parse_select_v1(
               sql, sizeof(sql) - 1, &statement, sizeof(statement), NULL) ==
           PRDB_SDK_SQL_OK);
    resolve.optimizer_response_len = build_optimizer_response(
        resolve.optimizer_response, limit_one_plan, payload_len);
    host_v4.struct_size = sizeof(host_v4);
    host_v4.abi_version = PRDB_QUERY_ABI_VERSION;
    host_v4.context = &resolve;
    host_v4.executor_request = resolve_plan_relation;
    host_v4.write_result = write_result;
    host_v4.statement_failure = statement_failure;
    host_v4.optimizer_request = optimizer_request;

    {
        static const uint8_t ordered_limit_zero_sql[] =
            "SELECT c0 FROM items ORDER BY c3 DESC LIMIT 0";
        uint32_t ordered_limit_zero_plan_len =
            build_ordered_c0_c3_plan(ordered_select_malformed, 0);
        uint32_t ordered_limit_zero_delivered = 99;
        uint32_t rows_before = projected_rows_seen;
        assert(prdb_sdk_sql_parse_select_v1(
                   ordered_limit_zero_sql,
                   sizeof(ordered_limit_zero_sql) - 1,
                   &ordered_select_statement,
                   sizeof(ordered_select_statement), NULL) ==
               PRDB_SDK_SQL_OK);
        resolve.optimizer_response_len = build_optimizer_response(
            resolve.optimizer_response, ordered_select_malformed,
            ordered_limit_zero_plan_len);
        resolve.optimizer_calls = 0;
        resolve.calls = 0;
        resolve.event_count = 0;
        resolve.statement_failures = 0;
        assert(prdb_sdk_optimizer_execute_ordered_select_ast_v1(
                   &host_v4, &ordered_select_statement,
                   (const uint8_t *)"public", 6, 2,
                   sizeof(batch_response), request, sizeof(request),
                   optimizer_response, sizeof(optimizer_response), response,
                   sizeof(response), scan_response, sizeof(scan_response),
                   batch_response, sizeof(batch_response), &acquired_relation,
                   sizeof(acquired_relation), acquired_columns, 4,
                   source_cells, 4, projected_values, 1, malformed,
                   sizeof(malformed), on_projected_row, NULL,
                   &ordered_limit_zero_delivered) ==
               PRDB_SDK_OPTIMIZER_PLAN_OK);
        assert(ordered_limit_zero_delivered == 0 &&
               projected_rows_seen == rows_before && resolve.calls == 2 &&
               resolve.optimizer_calls == 1 &&
               resolve.statement_failures == 0);
        assert(resolve.event_count == 3 &&
               resolve.events[0] == UINT16_C(0xffff) &&
               resolve.events[1] == PRDB_IPC_OP_RESOLVE_PLAN_RELATION &&
               resolve.events[2] == PRDB_SDK_EXECUTOR_OP_RESOLVE_RELATION);
        resolve.optimizer_response_len = build_optimizer_response(
            resolve.optimizer_response, limit_one_plan, payload_len);
    }

    {
        static const uint8_t ordered_materialization_sql[] =
            "SELECT c0 FROM items ORDER BY c3 DESC LIMIT 1";
        uint32_t ordered_plan_len =
            build_ordered_c0_c3_plan(ordered_select_malformed, 1);
        uint32_t ordered_delivered = 99;
        uint32_t rows_before = projected_rows_seen;
        uint32_t saw_ordered_open = 0;
        uint32_t saw_ordered_batch = 0;
        assert(prdb_sdk_sql_parse_select_v1(
                   ordered_materialization_sql,
                   sizeof(ordered_materialization_sql) - 1,
                   &ordered_select_statement,
                   sizeof(ordered_select_statement), NULL) ==
               PRDB_SDK_SQL_OK);
        resolve.optimizer_response_len = build_optimizer_response(
            resolve.optimizer_response, ordered_select_malformed,
            ordered_plan_len);
        resolve.batch_response_len =
            build_row_batch(resolve.batch_response);
        resolve.optimizer_calls = 0;
        resolve.calls = 0;
        resolve.event_count = 0;
        resolve.statement_failures = 0;
        resolve.cancellation_response[2] = 0;
        int32_t ordered_materialization_status =
            prdb_sdk_optimizer_execute_ordered_select_ast_v1(
                   &host_v4, &ordered_select_statement,
                   (const uint8_t *)"public", 6, 2,
                   sizeof(batch_response), request, sizeof(request),
                   optimizer_response, sizeof(optimizer_response), response,
                   sizeof(response), scan_response, sizeof(scan_response),
                   batch_response, sizeof(batch_response), &acquired_relation,
                   sizeof(acquired_relation), acquired_columns, 4,
                   source_cells, 4, projected_values, 1, malformed, 8,
                   on_projected_row, NULL, &ordered_delivered);
        assert(ordered_materialization_status ==
               PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL);
        assert(ordered_delivered == 0 && projected_rows_seen == rows_before &&
               resolve.optimizer_calls == 1 && resolve.statement_failures == 1);
        for (uint32_t event = 0; event < resolve.event_count; ++event) {
            if (resolve.events[event] == PRDB_SDK_EXECUTOR_OP_OPEN_SCAN)
                saw_ordered_open = 1;
            if (resolve.events[event] == PRDB_SDK_EXECUTOR_OP_NEXT_BATCH)
                saw_ordered_batch = 1;
        }
        assert(saw_ordered_open != 0 && saw_ordered_batch != 0);
        resolve.optimizer_response_len = build_optimizer_response(
            resolve.optimizer_response, limit_one_plan, payload_len);
        resolve.batch_response_len = build_continuing_row_batch(
            resolve.batch_response);
        resolve.statement_failures = 0;
    }

    {
        static const uint8_t ordered_cancel_sql[] =
            "SELECT c0 FROM items ORDER BY c3 DESC LIMIT 33";
        static uint8_t ordered_materialized[
            PRDB_SDK_SQL_EXEC_MAX_MATERIALIZED_BYTES];
        uint8_t ordered_executor_batch[4096];
        uint32_t ordered_plan_len =
            build_ordered_c0_c3_plan(ordered_select_malformed, 33);
        uint32_t ordered_batch_len =
            build_repeated_row_batch(ordered_executor_batch, 33);
        uint32_t ordered_delivered = 99;
        assert(prdb_sdk_sql_parse_select_v1(
                   ordered_cancel_sql, sizeof(ordered_cancel_sql) - 1,
                   &ordered_select_statement,
                   sizeof(ordered_select_statement), NULL) ==
               PRDB_SDK_SQL_OK);
        resolve.optimizer_response_len = build_optimizer_response(
            resolve.optimizer_response, ordered_select_malformed,
            ordered_plan_len);
        resolve.batch_response_len = ordered_batch_len;
        memcpy(resolve.batch_response, ordered_executor_batch,
               ordered_batch_len);
        resolve.optimizer_calls = 0;
        resolve.calls = 0;
        resolve.event_count = 0;
        resolve.statement_failures = 0;
        resolve.cancellation_check_calls = 0;
        resolve.cancel_on_check_call = 4;
        resolve.cancellation_response[2] = 0;
        projected_rows_seen = 0;
        assert(prdb_sdk_optimizer_execute_ordered_select_ast_v1(
                   &host_v4, &ordered_select_statement,
                   (const uint8_t *)"public", 6, 33,
                   sizeof(ordered_executor_batch), request, sizeof(request),
                   optimizer_response, sizeof(optimizer_response), response,
                   sizeof(response), scan_response, sizeof(scan_response),
                   ordered_executor_batch, sizeof(ordered_executor_batch),
                   &acquired_relation, sizeof(acquired_relation),
                   acquired_columns, 4, source_cells, 4, projected_values, 1,
                   ordered_materialized, sizeof(ordered_materialized),
                   on_projected_row, NULL, &ordered_delivered) ==
               PRDB_SDK_OPTIMIZER_PLAN_CANCELLED);
        assert(ordered_delivered == 32 && projected_rows_seen == 32 &&
               resolve.cancellation_check_calls == 4 &&
               resolve.statement_failures == 1);
        resolve.optimizer_response_len = build_optimizer_response(
            resolve.optimizer_response, limit_one_plan, payload_len);
        resolve.batch_response_len = build_continuing_row_batch(
            resolve.batch_response);
        resolve.cancel_on_check_call = 0;
        resolve.cancellation_check_calls = 0;
        resolve.cancellation_response[2] = 0;
        resolve.statement_failures = 0;
    }

    resolve.optimizer_calls = 0;
    resolve.calls = 0;
    resolve.event_count = 0;
    resolve.statement_failures = 0;
    projected_rows_seen = 0;
    assert(prdb_sdk_optimizer_execute_select_ast_v1(
               &host_v4, &statement, (const uint8_t *)"public", 6, 2,
               sizeof(batch_response), request, sizeof(request),
               optimizer_response, sizeof(optimizer_response), response,
               sizeof(response), scan_response, sizeof(scan_response),
               batch_response, sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL,
               &total_rows) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.optimizer_calls == 1 && resolve.calls == 7);
    assert(resolve.statement_failures == 0 && total_rows == 1 &&
           projected_rows_seen == 1);
    assert(resolve.event_count == 8 && resolve.events[0] == UINT16_C(0xffff));
    assert(resolve.events[1] == PRDB_IPC_OP_RESOLVE_PLAN_RELATION);
    assert(resolve.events[2] == PRDB_IPC_OP_RESOLVE_PLAN_RELATION);
    assert(resolve.events[3] == PRDB_SDK_EXECUTOR_OP_RESOLVE_RELATION);
    assert(resolve.events[4] == PRDB_SDK_EXECUTOR_OP_OPEN_SCAN);
    assert(resolve.events[5] == PRDB_SDK_EXECUTOR_OP_CHECK_CANCELLATION);
    assert(resolve.events[6] == PRDB_SDK_EXECUTOR_OP_NEXT_BATCH);
    assert(resolve.events[7] == PRDB_SDK_EXECUTOR_OP_CLOSE_CURSOR);

    resolve.calls = 0;
    resolve.event_count = 0;
    resolve.emitted_result_len = 0;
    resolve.result_write_calls = 0;
    assert(prdb_sdk_optimizer_execute_select_ast_to_result_v1(
               &host_v4, &statement, (const uint8_t *)"public", 6, 2,
               sizeof(batch_response), request, sizeof(request),
               optimizer_response, sizeof(optimizer_response), response,
               sizeof(response), scan_response, sizeof(scan_response),
               batch_response, sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, result_record_buffer,
               sizeof(result_record_buffer), &total_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(total_rows == 1 && resolve.emitted_result_len > 8);
    assert(memcmp(resolve.emitted_result, "SQLR", 4) == 0);
    assert(resolve.emitted_result[4] == 0 && resolve.emitted_result[5] == 1);
    assert(resolve.emitted_result[6] == 0 && resolve.emitted_result[7] == 1);
    assert(resolve.emitted_result[8] == 0 && resolve.emitted_result[9] == 2);
    assert(memcmp(resolve.emitted_result + 10, "c3", 2) == 0);
    assert(resolve.emitted_result[12] == 0 && resolve.emitted_result[13] == 0 &&
           resolve.emitted_result[14] == 0 &&
           resolve.emitted_result[15] == PRDB_SDK_VALUE_TYPE_TEXT);
    assert(resolve.emitted_result[18] == PRDB_SDK_VALUE_STATE_VALUE);
    assert(resolve.emitted_result[19] == 0 && resolve.emitted_result[20] == 0 &&
           resolve.emitted_result[21] == 0 && resolve.emitted_result[22] == 1);
    assert(resolve.emitted_result[23] == 'x');
    resolve.emitted_result_len = 0;
    resolve.statement_failures = 0;
    resolve.result_write_calls = 0;
    resolve.fail_result_write = 1;
    int32_t result_failure_status = prdb_sdk_optimizer_execute_select_ast_to_result_v1(
               &host_v4, &statement, (const uint8_t *)"public", 6, 2,
               sizeof(batch_response), request, sizeof(request),
               optimizer_response, sizeof(optimizer_response), response,
               sizeof(response), scan_response, sizeof(scan_response),
               batch_response, sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, result_record_buffer,
               sizeof(result_record_buffer), &total_rows);
    assert(result_failure_status ==
           PRDB_SDK_OPTIMIZER_PLAN_ROW_CALLBACK_ERROR);
    assert(resolve.result_write_calls == 1);
    assert(resolve.statement_failures == 1);
    resolve.fail_result_write = 0;
    resolve.statement_failures = 0;

    uint8_t sql_with_filter[] = "SELECT c3 FROM items WHERE c3 = $1";
    assert(prdb_sdk_sql_parse_select_v1(
               sql_with_filter, sizeof(sql_with_filter) - 1, &statement,
               sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    uint32_t optimizer_calls = resolve.optimizer_calls;
    assert(prdb_sdk_optimizer_execute_select_ast_v1(
               &host_v4, &statement, (const uint8_t *)"public", 6, 2,
               sizeof(batch_response), request, sizeof(request),
               optimizer_response, sizeof(optimizer_response), response,
               sizeof(response), scan_response, sizeof(scan_response),
               batch_response, sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL,
               &total_rows) == PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED);
    assert(resolve.optimizer_calls == optimizer_calls &&
           resolve.statement_failures == 0);

    assert(prdb_sdk_sql_parse_select_v1(
               sql_filtered, sizeof(sql_filtered) - 1, &statement,
               sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    resolve.optimizer_response_len = build_optimizer_response(
        resolve.optimizer_response, filtered_plan, filtered_plan_len);
    resolve.optimizer_calls = 0;
    resolve.calls = 0;
    resolve.event_count = 0;
    projected_rows_seen = 0;
    assert(prdb_sdk_optimizer_execute_select_ast_with_parameters_v1(
               &host_v4, &statement, filter_parameters, 1,
               (const uint8_t *)"public", 6, 2, sizeof(batch_response),
               request, sizeof(request), optimizer_response,
               sizeof(optimizer_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL,
               &total_rows) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.optimizer_calls == 1 && resolve.calls == 7 &&
           total_rows == 1 && projected_rows_seen == 1 &&
           resolve.statement_failures == 0);
    resolve.emitted_result_len = 0;
    resolve.result_write_calls = 0;
    assert(prdb_sdk_optimizer_execute_select_ast_with_parameters_to_result_v1(
               &host_v4, &statement, filter_parameters, 1,
               (const uint8_t *)"public", 6, 2, sizeof(batch_response),
               request, sizeof(request), optimizer_response,
               sizeof(optimizer_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, result_record_buffer,
               sizeof(result_record_buffer), &total_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(total_rows == 1 && resolve.result_write_calls == 1 &&
           resolve.emitted_result_len == 27);
    assert(memcmp(resolve.emitted_result, "SQLR", 4) == 0);
    assert(resolve.emitted_result[8] == 0 && resolve.emitted_result[9] == 2 &&
           memcmp(resolve.emitted_result + 10, "c0", 2) == 0);
    assert(resolve.emitted_result[15] == PRDB_SDK_VALUE_TYPE_INTEGER &&
           resolve.emitted_result[23] == 0 && resolve.emitted_result[24] == 0 &&
           resolve.emitted_result[25] == 0 && resolve.emitted_result[26] == 1);

    uint8_t boolean_filtered_plan[512];
    uint32_t boolean_predicate_offset;
    uint32_t boolean_filtered_plan_len = build_boolean_filter_plan(
        boolean_filtered_plan, &boolean_predicate_offset);
    (void)boolean_predicate_offset;
    uint8_t sql_boolean_filtered[] =
        "SELECT c0 FROM items WHERE c0 = $1 AND c0 = $1 LIMIT 1";
    assert(prdb_sdk_sql_parse_select_v1(
               sql_boolean_filtered, sizeof(sql_boolean_filtered) - 1,
               &statement, sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    resolve.optimizer_response_len = build_optimizer_response(
        resolve.optimizer_response, boolean_filtered_plan,
        boolean_filtered_plan_len);
    resolve.optimizer_calls = 0;
    resolve.calls = 0;
    resolve.event_count = 0;
    projected_rows_seen = 0;
    assert(prdb_sdk_optimizer_execute_select_ast_with_parameters_v1(
               &host_v4, &statement, filter_parameters, 1,
               (const uint8_t *)"public", 6, 2, sizeof(batch_response),
               request, sizeof(request), optimizer_response,
               sizeof(optimizer_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL,
               &total_rows) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.optimizer_calls == 1 && total_rows == 1 &&
           projected_rows_seen == 1 && resolve.statement_failures == 0);

    assert(prdb_sdk_sql_parse_select_v1(
               sql, sizeof(sql) - 1, &statement, sizeof(statement), NULL) ==
           PRDB_SDK_SQL_OK);
    resolve.optimizer_response[2] = PRDB_SDK_OPTIMIZER_STATUS_INTERNAL;
    resolve.optimizer_response_len = 3;
    assert(prdb_sdk_optimizer_execute_select_ast_v1(
               &host_v4, &statement, (const uint8_t *)"public", 6, 2,
               sizeof(batch_response), request, sizeof(request),
               optimizer_response, sizeof(optimizer_response), response,
               sizeof(response), scan_response, sizeof(scan_response),
               batch_response, sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, fail_projected_row, NULL,
               &total_rows) == PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR);
    assert(resolve.statement_failures == 1);
    resolve.cancellation_response[2] = 1;
    resolve.calls = 0;
    assert(prdb_sdk_optimizer_execute_unfiltered_plan_v1(
               &host, limit_one_plan, payload_len, 2, sizeof(batch_response),
               response, sizeof(response), scan_response, sizeof(scan_response),
               batch_response, sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL,
               &total_rows) == PRDB_SDK_OPTIMIZER_PLAN_CANCELLED);
    assert(total_rows == 0 && resolve.calls == 5);
    resolve.cancellation_response[2] = 0;

    memcpy(malformed, payload, payload_len);
    malformed[projected_id_offset] = 9;
    assert(prdb_sdk_optimizer_decode_plan_v1(
               malformed, payload_len, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    memcpy(malformed, payload, payload_len);
    malformed[project_node_offset] = 2;
    assert(prdb_sdk_optimizer_decode_plan_v1(
               malformed, payload_len, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED);

    memcpy(malformed, payload, payload_len);
    malformed[payload_len] = 0;
    assert(prdb_sdk_optimizer_decode_plan_v1(
               malformed, payload_len + 1, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    payload_len = build_filter_plan(payload, &predicate_offset);
    assert(prdb_sdk_optimizer_decode_plan_v1(
               payload, payload_len, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED);
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               payload, payload_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(filter.present == 1 && filter.prefix == payload + predicate_offset);
    assert(filter.prefix_len == 28);
    assert(decoded.has_limit == 1 && decoded.limit == 1);

    payload_len = build_delete_plan(payload, &predicate_offset);
    assert(prdb_sdk_optimizer_decode_delete_plan_v1(
               payload, payload_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(decoded.plan_kind == 2 && decoded.scan_column_count == 0 &&
           decoded.projection_count == 0 && decoded.has_limit == 0);
    assert(filter.present == 1 && filter.prefix == payload + predicate_offset &&
           filter.prefix_len == 2);
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               payload, payload_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    payload[payload_len - 1] = 4; /* SELECT Project instead of Delete */
    assert(prdb_sdk_optimizer_decode_delete_plan_v1(
               payload, payload_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    payload[payload_len - 1] = 5;
    payload[predicate_offset + 1] = 2; /* malformed Boolean */
    assert(prdb_sdk_optimizer_decode_delete_plan_v1(
               payload, payload_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    uint8_t update_plan[512];
    uint32_t update_plan_len = build_update_plan(update_plan);
    assert(prdb_sdk_optimizer_decode_update_plan_v1(
               update_plan, update_plan_len, &update_decoded,
               sizeof(update_decoded)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(update_decoded.plan.plan_kind == 3 &&
           update_decoded.plan.scan_column_count == 1 &&
           update_decoded.filter.present == 1 &&
           update_decoded.assignment_count == 1);
    assert(update_decoded.assignments[0].column.ordinal == 0 &&
           update_decoded.assignments[0].column.type_code == 3 &&
           update_decoded.assignments[0].value_kind ==
               PRDB_OPTIMIZER_UPDATE_PARAMETER_VALUE &&
           update_decoded.assignments[0].parameter_index == 2);

    uint32_t join_projection_side_offset;
    uint32_t join_right_key_type_offset;
    uint32_t join_payload_len = build_join_plan(
        join_payload, &join_projection_side_offset,
        &join_right_key_type_offset);
    assert(prdb_sdk_optimizer_decode_join_plan_v1(
               join_payload, join_payload_len, &join_decoded,
               sizeof(join_decoded)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(join_decoded.left_relation.relation_name_len == 5 &&
           memcmp(join_decoded.left_relation.relation_name, "lefts", 5) == 0);
    assert(join_decoded.right_relation.relation_name_len == 6 &&
           memcmp(join_decoded.right_relation.relation_name, "rights", 6) == 0);
    assert(join_decoded.left_relation.scan_column_count == 3 &&
           join_decoded.right_relation.scan_column_count == 3 &&
           join_decoded.projection_count == 2 &&
           join_decoded.projections[0].relation_side == 0 &&
           join_decoded.projections[1].relation_side == 1 &&
           join_decoded.join_key_count == 2 && join_decoded.has_limit == 1 &&
           join_decoded.limit == 5);

    uint32_t filtered_side_offset;
    uint32_t filtered_column_offset;
    uint32_t filtered_index_offset;
    uint32_t filtered_join_payload_len = build_filtered_join_plan(
        filtered_join_payload, &filtered_side_offset, &filtered_column_offset,
        &filtered_index_offset);
    assert(prdb_sdk_optimizer_decode_filtered_join_plan_v1(
               filtered_join_payload, filtered_join_payload_len,
               &filtered_join_decoded, sizeof(filtered_join_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(filtered_join_decoded.join.right_relation.scan_column_count == 3 &&
           filtered_join_decoded.filter.relation_side == 1 &&
           filtered_join_decoded.filter.column.ordinal == 1 &&
           filtered_join_decoded.filter.column.type_code == 1 &&
           filtered_join_decoded.filter.expression_kind == 0 &&
           filtered_join_decoded.filter.comparison == 4 &&
           filtered_join_decoded.filter.parameter_index == 3);
    assert(prdb_sdk_optimizer_decode_join_plan_v1(
               filtered_join_payload, filtered_join_payload_len,
               &join_decoded, sizeof(join_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    memcpy(filtered_join_malformed, filtered_join_payload,
           filtered_join_payload_len);
    filtered_join_malformed[filtered_side_offset] = 2;
    assert(prdb_sdk_optimizer_decode_filtered_join_plan_v1(
               filtered_join_malformed, filtered_join_payload_len,
               &filtered_join_decoded, sizeof(filtered_join_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    memcpy(filtered_join_malformed, filtered_join_payload,
           filtered_join_payload_len);
    filtered_join_malformed[filtered_column_offset] = 0x7f;
    assert(prdb_sdk_optimizer_decode_filtered_join_plan_v1(
               filtered_join_malformed, filtered_join_payload_len,
               &filtered_join_decoded, sizeof(filtered_join_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    memcpy(filtered_join_malformed, filtered_join_payload,
           filtered_join_payload_len);
    filtered_join_malformed[filtered_index_offset + 3] = 0;
    assert(prdb_sdk_optimizer_decode_filtered_join_plan_v1(
               filtered_join_malformed, filtered_join_payload_len,
               &filtered_join_decoded, sizeof(filtered_join_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    assert(prdb_sdk_optimizer_decode_filtered_join_plan_v1(
               filtered_join_payload, filtered_join_payload_len + 1,
               &filtered_join_decoded, sizeof(filtered_join_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    uint32_t group_key_id_offset;
    uint32_t group_count_payload_len = build_group_count_plan(
        group_count_payload, &group_key_id_offset);
    assert(prdb_sdk_optimizer_decode_group_count_plan_v1(
               group_count_payload, group_count_payload_len,
               &group_count_decoded, sizeof(group_count_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(group_count_decoded.plan.plan_kind == 5 &&
           group_count_decoded.plan.scan_column_count == 1 &&
           group_count_decoded.group_key.column_id[0] == 4 &&
           group_count_decoded.plan.has_limit == 1 &&
           group_count_decoded.plan.limit == 5);
    static const uint8_t group_count_sql[] =
        "SELECT id, COUNT(*) FROM items GROUP BY id LIMIT 5";
    assert(prdb_sdk_sql_parse_select_v1(
               group_count_sql, sizeof(group_count_sql) - 1,
               &group_count_statement, sizeof(group_count_statement), NULL) ==
           PRDB_SDK_SQL_OK);
    group_count_binding.struct_size = sizeof(group_count_binding);
    group_count_binding.abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
    group_count_binding.struct_size = sizeof(group_count_binding);
    group_count_binding.abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
    group_count_binding.projection_count = 1;
    group_count_binding.projection_ordinals[0] = 0;
    group_count_binding.has_group_by = 1;
    group_count_binding.aggregate_kind = PRDB_SDK_SQL_AGGREGATE_COUNT_STAR;
    group_count_binding.group_by_column.ordinal = 0;
    group_count_binding.group_by_column.type_code = 1;
    assert(prdb_sdk_optimizer_match_group_count_plan_v1(
               &group_count_statement, &group_count_binding,
               (const uint8_t *)"public", 6, group_count_payload,
               group_count_payload_len) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    group_count_statement.limit = 4;
    assert(prdb_sdk_optimizer_match_group_count_plan_v1(
               &group_count_statement, &group_count_binding,
               (const uint8_t *)"public", 6, group_count_payload,
               group_count_payload_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    group_count_statement.limit = 5;
    memcpy(group_count_malformed, group_count_payload,
           group_count_payload_len);
    group_count_malformed[group_key_id_offset] = 5;
    assert(prdb_sdk_optimizer_decode_group_count_plan_v1(
               group_count_malformed, group_count_payload_len,
               &group_count_decoded, sizeof(group_count_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    uint32_t ordered_group_count_payload_len =
        build_ordered_group_count_plan(ordered_group_count_payload);
    assert(prdb_sdk_optimizer_decode_ordered_group_count_plan_v1(
               ordered_group_count_payload, ordered_group_count_payload_len,
               &ordered_group_count_decoded,
               sizeof(ordered_group_count_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(ordered_group_count_decoded.plan.plan_kind == 8 &&
           ordered_group_count_decoded.plan.has_limit == 1 &&
           ordered_group_count_decoded.plan.limit == 5 &&
           ordered_group_count_decoded.order_count == 2 &&
           ordered_group_count_decoded.order_items[0].source == 1 &&
           ordered_group_count_decoded.order_items[0].descending == 1 &&
           ordered_group_count_decoded.order_items[1].source == 0 &&
           ordered_group_count_decoded.order_items[1].nulls_first == 1);
    static const uint8_t ordered_group_count_sql[] =
        "SELECT id, COUNT(*) FROM items GROUP BY id "
        "ORDER BY COUNT(*) DESC NULLS LAST, id ASC NULLS FIRST LIMIT 5";
    ordered_group_count_columns[0].name = (const uint8_t *)"id";
    ordered_group_count_columns[0].name_len = 2;
    ordered_group_count_columns[0].ordinal = 0;
    ordered_group_count_columns[0].type_code = PRDB_SDK_VALUE_TYPE_INTEGER;
    assert(prdb_sdk_sql_parse_select_v1(
               ordered_group_count_sql, sizeof(ordered_group_count_sql) - 1,
               &ordered_group_count_statement,
               sizeof(ordered_group_count_statement), NULL) ==
           PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_select_v1(
               &ordered_group_count_statement, (const uint8_t *)"items", 5,
               ordered_group_count_columns, 1,
               &ordered_group_count_binding,
               sizeof(ordered_group_count_binding)) == PRDB_SDK_SQL_BIND_OK);
    int32_t ordered_match_status =
        prdb_sdk_optimizer_match_ordered_group_count_plan_v1(
               &ordered_group_count_statement, &ordered_group_count_binding,
               (const uint8_t *)"public", 6, ordered_group_count_payload,
               ordered_group_count_payload_len);
    assert(ordered_match_status == PRDB_SDK_OPTIMIZER_PLAN_OK);
    ordered_group_count_statement.order_items[0].direction =
        PRDB_SDK_SQL_ORDER_ASCENDING;
    assert(prdb_sdk_optimizer_match_ordered_group_count_plan_v1(
               &ordered_group_count_statement, &ordered_group_count_binding,
               (const uint8_t *)"public", 6, ordered_group_count_payload,
               ordered_group_count_payload_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);

    uint32_t composite_group_key_offset;
    uint32_t composite_group_count_payload_len =
        build_composite_group_count_plan(composite_group_count_payload,
                                         &composite_group_key_offset);
    assert(prdb_sdk_optimizer_decode_composite_group_count_plan_v1(
               composite_group_count_payload,
               composite_group_count_payload_len,
               &composite_group_count_decoded,
               sizeof(composite_group_count_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(composite_group_count_decoded.plan.plan_kind == 6 &&
           composite_group_count_decoded.plan.scan_column_count == 2 &&
           composite_group_count_decoded.group_key_count == 2 &&
           composite_group_count_decoded.group_keys[0].ordinal == 1 &&
           composite_group_count_decoded.group_keys[1].ordinal == 0 &&
           composite_group_count_decoded.plan.has_limit == 1 &&
           composite_group_count_decoded.plan.limit == 5);
    static const uint8_t composite_group_count_sql[] =
        "SELECT active, id, COUNT(*) FROM items GROUP BY active, id LIMIT 5";
    assert(prdb_sdk_sql_parse_select_v1(
               composite_group_count_sql,
               sizeof(composite_group_count_sql) - 1,
               &composite_group_count_statement,
               sizeof(composite_group_count_statement), NULL) ==
           PRDB_SDK_SQL_OK);
    composite_group_count_binding.struct_size =
        sizeof(composite_group_count_binding);
    composite_group_count_binding.abi_version = PRDB_SDK_SQL_ABI_V1_VERSION;
    composite_group_count_binding.struct_size =
        sizeof(composite_group_count_binding);
    composite_group_count_binding.abi_version =
        PRDB_SDK_SQL_ABI_V1_VERSION;
    composite_group_count_binding.struct_size =
        sizeof(composite_group_count_binding);
    composite_group_count_binding.abi_version =
        PRDB_SDK_SQL_ABI_V1_VERSION;
    composite_group_count_binding.group_key_count = 2;
    composite_group_count_binding.group_keys[0].ordinal = 1;
    composite_group_count_binding.group_keys[0].type_code = 1;
    composite_group_count_binding.group_keys[1].ordinal = 0;
    composite_group_count_binding.group_keys[1].type_code = 1;
    assert(prdb_sdk_optimizer_match_composite_group_count_plan_v1(
               &composite_group_count_statement,
               &composite_group_count_binding,
               (const uint8_t *)"public", 6,
               composite_group_count_payload,
               composite_group_count_payload_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    composite_group_count_statement.limit = 4;
    assert(prdb_sdk_optimizer_match_composite_group_count_plan_v1(
               &composite_group_count_statement,
               &composite_group_count_binding,
               (const uint8_t *)"public", 6,
               composite_group_count_payload,
               composite_group_count_payload_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    composite_group_count_statement.limit = 5;
    memcpy(composite_group_count_malformed, composite_group_count_payload,
           composite_group_count_payload_len);
    composite_group_count_malformed[composite_group_key_offset] = 6;
    assert(prdb_sdk_optimizer_decode_composite_group_count_plan_v1(
               composite_group_count_malformed,
               composite_group_count_payload_len,
               &composite_group_count_decoded,
               sizeof(composite_group_count_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    composite_group_aggregate_columns[0] =
        (prdb_sdk_sql_column_shape_v1){
            (const uint8_t *)"id", 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0};
    composite_group_aggregate_columns[1] =
        (prdb_sdk_sql_column_shape_v1){
            (const uint8_t *)"active", 6, 1, PRDB_SDK_VALUE_TYPE_BOOLEAN, 0, 0};
    composite_group_aggregate_columns[2] =
        (prdb_sdk_sql_column_shape_v1){
            (const uint8_t *)"amount", 6, 2, PRDB_SDK_VALUE_TYPE_BIGINT, 0, 0};
    static const uint8_t *const composite_group_aggregate_sql[] = {
        (const uint8_t *)"SELECT active, id, COUNT(*) FROM items "
                         "GROUP BY active, id LIMIT 5",
        (const uint8_t *)"SELECT active, id, MIN(amount) FROM items "
                         "GROUP BY active, id LIMIT 5",
        (const uint8_t *)"SELECT active, id, MAX(amount) FROM items "
                         "GROUP BY active, id LIMIT 5",
        (const uint8_t *)"SELECT active, id, SUM(amount) FROM items "
                         "GROUP BY active, id LIMIT 5",
        (const uint8_t *)"SELECT active, id, AVG(amount) FROM items "
                         "GROUP BY active, id LIMIT 5",
    };
    static const uint32_t composite_group_aggregate_sql_len[] = {
        sizeof("SELECT active, id, COUNT(*) FROM items "
               "GROUP BY active, id LIMIT 5") - 1,
        sizeof("SELECT active, id, MIN(amount) FROM items "
               "GROUP BY active, id LIMIT 5") - 1,
        sizeof("SELECT active, id, MAX(amount) FROM items "
               "GROUP BY active, id LIMIT 5") - 1,
        sizeof("SELECT active, id, SUM(amount) FROM items "
               "GROUP BY active, id LIMIT 5") - 1,
        sizeof("SELECT active, id, AVG(amount) FROM items "
               "GROUP BY active, id LIMIT 5") - 1,
    };
    for (uint8_t aggregate_kind = PRDB_SDK_SQL_AGGREGATE_COUNT_STAR;
         aggregate_kind <= PRDB_SDK_SQL_AGGREGATE_AVG; ++aggregate_kind) {
        uint32_t composite_group_aggregate_payload_len =
            build_composite_group_aggregate_plan(
                composite_group_aggregate_payload, aggregate_kind);
        assert(prdb_sdk_optimizer_decode_composite_group_aggregate_plan_v1(
                   composite_group_aggregate_payload,
                   composite_group_aggregate_payload_len,
                   &composite_group_aggregate_decoded,
                   sizeof(composite_group_aggregate_decoded)) ==
               PRDB_SDK_OPTIMIZER_PLAN_OK);
        assert(composite_group_aggregate_decoded.plan.plan_kind == 13 &&
               composite_group_aggregate_decoded.group_key_count == 2 &&
               composite_group_aggregate_decoded.aggregate_kind ==
                   aggregate_kind &&
               composite_group_aggregate_decoded.plan.scan_column_count ==
                   (aggregate_kind == PRDB_SDK_SQL_AGGREGATE_COUNT_STAR
                        ? 2
                        : 3) &&
               composite_group_aggregate_decoded.group_keys[0].ordinal == 1 &&
               composite_group_aggregate_decoded.group_keys[1].ordinal == 0 &&
               composite_group_aggregate_decoded.has_aggregate_column ==
                   (aggregate_kind != PRDB_SDK_SQL_AGGREGATE_COUNT_STAR) &&
               composite_group_aggregate_decoded.plan.limit == 5);
        if (aggregate_kind != PRDB_SDK_SQL_AGGREGATE_COUNT_STAR) {
            assert(composite_group_aggregate_decoded.aggregate_column.ordinal ==
                   2);
        }
        assert(prdb_sdk_sql_parse_select_v1(
                   composite_group_aggregate_sql[aggregate_kind - 1],
                   composite_group_aggregate_sql_len[aggregate_kind - 1],
                   &composite_group_aggregate_statement,
                   sizeof(composite_group_aggregate_statement), NULL) ==
               PRDB_SDK_SQL_OK);
        assert(prdb_sdk_sql_bind_select_v1(
                   &composite_group_aggregate_statement,
                   (const uint8_t *)"items", 5,
                   composite_group_aggregate_columns, 3,
                   &composite_group_aggregate_binding,
                   sizeof(composite_group_aggregate_binding)) ==
               PRDB_SDK_SQL_BIND_OK);
        assert(prdb_sdk_optimizer_match_composite_group_aggregate_plan_v1(
                   &composite_group_aggregate_statement,
                   &composite_group_aggregate_binding,
                   (const uint8_t *)"public", 6,
                   composite_group_aggregate_payload,
                   composite_group_aggregate_payload_len) ==
               PRDB_SDK_OPTIMIZER_PLAN_OK);
    }
    static const uint8_t count_column_composite_sql[] =
        "SELECT active, id, COUNT(amount) FROM items "
        "GROUP BY active, id LIMIT 5";
    assert(prdb_sdk_sql_parse_select_v1(
               count_column_composite_sql,
               sizeof(count_column_composite_sql) - 1,
               &count_column_statement, sizeof(count_column_statement), NULL) ==
           PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_select_v1(
               &count_column_statement, (const uint8_t *)"items", 5,
               composite_group_aggregate_columns, 3, &count_column_binding,
               sizeof(count_column_binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(count_column_statement.aggregate_kind ==
           PRDB_SDK_SQL_AGGREGATE_COUNT_COLUMN);
    assert(prdb_sdk_optimizer_encode_composite_group_aggregate_v1(
               &count_column_statement,
               (const uint8_t *)"public", 6, count_column_optimizer_request,
               sizeof(count_column_optimizer_request),
               &count_column_optimizer_request_len) ==
           PRDB_SDK_OPTIMIZER_ENCODE_OK);
    assert(count_column_optimizer_request_len > 30 &&
           count_column_optimizer_request[28] ==
               PRDB_SDK_SQL_AGGREGATE_COUNT_COLUMN);
    uint32_t count_column_plan_len = build_composite_group_aggregate_plan(
        composite_group_aggregate_payload,
        PRDB_SDK_SQL_AGGREGATE_COUNT_COLUMN);
    assert(prdb_sdk_optimizer_match_composite_group_aggregate_plan_v1(
               &count_column_statement,
               &count_column_binding,
               (const uint8_t *)"public", 6,
               composite_group_aggregate_payload, count_column_plan_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    uint32_t filtered_composite_group_aggregate_payload_len =
        build_filtered_composite_group_aggregate_plan(
            filtered_composite_group_aggregate_payload);
    assert(prdb_sdk_optimizer_decode_filtered_composite_group_aggregate_plan_v1(
               filtered_composite_group_aggregate_payload,
               filtered_composite_group_aggregate_payload_len,
               &filtered_composite_group_aggregate_decoded,
               sizeof(filtered_composite_group_aggregate_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(filtered_composite_group_aggregate_decoded.plan.plan_kind == 15 &&
           filtered_composite_group_aggregate_decoded.group_key_count == 2 &&
           filtered_composite_group_aggregate_decoded.aggregate_kind ==
               PRDB_SDK_SQL_AGGREGATE_SUM &&
           filtered_composite_group_aggregate_decoded.filter.present == 1 &&
           filtered_composite_group_aggregate_decoded.plan.scan_column_count == 3 &&
           filtered_composite_group_aggregate_decoded.filter.prefix_len == 21 &&
           filtered_composite_group_aggregate_decoded.plan.limit == 5);
    memcpy(malformed, filtered_composite_group_aggregate_payload,
           filtered_composite_group_aggregate_payload_len);
    malformed[filtered_composite_group_aggregate_payload_len - 32] ^= 1;
    assert(prdb_sdk_optimizer_decode_filtered_composite_group_aggregate_plan_v1(
               malformed, filtered_composite_group_aggregate_payload_len,
               &filtered_composite_group_aggregate_decoded,
               sizeof(filtered_composite_group_aggregate_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    {
        static const uint8_t filtered_composite_sql[] =
            "SELECT active, id, SUM(amount) FROM items WHERE active = $1 "
            "GROUP BY active, id LIMIT 5";
        uint8_t bool_value = 1;
        prdb_sdk_value_v1 parameters[1] = {{
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BOOLEAN,
            &bool_value, 1}};
        static prdb_sdk_select_v1 filtered_statement;
        static prdb_sdk_select_binding_v1 filtered_binding;
        assert(prdb_sdk_sql_parse_select_v1(
                   filtered_composite_sql, sizeof(filtered_composite_sql) - 1,
                   &filtered_statement, sizeof(filtered_statement), NULL) ==
               PRDB_SDK_SQL_OK);
        assert(prdb_sdk_sql_bind_select_v1(
                   &filtered_statement, (const uint8_t *)"items", 5,
                   composite_group_aggregate_columns, 3, &filtered_binding,
                   sizeof(filtered_binding)) == PRDB_SDK_SQL_BIND_OK);
        assert(prdb_sdk_optimizer_match_filtered_composite_group_aggregate_plan_v1(
                   &filtered_statement, &filtered_binding, parameters, 1,
                   (const uint8_t *)"public", 6,
                   filtered_composite_group_aggregate_payload,
                   filtered_composite_group_aggregate_payload_len) ==
               PRDB_SDK_OPTIMIZER_PLAN_OK);
        filtered_composite_group_aggregate_payload[
            (uint32_t)(filtered_composite_group_aggregate_decoded.filter.prefix -
                       filtered_composite_group_aggregate_payload) +
            filtered_composite_group_aggregate_decoded.filter.prefix_len - 1] ^= 1;
        assert(prdb_sdk_optimizer_match_filtered_composite_group_aggregate_plan_v1(
                   &filtered_statement, &filtered_binding, parameters, 1,
                   (const uint8_t *)"public", 6,
                   filtered_composite_group_aggregate_payload,
                   filtered_composite_group_aggregate_payload_len) ==
               PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    }
    uint32_t ordered_composite_group_count_payload_len =
        build_ordered_composite_group_count_plan(ordered_composite_group_count_payload);
    assert(prdb_sdk_optimizer_decode_ordered_composite_group_count_plan_v1(
               ordered_composite_group_count_payload, ordered_composite_group_count_payload_len,
               &ordered_composite_group_count_decoded, sizeof(ordered_composite_group_count_decoded)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(ordered_composite_group_count_decoded.plan.plan_kind == 9 &&
           ordered_composite_group_count_decoded.group_key_count == 2 &&
           ordered_composite_group_count_decoded.order_count == 3 &&
           ordered_composite_group_count_decoded.order_items[0].group_key_index == 0 &&
           ordered_composite_group_count_decoded.order_items[1].source == 1 &&
           ordered_composite_group_count_decoded.plan.limit == 5);
    static const uint8_t ordered_composite_group_count_sql[] =
        "SELECT active, id, COUNT(*) FROM items GROUP BY active, id "
        "ORDER BY active DESC NULLS LAST, COUNT(*) DESC, "
        "id ASC NULLS FIRST LIMIT 5";
    ordered_composite_group_count_columns[0] = (prdb_sdk_sql_column_shape_v1){
        (const uint8_t *)"id", 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0};
    ordered_composite_group_count_columns[1] = (prdb_sdk_sql_column_shape_v1){
        (const uint8_t *)"active", 6, 1, PRDB_SDK_VALUE_TYPE_BOOLEAN, 0, 0};
    assert(prdb_sdk_sql_parse_select_v1(ordered_composite_group_count_sql,
               sizeof(ordered_composite_group_count_sql) - 1,
               &ordered_composite_group_count_statement,
               sizeof(ordered_composite_group_count_statement), NULL) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_select_v1(&ordered_composite_group_count_statement,
               (const uint8_t *)"items", 5, ordered_composite_group_count_columns, 2,
               &ordered_composite_group_count_binding,
               sizeof(ordered_composite_group_count_binding)) == PRDB_SDK_SQL_BIND_OK);
    int32_t ordered_composite_match_status = prdb_sdk_optimizer_match_ordered_composite_group_count_plan_v1(
               &ordered_composite_group_count_statement, &ordered_composite_group_count_binding,
               (const uint8_t *)"public", 6, ordered_composite_group_count_payload,
               ordered_composite_group_count_payload_len);
    assert(ordered_composite_match_status == PRDB_SDK_OPTIMIZER_PLAN_OK);
    ordered_composite_group_count_statement.order_items[0].direction = PRDB_SDK_SQL_ORDER_ASCENDING;
    assert(prdb_sdk_optimizer_match_ordered_composite_group_count_plan_v1(
               &ordered_composite_group_count_statement, &ordered_composite_group_count_binding,
               (const uint8_t *)"public", 6, ordered_composite_group_count_payload,
               ordered_composite_group_count_payload_len) == PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    uint32_t group_aggregate_payload_len = build_group_aggregate_plan(group_aggregate_payload);
    assert(prdb_sdk_optimizer_decode_group_aggregate_plan_v1(group_aggregate_payload,
               group_aggregate_payload_len, &group_aggregate_decoded, sizeof(group_aggregate_decoded)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(group_aggregate_decoded.plan.plan_kind == 10 && group_aggregate_decoded.aggregate_kind == PRDB_SDK_SQL_AGGREGATE_AVG &&
           group_aggregate_decoded.has_aggregate_column == 1 && group_aggregate_decoded.plan.scan_column_count == 2);
    static const uint8_t group_aggregate_sql[] = "SELECT id, AVG(amount) FROM items GROUP BY id LIMIT 5";
    group_aggregate_columns[0] = (prdb_sdk_sql_column_shape_v1){(const uint8_t *)"id", 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0};
    group_aggregate_columns[1] = (prdb_sdk_sql_column_shape_v1){(const uint8_t *)"amount", 6, 1, PRDB_SDK_VALUE_TYPE_BIGINT, 0, 0};
    assert(prdb_sdk_sql_parse_select_v1(group_aggregate_sql, sizeof(group_aggregate_sql) - 1,
               &group_aggregate_statement, sizeof(group_aggregate_statement), NULL) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_select_v1(&group_aggregate_statement, (const uint8_t *)"items", 5,
               group_aggregate_columns, 2, &group_aggregate_binding, sizeof(group_aggregate_binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(prdb_sdk_optimizer_match_group_aggregate_plan_v1(&group_aggregate_statement, &group_aggregate_binding,
               (const uint8_t *)"public", 6, group_aggregate_payload, group_aggregate_payload_len) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    group_aggregate_statement.limit = 4;
    assert(prdb_sdk_optimizer_match_group_aggregate_plan_v1(&group_aggregate_statement, &group_aggregate_binding,
               (const uint8_t *)"public", 6, group_aggregate_payload, group_aggregate_payload_len) == PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    static const uint8_t *const ordered_group_aggregate_sql[] = {
        (const uint8_t *)"SELECT id, COUNT(*) FROM items GROUP BY id "
                         "ORDER BY COUNT(*) DESC NULLS LAST, id ASC NULLS FIRST LIMIT 5",
        (const uint8_t *)"SELECT id, MIN(amount) FROM items GROUP BY id "
                         "ORDER BY MIN(amount) DESC NULLS LAST, id ASC NULLS FIRST LIMIT 5",
        (const uint8_t *)"SELECT id, MAX(amount) FROM items GROUP BY id "
                         "ORDER BY MAX(amount) DESC NULLS LAST, id ASC NULLS FIRST LIMIT 5",
        (const uint8_t *)"SELECT id, SUM(amount) FROM items GROUP BY id "
                         "ORDER BY SUM(amount) DESC NULLS LAST, id ASC NULLS FIRST LIMIT 5",
        (const uint8_t *)"SELECT id, AVG(amount) FROM items GROUP BY id "
                         "ORDER BY AVG(amount) DESC NULLS LAST, id ASC NULLS FIRST LIMIT 5",
    };
    static const uint32_t ordered_group_aggregate_sql_len[] = {
        sizeof("SELECT id, COUNT(*) FROM items GROUP BY id "
               "ORDER BY COUNT(*) DESC NULLS LAST, id ASC NULLS FIRST LIMIT 5") - 1,
        sizeof("SELECT id, MIN(amount) FROM items GROUP BY id "
               "ORDER BY MIN(amount) DESC NULLS LAST, id ASC NULLS FIRST LIMIT 5") - 1,
        sizeof("SELECT id, MAX(amount) FROM items GROUP BY id "
               "ORDER BY MAX(amount) DESC NULLS LAST, id ASC NULLS FIRST LIMIT 5") - 1,
        sizeof("SELECT id, SUM(amount) FROM items GROUP BY id "
               "ORDER BY SUM(amount) DESC NULLS LAST, id ASC NULLS FIRST LIMIT 5") - 1,
        sizeof("SELECT id, AVG(amount) FROM items GROUP BY id "
               "ORDER BY AVG(amount) DESC NULLS LAST, id ASC NULLS FIRST LIMIT 5") - 1,
    };
    uint32_t ordered_group_aggregate_order_offset = 0;
    uint32_t ordered_group_aggregate_payload_len = 0;
    for (uint8_t aggregate_kind = PRDB_SDK_SQL_AGGREGATE_COUNT_STAR;
         aggregate_kind <= PRDB_SDK_SQL_AGGREGATE_AVG; ++aggregate_kind) {
        ordered_group_aggregate_payload_len =
            build_ordered_group_aggregate_plan(
                ordered_group_aggregate_payload,
                &ordered_group_aggregate_order_offset, aggregate_kind);
        assert(prdb_sdk_optimizer_decode_ordered_group_aggregate_plan_v1(
                   ordered_group_aggregate_payload,
                   ordered_group_aggregate_payload_len,
                   &ordered_group_aggregate_decoded,
                   sizeof(ordered_group_aggregate_decoded)) ==
               PRDB_SDK_OPTIMIZER_PLAN_OK);
        assert(ordered_group_aggregate_decoded.plan.plan_kind == 11 &&
               ordered_group_aggregate_decoded.aggregate_kind ==
                   aggregate_kind &&
               ordered_group_aggregate_decoded.order_count == 2 &&
               ordered_group_aggregate_decoded.order_items[0].source == 1 &&
               ordered_group_aggregate_decoded.order_items[1].source == 0);
        assert(prdb_sdk_sql_parse_select_v1(
                   ordered_group_aggregate_sql[aggregate_kind - 1],
                   ordered_group_aggregate_sql_len[aggregate_kind - 1],
                   &ordered_group_aggregate_statement,
                   sizeof(ordered_group_aggregate_statement), NULL) ==
               PRDB_SDK_SQL_OK);
        assert(prdb_sdk_sql_bind_select_v1(
                   &ordered_group_aggregate_statement,
                   (const uint8_t *)"items", 5, group_aggregate_columns, 2,
                   &ordered_group_aggregate_binding,
                   sizeof(ordered_group_aggregate_binding)) ==
               PRDB_SDK_SQL_BIND_OK);
        assert(prdb_sdk_optimizer_match_ordered_group_aggregate_plan_v1(
                   &ordered_group_aggregate_statement,
                   &ordered_group_aggregate_binding,
                   (const uint8_t *)"public", 6,
                   ordered_group_aggregate_payload,
                   ordered_group_aggregate_payload_len) ==
               PRDB_SDK_OPTIMIZER_PLAN_OK);
    }
    ordered_group_aggregate_statement.limit = 4;
    assert(prdb_sdk_optimizer_match_ordered_group_aggregate_plan_v1(
               &ordered_group_aggregate_statement,
               &ordered_group_aggregate_binding, (const uint8_t *)"public", 6,
               ordered_group_aggregate_payload,
               ordered_group_aggregate_payload_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    ordered_group_aggregate_statement.limit = 5;
    memcpy(group_aggregate_malformed, ordered_group_aggregate_payload,
           ordered_group_aggregate_payload_len);
    group_aggregate_malformed[ordered_group_aggregate_order_offset] = 0;
    assert(prdb_sdk_optimizer_decode_ordered_group_aggregate_plan_v1(
               group_aggregate_malformed,
               ordered_group_aggregate_payload_len,
               &ordered_group_aggregate_decoded,
               sizeof(ordered_group_aggregate_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    static const uint8_t join_sql[] =
        "SELECT l.value, r.value FROM lefts AS l INNER JOIN rights AS r "
        "ON l.id = r.id AND l.active = r.active LIMIT 5;";
    static const uint8_t filtered_join_sql[] =
        "SELECT l.value, r.value FROM lefts AS l INNER JOIN rights AS r "
        "ON l.id = r.id AND l.active = r.active WHERE r.active > $3 LIMIT 5;";
    static const uint8_t join_left_name[] = "lefts";
    static const uint8_t join_right_name[] = "rights";
    static const uint8_t join_id_name[] = "id";
    static const uint8_t join_active_name[] = "active";
    static const uint8_t join_value_name[] = "value";
    const uint8_t *join_column_names[3] = {
        join_id_name, join_active_name, join_value_name};
    const uint32_t join_column_name_lengths[3] = {2, 6, 5};
    const uint32_t join_column_types[3] = {
        PRDB_SDK_VALUE_TYPE_INTEGER, PRDB_SDK_VALUE_TYPE_BOOLEAN,
        PRDB_SDK_VALUE_TYPE_TEXT};
    for (uint32_t index = 0; index < 3; ++index) {
        join_left_columns[index] = (prdb_sdk_sql_column_shape_v1){
            join_column_names[index], join_column_name_lengths[index], index,
            join_column_types[index], 0, 0};
        join_right_columns[index] = join_left_columns[index];
    }
    uint32_t join_error_offset = 0;
    assert(prdb_sdk_sql_parse_select_v1(
               join_sql, sizeof(join_sql) - 1, &join_statement,
               sizeof(join_statement), &join_error_offset) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_join_select_v1(
               &join_statement, join_left_name, sizeof(join_left_name) - 1,
               join_left_columns, 3, join_right_name,
               sizeof(join_right_name) - 1, join_right_columns, 3,
               &join_binding, sizeof(join_binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(prdb_sdk_optimizer_match_join_plan_v19_v1(
               &join_statement, &join_binding, (const uint8_t *)"public", 6,
               join_payload, join_payload_len) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    for (uint32_t index = 0; index < 3; ++index) {
        filtered_join_parameters[index] = (prdb_sdk_value_v1){
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BOOLEAN,
            filtered_join_parameter_bytes[index], 1};
    }
    assert(prdb_sdk_sql_parse_select_v1(
               filtered_join_sql, sizeof(filtered_join_sql) - 1,
               &filtered_join_statement, sizeof(filtered_join_statement),
               &join_error_offset) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_join_select_v1(
               &filtered_join_statement, join_left_name,
               sizeof(join_left_name) - 1, join_left_columns, 3,
               join_right_name, sizeof(join_right_name) - 1,
               join_right_columns, 3, &filtered_join_binding,
               sizeof(filtered_join_binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(prdb_sdk_optimizer_match_filtered_join_plan_v20_v1(
               &filtered_join_statement, &filtered_join_binding,
               filtered_join_parameters, 3, (const uint8_t *)"public", 6,
               filtered_join_payload, filtered_join_payload_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    filtered_join_statement.join_where_comparison = PRDB_SDK_SQL_COMPARE_EQUAL;
    assert(prdb_sdk_optimizer_match_filtered_join_plan_v20_v1(
               &filtered_join_statement, &filtered_join_binding,
               filtered_join_parameters, 3, (const uint8_t *)"public", 6,
               filtered_join_payload, filtered_join_payload_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    filtered_join_statement.join_where_comparison = PRDB_SDK_SQL_COMPARE_GREATER;
    filtered_join_statement.join_where_parameter_index = 2;
    assert(prdb_sdk_optimizer_match_filtered_join_plan_v20_v1(
               &filtered_join_statement, &filtered_join_binding,
               filtered_join_parameters, 3, (const uint8_t *)"public", 6,
               filtered_join_payload, filtered_join_payload_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    filtered_join_statement.join_where_parameter_index = 3;
    filtered_join_parameters[2].type_code = PRDB_SDK_VALUE_TYPE_INTEGER;
    assert(prdb_sdk_optimizer_match_filtered_join_plan_v20_v1(
               &filtered_join_statement, &filtered_join_binding,
               filtered_join_parameters, 3, (const uint8_t *)"public", 6,
               filtered_join_payload, filtered_join_payload_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    filtered_join_parameters[2].type_code = PRDB_SDK_VALUE_TYPE_BOOLEAN;
    join_statement.limit = 6;
    assert(prdb_sdk_optimizer_match_join_plan_v19_v1(
               &join_statement, &join_binding, (const uint8_t *)"public", 6,
               join_payload, join_payload_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    join_statement.limit = 5;
    resolve.calls = 0;
    assert(prdb_sdk_optimizer_validate_executor_join_relations_v1(
               &host, join_payload, join_payload_len, response,
               sizeof(response), &response_len) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.calls == 2);
    resolve.calls = 0;
    resolve.fail_join_identity = 1;
    assert(prdb_sdk_optimizer_validate_executor_join_relations_v1(
               &host, join_payload, join_payload_len, response,
               sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    assert(resolve.calls == 2);
    resolve.fail_join_identity = 0;
    resolve.calls = 0;
    assert(prdb_sdk_optimizer_validate_executor_filtered_join_relations_v1(
               &host, filtered_join_payload, filtered_join_payload_len,
               response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.calls == 2);
    resolve.calls = 0;
    resolve.fail_join_identity = 1;
    assert(prdb_sdk_optimizer_validate_executor_filtered_join_relations_v1(
               &host, filtered_join_payload, filtered_join_payload_len,
               response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    assert(resolve.calls == 2);
    resolve.fail_join_identity = 0;
    memcpy(join_malformed, join_payload, join_payload_len);
    join_malformed[join_projection_side_offset] = 2;
    assert(prdb_sdk_optimizer_decode_join_plan_v1(
               join_malformed, join_payload_len, &join_decoded,
               sizeof(join_decoded)) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    memcpy(join_malformed, join_payload, join_payload_len);
    join_malformed[join_right_key_type_offset] = 6;
    assert(prdb_sdk_optimizer_decode_join_plan_v1(
               join_malformed, join_payload_len, &join_decoded,
               sizeof(join_decoded)) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    memcpy(join_malformed, join_payload, join_payload_len);
    join_malformed[join_payload_len] = 0;
    assert(prdb_sdk_optimizer_decode_join_plan_v1(
               join_malformed, join_payload_len + 1, &join_decoded,
               sizeof(join_decoded)) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    assert(prdb_sdk_optimizer_validate_executor_relation_v1(
               &host, update_plan, update_plan_len, response,
               sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(prdb_sdk_optimizer_decode_update_plan_v1(
               update_plan, update_plan_len - 1, &update_decoded,
               sizeof(update_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    memcpy(malformed, update_plan, update_plan_len);
    malformed[update_plan_len - 5] = 9; /* unsupported assignment kind */
    assert(prdb_sdk_optimizer_decode_update_plan_v1(
               malformed, update_plan_len, &update_decoded,
               sizeof(update_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    memcpy(malformed, update_plan, update_plan_len);
    malformed[update_plan_len - 1] = 1; /* NULL must have zero parameter index */
    malformed[update_plan_len - 5] = PRDB_OPTIMIZER_UPDATE_NULL_VALUE;
    assert(prdb_sdk_optimizer_decode_update_plan_v1(
               malformed, update_plan_len, &update_decoded,
               sizeof(update_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    assert(prdb_sdk_optimizer_decode_update_plan_v1(
               update_plan, update_plan_len,
               (prdb_sdk_optimizer_update_plan_v1 *)update_plan,
               sizeof(update_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT);

    uint8_t update_sql[] = "UPDATE items SET c0 = $2 WHERE c0 = $1";
    uint8_t update_parameter_bytes[2][4] = {{0, 0, 0, 1}, {0, 0, 0, 2}};
    prdb_sdk_value_v1 update_parameters[2] = {
        {sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
         PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
         update_parameter_bytes[0], sizeof(update_parameter_bytes[0])},
        {sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
         PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
         update_parameter_bytes[1], sizeof(update_parameter_bytes[1])}};
    assert(prdb_sdk_sql_parse_update_v1(
               update_sql, sizeof(update_sql) - 1, &update_statement,
               sizeof(update_statement), NULL) == PRDB_SDK_SQL_OK);
    resolve.calls = 0;
    assert(prdb_sdk_optimizer_validate_update_plan_with_parameters_v1(
               &host, &update_statement, update_parameters, 2,
               (const uint8_t *)"public", 6, update_plan, update_plan_len,
               response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.calls == 1);
    update_statement.assignments[0].parameter_index = 1;
    assert(prdb_sdk_optimizer_validate_update_plan_with_parameters_v1(
               &host, &update_statement, update_parameters, 2,
               (const uint8_t *)"public", 6, update_plan, update_plan_len,
               response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    update_statement.assignments[0].parameter_index = 2;
    update_parameter_bytes[1][0] = 0;
    update_parameters[1].type_code = PRDB_SDK_VALUE_TYPE_BOOLEAN;
    assert(prdb_sdk_optimizer_validate_update_plan_with_parameters_v1(
               &host, &update_statement, update_parameters, 2,
               (const uint8_t *)"public", 6, update_plan, update_plan_len,
               response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    update_parameters[1].type_code = PRDB_SDK_VALUE_TYPE_INTEGER;
    update_parameter_bytes[1][0] = 0;

    uint8_t delete_plan[512];
    uint32_t delete_plan_len = build_delete_filter_plan(
        delete_plan, &predicate_offset);
    uint8_t delete_sql[] = "DELETE FROM items WHERE c0 = $1";
    uint8_t delete_parameter_bytes[4] = {0, 0, 0, 1};
    prdb_sdk_value_v1 delete_parameters[1] = {{
        sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
        delete_parameter_bytes, sizeof(delete_parameter_bytes)}};
    assert(prdb_sdk_sql_parse_delete_v1(
               delete_sql, sizeof(delete_sql) - 1, &delete_statement,
               sizeof(delete_statement), NULL) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_optimizer_decode_delete_plan_v1(
               delete_plan, delete_plan_len, &decoded, sizeof(decoded),
               &filter, sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    resolve.calls = 0;
    int32_t delete_validation_status =
        prdb_sdk_optimizer_validate_delete_plan_with_parameters_v1(
               &host, &delete_statement, delete_parameters, 1,
               (const uint8_t *)"public", 6, delete_plan, delete_plan_len,
               response, sizeof(response), &response_len);
    assert(delete_validation_status == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.calls == 1);
    assert(prdb_sdk_optimizer_acquire_plan_relation_v1(
               &host, delete_plan, delete_plan_len, response, sizeof(response),
               &response_len, &acquired_relation, sizeof(acquired_relation),
               acquired_columns, 4) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(acquired_relation.name_len == 5 &&
           memcmp(acquired_relation.name, "items", 5) == 0);
    delete_parameter_bytes[3] = 2;
    assert(prdb_sdk_optimizer_validate_delete_plan_with_parameters_v1(
               &host, &delete_statement, delete_parameters, 1,
               (const uint8_t *)"public", 6, delete_plan, delete_plan_len,
               response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    delete_parameter_bytes[3] = 1;
    uint8_t wrong_delete_sql[] = "DELETE FROM items WHERE c1 = $1";
    assert(prdb_sdk_sql_parse_delete_v1(
               wrong_delete_sql, sizeof(wrong_delete_sql) - 1,
               &delete_statement, sizeof(delete_statement), NULL) ==
           PRDB_SDK_SQL_OK);
    assert(prdb_sdk_optimizer_validate_delete_plan_with_parameters_v1(
               &host, &delete_statement, delete_parameters, 1,
               (const uint8_t *)"public", 6, delete_plan, delete_plan_len,
               response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);

    payload_len = build_filter_plan(payload, &predicate_offset);
    memcpy(malformed, payload, payload_len);
    malformed[predicate_offset] = 0x10; /* INTEGER is not a Boolean root. */
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               malformed, payload_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    memcpy(malformed, payload, payload_len);
    malformed[predicate_offset + 19] = 0x01; /* Boolean versus INTEGER. */
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               malformed, payload_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    memcpy(malformed, payload, payload_len);
    malformed[predicate_offset] = 0x03;
    malformed[predicate_offset + 1] = 0;
    malformed[predicate_offset + 2] = 1;
    malformed[predicate_offset + 3] = 0xff;
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               malformed, payload_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               payload, payload_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter) - 1) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT);

    probe_plan_len = build_unique_probe_plan(probe_plan,
                                              &probe_predicate_offset);
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               probe_plan, probe_plan_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(filter.present == 1 && filter.prefix_len != 0);
    probe_plan[probe_plan_len - 1] = 2;
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               probe_plan, probe_plan_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    probe_plan[probe_plan_len - 1] = 1;
    probe_plan[probe_plan_len - 9] = 0x01;
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               probe_plan, probe_plan_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    probe_plan[probe_plan_len - 9] = 0x02;
    memset(probe_plan + probe_plan_len - 9 - 26 - 16, 0, 16);
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               probe_plan, probe_plan_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    probe_plan_len = build_multi_point_probe_plan(probe_plan);
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               probe_plan, probe_plan_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    probe_plan[probe_plan_len - 1] = 3;
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               probe_plan, probe_plan_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    payload_len = build_plan(payload, &projected_id_offset,
                             &project_node_offset);
    assert(prdb_sdk_optimizer_decode_plan_v1(
               payload, payload_len, &decoded, sizeof(decoded) - 1) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT);
    assert(prdb_sdk_optimizer_decode_plan_v1(
               payload, payload_len, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    resolve.calls = 0;
    assert(prdb_sdk_optimizer_acquire_plan_relation_v1(
               &host, payload, payload_len, response, sizeof(response),
               &response_len, &acquired_relation, sizeof(acquired_relation),
               acquired_columns, 4) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.calls == 2);
    assert(acquired_relation.relation_id[0] == 2);
    assert(acquired_relation.executor_handle[0] == 6);
    assert(acquired_relation.column_count == 4);
    assert(acquired_columns[3].ordinal == 3 &&
           acquired_columns[3].type_code == 6);
    uint8_t cursor[16] = {0};
    uint32_t scan_opened = 0;
    resolve.calls = 0;
    assert(prdb_sdk_optimizer_open_plan_scan_v1(
               &host, payload, payload_len, response, sizeof(response),
               &response_len, malformed, sizeof(malformed),
               &scan_response_len,
               &acquired_relation, sizeof(acquired_relation), acquired_columns,
               4, cursor, &scan_opened) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.calls == 3 && scan_opened == 1 && cursor[0] == 7);
    uint8_t zero_limit_plan[sizeof(payload)];
    memcpy(zero_limit_plan, payload, payload_len);
    zero_limit_plan[payload_len - 1] = 0;
    resolve.calls = 0;
    assert(prdb_sdk_optimizer_open_plan_scan_v1(
               &host, zero_limit_plan, payload_len, response, sizeof(response),
               &response_len, malformed, sizeof(malformed),
               &scan_response_len,
               &acquired_relation, sizeof(acquired_relation), acquired_columns,
               4, cursor, &scan_opened) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.calls == 2 && scan_opened == 0 && cursor[0] == 0);
    resolve.executor_response[3] = 9;
    assert(prdb_sdk_optimizer_acquire_plan_relation_v1(
               &host, payload, payload_len, response, sizeof(response),
               &response_len, &acquired_relation, sizeof(acquired_relation),
               acquired_columns, 4) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    resolve.executor_response[3] = 2;

    uint8_t execute_delete_sql[] = "DELETE FROM items WHERE c0 = $1";
    uint8_t execute_delete_parameter_bytes[4] = {0, 0, 0, 1};
    prdb_sdk_value_v1 execute_delete_parameters[1] = {{
        sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
        execute_delete_parameter_bytes,
        sizeof(execute_delete_parameter_bytes)}};
    uint8_t execute_delete_plan[512];
    uint32_t execute_delete_plan_len = build_delete_filter_plan(
        execute_delete_plan, &predicate_offset);
    uint32_t deleted_count = 0;
    assert(prdb_sdk_sql_parse_delete_v1(
               execute_delete_sql, sizeof(execute_delete_sql) - 1,
               &delete_statement,
               sizeof(delete_statement), NULL) == PRDB_SDK_SQL_OK);
    resolve.optimizer_response_len = build_optimizer_response(
        resolve.optimizer_response, execute_delete_plan,
        execute_delete_plan_len);
    resolve.batch_response_len = build_row_batch(resolve.batch_response);
    resolve.calls = 0;
    resolve.optimizer_calls = 0;
    resolve.stage_delete_calls = 0;
    resolve.statement_failures = 0;
    resolve.event_count = 0;
    host_v5 = host_v4;
    host_v5.optimizer_request = optimizer_request_v5;
    assert(prdb_sdk_optimizer_encode_delete_v1(
               &delete_statement, execute_delete_parameters, 1,
               (const uint8_t *)"public", 6, request, sizeof(request),
               &response_len) == PRDB_SDK_OPTIMIZER_ENCODE_OK);
    assert(prdb_sdk_optimizer_execute_delete_ast_v1(
               &host_v5, &delete_statement, execute_delete_parameters, 1,
               (const uint8_t *)"public", 6, 2, sizeof(batch_response), request,
               sizeof(request), optimizer_response, sizeof(optimizer_response),
               response, sizeof(response), scan_response, sizeof(scan_response),
               batch_response, sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, &delete_binding,
               sizeof(delete_binding), delete_cell_scratch, 4,
               delete_target_handles,
               PRDB_SDK_OPTIMIZER_PLAN_MAX_DELETE_TARGETS,
               &deleted_count) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(deleted_count == 1 && resolve.optimizer_calls == 1 &&
           resolve.stage_delete_calls == 1 && resolve.statement_failures == 0);
    assert(resolve.calls == 7);

    uint8_t execute_update_sql[] =
        "UPDATE items SET c0 = $2 WHERE c0 = $1";
    static prdb_sdk_update_v1 execute_update_statement;
    assert(prdb_sdk_sql_parse_update_v1(
               execute_update_sql, sizeof(execute_update_sql) - 1,
               &execute_update_statement, sizeof(execute_update_statement),
               NULL) == PRDB_SDK_SQL_OK);
    uint8_t execute_update_plan[512];
    uint32_t execute_update_plan_len = build_update_plan(execute_update_plan);
    resolve.optimizer_response_len = build_optimizer_response(
        resolve.optimizer_response, execute_update_plan,
        execute_update_plan_len);
    resolve.calls = 0;
    resolve.optimizer_calls = 0;
    resolve.stage_update_calls = 0;
    resolve.statement_failures = 0;
    resolve.event_count = 0;
    uint32_t updated_count = 0;
    assert(prdb_sdk_optimizer_execute_update_ast_v1(
               &host_v5, &execute_update_statement, update_parameters, 2,
               (const uint8_t *)"public", 6, 2, sizeof(batch_response),
               request, sizeof(request), optimizer_response,
               sizeof(optimizer_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, &delete_binding,
               sizeof(delete_binding), delete_cell_scratch, 4,
               delete_target_handles,
               PRDB_SDK_OPTIMIZER_PLAN_MAX_DELETE_TARGETS,
               &updated_count) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(updated_count == 1 && resolve.optimizer_calls == 1 &&
           resolve.stage_update_calls == 1 && resolve.statement_failures == 0);
    assert(resolve.calls == 7);

    resolve.calls = 0;
    resolve.optimizer_calls = 0;
    resolve.stage_update_calls = 0;
    resolve.statement_failures = 0;
    resolve.event_count = 0;
    resolve.cancellation_response[2] = 1;
    updated_count = 99;
    assert(prdb_sdk_optimizer_execute_update_ast_v1(
               &host_v5, &execute_update_statement, update_parameters, 2,
               (const uint8_t *)"public", 6, 2, sizeof(batch_response),
               request, sizeof(request), optimizer_response,
               sizeof(optimizer_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, &delete_binding,
               sizeof(delete_binding), delete_cell_scratch, 4,
               delete_target_handles,
               PRDB_SDK_OPTIMIZER_PLAN_MAX_DELETE_TARGETS,
               &updated_count) == PRDB_SDK_OPTIMIZER_PLAN_CANCELLED);
    assert(updated_count == 0 && resolve.optimizer_calls == 1 &&
           resolve.stage_update_calls == 0 && resolve.statement_failures == 1);
    assert(resolve.calls == 6);
    assert(resolve.events[6] == PRDB_SDK_EXECUTOR_OP_CLOSE_CURSOR);
    resolve.cancellation_response[2] = 0;

    resolve.calls = 0;
    resolve.optimizer_calls = 0;
    resolve.stage_update_calls = 0;
    resolve.statement_failures = 0;
    resolve.event_count = 0;
    resolve.fail_stage_update = 1;
    updated_count = 99;
    assert(prdb_sdk_optimizer_execute_update_ast_v1(
               &host_v5, &execute_update_statement, update_parameters, 2,
               (const uint8_t *)"public", 6, 2, sizeof(batch_response),
               request, sizeof(request), optimizer_response,
               sizeof(optimizer_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, &delete_binding,
               sizeof(delete_binding), delete_cell_scratch, 4,
               delete_target_handles,
               PRDB_SDK_OPTIMIZER_PLAN_MAX_DELETE_TARGETS,
               &updated_count) == PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR);
    assert(updated_count == 0 && resolve.optimizer_calls == 1 &&
           resolve.stage_update_calls == 1 && resolve.statement_failures == 1);
    assert(resolve.calls == 7);
    assert(resolve.events[6] == PRDB_SDK_EXECUTOR_OP_NEXT_BATCH);
    assert(resolve.events[7] == PRDB_SDK_EXECUTOR_OP_STAGE_UPDATE);
    resolve.fail_stage_update = 0;

    resolve.calls = 0;
    resolve.optimizer_calls = 0;
    resolve.stage_update_calls = 0;
    resolve.statement_failures = 0;
    resolve.event_count = 0;
    resolve.fail_optimizer_request = 1;
    updated_count = 99;
    assert(prdb_sdk_optimizer_execute_update_ast_v1(
               &host_v5, &execute_update_statement, update_parameters, 2,
               (const uint8_t *)"public", 6, 2, sizeof(batch_response),
               request, sizeof(request), optimizer_response,
               sizeof(optimizer_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, &delete_binding,
               sizeof(delete_binding), delete_cell_scratch, 4,
               delete_target_handles,
               PRDB_SDK_OPTIMIZER_PLAN_MAX_DELETE_TARGETS,
               &updated_count) == PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR);
    assert(updated_count == 0 && resolve.optimizer_calls == 1 &&
           resolve.calls == 0 && resolve.stage_update_calls == 0 &&
           resolve.statement_failures == 1);
    assert(resolve.event_count == 1 && resolve.events[0] == UINT16_C(0xffff));
    resolve.fail_optimizer_request = 0;

    resolve.batch_response_len = build_two_row_batch(resolve.batch_response);
    resolve.calls = 0;
    resolve.optimizer_calls = 0;
    resolve.stage_update_calls = 0;
    resolve.statement_failures = 0;
    resolve.event_count = 0;
    updated_count = 99;
    assert(prdb_sdk_optimizer_execute_update_ast_v1(
               &host_v5, &execute_update_statement, update_parameters, 2,
               (const uint8_t *)"public", 6, 2, sizeof(batch_response),
               request, sizeof(request), optimizer_response,
               sizeof(optimizer_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, &delete_binding,
               sizeof(delete_binding), delete_cell_scratch, 4,
               delete_target_handles, 1, &updated_count) ==
           PRDB_SDK_OPTIMIZER_PLAN_REQUEST_LIMIT);
    assert(updated_count == 0 && resolve.optimizer_calls == 1 &&
           resolve.stage_update_calls == 0 && resolve.statement_failures == 1);
    return 0;
}
