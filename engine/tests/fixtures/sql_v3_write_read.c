#include "prdb_sdk_sql_exec_v2.h"
#include "prdb_sdk_sql_exec_v3.h"
#include "prdb_sdk_sql_exec_v5.h"
#include "prdb_sdk_sql_exec_v6.h"
#include "prdb_sdk_sql_exec_v7.h"
#include "prdb_sdk_sql_exec_v8.h"
#include "prdb_sdk_sql_exec_v10.h"
#include "prdb_sdk_sql_exec_v11.h"
#include "prdb_sdk_sql_exec_v12.h"
#include "prdb_sdk_sql_exec_v13.h"
#include "prdb_sdk_sql_exec_v15.h"
#include "prdb_sdk_sql_exec_v17.h"
#include "prdb_sdk_sql_exec_v18.h"
#include "prdb_sdk_sql_exec_v16.h"
#include "prdb_sdk_sql_exec_v19.h"
#include "prdb_sdk_sql_exec_v20.h"
#include "prdb_sdk_sql_exec_v21.h"
#include "prdb_sdk_sql_exec_v23.h"
#include "prdb_sdk_sql_exec_v24.h"
#include "prdb_sdk_sql_dispatch_v1.h"
#include "prdb_sdk_sql_exec_v4.h"
#include "prdb_sdk_sql_result_v1.h"
#include "prdb_sdk_executor_v1.h"
#include "prdb_sdk_optimizer_plan_v1.h"
#include "prdb_sdk_optimizer_v1.h"
#include "prdb_sdk_optimizer_v2.h"
#include "prdb_query_abi_payload_codecs.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

const prdb_query_descriptor_v3_record prdb_query_descriptor_v3 = {
    sizeof(prdb_query_descriptor_v3_record), PRDB_QUERY_ABI_V3_VERSION,
    sizeof(prdb_host_api_v3), sizeof(prdb_invocation_v3)};
const prdb_query_descriptor_v4_record prdb_query_descriptor_v4 = {
    sizeof(prdb_query_descriptor_v4_record), PRDB_QUERY_ABI_V4_VERSION,
    sizeof(prdb_host_api_v4), sizeof(prdb_invocation_v4)};
const prdb_query_descriptor_v5_record prdb_query_descriptor_v5 = {
    sizeof(prdb_query_descriptor_v5_record), PRDB_QUERY_ABI_V5_VERSION,
    sizeof(prdb_host_api_v5), sizeof(prdb_invocation_v5)};

static const prdb_host_api_v5 *cancel_update_original_host;
static uint32_t cancel_update_ready_sent;
static uint32_t fail_next_stage_update;
static uint32_t observe_next_stage_update;
static uint32_t observe_only_stage_update;
static const prdb_host_api_v4 *range_observer_original_host;
static uint32_t range_observer_scan_requests;
static uint32_t range_observer_open_scan_requests;
static uint32_t range_observer_scan_ranges_requests;
static uint32_t range_observer_cancellation_checks;


#include "sql_v3_write_read/callbacks_collectors.h"

static int32_t run_write_read_fixture(
    const prdb_host_api_v3 *host, const prdb_invocation_v3 *invocation,
    const prdb_host_api_v4 *optimizer_host,
    const prdb_host_api_v5 *optimizer_host_v5) {
    static const uint8_t insert_sql[] = "INSERT INTO records VALUES (NULL)";
    static const uint8_t generated_identity_insert_sql[] =
        "INSERT INTO identity_records (id) VALUES (DEFAULT)";
    static const uint8_t select_null_sql[] =
        "SELECT id FROM records WHERE id IS NULL";
    static const uint8_t select_schemata_sql[] =
        "SELECT schema_name FROM schemata WHERE schema_name = $1 LIMIT 1";
    static const uint8_t select_tables_sql[] =
        "SELECT table_name FROM tables WHERE table_schema = $1 LIMIT 1";
    static const uint8_t select_columns_sql[] =
        "SELECT column_name FROM columns WHERE table_schema = $1 LIMIT 1";
    static const uint8_t select_records_columns_sql[] =
        "SELECT column_name FROM columns WHERE table_schema = $1 AND "
        "table_name = $2 LIMIT 1";
    static const uint8_t select_private_table_sql[] =
        "SELECT table_name FROM tables WHERE table_schema = $1 AND table_name = $2 LIMIT 1";
    static const uint8_t select_private_columns_sql[] =
        "SELECT column_name FROM columns WHERE table_schema = $1 AND table_name = $2 LIMIT 1";
    static const uint8_t update_sql[] =
        "UPDATE records SET id = $1 WHERE id IS NULL";
    static const uint8_t select_value_sql[] =
        "SELECT id FROM records WHERE id = $1";
    static const uint8_t optimizer_select_sql[] =
        "SELECT id FROM records WHERE id = $1 LIMIT 1";
    static const uint8_t optimizer_large_range_select_sql[] =
        "SELECT range_key FROM analytics_range_benchmark_records "
        "WHERE range_key >= $1 AND range_key < $2 LIMIT 8";
    static const uint8_t optimizer_small_estimate_select_sql[] =
        "SELECT id FROM records WHERE id = $1 LIMIT 1";
    static const uint8_t optimizer_ordered_select_sql[] =
        "SELECT right_key FROM grouped_records "
        "ORDER BY left_key DESC LIMIT 2";
    static const uint8_t select_composite_sum_optimizer_sql[] =
        "SELECT left_key, right_key, SUM(left_key) FROM grouped_records "
        "GROUP BY left_key, right_key HAVING SUM(left_key) >= 2 AND "
        "COUNT(*) >= 1 ORDER BY left_key ASC LIMIT 2";
    static const uint8_t select_composite_min_optimizer_sql[] =
        "SELECT left_key, right_key, MIN(left_key) FROM grouped_records "
        "GROUP BY left_key, right_key ORDER BY MIN(left_key) DESC NULLS LAST, "
        "left_key ASC NULLS FIRST LIMIT 2";
    static const uint8_t select_composite_max_optimizer_sql[] =
        "SELECT left_key, right_key, MAX(left_key) FROM grouped_records "
        "GROUP BY left_key, right_key ORDER BY MAX(left_key) DESC NULLS LAST, "
        "left_key ASC NULLS FIRST LIMIT 2";
    static const uint8_t select_composite_avg_optimizer_sql[] =
        "SELECT left_key, right_key, AVG(left_key) FROM grouped_records "
        "GROUP BY left_key, right_key ORDER BY AVG(left_key) DESC NULLS LAST, "
        "left_key ASC NULLS FIRST LIMIT 2";
    static const uint8_t select_composite_decimal_avg_optimizer_sql[] =
        "SELECT left_key, right_key, AVG(decimal_value) "
        "FROM decimal_grouped_records GROUP BY left_key, right_key "
        "ORDER BY AVG(decimal_value) DESC NULLS LAST, "
        "left_key ASC NULLS FIRST LIMIT 3";
    static const uint8_t select_composite_decimal_min_optimizer_sql[] =
        "SELECT left_key, right_key, MIN(decimal_value) "
        "FROM decimal_grouped_records GROUP BY left_key, right_key "
        "ORDER BY MIN(decimal_value) DESC NULLS LAST, "
        "left_key ASC NULLS FIRST LIMIT 3";
    static const uint8_t select_composite_decimal_max_optimizer_sql[] =
        "SELECT left_key, right_key, MAX(decimal_value) "
        "FROM decimal_grouped_records GROUP BY left_key, right_key "
        "ORDER BY MAX(decimal_value) DESC NULLS LAST, "
        "left_key ASC NULLS FIRST LIMIT 3";
    static const uint8_t select_composite_decimal_sum_optimizer_sql[] =
        "SELECT left_key, right_key, SUM(decimal_value) "
        "FROM decimal_grouped_records GROUP BY left_key, right_key "
        "ORDER BY SUM(decimal_value) DESC NULLS LAST, left_key ASC NULLS FIRST "
        "LIMIT 3";
    static const uint8_t select_composite_decimal_count_optimizer_sql[] =
        "SELECT left_key, right_key, COUNT(*) "
        "FROM decimal_grouped_records GROUP BY left_key, right_key "
        "ORDER BY COUNT(*) DESC NULLS LAST, left_key ASC NULLS FIRST LIMIT 3";
    static const uint8_t select_filtered_composite_decimal_sum_sql[] =
        "SELECT left_key, right_key, SUM(decimal_value) "
        "FROM decimal_grouped_records "
        "WHERE left_key = $1 AND "
        "(decimal_value IS NULL OR decimal_value IS NOT NULL) "
        "GROUP BY left_key, right_key LIMIT 3";
    static const uint8_t select_filtered_ordered_composite_decimal_sum_sql[] =
        "SELECT left_key, right_key, SUM(decimal_value) "
        "FROM decimal_grouped_records WHERE left_key = $1 AND "
        "(decimal_value IS NULL OR decimal_value IS NOT NULL) "
        "GROUP BY left_key, right_key "
        "HAVING SUM(decimal_value) IS NOT NULL AND COUNT(*) >= 1 "
        "ORDER BY SUM(decimal_value) DESC NULLS LAST, "
        "left_key ASC NULLS FIRST, right_key DESC NULLS LAST LIMIT 3";
    static const uint8_t select_filtered_composite_text_min_sql[] =
        "SELECT group_key, measure, MIN(group_key) "
        "FROM analytics_pressure_records WHERE measure = $1 "
        "GROUP BY group_key, measure LIMIT 2";
    static const uint8_t select_redacted_null_filter_sql[] =
        "SELECT left_key, right_key, SUM(decimal_value) "
        "FROM decimal_grouped_records WHERE decimal_value IS NULL "
        "GROUP BY left_key, right_key LIMIT 3";
    static const uint8_t select_composite_bigint_sum_optimizer_sql[] =
        "SELECT left_key, right_key, SUM(measure) "
        "FROM bigint_grouped_records GROUP BY left_key, right_key "
        "ORDER BY SUM(measure) DESC NULLS LAST, "
        "left_key ASC NULLS FIRST LIMIT 3";
    static const uint8_t select_composite_bigint_min_optimizer_sql[] =
        "SELECT left_key, right_key, MIN(measure) "
        "FROM bigint_grouped_records GROUP BY left_key, right_key "
        "ORDER BY MIN(measure) DESC NULLS LAST, "
        "left_key ASC NULLS FIRST LIMIT 3";
    static const uint8_t select_composite_bigint_max_optimizer_sql[] =
        "SELECT left_key, right_key, MAX(measure) "
        "FROM bigint_grouped_records GROUP BY left_key, right_key "
        "ORDER BY MAX(measure) DESC NULLS LAST, "
        "left_key ASC NULLS FIRST LIMIT 3";
    static const uint8_t select_composite_bigint_avg_optimizer_sql[] =
        "SELECT left_key, right_key, AVG(measure) "
        "FROM bigint_grouped_records GROUP BY left_key, right_key "
        "ORDER BY AVG(measure) DESC NULLS LAST, "
        "left_key ASC NULLS FIRST LIMIT 3";
    static const uint8_t select_composite_smallint_sum_optimizer_sql[] =
        "SELECT left_key, right_key, SUM(small_measure) "
        "FROM bigint_grouped_records GROUP BY left_key, right_key "
        "ORDER BY SUM(small_measure) DESC NULLS LAST, "
        "left_key ASC NULLS FIRST LIMIT 3";
    static const uint8_t select_composite_smallint_min_optimizer_sql[] =
        "SELECT left_key, right_key, MIN(small_measure) "
        "FROM bigint_grouped_records GROUP BY left_key, right_key "
        "ORDER BY MIN(small_measure) DESC NULLS LAST, "
        "left_key ASC NULLS FIRST LIMIT 3";
    static const uint8_t select_composite_smallint_max_optimizer_sql[] =
        "SELECT left_key, right_key, MAX(small_measure) "
        "FROM bigint_grouped_records GROUP BY left_key, right_key "
        "ORDER BY MAX(small_measure) DESC NULLS LAST, "
        "left_key ASC NULLS FIRST LIMIT 3";
    static const uint8_t select_composite_smallint_avg_optimizer_sql[] =
        "SELECT left_key, right_key, AVG(small_measure) "
        "FROM bigint_grouped_records GROUP BY left_key, right_key "
        "ORDER BY AVG(small_measure) DESC NULLS LAST, "
        "left_key ASC NULLS FIRST LIMIT 3";
    static const uint8_t select_composite_integer_min_optimizer_sql[] =
        "SELECT left_key, right_key, MIN(integer_measure) "
        "FROM bigint_grouped_records GROUP BY left_key, right_key "
        "ORDER BY MIN(integer_measure) DESC NULLS LAST, "
        "left_key ASC NULLS FIRST LIMIT 3";
    static const uint8_t select_composite_integer_max_optimizer_sql[] =
        "SELECT left_key, right_key, MAX(integer_measure) "
        "FROM bigint_grouped_records GROUP BY left_key, right_key "
        "ORDER BY MAX(integer_measure) DESC NULLS LAST, "
        "left_key ASC NULLS FIRST LIMIT 3";
    static const uint8_t select_composite_integer_avg_optimizer_sql[] =
        "SELECT left_key, right_key, AVG(integer_measure) "
        "FROM bigint_grouped_records GROUP BY left_key, right_key "
        "ORDER BY AVG(integer_measure) DESC NULLS LAST, "
        "left_key ASC NULLS FIRST LIMIT 3";
    static const uint8_t select_bigint_visible_count_optimizer_sql[] =
        "SELECT left_key, right_key, COUNT(*) "
        "FROM bigint_grouped_records GROUP BY left_key, right_key "
        "ORDER BY COUNT(*) DESC NULLS LAST, left_key ASC NULLS FIRST LIMIT 3";
    static const uint8_t select_analytical_pressure_sql[] =
        "SELECT group_key, SUM(measure) FROM analytics_pressure_records "
        "GROUP BY group_key";
    static const uint8_t select_ordered_analytical_pressure_sql[] =
        "SELECT group_key, SUM(measure) FROM analytics_pressure_records "
        "GROUP BY group_key ORDER BY SUM(measure) DESC NULLS LAST, "
        "group_key ASC NULLS FIRST";
    static const uint8_t select_ordered_grouped_avg_sql[] =
        "SELECT right_key, AVG(left_key) FROM grouped_records "
        "GROUP BY right_key ORDER BY AVG(left_key) DESC NULLS LAST, "
        "right_key ASC NULLS FIRST LIMIT 2";
    static const uint8_t select_grouped_order_sql[] =
        "SELECT id, COUNT(*) FROM records GROUP BY id "
        "ORDER BY COUNT(*) DESC NULLS LAST, id ASC NULLS LAST LIMIT 1";
    static const uint8_t select_composite_group_sql[] =
        "SELECT left_key, right_key, COUNT(*) FROM grouped_records "
        "GROUP BY left_key, right_key LIMIT 2";
    static const uint8_t select_composite_order_sql[] =
        "SELECT left_key, right_key, COUNT(*) FROM grouped_records "
        "GROUP BY left_key, right_key "
        "ORDER BY right_key DESC NULLS LAST, COUNT(*) DESC, "
        "left_key ASC NULLS LAST LIMIT 2";
    static const uint8_t select_composite_literal_sql[] =
        "SELECT left_key, right_key, COUNT(*) FROM grouped_records "
        "GROUP BY left_key, right_key HAVING COUNT(*) >= 1 "
        "ORDER BY right_key DESC NULLS LAST, COUNT(*) DESC, "
        "left_key ASC NULLS LAST LIMIT 2";
    static const uint8_t select_composite_boolean_sql[] =
        "SELECT left_key, right_key, COUNT(*) FROM grouped_records "
        "GROUP BY left_key, right_key HAVING "
        "(COUNT(*) >= 2 OR COUNT(*) = 3) AND NOT COUNT(*) < 2 "
        "ORDER BY right_key DESC NULLS LAST, COUNT(*) DESC, "
        "left_key ASC NULLS LAST LIMIT 2";
    static const uint8_t select_composite_hidden_count_dispatch_sql[] =
        "SELECT left_key, right_key, SUM(decimal_value) "
        "FROM decimal_grouped_records GROUP BY left_key, right_key "
        "HAVING COUNT(*) IS NOT NULL AND COUNT(*) >= 2 "
        "AND SUM(decimal_value) IS NOT NULL "
        "AND SUM(decimal_value) >= 4.50e0";
    static const uint8_t select_composite_avg_numeric_v21_sql[] =
        "SELECT left_key, right_key, AVG(decimal_value) "
        "FROM decimal_grouped_records "
        "GROUP BY left_key, right_key HAVING AVG(decimal_value) >= 1.40e0";
    static const uint8_t select_composite_min_numeric_v21_sql[] =
        "SELECT left_key, right_key, MIN(decimal_value) "
        "FROM decimal_grouped_records "
        "GROUP BY left_key, right_key "
        "HAVING MIN(decimal_value) <= 1.00e0 "
        "ORDER BY left_key ASC LIMIT 1";
    static const uint8_t select_join_sql[] =
        "SELECT records.id, linked_records.id FROM records INNER JOIN "
        "linked_records ON records.id = linked_records.id LIMIT 1";
    static const uint8_t select_composite_join_sql[] =
        "SELECT g.left_key, g.right_key, l.id, l.match_key "
        "FROM grouped_records AS g INNER JOIN linked_records AS l ON "
        "g.left_key = l.id AND g.right_key = l.match_key LIMIT 2";
    static const uint8_t select_self_join_sql[] =
        "SELECT a.id, b.id FROM linked_records AS a INNER JOIN "
        "linked_records AS b ON a.id = b.id AND "
        "a.match_key = b.match_key";
    static const uint8_t select_filtered_composite_join_sql[] =
        "SELECT g.left_key, l.id FROM grouped_records AS g INNER JOIN "
        "linked_records AS l ON g.right_key = l.match_key AND "
        "g.right_key = l.match_key WHERE l.id >= $1 LIMIT 1";
    static const uint8_t filtered_join_parameter_bytes[] = {0, 0, 0, 42};
    const prdb_sdk_value_v1 filtered_join_parameter = {
        sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
        filtered_join_parameter_bytes, sizeof(filtered_join_parameter_bytes)};
    static const uint8_t filtered_join_parameter_block[] = {
        'P', 'S', 'P', '1', 0, 1, 0, 0,
        0, 0, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0, 0,
        0, 0, 0, 4, 42, 0, 0, 0
    };
    static const uint8_t single_key_having_sql[] =
        "SELECT right_key, SUM(decimal_value) FROM decimal_grouped_records "
        "WHERE (decimal_value IS NULL OR decimal_value >= $1) GROUP BY right_key "
        "HAVING SUM(decimal_value) IS NULL OR "
        "(COUNT(*) >= 2 AND SUM(decimal_value) >= $1) "
        "ORDER BY SUM(decimal_value) DESC NULLS LAST, right_key LIMIT 1";
    static const uint8_t min_amount_parameter[] = {
        'P', 'S', 'P', '1', 0, 1, 0, 0,
        0, 0, 0, PRDB_SDK_VALUE_TYPE_DECIMAL, 4, 2, 0, 0,
        0, 0, 0, 16,
        125, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    static const uint8_t delete_sql[] = "DELETE FROM records WHERE id = $1";
    static const uint8_t multi_insert_sql[] =
        "INSERT INTO records VALUES ($1), ($2)";
    static const uint8_t schema[] = "sql_roundtrip";
    static const uint8_t empty_schema[] = "empty_roundtrip";
    static const uint8_t information_schema[] = "information_schema";
    static const uint8_t expected_user_column[] = "id";
    static const uint8_t records_table_name[] = "records";
    static const uint8_t private_table_name[] = "private_records";
    static const uint8_t missing_table_name[] = "missing_records";
    static const uint8_t hidden_schema_name[] = "hidden_roundtrip";
    static const uint8_t missing_schema_name[] = "missing_roundtrip";
    static const uint8_t first_information_schema_table[] = "columns";
    static const uint8_t first_information_schema_column[] = "table_schema";
    static const uint8_t integer_bytes[] = {0, 0, 0, 42};
    static const uint8_t inserted_marker[] = "insert-visible";
    static const uint8_t updated_marker[] = "update-visible";
    uint8_t planner_response[1024] = {0};
    uint8_t executor_response[1024] = {0};
    uint8_t batch_response[4096] = {0};
    /* Large text rows need multiple rows per batch to stay below the v12
     * runner's 124-page scan ceiling for this 128-row workload. */
    static uint8_t ordered_pressure_batch_response[32 * 1024] = {0};
    uint8_t grouped_materialized[128] = {0};
    uint8_t grouped_result[256] = {0};
    static uint8_t ordered_pressure_result[16 * 1024] = {0};
    uint8_t composite_grouped_result[256] = {0};
    uint8_t join_result[128] = {0};
    uint8_t insert_request[256] = {0};
    uint8_t multi_insert_request[256] = {0};
    uint8_t update_request[256] = {0};
    uint8_t inserted_handle[16] = {0};
    uint8_t update_targets[PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS * 16] = {0};
    prdb_sdk_sql_column_shape_v2 planner_columns[9] = {{0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[9] = {{0}};
    uint8_t composite_join_left_planner_response[1024] = {0};
    uint8_t composite_join_right_planner_response[1024] = {0};
    uint8_t composite_join_left_executor_response[1024] = {0};
    uint8_t composite_join_right_executor_response[65536] = {0};
    static uint8_t composite_join_optimizer_request[
        PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES];
    static uint8_t composite_join_optimizer_response[
        PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES + 64];
    static uint8_t group_count_optimizer_request[
        PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES];
    static uint8_t group_count_optimizer_response[
        PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES + 64];
    prdb_sdk_sql_column_shape_v2 composite_join_left_planner_columns[9] = {{0}};
    prdb_sdk_sql_column_shape_v2 composite_join_right_planner_columns[9] = {{0}};
    prdb_sdk_sql_column_shape_v2 composite_join_left_executor_columns[9] = {{0}};
    prdb_sdk_sql_column_shape_v2 composite_join_right_executor_columns[9] = {{0}};
    static prdb_sdk_sql_join_materialized_row_v16 composite_join_left_rows[128];
    static prdb_sdk_sql_join_materialized_row_v16 composite_join_right_rows[4096];
    static uint8_t composite_join_materialized_bytes[
        PRDB_SDK_SQL_EXEC_V8_MAX_MATERIALIZED_BYTES];
    static uint8_t analytical_pressure_batch_response[PRDB_SDK_BATCH_MAX_BYTES];
    static uint8_t analytical_pressure_group_key_bytes[
        PRDB_SDK_SQL_EXEC_V4_MAX_GROUP_KEY_BYTES];
    prdb_sdk_executor_insert_value_v1 insert_values[2] = {{0}};
    prdb_sdk_executor_insert_value_v1 multi_insert_values[2] = {{0}};
    prdb_sdk_executor_update_patch_v1 update_patches[1] = {{0}};
    prdb_sdk_select_binding_v2 binding = {0};
    prdb_sdk_value_v1 cell_scratch[9] = {{0}};
    prdb_sdk_value_v1 parameter = {0};
    prdb_sdk_value_v1 multi_insert_parameters[2] = {{0}};
    prdb_sdk_value_v1 metadata_parameter = {0};
    prdb_sdk_value_v1 records_table_parameter = {0};
    prdb_sdk_value_v1 records_metadata_parameters[2] = {{0}};
    prdb_sdk_value_v1 private_table_parameter = {0};
    prdb_sdk_value_v1 private_metadata_parameters[2] = {{0}};
    static const uint8_t count_parameter_bytes[8] = {0, 0, 0, 0, 0, 0, 0, 1};
    prdb_sdk_value_v1 count_parameter = {
        sizeof(count_parameter), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BIGINT,
        count_parameter_bytes, sizeof(count_parameter_bytes)};
    static const uint8_t filtered_group_key_bytes[4] = {0, 0, 0, 1};
    prdb_sdk_value_v1 filtered_group_key_parameter = {
        sizeof(filtered_group_key_parameter), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
        filtered_group_key_bytes, sizeof(filtered_group_key_bytes)};
    result_context results = {0};
    static const uint8_t id_column_name[] = "id";
    prdb_sdk_sql_column_shape_v2 grouped_column = {
        id_column_name, sizeof(id_column_name) - 1, 0,
        PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0};
    grouped_result_context grouped_results = {
        host, &grouped_column, &grouped_column, grouped_result,
        sizeof(grouped_result), 0, 0};
    composite_grouped_result_context composite_grouped_results = {
        host, executor_columns, 2, composite_grouped_result,
        sizeof(composite_grouped_result), 0, 0};
    joined_result_context joined_results = {
        host, NULL, NULL,
        join_result, sizeof(join_result), 0, 0};
    uint32_t updated_count = 0;
    uint32_t deleted_count = 0;
    uint32_t multi_inserted_count = UINT32_MAX;
    uint32_t generated_inserted_count = UINT32_MAX;
    int32_t status;
    analytical_pressure_result_context analytical_pressure_results = {
        .host = host,
    };

    if (host == NULL || invocation == NULL ||
        host->struct_size < sizeof(*host) ||
        host->abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->executor_request == NULL || host->planner_request == NULL ||
        host->write_result == NULL || host->statement_failure == NULL) {
        return 1;
    }
    if (invocation->parameters_len == 1 && invocation->parameters != NULL &&
        (invocation->parameters[0] == '#' || invocation->parameters[0] == '$' ||
         invocation->parameters[0] == '&' || invocation->parameters[0] == '^' ||
         invocation->parameters[0] == '?' || invocation->parameters[0] == '@' ||
         invocation->parameters[0] == '~')) {
        static const uint8_t in_sql[] =
            "SELECT left_key, right_key FROM grouped_records "
            "WHERE left_key IN ($1, $2, $3, $4)";
        static const uint8_t in_fallback_sql[] =
            "SELECT left_key, right_key FROM grouped_records "
            "WHERE right_key IN ($1, $2, $3, $4)";
        static const uint8_t in_parameters[] = {
            'P', 'S', 'P', '1', 0, 4, 0, 0,
            0, 0, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0, 0,
            0, 0, 0, 4, 1, 0, 0, 0,
            0, 0, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0, 0,
            0, 0, 0, 4, 2, 0, 0, 0,
            0, 0, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0, 0,
            0, 0, 0, 4, 1, 0, 0, 0,
            0, 0, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 1, 0,
            0, 0, 0, 0};
        static const uint8_t in_fallback_parameters[] = {
            'P', 'S', 'P', '1', 0, 4, 0, 0,
            0, 0, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0, 0,
            0, 0, 0, 4, 7, 0, 0, 0,
            0, 0, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0, 0,
            0, 0, 0, 4, 9, 0, 0, 0,
            0, 0, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0, 0,
            0, 0, 0, 4, 7, 0, 0, 0,
            0, 0, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 1, 0,
            0, 0, 0, 0};
        prdb_host_api_v4 observed_host;

        if (optimizer_host == NULL) return 1;
        observed_host = *optimizer_host;
        range_observer_original_host = optimizer_host;
        range_observer_scan_ranges_requests = 0;
        range_observer_open_scan_requests = 0;
        range_observer_cancellation_checks = 0;
        observed_host.v3.executor_request = invocation->parameters[0] == '^'
            ? cancel_range_observer_executor_request
            : observe_range_executor_request;
        status = prdb_sdk_sql_run_statement_optimizer_v1(
            &observed_host, schema, sizeof(schema) - 1,
            invocation->parameters[0] == '$' ? in_fallback_sql : in_sql,
            invocation->parameters[0] == '$' ? sizeof(in_fallback_sql) - 1
                                             : sizeof(in_sql) - 1,
            invocation->parameters[0] == '$' ? in_fallback_parameters
                                             : in_parameters,
            sizeof(in_parameters));
        range_observer_original_host = NULL;
        if (invocation->parameters[0] == '?') {
            if (status == PRDB_SDK_SQL_DISPATCH_OK ||
                range_observer_scan_ranges_requests != 0 ||
                range_observer_open_scan_requests == 0) {
                static const uint8_t message[] = "fixture-error-in-denied-key-path";
                return report_failure(host, message, sizeof(message) - 1);
            }
            return 0;
        }
        if (invocation->parameters[0] == '^') {
            if (status == PRDB_SDK_SQL_DISPATCH_OK ||
                range_observer_cancellation_checks == 0 ||
                range_observer_scan_ranges_requests != 1 ||
                range_observer_open_scan_requests != 0) {
                static const uint8_t message[] = "fixture-error-in-cancellation-boundary";
                return report_failure(host, message, sizeof(message) - 1);
            }
            return 0;
        }
        if (status != PRDB_SDK_SQL_DISPATCH_OK) {
            static const uint8_t message[] = "fixture-error-batched-in-status";
            return report_failure(host, message, sizeof(message) - 1);
        }
        if (invocation->parameters[0] == '$') {
            if (range_observer_scan_ranges_requests != 0 ||
                range_observer_open_scan_requests == 0) {
                static const uint8_t message[] = "fixture-error-in-full-scan-fallback";
                return report_failure(host, message, sizeof(message) - 1);
            }
        } else if (invocation->parameters[0] == '@') {
            if (range_observer_scan_ranges_requests != 1 ||
                range_observer_open_scan_requests == 0) {
                static const uint8_t message[] = "fixture-error-in-over-budget-fallback";
                return report_failure(host, message, sizeof(message) - 1);
            }
        } else if (invocation->parameters[0] == '~') {
            if (range_observer_scan_ranges_requests != 1 ||
                range_observer_open_scan_requests != 0) {
                static const uint8_t message[] = "fixture-error-elevated-index-budget";
                return report_failure(host, message, sizeof(message) - 1);
            }
        } else if (range_observer_scan_ranges_requests != 1 ||
                   range_observer_open_scan_requests != 0) {
            static const uint8_t message[] = "fixture-error-batched-in-access-path";
            return report_failure(host, message, sizeof(message) - 1);
        }
        return 0;
    }
    if (invocation->parameters_len == 1 && invocation->parameters != NULL &&
        invocation->parameters[0] == '!') {
        static const uint8_t large_result_sql[] =
            "SELECT large_payload, large_payload, large_payload, "
            "large_payload, large_payload, large_payload, large_payload, "
            "large_payload, large_payload, large_payload, large_payload "
            "FROM decimal_grouped_records WHERE left_key = $1 LIMIT 800";
        static const uint8_t large_result_parameters[] = {
            'P', 'S', 'P', '1', 0, 1, 0, 0,
            0, 0, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0, 0,
            0, 0, 0, 4, 99, 0, 0, 0};
        status = prdb_sdk_sql_run_statement_optimizer_v1(
            optimizer_host, schema, sizeof(schema) - 1, large_result_sql,
            sizeof(large_result_sql) - 1, large_result_parameters,
            sizeof(large_result_parameters));
        if (status != PRDB_SDK_SQL_DISPATCH_OK) {
            char message[64];
            int length = snprintf(message, sizeof(message),
                                  "fixture-error-large-sql-result-%d", status);
            return length > 0 && (size_t)length < sizeof(message)
                       ? report_failure(host, (const uint8_t *)message,
                                        (uint64_t)length)
                       : 1;
        }
        return 0;
    }
    if (invocation->parameters_len == 1 && invocation->parameters != NULL &&
        invocation->parameters[0] == 'O') {
        uint8_t output_chunk[PRDB_QUERY_MAX_RESULT_CHUNK_BYTES];
        memset(output_chunk, 0xa5, sizeof(output_chunk));
        for (uint32_t index = 0; index < 256; ++index) {
            if (host->write_result(host->context, output_chunk,
                                   sizeof(output_chunk)) !=
                PRDB_QUERY_CALLBACK_OK) {
                return 1;
            }
        }
        return 0;
    }
    if (invocation->parameters_len == 1 && invocation->parameters != NULL &&
        (invocation->parameters[0] == '6' || invocation->parameters[0] == '7' ||
         invocation->parameters[0] == '8' || invocation->parameters[0] == '9' ||
         invocation->parameters[0] == '2' || invocation->parameters[0] == '3' ||
         invocation->parameters[0] == '4' || invocation->parameters[0] == '5' ||
         invocation->parameters[0] == 'c' ||
         invocation->parameters[0] == 'W' || invocation->parameters[0] == 'E' ||
         invocation->parameters[0] == 'q' ||
         invocation->parameters[0] == 's' ||
         invocation->parameters[0] == 'h' || invocation->parameters[0] == 'a' ||
         invocation->parameters[0] == 'b' ||
         invocation->parameters[0] == 'n' ||
         invocation->parameters[0] == 'p' ||
         invocation->parameters[0] == 'r' ||
         invocation->parameters[0] == 'j' || invocation->parameters[0] == 'k' ||
         invocation->parameters[0] == 'X' || invocation->parameters[0] == 'Y' ||
         invocation->parameters[0] == 'z')) {
        static const uint8_t benchmark_sql[] =
            "SELECT g.left_key, g.right_key, l.id, l.match_key "
            "FROM grouped_records AS g INNER JOIN linked_records AS l ON "
            "g.left_key = l.id AND g.right_key = l.match_key";
        static const uint8_t started[] = "JOIN_BENCHMARK_STARTED";
        static const uint8_t samples_finished[] =
            "JOIN_BENCHMARK_SAMPLES_FINISHED";
        if (invocation->parameters[0] == '6') {
            uint8_t workload_response[12] = {0};
            uint8_t empty_request = 0;
            uint64_t workload_response_len = 0;
            int32_t workload_status = host->executor_request(
                host->context, PRDB_QUERY_HOST_OP_GET_WORKLOAD_BUDGETS,
                &empty_request, 0, workload_response,
                sizeof(workload_response), &workload_response_len);
            uint32_t candidate_budget = workload_response_len == 12
                ? ((uint32_t)workload_response[8] << 24) |
                  ((uint32_t)workload_response[9] << 16) |
                  ((uint32_t)workload_response[10] << 8) |
                  (uint32_t)workload_response[11]
                : 0;
            if (workload_status != PRDB_QUERY_CALLBACK_OK ||
                workload_response_len != 12 ||
                memcmp(workload_response, "PQB1\0\1\0\14", 8) != 0 ||
                candidate_budget != 512) {
                char message[72];
                int message_len = snprintf(
                    message, sizeof(message),
                    "join-candidate-budget-status=%d value=%u",
                    workload_status, candidate_budget);
                return message_len > 0 && (size_t)message_len < sizeof(message)
                    ? report_failure(host, (const uint8_t *)message,
                                     (uint64_t)message_len)
                    : 1;
            }
        }
        const struct timespec load_settle_interval = {0, 100000000};
        const uint32_t outer_rows = invocation->parameters[0] == 'n' ||
                                            invocation->parameters[0] == 'j' ||
                                            invocation->parameters[0] == 'k' ? 18
                                    : invocation->parameters[0] == 'p' ||
                                            invocation->parameters[0] == 'r' ? 19
                                    : invocation->parameters[0] == 'Y' ||
                                            invocation->parameters[0] == 'z' ||
                                            invocation->parameters[0] == 'W' ||
                                            invocation->parameters[0] == 'E' ||
                                            invocation->parameters[0] == 'q' ||
                                            invocation->parameters[0] == 's' ||
                                            invocation->parameters[0] == 'h' ||
                                            invocation->parameters[0] == 'a' ||
                                            invocation->parameters[0] == 'b'
                                        ? 17
                                    : invocation->parameters[0] == 'X' ? 16
                                                                       : 9;
        const uint32_t fanout = invocation->parameters[0] == 'a' ||
                                        invocation->parameters[0] == 'b'
                                    ? 4
                                : invocation->parameters[0] == 'j' ||
                                          invocation->parameters[0] == 'k' ? 4
                                : invocation->parameters[0] == 'c' ? 4
                                : invocation->parameters[0] == '2' ? 2
                                : invocation->parameters[0] == '3' ? 3
                                : invocation->parameters[0] == '4' ? 4
                                : invocation->parameters[0] == '5' ? 5
                                : invocation->parameters[0] == 'n' ||
                                          invocation->parameters[0] == 'Y' ||
                                          invocation->parameters[0] == 'z' ||
                                          invocation->parameters[0] == 'W' ||
                                          invocation->parameters[0] == 'E' ||
                                          invocation->parameters[0] == 'q' ? 1
                                : invocation->parameters[0] == 'p' ? 1
                                : invocation->parameters[0] == 'r' ? 5
                                : invocation->parameters[0] == 'X' ? 1
                                : invocation->parameters[0] == '9' ? 1
                                                                  : 0;
        const uint32_t expected_join_rows = invocation->parameters[0] == 'h' ? 128
                                            : invocation->parameters[0] == 's' ? 16
                                            : invocation->parameters[0] == 'b' ? 69
                                            : invocation->parameters[0] == 'k' ? 73
                                            : fanout != 0 ? outer_rows * fanout
                                            : invocation->parameters[0] == '8' ? 1024
                                                                              : 512;
        const int64_t expected_join_checksum = invocation->parameters[0] == 's' ? 0
                                               : invocation->parameters[0] == 'n' ? 612
                                               : invocation->parameters[0] == 'j' ||
                                                         invocation->parameters[0] == 'k' ? 2448
                                               : invocation->parameters[0] == 'p' ? 684
                                               : invocation->parameters[0] == 'r' ? 3420
                                               : fanout != 0
                                                   ? (int64_t)((invocation->parameters[0] == 'Y' ||
                                                                        invocation->parameters[0] == 'z' ||
                                                                        invocation->parameters[0] == 'W' ||
                                                                        invocation->parameters[0] == 'E' ||
                                                                        invocation->parameters[0] == 'q' ||
                                                                        invocation->parameters[0] == 'a' ||
                                                                        invocation->parameters[0] == 'b'
                                                                    ? 544
                                                                : invocation->parameters[0] == 'X'
                                                                    ? 480
                                                                    : 144) * fanout)
                                                : invocation->parameters[0] == '6' ? 7168
                                                                                  : 0;
        joined_results.left_columns = composite_join_left_executor_columns;
        joined_results.right_columns = composite_join_right_executor_columns;
        if (optimizer_host_v5 == NULL ||
            host->write_result(host->context, started, sizeof(started) - 1) !=
                PRDB_QUERY_CALLBACK_OK ||
            sleep_without_signal_interruption(load_settle_interval) != 0) {
            static const uint8_t message[] = "fixture-error-join-benchmark-start";
            return report_failure(host, message, sizeof(message) - 1);
        }
        for (uint8_t sample = 0; sample < 2; ++sample) {
            uint64_t started_ns = 0;
            uint64_t finished_ns = 0;
            indexed_join_benchmark_result_context indexed_results = {
                .asymmetric_values = invocation->parameters[0] == '7' ||
                                     invocation->parameters[0] == '8',
            };
            if (!monotonic_time_ns(&started_ns)) {
                static const uint8_t message[] = "fixture-error-join-benchmark-timer";
                return report_failure(host, message, sizeof(message) - 1);
            }
            status = prdb_sdk_sql_execute_inner_join_optimizer_v1(
                optimizer_host_v5, schema, sizeof(schema) - 1,
                benchmark_sql, sizeof(benchmark_sql) - 1, 16,
                sizeof(batch_response), composite_join_optimizer_request,
                sizeof(composite_join_optimizer_request),
                composite_join_optimizer_response,
                sizeof(composite_join_optimizer_response),
                composite_join_left_planner_response,
                sizeof(composite_join_left_planner_response),
                composite_join_right_planner_response,
                sizeof(composite_join_right_planner_response),
                composite_join_left_executor_response,
                sizeof(composite_join_left_executor_response),
                composite_join_right_executor_response,
                sizeof(composite_join_right_executor_response), batch_response,
                sizeof(batch_response), composite_join_left_planner_columns, 9,
                composite_join_right_planner_columns, 9,
                composite_join_left_executor_columns, 9,
                composite_join_right_executor_columns, 9, cell_scratch, 9,
                composite_join_left_rows, 128, composite_join_right_rows, 128,
                composite_join_materialized_bytes,
                sizeof(composite_join_materialized_bytes),
                collect_indexed_join_benchmark_row, &indexed_results);
            if (!monotonic_time_ns(&finished_ns)) {
                static const uint8_t message[] = "fixture-error-indexed-join-timer";
                return report_failure(host, message, sizeof(message) - 1);
            }
            if (status != PRDB_SDK_SQL_EXEC_OK) {
                char message[64];
                int message_len = snprintf(message, sizeof(message),
                                           "indexed-join-exec-status=%d", status);
                if (message_len <= 0 || (size_t)message_len >= sizeof(message)) {
                    return 1;
                }
                return report_failure(host, (const uint8_t *)message,
                                      (uint64_t)message_len);
            }
            const uint32_t plan_len =
                ((uint32_t)composite_join_optimizer_response[3] << 24) |
                ((uint32_t)composite_join_optimizer_response[4] << 16) |
                ((uint32_t)composite_join_optimizer_response[5] << 8) |
                (uint32_t)composite_join_optimizer_response[6];
            const uint64_t optimizer_response_len =
                (uint64_t)PRDB_OPTIMIZER_RESPONSE_SUCCESS_HEADER_BYTES + plan_len;
            prdb_sdk_optimizer_response_v1 benchmark_plan = {0};
            const int32_t benchmark_plan_status =
                optimizer_response_len <= sizeof(composite_join_optimizer_response)
                    ? prdb_sdk_optimizer_decode_response_v1(
                          composite_join_optimizer_response,
                          optimizer_response_len, &benchmark_plan,
                          sizeof(benchmark_plan))
                    : PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE;
            const uint8_t expected_plan_kind =
                invocation->parameters[0] == '5' || invocation->parameters[0] == 'h' ||
                        invocation->parameters[0] == 'b' || invocation->parameters[0] == 'k'
                    ? PRDB_SDK_OPTIMIZER_PLAN_KIND_JOIN_SELECT
                : invocation->parameters[0] == '6'
                    ? PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_PREFIX_NESTED_LOOP_JOIN
                    : PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_NESTED_LOOP_JOIN;
            if (benchmark_plan_status != PRDB_SDK_OPTIMIZER_RESULT_OK ||
                benchmark_plan.status != PRDB_SDK_OPTIMIZER_STATUS_SUCCESS ||
                benchmark_plan.plan == NULL || benchmark_plan.plan_len < 3 ||
                benchmark_plan.plan[2] != expected_plan_kind) {
                char message[96];
                int message_len = snprintf(
                    message, sizeof(message),
                    "indexed-join-plan-decode=%d kind=%u length=%u",
                    benchmark_plan_status,
                    benchmark_plan.plan != NULL && benchmark_plan.plan_len >= 3
                        ? (unsigned)benchmark_plan.plan[2]
                        : 0u,
                    benchmark_plan.plan_len);
                if (message_len <= 0 || (size_t)message_len >= sizeof(message)) {
                    return 1;
                }
                return report_failure(host, (const uint8_t *)message,
                                      (uint64_t)message_len);
            }
            if (indexed_results.row_count != expected_join_rows) {
                char message[64];
                int message_len = snprintf(message, sizeof(message),
                                           "indexed-join-count=%u expected=%u",
                                           indexed_results.row_count,
                                           expected_join_rows);
                if (message_len <= 0 || (size_t)message_len >= sizeof(message))
                    return 1;
                return report_failure(host, (const uint8_t *)message,
                                      (uint64_t)message_len);
            }
            if (indexed_results.value_checksum != expected_join_checksum) {
                static const uint8_t message[] = "fixture-error-indexed-join-checksum";
                return report_failure(host, message, sizeof(message) - 1);
            }
            const uint8_t selected_strategy =
                benchmark_plan.plan[2] ==
                        PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_NESTED_LOOP_JOIN
                    || benchmark_plan.plan[2] ==
                           PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_PREFIX_NESTED_LOOP_JOIN
                    ? 'I'
                    : 'H';
            if (report_indexed_join_timing(host, selected_strategy,
                                           finished_ns - started_ns) !=
                PRDB_QUERY_CALLBACK_OK) {
                static const uint8_t message[] = "fixture-error-indexed-join-timing";
                return report_failure(host, message, sizeof(message) - 1);
            }

            if (invocation->parameters[0] == 'W' ||
                invocation->parameters[0] == 'E' ||
                invocation->parameters[0] == 'q' ||
                invocation->parameters[0] == 'p' ||
                invocation->parameters[0] == 'r') continue;
            indexed_join_benchmark_result_context hash_results = {
                .asymmetric_values = invocation->parameters[0] == '7' ||
                                     invocation->parameters[0] == '8',
            };
            if (!monotonic_time_ns(&started_ns)) {
                static const uint8_t message[] = "fixture-error-join-benchmark-timer";
                return report_failure(host, message, sizeof(message) - 1);
            }
            status = prdb_sdk_sql_execute_inner_join_v19(
                host, schema, sizeof(schema) - 1, benchmark_sql,
                sizeof(benchmark_sql) - 1, 16,
                sizeof(batch_response), composite_join_left_planner_response,
                sizeof(composite_join_left_planner_response),
                composite_join_right_planner_response,
                sizeof(composite_join_right_planner_response),
                composite_join_left_executor_response,
                sizeof(composite_join_left_executor_response),
                composite_join_right_executor_response,
                sizeof(composite_join_right_executor_response), batch_response,
                sizeof(batch_response), composite_join_left_planner_columns, 9,
                composite_join_right_planner_columns, 9,
                composite_join_left_executor_columns, 9,
                composite_join_right_executor_columns, 9, cell_scratch, 9,
                composite_join_left_rows, 128, composite_join_right_rows, 128,
                composite_join_materialized_bytes,
                sizeof(composite_join_materialized_bytes),
                collect_indexed_join_benchmark_row, &hash_results);
            if (!monotonic_time_ns(&finished_ns) ||
                status != PRDB_SDK_SQL_EXEC_OK ||
                hash_results.row_count != expected_join_rows ||
                hash_results.value_checksum != expected_join_checksum ||
                hash_results.row_count != indexed_results.row_count ||
                hash_results.value_checksum != indexed_results.value_checksum ||
                report_indexed_join_timing(host, 'H', finished_ns - started_ns) !=
                    PRDB_QUERY_CALLBACK_OK) {
                static const uint8_t message[] = "fixture-error-hash-join-benchmark";
                return report_failure(host, message, sizeof(message) - 1);
            }
        }
        if (host->write_result(host->context, samples_finished,
                               sizeof(samples_finished) - 1) !=
            PRDB_QUERY_CALLBACK_OK) {
            static const uint8_t message[] = "fixture-error-join-benchmark-finish";
            return report_failure(host, message, sizeof(message) - 1);
        }
        return 0;
    }
    if (invocation->parameters_len == 1 && invocation->parameters != NULL &&
        invocation->parameters[0] == 'C') {
        static const uint8_t update_sql[] =
            "UPDATE grouped_records SET left_key = $2 WHERE left_key = $1";
        static const uint8_t old_key_bytes[] = {0, 0, 0, 4};
        static const uint8_t new_key_bytes[] = {0, 0, 0, 5};
        static const uint8_t confirmed[] = "CANCEL_UPDATE_CONFIRMED";
        prdb_sdk_update_v1 update = {0};
        prdb_sdk_value_v1 parameters[2] = {
            {sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
             PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
             old_key_bytes, sizeof(old_key_bytes)},
            {sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
             PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
             new_key_bytes, sizeof(new_key_bytes)}};
        uint8_t request[4096] = {0};
        uint8_t response[4096] = {0};
        uint8_t relation_response[4096] = {0};
        uint8_t scan_response[4096] = {0};
        uint8_t batch_response[4096] = {0};
        uint8_t target_handles[32] = {0};
        prdb_sdk_executor_relation_v1 relation = {0};
        prdb_sdk_sql_column_shape_v2 columns[2] = {{0}};
        prdb_sdk_select_binding_v2 binding = {0};
        prdb_sdk_value_v1 scratch[2] = {{0}};
        prdb_host_api_v5 cancel_host;
        uint32_t updated = UINT32_MAX;
        int32_t update_status;

        if (optimizer_host_v5 == NULL ||
            optimizer_host_v5->optimizer_request == NULL ||
            prdb_sdk_sql_parse_update_v1(update_sql, sizeof(update_sql) - 1,
                                         &update, sizeof(update), NULL) !=
                PRDB_SDK_SQL_OK) {
            return 1;
        }
        cancel_host = *optimizer_host_v5;
        cancel_update_original_host = optimizer_host_v5;
        cancel_update_ready_sent = 0;
        cancel_host.v4.v3.executor_request = cancel_update_executor_request;
        update_status = prdb_sdk_optimizer_execute_update_ast_v1(
            &cancel_host, &update, parameters, 2, schema, sizeof(schema) - 1,
            16, sizeof(batch_response), request, sizeof(request), response,
            sizeof(response), relation_response, sizeof(relation_response),
            scan_response, sizeof(scan_response), batch_response,
            sizeof(batch_response), &relation, sizeof(relation), columns, 2,
            &binding, sizeof(binding), scratch, 2, target_handles, 2, &updated);
        cancel_update_original_host = NULL;
        if (update_status != PRDB_SDK_OPTIMIZER_PLAN_CANCELLED || updated != 0 ||
            cancel_update_ready_sent == 0 ||
            host->write_result(host->context, confirmed,
                               sizeof(confirmed) - 1) != 0) {
            return 1;
        }
        return 0;
    }
    if (invocation->parameters_len == 1 && invocation->parameters != NULL &&
        invocation->parameters[0] == 'G') {
        static const uint8_t update_sql[] =
            "UPDATE grouped_records SET left_key = $2 WHERE left_key = $1";
        static const uint8_t old_key_bytes[] = {0, 0, 0, 4};
        static const uint8_t new_key_bytes[] = {0, 0, 0, 6};
        static const uint8_t ready[] = "UPDATE_STAGE_FAILURE_READY";
        prdb_sdk_update_v1 update = {0};
        prdb_sdk_value_v1 parameters[2] = {
            {sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
             PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
             old_key_bytes, sizeof(old_key_bytes)},
            {sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
             PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
             new_key_bytes, sizeof(new_key_bytes)}};
        uint8_t request[4096] = {0};
        uint8_t response[4096] = {0};
        uint8_t relation_response[4096] = {0};
        uint8_t scan_response[4096] = {0};
        uint8_t batch_response[4096] = {0};
        uint8_t target_handles[32] = {0};
        prdb_sdk_executor_relation_v1 relation = {0};
        prdb_sdk_sql_column_shape_v2 columns[2] = {{0}};
        prdb_sdk_select_binding_v2 binding = {0};
        prdb_sdk_value_v1 scratch[2] = {{0}};
        prdb_host_api_v5 failure_host;
        uint32_t updated = UINT32_MAX;
        int32_t update_status;

        if (optimizer_host_v5 == NULL ||
            optimizer_host_v5->optimizer_request == NULL ||
            prdb_sdk_sql_parse_update_v1(update_sql, sizeof(update_sql) - 1,
                                         &update, sizeof(update), NULL) !=
                PRDB_SDK_SQL_OK ||
            host->write_result(host->context, ready, sizeof(ready) - 1) != 0) {
            return 1;
        }
        failure_host = *optimizer_host_v5;
        cancel_update_original_host = optimizer_host_v5;
        cancel_update_ready_sent = 1;
        /* Keep each encoded UPDATE batch to one row so the second staging
         * request can fail after the first one has been accepted. */
        fail_next_stage_update = 2;
        failure_host.v4.v3.executor_request = cancel_update_executor_request;
        update_status = prdb_sdk_optimizer_execute_update_ast_v1(
            &failure_host, &update, parameters, 2, schema, sizeof(schema) - 1,
            16, 64, request, sizeof(request), response,
            sizeof(response), relation_response, sizeof(relation_response),
            scan_response, sizeof(scan_response), batch_response,
            sizeof(batch_response), &relation, sizeof(relation), columns, 2,
            &binding, sizeof(binding), scratch, 2, target_handles, 2, &updated);
        cancel_update_original_host = NULL;
        if (update_status != PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR ||
            updated != 0 || fail_next_stage_update != 0) {
            static const uint8_t message[] =
                "fixture-error-update-stage-failure";
            return report_failure(host, message, sizeof(message) - 1);
        }
        return 0;
    }
    if (invocation->parameters_len == 1 && invocation->parameters != NULL &&
        invocation->parameters[0] == 'T') {
        static const uint8_t target_limit_sql[] =
            "UPDATE grouped_records SET left_key = $2 WHERE left_key = $1";
        static const uint8_t old_key_bytes[] = {0, 0, 0, 4};
        static const uint8_t new_key_bytes[] = {0, 0, 0, 5};
        static const uint8_t ready[] = "UPDATE_TARGET_LIMIT_READY";
        prdb_sdk_update_v1 update = {0};
        prdb_sdk_value_v1 parameters[2] = {
            {sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
             PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
             old_key_bytes, sizeof(old_key_bytes)},
            {sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
             PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
             new_key_bytes, sizeof(new_key_bytes)}};
        uint8_t request[4096] = {0};
        uint8_t response[4096] = {0};
        uint8_t relation_response[4096] = {0};
        uint8_t scan_response[4096] = {0};
        uint8_t batch_response[4096] = {0};
        uint8_t target_handles[16] = {0};
        prdb_sdk_executor_relation_v1 relation = {0};
        prdb_sdk_sql_column_shape_v2 columns[2] = {{0}};
        prdb_sdk_select_binding_v2 binding = {0};
        prdb_sdk_value_v1 scratch[2] = {{0}};
        uint32_t updated = UINT32_MAX;
        int32_t update_status;

        if (optimizer_host_v5 == NULL || optimizer_host_v5->optimizer_request == NULL ||
            prdb_sdk_sql_parse_update_v1(
                target_limit_sql, sizeof(target_limit_sql) - 1, &update,
                sizeof(update), NULL) != PRDB_SDK_SQL_OK ||
            host->write_result(host->context, ready, sizeof(ready) - 1) != 0) {
            return 1;
        }
        update_status = prdb_sdk_optimizer_execute_update_ast_v1(
            optimizer_host_v5, &update, parameters, 2, schema,
            sizeof(schema) - 1, 16, sizeof(batch_response), request,
            sizeof(request), response, sizeof(response), relation_response,
            sizeof(relation_response), scan_response, sizeof(scan_response),
            batch_response, sizeof(batch_response), &relation, sizeof(relation),
            columns, 2, &binding, sizeof(binding), scratch, 2, target_handles,
            1, &updated);
        if (update_status != PRDB_SDK_OPTIMIZER_PLAN_REQUEST_LIMIT ||
            updated != 0) {
            static const uint8_t message[] =
                "fixture-error-update-target-limit";
            return report_failure(host, message, sizeof(message) - 1);
        }
        return 0;
    }
    if (invocation->parameters_len == 1 && invocation->parameters != NULL &&
        invocation->parameters[0] == 'F') {
        static const uint8_t denied_update_sql[] =
            "UPDATE linked_records SET match_key = $2 WHERE id = $1";
        static const uint8_t old_id_bytes[] = {0, 0, 0, 42};
        static const uint8_t new_id_bytes[] = {0, 0, 0, 99};
        prdb_sdk_update_v1 denied_update = {0};
        prdb_sdk_value_v1 denied_parameters[2] = {
            {sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
             PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
             old_id_bytes, sizeof(old_id_bytes)},
            {sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
             PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
             new_id_bytes, sizeof(new_id_bytes)}};
        uint8_t request[4096] = {0};
        uint8_t response[4096] = {0};
        uint8_t relation_response[4096] = {0};
        uint8_t scan_response[4096] = {0};
        uint8_t batch_response[4096] = {0};
        uint8_t targets[PRDB_SDK_OPTIMIZER_PLAN_MAX_DELETE_TARGETS * 16] = {0};
        prdb_sdk_executor_relation_v1 relation = {0};
        prdb_sdk_sql_column_shape_v2 columns[2] = {{0}};
        prdb_sdk_select_binding_v2 binding = {0};
        prdb_sdk_value_v1 scratch[2] = {{0}};
        prdb_host_api_v5 observed_host;
        uint32_t updated = 0;
        int32_t update_status;

        if (optimizer_host_v5 == NULL || optimizer_host_v5->optimizer_request == NULL ||
            prdb_sdk_sql_parse_update_v1(
                denied_update_sql, sizeof(denied_update_sql) - 1,
                &denied_update, sizeof(denied_update), NULL) != PRDB_SDK_SQL_OK) {
            return 1;
        }
        {
            static const uint8_t ready[] = "UPDATE_REJECTION_READY";
            if (host->write_result(host->context, ready, sizeof(ready) - 1) != 0) {
                return 1;
            }
        }
        observed_host = *optimizer_host_v5;
        observed_host.v4.v3.executor_request = cancel_update_executor_request;
        cancel_update_original_host = optimizer_host_v5;
        observe_next_stage_update = 1;
        observe_only_stage_update = 1;
        update_status = prdb_sdk_optimizer_execute_update_ast_v1(
            &observed_host, &denied_update, denied_parameters, 2,
            schema, sizeof(schema) - 1, 16, sizeof(batch_response),
            request, sizeof(request), response, sizeof(response),
            relation_response, sizeof(relation_response), scan_response,
            sizeof(scan_response), batch_response, sizeof(batch_response),
            &relation, sizeof(relation), columns, 2, &binding, sizeof(binding),
            scratch, 2, targets, PRDB_SDK_OPTIMIZER_PLAN_MAX_DELETE_TARGETS,
            &updated);
        cancel_update_original_host = NULL;
        observe_only_stage_update = 0;
        if (update_status == PRDB_SDK_OPTIMIZER_PLAN_OK || updated != 0 ||
            observe_next_stage_update != 0) {
            static const uint8_t message[] = "fixture-error-update-was-not-denied";
            return report_failure(host, message, sizeof(message) - 1);
        }
        return 1;
    }
    if (invocation->parameters_len == 1 && invocation->parameters != NULL &&
        invocation->parameters[0] == 'J') {
        static const uint8_t join_sql[] =
            "SELECT a.right_key, b.right_key FROM decimal_grouped_records AS a "
            "INNER JOIN decimal_grouped_records AS b ON "
            "a.left_key = b.left_key AND a.right_key = b.right_key "
            "WHERE b.decimal_value >= $1 LIMIT 0";
        uint8_t mismatched_parameter[sizeof(min_amount_parameter)];
        memcpy(mismatched_parameter, min_amount_parameter,
               sizeof(mismatched_parameter));
        mismatched_parameter[13] = 1;
        status = prdb_sdk_sql_run_statement_v1(
            host, schema, sizeof(schema) - 1, join_sql, sizeof(join_sql) - 1,
            mismatched_parameter, sizeof(mismatched_parameter));
        /* Catch the bind error so only the host latch can abort on success. */
        if (status == PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR) return 0;
        static const uint8_t message[] = "fixture-error-join-parameter-scale";
        return report_failure(host, message, sizeof(message) - 1);
    }
    if (invocation->parameters_len == 1 && invocation->parameters != NULL &&
        invocation->parameters[0] == 'P') {
        uint8_t mismatched_parameter[sizeof(min_amount_parameter)];
        memcpy(mismatched_parameter, min_amount_parameter,
               sizeof(mismatched_parameter));
        mismatched_parameter[13] = 1; /* Declares 12.5, while the operand scale is 2. */
        status = prdb_sdk_sql_run_statement_v1(
            host, schema, sizeof(schema) - 1, single_key_having_sql,
            sizeof(single_key_having_sql) - 1, mismatched_parameter,
            sizeof(mismatched_parameter));
        /* Catch the bind error so only the host latch can abort on success. */
        if (status == PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR) return 0;
        static const uint8_t message[] = "fixture-error-grouped-parameter-scale";
        return report_failure(host, message, sizeof(message) - 1);
    }
    if (invocation->parameters_len == 1 && invocation->parameters != NULL &&
        invocation->parameters[0] == 'H') {
        static const uint8_t redacted_having_sql[] =
            "SELECT left_key, right_key, SUM(decimal_value) "
            "FROM decimal_grouped_records GROUP BY left_key, right_key "
            "HAVING COUNT(*) IS NULL AND SUM(decimal_value) IS NULL";
        uint32_t callbacks = 0;
        status = prdb_sdk_sql_execute_group_v23(
            host, schema, sizeof(schema) - 1, redacted_having_sql,
            sizeof(redacted_having_sql) - 1, NULL, 0, 16,
            sizeof(batch_response), planner_response, sizeof(planner_response),
            executor_response, sizeof(executor_response), batch_response,
            sizeof(batch_response), planner_columns, 9, executor_columns, 9,
            cell_scratch, 9, grouped_materialized, sizeof(grouped_materialized),
            count_unexpected_group, &callbacks);
        /* Catch the precise error and rely on the host latch to abort. */
        if (status == PRDB_SDK_SQL_EXEC_EVALUATION_ERROR && callbacks == 0)
            return 0;
        static const uint8_t message[] = "fixture-error-redacted-having";
        return report_failure(host, message, sizeof(message) - 1);
    }
    if (invocation->parameters_len == 1 && invocation->parameters != NULL &&
        (invocation->parameters[0] == 'V' || invocation->parameters[0] == 'Z' ||
         invocation->parameters[0] == 'I')) {
        static const uint8_t visible_count_column_sql[] =
            "SELECT left_key, right_key, COUNT(decimal_value) "
            "FROM decimal_grouped_records "
            "WHERE decimal_value IS NULL OR decimal_value IS NOT NULL "
            "GROUP BY left_key, right_key "
            "HAVING COUNT(decimal_value) >= 0 "
            "ORDER BY COUNT(decimal_value) DESC NULLS LAST, left_key ASC";
        static const uint8_t redacted_count_column_sql[] =
            "SELECT left_key, right_key, COUNT(decimal_value) "
            "FROM decimal_grouped_records WHERE left_key IS NOT NULL "
            "GROUP BY left_key, right_key "
            "HAVING COUNT(decimal_value) >= 0 "
            "ORDER BY COUNT(decimal_value) DESC NULLS LAST, left_key ASC";
        static const uint8_t visible_bigint_count_column_sql[] =
            "SELECT left_key, right_key, COUNT(bigint_value) "
            "FROM decimal_grouped_records "
            "WHERE bigint_value IS NULL OR bigint_value IS NOT NULL "
            "GROUP BY left_key, right_key "
            "HAVING COUNT(bigint_value) >= 0 "
            "ORDER BY COUNT(bigint_value) DESC NULLS LAST, left_key ASC";
        const int bigint_count_column = invocation->parameters[0] == 'I';
        const int redacted_count_column = invocation->parameters[0] == 'Z';
        const uint8_t *count_column_sql = redacted_count_column
            ? redacted_count_column_sql
            : bigint_count_column ? visible_bigint_count_column_sql
                                  : visible_count_column_sql;
        const uint64_t count_column_sql_len = redacted_count_column
            ? sizeof(redacted_count_column_sql) - 1
            : bigint_count_column ? sizeof(visible_bigint_count_column_sql) - 1
                                  : sizeof(visible_count_column_sql) - 1;
        composite_grouped_results.row_count = 0;
        composite_grouped_results.output_len = 0;
        composite_grouped_results.column_count = 3;
        status = prdb_sdk_sql_execute_group_having_optimizer_v24(
            optimizer_host_v5, schema, sizeof(schema) - 1, count_column_sql,
            count_column_sql_len, NULL, NULL, 0, 16, sizeof(batch_response),
            group_count_optimizer_request, sizeof(group_count_optimizer_request),
            group_count_optimizer_response, sizeof(group_count_optimizer_response),
            planner_response, sizeof(planner_response), executor_response,
            sizeof(executor_response), batch_response, sizeof(batch_response),
            planner_columns, 9, executor_columns, 9, cell_scratch, 9,
            grouped_materialized, sizeof(grouped_materialized),
            write_grouped_composite_count_column_optimizer_v24,
            &composite_grouped_results);
        if (redacted_count_column) {
            if (status == PRDB_SDK_SQL_EXEC_EVALUATION_ERROR &&
                composite_grouped_results.row_count == 0 &&
                composite_grouped_results.output_len == 0) {
                return 1;
            }
            static const uint8_t message[] =
                "fixture-error-redacted-count-column-not-rejected";
            return report_failure(host, message, sizeof(message) - 1);
        }
        if (status == PRDB_SDK_SQL_EXEC_OK &&
            composite_grouped_results.row_count == 3) return 0;
        static uint8_t message[] = "fixture-error-count-column-optimizer-0";
        if (composite_grouped_results.output_len >= 101 &&
            composite_grouped_results.output_len <= 104) {
            message[sizeof(message) - 2] =
                (uint8_t)('0' + composite_grouped_results.output_len - 100);
        } else if (status >= 0 && status <= 9) {
            message[sizeof(message) - 2] = (uint8_t)('0' + status);
        }
        return report_failure(host, message, sizeof(message) - 1);
    }
    if (invocation->parameters_len == 1 && invocation->parameters != NULL &&
        (invocation->parameters[0] == 'A' || invocation->parameters[0] == 'B' ||
         invocation->parameters[0] == 'R' || invocation->parameters[0] == 'W' ||
         invocation->parameters[0] == 'Q' || invocation->parameters[0] == 'N')) {
        static const uint8_t *const optimizer_sql[] = {
            select_composite_min_optimizer_sql,
            select_composite_max_optimizer_sql,
            select_composite_avg_optimizer_sql,
            select_composite_decimal_min_optimizer_sql,
            select_composite_decimal_max_optimizer_sql,
            select_composite_decimal_sum_optimizer_sql,
            select_composite_decimal_avg_optimizer_sql,
            select_composite_bigint_sum_optimizer_sql,
            select_composite_bigint_min_optimizer_sql,
            select_composite_bigint_max_optimizer_sql,
            select_composite_bigint_avg_optimizer_sql,
            select_composite_smallint_sum_optimizer_sql,
            select_composite_smallint_min_optimizer_sql,
            select_composite_smallint_max_optimizer_sql,
            select_composite_smallint_avg_optimizer_sql,
            select_composite_integer_min_optimizer_sql,
            select_composite_integer_max_optimizer_sql,
            select_composite_integer_avg_optimizer_sql};
        static const uint32_t optimizer_sql_len[] = {
            sizeof(select_composite_min_optimizer_sql) - 1,
            sizeof(select_composite_max_optimizer_sql) - 1,
            sizeof(select_composite_avg_optimizer_sql) - 1,
            sizeof(select_composite_decimal_min_optimizer_sql) - 1,
            sizeof(select_composite_decimal_max_optimizer_sql) - 1,
            sizeof(select_composite_decimal_sum_optimizer_sql) - 1,
            sizeof(select_composite_decimal_avg_optimizer_sql) - 1,
            sizeof(select_composite_bigint_sum_optimizer_sql) - 1,
            sizeof(select_composite_bigint_min_optimizer_sql) - 1,
            sizeof(select_composite_bigint_max_optimizer_sql) - 1,
            sizeof(select_composite_bigint_avg_optimizer_sql) - 1,
            sizeof(select_composite_smallint_sum_optimizer_sql) - 1,
            sizeof(select_composite_smallint_min_optimizer_sql) - 1,
            sizeof(select_composite_smallint_max_optimizer_sql) - 1,
            sizeof(select_composite_smallint_avg_optimizer_sql) - 1,
            sizeof(select_composite_integer_min_optimizer_sql) - 1,
            sizeof(select_composite_integer_max_optimizer_sql) - 1,
            sizeof(select_composite_integer_avg_optimizer_sql) - 1};
        static const uint32_t optimizer_kinds[] = {
            PRDB_SDK_SQL_V7_AGGREGATE_MIN,
            PRDB_SDK_SQL_V7_AGGREGATE_MAX,
            PRDB_SDK_SQL_V7_AGGREGATE_AVG,
            PRDB_SDK_SQL_V7_AGGREGATE_MIN,
            PRDB_SDK_SQL_V7_AGGREGATE_MAX,
            PRDB_SDK_SQL_V7_AGGREGATE_SUM,
            PRDB_SDK_SQL_V7_AGGREGATE_AVG,
            PRDB_SDK_SQL_V7_AGGREGATE_SUM,
            PRDB_SDK_SQL_V7_AGGREGATE_MIN,
            PRDB_SDK_SQL_V7_AGGREGATE_MAX,
            PRDB_SDK_SQL_V7_AGGREGATE_AVG,
            PRDB_SDK_SQL_V7_AGGREGATE_SUM,
            PRDB_SDK_SQL_V7_AGGREGATE_MIN,
            PRDB_SDK_SQL_V7_AGGREGATE_MAX,
            PRDB_SDK_SQL_V7_AGGREGATE_AVG,
            PRDB_SDK_SQL_V7_AGGREGATE_MIN,
            PRDB_SDK_SQL_V7_AGGREGATE_MAX,
            PRDB_SDK_SQL_V7_AGGREGATE_AVG};
        if (optimizer_host_v5 == NULL ||
            optimizer_host_v5->optimizer_request == NULL) {
            return 1;
        }
        if (invocation->parameters[0] == 'B') {
            generated_inserted_count = 0;
            status = prdb_sdk_sql_execute_insert_v2(
                host, schema, sizeof(schema) - 1,
                generated_identity_insert_sql,
                sizeof(generated_identity_insert_sql) - 1, NULL, 0,
                planner_response, sizeof(planner_response), executor_response,
                sizeof(executor_response), insert_request,
                sizeof(insert_request), planner_columns, 9, executor_columns,
                9, insert_values, 2, &generated_inserted_count);
            if (status != PRDB_SDK_SQL_EXEC_OK ||
                generated_inserted_count != 1) {
                static const uint8_t message[] =
                    "fixture-error-generated-identity-insert";
                return report_failure(host, message, sizeof(message) - 1);
            }
        }
        if (invocation->parameters[0] == 'A' ||
            invocation->parameters[0] == 'W') {
            grouped_results.group_column = &planner_columns[0];
            grouped_results.aggregate_column = &planner_columns[1];
            grouped_results.output = ordered_pressure_result;
            grouped_results.output_capacity = sizeof(ordered_pressure_result);
            grouped_results.output_len = 0;
            grouped_results.row_count = 0;
            status = prdb_sdk_sql_execute_group_v12(
                host, schema, sizeof(schema) - 1,
                select_ordered_analytical_pressure_sql,
                sizeof(select_ordered_analytical_pressure_sql) - 1, NULL, 0,
                128, sizeof(ordered_pressure_batch_response), planner_response,
                sizeof(planner_response), executor_response,
                sizeof(executor_response), ordered_pressure_batch_response,
                sizeof(ordered_pressure_batch_response), planner_columns, 9,
                executor_columns, 9, cell_scratch, 9,
                analytical_pressure_group_key_bytes,
                sizeof(analytical_pressure_group_key_bytes),
                write_ordered_analytical_pressure_sum, &grouped_results);
            if (status != PRDB_SDK_SQL_EXEC_OK) {
                static uint8_t message[] =
                    "fixture-error-ordered-sum-status-0";
                if (status >= 0 && status <= 9) {
                    message[sizeof(message) - 2] = (uint8_t)('0' + status);
                }
                return report_failure(host, message, sizeof(message) - 1);
            }
            if (grouped_results.row_count != 64) {
                static const uint8_t message[] =
                    "fixture-error-confined-ordered-analytical-pressure-sum-rows";
                return report_failure(host, message, sizeof(message) - 1);
            }
            if (invocation->parameters[0] == 'W') return 0;
        }
        if (invocation->parameters[0] == 'Q') {
            const prdb_sdk_sql_parameter_metadata_v1 filtered_metadata = {
                sizeof(filtered_metadata),
                PRDB_SDK_SQL_PARAMETER_METADATA_ABI_V1_VERSION,
                PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, {0, 0}};
            composite_grouped_results.row_count = 0;
            composite_grouped_results.output_len = 0;
            composite_grouped_results.column_count = 3;
            status = prdb_sdk_sql_execute_group_having_optimizer_v24(
                optimizer_host_v5, schema, sizeof(schema) - 1,
                select_filtered_ordered_composite_decimal_sum_sql,
                sizeof(select_filtered_ordered_composite_decimal_sum_sql) - 1,
                &filtered_group_key_parameter, &filtered_metadata, 1, 16,
                sizeof(batch_response), group_count_optimizer_request,
                sizeof(group_count_optimizer_request),
                group_count_optimizer_response,
                sizeof(group_count_optimizer_response), planner_response,
                sizeof(planner_response), executor_response,
                sizeof(executor_response), batch_response,
                sizeof(batch_response), planner_columns, 9, executor_columns,
                9, cell_scratch, 9, grouped_materialized,
                sizeof(grouped_materialized),
                write_filtered_ordered_composite_decimal_sum_v24,
                &composite_grouped_results);
            if (status != PRDB_SDK_SQL_EXEC_OK ||
                composite_grouped_results.row_count != 1) {
                static uint8_t message[] =
                    "fixture-error-filtered-ordered-composite-sum-status-0";
                if (status >= 0 && status <= 9) {
                    message[sizeof(message) - 2] = (uint8_t)('0' + status);
                }
                return report_failure(host, message, sizeof(message) - 1);
            }
            return 0;
        }
        if (invocation->parameters[0] == 'N') {
            composite_grouped_results.row_count = 0;
            composite_grouped_results.output_len = 0;
            composite_grouped_results.column_count = 3;
            status = prdb_sdk_sql_execute_filtered_group_composite_aggregate_optimizer_v1(
                optimizer_host_v5, schema, sizeof(schema) - 1,
                select_redacted_null_filter_sql,
                sizeof(select_redacted_null_filter_sql) - 1, NULL, 0, 16,
                sizeof(batch_response), group_count_optimizer_request,
                sizeof(group_count_optimizer_request),
                group_count_optimizer_response,
                sizeof(group_count_optimizer_response), planner_response,
                sizeof(planner_response), executor_response,
                sizeof(executor_response), batch_response,
                sizeof(batch_response), planner_columns, 9, executor_columns,
                9, cell_scratch, 9, grouped_materialized,
                sizeof(grouped_materialized),
                write_filtered_composite_decimal_sum_optimizer,
                &composite_grouped_results);
            return status == PRDB_SDK_SQL_EXEC_EVALUATION_ERROR &&
                           composite_grouped_results.row_count == 0 &&
                           composite_grouped_results.output_len == 0
                       ? 1
                       : report_failure(
                             host,
                             (const uint8_t *)
                                 "fixture-error-redacted-null-filter-not-rejected",
                             sizeof("fixture-error-redacted-null-filter-not-rejected") -
                                 1);
        }
        uint32_t first_aggregate = invocation->parameters[0] == 'R'
                                      ? 3
                                      : invocation->parameters[0] == 'B' ? 7 : 0;
        uint32_t aggregate_end = invocation->parameters[0] == 'R'
                                     ? 4
                                     : invocation->parameters[0] == 'B' ? 18 : 7;
        for (uint32_t index = first_aggregate; index < aggregate_end; ++index) {
            composite_aggregate_expectation expectation = {
                &composite_grouped_results, optimizer_kinds[index]};
            ordered_composite_aggregate_expectation ordered_expectation = {0};
            ordered_expectation.aggregate = expectation;
            if (index < 3) {
                ordered_expectation.first_left_key = 4;
                ordered_expectation.first_right_key = 9;
                ordered_expectation.second_left_key = 2;
                ordered_expectation.second_right_key = 9;
                ordered_expectation.first_matching_row_count = 2;
                ordered_expectation.second_matching_row_count = 3;
            } else if (index == 3) {
                ordered_expectation.first_left_key = 1;
                ordered_expectation.first_right_key = 7;
                ordered_expectation.second_left_key = 2;
                ordered_expectation.second_right_key = 9;
                ordered_expectation.first_matching_row_count = 2;
                ordered_expectation.second_matching_row_count = 3;
            } else {
                ordered_expectation.first_left_key = 2;
                ordered_expectation.first_right_key = 9;
                ordered_expectation.second_left_key = 1;
                ordered_expectation.second_right_key = 7;
                ordered_expectation.first_matching_row_count = 3;
                ordered_expectation.second_matching_row_count = 2;
            }
            ordered_expectation.expected_group_count =
                index >= 3 && index <= 17 ? 3 : 2;
            if (ordered_expectation.expected_group_count == 3) {
                ordered_expectation.third_left_key = 3;
                ordered_expectation.third_right_key = 11;
                ordered_expectation.third_matching_row_count = 1;
            }
            composite_grouped_results.row_count = 0;
            composite_grouped_results.output_len = 0;
            if (index <= 17) {
                status = prdb_sdk_sql_execute_ordered_composite_aggregate_optimizer_v1(
                    optimizer_host_v5, schema, sizeof(schema) - 1,
                    optimizer_sql[index], optimizer_sql_len[index], 16,
                    sizeof(batch_response), group_count_optimizer_request,
                    sizeof(group_count_optimizer_request),
                    group_count_optimizer_response,
                    sizeof(group_count_optimizer_response), planner_response,
                    sizeof(planner_response), executor_response,
                    sizeof(executor_response), batch_response,
                    sizeof(batch_response), planner_columns, 9,
                    executor_columns, 9, cell_scratch, 9,
                    grouped_materialized, sizeof(grouped_materialized),
                    write_ordered_composite_group_aggregate_optimizer,
                    &ordered_expectation);
            } else {
                status = prdb_sdk_sql_execute_group_composite_aggregate_optimizer_v1(
                    optimizer_host_v5, schema, sizeof(schema) - 1,
                    optimizer_sql[index], optimizer_sql_len[index], 16,
                    sizeof(batch_response), group_count_optimizer_request,
                    sizeof(group_count_optimizer_request),
                    group_count_optimizer_response,
                    sizeof(group_count_optimizer_response), planner_response,
                    sizeof(planner_response), executor_response,
                    sizeof(executor_response), batch_response,
                    sizeof(batch_response), planner_columns, 9,
                    executor_columns, 9, cell_scratch, 9,
                    grouped_materialized, sizeof(grouped_materialized),
                    write_grouped_composite_aggregate_optimizer, &expectation);
            }
            if (status != PRDB_SDK_SQL_EXEC_OK ||
                composite_grouped_results.row_count !=
                    ordered_expectation.expected_group_count ||
                ordered_expectation.next_group !=
                    ordered_expectation.expected_group_count) {
                if (invocation->parameters[0] == 'R') {
                    if (status == PRDB_SDK_SQL_EXEC_EVALUATION_ERROR &&
                        composite_grouped_results.row_count == 0) {
                        return 1;
                    }
                    static const uint8_t message[] =
                        "fixture-error-redacted-aggregate-not-rejected";
                    return report_failure(host, message, sizeof(message) - 1);
                }
                if (status != PRDB_SDK_SQL_EXEC_OK) {
                    static uint8_t message[] =
                        "fixture-error-composite-aggregate-status-0";
                    if (status >= 0 && status <= 9) {
                        message[sizeof(message) - 2] =
                            (uint8_t)('0' + status);
                    }
                    return report_failure(host, message, sizeof(message) - 1);
                }
                if (index == 7) {
                    static const uint8_t message[] =
                        "fixture-error-confined-bigint-sum-rows";
                    return report_failure(host, message, sizeof(message) - 1);
                }
                if (index == 8) {
                    static const uint8_t message[] =
                        "fixture-error-confined-bigint-min-rows";
                    return report_failure(host, message, sizeof(message) - 1);
                }
                if (index == 9) {
                    static const uint8_t message[] =
                        "fixture-error-confined-bigint-max-rows";
                    return report_failure(host, message, sizeof(message) - 1);
                }
                if (index == 11) {
                    static const uint8_t message[] =
                        "fixture-error-confined-smallint-sum-rows";
                    return report_failure(host, message, sizeof(message) - 1);
                }
                if (index == 12) {
                    static const uint8_t message[] =
                        "fixture-error-confined-smallint-min-rows";
                    return report_failure(host, message, sizeof(message) - 1);
                }
                if (index == 13) {
                    static const uint8_t message[] =
                        "fixture-error-confined-smallint-max-rows";
                    return report_failure(host, message, sizeof(message) - 1);
                }
                if (index == 14) {
                    static const uint8_t message[] =
                        "fixture-error-confined-smallint-avg-rows";
                    return report_failure(host, message, sizeof(message) - 1);
                }
                static const uint8_t message[] =
                    "fixture-error-confined-bigint-avg-rows";
                return report_failure(host, message, sizeof(message) - 1);
            }
        }
        if (invocation->parameters[0] == 'B') {
            composite_aggregate_expectation count_expectation = {
                &composite_grouped_results,
                PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR};
            ordered_composite_aggregate_expectation ordered_count_expectation = {
                .aggregate = count_expectation,
                .first_left_key = 2,
                .first_right_key = 9,
                .second_left_key = 1,
                .second_right_key = 7,
                .third_left_key = 3,
                .third_right_key = 11,
                .first_matching_row_count = 3,
                .second_matching_row_count = 2,
                .third_matching_row_count = 1,
                .expected_group_count = 3};
            composite_grouped_results.row_count = 0;
            composite_grouped_results.output_len = 0;
            status = prdb_sdk_sql_execute_ordered_composite_aggregate_optimizer_v1(
                optimizer_host_v5, schema, sizeof(schema) - 1,
                select_bigint_visible_count_optimizer_sql,
                sizeof(select_bigint_visible_count_optimizer_sql) - 1, 16,
                sizeof(batch_response), group_count_optimizer_request,
                sizeof(group_count_optimizer_request),
                group_count_optimizer_response,
                sizeof(group_count_optimizer_response), planner_response,
                sizeof(planner_response), executor_response,
                sizeof(executor_response), batch_response,
                sizeof(batch_response), planner_columns, 9, executor_columns, 9,
                cell_scratch, 9, grouped_materialized,
                sizeof(grouped_materialized),
                write_ordered_composite_group_aggregate_optimizer,
                &ordered_count_expectation);
            if (status != PRDB_SDK_SQL_EXEC_OK ||
                composite_grouped_results.row_count != 3 ||
                ordered_count_expectation.next_group != 3) {
                static uint8_t message[] =
                    "fixture-error-bigint-visible-count-status-0";
                if (status >= 0 && status <= 9) {
                    message[sizeof(message) - 2] = (uint8_t)('0' + status);
                }
                return report_failure(host, message, sizeof(message) - 1);
            }
        }
        if (invocation->parameters[0] == 'A') {
            composite_aggregate_expectation count_expectation = {
                &composite_grouped_results,
                PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR};
            ordered_composite_aggregate_expectation ordered_count_expectation = {
                .aggregate = count_expectation,
                .first_left_key = 2,
                .first_right_key = 9,
                .second_left_key = 1,
                .second_right_key = 7,
                .third_left_key = 3,
                .third_right_key = 11,
                .first_matching_row_count = 3,
                .second_matching_row_count = 2,
                .third_matching_row_count = 1,
                .expected_group_count = 3};
            composite_grouped_results.row_count = 0;
            composite_grouped_results.output_len = 0;
            status = prdb_sdk_sql_execute_ordered_composite_aggregate_optimizer_v1(
                optimizer_host_v5, schema, sizeof(schema) - 1,
                select_composite_decimal_count_optimizer_sql,
                sizeof(select_composite_decimal_count_optimizer_sql) - 1, 16,
                sizeof(batch_response), group_count_optimizer_request,
                sizeof(group_count_optimizer_request),
                group_count_optimizer_response,
                sizeof(group_count_optimizer_response), planner_response,
                sizeof(planner_response), executor_response,
                sizeof(executor_response), batch_response,
                sizeof(batch_response), planner_columns, 9, executor_columns, 9,
                cell_scratch, 9, grouped_materialized,
                sizeof(grouped_materialized),
                write_ordered_composite_group_aggregate_optimizer,
                &ordered_count_expectation);
            if (status != PRDB_SDK_SQL_EXEC_OK ||
                composite_grouped_results.row_count != 3 ||
                ordered_count_expectation.next_group != 3) {
                static uint8_t message[] =
                    "fixture-error-ordered-composite-count-status-0";
                if (status >= 0 && status <= 9) {
                    message[sizeof(message) - 2] = (uint8_t)('0' + status);
                }
                return report_failure(host, message, sizeof(message) - 1);
            }

            composite_grouped_results.row_count = 0;
            composite_grouped_results.output_len = 0;
            composite_grouped_results.column_count = 3;
            status = prdb_sdk_sql_execute_filtered_group_composite_aggregate_optimizer_v1(
                optimizer_host_v5, schema, sizeof(schema) - 1,
                select_filtered_composite_decimal_sum_sql,
                sizeof(select_filtered_composite_decimal_sum_sql) - 1,
                &filtered_group_key_parameter, 1, 16, sizeof(batch_response),
                group_count_optimizer_request,
                sizeof(group_count_optimizer_request),
                group_count_optimizer_response,
                sizeof(group_count_optimizer_response), planner_response,
                sizeof(planner_response), executor_response,
                sizeof(executor_response), batch_response,
                sizeof(batch_response), planner_columns, 9, executor_columns,
                9, cell_scratch, 9, grouped_materialized,
                sizeof(grouped_materialized),
                write_filtered_composite_decimal_sum_optimizer,
                &composite_grouped_results);
            if (status != PRDB_SDK_SQL_EXEC_OK ||
                composite_grouped_results.row_count != 1) {
                static uint8_t message[] =
                    "fixture-error-filtered-composite-sum-status-0";
                if (status >= 0 && status <= 9) {
                    message[sizeof(message) - 2] = (uint8_t)('0' + status);
                }
                return report_failure(host, message, sizeof(message) - 1);
            }
        }
        if (invocation->parameters[0] == 'A') {
            static const uint8_t null_having_sql[] =
                "SELECT left_key, right_key, SUM(decimal_value) "
                "FROM decimal_grouped_records GROUP BY left_key, right_key "
                "HAVING SUM(decimal_value) IS NULL "
                "AND COUNT(*) IS NOT NULL AND COUNT(*) >= 1 "
                "ORDER BY left_key LIMIT 1";
            status = prdb_sdk_sql_run_statement_v1(
                host, schema, sizeof(schema) - 1, null_having_sql,
                sizeof(null_having_sql) - 1, NULL, 0);
            if (status != PRDB_SDK_SQL_DISPATCH_OK) {
                static const uint8_t message[] = "fixture-error-null-having";
                return report_failure(host, message, sizeof(message) - 1);
            }

            status = prdb_sdk_sql_run_statement_v1(
                host, schema, sizeof(schema) - 1, single_key_having_sql,
                sizeof(single_key_having_sql) - 1, min_amount_parameter,
                sizeof(min_amount_parameter));
            if (status != PRDB_SDK_SQL_DISPATCH_OK) {
                static const uint8_t message[] = "fixture-error-single-key-having";
                return report_failure(host, message, sizeof(message) - 1);
            }
        }
        if (invocation->parameters[0] == 'A') {
            static const uint8_t measure_100_bytes[4] = {0, 0, 0, 100};
            prdb_sdk_value_v1 measure_100 = {
                sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
                PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
                measure_100_bytes, sizeof(measure_100_bytes)};
            composite_grouped_result_context text_min_results = {
                host, executor_columns, 3, analytical_pressure_group_key_bytes,
                sizeof(analytical_pressure_group_key_bytes), 0, 0};
            status = prdb_sdk_sql_execute_filtered_group_composite_aggregate_optimizer_v1(
                optimizer_host_v5, schema, sizeof(schema) - 1,
                select_filtered_composite_text_min_sql,
                sizeof(select_filtered_composite_text_min_sql) - 1,
                &measure_100, 1, 16, sizeof(ordered_pressure_batch_response),
                group_count_optimizer_request,
                sizeof(group_count_optimizer_request),
                group_count_optimizer_response,
                sizeof(group_count_optimizer_response), planner_response,
                sizeof(planner_response), executor_response,
                sizeof(executor_response), ordered_pressure_batch_response,
                sizeof(ordered_pressure_batch_response), planner_columns, 9,
                executor_columns, 9, cell_scratch, 9,
                composite_join_materialized_bytes,
                sizeof(composite_join_materialized_bytes),
                write_filtered_composite_text_min_optimizer,
                &text_min_results);
            if (status != PRDB_SDK_SQL_EXEC_OK ||
                text_min_results.row_count != 1 ||
                text_min_results.output_len == 0) {
                static uint8_t message[] =
                    "fixture-error-filtered-composite-text-min-status-0";
                if (status >= 0 && status <= 9) {
                    message[sizeof(message) - 2] = (uint8_t)('0' + status);
                }
                return report_failure(host, message, sizeof(message) - 1);
            }
        }
        return 0;
    }
    results.host = host;
    status = prdb_sdk_sql_execute_group_v6(
        host, schema, sizeof(schema) - 1, select_analytical_pressure_sql,
        sizeof(select_analytical_pressure_sql) - 1, NULL, 0, 128,
        PRDB_SDK_BATCH_MAX_BYTES, planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response),
        analytical_pressure_batch_response,
        sizeof(analytical_pressure_batch_response), planner_columns, 9,
        executor_columns, 9, cell_scratch, 9, analytical_pressure_group_key_bytes,
        sizeof(analytical_pressure_group_key_bytes),
        verify_analytical_pressure_sum, &analytical_pressure_results);
    if (status != PRDB_SDK_SQL_EXEC_OK ||
        analytical_pressure_results.group_count != 64) {
        static const uint8_t message[] = "fixture-error-analytical-pressure-group";
        return report_failure(host, message, sizeof(message) - 1);
    }
    parameter.struct_size = sizeof(parameter);
    parameter.abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
    parameter.state = PRDB_SDK_VALUE_STATE_VALUE;
    parameter.type_code = PRDB_SDK_VALUE_TYPE_INTEGER;
    parameter.data = integer_bytes;
    parameter.data_len = sizeof(integer_bytes);
    metadata_parameter.struct_size = sizeof(metadata_parameter);
    metadata_parameter.abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
    metadata_parameter.state = PRDB_SDK_VALUE_STATE_VALUE;
    metadata_parameter.type_code = PRDB_SDK_VALUE_TYPE_TEXT;
    metadata_parameter.data = information_schema;
    metadata_parameter.data_len = sizeof(information_schema) - 1;
    records_table_parameter.struct_size = sizeof(records_table_parameter);
    records_table_parameter.abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
    records_table_parameter.state = PRDB_SDK_VALUE_STATE_VALUE;
    records_table_parameter.type_code = PRDB_SDK_VALUE_TYPE_TEXT;
    records_table_parameter.data = records_table_name;
    records_table_parameter.data_len = sizeof(records_table_name) - 1;
    private_table_parameter.struct_size = sizeof(private_table_parameter);
    private_table_parameter.abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
    private_table_parameter.state = PRDB_SDK_VALUE_STATE_VALUE;
    private_table_parameter.type_code = PRDB_SDK_VALUE_TYPE_TEXT;
    private_table_parameter.data = private_table_name;
    private_table_parameter.data_len = sizeof(private_table_name) - 1;

    status = prdb_sdk_sql_execute_insert_v1(
        host, schema, sizeof(schema) - 1, insert_sql, sizeof(insert_sql) - 1,
        NULL, 0, planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), insert_request, sizeof(insert_request),
        planner_columns, 1, executor_columns, 1, insert_values, 1,
        inserted_handle);
    if (status != PRDB_SDK_SQL_EXEC_OK) {
        static const uint8_t planner_message[] = "fixture-error-insert-planner";
        static const uint8_t executor_message[] = "fixture-error-insert-executor";
        static const uint8_t shape_message[] = "fixture-error-insert-shape";
        static const uint8_t bind_message[] = "fixture-error-insert-bind";
        if (status == PRDB_SDK_SQL_EXEC_PLANNER_ERROR) {
            return report_failure(host, planner_message,
                                  sizeof(planner_message) - 1);
        }
        if (status == PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR) {
            return report_failure(host, executor_message,
                                  sizeof(executor_message) - 1);
        }
        if (status == PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH) {
            return report_failure(host, shape_message,
                                  sizeof(shape_message) - 1);
        }
        return report_failure(host, bind_message, sizeof(bind_message) - 1);
    }

    results.expected_state = PRDB_SDK_VALUE_STATE_NULL;
    results.marker = inserted_marker;
    results.marker_len = sizeof(inserted_marker) - 1;
    status = prdb_sdk_sql_execute_select_v2(
        host, schema, sizeof(schema) - 1, select_null_sql,
        sizeof(select_null_sql) - 1,
        NULL, 0, 16, sizeof(batch_response), planner_response,
        sizeof(planner_response), executor_response, sizeof(executor_response),
        batch_response, sizeof(batch_response), planner_columns, 1,
        executor_columns, 1, cell_scratch, 1, verify_selected_value, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 1) {
        static const uint8_t message[] = "fixture-error-null-select";
        return report_failure(host, message, sizeof(message) - 1);
    }

    results.expected_state = PRDB_SDK_VALUE_STATE_VALUE;
    results.expected_value = information_schema;
    results.expected_value_len = sizeof(information_schema) - 1;
    results.marker = NULL;
    results.marker_len = 0;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_schemata_sql, sizeof(select_schemata_sql) - 1,
        &metadata_parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 1, executor_columns, 1,
        cell_scratch, 1, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 1) {
        static const uint8_t message[] = "fixture-error-schemata-select";
        return report_failure(host, message, sizeof(message) - 1);
    }

    results.expected_value = first_information_schema_table;
    results.expected_value_len = sizeof(first_information_schema_table) - 1;
    results.expected_ordinal = 1;
    results.marker = NULL;
    results.marker_len = 0;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_tables_sql, sizeof(select_tables_sql) - 1,
        &metadata_parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK) {
        static const uint8_t planner_message[] = "fixture-error-tables-planner";
        static const uint8_t executor_message[] = "fixture-error-tables-executor";
        static const uint8_t shape_message[] = "fixture-error-tables-shape";
        static const uint8_t bind_message[] = "fixture-error-tables-bind";
        static const uint8_t evaluation_message[] = "fixture-error-tables-evaluation";
        static const uint8_t result_message[] = "fixture-error-tables-result";
        static const uint8_t request_message[] = "fixture-error-tables-request";
        static const uint8_t materialization_message[] =
            "fixture-error-tables-materialization";
        if (status == PRDB_SDK_SQL_EXEC_PLANNER_ERROR) {
            return report_failure(host, planner_message, sizeof(planner_message) - 1);
        }
        if (status == PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR) {
            return report_failure(host, executor_message, sizeof(executor_message) - 1);
        }
        if (status == PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH) {
            return report_failure(host, shape_message, sizeof(shape_message) - 1);
        }
        if (status == PRDB_SDK_SQL_EXEC_BIND_ERROR) {
            return report_failure(host, bind_message, sizeof(bind_message) - 1);
        }
        if (status == PRDB_SDK_SQL_EXEC_EVALUATION_ERROR) {
            return report_failure(host, evaluation_message, sizeof(evaluation_message) - 1);
        }
        if (status == PRDB_SDK_SQL_EXEC_RESULT_ERROR) {
            return report_failure(host, result_message, sizeof(result_message) - 1);
        }
        if (status == PRDB_SDK_SQL_EXEC_REQUEST_LIMIT) {
            return report_failure(host, request_message, sizeof(request_message) - 1);
        }
        if (status == PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT) {
            return report_failure(host, materialization_message,
                                  sizeof(materialization_message) - 1);
        }
        static const uint8_t message[] = "fixture-error-tables-status";
        return report_failure(host, message, sizeof(message) - 1);
    }
    if (results.row_count != 1) {
        static const uint8_t message[] = "fixture-error-tables-count";
        return report_failure(host, message, sizeof(message) - 1);
    }

    results.expected_value = first_information_schema_column;
    results.expected_value_len = sizeof(first_information_schema_column) - 1;
    results.expected_ordinal = 2;
    results.marker = NULL;
    results.marker_len = 0;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_columns_sql, sizeof(select_columns_sql) - 1,
        &metadata_parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 1) {
        static const uint8_t header_message[] = "fixture-error-columns-header";
        static const uint8_t cells_message[] = "fixture-error-columns-cells";
        static const uint8_t value_message[] = "fixture-error-columns-value";
        static const uint8_t status_message[] = "fixture-error-columns-status";
        static const uint8_t count_message[] = "fixture-error-columns-count";
        if (results.failure_code == 1) {
            return report_failure(host, header_message, sizeof(header_message) - 1);
        }
        if (results.failure_code == 2) {
            return report_failure(host, cells_message, sizeof(cells_message) - 1);
        }
        if (results.failure_code == 3) {
            return report_failure(host, value_message, sizeof(value_message) - 1);
        }
        if (status != PRDB_SDK_SQL_EXEC_OK) {
            return report_failure(host, status_message, sizeof(status_message) - 1);
        }
        return report_failure(host, count_message, sizeof(count_message) - 1);
    }

    metadata_parameter.data = schema;
    metadata_parameter.data_len = sizeof(schema) - 1;
    records_metadata_parameters[0] = metadata_parameter;
    records_metadata_parameters[1] = records_table_parameter;
    results.expected_value = schema;
    results.expected_value_len = sizeof(schema) - 1;
    results.expected_ordinal = 0;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_schemata_sql, sizeof(select_schemata_sql) - 1,
        &metadata_parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 1, executor_columns, 1,
        cell_scratch, 1, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 1) {
        static const uint8_t message[] = "fixture-error-user-schema-select";
        return report_failure(host, message, sizeof(message) - 1);
    }

    results.expected_value = NULL;
    results.expected_value_len = 0;
    results.expected_ordinal = 1;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_tables_sql, sizeof(select_tables_sql) - 1,
        &metadata_parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 1) {
        static const uint8_t message[] = "fixture-error-user-table-select";
        return report_failure(host, message, sizeof(message) - 1);
    }

    results.expected_value = expected_user_column;
    results.expected_value_len = sizeof(expected_user_column) - 1;
    results.expected_ordinal = 2;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_records_columns_sql, sizeof(select_records_columns_sql) - 1,
        records_metadata_parameters, 2, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 1) {
        static const uint8_t message[] = "fixture-error-user-column-select";
        return report_failure(host, message, sizeof(message) - 1);
    }

    metadata_parameter.data = empty_schema;
    metadata_parameter.data_len = sizeof(empty_schema) - 1;
    results.expected_value = empty_schema;
    results.expected_value_len = sizeof(empty_schema) - 1;
    results.expected_ordinal = 0;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_schemata_sql, sizeof(select_schemata_sql) - 1,
        &metadata_parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 1, executor_columns, 1,
        cell_scratch, 1, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 1) {
        static const uint8_t message[] = "fixture-error-empty-schema-select";
        return report_failure(host, message, sizeof(message) - 1);
    }

    results.expected_value = NULL;
    results.expected_value_len = 0;
    results.expected_ordinal = 1;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_tables_sql, sizeof(select_tables_sql) - 1,
        &metadata_parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 0) {
        static const uint8_t message[] = "fixture-error-empty-schema-tables";
        return report_failure(host, message, sizeof(message) - 1);
    }

    results.expected_ordinal = 2;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_columns_sql, sizeof(select_columns_sql) - 1,
        &metadata_parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 0) {
        static const uint8_t message[] = "fixture-error-empty-schema-columns";
        return report_failure(host, message, sizeof(message) - 1);
    }

    metadata_parameter.data = schema;
    metadata_parameter.data_len = sizeof(schema) - 1;
    private_metadata_parameters[0] = metadata_parameter;
    private_metadata_parameters[1] = private_table_parameter;
    results.expected_value = private_table_name;
    results.expected_value_len = sizeof(private_table_name) - 1;
    results.expected_ordinal = 1;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_private_table_sql, sizeof(select_private_table_sql) - 1,
        private_metadata_parameters, 2, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 0) {
        static const uint8_t message[] = "fixture-error-private-table-leaked";
        return report_failure(host, message, sizeof(message) - 1);
    }

    results.expected_ordinal = 2;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_private_columns_sql, sizeof(select_private_columns_sql) - 1,
        private_metadata_parameters, 2, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 0) {
        static const uint8_t message[] = "fixture-error-private-columns-leaked";
        return report_failure(host, message, sizeof(message) - 1);
    }

    private_table_parameter.data = missing_table_name;
    private_table_parameter.data_len = sizeof(missing_table_name) - 1;
    private_metadata_parameters[1] = private_table_parameter;
    results.expected_value = missing_table_name;
    results.expected_value_len = sizeof(missing_table_name) - 1;
    results.expected_ordinal = 1;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_private_table_sql, sizeof(select_private_table_sql) - 1,
        private_metadata_parameters, 2, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 0) {
        static const uint8_t message[] = "fixture-error-missing-table-result";
        return report_failure(host, message, sizeof(message) - 1);
    }

    results.expected_ordinal = 2;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_private_columns_sql, sizeof(select_private_columns_sql) - 1,
        private_metadata_parameters, 2, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 0) {
        static const uint8_t message[] = "fixture-error-missing-columns-result";
        return report_failure(host, message, sizeof(message) - 1);
    }

    metadata_parameter.data = hidden_schema_name;
    metadata_parameter.data_len = sizeof(hidden_schema_name) - 1;
    results.expected_value = NULL;
    results.expected_value_len = 0;
    results.expected_ordinal = 0;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_schemata_sql, sizeof(select_schemata_sql) - 1,
        &metadata_parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 1, executor_columns, 1,
        cell_scratch, 1, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 0) {
        static const uint8_t message[] = "fixture-error-hidden-schema-visible";
        return report_failure(host, message, sizeof(message) - 1);
    }

    results.expected_ordinal = 1;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_tables_sql, sizeof(select_tables_sql) - 1,
        &metadata_parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 0) {
        static const uint8_t message[] = "fixture-error-hidden-schema-tables-visible";
        return report_failure(host, message, sizeof(message) - 1);
    }

    results.expected_ordinal = 2;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_columns_sql, sizeof(select_columns_sql) - 1,
        &metadata_parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 0) {
        static const uint8_t message[] = "fixture-error-hidden-schema-columns-visible";
        return report_failure(host, message, sizeof(message) - 1);
    }

    metadata_parameter.data = missing_schema_name;
    metadata_parameter.data_len = sizeof(missing_schema_name) - 1;
    results.expected_ordinal = 0;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_schemata_sql, sizeof(select_schemata_sql) - 1,
        &metadata_parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 1, executor_columns, 1,
        cell_scratch, 1, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 0) {
        static const uint8_t message[] = "fixture-error-missing-schema-result";
        return report_failure(host, message, sizeof(message) - 1);
    }

    results.expected_ordinal = 1;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_tables_sql, sizeof(select_tables_sql) - 1,
        &metadata_parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 0) {
        static const uint8_t message[] = "fixture-error-missing-schema-tables-result";
        return report_failure(host, message, sizeof(message) - 1);
    }

    results.expected_ordinal = 2;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, information_schema, sizeof(information_schema) - 1,
        select_columns_sql, sizeof(select_columns_sql) - 1,
        &metadata_parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, verify_information_schema_name, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 0) {
        static const uint8_t message[] = "fixture-error-missing-schema-columns-result";
        return report_failure(host, message, sizeof(message) - 1);
    }

    status = prdb_sdk_sql_execute_update_v1(
        host, schema, sizeof(schema) - 1, update_sql, sizeof(update_sql) - 1,
        &parameter, 1, 16, sizeof(batch_response), planner_response,
        sizeof(planner_response), executor_response, sizeof(executor_response),
        batch_response, sizeof(batch_response), update_request,
        sizeof(update_request), planner_columns, 1, executor_columns, 1,
        &binding, sizeof(binding), cell_scratch, 1, update_targets,
        PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS, update_patches, 1,
        &updated_count);
    if (status != PRDB_SDK_SQL_EXEC_OK || updated_count != 1) {
        static const uint8_t message[] = "fixture-error-update";
        return report_failure(host, message, sizeof(message) - 1);
    }

    results.expected_state = PRDB_SDK_VALUE_STATE_VALUE;
    results.expected_value = integer_bytes;
    results.expected_value_len = sizeof(integer_bytes);
    results.marker = updated_marker;
    results.marker_len = sizeof(updated_marker) - 1;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, schema, sizeof(schema) - 1, select_value_sql,
        sizeof(select_value_sql) - 1, &parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 1, executor_columns, 1,
        cell_scratch, 1, verify_selected_value, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 1) {
        static const uint8_t message[] = "fixture-error-value-select";
        return report_failure(host, message, sizeof(message) - 1);
    }



    composite_grouped_results.row_count = 0;
    composite_grouped_results.output_len = 0;
    if (optimizer_host_v5 != NULL) {
        status = prdb_sdk_sql_execute_group_composite_aggregate_optimizer_v1(
            optimizer_host_v5, schema, sizeof(schema) - 1,
            select_composite_group_sql,
            sizeof(select_composite_group_sql) - 1, 16,
            sizeof(batch_response), group_count_optimizer_request,
            sizeof(group_count_optimizer_request),
            group_count_optimizer_response,
            sizeof(group_count_optimizer_response), planner_response,
            sizeof(planner_response), executor_response,
            sizeof(executor_response), batch_response,
            sizeof(batch_response), planner_columns, 9, executor_columns, 9,
            cell_scratch, 9, grouped_materialized,
            sizeof(grouped_materialized), write_grouped_composite_count,
            &composite_grouped_results);
    } else {
        status = prdb_sdk_sql_execute_group_v13(
            host, schema, sizeof(schema) - 1, select_composite_group_sql,
            sizeof(select_composite_group_sql) - 1, NULL, 0, 16,
            sizeof(batch_response), planner_response,
            sizeof(planner_response), executor_response,
            sizeof(executor_response), batch_response,
            sizeof(batch_response), planner_columns, 9, executor_columns, 9,
            cell_scratch, 9, grouped_materialized,
            sizeof(grouped_materialized), write_grouped_composite_count,
            &composite_grouped_results);
    }
    if (status != PRDB_SDK_SQL_EXEC_OK ||
        composite_grouped_results.row_count != 2) {
        static const uint8_t message[] = "fixture-error-composite-group";
        return report_failure(host, message, sizeof(message) - 1);
    }

    composite_grouped_results.row_count = 0;
    composite_grouped_results.output_len = 0;
    if (optimizer_host_v5 != NULL) {
        status = prdb_sdk_sql_execute_ordered_composite_count_optimizer_v1(
            optimizer_host_v5, schema, sizeof(schema) - 1,
            select_composite_order_sql, sizeof(select_composite_order_sql) - 1,
            16, sizeof(batch_response), group_count_optimizer_request,
            sizeof(group_count_optimizer_request), group_count_optimizer_response,
            sizeof(group_count_optimizer_response), planner_response,
            sizeof(planner_response), executor_response,
            sizeof(executor_response), batch_response, sizeof(batch_response),
            planner_columns, 9, executor_columns, 9, cell_scratch, 9,
            grouped_materialized, sizeof(grouped_materialized),
            write_grouped_composite_count_v15, &composite_grouped_results);
    } else {
        status = prdb_sdk_sql_execute_group_v15(
            host, schema, sizeof(schema) - 1, select_composite_order_sql,
            sizeof(select_composite_order_sql) - 1, NULL, 0, 16,
            sizeof(batch_response), planner_response, sizeof(planner_response),
            executor_response, sizeof(executor_response), batch_response,
            sizeof(batch_response), planner_columns, 9, executor_columns, 9,
            cell_scratch, 9, grouped_materialized, sizeof(grouped_materialized),
            write_grouped_composite_count_v15, &composite_grouped_results);
    }
    if (status != PRDB_SDK_SQL_EXEC_OK ||
        composite_grouped_results.row_count != 2) {
        static const uint8_t message[] = "fixture-error-composite-order";
        return report_failure(host, message, sizeof(message) - 1);
    }

    composite_grouped_results.row_count = 0;
    composite_grouped_results.output_len = 0;
    status = prdb_sdk_sql_execute_group_v17(
        host, schema, sizeof(schema) - 1, select_composite_literal_sql,
        sizeof(select_composite_literal_sql) - 1, NULL, 0, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, grouped_materialized, sizeof(grouped_materialized),
        write_grouped_composite_count_v17, &composite_grouped_results);
    if (status != PRDB_SDK_SQL_EXEC_OK ||
        composite_grouped_results.row_count != 2) {
        static const uint8_t message[] = "fixture-error-composite-literal";
        return report_failure(host, message, sizeof(message) - 1);
    }

    composite_grouped_results.row_count = 0;
    composite_grouped_results.output_len = 0;
    status = prdb_sdk_sql_execute_group_v18(
        host, schema, sizeof(schema) - 1, select_composite_boolean_sql,
        sizeof(select_composite_boolean_sql) - 1, NULL, 0, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, grouped_materialized, sizeof(grouped_materialized),
        write_grouped_composite_count_v18, &composite_grouped_results);
    if (status != PRDB_SDK_SQL_EXEC_OK ||
        composite_grouped_results.row_count != 2) {
        static const uint8_t message[] = "fixture-error-composite-boolean";
        return report_failure(host, message, sizeof(message) - 1);
    }

    composite_grouped_results.row_count = 0;
    composite_grouped_results.output_len = 0;
    composite_grouped_results.column_count = 3;
    composite_grouped_results.row_count = 0;
    composite_grouped_results.output_len = 0;
    status = prdb_sdk_sql_execute_group_v21(
        host, schema, sizeof(schema) - 1,
        select_composite_avg_numeric_v21_sql,
        sizeof(select_composite_avg_numeric_v21_sql) - 1, NULL, 0, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, grouped_materialized, sizeof(grouped_materialized),
        write_grouped_composite_avg_v21, &composite_grouped_results);
    if (status != PRDB_SDK_SQL_EXEC_OK) {
        uint8_t message[] = "fixture-error-v21-avg-status-0";
        if (status >= 0 && status <= 9)
            message[sizeof(message) - 2] = (uint8_t)('0' + status);
        return report_failure(host, message, sizeof(message) - 1);
    }
    if (composite_grouped_results.row_count != 1) {
        static const uint8_t message[] =
            "fixture-error-composite-avg-numeric-v21-row-count";
        return report_failure(host, message, sizeof(message) - 1);
    }

    composite_grouped_results.row_count = 0;
    composite_grouped_results.output_len = 0;
    status = prdb_sdk_sql_execute_group_v21(
        host, schema, sizeof(schema) - 1, select_composite_min_numeric_v21_sql,
        sizeof(select_composite_min_numeric_v21_sql) - 1, NULL, 0, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 9, executor_columns, 9,
        cell_scratch, 9, grouped_materialized, sizeof(grouped_materialized),
        write_grouped_composite_min_v21, &composite_grouped_results);
    if (status != PRDB_SDK_SQL_EXEC_OK) {
        return report_failure(host, (const uint8_t *)
                                       "fixture-error-composite-min-v21",
                              sizeof("fixture-error-composite-min-v21") - 1);
    }
    if (composite_grouped_results.row_count != 1) {
        uint8_t message[] = "fixture-error-composite-min-v21-count-00";
        uint32_t count = composite_grouped_results.row_count;
        if (count > 99) count = 99;
        message[sizeof(message) - 3] = (uint8_t)('0' + count / 10);
        message[sizeof(message) - 2] = (uint8_t)('0' + count % 10);
        return report_failure(host, message, sizeof(message) - 1);
    }

    composite_grouped_results.column_count = 2;

    status = prdb_sdk_sql_run_statement_v1(
        host, schema, sizeof(schema) - 1,
        select_composite_hidden_count_dispatch_sql,
        sizeof(select_composite_hidden_count_dispatch_sql) - 1, NULL, 0);
    if (status != PRDB_SDK_SQL_DISPATCH_OK) {
        static const uint8_t message[] =
            "fixture-error-composite-hidden-count-dispatch";
        return report_failure(host, message, sizeof(message) - 1);
    }

    composite_grouped_results.row_count = 0;
    composite_grouped_results.output_len = 0;
    if (optimizer_host_v5 != NULL) {
        status = prdb_sdk_sql_execute_group_having_optimizer_v24(
            optimizer_host_v5, schema, sizeof(schema) - 1,
            select_composite_sum_optimizer_sql,
            sizeof(select_composite_sum_optimizer_sql) - 1, NULL, NULL, 0,
            16, sizeof(batch_response), group_count_optimizer_request,
            sizeof(group_count_optimizer_request),
            group_count_optimizer_response,
            sizeof(group_count_optimizer_response), planner_response,
            sizeof(planner_response), executor_response,
            sizeof(executor_response), batch_response,
            sizeof(batch_response), planner_columns, 9, executor_columns, 9,
            cell_scratch, 9, grouped_materialized,
            sizeof(grouped_materialized),
            write_grouped_composite_sum_optimizer_v24,
            &composite_grouped_results);
    } else {
        status = prdb_sdk_sql_execute_group_v24(
            host, schema, sizeof(schema) - 1,
            select_composite_sum_optimizer_sql,
            sizeof(select_composite_sum_optimizer_sql) - 1, NULL, 0, 16,
            sizeof(batch_response), planner_response,
            sizeof(planner_response), executor_response,
            sizeof(executor_response), batch_response,
            sizeof(batch_response), planner_columns, 9, executor_columns, 9,
            cell_scratch, 9, grouped_materialized,
            sizeof(grouped_materialized),
            write_grouped_composite_sum_optimizer_v24,
            &composite_grouped_results);
    }
    if (status != PRDB_SDK_SQL_EXEC_OK ||
        composite_grouped_results.row_count != 2) {
        static const uint8_t message[] = "fixture-error-composite-optimizer-sum";
        return report_failure(host, message, sizeof(message) - 1);
    }

    grouped_results.row_count = 0;
    grouped_results.output_len = 0;
    grouped_results.group_column = &planner_columns[1];
    grouped_results.aggregate_column = &planner_columns[0];
    if (optimizer_host_v5 != NULL) {
        status = prdb_sdk_sql_execute_group_aggregate_ordered_optimizer_v1(
            optimizer_host_v5, schema, sizeof(schema) - 1,
            select_ordered_grouped_avg_sql,
            sizeof(select_ordered_grouped_avg_sql) - 1, 16,
            sizeof(batch_response), group_count_optimizer_request,
            sizeof(group_count_optimizer_request),
            group_count_optimizer_response,
            sizeof(group_count_optimizer_response), planner_response,
            sizeof(planner_response), executor_response,
            sizeof(executor_response), batch_response,
            sizeof(batch_response), planner_columns, 9, executor_columns, 9,
            cell_scratch, 9, grouped_materialized,
            sizeof(grouped_materialized), write_ordered_grouped_avg,
            &grouped_results);
    } else {
        status = prdb_sdk_sql_execute_group_v12(
            host, schema, sizeof(schema) - 1, select_ordered_grouped_avg_sql,
            sizeof(select_ordered_grouped_avg_sql) - 1, NULL, 0, 16,
            sizeof(batch_response), planner_response,
            sizeof(planner_response), executor_response,
            sizeof(executor_response), batch_response,
            sizeof(batch_response), planner_columns, 9, executor_columns, 9,
            cell_scratch, 9, grouped_materialized,
            sizeof(grouped_materialized), write_ordered_grouped_avg,
            &grouped_results);
    }
    if (status != PRDB_SDK_SQL_EXEC_OK || grouped_results.row_count != 2) {
        static const uint8_t message[] = "fixture-error-optimizer-ordered-avg";
        return report_failure(host, message, sizeof(message) - 1);
    }
    grouped_results.group_column = &grouped_column;
    grouped_results.aggregate_column = &grouped_column;

    grouped_results.row_count = 0;
    grouped_results.output_len = 0;
    if (optimizer_host_v5 != NULL) {
        status = prdb_sdk_sql_execute_group_count_ordered_optimizer_v1(
            optimizer_host_v5, schema, sizeof(schema) - 1,
            select_grouped_order_sql, sizeof(select_grouped_order_sql) - 1,
            NULL, 0, 16, sizeof(batch_response),
            group_count_optimizer_request, sizeof(group_count_optimizer_request),
            group_count_optimizer_response,
            sizeof(group_count_optimizer_response), planner_response,
            sizeof(planner_response), executor_response,
            sizeof(executor_response), batch_response,
            sizeof(batch_response), planner_columns, 1, executor_columns, 1,
            cell_scratch, 1, grouped_materialized,
            sizeof(grouped_materialized), write_grouped_ordered_count,
            &grouped_results);
    } else {
        status = prdb_sdk_sql_execute_group_v12(
            host, schema, sizeof(schema) - 1, select_grouped_order_sql,
            sizeof(select_grouped_order_sql) - 1, NULL, 0, 16,
            sizeof(batch_response), planner_response,
            sizeof(planner_response), executor_response,
            sizeof(executor_response), batch_response,
            sizeof(batch_response), planner_columns, 1, executor_columns, 1,
            cell_scratch, 1, grouped_materialized,
            sizeof(grouped_materialized), write_grouped_ordered_count,
            &grouped_results);
    }
    if (status != PRDB_SDK_SQL_EXEC_OK || grouped_results.row_count != 1) {
        static const uint8_t message[] = "fixture-error-grouped-order";
        return report_failure(host, message, sizeof(message) - 1);
    }

    status = prdb_sdk_sql_run_statement_v1(
        host, schema, sizeof(schema) - 1, select_join_sql,
        sizeof(select_join_sql) - 1, NULL, 0);
    if (status != PRDB_SDK_SQL_DISPATCH_OK) {
        static const uint8_t message[] = "fixture-error-inner-join";
        return report_failure(host, message, sizeof(message) - 1);
    }

    joined_results.row_count = 0;
    joined_results.output_len = 0;
    joined_results.left_columns = composite_join_left_executor_columns;
    joined_results.right_columns = composite_join_right_executor_columns;
    if (optimizer_host_v5 != NULL) {
        status = prdb_sdk_sql_execute_inner_join_optimizer_v1(
            optimizer_host_v5, schema, sizeof(schema) - 1,
            select_composite_join_sql, sizeof(select_composite_join_sql) - 1,
            16, sizeof(batch_response), composite_join_optimizer_request,
            sizeof(composite_join_optimizer_request),
            composite_join_optimizer_response,
            sizeof(composite_join_optimizer_response),
            composite_join_left_planner_response,
            sizeof(composite_join_left_planner_response),
            composite_join_right_planner_response,
            sizeof(composite_join_right_planner_response),
            composite_join_left_executor_response,
            sizeof(composite_join_left_executor_response),
            composite_join_right_executor_response,
            sizeof(composite_join_right_executor_response), batch_response,
            sizeof(batch_response), composite_join_left_planner_columns, 9,
            composite_join_right_planner_columns, 9,
            composite_join_left_executor_columns, 9,
            composite_join_right_executor_columns, 9, cell_scratch, 9,
            composite_join_left_rows, 128, composite_join_right_rows, 128,
            composite_join_materialized_bytes,
            sizeof(composite_join_materialized_bytes),
            write_composite_joined_row_v19, &joined_results);
    } else {
        status = prdb_sdk_sql_execute_inner_join_v19(
            host, schema, sizeof(schema) - 1, select_composite_join_sql,
            sizeof(select_composite_join_sql) - 1, 16, sizeof(batch_response),
            composite_join_left_planner_response,
            sizeof(composite_join_left_planner_response),
            composite_join_right_planner_response,
            sizeof(composite_join_right_planner_response),
            composite_join_left_executor_response,
            sizeof(composite_join_left_executor_response),
            composite_join_right_executor_response,
            sizeof(composite_join_right_executor_response), batch_response,
            sizeof(batch_response), composite_join_left_planner_columns, 9,
            composite_join_right_planner_columns, 9,
            composite_join_left_executor_columns, 9,
            composite_join_right_executor_columns, 9, cell_scratch, 9,
            composite_join_left_rows, 128, composite_join_right_rows, 128,
            composite_join_materialized_bytes,
            sizeof(composite_join_materialized_bytes),
            write_composite_joined_row_v19, &joined_results);
    }
    if (status != PRDB_SDK_SQL_EXEC_OK || joined_results.row_count != 2) {
        uint8_t message[64];
        const int message_len = snprintf(
            (char *)message, sizeof(message),
            "composite-inner-join-status=%d rows=%u v5=%u plan=%u", status,
            joined_results.row_count, optimizer_host_v5 != NULL,
            composite_join_optimizer_response[2]);
        if (message_len <= 0 || (size_t)message_len >= sizeof(message)) return 1;
        return report_failure(host, message, (uint64_t)message_len);
    }
    if (optimizer_host_v5 != NULL) {
        const uint32_t plan_len =
            ((uint32_t)composite_join_optimizer_response[3] << 24) |
            ((uint32_t)composite_join_optimizer_response[4] << 16) |
            ((uint32_t)composite_join_optimizer_response[5] << 8) |
            (uint32_t)composite_join_optimizer_response[6];
        const uint64_t response_len =
            (uint64_t)PRDB_OPTIMIZER_RESPONSE_SUCCESS_HEADER_BYTES + plan_len;
        prdb_sdk_optimizer_response_v1 selected_plan = {0};
        if (response_len > sizeof(composite_join_optimizer_response) ||
            prdb_sdk_optimizer_decode_response_v1(
                composite_join_optimizer_response, response_len, &selected_plan,
                sizeof(selected_plan)) != PRDB_SDK_OPTIMIZER_RESULT_OK ||
            selected_plan.status != PRDB_SDK_OPTIMIZER_STATUS_SUCCESS ||
            selected_plan.plan == NULL || selected_plan.plan_len < 3 ||
            selected_plan.plan[2] != PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_PREFIX_NESTED_LOOP_JOIN) {
            static const uint8_t message[] = "fixture-error-composite-prefix-plan";
            return report_failure(host, message, sizeof(message) - 1);
        }
    }

    joined_results.row_count = 0;
    joined_results.output_len = 0;
    joined_results.left_columns = composite_join_right_executor_columns;
    joined_results.right_columns = composite_join_right_executor_columns;
    status = prdb_sdk_sql_execute_inner_join_v19(
        host, schema, sizeof(schema) - 1, select_self_join_sql,
        sizeof(select_self_join_sql) - 1, 16, sizeof(batch_response),
        composite_join_left_planner_response,
        sizeof(composite_join_left_planner_response),
        composite_join_right_planner_response,
        sizeof(composite_join_right_planner_response),
        composite_join_left_executor_response,
        sizeof(composite_join_left_executor_response),
        composite_join_right_executor_response,
        sizeof(composite_join_right_executor_response), batch_response,
        sizeof(batch_response), composite_join_right_planner_columns, 9,
        composite_join_right_planner_columns, 9,
        composite_join_right_executor_columns, 9,
        composite_join_right_executor_columns, 9, cell_scratch, 9,
        composite_join_left_rows, 128, composite_join_right_rows, 128,
        composite_join_materialized_bytes,
        sizeof(composite_join_materialized_bytes),
        write_self_joined_row_v19, &joined_results);
    if (status != PRDB_SDK_SQL_EXEC_OK || joined_results.row_count != 2) {
        static const uint8_t message[] = "fixture-error-self-inner-join";
        return report_failure(host, message, sizeof(message) - 1);
    }

    joined_results.row_count = 0;
    joined_results.output_len = 0;
    joined_results.left_columns = composite_join_left_executor_columns;
    joined_results.right_columns = composite_join_right_executor_columns;
    status = prdb_sdk_sql_run_statement_v1(
        host, schema, sizeof(schema) - 1, select_filtered_composite_join_sql,
        sizeof(select_filtered_composite_join_sql) - 1,
        filtered_join_parameter_block, sizeof(filtered_join_parameter_block));
    if (status != PRDB_SDK_SQL_DISPATCH_OK) {
        static const uint8_t message[] = "fixture-error-filtered-inner-join";
        return report_failure(host, message, sizeof(message) - 1);
    }

    if (optimizer_host_v5 != NULL) {
        joined_results.row_count = 0;
        joined_results.output_len = 0;
        status = prdb_sdk_sql_execute_inner_join_optimizer_v20(
            optimizer_host_v5, schema, sizeof(schema) - 1,
            select_filtered_composite_join_sql,
            sizeof(select_filtered_composite_join_sql) - 1,
            &filtered_join_parameter, 1, 16, sizeof(batch_response),
            composite_join_optimizer_request,
            sizeof(composite_join_optimizer_request),
            composite_join_optimizer_response,
            sizeof(composite_join_optimizer_response),
            composite_join_left_planner_response,
            sizeof(composite_join_left_planner_response),
            composite_join_right_planner_response,
            sizeof(composite_join_right_planner_response),
            composite_join_left_executor_response,
            sizeof(composite_join_left_executor_response),
            composite_join_right_executor_response,
            sizeof(composite_join_right_executor_response), batch_response,
            sizeof(batch_response), composite_join_left_planner_columns, 9,
            composite_join_right_planner_columns, 9,
            composite_join_left_executor_columns, 9,
            composite_join_right_executor_columns, 9, cell_scratch, 9,
            composite_join_left_rows, 128, composite_join_right_rows, 128,
            composite_join_materialized_bytes,
            sizeof(composite_join_materialized_bytes),
            write_filtered_joined_row_v20, &joined_results);
        if (status != PRDB_SDK_SQL_EXEC_OK || joined_results.row_count != 1) {
            static const uint8_t message[] =
                "fixture-error-optimizer-filtered-inner-join";
            return report_failure(host, message, sizeof(message) - 1);
        }
    }

    if (optimizer_host != NULL) {
        prdb_sdk_select_v2 optimizer_statement = {0};
        uint8_t optimizer_request[4096] = {0};
        uint8_t optimizer_response[4096] = {0};
        uint8_t relation_response[4096] = {0};
        uint8_t scan_response[4096] = {0};
        uint8_t optimizer_batch[4096] = {0};
        uint8_t optimizer_parameter_bytes[4] = {0, 0, 0, 42};
        prdb_sdk_value_v1 optimizer_parameter = {
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
            optimizer_parameter_bytes, sizeof(optimizer_parameter_bytes)};
        uint8_t optimizer_large_range_lower_bytes[4] = {0, 0, 0, 10};
        uint8_t optimizer_large_range_upper_bytes[4] = {0, 0, 0, 11};
        uint8_t optimizer_small_estimate_parameter_bytes[4] = {0, 0, 0, 42};
        prdb_sdk_value_v1 optimizer_small_estimate_parameter = {
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
            optimizer_small_estimate_parameter_bytes,
            sizeof(optimizer_small_estimate_parameter_bytes)};
        prdb_sdk_value_v1 optimizer_large_range_parameters[2] = {
            {sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
             PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
             optimizer_large_range_lower_bytes,
             sizeof(optimizer_large_range_lower_bytes)},
            {sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
             PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
             optimizer_large_range_upper_bytes,
             sizeof(optimizer_large_range_upper_bytes)}};
        prdb_sdk_executor_relation_v1 optimizer_relation = {0};
        prdb_sdk_sql_column_shape_v2 optimizer_columns[9] = {{0}};
        prdb_sdk_value_v1 optimizer_source_cells[9] = {{0}};
        prdb_sdk_value_v1 optimizer_projected_values[9] = {{0}};
        optimizer_result_context optimizer_results = {0};
        prdb_host_api_v4 range_observer_host = {0};
        uint32_t optimizer_rows = 0;
        uint32_t parse_error_offset = 0;
        int32_t optimizer_range_status = PRDB_SDK_OPTIMIZER_PLAN_OK;
        if (optimizer_host->optimizer_request == NULL ||
            prdb_sdk_sql_parse_select_v2(
                optimizer_select_sql, sizeof(optimizer_select_sql) - 1,
                &optimizer_statement, sizeof(optimizer_statement),
                &parse_error_offset) != PRDB_SDK_SQL_OK ||
            prdb_sdk_optimizer_execute_select_ast_with_parameters_v1(
                optimizer_host, &optimizer_statement, &optimizer_parameter, 1,
                schema, sizeof(schema) - 1, 16, sizeof(optimizer_batch),
                optimizer_request, sizeof(optimizer_request),
                optimizer_response, sizeof(optimizer_response),
                relation_response, sizeof(relation_response), scan_response,
                sizeof(scan_response), optimizer_batch,
                sizeof(optimizer_batch), &optimizer_relation,
                sizeof(optimizer_relation), optimizer_columns, 9,
                optimizer_source_cells, 9, optimizer_projected_values, 9,
                verify_optimizer_row, &optimizer_results, &optimizer_rows) !=
                PRDB_SDK_OPTIMIZER_PLAN_OK ||
            optimizer_rows != 1 || optimizer_results.row_count != 1 ||
            !optimizer_results.matched_expected_value) {
            static const uint8_t message[] = "fixture-error-optimizer-filter";
            return report_failure(host, message, sizeof(message) - 1);
        }

        {
            range_result_context small_estimate_results = {0};
            range_result_context reference_results = {0};
            memset(&optimizer_statement, 0, sizeof(optimizer_statement));
            optimizer_rows = 0;
            range_observer_host = *optimizer_host;
            range_observer_original_host = optimizer_host;
            range_observer_scan_requests = 0;
            range_observer_open_scan_requests = 0;
            range_observer_host.v3.executor_request =
                observe_range_executor_request;
            if (prdb_sdk_sql_parse_select_v2(
                    optimizer_small_estimate_select_sql,
                    sizeof(optimizer_small_estimate_select_sql) - 1,
                    &optimizer_statement, sizeof(optimizer_statement),
                    &parse_error_offset) != PRDB_SDK_SQL_OK ||
                prdb_sdk_optimizer_execute_select_ast_with_parameters_v1(
                    &range_observer_host, &optimizer_statement,
                    &optimizer_small_estimate_parameter, 1,
                    schema, sizeof(schema) - 1, 16,
                    sizeof(optimizer_batch), optimizer_request,
                    sizeof(optimizer_request), optimizer_response,
                    sizeof(optimizer_response), relation_response,
                    sizeof(relation_response), scan_response,
                    sizeof(scan_response), optimizer_batch,
                    sizeof(optimizer_batch), &optimizer_relation,
                    sizeof(optimizer_relation), optimizer_columns, 9,
                    optimizer_source_cells, 9, optimizer_projected_values, 9,
                    collect_optimizer_range_row, &small_estimate_results,
                    &optimizer_rows) != PRDB_SDK_OPTIMIZER_PLAN_OK) {
                range_observer_original_host = NULL;
                static const uint8_t message[] =
                    "fixture-error-small-estimate-optimizer-execute";
                return report_failure(host, message, sizeof(message) - 1);
            }
            range_observer_original_host = NULL;
            if (range_observer_open_scan_requests != 1 ||
                range_observer_scan_requests != 0 || optimizer_rows != 1 ||
                small_estimate_results.count != 1 ||
                small_estimate_results.values[0] != 42) {
                static const uint8_t message[] =
                    "fixture-error-small-estimate-scan-choice";
                return report_failure(host, message, sizeof(message) - 1);
            }
            status = prdb_sdk_sql_execute_select_v2(
                host, schema, sizeof(schema) - 1,
                optimizer_small_estimate_select_sql,
                sizeof(optimizer_small_estimate_select_sql) - 1,
                &optimizer_small_estimate_parameter, 1, 16,
                sizeof(optimizer_batch), planner_response,
                sizeof(planner_response), executor_response,
                sizeof(executor_response), optimizer_batch,
                sizeof(optimizer_batch), planner_columns, 9,
                executor_columns, 9, optimizer_source_cells, 9,
                collect_reference_range_row, &reference_results);
            if (status != PRDB_SDK_SQL_EXEC_OK || reference_results.count != 1 ||
                memcmp(reference_results.values, small_estimate_results.values,
                       sizeof(small_estimate_results.values[0])) != 0) {
                static const uint8_t message[] =
                    "fixture-error-small-estimate-reference-result";
                return report_failure(host, message, sizeof(message) - 1);
            }
        }

        {
            range_result_context benchmark_optimizer_results = {0};
            range_result_context benchmark_reference_results = {0};
            uint64_t benchmark_optimizer_started_ns = 0;
            uint64_t benchmark_optimizer_finished_ns = 0;
            uint64_t benchmark_reference_started_ns = 0;
            uint64_t benchmark_reference_finished_ns = 0;

            memset(&optimizer_statement, 0, sizeof(optimizer_statement));
            optimizer_rows = 0;
            if (prdb_sdk_sql_parse_select_v2(
                    optimizer_large_range_select_sql,
                    sizeof(optimizer_large_range_select_sql) - 1,
                    &optimizer_statement, sizeof(optimizer_statement),
                    &parse_error_offset) != PRDB_SDK_SQL_OK ||
                !monotonic_time_ns(&benchmark_optimizer_started_ns)) {
                static const uint8_t message[] =
                    "fixture-error-range-benchmark-optimizer-parse";
                return report_failure(host, message, sizeof(message) - 1);
            }
            range_observer_host = *optimizer_host;
            range_observer_original_host = optimizer_host;
            range_observer_scan_requests = 0;
            range_observer_host.v3.executor_request =
                observe_range_executor_request;
            optimizer_range_status =
                prdb_sdk_optimizer_execute_select_ast_with_parameters_v1(
                    &range_observer_host, &optimizer_statement,
                    optimizer_large_range_parameters, 2,
                    schema, sizeof(schema) - 1, 32, sizeof(optimizer_batch),
                    optimizer_request, sizeof(optimizer_request),
                    optimizer_response, sizeof(optimizer_response),
                    relation_response, sizeof(relation_response),
                    scan_response, sizeof(scan_response), optimizer_batch,
                    sizeof(optimizer_batch), &optimizer_relation,
                    sizeof(optimizer_relation), optimizer_columns, 9,
                    optimizer_source_cells, 9, optimizer_projected_values, 9,
                    collect_optimizer_range_row, &benchmark_optimizer_results,
                    &optimizer_rows);
            range_observer_original_host = NULL;
            if (optimizer_range_status != PRDB_SDK_OPTIMIZER_PLAN_OK ||
                range_observer_scan_requests != 1 || optimizer_rows != 8 ||
                benchmark_optimizer_results.count != 8 ||
                !monotonic_time_ns(&benchmark_optimizer_finished_ns) ||
                benchmark_optimizer_finished_ns < benchmark_optimizer_started_ns) {
                static const uint8_t message[] =
                    "fixture-error-range-benchmark-optimizer-result";
                return report_failure(host, message, sizeof(message) - 1);
            }
            for (uint32_t index = 0; index < 8; ++index) {
                if (benchmark_optimizer_results.values[index] != 10) {
                    static const uint8_t message[] =
                        "fixture-error-range-benchmark-optimizer-values";
                    return report_failure(host, message, sizeof(message) - 1);
                }
            }

            if (!monotonic_time_ns(&benchmark_reference_started_ns)) {
                static const uint8_t message[] =
                    "fixture-error-range-benchmark-reference-clock";
                return report_failure(host, message, sizeof(message) - 1);
            }
            status = prdb_sdk_sql_execute_select_v2(
                host, schema, sizeof(schema) - 1,
                optimizer_large_range_select_sql,
                sizeof(optimizer_large_range_select_sql) - 1,
                optimizer_large_range_parameters, 2, 32,
                sizeof(optimizer_batch), planner_response,
                sizeof(planner_response), executor_response,
                sizeof(executor_response), optimizer_batch,
                sizeof(optimizer_batch), planner_columns, 9, executor_columns,
                9, optimizer_source_cells, 9, collect_reference_range_row,
                &benchmark_reference_results);
            if (status != PRDB_SDK_SQL_EXEC_OK ||
                benchmark_reference_results.count != 8 ||
                !monotonic_time_ns(&benchmark_reference_finished_ns) ||
                benchmark_reference_finished_ns < benchmark_reference_started_ns ||
                memcmp(benchmark_reference_results.values,
                       benchmark_optimizer_results.values,
                       8 * sizeof(benchmark_optimizer_results.values[0])) != 0) {
                static const uint8_t message[] =
                    "fixture-error-range-benchmark-reference-result";
                return report_failure(host, message, sizeof(message) - 1);
            }
            for (uint32_t index = 0; index < 8; ++index) {
                if (benchmark_reference_results.values[index] != 10) {
                    static const uint8_t message[] =
                        "fixture-error-range-benchmark-reference-values";
                    return report_failure(host, message, sizeof(message) - 1);
                }
            }
            if (report_range_timing(
                    host, 'L',
                    benchmark_optimizer_finished_ns - benchmark_optimizer_started_ns) !=
                    PRDB_QUERY_CALLBACK_OK ||
                report_range_timing(
                    host, 'F',
                    benchmark_reference_finished_ns - benchmark_reference_started_ns) !=
                    PRDB_QUERY_CALLBACK_OK) {
                static const uint8_t message[] =
                    "fixture-error-range-benchmark-timing-report";
                return report_failure(host, message, sizeof(message) - 1);
            }
        }

        {
            static uint8_t ordered_request[PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES];
            static uint8_t ordered_response[PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES + 64];
            static uint8_t ordered_relation_response[4096];
            static uint8_t ordered_scan_response[4096];
            static uint8_t ordered_batch[4096];
            static uint8_t ordered_materialized[
                PRDB_SDK_SQL_EXEC_V3_MAX_MATERIALIZED_BYTES];
            static uint8_t ordered_record[256];
            prdb_sdk_select_v3 ordered_statement = {0};
            prdb_sdk_executor_relation_v1 ordered_relation = {0};
            prdb_sdk_sql_column_shape_v2 ordered_columns[9] = {{0}};
            prdb_sdk_value_v1 ordered_source_cells[9] = {{0}};
            prdb_sdk_value_v1 ordered_projected_values[1] = {{0}};
            ordered_optimizer_result_context ordered_results = {
                host, ordered_columns, &ordered_relation, ordered_record,
                sizeof(ordered_record), 0, {9, 9}};
            uint32_t ordered_rows = 0;
            uint32_t ordered_parse_error = 0;
            if (prdb_sdk_sql_parse_select_v3(
                    optimizer_ordered_select_sql,
                    sizeof(optimizer_ordered_select_sql) - 1,
                    &ordered_statement, sizeof(ordered_statement),
                    &ordered_parse_error) != PRDB_SDK_SQL_OK ||
                prdb_sdk_optimizer_execute_ordered_select_ast_v1(
                    optimizer_host, &ordered_statement, schema,
                    sizeof(schema) - 1, 16, sizeof(ordered_batch),
                    ordered_request, sizeof(ordered_request),
                    ordered_response, sizeof(ordered_response),
                    ordered_relation_response,
                    sizeof(ordered_relation_response), ordered_scan_response,
                    sizeof(ordered_scan_response), ordered_batch,
                    sizeof(ordered_batch), &ordered_relation,
                    sizeof(ordered_relation), ordered_columns, 9,
                    ordered_source_cells, 9, ordered_projected_values, 1,
                    ordered_materialized, sizeof(ordered_materialized),
                    emit_ordered_optimizer_row, &ordered_results,
                    &ordered_rows) != PRDB_SDK_OPTIMIZER_PLAN_OK ||
                ordered_rows != 2 || ordered_results.row_count != 2) {
                static const uint8_t message[] =
                    "fixture-error-optimizer-ordered-select";
                return report_failure(host, message, sizeof(message) - 1);
            }
        }
    }

    status = prdb_sdk_sql_execute_delete_v1(
        host, schema, sizeof(schema) - 1, delete_sql, sizeof(delete_sql) - 1,
        &parameter, 1, 16, sizeof(batch_response), planner_response,
        sizeof(planner_response), executor_response, sizeof(executor_response),
        batch_response, sizeof(batch_response), planner_columns, 1,
        executor_columns, 1, &binding, sizeof(binding), cell_scratch, 1,
        update_targets, PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS, &deleted_count);
    if (status != PRDB_SDK_SQL_EXEC_OK || deleted_count != 1) {
        static const uint8_t message[] = "fixture-error-delete";
        return report_failure(host, message, sizeof(message) - 1);
    }

    results.marker = NULL;
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, schema, sizeof(schema) - 1, select_value_sql,
        sizeof(select_value_sql) - 1, &parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 1, executor_columns, 1,
        cell_scratch, 1, verify_selected_value, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 0) {
        static const uint8_t message[] = "fixture-error-final-select";
        return report_failure(host, message, sizeof(message) - 1);
    }

    multi_insert_values[0].struct_size = sizeof(multi_insert_values[0]);
    multi_insert_values[0].abi_version = PRDB_SDK_EXECUTOR_ABI_V1_VERSION;
    multi_insert_values[0].state = PRDB_SDK_EXECUTOR_INSERT_VALUE;
    multi_insert_values[0].value = parameter;
    multi_insert_values[1] = multi_insert_values[0];
    multi_insert_parameters[0] = parameter;
    multi_insert_parameters[1] = parameter;
    status = prdb_sdk_sql_execute_insert_v2(
        host, schema, sizeof(schema) - 1, multi_insert_sql,
        sizeof(multi_insert_sql) - 1, multi_insert_parameters, 2, planner_response,
        sizeof(planner_response), executor_response,
        sizeof(executor_response), multi_insert_request,
        sizeof(multi_insert_request), planner_columns, 1, executor_columns,
        1, multi_insert_values, 2, &multi_inserted_count);
    if (status != PRDB_SDK_SQL_EXEC_OK || multi_inserted_count != 2) {
        static const uint8_t message[] = "fixture-error-multi-insert";
        return report_failure(host, message, sizeof(message) - 1);
    }
    results.row_count = 0;
    status = prdb_sdk_sql_execute_select_v2(
        host, schema, sizeof(schema) - 1, select_value_sql,
        sizeof(select_value_sql) - 1, &parameter, 1, 16,
        sizeof(batch_response), planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 1, executor_columns, 1,
        cell_scratch, 1, verify_selected_value, &results);
    if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 2) {
        static const uint8_t message[] = "fixture-error-multi-insert-visible";
        return report_failure(host, message, sizeof(message) - 1);
    }
    if (optimizer_host_v5 != NULL) {
        static const uint8_t optimizer_grouped_update_sql[] =
            "UPDATE grouped_records SET left_key = $2, right_key = $3 "
            "WHERE left_key = $1";
        static const uint8_t grouped_match_bytes[4] = {0, 0, 0, 1};
        static const uint8_t grouped_left_bytes[4] = {0, 0, 0, 4};
        static const uint8_t grouped_right_bytes[4] = {0, 0, 0, 9};
        prdb_sdk_value_v1 grouped_parameters[3] = {
            {sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
             PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
             grouped_match_bytes, sizeof(grouped_match_bytes)},
            {sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
             PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
             grouped_left_bytes, sizeof(grouped_left_bytes)},
            {sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
             PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
             grouped_right_bytes, sizeof(grouped_right_bytes)}};
        prdb_sdk_update_v1 grouped_update = {0};
        prdb_sdk_executor_relation_v1 grouped_relation = {0};
        prdb_sdk_sql_column_shape_v2 grouped_columns[2] = {{0}};
        prdb_sdk_select_binding_v2 grouped_binding = {0};
        prdb_sdk_value_v1 grouped_scratch[2] = {{0}};
        uint8_t grouped_request[4096] = {0};
        uint8_t grouped_optimizer_response[4096] = {0};
        uint8_t grouped_relation_response[4096] = {0};
        uint8_t grouped_scan_response[4096] = {0};
        uint8_t grouped_batch_response[4096] = {0};
        uint8_t grouped_targets[
            PRDB_SDK_OPTIMIZER_PLAN_MAX_DELETE_TARGETS * 16] = {0};
        uint32_t grouped_update_count = 0;
        uint32_t parse_error_offset = 0;
        if (prdb_sdk_sql_parse_update_v1(
                optimizer_grouped_update_sql,
                sizeof(optimizer_grouped_update_sql) - 1, &grouped_update,
                sizeof(grouped_update), &parse_error_offset) !=
                PRDB_SDK_SQL_OK ||
            prdb_sdk_optimizer_execute_update_ast_v1(
                optimizer_host_v5, &grouped_update, grouped_parameters, 3,
                schema, sizeof(schema) - 1, 16,
                sizeof(grouped_batch_response), grouped_request,
                sizeof(grouped_request), grouped_optimizer_response,
                sizeof(grouped_optimizer_response), grouped_relation_response,
                sizeof(grouped_relation_response), grouped_scan_response,
                sizeof(grouped_scan_response), grouped_batch_response,
                sizeof(grouped_batch_response), &grouped_relation,
                sizeof(grouped_relation), grouped_columns, 2, &grouped_binding,
                sizeof(grouped_binding), grouped_scratch, 2, grouped_targets,
                PRDB_SDK_OPTIMIZER_PLAN_MAX_DELETE_TARGETS,
                &grouped_update_count) != PRDB_SDK_OPTIMIZER_PLAN_OK ||
            grouped_update_count != 2) {
            static const uint8_t message[] =
                "fixture-error-optimizer-update-multiple";
            return report_failure(host, message, sizeof(message) - 1);
        }
        static const uint8_t optimizer_grouped_verify_sql[] =
            "UPDATE grouped_records SET left_key = $2 "
            "WHERE left_key = $1 AND right_key = $3";
        prdb_sdk_value_v1 grouped_verify_parameters[3] = {
            {sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
             PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
             grouped_left_bytes, sizeof(grouped_left_bytes)},
            {sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
             PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
             grouped_left_bytes, sizeof(grouped_left_bytes)},
            {sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
             PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
             grouped_right_bytes, sizeof(grouped_right_bytes)}};
        uint32_t grouped_verify_count = 0;
        if (prdb_sdk_sql_parse_update_v1(
                optimizer_grouped_verify_sql,
                sizeof(optimizer_grouped_verify_sql) - 1, &grouped_update,
                sizeof(grouped_update), &parse_error_offset) !=
                PRDB_SDK_SQL_OK ||
            prdb_sdk_optimizer_execute_update_ast_v1(
                optimizer_host_v5, &grouped_update, grouped_verify_parameters,
                3, schema, sizeof(schema) - 1, 16,
                sizeof(grouped_batch_response), grouped_request,
                sizeof(grouped_request), grouped_optimizer_response,
                sizeof(grouped_optimizer_response), grouped_relation_response,
                sizeof(grouped_relation_response), grouped_scan_response,
                sizeof(grouped_scan_response), grouped_batch_response,
                sizeof(grouped_batch_response), &grouped_relation,
                sizeof(grouped_relation), grouped_columns, 2,
                &grouped_binding, sizeof(grouped_binding), grouped_scratch, 2,
                grouped_targets, PRDB_SDK_OPTIMIZER_PLAN_MAX_DELETE_TARGETS,
                &grouped_verify_count) != PRDB_SDK_OPTIMIZER_PLAN_OK ||
            grouped_verify_count != 2) {
            static const uint8_t message[] =
                "fixture-error-optimizer-update-multiple-visible";
            return report_failure(host, message, sizeof(message) - 1);
        }

        static const uint8_t optimizer_update_sql[] =
            "UPDATE records SET id = $2 WHERE id = $1";
        static const uint8_t updated_integer_bytes[] = {0, 0, 0, 43};
        prdb_sdk_update_v1 optimizer_update = {0};
        uint8_t optimizer_update_request[4096] = {0};
        uint8_t optimizer_update_response[4096] = {0};
        uint8_t optimizer_relation_response[4096] = {0};
        uint8_t optimizer_scan_response[4096] = {0};
        uint8_t optimizer_batch_response[4096] = {0};
        uint8_t old_parameter_bytes[4] = {0, 0, 0, 42};
        prdb_sdk_value_v1 optimizer_update_parameters[2] = {
            {sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
             PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
             old_parameter_bytes, sizeof(old_parameter_bytes)},
            {sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
             PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
             updated_integer_bytes, sizeof(updated_integer_bytes)}};
        prdb_sdk_executor_relation_v1 optimizer_update_relation = {0};
        prdb_sdk_sql_column_shape_v2 optimizer_update_columns[1] = {{0}};
        prdb_sdk_select_binding_v2 optimizer_update_binding = {0};
        prdb_sdk_value_v1 optimizer_update_cell_scratch[1] = {{0}};
        uint8_t optimizer_update_targets[
            PRDB_SDK_OPTIMIZER_PLAN_MAX_DELETE_TARGETS * 16] = {0};
        uint32_t optimized_update_count = 0;
        int32_t optimized_update_status;

        if (optimizer_host_v5->optimizer_request == NULL ||
            prdb_sdk_sql_parse_update_v1(
                optimizer_update_sql, sizeof(optimizer_update_sql) - 1,
                &optimizer_update, sizeof(optimizer_update), NULL) !=
                PRDB_SDK_SQL_OK) {
            static const uint8_t message[] = "fixture-error-optimizer-update";
            return report_failure(host, message, sizeof(message) - 1);
        }
        optimized_update_status = prdb_sdk_optimizer_execute_update_ast_v1(
            optimizer_host_v5, &optimizer_update, optimizer_update_parameters,
            2, schema, sizeof(schema) - 1, 16,
            sizeof(optimizer_batch_response), optimizer_update_request,
            sizeof(optimizer_update_request), optimizer_update_response,
            sizeof(optimizer_update_response), optimizer_relation_response,
            sizeof(optimizer_relation_response), optimizer_scan_response,
            sizeof(optimizer_scan_response), optimizer_batch_response,
            sizeof(optimizer_batch_response), &optimizer_update_relation,
            sizeof(optimizer_update_relation), optimizer_update_columns, 1,
            &optimizer_update_binding, sizeof(optimizer_update_binding),
            optimizer_update_cell_scratch, 1, optimizer_update_targets,
            PRDB_SDK_OPTIMIZER_PLAN_MAX_DELETE_TARGETS,
            &optimized_update_count);
        if (optimized_update_status != PRDB_SDK_OPTIMIZER_PLAN_OK) {
            static const uint8_t message[] = "fixture-error-optimizer-update";
            return report_failure(host, message, sizeof(message) - 1);
        }
        if (optimized_update_count != 2) {
            static const uint8_t message[] =
                "fixture-error-optimizer-update-count";
            return report_failure(host, message, sizeof(message) - 1);
        }
        parameter.data = updated_integer_bytes;
        parameter.data_len = sizeof(updated_integer_bytes);
        results.expected_state = PRDB_SDK_VALUE_STATE_VALUE;
        results.expected_value = updated_integer_bytes;
        results.expected_value_len = sizeof(updated_integer_bytes);
        results.marker = NULL;
        results.row_count = 0;
        status = prdb_sdk_sql_execute_select_v2(
            host, schema, sizeof(schema) - 1, select_value_sql,
            sizeof(select_value_sql) - 1, &parameter, 1, 16,
            sizeof(batch_response), planner_response, sizeof(planner_response),
            executor_response, sizeof(executor_response), batch_response,
            sizeof(batch_response), planner_columns, 1, executor_columns, 1,
            cell_scratch, 1, verify_selected_value, &results);
        if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 2) {
            static const uint8_t message[] =
                "fixture-error-optimizer-update-visible";
            return report_failure(host, message, sizeof(message) - 1);
        }

        static const uint8_t optimizer_null_update_sql[] =
            "UPDATE records SET id = NULL WHERE id = $1";
        prdb_sdk_value_v1 optimizer_null_parameter = {
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
            updated_integer_bytes, sizeof(updated_integer_bytes)};
        uint32_t optimized_null_update_count = 0;
        if (prdb_sdk_sql_parse_update_v1(
                optimizer_null_update_sql,
                sizeof(optimizer_null_update_sql) - 1, &optimizer_update,
                sizeof(optimizer_update), NULL) != PRDB_SDK_SQL_OK ||
            prdb_sdk_optimizer_execute_update_ast_v1(
                optimizer_host_v5, &optimizer_update, &optimizer_null_parameter,
                1, schema, sizeof(schema) - 1, 16,
                sizeof(optimizer_batch_response), optimizer_update_request,
                sizeof(optimizer_update_request), optimizer_update_response,
                sizeof(optimizer_update_response), optimizer_relation_response,
                sizeof(optimizer_relation_response), optimizer_scan_response,
                sizeof(optimizer_scan_response), optimizer_batch_response,
                sizeof(optimizer_batch_response), &optimizer_update_relation,
                sizeof(optimizer_update_relation), optimizer_update_columns, 1,
                &optimizer_update_binding, sizeof(optimizer_update_binding),
                optimizer_update_cell_scratch, 1, optimizer_update_targets,
                PRDB_SDK_OPTIMIZER_PLAN_MAX_DELETE_TARGETS,
                &optimized_null_update_count) != PRDB_SDK_OPTIMIZER_PLAN_OK ||
            optimized_null_update_count != 2) {
            static const uint8_t message[] =
                "fixture-error-optimizer-update-null";
            return report_failure(host, message, sizeof(message) - 1);
        }
        results.expected_state = PRDB_SDK_VALUE_STATE_NULL;
        results.expected_value = NULL;
        results.expected_value_len = 0;
        results.marker = NULL;
        results.row_count = 0;
        status = prdb_sdk_sql_execute_select_v2(
            host, schema, sizeof(schema) - 1, select_null_sql,
            sizeof(select_null_sql) - 1, NULL, 0, 16,
            sizeof(batch_response), planner_response, sizeof(planner_response),
            executor_response, sizeof(executor_response), batch_response,
            sizeof(batch_response), planner_columns, 1, executor_columns, 1,
            cell_scratch, 1, verify_selected_value, &results);
        if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 2) {
            static const uint8_t message[] =
                "fixture-error-optimizer-update-null-visible";
            return report_failure(host, message, sizeof(message) - 1);
        }
        status = prdb_sdk_sql_execute_update_v1(
            host, schema, sizeof(schema) - 1, update_sql,
            sizeof(update_sql) - 1, &optimizer_null_parameter, 1, 16,
            sizeof(batch_response), planner_response, sizeof(planner_response),
            executor_response, sizeof(executor_response), batch_response,
            sizeof(batch_response), update_request, sizeof(update_request),
            planner_columns, 1, executor_columns, 1, &binding,
            sizeof(binding), cell_scratch, 1, update_targets,
            PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS, update_patches, 1,
            &updated_count);
        if (status != PRDB_SDK_SQL_EXEC_OK || updated_count != 2) {
            static const uint8_t message[] =
                "fixture-error-optimizer-update-null-restore";
            return report_failure(host, message, sizeof(message) - 1);
        }

        static const uint8_t optimizer_delete_sql[] =
            "DELETE FROM records WHERE id = $1";
        prdb_sdk_delete_v1 optimizer_delete = {0};
        uint8_t optimizer_delete_targets[
            PRDB_SDK_OPTIMIZER_PLAN_MAX_DELETE_TARGETS * 16] = {0};
        uint32_t optimized_delete_count = 0;
        if (prdb_sdk_sql_parse_delete_v1(
                optimizer_delete_sql, sizeof(optimizer_delete_sql) - 1,
                &optimizer_delete, sizeof(optimizer_delete), NULL) !=
                PRDB_SDK_SQL_OK ||
            prdb_sdk_optimizer_execute_delete_ast_v1(
                optimizer_host_v5, &optimizer_delete,
                &optimizer_update_parameters[1], 1, schema,
                sizeof(schema) - 1, 16, sizeof(optimizer_batch_response),
                optimizer_update_request, sizeof(optimizer_update_request),
                optimizer_update_response, sizeof(optimizer_update_response),
                optimizer_relation_response, sizeof(optimizer_relation_response),
                optimizer_scan_response, sizeof(optimizer_scan_response),
                optimizer_batch_response, sizeof(optimizer_batch_response),
                &optimizer_update_relation, sizeof(optimizer_update_relation),
                optimizer_update_columns, 1, &optimizer_update_binding,
                sizeof(optimizer_update_binding), optimizer_update_cell_scratch,
                1, optimizer_delete_targets,
                PRDB_SDK_OPTIMIZER_PLAN_MAX_DELETE_TARGETS,
                &optimized_delete_count) != PRDB_SDK_OPTIMIZER_PLAN_OK ||
            optimized_delete_count != 2) {
            static const uint8_t message[] = "fixture-error-optimizer-delete";
            return report_failure(host, message, sizeof(message) - 1);
        }
        results.row_count = 0;
        status = prdb_sdk_sql_execute_select_v2(
            host, schema, sizeof(schema) - 1, select_value_sql,
            sizeof(select_value_sql) - 1, &parameter, 1, 16,
            sizeof(batch_response), planner_response, sizeof(planner_response),
            executor_response, sizeof(executor_response), batch_response,
            sizeof(batch_response), planner_columns, 1, executor_columns, 1,
            cell_scratch, 1, verify_selected_value, &results);
        if (status != PRDB_SDK_SQL_EXEC_OK || results.row_count != 0) {
            static const uint8_t message[] =
                "fixture-error-optimizer-delete-visible";
            return report_failure(host, message, sizeof(message) - 1);
        }
    }
    return 0;
}

int32_t prdb_query_main_v3(const prdb_host_api_v3 *host,
                           const prdb_invocation_v3 *invocation) {
    return run_write_read_fixture(host, invocation, NULL, NULL);
}

int32_t prdb_query_main_v4(const prdb_host_api_v4 *host,
                           const prdb_invocation_v4 *invocation) {
    if (host == NULL || host->v3.struct_size < sizeof(host->v3) ||
        host->v3.abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->optimizer_request == NULL) {
        return 1;
    }
    return run_write_read_fixture(&host->v3, invocation, host, NULL);
}

int32_t prdb_query_main_v5(const prdb_host_api_v5 *host,
                           const prdb_invocation_v5 *invocation) {
    if (host == NULL || host->v4.v3.struct_size < sizeof(host->v4.v3) ||
        host->v4.v3.abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->v4.optimizer_request == NULL || host->optimizer_request == NULL) {
        return 1;
    }
    return run_write_read_fixture(&host->v4.v3, invocation, &host->v4, host);
}
