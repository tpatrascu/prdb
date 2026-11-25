#include "prdb_sdk_sql_exec_v10.h"
#include "prdb_sdk_sql_exec_v5.h"
#include "prdb_sdk_sql_exec_v6.h"
#include "prdb_sdk_sql_exec_v7.h"
#include "prdb_sdk_sql_exec_v11.h"
#include "prdb_sdk_sql_exec_v12.h"
#include "prdb_sdk_sql_exec_v13.h"
#include "prdb_sdk_sql_exec_v14.h"
#include "prdb_sdk_sql_exec_v15.h"
#include "prdb_sdk_sql_exec_v17.h"
#include "prdb_sdk_sql_exec_v18.h"
#include "prdb_sdk_sql_exec_v21.h"
#include "prdb_sdk_sql_exec_v22.h"
#include "prdb_sdk_sql_exec_v23.h"
#include "prdb_sdk_sql_exec_v24.h"
#include "prdb_sdk_sql_result_v1.h"
#include "prdb_query_abi_payload_codecs.h"

#include <assert.h>
#include <string.h>
#include <stdio.h>


#include "sdk_sql_execute_v10/mock_harness.h"
#include "sdk_sql_execute_v10/runners_v10_v18.h"
#include "sdk_sql_execute_v10/runners_v19_v24.h"

int main(void) {
    static const uint8_t min_sql[] =
        "SELECT id, MIN(score) FROM widgets GROUP BY id";
    static const uint8_t max_sql[] =
        "SELECT id, MAX(score) FROM widgets GROUP BY id";
    static const uint8_t sum_sql[] =
        "SELECT id, SUM(score) FROM widgets GROUP BY id";
    static const uint8_t avg_sql[] =
        "SELECT id, AVG(score) FROM widgets GROUP BY id";
    static const uint8_t limit_sql[] =
        "SELECT id, MIN(score) FROM widgets GROUP BY id LIMIT 1";
    static const uint8_t where_sql[] =
        "SELECT id, MIN(score) FROM widgets WHERE score >= $1 GROUP BY id";
    static const uint8_t having_sql[] =
        "SELECT id, COUNT(*) FROM widgets GROUP BY id "
        "HAVING COUNT(*) = $1 LIMIT 1";
    static const uint8_t ordered_having_sql[] =
        "SELECT id, COUNT(*) FROM widgets GROUP BY id "
        "HAVING COUNT(*) >= $1 ORDER BY COUNT(*) ASC LIMIT 1";
    static const uint8_t ordered_key_sql[] =
        "SELECT id, COUNT(*) FROM widgets GROUP BY id "
        "ORDER BY id DESC LIMIT 1";
    static const uint8_t ordered_aggregate_sql[] =
        "SELECT id, MIN(score) FROM widgets GROUP BY id "
        "ORDER BY MIN(score) DESC LIMIT 1";
    static const uint8_t nulls_last_sql[] =
        "SELECT id, MIN(score) FROM widgets GROUP BY id "
        "ORDER BY MIN(score) ASC NULLS LAST";
    static const uint8_t default_null_order_sql[] =
        "SELECT id, MIN(score) FROM widgets GROUP BY id "
        "ORDER BY MIN(score) DESC";
    static const uint8_t composite_order_sql[] =
        "SELECT id, COUNT(*) FROM widgets GROUP BY id "
        "ORDER BY COUNT(*) ASC, id DESC NULLS LAST LIMIT 2";
    static const uint8_t ordered_group_limit_sql[] =
        "SELECT id, COUNT(*) FROM widgets GROUP BY id "
        "ORDER BY COUNT(*) DESC, id ASC LIMIT 128";
    static const uint8_t ordered_group_text_budget_sql[] =
        "SELECT id, COUNT(*) FROM widgets GROUP BY id "
        "ORDER BY COUNT(*) DESC, id ASC LIMIT 128";
    static const uint8_t ordered_grouped_sum_tie_sql[] =
        "SELECT id, SUM(score) FROM widgets GROUP BY id "
        "ORDER BY SUM(score) DESC NULLS LAST, id ASC NULLS FIRST";
    static const uint8_t *const ordered_aggregate_matrix_sql[] = {
        (const uint8_t *)"SELECT id, COUNT(*) FROM widgets GROUP BY id "
                         "ORDER BY COUNT(*) DESC NULLS LAST, id ASC NULLS FIRST",
        (const uint8_t *)"SELECT id, MIN(score) FROM widgets GROUP BY id "
                         "ORDER BY MIN(score) DESC NULLS LAST, id ASC NULLS FIRST",
        (const uint8_t *)"SELECT id, MAX(score) FROM widgets GROUP BY id "
                         "ORDER BY MAX(score) DESC NULLS LAST, id ASC NULLS FIRST",
        (const uint8_t *)"SELECT id, SUM(score) FROM widgets GROUP BY id "
                         "ORDER BY SUM(score) DESC NULLS LAST, id ASC NULLS FIRST",
        (const uint8_t *)"SELECT id, AVG(score) FROM widgets GROUP BY id "
                         "ORDER BY AVG(score) DESC NULLS LAST, id ASC NULLS FIRST",
    };
    static const uint32_t ordered_aggregate_matrix_sql_len[] = {
        sizeof("SELECT id, COUNT(*) FROM widgets GROUP BY id "
               "ORDER BY COUNT(*) DESC NULLS LAST, id ASC NULLS FIRST") - 1,
        sizeof("SELECT id, MIN(score) FROM widgets GROUP BY id "
               "ORDER BY MIN(score) DESC NULLS LAST, id ASC NULLS FIRST") - 1,
        sizeof("SELECT id, MAX(score) FROM widgets GROUP BY id "
               "ORDER BY MAX(score) DESC NULLS LAST, id ASC NULLS FIRST") - 1,
        sizeof("SELECT id, SUM(score) FROM widgets GROUP BY id "
               "ORDER BY SUM(score) DESC NULLS LAST, id ASC NULLS FIRST") - 1,
        sizeof("SELECT id, AVG(score) FROM widgets GROUP BY id "
               "ORDER BY AVG(score) DESC NULLS LAST, id ASC NULLS FIRST") - 1,
    };
    static const uint8_t v12_no_order_sql[] =
        "SELECT id, COUNT(*) FROM widgets GROUP BY id";
    static const uint8_t v13_composite_sql[] =
        "SELECT id, score, COUNT(*) FROM widgets GROUP BY id, score";
    static const uint8_t v13_nullable_composite_sql[] =
        "SELECT id, score, COUNT(*) FROM widgets GROUP BY id, score";
    static const uint8_t v13_sum_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score";
    static const uint8_t v13_min_sql[] =
        "SELECT id, score, MIN(score) FROM widgets GROUP BY id, score";
    static const uint8_t v13_max_sql[] =
        "SELECT id, score, MAX(score) FROM widgets GROUP BY id, score";
    static const uint8_t v13_avg_sql[] =
        "SELECT id, score, AVG(score) FROM widgets GROUP BY id, score";
    static const uint8_t *const v13_optimizer_sql[] = {
        v13_composite_sql, v13_min_sql, v13_max_sql, v13_sum_sql,
        v13_avg_sql};
    static const uint32_t v13_optimizer_sql_lens[] = {
        sizeof(v13_composite_sql) - 1, sizeof(v13_min_sql) - 1,
        sizeof(v13_max_sql) - 1, sizeof(v13_sum_sql) - 1,
        sizeof(v13_avg_sql) - 1};
    static const uint8_t v14_having_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING SUM(score) >= $1";
    static const uint8_t v24_optimizer_having_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING SUM(score) IS NOT NULL AND COUNT(*) IS NOT NULL";
    static const uint8_t v24_optimizer_single_having_sql[] =
        "SELECT id, SUM(score) FROM widgets GROUP BY id "
        "HAVING SUM(score) IS NOT NULL AND COUNT(*) IS NOT NULL";
    static const uint8_t v24_optimizer_single_filtered_having_sql[] =
        "SELECT id, SUM(score) FROM widgets WHERE id IS NOT NULL "
        "GROUP BY id HAVING SUM(score) IS NOT NULL AND COUNT(*) IS NOT NULL "
        "LIMIT 5";
    static const uint8_t v24_optimizer_single_ordered_having_sql[] =
        "SELECT id, SUM(score) FROM widgets GROUP BY id "
        "HAVING SUM(score) IS NOT NULL AND COUNT(*) IS NOT NULL "
        "ORDER BY SUM(score) DESC NULLS LAST, id ASC NULLS FIRST";
    static const uint8_t v24_optimizer_filtered_ordered_composite_having_sql[] =
        "SELECT id, score, SUM(score) FROM widgets WHERE id IS NOT NULL "
        "GROUP BY id, score HAVING SUM(score) IS NOT NULL AND "
        "COUNT(*) IS NOT NULL ORDER BY id ASC NULLS FIRST, score DESC "
        "NULLS LAST, SUM(score) DESC NULLS LAST LIMIT 5";
    static const uint8_t v15_order_sql[] =
        "SELECT id, score, COUNT(*) FROM widgets GROUP BY id, score "
        "HAVING COUNT(*) >= $1 ORDER BY score DESC NULLS LAST, "
        "COUNT(*) DESC, id ASC NULLS LAST LIMIT 3";
    static const uint8_t v17_literal_having_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING SUM(score) >= 10 ORDER BY score DESC NULLS LAST, "
        "SUM(score) DESC, id ASC NULLS LAST LIMIT 3";
    static const uint8_t v17_parameter_having_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING SUM(score) >= $1 ORDER BY score DESC NULLS LAST, "
        "SUM(score) DESC, id ASC NULLS LAST LIMIT 3";
    static const uint8_t v17_overflow_having_sql[] =
        "SELECT id, score, COUNT(*) FROM widgets GROUP BY id, score "
        "HAVING COUNT(*) > 9223372036854775808";
    static const uint8_t v18_compound_having_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING (SUM(score) >= 9 OR SUM(score) = $1) AND "
        "NOT SUM(score) < 3 ORDER BY score DESC LIMIT 2";
    static const uint8_t v18_negative_literal_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING SUM(score) >= -1";
    static const uint8_t v21_decimal_literal_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING SUM(score) >= 4.50e0";
    static const uint8_t bigint_sum_overflow_sql[] =
        "SELECT id, SUM(score) FROM widgets GROUP BY id";
    static const uint8_t v21_bigint_sum_literal_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING SUM(score) >= 6";
    static const uint8_t v21_bigint_sum_overflow_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING SUM(score) >= 0";
    static const uint8_t v22_hidden_count_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING COUNT(*) >= 2 AND SUM(score) >= 4.50e0";
    static const uint8_t v22_where_and_having_sql[] =
        "SELECT id, score, SUM(score) FROM widgets WHERE id >= $1 "
        "GROUP BY id, score "
        "HAVING COUNT(*) >= 1 AND SUM(score) >= 5.00";
    static const uint8_t v22_hidden_count_avg_sql[] =
        "SELECT id, score, AVG(score) FROM widgets GROUP BY id, score "
        "HAVING COUNT(*) >= 2 AND AVG(score) >= 3.00000000e0";
    static const uint8_t v22_hidden_count_min_sql[] =
        "SELECT id, score, MIN(score) FROM widgets GROUP BY id, score "
        "HAVING COUNT(*) >= 2 AND MIN(score) >= 3.00e0";
    static const uint8_t v22_hidden_count_max_sql[] =
        "SELECT id, score, MAX(score) FROM widgets GROUP BY id, score "
        "HAVING COUNT(*) >= 2 AND MAX(score) <= 3.00e0";
    static const uint8_t v22_null_count_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING COUNT(*) >= 2 OR SUM(score) >= 4.50e0";
    static const uint8_t v22_parameter_count_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING COUNT(*) >= $1 AND SUM(score) >= 4.50e0";
    static const uint8_t v21_decimal_equal_literal_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING SUM(score) = 4.50e0";
    static const uint8_t v21_decimal_avg_literal_sql[] =
        "SELECT id, score, AVG(score) FROM widgets GROUP BY id, score "
        "HAVING AVG(score) >= 1.40000000e0";
    static const uint8_t v21_decimal_min_literal_sql[] =
        "SELECT id, score, MIN(score) FROM widgets GROUP BY id, score "
        "HAVING MIN(score) <= 3.00e0 ORDER BY id ASC LIMIT 1";
    static const uint8_t v21_decimal_max_literal_sql[] =
        "SELECT id, score, MAX(score) FROM widgets GROUP BY id, score "
        "HAVING MAX(score) >= 8.00e0 ORDER BY id ASC LIMIT 1";
    static const uint8_t v21_decimal_sum_overflow_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING SUM(score) >= 0.00e0";
    static const uint8_t v21_decimal_inexact_scale_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING SUM(score) >= 1.251";
    static const uint8_t v21_decimal_overflow_literal_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING SUM(score) >= 1e36";
    static const uint8_t v13_many_groups_sql[] =
        "SELECT id, score, COUNT(*) FROM widgets GROUP BY id, score";
    static const uint8_t parameter_bytes[] = {0, 0, 0, 5};
    static const uint8_t having_parameter_bytes[] = {
        0, 0, 0, 0, 0, 0, 0, 1};
    static const uint8_t v14_having_parameter_bytes[] = {
        0, 0, 0, 0, 0, 0, 0, 6};
    static const uint8_t v17_having_parameter_bytes[] = {
        0, 0, 0, 0, 0, 0, 0, 10};
    static const uint8_t v18_having_parameter_bytes[] = {
        0, 0, 0, 0, 0, 0, 0, 3};
    static const uint8_t v22_where_parameter_bytes[] = {0, 0, 0, 2};
    const int32_t keys[] = {1, 1, 2};
    const int32_t scores[] = {9, 3, 5};
    const uint8_t value_states[] = {
        PRDB_SDK_VALUE_STATE_VALUE,
        PRDB_SDK_VALUE_STATE_VALUE,
        PRDB_SDK_VALUE_STATE_VALUE};
    mock_state mock = {0};
    group_capture capture = {0};
    prdb_sdk_value_v1 parameter = {
        sizeof(parameter), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
        parameter_bytes, sizeof(parameter_bytes)};
    prdb_sdk_value_v1 having_parameter = {
        sizeof(having_parameter), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BIGINT,
        having_parameter_bytes, sizeof(having_parameter_bytes)};
    prdb_sdk_value_v1 v14_having_parameter = {
        sizeof(v14_having_parameter), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BIGINT,
        v14_having_parameter_bytes, sizeof(v14_having_parameter_bytes)};
    prdb_sdk_value_v1 v15_having_parameter = {
        sizeof(v15_having_parameter), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BIGINT,
        having_parameter_bytes, sizeof(having_parameter_bytes)};
    prdb_sdk_value_v1 v17_having_parameter = {
        sizeof(v17_having_parameter), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BIGINT,
        v17_having_parameter_bytes, sizeof(v17_having_parameter_bytes)};
    prdb_sdk_value_v1 v18_having_parameter = {
        sizeof(v18_having_parameter), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BIGINT,
        v18_having_parameter_bytes, sizeof(v18_having_parameter_bytes)};
    prdb_sdk_value_v1 v22_where_parameter = {
        sizeof(v22_where_parameter), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
        v22_where_parameter_bytes, sizeof(v22_where_parameter_bytes)};
    mock.planner_len = make_relation(
        mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
        PRDB_SDK_VALUE_TYPE_INTEGER);
    mock.executor_len = make_relation(mock.executor, 6, 0x22,
                                      PRDB_SDK_VALUE_TYPE_INTEGER);
    mock.batch_len = make_rows(mock.batch, 1, keys, scores, value_states, 3);
    mock.batch_finished = 1;
    capture.mock = &mock;

    assert(run_query(&mock, min_sql, sizeof(min_sql) - 1, NULL, 0, 64,
                     &capture) == PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 2 && capture.keys[0] == 1 &&
           capture.aggregates[0] == 3 && capture.row_counts[0] == 2 &&
           capture.keys[1] == 2 && capture.aggregates[1] == 5 &&
           capture.row_counts[1] == 1 && mock.statement_failed == 0);

    reset_scan(&mock, &capture, 1);
    assert(run_query(&mock, max_sql, sizeof(max_sql) - 1, NULL, 0, 64,
                     &capture) == PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 2 && capture.aggregates[0] == 9 &&
           capture.aggregates[1] == 5);

    reset_scan(&mock, &capture, 1);
    assert(run_query(&mock, sum_sql, sizeof(sum_sql) - 1, NULL, 0, 64,
                     &capture) == PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 2 && capture.keys[0] == 1 &&
           capture.aggregates[0] == 12 && capture.row_counts[0] == 2 &&
           capture.keys[1] == 2 && capture.aggregates[1] == 5 &&
           capture.row_counts[1] == 1);

    reset_scan(&mock, &capture, 1);
    assert(run_query(&mock, avg_sql, sizeof(avg_sql) - 1, NULL, 0, 64,
                     &capture) == PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 2 && capture.keys[0] == 1 &&
           capture.aggregates[0] == 6000000 && capture.row_counts[0] == 2 &&
           capture.keys[1] == 2 && capture.aggregates[1] == 5000000 &&
           capture.row_counts[1] == 1);

    reset_scan(&mock, &capture, 1);
    assert(run_query(&mock, limit_sql, sizeof(limit_sql) - 1, NULL, 0, 64,
                     &capture) == PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 1 && capture.keys[0] == 1 &&
           capture.aggregates[0] == 3);

    reset_scan(&mock, &capture, 1);
    assert(run_query(&mock, where_sql, sizeof(where_sql) - 1, &parameter, 1,
                     64, &capture) == PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 2 && capture.keys[0] == 1 &&
           capture.aggregates[0] == 9 && capture.keys[1] == 2 &&
           capture.aggregates[1] == 5);

    reset_scan(&mock, &capture, 1);
    assert(run_query(&mock, having_sql, sizeof(having_sql) - 1,
                     &having_parameter, 1, 64, &capture) ==
           PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 1 && capture.keys[0] == 2 &&
           capture.aggregates[0] == 1);

    reset_scan(&mock, &capture, 1);
    assert(run_query(&mock, ordered_aggregate_sql,
                     sizeof(ordered_aggregate_sql) - 1, NULL, 0, 64,
                     &capture) == PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 1 && capture.keys[0] == 2 &&
           capture.aggregates[0] == 5);

    {
        const uint8_t null_then_value[] = {
            PRDB_SDK_VALUE_STATE_NULL,
            PRDB_SDK_VALUE_STATE_NULL,
            PRDB_SDK_VALUE_STATE_VALUE};
        mock.batch_len = make_rows(mock.batch, 1, keys, scores,
                                   null_then_value, 3);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v11(&mock, nulls_last_sql,
                             sizeof(nulls_last_sql) - 1, 64, &capture) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && capture.keys[0] == 2 &&
               capture.aggregates[0] == 5 && capture.keys[1] == 1 &&
               capture.aggregate_states[1] == PRDB_SDK_VALUE_STATE_NULL &&
               mock.statement_failed == 0);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v11(&mock, default_null_order_sql,
                             sizeof(default_null_order_sql) - 1, 64,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && capture.keys[0] == 1 &&
               capture.aggregate_states[0] == PRDB_SDK_VALUE_STATE_NULL &&
               capture.keys[1] == 2 && capture.aggregates[1] == 5 &&
               mock.statement_failed == 0);
        mock.batch_len = make_rows(mock.batch, 1, keys, scores,
                                   value_states, 3);
    }

    reset_scan(&mock, &capture, 1);
    assert(run_query_v12(&mock, v12_no_order_sql,
                         sizeof(v12_no_order_sql) - 1, 64, &capture) ==
           PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 2 && capture.keys[0] == 1 &&
           capture.keys[1] == 2 && capture.row_counts[0] == 2 &&
           capture.row_counts[1] == 1 && mock.statement_failed == 0);

    {
        const int32_t expected_keys[][2] = {
            {1, 2}, {2, 1}, {1, 2}, {1, 2}, {1, 2}};
        const int32_t expected_aggregates[][2] = {
            {2, 1}, {5, 3}, {9, 5}, {12, 5}, {6000000, 5000000}};
        for (uint32_t kind = 0; kind < 5; ++kind) {
            reset_scan(&mock, &capture, 1);
            assert(run_query_v12(
                       &mock, ordered_aggregate_matrix_sql[kind],
                       ordered_aggregate_matrix_sql_len[kind], 64,
                       &capture) == PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 2 &&
                   capture.keys[0] == expected_keys[kind][0] &&
                   capture.keys[1] == expected_keys[kind][1] &&
                   capture.aggregates[0] == expected_aggregates[kind][0] &&
                   capture.aggregates[1] == expected_aggregates[kind][1] &&
                   mock.statement_failed == 0);
        }
    }

    {
        int32_t many_group_keys[129];
        int32_t many_group_scores[129];
        uint8_t many_group_states[129];
        for (uint32_t index = 0; index < 129; ++index) {
            many_group_keys[index] = (int32_t)(129 - index);
            many_group_scores[index] = 1;
            many_group_states[index] = PRDB_SDK_VALUE_STATE_VALUE;
        }

        mock.batch_len = make_rows(mock.batch, 1, many_group_keys,
                                   many_group_scores, many_group_states, 128);
        mock.max_candidates = 128;
        reset_scan(&mock, &capture, 1);
        assert(run_query_v12(&mock, ordered_group_limit_sql,
                             sizeof(ordered_group_limit_sql) - 1, 8192,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 128 && mock.statement_failed == 0);
        for (uint32_t index = 0; index < 128; ++index) {
            assert(capture.keys[index] == (int32_t)(index + 2));
            assert(capture.aggregates[index] == 1);
        }

        mock.batch_len = make_rows(mock.batch, 1, many_group_keys,
                                   many_group_scores, many_group_states, 40);
        mock.max_candidates = 40;
        reset_scan(&mock, &capture, 1);
        mock.cancel_after_group_callbacks = 32;
        assert(run_query_v12(&mock, ordered_group_limit_sql,
                             sizeof(ordered_group_limit_sql) - 1, 8192,
                             &capture) ==
               PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
        assert(capture.calls == 32 && mock.cancelled == 1 &&
               mock.statement_failed == 1);
        mock.cancel_after_group_callbacks = 0;

        mock.batch_len = make_rows(mock.batch, 0, many_group_keys,
                                   many_group_scores, many_group_states, 128);
        mock.continued_batch_len = make_rows(
            mock.continued_batch, 1, many_group_keys + 128,
            many_group_scores + 128, many_group_states + 128, 1);
        mock.streamed_pages = 2;
        mock.max_candidates = 128;
        reset_scan(&mock, &capture, 1);
        int32_t over_limit_status = run_query_v12(
            &mock, ordered_group_limit_sql,
            sizeof(ordered_group_limit_sql) - 1, 8192, &capture);
        assert(over_limit_status == PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT);
        assert(capture.calls == 0 && mock.statement_failed == 1);
        mock.max_candidates = 0;
        mock.streamed_pages = 0;
        mock.continued_batch_len = 0;
        mock.batch_len = make_rows(mock.batch, 1, keys, scores,
                                   value_states, 3);
    }

    {
        mock.text_key_mode = 1;
        mock.planner_len = make_relation(
            mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
            PRDB_SDK_VALUE_TYPE_TEXT);
        mock.executor_len = make_relation(mock.executor, 6, 0x22,
                                          PRDB_SDK_VALUE_TYPE_TEXT);
        mock.batch_len = make_text_rows(mock.batch, 0, 0, 64, 8192);
        mock.continued_batch_len = make_text_rows(
            mock.continued_batch, 1, 64, 64, 8192);
        mock.streamed_pages = 2;
        mock.max_candidates = 64;
        capture.text_key_bytes = 8192;
        reset_scan(&mock, &capture, 1);
        assert(run_query_v12(&mock, ordered_group_text_budget_sql,
                             sizeof(ordered_group_text_budget_sql) - 1,
                             PRDB_SDK_SQL_EXEC_V7_MAX_MATERIALIZED_BYTES,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 128 && mock.statement_failed == 0);
        for (uint32_t index = 0; index < 128; ++index) {
            assert(capture.keys[index] == (int32_t)index);
        }

        mock.batch_len = make_text_rows(mock.batch, 0, 0, 64, 8193);
        mock.continued_batch_len = make_text_rows(
            mock.continued_batch, 1, 64, 64, 8193);
        capture.text_key_bytes = 8193;
        reset_scan(&mock, &capture, 1);
        assert(run_query_v12(&mock, ordered_group_text_budget_sql,
                             sizeof(ordered_group_text_budget_sql) - 1,
                             PRDB_SDK_SQL_EXEC_V7_MAX_MATERIALIZED_BYTES,
                             &capture) ==
               PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT);
        assert(capture.calls == 0 && mock.statement_failed == 1);

        mock.text_key_mode = 0;
        mock.max_candidates = 0;
        mock.streamed_pages = 0;
        mock.continued_batch_len = 0;
        capture.text_key_bytes = 0;
        mock.planner_len = make_relation(
            mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
            PRDB_SDK_VALUE_TYPE_INTEGER);
        mock.executor_len = make_relation(mock.executor, 6, 0x22,
                                          PRDB_SDK_VALUE_TYPE_INTEGER);
        mock.batch_len = make_rows(mock.batch, 1, keys, scores,
                                   value_states, 3);
    }

    {
        mock.text_key_mode = 1;
        mock.planner_len = make_relation(
            mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
            PRDB_SDK_VALUE_TYPE_TEXT);
        mock.executor_len = make_relation(mock.executor, 6, 0x22,
                                          PRDB_SDK_VALUE_TYPE_TEXT);
        mock.batch_len = make_text_grouped_sum_rows(
            mock.batch, 0, 0, 64, 8188);
        mock.continued_batch_len = make_text_grouped_sum_rows(
            mock.continued_batch, 1, 64, 64, 8188);
        mock.streamed_pages = 2;
        mock.max_candidates = 64;
        capture.text_key_bytes = 8188;
        capture.text_sum_tie_order_mode = 1;
        reset_scan(&mock, &capture, 1);
        assert(run_query_v12(&mock, ordered_grouped_sum_tie_sql,
                             sizeof(ordered_grouped_sum_tie_sql) - 1,
                             PRDB_SDK_SQL_EXEC_V7_MAX_MATERIALIZED_BYTES,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 64 && mock.statement_failed == 0);
        for (uint32_t group = 0; group < 64; ++group) {
            uint32_t expected_key = 62 - 2 * (group / 2) + group % 2;
            assert(capture.keys[group] == (int32_t)expected_key);
            assert(capture.aggregates[group] ==
                   (int32_t)(300 + (expected_key / 2) * 2));
            assert(capture.row_counts[group] == 2);
        }
        assert(mock.executor_requests <= 256);

        mock.batch_len = make_text_grouped_sum_rows(
            mock.batch, 0, 0, 64, 8189);
        mock.continued_batch_len = make_text_grouped_sum_rows(
            mock.continued_batch, 1, 64, 64, 8189);
        capture.text_key_bytes = 8189;
        reset_scan(&mock, &capture, 1);
        assert(run_query_v12(&mock, ordered_grouped_sum_tie_sql,
                             sizeof(ordered_grouped_sum_tie_sql) - 1,
                             PRDB_SDK_SQL_EXEC_V7_MAX_MATERIALIZED_BYTES,
                             &capture) ==
               PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT);
        assert(capture.calls == 0 && mock.statement_failed == 1);

        mock.text_key_mode = 0;
        mock.max_candidates = 0;
        mock.streamed_pages = 0;
        mock.continued_batch_len = 0;
        capture.text_key_bytes = 0;
        capture.text_sum_tie_order_mode = 0;
        mock.planner_len = make_relation(
            mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
            PRDB_SDK_VALUE_TYPE_INTEGER);
        mock.executor_len = make_relation(mock.executor, 6, 0x22,
                                          PRDB_SDK_VALUE_TYPE_INTEGER);
        mock.batch_len = make_rows(mock.batch, 1, keys, scores,
                                   value_states, 3);
    }

    {
        static const uint8_t eight_key_sql[] =
            "SELECT key_0, key_1, key_2, key_3, key_4, key_5, key_6, key_7, "
            "SUM(measure) FROM widgets GROUP BY key_0, key_1, key_2, key_3, "
            "key_4, key_5, key_6, key_7";
        uint8_t planner_response[512] = {0};
        uint8_t executor_response[512] = {0};
        uint8_t batch_response[4096] = {0};
        uint8_t materialized_bytes[4096] = {0};
        prdb_sdk_sql_column_shape_v2 planner_columns[9] = {{0}};
        prdb_sdk_sql_column_shape_v2 executor_columns[9] = {{0}};
        prdb_sdk_value_v1 cell_scratch[9] = {{0}};
        prdb_host_api_v3 host = {
            sizeof(host), PRDB_QUERY_ABI_V3_VERSION, &mock,
            executor_request, planner_request, NULL, statement_failure};
        group_capture capture = {0};
        int32_t status;
        capture.mock = &mock;
        mock.planner_len = make_eight_key_relation(
            mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99);
        mock.executor_len = make_eight_key_relation(mock.executor, 6, 0x22);
        mock.batch_len = make_eight_key_rows(mock.batch);
        reset_scan(&mock, &capture, 1);
        status = prdb_sdk_sql_execute_group_v13(
            &host, (const uint8_t *)"app", 3, eight_key_sql,
            sizeof(eight_key_sql) - 1, NULL, 0, 4, 4096, planner_response,
            sizeof(planner_response), executor_response,
            sizeof(executor_response), batch_response,
            sizeof(batch_response), planner_columns, 9, executor_columns, 9,
            cell_scratch, 9, materialized_bytes, sizeof(materialized_bytes),
            on_group_eight_keys, &capture);
        assert(status == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && mock.statement_failed == 0);
        mock.planner_len = make_relation(
            mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
            PRDB_SDK_VALUE_TYPE_INTEGER);
        mock.executor_len = make_relation(mock.executor, 6, 0x22,
                                          PRDB_SDK_VALUE_TYPE_INTEGER);
    }

    {
        static const uint8_t ordered_eight_key_sql[] =
            "SELECT key_0, key_1, key_2, key_3, key_4, key_5, key_6, key_7, "
            "SUM(measure) FROM widgets GROUP BY key_0, key_1, key_2, key_3, "
            "key_4, key_5, key_6, key_7 ORDER BY key_0 DESC, key_1 DESC, "
            "key_2 DESC, key_3 DESC, key_4 DESC, key_5 DESC, key_6 DESC, "
            "key_7 DESC, SUM(measure) DESC LIMIT 5";
        uint8_t planner_response[512] = {0};
        uint8_t executor_response[512] = {0};
        uint8_t batch_response[4096] = {0};
        uint8_t materialized_bytes[4096] = {0};
        prdb_sdk_sql_column_shape_v2 planner_columns[9] = {{0}};
        prdb_sdk_sql_column_shape_v2 executor_columns[9] = {{0}};
        prdb_sdk_value_v1 cell_scratch[9] = {{0}};
        prdb_host_api_v3 host = {
            sizeof(host), PRDB_QUERY_ABI_V3_VERSION, &mock,
            executor_request, planner_request, NULL, statement_failure};
        group_capture capture = {0};
        capture.mock = &mock;
        mock.planner_len = make_eight_key_relation(
            mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99);
        mock.executor_len = make_eight_key_relation(mock.executor, 6, 0x22);
        mock.batch_len = make_eight_key_rows(mock.batch);
        reset_scan(&mock, &capture, 1);
        assert(prdb_sdk_sql_execute_group_v15(
                   &host, (const uint8_t *)"app", 3, ordered_eight_key_sql,
                   sizeof(ordered_eight_key_sql) - 1, NULL, 0, 4, 4096,
                   planner_response, sizeof(planner_response),
                   executor_response, sizeof(executor_response), batch_response,
                   sizeof(batch_response), planner_columns, 9,
                   executor_columns, 9, cell_scratch, 9, materialized_bytes,
                   sizeof(materialized_bytes), on_ordered_group_eight_keys,
                   &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && mock.statement_failed == 0);
        mock.optimizer_aggregate_kind = PRDB_SDK_SQL_V7_AGGREGATE_SUM;
        mock.optimizer_relation_resolves = 0;
        mock.planner_len = make_eight_key_relation(
            mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99);
        mock.executor_len = make_eight_key_relation(mock.executor, 6, 0x22);
        mock.batch_len = make_eight_key_rows(mock.batch);
        reset_scan(&mock, &capture, 1);
        int32_t ordered_optimizer_status = run_ordered_eight_key_optimizer(
            &mock, ordered_eight_key_sql,
            sizeof(ordered_eight_key_sql) - 1, &capture);
        assert(ordered_optimizer_status == PRDB_SDK_SQL_EXEC_OK);
        assert(mock.optimizer_enabled == 1 &&
               mock.optimizer_relation_resolves == 1 &&
               capture.calls == 2 && mock.statement_failed == 0);
        mock.planner_len = make_relation(
            mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
            PRDB_SDK_VALUE_TYPE_INTEGER);
        mock.executor_len = make_relation(mock.executor, 6, 0x22,
                                          PRDB_SDK_VALUE_TYPE_INTEGER);
    }

    {
        const int32_t tuple_ids[] = {1, 1, 2};
        const int32_t tuple_scores[] = {9, 9, 5};
        mock.batch_len = make_rows(mock.batch, 1, tuple_ids, tuple_scores,
                                   value_states, 3);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v13(&mock, v13_composite_sql,
                             sizeof(v13_composite_sql) - 1, 64,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && capture.keys[0] == 1 &&
               capture.second_keys[0] == 9 && capture.row_counts[0] == 2 &&
               capture.keys[1] == 2 && capture.second_keys[1] == 5 &&
               capture.row_counts[1] == 1 && mock.statement_failed == 0);
        {
            group_capture reset_capture = {0};
            nullable_composite_capture null_capture = {&mock, 0};
            mock.cancel_when_batch_finishes = 0;
            mock.batch_len = make_nullable_composite_rows(mock.batch);
            reset_scan(&mock, &reset_capture, 1);
            int32_t nullable_status = run_nullable_composite_count_v13(
                &mock, v13_nullable_composite_sql,
                sizeof(v13_nullable_composite_sql) - 1, &null_capture);
            assert(nullable_status == PRDB_SDK_SQL_EXEC_OK);
            assert(null_capture.calls == 4 && mock.statement_failed == 0);
        }
        mock.batch_len = make_rows(mock.batch, 1, tuple_ids, tuple_scores,
                                   value_states, 3);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v13(&mock, v13_sum_sql,
                             sizeof(v13_sum_sql) - 1, 64,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && capture.aggregates[0] == 18 &&
               capture.aggregates[1] == 5 && mock.statement_failed == 0);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v13(&mock, v13_min_sql,
                             sizeof(v13_min_sql) - 1, 64,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && capture.aggregates[0] == 9 &&
               capture.aggregates[1] == 5 && mock.statement_failed == 0);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v13(&mock, v13_max_sql,
                             sizeof(v13_max_sql) - 1, 64,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && capture.aggregates[0] == 9 &&
               capture.aggregates[1] == 5 && mock.statement_failed == 0);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v13(&mock, v13_avg_sql,
                             sizeof(v13_avg_sql) - 1, 64,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && capture.aggregates[0] == 9 &&
               capture.aggregates[1] == 5 && mock.statement_failed == 0);
        for (uint32_t kind = PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR;
             kind <= PRDB_SDK_SQL_V7_AGGREGATE_AVG; ++kind) {
            int32_t optimizer_status;
            mock.optimizer_aggregate_kind = kind;
            reset_scan(&mock, &capture, 1);
            optimizer_status = run_query_v13_optimizer(
                &mock, v13_optimizer_sql[kind - 1],
                v13_optimizer_sql_lens[kind - 1], 64, &capture);
            assert(optimizer_status == PRDB_SDK_SQL_EXEC_OK);
            assert(mock.optimizer_enabled == 1 && capture.calls == 2 &&
                   capture.keys[0] == 1 && capture.second_keys[0] == 9 &&
                   capture.keys[1] == 2 && capture.second_keys[1] == 5 &&
                   mock.statement_failed == 0);
            if (kind == PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
                assert(capture.aggregates[0] == 2 &&
                       capture.aggregates[1] == 1 &&
                       capture.row_counts[0] == 2 &&
                       capture.row_counts[1] == 1);
            } else if (kind == PRDB_SDK_SQL_V7_AGGREGATE_SUM) {
                assert(capture.aggregates[0] == 18 &&
                       capture.aggregates[1] == 5);
            } else {
                assert(capture.aggregates[0] == 9 &&
                       capture.aggregates[1] == 5);
            }
        }
        {
            static const uint8_t filtered_optimizer_sql[] =
                "SELECT id, score, SUM(score) FROM widgets WHERE id = $1 "
                "GROUP BY id, score LIMIT 5";
            uint8_t parameter_bytes[4] = {0, 0, 0, 1};
            prdb_sdk_value_v1 parameters[1] = {{
                sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
                PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
                parameter_bytes, sizeof(parameter_bytes)}};
            mock.optimizer_aggregate_kind = PRDB_SDK_SQL_V7_AGGREGATE_SUM;
            reset_scan(&mock, &capture, 1);
            int32_t filtered_status = run_query_v13_optimizer_internal(
                &mock, filtered_optimizer_sql,
                sizeof(filtered_optimizer_sql) - 1, parameters, 1, 1, 64,
                &capture);
            assert(filtered_status == PRDB_SDK_SQL_EXEC_OK);
            assert(mock.optimizer_enabled == 1 && capture.calls == 1 &&
                   capture.keys[0] == 1 && capture.second_keys[0] == 9 &&
                   capture.aggregates[0] == 18 && capture.row_counts[0] == 2 &&
                   mock.statement_failed == 0);
        }
        {
            const int32_t having_keys[] = {1, 1, 2};
            const int32_t having_values[] = {0, 2, 3};
            const uint8_t having_states[] = {
                PRDB_SDK_VALUE_STATE_NULL,
                PRDB_SDK_VALUE_STATE_VALUE,
                PRDB_SDK_VALUE_STATE_VALUE};
            mock.planner_len = make_relation(
                mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
                PRDB_SDK_VALUE_TYPE_INTEGER);
            mock.executor_len = make_relation(
                mock.executor, 6, 0x22, PRDB_SDK_VALUE_TYPE_INTEGER);
            mock.batch_len = make_rows(mock.batch, 1, having_keys,
                                       having_values, having_states, 3);
            reset_scan(&mock, &capture, 1);
            int32_t optimizer_having_status = run_query_v24_optimizer_having(
                &mock, v24_optimizer_having_sql,
                sizeof(v24_optimizer_having_sql) - 1, 4096, &capture,
                on_group_v24_optimizer_having);
            if (optimizer_having_status != PRDB_SDK_SQL_EXEC_OK) {
                fprintf(stderr,
                        "v24 optimizer HAVING returned %d, failed=%u, planner=%u, executor=%u, optimizer=%u\n",
                        optimizer_having_status, mock.statement_failed,
                        mock.planner_calls, mock.executor_calls,
                        mock.optimizer_enabled);
            }
            assert(optimizer_having_status == PRDB_SDK_SQL_EXEC_OK);
            assert(mock.optimizer_enabled == 1 &&
                   mock.optimizer_relation_resolves == 1 &&
                   capture.calls == 2 && capture.keys[0] == 1 &&
                   capture.second_keys[0] == 2 &&
                   capture.aggregates[0] == 2 && capture.keys[1] == 2 &&
                   capture.second_keys[1] == 3 &&
                   capture.aggregates[1] == 3 &&
                   mock.statement_failed == 0);
            reset_scan(&mock, &capture, 1);
            optimizer_having_status = run_query_v24_optimizer_having(
                &mock, v24_optimizer_having_sql,
                sizeof(v24_optimizer_having_sql) - 1, 1, &capture,
                on_group_v24_optimizer_having);
            assert(optimizer_having_status != PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 0 && mock.statement_failed == 1);
            {
                const int32_t restored_keys[] = {1, 1, 2};
                const int32_t restored_values[] = {9, 9, 5};
                const uint8_t restored_states[] = {
                    PRDB_SDK_VALUE_STATE_VALUE,
                    PRDB_SDK_VALUE_STATE_VALUE,
                    PRDB_SDK_VALUE_STATE_VALUE};
                mock.planner_len = make_relation(
                    mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
                    PRDB_SDK_VALUE_TYPE_INTEGER);
                mock.executor_len = make_relation(
                    mock.executor, 6, 0x22, PRDB_SDK_VALUE_TYPE_INTEGER);
                mock.batch_len = make_rows(mock.batch, 1, restored_keys,
                                           restored_values, restored_states, 3);
            }
        }
        {
            const int32_t having_keys[] = {1, 1, 2};
            const int32_t having_values[] = {0, 2, 3};
            const uint8_t having_states[] = {
                PRDB_SDK_VALUE_STATE_NULL,
                PRDB_SDK_VALUE_STATE_VALUE,
                PRDB_SDK_VALUE_STATE_VALUE};
            mock.planner_len = make_relation(
                mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
                PRDB_SDK_VALUE_TYPE_INTEGER);
            mock.executor_len = make_relation(
                mock.executor, 6, 0x22, PRDB_SDK_VALUE_TYPE_INTEGER);
            mock.batch_len = make_rows(mock.batch, 1, having_keys,
                                       having_values, having_states, 3);
            reset_scan(&mock, &capture, 1);
            int32_t filtered_ordered_status = run_query_v24_optimizer_having(
                &mock, v24_optimizer_filtered_ordered_composite_having_sql,
                sizeof(v24_optimizer_filtered_ordered_composite_having_sql) - 1,
                4096, &capture, on_group_v24_optimizer_having);
            if (filtered_ordered_status != PRDB_SDK_SQL_EXEC_OK) {
                fprintf(stderr,
                        "v24 filtered ordered composite optimizer HAVING returned %d, failed=%u, planner=%u, executor=%u, optimizer=%u\n",
                        filtered_ordered_status, mock.statement_failed,
                        mock.planner_calls, mock.executor_calls,
                        mock.optimizer_enabled);
            }
            assert(filtered_ordered_status == PRDB_SDK_SQL_EXEC_OK);
            assert(mock.optimizer_enabled == 1 &&
                   mock.optimizer_relation_resolves == 1 &&
                   capture.calls == 2 && mock.statement_failed == 0);
            {
                const int32_t restored_keys[] = {1, 1, 2};
                const int32_t restored_values[] = {9, 9, 5};
                const uint8_t restored_states[] = {
                    PRDB_SDK_VALUE_STATE_VALUE,
                    PRDB_SDK_VALUE_STATE_VALUE,
                    PRDB_SDK_VALUE_STATE_VALUE};
                mock.planner_len = make_relation(
                    mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
                    PRDB_SDK_VALUE_TYPE_INTEGER);
                mock.executor_len = make_relation(
                    mock.executor, 6, 0x22, PRDB_SDK_VALUE_TYPE_INTEGER);
                mock.batch_len = make_rows(mock.batch, 1, restored_keys,
                                           restored_values, restored_states, 3);
            }
        }
        {
            const int32_t having_keys[] = {1, 1, 2};
            const int32_t having_values[] = {0, 2, 3};
            const uint8_t having_states[] = {
                PRDB_SDK_VALUE_STATE_NULL,
                PRDB_SDK_VALUE_STATE_VALUE,
                PRDB_SDK_VALUE_STATE_VALUE};
            mock.planner_len = make_relation(
                mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
                PRDB_SDK_VALUE_TYPE_INTEGER);
            mock.executor_len = make_relation(
                mock.executor, 6, 0x22, PRDB_SDK_VALUE_TYPE_INTEGER);
            mock.batch_len = make_rows(mock.batch, 1, having_keys,
                                       having_values, having_states, 3);
            reset_scan(&mock, &capture, 1);
            int32_t filtered_having_status = run_query_v24_optimizer_having(
                &mock, v24_optimizer_single_filtered_having_sql,
                sizeof(v24_optimizer_single_filtered_having_sql) - 1,
                4096, &capture, on_group_v24_optimizer_single_having);
            if (filtered_having_status != PRDB_SDK_SQL_EXEC_OK) {
                fprintf(stderr,
                        "v24 filtered single-key optimizer HAVING returned %d, failed=%u, planner=%u, executor=%u, optimizer=%u\n",
                        filtered_having_status, mock.statement_failed,
                        mock.planner_calls, mock.executor_calls,
                        mock.optimizer_enabled);
            }
            assert(filtered_having_status == PRDB_SDK_SQL_EXEC_OK);
            assert(mock.optimizer_enabled == 1 &&
                   mock.optimizer_relation_resolves == 1 &&
                   capture.calls == 2 && capture.keys[0] == 1 &&
                   capture.aggregates[0] == 2 && capture.keys[1] == 2 &&
                   capture.aggregates[1] == 3 &&
                   mock.statement_failed == 0);
            {
                const int32_t restored_keys[] = {1, 1, 2};
                const int32_t restored_values[] = {9, 9, 5};
                const uint8_t restored_states[] = {
                    PRDB_SDK_VALUE_STATE_VALUE,
                    PRDB_SDK_VALUE_STATE_VALUE,
                    PRDB_SDK_VALUE_STATE_VALUE};
                mock.planner_len = make_relation(
                    mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
                    PRDB_SDK_VALUE_TYPE_INTEGER);
                mock.executor_len = make_relation(
                    mock.executor, 6, 0x22, PRDB_SDK_VALUE_TYPE_INTEGER);
                mock.batch_len = make_rows(mock.batch, 1, restored_keys,
                                           restored_values, restored_states, 3);
            }
        }
        {
            const int32_t having_keys[] = {1, 1, 2};
            const int32_t having_values[] = {0, 2, 3};
            const uint8_t having_states[] = {
                PRDB_SDK_VALUE_STATE_NULL,
                PRDB_SDK_VALUE_STATE_VALUE,
                PRDB_SDK_VALUE_STATE_VALUE};
            mock.planner_len = make_relation(
                mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
                PRDB_SDK_VALUE_TYPE_INTEGER);
            mock.executor_len = make_relation(
                mock.executor, 6, 0x22, PRDB_SDK_VALUE_TYPE_INTEGER);
            mock.batch_len = make_rows(mock.batch, 1, having_keys,
                                       having_values, having_states, 3);
            reset_scan(&mock, &capture, 1);
            int32_t single_having_status = run_query_v24_optimizer_having(
                &mock, v24_optimizer_single_having_sql,
                sizeof(v24_optimizer_single_having_sql) - 1, 4096,
                &capture, on_group_v24_optimizer_single_having);
            if (single_having_status != PRDB_SDK_SQL_EXEC_OK) {
                fprintf(stderr,
                        "v24 single-key optimizer HAVING returned %d, failed=%u, planner=%u, executor=%u, optimizer=%u\n",
                        single_having_status, mock.statement_failed,
                        mock.planner_calls, mock.executor_calls,
                        mock.optimizer_enabled);
            }
            assert(single_having_status == PRDB_SDK_SQL_EXEC_OK);
            assert(mock.optimizer_enabled == 1 &&
                   mock.optimizer_relation_resolves == 1 &&
                   capture.calls == 2 && capture.keys[0] == 1 &&
                   capture.aggregates[0] == 2 && capture.keys[1] == 2 &&
                   capture.aggregates[1] == 3 &&
                   mock.statement_failed == 0);
            {
                const int32_t restored_keys[] = {1, 1, 2};
                const int32_t restored_values[] = {9, 9, 5};
                const uint8_t restored_states[] = {
                    PRDB_SDK_VALUE_STATE_VALUE,
                    PRDB_SDK_VALUE_STATE_VALUE,
                    PRDB_SDK_VALUE_STATE_VALUE};
                mock.planner_len = make_relation(
                    mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
                    PRDB_SDK_VALUE_TYPE_INTEGER);
                mock.executor_len = make_relation(
                    mock.executor, 6, 0x22, PRDB_SDK_VALUE_TYPE_INTEGER);
                mock.batch_len = make_rows(mock.batch, 1, restored_keys,
                                           restored_values, restored_states, 3);
            }
        }
        {
            const int32_t having_keys[] = {1, 1, 2};
            const int32_t having_values[] = {0, 2, 3};
            const uint8_t having_states[] = {
                PRDB_SDK_VALUE_STATE_NULL,
                PRDB_SDK_VALUE_STATE_VALUE,
                PRDB_SDK_VALUE_STATE_VALUE};
            mock.planner_len = make_relation(
                mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
                PRDB_SDK_VALUE_TYPE_INTEGER);
            mock.executor_len = make_relation(
                mock.executor, 6, 0x22, PRDB_SDK_VALUE_TYPE_INTEGER);
            mock.batch_len = make_rows(mock.batch, 1, having_keys,
                                       having_values, having_states, 3);
            reset_scan(&mock, &capture, 1);
            int32_t ordered_having_status = run_query_v24_optimizer_having(
                &mock, v24_optimizer_single_ordered_having_sql,
                sizeof(v24_optimizer_single_ordered_having_sql) - 1, 4096,
                &capture, on_group_v24_optimizer_single_ordered_having);
            if (ordered_having_status != PRDB_SDK_SQL_EXEC_OK) {
                fprintf(stderr,
                        "v24 ordered optimizer HAVING returned %d, failed=%u, planner=%u, executor=%u, optimizer=%u\n",
                        ordered_having_status, mock.statement_failed,
                        mock.planner_calls, mock.executor_calls,
                        mock.optimizer_enabled);
            }
            assert(ordered_having_status == PRDB_SDK_SQL_EXEC_OK);
            assert(mock.optimizer_enabled == 1 &&
                   mock.optimizer_relation_resolves == 1 &&
                   capture.calls == 2 && capture.keys[0] == 2 &&
                   capture.aggregates[0] == 3 && capture.keys[1] == 1 &&
                   capture.aggregates[1] == 2 &&
                   mock.statement_failed == 0);
            {
                const int32_t restored_keys[] = {1, 1, 2};
                const int32_t restored_values[] = {9, 9, 5};
                const uint8_t restored_states[] = {
                    PRDB_SDK_VALUE_STATE_VALUE,
                    PRDB_SDK_VALUE_STATE_VALUE,
                    PRDB_SDK_VALUE_STATE_VALUE};
                mock.planner_len = make_relation(
                    mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
                    PRDB_SDK_VALUE_TYPE_INTEGER);
                mock.executor_len = make_relation(
                    mock.executor, 6, 0x22, PRDB_SDK_VALUE_TYPE_INTEGER);
                mock.batch_len = make_rows(mock.batch, 1, restored_keys,
                                           restored_values, restored_states, 3);
            }
        }
        reset_scan(&mock, &capture, 1);
        assert(run_query_v14(&mock, v14_having_sql,
                             sizeof(v14_having_sql) - 1,
                             &v14_having_parameter, &capture) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 1 && capture.keys[0] == 1 &&
               capture.aggregates[0] == 18 && mock.statement_failed == 0);

        reset_scan(&mock, &capture, 1);
        assert(run_query_v17(&mock, v17_overflow_having_sql,
                             sizeof(v17_overflow_having_sql) - 1, NULL,
                             &capture) == PRDB_SDK_SQL_EXEC_BIND_ERROR);
        assert(capture.calls == 0 && mock.next_calls == 0 &&
               mock.statement_failed == 1);

        {
            const int32_t nullable_keys[] = {1, 1, 2, 3};
            const int32_t nullable_scores[] = {9, 3, 5, 0};
            const uint8_t nullable_states[] = {
                PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_STATE_VALUE,
                PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_STATE_NULL};
            mock.batch_len = make_rows(mock.batch, 1, nullable_keys,
                                       nullable_scores, nullable_states, 4);
            reset_scan(&mock, &capture, 1);
            /* The grouped inputs produce TRUE (9, 3), FALSE (5), and
             * UNKNOWN (the NULL-only group) for the compound HAVING tree. */
            assert(run_query_v18(&mock, v18_compound_having_sql,
                                 sizeof(v18_compound_having_sql) - 1,
                                 &v18_having_parameter, &capture) ==
                   PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 2 && capture.keys[0] == 1 &&
                   capture.second_keys[0] == 9 &&
                   capture.aggregates[0] == 9 && capture.keys[1] == 1 &&
                   capture.second_keys[1] == 3 &&
                   capture.aggregates[1] == 3 && mock.statement_failed == 0);

        reset_scan(&mock, &capture, 1);
        capture.expect_having_literal = 1;
        assert(run_query_v18(&mock, v18_negative_literal_sql,
                             sizeof(v18_negative_literal_sql) - 1, NULL,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 3 && capture.aggregates[0] == 9 &&
               capture.aggregates[1] == 3 && capture.aggregates[2] == 5 &&
               mock.statement_failed == 0);
        capture.expect_having_literal = 0;
        }
        {
            const int32_t one_key[] = {1};
            const int32_t one_score[] = {9};
            const uint8_t one_value_state[] = {
                PRDB_SDK_VALUE_STATE_VALUE};
            mock.batch_len = make_rows(mock.batch, 0, one_key, one_score,
                                       one_value_state, 1);
            mock.streamed_pages = 128;
            mock.max_candidates = 1;
            reset_scan(&mock, &capture, 1);
            capture.expected_row_count = 128;
            int32_t status = run_query_v18(
                &mock, v18_compound_having_sql,
                sizeof(v18_compound_having_sql) - 1, &v18_having_parameter,
                &capture);
            assert(capture.calls == 0);
            assert(mock.next_calls == 124);
            assert(mock.executor_requests == 251);
            assert(mock.statement_failed == 1);
            assert(status == PRDB_SDK_SQL_EXEC_REQUEST_LIMIT);
            mock.streamed_pages = 0;
            mock.max_candidates = 0;
            capture.expected_row_count = 0;
        }
        {
            int32_t many_keys[40];
            int32_t many_scores[40];
            uint8_t many_value_states[40];
            for (uint32_t index = 0; index < 40; ++index) {
                many_keys[index] = 1;
                many_scores[index] = (int32_t)index + 9;
                many_value_states[index] = PRDB_SDK_VALUE_STATE_VALUE;
            }
            mock.batch_len = make_rows(mock.batch, 1, many_keys, many_scores,
                                       many_value_states, 40);
            mock.max_candidates = 40;
            reset_scan(&mock, &capture, 1);
            mock.cancel_when_batch_finishes = 1;
            int32_t before_callback_status = run_query_v13(
                &mock, v13_many_groups_sql, sizeof(v13_many_groups_sql) - 1,
                4096, &capture);
            assert(before_callback_status == PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
            assert(capture.calls == 0 && mock.cancelled == 1);
            assert(mock.executor_requests == 5 && mock.statement_failed == 1);

            reset_scan(&mock, &capture, 1);
            mock.cancel_when_batch_finishes = 0;
            mock.cancel_after_group_callbacks = 32;
            int32_t status = run_query_v13(
                &mock, v13_many_groups_sql, sizeof(v13_many_groups_sql) - 1,
                4096, &capture);
            assert(status == PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
            assert(capture.calls == 32 && mock.cancelled == 1);
            assert(mock.executor_requests == 7 && mock.statement_failed == 1);
            mock.cancel_after_group_callbacks = 0;
            mock.max_candidates = 0;
        }
        mock.batch_len = make_rows(mock.batch, 1, keys, scores,
                                   value_states, 3);
    }

    {
        const int32_t tuple_ids[] = {2, 1, 1, 0};
        const int32_t tuple_scores[] = {9, 9, 9, 9};
        const uint8_t tuple_states[] = {
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_STATE_VALUE,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_STATE_VALUE};
        const int32_t sorted_ids[] = {1, 0, 2};
        mock.batch_len = make_rows(mock.batch, 1, tuple_ids, tuple_scores,
                                   tuple_states, 4);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v15(&mock, v15_order_sql,
                             sizeof(v15_order_sql) - 1,
                             &v15_having_parameter, &capture) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 3 && mock.statement_failed == 0);
        for (uint32_t index = 0; index < 3; ++index) {
            assert(capture.keys[index] == sorted_ids[index]);
            assert(capture.second_keys[index] == 9);
        }
        assert(capture.row_counts[0] == 2 && capture.row_counts[1] == 1 &&
               capture.row_counts[2] == 1);

        reset_scan(&mock, &capture, 1);
        capture.expect_having_literal = 1;
        assert(run_query_v17(&mock, v17_literal_having_sql,
                             sizeof(v17_literal_having_sql) - 1,
                             NULL,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 1 && capture.keys[0] == 1 &&
               capture.aggregates[0] == 18 && mock.statement_failed == 0);

        reset_scan(&mock, &capture, 1);
        capture.expect_having_literal = 0;
        assert(run_query_v17(&mock, v17_parameter_having_sql,
                             sizeof(v17_parameter_having_sql) - 1,
                             &v17_having_parameter, &capture) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 1 && capture.keys[0] == 1 &&
               capture.aggregates[0] == 18 && mock.statement_failed == 0);

        mock.batch_len = make_rows(mock.batch, 1, keys, scores,
                                   value_states, 3);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v18(&mock, v18_compound_having_sql,
                             sizeof(v18_compound_having_sql) - 1,
                             &v18_having_parameter, &capture) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && capture.keys[0] == 1 &&
               capture.second_keys[0] == 9 && capture.aggregates[0] == 9 &&
               capture.keys[1] == 1 && capture.second_keys[1] == 3 &&
               capture.aggregates[1] == 3 && mock.statement_failed == 0);

        reset_scan(&mock, &capture, 1);
        assert(run_query_v18(&mock, v18_compound_having_sql,
                             sizeof(v18_compound_having_sql) - 1, NULL,
                             &capture) == PRDB_SDK_SQL_EXEC_BIND_ERROR);
        assert(capture.calls == 0 && mock.next_calls == 0 &&
               mock.statement_failed == 1);

        reset_scan(&mock, &capture, 1);
        assert(run_query_v18(&mock, v18_compound_having_sql,
                             sizeof(v18_compound_having_sql) - 1, &parameter,
                             &capture) == PRDB_SDK_SQL_EXEC_BIND_ERROR);
        assert(capture.calls == 0 && mock.next_calls == 0 &&
               mock.statement_failed == 1);
        mock.batch_len = make_rows(mock.batch, 1, keys, scores,
                                   value_states, 3);
    }

    {
        const int32_t tied_group_keys[] = {1, 2, 3};
        mock.batch_len = make_rows(mock.batch, 1, tied_group_keys, scores,
                                   value_states, 3);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v12(&mock, composite_order_sql,
                             sizeof(composite_order_sql) - 1, 64,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && capture.keys[0] == 3 &&
               capture.keys[1] == 2 && capture.row_counts[0] == 1 &&
               capture.row_counts[1] == 1 && mock.statement_failed == 0);
        mock.batch_len = make_rows(mock.batch, 1, keys, scores,
                                   value_states, 3);
    }

    reset_scan(&mock, &capture, 1);
    assert(run_query(&mock, ordered_having_sql,
                     sizeof(ordered_having_sql) - 1, &having_parameter, 1, 64,
                     &capture) == PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 1 && capture.keys[0] == 2 &&
           capture.aggregates[0] == 1);

    reset_scan(&mock, &capture, 1);
    assert(run_query(&mock, ordered_key_sql, sizeof(ordered_key_sql) - 1,
                     NULL, 0, 64, &capture) == PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 1 && capture.keys[0] == 2 &&
           capture.aggregates[0] == 1);

    {
        prdb_sdk_value_v1 null_parameter = {
            sizeof(null_parameter), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_NULL, 0, NULL, 0};
        reset_scan(&mock, &capture, 1);
        assert(run_query(&mock, having_sql, sizeof(having_sql) - 1,
                         &null_parameter, 1, 64, &capture) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 0 && mock.next_calls == 1 &&
               mock.statement_failed == 0);
    }

    {
        prdb_sdk_value_v1 invalid_parameter = parameter;
        reset_scan(&mock, &capture, 1);
        assert(run_query(&mock, having_sql, sizeof(having_sql) - 1,
                         &invalid_parameter, 1, 64, &capture) ==
               PRDB_SDK_SQL_EXEC_BIND_ERROR);
        assert(capture.calls == 0 && mock.next_calls == 0 &&
               mock.statement_failed == 1);
        invalid_parameter.state = PRDB_SDK_VALUE_STATE_REDACTED;
        invalid_parameter.type_code = 0;
        invalid_parameter.data = NULL;
        invalid_parameter.data_len = 0;
        reset_scan(&mock, &capture, 1);
        assert(run_query(&mock, having_sql, sizeof(having_sql) - 1,
                         &invalid_parameter, 1, 64, &capture) ==
               PRDB_SDK_SQL_EXEC_BIND_ERROR);
        assert(capture.calls == 0 && mock.next_calls == 0 &&
               mock.statement_failed == 1);
    }

    {
        const uint8_t null_states[] = {
            PRDB_SDK_VALUE_STATE_NULL,
            PRDB_SDK_VALUE_STATE_NULL,
            PRDB_SDK_VALUE_STATE_NULL};
        mock.batch_len = make_rows(mock.batch, 1, keys, scores, null_states, 3);
        reset_scan(&mock, &capture, 1);
        assert(run_query(&mock, min_sql, sizeof(min_sql) - 1, NULL, 0, 64,
                         &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 &&
               capture.aggregate_states[0] == PRDB_SDK_VALUE_STATE_NULL &&
               capture.aggregate_states[1] == PRDB_SDK_VALUE_STATE_NULL);
        reset_scan(&mock, &capture, 1);
        assert(run_query(&mock, max_sql, sizeof(max_sql) - 1, NULL, 0, 64,
                         &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 &&
               capture.aggregate_states[0] == PRDB_SDK_VALUE_STATE_NULL &&
               capture.aggregate_states[1] == PRDB_SDK_VALUE_STATE_NULL);
        {
            const uint8_t partly_null_states[] = {
                PRDB_SDK_VALUE_STATE_NULL,
                PRDB_SDK_VALUE_STATE_NULL,
                PRDB_SDK_VALUE_STATE_VALUE};
            mock.batch_len = make_rows(mock.batch, 1, keys, scores,
                                       partly_null_states, 3);
            reset_scan(&mock, &capture, 1);
            assert(run_query(&mock, ordered_aggregate_sql,
                             sizeof(ordered_aggregate_sql) - 1, NULL, 0, 64,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 1 && capture.keys[0] == 1 &&
                   capture.aggregate_states[0] ==
                       PRDB_SDK_VALUE_STATE_NULL);
            reset_scan(&mock, &capture, 1);
            assert(run_query(&mock, max_sql, sizeof(max_sql) - 1, NULL, 0, 64,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 2 &&
                   capture.aggregate_states[0] == PRDB_SDK_VALUE_STATE_NULL &&
                   capture.aggregate_states[1] == PRDB_SDK_VALUE_STATE_VALUE &&
                   capture.aggregates[1] == 5);
            mock.batch_len = make_rows(mock.batch, 1, keys, scores,
                                       null_states, 3);
        }
        reset_scan(&mock, &capture, 1);
        assert(run_query(&mock, avg_sql, sizeof(avg_sql) - 1, NULL, 0, 64,
                         &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 &&
               capture.aggregate_states[0] == PRDB_SDK_VALUE_STATE_NULL &&
               capture.aggregate_states[1] == PRDB_SDK_VALUE_STATE_NULL);
        reset_scan(&mock, &capture, 1);
        assert(run_query(&mock, sum_sql, sizeof(sum_sql) - 1, NULL, 0, 64,
                         &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 &&
               capture.aggregate_states[0] == PRDB_SDK_VALUE_STATE_NULL &&
               capture.aggregate_states[1] == PRDB_SDK_VALUE_STATE_NULL);
    }

    {
        const uint8_t redacted_states[] = {
            PRDB_SDK_VALUE_STATE_VALUE,
            PRDB_SDK_VALUE_STATE_REDACTED,
            PRDB_SDK_VALUE_STATE_VALUE};
        mock.batch_len = make_rows(mock.batch, 0, keys, scores,
                                   redacted_states, 3);
        reset_scan(&mock, &capture, 0);
        assert(run_query(&mock, min_sql, sizeof(min_sql) - 1, NULL, 0, 64,
                         &capture) ==
               PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
        assert(capture.calls == 0 && mock.executor_calls == 4 &&
               mock.statement_failed == 1);
        reset_scan(&mock, &capture, 0);
        assert(run_query(&mock, max_sql, sizeof(max_sql) - 1, NULL, 0, 64,
                         &capture) ==
               PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
        assert(capture.calls == 0 && mock.executor_calls == 4 &&
               mock.statement_failed == 1);
        reset_scan(&mock, &capture, 0);
        assert(run_query(&mock, avg_sql, sizeof(avg_sql) - 1, NULL, 0, 64,
                         &capture) ==
               PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
        assert(capture.calls == 0 && mock.executor_calls == 4 &&
               mock.statement_failed == 1);
        reset_scan(&mock, &capture, 0);
        assert(run_query(&mock, sum_sql, sizeof(sum_sql) - 1, NULL, 0, 64,
                         &capture) ==
               PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
        assert(capture.calls == 0 && mock.executor_calls == 4 &&
               mock.statement_failed == 1);
    }

    mock.batch_len = make_rows(mock.batch, 0, keys, scores, value_states, 3);
    reset_scan(&mock, &capture, 0);
    assert(run_query(&mock, min_sql, sizeof(min_sql) - 1, NULL, 0, 4,
                     &capture) ==
           PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT);
    assert(capture.calls == 0 && mock.executor_calls == 4 &&
           mock.statement_failed == 1);

    reset_scan(&mock, &capture, 0);
    mock.cancelled = 1;
    assert(run_query(&mock, min_sql, sizeof(min_sql) - 1, NULL, 0, 64,
                     &capture) == PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    assert(capture.calls == 0 && mock.next_calls == 0 &&
           mock.executor_calls == 3 && mock.statement_failed == 1);

    mock.batch_len = make_rows(mock.batch, 0, keys, scores, value_states, 3);
    reset_scan(&mock, &capture, 0);
    mock.cancel_after_first_batch = 1;
    assert(run_query(&mock, min_sql, sizeof(min_sql) - 1, NULL, 0, 64,
                     &capture) == PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    assert(capture.calls == 0 && mock.next_calls == 1 &&
           mock.executor_calls == 4 && mock.statement_failed == 1);

    {
        /* Two all-NULL rows form (1,NULL); two values form (2,3.00). */
        const int32_t null_keys[] = {1, 1, 2, 2};
        const int64_t null_values[] = {0, 0, 300, 300};
        const uint8_t states[] = {PRDB_SDK_VALUE_STATE_NULL,
            PRDB_SDK_VALUE_STATE_NULL, PRDB_SDK_VALUE_STATE_VALUE,
            PRDB_SDK_VALUE_STATE_VALUE};
        mock.planner_len = make_decimal_relation(
            mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99, 10, 2);
        mock.executor_len = make_decimal_relation(mock.executor, 6, 0x22, 10, 2);
        mock.batch_len = make_decimal_rows(mock.batch, 1, null_keys,
                                           null_values, states, 4);
        const char *aggregates[] = {"SUM", "AVG", "MIN", "MAX"};
        for (uint32_t i = 0; i < 4; ++i) {
            char sql[512];
            snprintf(sql, sizeof(sql),
                "SELECT id, score, %s(score) FROM widgets GROUP BY id, score "
                "HAVING %s(score) IS NULL AND COUNT(*) IS NOT NULL "
                "AND COUNT(*) >= 2 ORDER BY id DESC LIMIT 1",
                aggregates[i], aggregates[i]);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v23_decimal(&mock, (const uint8_t *)sql,
                strlen(sql), NULL, 0, on_group_v23_null_test, &capture) ==
                PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 1 && capture.keys[0] == 1 &&
                   capture.aggregates[0] == -1 && !mock.statement_failed);
            snprintf(sql, sizeof(sql),
                "SELECT id, score, %s(score) FROM widgets GROUP BY id, score "
                "HAVING NOT %s(score) IS NULL AND COUNT(*) >= 2",
                aggregates[i], aggregates[i]);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v23_decimal(&mock, (const uint8_t *)sql,
                strlen(sql), NULL, 0, on_group_v23_null_test, &capture) ==
                PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 1 && capture.keys[0] == 2 &&
                   capture.aggregates[0] == 1 && !mock.statement_failed);
        }
        {
            prdb_sdk_value_v1 null_parameter = {
                sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
                PRDB_SDK_VALUE_STATE_NULL, 0, NULL, 0};
            const char *or_sql = "SELECT id, score, SUM(score) FROM widgets "
                "GROUP BY id, score HAVING SUM(score) IS NULL "
                "OR SUM(score) >= $1";
            reset_scan(&mock, &capture, 1);
            assert(run_query_v23_decimal(&mock, (const uint8_t *)or_sql,
                strlen(or_sql), &null_parameter, 1, on_group_v23_null_test,
                &capture) == PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 1 && capture.keys[0] == 1 &&
                   capture.aggregates[0] == -1 && !mock.statement_failed);
            const char *nonnull_sql = "SELECT id, score, SUM(score) FROM widgets "
                "GROUP BY id, score HAVING SUM(score) IS NOT NULL";
            reset_scan(&mock, &capture, 1);
            assert(run_query_v23_decimal(&mock, (const uint8_t *)nonnull_sql,
                strlen(nonnull_sql), NULL, 0, on_group_v23_null_test,
                &capture) == PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 1 && capture.keys[0] == 2 &&
                   capture.aggregates[0] == 1 && !mock.statement_failed);
        }
        const char *count_sql = "SELECT id, score, COUNT(*) FROM widgets "
            "GROUP BY id, score HAVING COUNT(*) IS NULL";
        reset_scan(&mock, &capture, 1);
        assert(run_query_v23_decimal(&mock, (const uint8_t *)count_sql,
            strlen(count_sql), NULL, 0, on_group_v23_null_test, &capture) ==
            PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 0 && !mock.statement_failed);
        const char *redacted_sql = "SELECT id, score, SUM(score) FROM widgets "
            "GROUP BY id, score HAVING SUM(score) IS NULL";
        const uint8_t denied[] = {PRDB_SDK_VALUE_STATE_NULL,
            PRDB_SDK_VALUE_STATE_REDACTED};
        mock.batch_len = make_decimal_rows(mock.batch, 1, null_keys,
                                           null_values, denied, 2);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v23_decimal(&mock, (const uint8_t *)redacted_sql,
            strlen(redacted_sql), NULL, 0, on_group_v23_null_test, &capture) ==
            PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
        assert(capture.calls == 0 && mock.statement_failed);
    }

    {
        const int64_t decimal_coefficients[] = {900, 300, 500};
        mock.planner_len = make_decimal_relation(
            mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99, 10, 2);
        mock.executor_len = make_decimal_relation(mock.executor, 6, 0x22,
                                                  10, 2);
        mock.batch_len = make_decimal_rows(mock.batch, 1, keys,
                                           decimal_coefficients, NULL, 3);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v22_decimal(
                   &mock, v22_hidden_count_sql,
                   sizeof(v22_hidden_count_sql) - 1, NULL, 0,
                   on_group_v21_decimal, &capture) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 0 && mock.statement_failed == 0);

        {
            const int32_t where_keys[] = {1, 1, 2};
            const int64_t where_coefficients[] = {300, 300, 500};
            mock.batch_len = make_decimal_rows(
                mock.batch, 1, where_keys, where_coefficients, NULL, 3);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v22_decimal(
                       &mock, v22_where_and_having_sql,
                       sizeof(v22_where_and_having_sql) - 1,
                       &v22_where_parameter, 1,
                       on_group_v22_where_and_having, &capture) ==
                   PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 1 && capture.keys[0] == 2 &&
                   capture.aggregates[0] == 5 && capture.row_counts[0] == 1 &&
                   mock.statement_failed == 0);
            mock.batch_len = make_decimal_rows(mock.batch, 1, keys,
                                               decimal_coefficients, NULL, 3);
        }

        {
            const int32_t avg_keys[] = {2, 2};
            const int64_t avg_coefficients[] = {300, 300};
            mock.batch_len = make_decimal_rows(mock.batch, 1, avg_keys,
                                               avg_coefficients, NULL, 2);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v22_decimal(
                       &mock, v22_hidden_count_avg_sql,
                       sizeof(v22_hidden_count_avg_sql) - 1, NULL, 0,
                       on_group_v22_hidden_count_avg, &capture) ==
                   PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 1 && capture.keys[0] == 2 &&
                   capture.aggregates[0] == 3 && mock.statement_failed == 0);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v22_decimal(
                       &mock, v22_hidden_count_min_sql,
                       sizeof(v22_hidden_count_min_sql) - 1, NULL, 0,
                       on_group_v22_hidden_count_minmax, &capture) ==
                   PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 1 && capture.keys[0] == 2 &&
                   capture.aggregates[0] == 3 && mock.statement_failed == 0);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v22_decimal(
                       &mock, v22_hidden_count_max_sql,
                       sizeof(v22_hidden_count_max_sql) - 1, NULL, 0,
                       on_group_v22_hidden_count_minmax, &capture) ==
                   PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 1 && capture.keys[0] == 2 &&
                   capture.aggregates[0] == 3 && mock.statement_failed == 0);
            mock.batch_len = make_decimal_rows(mock.batch, 1, keys,
                                               decimal_coefficients, NULL, 3);
        }

        {
            const int32_t null_keys[] = {1, 1};
            const int64_t null_coefficients[] = {0, 0};
            const uint8_t null_states[] = {PRDB_SDK_VALUE_STATE_NULL,
                                           PRDB_SDK_VALUE_STATE_NULL};
            mock.batch_len = make_decimal_rows(mock.batch, 1, null_keys,
                                               null_coefficients, null_states,
                                               2);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v22_decimal(
                       &mock, v22_null_count_sql,
                       sizeof(v22_null_count_sql) - 1, NULL, 0,
                       on_group_v22_null_sum, &capture) ==
                   PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 1 && capture.keys[0] == 1 &&
                   capture.aggregates[0] == -1 && mock.statement_failed == 0);
            mock.batch_len = make_decimal_rows(mock.batch, 1, keys,
                                               decimal_coefficients, NULL, 3);
        }

        {
            const uint8_t threshold_bytes[8] = {0, 0, 0, 0, 0, 0, 0, 2};
            const uint8_t decimal_bytes[16] = {0};
            prdb_sdk_value_v1 count_parameter = {
                sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
                PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BIGINT,
                threshold_bytes, sizeof(threshold_bytes)};
            prdb_sdk_value_v1 wrong_parameter = {
                sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
                PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_DECIMAL,
                decimal_bytes, sizeof(decimal_bytes)};
            reset_scan(&mock, &capture, 1);
            assert(run_query_v22_decimal(
                       &mock, v22_parameter_count_sql,
                       sizeof(v22_parameter_count_sql) - 1, &count_parameter,
                       1, on_group_v21_decimal, &capture) ==
                   PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 0 && mock.statement_failed == 0);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v22_decimal(
                       &mock, v22_parameter_count_sql,
                       sizeof(v22_parameter_count_sql) - 1, &wrong_parameter,
                       1, on_group_v21_decimal, &capture) ==
                   PRDB_SDK_SQL_EXEC_BIND_ERROR);
            assert(capture.calls == 0 && mock.next_calls == 0 &&
                   mock.statement_failed == 1);
        }

        reset_scan(&mock, &capture, 1);
        assert(run_query_v21_decimal(
                   &mock, v21_decimal_literal_sql,
                   sizeof(v21_decimal_literal_sql) - 1, &capture) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && capture.keys[0] == 1 &&
               capture.aggregates[0] == 9 && capture.keys[1] == 2 &&
               capture.aggregates[1] == 5 && mock.statement_failed == 0);

        {
            const int64_t exact_coefficients[] = {450, 449, 500};
            mock.batch_len = make_decimal_rows(mock.batch, 1, keys,
                                               exact_coefficients, NULL, 3);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v21_decimal(
                       &mock, v21_decimal_equal_literal_sql,
                       sizeof(v21_decimal_equal_literal_sql) - 1, &capture) ==
                   PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 1 && capture.keys[0] == 1 &&
                   capture.aggregates[0] == 4 && mock.statement_failed == 0);
        }

        {
            const int64_t average_coefficients[] = {150, 130, 250};
            mock.batch_len = make_decimal_rows(mock.batch, 1, keys,
                                               average_coefficients, NULL, 3);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v21_decimal(
                       &mock, v21_decimal_avg_literal_sql,
                       sizeof(v21_decimal_avg_literal_sql) - 1, &capture) ==
                   PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 2 && capture.keys[0] == 1 &&
                   capture.aggregates[0] == 150 && capture.keys[1] == 2 &&
                   capture.aggregates[1] == 250 && mock.statement_failed == 0);
        }

        reset_scan(&mock, &capture, 1);
        assert(run_query_v21_decimal(
                   &mock, v21_decimal_inexact_scale_sql,
                   sizeof(v21_decimal_inexact_scale_sql) - 1, &capture) ==
               PRDB_SDK_SQL_EXEC_BIND_ERROR);
        assert(capture.calls == 0 && mock.next_calls == 0 &&
               mock.statement_failed == 1);

        reset_scan(&mock, &capture, 1);
        assert(run_query_v21_decimal(
                   &mock, v21_decimal_overflow_literal_sql,
                   sizeof(v21_decimal_overflow_literal_sql) - 1, &capture) ==
               PRDB_SDK_SQL_EXEC_BIND_ERROR);
        assert(capture.calls == 0 && mock.next_calls == 0 &&
               mock.statement_failed == 1);

        reset_scan(&mock, &capture, 1);
        mock.cancelled = 1;
        assert(run_query_v21_decimal(
                   &mock, v21_decimal_literal_sql,
                   sizeof(v21_decimal_literal_sql) - 1, &capture) ==
               PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
        assert(capture.calls == 0 && mock.next_calls == 0 &&
               mock.statement_failed == 1);

        reset_scan(&mock, &capture, 1);
        mock.cancel_after_first_batch = 1;
        assert(run_query_v21_decimal(
                   &mock, v21_decimal_literal_sql,
                   sizeof(v21_decimal_literal_sql) - 1, &capture) ==
               PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
        assert(capture.calls == 0 && mock.next_calls == 1 &&
               mock.statement_failed == 1);

        {
            int32_t many_decimal_keys[40];
            int64_t many_decimal_coefficients[40];
            for (uint32_t index = 0; index < 40; ++index) {
                many_decimal_keys[index] = (int32_t)(index + 1);
                many_decimal_coefficients[index] = 900;
            }
            mock.batch_len = make_decimal_rows(
                mock.batch, 1, many_decimal_keys,
                many_decimal_coefficients, NULL, 40);
            mock.max_candidates = 40;
            reset_scan(&mock, &capture, 1);
            mock.cancel_after_group_callbacks = 32;
            assert(run_query_v21_decimal(
                       &mock, v21_decimal_literal_sql,
                       sizeof(v21_decimal_literal_sql) - 1, &capture) ==
                   PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
            assert(capture.calls == 32 && mock.cancelled == 1 &&
                   mock.statement_failed == 1);
            mock.cancel_after_group_callbacks = 0;
            mock.max_candidates = 0;
            mock.batch_len = make_decimal_rows(mock.batch, 1, keys,
                                               decimal_coefficients, NULL, 3);
        }

        mock.batch_len = make_decimal_rows(mock.batch, 1, keys,
                                           decimal_coefficients, NULL, 3);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v21_min_decimal(
                   &mock, v21_decimal_min_literal_sql,
                   sizeof(v21_decimal_min_literal_sql) - 1, &capture) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 1 && capture.keys[0] == 1 &&
               capture.aggregates[0] == 300 && mock.statement_failed == 0);

        reset_scan(&mock, &capture, 1);
        assert(run_query_v21_max_decimal(
                   &mock, v21_decimal_max_literal_sql,
                   sizeof(v21_decimal_max_literal_sql) - 1, &capture) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 1 && capture.keys[0] == 1 &&
               capture.aggregates[0] == 900 && mock.statement_failed == 0);

        {
            const uint8_t decimal_null_states[] = {
                PRDB_SDK_VALUE_STATE_NULL, PRDB_SDK_VALUE_STATE_NULL,
                PRDB_SDK_VALUE_STATE_NULL};
            mock.batch_len = make_decimal_rows(mock.batch, 1, keys,
                                               decimal_coefficients,
                                               decimal_null_states, 3);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v21_max_decimal(
                       &mock, v21_decimal_max_literal_sql,
                       sizeof(v21_decimal_max_literal_sql) - 1, &capture) ==
                   PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 0 && mock.statement_failed == 0);
        }

        {
            const uint8_t decimal_redacted_states[] = {
                PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_STATE_REDACTED,
                PRDB_SDK_VALUE_STATE_VALUE};
            mock.batch_len = make_decimal_rows(mock.batch, 1, keys,
                                               decimal_coefficients,
                                               decimal_redacted_states, 3);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v21_max_decimal(
                       &mock, v21_decimal_max_literal_sql,
                       sizeof(v21_decimal_max_literal_sql) - 1, &capture) ==
                   PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
            assert(capture.calls == 0 && mock.executor_calls > 0 &&
                   mock.statement_failed == 1);
        }

        {
            const int32_t overflow_keys[] = {1, 1};
            static const uint8_t max_decimal_coefficient[2][16] = {
                {0x4B, 0x3B, 0x4C, 0xA8, 0x5A, 0x86, 0xC4, 0x7A,
                 0x09, 0x8A, 0x22, 0x3F, 0xFF, 0xFF, 0xFF, 0xFF},
                {0x4B, 0x3B, 0x4C, 0xA8, 0x5A, 0x86, 0xC4, 0x7A,
                 0x09, 0x8A, 0x22, 0x3F, 0xFF, 0xFF, 0xFF, 0xFF}};
            mock.planner_len = make_decimal_relation(
                mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99, 38,
                2);
            mock.executor_len = make_decimal_relation(mock.executor, 6, 0x22,
                                                      38, 2);
            mock.batch_len = make_decimal_rows_bytes(
                mock.batch, 1, overflow_keys, max_decimal_coefficient, 2);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v21_decimal_wide_sum(
                       &mock, v21_decimal_sum_overflow_sql,
                       sizeof(v21_decimal_sum_overflow_sql) - 1, &capture) ==
                   PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
            assert(capture.calls == 0 && mock.executor_calls > 0 &&
                   mock.statement_failed == 1);
        }

        {
            const int32_t overflow_keys[] = {1, 1};
            const int64_t overflow_scores[] = {INT64_MAX, 1};
            mock.planner_len = make_bigint_relation(
                mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99);
            mock.executor_len = make_bigint_relation(mock.executor, 6, 0x22);
            mock.batch_len = make_bigint_rows(
                mock.batch, 1, overflow_keys, overflow_scores, 2);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v7_integer_sum(
                       &mock, bigint_sum_overflow_sql,
                       sizeof(bigint_sum_overflow_sql) - 1, &capture) ==
                   PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
            assert(capture.calls == 0 && mock.executor_calls > 0 &&
                   mock.statement_failed == 1);
        }

        {
            const int32_t overflow_keys[] = {1, 1};
            const int64_t overflow_scores[] = {INT64_MAX, INT64_MAX};
            mock.planner_len = make_bigint_relation(
                mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99);
            mock.executor_len = make_bigint_relation(mock.executor, 6, 0x22);
            mock.batch_len = make_bigint_rows(
                mock.batch, 1, overflow_keys, overflow_scores, 2);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v21_integer_sum(
                       &mock, v21_bigint_sum_overflow_sql,
                       sizeof(v21_bigint_sum_overflow_sql) - 1,
                       on_group_v21_integer_sum_unexpected, &capture) ==
                   PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
            assert(capture.calls == 0 && mock.executor_calls > 0 &&
                   mock.statement_failed == 1);
        }

        {
            const int32_t keys_with_filtered_group[] = {1, 1, 2};
            const int64_t scores_with_filtered_group[] = {3, 3, 6};
            mock.planner_len = make_bigint_relation(
                mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99);
            mock.executor_len = make_bigint_relation(mock.executor, 6, 0x22);
            mock.batch_len = make_bigint_rows(
                mock.batch, 1, keys_with_filtered_group,
                scores_with_filtered_group, 3);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v21_integer_sum(
                       &mock, v21_bigint_sum_literal_sql,
                       sizeof(v21_bigint_sum_literal_sql) - 1,
                       on_group_v21_integer_sum_literal, &capture) ==
                   PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 2 && mock.executor_calls > 0 &&
                   mock.statement_failed == 0);
        }

        {
            static const uint8_t count_column_sql[] =
                "SELECT id, COUNT(score) FROM widgets GROUP BY id "
                "HAVING COUNT(score) >= $1";
            const int32_t keys[] = {1, 1, 2, 3};
            const int32_t scores[] = {0, 9, 0, 0};
            const uint8_t states[] = {
                PRDB_SDK_VALUE_STATE_NULL, PRDB_SDK_VALUE_STATE_VALUE,
                PRDB_SDK_VALUE_STATE_NULL, PRDB_SDK_VALUE_STATE_NULL};
            mock.planner_len = make_relation(
                mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
                PRDB_SDK_VALUE_TYPE_INTEGER);
            mock.executor_len = make_relation(
                mock.executor, 6, 0x22, PRDB_SDK_VALUE_TYPE_INTEGER);
            mock.batch_len = make_rows(mock.batch, 1, keys, scores, states, 4);
            reset_scan(&mock, &capture, 1);
            int32_t count_column_status = run_query_v24_count_column(
                &mock, count_column_sql, sizeof(count_column_sql) - 1,
                &capture, PRDB_SDK_VALUE_TYPE_INTEGER,
                on_count_column_group);
            assert(count_column_status == PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 3 && mock.optimizer_enabled == 1 &&
                   mock.optimizer_relation_resolves == 1 &&
                   mock.statement_failed == 0);
            {
                const int32_t redacted_keys[] = {1};
                const int32_t redacted_scores[] = {0};
                const uint8_t redacted_states[] = {
                    PRDB_SDK_VALUE_STATE_REDACTED};
                mock.batch_len = make_rows(mock.batch, 1, redacted_keys,
                                           redacted_scores, redacted_states, 1);
                reset_scan(&mock, &capture, 1);
                assert(run_query_v24_count_column(
                           &mock, count_column_sql,
                           sizeof(count_column_sql) - 1, &capture,
                           PRDB_SDK_VALUE_TYPE_INTEGER,
                           on_count_column_group) ==
                       PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
                assert(capture.calls == 0 && mock.statement_failed == 1);
            }

            static const uint8_t boolean_count_sql[] =
                "SELECT id, COUNT(score) FROM widgets GROUP BY id "
                "HAVING COUNT(score) >= $1";
            mock.planner_len = make_relation_with_types(
                mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
                PRDB_SDK_VALUE_TYPE_INTEGER, PRDB_SDK_VALUE_TYPE_BOOLEAN);
            mock.executor_len = make_relation_with_types(
                mock.executor, 6, 0x22, PRDB_SDK_VALUE_TYPE_INTEGER,
                PRDB_SDK_VALUE_TYPE_BOOLEAN);
            mock.batch_len = make_boolean_count_rows(mock.batch);
            reset_scan(&mock, &capture, 1);
            int32_t boolean_status = run_query_v24_count_column(
                &mock, boolean_count_sql, sizeof(boolean_count_sql) - 1,
                &capture, PRDB_SDK_VALUE_TYPE_BOOLEAN,
                on_boolean_count_column_group);
            assert(boolean_status == PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 3 && mock.optimizer_enabled == 1 &&
                   mock.optimizer_relation_resolves == 1 &&
                   mock.statement_failed == 0);

            mock.planner_len = make_relation_with_types(
                mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
                PRDB_SDK_VALUE_TYPE_INTEGER, PRDB_SDK_VALUE_TYPE_TEXT);
            mock.executor_len = make_relation_with_types(
                mock.executor, 6, 0x22, PRDB_SDK_VALUE_TYPE_INTEGER,
                PRDB_SDK_VALUE_TYPE_TEXT);
            mock.batch_len = make_text_count_rows(mock.batch);
            reset_scan(&mock, &capture, 1);
            int32_t text_status = run_query_v24_count_column(
                &mock, boolean_count_sql, sizeof(boolean_count_sql) - 1,
                &capture, PRDB_SDK_VALUE_TYPE_TEXT,
                on_text_count_column_group);
            assert(text_status == PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 3 && mock.optimizer_enabled == 1 &&
                   mock.optimizer_relation_resolves == 1 &&
                   mock.statement_failed == 0);

            mock.planner_len = make_relation_with_types(
                mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
                PRDB_SDK_VALUE_TYPE_INTEGER, PRDB_SDK_VALUE_TYPE_SMALLINT);
            mock.executor_len = make_relation_with_types(
                mock.executor, 6, 0x22, PRDB_SDK_VALUE_TYPE_INTEGER,
                PRDB_SDK_VALUE_TYPE_SMALLINT);
            mock.batch_len = make_smallint_count_rows(mock.batch);
            reset_scan(&mock, &capture, 1);
            int32_t smallint_status = run_query_v24_count_column(
                &mock, boolean_count_sql, sizeof(boolean_count_sql) - 1,
                &capture, PRDB_SDK_VALUE_TYPE_SMALLINT,
                on_smallint_count_column_group);
            assert(smallint_status == PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 3 && mock.optimizer_enabled == 1 &&
                   mock.optimizer_relation_resolves == 1 &&
                   mock.statement_failed == 0);
        }
    }
    run_grouped_scalar_order_matrix(&mock);
    run_integral_aggregate_runner_matrix(&mock);
    return 0;
}
