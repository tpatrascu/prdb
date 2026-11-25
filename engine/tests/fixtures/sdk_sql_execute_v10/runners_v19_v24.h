static int32_t run_query_v21_decimal(mock_state *mock, const uint8_t *sql,
                                     uint64_t sql_len,
                                     group_capture *capture) {
    static const uint8_t schema[] = "app";
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[8192] = {0};
    uint8_t materialized_bytes[4096] = {0};
    prdb_sdk_sql_column_shape_v1 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 10, 2}};
    prdb_sdk_sql_column_shape_v1 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 10, 2}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v1(
        &host, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0,
        mock->max_candidates == 0 ? 4 : (uint16_t)mock->max_candidates, 8192,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes), on_group_v21_decimal,
        capture);
}

static int32_t run_query_v22_decimal(mock_state *mock, const uint8_t *sql,
                                     uint64_t sql_len,
                                     const prdb_sdk_value_v1 *parameters,
                                     uint32_t parameter_count,
                                     prdb_sdk_sql_group_row_v1_fn on_group,
                                     group_capture *capture) {
    static const uint8_t schema[] = "app";
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[8192] = {0};
    uint8_t materialized_bytes[4096] = {0};
    prdb_sdk_sql_column_shape_v1 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 10, 2}};
    prdb_sdk_sql_column_shape_v1 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 10, 2}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v1(
        &host, schema, sizeof(schema) - 1, sql, sql_len, parameters,
        parameter_count,
        mock->max_candidates == 0 ? 4 : (uint16_t)mock->max_candidates, 8192,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes), on_group, capture);
}

static int32_t on_group_v23_null_test(
    void *context, const prdb_sdk_value_v1 *keys, uint32_t key_count,
    const prdb_sdk_value_v1 *aggregate, uint64_t rows,
    const prdb_sdk_select_binding_v1 *binding) {
    group_capture *capture = context;
    assert(key_count == 2 && capture->mock->batch_finished);
    assert(binding != NULL && rows == 2);
    assert(prdb_sdk_value_get_integer_v1(&keys[0],
        &capture->keys[capture->calls]) == PRDB_SDK_VALUE_ACCESS_OK);
    capture->aggregates[capture->calls] = aggregate == NULL ? 99 :
        (aggregate->state == PRDB_SDK_VALUE_STATE_NULL ? -1 : 1);
    ++capture->calls;
    return 0;
}

static int32_t run_query_v23_decimal(mock_state *mock, const uint8_t *sql,
                                     uint64_t sql_len,
                                     const prdb_sdk_value_v1 *parameters,
                                     uint32_t parameter_count,
                                     prdb_sdk_sql_group_row_v1_fn on_group,
                                     group_capture *capture) {
    static const uint8_t schema[] = "app";
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[8192] = {0};
    uint8_t materialized_bytes[4096] = {0};
    prdb_sdk_sql_column_shape_v1 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 10, 2}};
    prdb_sdk_sql_column_shape_v1 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 10, 2}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v1(
        &host, schema, sizeof(schema) - 1, sql, sql_len, parameters,
        parameter_count,
        mock->max_candidates == 0 ? 4 : (uint16_t)mock->max_candidates, 8192,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes), on_group, capture);
}

static int32_t on_group_v24_optimizer_having(
    void *context, const prdb_sdk_value_v1 *keys, uint32_t key_count,
    const prdb_sdk_value_v1 *aggregate, uint64_t rows,
    const prdb_sdk_select_binding_v1 *binding) {
    group_capture *capture = context;
    int64_t aggregate_value = 0;
    assert(key_count == 2 && rows == 1 && binding != NULL);
    assert(binding->having_node_count >= 3);
    assert(capture->mock->batch_finished != 0);
    assert(prdb_sdk_value_get_integer_v1(
               &keys[0], &capture->keys[capture->calls]) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(prdb_sdk_value_get_integer_v1(
               &keys[1], &capture->second_keys[capture->calls]) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(prdb_sdk_value_get_bigint_v1(aggregate, &aggregate_value) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    capture->aggregates[capture->calls] = (int32_t)aggregate_value;
    ++capture->calls;
    return 0;
}

static int32_t on_group_v24_optimizer_single_having(
    void *context, const prdb_sdk_value_v1 *keys, uint32_t key_count,
    const prdb_sdk_value_v1 *aggregate, uint64_t rows,
    const prdb_sdk_select_binding_v1 *binding) {
    group_capture *capture = context;
    int64_t aggregate_value = 0;
    assert(key_count == 1 && rows != 0 && binding != NULL);
    assert(binding->having_node_count >= 3);
    assert(capture->mock->batch_finished != 0);
    assert(prdb_sdk_value_get_integer_v1(
               &keys[0], &capture->keys[capture->calls]) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(prdb_sdk_value_get_bigint_v1(aggregate, &aggregate_value) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    capture->aggregates[capture->calls] = (int32_t)aggregate_value;
    ++capture->calls;
    return 0;
}

static int32_t on_group_v24_optimizer_single_ordered_having(
    void *context, const prdb_sdk_value_v1 *keys, uint32_t key_count,
    const prdb_sdk_value_v1 *aggregate, uint64_t rows,
    const prdb_sdk_select_binding_v1 *binding) {
    group_capture *capture = context;
    const prdb_sdk_select_binding_v1 *ordered_binding;
    int64_t aggregate_value = 0;
    int32_t group_key = 0;
    assert(key_count == 1 &&
           rows == (capture->calls == 0 ? 1u : 2u) && binding != NULL);
    assert(binding->having_node_count >= 3);
    ordered_binding = binding;
    assert(ordered_binding->order_count == 2);
    assert(ordered_binding->order_items[0].item.kind ==
           PRDB_SDK_SQL_ORDER_AGGREGATE);
    assert(ordered_binding->order_items[0].item.direction ==
           PRDB_SDK_SQL_ORDER_DESCENDING);
    assert(ordered_binding->order_items[0].item.null_order ==
           PRDB_SDK_SQL_NULLS_LAST);
    assert(ordered_binding->order_items[1].item.kind ==
           PRDB_SDK_SQL_ORDER_GROUP_KEY);
    assert(ordered_binding->order_items[1].item.direction ==
           PRDB_SDK_SQL_ORDER_ASCENDING);
    assert(ordered_binding->order_items[1].item.null_order ==
           PRDB_SDK_SQL_NULLS_FIRST);
    assert(prdb_sdk_value_get_integer_v1(&keys[0], &group_key) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(prdb_sdk_value_get_bigint_v1(aggregate, &aggregate_value) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(group_key == (capture->calls == 0 ? 2 : 1));
    assert(aggregate_value == (capture->calls == 0 ? 3 : 2));
    capture->keys[capture->calls] = group_key;
    capture->aggregates[capture->calls] = (int32_t)aggregate_value;
    ++capture->calls;
    return 0;
}

static int32_t run_query_v24_optimizer_having(
    mock_state *mock, const uint8_t *sql, uint64_t sql_len,
    uint64_t arena_capacity, group_capture *capture,
    prdb_sdk_sql_group_row_v1_fn on_group) {
    static const uint8_t schema[] = "app";
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    uint8_t optimizer_request_bytes[4096] = {0};
    uint8_t optimizer_response[4096] = {0};
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[512] = {0};
    uint8_t batch_response[8192] = {0};
    uint8_t materialized_bytes[4096] = {0};
    prdb_sdk_sql_column_shape_v1 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_sql_column_shape_v1 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v5 host = {0};
    host.v4.v3 = (prdb_host_api_v3){
        sizeof(prdb_host_api_v3), PRDB_QUERY_ABI_V3_VERSION, mock,
        executor_request, planner_request, NULL, statement_failure};
    host.v4.optimizer_request = unused_optimizer_request;
    host.optimizer_request = optimizer_request;
    mock->optimizer_enabled = 0;
    mock->optimizer_aggregate_kind = PRDB_SDK_SQL_AGGREGATE_SUM;
    return prdb_sdk_sql_execute_group_having_optimizer_v1(
        &host, schema, sizeof(schema) - 1, sql, sql_len, NULL, NULL, 0,
        mock->max_candidates == 0 ? 4 : (uint16_t)mock->max_candidates,
        mock->max_candidates > 3 ? 8192 : 512,
        optimizer_request_bytes, sizeof(optimizer_request_bytes),
        optimizer_response, sizeof(optimizer_response), planner_response,
        sizeof(planner_response), executor_response, sizeof(executor_response),
        batch_response, sizeof(batch_response), planner_columns, 2,
        executor_columns, 2, cell_scratch, 2, materialized_bytes,
        arena_capacity, on_group, capture);
}

static int32_t run_query_v24_count_column(
    mock_state *mock, const uint8_t *sql, uint64_t sql_len,
    group_capture *capture, uint32_t input_type,
    prdb_sdk_sql_group_row_v1_fn on_group) {
    static const uint8_t schema[] = "app";
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    static const uint8_t zero_count_bytes[8] = {0};
    const prdb_sdk_value_v1 count_parameter = {
        sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BIGINT,
        zero_count_bytes, sizeof(zero_count_bytes)};
    const prdb_sdk_sql_parameter_metadata_v1 count_metadata = {
        sizeof(count_metadata), PRDB_SDK_SQL_PARAMETER_METADATA_ABI_V1_VERSION,
        PRDB_SDK_VALUE_TYPE_BIGINT, 0, 0, {0, 0}};
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[512] = {0};
    uint8_t batch_response[8192] = {0};
    uint8_t optimizer_request_bytes[4096] = {0};
    uint8_t optimizer_response[4096] = {0};
    uint8_t materialized_bytes[4096] = {0};
    prdb_sdk_sql_column_shape_v1 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, input_type, 0, 0}};
    prdb_sdk_sql_column_shape_v1 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, input_type, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v5 host = {0};
    host.v4.v3 = (prdb_host_api_v3){
        sizeof(prdb_host_api_v3), PRDB_QUERY_ABI_V3_VERSION, mock,
        executor_request, planner_request, NULL, statement_failure};
    host.v4.optimizer_request = unused_optimizer_request;
    host.optimizer_request = optimizer_request;
    mock->optimizer_enabled = 1;
    mock->optimizer_aggregate_kind =
        PRDB_SDK_SQL_AGGREGATE_COUNT_COLUMN;
    mock->optimizer_aggregate_type = input_type;
    return prdb_sdk_sql_execute_group_having_optimizer_v1(
        &host, schema, sizeof(schema) - 1, sql, sql_len, &count_parameter,
        &count_metadata, 1,
        mock->max_candidates == 0 ? 4 : (uint16_t)mock->max_candidates,
        mock->max_candidates > 3 ? 8192 : 512, optimizer_request_bytes,
        sizeof(optimizer_request_bytes), optimizer_response,
        sizeof(optimizer_response), planner_response,
        sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes), on_group, capture);
}

static int32_t on_boolean_count_column_group(
    void *opaque, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v1 *projection_binding) {
    group_capture *capture = opaque;
    int32_t key = 0;
    int64_t count = -1;
    static const prdb_sdk_sql_column_shape_v1 columns[] = {
        {(const uint8_t *)"id", 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {(const uint8_t *)"score", 5, 1, PRDB_SDK_VALUE_TYPE_BOOLEAN, 0, 0}};
    uint8_t sqlr[256] = {0};
    uint64_t sqlr_len = 0;
    assert(group_key_count == 1 && aggregate_value != NULL);
    assert(projection_binding != NULL &&
           projection_binding->aggregate_kind ==
               PRDB_SDK_SQL_AGGREGATE_COUNT_COLUMN &&
           projection_binding->aggregate_column
                   .type_code == PRDB_SDK_VALUE_TYPE_BOOLEAN);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &key) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(prdb_sdk_value_get_bigint_v1(aggregate_value, &count) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(capture->calls < 3);
    assert(key == (int32_t)capture->calls + 1);
    /* FALSE is a VALUE and contributes to COUNT(column). */
    assert(count == (capture->calls == 0 ? 2 : 0));
    assert(matching_row_count == (capture->calls == 0 ? 2 : 1));
    assert(prdb_sdk_sql_encode_group_composite_result_v1(
               projection_binding,
               columns, 2, group_keys, 1, aggregate_value,
               matching_row_count, sqlr, sizeof(sqlr), &sqlr_len) ==
           PRDB_SDK_SQL_RESULT_OK);
    assert(sqlr_len != 0);
    ++capture->calls;
    return 0;
}

static int32_t on_smallint_count_column_group(
    void *opaque, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v1 *projection_binding) {
    group_capture *capture = opaque;
    int32_t key = 0;
    int64_t count = -1;
    static const prdb_sdk_sql_column_shape_v1 columns[] = {
        {(const uint8_t *)"id", 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {(const uint8_t *)"score", 5, 1, PRDB_SDK_VALUE_TYPE_SMALLINT, 0, 0}};
    uint8_t sqlr[256] = {0};
    uint64_t sqlr_len = 0;
    assert(group_key_count == 1 && aggregate_value != NULL);
    assert(projection_binding != NULL &&
           projection_binding->aggregate_kind ==
               PRDB_SDK_SQL_AGGREGATE_COUNT_COLUMN &&
           projection_binding->aggregate_column
                   .type_code == PRDB_SDK_VALUE_TYPE_SMALLINT);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &key) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(prdb_sdk_value_get_bigint_v1(aggregate_value, &count) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(capture->calls < 3);
    assert(key == (int32_t)capture->calls + 1);
    assert(count == (capture->calls == 0 ? 2 : capture->calls == 2 ? 1 : 0));
    assert(matching_row_count == (capture->calls == 0 ? 2 : 1));
    assert(prdb_sdk_sql_encode_group_composite_result_v1(
               projection_binding,
               columns, 2, group_keys, 1, aggregate_value,
               matching_row_count, sqlr, sizeof(sqlr), &sqlr_len) ==
           PRDB_SDK_SQL_RESULT_OK);
    assert(sqlr_len != 0);
    ++capture->calls;
    return 0;
}

static int32_t on_text_count_column_group(
    void *opaque, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v1 *projection_binding) {
    group_capture *capture = opaque;
    int32_t key = 0;
    int64_t count = -1;
    static const prdb_sdk_sql_column_shape_v1 columns[] = {
        {(const uint8_t *)"id", 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {(const uint8_t *)"score", 5, 1, PRDB_SDK_VALUE_TYPE_TEXT, 0, 0}};
    uint8_t sqlr[256] = {0};
    uint64_t sqlr_len = 0;
    assert(group_key_count == 1 && aggregate_value != NULL);
    assert(projection_binding != NULL &&
           projection_binding->aggregate_kind ==
               PRDB_SDK_SQL_AGGREGATE_COUNT_COLUMN &&
           projection_binding->aggregate_column
                   .type_code == PRDB_SDK_VALUE_TYPE_TEXT);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &key) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(prdb_sdk_value_get_bigint_v1(aggregate_value, &count) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(capture->calls < 3);
    assert(key == (int32_t)capture->calls + 1);
    assert(count == (capture->calls == 0 ? 2 : capture->calls == 2 ? 1 : 0));
    assert(matching_row_count == (capture->calls == 0 ? 2 : 1));
    assert(prdb_sdk_sql_encode_group_composite_result_v1(
               projection_binding,
               columns, 2, group_keys, 1, aggregate_value,
               matching_row_count, sqlr, sizeof(sqlr), &sqlr_len) ==
           PRDB_SDK_SQL_RESULT_OK);
    assert(sqlr_len != 0);
    ++capture->calls;
    return 0;
}

static int32_t on_count_column_group(
    void *opaque, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v1 *projection_binding) {
    group_capture *capture = (group_capture *)opaque;
    int32_t key = 0;
    int64_t count = -1;
    (void)projection_binding;
    assert(group_key_count == 1 && aggregate_value != NULL);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &key) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(prdb_sdk_value_get_bigint_v1(aggregate_value, &count) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(capture->calls < 3);
    assert(key == (int64_t)capture->calls + 1);
    assert(count == (capture->calls == 0 ? 1 : 0));
    assert(matching_row_count == (capture->calls == 0 ? 2 : 1));
    {
        static const prdb_sdk_sql_column_shape_v1 columns[] = {
            {(const uint8_t *)"id", 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
            {(const uint8_t *)"score", 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
        uint8_t sqlr[256] = {0};
        uint64_t sqlr_len = 0;
        const prdb_sdk_select_binding_v1 *binding =
            projection_binding;
        assert(prdb_sdk_sql_encode_group_composite_result_v1(
                   binding, columns, 2, group_keys, 1, aggregate_value,
                   matching_row_count, sqlr, sizeof(sqlr), &sqlr_len) ==
               PRDB_SDK_SQL_RESULT_OK);
        assert(sqlr_len != 0);
    }
    ++capture->calls;
    return 0;
}

static int32_t run_query_v21_decimal_wide_sum(
    mock_state *mock, const uint8_t *sql, uint64_t sql_len,
    group_capture *capture) {
    static const uint8_t schema[] = "app";
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[2048] = {0};
    prdb_sdk_sql_column_shape_v1 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 38, 2}};
    prdb_sdk_sql_column_shape_v1 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 38, 2}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v1(
        &host, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0, 4, 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes), on_group_v21_decimal,
        capture);
}

static int32_t on_group_v7_integer_sum_unexpected(
    void *context, const prdb_sdk_value_v1 *group_key, uint32_t group_key_count,
    const prdb_sdk_value_v1 *aggregate_value, uint64_t matching_row_count,
    const prdb_sdk_select_binding_v1 *binding) {
    (void)group_key_count;
    (void)context;
    (void)group_key;
    (void)aggregate_value;
    (void)matching_row_count;
    (void)binding;
    assert(!"overflowing BIGINT SUM must not publish a group");
    return 1;
}

static int32_t run_query_v7_integer_sum(
    mock_state *mock, const uint8_t *sql, uint64_t sql_len,
    group_capture *capture) {
    static const uint8_t schema[] = "app";
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[2048] = {0};
    prdb_sdk_sql_column_shape_v1 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_BIGINT, 0, 0}};
    prdb_sdk_sql_column_shape_v1 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_BIGINT, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v1(
        &host, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0, 4, 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes),
        on_group_v7_integer_sum_unexpected, capture);
}

static int32_t on_group_v21_integer_sum_unexpected(
    void *context, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v1 *binding) {
    (void)context;
    (void)group_keys;
    (void)group_key_count;
    (void)aggregate_value;
    (void)matching_row_count;
    (void)binding;
    assert(!"overflowing BIGINT SUM must not publish a group");
    return 1;
}

static int32_t on_group_v21_integer_sum_literal(
    void *context, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v1 *binding) {
    group_capture *capture = (group_capture *)context;
    int32_t id = 0;
    int64_t score = 0;
    int64_t sum = 0;
    uint32_t index = capture->calls;
    assert(index < 2 && binding->abi_version == PRDB_SDK_SQL_ABI_V1_VERSION);
    assert(binding->having_type_code == PRDB_SDK_VALUE_TYPE_BIGINT &&
           binding->having_node_count == 1 &&
           binding->having_nodes[0].operand_kind ==
               PRDB_SDK_SQL_HAVING_OPERAND_NUMERIC_LITERAL &&
           binding->having_nodes[0].coefficient[15] == 6 &&
           binding->having_nodes[0].scale == 0);
    assert(group_key_count == 2 &&
           prdb_sdk_value_get_integer_v1(&group_keys[0], &id) ==
               PRDB_SDK_VALUE_ACCESS_OK &&
           prdb_sdk_value_get_bigint_v1(&group_keys[1], &score) ==
               PRDB_SDK_VALUE_ACCESS_OK);
    assert(id == (index == 0 ? 1 : 2) &&
           score == (index == 0 ? 3 : 6));
    assert(matching_row_count == (index == 0 ? 2 : 1) &&
           aggregate_value != NULL &&
           aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_BIGINT &&
           prdb_sdk_value_get_bigint_v1(aggregate_value, &sum) ==
               PRDB_SDK_VALUE_ACCESS_OK &&
           sum == 6);
    ++capture->calls;
    return 0;
}

static int32_t run_query_v21_integer_sum(
    mock_state *mock, const uint8_t *sql, uint64_t sql_len,
    prdb_sdk_sql_group_row_v1_fn on_group, void *capture) {
    static const uint8_t schema[] = "app";
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[2048] = {0};
    prdb_sdk_sql_column_shape_v1 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_BIGINT, 0, 0}};
    prdb_sdk_sql_column_shape_v1 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_BIGINT, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v1(
        &host, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0, 4, 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes),
        on_group, capture);
}

static int32_t run_query_v21_min_decimal(mock_state *mock, const uint8_t *sql,
                                         uint64_t sql_len,
                                         group_capture *capture) {
    static const uint8_t schema[] = "app";
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[2048] = {0};
    prdb_sdk_sql_column_shape_v1 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 10, 2}};
    prdb_sdk_sql_column_shape_v1 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 10, 2}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v1(
        &host, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0, 4, 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes),
        on_group_v21_min_decimal, capture);
}

static int32_t run_query_v21_max_decimal(mock_state *mock, const uint8_t *sql,
                                         uint64_t sql_len,
                                         group_capture *capture) {
    static const uint8_t schema[] = "app";
    static const uint8_t name[] = "id";
    static const uint8_t score_name[] = "score";
    prdb_sdk_sql_column_shape_v1 planner_columns[2] = {
        {name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 4, 2}};
    prdb_sdk_sql_column_shape_v1 executor_columns[2] = {
        {name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 4, 2}};
    uint8_t planner_response[512];
    uint8_t executor_response[512];
    uint8_t batch_response[512];
    uint8_t materialized_bytes[4096];
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v1(
        &host, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0, 4, 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes),
        on_group_v21_max_decimal, capture);
}

static void reset_scan(mock_state *mock, group_capture *capture,
                       uint32_t finished) {
    mock->executor_calls = 0;
    mock->optimizer_relation_resolves = 0;
    mock->executor_requests = 0;
    mock->next_calls = 0;
    mock->statement_failed = 0;
    mock->cancelled = 0;
    mock->cancel_after_first_batch = 0;
    mock->batch_finished = finished;
    capture->calls = 0;
}

