static int32_t run_query(mock_state *mock, const uint8_t *sql,
                         uint64_t sql_len,
                         const prdb_sdk_value_v1 *parameters,
                         uint32_t parameter_count, uint64_t arena_capacity,
                         group_capture *capture) {
    static const uint8_t schema[] = "app";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[64] = {0};
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v10(
        &host, schema, sizeof(schema) - 1, sql, sql_len, parameters,
        parameter_count, 4, 512, planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 2, executor_columns, 2,
        cell_scratch, 2, materialized_bytes, arena_capacity, on_group, capture);
}

static int32_t on_group_v11(void *context,
                            const prdb_sdk_value_v1 *group_key,
                            const prdb_sdk_value_v1 *aggregate_value,
                            uint64_t matching_row_count,
                            const prdb_sdk_select_binding_v11 *binding) {
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V11_VERSION);
    return on_group(context, group_key, aggregate_value, matching_row_count,
                    &binding->base);
}

static int32_t run_query_v11(mock_state *mock, const uint8_t *sql,
                             uint64_t sql_len, uint64_t arena_capacity,
                             group_capture *capture) {
    static const uint8_t schema[] = "app";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[64] = {0};
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v11(
        &host, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0, 4, 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, arena_capacity, on_group_v11, capture);
}

static uint32_t make_expected_group_count_sqlr(
    uint8_t *output, uint32_t key_type, uint8_t decimal_precision,
    uint8_t decimal_scale, const uint8_t *key_bytes, uint32_t key_len) {
    uint32_t offset = 0;
    memcpy(output + offset, "SQLR", 4);
    offset += 4;
    put_u16(output, &offset, 1);
    put_u16(output, &offset, 2);
    put_name(output, &offset, "id");
    put_u32(output, &offset, key_type);
    output[offset++] = decimal_precision;
    output[offset++] = decimal_scale;
    output[offset++] = PRDB_SDK_VALUE_STATE_VALUE;
    put_u32(output, &offset, key_len);
    memcpy(output + offset, key_bytes, key_len);
    offset += key_len;
    put_name(output, &offset, "count");
    put_u32(output, &offset, PRDB_SDK_VALUE_TYPE_BIGINT);
    output[offset++] = 0;
    output[offset++] = 0;
    output[offset++] = PRDB_SDK_VALUE_STATE_VALUE;
    put_u32(output, &offset, 8);
    memset(output + offset, 0, 7);
    output[offset + 7] = 1;
    offset += 8;
    return offset;
}

static uint32_t make_expected_group_sum_sqlr(
    uint8_t *output, const uint8_t *group_key, uint32_t group_key_len,
    uint64_t sum) {
    uint32_t offset = 0;
    memcpy(output + offset, "SQLR", 4);
    offset += 4;
    put_u16(output, &offset, 1);
    put_u16(output, &offset, 2);
    put_name(output, &offset, "group_key");
    put_u32(output, &offset, PRDB_SDK_VALUE_TYPE_TEXT);
    output[offset++] = 0;
    output[offset++] = 0;
    output[offset++] = PRDB_SDK_VALUE_STATE_VALUE;
    put_u32(output, &offset, group_key_len);
    memcpy(output + offset, group_key, group_key_len);
    offset += group_key_len;
    put_name(output, &offset, "sum");
    put_u32(output, &offset, PRDB_SDK_VALUE_TYPE_BIGINT);
    output[offset++] = 0;
    output[offset++] = 0;
    output[offset++] = PRDB_SDK_VALUE_STATE_VALUE;
    put_u32(output, &offset, 8);
    for (uint32_t byte = 0; byte < 8; ++byte) {
        output[offset + 7 - byte] = (uint8_t)(sum >> (byte * 8));
    }
    return offset + 8;
}

static int32_t on_group_v12(void *context,
                            const prdb_sdk_value_v1 *group_key,
                            const prdb_sdk_value_v1 *aggregate_value,
                            uint64_t matching_row_count,
                            const prdb_sdk_select_binding_v12 *binding) {
    group_capture *capture = (group_capture *)context;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V12_VERSION);
    assert(binding->order_count <= PRDB_SDK_SQL_V12_MAX_ORDER_KEYS);
    if (capture->scalar_order_mode) {
        uint32_t row = capture->calls;
        static const uint8_t id_name[] = "id";
        prdb_sdk_sql_column_shape_v2 group_column = {
            id_name, 2, 0, capture->scalar_order_type,
            capture->scalar_order_precision, capture->scalar_order_scale};
        uint8_t expected[128] = {0};
        uint32_t expected_len;
        assert(row < 3 && group_key != NULL && aggregate_value == NULL &&
               matching_row_count == 1);
        assert(binding->order_count == 1 &&
               binding->order_items[0].item.kind ==
                   PRDB_SDK_SQL_V10_ORDER_GROUP_KEY &&
               binding->order_items[0].item.direction ==
                   (capture->scalar_order_descending
                        ? PRDB_SDK_SQL_V10_ORDER_DESCENDING
                        : PRDB_SDK_SQL_V10_ORDER_ASCENDING) &&
               binding->order_items[0].type_code ==
                   capture->scalar_order_type);
        assert(group_key->state == PRDB_SDK_VALUE_STATE_VALUE &&
               group_key->type_code == capture->scalar_order_type &&
               group_key->data_len == capture->scalar_order_expected_len[row]);
        assert(memcmp(group_key->data, capture->scalar_order_expected[row],
                      group_key->data_len) == 0);
        expected_len = make_expected_group_count_sqlr(
            expected, capture->scalar_order_type,
            capture->scalar_order_precision, capture->scalar_order_scale,
            capture->scalar_order_expected[row],
            capture->scalar_order_expected_len[row]);
        assert(prdb_sdk_sql_encode_group_count_result_v1(
                   &group_column, group_key, matching_row_count,
                   capture->scalar_order_sqlr[row],
                   sizeof(capture->scalar_order_sqlr[row]),
                   &capture->scalar_order_sqlr_len[row]) ==
               PRDB_SDK_SQL_RESULT_OK);
        assert(capture->scalar_order_sqlr_len[row] == expected_len &&
               memcmp(capture->scalar_order_sqlr[row], expected,
                      expected_len) == 0);
        ++capture->calls;
        return 0;
    }
    if (binding->order_count == 2) {
        uint32_t expected_aggregate_type = PRDB_SDK_VALUE_TYPE_INTEGER;
        if (binding->base.base.base.base.aggregate_kind ==
                PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR ||
            binding->base.base.base.base.aggregate_kind ==
                PRDB_SDK_SQL_V7_AGGREGATE_SUM) {
            expected_aggregate_type = PRDB_SDK_VALUE_TYPE_BIGINT;
        } else if (binding->base.base.base.base.aggregate_kind ==
                   PRDB_SDK_SQL_V7_AGGREGATE_AVG) {
            expected_aggregate_type = PRDB_SDK_VALUE_TYPE_DECIMAL;
        }
        assert(binding->order_items[0].type_code == expected_aggregate_type);
        assert(binding->order_items[1].type_code ==
               (capture->mock->text_key_mode ? PRDB_SDK_VALUE_TYPE_TEXT
                                             : PRDB_SDK_VALUE_TYPE_INTEGER));
    }
    if (capture->mock->text_key_mode) {
        uint32_t key_index = 0;
        assert(group_key != NULL &&
               group_key->state == PRDB_SDK_VALUE_STATE_VALUE &&
               group_key->type_code == PRDB_SDK_VALUE_TYPE_TEXT &&
               group_key->data_len == capture->text_key_bytes);
        for (uint32_t index = group_key->data_len - 3;
             index < group_key->data_len; ++index) {
            assert(group_key->data[index] >= '0' &&
                   group_key->data[index] <= '9');
            key_index = key_index * 10 +
                        (uint32_t)(group_key->data[index] - '0');
        }
        assert(capture->calls < 128 && key_index < 128 &&
               matching_row_count == (capture->text_sum_tie_order_mode ? 2 : 1));
        if (capture->text_sum_tie_order_mode) {
            uint32_t expected_key = 62 - 2 * (capture->calls / 2) +
                                    capture->calls % 2;
            uint32_t sum = 300 + (key_index / 2) * 2;
            int64_t actual_sum = 0;
            uint8_t actual_sqlr[16384] = {0};
            uint8_t expected_sqlr[16384] = {0};
            uint64_t actual_sqlr_len = 0;
            uint32_t expected_sqlr_len;
            static const uint8_t group_name[] = "group_key";
            static const uint8_t measure_name[] = "measure";
            const prdb_sdk_sql_column_shape_v2 group_column = {
                group_name, sizeof(group_name) - 1, 0,
                PRDB_SDK_VALUE_TYPE_TEXT, 0, 0};
            const prdb_sdk_sql_column_shape_v2 measure_column = {
                measure_name, sizeof(measure_name) - 1, 1,
                PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0};
            assert(binding->base.base.base.base.aggregate_kind ==
                   PRDB_SDK_SQL_V7_AGGREGATE_SUM);
            assert(binding->base.base.base.base.aggregate_column.type_code ==
                   PRDB_SDK_VALUE_TYPE_INTEGER);
            assert(binding->order_count == 2);
            assert(binding->order_items[0].item.kind ==
                   PRDB_SDK_SQL_V10_ORDER_AGGREGATE);
            assert(binding->order_items[0].item.direction ==
                   PRDB_SDK_SQL_V10_ORDER_DESCENDING);
            assert(binding->order_items[0].item.null_order ==
                   PRDB_SDK_SQL_V11_NULLS_LAST);
            assert(binding->order_items[0].type_code ==
                   PRDB_SDK_VALUE_TYPE_BIGINT);
            assert(binding->order_items[1].item.kind ==
                   PRDB_SDK_SQL_V10_ORDER_GROUP_KEY);
            assert(binding->order_items[1].item.direction ==
                   PRDB_SDK_SQL_V10_ORDER_ASCENDING);
            assert(binding->order_items[1].item.null_order ==
                   PRDB_SDK_SQL_V11_NULLS_FIRST);
            assert(binding->order_items[1].type_code ==
                   PRDB_SDK_VALUE_TYPE_TEXT);
            assert(key_index == expected_key);
            assert(aggregate_value != NULL &&
                   aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_BIGINT &&
                   prdb_sdk_value_get_bigint_v1(aggregate_value, &actual_sum) ==
                       PRDB_SDK_VALUE_ACCESS_OK &&
                   actual_sum == (int64_t)sum);
            for (uint32_t index = 0; index < group_key->data_len - 3; ++index) {
                assert(group_key->data[index] == 'a');
            }
            expected_sqlr_len = make_expected_group_sum_sqlr(
                expected_sqlr, group_key->data, group_key->data_len, sum);
            assert(prdb_sdk_sql_encode_group_sum_result_v1(
                       &group_column, group_key, &measure_column,
                       aggregate_value, actual_sqlr, sizeof(actual_sqlr),
                       &actual_sqlr_len) == PRDB_SDK_SQL_RESULT_OK);
            assert(actual_sqlr_len == expected_sqlr_len &&
                   memcmp(actual_sqlr, expected_sqlr, expected_sqlr_len) == 0);
            capture->aggregates[capture->calls] = (int32_t)actual_sum;
        } else {
            capture->aggregates[capture->calls] = 1;
        }
        capture->keys[capture->calls] = (int32_t)key_index;
        capture->row_counts[capture->calls] = matching_row_count;
        ++capture->calls;
        if (capture->mock->cancel_after_group_callbacks == capture->calls) {
            capture->mock->cancelled = 1;
        }
        return 0;
    }
    return on_group(context, group_key, aggregate_value, matching_row_count,
                    &binding->base.base);
}

static int32_t run_query_v12(mock_state *mock, const uint8_t *sql,
                             uint64_t sql_len, uint64_t arena_capacity,
                             group_capture *capture) {
    static const uint8_t schema[] = "app";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[540000] = {0};
    uint8_t materialized_bytes[1048576] = {0};
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    uint32_t key_type = capture->scalar_order_mode
                            ? capture->scalar_order_type
                            : mock->text_key_mode ? PRDB_SDK_VALUE_TYPE_TEXT
                                                  : PRDB_SDK_VALUE_TYPE_INTEGER;
    uint8_t key_precision = capture->scalar_order_mode
                                ? capture->scalar_order_precision
                                : 0;
    uint8_t key_scale = capture->scalar_order_mode
                            ? capture->scalar_order_scale
                            : 0;
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, key_type, key_precision, key_scale},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, key_type, key_precision, key_scale},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v12(
        &host, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0,
        mock->max_candidates == 0 ? 4 : (uint16_t)mock->max_candidates,
        sizeof(batch_response),
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, arena_capacity, on_group_v12, capture);
}

typedef struct scalar_order_case {
    uint32_t type;
    uint8_t precision;
    uint8_t scale;
    uint32_t row_count;
    const uint8_t *lower;
    uint32_t lower_len;
    const uint8_t *middle;
    uint32_t middle_len;
    const uint8_t *higher;
    uint32_t higher_len;
} scalar_order_case;

typedef struct scalar_minmax_capture {
    mock_state *mock;
    uint32_t type;
    uint8_t precision;
    uint8_t scale;
    uint32_t aggregate_kind;
    const uint8_t *expected[2];
    uint32_t expected_len[2];
    uint32_t calls;
} scalar_minmax_capture;

static uint32_t make_typed_score_relation(uint8_t *p, uint8_t handle_kind,
                                          uint8_t handle_fill,
                                          const scalar_order_case *test) {
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    uint32_t offset = 0;
    put_u16(p, &offset, 1);
    p[offset++] = 0;
    memset(p + offset, 0x11, 16); offset += 16;
    p[offset++] = handle_kind;
    memset(p + offset, handle_fill, 15); offset += 15;
    put_name(p, &offset, "widgets");
    put_u16(p, &offset, 2);
    for (uint32_t index = 0; index < 2; ++index) {
        memset(p + offset, (int)(0x33 + index), 16); offset += 16;
        put_u32(p, &offset, index);
        p[offset++] = 1;
        p[offset++] = index == 0 ? PRDB_SDK_VALUE_TYPE_INTEGER
                                 : (uint8_t)test->type;
        if (index == 1 && test->type == PRDB_SDK_VALUE_TYPE_DECIMAL) {
            p[offset++] = test->precision;
            p[offset++] = test->scale;
            p[offset++] = 0;
            p[offset++] = 0;
        } else if (index == 1 && test->type == PRDB_SDK_VALUE_TYPE_VARCHAR) {
            put_u32(p, &offset, 128);
        } else {
            p[offset++] = 0;
            p[offset++] = 0;
            p[offset++] = 0;
            p[offset++] = 0;
        }
        put_name(p, &offset, index == 0 ? (const char *)id_name
                                        : (const char *)score_name);
    }
    return offset;
}

static uint32_t make_typed_score_rows(
    uint8_t *p, const scalar_order_case *test,
    const uint8_t *const scores[3], const uint32_t score_lengths[3]) {
    static const int32_t keys[3] = {1, 1, 2};
    uint32_t offset = 0;
    put_u16(p, &offset, 1);
    p[offset++] = 1;
    put_u16(p, &offset, 3);
    for (uint32_t index = 0; index < 3; ++index) {
        p[offset++] = 8;
        memset(p + offset, (int)(index + 1), 15); offset += 15;
        put_u16(p, &offset, 2);
        append_integer_cell(p, &offset, keys[index]);
        append_typed_cell(p, &offset, test->type, scores[index],
                          score_lengths[index]);
    }
    return offset;
}

static int32_t on_group_scalar_minmax(
    void *context, const prdb_sdk_value_v1 *group_key,
    const prdb_sdk_value_v1 *aggregate_value, uint64_t matching_row_count,
    const prdb_sdk_select_binding_v5 *binding) {
    scalar_minmax_capture *capture = (scalar_minmax_capture *)context;
    int32_t key = 0;
    uint32_t index = capture->calls;
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    prdb_sdk_sql_column_shape_v2 group_column = {
        id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0};
    prdb_sdk_sql_column_shape_v2 aggregate_column = {
        score_name, 5, 1, capture->type, capture->precision, capture->scale};
    uint8_t encoded[128] = {0};
    uint64_t encoded_len = 0;
    assert(index < 2 && capture->mock->batch_finished != 0);
    assert(binding->aggregate_kind == capture->aggregate_kind);
    assert(binding->aggregate_column.type_code == capture->type &&
           binding->aggregate_column.decimal_precision == capture->precision &&
           binding->aggregate_column.decimal_scale == capture->scale);
    assert(prdb_sdk_value_get_integer_v1(group_key, &key) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(key == (index == 0 ? 1 : 2));
    assert(matching_row_count == (index == 0 ? 2 : 1));
    assert(aggregate_value != NULL &&
           aggregate_value->state == PRDB_SDK_VALUE_STATE_VALUE &&
           aggregate_value->type_code == capture->type &&
           aggregate_value->data_len == capture->expected_len[index]);
    assert(memcmp(aggregate_value->data, capture->expected[index],
                  capture->expected_len[index]) == 0);
    assert(prdb_sdk_sql_encode_group_minmax_result_v1(
               &group_column, group_key, &aggregate_column, aggregate_value,
               capture->aggregate_kind, encoded, sizeof(encoded),
               &encoded_len) == PRDB_SDK_SQL_RESULT_OK);
    assert(encoded_len == 43 + capture->expected_len[index] &&
           memcmp(encoded, "SQLR\0\1\0\2", 8) == 0 &&
           encoded[27] == 0 && encoded[28] == 3 &&
           memcmp(encoded + 29,
                  capture->aggregate_kind == PRDB_SDK_SQL_V5_AGGREGATE_MIN
                      ? "min"
                      : "max",
                  3) == 0 &&
           encoded[32] == 0 && encoded[33] == 0 && encoded[34] == 0 &&
           encoded[35] == capture->type &&
           encoded[36] == capture->precision &&
           encoded[37] == capture->scale &&
           encoded[38] == PRDB_SDK_VALUE_STATE_VALUE &&
           encoded[42] == capture->expected_len[index] &&
           memcmp(encoded + 43, capture->expected[index],
                  capture->expected_len[index]) == 0);
    ++capture->calls;
    return 0;
}

static int32_t run_scalar_minmax(mock_state *mock, const uint8_t *sql,
                                 uint64_t sql_len,
                                 const scalar_order_case *test,
                                 prdb_sdk_sql_group_row_v5_fn on_group,
                                 void *capture) {
    static const uint8_t schema[] = "app";
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[2048] = {0};
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, test->type, test->precision, test->scale}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, test->type, test->precision, test->scale}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v5(
        &host, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0, 4, 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes), on_group, capture);
}

typedef struct scalar_integral_aggregate_capture {
    uint32_t input_type;
    uint32_t calls;
} scalar_integral_aggregate_capture;

static int32_t on_group_scalar_sum(
    void *context, const prdb_sdk_value_v1 *group_key,
    const prdb_sdk_value_v1 *aggregate_value, uint64_t matching_row_count,
    const prdb_sdk_select_binding_v6 *binding) {
    scalar_integral_aggregate_capture *capture =
        (scalar_integral_aggregate_capture *)context;
    int32_t key = 0;
    int64_t sum = 0;
    assert(capture->calls < 2 &&
           binding->aggregate_kind == PRDB_SDK_SQL_V6_AGGREGATE_SUM &&
           binding->aggregate_column.type_code == capture->input_type);
    assert(prdb_sdk_value_get_integer_v1(group_key, &key) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(key == (capture->calls == 0 ? 1 : 2));
    assert(matching_row_count == (capture->calls == 0 ? 2 : 1));
    assert(aggregate_value != NULL &&
           aggregate_value->state == PRDB_SDK_VALUE_STATE_VALUE &&
           aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_BIGINT &&
           prdb_sdk_value_get_bigint_v1(aggregate_value, &sum) ==
               PRDB_SDK_VALUE_ACCESS_OK &&
           sum == 6);
    ++capture->calls;
    return 0;
}

static int32_t on_group_scalar_avg(
    void *context, const prdb_sdk_value_v1 *group_key,
    const prdb_sdk_value_v1 *aggregate_value, uint64_t matching_row_count,
    const prdb_sdk_select_binding_v7 *binding) {
    static const uint8_t average_three[16] = {
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x2D, 0xC6, 0xC0};
    static const uint8_t average_six[16] = {
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x5B, 0x8D, 0x80};
    scalar_integral_aggregate_capture *capture =
        (scalar_integral_aggregate_capture *)context;
    const uint8_t *expected = capture->calls == 0 ? average_three : average_six;
    int32_t key = 0;
    assert(capture->calls < 2 &&
           binding->aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_AVG &&
           binding->aggregate_column.type_code == capture->input_type);
    assert(prdb_sdk_value_get_integer_v1(group_key, &key) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(key == (capture->calls == 0 ? 1 : 2));
    assert(matching_row_count == (capture->calls == 0 ? 2 : 1));
    assert(aggregate_value != NULL &&
           aggregate_value->state == PRDB_SDK_VALUE_STATE_VALUE &&
           aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
           aggregate_value->data_len == sizeof(average_three) &&
           memcmp(aggregate_value->data, expected, sizeof(average_three)) == 0);
    ++capture->calls;
    return 0;
}

static int32_t run_scalar_sum_or_avg(
    mock_state *mock, const uint8_t *sql, uint64_t sql_len,
    const scalar_order_case *test, uint32_t average,
    scalar_integral_aggregate_capture *capture) {
    static const uint8_t schema[] = "app";
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[2048] = {0};
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, test->type, test->precision, test->scale}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, test->type, test->precision, test->scale}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    if (average) {
        return prdb_sdk_sql_execute_group_v7(
            &host, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0, 4, 512,
            planner_response, sizeof(planner_response), executor_response,
            sizeof(executor_response), batch_response, sizeof(batch_response),
            planner_columns, 2, executor_columns, 2, cell_scratch, 2,
            materialized_bytes, sizeof(materialized_bytes), on_group_scalar_avg,
            capture);
    }
    return prdb_sdk_sql_execute_group_v6(
        &host, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0, 4, 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes), on_group_scalar_sum,
        capture);
}

static void reset_scan(mock_state *mock, group_capture *capture,
                       uint32_t finished);

static void run_grouped_scalar_order_matrix(mock_state *mock) {
    static const uint8_t asc_sql[] =
        "SELECT id, COUNT(*) FROM widgets GROUP BY id ORDER BY id ASC";
    static const uint8_t desc_sql[] =
        "SELECT id, COUNT(*) FROM widgets GROUP BY id ORDER BY id DESC";
    static const uint8_t min_sql[] =
        "SELECT id, MIN(score) FROM widgets GROUP BY id";
    static const uint8_t max_sql[] =
        "SELECT id, MAX(score) FROM widgets GROUP BY id";
    static const uint8_t boolean_lower[] = {0};
    static const uint8_t boolean_higher[] = {1};
    static const uint8_t smallint_lower[] = {0x80, 0};
    static const uint8_t smallint_middle[] = {0, 0};
    static const uint8_t smallint_higher[] = {0x7F, 0xFF};
    static const uint8_t integer_lower[] = {0x80, 0, 0, 0};
    static const uint8_t integer_middle[] = {0, 0, 0, 0};
    static const uint8_t integer_higher[] = {0x7F, 0xFF, 0xFF, 0xFF};
    static const uint8_t bigint_lower[] = {0x80, 0, 0, 0, 0, 0, 0, 0};
    static const uint8_t bigint_middle[] = {0, 0, 0, 0, 0, 0, 0, 0};
    static const uint8_t bigint_higher[] = {0x7F, 0xFF, 0xFF, 0xFF,
                                             0xFF, 0xFF, 0xFF, 0xFF};
    static const uint8_t decimal_lower[] = {0xFF, 0xFF, 0xFF, 0xFF,
                                             0xFF, 0xFF, 0xFF, 0xFF,
                                             0xFF, 0xFF, 0xFF, 0xFF,
                                             0xFF, 0xFF, 0xFF, 0x9C};
    static const uint8_t decimal_middle[16] = {0};
    static const uint8_t decimal_higher[] = {0, 0, 0, 0, 0, 0, 0, 0,
                                              0, 0, 0, 0, 0, 0, 0, 0x64};
    static const uint8_t text_lower[] = {'a'};
    static const uint8_t text_middle[] = {'b'};
    static const uint8_t text_higher[] = {'c'};
    static const uint8_t varchar_lower[] = {0xC2, 0xA2};
    static const uint8_t varchar_middle[] = {0xC3, 0xA9};
    static const uint8_t varchar_higher[] = {0xE2, 0x82, 0xAC};
    static const uint8_t bytea_lower[] = {0, 0};
    static const uint8_t bytea_middle[] = {0, 1};
    static const uint8_t bytea_higher[] = {1, 0};
    static const uint8_t uuid_lower[] = {0, 1, 2, 3, 4, 5, 6, 7,
                                          8, 9, 10, 11, 12, 13, 14, 15};
    static const uint8_t uuid_middle[] = {0, 1, 2, 3, 4, 5, 6, 7,
                                           8, 9, 10, 11, 12, 13, 14, 16};
    static const uint8_t uuid_higher[] = {0, 1, 2, 3, 4, 5, 6, 7,
                                           8, 9, 10, 11, 12, 13, 14, 17};
    static const uint8_t date_lower[] = {0xFF, 0xFF, 0xFF, 0xFF};
    static const uint8_t date_middle[] = {0, 0, 0, 0};
    static const uint8_t date_higher[] = {0, 0, 0, 1};
    static const uint8_t timestamp_lower[] = {0xFF, 0xFF, 0xFF, 0xFF,
                                               0xFF, 0xFF, 0xFF, 0xFF};
    static const uint8_t timestamp_middle[] = {0, 0, 0, 0, 0, 0, 0, 0};
    static const uint8_t timestamp_higher[] = {0, 0, 0, 0, 0, 0, 0, 1};
    static const scalar_order_case cases[] = {
        {PRDB_SDK_VALUE_TYPE_BOOLEAN, 0, 0, 2,
         boolean_lower, sizeof(boolean_lower), NULL, 0,
         boolean_higher, sizeof(boolean_higher)},
        {PRDB_SDK_VALUE_TYPE_SMALLINT, 0, 0, 3,
         smallint_lower, sizeof(smallint_lower), smallint_middle,
         sizeof(smallint_middle), smallint_higher, sizeof(smallint_higher)},
        {PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 3,
         integer_lower, sizeof(integer_lower), integer_middle,
         sizeof(integer_middle), integer_higher, sizeof(integer_higher)},
        {PRDB_SDK_VALUE_TYPE_BIGINT, 0, 0, 3,
         bigint_lower, sizeof(bigint_lower), bigint_middle,
         sizeof(bigint_middle), bigint_higher, sizeof(bigint_higher)},
        {PRDB_SDK_VALUE_TYPE_DECIMAL, 38, 2, 3,
         decimal_lower, sizeof(decimal_lower), decimal_middle,
         sizeof(decimal_middle), decimal_higher, sizeof(decimal_higher)},
        {PRDB_SDK_VALUE_TYPE_TEXT, 0, 0, 3,
         text_lower, sizeof(text_lower), text_middle, sizeof(text_middle),
         text_higher, sizeof(text_higher)},
        {PRDB_SDK_VALUE_TYPE_VARCHAR, 0, 0, 3,
         varchar_lower, sizeof(varchar_lower), varchar_middle,
         sizeof(varchar_middle), varchar_higher, sizeof(varchar_higher)},
        {PRDB_SDK_VALUE_TYPE_BYTEA, 0, 0, 3,
         bytea_lower, sizeof(bytea_lower), bytea_middle, sizeof(bytea_middle),
         bytea_higher, sizeof(bytea_higher)},
        {PRDB_SDK_VALUE_TYPE_UUID, 0, 0, 3,
         uuid_lower, sizeof(uuid_lower), uuid_middle, sizeof(uuid_middle),
         uuid_higher, sizeof(uuid_higher)},
        {PRDB_SDK_VALUE_TYPE_DATE, 0, 0, 3,
         date_lower, sizeof(date_lower), date_middle, sizeof(date_middle),
         date_higher, sizeof(date_higher)},
        {PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ, 0, 0, 3,
         timestamp_lower, sizeof(timestamp_lower), timestamp_middle,
         sizeof(timestamp_middle), timestamp_higher, sizeof(timestamp_higher)},
    };

    for (uint32_t case_index = 0;
         case_index < sizeof(cases) / sizeof(cases[0]); ++case_index) {
        const scalar_order_case *test = &cases[case_index];
        const uint8_t *input_values[3] = {
            test->higher, test->lower, test->middle};
        uint32_t input_lengths[3] = {
            test->higher_len, test->lower_len, test->middle_len};
        const uint8_t *ascending_values[3] = {
            test->lower, test->middle, test->higher};
        uint32_t ascending_lengths[3] = {
            test->lower_len, test->middle_len, test->higher_len};
        const uint8_t *descending_values[3] = {
            test->higher, test->middle, test->lower};
        uint32_t descending_lengths[3] = {
            test->higher_len, test->middle_len, test->lower_len};

        mock->text_key_mode = 0;
        mock->max_candidates = 0;
        mock->planner_len = make_relation(
            mock->planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
            test->type);
        mock->executor_len = make_relation(mock->executor, 6, 0x22,
                                           test->type);
        mock->batch_len = make_typed_key_rows(
            mock->batch, 1, test->type, input_values, input_lengths,
            test->row_count);
        for (uint32_t direction = 0; direction < 2; ++direction) {
            group_capture capture = {0};
            capture.mock = mock;
            capture.scalar_order_mode = 1;
            capture.scalar_order_type = test->type;
            capture.scalar_order_precision = test->precision;
            capture.scalar_order_scale = test->scale;
            capture.scalar_order_descending = direction;
            for (uint32_t row = 0; row < test->row_count; ++row) {
                if (test->row_count == 2) {
                    capture.scalar_order_expected[row] = direction
                        ? (row == 0 ? test->higher : test->lower)
                        : (row == 0 ? test->lower : test->higher);
                    capture.scalar_order_expected_len[row] = direction
                        ? (row == 0 ? test->higher_len : test->lower_len)
                        : (row == 0 ? test->lower_len : test->higher_len);
                } else {
                    capture.scalar_order_expected[row] = direction
                        ? descending_values[row]
                        : ascending_values[row];
                    capture.scalar_order_expected_len[row] = direction
                        ? descending_lengths[row]
                        : ascending_lengths[row];
                }
            }
            reset_scan(mock, &capture, 1);
            int32_t status = run_query_v12(
                mock, direction ? desc_sql : asc_sql,
                direction ? sizeof(desc_sql) - 1 : sizeof(asc_sql) - 1, 1024,
                &capture);
            assert(status == PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == test->row_count &&
                   mock->statement_failed == 0);
            for (uint32_t row = 0; row < capture.calls; ++row) {
                assert(capture.scalar_order_sqlr_len[row] > 0);
            }
        }

        {
            const uint8_t *middle = test->middle != NULL
                                        ? test->middle
                                        : test->lower;
            uint32_t middle_len = test->middle != NULL
                                      ? test->middle_len
                                      : test->lower_len;
            const uint8_t *score_values[3] = {
                test->higher, test->lower, middle};
            const uint32_t score_lengths[3] = {
                test->higher_len, test->lower_len, middle_len};
            mock->planner_len = make_typed_score_relation(
                mock->planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
                test);
            mock->executor_len = make_typed_score_relation(
                mock->executor, 6, 0x22, test);
            mock->batch_len = make_typed_score_rows(
                mock->batch, test, score_values, score_lengths);
            for (uint32_t aggregate = 0; aggregate < 2; ++aggregate) {
                scalar_minmax_capture capture = {0};
                capture.mock = mock;
                capture.type = test->type;
                capture.precision = test->precision;
                capture.scale = test->scale;
                capture.aggregate_kind = aggregate == 0
                    ? PRDB_SDK_SQL_V5_AGGREGATE_MIN
                    : PRDB_SDK_SQL_V5_AGGREGATE_MAX;
                capture.expected[0] = aggregate == 0
                    ? test->lower : test->higher;
                capture.expected_len[0] = aggregate == 0
                    ? test->lower_len : test->higher_len;
                capture.expected[1] = middle;
                capture.expected_len[1] = middle_len;
                group_capture reset_capture = {0};
                reset_scan(mock, &reset_capture, 1);
                int32_t status = run_scalar_minmax(
                    mock, aggregate == 0 ? min_sql : max_sql,
                    aggregate == 0 ? sizeof(min_sql) - 1
                                   : sizeof(max_sql) - 1,
                    test, on_group_scalar_minmax, &capture);
                assert(status == PRDB_SDK_SQL_EXEC_OK &&
                       capture.calls == 2 && mock->statement_failed == 0);
            }
        }
    }
}

static void run_integral_aggregate_runner_matrix(mock_state *mock) {
    static const uint8_t sum_sql[] =
        "SELECT id, SUM(score) FROM widgets GROUP BY id";
    static const uint8_t avg_sql[] =
        "SELECT id, AVG(score) FROM widgets GROUP BY id";
    static const uint8_t smallint_two[] = {0, 2};
    static const uint8_t smallint_four[] = {0, 4};
    static const uint8_t smallint_six[] = {0, 6};
    static const uint8_t integer_two[] = {0, 0, 0, 2};
    static const uint8_t integer_four[] = {0, 0, 0, 4};
    static const uint8_t integer_six[] = {0, 0, 0, 6};
    static const uint8_t bigint_two[] = {0, 0, 0, 0, 0, 0, 0, 2};
    static const uint8_t bigint_four[] = {0, 0, 0, 0, 0, 0, 0, 4};
    static const uint8_t bigint_six[] = {0, 0, 0, 0, 0, 0, 0, 6};
    static const scalar_order_case cases[] = {
        {PRDB_SDK_VALUE_TYPE_SMALLINT, 0, 0, 3, smallint_two,
         sizeof(smallint_two), smallint_six, sizeof(smallint_six),
         smallint_four, sizeof(smallint_four)},
        {PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 3, integer_two,
         sizeof(integer_two), integer_six, sizeof(integer_six), integer_four,
         sizeof(integer_four)},
        {PRDB_SDK_VALUE_TYPE_BIGINT, 0, 0, 3, bigint_two, sizeof(bigint_two),
         bigint_six, sizeof(bigint_six), bigint_four, sizeof(bigint_four)},
    };

    for (uint32_t index = 0; index < sizeof(cases) / sizeof(cases[0]);
         ++index) {
        const scalar_order_case *test = &cases[index];
        const uint8_t *scores[3] = {test->higher, test->lower, test->middle};
        const uint32_t score_lengths[3] = {
            test->higher_len, test->lower_len, test->middle_len};
        mock->planner_len = make_typed_score_relation(
            mock->planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99, test);
        mock->executor_len = make_typed_score_relation(
            mock->executor, 6, 0x22, test);
        mock->batch_len = make_typed_score_rows(
            mock->batch, test, scores, score_lengths);

        for (uint32_t average = 0; average < 2; ++average) {
            scalar_integral_aggregate_capture capture = {test->type, 0};
            group_capture reset_capture = {0};
            reset_scan(mock, &reset_capture, 1);
            int32_t status = run_scalar_sum_or_avg(
                mock, average ? avg_sql : sum_sql,
                average ? sizeof(avg_sql) - 1 : sizeof(sum_sql) - 1,
                test, average, &capture);
            assert(status == PRDB_SDK_SQL_EXEC_OK && capture.calls == 2 &&
                   mock->statement_failed == 0);
        }
    }
}

static int32_t on_group_v13(void *context,
                            const prdb_sdk_value_v1 *group_keys,
                            uint32_t group_key_count,
                            const prdb_sdk_value_v1 *aggregate_value,
                            uint64_t matching_row_count,
                            const prdb_sdk_select_binding_v13 *binding) {
    group_capture *capture = (group_capture *)context;
    int32_t first = 0, second = 0;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V13_VERSION);
    assert(group_key_count == 2 && binding->group_key_count == 2);
    assert(capture->mock->batch_finished != 0);
    assert(capture->mock->executor_calls ==
           (capture->mock->optimizer_enabled ? 4u : 3u));
    assert(capture->mock->optimizer_relation_resolves ==
           (capture->mock->optimizer_enabled ? 1u : 0u));
    assert(capture->calls < 128);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &first) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[1], &second) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(binding->base.aggregate_kind ==
               PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR ||
           binding->base.aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_MIN ||
           binding->base.aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_MAX ||
           binding->base.aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_SUM ||
           binding->base.aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_AVG);
    if (binding->base.aggregate_kind ==
        PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
        assert(aggregate_value == NULL);
        capture->aggregates[capture->calls] = (int32_t)matching_row_count;
    } else if (binding->base.aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_SUM) {
        int64_t sum = 0;
        assert(aggregate_value != NULL &&
               aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_BIGINT);
        assert(prdb_sdk_value_get_bigint_v1(aggregate_value, &sum) ==
               PRDB_SDK_VALUE_ACCESS_OK);
        capture->aggregates[capture->calls] = (int32_t)sum;
        assert((int64_t)capture->aggregates[capture->calls] == sum);
    } else if (binding->base.aggregate_kind ==
                   PRDB_SDK_SQL_V7_AGGREGATE_MIN ||
               binding->base.aggregate_kind ==
                   PRDB_SDK_SQL_V7_AGGREGATE_MAX) {
        int32_t value = 0;
        assert(aggregate_value != NULL &&
               aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_INTEGER);
        assert(prdb_sdk_value_get_integer_v1(aggregate_value, &value) ==
               PRDB_SDK_VALUE_ACCESS_OK);
        capture->aggregates[capture->calls] = value;
    } else {
        static const uint8_t average_nine[16] = {
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x89, 0x54, 0x40};
        static const uint8_t average_five[16] = {
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x4c, 0x4b, 0x40};
        const uint8_t *expected = first == 1 ? average_nine : average_five;
        assert(binding->base.aggregate_kind ==
               PRDB_SDK_SQL_V7_AGGREGATE_AVG);
        assert(aggregate_value != NULL &&
               aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
               aggregate_value->data_len == 16 &&
               memcmp(aggregate_value->data, expected, 16) == 0);
        capture->aggregates[capture->calls] = first == 1 ? 9 : 5;
    }
    capture->keys[capture->calls] = first;
    capture->second_keys[capture->calls] = second;
    capture->row_counts[capture->calls] = matching_row_count;
    ++capture->calls;
    if (capture->mock->cancel_after_group_callbacks == capture->calls) {
        capture->mock->cancelled = 1;
    }
    return 0;
}

typedef struct nullable_composite_capture {
    mock_state *mock;
    uint32_t calls;
} nullable_composite_capture;

static int32_t on_nullable_composite_group_v13(
    void *context, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v13 *binding) {
    static const uint32_t expected_counts[] = {2, 1, 1, 2};
    nullable_composite_capture *capture = context;
    int32_t first = 0, second = 0;
    uint32_t first_state = 0, second_state = 0;
    uint32_t index = capture->calls;
    assert(index < 4 && group_key_count == 2 && aggregate_value == NULL);
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V13_VERSION &&
           binding->base.aggregate_kind ==
               PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR);
    assert(capture->mock->batch_finished != 0 &&
           capture->mock->executor_calls == 3);
    assert(prdb_sdk_value_state_v1(&group_keys[0], &first_state) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(prdb_sdk_value_state_v1(&group_keys[1], &second_state) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    if (index == 0) {
        assert(first_state == PRDB_SDK_VALUE_STATE_VALUE &&
               second_state == PRDB_SDK_VALUE_STATE_NULL);
        assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &first) ==
                   PRDB_SDK_VALUE_ACCESS_OK &&
               first == 1);
    } else if (index == 1) {
        assert(first_state == PRDB_SDK_VALUE_STATE_VALUE &&
               second_state == PRDB_SDK_VALUE_STATE_VALUE);
        assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &first) ==
                   PRDB_SDK_VALUE_ACCESS_OK &&
               prdb_sdk_value_get_integer_v1(&group_keys[1], &second) ==
                   PRDB_SDK_VALUE_ACCESS_OK &&
               first == 1 && second == 9);
    } else if (index == 2) {
        assert(first_state == PRDB_SDK_VALUE_STATE_NULL &&
               second_state == PRDB_SDK_VALUE_STATE_VALUE);
        assert(prdb_sdk_value_get_integer_v1(&group_keys[1], &second) ==
                   PRDB_SDK_VALUE_ACCESS_OK &&
               second == 9);
    } else {
        assert(first_state == PRDB_SDK_VALUE_STATE_NULL &&
               second_state == PRDB_SDK_VALUE_STATE_NULL);
    }
    assert(matching_row_count == expected_counts[index]);
    ++capture->calls;
    return 0;
}

static int32_t on_group_eight_keys(
    void *context, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v13 *binding) {
    group_capture *capture = (group_capture *)context;
    int64_t sum = 0;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V13_VERSION);
    assert(group_key_count == 8 && binding->group_key_count == 8);
    assert(binding->base.aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_SUM);
    assert(binding->base.aggregate_column.ordinal == 8);
    assert(capture->calls < 2 && capture->mock->batch_finished != 0);
    for (uint32_t index = 0; index < 8; ++index) {
        int32_t value = 0;
        assert(group_keys[index].type_code == PRDB_SDK_VALUE_TYPE_INTEGER);
        assert(prdb_sdk_value_get_integer_v1(&group_keys[index], &value) ==
               PRDB_SDK_VALUE_ACCESS_OK);
        assert(value == (int32_t)(index + 1) +
                            (capture->calls == 1 && index == 7 ? 1 : 0));
    }
    assert(aggregate_value != NULL &&
           aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_BIGINT);
    assert(prdb_sdk_value_get_bigint_v1(aggregate_value, &sum) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(sum == (capture->calls == 0 ? 9 : 8));
    assert(matching_row_count == (capture->calls == 0 ? 2 : 1));
    ++capture->calls;
    return 0;
}

static int32_t on_ordered_group_eight_keys(
    void *context, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v15 *binding) {
    group_capture *capture = (group_capture *)context;
    int64_t sum = 0;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V15_VERSION);
    assert(binding->base.base.group_key_count == 8);
    assert(binding->base.base.base.aggregate_kind ==
           PRDB_SDK_SQL_V7_AGGREGATE_SUM);
    assert(binding->base.base.base.aggregate_column.ordinal == 8);
    assert(binding->order_count == 9);
    assert(group_key_count == 8 && capture->calls < 2);
    assert(capture->mock->batch_finished != 0 &&
           capture->mock->executor_calls ==
               (capture->mock->optimizer_enabled ? 4u : 3u));
    for (uint32_t index = 0; index < 8; ++index) {
        int32_t value = 0;
        assert(binding->order_items[index].item.kind ==
               PRDB_SDK_SQL_V10_ORDER_GROUP_KEY);
        assert(binding->order_items[index].item.group_key_index == index);
        assert(binding->order_items[index].item.direction ==
               PRDB_SDK_SQL_V10_ORDER_DESCENDING);
        assert(binding->order_items[index].type_code ==
               PRDB_SDK_VALUE_TYPE_INTEGER);
        assert(group_keys[index].type_code == PRDB_SDK_VALUE_TYPE_INTEGER);
        assert(prdb_sdk_value_get_integer_v1(&group_keys[index], &value) ==
               PRDB_SDK_VALUE_ACCESS_OK);
        assert(value == (int32_t)(index + 1) +
                            (capture->calls == 0 && index == 7 ? 1 : 0));
    }
    assert(binding->order_items[8].item.kind ==
           PRDB_SDK_SQL_V10_ORDER_AGGREGATE);
    assert(binding->order_items[8].item.direction ==
           PRDB_SDK_SQL_V10_ORDER_DESCENDING);
    assert(binding->order_items[8].type_code == PRDB_SDK_VALUE_TYPE_BIGINT);
    assert(aggregate_value != NULL &&
           aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_BIGINT);
    assert(prdb_sdk_value_get_bigint_v1(aggregate_value, &sum) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(sum == (capture->calls == 0 ? 8 : 9));
    assert(matching_row_count == (capture->calls == 0 ? 1 : 2));
    ++capture->calls;
    return 0;
}

static int32_t run_query_v13(mock_state *mock, const uint8_t *sql,
                             uint64_t sql_len, uint64_t arena_capacity,
                             group_capture *capture) {
    static const uint8_t schema[] = "app";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[4096] = {0};
    uint8_t materialized_bytes[4096] = {0};
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    mock->optimizer_enabled = 0;
    return prdb_sdk_sql_execute_group_v13(
        &host, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0,
        mock->max_candidates == 0 ? 4 : (uint16_t)mock->max_candidates,
        mock->max_candidates > 3 ? 4096 : 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, arena_capacity, on_group_v13, capture);
}

static int32_t run_nullable_composite_count_v13(
    mock_state *mock, const uint8_t *sql, uint64_t sql_len,
    nullable_composite_capture *capture) {
    static const uint8_t schema[] = "app";
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[4096] = {0};
    uint8_t materialized_bytes[4096] = {0};
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v13(
        &host, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0, 6, 4096,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes),
        on_nullable_composite_group_v13, capture);
}

static int32_t run_query_v13_optimizer_internal(
    mock_state *mock, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint32_t filtered, uint64_t arena_capacity, group_capture *capture) {
    static const uint8_t schema[] = "app";
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    uint8_t optimizer_request_bytes[4096] = {0};
    uint8_t optimizer_response[4096] = {0};
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[512] = {0};
    uint8_t batch_response[4096] = {0};
    uint8_t materialized_bytes[4096] = {0};
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
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
    if (filtered)
        return prdb_sdk_sql_execute_filtered_group_composite_aggregate_optimizer_v1(
            &host, schema, sizeof(schema) - 1, sql, sql_len, parameters,
            parameter_count,
            mock->max_candidates == 0 ? 4 : (uint16_t)mock->max_candidates,
            mock->max_candidates > 3 ? 4096 : 512, optimizer_request_bytes,
            sizeof(optimizer_request_bytes), optimizer_response,
            sizeof(optimizer_response), planner_response,
            sizeof(planner_response), executor_response,
            sizeof(executor_response), batch_response, sizeof(batch_response),
            planner_columns, 2, executor_columns, 2, cell_scratch, 2,
            materialized_bytes, arena_capacity, on_group_v13, capture);
    return prdb_sdk_sql_execute_group_composite_aggregate_optimizer_v1(
        &host, schema, sizeof(schema) - 1, sql, sql_len,
        mock->max_candidates == 0 ? 4 : (uint16_t)mock->max_candidates,
        mock->max_candidates > 3 ? 4096 : 512, optimizer_request_bytes,
        sizeof(optimizer_request_bytes), optimizer_response,
        sizeof(optimizer_response), planner_response,
        sizeof(planner_response), executor_response, sizeof(executor_response),
        batch_response, sizeof(batch_response), planner_columns, 2,
        executor_columns, 2, cell_scratch, 2, materialized_bytes,
        arena_capacity, on_group_v13, capture);
}

static int32_t run_query_v13_optimizer(mock_state *mock, const uint8_t *sql,
                                       uint64_t sql_len,
                                       uint64_t arena_capacity,
                                       group_capture *capture) {
    return run_query_v13_optimizer_internal(mock, sql, sql_len, NULL, 0, 0,
                                            arena_capacity, capture);
}

static int32_t run_ordered_eight_key_optimizer(
    mock_state *mock, const uint8_t *sql, uint64_t sql_len,
    group_capture *capture) {
    static const uint8_t schema[] = "app";
    static const uint8_t *names[9] = {
        (const uint8_t *)"key_0", (const uint8_t *)"key_1",
        (const uint8_t *)"key_2", (const uint8_t *)"key_3",
        (const uint8_t *)"key_4", (const uint8_t *)"key_5",
        (const uint8_t *)"key_6", (const uint8_t *)"key_7",
        (const uint8_t *)"measure"};
    prdb_sdk_sql_column_shape_v2 planner_columns[9] = {{0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[9] = {{0}};
    prdb_sdk_value_v1 cell_scratch[9] = {{0}};
    uint8_t optimizer_request_bytes[4096] = {0};
    uint8_t optimizer_response[4096] = {0};
    uint8_t planner_response[512] = {0};
    uint8_t executor_response[512] = {0};
    uint8_t batch_response[4096] = {0};
    uint8_t materialized_bytes[4096] = {0};
    prdb_host_api_v5 host = {0};
    for (uint32_t index = 0; index < 9; ++index) {
        planner_columns[index] = (prdb_sdk_sql_column_shape_v2){
            names[index], (uint32_t)strlen((const char *)names[index]), index,
            PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0};
        executor_columns[index] = planner_columns[index];
    }
    host.v4.v3 = (prdb_host_api_v3){
        sizeof(prdb_host_api_v3), PRDB_QUERY_ABI_V3_VERSION, mock,
        executor_request, planner_request, NULL, statement_failure};
    host.v4.optimizer_request = unused_optimizer_request;
    host.optimizer_request = optimizer_request;
    mock->optimizer_enabled = 0;
    int32_t result = prdb_sdk_sql_execute_ordered_composite_aggregate_optimizer_v1(
        &host, schema, sizeof(schema) - 1, sql, sql_len, 4, 4096,
        optimizer_request_bytes, sizeof(optimizer_request_bytes),
        optimizer_response, sizeof(optimizer_response), planner_response,
        sizeof(planner_response), executor_response, sizeof(executor_response),
        batch_response, sizeof(batch_response), planner_columns, 9,
        executor_columns, 9, cell_scratch, 9, materialized_bytes,
        sizeof(materialized_bytes), on_ordered_group_eight_keys, capture);
    return result;
}

static int32_t on_group_v14(void *context,
                            const prdb_sdk_value_v1 *group_keys,
                            uint32_t group_key_count,
                            const prdb_sdk_value_v1 *aggregate_value,
                            uint64_t matching_row_count,
                            const prdb_sdk_select_binding_v14 *binding) {
    group_capture *capture = (group_capture *)context;
    int32_t key = 0;
    int64_t sum = 0;
    (void)matching_row_count;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V14_VERSION);
    assert(binding->base.group_key_count == 2 && group_key_count == 2);
    assert(binding->has_having == 1 &&
           binding->having_parameter_index == 1);
    assert(capture->mock->batch_finished != 0);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &key) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(prdb_sdk_value_get_bigint_v1(aggregate_value, &sum) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    capture->keys[capture->calls] = key;
    capture->aggregates[capture->calls] = (int32_t)sum;
    ++capture->calls;
    return 0;
}

static int32_t run_query_v14(mock_state *mock, const uint8_t *sql,
                             uint64_t sql_len,
                             const prdb_sdk_value_v1 *parameter,
                             group_capture *capture) {
    static const uint8_t schema[] = "app";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[64] = {0};
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v14(
        &host, schema, sizeof(schema) - 1, sql, sql_len, parameter, 1, 4, 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes), on_group_v14, capture);
}

static int32_t on_group_v15(void *context,
                            const prdb_sdk_value_v1 *group_keys,
                            uint32_t group_key_count,
                            const prdb_sdk_value_v1 *aggregate_value,
                            uint64_t matching_row_count,
                            const prdb_sdk_select_binding_v15 *binding) {
    group_capture *capture = (group_capture *)context;
    int32_t first = 0, second = 0;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V15_VERSION);
    assert(binding->order_count == 3);
    assert(binding->order_items[0].type_code == PRDB_SDK_VALUE_TYPE_INTEGER);
    assert(binding->order_items[1].type_code == PRDB_SDK_VALUE_TYPE_BIGINT);
    assert(binding->order_items[2].type_code == PRDB_SDK_VALUE_TYPE_INTEGER);
    assert(group_key_count == 2 && aggregate_value == NULL);
    assert(capture->mock->batch_finished != 0);
    assert(capture->mock->executor_calls == 3);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &first) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[1], &second) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    capture->keys[capture->calls] = first;
    capture->second_keys[capture->calls] = second;
    capture->row_counts[capture->calls] = matching_row_count;
    ++capture->calls;
    return 0;
}

static int32_t run_query_v15(mock_state *mock, const uint8_t *sql,
                             uint64_t sql_len,
                             const prdb_sdk_value_v1 *parameter,
                             group_capture *capture) {
    static const uint8_t schema[] = "app";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[64] = {0};
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v15(
        &host, schema, sizeof(schema) - 1, sql, sql_len, parameter, 1, 4, 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes), on_group_v15, capture);
}

static int32_t on_group_v17(void *context,
                            const prdb_sdk_value_v1 *group_keys,
                            uint32_t group_key_count,
                            const prdb_sdk_value_v1 *aggregate_value,
                            uint64_t matching_row_count,
                            const prdb_sdk_select_binding_v17 *binding) {
    group_capture *capture = (group_capture *)context;
    int32_t key = 0;
    int64_t sum = 0;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V17_VERSION);
    assert(binding->has_having_literal == capture->expect_having_literal);
    assert(binding->having_literal ==
           (capture->expect_having_literal ? UINT64_C(10) : UINT64_C(0)));
    assert(binding->base.base.has_having == 1);
    assert(binding->base.base.having_parameter_index == 1);
    assert(binding->base.base.base.group_key_count == 2 &&
           group_key_count == 2);
    assert(aggregate_value != NULL && matching_row_count == 2);
    assert(capture->mock->batch_finished != 0);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &key) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(prdb_sdk_value_get_bigint_v1(aggregate_value, &sum) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    capture->keys[capture->calls] = key;
    capture->aggregates[capture->calls] = (int32_t)sum;
    ++capture->calls;
    return 0;
}

static int32_t run_query_v17(mock_state *mock, const uint8_t *sql,
                             uint64_t sql_len,
                             const prdb_sdk_value_v1 *parameter,
                             group_capture *capture) {
    static const uint8_t schema[] = "app";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[64] = {0};
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v17(
        &host, schema, sizeof(schema) - 1, sql, sql_len, parameter,
        parameter == NULL ? 0 : 1, 4, 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes), on_group_v17, capture);
}

static int32_t on_group_v18(void *context,
                            const prdb_sdk_value_v1 *group_keys,
                            uint32_t group_key_count,
                            const prdb_sdk_value_v1 *aggregate_value,
                            uint64_t matching_row_count,
                            const prdb_sdk_select_binding_v18 *binding) {
    group_capture *capture = (group_capture *)context;
    int32_t key = 0, second_key = 0;
    int64_t sum = 0;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V18_VERSION);
    assert(binding->having_node_count ==
           (capture->expect_having_literal ? 1u : 6u));
    assert(binding->having_root == binding->having_node_count - 1);
    assert(binding->having_type_code == PRDB_SDK_VALUE_TYPE_BIGINT);
    assert(binding->base.base.has_having == 0);
    assert(binding->base.base.base.group_key_count == 2 &&
           group_key_count == 2);
    assert(aggregate_value != NULL &&
           matching_row_count ==
               (capture->expected_row_count == 0
                    ? UINT64_C(1)
                    : capture->expected_row_count));
    assert(capture->mock->batch_finished != 0);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &key) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[1], &second_key) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(prdb_sdk_value_get_bigint_v1(aggregate_value, &sum) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    capture->keys[capture->calls] = key;
    capture->second_keys[capture->calls] = second_key;
    capture->aggregates[capture->calls] = (int32_t)sum;
    ++capture->calls;
    return 0;
}

static int32_t run_query_v18(mock_state *mock, const uint8_t *sql,
                             uint64_t sql_len,
                             const prdb_sdk_value_v1 *parameter,
                             group_capture *capture) {
    static const uint8_t schema[] = "app";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[2048] = {0};
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v18(
        &host, schema, sizeof(schema) - 1, sql, sql_len, parameter,
        parameter == NULL ? 0 : 1,
        mock->max_candidates == 0 ? 4 : (uint16_t)mock->max_candidates, 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes), on_group_v18, capture);
}

static int32_t on_group_v21_decimal(
    void *context, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v21 *binding) {
    group_capture *capture = (group_capture *)context;
    int32_t key = 0;
    uint64_t aggregate_bits = 0;
    int64_t aggregate_coefficient;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V21_VERSION);
    assert(binding->base.having_type_code == PRDB_SDK_VALUE_TYPE_DECIMAL);
    assert(binding->base.having_node_count == 1);
    assert(binding->having_nodes[0].base.operand_kind ==
           PRDB_SDK_SQL_V21_OPERAND_NUMERIC_LITERAL);
    if (binding->base.having_decimal_scale == 2) {
        assert(binding->having_nodes[0].scale == 2);
        assert(binding->having_nodes[0].coefficient[14] == 1 &&
               binding->having_nodes[0].coefficient[15] == 0xc2);
    } else {
        assert(binding->base.having_decimal_scale == 8);
        assert(binding->having_nodes[0].scale == 8);
        assert(binding->having_nodes[0].coefficient[12] == 0x08 &&
               binding->having_nodes[0].coefficient[13] == 0x58 &&
               binding->having_nodes[0].coefficient[14] == 0x3b &&
               binding->having_nodes[0].coefficient[15] == 0x00);
    }
    assert(group_key_count == 2 && aggregate_value != NULL);
    assert(matching_row_count == 1 && capture->mock->batch_finished != 0);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &key) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
           aggregate_value->data_len == 16);
    for (uint32_t index = 0; index < 8; ++index)
        assert(aggregate_value->data[index] == 0);
    for (uint32_t index = 0; index < 8; ++index)
        aggregate_bits = (aggregate_bits << 8) | aggregate_value->data[index + 8];
    aggregate_coefficient = (int64_t)aggregate_bits;
    capture->keys[capture->calls] = key;
    capture->aggregates[capture->calls] = binding->base.having_decimal_scale == 2
                                              ? (int32_t)(aggregate_coefficient / 100)
                                              : (int32_t)(aggregate_coefficient /
                                                          1000000);
    ++capture->calls;
    if (capture->mock->cancel_after_group_callbacks == capture->calls)
        capture->mock->cancelled = 1;
    return 0;
}

static int32_t on_group_v22_null_sum(
    void *context, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v21 *binding) {
    group_capture *capture = (group_capture *)context;
    int32_t key = 0;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V21_VERSION);
    assert(binding->base.having_type_code == PRDB_SDK_VALUE_TYPE_DECIMAL);
    assert(binding->base.having_node_count == 3);
    assert(binding->having_nodes[0].base.node_kind ==
           PRDB_SDK_SQL_PREDICATE_COMPARE);
    assert(binding->having_nodes[0].base.operand_kind ==
        PRDB_SDK_SQL_V21_OPERAND_NUMERIC_LITERAL);
    assert(binding->having_nodes[0].scale == 0 &&
           binding->having_nodes[0].coefficient[15] == 2);
    assert(binding->having_nodes[1].base.operand_kind ==
           PRDB_SDK_SQL_V21_OPERAND_NUMERIC_LITERAL);
    assert(binding->having_nodes[1].scale == 2 &&
           binding->having_nodes[1].coefficient[14] == 1 &&
           binding->having_nodes[1].coefficient[15] == 0xc2);
    assert(group_key_count == 2 && matching_row_count == 2);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &key) ==
           PRDB_SDK_VALUE_ACCESS_OK && key == 1);
    assert(group_keys[1].state == PRDB_SDK_VALUE_STATE_NULL);
    assert(aggregate_value != NULL &&
           aggregate_value->state == PRDB_SDK_VALUE_STATE_NULL &&
           aggregate_value->type_code == 0);
    capture->keys[capture->calls] = key;
    capture->aggregates[capture->calls] = -1;
    ++capture->calls;
    return 0;
}

static int32_t on_group_v22_hidden_count_avg(
    void *context, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v21 *binding) {
    group_capture *capture = (group_capture *)context;
    int32_t key = 0;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V21_VERSION);
    assert(binding->base.having_type_code == PRDB_SDK_VALUE_TYPE_DECIMAL);
    assert(binding->base.having_decimal_scale == 8);
    assert(group_key_count == 2 && matching_row_count == 2);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &key) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(key == 2 && group_keys[1].state == PRDB_SDK_VALUE_STATE_VALUE &&
           group_keys[1].type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
           group_keys[1].data_len == 16 && group_keys[1].data[14] == 1 &&
           group_keys[1].data[15] == 44);
    assert(aggregate_value != NULL &&
           aggregate_value->state == PRDB_SDK_VALUE_STATE_VALUE &&
           aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
           aggregate_value->data_len == 16);
    for (uint32_t index = 0; index < 12; ++index)
        assert(aggregate_value->data[index] == 0);
    assert(aggregate_value->data[12] == 0x11 &&
           aggregate_value->data[13] == 0xe1 &&
           aggregate_value->data[14] == 0xa3 &&
           aggregate_value->data[15] == 0x00);
    capture->keys[capture->calls] = key;
    capture->aggregates[capture->calls] = 3;
    ++capture->calls;
    return 0;
}

static int32_t on_group_v22_hidden_count_minmax(
    void *context, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v21 *binding) {
    group_capture *capture = (group_capture *)context;
    int32_t key = 0;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V21_VERSION);
    assert(binding->base.having_type_code == PRDB_SDK_VALUE_TYPE_DECIMAL);
    assert(binding->base.having_decimal_scale == 2);
    assert(group_key_count == 2 && matching_row_count == 2);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &key) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(key == 2 && group_keys[1].state == PRDB_SDK_VALUE_STATE_VALUE &&
           group_keys[1].type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
           group_keys[1].data_len == 16 && group_keys[1].data[14] == 1 &&
           group_keys[1].data[15] == 44);
    assert(aggregate_value != NULL &&
           aggregate_value->state == PRDB_SDK_VALUE_STATE_VALUE &&
           aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
           aggregate_value->data_len == 16);
    for (uint32_t index = 0; index < 14; ++index)
        assert(aggregate_value->data[index] == 0);
    assert(aggregate_value->data[14] == 1 &&
           aggregate_value->data[15] == 44);
    capture->keys[capture->calls] = key;
    capture->aggregates[capture->calls] = 3;
    ++capture->calls;
    return 0;
}

static int32_t on_group_v22_where_and_having(
    void *context, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v21 *binding) {
    group_capture *capture = (group_capture *)context;
    int32_t id = 0;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V21_VERSION);
    assert(binding->base.having_node_count == 3);
    assert(binding->base.having_decimal_scale == 2);
    assert(group_key_count == 2 && matching_row_count == 1);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &id) ==
           PRDB_SDK_VALUE_ACCESS_OK && id == 2);
    assert(group_keys[1].state == PRDB_SDK_VALUE_STATE_VALUE &&
           group_keys[1].type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
           group_keys[1].data_len == 16 && group_keys[1].data[14] == 1 &&
           group_keys[1].data[15] == 0xf4);
    assert(aggregate_value != NULL &&
           aggregate_value->state == PRDB_SDK_VALUE_STATE_VALUE &&
           aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
           aggregate_value->data_len == 16 && aggregate_value->data[14] == 1 &&
           aggregate_value->data[15] == 0xf4);
    capture->keys[capture->calls] = id;
    capture->aggregates[capture->calls] = 5;
    capture->row_counts[capture->calls] = matching_row_count;
    ++capture->calls;
    return 0;
}

static int32_t on_group_v21_min_decimal(
    void *context, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v21 *binding) {
    group_capture *capture = (group_capture *)context;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V21_VERSION);
    assert(binding->base.having_type_code == PRDB_SDK_VALUE_TYPE_DECIMAL);
    assert(binding->base.having_decimal_scale == 2);
    assert(binding->base.having_node_count == 1);
    assert(binding->having_nodes[0].scale == 2);
    assert(binding->having_nodes[0].base.operand_kind ==
           PRDB_SDK_SQL_V21_OPERAND_NUMERIC_LITERAL);
    assert(binding->having_nodes[0].coefficient[14] == 1 &&
           binding->having_nodes[0].coefficient[15] == 44);
    assert(group_key_count == 2 && aggregate_value != NULL);
    assert(matching_row_count == 1 && capture->mock->batch_finished != 0);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0],
                                         &capture->keys[capture->calls]) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(group_keys[1].type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
           group_keys[1].data_len == 16 && group_keys[1].data[14] == 1 &&
           group_keys[1].data[15] == 44);
    assert(aggregate_value->state == PRDB_SDK_VALUE_STATE_VALUE &&
           aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
           aggregate_value->data_len == 16);
    for (uint32_t index = 0; index < 14; ++index)
        assert(aggregate_value->data[index] == 0);
    assert(aggregate_value->data[14] == 1 &&
           aggregate_value->data[15] == 44);
    capture->aggregates[capture->calls] = 300;
    ++capture->calls;
    return 0;
}

static int32_t on_group_v21_max_decimal(
    void *context, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v21 *binding) {
    group_capture *capture = (group_capture *)context;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V21_VERSION);
    assert(binding->base.having_type_code == PRDB_SDK_VALUE_TYPE_DECIMAL);
    assert(binding->base.having_decimal_precision == 10);
    assert(binding->base.having_decimal_scale == 2);
    assert(binding->base.having_node_count == 1);
    assert(binding->having_nodes[0].scale == 2);
    assert(binding->having_nodes[0].base.operand_kind ==
           PRDB_SDK_SQL_V21_OPERAND_NUMERIC_LITERAL);
    assert(binding->having_nodes[0].coefficient[14] == 3 &&
           binding->having_nodes[0].coefficient[15] == 32);
    assert(group_key_count == 2 && aggregate_value != NULL);
    assert(matching_row_count == 1 && capture->mock->batch_finished != 0);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0],
                                         &capture->keys[capture->calls]) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(group_keys[1].type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
           group_keys[1].data_len == 16 && group_keys[1].data[14] == 3 &&
           group_keys[1].data[15] == 132);
    assert(aggregate_value->state == PRDB_SDK_VALUE_STATE_VALUE &&
           aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
           aggregate_value->data_len == 16);
    for (uint32_t index = 0; index < 14; ++index)
        assert(aggregate_value->data[index] == 0);
    assert(aggregate_value->data[14] == 3 &&
           aggregate_value->data[15] == 132);
    capture->aggregates[capture->calls] = 900;
    ++capture->calls;
    return 0;
}

