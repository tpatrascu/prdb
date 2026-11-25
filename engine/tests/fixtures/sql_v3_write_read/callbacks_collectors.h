static int32_t observe_range_executor_request(
    void *context, uint16_t operation, const uint8_t *request,
    uint64_t request_len, uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len) {
    if (range_observer_original_host == NULL ||
        context != range_observer_original_host->v3.context ||
        range_observer_original_host->v3.executor_request == NULL) {
        return PRDB_QUERY_CALLBACK_INVALID_ARGUMENT;
    }
    if (operation == PRDB_IPC_OP_SCAN_INDEX_RANGE) {
        ++range_observer_scan_requests;
    } else if (operation == PRDB_IPC_OP_OPEN_SCAN) {
        ++range_observer_open_scan_requests;
    } else if (operation == PRDB_IPC_OP_SCAN_INDEX_RANGES) {
        ++range_observer_scan_ranges_requests;
    }
    return range_observer_original_host->v3.executor_request(
        context, operation, request, request_len, response,
        response_capacity, response_len);
}

static int32_t cancel_range_observer_executor_request(
    void *context, uint16_t operation, const uint8_t *request,
    uint64_t request_len, uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len) {
    if (range_observer_original_host == NULL ||
        context != range_observer_original_host->v3.context ||
        response == NULL || response_len == NULL) {
        return PRDB_QUERY_CALLBACK_INVALID_ARGUMENT;
    }
    if (operation == PRDB_SDK_EXECUTOR_OP_CHECK_CANCELLATION) {
        if (response_capacity <
            PRDB_EXECUTOR_CANCELLATION_CHECK_RESPONSE_HEADER_BYTES) {
            return PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL;
        }
        ++range_observer_cancellation_checks;
        prdb_encode_executor_cancellation_check_response(response, 1);
        *response_len = PRDB_EXECUTOR_CANCELLATION_CHECK_RESPONSE_HEADER_BYTES;
        return PRDB_QUERY_CALLBACK_OK;
    }
    return observe_range_executor_request(
        context, operation, request, request_len, response, response_capacity,
        response_len);
}

static int sleep_without_signal_interruption(struct timespec interval) {
    struct timespec remaining = interval;
    struct timespec next;
    while (nanosleep(&remaining, &next) != 0) {
        if (errno != EINTR) return -1;
        remaining = next;
    }
    return 0;
}

static int monotonic_time_ns(uint64_t *value_out) {
    struct timespec value;
    if (value_out == NULL || clock_gettime(CLOCK_MONOTONIC, &value) != 0 ||
        value.tv_sec < 0 || value.tv_nsec < 0) {
        return 0;
    }
    *value_out = (uint64_t)value.tv_sec * UINT64_C(1000000000) +
                 (uint64_t)value.tv_nsec;
    return 1;
}

static int32_t report_range_timing(const prdb_host_api_v3 *host, uint8_t kind,
                                   uint64_t elapsed_ns) {
    uint8_t payload[16] = {'O', 'P', '5', 'R', 'N', 'G', '1', kind};
    if (host == NULL || host->write_result == NULL) {
        return PRDB_QUERY_CALLBACK_INVALID_ARGUMENT;
    }
    for (uint32_t index = 0; index < 8; ++index) {
        payload[8 + index] = (uint8_t)(elapsed_ns >> (56 - index * 8));
    }
    return host->write_result(host->context, payload, sizeof(payload)) == 0
               ? PRDB_QUERY_CALLBACK_OK
               : PRDB_QUERY_CALLBACK_IO_ERROR;
}

static int32_t report_indexed_join_timing(const prdb_host_api_v3 *host,
                                          uint8_t kind,
                                          uint64_t elapsed_ns) {
    uint8_t payload[16] = {'O', 'P', '1', '0', 'J', '0', '1', kind};
    if (host == NULL || host->write_result == NULL) {
        return PRDB_QUERY_CALLBACK_INVALID_ARGUMENT;
    }
    for (uint32_t index = 0; index < 8; ++index) {
        payload[8 + index] = (uint8_t)(elapsed_ns >> (56 - index * 8));
    }
    return host->write_result(host->context, payload, sizeof(payload)) == 0
               ? PRDB_QUERY_CALLBACK_OK
               : PRDB_QUERY_CALLBACK_IO_ERROR;
}

static int32_t cancel_update_executor_request(
    void *context, uint16_t operation, const uint8_t *request,
    uint64_t request_len, uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len) {
    if (cancel_update_original_host == NULL ||
        context != cancel_update_original_host->v4.v3.context ||
        cancel_update_original_host->v4.v3.executor_request == NULL ||
        cancel_update_original_host->v4.v3.write_result == NULL ||
        response == NULL || response_len == NULL) {
        return PRDB_QUERY_CALLBACK_INVALID_ARGUMENT;
    }
    if ((operation == PRDB_IPC_OP_STAGE_UPDATE ||
         operation == PRDB_IPC_OP_STAGE_UPDATES) &&
        fail_next_stage_update != 0) {
        if (fail_next_stage_update > 1) {
            static const uint8_t accepted[] = "UPDATE_STAGE_BATCH_ACCEPTED";
            --fail_next_stage_update;
            int32_t status = cancel_update_original_host->v4.v3.executor_request(
                context, operation, request, request_len, response,
                response_capacity, response_len);
            if (status != PRDB_QUERY_CALLBACK_OK ||
                cancel_update_original_host->v4.v3.write_result(
                    context, accepted, sizeof(accepted) - 1) != 0) {
                return PRDB_QUERY_CALLBACK_IO_ERROR;
            }
            return status;
        }
        static const uint8_t injected[] = "UPDATE_STAGE_FAILURE_INJECTED";
        fail_next_stage_update = 0;
        (void)cancel_update_original_host->v4.v3.write_result(
            context, injected, sizeof(injected) - 1);
        return PRDB_QUERY_CALLBACK_IO_ERROR;
    }
    if ((operation == PRDB_IPC_OP_STAGE_UPDATE ||
         operation == PRDB_IPC_OP_STAGE_UPDATES) &&
        observe_next_stage_update != 0) {
        static const uint8_t reached[] = "UPDATE_STAGE_REACHED";
        observe_next_stage_update = 0;
        if (cancel_update_original_host->v4.v3.write_result(
                context, reached, sizeof(reached) - 1) != 0) {
            return PRDB_QUERY_CALLBACK_IO_ERROR;
        }
    }
    if (observe_only_stage_update != 0) {
        int32_t status = cancel_update_original_host->v4.v3.executor_request(
            context, operation, request, request_len, response,
            response_capacity, response_len);
        if (operation == PRDB_IPC_OP_STAGE_UPDATE ||
            operation == PRDB_IPC_OP_STAGE_UPDATES) {
            static const uint8_t returned[] = "UPDATE_STAGE_RETURNED";
            if (cancel_update_original_host->v4.v3.write_result(
                    context, returned, sizeof(returned) - 1) != 0) {
                return PRDB_QUERY_CALLBACK_IO_ERROR;
            }
        }
        return status;
    }
    if (operation != PRDB_IPC_OP_EXECUTOR_CHECK_CANCELLATION ||
        cancel_update_ready_sent != 0) {
        return cancel_update_original_host->v4.v3.executor_request(
            context, operation, request, request_len, response,
            response_capacity, response_len);
    }

    static const uint8_t ready[] = "CANCEL_READY";
    static const uint8_t observed[] = "CANCEL_OBSERVED";
    if (cancel_update_original_host->v4.v3.write_result(
            context, ready, sizeof(ready) - 1) != 0) {
        return PRDB_QUERY_CALLBACK_IO_ERROR;
    }
    cancel_update_ready_sent = 1;
    const struct timespec poll_interval = {0, 100000000};
    for (uint32_t poll = 0; poll < 320; ++poll) {
        if (sleep_without_signal_interruption(poll_interval) != 0) {
            return PRDB_QUERY_CALLBACK_IO_ERROR;
        }
        int32_t status = cancel_update_original_host->v4.v3.executor_request(
            context, operation, request, request_len, response,
            response_capacity, response_len);
        if (status != PRDB_QUERY_CALLBACK_OK) return status;
        if (*response_len == 3 && response_capacity >= 3 && response[2] == 1) {
            return cancel_update_original_host->v4.v3.write_result(
                       context, observed, sizeof(observed) - 1) == 0
                       ? PRDB_QUERY_CALLBACK_OK
                       : PRDB_QUERY_CALLBACK_IO_ERROR;
        }
    }
    return PRDB_QUERY_CALLBACK_LIMIT_EXCEEDED;
}

typedef struct optimizer_result_context {
    uint32_t row_count;
    uint32_t matched_expected_value;
    uint32_t expected_value;
} optimizer_result_context;

typedef struct range_result_context {
    int32_t values[16];
    uint32_t count;
} range_result_context;

static int32_t decode_range_integer(const prdb_sdk_value_v1 *value,
                                    int32_t *decoded_out) {
    if (value == NULL || decoded_out == NULL ||
        value->state != PRDB_SDK_VALUE_STATE_VALUE ||
        value->type_code != PRDB_SDK_VALUE_TYPE_INTEGER ||
        value->data == NULL || value->data_len != 4) {
        return 0;
    }
    uint32_t bits = ((uint32_t)value->data[0] << 24) |
                    ((uint32_t)value->data[1] << 16) |
                    ((uint32_t)value->data[2] << 8) | value->data[3];
    memcpy(decoded_out, &bits, sizeof(bits));
    return 1;
}

static int32_t collect_optimizer_range_row(
    void *opaque, const prdb_sdk_optimizer_plan_column_v1 *projections,
    const prdb_sdk_value_v1 *values, uint32_t projection_count) {
    range_result_context *context = opaque;
    int32_t value;
    if (context == NULL || projections == NULL || values == NULL ||
        projection_count != 1 || context->count >= 16 ||
        !decode_range_integer(&values[0], &value)) {
        return 1;
    }
    context->values[context->count++] = value;
    return PRDB_QUERY_CALLBACK_OK;
}

static int32_t collect_reference_range_row(
    void *opaque, const prdb_sdk_row_v1 *row,
    const prdb_sdk_select_binding_v2 *binding) {
    range_result_context *context = opaque;
    prdb_sdk_cell_iterator_v1 cells = {0};
    prdb_sdk_value_v1 value = {0};
    int32_t decoded;
    if (context == NULL || row == NULL || binding == NULL ||
        binding->projection_count != 1 ||
        binding->projection_ordinals[0] >= row->cell_count ||
        context->count >= 16 ||
        prdb_sdk_row_cells_begin_v1(row, &cells, sizeof(cells)) !=
            PRDB_SDK_BATCH_OK ||
        prdb_sdk_cell_next_v1(&cells, &value, sizeof(value)) !=
            PRDB_SDK_BATCH_OK) {
        return 1;
    }
    for (uint32_t ordinal = 0; ordinal < binding->projection_ordinals[0];
         ++ordinal) {
        if (prdb_sdk_cell_next_v1(&cells, &value, sizeof(value)) !=
            PRDB_SDK_BATCH_OK) {
            return 1;
        }
    }
    if (!decode_range_integer(&value, &decoded)) return 1;
    context->values[context->count++] = decoded;
    return PRDB_QUERY_CALLBACK_OK;
}

typedef struct ordered_optimizer_result_context {
    const prdb_host_api_v3 *host;
    const prdb_sdk_sql_column_shape_v2 *columns;
    const prdb_sdk_executor_relation_v1 *relation;
    uint8_t *record_buffer;
    uint64_t record_capacity;
    uint32_t row_count;
    uint32_t expected_values[2];
} ordered_optimizer_result_context;

static int32_t verify_optimizer_row(
    void *opaque, const prdb_sdk_optimizer_plan_column_v1 *projections,
    const prdb_sdk_value_v1 *values, uint32_t projection_count) {
    optimizer_result_context *context = (optimizer_result_context *)opaque;
    uint8_t expected_value[4] = {0, 0, 0, 42};
    if (context == NULL || projections == NULL || values == NULL ||
        projection_count != 1 ||
        projections[0].type_code != PRDB_SDK_VALUE_TYPE_INTEGER ||
        values[0].state != PRDB_SDK_VALUE_STATE_VALUE ||
        values[0].type_code != PRDB_SDK_VALUE_TYPE_INTEGER ||
        values[0].data == NULL || values[0].data_len != sizeof(expected_value)) {
        return 1;
    }
    if (context->expected_value != 0) {
        expected_value[0] = (uint8_t)(context->expected_value >> 24);
        expected_value[1] = (uint8_t)(context->expected_value >> 16);
        expected_value[2] = (uint8_t)(context->expected_value >> 8);
        expected_value[3] = (uint8_t)context->expected_value;
    }
    context->matched_expected_value =
        memcmp(values[0].data, expected_value, sizeof(expected_value)) == 0;
    ++context->row_count;
    return context->matched_expected_value ? 0 : 1;
}

static int32_t emit_ordered_optimizer_row(
    void *opaque, const prdb_sdk_optimizer_plan_column_v1 *projections,
    const prdb_sdk_value_v1 *values, uint32_t projection_count) {
    ordered_optimizer_result_context *context = opaque;
    uint32_t ordinals[PRDB_SDK_OPTIMIZER_PLAN_MAX_PROJECTIONS];
    uint8_t expected_bytes[4];
    uint64_t record_len = 0;
    uint32_t expected_value;
    if (context != NULL && context->row_count < 2) {
        expected_value = context->expected_values[context->row_count];
        expected_bytes[0] = (uint8_t)(expected_value >> 24);
        expected_bytes[1] = (uint8_t)(expected_value >> 16);
        expected_bytes[2] = (uint8_t)(expected_value >> 8);
        expected_bytes[3] = (uint8_t)expected_value;
    } else {
        memset(expected_bytes, 0, sizeof(expected_bytes));
    }
    if (context == NULL || context->host == NULL || context->columns == NULL ||
        context->relation == NULL || projections == NULL || values == NULL ||
        projection_count != 1 || context->row_count >= 2 ||
        values[0].state != PRDB_SDK_VALUE_STATE_VALUE ||
        values[0].type_code != PRDB_SDK_VALUE_TYPE_INTEGER ||
        values[0].data == NULL || values[0].data_len != 4 ||
        memcmp(values[0].data, expected_bytes, sizeof(expected_bytes)) != 0) {
        return -1;
    }
    ordinals[0] = projections[0].ordinal;
    if (prdb_sdk_sql_write_projected_result_v1(
            context->host, context->columns, context->relation->column_count,
            ordinals, values, projection_count, context->record_buffer,
            context->record_capacity, &record_len) != PRDB_SDK_SQL_RESULT_OK) {
        return -1;
    }
    ++context->row_count;
    return 0;
}

typedef struct result_context {
    const prdb_host_api_v3 *host;
    uint32_t expected_state;
    const uint8_t *expected_value;
    uint64_t expected_value_len;
    const uint8_t *marker;
    uint64_t marker_len;
    uint32_t row_count;
    uint32_t expected_ordinal;
    uint32_t failure_code;
} result_context;

typedef struct grouped_result_context {
    const prdb_host_api_v3 *host;
    const prdb_sdk_sql_column_shape_v2 *group_column;
    const prdb_sdk_sql_column_shape_v2 *aggregate_column;
    uint8_t *output;
    uint64_t output_capacity;
    uint64_t output_len;
    uint32_t row_count;
} grouped_result_context;

typedef struct analytical_pressure_result_context {
    const prdb_host_api_v3 *host;
    uint8_t seen[64];
    uint32_t group_count;
} analytical_pressure_result_context;

static int32_t verify_analytical_pressure_sum(
    void *opaque, const prdb_sdk_value_v1 *group_key,
    const prdb_sdk_value_v1 *aggregate_value, uint64_t matching_row_count,
    const prdb_sdk_select_binding_v6 *binding) {
    analytical_pressure_result_context *context = opaque;
    static const uint8_t started[] = "ANALYTICAL_GROUP_STARTED";
    static const uint8_t resumed[] = "ANALYTICAL_GROUP_RESUMED";
    int64_t sum_value = 0;
    uint32_t index;
    if (context == NULL || context->host == NULL || group_key == NULL ||
        aggregate_value == NULL || binding == NULL ||
        binding->aggregate_kind != PRDB_SDK_SQL_V6_AGGREGATE_SUM ||
        binding->group_by_column.type_code != PRDB_SDK_VALUE_TYPE_TEXT ||
        binding->aggregate_column.type_code != PRDB_SDK_VALUE_TYPE_INTEGER ||
        group_key->state != PRDB_SDK_VALUE_STATE_VALUE ||
        group_key->type_code != PRDB_SDK_VALUE_TYPE_TEXT ||
        group_key->data == NULL || group_key->data_len != 8172 ||
        matching_row_count != 2 ||
        aggregate_value->state != PRDB_SDK_VALUE_STATE_VALUE ||
        aggregate_value->type_code != PRDB_SDK_VALUE_TYPE_BIGINT ||
        prdb_sdk_value_get_bigint_v1(aggregate_value, &sum_value) !=
            PRDB_SDK_VALUE_ACCESS_OK) {
        return 1;
    }
    if (group_key->data[0] < '0' || group_key->data[0] > '9' ||
        group_key->data[1] < '0' || group_key->data[1] > '9' ||
        group_key->data[2] < '0' || group_key->data[2] > '9' ||
        group_key->data[3] < '0' || group_key->data[3] > '9') {
        return 1;
    }
    index = (uint32_t)(group_key->data[0] - '0') * 1000 +
            (uint32_t)(group_key->data[1] - '0') * 100 +
            (uint32_t)(group_key->data[2] - '0') * 10 +
            (uint32_t)(group_key->data[3] - '0');
    if (index >= 64 || context->seen[index] != 0 ||
        sum_value != 300 + (int64_t)index * 2) {
        return 1;
    }
    for (uint32_t offset = 4; offset < 8172; ++offset) {
        if (group_key->data[offset] != 'a') {
            return 1;
        }
    }
    context->seen[index] = 1;
    ++context->group_count;
    if (context->group_count == 1 &&
        context->host->write_result(context->host->context, started,
                                    sizeof(started) - 1) != PRDB_QUERY_CALLBACK_OK) {
        return 1;
    }
    if (context->group_count == 2 &&
        context->host->write_result(context->host->context, resumed,
                                    sizeof(resumed) - 1) != PRDB_QUERY_CALLBACK_OK) {
        return 1;
    }
    if (context->group_count == 2) {
        /* Keep the eighth analytical slot active through the bounded mixed-load window. */
        const struct timespec saturation_interval = {31, 0};
        if (sleep_without_signal_interruption(saturation_interval) != 0) {
            return 1;
        }
    }
    /* Pace callbacks so concurrent operational and DDL work can progress. */
    const struct timespec callback_interval = {0, 1000000};
    if (sleep_without_signal_interruption(callback_interval) != 0) {
        return 1;
    }
    return 0;
}

typedef struct joined_result_context {
    const prdb_host_api_v3 *host;
    const prdb_sdk_sql_column_shape_v2 *left_columns;
    const prdb_sdk_sql_column_shape_v2 *right_columns;
    uint8_t *output;
    uint64_t output_capacity;
    uint64_t output_len;
    uint32_t row_count;
} joined_result_context;

typedef struct indexed_join_benchmark_result_context {
    uint32_t row_count;
    int64_t value_checksum;
    uint8_t asymmetric_values;
} indexed_join_benchmark_result_context;

typedef struct composite_grouped_result_context {
    const prdb_host_api_v3 *host;
    const prdb_sdk_sql_column_shape_v2 *columns;
    uint32_t column_count;
    uint8_t *output;
    uint64_t output_capacity;
    uint64_t output_len;
    uint32_t row_count;
} composite_grouped_result_context;

typedef struct composite_aggregate_expectation {
    composite_grouped_result_context *results;
    uint32_t aggregate_kind;
} composite_aggregate_expectation;

typedef struct ordered_composite_aggregate_expectation {
    composite_aggregate_expectation aggregate;
    uint32_t next_group;
    int32_t first_left_key;
    int32_t first_right_key;
    int32_t second_left_key;
    int32_t second_right_key;
    int32_t third_left_key;
    int32_t third_right_key;
    uint64_t first_matching_row_count;
    uint64_t second_matching_row_count;
    uint64_t third_matching_row_count;
    uint32_t expected_group_count;
} ordered_composite_aggregate_expectation;

static int32_t decimal_value_matches_coefficient(
    const prdb_sdk_value_v1 *value, uint64_t expected_coefficient) {
    uint64_t observed_coefficient = 0;
    if (value == NULL || value->state != PRDB_SDK_VALUE_STATE_VALUE ||
        value->type_code != PRDB_SDK_VALUE_TYPE_DECIMAL ||
        value->data == NULL || value->data_len != 16) {
        return 0;
    }
    for (uint32_t index = 0; index < 8; ++index) {
        if (value->data[index] != 0) return 0;
    }
    for (uint32_t index = 8; index < 16; ++index) {
        observed_coefficient = (observed_coefficient << 8) | value->data[index];
    }
    return observed_coefficient == expected_coefficient;
}

static int32_t write_grouped_composite_aggregate_optimizer(
    void *opaque, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v13 *binding) {
    composite_aggregate_expectation *expectation = opaque;
    composite_grouped_result_context *context;
    int32_t left_key, right_key, aggregate_integer = 0;
    if (expectation == NULL || expectation->results == NULL || binding == NULL ||
        group_keys == NULL ||
        (expectation->aggregate_kind !=
             PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR &&
         aggregate_value == NULL)) {
        return 1;
    }
    if (group_key_count != 2 || binding->group_key_count != 2) {
        return 1;
    }
    if (binding->base.aggregate_kind != expectation->aggregate_kind) {
        return 1;
    }
    if (!decode_range_integer(&group_keys[0], &left_key) ||
        !decode_range_integer(&group_keys[1], &right_key)) {
        return 1;
    }
    if (expectation->aggregate_kind ==
        PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
        int64_t expected_count = left_key == 1 ? 2 : left_key == 2 ? 3 : 1;
        if ((left_key == 1 && right_key != 7) ||
            (left_key == 2 && right_key != 9) ||
            (left_key == 3 && right_key != 11) ||
            (left_key != 1 && left_key != 2 && left_key != 3) ||
            matching_row_count != (uint64_t)expected_count ||
            aggregate_value != NULL) {
            return 1;
        }
    } else if (binding->base.aggregate_column.type_code ==
        PRDB_SDK_VALUE_TYPE_DECIMAL) {
        uint64_t expected_coefficient;
        if (expectation->aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_MIN) {
            expected_coefficient = 100;
        } else if (expectation->aggregate_kind ==
                   PRDB_SDK_SQL_V7_AGGREGATE_MAX) {
            expected_coefficient = left_key == 1 ? 100 : 225;
        } else if (expectation->aggregate_kind ==
                   PRDB_SDK_SQL_V7_AGGREGATE_SUM) {
            expected_coefficient = left_key == 1 ? 100 : 450;
        } else if (expectation->aggregate_kind ==
                   PRDB_SDK_SQL_V7_AGGREGATE_AVG) {
            expected_coefficient = left_key == 1 ? 100000000 : 150000000;
        } else {
            return 1;
        }
        if (binding->base.aggregate_column.decimal_precision != 4 ||
            binding->base.aggregate_column.decimal_scale != 2 ||
            (left_key == 1 && (right_key != 7 || matching_row_count != 2)) ||
            (left_key == 2 && (right_key != 9 || matching_row_count != 3)) ||
            (left_key == 3 &&
             (right_key != 11 || matching_row_count != 1 ||
              aggregate_value->state != PRDB_SDK_VALUE_STATE_NULL ||
              aggregate_value->type_code != 0 ||
              aggregate_value->data != NULL || aggregate_value->data_len != 0)) ||
            (left_key != 1 && left_key != 2 && left_key != 3) ||
            (left_key != 3 &&
             !decimal_value_matches_coefficient(aggregate_value,
                                                expected_coefficient))) {
            return 1;
        }
    } else if (binding->base.aggregate_column.type_code ==
               PRDB_SDK_VALUE_TYPE_BIGINT) {
        uint64_t expected_coefficient;
        if ((left_key == 1 && (right_key != 7 || matching_row_count != 2)) ||
            (left_key == 2 && (right_key != 9 || matching_row_count != 3)) ||
            (left_key == 3 && (right_key != 11 || matching_row_count != 1)) ||
            (left_key != 1 && left_key != 2 && left_key != 3)) {
            return 1;
        }
        if (left_key == 3) {
            if (aggregate_value->state != PRDB_SDK_VALUE_STATE_NULL ||
                aggregate_value->type_code != 0 || aggregate_value->data != NULL ||
                aggregate_value->data_len != 0) {
                return 1;
            }
        } else if (expectation->aggregate_kind ==
                   PRDB_SDK_SQL_V7_AGGREGATE_SUM) {
            int64_t observed_sum = 0;
            int64_t expected_sum = left_key == 1 ? 2 : 27;
            if (aggregate_value->type_code != PRDB_SDK_VALUE_TYPE_BIGINT ||
                prdb_sdk_value_get_bigint_v1(aggregate_value, &observed_sum) !=
                    PRDB_SDK_VALUE_ACCESS_OK ||
                observed_sum != expected_sum) {
                return 1;
            }
        } else if (expectation->aggregate_kind ==
                   PRDB_SDK_SQL_V7_AGGREGATE_AVG) {
            expected_coefficient = left_key == 1 ? 2000000 : 9000000;
            if (!decimal_value_matches_coefficient(aggregate_value,
                                                   expected_coefficient)) {
                return 1;
            }
        } else if (expectation->aggregate_kind ==
                       PRDB_SDK_SQL_V7_AGGREGATE_MIN ||
                   expectation->aggregate_kind ==
                       PRDB_SDK_SQL_V7_AGGREGATE_MAX) {
            int64_t observed_value = 0;
            int64_t expected_value;
            if (expectation->aggregate_kind ==
                PRDB_SDK_SQL_V7_AGGREGATE_MIN) {
                expected_value = left_key == 1 ? 2 : 6;
            } else {
                expected_value = left_key == 1 ? 2 : 12;
            }
            if (aggregate_value->type_code != PRDB_SDK_VALUE_TYPE_BIGINT ||
                prdb_sdk_value_get_bigint_v1(aggregate_value,
                                             &observed_value) !=
                    PRDB_SDK_VALUE_ACCESS_OK ||
                observed_value != expected_value) {
                return 1;
            }
        } else {
            return 1;
        }
    } else if (binding->base.aggregate_column.type_code ==
               PRDB_SDK_VALUE_TYPE_SMALLINT) {
        if ((left_key == 1 && (right_key != 7 || matching_row_count != 2)) ||
            (left_key == 2 && (right_key != 9 || matching_row_count != 3)) ||
            (left_key == 3 && (right_key != 11 || matching_row_count != 1)) ||
            (left_key != 1 && left_key != 2 && left_key != 3)) {
            return 1;
        }
        if (left_key == 3) {
            if (aggregate_value->state != PRDB_SDK_VALUE_STATE_NULL ||
                aggregate_value->type_code != 0 || aggregate_value->data != NULL ||
                aggregate_value->data_len != 0) {
                return 1;
            }
        } else if (expectation->aggregate_kind ==
                   PRDB_SDK_SQL_V7_AGGREGATE_SUM) {
            int64_t observed_sum = 0;
            int64_t expected_sum = left_key == 1 ? 6 : 27;
            if (aggregate_value->type_code != PRDB_SDK_VALUE_TYPE_BIGINT ||
                prdb_sdk_value_get_bigint_v1(aggregate_value, &observed_sum) !=
                    PRDB_SDK_VALUE_ACCESS_OK ||
                observed_sum != expected_sum) {
                return 1;
            }
        } else if (expectation->aggregate_kind ==
                       PRDB_SDK_SQL_V7_AGGREGATE_AVG) {
            uint64_t expected_coefficient =
                left_key == 1 ? 3000000 : 9000000;
            if (!decimal_value_matches_coefficient(aggregate_value,
                                                   expected_coefficient)) {
                return 1;
            }
        } else if (expectation->aggregate_kind ==
                       PRDB_SDK_SQL_V7_AGGREGATE_MIN ||
                   expectation->aggregate_kind ==
                       PRDB_SDK_SQL_V7_AGGREGATE_MAX) {
            int16_t observed_value = 0;
            int16_t expected_value;
            if (expectation->aggregate_kind ==
                PRDB_SDK_SQL_V7_AGGREGATE_MIN) {
                expected_value = left_key == 1 ? 2 : 6;
            } else {
                expected_value = left_key == 1 ? 4 : 12;
            }
            if (aggregate_value->type_code != PRDB_SDK_VALUE_TYPE_SMALLINT ||
                prdb_sdk_value_get_smallint_v1(aggregate_value,
                                               &observed_value) !=
                    PRDB_SDK_VALUE_ACCESS_OK ||
                observed_value != expected_value) {
                return 1;
            }
        } else {
            return 1;
        }
    } else if (binding->base.aggregate_column.type_code ==
                   PRDB_SDK_VALUE_TYPE_INTEGER &&
               binding->base.aggregate_column.ordinal == 4) {
        if ((left_key == 1 && (right_key != 7 || matching_row_count != 2)) ||
            (left_key == 2 && (right_key != 9 || matching_row_count != 3)) ||
            (left_key == 3 && (right_key != 11 || matching_row_count != 1)) ||
            (left_key != 1 && left_key != 2 && left_key != 3)) {
            return 1;
        }
        if (left_key == 3) {
            if (aggregate_value->state != PRDB_SDK_VALUE_STATE_NULL ||
                aggregate_value->type_code != 0 || aggregate_value->data != NULL ||
                aggregate_value->data_len != 0) {
                return 1;
            }
        } else if (expectation->aggregate_kind ==
                   PRDB_SDK_SQL_V7_AGGREGATE_AVG) {
            uint64_t expected_coefficient = left_key == 1 ? 2500000 : 9000000;
            if (!decimal_value_matches_coefficient(aggregate_value,
                                                   expected_coefficient)) {
                return 1;
            }
        } else if (expectation->aggregate_kind ==
                       PRDB_SDK_SQL_V7_AGGREGATE_MIN ||
                   expectation->aggregate_kind ==
                       PRDB_SDK_SQL_V7_AGGREGATE_MAX) {
            int32_t observed_value = 0;
            int32_t expected_value =
                expectation->aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_MIN
                    ? (left_key == 1 ? 2 : 6)
                    : (left_key == 1 ? 3 : 12);
            if (aggregate_value->type_code != PRDB_SDK_VALUE_TYPE_INTEGER ||
                prdb_sdk_value_get_integer_v1(aggregate_value, &observed_value) !=
                    PRDB_SDK_VALUE_ACCESS_OK ||
                observed_value != expected_value) {
                return 1;
            }
        } else {
            return 1;
        }
    } else if (expectation->aggregate_kind !=
                   PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR &&
               ((left_key == 4 &&
                 (right_key != 9 || matching_row_count != 2)) ||
                (left_key == 2 &&
                 (right_key != 9 || matching_row_count != 3)) ||
                (left_key != 4 && left_key != 2))) {
        return 1;
    }
    if (binding->base.aggregate_column.type_code ==
            PRDB_SDK_VALUE_TYPE_INTEGER &&
        binding->base.aggregate_column.ordinal != 4 &&
        expectation->aggregate_kind != PRDB_SDK_SQL_V7_AGGREGATE_AVG &&
        (aggregate_value->state != PRDB_SDK_VALUE_STATE_VALUE ||
               aggregate_value->type_code != PRDB_SDK_VALUE_TYPE_INTEGER ||
               prdb_sdk_value_get_integer_v1(aggregate_value,
                                             &aggregate_integer) !=
                   PRDB_SDK_VALUE_ACCESS_OK ||
               aggregate_integer != left_key)) {
        return 1;
    }
    context = expectation->results;
    context->column_count = binding->base.aggregate_column.ordinal + 1;
    for (uint32_t key = 0; key < group_key_count; ++key) {
        uint32_t required = binding->group_keys[key].ordinal + 1;
        if (required > context->column_count) context->column_count = required;
    }
    if (prdb_sdk_sql_write_group_composite_result_v1(
            context->host, binding, context->columns, context->column_count,
            group_keys, group_key_count, aggregate_value, matching_row_count,
            context->output, context->output_capacity,
            &context->output_len) != PRDB_SDK_SQL_RESULT_OK) {
        return 1;
    }
    ++context->row_count;
    return 0;
}

static int32_t write_filtered_composite_decimal_sum_optimizer(
    void *opaque, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v13 *binding) {
    composite_grouped_result_context *context = opaque;
    int32_t left_key = 0, right_key = 0;
    if (context == NULL || binding == NULL || group_keys == NULL ||
        aggregate_value == NULL || group_key_count != 2 ||
        binding->group_key_count != 2 ||
        binding->base.aggregate_kind != PRDB_SDK_SQL_V7_AGGREGATE_SUM ||
        binding->base.aggregate_column.type_code !=
            PRDB_SDK_VALUE_TYPE_DECIMAL ||
        binding->base.aggregate_column.decimal_precision != 4 ||
        binding->base.aggregate_column.decimal_scale != 2 ||
        !decode_range_integer(&group_keys[0], &left_key) ||
        !decode_range_integer(&group_keys[1], &right_key) || left_key != 1 ||
        right_key != 7 || matching_row_count != 2 || context->row_count != 0 ||
        !decimal_value_matches_coefficient(aggregate_value, 100) ||
        prdb_sdk_sql_write_group_composite_result_v1(
            context->host, binding, context->columns, 3, group_keys,
            group_key_count, aggregate_value, matching_row_count,
            context->output, context->output_capacity,
            &context->output_len) != PRDB_SDK_SQL_RESULT_OK) {
        return 1;
    }
    ++context->row_count;
    return 0;
}

static int32_t write_filtered_ordered_composite_decimal_sum_v24(
    void *opaque, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v21 *binding) {
    composite_grouped_result_context *context = opaque;
    const prdb_sdk_select_binding_v13 *projection;
    const prdb_sdk_select_binding_v15 *ordering;
    int32_t left_key = 0, right_key = 0;
    if (context == NULL || binding == NULL || group_keys == NULL ||
        aggregate_value == NULL || group_key_count != 2 ||
        binding->base.having_node_count < 3 ||
        !decode_range_integer(&group_keys[0], &left_key) ||
        !decode_range_integer(&group_keys[1], &right_key) || left_key != 1 ||
        right_key != 7 || matching_row_count != 2 || context->row_count != 0 ||
        !decimal_value_matches_coefficient(aggregate_value, 100)) {
        return 1;
    }
    projection = &binding->base.base.base.base;
    ordering = &binding->base.base;
    if (projection->group_key_count != 2 ||
        projection->base.aggregate_kind != PRDB_SDK_SQL_V7_AGGREGATE_SUM ||
        projection->base.aggregate_column.type_code !=
            PRDB_SDK_VALUE_TYPE_DECIMAL ||
        projection->base.aggregate_column.decimal_precision != 4 ||
        projection->base.aggregate_column.decimal_scale != 2 ||
        ordering->order_count != 3 ||
        ordering->order_items[0].item.kind !=
            PRDB_SDK_SQL_V10_ORDER_AGGREGATE ||
        ordering->order_items[0].item.direction !=
            PRDB_SDK_SQL_V10_ORDER_DESCENDING ||
        ordering->order_items[0].item.null_order !=
            PRDB_SDK_SQL_V11_NULLS_LAST ||
        ordering->order_items[1].item.kind !=
            PRDB_SDK_SQL_V10_ORDER_GROUP_KEY ||
        ordering->order_items[1].item.group_key_index != 0 ||
        ordering->order_items[1].item.direction !=
            PRDB_SDK_SQL_V10_ORDER_ASCENDING ||
        ordering->order_items[1].item.null_order !=
            PRDB_SDK_SQL_V11_NULLS_FIRST ||
        ordering->order_items[2].item.kind !=
            PRDB_SDK_SQL_V10_ORDER_GROUP_KEY ||
        ordering->order_items[2].item.group_key_index != 1 ||
        ordering->order_items[2].item.direction !=
            PRDB_SDK_SQL_V10_ORDER_DESCENDING ||
        ordering->order_items[2].item.null_order !=
            PRDB_SDK_SQL_V11_NULLS_LAST) {
        return 1;
    }
    if (prdb_sdk_sql_write_group_composite_result_v1(
            context->host, projection, context->columns, 3, group_keys,
            group_key_count, aggregate_value, matching_row_count,
            context->output, context->output_capacity,
            &context->output_len) != PRDB_SDK_SQL_RESULT_OK) {
        return 1;
    }
    ++context->row_count;
    return 0;
}

static int32_t write_filtered_composite_text_min_optimizer(
    void *opaque, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v13 *binding) {
    composite_grouped_result_context *context = opaque;
    int32_t measure = 0;
    if (context == NULL || binding == NULL || group_keys == NULL ||
        aggregate_value == NULL || group_key_count != 2 ||
        binding->group_key_count != 2 ||
        binding->base.aggregate_kind != PRDB_SDK_SQL_V7_AGGREGATE_MIN ||
        binding->base.aggregate_column.type_code != PRDB_SDK_VALUE_TYPE_TEXT ||
        group_keys[0].state != PRDB_SDK_VALUE_STATE_VALUE ||
        group_keys[0].type_code != PRDB_SDK_VALUE_TYPE_TEXT ||
        group_keys[0].data == NULL || group_keys[0].data_len != 8172 ||
        group_keys[1].type_code != PRDB_SDK_VALUE_TYPE_INTEGER ||
        !decode_range_integer(&group_keys[1], &measure) || measure != 100 ||
        aggregate_value->state != PRDB_SDK_VALUE_STATE_VALUE ||
        aggregate_value->type_code != PRDB_SDK_VALUE_TYPE_TEXT ||
        aggregate_value->data == NULL ||
        aggregate_value->data_len != group_keys[0].data_len ||
        matching_row_count != 1 || context->row_count != 0) {
        return 1;
    }
    const uint8_t *key = group_keys[0].data;
    const uint8_t *minimum = aggregate_value->data;
    if (memcmp(key, "0000", 4) != 0 ||
        memcmp(key, minimum, group_keys[0].data_len) != 0) {
        return 1;
    }
    for (uint32_t index = 4; index < group_keys[0].data_len; ++index) {
        if (key[index] != 'a') return 1;
    }
    if (prdb_sdk_sql_write_group_composite_result_v1(
            context->host, binding, context->columns, 3, group_keys,
            group_key_count, aggregate_value, matching_row_count,
            context->output, context->output_capacity,
            &context->output_len) != PRDB_SDK_SQL_RESULT_OK) {
        return 1;
    }
    ++context->row_count;
    return 0;
}

static int32_t write_ordered_composite_group_aggregate_optimizer(
    void *opaque, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v15 *binding) {
    ordered_composite_aggregate_expectation *expectation = opaque;
    int32_t left_key = 0, right_key = 0;
    if (expectation == NULL || binding == NULL || group_keys == NULL ||
        group_key_count != 2 || binding->order_count != 2 ||
        binding->order_items[0].item.kind !=
            PRDB_SDK_SQL_V10_ORDER_AGGREGATE ||
        binding->order_items[0].item.direction !=
            PRDB_SDK_SQL_V10_ORDER_DESCENDING ||
        binding->order_items[1].item.kind !=
            PRDB_SDK_SQL_V10_ORDER_GROUP_KEY ||
        binding->order_items[1].item.group_key_index != 0 ||
        binding->order_items[0].item.null_order !=
            PRDB_SDK_SQL_V11_NULLS_LAST ||
        binding->order_items[1].item.direction !=
            PRDB_SDK_SQL_V10_ORDER_ASCENDING ||
        binding->order_items[1].item.null_order !=
            PRDB_SDK_SQL_V11_NULLS_FIRST ||
        !decode_range_integer(&group_keys[0], &left_key) ||
        !decode_range_integer(&group_keys[1], &right_key)) {
        return 1;
    }
    int32_t first_left = expectation->first_left_key;
    int32_t first_right = expectation->first_right_key;
    int32_t second_left = expectation->second_left_key;
    int32_t second_right = expectation->second_right_key;
    int32_t third_left = expectation->third_left_key;
    int32_t third_right = expectation->third_right_key;
    if ((expectation->next_group == 0 &&
         (left_key != first_left || right_key != first_right ||
          matching_row_count != expectation->first_matching_row_count)) ||
        (expectation->next_group == 1 &&
         (left_key != second_left || right_key != second_right ||
          matching_row_count != expectation->second_matching_row_count)) ||
        (expectation->next_group == 2 &&
         (left_key != third_left || right_key != third_right ||
          matching_row_count != expectation->third_matching_row_count)) ||
        expectation->next_group >= expectation->expected_group_count) {
        return 1;
    }
    ++expectation->next_group;
    return write_grouped_composite_aggregate_optimizer(
        &expectation->aggregate, group_keys, group_key_count, aggregate_value,
        matching_row_count, &binding->base.base);
}

static int32_t write_grouped_composite_sum_optimizer(
    void *opaque, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v13 *binding) {
    composite_grouped_result_context *context = opaque;
    int32_t left_key, right_key;
    uint64_t sum = 0;
    if (context == NULL || binding == NULL || group_keys == NULL ||
        aggregate_value == NULL || group_key_count != 2 ||
        binding->base.aggregate_kind != PRDB_SDK_SQL_V7_AGGREGATE_SUM ||
        binding->group_key_count != 2 ||
        aggregate_value->state != PRDB_SDK_VALUE_STATE_VALUE ||
        aggregate_value->type_code != PRDB_SDK_VALUE_TYPE_BIGINT ||
        aggregate_value->data == NULL || aggregate_value->data_len != 8 ||
        !decode_range_integer(&group_keys[0], &left_key) ||
        !decode_range_integer(&group_keys[1], &right_key)) {
        if (context != NULL) context->output_len = 101;
        return 1;
    }
    for (uint32_t index = 0; index < 8; ++index) {
        sum = (sum << 8) | aggregate_value->data[index];
    }
    if ((left_key == 1 && (right_key != 7 || matching_row_count != 2 || sum != 2)) ||
        (left_key == 2 && (right_key != 9 || matching_row_count != 3 || sum != 6)) ||
        (left_key != 1 && left_key != 2)) {
        return 1;
    }
    if (prdb_sdk_sql_write_group_composite_result_v1(
            context->host, binding, context->columns, context->column_count,
            group_keys, group_key_count, aggregate_value, matching_row_count,
            context->output, context->output_capacity,
            &context->output_len) != PRDB_SDK_SQL_RESULT_OK) {
        return 1;
    }
    ++context->row_count;
    return 0;
}

static int32_t write_grouped_composite_sum_optimizer_v24(
    void *opaque, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v21 *binding) {
    if (binding == NULL) return 1;
    return write_grouped_composite_sum_optimizer(
        opaque, group_keys, group_key_count, aggregate_value,
        matching_row_count, &binding->base.base.base.base);
}

static int32_t write_grouped_composite_count_column_optimizer_v24(
    void *opaque, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v21 *binding) {
    composite_grouped_result_context *context = opaque;
    const prdb_sdk_select_binding_v13 *projection;
    const prdb_sdk_select_binding_v15 *ordering;
    int32_t left_key = 0, right_key = 0;
    uint64_t count = 0;
    if (context == NULL || binding == NULL || group_keys == NULL ||
        aggregate_value == NULL || group_key_count != 2 ||
        aggregate_value->state != PRDB_SDK_VALUE_STATE_VALUE ||
        aggregate_value->type_code != PRDB_SDK_VALUE_TYPE_BIGINT ||
        aggregate_value->data == NULL || aggregate_value->data_len != 8 ||
        !decode_range_integer(&group_keys[0], &left_key) ||
        !decode_range_integer(&group_keys[1], &right_key)) {
        if (context != NULL) context->output_len = 101;
        return 1;
    }
    for (uint32_t index = 0; index < 8; ++index)
        count = (count << 8) | aggregate_value->data[index];
    if ((context->row_count == 0 &&
         (left_key != 2 || right_key != 9 || count != 3 ||
          matching_row_count != 3)) ||
        (context->row_count == 1 &&
         (left_key != 1 || right_key != 7 || count != 1 ||
          matching_row_count != 2)) ||
        (context->row_count == 2 &&
         (left_key != 3 || right_key != 11 || count != 0 ||
          matching_row_count != 1)) ||
        context->row_count >= 3) {
        context->output_len = 102;
        return 1;
    }
    projection = &binding->base.base.base.base;
    ordering = &binding->base.base;
    if (projection->group_key_count != 2 ||
        projection->base.aggregate_kind !=
            PRDB_SDK_SQL_V7_AGGREGATE_COUNT_COLUMN ||
        (projection->base.aggregate_column.type_code !=
             PRDB_SDK_VALUE_TYPE_DECIMAL &&
         projection->base.aggregate_column.type_code !=
             PRDB_SDK_VALUE_TYPE_BIGINT) ||
        ordering->order_count != 2 ||
        projection->base.base.predicate_count == 0 ||
        projection->base.base.predicate_columns[0].ordinal !=
            (projection->base.aggregate_column.type_code ==
                     PRDB_SDK_VALUE_TYPE_BIGINT
                 ? 3
                 : 2) ||
        ordering->order_items[0].item.kind !=
            PRDB_SDK_SQL_V10_ORDER_AGGREGATE ||
        ordering->order_items[1].item.kind !=
            PRDB_SDK_SQL_V10_ORDER_GROUP_KEY ||
        ordering->order_items[1].item.group_key_index != 0) {
        context->output_len = 103;
        return 1;
    }
    if (prdb_sdk_sql_write_group_composite_result_v1(
            context->host, projection, context->columns,
            projection->base.aggregate_column.type_code ==
                    PRDB_SDK_VALUE_TYPE_BIGINT
                ? 4
                : 3,
            group_keys,
            group_key_count, aggregate_value, matching_row_count,
            context->output, context->output_capacity,
            &context->output_len) != PRDB_SDK_SQL_RESULT_OK) {
        context->output_len = 104;
        return 1;
    }
    ++context->row_count;
    return 0;
}

static int32_t write_grouped_composite_count(
    void *opaque, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v13 *binding) {
    composite_grouped_result_context *context =
        (composite_grouped_result_context *)opaque;
    if (context == NULL || binding == NULL || aggregate_value != NULL ||
        binding->base.aggregate_kind !=
            PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR ||
        group_key_count != 2 || binding->group_key_count != 2 ||
        matching_row_count == 0) {
        return 1;
    }
    int32_t status = prdb_sdk_sql_write_group_composite_result_v1(
        context->host, binding, context->columns, context->column_count,
        group_keys, group_key_count, NULL, matching_row_count,
        context->output, context->output_capacity, &context->output_len);
    if (status != PRDB_SDK_SQL_RESULT_OK) return 1;
    ++context->row_count;
    return 0;
}

static int32_t write_grouped_composite_count_v15(
    void *opaque, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v15 *binding) {
    if (binding == NULL || binding->order_count != 3 ||
        binding->order_items[0].item.kind !=
            PRDB_SDK_SQL_V10_ORDER_GROUP_KEY ||
        binding->order_items[0].item.group_key_index != 1 ||
        binding->order_items[1].item.kind !=
            PRDB_SDK_SQL_V10_ORDER_AGGREGATE ||
        binding->order_items[2].item.kind !=
            PRDB_SDK_SQL_V10_ORDER_GROUP_KEY ||
        binding->order_items[2].item.group_key_index != 0) {
        return 1;
    }
    return write_grouped_composite_count(
        opaque, group_keys, group_key_count, aggregate_value,
        matching_row_count, &binding->base.base);
}

static int32_t write_grouped_composite_count_v17(
    void *opaque, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v17 *binding) {
    if (binding == NULL || binding->has_having_literal != 1 ||
        binding->having_literal != UINT64_C(1)) {
        return 1;
    }
    return write_grouped_composite_count(
        opaque, group_keys, group_key_count, aggregate_value,
        matching_row_count, &binding->base.base.base);
}

static int32_t write_grouped_composite_count_v18(
    void *opaque, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v18 *binding) {
    if (binding == NULL || binding->having_node_count != 6 ||
        binding->having_root != 5 ||
        binding->having_type_code != PRDB_SDK_VALUE_TYPE_BIGINT ||
        binding->base.base.has_having != 0) {
        return 1;
    }
    return write_grouped_composite_count(
        opaque, group_keys, group_key_count, aggregate_value,
        matching_row_count, &binding->base.base.base);
}

static int32_t write_grouped_composite_avg_v21(
    void *opaque, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v21 *binding) {
    composite_grouped_result_context *context =
        (composite_grouped_result_context *)opaque;
    const prdb_sdk_select_binding_v7 *aggregate_binding;
    int32_t status;
    if (context == NULL || binding == NULL || aggregate_value == NULL ||
        group_key_count != 2 || matching_row_count == 0 ||
        aggregate_value->state != PRDB_SDK_VALUE_STATE_VALUE ||
        aggregate_value->type_code != PRDB_SDK_VALUE_TYPE_DECIMAL ||
        binding->base.having_node_count != 1 ||
        binding->base.having_type_code != PRDB_SDK_VALUE_TYPE_DECIMAL ||
        binding->base.having_decimal_precision != 38 ||
        binding->base.having_decimal_scale != 8 ||
        binding->having_nodes[0].base.operand_kind !=
            PRDB_SDK_SQL_V21_OPERAND_NUMERIC_LITERAL) {
        return 1;
    }
    aggregate_binding = &binding->base.base.base.base.base;
    if (aggregate_binding->aggregate_kind != PRDB_SDK_SQL_V7_AGGREGATE_AVG ||
        aggregate_binding->aggregate_column.type_code !=
            PRDB_SDK_VALUE_TYPE_DECIMAL ||
        aggregate_binding->aggregate_column.decimal_precision != 4 ||
        aggregate_binding->aggregate_column.decimal_scale != 2) {
        return 1;
    }
    status = prdb_sdk_sql_write_group_composite_result_v1(
        context->host, &binding->base.base.base.base, context->columns,
        context->column_count, group_keys, group_key_count, aggregate_value,
        matching_row_count, context->output, context->output_capacity,
        &context->output_len);
    if (status != PRDB_SDK_SQL_RESULT_OK) return 1;
    ++context->row_count;
    return 0;
}

static int32_t write_grouped_composite_min_v21(
    void *opaque, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v21 *binding) {
    composite_grouped_result_context *context =
        (composite_grouped_result_context *)opaque;
    const prdb_sdk_select_binding_v7 *aggregate_binding;
    int32_t status;
    if (context == NULL || binding == NULL || aggregate_value == NULL ||
        group_key_count != 2 || matching_row_count == 0) {
        return 1;
    }
    if (aggregate_value->state != PRDB_SDK_VALUE_STATE_VALUE ||
        aggregate_value->type_code != PRDB_SDK_VALUE_TYPE_DECIMAL ||
        aggregate_value->data_len != 16) {
        return 1;
    }
    if (binding->base.having_node_count != 1 ||
        binding->base.having_type_code != PRDB_SDK_VALUE_TYPE_DECIMAL ||
        binding->base.having_decimal_precision != 4 ||
        binding->base.having_decimal_scale != 2 ||
        binding->having_nodes[0].base.operand_kind !=
            PRDB_SDK_SQL_V21_OPERAND_NUMERIC_LITERAL ||
        binding->having_nodes[0].scale != 2 ||
        binding->having_nodes[0].coefficient[15] != 100) {
        return 1;
    }
    for (uint32_t index = 0; index < 15; ++index) {
        if (binding->having_nodes[0].coefficient[index] != 0) {
            return 1;
        }
    }
    aggregate_binding = &binding->base.base.base.base.base;
    if (aggregate_binding->aggregate_kind !=
            PRDB_SDK_SQL_V7_AGGREGATE_MIN ||
        aggregate_binding->aggregate_column.type_code !=
            PRDB_SDK_VALUE_TYPE_DECIMAL ||
        aggregate_binding->aggregate_column.decimal_precision != 4 ||
        aggregate_binding->aggregate_column.decimal_scale != 2) {
        return 1;
    }
    status = prdb_sdk_sql_write_group_composite_result_v1(
        context->host, &binding->base.base.base.base, context->columns,
        context->column_count, group_keys, group_key_count, aggregate_value,
        matching_row_count, context->output, context->output_capacity,
        &context->output_len);
    if (status != PRDB_SDK_SQL_RESULT_OK) return 1;
    ++context->row_count;
    return 0;
}

static int32_t write_composite_joined_row(
    void *opaque, const prdb_sdk_value_v1 *projected_values,
    uint32_t projection_count, const prdb_sdk_select_binding_v16 *binding) {
    joined_result_context *context = (joined_result_context *)opaque;
    if (context == NULL || binding == NULL ||
        binding->join_key_count != 2 || projection_count != 4 ||
        binding->base.projection_count != 4) {
        return 1;
    }
    int32_t status = prdb_sdk_sql_write_join_result_v1(
        context->host, &binding->base, context->left_columns, 2,
        context->right_columns, 2, projected_values, projection_count,
        context->output, context->output_capacity, &context->output_len);
    if (status != PRDB_SDK_SQL_RESULT_OK) return 1;
    ++context->row_count;
    return 0;
}

static int32_t write_composite_joined_row_v19(
    void *opaque, const prdb_sdk_value_v1 *projected_values,
    uint32_t projection_count,
    const prdb_sdk_select_binding_v19 *binding) {
    if (binding == NULL) return 1;
    return write_composite_joined_row(opaque, projected_values,
                                      projection_count, &binding->base);
}

static int32_t write_self_joined_row_v19(
    void *opaque, const prdb_sdk_value_v1 *projected_values,
    uint32_t projection_count,
    const prdb_sdk_select_binding_v19 *binding) {
    joined_result_context *context = (joined_result_context *)opaque;
    if (context == NULL || binding == NULL || projection_count != 2 ||
        binding->base.base.projection_count != 2 ||
        binding->base.join_key_count != 2) {
        return 1;
    }
    int32_t status = prdb_sdk_sql_write_join_result_v1(
        context->host, &binding->base.base, context->left_columns, 2,
        context->right_columns, 2, projected_values, projection_count,
        context->output, context->output_capacity, &context->output_len);
    if (status != PRDB_SDK_SQL_RESULT_OK) return 1;
    ++context->row_count;
    return 0;
}

static int32_t collect_indexed_join_benchmark_row(
    void *opaque, const prdb_sdk_value_v1 *projected_values,
    uint32_t projection_count,
    const prdb_sdk_select_binding_v19 *binding) {
    indexed_join_benchmark_result_context *context = opaque;
    int32_t values[4] = {0};
    if (context == NULL || projected_values == NULL || binding == NULL ||
        projection_count != 4 || binding->base.join_key_count != 2) {
        return 1;
    }
    for (uint32_t index = 0; index < 4; ++index) {
        if (prdb_sdk_value_get_integer_v1(&projected_values[index], &values[index]) !=
            PRDB_SDK_VALUE_ACCESS_OK) {
            return 1;
        }
    }
    if (context->asymmetric_values != 0) {
        if (values[0] != 0 || values[1] != 0 || values[2] != 0 || values[3] != 0) {
            return 1;
        }
    } else if (values[1] != values[0] || values[2] != values[0] ||
               values[3] != values[0]) {
        return 1;
    }
    if (context->row_count == UINT32_MAX) return 1;
    ++context->row_count;
    context->value_checksum += (int64_t)values[0] + values[1] + values[2] + values[3];
    return 0;
}

static int32_t write_filtered_joined_row_v20(
    void *opaque, const prdb_sdk_value_v1 *projected_values,
    uint32_t projection_count,
    const prdb_sdk_select_binding_v20 *binding) {
    joined_result_context *context = (joined_result_context *)opaque;
    if (context == NULL || binding == NULL || projection_count != 2 ||
        binding->output_projection_count != 2 ||
        binding->base.base.base.projection_count != 2 ||
        binding->base.base.join_key_count != 2) {
        return 1;
    }
    int32_t status = prdb_sdk_sql_write_join_result_v1(
        context->host, &binding->base.base.base, context->left_columns, 2,
        context->right_columns, 2, projected_values, projection_count,
        context->output, context->output_capacity, &context->output_len);
    if (status != PRDB_SDK_SQL_RESULT_OK) return 1;
    ++context->row_count;
    return 0;
}

static int32_t write_grouped_sum(void *opaque,
                                 const prdb_sdk_value_v1 *group_key,
                                 const prdb_sdk_value_v1 *aggregate_value,
                                 uint64_t matching_row_count,
                                 const prdb_sdk_select_binding_v6 *binding) {
    grouped_result_context *context = (grouped_result_context *)opaque;
    if (context == NULL || binding == NULL ||
        binding->aggregate_kind != PRDB_SDK_SQL_V6_AGGREGATE_SUM ||
        matching_row_count != 1) {
        return 1;
    }
    int32_t status = prdb_sdk_sql_write_group_sum_result_v1(
        context->host, context->group_column, group_key,
        context->aggregate_column, aggregate_value, context->output,
        context->output_capacity, &context->output_len);
    if (status != PRDB_SDK_SQL_RESULT_OK) return 1;
    ++context->row_count;
    return 0;
}

static int32_t write_grouped_avg(void *opaque,
                                 const prdb_sdk_value_v1 *group_key,
                                 const prdb_sdk_value_v1 *aggregate_value,
                                 uint64_t matching_row_count,
                                 const prdb_sdk_select_binding_v7 *binding) {
    grouped_result_context *context = (grouped_result_context *)opaque;
    if (context == NULL || binding == NULL ||
        binding->aggregate_kind != PRDB_SDK_SQL_V7_AGGREGATE_AVG ||
        matching_row_count != 1) {
        return 1;
    }
    int32_t status = prdb_sdk_sql_write_group_avg_result_v1(
        context->host, context->group_column, group_key,
        context->aggregate_column, aggregate_value, context->output,
        context->output_capacity, &context->output_len);
    if (status != PRDB_SDK_SQL_RESULT_OK) return 1;
    ++context->row_count;
    return 0;
}

static int32_t write_ordered_grouped_avg(
    void *opaque, const prdb_sdk_value_v1 *group_key,
    const prdb_sdk_value_v1 *aggregate_value, uint64_t matching_row_count,
    const prdb_sdk_select_binding_v12 *binding) {
    grouped_result_context *context = (grouped_result_context *)opaque;
    static const int32_t expected_keys[] = {9, 7};
    static const uint64_t expected_counts[] = {3, 2};
    int32_t key;
    if (context == NULL || binding == NULL || group_key == NULL ||
        context->row_count >= 2 || binding->order_count != 2 ||
        binding->base.base.base.base.aggregate_kind !=
            PRDB_SDK_SQL_V7_AGGREGATE_AVG ||
        binding->order_items[0].item.kind !=
            PRDB_SDK_SQL_V10_ORDER_AGGREGATE ||
        binding->order_items[0].item.direction !=
            PRDB_SDK_SQL_V10_ORDER_DESCENDING ||
        binding->order_items[0].item.null_order !=
            PRDB_SDK_SQL_V11_NULLS_LAST ||
        binding->order_items[1].item.kind !=
            PRDB_SDK_SQL_V10_ORDER_GROUP_KEY ||
        binding->order_items[1].item.direction !=
            PRDB_SDK_SQL_V10_ORDER_ASCENDING ||
        binding->order_items[1].item.null_order !=
            PRDB_SDK_SQL_V11_NULLS_FIRST ||
        prdb_sdk_value_get_integer_v1(group_key, &key) !=
            PRDB_SDK_VALUE_ACCESS_OK ||
        key != expected_keys[context->row_count] ||
        matching_row_count != expected_counts[context->row_count])
        return 1;
    int32_t status = prdb_sdk_sql_write_group_avg_result_v1(
        context->host, context->group_column, group_key,
        context->aggregate_column, aggregate_value, context->output,
        context->output_capacity, &context->output_len);
    if (status != PRDB_SDK_SQL_RESULT_OK) return 1;
    ++context->row_count;
    return 0;
}

static int32_t write_ordered_analytical_pressure_sum(
    void *opaque, const prdb_sdk_value_v1 *group_key,
    const prdb_sdk_value_v1 *aggregate_value, uint64_t matching_row_count,
    const prdb_sdk_select_binding_v12 *binding) {
    grouped_result_context *context = opaque;
    uint32_t group = 0;
    int64_t sum = 0;
    if (context == NULL || binding == NULL || group_key == NULL ||
        aggregate_value == NULL || context->row_count >= 64 ||
        group_key->state != PRDB_SDK_VALUE_STATE_VALUE ||
        group_key->type_code != PRDB_SDK_VALUE_TYPE_TEXT ||
        group_key->data == NULL || group_key->data_len != 8172 ||
        binding->order_count != 2 ||
        binding->base.base.base.base.aggregate_kind !=
            PRDB_SDK_SQL_V7_AGGREGATE_SUM ||
        binding->order_items[0].item.kind !=
            PRDB_SDK_SQL_V10_ORDER_AGGREGATE ||
        binding->order_items[0].item.direction !=
            PRDB_SDK_SQL_V10_ORDER_DESCENDING ||
        binding->order_items[0].item.null_order !=
            PRDB_SDK_SQL_V11_NULLS_LAST ||
        binding->order_items[1].item.kind !=
            PRDB_SDK_SQL_V10_ORDER_GROUP_KEY ||
        binding->order_items[1].item.direction !=
            PRDB_SDK_SQL_V10_ORDER_ASCENDING ||
        binding->order_items[1].item.null_order !=
            PRDB_SDK_SQL_V11_NULLS_FIRST ||
        prdb_sdk_value_get_bigint_v1(aggregate_value, &sum) !=
            PRDB_SDK_VALUE_ACCESS_OK ||
        group_key->data[0] < '0' || group_key->data[0] > '9' ||
        group_key->data[1] < '0' || group_key->data[1] > '9' ||
        group_key->data[2] < '0' || group_key->data[2] > '9' ||
        group_key->data[3] < '0' || group_key->data[3] > '9' ||
        matching_row_count != 2) {
        return 1;
    }
    group = (uint32_t)(group_key->data[0] - '0') * 1000 +
            (uint32_t)(group_key->data[1] - '0') * 100 +
            (uint32_t)(group_key->data[2] - '0') * 10 +
            (uint32_t)(group_key->data[3] - '0');
    if (group != 63 - context->row_count || sum != 300 + 2 * group) {
        return 1;
    }
    for (uint32_t offset = 4; offset < 8172; ++offset) {
        if (group_key->data[offset] != 'a') return 1;
    }
    int32_t status = prdb_sdk_sql_write_group_sum_result_v1(
        context->host, context->group_column, group_key,
        context->aggregate_column, aggregate_value, context->output,
        context->output_capacity, &context->output_len);
    if (status != PRDB_SDK_SQL_RESULT_OK) return 1;
    ++context->row_count;
    return 0;
}

static int32_t write_grouped_ordered_count(
    void *opaque, const prdb_sdk_value_v1 *group_key,
    const prdb_sdk_value_v1 *aggregate_value, uint64_t matching_row_count,
    const prdb_sdk_select_binding_v12 *binding) {
    grouped_result_context *context = (grouped_result_context *)opaque;
    if (context == NULL || binding == NULL || aggregate_value != NULL ||
        binding->base.base.base.base.aggregate_kind !=
            PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR ||
        binding->base.base.has_order_by != 1 ||
        binding->order_count != 2 ||
        binding->order_items[0].item.kind !=
            PRDB_SDK_SQL_V10_ORDER_AGGREGATE ||
        binding->order_items[0].item.direction !=
            PRDB_SDK_SQL_V10_ORDER_DESCENDING ||
        binding->order_items[0].item.null_order !=
            PRDB_SDK_SQL_V11_NULLS_LAST ||
        binding->order_items[1].item.kind !=
            PRDB_SDK_SQL_V10_ORDER_GROUP_KEY ||
        binding->order_items[1].item.direction !=
            PRDB_SDK_SQL_V10_ORDER_ASCENDING ||
        binding->order_items[1].item.null_order !=
            PRDB_SDK_SQL_V11_NULLS_LAST ||
        matching_row_count != 1) {
        return 1;
    }
    int32_t status = prdb_sdk_sql_write_group_count_result_v1(
        context->host, context->group_column, group_key, matching_row_count,
        context->output, context->output_capacity, &context->output_len);
    if (status != PRDB_SDK_SQL_RESULT_OK) return 1;
    ++context->row_count;
    return 0;
}

static int32_t verify_selected_value(void *opaque, const prdb_sdk_row_v1 *row,
                                     const prdb_sdk_select_binding_v2 *binding) {
    result_context *context = (result_context *)opaque;
    prdb_sdk_cell_iterator_v1 cells = {0};
    prdb_sdk_value_v1 value = {0};

    (void)binding;
    if (context == NULL || row == NULL || row->cell_count != 1 ||
        prdb_sdk_row_cells_begin_v1(row, &cells, sizeof(cells)) !=
            PRDB_SDK_BATCH_OK ||
        prdb_sdk_cell_next_v1(&cells, &value, sizeof(value)) !=
            PRDB_SDK_BATCH_OK ||
        value.state != context->expected_state ||
        cells.cells_remaining != 0) {
        return 1;
    }
    if (value.state == PRDB_SDK_VALUE_STATE_VALUE &&
        (value.type_code != PRDB_SDK_VALUE_TYPE_INTEGER ||
         value.data_len != context->expected_value_len ||
         memcmp(value.data, context->expected_value,
                (size_t)context->expected_value_len) != 0)) {
        return 1;
    }
    ++context->row_count;
    if (context->marker != NULL) {
        return context->host->write_result(context->host->context,
                                           context->marker,
                                           context->marker_len);
    }
    return 0;
}

static int32_t verify_information_schema_name(
    void *opaque, const prdb_sdk_row_v1 *row,
    const prdb_sdk_select_binding_v2 *binding) {
    result_context *context = (result_context *)opaque;
    prdb_sdk_cell_iterator_v1 cells = {0};
    prdb_sdk_value_v1 value = {0};

    if (context == NULL || row == NULL || binding == NULL ||
        binding->projection_count != 1 ||
        binding->projection_ordinals[0] != context->expected_ordinal ||
        row->cell_count <= context->expected_ordinal ||
        prdb_sdk_row_cells_begin_v1(row, &cells, sizeof(cells)) !=
            PRDB_SDK_BATCH_OK) {
        if (context != NULL) context->failure_code = 1;
        return 1;
    }
    for (uint32_t index = 0; index <= context->expected_ordinal; ++index) {
        if (prdb_sdk_cell_next_v1(&cells, &value, sizeof(value)) !=
            PRDB_SDK_BATCH_OK) {
            context->failure_code = 2;
            return 1;
        }
    }
    if (
        value.state != PRDB_SDK_VALUE_STATE_VALUE ||
        value.type_code != PRDB_SDK_VALUE_TYPE_TEXT ||
        (context->expected_value != NULL &&
         (value.data_len != context->expected_value_len ||
          memcmp(value.data, context->expected_value,
                 (size_t)context->expected_value_len) != 0))) {
        context->failure_code = 3;
        return 1;
    }
    ++context->row_count;
    if (context->marker != NULL) {
        return context->host->write_result(context->host->context,
                                           context->marker,
                                           context->marker_len);
    }
    return context->host->write_result(context->host->context, value.data,
                                       value.data_len);
}

static int32_t count_unexpected_group(
    void *context, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v21 *binding) {
    (void)group_keys;
    (void)group_key_count;
    (void)aggregate_value;
    (void)matching_row_count;
    (void)binding;
    ++*(uint32_t *)context;
    return 0;
}

static int32_t report_failure(const prdb_host_api_v3 *host,
                              const uint8_t *message, uint64_t length) {
    (void)host->write_result(host->context, message, length);
    return 1;
}

