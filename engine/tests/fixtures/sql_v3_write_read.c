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
#include "prdb_sdk_sql_dispatch_v1.h"
#include "prdb_sdk_sql_exec_v4.h"
#include "prdb_sdk_sql_result_v1.h"
#include "prdb_sdk_executor_v1.h"
#include "prdb_sdk_optimizer_plan_v1.h"
#include "prdb_sdk_optimizer_v1.h"
#include "prdb_sdk_optimizer_v2.h"

#include <errno.h>
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
    }
    return range_observer_original_host->v3.executor_request(
        context, operation, request, request_len, response,
        response_capacity, response_len);
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
    if (operation == PRDB_IPC_OP_STAGE_UPDATE &&
        fail_next_stage_update != 0) {
        static const uint8_t injected[] = "UPDATE_STAGE_FAILURE_INJECTED";
        fail_next_stage_update = 0;
        (void)cancel_update_original_host->v4.v3.write_result(
            context, injected, sizeof(injected) - 1);
        return PRDB_QUERY_CALLBACK_IO_ERROR;
    }
    if (operation == PRDB_IPC_OP_STAGE_UPDATE &&
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
        if (operation == PRDB_IPC_OP_STAGE_UPDATE) {
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
        "GROUP BY left_key, right_key LIMIT 2";
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
    uint8_t composite_join_right_executor_response[1024] = {0};
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
    static prdb_sdk_sql_join_materialized_row_v16 composite_join_right_rows[128];
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
        fail_next_stage_update = 1;
        failure_host.v4.v3.executor_request = cancel_update_executor_request;
        update_status = prdb_sdk_optimizer_execute_update_ast_v1(
            &failure_host, &update, parameters, 2, schema, sizeof(schema) - 1,
            16, sizeof(batch_response), request, sizeof(request), response,
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
        status = prdb_sdk_sql_execute_group_composite_aggregate_optimizer_v1(
            optimizer_host_v5, schema, sizeof(schema) - 1,
            select_composite_sum_optimizer_sql,
            sizeof(select_composite_sum_optimizer_sql) - 1, 16,
            sizeof(batch_response), group_count_optimizer_request,
            sizeof(group_count_optimizer_request),
            group_count_optimizer_response,
            sizeof(group_count_optimizer_response), planner_response,
            sizeof(planner_response), executor_response,
            sizeof(executor_response), batch_response,
            sizeof(batch_response), planner_columns, 9, executor_columns, 9,
            cell_scratch, 9, grouped_materialized,
            sizeof(grouped_materialized), write_grouped_composite_sum_optimizer,
            &composite_grouped_results);
    } else {
        status = prdb_sdk_sql_execute_group_v13(
            host, schema, sizeof(schema) - 1,
            select_composite_sum_optimizer_sql,
            sizeof(select_composite_sum_optimizer_sql) - 1, NULL, 0, 16,
            sizeof(batch_response), planner_response,
            sizeof(planner_response), executor_response,
            sizeof(executor_response), batch_response,
            sizeof(batch_response), planner_columns, 9, executor_columns, 9,
            cell_scratch, 9, grouped_materialized,
            sizeof(grouped_materialized), write_grouped_composite_sum_optimizer,
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
        static const uint8_t message[] = "fixture-error-composite-inner-join";
        return report_failure(host, message, sizeof(message) - 1);
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
