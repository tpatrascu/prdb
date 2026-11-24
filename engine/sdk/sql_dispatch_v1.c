#include "prdb_sdk_sql_dispatch_v1.h"
#include "prdb_sdk_sql_dml_v3.h"
#include "prdb_sdk_sql_exec_v10.h"
#include "prdb_sdk_sql_exec_v8.h"
#include "prdb_sdk_sql_exec_v20.h"
#include "prdb_sdk_sql_exec_v24.h"
#include "prdb_sdk_sql_exec_v25.h"
#include "prdb_sdk_sql_exec_v3.h"
#include "prdb_sdk_sql_order_v2.h"
#include "prdb_sdk_optimizer_plan_v1.h"
#include "prdb_sdk_sql_v26.h"
#include "prdb_sdk_sql_v25.h"
#include "prdb_sdk_sql_spill_sort_v1.h"

#include <stdlib.h>
#include <string.h>

#define SQL_DISPATCH_RESPONSE_BYTES UINT64_C(1048576)
#define SQL_DISPATCH_BATCH_BYTES UINT32_C(1048576)
#define SQL_DISPATCH_CANDIDATES UINT16_C(128)
#define SQL_DISPATCH_JOIN_SORT_ROW_BYTES UINT32_C(1024)

static void *dispatch_calloc(size_t count, size_t size,
                             uint64_t *allocated_heap_bytes) {
    size_t bytes;
    void *allocation;

    if (allocated_heap_bytes == NULL ||
        (size != 0 && count > SIZE_MAX / size)) {
        return NULL;
    }
    bytes = count * size;
    if (*allocated_heap_bytes > PRDB_SDK_SQL_DISPATCH_MAX_HEAP_BYTES ||
        (uint64_t)bytes > PRDB_SDK_SQL_DISPATCH_MAX_HEAP_BYTES -
                              *allocated_heap_bytes) {
        return NULL;
    }
    allocation = calloc(count, size);
    if (allocation != NULL) *allocated_heap_bytes += (uint64_t)bytes;
    return allocation;
}

typedef struct sql_result_context sql_result_context;

typedef struct ordered_join_spool {
    const prdb_host_api_v6 *spill_host;
    prdb_sdk_spill_stream_v1 stream;
    uint64_t row_count;
    uint32_t projection_count;
    prdb_sdk_select_binding_v20 binding;
    int32_t failure_status;
} ordered_join_spool;

typedef struct ordered_join_sort {
    const prdb_host_api_v6 *spill_host;
    const prdb_host_api_v3 *host;
    const prdb_sdk_select_v25 *statement;
    const prdb_sdk_select_binding_v25 *binding;
    ordered_join_spool *spool;
    const prdb_sdk_sql_column_shape_v2 *left_columns;
    uint32_t left_column_count;
    const prdb_sdk_sql_column_shape_v2 *right_columns;
    uint32_t right_column_count;
    uint64_t read_offset;
    uint64_t emitted;
    uint32_t projection_count;
    uint32_t order_projection_index;
    prdb_sdk_value_v1 *projected_values;
    sql_result_context *result_context;
} ordered_join_sort;

struct sql_result_context {
    const prdb_host_api_v3 *host;
    prdb_sdk_sql_column_shape_v2 *columns;
    prdb_sdk_sql_column_shape_v2 *right_columns;
    uint32_t column_count;
    prdb_sdk_value_v1 *cell_scratch;
    uint32_t cell_scratch_capacity;
    uint8_t *output;
    uint64_t output_capacity;
};

static uint16_t read_u16_be(const uint8_t *bytes) {
    return (uint16_t)(((uint16_t)bytes[0] << 8) | bytes[1]);
}

static uint32_t read_u32_be(const uint8_t *bytes) {
    return ((uint32_t)bytes[0] << 24) | ((uint32_t)bytes[1] << 16) |
           ((uint32_t)bytes[2] << 8) | (uint32_t)bytes[3];
}

static int type_code_valid(uint32_t type_code) {
    return type_code >= PRDB_SDK_VALUE_TYPE_BOOLEAN &&
           type_code <= PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ;
}

static int decimal_fits_precision(const uint8_t bytes[16], uint8_t precision) {
    uint8_t magnitude[16];
    uint8_t limit[16] = {1};
    uint32_t carry;
    int negative = (bytes[15] & 0x80) != 0;

    if (precision == 0 || precision > 38) return 0;
    memcpy(magnitude, bytes, sizeof(magnitude));
    if (negative) {
        carry = 1;
        for (uint32_t index = 0; index < sizeof(magnitude); ++index) {
            uint32_t value = (uint32_t)(uint8_t)~magnitude[index] + carry;
            magnitude[index] = (uint8_t)value;
            carry = value >> 8;
        }
    }
    for (uint32_t digit = 0; digit < precision; ++digit) {
        carry = 0;
        for (uint32_t index = 0; index < sizeof(limit); ++index) {
            uint32_t value = (uint32_t)limit[index] * 10 + carry;
            limit[index] = (uint8_t)value;
            carry = value >> 8;
        }
        if (carry != 0) return 0;
    }
    for (uint32_t reverse = 0; reverse < sizeof(magnitude); ++reverse) {
        uint32_t index = (uint32_t)sizeof(magnitude) - reverse - 1;
        if (magnitude[index] < limit[index]) return 1;
        if (magnitude[index] > limit[index]) return 0;
    }
    return 0;
}

static int32_t validate_parameter_range(const prdb_sdk_value_v1 *value) {
    if (value->state != PRDB_SDK_VALUE_STATE_VALUE) return 1;
    if (value->type_code == PRDB_SDK_VALUE_TYPE_DATE) {
        int32_t days;
        memcpy(&days, value->data, sizeof(days));
        return days >= -719162 && days <= 2932896;
    }
    if (value->type_code == PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ) {
        int64_t micros;
        memcpy(&micros, value->data, sizeof(micros));
        return micros >= -INT64_C(62135596800000000) &&
               micros <= INT64_C(253402300799999999);
    }
    return 1;
}

int32_t prdb_sdk_sql_decode_parameters_v1(
    const uint8_t *parameters, uint64_t parameters_len,
    prdb_sdk_value_v1 *values, uint32_t values_capacity,
    prdb_sdk_sql_parameter_metadata_v1 *metadata,
    uint32_t metadata_capacity, uint32_t *parameter_count_out) {
    uint64_t offset = 8;
    uint32_t count;

    if (parameter_count_out == NULL || parameters_len >
            PRDB_SDK_SQL_DISPATCH_MAX_PARAMETER_BYTES ||
        (parameters_len != 0 && parameters == NULL)) {
        return PRDB_SDK_SQL_DISPATCH_INVALID_ARGUMENT;
    }
    *parameter_count_out = 0;
    if (parameters_len == 0) return PRDB_SDK_SQL_DISPATCH_OK;
    if (parameters_len < 8 || memcmp(parameters, "PSP1", 4) != 0 ||
        read_u16_be(parameters + 6) != 0) {
        return PRDB_SDK_SQL_DISPATCH_INVALID_PARAMETERS;
    }
    count = read_u16_be(parameters + 4);
    if (count > values_capacity || count > metadata_capacity ||
        (count != 0 && (values == NULL || metadata == NULL))) {
        return PRDB_SDK_SQL_DISPATCH_INVALID_ARGUMENT;
    }

    for (uint32_t index = 0; index < count; ++index) {
        uint32_t type_code;
        uint32_t payload_len;
        uint8_t precision;
        uint8_t scale;
        uint8_t state;
        prdb_sdk_value_v1 value = {0};
        prdb_sdk_sql_parameter_metadata_v1 type = {0};

        if (parameters_len - offset < 12) {
            return PRDB_SDK_SQL_DISPATCH_INVALID_PARAMETERS;
        }
        type_code = read_u32_be(parameters + offset);
        precision = parameters[offset + 4];
        scale = parameters[offset + 5];
        state = parameters[offset + 6];
        payload_len = read_u32_be(parameters + offset + 8);
        offset += 12;
        if (!type_code_valid(type_code) || parameters[offset - 5] != 0 ||
            payload_len > parameters_len - offset ||
            (state != PRDB_SDK_VALUE_STATE_VALUE &&
             state != PRDB_SDK_VALUE_STATE_NULL) ||
            (state == PRDB_SDK_VALUE_STATE_NULL && payload_len != 0) ||
            (type_code == PRDB_SDK_VALUE_TYPE_DECIMAL
                 ? (precision == 0 || precision > 38 || scale > precision)
                 : (precision != 0 || scale != 0))) {
            return PRDB_SDK_SQL_DISPATCH_INVALID_PARAMETERS;
        }

        type.struct_size = sizeof(type);
        type.abi_version = PRDB_SDK_SQL_PARAMETER_METADATA_ABI_V1_VERSION;
        type.type_code = type_code;
        type.decimal_precision = precision;
        type.decimal_scale = scale;

        value.struct_size = sizeof(value);
        value.abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
        value.state = state;
        if (state == PRDB_SDK_VALUE_STATE_VALUE) {
            value.type_code = type_code;
            value.data = parameters + offset;
            value.data_len = payload_len;
            if (prdb_sdk_value_validate_v1(&value) != PRDB_SDK_VALUE_VALID ||
                !validate_parameter_range(&value) ||
                (type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
                 (payload_len != 16 ||
                  !decimal_fits_precision(value.data, precision)))) {
                return PRDB_SDK_SQL_DISPATCH_INVALID_PARAMETERS;
            }
        }
        values[index] = value;
        metadata[index] = type;
        offset += payload_len;
    }
    if (offset != parameters_len) {
        return PRDB_SDK_SQL_DISPATCH_INVALID_PARAMETERS;
    }
    *parameter_count_out = count;
    return PRDB_SDK_SQL_DISPATCH_OK;
}

/* PSP1 borrows little-endian scalar payloads. Evaluators and executor
 * request encoders consume big-endian SDK values. */
static void normalize_parameter_payloads(
    prdb_sdk_value_v1 *values, uint32_t count, uint8_t (*payloads)[16]) {
    for (uint32_t index = 0; index < count; ++index) {
        prdb_sdk_value_v1 *value = &values[index];
        if (value->state != PRDB_SDK_VALUE_STATE_VALUE) continue;
        switch (value->type_code) {
        case PRDB_SDK_VALUE_TYPE_SMALLINT:
        case PRDB_SDK_VALUE_TYPE_INTEGER:
        case PRDB_SDK_VALUE_TYPE_BIGINT:
        case PRDB_SDK_VALUE_TYPE_DECIMAL:
        case PRDB_SDK_VALUE_TYPE_DATE:
        case PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ:
            for (uint64_t byte = 0; byte < value->data_len; ++byte)
                payloads[index][byte] = value->data[value->data_len - 1 - byte];
            value->data = payloads[index];
            break;
        default:
            break;
        }
    }
}

static int32_t write_sqlr_row(
    void *opaque, const prdb_sdk_row_v1 *row,
    const prdb_sdk_select_binding_v2 *binding) {
    sql_result_context *context = (sql_result_context *)opaque;
    uint64_t output_len = 0;
    if (context == NULL || row == NULL || binding == NULL) return 1;
    context->column_count = binding->relation_column_count;
    return prdb_sdk_sql_write_result_row_v1(
               context->host, binding, context->columns, context->column_count,
               row, context->cell_scratch, context->cell_scratch_capacity,
               context->output, context->output_capacity, &output_len) ==
           PRDB_SDK_SQL_RESULT_OK
               ? 0
               : 1;
}

static int32_t write_group_sqlr_single_key(
    void *opaque, const prdb_sdk_value_v1 *group_key,
    const prdb_sdk_value_v1 *aggregate_value, uint64_t matching_row_count,
    const prdb_sdk_select_binding_v7 *group_binding) {
    sql_result_context *context = (sql_result_context *)opaque;
    const prdb_sdk_sql_column_shape_v2 *group_column;
    const prdb_sdk_sql_column_shape_v2 *aggregate_column;
    uint32_t kind;
    uint64_t output_len = 0;
    int32_t status;

    if (context == NULL || group_binding == NULL || group_key == NULL) return 1;
    context->column_count = group_binding->base.relation_column_count;
    if (group_binding->group_by_column.ordinal >= context->column_count)
        return 1;
    group_column = &context->columns[group_binding->group_by_column.ordinal];
    kind = group_binding->aggregate_kind;
    if (kind == PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
        status = prdb_sdk_sql_write_group_count_result_v1(
            context->host, group_column, group_key, matching_row_count,
            context->output, context->output_capacity, &output_len);
    } else {
        if (group_binding->aggregate_column.ordinal >= context->column_count ||
            aggregate_value == NULL)
            return 1;
        aggregate_column =
            &context->columns[group_binding->aggregate_column.ordinal];
        if (kind == PRDB_SDK_SQL_V7_AGGREGATE_MIN ||
            kind == PRDB_SDK_SQL_V7_AGGREGATE_MAX) {
            status = prdb_sdk_sql_write_group_minmax_result_v1(
                context->host, group_column, group_key, aggregate_column,
                aggregate_value, kind, context->output,
                context->output_capacity, &output_len);
        } else if (kind == PRDB_SDK_SQL_V7_AGGREGATE_SUM) {
            status = prdb_sdk_sql_write_group_sum_result_v1(
                context->host, group_column, group_key, aggregate_column,
                aggregate_value, context->output, context->output_capacity,
                &output_len);
        } else if (kind == PRDB_SDK_SQL_V7_AGGREGATE_AVG) {
            status = prdb_sdk_sql_write_group_avg_result_v1(
                context->host, group_column, group_key, aggregate_column,
                aggregate_value, context->output, context->output_capacity,
                &output_len);
        } else {
            return 1;
        }
    }
    return status == PRDB_SDK_SQL_RESULT_OK ? 0 : 1;
}

static int32_t write_group_sqlr_tuple(
    void *opaque, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v21 *binding) {
    sql_result_context *context = (sql_result_context *)opaque;
    const prdb_sdk_select_binding_v13 *composite_binding;
    uint64_t output_len = 0;
    if (context == NULL || binding == NULL) return 1;
    composite_binding = &binding->base.base.base.base;
    if (group_key_count == 1 &&
        composite_binding->base.aggregate_kind !=
            PRDB_SDK_SQL_V7_AGGREGATE_COUNT_COLUMN)
        return write_group_sqlr_single_key(
            opaque, group_keys, aggregate_value, matching_row_count,
            &composite_binding->base);
    context->column_count = binding->base.base.base.base.base.base
                                .relation_column_count;
    return prdb_sdk_sql_write_group_composite_result_v1(
               context->host, composite_binding, context->columns,
               context->column_count, group_keys, group_key_count,
               aggregate_value, matching_row_count, context->output,
               context->output_capacity, &output_len) ==
                   PRDB_SDK_SQL_RESULT_OK
               ? 0
               : 1;
}

static int32_t write_group_sqlr_row_v10(
    void *opaque, const prdb_sdk_value_v1 *group_key,
    const prdb_sdk_value_v1 *aggregate_value, uint64_t matching_row_count,
    const prdb_sdk_select_binding_v10 *binding) {
    if (binding == NULL) return 1;
    return write_group_sqlr_single_key(opaque, group_key, aggregate_value,
                                       matching_row_count, &binding->base.base);
}

static int32_t write_join_sqlr_row_v20(
    void *opaque, const prdb_sdk_value_v1 *projected_values,
    uint32_t projection_count,
    const prdb_sdk_select_binding_v20 *binding) {
    sql_result_context *context = (sql_result_context *)opaque;
    const prdb_sdk_select_binding_v8 *join_binding;
    uint64_t output_len = 0;
    if (context == NULL || binding == NULL || context->columns == NULL ||
        context->right_columns == NULL)
        return 1;
    join_binding = &binding->base.base.base;
    return prdb_sdk_sql_write_join_result_v1(
               context->host, join_binding, context->columns,
               join_binding->left_relation_column_count,
               context->right_columns,
               join_binding->right_relation_column_count, projected_values,
               projection_count, context->output, context->output_capacity,
               &output_len) == PRDB_SDK_SQL_RESULT_OK
               ? 0
               : 1;
}

static int32_t write_join_sqlr_row_v8(
    void *opaque, const prdb_sdk_value_v1 *projected_values,
    uint32_t projection_count,
    const prdb_sdk_select_binding_v8 *binding) {
    sql_result_context *context = (sql_result_context *)opaque;
    uint64_t output_len = 0;
    if (context == NULL || binding == NULL || context->columns == NULL ||
        context->right_columns == NULL)
        return 1;
    return prdb_sdk_sql_write_join_result_v1(
               context->host, binding, context->columns,
               binding->left_relation_column_count, context->right_columns,
               binding->right_relation_column_count, projected_values,
               projection_count, context->output, context->output_capacity,
               &output_len) == PRDB_SDK_SQL_RESULT_OK
               ? 0
               : 1;
}

static void dispatch_write_u32(uint8_t *bytes, uint32_t value) {
    bytes[0] = (uint8_t)(value >> 24);
    bytes[1] = (uint8_t)(value >> 16);
    bytes[2] = (uint8_t)(value >> 8);
    bytes[3] = (uint8_t)value;
}

static uint32_t dispatch_read_u32(const uint8_t *bytes) {
    return ((uint32_t)bytes[0] << 24) | ((uint32_t)bytes[1] << 16) |
           ((uint32_t)bytes[2] << 8) | bytes[3];
}

static int32_t spool_ordered_join_row(
    void *opaque, const prdb_sdk_value_v1 *projected_values,
    uint32_t projection_count,
    const prdb_sdk_select_binding_v20 *binding) {
    ordered_join_spool *spool = (ordered_join_spool *)opaque;
    uint8_t record[SQL_DISPATCH_JOIN_SORT_ROW_BYTES] = {0};
    uint32_t offset = 4;
    if (spool == NULL || projected_values == NULL || binding == NULL ||
        projection_count == 0 ||
        projection_count > PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS ||
        (spool->projection_count != 0 &&
         spool->projection_count != projection_count)) {
        if (spool != NULL)
            spool->failure_status = PRDB_SDK_SQL_EXEC_SHAPE_MISMATCH;
        return 1;
    }
    for (uint32_t index = 0; index < projection_count; ++index) {
        const prdb_sdk_value_v1 *value = &projected_values[index];
        uint32_t data_len;
        if (value->struct_size < sizeof(*value) ||
            value->abi_version != PRDB_SDK_VALUE_ABI_V1_VERSION ||
            value->state > PRDB_SDK_VALUE_STATE_REDACTED ||
            value->data_len > UINT32_MAX ||
            (value->state != PRDB_SDK_VALUE_STATE_VALUE &&
             value->data_len != 0) ||
            (value->data_len != 0 && value->data == NULL)) {
            spool->failure_status = PRDB_SDK_SQL_EXEC_EVALUATION_ERROR;
            return 1;
        }
        data_len = (uint32_t)value->data_len;
        if (offset > sizeof(record) || sizeof(record) - offset < 9 ||
            data_len > sizeof(record) - offset - 9) {
            spool->failure_status = PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT;
            return 1;
        }
        record[offset] = (uint8_t)value->state;
        dispatch_write_u32(record + offset + 1, value->type_code);
        dispatch_write_u32(record + offset + 5, data_len);
        if (data_len != 0)
            memcpy(record + offset + 9, value->data, data_len);
        offset += 9 + data_len;
    }
    dispatch_write_u32(record, projection_count);
    if (spool->stream.valid == 0 &&
        prdb_sdk_spill_create_v1(spool->spill_host, &spool->stream) != 0) {
        spool->failure_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
        return 1;
    }
    if (spool->row_count == UINT64_MAX /
                                SQL_DISPATCH_JOIN_SORT_ROW_BYTES ||
        prdb_sdk_spill_append_v1(spool->spill_host, &spool->stream, record,
                                 sizeof(record)) != 0) {
        spool->failure_status = PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR;
        return 1;
    }
    spool->projection_count = projection_count;
    spool->binding = *binding;
    ++spool->row_count;
    return 0;
}

static int32_t decode_ordered_join_record(
    const ordered_join_sort *sort, const uint8_t *record,
    prdb_sdk_value_v1 *values) {
    uint32_t count = dispatch_read_u32(record);
    uint32_t offset = 4;
    if (sort == NULL || record == NULL || values == NULL ||
        count != sort->projection_count || count == 0 ||
        count > PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS ||
        sort->order_projection_index >= count)
        return -1;
    for (uint32_t index = 0; index < count; ++index) {
        uint32_t data_len;
        uint32_t type_code;
        uint8_t state;
        if (offset > SQL_DISPATCH_JOIN_SORT_ROW_BYTES ||
            SQL_DISPATCH_JOIN_SORT_ROW_BYTES - offset < 9)
            return -1;
        state = record[offset];
        type_code = dispatch_read_u32(record + offset + 1);
        data_len = dispatch_read_u32(record + offset + 5);
        offset += 9;
        if (state > PRDB_SDK_VALUE_STATE_REDACTED ||
            data_len > SQL_DISPATCH_JOIN_SORT_ROW_BYTES - offset ||
            (state != PRDB_SDK_VALUE_STATE_VALUE && data_len != 0))
            return -1;
        values[index] = (prdb_sdk_value_v1){
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            state, type_code,
            state == PRDB_SDK_VALUE_STATE_VALUE ? record + offset : NULL,
            data_len};
        offset += data_len;
    }
    if (values[sort->order_projection_index].state ==
            PRDB_SDK_VALUE_STATE_VALUE &&
        values[sort->order_projection_index].type_code !=
            sort->binding->order_by_column.type_code)
        return -1;
    return 0;
}

static int32_t ordered_join_sort_source(void *opaque, uint8_t *record,
                                        uint32_t capacity, uint32_t *row_len,
                                        uint8_t *has_row) {
    ordered_join_sort *sort = (ordered_join_sort *)opaque;
    uint64_t read_bytes = 0;
    uint64_t total_bytes;
    if (sort == NULL || capacity < SQL_DISPATCH_JOIN_SORT_ROW_BYTES ||
        sort->spool == NULL || row_len == NULL || has_row == NULL ||
        sort->spool->row_count >
            UINT64_MAX / SQL_DISPATCH_JOIN_SORT_ROW_BYTES)
        return -1;
    total_bytes = sort->spool->row_count *
                  SQL_DISPATCH_JOIN_SORT_ROW_BYTES;
    *row_len = 0;
    *has_row = 0;
    if (sort->read_offset == total_bytes) return 0;
    if (sort->read_offset > total_bytes ||
        prdb_sdk_spill_read_at_v1(
            sort->spill_host, &sort->spool->stream, sort->read_offset, record,
            SQL_DISPATCH_JOIN_SORT_ROW_BYTES, &read_bytes) != 0 ||
        read_bytes != SQL_DISPATCH_JOIN_SORT_ROW_BYTES)
        return -1;
    sort->read_offset += SQL_DISPATCH_JOIN_SORT_ROW_BYTES;
    *row_len = SQL_DISPATCH_JOIN_SORT_ROW_BYTES;
    *has_row = 1;
    return 0;
}

static int32_t ordered_join_extract_key(const ordered_join_sort *sort,
                                        const uint8_t *record,
                                        prdb_sdk_value_v1 *key) {
    uint32_t count = dispatch_read_u32(record);
    uint32_t offset = 4;
    if (count != sort->projection_count ||
        sort->order_projection_index >= count)
        return -1;
    for (uint32_t index = 0; index < count; ++index) {
        uint32_t data_len;
        uint32_t type_code;
        uint8_t state;
        if (offset > SQL_DISPATCH_JOIN_SORT_ROW_BYTES ||
            SQL_DISPATCH_JOIN_SORT_ROW_BYTES - offset < 9)
            return -1;
        state = record[offset];
        type_code = dispatch_read_u32(record + offset + 1);
        data_len = dispatch_read_u32(record + offset + 5);
        offset += 9;
        if (state > PRDB_SDK_VALUE_STATE_REDACTED ||
            data_len > SQL_DISPATCH_JOIN_SORT_ROW_BYTES - offset ||
            (state != PRDB_SDK_VALUE_STATE_VALUE && data_len != 0))
            return -1;
        if (index == sort->order_projection_index) {
            if (state == PRDB_SDK_VALUE_STATE_VALUE &&
                type_code != sort->binding->order_by_column.type_code)
                return -1;
            *key = (prdb_sdk_value_v1){
                sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
                state, type_code,
                state == PRDB_SDK_VALUE_STATE_VALUE ? record + offset : NULL,
                data_len};
            return 0;
        }
        offset += data_len;
    }
    return -1;
}

static int32_t ordered_join_sort_compare(void *opaque, const uint8_t *left,
                                        const uint8_t *right,
                                        int32_t *ordering) {
    ordered_join_sort *sort = (ordered_join_sort *)opaque;
    prdb_sdk_value_v1 keys[2] = {{0}};
    uint32_t indices[2] = {0};
    uint32_t equal = 0;
    int32_t status;
    if (ordering == NULL || ordered_join_extract_key(sort, left, &keys[0]) != 0 ||
        ordered_join_extract_key(sort, right, &keys[1]) != 0 ||
        keys[0].state == PRDB_SDK_VALUE_STATE_REDACTED ||
        keys[1].state == PRDB_SDK_VALUE_STATE_REDACTED)
        return -1;
    if (keys[0].state == PRDB_SDK_VALUE_STATE_NULL &&
        keys[1].state == PRDB_SDK_VALUE_STATE_NULL) {
        *ordering = 0;
        return 0;
    }
    status = prdb_sdk_sql_order_row_indices_v2(
        keys, 2, sort->binding->order_by_column.type_code,
        sort->binding->order_by_column.decimal_scale,
        sort->statement->order_descending
            ? PRDB_SDK_SQL_ORDER_DESCENDING
            : PRDB_SDK_SQL_ORDER_ASCENDING,
        PRDB_SDK_SQL_ORDER_NULLS_FIRST, indices, 2);
    if (status != PRDB_SDK_SQL_EVAL_OK) return status;
    if (keys[0].state == PRDB_SDK_VALUE_STATE_NULL ||
        keys[1].state == PRDB_SDK_VALUE_STATE_NULL) {
        *ordering = indices[0] == 0 ? -1 : 1;
        return 0;
    }
    status = prdb_sdk_sql_compare_v1(
        &keys[0], &keys[1], PRDB_SDK_SQL_COMPARE_EQUAL,
        sort->binding->order_by_column.decimal_scale,
        sort->binding->order_by_column.decimal_scale, &equal);
    if (status != PRDB_SDK_SQL_EVAL_OK) return status;
    *ordering = equal == PRDB_SDK_SQL_TRUTH_TRUE
                    ? 0
                    : (indices[0] == 0 ? -1 : 1);
    return 0;
}

static int32_t ordered_join_sort_output(void *opaque, const uint8_t *record,
                                        uint32_t row_len) {
    ordered_join_sort *sort = (ordered_join_sort *)opaque;
    uint64_t output_len = 0;
    if (sort == NULL || row_len != SQL_DISPATCH_JOIN_SORT_ROW_BYTES ||
        decode_ordered_join_record(sort, record, sort->projected_values) != 0)
        return -1;
    if (sort->statement->base.base.base.base.has_limit != 0 &&
        sort->emitted >= sort->statement->base.base.base.base.limit)
        return 0;
    if (prdb_sdk_sql_write_join_result_v1(
            sort->host, &sort->spool->binding.base.base.base,
            sort->left_columns, sort->left_column_count, sort->right_columns,
            sort->right_column_count, sort->projected_values,
            sort->projection_count, sort->result_context->output,
            sort->result_context->output_capacity, &output_len) !=
        PRDB_SDK_SQL_RESULT_OK)
        return -1;
    ++sort->emitted;
    return 0;
}

static int32_t prepare_ordered_join_spool(
    ordered_join_sort *sort, uint32_t projection_count) {
    uint8_t record[SQL_DISPATCH_JOIN_SORT_ROW_BYTES];
    if (sort == NULL || sort->spool == NULL ||
        sort->spool->stream.valid != 1 || projection_count == 0 ||
        projection_count > PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS)
        return -1;
    sort->projection_count = projection_count;
    if (prdb_sdk_spill_seal_v1(sort->spill_host, &sort->spool->stream) != 0)
        return -1;
    for (uint64_t index = 0; index < sort->spool->row_count; ++index) {
        uint64_t read_bytes = 0;
        prdb_sdk_value_v1 key = {0};
        if (prdb_sdk_spill_read_at_v1(
                sort->spill_host, &sort->spool->stream,
                index * SQL_DISPATCH_JOIN_SORT_ROW_BYTES, record,
                sizeof(record), &read_bytes) != 0 || read_bytes != sizeof(record) ||
            ordered_join_extract_key(sort, record, &key) != 0 ||
            key.state == PRDB_SDK_VALUE_STATE_REDACTED ||
            prdb_sdk_value_validate_v1(&key) != PRDB_SDK_VALUE_VALID)
            return -1;
    }
    return 0;
}

static int32_t run_ordered_join_sort(ordered_join_sort *sort) {
    int32_t status;
    if (sort->spool->row_count == 0) return 0;
    status = prdb_sdk_sql_spill_sort_v1(
        sort->spill_host, sort, SQL_DISPATCH_JOIN_SORT_ROW_BYTES,
        UINT32_C(65536), ordered_join_sort_source,
        ordered_join_sort_compare, ordered_join_sort_output);
    return status;
}

static int32_t latch_sql_failure(const prdb_host_api_v3 *host, int32_t status) {
    if (host != NULL && host->statement_failure != NULL) {
        (void)host->statement_failure(host->context,
                                      PRDB_QUERY_STATEMENT_FAILURE_SQL);
    }
    return status;
}

static int32_t sql_run_internal(
    const prdb_host_api_v3 *host, const prdb_host_api_v6 *spill_host,
    const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const uint8_t *parameters, uint64_t parameters_len) {
    prdb_sdk_value_v1 *values = NULL;
    uint8_t (*parameter_payloads)[16] = NULL;
    prdb_sdk_sql_parameter_metadata_v1 *metadata = NULL;
    uint8_t *planner_response = NULL;
    uint8_t *right_planner_response = NULL;
    uint8_t *executor_response = NULL;
    uint8_t *right_executor_response = NULL;
    uint8_t *batch_response = NULL;
    uint8_t *output = NULL;
    uint8_t *materialized_bytes = NULL;
    uint8_t *ordered_join_sql = NULL;
    prdb_sdk_sql_column_shape_v2 *right_planner_columns = NULL;
    prdb_sdk_sql_column_shape_v2 *right_executor_columns = NULL;
    prdb_sdk_sql_join_materialized_row_v16 *left_rows = NULL;
    prdb_sdk_sql_join_materialized_row_v16 *right_rows = NULL;
    prdb_sdk_sql_join_materialized_row_v8 *left_rows_v8 = NULL;
    prdb_sdk_sql_join_materialized_row_v8 *right_rows_v8 = NULL;
    prdb_sdk_sql_column_shape_v2 *planner_columns = NULL;
    prdb_sdk_sql_column_shape_v2 *executor_columns = NULL;
    prdb_sdk_value_v1 *cell_scratch = NULL;
    sql_result_context result = {0};
    ordered_join_spool ordered_join_rows = {0};
    ordered_join_sort ordered_join_sorter = {0};
    prdb_sdk_select_v24 grouped_statement = {0};
    prdb_sdk_select_v25 ordered_join_statement = {0};
    prdb_sdk_select_binding_v25 ordered_join_binding = {0};
    prdb_sdk_select_v20 join_statement = {0};
    prdb_sdk_select_v8 single_key_join_statement = {0};
    prdb_sdk_select_v10 single_grouped_statement = {0};
    prdb_sdk_select_v3 ordered_row_statement = {0};
    int is_grouped = 0;
    int is_ordered_join = 0;
    int is_join = 0;
    int is_single_key_join = 0;
    int is_single_grouped = 0;
    int is_ordered_row_select = 0;
    int ordered_join_needs_latch = 0;
    uint32_t parameter_count = 0;
    uint32_t declared_count = 0;
    uint64_t allocated_heap_bytes = 0;
    int32_t status;

    if (host == NULL || host->struct_size < sizeof(*host) ||
        host->abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->statement_failure == NULL) {
        return PRDB_SDK_SQL_DISPATCH_INVALID_ARGUMENT;
    }
    if (host->write_result == NULL || schema == NULL || schema_len == 0 ||
        schema_len > 128 || sql == NULL ||
        sql_len == 0 || sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        parameters_len > PRDB_SDK_SQL_DISPATCH_MAX_PARAMETER_BYTES ||
        (parameters_len != 0 && parameters == NULL)) {
        return latch_sql_failure(host,
                                 PRDB_SDK_SQL_DISPATCH_INVALID_ARGUMENT);
    }
    is_grouped = prdb_sdk_sql_parse_select_v24(
                     sql, sql_len, &grouped_statement,
                     sizeof(grouped_statement), NULL) == PRDB_SDK_SQL_OK;
    if (!is_grouped) {
        is_ordered_join = prdb_sdk_sql_parse_select_v25(
                              sql, sql_len, &ordered_join_statement,
                              sizeof(ordered_join_statement), NULL) ==
                          PRDB_SDK_SQL_OK;
        if (is_ordered_join) {
            is_join = 1;
        } else {
            is_join = prdb_sdk_sql_parse_select_v20(
                      sql, sql_len, &join_statement, sizeof(join_statement),
                      NULL) == PRDB_SDK_SQL_OK;
        }
        if (!is_join) {
            is_single_key_join = prdb_sdk_sql_parse_select_v8(
                                     sql, sql_len, &single_key_join_statement,
                                     sizeof(single_key_join_statement), NULL) ==
                                 PRDB_SDK_SQL_OK;
            if (!is_single_key_join) {
                is_single_grouped = prdb_sdk_sql_parse_select_v10(
                                        sql, sql_len, &single_grouped_statement,
                                        sizeof(single_grouped_statement), NULL) ==
                                    PRDB_SDK_SQL_OK;
            }
        }
    }
    if (is_grouped || is_join || is_single_key_join || is_single_grouped) {
        materialized_bytes = (uint8_t *)dispatch_calloc(
            1, SQL_DISPATCH_BATCH_BYTES, &allocated_heap_bytes);
        if (materialized_bytes == NULL) {
            return latch_sql_failure(host,
                                     PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR);
        }
    }
    if (spill_host != NULL && !is_grouped && !is_join &&
        !is_single_key_join && !is_single_grouped &&
        prdb_sdk_sql_parse_select_v3(
            sql, sql_len, &ordered_row_statement,
            sizeof(ordered_row_statement), NULL) == PRDB_SDK_SQL_OK &&
        ordered_row_statement.has_order_by != 0) {
        is_ordered_row_select = 1;
    }
    if (parameters_len >= 8 && memcmp(parameters, "PSP1", 4) == 0) {
        declared_count = read_u16_be(parameters + 4);
    }
    if (declared_count != 0) {
        values = (prdb_sdk_value_v1 *)dispatch_calloc(
            declared_count, sizeof(*values), &allocated_heap_bytes);
        parameter_payloads = (uint8_t (*)[16])dispatch_calloc(
            declared_count, sizeof(*parameter_payloads), &allocated_heap_bytes);
        metadata = (prdb_sdk_sql_parameter_metadata_v1 *)dispatch_calloc(
            declared_count, sizeof(*metadata), &allocated_heap_bytes);
    }
    planner_response = (uint8_t *)dispatch_calloc(
        1, SQL_DISPATCH_RESPONSE_BYTES, &allocated_heap_bytes);
    if (is_join || is_single_key_join) {
        right_planner_response =
            (uint8_t *)dispatch_calloc(
                1, SQL_DISPATCH_RESPONSE_BYTES, &allocated_heap_bytes);
        right_executor_response =
            (uint8_t *)dispatch_calloc(
                1, SQL_DISPATCH_RESPONSE_BYTES, &allocated_heap_bytes);
    }
    executor_response = (uint8_t *)dispatch_calloc(
        1, SQL_DISPATCH_RESPONSE_BYTES, &allocated_heap_bytes);
    batch_response = (uint8_t *)dispatch_calloc(
        1, SQL_DISPATCH_BATCH_BYTES, &allocated_heap_bytes);
    output = (uint8_t *)dispatch_calloc(
        1, PRDB_SDK_SQL_RESULT_MAX_BYTES, &allocated_heap_bytes);
    planner_columns = (prdb_sdk_sql_column_shape_v2 *)dispatch_calloc(
        PRDB_SDK_SQL_MAX_RELATION_COLUMNS, sizeof(*planner_columns),
        &allocated_heap_bytes);
    executor_columns = (prdb_sdk_sql_column_shape_v2 *)dispatch_calloc(
        PRDB_SDK_SQL_MAX_RELATION_COLUMNS, sizeof(*executor_columns),
        &allocated_heap_bytes);
    if (is_join) {
        right_planner_columns = (prdb_sdk_sql_column_shape_v2 *)dispatch_calloc(
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, sizeof(*right_planner_columns),
            &allocated_heap_bytes);
        right_executor_columns = (prdb_sdk_sql_column_shape_v2 *)dispatch_calloc(
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, sizeof(*right_executor_columns),
            &allocated_heap_bytes);
        left_rows = (prdb_sdk_sql_join_materialized_row_v16 *)dispatch_calloc(
            PRDB_SDK_SQL_EXEC_V8_MAX_ROWS, sizeof(*left_rows),
            &allocated_heap_bytes);
        right_rows = (prdb_sdk_sql_join_materialized_row_v16 *)dispatch_calloc(
            PRDB_SDK_SQL_EXEC_V8_MAX_ROWS, sizeof(*right_rows),
            &allocated_heap_bytes);
    } else if (is_single_key_join) {
        right_planner_columns = (prdb_sdk_sql_column_shape_v2 *)dispatch_calloc(
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, sizeof(*right_planner_columns),
            &allocated_heap_bytes);
        right_executor_columns = (prdb_sdk_sql_column_shape_v2 *)dispatch_calloc(
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, sizeof(*right_executor_columns),
            &allocated_heap_bytes);
        left_rows_v8 = (prdb_sdk_sql_join_materialized_row_v8 *)dispatch_calloc(
            PRDB_SDK_SQL_EXEC_V8_MAX_ROWS, sizeof(*left_rows_v8),
            &allocated_heap_bytes);
        right_rows_v8 = (prdb_sdk_sql_join_materialized_row_v8 *)dispatch_calloc(
            PRDB_SDK_SQL_EXEC_V8_MAX_ROWS, sizeof(*right_rows_v8),
            &allocated_heap_bytes);
    }
    cell_scratch = (prdb_sdk_value_v1 *)dispatch_calloc(
        PRDB_SDK_SQL_MAX_RELATION_COLUMNS, sizeof(*cell_scratch),
        &allocated_heap_bytes);
    if ((declared_count != 0 && (values == NULL || metadata == NULL ||
                                 parameter_payloads == NULL)) ||
        planner_response == NULL || executor_response == NULL ||
        ((is_join || is_single_key_join) &&
         (right_planner_response == NULL ||
                     right_executor_response == NULL)) ||
        batch_response == NULL || output == NULL || planner_columns == NULL ||
        executor_columns == NULL || cell_scratch == NULL ||
        ((is_join || is_single_key_join) &&
         (right_planner_columns == NULL ||
          right_executor_columns == NULL)) ||
        (is_join && (left_rows == NULL || right_rows == NULL)) ||
        (is_single_key_join &&
         (left_rows_v8 == NULL || right_rows_v8 == NULL))) {
        status = latch_sql_failure(host, PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR);
        goto cleanup;
    }
    status = prdb_sdk_sql_decode_parameters_v1(
        parameters, parameters_len, values, declared_count, metadata,
        declared_count, &parameter_count);
    if (status != PRDB_SDK_SQL_DISPATCH_OK) {
        status = latch_sql_failure(host, status);
        goto cleanup;
    }
    normalize_parameter_payloads(values, parameter_count, parameter_payloads);

    result.host = host;
    result.columns = executor_columns;
    result.right_columns = right_executor_columns;
    result.column_count = PRDB_SDK_SQL_MAX_RELATION_COLUMNS;
    result.cell_scratch = cell_scratch;
    result.cell_scratch_capacity = PRDB_SDK_SQL_MAX_RELATION_COLUMNS;
    result.output = output;
    result.output_capacity = PRDB_SDK_SQL_RESULT_MAX_BYTES;
    if (is_grouped) {
        if (spill_host != NULL) {
            status = prdb_sdk_sql_execute_group_count_spill_v25(
                spill_host, schema, schema_len, sql, sql_len,
                SQL_DISPATCH_CANDIDATES, SQL_DISPATCH_BATCH_BYTES,
                planner_response, SQL_DISPATCH_RESPONSE_BYTES,
                executor_response, SQL_DISPATCH_RESPONSE_BYTES,
                batch_response, SQL_DISPATCH_BATCH_BYTES, planner_columns,
                PRDB_SDK_SQL_MAX_RELATION_COLUMNS, executor_columns,
                PRDB_SDK_SQL_MAX_RELATION_COLUMNS, cell_scratch,
                PRDB_SDK_SQL_MAX_RELATION_COLUMNS, write_group_sqlr_tuple,
                &result);
        } else {
            status = PRDB_SDK_SQL_EXEC_V25_UNSUPPORTED;
        }
        if (status == PRDB_SDK_SQL_EXEC_V25_UNSUPPORTED) {
            status = prdb_sdk_sql_execute_group_typed_v24(
                host, schema, schema_len, sql, sql_len, values, metadata,
                parameter_count, SQL_DISPATCH_CANDIDATES,
                SQL_DISPATCH_BATCH_BYTES, planner_response,
                SQL_DISPATCH_RESPONSE_BYTES, executor_response,
                SQL_DISPATCH_RESPONSE_BYTES, batch_response,
                SQL_DISPATCH_BATCH_BYTES, planner_columns,
                PRDB_SDK_SQL_MAX_RELATION_COLUMNS, executor_columns,
                PRDB_SDK_SQL_MAX_RELATION_COLUMNS, cell_scratch,
                PRDB_SDK_SQL_MAX_RELATION_COLUMNS, materialized_bytes,
                SQL_DISPATCH_BATCH_BYTES, write_group_sqlr_tuple, &result);
        }
    } else if (is_single_grouped) {
        status = prdb_sdk_sql_execute_group_typed_v10(
            host, schema, schema_len, sql, sql_len, values, metadata,
            parameter_count,
            SQL_DISPATCH_CANDIDATES, SQL_DISPATCH_BATCH_BYTES,
            planner_response, SQL_DISPATCH_RESPONSE_BYTES, executor_response,
            SQL_DISPATCH_RESPONSE_BYTES, batch_response,
            SQL_DISPATCH_BATCH_BYTES, planner_columns,
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, executor_columns,
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, cell_scratch,
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, materialized_bytes,
            SQL_DISPATCH_BATCH_BYTES, write_group_sqlr_row_v10, &result);
    } else if (is_single_key_join) {
        status = prdb_sdk_sql_execute_inner_join_v8(
            host, schema, schema_len, sql, sql_len, SQL_DISPATCH_CANDIDATES,
            SQL_DISPATCH_BATCH_BYTES, planner_response,
            SQL_DISPATCH_RESPONSE_BYTES, right_planner_response,
            SQL_DISPATCH_RESPONSE_BYTES, executor_response,
            SQL_DISPATCH_RESPONSE_BYTES, right_executor_response,
            SQL_DISPATCH_RESPONSE_BYTES, batch_response,
            SQL_DISPATCH_BATCH_BYTES, planner_columns,
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, right_planner_columns,
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, executor_columns,
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, right_executor_columns,
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, cell_scratch,
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, left_rows_v8,
            PRDB_SDK_SQL_EXEC_V8_MAX_ROWS, right_rows_v8,
            PRDB_SDK_SQL_EXEC_V8_MAX_ROWS, materialized_bytes,
            SQL_DISPATCH_BATCH_BYTES, write_join_sqlr_row_v8, &result);
    } else if (is_join) {
        if (is_ordered_join && spill_host != NULL) {
            const prdb_sdk_select_v8 *base_statement =
                &ordered_join_statement.base.base.base.base;
            prdb_sdk_executor_relation_v1 left_resolved = {0};
            prdb_sdk_executor_relation_v1 right_resolved = {0};
            prdb_host_api_v2 executor_host = {
                sizeof(prdb_host_api_v2), PRDB_QUERY_ABI_V2_VERSION,
                host->context, host->executor_request, host->planner_request,
                host->write_result};
            uint64_t response_len = 0;
            uint64_t synthetic_len = 0;
            uint32_t order_offset = ordered_join_statement.order_keyword_offset;
            const uint8_t limit_zero_clause[] = " LIMIT 0;";
            const uint8_t semicolon[] = ";";
            uint32_t suffix_len = base_statement->has_limit != 0 &&
                                          base_statement->limit == 0
                                      ? (uint32_t)sizeof(limit_zero_clause) - 1
                                      : (uint32_t)sizeof(semicolon) - 1;

            ordered_join_needs_latch = 1;
            if (order_offset > sql_len ||
                order_offset + suffix_len > PRDB_SDK_SQL_MAX_QUERY_BYTES) {
                status = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
                goto cleanup;
            }
            ordered_join_sql = (uint8_t *)dispatch_calloc(
                PRDB_SDK_SQL_MAX_QUERY_BYTES, sizeof(*ordered_join_sql),
                &allocated_heap_bytes);
            if (ordered_join_sql == NULL) {
                status = PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
                goto cleanup;
            }
            memcpy(ordered_join_sql, sql, order_offset);
            synthetic_len = order_offset;
            if (suffix_len == sizeof(limit_zero_clause) - 1) {
                memcpy(ordered_join_sql + synthetic_len, limit_zero_clause,
                       suffix_len);
            } else {
                memcpy(ordered_join_sql + synthetic_len, semicolon,
                       suffix_len);
            }
            synthetic_len += suffix_len;

            status = prdb_sdk_executor_resolve_relation_v1(
                &executor_host, schema, schema_len,
                sql + base_statement->left_relation.offset,
                base_statement->left_relation.length, executor_response,
                SQL_DISPATCH_RESPONSE_BYTES, &response_len, &left_resolved,
                sizeof(left_resolved), executor_columns,
                PRDB_SDK_SQL_MAX_RELATION_COLUMNS);
            if (status != PRDB_SDK_EXECUTOR_OK) {
                status = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
                goto cleanup;
            }
            status = prdb_sdk_executor_resolve_relation_v1(
                &executor_host, schema, schema_len,
                sql + base_statement->right_relation.offset,
                base_statement->right_relation.length,
                right_executor_response, SQL_DISPATCH_RESPONSE_BYTES,
                &response_len, &right_resolved, sizeof(right_resolved),
                right_executor_columns,
                PRDB_SDK_SQL_MAX_RELATION_COLUMNS);
            if (status != PRDB_SDK_EXECUTOR_OK) {
                status = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
                goto cleanup;
            }
            status = prdb_sdk_sql_bind_select_v25(
                &ordered_join_statement, left_resolved.name,
                left_resolved.name_len, executor_columns,
                left_resolved.column_count, right_resolved.name,
                right_resolved.name_len, right_executor_columns,
                right_resolved.column_count, &ordered_join_binding,
                sizeof(ordered_join_binding));
            if (status != PRDB_SDK_SQL_BIND_OK) {
                status = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
                goto cleanup;
            }

            ordered_join_rows.spill_host = spill_host;
            status = prdb_sdk_sql_execute_inner_join_typed_spill_v21(
                spill_host, schema, schema_len, ordered_join_sql,
                synthetic_len, values, metadata, parameter_count,
                SQL_DISPATCH_CANDIDATES, SQL_DISPATCH_BATCH_BYTES,
                planner_response, SQL_DISPATCH_RESPONSE_BYTES,
                right_planner_response, SQL_DISPATCH_RESPONSE_BYTES,
                executor_response, SQL_DISPATCH_RESPONSE_BYTES,
                right_executor_response, SQL_DISPATCH_RESPONSE_BYTES,
                batch_response, SQL_DISPATCH_BATCH_BYTES, planner_columns,
                PRDB_SDK_SQL_MAX_RELATION_COLUMNS, right_planner_columns,
                PRDB_SDK_SQL_MAX_RELATION_COLUMNS, executor_columns,
                PRDB_SDK_SQL_MAX_RELATION_COLUMNS, right_executor_columns,
                PRDB_SDK_SQL_MAX_RELATION_COLUMNS, cell_scratch,
                PRDB_SDK_SQL_MAX_RELATION_COLUMNS, left_rows,
                PRDB_SDK_SQL_EXEC_V8_MAX_ROWS, right_rows,
                PRDB_SDK_SQL_EXEC_V8_MAX_ROWS, materialized_bytes,
                SQL_DISPATCH_BATCH_BYTES, spool_ordered_join_row,
                &ordered_join_rows);
            if (status != PRDB_SDK_SQL_EXEC_OK)
                ordered_join_needs_latch = 0;
            if (status == PRDB_SDK_SQL_EXEC_OK &&
                ordered_join_rows.failure_status != 0)
                status = ordered_join_rows.failure_status;
            if (status == PRDB_SDK_SQL_EXEC_OK &&
                ordered_join_rows.row_count != 0) {
                ordered_join_sorter.spill_host = spill_host;
                ordered_join_sorter.host = host;
                ordered_join_sorter.statement = &ordered_join_statement;
                ordered_join_sorter.binding = &ordered_join_binding;
                ordered_join_sorter.spool = &ordered_join_rows;
                ordered_join_sorter.left_columns = executor_columns;
                ordered_join_sorter.left_column_count =
                    left_resolved.column_count;
                ordered_join_sorter.right_columns = right_executor_columns;
                ordered_join_sorter.right_column_count =
                    right_resolved.column_count;
                ordered_join_sorter.projection_count =
                    ordered_join_rows.projection_count;
                ordered_join_sorter.order_projection_index =
                    ordered_join_binding.order_by_projection_index;
                ordered_join_sorter.projected_values =
                    (prdb_sdk_value_v1 *)dispatch_calloc(
                        ordered_join_sorter.projection_count,
                        sizeof(*ordered_join_sorter.projected_values),
                        &allocated_heap_bytes);
                ordered_join_sorter.result_context = &result;
                if (ordered_join_sorter.projected_values == NULL ||
                    prepare_ordered_join_spool(
                        &ordered_join_sorter,
                        ordered_join_sorter.projection_count) != 0 ||
                    run_ordered_join_sort(&ordered_join_sorter) != 0) {
                    status = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
                }
            }
            if (status == PRDB_SDK_SQL_EXEC_OK)
                ordered_join_needs_latch = 0;
        } else if (spill_host != NULL && !is_ordered_join) {
            status = prdb_sdk_sql_execute_inner_join_typed_spill_v21(
                spill_host, schema, schema_len, sql, sql_len, values, metadata,
                parameter_count, SQL_DISPATCH_CANDIDATES,
                SQL_DISPATCH_BATCH_BYTES, planner_response,
                SQL_DISPATCH_RESPONSE_BYTES, right_planner_response,
                SQL_DISPATCH_RESPONSE_BYTES, executor_response,
                SQL_DISPATCH_RESPONSE_BYTES, right_executor_response,
                SQL_DISPATCH_RESPONSE_BYTES, batch_response,
                SQL_DISPATCH_BATCH_BYTES, planner_columns,
                PRDB_SDK_SQL_MAX_RELATION_COLUMNS, right_planner_columns,
                PRDB_SDK_SQL_MAX_RELATION_COLUMNS, executor_columns,
                PRDB_SDK_SQL_MAX_RELATION_COLUMNS, right_executor_columns,
                PRDB_SDK_SQL_MAX_RELATION_COLUMNS, cell_scratch,
                PRDB_SDK_SQL_MAX_RELATION_COLUMNS, left_rows,
                PRDB_SDK_SQL_EXEC_V8_MAX_ROWS, right_rows,
                PRDB_SDK_SQL_EXEC_V8_MAX_ROWS, materialized_bytes,
                SQL_DISPATCH_BATCH_BYTES, write_join_sqlr_row_v20, &result);
        } else {
            status = prdb_sdk_sql_execute_inner_join_typed_v20(
                host, schema, schema_len, sql, sql_len, values, metadata,
                parameter_count, SQL_DISPATCH_CANDIDATES,
                SQL_DISPATCH_BATCH_BYTES, planner_response,
                SQL_DISPATCH_RESPONSE_BYTES, right_planner_response,
                SQL_DISPATCH_RESPONSE_BYTES, executor_response,
                SQL_DISPATCH_RESPONSE_BYTES, right_executor_response,
                SQL_DISPATCH_RESPONSE_BYTES, batch_response,
                SQL_DISPATCH_BATCH_BYTES, planner_columns,
                PRDB_SDK_SQL_MAX_RELATION_COLUMNS, right_planner_columns,
                PRDB_SDK_SQL_MAX_RELATION_COLUMNS, executor_columns,
                PRDB_SDK_SQL_MAX_RELATION_COLUMNS, right_executor_columns,
                PRDB_SDK_SQL_MAX_RELATION_COLUMNS, cell_scratch,
                PRDB_SDK_SQL_MAX_RELATION_COLUMNS, left_rows,
                PRDB_SDK_SQL_EXEC_V8_MAX_ROWS, right_rows,
                PRDB_SDK_SQL_EXEC_V8_MAX_ROWS, materialized_bytes,
                SQL_DISPATCH_BATCH_BYTES, write_join_sqlr_row_v20, &result);
        }
    } else {
        if (is_ordered_row_select) {
            status = prdb_sdk_sql_execute_select_v4(
                spill_host, schema, schema_len, sql, sql_len, values,
                parameter_count, metadata, SQL_DISPATCH_CANDIDATES,
                SQL_DISPATCH_BATCH_BYTES, planner_response,
                SQL_DISPATCH_RESPONSE_BYTES, executor_response,
                SQL_DISPATCH_RESPONSE_BYTES, batch_response,
                SQL_DISPATCH_BATCH_BYTES, planner_columns,
                PRDB_SDK_SQL_MAX_RELATION_COLUMNS, executor_columns,
                PRDB_SDK_SQL_MAX_RELATION_COLUMNS, cell_scratch,
                PRDB_SDK_SQL_MAX_RELATION_COLUMNS, write_sqlr_row, &result);
        } else {
            status = prdb_sdk_sql_execute_select_typed_v1(
                host, schema, schema_len, sql, sql_len, values, metadata,
                parameter_count, SQL_DISPATCH_CANDIDATES,
                SQL_DISPATCH_BATCH_BYTES, planner_response,
                SQL_DISPATCH_RESPONSE_BYTES, executor_response,
                SQL_DISPATCH_RESPONSE_BYTES, batch_response,
                SQL_DISPATCH_BATCH_BYTES, planner_columns,
                PRDB_SDK_SQL_MAX_RELATION_COLUMNS, executor_columns,
                PRDB_SDK_SQL_MAX_RELATION_COLUMNS, cell_scratch,
                PRDB_SDK_SQL_MAX_RELATION_COLUMNS, write_sqlr_row, &result);
        }
    }
    if (status != PRDB_SDK_SQL_EXEC_OK) {
        status = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
    } else {
        status = PRDB_SDK_SQL_DISPATCH_OK;
    }

cleanup:
    if (ordered_join_needs_latch)
        status = latch_sql_failure(host, status);
    if (ordered_join_rows.stream.valid == 1 &&
        prdb_sdk_spill_destroy_v1(spill_host, &ordered_join_rows.stream) != 0 &&
        status == PRDB_SDK_SQL_DISPATCH_OK) {
        status = latch_sql_failure(
            host, PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR);
    }
    free(ordered_join_sorter.projected_values);
    free(ordered_join_sql);
    free(values);
    free(parameter_payloads);
    free(metadata);
    free(planner_response);
    free(right_planner_response);
    free(executor_response);
    free(right_executor_response);
    free(batch_response);
    free(output);
    free(materialized_bytes);
    free(right_planner_columns);
    free(right_executor_columns);
    free(left_rows);
    free(right_rows);
    free(left_rows_v8);
    free(right_rows_v8);
    free(planner_columns);
    free(executor_columns);
    free(cell_scratch);
    return status;
}

int32_t prdb_sdk_sql_run_v1(
    const prdb_host_api_v3 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const uint8_t *parameters, uint64_t parameters_len) {
    return sql_run_internal(host, NULL, schema, schema_len, sql, sql_len,
                            parameters, parameters_len);
}

static int32_t prdb_sdk_sql_run_v2(
    const prdb_host_api_v6 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const uint8_t *parameters, uint64_t parameters_len) {
    if (host == NULL ||
        host->v5.v4.v3.struct_size < sizeof(prdb_host_api_v3) ||
        host->v5.v4.v3.abi_version != PRDB_QUERY_ABI_V3_VERSION)
        return PRDB_SDK_SQL_DISPATCH_INVALID_ARGUMENT;
    if (host->spill_request == NULL)
        return prdb_sdk_sql_run_statement_v1(
            &host->v5.v4.v3, schema, schema_len, sql, sql_len, parameters,
            parameters_len);
    return sql_run_internal(&host->v5.v4.v3, host, schema, schema_len, sql,
                            sql_len, parameters, parameters_len);
}

static int append_join_sql_span(const uint8_t *sql, uint64_t sql_len,
                                prdb_sdk_sql_span_v1 span, uint8_t *output,
                                uint64_t capacity, uint64_t *used) {
    if (span.offset > sql_len || span.length > sql_len - span.offset ||
        span.length > capacity - *used) {
        return 0;
    }
    memcpy(output + *used, sql + span.offset, span.length);
    *used += span.length;
    return 1;
}

static int append_join_sql_bytes(const uint8_t *bytes, uint32_t length,
                                 uint8_t *output, uint64_t capacity,
                                 uint64_t *used) {
    if (length > capacity - *used) return 0;
    memcpy(output + *used, bytes, length);
    *used += length;
    return 1;
}

static int make_single_key_spill_join_sql(
    const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_select_v8 *statement, uint8_t *output,
    uint64_t output_capacity, uint64_t *output_len) {
    static const uint8_t select_kw[] = "SELECT ";
    static const uint8_t comma[] = ", ";
    static const uint8_t from_kw[] = " FROM ";
    static const uint8_t join_kw[] = " INNER JOIN ";
    static const uint8_t on_kw[] = " ON ";
    static const uint8_t dot[] = ".";
    static const uint8_t equals[] = " = ";
    static const uint8_t and_kw[] = " AND ";
    static const uint8_t limit_kw[] = " LIMIT ";
    uint64_t used = 0;
    uint8_t digits[10];
    uint32_t digit_count = 0;
    uint32_t limit = statement->limit;

    *output_len = 0;
    if (!append_join_sql_bytes(select_kw, sizeof(select_kw) - 1, output,
                               output_capacity, &used))
        return 0;
    for (uint32_t index = 0; index < statement->projection_count; ++index) {
        if (index != 0 && !append_join_sql_bytes(
                              comma, sizeof(comma) - 1, output,
                              output_capacity, &used))
            return 0;
        if (!append_join_sql_span(sql, sql_len,
                                  statement->projections[index].relation,
                                  output, output_capacity, &used) ||
            !append_join_sql_bytes(dot, sizeof(dot) - 1, output,
                                   output_capacity, &used) ||
            !append_join_sql_span(sql, sql_len,
                                  statement->projections[index].column,
                                  output, output_capacity, &used))
            return 0;
    }
    if (!append_join_sql_bytes(from_kw, sizeof(from_kw) - 1, output,
                               output_capacity, &used) ||
        !append_join_sql_span(sql, sql_len, statement->left_relation, output,
                              output_capacity, &used) ||
        !append_join_sql_bytes(join_kw, sizeof(join_kw) - 1, output,
                               output_capacity, &used) ||
        !append_join_sql_span(sql, sql_len, statement->right_relation, output,
                              output_capacity, &used) ||
        !append_join_sql_bytes(on_kw, sizeof(on_kw) - 1, output,
                               output_capacity, &used))
        return 0;
    for (uint32_t repetition = 0; repetition < 2; ++repetition) {
        if (repetition != 0 && !append_join_sql_bytes(
                                   and_kw, sizeof(and_kw) - 1, output,
                                   output_capacity, &used))
            return 0;
        if (!append_join_sql_span(sql, sql_len,
                                  statement->left_join_key.relation, output,
                                  output_capacity, &used) ||
            !append_join_sql_bytes(dot, sizeof(dot) - 1, output,
                                   output_capacity, &used) ||
            !append_join_sql_span(sql, sql_len,
                                  statement->left_join_key.column, output,
                                  output_capacity, &used) ||
            !append_join_sql_bytes(equals, sizeof(equals) - 1, output,
                                   output_capacity, &used) ||
            !append_join_sql_span(sql, sql_len,
                                  statement->right_join_key.relation, output,
                                  output_capacity, &used) ||
            !append_join_sql_bytes(dot, sizeof(dot) - 1, output,
                                   output_capacity, &used) ||
            !append_join_sql_span(sql, sql_len,
                                  statement->right_join_key.column, output,
                                  output_capacity, &used))
            return 0;
    }
    if (statement->has_limit != 0) {
        if (!append_join_sql_bytes(limit_kw, sizeof(limit_kw) - 1, output,
                                   output_capacity, &used))
            return 0;
        do {
            digits[digit_count++] = (uint8_t)('0' + (limit % 10));
            limit /= 10;
        } while (limit != 0);
        while (digit_count != 0) {
            uint8_t digit = digits[--digit_count];
            if (!append_join_sql_bytes(&digit, 1, output, output_capacity,
                                       &used))
                return 0;
        }
    }
    if (used == 0 || used > output_capacity) return 0;
    *output_len = used;
    return 1;
}

int32_t prdb_sdk_sql_run_statement_v1(
    const prdb_host_api_v3 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const uint8_t *parameters, uint64_t parameters_len) {
    prdb_sdk_insert_v3 insert_statement = {0};
    prdb_sdk_update_v1 update_statement = {0};
    prdb_sdk_delete_v1 delete_statement = {0};
    prdb_sdk_value_v1 *values = NULL;
    uint8_t (*parameter_payloads)[16] = NULL;
    prdb_sdk_sql_parameter_metadata_v1 *metadata = NULL;
    uint8_t *planner_response = NULL;
    uint8_t *executor_response = NULL;
    uint8_t *batch_response = NULL;
    uint8_t *mutation_request = NULL;
    prdb_sdk_sql_column_shape_v2 *planner_columns = NULL;
    prdb_sdk_sql_column_shape_v2 *executor_columns = NULL;
    prdb_sdk_executor_insert_value_v1 *insert_values = NULL;
    prdb_sdk_select_binding_v2 *binding = NULL;
    prdb_sdk_value_v1 *cell_scratch = NULL;
    uint8_t *target_handles = NULL;
    prdb_sdk_executor_update_patch_v1 *patches = NULL;
    uint32_t parameter_count = 0;
    uint32_t declared_count = 0;
    uint64_t allocated_heap_bytes = 0;
    int32_t parse_status;
    int32_t status;

    if (host == NULL || host->struct_size < sizeof(*host) ||
        host->abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->statement_failure == NULL || schema == NULL || schema_len == 0 ||
        schema_len > 128 || sql == NULL || sql_len == 0 ||
        sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        parameters_len > PRDB_SDK_SQL_DISPATCH_MAX_PARAMETER_BYTES ||
        (parameters_len != 0 && parameters == NULL)) {
        return latch_sql_failure(host,
                                 PRDB_SDK_SQL_DISPATCH_INVALID_ARGUMENT);
    }

    parse_status = prdb_sdk_sql_parse_insert_v3(
        sql, sql_len, &insert_statement, sizeof(insert_statement), NULL);
    if (parse_status != PRDB_SDK_SQL_OK) {
        parse_status = prdb_sdk_sql_parse_update_v1(
            sql, sql_len, &update_statement, sizeof(update_statement), NULL);
        if (parse_status != PRDB_SDK_SQL_OK) {
            parse_status = prdb_sdk_sql_parse_delete_v1(
                sql, sql_len, &delete_statement, sizeof(delete_statement), NULL);
        }
    }
    if (parse_status != PRDB_SDK_SQL_OK) {
        return prdb_sdk_sql_run_v1(host, schema, schema_len, sql, sql_len,
                                   parameters, parameters_len);
    }

    if (parameters_len >= 8 && parameters != NULL &&
        memcmp(parameters, "PSP1", 4) == 0) {
        declared_count = read_u16_be(parameters + 4);
    }
    if (declared_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX) {
        return latch_sql_failure(host,
                                 PRDB_SDK_SQL_DISPATCH_INVALID_PARAMETERS);
    }
    if (declared_count != 0) {
        values = (prdb_sdk_value_v1 *)dispatch_calloc(
            declared_count, sizeof(*values), &allocated_heap_bytes);
        parameter_payloads = (uint8_t (*)[16])dispatch_calloc(
            declared_count, sizeof(*parameter_payloads), &allocated_heap_bytes);
        metadata = (prdb_sdk_sql_parameter_metadata_v1 *)dispatch_calloc(
            declared_count, sizeof(*metadata), &allocated_heap_bytes);
    }
    planner_response = (uint8_t *)dispatch_calloc(
        1, SQL_DISPATCH_RESPONSE_BYTES, &allocated_heap_bytes);
    executor_response = (uint8_t *)dispatch_calloc(
        1, SQL_DISPATCH_RESPONSE_BYTES, &allocated_heap_bytes);
    batch_response = (uint8_t *)dispatch_calloc(
        1, SQL_DISPATCH_BATCH_BYTES, &allocated_heap_bytes);
    mutation_request = (uint8_t *)dispatch_calloc(
        1, PRDB_QUERY_MAX_IPC_REQUEST_BYTES, &allocated_heap_bytes);
    planner_columns = (prdb_sdk_sql_column_shape_v2 *)dispatch_calloc(
        PRDB_SDK_SQL_MAX_RELATION_COLUMNS, sizeof(*planner_columns),
        &allocated_heap_bytes);
    executor_columns = (prdb_sdk_sql_column_shape_v2 *)dispatch_calloc(
        PRDB_SDK_SQL_MAX_RELATION_COLUMNS, sizeof(*executor_columns),
        &allocated_heap_bytes);
    if (parse_status == PRDB_SDK_SQL_OK &&
        (declared_count != 0 && (values == NULL || metadata == NULL ||
         parameter_payloads == NULL))) {
        status = PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
        goto resource_error;
    }
    if (planner_response == NULL || executor_response == NULL ||
        batch_response == NULL || mutation_request == NULL ||
        planner_columns == NULL || executor_columns == NULL) {
        status = PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
        goto resource_error;
    }
    status = prdb_sdk_sql_decode_parameters_v1(
        parameters, parameters_len, values, declared_count, metadata,
        declared_count, &parameter_count);
    if (status != PRDB_SDK_SQL_DISPATCH_OK) {
        status = latch_sql_failure(host, status);
        goto cleanup;
    }
    normalize_parameter_payloads(values, parameter_count, parameter_payloads);

    if (prdb_sdk_sql_parse_insert_v3(
            sql, sql_len, &insert_statement, sizeof(insert_statement), NULL) ==
        PRDB_SDK_SQL_OK) {
        uint64_t insert_cell_capacity =
            (uint64_t)insert_statement.base.row_count *
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS;
        if (insert_cell_capacity > UINT32_MAX) {
            status = PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
            goto resource_error;
        }
        insert_values = (prdb_sdk_executor_insert_value_v1 *)dispatch_calloc(
            (uint32_t)insert_cell_capacity, sizeof(*insert_values),
            &allocated_heap_bytes);
        if (insert_values == NULL) {
            status = PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
            goto resource_error;
        }
        uint32_t inserted_count = 0;
        status = prdb_sdk_sql_execute_insert_v2(
            host, schema, schema_len, sql, sql_len, values, parameter_count,
            planner_response, SQL_DISPATCH_RESPONSE_BYTES, executor_response,
            SQL_DISPATCH_RESPONSE_BYTES, mutation_request,
            PRDB_QUERY_MAX_IPC_REQUEST_BYTES, planner_columns,
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, executor_columns,
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, insert_values,
            (uint32_t)insert_cell_capacity, &inserted_count);
    } else if (prdb_sdk_sql_parse_update_v1(
                   sql, sql_len, &update_statement, sizeof(update_statement),
                   NULL) == PRDB_SDK_SQL_OK) {
        binding = (prdb_sdk_select_binding_v2 *)dispatch_calloc(
            1, sizeof(*binding), &allocated_heap_bytes);
        cell_scratch = (prdb_sdk_value_v1 *)dispatch_calloc(
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, sizeof(*cell_scratch),
            &allocated_heap_bytes);
        target_handles = (uint8_t *)dispatch_calloc(
            PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS, 16,
            &allocated_heap_bytes);
        patches = (prdb_sdk_executor_update_patch_v1 *)dispatch_calloc(
            PRDB_SDK_SQL_MAX_UPDATE_ASSIGNMENTS, sizeof(*patches),
            &allocated_heap_bytes);
        if (binding == NULL || cell_scratch == NULL || target_handles == NULL ||
            patches == NULL) {
            status = PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
            goto resource_error;
        }
        uint32_t updated_count = 0;
        status = prdb_sdk_sql_execute_update_v1(
            host, schema, schema_len, sql, sql_len, values, parameter_count,
            SQL_DISPATCH_CANDIDATES, SQL_DISPATCH_BATCH_BYTES,
            planner_response, SQL_DISPATCH_RESPONSE_BYTES, executor_response,
            SQL_DISPATCH_RESPONSE_BYTES, batch_response,
            SQL_DISPATCH_BATCH_BYTES, mutation_request,
            PRDB_QUERY_MAX_IPC_REQUEST_BYTES, planner_columns,
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, executor_columns,
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, binding,
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, cell_scratch,
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, target_handles,
            PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS, patches,
            PRDB_SDK_SQL_MAX_UPDATE_ASSIGNMENTS, &updated_count);
    } else {
        binding = (prdb_sdk_select_binding_v2 *)dispatch_calloc(
            1, sizeof(*binding), &allocated_heap_bytes);
        cell_scratch = (prdb_sdk_value_v1 *)dispatch_calloc(
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, sizeof(*cell_scratch),
            &allocated_heap_bytes);
        target_handles = (uint8_t *)dispatch_calloc(
            PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS, 16,
            &allocated_heap_bytes);
        if (binding == NULL || cell_scratch == NULL || target_handles == NULL) {
            status = PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR;
            goto resource_error;
        }
        uint32_t deleted_count = 0;
        status = prdb_sdk_sql_execute_delete_v1(
            host, schema, schema_len, sql, sql_len, values, parameter_count,
            SQL_DISPATCH_CANDIDATES, SQL_DISPATCH_BATCH_BYTES,
            planner_response, SQL_DISPATCH_RESPONSE_BYTES, executor_response,
            SQL_DISPATCH_RESPONSE_BYTES, batch_response,
            SQL_DISPATCH_BATCH_BYTES, planner_columns,
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, executor_columns,
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, binding,
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, cell_scratch,
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, target_handles,
            PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS, &deleted_count);
    }
    status = status == PRDB_SDK_SQL_EXEC_OK
                 ? PRDB_SDK_SQL_DISPATCH_OK
                 : PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
    goto cleanup;

resource_error:
    status = latch_sql_failure(host, status);
cleanup:
    free(values);
    free(parameter_payloads);
    free(metadata);
    free(planner_response);
    free(executor_response);
    free(batch_response);
    free(mutation_request);
    free(planner_columns);
    free(executor_columns);
    free(insert_values);
    free(binding);
    free(cell_scratch);
    free(target_handles);
    free(patches);
    return status;
}

int32_t prdb_sdk_sql_run_statement_v2(
    const prdb_host_api_v6 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const uint8_t *parameters, uint64_t parameters_len) {
    prdb_sdk_select_v3 ordered_row_statement = {0};
    prdb_sdk_select_v20 spill_join_statement = {0};
    prdb_sdk_select_v25 ordered_join_statement = {0};
    prdb_sdk_select_v8 single_key_spill_statement = {0};
    uint8_t synthetic_join_sql[PRDB_SDK_SQL_MAX_QUERY_BYTES];
    uint64_t synthetic_join_sql_len = 0;
    if (host == NULL ||
        host->v5.v4.v3.struct_size < sizeof(prdb_host_api_v3) ||
        host->v5.v4.v3.abi_version != PRDB_QUERY_ABI_V3_VERSION)
        return PRDB_SDK_SQL_DISPATCH_INVALID_ARGUMENT;
    if (host->spill_request == NULL)
        return prdb_sdk_sql_run_statement_v1(
            &host->v5.v4.v3, schema, schema_len, sql, sql_len, parameters,
            parameters_len);
    if (sql != NULL &&
        prdb_sdk_sql_parse_select_v25(
            sql, sql_len, &ordered_join_statement,
            sizeof(ordered_join_statement), NULL) == PRDB_SDK_SQL_OK) {
        return prdb_sdk_sql_run_v2(host, schema, schema_len, sql, sql_len,
                                   parameters, parameters_len);
    }
    if (sql != NULL &&
        prdb_sdk_sql_parse_select_v20(
            sql, sql_len, &spill_join_statement,
            sizeof(spill_join_statement), NULL) == PRDB_SDK_SQL_OK) {
        return prdb_sdk_sql_run_v2(host, schema, schema_len, sql, sql_len,
                                   parameters, parameters_len);
    }
    if (sql != NULL &&
        prdb_sdk_sql_parse_select_v8(
            sql, sql_len, &single_key_spill_statement,
            sizeof(single_key_spill_statement), NULL) == PRDB_SDK_SQL_OK) {
        if (!make_single_key_spill_join_sql(
                sql, sql_len, &single_key_spill_statement,
                synthetic_join_sql, sizeof(synthetic_join_sql),
                &synthetic_join_sql_len)) {
            return latch_sql_failure(&host->v5.v4.v3,
                                     PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR);
        }
        return prdb_sdk_sql_run_v2(
            host, schema, schema_len, synthetic_join_sql,
            synthetic_join_sql_len, parameters, parameters_len);
    }
    if (sql != NULL &&
        prdb_sdk_sql_parse_select_v3(
            sql, sql_len, &ordered_row_statement,
            sizeof(ordered_row_statement), NULL) == PRDB_SDK_SQL_OK &&
        ordered_row_statement.has_order_by != 0) {
        return prdb_sdk_sql_run_v2(host, schema, schema_len, sql, sql_len,
                                   parameters, parameters_len);
    }
    if (sql != NULL) {
        prdb_sdk_select_v24 grouped_statement = {0};
        prdb_sdk_select_v10 single_grouped_statement = {0};
        if (prdb_sdk_sql_parse_select_v24(
                sql, sql_len, &grouped_statement,
                sizeof(grouped_statement), NULL) == PRDB_SDK_SQL_OK ||
            prdb_sdk_sql_parse_select_v10(
                sql, sql_len, &single_grouped_statement,
                sizeof(single_grouped_statement), NULL) == PRDB_SDK_SQL_OK) {
            return sql_run_internal(&host->v5.v4.v3, host, schema, schema_len,
                                    sql, sql_len, parameters,
                                    parameters_len);
        }
    }
    return prdb_sdk_sql_run_statement_v1(
        &host->v5.v4.v3, schema, schema_len, sql, sql_len, parameters,
        parameters_len);
}

int32_t prdb_sdk_sql_run_statement_optimizer_v1(
    const prdb_host_api_v4 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const uint8_t *parameters, uint64_t parameters_len) {
    prdb_sdk_select_v26 statement = {0};
    prdb_sdk_executor_relation_v1 relation = {0};
    prdb_sdk_sql_column_shape_v2 *columns = NULL;
    prdb_sdk_value_v1 *source_cells = NULL;
    prdb_sdk_value_v1 *projected_values = NULL;
    prdb_sdk_value_v1 *values = NULL;
    uint8_t (*parameter_payloads)[16] = NULL;
    prdb_sdk_sql_parameter_metadata_v1 *metadata = NULL;
    uint8_t *request = NULL;
    uint8_t *optimizer_response = NULL;
    uint8_t *relation_response = NULL;
    uint8_t *scan_response = NULL;
    uint8_t *batch_response = NULL;
    uint8_t *result_buffer = NULL;
    uint32_t parameter_count = 0;
    uint32_t declared_count = 0;
    uint32_t delivered_rows = 0;
    uint64_t allocated_heap_bytes = 0;
    int32_t status;

    if (host == NULL || host->v3.struct_size < sizeof(host->v3) ||
        host->v3.abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->optimizer_request == NULL || host->v3.statement_failure == NULL ||
        schema == NULL || schema_len == 0 || schema_len > 128 || sql == NULL ||
        sql_len == 0 || sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        parameters_len > PRDB_SDK_SQL_DISPATCH_MAX_PARAMETER_BYTES ||
        (parameters_len != 0 && parameters == NULL)) {
        return latch_sql_failure(host == NULL ? NULL : &host->v3,
                                 PRDB_SDK_SQL_DISPATCH_INVALID_ARGUMENT);
    }
    status = prdb_sdk_sql_parse_select_v26(
        sql, sql_len, &statement, sizeof(statement), NULL);
    if (status != PRDB_SDK_SQL_OK) {
        return latch_sql_failure(&host->v3,
                                 PRDB_SDK_SQL_DISPATCH_INVALID_ARGUMENT);
    }
    if (parameters_len >= 8 && memcmp(parameters, "PSP1", 4) == 0)
        declared_count = read_u16_be(parameters + 4);
    if (declared_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX)
        return latch_sql_failure(&host->v3,
                                 PRDB_SDK_SQL_DISPATCH_INVALID_PARAMETERS);
    if (declared_count != 0) {
        values = (prdb_sdk_value_v1 *)dispatch_calloc(
            declared_count, sizeof(*values), &allocated_heap_bytes);
        parameter_payloads = (uint8_t (*)[16])dispatch_calloc(
            declared_count, sizeof(*parameter_payloads), &allocated_heap_bytes);
        metadata = (prdb_sdk_sql_parameter_metadata_v1 *)dispatch_calloc(
            declared_count, sizeof(*metadata), &allocated_heap_bytes);
    }
    request = (uint8_t *)dispatch_calloc(
        1, PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES, &allocated_heap_bytes);
    optimizer_response = (uint8_t *)dispatch_calloc(
        1, SQL_DISPATCH_RESPONSE_BYTES, &allocated_heap_bytes);
    relation_response = (uint8_t *)dispatch_calloc(
        1, SQL_DISPATCH_RESPONSE_BYTES, &allocated_heap_bytes);
    scan_response = (uint8_t *)dispatch_calloc(
        1, SQL_DISPATCH_RESPONSE_BYTES, &allocated_heap_bytes);
    batch_response = (uint8_t *)dispatch_calloc(
        1, SQL_DISPATCH_BATCH_BYTES, &allocated_heap_bytes);
    result_buffer = (uint8_t *)dispatch_calloc(
        1, PRDB_SDK_SQL_RESULT_MAX_BYTES, &allocated_heap_bytes);
    columns = (prdb_sdk_sql_column_shape_v2 *)dispatch_calloc(
        PRDB_SDK_SQL_MAX_RELATION_COLUMNS, sizeof(*columns),
        &allocated_heap_bytes);
    source_cells = (prdb_sdk_value_v1 *)dispatch_calloc(
        PRDB_SDK_SQL_MAX_RELATION_COLUMNS, sizeof(*source_cells),
        &allocated_heap_bytes);
    projected_values = (prdb_sdk_value_v1 *)dispatch_calloc(
        PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS, sizeof(*projected_values),
        &allocated_heap_bytes);
    if (request == NULL || optimizer_response == NULL ||
        relation_response == NULL || scan_response == NULL ||
        batch_response == NULL || result_buffer == NULL || columns == NULL ||
        source_cells == NULL || projected_values == NULL ||
        (declared_count != 0 &&
         (values == NULL || parameter_payloads == NULL || metadata == NULL))) {
        status = latch_sql_failure(&host->v3,
                                   PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR);
        goto cleanup_optimizer;
    }
    status = prdb_sdk_sql_decode_parameters_v1(
        parameters, parameters_len, values, declared_count, metadata,
        declared_count, &parameter_count);
    if (status != PRDB_SDK_SQL_DISPATCH_OK) {
        status = latch_sql_failure(&host->v3, status);
        goto cleanup_optimizer;
    }
    normalize_parameter_payloads(values, parameter_count, parameter_payloads);
    status = prdb_sdk_optimizer_execute_select_ast_with_parameters_to_result_v1(
        host, &statement.base, values, parameter_count, schema, schema_len,
        SQL_DISPATCH_CANDIDATES, SQL_DISPATCH_BATCH_BYTES, request,
        PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES, optimizer_response,
        SQL_DISPATCH_RESPONSE_BYTES, relation_response,
        SQL_DISPATCH_RESPONSE_BYTES, scan_response, SQL_DISPATCH_RESPONSE_BYTES,
        batch_response, SQL_DISPATCH_BATCH_BYTES, &relation, sizeof(relation),
        columns, PRDB_SDK_SQL_MAX_RELATION_COLUMNS, source_cells,
        PRDB_SDK_SQL_MAX_RELATION_COLUMNS, projected_values,
        PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS, result_buffer,
        PRDB_SDK_SQL_RESULT_MAX_BYTES, &delivered_rows);
    status = status == PRDB_SDK_OPTIMIZER_PLAN_OK
                 ? PRDB_SDK_SQL_DISPATCH_OK
                 : PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;

cleanup_optimizer:
    free(values);
    free(parameter_payloads);
    free(metadata);
    free(request);
    free(optimizer_response);
    free(relation_response);
    free(scan_response);
    free(batch_response);
    free(result_buffer);
    free(columns);
    free(source_cells);
    free(projected_values);
    return status;
}
