#include "prdb_sdk_sql_dispatch_v1.h"
#include "prdb_sdk_sql_dml_v3.h"
#include "prdb_sdk_sql_exec_v10.h"
#include "prdb_sdk_sql_exec_v8.h"
#include "prdb_sdk_sql_exec_v20.h"
#include "prdb_sdk_sql_exec_v24.h"

#include <stdlib.h>
#include <string.h>

#define SQL_DISPATCH_RESPONSE_BYTES UINT64_C(1048576)
#define SQL_DISPATCH_BATCH_BYTES UINT32_C(1048576)
#define SQL_DISPATCH_CANDIDATES UINT16_C(128)

typedef struct sql_result_context {
    const prdb_host_api_v3 *host;
    prdb_sdk_sql_column_shape_v2 *columns;
    prdb_sdk_sql_column_shape_v2 *right_columns;
    uint32_t column_count;
    prdb_sdk_value_v1 *cell_scratch;
    uint32_t cell_scratch_capacity;
    uint8_t *output;
    uint64_t output_capacity;
} sql_result_context;

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
    if (group_key_count == 1)
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

static int32_t latch_sql_failure(const prdb_host_api_v3 *host, int32_t status) {
    if (host != NULL && host->statement_failure != NULL) {
        (void)host->statement_failure(host->context,
                                      PRDB_QUERY_STATEMENT_FAILURE_SQL);
    }
    return status;
}

int32_t prdb_sdk_sql_run_v1(
    const prdb_host_api_v3 *host, const uint8_t *schema,
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
    prdb_sdk_select_v24 grouped_statement = {0};
    prdb_sdk_select_v20 join_statement = {0};
    prdb_sdk_select_v8 single_key_join_statement = {0};
    prdb_sdk_select_v10 single_grouped_statement = {0};
    int is_grouped = 0;
    int is_join = 0;
    int is_single_key_join = 0;
    int is_single_grouped = 0;
    uint32_t parameter_count = 0;
    uint32_t declared_count = 0;
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
        is_join = prdb_sdk_sql_parse_select_v20(
                      sql, sql_len, &join_statement, sizeof(join_statement),
                      NULL) == PRDB_SDK_SQL_OK;
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
        materialized_bytes = (uint8_t *)calloc(1, SQL_DISPATCH_BATCH_BYTES);
        if (materialized_bytes == NULL) {
            return latch_sql_failure(host,
                                     PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR);
        }
    }
    if (parameters_len >= 8 && memcmp(parameters, "PSP1", 4) == 0) {
        declared_count = read_u16_be(parameters + 4);
    }
    if (declared_count != 0) {
        values = (prdb_sdk_value_v1 *)calloc(declared_count, sizeof(*values));
        parameter_payloads = calloc(declared_count, sizeof(*parameter_payloads));
        metadata = (prdb_sdk_sql_parameter_metadata_v1 *)calloc(
            declared_count, sizeof(*metadata));
    }
    planner_response = (uint8_t *)calloc(1, SQL_DISPATCH_RESPONSE_BYTES);
    if (is_join || is_single_key_join) {
        right_planner_response =
            (uint8_t *)calloc(1, SQL_DISPATCH_RESPONSE_BYTES);
        right_executor_response =
            (uint8_t *)calloc(1, SQL_DISPATCH_RESPONSE_BYTES);
    }
    executor_response = (uint8_t *)calloc(1, SQL_DISPATCH_RESPONSE_BYTES);
    batch_response = (uint8_t *)calloc(1, SQL_DISPATCH_BATCH_BYTES);
    output = (uint8_t *)calloc(1, PRDB_SDK_SQL_RESULT_MAX_BYTES);
    planner_columns = (prdb_sdk_sql_column_shape_v2 *)calloc(
        PRDB_SDK_SQL_MAX_RELATION_COLUMNS, sizeof(*planner_columns));
    executor_columns = (prdb_sdk_sql_column_shape_v2 *)calloc(
        PRDB_SDK_SQL_MAX_RELATION_COLUMNS, sizeof(*executor_columns));
    if (is_join) {
        right_planner_columns = (prdb_sdk_sql_column_shape_v2 *)calloc(
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, sizeof(*right_planner_columns));
        right_executor_columns = (prdb_sdk_sql_column_shape_v2 *)calloc(
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, sizeof(*right_executor_columns));
        left_rows = (prdb_sdk_sql_join_materialized_row_v16 *)calloc(
            PRDB_SDK_SQL_EXEC_V8_MAX_ROWS, sizeof(*left_rows));
        right_rows = (prdb_sdk_sql_join_materialized_row_v16 *)calloc(
            PRDB_SDK_SQL_EXEC_V8_MAX_ROWS, sizeof(*right_rows));
    } else if (is_single_key_join) {
        right_planner_columns = (prdb_sdk_sql_column_shape_v2 *)calloc(
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, sizeof(*right_planner_columns));
        right_executor_columns = (prdb_sdk_sql_column_shape_v2 *)calloc(
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, sizeof(*right_executor_columns));
        left_rows_v8 = (prdb_sdk_sql_join_materialized_row_v8 *)calloc(
            PRDB_SDK_SQL_EXEC_V8_MAX_ROWS, sizeof(*left_rows_v8));
        right_rows_v8 = (prdb_sdk_sql_join_materialized_row_v8 *)calloc(
            PRDB_SDK_SQL_EXEC_V8_MAX_ROWS, sizeof(*right_rows_v8));
    }
    cell_scratch = (prdb_sdk_value_v1 *)calloc(
        PRDB_SDK_SQL_MAX_RELATION_COLUMNS, sizeof(*cell_scratch));
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
        status = prdb_sdk_sql_execute_group_typed_v24(
            host, schema, schema_len, sql, sql_len, values, metadata,
            parameter_count,
            SQL_DISPATCH_CANDIDATES, SQL_DISPATCH_BATCH_BYTES,
            planner_response, SQL_DISPATCH_RESPONSE_BYTES, executor_response,
            SQL_DISPATCH_RESPONSE_BYTES, batch_response,
            SQL_DISPATCH_BATCH_BYTES, planner_columns,
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, executor_columns,
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, cell_scratch,
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, materialized_bytes,
            SQL_DISPATCH_BATCH_BYTES, write_group_sqlr_tuple, &result);
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
        status = prdb_sdk_sql_execute_inner_join_v20(
            host, schema, schema_len, sql, sql_len, values, parameter_count,
            SQL_DISPATCH_CANDIDATES, SQL_DISPATCH_BATCH_BYTES,
            planner_response, SQL_DISPATCH_RESPONSE_BYTES,
            right_planner_response, SQL_DISPATCH_RESPONSE_BYTES,
            executor_response, SQL_DISPATCH_RESPONSE_BYTES,
            right_executor_response, SQL_DISPATCH_RESPONSE_BYTES,
            batch_response,
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
        status = prdb_sdk_sql_execute_select_typed_v1(
            host, schema, schema_len, sql, sql_len, values, metadata,
            parameter_count, SQL_DISPATCH_CANDIDATES, SQL_DISPATCH_BATCH_BYTES,
            planner_response, SQL_DISPATCH_RESPONSE_BYTES, executor_response,
            SQL_DISPATCH_RESPONSE_BYTES, batch_response,
            SQL_DISPATCH_BATCH_BYTES, planner_columns,
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, executor_columns,
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, cell_scratch,
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, write_sqlr_row, &result);
    }
    if (status != PRDB_SDK_SQL_EXEC_OK) {
        status = PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR;
    } else {
        status = PRDB_SDK_SQL_DISPATCH_OK;
    }

cleanup:
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
        values = (prdb_sdk_value_v1 *)calloc(declared_count, sizeof(*values));
        parameter_payloads = calloc(declared_count, sizeof(*parameter_payloads));
        metadata = (prdb_sdk_sql_parameter_metadata_v1 *)calloc(
            declared_count, sizeof(*metadata));
    }
    planner_response = (uint8_t *)calloc(1, SQL_DISPATCH_RESPONSE_BYTES);
    executor_response = (uint8_t *)calloc(1, SQL_DISPATCH_RESPONSE_BYTES);
    batch_response = (uint8_t *)calloc(1, SQL_DISPATCH_BATCH_BYTES);
    mutation_request = (uint8_t *)calloc(1, PRDB_QUERY_MAX_IPC_REQUEST_BYTES);
    planner_columns = (prdb_sdk_sql_column_shape_v2 *)calloc(
        PRDB_SDK_SQL_MAX_RELATION_COLUMNS, sizeof(*planner_columns));
    executor_columns = (prdb_sdk_sql_column_shape_v2 *)calloc(
        PRDB_SDK_SQL_MAX_RELATION_COLUMNS, sizeof(*executor_columns));
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
        insert_values = (prdb_sdk_executor_insert_value_v1 *)calloc(
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, sizeof(*insert_values));
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
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, &inserted_count);
    } else if (prdb_sdk_sql_parse_update_v1(
                   sql, sql_len, &update_statement, sizeof(update_statement),
                   NULL) == PRDB_SDK_SQL_OK) {
        binding = (prdb_sdk_select_binding_v2 *)calloc(1, sizeof(*binding));
        cell_scratch = (prdb_sdk_value_v1 *)calloc(
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, sizeof(*cell_scratch));
        target_handles = (uint8_t *)calloc(
            PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS, 16);
        patches = (prdb_sdk_executor_update_patch_v1 *)calloc(
            PRDB_SDK_SQL_MAX_UPDATE_ASSIGNMENTS, sizeof(*patches));
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
        binding = (prdb_sdk_select_binding_v2 *)calloc(1, sizeof(*binding));
        cell_scratch = (prdb_sdk_value_v1 *)calloc(
            PRDB_SDK_SQL_MAX_RELATION_COLUMNS, sizeof(*cell_scratch));
        target_handles = (uint8_t *)calloc(
            PRDB_SDK_SQL_EXEC_MAX_MUTATION_TARGETS, 16);
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
