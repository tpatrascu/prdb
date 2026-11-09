#include "prdb_sdk_sql_result_v1.h"

#include <string.h>

static const uint8_t count_column_name_v1[] = "count";
static const uint8_t min_column_name_v1[] = "min";
static const uint8_t max_column_name_v1[] = "max";
static const uint8_t sum_column_name_v1[] = "sum";
static const uint8_t avg_column_name_v1[] = "avg";

typedef struct result_writer_v1 {
    uint8_t *bytes;
    uint64_t capacity;
    uint64_t offset;
} result_writer_v1;

static int valid_name_v1(const uint8_t *name, uint32_t length) {
    if (name == NULL || length == 0 || length > 128 ||
        !((name[0] >= 'A' && name[0] <= 'Z') ||
          (name[0] >= 'a' && name[0] <= 'z') || name[0] == '_')) {
        return 0;
    }
    for (uint32_t index = 1; index < length; ++index) {
        uint8_t byte = name[index];
        if (!((byte >= 'A' && byte <= 'Z') ||
              (byte >= 'a' && byte <= 'z') ||
              (byte >= '0' && byte <= '9') || byte == '_')) {
            return 0;
        }
    }
    return 1;
}

static int append_v1(result_writer_v1 *writer, const uint8_t *bytes,
                     uint64_t length) {
    uint64_t limit = writer->capacity;
    if (limit > PRDB_SDK_SQL_RESULT_MAX_BYTES) {
        limit = PRDB_SDK_SQL_RESULT_MAX_BYTES;
    }
    if (writer->offset > limit || length > limit - writer->offset ||
        (length != 0 && bytes == NULL)) {
        return 0;
    }
    if (length != 0) {
        memcpy(writer->bytes + writer->offset, bytes, (size_t)length);
    }
    writer->offset += length;
    return 1;
}

static int append_u8_v1(result_writer_v1 *writer, uint8_t value) {
    return append_v1(writer, &value, 1);
}

static int append_u16_v1(result_writer_v1 *writer, uint16_t value) {
    uint8_t bytes[2] = {(uint8_t)(value >> 8), (uint8_t)value};
    return append_v1(writer, bytes, sizeof(bytes));
}

static int append_u32_v1(result_writer_v1 *writer, uint32_t value) {
    uint8_t bytes[4] = {(uint8_t)(value >> 24), (uint8_t)(value >> 16),
                        (uint8_t)(value >> 8), (uint8_t)value};
    return append_v1(writer, bytes, sizeof(bytes));
}

static int valid_column_shape_v1(
    const prdb_sdk_sql_column_shape_v2 *column, uint32_t ordinal) {
    if (!valid_name_v1(column->name, column->name_len) ||
        column->ordinal != ordinal || column->type_code < 1 ||
        column->type_code > 11) {
        return 0;
    }
    if (column->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL) {
        return column->decimal_precision >= 1 &&
               column->decimal_precision <= 38 &&
               column->decimal_scale <= column->decimal_precision;
    }
    return column->decimal_precision == 0 && column->decimal_scale == 0;
}

int32_t prdb_sdk_sql_encode_result_row_v1(
    const prdb_sdk_select_binding_v2 *binding,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    const prdb_sdk_row_v1 *row, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *output,
    uint64_t output_capacity, uint64_t *output_len) {
    static const uint8_t magic[4] = {'S', 'Q', 'L', 'R'};
    prdb_sdk_cell_iterator_v1 cells = {0};
    result_writer_v1 writer;
    uint32_t projected_count;
    int32_t status;

    if (output_len != NULL) {
        *output_len = 0;
    }
    if (binding == NULL || columns == NULL || row == NULL ||
        cell_scratch == NULL || output == NULL || output_len == NULL ||
        output_capacity == 0) {
        return PRDB_SDK_SQL_RESULT_INVALID_ARGUMENT;
    }
    if (binding->struct_size < sizeof(*binding) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V2_VERSION ||
        binding->projection_star > 1 || column_count == 0 ||
        column_count > PRDB_SDK_SQL_MAX_RELATION_COLUMNS ||
        binding->relation_column_count != column_count ||
        binding->projection_count > PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS ||
        (binding->projection_star == 1
             ? binding->projection_count != 0
             : binding->projection_count == 0)) {
        return PRDB_SDK_SQL_RESULT_INVALID_BINDING;
    }
    if (row->struct_size < sizeof(*row) ||
        row->abi_version != PRDB_SDK_BATCH_ABI_V1_VERSION ||
        row->payload == NULL || row->payload_len > PRDB_SDK_BATCH_MAX_BYTES ||
        row->cells_offset > row->payload_len || row->cell_count != column_count ||
        cell_scratch_capacity < column_count) {
        return PRDB_SDK_SQL_RESULT_INVALID_ROW;
    }
    for (uint32_t index = 0; index < column_count; ++index) {
        if (!valid_column_shape_v1(&columns[index], index)) {
            return PRDB_SDK_SQL_RESULT_INVALID_SHAPE;
        }
    }
    if (binding->projection_star == 0) {
        for (uint32_t index = 0; index < binding->projection_count; ++index) {
            if (binding->projection_ordinals[index] >= column_count) {
                return PRDB_SDK_SQL_RESULT_INVALID_BINDING;
            }
        }
        projected_count = binding->projection_count;
    } else {
        projected_count = column_count;
    }

    status = prdb_sdk_row_cells_begin_v1(row, &cells, sizeof(cells));
    if (status != PRDB_SDK_BATCH_OK) {
        return PRDB_SDK_SQL_RESULT_INVALID_ROW;
    }
    for (uint32_t index = 0; index < column_count; ++index) {
        status = prdb_sdk_cell_next_v1(&cells, &cell_scratch[index],
                                       sizeof(cell_scratch[index]));
        if (status != PRDB_SDK_BATCH_OK ||
            prdb_sdk_value_validate_v1(&cell_scratch[index]) !=
                PRDB_SDK_VALUE_VALID ||
            (cell_scratch[index].state == PRDB_SDK_VALUE_STATE_VALUE &&
             cell_scratch[index].type_code != columns[index].type_code)) {
            return PRDB_SDK_SQL_RESULT_INVALID_ROW;
        }
    }

    writer.bytes = output;
    writer.capacity = output_capacity;
    writer.offset = 0;
    if (!append_v1(&writer, magic, sizeof(magic)) ||
        !append_u16_v1(&writer, PRDB_SDK_SQL_RESULT_WIRE_V1_VERSION) ||
        !append_u16_v1(&writer, (uint16_t)projected_count)) {
        return PRDB_SDK_SQL_RESULT_LIMIT_EXCEEDED;
    }
    for (uint32_t index = 0; index < projected_count; ++index) {
        uint32_t ordinal = binding->projection_star
                               ? index
                               : binding->projection_ordinals[index];
        const prdb_sdk_sql_column_shape_v2 *column = &columns[ordinal];
        const prdb_sdk_value_v1 *value = &cell_scratch[ordinal];
        uint32_t value_length = value->state == PRDB_SDK_VALUE_STATE_VALUE
                                    ? (uint32_t)value->data_len
                                    : 0;

        if (!append_u16_v1(&writer, (uint16_t)column->name_len) ||
            !append_v1(&writer, column->name, column->name_len) ||
            !append_u32_v1(&writer, column->type_code) ||
            !append_u8_v1(&writer, column->decimal_precision) ||
            !append_u8_v1(&writer, column->decimal_scale) ||
            !append_u8_v1(&writer, (uint8_t)value->state) ||
            !append_u32_v1(&writer, value_length) ||
            !append_v1(&writer, value->data, value_length)) {
            return PRDB_SDK_SQL_RESULT_LIMIT_EXCEEDED;
        }
    }
    *output_len = writer.offset;
    return PRDB_SDK_SQL_RESULT_OK;
}

int32_t prdb_sdk_sql_write_result_row_v1(
    const prdb_host_api_v3 *host,
    const prdb_sdk_select_binding_v2 *binding,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    const prdb_sdk_row_v1 *row, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *output,
    uint64_t output_capacity, uint64_t *output_len) {
    int32_t status;
    uint64_t offset = 0;

    if (output_len != NULL) {
        *output_len = 0;
    }
    if (host == NULL || output_len == NULL ||
        host->struct_size < sizeof(*host) ||
        host->abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->write_result == NULL) {
        return PRDB_SDK_SQL_RESULT_INVALID_ARGUMENT;
    }
    status = prdb_sdk_sql_encode_result_row_v1(
        binding, columns, column_count, row, cell_scratch,
        cell_scratch_capacity, output, output_capacity, output_len);
    if (status != PRDB_SDK_SQL_RESULT_OK) {
        return status;
    }
    while (offset < *output_len) {
        uint64_t chunk_length = *output_len - offset;
        if (chunk_length > PRDB_QUERY_MAX_RESULT_CHUNK_BYTES) {
            chunk_length = PRDB_QUERY_MAX_RESULT_CHUNK_BYTES;
        }
        if (host->write_result(host->context, output + offset, chunk_length) !=
            PRDB_QUERY_CALLBACK_OK) {
            *output_len = 0;
            return PRDB_SDK_SQL_RESULT_WRITE_FAILED;
        }
        offset += chunk_length;
    }
    return PRDB_SDK_SQL_RESULT_OK;
}

int32_t prdb_sdk_sql_encode_join_result_v1(
    const prdb_sdk_select_binding_v8 *binding,
    const prdb_sdk_sql_column_shape_v2 *left_columns,
    uint32_t left_column_count,
    const prdb_sdk_sql_column_shape_v2 *right_columns,
    uint32_t right_column_count,
    const prdb_sdk_value_v1 *projected_values, uint32_t projected_count,
    uint8_t *output, uint64_t output_capacity, uint64_t *output_len) {
    static const uint8_t magic[4] = {'S', 'Q', 'L', 'R'};
    result_writer_v1 writer;

    if (output_len != NULL) *output_len = 0;
    if (binding == NULL || left_columns == NULL || right_columns == NULL ||
        projected_values == NULL || output == NULL || output_len == NULL ||
        output_capacity == 0) {
        return PRDB_SDK_SQL_RESULT_INVALID_ARGUMENT;
    }
    if (binding->struct_size < sizeof(*binding) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V8_VERSION ||
        left_column_count == 0 || right_column_count == 0 ||
        left_column_count > PRDB_SDK_SQL_MAX_RELATION_COLUMNS ||
        right_column_count > PRDB_SDK_SQL_MAX_RELATION_COLUMNS ||
        binding->left_relation_column_count != left_column_count ||
        binding->right_relation_column_count != right_column_count ||
        binding->projection_count == 0 ||
        binding->projection_count > PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS ||
        projected_count != binding->projection_count) {
        return PRDB_SDK_SQL_RESULT_INVALID_BINDING;
    }
    for (uint32_t index = 0; index < left_column_count; ++index) {
        if (!valid_column_shape_v1(&left_columns[index], index)) {
            return PRDB_SDK_SQL_RESULT_INVALID_SHAPE;
        }
    }
    for (uint32_t index = 0; index < right_column_count; ++index) {
        if (!valid_column_shape_v1(&right_columns[index], index)) {
            return PRDB_SDK_SQL_RESULT_INVALID_SHAPE;
        }
    }
    for (uint32_t index = 0; index < projected_count; ++index) {
        const prdb_sdk_sql_join_projection_binding_v8 *projection =
            &binding->projections[index];
        const prdb_sdk_sql_column_shape_v2 *columns;
        uint32_t column_count;
        const prdb_sdk_sql_column_shape_v2 *column;

        if (projection->relation_side == PRDB_SDK_SQL_V8_LEFT) {
            columns = left_columns;
            column_count = left_column_count;
        } else if (projection->relation_side == PRDB_SDK_SQL_V8_RIGHT) {
            columns = right_columns;
            column_count = right_column_count;
        } else {
            return PRDB_SDK_SQL_RESULT_INVALID_BINDING;
        }
        if (projection->column.ordinal >= column_count) {
            return PRDB_SDK_SQL_RESULT_INVALID_BINDING;
        }
        column = &columns[projection->column.ordinal];
        if (projection->column.type_code != column->type_code ||
            projection->column.decimal_precision != column->decimal_precision ||
            projection->column.decimal_scale != column->decimal_scale ||
            prdb_sdk_value_validate_v1(&projected_values[index]) !=
                PRDB_SDK_VALUE_VALID ||
            (projected_values[index].state == PRDB_SDK_VALUE_STATE_VALUE &&
             projected_values[index].type_code != column->type_code)) {
            return PRDB_SDK_SQL_RESULT_INVALID_ROW;
        }
    }

    writer.bytes = output;
    writer.capacity = output_capacity;
    writer.offset = 0;
    if (!append_v1(&writer, magic, sizeof(magic)) ||
        !append_u16_v1(&writer, PRDB_SDK_SQL_RESULT_WIRE_V1_VERSION) ||
        !append_u16_v1(&writer, (uint16_t)projected_count)) {
        return PRDB_SDK_SQL_RESULT_LIMIT_EXCEEDED;
    }
    for (uint32_t index = 0; index < projected_count; ++index) {
        const prdb_sdk_sql_join_projection_binding_v8 *projection =
            &binding->projections[index];
        const prdb_sdk_sql_column_shape_v2 *column =
            projection->relation_side == PRDB_SDK_SQL_V8_LEFT
                ? &left_columns[projection->column.ordinal]
                : &right_columns[projection->column.ordinal];
        const prdb_sdk_value_v1 *value = &projected_values[index];
        uint32_t value_length = value->state == PRDB_SDK_VALUE_STATE_VALUE
                                    ? (uint32_t)value->data_len
                                    : 0;

        if (!append_u16_v1(&writer, (uint16_t)column->name_len) ||
            !append_v1(&writer, column->name, column->name_len) ||
            !append_u32_v1(&writer, column->type_code) ||
            !append_u8_v1(&writer, column->decimal_precision) ||
            !append_u8_v1(&writer, column->decimal_scale) ||
            !append_u8_v1(&writer, (uint8_t)value->state) ||
            !append_u32_v1(&writer, value_length) ||
            !append_v1(&writer, value->data, value_length)) {
            return PRDB_SDK_SQL_RESULT_LIMIT_EXCEEDED;
        }
    }
    *output_len = writer.offset;
    return PRDB_SDK_SQL_RESULT_OK;
}

int32_t prdb_sdk_sql_write_join_result_v1(
    const prdb_host_api_v3 *host,
    const prdb_sdk_select_binding_v8 *binding,
    const prdb_sdk_sql_column_shape_v2 *left_columns,
    uint32_t left_column_count,
    const prdb_sdk_sql_column_shape_v2 *right_columns,
    uint32_t right_column_count,
    const prdb_sdk_value_v1 *projected_values, uint32_t projected_count,
    uint8_t *output, uint64_t output_capacity, uint64_t *output_len) {
    int32_t status;
    uint64_t offset = 0;

    if (output_len != NULL) *output_len = 0;
    if (host == NULL || output_len == NULL ||
        host->struct_size < sizeof(*host) ||
        host->abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->write_result == NULL) {
        return PRDB_SDK_SQL_RESULT_INVALID_ARGUMENT;
    }
    status = prdb_sdk_sql_encode_join_result_v1(
        binding, left_columns, left_column_count, right_columns,
        right_column_count, projected_values, projected_count, output,
        output_capacity, output_len);
    if (status != PRDB_SDK_SQL_RESULT_OK) return status;
    while (offset < *output_len) {
        uint64_t chunk_length = *output_len - offset;
        if (chunk_length > PRDB_QUERY_MAX_RESULT_CHUNK_BYTES) {
            chunk_length = PRDB_QUERY_MAX_RESULT_CHUNK_BYTES;
        }
        if (host->write_result(host->context, output + offset, chunk_length) !=
            PRDB_QUERY_CALLBACK_OK) {
            *output_len = 0;
            return PRDB_SDK_SQL_RESULT_WRITE_FAILED;
        }
        offset += chunk_length;
    }
    return PRDB_SDK_SQL_RESULT_OK;
}

int32_t prdb_sdk_sql_encode_group_count_result_v1(
    const prdb_sdk_sql_column_shape_v2 *group_column,
    const prdb_sdk_value_v1 *group_key, uint64_t count,
    uint8_t *output, uint64_t output_capacity, uint64_t *output_len) {
    static const uint8_t magic[4] = {'S', 'Q', 'L', 'R'};
    uint8_t count_bytes[8];
    result_writer_v1 writer;
    int32_t value_status;

    if (output_len != NULL) *output_len = 0;
    if (group_column == NULL || group_key == NULL || output == NULL ||
        output_len == NULL || output_capacity == 0) {
        return PRDB_SDK_SQL_RESULT_INVALID_ARGUMENT;
    }
    if (!valid_column_shape_v1(group_column, group_column->ordinal)) {
        return PRDB_SDK_SQL_RESULT_INVALID_SHAPE;
    }
    value_status = prdb_sdk_value_validate_v1(group_key);
    if (value_status != PRDB_SDK_VALUE_VALID ||
        (group_key->state == PRDB_SDK_VALUE_STATE_VALUE &&
         group_key->type_code != group_column->type_code)) {
        return PRDB_SDK_SQL_RESULT_INVALID_ROW;
    }
    if (count == 0 || count > INT64_MAX) {
        return PRDB_SDK_SQL_RESULT_INVALID_ROW;
    }
    for (uint32_t index = 0; index < sizeof(count_bytes); ++index) {
        count_bytes[sizeof(count_bytes) - 1 - index] =
            (uint8_t)(count >> (index * 8));
    }
    writer.bytes = output;
    writer.capacity = output_capacity;
    writer.offset = 0;
    if (!append_v1(&writer, magic, sizeof(magic)) ||
        !append_u16_v1(&writer, PRDB_SDK_SQL_RESULT_WIRE_V1_VERSION) ||
        !append_u16_v1(&writer, 2) ||
        !append_u16_v1(&writer, (uint16_t)group_column->name_len) ||
        !append_v1(&writer, group_column->name, group_column->name_len) ||
        !append_u32_v1(&writer, group_column->type_code) ||
        !append_u8_v1(&writer, group_column->decimal_precision) ||
        !append_u8_v1(&writer, group_column->decimal_scale) ||
        !append_u8_v1(&writer, (uint8_t)group_key->state) ||
        !append_u32_v1(&writer,
                       group_key->state == PRDB_SDK_VALUE_STATE_VALUE
                           ? (uint32_t)group_key->data_len
                           : 0) ||
        !append_v1(&writer, group_key->data,
                   group_key->state == PRDB_SDK_VALUE_STATE_VALUE
                       ? group_key->data_len
                       : 0) ||
        !append_u16_v1(&writer,
                       (uint16_t)(sizeof(count_column_name_v1) - 1)) ||
        !append_v1(&writer, count_column_name_v1,
                   sizeof(count_column_name_v1) - 1) ||
        !append_u32_v1(&writer, PRDB_SDK_VALUE_TYPE_BIGINT) ||
        !append_u8_v1(&writer, 0) || !append_u8_v1(&writer, 0) ||
        !append_u8_v1(&writer, PRDB_SDK_VALUE_STATE_VALUE) ||
        !append_u32_v1(&writer, sizeof(count_bytes)) ||
        !append_v1(&writer, count_bytes, sizeof(count_bytes))) {
        return PRDB_SDK_SQL_RESULT_LIMIT_EXCEEDED;
    }
    *output_len = writer.offset;
    return PRDB_SDK_SQL_RESULT_OK;
}

int32_t prdb_sdk_sql_write_group_count_result_v1(
    const prdb_host_api_v3 *host,
    const prdb_sdk_sql_column_shape_v2 *group_column,
    const prdb_sdk_value_v1 *group_key, uint64_t count,
    uint8_t *output, uint64_t output_capacity, uint64_t *output_len) {
    int32_t status;
    uint64_t offset = 0;

    if (output_len != NULL) *output_len = 0;
    if (host == NULL || output_len == NULL ||
        host->struct_size < sizeof(*host) ||
        host->abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->write_result == NULL) {
        return PRDB_SDK_SQL_RESULT_INVALID_ARGUMENT;
    }
    status = prdb_sdk_sql_encode_group_count_result_v1(
        group_column, group_key, count, output, output_capacity, output_len);
    if (status != PRDB_SDK_SQL_RESULT_OK) return status;
    while (offset < *output_len) {
        uint64_t chunk_length = *output_len - offset;
        if (chunk_length > PRDB_QUERY_MAX_RESULT_CHUNK_BYTES) {
            chunk_length = PRDB_QUERY_MAX_RESULT_CHUNK_BYTES;
        }
        if (host->write_result(host->context, output + offset, chunk_length) !=
            PRDB_QUERY_CALLBACK_OK) {
            *output_len = 0;
            return PRDB_SDK_SQL_RESULT_WRITE_FAILED;
        }
        offset += chunk_length;
    }
    return PRDB_SDK_SQL_RESULT_OK;
}

static int32_t encode_group_result_v1(
    const prdb_sdk_sql_column_shape_v2 *group_column,
    const prdb_sdk_value_v1 *group_key,
    const prdb_sdk_sql_column_shape_v2 *aggregate_column,
    const prdb_sdk_value_v1 *aggregate_value,
    const uint8_t *aggregate_name, uint32_t aggregate_name_len,
    uint8_t *output, uint64_t output_capacity, uint64_t *output_len) {
    static const uint8_t magic[4] = {'S', 'Q', 'L', 'R'};
    result_writer_v1 writer;

    if (output_len != NULL) *output_len = 0;
    if (group_column == NULL || group_key == NULL ||
        aggregate_column == NULL || aggregate_value == NULL ||
        output == NULL || output_len == NULL || output_capacity == 0) {
        return PRDB_SDK_SQL_RESULT_INVALID_ARGUMENT;
    }
    if (!valid_column_shape_v1(group_column, group_column->ordinal) ||
        !valid_column_shape_v1(aggregate_column,
                               aggregate_column->ordinal)) {
        return PRDB_SDK_SQL_RESULT_INVALID_SHAPE;
    }
    if (prdb_sdk_value_validate_v1(group_key) != PRDB_SDK_VALUE_VALID ||
        (group_key->state == PRDB_SDK_VALUE_STATE_VALUE &&
         group_key->type_code != group_column->type_code) ||
        prdb_sdk_value_validate_v1(aggregate_value) !=
            PRDB_SDK_VALUE_VALID ||
        aggregate_value->state == PRDB_SDK_VALUE_STATE_REDACTED ||
        (aggregate_value->state == PRDB_SDK_VALUE_STATE_VALUE &&
         aggregate_value->type_code != aggregate_column->type_code)) {
        return PRDB_SDK_SQL_RESULT_INVALID_ROW;
    }

    writer.bytes = output;
    writer.capacity = output_capacity;
    writer.offset = 0;
    if (!append_v1(&writer, magic, sizeof(magic)) ||
        !append_u16_v1(&writer, PRDB_SDK_SQL_RESULT_WIRE_V1_VERSION) ||
        !append_u16_v1(&writer, 2) ||
        !append_u16_v1(&writer, (uint16_t)group_column->name_len) ||
        !append_v1(&writer, group_column->name, group_column->name_len) ||
        !append_u32_v1(&writer, group_column->type_code) ||
        !append_u8_v1(&writer, group_column->decimal_precision) ||
        !append_u8_v1(&writer, group_column->decimal_scale) ||
        !append_u8_v1(&writer, (uint8_t)group_key->state) ||
        !append_u32_v1(&writer,
                       group_key->state == PRDB_SDK_VALUE_STATE_VALUE
                           ? (uint32_t)group_key->data_len
                           : 0) ||
        !append_v1(&writer, group_key->data,
                   group_key->state == PRDB_SDK_VALUE_STATE_VALUE
                       ? group_key->data_len
                       : 0) ||
        !append_u16_v1(&writer, (uint16_t)aggregate_name_len) ||
        !append_v1(&writer, aggregate_name, aggregate_name_len) ||
        !append_u32_v1(&writer, aggregate_column->type_code) ||
        !append_u8_v1(&writer, aggregate_column->decimal_precision) ||
        !append_u8_v1(&writer, aggregate_column->decimal_scale) ||
        !append_u8_v1(&writer, (uint8_t)aggregate_value->state) ||
        !append_u32_v1(&writer,
                       aggregate_value->state == PRDB_SDK_VALUE_STATE_VALUE
                           ? (uint32_t)aggregate_value->data_len
                           : 0) ||
        !append_v1(&writer, aggregate_value->data,
                   aggregate_value->state == PRDB_SDK_VALUE_STATE_VALUE
                       ? aggregate_value->data_len
                       : 0)) {
        return PRDB_SDK_SQL_RESULT_LIMIT_EXCEEDED;
    }
    *output_len = writer.offset;
    return PRDB_SDK_SQL_RESULT_OK;
}

int32_t prdb_sdk_sql_encode_group_minmax_result_v1(
    const prdb_sdk_sql_column_shape_v2 *group_column,
    const prdb_sdk_value_v1 *group_key,
    const prdb_sdk_sql_column_shape_v2 *aggregate_column,
    const prdb_sdk_value_v1 *aggregate_value, uint32_t aggregate_kind,
    uint8_t *output, uint64_t output_capacity, uint64_t *output_len) {
    if (aggregate_kind == PRDB_SDK_SQL_V5_AGGREGATE_MIN) {
        return encode_group_result_v1(
            group_column, group_key, aggregate_column, aggregate_value,
            min_column_name_v1, sizeof(min_column_name_v1) - 1, output,
            output_capacity, output_len);
    }
    if (aggregate_kind == PRDB_SDK_SQL_V5_AGGREGATE_MAX) {
        return encode_group_result_v1(
            group_column, group_key, aggregate_column, aggregate_value,
            max_column_name_v1, sizeof(max_column_name_v1) - 1, output,
            output_capacity, output_len);
    }
    if (output_len != NULL) *output_len = 0;
    return PRDB_SDK_SQL_RESULT_INVALID_ARGUMENT;
}

int32_t prdb_sdk_sql_encode_group_sum_result_v1(
    const prdb_sdk_sql_column_shape_v2 *group_column,
    const prdb_sdk_value_v1 *group_key,
    const prdb_sdk_sql_column_shape_v2 *aggregate_input_column,
    const prdb_sdk_value_v1 *aggregate_value,
    uint8_t *output, uint64_t output_capacity, uint64_t *output_len) {
    prdb_sdk_sql_column_shape_v2 result_column;
    if (output_len != NULL) *output_len = 0;
    if (aggregate_input_column == NULL) {
        return PRDB_SDK_SQL_RESULT_INVALID_ARGUMENT;
    }
    if (!valid_column_shape_v1(aggregate_input_column,
                               aggregate_input_column->ordinal)) {
        return PRDB_SDK_SQL_RESULT_INVALID_SHAPE;
    }
    result_column = *aggregate_input_column;
    result_column.name = sum_column_name_v1;
    result_column.name_len = sizeof(sum_column_name_v1) - 1;
    if (aggregate_input_column->type_code == PRDB_SDK_VALUE_TYPE_SMALLINT ||
        aggregate_input_column->type_code == PRDB_SDK_VALUE_TYPE_INTEGER ||
        aggregate_input_column->type_code == PRDB_SDK_VALUE_TYPE_BIGINT) {
        result_column.type_code = PRDB_SDK_VALUE_TYPE_BIGINT;
        result_column.decimal_precision = 0;
        result_column.decimal_scale = 0;
    } else if (aggregate_input_column->type_code ==
               PRDB_SDK_VALUE_TYPE_DECIMAL) {
        result_column.type_code = PRDB_SDK_VALUE_TYPE_DECIMAL;
        result_column.decimal_precision = 38;
    } else {
        return PRDB_SDK_SQL_RESULT_INVALID_SHAPE;
    }
    return encode_group_result_v1(
        group_column, group_key, &result_column, aggregate_value,
        sum_column_name_v1, sizeof(sum_column_name_v1) - 1, output,
        output_capacity, output_len);
}

int32_t prdb_sdk_sql_encode_group_avg_result_v1(
    const prdb_sdk_sql_column_shape_v2 *group_column,
    const prdb_sdk_value_v1 *group_key,
    const prdb_sdk_sql_column_shape_v2 *aggregate_input_column,
    const prdb_sdk_value_v1 *aggregate_value,
    uint8_t *output, uint64_t output_capacity, uint64_t *output_len) {
    prdb_sdk_sql_column_shape_v2 result_column;
    uint32_t result_scale;
    if (output_len != NULL) *output_len = 0;
    if (aggregate_input_column == NULL) {
        return PRDB_SDK_SQL_RESULT_INVALID_ARGUMENT;
    }
    if (!valid_column_shape_v1(aggregate_input_column,
                               aggregate_input_column->ordinal)) {
        return PRDB_SDK_SQL_RESULT_INVALID_SHAPE;
    }
    result_column = *aggregate_input_column;
    result_column.name = avg_column_name_v1;
    result_column.name_len = sizeof(avg_column_name_v1) - 1;
    if (aggregate_input_column->type_code == PRDB_SDK_VALUE_TYPE_SMALLINT ||
        aggregate_input_column->type_code == PRDB_SDK_VALUE_TYPE_INTEGER ||
        aggregate_input_column->type_code == PRDB_SDK_VALUE_TYPE_BIGINT) {
        result_scale = 6;
    } else if (aggregate_input_column->type_code ==
               PRDB_SDK_VALUE_TYPE_DECIMAL) {
        uint32_t integer_digits = aggregate_input_column->decimal_precision -
                                  aggregate_input_column->decimal_scale;
        uint32_t desired_scale = aggregate_input_column->decimal_scale + 6;
        uint32_t available_scale = 38 - integer_digits;
        if (desired_scale < 6) desired_scale = 6;
        if (desired_scale > available_scale) desired_scale = available_scale;
        result_scale = desired_scale;
    } else {
        return PRDB_SDK_SQL_RESULT_INVALID_SHAPE;
    }
    result_column.type_code = PRDB_SDK_VALUE_TYPE_DECIMAL;
    result_column.decimal_precision = 38;
    result_column.decimal_scale = (uint8_t)result_scale;
    return encode_group_result_v1(
        group_column, group_key, &result_column, aggregate_value,
        avg_column_name_v1, sizeof(avg_column_name_v1) - 1, output,
        output_capacity, output_len);
}

int32_t prdb_sdk_sql_write_group_minmax_result_v1(
    const prdb_host_api_v3 *host,
    const prdb_sdk_sql_column_shape_v2 *group_column,
    const prdb_sdk_value_v1 *group_key,
    const prdb_sdk_sql_column_shape_v2 *aggregate_column,
    const prdb_sdk_value_v1 *aggregate_value, uint32_t aggregate_kind,
    uint8_t *output, uint64_t output_capacity, uint64_t *output_len) {
    int32_t status;
    uint64_t offset = 0;

    if (output_len != NULL) *output_len = 0;
    if (host == NULL || output_len == NULL ||
        host->struct_size < sizeof(*host) ||
        host->abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->write_result == NULL) {
        return PRDB_SDK_SQL_RESULT_INVALID_ARGUMENT;
    }
    status = prdb_sdk_sql_encode_group_minmax_result_v1(
        group_column, group_key, aggregate_column, aggregate_value,
        aggregate_kind, output, output_capacity, output_len);
    if (status != PRDB_SDK_SQL_RESULT_OK) return status;
    while (offset < *output_len) {
        uint64_t chunk_length = *output_len - offset;
        if (chunk_length > PRDB_QUERY_MAX_RESULT_CHUNK_BYTES) {
            chunk_length = PRDB_QUERY_MAX_RESULT_CHUNK_BYTES;
        }
        if (host->write_result(host->context, output + offset, chunk_length) !=
            PRDB_QUERY_CALLBACK_OK) {
            *output_len = 0;
            return PRDB_SDK_SQL_RESULT_WRITE_FAILED;
        }
        offset += chunk_length;
    }
    return PRDB_SDK_SQL_RESULT_OK;
}

int32_t prdb_sdk_sql_write_group_sum_result_v1(
    const prdb_host_api_v3 *host,
    const prdb_sdk_sql_column_shape_v2 *group_column,
    const prdb_sdk_value_v1 *group_key,
    const prdb_sdk_sql_column_shape_v2 *aggregate_input_column,
    const prdb_sdk_value_v1 *aggregate_value,
    uint8_t *output, uint64_t output_capacity, uint64_t *output_len) {
    int32_t status;
    uint64_t offset = 0;

    if (output_len != NULL) *output_len = 0;
    if (host == NULL || output_len == NULL ||
        host->struct_size < sizeof(*host) ||
        host->abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->write_result == NULL) {
        return PRDB_SDK_SQL_RESULT_INVALID_ARGUMENT;
    }
    status = prdb_sdk_sql_encode_group_sum_result_v1(
        group_column, group_key, aggregate_input_column, aggregate_value,
        output, output_capacity, output_len);
    if (status != PRDB_SDK_SQL_RESULT_OK) return status;
    while (offset < *output_len) {
        uint64_t chunk_length = *output_len - offset;
        if (chunk_length > PRDB_QUERY_MAX_RESULT_CHUNK_BYTES) {
            chunk_length = PRDB_QUERY_MAX_RESULT_CHUNK_BYTES;
        }
        if (host->write_result(host->context, output + offset, chunk_length) !=
            PRDB_QUERY_CALLBACK_OK) {
            *output_len = 0;
            return PRDB_SDK_SQL_RESULT_WRITE_FAILED;
        }
        offset += chunk_length;
    }
    return PRDB_SDK_SQL_RESULT_OK;
}

int32_t prdb_sdk_sql_write_group_avg_result_v1(
    const prdb_host_api_v3 *host,
    const prdb_sdk_sql_column_shape_v2 *group_column,
    const prdb_sdk_value_v1 *group_key,
    const prdb_sdk_sql_column_shape_v2 *aggregate_input_column,
    const prdb_sdk_value_v1 *aggregate_value,
    uint8_t *output, uint64_t output_capacity, uint64_t *output_len) {
    int32_t status;
    uint64_t offset = 0;

    if (output_len != NULL) *output_len = 0;
    if (host == NULL || output_len == NULL ||
        host->struct_size < sizeof(*host) ||
        host->abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->write_result == NULL) {
        return PRDB_SDK_SQL_RESULT_INVALID_ARGUMENT;
    }
    status = prdb_sdk_sql_encode_group_avg_result_v1(
        group_column, group_key, aggregate_input_column, aggregate_value,
        output, output_capacity, output_len);
    if (status != PRDB_SDK_SQL_RESULT_OK) return status;
    while (offset < *output_len) {
        uint64_t chunk_length = *output_len - offset;
        if (chunk_length > PRDB_QUERY_MAX_RESULT_CHUNK_BYTES) {
            chunk_length = PRDB_QUERY_MAX_RESULT_CHUNK_BYTES;
        }
        if (host->write_result(host->context, output + offset, chunk_length) !=
            PRDB_QUERY_CALLBACK_OK) {
            *output_len = 0;
            return PRDB_SDK_SQL_RESULT_WRITE_FAILED;
        }
        offset += chunk_length;
    }
    return PRDB_SDK_SQL_RESULT_OK;
}

static int append_value_field_v1(
    result_writer_v1 *writer,
    const prdb_sdk_sql_column_shape_v2 *column,
    const prdb_sdk_value_v1 *value) {
    if (value->state == PRDB_SDK_VALUE_STATE_VALUE &&
        value->data_len > UINT32_MAX) {
        return 0;
    }
    uint32_t data_len = value->state == PRDB_SDK_VALUE_STATE_VALUE
                            ? (uint32_t)value->data_len
                            : 0;
    return append_u16_v1(writer, (uint16_t)column->name_len) &&
           append_v1(writer, column->name, column->name_len) &&
           append_u32_v1(writer, column->type_code) &&
           append_u8_v1(writer, column->decimal_precision) &&
           append_u8_v1(writer, column->decimal_scale) &&
           append_u8_v1(writer, (uint8_t)value->state) &&
           append_u32_v1(writer, data_len) &&
           append_v1(writer, value->data, data_len);
}

int32_t prdb_sdk_sql_encode_group_composite_result_v1(
    const prdb_sdk_select_binding_v13 *binding,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    const prdb_sdk_value_v1 *group_keys, uint32_t group_key_count,
    const prdb_sdk_value_v1 *aggregate_value, uint64_t matching_row_count,
    uint8_t *output, uint64_t output_capacity, uint64_t *output_len) {
    static const uint8_t magic[4] = {'S', 'Q', 'L', 'R'};
    prdb_sdk_sql_column_shape_v2 aggregate_column = {0};
    prdb_sdk_value_v1 count_value = {0};
    uint8_t count_bytes[8];
    const prdb_sdk_value_v1 *result_value = aggregate_value;
    result_writer_v1 writer;
    uint32_t aggregate_kind;
    const uint8_t *aggregate_name = NULL;
    uint32_t aggregate_name_len = 0;

    if (output_len != NULL) *output_len = 0;
    if (binding == NULL || columns == NULL || group_keys == NULL ||
        output == NULL || output_len == NULL || output_capacity == 0) {
        return PRDB_SDK_SQL_RESULT_INVALID_ARGUMENT;
    }
    if (binding->struct_size < sizeof(*binding) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V13_VERSION ||
        binding->base.struct_size < sizeof(binding->base) ||
        binding->base.abi_version != PRDB_SDK_SQL_ABI_V7_VERSION ||
        binding->base.has_group_by != 1 ||
        binding->base.aggregate_kind <
            PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR ||
        binding->base.aggregate_kind > PRDB_SDK_SQL_V7_AGGREGATE_AVG ||
        binding->group_key_count < 2 ||
        binding->group_key_count > PRDB_SDK_SQL_V13_MAX_GROUP_KEYS ||
        binding->group_key_count != group_key_count ||
        group_key_count > UINT16_MAX - 1 || column_count == 0) {
        return PRDB_SDK_SQL_RESULT_INVALID_BINDING;
    }
    for (uint32_t key = 0; key < group_key_count; ++key) {
        const prdb_sdk_sql_bound_column_v2 *bound =
            &binding->group_keys[key];
        if (bound->ordinal >= column_count ||
            !valid_column_shape_v1(&columns[bound->ordinal], bound->ordinal) ||
            columns[bound->ordinal].type_code != bound->type_code ||
            columns[bound->ordinal].decimal_precision !=
                bound->decimal_precision ||
            columns[bound->ordinal].decimal_scale != bound->decimal_scale ||
            prdb_sdk_value_validate_v1(&group_keys[key]) !=
                PRDB_SDK_VALUE_VALID ||
            (group_keys[key].state == PRDB_SDK_VALUE_STATE_VALUE &&
             group_keys[key].type_code != bound->type_code)) {
            return PRDB_SDK_SQL_RESULT_INVALID_ROW;
        }
        for (uint32_t prior = 0; prior < key; ++prior) {
            if (binding->group_keys[prior].ordinal == bound->ordinal) {
                return PRDB_SDK_SQL_RESULT_INVALID_BINDING;
            }
        }
    }

    aggregate_kind = binding->base.aggregate_kind;
    if (aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
        if (aggregate_value != NULL) return PRDB_SDK_SQL_RESULT_INVALID_ROW;
        aggregate_column.name = count_column_name_v1;
        aggregate_column.name_len = sizeof(count_column_name_v1) - 1;
        aggregate_column.ordinal = column_count;
        aggregate_column.type_code = PRDB_SDK_VALUE_TYPE_BIGINT;
        for (uint32_t byte = 0; byte < 8; ++byte) {
            count_bytes[7 - byte] =
                (uint8_t)(matching_row_count & UINT64_C(0xff));
            matching_row_count >>= 8;
        }
        count_value.struct_size = sizeof(count_value);
        count_value.abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
        count_value.type_code = PRDB_SDK_VALUE_TYPE_BIGINT;
        count_value.state = PRDB_SDK_VALUE_STATE_VALUE;
        count_value.data = count_bytes;
        count_value.data_len = sizeof(count_bytes);
        result_value = &count_value;
        aggregate_name = count_column_name_v1;
        aggregate_name_len = sizeof(count_column_name_v1) - 1;
    } else {
        uint32_t aggregate_ordinal = binding->base.aggregate_column.ordinal;
        const prdb_sdk_sql_column_shape_v2 *input;
        if (aggregate_value == NULL || aggregate_ordinal >= column_count) {
            return PRDB_SDK_SQL_RESULT_INVALID_BINDING;
        }
        input = &columns[aggregate_ordinal];
        if (!valid_column_shape_v1(input, aggregate_ordinal) ||
            input->type_code != binding->base.aggregate_column.type_code ||
            input->decimal_precision !=
                binding->base.aggregate_column.decimal_precision ||
            input->decimal_scale !=
                binding->base.aggregate_column.decimal_scale) {
            return PRDB_SDK_SQL_RESULT_INVALID_SHAPE;
        }
        aggregate_column = *input;
        if (aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_MIN) {
            aggregate_name = min_column_name_v1;
            aggregate_name_len = sizeof(min_column_name_v1) - 1;
        } else if (aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_MAX) {
            aggregate_name = max_column_name_v1;
            aggregate_name_len = sizeof(max_column_name_v1) - 1;
        } else if (aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_SUM) {
            aggregate_name = sum_column_name_v1;
            aggregate_name_len = sizeof(sum_column_name_v1) - 1;
            if (input->type_code == PRDB_SDK_VALUE_TYPE_SMALLINT ||
                input->type_code == PRDB_SDK_VALUE_TYPE_INTEGER ||
                input->type_code == PRDB_SDK_VALUE_TYPE_BIGINT) {
                aggregate_column.type_code = PRDB_SDK_VALUE_TYPE_BIGINT;
                aggregate_column.decimal_precision = 0;
                aggregate_column.decimal_scale = 0;
            } else if (input->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL) {
                aggregate_column.type_code = PRDB_SDK_VALUE_TYPE_DECIMAL;
                aggregate_column.decimal_precision = 38;
            } else {
                return PRDB_SDK_SQL_RESULT_INVALID_SHAPE;
            }
        } else if (aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_AVG) {
            uint32_t integer_digits, desired_scale, available_scale;
            aggregate_name = avg_column_name_v1;
            aggregate_name_len = sizeof(avg_column_name_v1) - 1;
            if (input->type_code == PRDB_SDK_VALUE_TYPE_SMALLINT ||
                input->type_code == PRDB_SDK_VALUE_TYPE_INTEGER ||
                input->type_code == PRDB_SDK_VALUE_TYPE_BIGINT) {
                desired_scale = 6;
            } else if (input->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL) {
                integer_digits = input->decimal_precision - input->decimal_scale;
                desired_scale = input->decimal_scale + 6;
                available_scale = 38 - integer_digits;
                if (desired_scale > available_scale) {
                    desired_scale = available_scale;
                }
            } else {
                return PRDB_SDK_SQL_RESULT_INVALID_SHAPE;
            }
            aggregate_column.type_code = PRDB_SDK_VALUE_TYPE_DECIMAL;
            aggregate_column.decimal_precision = 38;
            aggregate_column.decimal_scale = (uint8_t)desired_scale;
        } else {
            return PRDB_SDK_SQL_RESULT_INVALID_BINDING;
        }
        aggregate_column.name = aggregate_name;
        aggregate_column.name_len = aggregate_name_len;
        if (prdb_sdk_value_validate_v1(result_value) != PRDB_SDK_VALUE_VALID ||
            result_value->state == PRDB_SDK_VALUE_STATE_REDACTED ||
            (result_value->state == PRDB_SDK_VALUE_STATE_VALUE &&
             result_value->type_code != aggregate_column.type_code)) {
            return PRDB_SDK_SQL_RESULT_INVALID_ROW;
        }
    }
    if (!valid_column_shape_v1(&aggregate_column, aggregate_column.ordinal)) {
        return PRDB_SDK_SQL_RESULT_INVALID_SHAPE;
    }

    writer.bytes = output;
    writer.capacity = output_capacity;
    writer.offset = 0;
    if (!append_v1(&writer, magic, sizeof(magic)) ||
        !append_u16_v1(&writer, PRDB_SDK_SQL_RESULT_WIRE_V1_VERSION) ||
        !append_u16_v1(&writer, (uint16_t)(group_key_count + 1))) {
        return PRDB_SDK_SQL_RESULT_LIMIT_EXCEEDED;
    }
    for (uint32_t key = 0; key < group_key_count; ++key) {
        const prdb_sdk_sql_column_shape_v2 *column =
            &columns[binding->group_keys[key].ordinal];
        if (!append_value_field_v1(&writer, column, &group_keys[key])) {
            return PRDB_SDK_SQL_RESULT_LIMIT_EXCEEDED;
        }
    }
    if (!append_value_field_v1(&writer, &aggregate_column, result_value)) {
        return PRDB_SDK_SQL_RESULT_LIMIT_EXCEEDED;
    }
    *output_len = writer.offset;
    return PRDB_SDK_SQL_RESULT_OK;
}

int32_t prdb_sdk_sql_write_group_composite_result_v1(
    const prdb_host_api_v3 *host,
    const prdb_sdk_select_binding_v13 *binding,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    const prdb_sdk_value_v1 *group_keys, uint32_t group_key_count,
    const prdb_sdk_value_v1 *aggregate_value, uint64_t matching_row_count,
    uint8_t *output, uint64_t output_capacity, uint64_t *output_len) {
    int32_t status;
    uint64_t offset = 0;

    if (output_len != NULL) *output_len = 0;
    if (host == NULL || output_len == NULL ||
        host->struct_size < sizeof(*host) ||
        host->abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->write_result == NULL) {
        return PRDB_SDK_SQL_RESULT_INVALID_ARGUMENT;
    }
    status = prdb_sdk_sql_encode_group_composite_result_v1(
        binding, columns, column_count, group_keys, group_key_count,
        aggregate_value, matching_row_count, output, output_capacity,
        output_len);
    if (status != PRDB_SDK_SQL_RESULT_OK) return status;
    while (offset < *output_len) {
        uint64_t chunk_length = *output_len - offset;
        if (chunk_length > PRDB_QUERY_MAX_RESULT_CHUNK_BYTES) {
            chunk_length = PRDB_QUERY_MAX_RESULT_CHUNK_BYTES;
        }
        if (host->write_result(host->context, output + offset, chunk_length) !=
            PRDB_QUERY_CALLBACK_OK) {
            *output_len = 0;
            return PRDB_SDK_SQL_RESULT_WRITE_FAILED;
        }
        offset += chunk_length;
    }
    return PRDB_SDK_SQL_RESULT_OK;
}
