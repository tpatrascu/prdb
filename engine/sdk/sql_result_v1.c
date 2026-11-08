#include "prdb_sdk_sql_result_v1.h"

#include <string.h>

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
