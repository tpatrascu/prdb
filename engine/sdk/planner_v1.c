#include "prdb_sdk_planner_v1.h"

#include <stddef.h>
#include <string.h>

static uint16_t read_u16(const uint8_t *payload, uint32_t offset) {
    return (uint16_t)(((uint16_t)payload[offset] << 8) | payload[offset + 1]);
}

static uint32_t read_u32(const uint8_t *payload, uint32_t offset) {
    return ((uint32_t)payload[offset] << 24) |
           ((uint32_t)payload[offset + 1] << 16) |
           ((uint32_t)payload[offset + 2] << 8) |
           (uint32_t)payload[offset + 3];
}

static int take_bytes(uint64_t payload_len, uint32_t *offset, uint32_t count) {
    if ((uint64_t)*offset + count > payload_len) {
        return 0;
    }
    *offset += count;
    return 1;
}

static int valid_identifier(const uint8_t *name, uint32_t name_len) {
    if (name == NULL || name_len == 0 ||
        name_len > PRDB_SDK_SQL_MAX_IDENTIFIER_BYTES ||
        !((name[0] >= 'a' && name[0] <= 'z') ||
          (name[0] >= 'A' && name[0] <= 'Z') || name[0] == '_')) {
        return 0;
    }
    for (uint32_t index = 1; index < name_len; ++index) {
        uint8_t byte = name[index];
        if (!((byte >= 'a' && byte <= 'z') ||
              (byte >= 'A' && byte <= 'Z') || byte == '_' ||
              (byte >= '0' && byte <= '9'))) {
            return 0;
        }
    }
    return 1;
}

static int take_name(const uint8_t *payload, uint64_t payload_len,
                     uint32_t *offset, const uint8_t **name_out,
                     uint32_t *name_len_out) {
    uint32_t name_len;
    if ((uint64_t)*offset + 2 > payload_len) {
        return 0;
    }
    name_len = read_u16(payload, *offset);
    *offset += 2;
    if (!take_bytes(payload_len, offset, name_len)) {
        return 0;
    }
    *name_out = payload + *offset - name_len;
    *name_len_out = name_len;
    return valid_identifier(*name_out, name_len);
}

static int validate_column(const uint8_t *payload, uint64_t payload_len,
                           uint32_t *offset, uint32_t expected_ordinal,
                           prdb_sdk_sql_column_shape_v2 *out) {
    uint32_t ordinal;
    uint8_t nullable;
    uint8_t type_code;
    uint8_t precision;
    uint8_t scale;
    uint32_t parameters;
    uint8_t column_id_is_zero = 1;
    const uint8_t *name;
    uint32_t name_len;

    if ((uint64_t)*offset + 26 > payload_len) {
        return 0;
    }
    for (uint32_t index = 0; index < 16; ++index) {
        column_id_is_zero &= payload[*offset + index] == 0;
    }
    if (column_id_is_zero) {
        return 0;
    }
    *offset += 16;
    ordinal = read_u32(payload, *offset);
    *offset += 4;
    nullable = payload[(*offset)++];
    type_code = payload[(*offset)++];
    precision = payload[*offset];
    scale = payload[*offset + 1];
    parameters = read_u32(payload, *offset);
    *offset += 4;
    if (ordinal != expected_ordinal || nullable > 1 || type_code < 1 ||
        type_code > 11 ||
        (type_code == PRDB_SDK_VALUE_TYPE_DECIMAL
             ? precision == 0 || precision > 38 || scale > precision ||
                   payload[*offset - 2] != 0 || payload[*offset - 1] != 0
             : type_code == PRDB_SDK_VALUE_TYPE_VARCHAR
                   ? parameters == 0
                   : parameters != 0)) {
        return 0;
    }
    if (!take_name(payload, payload_len, offset, &name, &name_len)) {
        return 0;
    }
    if (out != NULL) {
        out->name = name;
        out->name_len = name_len;
        out->ordinal = ordinal;
        out->type_code = type_code;
        out->decimal_precision =
            type_code == PRDB_SDK_VALUE_TYPE_DECIMAL ? precision : 0;
        out->decimal_scale =
            type_code == PRDB_SDK_VALUE_TYPE_DECIMAL ? scale : 0;
    }
    return 1;
}

int32_t prdb_sdk_planner_decode_relation_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_planner_relation_v1 *out, uint32_t out_capacity,
    prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_capacity) {
    prdb_sdk_planner_relation_v1 result = {0};
    uint32_t offset = 3;
    uint32_t column_count;
    uint8_t relation_id_is_zero = 1;

    if (payload == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_PLANNER_DECODE_INVALID_ARGUMENT;
    }
    if (payload_len > PRDB_SDK_PLANNER_MAX_RESPONSE_BYTES || payload_len < 3) {
        return PRDB_SDK_PLANNER_DECODE_INVALID_PAYLOAD;
    }
    if (read_u16(payload, 0) != PRDB_SDK_PLANNER_WIRE_V1_VERSION) {
        return PRDB_SDK_PLANNER_DECODE_UNSUPPORTED_VERSION;
    }
    if (payload[2] == 1 && payload_len == 3) {
        result.struct_size = sizeof(result);
        result.abi_version = PRDB_SDK_PLANNER_ABI_V1_VERSION;
        *out = result;
        return PRDB_SDK_PLANNER_DECODE_UNAVAILABLE;
    }
    if (payload[2] != 0 || payload_len < 40) {
        return PRDB_SDK_PLANNER_DECODE_INVALID_PAYLOAD;
    }
    for (uint32_t index = 0; index < 16; ++index) {
        relation_id_is_zero &= payload[offset + index] == 0;
    }
    if (relation_id_is_zero) {
        return PRDB_SDK_PLANNER_DECODE_INVALID_PAYLOAD;
    }
    memcpy(result.relation_id, payload + offset, 16);
    offset += 16;
    if (payload[offset] != PRDB_SDK_PLANNER_TABLE_HANDLE_KIND) {
        return PRDB_SDK_PLANNER_DECODE_INVALID_PAYLOAD;
    }
    memcpy(result.planning_handle, payload + offset, 16);
    offset += 16;
    if (!take_name(payload, payload_len, &offset, &result.name,
                   &result.name_len) ||
        (uint64_t)offset + 2 > payload_len) {
        return PRDB_SDK_PLANNER_DECODE_INVALID_PAYLOAD;
    }
    column_count = read_u16(payload, offset);
    offset += 2;
    if (column_count == 0 ||
        column_count > PRDB_SDK_PLANNER_MAX_RELATION_COLUMNS) {
        return PRDB_SDK_PLANNER_DECODE_INVALID_PAYLOAD;
    }
    for (uint32_t index = 0; index < column_count; ++index) {
        if (!validate_column(payload, payload_len, &offset, index, NULL)) {
            return PRDB_SDK_PLANNER_DECODE_INVALID_PAYLOAD;
        }
    }
    if (offset != payload_len) {
        return PRDB_SDK_PLANNER_DECODE_INVALID_PAYLOAD;
    }
    if (columns == NULL || column_capacity < column_count) {
        return PRDB_SDK_PLANNER_DECODE_RESPONSE_TOO_SMALL;
    }

    offset = 3 + 32;
    if (!take_name(payload, payload_len, &offset, &result.name,
                   &result.name_len)) {
        return PRDB_SDK_PLANNER_DECODE_INVALID_PAYLOAD;
    }
    offset += 2;
    for (uint32_t index = 0; index < column_count; ++index) {
        if (!validate_column(payload, payload_len, &offset, index,
                             &columns[index])) {
            return PRDB_SDK_PLANNER_DECODE_INVALID_PAYLOAD;
        }
    }
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_PLANNER_ABI_V1_VERSION;
    result.available = 1;
    result.column_count = column_count;
    *out = result;
    return PRDB_SDK_PLANNER_DECODE_OK;
}

int32_t prdb_sdk_executor_decode_relation_v1(
    const uint8_t *payload, uint64_t payload_len,
    uint8_t relation_id_out[16], uint8_t executor_handle_out[16],
    const uint8_t **name_out, uint32_t *name_len_out,
    uint32_t *column_count_out, uint32_t output_capacity,
    prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_capacity) {
    uint32_t offset = 3;
    uint32_t column_count;
    uint8_t relation_id[16];
    uint8_t executor_handle[16];
    const uint8_t *name;
    uint32_t name_len;

    if (payload == NULL || relation_id_out == NULL ||
        executor_handle_out == NULL || name_out == NULL ||
        name_len_out == NULL || column_count_out == NULL ||
        output_capacity < 4 || columns == NULL) {
        return PRDB_SDK_PLANNER_DECODE_INVALID_ARGUMENT;
    }
    if (payload_len > PRDB_SDK_PLANNER_MAX_RESPONSE_BYTES || payload_len < 3) {
        return PRDB_SDK_PLANNER_DECODE_INVALID_PAYLOAD;
    }
    if (read_u16(payload, 0) != PRDB_SDK_PLANNER_WIRE_V1_VERSION) {
        return PRDB_SDK_PLANNER_DECODE_UNSUPPORTED_VERSION;
    }
    if (payload[2] == 1 && payload_len == 3) {
        return PRDB_SDK_PLANNER_DECODE_UNAVAILABLE;
    }
    if (payload[2] != 0 || payload_len < 40) {
        return PRDB_SDK_PLANNER_DECODE_INVALID_PAYLOAD;
    }
    {
        uint8_t id_zero = 1;
        for (uint32_t index = 0; index < 16; ++index) {
            id_zero &= payload[offset + index] == 0;
        }
        if (id_zero) return PRDB_SDK_PLANNER_DECODE_INVALID_PAYLOAD;
    }
    memcpy(relation_id, payload + offset, 16);
    offset += 16;
    if (payload[offset] != 6) return PRDB_SDK_PLANNER_DECODE_INVALID_PAYLOAD;
    memcpy(executor_handle, payload + offset, 16);
    offset += 16;
    if (!take_name(payload, payload_len, &offset, &name, &name_len) ||
        (uint64_t)offset + 2 > payload_len) {
        return PRDB_SDK_PLANNER_DECODE_INVALID_PAYLOAD;
    }
    column_count = read_u16(payload, offset);
    offset += 2;
    if (column_count == 0 || column_count > PRDB_SDK_PLANNER_MAX_RELATION_COLUMNS) {
        return PRDB_SDK_PLANNER_DECODE_INVALID_PAYLOAD;
    }
    for (uint32_t index = 0; index < column_count; ++index) {
        if (!validate_column(payload, payload_len, &offset, index, NULL)) {
            return PRDB_SDK_PLANNER_DECODE_INVALID_PAYLOAD;
        }
    }
    if (offset != payload_len) return PRDB_SDK_PLANNER_DECODE_INVALID_PAYLOAD;
    if (column_capacity < column_count) return PRDB_SDK_PLANNER_DECODE_RESPONSE_TOO_SMALL;
    offset = 3 + 32;
    if (!take_name(payload, payload_len, &offset, &name, &name_len)) {
        return PRDB_SDK_PLANNER_DECODE_INVALID_PAYLOAD;
    }
    offset += 2;
    for (uint32_t index = 0; index < column_count; ++index) {
        if (!validate_column(payload, payload_len, &offset, index, &columns[index])) {
            return PRDB_SDK_PLANNER_DECODE_INVALID_PAYLOAD;
        }
    }
    memcpy(relation_id_out, relation_id, 16);
    memcpy(executor_handle_out, executor_handle, 16);
    *name_out = name;
    *name_len_out = name_len;
    *column_count_out = column_count;
    return PRDB_SDK_PLANNER_DECODE_OK;
}
