#include "prdb_sdk_planner_v1.h"
#include "prdb_query_abi_payload_codecs.h"

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
    uint32_t offset = PRDB_PLANNER_DESCRIBE_RELATION_AVAILABLE_RESPONSE_HEADER_BYTES;
    uint32_t column_count;
    uint8_t relation_id_is_zero = 1;
    uint8_t status;

    if (payload == NULL || out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_PLANNER_DECODE_INVALID_ARGUMENT;
    }
    if (payload_len > PRDB_SDK_PLANNER_MAX_RESPONSE_BYTES || payload_len < 3) {
        return PRDB_SDK_PLANNER_DECODE_INVALID_PAYLOAD;
    }
    if (read_u16(payload,
                  PRDB_PLANNER_DESCRIBE_RELATION_UNAVAILABLE_RESPONSE_VERSION_OFFSET) !=
        PRDB_PLANNER_PAYLOAD_VERSION) {
        return PRDB_SDK_PLANNER_DECODE_UNSUPPORTED_VERSION;
    }
    if (prdb_decode_planner_describe_relation_unavailable_response(
            payload, (size_t)payload_len, &status) && status == 1) {
        result.struct_size = sizeof(result);
        result.abi_version = PRDB_SDK_PLANNER_ABI_V1_VERSION;
        *out = result;
        return PRDB_SDK_PLANNER_DECODE_UNAVAILABLE;
    }
    if (!prdb_decode_planner_describe_relation_available_response(
            payload, (size_t)payload_len, &status, result.relation_id,
            result.planning_handle) || status != 0 ||
        payload_len < PRDB_PLANNER_DESCRIBE_RELATION_AVAILABLE_RESPONSE_HEADER_BYTES + 5) {
        return PRDB_SDK_PLANNER_DECODE_INVALID_PAYLOAD;
    }
    for (uint32_t index = 0; index < 16; ++index) {
        relation_id_is_zero &= result.relation_id[index] == 0;
    }
    if (relation_id_is_zero) {
        return PRDB_SDK_PLANNER_DECODE_INVALID_PAYLOAD;
    }
    if (result.planning_handle[0] != PRDB_SDK_PLANNER_TABLE_HANDLE_KIND) {
        return PRDB_SDK_PLANNER_DECODE_INVALID_PAYLOAD;
    }
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

    offset = PRDB_PLANNER_DESCRIBE_RELATION_AVAILABLE_RESPONSE_HEADER_BYTES;
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

static int planner_identifier(const uint8_t *value, uint32_t length) {
    if (value == NULL || length == 0 ||
        length > PRDB_SDK_SQL_MAX_IDENTIFIER_BYTES ||
        !((value[0] >= 'a' && value[0] <= 'z') ||
          (value[0] >= 'A' && value[0] <= 'Z') || value[0] == '_')) return 0;
    for (uint32_t index = 1; index < length; ++index) {
        uint8_t c = value[index];
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9') || c == '_')) return 0;
    }
    return 1;
}

int32_t prdb_sdk_planner_describe_relation_v1(
    const prdb_host_api_v2 *host, const uint8_t *schema, uint32_t schema_len,
    const uint8_t *relation_name, uint32_t relation_name_len, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len,
    prdb_sdk_planner_relation_v1 *out, uint32_t out_capacity,
    prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_capacity) {
    uint8_t request[PRDB_EXECUTOR_RESOLVE_RELATION_REQUEST_HEADER_BYTES +
                    2 * PRDB_SDK_SQL_MAX_IDENTIFIER_BYTES];
    int32_t callback_status;
    int32_t decode_status;
    uint32_t request_len;
    if (host == NULL || host->struct_size < sizeof(*host) ||
        host->abi_version != PRDB_QUERY_ABI_V2_VERSION ||
        host->planner_request == NULL || !planner_identifier(schema, schema_len) ||
        !planner_identifier(relation_name, relation_name_len) || response == NULL ||
        response_len == NULL || out == NULL || out_capacity < sizeof(*out) ||
        columns == NULL || response_capacity > PRDB_SDK_PLANNER_MAX_RESPONSE_BYTES) {
        return PRDB_SDK_PLANNER_REQUEST_INVALID_ARGUMENT;
    }
    request[PRDB_EXECUTOR_RESOLVE_RELATION_REQUEST_VERSION_OFFSET] =
        (uint8_t)(PRDB_RESOLVE_RELATION_PAYLOAD_VERSION >> 8);
    request[PRDB_EXECUTOR_RESOLVE_RELATION_REQUEST_VERSION_OFFSET + 1] =
        (uint8_t)PRDB_RESOLVE_RELATION_PAYLOAD_VERSION;
    request[PRDB_EXECUTOR_RESOLVE_RELATION_REQUEST_SCHEMA_NAME_LENGTH_OFFSET] =
        (uint8_t)(schema_len >> 8);
    request[PRDB_EXECUTOR_RESOLVE_RELATION_REQUEST_SCHEMA_NAME_LENGTH_OFFSET + 1] =
        (uint8_t)schema_len;
    request[PRDB_EXECUTOR_RESOLVE_RELATION_REQUEST_RELATION_NAME_LENGTH_OFFSET] =
        (uint8_t)(relation_name_len >> 8);
    request[PRDB_EXECUTOR_RESOLVE_RELATION_REQUEST_RELATION_NAME_LENGTH_OFFSET + 1] =
        (uint8_t)relation_name_len;
    memcpy(request + PRDB_EXECUTOR_RESOLVE_RELATION_REQUEST_HEADER_BYTES,
           schema, schema_len);
    memcpy(request + PRDB_EXECUTOR_RESOLVE_RELATION_REQUEST_HEADER_BYTES +
               schema_len,
           relation_name, relation_name_len);
    request_len = PRDB_EXECUTOR_RESOLVE_RELATION_REQUEST_HEADER_BYTES +
                  schema_len + relation_name_len;
    *response_len = 0;
    callback_status = host->planner_request(
        host->context, PRDB_SDK_PLANNER_OP_DESCRIBE_RELATION, request,
        request_len, response, response_capacity, response_len);
    if (callback_status == PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL) {
        return PRDB_SDK_PLANNER_REQUEST_RESPONSE_TOO_SMALL;
    }
    if (callback_status != PRDB_QUERY_CALLBACK_OK ||
        *response_len > response_capacity ||
        *response_len > PRDB_SDK_PLANNER_MAX_RESPONSE_BYTES) {
        return PRDB_SDK_PLANNER_REQUEST_INVALID_PAYLOAD;
    }
    decode_status = prdb_sdk_planner_decode_relation_v1(
        response, *response_len, out, out_capacity, columns, column_capacity);
    if (decode_status == PRDB_SDK_PLANNER_DECODE_OK) {
        return PRDB_SDK_PLANNER_REQUEST_OK;
    }
    if (decode_status == PRDB_SDK_PLANNER_DECODE_UNAVAILABLE) {
        return PRDB_SDK_PLANNER_REQUEST_UNAVAILABLE;
    }
    if (decode_status == PRDB_SDK_PLANNER_DECODE_RESPONSE_TOO_SMALL) {
        return PRDB_SDK_PLANNER_REQUEST_RESPONSE_TOO_SMALL;
    }
    return PRDB_SDK_PLANNER_REQUEST_INVALID_PAYLOAD;
}
