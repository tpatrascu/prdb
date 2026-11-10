#include "prdb_sdk_optimizer_v1.h"
#include "prdb_query_abi_payload_codecs.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#define OPTIMIZER_REQUEST_MAX_BYTES PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES

static int ranges_overlap(const void *left, uint64_t left_len,
                          const void *right, uint64_t right_len);

static void write_u16(uint8_t *bytes, uint16_t value) {
    bytes[0] = (uint8_t)(value >> 8);
    bytes[1] = (uint8_t)value;
}

static void write_u32(uint8_t *bytes, uint32_t value) {
    bytes[0] = (uint8_t)(value >> 24);
    bytes[1] = (uint8_t)(value >> 16);
    bytes[2] = (uint8_t)(value >> 8);
    bytes[3] = (uint8_t)value;
}

static int valid_identifier(const uint8_t *bytes, uint32_t length) {
    uint32_t index;

    if (bytes == NULL || length == 0 || length > 63 ||
        !((bytes[0] >= 'A' && bytes[0] <= 'Z') ||
          (bytes[0] >= 'a' && bytes[0] <= 'z') || bytes[0] == '_')) {
        return 0;
    }
    for (index = 1; index < length; ++index) {
        uint8_t byte = bytes[index];
        if (!((byte >= 'A' && byte <= 'Z') ||
              (byte >= 'a' && byte <= 'z') ||
              (byte >= '0' && byte <= '9') || byte == '_')) {
            return 0;
        }
    }
    return 1;
}

static int span_is_identifier(const prdb_sdk_select_v2 *statement,
                              prdb_sdk_sql_span_v1 span) {
    return span.length != 0 && span.offset <= statement->sql_len &&
           span.length <= statement->sql_len - span.offset &&
           valid_identifier(statement->sql + span.offset, span.length);
}

static int predicate_append(uint8_t *output, uint64_t capacity,
                            uint64_t *offset, const uint8_t *bytes,
                            uint64_t length) {
    if (*offset > UINT64_MAX - length ||
        (output != NULL && (*offset > capacity ||
                            length > capacity - *offset))) return 0;
    if (output != NULL && length != 0)
        memcpy(output + *offset, bytes, (size_t)length);
    *offset += length;
    return 1;
}

static int predicate_append_u8(uint8_t *output, uint64_t capacity,
                              uint64_t *offset, uint8_t value) {
    return predicate_append(output, capacity, offset, &value, 1);
}

static int predicate_append_u16(uint8_t *output, uint64_t capacity,
                               uint64_t *offset, uint16_t value) {
    uint8_t bytes[2] = {(uint8_t)(value >> 8), (uint8_t)value};
    return predicate_append(output, capacity, offset, bytes, sizeof(bytes));
}

static int predicate_append_u64(uint8_t *output, uint64_t capacity,
                               uint64_t *offset, uint64_t value) {
    uint8_t bytes[8];
    for (uint32_t index = 0; index < 8; ++index)
        bytes[index] = (uint8_t)(value >> (56 - index * 8));
    return predicate_append(output, capacity, offset, bytes, sizeof(bytes));
}

static int encode_optimizer_parameter(
    const prdb_sdk_value_v1 *value, uint8_t *output, uint64_t capacity,
    uint64_t *offset) {
    int32_t valid = prdb_sdk_value_validate_v1(value);
    if (valid != PRDB_SDK_VALUE_VALID) return 0;
    if (value->state == PRDB_SDK_VALUE_STATE_NULL)
        return predicate_append_u8(output, capacity, offset, 0x00);
    if (value->state != PRDB_SDK_VALUE_STATE_VALUE) return 0;
    switch (value->type_code) {
    case PRDB_SDK_VALUE_TYPE_BOOLEAN:
        return predicate_append_u8(output, capacity, offset, 0x01) &&
               predicate_append(output, capacity, offset, value->data, 1);
    case PRDB_SDK_VALUE_TYPE_INTEGER: {
        uint8_t prefix[4];
        memset(prefix, (value->data[0] & 0x80) != 0 ? 0xff : 0,
               sizeof(prefix));
        return predicate_append_u8(output, capacity, offset, 0x02) &&
               predicate_append(output, capacity, offset, prefix,
                                sizeof(prefix)) &&
               predicate_append(output, capacity, offset, value->data, 4);
    }
    case PRDB_SDK_VALUE_TYPE_TEXT:
        return value->data_len <= 4096 && value->data_len <= UINT16_MAX &&
               predicate_append_u8(output, capacity, offset, 0x03) &&
               predicate_append_u16(output, capacity, offset,
                                    (uint16_t)value->data_len) &&
               predicate_append(output, capacity, offset, value->data,
                                value->data_len);
    default:
        return 2;
    }
}

static int encode_optimizer_predicate_node(
    const prdb_sdk_select_v2 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint32_t node_index, uint32_t depth, uint8_t *seen,
    uint8_t *output, uint64_t capacity, uint64_t *offset) {
    const prdb_sdk_predicate_node_v2 *node;
    int status;
    if (depth > PRDB_SDK_SQL_MAX_EXPRESSION_DEPTH ||
        node_index >= statement->predicate_count || seen[node_index] != 0)
        return 0;
    seen[node_index] = 1;
    node = &statement->predicates[node_index];
    switch (node->node_kind) {
    case PRDB_SDK_SQL_PREDICATE_COMPARE: {
        prdb_sdk_sql_span_v1 column = node->column;
        if (node->comparison < PRDB_SDK_SQL_COMPARE_EQUAL ||
            node->comparison > PRDB_SDK_SQL_COMPARE_GREATER_EQUAL ||
            node->left_child != 0 || node->right_child != 0 ||
            !span_is_identifier(statement, column) ||
            node->parameter_index == 0 ||
            node->parameter_index > parameter_count || parameters == NULL)
            return 0;
        if (!predicate_append_u8(output, capacity, offset, 0x20) ||
            !predicate_append_u8(output, capacity, offset,
                                 (uint8_t)(node->comparison -
                                           PRDB_SDK_SQL_COMPARE_EQUAL)) ||
            !predicate_append_u8(output, capacity, offset, 0x10) ||
            !predicate_append_u8(output, capacity, offset,
                                 (uint8_t)column.length) ||
            !predicate_append(output, capacity, offset,
                              statement->sql + column.offset, column.length))
            return 0;
        status = encode_optimizer_parameter(
            &parameters[node->parameter_index - 1], output, capacity, offset);
        return status;
    }
    case PRDB_SDK_SQL_PREDICATE_AND:
    case PRDB_SDK_SQL_PREDICATE_OR: {
        uint8_t tag = node->node_kind == PRDB_SDK_SQL_PREDICATE_AND
                          ? 0x30
                          : 0x31;
        if (node->comparison != 0 || node->column.offset != 0 ||
            node->column.length != 0 || node->parameter_index != 0)
            return 0;
        if (!predicate_append_u8(output, capacity, offset, tag) ||
            !predicate_append_u8(output, capacity, offset, 2)) return 0;
        status = encode_optimizer_predicate_node(
            statement, parameters, parameter_count, node->left_child,
            depth + 1, seen, output, capacity, offset);
        if (status != 1) return status;
        return encode_optimizer_predicate_node(
            statement, parameters, parameter_count, node->right_child,
            depth + 1, seen, output, capacity, offset);
    }
    case PRDB_SDK_SQL_PREDICATE_NOT:
        if (node->right_child != 0 || node->comparison != 0 ||
            node->column.offset != 0 || node->column.length != 0 ||
            node->parameter_index != 0) return 0;
        if (!predicate_append_u8(output, capacity, offset, 0x32)) return 0;
        return encode_optimizer_predicate_node(
            statement, parameters, parameter_count, node->left_child,
            depth + 1, seen, output, capacity, offset);
    case PRDB_SDK_SQL_PREDICATE_IS_NULL:
    case PRDB_SDK_SQL_PREDICATE_IS_NOT_NULL:
        return 2;
    default:
        return 0;
    }
}

static int32_t optimizer_encode_select_internal(
    const prdb_sdk_select_v2 *statement, const uint8_t *schema,
    uint32_t schema_len, const prdb_sdk_value_v1 *parameters,
    uint32_t parameter_count, uint8_t *request, uint64_t request_capacity,
    uint64_t *request_len) {
    uint64_t encoded_len;
    uint64_t offset;
    uint64_t predicate_len = 0;
    uint32_t index;
    uint8_t seen[PRDB_SDK_SQL_MAX_PREDICATE_NODES] = {0};
    int predicate_status = 1;

    if (statement == NULL || schema == NULL || request == NULL ||
        request_len == NULL ||
        statement->struct_size != sizeof(prdb_sdk_select_v2) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V2_VERSION ||
        statement->sql == NULL || statement->sql_len == 0 ||
        statement->sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        request_capacity > PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES ||
        parameter_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        (parameter_count != 0 && parameters == NULL) ||
        statement->projection_star > 1 || statement->has_limit > 1 ||
        statement->limit > PRDB_SDK_SQL_MAX_LIMIT ||
        !valid_identifier(schema, schema_len) ||
        ranges_overlap(request_len, sizeof(*request_len), request,
                       request_capacity) ||
        ranges_overlap(request_len, sizeof(*request_len), statement,
                       sizeof(*statement)) ||
        ranges_overlap(request_len, sizeof(*request_len), schema, schema_len) ||
        ranges_overlap(request_len, sizeof(*request_len), statement->sql,
                       statement->sql_len) ||
        ranges_overlap(request_len, sizeof(*request_len), parameters,
                       (uint64_t)parameter_count * sizeof(*parameters)) ||
        ranges_overlap(request, request_capacity, statement,
                       sizeof(*statement)) ||
        ranges_overlap(request, request_capacity, schema, schema_len) ||
        ranges_overlap(request, request_capacity, statement->sql,
                       statement->sql_len) ||
        ranges_overlap(request, request_capacity, parameters,
                       (uint64_t)parameter_count * sizeof(*parameters))) {
        return request_capacity > PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES
                   ? PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED
                   : PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    }
    if (statement->projection_star != 0 ||
        statement->predicate_count > PRDB_SDK_SQL_MAX_PREDICATE_NODES) {
        return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    }
    if (statement->predicate_count == 0) {
        if (statement->predicate_root != 0) return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    } else {
        if (statement->predicate_root >= statement->predicate_count)
            return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
        predicate_status = encode_optimizer_predicate_node(
            statement, parameters, parameter_count, statement->predicate_root,
            1, seen, NULL, 0, &predicate_len);
        if (predicate_status == 2) return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
        if (predicate_status != 1)
            return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
        for (index = 0; index < statement->predicate_count; ++index)
            if (seen[index] == 0)
                return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
        if (predicate_len > UINT32_C(16384))
            return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;
        memset(seen, 0, sizeof(seen));
    }
    if (statement->projection_count == 0 ||
        statement->projection_count > PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS) {
        return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    }
    if (!span_is_identifier(statement, statement->relation)) {
        return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    }
    encoded_len = 2 + 1 + 1 + 1 + schema_len + 1 +
                  statement->relation.length + 2 + 4 + predicate_len + 1 +
                  (statement->has_limit != 0 ? 4 : 0);
    for (index = 0; index < statement->projection_count; ++index) {
        prdb_sdk_sql_span_v1 projection = statement->projection[index];
        if (!span_is_identifier(statement, projection)) {
            return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
        }
        encoded_len += 1 + projection.length;
    }
    if (encoded_len > PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES) {
        return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;
    }
    if (request_capacity < encoded_len) {
        *request_len = encoded_len;
        return PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL;
    }

    offset = 0;
    prdb_encode_optimizer_select_request(request,
                                         PRDB_OPTIMIZER_SELECT_STATEMENT, 0);
    offset = PRDB_OPTIMIZER_SELECT_REQUEST_HEADER_BYTES;
    request[offset++] = (uint8_t)schema_len;
    memcpy(request + offset, schema, schema_len);
    offset += schema_len;
    request[offset++] = (uint8_t)statement->relation.length;
    memcpy(request + offset, statement->sql + statement->relation.offset,
           statement->relation.length);
    offset += statement->relation.length;
    write_u16(request + offset, (uint16_t)statement->projection_count);
    offset += 2;
    for (index = 0; index < statement->projection_count; ++index) {
        prdb_sdk_sql_span_v1 projection = statement->projection[index];
        request[offset++] = (uint8_t)projection.length;
        memcpy(request + offset, statement->sql + projection.offset,
               projection.length);
        offset += projection.length;
    }
    write_u32(request + offset, (uint32_t)predicate_len);
    offset += 4;
    if (predicate_len != 0) {
        uint64_t written = 0;
        predicate_status = encode_optimizer_predicate_node(
            statement, parameters, parameter_count, statement->predicate_root,
            1, seen, request + offset, request_capacity - offset, &written);
        if (predicate_status != 1 || written != predicate_len)
            return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
        offset += written;
    }
    request[offset++] = (uint8_t)(statement->has_limit != 0);
    if (statement->has_limit != 0) {
        write_u32(request + offset, statement->limit);
        offset += 4;
    }
    *request_len = offset;
    return PRDB_SDK_OPTIMIZER_ENCODE_OK;
}

int32_t prdb_sdk_optimizer_encode_select_v1(
    const prdb_sdk_select_v2 *statement, const uint8_t *schema,
    uint32_t schema_len, uint8_t *request, uint64_t request_capacity,
    uint64_t *request_len) {
    if (statement != NULL && statement->predicate_count != 0)
        return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    return optimizer_encode_select_internal(
        statement, schema, schema_len, NULL, 0, request, request_capacity,
        request_len);
}

int32_t prdb_sdk_optimizer_encode_select_with_parameters_v1(
    const prdb_sdk_select_v2 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity, uint64_t *request_len) {
    return optimizer_encode_select_internal(
        statement, schema, schema_len, parameters, parameter_count, request,
        request_capacity, request_len);
}

static int ranges_overlap(const void *left, uint64_t left_len,
                          const void *right, uint64_t right_len) {
    uintptr_t left_start = (uintptr_t)left;
    uintptr_t right_start = (uintptr_t)right;
    uintptr_t left_end;
    uintptr_t right_end;

    if (left_len == 0 || right_len == 0) {
        return 0;
    }
    if (left_start > UINTPTR_MAX - left_len ||
        right_start > UINTPTR_MAX - right_len) {
        return 1;
    }
    left_end = left_start + (uintptr_t)left_len;
    right_end = right_start + (uintptr_t)right_len;
    return left_start < right_end && right_start < left_end;
}

int32_t prdb_sdk_optimizer_decode_response_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_response_v1 *out, uint32_t out_capacity) {
    prdb_sdk_optimizer_response_v1 decoded;
    uint32_t plan_len;

    if (payload == NULL || out == NULL ||
        out_capacity < sizeof(prdb_sdk_optimizer_response_v1) ||
        payload_len > PRDB_QUERY_MAX_IPC_REQUEST_BYTES || payload_len < 3 ||
        ranges_overlap(payload, payload_len, out, sizeof(*out))) {
        return PRDB_SDK_OPTIMIZER_RESULT_INVALID_ARGUMENT;
    }
    decoded.struct_size = sizeof(decoded);
    decoded.abi_version = PRDB_SDK_OPTIMIZER_ABI_V1_VERSION;
    decoded.status = payload[2];
    decoded.plan = NULL;
    decoded.plan_len = 0;
    if (decoded.status > PRDB_SDK_OPTIMIZER_STATUS_INTERNAL) {
        return PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE;
    }
    if (decoded.status != PRDB_SDK_OPTIMIZER_STATUS_SUCCESS) {
        uint8_t response_status;
        if (!prdb_decode_optimizer_response_failure(
                payload, (size_t)payload_len, &response_status) ||
            response_status != decoded.status) {
            return PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE;
        }
        *out = decoded;
        return PRDB_SDK_OPTIMIZER_RESULT_OK;
    }
    uint8_t response_status;
    if (!prdb_decode_optimizer_response_success(
            payload, (size_t)payload_len, &response_status, &plan_len) ||
        response_status != decoded.status) {
        return PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE;
    }
    if (plan_len == 0 || plan_len > PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES ||
        payload_len != (uint64_t)7 + plan_len) {
        return PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE;
    }
    decoded.plan = payload + 7;
    decoded.plan_len = plan_len;
    *out = decoded;
    return PRDB_SDK_OPTIMIZER_RESULT_OK;
}

int32_t prdb_sdk_optimizer_submit_select_v1(
    const prdb_host_api_v4 *host, const uint8_t *request, uint64_t request_len,
    uint8_t *response, uint64_t response_capacity, uint64_t *response_len,
    prdb_sdk_optimizer_response_v1 *out, uint32_t out_capacity) {
    int32_t callback_status;

    if (host == NULL || host->v3.struct_size != sizeof(prdb_host_api_v3) ||
        host->v3.abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->v3.context == NULL || host->optimizer_request == NULL ||
        request_len == 0 || request == NULL ||
        request_len > OPTIMIZER_REQUEST_MAX_BYTES || response_len == NULL ||
        response_capacity > PRDB_QUERY_MAX_IPC_REQUEST_BYTES ||
        (response_capacity != 0 && response == NULL) || out == NULL ||
        out_capacity < sizeof(*out) ||
        ranges_overlap(out, sizeof(*out), response, response_capacity)) {
        return request_len > OPTIMIZER_REQUEST_MAX_BYTES ||
                       response_capacity > PRDB_QUERY_MAX_IPC_REQUEST_BYTES
                   ? PRDB_SDK_OPTIMIZER_RESULT_LIMIT_EXCEEDED
                   : PRDB_SDK_OPTIMIZER_RESULT_INVALID_ARGUMENT;
    }
    callback_status = host->optimizer_request(
        host->v3.context, request, request_len, response, response_capacity,
        response_len);
    if (callback_status == PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL) {
        if (*response_len > PRDB_QUERY_MAX_IPC_REQUEST_BYTES) {
            return PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE;
        }
        return PRDB_SDK_OPTIMIZER_RESULT_RESPONSE_TOO_SMALL;
    }
    if (callback_status == PRDB_QUERY_CALLBACK_LIMIT_EXCEEDED) {
        return PRDB_SDK_OPTIMIZER_RESULT_LIMIT_EXCEEDED;
    }
    if (callback_status != PRDB_QUERY_CALLBACK_OK) {
        return PRDB_SDK_OPTIMIZER_RESULT_CALLBACK_ERROR;
    }
    if (*response_len > response_capacity) {
        return PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE;
    }
    return prdb_sdk_optimizer_decode_response_v1(response, *response_len, out,
                                                 out_capacity);
}
