#include "prdb_sdk_optimizer_v1.h"

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

int32_t prdb_sdk_optimizer_encode_select_v1(
    const prdb_sdk_select_v2 *statement, const uint8_t *schema,
    uint32_t schema_len, uint8_t *request, uint64_t request_capacity,
    uint64_t *request_len) {
    uint64_t encoded_len;
    uint64_t offset;
    uint32_t index;

    if (statement == NULL || schema == NULL || request == NULL ||
        request_len == NULL ||
        statement->struct_size != sizeof(prdb_sdk_select_v2) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V2_VERSION ||
        statement->sql == NULL || statement->sql_len == 0 ||
        statement->sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        request_capacity > PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES ||
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
        ranges_overlap(request, request_capacity, statement,
                       sizeof(*statement)) ||
        ranges_overlap(request, request_capacity, schema, schema_len) ||
        ranges_overlap(request, request_capacity, statement->sql,
                       statement->sql_len)) {
        return request_capacity > PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES
                   ? PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED
                   : PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    }
    if (statement->projection_star != 0 || statement->predicate_count != 0) {
        return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    }
    if (statement->projection_count == 0 ||
        statement->projection_count > PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS) {
        return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    }
    if (!span_is_identifier(statement, statement->relation)) {
        return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    }
    encoded_len = 2 + 1 + 1 + 1 + schema_len + 1 +
                  statement->relation.length + 2 + 4 + 1 +
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
    write_u16(request + offset, 1);
    offset += 2;
    request[offset++] = 1;
    request[offset++] = 0;
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
    write_u32(request + offset, 0);
    offset += 4;
    request[offset++] = (uint8_t)(statement->has_limit != 0);
    if (statement->has_limit != 0) {
        write_u32(request + offset, statement->limit);
        offset += 4;
    }
    *request_len = offset;
    return PRDB_SDK_OPTIMIZER_ENCODE_OK;
}

static uint16_t read_u16(const uint8_t *bytes) {
    return (uint16_t)(((uint16_t)bytes[0] << 8) | bytes[1]);
}

static uint32_t read_u32(const uint8_t *bytes) {
    return ((uint32_t)bytes[0] << 24) | ((uint32_t)bytes[1] << 16) |
           ((uint32_t)bytes[2] << 8) | (uint32_t)bytes[3];
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
    if (read_u16(payload) != PRDB_SDK_OPTIMIZER_RESPONSE_V1_VERSION) {
        return PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE;
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
        if (payload_len != 3) {
            return PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE;
        }
        *out = decoded;
        return PRDB_SDK_OPTIMIZER_RESULT_OK;
    }
    if (payload_len < 7) {
        return PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE;
    }
    plan_len = read_u32(payload + 3);
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
