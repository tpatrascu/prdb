#include "prdb_sdk_executor_v1.h"

#include <string.h>

static void write_u16(uint8_t *out, uint16_t value) {
    out[0] = (uint8_t)(value >> 8);
    out[1] = (uint8_t)value;
}

static uint16_t read_u16(const uint8_t *in) {
    return (uint16_t)(((uint16_t)in[0] << 8) | in[1]);
}

static int valid_identifier(const uint8_t *value, uint32_t length) {
    if (value == NULL || length == 0 || length > PRDB_SDK_SQL_MAX_IDENTIFIER_BYTES ||
        !((value[0] >= 'a' && value[0] <= 'z') ||
          (value[0] >= 'A' && value[0] <= 'Z') || value[0] == '_')) return 0;
    for (uint32_t i = 1; i < length; ++i) {
        uint8_t c = value[i];
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9') || c == '_')) return 0;
    }
    return 1;
}

static int32_t invoke(const prdb_host_api_v2 *host, uint16_t operation,
                      const uint8_t *request, uint64_t request_len,
                      uint8_t *response, uint64_t response_capacity,
                      uint64_t *response_len) {
    if (host == NULL || host->struct_size < sizeof(*host) ||
        host->abi_version != PRDB_QUERY_ABI_V2_VERSION ||
        host->executor_request == NULL || request == NULL || response == NULL ||
        response_len == NULL || request_len > PRDB_QUERY_MAX_IPC_REQUEST_BYTES ||
        response_capacity > PRDB_QUERY_MAX_IPC_RESPONSE_BYTES) {
        return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    }
    *response_len = 0;
    int32_t status = host->executor_request(host->context, operation, request,
                                            request_len, response,
                                            response_capacity, response_len);
    if (status == PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL) {
        return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;
    }
    if (status != PRDB_QUERY_CALLBACK_OK) return PRDB_SDK_EXECUTOR_INVALID_PAYLOAD;
    if (*response_len > response_capacity) return PRDB_SDK_EXECUTOR_INVALID_PAYLOAD;
    return PRDB_SDK_EXECUTOR_OK;
}

int32_t prdb_sdk_executor_resolve_relation_v1(
    const prdb_host_api_v2 *host, const uint8_t *schema, uint32_t schema_len,
    const uint8_t *relation, uint32_t relation_len, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len,
    prdb_sdk_executor_relation_v1 *out, uint32_t out_capacity,
    prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_capacity) {
    uint8_t request[6 + 2 * PRDB_SDK_SQL_MAX_IDENTIFIER_BYTES];
    prdb_sdk_executor_relation_v1 result = {0};
    const uint8_t *name;
    uint32_t name_len;
    uint32_t column_count;
    int32_t status;
    if (!valid_identifier(schema, schema_len) ||
        !valid_identifier(relation, relation_len) || out == NULL ||
        out_capacity < sizeof(*out) || columns == NULL) {
        return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    }
    write_u16(request, 1);
    write_u16(request + 2, (uint16_t)schema_len);
    write_u16(request + 4, (uint16_t)relation_len);
    memcpy(request + 6, schema, schema_len);
    memcpy(request + 6 + schema_len, relation, relation_len);
    status = invoke(host, PRDB_SDK_EXECUTOR_OP_RESOLVE_RELATION, request,
                    6 + schema_len + relation_len, response, response_capacity,
                    response_len);
    if (status != PRDB_SDK_EXECUTOR_OK) return status;
    status = prdb_sdk_executor_decode_relation_v1(
        response, *response_len, result.relation_id, result.executor_handle,
        &name, &name_len, &column_count, sizeof(result), columns, column_capacity);
    if (status == PRDB_SDK_PLANNER_DECODE_UNAVAILABLE) return PRDB_SDK_EXECUTOR_UNAVAILABLE;
    if (status == PRDB_SDK_PLANNER_DECODE_RESPONSE_TOO_SMALL) return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;
    if (status != PRDB_SDK_PLANNER_DECODE_OK) return PRDB_SDK_EXECUTOR_INVALID_PAYLOAD;
    result.struct_size = sizeof(result);
    result.abi_version = PRDB_SDK_EXECUTOR_ABI_V1_VERSION;
    result.name = name;
    result.name_len = name_len;
    result.column_count = column_count;
    *out = result;
    return PRDB_SDK_EXECUTOR_OK;
}

int32_t prdb_sdk_executor_open_full_scan_v1(
    const prdb_host_api_v2 *host, const uint8_t executor_handle[16],
    uint8_t *response, uint64_t response_capacity, uint64_t *response_len,
    uint8_t cursor_out[16]) {
    uint8_t request[20] = {0};
    uint8_t cursor[16];
    int32_t status;
    if (executor_handle == NULL || executor_handle[0] != 6 || cursor_out == NULL) {
        return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    }
    write_u16(request, 1);
    memcpy(request + 2, executor_handle, 16);
    status = invoke(host, PRDB_SDK_EXECUTOR_OP_OPEN_SCAN, request, sizeof(request),
                    response, response_capacity, response_len);
    if (status != PRDB_SDK_EXECUTOR_OK) return status;
    if (*response_len != 18 || read_u16(response) != 1 || response[2] != 7) {
        return PRDB_SDK_EXECUTOR_INVALID_PAYLOAD;
    }
    memcpy(cursor, response + 2, 16);
    memcpy(cursor_out, cursor, 16);
    return PRDB_SDK_EXECUTOR_OK;
}

int32_t prdb_sdk_executor_next_batch_v1(
    const prdb_host_api_v2 *host, const uint8_t cursor[16],
    uint16_t max_candidates, uint32_t max_bytes, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len,
    prdb_sdk_batch_v1 *batch, uint32_t batch_out_capacity) {
    uint8_t request[24];
    int32_t status;
    if (cursor == NULL || cursor[0] != 7 || max_candidates == 0 ||
        max_candidates > PRDB_SDK_BATCH_MAX_ROWS || max_bytes < 8 ||
        max_bytes > PRDB_SDK_BATCH_MAX_BYTES || batch == NULL ||
        batch_out_capacity < sizeof(*batch)) return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    write_u16(request, 1);
    memcpy(request + 2, cursor, 16);
    write_u16(request + 18, max_candidates);
    request[20] = (uint8_t)(max_bytes >> 24);
    request[21] = (uint8_t)(max_bytes >> 16);
    request[22] = (uint8_t)(max_bytes >> 8);
    request[23] = (uint8_t)max_bytes;
    status = invoke(host, PRDB_SDK_EXECUTOR_OP_NEXT_BATCH, request, sizeof(request),
                    response, response_capacity, response_len);
    if (status != PRDB_SDK_EXECUTOR_OK) return status;
    status = prdb_sdk_batch_open_v1(batch, batch_out_capacity, response, *response_len);
    if (status != PRDB_SDK_BATCH_OK) return PRDB_SDK_EXECUTOR_INVALID_PAYLOAD;
    if (!batch->finished &&
        (batch->continuation == NULL || batch->continuation[0] != 7)) {
        return PRDB_SDK_EXECUTOR_INVALID_PAYLOAD;
    }
    return PRDB_SDK_EXECUTOR_OK;
}

int32_t prdb_sdk_executor_close_cursor_v1(
    const prdb_host_api_v2 *host, const uint8_t cursor[16], uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len) {
    uint8_t request[18];
    int32_t status;
    if (cursor == NULL || cursor[0] != 7) return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    write_u16(request, 1);
    memcpy(request + 2, cursor, 16);
    status = invoke(host, PRDB_SDK_EXECUTOR_OP_CLOSE_CURSOR, request, sizeof(request),
                    response, response_capacity, response_len);
    if (status != PRDB_SDK_EXECUTOR_OK) return status;
    if (*response_len != 4 || read_u16(response) != 1 || read_u16(response + 2) != 0) {
        return PRDB_SDK_EXECUTOR_INVALID_PAYLOAD;
    }
    return PRDB_SDK_EXECUTOR_OK;
}

int32_t prdb_sdk_executor_stage_delete_v1(
    const prdb_host_api_v2 *host, const uint8_t row_handle[16],
    uint8_t *response, uint64_t response_capacity, uint64_t *response_len) {
    uint8_t request[18];
    int32_t status;
    if (row_handle == NULL || row_handle[0] != PRDB_SDK_BATCH_ROW_TOKEN_KIND) {
        return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    }
    write_u16(request, 1);
    memcpy(request + 2, row_handle, 16);
    status = invoke(host, PRDB_SDK_EXECUTOR_OP_STAGE_DELETE, request,
                    sizeof(request), response, response_capacity, response_len);
    if (status != PRDB_SDK_EXECUTOR_OK) return status;
    if (*response_len != 3 || read_u16(response) != 1 || response[2] != 0) {
        return PRDB_SDK_EXECUTOR_INVALID_PAYLOAD;
    }
    return PRDB_SDK_EXECUTOR_OK;
}
