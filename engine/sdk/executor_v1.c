#include "prdb_sdk_executor_v1.h"
#include "prdb_query_abi_payload_codecs.h"

#include <string.h>

static void write_u16(uint8_t *out, uint16_t value) {
    out[0] = (uint8_t)(value >> 8);
    out[1] = (uint8_t)value;
}

static void write_u32(uint8_t *out, uint32_t value) {
    out[0] = (uint8_t)(value >> 24);
    out[1] = (uint8_t)(value >> 16);
    out[2] = (uint8_t)(value >> 8);
    out[3] = (uint8_t)value;
}

static uint16_t read_u16(const uint8_t *in) {
    return (uint16_t)(((uint16_t)in[0] << 8) | in[1]);
}

static int valid_mutation_acknowledgement(const uint8_t *payload,
                                          uint64_t payload_len) {
    return payload_len == PRDB_EXECUTOR_MUTATION_ACKNOWLEDGEMENT_HEADER_BYTES &&
           read_u16(payload + PRDB_EXECUTOR_MUTATION_ACKNOWLEDGEMENT_VERSION_OFFSET) ==
               PRDB_EXECUTOR_BATCH_PAYLOAD_VERSION &&
           payload[PRDB_EXECUTOR_MUTATION_ACKNOWLEDGEMENT_STATUS_OFFSET] == 0;
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
    uint8_t request[PRDB_EXECUTOR_RESOLVE_RELATION_REQUEST_HEADER_BYTES +
                    2 * PRDB_SDK_SQL_MAX_IDENTIFIER_BYTES];
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
    write_u16(request + PRDB_EXECUTOR_RESOLVE_RELATION_REQUEST_VERSION_OFFSET,
              PRDB_RESOLVE_RELATION_PAYLOAD_VERSION);
    write_u16(request +
                  PRDB_EXECUTOR_RESOLVE_RELATION_REQUEST_SCHEMA_NAME_LENGTH_OFFSET,
              (uint16_t)schema_len);
    write_u16(request +
                  PRDB_EXECUTOR_RESOLVE_RELATION_REQUEST_RELATION_NAME_LENGTH_OFFSET,
              (uint16_t)relation_len);
    memcpy(request + PRDB_EXECUTOR_RESOLVE_RELATION_REQUEST_HEADER_BYTES,
           schema, schema_len);
    memcpy(request + PRDB_EXECUTOR_RESOLVE_RELATION_REQUEST_HEADER_BYTES +
               schema_len,
           relation, relation_len);
    status = invoke(host, PRDB_SDK_EXECUTOR_OP_RESOLVE_RELATION, request,
                    PRDB_EXECUTOR_RESOLVE_RELATION_REQUEST_HEADER_BYTES +
                        schema_len + relation_len,
                    response, response_capacity,
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
    uint8_t request[PRDB_EXECUTOR_OPEN_SCAN_REQUEST_HEADER_BYTES] = {0};
    uint8_t cursor[16];
    int32_t status;
    if (executor_handle == NULL || executor_handle[0] != 6 || cursor_out == NULL) {
        return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    }
    write_u16(request + PRDB_EXECUTOR_OPEN_SCAN_REQUEST_VERSION_OFFSET,
              PRDB_EXECUTOR_BATCH_PAYLOAD_VERSION);
    memcpy(request + PRDB_EXECUTOR_OPEN_SCAN_REQUEST_RELATION_HANDLE_OFFSET,
           executor_handle, 16);
    status = invoke(host, PRDB_SDK_EXECUTOR_OP_OPEN_SCAN, request, sizeof(request),
                    response, response_capacity, response_len);
    if (status != PRDB_SDK_EXECUTOR_OK) return status;
    if (*response_len != PRDB_EXECUTOR_OPEN_SCAN_RESPONSE_HEADER_BYTES ||
        read_u16(response + PRDB_EXECUTOR_OPEN_SCAN_RESPONSE_VERSION_OFFSET) !=
            PRDB_EXECUTOR_BATCH_PAYLOAD_VERSION ||
        response[PRDB_EXECUTOR_OPEN_SCAN_RESPONSE_CURSOR_HANDLE_OFFSET] != 7) {
        return PRDB_SDK_EXECUTOR_INVALID_PAYLOAD;
    }
    memcpy(cursor, response + PRDB_EXECUTOR_OPEN_SCAN_RESPONSE_CURSOR_HANDLE_OFFSET,
           16);
    memcpy(cursor_out, cursor, 16);
    return PRDB_SDK_EXECUTOR_OK;
}

int32_t prdb_sdk_executor_next_batch_v1(
    const prdb_host_api_v2 *host, const uint8_t cursor[16],
    uint16_t max_candidates, uint32_t max_bytes, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len,
    prdb_sdk_batch_v1 *batch, uint32_t batch_out_capacity) {
    uint8_t request[PRDB_EXECUTOR_NEXT_BATCH_REQUEST_HEADER_BYTES];
    int32_t status;
    if (cursor == NULL || cursor[0] != 7 || max_candidates == 0 ||
        max_candidates > PRDB_SDK_BATCH_MAX_ROWS || max_bytes < 8 ||
        max_bytes > PRDB_SDK_BATCH_MAX_BYTES || batch == NULL ||
        batch_out_capacity < sizeof(*batch)) return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    write_u16(request + PRDB_EXECUTOR_NEXT_BATCH_REQUEST_VERSION_OFFSET,
              PRDB_EXECUTOR_BATCH_PAYLOAD_VERSION);
    memcpy(request + PRDB_EXECUTOR_NEXT_BATCH_REQUEST_CURSOR_HANDLE_OFFSET,
           cursor, 16);
    write_u16(request + PRDB_EXECUTOR_NEXT_BATCH_REQUEST_MAX_SCANNED_OFFSET,
              max_candidates);
    write_u32(request + PRDB_EXECUTOR_NEXT_BATCH_REQUEST_MAX_BYTES_OFFSET,
              max_bytes);
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
    uint8_t request[PRDB_EXECUTOR_CLOSE_CURSOR_REQUEST_HEADER_BYTES];
    int32_t status;
    if (cursor == NULL || cursor[0] != 7) return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    prdb_encode_executor_close_cursor_request(request, cursor);
    status = invoke(host, PRDB_SDK_EXECUTOR_OP_CLOSE_CURSOR, request, sizeof(request),
                    response, response_capacity, response_len);
    if (status != PRDB_SDK_EXECUTOR_OK) return status;
    if (*response_len != PRDB_EXECUTOR_MUTATION_ACKNOWLEDGEMENT_HEADER_BYTES ||
        read_u16(response + PRDB_EXECUTOR_MUTATION_ACKNOWLEDGEMENT_VERSION_OFFSET) !=
            PRDB_EXECUTOR_BATCH_PAYLOAD_VERSION ||
        response[PRDB_EXECUTOR_MUTATION_ACKNOWLEDGEMENT_STATUS_OFFSET] != 0) {
        return PRDB_SDK_EXECUTOR_INVALID_PAYLOAD;
    }
    return PRDB_SDK_EXECUTOR_OK;
}

int32_t prdb_sdk_executor_stage_insert_v1(
    const prdb_host_api_v2 *host, const uint8_t relation_handle[16],
    const prdb_sdk_executor_insert_value_v1 *values, uint16_t value_count,
    uint8_t *request, uint64_t request_capacity, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len,
    uint8_t row_handle_out[16]) {
    uint64_t offset = PRDB_EXECUTOR_STAGE_INSERT_REQUEST_HEADER_BYTES;
    int32_t status;
    if (relation_handle == NULL || relation_handle[0] != 6 || values == NULL ||
        value_count > PRDB_SDK_BATCH_MAX_CELLS_PER_ROW || request == NULL ||
        request_capacity > PRDB_QUERY_MAX_IPC_REQUEST_BYTES || response == NULL ||
        response_len == NULL || row_handle_out == NULL) {
        return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    }
    if (request_capacity < offset) return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;
    write_u16(request + PRDB_EXECUTOR_STAGE_INSERT_REQUEST_VERSION_OFFSET,
              PRDB_EXECUTOR_BATCH_PAYLOAD_VERSION);
    memcpy(request + PRDB_EXECUTOR_STAGE_INSERT_REQUEST_RELATION_HANDLE_OFFSET,
           relation_handle, 16);
    write_u16(request + PRDB_EXECUTOR_STAGE_INSERT_REQUEST_VALUE_COUNT_OFFSET,
              value_count);
    for (uint16_t i = 0; i < value_count; ++i) {
        const prdb_sdk_executor_insert_value_v1 *input = &values[i];
        if (input->struct_size < sizeof(*input) ||
            input->abi_version != PRDB_SDK_EXECUTOR_ABI_V1_VERSION) {
            return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
        }
        if (input->state == PRDB_SDK_EXECUTOR_INSERT_DEFAULT) {
            if (offset >= request_capacity) return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;
            request[offset++] = 2;
        } else if (input->state == PRDB_SDK_EXECUTOR_INSERT_NULL ||
                   input->state == PRDB_SDK_EXECUTOR_INSERT_VALUE) {
            if (prdb_sdk_value_validate_v1(&input->value) != PRDB_SDK_VALUE_VALID ||
                (input->state == PRDB_SDK_EXECUTOR_INSERT_NULL &&
                 input->value.state != PRDB_SDK_VALUE_STATE_NULL) ||
                (input->state == PRDB_SDK_EXECUTOR_INSERT_VALUE &&
                 input->value.state != PRDB_SDK_VALUE_STATE_VALUE)) {
                return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
            }
            if (input->state == PRDB_SDK_EXECUTOR_INSERT_NULL) {
                if (offset >= request_capacity) return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;
                request[offset++] = 1;
            } else {
                uint64_t value_len = input->value.data_len;
                uint64_t value_end = offset + 6 + value_len;
                if (value_len > UINT32_MAX || value_end < offset ||
                    value_end > request_capacity) {
                    return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;
                }
                request[offset++] = 0;
                request[offset++] = (uint8_t)input->value.type_code;
                write_u32(request + offset, (uint32_t)value_len);
                offset += 4;
                if (value_len != 0) {
                    memcpy(request + offset, input->value.data, (size_t)value_len);
                }
                offset += value_len;
            }
        } else {
            return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
        }
        if (offset > PRDB_QUERY_MAX_IPC_REQUEST_BYTES) {
            return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;
        }
    }
    status = invoke(host, PRDB_SDK_EXECUTOR_OP_STAGE_INSERT, request, offset,
                    response, response_capacity, response_len);
    if (status != PRDB_SDK_EXECUTOR_OK) return status;
    if (*response_len != PRDB_EXECUTOR_STAGE_INSERT_RESPONSE_HEADER_BYTES ||
        read_u16(response + PRDB_EXECUTOR_STAGE_INSERT_RESPONSE_VERSION_OFFSET) !=
            PRDB_EXECUTOR_BATCH_PAYLOAD_VERSION ||
        response[PRDB_EXECUTOR_STAGE_INSERT_RESPONSE_ROW_HANDLE_OFFSET] != 8) {
        return PRDB_SDK_EXECUTOR_INVALID_PAYLOAD;
    }
    memcpy(row_handle_out,
           response + PRDB_EXECUTOR_STAGE_INSERT_RESPONSE_ROW_HANDLE_OFFSET, 16);
    return PRDB_SDK_EXECUTOR_OK;
}

int32_t prdb_sdk_executor_stage_update_v1(
    const prdb_host_api_v2 *host, const uint8_t row_handle[16],
    const prdb_sdk_executor_update_patch_v1 *patches, uint16_t patch_count,
    uint8_t *request, uint64_t request_capacity, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len) {
    uint8_t seen[PRDB_SDK_BATCH_MAX_CELLS_PER_ROW / 8] = {0};
    uint64_t offset = PRDB_EXECUTOR_STAGE_UPDATE_REQUEST_HEADER_BYTES;
    int32_t status;
    if (row_handle == NULL || row_handle[0] != PRDB_SDK_BATCH_ROW_TOKEN_KIND ||
        patches == NULL || patch_count > PRDB_SDK_BATCH_MAX_CELLS_PER_ROW ||
        request == NULL || request_capacity > PRDB_QUERY_MAX_IPC_REQUEST_BYTES ||
        response == NULL || response_len == NULL) {
        return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    }
    if (request_capacity < offset) return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;
    write_u16(request + PRDB_EXECUTOR_STAGE_UPDATE_REQUEST_VERSION_OFFSET,
              PRDB_EXECUTOR_BATCH_PAYLOAD_VERSION);
    memcpy(request + PRDB_EXECUTOR_STAGE_UPDATE_REQUEST_ROW_HANDLE_OFFSET,
           row_handle, 16);
    write_u16(request + PRDB_EXECUTOR_STAGE_UPDATE_REQUEST_PATCH_COUNT_OFFSET,
              patch_count);
    for (uint16_t i = 0; i < patch_count; ++i) {
        const prdb_sdk_executor_update_patch_v1 *patch = &patches[i];
        if (patch->struct_size < sizeof(*patch) ||
            patch->abi_version != PRDB_SDK_EXECUTOR_ABI_V1_VERSION ||
            patch->ordinal >= PRDB_SDK_BATCH_MAX_CELLS_PER_ROW) {
            return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
        }
        uint32_t byte_index = patch->ordinal / 8;
        uint8_t mask = (uint8_t)(1U << (patch->ordinal % 8));
        if ((seen[byte_index] & mask) != 0) return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
        seen[byte_index] |= mask;
        if (prdb_sdk_value_validate_v1(&patch->value) != PRDB_SDK_VALUE_VALID ||
            (patch->value.state != PRDB_SDK_VALUE_STATE_NULL &&
             patch->value.state != PRDB_SDK_VALUE_STATE_VALUE)) {
            return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
        }
        uint64_t value_len = patch->value.state == PRDB_SDK_VALUE_STATE_VALUE
                                 ? patch->value.data_len : 0;
        uint64_t value_end = offset + 5 + value_len +
                             (patch->value.state == PRDB_SDK_VALUE_STATE_VALUE ? 5 : 0);
        if (value_len > UINT32_MAX || value_end < offset ||
            value_end > request_capacity ||
            value_end > PRDB_QUERY_MAX_IPC_REQUEST_BYTES) {
            return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;
        }
        write_u32(request + offset, patch->ordinal);
        offset += 4;
        if (patch->value.state == PRDB_SDK_VALUE_STATE_NULL) {
            request[offset++] = 1;
        } else {
            request[offset++] = 0;
            request[offset++] = (uint8_t)patch->value.type_code;
            write_u32(request + offset, (uint32_t)value_len);
            offset += 4;
            if (value_len != 0) {
                memcpy(request + offset, patch->value.data, (size_t)value_len);
            }
            offset += value_len;
        }
    }
    status = invoke(host, PRDB_SDK_EXECUTOR_OP_STAGE_UPDATE, request, offset,
                    response, response_capacity, response_len);
    if (status != PRDB_SDK_EXECUTOR_OK) return status;
    if (!valid_mutation_acknowledgement(response, *response_len)) {
        return PRDB_SDK_EXECUTOR_INVALID_PAYLOAD;
    }
    return PRDB_SDK_EXECUTOR_OK;
}

int32_t prdb_sdk_executor_stage_delete_v1(
    const prdb_host_api_v2 *host, const uint8_t row_handle[16],
    uint8_t *response, uint64_t response_capacity, uint64_t *response_len) {
    uint8_t request[PRDB_EXECUTOR_STAGE_DELETE_REQUEST_HEADER_BYTES];
    int32_t status;
    if (row_handle == NULL || row_handle[0] != PRDB_SDK_BATCH_ROW_TOKEN_KIND) {
        return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    }
    write_u16(request + PRDB_EXECUTOR_STAGE_DELETE_REQUEST_VERSION_OFFSET,
              PRDB_EXECUTOR_BATCH_PAYLOAD_VERSION);
    memcpy(request + PRDB_EXECUTOR_STAGE_DELETE_REQUEST_ROW_HANDLE_OFFSET,
           row_handle, 16);
    status = invoke(host, PRDB_SDK_EXECUTOR_OP_STAGE_DELETE, request,
                    sizeof(request), response, response_capacity, response_len);
    if (status != PRDB_SDK_EXECUTOR_OK) return status;
    if (!valid_mutation_acknowledgement(response, *response_len)) {
        return PRDB_SDK_EXECUTOR_INVALID_PAYLOAD;
    }
    return PRDB_SDK_EXECUTOR_OK;
}

int32_t prdb_sdk_executor_check_cancelled_v1(
    const prdb_host_api_v2 *host, int32_t *cancelled_out) {
    const uint8_t empty_request = 0;
    uint8_t response[PRDB_EXECUTOR_CANCELLATION_CHECK_RESPONSE_HEADER_BYTES];
    uint64_t response_len = 0;
    int32_t status;
    if (cancelled_out == NULL) return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    status = invoke(host, PRDB_SDK_EXECUTOR_OP_CHECK_CANCELLATION,
                    &empty_request, 0, response, sizeof(response), &response_len);
    if (status != PRDB_SDK_EXECUTOR_OK) return status;
    if (response_len != sizeof(response) ||
        read_u16(response + PRDB_EXECUTOR_CANCELLATION_CHECK_RESPONSE_VERSION_OFFSET) !=
            PRDB_EXECUTOR_BATCH_PAYLOAD_VERSION ||
        response[PRDB_EXECUTOR_CANCELLATION_CHECK_RESPONSE_CANCELLED_OFFSET] > 1) {
        return PRDB_SDK_EXECUTOR_INVALID_PAYLOAD;
    }
    *cancelled_out = (int32_t)response[
        PRDB_EXECUTOR_CANCELLATION_CHECK_RESPONSE_CANCELLED_OFFSET];
    return PRDB_SDK_EXECUTOR_OK;
}
