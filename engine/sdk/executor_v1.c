#include "prdb_sdk_executor_v1.h"
#include "prdb_query_abi_payload_codecs.h"

#include <string.h>

static int valid_mutation_acknowledgement(const uint8_t *payload,
                                          uint64_t payload_len) {
    uint8_t status;
    return prdb_decode_executor_mutation_acknowledgement(
               payload, (size_t)payload_len, &status) &&
           status == 0;
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
    prdb_encode_executor_resolve_relation_request(
        request, (uint16_t)schema_len, (uint16_t)relation_len);
    if (!prdb_encode_executor_resolve_relation_request_schema_name(
            request + PRDB_EXECUTOR_RESOLVE_RELATION_REQUEST_HEADER_BYTES,
            sizeof(request) - PRDB_EXECUTOR_RESOLVE_RELATION_REQUEST_HEADER_BYTES,
            schema, schema_len) ||
        !prdb_encode_executor_resolve_relation_request_relation_name(
            request + PRDB_EXECUTOR_RESOLVE_RELATION_REQUEST_HEADER_BYTES +
                schema_len,
            sizeof(request) - PRDB_EXECUTOR_RESOLVE_RELATION_REQUEST_HEADER_BYTES -
                schema_len,
            relation, relation_len)) {
        return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    }
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
    prdb_encode_executor_open_scan_request(request, executor_handle, 0);
    status = invoke(host, PRDB_SDK_EXECUTOR_OP_OPEN_SCAN, request, sizeof(request),
                    response, response_capacity, response_len);
    if (status != PRDB_SDK_EXECUTOR_OK) return status;
    if (!prdb_decode_executor_open_scan_response(
            response, (size_t)*response_len, cursor) || cursor[0] != 7) {
        return PRDB_SDK_EXECUTOR_INVALID_PAYLOAD;
    }
    memcpy(cursor_out, cursor, 16);
    return PRDB_SDK_EXECUTOR_OK;
}

static int valid_range_bound(const prdb_sdk_value_v1 *bound) {
    return bound != NULL && bound->struct_size >= sizeof(*bound) &&
           bound->abi_version == PRDB_SDK_VALUE_ABI_V1_VERSION &&
           bound->state == PRDB_SDK_VALUE_STATE_VALUE &&
           prdb_sdk_value_validate_v1(bound) == PRDB_SDK_VALUE_VALID;
}

int32_t prdb_sdk_executor_scan_index_range_v1(
    const prdb_host_api_v2 *host, const uint8_t executor_handle[16],
    const uint8_t index_id[16], const prdb_sdk_value_v1 *lower_bound,
    uint8_t lower_inclusive, const prdb_sdk_value_v1 *upper_bound,
    uint8_t upper_inclusive, const uint8_t *column_ids,
    uint16_t column_count, uint8_t *request, uint64_t request_capacity,
    uint8_t *response, uint64_t response_capacity, uint64_t *response_len,
    prdb_sdk_index_range_v1 *range_out, uint32_t range_out_capacity) {
    uint64_t request_len =
        PRDB_EXECUTOR_SCAN_INDEX_RANGE_REQUEST_HEADER_BYTES + 4;
    uint64_t offset =
        PRDB_EXECUTOR_SCAN_INDEX_RANGE_REQUEST_HEADER_BYTES + 1;
    uint8_t decoded_request_bound_flags[2] = {0, 0};
    int32_t status;

    if (executor_handle == NULL || executor_handle[0] != 6 || index_id == NULL ||
        request == NULL || response == NULL || response_len == NULL ||
        range_out == NULL || range_out_capacity < sizeof(*range_out) ||
        lower_inclusive > 1 || upper_inclusive > 1 ||
        (lower_bound == NULL && upper_bound == NULL) ||
        (column_count != 0 && column_ids == NULL) ||
        column_count > PRDB_SDK_EXECUTOR_MAX_COLUMN_IDS) {
        return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    }
    {
        uint8_t nonzero = 0;
        for (uint32_t i = 0; i < 16; ++i) nonzero |= index_id[i];
        if (nonzero == 0) return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    }
    for (uint16_t i = 0; i < column_count; ++i) {
        const uint8_t *column_id = column_ids + (uint64_t)i * 16;
        uint8_t nonzero = 0;
        for (uint32_t j = 0; j < 16; ++j) nonzero |= column_id[j];
        if (nonzero == 0) return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
        for (uint16_t previous = 0; previous < i; ++previous) {
            if (memcmp(column_id, column_ids + (uint64_t)previous * 16, 16) == 0) {
                return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
            }
        }
    }
    if (lower_bound != NULL) {
        if (!valid_range_bound(lower_bound) || lower_bound->data_len > UINT32_MAX ||
            request_len > PRDB_QUERY_MAX_IPC_REQUEST_BYTES - 5 ||
            lower_bound->data_len > PRDB_QUERY_MAX_IPC_REQUEST_BYTES - request_len - 5) {
            return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
        }
        request_len += 5 + lower_bound->data_len;
        decoded_request_bound_flags[0] = (uint8_t)(lower_inclusive ? 1 : 2);
    }
    if (upper_bound != NULL) {
        if (!valid_range_bound(upper_bound) || upper_bound->data_len > UINT32_MAX ||
            request_len > PRDB_QUERY_MAX_IPC_REQUEST_BYTES - 5 ||
            upper_bound->data_len > PRDB_QUERY_MAX_IPC_REQUEST_BYTES - request_len - 5) {
            return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
        }
        request_len += 5 + upper_bound->data_len;
        decoded_request_bound_flags[1] = (uint8_t)(upper_inclusive ? 1 : 2);
    }
    request_len += (uint64_t)column_count * 16;
    if (request_len > PRDB_QUERY_MAX_IPC_REQUEST_BYTES) {
        return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    }
    if (request_capacity < request_len) {
        return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;
    }

    prdb_encode_executor_scan_index_range_request(request, executor_handle,
                                                 index_id);
    prdb_encode_executor_scan_index_range_bound_flag_record(
        request + PRDB_EXECUTOR_SCAN_INDEX_RANGE_REQUEST_HEADER_BYTES,
        decoded_request_bound_flags[0]);
    if (lower_bound != NULL) {
        uint8_t value_header[PRDB_SCALAR_VALUE_RECORD_HEADER_BYTES] = {0};
        prdb_encode_scalar_value_record(
            value_header, lower_bound->type_code,
            (uint32_t)lower_bound->data_len);
        memcpy(request + offset, value_header, sizeof(value_header));
        offset += sizeof(value_header);
        if (lower_bound->data_len != 0) {
            if (!prdb_encode_scalar_value_record_value_bytes(
                    request + offset,
                    (size_t)(request_capacity - offset), lower_bound->data,
                    (size_t)lower_bound->data_len)) {
                return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;
            }
            offset += lower_bound->data_len;
        }
    }
    prdb_encode_executor_scan_index_range_bound_flag_record(
        request + offset, decoded_request_bound_flags[1]);
    offset += PRDB_EXECUTOR_SCAN_INDEX_RANGE_BOUND_FLAG_RECORD_HEADER_BYTES;
    if (upper_bound != NULL) {
        uint8_t value_header[PRDB_SCALAR_VALUE_RECORD_HEADER_BYTES] = {0};
        prdb_encode_scalar_value_record(
            value_header, upper_bound->type_code,
            (uint32_t)upper_bound->data_len);
        memcpy(request + offset, value_header, sizeof(value_header));
        offset += sizeof(value_header);
        if (upper_bound->data_len != 0) {
            if (!prdb_encode_scalar_value_record_value_bytes(
                    request + offset,
                    (size_t)(request_capacity - offset), upper_bound->data,
                    (size_t)upper_bound->data_len)) {
                return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;
            }
            offset += upper_bound->data_len;
        }
    }
    prdb_encode_executor_scan_index_range_column_count_record(
        request + offset, column_count);
    offset += PRDB_EXECUTOR_SCAN_INDEX_RANGE_COLUMN_COUNT_RECORD_HEADER_BYTES;
    if (column_count != 0) {
        memcpy(request + offset, column_ids, (size_t)column_count * 16);
        offset += (uint64_t)column_count * 16;
    }
    if (offset != request_len) return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    status = invoke(host, PRDB_SDK_EXECUTOR_OP_SCAN_INDEX_RANGE, request,
                    request_len, response, response_capacity, response_len);
    if (status != PRDB_SDK_EXECUTOR_OK) return status;
    status = prdb_sdk_index_range_open_v1(range_out, range_out_capacity,
                                          response, *response_len);
    if (status == PRDB_SDK_BATCH_UNAVAILABLE) return PRDB_SDK_EXECUTOR_UNAVAILABLE;
    if (status != PRDB_SDK_BATCH_OK) return PRDB_SDK_EXECUTOR_INVALID_PAYLOAD;
    return PRDB_SDK_EXECUTOR_OK;
}

int32_t prdb_sdk_executor_scan_index_prefix_v1(
    const prdb_host_api_v2 *host, const uint8_t executor_handle[16],
    const uint8_t index_id[16], const prdb_sdk_value_v1 *values,
    uint8_t value_count, const uint8_t *column_ids, uint16_t column_count,
    uint8_t *request, uint64_t request_capacity, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len,
    prdb_sdk_index_range_v1 *range_out, uint32_t range_out_capacity) {
    uint64_t request_len = 37 + (uint64_t)column_count * 16;
    uint64_t offset = 35;
    int32_t status;
    if (executor_handle == NULL || executor_handle[0] != 6 || index_id == NULL ||
        values == NULL || value_count < 2 || value_count > 8 || request == NULL ||
        response == NULL || response_len == NULL || range_out == NULL ||
        range_out_capacity < sizeof(*range_out) ||
        (column_count != 0 && column_ids == NULL) ||
        column_count > PRDB_SDK_EXECUTOR_MAX_COLUMN_IDS) {
        return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    }
    {
        uint8_t nonzero = 0;
        for (uint32_t i = 0; i < 16; ++i) nonzero |= index_id[i];
        if (nonzero == 0) return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    }
    for (uint16_t i = 0; i < column_count; ++i) {
        const uint8_t *column_id = column_ids + (uint64_t)i * 16;
        uint8_t nonzero = 0;
        for (uint32_t j = 0; j < 16; ++j) nonzero |= column_id[j];
        if (nonzero == 0) return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
        for (uint16_t previous = 0; previous < i; ++previous) {
            if (memcmp(column_id, column_ids + (uint64_t)previous * 16, 16) == 0)
                return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
        }
    }
    for (uint8_t i = 0; i < value_count; ++i) {
        if (!valid_range_bound(&values[i]) || values[i].data_len > UINT32_MAX ||
            request_len > PRDB_QUERY_MAX_IPC_REQUEST_BYTES - 5 ||
            values[i].data_len > PRDB_QUERY_MAX_IPC_REQUEST_BYTES - request_len - 5)
            return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
        request_len += 5 + values[i].data_len;
    }
    if (request_len > PRDB_QUERY_MAX_IPC_REQUEST_BYTES)
        return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    if (request_capacity < request_len)
        return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;

    request[0] = 0;
    request[1] = 1;
    memcpy(request + 2, executor_handle, 16);
    memcpy(request + 18, index_id, 16);
    request[34] = value_count;
    for (uint8_t i = 0; i < value_count; ++i) {
        uint8_t value_header[PRDB_SCALAR_VALUE_RECORD_HEADER_BYTES] = {0};
        prdb_encode_scalar_value_record(value_header, values[i].type_code,
                                        (uint32_t)values[i].data_len);
        memcpy(request + offset, value_header, sizeof(value_header));
        offset += sizeof(value_header);
        if (values[i].data_len != 0) {
            if (!prdb_encode_scalar_value_record_value_bytes(
                    request + offset, (size_t)(request_capacity - offset),
                    values[i].data, (size_t)values[i].data_len))
                return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;
            offset += values[i].data_len;
        }
    }
    request[offset++] = (uint8_t)(column_count >> 8);
    request[offset++] = (uint8_t)column_count;
    if (column_count != 0) {
        memcpy(request + offset, column_ids, (size_t)column_count * 16);
        offset += (uint64_t)column_count * 16;
    }
    if (offset != request_len) return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    status = invoke(host, PRDB_SDK_EXECUTOR_OP_SCAN_INDEX_PREFIX, request,
                    request_len, response, response_capacity, response_len);
    if (status != PRDB_SDK_EXECUTOR_OK) return status;
    status = prdb_sdk_index_range_open_v1(range_out, range_out_capacity,
                                          response, *response_len);
    if (status == PRDB_SDK_BATCH_UNAVAILABLE) return PRDB_SDK_EXECUTOR_UNAVAILABLE;
    if (status != PRDB_SDK_BATCH_OK) return PRDB_SDK_EXECUTOR_INVALID_PAYLOAD;
    return PRDB_SDK_EXECUTOR_OK;
}

int32_t prdb_sdk_executor_scan_index_ranges_v1(
    const prdb_host_api_v2 *host, const uint8_t executor_handle[16],
    const uint8_t index_id[16],
    const prdb_sdk_executor_index_probe_v1 *probes, uint16_t probe_count,
    const uint8_t *column_ids, uint16_t column_count,
    uint8_t *request, uint64_t request_capacity, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len,
    prdb_sdk_index_ranges_v1 *ranges_out, uint32_t ranges_out_capacity) {
    uint64_t request_len = 38 + (uint64_t)column_count * 16;
    uint64_t offset = 36;
    int32_t status;
    if (executor_handle == NULL || executor_handle[0] != 6 || index_id == NULL ||
        probes == NULL || probe_count == 0 || probe_count > PRDB_SDK_BATCH_MAX_ROWS ||
        request == NULL || response == NULL || response_len == NULL ||
        ranges_out == NULL || ranges_out_capacity < sizeof(*ranges_out) ||
        (column_count != 0 && column_ids == NULL) ||
        column_count > PRDB_SDK_EXECUTOR_MAX_COLUMN_IDS)
        return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    {
        uint8_t nonzero = 0;
        for (uint32_t i = 0; i < 16; ++i) nonzero |= index_id[i];
        if (nonzero == 0) return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    }
    for (uint16_t i = 0; i < column_count; ++i) {
        const uint8_t *column_id = column_ids + (uint64_t)i * 16;
        uint8_t nonzero = 0;
        for (uint32_t j = 0; j < 16; ++j) nonzero |= column_id[j];
        if (nonzero == 0) return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
        for (uint16_t previous = 0; previous < i; ++previous) {
            if (memcmp(column_id, column_ids + (uint64_t)previous * 16, 16) == 0)
                return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
        }
    }
    for (uint16_t probe_index = 0; probe_index < probe_count; ++probe_index) {
        const prdb_sdk_executor_index_probe_v1 *probe = &probes[probe_index];
        if (probe->struct_size < sizeof(*probe) ||
            probe->abi_version != PRDB_SDK_EXECUTOR_ABI_V1_VERSION ||
            probe->values == NULL || probe->value_count == 0 ||
            probe->value_count > 8 || request_len >= PRDB_QUERY_MAX_IPC_REQUEST_BYTES)
            return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
        ++request_len;
        for (uint8_t value_index = 0; value_index < probe->value_count; ++value_index) {
            const prdb_sdk_value_v1 *value = &probe->values[value_index];
            if (!valid_range_bound(value) || value->data_len > UINT32_MAX ||
                request_len > PRDB_QUERY_MAX_IPC_REQUEST_BYTES - 5 ||
                value->data_len > PRDB_QUERY_MAX_IPC_REQUEST_BYTES - request_len - 5)
                return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
            request_len += 5 + value->data_len;
        }
    }
    if (request_len > PRDB_QUERY_MAX_IPC_REQUEST_BYTES)
        return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    if (request_capacity < request_len)
        return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;
    request[0] = 0;
    request[1] = 1;
    memcpy(request + 2, executor_handle, 16);
    memcpy(request + 18, index_id, 16);
    request[34] = (uint8_t)(probe_count >> 8);
    request[35] = (uint8_t)probe_count;
    for (uint16_t probe_index = 0; probe_index < probe_count; ++probe_index) {
        const prdb_sdk_executor_index_probe_v1 *probe = &probes[probe_index];
        request[offset++] = probe->value_count;
        for (uint8_t value_index = 0; value_index < probe->value_count; ++value_index) {
            const prdb_sdk_value_v1 *value = &probe->values[value_index];
            uint8_t value_header[PRDB_SCALAR_VALUE_RECORD_HEADER_BYTES] = {0};
            prdb_encode_scalar_value_record(value_header, value->type_code,
                                            (uint32_t)value->data_len);
            memcpy(request + offset, value_header, sizeof(value_header));
            offset += sizeof(value_header);
            if (value->data_len != 0) {
                if (!prdb_encode_scalar_value_record_value_bytes(
                        request + offset, (size_t)(request_capacity - offset),
                        value->data, (size_t)value->data_len))
                    return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;
                offset += value->data_len;
            }
        }
    }
    request[offset++] = (uint8_t)(column_count >> 8);
    request[offset++] = (uint8_t)column_count;
    if (column_count != 0) {
        memcpy(request + offset, column_ids, (size_t)column_count * 16);
        offset += (uint64_t)column_count * 16;
    }
    if (offset != request_len) return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    status = invoke(host, PRDB_SDK_EXECUTOR_OP_SCAN_INDEX_RANGES, request,
                    request_len, response, response_capacity, response_len);
    if (status != PRDB_SDK_EXECUTOR_OK) return status;
    status = prdb_sdk_index_ranges_open_v1(ranges_out, ranges_out_capacity,
                                           response, *response_len);
    if (status == PRDB_SDK_BATCH_UNAVAILABLE) return PRDB_SDK_EXECUTOR_UNAVAILABLE;
    if (status != PRDB_SDK_BATCH_OK) return PRDB_SDK_EXECUTOR_INVALID_PAYLOAD;
    if (ranges_out->group_count != probe_count)
        return PRDB_SDK_EXECUTOR_INVALID_PAYLOAD;
    return PRDB_SDK_EXECUTOR_OK;
}

int32_t prdb_sdk_executor_next_batch_v1(
    const prdb_host_api_v2 *host, const uint8_t cursor[16],
    uint16_t max_candidates, uint32_t max_bytes, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len,
    prdb_sdk_batch_v1 *batch, uint32_t batch_out_capacity) {
    uint8_t request[PRDB_EXECUTOR_NEXT_BATCH_REQUEST_HEADER_BYTES];
    prdb_sdk_batch_v1 decoded_batch = {0};
    int32_t status;
    if (cursor == NULL || cursor[0] != 7 || max_candidates == 0 ||
        max_candidates > PRDB_SDK_BATCH_MAX_ROWS || max_bytes < 8 ||
        max_bytes > PRDB_SDK_BATCH_MAX_BYTES || batch == NULL ||
        batch_out_capacity < sizeof(*batch)) return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    prdb_encode_executor_next_batch_request(request, cursor, max_candidates,
                                            max_bytes);
    status = invoke(host, PRDB_SDK_EXECUTOR_OP_NEXT_BATCH, request, sizeof(request),
                    response, response_capacity, response_len);
    if (status != PRDB_SDK_EXECUTOR_OK) return status;
    if (*response_len > max_bytes) return PRDB_SDK_EXECUTOR_INVALID_PAYLOAD;
    status = prdb_sdk_batch_open_v1(&decoded_batch, sizeof(decoded_batch),
                                    response, *response_len);
    if (status != PRDB_SDK_BATCH_OK) return PRDB_SDK_EXECUTOR_INVALID_PAYLOAD;
    if (decoded_batch.row_count > max_candidates ||
        (!decoded_batch.finished &&
         (decoded_batch.continuation == NULL ||
          decoded_batch.continuation[0] != 7))) {
        return PRDB_SDK_EXECUTOR_INVALID_PAYLOAD;
    }
    *batch = decoded_batch;
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
    if (!valid_mutation_acknowledgement(response, *response_len)) {
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
    uint8_t staged_row_handle[16];
    int32_t status;
    if (relation_handle == NULL || relation_handle[0] != 6 || values == NULL ||
        value_count > PRDB_SDK_BATCH_MAX_CELLS_PER_ROW || request == NULL ||
        request_capacity > PRDB_QUERY_MAX_IPC_REQUEST_BYTES || response == NULL ||
        response_len == NULL || row_handle_out == NULL) {
        return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    }
    if (request_capacity < offset) return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;
    prdb_encode_executor_stage_insert_request(request, relation_handle,
                                              value_count);
    for (uint16_t i = 0; i < value_count; ++i) {
        const prdb_sdk_executor_insert_value_v1 *input = &values[i];
        if (input->struct_size < sizeof(*input) ||
            input->abi_version != PRDB_SDK_EXECUTOR_ABI_V1_VERSION) {
            return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
        }
        if (input->state == PRDB_SDK_EXECUTOR_INSERT_DEFAULT) {
            if (offset + PRDB_MUTATION_CELL_TAG_RECORD_HEADER_BYTES > request_capacity) {
                return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;
            }
            prdb_encode_mutation_cell_tag_record(request + offset, 2);
            offset += PRDB_MUTATION_CELL_TAG_RECORD_HEADER_BYTES;
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
                if (offset + PRDB_MUTATION_CELL_TAG_RECORD_HEADER_BYTES > request_capacity) {
                    return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;
                }
                prdb_encode_mutation_cell_tag_record(request + offset, 1);
                offset += PRDB_MUTATION_CELL_TAG_RECORD_HEADER_BYTES;
            } else {
                uint64_t value_len = input->value.data_len;
                uint64_t value_end = offset + 6 + value_len;
                if (value_len > UINT32_MAX || value_end < offset ||
                    value_end > request_capacity) {
                    return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;
                }
                prdb_encode_mutation_cell_tag_record(request + offset, 0);
                offset += PRDB_MUTATION_CELL_TAG_RECORD_HEADER_BYTES;
                uint8_t value_header[PRDB_SCALAR_VALUE_RECORD_HEADER_BYTES] = {0};
                prdb_encode_scalar_value_record(
                    value_header, (uint8_t)input->value.type_code,
                    (uint32_t)value_len);
                memcpy(request + offset, value_header, sizeof(value_header));
                offset += sizeof(value_header);
                if (!prdb_encode_scalar_value_record_value_bytes(
                        request + offset, (size_t)(request_capacity - offset),
                        input->value.data, (size_t)value_len)) {
                    return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;
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
    if (!prdb_decode_executor_stage_insert_response(
            response, (size_t)*response_len, staged_row_handle) ||
        staged_row_handle[0] != 8) {
        return PRDB_SDK_EXECUTOR_INVALID_PAYLOAD;
    }
    memcpy(row_handle_out, staged_row_handle, sizeof(staged_row_handle));
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
    prdb_encode_executor_stage_update_request(request, row_handle, patch_count);
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
        prdb_encode_patch_ordinal_record(request + offset, patch->ordinal);
        offset += PRDB_PATCH_ORDINAL_RECORD_HEADER_BYTES;
        if (patch->value.state == PRDB_SDK_VALUE_STATE_NULL) {
            prdb_encode_mutation_cell_tag_record(request + offset, 1);
            offset += PRDB_MUTATION_CELL_TAG_RECORD_HEADER_BYTES;
        } else {
            prdb_encode_mutation_cell_tag_record(request + offset, 0);
            offset += PRDB_MUTATION_CELL_TAG_RECORD_HEADER_BYTES;
            uint8_t value_header[PRDB_SCALAR_VALUE_RECORD_HEADER_BYTES] = {0};
            prdb_encode_scalar_value_record(
                value_header, (uint8_t)patch->value.type_code,
                (uint32_t)value_len);
            memcpy(request + offset, value_header, sizeof(value_header));
            offset += sizeof(value_header);
            if (!prdb_encode_scalar_value_record_value_bytes(
                    request + offset, (size_t)(request_capacity - offset),
                    patch->value.data, (size_t)value_len)) {
                return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;
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
    prdb_encode_executor_stage_delete_request(request, row_handle);
    status = invoke(host, PRDB_SDK_EXECUTOR_OP_STAGE_DELETE, request,
                    sizeof(request), response, response_capacity, response_len);
    if (status != PRDB_SDK_EXECUTOR_OK) return status;
    if (!valid_mutation_acknowledgement(response, *response_len)) {
        return PRDB_SDK_EXECUTOR_INVALID_PAYLOAD;
    }
    return PRDB_SDK_EXECUTOR_OK;
}

static int32_t encode_batch_insert_record(
    const uint8_t relation_handle[16],
    const prdb_sdk_executor_insert_value_v1 *values, uint16_t value_count,
    uint8_t *out, uint64_t capacity, uint64_t *length_out) {
    uint64_t offset = PRDB_EXECUTOR_STAGE_INSERT_REQUEST_HEADER_BYTES;
    prdb_encode_executor_stage_insert_request(out, relation_handle, value_count);
    for (uint16_t i = 0; i < value_count; ++i) {
        const prdb_sdk_executor_insert_value_v1 *input = &values[i];
        if (input->struct_size < sizeof(*input) ||
            input->abi_version != PRDB_SDK_EXECUTOR_ABI_V1_VERSION) {
            return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
        }
        if (input->state == PRDB_SDK_EXECUTOR_INSERT_DEFAULT ||
            input->state == PRDB_SDK_EXECUTOR_INSERT_NULL) {
            if (offset + 1 > capacity) return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;
            prdb_encode_mutation_cell_tag_record(out + offset,
                input->state == PRDB_SDK_EXECUTOR_INSERT_DEFAULT ? 2 : 1);
            ++offset;
        } else if (input->state == PRDB_SDK_EXECUTOR_INSERT_VALUE) {
            if (prdb_sdk_value_validate_v1(&input->value) != PRDB_SDK_VALUE_VALID ||
                input->value.state != PRDB_SDK_VALUE_STATE_VALUE) {
                return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
            }
            uint64_t value_len = input->value.data_len;
            uint64_t end = offset + 1 + 5 + value_len;
            if (value_len > UINT32_MAX || end < offset || end > capacity)
                return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;
            prdb_encode_mutation_cell_tag_record(out + offset, 0);
            ++offset;
            uint8_t value_header[PRDB_SCALAR_VALUE_RECORD_HEADER_BYTES] = {0};
            prdb_encode_scalar_value_record(value_header,
                (uint8_t)input->value.type_code, (uint32_t)value_len);
            memcpy(out + offset, value_header, sizeof(value_header));
            offset += sizeof(value_header);
            if (!prdb_encode_scalar_value_record_value_bytes(
                    out + offset, (size_t)(capacity - offset), input->value.data,
                    (size_t)value_len)) return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;
            offset += value_len;
        } else return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    }
    *length_out = offset;
    return PRDB_SDK_EXECUTOR_OK;
}

static int32_t encode_batch_update_record(
    const uint8_t handle[16], const prdb_sdk_executor_update_patch_v1 *patches,
    uint16_t patch_count, uint8_t *out, uint64_t capacity, uint64_t *length_out) {
    uint8_t seen[PRDB_SDK_BATCH_MAX_CELLS_PER_ROW / 8] = {0};
    uint64_t offset = PRDB_EXECUTOR_STAGE_UPDATE_REQUEST_HEADER_BYTES;
    if (handle == NULL || handle[0] != PRDB_SDK_BATCH_ROW_TOKEN_KIND || patches == NULL)
        return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    prdb_encode_executor_stage_update_request(out, handle, patch_count);
    for (uint16_t i = 0; i < patch_count; ++i) {
        const prdb_sdk_executor_update_patch_v1 *patch = &patches[i];
        if (patch->struct_size < sizeof(*patch) ||
            patch->abi_version != PRDB_SDK_EXECUTOR_ABI_V1_VERSION ||
            patch->ordinal >= PRDB_SDK_BATCH_MAX_CELLS_PER_ROW ||
            prdb_sdk_value_validate_v1(&patch->value) != PRDB_SDK_VALUE_VALID ||
            (patch->value.state != PRDB_SDK_VALUE_STATE_NULL &&
             patch->value.state != PRDB_SDK_VALUE_STATE_VALUE))
            return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
        uint32_t byte_index = patch->ordinal / 8;
        uint8_t mask = (uint8_t)(1U << (patch->ordinal % 8));
        if ((seen[byte_index] & mask) != 0) return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
        seen[byte_index] |= mask;
        uint64_t value_len = patch->value.state == PRDB_SDK_VALUE_STATE_VALUE
                                 ? patch->value.data_len : 0;
        uint64_t end = offset + 5 + value_len + (value_len != 0 ||
            patch->value.state == PRDB_SDK_VALUE_STATE_VALUE ? 5 : 0);
        if (value_len > UINT32_MAX || end < offset || end > capacity)
            return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;
        prdb_encode_patch_ordinal_record(out + offset, patch->ordinal);
        offset += PRDB_PATCH_ORDINAL_RECORD_HEADER_BYTES;
        if (patch->value.state == PRDB_SDK_VALUE_STATE_NULL) {
            prdb_encode_mutation_cell_tag_record(out + offset, 1);
            ++offset;
        } else {
            prdb_encode_mutation_cell_tag_record(out + offset, 0);
            ++offset;
            uint8_t value_header[PRDB_SCALAR_VALUE_RECORD_HEADER_BYTES] = {0};
            prdb_encode_scalar_value_record(value_header,
                (uint8_t)patch->value.type_code, (uint32_t)value_len);
            memcpy(out + offset, value_header, sizeof(value_header));
            offset += sizeof(value_header);
            if (!prdb_encode_scalar_value_record_value_bytes(
                    out + offset, (size_t)(capacity - offset), patch->value.data,
                    (size_t)value_len)) return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;
            offset += value_len;
        }
    }
    *length_out = offset;
    return PRDB_SDK_EXECUTOR_OK;
}

static int32_t mutation_batch_invoke(
    const prdb_host_api_v2 *host, uint32_t operation, const uint8_t *request,
    uint64_t request_len, uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len) {
    int32_t status = invoke(host, operation, request, request_len, response,
                            response_capacity, response_len);
    if (status != PRDB_SDK_EXECUTOR_OK) return status;
    if (!valid_mutation_acknowledgement(response, *response_len))
        return PRDB_SDK_EXECUTOR_INVALID_PAYLOAD;
    return PRDB_SDK_EXECUTOR_OK;
}

int32_t prdb_sdk_executor_stage_inserts_v1(
    const prdb_host_api_v2 *host, const uint8_t relation_handle[16],
    const prdb_sdk_executor_insert_value_v1 *values, uint16_t row_count,
    uint16_t value_count, uint8_t *request, uint64_t request_capacity,
    uint8_t *response, uint64_t response_capacity, uint64_t *response_len,
    uint8_t *row_handles_out, uint64_t row_handles_capacity) {
    uint64_t offset = 4;
    if (relation_handle == NULL || relation_handle[0] != 6 || values == NULL ||
        row_count == 0 || row_count > 4096 || value_count == 0 ||
        request == NULL || request_capacity > PRDB_QUERY_MAX_IPC_REQUEST_BYTES ||
        response == NULL || response_len == NULL || row_handles_out == NULL ||
        row_handles_capacity < (uint64_t)row_count * 16 || request_capacity < 4)
        return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    request[0] = 0; request[1] = 1;
    request[2] = (uint8_t)(row_count >> 8); request[3] = (uint8_t)row_count;
    for (uint16_t row = 0; row < row_count; ++row) {
        if (offset + 4 > request_capacity) return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;
        uint64_t body_len = 0;
        int32_t status = encode_batch_insert_record(
            relation_handle, values + (uint64_t)row * value_count, value_count, request + offset + 4,
            request_capacity - offset - 4, &body_len);
        if (status != PRDB_SDK_EXECUTOR_OK) return status;
        if (body_len > UINT32_MAX || offset + 4 + body_len > PRDB_QUERY_MAX_IPC_REQUEST_BYTES)
            return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;
        request[offset] = (uint8_t)(body_len >> 24);
        request[offset + 1] = (uint8_t)(body_len >> 16);
        request[offset + 2] = (uint8_t)(body_len >> 8);
        request[offset + 3] = (uint8_t)body_len;
        offset += 4 + body_len;
    }
    int32_t status = invoke(host, PRDB_SDK_EXECUTOR_OP_STAGE_INSERTS, request, offset,
        response, response_capacity, response_len);
    if (status != PRDB_SDK_EXECUTOR_OK) return status;
    if (*response_len != 4 + (uint64_t)row_count * 16 || response[0] != 0 ||
        response[1] != 1 || response[2] != request[2] || response[3] != request[3])
        return PRDB_SDK_EXECUTOR_INVALID_PAYLOAD;
    for (uint16_t row = 0; row < row_count; ++row) {
        const uint8_t *handle = response + 4 + (uint64_t)row * 16;
        if (handle[0] != PRDB_SDK_BATCH_ROW_TOKEN_KIND)
            return PRDB_SDK_EXECUTOR_INVALID_PAYLOAD;
        memcpy(row_handles_out + (uint64_t)row * 16, handle, 16);
    }
    return PRDB_SDK_EXECUTOR_OK;
}

int32_t prdb_sdk_executor_stage_updates_v1(
    const prdb_host_api_v2 *host, const uint8_t *row_handles,
    uint16_t row_count, const prdb_sdk_executor_update_patch_v1 *patches,
    uint16_t patch_count, uint8_t *request, uint64_t request_capacity,
    uint8_t *response, uint64_t response_capacity, uint64_t *response_len) {
    uint64_t offset = 4;
    if (row_handles == NULL || row_count == 0 || row_count > 4096 ||
        patches == NULL || patch_count == 0 || request == NULL ||
        request_capacity > PRDB_QUERY_MAX_IPC_REQUEST_BYTES || request_capacity < 4)
        return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    request[0] = 0; request[1] = 1;
    request[2] = (uint8_t)(row_count >> 8); request[3] = (uint8_t)row_count;
    for (uint16_t row = 0; row < row_count; ++row) {
        if (offset + 4 > request_capacity) return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;
        uint64_t body_len = 0;
        int32_t status = encode_batch_update_record(row_handles + (uint64_t)row * 16,
            patches, patch_count, request + offset + 4, request_capacity - offset - 4,
            &body_len);
        if (status != PRDB_SDK_EXECUTOR_OK) return status;
        if (body_len > UINT32_MAX || offset + 4 + body_len > PRDB_QUERY_MAX_IPC_REQUEST_BYTES)
            return PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL;
        request[offset] = (uint8_t)(body_len >> 24);
        request[offset + 1] = (uint8_t)(body_len >> 16);
        request[offset + 2] = (uint8_t)(body_len >> 8);
        request[offset + 3] = (uint8_t)body_len;
        offset += 4 + body_len;
    }
    return mutation_batch_invoke(host, PRDB_SDK_EXECUTOR_OP_STAGE_UPDATES,
        request, offset, response, response_capacity, response_len);
}

int32_t prdb_sdk_executor_stage_deletes_v1(
    const prdb_host_api_v2 *host, const uint8_t *row_handles,
    uint16_t row_count, uint8_t *request, uint64_t request_capacity,
    uint8_t *response, uint64_t response_capacity, uint64_t *response_len) {
    uint64_t offset = 4;
    if (row_handles == NULL || row_count == 0 || row_count > 4096 || request == NULL ||
        request_capacity > PRDB_QUERY_MAX_IPC_REQUEST_BYTES || request_capacity < 4)
        return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    request[0] = 0; request[1] = 1;
    request[2] = (uint8_t)(row_count >> 8); request[3] = (uint8_t)row_count;
    for (uint16_t row = 0; row < row_count; ++row) {
        const uint8_t *handle = row_handles + (uint64_t)row * 16;
        if (handle[0] != PRDB_SDK_BATCH_ROW_TOKEN_KIND || offset + 22 > request_capacity)
            return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
        request[offset] = 0; request[offset + 1] = 0; request[offset + 2] = 0; request[offset + 3] = 18;
        request[offset + 4] = 0; request[offset + 5] = 1;
        memcpy(request + offset + 6, handle, 16);
        offset += 22;
    }
    return mutation_batch_invoke(host, PRDB_SDK_EXECUTOR_OP_STAGE_DELETES,
        request, offset, response, response_capacity, response_len);
}

int32_t prdb_sdk_executor_check_cancelled_v1(
    const prdb_host_api_v2 *host, int32_t *cancelled_out) {
    const uint8_t empty_request = 0;
    uint8_t response[PRDB_EXECUTOR_CANCELLATION_CHECK_RESPONSE_HEADER_BYTES];
    uint8_t cancelled;
    uint64_t response_len = 0;
    int32_t status;
    if (cancelled_out == NULL) return PRDB_SDK_EXECUTOR_INVALID_ARGUMENT;
    status = invoke(host, PRDB_SDK_EXECUTOR_OP_CHECK_CANCELLATION,
                    &empty_request, 0, response, sizeof(response), &response_len);
    if (status != PRDB_SDK_EXECUTOR_OK) return status;
    if (!prdb_decode_executor_cancellation_check_response(
            response, (size_t)response_len, &cancelled) || cancelled > 1) {
        return PRDB_SDK_EXECUTOR_INVALID_PAYLOAD;
    }
    *cancelled_out = (int32_t)cancelled;
    return PRDB_SDK_EXECUTOR_OK;
}
