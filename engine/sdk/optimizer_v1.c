#include "prdb_sdk_optimizer_v1.h"

#include <stddef.h>
#include <stdint.h>

#define OPTIMIZER_REQUEST_MAX_BYTES UINT64_C(65536)

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
