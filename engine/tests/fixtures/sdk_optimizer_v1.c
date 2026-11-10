#include "prdb_sdk_optimizer_v1.h"

#include <assert.h>
#include <string.h>

typedef struct mock_optimizer {
    const uint8_t *payload;
    uint64_t payload_len;
    int32_t callback_status;
} mock_optimizer;

static int32_t mock_request(void *opaque, const uint8_t *request,
                            uint64_t request_len, uint8_t *response,
                            uint64_t response_capacity,
                            uint64_t *response_len) {
    mock_optimizer *mock = (mock_optimizer *)opaque;
    static const uint8_t expected_request[] = {0x11, 0x22, 0x33};

    assert(request_len == sizeof(expected_request));
    assert(memcmp(request, expected_request, sizeof(expected_request)) == 0);
    if (mock->callback_status != PRDB_QUERY_CALLBACK_OK) {
        *response_len = mock->payload_len;
        return mock->callback_status;
    }
    if (response_capacity < mock->payload_len) {
        *response_len = mock->payload_len;
        return PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL;
    }
    memcpy(response, mock->payload, (size_t)mock->payload_len);
    *response_len = mock->payload_len;
    return PRDB_QUERY_CALLBACK_OK;
}

static prdb_host_api_v4 host_for(mock_optimizer *mock) {
    prdb_host_api_v4 host = {
        {
            sizeof(prdb_host_api_v3),
            PRDB_QUERY_ABI_V3_VERSION,
            mock,
            NULL,
            NULL,
            NULL,
            NULL,
        },
        mock_request,
    };
    return host;
}

int main(void) {
    static const uint8_t request[] = {0x11, 0x22, 0x33};
    static const uint8_t success[] = {0, 1, 0, 0, 0, 0, 3, 0xaa, 0xbb, 0xcc};
    static const uint8_t unavailable[] = {0, 1, 2};
    static const uint8_t malformed[] = {0, 1, 0, 0, 0, 0, 2, 0xaa};
    mock_optimizer mock = {success, sizeof(success),
                           PRDB_QUERY_CALLBACK_OK};
    prdb_host_api_v4 host = host_for(&mock);
    uint8_t response[sizeof(success)] = {0};
    uint64_t response_len = 0;
    prdb_sdk_optimizer_response_v1 decoded = {0};

    assert(prdb_sdk_optimizer_submit_select_v1(
               &host, request, sizeof(request), response, sizeof(response),
               &response_len, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_RESULT_OK);
    assert(response_len == sizeof(success));
    assert(decoded.status == PRDB_SDK_OPTIMIZER_STATUS_SUCCESS);
    assert(decoded.plan_len == 3);
    assert(decoded.plan == response + 7);
    assert(memcmp(decoded.plan, "\xaa\xbb\xcc", 3) == 0);

    mock.payload = unavailable;
    mock.payload_len = sizeof(unavailable);
    memset(response, 0, sizeof(response));
    assert(prdb_sdk_optimizer_submit_select_v1(
               &host, request, sizeof(request), response, sizeof(response),
               &response_len, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_RESULT_OK);
    assert(decoded.status == PRDB_SDK_OPTIMIZER_STATUS_UNAVAILABLE);
    assert(decoded.plan == NULL && decoded.plan_len == 0);

    mock.payload = success;
    mock.payload_len = sizeof(success);
    assert(prdb_sdk_optimizer_submit_select_v1(
               &host, request, sizeof(request), response, 2, &response_len,
               &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_RESULT_RESPONSE_TOO_SMALL);
    assert(response_len == sizeof(success));

    mock.payload_len = PRDB_QUERY_MAX_IPC_REQUEST_BYTES + 1;
    assert(prdb_sdk_optimizer_submit_select_v1(
               &host, request, sizeof(request), response, 2, &response_len,
               &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE);

    mock.callback_status = PRDB_QUERY_CALLBACK_IO_ERROR;
    assert(prdb_sdk_optimizer_submit_select_v1(
               &host, request, sizeof(request), response, sizeof(response),
               &response_len, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_RESULT_CALLBACK_ERROR);

    assert(prdb_sdk_optimizer_decode_response_v1(
               malformed, sizeof(malformed), &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE);
    return 0;
}
