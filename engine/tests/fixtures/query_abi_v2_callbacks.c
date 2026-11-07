#include "prdb_query_abi_v2.h"

#include <assert.h>
#include <string.h>

const prdb_query_descriptor_v2_record prdb_query_descriptor_v2 = {
    sizeof(prdb_query_descriptor_v2_record),
    PRDB_QUERY_ABI_V2_VERSION,
    sizeof(prdb_host_api_v2),
    sizeof(prdb_invocation_v2),
};

typedef struct test_context {
    uint8_t result[PRDB_QUERY_MAX_RESULT_CHUNK_BYTES];
    uint64_t result_len;
} test_context;

static int32_t test_executor_request(
    void *opaque, uint16_t operation, const uint8_t *request,
    uint64_t request_len, uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len) {
    static const uint8_t expected_request[] = {0x41, 0x42};
    static const uint8_t expected_response[] = {0x43, 0x44, 0x45};

    (void)opaque;
    assert(operation == 0x1101);
    assert(request_len == sizeof(expected_request));
    assert(memcmp(request, expected_request, sizeof(expected_request)) == 0);
    if (response_capacity < sizeof(expected_response)) {
        *response_len = sizeof(expected_response);
        return PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL;
    }
    memcpy(response, expected_response, sizeof(expected_response));
    *response_len = sizeof(expected_response);
    return PRDB_QUERY_CALLBACK_OK;
}

static int32_t test_write_result(void *opaque, const uint8_t *bytes,
                                 uint64_t bytes_len) {
    test_context *context = (test_context *)opaque;

    assert(bytes_len <= sizeof(context->result));
    assert(bytes != 0 || bytes_len == 0);
    if (bytes_len != 0) {
        memcpy(context->result, bytes, (size_t)bytes_len);
    }
    context->result_len = bytes_len;
    return PRDB_QUERY_CALLBACK_OK;
}

int32_t prdb_query_main_v2(const prdb_host_api_v2 *host,
                           const prdb_invocation_v2 *invocation) {
    static const uint8_t expected_parameters[] = {0x61, 0x62, 0x63};
    static const uint8_t request[] = {0x41, 0x42};
    static const uint8_t expected_response[] = {0x43, 0x44, 0x45};
    static const uint8_t output[] = {0x31, 0x32};
    uint8_t response[sizeof(expected_response)] = {0x7f, 0x7f, 0x7f};
    uint64_t response_len = 0;

    assert(host != 0);
    assert(host->struct_size == sizeof(*host));
    assert(host->abi_version == PRDB_QUERY_ABI_V2_VERSION);
    assert(invocation != 0);
    assert(invocation->struct_size == sizeof(*invocation));
    assert(invocation->abi_version == PRDB_QUERY_ABI_V2_VERSION);
    assert(invocation->parameters_len == sizeof(expected_parameters));
    assert(invocation->parameters != 0);
    assert(memcmp(invocation->parameters, expected_parameters,
                  sizeof(expected_parameters)) == 0);

    assert(host->executor_request(host->context, 0x1101, request,
                                  sizeof(request), response,
                                  sizeof(response) - 1, &response_len) ==
           PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL);
    assert(response_len == sizeof(expected_response));
    assert(response[0] == 0x7f && response[1] == 0x7f && response[2] == 0x7f);
    assert(host->executor_request(host->context, 0x1101, request,
                                  sizeof(request), response, sizeof(response),
                                  &response_len) == PRDB_QUERY_CALLBACK_OK);
    assert(response_len == sizeof(expected_response));
    assert(memcmp(response, expected_response, sizeof(expected_response)) == 0);
    assert(host->write_result(host->context, output, sizeof(output)) ==
           PRDB_QUERY_CALLBACK_OK);
    return 0;
}

int main(void) {
    static const uint8_t parameters[] = {0x61, 0x62, 0x63};
    static const uint8_t expected_output[] = {0x31, 0x32};
    test_context context = {{0}, 0};
    const prdb_host_api_v2 host = {
        sizeof(prdb_host_api_v2),
        PRDB_QUERY_ABI_V2_VERSION,
        &context,
        test_executor_request,
        test_executor_request,
        test_write_result,
    };
    const prdb_invocation_v2 invocation = {
        sizeof(prdb_invocation_v2),
        PRDB_QUERY_ABI_V2_VERSION,
        parameters,
        sizeof(parameters),
    };

    assert(prdb_query_descriptor_v2.struct_size ==
           sizeof(prdb_query_descriptor_v2));
    assert(prdb_query_descriptor_v2.abi_version ==
           PRDB_QUERY_ABI_V2_VERSION);
    assert(prdb_query_descriptor_v2.host_api_size == sizeof(host));
    assert(prdb_query_descriptor_v2.invocation_size == sizeof(invocation));
    assert(prdb_query_main_v2(&host, &invocation) == 0);
    assert(context.result_len == sizeof(expected_output));
    assert(memcmp(context.result, expected_output, sizeof(expected_output)) ==
           0);
    return 0;
}
