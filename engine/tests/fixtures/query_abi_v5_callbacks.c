#include "prdb_query_abi_v1.h"
#include "prdb_query_abi_layout_assertions.h"

#include <assert.h>
#include <string.h>

const prdb_query_descriptor_v1_record prdb_query_descriptor_v1 = {
    sizeof(prdb_query_descriptor_v1_record),
    PRDB_QUERY_ABI_VERSION,
    sizeof(prdb_host_api_v1),
    sizeof(prdb_invocation_v1),
};

static int32_t optimizer_request_v4(void *context, const uint8_t *request,
                                    uint64_t request_len, uint8_t *response,
                                    uint64_t response_capacity,
                                    uint64_t *response_len) {
    (void)context;
    (void)request;
    (void)request_len;
    (void)response;
    (void)response_capacity;
    (void)response_len;
    return PRDB_QUERY_CALLBACK_INTERNAL_ERROR;
}

static int32_t optimizer_request_v5(void *context, uint16_t operation,
                                    const uint8_t *request,
                                    uint64_t request_len, uint8_t *response,
                                    uint64_t response_capacity,
                                    uint64_t *response_len) {
    static const uint8_t expected[] = {0x41, 0x42};
    (void)context;
    assert(operation == PRDB_IPC_OP_OPTIMIZE_DELETE);
    assert(request_len == sizeof(expected));
    assert(memcmp(request, expected, sizeof(expected)) == 0);
    if (response_capacity < sizeof(expected)) {
        *response_len = sizeof(expected);
        return PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL;
    }
    memcpy(response, expected, sizeof(expected));
    *response_len = sizeof(expected);
    return PRDB_QUERY_CALLBACK_OK;
}

static int32_t request(void *context, uint16_t operation, const uint8_t *input,
                       uint64_t input_len, uint8_t *output,
                       uint64_t output_capacity, uint64_t *output_len) {
    (void)context;
    (void)operation;
    (void)input;
    (void)input_len;
    (void)output;
    (void)output_capacity;
    (void)output_len;
    return PRDB_QUERY_CALLBACK_OK;
}

static int32_t statement_failure(void *context, uint32_t failure_code) {
    (void)context;
    (void)failure_code;
    return PRDB_QUERY_CALLBACK_OK;
}

static int32_t write_result(void *context, const uint8_t *bytes,
                            uint64_t bytes_len) {
    (void)context;
    (void)bytes;
    (void)bytes_len;
    return PRDB_QUERY_CALLBACK_OK;
}

int32_t prdb_query_main_v1(const prdb_host_api_v1 *host,
                           const prdb_invocation_v1 *invocation) {
    static const uint8_t input[] = {0x41, 0x42};
    uint8_t output[sizeof(input)] = {0};
    uint64_t output_len = 0;
    assert(host != NULL && invocation != NULL);
    assert(host->abi_version == PRDB_QUERY_ABI_VERSION);
    assert(host->optimizer_request != NULL);
    assert(host->optimizer_request != NULL);
    assert(invocation->abi_version == PRDB_QUERY_ABI_VERSION);
    assert(host->optimizer_request(host->context,
                                   PRDB_IPC_OP_OPTIMIZE_DELETE, input,
                                   sizeof(input), output, sizeof(output),
                                   &output_len) == PRDB_QUERY_CALLBACK_OK);
    assert(output_len == sizeof(input));
    assert(memcmp(output, input, sizeof(input)) == 0);
    return 0;
}

int main(void) {
    const prdb_host_api_v1 host = {
        sizeof(prdb_host_api_v1), PRDB_QUERY_ABI_VERSION, NULL,
        request, request, write_result, statement_failure,
        optimizer_request_v5, NULL,
    };
    const prdb_invocation_v1 invocation = {
        sizeof(prdb_invocation_v1), PRDB_QUERY_ABI_VERSION, NULL, 0,
    };
    assert(prdb_query_descriptor_v1.abi_version == PRDB_QUERY_ABI_VERSION);
    assert(prdb_query_descriptor_v1.host_api_size == sizeof(host));
    assert(prdb_query_descriptor_v1.invocation_size == sizeof(invocation));
    assert(prdb_query_main_v1(&host, &invocation) == 0);
    return 0;
}
