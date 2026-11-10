#include "prdb_query_abi_v4.h"
#include "prdb_query_abi_layout_assertions.h"

#include <assert.h>
#include <string.h>

const prdb_query_descriptor_v4_record prdb_query_descriptor_v4 = {
    sizeof(prdb_query_descriptor_v4_record),
    PRDB_QUERY_ABI_V4_VERSION,
    sizeof(prdb_host_api_v4),
    sizeof(prdb_invocation_v4),
};

static int32_t optimizer_request(void *context, const uint8_t *request,
                                 uint64_t request_len, uint8_t *response,
                                 uint64_t response_capacity,
                                 uint64_t *response_len) {
    static const uint8_t expected_request[] = {0x41, 0x42};
    static const uint8_t expected_response[] = {0x51, 0x52, 0x53};

    (void)context;
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

int32_t prdb_query_main_v4(const prdb_host_api_v4 *host,
                           const prdb_invocation_v4 *invocation) {
    static const uint8_t input[] = {0x41, 0x42};
    static const uint8_t expected_output[] = {0x51, 0x52, 0x53};
    uint8_t output[sizeof(expected_output)] = {0};
    uint64_t output_len = 0;

    assert(host != 0);
    assert(host->v3.struct_size == sizeof(prdb_host_api_v3));
    assert(host->v3.abi_version == PRDB_QUERY_ABI_V3_VERSION);
    assert(host->v3.executor_request != 0);
    assert(host->v3.planner_request != 0);
    assert(host->v3.statement_failure != 0);
    assert(host->optimizer_request != 0);
    assert(invocation != 0);
    assert(invocation->abi_version == PRDB_QUERY_ABI_V4_VERSION);
    assert(host->optimizer_request(host->v3.context, input, sizeof(input),
                                  output, sizeof(output), &output_len) ==
           PRDB_QUERY_CALLBACK_OK);
    assert(output_len == sizeof(expected_output));
    assert(memcmp(output, expected_output, sizeof(expected_output)) == 0);
    return 0;
}

int main(void) {
    const prdb_query_descriptor_v4_record *descriptor =
        &prdb_query_descriptor_v4;
    const prdb_host_api_v4 host = {
        {
            sizeof(prdb_host_api_v3),
            PRDB_QUERY_ABI_V3_VERSION,
            0,
            request,
            request,
            write_result,
            statement_failure,
        },
        optimizer_request,
    };
    const prdb_invocation_v4 invocation = {
        sizeof(prdb_invocation_v4),
        PRDB_QUERY_ABI_V4_VERSION,
        0,
        0,
    };

    assert(descriptor->struct_size == sizeof(*descriptor));
    assert(descriptor->abi_version == PRDB_QUERY_ABI_V4_VERSION);
    assert(descriptor->host_api_size == sizeof(host));
    assert(descriptor->invocation_size == sizeof(invocation));
    assert(prdb_query_main_v4(&host, &invocation) == 0);
    return 0;
}
