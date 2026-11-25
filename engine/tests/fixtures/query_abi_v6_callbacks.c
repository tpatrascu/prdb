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

static int32_t spill_noop(void *context, uint16_t operation,
                          const uint8_t *request, uint64_t request_len,
                          uint8_t *response, uint64_t response_capacity,
                          uint64_t *response_len) {
    (void)context; (void)operation; (void)request; (void)request_len;
    (void)response; (void)response_capacity;
    *response_len = 0;
    return 0;
}

int32_t prdb_query_main_v1(const prdb_host_api_v1 *host,
                           const prdb_invocation_v1 *invocation) {
    if (host == NULL || invocation == NULL || host->spill_request == NULL ||
        invocation->abi_version != PRDB_QUERY_ABI_VERSION) return -1;
    return 0;
}

int main(void) {
    assert(prdb_query_descriptor_v1.abi_version == PRDB_QUERY_ABI_VERSION);
    assert(prdb_query_descriptor_v1.host_api_size == sizeof(prdb_host_api_v1));
    assert(prdb_query_descriptor_v1.invocation_size == sizeof(prdb_invocation_v1));
    prdb_host_api_v1 host = {0};
    prdb_invocation_v1 invocation = {0};
    host.spill_request = spill_noop;
    invocation.abi_version = PRDB_QUERY_ABI_VERSION;
    assert(prdb_query_main_v1(&host, &invocation) == 0);
    return 0;
}
