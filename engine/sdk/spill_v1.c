#include "prdb_sdk_spill_v1.h"

#include <stdlib.h>
#include <string.h>

static int32_t spill_call(const prdb_host_api_v1 *host, uint16_t operation,
                          const uint8_t *request, uint64_t request_len,
                          uint8_t *response, uint64_t response_capacity,
                          uint64_t *response_len) {
    if (host == NULL || host->spill_request == NULL || response_len == NULL ||
        (request_len != 0 && request == NULL) ||
        (response_capacity != 0 && response == NULL)) {
        return -1;
    }
    *response_len = 0;
    return host->spill_request(host->context, operation, request,
                               request_len, response, response_capacity,
                               response_len);
}

int32_t prdb_sdk_spill_create_v1(const prdb_host_api_v1 *host,
                                 prdb_sdk_spill_stream_v1 *stream) {
    uint64_t response_len = 0;
    if (stream == NULL) return -1;
    memset(stream, 0, sizeof(*stream));
    int32_t status = spill_call(host, PRDB_SPILL_OP_CREATE, NULL, 0,
                                stream->handle, sizeof(stream->handle),
                                &response_len);
    if (status != 0) return status;
    if (response_len != sizeof(stream->handle)) {
        memset(stream, 0, sizeof(*stream));
        return -1;
    }
    stream->valid = 1;
    return 0;
}

int32_t prdb_sdk_spill_append_v1(const prdb_host_api_v1 *host,
                                 const prdb_sdk_spill_stream_v1 *stream,
                                 const uint8_t *bytes, uint64_t bytes_len) {
    if (stream == NULL || stream->valid != 1 ||
        (bytes_len != 0 && bytes == NULL) || bytes_len > UINT32_MAX - 16u ||
        bytes_len > SIZE_MAX - 16u ||
        bytes_len > PRDB_SDK_SPILL_MAX_IO_BYTES - 16u) return -1;
    uint64_t request_len = 16 + bytes_len;
    uint8_t *request = (uint8_t *)malloc((size_t)request_len);
    if (request == NULL) return -1;
    memcpy(request, stream->handle, 16);
    if (bytes_len != 0) memcpy(request + 16, bytes, (size_t)bytes_len);
    uint64_t response_len = 0;
    int32_t status = spill_call(host, PRDB_SPILL_OP_APPEND, request, request_len,
                                NULL, 0, &response_len);
    free(request);
    if (status == 0 && response_len != 0) return -1;
    return status;
}

int32_t prdb_sdk_spill_seal_v1(const prdb_host_api_v1 *host,
                               const prdb_sdk_spill_stream_v1 *stream) {
    uint64_t response_len = 0;
    if (stream == NULL || stream->valid != 1) return -1;
    int32_t status = spill_call(host, PRDB_SPILL_OP_SEAL, stream->handle, 16,
                                NULL, 0, &response_len);
    if (status == 0 && response_len != 0) return -1;
    return status;
}

int32_t prdb_sdk_spill_read_at_v1(const prdb_host_api_v1 *host,
                                  const prdb_sdk_spill_stream_v1 *stream,
                                  uint64_t offset, uint8_t *bytes,
                                  uint32_t capacity, uint64_t *bytes_len) {
    uint8_t request[28];
    uint64_t response_len = 0;
    if (stream == NULL || stream->valid != 1 || bytes_len == NULL ||
        (capacity != 0 && bytes == NULL) ||
        capacity > PRDB_SDK_SPILL_MAX_IO_BYTES) return -1;
    memcpy(request, stream->handle, 16);
    for (unsigned i = 0; i < 8; ++i)
        request[16 + i] = (uint8_t)(offset >> (56 - i * 8));
    request[24] = (uint8_t)(capacity >> 24);
    request[25] = (uint8_t)(capacity >> 16);
    request[26] = (uint8_t)(capacity >> 8);
    request[27] = (uint8_t)capacity;
    int32_t status = spill_call(host, PRDB_SPILL_OP_READ_AT, request,
                                sizeof(request), bytes, capacity, &response_len);
    if (status != 0) return status;
    if (response_len > capacity) return -1;
    *bytes_len = response_len;
    return 0;
}

int32_t prdb_sdk_spill_destroy_v1(const prdb_host_api_v1 *host,
                                  prdb_sdk_spill_stream_v1 *stream) {
    uint64_t response_len = 0;
    if (stream == NULL || stream->valid != 1) return -1;
    int32_t status = spill_call(host, PRDB_SPILL_OP_DESTROY, stream->handle, 16,
                                NULL, 0, &response_len);
    if (status == 0 && response_len == 0) memset(stream, 0, sizeof(*stream));
    else if (status == 0) return -1;
    return status;
}
