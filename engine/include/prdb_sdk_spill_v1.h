#ifndef PRDB_SDK_SPILL_V1_H
#define PRDB_SDK_SPILL_V1_H

#include "prdb_query_abi_v1.h"

#define PRDB_SDK_SPILL_MAX_IO_BYTES UINT32_C(1048576)

typedef struct prdb_sdk_spill_stream_v1 {
    uint8_t handle[16];
    uint8_t valid;
} prdb_sdk_spill_stream_v1;

/* All operations use the invocation-scoped opaque host callback. The SDK
 * never exposes a path or descriptor to query code. */
int32_t prdb_sdk_spill_create_v1(const prdb_host_api_v1 *host,
                                 prdb_sdk_spill_stream_v1 *stream);
int32_t prdb_sdk_spill_append_v1(const prdb_host_api_v1 *host,
                                 const prdb_sdk_spill_stream_v1 *stream,
                                 const uint8_t *bytes, uint64_t bytes_len);
int32_t prdb_sdk_spill_seal_v1(const prdb_host_api_v1 *host,
                               const prdb_sdk_spill_stream_v1 *stream);
int32_t prdb_sdk_spill_read_at_v1(const prdb_host_api_v1 *host,
                                  const prdb_sdk_spill_stream_v1 *stream,
                                  uint64_t offset, uint8_t *bytes,
                                  uint32_t capacity, uint64_t *bytes_len);
int32_t prdb_sdk_spill_destroy_v1(const prdb_host_api_v1 *host,
                                  prdb_sdk_spill_stream_v1 *stream);

#endif
