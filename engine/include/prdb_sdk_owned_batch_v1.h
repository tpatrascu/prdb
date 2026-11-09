#ifndef PRDB_SDK_OWNED_BATCH_V1_H
#define PRDB_SDK_OWNED_BATCH_V1_H

#include "prdb_sdk_batch_v1.h"

#define PRDB_SDK_OWNED_BATCH_ABI_V1_VERSION UINT32_C(1)
#define PRDB_SDK_OWNED_BATCH_OUT_OF_MEMORY INT32_C(5)

/* Zero-initialize before first use. The payload is an SDK-owned immutable
 * copy. Every batch, row, cell, and value view opened from it becomes invalid
 * when release succeeds. Do not change the fields. */
typedef struct prdb_sdk_owned_batch_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    const uint8_t *payload;
    uint64_t payload_len;
} prdb_sdk_owned_batch_v1;

/* Copies and validates one complete executor next_batch response. The input
 * remains caller-owned. An active output is never overwritten. */
int32_t prdb_sdk_owned_batch_copy_v1(prdb_sdk_owned_batch_v1 *out,
                                     uint32_t out_capacity,
                                     const uint8_t *payload,
                                     uint64_t payload_len);

/* Opens a borrowed view into the SDK-owned copy. */
int32_t prdb_sdk_owned_batch_open_v1(const prdb_sdk_owned_batch_v1 *owned,
                                     prdb_sdk_batch_v1 *out,
                                     uint32_t out_capacity);

/* Frees the SDK-owned copy and clears it. Releasing an empty initialized or
 * zero-initialized wrapper is safe and leaves it ready for another copy. */
int32_t prdb_sdk_owned_batch_release_v1(prdb_sdk_owned_batch_v1 *owned);

#endif /* PRDB_SDK_OWNED_BATCH_V1_H */
