#include "prdb_sdk_owned_batch_v1.h"

#include <stdlib.h>
#include <string.h>

static int empty_wrapper(const prdb_sdk_owned_batch_v1 *owned) {
    return (owned->struct_size == 0 && owned->abi_version == 0 &&
            owned->payload == NULL && owned->payload_len == 0) ||
           (owned->struct_size == sizeof(*owned) &&
            owned->abi_version == PRDB_SDK_OWNED_BATCH_ABI_V1_VERSION &&
            owned->payload == NULL && owned->payload_len == 0);
}

static int valid_active_wrapper(const prdb_sdk_owned_batch_v1 *owned) {
    return owned->struct_size >= sizeof(*owned) &&
           owned->abi_version == PRDB_SDK_OWNED_BATCH_ABI_V1_VERSION &&
           owned->payload != NULL && owned->payload_len >= 5 &&
           owned->payload_len <= PRDB_SDK_BATCH_MAX_BYTES;
}

int32_t prdb_sdk_owned_batch_copy_v1(prdb_sdk_owned_batch_v1 *out,
                                     uint32_t out_capacity,
                                     const uint8_t *payload,
                                     uint64_t payload_len) {
    uint8_t *copy;
    prdb_sdk_batch_v1 decoded;
    int32_t status;

    if (out == NULL || out_capacity < sizeof(*out) || !empty_wrapper(out)) {
        return PRDB_SDK_BATCH_INVALID_ARGUMENT;
    }
    if (payload == NULL || payload_len < 5 ||
        payload_len > PRDB_SDK_BATCH_MAX_BYTES) {
        return PRDB_SDK_BATCH_INVALID_PAYLOAD;
    }
    copy = (uint8_t *)malloc((size_t)payload_len);
    if (copy == NULL) {
        return PRDB_SDK_OWNED_BATCH_OUT_OF_MEMORY;
    }
    memcpy(copy, payload, (size_t)payload_len);
    status = prdb_sdk_batch_open_v1(&decoded, sizeof(decoded), copy,
                                    payload_len);
    if (status != PRDB_SDK_BATCH_OK) {
        free(copy);
        return status;
    }

    out->struct_size = sizeof(*out);
    out->abi_version = PRDB_SDK_OWNED_BATCH_ABI_V1_VERSION;
    out->payload = copy;
    out->payload_len = payload_len;
    return PRDB_SDK_BATCH_OK;
}

int32_t prdb_sdk_owned_batch_open_v1(const prdb_sdk_owned_batch_v1 *owned,
                                     prdb_sdk_batch_v1 *out,
                                     uint32_t out_capacity) {
    if (owned == NULL || !valid_active_wrapper(owned)) {
        return PRDB_SDK_BATCH_INVALID_ARGUMENT;
    }
    return prdb_sdk_batch_open_v1(out, out_capacity, owned->payload,
                                  owned->payload_len);
}

int32_t prdb_sdk_owned_batch_release_v1(prdb_sdk_owned_batch_v1 *owned) {
    if (owned == NULL) {
        return PRDB_SDK_BATCH_INVALID_ARGUMENT;
    }
    if (empty_wrapper(owned)) {
        owned->struct_size = sizeof(*owned);
        owned->abi_version = PRDB_SDK_OWNED_BATCH_ABI_V1_VERSION;
        owned->payload = NULL;
        owned->payload_len = 0;
        return PRDB_SDK_BATCH_OK;
    }
    if (!valid_active_wrapper(owned)) {
        return PRDB_SDK_BATCH_INVALID_ARGUMENT;
    }
    free((void *)owned->payload);
    owned->struct_size = sizeof(*owned);
    owned->abi_version = PRDB_SDK_OWNED_BATCH_ABI_V1_VERSION;
    owned->payload = NULL;
    owned->payload_len = 0;
    return PRDB_SDK_BATCH_OK;
}
