#include "prdb_sdk_scope_v1.h"

#include <stddef.h>
#include <string.h>

#define PSC1_HEADER_BYTES ((size_t)6)
#define PSC1_MAX_ENCODED_BYTES ((size_t)6806)

static int id_is_zero(const uint8_t id[16]) {
    uint8_t aggregate = 0;
    for (size_t i = 0; i < 16; ++i) {
        aggregate |= id[i];
    }
    return aggregate == 0;
}

static int bytes_are_zero(const uint8_t *bytes, size_t length) {
    uint8_t aggregate = 0;
    for (size_t i = 0; i < length; ++i) {
        aggregate |= bytes[i];
    }
    return aggregate == 0;
}

static size_t encoded_record_bytes(
    const prdb_sdk_scope_capability_v1 *capability) {
    return capability->target_kind == PRDB_SDK_SCOPE_TARGET_COLUMN ? 34 : 18;
}

static int compare_capabilities(
    const prdb_sdk_scope_capability_v1 *left,
    const prdb_sdk_scope_capability_v1 *right) {
    if (left->operation != right->operation) {
        return left->operation < right->operation ? -1 : 1;
    }
    if (left->target_kind != right->target_kind) {
        return left->target_kind < right->target_kind ? -1 : 1;
    }
    int comparison = memcmp(left->object_id, right->object_id, 16);
    if (comparison != 0) {
        return comparison;
    }
    if (left->target_kind == PRDB_SDK_SCOPE_TARGET_COLUMN) {
        return memcmp(left->column_id, right->column_id, 16);
    }
    return 0;
}

static int32_t validate_capabilities(
    const prdb_sdk_scope_capability_v1 *capabilities,
    uint16_t capability_count, size_t *encoded_len) {
    if (capability_count == 0 ||
        capability_count > PRDB_SDK_SCOPE_MAX_CAPABILITIES) {
        return PRDB_SDK_SCOPE_INVALID_VALUE;
    }
    if (capabilities == NULL) {
        return PRDB_SDK_SCOPE_INVALID_ARGUMENT;
    }
    *encoded_len = PSC1_HEADER_BYTES;
    for (uint16_t i = 0; i < capability_count; ++i) {
        const prdb_sdk_scope_capability_v1 *capability = &capabilities[i];
        if (capability->struct_size != sizeof(*capability) ||
            capability->abi_version != PRDB_SDK_SCOPE_ABI_V1_VERSION ||
            capability->reserved[0] != 0 || capability->reserved[1] != 0 ||
            id_is_zero(capability->object_id)) {
            return PRDB_SDK_SCOPE_INVALID_VALUE;
        }
        if (capability->target_kind == PRDB_SDK_SCOPE_TARGET_SCHEMA) {
            if (capability->operation != PRDB_SDK_SCOPE_OPERATION_USAGE ||
                !bytes_are_zero(capability->column_id,
                                sizeof(capability->column_id))) {
                return PRDB_SDK_SCOPE_INVALID_VALUE;
            }
        } else if (capability->target_kind == PRDB_SDK_SCOPE_TARGET_TABLE) {
            if (capability->operation < PRDB_SDK_SCOPE_OPERATION_SELECT ||
                capability->operation > PRDB_SDK_SCOPE_OPERATION_DELETE ||
                !bytes_are_zero(capability->column_id,
                                sizeof(capability->column_id))) {
                return PRDB_SDK_SCOPE_INVALID_VALUE;
            }
        } else if (capability->target_kind == PRDB_SDK_SCOPE_TARGET_COLUMN) {
            if (capability->operation < PRDB_SDK_SCOPE_OPERATION_SELECT ||
                capability->operation > PRDB_SDK_SCOPE_OPERATION_DELETE ||
                id_is_zero(capability->column_id)) {
                return PRDB_SDK_SCOPE_INVALID_VALUE;
            }
        } else {
            return PRDB_SDK_SCOPE_INVALID_VALUE;
        }
        if (i != 0 && compare_capabilities(&capabilities[i - 1], capability) >= 0) {
            return PRDB_SDK_SCOPE_INVALID_VALUE;
        }
        *encoded_len += encoded_record_bytes(capability);
    }
    if (*encoded_len > PSC1_MAX_ENCODED_BYTES) {
        return PRDB_SDK_SCOPE_INVALID_VALUE;
    }
    return PRDB_SDK_SCOPE_OK;
}

int32_t prdb_sdk_scope_encode_v1(
    const prdb_sdk_scope_capability_v1 *capabilities,
    uint16_t capability_count, uint8_t *output, uint64_t output_capacity,
    uint64_t *output_bytes) {
    uint8_t encoded[PSC1_MAX_ENCODED_BYTES];
    size_t encoded_len = 0;
    size_t cursor = PSC1_HEADER_BYTES;
    int32_t status;

    if (output_bytes == NULL) {
        return PRDB_SDK_SCOPE_INVALID_ARGUMENT;
    }
    *output_bytes = 0;
    if (output == NULL && output_capacity != 0) {
        return PRDB_SDK_SCOPE_INVALID_ARGUMENT;
    }
    status = validate_capabilities(capabilities, capability_count, &encoded_len);
    if (status != PRDB_SDK_SCOPE_OK) {
        return status;
    }
    if (output == NULL || output_capacity < encoded_len) {
        *output_bytes = encoded_len;
        return PRDB_SDK_SCOPE_BUFFER_TOO_SMALL;
    }

    encoded[0] = 'P';
    encoded[1] = 'S';
    encoded[2] = 'C';
    encoded[3] = '1';
    encoded[4] = (uint8_t)(capability_count >> 8);
    encoded[5] = (uint8_t)capability_count;
    for (uint16_t i = 0; i < capability_count; ++i) {
        const prdb_sdk_scope_capability_v1 *capability = &capabilities[i];
        size_t record_bytes = encoded_record_bytes(capability);
        encoded[cursor++] = capability->operation;
        encoded[cursor++] = capability->target_kind;
        memcpy(&encoded[cursor], capability->object_id, 16);
        cursor += 16;
        if (record_bytes == 34) {
            memcpy(&encoded[cursor], capability->column_id, 16);
            cursor += 16;
        }
    }
    memcpy(output, encoded, encoded_len);
    *output_bytes = encoded_len;
    return PRDB_SDK_SCOPE_OK;
}
