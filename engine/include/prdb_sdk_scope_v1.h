#ifndef PRDB_SDK_SCOPE_V1_H
#define PRDB_SDK_SCOPE_V1_H

#include <stdint.h>

#define PRDB_SDK_SCOPE_ABI_V1_VERSION UINT32_C(1)
#define PRDB_SDK_SCOPE_MAX_CAPABILITIES UINT16_C(200)

#define PRDB_SDK_SCOPE_OPERATION_USAGE UINT8_C(1)
#define PRDB_SDK_SCOPE_OPERATION_SELECT UINT8_C(2)
#define PRDB_SDK_SCOPE_OPERATION_INSERT UINT8_C(3)
#define PRDB_SDK_SCOPE_OPERATION_UPDATE UINT8_C(4)
#define PRDB_SDK_SCOPE_OPERATION_DELETE UINT8_C(5)

#define PRDB_SDK_SCOPE_TARGET_SCHEMA UINT8_C(1)
#define PRDB_SDK_SCOPE_TARGET_TABLE UINT8_C(2)
#define PRDB_SDK_SCOPE_TARGET_COLUMN UINT8_C(3)

#define PRDB_SDK_SCOPE_OK INT32_C(0)
#define PRDB_SDK_SCOPE_INVALID_ARGUMENT INT32_C(1)
#define PRDB_SDK_SCOPE_INVALID_VALUE INT32_C(2)
#define PRDB_SDK_SCOPE_BUFFER_TOO_SMALL INT32_C(3)

typedef struct prdb_sdk_scope_capability_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    uint8_t operation;
    uint8_t target_kind;
    uint8_t reserved[2];
    uint8_t object_id[16];
    uint8_t column_id[16];
} prdb_sdk_scope_capability_v1;

/*
 * Emits canonical PSC1 records. Capabilities must be strictly sorted by
 * encoded record bytes. For schema/table targets column_id must be all zero;
 * schema supports only USAGE, while tables and columns support SELECT through
 * DELETE. IDs must be nonzero. The caller owns the output buffer.
 * A NULL output with zero capacity performs a size query; BUFFER_TOO_SMALL
 * reports the required length without writing partial bytes. output_bytes
 * must not overlap the output buffer or capability array.
 */
int32_t prdb_sdk_scope_encode_v1(
    const prdb_sdk_scope_capability_v1 *capabilities,
    uint16_t capability_count,
    uint8_t *output,
    uint64_t output_capacity,
    uint64_t *output_bytes);

#endif /* PRDB_SDK_SCOPE_V1_H */
