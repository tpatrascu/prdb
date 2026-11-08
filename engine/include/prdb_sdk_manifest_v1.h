#ifndef PRDB_SDK_MANIFEST_V1_H
#define PRDB_SDK_MANIFEST_V1_H

#include <stdint.h>

#define PRDB_SDK_MANIFEST_ABI_V1_VERSION UINT32_C(1)
#define PRDB_SDK_MANIFEST_MAX_DEPENDENCIES UINT16_C(32)
#define PRDB_SDK_MANIFEST_MAX_DEPENDENCY_NAME_BYTES UINT16_C(128)

#define PRDB_SDK_MANIFEST_OK INT32_C(0)
#define PRDB_SDK_MANIFEST_INVALID_ARGUMENT INT32_C(1)
#define PRDB_SDK_MANIFEST_INVALID_VALUE INT32_C(2)
#define PRDB_SDK_MANIFEST_BUFFER_TOO_SMALL INT32_C(3)

typedef struct prdb_sdk_manifest_dependency_v1 {
    const uint8_t *name;
    uint16_t name_len;
} prdb_sdk_manifest_dependency_v1;

/* Input strings and dependency names are borrowed for the duration of encode. */
typedef struct prdb_sdk_manifest_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    uint8_t native_abi_version;
    uint8_t reserved[3];
    uint32_t sdk_release;
    const uint8_t *cpu_baseline;
    uint16_t cpu_baseline_len;
    const uint8_t *runtime_profile;
    uint16_t runtime_profile_len;
    const uint8_t *compiler_identity;
    uint16_t compiler_identity_len;
    const prdb_sdk_manifest_dependency_v1 *dependencies;
    uint16_t dependency_count;
    uint16_t reserved2;
} prdb_sdk_manifest_v1;

/*
 * Emits canonical PMF1 bytes. Required output length is returned in
 * output_bytes for both success and BUFFER_TOO_SMALL; validation failures
 * leave it zero and do not modify output. A NULL output with zero capacity is
 * a supported size query. The output range must not overlap output_bytes.
 * output_bytes must not alias the manifest or its borrowed inputs.
 */
int32_t prdb_sdk_manifest_encode_v1(const prdb_sdk_manifest_v1 *manifest,
                                    uint8_t *output,
                                    uint64_t output_capacity,
                                    uint64_t *output_bytes);

#endif /* PRDB_SDK_MANIFEST_V1_H */
