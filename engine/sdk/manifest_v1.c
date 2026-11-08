#include "prdb_sdk_manifest_v1.h"

#include <stddef.h>
#include <string.h>

#define PMF1_HEADER_BYTES ((size_t)28)
#define PMF1_MAX_ENCODED_BYTES ((size_t)4572)

static const uint8_t descriptor_v1[] = "prdb_query_descriptor_v1";
static const uint8_t entry_v1[] = "prdb_query_main_v1";
static const uint8_t descriptor_v2[] = "prdb_query_descriptor_v2";
static const uint8_t entry_v2[] = "prdb_query_main_v2";
static const uint8_t descriptor_v3[] = "prdb_query_descriptor_v3";
static const uint8_t entry_v3[] = "prdb_query_main_v3";

static void write_u16_be(uint8_t *output, uint16_t value) {
    output[0] = (uint8_t)(value >> 8);
    output[1] = (uint8_t)value;
}

static void write_u32_be(uint8_t *output, uint32_t value) {
    output[0] = (uint8_t)(value >> 24);
    output[1] = (uint8_t)(value >> 16);
    output[2] = (uint8_t)(value >> 8);
    output[3] = (uint8_t)value;
}

static int is_profile_identifier(const uint8_t *bytes, uint16_t length) {
    if (length == 0 || length > 64) {
        return 0;
    }
    for (uint16_t i = 0; i < length; ++i) {
        uint8_t byte = bytes[i];
        if (!((byte >= 'a' && byte <= 'z') ||
              (byte >= 'A' && byte <= 'Z') ||
              (byte >= '0' && byte <= '9') || byte == '.' || byte == '_' ||
              byte == '-')) {
            return 0;
        }
    }
    return 1;
}

static int is_compiler_identity(const uint8_t *bytes, uint16_t length) {
    if (length == 0 || length > 128) {
        return 0;
    }
    for (uint16_t i = 0; i < length; ++i) {
        if (bytes[i] < 0x20 || bytes[i] > 0x7e) {
            return 0;
        }
    }
    return 1;
}

static int is_dependency_name(const uint8_t *bytes, uint16_t length) {
    if (length == 0 || length > PRDB_SDK_MANIFEST_MAX_DEPENDENCY_NAME_BYTES) {
        return 0;
    }
    for (uint16_t i = 0; i < length; ++i) {
        uint8_t byte = bytes[i];
        if (!((byte >= 'a' && byte <= 'z') ||
              (byte >= 'A' && byte <= 'Z') ||
              (byte >= '0' && byte <= '9') || byte == '.' || byte == '_' ||
              byte == '+' || byte == '-')) {
            return 0;
        }
    }
    for (uint16_t i = 1; i < length; ++i) {
        if (bytes[i - 1] == '.' && bytes[i] == '.') {
            return 0;
        }
    }
    return 1;
}

static int compare_dependency(const prdb_sdk_manifest_dependency_v1 *left,
                              const prdb_sdk_manifest_dependency_v1 *right) {
    uint16_t common = left->name_len < right->name_len ? left->name_len
                                                       : right->name_len;
    int comparison = memcmp(left->name, right->name, common);
    if (comparison != 0) {
        return comparison;
    }
    if (left->name_len < right->name_len) {
        return -1;
    }
    if (left->name_len > right->name_len) {
        return 1;
    }
    return 0;
}

static int32_t validate_manifest(const prdb_sdk_manifest_v1 *manifest,
                                 const uint8_t **descriptor,
                                 uint16_t *descriptor_len,
                                 const uint8_t **entry,
                                 uint16_t *entry_len,
                                 size_t *encoded_len) {
    if (manifest == NULL || manifest->struct_size != sizeof(*manifest) ||
        manifest->abi_version != PRDB_SDK_MANIFEST_ABI_V1_VERSION ||
        manifest->reserved[0] != 0 || manifest->reserved[1] != 0 ||
        manifest->reserved[2] != 0 || manifest->reserved2 != 0) {
        return PRDB_SDK_MANIFEST_INVALID_ARGUMENT;
    }
    if (manifest->native_abi_version == 1) {
        *descriptor = descriptor_v1;
        *descriptor_len = (uint16_t)(sizeof(descriptor_v1) - 1);
        *entry = entry_v1;
        *entry_len = (uint16_t)(sizeof(entry_v1) - 1);
    } else if (manifest->native_abi_version == 2) {
        *descriptor = descriptor_v2;
        *descriptor_len = (uint16_t)(sizeof(descriptor_v2) - 1);
        *entry = entry_v2;
        *entry_len = (uint16_t)(sizeof(entry_v2) - 1);
    } else if (manifest->native_abi_version == 3) {
        *descriptor = descriptor_v3;
        *descriptor_len = (uint16_t)(sizeof(descriptor_v3) - 1);
        *entry = entry_v3;
        *entry_len = (uint16_t)(sizeof(entry_v3) - 1);
    } else {
        return PRDB_SDK_MANIFEST_INVALID_VALUE;
    }
    if (manifest->sdk_release == 0 || manifest->cpu_baseline == NULL ||
        manifest->runtime_profile == NULL || manifest->compiler_identity == NULL ||
        !is_profile_identifier(manifest->cpu_baseline,
                               manifest->cpu_baseline_len) ||
        !is_profile_identifier(manifest->runtime_profile,
                               manifest->runtime_profile_len) ||
        !is_compiler_identity(manifest->compiler_identity,
                              manifest->compiler_identity_len) ||
        manifest->dependency_count > PRDB_SDK_MANIFEST_MAX_DEPENDENCIES ||
        (manifest->dependency_count != 0 && manifest->dependencies == NULL)) {
        return PRDB_SDK_MANIFEST_INVALID_VALUE;
    }

    for (uint16_t i = 0; i < manifest->dependency_count; ++i) {
        const prdb_sdk_manifest_dependency_v1 *dependency =
            &manifest->dependencies[i];
        if (dependency->name == NULL ||
            !is_dependency_name(dependency->name, dependency->name_len) ||
            (i != 0 && compare_dependency(&manifest->dependencies[i - 1],
                                          dependency) >= 0)) {
            return PRDB_SDK_MANIFEST_INVALID_VALUE;
        }
    }

    *encoded_len = PMF1_HEADER_BYTES + manifest->cpu_baseline_len +
                   manifest->runtime_profile_len +
                   manifest->compiler_identity_len + *descriptor_len + *entry_len;
    for (uint16_t i = 0; i < manifest->dependency_count; ++i) {
        *encoded_len += 2 + manifest->dependencies[i].name_len;
    }
    if (*encoded_len > PMF1_MAX_ENCODED_BYTES) {
        return PRDB_SDK_MANIFEST_INVALID_VALUE;
    }
    return PRDB_SDK_MANIFEST_OK;
}

int32_t prdb_sdk_manifest_encode_v1(const prdb_sdk_manifest_v1 *manifest,
                                    uint8_t *output,
                                    uint64_t output_capacity,
                                    uint64_t *output_bytes) {
    const uint8_t *descriptor = NULL;
    const uint8_t *entry = NULL;
    uint16_t descriptor_len = 0;
    uint16_t entry_len = 0;
    size_t encoded_len = 0;
    uint8_t encoded[PMF1_MAX_ENCODED_BYTES];
    size_t cursor = PMF1_HEADER_BYTES;
    int32_t status;

    if (output_bytes == NULL) {
        return PRDB_SDK_MANIFEST_INVALID_ARGUMENT;
    }
    *output_bytes = 0;
    if (output == NULL && output_capacity != 0) {
        return PRDB_SDK_MANIFEST_INVALID_ARGUMENT;
    }
    status = validate_manifest(manifest, &descriptor, &descriptor_len, &entry,
                               &entry_len, &encoded_len);
    if (status != PRDB_SDK_MANIFEST_OK) {
        return status;
    }

    if (output == NULL || output_capacity < encoded_len) {
        *output_bytes = encoded_len;
        return PRDB_SDK_MANIFEST_BUFFER_TOO_SMALL;
    }

    encoded[0] = 'P';
    encoded[1] = 'M';
    encoded[2] = 'F';
    encoded[3] = '1';
    encoded[4] = 1;
    encoded[5] = 1;
    encoded[6] = manifest->native_abi_version;
    encoded[7] = 0;
    write_u16_be(&encoded[8], 1);
    write_u16_be(&encoded[10], 0);
    write_u32_be(&encoded[12], manifest->sdk_release);
    write_u16_be(&encoded[16], manifest->dependency_count);
    write_u16_be(&encoded[18], manifest->cpu_baseline_len);
    write_u16_be(&encoded[20], manifest->runtime_profile_len);
    write_u16_be(&encoded[22], manifest->compiler_identity_len);
    write_u16_be(&encoded[24], descriptor_len);
    write_u16_be(&encoded[26], entry_len);

    memcpy(&encoded[cursor], manifest->cpu_baseline,
           manifest->cpu_baseline_len);
    cursor += manifest->cpu_baseline_len;
    memcpy(&encoded[cursor], manifest->runtime_profile,
           manifest->runtime_profile_len);
    cursor += manifest->runtime_profile_len;
    memcpy(&encoded[cursor], manifest->compiler_identity,
           manifest->compiler_identity_len);
    cursor += manifest->compiler_identity_len;
    memcpy(&encoded[cursor], descriptor, descriptor_len);
    cursor += descriptor_len;
    memcpy(&encoded[cursor], entry, entry_len);
    cursor += entry_len;
    for (uint16_t i = 0; i < manifest->dependency_count; ++i) {
        const prdb_sdk_manifest_dependency_v1 *dependency =
            &manifest->dependencies[i];
        write_u16_be(&encoded[cursor], dependency->name_len);
        cursor += 2;
        memcpy(&encoded[cursor], dependency->name, dependency->name_len);
        cursor += dependency->name_len;
    }

    memcpy(output, encoded, encoded_len);
    *output_bytes = encoded_len;
    return PRDB_SDK_MANIFEST_OK;
}
