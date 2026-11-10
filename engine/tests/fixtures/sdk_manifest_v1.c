#include "prdb_sdk_manifest_v1.h"

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

static const uint8_t expected[] = {
    0x50, 0x4d, 0x46, 0x31, 0x01, 0x01, 0x03, 0x00, 0x00, 0x01, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x01, 0x00, 0x02, 0x00, 0x09, 0x00, 0x0a, 0x00, 0x0d,
    0x00, 0x18, 0x00, 0x12, 0x78, 0x38, 0x36, 0x5f, 0x36, 0x34, 0x5f, 0x76,
    0x31, 0x67, 0x6c, 0x69, 0x62, 0x63, 0x5f, 0x32, 0x5f, 0x33, 0x31, 0x54,
    0x69, 0x6e, 0x79, 0x43, 0x43, 0x2d, 0x30, 0x2e, 0x39, 0x2e, 0x32, 0x37,
    0x70, 0x72, 0x64, 0x62, 0x5f, 0x71, 0x75, 0x65, 0x72, 0x79, 0x5f, 0x64,
    0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x6f, 0x72, 0x5f, 0x76, 0x33,
    0x70, 0x72, 0x64, 0x62, 0x5f, 0x71, 0x75, 0x65, 0x72, 0x79, 0x5f, 0x6d,
    0x61, 0x69, 0x6e, 0x5f, 0x76, 0x33, 0x00, 0x09, 0x6c, 0x69, 0x62, 0x63,
    0x2e, 0x73, 0x6f, 0x2e, 0x36, 0x00, 0x09, 0x6c, 0x69, 0x62, 0x6d, 0x2e,
    0x73, 0x6f, 0x2e, 0x36,
};

static const uint8_t cpu[] = "x86_64_v1";
static const uint8_t runtime[] = "glibc_2_31";
static const uint8_t compiler[] = "TinyCC-0.9.27";
static const uint8_t libc_name[] = "libc.so.6";
static const uint8_t libm_name[] = "libm.so.6";
static const uint8_t unsorted_name[] = "libz.so";
static const uint8_t descriptor_v1[] = "prdb_query_descriptor_v1";
static const uint8_t entry_v1[] = "prdb_query_main_v1";
static const uint8_t descriptor_v2[] = "prdb_query_descriptor_v2";
static const uint8_t entry_v2[] = "prdb_query_main_v2";
static const uint8_t descriptor_v4[] = "prdb_query_descriptor_v4";
static const uint8_t entry_v4[] = "prdb_query_main_v4";

static prdb_sdk_manifest_v1 make_manifest(void) {
    static prdb_sdk_manifest_dependency_v1 dependencies[2];
    prdb_sdk_manifest_v1 manifest;

    dependencies[0].name = libc_name;
    dependencies[0].name_len = (uint16_t)(sizeof(libc_name) - 1);
    dependencies[1].name = libm_name;
    dependencies[1].name_len = (uint16_t)(sizeof(libm_name) - 1);
    memset(&manifest, 0, sizeof(manifest));
    manifest.struct_size = (uint32_t)sizeof(manifest);
    manifest.abi_version = PRDB_SDK_MANIFEST_ABI_V1_VERSION;
    manifest.native_abi_version = 3;
    manifest.sdk_release = 1;
    manifest.cpu_baseline = cpu;
    manifest.cpu_baseline_len = (uint16_t)(sizeof(cpu) - 1);
    manifest.runtime_profile = runtime;
    manifest.runtime_profile_len = (uint16_t)(sizeof(runtime) - 1);
    manifest.compiler_identity = compiler;
    manifest.compiler_identity_len = (uint16_t)(sizeof(compiler) - 1);
    manifest.dependencies = dependencies;
    manifest.dependency_count = 2;
    return manifest;
}

int main(void) {
    prdb_sdk_manifest_v1 manifest = make_manifest();
    prdb_sdk_manifest_dependency_v1 dependencies[2];
    uint8_t output[sizeof(expected)];
    uint8_t small[sizeof(expected)];
    uint64_t output_bytes = UINT64_MAX;
    assert(sizeof(expected) == 124);

    assert(prdb_sdk_manifest_encode_v1(&manifest, NULL, 0, &output_bytes) ==
           PRDB_SDK_MANIFEST_BUFFER_TOO_SMALL);
    assert(output_bytes == sizeof(expected));
    memset(output, 0, sizeof(output));
    assert(prdb_sdk_manifest_encode_v1(&manifest, output, sizeof(output),
                                       &output_bytes) == PRDB_SDK_MANIFEST_OK);
    assert(output_bytes == sizeof(expected));
    assert(memcmp(output, expected, sizeof(expected)) == 0);

    manifest.native_abi_version = 1;
    assert(prdb_sdk_manifest_encode_v1(&manifest, output, sizeof(output),
                                       &output_bytes) == PRDB_SDK_MANIFEST_OK);
    assert(output[6] == 1);
    assert(memcmp(&output[60], descriptor_v1, sizeof(descriptor_v1) - 1) == 0);
    assert(memcmp(&output[84], entry_v1, sizeof(entry_v1) - 1) == 0);
    manifest.native_abi_version = 2;
    assert(prdb_sdk_manifest_encode_v1(&manifest, output, sizeof(output),
                                       &output_bytes) == PRDB_SDK_MANIFEST_OK);
    assert(output[6] == 2);
    assert(memcmp(&output[60], descriptor_v2, sizeof(descriptor_v2) - 1) == 0);
    assert(memcmp(&output[84], entry_v2, sizeof(entry_v2) - 1) == 0);
    manifest.native_abi_version = 3;
    assert(prdb_sdk_manifest_encode_v1(&manifest, output, sizeof(output),
                                       &output_bytes) == PRDB_SDK_MANIFEST_OK);
    assert(output[6] == 3);
    manifest.native_abi_version = 4;
    assert(prdb_sdk_manifest_encode_v1(&manifest, output, sizeof(output),
                                       &output_bytes) == PRDB_SDK_MANIFEST_OK);
    assert(output[6] == 4);
    assert(memcmp(&output[60], descriptor_v4, sizeof(descriptor_v4) - 1) == 0);
    assert(memcmp(&output[84], entry_v4, sizeof(entry_v4) - 1) == 0);
    manifest.native_abi_version = 3;

    memset(small, 0xa5, sizeof(small));
    output_bytes = 0;
    assert(prdb_sdk_manifest_encode_v1(&manifest, small, sizeof(small) - 1,
                                       &output_bytes) ==
           PRDB_SDK_MANIFEST_BUFFER_TOO_SMALL);
    assert(output_bytes == sizeof(expected));
    for (size_t i = 0; i < sizeof(small); ++i) {
        assert(small[i] == 0xa5);
    }

    dependencies[0] = manifest.dependencies[0];
    dependencies[1] = manifest.dependencies[1];
    manifest.dependencies = dependencies;
    dependencies[0].name = unsorted_name;
    dependencies[0].name_len = (uint16_t)(sizeof(unsorted_name) - 1);
    output_bytes = UINT64_MAX;
    memset(output, 0xa5, sizeof(output));
    assert(prdb_sdk_manifest_encode_v1(&manifest, output, sizeof(output),
                                       &output_bytes) ==
           PRDB_SDK_MANIFEST_INVALID_VALUE);
    assert(output_bytes == 0);
    for (size_t i = 0; i < sizeof(output); ++i) {
        assert(output[i] == 0xa5);
    }

    manifest = make_manifest();
    manifest.native_abi_version = 5;
    assert(prdb_sdk_manifest_encode_v1(&manifest, output, sizeof(output),
                                       &output_bytes) ==
           PRDB_SDK_MANIFEST_INVALID_VALUE);
    assert(output_bytes == 0);

    manifest = make_manifest();
    manifest.sdk_release = 0;
    assert(prdb_sdk_manifest_encode_v1(&manifest, output, sizeof(output),
                                       &output_bytes) ==
           PRDB_SDK_MANIFEST_INVALID_VALUE);
    assert(output_bytes == 0);

    assert(prdb_sdk_manifest_encode_v1(NULL, output, sizeof(output),
                                       &output_bytes) ==
           PRDB_SDK_MANIFEST_INVALID_ARGUMENT);
    assert(prdb_sdk_manifest_encode_v1(&manifest, output, sizeof(output), NULL) ==
           PRDB_SDK_MANIFEST_INVALID_ARGUMENT);
    output_bytes = UINT64_MAX;
    assert(prdb_sdk_manifest_encode_v1(&manifest, NULL, 1, &output_bytes) ==
           PRDB_SDK_MANIFEST_INVALID_ARGUMENT);
    assert(output_bytes == 0);
    return 0;
}
