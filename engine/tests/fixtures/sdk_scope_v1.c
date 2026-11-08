#include "prdb_sdk_scope_v1.h"

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

static const uint8_t expected[] = {
    'P', 'S', 'C', '1', 0x00, 0x03,
    0x01, 0x01, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
    0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
    0x02, 0x02, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
    0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
    0x05, 0x03,
    0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
    0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
    0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb,
    0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb,
};

static void initialize_capability(prdb_sdk_scope_capability_v1 *capability,
                                  uint8_t operation, uint8_t target_kind) {
    memset(capability, 0, sizeof(*capability));
    capability->struct_size = (uint32_t)sizeof(*capability);
    capability->abi_version = PRDB_SDK_SCOPE_ABI_V1_VERSION;
    capability->operation = operation;
    capability->target_kind = target_kind;
}

static void make_capabilities(prdb_sdk_scope_capability_v1 capabilities[3]) {
    initialize_capability(&capabilities[0], PRDB_SDK_SCOPE_OPERATION_USAGE,
                          PRDB_SDK_SCOPE_TARGET_SCHEMA);
    initialize_capability(&capabilities[1], PRDB_SDK_SCOPE_OPERATION_SELECT,
                          PRDB_SDK_SCOPE_TARGET_TABLE);
    initialize_capability(&capabilities[2], PRDB_SDK_SCOPE_OPERATION_DELETE,
                          PRDB_SDK_SCOPE_TARGET_COLUMN);
    for (uint8_t i = 0; i < 16; ++i) {
        capabilities[0].object_id[i] = (uint8_t)(i + 1);
        capabilities[1].object_id[i] = (uint8_t)(i + 1);
        capabilities[2].object_id[i] = 0xaa;
        capabilities[2].column_id[i] = 0xbb;
    }
}

static void make_max_capabilities(
    prdb_sdk_scope_capability_v1 capabilities[PRDB_SDK_SCOPE_MAX_CAPABILITIES]) {
    for (uint16_t i = 0; i < PRDB_SDK_SCOPE_MAX_CAPABILITIES; ++i) {
        uint8_t ordinal = (uint8_t)(i % 50 + 1);
        initialize_capability(&capabilities[i],
                              (uint8_t)(PRDB_SDK_SCOPE_OPERATION_SELECT + i / 50),
                              PRDB_SDK_SCOPE_TARGET_COLUMN);
        capabilities[i].object_id[15] = ordinal;
        capabilities[i].column_id[15] = (uint8_t)(ordinal + 1);
    }
}

int main(void) {
    prdb_sdk_scope_capability_v1 capabilities[3];
    prdb_sdk_scope_capability_v1 invalid[3];
    prdb_sdk_scope_capability_v1 maximum[PRDB_SDK_SCOPE_MAX_CAPABILITIES];
    uint8_t output[sizeof(expected)];
    uint8_t small[sizeof(expected)];
    uint8_t maximum_output[6806];
    uint64_t output_bytes = UINT64_MAX;
    assert(sizeof(expected) == 76);
    make_capabilities(capabilities);

    assert(prdb_sdk_scope_encode_v1(capabilities, 3, NULL, 0, &output_bytes) ==
           PRDB_SDK_SCOPE_BUFFER_TOO_SMALL);
    assert(output_bytes == sizeof(expected));
    assert(prdb_sdk_scope_encode_v1(capabilities, 3, output, sizeof(output),
                                    &output_bytes) == PRDB_SDK_SCOPE_OK);
    assert(output_bytes == sizeof(expected));
    assert(memcmp(output, expected, sizeof(expected)) == 0);

    memset(small, 0xa5, sizeof(small));
    assert(prdb_sdk_scope_encode_v1(capabilities, 3, small, sizeof(small) - 1,
                                    &output_bytes) ==
           PRDB_SDK_SCOPE_BUFFER_TOO_SMALL);
    assert(output_bytes == sizeof(expected));
    for (size_t i = 0; i < sizeof(small); ++i) {
        assert(small[i] == 0xa5);
    }

    memcpy(invalid, capabilities, sizeof(invalid));
    prdb_sdk_scope_capability_v1 first = invalid[0];
    invalid[0] = invalid[1];
    invalid[1] = first;
    assert(prdb_sdk_scope_encode_v1(invalid, 3, output, sizeof(output),
                                    &output_bytes) ==
           PRDB_SDK_SCOPE_INVALID_VALUE);
    assert(output_bytes == 0);

    memcpy(invalid, capabilities, sizeof(invalid));
    invalid[0].operation = PRDB_SDK_SCOPE_OPERATION_SELECT;
    invalid[0].target_kind = PRDB_SDK_SCOPE_TARGET_TABLE;
    assert(prdb_sdk_scope_encode_v1(invalid, 3, output, sizeof(output),
                                    &output_bytes) ==
           PRDB_SDK_SCOPE_INVALID_VALUE);
    assert(output_bytes == 0);

    memcpy(invalid, capabilities, sizeof(invalid));
    invalid[0].operation = PRDB_SDK_SCOPE_OPERATION_DELETE;
    assert(prdb_sdk_scope_encode_v1(invalid, 3, output, sizeof(output),
                                    &output_bytes) ==
           PRDB_SDK_SCOPE_INVALID_VALUE);
    assert(output_bytes == 0);

    memcpy(invalid, capabilities, sizeof(invalid));
    memset(invalid[0].object_id, 0, sizeof(invalid[0].object_id));
    assert(prdb_sdk_scope_encode_v1(invalid, 3, output, sizeof(output),
                                    &output_bytes) ==
           PRDB_SDK_SCOPE_INVALID_VALUE);

    memcpy(invalid, capabilities, sizeof(invalid));
    invalid[1].column_id[0] = 1;
    assert(prdb_sdk_scope_encode_v1(invalid, 3, output, sizeof(output),
                                    &output_bytes) ==
           PRDB_SDK_SCOPE_INVALID_VALUE);

    memcpy(invalid, capabilities, sizeof(invalid));
    memset(invalid[2].column_id, 0, sizeof(invalid[2].column_id));
    assert(prdb_sdk_scope_encode_v1(invalid, 3, output, sizeof(output),
                                    &output_bytes) ==
           PRDB_SDK_SCOPE_INVALID_VALUE);

    memcpy(invalid, capabilities, sizeof(invalid));
    invalid[0].operation = PRDB_SDK_SCOPE_OPERATION_DELETE;
    assert(prdb_sdk_scope_encode_v1(invalid, 3, output, sizeof(output),
                                    &output_bytes) ==
           PRDB_SDK_SCOPE_INVALID_VALUE);

    assert(prdb_sdk_scope_encode_v1(capabilities, 0, output, sizeof(output),
                                    &output_bytes) ==
           PRDB_SDK_SCOPE_INVALID_VALUE);
    assert(prdb_sdk_scope_encode_v1(capabilities,
                                    PRDB_SDK_SCOPE_MAX_CAPABILITIES + 1, output,
                                    sizeof(output), &output_bytes) ==
           PRDB_SDK_SCOPE_INVALID_VALUE);
    make_max_capabilities(maximum);
    assert(prdb_sdk_scope_encode_v1(maximum,
                                    PRDB_SDK_SCOPE_MAX_CAPABILITIES,
                                    maximum_output, sizeof(maximum_output),
                                    &output_bytes) == PRDB_SDK_SCOPE_OK);
    assert(output_bytes == sizeof(maximum_output));
    assert(prdb_sdk_scope_encode_v1(NULL, 1, output, sizeof(output),
                                    &output_bytes) ==
           PRDB_SDK_SCOPE_INVALID_ARGUMENT);
    output_bytes = UINT64_MAX;
    assert(prdb_sdk_scope_encode_v1(capabilities, 3, NULL, 1, &output_bytes) ==
           PRDB_SDK_SCOPE_INVALID_ARGUMENT);
    assert(output_bytes == 0);
    assert(prdb_sdk_scope_encode_v1(capabilities, 3, output, sizeof(output),
                                    NULL) == PRDB_SDK_SCOPE_INVALID_ARGUMENT);
    return 0;
}
