#include "prdb_sdk_value_v1.h"

#include <assert.h>
#include <stddef.h>
#include <string.h>

static prdb_sdk_value_v1 value(uint32_t state, uint32_t type,
                               const uint8_t *data, uint64_t length) {
    prdb_sdk_value_v1 result = {
        sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
        state, type, data, length};
    return result;
}

int main(void) {
    static const uint8_t integer_bytes[] = {0, 0, 0, 42};
    static const uint8_t text_bytes[] = {'o', 'k'};
    static const uint8_t bad_utf8[] = {0xc0, 0x80};
    static const uint8_t invalid_boolean[] = {2};
    static const uint8_t fixed_width_bytes[16] = {0};
    static const uint32_t fixed_types[] = {
        PRDB_SDK_VALUE_TYPE_BOOLEAN, PRDB_SDK_VALUE_TYPE_SMALLINT,
        PRDB_SDK_VALUE_TYPE_INTEGER, PRDB_SDK_VALUE_TYPE_BIGINT,
        PRDB_SDK_VALUE_TYPE_DECIMAL, PRDB_SDK_VALUE_TYPE_UUID,
        PRDB_SDK_VALUE_TYPE_DATE, PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ};
    static const uint64_t fixed_widths[] = {1, 2, 4, 8, 16, 16, 4, 8};
    prdb_sdk_value_v1 integer =
        value(PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
              integer_bytes, sizeof(integer_bytes));
    prdb_sdk_value_v1 null_value =
        value(PRDB_SDK_VALUE_STATE_NULL, 0, 0, 0);
    prdb_sdk_value_v1 redacted =
        value(PRDB_SDK_VALUE_STATE_REDACTED, 0, 0, 0);
    prdb_sdk_value_v1 text =
        value(PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_TEXT,
              text_bytes, sizeof(text_bytes));
    prdb_sdk_value_v1 malformed =
        value(PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
              integer_bytes, 3);

    assert(prdb_sdk_value_validate_v1(&integer) == PRDB_SDK_VALUE_VALID);
    assert(integer.data == integer_bytes);
    assert(prdb_sdk_value_validate_v1(&null_value) == PRDB_SDK_VALUE_VALID);
    assert(prdb_sdk_value_validate_v1(&redacted) == PRDB_SDK_VALUE_VALID);
    assert(prdb_sdk_value_validate_v1(&text) == PRDB_SDK_VALUE_VALID);
    assert(prdb_sdk_value_validate_v1(&malformed) ==
           PRDB_SDK_VALUE_INVALID_VALUE);

    for (size_t index = 0;
         index < sizeof(fixed_types) / sizeof(fixed_types[0]); ++index) {
        prdb_sdk_value_v1 fixed_value =
            value(PRDB_SDK_VALUE_STATE_VALUE, fixed_types[index],
                  fixed_width_bytes, fixed_widths[index]);
        assert(prdb_sdk_value_validate_v1(&fixed_value) ==
               PRDB_SDK_VALUE_VALID);
    }
    malformed = value(PRDB_SDK_VALUE_STATE_VALUE,
                      PRDB_SDK_VALUE_TYPE_BYTEA, 0, 0);
    assert(prdb_sdk_value_validate_v1(&malformed) == PRDB_SDK_VALUE_VALID);

    malformed = value(PRDB_SDK_VALUE_STATE_NULL,
                      PRDB_SDK_VALUE_TYPE_INTEGER, integer_bytes,
                      sizeof(integer_bytes));
    assert(prdb_sdk_value_validate_v1(&malformed) ==
           PRDB_SDK_VALUE_INVALID_VALUE);
    malformed = value(PRDB_SDK_VALUE_STATE_REDACTED, 0, integer_bytes,
                      sizeof(integer_bytes));
    assert(prdb_sdk_value_validate_v1(&malformed) ==
           PRDB_SDK_VALUE_INVALID_VALUE);
    malformed = value(PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_TEXT,
                      bad_utf8, sizeof(bad_utf8));
    assert(prdb_sdk_value_validate_v1(&malformed) ==
           PRDB_SDK_VALUE_INVALID_VALUE);
    malformed = value(PRDB_SDK_VALUE_STATE_VALUE,
                      PRDB_SDK_VALUE_TYPE_BOOLEAN, invalid_boolean,
                      sizeof(invalid_boolean));
    assert(prdb_sdk_value_validate_v1(&malformed) ==
           PRDB_SDK_VALUE_INVALID_VALUE);
    malformed = value(PRDB_SDK_VALUE_STATE_VALUE,
                      PRDB_SDK_VALUE_TYPE_INTEGER, 0, 4);
    assert(prdb_sdk_value_validate_v1(&malformed) ==
           PRDB_SDK_VALUE_INVALID_VALUE);
    malformed = value(99, PRDB_SDK_VALUE_TYPE_INTEGER, integer_bytes,
                      sizeof(integer_bytes));
    assert(prdb_sdk_value_validate_v1(&malformed) ==
           PRDB_SDK_VALUE_INVALID_VALUE);
    malformed.struct_size = sizeof(malformed) - 1;
    assert(prdb_sdk_value_validate_v1(&malformed) ==
           PRDB_SDK_VALUE_INVALID_VALUE);

    assert(sizeof(prdb_sdk_value_v1) == 32);
    assert(offsetof(prdb_sdk_value_v1, data) == 16);
    assert(offsetof(prdb_sdk_value_v1, data_len) == 24);

    malformed = value(PRDB_SDK_VALUE_STATE_VALUE,
                      PRDB_SDK_VALUE_TYPE_INTEGER, integer_bytes,
                      sizeof(integer_bytes));
    malformed.abi_version = 99;
    assert(prdb_sdk_value_validate_v1(&malformed) ==
           PRDB_SDK_VALUE_UNSUPPORTED_VERSION);
    return 0;
}
