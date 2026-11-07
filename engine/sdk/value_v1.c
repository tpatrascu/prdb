#include "prdb_sdk_value_v1.h"

#include <stddef.h>

static int valid_utf8(const uint8_t *bytes, size_t length) {
    size_t offset = 0;

    while (offset < length) {
        uint8_t first = bytes[offset++];
        uint32_t codepoint;
        size_t continuation_count;

        if (first <= 0x7f) {
            continue;
        } else if (first >= 0xc2 && first <= 0xdf) {
            codepoint = (uint32_t)(first & 0x1f);
            continuation_count = 1;
        } else if (first >= 0xe0 && first <= 0xef) {
            codepoint = (uint32_t)(first & 0x0f);
            continuation_count = 2;
        } else if (first >= 0xf0 && first <= 0xf4) {
            codepoint = (uint32_t)(first & 0x07);
            continuation_count = 3;
        } else {
            return 0;
        }

        if (continuation_count > length - offset) {
            return 0;
        }
        for (size_t index = 0; index < continuation_count; ++index) {
            uint8_t next = bytes[offset++];
            if ((next & 0xc0) != 0x80) {
                return 0;
            }
            codepoint = (codepoint << 6) | (uint32_t)(next & 0x3f);
        }
        if ((continuation_count == 1 && codepoint < 0x80) ||
            (continuation_count == 2 && codepoint < 0x800) ||
            (continuation_count == 3 && codepoint < 0x10000) ||
            (codepoint >= 0xd800 && codepoint <= 0xdfff) ||
            codepoint > 0x10ffff) {
            return 0;
        }
    }
    return 1;
}

int32_t prdb_sdk_value_validate_v1(const prdb_sdk_value_v1 *value) {
    size_t length;
    uint64_t expected_length = 0;

    if (value == NULL) {
        return PRDB_SDK_VALUE_INVALID_ARGUMENT;
    }
    if (value->struct_size < sizeof(*value)) {
        return PRDB_SDK_VALUE_INVALID_VALUE;
    }
    if (value->abi_version != PRDB_SDK_VALUE_ABI_V1_VERSION) {
        return PRDB_SDK_VALUE_UNSUPPORTED_VERSION;
    }
    if (value->data_len > PRDB_SDK_VALUE_MAX_BYTES ||
        (value->data_len != 0 && value->data == NULL)) {
        return PRDB_SDK_VALUE_INVALID_VALUE;
    }

    if (value->state == PRDB_SDK_VALUE_STATE_NULL ||
        value->state == PRDB_SDK_VALUE_STATE_REDACTED) {
        return value->type_code == 0 && value->data == NULL &&
                       value->data_len == 0
                   ? PRDB_SDK_VALUE_VALID
                   : PRDB_SDK_VALUE_INVALID_VALUE;
    }
    if (value->state != PRDB_SDK_VALUE_STATE_VALUE) {
        return PRDB_SDK_VALUE_INVALID_VALUE;
    }

    switch (value->type_code) {
    case PRDB_SDK_VALUE_TYPE_BOOLEAN:
        expected_length = 1;
        break;
    case PRDB_SDK_VALUE_TYPE_SMALLINT:
        expected_length = 2;
        break;
    case PRDB_SDK_VALUE_TYPE_INTEGER:
    case PRDB_SDK_VALUE_TYPE_DATE:
        expected_length = 4;
        break;
    case PRDB_SDK_VALUE_TYPE_BIGINT:
    case PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ:
        expected_length = 8;
        break;
    case PRDB_SDK_VALUE_TYPE_DECIMAL:
        expected_length = 16;
        break;
    case PRDB_SDK_VALUE_TYPE_UUID:
        expected_length = 16;
        break;
    case PRDB_SDK_VALUE_TYPE_TEXT:
    case PRDB_SDK_VALUE_TYPE_VARCHAR:
    case PRDB_SDK_VALUE_TYPE_BYTEA:
        break;
    default:
        return PRDB_SDK_VALUE_INVALID_VALUE;
    }

    if (expected_length != 0 && value->data_len != expected_length) {
        return PRDB_SDK_VALUE_INVALID_VALUE;
    }
    if (value->data_len == 0) {
        return value->type_code == PRDB_SDK_VALUE_TYPE_TEXT ||
                       value->type_code == PRDB_SDK_VALUE_TYPE_VARCHAR ||
                       value->type_code == PRDB_SDK_VALUE_TYPE_BYTEA
                   ? PRDB_SDK_VALUE_VALID
                   : PRDB_SDK_VALUE_INVALID_VALUE;
    }

    length = (size_t)value->data_len;
    if (value->type_code == PRDB_SDK_VALUE_TYPE_BOOLEAN && value->data[0] > 1) {
        return PRDB_SDK_VALUE_INVALID_VALUE;
    }
    if ((value->type_code == PRDB_SDK_VALUE_TYPE_TEXT ||
         value->type_code == PRDB_SDK_VALUE_TYPE_VARCHAR) &&
        !valid_utf8(value->data, length)) {
        return PRDB_SDK_VALUE_INVALID_VALUE;
    }
    return PRDB_SDK_VALUE_VALID;
}
