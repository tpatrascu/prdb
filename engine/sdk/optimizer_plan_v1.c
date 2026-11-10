#include "prdb_sdk_optimizer_plan_v1.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

typedef struct plan_reader {
    const uint8_t *bytes;
    uint64_t length;
    uint64_t offset;
} plan_reader;

static int ranges_overlap(const void *left, uint64_t left_len,
                          const void *right, uint64_t right_len) {
    uintptr_t left_start = (uintptr_t)left;
    uintptr_t right_start = (uintptr_t)right;
    uintptr_t left_end;
    uintptr_t right_end;

    if (left_len == 0 || right_len == 0) {
        return 0;
    }
    if (left_start > UINTPTR_MAX - left_len ||
        right_start > UINTPTR_MAX - right_len) {
        return 1;
    }
    left_end = left_start + (uintptr_t)left_len;
    right_end = right_start + (uintptr_t)right_len;
    return left_start < right_end && right_start < left_end;
}

static int take(plan_reader *reader, uint64_t count, const uint8_t **out) {
    if (count > reader->length - reader->offset) {
        return 0;
    }
    *out = reader->bytes + reader->offset;
    reader->offset += count;
    return 1;
}

static int read_u8(plan_reader *reader, uint8_t *out) {
    const uint8_t *bytes;
    if (!take(reader, 1, &bytes)) return 0;
    *out = bytes[0];
    return 1;
}

static int read_u16(plan_reader *reader, uint16_t *out) {
    const uint8_t *bytes;
    if (!take(reader, 2, &bytes)) return 0;
    *out = (uint16_t)(((uint16_t)bytes[0] << 8) | bytes[1]);
    return 1;
}

static int read_u32(plan_reader *reader, uint32_t *out) {
    const uint8_t *bytes;
    if (!take(reader, 4, &bytes)) return 0;
    *out = ((uint32_t)bytes[0] << 24) | ((uint32_t)bytes[1] << 16) |
           ((uint32_t)bytes[2] << 8) | (uint32_t)bytes[3];
    return 1;
}

static int read_u64(plan_reader *reader, uint64_t *out) {
    uint32_t high;
    uint32_t low;
    if (!read_u32(reader, &high) || !read_u32(reader, &low)) return 0;
    *out = ((uint64_t)high << 32) | low;
    return 1;
}

static int valid_identifier(const uint8_t *bytes, uint32_t length) {
    uint32_t index;
    if (bytes == NULL || length == 0 || length > 63 ||
        !((bytes[0] >= 'A' && bytes[0] <= 'Z') ||
          (bytes[0] >= 'a' && bytes[0] <= 'z') || bytes[0] == '_')) {
        return 0;
    }
    for (index = 1; index < length; ++index) {
        uint8_t byte = bytes[index];
        if (!((byte >= 'A' && byte <= 'Z') ||
              (byte >= 'a' && byte <= 'z') ||
              (byte >= '0' && byte <= '9') || byte == '_')) {
            return 0;
        }
    }
    return 1;
}

static int read_name(plan_reader *reader, const uint8_t **name_out,
                     uint32_t *length_out) {
    uint8_t length;
    const uint8_t *name;
    if (!read_u8(reader, &length) || !take(reader, length, &name) ||
        !valid_identifier(name, length)) {
        return 0;
    }
    *name_out = name;
    *length_out = length;
    return 1;
}

static int all_zero(const uint8_t *bytes, uint32_t length) {
    uint32_t index;
    for (index = 0; index < length; ++index) {
        if (bytes[index] != 0) return 0;
    }
    return 1;
}

static int valid_type(uint8_t type_code, const uint8_t parameters[4]) {
    switch (type_code) {
        case 1: case 2: case 3: case 4: case 6:
        case 8: case 9: case 10: case 11:
            return all_zero(parameters, 4);
        case 5:
            return parameters[0] >= 1 && parameters[0] <= 38 &&
                   parameters[1] <= parameters[0] && parameters[2] == 0 &&
                   parameters[3] == 0;
        case 7:
            return parameters[0] != 0 || parameters[1] != 0 ||
                   parameters[2] != 0 || parameters[3] != 0;
        default:
            return 0;
    }
}

static int read_column(plan_reader *reader,
                       prdb_sdk_optimizer_plan_column_v1 *column) {
    const uint8_t *bytes;
    if (!take(reader, 16, &bytes)) return 0;
    memcpy(column->column_id, bytes, 16);
    if (all_zero(column->column_id, 16) || !read_u32(reader, &column->ordinal) ||
        !read_u8(reader, &column->type_code) ||
        !take(reader, 4, &bytes)) {
        return 0;
    }
    memcpy(column->type_parameters, bytes, 4);
    if (!read_u8(reader, &column->nullable) || column->nullable > 1 ||
        !valid_type(column->type_code, column->type_parameters)) {
        return 0;
    }
    return 1;
}

static int columns_equal(const prdb_sdk_optimizer_plan_column_v1 *left,
                         const prdb_sdk_optimizer_plan_column_v1 *right) {
    return memcmp(left, right, sizeof(*left)) == 0;
}

int32_t prdb_sdk_optimizer_decode_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_plan_v1 *out, uint32_t out_capacity) {
    prdb_sdk_optimizer_plan_v1 decoded;
    plan_reader reader;
    uint16_t version;
    uint8_t kind;
    uint8_t reserved;
    uint8_t node_count;
    uint8_t node_tag;
    uint16_t count;
    uint32_t index;
    uint32_t other;
    uint8_t has_limit = 0;
    uint32_t limit = 0;

    if (payload == NULL || out == NULL || out_capacity < sizeof(*out) ||
        ranges_overlap(payload, payload_len, out, sizeof(*out))) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    if (payload_len > PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    memset(&decoded, 0, sizeof(decoded));
    reader.bytes = payload;
    reader.length = payload_len;
    reader.offset = 0;

    if (!read_u16(&reader, &version) || version != 1 ||
        !read_u8(&reader, &kind) || !read_u8(&reader, &reserved) ||
        kind != 1 || reserved != 0) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (!read_name(&reader, &decoded.schema_name, &decoded.schema_name_len) ||
        decoded.schema_name_len == 0) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    {
        const uint8_t *bytes;
        if (!take(&reader, 16, &bytes) || all_zero(bytes, 16)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        memcpy(decoded.schema_id, bytes, 16);
    }
    if (!read_u64(&reader, &decoded.schema_version) ||
        decoded.schema_version == 0 ||
        !read_name(&reader, &decoded.relation_name,
                   &decoded.relation_name_len)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    {
        const uint8_t *bytes;
        if (!take(&reader, 16, &bytes) || all_zero(bytes, 16)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        memcpy(decoded.relation_id, bytes, 16);
    }
    if (!read_u64(&reader, &decoded.table_epoch) || decoded.table_epoch == 0 ||
        !read_u8(&reader, &node_count) || node_count < 2 || node_count > 4) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (decoded.schema_name_len == 0 || decoded.relation_name_len == 0) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }

    if (!read_u8(&reader, &node_tag) || node_tag != 1 ||
        !read_u16(&reader, &count) || count == 0 ||
        count > PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    decoded.scan_column_count = count;
    for (index = 0; index < decoded.scan_column_count; ++index) {
        prdb_sdk_optimizer_plan_column_v1 *column =
            &decoded.scan_columns[index];
        if (!read_column(&reader, column)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        if (index != 0 &&
            decoded.scan_columns[index - 1].ordinal >= column->ordinal) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        for (other = 0; other < index; ++other) {
            if (memcmp(decoded.scan_columns[other].column_id,
                       column->column_id, 16) == 0) {
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            }
        }
    }

    if (!read_u8(&reader, &node_tag)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (node_tag == 2) {
        return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
    }
    if (node_tag != 3 || !read_u16(&reader, &count) || count == 0 ||
        count > PRDB_SDK_OPTIMIZER_PLAN_MAX_PROJECTIONS) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    decoded.projection_count = count;
    for (index = 0; index < decoded.projection_count; ++index) {
        prdb_sdk_optimizer_plan_column_v1 *projection =
            &decoded.projections[index];
        int found = 0;
        if (!read_column(&reader, projection)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        for (other = 0; other < decoded.scan_column_count; ++other) {
            if (columns_equal(projection, &decoded.scan_columns[other])) {
                found = 1;
                break;
            }
        }
        if (!found) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }

    if (node_count == 3) {
        if (!read_u8(&reader, &node_tag) || node_tag != 4 ||
            !read_u32(&reader, &limit)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        has_limit = 1;
    }
    if (node_count != (uint8_t)(2 + has_limit) || reader.offset != reader.length) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    decoded.struct_size = sizeof(decoded);
    decoded.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    decoded.has_limit = has_limit;
    decoded.limit = limit;
    *out = decoded;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}
