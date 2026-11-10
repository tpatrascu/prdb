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

typedef struct predicate_type {
    uint8_t is_null;
    uint8_t type_code;
    uint8_t parameters[4];
} predicate_type;

static int valid_utf8(const uint8_t *bytes, uint32_t length) {
    uint32_t offset = 0;
    while (offset < length) {
        uint8_t first = bytes[offset++];
        uint32_t codepoint;
        uint32_t continuation_count;
        uint32_t minimum;
        uint32_t index;
        if (first <= 0x7f) continue;
        if (first >= 0xc2 && first <= 0xdf) {
            codepoint = first & 0x1f;
            continuation_count = 1;
            minimum = 0x80;
        } else if (first >= 0xe0 && first <= 0xef) {
            codepoint = first & 0x0f;
            continuation_count = 2;
            minimum = 0x800;
        } else if (first >= 0xf0 && first <= 0xf4) {
            codepoint = first & 0x07;
            continuation_count = 3;
            minimum = 0x10000;
        } else {
            return 0;
        }
        if (continuation_count > length - offset) return 0;
        for (index = 0; index < continuation_count; ++index) {
            uint8_t next = bytes[offset++];
            if ((next & 0xc0) != 0x80) return 0;
            codepoint = (codepoint << 6) | (next & 0x3f);
        }
        if (codepoint < minimum || codepoint > 0x10ffff ||
            (codepoint >= 0xd800 && codepoint <= 0xdfff)) {
            return 0;
        }
    }
    return 1;
}

static int same_type(const predicate_type *left, const predicate_type *right) {
    return left->is_null || right->is_null ||
           (left->type_code == right->type_code &&
            memcmp(left->parameters, right->parameters, 4) == 0);
}

static int predicate_boolean(const predicate_type *type) {
    return !type->is_null && type->type_code == 1 &&
           all_zero(type->parameters, 4);
}

static int decode_predicate(plan_reader *reader,
                            const prdb_sdk_optimizer_plan_column_v1 *columns,
                            uint32_t column_count, uint32_t depth,
                            uint32_t *nodes, predicate_type *type_out) {
    uint8_t tag;
    const uint8_t *bytes;
    predicate_type type = {0};
    if (depth > 32 || ++*nodes > 256 || !read_u8(reader, &tag)) return 0;
    switch (tag) {
        case 0x00:
            type.is_null = 1;
            break;
        case 0x01:
            if (!read_u8(reader, &tag) || tag > 1) return 0;
            type.type_code = 1;
            break;
        case 0x02:
            if (!take(reader, 8, &bytes)) return 0;
            type.type_code = 3;
            break;
        case 0x03: {
            uint16_t text_len;
            if (!read_u16(reader, &text_len) || text_len > 4096 ||
                !take(reader, text_len, &bytes) ||
                !valid_utf8(bytes, text_len)) {
                return 0;
            }
            type.type_code = 6;
            break;
        }
        case 0x10: {
            uint32_t index;
            int found = 0;
            if (!take(reader, 16, &bytes)) return 0;
            for (index = 0; index < column_count; ++index) {
                if (memcmp(bytes, columns[index].column_id, 16) == 0) {
                    type.type_code = columns[index].type_code;
                    memcpy(type.parameters,
                           columns[index].type_parameters, 4);
                    found = 1;
                    break;
                }
            }
            if (!found) return 0;
            break;
        }
        case 0x20: {
            uint8_t comparison;
            predicate_type left;
            predicate_type right;
            if (!read_u8(reader, &comparison) || comparison > 5 ||
                !decode_predicate(reader, columns, column_count, depth + 1,
                                  nodes, &left) ||
                !decode_predicate(reader, columns, column_count, depth + 1,
                                  nodes, &right) ||
                !same_type(&left, &right)) {
                return 0;
            }
            type.type_code = 1;
            break;
        }
        case 0x30: case 0x31: {
            uint8_t child_count;
            uint32_t index;
            if (!read_u8(reader, &child_count) || child_count < 2 ||
                child_count > 16) {
                return 0;
            }
            for (index = 0; index < child_count; ++index) {
                predicate_type child;
                if (!decode_predicate(reader, columns, column_count,
                                      depth + 1, nodes, &child) ||
                    !predicate_boolean(&child)) {
                    return 0;
                }
            }
            type.type_code = 1;
            break;
        }
        case 0x32: {
            predicate_type child;
            if (!decode_predicate(reader, columns, column_count, depth + 1,
                                  nodes, &child) ||
                !predicate_boolean(&child)) {
                return 0;
            }
            type.type_code = 1;
            break;
        }
        default:
            return 0;
    }
    *type_out = type;
    return 1;
}

static int columns_equal(const prdb_sdk_optimizer_plan_column_v1 *left,
                         const prdb_sdk_optimizer_plan_column_v1 *right) {
    return memcmp(left, right, sizeof(*left)) == 0;
}

static int32_t decode_plan_internal(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_plan_v1 *out, uint32_t out_capacity,
    prdb_sdk_optimizer_filter_v1 *filter_out,
    uint32_t filter_out_capacity, int allow_filter) {
    prdb_sdk_optimizer_plan_v1 decoded;
    prdb_sdk_optimizer_filter_v1 decoded_filter = {0};
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
    uint8_t has_filter = 0;

    if (payload == NULL || out == NULL || out_capacity < sizeof(*out) ||
        (allow_filter &&
         (filter_out == NULL ||
          filter_out_capacity < sizeof(*filter_out) ||
          ranges_overlap(out, sizeof(*out), filter_out,
                         sizeof(*filter_out)))) ||
        ranges_overlap(payload, payload_len, out, sizeof(*out))) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    if (allow_filter && ranges_overlap(payload, payload_len, filter_out,
                                       sizeof(*filter_out))) {
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
        uint64_t predicate_start = reader.offset;
        uint32_t predicate_nodes = 0;
        predicate_type predicate_type_out;
        if (!allow_filter) return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
        if (!decode_predicate(&reader, decoded.scan_columns,
                              decoded.scan_column_count, 1, &predicate_nodes,
                              &predicate_type_out) ||
            !predicate_boolean(&predicate_type_out)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        has_filter = 1;
        decoded_filter.present = 1;
        decoded_filter.prefix = payload + predicate_start;
        decoded_filter.prefix_len =
            (uint32_t)(reader.offset - predicate_start);
        if (!read_u8(&reader, &node_tag)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
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

    if (node_count == (uint8_t)(3 + has_filter)) {
        if (!read_u8(&reader, &node_tag) || node_tag != 4 ||
            !read_u32(&reader, &limit)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        has_limit = 1;
    }
    if (node_count != (uint8_t)(2 + has_filter + has_limit) ||
        reader.offset != reader.length) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    decoded.struct_size = sizeof(decoded);
    decoded.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    decoded.has_limit = has_limit;
    decoded.limit = limit;
    *out = decoded;
    if (allow_filter) {
        decoded_filter.struct_size = sizeof(decoded_filter);
        decoded_filter.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
        *filter_out = decoded_filter;
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_decode_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_plan_v1 *out, uint32_t out_capacity) {
    return decode_plan_internal(payload, payload_len, out, out_capacity, NULL,
                                0, 0);
}

int32_t prdb_sdk_optimizer_decode_plan_with_filter_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_plan_v1 *out, uint32_t out_capacity,
    prdb_sdk_optimizer_filter_v1 *filter_out,
    uint32_t filter_out_capacity) {
    return decode_plan_internal(payload, payload_len, out, out_capacity,
                                filter_out, filter_out_capacity, 1);
}

static uint16_t response_u16(const uint8_t *bytes, uint64_t offset) {
    return (uint16_t)(((uint16_t)bytes[offset] << 8) | bytes[offset + 1]);
}

static uint32_t response_u32(const uint8_t *bytes, uint64_t offset) {
    return ((uint32_t)bytes[offset] << 24) |
           ((uint32_t)bytes[offset + 1] << 16) |
           ((uint32_t)bytes[offset + 2] << 8) | bytes[offset + 3];
}

static uint64_t response_u64(const uint8_t *bytes, uint64_t offset) {
    return ((uint64_t)response_u32(bytes, offset) << 32) |
           response_u32(bytes, offset + 4);
}

static int response_identifier(const uint8_t *bytes, uint32_t length) {
    return valid_identifier(bytes, length);
}

int32_t prdb_sdk_optimizer_validate_executor_relation_v1(
    const prdb_host_api_v2 *host, const uint8_t *plan_payload,
    uint64_t plan_payload_len, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len) {
    prdb_sdk_optimizer_plan_v1 plan = {0};
    prdb_sdk_optimizer_filter_v1 filter = {0};
    uint8_t request[6 + 2 * 63];
    uint64_t offset;
    uint32_t schema_len;
    uint32_t relation_len;
    uint32_t column_count;
    uint32_t scan_index = 0;
    int32_t callback_status;
    int32_t plan_status;

    if (host == NULL || host->struct_size < sizeof(*host) ||
        host->abi_version != PRDB_QUERY_ABI_V2_VERSION ||
        host->executor_request == NULL || plan_payload == NULL ||
        response == NULL || response_len == NULL ||
        response_capacity > PRDB_QUERY_MAX_IPC_RESPONSE_BYTES ||
        response_capacity < 3 ||
        plan_payload_len > PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES ||
        ranges_overlap(plan_payload, plan_payload_len, response,
                       response_capacity)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    plan_status = prdb_sdk_optimizer_decode_plan_with_filter_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan), &filter,
        sizeof(filter));
    if (plan_status != PRDB_SDK_OPTIMIZER_PLAN_OK) return plan_status;

    schema_len = plan.schema_name_len;
    relation_len = plan.relation_name_len;
    request[0] = 0;
    request[1] = 1;
    request[2] = (uint8_t)(schema_len >> 8);
    request[3] = (uint8_t)schema_len;
    request[4] = (uint8_t)(relation_len >> 8);
    request[5] = (uint8_t)relation_len;
    memcpy(request + 6, plan.schema_name, schema_len);
    memcpy(request + 6 + schema_len, plan.relation_name, relation_len);
    *response_len = 0;
    callback_status = host->executor_request(
        host->context, PRDB_IPC_OP_RESOLVE_PLAN_RELATION, request,
        6 + schema_len + relation_len, response, response_capacity,
        response_len);
    if (callback_status == PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL) {
        return PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL;
    }
    if (callback_status != PRDB_QUERY_CALLBACK_OK ||
        *response_len > response_capacity || *response_len < 3) {
        return PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
    }
    if (response_u16(response, 0) != 1) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (response[2] == 1 && *response_len == 3) {
        return PRDB_SDK_OPTIMIZER_PLAN_UNAVAILABLE;
    }
    if (response[2] != 0 || *response_len < 55) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (memcmp(response + 3, plan.schema_id, 16) != 0 ||
        response_u64(response, 19) != plan.schema_version ||
        response_u64(response, 27) != plan.table_epoch ||
        memcmp(response + 35, plan.relation_id, 16) != 0) {
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    offset = 51;
    if (offset + 2 > *response_len) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    relation_len = response_u16(response, offset);
    offset += 2;
    if (relation_len == 0 || relation_len > 63 ||
        offset + relation_len + 2 > *response_len ||
        !response_identifier(response + offset, relation_len)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (relation_len != plan.relation_name_len ||
        memcmp(response + offset, plan.relation_name, relation_len) != 0) {
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    offset += relation_len;
    column_count = response_u16(response, offset);
    offset += 2;
    if (column_count == 0 || column_count > 4096) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    for (uint32_t ordinal = 0; ordinal < column_count; ++ordinal) {
        uint32_t column_ordinal;
        uint8_t nullable;
        uint8_t type_code;
        const uint8_t *column_id;
        const uint8_t *parameters;
        uint32_t column_name_len;
        if (offset + 28 > *response_len) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        column_id = response + offset;
        if (all_zero(column_id, 16)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        offset += 16;
        column_ordinal = response_u32(response, offset);
        offset += 4;
        nullable = response[offset++];
        type_code = response[offset++];
        parameters = response + offset;
        offset += 4;
        if (column_ordinal != ordinal || nullable > 1 ||
            !valid_type(type_code, parameters) || offset + 2 > *response_len) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        column_name_len = response_u16(response, offset);
        offset += 2;
        if (column_name_len == 0 || column_name_len > 63 ||
            offset + column_name_len > *response_len ||
            !response_identifier(response + offset, column_name_len)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        offset += column_name_len;
        if (scan_index < plan.scan_column_count &&
            plan.scan_columns[scan_index].ordinal < ordinal) {
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        }
        if (scan_index < plan.scan_column_count &&
            plan.scan_columns[scan_index].ordinal == ordinal) {
            const prdb_sdk_optimizer_plan_column_v1 *expected =
                &plan.scan_columns[scan_index++];
            if (memcmp(column_id, expected->column_id, 16) != 0 ||
                type_code != expected->type_code || nullable != expected->nullable ||
                memcmp(parameters, expected->type_parameters, 4) != 0) {
                return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
            }
        }
    }
    if (offset != *response_len) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (scan_index != plan.scan_column_count) {
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}
