#include "optimizer_plan_codec_internal.h"

PRDB_SDK_INTERNAL int prdb_sdk_internal_ranges_overlap(const void *left, uint64_t left_len,
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

PRDB_SDK_INTERNAL int prdb_sdk_internal_take(plan_reader *reader, uint64_t count, const uint8_t **out) {
    if (count > reader->length - reader->offset) {
        return 0;
    }
    *out = reader->bytes + reader->offset;
    reader->offset += count;
    return 1;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_read_u8(plan_reader *reader, uint8_t *out) {
    const uint8_t *bytes;
    if (!prdb_sdk_internal_take(reader, 1, &bytes)) return 0;
    *out = bytes[0];
    return 1;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_read_text_predicate_header(plan_reader *reader,
                                      uint64_t header_offset,
                                      uint16_t *text_length) {
    const uint8_t *ignored;
    uint8_t node_tag;
    if (!prdb_sdk_internal_take(reader, 2, &ignored) ||
        !prdb_decode_optimizer_plan_text_predicate_header_record(
            reader->bytes + header_offset,
            PRDB_OPTIMIZER_PLAN_TEXT_PREDICATE_HEADER_RECORD_HEADER_BYTES,
            &node_tag, text_length)) {
        return 0;
    }
    return node_tag == 0x03;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_read_bytea_predicate_header(plan_reader *reader,
                                       uint64_t header_offset,
                                       uint16_t *bytea_length) {
    const uint8_t *ignored;
    if (header_offset >= reader->length ||
        reader->bytes[header_offset] != 0x07 ||
        reader->offset != header_offset + 1 || !prdb_sdk_internal_take(reader, 2, &ignored))
        return 0;
    *bytea_length = ((uint16_t)ignored[0] << 8) | ignored[1];
    return 1;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_read_integer_predicate_record(plan_reader *reader,
                                         uint64_t header_offset,
                                         int64_t *integer_value) {
    const uint8_t *ignored;
    uint8_t node_tag;
    if (!prdb_sdk_internal_take(reader,
              PRDB_OPTIMIZER_PLAN_INTEGER_PREDICATE_RECORD_HEADER_BYTES - 1,
              &ignored) ||
        !prdb_decode_optimizer_plan_integer_predicate_record(
            reader->bytes + header_offset,
            PRDB_OPTIMIZER_PLAN_INTEGER_PREDICATE_RECORD_HEADER_BYTES,
            &node_tag, integer_value)) {
        return 0;
    }
    return node_tag == 0x02;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_read_date_predicate_record(plan_reader *reader,
                                      uint64_t header_offset,
                                      const uint8_t **date_bytes) {
    const uint8_t *ignored;
    uint32_t raw_days;
    int64_t days;
    if (header_offset >= reader->length ||
        reader->bytes[header_offset] != 0x04 ||
        reader->offset != header_offset + 1 ||
        !prdb_sdk_internal_take(reader, 4, &ignored)) return 0;
    raw_days = ((uint32_t)ignored[0] << 24) |
               ((uint32_t)ignored[1] << 16) |
               ((uint32_t)ignored[2] << 8) | ignored[3];
    days = raw_days <= INT32_MAX
               ? (int64_t)raw_days
               : (int64_t)raw_days - INT64_C(4294967296);
    if (days < -719162 || days > 2932896) return 0;
    *date_bytes = ignored;
    return 1;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_read_uuid_predicate_record(plan_reader *reader,
                                      uint64_t header_offset,
                                      const uint8_t **uuid_bytes) {
    if (header_offset >= reader->length ||
        reader->bytes[header_offset] != 0x05 ||
        reader->offset != header_offset + 1 ||
        !prdb_sdk_internal_take(reader, 16, uuid_bytes)) return 0;
    return 1;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_read_timestamptz_predicate_record(plan_reader *reader,
                                             uint64_t header_offset,
                                             const uint8_t **timestamp_bytes) {
    static const uint8_t minimum[8] = {0xff, 0x23, 0x40, 0x01,
                                       0x00, 0xd4, 0x40, 0x00};
    static const uint8_t maximum[8] = {0x03, 0x84, 0x44, 0x0c,
                                       0xcc, 0x73, 0x5f, 0xff};
    const uint8_t *bytes;
    if (header_offset >= reader->length ||
        reader->bytes[header_offset] != 0x06 ||
        reader->offset != header_offset + 1 || !prdb_sdk_internal_take(reader, 8, &bytes))
        return 0;
    if ((bytes[0] & 0x80) != 0) {
        if (memcmp(bytes, minimum, 8) < 0) return 0;
    } else if (memcmp(bytes, maximum, 8) > 0) {
        return 0;
    }
    *timestamp_bytes = bytes;
    return 1;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_read_decimal_predicate_record(plan_reader *reader,
                                         uint64_t header_offset,
                                         const uint8_t **coefficient) {
    if (header_offset >= reader->length ||
        reader->bytes[header_offset] != 0x08 ||
        reader->offset != header_offset + 1 || !prdb_sdk_internal_take(reader, 16, coefficient))
        return 0;
    return 1;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_decimal_coefficient_fits_precision(const uint8_t coefficient[16],
                                               uint8_t precision) {
    uint8_t magnitude[16];
    uint8_t maximum[16] = {0};
    uint16_t carry;
    if (precision == 0 || precision > 38) return 0;
    memcpy(magnitude, coefficient, sizeof(magnitude));
    if ((magnitude[0] & 0x80) != 0) {
        carry = 1;
        for (int index = 15; index >= 0; --index) {
            uint16_t value = (uint16_t)(uint8_t)~magnitude[index] + carry;
            magnitude[index] = (uint8_t)value;
            carry = (uint16_t)(value >> 8);
        }
    }
    for (uint8_t digit = 0; digit < precision; ++digit) {
        carry = 9;
        for (int index = 15; index >= 0; --index) {
            uint16_t value = (uint16_t)maximum[index] * 10 + carry;
            maximum[index] = (uint8_t)value;
            carry = (uint16_t)(value >> 8);
        }
        if (carry != 0) return 0;
    }
    return memcmp(magnitude, maximum, sizeof(magnitude)) <= 0;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_read_column_predicate_record(plan_reader *reader,
                                        uint64_t header_offset,
                                        uint8_t column_id[16]) {
    const uint8_t *ignored;
    uint8_t node_tag;
    if (!prdb_sdk_internal_take(reader,
              PRDB_OPTIMIZER_PLAN_COLUMN_PREDICATE_RECORD_HEADER_BYTES - 1,
              &ignored) ||
        !prdb_decode_optimizer_plan_column_predicate_record(
            reader->bytes + header_offset,
            PRDB_OPTIMIZER_PLAN_COLUMN_PREDICATE_RECORD_HEADER_BYTES,
            &node_tag, column_id)) {
        return 0;
    }
    return node_tag == 0x10;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_read_compare_predicate_header(plan_reader *reader,
                                        uint64_t header_offset,
                                        uint8_t *comparison_operator) {
    const uint8_t *ignored;
    uint8_t node_tag;
    if (!prdb_sdk_internal_take(reader,
              PRDB_OPTIMIZER_PLAN_COMPARE_PREDICATE_HEADER_RECORD_HEADER_BYTES - 1,
              &ignored) ||
        !prdb_decode_optimizer_plan_compare_predicate_header_record(
            reader->bytes + header_offset,
            PRDB_OPTIMIZER_PLAN_COMPARE_PREDICATE_HEADER_RECORD_HEADER_BYTES,
            &node_tag, comparison_operator)) {
        return 0;
    }
    return node_tag == 0x20;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_read_boolean_predicate_header(plan_reader *reader,
                                         uint64_t header_offset,
                                         uint8_t *node_tag_out,
                                         uint8_t *child_count) {
    const uint8_t *ignored;
    uint8_t node_tag;
    if (!prdb_sdk_internal_take(reader,
              PRDB_OPTIMIZER_PLAN_BOOLEAN_PREDICATE_HEADER_RECORD_HEADER_BYTES - 1,
              &ignored) ||
        !prdb_decode_optimizer_plan_boolean_predicate_header_record(
            reader->bytes + header_offset,
            PRDB_OPTIMIZER_PLAN_BOOLEAN_PREDICATE_HEADER_RECORD_HEADER_BYTES,
            &node_tag, child_count) ||
        (node_tag != 0x30 && node_tag != 0x31)) {
        return 0;
    }
    if (node_tag_out != NULL) *node_tag_out = node_tag;
    return 1;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_read_not_predicate_tag(const plan_reader *reader,
                                  uint64_t header_offset) {
    uint8_t node_tag;
    if (header_offset > reader->length ||
        PRDB_OPTIMIZER_PLAN_NOT_PREDICATE_RECORD_HEADER_BYTES >
            reader->length - header_offset ||
        !prdb_decode_optimizer_plan_not_predicate_record(
            reader->bytes + header_offset,
            PRDB_OPTIMIZER_PLAN_NOT_PREDICATE_RECORD_HEADER_BYTES,
            &node_tag)) {
        return 0;
    }
    return node_tag == 0x32;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_read_null_predicate_tag(const plan_reader *reader,
                                   uint64_t header_offset) {
    uint8_t node_tag;
    if (header_offset > reader->length ||
        PRDB_OPTIMIZER_PLAN_NULL_PREDICATE_RECORD_HEADER_BYTES >
            reader->length - header_offset ||
        !prdb_decode_optimizer_plan_null_predicate_record(
            reader->bytes + header_offset,
            PRDB_OPTIMIZER_PLAN_NULL_PREDICATE_RECORD_HEADER_BYTES,
            &node_tag)) {
        return 0;
    }
    return node_tag == 0x00;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_read_boolean_literal_predicate_record(
    plan_reader *reader, uint64_t header_offset, uint8_t *boolean_value) {
    const uint8_t *ignored;
    uint8_t node_tag;
    if (!prdb_sdk_internal_take(reader,
              PRDB_OPTIMIZER_PLAN_BOOLEAN_LITERAL_PREDICATE_RECORD_HEADER_BYTES - 1,
              &ignored) ||
        !prdb_decode_optimizer_plan_boolean_literal_predicate_record(
            reader->bytes + header_offset,
            PRDB_OPTIMIZER_PLAN_BOOLEAN_LITERAL_PREDICATE_RECORD_HEADER_BYTES,
            &node_tag, boolean_value)) {
        return 0;
    }
    return node_tag == 0x01;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_read_column_list_header(plan_reader *reader, uint8_t expected_tag,
                                   uint16_t *column_count) {
    const uint8_t *ignored;
    uint8_t node_tag;
    uint64_t header_offset;
    if (reader->offset == 0) return 0;
    header_offset = reader->offset - 1;
    if (!prdb_sdk_internal_take(reader, 2, &ignored) ||
        !prdb_decode_optimizer_plan_column_list_header_record(
            reader->bytes + header_offset,
            PRDB_OPTIMIZER_PLAN_COLUMN_LIST_HEADER_RECORD_HEADER_BYTES,
            &node_tag, column_count)) {
        return 0;
    }
    return node_tag == expected_tag;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_read_limit_node(plan_reader *reader, uint32_t *row_limit) {
    const uint8_t *ignored;
    uint8_t node_tag;
    uint64_t header_offset;
    if (reader->offset == 0) return 0;
    header_offset = reader->offset - 1;
    if (!prdb_sdk_internal_take(reader, 4, &ignored) ||
        !prdb_decode_optimizer_plan_limit_node_record(
            reader->bytes + header_offset,
            PRDB_OPTIMIZER_PLAN_LIMIT_NODE_RECORD_HEADER_BYTES,
            &node_tag, row_limit)) {
        return 0;
    }
    return node_tag == 4;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_read_plan_node_count(plan_reader *reader, uint8_t *node_count) {
    const uint8_t *bytes;
    return prdb_sdk_internal_take(reader,
                PRDB_OPTIMIZER_PLAN_NODE_COUNT_RECORD_HEADER_BYTES, &bytes) &&
           prdb_decode_optimizer_plan_node_count_record(
               bytes, PRDB_OPTIMIZER_PLAN_NODE_COUNT_RECORD_HEADER_BYTES,
               node_count);
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_read_filter_node_tag(const plan_reader *reader,
                                uint64_t header_offset) {
    uint8_t node_tag;
    if (header_offset > reader->length ||
        PRDB_OPTIMIZER_PLAN_FILTER_NODE_RECORD_HEADER_BYTES >
            reader->length - header_offset ||
        !prdb_decode_optimizer_plan_filter_node_record(
            reader->bytes + header_offset,
            PRDB_OPTIMIZER_PLAN_FILTER_NODE_RECORD_HEADER_BYTES, &node_tag)) {
        return 0;
    }
    return node_tag == 2;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_valid_identifier(const uint8_t *bytes, uint32_t length) {
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

PRDB_SDK_INTERNAL int prdb_sdk_internal_read_name(plan_reader *reader, const uint8_t **name_out,
                     uint32_t *length_out) {
    const uint8_t *header;
    uint8_t length;
    const uint8_t *name;
    if (!prdb_sdk_internal_take(reader,
              PRDB_OPTIMIZER_PLAN_NAME_HEADER_RECORD_HEADER_BYTES, &header) ||
        !prdb_decode_optimizer_plan_name_header_record(
            header, PRDB_OPTIMIZER_PLAN_NAME_HEADER_RECORD_HEADER_BYTES,
            &length) ||
        !prdb_sdk_internal_take(reader, length, &name) ||
        !prdb_sdk_internal_valid_identifier(name, length)) {
        return 0;
    }
    *name_out = name;
    *length_out = length;
    return 1;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_all_zero(const uint8_t *bytes, uint32_t length) {
    uint32_t index;
    for (index = 0; index < length; ++index) {
        if (bytes[index] != 0) return 0;
    }
    return 1;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_valid_type(uint8_t type_code, const uint8_t parameters[4]) {
    switch (type_code) {
        case 1: case 2: case 3: case 4: case 6:
        case 8: case 9: case 10: case 11:
            return prdb_sdk_internal_all_zero(parameters, 4);
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

PRDB_SDK_INTERNAL int prdb_sdk_internal_read_column(plan_reader *reader,
                       prdb_sdk_optimizer_plan_column_v1 *column) {
    const uint8_t *bytes;
    if (!prdb_sdk_internal_take(reader, PRDB_OPTIMIZER_PLAN_COLUMN_RECORD_HEADER_BYTES, &bytes) ||
        !prdb_decode_optimizer_plan_column_record(
            bytes, PRDB_OPTIMIZER_PLAN_COLUMN_RECORD_HEADER_BYTES,
            column->column_id, &column->ordinal, &column->type_code,
            column->type_parameters, &column->nullable) ||
        prdb_sdk_internal_all_zero(column->column_id, 16) || column->nullable > 1 ||
        !prdb_sdk_internal_valid_type(column->type_code, column->type_parameters)) {
        return 0;
    }
    return 1;
}


PRDB_SDK_INTERNAL int prdb_sdk_internal_valid_utf8(const uint8_t *bytes, uint32_t length) {
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

PRDB_SDK_INTERNAL int prdb_sdk_internal_same_type(const predicate_type *left, const predicate_type *right) {
    if (left->is_null || right->is_null) return 1;
    if (left->decimal_literal || right->decimal_literal) {
        const predicate_type *literal = left->decimal_literal ? left : right;
        const predicate_type *column = left->decimal_literal ? right : left;
        return left->decimal_literal != right->decimal_literal &&
               column->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
               prdb_sdk_internal_decimal_coefficient_fits_precision(
                   literal->decimal_coefficient, column->parameters[0]);
    }
    return left->type_code == right->type_code &&
           memcmp(left->parameters, right->parameters, 4) == 0;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_predicate_boolean(const predicate_type *type) {
    return !type->is_null && type->type_code == 1 &&
           prdb_sdk_internal_all_zero(type->parameters, 4);
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_decode_predicate(plan_reader *reader,
                            const prdb_sdk_optimizer_plan_column_v1 *columns,
                            uint32_t column_count, uint32_t depth,
                            uint32_t *nodes, uint8_t *references,
                            predicate_type *type_out) {
    uint8_t tag;
    uint64_t tag_offset = reader->offset;
    const uint8_t *bytes;
    predicate_type type = {0};
    if (depth > 32 || ++*nodes > 256 || !prdb_sdk_internal_read_u8(reader, &tag)) return 0;
    switch (tag) {
        case 0x00:
            if (!prdb_sdk_internal_read_null_predicate_tag(reader, tag_offset)) return 0;
            type.is_null = 1;
            break;
        case 0x01:
            if (!prdb_sdk_internal_read_boolean_literal_predicate_record(reader, tag_offset, &tag) ||
                tag > 1) return 0;
            type.type_code = 1;
            break;
        case 0x02:
            {
                int64_t integer_value;
                if (!prdb_sdk_internal_read_integer_predicate_record(reader, tag_offset,
                                                   &integer_value)) return 0;
            }
            type.type_code = 3;
            break;
        case 0x03: {
            uint16_t text_len;
            if (!prdb_sdk_internal_read_text_predicate_header(reader, tag_offset, &text_len) ||
                text_len > 4096 ||
                !prdb_sdk_internal_take(reader, text_len, &bytes) ||
                !prdb_sdk_internal_valid_utf8(bytes, text_len)) {
                return 0;
            }
            type.type_code = 6;
            break;
        }
        case 0x04:
            if (!prdb_sdk_internal_read_date_predicate_record(reader, tag_offset, &bytes)) return 0;
            type.type_code = PRDB_SDK_VALUE_TYPE_DATE;
            break;
        case 0x05:
            if (!prdb_sdk_internal_read_uuid_predicate_record(reader, tag_offset, &bytes)) return 0;
            type.type_code = PRDB_SDK_VALUE_TYPE_UUID;
            break;
        case 0x06:
            if (!prdb_sdk_internal_read_timestamptz_predicate_record(reader, tag_offset, &bytes)) return 0;
            type.type_code = PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ;
            break;
        case 0x07: {
            uint16_t length;
            if (!prdb_sdk_internal_read_bytea_predicate_header(reader, tag_offset, &length) ||
                length > 4096 || !prdb_sdk_internal_take(reader, length, &bytes)) return 0;
            type.type_code = PRDB_SDK_VALUE_TYPE_BYTEA;
            break;
        }
        case 0x08: {
            const uint8_t *coefficient;
            if (!prdb_sdk_internal_read_decimal_predicate_record(reader, tag_offset, &coefficient))
                return 0;
            type.type_code = PRDB_SDK_VALUE_TYPE_DECIMAL;
            type.decimal_literal = 1;
            memcpy(type.decimal_coefficient, coefficient, 16);
            break;
        }
        case 0x10: {
            uint32_t index;
            int found = 0;
            uint8_t column_id[16];
            if (!prdb_sdk_internal_read_column_predicate_record(reader, tag_offset, column_id))
                return 0;
            for (index = 0; index < column_count; ++index) {
                if (memcmp(column_id, columns[index].column_id, 16) == 0) {
                    type.type_code = columns[index].type_code;
                    memcpy(type.parameters,
                           columns[index].type_parameters, 4);
                    if (references != NULL) references[index] = 1;
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
            if (!prdb_sdk_internal_read_compare_predicate_header(reader, tag_offset, &comparison) ||
                comparison > 5 ||
                !prdb_sdk_internal_decode_predicate(reader, columns, column_count, depth + 1,
                                  nodes, references, &left) ||
                !prdb_sdk_internal_decode_predicate(reader, columns, column_count, depth + 1,
                                  nodes, references, &right) ||
                !prdb_sdk_internal_same_type(&left, &right)) {
                return 0;
            }
            type.type_code = 1;
            break;
        }
        case 0x30: case 0x31: {
            uint8_t node_tag;
            uint8_t child_count;
            uint32_t index;
            if (!prdb_sdk_internal_read_boolean_predicate_header(reader, tag_offset, &node_tag,
                                               &child_count) || node_tag != tag ||
                child_count < 2 ||
                child_count > 16) {
                return 0;
            }
            for (index = 0; index < child_count; ++index) {
                predicate_type child;
                if (!prdb_sdk_internal_decode_predicate(reader, columns, column_count,
                                      depth + 1, nodes, references, &child) ||
                    !prdb_sdk_internal_predicate_boolean(&child)) {
                    return 0;
                }
            }
            type.type_code = 1;
            break;
        }
        case 0x32: {
            predicate_type child;
            if (!prdb_sdk_internal_read_not_predicate_tag(reader, tag_offset) ||
                !prdb_sdk_internal_decode_predicate(reader, columns, column_count, depth + 1,
                                  nodes, references, &child) ||
                !prdb_sdk_internal_predicate_boolean(&child)) {
                return 0;
            }
            type.type_code = 1;
            break;
        }
        case 0x40: case 0x41: {
            predicate_type child;
            if (reader->offset >= reader->length ||
                reader->bytes[reader->offset] != 0x10 ||
                !prdb_sdk_internal_decode_predicate(reader, columns, column_count, depth + 1,
                                  nodes, references, &child)) {
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

static int read_point_probe_leaf(plan_reader *reader, uint8_t column_id[16],
                                 const uint8_t **literal,
                                 uint32_t *literal_len) {
    uint8_t tag;
    uint64_t start = reader->offset;
    *literal = NULL;
    *literal_len = 0;
    if (!prdb_sdk_internal_read_u8(reader, &tag)) return 0;
    if (tag == 0x10) {
        return prdb_sdk_internal_read_column_predicate_record(reader, start, column_id);
    }
    if (tag == 0x01) {
        uint8_t value;
        if (!prdb_sdk_internal_read_boolean_literal_predicate_record(reader, start, &value))
            return 0;
    } else if (tag == 0x02) {
        int64_t value;
        if (!prdb_sdk_internal_read_integer_predicate_record(reader, start, &value)) return 0;
    } else if (tag == 0x03) {
        uint16_t length;
        const uint8_t *bytes;
        if (!prdb_sdk_internal_read_text_predicate_header(reader, start, &length) ||
            length > 4096 || !prdb_sdk_internal_take(reader, length, &bytes) ||
            !prdb_sdk_internal_valid_utf8(bytes, length)) return 0;
    } else if (tag == 0x04) {
        const uint8_t *date_bytes;
        if (!prdb_sdk_internal_read_date_predicate_record(reader, start, &date_bytes)) return 0;
    } else if (tag == 0x05) {
        const uint8_t *uuid_bytes;
        if (!prdb_sdk_internal_read_uuid_predicate_record(reader, start, &uuid_bytes)) return 0;
    } else if (tag == 0x06) {
        const uint8_t *timestamp_bytes;
        if (!prdb_sdk_internal_read_timestamptz_predicate_record(reader, start, &timestamp_bytes)) return 0;
    } else if (tag == 0x07) {
        uint16_t length;
        const uint8_t *bytea_bytes;
        if (!prdb_sdk_internal_read_bytea_predicate_header(reader, start, &length) ||
            length > 4096 || !prdb_sdk_internal_take(reader, length, &bytea_bytes)) return 0;
    } else if (tag == 0x08) {
        const uint8_t *coefficient;
        if (!prdb_sdk_internal_read_decimal_predicate_record(reader, start, &coefficient)) return 0;
    } else if (tag == 0x00) {
        if (!prdb_sdk_internal_read_null_predicate_tag(reader, start)) return 0;
        *literal = reader->bytes + start;
        *literal_len = 0;
        return 1;
    } else {
        return 0;
    }
    *literal = reader->bytes + start;
    *literal_len = (uint32_t)(reader->offset - start);
    return 1;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_point_probe_matches_filter(
    const prdb_sdk_optimizer_filter_v1 *filter,
    const prdb_sdk_optimizer_plan_column_v1 *key_column,
    const uint8_t *literal, uint32_t literal_len) {
    plan_reader reader;
    uint8_t tag;
    uint8_t comparison;
    uint8_t left_column[16] = {0};
    uint8_t right_column[16] = {0};
    const uint8_t *left_literal = NULL;
    const uint8_t *right_literal = NULL;
    uint32_t left_literal_len = 0;
    uint32_t right_literal_len = 0;
    int left_is_column;
    int right_is_column;
    if (filter == NULL || filter->present == 0 || filter->prefix == NULL ||
        literal == NULL) return 0;
    reader.bytes = filter->prefix;
    reader.length = filter->prefix_len;
    reader.offset = 0;
    uint64_t tag_offset = reader.offset;
    if (!prdb_sdk_internal_read_u8(&reader, &tag) || tag != 0x20 ||
        !prdb_sdk_internal_read_compare_predicate_header(&reader, tag_offset, &comparison) ||
        comparison != 0 ||
        !read_point_probe_leaf(&reader, left_column, &left_literal,
                               &left_literal_len) ||
        !read_point_probe_leaf(&reader, right_column, &right_literal,
                               &right_literal_len) ||
        reader.offset != reader.length) return 0;
    left_is_column = left_literal == NULL;
    right_is_column = right_literal == NULL;
    return (left_is_column && !right_is_column &&
            memcmp(left_column, key_column->column_id, 16) == 0 &&
            right_literal_len == literal_len &&
            memcmp(right_literal, literal, literal_len) == 0) ||
           (right_is_column && !left_is_column &&
            memcmp(right_column, key_column->column_id, 16) == 0 &&
            left_literal_len == literal_len &&
            memcmp(left_literal, literal, literal_len) == 0);
}

static int multi_point_filter_collect(
    plan_reader *reader, const prdb_sdk_optimizer_plan_column_v1 *key_column,
    const multi_point_probe_hint *hint, uint8_t seen[64], uint16_t *found) {
    uint8_t tag;
    uint64_t start = reader->offset;
    if (!prdb_sdk_internal_read_u8(reader, &tag)) return 0;
    if (tag == 0x31) {
        uint8_t node_tag;
        uint8_t child_count;
        if (!prdb_sdk_internal_read_boolean_predicate_header(reader, start, &node_tag, &child_count) ||
            node_tag != 0x31 || child_count < 2 || child_count > 16) return 0;
        for (uint8_t i = 0; i < child_count; ++i) {
            if (!multi_point_filter_collect(reader, key_column, hint, seen, found)) return 0;
        }
        return 1;
    }
    if (tag == 0x20) {
        uint8_t comparison;
        uint8_t left_column[16] = {0};
        uint8_t right_column[16] = {0};
        const uint8_t *left_literal = NULL;
        const uint8_t *right_literal = NULL;
        uint32_t left_len = 0, right_len = 0;
        int left_is_column, right_is_column, matched = 0;
        if (!prdb_sdk_internal_read_compare_predicate_header(reader, start, &comparison) || comparison != 0 ||
            !read_point_probe_leaf(reader, left_column, &left_literal, &left_len) ||
            !read_point_probe_leaf(reader, right_column, &right_literal, &right_len)) return 0;
        left_is_column = left_literal == NULL;
        right_is_column = right_literal == NULL;
        if (left_is_column && !right_is_column &&
            memcmp(left_column, key_column->column_id, 16) == 0) {
            if (right_len == 0) return 1;
            for (uint16_t i = 0; i < hint->count; ++i)
                if (hint->literal_lengths[i] == right_len &&
                    memcmp(hint->literals[i], right_literal, right_len) == 0) {
                    if (seen[i] == 0) { seen[i] = 1; ++*found; }
                    matched = 1;
                }
        } else if (right_is_column && !left_is_column &&
                   memcmp(right_column, key_column->column_id, 16) == 0) {
            if (left_len == 0) return 1;
            for (uint16_t i = 0; i < hint->count; ++i)
                if (hint->literal_lengths[i] == left_len &&
                    memcmp(hint->literals[i], left_literal, left_len) == 0) {
                    if (seen[i] == 0) { seen[i] = 1; ++*found; }
                    matched = 1;
                }
        }
        if (!matched) return 0;
        return 1;
    }
    return 0;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_multi_point_probe_matches_filter(
    const prdb_sdk_optimizer_filter_v1 *filter,
    const prdb_sdk_optimizer_plan_column_v1 *key_column,
    const multi_point_probe_hint *hint) {
    plan_reader reader;
    uint16_t found = 0;
    uint8_t seen[64] = {0};
    if (filter == NULL || filter->present == 0 || filter->prefix == NULL || hint == NULL)
        return 0;
    reader.bytes = filter->prefix;
    reader.length = filter->prefix_len;
    reader.offset = 0;
    return multi_point_filter_collect(&reader, key_column, hint, seen, &found) &&
           reader.offset == reader.length && found == hint->count;
}

static int range_comparison_matches_bound(
    plan_reader *reader,
    const prdb_sdk_optimizer_plan_column_v1 *key_column,
    const range_probe_hint *hint, int *is_lower_out) {
    uint8_t tag;
    uint8_t comparison;
    uint8_t left_column[16] = {0};
    uint8_t right_column[16] = {0};
    const uint8_t *left_literal = NULL;
    const uint8_t *right_literal = NULL;
    uint32_t left_literal_len = 0;
    uint32_t right_literal_len = 0;
    int left_is_column;
    int right_is_column;
    const uint8_t *expected_literal;
    uint32_t expected_literal_len;
    uint8_t expected_inclusive;
    int expected_lower;
    uint64_t tag_offset = reader->offset;
    if (!prdb_sdk_internal_read_u8(reader, &tag) || tag != 0x20 ||
        !prdb_sdk_internal_read_compare_predicate_header(reader, tag_offset, &comparison) ||
        comparison > 5 || comparison == 1 ||
        !read_point_probe_leaf(reader, left_column, &left_literal,
                               &left_literal_len) ||
        !read_point_probe_leaf(reader, right_column, &right_literal,
                               &right_literal_len)) return 0;
    left_is_column = left_literal == NULL;
    right_is_column = right_literal == NULL;
    if (left_is_column == right_is_column) return 0;
    if (left_is_column) {
        if (memcmp(left_column, key_column->column_id, 16) != 0) return 0;
        expected_literal = right_literal;
        expected_literal_len = right_literal_len;
        expected_lower = comparison >= 4;
    } else {
        if (memcmp(right_column, key_column->column_id, 16) != 0) return 0;
        expected_literal = left_literal;
        expected_literal_len = left_literal_len;
        if (comparison != 0) {
            comparison = comparison == 2 ? 4 : comparison == 3 ? 5 :
                         comparison == 4 ? 2 : 3;
        }
        expected_lower = comparison >= 4;
    }
    if (comparison == 0) {
        *is_lower_out = 2;
        return hint->lower_literal != NULL && hint->upper_literal != NULL &&
               hint->lower_inclusive != 0 && hint->upper_inclusive != 0 &&
               hint->lower_literal_len == expected_literal_len &&
               hint->upper_literal_len == expected_literal_len &&
               memcmp(hint->lower_literal, expected_literal,
                      expected_literal_len) == 0 &&
               memcmp(hint->upper_literal, expected_literal,
                      expected_literal_len) == 0;
    }
    expected_inclusive = (uint8_t)(comparison == 3 || comparison == 5);
    *is_lower_out = expected_lower;
    if (expected_lower) {
        return hint->lower_literal != NULL &&
               hint->lower_inclusive == expected_inclusive &&
               hint->lower_literal_len == expected_literal_len &&
               memcmp(hint->lower_literal, expected_literal,
                      expected_literal_len) == 0;
    }
    return hint->upper_literal != NULL &&
           hint->upper_inclusive == expected_inclusive &&
           hint->upper_literal_len == expected_literal_len &&
           memcmp(hint->upper_literal, expected_literal,
                  expected_literal_len) == 0;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_range_probe_matches_filter(
    const prdb_sdk_optimizer_filter_v1 *filter,
    const prdb_sdk_optimizer_plan_column_v1 *key_column,
    const range_probe_hint *hint) {
    plan_reader reader;
    uint8_t tag;
    if (filter == NULL || filter->present == 0 || filter->prefix == NULL ||
        hint == NULL || hint->present == 0) return 0;
    reader.bytes = filter->prefix;
    reader.length = filter->prefix_len;
    reader.offset = 0;
    uint64_t tag_offset = reader.offset;
    if (!prdb_sdk_internal_read_u8(&reader, &tag)) return 0;
    if (tag == 0x20) {
        int is_lower = 0;
        reader.offset = 0;
        if (!range_comparison_matches_bound(&reader, key_column, hint,
                                            &is_lower) ||
            reader.offset != reader.length) return 0;
        if (is_lower == 2) return 1;
        return is_lower ? hint->upper_literal == NULL
                        : hint->lower_literal == NULL;
    }
    if (tag == 0x30) {
        uint8_t node_tag;
        uint8_t child_count;
        int saw_lower = 0;
        int saw_upper = 0;
        if (!prdb_sdk_internal_read_boolean_predicate_header(&reader, tag_offset, &node_tag,
                                           &child_count) ||
            node_tag != 0x30 || child_count != 2) return 0;
        for (uint32_t child = 0; child < child_count; ++child) {
            int is_lower = 0;
            if (!range_comparison_matches_bound(&reader, key_column, hint,
                                                &is_lower) || is_lower > 1)
                return 0;
            if (is_lower) {
                if (saw_lower) return 0;
                saw_lower = 1;
            } else {
                if (saw_upper) return 0;
                saw_upper = 1;
            }
        }
        return saw_lower && saw_upper && reader.offset == reader.length &&
               hint->lower_literal != NULL && hint->upper_literal != NULL;
    }
    return 0;
}

static uint16_t point_read_u16_be(const uint8_t *bytes) {
    return (uint16_t)(((uint16_t)bytes[0] << 8) | bytes[1]);
}

static void write_u32_be(uint8_t *bytes, uint32_t value) {
    bytes[0] = (uint8_t)(value >> 24);
    bytes[1] = (uint8_t)(value >> 16);
    bytes[2] = (uint8_t)(value >> 8);
    bytes[3] = (uint8_t)value;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_encode_point_probe_request(
    const prdb_sdk_optimizer_plan_v1 *plan,
    const prdb_sdk_executor_relation_v1 *relation,
    const point_probe_hint *hint, const uint8_t *relation_response,
    uint64_t relation_response_len, uint8_t *request, uint64_t capacity,
    uint64_t *request_len) {
    uint64_t offset = PRDB_EXECUTOR_PROBE_UNIQUE_INDEX_REQUEST_HEADER_BYTES;
    uint8_t type_code;
    const uint8_t *value_bytes = NULL;
    uint32_t value_len = 0;
    uint8_t integer_bytes[4];
    if (plan == NULL || relation == NULL || hint == NULL ||
        hint->present == 0 || request == NULL || request_len == NULL ||
        plan->scan_column_count > PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS ||
        hint->literal == NULL || hint->literal_len == 0 ||
        relation_response == NULL || relation_response_len < 44 ||
        relation->column_count > PRDB_SDK_BATCH_MAX_CELLS_PER_ROW) return 0;
    switch (hint->literal[0]) {
        case 0x01:
            if (hint->literal_len != 2 || hint->literal[1] > 1 ||
                hint->key_column.type_code != 1) return 0;
            type_code = 1;
            value_bytes = hint->literal + 1;
            value_len = 1;
            break;
        case 0x02: {
            int64_t value;
            plan_reader literal_reader = {hint->literal,
                                          hint->literal_len, 0};
            uint8_t tag;
            if (!prdb_sdk_internal_read_u8(&literal_reader, &tag) || tag != 0x02 ||
                !prdb_sdk_internal_read_integer_predicate_record(&literal_reader, 0, &value) ||
                literal_reader.offset != literal_reader.length ||
                value < INT32_MIN || value > INT32_MAX ||
                hint->key_column.type_code != 3) return 0;
            write_u32_be(integer_bytes, (uint32_t)(int32_t)value);
            type_code = 3;
            value_bytes = integer_bytes;
            value_len = sizeof(integer_bytes);
            break;
        }
        case 0x03: {
            uint16_t text_len;
            plan_reader literal_reader = {hint->literal,
                                          hint->literal_len, 0};
            const uint8_t *text;
            uint8_t tag;
            if (!prdb_sdk_internal_read_u8(&literal_reader, &tag) || tag != 0x03 ||
                !prdb_sdk_internal_read_text_predicate_header(&literal_reader, 0, &text_len) ||
                text_len > 4096 || !prdb_sdk_internal_take(&literal_reader, text_len, &text) ||
                literal_reader.offset != literal_reader.length ||
                hint->key_column.type_code != 6) return 0;
            type_code = 6;
            value_bytes = text;
            value_len = text_len;
            break;
        }
        case 0x04: {
            const uint8_t *date_bytes;
            plan_reader literal_reader = {hint->literal,
                                          hint->literal_len, 0};
            uint8_t tag;
            if (!prdb_sdk_internal_read_u8(&literal_reader, &tag) || tag != 0x04 ||
                !prdb_sdk_internal_read_date_predicate_record(&literal_reader, 0, &date_bytes) ||
                literal_reader.offset != literal_reader.length ||
                hint->key_column.type_code != PRDB_SDK_VALUE_TYPE_DATE)
                return 0;
            type_code = PRDB_SDK_VALUE_TYPE_DATE;
            value_bytes = date_bytes;
            value_len = 4;
            break;
        }
        case 0x05: {
            const uint8_t *uuid_bytes;
            plan_reader literal_reader = {hint->literal,
                                          hint->literal_len, 0};
            uint8_t tag;
            if (!prdb_sdk_internal_read_u8(&literal_reader, &tag) || tag != 0x05 ||
                !prdb_sdk_internal_read_uuid_predicate_record(&literal_reader, 0, &uuid_bytes) ||
                literal_reader.offset != literal_reader.length ||
                hint->key_column.type_code != PRDB_SDK_VALUE_TYPE_UUID)
                return 0;
            type_code = PRDB_SDK_VALUE_TYPE_UUID;
            value_bytes = uuid_bytes;
            value_len = 16;
            break;
        }
        case 0x06: {
            const uint8_t *timestamp_bytes;
            plan_reader literal_reader = {hint->literal,
                                          hint->literal_len, 0};
            uint8_t tag;
            if (!prdb_sdk_internal_read_u8(&literal_reader, &tag) || tag != 0x06 ||
                !prdb_sdk_internal_read_timestamptz_predicate_record(&literal_reader, 0,
                                                   &timestamp_bytes) ||
                literal_reader.offset != literal_reader.length ||
                hint->key_column.type_code != PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ)
                return 0;
            type_code = PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ;
            value_bytes = timestamp_bytes;
            value_len = 8;
            break;
        }
        case 0x07: {
            uint16_t bytea_length;
            const uint8_t *bytea_bytes;
            plan_reader literal_reader = {hint->literal, hint->literal_len, 0};
            uint8_t tag;
            if (!prdb_sdk_internal_read_u8(&literal_reader, &tag) || tag != 0x07 ||
                !prdb_sdk_internal_read_bytea_predicate_header(&literal_reader, 0, &bytea_length) ||
                bytea_length > 4096 ||
                !prdb_sdk_internal_take(&literal_reader, bytea_length, &bytea_bytes) ||
                literal_reader.offset != literal_reader.length ||
                hint->key_column.type_code != PRDB_SDK_VALUE_TYPE_BYTEA)
                return 0;
            type_code = PRDB_SDK_VALUE_TYPE_BYTEA;
            value_bytes = bytea_bytes;
            value_len = bytea_length;
            break;
        }
        case 0x08: {
            const uint8_t *coefficient;
            plan_reader literal_reader = {hint->literal, hint->literal_len, 0};
            uint8_t tag;
            if (!prdb_sdk_internal_read_u8(&literal_reader, &tag) || tag != 0x08 ||
                !prdb_sdk_internal_read_decimal_predicate_record(&literal_reader, 0, &coefficient) ||
                literal_reader.offset != literal_reader.length ||
                hint->key_column.type_code != PRDB_SDK_VALUE_TYPE_DECIMAL ||
                !prdb_sdk_internal_decimal_coefficient_fits_precision(
                    coefficient, hint->key_column.type_parameters[0]))
                return 0;
            type_code = PRDB_SDK_VALUE_TYPE_DECIMAL;
            value_bytes = coefficient;
            value_len = 16;
            break;
        }
        default:
            return 0;
    }
    if (capacity < offset + PRDB_SCALAR_VALUE_RECORD_HEADER_BYTES + value_len +
                       (uint64_t)relation->column_count * 16) return 0;
    prdb_encode_executor_probe_unique_index_request(
        request, relation->executor_handle, hint->index_id, 1,
        (uint16_t)relation->column_count);
    uint8_t value_header[PRDB_SCALAR_VALUE_RECORD_HEADER_BYTES] = {0};
    prdb_encode_scalar_value_record(value_header, type_code, value_len);
    memcpy(request + offset, value_header, sizeof(value_header));
    offset += sizeof(value_header);
    if (!prdb_encode_scalar_value_record_value_bytes(
            request + offset, (size_t)(capacity - offset), value_bytes,
            value_len)) return 0;
    offset += value_len;
    uint64_t relation_offset = 35;
    uint16_t relation_name_len =
        point_read_u16_be(relation_response + relation_offset);
    relation_offset += 2 + relation_name_len;
    if (relation_offset + 2 > relation_response_len ||
        point_read_u16_be(relation_response + relation_offset) !=
            relation->column_count) return 0;
    relation_offset += 2;
    for (uint32_t index = 0; index < relation->column_count; ++index) {
        uint16_t column_name_len;
        if (relation_offset + 28 > relation_response_len) return 0;
        memcpy(request + offset, relation_response + relation_offset, 16);
        offset += 16;
        relation_offset += 26;
        column_name_len =
            point_read_u16_be(relation_response + relation_offset);
        relation_offset += 2;
        if (column_name_len > relation_response_len - relation_offset) return 0;
        relation_offset += column_name_len;
    }
    if (relation_offset != relation_response_len) return 0;
    *request_len = offset;
    return 1;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_range_literal_to_value(const uint8_t *literal,
                                  uint32_t literal_len,
                                  uint8_t expected_type,
                                  uint8_t decimal_precision,
                                  uint8_t integer_bytes[4],
                                  prdb_sdk_value_v1 *value) {
    plan_reader reader;
    uint8_t tag;
    if (literal == NULL || literal_len == 0 || value == NULL) return 0;
    reader.bytes = literal;
    reader.length = literal_len;
    reader.offset = 0;
    if (!prdb_sdk_internal_read_u8(&reader, &tag)) return 0;
    if (tag == 0x01) {
        uint8_t boolean_value;
        if (expected_type != PRDB_SDK_VALUE_TYPE_BOOLEAN ||
            !prdb_sdk_internal_read_boolean_literal_predicate_record(&reader, 0, &boolean_value) ||
            reader.offset != reader.length) return 0;
        memset(value, 0, sizeof(*value));
        value->struct_size = sizeof(*value);
        value->abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
        value->state = PRDB_SDK_VALUE_STATE_VALUE;
        value->type_code = PRDB_SDK_VALUE_TYPE_BOOLEAN;
        value->data = literal + 1;
        value->data_len = 1;
    } else if (tag == 0x02) {
        int64_t integer_value;
        if (expected_type != PRDB_SDK_VALUE_TYPE_INTEGER ||
            !prdb_sdk_internal_read_integer_predicate_record(&reader, 0, &integer_value) ||
            reader.offset != reader.length || integer_value < INT32_MIN ||
            integer_value > INT32_MAX) return 0;
        write_u32_be(integer_bytes, (uint32_t)(int32_t)integer_value);
        memset(value, 0, sizeof(*value));
        value->struct_size = sizeof(*value);
        value->abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
        value->state = PRDB_SDK_VALUE_STATE_VALUE;
        value->type_code = PRDB_SDK_VALUE_TYPE_INTEGER;
        value->data = integer_bytes;
        value->data_len = sizeof(uint32_t);
    } else if (tag == 0x03) {
        uint16_t text_len;
        const uint8_t *text;
        if (expected_type != PRDB_SDK_VALUE_TYPE_TEXT ||
            !prdb_sdk_internal_read_text_predicate_header(&reader, 0, &text_len) ||
            text_len > 4096 || !prdb_sdk_internal_take(&reader, text_len, &text) ||
            reader.offset != reader.length || !prdb_sdk_internal_valid_utf8(text, text_len)) return 0;
        memset(value, 0, sizeof(*value));
        value->struct_size = sizeof(*value);
        value->abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
        value->state = PRDB_SDK_VALUE_STATE_VALUE;
        value->type_code = PRDB_SDK_VALUE_TYPE_TEXT;
        value->data = text;
        value->data_len = text_len;
    } else if (tag == 0x04) {
        const uint8_t *date_bytes;
        if (expected_type != PRDB_SDK_VALUE_TYPE_DATE ||
            !prdb_sdk_internal_read_date_predicate_record(&reader, 0, &date_bytes) ||
            reader.offset != reader.length) return 0;
        memset(value, 0, sizeof(*value));
        value->struct_size = sizeof(*value);
        value->abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
        value->state = PRDB_SDK_VALUE_STATE_VALUE;
        value->type_code = PRDB_SDK_VALUE_TYPE_DATE;
        value->data = date_bytes;
        value->data_len = 4;
    } else if (tag == 0x05) {
        const uint8_t *uuid_bytes;
        if (expected_type != PRDB_SDK_VALUE_TYPE_UUID ||
            !prdb_sdk_internal_read_uuid_predicate_record(&reader, 0, &uuid_bytes) ||
            reader.offset != reader.length) return 0;
        memset(value, 0, sizeof(*value));
        value->struct_size = sizeof(*value);
        value->abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
        value->state = PRDB_SDK_VALUE_STATE_VALUE;
        value->type_code = PRDB_SDK_VALUE_TYPE_UUID;
        value->data = uuid_bytes;
        value->data_len = 16;
    } else if (tag == 0x06) {
        const uint8_t *timestamp_bytes;
        if (expected_type != PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ ||
            !prdb_sdk_internal_read_timestamptz_predicate_record(&reader, 0, &timestamp_bytes) ||
            reader.offset != reader.length) return 0;
        memset(value, 0, sizeof(*value));
        value->struct_size = sizeof(*value);
        value->abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
        value->state = PRDB_SDK_VALUE_STATE_VALUE;
        value->type_code = PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ;
        value->data = timestamp_bytes;
        value->data_len = 8;
    } else if (tag == 0x07) {
        uint16_t bytea_length;
        const uint8_t *bytea_bytes;
        if (expected_type != PRDB_SDK_VALUE_TYPE_BYTEA ||
            !prdb_sdk_internal_read_bytea_predicate_header(&reader, 0, &bytea_length) ||
            bytea_length > 4096 || !prdb_sdk_internal_take(&reader, bytea_length, &bytea_bytes) ||
            reader.offset != reader.length) return 0;
        memset(value, 0, sizeof(*value));
        value->struct_size = sizeof(*value);
        value->abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
        value->state = PRDB_SDK_VALUE_STATE_VALUE;
        value->type_code = PRDB_SDK_VALUE_TYPE_BYTEA;
        value->data = bytea_bytes;
        value->data_len = bytea_length;
    } else if (tag == 0x08) {
        const uint8_t *coefficient;
        if (expected_type != PRDB_SDK_VALUE_TYPE_DECIMAL ||
            !prdb_sdk_internal_read_decimal_predicate_record(&reader, 0, &coefficient) ||
            reader.offset != reader.length ||
            !prdb_sdk_internal_decimal_coefficient_fits_precision(coefficient, decimal_precision))
            return 0;
        memset(value, 0, sizeof(*value));
        value->struct_size = sizeof(*value);
        value->abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
        value->state = PRDB_SDK_VALUE_STATE_VALUE;
        value->type_code = PRDB_SDK_VALUE_TYPE_DECIMAL;
        value->data = coefficient;
        value->data_len = 16;
    } else {
        return 0;
    }
    return 1;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_relation_response_column_ids(const uint8_t *response,
                                        uint64_t response_len,
                                        uint32_t column_count,
                                        uint8_t *column_ids,
                                        uint64_t column_ids_capacity) {
    uint64_t offset = 35;
    uint16_t name_len;
    if (response == NULL || column_ids == NULL ||
        column_count > PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS ||
        column_ids_capacity < (uint64_t)column_count * 16 ||
        response_len < 44) return 0;
    name_len = point_read_u16_be(response + offset);
    offset += 2;
    if (name_len > response_len - offset) return 0;
    offset += name_len;
    if (offset + 2 > response_len ||
        point_read_u16_be(response + offset) != column_count) return 0;
    offset += 2;
    for (uint32_t index = 0; index < column_count; ++index) {
        uint16_t column_name_len;
        if (offset + 28 > response_len) return 0;
        memcpy(column_ids + (uint64_t)index * 16, response + offset, 16);
        offset += 26;
        column_name_len = point_read_u16_be(response + offset);
        offset += 2;
        if (column_name_len > response_len - offset) return 0;
        offset += column_name_len;
    }
    return offset == response_len;
}

/* Convert the no-handle range wire response into the existing internal batch
 * view. The synthetic token bytes remain private to this SELECT consumer. */
PRDB_SDK_INTERNAL int prdb_sdk_internal_range_response_to_internal_batch(
    uint8_t *payload, uint64_t *payload_len, uint64_t capacity,
    uint32_t max_batch_bytes, uint16_t max_candidates) {
    prdb_sdk_index_range_v1 range = {0};
    prdb_sdk_index_range_row_iterator_v1 rows = {0};
    uint32_t row_starts[PRDB_SDK_BATCH_MAX_ROWS];
    uint32_t row_ends[PRDB_SDK_BATCH_MAX_ROWS];
    uint64_t expanded_len;

    if (payload == NULL || payload_len == NULL ||
        prdb_sdk_index_range_open_v1(&range, sizeof(range), payload,
                                     *payload_len) != PRDB_SDK_BATCH_OK) return 0;
    if (range.row_count > max_candidates ||
        range.row_count > PRDB_SDK_BATCH_MAX_ROWS ||
        prdb_sdk_index_range_rows_begin_v1(&range, &rows, sizeof(rows)) !=
            PRDB_SDK_BATCH_OK) return 0;
    for (uint16_t index = 0; index < range.row_count; ++index) {
        prdb_sdk_index_range_row_v1 row = {0};
        row_starts[index] = rows.offset;
        if (prdb_sdk_index_range_row_next_v1(&rows, &row, sizeof(row)) !=
            PRDB_SDK_BATCH_OK) return 0;
        row_ends[index] = rows.offset;
    }
    if (rows.offset != range.payload_len) return 0;
    expanded_len = range.payload_len + 1 + (uint64_t)range.row_count * 16;
    if (expanded_len > capacity || expanded_len > max_batch_bytes ||
        expanded_len > PRDB_SDK_BATCH_MAX_BYTES) return 0;
    for (uint16_t reverse = range.row_count; reverse > 0; --reverse) {
        uint16_t index = reverse - 1;
        uint32_t source_start = row_starts[index];
        uint32_t source_end = row_ends[index];
        uint64_t new_start = (uint64_t)source_start + 1 + (uint64_t)index * 16;
        uint64_t new_cells_start = new_start + 16;
        memmove(payload + new_cells_start, payload + source_start,
                source_end - source_start);
        memset(payload + new_start, 0, 16);
        payload[new_start] = PRDB_SDK_BATCH_ROW_TOKEN_KIND;
        payload[new_start + 14] = (uint8_t)(index >> 8);
        payload[new_start + 15] = (uint8_t)index;
    }
    payload[0] = 0;
    payload[1] = 1;
    payload[2] = 1;
    payload[3] = (uint8_t)(range.row_count >> 8);
    payload[4] = (uint8_t)range.row_count;
    *payload_len = expanded_len;
    return 1;
}

static void predicate_value_init(predicate_value *out, uint32_t type,
                                 const uint8_t *bytes, uint64_t length) {
    memset(out, 0, sizeof(*out));
    out->value.struct_size = sizeof(out->value);
    out->value.abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
    out->value.state = PRDB_SDK_VALUE_STATE_VALUE;
    out->value.type_code = type;
    out->value.data = bytes;
    out->value.data_len = length;
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_skip_predicate_node(plan_reader *reader, uint32_t depth) {
    uint8_t tag;
    uint64_t tag_offset = reader->offset;
    const uint8_t *bytes;
    if (depth > 32 || !prdb_sdk_internal_read_u8(reader, &tag)) return 0;
    switch (tag) {
    case 0x00:
        return prdb_sdk_internal_read_null_predicate_tag(reader, tag_offset);
    case 0x01:
        {
            uint8_t boolean_value;
            return prdb_sdk_internal_read_boolean_literal_predicate_record(reader, tag_offset,
                                                         &boolean_value);
        }
    case 0x02:
        {
            int64_t integer_value;
            return prdb_sdk_internal_read_integer_predicate_record(reader, tag_offset,
                                                 &integer_value);
        }
    case 0x03: {
        uint16_t length;
        return prdb_sdk_internal_read_text_predicate_header(reader, tag_offset, &length) &&
               length <= 4096 &&
               prdb_sdk_internal_take(reader, length, &bytes);
    }
    case 0x07: {
        uint16_t length;
        return prdb_sdk_internal_read_bytea_predicate_header(reader, tag_offset, &length) &&
               length <= 4096 && prdb_sdk_internal_take(reader, length, &bytes);
    }
    case 0x08: {
        const uint8_t *coefficient;
        return prdb_sdk_internal_read_decimal_predicate_record(reader, tag_offset, &coefficient);
    }
    case 0x04: {
        const uint8_t *date_bytes;
        return prdb_sdk_internal_read_date_predicate_record(reader, tag_offset, &date_bytes);
    }
    case 0x10:
        {
            uint8_t column_id[16];
            return prdb_sdk_internal_read_column_predicate_record(reader, tag_offset, column_id);
        }
    case 0x20: {
        uint8_t comparison;
        return prdb_sdk_internal_read_compare_predicate_header(reader, tag_offset, &comparison) &&
               comparison <= 5 &&
               prdb_sdk_internal_skip_predicate_node(reader, depth + 1) &&
               prdb_sdk_internal_skip_predicate_node(reader, depth + 1);
    }
    case 0x30: case 0x31: {
        uint8_t node_tag;
        uint8_t child_count;
        if (!prdb_sdk_internal_read_boolean_predicate_header(reader, tag_offset, &node_tag,
                                           &child_count) || node_tag != tag ||
            child_count < 2 ||
            child_count > 16) return 0;
        for (uint32_t index = 0; index < child_count; ++index)
            if (!prdb_sdk_internal_skip_predicate_node(reader, depth + 1)) return 0;
        return 1;
    }
    case 0x32:
        return prdb_sdk_internal_read_not_predicate_tag(reader, tag_offset) &&
               prdb_sdk_internal_skip_predicate_node(reader, depth + 1);
    case 0x40: case 0x41:
        return reader->offset < reader->length &&
               reader->bytes[reader->offset] == 0x10 &&
               prdb_sdk_internal_skip_predicate_node(reader, depth + 1);
    default:
        return 0;
    }
}

PRDB_SDK_INTERNAL int32_t prdb_sdk_internal_evaluate_predicate_node(
    plan_reader *reader, const prdb_sdk_optimizer_plan_v1 *plan,
    const prdb_sdk_value_v1 *source_cells, predicate_value *out,
    uint32_t depth, uint32_t *nodes) {
    uint8_t tag;
    uint64_t tag_offset = reader->offset;
    const uint8_t *bytes;
    if (depth > 32 || ++*nodes > 256 || !prdb_sdk_internal_read_u8(reader, &tag))
        return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
    switch (tag) {
    case 0x00:
        if (!prdb_sdk_internal_read_null_predicate_tag(reader, tag_offset))
            return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
        predicate_value_init(out, 0, NULL, 0);
        out->value.state = PRDB_SDK_VALUE_STATE_NULL;
        return PRDB_SDK_SQL_EVAL_OK;
    case 0x01:
        {
            uint8_t boolean_value;
            if (!prdb_sdk_internal_read_boolean_literal_predicate_record(reader, tag_offset,
                                                       &boolean_value) ||
                boolean_value > 1)
                return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
            predicate_value_init(out, PRDB_SDK_VALUE_TYPE_BOOLEAN, NULL, 1);
            out->bytes[0] = boolean_value;
        }
        out->value.data = out->bytes;
        return PRDB_SDK_SQL_EVAL_OK;
    case 0x02: {
        int64_t integer_value;
        uint64_t raw;
        uint32_t high;
        if (!prdb_sdk_internal_read_integer_predicate_record(reader, tag_offset,
                                           &integer_value))
            return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
        raw = (uint64_t)integer_value;
        high = (uint32_t)(raw >> 32);
        if (!((high == 0 && (uint32_t)raw <= INT32_MAX) ||
              (high == UINT32_MAX && (uint32_t)raw >= UINT32_C(0x80000000))))
            return PRDB_SDK_SQL_EVAL_NUMERIC_OVERFLOW;
        predicate_value_init(out, PRDB_SDK_VALUE_TYPE_INTEGER, NULL, 4);
        out->bytes[0] = (uint8_t)(raw >> 24);
        out->bytes[1] = (uint8_t)(raw >> 16);
        out->bytes[2] = (uint8_t)(raw >> 8);
        out->bytes[3] = (uint8_t)raw;
        out->value.data = out->bytes;
        return PRDB_SDK_SQL_EVAL_OK;
    }
    case 0x05: {
        const uint8_t *uuid_bytes;
        if (!prdb_sdk_internal_read_uuid_predicate_record(reader, tag_offset, &uuid_bytes))
            return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
        predicate_value_init(out, PRDB_SDK_VALUE_TYPE_UUID, uuid_bytes, 16);
        return PRDB_SDK_SQL_EVAL_OK;
    }
    case 0x06: {
        const uint8_t *timestamp_bytes;
        if (!prdb_sdk_internal_read_timestamptz_predicate_record(reader, tag_offset,
                                               &timestamp_bytes))
            return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
        predicate_value_init(out, PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ,
                             timestamp_bytes, 8);
        return PRDB_SDK_SQL_EVAL_OK;
    }
    case 0x07: {
        uint16_t length;
        const uint8_t *bytea_bytes;
        if (!prdb_sdk_internal_read_bytea_predicate_header(reader, tag_offset, &length) ||
            length > 4096 || !prdb_sdk_internal_take(reader, length, &bytea_bytes))
            return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
        predicate_value_init(out, PRDB_SDK_VALUE_TYPE_BYTEA,
                             bytea_bytes, length);
        return PRDB_SDK_SQL_EVAL_OK;
    }
    case 0x08: {
        const uint8_t *coefficient;
        if (!prdb_sdk_internal_read_decimal_predicate_record(reader, tag_offset, &coefficient))
            return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
        predicate_value_init(out, PRDB_SDK_VALUE_TYPE_DECIMAL,
                             coefficient, 16);
        out->decimal_scale = UINT8_MAX;
        return PRDB_SDK_SQL_EVAL_OK;
    }
    case 0x03: {
        uint16_t length;
        if (!prdb_sdk_internal_read_text_predicate_header(reader, tag_offset, &length) ||
            length > 4096 ||
            !prdb_sdk_internal_take(reader, length, &bytes))
            return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
        predicate_value_init(out, PRDB_SDK_VALUE_TYPE_TEXT, bytes, length);
        return PRDB_SDK_SQL_EVAL_OK;
    }
    case 0x04: {
        const uint8_t *date_bytes;
        if (!prdb_sdk_internal_read_date_predicate_record(reader, tag_offset, &date_bytes))
            return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
        predicate_value_init(out, PRDB_SDK_VALUE_TYPE_DATE, date_bytes, 4);
        return PRDB_SDK_SQL_EVAL_OK;
    }
    case 0x10: {
        uint8_t column_id[16];
        if (!prdb_sdk_internal_read_column_predicate_record(reader, tag_offset, column_id))
            return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
        for (uint32_t index = 0; index < plan->scan_column_count; ++index) {
            const prdb_sdk_optimizer_plan_column_v1 *column =
                &plan->scan_columns[index];
            if (memcmp(column_id, column->column_id, 16) == 0) {
                out->value = source_cells[column->ordinal];
                out->decimal_scale = column->type_parameters[1];
                return PRDB_SDK_SQL_EVAL_OK;
            }
        }
        return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
    }
    case 0x20: {
        uint8_t comparison;
        predicate_value left = {0}, right = {0};
        uint32_t truth = PRDB_SDK_SQL_TRUTH_UNKNOWN;
        int32_t status;
        if (!prdb_sdk_internal_read_compare_predicate_header(reader, tag_offset, &comparison) ||
            comparison > 5)
            return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
        status = prdb_sdk_internal_evaluate_predicate_node(reader, plan, source_cells, &left,
                                         depth + 1, nodes);
        if (status != PRDB_SDK_SQL_EVAL_OK) return status;
        status = prdb_sdk_internal_evaluate_predicate_node(reader, plan, source_cells, &right,
                                         depth + 1, nodes);
        if (status != PRDB_SDK_SQL_EVAL_OK) return status;
        if (left.decimal_scale == UINT8_MAX &&
            right.value.type_code == PRDB_SDK_VALUE_TYPE_DECIMAL)
            left.decimal_scale = right.decimal_scale;
        if (right.decimal_scale == UINT8_MAX &&
            left.value.type_code == PRDB_SDK_VALUE_TYPE_DECIMAL)
            right.decimal_scale = left.decimal_scale;
        status = prdb_sdk_sql_compare_v1(
            &left.value, &right.value, (uint32_t)comparison + 1,
            left.decimal_scale,
            right.decimal_scale, &truth);
        if (status != PRDB_SDK_SQL_EVAL_OK) return status;
        out->is_truth = 1;
        out->truth = truth;
        return PRDB_SDK_SQL_EVAL_OK;
    }
    case 0x30: case 0x31: {
        uint8_t node_tag;
        uint8_t child_count;
        uint32_t truth = tag == 0x30 ? PRDB_SDK_SQL_TRUTH_TRUE
                                     : PRDB_SDK_SQL_TRUTH_FALSE;
        if (!prdb_sdk_internal_read_boolean_predicate_header(reader, tag_offset, &node_tag,
                                           &child_count) || node_tag != tag ||
            child_count < 2 ||
            child_count > 16) return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
        for (uint32_t index = 0; index < child_count; ++index) {
            predicate_value child = {0};
            uint32_t child_truth;
            int32_t status = prdb_sdk_internal_evaluate_predicate_node(
                reader, plan, source_cells, &child, depth + 1, nodes);
            if (status != PRDB_SDK_SQL_EVAL_OK) return status;
            status = child.is_truth != 0
                         ? PRDB_SDK_SQL_EVAL_OK
                         : prdb_sdk_sql_value_truth_v1(&child.value,
                                                       &child_truth);
            if (status != PRDB_SDK_SQL_EVAL_OK) return status;
            if (child.is_truth != 0) child_truth = child.truth;
            status = tag == 0x30
                         ? prdb_sdk_sql_truth_and_v1(truth, child_truth, &truth)
                         : prdb_sdk_sql_truth_or_v1(truth, child_truth, &truth);
            if (status != PRDB_SDK_SQL_EVAL_OK) return status;
            if ((tag == 0x30 && truth == PRDB_SDK_SQL_TRUTH_FALSE) ||
                (tag == 0x31 && truth == PRDB_SDK_SQL_TRUTH_TRUE)) {
                for (++index; index < child_count; ++index)
                    if (!prdb_sdk_internal_skip_predicate_node(reader, depth + 1))
                        return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
                break;
            }
        }
        out->is_truth = 1;
        out->truth = truth;
        return PRDB_SDK_SQL_EVAL_OK;
    }
    case 0x32: {
        predicate_value child = {0};
        uint32_t truth;
        int32_t status;
        if (!prdb_sdk_internal_read_not_predicate_tag(reader, tag_offset))
            return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
        status = prdb_sdk_internal_evaluate_predicate_node(reader, plan, source_cells, &child,
                                         depth + 1, nodes);
        if (status != PRDB_SDK_SQL_EVAL_OK) return status;
        status = child.is_truth != 0
                     ? PRDB_SDK_SQL_EVAL_OK
                     : prdb_sdk_sql_value_truth_v1(&child.value, &truth);
        if (status != PRDB_SDK_SQL_EVAL_OK) return status;
        if (child.is_truth != 0) truth = child.truth;
        status = prdb_sdk_sql_truth_not_v1(truth, &truth);
        if (status != PRDB_SDK_SQL_EVAL_OK) return status;
        out->is_truth = 1;
        out->truth = truth;
        return PRDB_SDK_SQL_EVAL_OK;
    }
    case 0x40: case 0x41: {
        predicate_value child = {0};
        int32_t status;
        if (reader->offset >= reader->length ||
            reader->bytes[reader->offset] != 0x10)
            return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
        status = prdb_sdk_internal_evaluate_predicate_node(reader, plan, source_cells, &child,
                                         depth + 1, nodes);
        if (status != PRDB_SDK_SQL_EVAL_OK) return status;
        if (child.value.state == PRDB_SDK_VALUE_STATE_REDACTED)
            return PRDB_SDK_SQL_EVAL_REDACTED_INPUT;
        if (child.value.state != PRDB_SDK_VALUE_STATE_NULL &&
            child.value.state != PRDB_SDK_VALUE_STATE_VALUE)
            return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
        out->is_truth = 1;
        out->truth = ((tag == 0x40) ==
                      (child.value.state == PRDB_SDK_VALUE_STATE_NULL))
                         ? PRDB_SDK_SQL_TRUTH_TRUE
                         : PRDB_SDK_SQL_TRUTH_FALSE;
        return PRDB_SDK_SQL_EVAL_OK;
    }
    default:
        return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
    }
}

PRDB_SDK_INTERNAL int prdb_sdk_internal_columns_equal(const prdb_sdk_optimizer_plan_column_v1 *left,
                         const prdb_sdk_optimizer_plan_column_v1 *right) {
    return memcmp(left->column_id, right->column_id, 16) == 0 &&
           left->ordinal == right->ordinal &&
           left->type_code == right->type_code &&
           memcmp(left->type_parameters, right->type_parameters, 4) == 0 &&
           left->nullable == right->nullable;
}

PRDB_SDK_INTERNAL uint16_t prdb_sdk_internal_read_be_u16(const uint8_t *bytes) {
    return (uint16_t)(((uint16_t)bytes[0] << 8) | bytes[1]);
}
