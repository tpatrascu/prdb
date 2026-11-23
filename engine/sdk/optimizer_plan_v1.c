#include "prdb_sdk_optimizer_plan_v1.h"
#include "prdb_sdk_optimizer_v2.h"
#include "prdb_sdk_sql_v1.h"
#include "prdb_sdk_sql_exec_v3.h"
#include "prdb_sdk_sql_order_v1.h"
#include "prdb_sdk_sql_result_v1.h"
#include "prdb_query_abi_payload_codecs.h"

#include <stddef.h>
#include <limits.h>
#include <stdint.h>
#include <string.h>

#define OPTIMIZER_ORDERED_SELECT_MAX_SCAN_PAGES UINT32_C(124)
#define OPTIMIZER_ORDERED_SELECT_CALLBACK_POLL_INTERVAL UINT32_C(32)

typedef struct plan_reader {
    const uint8_t *bytes;
    uint64_t length;
    uint64_t offset;
} plan_reader;

typedef struct predicate_value {
    prdb_sdk_value_v1 value;
    uint8_t bytes[8];
    uint8_t decimal_scale;
    uint32_t is_truth;
    uint32_t truth;
} predicate_value;

typedef struct point_probe_hint {
    uint8_t present;
    uint8_t index_id[16];
    prdb_sdk_optimizer_plan_column_v1 key_column;
    const uint8_t *literal;
    uint32_t literal_len;
} point_probe_hint;

typedef struct range_probe_hint {
    uint8_t present;
    uint8_t index_id[16];
    prdb_sdk_optimizer_plan_column_v1 key_column;
    const uint8_t *lower_literal;
    uint32_t lower_literal_len;
    uint8_t lower_inclusive;
    const uint8_t *upper_literal;
    uint32_t upper_literal_len;
    uint8_t upper_inclusive;
} range_probe_hint;

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

static int read_text_predicate_header(plan_reader *reader,
                                      uint64_t header_offset,
                                      uint16_t *text_length) {
    const uint8_t *ignored;
    uint8_t node_tag;
    if (!take(reader, 2, &ignored) ||
        !prdb_decode_optimizer_plan_text_predicate_header_record(
            reader->bytes + header_offset,
            PRDB_OPTIMIZER_PLAN_TEXT_PREDICATE_HEADER_RECORD_HEADER_BYTES,
            &node_tag, text_length)) {
        return 0;
    }
    return node_tag == 0x03;
}

static int read_integer_predicate_record(plan_reader *reader,
                                         uint64_t header_offset,
                                         int64_t *integer_value) {
    const uint8_t *ignored;
    uint8_t node_tag;
    if (!take(reader,
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

static int read_column_predicate_record(plan_reader *reader,
                                        uint64_t header_offset,
                                        uint8_t column_id[16]) {
    const uint8_t *ignored;
    uint8_t node_tag;
    if (!take(reader,
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

static int read_compare_predicate_header(plan_reader *reader,
                                        uint64_t header_offset,
                                        uint8_t *comparison_operator) {
    const uint8_t *ignored;
    uint8_t node_tag;
    if (!take(reader,
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

static int read_boolean_predicate_header(plan_reader *reader,
                                         uint64_t header_offset,
                                         uint8_t *node_tag_out,
                                         uint8_t *child_count) {
    const uint8_t *ignored;
    uint8_t node_tag;
    if (!take(reader,
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

static int read_not_predicate_tag(const plan_reader *reader,
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

static int read_null_predicate_tag(const plan_reader *reader,
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

static int read_boolean_literal_predicate_record(
    plan_reader *reader, uint64_t header_offset, uint8_t *boolean_value) {
    const uint8_t *ignored;
    uint8_t node_tag;
    if (!take(reader,
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

static int read_column_list_header(plan_reader *reader, uint8_t expected_tag,
                                   uint16_t *column_count) {
    const uint8_t *ignored;
    uint8_t node_tag;
    uint64_t header_offset;
    if (reader->offset == 0) return 0;
    header_offset = reader->offset - 1;
    if (!take(reader, 2, &ignored) ||
        !prdb_decode_optimizer_plan_column_list_header_record(
            reader->bytes + header_offset,
            PRDB_OPTIMIZER_PLAN_COLUMN_LIST_HEADER_RECORD_HEADER_BYTES,
            &node_tag, column_count)) {
        return 0;
    }
    return node_tag == expected_tag;
}

static int read_limit_node(plan_reader *reader, uint32_t *row_limit) {
    const uint8_t *ignored;
    uint8_t node_tag;
    uint64_t header_offset;
    if (reader->offset == 0) return 0;
    header_offset = reader->offset - 1;
    if (!take(reader, 4, &ignored) ||
        !prdb_decode_optimizer_plan_limit_node_record(
            reader->bytes + header_offset,
            PRDB_OPTIMIZER_PLAN_LIMIT_NODE_RECORD_HEADER_BYTES,
            &node_tag, row_limit)) {
        return 0;
    }
    return node_tag == 4;
}

static int read_plan_node_count(plan_reader *reader, uint8_t *node_count) {
    const uint8_t *bytes;
    return take(reader,
                PRDB_OPTIMIZER_PLAN_NODE_COUNT_RECORD_HEADER_BYTES, &bytes) &&
           prdb_decode_optimizer_plan_node_count_record(
               bytes, PRDB_OPTIMIZER_PLAN_NODE_COUNT_RECORD_HEADER_BYTES,
               node_count);
}

static int read_filter_node_tag(const plan_reader *reader,
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
    const uint8_t *header;
    uint8_t length;
    const uint8_t *name;
    if (!take(reader,
              PRDB_OPTIMIZER_PLAN_NAME_HEADER_RECORD_HEADER_BYTES, &header) ||
        !prdb_decode_optimizer_plan_name_header_record(
            header, PRDB_OPTIMIZER_PLAN_NAME_HEADER_RECORD_HEADER_BYTES,
            &length) ||
        !take(reader, length, &name) ||
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
    if (!take(reader, PRDB_OPTIMIZER_PLAN_COLUMN_RECORD_HEADER_BYTES, &bytes) ||
        !prdb_decode_optimizer_plan_column_record(
            bytes, PRDB_OPTIMIZER_PLAN_COLUMN_RECORD_HEADER_BYTES,
            column->column_id, &column->ordinal, &column->type_code,
            column->type_parameters, &column->nullable) ||
        all_zero(column->column_id, 16) || column->nullable > 1 ||
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
                            uint32_t *nodes, uint8_t *references,
                            predicate_type *type_out) {
    uint8_t tag;
    uint64_t tag_offset = reader->offset;
    const uint8_t *bytes;
    predicate_type type = {0};
    if (depth > 32 || ++*nodes > 256 || !read_u8(reader, &tag)) return 0;
    switch (tag) {
        case 0x00:
            if (!read_null_predicate_tag(reader, tag_offset)) return 0;
            type.is_null = 1;
            break;
        case 0x01:
            if (!read_boolean_literal_predicate_record(reader, tag_offset, &tag) ||
                tag > 1) return 0;
            type.type_code = 1;
            break;
        case 0x02:
            {
                int64_t integer_value;
                if (!read_integer_predicate_record(reader, tag_offset,
                                                   &integer_value)) return 0;
            }
            type.type_code = 3;
            break;
        case 0x03: {
            uint16_t text_len;
            if (!read_text_predicate_header(reader, tag_offset, &text_len) ||
                text_len > 4096 ||
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
            uint8_t column_id[16];
            if (!read_column_predicate_record(reader, tag_offset, column_id))
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
            if (!read_compare_predicate_header(reader, tag_offset, &comparison) ||
                comparison > 5 ||
                !decode_predicate(reader, columns, column_count, depth + 1,
                                  nodes, references, &left) ||
                !decode_predicate(reader, columns, column_count, depth + 1,
                                  nodes, references, &right) ||
                !same_type(&left, &right)) {
                return 0;
            }
            type.type_code = 1;
            break;
        }
        case 0x30: case 0x31: {
            uint8_t node_tag;
            uint8_t child_count;
            uint32_t index;
            if (!read_boolean_predicate_header(reader, tag_offset, &node_tag,
                                               &child_count) || node_tag != tag ||
                child_count < 2 ||
                child_count > 16) {
                return 0;
            }
            for (index = 0; index < child_count; ++index) {
                predicate_type child;
                if (!decode_predicate(reader, columns, column_count,
                                      depth + 1, nodes, references, &child) ||
                    !predicate_boolean(&child)) {
                    return 0;
                }
            }
            type.type_code = 1;
            break;
        }
        case 0x32: {
            predicate_type child;
            if (!read_not_predicate_tag(reader, tag_offset) ||
                !decode_predicate(reader, columns, column_count, depth + 1,
                                  nodes, references, &child) ||
                !predicate_boolean(&child)) {
                return 0;
            }
            type.type_code = 1;
            break;
        }
        case 0x40: case 0x41: {
            predicate_type child;
            if (reader->offset >= reader->length ||
                reader->bytes[reader->offset] != 0x10 ||
                !decode_predicate(reader, columns, column_count, depth + 1,
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
    if (!read_u8(reader, &tag)) return 0;
    if (tag == 0x10) {
        return read_column_predicate_record(reader, start, column_id);
    }
    if (tag == 0x01) {
        uint8_t value;
        if (!read_boolean_literal_predicate_record(reader, start, &value))
            return 0;
    } else if (tag == 0x02) {
        int64_t value;
        if (!read_integer_predicate_record(reader, start, &value)) return 0;
    } else if (tag == 0x03) {
        uint16_t length;
        const uint8_t *bytes;
        if (!read_text_predicate_header(reader, start, &length) ||
            length > 4096 || !take(reader, length, &bytes) ||
            !valid_utf8(bytes, length)) return 0;
    } else {
        return 0;
    }
    *literal = reader->bytes + start;
    *literal_len = (uint32_t)(reader->offset - start);
    return 1;
}

static int point_probe_matches_filter(
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
    if (!read_u8(&reader, &tag) || tag != 0x20 ||
        !read_compare_predicate_header(&reader, tag_offset, &comparison) ||
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
    if (!read_u8(reader, &tag) || tag != 0x20 ||
        !read_compare_predicate_header(reader, tag_offset, &comparison) ||
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

static int range_probe_matches_filter(
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
    if (!read_u8(&reader, &tag)) return 0;
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
        if (!read_boolean_predicate_header(&reader, tag_offset, &node_tag,
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

static int encode_point_probe_request(
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
            if (!read_u8(&literal_reader, &tag) || tag != 0x02 ||
                !read_integer_predicate_record(&literal_reader, 0, &value) ||
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
            if (!read_u8(&literal_reader, &tag) || tag != 0x03 ||
                !read_text_predicate_header(&literal_reader, 0, &text_len) ||
                text_len > 4096 || !take(&literal_reader, text_len, &text) ||
                literal_reader.offset != literal_reader.length ||
                hint->key_column.type_code != 6) return 0;
            type_code = 6;
            value_bytes = text;
            value_len = text_len;
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

static int range_literal_to_value(const uint8_t *literal,
                                  uint32_t literal_len,
                                  uint8_t expected_type,
                                  uint8_t integer_bytes[4],
                                  prdb_sdk_value_v1 *value) {
    plan_reader reader;
    uint8_t tag;
    if (literal == NULL || literal_len == 0 || value == NULL) return 0;
    reader.bytes = literal;
    reader.length = literal_len;
    reader.offset = 0;
    if (!read_u8(&reader, &tag)) return 0;
    if (tag == 0x01) {
        uint8_t boolean_value;
        if (expected_type != PRDB_SDK_VALUE_TYPE_BOOLEAN ||
            !read_boolean_literal_predicate_record(&reader, 0, &boolean_value) ||
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
            !read_integer_predicate_record(&reader, 0, &integer_value) ||
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
            !read_text_predicate_header(&reader, 0, &text_len) ||
            text_len > 4096 || !take(&reader, text_len, &text) ||
            reader.offset != reader.length || !valid_utf8(text, text_len)) return 0;
        memset(value, 0, sizeof(*value));
        value->struct_size = sizeof(*value);
        value->abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
        value->state = PRDB_SDK_VALUE_STATE_VALUE;
        value->type_code = PRDB_SDK_VALUE_TYPE_TEXT;
        value->data = text;
        value->data_len = text_len;
    } else {
        return 0;
    }
    return 1;
}

static int relation_response_column_ids(const uint8_t *response,
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
static int range_response_to_internal_batch(
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

static int skip_predicate_node(plan_reader *reader, uint32_t depth) {
    uint8_t tag;
    uint64_t tag_offset = reader->offset;
    const uint8_t *bytes;
    if (depth > 32 || !read_u8(reader, &tag)) return 0;
    switch (tag) {
    case 0x00:
        return read_null_predicate_tag(reader, tag_offset);
    case 0x01:
        {
            uint8_t boolean_value;
            return read_boolean_literal_predicate_record(reader, tag_offset,
                                                         &boolean_value);
        }
    case 0x02:
        {
            int64_t integer_value;
            return read_integer_predicate_record(reader, tag_offset,
                                                 &integer_value);
        }
    case 0x03: {
        uint16_t length;
        return read_text_predicate_header(reader, tag_offset, &length) &&
               length <= 4096 &&
               take(reader, length, &bytes);
    }
    case 0x10:
        {
            uint8_t column_id[16];
            return read_column_predicate_record(reader, tag_offset, column_id);
        }
    case 0x20: {
        uint8_t comparison;
        return read_compare_predicate_header(reader, tag_offset, &comparison) &&
               comparison <= 5 &&
               skip_predicate_node(reader, depth + 1) &&
               skip_predicate_node(reader, depth + 1);
    }
    case 0x30: case 0x31: {
        uint8_t node_tag;
        uint8_t child_count;
        if (!read_boolean_predicate_header(reader, tag_offset, &node_tag,
                                           &child_count) || node_tag != tag ||
            child_count < 2 ||
            child_count > 16) return 0;
        for (uint32_t index = 0; index < child_count; ++index)
            if (!skip_predicate_node(reader, depth + 1)) return 0;
        return 1;
    }
    case 0x32:
        return read_not_predicate_tag(reader, tag_offset) &&
               skip_predicate_node(reader, depth + 1);
    case 0x40: case 0x41:
        return reader->offset < reader->length &&
               reader->bytes[reader->offset] == 0x10 &&
               skip_predicate_node(reader, depth + 1);
    default:
        return 0;
    }
}

static int32_t evaluate_predicate_node(
    plan_reader *reader, const prdb_sdk_optimizer_plan_v1 *plan,
    const prdb_sdk_value_v1 *source_cells, predicate_value *out,
    uint32_t depth, uint32_t *nodes) {
    uint8_t tag;
    uint64_t tag_offset = reader->offset;
    const uint8_t *bytes;
    if (depth > 32 || ++*nodes > 256 || !read_u8(reader, &tag))
        return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
    switch (tag) {
    case 0x00:
        if (!read_null_predicate_tag(reader, tag_offset))
            return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
        predicate_value_init(out, 0, NULL, 0);
        out->value.state = PRDB_SDK_VALUE_STATE_NULL;
        return PRDB_SDK_SQL_EVAL_OK;
    case 0x01:
        {
            uint8_t boolean_value;
            if (!read_boolean_literal_predicate_record(reader, tag_offset,
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
        if (!read_integer_predicate_record(reader, tag_offset,
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
    case 0x03: {
        uint16_t length;
        if (!read_text_predicate_header(reader, tag_offset, &length) ||
            length > 4096 ||
            !take(reader, length, &bytes))
            return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
        predicate_value_init(out, PRDB_SDK_VALUE_TYPE_TEXT, bytes, length);
        return PRDB_SDK_SQL_EVAL_OK;
    }
    case 0x10: {
        uint8_t column_id[16];
        if (!read_column_predicate_record(reader, tag_offset, column_id))
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
        if (!read_compare_predicate_header(reader, tag_offset, &comparison) ||
            comparison > 5)
            return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
        status = evaluate_predicate_node(reader, plan, source_cells, &left,
                                         depth + 1, nodes);
        if (status != PRDB_SDK_SQL_EVAL_OK) return status;
        status = evaluate_predicate_node(reader, plan, source_cells, &right,
                                         depth + 1, nodes);
        if (status != PRDB_SDK_SQL_EVAL_OK) return status;
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
        if (!read_boolean_predicate_header(reader, tag_offset, &node_tag,
                                           &child_count) || node_tag != tag ||
            child_count < 2 ||
            child_count > 16) return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
        for (uint32_t index = 0; index < child_count; ++index) {
            predicate_value child = {0};
            uint32_t child_truth;
            int32_t status = evaluate_predicate_node(
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
                    if (!skip_predicate_node(reader, depth + 1))
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
        if (!read_not_predicate_tag(reader, tag_offset))
            return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
        status = evaluate_predicate_node(reader, plan, source_cells, &child,
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
        status = evaluate_predicate_node(reader, plan, source_cells, &child,
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

static int columns_equal(const prdb_sdk_optimizer_plan_column_v1 *left,
                         const prdb_sdk_optimizer_plan_column_v1 *right) {
    return memcmp(left->column_id, right->column_id, 16) == 0 &&
           left->ordinal == right->ordinal &&
           left->type_code == right->type_code &&
           memcmp(left->type_parameters, right->type_parameters, 4) == 0 &&
           left->nullable == right->nullable;
}

static uint16_t read_be_u16(const uint8_t *bytes) {
    return (uint16_t)(((uint16_t)bytes[0] << 8) | bytes[1]);
}

static int join_key_types_match(
    const prdb_sdk_optimizer_plan_column_v1 *left,
    const prdb_sdk_optimizer_plan_column_v1 *right) {
    if (left->type_code != right->type_code) return 0;
    if (left->type_code == 5)
        return left->type_parameters[1] == right->type_parameters[1];
    return memcmp(left->type_parameters, right->type_parameters, 4) == 0;
}

static int join_scan_contains(
    const prdb_sdk_optimizer_join_relation_v1 *relation,
    const prdb_sdk_optimizer_plan_column_v1 *column) {
    uint32_t index;
    for (index = 0; index < relation->scan_column_count; ++index) {
        if (columns_equal(&relation->scan_columns[index], column)) return 1;
    }
    return 0;
}

static int32_t decode_join_plan_internal(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_join_plan_v1 *out, uint32_t out_capacity,
    uint8_t expected_kind, prdb_sdk_optimizer_join_filter_v1 *filter_out,
    uint32_t *has_filter_out, uint32_t *outer_side_out,
    uint32_t *inner_side_out, uint8_t index_id_out[16],
    prdb_sdk_optimizer_plan_column_v1 *index_key_out) {
    prdb_sdk_optimizer_join_plan_v1 decoded;
    prdb_sdk_optimizer_join_filter_v1 decoded_filter = {0};
    plan_reader reader;
    uint16_t version;
    uint8_t kind;
    uint8_t reserved;
    const uint8_t *bytes;
    uint32_t side;
    uint32_t index;
    uint32_t other;
    uint16_t count;
    uint8_t flag;

    if (payload == NULL || out == NULL || out_capacity < sizeof(*out) ||
        payload_len > PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES ||
        ranges_overlap(payload, payload_len, out, sizeof(*out)))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    memset(&decoded, 0, sizeof(decoded));
    reader.bytes = payload;
    reader.length = payload_len;
    reader.offset = 0;
    if (!prdb_decode_optimizer_plan_header_record(
            payload, PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES,
            &version, &kind, &reserved) || version != 1 ||
        kind != expected_kind ||
        reserved != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    reader.offset = PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    if (!read_name(&reader, &decoded.schema_name,
                   &decoded.schema_name_len) ||
        !take(&reader, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_schema_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
            decoded.schema_id, &decoded.schema_version) ||
        all_zero(decoded.schema_id, 16) || decoded.schema_version == 0)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;

    for (side = 0; side < 2; ++side) {
        prdb_sdk_optimizer_join_relation_v1 *relation =
            side == 0 ? &decoded.left_relation : &decoded.right_relation;
        if (!read_name(&reader, &relation->relation_name,
                       &relation->relation_name_len) ||
            !take(&reader,
                  PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
                  &bytes) ||
            !prdb_decode_optimizer_plan_relation_reference_record(
                bytes,
                PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
                relation->relation_id, &relation->table_epoch) ||
            all_zero(relation->relation_id, 16) || relation->table_epoch == 0)
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    for (side = 0; side < 2; ++side) {
        prdb_sdk_optimizer_join_relation_v1 *relation =
            side == 0 ? &decoded.left_relation : &decoded.right_relation;
        if (!take(&reader, 2, &bytes))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        count = read_be_u16(bytes);
        if (count == 0 || count > PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS)
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        relation->scan_column_count = count;
        for (index = 0; index < relation->scan_column_count; ++index) {
            prdb_sdk_optimizer_plan_column_v1 *column =
                &relation->scan_columns[index];
            if (!read_column(&reader, column) ||
                (index != 0 &&
                 relation->scan_columns[index - 1].ordinal >= column->ordinal))
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            for (other = 0; other < index; ++other) {
                if (memcmp(relation->scan_columns[other].column_id,
                           column->column_id, 16) == 0)
                    return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            }
        }
    }
    if (!take(&reader, 2, &bytes))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    count = read_be_u16(bytes);
    if (count == 0 || count > PRDB_SDK_OPTIMIZER_PLAN_MAX_PROJECTIONS)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.projection_count = count;
    for (index = 0; index < decoded.projection_count; ++index) {
        prdb_sdk_optimizer_join_projection_v1 *projection =
            &decoded.projections[index];
        if (!read_u8(&reader, &flag) || flag > 1 ||
            !read_column(&reader, &projection->column))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        projection->relation_side = flag;
        if (!join_scan_contains(flag == 0 ? &decoded.left_relation
                                          : &decoded.right_relation,
                                &projection->column))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (!read_u8(&reader, &flag) || flag < 2 || flag > 8)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.join_key_count = flag;
    for (index = 0; index < decoded.join_key_count; ++index) {
        prdb_sdk_optimizer_join_key_v1 *key = &decoded.join_keys[index];
        if (!read_column(&reader, &key->left_column) ||
            !read_column(&reader, &key->right_column) ||
            !join_scan_contains(&decoded.left_relation, &key->left_column) ||
            !join_scan_contains(&decoded.right_relation, &key->right_column) ||
            !join_key_types_match(&key->left_column, &key->right_column))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (!read_u8(&reader, &flag) || flag > 1)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.has_limit = flag;
    if (decoded.has_limit) {
        if (!take(&reader, 4, &bytes))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        decoded.limit = ((uint32_t)bytes[0] << 24) |
                        ((uint32_t)bytes[1] << 16) |
                        ((uint32_t)bytes[2] << 8) | bytes[3];
    }
    uint32_t has_filter =
        kind == PRDB_SDK_OPTIMIZER_PLAN_KIND_FILTERED_JOIN_SELECT;
    if (kind == PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_NESTED_LOOP_JOIN) {
        if (!read_u8(&reader, &flag) || flag > 1)
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        has_filter = flag;
    }
    if (has_filter) {
        uint8_t expression_kind;
        if (!read_u8(&reader, &flag) || flag > 1 ||
            !read_column(&reader, &decoded_filter.column))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        decoded_filter.relation_side = flag;
        if (!join_scan_contains(flag == 0 ? &decoded.left_relation
                                          : &decoded.right_relation,
                                &decoded_filter.column) ||
            !read_u8(&reader, &expression_kind) || expression_kind > 2)
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        decoded_filter.expression_kind = expression_kind;
        if (expression_kind == 0) {
            uint8_t comparison;
            if (!read_u8(&reader, &comparison) || comparison > 5 ||
                !take(&reader, 4, &bytes))
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            decoded_filter.comparison = comparison;
            decoded_filter.parameter_index = ((uint32_t)bytes[0] << 24) |
                                             ((uint32_t)bytes[1] << 16) |
                                             ((uint32_t)bytes[2] << 8) |
                                             (uint32_t)bytes[3];
            if (decoded_filter.parameter_index == 0 ||
                decoded_filter.parameter_index >
                    PRDB_SDK_OPTIMIZER_PLAN_MAX_PARAMETER_INDEX)
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        if (filter_out == NULL)
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    if (kind == PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_NESTED_LOOP_JOIN) {
        uint32_t outer_side;
        uint32_t inner_side;
        uint8_t nonzero = 0;
        int key_found = 0;
        if (filter_out == NULL || has_filter_out == NULL ||
            outer_side_out == NULL || inner_side_out == NULL ||
            index_id_out == NULL || index_key_out == NULL ||
            !read_u8(&reader, &flag) || flag > 1)
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        outer_side = flag;
        if (!read_u8(&reader, &flag) || flag > 1)
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        inner_side = flag;
        if (outer_side == inner_side || !take(&reader, 16, &bytes) ||
            !read_column(&reader, index_key_out))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        memcpy(index_id_out, bytes, 16);
        for (uint32_t byte_index = 0; byte_index < 16; ++byte_index)
            nonzero |= index_id_out[byte_index];
        if (nonzero == 0 ||
            !join_scan_contains(inner_side == 0 ? &decoded.left_relation
                                                : &decoded.right_relation,
                                index_key_out) ||
            !valid_type(index_key_out->type_code,
                        index_key_out->type_parameters) ||
            (index_key_out->type_code != 1 &&
             index_key_out->type_code != 3 &&
             index_key_out->type_code != 6))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        for (index = 0; index < decoded.join_key_count; ++index) {
            const prdb_sdk_optimizer_join_key_v1 *key =
                &decoded.join_keys[index];
            const prdb_sdk_optimizer_plan_column_v1 *inner_key =
                inner_side == 0 ? &key->left_column : &key->right_column;
            if (columns_equal(inner_key, index_key_out)) key_found = 1;
        }
        if (!key_found)
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        *has_filter_out = has_filter;
        *outer_side_out = outer_side;
        *inner_side_out = inner_side;
    }
    if (reader.offset != reader.length)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.struct_size = sizeof(decoded);
    decoded.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    *out = decoded;
    if (filter_out != NULL) *filter_out = decoded_filter;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_decode_join_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_join_plan_v1 *out, uint32_t out_capacity) {
    return decode_join_plan_internal(payload, payload_len, out, out_capacity,
                                     PRDB_SDK_OPTIMIZER_PLAN_KIND_JOIN_SELECT,
                                     NULL, NULL, NULL, NULL, NULL, NULL);
}

int32_t prdb_sdk_optimizer_decode_filtered_join_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_filtered_join_plan_v1 *out, uint32_t out_capacity) {
    prdb_sdk_optimizer_filtered_join_plan_v1 decoded = {0};
    int32_t status;
    if (payload == NULL || out == NULL || out_capacity < sizeof(*out) ||
        payload_len > PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES ||
        ranges_overlap(payload, payload_len, out, sizeof(*out)))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    status = decode_join_plan_internal(
        payload, payload_len, &decoded.join, sizeof(decoded.join),
        PRDB_SDK_OPTIMIZER_PLAN_KIND_FILTERED_JOIN_SELECT, &decoded.filter,
        NULL, NULL, NULL, NULL, NULL);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    decoded.struct_size = sizeof(decoded);
    decoded.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    *out = decoded;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_decode_index_join_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_index_join_plan_v1 *out, uint32_t out_capacity) {
    prdb_sdk_optimizer_index_join_plan_v1 decoded = {0};
    int32_t status;
    if (payload == NULL || out == NULL || out_capacity < sizeof(*out) ||
        payload_len > PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES ||
        ranges_overlap(payload, payload_len, out, sizeof(*out)))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    status = decode_join_plan_internal(
        payload, payload_len, &decoded.join, sizeof(decoded.join),
        PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_NESTED_LOOP_JOIN,
        &decoded.filter, &decoded.has_filter, &decoded.outer_side,
        &decoded.inner_side, decoded.index_id, &decoded.key_column);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    decoded.struct_size = sizeof(decoded);
    decoded.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    *out = decoded;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_decode_group_count_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_group_count_plan_v1 *out, uint32_t out_capacity) {
    prdb_sdk_optimizer_group_count_plan_v1 decoded = {0};
    plan_reader reader;
    uint16_t version;
    uint8_t kind, reserved, tag, flag;
    const uint8_t *bytes;
    uint16_t scan_count;

    if (payload == NULL || out == NULL || out_capacity < sizeof(*out) ||
        payload_len > PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES ||
        ranges_overlap(payload, payload_len, out, sizeof(*out))) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    reader.bytes = payload;
    reader.length = payload_len;
    reader.offset = 0;
    if (!prdb_decode_optimizer_plan_header_record(
            payload, PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES,
            &version, &kind, &reserved) || version != 1 || kind != 5 ||
        reserved != 0) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    reader.offset = PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    if (!read_name(&reader, &decoded.plan.schema_name,
                   &decoded.plan.schema_name_len) ||
        !take(&reader, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_schema_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.schema_id, &decoded.plan.schema_version) ||
        all_zero(decoded.plan.schema_id, 16) ||
        decoded.plan.schema_version == 0 ||
        !read_name(&reader, &decoded.plan.relation_name,
                   &decoded.plan.relation_name_len) ||
        !take(&reader, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_relation_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.relation_id, &decoded.plan.table_epoch) ||
        all_zero(decoded.plan.relation_id, 16) ||
        decoded.plan.table_epoch == 0 || !take(&reader, 2, &bytes)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    scan_count = read_be_u16(bytes);
    if (scan_count != 1 || !read_column(&reader, &decoded.plan.scan_columns[0]) ||
        !read_u8(&reader, &tag) || tag != 7 ||
        !read_column(&reader, &decoded.group_key) ||
        !columns_equal(&decoded.plan.scan_columns[0], &decoded.group_key) ||
        !read_u8(&reader, &flag) || flag > 1) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    decoded.plan.scan_column_count = 1;
    decoded.plan.projection_count = 1;
    decoded.plan.projections[0] = decoded.group_key;
    decoded.plan.plan_kind = 5;
    decoded.plan.has_limit = flag;
    if (flag != 0) {
        if (!take(&reader, 4, &bytes)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        decoded.plan.limit = ((uint32_t)bytes[0] << 24) |
                             ((uint32_t)bytes[1] << 16) |
                             ((uint32_t)bytes[2] << 8) | bytes[3];
    }
    if (reader.offset != reader.length) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    decoded.plan.struct_size = sizeof(decoded.plan);
    decoded.plan.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    decoded.struct_size = sizeof(decoded);
    decoded.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    *out = decoded;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_decode_ordered_group_count_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_ordered_group_count_plan_v1 *out,
    uint32_t out_capacity) {
    prdb_sdk_optimizer_ordered_group_count_plan_v1 decoded = {0};
    plan_reader reader;
    uint16_t version, scan_count;
    uint8_t kind, reserved, tag, flag, seen = 0;
    const uint8_t *bytes;

    if (payload == NULL || out == NULL || out_capacity < sizeof(*out) ||
        payload_len > PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES ||
        ranges_overlap(payload, payload_len, out, sizeof(*out)))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    reader.bytes = payload;
    reader.length = payload_len;
    reader.offset = 0;
    if (!prdb_decode_optimizer_plan_header_record(
            payload, PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES,
            &version, &kind, &reserved) || version != 1 || kind != 8 ||
        reserved != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    reader.offset = PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    if (!read_name(&reader, &decoded.plan.schema_name,
                   &decoded.plan.schema_name_len) ||
        !take(&reader, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_schema_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.schema_id, &decoded.plan.schema_version) ||
        all_zero(decoded.plan.schema_id, 16) || decoded.plan.schema_version == 0 ||
        !read_name(&reader, &decoded.plan.relation_name,
                   &decoded.plan.relation_name_len) ||
        !take(&reader, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_relation_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.relation_id, &decoded.plan.table_epoch) ||
        all_zero(decoded.plan.relation_id, 16) || decoded.plan.table_epoch == 0 ||
        !take(&reader, 2, &bytes))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    scan_count = read_be_u16(bytes);
    if (scan_count != 1 ||
        !read_column(&reader, &decoded.plan.scan_columns[0]) ||
        !read_u8(&reader, &tag) || tag != 7 ||
        !read_column(&reader, &decoded.group_key) ||
        !columns_equal(&decoded.plan.scan_columns[0], &decoded.group_key) ||
        !read_u8(&reader, &flag) || flag == 0 || flag > 2)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.order_count = flag;
    for (uint32_t index = 0; index < decoded.order_count; ++index) {
        uint8_t source, descending, nulls_first;
        if (!read_u8(&reader, &source) || source > 1 ||
            (seen & (uint8_t)(1U << source)) != 0 ||
            !read_u8(&reader, &descending) || descending > 1 ||
            !read_u8(&reader, &nulls_first) || nulls_first > 1)
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        seen |= (uint8_t)(1U << source);
        decoded.order_items[index].source = source;
        decoded.order_items[index].descending = descending;
        decoded.order_items[index].nulls_first = nulls_first;
    }
    if (!read_u8(&reader, &flag) || flag > 1)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.plan.has_limit = flag;
    if (flag != 0) {
        if (!take(&reader, 4, &bytes))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        decoded.plan.limit = ((uint32_t)bytes[0] << 24) |
                             ((uint32_t)bytes[1] << 16) |
                             ((uint32_t)bytes[2] << 8) | bytes[3];
    }
    if (reader.offset != reader.length)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.plan.scan_column_count = 1;
    decoded.plan.projection_count = 1;
    decoded.plan.projections[0] = decoded.group_key;
    decoded.plan.plan_kind = 8;
    decoded.plan.struct_size = sizeof(decoded.plan);
    decoded.plan.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    decoded.struct_size = sizeof(decoded);
    decoded.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    *out = decoded;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_decode_ordered_select_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_ordered_select_plan_v1 *out, uint32_t out_capacity) {
    prdb_sdk_optimizer_ordered_select_plan_v1 decoded = {0};
    plan_reader reader;
    uint16_t version;
    uint8_t kind, reserved, tag, flag;
    const uint8_t *bytes;
    uint16_t scan_count, projection_count;
    uint32_t index, other;
    uint8_t scan_referenced[PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS] = {0};

    if (payload == NULL || out == NULL || out_capacity < sizeof(*out) ||
        payload_len > PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES ||
        ranges_overlap(payload, payload_len, out, sizeof(*out))) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    reader.bytes = payload;
    reader.length = payload_len;
    reader.offset = 0;
    if (!prdb_decode_optimizer_plan_header_record(
            payload, PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES,
            &version, &kind, &reserved) ||
        version != 1 || kind != 7 || reserved != 0) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    reader.offset = PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    if (!read_name(&reader, &decoded.plan.schema_name,
                   &decoded.plan.schema_name_len) ||
        !take(&reader, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_schema_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.schema_id, &decoded.plan.schema_version) ||
        all_zero(decoded.plan.schema_id, 16) ||
        decoded.plan.schema_version == 0 ||
        !read_name(&reader, &decoded.plan.relation_name,
                   &decoded.plan.relation_name_len) ||
        !take(&reader, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_relation_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.relation_id, &decoded.plan.table_epoch) ||
        all_zero(decoded.plan.relation_id, 16) ||
        decoded.plan.table_epoch == 0 || !take(&reader, 2, &bytes)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    scan_count = read_be_u16(bytes);
    if (scan_count == 0 ||
        scan_count > PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    decoded.plan.scan_column_count = scan_count;
    for (index = 0; index < scan_count; ++index) {
        if (!read_column(&reader, &decoded.plan.scan_columns[index]) ||
            (index != 0 &&
             decoded.plan.scan_columns[index - 1].ordinal >=
                 decoded.plan.scan_columns[index].ordinal)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
    }
    if (!take(&reader, 2, &bytes)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    projection_count = read_be_u16(bytes);
    if (projection_count == 0 ||
        projection_count > PRDB_SDK_OPTIMIZER_PLAN_MAX_PROJECTIONS) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    decoded.plan.projection_count = projection_count;
    for (index = 0; index < projection_count; ++index) {
        prdb_sdk_optimizer_plan_column_v1 *projection =
            &decoded.plan.projections[index];
        if (!read_column(&reader, projection)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        for (other = 0; other < scan_count; ++other) {
            if (columns_equal(projection, &decoded.plan.scan_columns[other])) {
                scan_referenced[other] = 1;
                break;
            }
        }
        if (other == scan_count) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        for (other = 0; other < index; ++other) {
            if (decoded.plan.projections[other].ordinal == projection->ordinal) {
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            }
        }
    }
    if (!read_u8(&reader, &tag) || tag != 9 ||
        !read_column(&reader, &decoded.order_key)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    for (other = 0; other < scan_count; ++other) {
        if (columns_equal(&decoded.order_key,
                          &decoded.plan.scan_columns[other])) {
            scan_referenced[other] = 1;
            break;
        }
    }
    if (other == scan_count) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    for (index = 0; index < scan_count; ++index) {
        if (scan_referenced[index] == 0) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
    }
    if (!read_u8(&reader, &flag) || flag > 1) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    decoded.descending = flag;
    if (!read_u8(&reader, &flag) || flag > 1) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    decoded.plan.has_limit = flag;
    if (flag != 0) {
        if (!take(&reader, 4, &bytes)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        decoded.plan.limit = ((uint32_t)bytes[0] << 24) |
                             ((uint32_t)bytes[1] << 16) |
                             ((uint32_t)bytes[2] << 8) | bytes[3];
    }
    if (reader.offset != reader.length) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    decoded.plan.plan_kind = 7;
    decoded.plan.struct_size = sizeof(decoded.plan);
    decoded.plan.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    decoded.struct_size = sizeof(decoded);
    decoded.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    *out = decoded;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_decode_composite_group_count_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_composite_group_count_plan_v1 *out,
    uint32_t out_capacity) {
    prdb_sdk_optimizer_composite_group_count_plan_v1 decoded = {0};
    plan_reader reader;
    uint16_t version;
    uint8_t kind, reserved, tag, flag, key_count;
    const uint8_t *bytes;
    uint16_t scan_count;

    if (payload == NULL || out == NULL || out_capacity < sizeof(*out) ||
        payload_len > PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES ||
        ranges_overlap(payload, payload_len, out, sizeof(*out)))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    reader.bytes = payload;
    reader.length = payload_len;
    reader.offset = 0;
    if (!prdb_decode_optimizer_plan_header_record(
            payload, PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES,
            &version, &kind, &reserved) || version != 1 || kind != 6 ||
        reserved != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    reader.offset = PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    if (!read_name(&reader, &decoded.plan.schema_name,
                   &decoded.plan.schema_name_len) ||
        !take(&reader, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_schema_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.schema_id, &decoded.plan.schema_version) ||
        all_zero(decoded.plan.schema_id, 16) ||
        decoded.plan.schema_version == 0 ||
        !read_name(&reader, &decoded.plan.relation_name,
                   &decoded.plan.relation_name_len) ||
        !take(&reader, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_relation_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.relation_id, &decoded.plan.table_epoch) ||
        all_zero(decoded.plan.relation_id, 16) ||
        decoded.plan.table_epoch == 0 || !take(&reader, 2, &bytes))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    scan_count = read_be_u16(bytes);
    if (scan_count < 2 || scan_count > 8)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    for (uint32_t index = 0; index < scan_count; ++index) {
        if (!read_column(&reader, &decoded.plan.scan_columns[index]) ||
            (index != 0 && decoded.plan.scan_columns[index - 1].ordinal >=
                               decoded.plan.scan_columns[index].ordinal))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (!read_u8(&reader, &tag) || tag != 8 || !read_u8(&reader, &key_count) ||
        key_count < 2 || key_count > 8 || key_count != scan_count)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    for (uint32_t key_index = 0; key_index < key_count; ++key_index) {
        prdb_sdk_optimizer_plan_column_v1 *key =
            &decoded.group_keys[key_index];
        if (!read_column(&reader, key))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        for (uint32_t prior = 0; prior < key_index; ++prior) {
            if (decoded.group_keys[prior].ordinal == key->ordinal)
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        int found = 0;
        for (uint32_t scan_index = 0; scan_index < scan_count; ++scan_index) {
            if (columns_equal(key, &decoded.plan.scan_columns[scan_index])) {
                found = 1;
                break;
            }
        }
        if (!found)
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        decoded.plan.projections[key_index] = *key;
    }
    if (!read_u8(&reader, &flag) || flag > 1)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.plan.scan_column_count = scan_count;
    decoded.group_key_count = key_count;
    decoded.plan.projection_count = key_count;
    decoded.plan.plan_kind = 6;
    decoded.plan.has_limit = flag;
    if (flag != 0) {
        if (!take(&reader, 4, &bytes))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        decoded.plan.limit = ((uint32_t)bytes[0] << 24) |
                             ((uint32_t)bytes[1] << 16) |
                             ((uint32_t)bytes[2] << 8) | bytes[3];
    }
    if (reader.offset != reader.length)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.plan.struct_size = sizeof(decoded.plan);
    decoded.plan.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    decoded.struct_size = sizeof(decoded);
    decoded.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    *out = decoded;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_decode_ordered_composite_group_count_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_ordered_composite_group_count_plan_v1 *out,
    uint32_t out_capacity) {
    prdb_sdk_optimizer_ordered_composite_group_count_plan_v1 decoded = {0};
    plan_reader reader;
    uint16_t version, scan_count;
    uint8_t kind, reserved, tag, flag;
    uint16_t seen_keys = 0;
    uint32_t seen_count = 0;
    const uint8_t *bytes;
    if (payload == NULL || out == NULL || out_capacity < sizeof(*out) ||
        payload_len > PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES ||
        ranges_overlap(payload, payload_len, out, sizeof(*out)))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    reader.bytes = payload; reader.length = payload_len; reader.offset = 0;
    if (!prdb_decode_optimizer_plan_header_record(payload, PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES, &version, &kind, &reserved) || version != 1 || kind != 9 || reserved != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    reader.offset = PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    if (!read_name(&reader, &decoded.plan.schema_name, &decoded.plan.schema_name_len) ||
        !take(&reader, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES, &bytes) ||
        !prdb_decode_optimizer_plan_schema_reference_record(bytes, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES, decoded.plan.schema_id, &decoded.plan.schema_version) || all_zero(decoded.plan.schema_id, 16) || decoded.plan.schema_version == 0 ||
        !read_name(&reader, &decoded.plan.relation_name, &decoded.plan.relation_name_len) ||
        !take(&reader, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES, &bytes) ||
        !prdb_decode_optimizer_plan_relation_reference_record(bytes, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES, decoded.plan.relation_id, &decoded.plan.table_epoch) || all_zero(decoded.plan.relation_id, 16) || decoded.plan.table_epoch == 0 || !take(&reader, 2, &bytes))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    scan_count = read_be_u16(bytes);
    if (scan_count < 2 || scan_count > 8) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    for (uint32_t i = 0; i < scan_count; ++i)
        if (!read_column(&reader, &decoded.plan.scan_columns[i]) || (i && decoded.plan.scan_columns[i-1].ordinal >= decoded.plan.scan_columns[i].ordinal)) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (!read_u8(&reader, &tag) || tag != 10 || !read_u8(&reader, &flag) || flag < 2 || flag > 8 || flag != scan_count) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.group_key_count = flag;
    for (uint32_t i = 0; i < decoded.group_key_count; ++i) {
        if (!read_column(&reader, &decoded.group_keys[i])) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        for (uint32_t j = 0; j < i; ++j) if (decoded.group_keys[j].ordinal == decoded.group_keys[i].ordinal) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        uint32_t found = 0;
        for (uint32_t j = 0; j < scan_count; ++j) if (columns_equal(&decoded.group_keys[i], &decoded.plan.scan_columns[j])) found = 1;
        if (!found) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        decoded.plan.projections[i] = decoded.group_keys[i];
    }
    if (!read_u8(&reader, &flag) || flag == 0 || flag > 9) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.order_count = flag;
    for (uint32_t i = 0; i < decoded.order_count; ++i) {
        uint8_t source, index, descending, nulls_first;
        if (!read_u8(&reader, &source) || source > 1 || !read_u8(&reader, &index) || !read_u8(&reader, &descending) || descending > 1 || !read_u8(&reader, &nulls_first) || nulls_first > 1) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        if (source == 0) { if (index >= decoded.group_key_count || (seen_keys & (1u << index))) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD; seen_keys |= (uint16_t)(1u << index); }
        else { if (index != UINT8_MAX || seen_count) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD; seen_count = 1; }
        decoded.order_items[i].source = source;
        decoded.order_items[i].group_key_index = source == 0 ? index : UINT32_MAX;
        decoded.order_items[i].descending = descending;
        decoded.order_items[i].nulls_first = nulls_first;
    }
    if (!read_u8(&reader, &flag) || flag > 1) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.plan.has_limit = flag;
    if (flag && (!take(&reader, 4, &bytes))) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (flag) decoded.plan.limit = ((uint32_t)bytes[0] << 24) | ((uint32_t)bytes[1] << 16) | ((uint32_t)bytes[2] << 8) | bytes[3];
    if (reader.offset != reader.length) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.plan.scan_column_count = scan_count; decoded.plan.projection_count = decoded.group_key_count; decoded.plan.plan_kind = 9;
    decoded.plan.struct_size = sizeof(decoded.plan); decoded.plan.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    decoded.struct_size = sizeof(decoded); decoded.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    *out = decoded;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_decode_group_aggregate_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_group_aggregate_plan_v1 *out,
    uint32_t out_capacity) {
    prdb_sdk_optimizer_group_aggregate_plan_v1 decoded = {0};
    plan_reader reader;
    uint16_t version, scan_count;
    uint8_t kind, reserved, tag, aggregate_kind, flag;
    const uint8_t *bytes;
    if (payload == NULL || out == NULL || out_capacity < sizeof(*out) || payload_len > PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES || ranges_overlap(payload, payload_len, out, sizeof(*out)))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    reader.bytes = payload; reader.length = payload_len; reader.offset = 0;
    if (!prdb_decode_optimizer_plan_header_record(payload, PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES, &version, &kind, &reserved) || version != 1 || kind != 10 || reserved != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    reader.offset = PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    if (!read_name(&reader, &decoded.plan.schema_name, &decoded.plan.schema_name_len) ||
        !take(&reader, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES, &bytes) ||
        !prdb_decode_optimizer_plan_schema_reference_record(bytes, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES, decoded.plan.schema_id, &decoded.plan.schema_version) || all_zero(decoded.plan.schema_id, 16) || decoded.plan.schema_version == 0 ||
        !read_name(&reader, &decoded.plan.relation_name, &decoded.plan.relation_name_len) ||
        !take(&reader, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES, &bytes) ||
        !prdb_decode_optimizer_plan_relation_reference_record(bytes, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES, decoded.plan.relation_id, &decoded.plan.table_epoch) || all_zero(decoded.plan.relation_id, 16) || decoded.plan.table_epoch == 0 || !take(&reader, 2, &bytes))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    scan_count = read_be_u16(bytes);
    if (scan_count < 1 || scan_count > 2) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    for (uint32_t i = 0; i < scan_count; ++i)
        if (!read_column(&reader, &decoded.plan.scan_columns[i]) || (i && decoded.plan.scan_columns[i-1].ordinal >= decoded.plan.scan_columns[i].ordinal)) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (!read_u8(&reader, &tag) || tag != 11 || !read_u8(&reader, &aggregate_kind) || aggregate_kind < 1 || aggregate_kind > PRDB_SDK_SQL_V7_AGGREGATE_COUNT_COLUMN || !read_column(&reader, &decoded.group_key) || !read_u8(&reader, &flag) || flag > 1)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.aggregate_kind = aggregate_kind;
    decoded.has_aggregate_column = flag;
    if ((aggregate_kind == 1 && flag != 0) || (aggregate_kind != 1 && flag != 1)) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (flag && !read_column(&reader, &decoded.aggregate_column)) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if ((aggregate_kind == 4 || aggregate_kind == 5) && decoded.aggregate_column.type_code != PRDB_SDK_VALUE_TYPE_SMALLINT && decoded.aggregate_column.type_code != PRDB_SDK_VALUE_TYPE_INTEGER && decoded.aggregate_column.type_code != PRDB_SDK_VALUE_TYPE_BIGINT && decoded.aggregate_column.type_code != PRDB_SDK_VALUE_TYPE_DECIMAL)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    uint32_t expected_scan = 0;
    for (uint32_t i = 0; i < scan_count; ++i) {
        if (columns_equal(&decoded.group_key, &decoded.plan.scan_columns[i])) expected_scan |= 1;
        if (flag && columns_equal(&decoded.aggregate_column, &decoded.plan.scan_columns[i])) expected_scan |= 2;
    }
    if (!(expected_scan & 1) || (flag && !(expected_scan & 2))) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if ((!flag || columns_equal(&decoded.group_key, &decoded.aggregate_column)) && scan_count != 1) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (flag && !columns_equal(&decoded.group_key, &decoded.aggregate_column) && scan_count != 2) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (!read_u8(&reader, &flag) || flag > 1) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.plan.has_limit = flag;
    if (flag && !take(&reader, 4, &bytes)) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (flag) decoded.plan.limit = ((uint32_t)bytes[0] << 24) | ((uint32_t)bytes[1] << 16) | ((uint32_t)bytes[2] << 8) | bytes[3];
    if (reader.offset != reader.length) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.plan.scan_column_count = scan_count; decoded.plan.projection_count = 1; decoded.plan.projections[0] = decoded.group_key; decoded.plan.plan_kind = 10;
    decoded.plan.struct_size = sizeof(decoded.plan); decoded.plan.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    decoded.struct_size = sizeof(decoded); decoded.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    *out = decoded;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_decode_composite_group_aggregate_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_composite_group_aggregate_plan_v1 *out,
    uint32_t out_capacity) {
    prdb_sdk_optimizer_composite_group_aggregate_plan_v1 decoded = {0};
    plan_reader reader;
    uint16_t version, scan_count;
    uint8_t kind, reserved, tag, aggregate_kind, key_count, flag;
    const uint8_t *bytes;
    if (payload == NULL || out == NULL || out_capacity < sizeof(*out) ||
        payload_len > PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES ||
        ranges_overlap(payload, payload_len, out, sizeof(*out)))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    reader.bytes = payload;
    reader.length = payload_len;
    reader.offset = 0;
    if (!prdb_decode_optimizer_plan_header_record(
            payload, PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES,
            &version, &kind, &reserved) || version != 1 || kind != 13 ||
        reserved != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    reader.offset = PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    if (!read_name(&reader, &decoded.plan.schema_name,
                   &decoded.plan.schema_name_len) ||
        !take(&reader, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_schema_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.schema_id, &decoded.plan.schema_version) ||
        all_zero(decoded.plan.schema_id, 16) || decoded.plan.schema_version == 0 ||
        !read_name(&reader, &decoded.plan.relation_name,
                   &decoded.plan.relation_name_len) ||
        !take(&reader, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_relation_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.relation_id, &decoded.plan.table_epoch) ||
        all_zero(decoded.plan.relation_id, 16) || decoded.plan.table_epoch == 0 ||
        !take(&reader, 2, &bytes))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    scan_count = read_be_u16(bytes);
    if (scan_count < PRDB_SDK_SQL_V13_MIN_GROUP_KEYS ||
        scan_count > PRDB_SDK_SQL_V13_MAX_GROUP_KEYS + 1 ||
        scan_count > PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    for (uint32_t index = 0; index < scan_count; ++index) {
        if (!read_column(&reader, &decoded.plan.scan_columns[index]) ||
            (index != 0 && decoded.plan.scan_columns[index - 1].ordinal >=
                               decoded.plan.scan_columns[index].ordinal))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (!read_u8(&reader, &tag) || tag != 13 ||
        !read_u8(&reader, &aggregate_kind) || aggregate_kind < 1 ||
        aggregate_kind > PRDB_SDK_SQL_V7_AGGREGATE_COUNT_COLUMN || !read_u8(&reader, &key_count) ||
        key_count < PRDB_SDK_SQL_V13_MIN_GROUP_KEYS ||
        key_count > PRDB_SDK_SQL_V13_MAX_GROUP_KEYS)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.group_key_count = key_count;
    decoded.aggregate_kind = aggregate_kind;
    for (uint32_t index = 0; index < key_count; ++index) {
        if (!read_column(&reader, &decoded.group_keys[index]))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        for (uint32_t prior = 0; prior < index; ++prior) {
            if (decoded.group_keys[prior].ordinal ==
                decoded.group_keys[index].ordinal)
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
    }
    if (!read_u8(&reader, &flag) || flag > 1 ||
        flag != (aggregate_kind == 1 ? 0 : 1))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.has_aggregate_column = flag;
    if (flag && !read_column(&reader, &decoded.aggregate_column))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (flag && (aggregate_kind == 4 || aggregate_kind == 5) &&
        decoded.aggregate_column.type_code != PRDB_SDK_VALUE_TYPE_SMALLINT &&
        decoded.aggregate_column.type_code != PRDB_SDK_VALUE_TYPE_INTEGER &&
        decoded.aggregate_column.type_code != PRDB_SDK_VALUE_TYPE_BIGINT &&
        decoded.aggregate_column.type_code != PRDB_SDK_VALUE_TYPE_DECIMAL)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    uint32_t expected_scan = 0;
    uint32_t aggregate_in_keys = 0;
    for (uint32_t index = 0; index < scan_count; ++index) {
        for (uint32_t key = 0; key < key_count; ++key) {
            if (columns_equal(&decoded.plan.scan_columns[index],
                              &decoded.group_keys[key])) {
                expected_scan |= UINT32_C(1) << key;
                if (flag && columns_equal(&decoded.group_keys[key],
                                          &decoded.aggregate_column))
                    aggregate_in_keys = 1;
            }
        }
        if (flag && columns_equal(&decoded.plan.scan_columns[index],
                                  &decoded.aggregate_column))
            expected_scan |= UINT32_C(1) << 8;
    }
    if ((expected_scan & ((UINT32_C(1) << key_count) - 1)) !=
            ((UINT32_C(1) << key_count) - 1) ||
        (flag && !(expected_scan & (UINT32_C(1) << 8))) ||
        (flag && !aggregate_in_keys && scan_count != key_count + 1) ||
        ((!flag || aggregate_in_keys) && scan_count != key_count))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (!read_u8(&reader, &flag) || flag > 1)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.plan.has_limit = flag;
    if (flag) {
        if (!take(&reader, 4, &bytes))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        decoded.plan.limit = ((uint32_t)bytes[0] << 24) |
                             ((uint32_t)bytes[1] << 16) |
                             ((uint32_t)bytes[2] << 8) | bytes[3];
    }
    if (reader.offset != reader.length)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.plan.scan_column_count = scan_count;
    decoded.plan.projection_count = key_count;
    decoded.plan.plan_kind = 13;
    for (uint32_t index = 0; index < key_count; ++index)
        decoded.plan.projections[index] = decoded.group_keys[index];
    decoded.plan.struct_size = sizeof(decoded.plan);
    decoded.plan.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    decoded.struct_size = sizeof(decoded);
    decoded.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    *out = decoded;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_decode_filtered_composite_group_aggregate_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_filtered_composite_group_aggregate_plan_v1 *out,
    uint32_t out_capacity) {
    prdb_sdk_optimizer_filtered_composite_group_aggregate_plan_v1 decoded = {0};
    plan_reader reader;
    predicate_type filter_type;
    uint16_t version;
    uint8_t kind, reserved, tag, aggregate_kind, key_count, flag;
    uint16_t scan_count;
    uint32_t predicate_nodes = 0;
    uint8_t filter_references[PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS] = {0};
    const uint8_t *bytes;
    if (payload == NULL || out == NULL || out_capacity < sizeof(*out) ||
        payload_len > PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES ||
        ranges_overlap(payload, payload_len, out, sizeof(*out)))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    reader = (plan_reader){payload, payload_len, 0};
    if (!prdb_decode_optimizer_plan_header_record(
            payload, PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES,
            &version, &kind, &reserved) || version != 1 || kind != 15 ||
        reserved != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    reader.offset = PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    if (!read_name(&reader, &decoded.plan.schema_name,
                   &decoded.plan.schema_name_len) ||
        !take(&reader, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_schema_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.schema_id, &decoded.plan.schema_version) ||
        all_zero(decoded.plan.schema_id, 16) || decoded.plan.schema_version == 0 ||
        !read_name(&reader, &decoded.plan.relation_name,
                   &decoded.plan.relation_name_len) ||
        !take(&reader, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_relation_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.relation_id, &decoded.plan.table_epoch) ||
        all_zero(decoded.plan.relation_id, 16) || decoded.plan.table_epoch == 0 ||
        !take(&reader, 2, &bytes))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    scan_count = read_be_u16(bytes);
    if (scan_count < 1 ||
        scan_count > PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.plan.scan_column_count = scan_count;
    for (uint32_t index = 0; index < scan_count; ++index) {
        if (!read_column(&reader, &decoded.plan.scan_columns[index]) ||
            (index != 0 && decoded.plan.scan_columns[index - 1].ordinal >=
                               decoded.plan.scan_columns[index].ordinal))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        for (uint32_t prior = 0; prior < index; ++prior)
            if (memcmp(decoded.plan.scan_columns[prior].column_id,
                       decoded.plan.scan_columns[index].column_id, 16) == 0)
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    {
        uint64_t filter_header = reader.offset;
        uint64_t predicate_start;
        if (!read_u8(&reader, &tag) || tag != 2 ||
            !read_filter_node_tag(&reader, filter_header))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        predicate_start = reader.offset;
        if (!decode_predicate(&reader, decoded.plan.scan_columns, scan_count, 1,
                              &predicate_nodes, filter_references,
                              &filter_type) || !predicate_boolean(&filter_type))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        decoded.filter.struct_size = sizeof(decoded.filter);
        decoded.filter.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
        decoded.filter.present = 1;
        decoded.filter.prefix = payload + predicate_start;
        decoded.filter.prefix_len = (uint32_t)(reader.offset - predicate_start);
    }
    if (!read_u8(&reader, &tag) || tag != 15 ||
        !read_u8(&reader, &aggregate_kind) || aggregate_kind < 1 ||
        aggregate_kind > PRDB_SDK_SQL_V7_AGGREGATE_COUNT_COLUMN || !read_u8(&reader, &key_count) ||
        key_count < 1 ||
        key_count > PRDB_SDK_SQL_V13_MAX_GROUP_KEYS)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.group_key_count = key_count;
    decoded.aggregate_kind = aggregate_kind;
    for (uint32_t index = 0; index < key_count; ++index) {
        if (!read_column(&reader, &decoded.group_keys[index]))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        for (uint32_t prior = 0; prior < index; ++prior)
            if (decoded.group_keys[prior].ordinal ==
                decoded.group_keys[index].ordinal)
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (!read_u8(&reader, &flag) || flag > 1 ||
        flag != (aggregate_kind == 1 ? 0 : 1))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.has_aggregate_column = flag;
    if (flag && !read_column(&reader, &decoded.aggregate_column))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (flag && (aggregate_kind == 4 || aggregate_kind == 5) &&
        decoded.aggregate_column.type_code != PRDB_SDK_VALUE_TYPE_SMALLINT &&
        decoded.aggregate_column.type_code != PRDB_SDK_VALUE_TYPE_INTEGER &&
        decoded.aggregate_column.type_code != PRDB_SDK_VALUE_TYPE_BIGINT &&
        decoded.aggregate_column.type_code != PRDB_SDK_VALUE_TYPE_DECIMAL)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    {
        uint8_t required[PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS] = {0};
        for (uint32_t index = 0; index < key_count; ++index) {
            uint32_t found = 0;
            for (uint32_t scan = 0; scan < scan_count; ++scan)
                if (columns_equal(&decoded.group_keys[index],
                                  &decoded.plan.scan_columns[scan])) {
                    required[scan] = 1;
                    found = 1;
                }
            if (!found) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        if (flag) {
            uint32_t found = 0;
            for (uint32_t scan = 0; scan < scan_count; ++scan)
                if (columns_equal(&decoded.aggregate_column,
                                  &decoded.plan.scan_columns[scan])) {
                    required[scan] = 1;
                    found = 1;
                }
            if (!found) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        for (uint32_t scan = 0; scan < scan_count; ++scan) {
            if (filter_references[scan]) required[scan] = 1;
            if (!required[scan]) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
    }
    if (!read_u8(&reader, &flag) || flag > 1)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.plan.has_limit = flag;
    if (flag) {
        if (!take(&reader, 4, &bytes))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        decoded.plan.limit = ((uint32_t)bytes[0] << 24) |
                             ((uint32_t)bytes[1] << 16) |
                             ((uint32_t)bytes[2] << 8) | bytes[3];
    }
    if (reader.offset != reader.length)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.plan.projection_count = key_count;
    decoded.plan.plan_kind = 15;
    for (uint32_t index = 0; index < key_count; ++index)
        decoded.plan.projections[index] = decoded.group_keys[index];
    decoded.plan.struct_size = sizeof(decoded.plan);
    decoded.plan.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    decoded.struct_size = sizeof(decoded);
    decoded.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    *out = decoded;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_decode_ordered_filtered_composite_group_aggregate_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_ordered_filtered_composite_group_aggregate_plan_v1 *out,
    uint32_t out_capacity) {
    uint8_t normalized[PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES];
    prdb_sdk_optimizer_filtered_composite_group_aggregate_plan_v1 base = {0};
    prdb_sdk_optimizer_ordered_filtered_composite_group_aggregate_plan_v1 decoded = {0};
    plan_reader reader;
    predicate_type filter_type;
    uint16_t version, scan_count;
    uint8_t kind, reserved, tag, key_count, aggregate_kind, flag;
    uint8_t seen_keys = 0;
    uint32_t seen_aggregate = 0;
    uint32_t predicate_nodes = 0;
    uint8_t filter_references[PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS] = {0};
    uint64_t node_offset, order_start, order_end, filter_predicate_start;
    const uint8_t *filter_prefix = NULL;
    const uint8_t *schema_name_ptr = NULL;
    const uint8_t *relation_name_ptr = NULL;
    uint32_t filter_prefix_len = 0;
    const uint8_t *bytes;
    if (payload == NULL || out == NULL || out_capacity < sizeof(*out) ||
        payload_len > PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES ||
        ranges_overlap(payload, payload_len, out, sizeof(*out)))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    reader = (plan_reader){payload, payload_len, 0};
    if (!prdb_decode_optimizer_plan_header_record(
            payload, PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES,
            &version, &kind, &reserved) || version != 1 || kind != 16 || reserved != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    reader.offset = PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    if (!read_name(&reader, &decoded.plan.schema_name, &decoded.plan.schema_name_len) ||
        !take(&reader, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES, &bytes) ||
        !prdb_decode_optimizer_plan_schema_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.schema_id, &decoded.plan.schema_version) ||
        all_zero(decoded.plan.schema_id, 16) || decoded.plan.schema_version == 0 ||
        !read_name(&reader, &decoded.plan.relation_name, &decoded.plan.relation_name_len) ||
        !take(&reader, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES, &bytes) ||
        !prdb_decode_optimizer_plan_relation_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.relation_id, &decoded.plan.table_epoch) ||
        all_zero(decoded.plan.relation_id, 16) || decoded.plan.table_epoch == 0 ||
        !take(&reader, 2, &bytes))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    schema_name_ptr = decoded.plan.schema_name;
    relation_name_ptr = decoded.plan.relation_name;
    scan_count = read_be_u16(bytes);
    if (scan_count == 0 || scan_count > PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.plan.scan_column_count = scan_count;
    for (uint32_t index = 0; index < scan_count; ++index) {
        if (!read_column(&reader, &decoded.plan.scan_columns[index]) ||
            (index != 0 && decoded.plan.scan_columns[index - 1].ordinal >=
                               decoded.plan.scan_columns[index].ordinal))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    {
        uint64_t filter_header = reader.offset;
        if (!read_u8(&reader, &tag) || tag != 2 || !read_filter_node_tag(&reader, filter_header))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        filter_predicate_start = reader.offset;
        if (
            !decode_predicate(&reader, decoded.plan.scan_columns, scan_count, 1,
                              &predicate_nodes, filter_references, &filter_type) ||
            !predicate_boolean(&filter_type))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        decoded.filter.struct_size = sizeof(decoded.filter);
        decoded.filter.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
        decoded.filter.present = 1;
        decoded.filter.prefix = payload + filter_predicate_start;
        decoded.filter.prefix_len = (uint32_t)(reader.offset - filter_predicate_start);
        filter_prefix = decoded.filter.prefix;
        filter_prefix_len = decoded.filter.prefix_len;
    }
    node_offset = reader.offset;
    if (!read_u8(&reader, &tag) || tag != 16 || !read_u8(&reader, &aggregate_kind) ||
        aggregate_kind < 1 || aggregate_kind > PRDB_SDK_SQL_V7_AGGREGATE_COUNT_COLUMN || !read_u8(&reader, &key_count) ||
        key_count < 1 || key_count > PRDB_SDK_SQL_V13_MAX_GROUP_KEYS)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.group_key_count = key_count;
    decoded.aggregate_kind = aggregate_kind;
    for (uint32_t index = 0; index < key_count; ++index) {
        if (!read_column(&reader, &decoded.group_keys[index]))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        for (uint32_t prior = 0; prior < index; ++prior)
            if (decoded.group_keys[prior].ordinal == decoded.group_keys[index].ordinal)
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (!read_u8(&reader, &flag) || flag > 1 || flag != (aggregate_kind == 1 ? 0 : 1))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.has_aggregate_column = flag;
    if (flag && !read_column(&reader, &decoded.aggregate_column))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    order_start = reader.offset;
    if (!read_u8(&reader, &tag) || tag == 0 || tag > 9)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.order_count = tag;
    for (uint32_t index = 0; index < decoded.order_count; ++index) {
        uint8_t source, key_index = 0, descending, nulls_first;
        if (!read_u8(&reader, &source)) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        if (source == 0) {
            if (!read_u8(&reader, &key_index) || key_index >= key_count ||
                (seen_keys & (1u << key_index)))
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            seen_keys |= (uint8_t)(1u << key_index);
        } else if (source == 1 && !seen_aggregate) {
            seen_aggregate = 1;
        } else {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        if (!read_u8(&reader, &descending) || descending > 1 ||
            !read_u8(&reader, &nulls_first) || nulls_first > 1)
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        decoded.order_items[index] = (prdb_sdk_optimizer_composite_order_item_v1){
            source, key_index, descending, nulls_first};
    }
    order_end = reader.offset;
    if (!read_u8(&reader, &flag) || flag > 1 ||
        (flag && !take(&reader, 4, &bytes)) || reader.offset != reader.length)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    memcpy(normalized, payload, (size_t)node_offset);
    normalized[2] = 15;
    normalized[node_offset] = 15;
    memcpy(normalized + node_offset + 1, payload + node_offset + 1,
           (size_t)(order_start - node_offset - 1));
    memcpy(normalized + order_start, payload + order_end,
           (size_t)(payload_len - order_end));
    {
        uint64_t normalized_len = payload_len - (order_end - order_start);
        int32_t status = prdb_sdk_optimizer_decode_filtered_composite_group_aggregate_plan_v1(
            normalized, normalized_len, &base, sizeof(base));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        decoded.plan = base.plan;
        decoded.plan.plan_kind = 16;
        decoded.plan.schema_name = schema_name_ptr;
        decoded.plan.relation_name = relation_name_ptr;
        decoded.filter = base.filter;
        decoded.filter.prefix = filter_prefix;
        decoded.filter.prefix_len = filter_prefix_len;
        decoded.group_key_count = base.group_key_count;
        memcpy(decoded.group_keys, base.group_keys, sizeof(decoded.group_keys));
        decoded.aggregate_kind = base.aggregate_kind;
        decoded.has_aggregate_column = base.has_aggregate_column;
        decoded.aggregate_column = base.aggregate_column;
    }
    decoded.struct_size = sizeof(decoded);
    decoded.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    *out = decoded;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_decode_ordered_composite_group_aggregate_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_ordered_composite_group_aggregate_plan_v1 *out,
    uint32_t out_capacity) {
    prdb_sdk_optimizer_ordered_composite_group_aggregate_plan_v1 decoded = {0};
    plan_reader reader;
    uint16_t version, scan_count;
    uint8_t kind, reserved, tag, aggregate_kind, key_count, flag;
    uint16_t seen_keys = 0;
    uint32_t seen_aggregate = 0;
    const uint8_t *bytes;
    if (payload == NULL || out == NULL || out_capacity < sizeof(*out) ||
        payload_len > PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES ||
        ranges_overlap(payload, payload_len, out, sizeof(*out)))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    reader.bytes = payload;
    reader.length = payload_len;
    reader.offset = 0;
    if (!prdb_decode_optimizer_plan_header_record(
            payload, PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES,
            &version, &kind, &reserved) ||
        version != 1 || kind != 14 || reserved != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    reader.offset = PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    if (!read_name(&reader, &decoded.plan.schema_name,
                   &decoded.plan.schema_name_len) ||
        !take(&reader, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_schema_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.schema_id, &decoded.plan.schema_version) ||
        all_zero(decoded.plan.schema_id, 16) || decoded.plan.schema_version == 0 ||
        !read_name(&reader, &decoded.plan.relation_name,
                   &decoded.plan.relation_name_len) ||
        !take(&reader, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_relation_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.relation_id, &decoded.plan.table_epoch) ||
        all_zero(decoded.plan.relation_id, 16) || decoded.plan.table_epoch == 0 ||
        !take(&reader, 2, &bytes))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    scan_count = read_be_u16(bytes);
    if (scan_count < PRDB_SDK_SQL_V13_MIN_GROUP_KEYS ||
        scan_count > PRDB_SDK_SQL_V13_MAX_GROUP_KEYS + 1 ||
        scan_count > PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    for (uint32_t index = 0; index < scan_count; ++index) {
        if (!read_column(&reader, &decoded.plan.scan_columns[index]) ||
            (index != 0 && decoded.plan.scan_columns[index - 1].ordinal >=
                               decoded.plan.scan_columns[index].ordinal))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (!read_u8(&reader, &tag) || tag != 14 ||
        !read_u8(&reader, &aggregate_kind) || aggregate_kind < 1 ||
        aggregate_kind > PRDB_SDK_SQL_V7_AGGREGATE_COUNT_COLUMN || !read_u8(&reader, &key_count) ||
        key_count < PRDB_SDK_SQL_V13_MIN_GROUP_KEYS ||
        key_count > PRDB_SDK_SQL_V13_MAX_GROUP_KEYS)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.group_key_count = key_count;
    decoded.aggregate_kind = aggregate_kind;
    for (uint32_t index = 0; index < key_count; ++index) {
        if (!read_column(&reader, &decoded.group_keys[index]))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        for (uint32_t prior = 0; prior < index; ++prior)
            if (decoded.group_keys[prior].ordinal ==
                decoded.group_keys[index].ordinal)
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (!read_u8(&reader, &flag) || flag > 1 ||
        flag != (aggregate_kind == 1 ? 0 : 1))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.has_aggregate_column = flag;
    if (flag && !read_column(&reader, &decoded.aggregate_column))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (flag && (aggregate_kind == 4 || aggregate_kind == 5) &&
        decoded.aggregate_column.type_code != PRDB_SDK_VALUE_TYPE_SMALLINT &&
        decoded.aggregate_column.type_code != PRDB_SDK_VALUE_TYPE_INTEGER &&
        decoded.aggregate_column.type_code != PRDB_SDK_VALUE_TYPE_BIGINT &&
        decoded.aggregate_column.type_code != PRDB_SDK_VALUE_TYPE_DECIMAL)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    uint32_t expected_scan = 0;
    uint32_t aggregate_in_keys = 0;
    for (uint32_t index = 0; index < scan_count; ++index) {
        for (uint32_t key = 0; key < key_count; ++key) {
            if (columns_equal(&decoded.plan.scan_columns[index],
                              &decoded.group_keys[key])) {
                expected_scan |= UINT32_C(1) << key;
                if (flag && columns_equal(&decoded.group_keys[key],
                                          &decoded.aggregate_column))
                    aggregate_in_keys = 1;
            }
        }
        if (flag && columns_equal(&decoded.plan.scan_columns[index],
                                  &decoded.aggregate_column))
            expected_scan |= UINT32_C(1) << 8;
    }
    if ((expected_scan & ((UINT32_C(1) << key_count) - 1)) !=
            ((UINT32_C(1) << key_count) - 1) ||
        (flag && !(expected_scan & (UINT32_C(1) << 8))) ||
        (flag && !aggregate_in_keys && scan_count != key_count + 1) ||
        ((!flag || aggregate_in_keys) && scan_count != key_count))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (!read_u8(&reader, &tag) || tag != 14 || !read_u8(&reader, &tag) ||
        tag < 1 || tag > 9)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.order_count = tag;
    for (uint32_t index = 0; index < decoded.order_count; ++index) {
        uint8_t source, key_index = UINT8_MAX, descending, nulls_first;
        if (!read_u8(&reader, &source) || source > 1 ||
            (source == 0 && !read_u8(&reader, &key_index)) ||
            !read_u8(&reader, &descending) || descending > 1 ||
            !read_u8(&reader, &nulls_first) || nulls_first > 1)
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        if (source == 0) {
            if (key_index >= key_count || (seen_keys & (1u << key_index)))
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            seen_keys |= (uint16_t)(1u << key_index);
        } else if (seen_aggregate || key_index != UINT8_MAX) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        } else {
            seen_aggregate = 1;
        }
        decoded.order_items[index].source = source;
        decoded.order_items[index].group_key_index =
            source == 0 ? key_index : UINT32_MAX;
        decoded.order_items[index].descending = descending;
        decoded.order_items[index].nulls_first = nulls_first;
    }
    if (!read_u8(&reader, &flag) || flag > 1)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.plan.has_limit = flag;
    if (flag) {
        if (!take(&reader, 4, &bytes))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        decoded.plan.limit = ((uint32_t)bytes[0] << 24) |
                             ((uint32_t)bytes[1] << 16) |
                             ((uint32_t)bytes[2] << 8) | bytes[3];
    }
    if (reader.offset != reader.length)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.plan.scan_column_count = scan_count;
    decoded.plan.projection_count = key_count;
    decoded.plan.plan_kind = 14;
    for (uint32_t index = 0; index < key_count; ++index)
        decoded.plan.projections[index] = decoded.group_keys[index];
    decoded.plan.struct_size = sizeof(decoded.plan);
    decoded.plan.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    decoded.struct_size = sizeof(decoded);
    decoded.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    *out = decoded;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_decode_ordered_group_aggregate_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_ordered_group_aggregate_plan_v1 *out,
    uint32_t out_capacity) {
    prdb_sdk_optimizer_ordered_group_aggregate_plan_v1 decoded = {0};
    plan_reader reader;
    uint16_t version, scan_count;
    uint8_t kind, reserved, tag, aggregate_kind, flag, seen = 0;
    const uint8_t *bytes;
    if (payload == NULL || out == NULL || out_capacity < sizeof(*out) ||
        payload_len > PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES ||
        ranges_overlap(payload, payload_len, out, sizeof(*out)))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    reader.bytes = payload;
    reader.length = payload_len;
    reader.offset = 0;
    if (!prdb_decode_optimizer_plan_header_record(
            payload, PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES,
            &version, &kind, &reserved) ||
        version != 1 || kind != 11 || reserved != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    reader.offset = PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    if (!read_name(&reader, &decoded.plan.schema_name,
                   &decoded.plan.schema_name_len) ||
        !take(&reader, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_schema_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.schema_id, &decoded.plan.schema_version) ||
        all_zero(decoded.plan.schema_id, 16) ||
        decoded.plan.schema_version == 0 ||
        !read_name(&reader, &decoded.plan.relation_name,
                   &decoded.plan.relation_name_len) ||
        !take(&reader,
              PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_relation_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.relation_id, &decoded.plan.table_epoch) ||
        all_zero(decoded.plan.relation_id, 16) ||
        decoded.plan.table_epoch == 0 || !take(&reader, 2, &bytes))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    scan_count = read_be_u16(bytes);
    if (scan_count < 1 || scan_count > 2)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    for (uint32_t index = 0; index < scan_count; ++index) {
        if (!read_column(&reader, &decoded.plan.scan_columns[index]) ||
            (index && decoded.plan.scan_columns[index - 1].ordinal >=
                          decoded.plan.scan_columns[index].ordinal))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (!read_u8(&reader, &tag) || tag != 12 ||
        !read_u8(&reader, &aggregate_kind) || aggregate_kind < 1 ||
        aggregate_kind > PRDB_SDK_SQL_V7_AGGREGATE_COUNT_COLUMN || !read_column(&reader, &decoded.group_key) ||
        !read_u8(&reader, &flag) || flag > 1)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.aggregate_kind = aggregate_kind;
    decoded.has_aggregate_column = flag;
    if ((aggregate_kind == 1 && flag != 0) ||
        (aggregate_kind != 1 && flag != 1))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (flag && !read_column(&reader, &decoded.aggregate_column))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if ((aggregate_kind == 4 || aggregate_kind == 5) &&
        decoded.aggregate_column.type_code != PRDB_SDK_VALUE_TYPE_SMALLINT &&
        decoded.aggregate_column.type_code != PRDB_SDK_VALUE_TYPE_INTEGER &&
        decoded.aggregate_column.type_code != PRDB_SDK_VALUE_TYPE_BIGINT &&
        decoded.aggregate_column.type_code != PRDB_SDK_VALUE_TYPE_DECIMAL)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    uint32_t expected_scan = 0;
    for (uint32_t index = 0; index < scan_count; ++index) {
        if (columns_equal(&decoded.group_key,
                          &decoded.plan.scan_columns[index]))
            expected_scan |= 1;
        if (flag && columns_equal(&decoded.aggregate_column,
                                  &decoded.plan.scan_columns[index]))
            expected_scan |= 2;
    }
    if (!(expected_scan & 1) || (flag && !(expected_scan & 2)) ||
        ((!flag || columns_equal(&decoded.group_key,
                                 &decoded.aggregate_column)) &&
         scan_count != 1) ||
        (flag && !columns_equal(&decoded.group_key,
                                &decoded.aggregate_column) &&
         scan_count != 2))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (!read_u8(&reader, &tag) || tag < 1 || tag > 2)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.order_count = tag;
    for (uint32_t index = 0; index < decoded.order_count; ++index) {
        uint8_t source, descending, nulls_first;
        if (!read_u8(&reader, &source) || source > 1 ||
            (seen & (1u << source)) != 0 || !read_u8(&reader, &descending) ||
            descending > 1 || !read_u8(&reader, &nulls_first) ||
            nulls_first > 1)
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        seen |= (uint8_t)(1u << source);
        decoded.order_items[index].source = source;
        decoded.order_items[index].descending = descending;
        decoded.order_items[index].nulls_first = nulls_first;
    }
    if (!read_u8(&reader, &flag) || flag > 1)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.plan.has_limit = flag;
    if (flag) {
        if (!take(&reader, 4, &bytes))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        decoded.plan.limit = ((uint32_t)bytes[0] << 24) |
                             ((uint32_t)bytes[1] << 16) |
                             ((uint32_t)bytes[2] << 8) | bytes[3];
    }
    if (reader.offset != reader.length)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.plan.scan_column_count = scan_count;
    decoded.plan.projection_count = 1;
    decoded.plan.projections[0] = decoded.group_key;
    decoded.plan.plan_kind = 11;
    decoded.plan.struct_size = sizeof(decoded.plan);
    decoded.plan.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    decoded.struct_size = sizeof(decoded);
    decoded.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    *out = decoded;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_match_group_count_plan_v1(
    const prdb_sdk_select_v4 *statement,
    const prdb_sdk_select_binding_v4 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len) {
    prdb_sdk_optimizer_group_count_plan_v1 plan = {0};
    const prdb_sdk_select_v2 *base;
    uint64_t sql_len;
    prdb_sdk_sql_span_v1 relation;
    prdb_sdk_sql_span_v1 key;
    int32_t status;

    if (statement == NULL || binding == NULL || schema == NULL ||
        plan_payload == NULL || schema_len == 0 || schema_len > 63 ||
        statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V4_VERSION ||
        statement->base.struct_size != sizeof(statement->base) ||
        statement->base.abi_version != PRDB_SDK_SQL_ABI_V2_VERSION ||
        statement->base.sql == NULL ||
        statement->base.sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        statement->base.projection_count != 1 ||
        statement->base.projection_star != 0 ||
        statement->base.predicate_count != 0 ||
        statement->base.has_limit > 1 || statement->has_group_by != 1 ||
        statement->aggregate_kind != PRDB_SDK_SQL_AGGREGATE_COUNT_STAR ||
        binding->struct_size != sizeof(*binding) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V4_VERSION ||
        binding->base.struct_size != sizeof(binding->base) ||
        binding->base.abi_version != PRDB_SDK_SQL_ABI_V2_VERSION ||
        binding->base.projection_count != 1 ||
        binding->base.projection_star != 0 ||
        binding->has_group_by != 1 ||
        binding->aggregate_kind != PRDB_SDK_SQL_AGGREGATE_COUNT_STAR)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;

    status = prdb_sdk_optimizer_decode_group_count_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    base = &statement->base;
    sql_len = base->sql_len;
    relation = base->relation;
    key = statement->group_by_column;
    if (relation.offset > sql_len || relation.length > sql_len - relation.offset ||
        key.offset > sql_len || key.length > sql_len - key.offset ||
        base->projection[0].offset > sql_len ||
        base->projection[0].length > sql_len - base->projection[0].offset ||
        relation.length != plan.plan.relation_name_len ||
        memcmp(base->sql + relation.offset, plan.plan.relation_name,
               relation.length) != 0 ||
        schema_len != plan.plan.schema_name_len ||
        memcmp(schema, plan.plan.schema_name, schema_len) != 0 ||
        base->projection[0].length != key.length ||
        memcmp(base->sql + base->projection[0].offset,
               base->sql + key.offset, key.length) != 0 ||
        plan.group_key.ordinal != binding->group_by_column.ordinal ||
        plan.group_key.type_code != binding->group_by_column.type_code ||
        (plan.group_key.type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
         (plan.group_key.type_parameters[0] !=
              binding->group_by_column.decimal_precision ||
          plan.group_key.type_parameters[1] !=
              binding->group_by_column.decimal_scale)) ||
        binding->base.projection_ordinals[0] !=
            binding->group_by_column.ordinal ||
        plan.plan.has_limit != base->has_limit ||
        (plan.plan.has_limit && plan.plan.limit != base->limit))
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_match_ordered_group_count_plan_v1(
    const prdb_sdk_select_v12 *statement,
    const prdb_sdk_select_binding_v12 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len) {
    prdb_sdk_optimizer_ordered_group_count_plan_v1 plan = {0};
    const prdb_sdk_select_v7 *statement_v7;
    const prdb_sdk_select_v2 *base;
    const prdb_sdk_select_binding_v7 *binding_v7;
    int32_t status;
    if (statement == NULL || binding == NULL || schema == NULL ||
        plan_payload == NULL || schema_len == 0 || schema_len > 63 ||
        statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V12_VERSION ||
        statement->order_count == 0 ||
        statement->order_count > PRDB_SDK_SQL_V12_MAX_ORDER_KEYS ||
        binding->struct_size != sizeof(*binding) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V12_VERSION ||
        binding->order_count != statement->order_count)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    statement_v7 = &statement->base.base.base.base;
    base = &statement_v7->base;
    binding_v7 = &binding->base.base.base.base;
    if (statement_v7->struct_size != sizeof(*statement_v7) ||
        statement_v7->abi_version != PRDB_SDK_SQL_ABI_V7_VERSION ||
        statement_v7->has_group_by != 1 ||
        statement_v7->aggregate_kind != PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR ||
        statement->base.base.base.has_having != 0 ||
        base->struct_size != sizeof(*base) ||
        base->abi_version != PRDB_SDK_SQL_ABI_V2_VERSION ||
        base->sql == NULL || base->projection_count != 1 ||
        base->projection_star != 0 || base->predicate_count != 0 ||
        base->has_limit > 1 || binding_v7->has_group_by != 1 ||
        binding_v7->aggregate_kind != PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR ||
        binding_v7->base.projection_count != 1 ||
        binding_v7->base.projection_ordinals[0] !=
            binding_v7->group_by_column.ordinal)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    status = prdb_sdk_optimizer_decode_ordered_group_count_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    if (base->relation.offset > base->sql_len ||
        base->relation.length > base->sql_len - base->relation.offset ||
        statement_v7->group_by_column.offset > base->sql_len ||
        statement_v7->group_by_column.length >
            base->sql_len - statement_v7->group_by_column.offset ||
        base->projection[0].offset > base->sql_len ||
        base->projection[0].length > base->sql_len - base->projection[0].offset ||
        base->relation.length != plan.plan.relation_name_len ||
        memcmp(base->sql + base->relation.offset, plan.plan.relation_name,
               base->relation.length) != 0 ||
        schema_len != plan.plan.schema_name_len ||
        memcmp(schema, plan.plan.schema_name, schema_len) != 0 ||
        base->projection[0].length != statement_v7->group_by_column.length ||
        memcmp(base->sql + base->projection[0].offset,
               base->sql + statement_v7->group_by_column.offset,
               base->projection[0].length) != 0 ||
        plan.group_key.ordinal != binding_v7->group_by_column.ordinal ||
        plan.group_key.type_code != binding_v7->group_by_column.type_code ||
        (plan.group_key.type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
         (plan.group_key.type_parameters[0] !=
              binding_v7->group_by_column.decimal_precision ||
          plan.group_key.type_parameters[1] !=
              binding_v7->group_by_column.decimal_scale)) ||
        plan.plan.has_limit != base->has_limit ||
        (plan.plan.has_limit && plan.plan.limit != base->limit))
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    if (plan.order_count != statement->order_count)
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    for (uint32_t index = 0; index < statement->order_count; ++index) {
        const prdb_sdk_sql_order_item_v12 *item = &statement->order_items[index];
        const prdb_sdk_sql_bound_order_item_v12 *bound_item =
            &binding->order_items[index];
        uint32_t source;
        uint32_t nulls_first;
        if (item->kind == PRDB_SDK_SQL_V10_ORDER_GROUP_KEY)
            source = 0;
        else if (item->kind == PRDB_SDK_SQL_V10_ORDER_AGGREGATE)
            source = 1;
        else
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        nulls_first = item->null_order != PRDB_SDK_SQL_V11_NULLS_LAST;
        if (plan.order_items[index].source != source ||
            plan.order_items[index].descending !=
                (item->direction == PRDB_SDK_SQL_V10_ORDER_DESCENDING) ||
            plan.order_items[index].nulls_first != nulls_first ||
            bound_item->item.kind != item->kind ||
            bound_item->item.direction != item->direction ||
            bound_item->item.null_order != item->null_order)
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

static int plan_column_matches_bound(
    const prdb_sdk_optimizer_plan_column_v1 *planned,
    const prdb_sdk_sql_bound_column_v2 *bound) {
    return planned->ordinal == bound->ordinal &&
           planned->type_code == bound->type_code &&
           (planned->type_code != PRDB_SDK_VALUE_TYPE_DECIMAL ||
            (planned->type_parameters[0] == bound->decimal_precision &&
             planned->type_parameters[1] == bound->decimal_scale));
}

static uint8_t plan_ascii_fold(uint8_t byte) {
    return byte >= 'A' && byte <= 'Z' ? (uint8_t)(byte + ('a' - 'A')) : byte;
}

static int plan_spans_equal(
    const uint8_t *sql, uint64_t sql_len, prdb_sdk_sql_span_v1 left,
    prdb_sdk_sql_span_v1 right) {
    if (left.length != right.length || left.length == 0 ||
        left.offset > sql_len || left.length > sql_len - left.offset ||
        right.offset > sql_len || right.length > sql_len - right.offset)
        return 0;
    for (uint32_t index = 0; index < left.length; ++index) {
        if (plan_ascii_fold(sql[left.offset + index]) !=
            plan_ascii_fold(sql[right.offset + index]))
            return 0;
    }
    return 1;
}

int32_t prdb_sdk_optimizer_match_ordered_select_plan_v1(
    const prdb_sdk_select_v3 *statement,
    const prdb_sdk_select_binding_v3 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len) {
    prdb_sdk_optimizer_ordered_select_plan_v1 plan = {0};
    const prdb_sdk_select_v2 *base;
    const uint8_t *sql;
    uint64_t sql_len;
    int32_t status;
    if (statement == NULL || binding == NULL || schema == NULL ||
        plan_payload == NULL || schema_len == 0 || schema_len > 63 ||
        statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V3_VERSION ||
        statement->base.struct_size != sizeof(statement->base) ||
        statement->base.abi_version != PRDB_SDK_SQL_ABI_V2_VERSION ||
        statement->base.sql == NULL ||
        statement->base.sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        statement->base.projection_count == 0 ||
        statement->base.projection_count > PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS ||
        statement->base.projection_star != 0 ||
        statement->base.predicate_count != 0 ||
        statement->base.has_limit > 1 || statement->has_order_by != 1 ||
        statement->order_descending > 1 ||
        binding->struct_size != sizeof(*binding) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V3_VERSION ||
        binding->base.struct_size != sizeof(binding->base) ||
        binding->base.abi_version != PRDB_SDK_SQL_ABI_V2_VERSION ||
        binding->base.projection_count != statement->base.projection_count ||
        binding->base.projection_star != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;

    status = prdb_sdk_optimizer_decode_ordered_select_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    base = &statement->base;
    sql = base->sql;
    sql_len = base->sql_len;
    if (base->relation.offset > sql_len ||
        base->relation.length > sql_len - base->relation.offset ||
        plan.plan.relation_name_len != base->relation.length ||
        schema_len != plan.plan.schema_name_len ||
        memcmp(schema, plan.plan.schema_name, schema_len) != 0 ||
        plan.plan.projection_count != base->projection_count ||
        plan.plan.has_limit != base->has_limit ||
        (base->has_limit != 0 && plan.plan.limit != base->limit) ||
        plan.descending != statement->order_descending ||
        binding->has_order_by != 1 ||
        !plan_column_matches_bound(&plan.order_key,
                                   &binding->order_by_column)) {
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    for (uint32_t index = 0; index < base->relation.length; ++index) {
        if (plan_ascii_fold(plan.plan.relation_name[index]) !=
            plan_ascii_fold(sql[base->relation.offset + index])) {
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        }
    }
    for (uint32_t index = 0; index < base->projection_count; ++index) {
        if (binding->base.projection_ordinals[index] !=
                plan.plan.projections[index].ordinal ||
            base->projection[index].offset > sql_len ||
            base->projection[index].length >
                sql_len - base->projection[index].offset) {
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        }
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_match_composite_group_count_plan_v1(
    const prdb_sdk_select_v13 *statement,
    const prdb_sdk_select_binding_v13 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len) {
    prdb_sdk_optimizer_composite_group_count_plan_v1 plan = {0};
    const prdb_sdk_select_v2 *base;
    const uint8_t *sql;
    uint64_t sql_len;
    int32_t status;

    if (statement == NULL || binding == NULL || schema == NULL ||
        plan_payload == NULL || schema_len == 0 || schema_len > 63 ||
        statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V13_VERSION ||
        statement->base.struct_size != sizeof(statement->base) ||
        statement->base.abi_version != PRDB_SDK_SQL_ABI_V7_VERSION ||
        statement->base.base.struct_size != sizeof(statement->base.base) ||
        statement->base.base.abi_version != PRDB_SDK_SQL_ABI_V2_VERSION ||
        statement->base.base.sql == NULL ||
        statement->base.base.sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        statement->group_key_count < 2 || statement->group_key_count > 8 ||
        statement->base.has_group_by != 1 ||
        statement->base.aggregate_kind !=
            PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR ||
        statement->base.base.projection_count != 1 ||
        statement->base.base.projection_star != 0 ||
        statement->base.base.predicate_count != 0 ||
        statement->base.base.has_limit > 1 ||
        binding->struct_size != sizeof(*binding) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V13_VERSION ||
        binding->base.struct_size != sizeof(binding->base) ||
        binding->base.abi_version != PRDB_SDK_SQL_ABI_V7_VERSION ||
        binding->base.base.struct_size != sizeof(binding->base.base) ||
        binding->base.base.abi_version != PRDB_SDK_SQL_ABI_V2_VERSION ||
        binding->group_key_count != statement->group_key_count)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;

    status = prdb_sdk_optimizer_decode_composite_group_count_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    base = &statement->base.base;
    sql = base->sql;
    sql_len = base->sql_len;
    if (base->relation.offset > sql_len ||
        base->relation.length > sql_len - base->relation.offset ||
        base->has_limit != plan.plan.has_limit ||
        (base->has_limit && base->limit != plan.plan.limit) ||
        base->relation.length != plan.plan.relation_name_len ||
        memcmp(sql + base->relation.offset, plan.plan.relation_name,
               base->relation.length) != 0 ||
        schema_len != plan.plan.schema_name_len ||
        memcmp(schema, plan.plan.schema_name, schema_len) != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    for (uint32_t index = 0; index < plan.group_key_count; ++index) {
        if (!plan_spans_equal(sql, sql_len, statement->group_keys[index],
                              statement->projection_keys[index]) ||
            !plan_column_matches_bound(&plan.group_keys[index],
                                       &binding->group_keys[index]))
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        for (uint32_t prior = 0; prior < index; ++prior) {
            if (plan_spans_equal(sql, sql_len, statement->group_keys[index],
                                 statement->group_keys[prior]))
                return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        }
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_match_composite_group_aggregate_plan_v1(
    const prdb_sdk_select_v13 *statement,
    const prdb_sdk_select_binding_v13 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len) {
    prdb_sdk_optimizer_composite_group_aggregate_plan_v1 plan = {0};
    const prdb_sdk_select_v2 *base;
    const uint8_t *sql;
    uint64_t sql_len;
    int32_t status;
    if (statement == NULL || binding == NULL || schema == NULL ||
        plan_payload == NULL || schema_len == 0 || schema_len > 63 ||
        statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V13_VERSION ||
        statement->base.struct_size != sizeof(statement->base) ||
        statement->base.abi_version != PRDB_SDK_SQL_ABI_V7_VERSION ||
        statement->base.base.struct_size != sizeof(statement->base.base) ||
        statement->base.base.abi_version != PRDB_SDK_SQL_ABI_V2_VERSION ||
        statement->group_key_count < PRDB_SDK_SQL_V13_MIN_GROUP_KEYS ||
        statement->group_key_count > PRDB_SDK_SQL_V13_MAX_GROUP_KEYS ||
        statement->base.has_group_by != 1 ||
        statement->base.aggregate_kind < PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR ||
        statement->base.aggregate_kind > PRDB_SDK_SQL_V7_AGGREGATE_COUNT_COLUMN ||
        statement->base.base.projection_count != 1 ||
        statement->base.base.projection_star != 0 ||
        statement->base.base.predicate_count != 0 ||
        statement->base.base.has_limit > 1 ||
        binding->struct_size != sizeof(*binding) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V13_VERSION ||
        binding->base.struct_size != sizeof(binding->base) ||
        binding->base.abi_version != PRDB_SDK_SQL_ABI_V7_VERSION ||
        binding->base.base.struct_size != sizeof(binding->base.base) ||
        binding->base.base.abi_version != PRDB_SDK_SQL_ABI_V2_VERSION ||
        binding->group_key_count != statement->group_key_count ||
        binding->base.aggregate_kind != statement->base.aggregate_kind)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    status = prdb_sdk_optimizer_decode_composite_group_aggregate_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    base = &statement->base.base;
    sql = base->sql;
    sql_len = base->sql_len;
    if (base->relation.offset > sql_len ||
        base->relation.length > sql_len - base->relation.offset ||
        base->has_limit != plan.plan.has_limit ||
        (base->has_limit && base->limit != plan.plan.limit) ||
        base->relation.length != plan.plan.relation_name_len ||
        memcmp(sql + base->relation.offset, plan.plan.relation_name,
               base->relation.length) != 0 ||
        schema_len != plan.plan.schema_name_len ||
        memcmp(schema, plan.plan.schema_name, schema_len) != 0 ||
        plan.aggregate_kind != statement->base.aggregate_kind ||
        plan.group_key_count != binding->group_key_count ||
        plan.has_aggregate_column !=
            (statement->base.aggregate_kind ==
                     PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR
                 ? 0
                 : 1))
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    for (uint32_t index = 0; index < plan.group_key_count; ++index) {
        if (!plan_spans_equal(sql, sql_len, statement->group_keys[index],
                              statement->projection_keys[index]) ||
            !plan_column_matches_bound(&plan.group_keys[index],
                                       &binding->group_keys[index]))
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        for (uint32_t prior = 0; prior < index; ++prior) {
            if (plan_spans_equal(sql, sql_len, statement->group_keys[index],
                                 statement->group_keys[prior]))
                return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        }
    }
    if (plan.has_aggregate_column &&
        !plan_column_matches_bound(&plan.aggregate_column,
                                   &binding->base.aggregate_column))
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

static int append_bytes(uint8_t *output, uint32_t capacity, uint32_t *offset,
                        const uint8_t *bytes, uint32_t length) {
    if (*offset > capacity || length > capacity - *offset) return 0;
    if (length != 0) memcpy(output + *offset, bytes, length);
    *offset += length;
    return 1;
}

static int encode_bound_sql_predicate(
    const prdb_sdk_select_v2 *statement,
    const prdb_sdk_select_binding_v13 *binding,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const prdb_sdk_optimizer_plan_column_v1 *scan_columns,
    uint32_t scan_count, uint32_t node_index, uint32_t depth,
    uint8_t *seen, uint8_t *output, uint32_t capacity, uint32_t *offset) {
    const prdb_sdk_predicate_node_v2 *node;
    uint8_t record[PRDB_OPTIMIZER_PLAN_COLUMN_PREDICATE_RECORD_HEADER_BYTES];
    if (depth > PRDB_SDK_SQL_MAX_EXPRESSION_DEPTH ||
        node_index >= statement->predicate_count || seen[node_index]) return 0;
    seen[node_index] = 1;
    node = &statement->predicates[node_index];
    switch (node->node_kind) {
    case PRDB_SDK_SQL_PREDICATE_COMPARE: {
        uint32_t ordinal;
        const prdb_sdk_optimizer_plan_column_v1 *column = NULL;
        const prdb_sdk_value_v1 *value;
        if (node->comparison < PRDB_SDK_SQL_COMPARE_EQUAL ||
            node->comparison > PRDB_SDK_SQL_COMPARE_GREATER_EQUAL ||
            node->parameter_index == 0 || node->parameter_index > parameter_count ||
            parameters == NULL || node_index >= binding->base.base.predicate_count)
            return 0;
        ordinal = binding->base.base.predicate_columns[node_index].ordinal;
        for (uint32_t index = 0; index < scan_count; ++index)
            if (scan_columns[index].ordinal == ordinal) column = &scan_columns[index];
        if (column == NULL) return 0;
        record[0] = 0x20;
        record[1] = (uint8_t)(node->comparison - PRDB_SDK_SQL_COMPARE_EQUAL);
        if (!append_bytes(output, capacity, offset, record, 2)) return 0;
        prdb_encode_optimizer_plan_column_predicate_record(
            record, 0x10, column->column_id);
        if (!append_bytes(output, capacity, offset, record,
                          PRDB_OPTIMIZER_PLAN_COLUMN_PREDICATE_RECORD_HEADER_BYTES))
            return 0;
        value = &parameters[node->parameter_index - 1];
        if (prdb_sdk_value_validate_v1(value) != PRDB_SDK_VALUE_VALID) return 0;
        if (value->state == PRDB_SDK_VALUE_STATE_NULL) {
            record[0] = 0;
            return append_bytes(output, capacity, offset, record, 1);
        }
        if (value->state != PRDB_SDK_VALUE_STATE_VALUE) return 0;
        if (value->type_code == PRDB_SDK_VALUE_TYPE_BOOLEAN) {
            prdb_encode_optimizer_plan_boolean_literal_predicate_record(
                record, 0x01, value->data[0]);
            return append_bytes(output, capacity, offset, record,
                                PRDB_OPTIMIZER_PLAN_BOOLEAN_LITERAL_PREDICATE_RECORD_HEADER_BYTES);
        }
        if (value->type_code == PRDB_SDK_VALUE_TYPE_INTEGER) {
            uint32_t raw = ((uint32_t)value->data[0] << 24) |
                           ((uint32_t)value->data[1] << 16) |
                           ((uint32_t)value->data[2] << 8) | value->data[3];
            int64_t integer = (int32_t)raw;
            prdb_encode_optimizer_plan_integer_predicate_record(record, 0x02,
                                                                 integer);
            return append_bytes(output, capacity, offset, record,
                                PRDB_OPTIMIZER_PLAN_INTEGER_PREDICATE_RECORD_HEADER_BYTES);
        }
        if (value->type_code == PRDB_SDK_VALUE_TYPE_TEXT &&
            value->data_len <= 4096 && value->data_len <= UINT16_MAX) {
            prdb_encode_optimizer_plan_text_predicate_header_record(
                record, 0x03, (uint16_t)value->data_len);
            return append_bytes(output, capacity, offset, record,
                                PRDB_OPTIMIZER_PLAN_TEXT_PREDICATE_HEADER_RECORD_HEADER_BYTES) &&
                   append_bytes(output, capacity, offset, value->data,
                                (uint32_t)value->data_len);
        }
        return 0;
    }
    case PRDB_SDK_SQL_PREDICATE_AND:
    case PRDB_SDK_SQL_PREDICATE_OR: {
        record[0] = node->node_kind == PRDB_SDK_SQL_PREDICATE_AND ? 0x30 : 0x31;
        record[1] = 2;
        return append_bytes(output, capacity, offset, record, 2) &&
               encode_bound_sql_predicate(statement, binding, parameters,
                   parameter_count, scan_columns, scan_count, node->left_child,
                   depth + 1, seen, output, capacity, offset) &&
               encode_bound_sql_predicate(statement, binding, parameters,
                   parameter_count, scan_columns, scan_count, node->right_child,
                   depth + 1, seen, output, capacity, offset);
    }
    case PRDB_SDK_SQL_PREDICATE_NOT:
        record[0] = 0x32;
        return append_bytes(output, capacity, offset, record, 1) &&
               encode_bound_sql_predicate(statement, binding, parameters,
                   parameter_count, scan_columns, scan_count, node->left_child,
                   depth + 1, seen, output, capacity, offset);
    case PRDB_SDK_SQL_PREDICATE_IS_NULL:
    case PRDB_SDK_SQL_PREDICATE_IS_NOT_NULL: {
        const prdb_sdk_optimizer_plan_column_v1 *column = NULL;
        uint32_t ordinal;
        if (node->left_child != 0 || node->right_child != 0 ||
            node->parameter_index != 0 ||
            node->comparison != 0 ||
            node_index >= binding->base.base.predicate_count)
            return 0;
        ordinal = binding->base.base.predicate_columns[node_index].ordinal;
        for (uint32_t index = 0; index < scan_count; ++index)
            if (scan_columns[index].ordinal == ordinal)
                column = &scan_columns[index];
        if (column == NULL) return 0;
        record[0] = node->node_kind == PRDB_SDK_SQL_PREDICATE_IS_NULL
                        ? 0x40
                        : 0x41;
        if (!append_bytes(output, capacity, offset, record, 1)) return 0;
        prdb_encode_optimizer_plan_column_predicate_record(
            record, 0x10, column->column_id);
        return append_bytes(output, capacity, offset, record,
                            PRDB_OPTIMIZER_PLAN_COLUMN_PREDICATE_RECORD_HEADER_BYTES);
    }
    default:
        return 0;
    }
}

int32_t prdb_sdk_optimizer_match_filtered_composite_group_aggregate_plan_v1(
    const prdb_sdk_select_v13 *statement,
    const prdb_sdk_select_binding_v13 *binding,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len) {
    prdb_sdk_optimizer_filtered_composite_group_aggregate_plan_v1 plan = {0};
    const prdb_sdk_select_v2 *base;
    uint8_t expected_filter[16384];
    uint8_t seen[PRDB_SDK_SQL_MAX_PREDICATE_NODES] = {0};
    uint32_t expected_filter_len = 0;
    int32_t status;
    if (statement == NULL || binding == NULL || schema == NULL || plan_payload == NULL ||
        schema_len == 0 || schema_len > 63 || parameter_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        (parameter_count != 0 && parameters == NULL) ||
        statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V13_VERSION ||
        statement->base.struct_size != sizeof(statement->base) ||
        statement->base.abi_version != PRDB_SDK_SQL_ABI_V7_VERSION ||
        statement->base.base.struct_size != sizeof(statement->base.base) ||
        statement->base.base.abi_version != PRDB_SDK_SQL_ABI_V2_VERSION ||
        statement->group_key_count < 1 ||
        statement->group_key_count > PRDB_SDK_SQL_V13_MAX_GROUP_KEYS ||
        statement->base.has_group_by != 1 ||
        statement->base.aggregate_kind < PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR ||
        statement->base.aggregate_kind > PRDB_SDK_SQL_V7_AGGREGATE_COUNT_COLUMN ||
        statement->base.base.projection_count != 1 ||
        statement->base.base.projection_star != 0 ||
        statement->base.base.predicate_count == 0 ||
        statement->base.base.predicate_count > PRDB_SDK_SQL_MAX_PREDICATE_NODES ||
        statement->base.base.predicate_root >= statement->base.base.predicate_count ||
        statement->base.base.has_limit > 1 ||
        binding->struct_size != sizeof(*binding) || binding->abi_version != PRDB_SDK_SQL_ABI_V13_VERSION ||
        binding->base.struct_size != sizeof(binding->base) ||
        binding->base.abi_version != PRDB_SDK_SQL_ABI_V7_VERSION ||
        binding->base.base.struct_size != sizeof(binding->base.base) ||
        binding->base.base.abi_version != PRDB_SDK_SQL_ABI_V2_VERSION ||
        binding->group_key_count != statement->group_key_count ||
        binding->base.aggregate_kind != statement->base.aggregate_kind ||
        binding->base.base.predicate_count != statement->base.base.predicate_count)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    status = prdb_sdk_optimizer_decode_filtered_composite_group_aggregate_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    base = &statement->base.base;
    if (base->sql == NULL || base->relation.offset > base->sql_len ||
        base->relation.length > base->sql_len - base->relation.offset ||
        base->relation.length != plan.plan.relation_name_len ||
        memcmp(base->sql + base->relation.offset, plan.plan.relation_name,
               base->relation.length) != 0 || schema_len != plan.plan.schema_name_len ||
        memcmp(schema, plan.plan.schema_name, schema_len) != 0 ||
        base->has_limit != plan.plan.has_limit ||
        (base->has_limit && base->limit != plan.plan.limit) ||
        plan.aggregate_kind != statement->base.aggregate_kind ||
        plan.group_key_count != statement->group_key_count ||
        plan.plan.scan_column_count > PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS)
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    for (uint32_t index = 0; index < statement->group_key_count; ++index) {
        if (!plan_spans_equal(base->sql, base->sql_len,
                              statement->group_keys[index],
                              statement->projection_keys[index]) ||
            !plan_column_matches_bound(&plan.group_keys[index],
                                       &binding->group_keys[index]))
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        for (uint32_t prior = 0; prior < index; ++prior)
            if (plan_spans_equal(base->sql, base->sql_len,
                                 statement->group_keys[index],
                                 statement->group_keys[prior]))
                return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    if (plan.has_aggregate_column &&
        !plan_column_matches_bound(&plan.aggregate_column,
                                   &binding->base.aggregate_column))
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    if (!encode_bound_sql_predicate(base, binding, parameters, parameter_count,
                                    plan.plan.scan_columns,
                                    plan.plan.scan_column_count,
                                    base->predicate_root, 1, seen,
                                    expected_filter, sizeof(expected_filter),
                                    &expected_filter_len))
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    for (uint32_t index = 0; index < base->predicate_count; ++index)
        if (!seen[index]) return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    if (expected_filter_len != plan.filter.prefix_len ||
        memcmp(expected_filter, plan.filter.prefix, expected_filter_len) != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_match_ordered_filtered_composite_group_aggregate_plan_v1(
    const prdb_sdk_select_v15 *statement,
    const prdb_sdk_select_binding_v15 *binding,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len) {
    prdb_sdk_optimizer_ordered_filtered_composite_group_aggregate_plan_v1 plan = {0};
    const prdb_sdk_select_v2 *base;
    uint8_t expected_filter[16384];
    uint8_t seen[PRDB_SDK_SQL_MAX_PREDICATE_NODES] = {0};
    uint32_t expected_filter_len = 0;
    int32_t status;
    if (statement == NULL || binding == NULL || schema == NULL || plan_payload == NULL ||
        schema_len == 0 || schema_len > 63 || parameter_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        (parameter_count != 0 && parameters == NULL) ||
        statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V15_VERSION ||
        statement->base.struct_size != sizeof(statement->base) ||
        statement->base.abi_version != PRDB_SDK_SQL_ABI_V14_VERSION ||
        statement->base.base.struct_size != sizeof(statement->base.base) ||
        statement->base.base.abi_version != PRDB_SDK_SQL_ABI_V13_VERSION ||
        statement->base.has_having != 0 || statement->order_count == 0 ||
        statement->order_count > PRDB_SDK_SQL_V15_MAX_ORDER_KEYS ||
        statement->base.base.group_key_count < 1 || statement->base.base.group_key_count > 8 ||
        statement->base.base.base.aggregate_kind < PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR ||
        statement->base.base.base.aggregate_kind > PRDB_SDK_SQL_V7_AGGREGATE_COUNT_COLUMN ||
        statement->base.base.base.base.projection_count != 1 ||
        statement->base.base.base.base.projection_star != 0 ||
        statement->base.base.base.base.predicate_count == 0 ||
        statement->base.base.base.base.predicate_count > PRDB_SDK_SQL_MAX_PREDICATE_NODES ||
        statement->base.base.base.base.predicate_root >= statement->base.base.base.base.predicate_count ||
        statement->base.base.base.base.has_limit > 1 ||
        binding->struct_size != sizeof(*binding) || binding->abi_version != PRDB_SDK_SQL_ABI_V15_VERSION ||
        binding->order_count != statement->order_count || binding->base.has_having != 0 ||
        binding->base.base.group_key_count != statement->base.base.group_key_count ||
        binding->base.base.base.aggregate_kind != statement->base.base.base.aggregate_kind ||
        binding->base.base.base.base.predicate_count != statement->base.base.base.base.predicate_count)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    status = prdb_sdk_optimizer_decode_ordered_filtered_composite_group_aggregate_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    base = &statement->base.base.base.base;
    if (base->sql == NULL || base->relation.offset > base->sql_len ||
        base->relation.length > base->sql_len - base->relation.offset ||
        base->relation.length != plan.plan.relation_name_len ||
        memcmp(base->sql + base->relation.offset, plan.plan.relation_name, base->relation.length) != 0 ||
        schema_len != plan.plan.schema_name_len || memcmp(schema, plan.plan.schema_name, schema_len) != 0 ||
        base->has_limit != plan.plan.has_limit || (base->has_limit && base->limit != plan.plan.limit) ||
        plan.aggregate_kind != statement->base.base.base.aggregate_kind ||
        plan.group_key_count != statement->base.base.group_key_count ||
        plan.order_count != statement->order_count) {
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    for (uint32_t index = 0; index < statement->base.base.group_key_count; ++index) {
        if (!plan_spans_equal(base->sql, base->sql_len,
                              statement->base.base.group_keys[index],
                              statement->base.base.projection_keys[index]) ||
            !plan_column_matches_bound(&plan.group_keys[index],
                                       &binding->base.base.group_keys[index]))
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        for (uint32_t prior = 0; prior < index; ++prior)
            if (plan_spans_equal(base->sql, base->sql_len,
                                 statement->base.base.group_keys[index],
                                 statement->base.base.group_keys[prior]))
                return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    if (plan.has_aggregate_column &&
        !plan_column_matches_bound(&plan.aggregate_column,
                                   &binding->base.base.base.aggregate_column))
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    for (uint32_t index = 0; index < plan.order_count; ++index) {
        const prdb_sdk_sql_order_item_v15 *item = &statement->order_items[index];
        const prdb_sdk_optimizer_composite_order_item_v1 *planned = &plan.order_items[index];
        uint32_t source = item->kind == PRDB_SDK_SQL_V10_ORDER_GROUP_KEY ? 0 : 1;
        uint32_t nulls_first = item->null_order == PRDB_SDK_SQL_V11_NULL_ORDER_DEFAULT ||
                               item->null_order == PRDB_SDK_SQL_V11_NULLS_FIRST;
        if (planned->source != source ||
            planned->descending != (item->direction == PRDB_SDK_SQL_V10_ORDER_DESCENDING) ||
            planned->nulls_first != nulls_first ||
            (source == 0 && planned->group_key_index != item->group_key_index))
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    if (!encode_bound_sql_predicate(base, &binding->base.base, parameters,
                                    parameter_count, plan.plan.scan_columns,
                                    plan.plan.scan_column_count, base->predicate_root, 1,
                                    seen, expected_filter, sizeof(expected_filter),
                                    &expected_filter_len))
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    for (uint32_t index = 0; index < base->predicate_count; ++index)
        if (!seen[index]) return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    if (expected_filter_len != plan.filter.prefix_len ||
        memcmp(expected_filter, plan.filter.prefix, expected_filter_len) != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_match_ordered_composite_group_count_plan_v1(
    const prdb_sdk_select_v15 *statement,
    const prdb_sdk_select_binding_v15 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len) {
    prdb_sdk_optimizer_ordered_composite_group_count_plan_v1 plan = {0};
    const prdb_sdk_select_v2 *base;
    const uint8_t *sql;
    uint64_t sql_len;
    int32_t status;
    if (statement == NULL || binding == NULL || schema == NULL || plan_payload == NULL || schema_len == 0 || schema_len > 63 ||
        statement->struct_size != sizeof(*statement) || statement->abi_version != PRDB_SDK_SQL_ABI_V15_VERSION ||
        statement->base.struct_size != sizeof(statement->base) || statement->base.abi_version != PRDB_SDK_SQL_ABI_V14_VERSION ||
        statement->base.base.struct_size != sizeof(statement->base.base) || statement->base.base.abi_version != PRDB_SDK_SQL_ABI_V13_VERSION ||
        statement->base.base.group_key_count < 2 || statement->base.base.group_key_count > 8 || statement->base.base.base.aggregate_kind != PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR ||
        statement->base.base.base.base.projection_count != 1 || statement->base.base.base.base.projection_star != 0 ||
        statement->base.base.base.base.predicate_count != 0 || statement->base.has_having != 0 || statement->order_count == 0 || statement->order_count > 9 ||
        binding->struct_size != sizeof(*binding) || binding->abi_version != PRDB_SDK_SQL_ABI_V15_VERSION || binding->order_count != statement->order_count ||
        binding->base.has_having != 0 || binding->base.base.group_key_count != statement->base.base.group_key_count)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    status = prdb_sdk_optimizer_decode_ordered_composite_group_count_plan_v1(plan_payload, plan_payload_len, &plan, sizeof(plan));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    base = &statement->base.base.base.base;
    sql = base->sql; sql_len = base->sql_len;
    if (base->relation.offset > sql_len || base->relation.length > sql_len - base->relation.offset || base->has_limit != plan.plan.has_limit ||
        (base->has_limit && base->limit != plan.plan.limit) || base->relation.length != plan.plan.relation_name_len ||
        memcmp(sql + base->relation.offset, plan.plan.relation_name, base->relation.length) != 0 || schema_len != plan.plan.schema_name_len || memcmp(schema, plan.plan.schema_name, schema_len) != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    for (uint32_t i = 0; i < plan.group_key_count; ++i) {
        if (!plan_spans_equal(sql, sql_len, statement->base.base.group_keys[i], statement->base.base.projection_keys[i]) ||
            !plan_column_matches_bound(&plan.group_keys[i], &binding->base.base.group_keys[i])) return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        for (uint32_t j = 0; j < i; ++j) if (plan_spans_equal(sql, sql_len, statement->base.base.group_keys[i], statement->base.base.group_keys[j])) return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    for (uint32_t i = 0; i < plan.order_count; ++i) {
        const prdb_sdk_sql_order_item_v15 *item = &statement->order_items[i];
        const prdb_sdk_optimizer_composite_order_item_v1 *planned = &plan.order_items[i];
        uint32_t source = item->kind == PRDB_SDK_SQL_V10_ORDER_GROUP_KEY ? 0 : 1;
        uint32_t nulls_first = item->null_order == PRDB_SDK_SQL_V11_NULL_ORDER_DEFAULT || item->null_order == PRDB_SDK_SQL_V11_NULLS_FIRST;
        if (planned->source != source || planned->descending != (item->direction == PRDB_SDK_SQL_V10_ORDER_DESCENDING) || planned->nulls_first != nulls_first ||
            (source == 0 && planned->group_key_index != item->group_key_index)) return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_match_ordered_composite_group_aggregate_plan_v1(
    const prdb_sdk_select_v15 *statement,
    const prdb_sdk_select_binding_v15 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len) {
    prdb_sdk_optimizer_ordered_composite_group_aggregate_plan_v1 plan = {0};
    const prdb_sdk_select_v2 *base;
    const uint8_t *sql;
    uint64_t sql_len;
    int32_t status;
    if (statement == NULL || binding == NULL || schema == NULL ||
        plan_payload == NULL || schema_len == 0 || schema_len > 63 ||
        statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V15_VERSION ||
        statement->base.struct_size != sizeof(statement->base) ||
        statement->base.abi_version != PRDB_SDK_SQL_ABI_V14_VERSION ||
        statement->base.base.struct_size != sizeof(statement->base.base) ||
        statement->base.base.abi_version != PRDB_SDK_SQL_ABI_V13_VERSION ||
        statement->base.base.group_key_count < 2 ||
        statement->base.base.group_key_count > 8 ||
        statement->base.base.base.aggregate_kind <
            PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR ||
        statement->base.base.base.aggregate_kind >
            PRDB_SDK_SQL_V7_AGGREGATE_COUNT_COLUMN ||
        statement->base.base.base.base.projection_count != 1 ||
        statement->base.base.base.base.projection_star != 0 ||
        statement->base.base.base.base.predicate_count != 0 ||
        statement->base.has_having != 0 || statement->order_count == 0 ||
        statement->order_count > PRDB_SDK_SQL_V15_MAX_ORDER_KEYS ||
        binding->struct_size != sizeof(*binding) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V15_VERSION ||
        binding->order_count != statement->order_count ||
        binding->base.has_having != 0 ||
        binding->base.base.group_key_count !=
            statement->base.base.group_key_count ||
        binding->base.base.base.aggregate_kind !=
            statement->base.base.base.aggregate_kind)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    status = prdb_sdk_optimizer_decode_ordered_composite_group_aggregate_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    base = &statement->base.base.base.base;
    sql = base->sql;
    sql_len = base->sql_len;
    if (base->relation.offset > sql_len ||
        base->relation.length > sql_len - base->relation.offset ||
        base->has_limit != plan.plan.has_limit ||
        (base->has_limit && base->limit != plan.plan.limit) ||
        base->relation.length != plan.plan.relation_name_len ||
        memcmp(sql + base->relation.offset, plan.plan.relation_name,
               base->relation.length) != 0 ||
        schema_len != plan.plan.schema_name_len ||
        memcmp(schema, plan.plan.schema_name, schema_len) != 0 ||
        plan.aggregate_kind != statement->base.base.base.aggregate_kind ||
        plan.group_key_count != binding->base.base.group_key_count ||
        plan.has_aggregate_column !=
            (statement->base.base.base.aggregate_kind ==
                     PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR
                 ? 0
                 : 1))
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    for (uint32_t index = 0; index < plan.group_key_count; ++index) {
        if (!plan_spans_equal(sql, sql_len,
                              statement->base.base.group_keys[index],
                              statement->base.base.projection_keys[index]) ||
            !plan_column_matches_bound(
                &plan.group_keys[index],
                &binding->base.base.group_keys[index]))
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        for (uint32_t prior = 0; prior < index; ++prior)
            if (plan_spans_equal(sql, sql_len,
                                 statement->base.base.group_keys[index],
                                 statement->base.base.group_keys[prior]))
                return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    if (plan.has_aggregate_column &&
        !plan_column_matches_bound(
            &plan.aggregate_column,
            &binding->base.base.base.aggregate_column))
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    if (plan.order_count != statement->order_count)
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    for (uint32_t index = 0; index < plan.order_count; ++index) {
        const prdb_sdk_sql_order_item_v15 *item = &statement->order_items[index];
        const prdb_sdk_optimizer_composite_order_item_v1 *planned =
            &plan.order_items[index];
        uint32_t source =
            item->kind == PRDB_SDK_SQL_V10_ORDER_GROUP_KEY ? 0 : 1;
        uint32_t nulls_first =
            item->null_order == PRDB_SDK_SQL_V11_NULL_ORDER_DEFAULT ||
            item->null_order == PRDB_SDK_SQL_V11_NULLS_FIRST;
        if (planned->source != source ||
            planned->descending !=
                (item->direction == PRDB_SDK_SQL_V10_ORDER_DESCENDING) ||
            planned->nulls_first != nulls_first ||
            (source == 0 &&
             planned->group_key_index != item->group_key_index))
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_match_group_aggregate_plan_v1(
    const prdb_sdk_select_v7 *statement,
    const prdb_sdk_select_binding_v7 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len) {
    prdb_sdk_optimizer_group_aggregate_plan_v1 plan = {0};
    const prdb_sdk_select_v2 *base;
    const uint8_t *sql;
    uint64_t sql_len;
    int32_t status;
    if (statement == NULL || binding == NULL || schema == NULL || plan_payload == NULL || schema_len == 0 || schema_len > 63 ||
        statement->struct_size != sizeof(*statement) || statement->abi_version != PRDB_SDK_SQL_ABI_V7_VERSION ||
        statement->has_group_by != 1 || statement->aggregate_kind < PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR || statement->aggregate_kind > PRDB_SDK_SQL_V7_AGGREGATE_COUNT_COLUMN ||
        statement->base.projection_count != 1 || statement->base.projection_star != 0 || statement->base.predicate_count != 0 || statement->base.has_limit > 1 ||
        binding->struct_size != sizeof(*binding) || binding->abi_version != PRDB_SDK_SQL_ABI_V7_VERSION || binding->has_group_by != 1 || binding->aggregate_kind != statement->aggregate_kind)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    status = prdb_sdk_optimizer_decode_group_aggregate_plan_v1(plan_payload, plan_payload_len, &plan, sizeof(plan));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    base = &statement->base; sql = base->sql; sql_len = base->sql_len;
    if (base->relation.offset > sql_len || base->relation.length > sql_len - base->relation.offset ||
        base->has_limit != plan.plan.has_limit || (base->has_limit && base->limit != plan.plan.limit) ||
        base->relation.length != plan.plan.relation_name_len || memcmp(sql + base->relation.offset, plan.plan.relation_name, base->relation.length) != 0 ||
        schema_len != plan.plan.schema_name_len || memcmp(schema, plan.plan.schema_name, schema_len) != 0 ||
        !plan_spans_equal(sql, sql_len, statement->group_by_column, base->projection[0]) ||
        plan.aggregate_kind != statement->aggregate_kind || !plan_column_matches_bound(&plan.group_key, &binding->group_by_column))
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    if (statement->aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
        if (plan.has_aggregate_column != 0 || statement->aggregate_column.length != 0) return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    } else if (plan.has_aggregate_column == 0 || statement->aggregate_column.length == 0 ||
               statement->aggregate_column.offset > sql_len || statement->aggregate_column.length > sql_len - statement->aggregate_column.offset ||
               !plan_column_matches_bound(&plan.aggregate_column, &binding->aggregate_column)) {
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_match_ordered_group_aggregate_plan_v1(
    const prdb_sdk_select_v12 *statement,
    const prdb_sdk_select_binding_v12 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len) {
    prdb_sdk_optimizer_ordered_group_aggregate_plan_v1 plan = {0};
    const prdb_sdk_select_v7 *statement_v7;
    const prdb_sdk_select_binding_v7 *binding_v7;
    const prdb_sdk_select_v2 *base;
    const uint8_t *sql;
    uint64_t sql_len;
    int32_t status;
    if (statement == NULL || binding == NULL || schema == NULL ||
        plan_payload == NULL || schema_len == 0 || schema_len > 63 ||
        statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V12_VERSION ||
        statement->order_count == 0 ||
        statement->order_count > PRDB_SDK_SQL_V12_MAX_ORDER_KEYS ||
        statement->base.struct_size != sizeof(statement->base) ||
        statement->base.abi_version != PRDB_SDK_SQL_ABI_V11_VERSION ||
        statement->base.base.struct_size != sizeof(statement->base.base) ||
        statement->base.base.abi_version != PRDB_SDK_SQL_ABI_V10_VERSION ||
        statement->base.base.base.struct_size !=
            sizeof(statement->base.base.base) ||
        statement->base.base.base.abi_version != PRDB_SDK_SQL_ABI_V9_VERSION ||
        binding->struct_size != sizeof(*binding) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V12_VERSION ||
        binding->order_count != statement->order_count ||
        binding->base.struct_size != sizeof(binding->base) ||
        binding->base.abi_version != PRDB_SDK_SQL_ABI_V11_VERSION ||
        binding->base.base.struct_size != sizeof(binding->base.base) ||
        binding->base.base.abi_version != PRDB_SDK_SQL_ABI_V10_VERSION ||
        binding->base.base.base.struct_size !=
            sizeof(binding->base.base.base) ||
        binding->base.base.base.abi_version != PRDB_SDK_SQL_ABI_V9_VERSION)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    statement_v7 = &statement->base.base.base.base;
    binding_v7 = &binding->base.base.base.base;
    base = &statement_v7->base;
    if (statement_v7->struct_size != sizeof(*statement_v7) ||
        statement_v7->abi_version != PRDB_SDK_SQL_ABI_V7_VERSION ||
        statement_v7->has_group_by != 1 ||
        statement_v7->aggregate_kind < PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR ||
        statement_v7->aggregate_kind > PRDB_SDK_SQL_V7_AGGREGATE_COUNT_COLUMN ||
        statement->base.base.base.has_having != 0 ||
        statement->base.base.has_order_by != 1 ||
        base->struct_size != sizeof(*base) ||
        base->abi_version != PRDB_SDK_SQL_ABI_V2_VERSION || base->sql == NULL ||
        base->projection_count != 1 || base->projection_star != 0 ||
        base->predicate_count != 0 || base->has_limit > 1 ||
        binding_v7->struct_size != sizeof(*binding_v7) ||
        binding_v7->abi_version != PRDB_SDK_SQL_ABI_V7_VERSION ||
        binding_v7->has_group_by != 1 ||
        binding_v7->aggregate_kind != statement_v7->aggregate_kind ||
        binding_v7->base.projection_count != 1 ||
        binding_v7->base.projection_ordinals[0] !=
            binding_v7->group_by_column.ordinal)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    status = prdb_sdk_optimizer_decode_ordered_group_aggregate_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    sql = base->sql;
    sql_len = base->sql_len;
    if (base->relation.offset > sql_len ||
        base->relation.length > sql_len - base->relation.offset ||
        base->projection[0].offset > sql_len ||
        base->projection[0].length > sql_len - base->projection[0].offset ||
        statement_v7->group_by_column.offset > sql_len ||
        statement_v7->group_by_column.length >
            sql_len - statement_v7->group_by_column.offset ||
        base->relation.length != plan.plan.relation_name_len ||
        memcmp(sql + base->relation.offset, plan.plan.relation_name,
               base->relation.length) != 0 ||
        schema_len != plan.plan.schema_name_len ||
        memcmp(schema, plan.plan.schema_name, schema_len) != 0 ||
        !plan_spans_equal(sql, sql_len, base->projection[0],
                          statement_v7->group_by_column) ||
        plan.aggregate_kind != statement_v7->aggregate_kind ||
        !plan_column_matches_bound(&plan.group_key,
                                   &binding_v7->group_by_column) ||
        plan.plan.has_limit != base->has_limit ||
        (plan.plan.has_limit && plan.plan.limit != base->limit))
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    if (statement_v7->aggregate_kind ==
        PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
        if (plan.has_aggregate_column != 0 ||
            statement_v7->aggregate_column.length != 0)
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    } else if (plan.has_aggregate_column == 0 ||
               statement_v7->aggregate_column.length == 0 ||
               statement_v7->aggregate_column.offset > sql_len ||
               statement_v7->aggregate_column.length >
                   sql_len - statement_v7->aggregate_column.offset ||
               !plan_column_matches_bound(&plan.aggregate_column,
                                          &binding_v7->aggregate_column)) {
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    if (plan.order_count != statement->order_count)
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    for (uint32_t index = 0; index < statement->order_count; ++index) {
        const prdb_sdk_sql_order_item_v12 *item =
            &statement->order_items[index];
        const prdb_sdk_sql_bound_order_item_v12 *bound_item =
            &binding->order_items[index];
        uint32_t source;
        uint32_t nulls_first;
        if (item->kind == PRDB_SDK_SQL_V10_ORDER_GROUP_KEY)
            source = 0;
        else if (item->kind == PRDB_SDK_SQL_V10_ORDER_AGGREGATE)
            source = 1;
        else
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        nulls_first = item->null_order != PRDB_SDK_SQL_V11_NULLS_LAST;
        if (plan.order_items[index].source != source ||
            plan.order_items[index].descending !=
                (item->direction == PRDB_SDK_SQL_V10_ORDER_DESCENDING) ||
            plan.order_items[index].nulls_first != nulls_first ||
            bound_item->item.kind != item->kind ||
            bound_item->item.direction != item->direction ||
            bound_item->item.null_order != item->null_order)
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

static int32_t match_join_plan_decoded_v19_v1(
    const prdb_sdk_select_v19 *statement,
    const prdb_sdk_select_binding_v19 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const prdb_sdk_optimizer_join_plan_v1 *plan) {
    const uint8_t *sql;
    uint64_t sql_len;
    prdb_sdk_sql_span_v1 left_name;
    prdb_sdk_sql_span_v1 right_name;
    if (statement == NULL || binding == NULL || schema == NULL ||
        plan == NULL || schema_len == 0 || schema_len > 63 ||
        statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V19_VERSION ||
        statement->base.struct_size != sizeof(statement->base) ||
        statement->base.abi_version != PRDB_SDK_SQL_ABI_V16_VERSION ||
        statement->base.base.struct_size != sizeof(statement->base.base) ||
        statement->base.base.sql == NULL ||
        statement->base.base.sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        statement->base.base.projection_count == 0 ||
        statement->base.base.projection_count > PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS ||
        statement->base.join_key_count < 2 ||
        statement->base.join_key_count > PRDB_SDK_SQL_V16_MAX_JOIN_KEYS ||
        statement->base.base.has_limit > 1 ||
        binding->struct_size != sizeof(*binding) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V19_VERSION ||
        binding->base.struct_size != sizeof(binding->base) ||
        binding->base.abi_version != PRDB_SDK_SQL_ABI_V16_VERSION)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    sql = statement->base.base.sql;
    sql_len = statement->base.base.sql_len;
    left_name = statement->base.base.left_relation;
    right_name = statement->base.base.right_relation;
    if (sql == NULL || left_name.offset > sql_len ||
        left_name.length > sql_len - left_name.offset ||
        right_name.offset > sql_len ||
        right_name.length > sql_len - right_name.offset ||
        left_name.length != plan->left_relation.relation_name_len ||
        right_name.length != plan->right_relation.relation_name_len ||
        memcmp(sql + left_name.offset, plan->left_relation.relation_name,
               left_name.length) != 0 ||
        memcmp(sql + right_name.offset, plan->right_relation.relation_name,
               right_name.length) != 0 ||
        schema_len != plan->schema_name_len ||
        memcmp(schema, plan->schema_name, schema_len) != 0 ||
        statement->base.base.projection_count != plan->projection_count ||
        binding->base.base.projection_count != plan->projection_count ||
        statement->base.join_key_count != plan->join_key_count ||
        binding->base.join_key_count != plan->join_key_count ||
        statement->base.base.has_limit != plan->has_limit ||
        (plan->has_limit && statement->base.base.limit != plan->limit))
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    for (uint32_t index = 0; index < plan->projection_count; ++index) {
        const prdb_sdk_sql_join_projection_binding_v8 *bound =
            &binding->base.base.projections[index];
        if (plan->projections[index].relation_side != bound->relation_side ||
            !plan_column_matches_bound(&plan->projections[index].column,
                                       &bound->column))
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    for (uint32_t index = 0; index < plan->join_key_count; ++index) {
        const prdb_sdk_sql_join_key_binding_v16 *bound =
            &binding->base.join_keys[index];
        if (!plan_column_matches_bound(&plan->join_keys[index].left_column,
                                       &bound->left) ||
            !plan_column_matches_bound(&plan->join_keys[index].right_column,
                                       &bound->right))
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_match_join_plan_v19_v1(
    const prdb_sdk_select_v19 *statement,
    const prdb_sdk_select_binding_v19 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len) {
    prdb_sdk_optimizer_join_plan_v1 plan = {0};
    prdb_sdk_optimizer_index_join_plan_v1 index_plan = {0};
    uint16_t version;
    uint8_t kind;
    uint8_t reserved;
    int32_t status;
    if (plan_payload == NULL)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    if (!prdb_decode_optimizer_plan_header_record(
            plan_payload, PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES,
            &version, &kind, &reserved) || version != 1 || reserved != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (kind == PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_NESTED_LOOP_JOIN) {
        status = prdb_sdk_optimizer_decode_index_join_plan_v1(
            plan_payload, plan_payload_len, &index_plan, sizeof(index_plan));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        if (index_plan.has_filter != 0)
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        status = match_join_plan_decoded_v19_v1(
            statement, binding, schema, schema_len, &index_plan.join);
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        for (uint32_t index = 0; index < binding->base.join_key_count; ++index) {
            const prdb_sdk_sql_join_key_binding_v16 *key =
                &binding->base.join_keys[index];
            const prdb_sdk_sql_bound_column_v2 *inner =
                index_plan.inner_side == 0 ? &key->left : &key->right;
            if (plan_column_matches_bound(&index_plan.key_column, inner))
                return PRDB_SDK_OPTIMIZER_PLAN_OK;
        }
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    status = prdb_sdk_optimizer_decode_join_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    return match_join_plan_decoded_v19_v1(statement, binding, schema,
                                          schema_len, &plan);
}

int32_t prdb_sdk_optimizer_match_filtered_join_plan_v20_v1(
    const prdb_sdk_select_v20 *statement,
    const prdb_sdk_select_binding_v20 *binding,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len) {
    prdb_sdk_optimizer_filtered_join_plan_v1 plan = {0};
    prdb_sdk_optimizer_index_join_plan_v1 index_plan = {0};
    const prdb_sdk_optimizer_join_plan_v1 *join_plan;
    const prdb_sdk_optimizer_join_filter_v1 *filter_plan;
    prdb_sdk_select_binding_v19 base_binding;
    prdb_sdk_value_v1 bound_parameter = {0};
    uint32_t expression_kind;
    uint16_t version;
    uint8_t kind;
    uint8_t reserved;
    int32_t status;
    if (statement == NULL || binding == NULL || plan_payload == NULL ||
        statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V20_VERSION ||
        statement->has_where != 1 ||
        binding->struct_size != sizeof(*binding) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V20_VERSION ||
        binding->output_projection_count !=
            statement->base.base.base.projection_count ||
        binding->base.base.base.projection_count !=
            binding->output_projection_count + 1 ||
        binding->where_projection_index != binding->output_projection_count ||
        binding->where_relation_side > 1)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    if (!prdb_decode_optimizer_plan_header_record(
            plan_payload, PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES,
            &version, &kind, &reserved) || version != 1 || reserved != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (kind == PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_NESTED_LOOP_JOIN) {
        status = prdb_sdk_optimizer_decode_index_join_plan_v1(
            plan_payload, plan_payload_len, &index_plan, sizeof(index_plan));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        if (index_plan.has_filter != 1)
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        join_plan = &index_plan.join;
        filter_plan = &index_plan.filter;
    } else {
        status = prdb_sdk_optimizer_decode_filtered_join_plan_v1(
            plan_payload, plan_payload_len, &plan, sizeof(plan));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        join_plan = &plan.join;
        filter_plan = &plan.filter;
    }
    base_binding = binding->base;
    base_binding.base.base.projection_count = binding->output_projection_count;
    status = match_join_plan_decoded_v19_v1(
        &statement->base, &base_binding, schema, schema_len, join_plan);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    if (statement->comparison >= PRDB_SDK_SQL_COMPARE_EQUAL &&
        statement->comparison <= PRDB_SDK_SQL_COMPARE_GREATER_EQUAL) {
        expression_kind = 0;
        if (statement->parameter_index == 0 ||
            statement->parameter_index > parameter_count ||
            parameters == NULL || filter_plan->expression_kind != 0 ||
            filter_plan->comparison != statement->comparison -
                                         PRDB_SDK_SQL_COMPARE_EQUAL ||
            filter_plan->parameter_index != statement->parameter_index ||
            prdb_sdk_sql_bind_parameter_v1(
                statement->parameter_index, parameters, parameter_count,
                binding->where_column.type_code,
                binding->where_column.decimal_precision,
                binding->where_column.decimal_scale, &bound_parameter,
                sizeof(bound_parameter)) != PRDB_SDK_SQL_EVAL_OK)
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    } else if (statement->comparison == PRDB_SDK_SQL_COMPARE_IS_NULL) {
        expression_kind = 1;
    } else if (statement->comparison == PRDB_SDK_SQL_COMPARE_IS_NOT_NULL) {
        expression_kind = 2;
    } else {
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    if (filter_plan->relation_side != binding->where_relation_side ||
        !plan_column_matches_bound(&filter_plan->column,
                                   &binding->where_column) ||
        filter_plan->expression_kind != expression_kind ||
        (expression_kind != 0 &&
         (statement->parameter_index != 0 ||
          filter_plan->parameter_index != 0 || filter_plan->comparison != 0)))
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    if (kind == PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_NESTED_LOOP_JOIN) {
        int key_found = 0;
        for (uint32_t index = 0; index < binding->base.base.join_key_count;
             ++index) {
            const prdb_sdk_sql_join_key_binding_v16 *key =
                &binding->base.base.join_keys[index];
            const prdb_sdk_sql_bound_column_v2 *inner =
                index_plan.inner_side == 0 ? &key->left : &key->right;
            if (plan_column_matches_bound(&index_plan.key_column, inner))
                key_found = 1;
        }
        if (!key_found)
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

static int32_t decode_plan_internal(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_plan_v1 *out, uint32_t out_capacity,
    prdb_sdk_optimizer_filter_v1 *filter_out,
    uint32_t filter_out_capacity, int allow_filter,
    point_probe_hint *point_probe_out,
    range_probe_hint *range_probe_out,
    uint8_t *empty_view_out,
    uint8_t expected_kind) {
    prdb_sdk_optimizer_plan_v1 decoded;
    prdb_sdk_optimizer_filter_v1 decoded_filter = {0};
    plan_reader reader;
    uint16_t version;
    uint8_t kind;
    uint8_t reserved;
    uint8_t node_count;
    uint8_t node_tag;
    uint64_t node_tag_offset;
    uint16_t count;
    uint32_t index;
    uint32_t other;
    uint8_t has_limit = 0;
    uint32_t limit = 0;
    uint8_t has_filter = 0;
    uint8_t has_unique_probe = 0;
    uint8_t empty_view_hint = 0;
    uint8_t predicate_references[PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS] = {0};
    point_probe_hint decoded_point_probe = {0};
    range_probe_hint decoded_range_probe = {0};

    if (point_probe_out != NULL) memset(point_probe_out, 0, sizeof(*point_probe_out));
    if (range_probe_out != NULL) memset(range_probe_out, 0, sizeof(*range_probe_out));
    if (empty_view_out != NULL) *empty_view_out = 0;

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

    if (!prdb_decode_optimizer_plan_header_record(
            payload, PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES,
            &version, &kind, &reserved)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    reader.offset = PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    if (version != 1 || kind != expected_kind || reserved != 0) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (!read_name(&reader, &decoded.schema_name, &decoded.schema_name_len) ||
        decoded.schema_name_len == 0) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    {
        const uint8_t *bytes;
        if (!take(&reader,
                  PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
                  &bytes) ||
            !prdb_decode_optimizer_plan_schema_reference_record(
                bytes, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
                decoded.schema_id, &decoded.schema_version) ||
            all_zero(decoded.schema_id, 16) || decoded.schema_version == 0) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
    }
    if (!read_name(&reader, &decoded.relation_name,
                   &decoded.relation_name_len)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    {
        const uint8_t *bytes;
        if (!take(&reader,
                  PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
                  &bytes) ||
            !prdb_decode_optimizer_plan_relation_reference_record(
                bytes,
                PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
                decoded.relation_id, &decoded.table_epoch) ||
            all_zero(decoded.relation_id, 16) || decoded.table_epoch == 0) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
    }
    if (!read_plan_node_count(&reader, &node_count) || node_count < 2 ||
        node_count > 4) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (decoded.schema_name_len == 0 || decoded.relation_name_len == 0) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }

    if (!read_u8(&reader, &node_tag) || node_tag != 1 ||
        !read_column_list_header(&reader, 1, &count) ||
        (expected_kind == 1 && count == 0) ||
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

    node_tag_offset = reader.offset;
    if (!read_u8(&reader, &node_tag)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (node_tag == 2) {
        if (!read_filter_node_tag(&reader, node_tag_offset)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        uint64_t predicate_start = reader.offset;
        uint32_t predicate_nodes = 0;
        predicate_type predicate_type_out;
        if (!allow_filter) return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
        if (!decode_predicate(&reader, decoded.scan_columns,
                              decoded.scan_column_count, 1, &predicate_nodes,
                              predicate_references, &predicate_type_out) ||
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
    if (expected_kind == 2) {
        if (node_tag != 5 || !has_filter || node_count != 3 ||
            reader.offset != reader.length) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        decoded.plan_kind = 2;
        decoded.struct_size = sizeof(decoded);
        decoded.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
        *out = decoded;
        decoded_filter.struct_size = sizeof(decoded_filter);
        decoded_filter.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
        *filter_out = decoded_filter;
        return PRDB_SDK_OPTIMIZER_PLAN_OK;
    }
    if (node_tag != 3 || !read_column_list_header(&reader, 3, &count) || count == 0 ||
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
            !read_limit_node(&reader, &limit)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        has_limit = 1;
    }
    if (node_count != (uint8_t)(2 + has_filter + has_limit)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (reader.offset != reader.length) {
        uint8_t tag;
        const uint8_t *index_id;
        prdb_sdk_optimizer_plan_column_v1 key_column = {0};
        int key_found = 0;
        if (expected_kind != 1 || !read_u8(&reader, &tag)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        if (tag == 0xa3) {
            if (reader.offset != reader.length) {
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            }
            empty_view_hint = 1;
        } else if ((tag != 0xa1 && tag != 0xa2) ||
            !take(&reader, 16, &index_id) || all_zero(index_id, 16) ||
            !read_column(&reader, &key_column)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        } else {
            for (index = 0; index < decoded.scan_column_count; ++index) {
                if (columns_equal(&key_column, &decoded.scan_columns[index])) {
                    key_found = 1;
                    break;
                }
            }
            if (!key_found) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            if (tag == 0xa1) {
                predicate_type value_type = {0};
                uint32_t value_nodes = 0;
                uint8_t value_references[PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS] = {0};
                uint64_t literal_offset;
                uint8_t literal_tag;
                if (!read_u8(&reader, &literal_tag) ||
                    (literal_tag != 0x01 && literal_tag != 0x02 && literal_tag != 0x03)) {
                    return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
                }
                --reader.offset;
                literal_offset = reader.offset;
                if (!decode_predicate(&reader, decoded.scan_columns,
                                      decoded.scan_column_count, 1, &value_nodes,
                                      value_references, &value_type) ||
                    value_nodes != 1 || value_type.is_null != 0 ||
                    value_type.type_code != key_column.type_code ||
                    memcmp(value_type.parameters, key_column.type_parameters, 4) != 0 ||
                    reader.offset != reader.length) {
                    return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
                }
                if (literal_tag == 0x02) {
                    plan_reader integer_reader = {
                        payload + literal_offset, reader.offset - literal_offset, 1};
                    int64_t integer_value;
                    if (!read_integer_predicate_record(&integer_reader, 0,
                                                       &integer_value) ||
                        integer_reader.offset != integer_reader.length ||
                        integer_value < INT32_MIN || integer_value > INT32_MAX) {
                        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
                    }
                }
                decoded_point_probe.present = 1;
                memcpy(decoded_point_probe.index_id, index_id, 16);
                decoded_point_probe.key_column = key_column;
                decoded_point_probe.literal = payload + literal_offset;
                decoded_point_probe.literal_len =
                    (uint32_t)(reader.offset - literal_offset);
                has_unique_probe = 1;
            } else {
                uint8_t *inclusive_values[2] = {
                    &decoded_range_probe.lower_inclusive,
                    &decoded_range_probe.upper_inclusive};
                const uint8_t **literal_values[2] = {
                    &decoded_range_probe.lower_literal,
                    &decoded_range_probe.upper_literal};
                uint32_t *literal_lengths[2] = {
                    &decoded_range_probe.lower_literal_len,
                    &decoded_range_probe.upper_literal_len};
                uint8_t has_bound[2] = {0, 0};
                for (uint32_t bound_index = 0; bound_index < 2; ++bound_index) {
                    uint8_t bound_flag;
                    if (!read_u8(&reader, &bound_flag) || bound_flag > 2) {
                        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
                    }
                    if (bound_flag == 0) continue;
                    uint64_t literal_offset = reader.offset;
                    uint32_t value_nodes = 0;
                    predicate_type value_type = {0};
                    uint8_t value_references[PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS] = {0};
                    if (!decode_predicate(&reader, decoded.scan_columns,
                                          decoded.scan_column_count, 1, &value_nodes,
                                          value_references, &value_type) ||
                        value_nodes != 1 || value_type.is_null != 0 ||
                        value_type.type_code != key_column.type_code ||
                        memcmp(value_type.parameters, key_column.type_parameters, 4) != 0) {
                        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
                    }
                    if (payload[literal_offset] == 0x02) {
                        plan_reader integer_reader = {
                            payload + literal_offset,
                            reader.offset - literal_offset, 1};
                        int64_t integer_value;
                        if (!read_integer_predicate_record(&integer_reader, 0,
                                                           &integer_value) ||
                            integer_reader.offset != integer_reader.length ||
                            integer_value < INT32_MIN || integer_value > INT32_MAX) {
                            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
                        }
                    }
                    *literal_values[bound_index] = payload + literal_offset;
                    *literal_lengths[bound_index] =
                        (uint32_t)(reader.offset - literal_offset);
                    *inclusive_values[bound_index] = (uint8_t)(bound_flag == 1);
                    has_bound[bound_index] = 1;
                }
                decoded_range_probe.present = 1;
                memcpy(decoded_range_probe.index_id, index_id, 16);
                decoded_range_probe.key_column = key_column;
                if ((!has_bound[0] && !has_bound[1]) || reader.offset != reader.length ||
                    !range_probe_matches_filter(&decoded_filter, &key_column,
                                                &decoded_range_probe)) {
                    return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
                }
            }
        }
    }
    if (has_unique_probe != 0 &&
        !point_probe_matches_filter(&decoded_filter,
                                    &decoded_point_probe.key_column,
                                    decoded_point_probe.literal,
                                    decoded_point_probe.literal_len)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    decoded.struct_size = sizeof(decoded);
    decoded.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    decoded.has_limit = has_limit;
    decoded.limit = limit;
    decoded.plan_kind = 1;
    *out = decoded;
    if (allow_filter) {
        decoded_filter.struct_size = sizeof(decoded_filter);
        decoded_filter.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
        *filter_out = decoded_filter;
    }
    if (point_probe_out != NULL) *point_probe_out = decoded_point_probe;
    if (range_probe_out != NULL) *range_probe_out = decoded_range_probe;
    if (empty_view_out != NULL) *empty_view_out = empty_view_hint;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_decode_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_plan_v1 *out, uint32_t out_capacity) {
    return decode_plan_internal(payload, payload_len, out, out_capacity, NULL,
                                0, 0, NULL, NULL, NULL, 1);
}

int32_t prdb_sdk_optimizer_decode_plan_with_filter_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_plan_v1 *out, uint32_t out_capacity,
    prdb_sdk_optimizer_filter_v1 *filter_out,
    uint32_t filter_out_capacity) {
    return decode_plan_internal(payload, payload_len, out, out_capacity,
                                filter_out, filter_out_capacity, 1, NULL, NULL, NULL, 1);
}

int32_t prdb_sdk_optimizer_decode_delete_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_plan_v1 *out, uint32_t out_capacity,
    prdb_sdk_optimizer_filter_v1 *filter_out,
    uint32_t filter_out_capacity) {
    return decode_plan_internal(payload, payload_len, out, out_capacity,
                                filter_out, filter_out_capacity, 1, NULL, NULL, NULL, 2);
}

static int32_t decode_select_plan_with_access_path_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_plan_v1 *out, uint32_t out_capacity,
    prdb_sdk_optimizer_filter_v1 *filter_out, uint32_t filter_out_capacity,
    point_probe_hint *point_probe_out,
    range_probe_hint *range_probe_out, uint8_t *empty_view_out) {
    if (point_probe_out == NULL || range_probe_out == NULL)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    return decode_plan_internal(payload, payload_len, out, out_capacity,
                                filter_out, filter_out_capacity, 1,
                                point_probe_out, range_probe_out,
                                empty_view_out, 1);
}

int32_t prdb_sdk_optimizer_decode_update_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_update_plan_v1 *out, uint32_t out_capacity) {
    prdb_sdk_optimizer_update_plan_v1 decoded;
    plan_reader reader;
    uint16_t version;
    uint8_t kind;
    uint8_t reserved;
    uint8_t node_count;
    uint8_t node_tag;
    uint16_t column_count;
    uint16_t assignment_count;
    uint32_t index;
    uint32_t other;
    uint8_t predicate_references[PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS] = {0};

    if (payload == NULL || out == NULL ||
        out_capacity < sizeof(*out) ||
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
    if (!prdb_decode_optimizer_plan_header_record(
            payload, PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES,
            &version, &kind, &reserved) || version != 1 || kind != 3 ||
        reserved != 0) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    reader.offset = PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    if (!read_name(&reader, &decoded.plan.schema_name,
                   &decoded.plan.schema_name_len) ||
        decoded.plan.schema_name_len == 0) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    {
        const uint8_t *bytes;
        if (!take(&reader,
                  PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
                  &bytes) ||
            !prdb_decode_optimizer_plan_schema_reference_record(
                bytes, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
                decoded.plan.schema_id, &decoded.plan.schema_version) ||
            all_zero(decoded.plan.schema_id, 16) ||
            decoded.plan.schema_version == 0) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
    }
    if (!read_name(&reader, &decoded.plan.relation_name,
                   &decoded.plan.relation_name_len) ||
        decoded.plan.relation_name_len == 0) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    {
        const uint8_t *bytes;
        if (!take(&reader,
                  PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
                  &bytes) ||
            !prdb_decode_optimizer_plan_relation_reference_record(
                bytes, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
                decoded.plan.relation_id, &decoded.plan.table_epoch) ||
            all_zero(decoded.plan.relation_id, 16) ||
            decoded.plan.table_epoch == 0) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
    }
    if (!read_plan_node_count(&reader, &node_count) || node_count != 3 ||
        !read_u8(&reader, &node_tag) || node_tag != 1 ||
        !read_column_list_header(&reader, 1, &column_count) ||
        column_count == 0 ||
        column_count > PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    decoded.plan.scan_column_count = column_count;
    for (index = 0; index < decoded.plan.scan_column_count; ++index) {
        prdb_sdk_optimizer_plan_column_v1 *column =
            &decoded.plan.scan_columns[index];
        if (!read_column(&reader, column) ||
            (index != 0 &&
             decoded.plan.scan_columns[index - 1].ordinal >= column->ordinal)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        for (other = 0; other < index; ++other) {
            if (memcmp(decoded.plan.scan_columns[other].column_id,
                       column->column_id, 16) == 0) {
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            }
        }
    }
    {
        uint64_t filter_node_offset = reader.offset;
        uint64_t predicate_start;
        uint32_t predicate_nodes = 0;
        predicate_type predicate_type_out;
        const uint8_t *node_bytes;
        if (!read_u8(&reader, &node_tag) || node_tag != 2 ||
            !read_filter_node_tag(&reader, filter_node_offset)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        predicate_start = reader.offset;
        if (!decode_predicate(&reader, decoded.plan.scan_columns,
                              decoded.plan.scan_column_count, 1,
                              &predicate_nodes, predicate_references,
                              &predicate_type_out) ||
            !predicate_boolean(&predicate_type_out) ||
            !take(&reader, PRDB_OPTIMIZER_PLAN_UPDATE_NODE_RECORD_HEADER_BYTES,
                  &node_bytes) ||
            !prdb_decode_optimizer_plan_update_node_record(
                node_bytes, PRDB_OPTIMIZER_PLAN_UPDATE_NODE_RECORD_HEADER_BYTES,
                &node_tag, &assignment_count) ||
            node_tag != PRDB_OPTIMIZER_PLAN_UPDATE_NODE ||
            assignment_count == 0 ||
            assignment_count > PRDB_SDK_OPTIMIZER_PLAN_MAX_UPDATE_ASSIGNMENTS) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        decoded.filter.present = 1;
        decoded.filter.prefix = payload + predicate_start;
        decoded.filter.prefix_len = (uint32_t)(reader.offset - predicate_start -
            PRDB_OPTIMIZER_PLAN_UPDATE_NODE_RECORD_HEADER_BYTES);
    }
    decoded.assignment_count = assignment_count;
    for (index = 0; index < decoded.assignment_count; ++index) {
        prdb_sdk_optimizer_assignment_v1 *assignment =
            &decoded.assignments[index];
        const uint8_t *value_bytes;
        uint8_t value_kind;
        uint32_t parameter_index;
        int found = 0;
        if (!read_column(&reader, &assignment->column) ||
            !take(&reader,
                  PRDB_OPTIMIZER_PLAN_UPDATE_ASSIGNMENT_VALUE_RECORD_HEADER_BYTES,
                  &value_bytes) ||
            !prdb_decode_optimizer_plan_update_assignment_value_record(
                value_bytes,
                PRDB_OPTIMIZER_PLAN_UPDATE_ASSIGNMENT_VALUE_RECORD_HEADER_BYTES,
                &value_kind, &parameter_index)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        for (other = 0; other < decoded.plan.scan_column_count; ++other) {
            if (columns_equal(&assignment->column,
                              &decoded.plan.scan_columns[other])) {
                predicate_references[other] = 1;
                found = 1;
                break;
            }
        }
        if (!found) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        for (other = 0; other < index; ++other) {
            if (decoded.assignments[other].column.ordinal ==
                    assignment->column.ordinal ||
                memcmp(decoded.assignments[other].column.column_id,
                       assignment->column.column_id, 16) == 0) {
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            }
        }
        if (value_kind == PRDB_OPTIMIZER_UPDATE_PARAMETER_VALUE) {
            if (parameter_index == 0 || parameter_index > 256) {
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            }
            assignment->value_kind = value_kind;
            assignment->parameter_index = parameter_index;
        } else if (value_kind == PRDB_OPTIMIZER_UPDATE_NULL_VALUE) {
            if (parameter_index != 0 || assignment->column.nullable == 0) {
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            }
            assignment->value_kind = value_kind;
        } else {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
    }
    for (index = 0; index < decoded.plan.scan_column_count; ++index) {
        if (predicate_references[index] == 0) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
    }
    if (reader.offset != reader.length) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    decoded.plan.plan_kind = 3;
    decoded.plan.struct_size = sizeof(decoded.plan);
    decoded.plan.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    decoded.filter.struct_size = sizeof(decoded.filter);
    decoded.filter.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    decoded.struct_size = sizeof(decoded);
    decoded.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    *out = decoded;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

static int32_t decode_any_optimizer_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_plan_v1 *plan, uint32_t plan_capacity,
    prdb_sdk_optimizer_filter_v1 *filter, uint32_t filter_capacity) {
    if (payload == NULL || payload_len < 3)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (payload[2] == 3) {
        prdb_sdk_optimizer_update_plan_v1 update = {0};
        if (plan == NULL || plan_capacity < sizeof(*plan) || filter == NULL ||
            filter_capacity < sizeof(*filter) ||
            ranges_overlap(payload, payload_len, plan, sizeof(*plan)) ||
            ranges_overlap(payload, payload_len, filter, sizeof(*filter)) ||
            ranges_overlap(plan, sizeof(*plan), filter, sizeof(*filter))) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
        }
        int32_t status = prdb_sdk_optimizer_decode_update_plan_v1(
            payload, payload_len, &update, sizeof(update));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        *plan = update.plan;
        *filter = update.filter;
        return PRDB_SDK_OPTIMIZER_PLAN_OK;
    }
    if (payload[2] == 2) {
        return prdb_sdk_optimizer_decode_delete_plan_v1(
            payload, payload_len, plan, plan_capacity, filter,
            filter_capacity);
    }
    if (payload[2] == 5) {
        prdb_sdk_optimizer_group_count_plan_v1 group = {0};
        if (plan == NULL || plan_capacity < sizeof(*plan) || filter == NULL ||
            filter_capacity < sizeof(*filter) ||
            ranges_overlap(payload, payload_len, plan, sizeof(*plan)) ||
            ranges_overlap(payload, payload_len, filter, sizeof(*filter)) ||
            ranges_overlap(plan, sizeof(*plan), filter, sizeof(*filter)))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
        int32_t status = prdb_sdk_optimizer_decode_group_count_plan_v1(
            payload, payload_len, &group, sizeof(group));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        *plan = group.plan;
        memset(filter, 0, sizeof(*filter));
        filter->struct_size = sizeof(*filter);
        filter->abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
        return PRDB_SDK_OPTIMIZER_PLAN_OK;
    }
    if (payload[2] == 8) {
        prdb_sdk_optimizer_ordered_group_count_plan_v1 group = {0};
        if (plan == NULL || plan_capacity < sizeof(*plan) || filter == NULL ||
            filter_capacity < sizeof(*filter) ||
            ranges_overlap(payload, payload_len, plan, sizeof(*plan)) ||
            ranges_overlap(payload, payload_len, filter, sizeof(*filter)) ||
            ranges_overlap(plan, sizeof(*plan), filter, sizeof(*filter)))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
        int32_t status = prdb_sdk_optimizer_decode_ordered_group_count_plan_v1(
            payload, payload_len, &group, sizeof(group));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        *plan = group.plan;
        memset(filter, 0, sizeof(*filter));
        filter->struct_size = sizeof(*filter);
        filter->abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
        return PRDB_SDK_OPTIMIZER_PLAN_OK;
    }
    if (payload[2] == 9) {
        prdb_sdk_optimizer_ordered_composite_group_count_plan_v1 group = {0};
        if (plan == NULL || plan_capacity < sizeof(*plan) || filter == NULL ||
            filter_capacity < sizeof(*filter) ||
            ranges_overlap(payload, payload_len, plan, sizeof(*plan)) ||
            ranges_overlap(payload, payload_len, filter, sizeof(*filter)) ||
            ranges_overlap(plan, sizeof(*plan), filter, sizeof(*filter)))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
        int32_t status = prdb_sdk_optimizer_decode_ordered_composite_group_count_plan_v1(payload, payload_len, &group, sizeof(group));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        *plan = group.plan;
        memset(filter, 0, sizeof(*filter));
        filter->struct_size = sizeof(*filter); filter->abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
        return PRDB_SDK_OPTIMIZER_PLAN_OK;
    }
    if (payload[2] == 10) {
        prdb_sdk_optimizer_group_aggregate_plan_v1 group = {0};
        if (plan == NULL || plan_capacity < sizeof(*plan) || filter == NULL || filter_capacity < sizeof(*filter) ||
            ranges_overlap(payload, payload_len, plan, sizeof(*plan)) || ranges_overlap(payload, payload_len, filter, sizeof(*filter)) || ranges_overlap(plan, sizeof(*plan), filter, sizeof(*filter)))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
        int32_t status = prdb_sdk_optimizer_decode_group_aggregate_plan_v1(payload, payload_len, &group, sizeof(group));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        *plan = group.plan;
        memset(filter, 0, sizeof(*filter)); filter->struct_size = sizeof(*filter); filter->abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
        return PRDB_SDK_OPTIMIZER_PLAN_OK;
    }
    if (payload[2] == 11) {
        prdb_sdk_optimizer_ordered_group_aggregate_plan_v1 group = {0};
        if (plan == NULL || plan_capacity < sizeof(*plan) || filter == NULL ||
            filter_capacity < sizeof(*filter) ||
            ranges_overlap(payload, payload_len, plan, sizeof(*plan)) ||
            ranges_overlap(payload, payload_len, filter, sizeof(*filter)) ||
            ranges_overlap(plan, sizeof(*plan), filter, sizeof(*filter)))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
        int32_t status =
            prdb_sdk_optimizer_decode_ordered_group_aggregate_plan_v1(
                payload, payload_len, &group, sizeof(group));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        *plan = group.plan;
        memset(filter, 0, sizeof(*filter));
        filter->struct_size = sizeof(*filter);
        filter->abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
        return PRDB_SDK_OPTIMIZER_PLAN_OK;
    }
    if (payload[2] == 6) {
        prdb_sdk_optimizer_composite_group_count_plan_v1 group = {0};
        if (plan == NULL || plan_capacity < sizeof(*plan) || filter == NULL ||
            filter_capacity < sizeof(*filter) ||
            ranges_overlap(payload, payload_len, plan, sizeof(*plan)) ||
            ranges_overlap(payload, payload_len, filter, sizeof(*filter)) ||
            ranges_overlap(plan, sizeof(*plan), filter, sizeof(*filter)))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
        int32_t status =
            prdb_sdk_optimizer_decode_composite_group_count_plan_v1(
                payload, payload_len, &group, sizeof(group));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        *plan = group.plan;
        memset(filter, 0, sizeof(*filter));
        filter->struct_size = sizeof(*filter);
        filter->abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
        return PRDB_SDK_OPTIMIZER_PLAN_OK;
    }
    if (payload[2] == 13) {
        prdb_sdk_optimizer_composite_group_aggregate_plan_v1 group = {0};
        if (plan == NULL || plan_capacity < sizeof(*plan) || filter == NULL ||
            filter_capacity < sizeof(*filter) ||
            ranges_overlap(payload, payload_len, plan, sizeof(*plan)) ||
            ranges_overlap(payload, payload_len, filter, sizeof(*filter)) ||
            ranges_overlap(plan, sizeof(*plan), filter, sizeof(*filter)))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
        int32_t status =
            prdb_sdk_optimizer_decode_composite_group_aggregate_plan_v1(
                payload, payload_len, &group, sizeof(group));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        *plan = group.plan;
        memset(filter, 0, sizeof(*filter));
        filter->struct_size = sizeof(*filter);
        filter->abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
        return PRDB_SDK_OPTIMIZER_PLAN_OK;
    }
    if (payload[2] == 14) {
        prdb_sdk_optimizer_ordered_composite_group_aggregate_plan_v1 group = {0};
        if (plan == NULL || plan_capacity < sizeof(*plan) || filter == NULL ||
            filter_capacity < sizeof(*filter) ||
            ranges_overlap(payload, payload_len, plan, sizeof(*plan)) ||
            ranges_overlap(payload, payload_len, filter, sizeof(*filter)) ||
            ranges_overlap(plan, sizeof(*plan), filter, sizeof(*filter)))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
        int32_t status =
            prdb_sdk_optimizer_decode_ordered_composite_group_aggregate_plan_v1(
                payload, payload_len, &group, sizeof(group));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        *plan = group.plan;
        memset(filter, 0, sizeof(*filter));
        filter->struct_size = sizeof(*filter);
        filter->abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
        return PRDB_SDK_OPTIMIZER_PLAN_OK;
    }
    if (payload[2] == 15) {
        prdb_sdk_optimizer_filtered_composite_group_aggregate_plan_v1 group = {0};
        if (plan == NULL || plan_capacity < sizeof(*plan) || filter == NULL ||
            filter_capacity < sizeof(*filter) ||
            ranges_overlap(payload, payload_len, plan, sizeof(*plan)) ||
            ranges_overlap(payload, payload_len, filter, sizeof(*filter)) ||
            ranges_overlap(plan, sizeof(*plan), filter, sizeof(*filter)))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
        int32_t status =
            prdb_sdk_optimizer_decode_filtered_composite_group_aggregate_plan_v1(
                payload, payload_len, &group, sizeof(group));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        *plan = group.plan;
        *filter = group.filter;
        return PRDB_SDK_OPTIMIZER_PLAN_OK;
    }
    if (payload[2] == 16) {
        prdb_sdk_optimizer_ordered_filtered_composite_group_aggregate_plan_v1 group = {0};
        if (plan == NULL || plan_capacity < sizeof(*plan) || filter == NULL ||
            filter_capacity < sizeof(*filter) ||
            ranges_overlap(payload, payload_len, plan, sizeof(*plan)) ||
            ranges_overlap(payload, payload_len, filter, sizeof(*filter)) ||
            ranges_overlap(plan, sizeof(*plan), filter, sizeof(*filter)))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
        int32_t status =
            prdb_sdk_optimizer_decode_ordered_filtered_composite_group_aggregate_plan_v1(
                payload, payload_len, &group, sizeof(group));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        *plan = group.plan;
        *filter = group.filter;
        return PRDB_SDK_OPTIMIZER_PLAN_OK;
    }
    if (payload[2] == 7) {
        prdb_sdk_optimizer_ordered_select_plan_v1 ordered = {0};
        if (plan == NULL || plan_capacity < sizeof(*plan) || filter == NULL ||
            filter_capacity < sizeof(*filter) ||
            ranges_overlap(payload, payload_len, plan, sizeof(*plan)) ||
            ranges_overlap(payload, payload_len, filter, sizeof(*filter)) ||
            ranges_overlap(plan, sizeof(*plan), filter, sizeof(*filter)))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
        int32_t status = prdb_sdk_optimizer_decode_ordered_select_plan_v1(
            payload, payload_len, &ordered, sizeof(ordered));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        *plan = ordered.plan;
        memset(filter, 0, sizeof(*filter));
        filter->struct_size = sizeof(*filter);
        filter->abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
        return PRDB_SDK_OPTIMIZER_PLAN_OK;
    }
    return prdb_sdk_optimizer_decode_plan_with_filter_v1(
        payload, payload_len, plan, plan_capacity, filter, filter_capacity);
}

int32_t prdb_sdk_optimizer_validate_executor_group_count_relation_v1(
    const prdb_host_api_v2 *host, const uint8_t *plan_payload,
    uint64_t plan_payload_len, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len) {
    prdb_sdk_optimizer_group_count_plan_v1 plan = {0};
    int32_t status;
    if (plan_payload == NULL) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    status = prdb_sdk_optimizer_decode_group_count_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    return prdb_sdk_optimizer_validate_executor_relation_v1(
        host, plan_payload, plan_payload_len, response, response_capacity,
        response_len);
}

int32_t prdb_sdk_optimizer_validate_executor_composite_group_count_relation_v1(
    const prdb_host_api_v2 *host, const uint8_t *plan_payload,
    uint64_t plan_payload_len, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len) {
    prdb_sdk_optimizer_composite_group_count_plan_v1 plan = {0};
    int32_t status;
    if (plan_payload == NULL) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    status = prdb_sdk_optimizer_decode_composite_group_count_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    return prdb_sdk_optimizer_validate_executor_relation_v1(
        host, plan_payload, plan_payload_len, response, response_capacity,
        response_len);
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
    uint8_t relation_status;
    uint8_t response_schema_id[16];
    uint8_t response_relation_id[16];
    uint64_t response_schema_version;
    uint64_t response_table_epoch;
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
    plan_status = decode_any_optimizer_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan), &filter,
        sizeof(filter));
    if (plan_status != PRDB_SDK_OPTIMIZER_PLAN_OK) return plan_status;

    schema_len = plan.schema_name_len;
    relation_len = plan.relation_name_len;
    prdb_encode_executor_resolve_relation_request(
        request, (uint16_t)schema_len, (uint16_t)relation_len);
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
    if (prdb_decode_planner_describe_plan_relation_unavailable_response(
            response, (size_t)*response_len, &relation_status) &&
        relation_status == 1) {
        return PRDB_SDK_OPTIMIZER_PLAN_UNAVAILABLE;
    }
    if (!prdb_decode_planner_describe_plan_relation_available_response(
            response, (size_t)*response_len, &relation_status,
            response_schema_id, &response_schema_version,
            &response_table_epoch, response_relation_id) ||
        relation_status != 0 || *response_len < 55) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (memcmp(response_schema_id, plan.schema_id, 16) != 0 ||
        response_schema_version != plan.schema_version ||
        response_table_epoch != plan.table_epoch ||
        memcmp(response_relation_id, plan.relation_id, 16) != 0) {
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

static int32_t validate_join_relation_identity(
    const prdb_host_api_v2 *host, const uint8_t *schema_name,
    uint32_t schema_name_len, const uint8_t schema_id[16],
    uint64_t schema_version,
    const prdb_sdk_optimizer_join_relation_v1 *plan_relation,
    uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len) {
    uint8_t request[6 + 2 * 63];
    uint8_t relation_status;
    uint8_t response_schema_id[16];
    uint8_t response_relation_id[16];
    uint64_t response_schema_version;
    uint64_t response_table_epoch;
    uint64_t offset;
    uint32_t column_count;
    uint32_t scan_index = 0;
    int32_t callback_status;

    prdb_encode_executor_resolve_relation_request(
        request, (uint16_t)schema_name_len,
        (uint16_t)plan_relation->relation_name_len);
    memcpy(request + 6, schema_name, schema_name_len);
    memcpy(request + 6 + schema_name_len, plan_relation->relation_name,
           plan_relation->relation_name_len);
    *response_len = 0;
    callback_status = host->executor_request(
        host->context, PRDB_IPC_OP_RESOLVE_PLAN_RELATION, request,
        6 + schema_name_len + plan_relation->relation_name_len, response,
        response_capacity, response_len);
    if (callback_status == PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL)
        return PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL;
    if (callback_status != PRDB_QUERY_CALLBACK_OK ||
        *response_len > response_capacity || *response_len < 3)
        return PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
    if (prdb_decode_planner_describe_plan_relation_unavailable_response(
            response, (size_t)*response_len, &relation_status) &&
        relation_status == 1)
        return PRDB_SDK_OPTIMIZER_PLAN_UNAVAILABLE;
    if (!prdb_decode_planner_describe_plan_relation_available_response(
            response, (size_t)*response_len, &relation_status,
            response_schema_id, &response_schema_version,
            &response_table_epoch, response_relation_id) ||
        relation_status != 0 || *response_len < 55)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (memcmp(response_schema_id, schema_id, 16) != 0 ||
        response_schema_version != schema_version ||
        response_table_epoch != plan_relation->table_epoch ||
        memcmp(response_relation_id, plan_relation->relation_id, 16) != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    offset = 51;
    if (offset + 2 > *response_len)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    uint32_t relation_name_len = response_u16(response, offset);
    offset += 2;
    if (relation_name_len == 0 || relation_name_len > 63 ||
        offset + relation_name_len + 2 > *response_len ||
        !response_identifier(response + offset, relation_name_len))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (relation_name_len != plan_relation->relation_name_len ||
        memcmp(response + offset, plan_relation->relation_name,
               relation_name_len) != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    offset += relation_name_len;
    column_count = response_u16(response, offset);
    offset += 2;
    if (column_count == 0 || column_count > 4096)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    for (uint32_t ordinal = 0; ordinal < column_count; ++ordinal) {
        uint32_t column_ordinal;
        uint8_t nullable;
        uint8_t type_code;
        const uint8_t *column_id;
        const uint8_t *parameters;
        uint32_t column_name_len;
        if (offset + 28 > *response_len)
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        column_id = response + offset;
        if (all_zero(column_id, 16))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        offset += 16;
        column_ordinal = response_u32(response, offset);
        offset += 4;
        nullable = response[offset++];
        type_code = response[offset++];
        parameters = response + offset;
        offset += 4;
        if (column_ordinal != ordinal || nullable > 1 ||
            !valid_type(type_code, parameters) || offset + 2 > *response_len)
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        column_name_len = response_u16(response, offset);
        offset += 2;
        if (column_name_len == 0 || column_name_len > 63 ||
            offset + column_name_len > *response_len ||
            !response_identifier(response + offset, column_name_len))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        offset += column_name_len;
        if (scan_index < plan_relation->scan_column_count &&
            plan_relation->scan_columns[scan_index].ordinal < ordinal)
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        if (scan_index < plan_relation->scan_column_count &&
            plan_relation->scan_columns[scan_index].ordinal == ordinal) {
            const prdb_sdk_optimizer_plan_column_v1 *expected =
                &plan_relation->scan_columns[scan_index++];
            if (memcmp(column_id, expected->column_id, 16) != 0 ||
                type_code != expected->type_code ||
                nullable != expected->nullable ||
                memcmp(parameters, expected->type_parameters, 4) != 0)
                return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        }
    }
    if (offset != *response_len)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (scan_index != plan_relation->scan_column_count)
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_validate_executor_join_relations_v1(
    const prdb_host_api_v2 *host, const uint8_t *plan_payload,
    uint64_t plan_payload_len, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len) {
    prdb_sdk_optimizer_join_plan_v1 plan = {0};
    prdb_sdk_optimizer_index_join_plan_v1 index_plan = {0};
    uint16_t version;
    uint8_t kind;
    uint8_t reserved;
    const prdb_sdk_optimizer_join_plan_v1 *join_plan;
    int32_t status;
    if (host == NULL || host->struct_size < sizeof(*host) ||
        host->abi_version != PRDB_QUERY_ABI_V2_VERSION ||
        host->executor_request == NULL || plan_payload == NULL ||
        response == NULL || response_len == NULL ||
        response_capacity > PRDB_QUERY_MAX_IPC_RESPONSE_BYTES ||
        response_capacity < 3 ||
        plan_payload_len > PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES ||
        ranges_overlap(plan_payload, plan_payload_len, response,
                       response_capacity))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    if (!prdb_decode_optimizer_plan_header_record(
            plan_payload, PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES,
            &version, &kind, &reserved) || version != 1 || reserved != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (kind == PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_NESTED_LOOP_JOIN) {
        status = prdb_sdk_optimizer_decode_index_join_plan_v1(
            plan_payload, plan_payload_len, &index_plan, sizeof(index_plan));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        join_plan = &index_plan.join;
    } else {
        status = prdb_sdk_optimizer_decode_join_plan_v1(
            plan_payload, plan_payload_len, &plan, sizeof(plan));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        join_plan = &plan;
    }
    status = validate_join_relation_identity(
        host, join_plan->schema_name, join_plan->schema_name_len,
        join_plan->schema_id, join_plan->schema_version,
        &join_plan->left_relation, response,
        response_capacity, response_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    return validate_join_relation_identity(
        host, join_plan->schema_name, join_plan->schema_name_len,
        join_plan->schema_id, join_plan->schema_version,
        &join_plan->right_relation, response,
        response_capacity, response_len);
}

int32_t prdb_sdk_optimizer_validate_executor_filtered_join_relations_v1(
    const prdb_host_api_v2 *host, const uint8_t *plan_payload,
    uint64_t plan_payload_len, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len) {
    prdb_sdk_optimizer_filtered_join_plan_v1 plan = {0};
    prdb_sdk_optimizer_index_join_plan_v1 index_plan = {0};
    uint16_t version;
    uint8_t kind;
    uint8_t reserved;
    const prdb_sdk_optimizer_join_plan_v1 *join_plan;
    int32_t status;
    if (host == NULL || host->struct_size < sizeof(*host) ||
        host->abi_version != PRDB_QUERY_ABI_V2_VERSION ||
        host->executor_request == NULL || plan_payload == NULL ||
        response == NULL || response_len == NULL ||
        response_capacity > PRDB_QUERY_MAX_IPC_RESPONSE_BYTES ||
        response_capacity < 3 ||
        plan_payload_len > PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES ||
        ranges_overlap(plan_payload, plan_payload_len, response,
                       response_capacity))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    if (!prdb_decode_optimizer_plan_header_record(
            plan_payload, PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES,
            &version, &kind, &reserved) || version != 1 || reserved != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (kind == PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_NESTED_LOOP_JOIN) {
        status = prdb_sdk_optimizer_decode_index_join_plan_v1(
            plan_payload, plan_payload_len, &index_plan, sizeof(index_plan));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        if (index_plan.has_filter != 1)
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        join_plan = &index_plan.join;
    } else {
        status = prdb_sdk_optimizer_decode_filtered_join_plan_v1(
            plan_payload, plan_payload_len, &plan, sizeof(plan));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        join_plan = &plan.join;
    }
    status = validate_join_relation_identity(
        host, join_plan->schema_name, join_plan->schema_name_len,
        join_plan->schema_id, join_plan->schema_version,
        &join_plan->left_relation, response, response_capacity, response_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    return validate_join_relation_identity(
        host, join_plan->schema_name, join_plan->schema_name_len,
        join_plan->schema_id, join_plan->schema_version,
        &join_plan->right_relation, response, response_capacity, response_len);
}

int32_t prdb_sdk_optimizer_acquire_plan_relation_v1(
    const prdb_host_api_v2 *host, const uint8_t *plan_payload,
    uint64_t plan_payload_len, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_capacity) {
    prdb_sdk_optimizer_plan_v1 plan = {0};
    prdb_sdk_optimizer_filter_v1 filter = {0};
    int32_t status;
    uint64_t columns_bytes =
        (uint64_t)column_capacity * sizeof(prdb_sdk_sql_column_shape_v2);

    if (relation_out == NULL || relation_out_capacity < sizeof(*relation_out) ||
        columns == NULL || column_capacity == 0 ||
        ranges_overlap(relation_out, sizeof(*relation_out), columns,
                       columns_bytes) ||
        ranges_overlap(relation_out, sizeof(*relation_out), response,
                       response_capacity) ||
        ranges_overlap(columns, columns_bytes, response, response_capacity) ||
        ranges_overlap(relation_out, sizeof(*relation_out), plan_payload,
                       plan_payload_len) ||
        ranges_overlap(columns, columns_bytes, plan_payload, plan_payload_len)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    status = prdb_sdk_optimizer_validate_executor_relation_v1(
        host, plan_payload, plan_payload_len, response, response_capacity,
        response_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    status = decode_any_optimizer_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan), &filter,
        sizeof(filter));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;

    status = prdb_sdk_executor_resolve_relation_v1(
        host, plan.schema_name, plan.schema_name_len, plan.relation_name,
        plan.relation_name_len, response, response_capacity, response_len,
        relation_out, relation_out_capacity, columns, column_capacity);
    if (status == PRDB_SDK_EXECUTOR_UNAVAILABLE) {
        return PRDB_SDK_OPTIMIZER_PLAN_UNAVAILABLE;
    }
    if (status == PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL) {
        return PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL;
    }
    if (status != PRDB_SDK_EXECUTOR_OK) {
        return PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
    }
    if (memcmp(relation_out->relation_id, plan.relation_id, 16) != 0 ||
        relation_out->name_len != plan.relation_name_len ||
        memcmp(relation_out->name, plan.relation_name,
               plan.relation_name_len) != 0) {
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_open_plan_scan_v1(
    const prdb_host_api_v2 *host, const uint8_t *plan_payload,
    uint64_t plan_payload_len, uint8_t *relation_response,
    uint64_t relation_response_capacity, uint64_t *relation_response_len,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint64_t *scan_response_len, prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_capacity,
    uint8_t cursor_out[16], uint32_t *scan_opened_out) {
    prdb_sdk_optimizer_plan_v1 plan = {0};
    prdb_sdk_optimizer_filter_v1 filter = {0};
    int32_t status;

    if (relation_response == NULL || scan_response == NULL ||
        relation_response_len == NULL || scan_response_len == NULL ||
        cursor_out == NULL || scan_opened_out == NULL ||
        ranges_overlap(relation_response, relation_response_capacity,
                       scan_response, scan_response_capacity) ||
        ranges_overlap(cursor_out, 16, relation_response,
                       relation_response_capacity) ||
        ranges_overlap(cursor_out, 16, scan_response,
                       scan_response_capacity) ||
        ranges_overlap(scan_opened_out, sizeof(*scan_opened_out),
                       relation_response, relation_response_capacity) ||
        ranges_overlap(scan_opened_out, sizeof(*scan_opened_out),
                       scan_response, scan_response_capacity)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    if (ranges_overlap(cursor_out, 16, relation_out,
                       relation_out_capacity) ||
        ranges_overlap(cursor_out, 16, columns,
                       (uint64_t)column_capacity * sizeof(*columns)) ||
        ranges_overlap(scan_opened_out, sizeof(*scan_opened_out), cursor_out,
                       16) ||
        ranges_overlap(scan_opened_out, sizeof(*scan_opened_out), relation_out,
                       relation_out_capacity) ||
        ranges_overlap(scan_opened_out, sizeof(*scan_opened_out), columns,
                       (uint64_t)column_capacity * sizeof(*columns))) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    status = decode_any_optimizer_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan), &filter,
        sizeof(filter));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    status = prdb_sdk_optimizer_acquire_plan_relation_v1(
        host, plan_payload, plan_payload_len, relation_response,
        relation_response_capacity, relation_response_len,
        relation_out, relation_out_capacity, columns, column_capacity);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    if (plan.has_limit != 0 && plan.limit == 0) {
        memset(cursor_out, 0, 16);
        *scan_response_len = 0;
        *scan_opened_out = 0;
        return PRDB_SDK_OPTIMIZER_PLAN_OK;
    }
    status = prdb_sdk_executor_open_full_scan_v1(
        host, relation_out->executor_handle, scan_response,
        scan_response_capacity, scan_response_len, cursor_out);
    if (status == PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL) {
        return PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL;
    }
    if (status != PRDB_SDK_EXECUTOR_OK) {
        return PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
    }
    *scan_opened_out = 1;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_scan_state_init_v1(
    const uint8_t *plan_payload, uint64_t plan_payload_len,
    prdb_sdk_optimizer_scan_state_v1 *state_out,
    uint32_t state_out_capacity) {
    prdb_sdk_optimizer_plan_v1 plan = {0};
    prdb_sdk_optimizer_filter_v1 filter = {0};
    prdb_sdk_optimizer_scan_state_v1 state = {0};
    int32_t status;
    if (state_out == NULL || state_out_capacity < sizeof(*state_out) ||
        ranges_overlap(state_out, sizeof(*state_out), plan_payload,
                       plan_payload_len)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    status = prdb_sdk_optimizer_decode_plan_with_filter_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan), &filter,
        sizeof(filter));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    state.struct_size = sizeof(state);
    state.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    state.projection_count = plan.projection_count;
    state.has_limit = plan.has_limit;
    state.limit = plan.limit;
    state.done = plan.has_limit != 0 && plan.limit == 0;
    memcpy(state.projections, plan.projections,
           plan.projection_count * sizeof(plan.projections[0]));
    *state_out = state;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

static int32_t consume_scan_batch_internal(
    prdb_sdk_optimizer_scan_state_v1 *state,
    const prdb_sdk_batch_v1 *batch, uint32_t relation_column_count,
    prdb_sdk_value_v1 *source_cells, uint32_t source_cell_capacity,
    prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity,
    const prdb_sdk_optimizer_plan_v1 *plan,
    const prdb_sdk_optimizer_filter_v1 *filter,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out) {
    prdb_sdk_row_iterator_v1 rows = {0};
    uint32_t delivered = 0;
    int32_t status;
    if (state == NULL || state->struct_size < sizeof(*state) ||
        state->abi_version != PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION ||
        state->projection_count == 0 ||
        state->projection_count > PRDB_SDK_OPTIMIZER_PLAN_MAX_PROJECTIONS ||
        state->has_limit > 1 || state->done > 1 ||
        (state->has_limit != 0 && state->delivered_rows > state->limit) ||
        batch == NULL || relation_column_count == 0 || source_cells == NULL ||
        relation_column_count > PRDB_SDK_BATCH_MAX_CELLS_PER_ROW ||
        source_cell_capacity < relation_column_count || projected_values == NULL ||
        projected_value_capacity < state->projection_count || on_row == NULL ||
        delivered_rows_out == NULL) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    *delivered_rows_out = 0;
    if (state->done != 0) return PRDB_SDK_OPTIMIZER_PLAN_OK;
    for (uint32_t index = 0; index < state->projection_count; ++index) {
        const prdb_sdk_optimizer_plan_column_v1 *projection =
            &state->projections[index];
        if (projection->ordinal >= relation_column_count ||
            !valid_type(projection->type_code,
                        projection->type_parameters)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
    }
    status = prdb_sdk_batch_rows_begin_v1(batch, &rows, sizeof(rows));
    if (status != PRDB_SDK_BATCH_OK) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    while (rows.rows_remaining != 0) {
        prdb_sdk_row_v1 row = {0};
        prdb_sdk_cell_iterator_v1 cells = {0};
        status = prdb_sdk_batch_row_next_v1(&rows, &row, sizeof(row));
        if (status != PRDB_SDK_BATCH_OK ||
            row.cell_count != relation_column_count ||
            prdb_sdk_row_cells_begin_v1(&row, &cells, sizeof(cells)) !=
                PRDB_SDK_BATCH_OK) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        for (uint32_t ordinal = 0; ordinal < relation_column_count; ++ordinal) {
            if (prdb_sdk_cell_next_v1(&cells, &source_cells[ordinal],
                                      sizeof(source_cells[ordinal])) !=
                PRDB_SDK_BATCH_OK) {
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            }
        }
        if (filter != NULL && filter->present != 0) {
            plan_reader predicate_reader = {filter->prefix,
                                            filter->prefix_len, 0};
            predicate_value predicate = {0};
            uint32_t nodes = 0;
            int32_t eval_status = evaluate_predicate_node(
                &predicate_reader, plan, source_cells, &predicate, 1, &nodes);
            if (eval_status == PRDB_SDK_SQL_EVAL_OK &&
                predicate.is_truth == 0) {
                eval_status = prdb_sdk_sql_value_truth_v1(
                    &predicate.value, &predicate.truth);
                predicate.is_truth = eval_status == PRDB_SDK_SQL_EVAL_OK;
            }
            if (eval_status != PRDB_SDK_SQL_EVAL_OK ||
                predicate_reader.offset != predicate_reader.length ||
                predicate.is_truth == 0) {
                return PRDB_SDK_OPTIMIZER_PLAN_FILTER_ERROR;
            }
            if (predicate.truth != PRDB_SDK_SQL_TRUTH_TRUE) continue;
        }
        for (uint32_t index = 0; index < state->projection_count; ++index) {
            const prdb_sdk_optimizer_plan_column_v1 *projection =
                &state->projections[index];
            const prdb_sdk_value_v1 *value =
                &source_cells[projection->ordinal];
            if (value->state == PRDB_SDK_VALUE_STATE_VALUE &&
                value->type_code != projection->type_code) {
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            }
            projected_values[index] = *value;
        }
        if (on_row(row_context, state->projections, projected_values,
                   state->projection_count) != 0) {
            *delivered_rows_out = delivered;
            return PRDB_SDK_OPTIMIZER_PLAN_ROW_CALLBACK_ERROR;
        }
        ++delivered;
        ++state->delivered_rows;
        if (state->has_limit != 0 &&
            state->delivered_rows >= state->limit) {
            state->done = 1;
            break;
        }
    }
    *delivered_rows_out = delivered;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_consume_scan_batch_v1(
    prdb_sdk_optimizer_scan_state_v1 *state,
    const prdb_sdk_batch_v1 *batch, uint32_t relation_column_count,
    prdb_sdk_value_v1 *source_cells, uint32_t source_cell_capacity,
    prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out) {
    return consume_scan_batch_internal(
        state, batch, relation_column_count, source_cells,
        source_cell_capacity, projected_values, projected_value_capacity,
        NULL, NULL, on_row, row_context, delivered_rows_out);
}

int32_t prdb_sdk_optimizer_consume_plan_batch_v1(
    const uint8_t *plan_payload, uint64_t plan_payload_len,
    prdb_sdk_optimizer_scan_state_v1 *state,
    const prdb_sdk_batch_v1 *batch, uint32_t relation_column_count,
    prdb_sdk_value_v1 *source_cells, uint32_t source_cell_capacity,
    prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out) {
    prdb_sdk_optimizer_plan_v1 plan = {0};
    prdb_sdk_optimizer_filter_v1 filter = {0};
    int32_t status = prdb_sdk_optimizer_decode_plan_with_filter_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan), &filter,
        sizeof(filter));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    if (state == NULL || state->struct_size < sizeof(*state) ||
        state->abi_version != PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION ||
        state->projection_count != plan.projection_count ||
        state->has_limit != plan.has_limit ||
        (state->has_limit != 0 && state->limit != plan.limit) ||
        memcmp(state->projections, plan.projections,
               plan.projection_count * sizeof(plan.projections[0])) != 0) {
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    return consume_scan_batch_internal(
        state, batch, relation_column_count, source_cells,
        source_cell_capacity, projected_values, projected_value_capacity,
        &plan, &filter, on_row, row_context, delivered_rows_out);
}

int32_t prdb_sdk_optimizer_execute_unfiltered_plan_v1(
    const prdb_host_api_v2 *host, const uint8_t *plan_payload,
    uint64_t plan_payload_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *relation_response,
    uint64_t relation_response_capacity, uint8_t *scan_response,
    uint64_t scan_response_capacity, uint8_t *batch_response,
    uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v2 *relation_columns,
    uint32_t relation_column_capacity, prdb_sdk_value_v1 *source_cells,
    uint32_t source_cell_capacity, prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out) {
    prdb_sdk_optimizer_scan_state_v1 state = {0};
    prdb_sdk_optimizer_plan_v1 plan = {0};
    prdb_sdk_optimizer_filter_v1 filter = {0};
    point_probe_hint point_probe = {0};
    range_probe_hint range_probe = {0};
    prdb_sdk_batch_v1 batch = {0};
    uint8_t cursor[16] = {0};
    uint64_t relation_response_len = 0;
    uint64_t scan_response_len = 0;
    uint64_t batch_response_len = 0;
    uint32_t total_delivered = 0;
    uint32_t scan_opened = 0;
    uint8_t empty_view_hint = 0;
    int use_full_scan = 1;
    uint32_t max_scan_pages = PRDB_SDK_OPTIMIZER_PLAN_MAX_NEXT_REQUESTS;
    int cursor_open = 0;
    int32_t final_status = PRDB_SDK_OPTIMIZER_PLAN_OK;
    int32_t status;

    if (relation_response == NULL || scan_response == NULL ||
        batch_response == NULL || delivered_rows_out == NULL ||
        on_row == NULL || max_candidates == 0 ||
        max_candidates > PRDB_SDK_BATCH_MAX_ROWS || max_batch_bytes < 8 ||
        max_batch_bytes > PRDB_SDK_BATCH_MAX_BYTES ||
        batch_response_capacity < max_batch_bytes ||
        ranges_overlap(relation_response, relation_response_capacity,
                       scan_response, scan_response_capacity) ||
        ranges_overlap(relation_response, relation_response_capacity,
                       batch_response, batch_response_capacity) ||
        ranges_overlap(scan_response, scan_response_capacity,
                       batch_response, batch_response_capacity)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    *delivered_rows_out = 0;
    status = decode_select_plan_with_access_path_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan), &filter,
        sizeof(filter), &point_probe, &range_probe, &empty_view_hint);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    status = prdb_sdk_optimizer_scan_state_init_v1(
        plan_payload, plan_payload_len, &state, sizeof(state));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    status = prdb_sdk_optimizer_acquire_plan_relation_v1(
        host, plan_payload, plan_payload_len, relation_response,
        relation_response_capacity, &relation_response_len, relation_out,
        relation_out_capacity, relation_columns, relation_column_capacity);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    if (empty_view_hint != 0) {
        int32_t cancelled = 0;
        status = prdb_sdk_executor_check_cancelled_v1(host, &cancelled);
        if (status != PRDB_SDK_EXECUTOR_OK)
            return PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
        if (cancelled != 0) return PRDB_SDK_OPTIMIZER_PLAN_CANCELLED;
        return PRDB_SDK_OPTIMIZER_PLAN_OK;
    }
    if (plan.has_limit != 0 && plan.limit == 0) return PRDB_SDK_OPTIMIZER_PLAN_OK;

    if (range_probe.present != 0) {
        uint8_t request[38 + 2 * (5 + 4096) +
                        PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS * 16];
        uint8_t column_ids[PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS * 16];
        uint8_t lower_integer_bytes[4];
        uint8_t upper_integer_bytes[4];
        prdb_sdk_value_v1 lower_value = {0};
        prdb_sdk_value_v1 upper_value = {0};
        const prdb_sdk_value_v1 *lower_bound = NULL;
        const prdb_sdk_value_v1 *upper_bound = NULL;
        prdb_sdk_index_range_v1 range = {0};
        uint64_t range_response_capacity;
        uint64_t request_len = 0;
        int32_t cancelled = 0;
        uint32_t delivered = 0;
        int32_t executor_status;

        if (!relation_response_column_ids(
                relation_response, relation_response_len,
                relation_out->column_count, column_ids, sizeof(column_ids))) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        if (range_probe.lower_literal != NULL) {
            if (!range_literal_to_value(
                    range_probe.lower_literal, range_probe.lower_literal_len,
                    range_probe.key_column.type_code, lower_integer_bytes,
                    &lower_value)) {
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            }
            lower_bound = &lower_value;
        }
        if (range_probe.upper_literal != NULL) {
            if (!range_literal_to_value(
                    range_probe.upper_literal, range_probe.upper_literal_len,
                    range_probe.key_column.type_code, upper_integer_bytes,
                    &upper_value)) {
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            }
            upper_bound = &upper_value;
        }
        range_response_capacity = batch_response_capacity;
        if (range_response_capacity > max_batch_bytes)
            range_response_capacity = max_batch_bytes;
        executor_status = prdb_sdk_executor_scan_index_range_v1(
            host, relation_out->executor_handle, range_probe.index_id,
            lower_bound, range_probe.lower_inclusive, upper_bound,
            range_probe.upper_inclusive, column_ids,
            (uint16_t)relation_out->column_count, request, sizeof(request),
            batch_response, range_response_capacity, &batch_response_len,
            &range, sizeof(range));
        if (executor_status == PRDB_SDK_EXECUTOR_UNAVAILABLE ||
            executor_status == PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL) {
            use_full_scan = 1;
            max_scan_pages--;
        } else if (executor_status != PRDB_SDK_EXECUTOR_OK) {
            return executor_status == PRDB_SDK_EXECUTOR_INVALID_PAYLOAD
                       ? PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD
                       : PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
        } else {
            status = prdb_sdk_executor_check_cancelled_v1(host, &cancelled);
            if (status != PRDB_SDK_EXECUTOR_OK)
                return PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
            if (cancelled != 0) return PRDB_SDK_OPTIMIZER_PLAN_CANCELLED;
            if (!range_response_to_internal_batch(
                    batch_response, &batch_response_len,
                    batch_response_capacity, max_batch_bytes,
                    max_candidates)) {
                use_full_scan = 1;
                max_scan_pages--;
            } else {
                status = prdb_sdk_batch_open_v1(
                    &batch, sizeof(batch), batch_response,
                    batch_response_len);
                if (status != PRDB_SDK_BATCH_OK || batch.finished == 0) {
                    return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
                }
                status = consume_scan_batch_internal(
                    &state, &batch, relation_out->column_count,
                    source_cells, source_cell_capacity, projected_values,
                    projected_value_capacity, &plan, &filter, on_row,
                    row_context, &delivered);
                if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
                *delivered_rows_out = delivered;
                return PRDB_SDK_OPTIMIZER_PLAN_OK;
            }
        }
    }

    if (point_probe.present != 0) {
        uint8_t request[38 + 5 + 4096 +
                        PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS * 16];
        uint64_t request_len = 0;
        uint64_t probe_response_capacity;
        int32_t cancelled = 0;
        int32_t callback_status;
        uint8_t probe_version;
        uint8_t probe_status;
        if (!encode_point_probe_request(
                &plan, relation_out, &point_probe, relation_response,
                relation_response_len, request, sizeof(request),
                &request_len)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        probe_response_capacity = max_batch_bytes - 3;
        if (probe_response_capacity > batch_response_capacity) {
            probe_response_capacity = batch_response_capacity;
        }
        callback_status = host->executor_request(
            host->context, PRDB_SDK_EXECUTOR_OP_PROBE_UNIQUE_INDEX, request,
            request_len, batch_response, probe_response_capacity,
            &batch_response_len);
        if (callback_status == PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL) {
            use_full_scan = 1;
            max_scan_pages--;
        } else if (callback_status != PRDB_QUERY_CALLBACK_OK ||
                   batch_response_len > probe_response_capacity) {
            return PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
        } else if (!prdb_decode_executor_probe_unique_index_response_match_record(
                       batch_response, (size_t)batch_response_len,
                       &probe_version, &probe_status) ||
                   probe_version != PRDB_PROBE_UNIQUE_INDEX_RESPONSE_VERSION) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        } else if (probe_status == 0 &&
                   prdb_decode_executor_probe_unique_index_response_unavailable_record(
                       batch_response, (size_t)batch_response_len,
                       &probe_version, &probe_status)) {
            use_full_scan = 1;
            max_scan_pages--;
        } else if (probe_status == 1 &&
                   prdb_decode_executor_probe_unique_index_response_no_match_record(
                       batch_response, (size_t)batch_response_len,
                       &probe_version, &probe_status)) {
            status = prdb_sdk_executor_check_cancelled_v1(host, &cancelled);
            if (status != PRDB_SDK_EXECUTOR_OK) {
                return PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
            }
            if (cancelled != 0) return PRDB_SDK_OPTIMIZER_PLAN_CANCELLED;
            return PRDB_SDK_OPTIMIZER_PLAN_OK;
        } else if (probe_status == 2 && batch_response_len >=
                   PRDB_EXECUTOR_PROBE_UNIQUE_INDEX_RESPONSE_MATCH_RECORD_HEADER_BYTES +
                       PRDB_EXECUTOR_BATCH_ROW_RECORD_HEADER_BYTES) {
            uint32_t delivered = 0;
            status = prdb_sdk_executor_check_cancelled_v1(host, &cancelled);
            if (status != PRDB_SDK_EXECUTOR_OK) {
                return PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
            }
            if (cancelled != 0) return PRDB_SDK_OPTIMIZER_PLAN_CANCELLED;
            if (batch_response_len + 3 > max_batch_bytes ||
                batch_response_len + 3 > batch_response_capacity) {
                use_full_scan = 1;
                max_scan_pages--;
            } else {
                memmove(batch_response + 5,
                        batch_response +
                            PRDB_EXECUTOR_PROBE_UNIQUE_INDEX_RESPONSE_MATCH_RECORD_HEADER_BYTES,
                        (size_t)(batch_response_len -
                                 PRDB_EXECUTOR_PROBE_UNIQUE_INDEX_RESPONSE_MATCH_RECORD_HEADER_BYTES));
                batch_response[0] = 0;
                batch_response[1] = 1;
                batch_response[2] = 1;
                batch_response[3] = 0;
                batch_response[4] = 1;
                batch_response_len +=
                    5 - PRDB_EXECUTOR_PROBE_UNIQUE_INDEX_RESPONSE_MATCH_RECORD_HEADER_BYTES;
                status = prdb_sdk_batch_open_v1(
                    &batch, sizeof(batch), batch_response,
                    batch_response_len);
                if (status != PRDB_SDK_BATCH_OK || batch.finished == 0 ||
                    batch.row_count != 1) {
                    return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
                }
                status = consume_scan_batch_internal(
                    &state, &batch, relation_out->column_count,
                    source_cells, source_cell_capacity, projected_values,
                    projected_value_capacity, &plan, &filter, on_row,
                    row_context, &delivered);
                if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
                *delivered_rows_out = delivered;
                return PRDB_SDK_OPTIMIZER_PLAN_OK;
            }
        } else {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
    }
    if (use_full_scan != 0) {
        status = prdb_sdk_executor_open_full_scan_v1(
            host, relation_out->executor_handle, scan_response,
            scan_response_capacity, &scan_response_len, cursor);
        if (status == PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL) {
            return PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL;
        }
        if (status != PRDB_SDK_EXECUTOR_OK) {
            return PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
        }
        scan_opened = 1;
    }
    if (scan_opened == 0) return PRDB_SDK_OPTIMIZER_PLAN_OK;
    cursor_open = 1;

    for (uint32_t page = 0; page < max_scan_pages; ++page) {
        int32_t cancelled = 0;
        uint32_t page_rows = 0;
        status = prdb_sdk_executor_check_cancelled_v1(host, &cancelled);
        if (status != PRDB_SDK_EXECUTOR_OK) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
            break;
        }
        if (cancelled != 0) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_CANCELLED;
            break;
        }
        status = prdb_sdk_executor_next_batch_v1(
            host, cursor, max_candidates, max_batch_bytes, batch_response,
            batch_response_capacity, &batch_response_len, &batch,
            sizeof(batch));
        if (status == PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL;
            break;
        }
        if (status != PRDB_SDK_EXECUTOR_OK) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
            break;
        }
        if (batch.finished != 0) {
            cursor_open = 0;
        } else {
            memcpy(cursor, batch.continuation, sizeof(cursor));
        }
        status = prdb_sdk_optimizer_consume_plan_batch_v1(
            plan_payload, plan_payload_len, &state, &batch,
            relation_out->column_count, source_cells,
            source_cell_capacity, projected_values,
            projected_value_capacity, on_row, row_context, &page_rows);
        if (UINT32_MAX - total_delivered < page_rows) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            break;
        }
        total_delivered += page_rows;
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) {
            final_status = status;
            break;
        }
        if (state.done != 0 || batch.finished != 0) break;
        if (page + 1 == max_scan_pages) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_REQUEST_LIMIT;
        }
    }

    if (cursor_open != 0) {
        status = prdb_sdk_executor_close_cursor_v1(
            host, cursor, scan_response, scan_response_capacity,
            &scan_response_len);
        if (status != PRDB_SDK_EXECUTOR_OK &&
            final_status == PRDB_SDK_OPTIMIZER_PLAN_OK) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
        }
    }
    *delivered_rows_out = total_delivered;
    return final_status;
}

static int32_t latch_optimizer_statement_failure(
    const prdb_host_api_v4 *host, int32_t status) {
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK &&
        status != PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT &&
        host != NULL && host->v3.statement_failure != NULL) {
        (void)host->v3.statement_failure(
            host->v3.context, PRDB_QUERY_STATEMENT_FAILURE_SQL);
    }
    return status;
}

static int identifier_equal_ascii_casefold(const uint8_t *left,
                                           const uint8_t *right,
                                           uint32_t length) {
    for (uint32_t index = 0; index < length; ++index) {
        uint8_t left_byte = left[index];
        uint8_t right_byte = right[index];
        if (left_byte >= 'A' && left_byte <= 'Z')
            left_byte = (uint8_t)(left_byte + ('a' - 'A'));
        if (right_byte >= 'A' && right_byte <= 'Z')
            right_byte = (uint8_t)(right_byte + ('a' - 'A'));
        if (left_byte != right_byte) return 0;
    }
    return 1;
}

static int response_column_name(const uint8_t *response,
                                uint64_t response_len, uint32_t target_ordinal,
                                const uint8_t **name_out,
                                uint32_t *name_len_out) {
    uint64_t offset = 51;
    uint32_t relation_name_len;
    uint32_t column_count;
    if (response == NULL || name_out == NULL || name_len_out == NULL ||
        response_len < offset + 2) return 0;
    relation_name_len = response_u16(response, offset);
    offset += 2;
    if (relation_name_len > response_len - offset) return 0;
    offset += relation_name_len;
    if (response_len - offset < 2) return 0;
    column_count = response_u16(response, offset);
    offset += 2;
    for (uint32_t ordinal = 0; ordinal < column_count; ++ordinal) {
        uint32_t column_name_len;
        if (response_len - offset < 28) return 0;
        offset += 26;
        column_name_len = response_u16(response, offset);
        offset += 2;
        if (column_name_len > response_len - offset) return 0;
        if (ordinal == target_ordinal) {
            *name_out = response + offset;
            *name_len_out = column_name_len;
            return 1;
        }
        offset += column_name_len;
    }
    return 0;
}

static int match_optimizer_parameter(plan_reader *reader,
                                     const prdb_sdk_value_v1 *value) {
    uint8_t tag;
    uint64_t tag_offset = reader->offset;
    const uint8_t *bytes;
    if (value == NULL || prdb_sdk_value_validate_v1(value) !=
                             PRDB_SDK_VALUE_VALID ||
        !read_u8(reader, &tag)) return 0;
    if (value->state == PRDB_SDK_VALUE_STATE_NULL)
        return tag == 0x00 && read_null_predicate_tag(reader, tag_offset);
    if (value->state != PRDB_SDK_VALUE_STATE_VALUE) return 0;
    switch (value->type_code) {
    case PRDB_SDK_VALUE_TYPE_BOOLEAN:
        {
            uint8_t boolean_value;
            return tag == 0x01 &&
                   read_boolean_literal_predicate_record(
                       reader, tag_offset, &boolean_value) &&
                   boolean_value == value->data[0];
        }
    case PRDB_SDK_VALUE_TYPE_INTEGER: {
        int64_t integer_value;
        int32_t expected;
        if (prdb_sdk_value_get_integer_v1(value, &expected) !=
            PRDB_SDK_VALUE_ACCESS_OK) return 0;
        if (tag != 0x02 ||
            !read_integer_predicate_record(reader, tag_offset,
                                           &integer_value)) return 0;
        return integer_value == expected;
    }
    case PRDB_SDK_VALUE_TYPE_TEXT: {
        uint16_t length;
        if (tag != 0x03 ||
            !read_text_predicate_header(reader, tag_offset, &length) ||
            length != value->data_len || length > 4096 ||
            !take(reader, length, &bytes)) return 0;
        return length == 0 || memcmp(bytes, value->data, length) == 0;
    }
    default:
        return 0;
    }
}

static int match_optimizer_filter_node(
    plan_reader *reader, const prdb_sdk_optimizer_plan_v1 *plan,
    const uint8_t *metadata_response, uint64_t metadata_response_len,
    const prdb_sdk_select_v2 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint32_t node_index, uint8_t *seen, uint32_t depth) {
    const prdb_sdk_predicate_node_v2 *node;
    uint8_t tag;
    uint64_t tag_offset = reader->offset;
    if (depth > PRDB_SDK_SQL_MAX_EXPRESSION_DEPTH ||
        node_index >= statement->predicate_count || seen[node_index] != 0 ||
        !read_u8(reader, &tag)) return 0;
    seen[node_index] = 1;
    node = &statement->predicates[node_index];
    switch (node->node_kind) {
    case PRDB_SDK_SQL_PREDICATE_COMPARE: {
        uint8_t comparison;
        uint8_t column_tag;
        uint8_t column_id[16];
        const uint8_t *column_name = NULL;
        uint32_t column_name_len = 0;
        uint32_t ordinal = UINT32_MAX;
        if (tag != 0x20 || node->comparison < PRDB_SDK_SQL_COMPARE_EQUAL ||
            node->comparison > PRDB_SDK_SQL_COMPARE_GREATER_EQUAL ||
            node->column.offset > statement->sql_len ||
            node->column.length == 0 ||
            node->column.length > statement->sql_len - node->column.offset ||
            !valid_identifier(statement->sql + node->column.offset,
                              node->column.length) ||
            !read_compare_predicate_header(reader, tag_offset, &comparison) ||
            comparison != node->comparison - PRDB_SDK_SQL_COMPARE_EQUAL ||
            !read_u8(reader, &column_tag) || column_tag != 0x10 ||
            !read_column_predicate_record(reader, reader->offset - 1,
                                          column_id))
            return 0;
        for (uint32_t index = 0; index < plan->scan_column_count; ++index) {
            if (memcmp(column_id, plan->scan_columns[index].column_id, 16) == 0) {
                ordinal = plan->scan_columns[index].ordinal;
                break;
            }
        }
        if (ordinal == UINT32_MAX ||
            !response_column_name(metadata_response, metadata_response_len,
                                  ordinal, &column_name, &column_name_len) ||
            column_name_len != node->column.length ||
            !identifier_equal_ascii_casefold(
                column_name, statement->sql + node->column.offset,
                node->column.length) ||
            node->parameter_index == 0 ||
            node->parameter_index > parameter_count) return 0;
        return match_optimizer_parameter(
            reader, &parameters[node->parameter_index - 1]);
    }
    case PRDB_SDK_SQL_PREDICATE_AND:
    case PRDB_SDK_SQL_PREDICATE_OR: {
        uint8_t node_tag;
        uint8_t child_count;
        uint8_t expected_tag = node->node_kind == PRDB_SDK_SQL_PREDICATE_AND
                                   ? 0x30
                                   : 0x31;
        return tag == expected_tag &&
               read_boolean_predicate_header(reader, tag_offset, &node_tag,
                                             &child_count) &&
               node_tag == expected_tag &&
               child_count == 2 &&
               match_optimizer_filter_node(
                   reader, plan, metadata_response, metadata_response_len,
                   statement, parameters, parameter_count, node->left_child,
                   seen, depth + 1) &&
               match_optimizer_filter_node(
                   reader, plan, metadata_response, metadata_response_len,
                   statement, parameters, parameter_count, node->right_child,
                   seen, depth + 1);
    }
    case PRDB_SDK_SQL_PREDICATE_NOT:
        return tag == 0x32 && read_not_predicate_tag(reader, tag_offset) &&
               match_optimizer_filter_node(
                   reader, plan, metadata_response, metadata_response_len,
                   statement, parameters, parameter_count, node->left_child,
                   seen, depth + 1);
    case PRDB_SDK_SQL_PREDICATE_IS_NULL:
    case PRDB_SDK_SQL_PREDICATE_IS_NOT_NULL: {
        uint8_t column_tag;
        uint8_t column_id[16];
        const uint8_t *column_name = NULL;
        uint32_t column_name_len = 0;
        uint32_t ordinal = UINT32_MAX;
        uint8_t expected_tag =
            node->node_kind == PRDB_SDK_SQL_PREDICATE_IS_NULL ? 0x40 : 0x41;
        if (tag != expected_tag || node->left_child != 0 ||
            node->right_child != 0 || node->parameter_index != 0 ||
            node->comparison != 0 ||
            node->column.offset > statement->sql_len ||
            node->column.length == 0 ||
            node->column.length > statement->sql_len - node->column.offset ||
            !valid_identifier(statement->sql + node->column.offset,
                              node->column.length) ||
            !read_u8(reader, &column_tag) || column_tag != 0x10 ||
            !read_column_predicate_record(reader, reader->offset - 1,
                                          column_id))
            return 0;
        for (uint32_t index = 0; index < plan->scan_column_count; ++index) {
            if (memcmp(column_id, plan->scan_columns[index].column_id, 16) == 0) {
                ordinal = plan->scan_columns[index].ordinal;
                break;
            }
        }
        return ordinal != UINT32_MAX &&
               response_column_name(metadata_response, metadata_response_len,
                                    ordinal, &column_name, &column_name_len) &&
               column_name_len == node->column.length &&
               identifier_equal_ascii_casefold(
                   column_name, statement->sql + node->column.offset,
                   node->column.length);
    }
    default:
        return 0;
    }
}

static int32_t validate_select_plan_internal(
    const prdb_host_api_v2 *host, const prdb_sdk_select_v2 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    int allow_filter,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len,
    uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len) {
    prdb_sdk_optimizer_plan_v1 plan = {0};
    prdb_sdk_optimizer_filter_v1 filter = {0};
    uint64_t offset;
    uint32_t relation_name_len;
    uint32_t column_count;
    int32_t status;

    if (statement == NULL || statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V2_VERSION ||
        statement->sql == NULL || statement->sql_len == 0 || schema == NULL ||
        parameter_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        (parameter_count != 0 && parameters == NULL) ||
        statement->predicate_count > PRDB_SDK_SQL_MAX_PREDICATE_NODES ||
        !valid_identifier(schema, schema_len) || statement->projection_star != 0 ||
        (!allow_filter && statement->predicate_count != 0) ||
        statement->projection_count == 0 ||
        statement->projection_count > PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS ||
        statement->has_limit > 1 ||
        statement->relation.offset > statement->sql_len ||
        statement->relation.length == 0 ||
        statement->relation.length > statement->sql_len - statement->relation.offset ||
        !valid_identifier(statement->sql + statement->relation.offset,
                          statement->relation.length)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    if ((statement->predicate_count == 0 && statement->predicate_root != 0) ||
        (statement->predicate_count != 0 &&
         statement->predicate_root >= statement->predicate_count)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    for (uint32_t index = 0; index < statement->projection_count; ++index) {
        prdb_sdk_sql_span_v1 projection = statement->projection[index];
        if (projection.offset > statement->sql_len || projection.length == 0 ||
            projection.length > statement->sql_len - projection.offset ||
            !valid_identifier(statement->sql + projection.offset,
                              projection.length)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
        }
    }
    status = prdb_sdk_optimizer_decode_plan_with_filter_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan), &filter,
        sizeof(filter));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    if ((filter.present != 0) != (statement->predicate_count != 0) ||
        plan.schema_name_len != schema_len ||
        memcmp(plan.schema_name, schema, schema_len) != 0 ||
        plan.relation_name_len != statement->relation.length ||
        !identifier_equal_ascii_casefold(
            plan.relation_name, statement->sql + statement->relation.offset,
            statement->relation.length) ||
        plan.projection_count != statement->projection_count ||
        plan.has_limit != statement->has_limit ||
        (plan.has_limit != 0 && plan.limit != statement->limit)) {
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    status = prdb_sdk_optimizer_validate_executor_relation_v1(
        host, plan_payload, plan_payload_len, response, response_capacity,
        response_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;

    offset = 51;
    relation_name_len = response_u16(response, offset);
    offset += 2 + relation_name_len;
    column_count = response_u16(response, offset);
    offset += 2;
    for (uint32_t ordinal = 0; ordinal < column_count; ++ordinal) {
        uint32_t column_name_len;
        offset += 26;
        column_name_len = response_u16(response, offset);
        offset += 2;
        for (uint32_t projection_index = 0;
             projection_index < plan.projection_count; ++projection_index) {
            const prdb_sdk_optimizer_plan_column_v1 *projection =
                &plan.projections[projection_index];
            prdb_sdk_sql_span_v1 requested =
                statement->projection[projection_index];
            if (projection->ordinal == ordinal &&
                (column_name_len != requested.length ||
                 !identifier_equal_ascii_casefold(
                     response + offset, statement->sql + requested.offset,
                     requested.length))) {
                return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
            }
        }
        offset += column_name_len;
    }
    if (filter.present != 0) {
        plan_reader filter_reader = {filter.prefix, filter.prefix_len, 0};
        uint8_t seen[PRDB_SDK_SQL_MAX_PREDICATE_NODES] = {0};
        if (parameters == NULL ||
            !match_optimizer_filter_node(
                &filter_reader, &plan, response, *response_len, statement,
                parameters, parameter_count, statement->predicate_root, seen,
                1) ||
            filter_reader.offset != filter_reader.length) {
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        }
        for (uint32_t index = 0; index < statement->predicate_count; ++index)
            if (seen[index] == 0)
                return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_validate_select_plan_v1(
    const prdb_host_api_v2 *host, const prdb_sdk_select_v2 *statement,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len,
    uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len) {
    return validate_select_plan_internal(
        host, statement, NULL, 0, 0, schema, schema_len, plan_payload,
        plan_payload_len, response, response_capacity, response_len);
}

int32_t prdb_sdk_optimizer_validate_select_plan_with_parameters_v1(
    const prdb_host_api_v2 *host, const prdb_sdk_select_v2 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len,
    uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len) {
    return validate_select_plan_internal(
        host, statement, parameters, parameter_count, 1, schema, schema_len,
        plan_payload, plan_payload_len, response, response_capacity,
        response_len);
}

int32_t prdb_sdk_optimizer_validate_delete_plan_with_parameters_v1(
    const prdb_host_api_v2 *host, const prdb_sdk_delete_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len,
    uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len) {
    const prdb_sdk_select_v2 *predicate;
    prdb_sdk_optimizer_plan_v1 plan = {0};
    prdb_sdk_optimizer_filter_v1 filter = {0};
    plan_reader filter_reader;
    uint8_t seen[PRDB_SDK_SQL_MAX_PREDICATE_NODES] = {0};
    int32_t status;

    if (statement == NULL || statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_DML_ABI_V1_VERSION ||
        statement->sql == NULL || statement->sql_len == 0 ||
        statement->sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES || schema == NULL ||
        !valid_identifier(schema, schema_len) ||
        statement->relation.offset > statement->sql_len ||
        statement->relation.length == 0 ||
        statement->relation.length > statement->sql_len -
                                         statement->relation.offset ||
        !valid_identifier(statement->sql + statement->relation.offset,
                          statement->relation.length) ||
        parameter_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        (parameter_count != 0 && parameters == NULL)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    predicate = &statement->predicate_statement;
    if (predicate->struct_size != sizeof(*predicate) ||
        predicate->abi_version != PRDB_SDK_SQL_ABI_V2_VERSION ||
        predicate->sql != statement->sql ||
        predicate->sql_len != statement->sql_len ||
        predicate->relation.offset != statement->relation.offset ||
        predicate->relation.length != statement->relation.length ||
        predicate->projection_star != 1 || predicate->projection_count != 0 ||
        predicate->has_limit != 0 || predicate->limit != 0 ||
        predicate->predicate_count == 0 ||
        predicate->predicate_count > PRDB_SDK_SQL_MAX_PREDICATE_NODES ||
        predicate->predicate_root >= predicate->predicate_count) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    status = prdb_sdk_optimizer_decode_delete_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan), &filter,
        sizeof(filter));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    if (filter.present == 0 || plan.schema_name_len != schema_len ||
        memcmp(plan.schema_name, schema, schema_len) != 0 ||
        plan.relation_name_len != statement->relation.length ||
        !identifier_equal_ascii_casefold(
            plan.relation_name,
            statement->sql + statement->relation.offset,
            statement->relation.length)) {
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    status = prdb_sdk_optimizer_validate_executor_relation_v1(
        host, plan_payload, plan_payload_len, response, response_capacity,
        response_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;

    filter_reader = (plan_reader){filter.prefix, filter.prefix_len, 0};
    if (!match_optimizer_filter_node(
            &filter_reader, &plan, response, *response_len, predicate,
            parameters, parameter_count, predicate->predicate_root, seen, 1) ||
        filter_reader.offset != filter_reader.length) {
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    for (uint32_t index = 0; index < predicate->predicate_count; ++index) {
        if (seen[index] == 0)
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_validate_update_plan_with_parameters_v1(
    const prdb_host_api_v2 *host, const prdb_sdk_update_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len,
    uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len) {
    const prdb_sdk_select_v2 *predicate;
    prdb_sdk_optimizer_update_plan_v1 update = {0};
    plan_reader filter_reader;
    uint8_t seen[PRDB_SDK_SQL_MAX_PREDICATE_NODES] = {0};
    int32_t status;

    if (statement == NULL || statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_DML_ABI_V1_VERSION ||
        statement->sql == NULL || statement->sql_len == 0 ||
        statement->sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES || schema == NULL ||
        !valid_identifier(schema, schema_len) ||
        statement->relation.offset > statement->sql_len ||
        statement->relation.length == 0 ||
        statement->relation.length > statement->sql_len -
                                         statement->relation.offset ||
        !valid_identifier(statement->sql + statement->relation.offset,
                          statement->relation.length) ||
        statement->assignment_count == 0 ||
        statement->assignment_count > PRDB_SDK_SQL_MAX_UPDATE_ASSIGNMENTS ||
        parameter_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        (parameter_count != 0 && parameters == NULL)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    predicate = &statement->predicate_statement;
    if (predicate->struct_size != sizeof(*predicate) ||
        predicate->abi_version != PRDB_SDK_SQL_ABI_V2_VERSION ||
        predicate->sql != statement->sql ||
        predicate->sql_len != statement->sql_len ||
        predicate->relation.offset != statement->relation.offset ||
        predicate->relation.length != statement->relation.length ||
        predicate->projection_star != 0 ||
        predicate->projection_count != statement->assignment_count ||
        predicate->has_limit != 0 || predicate->limit != 0 ||
        predicate->predicate_count == 0 ||
        predicate->predicate_count > PRDB_SDK_SQL_MAX_PREDICATE_NODES ||
        predicate->predicate_root >= predicate->predicate_count) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    for (uint32_t index = 0; index < statement->assignment_count; ++index) {
        const prdb_sdk_sql_update_assignment_v1 *assignment =
            &statement->assignments[index];
        if (assignment->column.offset > statement->sql_len ||
            assignment->column.length == 0 ||
            assignment->column.length >
                statement->sql_len - assignment->column.offset ||
            !valid_identifier(statement->sql + assignment->column.offset,
                              assignment->column.length) ||
            predicate->projection[index].offset != assignment->column.offset ||
            predicate->projection[index].length != assignment->column.length) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
        }
    }
    status = prdb_sdk_optimizer_decode_update_plan_v1(
        plan_payload, plan_payload_len, &update, sizeof(update));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    if (update.filter.present == 0 ||
        update.assignment_count != statement->assignment_count ||
        update.plan.schema_name_len != schema_len ||
        memcmp(update.plan.schema_name, schema, schema_len) != 0 ||
        update.plan.relation_name_len != statement->relation.length ||
        !identifier_equal_ascii_casefold(
            update.plan.relation_name,
            statement->sql + statement->relation.offset,
            statement->relation.length)) {
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }

    status = prdb_sdk_optimizer_validate_executor_relation_v1(
        host, plan_payload, plan_payload_len, response, response_capacity,
        response_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;

    filter_reader = (plan_reader){update.filter.prefix,
                                  update.filter.prefix_len, 0};
    if (!match_optimizer_filter_node(
            &filter_reader, &update.plan, response, *response_len, predicate,
            parameters, parameter_count, predicate->predicate_root, seen, 1) ||
        filter_reader.offset != filter_reader.length) {
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    for (uint32_t index = 0; index < predicate->predicate_count; ++index) {
        if (seen[index] == 0)
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }

    for (uint32_t index = 0; index < statement->assignment_count; ++index) {
        const prdb_sdk_sql_update_assignment_v1 *requested =
            &statement->assignments[index];
        const prdb_sdk_optimizer_assignment_v1 *planned =
            &update.assignments[index];
        const uint8_t *column_name = NULL;
        uint32_t column_name_len = 0;
        if (!response_column_name(response, *response_len,
                                  planned->column.ordinal, &column_name,
                                  &column_name_len) ||
            column_name_len != requested->column.length ||
            !identifier_equal_ascii_casefold(
                column_name, statement->sql + requested->column.offset,
                requested->column.length)) {
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        }
        if (requested->kind == PRDB_SDK_SQL_UPDATE_PARAMETER) {
            const prdb_sdk_value_v1 *value;
            if (requested->parameter_index == 0 ||
                requested->parameter_index > parameter_count ||
                planned->value_kind !=
                    PRDB_OPTIMIZER_UPDATE_PARAMETER_VALUE ||
                planned->parameter_index != requested->parameter_index) {
                return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
            }
            value = &parameters[requested->parameter_index - 1];
            if (prdb_sdk_value_validate_v1(value) != PRDB_SDK_VALUE_VALID ||
                value->state != PRDB_SDK_VALUE_STATE_VALUE ||
                value->type_code != planned->column.type_code) {
                return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
            }
        } else if (requested->kind == PRDB_SDK_SQL_UPDATE_NULL) {
            if (requested->parameter_index != 0 ||
                planned->value_kind != PRDB_OPTIMIZER_UPDATE_NULL_VALUE ||
                planned->parameter_index != 0 ||
                planned->column.nullable == 0) {
                return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
            }
        } else {
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        }
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_execute_delete_ast_v1(
    const prdb_host_api_v5 *host, const prdb_sdk_delete_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v2 *relation_columns,
    uint32_t relation_column_capacity,
    prdb_sdk_select_binding_v2 *binding, uint32_t binding_capacity,
    prdb_sdk_value_v1 *cell_scratch, uint32_t cell_scratch_capacity,
    uint8_t *target_handles, uint32_t target_capacity,
    uint32_t *deleted_count_out) {
    prdb_host_api_v2 base_host = {0};
    prdb_sdk_optimizer_response_v1 decoded_response = {0};
    prdb_sdk_batch_v1 batch = {0};
    prdb_sdk_row_iterator_v1 rows = {0};
    prdb_sdk_row_v1 row = {0};
    uint8_t cursor[16] = {0};
    uint64_t request_len = 0;
    uint64_t optimizer_response_len = 0;
    uint64_t relation_response_len = 0;
    uint64_t scan_response_len = 0;
    uint64_t batch_response_len = 0;
    uint32_t target_count = 0;
    uint32_t page_count = 0;
    uint32_t close_count = 0;
    uint32_t scan_opened = 0;
    int cursor_open = 0;
    int32_t final_status = PRDB_SDK_OPTIMIZER_PLAN_OK;
    int32_t status;
    const prdb_sdk_select_v2 *predicate;

    if (host == NULL ||
        host->v4.v3.struct_size < sizeof(prdb_host_api_v3) ||
        host->v4.v3.abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->v4.v3.context == NULL ||
        host->v4.v3.executor_request == NULL ||
        host->v4.v3.statement_failure == NULL ||
        host->v4.optimizer_request == NULL || host->optimizer_request == NULL ||
        request == NULL || optimizer_response == NULL ||
        relation_response == NULL || scan_response == NULL ||
        batch_response == NULL || relation_out == NULL ||
        relation_columns == NULL || binding == NULL || cell_scratch == NULL ||
        target_handles == NULL || deleted_count_out == NULL ||
        relation_out_capacity < sizeof(*relation_out) ||
        binding_capacity < sizeof(*binding) ||
        parameter_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        (parameter_count != 0 && parameters == NULL) || max_candidates == 0 ||
        max_candidates > PRDB_SDK_BATCH_MAX_ROWS || max_batch_bytes < 8 ||
        max_batch_bytes > PRDB_SDK_BATCH_MAX_BYTES ||
        batch_response_capacity < max_batch_bytes || request_capacity == 0 ||
        request_capacity > PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES ||
        optimizer_response_capacity == 0 ||
        optimizer_response_capacity > PRDB_QUERY_MAX_IPC_REQUEST_BYTES ||
        relation_response_capacity == 0 || scan_response_capacity == 0 ||
        relation_column_capacity == 0 || cell_scratch_capacity == 0 ||
        target_capacity == 0 ||
        target_capacity > PRDB_SDK_OPTIMIZER_PLAN_MAX_DELETE_TARGETS ||
        ranges_overlap(request, request_capacity, optimizer_response,
                       optimizer_response_capacity) ||
        ranges_overlap(request, request_capacity, relation_response,
                       relation_response_capacity) ||
        ranges_overlap(optimizer_response, optimizer_response_capacity,
                       relation_response, relation_response_capacity) ||
        ranges_overlap(relation_response, relation_response_capacity,
                       scan_response, scan_response_capacity) ||
        ranges_overlap(relation_response, relation_response_capacity,
                       batch_response, batch_response_capacity) ||
        ranges_overlap(scan_response, scan_response_capacity, batch_response,
                       batch_response_capacity) ||
        ranges_overlap(target_handles, (uint64_t)target_capacity * 16,
                       request, request_capacity) ||
        ranges_overlap(target_handles, (uint64_t)target_capacity * 16,
                       optimizer_response, optimizer_response_capacity) ||
        ranges_overlap(target_handles, (uint64_t)target_capacity * 16,
                       relation_response, relation_response_capacity) ||
        ranges_overlap(target_handles, (uint64_t)target_capacity * 16,
                       scan_response, scan_response_capacity) ||
        ranges_overlap(target_handles, (uint64_t)target_capacity * 16,
                       batch_response, batch_response_capacity)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    *deleted_count_out = 0;
    base_host.struct_size = sizeof(base_host);
    base_host.abi_version = PRDB_QUERY_ABI_V2_VERSION;
    base_host.context = host->v4.v3.context;
    base_host.executor_request = host->v4.v3.executor_request;
    base_host.planner_request = host->v4.v3.planner_request;
    base_host.write_result = host->v4.v3.write_result;
    predicate = &statement->predicate_statement;

    status = prdb_sdk_optimizer_encode_delete_v1(
        statement, parameters, parameter_count, schema, schema_len, request,
        request_capacity, &request_len);
    if (status == PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED)
        return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
    if (status == PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL ||
        status == PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED)
        return PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL;
    if (status != PRDB_SDK_OPTIMIZER_ENCODE_OK)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;

    status = prdb_sdk_optimizer_submit_delete_v1(
        host, request, request_len, optimizer_response,
        optimizer_response_capacity, &optimizer_response_len,
        &decoded_response, sizeof(decoded_response));
    if (status == PRDB_SDK_OPTIMIZER_RESULT_RESPONSE_TOO_SMALL) {
        status = PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL;
    } else if (status != PRDB_SDK_OPTIMIZER_RESULT_OK) {
        status = status == PRDB_SDK_OPTIMIZER_RESULT_INVALID_ARGUMENT
                     ? PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT
                     : PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
    } else if (decoded_response.status ==
               PRDB_SDK_OPTIMIZER_STATUS_UNAVAILABLE) {
        status = PRDB_SDK_OPTIMIZER_PLAN_UNAVAILABLE;
    } else if (decoded_response.status !=
               PRDB_SDK_OPTIMIZER_STATUS_SUCCESS) {
        status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
    }
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK)
        return latch_optimizer_statement_failure(&host->v4, status);

    status = prdb_sdk_optimizer_validate_delete_plan_with_parameters_v1(
        &base_host, statement, parameters, parameter_count, schema, schema_len,
        decoded_response.plan, decoded_response.plan_len, relation_response,
        relation_response_capacity, &relation_response_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK)
        return latch_optimizer_statement_failure(&host->v4, status);

    status = prdb_sdk_optimizer_open_plan_scan_v1(
        &base_host, decoded_response.plan, decoded_response.plan_len,
        relation_response, relation_response_capacity, &relation_response_len,
        scan_response, scan_response_capacity, &scan_response_len,
        relation_out, relation_out_capacity, relation_columns,
        relation_column_capacity, cursor, &scan_opened);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK)
        return latch_optimizer_statement_failure(&host->v4, status);
    if (relation_out->column_count > PRDB_SDK_SQL_MAX_RELATION_COLUMNS ||
        relation_out->column_count > cell_scratch_capacity) {
        return latch_optimizer_statement_failure(
            &host->v4, PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT);
    }
    status = prdb_sdk_sql_bind_select_v2(
        predicate, relation_out->name, relation_out->name_len,
        relation_columns, relation_out->column_count, binding,
        binding_capacity);
    if (status != PRDB_SDK_SQL_BIND_OK) {
        return latch_optimizer_statement_failure(
            &host->v4, PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    }
    if (scan_opened == 0) return PRDB_SDK_OPTIMIZER_PLAN_OK;
    cursor_open = 1;

    while (page_count < PRDB_SDK_OPTIMIZER_PLAN_MAX_NEXT_REQUESTS) {
        int32_t cancelled = 0;
        status = prdb_sdk_executor_check_cancelled_v1(&base_host, &cancelled);
        if (status != PRDB_SDK_EXECUTOR_OK || cancelled) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_CANCELLED;
            break;
        }
        status = prdb_sdk_executor_next_batch_v1(
            &base_host, cursor, max_candidates, max_batch_bytes, batch_response,
            batch_response_capacity, &batch_response_len, &batch, sizeof(batch));
        if (status != PRDB_SDK_EXECUTOR_OK) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
            break;
        }
        ++page_count;
        if (batch.finished) {
            cursor_open = 0;
        } else {
            memcpy(cursor, batch.continuation, sizeof(cursor));
        }
        status = prdb_sdk_batch_rows_begin_v1(&batch, &rows, sizeof(rows));
        if (status != PRDB_SDK_BATCH_OK) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
            break;
        }
        while (rows.rows_remaining != 0) {
            uint32_t truth = PRDB_SDK_SQL_TRUTH_UNKNOWN;
            status = prdb_sdk_batch_row_next_v1(&rows, &row, sizeof(row));
            if (status != PRDB_SDK_BATCH_OK) {
                final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
                break;
            }
            status = prdb_sdk_sql_eval_bound_select_row_v2(
                predicate, binding, parameters, parameter_count, &row,
                cell_scratch, cell_scratch_capacity, &truth);
            if (status != PRDB_SDK_SQL_EVAL_OK) {
                final_status = PRDB_SDK_OPTIMIZER_PLAN_FILTER_ERROR;
                break;
            }
            if (truth == PRDB_SDK_SQL_TRUTH_TRUE) {
                if (target_count >= target_capacity) {
                    final_status = PRDB_SDK_OPTIMIZER_PLAN_REQUEST_LIMIT;
                    break;
                }
                memcpy(target_handles + (uint64_t)target_count * 16,
                       row.handle, 16);
                ++target_count;
            }
        }
        if (final_status != PRDB_SDK_OPTIMIZER_PLAN_OK || batch.finished)
            break;
    }
    if (cursor_open && final_status == PRDB_SDK_OPTIMIZER_PLAN_OK)
        final_status = PRDB_SDK_OPTIMIZER_PLAN_REQUEST_LIMIT;
    if (cursor_open) {
        status = prdb_sdk_executor_close_cursor_v1(
            &base_host, cursor, scan_response, scan_response_capacity,
            &scan_response_len);
        ++close_count;
        if (status != PRDB_SDK_EXECUTOR_OK &&
            final_status == PRDB_SDK_OPTIMIZER_PLAN_OK) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
        }
    }
    if (final_status != PRDB_SDK_OPTIMIZER_PLAN_OK)
        return latch_optimizer_statement_failure(&host->v4, final_status);
    if (4U + 2U * page_count + close_count + target_count > 256U) {
        return latch_optimizer_statement_failure(
            &host->v4, PRDB_SDK_OPTIMIZER_PLAN_REQUEST_LIMIT);
    }
    for (uint32_t index = 0; index < target_count; ++index) {
        status = prdb_sdk_executor_stage_delete_v1(
            &base_host, target_handles + (uint64_t)index * 16,
            scan_response, scan_response_capacity, &scan_response_len);
        if (status != PRDB_SDK_EXECUTOR_OK) {
            return latch_optimizer_statement_failure(
                &host->v4, PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR);
        }
    }
    *deleted_count_out = target_count;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_execute_update_ast_v1(
    const prdb_host_api_v5 *host, const prdb_sdk_update_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v2 *relation_columns,
    uint32_t relation_column_capacity,
    prdb_sdk_select_binding_v2 *binding, uint32_t binding_capacity,
    prdb_sdk_value_v1 *cell_scratch, uint32_t cell_scratch_capacity,
    uint8_t *target_handles, uint32_t target_capacity,
    uint32_t *updated_count_out) {
    prdb_host_api_v2 base_host = {0};
    prdb_sdk_optimizer_response_v1 decoded_response = {0};
    prdb_sdk_optimizer_update_plan_v1 update = {0};
    prdb_sdk_batch_v1 batch = {0};
    prdb_sdk_row_iterator_v1 rows = {0};
    prdb_sdk_row_v1 row = {0};
    prdb_sdk_executor_update_patch_v1 patches[
        PRDB_SDK_OPTIMIZER_PLAN_MAX_UPDATE_ASSIGNMENTS] = {{0}};
    uint8_t cursor[16] = {0};
    uint64_t request_len = 0;
    uint64_t optimizer_response_len = 0;
    uint64_t relation_response_len = 0;
    uint64_t scan_response_len = 0;
    uint64_t batch_response_len = 0;
    uint32_t target_count = 0;
    uint32_t page_count = 0;
    uint32_t close_count = 0;
    uint32_t scan_opened = 0;
    int cursor_open = 0;
    int32_t final_status = PRDB_SDK_OPTIMIZER_PLAN_OK;
    int32_t status;
    const prdb_sdk_select_v2 *predicate;

    if (host == NULL ||
        host->v4.v3.struct_size < sizeof(prdb_host_api_v3) ||
        host->v4.v3.abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->v4.v3.context == NULL ||
        host->v4.v3.executor_request == NULL ||
        host->v4.v3.statement_failure == NULL ||
        host->v4.optimizer_request == NULL || host->optimizer_request == NULL ||
        request == NULL || optimizer_response == NULL ||
        relation_response == NULL || scan_response == NULL ||
        batch_response == NULL || relation_out == NULL ||
        relation_columns == NULL || binding == NULL || cell_scratch == NULL ||
        target_handles == NULL || updated_count_out == NULL ||
        relation_out_capacity < sizeof(*relation_out) ||
        binding_capacity < sizeof(*binding) ||
        parameter_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        (parameter_count != 0 && parameters == NULL) || max_candidates == 0 ||
        max_candidates > PRDB_SDK_BATCH_MAX_ROWS || max_batch_bytes < 8 ||
        max_batch_bytes > PRDB_SDK_BATCH_MAX_BYTES ||
        batch_response_capacity < max_batch_bytes || request_capacity == 0 ||
        request_capacity > PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES ||
        optimizer_response_capacity == 0 ||
        optimizer_response_capacity > PRDB_QUERY_MAX_IPC_REQUEST_BYTES ||
        relation_response_capacity == 0 || scan_response_capacity == 0 ||
        relation_column_capacity == 0 || cell_scratch_capacity == 0 ||
        target_capacity == 0 ||
        target_capacity > PRDB_SDK_OPTIMIZER_PLAN_MAX_DELETE_TARGETS ||
        ranges_overlap(request, request_capacity, optimizer_response,
                       optimizer_response_capacity) ||
        ranges_overlap(request, request_capacity, relation_response,
                       relation_response_capacity) ||
        ranges_overlap(optimizer_response, optimizer_response_capacity,
                       relation_response, relation_response_capacity) ||
        ranges_overlap(relation_response, relation_response_capacity,
                       scan_response, scan_response_capacity) ||
        ranges_overlap(relation_response, relation_response_capacity,
                       batch_response, batch_response_capacity) ||
        ranges_overlap(scan_response, scan_response_capacity, batch_response,
                       batch_response_capacity) ||
        ranges_overlap(target_handles, (uint64_t)target_capacity * 16,
                       request, request_capacity) ||
        ranges_overlap(target_handles, (uint64_t)target_capacity * 16,
                       optimizer_response, optimizer_response_capacity) ||
        ranges_overlap(target_handles, (uint64_t)target_capacity * 16,
                       relation_response, relation_response_capacity) ||
        ranges_overlap(target_handles, (uint64_t)target_capacity * 16,
                       scan_response, scan_response_capacity) ||
        ranges_overlap(target_handles, (uint64_t)target_capacity * 16,
                       batch_response, batch_response_capacity)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    *updated_count_out = 0;
    base_host.struct_size = sizeof(base_host);
    base_host.abi_version = PRDB_QUERY_ABI_V2_VERSION;
    base_host.context = host->v4.v3.context;
    base_host.executor_request = host->v4.v3.executor_request;
    base_host.planner_request = host->v4.v3.planner_request;
    base_host.write_result = host->v4.v3.write_result;
    predicate = &statement->predicate_statement;

    status = prdb_sdk_optimizer_encode_update_v1(
        statement, parameters, parameter_count, schema, schema_len, request,
        request_capacity, &request_len);
    if (status == PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED)
        return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
    if (status == PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL ||
        status == PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED)
        return PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL;
    if (status != PRDB_SDK_OPTIMIZER_ENCODE_OK)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;

    status = prdb_sdk_optimizer_submit_update_v1(
        host, request, request_len, optimizer_response,
        optimizer_response_capacity, &optimizer_response_len,
        &decoded_response, sizeof(decoded_response));
    if (status == PRDB_SDK_OPTIMIZER_RESULT_RESPONSE_TOO_SMALL) {
        status = PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL;
    } else if (status != PRDB_SDK_OPTIMIZER_RESULT_OK) {
        status = status == PRDB_SDK_OPTIMIZER_RESULT_INVALID_ARGUMENT
                     ? PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT
                     : PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
    } else if (decoded_response.status ==
               PRDB_SDK_OPTIMIZER_STATUS_UNAVAILABLE) {
        status = PRDB_SDK_OPTIMIZER_PLAN_UNAVAILABLE;
    } else if (decoded_response.status !=
               PRDB_SDK_OPTIMIZER_STATUS_SUCCESS) {
        status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
    }
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK)
        return latch_optimizer_statement_failure(&host->v4, status);

    status = prdb_sdk_optimizer_validate_update_plan_with_parameters_v1(
        &base_host, statement, parameters, parameter_count, schema, schema_len,
        decoded_response.plan, decoded_response.plan_len, relation_response,
        relation_response_capacity, &relation_response_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK)
        return latch_optimizer_statement_failure(&host->v4, status);

    status = prdb_sdk_optimizer_open_plan_scan_v1(
        &base_host, decoded_response.plan, decoded_response.plan_len,
        relation_response, relation_response_capacity, &relation_response_len,
        scan_response, scan_response_capacity, &scan_response_len,
        relation_out, relation_out_capacity, relation_columns,
        relation_column_capacity, cursor, &scan_opened);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK)
        return latch_optimizer_statement_failure(&host->v4, status);
    if (relation_out->column_count > PRDB_SDK_SQL_MAX_RELATION_COLUMNS ||
        relation_out->column_count > cell_scratch_capacity) {
        return latch_optimizer_statement_failure(
            &host->v4, PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT);
    }
    status = prdb_sdk_sql_bind_select_v2(
        predicate, relation_out->name, relation_out->name_len,
        relation_columns, relation_out->column_count, binding,
        binding_capacity);
    if (status != PRDB_SDK_SQL_BIND_OK) {
        return latch_optimizer_statement_failure(
            &host->v4, PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    }
    if (scan_opened != 0) {
        cursor_open = 1;
        while (page_count < PRDB_SDK_OPTIMIZER_PLAN_MAX_NEXT_REQUESTS) {
            int32_t cancelled = 0;
            status = prdb_sdk_executor_check_cancelled_v1(&base_host,
                                                          &cancelled);
            if (status != PRDB_SDK_EXECUTOR_OK || cancelled) {
                final_status = PRDB_SDK_OPTIMIZER_PLAN_CANCELLED;
                break;
            }
            status = prdb_sdk_executor_next_batch_v1(
                &base_host, cursor, max_candidates, max_batch_bytes,
                batch_response, batch_response_capacity, &batch_response_len,
                &batch, sizeof(batch));
            if (status != PRDB_SDK_EXECUTOR_OK) {
                final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
                break;
            }
            ++page_count;
            if (batch.finished) {
                cursor_open = 0;
            } else {
                memcpy(cursor, batch.continuation, sizeof(cursor));
            }
            status = prdb_sdk_batch_rows_begin_v1(&batch, &rows, sizeof(rows));
            if (status != PRDB_SDK_BATCH_OK) {
                final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
                break;
            }
            while (rows.rows_remaining != 0) {
                uint32_t truth = PRDB_SDK_SQL_TRUTH_UNKNOWN;
                status = prdb_sdk_batch_row_next_v1(&rows, &row, sizeof(row));
                if (status != PRDB_SDK_BATCH_OK) {
                    final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
                    break;
                }
                status = prdb_sdk_sql_eval_bound_select_row_v2(
                    predicate, binding, parameters, parameter_count, &row,
                    cell_scratch, cell_scratch_capacity, &truth);
                if (status != PRDB_SDK_SQL_EVAL_OK) {
                    final_status = PRDB_SDK_OPTIMIZER_PLAN_FILTER_ERROR;
                    break;
                }
                if (truth == PRDB_SDK_SQL_TRUTH_TRUE) {
                    if (target_count >= target_capacity) {
                        final_status = PRDB_SDK_OPTIMIZER_PLAN_REQUEST_LIMIT;
                        break;
                    }
                    memcpy(target_handles + (uint64_t)target_count * 16,
                           row.handle, 16);
                    ++target_count;
                }
            }
            if (final_status != PRDB_SDK_OPTIMIZER_PLAN_OK || batch.finished)
                break;
        }
    }
    if (cursor_open && final_status == PRDB_SDK_OPTIMIZER_PLAN_OK)
        final_status = PRDB_SDK_OPTIMIZER_PLAN_REQUEST_LIMIT;
    if (cursor_open) {
        status = prdb_sdk_executor_close_cursor_v1(
            &base_host, cursor, scan_response, scan_response_capacity,
            &scan_response_len);
        ++close_count;
        if (status != PRDB_SDK_EXECUTOR_OK &&
            final_status == PRDB_SDK_OPTIMIZER_PLAN_OK) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
        }
    }
    if (final_status != PRDB_SDK_OPTIMIZER_PLAN_OK)
        return latch_optimizer_statement_failure(&host->v4, final_status);
    if (4U + 2U * page_count + close_count + target_count > 256U) {
        return latch_optimizer_statement_failure(
            &host->v4, PRDB_SDK_OPTIMIZER_PLAN_REQUEST_LIMIT);
    }

    status = prdb_sdk_optimizer_decode_update_plan_v1(
        decoded_response.plan, decoded_response.plan_len, &update,
        sizeof(update));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK)
        return latch_optimizer_statement_failure(&host->v4, status);
    for (uint32_t index = 0; index < update.assignment_count; ++index) {
        patches[index].struct_size = sizeof(patches[index]);
        patches[index].abi_version = PRDB_SDK_EXECUTOR_ABI_V1_VERSION;
        patches[index].ordinal = update.assignments[index].column.ordinal;
        if (update.assignments[index].value_kind ==
            PRDB_OPTIMIZER_UPDATE_PARAMETER_VALUE) {
            patches[index].value =
                parameters[update.assignments[index].parameter_index - 1];
        } else {
            patches[index].value = (prdb_sdk_value_v1){
                sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
                PRDB_SDK_VALUE_STATE_NULL, 0, NULL, 0};
        }
    }
    for (uint32_t index = 0; index < target_count; ++index) {
        status = prdb_sdk_executor_stage_update_v1(
            &base_host, target_handles + (uint64_t)index * 16, patches,
            (uint16_t)update.assignment_count, request, request_capacity,
            scan_response, scan_response_capacity, &scan_response_len);
        if (status != PRDB_SDK_EXECUTOR_OK) {
            return latch_optimizer_statement_failure(
                &host->v4, PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR);
        }
    }
    *updated_count_out = target_count;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

static int32_t execute_select_ast_internal(
    const prdb_host_api_v4 *host, const prdb_sdk_select_v2 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    int parameterized,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v2 *relation_columns,
    uint32_t relation_column_capacity, prdb_sdk_value_v1 *source_cells,
    uint32_t source_cell_capacity, prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out) {
    prdb_sdk_optimizer_response_v1 decoded_response = {0};
    prdb_host_api_v2 base_host = {0};
    uint64_t request_len = 0;
    uint64_t optimizer_response_len = 0;
    int32_t status;

    if (host == NULL || host->v3.struct_size < sizeof(host->v3) ||
        host->v3.abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->v3.executor_request == NULL ||
        host->v3.statement_failure == NULL || host->optimizer_request == NULL ||
        delivered_rows_out == NULL) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    *delivered_rows_out = 0;
    status = parameterized != 0
                 ? prdb_sdk_optimizer_encode_select_with_parameters_v1(
                       statement, parameters, parameter_count, schema,
                       schema_len, request, request_capacity, &request_len)
                 : prdb_sdk_optimizer_encode_select_v1(
                       statement, schema, schema_len, request,
                       request_capacity, &request_len);
    if (status == PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED) {
        return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
    }
    if (status == PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL ||
        status == PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED) {
        return PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL;
    }
    if (status != PRDB_SDK_OPTIMIZER_ENCODE_OK) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    status = prdb_sdk_optimizer_submit_select_v1(
        host, request, request_len, optimizer_response,
        optimizer_response_capacity, &optimizer_response_len,
        &decoded_response, sizeof(decoded_response));
    if (status == PRDB_SDK_OPTIMIZER_RESULT_RESPONSE_TOO_SMALL) {
        status = PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL;
    } else if (status != PRDB_SDK_OPTIMIZER_RESULT_OK) {
        status = status == PRDB_SDK_OPTIMIZER_RESULT_INVALID_ARGUMENT
                     ? PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT
                     : PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
    } else if (decoded_response.status ==
               PRDB_SDK_OPTIMIZER_STATUS_UNAVAILABLE) {
        status = PRDB_SDK_OPTIMIZER_PLAN_UNAVAILABLE;
    } else if (decoded_response.status !=
               PRDB_SDK_OPTIMIZER_STATUS_SUCCESS) {
        status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
    }
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) {
        return latch_optimizer_statement_failure(host, status);
    }
    base_host.struct_size = sizeof(base_host);
    base_host.abi_version = PRDB_QUERY_ABI_V2_VERSION;
    base_host.context = host->v3.context;
    base_host.executor_request = host->v3.executor_request;
    base_host.planner_request = host->v3.planner_request;
    base_host.write_result = host->v3.write_result;
    status = parameterized != 0
                 ? prdb_sdk_optimizer_validate_select_plan_with_parameters_v1(
                       &base_host, statement, parameters, parameter_count,
                       schema, schema_len, decoded_response.plan,
                       decoded_response.plan_len, relation_response,
                       relation_response_capacity, &optimizer_response_len)
                 : prdb_sdk_optimizer_validate_select_plan_v1(
                       &base_host, statement, schema, schema_len,
                       decoded_response.plan, decoded_response.plan_len,
                       relation_response, relation_response_capacity,
                       &optimizer_response_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) {
        return latch_optimizer_statement_failure(host, status);
    }
    status = prdb_sdk_optimizer_execute_unfiltered_plan_v1(
        &base_host, decoded_response.plan, decoded_response.plan_len,
        max_candidates, max_batch_bytes, relation_response,
        relation_response_capacity, scan_response, scan_response_capacity,
        batch_response, batch_response_capacity, relation_out,
        relation_out_capacity, relation_columns, relation_column_capacity,
        source_cells, source_cell_capacity, projected_values,
        projected_value_capacity, on_row, row_context, delivered_rows_out);
    if (status == PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED) {
        status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
    }
    return latch_optimizer_statement_failure(host, status);
}

int32_t prdb_sdk_optimizer_execute_ordered_select_ast_v1(
    const prdb_host_api_v4 *host, const prdb_sdk_select_v3 *statement,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v2 *relation_columns,
    uint32_t relation_column_capacity, prdb_sdk_value_v1 *source_cells,
    uint32_t source_cell_capacity, prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity, uint8_t *materialized_rows,
    uint64_t materialized_rows_capacity,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out) {
    prdb_sdk_optimizer_response_v1 response = {0};
    prdb_sdk_optimizer_ordered_select_plan_v1 ordered = {0};
    prdb_sdk_select_binding_v3 binding = {0};
    prdb_host_api_v2 executor_host = {0};
    prdb_sdk_batch_v1 batch = {0};
    prdb_sdk_row_iterator_v1 rows = {0};
    prdb_sdk_row_v1 ordered_rows[PRDB_SDK_SQL_ORDER_MAX_ROWS];
    prdb_sdk_value_v1 order_keys[PRDB_SDK_SQL_ORDER_MAX_ROWS];
    uint32_t order_indices[PRDB_SDK_SQL_ORDER_MAX_ROWS];
    uint8_t cursor[16] = {0};
    uint64_t optimizer_response_len = 0;
    uint64_t relation_response_len = 0;
    uint64_t scan_response_len = 0;
    uint64_t batch_response_len = 0;
    uint64_t materialized_offset = 5;
    uint32_t materialized_count = 0;
    uint32_t page_count = 0;
    uint32_t cursor_open = 0;
    uint32_t delivered = 0;
    int32_t status;
    int32_t final_status = PRDB_SDK_OPTIMIZER_PLAN_OK;
    const void *output_regions[10];
    uint64_t output_region_lengths[10];

    if (host == NULL || host->v3.struct_size < sizeof(host->v3) ||
        host->v3.abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->v3.statement_failure == NULL ||
        host->v3.executor_request == NULL || host->optimizer_request == NULL ||
        statement == NULL || statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V3_VERSION ||
        statement->base.sql == NULL || statement->base.sql_len == 0 ||
        schema == NULL || request == NULL || optimizer_response == NULL ||
        relation_response == NULL || scan_response == NULL ||
        batch_response == NULL || relation_out == NULL ||
        relation_columns == NULL || source_cells == NULL ||
        projected_values == NULL || materialized_rows == NULL ||
        on_row == NULL || delivered_rows_out == NULL ||
        max_candidates == 0 || max_candidates > PRDB_SDK_BATCH_MAX_ROWS ||
        max_batch_bytes < 8 || max_batch_bytes > PRDB_SDK_BATCH_MAX_BYTES ||
        batch_response_capacity < max_batch_bytes ||
        materialized_rows_capacity < 5 ||
        materialized_rows_capacity >
            PRDB_SDK_SQL_EXEC_V3_MAX_MATERIALIZED_BYTES ||
        relation_column_capacity == 0 || source_cell_capacity == 0 ||
        projected_value_capacity == 0 ||
        request_capacity > PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES ||
        optimizer_response_capacity == 0 ||
        relation_response_capacity == 0 || scan_response_capacity == 0 ||
        schema_len == 0 || schema_len > 63) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    if (ranges_overlap(delivered_rows_out, sizeof(*delivered_rows_out),
                       statement, sizeof(*statement)) ||
        ranges_overlap(delivered_rows_out, sizeof(*delivered_rows_out),
                       schema, schema_len) ||
        ranges_overlap(delivered_rows_out, sizeof(*delivered_rows_out), host,
                       sizeof(*host))) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    output_regions[0] = request;
    output_region_lengths[0] = request_capacity;
    output_regions[1] = optimizer_response;
    output_region_lengths[1] = optimizer_response_capacity;
    output_regions[2] = relation_response;
    output_region_lengths[2] = relation_response_capacity;
    output_regions[3] = scan_response;
    output_region_lengths[3] = scan_response_capacity;
    output_regions[4] = batch_response;
    output_region_lengths[4] = batch_response_capacity;
    output_regions[5] = materialized_rows;
    output_region_lengths[5] = materialized_rows_capacity;
    output_regions[6] = relation_out;
    output_region_lengths[6] = relation_out_capacity;
    output_regions[7] = relation_columns;
    output_region_lengths[7] =
        (uint64_t)relation_column_capacity * sizeof(*relation_columns);
    output_regions[8] = source_cells;
    output_region_lengths[8] =
        (uint64_t)source_cell_capacity * sizeof(*source_cells);
    output_regions[9] = projected_values;
    output_region_lengths[9] =
        (uint64_t)projected_value_capacity * sizeof(*projected_values);
    for (uint32_t left = 0; left < 10; ++left) {
        if (ranges_overlap(output_regions[left], output_region_lengths[left],
                           delivered_rows_out,
                           sizeof(*delivered_rows_out)) ||
            ranges_overlap(output_regions[left], output_region_lengths[left],
                           statement, sizeof(*statement)) ||
            ranges_overlap(output_regions[left], output_region_lengths[left],
                           statement->base.sql,
                           statement->base.sql_len) ||
            ranges_overlap(output_regions[left], output_region_lengths[left],
                           schema, schema_len) ||
            ranges_overlap(output_regions[left], output_region_lengths[left],
                           host, sizeof(*host))) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
        }
        for (uint32_t right = left + 1; right < 10; ++right) {
            if (ranges_overlap(output_regions[left],
                               output_region_lengths[left],
                               output_regions[right],
                               output_region_lengths[right])) {
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
            }
        }
    }
    *delivered_rows_out = 0;
    status = prdb_sdk_optimizer_encode_ordered_select_v1(
        statement, schema, schema_len, request, request_capacity,
        &optimizer_response_len);
    if (status == PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED) {
        return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
    }
    if (status == PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL ||
        status == PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED) {
        return latch_optimizer_statement_failure(
            host, PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL);
    }
    if (status != PRDB_SDK_OPTIMIZER_ENCODE_OK) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    status = prdb_sdk_optimizer_submit_select_v1(
        host, request, optimizer_response_len, optimizer_response,
        optimizer_response_capacity, &optimizer_response_len, &response,
        sizeof(response));
    if (status == PRDB_SDK_OPTIMIZER_RESULT_RESPONSE_TOO_SMALL) {
        final_status = PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL;
    } else if (status != PRDB_SDK_OPTIMIZER_RESULT_OK) {
        final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
    } else if (response.status == PRDB_SDK_OPTIMIZER_STATUS_UNAVAILABLE) {
        final_status = PRDB_SDK_OPTIMIZER_PLAN_UNAVAILABLE;
    } else if (response.status != PRDB_SDK_OPTIMIZER_STATUS_SUCCESS) {
        final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
    }
    if (final_status != PRDB_SDK_OPTIMIZER_PLAN_OK) {
        return latch_optimizer_statement_failure(host, final_status);
    }
    status = prdb_sdk_optimizer_decode_ordered_select_plan_v1(
        response.plan, response.plan_len, &ordered, sizeof(ordered));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) {
        return latch_optimizer_statement_failure(host, status);
    }

    executor_host.struct_size = sizeof(executor_host);
    executor_host.abi_version = PRDB_QUERY_ABI_V2_VERSION;
    executor_host.context = host->v3.context;
    executor_host.executor_request = host->v3.executor_request;
    executor_host.planner_request = host->v3.planner_request;
    executor_host.write_result = host->v3.write_result;
    status = prdb_sdk_optimizer_acquire_plan_relation_v1(
        &executor_host, response.plan, response.plan_len, relation_response,
        relation_response_capacity, &relation_response_len, relation_out,
        relation_out_capacity, relation_columns, relation_column_capacity);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) {
        return latch_optimizer_statement_failure(host, status);
    }
    if (source_cell_capacity < relation_out->column_count ||
        ordered.order_key.ordinal >= relation_out->column_count ||
        projected_value_capacity < ordered.plan.projection_count) {
        return latch_optimizer_statement_failure(
            host, PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    }
    status = prdb_sdk_sql_bind_select_v3(
        statement, relation_out->name, relation_out->name_len,
        relation_columns, relation_out->column_count, &binding,
        sizeof(binding));
    if (status != PRDB_SDK_SQL_BIND_OK ||
        prdb_sdk_optimizer_match_ordered_select_plan_v1(
            statement, &binding, schema, schema_len, response.plan,
            response.plan_len) != PRDB_SDK_OPTIMIZER_PLAN_OK) {
        return latch_optimizer_statement_failure(
            host, PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    }
    if (ordered.plan.has_limit != 0 && ordered.plan.limit == 0) {
        return PRDB_SDK_OPTIMIZER_PLAN_OK;
    }
    status = prdb_sdk_executor_open_full_scan_v1(
        &executor_host, relation_out->executor_handle, scan_response,
        scan_response_capacity, &scan_response_len, cursor);
    if (status == PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL) {
        final_status = PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL;
    } else if (status != PRDB_SDK_EXECUTOR_OK) {
        final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
    } else {
        cursor_open = 1;
    }
    while (final_status == PRDB_SDK_OPTIMIZER_PLAN_OK &&
           cursor_open != 0) {
        int32_t cancelled = 0;
        if (page_count >= OPTIMIZER_ORDERED_SELECT_MAX_SCAN_PAGES) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_REQUEST_LIMIT;
            break;
        }
        status = prdb_sdk_executor_check_cancelled_v1(
            &executor_host, &cancelled);
        if (status != PRDB_SDK_EXECUTOR_OK || cancelled != 0) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_CANCELLED;
            break;
        }
        status = prdb_sdk_executor_next_batch_v1(
            &executor_host, cursor, max_candidates, max_batch_bytes,
            batch_response, batch_response_capacity, &batch_response_len,
            &batch, sizeof(batch));
        if (status != PRDB_SDK_EXECUTOR_OK) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
            break;
        }
        ++page_count;
        if (batch.finished != 0) {
            cursor_open = 0;
        } else {
            memcpy(cursor, batch.continuation, sizeof(cursor));
        }
        if (prdb_sdk_batch_rows_begin_v1(&batch, &rows, sizeof(rows)) !=
            PRDB_SDK_BATCH_OK) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            break;
        }
        while (rows.rows_remaining != 0) {
            prdb_sdk_row_v1 row = {0};
            uint64_t row_start;
            uint64_t row_length;
            if (materialized_count >= PRDB_SDK_SQL_ORDER_MAX_ROWS ||
                prdb_sdk_batch_row_next_v1(&rows, &row, sizeof(row)) !=
                    PRDB_SDK_BATCH_OK ||
                row.cells_offset < 18 || rows.offset < row.cells_offset) {
                final_status = PRDB_SDK_OPTIMIZER_PLAN_REQUEST_LIMIT;
                break;
            }
            row_start = row.cells_offset - 18;
            row_length = rows.offset - row_start;
            if (row_length > materialized_rows_capacity -
                                 materialized_offset) {
                final_status = PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL;
                break;
            }
            memcpy(materialized_rows + materialized_offset,
                   batch_response + row_start, (size_t)row_length);
            materialized_offset += row_length;
            ++materialized_count;
        }
    }
    if (cursor_open != 0) {
        status = prdb_sdk_executor_close_cursor_v1(
            &executor_host, cursor, scan_response, scan_response_capacity,
            &scan_response_len);
        if (status != PRDB_SDK_EXECUTOR_OK &&
            final_status == PRDB_SDK_OPTIMIZER_PLAN_OK) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
        }
    }
    if (final_status == PRDB_SDK_OPTIMIZER_PLAN_OK) {
        int32_t cancelled = 0;
        status = prdb_sdk_executor_check_cancelled_v1(
            &executor_host, &cancelled);
        if (status != PRDB_SDK_EXECUTOR_OK || cancelled != 0) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_CANCELLED;
        }
    }
    if (final_status == PRDB_SDK_OPTIMIZER_PLAN_OK) {
        prdb_sdk_batch_v1 materialized_batch = {0};
        prdb_sdk_row_iterator_v1 materialized_iterator = {0};
        materialized_rows[0] = 0;
        materialized_rows[1] = PRDB_SDK_BATCH_WIRE_V1_VERSION;
        materialized_rows[2] = 1;
        materialized_rows[3] = (uint8_t)(materialized_count >> 8);
        materialized_rows[4] = (uint8_t)materialized_count;
        if (prdb_sdk_batch_open_v1(
                &materialized_batch, sizeof(materialized_batch),
                materialized_rows, materialized_offset) != PRDB_SDK_BATCH_OK ||
            prdb_sdk_batch_rows_begin_v1(
                &materialized_batch, &materialized_iterator,
                sizeof(materialized_iterator)) != PRDB_SDK_BATCH_OK) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        for (uint32_t index = 0;
             final_status == PRDB_SDK_OPTIMIZER_PLAN_OK &&
             index < materialized_count; ++index) {
            prdb_sdk_row_v1 *row = &ordered_rows[index];
            prdb_sdk_cell_iterator_v1 cells = {0};
            if (prdb_sdk_batch_row_next_v1(
                    &materialized_iterator, row, sizeof(*row)) !=
                    PRDB_SDK_BATCH_OK ||
                row->cell_count != relation_out->column_count ||
                prdb_sdk_row_cells_begin_v1(row, &cells, sizeof(cells)) !=
                    PRDB_SDK_BATCH_OK) {
                final_status = PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
                break;
            }
            for (uint32_t ordinal = 0;
                 ordinal < relation_out->column_count; ++ordinal) {
                if (prdb_sdk_cell_next_v1(
                        &cells, &source_cells[ordinal],
                        sizeof(source_cells[ordinal])) != PRDB_SDK_BATCH_OK) {
                    final_status = PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
                    break;
                }
            }
            order_keys[index] =
                source_cells[ordered.order_key.ordinal];
        }
        if (final_status == PRDB_SDK_OPTIMIZER_PLAN_OK &&
            prdb_sdk_sql_order_row_indices_v1(
                order_keys, materialized_count, ordered.order_key.type_code,
                ordered.order_key.type_parameters[1],
                ordered.descending != 0
                    ? PRDB_SDK_SQL_ORDER_DESCENDING
                    : PRDB_SDK_SQL_ORDER_ASCENDING,
                order_indices, PRDB_SDK_SQL_ORDER_MAX_ROWS) !=
                PRDB_SDK_SQL_ORDER_OK) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_FILTER_ERROR;
        }
        while (final_status == PRDB_SDK_OPTIMIZER_PLAN_OK &&
               delivered < materialized_count &&
               (ordered.plan.has_limit == 0 ||
                delivered < ordered.plan.limit)) {
            if (delivered % OPTIMIZER_ORDERED_SELECT_CALLBACK_POLL_INTERVAL ==
                0) {
                int32_t cancelled = 0;
                status = prdb_sdk_executor_check_cancelled_v1(
                    &executor_host, &cancelled);
                if (status != PRDB_SDK_EXECUTOR_OK || cancelled != 0) {
                    final_status = PRDB_SDK_OPTIMIZER_PLAN_CANCELLED;
                    break;
                }
            }
            const prdb_sdk_row_v1 *row = &ordered_rows[order_indices[delivered]];
            prdb_sdk_cell_iterator_v1 cells = {0};
            if (prdb_sdk_row_cells_begin_v1(row, &cells, sizeof(cells)) !=
                PRDB_SDK_BATCH_OK) {
                final_status = PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
                break;
            }
            for (uint32_t ordinal = 0;
                 ordinal < relation_out->column_count; ++ordinal) {
                if (prdb_sdk_cell_next_v1(
                        &cells, &source_cells[ordinal],
                        sizeof(source_cells[ordinal])) != PRDB_SDK_BATCH_OK) {
                    final_status = PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
                    break;
                }
            }
            if (final_status != PRDB_SDK_OPTIMIZER_PLAN_OK) break;
            for (uint32_t index = 0;
                 index < ordered.plan.projection_count; ++index) {
                const prdb_sdk_optimizer_plan_column_v1 *projection =
                    &ordered.plan.projections[index];
                if (projection->ordinal >= relation_out->column_count) {
                    final_status =
                        PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
                    break;
                }
                projected_values[index] = source_cells[projection->ordinal];
            }
            if (final_status != PRDB_SDK_OPTIMIZER_PLAN_OK) break;
            if (on_row(row_context, ordered.plan.projections,
                       projected_values, ordered.plan.projection_count) != 0) {
                final_status = PRDB_SDK_OPTIMIZER_PLAN_ROW_CALLBACK_ERROR;
                break;
            }
            ++delivered;
        }
    }
    *delivered_rows_out = delivered;
    return latch_optimizer_statement_failure(host, final_status);
}

int32_t prdb_sdk_optimizer_execute_select_ast_v1(
    const prdb_host_api_v4 *host, const prdb_sdk_select_v2 *statement,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v2 *relation_columns,
    uint32_t relation_column_capacity, prdb_sdk_value_v1 *source_cells,
    uint32_t source_cell_capacity, prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out) {
    return execute_select_ast_internal(
        host, statement, NULL, 0, 0, schema, schema_len, max_candidates,
        max_batch_bytes, request, request_capacity, optimizer_response,
        optimizer_response_capacity, relation_response,
        relation_response_capacity, scan_response, scan_response_capacity,
        batch_response, batch_response_capacity, relation_out,
        relation_out_capacity, relation_columns, relation_column_capacity,
        source_cells, source_cell_capacity, projected_values,
        projected_value_capacity, on_row, row_context, delivered_rows_out);
}

int32_t prdb_sdk_optimizer_execute_select_ast_with_parameters_v1(
    const prdb_host_api_v4 *host, const prdb_sdk_select_v2 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v2 *relation_columns,
    uint32_t relation_column_capacity, prdb_sdk_value_v1 *source_cells,
    uint32_t source_cell_capacity, prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out) {
    return execute_select_ast_internal(
        host, statement, parameters, parameter_count, 1, schema, schema_len,
        max_candidates, max_batch_bytes, request, request_capacity,
        optimizer_response, optimizer_response_capacity, relation_response,
        relation_response_capacity, scan_response, scan_response_capacity,
        batch_response, batch_response_capacity, relation_out,
        relation_out_capacity, relation_columns, relation_column_capacity,
        source_cells, source_cell_capacity, projected_values,
        projected_value_capacity, on_row, row_context, delivered_rows_out);
}

typedef struct optimizer_result_context_v1 {
    const prdb_host_api_v3 *host;
    const prdb_sdk_sql_column_shape_v2 *columns;
    const prdb_sdk_executor_relation_v1 *relation;
    uint8_t *record_buffer;
    uint64_t record_capacity;
} optimizer_result_context_v1;

static int32_t emit_optimizer_projected_row_v1(
    void *context, const prdb_sdk_optimizer_plan_column_v1 *projections,
    const prdb_sdk_value_v1 *values, uint32_t projection_count) {
    optimizer_result_context_v1 *result = context;
    uint32_t ordinals[PRDB_SDK_OPTIMIZER_PLAN_MAX_PROJECTIONS];
    uint64_t record_len = 0;
    if (result == NULL || projections == NULL || values == NULL ||
        projection_count == 0 ||
        projection_count > PRDB_SDK_OPTIMIZER_PLAN_MAX_PROJECTIONS) {
        return -1;
    }
    for (uint32_t index = 0; index < projection_count; ++index)
        ordinals[index] = projections[index].ordinal;
    return prdb_sdk_sql_write_projected_result_v1(
               result->host, result->columns, result->relation->column_count,
               ordinals, values, projection_count, result->record_buffer,
               result->record_capacity, &record_len) == PRDB_SDK_SQL_RESULT_OK
               ? 0
               : -1;
}

static int32_t execute_select_ast_to_result_internal(
    const prdb_host_api_v4 *host, const prdb_sdk_select_v2 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    int parameterized,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v2 *relation_columns,
    uint32_t relation_column_capacity, prdb_sdk_value_v1 *source_cells,
    uint32_t source_cell_capacity, prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity, uint8_t *result_record_buffer,
    uint64_t result_record_capacity, uint32_t *delivered_rows_out) {
    optimizer_result_context_v1 result = {0};
    if (host == NULL || host->v3.write_result == NULL || relation_out == NULL ||
        relation_columns == NULL || relation_column_capacity == 0 ||
        result_record_buffer == NULL || result_record_capacity == 0) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    result.host = &host->v3;
    result.columns = relation_columns;
    result.relation = relation_out;
    result.record_buffer = result_record_buffer;
    result.record_capacity = result_record_capacity;
    if (parameterized != 0)
        return prdb_sdk_optimizer_execute_select_ast_with_parameters_v1(
            host, statement, parameters, parameter_count, schema, schema_len,
            max_candidates, max_batch_bytes, request, request_capacity,
            optimizer_response, optimizer_response_capacity,
            relation_response, relation_response_capacity, scan_response,
            scan_response_capacity, batch_response, batch_response_capacity,
            relation_out, relation_out_capacity, relation_columns,
            relation_column_capacity, source_cells, source_cell_capacity,
            projected_values, projected_value_capacity,
            emit_optimizer_projected_row_v1, &result, delivered_rows_out);
    return prdb_sdk_optimizer_execute_select_ast_v1(
        host, statement, schema, schema_len, max_candidates, max_batch_bytes,
        request, request_capacity, optimizer_response,
        optimizer_response_capacity, relation_response,
        relation_response_capacity, scan_response, scan_response_capacity,
        batch_response, batch_response_capacity, relation_out,
        relation_out_capacity, relation_columns, relation_column_capacity,
        source_cells, source_cell_capacity, projected_values,
        projected_value_capacity, emit_optimizer_projected_row_v1, &result,
        delivered_rows_out);
}

int32_t prdb_sdk_optimizer_execute_select_ast_to_result_v1(
    const prdb_host_api_v4 *host, const prdb_sdk_select_v2 *statement,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v2 *relation_columns,
    uint32_t relation_column_capacity, prdb_sdk_value_v1 *source_cells,
    uint32_t source_cell_capacity, prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity, uint8_t *result_record_buffer,
    uint64_t result_record_capacity, uint32_t *delivered_rows_out) {
    return execute_select_ast_to_result_internal(
        host, statement, NULL, 0, 0, schema, schema_len, max_candidates,
        max_batch_bytes, request, request_capacity, optimizer_response,
        optimizer_response_capacity, relation_response,
        relation_response_capacity, scan_response, scan_response_capacity,
        batch_response, batch_response_capacity, relation_out,
        relation_out_capacity, relation_columns, relation_column_capacity,
        source_cells, source_cell_capacity, projected_values,
        projected_value_capacity, result_record_buffer, result_record_capacity,
        delivered_rows_out);
}

int32_t prdb_sdk_optimizer_execute_select_ast_with_parameters_to_result_v1(
    const prdb_host_api_v4 *host, const prdb_sdk_select_v2 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v2 *relation_columns,
    uint32_t relation_column_capacity, prdb_sdk_value_v1 *source_cells,
    uint32_t source_cell_capacity, prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity, uint8_t *result_record_buffer,
    uint64_t result_record_capacity, uint32_t *delivered_rows_out) {
    return execute_select_ast_to_result_internal(
        host, statement, parameters, parameter_count, 1, schema, schema_len,
        max_candidates, max_batch_bytes, request, request_capacity,
        optimizer_response, optimizer_response_capacity, relation_response,
        relation_response_capacity, scan_response, scan_response_capacity,
        batch_response, batch_response_capacity, relation_out,
        relation_out_capacity, relation_columns, relation_column_capacity,
        source_cells, source_cell_capacity, projected_values,
        projected_value_capacity, result_record_buffer, result_record_capacity,
        delivered_rows_out);
}
