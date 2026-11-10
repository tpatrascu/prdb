#include "prdb_sdk_optimizer_plan_v1.h"
#include "prdb_sdk_sql_v1.h"
#include "prdb_sdk_sql_result_v1.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

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
    const uint8_t *bytes;
    if (depth > 32 || !read_u8(reader, &tag)) return 0;
    switch (tag) {
    case 0x00:
        return 1;
    case 0x01:
        return take(reader, 1, &bytes);
    case 0x02:
        return take(reader, 8, &bytes);
    case 0x03: {
        uint16_t length;
        return read_u16(reader, &length) && length <= 4096 &&
               take(reader, length, &bytes);
    }
    case 0x10:
        return take(reader, 16, &bytes);
    case 0x20: {
        uint8_t comparison;
        return read_u8(reader, &comparison) && comparison <= 5 &&
               skip_predicate_node(reader, depth + 1) &&
               skip_predicate_node(reader, depth + 1);
    }
    case 0x30: case 0x31: {
        uint8_t child_count;
        if (!read_u8(reader, &child_count) || child_count < 2 ||
            child_count > 16) return 0;
        for (uint32_t index = 0; index < child_count; ++index)
            if (!skip_predicate_node(reader, depth + 1)) return 0;
        return 1;
    }
    case 0x32:
        return skip_predicate_node(reader, depth + 1);
    default:
        return 0;
    }
}

static int32_t evaluate_predicate_node(
    plan_reader *reader, const prdb_sdk_optimizer_plan_v1 *plan,
    const prdb_sdk_value_v1 *source_cells, predicate_value *out,
    uint32_t depth, uint32_t *nodes) {
    uint8_t tag;
    const uint8_t *bytes;
    if (depth > 32 || ++*nodes > 256 || !read_u8(reader, &tag))
        return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
    switch (tag) {
    case 0x00:
        predicate_value_init(out, 0, NULL, 0);
        out->value.state = PRDB_SDK_VALUE_STATE_NULL;
        return PRDB_SDK_SQL_EVAL_OK;
    case 0x01:
        if (!take(reader, 1, &bytes) || bytes[0] > 1)
            return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
        predicate_value_init(out, PRDB_SDK_VALUE_TYPE_BOOLEAN, NULL, 1);
        out->bytes[0] = bytes[0];
        out->value.data = out->bytes;
        return PRDB_SDK_SQL_EVAL_OK;
    case 0x02: {
        uint64_t raw = 0;
        uint32_t high;
        if (!take(reader, 8, &bytes)) return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
        for (uint32_t index = 0; index < 8; ++index)
            raw = (raw << 8) | bytes[index];
        high = (uint32_t)(raw >> 32);
        if (!((high == 0 && (uint32_t)raw <= INT32_MAX) ||
              (high == UINT32_MAX && (uint32_t)raw >= UINT32_C(0x80000000))))
            return PRDB_SDK_SQL_EVAL_NUMERIC_OVERFLOW;
        predicate_value_init(out, PRDB_SDK_VALUE_TYPE_INTEGER, NULL, 4);
        memcpy(out->bytes, bytes + 4, 4);
        out->value.data = out->bytes;
        return PRDB_SDK_SQL_EVAL_OK;
    }
    case 0x03: {
        uint16_t length;
        if (!read_u16(reader, &length) || length > 4096 ||
            !take(reader, length, &bytes))
            return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
        predicate_value_init(out, PRDB_SDK_VALUE_TYPE_TEXT, bytes, length);
        return PRDB_SDK_SQL_EVAL_OK;
    }
    case 0x10: {
        if (!take(reader, 16, &bytes)) return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
        for (uint32_t index = 0; index < plan->scan_column_count; ++index) {
            const prdb_sdk_optimizer_plan_column_v1 *column =
                &plan->scan_columns[index];
            if (memcmp(bytes, column->column_id, 16) == 0) {
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
        if (!read_u8(reader, &comparison) || comparison > 5)
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
        uint8_t child_count;
        uint32_t truth = tag == 0x30 ? PRDB_SDK_SQL_TRUTH_TRUE
                                     : PRDB_SDK_SQL_TRUTH_FALSE;
        if (!read_u8(reader, &child_count) || child_count < 2 ||
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
        int32_t status = evaluate_predicate_node(
            reader, plan, source_cells, &child, depth + 1, nodes);
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
    default:
        return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
    }
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
    status = prdb_sdk_optimizer_decode_plan_with_filter_v1(
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
    status = prdb_sdk_optimizer_decode_plan_with_filter_v1(
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
    prdb_sdk_batch_v1 batch = {0};
    uint8_t cursor[16] = {0};
    uint64_t relation_response_len = 0;
    uint64_t scan_response_len = 0;
    uint64_t batch_response_len = 0;
    uint32_t total_delivered = 0;
    uint32_t scan_opened = 0;
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
    status = prdb_sdk_optimizer_scan_state_init_v1(
        plan_payload, plan_payload_len, &state, sizeof(state));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    status = prdb_sdk_optimizer_open_plan_scan_v1(
        host, plan_payload, plan_payload_len, relation_response,
        relation_response_capacity, &relation_response_len, scan_response,
        scan_response_capacity, &scan_response_len, relation_out,
        relation_out_capacity, relation_columns, relation_column_capacity,
        cursor, &scan_opened);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    if (scan_opened == 0) return PRDB_SDK_OPTIMIZER_PLAN_OK;
    cursor_open = 1;

    for (uint32_t page = 0;
         page < PRDB_SDK_OPTIMIZER_PLAN_MAX_NEXT_REQUESTS; ++page) {
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
        if (page + 1 == PRDB_SDK_OPTIMIZER_PLAN_MAX_NEXT_REQUESTS) {
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
    const uint8_t *bytes;
    if (value == NULL || prdb_sdk_value_validate_v1(value) !=
                             PRDB_SDK_VALUE_VALID ||
        !read_u8(reader, &tag)) return 0;
    if (value->state == PRDB_SDK_VALUE_STATE_NULL) return tag == 0x00;
    if (value->state != PRDB_SDK_VALUE_STATE_VALUE) return 0;
    switch (value->type_code) {
    case PRDB_SDK_VALUE_TYPE_BOOLEAN:
        return tag == 0x01 && take(reader, 1, &bytes) &&
               bytes[0] == value->data[0];
    case PRDB_SDK_VALUE_TYPE_INTEGER: {
        uint8_t expected[8];
        if (tag != 0x02 || !take(reader, sizeof(expected), &bytes)) return 0;
        memset(expected, (value->data[0] & 0x80) != 0 ? 0xff : 0, 4);
        memcpy(expected + 4, value->data, 4);
        return memcmp(bytes, expected, sizeof(expected)) == 0;
    }
    case PRDB_SDK_VALUE_TYPE_TEXT: {
        uint16_t length;
        if (tag != 0x03 || !read_u16(reader, &length) ||
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
    if (depth > PRDB_SDK_SQL_MAX_EXPRESSION_DEPTH ||
        node_index >= statement->predicate_count || seen[node_index] != 0 ||
        !read_u8(reader, &tag)) return 0;
    seen[node_index] = 1;
    node = &statement->predicates[node_index];
    switch (node->node_kind) {
    case PRDB_SDK_SQL_PREDICATE_COMPARE: {
        uint8_t comparison;
        uint8_t column_tag;
        const uint8_t *column_id;
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
            !read_u8(reader, &comparison) ||
            comparison != node->comparison - PRDB_SDK_SQL_COMPARE_EQUAL ||
            !read_u8(reader, &column_tag) || column_tag != 0x10 ||
            !take(reader, 16, &column_id)) return 0;
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
        uint8_t child_count;
        uint8_t expected_tag = node->node_kind == PRDB_SDK_SQL_PREDICATE_AND
                                   ? 0x30
                                   : 0x31;
        return tag == expected_tag && read_u8(reader, &child_count) &&
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
        return tag == 0x32 &&
               match_optimizer_filter_node(
                   reader, plan, metadata_response, metadata_response_len,
                   statement, parameters, parameter_count, node->left_child,
                   seen, depth + 1);
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
