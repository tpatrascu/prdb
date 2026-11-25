#include "prdb_sdk_optimizer_v1.h"
#include "prdb_query_abi_payload_codecs.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#define OPTIMIZER_REQUEST_MAX_BYTES PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES

static int ranges_overlap(const void *left, uint64_t left_len,
                          const void *right, uint64_t right_len);

static void write_u16(uint8_t *bytes, uint16_t value) {
    bytes[0] = (uint8_t)(value >> 8);
    bytes[1] = (uint8_t)value;
}

static void write_u32(uint8_t *bytes, uint32_t value) {
    bytes[0] = (uint8_t)(value >> 24);
    bytes[1] = (uint8_t)(value >> 16);
    bytes[2] = (uint8_t)(value >> 8);
    bytes[3] = (uint8_t)value;
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

static int span_is_identifier(const prdb_sdk_select_v1 *statement,
                              prdb_sdk_sql_span_v1 span) {
    return span.length != 0 && span.offset <= statement->sql_len &&
           span.length <= statement->sql_len - span.offset &&
           valid_identifier(statement->sql + span.offset, span.length);
}

static int predicate_append(uint8_t *output, uint64_t capacity,
                            uint64_t *offset, const uint8_t *bytes,
                            uint64_t length) {
    if (*offset > UINT64_MAX - length ||
        (output != NULL && (*offset > capacity ||
                            length > capacity - *offset))) return 0;
    if (output != NULL && length != 0)
        memcpy(output + *offset, bytes, (size_t)length);
    *offset += length;
    return 1;
}

static int predicate_append_u8(uint8_t *output, uint64_t capacity,
                              uint64_t *offset, uint8_t value) {
    return predicate_append(output, capacity, offset, &value, 1);
}

static int predicate_append_u16(uint8_t *output, uint64_t capacity,
                               uint64_t *offset, uint16_t value) {
    uint8_t bytes[2] = {(uint8_t)(value >> 8), (uint8_t)value};
    return predicate_append(output, capacity, offset, bytes, sizeof(bytes));
}

static int predicate_append_u64(uint8_t *output, uint64_t capacity,
                               uint64_t *offset, uint64_t value) {
    uint8_t bytes[8];
    for (uint32_t index = 0; index < 8; ++index)
        bytes[index] = (uint8_t)(value >> (56 - index * 8));
    return predicate_append(output, capacity, offset, bytes, sizeof(bytes));
}

static int encode_optimizer_parameter(
    const prdb_sdk_value_v1 *value, uint8_t *output, uint64_t capacity,
    uint64_t *offset) {
    int32_t valid = prdb_sdk_value_validate_v1(value);
    if (valid != PRDB_SDK_VALUE_VALID) return 0;
    if (value->state == PRDB_SDK_VALUE_STATE_NULL)
        return predicate_append_u8(output, capacity, offset, 0x00);
    if (value->state != PRDB_SDK_VALUE_STATE_VALUE) return 0;
    switch (value->type_code) {
    case PRDB_SDK_VALUE_TYPE_BOOLEAN:
        return predicate_append_u8(output, capacity, offset, 0x01) &&
               predicate_append(output, capacity, offset, value->data, 1);
    case PRDB_SDK_VALUE_TYPE_INTEGER: {
        uint8_t prefix[4];
        memset(prefix, (value->data[0] & 0x80) != 0 ? 0xff : 0,
               sizeof(prefix));
        return predicate_append_u8(output, capacity, offset, 0x02) &&
               predicate_append(output, capacity, offset, prefix,
                                sizeof(prefix)) &&
               predicate_append(output, capacity, offset, value->data, 4);
    }
    case PRDB_SDK_VALUE_TYPE_BIGINT:
        return predicate_append_u8(output, capacity, offset, 0x02) &&
               predicate_append(output, capacity, offset, value->data, 8);
    case PRDB_SDK_VALUE_TYPE_TEXT:
        return value->data_len <= 4096 && value->data_len <= UINT16_MAX &&
               predicate_append_u8(output, capacity, offset, 0x03) &&
               predicate_append_u16(output, capacity, offset,
                                    (uint16_t)value->data_len) &&
               predicate_append(output, capacity, offset, value->data,
                                value->data_len);
    case PRDB_SDK_VALUE_TYPE_DATE:
        return predicate_append_u8(output, capacity, offset, 0x04) &&
               predicate_append(output, capacity, offset, value->data, 4);
    case PRDB_SDK_VALUE_TYPE_UUID:
        return predicate_append_u8(output, capacity, offset, 0x05) &&
               predicate_append(output, capacity, offset, value->data, 16);
    case PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ:
        return predicate_append_u8(output, capacity, offset, 0x06) &&
               predicate_append(output, capacity, offset, value->data, 8);
    case PRDB_SDK_VALUE_TYPE_BYTEA:
        return value->data_len <= 4096 && value->data_len <= UINT16_MAX &&
               predicate_append_u8(output, capacity, offset, 0x07) &&
               predicate_append_u16(output, capacity, offset,
                                    (uint16_t)value->data_len) &&
               predicate_append(output, capacity, offset, value->data,
                                value->data_len);
    case PRDB_SDK_VALUE_TYPE_DECIMAL:
        return predicate_append_u8(output, capacity, offset, 0x08) &&
               predicate_append(output, capacity, offset, value->data, 16);
    default:
        return 2;
    }
}

static int encode_optimizer_predicate_node(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint32_t node_index, uint32_t depth, uint8_t *seen,
    uint8_t *output, uint64_t capacity, uint64_t *offset) {
    const prdb_sdk_predicate_node_v1 *node;
    int status;
    if (depth > PRDB_SDK_SQL_MAX_EXPRESSION_DEPTH ||
        node_index >= statement->predicate_count || seen[node_index] != 0)
        return 0;
    seen[node_index] = 1;
    node = &statement->predicates[node_index];
    switch (node->node_kind) {
    case PRDB_SDK_SQL_PREDICATE_COMPARE: {
        prdb_sdk_sql_span_v1 column = node->column;
        if (node->comparison < PRDB_SDK_SQL_COMPARE_EQUAL ||
            node->comparison > PRDB_SDK_SQL_COMPARE_GREATER_EQUAL ||
            node->left_child != 0 || node->right_child != 0 ||
            !span_is_identifier(statement, column) ||
            node->parameter_index == 0 ||
            node->parameter_index > parameter_count || parameters == NULL)
            return 0;
        if (!predicate_append_u8(output, capacity, offset, 0x20) ||
            !predicate_append_u8(output, capacity, offset,
                                 (uint8_t)(node->comparison -
                                           PRDB_SDK_SQL_COMPARE_EQUAL)) ||
            !predicate_append_u8(output, capacity, offset, 0x10) ||
            !predicate_append_u8(output, capacity, offset,
                                 (uint8_t)column.length) ||
            !predicate_append(output, capacity, offset,
                              statement->sql + column.offset, column.length))
            return 0;
        status = encode_optimizer_parameter(
            &parameters[node->parameter_index - 1], output, capacity, offset);
        return status;
    }
    case PRDB_SDK_SQL_PREDICATE_AND:
    case PRDB_SDK_SQL_PREDICATE_OR: {
        uint8_t tag = node->node_kind == PRDB_SDK_SQL_PREDICATE_AND
                          ? 0x30
                          : 0x31;
        if (node->comparison != 0 || node->column.offset != 0 ||
            node->column.length != 0 || node->parameter_index != 0)
            return 0;
        if (!predicate_append_u8(output, capacity, offset, tag) ||
            !predicate_append_u8(output, capacity, offset, 2)) return 0;
        status = encode_optimizer_predicate_node(
            statement, parameters, parameter_count, node->left_child,
            depth + 1, seen, output, capacity, offset);
        if (status != 1) return status;
        return encode_optimizer_predicate_node(
            statement, parameters, parameter_count, node->right_child,
            depth + 1, seen, output, capacity, offset);
    }
    case PRDB_SDK_SQL_PREDICATE_NOT:
        if (node->right_child != 0 || node->comparison != 0 ||
            node->column.offset != 0 || node->column.length != 0 ||
            node->parameter_index != 0) return 0;
        if (!predicate_append_u8(output, capacity, offset, 0x32)) return 0;
        return encode_optimizer_predicate_node(
            statement, parameters, parameter_count, node->left_child,
            depth + 1, seen, output, capacity, offset);
    case PRDB_SDK_SQL_PREDICATE_IS_NULL:
    case PRDB_SDK_SQL_PREDICATE_IS_NOT_NULL: {
        prdb_sdk_sql_span_v1 column = node->column;
        uint8_t tag = node->node_kind == PRDB_SDK_SQL_PREDICATE_IS_NULL
                          ? 0x40
                          : 0x41;
        if (node->comparison != 0 || node->left_child != 0 ||
            node->right_child != 0 || node->parameter_index != 0 ||
            !span_is_identifier(statement, column))
            return 0;
        return predicate_append_u8(output, capacity, offset, tag) &&
               predicate_append_u8(output, capacity, offset, 0x10) &&
               predicate_append_u8(output, capacity, offset,
                                   (uint8_t)column.length) &&
               predicate_append(output, capacity, offset,
                                statement->sql + column.offset,
                                column.length);
    }
    default:
        return 0;
    }
}

static int32_t optimizer_encode_select_internal(
    const prdb_sdk_select_v1 *statement, const uint8_t *schema,
    uint32_t schema_len, const prdb_sdk_value_v1 *parameters,
    uint32_t parameter_count, uint8_t *request, uint64_t request_capacity,
    uint64_t *request_len) {
    uint64_t encoded_len;
    uint64_t offset;
    uint64_t predicate_len = 0;
    uint32_t index;
    uint8_t seen[PRDB_SDK_SQL_MAX_PREDICATE_NODES] = {0};
    int predicate_status = 1;

    if (statement == NULL || schema == NULL || request == NULL ||
        request_len == NULL ||
        statement->struct_size != sizeof(prdb_sdk_select_v1) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->sql == NULL || statement->sql_len == 0 ||
        statement->sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        request_capacity > PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES ||
        parameter_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        (parameter_count != 0 && parameters == NULL) ||
        statement->projection_star > 1 || statement->has_limit > 1 ||
        statement->limit > PRDB_SDK_SQL_MAX_LIMIT ||
        !valid_identifier(schema, schema_len) ||
        ranges_overlap(request_len, sizeof(*request_len), request,
                       request_capacity) ||
        ranges_overlap(request_len, sizeof(*request_len), statement,
                       sizeof(*statement)) ||
        ranges_overlap(request_len, sizeof(*request_len), schema, schema_len) ||
        ranges_overlap(request_len, sizeof(*request_len), statement->sql,
                       statement->sql_len) ||
        ranges_overlap(request_len, sizeof(*request_len), parameters,
                       (uint64_t)parameter_count * sizeof(*parameters)) ||
        ranges_overlap(request, request_capacity, statement,
                       sizeof(*statement)) ||
        ranges_overlap(request, request_capacity, schema, schema_len) ||
        ranges_overlap(request, request_capacity, statement->sql,
                       statement->sql_len) ||
        ranges_overlap(request, request_capacity, parameters,
                       (uint64_t)parameter_count * sizeof(*parameters))) {
        return request_capacity > PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES
                   ? PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED
                   : PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    }
    if (statement->projection_star != 0 ||
        statement->predicate_count > PRDB_SDK_SQL_MAX_PREDICATE_NODES) {
        return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    }
    if (statement->predicate_count == 0) {
        if (statement->predicate_root != 0) return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    } else {
        if (statement->predicate_root >= statement->predicate_count)
            return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
        predicate_status = encode_optimizer_predicate_node(
            statement, parameters, parameter_count, statement->predicate_root,
            1, seen, NULL, 0, &predicate_len);
        if (predicate_status == 2) return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
        if (predicate_status != 1)
            return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
        for (index = 0; index < statement->predicate_count; ++index)
            if (seen[index] == 0)
                return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
        if (predicate_len > UINT32_C(16384))
            return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;
        memset(seen, 0, sizeof(seen));
    }
    if (statement->projection_count == 0 ||
        statement->projection_count > PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS) {
        return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    }
    if (!span_is_identifier(statement, statement->relation)) {
        return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    }
    encoded_len = 2 + 1 + 1 + 1 + schema_len + 1 +
                  statement->relation.length + 2 + 4 + predicate_len + 1 +
                  (statement->has_limit != 0 ? 4 : 0);
    for (index = 0; index < statement->projection_count; ++index) {
        prdb_sdk_sql_span_v1 projection = statement->projection[index];
        if (!span_is_identifier(statement, projection)) {
            return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
        }
        encoded_len += 1 + projection.length;
    }
    if (encoded_len > PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES) {
        return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;
    }
    if (request_capacity < encoded_len) {
        *request_len = encoded_len;
        return PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL;
    }

    offset = 0;
    prdb_encode_optimizer_select_request(request,
                                         PRDB_OPTIMIZER_SELECT_STATEMENT, 0);
    offset = PRDB_OPTIMIZER_SELECT_REQUEST_HEADER_BYTES;
    request[offset++] = (uint8_t)schema_len;
    memcpy(request + offset, schema, schema_len);
    offset += schema_len;
    request[offset++] = (uint8_t)statement->relation.length;
    memcpy(request + offset, statement->sql + statement->relation.offset,
           statement->relation.length);
    offset += statement->relation.length;
    write_u16(request + offset, (uint16_t)statement->projection_count);
    offset += 2;
    for (index = 0; index < statement->projection_count; ++index) {
        prdb_sdk_sql_span_v1 projection = statement->projection[index];
        request[offset++] = (uint8_t)projection.length;
        memcpy(request + offset, statement->sql + projection.offset,
               projection.length);
        offset += projection.length;
    }
    write_u32(request + offset, (uint32_t)predicate_len);
    offset += 4;
    if (predicate_len != 0) {
        uint64_t written = 0;
        predicate_status = encode_optimizer_predicate_node(
            statement, parameters, parameter_count, statement->predicate_root,
            1, seen, request + offset, request_capacity - offset, &written);
        if (predicate_status != 1 || written != predicate_len)
            return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
        offset += written;
    }
    request[offset++] = (uint8_t)(statement->has_limit != 0);
    if (statement->has_limit != 0) {
        write_u32(request + offset, statement->limit);
        offset += 4;
    }
    *request_len = offset;
    return PRDB_SDK_OPTIMIZER_ENCODE_OK;
}

static int span_equals(const uint8_t *sql, uint64_t sql_len,
                       prdb_sdk_sql_span_v1 span,
                       prdb_sdk_sql_span_v1 other) {
    return span.length == other.length && span.length != 0 &&
           span.offset <= sql_len && span.length <= sql_len - span.offset &&
           other.offset <= sql_len && other.length <= sql_len - other.offset &&
           memcmp(sql + span.offset, sql + other.offset, span.length) == 0;
}

static int append_identifier_span(const uint8_t *sql, uint64_t sql_len,
                                  prdb_sdk_sql_span_v1 span,
                                  uint8_t *output, uint64_t capacity,
                                  uint64_t *offset) {
    if (span.length == 0 || span.length > 63 || span.offset > sql_len ||
        span.length > sql_len - span.offset ||
        !valid_identifier(sql + span.offset, span.length))
        return 0;
    return predicate_append_u8(output, capacity, offset,
                               (uint8_t)span.length) &&
           predicate_append(output, capacity, offset, sql + span.offset,
                            span.length);
}

int32_t prdb_sdk_optimizer_encode_group_count_v1(
    const prdb_sdk_select_v1 *statement,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity, uint64_t *request_len) {
    uint64_t offset = 0;
    const prdb_sdk_select_v1 *base;
    const uint8_t *sql;
    uint64_t sql_len;
    uint8_t header[4] = {0, 1, PRDB_OPTIMIZER_GROUP_COUNT_STATEMENT, 0};
    uint64_t required;

    if (statement == NULL || schema == NULL || request == NULL ||
        request_len == NULL || statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->sql == NULL ||
        statement->sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        statement->has_group_by != 1 ||
        statement->aggregate_kind != PRDB_SDK_SQL_AGGREGATE_COUNT_STAR ||
        statement->projection_count != 1 ||
        statement->projection_star != 0 ||
        statement->has_limit > 1 || schema_len == 0 || schema_len > 63 ||
        !valid_identifier(schema, schema_len) ||
        request_capacity > OPTIMIZER_REQUEST_MAX_BYTES ||
        ranges_overlap(statement, sizeof(*statement), request, request_capacity) ||
        ranges_overlap(schema, schema_len, request, request_capacity) ||
        ranges_overlap(request_len, sizeof(*request_len), request,
                       request_capacity) ||
        ranges_overlap(request_len, sizeof(*request_len), statement,
                       sizeof(*statement)) ||
        ranges_overlap(request_len, sizeof(*request_len), schema, schema_len)) {
        return request_capacity > OPTIMIZER_REQUEST_MAX_BYTES
                   ? PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED
                   : PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    }
    base = statement;
    if (base->predicate_count != 0)
        return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    sql = base->sql;
    sql_len = base->sql_len;
    if (ranges_overlap(sql, sql_len, request, request_capacity) ||
        ranges_overlap(request_len, sizeof(*request_len), sql, sql_len)) {
        return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    }
    if (!span_equals(sql, sql_len, statement->group_by_column,
                     base->projection[0])) {
        return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    }
    offset = 0;
    if (!append_identifier_span(sql, sql_len, base->relation, NULL, 0,
                                &offset) ||
        !append_identifier_span(sql, sql_len, statement->group_by_column,
                                NULL, 0, &offset)) {
        return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    }
    required = sizeof(header) + 1 + schema_len + 1 + base->relation.length +
               1 + statement->group_by_column.length + 1 +
               (base->has_limit != 0 ? 4 : 0);
    if (required > OPTIMIZER_REQUEST_MAX_BYTES)
        return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;
    if (request_capacity < required) {
        *request_len = required;
        return PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL;
    }
    offset = 0;
#define APPEND_GROUP(ptr, len)                                                 \
    do {                                                                        \
        if (!predicate_append(request, request_capacity, &offset, (ptr), (len)))\
            return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;                    \
    } while (0)
    APPEND_GROUP(header, sizeof(header));
    if (!predicate_append_u8(request, request_capacity, &offset,
                             (uint8_t)schema_len))
        return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;
    APPEND_GROUP(schema, schema_len);
    if (!append_identifier_span(sql, sql_len, base->relation, request,
                                request_capacity, &offset) ||
        !append_identifier_span(sql, sql_len, statement->group_by_column,
                                request, request_capacity, &offset))
        return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    uint8_t has_limit = (uint8_t)base->has_limit;
    APPEND_GROUP(&has_limit, 1);
    if (has_limit) {
        uint8_t limit_bytes[4];
        write_u32(limit_bytes, base->limit);
        APPEND_GROUP(limit_bytes, sizeof(limit_bytes));
    }
#undef APPEND_GROUP
    *request_len = offset;
    return PRDB_SDK_OPTIMIZER_ENCODE_OK;
}

int32_t prdb_sdk_optimizer_encode_ordered_group_count_v1(
    const prdb_sdk_select_v1 *statement,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity, uint64_t *request_len) {
    const prdb_sdk_select_v1 *statement_v7;
    const prdb_sdk_select_v1 *statement_v9;
    const prdb_sdk_select_v1 *base;
    uint8_t header[4] = {
        0, 1, PRDB_OPTIMIZER_GROUP_COUNT_ORDERED_STATEMENT, 0};
    uint64_t offset = 0;
    uint64_t required;
    uint8_t seen = 0;
    if (statement == NULL || schema == NULL || request == NULL ||
        request_len == NULL || statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->order_count == 0 ||
        statement->order_count > PRDB_SDK_SQL_MAX_ORDER_KEYS ||
        schema_len == 0 || schema_len > 63 || !valid_identifier(schema, schema_len) ||
        request_capacity > OPTIMIZER_REQUEST_MAX_BYTES ||
        ranges_overlap(statement, sizeof(*statement), request, request_capacity) ||
        ranges_overlap(schema, schema_len, request, request_capacity) ||
        ranges_overlap(request_len, sizeof(*request_len), request, request_capacity) ||
        ranges_overlap(request_len, sizeof(*request_len), statement,
                       sizeof(*statement)) ||
        ranges_overlap(request_len, sizeof(*request_len), schema, schema_len))
        return request_capacity > OPTIMIZER_REQUEST_MAX_BYTES
                   ? PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED
                   : PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    statement_v7 = statement;
    statement_v9 = statement;
    base = statement_v7;
    if (statement_v7->struct_size != sizeof(*statement_v7) ||
        statement_v7->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement_v7->has_group_by != 1 ||
        statement_v7->aggregate_kind != PRDB_SDK_SQL_AGGREGATE_COUNT_STAR ||
        statement_v9->has_having != 0 || base->struct_size != sizeof(*base) ||
        base->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION || base->sql == NULL ||
        base->sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES || base->projection_count != 1 ||
        base->projection_star != 0 || base->predicate_count != 0 ||
        base->has_limit > 1 ||
        !span_equals(base->sql, base->sql_len, statement_v7->group_by_column,
                     base->projection[0]))
        return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    if (ranges_overlap(base->sql, base->sql_len, request, request_capacity) ||
        ranges_overlap(request_len, sizeof(*request_len), base->sql, base->sql_len))
        return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    for (uint32_t index = 0; index < statement->order_count; ++index) {
        const prdb_sdk_sql_order_item_v1 *item = &statement->order_items[index];
        uint8_t source;
        if (item->kind == PRDB_SDK_SQL_ORDER_GROUP_KEY)
            source = 0;
        else if (item->kind == PRDB_SDK_SQL_ORDER_AGGREGATE)
            source = 1;
        else
            return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
        if (item->direction > PRDB_SDK_SQL_ORDER_DESCENDING ||
            item->null_order > PRDB_SDK_SQL_NULLS_LAST ||
            (seen & (uint8_t)(1U << source)) != 0)
            return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
        seen |= (uint8_t)(1U << source);
    }
    if (!span_is_identifier(base, base->relation) ||
        !span_is_identifier(base, statement_v7->group_by_column))
        return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    required = sizeof(header) + 1 + schema_len + 1 + base->relation.length +
               1 + statement_v7->group_by_column.length + 1 +
               (uint64_t)statement->order_count * 3 + 1 +
               (base->has_limit != 0 ? 4 : 0);
    if (required > OPTIMIZER_REQUEST_MAX_BYTES)
        return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;
    *request_len = required;
    if (request_capacity < required)
        return PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL;
    memcpy(request + offset, header, sizeof(header));
    offset += sizeof(header);
    request[offset++] = (uint8_t)schema_len;
    memcpy(request + offset, schema, schema_len);
    offset += schema_len;
    if (!append_identifier_span(base->sql, base->sql_len, base->relation,
                                request, request_capacity, &offset) ||
        !append_identifier_span(base->sql, base->sql_len,
                                statement_v7->group_by_column, request,
                                request_capacity, &offset))
        return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    request[offset++] = (uint8_t)statement->order_count;
    for (uint32_t index = 0; index < statement->order_count; ++index) {
        const prdb_sdk_sql_order_item_v1 *item = &statement->order_items[index];
        request[offset++] = (uint8_t)(item->kind == PRDB_SDK_SQL_ORDER_AGGREGATE);
        request[offset++] = (uint8_t)item->direction;
        request[offset++] = (uint8_t)(item->null_order != PRDB_SDK_SQL_NULLS_LAST);
    }
    request[offset++] = (uint8_t)base->has_limit;
    if (base->has_limit != 0) {
        write_u32(request + offset, base->limit);
        offset += 4;
    }
    if (offset != required) {
        *request_len = 0;
        return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    }
    return PRDB_SDK_OPTIMIZER_ENCODE_OK;
}

static uint8_t optimizer_ascii_fold(uint8_t byte) {
    return byte >= 'A' && byte <= 'Z' ? (uint8_t)(byte + ('a' - 'A')) : byte;
}

static int optimizer_spans_equal_folded(
    const uint8_t *sql, uint64_t sql_len, prdb_sdk_sql_span_v1 left,
    prdb_sdk_sql_span_v1 right) {
    if (left.length != right.length || left.length == 0 ||
        left.offset > sql_len || left.length > sql_len - left.offset ||
        right.offset > sql_len || right.length > sql_len - right.offset)
        return 0;
    for (uint32_t index = 0; index < left.length; ++index) {
        if (optimizer_ascii_fold(sql[left.offset + index]) !=
            optimizer_ascii_fold(sql[right.offset + index]))
            return 0;
    }
    return 1;
}

int32_t prdb_sdk_optimizer_encode_composite_group_count_v1(
    const prdb_sdk_select_v1 *statement,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity, uint64_t *request_len) {
    const prdb_sdk_select_v1 *base;
    const uint8_t *sql;
    uint64_t sql_len;
    uint64_t offset = 0;
    uint64_t required = 0;
    uint8_t header[4] = {
        0, 1, PRDB_OPTIMIZER_COMPOSITE_GROUP_COUNT_STATEMENT, 0};

    if (statement == NULL || schema == NULL || request == NULL ||
        request_len == NULL || statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->sql == NULL ||
        statement->sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        statement->group_key_count < PRDB_SDK_SQL_MIN_GROUP_KEYS ||
        statement->group_key_count > PRDB_SDK_SQL_MAX_GROUP_KEYS ||
        statement->has_group_by != 1 ||
        statement->aggregate_kind !=
            PRDB_SDK_SQL_AGGREGATE_COUNT_STAR ||
        statement->projection_count != 1 ||
        statement->projection_star != 0 ||
        statement->predicate_count != 0 ||
        statement->has_limit > 1 || schema_len == 0 ||
        schema_len > 63 || !valid_identifier(schema, schema_len) ||
        request_capacity > OPTIMIZER_REQUEST_MAX_BYTES ||
        ranges_overlap(statement, sizeof(*statement), request,
                       request_capacity) ||
        ranges_overlap(schema, schema_len, request, request_capacity) ||
        ranges_overlap(request_len, sizeof(*request_len), request,
                       request_capacity) ||
        ranges_overlap(request_len, sizeof(*request_len), statement,
                       sizeof(*statement)) ||
        ranges_overlap(request_len, sizeof(*request_len), schema, schema_len))
        return request_capacity > OPTIMIZER_REQUEST_MAX_BYTES
                   ? PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED
                   : PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;

    base = statement;
    sql = base->sql;
    sql_len = base->sql_len;
    if (ranges_overlap(sql, sql_len, request, request_capacity) ||
        ranges_overlap(request_len, sizeof(*request_len), sql, sql_len))
        return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    for (uint32_t index = 0; index < statement->group_key_count; ++index) {
        if (!span_is_identifier(base, statement->group_keys[index]) ||
            !span_is_identifier(base, statement->projection_keys[index]) ||
            !optimizer_spans_equal_folded(
                sql, sql_len, statement->group_keys[index],
                statement->projection_keys[index]))
            return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
        for (uint32_t prior = 0; prior < index; ++prior) {
            if (optimizer_spans_equal_folded(
                    sql, sql_len, statement->group_keys[index],
                    statement->group_keys[prior]))
                return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
        }
        if (!append_identifier_span(sql, sql_len, statement->group_keys[index],
                                    NULL, 0, &required))
            return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    }
    uint64_t request_bytes = sizeof(header) + 1 + schema_len +
                             1 + base->relation.length + 1 + required + 1 +
                             (base->has_limit ? 4 : 0);
    if (!span_is_identifier(base, base->relation))
        return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    if (request_bytes > OPTIMIZER_REQUEST_MAX_BYTES)
        return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;
    if (request_capacity < request_bytes) {
        *request_len = request_bytes;
        return PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL;
    }
    if (!predicate_append(request, request_capacity, &offset, header,
                          sizeof(header)) ||
        !predicate_append_u8(request, request_capacity, &offset,
                             (uint8_t)schema_len) ||
        !predicate_append(request, request_capacity, &offset, schema,
                          schema_len) ||
        !append_identifier_span(sql, sql_len, base->relation, request,
                                request_capacity, &offset) ||
        !predicate_append_u8(request, request_capacity, &offset,
                             (uint8_t)statement->group_key_count))
        return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;
    for (uint32_t index = 0; index < statement->group_key_count; ++index) {
        if (!append_identifier_span(sql, sql_len, statement->group_keys[index],
                                    request, request_capacity, &offset))
            return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;
    }
    uint8_t has_limit = (uint8_t)base->has_limit;
    if (!predicate_append_u8(request, request_capacity, &offset, has_limit))
        return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;
    if (has_limit) {
        uint8_t limit_bytes[4];
        write_u32(limit_bytes, base->limit);
        if (!predicate_append(request, request_capacity, &offset, limit_bytes,
                              sizeof(limit_bytes)))
            return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;
    }
    *request_len = offset;
    return PRDB_SDK_OPTIMIZER_ENCODE_OK;
}

int32_t prdb_sdk_optimizer_encode_composite_group_aggregate_v1(
    const prdb_sdk_select_v1 *statement, const uint8_t *schema,
    uint32_t schema_len, uint8_t *request, uint64_t request_capacity,
    uint64_t *request_len) {
    const prdb_sdk_select_v1 *aggregate_statement;
    const prdb_sdk_select_v1 *base;
    const uint8_t *sql;
    uint64_t sql_len, offset = 0, required;
    uint8_t header[4] = {
        0, 1, PRDB_OPTIMIZER_COMPOSITE_GROUP_AGGREGATE_STATEMENT, 0};
    if (statement == NULL || schema == NULL || request == NULL ||
        request_len == NULL || statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->group_key_count < PRDB_SDK_SQL_MIN_GROUP_KEYS ||
        statement->group_key_count > PRDB_SDK_SQL_MAX_GROUP_KEYS ||
        statement->has_group_by != 1 ||
        statement->aggregate_kind < PRDB_SDK_SQL_AGGREGATE_COUNT_STAR ||
        statement->aggregate_kind > PRDB_SDK_SQL_AGGREGATE_COUNT_COLUMN ||
        statement->projection_count != 1 ||
        statement->projection_star != 0 ||
        statement->predicate_count != 0 ||
        statement->has_limit > 1 || schema_len == 0 ||
        schema_len > 63 || !valid_identifier(schema, schema_len) ||
        request_capacity > OPTIMIZER_REQUEST_MAX_BYTES)
        return request_capacity > OPTIMIZER_REQUEST_MAX_BYTES
                   ? PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED
                   : PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    aggregate_statement = statement;
    base = aggregate_statement;
    sql = base->sql;
    sql_len = base->sql_len;
    if (sql == NULL || sql_len == 0 ||
        sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        !span_is_identifier(base, base->relation) ||
        (aggregate_statement->aggregate_kind ==
                 PRDB_SDK_SQL_AGGREGATE_COUNT_STAR
             ? aggregate_statement->aggregate_column.length != 0
             : !span_is_identifier(base, aggregate_statement->aggregate_column)))
        return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    if (ranges_overlap(statement, sizeof(*statement), request,
                       request_capacity) ||
        ranges_overlap(schema, schema_len, request, request_capacity) ||
        ranges_overlap(sql, sql_len, request, request_capacity) ||
        ranges_overlap(request_len, sizeof(*request_len), request,
                       request_capacity) ||
        ranges_overlap(request_len, sizeof(*request_len), statement,
                       sizeof(*statement)) ||
        ranges_overlap(request_len, sizeof(*request_len), schema, schema_len) ||
        ranges_overlap(request_len, sizeof(*request_len), sql, sql_len))
        return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    required = sizeof(header) + 1 + schema_len + 1 + base->relation.length + 1;
    for (uint32_t index = 0; index < statement->group_key_count; ++index) {
        if (!span_is_identifier(base, statement->group_keys[index]) ||
            !span_is_identifier(base, statement->projection_keys[index]) ||
            !optimizer_spans_equal_folded(
                sql, sql_len, statement->group_keys[index],
                statement->projection_keys[index]))
            return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
        for (uint32_t prior = 0; prior < index; ++prior) {
            if (optimizer_spans_equal_folded(
                    sql, sql_len, statement->group_keys[index],
                    statement->group_keys[prior]))
                return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
        }
        if (!append_identifier_span(sql, sql_len, statement->group_keys[index],
                                    NULL, 0, &required))
            return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    }
    required += 1;
    if (aggregate_statement->aggregate_kind !=
        PRDB_SDK_SQL_AGGREGATE_COUNT_STAR) {
        if (!append_identifier_span(sql, sql_len,
                                    aggregate_statement->aggregate_column,
                                    NULL, 0, &required))
            return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
        required += 1;
    }
    required += 1 + (base->has_limit ? 4 : 0);
    if (required > OPTIMIZER_REQUEST_MAX_BYTES)
        return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;
    if (request_capacity < required) {
        *request_len = required;
        return PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL;
    }
    predicate_append(request, request_capacity, &offset, header, sizeof(header));
    predicate_append_u8(request, request_capacity, &offset, (uint8_t)schema_len);
    predicate_append(request, request_capacity, &offset, schema, schema_len);
    append_identifier_span(sql, sql_len, base->relation, request,
                           request_capacity, &offset);
    predicate_append_u8(request, request_capacity, &offset,
                        (uint8_t)statement->group_key_count);
    for (uint32_t index = 0; index < statement->group_key_count; ++index)
        append_identifier_span(sql, sql_len, statement->group_keys[index],
                               request, request_capacity, &offset);
    predicate_append_u8(request, request_capacity, &offset,
                        (uint8_t)aggregate_statement->aggregate_kind);
    if (aggregate_statement->aggregate_kind !=
        PRDB_SDK_SQL_AGGREGATE_COUNT_STAR)
        append_identifier_span(sql, sql_len,
                               aggregate_statement->aggregate_column, request,
                               request_capacity, &offset);
    predicate_append_u8(request, request_capacity, &offset,
                        (uint8_t)base->has_limit);
    if (base->has_limit) {
        uint8_t limit_bytes[4];
        write_u32(limit_bytes, base->limit);
        predicate_append(request, request_capacity, &offset, limit_bytes,
                         sizeof(limit_bytes));
    }
    *request_len = offset;
    return PRDB_SDK_OPTIMIZER_ENCODE_OK;
}

int32_t prdb_sdk_optimizer_encode_filtered_composite_group_aggregate_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity, uint64_t *request_len) {
    const prdb_sdk_select_v1 *aggregate;
    const prdb_sdk_select_v1 *base;
    uint8_t header[4] = {0, 1, PRDB_OPTIMIZER_FILTERED_COMPOSITE_GROUP_AGGREGATE_STATEMENT, 0};
    uint8_t seen[PRDB_SDK_SQL_MAX_PREDICATE_NODES] = {0};
    uint64_t predicate_len = 0, offset = 0, required;
    int predicate_status;
    if (statement == NULL || schema == NULL || request == NULL || request_len == NULL ||
        statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->group_key_count < 1 ||
        statement->group_key_count > PRDB_SDK_SQL_MAX_GROUP_KEYS ||
        statement->has_group_by != 1 || statement->projection_count != 1 ||
        statement->projection_star != 0 || statement->predicate_count == 0 ||
        statement->predicate_count > PRDB_SDK_SQL_MAX_PREDICATE_NODES ||
        statement->predicate_root >= statement->predicate_count ||
        statement->has_limit > 1 || parameter_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        (parameter_count != 0 && parameters == NULL) || schema_len == 0 || schema_len > 63 ||
        !valid_identifier(schema, schema_len) || request_capacity > OPTIMIZER_REQUEST_MAX_BYTES)
        return request_capacity > OPTIMIZER_REQUEST_MAX_BYTES
                   ? PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED
                   : PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    aggregate = statement;
    base = aggregate;
    if (base->sql == NULL || base->sql_len == 0 ||
        base->sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        !span_is_identifier(base, base->relation) ||
        aggregate->aggregate_kind < PRDB_SDK_SQL_AGGREGATE_COUNT_STAR ||
        aggregate->aggregate_kind > PRDB_SDK_SQL_AGGREGATE_COUNT_COLUMN ||
        (aggregate->aggregate_kind == PRDB_SDK_SQL_AGGREGATE_COUNT_STAR
             ? aggregate->aggregate_column.length != 0
             : !span_is_identifier(base, aggregate->aggregate_column)))
        return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    if (ranges_overlap(request, request_capacity, statement, sizeof(*statement)) ||
        ranges_overlap(request, request_capacity, schema, schema_len) ||
        ranges_overlap(request, request_capacity, base->sql, base->sql_len) ||
        ranges_overlap(request_len, sizeof(*request_len), request, request_capacity) ||
        ranges_overlap(request_len, sizeof(*request_len), statement, sizeof(*statement)) ||
        ranges_overlap(request_len, sizeof(*request_len), schema, schema_len) ||
        ranges_overlap(request_len, sizeof(*request_len), base->sql, base->sql_len) ||
        ranges_overlap(request, request_capacity, parameters,
                       (uint64_t)parameter_count * sizeof(*parameters)) ||
        ranges_overlap(request_len, sizeof(*request_len), parameters,
                       (uint64_t)parameter_count * sizeof(*parameters)))
        return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    for (uint32_t index = 0; index < parameter_count; ++index) {
        if (parameters[index].data_len != 0 &&
            (ranges_overlap(request, request_capacity, parameters[index].data,
                            parameters[index].data_len) ||
             ranges_overlap(request_len, sizeof(*request_len), parameters[index].data,
                            parameters[index].data_len)))
            return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    }
    predicate_status = encode_optimizer_predicate_node(
        base, parameters, parameter_count, base->predicate_root, 1, seen,
        NULL, 0, &predicate_len);
    if (predicate_status == 2) return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    if (predicate_status != 1 || predicate_len == 0 || predicate_len > UINT32_C(16384))
        return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    for (uint32_t index = 0; index < base->predicate_count; ++index)
        if (seen[index] == 0) return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    required = sizeof(header) + 1 + schema_len + 1 + base->relation.length + 1;
    for (uint32_t index = 0; index < statement->group_key_count; ++index) {
        if (!span_is_identifier(base, statement->group_keys[index]) ||
            !span_is_identifier(base, statement->projection_keys[index]) ||
            !optimizer_spans_equal_folded(base->sql, base->sql_len,
                                          statement->group_keys[index],
                                          statement->projection_keys[index]) ||
            !append_identifier_span(base->sql, base->sql_len,
                                    statement->group_keys[index], NULL, 0,
                                    &required))
            return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
        for (uint32_t prior = 0; prior < index; ++prior)
            if (optimizer_spans_equal_folded(base->sql, base->sql_len,
                                             statement->group_keys[index],
                                             statement->group_keys[prior]))
                return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    }
    required += 1;
    if (aggregate->aggregate_kind != PRDB_SDK_SQL_AGGREGATE_COUNT_STAR) {
        if (!append_identifier_span(base->sql, base->sql_len,
                                    aggregate->aggregate_column, NULL, 0,
                                    &required))
            return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
        required += 1;
    }
    required += 4 + predicate_len + 1 + (base->has_limit ? 4 : 0);
    if (required > OPTIMIZER_REQUEST_MAX_BYTES)
        return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;
    if (request_capacity < required) {
        *request_len = required;
        return PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL;
    }
    predicate_append(request, request_capacity, &offset, header, sizeof(header));
    predicate_append_u8(request, request_capacity, &offset, (uint8_t)schema_len);
    predicate_append(request, request_capacity, &offset, schema, schema_len);
    append_identifier_span(base->sql, base->sql_len, base->relation, request,
                           request_capacity, &offset);
    predicate_append_u8(request, request_capacity, &offset,
                        (uint8_t)statement->group_key_count);
    for (uint32_t index = 0; index < statement->group_key_count; ++index)
        append_identifier_span(base->sql, base->sql_len,
                               statement->group_keys[index], request,
                               request_capacity, &offset);
    predicate_append_u8(request, request_capacity, &offset,
                        (uint8_t)aggregate->aggregate_kind);
    if (aggregate->aggregate_kind != PRDB_SDK_SQL_AGGREGATE_COUNT_STAR)
        append_identifier_span(base->sql, base->sql_len,
                               aggregate->aggregate_column, request,
                               request_capacity, &offset);
    write_u32(request + offset, (uint32_t)predicate_len);
    offset += 4;
    memset(seen, 0, sizeof(seen));
    {
        uint64_t written = 0;
        predicate_status = encode_optimizer_predicate_node(
            base, parameters, parameter_count, base->predicate_root, 1, seen,
            request + offset, request_capacity - offset, &written);
        if (predicate_status != 1 || written != predicate_len)
            return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
        offset += written;
    }
    predicate_append_u8(request, request_capacity, &offset,
                        (uint8_t)base->has_limit);
    if (base->has_limit) {
        uint8_t limit_bytes[4];
        write_u32(limit_bytes, base->limit);
        predicate_append(request, request_capacity, &offset, limit_bytes,
                         sizeof(limit_bytes));
    }
    *request_len = offset;
    return PRDB_SDK_OPTIMIZER_ENCODE_OK;
}

int32_t prdb_sdk_optimizer_encode_ordered_filtered_composite_group_aggregate_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity, uint64_t *request_len) {
    uint8_t intermediate[OPTIMIZER_REQUEST_MAX_BYTES];
    uint64_t intermediate_len = 0;
    uint64_t old_limit_bytes;
    uint64_t rewritten_len;
    uint64_t output_offset;
    uint8_t header[4] = {0, 1, PRDB_OPTIMIZER_ORDERED_FILTERED_COMPOSITE_GROUP_AGGREGATE_STATEMENT, 0};
    uint8_t seen_keys = 0;
    uint32_t aggregate_seen = 0;
    const prdb_sdk_select_v1 *aggregate;
    int32_t status;
    if (statement == NULL || request == NULL || request_len == NULL ||
        statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->has_having != 0 || statement->order_count == 0 ||
        statement->order_count > PRDB_SDK_SQL_MAX_ORDER_KEYS ||
        statement->predicate_count == 0)
        return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    aggregate = statement;
    status = prdb_sdk_optimizer_encode_filtered_composite_group_aggregate_v1(
        aggregate, parameters, parameter_count, schema, schema_len,
        intermediate, sizeof(intermediate), &intermediate_len);
    if (status != PRDB_SDK_OPTIMIZER_ENCODE_OK) return status;
    old_limit_bytes = aggregate->has_limit ? 5 : 1;
    if (intermediate_len < old_limit_bytes + 1)
        return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    rewritten_len = intermediate_len + 1;
    for (uint32_t index = 0; index < statement->order_count; ++index) {
        const prdb_sdk_sql_order_item_v1 *item = &statement->order_items[index];
        uint64_t item_len;
        if (item->direction > PRDB_SDK_SQL_ORDER_DESCENDING ||
            item->null_order > PRDB_SDK_SQL_NULLS_LAST)
            return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
        if (item->kind == PRDB_SDK_SQL_ORDER_GROUP_KEY &&
            item->group_key_index < aggregate->group_key_count &&
            !(seen_keys & (1u << item->group_key_index))) {
            seen_keys |= (uint8_t)(1u << item->group_key_index);
            item_len = 4;
        }
        else if (item->kind == PRDB_SDK_SQL_ORDER_AGGREGATE &&
                 item->group_key_index == 0 && !aggregate_seen) {
            aggregate_seen = 1;
            item_len = 3;
        }
        else
            return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
        if (rewritten_len > OPTIMIZER_REQUEST_MAX_BYTES - item_len)
            return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;
        rewritten_len += item_len;
    }
    if (rewritten_len > OPTIMIZER_REQUEST_MAX_BYTES)
        return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;
    *request_len = rewritten_len;
    if (request_capacity < rewritten_len)
        return PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL;
    if (ranges_overlap(request, request_capacity, statement, sizeof(*statement)) ||
        ranges_overlap(request, request_capacity, schema, schema_len) ||
        ranges_overlap(request, request_capacity, aggregate->sql,
                       aggregate->sql_len) ||
        ranges_overlap(request_len, sizeof(*request_len), request, request_capacity) ||
        ranges_overlap(request_len, sizeof(*request_len), statement, sizeof(*statement)) ||
        ranges_overlap(request_len, sizeof(*request_len), schema, schema_len) ||
        ranges_overlap(request_len, sizeof(*request_len), aggregate->sql,
                       aggregate->sql_len) ||
        ranges_overlap(request_len, sizeof(*request_len), parameters,
                       (uint64_t)parameter_count * sizeof(*parameters)) ||
        ranges_overlap(request, request_capacity, parameters,
                       (uint64_t)parameter_count * sizeof(*parameters)))
        return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    for (uint32_t index = 0; index < parameter_count; ++index) {
        if (parameters[index].data_len != 0 &&
            (ranges_overlap(request, request_capacity, parameters[index].data,
                            parameters[index].data_len) ||
             ranges_overlap(request_len, sizeof(*request_len), parameters[index].data,
                            parameters[index].data_len)))
            return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    }
    memcpy(request, intermediate, (size_t)(intermediate_len - old_limit_bytes));
    memcpy(request, header, sizeof(header));
    output_offset = intermediate_len - old_limit_bytes;
    request[output_offset++] = (uint8_t)statement->order_count;
    for (uint32_t index = 0; index < statement->order_count; ++index) {
        const prdb_sdk_sql_order_item_v1 *item = &statement->order_items[index];
        request[output_offset++] = item->kind == PRDB_SDK_SQL_ORDER_GROUP_KEY ? 0 : 1;
        if (item->kind == PRDB_SDK_SQL_ORDER_GROUP_KEY)
            request[output_offset++] = (uint8_t)item->group_key_index;
        request[output_offset++] = (uint8_t)item->direction;
        request[output_offset++] = (uint8_t)(
            item->null_order == PRDB_SDK_SQL_NULL_ORDER_DEFAULT ||
            item->null_order == PRDB_SDK_SQL_NULLS_FIRST);
    }
    memcpy(request + output_offset, intermediate + intermediate_len - old_limit_bytes,
           (size_t)old_limit_bytes);
    return PRDB_SDK_OPTIMIZER_ENCODE_OK;
}

int32_t prdb_sdk_optimizer_encode_ordered_composite_group_count_v1(
    const prdb_sdk_select_v1 *statement, const uint8_t *schema,
    uint32_t schema_len, uint8_t *request, uint64_t request_capacity,
    uint64_t *request_len) {
    const prdb_sdk_select_v1 *base;
    const uint8_t *sql;
    uint64_t offset = 0;
    uint8_t header[4] = {0, 1, PRDB_OPTIMIZER_COMPOSITE_GROUP_COUNT_ORDERED_STATEMENT, 0};
    uint16_t seen_keys = 0;
    uint32_t seen_count = 0;
    if (statement == NULL || schema == NULL || request == NULL || request_len == NULL ||
        statement->struct_size != sizeof(*statement) || statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) || statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) || statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->group_key_count < 2 || statement->group_key_count > 8 ||
        statement->aggregate_kind != PRDB_SDK_SQL_AGGREGATE_COUNT_STAR ||
        statement->projection_count != 1 ||
        statement->projection_star != 0 ||
        statement->predicate_count != 0 ||
        statement->has_having != 0 || statement->has_limit > 1 ||
        statement->order_count == 0 || statement->order_count > 9 || schema_len == 0 || schema_len > 63 ||
        !valid_identifier(schema, schema_len) || request_capacity > OPTIMIZER_REQUEST_MAX_BYTES)
        return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    base = statement;
    sql = base->sql;
    if (sql == NULL || base->sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        !span_is_identifier(base, base->relation)) return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    if (ranges_overlap(statement, sizeof(*statement), request, request_capacity) ||
        ranges_overlap(schema, schema_len, request, request_capacity) ||
        ranges_overlap(sql, base->sql_len, request, request_capacity) ||
        ranges_overlap(request_len, sizeof(*request_len), request, request_capacity) ||
        ranges_overlap(request_len, sizeof(*request_len), statement, sizeof(*statement)) ||
        ranges_overlap(request_len, sizeof(*request_len), schema, schema_len) ||
        ranges_overlap(request_len, sizeof(*request_len), sql, base->sql_len))
        return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    uint64_t required = 4 + 1 + schema_len + 1 + base->relation.length + 1 + 1;
    for (uint32_t i = 0; i < statement->group_key_count; ++i) {
        if (!span_is_identifier(base, statement->group_keys[i]) ||
            !span_is_identifier(base, statement->projection_keys[i])) return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
        for (uint32_t j = 0; j < i; ++j)
            if (optimizer_spans_equal_folded(sql, base->sql_len, statement->group_keys[i], statement->group_keys[j])) return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
        if (!append_identifier_span(sql, base->sql_len, statement->group_keys[i], NULL, 0, &required)) return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    }
    required += 1 + (base->has_limit ? 4 : 0);
    for (uint32_t i = 0; i < statement->order_count; ++i) {
        const prdb_sdk_sql_order_item_v1 *item = &statement->order_items[i];
        if (item->direction > PRDB_SDK_SQL_ORDER_DESCENDING || item->null_order > PRDB_SDK_SQL_NULLS_LAST) return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
        if (item->kind == PRDB_SDK_SQL_ORDER_GROUP_KEY) {
            if (item->group_key_index >= statement->group_key_count || (seen_keys & (1u << item->group_key_index))) return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
            seen_keys |= (uint16_t)(1u << item->group_key_index);
            required += 4;
        } else if (item->kind == PRDB_SDK_SQL_ORDER_AGGREGATE && item->group_key_index == 0 && !seen_count) {
            seen_count = 1; required += 3;
        } else return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    }
    required += base->has_limit ? 0 : 0;
    if (required > OPTIMIZER_REQUEST_MAX_BYTES) return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;
    if (request_capacity < required) { *request_len = required; return PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL; }
    predicate_append(request, request_capacity, &offset, header, sizeof(header));
    predicate_append_u8(request, request_capacity, &offset, (uint8_t)schema_len);
    predicate_append(request, request_capacity, &offset, schema, schema_len);
    append_identifier_span(sql, base->sql_len, base->relation, request, request_capacity, &offset);
    predicate_append_u8(request, request_capacity, &offset, (uint8_t)statement->group_key_count);
    for (uint32_t i = 0; i < statement->group_key_count; ++i) append_identifier_span(sql, base->sql_len, statement->group_keys[i], request, request_capacity, &offset);
    predicate_append_u8(request, request_capacity, &offset, (uint8_t)statement->order_count);
    for (uint32_t i = 0; i < statement->order_count; ++i) {
        const prdb_sdk_sql_order_item_v1 *item = &statement->order_items[i];
        predicate_append_u8(request, request_capacity, &offset, item->kind == PRDB_SDK_SQL_ORDER_GROUP_KEY ? 0 : 1);
        if (item->kind == PRDB_SDK_SQL_ORDER_GROUP_KEY) predicate_append_u8(request, request_capacity, &offset, (uint8_t)item->group_key_index);
        predicate_append_u8(request, request_capacity, &offset, (uint8_t)item->direction);
        predicate_append_u8(request, request_capacity, &offset, (uint8_t)(item->null_order == PRDB_SDK_SQL_NULL_ORDER_DEFAULT || item->null_order == PRDB_SDK_SQL_NULLS_FIRST));
    }
    predicate_append_u8(request, request_capacity, &offset, (uint8_t)base->has_limit);
    if (base->has_limit) { uint8_t limit[4]; write_u32(limit, base->limit); predicate_append(request, request_capacity, &offset, limit, 4); }
    *request_len = offset;
    return PRDB_SDK_OPTIMIZER_ENCODE_OK;
}

int32_t prdb_sdk_optimizer_encode_ordered_composite_group_aggregate_v1(
    const prdb_sdk_select_v1 *statement, const uint8_t *schema,
    uint32_t schema_len, uint8_t *request, uint64_t request_capacity,
    uint64_t *request_len) {
    uint8_t base_request[OPTIMIZER_REQUEST_MAX_BYTES];
    const prdb_sdk_select_v1 *base;
    uint64_t base_len = 0;
    uint64_t insert_len = 1;
    uint64_t required;
    uint64_t limit_len;
    uint64_t limit_offset;
    uint64_t output_offset = 0;
    uint16_t seen_keys = 0;
    uint32_t seen_aggregate = 0;
    int32_t status;
    if (statement == NULL || schema == NULL || request == NULL ||
        request_len == NULL ||
        statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->group_key_count < 2 ||
        statement->group_key_count > 8 ||
        statement->aggregate_kind <
            PRDB_SDK_SQL_AGGREGATE_COUNT_STAR ||
        statement->aggregate_kind >
            PRDB_SDK_SQL_AGGREGATE_COUNT_COLUMN ||
        statement->has_having != 0 || statement->order_count == 0 ||
        statement->order_count > PRDB_SDK_SQL_MAX_ORDER_KEYS ||
        statement->has_limit > 1 || schema_len == 0 ||
        schema_len > 63 || !valid_identifier(schema, schema_len) ||
        request_capacity > OPTIMIZER_REQUEST_MAX_BYTES)
        return request_capacity > OPTIMIZER_REQUEST_MAX_BYTES
                   ? PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED
                   : PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    base = statement;
    if (ranges_overlap(statement, sizeof(*statement), request,
                       request_capacity) ||
        ranges_overlap(schema, schema_len, request, request_capacity) ||
        ranges_overlap(request_len, sizeof(*request_len), request,
                       request_capacity) ||
        ranges_overlap(request_len, sizeof(*request_len), statement,
                       sizeof(*statement)) ||
        ranges_overlap(request_len, sizeof(*request_len), schema, schema_len) ||
        (base->sql != NULL &&
         (ranges_overlap(base->sql, base->sql_len, request,
                         request_capacity) ||
          ranges_overlap(request_len, sizeof(*request_len), base->sql,
                         base->sql_len))))
        return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    for (uint32_t index = 0; index < statement->order_count; ++index) {
        const prdb_sdk_sql_order_item_v1 *item =
            &statement->order_items[index];
        if (item->direction > PRDB_SDK_SQL_ORDER_DESCENDING ||
            item->null_order > PRDB_SDK_SQL_NULLS_LAST)
            return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
        if (item->kind == PRDB_SDK_SQL_ORDER_GROUP_KEY) {
            if (item->group_key_index >= statement->group_key_count ||
                (seen_keys & (uint16_t)(1u << item->group_key_index)) != 0)
                return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
            seen_keys |= (uint16_t)(1u << item->group_key_index);
            insert_len += 4;
        } else if (item->kind == PRDB_SDK_SQL_ORDER_AGGREGATE &&
                   item->group_key_index == 0 && !seen_aggregate) {
            seen_aggregate = 1;
            insert_len += 3;
        } else {
            return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
        }
    }
    status = prdb_sdk_optimizer_encode_composite_group_aggregate_v1(
        statement, schema, schema_len, base_request,
        sizeof(base_request), &base_len);
    if (status != PRDB_SDK_OPTIMIZER_ENCODE_OK) return status;
    limit_len = statement->has_limit ? 5 : 1;
    if (base_len < limit_len) return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    limit_offset = base_len - limit_len;
    required = base_len + insert_len;
    if (required > OPTIMIZER_REQUEST_MAX_BYTES)
        return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;
    if (request_capacity < required) {
        *request_len = required;
        return PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL;
    }
    memcpy(request, base_request, (size_t)limit_offset);
    request[2] = PRDB_OPTIMIZER_ORDERED_COMPOSITE_GROUP_AGGREGATE_STATEMENT;
    output_offset = limit_offset;
    request[output_offset++] = (uint8_t)statement->order_count;
    for (uint32_t index = 0; index < statement->order_count; ++index) {
        const prdb_sdk_sql_order_item_v1 *item =
            &statement->order_items[index];
        request[output_offset++] =
            (uint8_t)(item->kind == PRDB_SDK_SQL_ORDER_GROUP_KEY ? 0 : 1);
        if (item->kind == PRDB_SDK_SQL_ORDER_GROUP_KEY)
            request[output_offset++] = (uint8_t)item->group_key_index;
        request[output_offset++] = (uint8_t)item->direction;
        request[output_offset++] =
            (uint8_t)(item->null_order == PRDB_SDK_SQL_NULL_ORDER_DEFAULT ||
                      item->null_order == PRDB_SDK_SQL_NULLS_FIRST);
    }
    memcpy(request + output_offset, base_request + limit_offset,
           (size_t)limit_len);
    output_offset += limit_len;
    *request_len = output_offset;
    return PRDB_SDK_OPTIMIZER_ENCODE_OK;
}

int32_t prdb_sdk_optimizer_encode_group_aggregate_v1(
    const prdb_sdk_select_v1 *statement, const uint8_t *schema,
    uint32_t schema_len, uint8_t *request, uint64_t request_capacity,
    uint64_t *request_len) {
    const prdb_sdk_select_v1 *base;
    const uint8_t *sql;
    uint64_t offset = 0, required = 4 + 1 + schema_len;
    uint8_t header[4] = {0, 1, PRDB_OPTIMIZER_GROUP_AGGREGATE_STATEMENT, 0};
    if (statement == NULL || schema == NULL || request == NULL || request_len == NULL ||
        statement->struct_size != sizeof(*statement) || statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->has_group_by != 1 || statement->aggregate_kind < PRDB_SDK_SQL_AGGREGATE_COUNT_STAR || statement->aggregate_kind > PRDB_SDK_SQL_AGGREGATE_COUNT_COLUMN ||
        statement->projection_count != 1 || statement->projection_star != 0 || statement->predicate_count != 0 || statement->has_limit > 1 ||
        schema_len == 0 || schema_len > 63 || !valid_identifier(schema, schema_len) || request_capacity > OPTIMIZER_REQUEST_MAX_BYTES)
        return request_capacity > OPTIMIZER_REQUEST_MAX_BYTES ? PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED : PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    base = statement; sql = base->sql;
    if (sql == NULL || base->sql_len == 0 || base->sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES || !span_is_identifier(base, base->relation) ||
        !span_is_identifier(base, statement->group_by_column) || !span_is_identifier(base, base->projection[0]) ||
        !optimizer_spans_equal_folded(sql, base->sql_len, statement->group_by_column, base->projection[0]) ||
        (statement->aggregate_kind == PRDB_SDK_SQL_AGGREGATE_COUNT_STAR ? statement->aggregate_column.length != 0 : !span_is_identifier(base, statement->aggregate_column)))
        return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    if (ranges_overlap(statement, sizeof(*statement), request, request_capacity) || ranges_overlap(schema, schema_len, request, request_capacity) ||
        ranges_overlap(sql, base->sql_len, request, request_capacity) || ranges_overlap(request_len, sizeof(*request_len), request, request_capacity) ||
        ranges_overlap(request_len, sizeof(*request_len), statement, sizeof(*statement)) || ranges_overlap(request_len, sizeof(*request_len), schema, schema_len) ||
        ranges_overlap(request_len, sizeof(*request_len), sql, base->sql_len)) return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    if (!append_identifier_span(sql, base->sql_len, base->relation, NULL, 0, &required) ||
        !append_identifier_span(sql, base->sql_len, statement->group_by_column, NULL, 0, &required)) return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    required += 1;
    if (statement->aggregate_kind != PRDB_SDK_SQL_AGGREGATE_COUNT_STAR &&
        !append_identifier_span(sql, base->sql_len, statement->aggregate_column, NULL, 0, &required)) return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    required += 1 + (base->has_limit ? 4 : 0);
    if (required > OPTIMIZER_REQUEST_MAX_BYTES) return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;
    if (request_capacity < required) { *request_len = required; return PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL; }
    predicate_append(request, request_capacity, &offset, header, sizeof(header));
    predicate_append_u8(request, request_capacity, &offset, (uint8_t)schema_len);
    predicate_append(request, request_capacity, &offset, schema, schema_len);
    append_identifier_span(sql, base->sql_len, base->relation, request, request_capacity, &offset);
    append_identifier_span(sql, base->sql_len, statement->group_by_column, request, request_capacity, &offset);
    predicate_append_u8(request, request_capacity, &offset, (uint8_t)statement->aggregate_kind);
    if (statement->aggregate_kind != PRDB_SDK_SQL_AGGREGATE_COUNT_STAR) append_identifier_span(sql, base->sql_len, statement->aggregate_column, request, request_capacity, &offset);
    predicate_append_u8(request, request_capacity, &offset, (uint8_t)base->has_limit);
    if (base->has_limit) { uint8_t limit[4]; write_u32(limit, base->limit); predicate_append(request, request_capacity, &offset, limit, sizeof(limit)); }
    *request_len = offset;
    return PRDB_SDK_OPTIMIZER_ENCODE_OK;
}

int32_t prdb_sdk_optimizer_encode_ordered_group_aggregate_v1(
    const prdb_sdk_select_v1 *statement, const uint8_t *schema,
    uint32_t schema_len, uint8_t *request, uint64_t request_capacity,
    uint64_t *request_len) {
    const prdb_sdk_select_v1 *statement_v7;
    const prdb_sdk_select_v1 *base;
    const uint8_t *sql;
    uint64_t offset = 0, required = 4 + 1 + schema_len;
    uint8_t header[4] = {
        0, 1, PRDB_OPTIMIZER_ORDERED_GROUP_AGGREGATE_STATEMENT, 0};
    uint8_t seen = 0;
    if (statement == NULL || schema == NULL || request == NULL ||
        request_len == NULL || statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->order_count == 0 ||
        statement->order_count > PRDB_SDK_SQL_MAX_ORDER_KEYS ||
        schema_len == 0 || schema_len > 63 ||
        !valid_identifier(schema, schema_len) ||
        request_capacity > OPTIMIZER_REQUEST_MAX_BYTES)
        return request_capacity > OPTIMIZER_REQUEST_MAX_BYTES
                   ? PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED
                   : PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    statement_v7 = statement;
    base = statement_v7;
    if (statement_v7->struct_size != sizeof(*statement_v7) ||
        statement_v7->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement_v7->has_group_by != 1 || statement_v7->aggregate_kind < 1 ||
        statement_v7->aggregate_kind > PRDB_SDK_SQL_AGGREGATE_COUNT_COLUMN ||
        base->projection_count != 1 ||
        base->projection_star != 0 || base->predicate_count != 0 ||
        base->has_limit > 1 ||
        statement->has_having != 0 || schema_len == 0)
        return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    sql = base->sql;
    if (sql == NULL || base->sql_len == 0 ||
        base->sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        !span_is_identifier(base, base->relation) ||
        !span_is_identifier(base, statement_v7->group_by_column) ||
        !span_is_identifier(base, base->projection[0]) ||
        !optimizer_spans_equal_folded(sql, base->sql_len,
                                      statement_v7->group_by_column,
                                      base->projection[0]) ||
        (statement_v7->aggregate_kind ==
                 PRDB_SDK_SQL_AGGREGATE_COUNT_STAR
             ? statement_v7->aggregate_column.length != 0
             : !span_is_identifier(base, statement_v7->aggregate_column)))
        return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    if (ranges_overlap(statement, sizeof(*statement), request,
                       request_capacity) ||
        ranges_overlap(schema, schema_len, request, request_capacity) ||
        ranges_overlap(sql, base->sql_len, request, request_capacity) ||
        ranges_overlap(request_len, sizeof(*request_len), request,
                       request_capacity) ||
        ranges_overlap(request_len, sizeof(*request_len), statement,
                       sizeof(*statement)) ||
        ranges_overlap(request_len, sizeof(*request_len), schema, schema_len) ||
        ranges_overlap(request_len, sizeof(*request_len), sql, base->sql_len))
        return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    if (!append_identifier_span(sql, base->sql_len, base->relation, NULL, 0,
                                &required) ||
        !append_identifier_span(sql, base->sql_len,
                                statement_v7->group_by_column, NULL, 0,
                                &required))
        return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    required += 1;
    if (statement_v7->aggregate_kind !=
        PRDB_SDK_SQL_AGGREGATE_COUNT_STAR) {
        if (!append_identifier_span(sql, base->sql_len,
                                    statement_v7->aggregate_column, NULL, 0,
                                    &required))
            return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    }
    required += 1 + (uint64_t)statement->order_count * 3;
    required += 1 + (base->has_limit ? 4 : 0);
    for (uint32_t index = 0; index < statement->order_count; ++index) {
        const prdb_sdk_sql_order_item_v1 *item =
            &statement->order_items[index];
        if (item->direction > PRDB_SDK_SQL_ORDER_DESCENDING ||
            item->null_order > PRDB_SDK_SQL_NULLS_LAST ||
            item->kind < PRDB_SDK_SQL_ORDER_GROUP_KEY ||
            item->kind > PRDB_SDK_SQL_ORDER_AGGREGATE ||
            (seen & (1u << (item->kind - 1))) != 0)
            return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
        seen |= (uint8_t)(1u << (item->kind - 1));
    }
    if (required > OPTIMIZER_REQUEST_MAX_BYTES)
        return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;
    if (request_capacity < required) {
        *request_len = required;
        return PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL;
    }
    predicate_append(request, request_capacity, &offset, header, sizeof(header));
    predicate_append_u8(request, request_capacity, &offset, (uint8_t)schema_len);
    predicate_append(request, request_capacity, &offset, schema, schema_len);
    append_identifier_span(sql, base->sql_len, base->relation, request,
                           request_capacity, &offset);
    append_identifier_span(sql, base->sql_len, statement_v7->group_by_column,
                           request, request_capacity, &offset);
    predicate_append_u8(request, request_capacity, &offset,
                        (uint8_t)statement_v7->aggregate_kind);
    if (statement_v7->aggregate_kind !=
        PRDB_SDK_SQL_AGGREGATE_COUNT_STAR)
        append_identifier_span(sql, base->sql_len,
                               statement_v7->aggregate_column, request,
                               request_capacity, &offset);
    predicate_append_u8(request, request_capacity, &offset,
                        (uint8_t)statement->order_count);
    for (uint32_t index = 0; index < statement->order_count; ++index) {
        const prdb_sdk_sql_order_item_v1 *item =
            &statement->order_items[index];
        predicate_append_u8(
            request, request_capacity, &offset,
            (uint8_t)(item->kind == PRDB_SDK_SQL_ORDER_GROUP_KEY ? 0 : 1));
        predicate_append_u8(request, request_capacity, &offset,
                            (uint8_t)item->direction);
        predicate_append_u8(
            request, request_capacity, &offset,
            (uint8_t)(item->null_order == PRDB_SDK_SQL_NULL_ORDER_DEFAULT ||
                      item->null_order == PRDB_SDK_SQL_NULLS_FIRST));
    }
    predicate_append_u8(request, request_capacity, &offset,
                        (uint8_t)base->has_limit);
    if (base->has_limit) {
        uint8_t limit[4];
        write_u32(limit, base->limit);
        predicate_append(request, request_capacity, &offset, limit,
                         sizeof(limit));
    }
    *request_len = offset;
    return PRDB_SDK_OPTIMIZER_ENCODE_OK;
}

static int32_t optimizer_encode_join_select_internal(
    const prdb_sdk_select_v1 *statement, const uint8_t *schema,
    uint32_t schema_len, uint8_t *request, uint64_t request_capacity,
    uint64_t *request_len) {
    uint64_t offset = 0;
    const uint8_t *sql;
    uint64_t sql_len;
    prdb_sdk_sql_span_v1 left_relation;
    prdb_sdk_sql_span_v1 right_relation;
    prdb_sdk_sql_span_v1 left_qualifier;
    prdb_sdk_sql_span_v1 right_qualifier;
    uint8_t header[4] = {0, 1, PRDB_OPTIMIZER_JOIN_SELECT_STATEMENT, 0};

    if (statement == NULL || schema == NULL || request == NULL ||
        request_len == NULL ||
        statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->sql == NULL ||
        statement->sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        statement->join_projection_count == 0 ||
        statement->join_projection_count > PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS ||
        statement->join_key_count < 2 ||
        statement->join_key_count > PRDB_SDK_SQL_MAX_JOIN_KEYS ||
        statement->has_limit > 1 || schema_len == 0 || schema_len > 63 ||
        !valid_identifier(schema, schema_len) ||
        request_capacity > OPTIMIZER_REQUEST_MAX_BYTES ||
        ranges_overlap(statement, sizeof(*statement), request, request_capacity) ||
        ranges_overlap(schema, schema_len, request, request_capacity)) {
        return request_capacity > OPTIMIZER_REQUEST_MAX_BYTES
                   ? PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED
                   : PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    }
    sql = statement->sql;
    sql_len = statement->sql_len;
    if (ranges_overlap(sql, sql_len, request, request_capacity))
        return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    left_relation = statement->left_relation;
    right_relation = statement->right_relation;
    left_qualifier = statement->left_alias.length != 0
                         ? statement->left_alias : left_relation;
    right_qualifier = statement->right_alias.length != 0
                          ? statement->right_alias : right_relation;
    if ((statement->left_alias.length != 0 &&
         !append_identifier_span(sql, sql_len, statement->left_alias, NULL,
                                 0, &offset)) ||
        (statement->right_alias.length != 0 &&
         !append_identifier_span(sql, sql_len, statement->right_alias, NULL,
                                 0, &offset)) ||
        !append_identifier_span(sql, sql_len, left_relation, NULL, 0, &offset) ||
        !append_identifier_span(sql, sql_len, right_relation, NULL, 0, &offset))
        return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    offset = 0;
    uint64_t required = 4 + 1 + schema_len + 1 + left_relation.length +
                        1 + right_relation.length + 2;
    for (uint32_t index = 0; index < statement->join_projection_count;
         ++index) {
        prdb_sdk_sql_qualified_column_v1 projection =
            statement->join_projections[index];
        if ((!span_equals(sql, sql_len, projection.relation, left_qualifier) &&
             !span_equals(sql, sql_len, projection.relation, right_qualifier)) ||
            !append_identifier_span(sql, sql_len, projection.column, NULL,
                                    0, &offset))
            return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
        required += 2 + projection.column.length;
    }
    required += 1;
    for (uint32_t index = 0; index < statement->join_key_count; ++index) {
        if (!append_identifier_span(sql, sql_len,
                                    statement->left_join_keys[index].column,
                                    NULL, 0, &offset) ||
            !append_identifier_span(sql, sql_len,
                                    statement->right_join_keys[index].column,
                                    NULL, 0, &offset))
            return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
        required += 2 + statement->left_join_keys[index].column.length +
                    statement->right_join_keys[index].column.length;
    }
    required += 1 + (statement->has_limit ? 4 : 0);
    if (required > OPTIMIZER_REQUEST_MAX_BYTES)
        return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;
    if (request_capacity < required) {
        *request_len = required;
        return PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL;
    }
    offset = 0;
#define APPEND_JOIN(ptr, len)                                                   \
    do {                                                                        \
        if (!predicate_append(request, request_capacity, &offset, (ptr), (len)))\
            return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;                    \
    } while (0)
    APPEND_JOIN(header, sizeof(header));
    if (!predicate_append_u8(request, request_capacity, &offset,
                             (uint8_t)schema_len))
        return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;
    APPEND_JOIN(schema, schema_len);
    if (!append_identifier_span(sql, sql_len, left_relation, request,
                                request_capacity, &offset) ||
        !append_identifier_span(sql, sql_len, right_relation, request,
                                request_capacity, &offset))
        return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    uint8_t count_bytes[2];
    write_u16(count_bytes, (uint16_t)statement->join_projection_count);
    APPEND_JOIN(count_bytes, sizeof(count_bytes));
    for (uint32_t index = 0; index < statement->join_projection_count;
         ++index) {
        prdb_sdk_sql_qualified_column_v1 projection =
            statement->join_projections[index];
        uint8_t side = span_equals(sql, sql_len, projection.relation,
                                   left_qualifier) ? 0 : 1;
        APPEND_JOIN(&side, 1);
        if (!append_identifier_span(sql, sql_len, projection.column, request,
                                    request_capacity, &offset))
            return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    }
    uint8_t key_count = (uint8_t)statement->join_key_count;
    APPEND_JOIN(&key_count, 1);
    for (uint32_t index = 0; index < statement->join_key_count; ++index) {
        if (!append_identifier_span(sql, sql_len,
                                    statement->left_join_keys[index].column,
                                    request, request_capacity, &offset) ||
            !append_identifier_span(sql, sql_len,
                                    statement->right_join_keys[index].column,
                                    request, request_capacity, &offset))
            return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    }
    uint8_t has_limit = (uint8_t)statement->has_limit;
    APPEND_JOIN(&has_limit, 1);
    if (has_limit) {
        uint8_t limit_bytes[4];
        write_u32(limit_bytes, statement->limit);
        APPEND_JOIN(limit_bytes, sizeof(limit_bytes));
    }
#undef APPEND_JOIN
    *request_len = offset;
    return PRDB_SDK_OPTIMIZER_ENCODE_OK;
}

int32_t prdb_sdk_optimizer_encode_join_select_v1(
    const prdb_sdk_select_v1 *statement, const uint8_t *schema,
    uint32_t schema_len, uint8_t *request, uint64_t request_capacity,
    uint64_t *request_len) {
    return optimizer_encode_join_select_internal(
        statement, schema, schema_len, request, request_capacity, request_len);
}

int32_t prdb_sdk_optimizer_encode_filtered_join_select_v1(
    const prdb_sdk_select_v1 *statement, const uint8_t *schema,
    uint32_t schema_len, uint8_t *request, uint64_t request_capacity,
    uint64_t *request_len) {
    const prdb_sdk_select_v1 *base;
    const uint8_t *sql;
    uint64_t sql_len;
    prdb_sdk_sql_span_v1 left_relation, right_relation;
    prdb_sdk_sql_span_v1 left_qualifier, right_qualifier;
    prdb_sdk_sql_qualified_column_v1 filter;
    uint32_t expression_kind, filter_side, comparison = 0;
    uint64_t prefix_len = 0, extra_len;
    int32_t status;

    if (statement == NULL || request == NULL || request_len == NULL ||
        statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->has_join_where != 1)
        return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    if (request_capacity > OPTIMIZER_REQUEST_MAX_BYTES)
        return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;
    if (ranges_overlap(statement, sizeof(*statement), request,
                       request_capacity))
        return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    base = statement;
    sql = base->sql;
    sql_len = base->sql_len;
    filter = statement->join_where_column;
    left_relation = base->left_relation;
    right_relation = base->right_relation;
    left_qualifier = base->left_alias.length != 0
                         ? base->left_alias : left_relation;
    right_qualifier = base->right_alias.length != 0
                          ? base->right_alias : right_relation;
    if (sql == NULL || sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        filter.relation.length == 0 || filter.column.length == 0 ||
        !append_identifier_span(sql, sql_len, filter.column, NULL, 0,
                                &prefix_len))
        return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    if (ranges_overlap(sql, sql_len, request, request_capacity) ||
        ranges_overlap(schema, schema_len, request, request_capacity))
        return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    if (span_equals(sql, sql_len, filter.relation, left_qualifier)) {
        filter_side = 0;
    } else if (span_equals(sql, sql_len, filter.relation, right_qualifier)) {
        filter_side = 1;
    } else {
        return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    }
    if (statement->join_where_comparison >= PRDB_SDK_SQL_COMPARE_EQUAL &&
        statement->join_where_comparison <= PRDB_SDK_SQL_COMPARE_GREATER_EQUAL) {
        if (statement->join_where_parameter_index == 0 ||
            statement->join_where_parameter_index > 256)
            return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
        expression_kind = 0;
        comparison = statement->join_where_comparison -
                     PRDB_SDK_SQL_COMPARE_EQUAL;
    } else if (statement->join_where_comparison == PRDB_SDK_SQL_COMPARE_IS_NULL) {
        if (statement->join_where_parameter_index != 0)
            return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
        expression_kind = 1;
    } else if (statement->join_where_comparison ==
               PRDB_SDK_SQL_COMPARE_IS_NOT_NULL) {
        if (statement->join_where_parameter_index != 0)
            return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
        expression_kind = 2;
    } else {
        return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    }
    extra_len = 1 + 1 + filter.column.length + 1 +
                (expression_kind == 0 ? 5 : 0);
    status = optimizer_encode_join_select_internal(
        base, schema, schema_len, request,
        request_capacity >= extra_len ? request_capacity - extra_len : 0,
        &prefix_len);
    if (status == PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL) {
        *request_len = prefix_len + extra_len;
        return status;
    }
    if (status != PRDB_SDK_OPTIMIZER_ENCODE_OK) return status;
    request[2] = PRDB_OPTIMIZER_FILTERED_JOIN_SELECT_STATEMENT;
    uint64_t offset = prefix_len;
    request[offset++] = (uint8_t)filter_side;
    if (!append_identifier_span(sql, sql_len, filter.column, request,
                                request_capacity, &offset))
        return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    request[offset++] = (uint8_t)expression_kind;
    if (expression_kind == 0) {
        request[offset++] = (uint8_t)comparison;
        write_u32(request + offset, statement->join_where_parameter_index);
        offset += 4;
    }
    *request_len = offset;
    return PRDB_SDK_OPTIMIZER_ENCODE_OK;
}

int32_t prdb_sdk_optimizer_encode_select_v1(
    const prdb_sdk_select_v1 *statement, const uint8_t *schema,
    uint32_t schema_len, uint8_t *request, uint64_t request_capacity,
    uint64_t *request_len) {
    if (statement != NULL && statement->predicate_count != 0)
        return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    return optimizer_encode_select_internal(
        statement, schema, schema_len, NULL, 0, request, request_capacity,
        request_len);
}

int32_t prdb_sdk_optimizer_encode_select_with_parameters_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity, uint64_t *request_len) {
    return optimizer_encode_select_internal(
        statement, schema, schema_len, parameters, parameter_count, request,
        request_capacity, request_len);
}

int32_t prdb_sdk_optimizer_encode_ordered_select_v1(
    const prdb_sdk_select_v1 *statement, const uint8_t *schema,
    uint32_t schema_len, uint8_t *request, uint64_t request_capacity,
    uint64_t *request_len) {
    const prdb_sdk_select_v1 *base;
    uint64_t encoded_len;
    uint64_t offset;
    if (request_len == NULL) return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    if (statement == NULL || schema == NULL ||
        statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->sql == NULL || statement->sql_len == 0 ||
        statement->sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        statement->join_projection_count >
            PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS ||
        statement->projection_star != 0 ||
        statement->predicate_count != 0 ||
        statement->predicate_root != 0 || statement->has_order_by != 1 ||
        statement->order_descending > 1 || statement->has_limit > 1 ||
        statement->limit > PRDB_SDK_SQL_MAX_LIMIT ||
        !valid_identifier(schema, schema_len) ||
        (request_capacity != 0 && request == NULL) ||
        request_capacity > PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES ||
        ranges_overlap(request_len, sizeof(*request_len), statement,
                       sizeof(*statement)) ||
        ranges_overlap(request_len, sizeof(*request_len), schema, schema_len) ||
        ranges_overlap(request_len, sizeof(*request_len), statement->sql,
                       statement->sql_len) ||
        ranges_overlap(request, request_capacity, statement,
                       sizeof(*statement)) ||
        ranges_overlap(request, request_capacity, schema, schema_len) ||
        ranges_overlap(request, request_capacity, statement->sql,
                       statement->sql_len)) {
        return request_capacity > PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES
                   ? PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED
                   : PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    }
    base = statement;
    if (base->projection_count == 0 || base->projection_count > 128 ||
        !span_is_identifier(base, base->relation) ||
        !span_is_identifier(base, statement->order_by_column)) {
        return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    }
    encoded_len = PRDB_OPTIMIZER_SELECT_REQUEST_HEADER_BYTES + 1 + schema_len +
                  1 + base->relation.length + 2 + 1 +
                  statement->order_by_column.length + 2 +
                  (base->has_limit != 0 ? 4 : 0);
    for (uint32_t index = 0; index < base->projection_count; ++index) {
        prdb_sdk_sql_span_v1 projection = base->projection[index];
        if (!span_is_identifier(base, projection)) {
            return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
        }
        encoded_len += 1 + projection.length;
    }
    if (encoded_len > PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES) {
        return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;
    }
    *request_len = encoded_len;
    if (request_capacity < encoded_len) {
        return PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL;
    }
    offset = 0;
    prdb_encode_optimizer_select_request(
        request, PRDB_OPTIMIZER_ORDERED_SELECT_STATEMENT, 0);
    offset = PRDB_OPTIMIZER_SELECT_REQUEST_HEADER_BYTES;
    request[offset++] = (uint8_t)schema_len;
    memcpy(request + offset, schema, schema_len);
    offset += schema_len;
    request[offset++] = (uint8_t)base->relation.length;
    memcpy(request + offset, base->sql + base->relation.offset,
           base->relation.length);
    offset += base->relation.length;
    write_u16(request + offset, (uint16_t)base->projection_count);
    offset += 2;
    for (uint32_t index = 0; index < base->projection_count; ++index) {
        prdb_sdk_sql_span_v1 projection = base->projection[index];
        request[offset++] = (uint8_t)projection.length;
        memcpy(request + offset, base->sql + projection.offset,
               projection.length);
        offset += projection.length;
    }
    request[offset++] = (uint8_t)statement->order_by_column.length;
    memcpy(request + offset,
           base->sql + statement->order_by_column.offset,
           statement->order_by_column.length);
    offset += statement->order_by_column.length;
    request[offset++] = (uint8_t)statement->order_descending;
    request[offset++] = (uint8_t)base->has_limit;
    if (base->has_limit != 0) {
        write_u32(request + offset, base->limit);
        offset += 4;
    }
    if (offset != encoded_len) {
        *request_len = 0;
        return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    }
    return PRDB_SDK_OPTIMIZER_ENCODE_OK;
}

static int32_t optimizer_encode_delete_internal(
    const prdb_sdk_delete_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity, uint64_t *request_len) {
    const prdb_sdk_select_v1 *predicate;
    uint8_t seen[PRDB_SDK_SQL_MAX_PREDICATE_NODES] = {0};
    uint64_t predicate_len = 0;
    uint64_t encoded_len;
    uint64_t offset;
    uint32_t index;
    int predicate_status;

    if (request_len == NULL) return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    if (statement == NULL || statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_DML_ABI_V1_VERSION ||
        statement->sql == NULL || statement->sql_len == 0 ||
        statement->sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        request_capacity > PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES ||
        (request_capacity != 0 && request == NULL) ||
        parameter_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        (parameter_count != 0 && parameters == NULL) ||
        !valid_identifier(schema, schema_len) ||
        ranges_overlap(request_len, sizeof(*request_len), statement,
                       sizeof(*statement)) ||
        ranges_overlap(request_len, sizeof(*request_len), schema, schema_len) ||
        ranges_overlap(request_len, sizeof(*request_len), statement->sql,
                       statement->sql_len) ||
        ranges_overlap(request_len, sizeof(*request_len), parameters,
                       (uint64_t)parameter_count * sizeof(*parameters)) ||
        ranges_overlap(request, request_capacity, statement,
                       sizeof(*statement)) ||
        ranges_overlap(request, request_capacity, schema, schema_len) ||
        ranges_overlap(request, request_capacity, statement->sql,
                       statement->sql_len) ||
        ranges_overlap(request, request_capacity, parameters,
                       (uint64_t)parameter_count * sizeof(*parameters))) {
        return request_capacity > PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES
                   ? PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED
                   : PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    }
    for (index = 0; index < parameter_count; ++index) {
        uint64_t data_len = parameters[index].data_len;
        const uint8_t *data = parameters[index].data;
        if (data_len != 0 &&
            (ranges_overlap(request_len, sizeof(*request_len), data, data_len) ||
             ranges_overlap(request, request_capacity, data, data_len))) {
            return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
        }
    }
    *request_len = 0;
    predicate = &statement->predicate_statement;
    if (predicate->struct_size != sizeof(*predicate) ||
        predicate->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        predicate->sql != statement->sql ||
        predicate->sql_len != statement->sql_len ||
        predicate->projection_star != 1 || predicate->projection_count != 0 ||
        predicate->has_limit != 0 || predicate->limit != 0 ||
        predicate->predicate_count == 0 ||
        predicate->predicate_count > PRDB_SDK_SQL_MAX_PREDICATE_NODES ||
        predicate->predicate_root >= predicate->predicate_count ||
        predicate->relation.offset != statement->relation.offset ||
        predicate->relation.length != statement->relation.length ||
        !span_is_identifier(predicate, statement->relation)) {
        return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    }
    predicate_status = encode_optimizer_predicate_node(
        predicate, parameters, parameter_count, predicate->predicate_root,
        1, seen, NULL, 0, &predicate_len);
    if (predicate_status == 2) return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    if (predicate_status != 1) return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    for (index = 0; index < predicate->predicate_count; ++index) {
        if (seen[index] == 0) return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    }
    if (predicate_len == 0 || predicate_len > UINT32_C(16384))
        return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;

    encoded_len = PRDB_OPTIMIZER_SELECT_REQUEST_HEADER_BYTES + 1 + schema_len +
                  1 + statement->relation.length + 4 + predicate_len;
    if (encoded_len > PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES)
        return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;
    if (request_capacity < encoded_len) {
        *request_len = encoded_len;
        return PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL;
    }
    offset = 0;
    prdb_encode_optimizer_select_request(request,
                                         PRDB_OPTIMIZER_DELETE_STATEMENT, 0);
    offset = PRDB_OPTIMIZER_SELECT_REQUEST_HEADER_BYTES;
    request[offset++] = (uint8_t)schema_len;
    memcpy(request + offset, schema, schema_len);
    offset += schema_len;
    request[offset++] = (uint8_t)statement->relation.length;
    memcpy(request + offset, statement->sql + statement->relation.offset,
           statement->relation.length);
    offset += statement->relation.length;
    write_u32(request + offset, (uint32_t)predicate_len);
    offset += 4;
    {
        uint64_t written = 0;
        memset(seen, 0, sizeof(seen));
        predicate_status = encode_optimizer_predicate_node(
            predicate, parameters, parameter_count, predicate->predicate_root,
            1, seen, request + offset, request_capacity - offset, &written);
        if (predicate_status != 1 || written != predicate_len)
            return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
        offset += written;
    }
    *request_len = offset;
    return PRDB_SDK_OPTIMIZER_ENCODE_OK;
}

int32_t prdb_sdk_optimizer_encode_delete_v1(
    const prdb_sdk_delete_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity, uint64_t *request_len) {
    return optimizer_encode_delete_internal(
        statement, parameters, parameter_count, schema, schema_len, request,
        request_capacity, request_len);
}

static uint8_t ascii_lower(uint8_t byte) {
    return byte >= 'A' && byte <= 'Z' ? (uint8_t)(byte + ('a' - 'A')) : byte;
}

static int update_columns_equal(const prdb_sdk_update_v1 *statement,
                                prdb_sdk_sql_span_v1 left,
                                prdb_sdk_sql_span_v1 right) {
    uint32_t index;
    if (left.length != right.length) return 0;
    for (index = 0; index < left.length; ++index) {
        if (ascii_lower(statement->sql[left.offset + index]) !=
            ascii_lower(statement->sql[right.offset + index])) return 0;
    }
    return 1;
}

static int32_t optimizer_encode_update_internal(
    const prdb_sdk_update_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity, uint64_t *request_len) {
    const prdb_sdk_select_v1 *predicate;
    uint8_t seen[PRDB_SDK_SQL_MAX_PREDICATE_NODES] = {0};
    uint64_t predicate_len = 0;
    uint64_t encoded_len;
    uint64_t offset;
    uint32_t index;
    int predicate_status;

    if (request_len == NULL) return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    if (statement == NULL || statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_DML_ABI_V1_VERSION ||
        statement->sql == NULL || statement->sql_len == 0 ||
        statement->sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        request_capacity > PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES ||
        (request_capacity != 0 && request == NULL) ||
        parameter_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        (parameter_count != 0 && parameters == NULL) ||
        !valid_identifier(schema, schema_len) ||
        ranges_overlap(request_len, sizeof(*request_len), statement,
                       sizeof(*statement)) ||
        ranges_overlap(request_len, sizeof(*request_len), schema, schema_len) ||
        ranges_overlap(request_len, sizeof(*request_len), statement->sql,
                       statement->sql_len) ||
        ranges_overlap(request_len, sizeof(*request_len), parameters,
                       (uint64_t)parameter_count * sizeof(*parameters)) ||
        ranges_overlap(request, request_capacity, statement,
                       sizeof(*statement)) ||
        ranges_overlap(request, request_capacity, schema, schema_len) ||
        ranges_overlap(request, request_capacity, statement->sql,
                       statement->sql_len) ||
        ranges_overlap(request, request_capacity, parameters,
                       (uint64_t)parameter_count * sizeof(*parameters))) {
        return request_capacity > PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES
                   ? PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED
                   : PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    }
    for (index = 0; index < parameter_count; ++index) {
        const uint8_t *data = parameters[index].data;
        uint64_t data_len = parameters[index].data_len;
        if (data_len != 0 &&
            (ranges_overlap(request_len, sizeof(*request_len), data, data_len) ||
             ranges_overlap(request, request_capacity, data, data_len)))
            return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    }
    *request_len = 0;
    predicate = &statement->predicate_statement;
    if (predicate->struct_size != sizeof(*predicate) ||
        predicate->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        predicate->sql != statement->sql ||
        predicate->sql_len != statement->sql_len ||
        predicate->projection_star != 0 ||
        predicate->projection_count != statement->assignment_count ||
        predicate->has_limit != 0 || predicate->limit != 0 ||
        predicate->predicate_count == 0 ||
        predicate->predicate_count > PRDB_SDK_SQL_MAX_PREDICATE_NODES ||
        predicate->predicate_root >= predicate->predicate_count ||
        predicate->relation.offset != statement->relation.offset ||
        predicate->relation.length != statement->relation.length ||
        !span_is_identifier(predicate, statement->relation) ||
        statement->assignment_count == 0 ||
        statement->assignment_count > PRDB_SDK_SQL_MAX_UPDATE_ASSIGNMENTS ||
        statement->relation.offset > statement->sql_len ||
        statement->relation.length > statement->sql_len - statement->relation.offset)
        return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    for (index = 0; index < statement->assignment_count; ++index) {
        if (predicate->projection[index].offset !=
                statement->assignments[index].column.offset ||
            predicate->projection[index].length !=
                statement->assignments[index].column.length)
            return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    }

    encoded_len = PRDB_OPTIMIZER_SELECT_REQUEST_HEADER_BYTES + 1 + schema_len +
                  1 + statement->relation.length + 2 + 4;
    for (index = 0; index < statement->assignment_count; ++index) {
        const prdb_sdk_sql_update_assignment_v1 *assignment =
            &statement->assignments[index];
        uint32_t previous;
        if (!span_is_identifier(predicate, assignment->column))
            return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
        for (previous = 0; previous < index; ++previous) {
            if (update_columns_equal(statement, assignment->column,
                                     statement->assignments[previous].column))
                return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
        }
        encoded_len += 1 + assignment->column.length + 1;
        if (assignment->kind == PRDB_SDK_SQL_UPDATE_PARAMETER) {
            if (assignment->parameter_index == 0 ||
                assignment->parameter_index > parameter_count)
                return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
            if (prdb_sdk_value_validate_v1(
                    &parameters[assignment->parameter_index - 1]) !=
                PRDB_SDK_VALUE_VALID)
                return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
            encoded_len += 4;
        } else if (assignment->kind == PRDB_SDK_SQL_UPDATE_NULL) {
            if (assignment->parameter_index != 0)
                return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
        } else {
            return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
        }
    }
    predicate_status = encode_optimizer_predicate_node(
        predicate, parameters, parameter_count, predicate->predicate_root,
        1, seen, NULL, 0, &predicate_len);
    if (predicate_status == 2) return PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED;
    if (predicate_status != 1) return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    for (index = 0; index < predicate->predicate_count; ++index) {
        if (seen[index] == 0) return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
    }
    if (predicate_len == 0 || predicate_len > UINT32_C(16384))
        return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;
    encoded_len += predicate_len;
    if (encoded_len > PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES)
        return PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED;
    if (request_capacity < encoded_len) {
        *request_len = encoded_len;
        return PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL;
    }

    prdb_encode_optimizer_select_request(request,
                                         PRDB_OPTIMIZER_UPDATE_STATEMENT, 0);
    offset = PRDB_OPTIMIZER_SELECT_REQUEST_HEADER_BYTES;
    request[offset++] = (uint8_t)schema_len;
    memcpy(request + offset, schema, schema_len);
    offset += schema_len;
    request[offset++] = (uint8_t)statement->relation.length;
    memcpy(request + offset, statement->sql + statement->relation.offset,
           statement->relation.length);
    offset += statement->relation.length;
    write_u16(request + offset, (uint16_t)statement->assignment_count);
    offset += 2;
    for (index = 0; index < statement->assignment_count; ++index) {
        const prdb_sdk_sql_update_assignment_v1 *assignment =
            &statement->assignments[index];
        request[offset++] = (uint8_t)assignment->column.length;
        memcpy(request + offset, statement->sql + assignment->column.offset,
               assignment->column.length);
        offset += assignment->column.length;
        request[offset++] = (uint8_t)assignment->kind;
        if (assignment->kind == PRDB_SDK_SQL_UPDATE_PARAMETER) {
            write_u32(request + offset, assignment->parameter_index);
            offset += 4;
        }
    }
    write_u32(request + offset, (uint32_t)predicate_len);
    offset += 4;
    {
        uint64_t written = 0;
        memset(seen, 0, sizeof(seen));
        predicate_status = encode_optimizer_predicate_node(
            predicate, parameters, parameter_count, predicate->predicate_root,
            1, seen, request + offset, request_capacity - offset, &written);
        if (predicate_status != 1 || written != predicate_len)
            return PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT;
        offset += written;
    }
    *request_len = offset;
    return PRDB_SDK_OPTIMIZER_ENCODE_OK;
}

int32_t prdb_sdk_optimizer_encode_update_v1(
    const prdb_sdk_update_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    uint8_t *request, uint64_t request_capacity, uint64_t *request_len) {
    return optimizer_encode_update_internal(
        statement, parameters, parameter_count, schema, schema_len, request,
        request_capacity, request_len);
}

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

int32_t prdb_sdk_optimizer_decode_response_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_response_v1 *out, uint32_t out_capacity) {
    prdb_sdk_optimizer_response_v1 decoded;
    uint32_t plan_len;
    size_t decoded_plan_len;

    if (payload == NULL || out == NULL ||
        out_capacity < sizeof(prdb_sdk_optimizer_response_v1) ||
        payload_len > PRDB_QUERY_MAX_IPC_REQUEST_BYTES || payload_len < 3 ||
        ranges_overlap(payload, payload_len, out, sizeof(*out))) {
        return PRDB_SDK_OPTIMIZER_RESULT_INVALID_ARGUMENT;
    }
    decoded.struct_size = sizeof(decoded);
    decoded.abi_version = PRDB_SDK_OPTIMIZER_ABI_V1_VERSION;
    decoded.status = payload[2];
    decoded.plan = NULL;
    decoded.plan_len = 0;
    if (decoded.status > PRDB_SDK_OPTIMIZER_STATUS_INTERNAL) {
        return PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE;
    }
    if (decoded.status != PRDB_SDK_OPTIMIZER_STATUS_SUCCESS) {
        uint8_t response_status;
        if (!prdb_decode_optimizer_response_failure(
                payload, (size_t)payload_len, &response_status) ||
            response_status != decoded.status) {
            return PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE;
        }
        *out = decoded;
        return PRDB_SDK_OPTIMIZER_RESULT_OK;
    }
    uint8_t response_status;
    if (!prdb_decode_optimizer_response_success(
            payload, (size_t)payload_len, &response_status, &plan_len) ||
        response_status != decoded.status) {
        return PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE;
    }
    if (plan_len == 0 || plan_len > PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES ||
        !prdb_decode_optimizer_response_success_plan(
            payload + PRDB_OPTIMIZER_RESPONSE_SUCCESS_HEADER_BYTES,
            (size_t)payload_len - PRDB_OPTIMIZER_RESPONSE_SUCCESS_HEADER_BYTES,
            plan_len, &decoded.plan, &decoded_plan_len) ||
        decoded_plan_len != plan_len) {
        return PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE;
    }
    decoded.plan_len = plan_len;
    *out = decoded;
    return PRDB_SDK_OPTIMIZER_RESULT_OK;
}

int32_t prdb_sdk_optimizer_submit_select_v1(
    const prdb_host_api_v1 *host, const uint8_t *request, uint64_t request_len,
    uint8_t *response, uint64_t response_capacity, uint64_t *response_len,
    prdb_sdk_optimizer_response_v1 *out, uint32_t out_capacity) {
    int32_t callback_status;

    if (host == NULL || host->struct_size != sizeof(prdb_host_api_v1) ||
        host->abi_version != PRDB_QUERY_ABI_VERSION ||
        host->context == NULL || host->optimizer_request == NULL ||
        request_len == 0 || request == NULL ||
        request_len > OPTIMIZER_REQUEST_MAX_BYTES || response_len == NULL ||
        response_capacity > PRDB_QUERY_MAX_IPC_REQUEST_BYTES ||
        (response_capacity != 0 && response == NULL) || out == NULL ||
        out_capacity < sizeof(*out) ||
        ranges_overlap(out, sizeof(*out), response, response_capacity)) {
        return request_len > OPTIMIZER_REQUEST_MAX_BYTES ||
                       response_capacity > PRDB_QUERY_MAX_IPC_REQUEST_BYTES
                   ? PRDB_SDK_OPTIMIZER_RESULT_LIMIT_EXCEEDED
                   : PRDB_SDK_OPTIMIZER_RESULT_INVALID_ARGUMENT;
    }
    callback_status = host->optimizer_request(
        host->context, PRDB_IPC_OP_OPTIMIZE_SELECT, request, request_len,
        response, response_capacity, response_len);
    if (callback_status == PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL) {
        if (*response_len > PRDB_QUERY_MAX_IPC_REQUEST_BYTES) {
            return PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE;
        }
        return PRDB_SDK_OPTIMIZER_RESULT_RESPONSE_TOO_SMALL;
    }
    if (callback_status == PRDB_QUERY_CALLBACK_LIMIT_EXCEEDED) {
        return PRDB_SDK_OPTIMIZER_RESULT_LIMIT_EXCEEDED;
    }
    if (callback_status != PRDB_QUERY_CALLBACK_OK) {
        return PRDB_SDK_OPTIMIZER_RESULT_CALLBACK_ERROR;
    }
    if (*response_len > response_capacity) {
        return PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE;
    }
    return prdb_sdk_optimizer_decode_response_v1(response, *response_len, out,
                                                 out_capacity);
}

int32_t prdb_sdk_optimizer_submit_delete_v1(
    const prdb_host_api_v1 *host, const uint8_t *request, uint64_t request_len,
    uint8_t *response, uint64_t response_capacity, uint64_t *response_len,
    prdb_sdk_optimizer_response_v1 *out, uint32_t out_capacity) {
    int32_t callback_status;
    if (host == NULL || host->struct_size != sizeof(prdb_host_api_v1) ||
        host->abi_version != PRDB_QUERY_ABI_VERSION ||
        host->context == NULL || host->optimizer_request == NULL || request_len == 0 || request == NULL ||
        request_len > OPTIMIZER_REQUEST_MAX_BYTES || response_len == NULL ||
        response_capacity > PRDB_QUERY_MAX_IPC_REQUEST_BYTES ||
        (response_capacity != 0 && response == NULL) || out == NULL ||
        out_capacity < sizeof(*out) ||
        ranges_overlap(out, sizeof(*out), response, response_capacity)) {
        return request_len > OPTIMIZER_REQUEST_MAX_BYTES ||
                       response_capacity > PRDB_QUERY_MAX_IPC_REQUEST_BYTES
                   ? PRDB_SDK_OPTIMIZER_RESULT_LIMIT_EXCEEDED
                   : PRDB_SDK_OPTIMIZER_RESULT_INVALID_ARGUMENT;
    }
    callback_status = host->optimizer_request(
        host->context, PRDB_IPC_OP_OPTIMIZE_DELETE, request, request_len,
        response, response_capacity, response_len);
    if (callback_status == PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL) {
        if (*response_len > PRDB_QUERY_MAX_IPC_REQUEST_BYTES)
            return PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE;
        return PRDB_SDK_OPTIMIZER_RESULT_RESPONSE_TOO_SMALL;
    }
    if (callback_status == PRDB_QUERY_CALLBACK_LIMIT_EXCEEDED)
        return PRDB_SDK_OPTIMIZER_RESULT_LIMIT_EXCEEDED;
    if (callback_status != PRDB_QUERY_CALLBACK_OK)
        return PRDB_SDK_OPTIMIZER_RESULT_CALLBACK_ERROR;
    if (*response_len > response_capacity)
        return PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE;
    return prdb_sdk_optimizer_decode_response_v1(response, *response_len, out,
                                                 out_capacity);
}

int32_t prdb_sdk_optimizer_submit_update_v1(
    const prdb_host_api_v1 *host, const uint8_t *request, uint64_t request_len,
    uint8_t *response, uint64_t response_capacity, uint64_t *response_len,
    prdb_sdk_optimizer_response_v1 *out, uint32_t out_capacity) {
    int32_t callback_status;
    if (host == NULL || host->struct_size != sizeof(prdb_host_api_v1) ||
        host->abi_version != PRDB_QUERY_ABI_VERSION ||
        host->context == NULL || host->optimizer_request == NULL || request_len == 0 || request == NULL ||
        request_len > OPTIMIZER_REQUEST_MAX_BYTES || response_len == NULL ||
        response_capacity > PRDB_QUERY_MAX_IPC_REQUEST_BYTES ||
        (response_capacity != 0 && response == NULL) || out == NULL ||
        out_capacity < sizeof(*out) ||
        ranges_overlap(out, sizeof(*out), response, response_capacity)) {
        return request_len > OPTIMIZER_REQUEST_MAX_BYTES ||
                       response_capacity > PRDB_QUERY_MAX_IPC_REQUEST_BYTES
                   ? PRDB_SDK_OPTIMIZER_RESULT_LIMIT_EXCEEDED
                   : PRDB_SDK_OPTIMIZER_RESULT_INVALID_ARGUMENT;
    }
    callback_status = host->optimizer_request(
        host->context, PRDB_IPC_OP_OPTIMIZE_UPDATE, request, request_len,
        response, response_capacity, response_len);
    if (callback_status == PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL) {
        if (*response_len > PRDB_QUERY_MAX_IPC_REQUEST_BYTES)
            return PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE;
        return PRDB_SDK_OPTIMIZER_RESULT_RESPONSE_TOO_SMALL;
    }
    if (callback_status == PRDB_QUERY_CALLBACK_LIMIT_EXCEEDED)
        return PRDB_SDK_OPTIMIZER_RESULT_LIMIT_EXCEEDED;
    if (callback_status != PRDB_QUERY_CALLBACK_OK)
        return PRDB_SDK_OPTIMIZER_RESULT_CALLBACK_ERROR;
    if (*response_len > response_capacity)
        return PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE;
    return prdb_sdk_optimizer_decode_response_v1(response, *response_len, out,
                                                 out_capacity);
}

int32_t prdb_sdk_optimizer_submit_join_select_v1(
    const prdb_host_api_v1 *host, const uint8_t *request,
    uint64_t request_len, uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len, prdb_sdk_optimizer_response_v1 *out,
    uint32_t out_capacity) {
    int32_t callback_status;
    if (host == NULL || host->struct_size != sizeof(prdb_host_api_v1) ||
        host->abi_version != PRDB_QUERY_ABI_VERSION ||
        host->context == NULL || host->optimizer_request == NULL || request_len == 0 || request == NULL ||
        request_len > OPTIMIZER_REQUEST_MAX_BYTES || response_len == NULL ||
        response_capacity > PRDB_QUERY_MAX_IPC_REQUEST_BYTES ||
        (response_capacity != 0 && response == NULL) || out == NULL ||
        out_capacity < sizeof(*out) ||
        ranges_overlap(out, sizeof(*out), response, response_capacity)) {
        return request_len > OPTIMIZER_REQUEST_MAX_BYTES ||
                       response_capacity > PRDB_QUERY_MAX_IPC_REQUEST_BYTES
                   ? PRDB_SDK_OPTIMIZER_RESULT_LIMIT_EXCEEDED
                   : PRDB_SDK_OPTIMIZER_RESULT_INVALID_ARGUMENT;
    }
    callback_status = host->optimizer_request(
        host->context, PRDB_IPC_OP_OPTIMIZE_JOIN_SELECT, request,
        request_len, response, response_capacity, response_len);
    if (callback_status == PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL) {
        if (*response_len > PRDB_QUERY_MAX_IPC_REQUEST_BYTES)
            return PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE;
        return PRDB_SDK_OPTIMIZER_RESULT_RESPONSE_TOO_SMALL;
    }
    if (callback_status == PRDB_QUERY_CALLBACK_LIMIT_EXCEEDED)
        return PRDB_SDK_OPTIMIZER_RESULT_LIMIT_EXCEEDED;
    if (callback_status != PRDB_QUERY_CALLBACK_OK)
        return PRDB_SDK_OPTIMIZER_RESULT_CALLBACK_ERROR;
    if (*response_len > response_capacity)
        return PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE;
    return prdb_sdk_optimizer_decode_response_v1(response, *response_len, out,
                                                 out_capacity);
}

int32_t prdb_sdk_optimizer_submit_group_count_v1(
    const prdb_host_api_v1 *host, const uint8_t *request,
    uint64_t request_len, uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len, prdb_sdk_optimizer_response_v1 *out,
    uint32_t out_capacity) {
    int32_t callback_status;
    if (host == NULL || host->struct_size != sizeof(prdb_host_api_v1) ||
        host->abi_version != PRDB_QUERY_ABI_VERSION ||
        host->context == NULL || host->optimizer_request == NULL || request_len == 0 || request == NULL ||
        request_len > OPTIMIZER_REQUEST_MAX_BYTES || response_len == NULL ||
        response_capacity > PRDB_QUERY_MAX_IPC_REQUEST_BYTES ||
        (response_capacity != 0 && response == NULL) || out == NULL ||
        out_capacity < sizeof(*out) ||
        ranges_overlap(out, sizeof(*out), response, response_capacity)) {
        return request_len > OPTIMIZER_REQUEST_MAX_BYTES ||
                       response_capacity > PRDB_QUERY_MAX_IPC_REQUEST_BYTES
                   ? PRDB_SDK_OPTIMIZER_RESULT_LIMIT_EXCEEDED
                   : PRDB_SDK_OPTIMIZER_RESULT_INVALID_ARGUMENT;
    }
    callback_status = host->optimizer_request(
        host->context, PRDB_IPC_OP_OPTIMIZE_GROUP_COUNT, request,
        request_len, response, response_capacity, response_len);
    if (callback_status == PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL) {
        if (*response_len > PRDB_QUERY_MAX_IPC_REQUEST_BYTES)
            return PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE;
        return PRDB_SDK_OPTIMIZER_RESULT_RESPONSE_TOO_SMALL;
    }
    if (callback_status == PRDB_QUERY_CALLBACK_LIMIT_EXCEEDED)
        return PRDB_SDK_OPTIMIZER_RESULT_LIMIT_EXCEEDED;
    if (callback_status != PRDB_QUERY_CALLBACK_OK)
        return PRDB_SDK_OPTIMIZER_RESULT_CALLBACK_ERROR;
    if (*response_len > response_capacity)
        return PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE;
    return prdb_sdk_optimizer_decode_response_v1(response, *response_len, out,
                                                 out_capacity);
}
