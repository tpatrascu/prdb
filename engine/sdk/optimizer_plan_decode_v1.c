#include "optimizer_plan_codec_internal.h"

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
        if (prdb_sdk_internal_columns_equal(&relation->scan_columns[index], column)) return 1;
    }
    return 0;
}

static int32_t decode_join_plan_internal(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_join_plan_v1 *out, uint32_t out_capacity,
    uint8_t expected_kind, prdb_sdk_optimizer_join_filter_v1 *filter_out,
    uint32_t *has_filter_out, uint32_t *outer_side_out,
    uint32_t *inner_side_out, uint8_t index_id_out[16],
    prdb_sdk_optimizer_plan_column_v1 *index_key_out,
    prdb_sdk_optimizer_plan_column_v1 *index_prefix_out,
    uint32_t index_prefix_capacity, uint32_t *index_prefix_count_out) {
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
        prdb_sdk_internal_ranges_overlap(payload, payload_len, out, sizeof(*out)))
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
    if (!prdb_sdk_internal_read_name(&reader, &decoded.schema_name,
                   &decoded.schema_name_len) ||
        !prdb_sdk_internal_take(&reader, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_schema_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
            decoded.schema_id, &decoded.schema_version) ||
        prdb_sdk_internal_all_zero(decoded.schema_id, 16) || decoded.schema_version == 0)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;

    for (side = 0; side < 2; ++side) {
        prdb_sdk_optimizer_join_relation_v1 *relation =
            side == 0 ? &decoded.left_relation : &decoded.right_relation;
        if (!prdb_sdk_internal_read_name(&reader, &relation->relation_name,
                       &relation->relation_name_len) ||
            !prdb_sdk_internal_take(&reader,
                  PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
                  &bytes) ||
            !prdb_decode_optimizer_plan_relation_reference_record(
                bytes,
                PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
                relation->relation_id, &relation->table_epoch) ||
            prdb_sdk_internal_all_zero(relation->relation_id, 16) || relation->table_epoch == 0)
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    for (side = 0; side < 2; ++side) {
        prdb_sdk_optimizer_join_relation_v1 *relation =
            side == 0 ? &decoded.left_relation : &decoded.right_relation;
        if (!prdb_sdk_internal_take(&reader, 2, &bytes))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        count = prdb_sdk_internal_read_be_u16(bytes);
        if (count == 0 || count > PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS)
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        relation->scan_column_count = count;
        for (index = 0; index < relation->scan_column_count; ++index) {
            prdb_sdk_optimizer_plan_column_v1 *column =
                &relation->scan_columns[index];
            if (!prdb_sdk_internal_read_column(&reader, column) ||
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
    if (!prdb_sdk_internal_take(&reader, 2, &bytes))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    count = prdb_sdk_internal_read_be_u16(bytes);
    if (count == 0 || count > PRDB_SDK_OPTIMIZER_PLAN_MAX_PROJECTIONS)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.projection_count = count;
    for (index = 0; index < decoded.projection_count; ++index) {
        prdb_sdk_optimizer_join_projection_v1 *projection =
            &decoded.projections[index];
        if (!prdb_sdk_internal_read_u8(&reader, &flag) || flag > 1 ||
            !prdb_sdk_internal_read_column(&reader, &projection->column))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        projection->relation_side = flag;
        if (!join_scan_contains(flag == 0 ? &decoded.left_relation
                                          : &decoded.right_relation,
                                &projection->column))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (!prdb_sdk_internal_read_u8(&reader, &flag) || flag < 2 || flag > 8)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.join_key_count = flag;
    for (index = 0; index < decoded.join_key_count; ++index) {
        prdb_sdk_optimizer_join_key_v1 *key = &decoded.join_keys[index];
        if (!prdb_sdk_internal_read_column(&reader, &key->left_column) ||
            !prdb_sdk_internal_read_column(&reader, &key->right_column) ||
            !join_scan_contains(&decoded.left_relation, &key->left_column) ||
            !join_scan_contains(&decoded.right_relation, &key->right_column) ||
            !join_key_types_match(&key->left_column, &key->right_column))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (!prdb_sdk_internal_read_u8(&reader, &flag) || flag > 1)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.has_limit = flag;
    if (decoded.has_limit) {
        if (!prdb_sdk_internal_take(&reader, 4, &bytes))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        decoded.limit = ((uint32_t)bytes[0] << 24) |
                        ((uint32_t)bytes[1] << 16) |
                        ((uint32_t)bytes[2] << 8) | bytes[3];
    }
    uint32_t has_filter =
        kind == PRDB_SDK_OPTIMIZER_PLAN_KIND_FILTERED_JOIN_SELECT;
    if (kind == PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_NESTED_LOOP_JOIN ||
        kind == PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_PREFIX_NESTED_LOOP_JOIN) {
        if (!prdb_sdk_internal_read_u8(&reader, &flag) || flag > 1)
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        has_filter = flag;
    }
    if (has_filter) {
        uint8_t expression_kind;
        if (!prdb_sdk_internal_read_u8(&reader, &flag) || flag > 1 ||
            !prdb_sdk_internal_read_column(&reader, &decoded_filter.column))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        decoded_filter.relation_side = flag;
        if (!join_scan_contains(flag == 0 ? &decoded.left_relation
                                          : &decoded.right_relation,
                                &decoded_filter.column) ||
            !prdb_sdk_internal_read_u8(&reader, &expression_kind) || expression_kind > 2)
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        decoded_filter.expression_kind = expression_kind;
        if (expression_kind == 0) {
            uint8_t comparison;
            if (!prdb_sdk_internal_read_u8(&reader, &comparison) || comparison > 5 ||
                !prdb_sdk_internal_take(&reader, 4, &bytes))
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
    if (kind == PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_NESTED_LOOP_JOIN ||
        kind == PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_PREFIX_NESTED_LOOP_JOIN) {
        uint32_t outer_side;
        uint32_t inner_side;
        uint8_t nonzero = 0;
        int key_found = 0;
        if (filter_out == NULL || has_filter_out == NULL ||
            outer_side_out == NULL || inner_side_out == NULL ||
            index_id_out == NULL || index_key_out == NULL ||
            !prdb_sdk_internal_read_u8(&reader, &flag) || flag > 1)
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        outer_side = flag;
        if (!prdb_sdk_internal_read_u8(&reader, &flag) || flag > 1)
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        inner_side = flag;
        if (outer_side == inner_side || !prdb_sdk_internal_take(&reader, 16, &bytes))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        memcpy(index_id_out, bytes, 16);
        for (uint32_t byte_index = 0; byte_index < 16; ++byte_index)
            nonzero |= index_id_out[byte_index];
        uint32_t prefix_count = 1;
        if (kind == PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_PREFIX_NESTED_LOOP_JOIN) {
            if (index_prefix_out == NULL || index_prefix_count_out == NULL ||
                index_prefix_capacity < 2 || !prdb_sdk_internal_read_u8(&reader, &flag) ||
                flag < 2 || flag > 8 || flag > index_prefix_capacity)
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            prefix_count = flag;
        }
        if (nonzero == 0 || index_key_out == NULL)
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        for (uint32_t prefix_index = 0; prefix_index < prefix_count;
             ++prefix_index) {
            prdb_sdk_optimizer_plan_column_v1 column = {0};
            if (!prdb_sdk_internal_read_column(&reader, &column) ||
                !join_scan_contains(inner_side == 0 ? &decoded.left_relation
                                                    : &decoded.right_relation,
                                    &column) ||
                !prdb_sdk_internal_valid_type(column.type_code, column.type_parameters))
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            if (prefix_index == 0) *index_key_out = column;
            if (index_prefix_out != NULL) index_prefix_out[prefix_index] = column;
            key_found = 0;
            for (index = 0; index < decoded.join_key_count; ++index) {
                const prdb_sdk_optimizer_join_key_v1 *key =
                    &decoded.join_keys[index];
                const prdb_sdk_optimizer_plan_column_v1 *inner_key =
                    inner_side == 0 ? &key->left_column : &key->right_column;
                if (prdb_sdk_internal_columns_equal(inner_key, &column)) key_found = 1;
            }
            if (!key_found) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            if (kind == PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_PREFIX_NESTED_LOOP_JOIN)
                for (uint32_t prior = 0; prior < prefix_index; ++prior)
                    if (memcmp(index_prefix_out[prior].column_id,
                               column.column_id, 16) == 0)
                        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        if (index_prefix_count_out != NULL) *index_prefix_count_out = prefix_count;
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
                                     NULL, NULL, NULL, NULL, NULL, NULL,
                                     NULL, 0, NULL);
}

int32_t prdb_sdk_optimizer_decode_filtered_join_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_filtered_join_plan_v1 *out, uint32_t out_capacity) {
    prdb_sdk_optimizer_filtered_join_plan_v1 decoded = {0};
    int32_t status;
    if (payload == NULL || out == NULL || out_capacity < sizeof(*out) ||
        payload_len > PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES ||
        prdb_sdk_internal_ranges_overlap(payload, payload_len, out, sizeof(*out)))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    status = decode_join_plan_internal(
        payload, payload_len, &decoded.join, sizeof(decoded.join),
        PRDB_SDK_OPTIMIZER_PLAN_KIND_FILTERED_JOIN_SELECT, &decoded.filter,
        NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL);
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
        prdb_sdk_internal_ranges_overlap(payload, payload_len, out, sizeof(*out)))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    status = decode_join_plan_internal(
        payload, payload_len, &decoded.join, sizeof(decoded.join),
        PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_NESTED_LOOP_JOIN,
        &decoded.filter, &decoded.has_filter, &decoded.outer_side,
        &decoded.inner_side, decoded.index_id, &decoded.key_column,
        NULL, 0, NULL);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    decoded.struct_size = sizeof(decoded);
    decoded.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    *out = decoded;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_decode_index_prefix_join_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_index_prefix_join_plan_v1 *out,
    uint32_t out_capacity) {
    prdb_sdk_optimizer_index_prefix_join_plan_v1 decoded = {0};
    int32_t status;
    if (payload == NULL || out == NULL || out_capacity < sizeof(*out) ||
        payload_len > PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES ||
        prdb_sdk_internal_ranges_overlap(payload, payload_len, out, sizeof(*out)))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    status = decode_join_plan_internal(
        payload, payload_len, &decoded.join, sizeof(decoded.join),
        PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_PREFIX_NESTED_LOOP_JOIN,
        &decoded.filter, &decoded.has_filter, &decoded.outer_side,
        &decoded.inner_side, decoded.index_id, &decoded.prefix_columns[0],
        decoded.prefix_columns, 8, &decoded.prefix_column_count);
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
        prdb_sdk_internal_ranges_overlap(payload, payload_len, out, sizeof(*out))) {
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
    if (!prdb_sdk_internal_read_name(&reader, &decoded.plan.schema_name,
                   &decoded.plan.schema_name_len) ||
        !prdb_sdk_internal_take(&reader, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_schema_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.schema_id, &decoded.plan.schema_version) ||
        prdb_sdk_internal_all_zero(decoded.plan.schema_id, 16) ||
        decoded.plan.schema_version == 0 ||
        !prdb_sdk_internal_read_name(&reader, &decoded.plan.relation_name,
                   &decoded.plan.relation_name_len) ||
        !prdb_sdk_internal_take(&reader, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_relation_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.relation_id, &decoded.plan.table_epoch) ||
        prdb_sdk_internal_all_zero(decoded.plan.relation_id, 16) ||
        decoded.plan.table_epoch == 0 || !prdb_sdk_internal_take(&reader, 2, &bytes)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    scan_count = prdb_sdk_internal_read_be_u16(bytes);
    if (scan_count != 1 || !prdb_sdk_internal_read_column(&reader, &decoded.plan.scan_columns[0]) ||
        !prdb_sdk_internal_read_u8(&reader, &tag) || tag != 7 ||
        !prdb_sdk_internal_read_column(&reader, &decoded.group_key) ||
        !prdb_sdk_internal_columns_equal(&decoded.plan.scan_columns[0], &decoded.group_key) ||
        !prdb_sdk_internal_read_u8(&reader, &flag) || flag > 1) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    decoded.plan.scan_column_count = 1;
    decoded.plan.projection_count = 1;
    decoded.plan.projections[0] = decoded.group_key;
    decoded.plan.plan_kind = 5;
    decoded.plan.has_limit = flag;
    if (flag != 0) {
        if (!prdb_sdk_internal_take(&reader, 4, &bytes)) {
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
        prdb_sdk_internal_ranges_overlap(payload, payload_len, out, sizeof(*out)))
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
    if (!prdb_sdk_internal_read_name(&reader, &decoded.plan.schema_name,
                   &decoded.plan.schema_name_len) ||
        !prdb_sdk_internal_take(&reader, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_schema_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.schema_id, &decoded.plan.schema_version) ||
        prdb_sdk_internal_all_zero(decoded.plan.schema_id, 16) || decoded.plan.schema_version == 0 ||
        !prdb_sdk_internal_read_name(&reader, &decoded.plan.relation_name,
                   &decoded.plan.relation_name_len) ||
        !prdb_sdk_internal_take(&reader, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_relation_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.relation_id, &decoded.plan.table_epoch) ||
        prdb_sdk_internal_all_zero(decoded.plan.relation_id, 16) || decoded.plan.table_epoch == 0 ||
        !prdb_sdk_internal_take(&reader, 2, &bytes))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    scan_count = prdb_sdk_internal_read_be_u16(bytes);
    if (scan_count != 1 ||
        !prdb_sdk_internal_read_column(&reader, &decoded.plan.scan_columns[0]) ||
        !prdb_sdk_internal_read_u8(&reader, &tag) || tag != 7 ||
        !prdb_sdk_internal_read_column(&reader, &decoded.group_key) ||
        !prdb_sdk_internal_columns_equal(&decoded.plan.scan_columns[0], &decoded.group_key) ||
        !prdb_sdk_internal_read_u8(&reader, &flag) || flag == 0 || flag > 2)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.order_count = flag;
    for (uint32_t index = 0; index < decoded.order_count; ++index) {
        uint8_t source, descending, nulls_first;
        if (!prdb_sdk_internal_read_u8(&reader, &source) || source > 1 ||
            (seen & (uint8_t)(1U << source)) != 0 ||
            !prdb_sdk_internal_read_u8(&reader, &descending) || descending > 1 ||
            !prdb_sdk_internal_read_u8(&reader, &nulls_first) || nulls_first > 1)
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        seen |= (uint8_t)(1U << source);
        decoded.order_items[index].source = source;
        decoded.order_items[index].descending = descending;
        decoded.order_items[index].nulls_first = nulls_first;
    }
    if (!prdb_sdk_internal_read_u8(&reader, &flag) || flag > 1)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.plan.has_limit = flag;
    if (flag != 0) {
        if (!prdb_sdk_internal_take(&reader, 4, &bytes))
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
        prdb_sdk_internal_ranges_overlap(payload, payload_len, out, sizeof(*out))) {
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
    if (!prdb_sdk_internal_read_name(&reader, &decoded.plan.schema_name,
                   &decoded.plan.schema_name_len) ||
        !prdb_sdk_internal_take(&reader, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_schema_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.schema_id, &decoded.plan.schema_version) ||
        prdb_sdk_internal_all_zero(decoded.plan.schema_id, 16) ||
        decoded.plan.schema_version == 0 ||
        !prdb_sdk_internal_read_name(&reader, &decoded.plan.relation_name,
                   &decoded.plan.relation_name_len) ||
        !prdb_sdk_internal_take(&reader, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_relation_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.relation_id, &decoded.plan.table_epoch) ||
        prdb_sdk_internal_all_zero(decoded.plan.relation_id, 16) ||
        decoded.plan.table_epoch == 0 || !prdb_sdk_internal_take(&reader, 2, &bytes)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    scan_count = prdb_sdk_internal_read_be_u16(bytes);
    if (scan_count == 0 ||
        scan_count > PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    decoded.plan.scan_column_count = scan_count;
    for (index = 0; index < scan_count; ++index) {
        if (!prdb_sdk_internal_read_column(&reader, &decoded.plan.scan_columns[index]) ||
            (index != 0 &&
             decoded.plan.scan_columns[index - 1].ordinal >=
                 decoded.plan.scan_columns[index].ordinal)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
    }
    if (!prdb_sdk_internal_take(&reader, 2, &bytes)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    projection_count = prdb_sdk_internal_read_be_u16(bytes);
    if (projection_count == 0 ||
        projection_count > PRDB_SDK_OPTIMIZER_PLAN_MAX_PROJECTIONS) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    decoded.plan.projection_count = projection_count;
    for (index = 0; index < projection_count; ++index) {
        prdb_sdk_optimizer_plan_column_v1 *projection =
            &decoded.plan.projections[index];
        if (!prdb_sdk_internal_read_column(&reader, projection)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        for (other = 0; other < scan_count; ++other) {
            if (prdb_sdk_internal_columns_equal(projection, &decoded.plan.scan_columns[other])) {
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
    if (!prdb_sdk_internal_read_u8(&reader, &tag) || tag != 9 ||
        !prdb_sdk_internal_read_column(&reader, &decoded.order_key)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    for (other = 0; other < scan_count; ++other) {
        if (prdb_sdk_internal_columns_equal(&decoded.order_key,
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
    if (!prdb_sdk_internal_read_u8(&reader, &flag) || flag > 1) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    decoded.descending = flag;
    if (!prdb_sdk_internal_read_u8(&reader, &flag) || flag > 1) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    decoded.plan.has_limit = flag;
    if (flag != 0) {
        if (!prdb_sdk_internal_take(&reader, 4, &bytes)) {
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
        prdb_sdk_internal_ranges_overlap(payload, payload_len, out, sizeof(*out)))
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
    if (!prdb_sdk_internal_read_name(&reader, &decoded.plan.schema_name,
                   &decoded.plan.schema_name_len) ||
        !prdb_sdk_internal_take(&reader, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_schema_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.schema_id, &decoded.plan.schema_version) ||
        prdb_sdk_internal_all_zero(decoded.plan.schema_id, 16) ||
        decoded.plan.schema_version == 0 ||
        !prdb_sdk_internal_read_name(&reader, &decoded.plan.relation_name,
                   &decoded.plan.relation_name_len) ||
        !prdb_sdk_internal_take(&reader, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_relation_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.relation_id, &decoded.plan.table_epoch) ||
        prdb_sdk_internal_all_zero(decoded.plan.relation_id, 16) ||
        decoded.plan.table_epoch == 0 || !prdb_sdk_internal_take(&reader, 2, &bytes))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    scan_count = prdb_sdk_internal_read_be_u16(bytes);
    if (scan_count < 2 || scan_count > 8)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    for (uint32_t index = 0; index < scan_count; ++index) {
        if (!prdb_sdk_internal_read_column(&reader, &decoded.plan.scan_columns[index]) ||
            (index != 0 && decoded.plan.scan_columns[index - 1].ordinal >=
                               decoded.plan.scan_columns[index].ordinal))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (!prdb_sdk_internal_read_u8(&reader, &tag) || tag != 8 || !prdb_sdk_internal_read_u8(&reader, &key_count) ||
        key_count < 2 || key_count > 8 || key_count != scan_count)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    for (uint32_t key_index = 0; key_index < key_count; ++key_index) {
        prdb_sdk_optimizer_plan_column_v1 *key =
            &decoded.group_keys[key_index];
        if (!prdb_sdk_internal_read_column(&reader, key))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        for (uint32_t prior = 0; prior < key_index; ++prior) {
            if (decoded.group_keys[prior].ordinal == key->ordinal)
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        int found = 0;
        for (uint32_t scan_index = 0; scan_index < scan_count; ++scan_index) {
            if (prdb_sdk_internal_columns_equal(key, &decoded.plan.scan_columns[scan_index])) {
                found = 1;
                break;
            }
        }
        if (!found)
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        decoded.plan.projections[key_index] = *key;
    }
    if (!prdb_sdk_internal_read_u8(&reader, &flag) || flag > 1)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.plan.scan_column_count = scan_count;
    decoded.group_key_count = key_count;
    decoded.plan.projection_count = key_count;
    decoded.plan.plan_kind = 6;
    decoded.plan.has_limit = flag;
    if (flag != 0) {
        if (!prdb_sdk_internal_take(&reader, 4, &bytes))
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
        prdb_sdk_internal_ranges_overlap(payload, payload_len, out, sizeof(*out)))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    reader.bytes = payload; reader.length = payload_len; reader.offset = 0;
    if (!prdb_decode_optimizer_plan_header_record(payload, PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES, &version, &kind, &reserved) || version != 1 || kind != 9 || reserved != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    reader.offset = PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    if (!prdb_sdk_internal_read_name(&reader, &decoded.plan.schema_name, &decoded.plan.schema_name_len) ||
        !prdb_sdk_internal_take(&reader, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES, &bytes) ||
        !prdb_decode_optimizer_plan_schema_reference_record(bytes, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES, decoded.plan.schema_id, &decoded.plan.schema_version) || prdb_sdk_internal_all_zero(decoded.plan.schema_id, 16) || decoded.plan.schema_version == 0 ||
        !prdb_sdk_internal_read_name(&reader, &decoded.plan.relation_name, &decoded.plan.relation_name_len) ||
        !prdb_sdk_internal_take(&reader, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES, &bytes) ||
        !prdb_decode_optimizer_plan_relation_reference_record(bytes, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES, decoded.plan.relation_id, &decoded.plan.table_epoch) || prdb_sdk_internal_all_zero(decoded.plan.relation_id, 16) || decoded.plan.table_epoch == 0 || !prdb_sdk_internal_take(&reader, 2, &bytes))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    scan_count = prdb_sdk_internal_read_be_u16(bytes);
    if (scan_count < 2 || scan_count > 8) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    for (uint32_t i = 0; i < scan_count; ++i)
        if (!prdb_sdk_internal_read_column(&reader, &decoded.plan.scan_columns[i]) || (i && decoded.plan.scan_columns[i-1].ordinal >= decoded.plan.scan_columns[i].ordinal)) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (!prdb_sdk_internal_read_u8(&reader, &tag) || tag != 10 || !prdb_sdk_internal_read_u8(&reader, &flag) || flag < 2 || flag > 8 || flag != scan_count) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.group_key_count = flag;
    for (uint32_t i = 0; i < decoded.group_key_count; ++i) {
        if (!prdb_sdk_internal_read_column(&reader, &decoded.group_keys[i])) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        for (uint32_t j = 0; j < i; ++j) if (decoded.group_keys[j].ordinal == decoded.group_keys[i].ordinal) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        uint32_t found = 0;
        for (uint32_t j = 0; j < scan_count; ++j) if (prdb_sdk_internal_columns_equal(&decoded.group_keys[i], &decoded.plan.scan_columns[j])) found = 1;
        if (!found) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        decoded.plan.projections[i] = decoded.group_keys[i];
    }
    if (!prdb_sdk_internal_read_u8(&reader, &flag) || flag == 0 || flag > 9) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.order_count = flag;
    for (uint32_t i = 0; i < decoded.order_count; ++i) {
        uint8_t source, index, descending, nulls_first;
        if (!prdb_sdk_internal_read_u8(&reader, &source) || source > 1 || !prdb_sdk_internal_read_u8(&reader, &index) || !prdb_sdk_internal_read_u8(&reader, &descending) || descending > 1 || !prdb_sdk_internal_read_u8(&reader, &nulls_first) || nulls_first > 1) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        if (source == 0) { if (index >= decoded.group_key_count || (seen_keys & (1u << index))) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD; seen_keys |= (uint16_t)(1u << index); }
        else { if (index != UINT8_MAX || seen_count) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD; seen_count = 1; }
        decoded.order_items[i].source = source;
        decoded.order_items[i].group_key_index = source == 0 ? index : UINT32_MAX;
        decoded.order_items[i].descending = descending;
        decoded.order_items[i].nulls_first = nulls_first;
    }
    if (!prdb_sdk_internal_read_u8(&reader, &flag) || flag > 1) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.plan.has_limit = flag;
    if (flag && (!prdb_sdk_internal_take(&reader, 4, &bytes))) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
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
    if (payload == NULL || out == NULL || out_capacity < sizeof(*out) || payload_len > PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES || prdb_sdk_internal_ranges_overlap(payload, payload_len, out, sizeof(*out)))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    reader.bytes = payload; reader.length = payload_len; reader.offset = 0;
    if (!prdb_decode_optimizer_plan_header_record(payload, PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES, &version, &kind, &reserved) || version != 1 || kind != 10 || reserved != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    reader.offset = PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    if (!prdb_sdk_internal_read_name(&reader, &decoded.plan.schema_name, &decoded.plan.schema_name_len) ||
        !prdb_sdk_internal_take(&reader, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES, &bytes) ||
        !prdb_decode_optimizer_plan_schema_reference_record(bytes, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES, decoded.plan.schema_id, &decoded.plan.schema_version) || prdb_sdk_internal_all_zero(decoded.plan.schema_id, 16) || decoded.plan.schema_version == 0 ||
        !prdb_sdk_internal_read_name(&reader, &decoded.plan.relation_name, &decoded.plan.relation_name_len) ||
        !prdb_sdk_internal_take(&reader, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES, &bytes) ||
        !prdb_decode_optimizer_plan_relation_reference_record(bytes, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES, decoded.plan.relation_id, &decoded.plan.table_epoch) || prdb_sdk_internal_all_zero(decoded.plan.relation_id, 16) || decoded.plan.table_epoch == 0 || !prdb_sdk_internal_take(&reader, 2, &bytes))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    scan_count = prdb_sdk_internal_read_be_u16(bytes);
    if (scan_count < 1 || scan_count > 2) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    for (uint32_t i = 0; i < scan_count; ++i)
        if (!prdb_sdk_internal_read_column(&reader, &decoded.plan.scan_columns[i]) || (i && decoded.plan.scan_columns[i-1].ordinal >= decoded.plan.scan_columns[i].ordinal)) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (!prdb_sdk_internal_read_u8(&reader, &tag) || tag != 11 || !prdb_sdk_internal_read_u8(&reader, &aggregate_kind) || aggregate_kind < 1 || aggregate_kind > PRDB_SDK_SQL_V7_AGGREGATE_COUNT_COLUMN || !prdb_sdk_internal_read_column(&reader, &decoded.group_key) || !prdb_sdk_internal_read_u8(&reader, &flag) || flag > 1)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.aggregate_kind = aggregate_kind;
    decoded.has_aggregate_column = flag;
    if ((aggregate_kind == 1 && flag != 0) || (aggregate_kind != 1 && flag != 1)) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (flag && !prdb_sdk_internal_read_column(&reader, &decoded.aggregate_column)) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if ((aggregate_kind == 4 || aggregate_kind == 5) && decoded.aggregate_column.type_code != PRDB_SDK_VALUE_TYPE_SMALLINT && decoded.aggregate_column.type_code != PRDB_SDK_VALUE_TYPE_INTEGER && decoded.aggregate_column.type_code != PRDB_SDK_VALUE_TYPE_BIGINT && decoded.aggregate_column.type_code != PRDB_SDK_VALUE_TYPE_DECIMAL)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    uint32_t expected_scan = 0;
    for (uint32_t i = 0; i < scan_count; ++i) {
        if (prdb_sdk_internal_columns_equal(&decoded.group_key, &decoded.plan.scan_columns[i])) expected_scan |= 1;
        if (flag && prdb_sdk_internal_columns_equal(&decoded.aggregate_column, &decoded.plan.scan_columns[i])) expected_scan |= 2;
    }
    if (!(expected_scan & 1) || (flag && !(expected_scan & 2))) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if ((!flag || prdb_sdk_internal_columns_equal(&decoded.group_key, &decoded.aggregate_column)) && scan_count != 1) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (flag && !prdb_sdk_internal_columns_equal(&decoded.group_key, &decoded.aggregate_column) && scan_count != 2) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (!prdb_sdk_internal_read_u8(&reader, &flag) || flag > 1) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.plan.has_limit = flag;
    if (flag && !prdb_sdk_internal_take(&reader, 4, &bytes)) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
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
        prdb_sdk_internal_ranges_overlap(payload, payload_len, out, sizeof(*out)))
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
    if (!prdb_sdk_internal_read_name(&reader, &decoded.plan.schema_name,
                   &decoded.plan.schema_name_len) ||
        !prdb_sdk_internal_take(&reader, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_schema_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.schema_id, &decoded.plan.schema_version) ||
        prdb_sdk_internal_all_zero(decoded.plan.schema_id, 16) || decoded.plan.schema_version == 0 ||
        !prdb_sdk_internal_read_name(&reader, &decoded.plan.relation_name,
                   &decoded.plan.relation_name_len) ||
        !prdb_sdk_internal_take(&reader, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_relation_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.relation_id, &decoded.plan.table_epoch) ||
        prdb_sdk_internal_all_zero(decoded.plan.relation_id, 16) || decoded.plan.table_epoch == 0 ||
        !prdb_sdk_internal_take(&reader, 2, &bytes))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    scan_count = prdb_sdk_internal_read_be_u16(bytes);
    if (scan_count < PRDB_SDK_SQL_V13_MIN_GROUP_KEYS ||
        scan_count > PRDB_SDK_SQL_V13_MAX_GROUP_KEYS + 1 ||
        scan_count > PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    for (uint32_t index = 0; index < scan_count; ++index) {
        if (!prdb_sdk_internal_read_column(&reader, &decoded.plan.scan_columns[index]) ||
            (index != 0 && decoded.plan.scan_columns[index - 1].ordinal >=
                               decoded.plan.scan_columns[index].ordinal))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (!prdb_sdk_internal_read_u8(&reader, &tag) || tag != 13 ||
        !prdb_sdk_internal_read_u8(&reader, &aggregate_kind) || aggregate_kind < 1 ||
        aggregate_kind > PRDB_SDK_SQL_V7_AGGREGATE_COUNT_COLUMN || !prdb_sdk_internal_read_u8(&reader, &key_count) ||
        key_count < PRDB_SDK_SQL_V13_MIN_GROUP_KEYS ||
        key_count > PRDB_SDK_SQL_V13_MAX_GROUP_KEYS)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.group_key_count = key_count;
    decoded.aggregate_kind = aggregate_kind;
    for (uint32_t index = 0; index < key_count; ++index) {
        if (!prdb_sdk_internal_read_column(&reader, &decoded.group_keys[index]))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        for (uint32_t prior = 0; prior < index; ++prior) {
            if (decoded.group_keys[prior].ordinal ==
                decoded.group_keys[index].ordinal)
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
    }
    if (!prdb_sdk_internal_read_u8(&reader, &flag) || flag > 1 ||
        flag != (aggregate_kind == 1 ? 0 : 1))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.has_aggregate_column = flag;
    if (flag && !prdb_sdk_internal_read_column(&reader, &decoded.aggregate_column))
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
            if (prdb_sdk_internal_columns_equal(&decoded.plan.scan_columns[index],
                              &decoded.group_keys[key])) {
                expected_scan |= UINT32_C(1) << key;
                if (flag && prdb_sdk_internal_columns_equal(&decoded.group_keys[key],
                                          &decoded.aggregate_column))
                    aggregate_in_keys = 1;
            }
        }
        if (flag && prdb_sdk_internal_columns_equal(&decoded.plan.scan_columns[index],
                                  &decoded.aggregate_column))
            expected_scan |= UINT32_C(1) << 8;
    }
    if ((expected_scan & ((UINT32_C(1) << key_count) - 1)) !=
            ((UINT32_C(1) << key_count) - 1) ||
        (flag && !(expected_scan & (UINT32_C(1) << 8))) ||
        (flag && !aggregate_in_keys && scan_count != key_count + 1) ||
        ((!flag || aggregate_in_keys) && scan_count != key_count))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (!prdb_sdk_internal_read_u8(&reader, &flag) || flag > 1)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.plan.has_limit = flag;
    if (flag) {
        if (!prdb_sdk_internal_take(&reader, 4, &bytes))
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
        prdb_sdk_internal_ranges_overlap(payload, payload_len, out, sizeof(*out)))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    reader = (plan_reader){payload, payload_len, 0};
    if (!prdb_decode_optimizer_plan_header_record(
            payload, PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES,
            &version, &kind, &reserved) || version != 1 || kind != 15 ||
        reserved != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    reader.offset = PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    if (!prdb_sdk_internal_read_name(&reader, &decoded.plan.schema_name,
                   &decoded.plan.schema_name_len) ||
        !prdb_sdk_internal_take(&reader, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_schema_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.schema_id, &decoded.plan.schema_version) ||
        prdb_sdk_internal_all_zero(decoded.plan.schema_id, 16) || decoded.plan.schema_version == 0 ||
        !prdb_sdk_internal_read_name(&reader, &decoded.plan.relation_name,
                   &decoded.plan.relation_name_len) ||
        !prdb_sdk_internal_take(&reader, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_relation_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.relation_id, &decoded.plan.table_epoch) ||
        prdb_sdk_internal_all_zero(decoded.plan.relation_id, 16) || decoded.plan.table_epoch == 0 ||
        !prdb_sdk_internal_take(&reader, 2, &bytes))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    scan_count = prdb_sdk_internal_read_be_u16(bytes);
    if (scan_count < 1 ||
        scan_count > PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.plan.scan_column_count = scan_count;
    for (uint32_t index = 0; index < scan_count; ++index) {
        if (!prdb_sdk_internal_read_column(&reader, &decoded.plan.scan_columns[index]) ||
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
        if (!prdb_sdk_internal_read_u8(&reader, &tag) || tag != 2 ||
            !prdb_sdk_internal_read_filter_node_tag(&reader, filter_header))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        predicate_start = reader.offset;
        if (!prdb_sdk_internal_decode_predicate(&reader, decoded.plan.scan_columns, scan_count, 1,
                              &predicate_nodes, filter_references,
                              &filter_type) || !prdb_sdk_internal_predicate_boolean(&filter_type))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        decoded.filter.struct_size = sizeof(decoded.filter);
        decoded.filter.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
        decoded.filter.present = 1;
        decoded.filter.prefix = payload + predicate_start;
        decoded.filter.prefix_len = (uint32_t)(reader.offset - predicate_start);
    }
    if (!prdb_sdk_internal_read_u8(&reader, &tag) || tag != 15 ||
        !prdb_sdk_internal_read_u8(&reader, &aggregate_kind) || aggregate_kind < 1 ||
        aggregate_kind > PRDB_SDK_SQL_V7_AGGREGATE_COUNT_COLUMN || !prdb_sdk_internal_read_u8(&reader, &key_count) ||
        key_count < 1 ||
        key_count > PRDB_SDK_SQL_V13_MAX_GROUP_KEYS)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.group_key_count = key_count;
    decoded.aggregate_kind = aggregate_kind;
    for (uint32_t index = 0; index < key_count; ++index) {
        if (!prdb_sdk_internal_read_column(&reader, &decoded.group_keys[index]))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        for (uint32_t prior = 0; prior < index; ++prior)
            if (decoded.group_keys[prior].ordinal ==
                decoded.group_keys[index].ordinal)
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (!prdb_sdk_internal_read_u8(&reader, &flag) || flag > 1 ||
        flag != (aggregate_kind == 1 ? 0 : 1))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.has_aggregate_column = flag;
    if (flag && !prdb_sdk_internal_read_column(&reader, &decoded.aggregate_column))
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
                if (prdb_sdk_internal_columns_equal(&decoded.group_keys[index],
                                  &decoded.plan.scan_columns[scan])) {
                    required[scan] = 1;
                    found = 1;
                }
            if (!found) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        if (flag) {
            uint32_t found = 0;
            for (uint32_t scan = 0; scan < scan_count; ++scan)
                if (prdb_sdk_internal_columns_equal(&decoded.aggregate_column,
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
    if (!prdb_sdk_internal_read_u8(&reader, &flag) || flag > 1)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.plan.has_limit = flag;
    if (flag) {
        if (!prdb_sdk_internal_take(&reader, 4, &bytes))
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
        prdb_sdk_internal_ranges_overlap(payload, payload_len, out, sizeof(*out)))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    reader = (plan_reader){payload, payload_len, 0};
    if (!prdb_decode_optimizer_plan_header_record(
            payload, PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES,
            &version, &kind, &reserved) || version != 1 || kind != 16 || reserved != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    reader.offset = PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    if (!prdb_sdk_internal_read_name(&reader, &decoded.plan.schema_name, &decoded.plan.schema_name_len) ||
        !prdb_sdk_internal_take(&reader, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES, &bytes) ||
        !prdb_decode_optimizer_plan_schema_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.schema_id, &decoded.plan.schema_version) ||
        prdb_sdk_internal_all_zero(decoded.plan.schema_id, 16) || decoded.plan.schema_version == 0 ||
        !prdb_sdk_internal_read_name(&reader, &decoded.plan.relation_name, &decoded.plan.relation_name_len) ||
        !prdb_sdk_internal_take(&reader, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES, &bytes) ||
        !prdb_decode_optimizer_plan_relation_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.relation_id, &decoded.plan.table_epoch) ||
        prdb_sdk_internal_all_zero(decoded.plan.relation_id, 16) || decoded.plan.table_epoch == 0 ||
        !prdb_sdk_internal_take(&reader, 2, &bytes))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    schema_name_ptr = decoded.plan.schema_name;
    relation_name_ptr = decoded.plan.relation_name;
    scan_count = prdb_sdk_internal_read_be_u16(bytes);
    if (scan_count == 0 || scan_count > PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.plan.scan_column_count = scan_count;
    for (uint32_t index = 0; index < scan_count; ++index) {
        if (!prdb_sdk_internal_read_column(&reader, &decoded.plan.scan_columns[index]) ||
            (index != 0 && decoded.plan.scan_columns[index - 1].ordinal >=
                               decoded.plan.scan_columns[index].ordinal))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    {
        uint64_t filter_header = reader.offset;
        if (!prdb_sdk_internal_read_u8(&reader, &tag) || tag != 2 || !prdb_sdk_internal_read_filter_node_tag(&reader, filter_header))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        filter_predicate_start = reader.offset;
        if (
            !prdb_sdk_internal_decode_predicate(&reader, decoded.plan.scan_columns, scan_count, 1,
                              &predicate_nodes, filter_references, &filter_type) ||
            !prdb_sdk_internal_predicate_boolean(&filter_type))
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
    if (!prdb_sdk_internal_read_u8(&reader, &tag) || tag != 16 || !prdb_sdk_internal_read_u8(&reader, &aggregate_kind) ||
        aggregate_kind < 1 || aggregate_kind > PRDB_SDK_SQL_V7_AGGREGATE_COUNT_COLUMN || !prdb_sdk_internal_read_u8(&reader, &key_count) ||
        key_count < 1 || key_count > PRDB_SDK_SQL_V13_MAX_GROUP_KEYS)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.group_key_count = key_count;
    decoded.aggregate_kind = aggregate_kind;
    for (uint32_t index = 0; index < key_count; ++index) {
        if (!prdb_sdk_internal_read_column(&reader, &decoded.group_keys[index]))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        for (uint32_t prior = 0; prior < index; ++prior)
            if (decoded.group_keys[prior].ordinal == decoded.group_keys[index].ordinal)
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (!prdb_sdk_internal_read_u8(&reader, &flag) || flag > 1 || flag != (aggregate_kind == 1 ? 0 : 1))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.has_aggregate_column = flag;
    if (flag && !prdb_sdk_internal_read_column(&reader, &decoded.aggregate_column))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    order_start = reader.offset;
    if (!prdb_sdk_internal_read_u8(&reader, &tag) || tag == 0 || tag > 9)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.order_count = tag;
    for (uint32_t index = 0; index < decoded.order_count; ++index) {
        uint8_t source, key_index = 0, descending, nulls_first;
        if (!prdb_sdk_internal_read_u8(&reader, &source)) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        if (source == 0) {
            if (!prdb_sdk_internal_read_u8(&reader, &key_index) || key_index >= key_count ||
                (seen_keys & (1u << key_index)))
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            seen_keys |= (uint8_t)(1u << key_index);
        } else if (source == 1 && !seen_aggregate) {
            seen_aggregate = 1;
        } else {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        if (!prdb_sdk_internal_read_u8(&reader, &descending) || descending > 1 ||
            !prdb_sdk_internal_read_u8(&reader, &nulls_first) || nulls_first > 1)
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        decoded.order_items[index] = (prdb_sdk_optimizer_composite_order_item_v1){
            source, key_index, descending, nulls_first};
    }
    order_end = reader.offset;
    if (!prdb_sdk_internal_read_u8(&reader, &flag) || flag > 1 ||
        (flag && !prdb_sdk_internal_take(&reader, 4, &bytes)) || reader.offset != reader.length)
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
        prdb_sdk_internal_ranges_overlap(payload, payload_len, out, sizeof(*out)))
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
    if (!prdb_sdk_internal_read_name(&reader, &decoded.plan.schema_name,
                   &decoded.plan.schema_name_len) ||
        !prdb_sdk_internal_take(&reader, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_schema_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.schema_id, &decoded.plan.schema_version) ||
        prdb_sdk_internal_all_zero(decoded.plan.schema_id, 16) || decoded.plan.schema_version == 0 ||
        !prdb_sdk_internal_read_name(&reader, &decoded.plan.relation_name,
                   &decoded.plan.relation_name_len) ||
        !prdb_sdk_internal_take(&reader, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_relation_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.relation_id, &decoded.plan.table_epoch) ||
        prdb_sdk_internal_all_zero(decoded.plan.relation_id, 16) || decoded.plan.table_epoch == 0 ||
        !prdb_sdk_internal_take(&reader, 2, &bytes))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    scan_count = prdb_sdk_internal_read_be_u16(bytes);
    if (scan_count < PRDB_SDK_SQL_V13_MIN_GROUP_KEYS ||
        scan_count > PRDB_SDK_SQL_V13_MAX_GROUP_KEYS + 1 ||
        scan_count > PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    for (uint32_t index = 0; index < scan_count; ++index) {
        if (!prdb_sdk_internal_read_column(&reader, &decoded.plan.scan_columns[index]) ||
            (index != 0 && decoded.plan.scan_columns[index - 1].ordinal >=
                               decoded.plan.scan_columns[index].ordinal))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (!prdb_sdk_internal_read_u8(&reader, &tag) || tag != 14 ||
        !prdb_sdk_internal_read_u8(&reader, &aggregate_kind) || aggregate_kind < 1 ||
        aggregate_kind > PRDB_SDK_SQL_V7_AGGREGATE_COUNT_COLUMN || !prdb_sdk_internal_read_u8(&reader, &key_count) ||
        key_count < PRDB_SDK_SQL_V13_MIN_GROUP_KEYS ||
        key_count > PRDB_SDK_SQL_V13_MAX_GROUP_KEYS)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.group_key_count = key_count;
    decoded.aggregate_kind = aggregate_kind;
    for (uint32_t index = 0; index < key_count; ++index) {
        if (!prdb_sdk_internal_read_column(&reader, &decoded.group_keys[index]))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        for (uint32_t prior = 0; prior < index; ++prior)
            if (decoded.group_keys[prior].ordinal ==
                decoded.group_keys[index].ordinal)
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (!prdb_sdk_internal_read_u8(&reader, &flag) || flag > 1 ||
        flag != (aggregate_kind == 1 ? 0 : 1))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.has_aggregate_column = flag;
    if (flag && !prdb_sdk_internal_read_column(&reader, &decoded.aggregate_column))
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
            if (prdb_sdk_internal_columns_equal(&decoded.plan.scan_columns[index],
                              &decoded.group_keys[key])) {
                expected_scan |= UINT32_C(1) << key;
                if (flag && prdb_sdk_internal_columns_equal(&decoded.group_keys[key],
                                          &decoded.aggregate_column))
                    aggregate_in_keys = 1;
            }
        }
        if (flag && prdb_sdk_internal_columns_equal(&decoded.plan.scan_columns[index],
                                  &decoded.aggregate_column))
            expected_scan |= UINT32_C(1) << 8;
    }
    if ((expected_scan & ((UINT32_C(1) << key_count) - 1)) !=
            ((UINT32_C(1) << key_count) - 1) ||
        (flag && !(expected_scan & (UINT32_C(1) << 8))) ||
        (flag && !aggregate_in_keys && scan_count != key_count + 1) ||
        ((!flag || aggregate_in_keys) && scan_count != key_count))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (!prdb_sdk_internal_read_u8(&reader, &tag) || tag != 14 || !prdb_sdk_internal_read_u8(&reader, &tag) ||
        tag < 1 || tag > 9)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.order_count = tag;
    for (uint32_t index = 0; index < decoded.order_count; ++index) {
        uint8_t source, key_index = UINT8_MAX, descending, nulls_first;
        if (!prdb_sdk_internal_read_u8(&reader, &source) || source > 1 ||
            (source == 0 && !prdb_sdk_internal_read_u8(&reader, &key_index)) ||
            !prdb_sdk_internal_read_u8(&reader, &descending) || descending > 1 ||
            !prdb_sdk_internal_read_u8(&reader, &nulls_first) || nulls_first > 1)
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
    if (!prdb_sdk_internal_read_u8(&reader, &flag) || flag > 1)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.plan.has_limit = flag;
    if (flag) {
        if (!prdb_sdk_internal_take(&reader, 4, &bytes))
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
        prdb_sdk_internal_ranges_overlap(payload, payload_len, out, sizeof(*out)))
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
    if (!prdb_sdk_internal_read_name(&reader, &decoded.plan.schema_name,
                   &decoded.plan.schema_name_len) ||
        !prdb_sdk_internal_take(&reader, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_schema_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.schema_id, &decoded.plan.schema_version) ||
        prdb_sdk_internal_all_zero(decoded.plan.schema_id, 16) ||
        decoded.plan.schema_version == 0 ||
        !prdb_sdk_internal_read_name(&reader, &decoded.plan.relation_name,
                   &decoded.plan.relation_name_len) ||
        !prdb_sdk_internal_take(&reader,
              PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
              &bytes) ||
        !prdb_decode_optimizer_plan_relation_reference_record(
            bytes, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
            decoded.plan.relation_id, &decoded.plan.table_epoch) ||
        prdb_sdk_internal_all_zero(decoded.plan.relation_id, 16) ||
        decoded.plan.table_epoch == 0 || !prdb_sdk_internal_take(&reader, 2, &bytes))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    scan_count = prdb_sdk_internal_read_be_u16(bytes);
    if (scan_count < 1 || scan_count > 2)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    for (uint32_t index = 0; index < scan_count; ++index) {
        if (!prdb_sdk_internal_read_column(&reader, &decoded.plan.scan_columns[index]) ||
            (index && decoded.plan.scan_columns[index - 1].ordinal >=
                          decoded.plan.scan_columns[index].ordinal))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (!prdb_sdk_internal_read_u8(&reader, &tag) || tag != 12 ||
        !prdb_sdk_internal_read_u8(&reader, &aggregate_kind) || aggregate_kind < 1 ||
        aggregate_kind > PRDB_SDK_SQL_V7_AGGREGATE_COUNT_COLUMN || !prdb_sdk_internal_read_column(&reader, &decoded.group_key) ||
        !prdb_sdk_internal_read_u8(&reader, &flag) || flag > 1)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.aggregate_kind = aggregate_kind;
    decoded.has_aggregate_column = flag;
    if ((aggregate_kind == 1 && flag != 0) ||
        (aggregate_kind != 1 && flag != 1))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (flag && !prdb_sdk_internal_read_column(&reader, &decoded.aggregate_column))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if ((aggregate_kind == 4 || aggregate_kind == 5) &&
        decoded.aggregate_column.type_code != PRDB_SDK_VALUE_TYPE_SMALLINT &&
        decoded.aggregate_column.type_code != PRDB_SDK_VALUE_TYPE_INTEGER &&
        decoded.aggregate_column.type_code != PRDB_SDK_VALUE_TYPE_BIGINT &&
        decoded.aggregate_column.type_code != PRDB_SDK_VALUE_TYPE_DECIMAL)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    uint32_t expected_scan = 0;
    for (uint32_t index = 0; index < scan_count; ++index) {
        if (prdb_sdk_internal_columns_equal(&decoded.group_key,
                          &decoded.plan.scan_columns[index]))
            expected_scan |= 1;
        if (flag && prdb_sdk_internal_columns_equal(&decoded.aggregate_column,
                                  &decoded.plan.scan_columns[index]))
            expected_scan |= 2;
    }
    if (!(expected_scan & 1) || (flag && !(expected_scan & 2)) ||
        ((!flag || prdb_sdk_internal_columns_equal(&decoded.group_key,
                                 &decoded.aggregate_column)) &&
         scan_count != 1) ||
        (flag && !prdb_sdk_internal_columns_equal(&decoded.group_key,
                                &decoded.aggregate_column) &&
         scan_count != 2))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (!prdb_sdk_internal_read_u8(&reader, &tag) || tag < 1 || tag > 2)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.order_count = tag;
    for (uint32_t index = 0; index < decoded.order_count; ++index) {
        uint8_t source, descending, nulls_first;
        if (!prdb_sdk_internal_read_u8(&reader, &source) || source > 1 ||
            (seen & (1u << source)) != 0 || !prdb_sdk_internal_read_u8(&reader, &descending) ||
            descending > 1 || !prdb_sdk_internal_read_u8(&reader, &nulls_first) ||
            nulls_first > 1)
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        seen |= (uint8_t)(1u << source);
        decoded.order_items[index].source = source;
        decoded.order_items[index].descending = descending;
        decoded.order_items[index].nulls_first = nulls_first;
    }
    if (!prdb_sdk_internal_read_u8(&reader, &flag) || flag > 1)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    decoded.plan.has_limit = flag;
    if (flag) {
        if (!prdb_sdk_internal_take(&reader, 4, &bytes))
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
