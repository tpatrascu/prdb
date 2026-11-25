#include "optimizer_plan_codec_internal.h"

static int32_t decode_plan_internal(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_plan_v1 *out, uint32_t out_capacity,
    prdb_sdk_optimizer_filter_v1 *filter_out,
    uint32_t filter_out_capacity, int allow_filter,
    point_probe_hint *point_probe_out,
    range_probe_hint *range_probe_out,
    multi_point_probe_hint *multi_point_probe_out,
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
    multi_point_probe_hint decoded_multi_point_probe = {0};

    if (point_probe_out != NULL) memset(point_probe_out, 0, sizeof(*point_probe_out));
    if (range_probe_out != NULL) memset(range_probe_out, 0, sizeof(*range_probe_out));
    if (multi_point_probe_out != NULL) memset(multi_point_probe_out, 0, sizeof(*multi_point_probe_out));
    if (empty_view_out != NULL) *empty_view_out = 0;

    if (payload == NULL || out == NULL || out_capacity < sizeof(*out) ||
        (allow_filter &&
         (filter_out == NULL ||
          filter_out_capacity < sizeof(*filter_out) ||
          prdb_sdk_internal_ranges_overlap(out, sizeof(*out), filter_out,
                         sizeof(*filter_out)))) ||
        prdb_sdk_internal_ranges_overlap(payload, payload_len, out, sizeof(*out))) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    if (allow_filter && prdb_sdk_internal_ranges_overlap(payload, payload_len, filter_out,
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
    if (!prdb_sdk_internal_read_name(&reader, &decoded.schema_name, &decoded.schema_name_len) ||
        decoded.schema_name_len == 0) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    {
        const uint8_t *bytes;
        if (!prdb_sdk_internal_take(&reader,
                  PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
                  &bytes) ||
            !prdb_decode_optimizer_plan_schema_reference_record(
                bytes, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
                decoded.schema_id, &decoded.schema_version) ||
            prdb_sdk_internal_all_zero(decoded.schema_id, 16) || decoded.schema_version == 0) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
    }
    if (!prdb_sdk_internal_read_name(&reader, &decoded.relation_name,
                   &decoded.relation_name_len)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    {
        const uint8_t *bytes;
        if (!prdb_sdk_internal_take(&reader,
                  PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
                  &bytes) ||
            !prdb_decode_optimizer_plan_relation_reference_record(
                bytes,
                PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
                decoded.relation_id, &decoded.table_epoch) ||
            prdb_sdk_internal_all_zero(decoded.relation_id, 16) || decoded.table_epoch == 0) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
    }
    if (!prdb_sdk_internal_read_plan_node_count(&reader, &node_count) || node_count < 2 ||
        node_count > 4) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (decoded.schema_name_len == 0 || decoded.relation_name_len == 0) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }

    if (!prdb_sdk_internal_read_u8(&reader, &node_tag) || node_tag != 1 ||
        !prdb_sdk_internal_read_column_list_header(&reader, 1, &count) ||
        (expected_kind == 1 && count == 0) ||
        count > PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    decoded.scan_column_count = count;
    for (index = 0; index < decoded.scan_column_count; ++index) {
        prdb_sdk_optimizer_plan_column_v1 *column =
            &decoded.scan_columns[index];
        if (!prdb_sdk_internal_read_column(&reader, column)) {
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
    if (!prdb_sdk_internal_read_u8(&reader, &node_tag)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (node_tag == 2) {
        if (!prdb_sdk_internal_read_filter_node_tag(&reader, node_tag_offset)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        uint64_t predicate_start = reader.offset;
        uint32_t predicate_nodes = 0;
        predicate_type predicate_type_out;
        if (!allow_filter) return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
        if (!prdb_sdk_internal_decode_predicate(&reader, decoded.scan_columns,
                              decoded.scan_column_count, 1, &predicate_nodes,
                              predicate_references, &predicate_type_out) ||
            !prdb_sdk_internal_predicate_boolean(&predicate_type_out)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        has_filter = 1;
        decoded_filter.present = 1;
        decoded_filter.prefix = payload + predicate_start;
        decoded_filter.prefix_len =
            (uint32_t)(reader.offset - predicate_start);
        if (!prdb_sdk_internal_read_u8(&reader, &node_tag)) {
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
    if (node_tag != 3 || !prdb_sdk_internal_read_column_list_header(&reader, 3, &count) || count == 0 ||
        count > PRDB_SDK_OPTIMIZER_PLAN_MAX_PROJECTIONS) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    decoded.projection_count = count;
    for (index = 0; index < decoded.projection_count; ++index) {
        prdb_sdk_optimizer_plan_column_v1 *projection =
            &decoded.projections[index];
        int found = 0;
        if (!prdb_sdk_internal_read_column(&reader, projection)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        for (other = 0; other < decoded.scan_column_count; ++other) {
            if (prdb_sdk_internal_columns_equal(projection, &decoded.scan_columns[other])) {
                found = 1;
                break;
            }
        }
        if (!found) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }

    if (node_count == (uint8_t)(3 + has_filter)) {
        if (!prdb_sdk_internal_read_u8(&reader, &node_tag) || node_tag != 4 ||
            !prdb_sdk_internal_read_limit_node(&reader, &limit)) {
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
        if (expected_kind != 1 || !prdb_sdk_internal_read_u8(&reader, &tag)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        if (tag == 0xa3) {
            if (reader.offset != reader.length) {
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            }
            empty_view_hint = 1;
        } else if ((tag != 0xa1 && tag != 0xa2 && tag != 0xa4) ||
            !prdb_sdk_internal_take(&reader, 16, &index_id) || prdb_sdk_internal_all_zero(index_id, 16) ||
            !prdb_sdk_internal_read_column(&reader, &key_column)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        } else {
            for (index = 0; index < decoded.scan_column_count; ++index) {
                if (prdb_sdk_internal_columns_equal(&key_column, &decoded.scan_columns[index])) {
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
                if (!prdb_sdk_internal_read_u8(&reader, &literal_tag) ||
                    (literal_tag != 0x01 && literal_tag != 0x02 &&
                     literal_tag != 0x03 && literal_tag != 0x04 &&
                     literal_tag != 0x05 && literal_tag != 0x06 &&
                     literal_tag != 0x07 && literal_tag != 0x08)) {
                    return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
                }
                --reader.offset;
                literal_offset = reader.offset;
                if (!prdb_sdk_internal_decode_predicate(&reader, decoded.scan_columns,
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
                    if (!prdb_sdk_internal_read_integer_predicate_record(&integer_reader, 0,
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
            } else if (tag == 0xa2) {
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
                    if (!prdb_sdk_internal_read_u8(&reader, &bound_flag) || bound_flag > 2) {
                        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
                    }
                    if (bound_flag == 0) continue;
                    uint64_t literal_offset = reader.offset;
                    uint32_t value_nodes = 0;
                    predicate_type value_type = {0};
                    uint8_t value_references[PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS] = {0};
                    if (!prdb_sdk_internal_decode_predicate(&reader, decoded.scan_columns,
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
                        if (!prdb_sdk_internal_read_integer_predicate_record(&integer_reader, 0,
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
                    !prdb_sdk_internal_range_probe_matches_filter(&decoded_filter, &key_column,
                                                &decoded_range_probe)) {
                    return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
                }
            } else {
                const uint8_t *count_bytes;
                uint16_t value_count;
                if (!prdb_sdk_internal_take(&reader, 2, &count_bytes))
                    return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
                value_count = (uint16_t)(((uint16_t)count_bytes[0] << 8) | count_bytes[1]);
                if (value_count < 1 || value_count > 64)
                    return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
                decoded_multi_point_probe.present = 1;
                decoded_multi_point_probe.count = value_count;
                decoded_multi_point_probe.key_column = key_column;
                memcpy(decoded_multi_point_probe.index_id, index_id, 16);
                for (uint16_t value_index = 0; value_index < value_count; ++value_index) {
                    predicate_type value_type = {0};
                    uint32_t value_nodes = 0;
                    uint8_t value_references[PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS] = {0};
                    uint64_t literal_offset = reader.offset;
                    if (!prdb_sdk_internal_decode_predicate(&reader, decoded.scan_columns,
                                          decoded.scan_column_count, 1, &value_nodes,
                                          value_references, &value_type) ||
                        value_nodes != 1 || value_type.is_null != 0 ||
                        value_type.type_code != key_column.type_code ||
                        memcmp(value_type.parameters, key_column.type_parameters, 4) != 0)
                        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
                    decoded_multi_point_probe.literals[value_index] = payload + literal_offset;
                    decoded_multi_point_probe.literal_lengths[value_index] =
                        (uint32_t)(reader.offset - literal_offset);
                    for (uint16_t previous = 0; previous < value_index; ++previous) {
                        if (decoded_multi_point_probe.literal_lengths[previous] ==
                                decoded_multi_point_probe.literal_lengths[value_index] &&
                            memcmp(decoded_multi_point_probe.literals[previous],
                                   decoded_multi_point_probe.literals[value_index],
                                   decoded_multi_point_probe.literal_lengths[value_index]) == 0)
                            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
                    }
                }
                if (reader.offset != reader.length ||
                    !prdb_sdk_internal_multi_point_probe_matches_filter(&decoded_filter, &key_column,
                                                      &decoded_multi_point_probe))
                    return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            }
        }
    }
    if (has_unique_probe != 0 &&
        !prdb_sdk_internal_point_probe_matches_filter(&decoded_filter,
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
    if (multi_point_probe_out != NULL) *multi_point_probe_out = decoded_multi_point_probe;
    if (empty_view_out != NULL) *empty_view_out = empty_view_hint;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_decode_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_plan_v1 *out, uint32_t out_capacity) {
    return decode_plan_internal(payload, payload_len, out, out_capacity, NULL,
                                0, 0, NULL, NULL, NULL, NULL, 1);
}

int32_t prdb_sdk_optimizer_decode_plan_with_filter_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_plan_v1 *out, uint32_t out_capacity,
    prdb_sdk_optimizer_filter_v1 *filter_out,
    uint32_t filter_out_capacity) {
    return decode_plan_internal(payload, payload_len, out, out_capacity,
                                filter_out, filter_out_capacity, 1, NULL, NULL, NULL, NULL, 1);
}

int32_t prdb_sdk_optimizer_decode_delete_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_plan_v1 *out, uint32_t out_capacity,
    prdb_sdk_optimizer_filter_v1 *filter_out,
    uint32_t filter_out_capacity) {
    return decode_plan_internal(payload, payload_len, out, out_capacity,
                                filter_out, filter_out_capacity, 1, NULL, NULL, NULL, NULL, 2);
}

PRDB_SDK_INTERNAL int32_t prdb_sdk_internal_decode_select_plan_with_access_path_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_plan_v1 *out, uint32_t out_capacity,
    prdb_sdk_optimizer_filter_v1 *filter_out, uint32_t filter_out_capacity,
    point_probe_hint *point_probe_out,
    range_probe_hint *range_probe_out,
    multi_point_probe_hint *multi_point_probe_out, uint8_t *empty_view_out) {
    if (point_probe_out == NULL || range_probe_out == NULL || multi_point_probe_out == NULL)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    return decode_plan_internal(payload, payload_len, out, out_capacity,
                                filter_out, filter_out_capacity, 1,
                                point_probe_out, range_probe_out,
                                multi_point_probe_out, empty_view_out, 1);
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
        prdb_sdk_internal_ranges_overlap(payload, payload_len, out, sizeof(*out))) {
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
    if (!prdb_sdk_internal_read_name(&reader, &decoded.plan.schema_name,
                   &decoded.plan.schema_name_len) ||
        decoded.plan.schema_name_len == 0) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    {
        const uint8_t *bytes;
        if (!prdb_sdk_internal_take(&reader,
                  PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
                  &bytes) ||
            !prdb_decode_optimizer_plan_schema_reference_record(
                bytes, PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES,
                decoded.plan.schema_id, &decoded.plan.schema_version) ||
            prdb_sdk_internal_all_zero(decoded.plan.schema_id, 16) ||
            decoded.plan.schema_version == 0) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
    }
    if (!prdb_sdk_internal_read_name(&reader, &decoded.plan.relation_name,
                   &decoded.plan.relation_name_len) ||
        decoded.plan.relation_name_len == 0) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    {
        const uint8_t *bytes;
        if (!prdb_sdk_internal_take(&reader,
                  PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
                  &bytes) ||
            !prdb_decode_optimizer_plan_relation_reference_record(
                bytes, PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES,
                decoded.plan.relation_id, &decoded.plan.table_epoch) ||
            prdb_sdk_internal_all_zero(decoded.plan.relation_id, 16) ||
            decoded.plan.table_epoch == 0) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
    }
    if (!prdb_sdk_internal_read_plan_node_count(&reader, &node_count) || node_count != 3 ||
        !prdb_sdk_internal_read_u8(&reader, &node_tag) || node_tag != 1 ||
        !prdb_sdk_internal_read_column_list_header(&reader, 1, &column_count) ||
        column_count == 0 ||
        column_count > PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    decoded.plan.scan_column_count = column_count;
    for (index = 0; index < decoded.plan.scan_column_count; ++index) {
        prdb_sdk_optimizer_plan_column_v1 *column =
            &decoded.plan.scan_columns[index];
        if (!prdb_sdk_internal_read_column(&reader, column) ||
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
        if (!prdb_sdk_internal_read_u8(&reader, &node_tag) || node_tag != 2 ||
            !prdb_sdk_internal_read_filter_node_tag(&reader, filter_node_offset)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        predicate_start = reader.offset;
        if (!prdb_sdk_internal_decode_predicate(&reader, decoded.plan.scan_columns,
                              decoded.plan.scan_column_count, 1,
                              &predicate_nodes, predicate_references,
                              &predicate_type_out) ||
            !prdb_sdk_internal_predicate_boolean(&predicate_type_out) ||
            !prdb_sdk_internal_take(&reader, PRDB_OPTIMIZER_PLAN_UPDATE_NODE_RECORD_HEADER_BYTES,
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
        if (!prdb_sdk_internal_read_column(&reader, &assignment->column) ||
            !prdb_sdk_internal_take(&reader,
                  PRDB_OPTIMIZER_PLAN_UPDATE_ASSIGNMENT_VALUE_RECORD_HEADER_BYTES,
                  &value_bytes) ||
            !prdb_decode_optimizer_plan_update_assignment_value_record(
                value_bytes,
                PRDB_OPTIMIZER_PLAN_UPDATE_ASSIGNMENT_VALUE_RECORD_HEADER_BYTES,
                &value_kind, &parameter_index)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        for (other = 0; other < decoded.plan.scan_column_count; ++other) {
            if (prdb_sdk_internal_columns_equal(&assignment->column,
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

PRDB_SDK_INTERNAL int32_t prdb_sdk_internal_decode_any_optimizer_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_plan_v1 *plan, uint32_t plan_capacity,
    prdb_sdk_optimizer_filter_v1 *filter, uint32_t filter_capacity) {
    if (payload == NULL || payload_len < 3)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (payload[2] == 3) {
        prdb_sdk_optimizer_update_plan_v1 update = {0};
        if (plan == NULL || plan_capacity < sizeof(*plan) || filter == NULL ||
            filter_capacity < sizeof(*filter) ||
            prdb_sdk_internal_ranges_overlap(payload, payload_len, plan, sizeof(*plan)) ||
            prdb_sdk_internal_ranges_overlap(payload, payload_len, filter, sizeof(*filter)) ||
            prdb_sdk_internal_ranges_overlap(plan, sizeof(*plan), filter, sizeof(*filter))) {
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
            prdb_sdk_internal_ranges_overlap(payload, payload_len, plan, sizeof(*plan)) ||
            prdb_sdk_internal_ranges_overlap(payload, payload_len, filter, sizeof(*filter)) ||
            prdb_sdk_internal_ranges_overlap(plan, sizeof(*plan), filter, sizeof(*filter)))
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
            prdb_sdk_internal_ranges_overlap(payload, payload_len, plan, sizeof(*plan)) ||
            prdb_sdk_internal_ranges_overlap(payload, payload_len, filter, sizeof(*filter)) ||
            prdb_sdk_internal_ranges_overlap(plan, sizeof(*plan), filter, sizeof(*filter)))
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
            prdb_sdk_internal_ranges_overlap(payload, payload_len, plan, sizeof(*plan)) ||
            prdb_sdk_internal_ranges_overlap(payload, payload_len, filter, sizeof(*filter)) ||
            prdb_sdk_internal_ranges_overlap(plan, sizeof(*plan), filter, sizeof(*filter)))
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
            prdb_sdk_internal_ranges_overlap(payload, payload_len, plan, sizeof(*plan)) || prdb_sdk_internal_ranges_overlap(payload, payload_len, filter, sizeof(*filter)) || prdb_sdk_internal_ranges_overlap(plan, sizeof(*plan), filter, sizeof(*filter)))
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
            prdb_sdk_internal_ranges_overlap(payload, payload_len, plan, sizeof(*plan)) ||
            prdb_sdk_internal_ranges_overlap(payload, payload_len, filter, sizeof(*filter)) ||
            prdb_sdk_internal_ranges_overlap(plan, sizeof(*plan), filter, sizeof(*filter)))
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
            prdb_sdk_internal_ranges_overlap(payload, payload_len, plan, sizeof(*plan)) ||
            prdb_sdk_internal_ranges_overlap(payload, payload_len, filter, sizeof(*filter)) ||
            prdb_sdk_internal_ranges_overlap(plan, sizeof(*plan), filter, sizeof(*filter)))
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
            prdb_sdk_internal_ranges_overlap(payload, payload_len, plan, sizeof(*plan)) ||
            prdb_sdk_internal_ranges_overlap(payload, payload_len, filter, sizeof(*filter)) ||
            prdb_sdk_internal_ranges_overlap(plan, sizeof(*plan), filter, sizeof(*filter)))
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
            prdb_sdk_internal_ranges_overlap(payload, payload_len, plan, sizeof(*plan)) ||
            prdb_sdk_internal_ranges_overlap(payload, payload_len, filter, sizeof(*filter)) ||
            prdb_sdk_internal_ranges_overlap(plan, sizeof(*plan), filter, sizeof(*filter)))
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
            prdb_sdk_internal_ranges_overlap(payload, payload_len, plan, sizeof(*plan)) ||
            prdb_sdk_internal_ranges_overlap(payload, payload_len, filter, sizeof(*filter)) ||
            prdb_sdk_internal_ranges_overlap(plan, sizeof(*plan), filter, sizeof(*filter)))
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
            prdb_sdk_internal_ranges_overlap(payload, payload_len, plan, sizeof(*plan)) ||
            prdb_sdk_internal_ranges_overlap(payload, payload_len, filter, sizeof(*filter)) ||
            prdb_sdk_internal_ranges_overlap(plan, sizeof(*plan), filter, sizeof(*filter)))
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
            prdb_sdk_internal_ranges_overlap(payload, payload_len, plan, sizeof(*plan)) ||
            prdb_sdk_internal_ranges_overlap(payload, payload_len, filter, sizeof(*filter)) ||
            prdb_sdk_internal_ranges_overlap(plan, sizeof(*plan), filter, sizeof(*filter)))
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
