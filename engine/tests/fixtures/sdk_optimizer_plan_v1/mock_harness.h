static void put_u16(uint8_t *bytes, uint32_t *offset, uint16_t value) {
    bytes[(*offset)++] = (uint8_t)(value >> 8);
    bytes[(*offset)++] = (uint8_t)value;
}

static void put_u32(uint8_t *bytes, uint32_t *offset, uint32_t value) {
    bytes[(*offset)++] = (uint8_t)(value >> 24);
    bytes[(*offset)++] = (uint8_t)(value >> 16);
    bytes[(*offset)++] = (uint8_t)(value >> 8);
    bytes[(*offset)++] = (uint8_t)value;
}

static void put_u64(uint8_t *bytes, uint32_t *offset, uint64_t value) {
    put_u32(bytes, offset, (uint32_t)(value >> 32));
    put_u32(bytes, offset, (uint32_t)value);
}

static uint32_t put_column(uint8_t *bytes, uint32_t *offset,
                           uint8_t type_code, uint8_t nullable) {
    uint32_t id_offset = *offset;
    bytes[(*offset)++] = 3;
    memset(bytes + *offset, 0, 15);
    *offset += 15;
    put_u32(bytes, offset, 3);
    bytes[(*offset)++] = type_code;
    memset(bytes + *offset, 0, 4);
    *offset += 4;
    bytes[(*offset)++] = nullable;
    return id_offset;
}

static void put_name(uint8_t *bytes, uint32_t *offset, const char *name) {
    uint8_t length = (uint8_t)strlen(name);
    bytes[(*offset)++] = length;
    memcpy(bytes + *offset, name, length);
    *offset += length;
}

static void set_column_ordinal(uint8_t *bytes, uint32_t column_offset,
                               uint32_t ordinal) {
    bytes[column_offset + 16] = (uint8_t)(ordinal >> 24);
    bytes[column_offset + 17] = (uint8_t)(ordinal >> 16);
    bytes[column_offset + 18] = (uint8_t)(ordinal >> 8);
    bytes[column_offset + 19] = (uint8_t)ordinal;
}

static uint32_t build_join_plan(uint8_t *bytes, uint32_t *projection_side_offset,
                                uint32_t *right_key_type_offset) {
    uint32_t offset = 0;
    uint8_t identifier[16];
    prdb_encode_optimizer_plan_header_record(bytes + offset, 1, 4, 0);
    offset += PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    put_name(bytes, &offset, "public");
    memset(identifier, 1, sizeof(identifier));
    prdb_encode_optimizer_plan_schema_reference_record(bytes + offset,
                                                       identifier, 3);
    offset += PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES;
    for (uint32_t side = 0; side < 2; ++side) {
        put_name(bytes, &offset, side == 0 ? "lefts" : "rights");
        memset(identifier, side == 0 ? 0x31 : 0x32, sizeof(identifier));
        prdb_encode_optimizer_plan_relation_reference_record(
            bytes + offset, identifier, side == 0 ? 4 : 5);
        offset += PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES;
    }
    for (uint32_t side = 0; side < 2; ++side) {
        put_u16(bytes, &offset, 3);
        uint32_t column = put_column(bytes, &offset, 3, 0);
        bytes[column] = side == 0 ? 0x41 : 0x51;
        set_column_ordinal(bytes, column, 0);
        column = put_column(bytes, &offset, 1, 0);
        bytes[column] = side == 0 ? 0x42 : 0x52;
        set_column_ordinal(bytes, column, 1);
        column = put_column(bytes, &offset, 6, 1);
        bytes[column] = side == 0 ? 0x43 : 0x53;
        set_column_ordinal(bytes, column, 2);
    }
    put_u16(bytes, &offset, 2);
    *projection_side_offset = offset;
    bytes[offset++] = 0;
    uint32_t column = put_column(bytes, &offset, 6, 1);
    bytes[column] = 0x43;
    set_column_ordinal(bytes, column, 2);
    bytes[offset++] = 1;
    column = put_column(bytes, &offset, 6, 1);
    bytes[column] = 0x53;
    set_column_ordinal(bytes, column, 2);
    bytes[offset++] = 2;
    column = put_column(bytes, &offset, 3, 0);
    bytes[column] = 0x41;
    set_column_ordinal(bytes, column, 0);
    column = put_column(bytes, &offset, 3, 0);
    bytes[column] = 0x51;
    set_column_ordinal(bytes, column, 0);
    *right_key_type_offset = column + 20;
    column = put_column(bytes, &offset, 1, 0);
    bytes[column] = 0x42;
    set_column_ordinal(bytes, column, 1);
    column = put_column(bytes, &offset, 1, 0);
    bytes[column] = 0x52;
    set_column_ordinal(bytes, column, 1);
    bytes[offset++] = 1;
    put_u32(bytes, &offset, 5);
    return offset;
}

static uint32_t build_filtered_join_plan(uint8_t *bytes,
                                         uint32_t *filter_side_offset,
                                         uint32_t *filter_column_id_offset,
                                         uint32_t *parameter_index_offset) {
    uint32_t projection_side_offset;
    uint32_t right_key_type_offset;
    uint32_t offset = build_join_plan(bytes, &projection_side_offset,
                                      &right_key_type_offset);
    (void)projection_side_offset;
    (void)right_key_type_offset;
    bytes[2] = PRDB_SDK_OPTIMIZER_PLAN_KIND_FILTERED_JOIN_SELECT;
    *filter_side_offset = offset;
    bytes[offset++] = 1;
    *filter_column_id_offset = put_column(bytes, &offset, 1, 0);
    bytes[*filter_column_id_offset] = 0x52;
    set_column_ordinal(bytes, *filter_column_id_offset, 1);
    bytes[offset++] = 0; /* parameter comparison */
    bytes[offset++] = 4; /* greater-than */
    *parameter_index_offset = offset;
    put_u32(bytes, &offset, 3);
    return offset;
}

static uint32_t build_group_count_plan(uint8_t *bytes,
                                       uint32_t *group_key_id_offset) {
    uint32_t offset = 0;
    uint8_t identifier[16];
    uint32_t column;
    prdb_encode_optimizer_plan_header_record(bytes + offset, 1, 5, 0);
    offset += PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    put_name(bytes, &offset, "public");
    memset(identifier, 1, sizeof(identifier));
    prdb_encode_optimizer_plan_schema_reference_record(bytes + offset,
                                                       identifier, 3);
    offset += PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES;
    put_name(bytes, &offset, "items");
    memset(identifier, 2, sizeof(identifier));
    prdb_encode_optimizer_plan_relation_reference_record(bytes + offset,
                                                         identifier, 4);
    offset += PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES;
    put_u16(bytes, &offset, 1);
    column = put_column(bytes, &offset, 1, 0);
    bytes[column] = 4;
    set_column_ordinal(bytes, column, 0);
    bytes[offset++] = 7;
    *group_key_id_offset = offset;
    column = put_column(bytes, &offset, 1, 0);
    bytes[column] = 4;
    set_column_ordinal(bytes, column, 0);
    bytes[offset++] = 1;
    put_u32(bytes, &offset, 5);
    return offset;
}

static uint32_t build_ordered_group_count_plan(uint8_t *bytes) {
    uint32_t offset = 0;
    uint8_t identifier[16];
    uint32_t column;
    prdb_encode_optimizer_plan_header_record(bytes + offset, 1, 8, 0);
    offset += PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    put_name(bytes, &offset, "public");
    memset(identifier, 1, sizeof(identifier));
    prdb_encode_optimizer_plan_schema_reference_record(bytes + offset,
                                                       identifier, 3);
    offset += PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES;
    put_name(bytes, &offset, "items");
    memset(identifier, 2, sizeof(identifier));
    prdb_encode_optimizer_plan_relation_reference_record(bytes + offset,
                                                         identifier, 4);
    offset += PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES;
    put_u16(bytes, &offset, 1);
    column = put_column(bytes, &offset, 3, 0);
    bytes[column] = 4;
    set_column_ordinal(bytes, column, 0);
    bytes[offset++] = 7;
    column = put_column(bytes, &offset, 3, 0);
    bytes[column] = 4;
    set_column_ordinal(bytes, column, 0);
    bytes[offset++] = 2;
    bytes[offset++] = 1;
    bytes[offset++] = 1;
    bytes[offset++] = 0;
    bytes[offset++] = 0;
    bytes[offset++] = 0;
    bytes[offset++] = 1;
    bytes[offset++] = 1;
    put_u32(bytes, &offset, 5);
    return offset;
}

static uint32_t build_composite_group_count_plan(
    uint8_t *bytes, uint32_t *second_key_offset) {
    uint32_t offset = 0;
    uint8_t identifier[16];
    uint32_t column;
    prdb_encode_optimizer_plan_header_record(bytes + offset, 1, 6, 0);
    offset += PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    put_name(bytes, &offset, "public");
    memset(identifier, 1, sizeof(identifier));
    prdb_encode_optimizer_plan_schema_reference_record(bytes + offset,
                                                       identifier, 3);
    offset += PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES;
    put_name(bytes, &offset, "items");
    memset(identifier, 2, sizeof(identifier));
    prdb_encode_optimizer_plan_relation_reference_record(bytes + offset,
                                                         identifier, 4);
    offset += PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES;
    put_u16(bytes, &offset, 2);
    column = put_column(bytes, &offset, 1, 0);
    bytes[column] = 4;
    set_column_ordinal(bytes, column, 0);
    column = put_column(bytes, &offset, 1, 0);
    bytes[column] = 5;
    set_column_ordinal(bytes, column, 1);
    bytes[offset++] = 8;
    bytes[offset++] = 2;
    column = put_column(bytes, &offset, 1, 0);
    bytes[column] = 5;
    set_column_ordinal(bytes, column, 1);
    *second_key_offset = offset;
    column = put_column(bytes, &offset, 1, 0);
    bytes[column] = 4;
    set_column_ordinal(bytes, column, 0);
    bytes[offset++] = 1;
    put_u32(bytes, &offset, 5);
    return offset;
}

static uint32_t build_composite_group_aggregate_plan(uint8_t *bytes,
                                                     uint8_t aggregate_kind) {
    uint32_t offset = 0;
    uint8_t identifier[16];
    uint32_t column;
    prdb_encode_optimizer_plan_header_record(bytes + offset, 1, 13, 0);
    offset += PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    put_name(bytes, &offset, "public");
    memset(identifier, 1, sizeof(identifier));
    prdb_encode_optimizer_plan_schema_reference_record(bytes + offset,
                                                       identifier, 3);
    offset += PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES;
    put_name(bytes, &offset, "items");
    memset(identifier, 2, sizeof(identifier));
    prdb_encode_optimizer_plan_relation_reference_record(bytes + offset,
                                                         identifier, 4);
    offset += PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES;
    put_u16(bytes, &offset, aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR
                                ? 2
                                : 3);
    column = put_column(bytes, &offset, PRDB_SDK_VALUE_TYPE_INTEGER, 0);
    bytes[column] = 5;
    set_column_ordinal(bytes, column, 0);
    column = put_column(bytes, &offset, PRDB_SDK_VALUE_TYPE_BOOLEAN, 0);
    bytes[column] = 4;
    set_column_ordinal(bytes, column, 1);
    if (aggregate_kind != PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
        column = put_column(bytes, &offset, PRDB_SDK_VALUE_TYPE_BIGINT, 0);
        bytes[column] = 6;
        set_column_ordinal(bytes, column, 2);
    }
    bytes[offset++] = 13;
    bytes[offset++] = aggregate_kind;
    bytes[offset++] = 2;
    column = put_column(bytes, &offset, PRDB_SDK_VALUE_TYPE_BOOLEAN, 0);
    bytes[column] = 4;
    set_column_ordinal(bytes, column, 1);
    column = put_column(bytes, &offset, PRDB_SDK_VALUE_TYPE_INTEGER, 0);
    bytes[column] = 5;
    set_column_ordinal(bytes, column, 0);
    bytes[offset++] = aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR
                          ? 0
                          : 1;
    if (aggregate_kind != PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
        column = put_column(bytes, &offset, PRDB_SDK_VALUE_TYPE_BIGINT, 0);
        bytes[column] = 6;
        set_column_ordinal(bytes, column, 2);
    }
    bytes[offset++] = 1;
    put_u32(bytes, &offset, 5);
    return offset;
}

static uint32_t build_filtered_composite_group_aggregate_plan(uint8_t *bytes) {
    uint32_t offset = 0;
    uint8_t identifier[16];
    uint32_t column;
    prdb_encode_optimizer_plan_header_record(bytes + offset, 1, 15, 0);
    offset += PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    put_name(bytes, &offset, "public");
    memset(identifier, 1, sizeof(identifier));
    prdb_encode_optimizer_plan_schema_reference_record(bytes + offset,
                                                       identifier, 3);
    offset += PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES;
    put_name(bytes, &offset, "items");
    memset(identifier, 2, sizeof(identifier));
    prdb_encode_optimizer_plan_relation_reference_record(bytes + offset,
                                                         identifier, 4);
    offset += PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES;
    put_u16(bytes, &offset, 3);
    column = put_column(bytes, &offset, PRDB_SDK_VALUE_TYPE_INTEGER, 0);
    bytes[column] = 5;
    set_column_ordinal(bytes, column, 0);
    column = put_column(bytes, &offset, PRDB_SDK_VALUE_TYPE_BOOLEAN, 0);
    bytes[column] = 4;
    set_column_ordinal(bytes, column, 1);
    column = put_column(bytes, &offset, PRDB_SDK_VALUE_TYPE_BIGINT, 1);
    bytes[column] = 6;
    set_column_ordinal(bytes, column, 2);
    prdb_encode_optimizer_plan_filter_node_record(bytes + offset, 2);
    offset += PRDB_OPTIMIZER_PLAN_FILTER_NODE_RECORD_HEADER_BYTES;
    prdb_encode_optimizer_plan_compare_predicate_header_record(bytes + offset,
                                                               0x20, 0);
    offset += PRDB_OPTIMIZER_PLAN_COMPARE_PREDICATE_HEADER_RECORD_HEADER_BYTES;
    identifier[0] = 4;
    memset(identifier + 1, 0, 15);
    prdb_encode_optimizer_plan_column_predicate_record(bytes + offset, 0x10,
                                                       identifier);
    offset += PRDB_OPTIMIZER_PLAN_COLUMN_PREDICATE_RECORD_HEADER_BYTES;
    prdb_encode_optimizer_plan_boolean_literal_predicate_record(bytes + offset,
                                                                 0x01, 1);
    offset += PRDB_OPTIMIZER_PLAN_BOOLEAN_LITERAL_PREDICATE_RECORD_HEADER_BYTES;
    bytes[offset++] = 15;
    bytes[offset++] = PRDB_SDK_SQL_V7_AGGREGATE_SUM;
    bytes[offset++] = 2;
    column = put_column(bytes, &offset, PRDB_SDK_VALUE_TYPE_BOOLEAN, 0);
    bytes[column] = 4;
    set_column_ordinal(bytes, column, 1);
    column = put_column(bytes, &offset, PRDB_SDK_VALUE_TYPE_INTEGER, 0);
    bytes[column] = 5;
    set_column_ordinal(bytes, column, 0);
    bytes[offset++] = 1;
    column = put_column(bytes, &offset, PRDB_SDK_VALUE_TYPE_BIGINT, 1);
    bytes[column] = 6;
    set_column_ordinal(bytes, column, 2);
    bytes[offset++] = 1;
    put_u32(bytes, &offset, 5);
    return offset;
}

static uint32_t build_ordered_composite_group_count_plan(uint8_t *bytes) {
    uint32_t offset = 0;
    uint8_t identifier[16];
    uint32_t column;
    prdb_encode_optimizer_plan_header_record(bytes + offset, 1, 9, 0);
    offset += PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    put_name(bytes, &offset, "public");
    memset(identifier, 1, sizeof(identifier));
    prdb_encode_optimizer_plan_schema_reference_record(bytes + offset, identifier, 3);
    offset += PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES;
    put_name(bytes, &offset, "items");
    memset(identifier, 2, sizeof(identifier));
    prdb_encode_optimizer_plan_relation_reference_record(bytes + offset, identifier, 4);
    offset += PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES;
    put_u16(bytes, &offset, 2);
    column = put_column(bytes, &offset, 3, 0); bytes[column] = 3; bytes[column + 20] = PRDB_SDK_VALUE_TYPE_INTEGER; set_column_ordinal(bytes, column, 0);
    column = put_column(bytes, &offset, 3, 0); bytes[column] = 3; bytes[column + 20] = PRDB_SDK_VALUE_TYPE_BOOLEAN; set_column_ordinal(bytes, column, 1);
    bytes[offset++] = 10;
    bytes[offset++] = 2;
    column = put_column(bytes, &offset, 3, 0); bytes[column] = 3; bytes[column + 20] = PRDB_SDK_VALUE_TYPE_BOOLEAN; set_column_ordinal(bytes, column, 1);
    column = put_column(bytes, &offset, 3, 0); bytes[column] = 3; bytes[column + 20] = PRDB_SDK_VALUE_TYPE_INTEGER; set_column_ordinal(bytes, column, 0);
    bytes[offset++] = 3;
    bytes[offset++] = 0; bytes[offset++] = 0; bytes[offset++] = 1; bytes[offset++] = 0;
    bytes[offset++] = 1; bytes[offset++] = UINT8_MAX; bytes[offset++] = 1; bytes[offset++] = 1;
    bytes[offset++] = 0; bytes[offset++] = 1; bytes[offset++] = 0; bytes[offset++] = 1;
    bytes[offset++] = 1; put_u32(bytes, &offset, 5);
    return offset;
}

static uint32_t build_group_aggregate_plan(uint8_t *bytes) {
    uint32_t offset = 0;
    uint8_t identifier[16];
    uint32_t column;
    prdb_encode_optimizer_plan_header_record(bytes + offset, 1, 10, 0);
    offset += PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    put_name(bytes, &offset, "public"); memset(identifier, 1, sizeof(identifier));
    prdb_encode_optimizer_plan_schema_reference_record(bytes + offset, identifier, 3);
    offset += PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES;
    put_name(bytes, &offset, "items"); memset(identifier, 2, sizeof(identifier));
    prdb_encode_optimizer_plan_relation_reference_record(bytes + offset, identifier, 4);
    offset += PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES;
    put_u16(bytes, &offset, 2);
    column = put_column(bytes, &offset, 3, 0); bytes[column] = 3; bytes[column + 20] = PRDB_SDK_VALUE_TYPE_INTEGER; set_column_ordinal(bytes, column, 0);
    column = put_column(bytes, &offset, 3, 0); bytes[column] = 4; bytes[column + 20] = PRDB_SDK_VALUE_TYPE_BIGINT; set_column_ordinal(bytes, column, 1);
    bytes[offset++] = 11; bytes[offset++] = 5;
    column = put_column(bytes, &offset, 3, 0); bytes[column] = 3; bytes[column + 20] = PRDB_SDK_VALUE_TYPE_INTEGER; set_column_ordinal(bytes, column, 0);
    bytes[offset++] = 1;
    column = put_column(bytes, &offset, 3, 0); bytes[column] = 4; bytes[column + 20] = PRDB_SDK_VALUE_TYPE_BIGINT; set_column_ordinal(bytes, column, 1);
    bytes[offset++] = 1; put_u32(bytes, &offset, 5);
    return offset;
}

static uint32_t build_ordered_group_aggregate_plan(
    uint8_t *bytes, uint32_t *first_order_source_offset,
    uint8_t aggregate_kind) {
    uint32_t offset = 0;
    uint8_t identifier[16];
    uint32_t column;
    prdb_encode_optimizer_plan_header_record(bytes + offset, 1, 11, 0);
    offset += PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    put_name(bytes, &offset, "public");
    memset(identifier, 1, sizeof(identifier));
    prdb_encode_optimizer_plan_schema_reference_record(bytes + offset,
                                                       identifier, 3);
    offset += PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES;
    put_name(bytes, &offset, "items");
    memset(identifier, 2, sizeof(identifier));
    prdb_encode_optimizer_plan_relation_reference_record(bytes + offset,
                                                        identifier, 4);
    offset += PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES;
    put_u16(bytes, &offset,
            aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR ? 1 : 2);
    column = put_column(bytes, &offset, 3, 0);
    bytes[column] = 3;
    bytes[column + 20] = PRDB_SDK_VALUE_TYPE_INTEGER;
    set_column_ordinal(bytes, column, 0);
    if (aggregate_kind != PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
        column = put_column(bytes, &offset, 3, 0);
        bytes[column] = 4;
        bytes[column + 20] = PRDB_SDK_VALUE_TYPE_BIGINT;
        set_column_ordinal(bytes, column, 1);
    }
    bytes[offset++] = 12;
    bytes[offset++] = aggregate_kind;
    column = put_column(bytes, &offset, 3, 0);
    bytes[column] = 3;
    bytes[column + 20] = PRDB_SDK_VALUE_TYPE_INTEGER;
    set_column_ordinal(bytes, column, 0);
    bytes[offset++] = aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR ? 0 : 1;
    if (aggregate_kind != PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
        column = put_column(bytes, &offset, 3, 0);
        bytes[column] = 4;
        bytes[column + 20] = PRDB_SDK_VALUE_TYPE_BIGINT;
        set_column_ordinal(bytes, column, 1);
    }
    bytes[offset++] = 2;
    *first_order_source_offset = offset;
    bytes[offset++] = 1;
    bytes[offset++] = 1;
    bytes[offset++] = 0;
    bytes[offset++] = 0;
    bytes[offset++] = 0;
    bytes[offset++] = 1;
    bytes[offset++] = 1;
    put_u32(bytes, &offset, 5);
    return offset;
}

static uint32_t build_ordered_select_plan(uint8_t *bytes,
                                          uint32_t *order_key_id_offset) {
    uint32_t offset = 0;
    uint8_t identifier[16];
    uint32_t column;
    prdb_encode_optimizer_plan_header_record(bytes + offset, 1, 7, 0);
    offset += PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    put_name(bytes, &offset, "public");
    memset(identifier, 1, sizeof(identifier));
    prdb_encode_optimizer_plan_schema_reference_record(bytes + offset,
                                                       identifier, 3);
    offset += PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES;
    put_name(bytes, &offset, "items");
    memset(identifier, 2, sizeof(identifier));
    prdb_encode_optimizer_plan_relation_reference_record(bytes + offset,
                                                         identifier, 4);
    offset += PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES;
    put_u16(bytes, &offset, 2);
    column = put_column(bytes, &offset, 3, 0);
    bytes[column] = 4;
    set_column_ordinal(bytes, column, 0);
    column = put_column(bytes, &offset, 6, 1);
    bytes[column] = 5;
    set_column_ordinal(bytes, column, 1);
    put_u16(bytes, &offset, 1);
    column = put_column(bytes, &offset, 3, 0);
    bytes[column] = 4;
    set_column_ordinal(bytes, column, 0);
    bytes[offset++] = 9;
    *order_key_id_offset = offset;
    column = put_column(bytes, &offset, 6, 1);
    bytes[column] = 5;
    set_column_ordinal(bytes, column, 1);
    bytes[offset++] = 1;
    bytes[offset++] = 1;
    put_u32(bytes, &offset, 5);
    return offset;
}

static uint32_t build_ordered_c0_c3_plan(uint8_t *bytes, uint32_t limit) {
    uint32_t offset = 0;
    uint8_t identifier[16] = {0};
    uint32_t column;
    prdb_encode_optimizer_plan_header_record(bytes + offset, 1, 7, 0);
    offset += PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    put_name(bytes, &offset, "public");
    identifier[0] = 1;
    prdb_encode_optimizer_plan_schema_reference_record(bytes + offset,
                                                       identifier, 1);
    offset += PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES;
    put_name(bytes, &offset, "items");
    memset(identifier, 0, sizeof(identifier));
    identifier[0] = 2;
    prdb_encode_optimizer_plan_relation_reference_record(bytes + offset,
                                                         identifier, 1);
    offset += PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES;
    put_u16(bytes, &offset, 2);
    column = put_column(bytes, &offset, 3, 0);
    bytes[column] = 10;
    set_column_ordinal(bytes, column, 0);
    column = put_column(bytes, &offset, 6, 1);
    bytes[column] = 3;
    set_column_ordinal(bytes, column, 3);
    put_u16(bytes, &offset, 1);
    column = put_column(bytes, &offset, 3, 0);
    bytes[column] = 10;
    set_column_ordinal(bytes, column, 0);
    bytes[offset++] = 9;
    column = put_column(bytes, &offset, 6, 1);
    bytes[column] = 3;
    set_column_ordinal(bytes, column, 3);
    bytes[offset++] = 1;
    bytes[offset++] = 1;
    put_u32(bytes, &offset, limit);
    return offset;
}

static uint32_t put_header(uint8_t *bytes, uint32_t *offset,
                           uint8_t node_count) {
    bytes[(*offset)++] = 0;
    bytes[(*offset)++] = 1; /* version */
    bytes[(*offset)++] = 1; /* SELECT */
    bytes[(*offset)++] = 0; /* reserved */
    bytes[(*offset)++] = 6;
    memcpy(bytes + *offset, "public", 6);
    *offset += 6;
    bytes[(*offset)++] = 1;
    memset(bytes + *offset, 0, 15);
    *offset += 15;
    put_u64(bytes, offset, 1); /* schema version */
    bytes[(*offset)++] = 5;
    memcpy(bytes + *offset, "items", 5);
    *offset += 5;
    bytes[(*offset)++] = 2;
    memset(bytes + *offset, 0, 15);
    *offset += 15;
    put_u64(bytes, offset, 1); /* table epoch */
    bytes[(*offset)++] = node_count;
    return *offset;
}

static uint32_t build_plan(uint8_t *bytes, uint32_t *projected_id_offset,
                           uint32_t *project_node_offset) {
    uint32_t offset = 0;
    (void)put_header(bytes, &offset, 3);
    bytes[offset++] = 1;
    put_u16(bytes, &offset, 1);
    (void)put_column(bytes, &offset, 6, 1);
    *project_node_offset = offset;
    bytes[offset++] = 3;
    put_u16(bytes, &offset, 1);
    *projected_id_offset = put_column(bytes, &offset, 6, 1);
    bytes[offset++] = 4;
    put_u32(bytes, &offset, 7);
    return offset;
}

static void put_integer_comparison(uint8_t *bytes, uint32_t *offset,
                                   uint8_t integer_value) {
    bytes[(*offset)++] = 0x20;
    bytes[(*offset)++] = 0;
    bytes[(*offset)++] = 0x10;
    bytes[(*offset)++] = 10;
    memset(bytes + *offset, 0, 15);
    *offset += 15;
    bytes[(*offset)++] = 0x02;
    memset(bytes + *offset, 0, 7);
    *offset += 7;
    bytes[(*offset)++] = integer_value;
}

static void put_integer_literal_column_comparison(uint8_t *bytes,
                                                  uint32_t *offset,
                                                  uint8_t comparison,
                                                  uint8_t integer_value) {
    bytes[(*offset)++] = 0x20;
    bytes[(*offset)++] = comparison;
    bytes[(*offset)++] = 0x02;
    memset(bytes + *offset, 0, 7);
    *offset += 7;
    bytes[(*offset)++] = integer_value;
    bytes[(*offset)++] = 0x10;
    bytes[(*offset)++] = 10;
    memset(bytes + *offset, 0, 15);
    *offset += 15;
}

static void put_integer_null_comparison(uint8_t *bytes, uint32_t *offset) {
    bytes[(*offset)++] = 0x20;
    bytes[(*offset)++] = 0;
    bytes[(*offset)++] = 0x10;
    bytes[(*offset)++] = 10;
    memset(bytes + *offset, 0, 15);
    *offset += 15;
    bytes[(*offset)++] = 0x00;
}

static uint32_t build_filter_plan(uint8_t *bytes,
                                  uint32_t *predicate_offset) {
    uint32_t offset = 0;
    (void)put_header(bytes, &offset, 4);
    bytes[offset++] = 1;
    put_u16(bytes, &offset, 1);
    uint32_t column_id_offset = put_column(bytes, &offset, 3, 0);
    bytes[column_id_offset] = 10;
    bytes[column_id_offset + 16] = 0;
    bytes[column_id_offset + 17] = 0;
    bytes[column_id_offset + 18] = 0;
    bytes[column_id_offset + 19] = 0; /* ordinal 0 */
    bytes[offset++] = 2;
    *predicate_offset = offset;
    /* Comparison predicate: scanned column equals parameter value one. */
    put_integer_comparison(bytes, &offset, 1);
    bytes[offset++] = 3;
    put_u16(bytes, &offset, 1);
    column_id_offset = put_column(bytes, &offset, 3, 0);
    bytes[column_id_offset] = 10;
    bytes[column_id_offset + 16] = 0;
    bytes[column_id_offset + 17] = 0;
    bytes[column_id_offset + 18] = 0;
    bytes[column_id_offset + 19] = 0; /* ordinal 0 */
    bytes[offset++] = 4;
    put_u32(bytes, &offset, 1);
    return offset;
}

static uint32_t build_date_filter_plan(uint8_t *bytes) {
    uint32_t offset = 0;
    (void)put_header(bytes, &offset, 4);
    bytes[offset++] = 1;
    put_u16(bytes, &offset, 1);
    uint32_t column_id_offset = put_column(
        bytes, &offset, PRDB_SDK_VALUE_TYPE_DATE, 0);
    bytes[column_id_offset] = 10;
    set_column_ordinal(bytes, column_id_offset, 0);
    bytes[offset++] = 2;
    bytes[offset++] = 0x20;
    bytes[offset++] = 0;
    bytes[offset++] = 0x10;
    bytes[offset++] = 10;
    memset(bytes + offset, 0, 15);
    offset += 15;
    bytes[offset++] = 0x04;
    bytes[offset++] = 0xff;
    bytes[offset++] = 0xff;
    bytes[offset++] = 0xff;
    bytes[offset++] = 0xd6;
    bytes[offset++] = 3;
    put_u16(bytes, &offset, 1);
    column_id_offset = put_column(
        bytes, &offset, PRDB_SDK_VALUE_TYPE_DATE, 0);
    bytes[column_id_offset] = 10;
    set_column_ordinal(bytes, column_id_offset, 0);
    bytes[offset++] = 4;
    put_u32(bytes, &offset, 1);
    return offset;
}

static uint32_t build_uuid_filter_plan(uint8_t *bytes) {
    uint32_t offset = 0;
    (void)put_header(bytes, &offset, 4);
    bytes[offset++] = 1;
    put_u16(bytes, &offset, 1);
    uint32_t column_id_offset = put_column(
        bytes, &offset, PRDB_SDK_VALUE_TYPE_UUID, 0);
    bytes[column_id_offset] = 10;
    set_column_ordinal(bytes, column_id_offset, 0);
    bytes[offset++] = 2;
    bytes[offset++] = 0x20;
    bytes[offset++] = 0;
    bytes[offset++] = 0x10;
    bytes[offset++] = 10;
    memset(bytes + offset, 0, 15);
    offset += 15;
    bytes[offset++] = 0x05;
    memset(bytes + offset, 0x12, 16);
    offset += 16;
    bytes[offset++] = 3;
    put_u16(bytes, &offset, 1);
    column_id_offset = put_column(
        bytes, &offset, PRDB_SDK_VALUE_TYPE_UUID, 0);
    bytes[column_id_offset] = 10;
    set_column_ordinal(bytes, column_id_offset, 0);
    bytes[offset++] = 4;
    put_u32(bytes, &offset, 1);
    return offset;
}

static uint32_t build_timestamptz_filter_plan(uint8_t *bytes) {
    uint32_t offset = 0;
    (void)put_header(bytes, &offset, 4);
    bytes[offset++] = 1;
    put_u16(bytes, &offset, 1);
    uint32_t column_id_offset = put_column(
        bytes, &offset, PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ, 0);
    bytes[column_id_offset] = 10;
    set_column_ordinal(bytes, column_id_offset, 0);
    bytes[offset++] = 2;
    bytes[offset++] = 0x20;
    bytes[offset++] = 0;
    bytes[offset++] = 0x10;
    bytes[offset++] = 10;
    memset(bytes + offset, 0, 15);
    offset += 15;
    bytes[offset++] = 0x06;
    memset(bytes + offset, 0, 8);
    bytes[offset + 7] = 11;
    offset += 8;
    bytes[offset++] = 3;
    put_u16(bytes, &offset, 1);
    column_id_offset = put_column(
        bytes, &offset, PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ, 0);
    bytes[column_id_offset] = 10;
    set_column_ordinal(bytes, column_id_offset, 0);
    bytes[offset++] = 4;
    put_u32(bytes, &offset, 1);
    return offset;
}

static uint32_t build_bytea_filter_plan(uint8_t *bytes) {
    uint32_t offset = 0;
    (void)put_header(bytes, &offset, 4);
    bytes[offset++] = 1;
    put_u16(bytes, &offset, 1);
    uint32_t column_id_offset = put_column(
        bytes, &offset, PRDB_SDK_VALUE_TYPE_BYTEA, 0);
    bytes[column_id_offset] = 10;
    set_column_ordinal(bytes, column_id_offset, 0);
    bytes[offset++] = 2;
    bytes[offset++] = 0x20;
    bytes[offset++] = 0;
    bytes[offset++] = 0x10;
    bytes[offset++] = 10;
    memset(bytes + offset, 0, 15);
    offset += 15;
    bytes[offset++] = 0x07;
    bytes[offset++] = 0;
    bytes[offset++] = 3;
    bytes[offset++] = 0;
    bytes[offset++] = 0x80;
    bytes[offset++] = 0xff;
    bytes[offset++] = 3;
    put_u16(bytes, &offset, 1);
    column_id_offset = put_column(
        bytes, &offset, PRDB_SDK_VALUE_TYPE_BYTEA, 0);
    bytes[column_id_offset] = 10;
    set_column_ordinal(bytes, column_id_offset, 0);
    bytes[offset++] = 4;
    put_u32(bytes, &offset, 1);
    return offset;
}

static uint32_t build_decimal_filter_plan(uint8_t *bytes) {
    uint32_t offset = 0;
    (void)put_header(bytes, &offset, 4);
    bytes[offset++] = 1;
    put_u16(bytes, &offset, 1);
    uint32_t column_id_offset = put_column(
        bytes, &offset, PRDB_SDK_VALUE_TYPE_DECIMAL, 0);
    bytes[column_id_offset] = 10;
    bytes[column_id_offset + 21] = 8;
    bytes[column_id_offset + 22] = 2;
    set_column_ordinal(bytes, column_id_offset, 0);
    bytes[offset++] = 2;
    bytes[offset++] = 0x20;
    bytes[offset++] = 0;
    bytes[offset++] = 0x10;
    bytes[offset++] = 10;
    memset(bytes + offset, 0, 15);
    offset += 15;
    bytes[offset++] = 0x08;
    memset(bytes + offset, 0, 13);
    offset += 13;
    bytes[offset++] = 1;
    bytes[offset++] = 0xe2;
    bytes[offset++] = 0x40;
    bytes[offset++] = 3;
    put_u16(bytes, &offset, 1);
    column_id_offset = put_column(
        bytes, &offset, PRDB_SDK_VALUE_TYPE_DECIMAL, 0);
    bytes[column_id_offset] = 10;
    bytes[column_id_offset + 21] = 8;
    bytes[column_id_offset + 22] = 2;
    set_column_ordinal(bytes, column_id_offset, 0);
    bytes[offset++] = 4;
    put_u32(bytes, &offset, 1);
    return offset;
}

static uint32_t build_null_filter_plan(uint8_t *bytes,
                                       uint8_t predicate_tag,
                                       uint32_t *predicate_offset) {
    uint32_t offset = 0;
    (void)put_header(bytes, &offset, 4);
    bytes[offset++] = 1;
    put_u16(bytes, &offset, 1);
    uint32_t column_id_offset = put_column(bytes, &offset, 3, 1);
    bytes[column_id_offset] = 10;
    set_column_ordinal(bytes, column_id_offset, 0);
    bytes[offset++] = 2;
    *predicate_offset = offset;
    bytes[offset++] = predicate_tag;
    bytes[offset++] = 0x10;
    memset(bytes + offset, 0, 16);
    bytes[offset] = 10;
    offset += 16;
    bytes[offset++] = 3;
    put_u16(bytes, &offset, 1);
    column_id_offset = put_column(bytes, &offset, 3, 1);
    bytes[column_id_offset] = 10;
    set_column_ordinal(bytes, column_id_offset, 0);
    bytes[offset++] = 4;
    put_u32(bytes, &offset, 1);
    return offset;
}

static uint32_t build_cell_state_row_batch(uint8_t *bytes, uint8_t state) {
    uint32_t offset = 0;
    put_u16(bytes, &offset, 1);
    bytes[offset++] = 1;
    put_u16(bytes, &offset, 1);
    bytes[offset++] = 8;
    memset(bytes + offset, 0, 15);
    offset += 15;
    put_u16(bytes, &offset, 4);
    bytes[offset++] = state;
    if (state == PRDB_SDK_VALUE_STATE_VALUE) {
        bytes[offset++] = PRDB_SDK_VALUE_TYPE_INTEGER;
        put_u32(bytes, &offset, 4);
        memset(bytes + offset, 0, 3);
        bytes[offset + 3] = 1;
        offset += 4;
    }
    for (uint32_t ordinal = 1; ordinal < 4; ++ordinal) {
        bytes[offset++] = PRDB_SDK_VALUE_STATE_VALUE;
        bytes[offset++] = ordinal == 3 ? PRDB_SDK_VALUE_TYPE_TEXT
                                       : PRDB_SDK_VALUE_TYPE_INTEGER;
        put_u32(bytes, &offset, ordinal == 3 ? 1 : 4);
        if (ordinal == 3) {
            bytes[offset++] = 'x';
        } else {
            memset(bytes + offset, 0, 3);
            bytes[offset + 3] = (uint8_t)(ordinal + 1);
            offset += 4;
        }
    }
    return offset;
}

static uint32_t append_integer_point_hint(uint8_t *bytes, uint32_t offset) {
    bytes[offset++] = 0xa1;
    memset(bytes + offset, 0x77, 16);
    offset += 16;
    uint32_t column_id_offset = put_column(bytes, &offset, 3, 0);
    bytes[column_id_offset] = 10;
    memset(bytes + column_id_offset + 16, 0, 4);
    bytes[offset++] = 0x02;
    memset(bytes + offset, 0, 7);
    offset += 7;
    bytes[offset++] = 1;
    return offset;
}

static uint32_t append_integer_range_hint(uint8_t *bytes, uint32_t offset) {
    bytes[offset++] = 0xa2;
    memset(bytes + offset, 0x66, 16);
    offset += 16;
    uint32_t column_id_offset = put_column(bytes, &offset, 3, 0);
    bytes[column_id_offset] = 10;
    bytes[column_id_offset + 16] = 0;
    bytes[column_id_offset + 17] = 0;
    bytes[column_id_offset + 18] = 0;
    bytes[column_id_offset + 19] = 0;
    bytes[offset++] = 1; /* inclusive lower bound */
    bytes[offset++] = 0x02;
    memset(bytes + offset, 0, 7);
    offset += 7;
    bytes[offset++] = 1;
    bytes[offset++] = 0; /* no upper bound */
    return offset;
}

static uint32_t append_integer_equality_range_hint(uint8_t *bytes,
                                                   uint32_t offset) {
    bytes[offset++] = 0xa2;
    memset(bytes + offset, 0x66, 16);
    offset += 16;
    uint32_t column_id_offset = put_column(bytes, &offset, 3, 0);
    bytes[column_id_offset] = 10;
    memset(bytes + column_id_offset + 16, 0, 4);
    bytes[offset++] = 1;
    bytes[offset++] = 0x02;
    memset(bytes + offset, 0, 7);
    offset += 7;
    bytes[offset++] = 1;
    bytes[offset++] = 1;
    bytes[offset++] = 0x02;
    memset(bytes + offset, 0, 7);
    offset += 7;
    bytes[offset++] = 1;
    return offset;
}

static uint32_t build_two_sided_range_plan(uint8_t *bytes,
                                           uint32_t *predicate_offset) {
    uint32_t offset = 0;
    (void)put_header(bytes, &offset, 4);
    bytes[offset++] = 1;
    put_u16(bytes, &offset, 1);
    uint32_t column_id_offset = put_column(bytes, &offset, 3, 0);
    bytes[column_id_offset] = 10;
    bytes[column_id_offset + 16] = 0;
    bytes[column_id_offset + 17] = 0;
    bytes[column_id_offset + 18] = 0;
    bytes[column_id_offset + 19] = 0;
    bytes[offset++] = 2;
    *predicate_offset = offset;
    bytes[offset++] = 0x30;
    bytes[offset++] = 2;
    put_integer_comparison(bytes, &offset, 20);
    bytes[*predicate_offset + 3] = 2; /* column < 20 */
    put_integer_literal_column_comparison(bytes, &offset, 3, 10);
    bytes[offset++] = 3;
    put_u16(bytes, &offset, 1);
    column_id_offset = put_column(bytes, &offset, 3, 0);
    bytes[column_id_offset] = 10;
    bytes[column_id_offset + 16] = 0;
    bytes[column_id_offset + 17] = 0;
    bytes[column_id_offset + 18] = 0;
    bytes[column_id_offset + 19] = 0;
    bytes[offset++] = 4;
    put_u32(bytes, &offset, 1);
    return offset;
}

static uint32_t append_integer_two_sided_range_hint(uint8_t *bytes,
                                                    uint32_t offset) {
    bytes[offset++] = 0xa2;
    memset(bytes + offset, 0x66, 16);
    offset += 16;
    uint32_t column_id_offset = put_column(bytes, &offset, 3, 0);
    bytes[column_id_offset] = 10;
    bytes[column_id_offset + 16] = 0;
    bytes[column_id_offset + 17] = 0;
    bytes[column_id_offset + 18] = 0;
    bytes[column_id_offset + 19] = 0;
    bytes[offset++] = 1;
    bytes[offset++] = 0x02;
    memset(bytes + offset, 0, 7);
    offset += 7;
    bytes[offset++] = 10;
    bytes[offset++] = 2;
    bytes[offset++] = 0x02;
    memset(bytes + offset, 0, 7);
    offset += 7;
    bytes[offset++] = 20;
    return offset;
}

static uint32_t build_unique_probe_plan(uint8_t *bytes,
                                        uint32_t *predicate_offset) {
    uint32_t offset = build_filter_plan(bytes, predicate_offset);
    bytes[offset++] = 0xa1;
    memset(bytes + offset, 0x71, 16);
    offset += 16;
    uint32_t column_id_offset = put_column(bytes, &offset, 3, 0);
    bytes[column_id_offset] = 10;
    bytes[column_id_offset + 16] = 0;
    bytes[column_id_offset + 17] = 0;
    bytes[column_id_offset + 18] = 0;
    bytes[column_id_offset + 19] = 0;
    bytes[offset++] = 0x02;
    memset(bytes + offset, 0, 7);
    offset += 7;
    bytes[offset++] = 1;
    return offset;
}

static uint32_t build_multi_point_probe_plan(uint8_t *bytes) {
    uint32_t predicate_offset;
    uint32_t offset = build_filter_plan(bytes, &predicate_offset);
    const uint32_t old_predicate_len = 28;
    const uint32_t new_predicate_len = 2 + 2 * old_predicate_len + 20;
    memmove(bytes + predicate_offset + new_predicate_len,
            bytes + predicate_offset + old_predicate_len,
            offset - predicate_offset - old_predicate_len);
    uint32_t predicate_end = predicate_offset;
    bytes[predicate_end++] = 0x31;
    bytes[predicate_end++] = 3;
    put_integer_comparison(bytes, &predicate_end, 1);
    put_integer_comparison(bytes, &predicate_end, 2);
    put_integer_null_comparison(bytes, &predicate_end);
    offset += new_predicate_len - old_predicate_len;
    bytes[offset++] = 0xa4;
    memset(bytes + offset, 0x72, 16);
    offset += 16;
    uint32_t column_offset = put_column(bytes, &offset, 3, 0);
    bytes[column_offset] = 10;
    set_column_ordinal(bytes, column_offset, 0);
    put_u16(bytes, &offset, 2);
    for (uint8_t value = 1; value <= 2; ++value) {
        bytes[offset++] = 0x02;
        memset(bytes + offset, 0, 7);
        offset += 7;
        bytes[offset++] = value;
    }
    return offset;
}

static uint32_t build_boolean_filter_plan(uint8_t *bytes,
                                          uint32_t *predicate_offset) {
    uint32_t offset = 0;
    (void)put_header(bytes, &offset, 4);
    bytes[offset++] = 1;
    put_u16(bytes, &offset, 1);
    uint32_t column_id_offset = put_column(bytes, &offset, 3, 0);
    bytes[column_id_offset] = 10;
    bytes[column_id_offset + 16] = 0;
    bytes[column_id_offset + 17] = 0;
    bytes[column_id_offset + 18] = 0;
    bytes[column_id_offset + 19] = 0;
    bytes[offset++] = 2;
    *predicate_offset = offset;
    bytes[offset++] = 0x30;
    bytes[offset++] = 2;
    put_integer_comparison(bytes, &offset, 1);
    put_integer_comparison(bytes, &offset, 1);
    bytes[offset++] = 3;
    put_u16(bytes, &offset, 1);
    column_id_offset = put_column(bytes, &offset, 3, 0);
    bytes[column_id_offset] = 10;
    bytes[column_id_offset + 16] = 0;
    bytes[column_id_offset + 17] = 0;
    bytes[column_id_offset + 18] = 0;
    bytes[column_id_offset + 19] = 0;
    bytes[offset++] = 4;
    put_u32(bytes, &offset, 1);
    return offset;
}

static uint32_t build_delete_plan(uint8_t *bytes,
                                  uint32_t *predicate_offset) {
    uint32_t offset = 0;
    (void)put_header(bytes, &offset, 3);
    bytes[2] = 2; /* DELETE */
    bytes[offset++] = 1;
    put_u16(bytes, &offset, 0); /* constant predicate needs no scan columns */
    bytes[offset++] = 2;
    *predicate_offset = offset;
    bytes[offset++] = 0x01;
    bytes[offset++] = 1;
    bytes[offset++] = 5; /* Delete */
    return offset;
}

static uint32_t build_delete_filter_plan(uint8_t *bytes,
                                         uint32_t *predicate_offset) {
    uint32_t offset = 0;
    uint32_t column_id_offset;
    (void)put_header(bytes, &offset, 3);
    bytes[2] = 2; /* DELETE */
    bytes[offset++] = 1;
    put_u16(bytes, &offset, 1);
    column_id_offset = put_column(bytes, &offset, 3, 0);
    bytes[column_id_offset] = 10; /* c0 */
    bytes[column_id_offset + 16] = 0;
    bytes[column_id_offset + 17] = 0;
    bytes[column_id_offset + 18] = 0;
    bytes[column_id_offset + 19] = 0; /* ordinal 0 */
    bytes[offset++] = 2;
    *predicate_offset = offset;
    put_integer_comparison(bytes, &offset, 1);
    bytes[offset++] = 5; /* Delete */
    return offset;
}

static uint32_t build_update_plan(uint8_t *bytes) {
    uint32_t offset = 0;
    uint32_t column_id_offset;
    (void)put_header(bytes, &offset, 3);
    bytes[2] = 3; /* UPDATE */
    bytes[offset++] = 1;
    put_u16(bytes, &offset, 1);
    column_id_offset = put_column(bytes, &offset, 3, 0);
    bytes[column_id_offset] = 10;
    bytes[column_id_offset + 16] = 0;
    bytes[column_id_offset + 17] = 0;
    bytes[column_id_offset + 18] = 0;
    bytes[column_id_offset + 19] = 0;
    bytes[offset++] = 2;
    put_integer_comparison(bytes, &offset, 1);
    prdb_encode_optimizer_plan_update_node_record(
        bytes + offset, PRDB_OPTIMIZER_PLAN_UPDATE_NODE, 1);
    offset += PRDB_OPTIMIZER_PLAN_UPDATE_NODE_RECORD_HEADER_BYTES;
    column_id_offset = put_column(bytes, &offset, 3, 0);
    bytes[column_id_offset] = 10;
    bytes[column_id_offset + 16] = 0;
    bytes[column_id_offset + 17] = 0;
    bytes[column_id_offset + 18] = 0;
    bytes[column_id_offset + 19] = 0;
    prdb_encode_optimizer_plan_update_assignment_value_record(
        bytes + offset, PRDB_OPTIMIZER_UPDATE_PARAMETER_VALUE, 2);
    offset += PRDB_OPTIMIZER_PLAN_UPDATE_ASSIGNMENT_VALUE_RECORD_HEADER_BYTES;
    return offset;
}

typedef struct resolve_context {
    uint8_t response[512];
    uint32_t response_len;
    uint8_t executor_response[1024];
    uint32_t executor_response_len;
    uint8_t scan_response[18];
    uint8_t batch_response[4096];
    uint8_t probe_response[4096];
    uint8_t range_response[4096];
    uint32_t range_response_len;
    uint32_t range_response_too_small;
    uint32_t batch_response_len;
    uint32_t probe_response_len;
    uint32_t probe_calls;
    uint32_t range_calls;
    uint8_t cancellation_response[3];
    uint8_t close_response[3];
    uint8_t emitted_result[256];
    uint32_t emitted_result_len;
    uint32_t fail_result_write;
    uint32_t result_write_calls;
    uint8_t optimizer_response[512];
    uint32_t optimizer_response_len;
    uint32_t calls;
    uint32_t optimizer_calls;
    uint32_t fail_optimizer_request;
    uint32_t fail_join_identity;
    uint32_t stage_delete_calls;
    uint32_t stage_update_calls;
    uint32_t fail_stage_update;
    uint32_t cancel_on_check_call;
    uint32_t cancellation_check_calls;
    uint32_t statement_failures;
    uint16_t events[32];
    uint32_t event_count;
} resolve_context;

static int32_t optimizer_request(void *context, const uint8_t *request,
                                 uint64_t request_len, uint8_t *response,
                                 uint64_t response_capacity,
                                 uint64_t *response_len) {
    resolve_context *resolved = (resolve_context *)context;
    assert(request_len >= 16 && request[0] == 0 && request[1] == 1);
    if (resolved->event_count < 32) {
        resolved->events[resolved->event_count++] = UINT16_C(0xffff);
    }
    resolved->optimizer_calls++;
    if (resolved->fail_optimizer_request != 0)
        return PRDB_QUERY_CALLBACK_INTERNAL_ERROR;
    if (response_capacity < resolved->optimizer_response_len) {
        *response_len = resolved->optimizer_response_len;
        return PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL;
    }
    memcpy(response, resolved->optimizer_response,
           resolved->optimizer_response_len);
    *response_len = resolved->optimizer_response_len;
    return PRDB_QUERY_CALLBACK_OK;
}

static int32_t optimizer_request_v5(
    void *context, uint16_t operation, const uint8_t *request,
    uint64_t request_len, uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len) {
    assert(operation == PRDB_IPC_OP_OPTIMIZE_DELETE ||
           operation == PRDB_IPC_OP_OPTIMIZE_UPDATE);
    assert(request_len >= PRDB_OPTIMIZER_SELECT_REQUEST_HEADER_BYTES);
    assert(request[2] == (operation == PRDB_IPC_OP_OPTIMIZE_DELETE
                              ? PRDB_OPTIMIZER_DELETE_STATEMENT
                              : PRDB_OPTIMIZER_UPDATE_STATEMENT));
    return optimizer_request(context, request, request_len, response,
                             response_capacity, response_len);
}

static int32_t statement_failure(void *context, uint32_t failure_code) {
    resolve_context *resolved = (resolve_context *)context;
    assert(failure_code == PRDB_QUERY_STATEMENT_FAILURE_SQL);
    resolved->statement_failures++;
    return PRDB_QUERY_CALLBACK_OK;
}

static int32_t write_result(void *context, const uint8_t *bytes,
                            uint64_t bytes_len) {
    resolve_context *resolved = (resolve_context *)context;
    resolved->result_write_calls++;
    if (resolved->fail_result_write != 0)
        return PRDB_QUERY_CALLBACK_INTERNAL_ERROR;
    if (bytes_len > sizeof(resolved->emitted_result) -
                        resolved->emitted_result_len) {
        return PRDB_QUERY_CALLBACK_LIMIT_EXCEEDED;
    }
    memcpy(resolved->emitted_result + resolved->emitted_result_len, bytes,
           (size_t)bytes_len);
    resolved->emitted_result_len += (uint32_t)bytes_len;
    return PRDB_QUERY_CALLBACK_OK;
}

static uint32_t build_join_relation_response(uint8_t *bytes, uint32_t side);

static int32_t resolve_plan_relation(void *context, uint16_t operation,
                                     const uint8_t *request,
                                     uint64_t request_len, uint8_t *response,
                                     uint64_t response_capacity,
                                     uint64_t *response_len) {
    resolve_context *resolved = (resolve_context *)context;
    const uint8_t *source;
    uint32_t source_len;
    uint8_t join_relation_response[512];
    int32_t callback_status = PRDB_QUERY_CALLBACK_OK;
    if (operation == PRDB_IPC_OP_RESOLVE_PLAN_RELATION) {
        assert((request_len == 17 || request_len == 18) &&
               request[0] == 0 && request[1] == 1);
        assert(request[2] == 0 && request[3] == 6);
        assert(memcmp(request + 6, "public", 6) == 0);
        assert(request[4] == 0);
        if (request[5] == 5 && memcmp(request + 12, "items", 5) == 0) {
            source = resolved->response;
            source_len = resolved->response_len;
        } else if (request[5] == 5 &&
                   memcmp(request + 12, "lefts", 5) == 0) {
            source_len = build_join_relation_response(join_relation_response, 0);
            source = join_relation_response;
        } else {
            assert(request[5] == 6 && request_len == 18 &&
                   memcmp(request + 12, "rights", 6) == 0);
            source_len = build_join_relation_response(join_relation_response, 1);
            if (resolved->fail_join_identity != 0)
                join_relation_response[34] ^= 1;
            source = join_relation_response;
        }
    } else if (operation == PRDB_SDK_EXECUTOR_OP_RESOLVE_RELATION) {
        assert(request_len == 17 && request[0] == 0 && request[1] == 1);
        assert(request[2] == 0 && request[3] == 6);
        assert(memcmp(request + 6, "public", 6) == 0);
        assert(request[4] == 0 && request[5] == 5);
        assert(memcmp(request + 12, "items", 5) == 0);
        source = resolved->executor_response;
        source_len = resolved->executor_response_len;
    } else if (operation == PRDB_SDK_EXECUTOR_OP_OPEN_SCAN) {
        assert(request_len == 20 && request[0] == 0 && request[1] == 1);
        assert(request[2] == 6);
        assert(request[18] == 0 && request[19] == 0);
        source = resolved->scan_response;
        source_len = sizeof(resolved->scan_response);
    } else if (operation == PRDB_SDK_EXECUTOR_OP_CHECK_CANCELLATION) {
        assert(request_len == 0);
        ++resolved->cancellation_check_calls;
        if (resolved->cancel_on_check_call != 0) {
            resolved->cancellation_response[2] =
                resolved->cancellation_check_calls >=
                resolved->cancel_on_check_call;
        }
        source = resolved->cancellation_response;
        source_len = sizeof(resolved->cancellation_response);
    } else if (operation == PRDB_SDK_EXECUTOR_OP_NEXT_BATCH) {
        assert(request_len == 24 && request[2] == 7);
        source = resolved->batch_response;
        source_len = resolved->batch_response_len;
    } else if (operation == PRDB_SDK_EXECUTOR_OP_PROBE_UNIQUE_INDEX) {
        assert(request_len == 38 + 9 + 4 * 16);
        assert(request[0] == 0 && request[1] == 1 && request[2] == 6);
        assert(request[18] == 0x77 && request[34] == 0 && request[35] == 1);
        assert(request[36] == 0 && request[37] == 4);
        assert(request[38] == PRDB_SDK_VALUE_TYPE_INTEGER &&
               request[39] == 0 && request[40] == 0 && request[41] == 0 &&
               request[42] == 4 && request[43] == 0 && request[44] == 0 &&
               request[45] == 0 && request[46] == 1);
        assert(request[47] == 10 && request[63] == 11 &&
               request[79] == 12 && request[95] == 3);
        ++resolved->probe_calls;
        source = resolved->probe_response;
        source_len = resolved->probe_response_len;
    } else if (operation == PRDB_SDK_EXECUTOR_OP_SCAN_INDEX_RANGE) {
        assert(request_len == 38 + (request[44] == 0 ? 9 : 18) + 4 * 16);
        assert(request[0] == 0 && request[1] == 1 && request[2] == 6);
        assert(request[18] == 0x66 && request[34] == 1);
        assert(request[35] == PRDB_SDK_VALUE_TYPE_INTEGER &&
               request[36] == 0 && request[37] == 0 && request[38] == 0 &&
               request[39] == 4 && request[40] == 0 && request[41] == 0 &&
               request[42] == 0 && request[43] == 1);
        if (request[44] == 0) {
            assert(request[45] == 0 && request[46] == 4);
        } else {
            assert(request[44] == 1 &&
                   request[45] == PRDB_SDK_VALUE_TYPE_INTEGER &&
                   request[46] == 0 && request[47] == 0 &&
                   request[48] == 0 && request[49] == 4 &&
                   request[50] == 0 && request[51] == 0 &&
                   request[52] == 0 && request[53] == 1);
        }
        ++resolved->range_calls;
        if (resolved->range_response_too_small != 0)
            callback_status = PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL;
        source = resolved->range_response;
        source_len = resolved->range_response_len;
    } else if (operation == PRDB_SDK_EXECUTOR_OP_STAGE_DELETE) {
        static const uint8_t acknowledgement[] = {0, 1, 0};
        assert(request_len == 18 && request[0] == 0 && request[1] == 1);
        assert(request[2] == 8);
        ++resolved->stage_delete_calls;
        source = acknowledgement;
        source_len = sizeof(acknowledgement);
    } else if (operation == PRDB_SDK_EXECUTOR_OP_STAGE_UPDATE) {
        static const uint8_t acknowledgement[] = {0, 1, 0};
        assert(request_len == 34 && request[0] == 0 && request[1] == 1);
        assert(request[2] == 8 && request[18] == 0 && request[19] == 1);
        assert(request[20] == 0 && request[21] == 0 &&
               request[22] == 0 && request[23] == 0);
        assert(request[24] == 0 &&
               request[25] == PRDB_SDK_VALUE_TYPE_INTEGER);
        ++resolved->stage_update_calls;
        if (resolved->fail_stage_update != 0)
            callback_status = PRDB_QUERY_CALLBACK_INTERNAL_ERROR;
        source = acknowledgement;
        source_len = sizeof(acknowledgement);
    } else {
        assert(operation == PRDB_SDK_EXECUTOR_OP_CLOSE_CURSOR);
        assert(request_len == 18 && request[2] == 7);
        source = resolved->close_response;
        source_len = sizeof(resolved->close_response);
    }
    resolved->calls++;
    if (resolved->event_count < 32) {
        resolved->events[resolved->event_count++] = operation;
    }
    if (callback_status != PRDB_QUERY_CALLBACK_OK) return callback_status;
    if (response_capacity < source_len) {
        *response_len = source_len;
        return PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL;
    }
    memcpy(response, source, source_len);
    *response_len = source_len;
    return PRDB_QUERY_CALLBACK_OK;
}

static uint32_t build_relation_response(uint8_t *bytes) {
    uint32_t offset = 0;
    bytes[offset++] = 0;
    bytes[offset++] = 1;
    bytes[offset++] = 0;
    bytes[offset++] = 1;
    memset(bytes + offset, 0, 15);
    offset += 15;
    put_u64(bytes, &offset, 1);
    put_u64(bytes, &offset, 1);
    bytes[offset++] = 2;
    memset(bytes + offset, 0, 15);
    offset += 15;
    put_u16(bytes, &offset, 5);
    memcpy(bytes + offset, "items", 5);
    offset += 5;
    put_u16(bytes, &offset, 4);
    for (uint32_t ordinal = 0; ordinal < 4; ++ordinal) {
        bytes[offset++] = ordinal == 3 ? 3 : (uint8_t)(10 + ordinal);
        memset(bytes + offset, 0, 15);
        offset += 15;
        put_u32(bytes, &offset, ordinal);
        bytes[offset++] = ordinal == 3 ? 1 : 0;
        bytes[offset++] = ordinal == 3 ? 6 : 3;
        memset(bytes + offset, 0, 4);
        offset += 4;
        put_u16(bytes, &offset, 2);
        bytes[offset++] = 'c';
        bytes[offset++] = (uint8_t)('0' + ordinal);
    }
    return offset;
}

static uint32_t build_join_relation_response(uint8_t *bytes, uint32_t side) {
    uint32_t offset = 0;
    const char *name = side == 0 ? "lefts" : "rights";
    uint8_t relation_id = side == 0 ? 0x31 : 0x32;
    uint8_t column_base = side == 0 ? 0x41 : 0x51;
    bytes[offset++] = 0;
    bytes[offset++] = 1;
    bytes[offset++] = 0;
    memset(bytes + offset, 1, 16);
    offset += 16;
    put_u64(bytes, &offset, 3);
    put_u64(bytes, &offset, side == 0 ? 4 : 5);
    memset(bytes + offset, relation_id, 16);
    offset += 16;
    put_u16(bytes, &offset, (uint16_t)strlen(name));
    memcpy(bytes + offset, name, strlen(name));
    offset += (uint32_t)strlen(name);
    put_u16(bytes, &offset, 3);
    for (uint32_t ordinal = 0; ordinal < 3; ++ordinal) {
        bytes[offset++] = (uint8_t)(column_base + ordinal);
        memset(bytes + offset, 0, 15);
        offset += 15;
        put_u32(bytes, &offset, ordinal);
        bytes[offset++] = ordinal == 2 ? 1 : 0;
        bytes[offset++] = ordinal == 0 ? 3 : ordinal == 1 ? 1 : 6;
        memset(bytes + offset, 0, 4);
        offset += 4;
        put_u16(bytes, &offset,
                (uint16_t)(ordinal == 0 ? 2 : ordinal == 1 ? 6 : 5));
        if (ordinal == 0) {
            memcpy(bytes + offset, "id", 2);
        } else if (ordinal == 1) {
            memcpy(bytes + offset, "active", 6);
        } else {
            memcpy(bytes + offset, "value", 5);
        }
        offset += ordinal == 0 ? 2 : ordinal == 1 ? 6 : 5;
    }
    return offset;
}

static uint32_t build_executor_relation_response(uint8_t *bytes) {
    uint32_t offset = 0;
    bytes[offset++] = 0;
    bytes[offset++] = 1;
    bytes[offset++] = 0;
    bytes[offset++] = 2;
    memset(bytes + offset, 0, 15);
    offset += 15;
    bytes[offset++] = 6;
    memset(bytes + offset, 0, 15);
    offset += 15;
    put_u16(bytes, &offset, 5);
    memcpy(bytes + offset, "items", 5);
    offset += 5;
    put_u16(bytes, &offset, 4);
    for (uint32_t ordinal = 0; ordinal < 4; ++ordinal) {
        bytes[offset++] = ordinal == 3 ? 3 : (uint8_t)(10 + ordinal);
        memset(bytes + offset, 0, 15);
        offset += 15;
        put_u32(bytes, &offset, ordinal);
        bytes[offset++] = ordinal == 3 ? 1 : 0;
        bytes[offset++] = ordinal == 3 ? 6 : 3;
        memset(bytes + offset, 0, 4);
        offset += 4;
        put_u16(bytes, &offset, 2);
        bytes[offset++] = 'c';
        bytes[offset++] = (uint8_t)('0' + ordinal);
    }
    return offset;
}

static uint32_t build_row_batch(uint8_t *bytes) {
    uint32_t offset = 0;
    put_u16(bytes, &offset, 1);
    bytes[offset++] = 1;
    put_u16(bytes, &offset, 1);
    bytes[offset++] = 8;
    memset(bytes + offset, 0, 15);
    offset += 15;
    put_u16(bytes, &offset, 4);
    for (uint32_t ordinal = 0; ordinal < 4; ++ordinal) {
        bytes[offset++] = PRDB_SDK_VALUE_STATE_VALUE;
        bytes[offset++] = ordinal == 3 ? PRDB_SDK_VALUE_TYPE_TEXT
                                       : PRDB_SDK_VALUE_TYPE_INTEGER;
        put_u32(bytes, &offset, ordinal == 3 ? 1 : 4);
        if (ordinal == 3) {
            bytes[offset++] = 'x';
        } else {
            memset(bytes + offset, 0, 3);
            bytes[offset + 3] = (uint8_t)(ordinal + 1);
            offset += 4;
        }
    }
    return offset;
}

static uint32_t build_two_row_batch(uint8_t *bytes) {
    uint32_t length = build_row_batch(bytes);
    memcpy(bytes + length, bytes + 5, length - 5);
    bytes[4] = 2;
    bytes[length + 1] = 9;
    return length * 2 - 5;
}

static uint32_t build_repeated_row_batch(uint8_t *bytes, uint16_t row_count) {
    uint32_t offset = 0;
    put_u16(bytes, &offset, 1);
    bytes[offset++] = 1;
    put_u16(bytes, &offset, row_count);
    for (uint16_t row = 0; row < row_count; ++row) {
        bytes[offset++] = PRDB_SDK_BATCH_ROW_TOKEN_KIND;
        memset(bytes + offset, 0, 15);
        bytes[offset] = (uint8_t)(row + 1);
        offset += 15;
        put_u16(bytes, &offset, 4);
        for (uint32_t ordinal = 0; ordinal < 4; ++ordinal) {
            uint8_t type = ordinal == 3 ? PRDB_SDK_VALUE_TYPE_TEXT
                                        : PRDB_SDK_VALUE_TYPE_INTEGER;
            bytes[offset++] = PRDB_SDK_VALUE_STATE_VALUE;
            bytes[offset++] = type;
            put_u32(bytes, &offset, ordinal == 3 ? 1 : 4);
            if (ordinal == 3) {
                bytes[offset++] = 'x';
            } else {
                memset(bytes + offset, 0, 3);
                bytes[offset + 3] = 1;
                offset += 4;
            }
        }
    }
    return offset;
}

static uint32_t build_continuing_row_batch(uint8_t *bytes) {
    uint32_t length = build_row_batch(bytes);
    memmove(bytes + 19, bytes + 3, length - 3);
    bytes[2] = 0;
    bytes[3] = 7;
    memset(bytes + 4, 0, 15);
    return length + 16;
}

static uint32_t build_optimizer_response(uint8_t *bytes,
                                         const uint8_t *plan,
                                         uint32_t plan_len) {
    uint32_t offset = 0;
    put_u16(bytes, &offset, 1);
    bytes[offset++] = PRDB_SDK_OPTIMIZER_STATUS_SUCCESS;
    put_u32(bytes, &offset, plan_len);
    memcpy(bytes + offset, plan, plan_len);
    return offset + plan_len;
}

static uint32_t projected_rows_seen;

static int32_t on_projected_row(
    void *context, const prdb_sdk_optimizer_plan_column_v1 *projections,
    const prdb_sdk_value_v1 *values, uint32_t projection_count) {
    (void)context;
    assert(projection_count == 1);
    if (projections[0].ordinal == 3) {
        assert(values[0].type_code == PRDB_SDK_VALUE_TYPE_TEXT);
        assert(values[0].data_len == 1 && values[0].data[0] == 'x');
    } else {
        assert(projections[0].ordinal == 0);
        assert(values[0].type_code == PRDB_SDK_VALUE_TYPE_INTEGER);
        assert(values[0].data_len == 4 && values[0].data[3] == 1);
    }
    projected_rows_seen++;
    return 0;
}

static uint32_t predicate_rows_seen;

static int32_t count_predicate_row(
    void *context, const prdb_sdk_optimizer_plan_column_v1 *projections,
    const prdb_sdk_value_v1 *values, uint32_t projection_count) {
    (void)context;
    (void)projections;
    (void)values;
    assert(projection_count == 1);
    ++predicate_rows_seen;
    return 0;
}

static int32_t fail_projected_row(
    void *context, const prdb_sdk_optimizer_plan_column_v1 *projections,
    const prdb_sdk_value_v1 *values, uint32_t projection_count) {
    (void)context;
    (void)projections;
    (void)values;
    (void)projection_count;
    return 1;
}

