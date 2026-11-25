typedef struct mock_state {
    uint8_t planner[512];
    uint32_t planner_len;
    uint8_t executor[512];
    uint32_t executor_len;
    uint8_t batch[540000];
    uint32_t batch_len;
    uint8_t continued_batch[540000];
    uint32_t continued_batch_len;
    uint8_t optimizer_plan[1024];
    uint32_t optimizer_plan_len;
    uint8_t optimizer_relation[512];
    uint32_t optimizer_relation_len;
    uint32_t optimizer_aggregate_kind;
    uint32_t optimizer_aggregate_type;
    uint32_t optimizer_enabled;
    uint32_t optimizer_relation_resolves;
    uint32_t batch_finished;
    uint32_t planner_calls;
    uint32_t executor_calls;
    uint32_t executor_requests;
    uint32_t next_calls;
    uint32_t statement_failed;
    uint32_t cancelled;
    uint32_t cancel_after_first_batch;
    uint32_t cancel_when_batch_finishes;
    uint32_t cancel_after_group_callbacks;
    uint32_t streamed_pages;
    uint32_t max_candidates;
    uint32_t text_key_mode;
} mock_state;

typedef struct group_capture {
    mock_state *mock;
    uint32_t calls;
    uint32_t expect_having_literal;
    uint64_t expected_row_count;
    int32_t keys[128];
    int32_t second_keys[128];
    int32_t aggregates[128];
    uint32_t aggregate_states[128];
    uint64_t row_counts[128];
    uint32_t text_key_bytes;
    uint32_t scalar_order_mode;
    uint32_t scalar_order_type;
    uint8_t scalar_order_precision;
    uint8_t scalar_order_scale;
    uint32_t scalar_order_descending;
    uint32_t text_sum_tie_order_mode;
    const uint8_t *scalar_order_expected[3];
    uint32_t scalar_order_expected_len[3];
    uint8_t scalar_order_sqlr[3][128];
    uint64_t scalar_order_sqlr_len[3];
} group_capture;

static void put_u16(uint8_t *p, uint32_t *offset, uint16_t value) {
    p[(*offset)++] = (uint8_t)(value >> 8);
    p[(*offset)++] = (uint8_t)value;
}

static void put_u32(uint8_t *p, uint32_t *offset, uint32_t value) {
    p[(*offset)++] = (uint8_t)(value >> 24);
    p[(*offset)++] = (uint8_t)(value >> 16);
    p[(*offset)++] = (uint8_t)(value >> 8);
    p[(*offset)++] = (uint8_t)value;
}

static void put_name(uint8_t *p, uint32_t *offset, const char *name) {
    uint16_t length = (uint16_t)strlen(name);
    put_u16(p, offset, length);
    memcpy(p + *offset, name, length);
    *offset += length;
}

static void put_optimizer_name(uint8_t *p, uint32_t *offset,
                               const char *name) {
    uint8_t length = (uint8_t)strlen(name);
    p[(*offset)++] = length;
    memcpy(p + *offset, name, length);
    *offset += length;
}

static uint32_t put_optimizer_column(uint8_t *bytes, uint32_t offset,
                                     uint8_t id_fill, uint32_t ordinal,
                                     uint8_t type_code, uint8_t nullable) {
    uint8_t column_id[16];
    uint8_t type_parameters[4] = {0};
    memset(column_id, id_fill, sizeof(column_id));
    prdb_encode_optimizer_plan_column_record(bytes + offset, column_id,
                                             ordinal, type_code,
                                             type_parameters, nullable);
    return offset + PRDB_OPTIMIZER_PLAN_COLUMN_RECORD_HEADER_BYTES;
}

static uint32_t build_composite_aggregate_plan(mock_state *mock,
                                              uint8_t plan_kind,
                                              uint8_t key_count,
                                              uint8_t filtered_null) {
    uint32_t offset = 0;
    uint8_t schema_id[16];
    uint8_t relation_id[16];
    prdb_encode_optimizer_plan_header_record(mock->optimizer_plan + offset,
                                             1, plan_kind, 0);
    offset += PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    put_optimizer_name(mock->optimizer_plan, &offset, "app");
    memset(schema_id, 0x11, sizeof(schema_id));
    prdb_encode_optimizer_plan_schema_reference_record(
        mock->optimizer_plan + offset, schema_id, 1);
    offset += PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES;
    put_optimizer_name(mock->optimizer_plan, &offset, "widgets");
    memset(relation_id, 0x22, sizeof(relation_id));
    prdb_encode_optimizer_plan_relation_reference_record(
        mock->optimizer_plan + offset, relation_id, 1);
    offset += PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES;
    put_u16(mock->optimizer_plan, &offset, 2);
    offset = put_optimizer_column(mock->optimizer_plan, offset, 0x33, 0,
                                  PRDB_SDK_VALUE_TYPE_INTEGER, 1);
    offset = put_optimizer_column(mock->optimizer_plan, offset, 0x34, 1,
                                  mock->optimizer_aggregate_type == 0
                                      ? PRDB_SDK_VALUE_TYPE_INTEGER
                                      : mock->optimizer_aggregate_type,
                                  1);
    if (plan_kind == 15 || plan_kind == 16) {
        uint8_t id[16];
        prdb_encode_optimizer_plan_filter_node_record(
            mock->optimizer_plan + offset, 2);
        offset += PRDB_OPTIMIZER_PLAN_FILTER_NODE_RECORD_HEADER_BYTES;
        memset(id, 0x33, sizeof(id));
        if (filtered_null) {
            mock->optimizer_plan[offset++] = 0x41;
            prdb_encode_optimizer_plan_column_predicate_record(
                mock->optimizer_plan + offset, 0x10, id);
            offset += PRDB_OPTIMIZER_PLAN_COLUMN_PREDICATE_RECORD_HEADER_BYTES;
        } else {
            prdb_encode_optimizer_plan_compare_predicate_header_record(
                mock->optimizer_plan + offset, 0x20, 0);
            offset += PRDB_OPTIMIZER_PLAN_COMPARE_PREDICATE_HEADER_RECORD_HEADER_BYTES;
            prdb_encode_optimizer_plan_column_predicate_record(
                mock->optimizer_plan + offset, 0x10, id);
            offset += PRDB_OPTIMIZER_PLAN_COLUMN_PREDICATE_RECORD_HEADER_BYTES;
            prdb_encode_optimizer_plan_integer_predicate_record(
                mock->optimizer_plan + offset, 0x02, 1);
            offset += PRDB_OPTIMIZER_PLAN_INTEGER_PREDICATE_RECORD_HEADER_BYTES;
        }
    }
    mock->optimizer_plan[offset++] = plan_kind;
    mock->optimizer_plan[offset++] = (uint8_t)mock->optimizer_aggregate_kind;
    mock->optimizer_plan[offset++] = key_count;
    offset = put_optimizer_column(mock->optimizer_plan, offset, 0x33, 0,
                                  PRDB_SDK_VALUE_TYPE_INTEGER, 1);
    if (key_count > 1)
        offset = put_optimizer_column(mock->optimizer_plan, offset, 0x34, 1,
                                      PRDB_SDK_VALUE_TYPE_INTEGER, 1);
    mock->optimizer_plan[offset++] =
        mock->optimizer_aggregate_kind ==
                PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR
            ? 0
            : 1;
    if (mock->optimizer_aggregate_kind !=
        PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
        offset = put_optimizer_column(mock->optimizer_plan, offset, 0x34, 1,
                                      PRDB_SDK_VALUE_TYPE_INTEGER, 1);
    }
    if (plan_kind == 16) {
        static const uint8_t ordered_items[] = {
            3, 0, 0, 0, 1, 0, 1, 1, 0, 1, 1, 0};
        memcpy(mock->optimizer_plan + offset, ordered_items,
               sizeof(ordered_items));
        offset += sizeof(ordered_items);
    }
    mock->optimizer_plan[offset++] = (plan_kind == 15 || plan_kind == 16) ? 1 : 0;
    if (plan_kind == 15 || plan_kind == 16) put_u32(mock->optimizer_plan, &offset, 5);
    mock->optimizer_plan_len = offset;

    prdb_encode_planner_describe_plan_relation_available_response(
        mock->optimizer_relation, 0, schema_id, 1, 1, relation_id);
    offset = 51;
    prdb_encode_planner_describe_plan_relation_available_response_relation_name(
        mock->optimizer_relation + offset,
        sizeof(mock->optimizer_relation) - offset,
        (const uint8_t *)"widgets", sizeof("widgets") - 1);
    offset += 2 + sizeof("widgets") - 1;
    prdb_encode_planner_describe_plan_relation_available_response_column_count(
        mock->optimizer_relation + offset, 2);
    offset += 2;
    for (uint32_t ordinal = 0; ordinal < 2; ++ordinal) {
        uint8_t column_id[16];
        memset(column_id, (int)(0x33 + ordinal), sizeof(column_id));
        memcpy(mock->optimizer_relation + offset, column_id, 16);
        offset += 16;
        put_u32(mock->optimizer_relation, &offset, ordinal);
        mock->optimizer_relation[offset++] = 1;
        mock->optimizer_relation[offset++] = PRDB_SDK_VALUE_TYPE_INTEGER;
        memset(mock->optimizer_relation + offset, 0, 4);
        offset += 4;
        put_name(mock->optimizer_relation, &offset,
                 ordinal == 0 ? "id" : "score");
    }
    mock->optimizer_relation_len = offset;
    return mock->optimizer_plan_len;
}

static uint32_t build_ordered_eight_key_aggregate_plan(mock_state *mock) {
    uint32_t offset = 0;
    uint8_t schema_id[16], relation_id[16];
    prdb_encode_optimizer_plan_header_record(mock->optimizer_plan + offset,
                                             1, 14, 0);
    offset += PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    put_optimizer_name(mock->optimizer_plan, &offset, "app");
    memset(schema_id, 0x11, sizeof(schema_id));
    prdb_encode_optimizer_plan_schema_reference_record(
        mock->optimizer_plan + offset, schema_id, 1);
    offset += PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES;
    put_optimizer_name(mock->optimizer_plan, &offset, "widgets");
    memset(relation_id, 0x22, sizeof(relation_id));
    prdb_encode_optimizer_plan_relation_reference_record(
        mock->optimizer_plan + offset, relation_id, 1);
    offset += PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES;
    put_u16(mock->optimizer_plan, &offset, 9);
    for (uint32_t index = 0; index < 9; ++index)
        offset = put_optimizer_column(
            mock->optimizer_plan, offset, (uint8_t)(0x33 + index), index,
            PRDB_SDK_VALUE_TYPE_INTEGER, index == 8);
    mock->optimizer_plan[offset++] = 14;
    mock->optimizer_plan[offset++] = (uint8_t)mock->optimizer_aggregate_kind;
    mock->optimizer_plan[offset++] = 8;
    for (uint32_t index = 0; index < 8; ++index)
        offset = put_optimizer_column(
            mock->optimizer_plan, offset, (uint8_t)(0x33 + index), index,
            PRDB_SDK_VALUE_TYPE_INTEGER, 0);
    mock->optimizer_plan[offset++] = 1;
    offset = put_optimizer_column(mock->optimizer_plan, offset, 0x3b, 8,
                                  PRDB_SDK_VALUE_TYPE_INTEGER, 1);
    mock->optimizer_plan[offset++] = 14;
    mock->optimizer_plan[offset++] = 9;
    for (uint32_t index = 0; index < 8; ++index) {
        mock->optimizer_plan[offset++] = 0;
        mock->optimizer_plan[offset++] = (uint8_t)index;
        mock->optimizer_plan[offset++] = 1;
        mock->optimizer_plan[offset++] = 1;
    }
    mock->optimizer_plan[offset++] = 1;
    mock->optimizer_plan[offset++] = 1;
    mock->optimizer_plan[offset++] = 1;
    mock->optimizer_plan[offset++] = 1;
    put_u32(mock->optimizer_plan, &offset, 5);
    mock->optimizer_plan_len = offset;

    prdb_encode_planner_describe_plan_relation_available_response(
        mock->optimizer_relation, 0, schema_id, 1, 1, relation_id);
    offset = 51;
    prdb_encode_planner_describe_plan_relation_available_response_relation_name(
        mock->optimizer_relation + offset,
        sizeof(mock->optimizer_relation) - offset,
        (const uint8_t *)"widgets", sizeof("widgets") - 1);
    offset += 2 + sizeof("widgets") - 1;
    prdb_encode_planner_describe_plan_relation_available_response_column_count(
        mock->optimizer_relation + offset, 9);
    offset += 2;
    for (uint32_t index = 0; index < 9; ++index) {
        uint8_t column_id[16];
        static const char *key_names[8] = {
            "key_0", "key_1", "key_2", "key_3",
            "key_4", "key_5", "key_6", "key_7"};
        memset(column_id, (int)(0x33 + index), sizeof(column_id));
        memcpy(mock->optimizer_relation + offset, column_id, 16);
        offset += 16;
        put_u32(mock->optimizer_relation, &offset, index);
        mock->optimizer_relation[offset++] = index == 8 ? 1 : 0;
        mock->optimizer_relation[offset++] = PRDB_SDK_VALUE_TYPE_INTEGER;
        memset(mock->optimizer_relation + offset, 0, 4);
        offset += 4;
        put_name(mock->optimizer_relation, &offset,
                 index == 8 ? "measure" : key_names[index]);
    }
    mock->optimizer_relation_len = offset;
    return mock->optimizer_plan_len;
}

static uint32_t make_relation_with_types(uint8_t *p, uint8_t handle_kind,
                                         uint8_t handle_fill,
                                         uint32_t first_type,
                                         uint32_t second_type) {
    static const char *names[] = {"id", "score"};
    uint32_t offset = 0;
    put_u16(p, &offset, 1);
    p[offset++] = 0;
    memset(p + offset, 0x11, 16); offset += 16;
    p[offset++] = handle_kind;
    memset(p + offset, handle_fill, 15); offset += 15;
    put_name(p, &offset, "widgets");
    put_u16(p, &offset, 2);
    for (uint32_t index = 0; index < 2; ++index) {
        memset(p + offset, (int)(0x33 + index), 16); offset += 16;
        put_u32(p, &offset, index);
        p[offset++] = 1;
        uint32_t type = index == 0 ? first_type : second_type;
        p[offset++] = (uint8_t)type;
        if (type == PRDB_SDK_VALUE_TYPE_DECIMAL) {
            p[offset++] = 38;
            p[offset++] = 2;
            p[offset++] = 0;
            p[offset++] = 0;
        } else if (type == PRDB_SDK_VALUE_TYPE_VARCHAR) {
            put_u32(p, &offset, 128);
        } else {
            p[offset++] = 0;
            p[offset++] = 0;
            p[offset++] = 0;
            p[offset++] = 0;
        }
        put_name(p, &offset, names[index]);
    }
    return offset;
}

static uint32_t make_relation(uint8_t *p, uint8_t handle_kind,
                              uint8_t handle_fill, uint32_t key_type) {
    return make_relation_with_types(p, handle_kind, handle_fill, key_type,
                                    PRDB_SDK_VALUE_TYPE_INTEGER);
}

static uint32_t build_single_aggregate_plan(mock_state *mock) {
    uint32_t offset = 0;
    uint8_t schema_id[16];
    uint8_t relation_id[16];
    prdb_encode_optimizer_plan_header_record(mock->optimizer_plan + offset,
                                             1, 10, 0);
    offset += PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    put_optimizer_name(mock->optimizer_plan, &offset, "app");
    memset(schema_id, 0x11, sizeof(schema_id));
    prdb_encode_optimizer_plan_schema_reference_record(
        mock->optimizer_plan + offset, schema_id, 1);
    offset += PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES;
    put_optimizer_name(mock->optimizer_plan, &offset, "widgets");
    memset(relation_id, 0x22, sizeof(relation_id));
    prdb_encode_optimizer_plan_relation_reference_record(
        mock->optimizer_plan + offset, relation_id, 1);
    offset += PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES;
    put_u16(mock->optimizer_plan, &offset, 2);
    offset = put_optimizer_column(mock->optimizer_plan, offset, 0x33, 0,
                                  PRDB_SDK_VALUE_TYPE_INTEGER, 1);
    offset = put_optimizer_column(mock->optimizer_plan, offset, 0x34, 1,
                                  mock->optimizer_aggregate_type == 0
                                      ? PRDB_SDK_VALUE_TYPE_INTEGER
                                      : mock->optimizer_aggregate_type,
                                  1);
    mock->optimizer_plan[offset++] = 11;
    mock->optimizer_plan[offset++] = (uint8_t)mock->optimizer_aggregate_kind;
    offset = put_optimizer_column(mock->optimizer_plan, offset, 0x33, 0,
                                  PRDB_SDK_VALUE_TYPE_INTEGER, 1);
    mock->optimizer_plan[offset++] = 1;
    offset = put_optimizer_column(mock->optimizer_plan, offset, 0x34, 1,
                                  mock->optimizer_aggregate_type == 0
                                      ? PRDB_SDK_VALUE_TYPE_INTEGER
                                      : mock->optimizer_aggregate_type,
                                  1);
    mock->optimizer_plan[offset++] = 0;
    mock->optimizer_plan_len = offset;

    prdb_encode_planner_describe_plan_relation_available_response(
        mock->optimizer_relation, 0, schema_id, 1, 1, relation_id);
    offset = 51;
    prdb_encode_planner_describe_plan_relation_available_response_relation_name(
        mock->optimizer_relation + offset,
        sizeof(mock->optimizer_relation) - offset,
        (const uint8_t *)"widgets", sizeof("widgets") - 1);
    offset += 2 + sizeof("widgets") - 1;
    prdb_encode_planner_describe_plan_relation_available_response_column_count(
        mock->optimizer_relation + offset, 2);
    offset += 2;
    for (uint32_t ordinal = 0; ordinal < 2; ++ordinal) {
        uint8_t column_id[16];
        uint32_t type = ordinal == 0 || mock->optimizer_aggregate_type == 0
                            ? PRDB_SDK_VALUE_TYPE_INTEGER
                            : mock->optimizer_aggregate_type;
        memset(column_id, (int)(0x33 + ordinal), sizeof(column_id));
        memcpy(mock->optimizer_relation + offset, column_id, 16);
        offset += 16;
        put_u32(mock->optimizer_relation, &offset, ordinal);
        mock->optimizer_relation[offset++] = 1;
        mock->optimizer_relation[offset++] = (uint8_t)type;
        memset(mock->optimizer_relation + offset, 0, 4);
        offset += 4;
        put_name(mock->optimizer_relation, &offset,
                 ordinal == 0 ? "id" : "score");
    }
    mock->optimizer_relation_len = offset;
    return offset;
}

static uint32_t build_ordered_single_aggregate_plan(mock_state *mock) {
    uint32_t offset = 0;
    uint8_t schema_id[16];
    uint8_t relation_id[16];
    prdb_encode_optimizer_plan_header_record(mock->optimizer_plan + offset,
                                             1, 11, 0);
    offset += PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES;
    put_optimizer_name(mock->optimizer_plan, &offset, "app");
    memset(schema_id, 0x11, sizeof(schema_id));
    prdb_encode_optimizer_plan_schema_reference_record(
        mock->optimizer_plan + offset, schema_id, 1);
    offset += PRDB_OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES;
    put_optimizer_name(mock->optimizer_plan, &offset, "widgets");
    memset(relation_id, 0x22, sizeof(relation_id));
    prdb_encode_optimizer_plan_relation_reference_record(
        mock->optimizer_plan + offset, relation_id, 1);
    offset += PRDB_OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES;
    put_u16(mock->optimizer_plan, &offset, 2);
    offset = put_optimizer_column(mock->optimizer_plan, offset, 0x33, 0,
                                  PRDB_SDK_VALUE_TYPE_INTEGER, 1);
    offset = put_optimizer_column(mock->optimizer_plan, offset, 0x34, 1,
                                  PRDB_SDK_VALUE_TYPE_INTEGER, 1);
    mock->optimizer_plan[offset++] = 12;
    mock->optimizer_plan[offset++] = PRDB_SDK_SQL_V7_AGGREGATE_SUM;
    offset = put_optimizer_column(mock->optimizer_plan, offset, 0x33, 0,
                                  PRDB_SDK_VALUE_TYPE_INTEGER, 1);
    mock->optimizer_plan[offset++] = 1;
    offset = put_optimizer_column(mock->optimizer_plan, offset, 0x34, 1,
                                  PRDB_SDK_VALUE_TYPE_INTEGER, 1);
    mock->optimizer_plan[offset++] = 2;
    mock->optimizer_plan[offset++] = 1;
    mock->optimizer_plan[offset++] = 1;
    mock->optimizer_plan[offset++] = 0;
    mock->optimizer_plan[offset++] = 0;
    mock->optimizer_plan[offset++] = 0;
    mock->optimizer_plan[offset++] = 1;
    mock->optimizer_plan[offset++] = 0;
    mock->optimizer_plan_len = offset;
    return offset;
}

static uint32_t make_eight_key_relation(uint8_t *p, uint8_t handle_kind,
                                        uint8_t handle_fill) {
    uint32_t offset = 0;
    put_u16(p, &offset, 1);
    p[offset++] = 0;
    memset(p + offset, 0x11, 16); offset += 16;
    p[offset++] = handle_kind;
    memset(p + offset, handle_fill, 15); offset += 15;
    put_name(p, &offset, "widgets");
    put_u16(p, &offset, 9);
    for (uint32_t ordinal = 0; ordinal < 9; ++ordinal) {
        memset(p + offset, (int)(0x33 + ordinal), 16); offset += 16;
        put_u32(p, &offset, ordinal);
        p[offset++] = ordinal == 8 ? 1 : 0;
        p[offset++] = PRDB_SDK_VALUE_TYPE_INTEGER;
        p[offset++] = 0;
        p[offset++] = 0;
        p[offset++] = 0;
        p[offset++] = 0;
        if (ordinal == 8) {
            put_name(p, &offset, "measure");
        } else {
            char name[] = "key_0";
            name[4] = (char)('0' + ordinal);
            put_name(p, &offset, name);
        }
    }
    return offset;
}

static uint32_t make_decimal_relation(uint8_t *p, uint8_t handle_kind,
                                     uint8_t handle_fill,
                                     uint8_t decimal_precision,
                                     uint8_t decimal_scale) {
    uint32_t offset = 0;
    put_u16(p, &offset, 1);
    p[offset++] = 0;
    memset(p + offset, 0x11, 16); offset += 16;
    p[offset++] = handle_kind;
    memset(p + offset, handle_fill, 15); offset += 15;
    put_name(p, &offset, "widgets");
    put_u16(p, &offset, 2);
    for (uint32_t index = 0; index < 2; ++index) {
        memset(p + offset, (int)(0x33 + index), 16); offset += 16;
        put_u32(p, &offset, index);
        p[offset++] = 1;
        p[offset++] = index == 0 ? PRDB_SDK_VALUE_TYPE_INTEGER
                                 : PRDB_SDK_VALUE_TYPE_DECIMAL;
        p[offset++] = index == 0 ? 0 : decimal_precision;
        p[offset++] = index == 0 ? 0 : decimal_scale;
        p[offset++] = 0;
        p[offset++] = 0;
        put_name(p, &offset, index == 0 ? "id" : "score");
    }
    return offset;
}

static uint32_t make_bigint_relation(uint8_t *p, uint8_t handle_kind,
                                     uint8_t handle_fill) {
    uint32_t offset = 0;
    put_u16(p, &offset, 1);
    p[offset++] = 0;
    memset(p + offset, 0x11, 16); offset += 16;
    p[offset++] = handle_kind;
    memset(p + offset, handle_fill, 15); offset += 15;
    put_name(p, &offset, "widgets");
    put_u16(p, &offset, 2);
    for (uint32_t index = 0; index < 2; ++index) {
        memset(p + offset, (int)(0x33 + index), 16); offset += 16;
        put_u32(p, &offset, index);
        p[offset++] = 1;
        p[offset++] = index == 0 ? PRDB_SDK_VALUE_TYPE_INTEGER
                                 : PRDB_SDK_VALUE_TYPE_BIGINT;
        p[offset++] = 0;
        p[offset++] = 0;
        p[offset++] = 0;
        p[offset++] = 0;
        put_name(p, &offset, index == 0 ? "id" : "score");
    }
    return offset;
}

static void append_integer_cell(uint8_t *p, uint32_t *offset, int32_t value) {
    uint32_t bits = (uint32_t)value;
    p[(*offset)++] = PRDB_SDK_VALUE_STATE_VALUE;
    p[(*offset)++] = PRDB_SDK_VALUE_TYPE_INTEGER;
    put_u32(p, offset, 4);
    put_u32(p, offset, bits);
}

static void append_typed_cell(uint8_t *p, uint32_t *offset, uint32_t type,
                              const uint8_t *bytes, uint32_t byte_count) {
    p[(*offset)++] = PRDB_SDK_VALUE_STATE_VALUE;
    p[(*offset)++] = (uint8_t)type;
    put_u32(p, offset, byte_count);
    memcpy(p + *offset, bytes, byte_count);
    *offset += byte_count;
}

static uint32_t make_typed_key_rows(
    uint8_t *p, uint32_t finished, uint32_t key_type,
    const uint8_t *const key_values[3], const uint32_t key_lengths[3],
    uint32_t row_count) {
    uint32_t offset = 0;
    put_u16(p, &offset, 1);
    p[offset++] = (uint8_t)finished;
    if (!finished) {
        p[offset++] = 7;
        memset(p + offset, 0x55, 15);
        offset += 15;
    }
    put_u16(p, &offset, (uint16_t)row_count);
    for (uint32_t index = 0; index < row_count; ++index) {
        p[offset++] = 8;
        memset(p + offset, (int)(index + 1), 15);
        offset += 15;
        put_u16(p, &offset, 2);
        append_typed_cell(p, &offset, key_type, key_values[index],
                          key_lengths[index]);
        append_integer_cell(p, &offset, 1);
    }
    return offset;
}

static void append_decimal_cell(uint8_t *p, uint32_t *offset,
                                int64_t coefficient) {
    uint64_t bits = (uint64_t)coefficient;
    p[(*offset)++] = PRDB_SDK_VALUE_STATE_VALUE;
    p[(*offset)++] = PRDB_SDK_VALUE_TYPE_DECIMAL;
    put_u32(p, offset, 16);
    memset(p + *offset, coefficient < 0 ? 0xff : 0, 16);
    for (uint32_t index = 0; index < 8; ++index) {
        p[*offset + 15 - index] = (uint8_t)bits;
        bits >>= 8;
    }
    *offset += 16;
}

static void append_bigint_cell(uint8_t *p, uint32_t *offset, int64_t value) {
    uint64_t bits = (uint64_t)value;
    p[(*offset)++] = PRDB_SDK_VALUE_STATE_VALUE;
    p[(*offset)++] = PRDB_SDK_VALUE_TYPE_BIGINT;
    put_u32(p, offset, 8);
    for (uint32_t index = 0; index < 8; ++index) {
        p[*offset + 7 - index] = (uint8_t)bits;
        bits >>= 8;
    }
    *offset += 8;
}

static uint32_t make_bigint_rows(uint8_t *p, uint32_t finished,
                                 const int32_t *keys, const int64_t *scores,
                                 uint16_t row_count) {
    uint32_t offset = 0;
    put_u16(p, &offset, 1);
    p[offset++] = (uint8_t)finished;
    if (!finished) {
        p[offset++] = 7;
        memset(p + offset, 0x55, 15); offset += 15;
    }
    put_u16(p, &offset, row_count);
    for (uint32_t index = 0; index < row_count; ++index) {
        p[offset++] = 8;
        memset(p + offset, (int)(index + 1), 15); offset += 15;
        put_u16(p, &offset, 2);
        append_integer_cell(p, &offset, keys[index]);
        append_bigint_cell(p, &offset, scores[index]);
    }
    return offset;
}

static void append_text_cell(uint8_t *p, uint32_t *offset,
                             uint32_t key_index, uint32_t byte_count) {
    p[(*offset)++] = PRDB_SDK_VALUE_STATE_VALUE;
    p[(*offset)++] = PRDB_SDK_VALUE_TYPE_TEXT;
    put_u32(p, offset, byte_count);
    memset(p + *offset, 'a', byte_count);
    p[*offset + byte_count - 3] = (uint8_t)('0' + key_index / 100);
    p[*offset + byte_count - 2] =
        (uint8_t)('0' + (key_index / 10) % 10);
    p[*offset + byte_count - 1] = (uint8_t)('0' + key_index % 10);
    *offset += byte_count;
}

static uint32_t make_text_rows(uint8_t *p, uint32_t finished,
                               uint32_t first_key, uint16_t row_count,
                               uint32_t key_bytes) {
    uint32_t offset = 0;
    put_u16(p, &offset, 1);
    p[offset++] = (uint8_t)finished;
    if (!finished) {
        p[offset++] = 7;
        memset(p + offset, 0x55, 15);
        offset += 15;
    }
    put_u16(p, &offset, row_count);
    for (uint32_t index = 0; index < row_count; ++index) {
        p[offset++] = 8;
        memset(p + offset, (int)(index + 1), 15);
        offset += 15;
        put_u16(p, &offset, 2);
        append_text_cell(p, &offset, first_key + index, key_bytes);
        append_integer_cell(p, &offset, 1);
    }
    return offset;
}

static uint32_t make_text_grouped_sum_rows(uint8_t *p, uint32_t finished,
                                           uint32_t first_row,
                                           uint16_t row_count,
                                           uint32_t key_bytes) {
    uint32_t offset = 0;
    put_u16(p, &offset, 1);
    p[offset++] = (uint8_t)finished;
    if (!finished) {
        p[offset++] = 7;
        memset(p + offset, 0x55, 15);
        offset += 15;
    }
    put_u16(p, &offset, row_count);
    for (uint32_t index = 0; index < row_count; ++index) {
        uint32_t row_index = first_row + index;
        uint32_t group_index = row_index / 2;
        uint32_t bucket = group_index / 2;
        p[offset++] = 8;
        memset(p + offset, (int)(row_index + 1), 15);
        offset += 15;
        put_u16(p, &offset, 2);
        append_text_cell(p, &offset, group_index, key_bytes);
        append_integer_cell(p, &offset,
                            (int32_t)(row_index % 2 == 0 ? 100 + bucket
                                                        : 200 + bucket));
    }
    return offset;
}

static uint32_t make_rows(uint8_t *p, uint32_t finished,
                          const int32_t *keys, const int32_t *scores,
                          const uint8_t *score_states, uint16_t row_count) {
    uint32_t offset = 0;
    put_u16(p, &offset, 1);
    p[offset++] = (uint8_t)finished;
    if (!finished) {
        p[offset++] = 7;
        memset(p + offset, 0x55, 15); offset += 15;
    }
    put_u16(p, &offset, row_count);
    for (uint32_t index = 0; index < row_count; ++index) {
        p[offset++] = 8;
        memset(p + offset, (int)(index + 1), 15); offset += 15;
        put_u16(p, &offset, 2);
        append_integer_cell(p, &offset, keys[index]);
        if (score_states[index] == PRDB_SDK_VALUE_STATE_VALUE) {
            append_integer_cell(p, &offset, scores[index]);
        } else {
            p[offset++] = score_states[index];
        }
    }
    return offset;
}

static uint32_t make_boolean_count_rows(uint8_t *p) {
    static const int32_t keys[] = {1, 1, 2, 3};
    static const uint8_t states[] = {
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_STATE_VALUE,
        PRDB_SDK_VALUE_STATE_NULL, PRDB_SDK_VALUE_STATE_NULL};
    static const uint8_t booleans[] = {0, 1, 0, 0};
    uint32_t offset = 0;
    put_u16(p, &offset, 1);
    p[offset++] = 1;
    put_u16(p, &offset, 4);
    for (uint32_t index = 0; index < 4; ++index) {
        p[offset++] = 8;
        memset(p + offset, (int)(index + 1), 15);
        offset += 15;
        put_u16(p, &offset, 2);
        append_integer_cell(p, &offset, keys[index]);
        if (states[index] == PRDB_SDK_VALUE_STATE_VALUE)
            append_typed_cell(p, &offset, PRDB_SDK_VALUE_TYPE_BOOLEAN,
                              &booleans[index], 1);
        else
            p[offset++] = states[index];
    }
    return offset;
}

static uint32_t make_smallint_count_rows(uint8_t *p) {
    static const int32_t keys[] = {1, 1, 2, 3};
    static const uint8_t states[] = {
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_STATE_VALUE,
        PRDB_SDK_VALUE_STATE_NULL, PRDB_SDK_VALUE_STATE_VALUE};
    static const uint8_t smallints[][2] = {
        {0, 2}, {0, 4}, {0, 0}, {0, 7}};
    uint32_t offset = 0;
    put_u16(p, &offset, 1);
    p[offset++] = 1;
    put_u16(p, &offset, 4);
    for (uint32_t index = 0; index < 4; ++index) {
        p[offset++] = 8;
        memset(p + offset, (int)(index + 1), 15);
        offset += 15;
        put_u16(p, &offset, 2);
        append_integer_cell(p, &offset, keys[index]);
        if (states[index] == PRDB_SDK_VALUE_STATE_VALUE)
            append_typed_cell(p, &offset, PRDB_SDK_VALUE_TYPE_SMALLINT,
                              smallints[index], sizeof(smallints[index]));
        else
            p[offset++] = states[index];
    }
    return offset;
}

static uint32_t make_text_count_rows(uint8_t *p) {
    static const int32_t keys[] = {1, 1, 2, 3};
    static const uint8_t states[] = {
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_STATE_VALUE,
        PRDB_SDK_VALUE_STATE_NULL, PRDB_SDK_VALUE_STATE_VALUE};
    static const uint8_t *const values[] = {
        (const uint8_t *)"alpha", (const uint8_t *)"", NULL,
        (const uint8_t *)"omega"};
    static const uint32_t lengths[] = {5, 0, 0, 5};
    uint32_t offset = 0;
    put_u16(p, &offset, 1);
    p[offset++] = 1;
    put_u16(p, &offset, 4);
    for (uint32_t index = 0; index < 4; ++index) {
        p[offset++] = 8;
        memset(p + offset, (int)(index + 1), 15);
        offset += 15;
        put_u16(p, &offset, 2);
        append_integer_cell(p, &offset, keys[index]);
        if (states[index] == PRDB_SDK_VALUE_STATE_VALUE)
            append_typed_cell(p, &offset, PRDB_SDK_VALUE_TYPE_TEXT,
                              values[index], lengths[index]);
        else
            p[offset++] = states[index];
    }
    return offset;
}

static uint32_t make_nullable_composite_rows(uint8_t *p) {
    static const uint8_t first_states[] = {
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_STATE_VALUE,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_STATE_NULL,
        PRDB_SDK_VALUE_STATE_NULL,
        PRDB_SDK_VALUE_STATE_NULL};
    static const uint8_t second_states[] = {
        PRDB_SDK_VALUE_STATE_NULL, PRDB_SDK_VALUE_STATE_NULL,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_STATE_VALUE,
        PRDB_SDK_VALUE_STATE_NULL,
        PRDB_SDK_VALUE_STATE_NULL};
    static const int32_t first_values[] = {1, 1, 1, 0, 0, 0};
    static const int32_t second_values[] = {0, 0, 9, 9, 0, 0};
    uint32_t offset = 0;
    put_u16(p, &offset, 1);
    p[offset++] = 1;
    put_u16(p, &offset, 6);
    for (uint32_t row = 0; row < 6; ++row) {
        p[offset++] = 8;
        memset(p + offset, (int)(row + 1), 15);
        offset += 15;
        put_u16(p, &offset, 2);
        if (first_states[row] == PRDB_SDK_VALUE_STATE_VALUE)
            append_integer_cell(p, &offset, first_values[row]);
        else
            p[offset++] = first_states[row];
        if (second_states[row] == PRDB_SDK_VALUE_STATE_VALUE)
            append_integer_cell(p, &offset, second_values[row]);
        else
            p[offset++] = second_states[row];
    }
    return offset;
}

static uint32_t make_eight_key_rows(uint8_t *p) {
    static const int32_t measures[] = {4, 5, 8};
    uint32_t offset = 0;
    put_u16(p, &offset, 1);
    p[offset++] = 1;
    put_u16(p, &offset, 3);
    for (uint32_t row = 0; row < 3; ++row) {
        p[offset++] = 8;
        memset(p + offset, (int)(row + 1), 15); offset += 15;
        put_u16(p, &offset, 9);
        for (uint32_t key = 0; key < 8; ++key) {
            int32_t value = (int32_t)(key + 1);
            if (row == 2 && key == 7) value = 9;
            append_integer_cell(p, &offset, value);
        }
        append_integer_cell(p, &offset, measures[row]);
    }
    return offset;
}

static uint32_t make_decimal_rows(uint8_t *p, uint32_t finished,
                                 const int32_t *keys,
                                 const int64_t *coefficients,
                                 const uint8_t *score_states,
                                 uint16_t row_count) {
    uint32_t offset = 0;
    put_u16(p, &offset, 1);
    p[offset++] = (uint8_t)finished;
    if (!finished) {
        p[offset++] = 7;
        memset(p + offset, 0x55, 15); offset += 15;
    }
    put_u16(p, &offset, row_count);
    for (uint32_t index = 0; index < row_count; ++index) {
        p[offset++] = 8;
        memset(p + offset, (int)(index + 1), 15); offset += 15;
        put_u16(p, &offset, 2);
        append_integer_cell(p, &offset, keys[index]);
        if (score_states == NULL ||
            score_states[index] == PRDB_SDK_VALUE_STATE_VALUE) {
            append_decimal_cell(p, &offset, coefficients[index]);
        } else {
            p[offset++] = score_states[index];
        }
    }
    return offset;
}

static uint32_t make_decimal_rows_bytes(
    uint8_t *p, uint32_t finished, const int32_t *keys,
    const uint8_t (*coefficients)[16], uint16_t row_count) {
    uint32_t offset = 0;
    put_u16(p, &offset, 1);
    p[offset++] = (uint8_t)finished;
    if (!finished) {
        p[offset++] = 7;
        memset(p + offset, 0x55, 15);
        offset += 15;
    }
    put_u16(p, &offset, row_count);
    for (uint32_t index = 0; index < row_count; ++index) {
        p[offset++] = 8;
        memset(p + offset, (int)(index + 1), 15);
        offset += 15;
        put_u16(p, &offset, 2);
        append_integer_cell(p, &offset, keys[index]);
        p[offset++] = PRDB_SDK_VALUE_STATE_VALUE;
        p[offset++] = PRDB_SDK_VALUE_TYPE_DECIMAL;
        put_u32(p, &offset, 16);
        memcpy(p + offset, coefficients[index], 16);
        offset += 16;
    }
    return offset;
}

static int32_t statement_failure(void *context, uint32_t failure_code) {
    mock_state *mock = (mock_state *)context;
    assert(failure_code == PRDB_QUERY_STATEMENT_FAILURE_SQL);
    mock->statement_failed = 1;
    return PRDB_QUERY_CALLBACK_OK;
}

static int32_t planner_request(void *context, uint16_t operation,
                               const uint8_t *request, uint64_t request_len,
                               uint8_t *response, uint64_t capacity,
                               uint64_t *response_len) {
    static const uint8_t expected[] = {
        0, 1, 0, 3, 0, 7, 'a', 'p', 'p', 'w', 'i', 'd', 'g', 'e', 't', 's'};
    mock_state *mock = (mock_state *)context;
    assert(operation == PRDB_SDK_PLANNER_OP_DESCRIBE_RELATION);
    assert(request_len == sizeof(expected));
    assert(memcmp(request, expected, sizeof(expected)) == 0);
    assert(mock->planner_len <= capacity);
    memcpy(response, mock->planner, mock->planner_len);
    *response_len = mock->planner_len;
    ++mock->planner_calls;
    return PRDB_QUERY_CALLBACK_OK;
}

static int32_t optimizer_request(void *context, uint16_t operation,
                                 const uint8_t *request,
                                 uint64_t request_len, uint8_t *response,
                                 uint64_t capacity, uint64_t *response_len) {
    mock_state *mock = context;
    uint32_t offset = 4;
    uint8_t schema_len, relation_len;
    uint8_t key_count, aggregate_kind;
    uint8_t statement_kind;
    if (operation != PRDB_IPC_OP_OPTIMIZE_GROUP_COUNT || request == NULL ||
        request_len < 4 || response == NULL || response_len == NULL) {
        return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
    }
    statement_kind = request[2];
    if (offset >= request_len) return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
    schema_len = request[offset++];
    if (schema_len > request_len - offset)
        return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
    offset += schema_len;
    if (offset >= request_len) return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
    relation_len = request[offset++];
    if (relation_len > request_len - offset)
        return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
    offset += relation_len;
    if (statement_kind == 11) {
        uint8_t group_name_len;
        uint8_t aggregate_name_len;
        if (request_len - offset < 1) return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
        group_name_len = request[offset++];
        if (group_name_len != 2 || request_len - offset < 2 + 1 + 1 + 5 + 1 + 7 ||
            memcmp(request + offset, "id", 2) != 0)
            return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
        offset += group_name_len;
        if (request[offset++] != mock->optimizer_aggregate_kind)
            return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
        aggregate_name_len = request[offset++];
        if (aggregate_name_len != 5 || request_len - offset != 5 + 1 + 6 + 1 ||
            memcmp(request + offset, "score", 5) != 0)
            return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
        offset += aggregate_name_len;
        if (request[offset++] != 2 || request[offset++] != 1 ||
            request[offset++] != 1 || request[offset++] != 0 ||
            request[offset++] != 0 || request[offset++] != 0 ||
            request[offset++] != 1 || request[offset++] != 0 ||
            offset != request_len)
            return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
        if (build_ordered_single_aggregate_plan(mock) == 0)
            return PRDB_QUERY_CALLBACK_INTERNAL_ERROR;
        goto optimizer_plan_ready;
    }
    if (statement_kind == 10) {
        uint8_t group_name_len;
        uint8_t aggregate_name_len;
        if (offset >= request_len) return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
        group_name_len = request[offset++];
        if (group_name_len != 2 || request_len - offset < 2 + 1 + 1 + 5 + 1 ||
            memcmp(request + offset, "id", 2) != 0)
            return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
        offset += group_name_len;
        if (request[offset++] != mock->optimizer_aggregate_kind)
            return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
        aggregate_name_len = request[offset++];
        if (aggregate_name_len != 5 || request_len - offset != 6 ||
            memcmp(request + offset, "score", 5) != 0 ||
            request[offset + 5] != 0)
            return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
        if (build_single_aggregate_plan(mock) == 0)
            return PRDB_QUERY_CALLBACK_INTERNAL_ERROR;
        goto optimizer_plan_ready;
    }
    if (offset >= request_len) return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
    key_count = request[offset++];
    for (uint32_t index = 0; index < key_count; ++index) {
        uint8_t name_len;
        if (offset >= request_len) return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
        name_len = request[offset++];
        if (name_len > request_len - offset)
            return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
        offset += name_len;
    }
    if (offset >= request_len) return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
    aggregate_kind = request[offset++];
    if (statement_kind == 14) {
        uint8_t aggregate_name_len;
        if (aggregate_kind != PRDB_SDK_SQL_V7_AGGREGATE_SUM ||
            offset >= request_len)
            return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
        aggregate_name_len = request[offset++];
        if (aggregate_name_len > request_len - offset)
            return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
        offset += aggregate_name_len;
        if (offset >= request_len || request[offset++] != 9)
            return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
        for (uint32_t index = 0; index < 8; ++index) {
            if (request_len - offset < 4 || request[offset++] != 0 ||
                request[offset++] != index || request[offset++] != 1 ||
                request[offset++] != 1)
                return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
        }
        if (request_len - offset < 4 || request[offset++] != 1 ||
            request[offset++] != 1 || request[offset++] != 1 ||
            request[offset++] != 1 || request_len - offset != 4 ||
            request[offset] != 0 || request[offset + 1] != 0 ||
            request[offset + 2] != 0 || request[offset + 3] != 5 ||
            key_count != 8)
            return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
        if (build_ordered_eight_key_aggregate_plan(mock) == 0)
            return PRDB_QUERY_CALLBACK_INTERNAL_ERROR;
    } else if (statement_kind == 15 || statement_kind == 16) {
        static const uint8_t expected_filter[] = {
            0x20, 0, 0x10, 2, 'i', 'd', 0x02, 0, 0, 0, 0, 0, 0, 0, 1};
        static const uint8_t expected_single_filter[] = {
            0x41, 0x10, 2, 'i', 'd'};
        uint32_t predicate_len;
        uint8_t aggregate_name_len;
        if ((key_count != 1 && key_count != 2) ||
            aggregate_kind != mock->optimizer_aggregate_kind ||
            aggregate_kind != PRDB_SDK_SQL_V7_AGGREGATE_SUM ||
            offset >= request_len)
            return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
        aggregate_name_len = request[offset++];
        if (aggregate_name_len != 5 || request_len - offset < aggregate_name_len ||
            memcmp(request + offset, "score", 5) != 0)
            return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
        offset += aggregate_name_len;
        if (request_len - offset < 4) return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
        predicate_len = ((uint32_t)request[offset] << 24) |
                        ((uint32_t)request[offset + 1] << 16) |
                        ((uint32_t)request[offset + 2] << 8) | request[offset + 3];
        offset += 4;
        if (request_len - offset < predicate_len ||
            (statement_kind == 16 || key_count == 1
                 ? predicate_len != sizeof(expected_single_filter) ||
                       memcmp(request + offset, expected_single_filter,
                              sizeof(expected_single_filter)) != 0
                 : predicate_len != sizeof(expected_filter) ||
                       memcmp(request + offset, expected_filter,
                              sizeof(expected_filter)) != 0))
            return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
        offset += predicate_len;
        if (statement_kind == 16) {
            static const uint8_t expected_order[] = {
                3, 0, 0, 0, 1, 0, 1, 1, 0, 1, 1, 0};
            if (request_len - offset != sizeof(expected_order) + 5 ||
                memcmp(request + offset, expected_order, sizeof(expected_order)) != 0)
                return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
            offset += sizeof(expected_order);
        }
        if (request_len - offset != 5 || request[offset++] != 1 ||
            request[offset++] != 0 || request[offset++] != 0 ||
            request[offset++] != 0 || request[offset++] != 5)
            return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
        if (build_composite_aggregate_plan(mock, statement_kind, key_count,
                                           statement_kind == 16 || key_count == 1) == 0)
            return PRDB_QUERY_CALLBACK_INTERNAL_ERROR;
    } else if (key_count != 2 ||
               aggregate_kind != mock->optimizer_aggregate_kind) {
        fprintf(stderr,
                "unsupported aggregate optimizer request kind=%u keys=%u aggregate=%u len=%llu\n",
                statement_kind, key_count, aggregate_kind,
                (unsigned long long)request_len);
        return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
    } else if (build_composite_aggregate_plan(mock, 13, 2, 0) == 0) {
        return PRDB_QUERY_CALLBACK_INTERNAL_ERROR;
    }
optimizer_plan_ready:
    if (capacity < 7 + mock->optimizer_plan_len) {
        *response_len = 7 + mock->optimizer_plan_len;
        return PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL;
    }
    if (!prdb_encode_optimizer_response_success_plan(
            response + 7, capacity - 7, mock->optimizer_plan,
            mock->optimizer_plan_len))
        return PRDB_QUERY_CALLBACK_INTERNAL_ERROR;
    prdb_encode_optimizer_response_success(
        response, PRDB_SDK_OPTIMIZER_STATUS_SUCCESS,
        mock->optimizer_plan_len);
    *response_len = 7 + mock->optimizer_plan_len;
    mock->optimizer_enabled = 1;
    return PRDB_QUERY_CALLBACK_OK;
}

static int32_t unused_optimizer_request(void *context,
                                        const uint8_t *request,
                                        uint64_t request_len,
                                        uint8_t *response,
                                        uint64_t capacity,
                                        uint64_t *response_len) {
    (void)context;
    (void)request;
    (void)request_len;
    (void)response;
    (void)capacity;
    (void)response_len;
    return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
}

static int32_t executor_request(void *context, uint16_t operation,
                                const uint8_t *request, uint64_t request_len,
                                uint8_t *response, uint64_t capacity,
                                uint64_t *response_len) {
    mock_state *mock = (mock_state *)context;
    ++mock->executor_requests;
    if (operation == PRDB_SDK_EXECUTOR_OP_CHECK_CANCELLATION) {
        const uint8_t reply[] = {0, 1, (uint8_t)mock->cancelled};
        assert(request_len == 0 && sizeof(reply) <= capacity);
        memcpy(response, reply, sizeof(reply));
        *response_len = sizeof(reply);
    } else if (operation == PRDB_SDK_EXECUTOR_OP_RESOLVE_PLAN_RELATION) {
        ++mock->optimizer_relation_resolves;
        assert(mock->optimizer_relation_len <= capacity);
        memcpy(response, mock->optimizer_relation,
               mock->optimizer_relation_len);
        *response_len = mock->optimizer_relation_len;
    } else if (operation == PRDB_SDK_EXECUTOR_OP_RESOLVE_RELATION) {
        static const uint8_t expected[] = {
            0, 1, 0, 3, 0, 7, 'a', 'p', 'p', 'w', 'i', 'd', 'g', 'e', 't', 's'};
        assert(request_len == sizeof(expected));
        assert(memcmp(request, expected, sizeof(expected)) == 0);
        assert(mock->executor_len <= capacity);
        memcpy(response, mock->executor, mock->executor_len);
        *response_len = mock->executor_len;
    } else if (operation == PRDB_SDK_EXECUTOR_OP_OPEN_SCAN) {
        static const uint8_t expected[20] = {
            0, 1, 6, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22,
            0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0, 0};
        static const uint8_t reply[18] = {
            0, 1, 7, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
            0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44};
        assert(request_len == sizeof(expected));
        assert(memcmp(request, expected, sizeof(expected)) == 0);
        assert(sizeof(reply) <= capacity);
        memcpy(response, reply, sizeof(reply));
        *response_len = sizeof(reply);
    } else if (operation == PRDB_SDK_EXECUTOR_OP_NEXT_BATCH) {
        static const uint8_t finished_reply[5] = {0, 1, 1, 0, 0};
        static const uint8_t initial_cursor_tail[16] = {
            0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
            0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0};
        static const uint8_t continued_cursor_tail[16] = {
            0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
            0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0};
        assert(request_len == 24);
        assert(request[0] == 0 && request[1] == 1 && request[2] == 7);
        if (mock->streamed_pages != 0 &&
            mock->next_calls < mock->streamed_pages) {
            const uint8_t *batch = mock->batch;
            uint32_t batch_len = mock->batch_len;
            if (mock->next_calls != 0 && mock->continued_batch_len != 0) {
                batch = mock->continued_batch;
                batch_len = mock->continued_batch_len;
            }
            assert(batch_len <= capacity);
            memcpy(response, batch, batch_len);
            *response_len = batch_len;
        } else if (mock->next_calls == 0) {
            assert(memcmp(request + 3, initial_cursor_tail, 16) == 0);
            assert(mock->batch_len <= capacity);
            memcpy(response, mock->batch, mock->batch_len);
            *response_len = mock->batch_len;
        } else {
            assert(memcmp(request + 3, continued_cursor_tail, 16) == 0);
            assert(sizeof(finished_reply) <= capacity);
            memcpy(response, finished_reply, sizeof(finished_reply));
            *response_len = sizeof(finished_reply);
        }
        ++mock->next_calls;
        if (mock->next_calls == 1 && mock->cancel_after_first_batch) {
            mock->cancelled = 1;
        }
        if (mock->cancel_when_batch_finishes && mock->batch_finished) {
            mock->cancelled = 1;
        }
    } else if (operation == PRDB_SDK_EXECUTOR_OP_CLOSE_CURSOR) {
        static const uint8_t reply[] = {0, 1, 0};
        static const uint8_t continued_cursor_tail[15] = {
            0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
            0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55};
        static const uint8_t initial_cursor_tail[15] = {
            0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
            0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44};
        assert(request_len == 18);
        assert(request[0] == 0 && request[1] == 1 && request[2] == 7);
        assert(memcmp(request + 3,
                      request[3] == 0x44 ? initial_cursor_tail
                                          : continued_cursor_tail,
                      15) == 0);
        assert(sizeof(reply) <= capacity);
        memcpy(response, reply, sizeof(reply));
        *response_len = sizeof(reply);
    } else {
        assert(!"unexpected executor operation");
    }
    if (operation != PRDB_SDK_EXECUTOR_OP_CHECK_CANCELLATION) {
        ++mock->executor_calls;
    }
    return PRDB_QUERY_CALLBACK_OK;
}

static int32_t on_group(void *context, const prdb_sdk_value_v1 *group_key,
                        const prdb_sdk_value_v1 *aggregate_value,
                        uint64_t matching_row_count,
                        const prdb_sdk_select_binding_v10 *binding) {
    group_capture *capture = (group_capture *)context;
    int32_t key = 0;
    int32_t aggregate = 0;
    assert(capture->mock->batch_finished != 0);
    assert(capture->mock->executor_calls == 3);
    assert(capture->calls < 128);
    assert(binding->base.base.aggregate_kind ==
               PRDB_SDK_SQL_V7_AGGREGATE_MIN ||
           binding->base.base.aggregate_kind ==
               PRDB_SDK_SQL_V7_AGGREGATE_MAX ||
           binding->base.base.aggregate_kind ==
               PRDB_SDK_SQL_V7_AGGREGATE_SUM ||
           binding->base.base.aggregate_kind ==
               PRDB_SDK_SQL_V7_AGGREGATE_AVG ||
           binding->base.base.aggregate_kind ==
               PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR);
    assert(prdb_sdk_value_get_integer_v1(group_key, &key) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    if (binding->base.base.aggregate_kind ==
        PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
        assert(aggregate_value == NULL);
        aggregate = (int32_t)matching_row_count;
        capture->aggregate_states[capture->calls] =
            PRDB_SDK_VALUE_STATE_VALUE;
    } else {
        assert(aggregate_value != NULL);
        capture->aggregate_states[capture->calls] = aggregate_value->state;
        if (aggregate_value->state == PRDB_SDK_VALUE_STATE_VALUE) {
            if (binding->base.base.aggregate_kind ==
                PRDB_SDK_SQL_V7_AGGREGATE_SUM) {
                int64_t sum = 0;
                assert(prdb_sdk_value_get_bigint_v1(aggregate_value, &sum) ==
                       PRDB_SDK_VALUE_ACCESS_OK);
                aggregate = (int32_t)sum;
                assert(sum == aggregate);
            } else if (binding->base.base.aggregate_kind ==
                       PRDB_SDK_SQL_V7_AGGREGATE_AVG) {
                uint32_t coefficient = 0;
                assert(aggregate_value->type_code ==
                           PRDB_SDK_VALUE_TYPE_DECIMAL &&
                       aggregate_value->data_len == 16);
                for (uint32_t index = 0; index < 12; ++index) {
                    assert(aggregate_value->data[index] == 0);
                }
                for (uint32_t index = 12; index < 16; ++index) {
                    coefficient =
                        (coefficient << 8) | aggregate_value->data[index];
                }
                aggregate = (int32_t)coefficient;
            } else {
                assert(prdb_sdk_value_get_integer_v1(aggregate_value,
                                                     &aggregate) ==
                       PRDB_SDK_VALUE_ACCESS_OK);
            }
        } else {
            assert(aggregate_value->state == PRDB_SDK_VALUE_STATE_NULL);
        }
    }
    capture->keys[capture->calls] = key;
    capture->aggregates[capture->calls] = aggregate;
    capture->row_counts[capture->calls] = matching_row_count;
    ++capture->calls;
    if (capture->mock->cancel_after_group_callbacks == capture->calls) {
        capture->mock->cancelled = 1;
    }
    return 0;
}

