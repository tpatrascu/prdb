#include "prdb_sdk_optimizer_plan_v1.h"
#include "prdb_sdk_optimizer_v2.h"
#include "prdb_sdk_sql_exec_v3.h"
#include "prdb_sdk_sql_v24.h"
#include "prdb_query_abi_payload_codecs.h"

#include <assert.h>
#include <stdint.h>
#include <string.h>

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

int main(void) {
    uint8_t payload[256] = {0};
    uint8_t null_filter_payload[512] = {0};
    uint8_t malformed[sizeof(payload)];
    uint8_t join_payload[1024] = {0};
    uint8_t join_malformed[sizeof(join_payload)];
    uint8_t filtered_join_payload[sizeof(join_payload)] = {0};
    uint8_t filtered_join_malformed[sizeof(filtered_join_payload)];
    uint8_t group_count_payload[256] = {0};
    uint8_t group_count_malformed[sizeof(group_count_payload)];
    uint8_t ordered_group_count_payload[256] = {0};
    uint8_t composite_group_count_payload[512] = {0};
    uint8_t composite_group_aggregate_payload[512] = {0};
    uint8_t filtered_composite_group_aggregate_payload[512] = {0};
    uint8_t ordered_composite_group_count_payload[512] = {0};
    uint8_t group_aggregate_payload[512] = {0};
    uint8_t ordered_group_aggregate_payload[512] = {0};
    uint8_t group_aggregate_malformed[512] = {0};
    uint8_t composite_group_count_malformed[
        sizeof(composite_group_count_payload)];
    uint8_t ordered_select_payload[512] = {0};
    uint8_t ordered_select_malformed[sizeof(ordered_select_payload)];
    uint32_t projected_id_offset;
    uint32_t project_node_offset;
    uint32_t predicate_offset;
    uint32_t null_predicate_offset;
    uint32_t payload_len = build_plan(payload, &projected_id_offset,
                                      &project_node_offset);
    static prdb_sdk_optimizer_plan_v1 decoded;
    static prdb_sdk_optimizer_filter_v1 filter;
    static prdb_sdk_optimizer_update_plan_v1 update_decoded;
    static prdb_sdk_optimizer_join_plan_v1 join_decoded;
    static prdb_sdk_optimizer_filtered_join_plan_v1 filtered_join_decoded;
    static prdb_sdk_optimizer_group_count_plan_v1 group_count_decoded;
    static prdb_sdk_optimizer_ordered_group_count_plan_v1
        ordered_group_count_decoded;
    static prdb_sdk_optimizer_composite_group_count_plan_v1
        composite_group_count_decoded;
    static prdb_sdk_optimizer_composite_group_aggregate_plan_v1
        composite_group_aggregate_decoded;
    static prdb_sdk_optimizer_filtered_composite_group_aggregate_plan_v1
        filtered_composite_group_aggregate_decoded;
    static prdb_sdk_optimizer_ordered_composite_group_count_plan_v1
        ordered_composite_group_count_decoded;
    static prdb_sdk_optimizer_group_aggregate_plan_v1 group_aggregate_decoded;
    static prdb_sdk_optimizer_ordered_group_aggregate_plan_v1
        ordered_group_aggregate_decoded;
    static prdb_sdk_optimizer_ordered_select_plan_v1 ordered_select_decoded;
    static prdb_sdk_select_v3 ordered_select_statement;
    static prdb_sdk_select_binding_v3 ordered_select_binding;
    static prdb_sdk_sql_column_shape_v2 ordered_select_columns[2];
    static prdb_sdk_select_v19 join_statement;
    static prdb_sdk_select_binding_v19 join_binding;
    static prdb_sdk_select_v20 filtered_join_statement;
    static prdb_sdk_select_binding_v20 filtered_join_binding;
    static prdb_sdk_value_v1 filtered_join_parameters[3];
    static uint8_t filtered_join_parameter_bytes[3][1] = {{1}, {0}, {1}};
    static prdb_sdk_select_v4 group_count_statement;
    static prdb_sdk_select_binding_v4 group_count_binding;
    static prdb_sdk_select_v12 ordered_group_count_statement;
    static prdb_sdk_select_binding_v12 ordered_group_count_binding;
    static prdb_sdk_sql_column_shape_v2 ordered_group_count_columns[1];
    static prdb_sdk_select_v13 composite_group_count_statement;
    static prdb_sdk_select_binding_v13 composite_group_count_binding;
    static prdb_sdk_select_v13 composite_group_aggregate_statement;
    static prdb_sdk_select_binding_v13 composite_group_aggregate_binding;
    static prdb_sdk_select_v24 count_column_statement;
    static prdb_sdk_select_binding_v24 count_column_binding;
    static prdb_sdk_sql_column_shape_v2 composite_group_aggregate_columns[3];
    static uint8_t count_column_optimizer_request[512];
    static uint64_t count_column_optimizer_request_len;
    static prdb_sdk_select_v15 ordered_composite_group_count_statement;
    static prdb_sdk_select_binding_v15 ordered_composite_group_count_binding;
    static prdb_sdk_sql_column_shape_v2 ordered_composite_group_count_columns[2];
    static prdb_sdk_select_v7 group_aggregate_statement;
    static prdb_sdk_select_binding_v7 group_aggregate_binding;
    static prdb_sdk_select_v12 ordered_group_aggregate_statement;
    static prdb_sdk_select_binding_v12 ordered_group_aggregate_binding;
    static prdb_sdk_sql_column_shape_v2 group_aggregate_columns[2];
    uint32_t ordered_select_order_key_offset;
    uint32_t ordered_select_payload_len = build_ordered_select_plan(
        ordered_select_payload, &ordered_select_order_key_offset);
    assert(prdb_sdk_optimizer_decode_ordered_select_plan_v1(
               ordered_select_payload, ordered_select_payload_len,
               &ordered_select_decoded, sizeof(ordered_select_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(ordered_select_decoded.plan.plan_kind == 7 &&
           ordered_select_decoded.plan.scan_column_count == 2 &&
           ordered_select_decoded.plan.projection_count == 1 &&
           ordered_select_decoded.plan.projections[0].ordinal == 0 &&
           ordered_select_decoded.order_key.ordinal == 1 &&
           ordered_select_decoded.descending == 1 &&
           ordered_select_decoded.plan.has_limit == 1 &&
           ordered_select_decoded.plan.limit == 5);
    static const uint8_t ordered_select_sql[] =
        "SELECT id FROM items ORDER BY label DESC LIMIT 5";
    assert(prdb_sdk_sql_parse_select_v3(
               ordered_select_sql, sizeof(ordered_select_sql) - 1,
               &ordered_select_statement, sizeof(ordered_select_statement),
               NULL) == PRDB_SDK_SQL_OK);
    ordered_select_columns[0] = (prdb_sdk_sql_column_shape_v2){
        (const uint8_t *)"id", 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0};
    ordered_select_columns[1] = (prdb_sdk_sql_column_shape_v2){
        (const uint8_t *)"label", 5, 1, PRDB_SDK_VALUE_TYPE_TEXT, 0, 0};
    assert(prdb_sdk_sql_bind_select_v3(
               &ordered_select_statement, (const uint8_t *)"items", 5,
               ordered_select_columns, 2, &ordered_select_binding,
               sizeof(ordered_select_binding)) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_optimizer_match_ordered_select_plan_v1(
               &ordered_select_statement, &ordered_select_binding,
               (const uint8_t *)"public", 6, ordered_select_payload,
               ordered_select_payload_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    ordered_select_statement.order_descending = 0;
    assert(prdb_sdk_optimizer_match_ordered_select_plan_v1(
               &ordered_select_statement, &ordered_select_binding,
               (const uint8_t *)"public", 6, ordered_select_payload,
               ordered_select_payload_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    ordered_select_statement.order_descending = 1;
    memcpy(ordered_select_malformed, ordered_select_payload,
           ordered_select_payload_len);
    ordered_select_malformed[ordered_select_order_key_offset] = 9;
    assert(prdb_sdk_optimizer_decode_ordered_select_plan_v1(
               ordered_select_malformed, ordered_select_payload_len,
               &ordered_select_decoded, sizeof(ordered_select_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    static prdb_sdk_sql_column_shape_v2 join_left_columns[3];
    static prdb_sdk_sql_column_shape_v2 join_right_columns[3];
    static resolve_context resolve = {0};
    static prdb_host_api_v2 host = {0};
    static prdb_host_api_v4 host_v4 = {0};
    static prdb_host_api_v5 host_v5 = {0};
    static prdb_sdk_executor_relation_v1 acquired_relation;
    static prdb_sdk_sql_column_shape_v2 acquired_columns[4];
    static prdb_sdk_optimizer_scan_state_v1 scan_state;
    static prdb_sdk_batch_v1 row_batch;
    static prdb_sdk_value_v1 source_cells[4];
    static prdb_sdk_value_v1 projected_values[1];
    static prdb_sdk_value_v1 delete_cell_scratch[4];
    static prdb_sdk_select_binding_v2 delete_binding;
    uint8_t row_batch_bytes[128];
    uint8_t batch_response[128];
    uint8_t result_record_buffer[256];
    uint8_t scan_response[18];
    uint32_t row_batch_len;
    uint32_t delivered_rows = 0;
    uint32_t total_rows = 0;
    uint8_t response[512];
    uint8_t request[512];
    uint8_t optimizer_response[512];
    uint8_t probe_plan[512];
    uint32_t probe_predicate_offset;
    uint32_t probe_plan_len;
    uint8_t delete_target_handles[
        PRDB_SDK_OPTIMIZER_PLAN_MAX_DELETE_TARGETS * 16];
    uint8_t sql[] = "SELECT c3 FROM items LIMIT 1";
    static prdb_sdk_select_v2 statement;
    static prdb_sdk_delete_v1 delete_statement;
    static prdb_sdk_update_v1 update_statement;
    uint64_t response_len = 0;
    uint64_t scan_response_len = 0;

    assert(prdb_sdk_optimizer_decode_plan_v1(
               payload, payload_len, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               payload, payload_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(filter.present == 0 && filter.prefix == NULL &&
           filter.prefix_len == 0);
    assert(decoded.struct_size == sizeof(decoded));
    assert(decoded.abi_version == PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION);
    assert(decoded.schema_name_len == 6);
    assert(memcmp(decoded.schema_name, "public", 6) == 0);
    assert(decoded.relation_name_len == 5);
    assert(memcmp(decoded.relation_name, "items", 5) == 0);
    assert(decoded.schema_version == 1 && decoded.table_epoch == 1);
    assert(decoded.scan_column_count == 1 && decoded.projection_count == 1);
    assert(decoded.scan_columns[0].ordinal == 3);
    assert(decoded.projections[0].type_code == 6);
    assert(decoded.has_limit == 1 && decoded.limit == 7);
    assert(prdb_sdk_optimizer_scan_state_init_v1(
               payload, payload_len, &scan_state, sizeof(scan_state)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    row_batch_len = build_row_batch(row_batch_bytes);
    assert(prdb_sdk_batch_open_v1(&row_batch, sizeof(row_batch), row_batch_bytes,
                                  row_batch_len) == PRDB_SDK_BATCH_OK);
    projected_rows_seen = 0;
    assert(prdb_sdk_optimizer_consume_scan_batch_v1(
               &scan_state, &row_batch, 4, source_cells, 4, projected_values,
               1, on_projected_row, NULL, &delivered_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(delivered_rows == 1 && projected_rows_seen == 1);
    uint8_t limit_one_plan[sizeof(payload)];
    memcpy(limit_one_plan, payload, payload_len);
    limit_one_plan[payload_len - 1] = 1;
    assert(prdb_sdk_optimizer_scan_state_init_v1(
               limit_one_plan, payload_len, &scan_state, sizeof(scan_state)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(prdb_sdk_optimizer_consume_scan_batch_v1(
               &scan_state, &row_batch, 4, source_cells, 4, projected_values,
               1, on_projected_row, NULL, &delivered_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(delivered_rows == 1 && scan_state.done == 1);
    assert(prdb_sdk_optimizer_consume_scan_batch_v1(
               &scan_state, &row_batch, 4, source_cells, 4, projected_values,
               1, on_projected_row, NULL, &delivered_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(delivered_rows == 0 && projected_rows_seen == 2);
    payload_len = build_filter_plan(payload, &predicate_offset);
    assert(prdb_sdk_optimizer_scan_state_init_v1(
               payload, payload_len, &scan_state, sizeof(scan_state)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    scan_state.projections[0].ordinal = 1;
    assert(prdb_sdk_optimizer_consume_plan_batch_v1(
               payload, payload_len, &scan_state, &row_batch, 4, source_cells,
               4, projected_values, 1, on_projected_row, NULL,
               &delivered_rows) == PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    scan_state.projections[0].ordinal = 0;
    projected_rows_seen = 0;
    assert(prdb_sdk_optimizer_consume_plan_batch_v1(
               payload, payload_len, &scan_state, &row_batch, 4, source_cells,
               4, projected_values, 1, on_projected_row, NULL,
               &delivered_rows) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(delivered_rows == 1 && projected_rows_seen == 1);
    row_batch_bytes[32] = 2;
    assert(prdb_sdk_batch_open_v1(&row_batch, sizeof(row_batch), row_batch_bytes,
                                  row_batch_len) == PRDB_SDK_BATCH_OK);
    assert(prdb_sdk_optimizer_scan_state_init_v1(
               payload, payload_len, &scan_state, sizeof(scan_state)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(prdb_sdk_optimizer_consume_plan_batch_v1(
               payload, payload_len, &scan_state, &row_batch, 4, source_cells,
               4, projected_values, 1, on_projected_row, NULL,
               &delivered_rows) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(delivered_rows == 0 && projected_rows_seen == 1);
    row_batch_bytes[32] = 1;
    assert(prdb_sdk_batch_open_v1(&row_batch, sizeof(row_batch), row_batch_bytes,
                                  row_batch_len) == PRDB_SDK_BATCH_OK);
    for (uint8_t predicate_tag = 0x40; predicate_tag <= 0x41;
         ++predicate_tag) {
        uint32_t null_filter_len = build_null_filter_plan(
            null_filter_payload, predicate_tag, &null_predicate_offset);
        assert(null_predicate_offset < null_filter_len);
        for (uint8_t state = PRDB_SDK_VALUE_STATE_VALUE;
             state <= PRDB_SDK_VALUE_STATE_REDACTED; ++state) {
            static const uint32_t expected_rows[2][3] = {
                {0, 1, 0}, /* IS NULL */
                {1, 0, 0}, /* IS NOT NULL */
            };
            uint32_t state_index = state == PRDB_SDK_VALUE_STATE_VALUE
                                       ? 0
                                       : state == PRDB_SDK_VALUE_STATE_NULL
                                             ? 1
                                             : 2;
            row_batch_len = build_cell_state_row_batch(row_batch_bytes,
                                                       state);
            assert(prdb_sdk_batch_open_v1(
                       &row_batch, sizeof(row_batch), row_batch_bytes,
                       row_batch_len) == PRDB_SDK_BATCH_OK);
            assert(prdb_sdk_optimizer_scan_state_init_v1(
                       null_filter_payload, null_filter_len, &scan_state,
                       sizeof(scan_state)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
            predicate_rows_seen = 0;
            int32_t filter_status = prdb_sdk_optimizer_consume_plan_batch_v1(
                null_filter_payload, null_filter_len, &scan_state, &row_batch,
                4, source_cells, 4, projected_values, 1,
                count_predicate_row, NULL, &delivered_rows);
            if (state == PRDB_SDK_VALUE_STATE_REDACTED) {
                assert(filter_status == PRDB_SDK_OPTIMIZER_PLAN_FILTER_ERROR);
                assert(delivered_rows == 0 && predicate_rows_seen == 0);
            } else {
                uint32_t tag_index = predicate_tag == 0x40 ? 0 : 1;
                assert(filter_status == PRDB_SDK_OPTIMIZER_PLAN_OK);
                assert(delivered_rows == expected_rows[tag_index][state_index]);
                assert(predicate_rows_seen == expected_rows[tag_index][state_index]);
            }
        }
    }
    row_batch_len = build_row_batch(row_batch_bytes);
    assert(prdb_sdk_batch_open_v1(&row_batch, sizeof(row_batch), row_batch_bytes,
                                  row_batch_len) == PRDB_SDK_BATCH_OK);
    payload_len = build_plan(payload, &projected_id_offset,
                              &project_node_offset);

    resolve.response_len = build_relation_response(resolve.response);
    resolve.executor_response_len = build_executor_relation_response(
        resolve.executor_response);
    resolve.scan_response[1] = 1;
    resolve.scan_response[2] = 7;
    resolve.batch_response_len = build_continuing_row_batch(
        resolve.batch_response);
    resolve.cancellation_response[1] = 1;
    resolve.close_response[1] = 1;
    host.struct_size = sizeof(host);
    host.abi_version = PRDB_QUERY_ABI_V2_VERSION;
    host.context = &resolve;
    host.executor_request = resolve_plan_relation;
    {
        uint8_t empty_view_plan[512];
        prdb_sdk_optimizer_plan_v1 empty_decoded = {0};
        memcpy(empty_view_plan, payload, payload_len);
        empty_view_plan[payload_len] = 0xa3;
        resolve.calls = 0;
        resolve.cancellation_check_calls = 0;
        resolve.cancellation_response[2] = 0;
        projected_rows_seen = 0;
        total_rows = UINT32_MAX;
        assert(prdb_sdk_optimizer_execute_unfiltered_plan_v1(
                   &host, empty_view_plan, payload_len + 1, 2,
                   sizeof(batch_response), response, sizeof(response),
                   scan_response, sizeof(scan_response), batch_response,
                   sizeof(batch_response), &acquired_relation,
                   sizeof(acquired_relation), acquired_columns, 4, source_cells,
                   4, projected_values, 1, on_projected_row, NULL,
                   &total_rows) == PRDB_SDK_OPTIMIZER_PLAN_OK);
        assert(total_rows == 0 && projected_rows_seen == 0 &&
               resolve.calls == 3 && resolve.cancellation_check_calls == 1);
        resolve.calls = 0;
        resolve.cancellation_check_calls = 0;
        resolve.cancel_on_check_call = 1;
        assert(prdb_sdk_optimizer_execute_unfiltered_plan_v1(
                   &host, empty_view_plan, payload_len + 1, 2,
                   sizeof(batch_response), response, sizeof(response),
                   scan_response, sizeof(scan_response), batch_response,
                   sizeof(batch_response), &acquired_relation,
                   sizeof(acquired_relation), acquired_columns, 4, source_cells,
                   4, projected_values, 1, on_projected_row, NULL,
                   &total_rows) == PRDB_SDK_OPTIMIZER_PLAN_CANCELLED);
        assert(total_rows == 0 && projected_rows_seen == 0 &&
               resolve.calls == 3 && resolve.cancellation_check_calls == 1);
        resolve.cancel_on_check_call = 0;
        resolve.cancellation_response[2] = 0;
        empty_view_plan[payload_len + 1] = 0;
        assert(prdb_sdk_optimizer_decode_plan_v1(
                   empty_view_plan, payload_len + 2, &empty_decoded,
                   sizeof(empty_decoded)) ==
               PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    }
    uint8_t filtered_plan[512];
    uint8_t date_filter_plan[512];
    uint8_t uuid_filter_plan[512];
    uint8_t timestamptz_filter_plan[512];
    uint8_t bytea_filter_plan[512];
    uint8_t decimal_filter_plan[512];
    uint8_t probed_plan[512];
    uint8_t ranged_plan[512];
    uint8_t equality_ranged_plan[512];
    uint32_t filtered_plan_len = build_filter_plan(filtered_plan,
                                                   &predicate_offset);
    uint32_t date_filter_plan_len = build_date_filter_plan(date_filter_plan);
    prdb_sdk_optimizer_plan_v1 decoded_date_plan = {0};
    prdb_sdk_optimizer_filter_v1 decoded_date_filter = {0};
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               date_filter_plan, date_filter_plan_len, &decoded_date_plan,
               sizeof(decoded_date_plan), &decoded_date_filter,
               sizeof(decoded_date_filter)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(decoded_date_filter.present == 1 &&
           decoded_date_filter.prefix_len == 24 &&
           decoded_date_filter.prefix[0] == 0x20 &&
           decoded_date_filter.prefix[19] == 0x04);
    uint32_t uuid_filter_plan_len = build_uuid_filter_plan(uuid_filter_plan);
    prdb_sdk_optimizer_filter_v1 decoded_uuid_filter = {0};
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               uuid_filter_plan, uuid_filter_plan_len, &decoded_date_plan,
               sizeof(decoded_date_plan), &decoded_uuid_filter,
               sizeof(decoded_uuid_filter)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(decoded_uuid_filter.present == 1 &&
           decoded_uuid_filter.prefix_len == 36 &&
           decoded_uuid_filter.prefix[0] == 0x20 &&
           decoded_uuid_filter.prefix[19] == 0x05);
    uint32_t timestamptz_filter_plan_len =
        build_timestamptz_filter_plan(timestamptz_filter_plan);
    prdb_sdk_optimizer_filter_v1 decoded_timestamptz_filter = {0};
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               timestamptz_filter_plan, timestamptz_filter_plan_len,
               &decoded_date_plan, sizeof(decoded_date_plan),
               &decoded_timestamptz_filter,
               sizeof(decoded_timestamptz_filter)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(decoded_timestamptz_filter.present == 1 &&
           decoded_timestamptz_filter.prefix_len == 28 &&
           decoded_timestamptz_filter.prefix[0] == 0x20 &&
           decoded_timestamptz_filter.prefix[19] == 0x06);
    uint32_t bytea_filter_plan_len = build_bytea_filter_plan(bytea_filter_plan);
    prdb_sdk_optimizer_filter_v1 decoded_bytea_filter = {0};
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               bytea_filter_plan, bytea_filter_plan_len, &decoded_date_plan,
               sizeof(decoded_date_plan), &decoded_bytea_filter,
               sizeof(decoded_bytea_filter)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(decoded_bytea_filter.present == 1 &&
           decoded_bytea_filter.prefix_len == 25 &&
           decoded_bytea_filter.prefix[0] == 0x20 &&
           decoded_bytea_filter.prefix[19] == 0x07);
    uint32_t decimal_filter_plan_len =
        build_decimal_filter_plan(decimal_filter_plan);
    prdb_sdk_optimizer_filter_v1 decoded_decimal_filter = {0};
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               decimal_filter_plan, decimal_filter_plan_len, &decoded_date_plan,
               sizeof(decoded_date_plan), &decoded_decimal_filter,
               sizeof(decoded_decimal_filter)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(decoded_decimal_filter.present == 1 &&
           decoded_decimal_filter.prefix_len == 36 &&
           decoded_decimal_filter.prefix[0] == 0x20 &&
           decoded_decimal_filter.prefix[19] == 0x08);
    uint8_t sql_filtered[] = "SELECT c0 FROM items WHERE c0 = $1 LIMIT 1";
    uint8_t filter_parameter_bytes[4] = {0, 0, 0, 1};
    prdb_sdk_value_v1 filter_parameters[1] = {{
        sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
        filter_parameter_bytes, sizeof(filter_parameter_bytes)}};
    assert(prdb_sdk_sql_parse_select_v2(
               sql_filtered, sizeof(sql_filtered) - 1, &statement,
               sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    resolve.calls = 0;
    assert(prdb_sdk_optimizer_validate_select_plan_with_parameters_v1(
               &host, &statement, filter_parameters, 1,
               (const uint8_t *)"public", 6, filtered_plan,
               filtered_plan_len, response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.calls == 1);
    filter_parameter_bytes[3] = 2;
    assert(prdb_sdk_optimizer_validate_select_plan_with_parameters_v1(
               &host, &statement, filter_parameters, 1,
               (const uint8_t *)"public", 6, filtered_plan,
               filtered_plan_len, response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    filter_parameter_bytes[3] = 1;
    memcpy(probed_plan, filtered_plan, filtered_plan_len);
    uint32_t probed_plan_len = append_integer_point_hint(
        probed_plan, filtered_plan_len);
    resolve.probe_response[0] = 1;
    resolve.probe_response[1] = 2;
    memcpy(resolve.probe_response + 2, row_batch_bytes + 5,
           row_batch_len - 5);
    resolve.probe_response_len = 2 + row_batch_len - 5;
    memcpy(ranged_plan, filtered_plan, filtered_plan_len);
    ranged_plan[predicate_offset + 1] = 5; /* >= */
    uint32_t ranged_plan_len = append_integer_range_hint(
        ranged_plan, filtered_plan_len);
    memcpy(equality_ranged_plan, filtered_plan, filtered_plan_len);
    uint32_t equality_ranged_plan_len = append_integer_equality_range_hint(
        equality_ranged_plan, filtered_plan_len);
    prdb_sdk_optimizer_plan_v1 decoded_range_plan = {0};
    prdb_sdk_optimizer_filter_v1 decoded_range_filter = {0};
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               equality_ranged_plan, equality_ranged_plan_len,
               &decoded_range_plan, sizeof(decoded_range_plan),
               &decoded_range_filter, sizeof(decoded_range_filter)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(prdb_sdk_optimizer_validate_select_plan_with_parameters_v1(
               &host, &statement, filter_parameters, 1,
               (const uint8_t *)"public", 6, equality_ranged_plan,
               equality_ranged_plan_len, response, sizeof(response),
               &response_len) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               ranged_plan, ranged_plan_len, &decoded_range_plan,
               sizeof(decoded_range_plan), &decoded_range_filter,
               sizeof(decoded_range_filter)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    uint8_t malformed_range_plan[512];
    memcpy(malformed_range_plan, ranged_plan, ranged_plan_len);
    malformed_range_plan[filtered_plan_len + 43] = 3;
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               malformed_range_plan, ranged_plan_len, &decoded_range_plan,
               sizeof(decoded_range_plan), &decoded_range_filter,
               sizeof(decoded_range_filter)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    malformed_range_plan[filtered_plan_len + 43] = 1;
    malformed_range_plan[predicate_offset + 1] = 2; /* < contradicts >= */
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               malformed_range_plan, ranged_plan_len, &decoded_range_plan,
               sizeof(decoded_range_plan), &decoded_range_filter,
               sizeof(decoded_range_filter)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    uint8_t two_sided_range_plan[512];
    uint32_t two_sided_predicate_offset = 0;
    uint32_t two_sided_plan_len = build_two_sided_range_plan(
        two_sided_range_plan, &two_sided_predicate_offset);
    two_sided_plan_len = append_integer_two_sided_range_hint(
        two_sided_range_plan, two_sided_plan_len);
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               two_sided_range_plan, two_sided_plan_len,
               &decoded_range_plan, sizeof(decoded_range_plan),
               &decoded_range_filter, sizeof(decoded_range_filter)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    two_sided_range_plan[two_sided_predicate_offset + 40] = 11;
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               two_sided_range_plan, two_sided_plan_len,
               &decoded_range_plan, sizeof(decoded_range_plan),
               &decoded_range_filter, sizeof(decoded_range_filter)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    resolve.range_response[0] = 1;
    resolve.range_response[1] = 1;
    resolve.range_response[2] = 0;
    resolve.range_response[3] = 1;
    memcpy(resolve.range_response + 4, row_batch_bytes + 21,
           row_batch_len - 21);
    resolve.range_response_len = 4 + row_batch_len - 21;
    resolve.calls = 0;
    resolve.cancellation_check_calls = 0;
    projected_rows_seen = 0;
    total_rows = UINT32_MAX;
    assert(prdb_sdk_optimizer_execute_unfiltered_plan_v1(
               &host, ranged_plan, ranged_plan_len, 2,
               sizeof(batch_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL, &total_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(total_rows == 1 && projected_rows_seen == 1 &&
           resolve.calls == 4 && resolve.range_calls == 1);
    resolve.range_response[2] = 0;
    resolve.range_response[3] = 0;
    resolve.range_response_len = 4;
    resolve.calls = 0;
    resolve.cancellation_check_calls = 0;
    projected_rows_seen = 0;
    assert(prdb_sdk_optimizer_execute_unfiltered_plan_v1(
               &host, ranged_plan, ranged_plan_len, 2,
               sizeof(batch_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL, &total_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(total_rows == 0 && projected_rows_seen == 0 &&
           resolve.calls == 4 && resolve.range_calls == 2);
    resolve.range_response[1] = 0; /* unavailable requests a safe full scan */
    resolve.range_response_len = 2;
    memcpy(resolve.batch_response, row_batch_bytes, row_batch_len);
    resolve.batch_response_len = row_batch_len;
    resolve.calls = 0;
    resolve.cancellation_check_calls = 0;
    projected_rows_seen = 0;
    assert(prdb_sdk_optimizer_execute_unfiltered_plan_v1(
               &host, ranged_plan, ranged_plan_len, 2,
               sizeof(batch_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL, &total_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(total_rows == 1 && projected_rows_seen == 1 &&
           resolve.range_calls == 3 && resolve.calls == 6);
    resolve.range_response_too_small = 1;
    resolve.calls = 0;
    resolve.cancellation_check_calls = 0;
    projected_rows_seen = 0;
    assert(prdb_sdk_optimizer_execute_unfiltered_plan_v1(
               &host, ranged_plan, ranged_plan_len, 2,
               sizeof(batch_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL, &total_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(total_rows == 1 && projected_rows_seen == 1 &&
           resolve.range_calls == 4 && resolve.calls == 6);
    resolve.range_response_too_small = 0;
    resolve.range_response[0] = 1;
    resolve.range_response[1] = 1;
    resolve.range_response[2] = 0;
    resolve.range_response[3] = 1;
    memcpy(resolve.range_response + 4, row_batch_bytes + 21,
           row_batch_len - 21);
    resolve.range_response_len = 4 + row_batch_len - 21;
    resolve.calls = 0;
    resolve.cancellation_check_calls = 0;
    projected_rows_seen = 0;
    assert(prdb_sdk_optimizer_execute_unfiltered_plan_v1(
               &host, equality_ranged_plan, equality_ranged_plan_len, 2,
               sizeof(batch_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL, &total_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(total_rows == 1 && projected_rows_seen == 1 &&
           resolve.range_calls == 5 && resolve.calls == 4);
    resolve.batch_response_len = build_continuing_row_batch(
        resolve.batch_response);
    resolve.calls = 0;
    assert(prdb_sdk_optimizer_validate_select_plan_with_parameters_v1(
               &host, &statement, filter_parameters, 1,
               (const uint8_t *)"public", 6, probed_plan, probed_plan_len,
               response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.calls == 1);
    resolve.calls = 0;
    projected_rows_seen = 0;
    int32_t unique_probe_status = prdb_sdk_optimizer_execute_unfiltered_plan_v1(
               &host, probed_plan, probed_plan_len, 2,
               sizeof(batch_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL, &total_rows);
    assert(unique_probe_status == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(total_rows == 1 && projected_rows_seen == 1 &&
           resolve.calls == 4 && resolve.probe_calls == 1);
    resolve.probe_response[1] = 1;
    resolve.probe_response_len = 2;
    resolve.calls = 0;
    projected_rows_seen = 0;
    total_rows = UINT32_MAX;
    assert(prdb_sdk_optimizer_execute_unfiltered_plan_v1(
               &host, probed_plan, probed_plan_len, 2,
               sizeof(batch_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL, &total_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(total_rows == 0 && projected_rows_seen == 0 &&
           resolve.calls == 4 && resolve.probe_calls == 2);
    resolve.probe_response[1] = 0;
    resolve.calls = 0;
    projected_rows_seen = 0;
    assert(prdb_sdk_optimizer_execute_unfiltered_plan_v1(
               &host, probed_plan, probed_plan_len, 2,
               sizeof(batch_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL, &total_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(total_rows == 1 && projected_rows_seen == 1 &&
           resolve.calls == 7 && resolve.probe_calls == 3);
    resolve.probe_response[1] = 2;
    resolve.probe_response_len = sizeof(resolve.probe_response);
    resolve.calls = 0;
    projected_rows_seen = 0;
    assert(prdb_sdk_optimizer_execute_unfiltered_plan_v1(
               &host, probed_plan, probed_plan_len, 2,
               sizeof(batch_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL, &total_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(total_rows == 1 && projected_rows_seen == 1 &&
           resolve.calls == 7 && resolve.probe_calls == 4);
    resolve.probe_response_len = 2 + row_batch_len - 5;
    resolve.calls = 0;
    resolve.cancellation_check_calls = 0;
    resolve.cancel_on_check_call = 1;
    projected_rows_seen = 0;
    total_rows = UINT32_MAX;
    assert(prdb_sdk_optimizer_execute_unfiltered_plan_v1(
               &host, probed_plan, probed_plan_len, 2,
               sizeof(batch_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL, &total_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_CANCELLED);
    assert(total_rows == 0 && projected_rows_seen == 0 &&
           resolve.calls == 4 && resolve.probe_calls == 5);
    resolve.cancel_on_check_call = 0;
    resolve.cancellation_check_calls = 0;
    resolve.cancellation_response[2] = 0;
    assert(prdb_sdk_optimizer_validate_executor_relation_v1(
               &host, payload, payload_len, response, sizeof(response),
               &response_len) == PRDB_SDK_OPTIMIZER_PLAN_OK);

    resolve.response[34] = 2; /* A newer table epoch must reject this plan. */
    assert(prdb_sdk_optimizer_validate_executor_relation_v1(
               &host, payload, payload_len, response, sizeof(response),
               &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    resolve.response[34] = 1;
    resolve.response[150] = 4; /* Scanned column ID differs at ordinal 3. */
    assert(prdb_sdk_optimizer_validate_executor_relation_v1(
               &host, payload, payload_len, response, sizeof(response),
               &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    resolve.response[150] = 3;
    resolve.response[2] = 1;
    resolve.response_len = 3;
    assert(prdb_sdk_optimizer_validate_executor_relation_v1(
               &host, payload, payload_len, response, sizeof(response),
               &response_len) == PRDB_SDK_OPTIMIZER_PLAN_UNAVAILABLE);
    resolve.response[2] = 0;
    resolve.response_len = build_relation_response(resolve.response);
    assert(prdb_sdk_optimizer_validate_executor_relation_v1(
               &host, payload, payload_len, response, 3, &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL);

    memcpy(limit_one_plan, payload, payload_len);
    limit_one_plan[payload_len - 1] = 1;
    projected_rows_seen = 0;
    resolve.calls = 0;
    int32_t execute_status = prdb_sdk_optimizer_execute_unfiltered_plan_v1(
               &host, limit_one_plan, payload_len, 2, sizeof(batch_response), response,
               sizeof(response), scan_response, sizeof(scan_response),
               batch_response, sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL,
               &total_rows);
    assert(execute_status == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(total_rows == 1 && projected_rows_seen == 1 && resolve.calls == 6);

    assert(prdb_sdk_sql_parse_select_v2(
               sql, sizeof(sql) - 1, &statement, sizeof(statement), NULL) ==
           PRDB_SDK_SQL_OK);
    resolve.calls = 0;
    assert(prdb_sdk_optimizer_validate_select_plan_v1(
               &host, &statement, (const uint8_t *)"public", 6, limit_one_plan,
               payload_len, response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.calls == 1);
    uint8_t sql_mixed_case[] = "SELECT C3 FROM ITEMS LIMIT 1";
    assert(prdb_sdk_sql_parse_select_v2(
               sql_mixed_case, sizeof(sql_mixed_case) - 1, &statement,
               sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_optimizer_validate_select_plan_v1(
               &host, &statement, (const uint8_t *)"public", 6, limit_one_plan,
               payload_len, response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.calls == 2);
    uint8_t sql_wrong_projection[] = "SELECT c2 FROM items LIMIT 1";
    assert(prdb_sdk_sql_parse_select_v2(
               sql_wrong_projection, sizeof(sql_wrong_projection) - 1,
               &statement, sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    uint32_t calls_before_projection_mismatch = resolve.calls;
    assert(prdb_sdk_optimizer_validate_select_plan_v1(
               &host, &statement, (const uint8_t *)"public", 6, limit_one_plan,
               payload_len, response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    assert(resolve.calls == calls_before_projection_mismatch + 1);
    uint8_t sql_wrong_limit[] = "SELECT c3 FROM items LIMIT 2";
    assert(prdb_sdk_sql_parse_select_v2(
               sql_wrong_limit, sizeof(sql_wrong_limit) - 1, &statement,
               sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    calls_before_projection_mismatch = resolve.calls;
    assert(prdb_sdk_optimizer_validate_select_plan_v1(
               &host, &statement, (const uint8_t *)"public", 6, limit_one_plan,
               payload_len, response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    assert(resolve.calls == calls_before_projection_mismatch);
    assert(prdb_sdk_sql_parse_select_v2(
               sql, sizeof(sql) - 1, &statement, sizeof(statement), NULL) ==
           PRDB_SDK_SQL_OK);
    resolve.optimizer_response_len = build_optimizer_response(
        resolve.optimizer_response, limit_one_plan, payload_len);
    host_v4.v3.struct_size = sizeof(host_v4.v3);
    host_v4.v3.abi_version = PRDB_QUERY_ABI_V3_VERSION;
    host_v4.v3.context = &resolve;
    host_v4.v3.executor_request = resolve_plan_relation;
    host_v4.v3.write_result = write_result;
    host_v4.v3.statement_failure = statement_failure;
    host_v4.optimizer_request = optimizer_request;

    {
        static const uint8_t ordered_limit_zero_sql[] =
            "SELECT c0 FROM items ORDER BY c3 DESC LIMIT 0";
        uint32_t ordered_limit_zero_plan_len =
            build_ordered_c0_c3_plan(ordered_select_malformed, 0);
        uint32_t ordered_limit_zero_delivered = 99;
        uint32_t rows_before = projected_rows_seen;
        assert(prdb_sdk_sql_parse_select_v3(
                   ordered_limit_zero_sql,
                   sizeof(ordered_limit_zero_sql) - 1,
                   &ordered_select_statement,
                   sizeof(ordered_select_statement), NULL) ==
               PRDB_SDK_SQL_OK);
        resolve.optimizer_response_len = build_optimizer_response(
            resolve.optimizer_response, ordered_select_malformed,
            ordered_limit_zero_plan_len);
        resolve.optimizer_calls = 0;
        resolve.calls = 0;
        resolve.event_count = 0;
        resolve.statement_failures = 0;
        assert(prdb_sdk_optimizer_execute_ordered_select_ast_v1(
                   &host_v4, &ordered_select_statement,
                   (const uint8_t *)"public", 6, 2,
                   sizeof(batch_response), request, sizeof(request),
                   optimizer_response, sizeof(optimizer_response), response,
                   sizeof(response), scan_response, sizeof(scan_response),
                   batch_response, sizeof(batch_response), &acquired_relation,
                   sizeof(acquired_relation), acquired_columns, 4,
                   source_cells, 4, projected_values, 1, malformed,
                   sizeof(malformed), on_projected_row, NULL,
                   &ordered_limit_zero_delivered) ==
               PRDB_SDK_OPTIMIZER_PLAN_OK);
        assert(ordered_limit_zero_delivered == 0 &&
               projected_rows_seen == rows_before && resolve.calls == 2 &&
               resolve.optimizer_calls == 1 &&
               resolve.statement_failures == 0);
        assert(resolve.event_count == 3 &&
               resolve.events[0] == UINT16_C(0xffff) &&
               resolve.events[1] == PRDB_IPC_OP_RESOLVE_PLAN_RELATION &&
               resolve.events[2] == PRDB_SDK_EXECUTOR_OP_RESOLVE_RELATION);
        resolve.optimizer_response_len = build_optimizer_response(
            resolve.optimizer_response, limit_one_plan, payload_len);
    }

    {
        static const uint8_t ordered_materialization_sql[] =
            "SELECT c0 FROM items ORDER BY c3 DESC LIMIT 1";
        uint32_t ordered_plan_len =
            build_ordered_c0_c3_plan(ordered_select_malformed, 1);
        uint32_t ordered_delivered = 99;
        uint32_t rows_before = projected_rows_seen;
        uint32_t saw_ordered_open = 0;
        uint32_t saw_ordered_batch = 0;
        assert(prdb_sdk_sql_parse_select_v3(
                   ordered_materialization_sql,
                   sizeof(ordered_materialization_sql) - 1,
                   &ordered_select_statement,
                   sizeof(ordered_select_statement), NULL) ==
               PRDB_SDK_SQL_OK);
        resolve.optimizer_response_len = build_optimizer_response(
            resolve.optimizer_response, ordered_select_malformed,
            ordered_plan_len);
        resolve.batch_response_len =
            build_row_batch(resolve.batch_response);
        resolve.optimizer_calls = 0;
        resolve.calls = 0;
        resolve.event_count = 0;
        resolve.statement_failures = 0;
        resolve.cancellation_response[2] = 0;
        int32_t ordered_materialization_status =
            prdb_sdk_optimizer_execute_ordered_select_ast_v1(
                   &host_v4, &ordered_select_statement,
                   (const uint8_t *)"public", 6, 2,
                   sizeof(batch_response), request, sizeof(request),
                   optimizer_response, sizeof(optimizer_response), response,
                   sizeof(response), scan_response, sizeof(scan_response),
                   batch_response, sizeof(batch_response), &acquired_relation,
                   sizeof(acquired_relation), acquired_columns, 4,
                   source_cells, 4, projected_values, 1, malformed, 8,
                   on_projected_row, NULL, &ordered_delivered);
        assert(ordered_materialization_status ==
               PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL);
        assert(ordered_delivered == 0 && projected_rows_seen == rows_before &&
               resolve.optimizer_calls == 1 && resolve.statement_failures == 1);
        for (uint32_t event = 0; event < resolve.event_count; ++event) {
            if (resolve.events[event] == PRDB_SDK_EXECUTOR_OP_OPEN_SCAN)
                saw_ordered_open = 1;
            if (resolve.events[event] == PRDB_SDK_EXECUTOR_OP_NEXT_BATCH)
                saw_ordered_batch = 1;
        }
        assert(saw_ordered_open != 0 && saw_ordered_batch != 0);
        resolve.optimizer_response_len = build_optimizer_response(
            resolve.optimizer_response, limit_one_plan, payload_len);
        resolve.batch_response_len = build_continuing_row_batch(
            resolve.batch_response);
        resolve.statement_failures = 0;
    }

    {
        static const uint8_t ordered_cancel_sql[] =
            "SELECT c0 FROM items ORDER BY c3 DESC LIMIT 33";
        static uint8_t ordered_materialized[
            PRDB_SDK_SQL_EXEC_V3_MAX_MATERIALIZED_BYTES];
        uint8_t ordered_executor_batch[4096];
        uint32_t ordered_plan_len =
            build_ordered_c0_c3_plan(ordered_select_malformed, 33);
        uint32_t ordered_batch_len =
            build_repeated_row_batch(ordered_executor_batch, 33);
        uint32_t ordered_delivered = 99;
        assert(prdb_sdk_sql_parse_select_v3(
                   ordered_cancel_sql, sizeof(ordered_cancel_sql) - 1,
                   &ordered_select_statement,
                   sizeof(ordered_select_statement), NULL) ==
               PRDB_SDK_SQL_OK);
        resolve.optimizer_response_len = build_optimizer_response(
            resolve.optimizer_response, ordered_select_malformed,
            ordered_plan_len);
        resolve.batch_response_len = ordered_batch_len;
        memcpy(resolve.batch_response, ordered_executor_batch,
               ordered_batch_len);
        resolve.optimizer_calls = 0;
        resolve.calls = 0;
        resolve.event_count = 0;
        resolve.statement_failures = 0;
        resolve.cancellation_check_calls = 0;
        resolve.cancel_on_check_call = 4;
        resolve.cancellation_response[2] = 0;
        projected_rows_seen = 0;
        assert(prdb_sdk_optimizer_execute_ordered_select_ast_v1(
                   &host_v4, &ordered_select_statement,
                   (const uint8_t *)"public", 6, 33,
                   sizeof(ordered_executor_batch), request, sizeof(request),
                   optimizer_response, sizeof(optimizer_response), response,
                   sizeof(response), scan_response, sizeof(scan_response),
                   ordered_executor_batch, sizeof(ordered_executor_batch),
                   &acquired_relation, sizeof(acquired_relation),
                   acquired_columns, 4, source_cells, 4, projected_values, 1,
                   ordered_materialized, sizeof(ordered_materialized),
                   on_projected_row, NULL, &ordered_delivered) ==
               PRDB_SDK_OPTIMIZER_PLAN_CANCELLED);
        assert(ordered_delivered == 32 && projected_rows_seen == 32 &&
               resolve.cancellation_check_calls == 4 &&
               resolve.statement_failures == 1);
        resolve.optimizer_response_len = build_optimizer_response(
            resolve.optimizer_response, limit_one_plan, payload_len);
        resolve.batch_response_len = build_continuing_row_batch(
            resolve.batch_response);
        resolve.cancel_on_check_call = 0;
        resolve.cancellation_check_calls = 0;
        resolve.cancellation_response[2] = 0;
        resolve.statement_failures = 0;
    }

    resolve.optimizer_calls = 0;
    resolve.calls = 0;
    resolve.event_count = 0;
    resolve.statement_failures = 0;
    projected_rows_seen = 0;
    assert(prdb_sdk_optimizer_execute_select_ast_v1(
               &host_v4, &statement, (const uint8_t *)"public", 6, 2,
               sizeof(batch_response), request, sizeof(request),
               optimizer_response, sizeof(optimizer_response), response,
               sizeof(response), scan_response, sizeof(scan_response),
               batch_response, sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL,
               &total_rows) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.optimizer_calls == 1 && resolve.calls == 7);
    assert(resolve.statement_failures == 0 && total_rows == 1 &&
           projected_rows_seen == 1);
    assert(resolve.event_count == 8 && resolve.events[0] == UINT16_C(0xffff));
    assert(resolve.events[1] == PRDB_IPC_OP_RESOLVE_PLAN_RELATION);
    assert(resolve.events[2] == PRDB_IPC_OP_RESOLVE_PLAN_RELATION);
    assert(resolve.events[3] == PRDB_SDK_EXECUTOR_OP_RESOLVE_RELATION);
    assert(resolve.events[4] == PRDB_SDK_EXECUTOR_OP_OPEN_SCAN);
    assert(resolve.events[5] == PRDB_SDK_EXECUTOR_OP_CHECK_CANCELLATION);
    assert(resolve.events[6] == PRDB_SDK_EXECUTOR_OP_NEXT_BATCH);
    assert(resolve.events[7] == PRDB_SDK_EXECUTOR_OP_CLOSE_CURSOR);

    resolve.calls = 0;
    resolve.event_count = 0;
    resolve.emitted_result_len = 0;
    resolve.result_write_calls = 0;
    assert(prdb_sdk_optimizer_execute_select_ast_to_result_v1(
               &host_v4, &statement, (const uint8_t *)"public", 6, 2,
               sizeof(batch_response), request, sizeof(request),
               optimizer_response, sizeof(optimizer_response), response,
               sizeof(response), scan_response, sizeof(scan_response),
               batch_response, sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, result_record_buffer,
               sizeof(result_record_buffer), &total_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(total_rows == 1 && resolve.emitted_result_len > 8);
    assert(memcmp(resolve.emitted_result, "SQLR", 4) == 0);
    assert(resolve.emitted_result[4] == 0 && resolve.emitted_result[5] == 1);
    assert(resolve.emitted_result[6] == 0 && resolve.emitted_result[7] == 1);
    assert(resolve.emitted_result[8] == 0 && resolve.emitted_result[9] == 2);
    assert(memcmp(resolve.emitted_result + 10, "c3", 2) == 0);
    assert(resolve.emitted_result[12] == 0 && resolve.emitted_result[13] == 0 &&
           resolve.emitted_result[14] == 0 &&
           resolve.emitted_result[15] == PRDB_SDK_VALUE_TYPE_TEXT);
    assert(resolve.emitted_result[18] == PRDB_SDK_VALUE_STATE_VALUE);
    assert(resolve.emitted_result[19] == 0 && resolve.emitted_result[20] == 0 &&
           resolve.emitted_result[21] == 0 && resolve.emitted_result[22] == 1);
    assert(resolve.emitted_result[23] == 'x');
    resolve.emitted_result_len = 0;
    resolve.statement_failures = 0;
    resolve.result_write_calls = 0;
    resolve.fail_result_write = 1;
    int32_t result_failure_status = prdb_sdk_optimizer_execute_select_ast_to_result_v1(
               &host_v4, &statement, (const uint8_t *)"public", 6, 2,
               sizeof(batch_response), request, sizeof(request),
               optimizer_response, sizeof(optimizer_response), response,
               sizeof(response), scan_response, sizeof(scan_response),
               batch_response, sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, result_record_buffer,
               sizeof(result_record_buffer), &total_rows);
    assert(result_failure_status ==
           PRDB_SDK_OPTIMIZER_PLAN_ROW_CALLBACK_ERROR);
    assert(resolve.result_write_calls == 1);
    assert(resolve.statement_failures == 1);
    resolve.fail_result_write = 0;
    resolve.statement_failures = 0;

    uint8_t sql_with_filter[] = "SELECT c3 FROM items WHERE c3 = $1";
    assert(prdb_sdk_sql_parse_select_v2(
               sql_with_filter, sizeof(sql_with_filter) - 1, &statement,
               sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    uint32_t optimizer_calls = resolve.optimizer_calls;
    assert(prdb_sdk_optimizer_execute_select_ast_v1(
               &host_v4, &statement, (const uint8_t *)"public", 6, 2,
               sizeof(batch_response), request, sizeof(request),
               optimizer_response, sizeof(optimizer_response), response,
               sizeof(response), scan_response, sizeof(scan_response),
               batch_response, sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL,
               &total_rows) == PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED);
    assert(resolve.optimizer_calls == optimizer_calls &&
           resolve.statement_failures == 0);

    assert(prdb_sdk_sql_parse_select_v2(
               sql_filtered, sizeof(sql_filtered) - 1, &statement,
               sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    resolve.optimizer_response_len = build_optimizer_response(
        resolve.optimizer_response, filtered_plan, filtered_plan_len);
    resolve.optimizer_calls = 0;
    resolve.calls = 0;
    resolve.event_count = 0;
    projected_rows_seen = 0;
    assert(prdb_sdk_optimizer_execute_select_ast_with_parameters_v1(
               &host_v4, &statement, filter_parameters, 1,
               (const uint8_t *)"public", 6, 2, sizeof(batch_response),
               request, sizeof(request), optimizer_response,
               sizeof(optimizer_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL,
               &total_rows) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.optimizer_calls == 1 && resolve.calls == 7 &&
           total_rows == 1 && projected_rows_seen == 1 &&
           resolve.statement_failures == 0);
    resolve.emitted_result_len = 0;
    resolve.result_write_calls = 0;
    assert(prdb_sdk_optimizer_execute_select_ast_with_parameters_to_result_v1(
               &host_v4, &statement, filter_parameters, 1,
               (const uint8_t *)"public", 6, 2, sizeof(batch_response),
               request, sizeof(request), optimizer_response,
               sizeof(optimizer_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, result_record_buffer,
               sizeof(result_record_buffer), &total_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(total_rows == 1 && resolve.result_write_calls == 1 &&
           resolve.emitted_result_len == 27);
    assert(memcmp(resolve.emitted_result, "SQLR", 4) == 0);
    assert(resolve.emitted_result[8] == 0 && resolve.emitted_result[9] == 2 &&
           memcmp(resolve.emitted_result + 10, "c0", 2) == 0);
    assert(resolve.emitted_result[15] == PRDB_SDK_VALUE_TYPE_INTEGER &&
           resolve.emitted_result[23] == 0 && resolve.emitted_result[24] == 0 &&
           resolve.emitted_result[25] == 0 && resolve.emitted_result[26] == 1);

    uint8_t boolean_filtered_plan[512];
    uint32_t boolean_predicate_offset;
    uint32_t boolean_filtered_plan_len = build_boolean_filter_plan(
        boolean_filtered_plan, &boolean_predicate_offset);
    (void)boolean_predicate_offset;
    uint8_t sql_boolean_filtered[] =
        "SELECT c0 FROM items WHERE c0 = $1 AND c0 = $1 LIMIT 1";
    assert(prdb_sdk_sql_parse_select_v2(
               sql_boolean_filtered, sizeof(sql_boolean_filtered) - 1,
               &statement, sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    resolve.optimizer_response_len = build_optimizer_response(
        resolve.optimizer_response, boolean_filtered_plan,
        boolean_filtered_plan_len);
    resolve.optimizer_calls = 0;
    resolve.calls = 0;
    resolve.event_count = 0;
    projected_rows_seen = 0;
    assert(prdb_sdk_optimizer_execute_select_ast_with_parameters_v1(
               &host_v4, &statement, filter_parameters, 1,
               (const uint8_t *)"public", 6, 2, sizeof(batch_response),
               request, sizeof(request), optimizer_response,
               sizeof(optimizer_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL,
               &total_rows) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.optimizer_calls == 1 && total_rows == 1 &&
           projected_rows_seen == 1 && resolve.statement_failures == 0);

    assert(prdb_sdk_sql_parse_select_v2(
               sql, sizeof(sql) - 1, &statement, sizeof(statement), NULL) ==
           PRDB_SDK_SQL_OK);
    resolve.optimizer_response[2] = PRDB_SDK_OPTIMIZER_STATUS_INTERNAL;
    resolve.optimizer_response_len = 3;
    assert(prdb_sdk_optimizer_execute_select_ast_v1(
               &host_v4, &statement, (const uint8_t *)"public", 6, 2,
               sizeof(batch_response), request, sizeof(request),
               optimizer_response, sizeof(optimizer_response), response,
               sizeof(response), scan_response, sizeof(scan_response),
               batch_response, sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, fail_projected_row, NULL,
               &total_rows) == PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR);
    assert(resolve.statement_failures == 1);
    resolve.cancellation_response[2] = 1;
    resolve.calls = 0;
    assert(prdb_sdk_optimizer_execute_unfiltered_plan_v1(
               &host, limit_one_plan, payload_len, 2, sizeof(batch_response),
               response, sizeof(response), scan_response, sizeof(scan_response),
               batch_response, sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL,
               &total_rows) == PRDB_SDK_OPTIMIZER_PLAN_CANCELLED);
    assert(total_rows == 0 && resolve.calls == 5);
    resolve.cancellation_response[2] = 0;

    memcpy(malformed, payload, payload_len);
    malformed[projected_id_offset] = 9;
    assert(prdb_sdk_optimizer_decode_plan_v1(
               malformed, payload_len, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    memcpy(malformed, payload, payload_len);
    malformed[project_node_offset] = 2;
    assert(prdb_sdk_optimizer_decode_plan_v1(
               malformed, payload_len, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED);

    memcpy(malformed, payload, payload_len);
    malformed[payload_len] = 0;
    assert(prdb_sdk_optimizer_decode_plan_v1(
               malformed, payload_len + 1, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    payload_len = build_filter_plan(payload, &predicate_offset);
    assert(prdb_sdk_optimizer_decode_plan_v1(
               payload, payload_len, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED);
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               payload, payload_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(filter.present == 1 && filter.prefix == payload + predicate_offset);
    assert(filter.prefix_len == 28);
    assert(decoded.has_limit == 1 && decoded.limit == 1);

    payload_len = build_delete_plan(payload, &predicate_offset);
    assert(prdb_sdk_optimizer_decode_delete_plan_v1(
               payload, payload_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(decoded.plan_kind == 2 && decoded.scan_column_count == 0 &&
           decoded.projection_count == 0 && decoded.has_limit == 0);
    assert(filter.present == 1 && filter.prefix == payload + predicate_offset &&
           filter.prefix_len == 2);
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               payload, payload_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    payload[payload_len - 1] = 4; /* SELECT Project instead of Delete */
    assert(prdb_sdk_optimizer_decode_delete_plan_v1(
               payload, payload_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    payload[payload_len - 1] = 5;
    payload[predicate_offset + 1] = 2; /* malformed Boolean */
    assert(prdb_sdk_optimizer_decode_delete_plan_v1(
               payload, payload_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    uint8_t update_plan[512];
    uint32_t update_plan_len = build_update_plan(update_plan);
    assert(prdb_sdk_optimizer_decode_update_plan_v1(
               update_plan, update_plan_len, &update_decoded,
               sizeof(update_decoded)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(update_decoded.plan.plan_kind == 3 &&
           update_decoded.plan.scan_column_count == 1 &&
           update_decoded.filter.present == 1 &&
           update_decoded.assignment_count == 1);
    assert(update_decoded.assignments[0].column.ordinal == 0 &&
           update_decoded.assignments[0].column.type_code == 3 &&
           update_decoded.assignments[0].value_kind ==
               PRDB_OPTIMIZER_UPDATE_PARAMETER_VALUE &&
           update_decoded.assignments[0].parameter_index == 2);

    uint32_t join_projection_side_offset;
    uint32_t join_right_key_type_offset;
    uint32_t join_payload_len = build_join_plan(
        join_payload, &join_projection_side_offset,
        &join_right_key_type_offset);
    assert(prdb_sdk_optimizer_decode_join_plan_v1(
               join_payload, join_payload_len, &join_decoded,
               sizeof(join_decoded)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(join_decoded.left_relation.relation_name_len == 5 &&
           memcmp(join_decoded.left_relation.relation_name, "lefts", 5) == 0);
    assert(join_decoded.right_relation.relation_name_len == 6 &&
           memcmp(join_decoded.right_relation.relation_name, "rights", 6) == 0);
    assert(join_decoded.left_relation.scan_column_count == 3 &&
           join_decoded.right_relation.scan_column_count == 3 &&
           join_decoded.projection_count == 2 &&
           join_decoded.projections[0].relation_side == 0 &&
           join_decoded.projections[1].relation_side == 1 &&
           join_decoded.join_key_count == 2 && join_decoded.has_limit == 1 &&
           join_decoded.limit == 5);

    uint32_t filtered_side_offset;
    uint32_t filtered_column_offset;
    uint32_t filtered_index_offset;
    uint32_t filtered_join_payload_len = build_filtered_join_plan(
        filtered_join_payload, &filtered_side_offset, &filtered_column_offset,
        &filtered_index_offset);
    assert(prdb_sdk_optimizer_decode_filtered_join_plan_v1(
               filtered_join_payload, filtered_join_payload_len,
               &filtered_join_decoded, sizeof(filtered_join_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(filtered_join_decoded.join.right_relation.scan_column_count == 3 &&
           filtered_join_decoded.filter.relation_side == 1 &&
           filtered_join_decoded.filter.column.ordinal == 1 &&
           filtered_join_decoded.filter.column.type_code == 1 &&
           filtered_join_decoded.filter.expression_kind == 0 &&
           filtered_join_decoded.filter.comparison == 4 &&
           filtered_join_decoded.filter.parameter_index == 3);
    assert(prdb_sdk_optimizer_decode_join_plan_v1(
               filtered_join_payload, filtered_join_payload_len,
               &join_decoded, sizeof(join_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    memcpy(filtered_join_malformed, filtered_join_payload,
           filtered_join_payload_len);
    filtered_join_malformed[filtered_side_offset] = 2;
    assert(prdb_sdk_optimizer_decode_filtered_join_plan_v1(
               filtered_join_malformed, filtered_join_payload_len,
               &filtered_join_decoded, sizeof(filtered_join_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    memcpy(filtered_join_malformed, filtered_join_payload,
           filtered_join_payload_len);
    filtered_join_malformed[filtered_column_offset] = 0x7f;
    assert(prdb_sdk_optimizer_decode_filtered_join_plan_v1(
               filtered_join_malformed, filtered_join_payload_len,
               &filtered_join_decoded, sizeof(filtered_join_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    memcpy(filtered_join_malformed, filtered_join_payload,
           filtered_join_payload_len);
    filtered_join_malformed[filtered_index_offset + 3] = 0;
    assert(prdb_sdk_optimizer_decode_filtered_join_plan_v1(
               filtered_join_malformed, filtered_join_payload_len,
               &filtered_join_decoded, sizeof(filtered_join_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    assert(prdb_sdk_optimizer_decode_filtered_join_plan_v1(
               filtered_join_payload, filtered_join_payload_len + 1,
               &filtered_join_decoded, sizeof(filtered_join_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    uint32_t group_key_id_offset;
    uint32_t group_count_payload_len = build_group_count_plan(
        group_count_payload, &group_key_id_offset);
    assert(prdb_sdk_optimizer_decode_group_count_plan_v1(
               group_count_payload, group_count_payload_len,
               &group_count_decoded, sizeof(group_count_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(group_count_decoded.plan.plan_kind == 5 &&
           group_count_decoded.plan.scan_column_count == 1 &&
           group_count_decoded.group_key.column_id[0] == 4 &&
           group_count_decoded.plan.has_limit == 1 &&
           group_count_decoded.plan.limit == 5);
    static const uint8_t group_count_sql[] =
        "SELECT id, COUNT(*) FROM items GROUP BY id LIMIT 5";
    assert(prdb_sdk_sql_parse_select_v4(
               group_count_sql, sizeof(group_count_sql) - 1,
               &group_count_statement, sizeof(group_count_statement), NULL) ==
           PRDB_SDK_SQL_OK);
    group_count_binding.struct_size = sizeof(group_count_binding);
    group_count_binding.abi_version = PRDB_SDK_SQL_ABI_V4_VERSION;
    group_count_binding.base.struct_size = sizeof(group_count_binding.base);
    group_count_binding.base.abi_version = PRDB_SDK_SQL_ABI_V2_VERSION;
    group_count_binding.base.projection_count = 1;
    group_count_binding.base.projection_ordinals[0] = 0;
    group_count_binding.has_group_by = 1;
    group_count_binding.aggregate_kind = PRDB_SDK_SQL_AGGREGATE_COUNT_STAR;
    group_count_binding.group_by_column.ordinal = 0;
    group_count_binding.group_by_column.type_code = 1;
    assert(prdb_sdk_optimizer_match_group_count_plan_v1(
               &group_count_statement, &group_count_binding,
               (const uint8_t *)"public", 6, group_count_payload,
               group_count_payload_len) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    group_count_statement.base.limit = 4;
    assert(prdb_sdk_optimizer_match_group_count_plan_v1(
               &group_count_statement, &group_count_binding,
               (const uint8_t *)"public", 6, group_count_payload,
               group_count_payload_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    group_count_statement.base.limit = 5;
    memcpy(group_count_malformed, group_count_payload,
           group_count_payload_len);
    group_count_malformed[group_key_id_offset] = 5;
    assert(prdb_sdk_optimizer_decode_group_count_plan_v1(
               group_count_malformed, group_count_payload_len,
               &group_count_decoded, sizeof(group_count_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    uint32_t ordered_group_count_payload_len =
        build_ordered_group_count_plan(ordered_group_count_payload);
    assert(prdb_sdk_optimizer_decode_ordered_group_count_plan_v1(
               ordered_group_count_payload, ordered_group_count_payload_len,
               &ordered_group_count_decoded,
               sizeof(ordered_group_count_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(ordered_group_count_decoded.plan.plan_kind == 8 &&
           ordered_group_count_decoded.plan.has_limit == 1 &&
           ordered_group_count_decoded.plan.limit == 5 &&
           ordered_group_count_decoded.order_count == 2 &&
           ordered_group_count_decoded.order_items[0].source == 1 &&
           ordered_group_count_decoded.order_items[0].descending == 1 &&
           ordered_group_count_decoded.order_items[1].source == 0 &&
           ordered_group_count_decoded.order_items[1].nulls_first == 1);
    static const uint8_t ordered_group_count_sql[] =
        "SELECT id, COUNT(*) FROM items GROUP BY id "
        "ORDER BY COUNT(*) DESC NULLS LAST, id ASC NULLS FIRST LIMIT 5";
    ordered_group_count_columns[0].name = (const uint8_t *)"id";
    ordered_group_count_columns[0].name_len = 2;
    ordered_group_count_columns[0].ordinal = 0;
    ordered_group_count_columns[0].type_code = PRDB_SDK_VALUE_TYPE_INTEGER;
    assert(prdb_sdk_sql_parse_select_v12(
               ordered_group_count_sql, sizeof(ordered_group_count_sql) - 1,
               &ordered_group_count_statement,
               sizeof(ordered_group_count_statement), NULL) ==
           PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_select_v12(
               &ordered_group_count_statement, (const uint8_t *)"items", 5,
               ordered_group_count_columns, 1,
               &ordered_group_count_binding,
               sizeof(ordered_group_count_binding)) == PRDB_SDK_SQL_BIND_OK);
    int32_t ordered_match_status =
        prdb_sdk_optimizer_match_ordered_group_count_plan_v1(
               &ordered_group_count_statement, &ordered_group_count_binding,
               (const uint8_t *)"public", 6, ordered_group_count_payload,
               ordered_group_count_payload_len);
    assert(ordered_match_status == PRDB_SDK_OPTIMIZER_PLAN_OK);
    ordered_group_count_statement.order_items[0].direction =
        PRDB_SDK_SQL_V10_ORDER_ASCENDING;
    assert(prdb_sdk_optimizer_match_ordered_group_count_plan_v1(
               &ordered_group_count_statement, &ordered_group_count_binding,
               (const uint8_t *)"public", 6, ordered_group_count_payload,
               ordered_group_count_payload_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);

    uint32_t composite_group_key_offset;
    uint32_t composite_group_count_payload_len =
        build_composite_group_count_plan(composite_group_count_payload,
                                         &composite_group_key_offset);
    assert(prdb_sdk_optimizer_decode_composite_group_count_plan_v1(
               composite_group_count_payload,
               composite_group_count_payload_len,
               &composite_group_count_decoded,
               sizeof(composite_group_count_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(composite_group_count_decoded.plan.plan_kind == 6 &&
           composite_group_count_decoded.plan.scan_column_count == 2 &&
           composite_group_count_decoded.group_key_count == 2 &&
           composite_group_count_decoded.group_keys[0].ordinal == 1 &&
           composite_group_count_decoded.group_keys[1].ordinal == 0 &&
           composite_group_count_decoded.plan.has_limit == 1 &&
           composite_group_count_decoded.plan.limit == 5);
    static const uint8_t composite_group_count_sql[] =
        "SELECT active, id, COUNT(*) FROM items GROUP BY active, id LIMIT 5";
    assert(prdb_sdk_sql_parse_select_v13(
               composite_group_count_sql,
               sizeof(composite_group_count_sql) - 1,
               &composite_group_count_statement,
               sizeof(composite_group_count_statement), NULL) ==
           PRDB_SDK_SQL_OK);
    composite_group_count_binding.struct_size =
        sizeof(composite_group_count_binding);
    composite_group_count_binding.abi_version = PRDB_SDK_SQL_ABI_V13_VERSION;
    composite_group_count_binding.base.struct_size =
        sizeof(composite_group_count_binding.base);
    composite_group_count_binding.base.abi_version =
        PRDB_SDK_SQL_ABI_V7_VERSION;
    composite_group_count_binding.base.base.struct_size =
        sizeof(composite_group_count_binding.base.base);
    composite_group_count_binding.base.base.abi_version =
        PRDB_SDK_SQL_ABI_V2_VERSION;
    composite_group_count_binding.group_key_count = 2;
    composite_group_count_binding.group_keys[0].ordinal = 1;
    composite_group_count_binding.group_keys[0].type_code = 1;
    composite_group_count_binding.group_keys[1].ordinal = 0;
    composite_group_count_binding.group_keys[1].type_code = 1;
    assert(prdb_sdk_optimizer_match_composite_group_count_plan_v1(
               &composite_group_count_statement,
               &composite_group_count_binding,
               (const uint8_t *)"public", 6,
               composite_group_count_payload,
               composite_group_count_payload_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    composite_group_count_statement.base.base.limit = 4;
    assert(prdb_sdk_optimizer_match_composite_group_count_plan_v1(
               &composite_group_count_statement,
               &composite_group_count_binding,
               (const uint8_t *)"public", 6,
               composite_group_count_payload,
               composite_group_count_payload_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    composite_group_count_statement.base.base.limit = 5;
    memcpy(composite_group_count_malformed, composite_group_count_payload,
           composite_group_count_payload_len);
    composite_group_count_malformed[composite_group_key_offset] = 6;
    assert(prdb_sdk_optimizer_decode_composite_group_count_plan_v1(
               composite_group_count_malformed,
               composite_group_count_payload_len,
               &composite_group_count_decoded,
               sizeof(composite_group_count_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    composite_group_aggregate_columns[0] =
        (prdb_sdk_sql_column_shape_v2){
            (const uint8_t *)"id", 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0};
    composite_group_aggregate_columns[1] =
        (prdb_sdk_sql_column_shape_v2){
            (const uint8_t *)"active", 6, 1, PRDB_SDK_VALUE_TYPE_BOOLEAN, 0, 0};
    composite_group_aggregate_columns[2] =
        (prdb_sdk_sql_column_shape_v2){
            (const uint8_t *)"amount", 6, 2, PRDB_SDK_VALUE_TYPE_BIGINT, 0, 0};
    static const uint8_t *const composite_group_aggregate_sql[] = {
        (const uint8_t *)"SELECT active, id, COUNT(*) FROM items "
                         "GROUP BY active, id LIMIT 5",
        (const uint8_t *)"SELECT active, id, MIN(amount) FROM items "
                         "GROUP BY active, id LIMIT 5",
        (const uint8_t *)"SELECT active, id, MAX(amount) FROM items "
                         "GROUP BY active, id LIMIT 5",
        (const uint8_t *)"SELECT active, id, SUM(amount) FROM items "
                         "GROUP BY active, id LIMIT 5",
        (const uint8_t *)"SELECT active, id, AVG(amount) FROM items "
                         "GROUP BY active, id LIMIT 5",
    };
    static const uint32_t composite_group_aggregate_sql_len[] = {
        sizeof("SELECT active, id, COUNT(*) FROM items "
               "GROUP BY active, id LIMIT 5") - 1,
        sizeof("SELECT active, id, MIN(amount) FROM items "
               "GROUP BY active, id LIMIT 5") - 1,
        sizeof("SELECT active, id, MAX(amount) FROM items "
               "GROUP BY active, id LIMIT 5") - 1,
        sizeof("SELECT active, id, SUM(amount) FROM items "
               "GROUP BY active, id LIMIT 5") - 1,
        sizeof("SELECT active, id, AVG(amount) FROM items "
               "GROUP BY active, id LIMIT 5") - 1,
    };
    for (uint8_t aggregate_kind = PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR;
         aggregate_kind <= PRDB_SDK_SQL_V7_AGGREGATE_AVG; ++aggregate_kind) {
        uint32_t composite_group_aggregate_payload_len =
            build_composite_group_aggregate_plan(
                composite_group_aggregate_payload, aggregate_kind);
        assert(prdb_sdk_optimizer_decode_composite_group_aggregate_plan_v1(
                   composite_group_aggregate_payload,
                   composite_group_aggregate_payload_len,
                   &composite_group_aggregate_decoded,
                   sizeof(composite_group_aggregate_decoded)) ==
               PRDB_SDK_OPTIMIZER_PLAN_OK);
        assert(composite_group_aggregate_decoded.plan.plan_kind == 13 &&
               composite_group_aggregate_decoded.group_key_count == 2 &&
               composite_group_aggregate_decoded.aggregate_kind ==
                   aggregate_kind &&
               composite_group_aggregate_decoded.plan.scan_column_count ==
                   (aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR
                        ? 2
                        : 3) &&
               composite_group_aggregate_decoded.group_keys[0].ordinal == 1 &&
               composite_group_aggregate_decoded.group_keys[1].ordinal == 0 &&
               composite_group_aggregate_decoded.has_aggregate_column ==
                   (aggregate_kind != PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) &&
               composite_group_aggregate_decoded.plan.limit == 5);
        if (aggregate_kind != PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
            assert(composite_group_aggregate_decoded.aggregate_column.ordinal ==
                   2);
        }
        assert(prdb_sdk_sql_parse_select_v13(
                   composite_group_aggregate_sql[aggregate_kind - 1],
                   composite_group_aggregate_sql_len[aggregate_kind - 1],
                   &composite_group_aggregate_statement,
                   sizeof(composite_group_aggregate_statement), NULL) ==
               PRDB_SDK_SQL_OK);
        assert(prdb_sdk_sql_bind_select_v13(
                   &composite_group_aggregate_statement,
                   (const uint8_t *)"items", 5,
                   composite_group_aggregate_columns, 3,
                   &composite_group_aggregate_binding,
                   sizeof(composite_group_aggregate_binding)) ==
               PRDB_SDK_SQL_BIND_OK);
        assert(prdb_sdk_optimizer_match_composite_group_aggregate_plan_v1(
                   &composite_group_aggregate_statement,
                   &composite_group_aggregate_binding,
                   (const uint8_t *)"public", 6,
                   composite_group_aggregate_payload,
                   composite_group_aggregate_payload_len) ==
               PRDB_SDK_OPTIMIZER_PLAN_OK);
    }
    static const uint8_t count_column_composite_sql[] =
        "SELECT active, id, COUNT(amount) FROM items "
        "GROUP BY active, id LIMIT 5";
    assert(prdb_sdk_sql_parse_select_v24(
               count_column_composite_sql,
               sizeof(count_column_composite_sql) - 1,
               &count_column_statement, sizeof(count_column_statement), NULL) ==
           PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_select_v24(
               &count_column_statement, (const uint8_t *)"items", 5,
               composite_group_aggregate_columns, 3, &count_column_binding,
               sizeof(count_column_binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(count_column_statement.base.base.base.base.base.base.aggregate_kind ==
           PRDB_SDK_SQL_V7_AGGREGATE_COUNT_COLUMN);
    assert(prdb_sdk_optimizer_encode_composite_group_aggregate_v1(
               &count_column_statement.base.base.base.base.base,
               (const uint8_t *)"public", 6, count_column_optimizer_request,
               sizeof(count_column_optimizer_request),
               &count_column_optimizer_request_len) ==
           PRDB_SDK_OPTIMIZER_ENCODE_OK);
    assert(count_column_optimizer_request_len > 30 &&
           count_column_optimizer_request[28] ==
               PRDB_SDK_SQL_V7_AGGREGATE_COUNT_COLUMN);
    uint32_t count_column_plan_len = build_composite_group_aggregate_plan(
        composite_group_aggregate_payload,
        PRDB_SDK_SQL_V7_AGGREGATE_COUNT_COLUMN);
    assert(prdb_sdk_optimizer_match_composite_group_aggregate_plan_v1(
               &count_column_statement.base.base.base.base.base,
               &count_column_binding.base.base.base.base.base,
               (const uint8_t *)"public", 6,
               composite_group_aggregate_payload, count_column_plan_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    uint32_t filtered_composite_group_aggregate_payload_len =
        build_filtered_composite_group_aggregate_plan(
            filtered_composite_group_aggregate_payload);
    assert(prdb_sdk_optimizer_decode_filtered_composite_group_aggregate_plan_v1(
               filtered_composite_group_aggregate_payload,
               filtered_composite_group_aggregate_payload_len,
               &filtered_composite_group_aggregate_decoded,
               sizeof(filtered_composite_group_aggregate_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(filtered_composite_group_aggregate_decoded.plan.plan_kind == 15 &&
           filtered_composite_group_aggregate_decoded.group_key_count == 2 &&
           filtered_composite_group_aggregate_decoded.aggregate_kind ==
               PRDB_SDK_SQL_V7_AGGREGATE_SUM &&
           filtered_composite_group_aggregate_decoded.filter.present == 1 &&
           filtered_composite_group_aggregate_decoded.plan.scan_column_count == 3 &&
           filtered_composite_group_aggregate_decoded.filter.prefix_len == 21 &&
           filtered_composite_group_aggregate_decoded.plan.limit == 5);
    memcpy(malformed, filtered_composite_group_aggregate_payload,
           filtered_composite_group_aggregate_payload_len);
    malformed[filtered_composite_group_aggregate_payload_len - 32] ^= 1;
    assert(prdb_sdk_optimizer_decode_filtered_composite_group_aggregate_plan_v1(
               malformed, filtered_composite_group_aggregate_payload_len,
               &filtered_composite_group_aggregate_decoded,
               sizeof(filtered_composite_group_aggregate_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    {
        static const uint8_t filtered_composite_sql[] =
            "SELECT active, id, SUM(amount) FROM items WHERE active = $1 "
            "GROUP BY active, id LIMIT 5";
        uint8_t bool_value = 1;
        prdb_sdk_value_v1 parameters[1] = {{
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BOOLEAN,
            &bool_value, 1}};
        static prdb_sdk_select_v13 filtered_statement;
        static prdb_sdk_select_binding_v13 filtered_binding;
        assert(prdb_sdk_sql_parse_select_v13(
                   filtered_composite_sql, sizeof(filtered_composite_sql) - 1,
                   &filtered_statement, sizeof(filtered_statement), NULL) ==
               PRDB_SDK_SQL_OK);
        assert(prdb_sdk_sql_bind_select_v13(
                   &filtered_statement, (const uint8_t *)"items", 5,
                   composite_group_aggregate_columns, 3, &filtered_binding,
                   sizeof(filtered_binding)) == PRDB_SDK_SQL_BIND_OK);
        assert(prdb_sdk_optimizer_match_filtered_composite_group_aggregate_plan_v1(
                   &filtered_statement, &filtered_binding, parameters, 1,
                   (const uint8_t *)"public", 6,
                   filtered_composite_group_aggregate_payload,
                   filtered_composite_group_aggregate_payload_len) ==
               PRDB_SDK_OPTIMIZER_PLAN_OK);
        filtered_composite_group_aggregate_payload[
            (uint32_t)(filtered_composite_group_aggregate_decoded.filter.prefix -
                       filtered_composite_group_aggregate_payload) +
            filtered_composite_group_aggregate_decoded.filter.prefix_len - 1] ^= 1;
        assert(prdb_sdk_optimizer_match_filtered_composite_group_aggregate_plan_v1(
                   &filtered_statement, &filtered_binding, parameters, 1,
                   (const uint8_t *)"public", 6,
                   filtered_composite_group_aggregate_payload,
                   filtered_composite_group_aggregate_payload_len) ==
               PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    }
    uint32_t ordered_composite_group_count_payload_len =
        build_ordered_composite_group_count_plan(ordered_composite_group_count_payload);
    assert(prdb_sdk_optimizer_decode_ordered_composite_group_count_plan_v1(
               ordered_composite_group_count_payload, ordered_composite_group_count_payload_len,
               &ordered_composite_group_count_decoded, sizeof(ordered_composite_group_count_decoded)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(ordered_composite_group_count_decoded.plan.plan_kind == 9 &&
           ordered_composite_group_count_decoded.group_key_count == 2 &&
           ordered_composite_group_count_decoded.order_count == 3 &&
           ordered_composite_group_count_decoded.order_items[0].group_key_index == 0 &&
           ordered_composite_group_count_decoded.order_items[1].source == 1 &&
           ordered_composite_group_count_decoded.plan.limit == 5);
    static const uint8_t ordered_composite_group_count_sql[] =
        "SELECT active, id, COUNT(*) FROM items GROUP BY active, id "
        "ORDER BY active DESC NULLS LAST, COUNT(*) DESC, "
        "id ASC NULLS FIRST LIMIT 5";
    ordered_composite_group_count_columns[0] = (prdb_sdk_sql_column_shape_v2){
        (const uint8_t *)"id", 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0};
    ordered_composite_group_count_columns[1] = (prdb_sdk_sql_column_shape_v2){
        (const uint8_t *)"active", 6, 1, PRDB_SDK_VALUE_TYPE_BOOLEAN, 0, 0};
    assert(prdb_sdk_sql_parse_select_v15(ordered_composite_group_count_sql,
               sizeof(ordered_composite_group_count_sql) - 1,
               &ordered_composite_group_count_statement,
               sizeof(ordered_composite_group_count_statement), NULL) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_select_v15(&ordered_composite_group_count_statement,
               (const uint8_t *)"items", 5, ordered_composite_group_count_columns, 2,
               &ordered_composite_group_count_binding,
               sizeof(ordered_composite_group_count_binding)) == PRDB_SDK_SQL_BIND_OK);
    int32_t ordered_composite_match_status = prdb_sdk_optimizer_match_ordered_composite_group_count_plan_v1(
               &ordered_composite_group_count_statement, &ordered_composite_group_count_binding,
               (const uint8_t *)"public", 6, ordered_composite_group_count_payload,
               ordered_composite_group_count_payload_len);
    assert(ordered_composite_match_status == PRDB_SDK_OPTIMIZER_PLAN_OK);
    ordered_composite_group_count_statement.order_items[0].direction = PRDB_SDK_SQL_V10_ORDER_ASCENDING;
    assert(prdb_sdk_optimizer_match_ordered_composite_group_count_plan_v1(
               &ordered_composite_group_count_statement, &ordered_composite_group_count_binding,
               (const uint8_t *)"public", 6, ordered_composite_group_count_payload,
               ordered_composite_group_count_payload_len) == PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    uint32_t group_aggregate_payload_len = build_group_aggregate_plan(group_aggregate_payload);
    assert(prdb_sdk_optimizer_decode_group_aggregate_plan_v1(group_aggregate_payload,
               group_aggregate_payload_len, &group_aggregate_decoded, sizeof(group_aggregate_decoded)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(group_aggregate_decoded.plan.plan_kind == 10 && group_aggregate_decoded.aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_AVG &&
           group_aggregate_decoded.has_aggregate_column == 1 && group_aggregate_decoded.plan.scan_column_count == 2);
    static const uint8_t group_aggregate_sql[] = "SELECT id, AVG(amount) FROM items GROUP BY id LIMIT 5";
    group_aggregate_columns[0] = (prdb_sdk_sql_column_shape_v2){(const uint8_t *)"id", 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0};
    group_aggregate_columns[1] = (prdb_sdk_sql_column_shape_v2){(const uint8_t *)"amount", 6, 1, PRDB_SDK_VALUE_TYPE_BIGINT, 0, 0};
    assert(prdb_sdk_sql_parse_select_v7(group_aggregate_sql, sizeof(group_aggregate_sql) - 1,
               &group_aggregate_statement, sizeof(group_aggregate_statement), NULL) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_select_v7(&group_aggregate_statement, (const uint8_t *)"items", 5,
               group_aggregate_columns, 2, &group_aggregate_binding, sizeof(group_aggregate_binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(prdb_sdk_optimizer_match_group_aggregate_plan_v1(&group_aggregate_statement, &group_aggregate_binding,
               (const uint8_t *)"public", 6, group_aggregate_payload, group_aggregate_payload_len) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    group_aggregate_statement.base.limit = 4;
    assert(prdb_sdk_optimizer_match_group_aggregate_plan_v1(&group_aggregate_statement, &group_aggregate_binding,
               (const uint8_t *)"public", 6, group_aggregate_payload, group_aggregate_payload_len) == PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    static const uint8_t *const ordered_group_aggregate_sql[] = {
        (const uint8_t *)"SELECT id, COUNT(*) FROM items GROUP BY id "
                         "ORDER BY COUNT(*) DESC NULLS LAST, id ASC NULLS FIRST LIMIT 5",
        (const uint8_t *)"SELECT id, MIN(amount) FROM items GROUP BY id "
                         "ORDER BY MIN(amount) DESC NULLS LAST, id ASC NULLS FIRST LIMIT 5",
        (const uint8_t *)"SELECT id, MAX(amount) FROM items GROUP BY id "
                         "ORDER BY MAX(amount) DESC NULLS LAST, id ASC NULLS FIRST LIMIT 5",
        (const uint8_t *)"SELECT id, SUM(amount) FROM items GROUP BY id "
                         "ORDER BY SUM(amount) DESC NULLS LAST, id ASC NULLS FIRST LIMIT 5",
        (const uint8_t *)"SELECT id, AVG(amount) FROM items GROUP BY id "
                         "ORDER BY AVG(amount) DESC NULLS LAST, id ASC NULLS FIRST LIMIT 5",
    };
    static const uint32_t ordered_group_aggregate_sql_len[] = {
        sizeof("SELECT id, COUNT(*) FROM items GROUP BY id "
               "ORDER BY COUNT(*) DESC NULLS LAST, id ASC NULLS FIRST LIMIT 5") - 1,
        sizeof("SELECT id, MIN(amount) FROM items GROUP BY id "
               "ORDER BY MIN(amount) DESC NULLS LAST, id ASC NULLS FIRST LIMIT 5") - 1,
        sizeof("SELECT id, MAX(amount) FROM items GROUP BY id "
               "ORDER BY MAX(amount) DESC NULLS LAST, id ASC NULLS FIRST LIMIT 5") - 1,
        sizeof("SELECT id, SUM(amount) FROM items GROUP BY id "
               "ORDER BY SUM(amount) DESC NULLS LAST, id ASC NULLS FIRST LIMIT 5") - 1,
        sizeof("SELECT id, AVG(amount) FROM items GROUP BY id "
               "ORDER BY AVG(amount) DESC NULLS LAST, id ASC NULLS FIRST LIMIT 5") - 1,
    };
    uint32_t ordered_group_aggregate_order_offset = 0;
    uint32_t ordered_group_aggregate_payload_len = 0;
    for (uint8_t aggregate_kind = PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR;
         aggregate_kind <= PRDB_SDK_SQL_V7_AGGREGATE_AVG; ++aggregate_kind) {
        ordered_group_aggregate_payload_len =
            build_ordered_group_aggregate_plan(
                ordered_group_aggregate_payload,
                &ordered_group_aggregate_order_offset, aggregate_kind);
        assert(prdb_sdk_optimizer_decode_ordered_group_aggregate_plan_v1(
                   ordered_group_aggregate_payload,
                   ordered_group_aggregate_payload_len,
                   &ordered_group_aggregate_decoded,
                   sizeof(ordered_group_aggregate_decoded)) ==
               PRDB_SDK_OPTIMIZER_PLAN_OK);
        assert(ordered_group_aggregate_decoded.plan.plan_kind == 11 &&
               ordered_group_aggregate_decoded.aggregate_kind ==
                   aggregate_kind &&
               ordered_group_aggregate_decoded.order_count == 2 &&
               ordered_group_aggregate_decoded.order_items[0].source == 1 &&
               ordered_group_aggregate_decoded.order_items[1].source == 0);
        assert(prdb_sdk_sql_parse_select_v12(
                   ordered_group_aggregate_sql[aggregate_kind - 1],
                   ordered_group_aggregate_sql_len[aggregate_kind - 1],
                   &ordered_group_aggregate_statement,
                   sizeof(ordered_group_aggregate_statement), NULL) ==
               PRDB_SDK_SQL_OK);
        assert(prdb_sdk_sql_bind_select_v12(
                   &ordered_group_aggregate_statement,
                   (const uint8_t *)"items", 5, group_aggregate_columns, 2,
                   &ordered_group_aggregate_binding,
                   sizeof(ordered_group_aggregate_binding)) ==
               PRDB_SDK_SQL_BIND_OK);
        assert(prdb_sdk_optimizer_match_ordered_group_aggregate_plan_v1(
                   &ordered_group_aggregate_statement,
                   &ordered_group_aggregate_binding,
                   (const uint8_t *)"public", 6,
                   ordered_group_aggregate_payload,
                   ordered_group_aggregate_payload_len) ==
               PRDB_SDK_OPTIMIZER_PLAN_OK);
    }
    ordered_group_aggregate_statement.base.base.base.base.base.limit = 4;
    assert(prdb_sdk_optimizer_match_ordered_group_aggregate_plan_v1(
               &ordered_group_aggregate_statement,
               &ordered_group_aggregate_binding, (const uint8_t *)"public", 6,
               ordered_group_aggregate_payload,
               ordered_group_aggregate_payload_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    ordered_group_aggregate_statement.base.base.base.base.base.limit = 5;
    memcpy(group_aggregate_malformed, ordered_group_aggregate_payload,
           ordered_group_aggregate_payload_len);
    group_aggregate_malformed[ordered_group_aggregate_order_offset] = 0;
    assert(prdb_sdk_optimizer_decode_ordered_group_aggregate_plan_v1(
               group_aggregate_malformed,
               ordered_group_aggregate_payload_len,
               &ordered_group_aggregate_decoded,
               sizeof(ordered_group_aggregate_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    static const uint8_t join_sql[] =
        "SELECT l.value, r.value FROM lefts AS l INNER JOIN rights AS r "
        "ON l.id = r.id AND l.active = r.active LIMIT 5;";
    static const uint8_t filtered_join_sql[] =
        "SELECT l.value, r.value FROM lefts AS l INNER JOIN rights AS r "
        "ON l.id = r.id AND l.active = r.active WHERE r.active > $3 LIMIT 5;";
    static const uint8_t join_left_name[] = "lefts";
    static const uint8_t join_right_name[] = "rights";
    static const uint8_t join_id_name[] = "id";
    static const uint8_t join_active_name[] = "active";
    static const uint8_t join_value_name[] = "value";
    const uint8_t *join_column_names[3] = {
        join_id_name, join_active_name, join_value_name};
    const uint32_t join_column_name_lengths[3] = {2, 6, 5};
    const uint32_t join_column_types[3] = {
        PRDB_SDK_VALUE_TYPE_INTEGER, PRDB_SDK_VALUE_TYPE_BOOLEAN,
        PRDB_SDK_VALUE_TYPE_TEXT};
    for (uint32_t index = 0; index < 3; ++index) {
        join_left_columns[index] = (prdb_sdk_sql_column_shape_v2){
            join_column_names[index], join_column_name_lengths[index], index,
            join_column_types[index], 0, 0};
        join_right_columns[index] = join_left_columns[index];
    }
    uint32_t join_error_offset = 0;
    assert(prdb_sdk_sql_parse_select_v19(
               join_sql, sizeof(join_sql) - 1, &join_statement,
               sizeof(join_statement), &join_error_offset) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_select_v19(
               &join_statement, join_left_name, sizeof(join_left_name) - 1,
               join_left_columns, 3, join_right_name,
               sizeof(join_right_name) - 1, join_right_columns, 3,
               &join_binding, sizeof(join_binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(prdb_sdk_optimizer_match_join_plan_v19_v1(
               &join_statement, &join_binding, (const uint8_t *)"public", 6,
               join_payload, join_payload_len) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    for (uint32_t index = 0; index < 3; ++index) {
        filtered_join_parameters[index] = (prdb_sdk_value_v1){
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BOOLEAN,
            filtered_join_parameter_bytes[index], 1};
    }
    assert(prdb_sdk_sql_parse_select_v20(
               filtered_join_sql, sizeof(filtered_join_sql) - 1,
               &filtered_join_statement, sizeof(filtered_join_statement),
               &join_error_offset) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_select_v20(
               &filtered_join_statement, join_left_name,
               sizeof(join_left_name) - 1, join_left_columns, 3,
               join_right_name, sizeof(join_right_name) - 1,
               join_right_columns, 3, &filtered_join_binding,
               sizeof(filtered_join_binding)) == PRDB_SDK_SQL_BIND_OK);
    assert(prdb_sdk_optimizer_match_filtered_join_plan_v20_v1(
               &filtered_join_statement, &filtered_join_binding,
               filtered_join_parameters, 3, (const uint8_t *)"public", 6,
               filtered_join_payload, filtered_join_payload_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    filtered_join_statement.comparison = PRDB_SDK_SQL_COMPARE_EQUAL;
    assert(prdb_sdk_optimizer_match_filtered_join_plan_v20_v1(
               &filtered_join_statement, &filtered_join_binding,
               filtered_join_parameters, 3, (const uint8_t *)"public", 6,
               filtered_join_payload, filtered_join_payload_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    filtered_join_statement.comparison = PRDB_SDK_SQL_COMPARE_GREATER;
    filtered_join_statement.parameter_index = 2;
    assert(prdb_sdk_optimizer_match_filtered_join_plan_v20_v1(
               &filtered_join_statement, &filtered_join_binding,
               filtered_join_parameters, 3, (const uint8_t *)"public", 6,
               filtered_join_payload, filtered_join_payload_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    filtered_join_statement.parameter_index = 3;
    filtered_join_parameters[2].type_code = PRDB_SDK_VALUE_TYPE_INTEGER;
    assert(prdb_sdk_optimizer_match_filtered_join_plan_v20_v1(
               &filtered_join_statement, &filtered_join_binding,
               filtered_join_parameters, 3, (const uint8_t *)"public", 6,
               filtered_join_payload, filtered_join_payload_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    filtered_join_parameters[2].type_code = PRDB_SDK_VALUE_TYPE_BOOLEAN;
    join_statement.base.base.limit = 6;
    assert(prdb_sdk_optimizer_match_join_plan_v19_v1(
               &join_statement, &join_binding, (const uint8_t *)"public", 6,
               join_payload, join_payload_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    join_statement.base.base.limit = 5;
    resolve.calls = 0;
    assert(prdb_sdk_optimizer_validate_executor_join_relations_v1(
               &host, join_payload, join_payload_len, response,
               sizeof(response), &response_len) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.calls == 2);
    resolve.calls = 0;
    resolve.fail_join_identity = 1;
    assert(prdb_sdk_optimizer_validate_executor_join_relations_v1(
               &host, join_payload, join_payload_len, response,
               sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    assert(resolve.calls == 2);
    resolve.fail_join_identity = 0;
    resolve.calls = 0;
    assert(prdb_sdk_optimizer_validate_executor_filtered_join_relations_v1(
               &host, filtered_join_payload, filtered_join_payload_len,
               response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.calls == 2);
    resolve.calls = 0;
    resolve.fail_join_identity = 1;
    assert(prdb_sdk_optimizer_validate_executor_filtered_join_relations_v1(
               &host, filtered_join_payload, filtered_join_payload_len,
               response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    assert(resolve.calls == 2);
    resolve.fail_join_identity = 0;
    memcpy(join_malformed, join_payload, join_payload_len);
    join_malformed[join_projection_side_offset] = 2;
    assert(prdb_sdk_optimizer_decode_join_plan_v1(
               join_malformed, join_payload_len, &join_decoded,
               sizeof(join_decoded)) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    memcpy(join_malformed, join_payload, join_payload_len);
    join_malformed[join_right_key_type_offset] = 6;
    assert(prdb_sdk_optimizer_decode_join_plan_v1(
               join_malformed, join_payload_len, &join_decoded,
               sizeof(join_decoded)) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    memcpy(join_malformed, join_payload, join_payload_len);
    join_malformed[join_payload_len] = 0;
    assert(prdb_sdk_optimizer_decode_join_plan_v1(
               join_malformed, join_payload_len + 1, &join_decoded,
               sizeof(join_decoded)) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    assert(prdb_sdk_optimizer_validate_executor_relation_v1(
               &host, update_plan, update_plan_len, response,
               sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(prdb_sdk_optimizer_decode_update_plan_v1(
               update_plan, update_plan_len - 1, &update_decoded,
               sizeof(update_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    memcpy(malformed, update_plan, update_plan_len);
    malformed[update_plan_len - 5] = 9; /* unsupported assignment kind */
    assert(prdb_sdk_optimizer_decode_update_plan_v1(
               malformed, update_plan_len, &update_decoded,
               sizeof(update_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    memcpy(malformed, update_plan, update_plan_len);
    malformed[update_plan_len - 1] = 1; /* NULL must have zero parameter index */
    malformed[update_plan_len - 5] = PRDB_OPTIMIZER_UPDATE_NULL_VALUE;
    assert(prdb_sdk_optimizer_decode_update_plan_v1(
               malformed, update_plan_len, &update_decoded,
               sizeof(update_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    assert(prdb_sdk_optimizer_decode_update_plan_v1(
               update_plan, update_plan_len,
               (prdb_sdk_optimizer_update_plan_v1 *)update_plan,
               sizeof(update_decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT);

    uint8_t update_sql[] = "UPDATE items SET c0 = $2 WHERE c0 = $1";
    uint8_t update_parameter_bytes[2][4] = {{0, 0, 0, 1}, {0, 0, 0, 2}};
    prdb_sdk_value_v1 update_parameters[2] = {
        {sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
         PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
         update_parameter_bytes[0], sizeof(update_parameter_bytes[0])},
        {sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
         PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
         update_parameter_bytes[1], sizeof(update_parameter_bytes[1])}};
    assert(prdb_sdk_sql_parse_update_v1(
               update_sql, sizeof(update_sql) - 1, &update_statement,
               sizeof(update_statement), NULL) == PRDB_SDK_SQL_OK);
    resolve.calls = 0;
    assert(prdb_sdk_optimizer_validate_update_plan_with_parameters_v1(
               &host, &update_statement, update_parameters, 2,
               (const uint8_t *)"public", 6, update_plan, update_plan_len,
               response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.calls == 1);
    update_statement.assignments[0].parameter_index = 1;
    assert(prdb_sdk_optimizer_validate_update_plan_with_parameters_v1(
               &host, &update_statement, update_parameters, 2,
               (const uint8_t *)"public", 6, update_plan, update_plan_len,
               response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    update_statement.assignments[0].parameter_index = 2;
    update_parameter_bytes[1][0] = 0;
    update_parameters[1].type_code = PRDB_SDK_VALUE_TYPE_BOOLEAN;
    assert(prdb_sdk_optimizer_validate_update_plan_with_parameters_v1(
               &host, &update_statement, update_parameters, 2,
               (const uint8_t *)"public", 6, update_plan, update_plan_len,
               response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    update_parameters[1].type_code = PRDB_SDK_VALUE_TYPE_INTEGER;
    update_parameter_bytes[1][0] = 0;

    uint8_t delete_plan[512];
    uint32_t delete_plan_len = build_delete_filter_plan(
        delete_plan, &predicate_offset);
    uint8_t delete_sql[] = "DELETE FROM items WHERE c0 = $1";
    uint8_t delete_parameter_bytes[4] = {0, 0, 0, 1};
    prdb_sdk_value_v1 delete_parameters[1] = {{
        sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
        delete_parameter_bytes, sizeof(delete_parameter_bytes)}};
    assert(prdb_sdk_sql_parse_delete_v1(
               delete_sql, sizeof(delete_sql) - 1, &delete_statement,
               sizeof(delete_statement), NULL) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_optimizer_decode_delete_plan_v1(
               delete_plan, delete_plan_len, &decoded, sizeof(decoded),
               &filter, sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    resolve.calls = 0;
    int32_t delete_validation_status =
        prdb_sdk_optimizer_validate_delete_plan_with_parameters_v1(
               &host, &delete_statement, delete_parameters, 1,
               (const uint8_t *)"public", 6, delete_plan, delete_plan_len,
               response, sizeof(response), &response_len);
    assert(delete_validation_status == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.calls == 1);
    assert(prdb_sdk_optimizer_acquire_plan_relation_v1(
               &host, delete_plan, delete_plan_len, response, sizeof(response),
               &response_len, &acquired_relation, sizeof(acquired_relation),
               acquired_columns, 4) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(acquired_relation.name_len == 5 &&
           memcmp(acquired_relation.name, "items", 5) == 0);
    delete_parameter_bytes[3] = 2;
    assert(prdb_sdk_optimizer_validate_delete_plan_with_parameters_v1(
               &host, &delete_statement, delete_parameters, 1,
               (const uint8_t *)"public", 6, delete_plan, delete_plan_len,
               response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    delete_parameter_bytes[3] = 1;
    uint8_t wrong_delete_sql[] = "DELETE FROM items WHERE c1 = $1";
    assert(prdb_sdk_sql_parse_delete_v1(
               wrong_delete_sql, sizeof(wrong_delete_sql) - 1,
               &delete_statement, sizeof(delete_statement), NULL) ==
           PRDB_SDK_SQL_OK);
    assert(prdb_sdk_optimizer_validate_delete_plan_with_parameters_v1(
               &host, &delete_statement, delete_parameters, 1,
               (const uint8_t *)"public", 6, delete_plan, delete_plan_len,
               response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);

    payload_len = build_filter_plan(payload, &predicate_offset);
    memcpy(malformed, payload, payload_len);
    malformed[predicate_offset] = 0x10; /* INTEGER is not a Boolean root. */
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               malformed, payload_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    memcpy(malformed, payload, payload_len);
    malformed[predicate_offset + 19] = 0x01; /* Boolean versus INTEGER. */
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               malformed, payload_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    memcpy(malformed, payload, payload_len);
    malformed[predicate_offset] = 0x03;
    malformed[predicate_offset + 1] = 0;
    malformed[predicate_offset + 2] = 1;
    malformed[predicate_offset + 3] = 0xff;
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               malformed, payload_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               payload, payload_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter) - 1) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT);

    probe_plan_len = build_unique_probe_plan(probe_plan,
                                              &probe_predicate_offset);
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               probe_plan, probe_plan_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(filter.present == 1 && filter.prefix_len != 0);
    probe_plan[probe_plan_len - 1] = 2;
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               probe_plan, probe_plan_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    probe_plan[probe_plan_len - 1] = 1;
    probe_plan[probe_plan_len - 9] = 0x01;
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               probe_plan, probe_plan_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);
    probe_plan[probe_plan_len - 9] = 0x02;
    memset(probe_plan + probe_plan_len - 9 - 26 - 16, 0, 16);
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               probe_plan, probe_plan_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    payload_len = build_plan(payload, &projected_id_offset,
                             &project_node_offset);
    assert(prdb_sdk_optimizer_decode_plan_v1(
               payload, payload_len, &decoded, sizeof(decoded) - 1) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT);
    assert(prdb_sdk_optimizer_decode_plan_v1(
               payload, payload_len, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    resolve.calls = 0;
    assert(prdb_sdk_optimizer_acquire_plan_relation_v1(
               &host, payload, payload_len, response, sizeof(response),
               &response_len, &acquired_relation, sizeof(acquired_relation),
               acquired_columns, 4) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.calls == 2);
    assert(acquired_relation.relation_id[0] == 2);
    assert(acquired_relation.executor_handle[0] == 6);
    assert(acquired_relation.column_count == 4);
    assert(acquired_columns[3].ordinal == 3 &&
           acquired_columns[3].type_code == 6);
    uint8_t cursor[16] = {0};
    uint32_t scan_opened = 0;
    resolve.calls = 0;
    assert(prdb_sdk_optimizer_open_plan_scan_v1(
               &host, payload, payload_len, response, sizeof(response),
               &response_len, malformed, sizeof(malformed),
               &scan_response_len,
               &acquired_relation, sizeof(acquired_relation), acquired_columns,
               4, cursor, &scan_opened) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.calls == 3 && scan_opened == 1 && cursor[0] == 7);
    uint8_t zero_limit_plan[sizeof(payload)];
    memcpy(zero_limit_plan, payload, payload_len);
    zero_limit_plan[payload_len - 1] = 0;
    resolve.calls = 0;
    assert(prdb_sdk_optimizer_open_plan_scan_v1(
               &host, zero_limit_plan, payload_len, response, sizeof(response),
               &response_len, malformed, sizeof(malformed),
               &scan_response_len,
               &acquired_relation, sizeof(acquired_relation), acquired_columns,
               4, cursor, &scan_opened) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.calls == 2 && scan_opened == 0 && cursor[0] == 0);
    resolve.executor_response[3] = 9;
    assert(prdb_sdk_optimizer_acquire_plan_relation_v1(
               &host, payload, payload_len, response, sizeof(response),
               &response_len, &acquired_relation, sizeof(acquired_relation),
               acquired_columns, 4) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    resolve.executor_response[3] = 2;

    uint8_t execute_delete_sql[] = "DELETE FROM items WHERE c0 = $1";
    uint8_t execute_delete_parameter_bytes[4] = {0, 0, 0, 1};
    prdb_sdk_value_v1 execute_delete_parameters[1] = {{
        sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
        execute_delete_parameter_bytes,
        sizeof(execute_delete_parameter_bytes)}};
    uint8_t execute_delete_plan[512];
    uint32_t execute_delete_plan_len = build_delete_filter_plan(
        execute_delete_plan, &predicate_offset);
    uint32_t deleted_count = 0;
    assert(prdb_sdk_sql_parse_delete_v1(
               execute_delete_sql, sizeof(execute_delete_sql) - 1,
               &delete_statement,
               sizeof(delete_statement), NULL) == PRDB_SDK_SQL_OK);
    resolve.optimizer_response_len = build_optimizer_response(
        resolve.optimizer_response, execute_delete_plan,
        execute_delete_plan_len);
    resolve.batch_response_len = build_row_batch(resolve.batch_response);
    resolve.calls = 0;
    resolve.optimizer_calls = 0;
    resolve.stage_delete_calls = 0;
    resolve.statement_failures = 0;
    resolve.event_count = 0;
    host_v5.v4 = host_v4;
    host_v5.optimizer_request = optimizer_request_v5;
    assert(prdb_sdk_optimizer_encode_delete_v1(
               &delete_statement, execute_delete_parameters, 1,
               (const uint8_t *)"public", 6, request, sizeof(request),
               &response_len) == PRDB_SDK_OPTIMIZER_ENCODE_OK);
    assert(prdb_sdk_optimizer_execute_delete_ast_v1(
               &host_v5, &delete_statement, execute_delete_parameters, 1,
               (const uint8_t *)"public", 6, 2, sizeof(batch_response), request,
               sizeof(request), optimizer_response, sizeof(optimizer_response),
               response, sizeof(response), scan_response, sizeof(scan_response),
               batch_response, sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, &delete_binding,
               sizeof(delete_binding), delete_cell_scratch, 4,
               delete_target_handles,
               PRDB_SDK_OPTIMIZER_PLAN_MAX_DELETE_TARGETS,
               &deleted_count) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(deleted_count == 1 && resolve.optimizer_calls == 1 &&
           resolve.stage_delete_calls == 1 && resolve.statement_failures == 0);
    assert(resolve.calls == 7);

    uint8_t execute_update_sql[] =
        "UPDATE items SET c0 = $2 WHERE c0 = $1";
    static prdb_sdk_update_v1 execute_update_statement;
    assert(prdb_sdk_sql_parse_update_v1(
               execute_update_sql, sizeof(execute_update_sql) - 1,
               &execute_update_statement, sizeof(execute_update_statement),
               NULL) == PRDB_SDK_SQL_OK);
    uint8_t execute_update_plan[512];
    uint32_t execute_update_plan_len = build_update_plan(execute_update_plan);
    resolve.optimizer_response_len = build_optimizer_response(
        resolve.optimizer_response, execute_update_plan,
        execute_update_plan_len);
    resolve.calls = 0;
    resolve.optimizer_calls = 0;
    resolve.stage_update_calls = 0;
    resolve.statement_failures = 0;
    resolve.event_count = 0;
    uint32_t updated_count = 0;
    assert(prdb_sdk_optimizer_execute_update_ast_v1(
               &host_v5, &execute_update_statement, update_parameters, 2,
               (const uint8_t *)"public", 6, 2, sizeof(batch_response),
               request, sizeof(request), optimizer_response,
               sizeof(optimizer_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, &delete_binding,
               sizeof(delete_binding), delete_cell_scratch, 4,
               delete_target_handles,
               PRDB_SDK_OPTIMIZER_PLAN_MAX_DELETE_TARGETS,
               &updated_count) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(updated_count == 1 && resolve.optimizer_calls == 1 &&
           resolve.stage_update_calls == 1 && resolve.statement_failures == 0);
    assert(resolve.calls == 7);

    resolve.calls = 0;
    resolve.optimizer_calls = 0;
    resolve.stage_update_calls = 0;
    resolve.statement_failures = 0;
    resolve.event_count = 0;
    resolve.cancellation_response[2] = 1;
    updated_count = 99;
    assert(prdb_sdk_optimizer_execute_update_ast_v1(
               &host_v5, &execute_update_statement, update_parameters, 2,
               (const uint8_t *)"public", 6, 2, sizeof(batch_response),
               request, sizeof(request), optimizer_response,
               sizeof(optimizer_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, &delete_binding,
               sizeof(delete_binding), delete_cell_scratch, 4,
               delete_target_handles,
               PRDB_SDK_OPTIMIZER_PLAN_MAX_DELETE_TARGETS,
               &updated_count) == PRDB_SDK_OPTIMIZER_PLAN_CANCELLED);
    assert(updated_count == 0 && resolve.optimizer_calls == 1 &&
           resolve.stage_update_calls == 0 && resolve.statement_failures == 1);
    assert(resolve.calls == 6);
    assert(resolve.events[6] == PRDB_SDK_EXECUTOR_OP_CLOSE_CURSOR);
    resolve.cancellation_response[2] = 0;

    resolve.calls = 0;
    resolve.optimizer_calls = 0;
    resolve.stage_update_calls = 0;
    resolve.statement_failures = 0;
    resolve.event_count = 0;
    resolve.fail_stage_update = 1;
    updated_count = 99;
    assert(prdb_sdk_optimizer_execute_update_ast_v1(
               &host_v5, &execute_update_statement, update_parameters, 2,
               (const uint8_t *)"public", 6, 2, sizeof(batch_response),
               request, sizeof(request), optimizer_response,
               sizeof(optimizer_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, &delete_binding,
               sizeof(delete_binding), delete_cell_scratch, 4,
               delete_target_handles,
               PRDB_SDK_OPTIMIZER_PLAN_MAX_DELETE_TARGETS,
               &updated_count) == PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR);
    assert(updated_count == 0 && resolve.optimizer_calls == 1 &&
           resolve.stage_update_calls == 1 && resolve.statement_failures == 1);
    assert(resolve.calls == 7);
    assert(resolve.events[6] == PRDB_SDK_EXECUTOR_OP_NEXT_BATCH);
    assert(resolve.events[7] == PRDB_SDK_EXECUTOR_OP_STAGE_UPDATE);
    resolve.fail_stage_update = 0;

    resolve.calls = 0;
    resolve.optimizer_calls = 0;
    resolve.stage_update_calls = 0;
    resolve.statement_failures = 0;
    resolve.event_count = 0;
    resolve.fail_optimizer_request = 1;
    updated_count = 99;
    assert(prdb_sdk_optimizer_execute_update_ast_v1(
               &host_v5, &execute_update_statement, update_parameters, 2,
               (const uint8_t *)"public", 6, 2, sizeof(batch_response),
               request, sizeof(request), optimizer_response,
               sizeof(optimizer_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, &delete_binding,
               sizeof(delete_binding), delete_cell_scratch, 4,
               delete_target_handles,
               PRDB_SDK_OPTIMIZER_PLAN_MAX_DELETE_TARGETS,
               &updated_count) == PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR);
    assert(updated_count == 0 && resolve.optimizer_calls == 1 &&
           resolve.calls == 0 && resolve.stage_update_calls == 0 &&
           resolve.statement_failures == 1);
    assert(resolve.event_count == 1 && resolve.events[0] == UINT16_C(0xffff));
    resolve.fail_optimizer_request = 0;

    resolve.batch_response_len = build_two_row_batch(resolve.batch_response);
    resolve.calls = 0;
    resolve.optimizer_calls = 0;
    resolve.stage_update_calls = 0;
    resolve.statement_failures = 0;
    resolve.event_count = 0;
    updated_count = 99;
    assert(prdb_sdk_optimizer_execute_update_ast_v1(
               &host_v5, &execute_update_statement, update_parameters, 2,
               (const uint8_t *)"public", 6, 2, sizeof(batch_response),
               request, sizeof(request), optimizer_response,
               sizeof(optimizer_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, &delete_binding,
               sizeof(delete_binding), delete_cell_scratch, 4,
               delete_target_handles, 1, &updated_count) ==
           PRDB_SDK_OPTIMIZER_PLAN_REQUEST_LIMIT);
    assert(updated_count == 0 && resolve.optimizer_calls == 1 &&
           resolve.stage_update_calls == 0 && resolve.statement_failures == 1);
    return 0;
}
