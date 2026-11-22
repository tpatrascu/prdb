#include "prdb_sdk_sql_exec_v10.h"
#include "prdb_sdk_sql_exec_v5.h"
#include "prdb_sdk_sql_exec_v6.h"
#include "prdb_sdk_sql_exec_v7.h"
#include "prdb_sdk_sql_exec_v11.h"
#include "prdb_sdk_sql_exec_v12.h"
#include "prdb_sdk_sql_exec_v13.h"
#include "prdb_sdk_sql_exec_v14.h"
#include "prdb_sdk_sql_exec_v15.h"
#include "prdb_sdk_sql_exec_v17.h"
#include "prdb_sdk_sql_exec_v18.h"
#include "prdb_sdk_sql_exec_v21.h"
#include "prdb_sdk_sql_exec_v22.h"
#include "prdb_sdk_sql_exec_v23.h"
#include "prdb_sdk_sql_result_v1.h"
#include "prdb_query_abi_payload_codecs.h"

#include <assert.h>
#include <string.h>
#include <stdio.h>

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
                                              uint8_t plan_kind) {
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
                                  PRDB_SDK_VALUE_TYPE_INTEGER, 1);
    if (plan_kind == 15) {
        uint8_t id[16];
        prdb_encode_optimizer_plan_filter_node_record(
            mock->optimizer_plan + offset, 2);
        offset += PRDB_OPTIMIZER_PLAN_FILTER_NODE_RECORD_HEADER_BYTES;
        prdb_encode_optimizer_plan_compare_predicate_header_record(
            mock->optimizer_plan + offset, 0x20, 0);
        offset += PRDB_OPTIMIZER_PLAN_COMPARE_PREDICATE_HEADER_RECORD_HEADER_BYTES;
        memset(id, 0x33, sizeof(id));
        prdb_encode_optimizer_plan_column_predicate_record(
            mock->optimizer_plan + offset, 0x10, id);
        offset += PRDB_OPTIMIZER_PLAN_COLUMN_PREDICATE_RECORD_HEADER_BYTES;
        prdb_encode_optimizer_plan_integer_predicate_record(
            mock->optimizer_plan + offset, 0x02, 1);
        offset += PRDB_OPTIMIZER_PLAN_INTEGER_PREDICATE_RECORD_HEADER_BYTES;
    }
    mock->optimizer_plan[offset++] = plan_kind;
    mock->optimizer_plan[offset++] = (uint8_t)mock->optimizer_aggregate_kind;
    mock->optimizer_plan[offset++] = 2;
    offset = put_optimizer_column(mock->optimizer_plan, offset, 0x33, 0,
                                  PRDB_SDK_VALUE_TYPE_INTEGER, 1);
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
    mock->optimizer_plan[offset++] = plan_kind == 15 ? 1 : 0;
    if (plan_kind == 15) put_u32(mock->optimizer_plan, &offset, 5);
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
    mock->optimizer_plan[offset++] = PRDB_SDK_SQL_V7_AGGREGATE_SUM;
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

static uint32_t make_relation(uint8_t *p, uint8_t handle_kind,
                              uint8_t handle_fill, uint32_t key_type) {
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
        p[offset++] = index == 0 ? key_type : PRDB_SDK_VALUE_TYPE_INTEGER;
        if (index == 0 && key_type == PRDB_SDK_VALUE_TYPE_DECIMAL) {
            p[offset++] = 38;
            p[offset++] = 2;
            p[offset++] = 0;
            p[offset++] = 0;
        } else if (index == 0 &&
                   key_type == PRDB_SDK_VALUE_TYPE_VARCHAR) {
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
    } else if (statement_kind == 15) {
        static const uint8_t expected_filter[] = {
            0x20, 0, 0x10, 2, 'i', 'd', 0x02, 0, 0, 0, 0, 0, 0, 0, 1};
        uint32_t predicate_len;
        uint8_t aggregate_name_len;
        if (key_count != 2 || aggregate_kind != mock->optimizer_aggregate_kind ||
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
        if (predicate_len != sizeof(expected_filter) ||
            request_len - offset < predicate_len ||
            memcmp(request + offset, expected_filter, sizeof(expected_filter)) != 0)
            return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
        offset += predicate_len;
        if (request_len - offset != 5 || request[offset++] != 1 ||
            request[offset++] != 0 || request[offset++] != 0 ||
            request[offset++] != 0 || request[offset++] != 5)
            return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
        if (build_composite_aggregate_plan(mock, 15) == 0)
            return PRDB_QUERY_CALLBACK_INTERNAL_ERROR;
    } else if (key_count != 2 ||
               aggregate_kind != mock->optimizer_aggregate_kind) {
        return PRDB_QUERY_CALLBACK_PROTOCOL_ERROR;
    } else if (build_composite_aggregate_plan(mock, 13) == 0) {
        return PRDB_QUERY_CALLBACK_INTERNAL_ERROR;
    }
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

static int32_t run_query(mock_state *mock, const uint8_t *sql,
                         uint64_t sql_len,
                         const prdb_sdk_value_v1 *parameters,
                         uint32_t parameter_count, uint64_t arena_capacity,
                         group_capture *capture) {
    static const uint8_t schema[] = "app";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[64] = {0};
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v10(
        &host, schema, sizeof(schema) - 1, sql, sql_len, parameters,
        parameter_count, 4, 512, planner_response, sizeof(planner_response),
        executor_response, sizeof(executor_response), batch_response,
        sizeof(batch_response), planner_columns, 2, executor_columns, 2,
        cell_scratch, 2, materialized_bytes, arena_capacity, on_group, capture);
}

static int32_t on_group_v11(void *context,
                            const prdb_sdk_value_v1 *group_key,
                            const prdb_sdk_value_v1 *aggregate_value,
                            uint64_t matching_row_count,
                            const prdb_sdk_select_binding_v11 *binding) {
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V11_VERSION);
    return on_group(context, group_key, aggregate_value, matching_row_count,
                    &binding->base);
}

static int32_t run_query_v11(mock_state *mock, const uint8_t *sql,
                             uint64_t sql_len, uint64_t arena_capacity,
                             group_capture *capture) {
    static const uint8_t schema[] = "app";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[64] = {0};
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v11(
        &host, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0, 4, 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, arena_capacity, on_group_v11, capture);
}

static uint32_t make_expected_group_count_sqlr(
    uint8_t *output, uint32_t key_type, uint8_t decimal_precision,
    uint8_t decimal_scale, const uint8_t *key_bytes, uint32_t key_len) {
    uint32_t offset = 0;
    memcpy(output + offset, "SQLR", 4);
    offset += 4;
    put_u16(output, &offset, 1);
    put_u16(output, &offset, 2);
    put_name(output, &offset, "id");
    put_u32(output, &offset, key_type);
    output[offset++] = decimal_precision;
    output[offset++] = decimal_scale;
    output[offset++] = PRDB_SDK_VALUE_STATE_VALUE;
    put_u32(output, &offset, key_len);
    memcpy(output + offset, key_bytes, key_len);
    offset += key_len;
    put_name(output, &offset, "count");
    put_u32(output, &offset, PRDB_SDK_VALUE_TYPE_BIGINT);
    output[offset++] = 0;
    output[offset++] = 0;
    output[offset++] = PRDB_SDK_VALUE_STATE_VALUE;
    put_u32(output, &offset, 8);
    memset(output + offset, 0, 7);
    output[offset + 7] = 1;
    offset += 8;
    return offset;
}

static uint32_t make_expected_group_sum_sqlr(
    uint8_t *output, const uint8_t *group_key, uint32_t group_key_len,
    uint64_t sum) {
    uint32_t offset = 0;
    memcpy(output + offset, "SQLR", 4);
    offset += 4;
    put_u16(output, &offset, 1);
    put_u16(output, &offset, 2);
    put_name(output, &offset, "group_key");
    put_u32(output, &offset, PRDB_SDK_VALUE_TYPE_TEXT);
    output[offset++] = 0;
    output[offset++] = 0;
    output[offset++] = PRDB_SDK_VALUE_STATE_VALUE;
    put_u32(output, &offset, group_key_len);
    memcpy(output + offset, group_key, group_key_len);
    offset += group_key_len;
    put_name(output, &offset, "sum");
    put_u32(output, &offset, PRDB_SDK_VALUE_TYPE_BIGINT);
    output[offset++] = 0;
    output[offset++] = 0;
    output[offset++] = PRDB_SDK_VALUE_STATE_VALUE;
    put_u32(output, &offset, 8);
    for (uint32_t byte = 0; byte < 8; ++byte) {
        output[offset + 7 - byte] = (uint8_t)(sum >> (byte * 8));
    }
    return offset + 8;
}

static int32_t on_group_v12(void *context,
                            const prdb_sdk_value_v1 *group_key,
                            const prdb_sdk_value_v1 *aggregate_value,
                            uint64_t matching_row_count,
                            const prdb_sdk_select_binding_v12 *binding) {
    group_capture *capture = (group_capture *)context;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V12_VERSION);
    assert(binding->order_count <= PRDB_SDK_SQL_V12_MAX_ORDER_KEYS);
    if (capture->scalar_order_mode) {
        uint32_t row = capture->calls;
        static const uint8_t id_name[] = "id";
        prdb_sdk_sql_column_shape_v2 group_column = {
            id_name, 2, 0, capture->scalar_order_type,
            capture->scalar_order_precision, capture->scalar_order_scale};
        uint8_t expected[128] = {0};
        uint32_t expected_len;
        assert(row < 3 && group_key != NULL && aggregate_value == NULL &&
               matching_row_count == 1);
        assert(binding->order_count == 1 &&
               binding->order_items[0].item.kind ==
                   PRDB_SDK_SQL_V10_ORDER_GROUP_KEY &&
               binding->order_items[0].item.direction ==
                   (capture->scalar_order_descending
                        ? PRDB_SDK_SQL_V10_ORDER_DESCENDING
                        : PRDB_SDK_SQL_V10_ORDER_ASCENDING) &&
               binding->order_items[0].type_code ==
                   capture->scalar_order_type);
        assert(group_key->state == PRDB_SDK_VALUE_STATE_VALUE &&
               group_key->type_code == capture->scalar_order_type &&
               group_key->data_len == capture->scalar_order_expected_len[row]);
        assert(memcmp(group_key->data, capture->scalar_order_expected[row],
                      group_key->data_len) == 0);
        expected_len = make_expected_group_count_sqlr(
            expected, capture->scalar_order_type,
            capture->scalar_order_precision, capture->scalar_order_scale,
            capture->scalar_order_expected[row],
            capture->scalar_order_expected_len[row]);
        assert(prdb_sdk_sql_encode_group_count_result_v1(
                   &group_column, group_key, matching_row_count,
                   capture->scalar_order_sqlr[row],
                   sizeof(capture->scalar_order_sqlr[row]),
                   &capture->scalar_order_sqlr_len[row]) ==
               PRDB_SDK_SQL_RESULT_OK);
        assert(capture->scalar_order_sqlr_len[row] == expected_len &&
               memcmp(capture->scalar_order_sqlr[row], expected,
                      expected_len) == 0);
        ++capture->calls;
        return 0;
    }
    if (binding->order_count == 2) {
        uint32_t expected_aggregate_type = PRDB_SDK_VALUE_TYPE_INTEGER;
        if (binding->base.base.base.base.aggregate_kind ==
                PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR ||
            binding->base.base.base.base.aggregate_kind ==
                PRDB_SDK_SQL_V7_AGGREGATE_SUM) {
            expected_aggregate_type = PRDB_SDK_VALUE_TYPE_BIGINT;
        } else if (binding->base.base.base.base.aggregate_kind ==
                   PRDB_SDK_SQL_V7_AGGREGATE_AVG) {
            expected_aggregate_type = PRDB_SDK_VALUE_TYPE_DECIMAL;
        }
        assert(binding->order_items[0].type_code == expected_aggregate_type);
        assert(binding->order_items[1].type_code ==
               (capture->mock->text_key_mode ? PRDB_SDK_VALUE_TYPE_TEXT
                                             : PRDB_SDK_VALUE_TYPE_INTEGER));
    }
    if (capture->mock->text_key_mode) {
        uint32_t key_index = 0;
        assert(group_key != NULL &&
               group_key->state == PRDB_SDK_VALUE_STATE_VALUE &&
               group_key->type_code == PRDB_SDK_VALUE_TYPE_TEXT &&
               group_key->data_len == capture->text_key_bytes);
        for (uint32_t index = group_key->data_len - 3;
             index < group_key->data_len; ++index) {
            assert(group_key->data[index] >= '0' &&
                   group_key->data[index] <= '9');
            key_index = key_index * 10 +
                        (uint32_t)(group_key->data[index] - '0');
        }
        assert(capture->calls < 128 && key_index < 128 &&
               matching_row_count == (capture->text_sum_tie_order_mode ? 2 : 1));
        if (capture->text_sum_tie_order_mode) {
            uint32_t expected_key = 62 - 2 * (capture->calls / 2) +
                                    capture->calls % 2;
            uint32_t sum = 300 + (key_index / 2) * 2;
            int64_t actual_sum = 0;
            uint8_t actual_sqlr[16384] = {0};
            uint8_t expected_sqlr[16384] = {0};
            uint64_t actual_sqlr_len = 0;
            uint32_t expected_sqlr_len;
            static const uint8_t group_name[] = "group_key";
            static const uint8_t measure_name[] = "measure";
            const prdb_sdk_sql_column_shape_v2 group_column = {
                group_name, sizeof(group_name) - 1, 0,
                PRDB_SDK_VALUE_TYPE_TEXT, 0, 0};
            const prdb_sdk_sql_column_shape_v2 measure_column = {
                measure_name, sizeof(measure_name) - 1, 1,
                PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0};
            assert(binding->base.base.base.base.aggregate_kind ==
                   PRDB_SDK_SQL_V7_AGGREGATE_SUM);
            assert(binding->base.base.base.base.aggregate_column.type_code ==
                   PRDB_SDK_VALUE_TYPE_INTEGER);
            assert(binding->order_count == 2);
            assert(binding->order_items[0].item.kind ==
                   PRDB_SDK_SQL_V10_ORDER_AGGREGATE);
            assert(binding->order_items[0].item.direction ==
                   PRDB_SDK_SQL_V10_ORDER_DESCENDING);
            assert(binding->order_items[0].item.null_order ==
                   PRDB_SDK_SQL_V11_NULLS_LAST);
            assert(binding->order_items[0].type_code ==
                   PRDB_SDK_VALUE_TYPE_BIGINT);
            assert(binding->order_items[1].item.kind ==
                   PRDB_SDK_SQL_V10_ORDER_GROUP_KEY);
            assert(binding->order_items[1].item.direction ==
                   PRDB_SDK_SQL_V10_ORDER_ASCENDING);
            assert(binding->order_items[1].item.null_order ==
                   PRDB_SDK_SQL_V11_NULLS_FIRST);
            assert(binding->order_items[1].type_code ==
                   PRDB_SDK_VALUE_TYPE_TEXT);
            assert(key_index == expected_key);
            assert(aggregate_value != NULL &&
                   aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_BIGINT &&
                   prdb_sdk_value_get_bigint_v1(aggregate_value, &actual_sum) ==
                       PRDB_SDK_VALUE_ACCESS_OK &&
                   actual_sum == (int64_t)sum);
            for (uint32_t index = 0; index < group_key->data_len - 3; ++index) {
                assert(group_key->data[index] == 'a');
            }
            expected_sqlr_len = make_expected_group_sum_sqlr(
                expected_sqlr, group_key->data, group_key->data_len, sum);
            assert(prdb_sdk_sql_encode_group_sum_result_v1(
                       &group_column, group_key, &measure_column,
                       aggregate_value, actual_sqlr, sizeof(actual_sqlr),
                       &actual_sqlr_len) == PRDB_SDK_SQL_RESULT_OK);
            assert(actual_sqlr_len == expected_sqlr_len &&
                   memcmp(actual_sqlr, expected_sqlr, expected_sqlr_len) == 0);
            capture->aggregates[capture->calls] = (int32_t)actual_sum;
        } else {
            capture->aggregates[capture->calls] = 1;
        }
        capture->keys[capture->calls] = (int32_t)key_index;
        capture->row_counts[capture->calls] = matching_row_count;
        ++capture->calls;
        if (capture->mock->cancel_after_group_callbacks == capture->calls) {
            capture->mock->cancelled = 1;
        }
        return 0;
    }
    return on_group(context, group_key, aggregate_value, matching_row_count,
                    &binding->base.base);
}

static int32_t run_query_v12(mock_state *mock, const uint8_t *sql,
                             uint64_t sql_len, uint64_t arena_capacity,
                             group_capture *capture) {
    static const uint8_t schema[] = "app";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[540000] = {0};
    uint8_t materialized_bytes[1048576] = {0};
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    uint32_t key_type = capture->scalar_order_mode
                            ? capture->scalar_order_type
                            : mock->text_key_mode ? PRDB_SDK_VALUE_TYPE_TEXT
                                                  : PRDB_SDK_VALUE_TYPE_INTEGER;
    uint8_t key_precision = capture->scalar_order_mode
                                ? capture->scalar_order_precision
                                : 0;
    uint8_t key_scale = capture->scalar_order_mode
                            ? capture->scalar_order_scale
                            : 0;
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, key_type, key_precision, key_scale},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, key_type, key_precision, key_scale},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v12(
        &host, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0,
        mock->max_candidates == 0 ? 4 : (uint16_t)mock->max_candidates,
        sizeof(batch_response),
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, arena_capacity, on_group_v12, capture);
}

typedef struct scalar_order_case {
    uint32_t type;
    uint8_t precision;
    uint8_t scale;
    uint32_t row_count;
    const uint8_t *lower;
    uint32_t lower_len;
    const uint8_t *middle;
    uint32_t middle_len;
    const uint8_t *higher;
    uint32_t higher_len;
} scalar_order_case;

typedef struct scalar_minmax_capture {
    mock_state *mock;
    uint32_t type;
    uint8_t precision;
    uint8_t scale;
    uint32_t aggregate_kind;
    const uint8_t *expected[2];
    uint32_t expected_len[2];
    uint32_t calls;
} scalar_minmax_capture;

static uint32_t make_typed_score_relation(uint8_t *p, uint8_t handle_kind,
                                          uint8_t handle_fill,
                                          const scalar_order_case *test) {
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
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
                                 : (uint8_t)test->type;
        if (index == 1 && test->type == PRDB_SDK_VALUE_TYPE_DECIMAL) {
            p[offset++] = test->precision;
            p[offset++] = test->scale;
            p[offset++] = 0;
            p[offset++] = 0;
        } else if (index == 1 && test->type == PRDB_SDK_VALUE_TYPE_VARCHAR) {
            put_u32(p, &offset, 128);
        } else {
            p[offset++] = 0;
            p[offset++] = 0;
            p[offset++] = 0;
            p[offset++] = 0;
        }
        put_name(p, &offset, index == 0 ? (const char *)id_name
                                        : (const char *)score_name);
    }
    return offset;
}

static uint32_t make_typed_score_rows(
    uint8_t *p, const scalar_order_case *test,
    const uint8_t *const scores[3], const uint32_t score_lengths[3]) {
    static const int32_t keys[3] = {1, 1, 2};
    uint32_t offset = 0;
    put_u16(p, &offset, 1);
    p[offset++] = 1;
    put_u16(p, &offset, 3);
    for (uint32_t index = 0; index < 3; ++index) {
        p[offset++] = 8;
        memset(p + offset, (int)(index + 1), 15); offset += 15;
        put_u16(p, &offset, 2);
        append_integer_cell(p, &offset, keys[index]);
        append_typed_cell(p, &offset, test->type, scores[index],
                          score_lengths[index]);
    }
    return offset;
}

static int32_t on_group_scalar_minmax(
    void *context, const prdb_sdk_value_v1 *group_key,
    const prdb_sdk_value_v1 *aggregate_value, uint64_t matching_row_count,
    const prdb_sdk_select_binding_v5 *binding) {
    scalar_minmax_capture *capture = (scalar_minmax_capture *)context;
    int32_t key = 0;
    uint32_t index = capture->calls;
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    prdb_sdk_sql_column_shape_v2 group_column = {
        id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0};
    prdb_sdk_sql_column_shape_v2 aggregate_column = {
        score_name, 5, 1, capture->type, capture->precision, capture->scale};
    uint8_t encoded[128] = {0};
    uint64_t encoded_len = 0;
    assert(index < 2 && capture->mock->batch_finished != 0);
    assert(binding->aggregate_kind == capture->aggregate_kind);
    assert(binding->aggregate_column.type_code == capture->type &&
           binding->aggregate_column.decimal_precision == capture->precision &&
           binding->aggregate_column.decimal_scale == capture->scale);
    assert(prdb_sdk_value_get_integer_v1(group_key, &key) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(key == (index == 0 ? 1 : 2));
    assert(matching_row_count == (index == 0 ? 2 : 1));
    assert(aggregate_value != NULL &&
           aggregate_value->state == PRDB_SDK_VALUE_STATE_VALUE &&
           aggregate_value->type_code == capture->type &&
           aggregate_value->data_len == capture->expected_len[index]);
    assert(memcmp(aggregate_value->data, capture->expected[index],
                  capture->expected_len[index]) == 0);
    assert(prdb_sdk_sql_encode_group_minmax_result_v1(
               &group_column, group_key, &aggregate_column, aggregate_value,
               capture->aggregate_kind, encoded, sizeof(encoded),
               &encoded_len) == PRDB_SDK_SQL_RESULT_OK);
    assert(encoded_len == 43 + capture->expected_len[index] &&
           memcmp(encoded, "SQLR\0\1\0\2", 8) == 0 &&
           encoded[27] == 0 && encoded[28] == 3 &&
           memcmp(encoded + 29,
                  capture->aggregate_kind == PRDB_SDK_SQL_V5_AGGREGATE_MIN
                      ? "min"
                      : "max",
                  3) == 0 &&
           encoded[32] == 0 && encoded[33] == 0 && encoded[34] == 0 &&
           encoded[35] == capture->type &&
           encoded[36] == capture->precision &&
           encoded[37] == capture->scale &&
           encoded[38] == PRDB_SDK_VALUE_STATE_VALUE &&
           encoded[42] == capture->expected_len[index] &&
           memcmp(encoded + 43, capture->expected[index],
                  capture->expected_len[index]) == 0);
    ++capture->calls;
    return 0;
}

static int32_t run_scalar_minmax(mock_state *mock, const uint8_t *sql,
                                 uint64_t sql_len,
                                 const scalar_order_case *test,
                                 prdb_sdk_sql_group_row_v5_fn on_group,
                                 void *capture) {
    static const uint8_t schema[] = "app";
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[2048] = {0};
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, test->type, test->precision, test->scale}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, test->type, test->precision, test->scale}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v5(
        &host, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0, 4, 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes), on_group, capture);
}

typedef struct scalar_integral_aggregate_capture {
    uint32_t input_type;
    uint32_t calls;
} scalar_integral_aggregate_capture;

static int32_t on_group_scalar_sum(
    void *context, const prdb_sdk_value_v1 *group_key,
    const prdb_sdk_value_v1 *aggregate_value, uint64_t matching_row_count,
    const prdb_sdk_select_binding_v6 *binding) {
    scalar_integral_aggregate_capture *capture =
        (scalar_integral_aggregate_capture *)context;
    int32_t key = 0;
    int64_t sum = 0;
    assert(capture->calls < 2 &&
           binding->aggregate_kind == PRDB_SDK_SQL_V6_AGGREGATE_SUM &&
           binding->aggregate_column.type_code == capture->input_type);
    assert(prdb_sdk_value_get_integer_v1(group_key, &key) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(key == (capture->calls == 0 ? 1 : 2));
    assert(matching_row_count == (capture->calls == 0 ? 2 : 1));
    assert(aggregate_value != NULL &&
           aggregate_value->state == PRDB_SDK_VALUE_STATE_VALUE &&
           aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_BIGINT &&
           prdb_sdk_value_get_bigint_v1(aggregate_value, &sum) ==
               PRDB_SDK_VALUE_ACCESS_OK &&
           sum == 6);
    ++capture->calls;
    return 0;
}

static int32_t on_group_scalar_avg(
    void *context, const prdb_sdk_value_v1 *group_key,
    const prdb_sdk_value_v1 *aggregate_value, uint64_t matching_row_count,
    const prdb_sdk_select_binding_v7 *binding) {
    static const uint8_t average_three[16] = {
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x2D, 0xC6, 0xC0};
    static const uint8_t average_six[16] = {
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x5B, 0x8D, 0x80};
    scalar_integral_aggregate_capture *capture =
        (scalar_integral_aggregate_capture *)context;
    const uint8_t *expected = capture->calls == 0 ? average_three : average_six;
    int32_t key = 0;
    assert(capture->calls < 2 &&
           binding->aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_AVG &&
           binding->aggregate_column.type_code == capture->input_type);
    assert(prdb_sdk_value_get_integer_v1(group_key, &key) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(key == (capture->calls == 0 ? 1 : 2));
    assert(matching_row_count == (capture->calls == 0 ? 2 : 1));
    assert(aggregate_value != NULL &&
           aggregate_value->state == PRDB_SDK_VALUE_STATE_VALUE &&
           aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
           aggregate_value->data_len == sizeof(average_three) &&
           memcmp(aggregate_value->data, expected, sizeof(average_three)) == 0);
    ++capture->calls;
    return 0;
}

static int32_t run_scalar_sum_or_avg(
    mock_state *mock, const uint8_t *sql, uint64_t sql_len,
    const scalar_order_case *test, uint32_t average,
    scalar_integral_aggregate_capture *capture) {
    static const uint8_t schema[] = "app";
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[2048] = {0};
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, test->type, test->precision, test->scale}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, test->type, test->precision, test->scale}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    if (average) {
        return prdb_sdk_sql_execute_group_v7(
            &host, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0, 4, 512,
            planner_response, sizeof(planner_response), executor_response,
            sizeof(executor_response), batch_response, sizeof(batch_response),
            planner_columns, 2, executor_columns, 2, cell_scratch, 2,
            materialized_bytes, sizeof(materialized_bytes), on_group_scalar_avg,
            capture);
    }
    return prdb_sdk_sql_execute_group_v6(
        &host, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0, 4, 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes), on_group_scalar_sum,
        capture);
}

static void reset_scan(mock_state *mock, group_capture *capture,
                       uint32_t finished);

static void run_grouped_scalar_order_matrix(mock_state *mock) {
    static const uint8_t asc_sql[] =
        "SELECT id, COUNT(*) FROM widgets GROUP BY id ORDER BY id ASC";
    static const uint8_t desc_sql[] =
        "SELECT id, COUNT(*) FROM widgets GROUP BY id ORDER BY id DESC";
    static const uint8_t min_sql[] =
        "SELECT id, MIN(score) FROM widgets GROUP BY id";
    static const uint8_t max_sql[] =
        "SELECT id, MAX(score) FROM widgets GROUP BY id";
    static const uint8_t boolean_lower[] = {0};
    static const uint8_t boolean_higher[] = {1};
    static const uint8_t smallint_lower[] = {0x80, 0};
    static const uint8_t smallint_middle[] = {0, 0};
    static const uint8_t smallint_higher[] = {0x7F, 0xFF};
    static const uint8_t integer_lower[] = {0x80, 0, 0, 0};
    static const uint8_t integer_middle[] = {0, 0, 0, 0};
    static const uint8_t integer_higher[] = {0x7F, 0xFF, 0xFF, 0xFF};
    static const uint8_t bigint_lower[] = {0x80, 0, 0, 0, 0, 0, 0, 0};
    static const uint8_t bigint_middle[] = {0, 0, 0, 0, 0, 0, 0, 0};
    static const uint8_t bigint_higher[] = {0x7F, 0xFF, 0xFF, 0xFF,
                                             0xFF, 0xFF, 0xFF, 0xFF};
    static const uint8_t decimal_lower[] = {0xFF, 0xFF, 0xFF, 0xFF,
                                             0xFF, 0xFF, 0xFF, 0xFF,
                                             0xFF, 0xFF, 0xFF, 0xFF,
                                             0xFF, 0xFF, 0xFF, 0x9C};
    static const uint8_t decimal_middle[16] = {0};
    static const uint8_t decimal_higher[] = {0, 0, 0, 0, 0, 0, 0, 0,
                                              0, 0, 0, 0, 0, 0, 0, 0x64};
    static const uint8_t text_lower[] = {'a'};
    static const uint8_t text_middle[] = {'b'};
    static const uint8_t text_higher[] = {'c'};
    static const uint8_t varchar_lower[] = {0xC2, 0xA2};
    static const uint8_t varchar_middle[] = {0xC3, 0xA9};
    static const uint8_t varchar_higher[] = {0xE2, 0x82, 0xAC};
    static const uint8_t bytea_lower[] = {0, 0};
    static const uint8_t bytea_middle[] = {0, 1};
    static const uint8_t bytea_higher[] = {1, 0};
    static const uint8_t uuid_lower[] = {0, 1, 2, 3, 4, 5, 6, 7,
                                          8, 9, 10, 11, 12, 13, 14, 15};
    static const uint8_t uuid_middle[] = {0, 1, 2, 3, 4, 5, 6, 7,
                                           8, 9, 10, 11, 12, 13, 14, 16};
    static const uint8_t uuid_higher[] = {0, 1, 2, 3, 4, 5, 6, 7,
                                           8, 9, 10, 11, 12, 13, 14, 17};
    static const uint8_t date_lower[] = {0xFF, 0xFF, 0xFF, 0xFF};
    static const uint8_t date_middle[] = {0, 0, 0, 0};
    static const uint8_t date_higher[] = {0, 0, 0, 1};
    static const uint8_t timestamp_lower[] = {0xFF, 0xFF, 0xFF, 0xFF,
                                               0xFF, 0xFF, 0xFF, 0xFF};
    static const uint8_t timestamp_middle[] = {0, 0, 0, 0, 0, 0, 0, 0};
    static const uint8_t timestamp_higher[] = {0, 0, 0, 0, 0, 0, 0, 1};
    static const scalar_order_case cases[] = {
        {PRDB_SDK_VALUE_TYPE_BOOLEAN, 0, 0, 2,
         boolean_lower, sizeof(boolean_lower), NULL, 0,
         boolean_higher, sizeof(boolean_higher)},
        {PRDB_SDK_VALUE_TYPE_SMALLINT, 0, 0, 3,
         smallint_lower, sizeof(smallint_lower), smallint_middle,
         sizeof(smallint_middle), smallint_higher, sizeof(smallint_higher)},
        {PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 3,
         integer_lower, sizeof(integer_lower), integer_middle,
         sizeof(integer_middle), integer_higher, sizeof(integer_higher)},
        {PRDB_SDK_VALUE_TYPE_BIGINT, 0, 0, 3,
         bigint_lower, sizeof(bigint_lower), bigint_middle,
         sizeof(bigint_middle), bigint_higher, sizeof(bigint_higher)},
        {PRDB_SDK_VALUE_TYPE_DECIMAL, 38, 2, 3,
         decimal_lower, sizeof(decimal_lower), decimal_middle,
         sizeof(decimal_middle), decimal_higher, sizeof(decimal_higher)},
        {PRDB_SDK_VALUE_TYPE_TEXT, 0, 0, 3,
         text_lower, sizeof(text_lower), text_middle, sizeof(text_middle),
         text_higher, sizeof(text_higher)},
        {PRDB_SDK_VALUE_TYPE_VARCHAR, 0, 0, 3,
         varchar_lower, sizeof(varchar_lower), varchar_middle,
         sizeof(varchar_middle), varchar_higher, sizeof(varchar_higher)},
        {PRDB_SDK_VALUE_TYPE_BYTEA, 0, 0, 3,
         bytea_lower, sizeof(bytea_lower), bytea_middle, sizeof(bytea_middle),
         bytea_higher, sizeof(bytea_higher)},
        {PRDB_SDK_VALUE_TYPE_UUID, 0, 0, 3,
         uuid_lower, sizeof(uuid_lower), uuid_middle, sizeof(uuid_middle),
         uuid_higher, sizeof(uuid_higher)},
        {PRDB_SDK_VALUE_TYPE_DATE, 0, 0, 3,
         date_lower, sizeof(date_lower), date_middle, sizeof(date_middle),
         date_higher, sizeof(date_higher)},
        {PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ, 0, 0, 3,
         timestamp_lower, sizeof(timestamp_lower), timestamp_middle,
         sizeof(timestamp_middle), timestamp_higher, sizeof(timestamp_higher)},
    };

    for (uint32_t case_index = 0;
         case_index < sizeof(cases) / sizeof(cases[0]); ++case_index) {
        const scalar_order_case *test = &cases[case_index];
        const uint8_t *input_values[3] = {
            test->higher, test->lower, test->middle};
        uint32_t input_lengths[3] = {
            test->higher_len, test->lower_len, test->middle_len};
        const uint8_t *ascending_values[3] = {
            test->lower, test->middle, test->higher};
        uint32_t ascending_lengths[3] = {
            test->lower_len, test->middle_len, test->higher_len};
        const uint8_t *descending_values[3] = {
            test->higher, test->middle, test->lower};
        uint32_t descending_lengths[3] = {
            test->higher_len, test->middle_len, test->lower_len};

        mock->text_key_mode = 0;
        mock->max_candidates = 0;
        mock->planner_len = make_relation(
            mock->planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
            test->type);
        mock->executor_len = make_relation(mock->executor, 6, 0x22,
                                           test->type);
        mock->batch_len = make_typed_key_rows(
            mock->batch, 1, test->type, input_values, input_lengths,
            test->row_count);
        for (uint32_t direction = 0; direction < 2; ++direction) {
            group_capture capture = {0};
            capture.mock = mock;
            capture.scalar_order_mode = 1;
            capture.scalar_order_type = test->type;
            capture.scalar_order_precision = test->precision;
            capture.scalar_order_scale = test->scale;
            capture.scalar_order_descending = direction;
            for (uint32_t row = 0; row < test->row_count; ++row) {
                if (test->row_count == 2) {
                    capture.scalar_order_expected[row] = direction
                        ? (row == 0 ? test->higher : test->lower)
                        : (row == 0 ? test->lower : test->higher);
                    capture.scalar_order_expected_len[row] = direction
                        ? (row == 0 ? test->higher_len : test->lower_len)
                        : (row == 0 ? test->lower_len : test->higher_len);
                } else {
                    capture.scalar_order_expected[row] = direction
                        ? descending_values[row]
                        : ascending_values[row];
                    capture.scalar_order_expected_len[row] = direction
                        ? descending_lengths[row]
                        : ascending_lengths[row];
                }
            }
            reset_scan(mock, &capture, 1);
            int32_t status = run_query_v12(
                mock, direction ? desc_sql : asc_sql,
                direction ? sizeof(desc_sql) - 1 : sizeof(asc_sql) - 1, 1024,
                &capture);
            assert(status == PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == test->row_count &&
                   mock->statement_failed == 0);
            for (uint32_t row = 0; row < capture.calls; ++row) {
                assert(capture.scalar_order_sqlr_len[row] > 0);
            }
        }

        {
            const uint8_t *middle = test->middle != NULL
                                        ? test->middle
                                        : test->lower;
            uint32_t middle_len = test->middle != NULL
                                      ? test->middle_len
                                      : test->lower_len;
            const uint8_t *score_values[3] = {
                test->higher, test->lower, middle};
            const uint32_t score_lengths[3] = {
                test->higher_len, test->lower_len, middle_len};
            mock->planner_len = make_typed_score_relation(
                mock->planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
                test);
            mock->executor_len = make_typed_score_relation(
                mock->executor, 6, 0x22, test);
            mock->batch_len = make_typed_score_rows(
                mock->batch, test, score_values, score_lengths);
            for (uint32_t aggregate = 0; aggregate < 2; ++aggregate) {
                scalar_minmax_capture capture = {0};
                capture.mock = mock;
                capture.type = test->type;
                capture.precision = test->precision;
                capture.scale = test->scale;
                capture.aggregate_kind = aggregate == 0
                    ? PRDB_SDK_SQL_V5_AGGREGATE_MIN
                    : PRDB_SDK_SQL_V5_AGGREGATE_MAX;
                capture.expected[0] = aggregate == 0
                    ? test->lower : test->higher;
                capture.expected_len[0] = aggregate == 0
                    ? test->lower_len : test->higher_len;
                capture.expected[1] = middle;
                capture.expected_len[1] = middle_len;
                group_capture reset_capture = {0};
                reset_scan(mock, &reset_capture, 1);
                int32_t status = run_scalar_minmax(
                    mock, aggregate == 0 ? min_sql : max_sql,
                    aggregate == 0 ? sizeof(min_sql) - 1
                                   : sizeof(max_sql) - 1,
                    test, on_group_scalar_minmax, &capture);
                assert(status == PRDB_SDK_SQL_EXEC_OK &&
                       capture.calls == 2 && mock->statement_failed == 0);
            }
        }
    }
}

static void run_integral_aggregate_runner_matrix(mock_state *mock) {
    static const uint8_t sum_sql[] =
        "SELECT id, SUM(score) FROM widgets GROUP BY id";
    static const uint8_t avg_sql[] =
        "SELECT id, AVG(score) FROM widgets GROUP BY id";
    static const uint8_t smallint_two[] = {0, 2};
    static const uint8_t smallint_four[] = {0, 4};
    static const uint8_t smallint_six[] = {0, 6};
    static const uint8_t integer_two[] = {0, 0, 0, 2};
    static const uint8_t integer_four[] = {0, 0, 0, 4};
    static const uint8_t integer_six[] = {0, 0, 0, 6};
    static const uint8_t bigint_two[] = {0, 0, 0, 0, 0, 0, 0, 2};
    static const uint8_t bigint_four[] = {0, 0, 0, 0, 0, 0, 0, 4};
    static const uint8_t bigint_six[] = {0, 0, 0, 0, 0, 0, 0, 6};
    static const scalar_order_case cases[] = {
        {PRDB_SDK_VALUE_TYPE_SMALLINT, 0, 0, 3, smallint_two,
         sizeof(smallint_two), smallint_six, sizeof(smallint_six),
         smallint_four, sizeof(smallint_four)},
        {PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 3, integer_two,
         sizeof(integer_two), integer_six, sizeof(integer_six), integer_four,
         sizeof(integer_four)},
        {PRDB_SDK_VALUE_TYPE_BIGINT, 0, 0, 3, bigint_two, sizeof(bigint_two),
         bigint_six, sizeof(bigint_six), bigint_four, sizeof(bigint_four)},
    };

    for (uint32_t index = 0; index < sizeof(cases) / sizeof(cases[0]);
         ++index) {
        const scalar_order_case *test = &cases[index];
        const uint8_t *scores[3] = {test->higher, test->lower, test->middle};
        const uint32_t score_lengths[3] = {
            test->higher_len, test->lower_len, test->middle_len};
        mock->planner_len = make_typed_score_relation(
            mock->planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99, test);
        mock->executor_len = make_typed_score_relation(
            mock->executor, 6, 0x22, test);
        mock->batch_len = make_typed_score_rows(
            mock->batch, test, scores, score_lengths);

        for (uint32_t average = 0; average < 2; ++average) {
            scalar_integral_aggregate_capture capture = {test->type, 0};
            group_capture reset_capture = {0};
            reset_scan(mock, &reset_capture, 1);
            int32_t status = run_scalar_sum_or_avg(
                mock, average ? avg_sql : sum_sql,
                average ? sizeof(avg_sql) - 1 : sizeof(sum_sql) - 1,
                test, average, &capture);
            assert(status == PRDB_SDK_SQL_EXEC_OK && capture.calls == 2 &&
                   mock->statement_failed == 0);
        }
    }
}

static int32_t on_group_v13(void *context,
                            const prdb_sdk_value_v1 *group_keys,
                            uint32_t group_key_count,
                            const prdb_sdk_value_v1 *aggregate_value,
                            uint64_t matching_row_count,
                            const prdb_sdk_select_binding_v13 *binding) {
    group_capture *capture = (group_capture *)context;
    int32_t first = 0, second = 0;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V13_VERSION);
    assert(group_key_count == 2 && binding->group_key_count == 2);
    assert(capture->mock->batch_finished != 0);
    assert(capture->mock->executor_calls ==
           (capture->mock->optimizer_enabled ? 4u : 3u));
    assert(capture->mock->optimizer_relation_resolves ==
           (capture->mock->optimizer_enabled ? 1u : 0u));
    assert(capture->calls < 128);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &first) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[1], &second) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(binding->base.aggregate_kind ==
               PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR ||
           binding->base.aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_MIN ||
           binding->base.aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_MAX ||
           binding->base.aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_SUM ||
           binding->base.aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_AVG);
    if (binding->base.aggregate_kind ==
        PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
        assert(aggregate_value == NULL);
        capture->aggregates[capture->calls] = (int32_t)matching_row_count;
    } else if (binding->base.aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_SUM) {
        int64_t sum = 0;
        assert(aggregate_value != NULL &&
               aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_BIGINT);
        assert(prdb_sdk_value_get_bigint_v1(aggregate_value, &sum) ==
               PRDB_SDK_VALUE_ACCESS_OK);
        capture->aggregates[capture->calls] = (int32_t)sum;
        assert((int64_t)capture->aggregates[capture->calls] == sum);
    } else if (binding->base.aggregate_kind ==
                   PRDB_SDK_SQL_V7_AGGREGATE_MIN ||
               binding->base.aggregate_kind ==
                   PRDB_SDK_SQL_V7_AGGREGATE_MAX) {
        int32_t value = 0;
        assert(aggregate_value != NULL &&
               aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_INTEGER);
        assert(prdb_sdk_value_get_integer_v1(aggregate_value, &value) ==
               PRDB_SDK_VALUE_ACCESS_OK);
        capture->aggregates[capture->calls] = value;
    } else {
        static const uint8_t average_nine[16] = {
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x89, 0x54, 0x40};
        static const uint8_t average_five[16] = {
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x4c, 0x4b, 0x40};
        const uint8_t *expected = first == 1 ? average_nine : average_five;
        assert(binding->base.aggregate_kind ==
               PRDB_SDK_SQL_V7_AGGREGATE_AVG);
        assert(aggregate_value != NULL &&
               aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
               aggregate_value->data_len == 16 &&
               memcmp(aggregate_value->data, expected, 16) == 0);
        capture->aggregates[capture->calls] = first == 1 ? 9 : 5;
    }
    capture->keys[capture->calls] = first;
    capture->second_keys[capture->calls] = second;
    capture->row_counts[capture->calls] = matching_row_count;
    ++capture->calls;
    if (capture->mock->cancel_after_group_callbacks == capture->calls) {
        capture->mock->cancelled = 1;
    }
    return 0;
}

typedef struct nullable_composite_capture {
    mock_state *mock;
    uint32_t calls;
} nullable_composite_capture;

static int32_t on_nullable_composite_group_v13(
    void *context, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v13 *binding) {
    static const uint32_t expected_counts[] = {2, 1, 1, 2};
    nullable_composite_capture *capture = context;
    int32_t first = 0, second = 0;
    uint32_t first_state = 0, second_state = 0;
    uint32_t index = capture->calls;
    assert(index < 4 && group_key_count == 2 && aggregate_value == NULL);
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V13_VERSION &&
           binding->base.aggregate_kind ==
               PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR);
    assert(capture->mock->batch_finished != 0 &&
           capture->mock->executor_calls == 3);
    assert(prdb_sdk_value_state_v1(&group_keys[0], &first_state) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(prdb_sdk_value_state_v1(&group_keys[1], &second_state) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    if (index == 0) {
        assert(first_state == PRDB_SDK_VALUE_STATE_VALUE &&
               second_state == PRDB_SDK_VALUE_STATE_NULL);
        assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &first) ==
                   PRDB_SDK_VALUE_ACCESS_OK &&
               first == 1);
    } else if (index == 1) {
        assert(first_state == PRDB_SDK_VALUE_STATE_VALUE &&
               second_state == PRDB_SDK_VALUE_STATE_VALUE);
        assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &first) ==
                   PRDB_SDK_VALUE_ACCESS_OK &&
               prdb_sdk_value_get_integer_v1(&group_keys[1], &second) ==
                   PRDB_SDK_VALUE_ACCESS_OK &&
               first == 1 && second == 9);
    } else if (index == 2) {
        assert(first_state == PRDB_SDK_VALUE_STATE_NULL &&
               second_state == PRDB_SDK_VALUE_STATE_VALUE);
        assert(prdb_sdk_value_get_integer_v1(&group_keys[1], &second) ==
                   PRDB_SDK_VALUE_ACCESS_OK &&
               second == 9);
    } else {
        assert(first_state == PRDB_SDK_VALUE_STATE_NULL &&
               second_state == PRDB_SDK_VALUE_STATE_NULL);
    }
    assert(matching_row_count == expected_counts[index]);
    ++capture->calls;
    return 0;
}

static int32_t on_group_eight_keys(
    void *context, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v13 *binding) {
    group_capture *capture = (group_capture *)context;
    int64_t sum = 0;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V13_VERSION);
    assert(group_key_count == 8 && binding->group_key_count == 8);
    assert(binding->base.aggregate_kind == PRDB_SDK_SQL_V7_AGGREGATE_SUM);
    assert(binding->base.aggregate_column.ordinal == 8);
    assert(capture->calls < 2 && capture->mock->batch_finished != 0);
    for (uint32_t index = 0; index < 8; ++index) {
        int32_t value = 0;
        assert(group_keys[index].type_code == PRDB_SDK_VALUE_TYPE_INTEGER);
        assert(prdb_sdk_value_get_integer_v1(&group_keys[index], &value) ==
               PRDB_SDK_VALUE_ACCESS_OK);
        assert(value == (int32_t)(index + 1) +
                            (capture->calls == 1 && index == 7 ? 1 : 0));
    }
    assert(aggregate_value != NULL &&
           aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_BIGINT);
    assert(prdb_sdk_value_get_bigint_v1(aggregate_value, &sum) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(sum == (capture->calls == 0 ? 9 : 8));
    assert(matching_row_count == (capture->calls == 0 ? 2 : 1));
    ++capture->calls;
    return 0;
}

static int32_t on_ordered_group_eight_keys(
    void *context, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v15 *binding) {
    group_capture *capture = (group_capture *)context;
    int64_t sum = 0;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V15_VERSION);
    assert(binding->base.base.group_key_count == 8);
    assert(binding->base.base.base.aggregate_kind ==
           PRDB_SDK_SQL_V7_AGGREGATE_SUM);
    assert(binding->base.base.base.aggregate_column.ordinal == 8);
    assert(binding->order_count == 9);
    assert(group_key_count == 8 && capture->calls < 2);
    assert(capture->mock->batch_finished != 0 &&
           capture->mock->executor_calls ==
               (capture->mock->optimizer_enabled ? 4u : 3u));
    for (uint32_t index = 0; index < 8; ++index) {
        int32_t value = 0;
        assert(binding->order_items[index].item.kind ==
               PRDB_SDK_SQL_V10_ORDER_GROUP_KEY);
        assert(binding->order_items[index].item.group_key_index == index);
        assert(binding->order_items[index].item.direction ==
               PRDB_SDK_SQL_V10_ORDER_DESCENDING);
        assert(binding->order_items[index].type_code ==
               PRDB_SDK_VALUE_TYPE_INTEGER);
        assert(group_keys[index].type_code == PRDB_SDK_VALUE_TYPE_INTEGER);
        assert(prdb_sdk_value_get_integer_v1(&group_keys[index], &value) ==
               PRDB_SDK_VALUE_ACCESS_OK);
        assert(value == (int32_t)(index + 1) +
                            (capture->calls == 0 && index == 7 ? 1 : 0));
    }
    assert(binding->order_items[8].item.kind ==
           PRDB_SDK_SQL_V10_ORDER_AGGREGATE);
    assert(binding->order_items[8].item.direction ==
           PRDB_SDK_SQL_V10_ORDER_DESCENDING);
    assert(binding->order_items[8].type_code == PRDB_SDK_VALUE_TYPE_BIGINT);
    assert(aggregate_value != NULL &&
           aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_BIGINT);
    assert(prdb_sdk_value_get_bigint_v1(aggregate_value, &sum) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(sum == (capture->calls == 0 ? 8 : 9));
    assert(matching_row_count == (capture->calls == 0 ? 1 : 2));
    ++capture->calls;
    return 0;
}

static int32_t run_query_v13(mock_state *mock, const uint8_t *sql,
                             uint64_t sql_len, uint64_t arena_capacity,
                             group_capture *capture) {
    static const uint8_t schema[] = "app";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[4096] = {0};
    uint8_t materialized_bytes[4096] = {0};
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    mock->optimizer_enabled = 0;
    return prdb_sdk_sql_execute_group_v13(
        &host, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0,
        mock->max_candidates == 0 ? 4 : (uint16_t)mock->max_candidates,
        mock->max_candidates > 3 ? 4096 : 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, arena_capacity, on_group_v13, capture);
}

static int32_t run_nullable_composite_count_v13(
    mock_state *mock, const uint8_t *sql, uint64_t sql_len,
    nullable_composite_capture *capture) {
    static const uint8_t schema[] = "app";
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[4096] = {0};
    uint8_t materialized_bytes[4096] = {0};
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v13(
        &host, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0, 6, 4096,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes),
        on_nullable_composite_group_v13, capture);
}

static int32_t run_query_v13_optimizer_internal(
    mock_state *mock, const uint8_t *sql, uint64_t sql_len,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint32_t filtered, uint64_t arena_capacity, group_capture *capture) {
    static const uint8_t schema[] = "app";
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    uint8_t optimizer_request_bytes[4096] = {0};
    uint8_t optimizer_response[4096] = {0};
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[512] = {0};
    uint8_t batch_response[4096] = {0};
    uint8_t materialized_bytes[4096] = {0};
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v5 host = {0};
    host.v4.v3 = (prdb_host_api_v3){
        sizeof(prdb_host_api_v3), PRDB_QUERY_ABI_V3_VERSION, mock,
        executor_request, planner_request, NULL, statement_failure};
    host.v4.optimizer_request = unused_optimizer_request;
    host.optimizer_request = optimizer_request;
    mock->optimizer_enabled = 0;
    if (filtered)
        return prdb_sdk_sql_execute_filtered_group_composite_aggregate_optimizer_v1(
            &host, schema, sizeof(schema) - 1, sql, sql_len, parameters,
            parameter_count,
            mock->max_candidates == 0 ? 4 : (uint16_t)mock->max_candidates,
            mock->max_candidates > 3 ? 4096 : 512, optimizer_request_bytes,
            sizeof(optimizer_request_bytes), optimizer_response,
            sizeof(optimizer_response), planner_response,
            sizeof(planner_response), executor_response,
            sizeof(executor_response), batch_response, sizeof(batch_response),
            planner_columns, 2, executor_columns, 2, cell_scratch, 2,
            materialized_bytes, arena_capacity, on_group_v13, capture);
    return prdb_sdk_sql_execute_group_composite_aggregate_optimizer_v1(
        &host, schema, sizeof(schema) - 1, sql, sql_len,
        mock->max_candidates == 0 ? 4 : (uint16_t)mock->max_candidates,
        mock->max_candidates > 3 ? 4096 : 512, optimizer_request_bytes,
        sizeof(optimizer_request_bytes), optimizer_response,
        sizeof(optimizer_response), planner_response,
        sizeof(planner_response), executor_response, sizeof(executor_response),
        batch_response, sizeof(batch_response), planner_columns, 2,
        executor_columns, 2, cell_scratch, 2, materialized_bytes,
        arena_capacity, on_group_v13, capture);
}

static int32_t run_query_v13_optimizer(mock_state *mock, const uint8_t *sql,
                                       uint64_t sql_len,
                                       uint64_t arena_capacity,
                                       group_capture *capture) {
    return run_query_v13_optimizer_internal(mock, sql, sql_len, NULL, 0, 0,
                                            arena_capacity, capture);
}

static int32_t run_ordered_eight_key_optimizer(
    mock_state *mock, const uint8_t *sql, uint64_t sql_len,
    group_capture *capture) {
    static const uint8_t schema[] = "app";
    static const uint8_t *names[9] = {
        (const uint8_t *)"key_0", (const uint8_t *)"key_1",
        (const uint8_t *)"key_2", (const uint8_t *)"key_3",
        (const uint8_t *)"key_4", (const uint8_t *)"key_5",
        (const uint8_t *)"key_6", (const uint8_t *)"key_7",
        (const uint8_t *)"measure"};
    prdb_sdk_sql_column_shape_v2 planner_columns[9] = {{0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[9] = {{0}};
    prdb_sdk_value_v1 cell_scratch[9] = {{0}};
    uint8_t optimizer_request_bytes[4096] = {0};
    uint8_t optimizer_response[4096] = {0};
    uint8_t planner_response[512] = {0};
    uint8_t executor_response[512] = {0};
    uint8_t batch_response[4096] = {0};
    uint8_t materialized_bytes[4096] = {0};
    prdb_host_api_v5 host = {0};
    for (uint32_t index = 0; index < 9; ++index) {
        planner_columns[index] = (prdb_sdk_sql_column_shape_v2){
            names[index], (uint32_t)strlen((const char *)names[index]), index,
            PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0};
        executor_columns[index] = planner_columns[index];
    }
    host.v4.v3 = (prdb_host_api_v3){
        sizeof(prdb_host_api_v3), PRDB_QUERY_ABI_V3_VERSION, mock,
        executor_request, planner_request, NULL, statement_failure};
    host.v4.optimizer_request = unused_optimizer_request;
    host.optimizer_request = optimizer_request;
    mock->optimizer_enabled = 0;
    int32_t result = prdb_sdk_sql_execute_ordered_composite_aggregate_optimizer_v1(
        &host, schema, sizeof(schema) - 1, sql, sql_len, 4, 4096,
        optimizer_request_bytes, sizeof(optimizer_request_bytes),
        optimizer_response, sizeof(optimizer_response), planner_response,
        sizeof(planner_response), executor_response, sizeof(executor_response),
        batch_response, sizeof(batch_response), planner_columns, 9,
        executor_columns, 9, cell_scratch, 9, materialized_bytes,
        sizeof(materialized_bytes), on_ordered_group_eight_keys, capture);
    return result;
}

static int32_t on_group_v14(void *context,
                            const prdb_sdk_value_v1 *group_keys,
                            uint32_t group_key_count,
                            const prdb_sdk_value_v1 *aggregate_value,
                            uint64_t matching_row_count,
                            const prdb_sdk_select_binding_v14 *binding) {
    group_capture *capture = (group_capture *)context;
    int32_t key = 0;
    int64_t sum = 0;
    (void)matching_row_count;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V14_VERSION);
    assert(binding->base.group_key_count == 2 && group_key_count == 2);
    assert(binding->has_having == 1 &&
           binding->having_parameter_index == 1);
    assert(capture->mock->batch_finished != 0);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &key) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(prdb_sdk_value_get_bigint_v1(aggregate_value, &sum) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    capture->keys[capture->calls] = key;
    capture->aggregates[capture->calls] = (int32_t)sum;
    ++capture->calls;
    return 0;
}

static int32_t run_query_v14(mock_state *mock, const uint8_t *sql,
                             uint64_t sql_len,
                             const prdb_sdk_value_v1 *parameter,
                             group_capture *capture) {
    static const uint8_t schema[] = "app";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[64] = {0};
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v14(
        &host, schema, sizeof(schema) - 1, sql, sql_len, parameter, 1, 4, 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes), on_group_v14, capture);
}

static int32_t on_group_v15(void *context,
                            const prdb_sdk_value_v1 *group_keys,
                            uint32_t group_key_count,
                            const prdb_sdk_value_v1 *aggregate_value,
                            uint64_t matching_row_count,
                            const prdb_sdk_select_binding_v15 *binding) {
    group_capture *capture = (group_capture *)context;
    int32_t first = 0, second = 0;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V15_VERSION);
    assert(binding->order_count == 3);
    assert(binding->order_items[0].type_code == PRDB_SDK_VALUE_TYPE_INTEGER);
    assert(binding->order_items[1].type_code == PRDB_SDK_VALUE_TYPE_BIGINT);
    assert(binding->order_items[2].type_code == PRDB_SDK_VALUE_TYPE_INTEGER);
    assert(group_key_count == 2 && aggregate_value == NULL);
    assert(capture->mock->batch_finished != 0);
    assert(capture->mock->executor_calls == 3);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &first) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[1], &second) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    capture->keys[capture->calls] = first;
    capture->second_keys[capture->calls] = second;
    capture->row_counts[capture->calls] = matching_row_count;
    ++capture->calls;
    return 0;
}

static int32_t run_query_v15(mock_state *mock, const uint8_t *sql,
                             uint64_t sql_len,
                             const prdb_sdk_value_v1 *parameter,
                             group_capture *capture) {
    static const uint8_t schema[] = "app";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[64] = {0};
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v15(
        &host, schema, sizeof(schema) - 1, sql, sql_len, parameter, 1, 4, 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes), on_group_v15, capture);
}

static int32_t on_group_v17(void *context,
                            const prdb_sdk_value_v1 *group_keys,
                            uint32_t group_key_count,
                            const prdb_sdk_value_v1 *aggregate_value,
                            uint64_t matching_row_count,
                            const prdb_sdk_select_binding_v17 *binding) {
    group_capture *capture = (group_capture *)context;
    int32_t key = 0;
    int64_t sum = 0;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V17_VERSION);
    assert(binding->has_having_literal == capture->expect_having_literal);
    assert(binding->having_literal ==
           (capture->expect_having_literal ? UINT64_C(10) : UINT64_C(0)));
    assert(binding->base.base.has_having == 1);
    assert(binding->base.base.having_parameter_index == 1);
    assert(binding->base.base.base.group_key_count == 2 &&
           group_key_count == 2);
    assert(aggregate_value != NULL && matching_row_count == 2);
    assert(capture->mock->batch_finished != 0);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &key) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(prdb_sdk_value_get_bigint_v1(aggregate_value, &sum) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    capture->keys[capture->calls] = key;
    capture->aggregates[capture->calls] = (int32_t)sum;
    ++capture->calls;
    return 0;
}

static int32_t run_query_v17(mock_state *mock, const uint8_t *sql,
                             uint64_t sql_len,
                             const prdb_sdk_value_v1 *parameter,
                             group_capture *capture) {
    static const uint8_t schema[] = "app";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[64] = {0};
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v17(
        &host, schema, sizeof(schema) - 1, sql, sql_len, parameter,
        parameter == NULL ? 0 : 1, 4, 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes), on_group_v17, capture);
}

static int32_t on_group_v18(void *context,
                            const prdb_sdk_value_v1 *group_keys,
                            uint32_t group_key_count,
                            const prdb_sdk_value_v1 *aggregate_value,
                            uint64_t matching_row_count,
                            const prdb_sdk_select_binding_v18 *binding) {
    group_capture *capture = (group_capture *)context;
    int32_t key = 0, second_key = 0;
    int64_t sum = 0;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V18_VERSION);
    assert(binding->having_node_count ==
           (capture->expect_having_literal ? 1u : 6u));
    assert(binding->having_root == binding->having_node_count - 1);
    assert(binding->having_type_code == PRDB_SDK_VALUE_TYPE_BIGINT);
    assert(binding->base.base.has_having == 0);
    assert(binding->base.base.base.group_key_count == 2 &&
           group_key_count == 2);
    assert(aggregate_value != NULL &&
           matching_row_count ==
               (capture->expected_row_count == 0
                    ? UINT64_C(1)
                    : capture->expected_row_count));
    assert(capture->mock->batch_finished != 0);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &key) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[1], &second_key) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(prdb_sdk_value_get_bigint_v1(aggregate_value, &sum) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    capture->keys[capture->calls] = key;
    capture->second_keys[capture->calls] = second_key;
    capture->aggregates[capture->calls] = (int32_t)sum;
    ++capture->calls;
    return 0;
}

static int32_t run_query_v18(mock_state *mock, const uint8_t *sql,
                             uint64_t sql_len,
                             const prdb_sdk_value_v1 *parameter,
                             group_capture *capture) {
    static const uint8_t schema[] = "app";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[2048] = {0};
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v18(
        &host, schema, sizeof(schema) - 1, sql, sql_len, parameter,
        parameter == NULL ? 0 : 1,
        mock->max_candidates == 0 ? 4 : (uint16_t)mock->max_candidates, 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes), on_group_v18, capture);
}

static int32_t on_group_v21_decimal(
    void *context, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v21 *binding) {
    group_capture *capture = (group_capture *)context;
    int32_t key = 0;
    uint64_t aggregate_bits = 0;
    int64_t aggregate_coefficient;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V21_VERSION);
    assert(binding->base.having_type_code == PRDB_SDK_VALUE_TYPE_DECIMAL);
    assert(binding->base.having_node_count == 1);
    assert(binding->having_nodes[0].base.operand_kind ==
           PRDB_SDK_SQL_V21_OPERAND_NUMERIC_LITERAL);
    if (binding->base.having_decimal_scale == 2) {
        assert(binding->having_nodes[0].scale == 2);
        assert(binding->having_nodes[0].coefficient[14] == 1 &&
               binding->having_nodes[0].coefficient[15] == 0xc2);
    } else {
        assert(binding->base.having_decimal_scale == 8);
        assert(binding->having_nodes[0].scale == 8);
        assert(binding->having_nodes[0].coefficient[12] == 0x08 &&
               binding->having_nodes[0].coefficient[13] == 0x58 &&
               binding->having_nodes[0].coefficient[14] == 0x3b &&
               binding->having_nodes[0].coefficient[15] == 0x00);
    }
    assert(group_key_count == 2 && aggregate_value != NULL);
    assert(matching_row_count == 1 && capture->mock->batch_finished != 0);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &key) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
           aggregate_value->data_len == 16);
    for (uint32_t index = 0; index < 8; ++index)
        assert(aggregate_value->data[index] == 0);
    for (uint32_t index = 0; index < 8; ++index)
        aggregate_bits = (aggregate_bits << 8) | aggregate_value->data[index + 8];
    aggregate_coefficient = (int64_t)aggregate_bits;
    capture->keys[capture->calls] = key;
    capture->aggregates[capture->calls] = binding->base.having_decimal_scale == 2
                                              ? (int32_t)(aggregate_coefficient / 100)
                                              : (int32_t)(aggregate_coefficient /
                                                          1000000);
    ++capture->calls;
    if (capture->mock->cancel_after_group_callbacks == capture->calls)
        capture->mock->cancelled = 1;
    return 0;
}

static int32_t on_group_v22_null_sum(
    void *context, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v21 *binding) {
    group_capture *capture = (group_capture *)context;
    int32_t key = 0;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V21_VERSION);
    assert(binding->base.having_type_code == PRDB_SDK_VALUE_TYPE_DECIMAL);
    assert(binding->base.having_node_count == 3);
    assert(binding->having_nodes[0].base.node_kind ==
           PRDB_SDK_SQL_PREDICATE_COMPARE);
    assert(binding->having_nodes[0].base.operand_kind ==
        PRDB_SDK_SQL_V21_OPERAND_NUMERIC_LITERAL);
    assert(binding->having_nodes[0].scale == 0 &&
           binding->having_nodes[0].coefficient[15] == 2);
    assert(binding->having_nodes[1].base.operand_kind ==
           PRDB_SDK_SQL_V21_OPERAND_NUMERIC_LITERAL);
    assert(binding->having_nodes[1].scale == 2 &&
           binding->having_nodes[1].coefficient[14] == 1 &&
           binding->having_nodes[1].coefficient[15] == 0xc2);
    assert(group_key_count == 2 && matching_row_count == 2);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &key) ==
           PRDB_SDK_VALUE_ACCESS_OK && key == 1);
    assert(group_keys[1].state == PRDB_SDK_VALUE_STATE_NULL);
    assert(aggregate_value != NULL &&
           aggregate_value->state == PRDB_SDK_VALUE_STATE_NULL &&
           aggregate_value->type_code == 0);
    capture->keys[capture->calls] = key;
    capture->aggregates[capture->calls] = -1;
    ++capture->calls;
    return 0;
}

static int32_t on_group_v22_hidden_count_avg(
    void *context, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v21 *binding) {
    group_capture *capture = (group_capture *)context;
    int32_t key = 0;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V21_VERSION);
    assert(binding->base.having_type_code == PRDB_SDK_VALUE_TYPE_DECIMAL);
    assert(binding->base.having_decimal_scale == 8);
    assert(group_key_count == 2 && matching_row_count == 2);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &key) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(key == 2 && group_keys[1].state == PRDB_SDK_VALUE_STATE_VALUE &&
           group_keys[1].type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
           group_keys[1].data_len == 16 && group_keys[1].data[14] == 1 &&
           group_keys[1].data[15] == 44);
    assert(aggregate_value != NULL &&
           aggregate_value->state == PRDB_SDK_VALUE_STATE_VALUE &&
           aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
           aggregate_value->data_len == 16);
    for (uint32_t index = 0; index < 12; ++index)
        assert(aggregate_value->data[index] == 0);
    assert(aggregate_value->data[12] == 0x11 &&
           aggregate_value->data[13] == 0xe1 &&
           aggregate_value->data[14] == 0xa3 &&
           aggregate_value->data[15] == 0x00);
    capture->keys[capture->calls] = key;
    capture->aggregates[capture->calls] = 3;
    ++capture->calls;
    return 0;
}

static int32_t on_group_v22_hidden_count_minmax(
    void *context, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v21 *binding) {
    group_capture *capture = (group_capture *)context;
    int32_t key = 0;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V21_VERSION);
    assert(binding->base.having_type_code == PRDB_SDK_VALUE_TYPE_DECIMAL);
    assert(binding->base.having_decimal_scale == 2);
    assert(group_key_count == 2 && matching_row_count == 2);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &key) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(key == 2 && group_keys[1].state == PRDB_SDK_VALUE_STATE_VALUE &&
           group_keys[1].type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
           group_keys[1].data_len == 16 && group_keys[1].data[14] == 1 &&
           group_keys[1].data[15] == 44);
    assert(aggregate_value != NULL &&
           aggregate_value->state == PRDB_SDK_VALUE_STATE_VALUE &&
           aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
           aggregate_value->data_len == 16);
    for (uint32_t index = 0; index < 14; ++index)
        assert(aggregate_value->data[index] == 0);
    assert(aggregate_value->data[14] == 1 &&
           aggregate_value->data[15] == 44);
    capture->keys[capture->calls] = key;
    capture->aggregates[capture->calls] = 3;
    ++capture->calls;
    return 0;
}

static int32_t on_group_v22_where_and_having(
    void *context, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v21 *binding) {
    group_capture *capture = (group_capture *)context;
    int32_t id = 0;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V21_VERSION);
    assert(binding->base.having_node_count == 3);
    assert(binding->base.having_decimal_scale == 2);
    assert(group_key_count == 2 && matching_row_count == 1);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0], &id) ==
           PRDB_SDK_VALUE_ACCESS_OK && id == 2);
    assert(group_keys[1].state == PRDB_SDK_VALUE_STATE_VALUE &&
           group_keys[1].type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
           group_keys[1].data_len == 16 && group_keys[1].data[14] == 1 &&
           group_keys[1].data[15] == 0xf4);
    assert(aggregate_value != NULL &&
           aggregate_value->state == PRDB_SDK_VALUE_STATE_VALUE &&
           aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
           aggregate_value->data_len == 16 && aggregate_value->data[14] == 1 &&
           aggregate_value->data[15] == 0xf4);
    capture->keys[capture->calls] = id;
    capture->aggregates[capture->calls] = 5;
    capture->row_counts[capture->calls] = matching_row_count;
    ++capture->calls;
    return 0;
}

static int32_t on_group_v21_min_decimal(
    void *context, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v21 *binding) {
    group_capture *capture = (group_capture *)context;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V21_VERSION);
    assert(binding->base.having_type_code == PRDB_SDK_VALUE_TYPE_DECIMAL);
    assert(binding->base.having_decimal_scale == 2);
    assert(binding->base.having_node_count == 1);
    assert(binding->having_nodes[0].scale == 2);
    assert(binding->having_nodes[0].base.operand_kind ==
           PRDB_SDK_SQL_V21_OPERAND_NUMERIC_LITERAL);
    assert(binding->having_nodes[0].coefficient[14] == 1 &&
           binding->having_nodes[0].coefficient[15] == 44);
    assert(group_key_count == 2 && aggregate_value != NULL);
    assert(matching_row_count == 1 && capture->mock->batch_finished != 0);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0],
                                         &capture->keys[capture->calls]) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(group_keys[1].type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
           group_keys[1].data_len == 16 && group_keys[1].data[14] == 1 &&
           group_keys[1].data[15] == 44);
    assert(aggregate_value->state == PRDB_SDK_VALUE_STATE_VALUE &&
           aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
           aggregate_value->data_len == 16);
    for (uint32_t index = 0; index < 14; ++index)
        assert(aggregate_value->data[index] == 0);
    assert(aggregate_value->data[14] == 1 &&
           aggregate_value->data[15] == 44);
    capture->aggregates[capture->calls] = 300;
    ++capture->calls;
    return 0;
}

static int32_t on_group_v21_max_decimal(
    void *context, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v21 *binding) {
    group_capture *capture = (group_capture *)context;
    assert(binding->abi_version == PRDB_SDK_SQL_ABI_V21_VERSION);
    assert(binding->base.having_type_code == PRDB_SDK_VALUE_TYPE_DECIMAL);
    assert(binding->base.having_decimal_precision == 10);
    assert(binding->base.having_decimal_scale == 2);
    assert(binding->base.having_node_count == 1);
    assert(binding->having_nodes[0].scale == 2);
    assert(binding->having_nodes[0].base.operand_kind ==
           PRDB_SDK_SQL_V21_OPERAND_NUMERIC_LITERAL);
    assert(binding->having_nodes[0].coefficient[14] == 3 &&
           binding->having_nodes[0].coefficient[15] == 32);
    assert(group_key_count == 2 && aggregate_value != NULL);
    assert(matching_row_count == 1 && capture->mock->batch_finished != 0);
    assert(prdb_sdk_value_get_integer_v1(&group_keys[0],
                                         &capture->keys[capture->calls]) ==
           PRDB_SDK_VALUE_ACCESS_OK);
    assert(group_keys[1].type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
           group_keys[1].data_len == 16 && group_keys[1].data[14] == 3 &&
           group_keys[1].data[15] == 132);
    assert(aggregate_value->state == PRDB_SDK_VALUE_STATE_VALUE &&
           aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
           aggregate_value->data_len == 16);
    for (uint32_t index = 0; index < 14; ++index)
        assert(aggregate_value->data[index] == 0);
    assert(aggregate_value->data[14] == 3 &&
           aggregate_value->data[15] == 132);
    capture->aggregates[capture->calls] = 900;
    ++capture->calls;
    return 0;
}

static int32_t run_query_v21_decimal(mock_state *mock, const uint8_t *sql,
                                     uint64_t sql_len,
                                     group_capture *capture) {
    static const uint8_t schema[] = "app";
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[8192] = {0};
    uint8_t materialized_bytes[4096] = {0};
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 10, 2}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 10, 2}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v21(
        &host, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0,
        mock->max_candidates == 0 ? 4 : (uint16_t)mock->max_candidates, 8192,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes), on_group_v21_decimal,
        capture);
}

static int32_t run_query_v22_decimal(mock_state *mock, const uint8_t *sql,
                                     uint64_t sql_len,
                                     const prdb_sdk_value_v1 *parameters,
                                     uint32_t parameter_count,
                                     prdb_sdk_sql_group_row_v22_fn on_group,
                                     group_capture *capture) {
    static const uint8_t schema[] = "app";
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[8192] = {0};
    uint8_t materialized_bytes[4096] = {0};
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 10, 2}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 10, 2}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v22(
        &host, schema, sizeof(schema) - 1, sql, sql_len, parameters,
        parameter_count,
        mock->max_candidates == 0 ? 4 : (uint16_t)mock->max_candidates, 8192,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes), on_group, capture);
}

static int32_t on_group_v23_null_test(
    void *context, const prdb_sdk_value_v1 *keys, uint32_t key_count,
    const prdb_sdk_value_v1 *aggregate, uint64_t rows,
    const prdb_sdk_select_binding_v21 *binding) {
    group_capture *capture = context;
    assert(key_count == 2 && capture->mock->batch_finished);
    assert(binding != NULL && rows == 2);
    assert(prdb_sdk_value_get_integer_v1(&keys[0],
        &capture->keys[capture->calls]) == PRDB_SDK_VALUE_ACCESS_OK);
    capture->aggregates[capture->calls] = aggregate == NULL ? 99 :
        (aggregate->state == PRDB_SDK_VALUE_STATE_NULL ? -1 : 1);
    ++capture->calls;
    return 0;
}

static int32_t run_query_v23_decimal(mock_state *mock, const uint8_t *sql,
                                     uint64_t sql_len,
                                     const prdb_sdk_value_v1 *parameters,
                                     uint32_t parameter_count,
                                     prdb_sdk_sql_group_row_v23_fn on_group,
                                     group_capture *capture) {
    static const uint8_t schema[] = "app";
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[8192] = {0};
    uint8_t materialized_bytes[4096] = {0};
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 10, 2}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 10, 2}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v23(
        &host, schema, sizeof(schema) - 1, sql, sql_len, parameters,
        parameter_count,
        mock->max_candidates == 0 ? 4 : (uint16_t)mock->max_candidates, 8192,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes), on_group, capture);
}

static int32_t run_query_v21_decimal_wide_sum(
    mock_state *mock, const uint8_t *sql, uint64_t sql_len,
    group_capture *capture) {
    static const uint8_t schema[] = "app";
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[2048] = {0};
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 38, 2}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 38, 2}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v21(
        &host, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0, 4, 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes), on_group_v21_decimal,
        capture);
}

static int32_t on_group_v7_integer_sum_unexpected(
    void *context, const prdb_sdk_value_v1 *group_key,
    const prdb_sdk_value_v1 *aggregate_value, uint64_t matching_row_count,
    const prdb_sdk_select_binding_v7 *binding) {
    (void)context;
    (void)group_key;
    (void)aggregate_value;
    (void)matching_row_count;
    (void)binding;
    assert(!"overflowing BIGINT SUM must not publish a group");
    return 1;
}

static int32_t run_query_v7_integer_sum(
    mock_state *mock, const uint8_t *sql, uint64_t sql_len,
    group_capture *capture) {
    static const uint8_t schema[] = "app";
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[2048] = {0};
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_BIGINT, 0, 0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_BIGINT, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v7(
        &host, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0, 4, 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes),
        on_group_v7_integer_sum_unexpected, capture);
}

static int32_t on_group_v21_integer_sum_unexpected(
    void *context, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v21 *binding) {
    (void)context;
    (void)group_keys;
    (void)group_key_count;
    (void)aggregate_value;
    (void)matching_row_count;
    (void)binding;
    assert(!"overflowing BIGINT SUM must not publish a group");
    return 1;
}

static int32_t on_group_v21_integer_sum_literal(
    void *context, const prdb_sdk_value_v1 *group_keys,
    uint32_t group_key_count, const prdb_sdk_value_v1 *aggregate_value,
    uint64_t matching_row_count,
    const prdb_sdk_select_binding_v21 *binding) {
    group_capture *capture = (group_capture *)context;
    int32_t id = 0;
    int64_t score = 0;
    int64_t sum = 0;
    uint32_t index = capture->calls;
    assert(index < 2 && binding->abi_version == PRDB_SDK_SQL_ABI_V21_VERSION);
    assert(binding->base.having_type_code == PRDB_SDK_VALUE_TYPE_BIGINT &&
           binding->base.having_node_count == 1 &&
           binding->having_nodes[0].base.operand_kind ==
               PRDB_SDK_SQL_V21_OPERAND_NUMERIC_LITERAL &&
           binding->having_nodes[0].coefficient[15] == 6 &&
           binding->having_nodes[0].scale == 0);
    assert(group_key_count == 2 &&
           prdb_sdk_value_get_integer_v1(&group_keys[0], &id) ==
               PRDB_SDK_VALUE_ACCESS_OK &&
           prdb_sdk_value_get_bigint_v1(&group_keys[1], &score) ==
               PRDB_SDK_VALUE_ACCESS_OK);
    assert(id == (index == 0 ? 1 : 2) &&
           score == (index == 0 ? 3 : 6));
    assert(matching_row_count == (index == 0 ? 2 : 1) &&
           aggregate_value != NULL &&
           aggregate_value->type_code == PRDB_SDK_VALUE_TYPE_BIGINT &&
           prdb_sdk_value_get_bigint_v1(aggregate_value, &sum) ==
               PRDB_SDK_VALUE_ACCESS_OK &&
           sum == 6);
    ++capture->calls;
    return 0;
}

static int32_t run_query_v21_integer_sum(
    mock_state *mock, const uint8_t *sql, uint64_t sql_len,
    prdb_sdk_sql_group_row_v21_fn on_group, void *capture) {
    static const uint8_t schema[] = "app";
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[2048] = {0};
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_BIGINT, 0, 0}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_BIGINT, 0, 0}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v21(
        &host, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0, 4, 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes),
        on_group, capture);
}

static int32_t run_query_v21_min_decimal(mock_state *mock, const uint8_t *sql,
                                         uint64_t sql_len,
                                         group_capture *capture) {
    static const uint8_t schema[] = "app";
    static const uint8_t id_name[] = "id";
    static const uint8_t score_name[] = "score";
    uint8_t planner_response[256] = {0};
    uint8_t executor_response[256] = {0};
    uint8_t batch_response[512] = {0};
    uint8_t materialized_bytes[2048] = {0};
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 10, 2}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {id_name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 10, 2}};
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v21(
        &host, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0, 4, 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes),
        on_group_v21_min_decimal, capture);
}

static int32_t run_query_v21_max_decimal(mock_state *mock, const uint8_t *sql,
                                         uint64_t sql_len,
                                         group_capture *capture) {
    static const uint8_t schema[] = "app";
    static const uint8_t name[] = "id";
    static const uint8_t score_name[] = "score";
    prdb_sdk_sql_column_shape_v2 planner_columns[2] = {
        {name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 4, 2}};
    prdb_sdk_sql_column_shape_v2 executor_columns[2] = {
        {name, 2, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
        {score_name, 5, 1, PRDB_SDK_VALUE_TYPE_DECIMAL, 4, 2}};
    uint8_t planner_response[512];
    uint8_t executor_response[512];
    uint8_t batch_response[512];
    uint8_t materialized_bytes[4096];
    prdb_sdk_value_v1 cell_scratch[2] = {{0}};
    prdb_host_api_v3 host = {sizeof(host), PRDB_QUERY_ABI_V3_VERSION,
                             mock, executor_request, planner_request, NULL,
                             statement_failure};
    return prdb_sdk_sql_execute_group_v21(
        &host, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0, 4, 512,
        planner_response, sizeof(planner_response), executor_response,
        sizeof(executor_response), batch_response, sizeof(batch_response),
        planner_columns, 2, executor_columns, 2, cell_scratch, 2,
        materialized_bytes, sizeof(materialized_bytes),
        on_group_v21_max_decimal, capture);
}

static void reset_scan(mock_state *mock, group_capture *capture,
                       uint32_t finished) {
    mock->executor_calls = 0;
    mock->optimizer_relation_resolves = 0;
    mock->executor_requests = 0;
    mock->next_calls = 0;
    mock->statement_failed = 0;
    mock->cancelled = 0;
    mock->cancel_after_first_batch = 0;
    mock->batch_finished = finished;
    capture->calls = 0;
}

int main(void) {
    static const uint8_t min_sql[] =
        "SELECT id, MIN(score) FROM widgets GROUP BY id";
    static const uint8_t max_sql[] =
        "SELECT id, MAX(score) FROM widgets GROUP BY id";
    static const uint8_t sum_sql[] =
        "SELECT id, SUM(score) FROM widgets GROUP BY id";
    static const uint8_t avg_sql[] =
        "SELECT id, AVG(score) FROM widgets GROUP BY id";
    static const uint8_t limit_sql[] =
        "SELECT id, MIN(score) FROM widgets GROUP BY id LIMIT 1";
    static const uint8_t where_sql[] =
        "SELECT id, MIN(score) FROM widgets WHERE score >= $1 GROUP BY id";
    static const uint8_t having_sql[] =
        "SELECT id, COUNT(*) FROM widgets GROUP BY id "
        "HAVING COUNT(*) = $1 LIMIT 1";
    static const uint8_t ordered_having_sql[] =
        "SELECT id, COUNT(*) FROM widgets GROUP BY id "
        "HAVING COUNT(*) >= $1 ORDER BY COUNT(*) ASC LIMIT 1";
    static const uint8_t ordered_key_sql[] =
        "SELECT id, COUNT(*) FROM widgets GROUP BY id "
        "ORDER BY id DESC LIMIT 1";
    static const uint8_t ordered_aggregate_sql[] =
        "SELECT id, MIN(score) FROM widgets GROUP BY id "
        "ORDER BY MIN(score) DESC LIMIT 1";
    static const uint8_t nulls_last_sql[] =
        "SELECT id, MIN(score) FROM widgets GROUP BY id "
        "ORDER BY MIN(score) ASC NULLS LAST";
    static const uint8_t default_null_order_sql[] =
        "SELECT id, MIN(score) FROM widgets GROUP BY id "
        "ORDER BY MIN(score) DESC";
    static const uint8_t composite_order_sql[] =
        "SELECT id, COUNT(*) FROM widgets GROUP BY id "
        "ORDER BY COUNT(*) ASC, id DESC NULLS LAST LIMIT 2";
    static const uint8_t ordered_group_limit_sql[] =
        "SELECT id, COUNT(*) FROM widgets GROUP BY id "
        "ORDER BY COUNT(*) DESC, id ASC LIMIT 128";
    static const uint8_t ordered_group_text_budget_sql[] =
        "SELECT id, COUNT(*) FROM widgets GROUP BY id "
        "ORDER BY COUNT(*) DESC, id ASC LIMIT 128";
    static const uint8_t ordered_grouped_sum_tie_sql[] =
        "SELECT id, SUM(score) FROM widgets GROUP BY id "
        "ORDER BY SUM(score) DESC NULLS LAST, id ASC NULLS FIRST";
    static const uint8_t *const ordered_aggregate_matrix_sql[] = {
        (const uint8_t *)"SELECT id, COUNT(*) FROM widgets GROUP BY id "
                         "ORDER BY COUNT(*) DESC NULLS LAST, id ASC NULLS FIRST",
        (const uint8_t *)"SELECT id, MIN(score) FROM widgets GROUP BY id "
                         "ORDER BY MIN(score) DESC NULLS LAST, id ASC NULLS FIRST",
        (const uint8_t *)"SELECT id, MAX(score) FROM widgets GROUP BY id "
                         "ORDER BY MAX(score) DESC NULLS LAST, id ASC NULLS FIRST",
        (const uint8_t *)"SELECT id, SUM(score) FROM widgets GROUP BY id "
                         "ORDER BY SUM(score) DESC NULLS LAST, id ASC NULLS FIRST",
        (const uint8_t *)"SELECT id, AVG(score) FROM widgets GROUP BY id "
                         "ORDER BY AVG(score) DESC NULLS LAST, id ASC NULLS FIRST",
    };
    static const uint32_t ordered_aggregate_matrix_sql_len[] = {
        sizeof("SELECT id, COUNT(*) FROM widgets GROUP BY id "
               "ORDER BY COUNT(*) DESC NULLS LAST, id ASC NULLS FIRST") - 1,
        sizeof("SELECT id, MIN(score) FROM widgets GROUP BY id "
               "ORDER BY MIN(score) DESC NULLS LAST, id ASC NULLS FIRST") - 1,
        sizeof("SELECT id, MAX(score) FROM widgets GROUP BY id "
               "ORDER BY MAX(score) DESC NULLS LAST, id ASC NULLS FIRST") - 1,
        sizeof("SELECT id, SUM(score) FROM widgets GROUP BY id "
               "ORDER BY SUM(score) DESC NULLS LAST, id ASC NULLS FIRST") - 1,
        sizeof("SELECT id, AVG(score) FROM widgets GROUP BY id "
               "ORDER BY AVG(score) DESC NULLS LAST, id ASC NULLS FIRST") - 1,
    };
    static const uint8_t v12_no_order_sql[] =
        "SELECT id, COUNT(*) FROM widgets GROUP BY id";
    static const uint8_t v13_composite_sql[] =
        "SELECT id, score, COUNT(*) FROM widgets GROUP BY id, score";
    static const uint8_t v13_nullable_composite_sql[] =
        "SELECT id, score, COUNT(*) FROM widgets GROUP BY id, score";
    static const uint8_t v13_sum_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score";
    static const uint8_t v13_min_sql[] =
        "SELECT id, score, MIN(score) FROM widgets GROUP BY id, score";
    static const uint8_t v13_max_sql[] =
        "SELECT id, score, MAX(score) FROM widgets GROUP BY id, score";
    static const uint8_t v13_avg_sql[] =
        "SELECT id, score, AVG(score) FROM widgets GROUP BY id, score";
    static const uint8_t *const v13_optimizer_sql[] = {
        v13_composite_sql, v13_min_sql, v13_max_sql, v13_sum_sql,
        v13_avg_sql};
    static const uint32_t v13_optimizer_sql_lens[] = {
        sizeof(v13_composite_sql) - 1, sizeof(v13_min_sql) - 1,
        sizeof(v13_max_sql) - 1, sizeof(v13_sum_sql) - 1,
        sizeof(v13_avg_sql) - 1};
    static const uint8_t v14_having_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING SUM(score) >= $1";
    static const uint8_t v15_order_sql[] =
        "SELECT id, score, COUNT(*) FROM widgets GROUP BY id, score "
        "HAVING COUNT(*) >= $1 ORDER BY score DESC NULLS LAST, "
        "COUNT(*) DESC, id ASC NULLS LAST LIMIT 3";
    static const uint8_t v17_literal_having_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING SUM(score) >= 10 ORDER BY score DESC NULLS LAST, "
        "SUM(score) DESC, id ASC NULLS LAST LIMIT 3";
    static const uint8_t v17_parameter_having_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING SUM(score) >= $1 ORDER BY score DESC NULLS LAST, "
        "SUM(score) DESC, id ASC NULLS LAST LIMIT 3";
    static const uint8_t v17_overflow_having_sql[] =
        "SELECT id, score, COUNT(*) FROM widgets GROUP BY id, score "
        "HAVING COUNT(*) > 9223372036854775808";
    static const uint8_t v18_compound_having_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING (SUM(score) >= 9 OR SUM(score) = $1) AND "
        "NOT SUM(score) < 3 ORDER BY score DESC LIMIT 2";
    static const uint8_t v18_negative_literal_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING SUM(score) >= -1";
    static const uint8_t v21_decimal_literal_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING SUM(score) >= 4.50e0";
    static const uint8_t bigint_sum_overflow_sql[] =
        "SELECT id, SUM(score) FROM widgets GROUP BY id";
    static const uint8_t v21_bigint_sum_literal_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING SUM(score) >= 6";
    static const uint8_t v21_bigint_sum_overflow_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING SUM(score) >= 0";
    static const uint8_t v22_hidden_count_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING COUNT(*) >= 2 AND SUM(score) >= 4.50e0";
    static const uint8_t v22_where_and_having_sql[] =
        "SELECT id, score, SUM(score) FROM widgets WHERE id >= $1 "
        "GROUP BY id, score "
        "HAVING COUNT(*) >= 1 AND SUM(score) >= 5.00";
    static const uint8_t v22_hidden_count_avg_sql[] =
        "SELECT id, score, AVG(score) FROM widgets GROUP BY id, score "
        "HAVING COUNT(*) >= 2 AND AVG(score) >= 3.00000000e0";
    static const uint8_t v22_hidden_count_min_sql[] =
        "SELECT id, score, MIN(score) FROM widgets GROUP BY id, score "
        "HAVING COUNT(*) >= 2 AND MIN(score) >= 3.00e0";
    static const uint8_t v22_hidden_count_max_sql[] =
        "SELECT id, score, MAX(score) FROM widgets GROUP BY id, score "
        "HAVING COUNT(*) >= 2 AND MAX(score) <= 3.00e0";
    static const uint8_t v22_null_count_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING COUNT(*) >= 2 OR SUM(score) >= 4.50e0";
    static const uint8_t v22_parameter_count_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING COUNT(*) >= $1 AND SUM(score) >= 4.50e0";
    static const uint8_t v21_decimal_equal_literal_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING SUM(score) = 4.50e0";
    static const uint8_t v21_decimal_avg_literal_sql[] =
        "SELECT id, score, AVG(score) FROM widgets GROUP BY id, score "
        "HAVING AVG(score) >= 1.40000000e0";
    static const uint8_t v21_decimal_min_literal_sql[] =
        "SELECT id, score, MIN(score) FROM widgets GROUP BY id, score "
        "HAVING MIN(score) <= 3.00e0 ORDER BY id ASC LIMIT 1";
    static const uint8_t v21_decimal_max_literal_sql[] =
        "SELECT id, score, MAX(score) FROM widgets GROUP BY id, score "
        "HAVING MAX(score) >= 8.00e0 ORDER BY id ASC LIMIT 1";
    static const uint8_t v21_decimal_sum_overflow_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING SUM(score) >= 0.00e0";
    static const uint8_t v21_decimal_inexact_scale_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING SUM(score) >= 1.251";
    static const uint8_t v21_decimal_overflow_literal_sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING SUM(score) >= 1e36";
    static const uint8_t v13_many_groups_sql[] =
        "SELECT id, score, COUNT(*) FROM widgets GROUP BY id, score";
    static const uint8_t parameter_bytes[] = {0, 0, 0, 5};
    static const uint8_t having_parameter_bytes[] = {
        0, 0, 0, 0, 0, 0, 0, 1};
    static const uint8_t v14_having_parameter_bytes[] = {
        0, 0, 0, 0, 0, 0, 0, 6};
    static const uint8_t v17_having_parameter_bytes[] = {
        0, 0, 0, 0, 0, 0, 0, 10};
    static const uint8_t v18_having_parameter_bytes[] = {
        0, 0, 0, 0, 0, 0, 0, 3};
    static const uint8_t v22_where_parameter_bytes[] = {0, 0, 0, 2};
    const int32_t keys[] = {1, 1, 2};
    const int32_t scores[] = {9, 3, 5};
    const uint8_t value_states[] = {
        PRDB_SDK_VALUE_STATE_VALUE,
        PRDB_SDK_VALUE_STATE_VALUE,
        PRDB_SDK_VALUE_STATE_VALUE};
    mock_state mock = {0};
    group_capture capture = {0};
    prdb_sdk_value_v1 parameter = {
        sizeof(parameter), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
        parameter_bytes, sizeof(parameter_bytes)};
    prdb_sdk_value_v1 having_parameter = {
        sizeof(having_parameter), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BIGINT,
        having_parameter_bytes, sizeof(having_parameter_bytes)};
    prdb_sdk_value_v1 v14_having_parameter = {
        sizeof(v14_having_parameter), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BIGINT,
        v14_having_parameter_bytes, sizeof(v14_having_parameter_bytes)};
    prdb_sdk_value_v1 v15_having_parameter = {
        sizeof(v15_having_parameter), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BIGINT,
        having_parameter_bytes, sizeof(having_parameter_bytes)};
    prdb_sdk_value_v1 v17_having_parameter = {
        sizeof(v17_having_parameter), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BIGINT,
        v17_having_parameter_bytes, sizeof(v17_having_parameter_bytes)};
    prdb_sdk_value_v1 v18_having_parameter = {
        sizeof(v18_having_parameter), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BIGINT,
        v18_having_parameter_bytes, sizeof(v18_having_parameter_bytes)};
    prdb_sdk_value_v1 v22_where_parameter = {
        sizeof(v22_where_parameter), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
        v22_where_parameter_bytes, sizeof(v22_where_parameter_bytes)};
    mock.planner_len = make_relation(
        mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
        PRDB_SDK_VALUE_TYPE_INTEGER);
    mock.executor_len = make_relation(mock.executor, 6, 0x22,
                                      PRDB_SDK_VALUE_TYPE_INTEGER);
    mock.batch_len = make_rows(mock.batch, 1, keys, scores, value_states, 3);
    mock.batch_finished = 1;
    capture.mock = &mock;

    assert(run_query(&mock, min_sql, sizeof(min_sql) - 1, NULL, 0, 64,
                     &capture) == PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 2 && capture.keys[0] == 1 &&
           capture.aggregates[0] == 3 && capture.row_counts[0] == 2 &&
           capture.keys[1] == 2 && capture.aggregates[1] == 5 &&
           capture.row_counts[1] == 1 && mock.statement_failed == 0);

    reset_scan(&mock, &capture, 1);
    assert(run_query(&mock, max_sql, sizeof(max_sql) - 1, NULL, 0, 64,
                     &capture) == PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 2 && capture.aggregates[0] == 9 &&
           capture.aggregates[1] == 5);

    reset_scan(&mock, &capture, 1);
    assert(run_query(&mock, sum_sql, sizeof(sum_sql) - 1, NULL, 0, 64,
                     &capture) == PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 2 && capture.keys[0] == 1 &&
           capture.aggregates[0] == 12 && capture.row_counts[0] == 2 &&
           capture.keys[1] == 2 && capture.aggregates[1] == 5 &&
           capture.row_counts[1] == 1);

    reset_scan(&mock, &capture, 1);
    assert(run_query(&mock, avg_sql, sizeof(avg_sql) - 1, NULL, 0, 64,
                     &capture) == PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 2 && capture.keys[0] == 1 &&
           capture.aggregates[0] == 6000000 && capture.row_counts[0] == 2 &&
           capture.keys[1] == 2 && capture.aggregates[1] == 5000000 &&
           capture.row_counts[1] == 1);

    reset_scan(&mock, &capture, 1);
    assert(run_query(&mock, limit_sql, sizeof(limit_sql) - 1, NULL, 0, 64,
                     &capture) == PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 1 && capture.keys[0] == 1 &&
           capture.aggregates[0] == 3);

    reset_scan(&mock, &capture, 1);
    assert(run_query(&mock, where_sql, sizeof(where_sql) - 1, &parameter, 1,
                     64, &capture) == PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 2 && capture.keys[0] == 1 &&
           capture.aggregates[0] == 9 && capture.keys[1] == 2 &&
           capture.aggregates[1] == 5);

    reset_scan(&mock, &capture, 1);
    assert(run_query(&mock, having_sql, sizeof(having_sql) - 1,
                     &having_parameter, 1, 64, &capture) ==
           PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 1 && capture.keys[0] == 2 &&
           capture.aggregates[0] == 1);

    reset_scan(&mock, &capture, 1);
    assert(run_query(&mock, ordered_aggregate_sql,
                     sizeof(ordered_aggregate_sql) - 1, NULL, 0, 64,
                     &capture) == PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 1 && capture.keys[0] == 2 &&
           capture.aggregates[0] == 5);

    {
        const uint8_t null_then_value[] = {
            PRDB_SDK_VALUE_STATE_NULL,
            PRDB_SDK_VALUE_STATE_NULL,
            PRDB_SDK_VALUE_STATE_VALUE};
        mock.batch_len = make_rows(mock.batch, 1, keys, scores,
                                   null_then_value, 3);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v11(&mock, nulls_last_sql,
                             sizeof(nulls_last_sql) - 1, 64, &capture) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && capture.keys[0] == 2 &&
               capture.aggregates[0] == 5 && capture.keys[1] == 1 &&
               capture.aggregate_states[1] == PRDB_SDK_VALUE_STATE_NULL &&
               mock.statement_failed == 0);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v11(&mock, default_null_order_sql,
                             sizeof(default_null_order_sql) - 1, 64,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && capture.keys[0] == 1 &&
               capture.aggregate_states[0] == PRDB_SDK_VALUE_STATE_NULL &&
               capture.keys[1] == 2 && capture.aggregates[1] == 5 &&
               mock.statement_failed == 0);
        mock.batch_len = make_rows(mock.batch, 1, keys, scores,
                                   value_states, 3);
    }

    reset_scan(&mock, &capture, 1);
    assert(run_query_v12(&mock, v12_no_order_sql,
                         sizeof(v12_no_order_sql) - 1, 64, &capture) ==
           PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 2 && capture.keys[0] == 1 &&
           capture.keys[1] == 2 && capture.row_counts[0] == 2 &&
           capture.row_counts[1] == 1 && mock.statement_failed == 0);

    {
        const int32_t expected_keys[][2] = {
            {1, 2}, {2, 1}, {1, 2}, {1, 2}, {1, 2}};
        const int32_t expected_aggregates[][2] = {
            {2, 1}, {5, 3}, {9, 5}, {12, 5}, {6000000, 5000000}};
        for (uint32_t kind = 0; kind < 5; ++kind) {
            reset_scan(&mock, &capture, 1);
            assert(run_query_v12(
                       &mock, ordered_aggregate_matrix_sql[kind],
                       ordered_aggregate_matrix_sql_len[kind], 64,
                       &capture) == PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 2 &&
                   capture.keys[0] == expected_keys[kind][0] &&
                   capture.keys[1] == expected_keys[kind][1] &&
                   capture.aggregates[0] == expected_aggregates[kind][0] &&
                   capture.aggregates[1] == expected_aggregates[kind][1] &&
                   mock.statement_failed == 0);
        }
    }

    {
        int32_t many_group_keys[129];
        int32_t many_group_scores[129];
        uint8_t many_group_states[129];
        for (uint32_t index = 0; index < 129; ++index) {
            many_group_keys[index] = (int32_t)(129 - index);
            many_group_scores[index] = 1;
            many_group_states[index] = PRDB_SDK_VALUE_STATE_VALUE;
        }

        mock.batch_len = make_rows(mock.batch, 1, many_group_keys,
                                   many_group_scores, many_group_states, 128);
        mock.max_candidates = 128;
        reset_scan(&mock, &capture, 1);
        assert(run_query_v12(&mock, ordered_group_limit_sql,
                             sizeof(ordered_group_limit_sql) - 1, 8192,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 128 && mock.statement_failed == 0);
        for (uint32_t index = 0; index < 128; ++index) {
            assert(capture.keys[index] == (int32_t)(index + 2));
            assert(capture.aggregates[index] == 1);
        }

        mock.batch_len = make_rows(mock.batch, 1, many_group_keys,
                                   many_group_scores, many_group_states, 40);
        mock.max_candidates = 40;
        reset_scan(&mock, &capture, 1);
        mock.cancel_after_group_callbacks = 32;
        assert(run_query_v12(&mock, ordered_group_limit_sql,
                             sizeof(ordered_group_limit_sql) - 1, 8192,
                             &capture) ==
               PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
        assert(capture.calls == 32 && mock.cancelled == 1 &&
               mock.statement_failed == 1);
        mock.cancel_after_group_callbacks = 0;

        mock.batch_len = make_rows(mock.batch, 0, many_group_keys,
                                   many_group_scores, many_group_states, 128);
        mock.continued_batch_len = make_rows(
            mock.continued_batch, 1, many_group_keys + 128,
            many_group_scores + 128, many_group_states + 128, 1);
        mock.streamed_pages = 2;
        mock.max_candidates = 128;
        reset_scan(&mock, &capture, 1);
        int32_t over_limit_status = run_query_v12(
            &mock, ordered_group_limit_sql,
            sizeof(ordered_group_limit_sql) - 1, 8192, &capture);
        assert(over_limit_status == PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT);
        assert(capture.calls == 0 && mock.statement_failed == 1);
        mock.max_candidates = 0;
        mock.streamed_pages = 0;
        mock.continued_batch_len = 0;
        mock.batch_len = make_rows(mock.batch, 1, keys, scores,
                                   value_states, 3);
    }

    {
        mock.text_key_mode = 1;
        mock.planner_len = make_relation(
            mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
            PRDB_SDK_VALUE_TYPE_TEXT);
        mock.executor_len = make_relation(mock.executor, 6, 0x22,
                                          PRDB_SDK_VALUE_TYPE_TEXT);
        mock.batch_len = make_text_rows(mock.batch, 0, 0, 64, 8192);
        mock.continued_batch_len = make_text_rows(
            mock.continued_batch, 1, 64, 64, 8192);
        mock.streamed_pages = 2;
        mock.max_candidates = 64;
        capture.text_key_bytes = 8192;
        reset_scan(&mock, &capture, 1);
        assert(run_query_v12(&mock, ordered_group_text_budget_sql,
                             sizeof(ordered_group_text_budget_sql) - 1,
                             PRDB_SDK_SQL_EXEC_V7_MAX_MATERIALIZED_BYTES,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 128 && mock.statement_failed == 0);
        for (uint32_t index = 0; index < 128; ++index) {
            assert(capture.keys[index] == (int32_t)index);
        }

        mock.batch_len = make_text_rows(mock.batch, 0, 0, 64, 8193);
        mock.continued_batch_len = make_text_rows(
            mock.continued_batch, 1, 64, 64, 8193);
        capture.text_key_bytes = 8193;
        reset_scan(&mock, &capture, 1);
        assert(run_query_v12(&mock, ordered_group_text_budget_sql,
                             sizeof(ordered_group_text_budget_sql) - 1,
                             PRDB_SDK_SQL_EXEC_V7_MAX_MATERIALIZED_BYTES,
                             &capture) ==
               PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT);
        assert(capture.calls == 0 && mock.statement_failed == 1);

        mock.text_key_mode = 0;
        mock.max_candidates = 0;
        mock.streamed_pages = 0;
        mock.continued_batch_len = 0;
        capture.text_key_bytes = 0;
        mock.planner_len = make_relation(
            mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
            PRDB_SDK_VALUE_TYPE_INTEGER);
        mock.executor_len = make_relation(mock.executor, 6, 0x22,
                                          PRDB_SDK_VALUE_TYPE_INTEGER);
        mock.batch_len = make_rows(mock.batch, 1, keys, scores,
                                   value_states, 3);
    }

    {
        mock.text_key_mode = 1;
        mock.planner_len = make_relation(
            mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
            PRDB_SDK_VALUE_TYPE_TEXT);
        mock.executor_len = make_relation(mock.executor, 6, 0x22,
                                          PRDB_SDK_VALUE_TYPE_TEXT);
        mock.batch_len = make_text_grouped_sum_rows(
            mock.batch, 0, 0, 64, 8188);
        mock.continued_batch_len = make_text_grouped_sum_rows(
            mock.continued_batch, 1, 64, 64, 8188);
        mock.streamed_pages = 2;
        mock.max_candidates = 64;
        capture.text_key_bytes = 8188;
        capture.text_sum_tie_order_mode = 1;
        reset_scan(&mock, &capture, 1);
        assert(run_query_v12(&mock, ordered_grouped_sum_tie_sql,
                             sizeof(ordered_grouped_sum_tie_sql) - 1,
                             PRDB_SDK_SQL_EXEC_V7_MAX_MATERIALIZED_BYTES,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 64 && mock.statement_failed == 0);
        for (uint32_t group = 0; group < 64; ++group) {
            uint32_t expected_key = 62 - 2 * (group / 2) + group % 2;
            assert(capture.keys[group] == (int32_t)expected_key);
            assert(capture.aggregates[group] ==
                   (int32_t)(300 + (expected_key / 2) * 2));
            assert(capture.row_counts[group] == 2);
        }
        assert(mock.executor_requests <= 256);

        mock.batch_len = make_text_grouped_sum_rows(
            mock.batch, 0, 0, 64, 8189);
        mock.continued_batch_len = make_text_grouped_sum_rows(
            mock.continued_batch, 1, 64, 64, 8189);
        capture.text_key_bytes = 8189;
        reset_scan(&mock, &capture, 1);
        assert(run_query_v12(&mock, ordered_grouped_sum_tie_sql,
                             sizeof(ordered_grouped_sum_tie_sql) - 1,
                             PRDB_SDK_SQL_EXEC_V7_MAX_MATERIALIZED_BYTES,
                             &capture) ==
               PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT);
        assert(capture.calls == 0 && mock.statement_failed == 1);

        mock.text_key_mode = 0;
        mock.max_candidates = 0;
        mock.streamed_pages = 0;
        mock.continued_batch_len = 0;
        capture.text_key_bytes = 0;
        capture.text_sum_tie_order_mode = 0;
        mock.planner_len = make_relation(
            mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
            PRDB_SDK_VALUE_TYPE_INTEGER);
        mock.executor_len = make_relation(mock.executor, 6, 0x22,
                                          PRDB_SDK_VALUE_TYPE_INTEGER);
        mock.batch_len = make_rows(mock.batch, 1, keys, scores,
                                   value_states, 3);
    }

    {
        static const uint8_t eight_key_sql[] =
            "SELECT key_0, key_1, key_2, key_3, key_4, key_5, key_6, key_7, "
            "SUM(measure) FROM widgets GROUP BY key_0, key_1, key_2, key_3, "
            "key_4, key_5, key_6, key_7";
        uint8_t planner_response[512] = {0};
        uint8_t executor_response[512] = {0};
        uint8_t batch_response[4096] = {0};
        uint8_t materialized_bytes[4096] = {0};
        prdb_sdk_sql_column_shape_v2 planner_columns[9] = {{0}};
        prdb_sdk_sql_column_shape_v2 executor_columns[9] = {{0}};
        prdb_sdk_value_v1 cell_scratch[9] = {{0}};
        prdb_host_api_v3 host = {
            sizeof(host), PRDB_QUERY_ABI_V3_VERSION, &mock,
            executor_request, planner_request, NULL, statement_failure};
        group_capture capture = {0};
        int32_t status;
        capture.mock = &mock;
        mock.planner_len = make_eight_key_relation(
            mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99);
        mock.executor_len = make_eight_key_relation(mock.executor, 6, 0x22);
        mock.batch_len = make_eight_key_rows(mock.batch);
        reset_scan(&mock, &capture, 1);
        status = prdb_sdk_sql_execute_group_v13(
            &host, (const uint8_t *)"app", 3, eight_key_sql,
            sizeof(eight_key_sql) - 1, NULL, 0, 4, 4096, planner_response,
            sizeof(planner_response), executor_response,
            sizeof(executor_response), batch_response,
            sizeof(batch_response), planner_columns, 9, executor_columns, 9,
            cell_scratch, 9, materialized_bytes, sizeof(materialized_bytes),
            on_group_eight_keys, &capture);
        assert(status == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && mock.statement_failed == 0);
        mock.planner_len = make_relation(
            mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
            PRDB_SDK_VALUE_TYPE_INTEGER);
        mock.executor_len = make_relation(mock.executor, 6, 0x22,
                                          PRDB_SDK_VALUE_TYPE_INTEGER);
    }

    {
        static const uint8_t ordered_eight_key_sql[] =
            "SELECT key_0, key_1, key_2, key_3, key_4, key_5, key_6, key_7, "
            "SUM(measure) FROM widgets GROUP BY key_0, key_1, key_2, key_3, "
            "key_4, key_5, key_6, key_7 ORDER BY key_0 DESC, key_1 DESC, "
            "key_2 DESC, key_3 DESC, key_4 DESC, key_5 DESC, key_6 DESC, "
            "key_7 DESC, SUM(measure) DESC LIMIT 5";
        uint8_t planner_response[512] = {0};
        uint8_t executor_response[512] = {0};
        uint8_t batch_response[4096] = {0};
        uint8_t materialized_bytes[4096] = {0};
        prdb_sdk_sql_column_shape_v2 planner_columns[9] = {{0}};
        prdb_sdk_sql_column_shape_v2 executor_columns[9] = {{0}};
        prdb_sdk_value_v1 cell_scratch[9] = {{0}};
        prdb_host_api_v3 host = {
            sizeof(host), PRDB_QUERY_ABI_V3_VERSION, &mock,
            executor_request, planner_request, NULL, statement_failure};
        group_capture capture = {0};
        capture.mock = &mock;
        mock.planner_len = make_eight_key_relation(
            mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99);
        mock.executor_len = make_eight_key_relation(mock.executor, 6, 0x22);
        mock.batch_len = make_eight_key_rows(mock.batch);
        reset_scan(&mock, &capture, 1);
        assert(prdb_sdk_sql_execute_group_v15(
                   &host, (const uint8_t *)"app", 3, ordered_eight_key_sql,
                   sizeof(ordered_eight_key_sql) - 1, NULL, 0, 4, 4096,
                   planner_response, sizeof(planner_response),
                   executor_response, sizeof(executor_response), batch_response,
                   sizeof(batch_response), planner_columns, 9,
                   executor_columns, 9, cell_scratch, 9, materialized_bytes,
                   sizeof(materialized_bytes), on_ordered_group_eight_keys,
                   &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && mock.statement_failed == 0);
        mock.optimizer_aggregate_kind = PRDB_SDK_SQL_V7_AGGREGATE_SUM;
        mock.optimizer_relation_resolves = 0;
        mock.planner_len = make_eight_key_relation(
            mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99);
        mock.executor_len = make_eight_key_relation(mock.executor, 6, 0x22);
        mock.batch_len = make_eight_key_rows(mock.batch);
        reset_scan(&mock, &capture, 1);
        int32_t ordered_optimizer_status = run_ordered_eight_key_optimizer(
            &mock, ordered_eight_key_sql,
            sizeof(ordered_eight_key_sql) - 1, &capture);
        assert(ordered_optimizer_status == PRDB_SDK_SQL_EXEC_OK);
        assert(mock.optimizer_enabled == 1 &&
               mock.optimizer_relation_resolves == 1 &&
               capture.calls == 2 && mock.statement_failed == 0);
        mock.planner_len = make_relation(
            mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99,
            PRDB_SDK_VALUE_TYPE_INTEGER);
        mock.executor_len = make_relation(mock.executor, 6, 0x22,
                                          PRDB_SDK_VALUE_TYPE_INTEGER);
    }

    {
        const int32_t tuple_ids[] = {1, 1, 2};
        const int32_t tuple_scores[] = {9, 9, 5};
        mock.batch_len = make_rows(mock.batch, 1, tuple_ids, tuple_scores,
                                   value_states, 3);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v13(&mock, v13_composite_sql,
                             sizeof(v13_composite_sql) - 1, 64,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && capture.keys[0] == 1 &&
               capture.second_keys[0] == 9 && capture.row_counts[0] == 2 &&
               capture.keys[1] == 2 && capture.second_keys[1] == 5 &&
               capture.row_counts[1] == 1 && mock.statement_failed == 0);
        {
            group_capture reset_capture = {0};
            nullable_composite_capture null_capture = {&mock, 0};
            mock.cancel_when_batch_finishes = 0;
            mock.batch_len = make_nullable_composite_rows(mock.batch);
            reset_scan(&mock, &reset_capture, 1);
            int32_t nullable_status = run_nullable_composite_count_v13(
                &mock, v13_nullable_composite_sql,
                sizeof(v13_nullable_composite_sql) - 1, &null_capture);
            assert(nullable_status == PRDB_SDK_SQL_EXEC_OK);
            assert(null_capture.calls == 4 && mock.statement_failed == 0);
        }
        mock.batch_len = make_rows(mock.batch, 1, tuple_ids, tuple_scores,
                                   value_states, 3);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v13(&mock, v13_sum_sql,
                             sizeof(v13_sum_sql) - 1, 64,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && capture.aggregates[0] == 18 &&
               capture.aggregates[1] == 5 && mock.statement_failed == 0);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v13(&mock, v13_min_sql,
                             sizeof(v13_min_sql) - 1, 64,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && capture.aggregates[0] == 9 &&
               capture.aggregates[1] == 5 && mock.statement_failed == 0);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v13(&mock, v13_max_sql,
                             sizeof(v13_max_sql) - 1, 64,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && capture.aggregates[0] == 9 &&
               capture.aggregates[1] == 5 && mock.statement_failed == 0);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v13(&mock, v13_avg_sql,
                             sizeof(v13_avg_sql) - 1, 64,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && capture.aggregates[0] == 9 &&
               capture.aggregates[1] == 5 && mock.statement_failed == 0);
        for (uint32_t kind = PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR;
             kind <= PRDB_SDK_SQL_V7_AGGREGATE_AVG; ++kind) {
            int32_t optimizer_status;
            mock.optimizer_aggregate_kind = kind;
            reset_scan(&mock, &capture, 1);
            optimizer_status = run_query_v13_optimizer(
                &mock, v13_optimizer_sql[kind - 1],
                v13_optimizer_sql_lens[kind - 1], 64, &capture);
            assert(optimizer_status == PRDB_SDK_SQL_EXEC_OK);
            assert(mock.optimizer_enabled == 1 && capture.calls == 2 &&
                   capture.keys[0] == 1 && capture.second_keys[0] == 9 &&
                   capture.keys[1] == 2 && capture.second_keys[1] == 5 &&
                   mock.statement_failed == 0);
            if (kind == PRDB_SDK_SQL_V7_AGGREGATE_COUNT_STAR) {
                assert(capture.aggregates[0] == 2 &&
                       capture.aggregates[1] == 1 &&
                       capture.row_counts[0] == 2 &&
                       capture.row_counts[1] == 1);
            } else if (kind == PRDB_SDK_SQL_V7_AGGREGATE_SUM) {
                assert(capture.aggregates[0] == 18 &&
                       capture.aggregates[1] == 5);
            } else {
                assert(capture.aggregates[0] == 9 &&
                       capture.aggregates[1] == 5);
            }
        }
        {
            static const uint8_t filtered_optimizer_sql[] =
                "SELECT id, score, SUM(score) FROM widgets WHERE id = $1 "
                "GROUP BY id, score LIMIT 5";
            uint8_t parameter_bytes[4] = {0, 0, 0, 1};
            prdb_sdk_value_v1 parameters[1] = {{
                sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
                PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
                parameter_bytes, sizeof(parameter_bytes)}};
            mock.optimizer_aggregate_kind = PRDB_SDK_SQL_V7_AGGREGATE_SUM;
            reset_scan(&mock, &capture, 1);
            int32_t filtered_status = run_query_v13_optimizer_internal(
                &mock, filtered_optimizer_sql,
                sizeof(filtered_optimizer_sql) - 1, parameters, 1, 1, 64,
                &capture);
            assert(filtered_status == PRDB_SDK_SQL_EXEC_OK);
            assert(mock.optimizer_enabled == 1 && capture.calls == 1 &&
                   capture.keys[0] == 1 && capture.second_keys[0] == 9 &&
                   capture.aggregates[0] == 18 && capture.row_counts[0] == 2 &&
                   mock.statement_failed == 0);
        }
        reset_scan(&mock, &capture, 1);
        assert(run_query_v14(&mock, v14_having_sql,
                             sizeof(v14_having_sql) - 1,
                             &v14_having_parameter, &capture) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 1 && capture.keys[0] == 1 &&
               capture.aggregates[0] == 18 && mock.statement_failed == 0);

        reset_scan(&mock, &capture, 1);
        assert(run_query_v17(&mock, v17_overflow_having_sql,
                             sizeof(v17_overflow_having_sql) - 1, NULL,
                             &capture) == PRDB_SDK_SQL_EXEC_BIND_ERROR);
        assert(capture.calls == 0 && mock.next_calls == 0 &&
               mock.statement_failed == 1);

        {
            const int32_t nullable_keys[] = {1, 1, 2, 3};
            const int32_t nullable_scores[] = {9, 3, 5, 0};
            const uint8_t nullable_states[] = {
                PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_STATE_VALUE,
                PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_STATE_NULL};
            mock.batch_len = make_rows(mock.batch, 1, nullable_keys,
                                       nullable_scores, nullable_states, 4);
            reset_scan(&mock, &capture, 1);
            /* The grouped inputs produce TRUE (9, 3), FALSE (5), and
             * UNKNOWN (the NULL-only group) for the compound HAVING tree. */
            assert(run_query_v18(&mock, v18_compound_having_sql,
                                 sizeof(v18_compound_having_sql) - 1,
                                 &v18_having_parameter, &capture) ==
                   PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 2 && capture.keys[0] == 1 &&
                   capture.second_keys[0] == 9 &&
                   capture.aggregates[0] == 9 && capture.keys[1] == 1 &&
                   capture.second_keys[1] == 3 &&
                   capture.aggregates[1] == 3 && mock.statement_failed == 0);

        reset_scan(&mock, &capture, 1);
        capture.expect_having_literal = 1;
        assert(run_query_v18(&mock, v18_negative_literal_sql,
                             sizeof(v18_negative_literal_sql) - 1, NULL,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 3 && capture.aggregates[0] == 9 &&
               capture.aggregates[1] == 3 && capture.aggregates[2] == 5 &&
               mock.statement_failed == 0);
        capture.expect_having_literal = 0;
        }
        {
            const int32_t one_key[] = {1};
            const int32_t one_score[] = {9};
            const uint8_t one_value_state[] = {
                PRDB_SDK_VALUE_STATE_VALUE};
            mock.batch_len = make_rows(mock.batch, 0, one_key, one_score,
                                       one_value_state, 1);
            mock.streamed_pages = 128;
            mock.max_candidates = 1;
            reset_scan(&mock, &capture, 1);
            capture.expected_row_count = 128;
            int32_t status = run_query_v18(
                &mock, v18_compound_having_sql,
                sizeof(v18_compound_having_sql) - 1, &v18_having_parameter,
                &capture);
            assert(capture.calls == 0);
            assert(mock.next_calls == 124);
            assert(mock.executor_requests == 251);
            assert(mock.statement_failed == 1);
            assert(status == PRDB_SDK_SQL_EXEC_REQUEST_LIMIT);
            mock.streamed_pages = 0;
            mock.max_candidates = 0;
            capture.expected_row_count = 0;
        }
        {
            int32_t many_keys[40];
            int32_t many_scores[40];
            uint8_t many_value_states[40];
            for (uint32_t index = 0; index < 40; ++index) {
                many_keys[index] = 1;
                many_scores[index] = (int32_t)index + 9;
                many_value_states[index] = PRDB_SDK_VALUE_STATE_VALUE;
            }
            mock.batch_len = make_rows(mock.batch, 1, many_keys, many_scores,
                                       many_value_states, 40);
            mock.max_candidates = 40;
            reset_scan(&mock, &capture, 1);
            mock.cancel_when_batch_finishes = 1;
            int32_t before_callback_status = run_query_v13(
                &mock, v13_many_groups_sql, sizeof(v13_many_groups_sql) - 1,
                4096, &capture);
            assert(before_callback_status == PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
            assert(capture.calls == 0 && mock.cancelled == 1);
            assert(mock.executor_requests == 5 && mock.statement_failed == 1);

            reset_scan(&mock, &capture, 1);
            mock.cancel_when_batch_finishes = 0;
            mock.cancel_after_group_callbacks = 32;
            int32_t status = run_query_v13(
                &mock, v13_many_groups_sql, sizeof(v13_many_groups_sql) - 1,
                4096, &capture);
            assert(status == PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
            assert(capture.calls == 32 && mock.cancelled == 1);
            assert(mock.executor_requests == 7 && mock.statement_failed == 1);
            mock.cancel_after_group_callbacks = 0;
            mock.max_candidates = 0;
        }
        mock.batch_len = make_rows(mock.batch, 1, keys, scores,
                                   value_states, 3);
    }

    {
        const int32_t tuple_ids[] = {2, 1, 1, 0};
        const int32_t tuple_scores[] = {9, 9, 9, 9};
        const uint8_t tuple_states[] = {
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_STATE_VALUE,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_STATE_VALUE};
        const int32_t sorted_ids[] = {1, 0, 2};
        mock.batch_len = make_rows(mock.batch, 1, tuple_ids, tuple_scores,
                                   tuple_states, 4);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v15(&mock, v15_order_sql,
                             sizeof(v15_order_sql) - 1,
                             &v15_having_parameter, &capture) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 3 && mock.statement_failed == 0);
        for (uint32_t index = 0; index < 3; ++index) {
            assert(capture.keys[index] == sorted_ids[index]);
            assert(capture.second_keys[index] == 9);
        }
        assert(capture.row_counts[0] == 2 && capture.row_counts[1] == 1 &&
               capture.row_counts[2] == 1);

        reset_scan(&mock, &capture, 1);
        capture.expect_having_literal = 1;
        assert(run_query_v17(&mock, v17_literal_having_sql,
                             sizeof(v17_literal_having_sql) - 1,
                             NULL,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 1 && capture.keys[0] == 1 &&
               capture.aggregates[0] == 18 && mock.statement_failed == 0);

        reset_scan(&mock, &capture, 1);
        capture.expect_having_literal = 0;
        assert(run_query_v17(&mock, v17_parameter_having_sql,
                             sizeof(v17_parameter_having_sql) - 1,
                             &v17_having_parameter, &capture) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 1 && capture.keys[0] == 1 &&
               capture.aggregates[0] == 18 && mock.statement_failed == 0);

        mock.batch_len = make_rows(mock.batch, 1, keys, scores,
                                   value_states, 3);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v18(&mock, v18_compound_having_sql,
                             sizeof(v18_compound_having_sql) - 1,
                             &v18_having_parameter, &capture) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && capture.keys[0] == 1 &&
               capture.second_keys[0] == 9 && capture.aggregates[0] == 9 &&
               capture.keys[1] == 1 && capture.second_keys[1] == 3 &&
               capture.aggregates[1] == 3 && mock.statement_failed == 0);

        reset_scan(&mock, &capture, 1);
        assert(run_query_v18(&mock, v18_compound_having_sql,
                             sizeof(v18_compound_having_sql) - 1, NULL,
                             &capture) == PRDB_SDK_SQL_EXEC_BIND_ERROR);
        assert(capture.calls == 0 && mock.next_calls == 0 &&
               mock.statement_failed == 1);

        reset_scan(&mock, &capture, 1);
        assert(run_query_v18(&mock, v18_compound_having_sql,
                             sizeof(v18_compound_having_sql) - 1, &parameter,
                             &capture) == PRDB_SDK_SQL_EXEC_BIND_ERROR);
        assert(capture.calls == 0 && mock.next_calls == 0 &&
               mock.statement_failed == 1);
        mock.batch_len = make_rows(mock.batch, 1, keys, scores,
                                   value_states, 3);
    }

    {
        const int32_t tied_group_keys[] = {1, 2, 3};
        mock.batch_len = make_rows(mock.batch, 1, tied_group_keys, scores,
                                   value_states, 3);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v12(&mock, composite_order_sql,
                             sizeof(composite_order_sql) - 1, 64,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && capture.keys[0] == 3 &&
               capture.keys[1] == 2 && capture.row_counts[0] == 1 &&
               capture.row_counts[1] == 1 && mock.statement_failed == 0);
        mock.batch_len = make_rows(mock.batch, 1, keys, scores,
                                   value_states, 3);
    }

    reset_scan(&mock, &capture, 1);
    assert(run_query(&mock, ordered_having_sql,
                     sizeof(ordered_having_sql) - 1, &having_parameter, 1, 64,
                     &capture) == PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 1 && capture.keys[0] == 2 &&
           capture.aggregates[0] == 1);

    reset_scan(&mock, &capture, 1);
    assert(run_query(&mock, ordered_key_sql, sizeof(ordered_key_sql) - 1,
                     NULL, 0, 64, &capture) == PRDB_SDK_SQL_EXEC_OK);
    assert(capture.calls == 1 && capture.keys[0] == 2 &&
           capture.aggregates[0] == 1);

    {
        prdb_sdk_value_v1 null_parameter = {
            sizeof(null_parameter), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_NULL, 0, NULL, 0};
        reset_scan(&mock, &capture, 1);
        assert(run_query(&mock, having_sql, sizeof(having_sql) - 1,
                         &null_parameter, 1, 64, &capture) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 0 && mock.next_calls == 1 &&
               mock.statement_failed == 0);
    }

    {
        prdb_sdk_value_v1 invalid_parameter = parameter;
        reset_scan(&mock, &capture, 1);
        assert(run_query(&mock, having_sql, sizeof(having_sql) - 1,
                         &invalid_parameter, 1, 64, &capture) ==
               PRDB_SDK_SQL_EXEC_BIND_ERROR);
        assert(capture.calls == 0 && mock.next_calls == 0 &&
               mock.statement_failed == 1);
        invalid_parameter.state = PRDB_SDK_VALUE_STATE_REDACTED;
        invalid_parameter.type_code = 0;
        invalid_parameter.data = NULL;
        invalid_parameter.data_len = 0;
        reset_scan(&mock, &capture, 1);
        assert(run_query(&mock, having_sql, sizeof(having_sql) - 1,
                         &invalid_parameter, 1, 64, &capture) ==
               PRDB_SDK_SQL_EXEC_BIND_ERROR);
        assert(capture.calls == 0 && mock.next_calls == 0 &&
               mock.statement_failed == 1);
    }

    {
        const uint8_t null_states[] = {
            PRDB_SDK_VALUE_STATE_NULL,
            PRDB_SDK_VALUE_STATE_NULL,
            PRDB_SDK_VALUE_STATE_NULL};
        mock.batch_len = make_rows(mock.batch, 1, keys, scores, null_states, 3);
        reset_scan(&mock, &capture, 1);
        assert(run_query(&mock, min_sql, sizeof(min_sql) - 1, NULL, 0, 64,
                         &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 &&
               capture.aggregate_states[0] == PRDB_SDK_VALUE_STATE_NULL &&
               capture.aggregate_states[1] == PRDB_SDK_VALUE_STATE_NULL);
        reset_scan(&mock, &capture, 1);
        assert(run_query(&mock, max_sql, sizeof(max_sql) - 1, NULL, 0, 64,
                         &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 &&
               capture.aggregate_states[0] == PRDB_SDK_VALUE_STATE_NULL &&
               capture.aggregate_states[1] == PRDB_SDK_VALUE_STATE_NULL);
        {
            const uint8_t partly_null_states[] = {
                PRDB_SDK_VALUE_STATE_NULL,
                PRDB_SDK_VALUE_STATE_NULL,
                PRDB_SDK_VALUE_STATE_VALUE};
            mock.batch_len = make_rows(mock.batch, 1, keys, scores,
                                       partly_null_states, 3);
            reset_scan(&mock, &capture, 1);
            assert(run_query(&mock, ordered_aggregate_sql,
                             sizeof(ordered_aggregate_sql) - 1, NULL, 0, 64,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 1 && capture.keys[0] == 1 &&
                   capture.aggregate_states[0] ==
                       PRDB_SDK_VALUE_STATE_NULL);
            reset_scan(&mock, &capture, 1);
            assert(run_query(&mock, max_sql, sizeof(max_sql) - 1, NULL, 0, 64,
                             &capture) == PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 2 &&
                   capture.aggregate_states[0] == PRDB_SDK_VALUE_STATE_NULL &&
                   capture.aggregate_states[1] == PRDB_SDK_VALUE_STATE_VALUE &&
                   capture.aggregates[1] == 5);
            mock.batch_len = make_rows(mock.batch, 1, keys, scores,
                                       null_states, 3);
        }
        reset_scan(&mock, &capture, 1);
        assert(run_query(&mock, avg_sql, sizeof(avg_sql) - 1, NULL, 0, 64,
                         &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 &&
               capture.aggregate_states[0] == PRDB_SDK_VALUE_STATE_NULL &&
               capture.aggregate_states[1] == PRDB_SDK_VALUE_STATE_NULL);
        reset_scan(&mock, &capture, 1);
        assert(run_query(&mock, sum_sql, sizeof(sum_sql) - 1, NULL, 0, 64,
                         &capture) == PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 &&
               capture.aggregate_states[0] == PRDB_SDK_VALUE_STATE_NULL &&
               capture.aggregate_states[1] == PRDB_SDK_VALUE_STATE_NULL);
    }

    {
        const uint8_t redacted_states[] = {
            PRDB_SDK_VALUE_STATE_VALUE,
            PRDB_SDK_VALUE_STATE_REDACTED,
            PRDB_SDK_VALUE_STATE_VALUE};
        mock.batch_len = make_rows(mock.batch, 0, keys, scores,
                                   redacted_states, 3);
        reset_scan(&mock, &capture, 0);
        assert(run_query(&mock, min_sql, sizeof(min_sql) - 1, NULL, 0, 64,
                         &capture) ==
               PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
        assert(capture.calls == 0 && mock.executor_calls == 4 &&
               mock.statement_failed == 1);
        reset_scan(&mock, &capture, 0);
        assert(run_query(&mock, max_sql, sizeof(max_sql) - 1, NULL, 0, 64,
                         &capture) ==
               PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
        assert(capture.calls == 0 && mock.executor_calls == 4 &&
               mock.statement_failed == 1);
        reset_scan(&mock, &capture, 0);
        assert(run_query(&mock, avg_sql, sizeof(avg_sql) - 1, NULL, 0, 64,
                         &capture) ==
               PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
        assert(capture.calls == 0 && mock.executor_calls == 4 &&
               mock.statement_failed == 1);
        reset_scan(&mock, &capture, 0);
        assert(run_query(&mock, sum_sql, sizeof(sum_sql) - 1, NULL, 0, 64,
                         &capture) ==
               PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
        assert(capture.calls == 0 && mock.executor_calls == 4 &&
               mock.statement_failed == 1);
    }

    mock.batch_len = make_rows(mock.batch, 0, keys, scores, value_states, 3);
    reset_scan(&mock, &capture, 0);
    assert(run_query(&mock, min_sql, sizeof(min_sql) - 1, NULL, 0, 4,
                     &capture) ==
           PRDB_SDK_SQL_EXEC_MATERIALIZATION_LIMIT);
    assert(capture.calls == 0 && mock.executor_calls == 4 &&
           mock.statement_failed == 1);

    reset_scan(&mock, &capture, 0);
    mock.cancelled = 1;
    assert(run_query(&mock, min_sql, sizeof(min_sql) - 1, NULL, 0, 64,
                     &capture) == PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    assert(capture.calls == 0 && mock.next_calls == 0 &&
           mock.executor_calls == 3 && mock.statement_failed == 1);

    mock.batch_len = make_rows(mock.batch, 0, keys, scores, value_states, 3);
    reset_scan(&mock, &capture, 0);
    mock.cancel_after_first_batch = 1;
    assert(run_query(&mock, min_sql, sizeof(min_sql) - 1, NULL, 0, 64,
                     &capture) == PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
    assert(capture.calls == 0 && mock.next_calls == 1 &&
           mock.executor_calls == 4 && mock.statement_failed == 1);

    {
        /* Two all-NULL rows form (1,NULL); two values form (2,3.00). */
        const int32_t null_keys[] = {1, 1, 2, 2};
        const int64_t null_values[] = {0, 0, 300, 300};
        const uint8_t states[] = {PRDB_SDK_VALUE_STATE_NULL,
            PRDB_SDK_VALUE_STATE_NULL, PRDB_SDK_VALUE_STATE_VALUE,
            PRDB_SDK_VALUE_STATE_VALUE};
        mock.planner_len = make_decimal_relation(
            mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99, 10, 2);
        mock.executor_len = make_decimal_relation(mock.executor, 6, 0x22, 10, 2);
        mock.batch_len = make_decimal_rows(mock.batch, 1, null_keys,
                                           null_values, states, 4);
        const char *aggregates[] = {"SUM", "AVG", "MIN", "MAX"};
        for (uint32_t i = 0; i < 4; ++i) {
            char sql[512];
            snprintf(sql, sizeof(sql),
                "SELECT id, score, %s(score) FROM widgets GROUP BY id, score "
                "HAVING %s(score) IS NULL AND COUNT(*) IS NOT NULL "
                "AND COUNT(*) >= 2 ORDER BY id DESC LIMIT 1",
                aggregates[i], aggregates[i]);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v23_decimal(&mock, (const uint8_t *)sql,
                strlen(sql), NULL, 0, on_group_v23_null_test, &capture) ==
                PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 1 && capture.keys[0] == 1 &&
                   capture.aggregates[0] == -1 && !mock.statement_failed);
            snprintf(sql, sizeof(sql),
                "SELECT id, score, %s(score) FROM widgets GROUP BY id, score "
                "HAVING NOT %s(score) IS NULL AND COUNT(*) >= 2",
                aggregates[i], aggregates[i]);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v23_decimal(&mock, (const uint8_t *)sql,
                strlen(sql), NULL, 0, on_group_v23_null_test, &capture) ==
                PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 1 && capture.keys[0] == 2 &&
                   capture.aggregates[0] == 1 && !mock.statement_failed);
        }
        {
            prdb_sdk_value_v1 null_parameter = {
                sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
                PRDB_SDK_VALUE_STATE_NULL, 0, NULL, 0};
            const char *or_sql = "SELECT id, score, SUM(score) FROM widgets "
                "GROUP BY id, score HAVING SUM(score) IS NULL "
                "OR SUM(score) >= $1";
            reset_scan(&mock, &capture, 1);
            assert(run_query_v23_decimal(&mock, (const uint8_t *)or_sql,
                strlen(or_sql), &null_parameter, 1, on_group_v23_null_test,
                &capture) == PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 1 && capture.keys[0] == 1 &&
                   capture.aggregates[0] == -1 && !mock.statement_failed);
            const char *nonnull_sql = "SELECT id, score, SUM(score) FROM widgets "
                "GROUP BY id, score HAVING SUM(score) IS NOT NULL";
            reset_scan(&mock, &capture, 1);
            assert(run_query_v23_decimal(&mock, (const uint8_t *)nonnull_sql,
                strlen(nonnull_sql), NULL, 0, on_group_v23_null_test,
                &capture) == PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 1 && capture.keys[0] == 2 &&
                   capture.aggregates[0] == 1 && !mock.statement_failed);
        }
        const char *count_sql = "SELECT id, score, COUNT(*) FROM widgets "
            "GROUP BY id, score HAVING COUNT(*) IS NULL";
        reset_scan(&mock, &capture, 1);
        assert(run_query_v23_decimal(&mock, (const uint8_t *)count_sql,
            strlen(count_sql), NULL, 0, on_group_v23_null_test, &capture) ==
            PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 0 && !mock.statement_failed);
        const char *redacted_sql = "SELECT id, score, SUM(score) FROM widgets "
            "GROUP BY id, score HAVING SUM(score) IS NULL";
        const uint8_t denied[] = {PRDB_SDK_VALUE_STATE_NULL,
            PRDB_SDK_VALUE_STATE_REDACTED};
        mock.batch_len = make_decimal_rows(mock.batch, 1, null_keys,
                                           null_values, denied, 2);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v23_decimal(&mock, (const uint8_t *)redacted_sql,
            strlen(redacted_sql), NULL, 0, on_group_v23_null_test, &capture) ==
            PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
        assert(capture.calls == 0 && mock.statement_failed);
    }

    {
        const int64_t decimal_coefficients[] = {900, 300, 500};
        mock.planner_len = make_decimal_relation(
            mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99, 10, 2);
        mock.executor_len = make_decimal_relation(mock.executor, 6, 0x22,
                                                  10, 2);
        mock.batch_len = make_decimal_rows(mock.batch, 1, keys,
                                           decimal_coefficients, NULL, 3);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v22_decimal(
                   &mock, v22_hidden_count_sql,
                   sizeof(v22_hidden_count_sql) - 1, NULL, 0,
                   on_group_v21_decimal, &capture) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 0 && mock.statement_failed == 0);

        {
            const int32_t where_keys[] = {1, 1, 2};
            const int64_t where_coefficients[] = {300, 300, 500};
            mock.batch_len = make_decimal_rows(
                mock.batch, 1, where_keys, where_coefficients, NULL, 3);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v22_decimal(
                       &mock, v22_where_and_having_sql,
                       sizeof(v22_where_and_having_sql) - 1,
                       &v22_where_parameter, 1,
                       on_group_v22_where_and_having, &capture) ==
                   PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 1 && capture.keys[0] == 2 &&
                   capture.aggregates[0] == 5 && capture.row_counts[0] == 1 &&
                   mock.statement_failed == 0);
            mock.batch_len = make_decimal_rows(mock.batch, 1, keys,
                                               decimal_coefficients, NULL, 3);
        }

        {
            const int32_t avg_keys[] = {2, 2};
            const int64_t avg_coefficients[] = {300, 300};
            mock.batch_len = make_decimal_rows(mock.batch, 1, avg_keys,
                                               avg_coefficients, NULL, 2);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v22_decimal(
                       &mock, v22_hidden_count_avg_sql,
                       sizeof(v22_hidden_count_avg_sql) - 1, NULL, 0,
                       on_group_v22_hidden_count_avg, &capture) ==
                   PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 1 && capture.keys[0] == 2 &&
                   capture.aggregates[0] == 3 && mock.statement_failed == 0);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v22_decimal(
                       &mock, v22_hidden_count_min_sql,
                       sizeof(v22_hidden_count_min_sql) - 1, NULL, 0,
                       on_group_v22_hidden_count_minmax, &capture) ==
                   PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 1 && capture.keys[0] == 2 &&
                   capture.aggregates[0] == 3 && mock.statement_failed == 0);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v22_decimal(
                       &mock, v22_hidden_count_max_sql,
                       sizeof(v22_hidden_count_max_sql) - 1, NULL, 0,
                       on_group_v22_hidden_count_minmax, &capture) ==
                   PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 1 && capture.keys[0] == 2 &&
                   capture.aggregates[0] == 3 && mock.statement_failed == 0);
            mock.batch_len = make_decimal_rows(mock.batch, 1, keys,
                                               decimal_coefficients, NULL, 3);
        }

        {
            const int32_t null_keys[] = {1, 1};
            const int64_t null_coefficients[] = {0, 0};
            const uint8_t null_states[] = {PRDB_SDK_VALUE_STATE_NULL,
                                           PRDB_SDK_VALUE_STATE_NULL};
            mock.batch_len = make_decimal_rows(mock.batch, 1, null_keys,
                                               null_coefficients, null_states,
                                               2);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v22_decimal(
                       &mock, v22_null_count_sql,
                       sizeof(v22_null_count_sql) - 1, NULL, 0,
                       on_group_v22_null_sum, &capture) ==
                   PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 1 && capture.keys[0] == 1 &&
                   capture.aggregates[0] == -1 && mock.statement_failed == 0);
            mock.batch_len = make_decimal_rows(mock.batch, 1, keys,
                                               decimal_coefficients, NULL, 3);
        }

        {
            const uint8_t threshold_bytes[8] = {0, 0, 0, 0, 0, 0, 0, 2};
            const uint8_t decimal_bytes[16] = {0};
            prdb_sdk_value_v1 count_parameter = {
                sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
                PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BIGINT,
                threshold_bytes, sizeof(threshold_bytes)};
            prdb_sdk_value_v1 wrong_parameter = {
                sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
                PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_DECIMAL,
                decimal_bytes, sizeof(decimal_bytes)};
            reset_scan(&mock, &capture, 1);
            assert(run_query_v22_decimal(
                       &mock, v22_parameter_count_sql,
                       sizeof(v22_parameter_count_sql) - 1, &count_parameter,
                       1, on_group_v21_decimal, &capture) ==
                   PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 0 && mock.statement_failed == 0);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v22_decimal(
                       &mock, v22_parameter_count_sql,
                       sizeof(v22_parameter_count_sql) - 1, &wrong_parameter,
                       1, on_group_v21_decimal, &capture) ==
                   PRDB_SDK_SQL_EXEC_BIND_ERROR);
            assert(capture.calls == 0 && mock.next_calls == 0 &&
                   mock.statement_failed == 1);
        }

        reset_scan(&mock, &capture, 1);
        assert(run_query_v21_decimal(
                   &mock, v21_decimal_literal_sql,
                   sizeof(v21_decimal_literal_sql) - 1, &capture) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 2 && capture.keys[0] == 1 &&
               capture.aggregates[0] == 9 && capture.keys[1] == 2 &&
               capture.aggregates[1] == 5 && mock.statement_failed == 0);

        {
            const int64_t exact_coefficients[] = {450, 449, 500};
            mock.batch_len = make_decimal_rows(mock.batch, 1, keys,
                                               exact_coefficients, NULL, 3);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v21_decimal(
                       &mock, v21_decimal_equal_literal_sql,
                       sizeof(v21_decimal_equal_literal_sql) - 1, &capture) ==
                   PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 1 && capture.keys[0] == 1 &&
                   capture.aggregates[0] == 4 && mock.statement_failed == 0);
        }

        {
            const int64_t average_coefficients[] = {150, 130, 250};
            mock.batch_len = make_decimal_rows(mock.batch, 1, keys,
                                               average_coefficients, NULL, 3);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v21_decimal(
                       &mock, v21_decimal_avg_literal_sql,
                       sizeof(v21_decimal_avg_literal_sql) - 1, &capture) ==
                   PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 2 && capture.keys[0] == 1 &&
                   capture.aggregates[0] == 150 && capture.keys[1] == 2 &&
                   capture.aggregates[1] == 250 && mock.statement_failed == 0);
        }

        reset_scan(&mock, &capture, 1);
        assert(run_query_v21_decimal(
                   &mock, v21_decimal_inexact_scale_sql,
                   sizeof(v21_decimal_inexact_scale_sql) - 1, &capture) ==
               PRDB_SDK_SQL_EXEC_BIND_ERROR);
        assert(capture.calls == 0 && mock.next_calls == 0 &&
               mock.statement_failed == 1);

        reset_scan(&mock, &capture, 1);
        assert(run_query_v21_decimal(
                   &mock, v21_decimal_overflow_literal_sql,
                   sizeof(v21_decimal_overflow_literal_sql) - 1, &capture) ==
               PRDB_SDK_SQL_EXEC_BIND_ERROR);
        assert(capture.calls == 0 && mock.next_calls == 0 &&
               mock.statement_failed == 1);

        reset_scan(&mock, &capture, 1);
        mock.cancelled = 1;
        assert(run_query_v21_decimal(
                   &mock, v21_decimal_literal_sql,
                   sizeof(v21_decimal_literal_sql) - 1, &capture) ==
               PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
        assert(capture.calls == 0 && mock.next_calls == 0 &&
               mock.statement_failed == 1);

        reset_scan(&mock, &capture, 1);
        mock.cancel_after_first_batch = 1;
        assert(run_query_v21_decimal(
                   &mock, v21_decimal_literal_sql,
                   sizeof(v21_decimal_literal_sql) - 1, &capture) ==
               PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
        assert(capture.calls == 0 && mock.next_calls == 1 &&
               mock.statement_failed == 1);

        {
            int32_t many_decimal_keys[40];
            int64_t many_decimal_coefficients[40];
            for (uint32_t index = 0; index < 40; ++index) {
                many_decimal_keys[index] = (int32_t)(index + 1);
                many_decimal_coefficients[index] = 900;
            }
            mock.batch_len = make_decimal_rows(
                mock.batch, 1, many_decimal_keys,
                many_decimal_coefficients, NULL, 40);
            mock.max_candidates = 40;
            reset_scan(&mock, &capture, 1);
            mock.cancel_after_group_callbacks = 32;
            assert(run_query_v21_decimal(
                       &mock, v21_decimal_literal_sql,
                       sizeof(v21_decimal_literal_sql) - 1, &capture) ==
                   PRDB_SDK_SQL_EXEC_EXECUTOR_ERROR);
            assert(capture.calls == 32 && mock.cancelled == 1 &&
                   mock.statement_failed == 1);
            mock.cancel_after_group_callbacks = 0;
            mock.max_candidates = 0;
            mock.batch_len = make_decimal_rows(mock.batch, 1, keys,
                                               decimal_coefficients, NULL, 3);
        }

        mock.batch_len = make_decimal_rows(mock.batch, 1, keys,
                                           decimal_coefficients, NULL, 3);
        reset_scan(&mock, &capture, 1);
        assert(run_query_v21_min_decimal(
                   &mock, v21_decimal_min_literal_sql,
                   sizeof(v21_decimal_min_literal_sql) - 1, &capture) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 1 && capture.keys[0] == 1 &&
               capture.aggregates[0] == 300 && mock.statement_failed == 0);

        reset_scan(&mock, &capture, 1);
        assert(run_query_v21_max_decimal(
                   &mock, v21_decimal_max_literal_sql,
                   sizeof(v21_decimal_max_literal_sql) - 1, &capture) ==
               PRDB_SDK_SQL_EXEC_OK);
        assert(capture.calls == 1 && capture.keys[0] == 1 &&
               capture.aggregates[0] == 900 && mock.statement_failed == 0);

        {
            const uint8_t decimal_null_states[] = {
                PRDB_SDK_VALUE_STATE_NULL, PRDB_SDK_VALUE_STATE_NULL,
                PRDB_SDK_VALUE_STATE_NULL};
            mock.batch_len = make_decimal_rows(mock.batch, 1, keys,
                                               decimal_coefficients,
                                               decimal_null_states, 3);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v21_max_decimal(
                       &mock, v21_decimal_max_literal_sql,
                       sizeof(v21_decimal_max_literal_sql) - 1, &capture) ==
                   PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 0 && mock.statement_failed == 0);
        }

        {
            const uint8_t decimal_redacted_states[] = {
                PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_STATE_REDACTED,
                PRDB_SDK_VALUE_STATE_VALUE};
            mock.batch_len = make_decimal_rows(mock.batch, 1, keys,
                                               decimal_coefficients,
                                               decimal_redacted_states, 3);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v21_max_decimal(
                       &mock, v21_decimal_max_literal_sql,
                       sizeof(v21_decimal_max_literal_sql) - 1, &capture) ==
                   PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
            assert(capture.calls == 0 && mock.executor_calls > 0 &&
                   mock.statement_failed == 1);
        }

        {
            const int32_t overflow_keys[] = {1, 1};
            static const uint8_t max_decimal_coefficient[2][16] = {
                {0x4B, 0x3B, 0x4C, 0xA8, 0x5A, 0x86, 0xC4, 0x7A,
                 0x09, 0x8A, 0x22, 0x3F, 0xFF, 0xFF, 0xFF, 0xFF},
                {0x4B, 0x3B, 0x4C, 0xA8, 0x5A, 0x86, 0xC4, 0x7A,
                 0x09, 0x8A, 0x22, 0x3F, 0xFF, 0xFF, 0xFF, 0xFF}};
            mock.planner_len = make_decimal_relation(
                mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99, 38,
                2);
            mock.executor_len = make_decimal_relation(mock.executor, 6, 0x22,
                                                      38, 2);
            mock.batch_len = make_decimal_rows_bytes(
                mock.batch, 1, overflow_keys, max_decimal_coefficient, 2);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v21_decimal_wide_sum(
                       &mock, v21_decimal_sum_overflow_sql,
                       sizeof(v21_decimal_sum_overflow_sql) - 1, &capture) ==
                   PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
            assert(capture.calls == 0 && mock.executor_calls > 0 &&
                   mock.statement_failed == 1);
        }

        {
            const int32_t overflow_keys[] = {1, 1};
            const int64_t overflow_scores[] = {INT64_MAX, 1};
            mock.planner_len = make_bigint_relation(
                mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99);
            mock.executor_len = make_bigint_relation(mock.executor, 6, 0x22);
            mock.batch_len = make_bigint_rows(
                mock.batch, 1, overflow_keys, overflow_scores, 2);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v7_integer_sum(
                       &mock, bigint_sum_overflow_sql,
                       sizeof(bigint_sum_overflow_sql) - 1, &capture) ==
                   PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
            assert(capture.calls == 0 && mock.executor_calls > 0 &&
                   mock.statement_failed == 1);
        }

        {
            const int32_t overflow_keys[] = {1, 1};
            const int64_t overflow_scores[] = {INT64_MAX, INT64_MAX};
            mock.planner_len = make_bigint_relation(
                mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99);
            mock.executor_len = make_bigint_relation(mock.executor, 6, 0x22);
            mock.batch_len = make_bigint_rows(
                mock.batch, 1, overflow_keys, overflow_scores, 2);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v21_integer_sum(
                       &mock, v21_bigint_sum_overflow_sql,
                       sizeof(v21_bigint_sum_overflow_sql) - 1,
                       on_group_v21_integer_sum_unexpected, &capture) ==
                   PRDB_SDK_SQL_EXEC_EVALUATION_ERROR);
            assert(capture.calls == 0 && mock.executor_calls > 0 &&
                   mock.statement_failed == 1);
        }

        {
            const int32_t keys_with_filtered_group[] = {1, 1, 2};
            const int64_t scores_with_filtered_group[] = {3, 3, 6};
            mock.planner_len = make_bigint_relation(
                mock.planner, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x99);
            mock.executor_len = make_bigint_relation(mock.executor, 6, 0x22);
            mock.batch_len = make_bigint_rows(
                mock.batch, 1, keys_with_filtered_group,
                scores_with_filtered_group, 3);
            reset_scan(&mock, &capture, 1);
            assert(run_query_v21_integer_sum(
                       &mock, v21_bigint_sum_literal_sql,
                       sizeof(v21_bigint_sum_literal_sql) - 1,
                       on_group_v21_integer_sum_literal, &capture) ==
                   PRDB_SDK_SQL_EXEC_OK);
            assert(capture.calls == 2 && mock.executor_calls > 0 &&
                   mock.statement_failed == 0);
        }
    }
    run_grouped_scalar_order_matrix(&mock);
    run_integral_aggregate_runner_matrix(&mock);
    return 0;
}
