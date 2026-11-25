#ifndef PRDB_SDK_OPTIMIZER_PLAN_CODEC_INTERNAL_H
#define PRDB_SDK_OPTIMIZER_PLAN_CODEC_INTERNAL_H

#include "prdb_sdk_optimizer_plan_v1.h"
#include "prdb_sdk_optimizer_v1.h"
#include "prdb_sdk_sql_v1.h"
#include "prdb_sdk_sql_exec_v1.h"
#include "prdb_sdk_sql_order_v1.h"
#include "prdb_sdk_sql_result_v1.h"
#include "prdb_query_abi_payload_codecs.h"

#include <stddef.h>
#include <limits.h>
#include <stdint.h>
#include <string.h>

#define OPTIMIZER_ORDERED_SELECT_MAX_SCAN_PAGES UINT32_C(124)
#define OPTIMIZER_ORDERED_SELECT_CALLBACK_POLL_INTERVAL UINT32_C(32)


#ifndef PRDB_SDK_INTERNAL
#define PRDB_SDK_INTERNAL __attribute__((visibility("hidden")))
#endif

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

typedef struct multi_point_probe_hint {
    uint8_t present;
    uint8_t index_id[16];
    prdb_sdk_optimizer_plan_column_v1 key_column;
    uint16_t count;
    const uint8_t *literals[64];
    uint32_t literal_lengths[64];
} multi_point_probe_hint;

typedef struct predicate_type {
    uint8_t is_null;
    uint8_t decimal_literal;
    uint8_t type_code;
    uint8_t parameters[4];
    uint8_t decimal_coefficient[16];
} predicate_type;

PRDB_SDK_INTERNAL int prdb_sdk_internal_ranges_overlap(const void *left, uint64_t left_len,
                          const void *right, uint64_t right_len);
PRDB_SDK_INTERNAL int prdb_sdk_internal_take(plan_reader *reader, uint64_t count, const uint8_t **out);
PRDB_SDK_INTERNAL int prdb_sdk_internal_read_u8(plan_reader *reader, uint8_t *out);
PRDB_SDK_INTERNAL int prdb_sdk_internal_read_text_predicate_header(plan_reader *reader,
                                      uint64_t header_offset,
                                      uint16_t *text_length);
PRDB_SDK_INTERNAL int prdb_sdk_internal_read_bytea_predicate_header(plan_reader *reader,
                                       uint64_t header_offset,
                                       uint16_t *bytea_length);
PRDB_SDK_INTERNAL int prdb_sdk_internal_read_integer_predicate_record(plan_reader *reader,
                                         uint64_t header_offset,
                                         int64_t *integer_value);
PRDB_SDK_INTERNAL int prdb_sdk_internal_read_date_predicate_record(plan_reader *reader,
                                      uint64_t header_offset,
                                      const uint8_t **date_bytes);
PRDB_SDK_INTERNAL int prdb_sdk_internal_read_uuid_predicate_record(plan_reader *reader,
                                      uint64_t header_offset,
                                      const uint8_t **uuid_bytes);
PRDB_SDK_INTERNAL int prdb_sdk_internal_read_timestamptz_predicate_record(plan_reader *reader,
                                             uint64_t header_offset,
                                             const uint8_t **timestamp_bytes);
PRDB_SDK_INTERNAL int prdb_sdk_internal_read_decimal_predicate_record(plan_reader *reader,
                                         uint64_t header_offset,
                                         const uint8_t **coefficient);
PRDB_SDK_INTERNAL int prdb_sdk_internal_decimal_coefficient_fits_precision(const uint8_t coefficient[16],
                                               uint8_t precision);
PRDB_SDK_INTERNAL int prdb_sdk_internal_read_column_predicate_record(plan_reader *reader,
                                        uint64_t header_offset,
                                        uint8_t column_id[16]);
PRDB_SDK_INTERNAL int prdb_sdk_internal_read_compare_predicate_header(plan_reader *reader,
                                        uint64_t header_offset,
                                        uint8_t *comparison_operator);
PRDB_SDK_INTERNAL int prdb_sdk_internal_read_boolean_predicate_header(plan_reader *reader,
                                         uint64_t header_offset,
                                         uint8_t *node_tag_out,
                                         uint8_t *child_count);
PRDB_SDK_INTERNAL int prdb_sdk_internal_read_not_predicate_tag(const plan_reader *reader,
                                  uint64_t header_offset);
PRDB_SDK_INTERNAL int prdb_sdk_internal_read_null_predicate_tag(const plan_reader *reader,
                                   uint64_t header_offset);
PRDB_SDK_INTERNAL int prdb_sdk_internal_read_boolean_literal_predicate_record(
    plan_reader *reader, uint64_t header_offset, uint8_t *boolean_value);
PRDB_SDK_INTERNAL int prdb_sdk_internal_read_column_list_header(plan_reader *reader, uint8_t expected_tag,
                                   uint16_t *column_count);
PRDB_SDK_INTERNAL int prdb_sdk_internal_read_limit_node(plan_reader *reader, uint32_t *row_limit);
PRDB_SDK_INTERNAL int prdb_sdk_internal_read_plan_node_count(plan_reader *reader, uint8_t *node_count);
PRDB_SDK_INTERNAL int prdb_sdk_internal_read_filter_node_tag(const plan_reader *reader,
                                uint64_t header_offset);
PRDB_SDK_INTERNAL int prdb_sdk_internal_valid_identifier(const uint8_t *bytes, uint32_t length);
PRDB_SDK_INTERNAL int prdb_sdk_internal_read_name(plan_reader *reader, const uint8_t **name_out,
                     uint32_t *length_out);
PRDB_SDK_INTERNAL int prdb_sdk_internal_all_zero(const uint8_t *bytes, uint32_t length);
PRDB_SDK_INTERNAL int prdb_sdk_internal_valid_type(uint8_t type_code, const uint8_t parameters[4]);
PRDB_SDK_INTERNAL int prdb_sdk_internal_read_column(plan_reader *reader,
                       prdb_sdk_optimizer_plan_column_v1 *column);
PRDB_SDK_INTERNAL int prdb_sdk_internal_valid_utf8(const uint8_t *bytes, uint32_t length);
PRDB_SDK_INTERNAL int prdb_sdk_internal_same_type(const predicate_type *left, const predicate_type *right);
PRDB_SDK_INTERNAL int prdb_sdk_internal_predicate_boolean(const predicate_type *type);
PRDB_SDK_INTERNAL int prdb_sdk_internal_decode_predicate(plan_reader *reader,
                            const prdb_sdk_optimizer_plan_column_v1 *columns,
                            uint32_t column_count, uint32_t depth,
                            uint32_t *nodes, uint8_t *references,
                            predicate_type *type_out);
PRDB_SDK_INTERNAL int prdb_sdk_internal_point_probe_matches_filter(
    const prdb_sdk_optimizer_filter_v1 *filter,
    const prdb_sdk_optimizer_plan_column_v1 *key_column,
    const uint8_t *literal, uint32_t literal_len);
PRDB_SDK_INTERNAL int prdb_sdk_internal_multi_point_probe_matches_filter(
    const prdb_sdk_optimizer_filter_v1 *filter,
    const prdb_sdk_optimizer_plan_column_v1 *key_column,
    const multi_point_probe_hint *hint);
PRDB_SDK_INTERNAL int prdb_sdk_internal_range_probe_matches_filter(
    const prdb_sdk_optimizer_filter_v1 *filter,
    const prdb_sdk_optimizer_plan_column_v1 *key_column,
    const range_probe_hint *hint);
PRDB_SDK_INTERNAL int prdb_sdk_internal_encode_point_probe_request(
    const prdb_sdk_optimizer_plan_v1 *plan,
    const prdb_sdk_executor_relation_v1 *relation,
    const point_probe_hint *hint, const uint8_t *relation_response,
    uint64_t relation_response_len, uint8_t *request, uint64_t capacity,
    uint64_t *request_len);
PRDB_SDK_INTERNAL int prdb_sdk_internal_range_literal_to_value(const uint8_t *literal,
                                  uint32_t literal_len,
                                  uint8_t expected_type,
                                  uint8_t decimal_precision,
                                  uint8_t integer_bytes[4],
                                  prdb_sdk_value_v1 *value);
PRDB_SDK_INTERNAL int prdb_sdk_internal_relation_response_column_ids(const uint8_t *response,
                                        uint64_t response_len,
                                        uint32_t column_count,
                                        uint8_t *column_ids,
                                        uint64_t column_ids_capacity);
PRDB_SDK_INTERNAL int prdb_sdk_internal_range_response_to_internal_batch(
    uint8_t *payload, uint64_t *payload_len, uint64_t capacity,
    uint32_t max_batch_bytes, uint16_t max_candidates);
PRDB_SDK_INTERNAL int prdb_sdk_internal_skip_predicate_node(plan_reader *reader, uint32_t depth);
PRDB_SDK_INTERNAL int32_t prdb_sdk_internal_evaluate_predicate_node(
    plan_reader *reader, const prdb_sdk_optimizer_plan_v1 *plan,
    const prdb_sdk_value_v1 *source_cells, predicate_value *out,
    uint32_t depth, uint32_t *nodes);
PRDB_SDK_INTERNAL int prdb_sdk_internal_columns_equal(const prdb_sdk_optimizer_plan_column_v1 *left,
                         const prdb_sdk_optimizer_plan_column_v1 *right);
PRDB_SDK_INTERNAL uint16_t prdb_sdk_internal_read_be_u16(const uint8_t *bytes);
PRDB_SDK_INTERNAL int32_t prdb_sdk_internal_decode_select_plan_with_access_path_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_plan_v1 *out, uint32_t out_capacity,
    prdb_sdk_optimizer_filter_v1 *filter_out, uint32_t filter_out_capacity,
    point_probe_hint *point_probe_out,
    range_probe_hint *range_probe_out,
    multi_point_probe_hint *multi_point_probe_out, uint8_t *empty_view_out);
PRDB_SDK_INTERNAL int32_t prdb_sdk_internal_decode_any_optimizer_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_plan_v1 *plan, uint32_t plan_capacity,
    prdb_sdk_optimizer_filter_v1 *filter, uint32_t filter_capacity);
PRDB_SDK_INTERNAL uint16_t prdb_sdk_internal_response_u16(const uint8_t *bytes, uint64_t offset);
PRDB_SDK_INTERNAL uint32_t prdb_sdk_internal_response_u32(const uint8_t *bytes, uint64_t offset);
PRDB_SDK_INTERNAL int32_t prdb_sdk_internal_execute_unfiltered_plan_internal(
    const prdb_host_api_v2 *host, const uint8_t *plan_payload,
    uint64_t plan_payload_len, uint16_t max_candidates,
    uint16_t max_index_candidates,
    uint32_t max_batch_bytes, uint8_t *relation_response,
    uint64_t relation_response_capacity, uint8_t *scan_response,
    uint64_t scan_response_capacity, uint8_t *batch_response,
    uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns,
    uint32_t relation_column_capacity, prdb_sdk_value_v1 *source_cells,
    uint32_t source_cell_capacity, prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out);

#endif
