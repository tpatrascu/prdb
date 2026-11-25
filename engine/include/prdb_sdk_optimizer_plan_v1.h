#ifndef PRDB_SDK_OPTIMIZER_PLAN_V1_H
#define PRDB_SDK_OPTIMIZER_PLAN_V1_H

#include "prdb_sdk_optimizer_v1.h"
#include "prdb_sdk_sql_v1.h"
#include "prdb_sdk_executor_v1.h"
#include "prdb_sdk_sql_dml_v1.h"
#include "prdb_query_abi_v1.h"

#define PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION UINT32_C(1)
#define PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS UINT32_C(256)
#define PRDB_SDK_OPTIMIZER_PLAN_MAX_PROJECTIONS UINT32_C(128)
#define PRDB_SDK_OPTIMIZER_PLAN_MAX_NEXT_REQUESTS UINT32_C(125)
#define PRDB_SDK_OPTIMIZER_PLAN_MAX_DELETE_TARGETS UINT32_C(128)
#define PRDB_SDK_OPTIMIZER_PLAN_MAX_UPDATE_ASSIGNMENTS UINT32_C(64)
#define PRDB_SDK_OPTIMIZER_PLAN_KIND_JOIN_SELECT UINT32_C(4)
#define PRDB_SDK_OPTIMIZER_PLAN_KIND_FILTERED_JOIN_SELECT UINT32_C(12)
#define PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_NESTED_LOOP_JOIN UINT32_C(17)
#define PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_PREFIX_NESTED_LOOP_JOIN UINT32_C(18)
#define PRDB_SDK_OPTIMIZER_PLAN_MAX_PARAMETER_INDEX UINT32_C(256)

#define PRDB_SDK_OPTIMIZER_PLAN_OK INT32_C(0)
#define PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT INT32_C(1)
#define PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD INT32_C(2)
#define PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED INT32_C(3)
#define PRDB_SDK_OPTIMIZER_PLAN_UNAVAILABLE INT32_C(4)
#define PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH INT32_C(5)
#define PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR INT32_C(6)
#define PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL INT32_C(7)
#define PRDB_SDK_OPTIMIZER_PLAN_ROW_CALLBACK_ERROR INT32_C(8)
#define PRDB_SDK_OPTIMIZER_PLAN_CANCELLED INT32_C(9)
#define PRDB_SDK_OPTIMIZER_PLAN_REQUEST_LIMIT INT32_C(10)
#define PRDB_SDK_OPTIMIZER_PLAN_FILTER_ERROR INT32_C(11)

/* Plan names borrow the encoded payload. Column arrays are owned by this
 * output record and contain stable catalog IDs, never executor handles. */
typedef struct prdb_sdk_optimizer_plan_column_v1 {
    uint8_t column_id[16];
    uint32_t ordinal;
    uint8_t type_code;
    uint8_t type_parameters[4];
    uint8_t nullable;
} prdb_sdk_optimizer_plan_column_v1;

typedef struct prdb_sdk_optimizer_plan_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    const uint8_t *schema_name;
    uint32_t schema_name_len;
    uint8_t schema_id[16];
    uint64_t schema_version;
    const uint8_t *relation_name;
    uint32_t relation_name_len;
    uint8_t relation_id[16];
    uint64_t table_epoch;
    uint32_t scan_column_count;
    prdb_sdk_optimizer_plan_column_v1
        scan_columns[PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS];
    uint32_t projection_count;
    prdb_sdk_optimizer_plan_column_v1
        projections[PRDB_SDK_OPTIMIZER_PLAN_MAX_PROJECTIONS];
    uint32_t has_limit;
    uint32_t limit;
    uint32_t plan_kind;
} prdb_sdk_optimizer_plan_v1;

/* Kind-7 ordered SELECT plans keep their sort key and direction alongside
 * the common scan/projection shape. */
typedef struct prdb_sdk_optimizer_ordered_select_plan_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_optimizer_plan_v1 plan;
    prdb_sdk_optimizer_plan_column_v1 order_key;
    uint32_t descending;
} prdb_sdk_optimizer_ordered_select_plan_v1;

/* A validated filter subtree borrows its canonical prefix bytes from the
 * plan payload. It is absent when the plan has no Filter node. */
typedef struct prdb_sdk_optimizer_filter_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    uint32_t present;
    const uint8_t *prefix;
    uint32_t prefix_len;
} prdb_sdk_optimizer_filter_v1;

typedef struct prdb_sdk_optimizer_assignment_v1 {
    prdb_sdk_optimizer_plan_column_v1 column;
    uint32_t value_kind;
    uint32_t parameter_index;
} prdb_sdk_optimizer_assignment_v1;

/* Kind-3 UPDATE plans expose a validated base relation/scan/filter plan and
 * source-order assignment records. Names and filter bytes borrow the payload. */
typedef struct prdb_sdk_optimizer_update_plan_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_optimizer_plan_v1 plan;
    prdb_sdk_optimizer_filter_v1 filter;
    uint32_t assignment_count;
    prdb_sdk_optimizer_assignment_v1
        assignments[PRDB_SDK_OPTIMIZER_PLAN_MAX_UPDATE_ASSIGNMENTS];
} prdb_sdk_optimizer_update_plan_v1;

typedef struct prdb_sdk_optimizer_join_relation_v1 {
    const uint8_t *relation_name;
    uint32_t relation_name_len;
    uint8_t relation_id[16];
    uint64_t table_epoch;
    uint32_t scan_column_count;
    prdb_sdk_optimizer_plan_column_v1
        scan_columns[PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS];
} prdb_sdk_optimizer_join_relation_v1;

typedef struct prdb_sdk_optimizer_join_projection_v1 {
    uint32_t relation_side;
    prdb_sdk_optimizer_plan_column_v1 column;
} prdb_sdk_optimizer_join_projection_v1;

typedef struct prdb_sdk_optimizer_join_key_v1 {
    prdb_sdk_optimizer_plan_column_v1 left_column;
    prdb_sdk_optimizer_plan_column_v1 right_column;
} prdb_sdk_optimizer_join_key_v1;

/* Kind-4 join plans retain independent metadata and scan shape for each side.
 * Relation/schema names borrow the payload; columns are copied. */
typedef struct prdb_sdk_optimizer_join_plan_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    const uint8_t *schema_name;
    uint32_t schema_name_len;
    uint8_t schema_id[16];
    uint64_t schema_version;
    prdb_sdk_optimizer_join_relation_v1 left_relation;
    prdb_sdk_optimizer_join_relation_v1 right_relation;
    uint32_t projection_count;
    prdb_sdk_optimizer_join_projection_v1
        projections[PRDB_SDK_OPTIMIZER_PLAN_MAX_PROJECTIONS];
    uint32_t join_key_count;
    prdb_sdk_optimizer_join_key_v1 join_keys[8];
    uint32_t has_limit;
    uint32_t limit;
} prdb_sdk_optimizer_join_plan_v1;

typedef struct prdb_sdk_optimizer_join_filter_v1 {
    uint32_t relation_side;
    prdb_sdk_optimizer_plan_column_v1 column;
    /* 0 parameter comparison, 1 IS NULL, 2 IS NOT NULL. */
    uint32_t expression_kind;
    /* 0 equals through 5 greater-or-equal, used for kind 0 only. */
    uint32_t comparison;
    uint32_t parameter_index;
} prdb_sdk_optimizer_join_filter_v1;

typedef struct prdb_sdk_optimizer_filtered_join_plan_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_optimizer_join_plan_v1 join;
    prdb_sdk_optimizer_join_filter_v1 filter;
} prdb_sdk_optimizer_filtered_join_plan_v1;

/* Kind-17 plan: existing join fields, an optional v20 filter, and the
 * authorized inner index selected for exact leading-key probes. */
typedef struct prdb_sdk_optimizer_index_join_plan_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_optimizer_join_plan_v1 join;
    uint32_t has_filter;
    prdb_sdk_optimizer_join_filter_v1 filter;
    uint32_t outer_side;
    uint32_t inner_side;
    uint8_t index_id[16];
    prdb_sdk_optimizer_plan_column_v1 key_column;
} prdb_sdk_optimizer_index_join_plan_v1;

/* Kind-18 plan for an indexed join using two to eight leading index keys. */
typedef struct prdb_sdk_optimizer_index_prefix_join_plan_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_optimizer_join_plan_v1 join;
    uint32_t has_filter;
    prdb_sdk_optimizer_join_filter_v1 filter;
    uint32_t outer_side;
    uint32_t inner_side;
    uint8_t index_id[16];
    uint32_t prefix_column_count;
    prdb_sdk_optimizer_plan_column_v1 prefix_columns[8];
} prdb_sdk_optimizer_index_prefix_join_plan_v1;

/* Kind-5 single-key COUNT(*) plan. Its scan shape contains exactly the
 * grouping key; relation and column names borrow the encoded payload. */
typedef struct prdb_sdk_optimizer_group_count_plan_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_optimizer_plan_v1 plan;
    prdb_sdk_optimizer_plan_column_v1 group_key;
} prdb_sdk_optimizer_group_count_plan_v1;

typedef struct prdb_sdk_optimizer_group_count_order_item_v1 {
    uint32_t source;
    uint32_t descending;
    uint32_t nulls_first;
} prdb_sdk_optimizer_group_count_order_item_v1;

typedef struct prdb_sdk_optimizer_ordered_group_count_plan_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_optimizer_plan_v1 plan;
    prdb_sdk_optimizer_plan_column_v1 group_key;
    uint32_t order_count;
    prdb_sdk_optimizer_group_count_order_item_v1
        order_items[PRDB_SDK_SQL_MAX_ORDER_KEYS];
} prdb_sdk_optimizer_ordered_group_count_plan_v1;

/* Kind-6 composite COUNT(*) plan. Group keys retain SQL order while the
 * embedded scan shape is unique and ordinal-sorted. */
typedef struct prdb_sdk_optimizer_composite_group_count_plan_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_optimizer_plan_v1 plan;
    uint32_t group_key_count;
    prdb_sdk_optimizer_plan_column_v1 group_keys[8];
} prdb_sdk_optimizer_composite_group_count_plan_v1;

typedef struct prdb_sdk_optimizer_composite_order_item_v1 {
    uint32_t source;
    uint32_t group_key_index;
    uint32_t descending;
    uint32_t nulls_first;
} prdb_sdk_optimizer_composite_order_item_v1;

typedef struct prdb_sdk_optimizer_ordered_composite_group_count_plan_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_optimizer_plan_v1 plan;
    uint32_t group_key_count;
    prdb_sdk_optimizer_plan_column_v1 group_keys[8];
    uint32_t order_count;
    prdb_sdk_optimizer_composite_order_item_v1 order_items[9];
} prdb_sdk_optimizer_ordered_composite_group_count_plan_v1;

typedef struct prdb_sdk_optimizer_group_aggregate_plan_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_optimizer_plan_v1 plan;
    prdb_sdk_optimizer_plan_column_v1 group_key;
    uint32_t aggregate_kind;
    uint32_t has_aggregate_column;
    prdb_sdk_optimizer_plan_column_v1 aggregate_column;
} prdb_sdk_optimizer_group_aggregate_plan_v1;

typedef struct prdb_sdk_optimizer_composite_group_aggregate_plan_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_optimizer_plan_v1 plan;
    uint32_t group_key_count;
    prdb_sdk_optimizer_plan_column_v1 group_keys[8];
    uint32_t aggregate_kind;
    uint32_t has_aggregate_column;
    prdb_sdk_optimizer_plan_column_v1 aggregate_column;
} prdb_sdk_optimizer_composite_group_aggregate_plan_v1;

typedef struct prdb_sdk_optimizer_filtered_composite_group_aggregate_plan_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_optimizer_plan_v1 plan;
    prdb_sdk_optimizer_filter_v1 filter;
    uint32_t group_key_count;
    prdb_sdk_optimizer_plan_column_v1 group_keys[8];
    uint32_t aggregate_kind;
    uint32_t has_aggregate_column;
    prdb_sdk_optimizer_plan_column_v1 aggregate_column;
} prdb_sdk_optimizer_filtered_composite_group_aggregate_plan_v1;

typedef struct prdb_sdk_optimizer_ordered_filtered_composite_group_aggregate_plan_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_optimizer_plan_v1 plan;
    prdb_sdk_optimizer_filter_v1 filter;
    uint32_t group_key_count;
    prdb_sdk_optimizer_plan_column_v1 group_keys[8];
    uint32_t aggregate_kind;
    uint32_t has_aggregate_column;
    prdb_sdk_optimizer_plan_column_v1 aggregate_column;
    uint32_t order_count;
    prdb_sdk_optimizer_composite_order_item_v1 order_items[9];
} prdb_sdk_optimizer_ordered_filtered_composite_group_aggregate_plan_v1;

typedef struct prdb_sdk_optimizer_ordered_composite_group_aggregate_plan_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_optimizer_plan_v1 plan;
    uint32_t group_key_count;
    prdb_sdk_optimizer_plan_column_v1 group_keys[8];
    uint32_t aggregate_kind;
    uint32_t has_aggregate_column;
    prdb_sdk_optimizer_plan_column_v1 aggregate_column;
    uint32_t order_count;
    prdb_sdk_optimizer_composite_order_item_v1 order_items[9];
} prdb_sdk_optimizer_ordered_composite_group_aggregate_plan_v1;

typedef struct prdb_sdk_optimizer_ordered_group_aggregate_plan_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    prdb_sdk_optimizer_plan_v1 plan;
    prdb_sdk_optimizer_plan_column_v1 group_key;
    uint32_t aggregate_kind;
    uint32_t has_aggregate_column;
    prdb_sdk_optimizer_plan_column_v1 aggregate_column;
    uint32_t order_count;
    prdb_sdk_optimizer_group_count_order_item_v1
        order_items[PRDB_SDK_SQL_MAX_ORDER_KEYS];
} prdb_sdk_optimizer_ordered_group_aggregate_plan_v1;

typedef struct prdb_sdk_optimizer_scan_state_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    uint32_t projection_count;
    uint32_t has_limit;
    uint32_t limit;
    uint32_t delivered_rows;
    uint32_t done;
    prdb_sdk_optimizer_plan_column_v1
        projections[PRDB_SDK_OPTIMIZER_PLAN_MAX_PROJECTIONS];
} prdb_sdk_optimizer_scan_state_v1;

typedef int32_t (*prdb_sdk_optimizer_projected_row_v1_fn)(
    void *context, const prdb_sdk_optimizer_plan_column_v1 *projections,
    const prdb_sdk_value_v1 *values, uint32_t projection_count);

/* Independently decodes the canonical plan v1 payload for the initial
 * Scan -> Project -> optional Limit subset. Filter plans return UNSUPPORTED;
 * use decode_plan_with_filter_v1 for the complete pipeline. Names borrow the
 * payload; the output record must not overlap it. */
int32_t prdb_sdk_optimizer_decode_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_plan_v1 *out, uint32_t out_capacity);

int32_t prdb_sdk_optimizer_decode_ordered_select_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_ordered_select_plan_v1 *out, uint32_t out_capacity);

int32_t prdb_sdk_optimizer_match_ordered_select_plan_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len);

/* Decode all plan-v1 pipelines, independently validating a Filter node's
 * bounded prefix tree, scanned column references, scalar compatibility, and
 * Boolean root. `filter_out` is written only after the whole plan succeeds. */
int32_t prdb_sdk_optimizer_decode_plan_with_filter_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_plan_v1 *out, uint32_t out_capacity,
    prdb_sdk_optimizer_filter_v1 *filter_out, uint32_t filter_out_capacity);

/* Independently decode a canonical DELETE `Scan -> Filter -> Delete` plan.
 * Predicate bytes borrow the payload. DELETE plans expose no projections or
 * limit and may have an empty scan-column list for constant predicates. */
int32_t prdb_sdk_optimizer_decode_delete_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_plan_v1 *out, uint32_t out_capacity,
    prdb_sdk_optimizer_filter_v1 *filter_out,
    uint32_t filter_out_capacity);

/* Independently decode canonical kind-3 `Scan -> Filter -> Update` plans,
 * checking exact predicate/assignment scan shape, stable target metadata,
 * target uniqueness, parameter bounds, NULLability, and canonical values. */
int32_t prdb_sdk_optimizer_decode_update_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_update_plan_v1 *out, uint32_t out_capacity);

/* Independently decodes kind-4 two-relation equality join plans, checking
 * both scan shapes, side-qualified projections, key references/types, LIMIT,
 * stable relation identities, and canonical framing. */
int32_t prdb_sdk_optimizer_decode_join_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_join_plan_v1 *out, uint32_t out_capacity);

/* Independently decodes kind 12, including the authorized filter column and
 * its bounded comparison or NULL-test metadata. */
int32_t prdb_sdk_optimizer_decode_filtered_join_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_filtered_join_plan_v1 *out, uint32_t out_capacity);

int32_t prdb_sdk_optimizer_decode_index_join_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_index_join_plan_v1 *out, uint32_t out_capacity);

int32_t prdb_sdk_optimizer_decode_index_prefix_join_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_index_prefix_join_plan_v1 *out,
    uint32_t out_capacity);

/* Independently decode kind-5 single-key COUNT(*) plans, requiring an exact
 * one-column scan shape and canonical relation/limit records. */
int32_t prdb_sdk_optimizer_decode_group_count_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_group_count_plan_v1 *out, uint32_t out_capacity);

int32_t prdb_sdk_optimizer_decode_ordered_group_count_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_ordered_group_count_plan_v1 *out,
    uint32_t out_capacity);

/* Independently decode canonical kind-6 composite COUNT(*) plans, requiring
 * two-to-eight ordered keys and an exact ordinal-sorted scan-key set. */
int32_t prdb_sdk_optimizer_decode_composite_group_count_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_composite_group_count_plan_v1 *out,
    uint32_t out_capacity);

int32_t prdb_sdk_optimizer_decode_ordered_composite_group_count_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_ordered_composite_group_count_plan_v1 *out,
    uint32_t out_capacity);

int32_t prdb_sdk_optimizer_decode_group_aggregate_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_group_aggregate_plan_v1 *out,
    uint32_t out_capacity);

int32_t prdb_sdk_optimizer_decode_composite_group_aggregate_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_composite_group_aggregate_plan_v1 *out,
    uint32_t out_capacity);

int32_t prdb_sdk_optimizer_decode_filtered_composite_group_aggregate_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_filtered_composite_group_aggregate_plan_v1 *out,
    uint32_t out_capacity);

int32_t prdb_sdk_optimizer_decode_ordered_filtered_composite_group_aggregate_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_ordered_filtered_composite_group_aggregate_plan_v1 *out,
    uint32_t out_capacity);

int32_t prdb_sdk_optimizer_decode_ordered_composite_group_aggregate_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_ordered_composite_group_aggregate_plan_v1 *out,
    uint32_t out_capacity);

int32_t prdb_sdk_optimizer_decode_ordered_group_aggregate_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_ordered_group_aggregate_plan_v1 *out,
    uint32_t out_capacity);

/* Require a decoded kind-5 plan to match the parsed single-key COUNT(*)
 * statement, authorized grouping-key binding, schema, and optional LIMIT. */
int32_t prdb_sdk_optimizer_match_group_count_plan_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len);

int32_t prdb_sdk_optimizer_match_ordered_group_count_plan_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len);

int32_t prdb_sdk_optimizer_match_composite_group_count_plan_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len);

int32_t prdb_sdk_optimizer_match_composite_group_aggregate_plan_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len);

int32_t prdb_sdk_optimizer_match_filtered_composite_group_aggregate_plan_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len);

int32_t prdb_sdk_optimizer_match_ordered_filtered_composite_group_aggregate_plan_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len);

int32_t prdb_sdk_optimizer_match_ordered_composite_group_count_plan_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len);

int32_t prdb_sdk_optimizer_match_ordered_composite_group_aggregate_plan_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len);

int32_t prdb_sdk_optimizer_match_group_aggregate_plan_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len);

int32_t prdb_sdk_optimizer_match_ordered_group_aggregate_plan_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len);

/* Require a decoded kind-4 plan to preserve the parsed v19 join's qualified
 * projections, composite keys, relation names, and LIMIT. Relation IDs and
 * scan column identities are checked separately against executor storage. */
int32_t prdb_sdk_optimizer_match_join_plan_v19_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len);

/* Require a decoded kind-12 plan to preserve the v20 join and its bound
 * side-qualified WHERE column, comparison, and parameter index. */
int32_t prdb_sdk_optimizer_match_filtered_join_plan_v20_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len);

/* Validate a DELETE plan against the parsed DELETE statement and its bound
 * parameters, then re-resolve its relation through executor storage. */
int32_t prdb_sdk_optimizer_validate_delete_plan_with_parameters_v1(
    const prdb_host_api_v1 *host, const prdb_sdk_delete_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len,
    uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len);

/* Validate a returned UPDATE plan against its parsed AST and parameters,
 * then re-resolve the relation through executor storage. */
int32_t prdb_sdk_optimizer_validate_update_plan_with_parameters_v1(
    const prdb_host_api_v1 *host, const prdb_sdk_update_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len,
    uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len);

/* Submit, verify, scan, and execute a parsed DELETE through ABI v5. Matched
 * opaque row handles are staged only through ordinary executor mutation IPC. */
int32_t prdb_sdk_optimizer_execute_delete_ast_v1(
    const prdb_host_api_v1 *host, const prdb_sdk_delete_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns,
    uint32_t relation_column_capacity,
    prdb_sdk_select_binding_v1 *binding, uint32_t binding_capacity,
    prdb_sdk_value_v1 *cell_scratch, uint32_t cell_scratch_capacity,
    uint8_t *target_handles, uint32_t target_capacity,
    uint32_t *deleted_count_out);

/* Submit, verify, scan and execute a parsed UPDATE through ABI v5. Row handles
 * are collected before staging so scan iteration completes before writes. */
int32_t prdb_sdk_optimizer_execute_update_ast_v1(
    const prdb_host_api_v1 *host, const prdb_sdk_update_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns,
    uint32_t relation_column_capacity,
    prdb_sdk_select_binding_v1 *binding, uint32_t binding_capacity,
    prdb_sdk_value_v1 *cell_scratch, uint32_t cell_scratch_capacity,
    uint8_t *target_handles, uint32_t target_capacity,
    uint32_t *updated_count_out);

/* Re-resolve a validated plan's qualified relation through executor storage
 * and require its stable identity and scanned column definitions to match.
 * The response buffer remains caller-owned. No executor handle is returned. */
int32_t prdb_sdk_optimizer_validate_executor_relation_v1(
    const prdb_host_api_v1 *host, const uint8_t *plan_payload,
    uint64_t plan_payload_len, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len);

/* Re-resolve both relations in a validated kind-4 join plan through executor
 * storage and require each stable identity and scan shape to match. */
int32_t prdb_sdk_optimizer_validate_executor_join_relations_v1(
    const prdb_host_api_v1 *host, const uint8_t *plan_payload,
    uint64_t plan_payload_len, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len);

/* Re-resolve both relations in a validated kind-12 join plan through executor
 * storage and require each stable identity and filtered scan shape to match. */
int32_t prdb_sdk_optimizer_validate_executor_filtered_join_relations_v1(
    const prdb_host_api_v1 *host, const uint8_t *plan_payload,
    uint64_t plan_payload_len, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len);

/* Re-resolve the single relation in a validated kind-5 plan and compare the
 * stable identity plus the exact grouping-key column definition. */
int32_t prdb_sdk_optimizer_validate_executor_group_count_relation_v1(
    const prdb_host_api_v1 *host, const uint8_t *plan_payload,
    uint64_t plan_payload_len, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len);

int32_t prdb_sdk_optimizer_validate_executor_composite_group_count_relation_v1(
    const prdb_host_api_v1 *host, const uint8_t *plan_payload,
    uint64_t plan_payload_len, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len);

/* First validates plan metadata as above, then obtains a regular executor
 * relation handle. Relation/name outputs are valid only on success; column
 * shapes describe the full authorized relation and borrow response names. */
int32_t prdb_sdk_optimizer_acquire_plan_relation_v1(
    const prdb_host_api_v1 *host, const uint8_t *plan_payload,
    uint64_t plan_payload_len, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *columns, uint32_t column_capacity);

/* Acquire the checked relation capability and open its full scan. A zero
 * LIMIT returns success with scan_opened_out=0 and a zero cursor. Relation
 * names and column metadata borrow relation_response; cursor bytes are copied. */
int32_t prdb_sdk_optimizer_open_plan_scan_v1(
    const prdb_host_api_v1 *host, const uint8_t *plan_payload,
    uint64_t plan_payload_len, uint8_t *relation_response,
    uint64_t relation_response_capacity, uint64_t *relation_response_len,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint64_t *scan_response_len, prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *columns, uint32_t column_capacity,
    uint8_t cursor_out[16], uint32_t *scan_opened_out);

/* Initialize and consume the initial unfiltered scan pipeline. Each callback
 * receives projection-ordered values borrowing the current immutable batch. */
int32_t prdb_sdk_optimizer_scan_state_init_v1(
    const uint8_t *plan_payload, uint64_t plan_payload_len,
    prdb_sdk_optimizer_scan_state_v1 *state_out,
    uint32_t state_out_capacity);
int32_t prdb_sdk_optimizer_consume_scan_batch_v1(
    prdb_sdk_optimizer_scan_state_v1 *state,
    const prdb_sdk_batch_v1 *batch, uint32_t relation_column_count,
    prdb_sdk_value_v1 *source_cells, uint32_t source_cell_capacity,
    prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out);

/* Consume a batch using a fully validated plan. Filter predicates are
 * evaluated against the scanned row before projection; only TRUE rows emit. */
int32_t prdb_sdk_optimizer_consume_plan_batch_v1(
    const uint8_t *plan_payload, uint64_t plan_payload_len,
    prdb_sdk_optimizer_scan_state_v1 *state,
    const prdb_sdk_batch_v1 *batch, uint32_t relation_column_count,
    prdb_sdk_value_v1 *source_cells, uint32_t source_cell_capacity,
    prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out);

/* Executes a bounded scan: validate/acquire/open, poll
 * cancellation between pages, project values, apply LIMIT, and close an early
 * cursor. Filter evaluation errors fail the statement. */
int32_t prdb_sdk_optimizer_execute_unfiltered_plan_v1(
    const prdb_host_api_v1 *host, const uint8_t *plan_payload,
    uint64_t plan_payload_len, uint16_t max_candidates,
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

/* Submit an already parsed explicit-projection SELECT AST to the isolated
 * optimizer and execute its returned unfiltered plan through the executor.
 * ENCODE_UNSUPPORTED leaves the statement unlatchable so callers can use the
 * ordinary SQL path; execution errors latch statement failure through ABI v3. */
int32_t prdb_sdk_optimizer_execute_select_ast_v1(
    const prdb_host_api_v1 *host, const prdb_sdk_select_v1 *statement,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns,
    uint32_t relation_column_capacity, prdb_sdk_value_v1 *source_cells,
    uint32_t source_cell_capacity, prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out);

/* Submit SQL v3's bounded one-key ORDER BY query, validate the kind-7 plan
 * against the AST and executor metadata, then stable-sort at most 128 rows
 * before projecting and applying LIMIT. */
int32_t prdb_sdk_optimizer_execute_ordered_select_ast_v1(
    const prdb_host_api_v1 *host, const prdb_sdk_select_v1 *statement,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns,
    uint32_t relation_column_capacity, prdb_sdk_value_v1 *source_cells,
    uint32_t source_cell_capacity, prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity, uint8_t *materialized_rows,
    uint64_t materialized_rows_capacity,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out);

/* Parameterized counterpart that submits and validates supported WHERE
 * predicates before executing a filtered plan. */
int32_t prdb_sdk_optimizer_execute_select_ast_with_parameters_v1(
    const prdb_host_api_v1 *host, const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns,
    uint32_t relation_column_capacity, prdb_sdk_value_v1 *source_cells,
    uint32_t source_cell_capacity, prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out);

/* Execute the supported explicit-projection SELECT subset and emit one SQLR
 * v1 record per row through the host's provisional result callback. */
int32_t prdb_sdk_optimizer_execute_select_ast_to_result_v1(
    const prdb_host_api_v1 *host, const prdb_sdk_select_v1 *statement,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns,
    uint32_t relation_column_capacity, prdb_sdk_value_v1 *source_cells,
    uint32_t source_cell_capacity, prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity, uint8_t *result_record_buffer,
    uint64_t result_record_capacity, uint32_t *delivered_rows_out);

int32_t prdb_sdk_optimizer_execute_select_ast_with_parameters_to_result_v1(
    const prdb_host_api_v1 *host, const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns,
    uint32_t relation_column_capacity, prdb_sdk_value_v1 *source_cells,
    uint32_t source_cell_capacity, prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity, uint8_t *result_record_buffer,
    uint64_t result_record_capacity, uint32_t *delivered_rows_out);

/* Versioned variant with an independent trusted cumulative candidate ceiling
 * for a batched index probe. max_candidates remains the per-page scan bound. */
int32_t prdb_sdk_optimizer_execute_select_ast_with_parameters_to_result_v2(
    const prdb_host_api_v1 *host, const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint16_t max_index_candidates, uint32_t max_batch_bytes,
    uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns,
    uint32_t relation_column_capacity, prdb_sdk_value_v1 *source_cells,
    uint32_t source_cell_capacity, prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity, uint8_t *result_record_buffer,
    uint64_t result_record_capacity, uint32_t *delivered_rows_out);

/* Verify that a returned plan represents the submitted SQL relation,
 * projection names/order, and LIMIT using executor-authorized column names. */
int32_t prdb_sdk_optimizer_validate_select_plan_v1(
    const prdb_host_api_v1 *host, const prdb_sdk_select_v1 *statement,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len,
    uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len);

/* Validate a filtered returned plan against the submitted AST and supplied
 * parameters as well as executor-authorized relation metadata. */
int32_t prdb_sdk_optimizer_validate_select_plan_with_parameters_v1(
    const prdb_host_api_v1 *host, const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len,
    uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len);

#endif /* PRDB_SDK_OPTIMIZER_PLAN_V1_H */
