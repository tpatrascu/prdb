#ifndef PRDB_SDK_SQL_V1_H
#define PRDB_SDK_SQL_V1_H

#include "prdb_sdk_batch_v1.h"
#include "prdb_sdk_value_v1.h"

#include <stdint.h>

#define PRDB_SDK_SQL_ABI_V1_VERSION UINT32_C(1)
#define PRDB_SDK_SQL_MAX_QUERY_BYTES UINT64_C(65536)
#define PRDB_SDK_SQL_MAX_IDENTIFIER_BYTES UINT32_C(128)
#define PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS UINT32_C(64)
#define PRDB_SDK_SQL_MAX_PARAMETER_INDEX UINT32_C(65535)
#define PRDB_SDK_SQL_MAX_PREDICATE_NODES UINT32_C(64)
#define PRDB_SDK_SQL_MAX_EXPRESSION_DEPTH UINT32_C(32)
#define PRDB_SDK_SQL_MAX_RELATION_COLUMNS UINT32_C(4096)
#define PRDB_SDK_SQL_MAX_HAVING_NODES PRDB_SDK_SQL_MAX_PREDICATE_NODES
#define PRDB_SDK_SQL_MAX_ORDER_KEYS UINT32_C(9)
#define PRDB_SDK_SQL_MIN_GROUP_KEYS UINT32_C(2)
#define PRDB_SDK_SQL_MAX_GROUP_KEYS UINT32_C(8)
#define PRDB_SDK_SQL_MAX_JOIN_KEYS UINT32_C(8)
#define PRDB_SDK_SQL_MAX_IN_VALUES UINT32_C(32)

#define PRDB_SDK_SQL_PARAMETER_METADATA_ABI_V1_VERSION UINT32_C(1)

/* SQL-only type information that cannot be carried by prdb_sdk_value_v1,
 * notably Decimal precision/scale. The value remains independently versioned. */
typedef struct prdb_sdk_sql_parameter_metadata_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    uint32_t type_code;
    uint8_t decimal_precision;
    uint8_t decimal_scale;
    uint8_t reserved[2];
} prdb_sdk_sql_parameter_metadata_v1;
#define PRDB_SDK_SQL_MAX_LIMIT UINT32_C(1000000)

#define PRDB_SDK_SQL_OK INT32_C(0)
#define PRDB_SDK_SQL_UNSUPPORTED_STATEMENT INT32_C(1)
#define PRDB_SDK_SQL_SYNTAX_ERROR INT32_C(2)
#define PRDB_SDK_SQL_LIMIT_EXCEEDED INT32_C(3)
#define PRDB_SDK_SQL_INVALID_ARGUMENT INT32_C(4)

#define PRDB_SDK_SQL_BIND_OK INT32_C(0)
#define PRDB_SDK_SQL_BIND_INVALID_ARGUMENT INT32_C(1)
#define PRDB_SDK_SQL_BIND_INVALID_STATEMENT INT32_C(2)
#define PRDB_SDK_SQL_BIND_RELATION_MISMATCH INT32_C(3)
#define PRDB_SDK_SQL_BIND_COLUMN_NOT_FOUND INT32_C(4)
#define PRDB_SDK_SQL_BIND_AMBIGUOUS_COLUMN INT32_C(5)
#define PRDB_SDK_SQL_BIND_INVALID_SCHEMA INT32_C(6)
#define PRDB_SDK_SQL_BIND_INVALID_AGGREGATE_TYPE INT32_C(7)
#define PRDB_SDK_SQL_BIND_JOIN_TYPE_MISMATCH INT32_C(8)
#define PRDB_SDK_SQL_BIND_LITERAL_TYPE_MISMATCH INT32_C(9)

#define PRDB_SDK_SQL_COMPARE_EQUAL UINT32_C(1)
#define PRDB_SDK_SQL_COMPARE_NOT_EQUAL UINT32_C(2)
#define PRDB_SDK_SQL_COMPARE_LESS UINT32_C(3)
#define PRDB_SDK_SQL_COMPARE_LESS_EQUAL UINT32_C(4)
#define PRDB_SDK_SQL_COMPARE_GREATER UINT32_C(5)
#define PRDB_SDK_SQL_COMPARE_GREATER_EQUAL UINT32_C(6)
#define PRDB_SDK_SQL_COMPARE_IS_NULL UINT32_C(7)
#define PRDB_SDK_SQL_COMPARE_IS_NOT_NULL UINT32_C(8)

#define PRDB_SDK_SQL_PREDICATE_COMPARE UINT32_C(1)
#define PRDB_SDK_SQL_PREDICATE_IS_NULL UINT32_C(2)
#define PRDB_SDK_SQL_PREDICATE_IS_NOT_NULL UINT32_C(3)
#define PRDB_SDK_SQL_PREDICATE_AND UINT32_C(4)
#define PRDB_SDK_SQL_PREDICATE_OR UINT32_C(5)
#define PRDB_SDK_SQL_PREDICATE_NOT UINT32_C(6)

#define PRDB_SDK_SQL_AGGREGATE_COUNT_STAR UINT32_C(1)
#define PRDB_SDK_SQL_AGGREGATE_MIN UINT32_C(2)
#define PRDB_SDK_SQL_AGGREGATE_MAX UINT32_C(3)
#define PRDB_SDK_SQL_AGGREGATE_SUM UINT32_C(4)
#define PRDB_SDK_SQL_AGGREGATE_AVG UINT32_C(5)
#define PRDB_SDK_SQL_AGGREGATE_COUNT_COLUMN UINT32_C(6)

#define PRDB_SDK_SQL_JOIN_LEFT UINT32_C(0)
#define PRDB_SDK_SQL_JOIN_RIGHT UINT32_C(1)

#define PRDB_SDK_SQL_ORDER_GROUP_KEY UINT32_C(1)
#define PRDB_SDK_SQL_ORDER_AGGREGATE UINT32_C(2)
#define PRDB_SDK_SQL_ORDER_ASCENDING UINT32_C(0)
#define PRDB_SDK_SQL_ORDER_DESCENDING UINT32_C(1)
#define PRDB_SDK_SQL_NULL_ORDER_DEFAULT UINT32_C(0)
#define PRDB_SDK_SQL_NULLS_FIRST UINT32_C(1)
#define PRDB_SDK_SQL_NULLS_LAST UINT32_C(2)

#define PRDB_SDK_SQL_HAVING_OPERAND_PARAMETER UINT32_C(1)
#define PRDB_SDK_SQL_HAVING_OPERAND_BIGINT_LITERAL UINT32_C(2)
#define PRDB_SDK_SQL_HAVING_OPERAND_NUMERIC_LITERAL UINT32_C(3)

#define PRDB_SDK_SQL_TRUTH_UNKNOWN UINT32_C(0)
#define PRDB_SDK_SQL_TRUTH_FALSE UINT32_C(1)
#define PRDB_SDK_SQL_TRUTH_TRUE UINT32_C(2)

#define PRDB_SDK_SQL_EVAL_OK INT32_C(0)
#define PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT INT32_C(1)
#define PRDB_SDK_SQL_EVAL_INVALID_VALUE INT32_C(2)
#define PRDB_SDK_SQL_EVAL_TYPE_MISMATCH INT32_C(3)
#define PRDB_SDK_SQL_EVAL_REDACTED_INPUT INT32_C(4)
#define PRDB_SDK_SQL_EVAL_PARAMETER_MISSING INT32_C(5)
#define PRDB_SDK_SQL_EVAL_NUMERIC_OVERFLOW INT32_C(6)

typedef struct prdb_sdk_sql_span_v1 {
    uint32_t offset;
    uint32_t length;
} prdb_sdk_sql_span_v1;

typedef struct prdb_sdk_predicate_node_v1 {
    uint32_t node_kind;
    uint32_t left_child;
    uint32_t right_child;
    uint32_t comparison;
    prdb_sdk_sql_span_v1 column;
    uint32_t parameter_index;
} prdb_sdk_predicate_node_v1;

/* Borrowed column metadata from one authorized relation description. */
typedef struct prdb_sdk_sql_column_shape_v1 {
    const uint8_t *name;
    uint32_t name_len;
    uint32_t ordinal;
    uint32_t type_code;
    uint8_t decimal_precision;
    uint8_t decimal_scale;
} prdb_sdk_sql_column_shape_v1;

typedef struct prdb_sdk_sql_bound_column_v1 {
    uint32_t ordinal;
    uint32_t type_code;
    uint8_t decimal_precision;
    uint8_t decimal_scale;
} prdb_sdk_sql_bound_column_v1;

typedef struct prdb_sdk_sql_qualified_column_v1 {
    prdb_sdk_sql_span_v1 relation;
    prdb_sdk_sql_span_v1 column;
} prdb_sdk_sql_qualified_column_v1;

typedef struct prdb_sdk_sql_join_projection_binding_v1 {
    uint32_t relation_side;
    prdb_sdk_sql_bound_column_v1 column;
} prdb_sdk_sql_join_projection_binding_v1;

typedef struct prdb_sdk_sql_join_key_binding_v1 {
    prdb_sdk_sql_bound_column_v1 left;
    prdb_sdk_sql_bound_column_v1 right;
} prdb_sdk_sql_join_key_binding_v1;

typedef struct prdb_sdk_sql_order_item_v1 {
    uint32_t kind;
    uint32_t group_key_index;
    uint32_t direction;
    uint32_t null_order;
} prdb_sdk_sql_order_item_v1;

typedef struct prdb_sdk_sql_bound_order_item_v1 {
    prdb_sdk_sql_order_item_v1 item;
    uint32_t type_code;
    uint8_t decimal_precision;
    uint8_t decimal_scale;
} prdb_sdk_sql_bound_order_item_v1;

/* BIGINT literal bits use canonical two's-complement signed encoding.
 * Numeric literal coefficient is signed big-endian two's complement. */
typedef struct prdb_sdk_sql_having_node_v1 {
    uint32_t node_kind;
    uint32_t left_child;
    uint32_t right_child;
    uint32_t comparison;
    uint32_t operand_kind;
    uint32_t parameter_index;
    uint64_t literal;
    uint8_t coefficient[16];
    uint8_t scale;
    uint8_t reserved[7];
} prdb_sdk_sql_having_node_v1;

/* Unified SELECT AST supporting single-relation SELECT (WHERE predicates, IN
 * lowering, ORDER BY, single/composite GROUP BY, aggregates, HAVING, LIMIT) and
 * two-relation INNER JOIN (aliases, qualified projections, composite join keys,
 * WHERE, ORDER BY, LIMIT). */
typedef struct prdb_sdk_select_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    const uint8_t *sql;
    uint64_t sql_len;
    prdb_sdk_sql_span_v1 relation;
    prdb_sdk_sql_span_v1 projection[PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS];
    uint32_t projection_count;
    uint32_t projection_star;
    uint32_t predicate_count;
    uint32_t predicate_root;
    prdb_sdk_predicate_node_v1
        predicates[PRDB_SDK_SQL_MAX_PREDICATE_NODES];
    uint32_t has_limit;
    uint32_t limit;
    uint32_t has_order_by;
    prdb_sdk_sql_span_v1 order_by_column;
    uint32_t order_descending;
    uint32_t has_group_by;
    prdb_sdk_sql_span_v1 group_by_column;
    uint32_t aggregate_kind;
    prdb_sdk_sql_span_v1 aggregate_column;
    uint32_t group_key_count;
    prdb_sdk_sql_span_v1 projection_keys[PRDB_SDK_SQL_MAX_GROUP_KEYS];
    prdb_sdk_sql_span_v1 group_keys[PRDB_SDK_SQL_MAX_GROUP_KEYS];
    uint32_t has_having;
    uint32_t having_comparison;
    uint32_t having_parameter_index;
    uint32_t has_having_literal;
    uint64_t having_literal;
    uint32_t having_node_count;
    uint32_t having_root;
    prdb_sdk_sql_having_node_v1
        having_nodes[PRDB_SDK_SQL_MAX_HAVING_NODES];
    uint64_t having_count_mask;
    uint32_t order_kind;
    uint32_t order_direction;
    uint32_t null_order;
    uint32_t order_count;
    prdb_sdk_sql_order_item_v1 order_items[PRDB_SDK_SQL_MAX_ORDER_KEYS];
    uint32_t is_join;
    prdb_sdk_sql_span_v1 left_relation;
    prdb_sdk_sql_span_v1 right_relation;
    prdb_sdk_sql_span_v1 left_alias;
    prdb_sdk_sql_span_v1 right_alias;
    prdb_sdk_sql_qualified_column_v1
        join_projections[PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS];
    uint32_t join_projection_count;
    uint32_t join_key_count;
    prdb_sdk_sql_qualified_column_v1
        left_join_keys[PRDB_SDK_SQL_MAX_JOIN_KEYS];
    prdb_sdk_sql_qualified_column_v1
        right_join_keys[PRDB_SDK_SQL_MAX_JOIN_KEYS];
    uint32_t has_join_where;
    prdb_sdk_sql_qualified_column_v1 join_where_column;
    uint32_t join_where_comparison;
    uint32_t join_where_parameter_index;
    uint32_t where_keyword_offset;
    uint32_t where_clause_end;
    uint32_t from_keyword_offset;
    uint32_t order_keyword_offset;
    uint32_t join_has_order_by;
    prdb_sdk_sql_qualified_column_v1 join_order_by_column;
    uint32_t join_order_descending;
} prdb_sdk_select_v1;

typedef struct prdb_sdk_select_binding_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    uint32_t projection_star;
    uint32_t projection_count;
    uint32_t projection_ordinals[PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS];
    uint32_t relation_column_count;
    uint32_t predicate_count;
    prdb_sdk_sql_bound_column_v1
        predicate_columns[PRDB_SDK_SQL_MAX_PREDICATE_NODES];
    uint32_t has_order_by;
    prdb_sdk_sql_bound_column_v1 order_by_column;
    uint32_t has_group_by;
    prdb_sdk_sql_bound_column_v1 group_by_column;
    uint32_t aggregate_kind;
    prdb_sdk_sql_bound_column_v1 aggregate_column;
    uint32_t group_key_count;
    prdb_sdk_sql_bound_column_v1 group_keys[PRDB_SDK_SQL_MAX_GROUP_KEYS];
    uint32_t has_having;
    uint32_t having_comparison;
    uint32_t having_parameter_index;
    uint32_t having_type_code;
    uint8_t having_decimal_precision;
    uint8_t having_decimal_scale;
    uint32_t has_having_literal;
    uint64_t having_literal;
    uint32_t having_node_count;
    uint32_t having_root;
    prdb_sdk_sql_having_node_v1
        having_nodes[PRDB_SDK_SQL_MAX_HAVING_NODES];
    uint64_t having_count_mask;
    uint32_t order_kind;
    uint32_t order_direction;
    uint32_t null_order;
    uint32_t order_type_code;
    uint8_t order_decimal_precision;
    uint8_t order_decimal_scale;
    uint32_t order_count;
    prdb_sdk_sql_bound_order_item_v1 order_items[PRDB_SDK_SQL_MAX_ORDER_KEYS];
    uint32_t is_join;
    uint32_t left_relation_column_count;
    uint32_t right_relation_column_count;
    uint32_t output_projection_count;
    uint32_t join_projection_count;
    prdb_sdk_sql_join_projection_binding_v1
        join_projections[PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS];
    uint32_t join_key_count;
    prdb_sdk_sql_join_key_binding_v1
        join_keys[PRDB_SDK_SQL_MAX_JOIN_KEYS];
    uint32_t where_relation_side;
    uint32_t where_projection_index;
    prdb_sdk_sql_bound_column_v1 where_column;
    uint32_t order_by_projection_index;
    uint32_t order_relation_side;
    prdb_sdk_sql_bound_column_v1 join_order_by_column;
} prdb_sdk_select_binding_v1;

/* Resolves one predicate column to its already-authorized row-cell view and
 * pinned schema type metadata. Return a PRDB_SDK_SQL_EVAL_* status. */
typedef int32_t (*prdb_sdk_sql_resolve_column_v1)(
    void *context, const uint8_t *sql, uint64_t sql_len,
    prdb_sdk_sql_span_v1 column, const prdb_sdk_value_v1 **value_out,
    uint32_t *type_out, uint8_t *decimal_precision_out,
    uint8_t *decimal_scale_out);

/* Resolves a bound schema ordinal in one already-authorized sanitized row. */
typedef int32_t (*prdb_sdk_sql_resolve_bound_column_v1)(
    void *context, uint32_t ordinal, const prdb_sdk_value_v1 **value_out);

/* Parses any supported SELECT statement into the unified prdb_sdk_select_v1 AST. */
int32_t prdb_sdk_sql_parse_select_v1(const uint8_t *sql, uint64_t sql_len,
                                     prdb_sdk_select_v1 *out,
                                     uint32_t out_capacity,
                                     uint32_t *error_offset_out);

/* Binds a single-relation SELECT against one authorized relation description. */
int32_t prdb_sdk_sql_bind_select_v1(
    const prdb_sdk_select_v1 *statement, const uint8_t *relation_name,
    uint32_t relation_name_len,
    const prdb_sdk_sql_column_shape_v1 *columns, uint32_t column_count,
    prdb_sdk_select_binding_v1 *out, uint32_t out_capacity);

/* Binds a two-relation JOIN SELECT against both authorized relation descriptions. */
int32_t prdb_sdk_sql_bind_join_select_v1(
    const prdb_sdk_select_v1 *statement,
    const uint8_t *left_relation_name, uint32_t left_relation_name_len,
    const prdb_sdk_sql_column_shape_v1 *left_columns,
    uint32_t left_column_count,
    const uint8_t *right_relation_name, uint32_t right_relation_name_len,
    const prdb_sdk_sql_column_shape_v1 *right_columns,
    uint32_t right_column_count, prdb_sdk_select_binding_v1 *out,
    uint32_t out_capacity);

/* Validates one-based parameter values against a schema-declared type. */
int32_t prdb_sdk_sql_bind_parameter_v1(
    uint32_t parameter_index, const prdb_sdk_value_v1 *parameters,
    uint32_t parameter_count, uint32_t expected_type,
    uint8_t expected_decimal_precision, uint8_t expected_decimal_scale,
    prdb_sdk_value_v1 *out, uint32_t out_capacity);

int32_t prdb_sdk_sql_eval_select_predicate_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    prdb_sdk_sql_resolve_column_v1 resolve_column, void *resolver_context,
    uint32_t *truth_out);

int32_t prdb_sdk_sql_eval_bound_select_predicate_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    prdb_sdk_sql_resolve_bound_column_v1 resolve_column,
    void *resolver_context, uint32_t *truth_out);

int32_t prdb_sdk_sql_eval_bound_select_row_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const prdb_sdk_row_v1 *row, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint32_t *truth_out);

int32_t prdb_sdk_sql_compare_v1(const prdb_sdk_value_v1 *left,
                                const prdb_sdk_value_v1 *right,
                                uint32_t comparison, uint8_t left_decimal_scale,
                                uint8_t right_decimal_scale,
                                uint32_t *truth_out);
int32_t prdb_sdk_sql_value_is_null_v1(const prdb_sdk_value_v1 *value,
                                      uint32_t *truth_out);
int32_t prdb_sdk_sql_value_truth_v1(const prdb_sdk_value_v1 *value,
                                    uint32_t *truth_out);
int32_t prdb_sdk_sql_truth_not_v1(uint32_t operand, uint32_t *truth_out);
int32_t prdb_sdk_sql_truth_and_v1(uint32_t left, uint32_t right,
                                  uint32_t *truth_out);
int32_t prdb_sdk_sql_truth_or_v1(uint32_t left, uint32_t right,
                                 uint32_t *truth_out);

#endif /* PRDB_SDK_SQL_V1_H */
