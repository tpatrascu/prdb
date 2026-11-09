#ifndef PRDB_SDK_SQL_RESULT_V1_H
#define PRDB_SDK_SQL_RESULT_V1_H

#include "prdb_sdk_sql_v2.h"
#include "prdb_sdk_sql_v5.h"
#include "prdb_sdk_sql_v6.h"
#include "prdb_sdk_sql_v8.h"
#include "prdb_query_abi_v3.h"

#define PRDB_SDK_SQL_RESULT_V1_ABI_VERSION UINT32_C(1)
#define PRDB_SDK_SQL_RESULT_WIRE_V1_VERSION UINT16_C(1)
#define PRDB_SDK_SQL_RESULT_MAX_BYTES UINT64_C(1048576)

#define PRDB_SDK_SQL_RESULT_OK INT32_C(0)
#define PRDB_SDK_SQL_RESULT_INVALID_ARGUMENT INT32_C(1)
#define PRDB_SDK_SQL_RESULT_INVALID_BINDING INT32_C(2)
#define PRDB_SDK_SQL_RESULT_INVALID_SHAPE INT32_C(3)
#define PRDB_SDK_SQL_RESULT_INVALID_ROW INT32_C(4)
#define PRDB_SDK_SQL_RESULT_LIMIT_EXCEEDED INT32_C(5)
#define PRDB_SDK_SQL_RESULT_WRITE_FAILED INT32_C(6)

/* Encodes one projected authorized row as an SQLR v1 record. Output contains
 * declared column names/types and preserves VALUE, NULL, and REDACTED states.
 * The caller supplies scratch for every source cell and a bounded output
 * buffer. Inputs, scratch, and output must not overlap. output_len is zeroed
 * on failure; the function never emits data or takes ownership of buffers. */
int32_t prdb_sdk_sql_encode_result_row_v1(
    const prdb_sdk_select_binding_v2 *binding,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    const prdb_sdk_row_v1 *row, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *output,
    uint64_t output_capacity, uint64_t *output_len);

/* Encodes and emits one SQLR row through the v3 result callback in chunks no
 * larger than the host ABI limit. The result remains provisional. A caller
 * inside the SELECT row callback must propagate errors so the SQL runner can
 * latch a statement failure; an error may follow already-emitted chunks and
 * clears output_len. */
int32_t prdb_sdk_sql_write_result_row_v1(
    const prdb_host_api_v3 *host,
    const prdb_sdk_select_binding_v2 *binding,
    const prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_count,
    const prdb_sdk_row_v1 *row, prdb_sdk_value_v1 *cell_scratch,
    uint32_t cell_scratch_capacity, uint8_t *output,
    uint64_t output_capacity, uint64_t *output_len);

/* Encodes one joined projection row as SQLR v1 using each binding's source
 * side, ordinal, and declared column metadata. VALUE, NULL, and REDACTED
 * projection states are preserved. All inputs and output must be disjoint. */
int32_t prdb_sdk_sql_encode_join_result_v1(
    const prdb_sdk_select_binding_v8 *binding,
    const prdb_sdk_sql_column_shape_v2 *left_columns,
    uint32_t left_column_count,
    const prdb_sdk_sql_column_shape_v2 *right_columns,
    uint32_t right_column_count,
    const prdb_sdk_value_v1 *projected_values, uint32_t projected_count,
    uint8_t *output, uint64_t output_capacity, uint64_t *output_len);

/* Encodes and emits one joined SQLR row through the v3 result callback in
 * host-sized provisional chunks. */
int32_t prdb_sdk_sql_write_join_result_v1(
    const prdb_host_api_v3 *host,
    const prdb_sdk_select_binding_v8 *binding,
    const prdb_sdk_sql_column_shape_v2 *left_columns,
    uint32_t left_column_count,
    const prdb_sdk_sql_column_shape_v2 *right_columns,
    uint32_t right_column_count,
    const prdb_sdk_value_v1 *projected_values, uint32_t projected_count,
    uint8_t *output, uint64_t output_capacity, uint64_t *output_len);

/* Encodes one GROUP BY COUNT(*) row as SQLR v1. The first field uses the
 * declared group-column metadata and the second field is named `count` with
 * BIGINT type. VALUE/NULL/REDACTED group-key states are preserved. Input and
 * output buffers must not overlap; output_len is zero on failure. */
int32_t prdb_sdk_sql_encode_group_count_result_v1(
    const prdb_sdk_sql_column_shape_v2 *group_column,
    const prdb_sdk_value_v1 *group_key, uint64_t count,
    uint8_t *output, uint64_t output_capacity, uint64_t *output_len);

/* Encodes and emits one grouped SQLR record through the v3 result callback,
 * split into host-sized chunks. Output remains provisional. */
int32_t prdb_sdk_sql_write_group_count_result_v1(
    const prdb_host_api_v3 *host,
    const prdb_sdk_sql_column_shape_v2 *group_column,
    const prdb_sdk_value_v1 *group_key, uint64_t count,
    uint8_t *output, uint64_t output_capacity, uint64_t *output_len);

/* Encodes one grouped MIN/MAX SQLR v1 row. The first field is the declared
 * group key; the second is named `min` or `max` and uses the aggregate
 * argument's declared type metadata. */
int32_t prdb_sdk_sql_encode_group_minmax_result_v1(
    const prdb_sdk_sql_column_shape_v2 *group_column,
    const prdb_sdk_value_v1 *group_key,
    const prdb_sdk_sql_column_shape_v2 *aggregate_column,
    const prdb_sdk_value_v1 *aggregate_value, uint32_t aggregate_kind,
    uint8_t *output, uint64_t output_capacity, uint64_t *output_len);

/* Encodes and emits one grouped MIN/MAX SQLR record through the v3 result
 * callback, split into host-sized provisional chunks. */
int32_t prdb_sdk_sql_write_group_minmax_result_v1(
    const prdb_host_api_v3 *host,
    const prdb_sdk_sql_column_shape_v2 *group_column,
    const prdb_sdk_value_v1 *group_key,
    const prdb_sdk_sql_column_shape_v2 *aggregate_column,
    const prdb_sdk_value_v1 *aggregate_value, uint32_t aggregate_kind,
    uint8_t *output, uint64_t output_capacity, uint64_t *output_len);

/* Encodes one grouped SUM SQLR row. The `sum` field is BIGINT for integer
 * arguments and DECIMAL(38,s) for DECIMAL(p,s) arguments. */
int32_t prdb_sdk_sql_encode_group_sum_result_v1(
    const prdb_sdk_sql_column_shape_v2 *group_column,
    const prdb_sdk_value_v1 *group_key,
    const prdb_sdk_sql_column_shape_v2 *aggregate_input_column,
    const prdb_sdk_value_v1 *aggregate_value,
    uint8_t *output, uint64_t output_capacity, uint64_t *output_len);

/* Encodes and emits one grouped SUM SQLR row in host-sized provisional
 * chunks. */
int32_t prdb_sdk_sql_write_group_sum_result_v1(
    const prdb_host_api_v3 *host,
    const prdb_sdk_sql_column_shape_v2 *group_column,
    const prdb_sdk_value_v1 *group_key,
    const prdb_sdk_sql_column_shape_v2 *aggregate_input_column,
    const prdb_sdk_value_v1 *aggregate_value,
    uint8_t *output, uint64_t output_capacity, uint64_t *output_len);

/* Encodes one grouped AVG SQLR row. The `avg` field is DECIMAL(38,6) for
 * integer arguments, or DECIMAL(38,r) using the declared Decimal metadata. */
int32_t prdb_sdk_sql_encode_group_avg_result_v1(
    const prdb_sdk_sql_column_shape_v2 *group_column,
    const prdb_sdk_value_v1 *group_key,
    const prdb_sdk_sql_column_shape_v2 *aggregate_input_column,
    const prdb_sdk_value_v1 *aggregate_value,
    uint8_t *output, uint64_t output_capacity, uint64_t *output_len);

/* Encodes and emits one grouped AVG SQLR row in host-sized provisional
 * chunks. */
int32_t prdb_sdk_sql_write_group_avg_result_v1(
    const prdb_host_api_v3 *host,
    const prdb_sdk_sql_column_shape_v2 *group_column,
    const prdb_sdk_value_v1 *group_key,
    const prdb_sdk_sql_column_shape_v2 *aggregate_input_column,
    const prdb_sdk_value_v1 *aggregate_value,
    uint8_t *output, uint64_t output_capacity, uint64_t *output_len);

#endif /* PRDB_SDK_SQL_RESULT_V1_H */
