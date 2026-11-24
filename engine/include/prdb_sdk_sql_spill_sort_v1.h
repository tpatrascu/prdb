#ifndef PRDB_SDK_SQL_SPILL_SORT_V1_H
#define PRDB_SDK_SQL_SPILL_SORT_V1_H

#include "prdb_sdk_spill_v1.h"

#define PRDB_SDK_SQL_SPILL_SORT_MAX_ROW_BYTES UINT32_C(1024)
#define PRDB_SDK_SQL_SPILL_SORT_MAX_RUNS UINT32_C(8)
/* Hard ceiling for simultaneous dispatcher requests made by sort/group code. */
#define PRDB_SDK_SQL_SPILL_SORT_MAX_WORKSPACE_BYTES UINT32_C(262144)

typedef int32_t (*prdb_sdk_sql_spill_sort_source_v1_fn)(
    void *context, uint8_t *row, uint32_t row_capacity, uint32_t *row_len,
    uint8_t *has_row);
typedef int32_t (*prdb_sdk_sql_spill_sort_compare_v1_fn)(
    void *context, const uint8_t *left, const uint8_t *right,
    int32_t *ordering);
typedef int32_t (*prdb_sdk_sql_spill_sort_output_v1_fn)(
    void *context, const uint8_t *row, uint32_t row_len);

/* Stable external sort over fixed-width, caller-encoded authorized rows.
 * Runs use at most 64 KiB; all live sort/group heap requests share the hard
 * workspace ceiling above; merge fan-in is eight. The comparator applies SQL
 * scalar/null/redaction rules and must return ordering in -1..1. */
int32_t prdb_sdk_sql_spill_sort_v1(
    const prdb_host_api_v6 *host, void *context, uint32_t row_bytes,
    uint32_t memory_budget_bytes,
    prdb_sdk_sql_spill_sort_source_v1_fn source,
    prdb_sdk_sql_spill_sort_compare_v1_fn compare,
    prdb_sdk_sql_spill_sort_output_v1_fn output);

#endif
