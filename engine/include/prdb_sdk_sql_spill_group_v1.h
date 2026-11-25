#ifndef PRDB_SDK_SQL_SPILL_GROUP_V1_H
#define PRDB_SDK_SQL_SPILL_GROUP_V1_H

#include "prdb_sdk_sql_spill_sort_v1.h"

#define PRDB_SDK_SQL_SPILL_GROUP_MAX_WORKSPACE_BYTES \
    (PRDB_SDK_SQL_SPILL_SORT_MAX_WORKSPACE_BYTES + \
     PRDB_SDK_SQL_SPILL_SORT_MAX_ROW_BYTES)

typedef int32_t (*prdb_sdk_sql_spill_group_output_v1_fn)(
    void *context, const uint8_t *representative, uint32_t row_bytes,
    uint64_t group_row_count);

/* External GROUP BY COUNT(*) over caller-encoded, fixed-width key records.
 * The comparator defines SQL key equality and ordering. Memory use is bounded
 * by the sorter's configured run budget; rows are read through the opaque
 * broker and emitted one group at a time. */
int32_t prdb_sdk_sql_spill_group_count_v1(
    const prdb_host_api_v1 *host, void *context, uint32_t key_bytes,
    uint32_t memory_budget_bytes,
    prdb_sdk_sql_spill_sort_source_v1_fn source,
    prdb_sdk_sql_spill_sort_compare_v1_fn compare,
    prdb_sdk_sql_spill_group_output_v1_fn output);

#endif /* PRDB_SDK_SQL_SPILL_GROUP_V1_H */
