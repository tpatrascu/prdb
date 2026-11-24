#ifndef PRDB_SQL_SPILL_WORKSPACE_INTERNAL_H
#define PRDB_SQL_SPILL_WORKSPACE_INTERNAL_H

#include "prdb_sdk_sql_spill_sort_v1.h"

#include <stddef.h>
#include <stdlib.h>

typedef struct prdb_sql_spill_heap_budget {
    size_t requested_bytes;
} prdb_sql_spill_heap_budget;

static inline void *prdb_sql_spill_heap_alloc(
    prdb_sql_spill_heap_budget *budget, size_t bytes) {
    void *allocation;

    if (budget == NULL ||
        budget->requested_bytes > PRDB_SDK_SQL_SPILL_SORT_MAX_WORKSPACE_BYTES ||
        bytes > PRDB_SDK_SQL_SPILL_SORT_MAX_WORKSPACE_BYTES -
                    budget->requested_bytes) {
        return NULL;
    }
    allocation = malloc(bytes);
    if (allocation != NULL) budget->requested_bytes += bytes;
    return allocation;
}

static inline void prdb_sql_spill_heap_free(
    prdb_sql_spill_heap_budget *budget, void *allocation, size_t bytes) {
    if (allocation == NULL) return;
    free(allocation);
    if (budget != NULL && bytes <= budget->requested_bytes)
        budget->requested_bytes -= bytes;
}

int32_t prdb_sdk_sql_spill_sort_with_budget_internal_v1(
    const prdb_host_api_v6 *host, void *context, uint32_t row_bytes,
    uint32_t memory_budget_bytes,
    prdb_sdk_sql_spill_sort_source_v1_fn source,
    prdb_sdk_sql_spill_sort_compare_v1_fn compare,
    prdb_sdk_sql_spill_sort_output_v1_fn output,
    prdb_sql_spill_heap_budget *heap_budget);

#endif
