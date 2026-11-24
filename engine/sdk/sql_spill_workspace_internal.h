#ifndef PRDB_SQL_SPILL_WORKSPACE_INTERNAL_H
#define PRDB_SQL_SPILL_WORKSPACE_INTERNAL_H

#include "prdb_sdk_sql_spill_sort_v1.h"

#include <stddef.h>
#include <stdlib.h>

typedef struct prdb_sql_spill_heap_budget {
    size_t requested_bytes;
    size_t maximum_bytes;
} prdb_sql_spill_heap_budget;

static inline void *prdb_sql_spill_heap_alloc(
    prdb_sql_spill_heap_budget *budget, size_t bytes) {
    void *allocation;

    if (budget == NULL || budget->requested_bytes > budget->maximum_bytes ||
        bytes > budget->maximum_bytes - budget->requested_bytes) {
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

#endif
