#include "prdb_sdk_sql_spill_group_v1.h"

#include <stdlib.h>
#include <string.h>

typedef struct group_state {
    void *context;
    uint32_t key_bytes;
    prdb_sdk_sql_spill_sort_source_v1_fn source;
    prdb_sdk_sql_spill_sort_compare_v1_fn compare;
    prdb_sdk_sql_spill_group_output_v1_fn output;
    uint8_t *representative;
    uint64_t count;
    uint8_t active;
} group_state;

static int32_t source_group(void *opaque, uint8_t *row, uint32_t row_capacity,
                            uint32_t *row_len, uint8_t *has_row) {
    group_state *state = (group_state *)opaque;
    return state->source(state->context, row, row_capacity, row_len, has_row);
}

static int32_t compare_group(void *opaque, const uint8_t *left,
                             const uint8_t *right, int32_t *ordering) {
    group_state *state = (group_state *)opaque;
    return state->compare(state->context, left, right, ordering);
}

static int32_t emit_group(group_state *state) {
    if (!state->active) return 0;
    int32_t status = state->output(state->context, state->representative,
                                   state->key_bytes, state->count);
    if (status != 0) return status;
    state->active = 0;
    state->count = 0;
    return 0;
}

static int32_t consume_sorted(void *opaque, const uint8_t *row,
                              uint32_t row_len) {
    group_state *state = (group_state *)opaque;
    int32_t ordering = 0;
    if (state == NULL || row == NULL || row_len != state->key_bytes) return -1;
    if (!state->active) {
        memcpy(state->representative, row, row_len);
        state->count = 1;
        state->active = 1;
        return 0;
    }
    int32_t status = compare_group(state, state->representative, row,
                                   &ordering);
    if (status != 0 || ordering < -1 || ordering > 1)
        return status != 0 ? status : -1;
    if (ordering == 0) {
        if (state->count == UINT64_MAX) return -1;
        state->count++;
        return 0;
    }
    status = emit_group(state);
    if (status != 0) return status;
    memcpy(state->representative, row, row_len);
    state->count = 1;
    state->active = 1;
    return 0;
}

int32_t prdb_sdk_sql_spill_group_count_v1(
    const prdb_host_api_v6 *host, void *context, uint32_t key_bytes,
    uint32_t memory_budget_bytes,
    prdb_sdk_sql_spill_sort_source_v1_fn source,
    prdb_sdk_sql_spill_sort_compare_v1_fn compare,
    prdb_sdk_sql_spill_group_output_v1_fn output) {
    group_state state = {0};
    int32_t status;
    if (host == NULL || source == NULL || compare == NULL || output == NULL ||
        key_bytes == 0 || key_bytes > PRDB_SDK_SQL_SPILL_SORT_MAX_ROW_BYTES)
        return -1;
    state.context = context;
    state.key_bytes = key_bytes;
    state.source = source;
    state.compare = compare;
    state.output = output;
    state.representative = (uint8_t *)malloc(key_bytes);
    if (state.representative == NULL) return -1;
    status = prdb_sdk_sql_spill_sort_v1(
        host, &state, key_bytes, memory_budget_bytes, source_group,
        compare_group, consume_sorted);
    if (status == 0) status = emit_group(&state);
    free(state.representative);
    return status;
}
