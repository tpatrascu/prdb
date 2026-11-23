#include "prdb_sdk_sql_spill_sort_v1.h"

#include <stdlib.h>
#include <string.h>

#define SORT_RUN_MEMORY_BYTES UINT32_C(65536)
#define SORT_OUTPUT_BUFFER_BYTES UINT32_C(65536)

typedef struct sort_run {
    prdb_sdk_spill_stream_v1 stream;
} sort_run;

typedef struct sort_cursor {
    uint64_t offset;
    uint8_t has_record;
    uint8_t *record;
} sort_cursor;

static void encode_u64(uint8_t out[8], uint64_t value) {
    for (unsigned index = 0; index < 8; ++index)
        out[index] = (uint8_t)(value >> (56 - index * 8));
}

static uint64_t decode_u64(const uint8_t in[8]) {
    uint64_t value = 0;
    for (unsigned index = 0; index < 8; ++index)
        value = (value << 8) | in[index];
    return value;
}

static int32_t destroy_run(const prdb_host_api_v6 *host, sort_run *run) {
    if (run->stream.valid != 1) return 0;
    return prdb_sdk_spill_destroy_v1(host, &run->stream);
}

typedef struct run_cursor {
    prdb_sdk_spill_stream_v1 stream;
    sort_cursor cursor;
} run_cursor;

static int32_t load_cursor_record(const prdb_host_api_v6 *host,
                                  run_cursor *cursor, uint32_t record_bytes) {
    uint64_t read_bytes = 0;
    int32_t status = prdb_sdk_spill_read_at_v1(
        host, &cursor->stream, cursor->cursor.offset, cursor->cursor.record,
        record_bytes, &read_bytes);
    if (status != 0) return status;
    if (read_bytes == 0) {
        cursor->cursor.has_record = 0;
        return 0;
    }
    if (read_bytes != record_bytes) return -1;
    cursor->cursor.offset += record_bytes;
    cursor->cursor.has_record = 1;
    return 0;
}

static int32_t merge_runs(
    const prdb_host_api_v6 *host, sort_run *runs, uint32_t run_count,
    uint32_t row_bytes, prdb_sdk_sql_spill_sort_compare_v1_fn compare,
    void *context, sort_run *merged, prdb_sdk_sql_spill_sort_output_v1_fn output) {
    const uint32_t record_bytes = row_bytes + 8;
    run_cursor cursors[PRDB_SDK_SQL_SPILL_SORT_MAX_RUNS];
    uint8_t *cursor_storage = NULL;
    uint8_t *output_buffer = NULL;
    uint32_t buffered_rows = 0;
    const uint32_t output_capacity = SORT_OUTPUT_BUFFER_BYTES / record_bytes;
    int32_t status = 0;
    memset(cursors, 0, sizeof(cursors));
    cursor_storage = (uint8_t *)malloc((size_t)run_count * record_bytes);
    if (cursor_storage == NULL) return -1;
    output_buffer = output == NULL
                        ? (uint8_t *)malloc((size_t)output_capacity * record_bytes)
                        : NULL;
    if (output == NULL && output_buffer == NULL) {
        free(cursor_storage);
        return -1;
    }
    if (output == NULL) {
        status = prdb_sdk_spill_create_v1(host, &merged->stream);
        if (status != 0) goto done;
    }
    for (uint32_t index = 0; index < run_count; ++index) {
        cursors[index].stream = runs[index].stream;
        cursors[index].cursor.record = cursor_storage + (size_t)index * record_bytes;
        status = load_cursor_record(host, &cursors[index], record_bytes);
        if (status != 0) goto done;
    }
    for (;;) {
        uint32_t selected = run_count;
        for (uint32_t index = 0; index < run_count; ++index) {
            if (!cursors[index].cursor.has_record) continue;
            if (selected == run_count) {
                selected = index;
                continue;
            }
            int32_t ordering = 0;
            status = compare(context, cursors[index].cursor.record + 8,
                             cursors[selected].cursor.record + 8, &ordering);
            if (status != 0 || ordering < -1 || ordering > 1) {
                if (status == 0) status = -1;
                goto done;
            }
            uint64_t left_ordinal = decode_u64(cursors[index].cursor.record);
            uint64_t right_ordinal = decode_u64(cursors[selected].cursor.record);
            if (ordering < 0 || (ordering == 0 && left_ordinal < right_ordinal))
                selected = index;
        }
        if (selected == run_count) break;
        uint8_t *record = cursors[selected].cursor.record;
        if (output != NULL) {
            status = output(context, record + 8, row_bytes);
            if (status != 0) goto done;
        } else {
            memcpy(output_buffer + (size_t)buffered_rows * record_bytes,
                   record, record_bytes);
            buffered_rows++;
            if (buffered_rows == output_capacity) {
                status = prdb_sdk_spill_append_v1(
                    host, &merged->stream, output_buffer,
                    (uint64_t)buffered_rows * record_bytes);
                if (status != 0) goto done;
                buffered_rows = 0;
            }
        }
        status = load_cursor_record(host, &cursors[selected], record_bytes);
        if (status != 0) goto done;
    }
    if (output == NULL) {
        if (buffered_rows != 0) {
            status = prdb_sdk_spill_append_v1(
                host, &merged->stream, output_buffer,
                (uint64_t)buffered_rows * record_bytes);
            if (status != 0) goto done;
        }
        status = prdb_sdk_spill_seal_v1(host, &merged->stream);
    }
done:
    if (status != 0 && output == NULL) (void)destroy_run(host, merged);
    free(output_buffer);
    free(cursor_storage);
    return status;
}

static int32_t sort_run_records(
    uint8_t *records, uint64_t *ordinals, uint32_t count, uint32_t row_bytes,
    prdb_sdk_sql_spill_sort_compare_v1_fn compare, void *context) {
    uint8_t *scratch = (uint8_t *)malloc((size_t)count * row_bytes);
    uint64_t *scratch_ordinals =
        (uint64_t *)malloc((size_t)count * sizeof(*scratch_ordinals));
    uint8_t *source_rows = records, *destination_rows = scratch;
    uint64_t *source_ordinals = ordinals, *destination_ordinals = scratch_ordinals;
    if (scratch == NULL || scratch_ordinals == NULL) {
        free(scratch);
        free(scratch_ordinals);
        return -1;
    }
    for (uint32_t width = 1; width < count;) {
        for (uint32_t start = 0; start < count; start += width * 2) {
            uint32_t middle = start + width < count ? start + width : count;
            uint32_t end = middle + width < count ? middle + width : count;
            uint32_t left = start, right = middle, out = start;
            while (left < middle && right < end) {
                int32_t ordering = 0;
                int32_t status = compare(
                    context, source_rows + (size_t)left * row_bytes,
                    source_rows + (size_t)right * row_bytes, &ordering);
                if (status != 0 || ordering < -1 || ordering > 1) {
                    free(scratch);
                    free(scratch_ordinals);
                    return status != 0 ? status : -1;
                }
                if (ordering < 0 ||
                    (ordering == 0 &&
                     source_ordinals[left] <= source_ordinals[right])) {
                    memcpy(destination_rows + (size_t)out * row_bytes,
                           source_rows + (size_t)left * row_bytes, row_bytes);
                    destination_ordinals[out++] = source_ordinals[left++];
                } else {
                    memcpy(destination_rows + (size_t)out * row_bytes,
                           source_rows + (size_t)right * row_bytes, row_bytes);
                    destination_ordinals[out++] = source_ordinals[right++];
                }
            }
            while (left < middle) {
                memcpy(destination_rows + (size_t)out * row_bytes,
                       source_rows + (size_t)left * row_bytes, row_bytes);
                destination_ordinals[out++] = source_ordinals[left++];
            }
            while (right < end) {
                memcpy(destination_rows + (size_t)out * row_bytes,
                       source_rows + (size_t)right * row_bytes, row_bytes);
                destination_ordinals[out++] = source_ordinals[right++];
            }
        }
        uint8_t *row_swap = source_rows;
        source_rows = destination_rows;
        destination_rows = row_swap;
        uint64_t *ordinal_swap = source_ordinals;
        source_ordinals = destination_ordinals;
        destination_ordinals = ordinal_swap;
        if (width > count / 2) width = count;
        else width *= 2;
    }
    if (source_rows != records) {
        memcpy(records, source_rows, (size_t)count * row_bytes);
        memcpy(ordinals, source_ordinals, (size_t)count * sizeof(*ordinals));
    }
    free(scratch);
    free(scratch_ordinals);
    return 0;
}

static int32_t flush_run(const prdb_host_api_v6 *host, sort_run *run,
                         const uint8_t *rows, const uint64_t *ordinals,
                         uint32_t count, uint32_t row_bytes) {
    uint32_t record_bytes = row_bytes + 8;
    uint8_t *encoded = (uint8_t *)malloc((size_t)count * record_bytes);
    if (encoded == NULL) return -1;
    int32_t status = prdb_sdk_spill_create_v1(host, &run->stream);
    if (status == 0) {
        for (uint32_t index = 0; index < count; ++index) {
            uint8_t *record = encoded + (size_t)index * record_bytes;
            encode_u64(record, ordinals[index]);
            memcpy(record + 8, rows + (size_t)index * row_bytes, row_bytes);
        }
        status = prdb_sdk_spill_append_v1(
            host, &run->stream, encoded, (uint64_t)count * record_bytes);
        if (status == 0) status = prdb_sdk_spill_seal_v1(host, &run->stream);
    }
    if (status != 0) (void)destroy_run(host, run);
    free(encoded);
    return status;
}

int32_t prdb_sdk_sql_spill_sort_v1(
    const prdb_host_api_v6 *host, void *context, uint32_t row_bytes,
    uint32_t memory_budget_bytes,
    prdb_sdk_sql_spill_sort_source_v1_fn source,
    prdb_sdk_sql_spill_sort_compare_v1_fn compare,
    prdb_sdk_sql_spill_sort_output_v1_fn output) {
    sort_run runs[PRDB_SDK_SQL_SPILL_SORT_MAX_RUNS];
    uint8_t *rows = NULL;
    uint64_t *ordinals = NULL;
    uint32_t run_count = 0, row_count = 0, row_capacity = 0;
    uint64_t next_ordinal = 0;
    int32_t status = 0;
    memset(runs, 0, sizeof(runs));
    if (host == NULL || source == NULL || compare == NULL || output == NULL ||
        row_bytes == 0 || row_bytes > PRDB_SDK_SQL_SPILL_SORT_MAX_ROW_BYTES ||
        memory_budget_bytes < row_bytes + 8 ||
        memory_budget_bytes > SORT_RUN_MEMORY_BYTES ||
        memory_budget_bytes > PRDB_SDK_SQL_SPILL_SORT_MAX_WORKSPACE_BYTES)
        return -1;
    row_capacity = memory_budget_bytes / (row_bytes + 8);
    if (row_capacity == 0) return -1;
    rows = (uint8_t *)malloc((size_t)row_capacity * row_bytes);
    ordinals = (uint64_t *)malloc((size_t)row_capacity * sizeof(*ordinals));
    if (rows == NULL || ordinals == NULL) { status = -1; goto cleanup; }
    for (;;) {
        uint32_t row_len = 0;
        uint8_t has_row = 0;
        status = source(context, rows + (size_t)row_count * row_bytes,
                        row_bytes, &row_len, &has_row);
        if (status != 0) goto cleanup;
        if (has_row > 1 || (has_row != 0 && row_len != row_bytes) ||
            (has_row == 0 && row_len != 0)) { status = -1; goto cleanup; }
        if (has_row == 0) break;
        if (next_ordinal == UINT64_MAX) { status = -1; goto cleanup; }
        ordinals[row_count++] = next_ordinal++;
        if (row_count == row_capacity) {
            status = sort_run_records(rows, ordinals, row_count, row_bytes,
                                      compare, context);
            if (status != 0) goto cleanup;
            status = flush_run(host, &runs[run_count], rows, ordinals,
                               row_count, row_bytes);
            if (status != 0) goto cleanup;
            run_count++;
            row_count = 0;
            if (run_count == PRDB_SDK_SQL_SPILL_SORT_MAX_RUNS) {
                sort_run merged = {0};
                status = merge_runs(host, runs, run_count, row_bytes, compare,
                                    context, &merged, NULL);
                if (status != 0) { (void)destroy_run(host, &merged); goto cleanup; }
                for (uint32_t index = 0; index < run_count; ++index) {
                    status = destroy_run(host, &runs[index]);
                    if (status != 0) { (void)destroy_run(host, &merged); goto cleanup; }
                }
                memset(runs, 0, sizeof(runs));
                runs[0] = merged;
                run_count = 1;
            }
        }
    }
    if (row_count != 0) {
        status = sort_run_records(rows, ordinals, row_count, row_bytes,
                                  compare, context);
        if (status != 0) goto cleanup;
        status = flush_run(host, &runs[run_count], rows, ordinals,
                           row_count, row_bytes);
        if (status != 0) goto cleanup;
        run_count++;
    }
    if (run_count == 1) {
        uint32_t record_bytes = row_bytes + 8;
        uint64_t offset = 0;
        uint8_t *record = (uint8_t *)malloc(record_bytes);
        if (record == NULL) { status = -1; goto cleanup; }
        for (;;) {
            uint64_t read_bytes = 0;
            status = prdb_sdk_spill_read_at_v1(
                host, &runs[0].stream, offset, record, record_bytes, &read_bytes);
            if (status != 0 || read_bytes == 0) break;
            if (read_bytes != record_bytes) { status = -1; break; }
            status = output(context, record + 8, row_bytes);
            if (status != 0) break;
            offset += record_bytes;
        }
        free(record);
    } else if (run_count > 1) {
        status = merge_runs(host, runs, run_count, row_bytes, compare,
                            context, NULL, output);
    }
cleanup:
    for (uint32_t index = 0; index < PRDB_SDK_SQL_SPILL_SORT_MAX_RUNS; ++index) {
        int32_t cleanup_status = destroy_run(host, &runs[index]);
        if (status == 0 && cleanup_status != 0) status = cleanup_status;
    }
    free(ordinals);
    free(rows);
    return status;
}
