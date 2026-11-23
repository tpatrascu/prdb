#include "prdb_sdk_sql_spill_join_v1.h"

#include <stdlib.h>
#include <string.h>

#define SQL_SPILL_JOIN_CANCEL_POLL_BUDGET UINT64_C(16)

static int32_t check_cancel(
    prdb_sdk_sql_spill_join_cancel_v1_fn check_cancelled,
    void *cancel_context) {
    if (check_cancelled == NULL) return PRDB_SDK_SQL_SPILL_JOIN_OK;
    return check_cancelled(cancel_context) == 0
               ? PRDB_SDK_SQL_SPILL_JOIN_OK
               : PRDB_SDK_SQL_SPILL_JOIN_CANCELLED;
}

static int32_t read_records(const prdb_host_api_v6 *host,
                            const prdb_sdk_spill_stream_v1 *stream,
                            uint64_t first, uint32_t count,
                            uint32_t record_bytes, uint8_t *records) {
    uint64_t offset = first * (uint64_t)record_bytes;
    uint64_t length = (uint64_t)count * record_bytes;
    uint64_t actual = 0;
    if (length > PRDB_SDK_SPILL_MAX_IO_BYTES || length > UINT32_MAX) {
        return PRDB_SDK_SQL_SPILL_JOIN_BROKER_ERROR;
    }
    if (prdb_sdk_spill_read_at_v1(host, stream, offset, records,
                                  (uint32_t)length, &actual) != 0 ||
        actual != length) {
        return PRDB_SDK_SQL_SPILL_JOIN_BROKER_ERROR;
    }
    return PRDB_SDK_SQL_SPILL_JOIN_OK;
}

int32_t prdb_sdk_sql_spill_hash_join_v1(
    const prdb_host_api_v6 *host, uint32_t record_bytes,
    prdb_sdk_sql_spill_join_source_v1_fn left_source,
    void *left_source_context,
    prdb_sdk_sql_spill_join_source_v1_fn right_source,
    void *right_source_context,
    prdb_sdk_sql_spill_join_hash_v1_fn hash_record, void *hash_context,
    prdb_sdk_sql_spill_join_equal_v1_fn records_equal,
    void *equal_context,
    prdb_sdk_sql_spill_join_output_v1_fn output, void *output_context,
    prdb_sdk_sql_spill_join_cancel_v1_fn check_cancelled,
    void *cancel_context) {
    prdb_sdk_spill_stream_v1 streams[PRDB_SDK_SQL_SPILL_JOIN_PARTITIONS * 2] =
        {{{0}}};
    uint64_t counts[2][PRDB_SDK_SQL_SPILL_JOIN_PARTITIONS] = {{0}};
    uint8_t *record = NULL, *left_block = NULL, *right_block = NULL;
    int32_t result = PRDB_SDK_SQL_SPILL_JOIN_OK;
    uint32_t created = 0;
    uint64_t comparisons_since_cancel = 0;
    uint64_t total_comparisons = 0, cancel_interval = 0;

    if (host == NULL || host->spill_request == NULL || record_bytes == 0 ||
        record_bytes > PRDB_SDK_SQL_SPILL_JOIN_MAX_RECORD_BYTES ||
        left_source == NULL || right_source == NULL || hash_record == NULL ||
        records_equal == NULL || output == NULL || check_cancelled == NULL) {
        return PRDB_SDK_SQL_SPILL_JOIN_INVALID_ARGUMENT;
    }
    record = (uint8_t *)malloc(record_bytes);
    left_block = (uint8_t *)malloc((size_t)record_bytes *
                                   PRDB_SDK_SQL_SPILL_JOIN_BLOCK_ROWS);
    right_block = (uint8_t *)malloc((size_t)record_bytes *
                                    PRDB_SDK_SQL_SPILL_JOIN_BLOCK_ROWS);
    if (record == NULL || left_block == NULL || right_block == NULL) {
        result = PRDB_SDK_SQL_SPILL_JOIN_MEMORY_ERROR;
        goto cleanup;
    }
    for (uint32_t index = 0; index < PRDB_SDK_SQL_SPILL_JOIN_PARTITIONS * 2;
         ++index) {
        if (prdb_sdk_spill_create_v1(host, &streams[index]) != 0) {
            result = PRDB_SDK_SQL_SPILL_JOIN_BROKER_ERROR;
            goto cleanup;
        }
        ++created;
    }
    result = check_cancel(check_cancelled, cancel_context);
    if (result != PRDB_SDK_SQL_SPILL_JOIN_OK) goto cleanup;

    for (uint32_t side = 0; side < 2; ++side) {
        prdb_sdk_sql_spill_join_source_v1_fn source =
            side == 0 ? left_source : right_source;
        void *source_context = side == 0 ? left_source_context
                                         : right_source_context;
        for (uint64_t ordinal = 0;; ++ordinal) {
            uint64_t hash = 0;
            uint32_t partition;
            uint32_t stream_index;
            uint8_t has_record = 0;
            if (source(source_context, record, record_bytes,
                       &has_record) != 0 || has_record > 1) {
                result = PRDB_SDK_SQL_SPILL_JOIN_SOURCE_ERROR;
                goto cleanup;
            }
            if (!has_record) break;
            if (hash_record(hash_context, record, record_bytes, &hash) != 0) {
                result = PRDB_SDK_SQL_SPILL_JOIN_CALLBACK_ERROR;
                goto cleanup;
            }
            partition = (uint32_t)(hash %
                                   PRDB_SDK_SQL_SPILL_JOIN_PARTITIONS);
            stream_index = side * PRDB_SDK_SQL_SPILL_JOIN_PARTITIONS +
                           partition;
            if (counts[side][partition] == UINT64_MAX ||
                counts[side][partition] >= UINT64_MAX / record_bytes ||
                prdb_sdk_spill_append_v1(host, &streams[stream_index], record,
                                         record_bytes) != 0) {
                result = PRDB_SDK_SQL_SPILL_JOIN_BROKER_ERROR;
                goto cleanup;
            }
            ++counts[side][partition];
        }
    }
    for (uint32_t index = 0; index < created; ++index) {
        if (prdb_sdk_spill_seal_v1(host, &streams[index]) != 0) {
            result = PRDB_SDK_SQL_SPILL_JOIN_BROKER_ERROR;
            goto cleanup;
        }
    }
    for (uint32_t partition = 0;
         partition < PRDB_SDK_SQL_SPILL_JOIN_PARTITIONS; ++partition) {
        uint64_t partition_comparisons;
        if (counts[0][partition] != 0 &&
            counts[1][partition] > UINT64_MAX / counts[0][partition]) {
            result = PRDB_SDK_SQL_SPILL_JOIN_BROKER_ERROR;
            goto cleanup;
        }
        partition_comparisons =
            counts[0][partition] * counts[1][partition];
        if (partition_comparisons > UINT64_MAX - total_comparisons) {
            result = PRDB_SDK_SQL_SPILL_JOIN_BROKER_ERROR;
            goto cleanup;
        }
        total_comparisons += partition_comparisons;
    }
    if (total_comparisons != 0) {
        cancel_interval = total_comparisons / SQL_SPILL_JOIN_CANCEL_POLL_BUDGET;
        if (total_comparisons % SQL_SPILL_JOIN_CANCEL_POLL_BUDGET != 0)
            ++cancel_interval;
        if (cancel_interval == 0) cancel_interval = 1;
    }

    for (uint32_t partition = 0;
         partition < PRDB_SDK_SQL_SPILL_JOIN_PARTITIONS; ++partition) {
        uint32_t left_stream = partition;
        uint32_t right_stream = PRDB_SDK_SQL_SPILL_JOIN_PARTITIONS + partition;
        for (uint64_t left_first = 0; left_first < counts[0][partition];
             left_first += PRDB_SDK_SQL_SPILL_JOIN_BLOCK_ROWS) {
            uint32_t left_rows = (uint32_t)(counts[0][partition] - left_first);
            if (left_rows > PRDB_SDK_SQL_SPILL_JOIN_BLOCK_ROWS) {
                left_rows = PRDB_SDK_SQL_SPILL_JOIN_BLOCK_ROWS;
            }
            result = read_records(host, &streams[left_stream], left_first,
                                  left_rows, record_bytes, left_block);
            if (result != PRDB_SDK_SQL_SPILL_JOIN_OK) goto cleanup;
            for (uint64_t right_first = 0;
                 right_first < counts[1][partition];
                 right_first += PRDB_SDK_SQL_SPILL_JOIN_BLOCK_ROWS) {
                uint32_t right_rows =
                    (uint32_t)(counts[1][partition] - right_first);
                if (right_rows > PRDB_SDK_SQL_SPILL_JOIN_BLOCK_ROWS) {
                    right_rows = PRDB_SDK_SQL_SPILL_JOIN_BLOCK_ROWS;
                }
                result = read_records(host, &streams[right_stream], right_first,
                                      right_rows, record_bytes, right_block);
                if (result != PRDB_SDK_SQL_SPILL_JOIN_OK) goto cleanup;
                for (uint32_t left_index = 0; left_index < left_rows;
                     ++left_index) {
                    const uint8_t *left =
                        left_block + (size_t)left_index * record_bytes;
                    for (uint32_t right_index = 0; right_index < right_rows;
                         ++right_index) {
                        const uint8_t *right =
                            right_block + (size_t)right_index * record_bytes;
                        uint8_t equal = 0;
                        if (cancel_interval != 0 &&
                            ++comparisons_since_cancel >= cancel_interval) {
                            comparisons_since_cancel = 0;
                            result = check_cancel(check_cancelled,
                                                  cancel_context);
                            if (result != PRDB_SDK_SQL_SPILL_JOIN_OK)
                                goto cleanup;
                        }
                        if (records_equal(equal_context, left, right,
                                          record_bytes, &equal) != 0) {
                            result = PRDB_SDK_SQL_SPILL_JOIN_CALLBACK_ERROR;
                            goto cleanup;
                        }
                        if (equal > 1) {
                            result = PRDB_SDK_SQL_SPILL_JOIN_CALLBACK_ERROR;
                            goto cleanup;
                        }
                        if (equal && output(output_context, left, right,
                                            record_bytes) != 0) {
                            result = PRDB_SDK_SQL_SPILL_JOIN_CALLBACK_ERROR;
                            goto cleanup;
                        }
                    }
                }
            }
        }
    }

cleanup:
    for (uint32_t index = 0; index < created; ++index) {
        if (prdb_sdk_spill_destroy_v1(host, &streams[index]) != 0 &&
            result == PRDB_SDK_SQL_SPILL_JOIN_OK) {
            result = PRDB_SDK_SQL_SPILL_JOIN_BROKER_ERROR;
        }
    }
    free(record);
    free(left_block);
    free(right_block);
    return result;
}
