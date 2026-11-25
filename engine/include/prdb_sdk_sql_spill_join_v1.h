#ifndef PRDB_SDK_SQL_SPILL_JOIN_V1_H
#define PRDB_SDK_SQL_SPILL_JOIN_V1_H

#include "prdb_sdk_spill_v1.h"

#define PRDB_SDK_SQL_SPILL_JOIN_PARTITIONS UINT32_C(8)
#define PRDB_SDK_SQL_SPILL_JOIN_BLOCK_ROWS UINT32_C(128)
#define PRDB_SDK_SQL_SPILL_JOIN_MAX_RECORD_BYTES UINT32_C(4096)
/* record buffer plus both maximum 128-row input blocks. */
#define PRDB_SDK_SQL_SPILL_JOIN_MAX_WORKSPACE_BYTES \
    (PRDB_SDK_SQL_SPILL_JOIN_MAX_RECORD_BYTES * \
     (UINT32_C(1) + UINT32_C(2) * PRDB_SDK_SQL_SPILL_JOIN_BLOCK_ROWS))

#define PRDB_SDK_SQL_SPILL_JOIN_OK INT32_C(0)
#define PRDB_SDK_SQL_SPILL_JOIN_INVALID_ARGUMENT INT32_C(1)
#define PRDB_SDK_SQL_SPILL_JOIN_SOURCE_ERROR INT32_C(2)
#define PRDB_SDK_SQL_SPILL_JOIN_BROKER_ERROR INT32_C(3)
#define PRDB_SDK_SQL_SPILL_JOIN_CALLBACK_ERROR INT32_C(4)
#define PRDB_SDK_SQL_SPILL_JOIN_MEMORY_ERROR INT32_C(5)
#define PRDB_SDK_SQL_SPILL_JOIN_CANCELLED INT32_C(6)

typedef int32_t (*prdb_sdk_sql_spill_join_cancel_v1_fn)(void *context);

typedef int32_t (*prdb_sdk_sql_spill_join_source_v1_fn)(
    void *context, uint8_t *record, uint32_t capacity, uint8_t *has_record_out);
typedef int32_t (*prdb_sdk_sql_spill_join_hash_v1_fn)(
    void *context, const uint8_t *record, uint32_t record_bytes,
    uint64_t *hash_out);
typedef int32_t (*prdb_sdk_sql_spill_join_equal_v1_fn)(
    void *context, const uint8_t *left, const uint8_t *right,
    uint32_t record_bytes, uint8_t *equal_out);
typedef int32_t (*prdb_sdk_sql_spill_join_output_v1_fn)(
    void *context, const uint8_t *left, const uint8_t *right,
    uint32_t record_bytes);

/* Hash partitions both inputs into eight invocation-scoped opaque streams,
 * then compares bounded 128-row blocks within each partition. Input records
 * have one fixed, caller-defined size so reads use checked offsets. The live
 * record and block heap buffers share the workspace ceiling above. Rows with
 * equal hashes are still checked by the equality callback. All streams are
 * destroyed on success and failure. Output order is partition order; callers
 * requiring SQL ORDER BY must apply the external sorter afterward. The
 * cancellation callback is mandatory and polled before input and throughout
 * pair comparison. Source callbacks must poll while producing records. */
int32_t prdb_sdk_sql_spill_hash_join_v1(
    const prdb_host_api_v1 *host, uint32_t record_bytes,
    prdb_sdk_sql_spill_join_source_v1_fn left_source,
    void *left_source_context,
    prdb_sdk_sql_spill_join_source_v1_fn right_source,
    void *right_source_context,
    prdb_sdk_sql_spill_join_hash_v1_fn hash_record, void *hash_context,
    prdb_sdk_sql_spill_join_equal_v1_fn records_equal,
    void *equal_context,
    prdb_sdk_sql_spill_join_output_v1_fn output, void *output_context,
    prdb_sdk_sql_spill_join_cancel_v1_fn check_cancelled,
    void *cancel_context);

#endif /* PRDB_SDK_SQL_SPILL_JOIN_V1_H */
