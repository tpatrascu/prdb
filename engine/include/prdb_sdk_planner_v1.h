#ifndef PRDB_SDK_PLANNER_V1_H
#define PRDB_SDK_PLANNER_V1_H

#include "prdb_sdk_sql_v2.h"

#define PRDB_SDK_PLANNER_ABI_V1_VERSION UINT32_C(1)
#define PRDB_SDK_PLANNER_WIRE_V1_VERSION UINT16_C(1)
#define PRDB_SDK_PLANNER_MAX_RESPONSE_BYTES UINT64_C(1048576)
#define PRDB_SDK_PLANNER_MAX_RELATION_COLUMNS UINT32_C(4096)
#define PRDB_SDK_PLANNER_TABLE_HANDLE_KIND UINT8_C(9)

#define PRDB_SDK_PLANNER_DECODE_OK INT32_C(0)
#define PRDB_SDK_PLANNER_DECODE_UNAVAILABLE INT32_C(1)
#define PRDB_SDK_PLANNER_DECODE_INVALID_ARGUMENT INT32_C(2)
#define PRDB_SDK_PLANNER_DECODE_INVALID_PAYLOAD INT32_C(3)
#define PRDB_SDK_PLANNER_DECODE_UNSUPPORTED_VERSION INT32_C(4)
#define PRDB_SDK_PLANNER_DECODE_RESPONSE_TOO_SMALL INT32_C(5)

/* Names and handles borrow the complete immutable planner response. The
 * table handle is planner-only and must never be passed to executor calls. */
typedef struct prdb_sdk_planner_relation_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    uint32_t available;
    uint8_t relation_id[16];
    uint8_t planning_handle[16];
    const uint8_t *name;
    uint32_t name_len;
    uint32_t column_count;
} prdb_sdk_planner_relation_v1;

/* Decode planner IPC describe_relation response v1 into the column shape
 * consumed by prdb_sdk_sql_bind_select_v2. Columns are written only after
 * complete validation. Output records/columns must not overlap each other or
 * the borrowed payload. The included planning handle is never an executor
 * handle. */
int32_t prdb_sdk_planner_decode_relation_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_planner_relation_v1 *out, uint32_t out_capacity,
    prdb_sdk_sql_column_shape_v2 *columns, uint32_t column_capacity);

#endif /* PRDB_SDK_PLANNER_V1_H */
