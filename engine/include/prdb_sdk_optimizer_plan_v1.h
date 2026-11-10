#ifndef PRDB_SDK_OPTIMIZER_PLAN_V1_H
#define PRDB_SDK_OPTIMIZER_PLAN_V1_H

#include "prdb_sdk_optimizer_v1.h"

#define PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION UINT32_C(1)
#define PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS UINT32_C(256)
#define PRDB_SDK_OPTIMIZER_PLAN_MAX_PROJECTIONS UINT32_C(128)

#define PRDB_SDK_OPTIMIZER_PLAN_OK INT32_C(0)
#define PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT INT32_C(1)
#define PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD INT32_C(2)
#define PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED INT32_C(3)

/* Plan names borrow the encoded payload. Column arrays are owned by this
 * output record and contain stable catalog IDs, never executor handles. */
typedef struct prdb_sdk_optimizer_plan_column_v1 {
    uint8_t column_id[16];
    uint32_t ordinal;
    uint8_t type_code;
    uint8_t type_parameters[4];
    uint8_t nullable;
} prdb_sdk_optimizer_plan_column_v1;

typedef struct prdb_sdk_optimizer_plan_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    const uint8_t *schema_name;
    uint32_t schema_name_len;
    uint8_t schema_id[16];
    uint64_t schema_version;
    const uint8_t *relation_name;
    uint32_t relation_name_len;
    uint8_t relation_id[16];
    uint64_t table_epoch;
    uint32_t scan_column_count;
    prdb_sdk_optimizer_plan_column_v1
        scan_columns[PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS];
    uint32_t projection_count;
    prdb_sdk_optimizer_plan_column_v1
        projections[PRDB_SDK_OPTIMIZER_PLAN_MAX_PROJECTIONS];
    uint32_t has_limit;
    uint32_t limit;
} prdb_sdk_optimizer_plan_v1;

/* Independently decodes the canonical plan v1 payload for the initial
 * Scan -> Project -> optional Limit subset. Filter plans return UNSUPPORTED
 * until their predicate tree is independently decoded and type checked.
 * Names borrow payload; the output record must not overlap it. */
int32_t prdb_sdk_optimizer_decode_plan_v1(
    const uint8_t *payload, uint64_t payload_len,
    prdb_sdk_optimizer_plan_v1 *out, uint32_t out_capacity);

#endif /* PRDB_SDK_OPTIMIZER_PLAN_V1_H */
