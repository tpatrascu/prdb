#ifndef PRDB_SDK_VALUE_V1_H
#define PRDB_SDK_VALUE_V1_H

#include <stdint.h>

#define PRDB_SDK_VALUE_ABI_V1_VERSION UINT32_C(1)
#define PRDB_SDK_VALUE_MAX_BYTES UINT64_C(1048576)

#define PRDB_SDK_VALUE_STATE_VALUE UINT32_C(0)
#define PRDB_SDK_VALUE_STATE_NULL UINT32_C(1)
#define PRDB_SDK_VALUE_STATE_REDACTED UINT32_C(2)

#define PRDB_SDK_VALUE_TYPE_BOOLEAN UINT32_C(1)
#define PRDB_SDK_VALUE_TYPE_SMALLINT UINT32_C(2)
#define PRDB_SDK_VALUE_TYPE_INTEGER UINT32_C(3)
#define PRDB_SDK_VALUE_TYPE_BIGINT UINT32_C(4)
#define PRDB_SDK_VALUE_TYPE_DECIMAL UINT32_C(5)
#define PRDB_SDK_VALUE_TYPE_TEXT UINT32_C(6)
#define PRDB_SDK_VALUE_TYPE_VARCHAR UINT32_C(7)
#define PRDB_SDK_VALUE_TYPE_BYTEA UINT32_C(8)
#define PRDB_SDK_VALUE_TYPE_UUID UINT32_C(9)
#define PRDB_SDK_VALUE_TYPE_DATE UINT32_C(10)
#define PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ UINT32_C(11)

#define PRDB_SDK_VALUE_VALID INT32_C(0)
#define PRDB_SDK_VALUE_INVALID_VALUE INT32_C(1)
#define PRDB_SDK_VALUE_UNSUPPORTED_VERSION INT32_C(2)
#define PRDB_SDK_VALUE_INVALID_ARGUMENT INT32_C(3)

/* data is borrowed and remains valid only as long as its source batch. */
typedef struct prdb_sdk_value_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    uint32_t state;
    uint32_t type_code;
    const uint8_t *data;
    uint64_t data_len;
} prdb_sdk_value_v1;

/* Validates representation only; it cannot make an invalid native pointer safe. */
int32_t prdb_sdk_value_validate_v1(const prdb_sdk_value_v1 *value);

#endif /* PRDB_SDK_VALUE_V1_H */
