#ifndef PRDB_SDK_BATCH_V1_H
#define PRDB_SDK_BATCH_V1_H

#include "prdb_sdk_value_v1.h"

#define PRDB_SDK_BATCH_ABI_V1_VERSION UINT32_C(1)
#define PRDB_SDK_BATCH_WIRE_V1_VERSION UINT16_C(1)
#define PRDB_SDK_BATCH_MAX_BYTES UINT64_C(1048576)
#define PRDB_SDK_BATCH_MAX_ROWS UINT16_C(128)
#define PRDB_SDK_BATCH_MAX_CELLS_PER_ROW UINT16_C(4096)

#define PRDB_SDK_BATCH_OK INT32_C(0)
#define PRDB_SDK_BATCH_END INT32_C(1)
#define PRDB_SDK_BATCH_INVALID_ARGUMENT INT32_C(2)
#define PRDB_SDK_BATCH_INVALID_PAYLOAD INT32_C(3)
#define PRDB_SDK_BATCH_UNSUPPORTED_VERSION INT32_C(4)

/* The token kind bytes are validated but the token contents remain opaque. */
#define PRDB_SDK_BATCH_CURSOR_TOKEN_KIND UINT8_C(7)
#define PRDB_SDK_BATCH_ROW_TOKEN_KIND UINT8_C(8)

typedef struct prdb_sdk_batch_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    const uint8_t *payload;
    uint64_t payload_len;
    uint32_t rows_offset;
    uint16_t row_count;
    uint8_t finished;
    const uint8_t *continuation;
} prdb_sdk_batch_v1;

typedef struct prdb_sdk_row_iterator_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    const prdb_sdk_batch_v1 *batch;
    uint32_t offset;
    uint16_t rows_remaining;
} prdb_sdk_row_iterator_v1;

typedef struct prdb_sdk_row_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    const uint8_t *handle;
    const uint8_t *payload;
    uint64_t payload_len;
    uint32_t cells_offset;
    uint16_t cell_count;
} prdb_sdk_row_v1;

typedef struct prdb_sdk_cell_iterator_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    const uint8_t *payload;
    uint64_t payload_len;
    uint32_t offset;
    uint16_t cells_remaining;
} prdb_sdk_cell_iterator_v1;

/* Opens and validates the complete immutable executor next_batch response. */
int32_t prdb_sdk_batch_open_v1(prdb_sdk_batch_v1 *out, uint32_t out_capacity,
                               const uint8_t *payload, uint64_t payload_len);
int32_t prdb_sdk_batch_rows_begin_v1(const prdb_sdk_batch_v1 *batch,
                                     prdb_sdk_row_iterator_v1 *out,
                                     uint32_t out_capacity);
int32_t prdb_sdk_batch_row_next_v1(prdb_sdk_row_iterator_v1 *iterator,
                                   prdb_sdk_row_v1 *out,
                                   uint32_t out_capacity);
int32_t prdb_sdk_row_cells_begin_v1(const prdb_sdk_row_v1 *row,
                                    prdb_sdk_cell_iterator_v1 *out,
                                    uint32_t out_capacity);
int32_t prdb_sdk_cell_next_v1(prdb_sdk_cell_iterator_v1 *iterator,
                              prdb_sdk_value_v1 *out,
                              uint32_t out_capacity);

#endif /* PRDB_SDK_BATCH_V1_H */
