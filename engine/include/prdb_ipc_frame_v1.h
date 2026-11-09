#ifndef PRDB_IPC_FRAME_V1_H
#define PRDB_IPC_FRAME_V1_H

#include <stdint.h>
#include "prdb_query_abi_constants.h"

typedef enum prdb_ipc_frame_status_v1 {
    PRDB_IPC_FRAME_OK = 0,
    PRDB_IPC_FRAME_INVALID_ARGUMENT = 1,
    PRDB_IPC_FRAME_INVALID_MAGIC = 2,
    PRDB_IPC_FRAME_UNSUPPORTED_VERSION = 3,
    PRDB_IPC_FRAME_INVALID_OPERATION = 4,
    PRDB_IPC_FRAME_UNKNOWN_OPERATION = 5,
    PRDB_IPC_FRAME_INVALID_REQUEST_ID = 6,
    PRDB_IPC_FRAME_PAYLOAD_TOO_LARGE = 7,
    PRDB_IPC_FRAME_TRUNCATED = 8,
    PRDB_IPC_FRAME_OUTPUT_TOO_SMALL = 9
} prdb_ipc_frame_status_v1;

/* Payload bytes are borrowed. The structure describes one frame and is not
 * copied to or from the wire. */
typedef struct prdb_ipc_frame_v1 {
    uint16_t operation;
    uint64_t request_id;
    const uint8_t *payload;
    uint32_t payload_len;
} prdb_ipc_frame_v1;

/* A NULL output with zero capacity is a size query. On output-too-small,
 * output_len reports the complete required frame size and output is untouched.
 * output_len must not alias frame, output, or the borrowed payload. */
int32_t prdb_ipc_frame_encode_v1(const prdb_ipc_frame_v1 *frame,
                                 uint8_t *output,
                                 uint64_t output_capacity,
                                 uint64_t *output_len);

/* Decodes one frame prefix, allowing subsequent coalesced frames in input.
 * The output view borrows payload bytes from input. Outputs remain unchanged
 * on failure. frame and consumed_len must not alias each other or input. */
int32_t prdb_ipc_frame_decode_v1(const uint8_t *input,
                                 uint64_t input_len,
                                 prdb_ipc_frame_v1 *frame,
                                 uint64_t *consumed_len);

#endif /* PRDB_IPC_FRAME_V1_H */
