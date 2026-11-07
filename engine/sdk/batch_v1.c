#include "prdb_sdk_batch_v1.h"

#include <stddef.h>

static uint16_t read_u16(const uint8_t *payload, uint32_t offset) {
    return (uint16_t)(((uint16_t)payload[offset] << 8) | payload[offset + 1]);
}

static uint32_t read_u32(const uint8_t *payload, uint32_t offset) {
    return ((uint32_t)payload[offset] << 24) |
           ((uint32_t)payload[offset + 1] << 16) |
           ((uint32_t)payload[offset + 2] << 8) |
           (uint32_t)payload[offset + 3];
}

static int take(const uint8_t *payload, uint64_t payload_len, uint32_t *offset,
                uint32_t length, const uint8_t **bytes) {
    if ((uint64_t)*offset > payload_len ||
        (uint64_t)length > payload_len - (uint64_t)*offset) {
        return 0;
    }
    *bytes = payload + *offset;
    *offset += length;
    return 1;
}

static int32_t decode_cell(const uint8_t *payload, uint64_t payload_len,
                           uint32_t *offset, prdb_sdk_value_v1 *value) {
    const uint8_t *bytes;
    uint32_t start = *offset;
    uint8_t state;

    if (!take(payload, payload_len, offset, 1, &bytes)) {
        return PRDB_SDK_BATCH_INVALID_PAYLOAD;
    }
    state = bytes[0];
    value->struct_size = sizeof(*value);
    value->abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
    value->data = NULL;
    value->data_len = 0;
    value->type_code = 0;
    if (state == PRDB_SDK_VALUE_STATE_VALUE) {
        uint8_t type;
        uint32_t value_len;
        if (!take(payload, payload_len, offset, 1, &bytes)) {
            return PRDB_SDK_BATCH_INVALID_PAYLOAD;
        }
        type = bytes[0];
        if (!take(payload, payload_len, offset, 4, &bytes)) {
            return PRDB_SDK_BATCH_INVALID_PAYLOAD;
        }
        value_len = read_u32(payload, *offset - 4);
        if (!take(payload, payload_len, offset, value_len, &bytes)) {
            return PRDB_SDK_BATCH_INVALID_PAYLOAD;
        }
        value->state = PRDB_SDK_VALUE_STATE_VALUE;
        value->type_code = type;
        value->data = bytes;
        value->data_len = value_len;
    } else if (state == PRDB_SDK_VALUE_STATE_NULL ||
               state == PRDB_SDK_VALUE_STATE_REDACTED) {
        value->state = state;
    } else {
        return PRDB_SDK_BATCH_INVALID_PAYLOAD;
    }
    if (prdb_sdk_value_validate_v1(value) != PRDB_SDK_VALUE_VALID) {
        return PRDB_SDK_BATCH_INVALID_PAYLOAD;
    }
    if (*offset <= start) {
        return PRDB_SDK_BATCH_INVALID_PAYLOAD;
    }
    return PRDB_SDK_BATCH_OK;
}

static int32_t skip_cell(const uint8_t *payload, uint64_t payload_len,
                         uint32_t *offset) {
    prdb_sdk_value_v1 value;
    return decode_cell(payload, payload_len, offset, &value);
}

int32_t prdb_sdk_batch_open_v1(prdb_sdk_batch_v1 *out, uint32_t out_capacity,
                               const uint8_t *payload, uint64_t payload_len) {
    uint32_t offset = 0;
    uint16_t version;
    uint8_t finished;
    uint16_t row_count;
    const uint8_t *bytes;
    uint32_t rows_offset;

    if (out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_BATCH_INVALID_ARGUMENT;
    }
    if (payload == NULL || payload_len < 5 ||
        payload_len > PRDB_SDK_BATCH_MAX_BYTES) {
        return PRDB_SDK_BATCH_INVALID_PAYLOAD;
    }
    version = read_u16(payload, offset);
    offset += 2;
    if (version != PRDB_SDK_BATCH_WIRE_V1_VERSION) {
        return PRDB_SDK_BATCH_UNSUPPORTED_VERSION;
    }
    if (!take(payload, payload_len, &offset, 1, &bytes)) {
        return PRDB_SDK_BATCH_INVALID_PAYLOAD;
    }
    finished = bytes[0];
    if (finished > 1) {
        return PRDB_SDK_BATCH_INVALID_PAYLOAD;
    }
    if (finished == 0) {
        if (!take(payload, payload_len, &offset, 16, &bytes) ||
            bytes[0] != PRDB_SDK_BATCH_CURSOR_TOKEN_KIND) {
            return PRDB_SDK_BATCH_INVALID_PAYLOAD;
        }
    }
    if (!take(payload, payload_len, &offset, 2, &bytes)) {
        return PRDB_SDK_BATCH_INVALID_PAYLOAD;
    }
    row_count = read_u16(payload, offset - 2);
    if (row_count > PRDB_SDK_BATCH_MAX_ROWS) {
        return PRDB_SDK_BATCH_INVALID_PAYLOAD;
    }
    rows_offset = offset;

    for (uint16_t row_index = 0; row_index < row_count; ++row_index) {
        uint16_t cell_count;
        if (!take(payload, payload_len, &offset, 16, &bytes) ||
            bytes[0] != PRDB_SDK_BATCH_ROW_TOKEN_KIND ||
            !take(payload, payload_len, &offset, 2, &bytes)) {
            return PRDB_SDK_BATCH_INVALID_PAYLOAD;
        }
        cell_count = read_u16(payload, offset - 2);
        if (cell_count > PRDB_SDK_BATCH_MAX_CELLS_PER_ROW) {
            return PRDB_SDK_BATCH_INVALID_PAYLOAD;
        }
        for (uint16_t cell_index = 0; cell_index < cell_count; ++cell_index) {
            if (skip_cell(payload, payload_len, &offset) !=
                PRDB_SDK_BATCH_OK) {
                return PRDB_SDK_BATCH_INVALID_PAYLOAD;
            }
        }
    }
    if ((uint64_t)offset != payload_len) {
        return PRDB_SDK_BATCH_INVALID_PAYLOAD;
    }

    out->struct_size = sizeof(*out);
    out->abi_version = PRDB_SDK_BATCH_ABI_V1_VERSION;
    out->payload = payload;
    out->payload_len = payload_len;
    out->rows_offset = rows_offset;
    out->row_count = row_count;
    out->finished = finished;
    out->continuation = finished == 0 ? payload + 3 : NULL;
    return PRDB_SDK_BATCH_OK;
}

int32_t prdb_sdk_batch_rows_begin_v1(const prdb_sdk_batch_v1 *batch,
                                     prdb_sdk_row_iterator_v1 *out,
                                     uint32_t out_capacity) {
    if (batch == NULL || out == NULL || out_capacity < sizeof(*out) ||
        batch->struct_size < sizeof(*batch) ||
        batch->abi_version != PRDB_SDK_BATCH_ABI_V1_VERSION) {
        return PRDB_SDK_BATCH_INVALID_ARGUMENT;
    }
    out->struct_size = sizeof(*out);
    out->abi_version = PRDB_SDK_BATCH_ABI_V1_VERSION;
    out->batch = batch;
    out->offset = batch->rows_offset;
    out->rows_remaining = batch->row_count;
    return PRDB_SDK_BATCH_OK;
}

int32_t prdb_sdk_batch_row_next_v1(prdb_sdk_row_iterator_v1 *iterator,
                                   prdb_sdk_row_v1 *out,
                                   uint32_t out_capacity) {
    const prdb_sdk_batch_v1 *batch;
    const uint8_t *handle;
    const uint8_t *bytes;
    uint32_t offset;
    uint16_t cell_count;

    if (iterator == NULL || out == NULL || out_capacity < sizeof(*out) ||
        iterator->struct_size < sizeof(*iterator) ||
        iterator->abi_version != PRDB_SDK_BATCH_ABI_V1_VERSION ||
        iterator->batch == NULL) {
        return PRDB_SDK_BATCH_INVALID_ARGUMENT;
    }
    if (iterator->rows_remaining == 0) {
        return PRDB_SDK_BATCH_END;
    }
    batch = iterator->batch;
    if (batch->struct_size < sizeof(*batch) ||
        batch->abi_version != PRDB_SDK_BATCH_ABI_V1_VERSION ||
        !take(batch->payload, batch->payload_len, &iterator->offset, 16,
              &handle) ||
        handle[0] != PRDB_SDK_BATCH_ROW_TOKEN_KIND ||
        !take(batch->payload, batch->payload_len, &iterator->offset, 2,
              &bytes)) {
        return PRDB_SDK_BATCH_INVALID_PAYLOAD;
    }
    cell_count = read_u16(batch->payload, iterator->offset - 2);
    if (cell_count > PRDB_SDK_BATCH_MAX_CELLS_PER_ROW) {
        return PRDB_SDK_BATCH_INVALID_PAYLOAD;
    }
    out->struct_size = sizeof(*out);
    out->abi_version = PRDB_SDK_BATCH_ABI_V1_VERSION;
    out->handle = handle;
    out->payload = batch->payload;
    out->payload_len = batch->payload_len;
    out->cells_offset = iterator->offset;
    out->cell_count = cell_count;
    for (uint16_t index = 0; index < cell_count; ++index) {
        if (skip_cell(batch->payload, batch->payload_len, &iterator->offset) !=
            PRDB_SDK_BATCH_OK) {
            return PRDB_SDK_BATCH_INVALID_PAYLOAD;
        }
    }
    --iterator->rows_remaining;
    return PRDB_SDK_BATCH_OK;
}

int32_t prdb_sdk_row_cells_begin_v1(const prdb_sdk_row_v1 *row,
                                    prdb_sdk_cell_iterator_v1 *out,
                                    uint32_t out_capacity) {
    if (row == NULL || out == NULL || out_capacity < sizeof(*out) ||
        row->struct_size < sizeof(*row) ||
        row->abi_version != PRDB_SDK_BATCH_ABI_V1_VERSION) {
        return PRDB_SDK_BATCH_INVALID_ARGUMENT;
    }
    out->struct_size = sizeof(*out);
    out->abi_version = PRDB_SDK_BATCH_ABI_V1_VERSION;
    out->payload = row->payload;
    out->payload_len = row->payload_len;
    out->offset = row->cells_offset;
    out->cells_remaining = row->cell_count;
    return PRDB_SDK_BATCH_OK;
}

int32_t prdb_sdk_cell_next_v1(prdb_sdk_cell_iterator_v1 *iterator,
                              prdb_sdk_value_v1 *out,
                              uint32_t out_capacity) {
    if (iterator == NULL || out == NULL || out_capacity < sizeof(*out) ||
        iterator->struct_size < sizeof(*iterator) ||
        iterator->abi_version != PRDB_SDK_BATCH_ABI_V1_VERSION) {
        return PRDB_SDK_BATCH_INVALID_ARGUMENT;
    }
    if (iterator->cells_remaining == 0) {
        return PRDB_SDK_BATCH_END;
    }
    if (decode_cell(iterator->payload, iterator->payload_len, &iterator->offset,
                    out) != PRDB_SDK_BATCH_OK) {
        return PRDB_SDK_BATCH_INVALID_PAYLOAD;
    }
    --iterator->cells_remaining;
    return PRDB_SDK_BATCH_OK;
}
