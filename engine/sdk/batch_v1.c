#include "prdb_sdk_batch_v1.h"
#include "prdb_query_abi_payload_codecs.h"

#include <stddef.h>

static uint16_t read_u16(const uint8_t *payload, uint32_t offset) {
    return (uint16_t)(((uint16_t)payload[offset] << 8) | payload[offset + 1]);
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

    if (!take(payload, payload_len, offset,
              PRDB_SANITIZED_CELL_TAG_RECORD_HEADER_BYTES, &bytes) ||
        !prdb_decode_sanitized_cell_tag_record(
            bytes, PRDB_SANITIZED_CELL_TAG_RECORD_HEADER_BYTES, &state)) {
        return PRDB_SDK_BATCH_INVALID_PAYLOAD;
    }
    value->struct_size = sizeof(*value);
    value->abi_version = PRDB_SDK_VALUE_ABI_V1_VERSION;
    value->data = NULL;
    value->data_len = 0;
    value->type_code = 0;
    if (state == PRDB_SDK_VALUE_STATE_VALUE) {
        uint8_t type;
        uint32_t value_len;
        const uint8_t *value_bytes;
        size_t value_bytes_len;
        if (!take(payload, payload_len, offset,
                  PRDB_SCALAR_VALUE_RECORD_HEADER_BYTES, &bytes) ||
            !prdb_decode_scalar_value_record(
                bytes, PRDB_SCALAR_VALUE_RECORD_HEADER_BYTES, &type,
                &value_len) ||
            !take(payload, payload_len, offset, value_len, &bytes) ||
            !prdb_decode_scalar_value_record_value_bytes(
                bytes, (size_t)value_len, value_len, &value_bytes,
                &value_bytes_len)) {
            return PRDB_SDK_BATCH_INVALID_PAYLOAD;
        }
        value->state = PRDB_SDK_VALUE_STATE_VALUE;
        value->type_code = type;
        value->data = value_bytes;
        value->data_len = value_bytes_len;
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
    uint32_t offset;
    uint8_t finished;
    uint16_t row_count;
    uint8_t continuation_handle[16];
    uint32_t rows_offset;

    if (out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_BATCH_INVALID_ARGUMENT;
    }
    if (payload == NULL || payload_len < 5 ||
        payload_len > PRDB_SDK_BATCH_MAX_BYTES) {
        return PRDB_SDK_BATCH_INVALID_PAYLOAD;
    }
    if (!prdb_decode_executor_next_batch_response_finished(
            payload, (size_t)payload_len, &finished, &row_count)) {
        if (payload_len >= 2 &&
            read_u16(payload, 0) != PRDB_EXECUTOR_BATCH_PAYLOAD_VERSION) {
            return PRDB_SDK_BATCH_UNSUPPORTED_VERSION;
        }
        return PRDB_SDK_BATCH_INVALID_PAYLOAD;
    }
    if (finished > 1) {
        return PRDB_SDK_BATCH_INVALID_PAYLOAD;
    }
    if (finished == 0) {
        uint8_t continuation_finished;
        if (!prdb_decode_executor_next_batch_response_continuation(
                payload, (size_t)payload_len, &continuation_finished,
                continuation_handle, &row_count) ||
            continuation_finished != 0 ||
            continuation_handle[0] != PRDB_SDK_BATCH_CURSOR_TOKEN_KIND) {
            return PRDB_SDK_BATCH_INVALID_PAYLOAD;
        }
        offset = PRDB_EXECUTOR_NEXT_BATCH_RESPONSE_CONTINUATION_HEADER_BYTES;
    } else {
        offset = PRDB_EXECUTOR_NEXT_BATCH_RESPONSE_FINISHED_HEADER_BYTES;
    }
    if (row_count > PRDB_SDK_BATCH_MAX_ROWS) {
        return PRDB_SDK_BATCH_INVALID_PAYLOAD;
    }
    rows_offset = offset;

    for (uint16_t row_index = 0; row_index < row_count; ++row_index) {
        uint16_t cell_count;
        uint8_t row_handle[16];
        if ((uint64_t)offset > payload_len ||
            !prdb_decode_executor_batch_row_record(
                payload + offset, (size_t)(payload_len - offset), row_handle,
                &cell_count) ||
            row_handle[0] != PRDB_SDK_BATCH_ROW_TOKEN_KIND) {
            return PRDB_SDK_BATCH_INVALID_PAYLOAD;
        }
        offset += PRDB_EXECUTOR_BATCH_ROW_RECORD_HEADER_BYTES;
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
    out->continuation = finished == 0
                            ? payload + PRDB_EXECUTOR_NEXT_BATCH_RESPONSE_CONTINUATION_CURSOR_HANDLE_OFFSET
                            : NULL;
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
    uint32_t offset;
    uint16_t cell_count;
    uint8_t decoded_handle[16];

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
        (uint64_t)iterator->offset > batch->payload_len ||
        !prdb_decode_executor_batch_row_record(
            batch->payload + iterator->offset,
            (size_t)(batch->payload_len - iterator->offset), decoded_handle,
            &cell_count) ||
        decoded_handle[0] != PRDB_SDK_BATCH_ROW_TOKEN_KIND) {
        return PRDB_SDK_BATCH_INVALID_PAYLOAD;
    }
    handle = batch->payload + iterator->offset;
    iterator->offset += PRDB_EXECUTOR_BATCH_ROW_RECORD_HEADER_BYTES;
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

int32_t prdb_sdk_index_range_open_v1(
    prdb_sdk_index_range_v1 *out, uint32_t out_capacity,
    const uint8_t *payload, uint64_t payload_len) {
    uint32_t offset = PRDB_INDEX_RANGE_RESPONSE_ROWS_RECORD_HEADER_BYTES;
    uint16_t row_count;
    uint8_t version;
    uint8_t status;

    if (out == NULL || out_capacity < sizeof(*out)) {
        return PRDB_SDK_BATCH_INVALID_ARGUMENT;
    }
    if (payload == NULL || payload_len < 2 ||
        payload_len > PRDB_SDK_BATCH_MAX_BYTES) {
        return PRDB_SDK_BATCH_INVALID_PAYLOAD;
    }
    if (payload[0] != PRDB_INDEX_RANGE_RESPONSE_VERSION) {
        return PRDB_SDK_BATCH_UNSUPPORTED_VERSION;
    }
    if (prdb_decode_index_range_response_unavailable_record(
            payload, (size_t)payload_len, &version, &status)) {
        return status == 0 ? PRDB_SDK_BATCH_UNAVAILABLE
                           : PRDB_SDK_BATCH_INVALID_PAYLOAD;
    }
    if (!prdb_decode_index_range_response_rows_record(
            payload, (size_t)payload_len, &version, &status, &row_count)) {
        return PRDB_SDK_BATCH_INVALID_PAYLOAD;
    }
    if (version != PRDB_INDEX_RANGE_RESPONSE_VERSION) {
        return PRDB_SDK_BATCH_UNSUPPORTED_VERSION;
    }
    if (status != 1) return PRDB_SDK_BATCH_INVALID_PAYLOAD;
    if (row_count > PRDB_SDK_INDEX_RANGE_MAX_ROWS) {
        return PRDB_SDK_BATCH_INVALID_PAYLOAD;
    }
    for (uint16_t row_index = 0; row_index < row_count; ++row_index) {
        const uint8_t *count_bytes;
        uint16_t cell_count;
        if (!take(payload, payload_len, &offset,
                  PRDB_INDEX_RANGE_RESPONSE_ROW_RECORD_HEADER_BYTES,
                  &count_bytes) ||
            !prdb_decode_index_range_response_row_record(
                count_bytes,
                PRDB_INDEX_RANGE_RESPONSE_ROW_RECORD_HEADER_BYTES,
                &cell_count)) {
            return PRDB_SDK_BATCH_INVALID_PAYLOAD;
        }
        if (cell_count > PRDB_SDK_BATCH_MAX_CELLS_PER_ROW) {
            return PRDB_SDK_BATCH_INVALID_PAYLOAD;
        }
        for (uint16_t cell_index = 0; cell_index < cell_count; ++cell_index) {
            if (skip_cell(payload, payload_len, &offset) != PRDB_SDK_BATCH_OK) {
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
    out->rows_offset = PRDB_INDEX_RANGE_RESPONSE_ROWS_RECORD_HEADER_BYTES;
    out->row_count = row_count;
    return PRDB_SDK_BATCH_OK;
}

int32_t prdb_sdk_index_ranges_open_v1(
    prdb_sdk_index_ranges_v1 *out, uint32_t out_capacity,
    const uint8_t *payload, uint64_t payload_len) {
    uint32_t offset = 4;
    uint32_t candidate_count = 0;
    uint16_t group_count;

    if (out == NULL || out_capacity < sizeof(*out))
        return PRDB_SDK_BATCH_INVALID_ARGUMENT;
    if (payload == NULL || payload_len < 2 ||
        payload_len > PRDB_SDK_BATCH_MAX_BYTES)
        return PRDB_SDK_BATCH_INVALID_PAYLOAD;
    if (payload[0] != 1) return PRDB_SDK_BATCH_UNSUPPORTED_VERSION;
    if (payload[1] == 0)
        return payload_len == 2 ? PRDB_SDK_BATCH_UNAVAILABLE
                                : PRDB_SDK_BATCH_INVALID_PAYLOAD;
    if (payload[1] != 1 || payload_len < 4)
        return PRDB_SDK_BATCH_INVALID_PAYLOAD;
    group_count = read_u16(payload, 2);
    if (group_count == 0 || group_count > PRDB_SDK_BATCH_MAX_ROWS)
        return PRDB_SDK_BATCH_INVALID_PAYLOAD;

    for (uint16_t group_index = 0; group_index < group_count; ++group_index) {
        const uint8_t *row_count_bytes;
        uint16_t row_count;
        if (!take(payload, payload_len, &offset, 2, &row_count_bytes))
            return PRDB_SDK_BATCH_INVALID_PAYLOAD;
        row_count = read_u16(row_count_bytes, 0);
        if (row_count > PRDB_SDK_INDEX_RANGE_MAX_ROWS)
            return PRDB_SDK_BATCH_INVALID_PAYLOAD;
        candidate_count += row_count;
        if (candidate_count > UINT32_C(16384))
            return PRDB_SDK_BATCH_INVALID_PAYLOAD;
        for (uint16_t row_index = 0; row_index < row_count; ++row_index) {
            const uint8_t *cell_count_bytes;
            uint16_t cell_count;
            if (!take(payload, payload_len, &offset,
                      PRDB_INDEX_RANGE_RESPONSE_ROW_RECORD_HEADER_BYTES,
                      &cell_count_bytes) ||
                !prdb_decode_index_range_response_row_record(
                    cell_count_bytes,
                    PRDB_INDEX_RANGE_RESPONSE_ROW_RECORD_HEADER_BYTES,
                    &cell_count) ||
                cell_count > PRDB_SDK_BATCH_MAX_CELLS_PER_ROW)
                return PRDB_SDK_BATCH_INVALID_PAYLOAD;
            for (uint16_t cell_index = 0; cell_index < cell_count; ++cell_index) {
                if (skip_cell(payload, payload_len, &offset) != PRDB_SDK_BATCH_OK)
                    return PRDB_SDK_BATCH_INVALID_PAYLOAD;
            }
        }
    }
    if ((uint64_t)offset != payload_len)
        return PRDB_SDK_BATCH_INVALID_PAYLOAD;
    out->struct_size = sizeof(*out);
    out->abi_version = PRDB_SDK_BATCH_ABI_V1_VERSION;
    out->payload = payload;
    out->payload_len = payload_len;
    out->next_group_offset = 4;
    out->group_count = group_count;
    out->groups_remaining = group_count;
    return PRDB_SDK_BATCH_OK;
}

int32_t prdb_sdk_index_ranges_groups_begin_v1(
    const prdb_sdk_index_ranges_v1 *ranges,
    prdb_sdk_index_ranges_group_iterator_v1 *out, uint32_t out_capacity) {
    if (ranges == NULL || out == NULL || out_capacity < sizeof(*out) ||
        ranges->struct_size < sizeof(*ranges) ||
        ranges->abi_version != PRDB_SDK_BATCH_ABI_V1_VERSION)
        return PRDB_SDK_BATCH_INVALID_ARGUMENT;
    out->struct_size = sizeof(*out);
    out->abi_version = PRDB_SDK_BATCH_ABI_V1_VERSION;
    out->ranges = ranges;
    out->offset = ranges->next_group_offset;
    out->groups_remaining = ranges->groups_remaining;
    return PRDB_SDK_BATCH_OK;
}

int32_t prdb_sdk_index_ranges_group_next_v1(
    prdb_sdk_index_ranges_group_iterator_v1 *iterator,
    prdb_sdk_index_range_v1 *out, uint32_t out_capacity) {
    const prdb_sdk_index_ranges_v1 *ranges;
    const uint8_t *row_count_bytes;
    uint16_t row_count;
    uint32_t rows_offset;
    uint32_t offset;
    if (iterator == NULL || out == NULL || out_capacity < sizeof(*out) ||
        iterator->struct_size < sizeof(*iterator) ||
        iterator->abi_version != PRDB_SDK_BATCH_ABI_V1_VERSION ||
        iterator->ranges == NULL)
        return PRDB_SDK_BATCH_INVALID_ARGUMENT;
    if (iterator->groups_remaining == 0) return PRDB_SDK_BATCH_END;
    ranges = iterator->ranges;
    if (ranges->struct_size < sizeof(*ranges) ||
        ranges->abi_version != PRDB_SDK_BATCH_ABI_V1_VERSION ||
        !take(ranges->payload, ranges->payload_len, &iterator->offset, 2,
              &row_count_bytes))
        return PRDB_SDK_BATCH_INVALID_PAYLOAD;
    row_count = read_u16(row_count_bytes, 0);
    rows_offset = iterator->offset;
    offset = rows_offset;
    for (uint16_t row_index = 0; row_index < row_count; ++row_index) {
        const uint8_t *cell_count_bytes;
        uint16_t cell_count;
        if (!take(ranges->payload, ranges->payload_len, &offset,
                  PRDB_INDEX_RANGE_RESPONSE_ROW_RECORD_HEADER_BYTES,
                  &cell_count_bytes) ||
            !prdb_decode_index_range_response_row_record(
                cell_count_bytes,
                PRDB_INDEX_RANGE_RESPONSE_ROW_RECORD_HEADER_BYTES,
                &cell_count))
            return PRDB_SDK_BATCH_INVALID_PAYLOAD;
        for (uint16_t cell_index = 0; cell_index < cell_count; ++cell_index) {
            if (skip_cell(ranges->payload, ranges->payload_len, &offset) !=
                PRDB_SDK_BATCH_OK)
                return PRDB_SDK_BATCH_INVALID_PAYLOAD;
        }
    }
    out->struct_size = sizeof(*out);
    out->abi_version = PRDB_SDK_BATCH_ABI_V1_VERSION;
    out->payload = ranges->payload;
    out->payload_len = ranges->payload_len;
    out->rows_offset = rows_offset;
    out->row_count = row_count;
    iterator->offset = offset;
    --iterator->groups_remaining;
    return PRDB_SDK_BATCH_OK;
}

int32_t prdb_sdk_index_range_rows_begin_v1(
    const prdb_sdk_index_range_v1 *range,
    prdb_sdk_index_range_row_iterator_v1 *out, uint32_t out_capacity) {
    if (range == NULL || out == NULL || out_capacity < sizeof(*out) ||
        range->struct_size < sizeof(*range) ||
        range->abi_version != PRDB_SDK_BATCH_ABI_V1_VERSION) {
        return PRDB_SDK_BATCH_INVALID_ARGUMENT;
    }
    out->struct_size = sizeof(*out);
    out->abi_version = PRDB_SDK_BATCH_ABI_V1_VERSION;
    out->range = range;
    out->offset = range->rows_offset;
    out->rows_remaining = range->row_count;
    return PRDB_SDK_BATCH_OK;
}

int32_t prdb_sdk_index_range_row_next_v1(
    prdb_sdk_index_range_row_iterator_v1 *iterator,
    prdb_sdk_index_range_row_v1 *out, uint32_t out_capacity) {
    const prdb_sdk_index_range_v1 *range;
    const uint8_t *count_bytes;
    uint32_t offset;
    uint16_t cell_count;

    if (iterator == NULL || out == NULL || out_capacity < sizeof(*out) ||
        iterator->struct_size < sizeof(*iterator) ||
        iterator->abi_version != PRDB_SDK_BATCH_ABI_V1_VERSION ||
        iterator->range == NULL) {
        return PRDB_SDK_BATCH_INVALID_ARGUMENT;
    }
    if (iterator->rows_remaining == 0) return PRDB_SDK_BATCH_END;
    range = iterator->range;
    if (range->struct_size < sizeof(*range) ||
        range->abi_version != PRDB_SDK_BATCH_ABI_V1_VERSION ||
        range->payload == NULL ||
        !take(range->payload, range->payload_len, &iterator->offset,
              PRDB_INDEX_RANGE_RESPONSE_ROW_RECORD_HEADER_BYTES,
              &count_bytes) ||
        !prdb_decode_index_range_response_row_record(
            count_bytes, PRDB_INDEX_RANGE_RESPONSE_ROW_RECORD_HEADER_BYTES,
            &cell_count)) {
        return PRDB_SDK_BATCH_INVALID_PAYLOAD;
    }
    if (cell_count > PRDB_SDK_BATCH_MAX_CELLS_PER_ROW) {
        return PRDB_SDK_BATCH_INVALID_PAYLOAD;
    }
    offset = iterator->offset;
    for (uint16_t cell_index = 0; cell_index < cell_count; ++cell_index) {
        if (skip_cell(range->payload, range->payload_len, &offset) !=
            PRDB_SDK_BATCH_OK) {
            return PRDB_SDK_BATCH_INVALID_PAYLOAD;
        }
    }
    out->struct_size = sizeof(*out);
    out->abi_version = PRDB_SDK_BATCH_ABI_V1_VERSION;
    out->payload = range->payload;
    out->payload_len = range->payload_len;
    out->cells_offset = iterator->offset;
    out->cell_count = cell_count;
    iterator->offset = offset;
    --iterator->rows_remaining;
    return PRDB_SDK_BATCH_OK;
}

int32_t prdb_sdk_index_range_row_cells_begin_v1(
    const prdb_sdk_index_range_row_v1 *row,
    prdb_sdk_cell_iterator_v1 *out, uint32_t out_capacity) {
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
