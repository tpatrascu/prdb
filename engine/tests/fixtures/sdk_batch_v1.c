#include "prdb_sdk_batch_v1.h"

#include <assert.h>
#include <string.h>

int main(void) {
    static const uint8_t payload[] = {
        0x00, 0x01, 0x01, 0x00, 0x01,
        0x08, 0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70,
        0x80, 0x90, 0xa0, 0xb0, 0xc0, 0xd0, 0xe0, 0xf0,
        0x00, 0x03,
        0x00, PRDB_SDK_VALUE_TYPE_INTEGER, 0x00, 0x00, 0x00, 0x04,
        0x00, 0x00, 0x00, 0x2a,
        0x01,
        0x02};
    static const uint8_t continuing_payload[] = {
        0x00, 0x01, 0x00,
        0x07, 0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70,
        0x80, 0x90, 0xa0, 0xb0, 0xc0, 0xd0, 0xe0, 0xf0,
        0x00, 0x00};
    uint8_t malformed_payload[sizeof(payload)];
    prdb_sdk_batch_v1 batch;
    prdb_sdk_row_iterator_v1 rows;
    prdb_sdk_row_v1 row;
    prdb_sdk_cell_iterator_v1 cells;
    prdb_sdk_value_v1 cell;

    assert(prdb_sdk_batch_open_v1(&batch, sizeof(batch), payload,
                                  sizeof(payload)) ==
           PRDB_SDK_BATCH_OK);
    assert(batch.finished == 1);
    assert(batch.row_count == 1);
    assert(batch.continuation == 0);
    assert(prdb_sdk_batch_rows_begin_v1(&batch, &rows, sizeof(rows)) ==
           PRDB_SDK_BATCH_OK);
    assert(prdb_sdk_batch_row_next_v1(&rows, &row, sizeof(row)) ==
           PRDB_SDK_BATCH_OK);
    assert(row.cell_count == 3);
    assert(row.handle[0] == 8);
    assert(prdb_sdk_row_cells_begin_v1(&row, &cells, sizeof(cells)) ==
           PRDB_SDK_BATCH_OK);
    assert(prdb_sdk_cell_next_v1(&cells, &cell, sizeof(cell)) ==
           PRDB_SDK_BATCH_OK);
    assert(cell.state == PRDB_SDK_VALUE_STATE_VALUE);
    assert(cell.type_code == PRDB_SDK_VALUE_TYPE_INTEGER);
    assert(cell.data_len == 4);
    assert(cell.data[3] == 42);
    assert(cell.data >= payload && cell.data < payload + sizeof(payload));
    assert(prdb_sdk_cell_next_v1(&cells, &cell, sizeof(cell)) ==
           PRDB_SDK_BATCH_OK);
    assert(cell.state == PRDB_SDK_VALUE_STATE_NULL);
    assert(prdb_sdk_cell_next_v1(&cells, &cell, sizeof(cell)) ==
           PRDB_SDK_BATCH_OK);
    assert(cell.state == PRDB_SDK_VALUE_STATE_REDACTED);
    assert(prdb_sdk_cell_next_v1(&cells, &cell, sizeof(cell)) ==
           PRDB_SDK_BATCH_END);
    assert(prdb_sdk_batch_row_next_v1(&rows, &row, sizeof(row)) ==
           PRDB_SDK_BATCH_END);

    assert(prdb_sdk_batch_open_v1(&batch, sizeof(batch), payload,
                                  sizeof(payload) - 1) ==
           PRDB_SDK_BATCH_INVALID_PAYLOAD);
    assert(prdb_sdk_batch_open_v1(&batch, sizeof(batch), continuing_payload,
                                  sizeof(continuing_payload)) ==
           PRDB_SDK_BATCH_OK);
    assert(batch.finished == 0);
    assert(batch.row_count == 0);
    assert(batch.continuation == continuing_payload + 3);

    memcpy(malformed_payload, payload, sizeof(payload));
    malformed_payload[1] = 2;
    assert(prdb_sdk_batch_open_v1(&batch, sizeof(batch), malformed_payload,
                                  sizeof(malformed_payload)) ==
           PRDB_SDK_BATCH_UNSUPPORTED_VERSION);
    memcpy(malformed_payload, payload, sizeof(payload));
    malformed_payload[5] = 9;
    assert(prdb_sdk_batch_open_v1(&batch, sizeof(batch), malformed_payload,
                                  sizeof(malformed_payload)) ==
           PRDB_SDK_BATCH_INVALID_PAYLOAD);
    memcpy(malformed_payload, payload, sizeof(payload));
    malformed_payload[21] = 0xff;
    malformed_payload[22] = 0xff;
    assert(prdb_sdk_batch_open_v1(&batch, sizeof(batch), malformed_payload,
                                  sizeof(malformed_payload)) ==
           PRDB_SDK_BATCH_INVALID_PAYLOAD);
    return 0;
}
