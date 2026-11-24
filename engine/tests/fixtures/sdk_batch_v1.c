#include "prdb_sdk_batch_v1.h"
#include "prdb_query_abi_constants.h"
#include "prdb_sdk_owned_batch_v1.h"

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
    static const uint8_t range_payload[] = {
        0x01, 0x01, 0x00, 0x01,
        0x00, 0x01,
        0x00, PRDB_SDK_VALUE_TYPE_INTEGER, 0x00, 0x00, 0x00, 0x04,
        0x00, 0x00, 0x00, 0x2a};
    prdb_sdk_index_range_v1 range;
    prdb_sdk_index_range_row_iterator_v1 range_rows;
    prdb_sdk_index_range_row_v1 range_row;
    static const uint8_t ranges_payload[] = {
        0x01, 0x01, 0x00, 0x02,
        0x00, 0x01, 0x00, 0x01, 0x00,
        PRDB_SDK_VALUE_TYPE_INTEGER, 0x00, 0x00, 0x00, 0x04,
        0x00, 0x00, 0x00, 0x2a,
        0x00, 0x00};
    prdb_sdk_index_ranges_v1 ranges;
    prdb_sdk_index_ranges_group_iterator_v1 range_groups;

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

    assert(prdb_sdk_index_range_open_v1(&range, sizeof(range), range_payload,
                                        sizeof(range_payload)) ==
           PRDB_SDK_BATCH_OK);
    assert(range.row_count == 1);
    assert(prdb_sdk_index_range_rows_begin_v1(
               &range, &range_rows, sizeof(range_rows)) == PRDB_SDK_BATCH_OK);
    assert(prdb_sdk_index_range_row_next_v1(
               &range_rows, &range_row, sizeof(range_row)) ==
           PRDB_SDK_BATCH_OK);
    assert(range_row.cell_count == 1);
    assert(prdb_sdk_index_range_row_cells_begin_v1(
               &range_row, &cells, sizeof(cells)) == PRDB_SDK_BATCH_OK);
    assert(prdb_sdk_cell_next_v1(&cells, &cell, sizeof(cell)) ==
           PRDB_SDK_BATCH_OK);
    assert(cell.type_code == PRDB_SDK_VALUE_TYPE_INTEGER && cell.data[3] == 42);
    {
        static const uint8_t unavailable[] = {0x01, 0x00};
        static const uint8_t trailing[] = {
            0x01, 0x01, 0x00, 0x00, 0x00};
        static const uint8_t invalid_state[] = {
            0x01, 0x01, 0x00, 0x01, 0x00, 0x01, 0x09};
        static const uint8_t too_many_rows[] = {0x01, 0x01, 0x04, 0x01};
        assert(prdb_sdk_index_range_open_v1(
                   &range, sizeof(range), unavailable, sizeof(unavailable)) ==
               PRDB_SDK_BATCH_UNAVAILABLE);
        assert(prdb_sdk_index_range_open_v1(
                   &range, sizeof(range), trailing, sizeof(trailing)) ==
               PRDB_SDK_BATCH_INVALID_PAYLOAD);
        assert(prdb_sdk_index_range_open_v1(
                   &range, sizeof(range), invalid_state,
                   sizeof(invalid_state)) == PRDB_SDK_BATCH_INVALID_PAYLOAD);
        assert(prdb_sdk_index_range_open_v1(
                   &range, sizeof(range), too_many_rows,
                   sizeof(too_many_rows)) == PRDB_SDK_BATCH_INVALID_PAYLOAD);
        assert(prdb_sdk_index_range_open_v1(
                   &range, sizeof(range), range_payload,
                   sizeof(range_payload) - 1) ==
               PRDB_SDK_BATCH_INVALID_PAYLOAD);
        {
            static const uint8_t unavailable_batch[] = {0x01, 0x00};
            static const uint8_t malformed_batch_count[] = {0x01, 0x01, 0x00, 0x00};
            static const uint8_t batch_trailing[] = {
                0x01, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00};
            assert(prdb_sdk_index_ranges_open_v1(
                       &ranges, sizeof(ranges), unavailable_batch,
                       sizeof(unavailable_batch)) == PRDB_SDK_BATCH_UNAVAILABLE);
            assert(prdb_sdk_index_ranges_open_v1(
                       &ranges, sizeof(ranges), malformed_batch_count,
                       sizeof(malformed_batch_count)) == PRDB_SDK_BATCH_INVALID_PAYLOAD);
            assert(prdb_sdk_index_ranges_open_v1(
                       &ranges, sizeof(ranges), batch_trailing,
                       sizeof(batch_trailing)) == PRDB_SDK_BATCH_INVALID_PAYLOAD);
        }
    }
    assert(prdb_sdk_index_range_row_next_v1(
               &range_rows, &range_row, sizeof(range_row)) ==
           PRDB_SDK_BATCH_END);

    assert(prdb_sdk_index_ranges_open_v1(&ranges, sizeof(ranges), ranges_payload,
                                         sizeof(ranges_payload)) ==
           PRDB_SDK_BATCH_OK);
    assert(ranges.group_count == 2);
    assert(prdb_sdk_index_ranges_groups_begin_v1(
               &ranges, &range_groups, sizeof(range_groups)) ==
           PRDB_SDK_BATCH_OK);
    assert(prdb_sdk_index_ranges_group_next_v1(
               &range_groups, &range, sizeof(range)) == PRDB_SDK_BATCH_OK);
    assert(range.row_count == 1);
    assert(prdb_sdk_index_range_rows_begin_v1(
               &range, &range_rows, sizeof(range_rows)) == PRDB_SDK_BATCH_OK);
    assert(prdb_sdk_index_range_row_next_v1(
               &range_rows, &range_row, sizeof(range_row)) ==
           PRDB_SDK_BATCH_OK);
    assert(prdb_sdk_index_range_row_cells_begin_v1(
               &range_row, &cells, sizeof(cells)) == PRDB_SDK_BATCH_OK);
    assert(prdb_sdk_cell_next_v1(&cells, &cell, sizeof(cell)) ==
           PRDB_SDK_BATCH_OK);
    assert(cell.type_code == PRDB_SDK_VALUE_TYPE_INTEGER && cell.data[3] == 42);
    assert(prdb_sdk_index_ranges_group_next_v1(
               &range_groups, &range, sizeof(range)) == PRDB_SDK_BATCH_OK);
    assert(range.row_count == 0);
    assert(prdb_sdk_index_ranges_group_next_v1(
               &range_groups, &range, sizeof(range)) == PRDB_SDK_BATCH_END);

    assert(prdb_sdk_batch_open_v1(&batch, sizeof(batch), payload,
                                  sizeof(payload) - 1) ==
           PRDB_SDK_BATCH_INVALID_PAYLOAD);
    assert(prdb_sdk_batch_open_v1(&batch, sizeof(batch), continuing_payload,
                                  sizeof(continuing_payload)) ==
           PRDB_SDK_BATCH_OK);
    assert(batch.finished == 0);
    assert(batch.row_count == 0);
    assert(batch.continuation ==
           continuing_payload +
               PRDB_EXECUTOR_NEXT_BATCH_RESPONSE_CONTINUATION_CURSOR_HANDLE_OFFSET);

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
    {
        prdb_sdk_owned_batch_v1 owned = {0};
        prdb_sdk_owned_batch_v1 invalid = {0};
        uint8_t source_payload[sizeof(payload)];
        prdb_sdk_batch_v1 owned_batch;
        prdb_sdk_row_iterator_v1 owned_rows;
        prdb_sdk_row_v1 owned_row;
        prdb_sdk_cell_iterator_v1 owned_cells;
        prdb_sdk_value_v1 owned_cell;

        assert(prdb_sdk_owned_batch_copy_v1(
                   &owned, (uint32_t)sizeof(owned) - 1, payload,
                   sizeof(payload)) == PRDB_SDK_BATCH_INVALID_ARGUMENT);
        memcpy(source_payload, payload, sizeof(payload));
        assert(prdb_sdk_owned_batch_copy_v1(
                   &owned, sizeof(owned), source_payload,
                   sizeof(source_payload)) ==
               PRDB_SDK_BATCH_OK);
        assert(prdb_sdk_owned_batch_copy_v1(
                   &owned, sizeof(owned), payload, sizeof(payload)) ==
               PRDB_SDK_BATCH_INVALID_ARGUMENT);
        assert(owned.payload != payload);
        assert(owned.payload_len == sizeof(payload));
        assert(memcmp(owned.payload, payload, sizeof(payload)) == 0);
        source_payload[32] = 0;
        assert(owned.payload[32] == 42);
        assert(prdb_sdk_owned_batch_open_v1(&owned, &owned_batch,
                                            sizeof(owned_batch)) ==
               PRDB_SDK_BATCH_OK);
        assert(prdb_sdk_batch_rows_begin_v1(&owned_batch, &owned_rows,
                                            sizeof(owned_rows)) ==
               PRDB_SDK_BATCH_OK);
        assert(prdb_sdk_batch_row_next_v1(&owned_rows, &owned_row,
                                          sizeof(owned_row)) ==
               PRDB_SDK_BATCH_OK);
        assert(prdb_sdk_row_cells_begin_v1(&owned_row, &owned_cells,
                                           sizeof(owned_cells)) ==
               PRDB_SDK_BATCH_OK);
        assert(prdb_sdk_cell_next_v1(&owned_cells, &owned_cell,
                                     sizeof(owned_cell)) == PRDB_SDK_BATCH_OK);
        assert(owned_cell.data >= owned.payload &&
               owned_cell.data < owned.payload + owned.payload_len);
        assert(prdb_sdk_owned_batch_release_v1(&owned) == PRDB_SDK_BATCH_OK);
        assert(owned.payload == NULL && owned.payload_len == 0);
        assert(prdb_sdk_owned_batch_release_v1(&owned) == PRDB_SDK_BATCH_OK);
        assert(prdb_sdk_owned_batch_open_v1(&owned, &owned_batch,
                                            sizeof(owned_batch)) ==
               PRDB_SDK_BATCH_INVALID_ARGUMENT);
        assert(prdb_sdk_owned_batch_copy_v1(
                   &owned, sizeof(owned), payload, sizeof(payload)) ==
               PRDB_SDK_BATCH_OK);
        assert(prdb_sdk_owned_batch_release_v1(&owned) == PRDB_SDK_BATCH_OK);
        memcpy(malformed_payload, payload, sizeof(payload));
        malformed_payload[1] = 2;
        assert(prdb_sdk_owned_batch_copy_v1(
                   &invalid, sizeof(invalid), malformed_payload,
                   sizeof(malformed_payload)) ==
               PRDB_SDK_BATCH_UNSUPPORTED_VERSION);
        assert(invalid.payload == NULL && invalid.payload_len == 0);
        memcpy(malformed_payload, payload, sizeof(payload));
        malformed_payload[5] = 9;
        assert(prdb_sdk_owned_batch_copy_v1(
                   &invalid, sizeof(invalid), malformed_payload,
                   sizeof(malformed_payload)) ==
               PRDB_SDK_BATCH_INVALID_PAYLOAD);
        assert(invalid.payload == NULL && invalid.payload_len == 0);
    }
    return 0;
}
