#include "prdb_sdk_optimizer_plan_v1.h"

#include <assert.h>
#include <stdint.h>
#include <string.h>

static void put_u16(uint8_t *bytes, uint32_t *offset, uint16_t value) {
    bytes[(*offset)++] = (uint8_t)(value >> 8);
    bytes[(*offset)++] = (uint8_t)value;
}

static void put_u32(uint8_t *bytes, uint32_t *offset, uint32_t value) {
    bytes[(*offset)++] = (uint8_t)(value >> 24);
    bytes[(*offset)++] = (uint8_t)(value >> 16);
    bytes[(*offset)++] = (uint8_t)(value >> 8);
    bytes[(*offset)++] = (uint8_t)value;
}

static void put_u64(uint8_t *bytes, uint32_t *offset, uint64_t value) {
    put_u32(bytes, offset, (uint32_t)(value >> 32));
    put_u32(bytes, offset, (uint32_t)value);
}

static uint32_t put_column(uint8_t *bytes, uint32_t *offset,
                           uint8_t type_code, uint8_t nullable) {
    uint32_t id_offset = *offset;
    bytes[(*offset)++] = 3;
    memset(bytes + *offset, 0, 15);
    *offset += 15;
    put_u32(bytes, offset, 3);
    bytes[(*offset)++] = type_code;
    memset(bytes + *offset, 0, 4);
    *offset += 4;
    bytes[(*offset)++] = nullable;
    return id_offset;
}

static uint32_t put_header(uint8_t *bytes, uint32_t *offset,
                           uint8_t node_count) {
    bytes[(*offset)++] = 0;
    bytes[(*offset)++] = 1; /* version */
    bytes[(*offset)++] = 1; /* SELECT */
    bytes[(*offset)++] = 0; /* reserved */
    bytes[(*offset)++] = 6;
    memcpy(bytes + *offset, "public", 6);
    *offset += 6;
    bytes[(*offset)++] = 1;
    memset(bytes + *offset, 0, 15);
    *offset += 15;
    put_u64(bytes, offset, 1); /* schema version */
    bytes[(*offset)++] = 5;
    memcpy(bytes + *offset, "items", 5);
    *offset += 5;
    bytes[(*offset)++] = 2;
    memset(bytes + *offset, 0, 15);
    *offset += 15;
    put_u64(bytes, offset, 1); /* table epoch */
    bytes[(*offset)++] = node_count;
    return *offset;
}

static uint32_t build_plan(uint8_t *bytes, uint32_t *projected_id_offset,
                           uint32_t *project_node_offset) {
    uint32_t offset = 0;
    (void)put_header(bytes, &offset, 3);
    bytes[offset++] = 1;
    put_u16(bytes, &offset, 1);
    (void)put_column(bytes, &offset, 6, 1);
    *project_node_offset = offset;
    bytes[offset++] = 3;
    put_u16(bytes, &offset, 1);
    *projected_id_offset = put_column(bytes, &offset, 6, 1);
    bytes[offset++] = 4;
    put_u32(bytes, &offset, 7);
    return offset;
}

static void put_integer_comparison(uint8_t *bytes, uint32_t *offset,
                                   uint8_t integer_value) {
    bytes[(*offset)++] = 0x20;
    bytes[(*offset)++] = 0;
    bytes[(*offset)++] = 0x10;
    bytes[(*offset)++] = 10;
    memset(bytes + *offset, 0, 15);
    *offset += 15;
    bytes[(*offset)++] = 0x02;
    memset(bytes + *offset, 0, 7);
    *offset += 7;
    bytes[(*offset)++] = integer_value;
}

static uint32_t build_filter_plan(uint8_t *bytes,
                                  uint32_t *predicate_offset) {
    uint32_t offset = 0;
    (void)put_header(bytes, &offset, 4);
    bytes[offset++] = 1;
    put_u16(bytes, &offset, 1);
    uint32_t column_id_offset = put_column(bytes, &offset, 3, 0);
    bytes[column_id_offset] = 10;
    bytes[column_id_offset + 16] = 0;
    bytes[column_id_offset + 17] = 0;
    bytes[column_id_offset + 18] = 0;
    bytes[column_id_offset + 19] = 0; /* ordinal 0 */
    bytes[offset++] = 2;
    *predicate_offset = offset;
    bytes[offset++] = 0x30; /* AND of comparison and TRUE */
    bytes[offset++] = 2;
    put_integer_comparison(bytes, &offset, 1);
    bytes[offset++] = 0x01;
    bytes[offset++] = 1;
    bytes[offset++] = 3;
    put_u16(bytes, &offset, 1);
    column_id_offset = put_column(bytes, &offset, 3, 0);
    bytes[column_id_offset] = 10;
    bytes[column_id_offset + 16] = 0;
    bytes[column_id_offset + 17] = 0;
    bytes[column_id_offset + 18] = 0;
    bytes[column_id_offset + 19] = 0; /* ordinal 0 */
    bytes[offset++] = 4;
    put_u32(bytes, &offset, 1);
    return offset;
}

typedef struct resolve_context {
    uint8_t response[512];
    uint32_t response_len;
    uint8_t executor_response[1024];
    uint32_t executor_response_len;
    uint8_t scan_response[18];
    uint8_t batch_response[128];
    uint32_t batch_response_len;
    uint8_t cancellation_response[3];
    uint8_t close_response[3];
    uint8_t optimizer_response[512];
    uint32_t optimizer_response_len;
    uint32_t calls;
    uint32_t optimizer_calls;
    uint32_t statement_failures;
    uint16_t events[32];
    uint32_t event_count;
} resolve_context;

static int32_t optimizer_request(void *context, const uint8_t *request,
                                 uint64_t request_len, uint8_t *response,
                                 uint64_t response_capacity,
                                 uint64_t *response_len) {
    resolve_context *resolved = (resolve_context *)context;
    assert(request_len >= 16 && request[0] == 0 && request[1] == 1);
    if (resolved->event_count < 32) {
        resolved->events[resolved->event_count++] = UINT16_C(0xffff);
    }
    resolved->optimizer_calls++;
    if (response_capacity < resolved->optimizer_response_len) {
        *response_len = resolved->optimizer_response_len;
        return PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL;
    }
    memcpy(response, resolved->optimizer_response,
           resolved->optimizer_response_len);
    *response_len = resolved->optimizer_response_len;
    return PRDB_QUERY_CALLBACK_OK;
}

static int32_t statement_failure(void *context, uint32_t failure_code) {
    resolve_context *resolved = (resolve_context *)context;
    assert(failure_code == PRDB_QUERY_STATEMENT_FAILURE_SQL);
    resolved->statement_failures++;
    return PRDB_QUERY_CALLBACK_OK;
}

static int32_t resolve_plan_relation(void *context, uint16_t operation,
                                     const uint8_t *request,
                                     uint64_t request_len, uint8_t *response,
                                     uint64_t response_capacity,
                                     uint64_t *response_len) {
    resolve_context *resolved = (resolve_context *)context;
    const uint8_t *source;
    uint32_t source_len;
    if (operation == PRDB_IPC_OP_RESOLVE_PLAN_RELATION) {
        assert(request_len == 17 && request[0] == 0 && request[1] == 1);
        assert(request[2] == 0 && request[3] == 6);
        assert(memcmp(request + 6, "public", 6) == 0);
        assert(request[4] == 0 && request[5] == 5);
        assert(memcmp(request + 12, "items", 5) == 0);
        source = resolved->response;
        source_len = resolved->response_len;
    } else if (operation == PRDB_SDK_EXECUTOR_OP_RESOLVE_RELATION) {
        assert(request_len == 17 && request[0] == 0 && request[1] == 1);
        assert(request[2] == 0 && request[3] == 6);
        assert(memcmp(request + 6, "public", 6) == 0);
        assert(request[4] == 0 && request[5] == 5);
        assert(memcmp(request + 12, "items", 5) == 0);
        source = resolved->executor_response;
        source_len = resolved->executor_response_len;
    } else if (operation == PRDB_SDK_EXECUTOR_OP_OPEN_SCAN) {
        assert(request_len == 20 && request[0] == 0 && request[1] == 1);
        assert(request[2] == 6);
        assert(request[18] == 0 && request[19] == 0);
        source = resolved->scan_response;
        source_len = sizeof(resolved->scan_response);
    } else if (operation == PRDB_SDK_EXECUTOR_OP_CHECK_CANCELLATION) {
        assert(request_len == 0);
        source = resolved->cancellation_response;
        source_len = sizeof(resolved->cancellation_response);
    } else if (operation == PRDB_SDK_EXECUTOR_OP_NEXT_BATCH) {
        assert(request_len == 24 && request[2] == 7);
        source = resolved->batch_response;
        source_len = resolved->batch_response_len;
    } else {
        assert(operation == PRDB_SDK_EXECUTOR_OP_CLOSE_CURSOR);
        assert(request_len == 18 && request[2] == 7);
        source = resolved->close_response;
        source_len = sizeof(resolved->close_response);
    }
    resolved->calls++;
    if (resolved->event_count < 32) {
        resolved->events[resolved->event_count++] = operation;
    }
    if (response_capacity < source_len) {
        *response_len = source_len;
        return PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL;
    }
    memcpy(response, source, source_len);
    *response_len = source_len;
    return PRDB_QUERY_CALLBACK_OK;
}

static uint32_t build_relation_response(uint8_t *bytes) {
    uint32_t offset = 0;
    bytes[offset++] = 0;
    bytes[offset++] = 1;
    bytes[offset++] = 0;
    bytes[offset++] = 1;
    memset(bytes + offset, 0, 15);
    offset += 15;
    put_u64(bytes, &offset, 1);
    put_u64(bytes, &offset, 1);
    bytes[offset++] = 2;
    memset(bytes + offset, 0, 15);
    offset += 15;
    put_u16(bytes, &offset, 5);
    memcpy(bytes + offset, "items", 5);
    offset += 5;
    put_u16(bytes, &offset, 4);
    for (uint32_t ordinal = 0; ordinal < 4; ++ordinal) {
        bytes[offset++] = ordinal == 3 ? 3 : (uint8_t)(10 + ordinal);
        memset(bytes + offset, 0, 15);
        offset += 15;
        put_u32(bytes, &offset, ordinal);
        bytes[offset++] = ordinal == 3 ? 1 : 0;
        bytes[offset++] = ordinal == 3 ? 6 : 3;
        memset(bytes + offset, 0, 4);
        offset += 4;
        put_u16(bytes, &offset, 2);
        bytes[offset++] = 'c';
        bytes[offset++] = (uint8_t)('0' + ordinal);
    }
    return offset;
}

static uint32_t build_executor_relation_response(uint8_t *bytes) {
    uint32_t offset = 0;
    bytes[offset++] = 0;
    bytes[offset++] = 1;
    bytes[offset++] = 0;
    bytes[offset++] = 2;
    memset(bytes + offset, 0, 15);
    offset += 15;
    bytes[offset++] = 6;
    memset(bytes + offset, 0, 15);
    offset += 15;
    put_u16(bytes, &offset, 5);
    memcpy(bytes + offset, "items", 5);
    offset += 5;
    put_u16(bytes, &offset, 4);
    for (uint32_t ordinal = 0; ordinal < 4; ++ordinal) {
        bytes[offset++] = ordinal == 3 ? 3 : (uint8_t)(10 + ordinal);
        memset(bytes + offset, 0, 15);
        offset += 15;
        put_u32(bytes, &offset, ordinal);
        bytes[offset++] = ordinal == 3 ? 1 : 0;
        bytes[offset++] = ordinal == 3 ? 6 : 3;
        memset(bytes + offset, 0, 4);
        offset += 4;
        put_u16(bytes, &offset, 2);
        bytes[offset++] = 'c';
        bytes[offset++] = (uint8_t)('0' + ordinal);
    }
    return offset;
}

static uint32_t build_row_batch(uint8_t *bytes) {
    uint32_t offset = 0;
    put_u16(bytes, &offset, 1);
    bytes[offset++] = 1;
    put_u16(bytes, &offset, 1);
    bytes[offset++] = 8;
    memset(bytes + offset, 0, 15);
    offset += 15;
    put_u16(bytes, &offset, 4);
    for (uint32_t ordinal = 0; ordinal < 4; ++ordinal) {
        bytes[offset++] = PRDB_SDK_VALUE_STATE_VALUE;
        bytes[offset++] = ordinal == 3 ? PRDB_SDK_VALUE_TYPE_TEXT
                                       : PRDB_SDK_VALUE_TYPE_INTEGER;
        put_u32(bytes, &offset, ordinal == 3 ? 1 : 4);
        if (ordinal == 3) {
            bytes[offset++] = 'x';
        } else {
            memset(bytes + offset, 0, 3);
            bytes[offset + 3] = (uint8_t)(ordinal + 1);
            offset += 4;
        }
    }
    return offset;
}

static uint32_t build_continuing_row_batch(uint8_t *bytes) {
    uint32_t length = build_row_batch(bytes);
    memmove(bytes + 19, bytes + 3, length - 3);
    bytes[2] = 0;
    bytes[3] = 7;
    memset(bytes + 4, 0, 15);
    return length + 16;
}

static uint32_t build_optimizer_response(uint8_t *bytes,
                                         const uint8_t *plan,
                                         uint32_t plan_len) {
    uint32_t offset = 0;
    put_u16(bytes, &offset, 1);
    bytes[offset++] = PRDB_SDK_OPTIMIZER_STATUS_SUCCESS;
    put_u32(bytes, &offset, plan_len);
    memcpy(bytes + offset, plan, plan_len);
    return offset + plan_len;
}

static uint32_t projected_rows_seen;

static int32_t on_projected_row(
    void *context, const prdb_sdk_optimizer_plan_column_v1 *projections,
    const prdb_sdk_value_v1 *values, uint32_t projection_count) {
    (void)context;
    assert(projection_count == 1);
    if (projections[0].ordinal == 3) {
        assert(values[0].type_code == PRDB_SDK_VALUE_TYPE_TEXT);
        assert(values[0].data_len == 1 && values[0].data[0] == 'x');
    } else {
        assert(projections[0].ordinal == 0);
        assert(values[0].type_code == PRDB_SDK_VALUE_TYPE_INTEGER);
        assert(values[0].data_len == 4 && values[0].data[3] == 1);
    }
    projected_rows_seen++;
    return 0;
}

static int32_t fail_projected_row(
    void *context, const prdb_sdk_optimizer_plan_column_v1 *projections,
    const prdb_sdk_value_v1 *values, uint32_t projection_count) {
    (void)context;
    (void)projections;
    (void)values;
    (void)projection_count;
    return 1;
}

int main(void) {
    uint8_t payload[256] = {0};
    uint8_t malformed[sizeof(payload)];
    uint32_t projected_id_offset;
    uint32_t project_node_offset;
    uint32_t predicate_offset;
    uint32_t payload_len = build_plan(payload, &projected_id_offset,
                                      &project_node_offset);
    static prdb_sdk_optimizer_plan_v1 decoded;
    static prdb_sdk_optimizer_filter_v1 filter;
    static resolve_context resolve = {0};
    static prdb_host_api_v2 host = {0};
    static prdb_host_api_v4 host_v4 = {0};
    static prdb_sdk_executor_relation_v1 acquired_relation;
    static prdb_sdk_sql_column_shape_v2 acquired_columns[4];
    static prdb_sdk_optimizer_scan_state_v1 scan_state;
    static prdb_sdk_batch_v1 row_batch;
    static prdb_sdk_value_v1 source_cells[4];
    static prdb_sdk_value_v1 projected_values[1];
    uint8_t row_batch_bytes[128];
    uint8_t batch_response[128];
    uint8_t scan_response[18];
    uint32_t row_batch_len;
    uint32_t delivered_rows = 0;
    uint32_t total_rows = 0;
    uint8_t response[512];
    uint8_t request[512];
    uint8_t optimizer_response[512];
    uint8_t sql[] = "SELECT c3 FROM items LIMIT 1";
    static prdb_sdk_select_v2 statement;
    uint64_t response_len = 0;
    uint64_t scan_response_len = 0;

    assert(prdb_sdk_optimizer_decode_plan_v1(
               payload, payload_len, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               payload, payload_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(filter.present == 0 && filter.prefix == NULL &&
           filter.prefix_len == 0);
    assert(decoded.struct_size == sizeof(decoded));
    assert(decoded.abi_version == PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION);
    assert(decoded.schema_name_len == 6);
    assert(memcmp(decoded.schema_name, "public", 6) == 0);
    assert(decoded.relation_name_len == 5);
    assert(memcmp(decoded.relation_name, "items", 5) == 0);
    assert(decoded.schema_version == 1 && decoded.table_epoch == 1);
    assert(decoded.scan_column_count == 1 && decoded.projection_count == 1);
    assert(decoded.scan_columns[0].ordinal == 3);
    assert(decoded.projections[0].type_code == 6);
    assert(decoded.has_limit == 1 && decoded.limit == 7);
    assert(prdb_sdk_optimizer_scan_state_init_v1(
               payload, payload_len, &scan_state, sizeof(scan_state)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    row_batch_len = build_row_batch(row_batch_bytes);
    assert(prdb_sdk_batch_open_v1(&row_batch, sizeof(row_batch), row_batch_bytes,
                                  row_batch_len) == PRDB_SDK_BATCH_OK);
    projected_rows_seen = 0;
    assert(prdb_sdk_optimizer_consume_scan_batch_v1(
               &scan_state, &row_batch, 4, source_cells, 4, projected_values,
               1, on_projected_row, NULL, &delivered_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(delivered_rows == 1 && projected_rows_seen == 1);
    uint8_t limit_one_plan[sizeof(payload)];
    memcpy(limit_one_plan, payload, payload_len);
    limit_one_plan[payload_len - 1] = 1;
    assert(prdb_sdk_optimizer_scan_state_init_v1(
               limit_one_plan, payload_len, &scan_state, sizeof(scan_state)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(prdb_sdk_optimizer_consume_scan_batch_v1(
               &scan_state, &row_batch, 4, source_cells, 4, projected_values,
               1, on_projected_row, NULL, &delivered_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(delivered_rows == 1 && scan_state.done == 1);
    assert(prdb_sdk_optimizer_consume_scan_batch_v1(
               &scan_state, &row_batch, 4, source_cells, 4, projected_values,
               1, on_projected_row, NULL, &delivered_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(delivered_rows == 0 && projected_rows_seen == 2);
    payload_len = build_filter_plan(payload, &predicate_offset);
    assert(prdb_sdk_optimizer_scan_state_init_v1(
               payload, payload_len, &scan_state, sizeof(scan_state)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    scan_state.projections[0].ordinal = 1;
    assert(prdb_sdk_optimizer_consume_plan_batch_v1(
               payload, payload_len, &scan_state, &row_batch, 4, source_cells,
               4, projected_values, 1, on_projected_row, NULL,
               &delivered_rows) == PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    scan_state.projections[0].ordinal = 0;
    projected_rows_seen = 0;
    assert(prdb_sdk_optimizer_consume_plan_batch_v1(
               payload, payload_len, &scan_state, &row_batch, 4, source_cells,
               4, projected_values, 1, on_projected_row, NULL,
               &delivered_rows) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(delivered_rows == 1 && projected_rows_seen == 1);
    row_batch_bytes[32] = 2;
    assert(prdb_sdk_batch_open_v1(&row_batch, sizeof(row_batch), row_batch_bytes,
                                  row_batch_len) == PRDB_SDK_BATCH_OK);
    assert(prdb_sdk_optimizer_scan_state_init_v1(
               payload, payload_len, &scan_state, sizeof(scan_state)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(prdb_sdk_optimizer_consume_plan_batch_v1(
               payload, payload_len, &scan_state, &row_batch, 4, source_cells,
               4, projected_values, 1, on_projected_row, NULL,
               &delivered_rows) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(delivered_rows == 0 && projected_rows_seen == 1);
    row_batch_bytes[32] = 1;
    assert(prdb_sdk_batch_open_v1(&row_batch, sizeof(row_batch), row_batch_bytes,
                                  row_batch_len) == PRDB_SDK_BATCH_OK);
    payload_len = build_plan(payload, &projected_id_offset,
                              &project_node_offset);

    resolve.response_len = build_relation_response(resolve.response);
    resolve.executor_response_len = build_executor_relation_response(
        resolve.executor_response);
    resolve.scan_response[1] = 1;
    resolve.scan_response[2] = 7;
    resolve.batch_response_len = build_continuing_row_batch(
        resolve.batch_response);
    resolve.cancellation_response[1] = 1;
    resolve.close_response[1] = 1;
    host.struct_size = sizeof(host);
    host.abi_version = PRDB_QUERY_ABI_V2_VERSION;
    host.context = &resolve;
    host.executor_request = resolve_plan_relation;
    uint8_t filtered_plan[512];
    uint32_t filtered_plan_len = build_filter_plan(filtered_plan,
                                                   &predicate_offset);
    resolve.calls = 0;
    projected_rows_seen = 0;
    assert(prdb_sdk_optimizer_execute_unfiltered_plan_v1(
               &host, filtered_plan, filtered_plan_len, 2,
               sizeof(batch_response), response, sizeof(response),
               scan_response, sizeof(scan_response), batch_response,
               sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL, &total_rows) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(total_rows == 1 && projected_rows_seen == 1 && resolve.calls == 6);
    assert(prdb_sdk_optimizer_validate_executor_relation_v1(
               &host, payload, payload_len, response, sizeof(response),
               &response_len) == PRDB_SDK_OPTIMIZER_PLAN_OK);

    resolve.response[34] = 2; /* A newer table epoch must reject this plan. */
    assert(prdb_sdk_optimizer_validate_executor_relation_v1(
               &host, payload, payload_len, response, sizeof(response),
               &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    resolve.response[34] = 1;
    resolve.response[150] = 4; /* Scanned column ID differs at ordinal 3. */
    assert(prdb_sdk_optimizer_validate_executor_relation_v1(
               &host, payload, payload_len, response, sizeof(response),
               &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    resolve.response[150] = 3;
    resolve.response[2] = 1;
    resolve.response_len = 3;
    assert(prdb_sdk_optimizer_validate_executor_relation_v1(
               &host, payload, payload_len, response, sizeof(response),
               &response_len) == PRDB_SDK_OPTIMIZER_PLAN_UNAVAILABLE);
    resolve.response[2] = 0;
    resolve.response_len = build_relation_response(resolve.response);
    assert(prdb_sdk_optimizer_validate_executor_relation_v1(
               &host, payload, payload_len, response, 3, &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL);

    memcpy(limit_one_plan, payload, payload_len);
    limit_one_plan[payload_len - 1] = 1;
    projected_rows_seen = 0;
    resolve.calls = 0;
    int32_t execute_status = prdb_sdk_optimizer_execute_unfiltered_plan_v1(
               &host, limit_one_plan, payload_len, 2, sizeof(batch_response), response,
               sizeof(response), scan_response, sizeof(scan_response),
               batch_response, sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL,
               &total_rows);
    assert(execute_status == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(total_rows == 1 && projected_rows_seen == 1 && resolve.calls == 6);

    assert(prdb_sdk_sql_parse_select_v2(
               sql, sizeof(sql) - 1, &statement, sizeof(statement), NULL) ==
           PRDB_SDK_SQL_OK);
    resolve.calls = 0;
    assert(prdb_sdk_optimizer_validate_select_plan_v1(
               &host, &statement, (const uint8_t *)"public", 6, limit_one_plan,
               payload_len, response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.calls == 1);
    uint8_t sql_wrong_projection[] = "SELECT c2 FROM items LIMIT 1";
    assert(prdb_sdk_sql_parse_select_v2(
               sql_wrong_projection, sizeof(sql_wrong_projection) - 1,
               &statement, sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    uint32_t calls_before_projection_mismatch = resolve.calls;
    assert(prdb_sdk_optimizer_validate_select_plan_v1(
               &host, &statement, (const uint8_t *)"public", 6, limit_one_plan,
               payload_len, response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    assert(resolve.calls == calls_before_projection_mismatch + 1);
    uint8_t sql_wrong_limit[] = "SELECT c3 FROM items LIMIT 2";
    assert(prdb_sdk_sql_parse_select_v2(
               sql_wrong_limit, sizeof(sql_wrong_limit) - 1, &statement,
               sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    calls_before_projection_mismatch = resolve.calls;
    assert(prdb_sdk_optimizer_validate_select_plan_v1(
               &host, &statement, (const uint8_t *)"public", 6, limit_one_plan,
               payload_len, response, sizeof(response), &response_len) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    assert(resolve.calls == calls_before_projection_mismatch);
    assert(prdb_sdk_sql_parse_select_v2(
               sql, sizeof(sql) - 1, &statement, sizeof(statement), NULL) ==
           PRDB_SDK_SQL_OK);
    resolve.optimizer_response_len = build_optimizer_response(
        resolve.optimizer_response, limit_one_plan, payload_len);
    host_v4.v3.struct_size = sizeof(host_v4.v3);
    host_v4.v3.abi_version = PRDB_QUERY_ABI_V3_VERSION;
    host_v4.v3.context = &resolve;
    host_v4.v3.executor_request = resolve_plan_relation;
    host_v4.v3.statement_failure = statement_failure;
    host_v4.optimizer_request = optimizer_request;
    resolve.optimizer_calls = 0;
    resolve.calls = 0;
    resolve.event_count = 0;
    resolve.statement_failures = 0;
    projected_rows_seen = 0;
    assert(prdb_sdk_optimizer_execute_select_ast_v1(
               &host_v4, &statement, (const uint8_t *)"public", 6, 2,
               sizeof(batch_response), request, sizeof(request),
               optimizer_response, sizeof(optimizer_response), response,
               sizeof(response), scan_response, sizeof(scan_response),
               batch_response, sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL,
               &total_rows) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.optimizer_calls == 1 && resolve.calls == 7);
    assert(resolve.statement_failures == 0 && total_rows == 1 &&
           projected_rows_seen == 1);
    assert(resolve.event_count == 8 && resolve.events[0] == UINT16_C(0xffff));
    assert(resolve.events[1] == PRDB_IPC_OP_RESOLVE_PLAN_RELATION);
    assert(resolve.events[2] == PRDB_IPC_OP_RESOLVE_PLAN_RELATION);
    assert(resolve.events[3] == PRDB_SDK_EXECUTOR_OP_RESOLVE_RELATION);
    assert(resolve.events[4] == PRDB_SDK_EXECUTOR_OP_OPEN_SCAN);
    assert(resolve.events[5] == PRDB_SDK_EXECUTOR_OP_CHECK_CANCELLATION);
    assert(resolve.events[6] == PRDB_SDK_EXECUTOR_OP_NEXT_BATCH);
    assert(resolve.events[7] == PRDB_SDK_EXECUTOR_OP_CLOSE_CURSOR);

    uint8_t sql_with_filter[] = "SELECT c3 FROM items WHERE c3 = $1";
    assert(prdb_sdk_sql_parse_select_v2(
               sql_with_filter, sizeof(sql_with_filter) - 1, &statement,
               sizeof(statement), NULL) == PRDB_SDK_SQL_OK);
    uint32_t optimizer_calls = resolve.optimizer_calls;
    assert(prdb_sdk_optimizer_execute_select_ast_v1(
               &host_v4, &statement, (const uint8_t *)"public", 6, 2,
               sizeof(batch_response), request, sizeof(request),
               optimizer_response, sizeof(optimizer_response), response,
               sizeof(response), scan_response, sizeof(scan_response),
               batch_response, sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL,
               &total_rows) == PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED);
    assert(resolve.optimizer_calls == optimizer_calls &&
           resolve.statement_failures == 0);

    assert(prdb_sdk_sql_parse_select_v2(
               sql, sizeof(sql) - 1, &statement, sizeof(statement), NULL) ==
           PRDB_SDK_SQL_OK);
    resolve.optimizer_response[2] = PRDB_SDK_OPTIMIZER_STATUS_INTERNAL;
    resolve.optimizer_response_len = 3;
    assert(prdb_sdk_optimizer_execute_select_ast_v1(
               &host_v4, &statement, (const uint8_t *)"public", 6, 2,
               sizeof(batch_response), request, sizeof(request),
               optimizer_response, sizeof(optimizer_response), response,
               sizeof(response), scan_response, sizeof(scan_response),
               batch_response, sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, fail_projected_row, NULL,
               &total_rows) == PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR);
    assert(resolve.statement_failures == 1);
    resolve.cancellation_response[2] = 1;
    resolve.calls = 0;
    assert(prdb_sdk_optimizer_execute_unfiltered_plan_v1(
               &host, limit_one_plan, payload_len, 2, sizeof(batch_response),
               response, sizeof(response), scan_response, sizeof(scan_response),
               batch_response, sizeof(batch_response), &acquired_relation,
               sizeof(acquired_relation), acquired_columns, 4, source_cells, 4,
               projected_values, 1, on_projected_row, NULL,
               &total_rows) == PRDB_SDK_OPTIMIZER_PLAN_CANCELLED);
    assert(total_rows == 0 && resolve.calls == 5);
    resolve.cancellation_response[2] = 0;

    memcpy(malformed, payload, payload_len);
    malformed[projected_id_offset] = 9;
    assert(prdb_sdk_optimizer_decode_plan_v1(
               malformed, payload_len, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    memcpy(malformed, payload, payload_len);
    malformed[project_node_offset] = 2;
    assert(prdb_sdk_optimizer_decode_plan_v1(
               malformed, payload_len, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED);

    memcpy(malformed, payload, payload_len);
    malformed[payload_len] = 0;
    assert(prdb_sdk_optimizer_decode_plan_v1(
               malformed, payload_len + 1, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    payload_len = build_filter_plan(payload, &predicate_offset);
    assert(prdb_sdk_optimizer_decode_plan_v1(
               payload, payload_len, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED);
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               payload, payload_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(filter.present == 1 && filter.prefix == payload + predicate_offset);
    assert(filter.prefix_len == 32);
    assert(decoded.has_limit == 1 && decoded.limit == 1);

    memcpy(malformed, payload, payload_len);
    malformed[predicate_offset] = 0x10; /* INTEGER is not a Boolean root. */
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               malformed, payload_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    memcpy(malformed, payload, payload_len);
    malformed[predicate_offset + 21] = 0x01; /* Boolean versus INTEGER. */
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               malformed, payload_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    memcpy(malformed, payload, payload_len);
    malformed[predicate_offset] = 0x03;
    malformed[predicate_offset + 1] = 0;
    malformed[predicate_offset + 2] = 1;
    malformed[predicate_offset + 3] = 0xff;
    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               malformed, payload_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter)) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    assert(prdb_sdk_optimizer_decode_plan_with_filter_v1(
               payload, payload_len, &decoded, sizeof(decoded), &filter,
               sizeof(filter) - 1) == PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT);

    payload_len = build_plan(payload, &projected_id_offset,
                             &project_node_offset);
    assert(prdb_sdk_optimizer_decode_plan_v1(
               payload, payload_len, &decoded, sizeof(decoded) - 1) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT);
    assert(prdb_sdk_optimizer_decode_plan_v1(
               payload, payload_len, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    resolve.calls = 0;
    assert(prdb_sdk_optimizer_acquire_plan_relation_v1(
               &host, payload, payload_len, response, sizeof(response),
               &response_len, &acquired_relation, sizeof(acquired_relation),
               acquired_columns, 4) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.calls == 2);
    assert(acquired_relation.relation_id[0] == 2);
    assert(acquired_relation.executor_handle[0] == 6);
    assert(acquired_relation.column_count == 4);
    assert(acquired_columns[3].ordinal == 3 &&
           acquired_columns[3].type_code == 6);
    uint8_t cursor[16] = {0};
    uint32_t scan_opened = 0;
    resolve.calls = 0;
    assert(prdb_sdk_optimizer_open_plan_scan_v1(
               &host, payload, payload_len, response, sizeof(response),
               &response_len, malformed, sizeof(malformed),
               &scan_response_len,
               &acquired_relation, sizeof(acquired_relation), acquired_columns,
               4, cursor, &scan_opened) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.calls == 3 && scan_opened == 1 && cursor[0] == 7);
    uint8_t zero_limit_plan[sizeof(payload)];
    memcpy(zero_limit_plan, payload, payload_len);
    zero_limit_plan[payload_len - 1] = 0;
    resolve.calls = 0;
    assert(prdb_sdk_optimizer_open_plan_scan_v1(
               &host, zero_limit_plan, payload_len, response, sizeof(response),
               &response_len, malformed, sizeof(malformed),
               &scan_response_len,
               &acquired_relation, sizeof(acquired_relation), acquired_columns,
               4, cursor, &scan_opened) == PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(resolve.calls == 2 && scan_opened == 0 && cursor[0] == 0);
    resolve.executor_response[3] = 9;
    assert(prdb_sdk_optimizer_acquire_plan_relation_v1(
               &host, payload, payload_len, response, sizeof(response),
               &response_len, &acquired_relation, sizeof(acquired_relation),
               acquired_columns, 4) ==
           PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    resolve.executor_response[3] = 2;
    return 0;
}
