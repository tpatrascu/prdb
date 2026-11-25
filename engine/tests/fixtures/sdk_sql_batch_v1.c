#include "prdb_sdk_sql_batch_v1.h"

#include <stdio.h>
#include <string.h>

typedef struct capture {
    uint8_t bytes[8192];
    uint64_t length;
    uint32_t write_calls;
    uint32_t fail_write_call;
    uint32_t failure_calls;
} capture;

static uint32_t runner_calls;

static int32_t capture_write(void *opaque, const uint8_t *bytes,
                             uint64_t bytes_len) {
    capture *state = (capture *)opaque;
    if (state == NULL || (bytes_len != 0 && bytes == NULL)) return 1;
    if (state->write_calls++ == state->fail_write_call) return 1;
    if (bytes_len > sizeof(state->bytes) - state->length) return 1;
    memcpy(state->bytes + state->length, bytes, (size_t)bytes_len);
    state->length += bytes_len;
    return 0;
}

static int32_t capture_failure(void *opaque, uint32_t reason) {
    capture *state = (capture *)opaque;
    if (state == NULL || reason != PRDB_QUERY_STATEMENT_FAILURE_SQL) return 1;
    state->failure_calls += 1;
    return 0;
}

static int32_t emit_statement(const prdb_host_api_v1 *host,
                              const uint8_t *schema, uint32_t schema_len,
                              const prdb_sdk_sql_batch_statement_v1 *statement) {
    if (schema_len != 3 || memcmp(schema, "app", 3) != 0) return 11;
    runner_calls += 1;
    if (statement->sql_len == 4 && memcmp(statement->sql, "fail", 4) == 0) {
        return 42;
    }
    return host->write_result(host->context, statement->sql, 1);
}

static uint16_t read_u16(const uint8_t *bytes) {
    return (uint16_t)(((uint16_t)bytes[0] << 8) | bytes[1]);
}

static uint32_t read_u32(const uint8_t *bytes) {
    return ((uint32_t)bytes[0] << 24) | ((uint32_t)bytes[1] << 16) |
           ((uint32_t)bytes[2] << 8) | bytes[3];
}

static int expect_event(const capture *state, uint64_t *offset,
                        uint8_t event, uint16_t ordinal,
                        const uint8_t *payload, uint32_t payload_len) {
    uint32_t encoded_len;
    if (state->length - *offset < PRDB_SDK_SQL_BATCH_RESULT_HEADER_BYTES) return 0;
    const uint8_t *header = state->bytes + *offset;
    if (memcmp(header, "SQLB", 4) != 0 || header[4] != 1 ||
        header[5] != event || read_u16(header + 6) != ordinal) return 0;
    encoded_len = read_u32(header + 8);
    if (encoded_len != payload_len ||
        state->length - *offset - PRDB_SDK_SQL_BATCH_RESULT_HEADER_BYTES < encoded_len) return 0;
    *offset += PRDB_SDK_SQL_BATCH_RESULT_HEADER_BYTES;
    if (encoded_len != 0 && memcmp(state->bytes + *offset, payload, encoded_len) != 0) return 0;
    *offset += encoded_len;
    return 1;
}

static int test_ordered_result_framing(void) {
    capture state = {{0}, 0, 0, UINT32_MAX, 0};
    const uint8_t schema[] = "app";
    const uint8_t sql1[] = "a";
    const uint8_t sql2[] = "b";
    prdb_sdk_sql_batch_statement_v1 statements[] = {
        {sql1, 1, NULL, 0}, {sql2, 1, NULL, 0}};
    prdb_host_api_v1 host = {
        sizeof(host), PRDB_QUERY_ABI_VERSION, &state, NULL, NULL,
        capture_write, capture_failure};
    uint64_t offset = 0;
    runner_calls = 0;
    if (prdb_sdk_sql_run_batch_v1(&host, schema, 3, 2, statements,
                                  emit_statement) != PRDB_SDK_SQL_BATCH_OK ||
        runner_calls != 2 || state.failure_calls != 0) return 1;
    if (!expect_event(&state, &offset, PRDB_SDK_SQL_BATCH_EVENT_START, 0, NULL, 0) ||
        !expect_event(&state, &offset, PRDB_SDK_SQL_BATCH_EVENT_DATA, 0, sql1, 1) ||
        !expect_event(&state, &offset, PRDB_SDK_SQL_BATCH_EVENT_COMPLETE, 0, NULL, 0) ||
        !expect_event(&state, &offset, PRDB_SDK_SQL_BATCH_EVENT_START, 1, NULL, 0) ||
        !expect_event(&state, &offset, PRDB_SDK_SQL_BATCH_EVENT_DATA, 1, sql2, 1) ||
        !expect_event(&state, &offset, PRDB_SDK_SQL_BATCH_EVENT_COMPLETE, 1, NULL, 0) ||
        offset != state.length) return 1;
    return 0;
}

static int test_statement_failure_stops_package(void) {
    capture state = {{0}, 0, 0, UINT32_MAX, 0};
    const uint8_t schema[] = "app";
    const uint8_t sql1[] = "a";
    const uint8_t sql2[] = "fail";
    const uint8_t sql3[] = "c";
    prdb_sdk_sql_batch_statement_v1 statements[] = {
        {sql1, 1, NULL, 0}, {sql2, 4, NULL, 0}, {sql3, 1, NULL, 0}};
    prdb_host_api_v1 host = {
        sizeof(host), PRDB_QUERY_ABI_VERSION, &state, NULL, NULL,
        capture_write, capture_failure};
    runner_calls = 0;
    if (prdb_sdk_sql_run_batch_v1(&host, schema, 3, 3, statements,
                                  emit_statement) !=
            PRDB_SDK_SQL_BATCH_STATEMENT_FAILED ||
        runner_calls != 2 || state.failure_calls != 1) return 1;
    return 0;
}

static int test_output_failure_latches_and_stops(void) {
    capture state = {{0}, 0, 0, 2, 0};
    const uint8_t schema[] = "app";
    const uint8_t sql1[] = "a";
    const uint8_t sql2[] = "b";
    prdb_sdk_sql_batch_statement_v1 statements[] = {
        {sql1, 1, NULL, 0}, {sql2, 1, NULL, 0}};
    prdb_host_api_v1 host = {
        sizeof(host), PRDB_QUERY_ABI_VERSION, &state, NULL, NULL,
        capture_write, capture_failure};
    runner_calls = 0;
    if (prdb_sdk_sql_run_batch_v1(&host, schema, 3, 2, statements,
                                  emit_statement) !=
            PRDB_SDK_SQL_BATCH_STATEMENT_FAILED ||
        runner_calls != 1 || state.failure_calls != 1) return 1;
    return 0;
}

static int test_rejects_over_limit_count(void) {
    capture state = {{0}, 0, 0, UINT32_MAX, 0};
    const uint8_t schema[] = "app";
    prdb_host_api_v1 host = {
        sizeof(host), PRDB_QUERY_ABI_VERSION, &state, NULL, NULL,
        capture_write, capture_failure};
    if (prdb_sdk_sql_run_batch_v1(&host, schema, 3,
            PRDB_SDK_SQL_BATCH_MAX_STATEMENTS + 1, NULL, emit_statement) !=
        PRDB_SDK_SQL_BATCH_LIMIT_EXCEEDED) return 1;
    return state.failure_calls == 0 ? 0 : 1;
}

int main(void) {
    if (test_ordered_result_framing() != 0) {
        fputs("ordered SQL batch framing failed\n", stderr);
        return 1;
    }
    if (test_statement_failure_stops_package() != 0) {
        fputs("SQL batch failure did not stop and latch\n", stderr);
        return 1;
    }
    if (test_output_failure_latches_and_stops() != 0) {
        fputs("SQL batch output failure did not stop and latch\n", stderr);
        return 1;
    }
    if (test_rejects_over_limit_count() != 0) {
        fputs("SQL batch accepted an over-limit statement count\n", stderr);
        return 1;
    }
    return 0;
}
