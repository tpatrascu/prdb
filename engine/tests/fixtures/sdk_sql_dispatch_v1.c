#include "prdb_sdk_sql_dispatch_v1.h"
#include "prdb_sdk_executor_v1.h"
#include "prdb_sdk_planner_v1.h"
#include "prdb_sdk_sql_exec_v8.h"
#include "prdb_sdk_sql_exec_v20.h"

#include <stdio.h>
#include <string.h>

typedef struct dispatch_test_context {
    uint32_t planner_calls;
    uint32_t failure_calls;
    uint32_t group_success;
    uint32_t open_calls;
    uint32_t next_calls;
    uint32_t result_calls;
    uint32_t insert_calls;
    uint32_t result_len;
    uint32_t join_open_calls;
    uint32_t join_next_calls[2];
    uint32_t join_resolve_calls;
    uint32_t join_planner_calls;
    uint8_t planner_reply[256];
    uint32_t planner_reply_len;
    uint8_t right_planner_reply[256];
    uint32_t right_planner_reply_len;
    uint8_t executor_reply[256];
    uint32_t executor_reply_len;
    uint8_t right_executor_reply[256];
    uint32_t right_executor_reply_len;
    uint8_t batch_reply[512];
    uint32_t batch_reply_len;
    uint8_t result_bytes[512];
} dispatch_test_context;

static int32_t join_executor_request(
    void *opaque, uint16_t operation, const uint8_t *request,
    uint64_t request_len, uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len) {
    dispatch_test_context *context = (dispatch_test_context *)opaque;
    static const uint8_t ready[] = {0, 1, 0};
    static const uint8_t finished[] = {0, 1, 1, 0, 0};
    if (context == NULL || response_len == NULL || !context->group_success)
        return PRDB_QUERY_CALLBACK_IO_ERROR;
    if (operation == PRDB_SDK_EXECUTOR_OP_RESOLVE_RELATION) {
        const uint8_t *reply = context->executor_reply;
        uint32_t reply_len = context->executor_reply_len;
        if (context->right_executor_reply_len != 0 &&
            context->join_resolve_calls++ != 0) {
            reply = context->right_executor_reply;
            reply_len = context->right_executor_reply_len;
        }
        if (reply_len > response_capacity)
            return PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL;
        memcpy(response, reply, reply_len);
        *response_len = reply_len;
    } else if (operation == PRDB_SDK_EXECUTOR_OP_CHECK_CANCELLATION) {
        if (request_len != 0 || sizeof(ready) > response_capacity)
            return PRDB_QUERY_CALLBACK_INVALID_ARGUMENT;
        memcpy(response, ready, sizeof(ready));
        *response_len = sizeof(ready);
    } else if (operation == PRDB_SDK_EXECUTOR_OP_OPEN_SCAN) {
        uint8_t opened[18] = {0, 1, 7};
        if (request_len == 0 || sizeof(opened) > response_capacity ||
            context->join_open_calls >= 2)
            return PRDB_QUERY_CALLBACK_INVALID_ARGUMENT;
        memset(opened + 3, context->join_open_calls == 0 ? 0x44 : 0x55, 15);
        ++context->join_open_calls;
        memcpy(response, opened, sizeof(opened));
        *response_len = sizeof(opened);
    } else if (operation == PRDB_SDK_EXECUTOR_OP_NEXT_BATCH) {
        uint32_t scan;
        if (context->join_open_calls == 0 || context->join_open_calls > 2)
            return PRDB_QUERY_CALLBACK_INVALID_ARGUMENT;
        scan = context->join_open_calls - 1;
        if (context->join_next_calls[scan]++ == 0) {
            if (context->batch_reply_len > response_capacity)
                return PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL;
            memcpy(response, context->batch_reply, context->batch_reply_len);
            *response_len = context->batch_reply_len;
        } else {
            if (sizeof(finished) > response_capacity)
                return PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL;
            memcpy(response, finished, sizeof(finished));
            *response_len = sizeof(finished);
        }
    } else if (operation == PRDB_SDK_EXECUTOR_OP_CLOSE_CURSOR) {
        if (sizeof(ready) > response_capacity)
            return PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL;
        memcpy(response, ready, sizeof(ready));
        *response_len = sizeof(ready);
    } else {
        return PRDB_QUERY_CALLBACK_IO_ERROR;
    }
    (void)request;
    return PRDB_QUERY_CALLBACK_OK;
}

static void put_u16(uint8_t *bytes, uint32_t *offset, uint16_t value) {
    bytes[(*offset)++] = (uint8_t)(value >> 8);
    bytes[(*offset)++] = (uint8_t)value;
}

static void put_u32_at(uint8_t *bytes, uint32_t *offset, uint32_t value) {
    bytes[(*offset)++] = (uint8_t)(value >> 24);
    bytes[(*offset)++] = (uint8_t)(value >> 16);
    bytes[(*offset)++] = (uint8_t)(value >> 8);
    bytes[(*offset)++] = (uint8_t)value;
}

static void put_name(uint8_t *bytes, uint32_t *offset, const char *name) {
    uint16_t length = (uint16_t)strlen(name);
    put_u16(bytes, offset, length);
    memcpy(bytes + *offset, name, length);
    *offset += length;
}

static uint32_t make_group_relation(uint8_t *bytes, uint8_t handle_kind,
                                      uint8_t handle_fill, uint8_t score_type) {
    uint32_t offset = 0;
    put_u16(bytes, &offset, 1);
    bytes[offset++] = 0;
    memset(bytes + offset, 0x11, 16); offset += 16;
    bytes[offset++] = handle_kind;
    memset(bytes + offset, handle_fill, 15); offset += 15;
    put_name(bytes, &offset, "widgets");
    put_u16(bytes, &offset, 2);
    for (uint32_t index = 0; index < 2; ++index) {
        memset(bytes + offset, (int)(0x33 + index), 16); offset += 16;
        put_u32_at(bytes, &offset, index);
        bytes[offset++] = 1;
        uint8_t type = index == 0 ? PRDB_SDK_VALUE_TYPE_INTEGER : score_type;
        bytes[offset++] = type;
        bytes[offset++] = type == PRDB_SDK_VALUE_TYPE_DECIMAL ? 10 : 0;
        bytes[offset++] = type == PRDB_SDK_VALUE_TYPE_DECIMAL ? 2 : 0;
        bytes[offset++] = 0;
        bytes[offset++] = 0;
        put_name(bytes, &offset, index == 0 ? "id" : "score");
    }
    return offset;
}

static uint32_t make_integer_relation(uint8_t *bytes, uint8_t handle_kind,
                                      uint8_t handle_fill) {
    return make_group_relation(bytes, handle_kind, handle_fill,
                                  PRDB_SDK_VALUE_TYPE_INTEGER);
}

static uint32_t make_integer_relation_named(uint8_t *bytes,
                                           uint8_t handle_kind,
                                           uint8_t handle_fill,
                                           uint8_t relation_fill,
                                           const char *relation_name) {
    uint32_t offset = 0;
    put_u16(bytes, &offset, 1);
    bytes[offset++] = 0;
    memset(bytes + offset, relation_fill, 16); offset += 16;
    bytes[offset++] = handle_kind;
    memset(bytes + offset, handle_fill, 15); offset += 15;
    put_name(bytes, &offset, relation_name);
    put_u16(bytes, &offset, 2);
    for (uint32_t index = 0; index < 2; ++index) {
        memset(bytes + offset, (int)(0x33 + index), 16); offset += 16;
        put_u32_at(bytes, &offset, index);
        bytes[offset++] = 1;
        bytes[offset++] = PRDB_SDK_VALUE_TYPE_INTEGER;
        bytes[offset++] = 0;
        bytes[offset++] = 0;
        bytes[offset++] = 0;
        bytes[offset++] = 0;
        put_name(bytes, &offset, index == 0 ? "id" : "score");
    }
    return offset;
}

static int32_t join_planner_request(
    void *opaque, uint16_t operation, const uint8_t *request,
    uint64_t request_len, uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len) {
    dispatch_test_context *context = (dispatch_test_context *)opaque;
    const uint8_t *reply;
    uint32_t reply_len;
    (void)request;
    (void)request_len;
    if (context == NULL || response_len == NULL || !context->group_success ||
        operation != PRDB_SDK_PLANNER_OP_DESCRIBE_RELATION)
        return PRDB_QUERY_CALLBACK_IO_ERROR;
    if (context->join_planner_calls++ == 0) {
        reply = context->planner_reply;
        reply_len = context->planner_reply_len;
    } else {
        reply = context->right_planner_reply;
        reply_len = context->right_planner_reply_len;
    }
    ++context->planner_calls;
    if (reply_len > response_capacity)
        return PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL;
    memcpy(response, reply, reply_len);
    *response_len = reply_len;
    return PRDB_QUERY_CALLBACK_OK;
}

static uint32_t make_group_rows_with_score_type(uint8_t *bytes, uint8_t score_type) {
    uint32_t offset = 0;
    put_u16(bytes, &offset, 1);
    bytes[offset++] = 1;
    put_u16(bytes, &offset, 2);
    for (uint32_t row = 0; row < 2; ++row) {
        bytes[offset++] = 8;
        memset(bytes + offset, (int)(row + 1), 15); offset += 15;
        put_u16(bytes, &offset, 2);
        for (uint32_t cell = 0; cell < 2; ++cell) {
            bytes[offset++] = PRDB_SDK_VALUE_STATE_VALUE;
            uint8_t type = cell == 0 ? PRDB_SDK_VALUE_TYPE_INTEGER : score_type;
            bytes[offset++] = type;
            if (type == PRDB_SDK_VALUE_TYPE_DECIMAL) {
                put_u32_at(bytes, &offset, 16);
                memset(bytes + offset, 0, 12);
                offset += 12;
                put_u32_at(bytes, &offset, 300);
            } else {
                put_u32_at(bytes, &offset, 4);
                put_u32_at(bytes, &offset, cell == 0 ? 1 : 3);
            }
        }
    }
    return offset;
}

static uint32_t make_group_rows(uint8_t *bytes) {
    return make_group_rows_with_score_type(bytes, PRDB_SDK_VALUE_TYPE_INTEGER);
}

static int32_t fail_planner_request(
    void *opaque, uint16_t operation, const uint8_t *request,
    uint64_t request_len, uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len) {
    dispatch_test_context *context = (dispatch_test_context *)opaque;
    (void)operation;
    (void)request;
    (void)request_len;
    if (context == NULL || response_len == NULL) return PRDB_QUERY_CALLBACK_INVALID_ARGUMENT;
    ++context->planner_calls;
    if (context->group_success &&
        operation == PRDB_SDK_PLANNER_OP_DESCRIBE_RELATION) {
        if (context->planner_reply_len > response_capacity)
            return PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL;
        memcpy(response, context->planner_reply, context->planner_reply_len);
        *response_len = context->planner_reply_len;
        return PRDB_QUERY_CALLBACK_OK;
    }
    *response_len = 0;
    return PRDB_QUERY_CALLBACK_IO_ERROR;
}

static int32_t unused_executor_request(
    void *opaque, uint16_t operation, const uint8_t *request,
    uint64_t request_len, uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len) {
    dispatch_test_context *context = (dispatch_test_context *)opaque;
    if (context == NULL || response_len == NULL)
        return PRDB_QUERY_CALLBACK_INVALID_ARGUMENT;
    if (!context->group_success) {
        *response_len = 0;
        return PRDB_QUERY_CALLBACK_IO_ERROR;
    }
    if (operation == PRDB_SDK_EXECUTOR_OP_RESOLVE_RELATION) {
        if (context->executor_reply_len > response_capacity)
            return PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL;
        memcpy(response, context->executor_reply, context->executor_reply_len);
        *response_len = context->executor_reply_len;
    } else if (operation == PRDB_SDK_EXECUTOR_OP_STAGE_INSERT) {
        static const uint8_t insert_cells[] = {
            0, 2,
            PRDB_SDK_EXECUTOR_INSERT_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
            0, 0, 0, 4, 0xff, 0xff, 0xff, 0xd6,
            PRDB_SDK_EXECUTOR_INSERT_NULL};
        uint8_t reply[18] = {0, 1, 8};
        if (request_len != 18 + sizeof(insert_cells) ||
            memcmp(request + 18, insert_cells, sizeof(insert_cells)) != 0 ||
            sizeof(reply) > response_capacity)
            return PRDB_QUERY_CALLBACK_INVALID_ARGUMENT;
        memset(reply + 3, 0x44, 15);
        memcpy(response, reply, sizeof(reply));
        *response_len = sizeof(reply);
        ++context->insert_calls;
    } else if (operation == PRDB_SDK_EXECUTOR_OP_CHECK_CANCELLATION) {
        static const uint8_t reply[] = {0, 1, 0};
        if (request_len != 0 || sizeof(reply) > response_capacity)
            return PRDB_QUERY_CALLBACK_INVALID_ARGUMENT;
        memcpy(response, reply, sizeof(reply));
        *response_len = sizeof(reply);
    } else if (operation == PRDB_SDK_EXECUTOR_OP_OPEN_SCAN) {
        ++context->open_calls;
        uint8_t reply[18] = {0, 1, 7};
        memset(reply + 3, 0x44, 15);
        if (request_len == 0 || sizeof(reply) > response_capacity)
            return PRDB_QUERY_CALLBACK_INVALID_ARGUMENT;
        memcpy(response, reply, sizeof(reply));
        *response_len = sizeof(reply);
    } else if (operation == PRDB_SDK_EXECUTOR_OP_NEXT_BATCH) {
        if (context->next_calls++ == 0) {
            if (context->batch_reply_len > response_capacity)
                return PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL;
            memcpy(response, context->batch_reply, context->batch_reply_len);
            *response_len = context->batch_reply_len;
        } else {
            static const uint8_t finished[] = {0, 1, 1, 0, 0};
            if (sizeof(finished) > response_capacity)
                return PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL;
            memcpy(response, finished, sizeof(finished));
            *response_len = sizeof(finished);
        }
    } else if (operation == PRDB_SDK_EXECUTOR_OP_CLOSE_CURSOR) {
        static const uint8_t reply[] = {0, 1, 0};
        if (sizeof(reply) > response_capacity)
            return PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL;
        memcpy(response, reply, sizeof(reply));
        *response_len = sizeof(reply);
    } else {
        return PRDB_QUERY_CALLBACK_IO_ERROR;
    }
    (void)request;
    return PRDB_QUERY_CALLBACK_OK;
}

static int32_t discard_result(void *opaque, const uint8_t *bytes,
                              uint64_t bytes_len) {
    dispatch_test_context *context = (dispatch_test_context *)opaque;
    if (context != NULL && context->group_success) {
        if (bytes_len > sizeof(context->result_bytes) - context->result_len)
            return PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL;
        memcpy(context->result_bytes + context->result_len, bytes,
               (size_t)bytes_len);
        context->result_len += (uint32_t)bytes_len;
        ++context->result_calls;
    }
    return PRDB_QUERY_CALLBACK_OK;
}

static int32_t count_statement_failure(void *opaque, uint32_t reason) {
    dispatch_test_context *context = (dispatch_test_context *)opaque;
    (void)reason;
    if (context == NULL) return PRDB_QUERY_CALLBACK_INVALID_ARGUMENT;
    ++context->failure_calls;
    return PRDB_QUERY_CALLBACK_OK;
}

static int test_statement_dispatch_routes_supported_dml(void) {
    static const char *statements[] = {
        "INSERT INTO t VALUES ($1)",
        "UPDATE t SET x = NULL WHERE id = $1",
        "DELETE FROM t WHERE id = $1",
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING COUNT(*) >= 2 AND SUM(score) >= 4.50",
    };
    dispatch_test_context context = {0};
    prdb_host_api_v3 host = {0};
    host.struct_size = sizeof(host);
    host.abi_version = PRDB_QUERY_ABI_V3_VERSION;
    host.context = &context;
    host.executor_request = unused_executor_request;
    host.planner_request = fail_planner_request;
    host.write_result = discard_result;
    host.statement_failure = count_statement_failure;
    for (uint32_t index = 0; index < sizeof(statements) / sizeof(statements[0]);
         ++index) {
        const char *sql = statements[index];
        int32_t status = prdb_sdk_sql_run_statement_v1(
            &host, (const uint8_t *)"public", 6, (const uint8_t *)sql,
            strlen(sql), NULL, 0);
        if (status != PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR ||
            context.planner_calls != index + 1 ||
            context.failure_calls != index + 1) {
            return 1;
        }
    }
    return 0;
}

static uint32_t read_u32(const uint8_t *bytes) {
    return ((uint32_t)bytes[0] << 24) | ((uint32_t)bytes[1] << 16) |
           ((uint32_t)bytes[2] << 8) | bytes[3];
}

static int test_dispatch_insert_normalizes_signed_parameter(void) {
    static const uint8_t sql[] = "INSERT INTO widgets VALUES ($1, NULL)";
    static const uint8_t parameters[] = {
        'P', 'S', 'P', '1', 0, 1, 0, 0,
        0, 0, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0, 0,
        0, 0, 0, 4, 0xd6, 0xff, 0xff, 0xff};
    dispatch_test_context context = {0};
    prdb_host_api_v3 host = {
        sizeof(host), PRDB_QUERY_ABI_V3_VERSION, &context,
        unused_executor_request, fail_planner_request, discard_result,
        count_statement_failure};
    context.group_success = 1;
    context.planner_reply_len = make_integer_relation(
        context.planner_reply, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x22);
    context.executor_reply_len = make_integer_relation(
        context.executor_reply, 6, 0x22);
    return prdb_sdk_sql_run_statement_v1(
               &host, (const uint8_t *)"app", 3, sql, sizeof(sql) - 1,
               parameters, sizeof(parameters)) != PRDB_SDK_SQL_DISPATCH_OK ||
           context.insert_calls != 1 || context.failure_calls != 0;
}

static int test_dispatch_grouped_select_emits_sqlr(void) {
    static const uint8_t sql[] =
        "SELECT id, score, SUM(score) FROM widgets GROUP BY id, score "
        "HAVING COUNT(*) >= 2 AND SUM(score) >= 4";
    dispatch_test_context context = {0};
    prdb_host_api_v3 host = {0};
    uint32_t offset = 0;
    uint16_t name_len;

    context.group_success = 1;
    context.planner_reply_len = make_integer_relation(
        context.planner_reply, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x22);
    context.executor_reply_len = make_integer_relation(
        context.executor_reply, 6, 0x22);
    context.batch_reply_len = make_group_rows(context.batch_reply);
    host.struct_size = sizeof(host);
    host.abi_version = PRDB_QUERY_ABI_V3_VERSION;
    host.context = &context;
    host.executor_request = unused_executor_request;
    host.planner_request = fail_planner_request;
    host.write_result = discard_result;
    host.statement_failure = count_statement_failure;
    if (prdb_sdk_sql_run_statement_v1(
            &host, (const uint8_t *)"app", 3, sql, sizeof(sql) - 1,
            NULL, 0) != PRDB_SDK_SQL_DISPATCH_OK ||
        context.planner_calls != 1 || context.failure_calls != 0 ||
        context.result_calls != 1 || context.result_len < 8 ||
        memcmp(context.result_bytes, "SQLR", 4) != 0) return 1;
    offset = 4;
    if (context.result_bytes[offset++] != 0 ||
        context.result_bytes[offset++] != 1 ||
        context.result_bytes[offset++] != 0 ||
        context.result_bytes[offset++] != 3) return 1;
    name_len = (uint16_t)(((uint16_t)context.result_bytes[offset] << 8) |
                         context.result_bytes[offset + 1]);
    offset += 2;
    if (name_len != 2 || memcmp(context.result_bytes + offset, "id", 2) != 0)
        return 1;
    offset += name_len;
    if (read_u32(context.result_bytes + offset) !=
            PRDB_SDK_VALUE_TYPE_INTEGER) return 1;
    offset += 6;
    if (context.result_bytes[offset++] != PRDB_SDK_VALUE_STATE_VALUE ||
        read_u32(context.result_bytes + offset) != 4) return 1;
    offset += 4;
    if (read_u32(context.result_bytes + offset) != 1) return 1;
    offset += 4;

    name_len = (uint16_t)(((uint16_t)context.result_bytes[offset] << 8) |
                         context.result_bytes[offset + 1]);
    offset += 2;
    if (name_len != 5 || memcmp(context.result_bytes + offset, "score", 5) != 0)
        return 1;
    offset += name_len + 6;
    if (context.result_bytes[offset++] != PRDB_SDK_VALUE_STATE_VALUE ||
        read_u32(context.result_bytes + offset) != 4) return 1;
    offset += 4;
    if (read_u32(context.result_bytes + offset) != 3) return 1;
    offset += 4;

    name_len = (uint16_t)(((uint16_t)context.result_bytes[offset] << 8) |
                         context.result_bytes[offset + 1]);
    offset += 2;
    if (name_len != 3 || memcmp(context.result_bytes + offset, "sum", 3) != 0)
        return 1;
    offset += name_len;
    if (read_u32(context.result_bytes + offset) != PRDB_SDK_VALUE_TYPE_BIGINT)
        return 1;
    offset += 6;
    if (context.result_bytes[offset++] != PRDB_SDK_VALUE_STATE_VALUE ||
        read_u32(context.result_bytes + offset) != 8) return 1;
    offset += 4;
    if (read_u32(context.result_bytes + offset + 4) != 6) return 1;
    offset += 8;
    if (offset != context.result_len) return 1;
    return 0;
}

static int test_dispatch_single_key_grouped_select_emits_sqlr(
    const uint8_t *sql, uint64_t sql_len) {
    dispatch_test_context context = {0};
    prdb_host_api_v3 host = {0};
    uint32_t offset = 0;
    uint16_t name_len;

    context.group_success = 1;
    context.planner_reply_len = make_integer_relation(
        context.planner_reply, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x22);
    context.executor_reply_len = make_integer_relation(
        context.executor_reply, 6, 0x22);
    context.batch_reply_len = make_group_rows(context.batch_reply);
    host.struct_size = sizeof(host);
    host.abi_version = PRDB_QUERY_ABI_V3_VERSION;
    host.context = &context;
    host.executor_request = unused_executor_request;
    host.planner_request = fail_planner_request;
    host.write_result = discard_result;
    host.statement_failure = count_statement_failure;
    if (prdb_sdk_sql_run_statement_v1(
            &host, (const uint8_t *)"app", 3, sql, sql_len,
            NULL, 0) != PRDB_SDK_SQL_DISPATCH_OK ||
        context.planner_calls != 1 || context.failure_calls != 0 ||
        context.result_calls != 1 || context.result_len < 8 ||
        memcmp(context.result_bytes, "SQLR", 4) != 0) return 1;
    offset = 4;
    if (context.result_bytes[offset++] != 0 ||
        context.result_bytes[offset++] != 1 ||
        context.result_bytes[offset++] != 0 ||
        context.result_bytes[offset++] != 2) return 1;
    name_len = (uint16_t)(((uint16_t)context.result_bytes[offset] << 8) |
                         context.result_bytes[offset + 1]);
    offset += 2;
    if (name_len != 2 || memcmp(context.result_bytes + offset, "id", 2) != 0)
        return 1;
    offset += name_len;
    if (read_u32(context.result_bytes + offset) !=
            PRDB_SDK_VALUE_TYPE_INTEGER) return 1;
    offset += 6;
    if (context.result_bytes[offset++] != PRDB_SDK_VALUE_STATE_VALUE ||
        read_u32(context.result_bytes + offset) != 4) return 1;
    offset += 4;
    if (read_u32(context.result_bytes + offset) != 1) return 1;
    offset += 4;
    name_len = (uint16_t)(((uint16_t)context.result_bytes[offset] << 8) |
                         context.result_bytes[offset + 1]);
    offset += 2;
    if (name_len != 3 || memcmp(context.result_bytes + offset, "sum", 3) != 0)
        return 1;
    offset += name_len;
    if (read_u32(context.result_bytes + offset) != PRDB_SDK_VALUE_TYPE_BIGINT)
        return 1;
    offset += 6;
    if (context.result_bytes[offset++] != PRDB_SDK_VALUE_STATE_VALUE ||
        read_u32(context.result_bytes + offset) != 8) return 1;
    offset += 4;
    if (read_u32(context.result_bytes + offset + 4) != 6) return 1;
    offset += 8;
    if (offset != context.result_len) return 1;
    return 0;
}

static int test_grouped_parameter_metadata(void) {
    static const char where_sql[] =
        "SELECT id, SUM(score) FROM widgets WHERE score >= $1 "
        "GROUP BY id LIMIT 0";
    static const char having_sql[] =
        "SELECT id, SUM(score) FROM widgets GROUP BY id "
        "HAVING SUM(score) >= $1";
    static const char mixed_sql[] =
        "SELECT id, SUM(score) FROM widgets WHERE score >= $1 "
        "GROUP BY id HAVING SUM(score) >= $1";
    static const char different_operand_scales_sql[] =
        "SELECT id, AVG(score) FROM widgets WHERE score >= $1 "
        "GROUP BY id HAVING AVG(score) >= $1";
    static const char hidden_count_sql[] =
        "SELECT id, SUM(score) FROM widgets GROUP BY id "
        "HAVING SUM(score) IS NOT NULL OR COUNT(*) > $1";
    static const struct {
        const char *sql;
        uint8_t type, scale, is_null;
        int accepted;
    } cases[] = {
        {where_sql, PRDB_SDK_VALUE_TYPE_DECIMAL, 1, 0, 0},
        {where_sql, PRDB_SDK_VALUE_TYPE_DECIMAL, 2, 0, 1},
        {having_sql, PRDB_SDK_VALUE_TYPE_DECIMAL, 1, 1, 0},
        {having_sql, PRDB_SDK_VALUE_TYPE_DECIMAL, 2, 1, 1},
        {mixed_sql, PRDB_SDK_VALUE_TYPE_DECIMAL, 1, 0, 0},
        {mixed_sql, PRDB_SDK_VALUE_TYPE_DECIMAL, 2, 0, 1},
        {different_operand_scales_sql, PRDB_SDK_VALUE_TYPE_DECIMAL, 2, 0, 0},
        {hidden_count_sql, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 1, 0},
        {hidden_count_sql, PRDB_SDK_VALUE_TYPE_BIGINT, 0, 1, 1},
    };
    for (uint32_t index = 0; index < sizeof(cases) / sizeof(cases[0]); ++index) {
        uint8_t parameters[] = {
            'P', 'S', 'P', '1', 0, 1, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        dispatch_test_context context = {0};
        prdb_host_api_v3 host = {
            sizeof(host), PRDB_QUERY_ABI_V3_VERSION, &context,
            unused_executor_request, fail_planner_request, discard_result,
            count_statement_failure};
        parameters[11] = cases[index].type;
        parameters[12] = cases[index].type == PRDB_SDK_VALUE_TYPE_DECIMAL ? 4 : 0;
        parameters[13] = cases[index].scale;
        parameters[14] = cases[index].is_null;
        parameters[19] = cases[index].is_null ? 0 : 16;
        context.group_success = 1;
        context.planner_reply_len = make_group_relation(
            context.planner_reply, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x22,
            PRDB_SDK_VALUE_TYPE_DECIMAL);
        context.executor_reply_len = make_group_relation(
            context.executor_reply, 6, 0x22, PRDB_SDK_VALUE_TYPE_DECIMAL);
        context.batch_reply_len = make_group_rows_with_score_type(
            context.batch_reply, PRDB_SDK_VALUE_TYPE_DECIMAL);
        int32_t status = prdb_sdk_sql_run_statement_v1(
            &host, (const uint8_t *)"app", 3,
            (const uint8_t *)cases[index].sql, strlen(cases[index].sql),
            parameters, 20 + parameters[19]);
        uint32_t scans = cases[index].accepted && cases[index].sql != where_sql;
        uint32_t rows = scans && cases[index].sql != having_sql;
        if (status != (cases[index].accepted ? PRDB_SDK_SQL_DISPATCH_OK
                                            : PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR) ||
            context.failure_calls != (uint32_t)!cases[index].accepted ||
            context.planner_calls != 1 || context.open_calls != scans ||
            context.result_calls != rows) {
            fprintf(stderr, "grouped metadata case %u returned %d, opened %u scans, emitted %u rows\n",
                    index, status, context.open_calls, context.result_calls);
            return 1;
        }
    }
    return 0;
}

static int test_dispatch_filtered_join_emits_sqlr(void) {
    static const uint8_t sql[] =
        "SELECT l.id, r.score FROM widgets AS l INNER JOIN widgets AS r "
        "ON l.id = r.id AND l.score = r.score "
        "WHERE l.score >= $1 LIMIT 1";
    static const uint8_t parameters[] = {
        'P', 'S', 'P', '1', 0, 1, 0, 0,
        0, 0, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0, 0, 0,
        0, 0, 0, 4, 3, 0, 0, 0
    };
    dispatch_test_context context = {0};
    prdb_host_api_v3 host = {0};
    int32_t status;
    uint32_t offset = 0;
    uint16_t name_len;

    context.group_success = 1;
    context.planner_reply_len = make_integer_relation(
        context.planner_reply, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x22);
    context.executor_reply_len = make_integer_relation(
        context.executor_reply, 6, 0x22);
    context.batch_reply_len = make_group_rows(context.batch_reply);
    host.struct_size = sizeof(host);
    host.abi_version = PRDB_QUERY_ABI_V3_VERSION;
    host.context = &context;
    host.executor_request = join_executor_request;
    host.planner_request = fail_planner_request;
    host.write_result = discard_result;
    host.statement_failure = count_statement_failure;
    status = prdb_sdk_sql_run_statement_v1(
            &host, (const uint8_t *)"app", 3, sql, sizeof(sql) - 1,
            parameters, sizeof(parameters));
    if (status != PRDB_SDK_SQL_DISPATCH_OK ||
        context.planner_calls != 4 || context.failure_calls != 0 ||
        context.join_open_calls != 2 || context.result_calls != 1 ||
        context.result_len < 8 || memcmp(context.result_bytes, "SQLR", 4) != 0) {
        fprintf(stderr, "join dispatch status=%d planner=%u failed=%u open=%u results=%u len=%u\n",
                status, context.planner_calls, context.failure_calls,
                context.join_open_calls, context.result_calls,
                context.result_len);
        return 1;
    }
    if (context.join_next_calls[0] != 1 || context.join_next_calls[1] != 1)
        return 1;
    offset = 4;
    if (context.result_bytes[offset++] != 0 ||
        context.result_bytes[offset++] != 1 ||
        context.result_bytes[offset++] != 0 ||
        context.result_bytes[offset++] != 2) return 1;
    name_len = (uint16_t)(((uint16_t)context.result_bytes[offset] << 8) |
                         context.result_bytes[offset + 1]);
    offset += 2;
    if (name_len != 2 || memcmp(context.result_bytes + offset, "id", 2) != 0)
        return 1;
    offset += name_len;
    if (read_u32(context.result_bytes + offset) !=
        PRDB_SDK_VALUE_TYPE_INTEGER) return 1;
    offset += 6;
    if (context.result_bytes[offset++] != PRDB_SDK_VALUE_STATE_VALUE ||
        read_u32(context.result_bytes + offset) != 4) return 1;
    offset += 4;
    if (read_u32(context.result_bytes + offset) != 1) return 1;
    offset += 4;
    name_len = (uint16_t)(((uint16_t)context.result_bytes[offset] << 8) |
                         context.result_bytes[offset + 1]);
    offset += 2;
    if (name_len != 5 ||
        memcmp(context.result_bytes + offset, "score", 5) != 0) return 1;
    offset += name_len;
    if (read_u32(context.result_bytes + offset) !=
        PRDB_SDK_VALUE_TYPE_INTEGER) return 1;
    offset += 6;
    if (context.result_bytes[offset++] != PRDB_SDK_VALUE_STATE_VALUE ||
        read_u32(context.result_bytes + offset) != 4) return 1;
    offset += 4;
    if (read_u32(context.result_bytes + offset) != 3) return 1;
    offset += 4;
    if (offset != context.result_len) return 1;
    return 0;
}

static int test_dispatch_single_key_join_emits_sqlr(void) {
    static const uint8_t sql[] =
        "SELECT lefts.id, rights.score FROM lefts INNER JOIN rights "
        "ON lefts.id = rights.id LIMIT 1";
    dispatch_test_context context = {0};
    prdb_host_api_v3 host = {0};
    uint32_t offset = 0;
    uint16_t name_len;

    context.group_success = 1;
    context.planner_reply_len = make_integer_relation_named(
        context.planner_reply, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND, 0x22,
        0x21, "lefts");
    context.right_planner_reply_len = make_integer_relation_named(
        context.right_planner_reply, PRDB_SDK_PLANNER_TABLE_HANDLE_KIND,
        0x23, 0x22, "rights");
    context.executor_reply_len = make_integer_relation_named(
        context.executor_reply, 6, 0x22, 0x21, "lefts");
    context.right_executor_reply_len = make_integer_relation_named(
        context.right_executor_reply, 6, 0x23, 0x22, "rights");
    context.batch_reply_len = make_group_rows(context.batch_reply);
    host.struct_size = sizeof(host);
    host.abi_version = PRDB_QUERY_ABI_V3_VERSION;
    host.context = &context;
    host.executor_request = join_executor_request;
    host.planner_request = join_planner_request;
    host.write_result = discard_result;
    host.statement_failure = count_statement_failure;
    if (prdb_sdk_sql_run_statement_v1(
            &host, (const uint8_t *)"app", 3, sql, sizeof(sql) - 1,
            NULL, 0) != PRDB_SDK_SQL_DISPATCH_OK ||
        context.planner_calls != 2 || context.failure_calls != 0 ||
        context.join_resolve_calls != 2 || context.join_open_calls != 2 ||
        context.result_calls != 1 || context.result_len < 8 ||
        memcmp(context.result_bytes, "SQLR", 4) != 0)
        return 1;
    offset = 4;
    if (context.result_bytes[offset++] != 0 ||
        context.result_bytes[offset++] != 1 ||
        context.result_bytes[offset++] != 0 ||
        context.result_bytes[offset++] != 2) return 1;
    name_len = (uint16_t)(((uint16_t)context.result_bytes[offset] << 8) |
                         context.result_bytes[offset + 1]);
    offset += 2;
    if (name_len != 2 || memcmp(context.result_bytes + offset, "id", 2) != 0)
        return 1;
    offset += name_len + 6;
    if (context.result_bytes[offset++] != PRDB_SDK_VALUE_STATE_VALUE ||
        read_u32(context.result_bytes + offset) != 4) return 1;
    offset += 4;
    if (read_u32(context.result_bytes + offset) != 1) return 1;
    offset += 4;
    name_len = (uint16_t)(((uint16_t)context.result_bytes[offset] << 8) |
                         context.result_bytes[offset + 1]);
    offset += 2;
    if (name_len != 5 ||
        memcmp(context.result_bytes + offset, "score", 5) != 0) return 1;
    offset += name_len + 6;
    if (context.result_bytes[offset++] != PRDB_SDK_VALUE_STATE_VALUE ||
        read_u32(context.result_bytes + offset) != 4) return 1;
    offset += 4;
    if (read_u32(context.result_bytes + offset) != 3) return 1;
    offset += 4;
    return offset == context.result_len ? 0 : 1;
}

static void put_u32(uint8_t *out, uint32_t value) {
    out[0] = (uint8_t)(value >> 24);
    out[1] = (uint8_t)(value >> 16);
    out[2] = (uint8_t)(value >> 8);
    out[3] = (uint8_t)value;
}

static int test_typed_values_and_null(void) {
    uint8_t bytes[] = {
        'P','S','P','1', 0,2, 0,0,
        0,0,0,3, 0,0, 0,0, 0,0,0,4, 42,0,0,0,
        0,0,0,5, 8,2, 1,0, 0,0,0,0
    };
    prdb_sdk_value_v1 values[2] = {0};
    prdb_sdk_sql_parameter_metadata_v1 metadata[2] = {0};
    uint32_t count = 99;
    int32_t result = prdb_sdk_sql_decode_parameters_v1(
        bytes, sizeof(bytes), values, 2, metadata, 2, &count);
    if (result != PRDB_SDK_SQL_DISPATCH_OK || count != 2 ||
        values[0].type_code != PRDB_SDK_VALUE_TYPE_INTEGER ||
        values[0].data != bytes + 20 || values[0].data_len != 4 ||
        metadata[0].type_code != PRDB_SDK_VALUE_TYPE_INTEGER ||
        values[1].state != PRDB_SDK_VALUE_STATE_NULL ||
        values[1].type_code != 0 || values[1].data != NULL ||
        metadata[1].type_code != PRDB_SDK_VALUE_TYPE_DECIMAL ||
        metadata[1].decimal_precision != 8 || metadata[1].decimal_scale != 2) {
        return 1;
    }
    return 0;
}

static int test_rejects_malformed_blocks(void) {
    uint8_t bytes[] = {
        'P','S','P','1', 0,1, 0,0,
        0,0,0,3, 0,0, 0,0, 0,0,0,4, 1,0,0,0
    };
    prdb_sdk_value_v1 values[1] = {0};
    prdb_sdk_sql_parameter_metadata_v1 metadata[1] = {0};
    uint32_t count = 99;
    if (prdb_sdk_sql_decode_parameters_v1(bytes, sizeof(bytes) - 1,
            values, 1, metadata, 1, &count) !=
            PRDB_SDK_SQL_DISPATCH_INVALID_PARAMETERS || count != 0) return 1;
    bytes[15] = 1;
    if (prdb_sdk_sql_decode_parameters_v1(bytes, sizeof(bytes),
            values, 1, metadata, 1, &count) !=
            PRDB_SDK_SQL_DISPATCH_INVALID_PARAMETERS) return 1;
    bytes[15] = 0;
    put_u32(bytes + 8, PRDB_SDK_VALUE_TYPE_DECIMAL);
    bytes[12] = 39;
    if (prdb_sdk_sql_decode_parameters_v1(bytes, sizeof(bytes),
            values, 1, metadata, 1, &count) !=
            PRDB_SDK_SQL_DISPATCH_INVALID_PARAMETERS) return 1;
    return 0;
}

int main(void) {
    if (prdb_sdk_sql_run_statement_v1(NULL, NULL, 0, NULL, 0, NULL, 0) !=
        PRDB_SDK_SQL_DISPATCH_INVALID_ARGUMENT) {
        fputs("interactive statement dispatcher accepted an invalid host\n", stderr);
        return 1;
    }
    if (test_statement_dispatch_routes_supported_dml() != 0) {
        fputs("interactive statement dispatcher did not route supported DML\n", stderr);
        return 1;
    }
    if (test_dispatch_insert_normalizes_signed_parameter() != 0) {
        fputs("INSERT dispatcher did not normalize signed PSP1 input\n", stderr);
        return 1;
    }
    if (test_dispatch_grouped_select_emits_sqlr() != 0) {
        fputs("grouped SELECT dispatch did not emit expected SQLR\n", stderr);
        return 1;
    }
    static const uint8_t single_key_sql[] =
        "SELECT id, SUM(score) FROM widgets GROUP BY id";
    static const uint8_t single_key_having_sql[] =
        "SELECT id, SUM(score) FROM widgets GROUP BY id "
        "HAVING SUM(score) IS NOT NULL AND (COUNT(*) >= 2 OR SUM(score) > 10) "
        "ORDER BY SUM(score) DESC NULLS LAST, id ASC LIMIT 1";
    if (test_dispatch_single_key_grouped_select_emits_sqlr(
            single_key_sql, sizeof(single_key_sql) - 1) != 0 ||
        test_dispatch_single_key_grouped_select_emits_sqlr(
            single_key_having_sql, sizeof(single_key_having_sql) - 1) != 0) {
        fputs("single-key grouped SELECT dispatch did not emit expected SQLR\n", stderr);
        return 1;
    }
    if (test_grouped_parameter_metadata() != 0) return 1;
    if (test_dispatch_filtered_join_emits_sqlr() != 0) {
        fputs("filtered JOIN dispatch did not emit expected SQLR\n", stderr);
        return 1;
    }
    if (test_dispatch_single_key_join_emits_sqlr() != 0) {
        fputs("single-key JOIN dispatch did not emit expected SQLR\n", stderr);
        return 1;
    }
    if (test_typed_values_and_null() != 0) {
        fputs("typed PSP1 decode or NULL metadata failed\n", stderr);
        return 1;
    }
    if (test_rejects_malformed_blocks() != 0) {
        fputs("malformed PSP1 block was accepted\n", stderr);
        return 1;
    }
    return 0;
}
