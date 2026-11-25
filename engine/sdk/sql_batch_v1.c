#include "prdb_sdk_sql_batch_v1.h"

#include <string.h>

typedef struct batch_host_context {
    const prdb_host_api_v1 *base;
    uint16_t statement_ordinal;
    int failure_latched;
} batch_host_context;

static void write_u16_be(uint8_t *bytes, uint16_t value) {
    bytes[0] = (uint8_t)(value >> 8);
    bytes[1] = (uint8_t)value;
}

static void write_u32_be(uint8_t *bytes, uint32_t value) {
    bytes[0] = (uint8_t)(value >> 24);
    bytes[1] = (uint8_t)(value >> 16);
    bytes[2] = (uint8_t)(value >> 8);
    bytes[3] = (uint8_t)value;
}

static int32_t latch_failure(batch_host_context *context, uint32_t reason) {
    if (context == NULL || context->base == NULL) {
        return PRDB_QUERY_CALLBACK_INVALID_ARGUMENT;
    }
    context->failure_latched = 1;
    if (context->base->statement_failure != NULL) {
        (void)context->base->statement_failure(context->base->context, reason);
    }
    return PRDB_QUERY_CALLBACK_IO_ERROR;
}

static int32_t forward_executor_request(
    void *opaque, uint16_t operation, const uint8_t *request,
    uint64_t request_len, uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len) {
    batch_host_context *context = (batch_host_context *)opaque;
    if (context == NULL || context->base == NULL ||
        context->base->executor_request == NULL) {
        return PRDB_QUERY_CALLBACK_INVALID_ARGUMENT;
    }
    return context->base->executor_request(
        context->base->context, operation, request, request_len, response,
        response_capacity, response_len);
}

static int32_t forward_planner_request(
    void *opaque, uint16_t operation, const uint8_t *request,
    uint64_t request_len, uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len) {
    batch_host_context *context = (batch_host_context *)opaque;
    if (context == NULL || context->base == NULL ||
        context->base->planner_request == NULL) {
        return PRDB_QUERY_CALLBACK_INVALID_ARGUMENT;
    }
    return context->base->planner_request(
        context->base->context, operation, request, request_len, response,
        response_capacity, response_len);
}

static int32_t emit_record(batch_host_context *context, uint8_t event,
                           const uint8_t *payload, uint64_t payload_len) {
    uint8_t header[PRDB_SDK_SQL_BATCH_RESULT_HEADER_BYTES] = {
        'S', 'Q', 'L', 'B', 1, event, 0, 0, 0, 0, 0, 0};
    int32_t status;
    if (context == NULL || context->base == NULL ||
        context->base->write_result == NULL || payload_len > UINT32_MAX ||
        (payload_len != 0 && payload == NULL)) {
        return PRDB_QUERY_CALLBACK_INVALID_ARGUMENT;
    }
    write_u16_be(header + 6, context->statement_ordinal);
    write_u32_be(header + 8, (uint32_t)payload_len);
    status = context->base->write_result(context->base->context, header,
                                         sizeof(header));
    if (status != 0) return status;
    if (payload_len == 0) return 0;
    return context->base->write_result(context->base->context, payload,
                                       payload_len);
}

static int32_t frame_result(void *opaque, const uint8_t *bytes,
                            uint64_t bytes_len) {
    batch_host_context *context = (batch_host_context *)opaque;
    int32_t status = emit_record(context, PRDB_SDK_SQL_BATCH_EVENT_DATA,
                                 bytes, bytes_len);
    if (status != 0) {
        return latch_failure(context, PRDB_QUERY_STATEMENT_FAILURE_SQL);
    }
    return 0;
}

static int32_t forward_statement_failure(void *opaque, uint32_t reason) {
    batch_host_context *context = (batch_host_context *)opaque;
    if (context == NULL || context->base == NULL) {
        return PRDB_QUERY_CALLBACK_INVALID_ARGUMENT;
    }
    if (context->failure_latched) return 0;
    context->failure_latched = 1;
    if (context->base->statement_failure == NULL) {
        return PRDB_QUERY_CALLBACK_INVALID_ARGUMENT;
    }
    return context->base->statement_failure(context->base->context, reason);
}

static prdb_host_api_v1 wrap_host(batch_host_context *context) {
    prdb_host_api_v1 host = {0};
    host.struct_size = sizeof(host);
    host.abi_version = PRDB_QUERY_ABI_VERSION;
    host.context = context;
    host.executor_request = forward_executor_request;
    host.planner_request = forward_planner_request;
    host.write_result = frame_result;
    host.statement_failure = forward_statement_failure;
    return host;
}

int32_t prdb_sdk_sql_run_batch_v1(
    const prdb_host_api_v1 *host, const uint8_t *schema,
    uint32_t schema_len, uint32_t statement_count,
    const prdb_sdk_sql_batch_statement_v1 *statements,
    prdb_sdk_sql_batch_statement_runner_v1_fn run_statement) {
    uint64_t package_bytes = 0;
    prdb_host_api_v1 wrapped_host;
    batch_host_context context;
    if (host == NULL || host->struct_size < sizeof(*host) ||
        host->abi_version != PRDB_QUERY_ABI_VERSION ||
        host->statement_failure == NULL || host->write_result == NULL ||
        schema == NULL || schema_len == 0 || schema_len > 128 ||
        statement_count == 0 || run_statement == NULL) {
        return PRDB_SDK_SQL_BATCH_INVALID_ARGUMENT;
    }
    if (statement_count > PRDB_SDK_SQL_BATCH_MAX_STATEMENTS) {
        return PRDB_SDK_SQL_BATCH_LIMIT_EXCEEDED;
    }
    if (statements == NULL) return PRDB_SDK_SQL_BATCH_INVALID_ARGUMENT;
    for (uint32_t index = 0; index < statement_count; ++index) {
        const prdb_sdk_sql_batch_statement_v1 *statement = &statements[index];
        if (statement->sql == NULL || statement->sql_len == 0 ||
            statement->parameters_len > PRDB_SDK_SQL_BATCH_MAX_PARAMETER_BYTES ||
            (statement->parameters_len != 0 && statement->parameters == NULL)) {
            return PRDB_SDK_SQL_BATCH_INVALID_ARGUMENT;
        }
        if (statement->sql_len > PRDB_SDK_SQL_BATCH_MAX_STATEMENT_BYTES ||
            statement->sql_len > PRDB_SDK_SQL_BATCH_MAX_PACKAGE_BYTES - package_bytes) {
            return PRDB_SDK_SQL_BATCH_LIMIT_EXCEEDED;
        }
        package_bytes += statement->sql_len;
        if (statement->parameters_len >
            PRDB_SDK_SQL_BATCH_MAX_PACKAGE_BYTES - package_bytes) {
            return PRDB_SDK_SQL_BATCH_LIMIT_EXCEEDED;
        }
        package_bytes += statement->parameters_len;
    }

    context.base = host;
    context.statement_ordinal = 0;
    context.failure_latched = 0;
    wrapped_host = wrap_host(&context);
    for (uint32_t index = 0; index < statement_count; ++index) {
        uint8_t failure_status[4];
        int32_t status;
        context.statement_ordinal = (uint16_t)index;
        if (emit_record(&context, PRDB_SDK_SQL_BATCH_EVENT_START, NULL, 0) != 0) {
            return latch_failure(&context, PRDB_QUERY_STATEMENT_FAILURE_SQL);
        }
        context.failure_latched = 0;
        status = run_statement(&wrapped_host, schema, schema_len,
                               &statements[index]);
        if (status != 0 || context.failure_latched) {
            write_u32_be(failure_status, (uint32_t)(status == 0
                                                        ? PRDB_SDK_SQL_BATCH_STATEMENT_FAILED
                                                        : status));
            (void)emit_record(&context, PRDB_SDK_SQL_BATCH_EVENT_FAILED,
                              failure_status, sizeof(failure_status));
            if (!context.failure_latched) {
                (void)latch_failure(&context,
                                    PRDB_QUERY_STATEMENT_FAILURE_SQL);
            }
            return PRDB_SDK_SQL_BATCH_STATEMENT_FAILED;
        }
        if (emit_record(&context, PRDB_SDK_SQL_BATCH_EVENT_COMPLETE, NULL, 0) != 0) {
            return latch_failure(&context, PRDB_QUERY_STATEMENT_FAILURE_SQL);
        }
    }
    return PRDB_SDK_SQL_BATCH_OK;
}
