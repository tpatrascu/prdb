#include "prdb_sdk_optimizer_v1.h"

#include <assert.h>
#include <string.h>

typedef struct mock_optimizer {
    const uint8_t *expected_request;
    uint64_t expected_request_len;
} mock_optimizer;

static int32_t submit_delete(void *opaque, uint16_t operation,
                             const uint8_t *request, uint64_t request_len,
                             uint8_t *response, uint64_t response_capacity,
                             uint64_t *response_len) {
    static const uint8_t success[] = {0, 1, 0, 0, 0, 0, 1, 0xaa};
    mock_optimizer *mock = (mock_optimizer *)opaque;
    assert(operation == PRDB_IPC_OP_OPTIMIZE_DELETE);
    assert(request_len == mock->expected_request_len);
    assert(memcmp(request, mock->expected_request, request_len) == 0);
    if (response_capacity < sizeof(success)) {
        *response_len = sizeof(success);
        return PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL;
    }
    memcpy(response, success, sizeof(success));
    *response_len = sizeof(success);
    return PRDB_QUERY_CALLBACK_OK;
}

static int32_t submit_select_unused(void *context, const uint8_t *request,
                                    uint64_t request_len, uint8_t *response,
                                    uint64_t response_capacity,
                                    uint64_t *response_len) {
    (void)context;
    (void)request;
    (void)request_len;
    (void)response;
    (void)response_capacity;
    (void)response_len;
    return PRDB_QUERY_CALLBACK_INTERNAL_ERROR;
}

int main(void) {
    static const uint8_t sql[] = "DELETE FROM items WHERE id = $1;";
    static const uint8_t unsupported_sql[] =
        "DELETE FROM items WHERE id IS NULL;";
    static const uint8_t schema[] = "public";
    static const uint8_t expected[] = {
        0, 1, 2, 0, 6, 'p', 'u', 'b', 'l', 'i', 'c', 5, 'i', 't', 'e', 'm',
        's', 0, 0, 0, 15, 0x20, 0, 0x10, 2, 'i', 'd', 0x02,
        0, 0, 0, 0, 0, 0, 0, 7};
    uint8_t integer_bytes[4] = {0, 0, 0, 7};
    prdb_sdk_value_v1 parameters[] = {{
        sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
        integer_bytes, sizeof(integer_bytes)}};
    prdb_sdk_delete_v1 statement = {0};
    prdb_sdk_delete_v1 unsupported = {0};
    uint8_t request[64];
    uint64_t request_len = 0;
    uint32_t error_offset = 0;
    mock_optimizer mock = {expected, sizeof(expected)};
    prdb_host_api_v1 host = {
        .struct_size = sizeof(prdb_host_api_v1),
        .abi_version = PRDB_QUERY_ABI_VERSION,
        .context = &mock,
        .optimizer_request = submit_delete,
    };
    uint8_t response[8];
    uint64_t response_len = 0;
    prdb_sdk_optimizer_response_v1 decoded = {0};

    assert(prdb_sdk_sql_parse_delete_v1(sql, sizeof(sql) - 1, &statement,
                                        sizeof(statement), &error_offset) ==
           PRDB_SDK_SQL_OK);
    assert(prdb_sdk_optimizer_encode_delete_v1(
               &statement, parameters, 1, schema, sizeof(schema) - 1, request,
               sizeof(request), &request_len) == PRDB_SDK_OPTIMIZER_ENCODE_OK);
    assert(request_len == sizeof(expected));
    assert(memcmp(request, expected, sizeof(expected)) == 0);

    memset(request, 0xa5, sizeof(request));
    request_len = 0;
    assert(prdb_sdk_optimizer_encode_delete_v1(
               &statement, parameters, 1, schema, sizeof(schema) - 1, request,
               1, &request_len) ==
           PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL);
    assert(request_len == sizeof(expected) && request[0] == 0xa5);
    assert(prdb_sdk_optimizer_encode_delete_v1(
               &statement, parameters, 1, schema, sizeof(schema) - 1, NULL,
               sizeof(request), &request_len) ==
           PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT);
    assert(prdb_sdk_optimizer_encode_delete_v1(
               &statement, parameters, 1, schema, sizeof(schema) - 1,
               (uint8_t *)statement.predicate_statement.predicates,
               sizeof(request), &request_len) ==
           PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT);
    assert(prdb_sdk_optimizer_encode_delete_v1(
               &statement, parameters, 1, schema, sizeof(schema) - 1,
               integer_bytes, sizeof(request), &request_len) ==
           PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT);
    assert(prdb_sdk_optimizer_encode_delete_v1(
               &statement, NULL, 0, schema, sizeof(schema) - 1, request,
               sizeof(request), &request_len) ==
           PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT);

    assert(prdb_sdk_sql_parse_delete_v1(
               unsupported_sql, sizeof(unsupported_sql) - 1, &unsupported,
               sizeof(unsupported), &error_offset) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_optimizer_encode_delete_v1(
               &unsupported, NULL, 0, schema, sizeof(schema) - 1, request,
               sizeof(request), &request_len) ==
           PRDB_SDK_OPTIMIZER_ENCODE_OK);
    assert(request_len == 26 && request[21] == 0x40 && request[22] == 0x10 &&
           request[23] == 2 && request[24] == 'i' && request[25] == 'd');

    assert(prdb_sdk_optimizer_encode_delete_v1(
               &statement, parameters, 1, schema, sizeof(schema) - 1, request,
               sizeof(request), &request_len) == PRDB_SDK_OPTIMIZER_ENCODE_OK);
    mock.expected_request = request;
    mock.expected_request_len = request_len;
    assert(prdb_sdk_optimizer_submit_delete_v1(
               &host, request, request_len, response, sizeof(response),
               &response_len, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_RESULT_OK);
    assert(decoded.status == PRDB_SDK_OPTIMIZER_STATUS_SUCCESS);
    assert(decoded.plan_len == 1 && decoded.plan == response + 7);
    return 0;
}
