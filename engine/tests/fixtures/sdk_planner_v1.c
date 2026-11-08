#include "prdb_sdk_planner_v1.h"

#include <assert.h>
#include <string.h>

typedef struct planner_mock {
    const uint8_t *reply;
    uint64_t reply_len;
    uint32_t calls;
} planner_mock;

static int32_t planner_request(void *context, uint16_t operation,
                               const uint8_t *request, uint64_t request_len,
                               uint8_t *response, uint64_t capacity,
                               uint64_t *response_len) {
    static const uint8_t expected[] = {
        0, 1, 0, 6, 0, 9, 'p', 'u', 'b', 'l', 'i', 'c',
        'e', 'm', 'p', 'l', 'o', 'y', 'e', 'e', 's'};
    planner_mock *mock = (planner_mock *)context;
    assert(operation == PRDB_SDK_PLANNER_OP_DESCRIBE_RELATION);
    assert(request_len == sizeof(expected));
    assert(memcmp(request, expected, sizeof(expected)) == 0);
    assert(mock->reply_len <= capacity);
    memcpy(response, mock->reply, mock->reply_len);
    *response_len = mock->reply_len;
    ++mock->calls;
    return PRDB_QUERY_CALLBACK_OK;
}

static void put_u16(uint8_t *buffer, uint32_t *offset, uint16_t value) {
    buffer[(*offset)++] = (uint8_t)(value >> 8);
    buffer[(*offset)++] = (uint8_t)value;
}

static void put_u32(uint8_t *buffer, uint32_t *offset, uint32_t value) {
    buffer[(*offset)++] = (uint8_t)(value >> 24);
    buffer[(*offset)++] = (uint8_t)(value >> 16);
    buffer[(*offset)++] = (uint8_t)(value >> 8);
    buffer[(*offset)++] = (uint8_t)value;
}

static void put_name(uint8_t *buffer, uint32_t *offset, const char *name) {
    uint16_t length = (uint16_t)strlen(name);
    put_u16(buffer, offset, length);
    memcpy(buffer + *offset, name, length);
    *offset += length;
}

static void put_column(uint8_t *buffer, uint32_t *offset, uint8_t id,
                       uint32_t ordinal, uint8_t nullable, uint8_t type,
                       uint8_t precision, uint8_t scale, const char *name) {
    memset(buffer + *offset, id, 16);
    *offset += 16;
    put_u32(buffer, offset, ordinal);
    buffer[(*offset)++] = nullable;
    buffer[(*offset)++] = type;
    buffer[(*offset)++] = precision;
    buffer[(*offset)++] = scale;
    buffer[(*offset)++] = 0;
    buffer[(*offset)++] = 0;
    put_name(buffer, offset, name);
}

int main(void) {
    static const uint8_t sql[] =
        "SELECT salary FROM employees WHERE salary >= $1";
    uint8_t response[256] = {0};
    uint8_t unavailable[3] = {0, 1, 1};
    uint32_t length = 0;
    prdb_sdk_planner_relation_v1 relation = {0};
    prdb_sdk_sql_column_shape_v2 columns[2] = {{0}};
    prdb_sdk_select_v2 statement = {0};
    prdb_sdk_select_binding_v2 binding = {0};

    put_u16(response, &length, PRDB_SDK_PLANNER_WIRE_V1_VERSION);
    response[length++] = 0;
    for (uint8_t id = 1; id <= 16; ++id) {
        response[length++] = id;
    }
    response[length++] = PRDB_SDK_PLANNER_TABLE_HANDLE_KIND;
    for (uint8_t byte = 1; byte < 16; ++byte) {
        response[length++] = byte;
    }
    put_name(response, &length, "employees");
    put_u16(response, &length, 2);
    put_column(response, &length, 0x21, 0, 0,
               PRDB_SDK_VALUE_TYPE_DECIMAL, 10, 2, "salary");
    put_column(response, &length, 0x22, 1, 1, PRDB_SDK_VALUE_TYPE_TEXT, 0, 0,
               "name");

    assert(prdb_sdk_planner_decode_relation_v1(
               response, length, &relation, sizeof(relation), columns, 2) ==
           PRDB_SDK_PLANNER_DECODE_OK);
    assert(relation.struct_size == sizeof(relation));
    assert(relation.abi_version == PRDB_SDK_PLANNER_ABI_V1_VERSION);
    assert(relation.available == 1 && relation.column_count == 2);
    assert(relation.name_len == 9 && memcmp(relation.name, "employees", 9) == 0);
    assert(relation.planning_handle[0] ==
           PRDB_SDK_PLANNER_TABLE_HANDLE_KIND);
    assert(columns[0].ordinal == 0 && columns[0].type_code ==
           PRDB_SDK_VALUE_TYPE_DECIMAL);
    assert(columns[0].decimal_precision == 10 && columns[0].decimal_scale == 2);
    assert(columns[0].name_len == 6 && memcmp(columns[0].name, "salary", 6) == 0);
    assert(columns[1].ordinal == 1 && columns[1].type_code ==
           PRDB_SDK_VALUE_TYPE_TEXT);
    assert(columns[1].decimal_precision == 0 && columns[1].decimal_scale == 0);
    assert(prdb_sdk_sql_parse_select_v2(sql, sizeof(sql) - 1, &statement,
                                        sizeof(statement), NULL) ==
           PRDB_SDK_SQL_OK);
    assert(prdb_sdk_sql_bind_select_v2(
               &statement, relation.name, relation.name_len, columns,
               relation.column_count, &binding, sizeof(binding)) ==
           PRDB_SDK_SQL_BIND_OK);
    assert(binding.projection_ordinals[0] == 0);
    assert(binding.predicate_columns[0].ordinal == 0);
    assert(binding.predicate_columns[0].type_code ==
           PRDB_SDK_VALUE_TYPE_DECIMAL);
    assert(binding.predicate_columns[0].decimal_precision == 10);
    assert(binding.predicate_columns[0].decimal_scale == 2);

    {
        uint8_t reply[256];
        const uint8_t schema[] = "public";
        const uint8_t relation_name[] = "employees";
        planner_mock mock = {reply, length, 0};
        prdb_host_api_v2 host = {sizeof(host), PRDB_QUERY_ABI_V2_VERSION,
                                 &mock, NULL, planner_request, NULL};
        uint64_t response_len = 0;
        memcpy(reply, response, length);
        assert(prdb_sdk_planner_describe_relation_v1(
                   &host, schema, sizeof(schema) - 1, relation_name,
                   sizeof(relation_name) - 1, response, sizeof(response),
                   &response_len, &relation, sizeof(relation), columns, 2) ==
               PRDB_SDK_PLANNER_REQUEST_OK);
        assert(mock.calls == 1 && response_len == length);
        assert(relation.planning_handle[0] ==
               PRDB_SDK_PLANNER_TABLE_HANDLE_KIND);
    }

    assert(prdb_sdk_planner_decode_relation_v1(
               unavailable, sizeof(unavailable), &relation, sizeof(relation),
               columns, 2) == PRDB_SDK_PLANNER_DECODE_UNAVAILABLE);
    assert(relation.available == 0 && relation.column_count == 0);

    relation.struct_size = 123;
    columns[0].ordinal = 77;
    assert(prdb_sdk_planner_decode_relation_v1(
               response, length - 1, &relation, sizeof(relation), columns,
               2) == PRDB_SDK_PLANNER_DECODE_INVALID_PAYLOAD);
    assert(relation.struct_size == 123 && columns[0].ordinal == 77);
    assert(prdb_sdk_planner_decode_relation_v1(
               response, length, &relation, sizeof(relation), columns, 1) ==
           PRDB_SDK_PLANNER_DECODE_RESPONSE_TOO_SMALL);
    assert(relation.struct_size == 123 && columns[0].ordinal == 77);
    response[1] = 2;
    assert(prdb_sdk_planner_decode_relation_v1(
               response, length, &relation, sizeof(relation), columns, 2) ==
           PRDB_SDK_PLANNER_DECODE_UNSUPPORTED_VERSION);
    response[1] = 1;
    response[19] = 8;
    assert(prdb_sdk_planner_decode_relation_v1(
               response, length, &relation, sizeof(relation), columns,
               2) == PRDB_SDK_PLANNER_DECODE_INVALID_PAYLOAD);
    response[19] = PRDB_SDK_PLANNER_TABLE_HANDLE_KIND;
    response[70] = 0;
    assert(prdb_sdk_planner_decode_relation_v1(
               response, length, &relation, sizeof(relation), columns,
               2) == PRDB_SDK_PLANNER_DECODE_INVALID_PAYLOAD);
    response[70] = 10;
    response[length++] = 0;
    assert(prdb_sdk_planner_decode_relation_v1(
               response, length, &relation, sizeof(relation), columns, 2) ==
           PRDB_SDK_PLANNER_DECODE_INVALID_PAYLOAD);
    return 0;
}
