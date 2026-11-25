#include "prdb_sdk_optimizer_v1.h"
#include "prdb_sdk_sql_v1.h"

#include <assert.h>
#include <string.h>

typedef struct mock_optimizer {
    const uint8_t *payload;
    uint64_t payload_len;
    int32_t callback_status;
} mock_optimizer;

static int32_t mock_request(void *opaque, const uint8_t *request,
                            uint64_t request_len, uint8_t *response,
                            uint64_t response_capacity,
                            uint64_t *response_len) {
    mock_optimizer *mock = (mock_optimizer *)opaque;
    static const uint8_t expected_request[] = {0x11, 0x22, 0x33};

    assert(request_len == sizeof(expected_request));
    assert(memcmp(request, expected_request, sizeof(expected_request)) == 0);
    if (mock->callback_status != PRDB_QUERY_CALLBACK_OK) {
        *response_len = mock->payload_len;
        return mock->callback_status;
    }
    if (response_capacity < mock->payload_len) {
        *response_len = mock->payload_len;
        return PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL;
    }
    memcpy(response, mock->payload, (size_t)mock->payload_len);
    *response_len = mock->payload_len;
    return PRDB_QUERY_CALLBACK_OK;
}

static int32_t mock_request_v5(void *opaque, uint16_t operation,
                               const uint8_t *request, uint64_t request_len,
                               uint8_t *response,
                               uint64_t response_capacity,
                               uint64_t *response_len) {
    mock_optimizer *mock = (mock_optimizer *)opaque;
    assert(request_len >= 4);
    assert((operation == PRDB_IPC_OP_OPTIMIZE_JOIN_SELECT &&
            (request[2] == PRDB_OPTIMIZER_JOIN_SELECT_STATEMENT ||
             request[2] ==
                 PRDB_OPTIMIZER_FILTERED_JOIN_SELECT_STATEMENT)) ||
           (operation == PRDB_IPC_OP_OPTIMIZE_GROUP_COUNT &&
            (request[2] == PRDB_OPTIMIZER_GROUP_COUNT_STATEMENT ||
             request[2] ==
                 PRDB_OPTIMIZER_COMPOSITE_GROUP_COUNT_STATEMENT ||
             request[2] ==
                 PRDB_OPTIMIZER_GROUP_COUNT_ORDERED_STATEMENT ||
             request[2] == PRDB_OPTIMIZER_GROUP_AGGREGATE_STATEMENT ||
             request[2] ==
                 PRDB_OPTIMIZER_ORDERED_GROUP_AGGREGATE_STATEMENT ||
             request[2] ==
                 PRDB_OPTIMIZER_COMPOSITE_GROUP_AGGREGATE_STATEMENT ||
             request[2] ==
                 PRDB_OPTIMIZER_FILTERED_COMPOSITE_GROUP_AGGREGATE_STATEMENT)));
    assert(request_len >= 4 && request[0] == 0 && request[1] == 1 &&
           request[3] == 0);
    if (mock->callback_status != PRDB_QUERY_CALLBACK_OK) {
        *response_len = mock->payload_len;
        return mock->callback_status;
    }
    if (response_capacity < mock->payload_len) {
        *response_len = mock->payload_len;
        return PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL;
    }
    memcpy(response, mock->payload, (size_t)mock->payload_len);
    *response_len = mock->payload_len;
    return PRDB_QUERY_CALLBACK_OK;
}

static prdb_host_api_v4 host_for(mock_optimizer *mock) {
    prdb_host_api_v4 host = {
        {
            sizeof(prdb_host_api_v3),
            PRDB_QUERY_ABI_V3_VERSION,
            mock,
            NULL,
            NULL,
            NULL,
            NULL,
        },
        mock_request,
    };
    return host;
}

int main(void) {
    static const uint8_t sql[] = "SELECT name FROM items LIMIT 7;";
    static const uint8_t schema[] = "public";
    static const uint8_t join_sql[] =
        "SELECT l.value, r.value FROM lefts AS l INNER JOIN rights AS r "
        "ON l.id = r.id AND l.active = r.active LIMIT 5;";
    static const uint8_t filtered_join_sql[] =
        "SELECT l.value, r.value FROM lefts AS l INNER JOIN rights AS r "
        "ON l.id = r.id AND l.active = r.active WHERE r.active > $3 LIMIT 5;";
    static const uint8_t null_filtered_join_sql[] =
        "SELECT l.value, r.value FROM lefts AS l INNER JOIN rights AS r "
        "ON l.id = r.id AND l.active = r.active "
        "WHERE l.active IS NOT NULL LIMIT 5;";
    static const uint8_t group_count_sql[] =
        "SELECT category, COUNT(*) FROM items GROUP BY category LIMIT 5;";
    static const uint8_t composite_group_count_sql[] =
        "SELECT active, id, COUNT(*) FROM items GROUP BY active, id LIMIT 5;";
    static const uint8_t composite_group_sum_sql[] =
        "SELECT active, id, SUM(amount) FROM items GROUP BY active, id LIMIT 5;";
    static const uint8_t filtered_composite_group_sum_sql[] =
        "SELECT active, id, SUM(amount) FROM items WHERE active = $1 "
        "GROUP BY active, id LIMIT 5;";
    static const uint8_t ordered_group_count_sql[] =
        "SELECT id, COUNT(*) FROM items GROUP BY id "
        "ORDER BY COUNT(*) DESC NULLS LAST, id ASC NULLS FIRST LIMIT 5;";
    static const uint8_t ordered_composite_group_count_sql[] =
        "SELECT left_key, right_key, COUNT(*) FROM grouped_records "
        "GROUP BY left_key, right_key ORDER BY right_key DESC NULLS LAST, "
        "COUNT(*) DESC, left_key ASC NULLS FIRST LIMIT 2;";
    static const uint8_t ordered_composite_group_sum_sql[] =
        "SELECT left_key, right_key, SUM(measure) FROM grouped_records "
        "GROUP BY left_key, right_key ORDER BY SUM(measure) DESC NULLS LAST, "
        "left_key ASC NULLS FIRST LIMIT 2;";
    static const uint8_t group_average_sql[] =
        "SELECT category, AVG(amount) FROM items GROUP BY category LIMIT 5;";
    static const uint8_t ordered_group_average_sql[] =
        "SELECT left_key, AVG(right_key) FROM grouped_records "
        "GROUP BY left_key ORDER BY AVG(right_key) DESC NULLS LAST, "
        "left_key ASC NULLS FIRST LIMIT 2;";
    static const uint8_t ordered_select_sql[] =
        "SELECT id, label FROM items ORDER BY label DESC LIMIT 5;";
    static const uint8_t expected_group_count_request[] = {
        0, 1, 5, 0, 6, 'p', 'u', 'b', 'l', 'i', 'c', 5, 'i', 't', 'e', 'm',
        's', 8, 'c', 'a', 't', 'e', 'g', 'o', 'r', 'y', 1, 0, 0, 0, 5};
    static const uint8_t expected_composite_group_count_request[] = {
        0, 1, 6, 0, 6, 'p', 'u', 'b', 'l', 'i', 'c', 5, 'i', 't', 'e', 'm',
        's', 2, 6, 'a', 'c', 't', 'i', 'v', 'e', 2, 'i', 'd', 1, 0, 0, 0, 5};
    static const uint8_t expected_composite_group_sum_request[] = {
        0, 1, 13, 0, 6, 'p', 'u', 'b', 'l', 'i', 'c', 5, 'i', 't', 'e', 'm',
        's', 2, 6, 'a', 'c', 't', 'i', 'v', 'e', 2, 'i', 'd', 4, 6, 'a', 'm',
        'o', 'u', 'n', 't', 1, 0, 0, 0, 5};
    static const uint8_t expected_filtered_composite_group_sum_request[] = {
        0, 1, 15, 0, 6, 'p', 'u', 'b', 'l', 'i', 'c', 5, 'i', 't', 'e',
        'm', 's', 2, 6, 'a', 'c', 't', 'i', 'v', 'e', 2, 'i', 'd', 4, 6,
        'a', 'm', 'o', 'u', 'n', 't', 0, 0, 0, 12, 0x20, 0, 0x10, 6,
        'a', 'c', 't', 'i', 'v', 'e', 0x01, 1, 1, 0, 0, 0, 5};
    static const uint8_t expected_ordered_group_count_request[] = {
        0, 1, 8, 0, 6, 'p', 'u', 'b', 'l', 'i', 'c', 5, 'i', 't', 'e', 'm',
        's', 2, 'i', 'd', 2, 1, 1, 0, 0, 0, 1, 1, 0, 0, 0, 5};
    static const uint8_t expected_ordered_composite_group_count_request[] = {
        0, 1, 9, 0, 6, 'p', 'u', 'b', 'l', 'i', 'c', 15, 'g', 'r', 'o', 'u', 'p', 'e', 'd', '_', 'r', 'e', 'c', 'o', 'r', 'd', 's',
        2, 8, 'l', 'e', 'f', 't', '_', 'k', 'e', 'y', 9, 'r', 'i', 'g', 'h', 't', '_', 'k', 'e', 'y', 3,
        0, 1, 1, 0, 1, 1, 1, 0, 0, 0, 1, 1, 0, 0, 0, 2};
    static const uint8_t expected_ordered_composite_group_sum_request[] = {
        0, 1, 14, 0, 6, 'p', 'u', 'b', 'l', 'i', 'c',
        15, 'g', 'r', 'o', 'u', 'p', 'e', 'd', '_', 'r', 'e', 'c', 'o', 'r', 'd', 's',
        2, 8, 'l', 'e', 'f', 't', '_', 'k', 'e', 'y',
        9, 'r', 'i', 'g', 'h', 't', '_', 'k', 'e', 'y',
        4, 7, 'm', 'e', 'a', 's', 'u', 'r', 'e',
        2, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 2};
    static const uint8_t expected_group_average_request[] = {
        0, 1, 10, 0, 6, 'p', 'u', 'b', 'l', 'i', 'c', 5, 'i', 't', 'e', 'm', 's',
        8, 'c', 'a', 't', 'e', 'g', 'o', 'r', 'y', 5, 6, 'a', 'm', 'o', 'u', 'n', 't', 1, 0, 0, 0, 5};
    static const uint8_t expected_ordered_group_average_request[] = {
        0, 1, 11, 0, 6, 'p', 'u', 'b', 'l', 'i', 'c', 15, 'g', 'r', 'o', 'u', 'p',
        'e', 'd', '_', 'r', 'e', 'c', 'o', 'r', 'd', 's', 8, 'l', 'e', 'f', 't',
        '_', 'k', 'e', 'y', 5, 9, 'r', 'i', 'g', 'h', 't', '_', 'k', 'e', 'y',
        2, 1, 1, 0, 0, 0, 1, 1, 0, 0, 0, 2};
    static const uint8_t expected_ordered_select_request[] = {
        0, 1, 7, 0, 6, 'p', 'u', 'b', 'l', 'i', 'c', 5, 'i', 't', 'e', 'm',
        's', 0, 2, 2, 'i', 'd', 5, 'l', 'a', 'b', 'e', 'l', 5, 'l', 'a',
        'b', 'e', 'l', 1, 1, 0, 0, 0, 5};
    static const uint8_t expected_join_request[] = {
        0, 1, 4, 0, 6, 'p', 'u', 'b', 'l', 'i', 'c', 5, 'l', 'e', 'f', 't',
        's', 6, 'r', 'i', 'g', 'h', 't', 's', 0, 2, 0, 5, 'v', 'a', 'l', 'u',
        'e', 1, 5, 'v', 'a', 'l', 'u', 'e', 2, 2, 'i', 'd', 2, 'i', 'd', 6,
        'a', 'c', 't', 'i', 'v', 'e', 6, 'a', 'c', 't', 'i', 'v', 'e', 1, 0,
        0, 0, 5};
    static const uint8_t filtered_join_tail[] = {
        1, 6, 'a', 'c', 't', 'i', 'v', 'e', 0, 4, 0, 0, 0, 3};
    static const uint8_t null_filtered_join_tail[] = {
        0, 6, 'a', 'c', 't', 'i', 'v', 'e', 2};
    static const uint8_t expected_encoded[] = {
        0, 1, 1, 0, 6, 'p', 'u', 'b', 'l', 'i', 'c', 5, 'i', 't', 'e', 'm',
        's', 0, 1, 4, 'n', 'a', 'm', 'e', 0, 0, 0, 0, 1, 0, 0, 0, 7};
    static const uint8_t request[] = {0x11, 0x22, 0x33};
    static const uint8_t success[] = {0, 1, 0, 0, 0, 0, 3, 0xaa, 0xbb, 0xcc};
    static const uint8_t unavailable[] = {0, 1, 2};
    static const uint8_t malformed[] = {0, 1, 0, 0, 0, 0, 2, 0xaa};
    mock_optimizer mock = {success, sizeof(success),
                           PRDB_QUERY_CALLBACK_OK};
    prdb_host_api_v4 host = host_for(&mock);
    prdb_host_api_v5 host_v5 = {host, mock_request_v5};
    uint8_t response[sizeof(success)] = {0};
    uint64_t response_len = 0;
    prdb_sdk_optimizer_response_v1 decoded = {0};
    prdb_sdk_select_v1 statement = {0};
    uint8_t encoded[128];
    uint64_t encoded_len = 0;
    uint32_t error_offset = 0;
    prdb_sdk_select_v1 join_statement = {0};
    prdb_sdk_select_v1 filtered_join_statement = {0};
    prdb_sdk_select_v1 group_count_statement = {0};
    prdb_sdk_select_v1 ordered_group_count_statement = {0};
    prdb_sdk_select_v1 composite_group_count_statement = {0};
    prdb_sdk_select_v1 composite_group_sum_statement = {0};
    prdb_sdk_select_v1 filtered_composite_group_sum_statement = {0};
    prdb_sdk_select_v1 ordered_composite_group_count_statement = {0};
    prdb_sdk_select_v1 ordered_composite_group_sum_statement = {0};
    prdb_sdk_select_v1 group_average_statement = {0};
    prdb_sdk_select_v1 ordered_group_average_statement = {0};
    prdb_sdk_select_v1 ordered_select_statement = {0};

    assert(prdb_sdk_sql_parse_select_v1(
               sql, sizeof(sql) - 1, &statement, sizeof(statement),
               &error_offset) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_optimizer_encode_select_v1(
               &statement, schema, sizeof(schema) - 1, encoded,
               sizeof(encoded), &encoded_len) ==
           PRDB_SDK_OPTIMIZER_ENCODE_OK);
    assert(encoded_len == sizeof(expected_encoded));
    assert(memcmp(encoded, expected_encoded, sizeof(expected_encoded)) == 0);

    assert(prdb_sdk_sql_parse_select_v1(
               ordered_select_sql, sizeof(ordered_select_sql) - 1,
               &ordered_select_statement, sizeof(ordered_select_statement),
               &error_offset) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_optimizer_encode_ordered_select_v1(
               &ordered_select_statement, schema, sizeof(schema) - 1,
               encoded, sizeof(encoded), &encoded_len) ==
           PRDB_SDK_OPTIMIZER_ENCODE_OK);
    assert(encoded_len == sizeof(expected_ordered_select_request));
    assert(memcmp(encoded, expected_ordered_select_request,
                  sizeof(expected_ordered_select_request)) == 0);
    ordered_select_statement.order_descending = 2;
    assert(prdb_sdk_optimizer_encode_ordered_select_v1(
               &ordered_select_statement, schema, sizeof(schema) - 1,
               encoded, sizeof(encoded), &encoded_len) ==
           PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT);
    ordered_select_statement.order_descending = 1;

    assert(prdb_sdk_sql_parse_select_v1(
               join_sql, sizeof(join_sql) - 1, &join_statement,
               sizeof(join_statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_optimizer_encode_join_select_v1(
               &join_statement, schema, sizeof(schema) - 1, encoded,
               sizeof(encoded), &encoded_len) ==
           PRDB_SDK_OPTIMIZER_ENCODE_OK);
    assert(encoded_len == sizeof(expected_join_request));
    assert(memcmp(encoded, expected_join_request,
                  sizeof(expected_join_request)) == 0);
    memset(encoded, 0xa5, sizeof(encoded));
    encoded_len = 0;
    assert(prdb_sdk_optimizer_encode_join_select_v1(
               &join_statement, schema, sizeof(schema) - 1, encoded, 1,
               &encoded_len) ==
           PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL);
    assert(encoded_len == sizeof(expected_join_request));
    assert(encoded[0] == 0xa5);
    assert(prdb_sdk_optimizer_encode_join_select_v1(
               &join_statement, schema, sizeof(schema) - 1, encoded,
               sizeof(encoded), &encoded_len) ==
           PRDB_SDK_OPTIMIZER_ENCODE_OK);
    assert(prdb_sdk_optimizer_submit_join_select_v1(
               &host_v5, encoded, encoded_len, response, sizeof(response),
               &response_len, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_RESULT_OK);
    assert(decoded.status == PRDB_SDK_OPTIMIZER_STATUS_SUCCESS);

    uint8_t expected_filtered_join_request[
        sizeof(expected_join_request) + sizeof(filtered_join_tail)];
    memcpy(expected_filtered_join_request, expected_join_request,
           sizeof(expected_join_request));
    expected_filtered_join_request[2] =
        PRDB_OPTIMIZER_FILTERED_JOIN_SELECT_STATEMENT;
    memcpy(expected_filtered_join_request + sizeof(expected_join_request),
           filtered_join_tail, sizeof(filtered_join_tail));
    assert(prdb_sdk_sql_parse_select_v1(
               filtered_join_sql, sizeof(filtered_join_sql) - 1,
               &filtered_join_statement, sizeof(filtered_join_statement),
               &error_offset) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_optimizer_encode_filtered_join_select_v1(
               &filtered_join_statement, schema, sizeof(schema) - 1, encoded,
               sizeof(encoded), &encoded_len) == PRDB_SDK_OPTIMIZER_ENCODE_OK);
    assert(encoded_len == sizeof(expected_filtered_join_request));
    assert(memcmp(encoded, expected_filtered_join_request,
                  sizeof(expected_filtered_join_request)) == 0);
    assert(prdb_sdk_optimizer_submit_join_select_v1(
               &host_v5, encoded, encoded_len, response, sizeof(response),
               &response_len, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_RESULT_OK);
    assert(decoded.status == PRDB_SDK_OPTIMIZER_STATUS_SUCCESS);
    uint8_t expected_null_filtered_join_request[
        sizeof(expected_join_request) + sizeof(null_filtered_join_tail)];
    memcpy(expected_null_filtered_join_request, expected_join_request,
           sizeof(expected_join_request));
    expected_null_filtered_join_request[2] =
        PRDB_OPTIMIZER_FILTERED_JOIN_SELECT_STATEMENT;
    memcpy(expected_null_filtered_join_request + sizeof(expected_join_request),
           null_filtered_join_tail, sizeof(null_filtered_join_tail));
    assert(prdb_sdk_sql_parse_select_v1(
               null_filtered_join_sql, sizeof(null_filtered_join_sql) - 1,
               &filtered_join_statement, sizeof(filtered_join_statement),
               &error_offset) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_optimizer_encode_filtered_join_select_v1(
               &filtered_join_statement, schema, sizeof(schema) - 1, encoded,
               sizeof(encoded), &encoded_len) == PRDB_SDK_OPTIMIZER_ENCODE_OK);
    assert(encoded_len == sizeof(expected_null_filtered_join_request));
    assert(memcmp(encoded, expected_null_filtered_join_request,
                  sizeof(expected_null_filtered_join_request)) == 0);
    assert(prdb_sdk_sql_parse_select_v1(
               filtered_join_sql, sizeof(filtered_join_sql) - 1,
               &filtered_join_statement, sizeof(filtered_join_statement),
               &error_offset) == PRDB_SDK_SQL_OK);
    memset(encoded, 0xa5, sizeof(encoded));
    encoded_len = 0;
    assert(prdb_sdk_optimizer_encode_filtered_join_select_v1(
               &filtered_join_statement, schema, sizeof(schema) - 1, encoded,
               sizeof(expected_filtered_join_request) - 1, &encoded_len) ==
           PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL);
    assert(encoded_len == sizeof(expected_filtered_join_request));
    assert(encoded[0] == 0xa5);
    filtered_join_statement.join_where_parameter_index = 0;
    assert(prdb_sdk_optimizer_encode_filtered_join_select_v1(
               &filtered_join_statement, schema, sizeof(schema) - 1, encoded,
               sizeof(encoded), &encoded_len) ==
           PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED);

    assert(prdb_sdk_sql_parse_select_v1(
               ordered_composite_group_count_sql,
               sizeof(ordered_composite_group_count_sql) - 1,
               &ordered_composite_group_count_statement,
               sizeof(ordered_composite_group_count_statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_optimizer_encode_ordered_composite_group_count_v1(
               &ordered_composite_group_count_statement, schema, sizeof(schema) - 1,
               encoded, sizeof(encoded), &encoded_len) == PRDB_SDK_OPTIMIZER_ENCODE_OK);
    assert(encoded_len == sizeof(expected_ordered_composite_group_count_request));
    assert(memcmp(encoded, expected_ordered_composite_group_count_request, encoded_len) == 0);
    assert(prdb_sdk_sql_parse_select_v1(
               ordered_composite_group_sum_sql,
               sizeof(ordered_composite_group_sum_sql) - 1,
               &ordered_composite_group_sum_statement,
               sizeof(ordered_composite_group_sum_statement), &error_offset) ==
           PRDB_SDK_SQL_OK);
    assert(prdb_sdk_optimizer_encode_ordered_composite_group_aggregate_v1(
               &ordered_composite_group_sum_statement, schema,
               sizeof(schema) - 1, encoded, sizeof(encoded), &encoded_len) ==
           PRDB_SDK_OPTIMIZER_ENCODE_OK);
    assert(encoded_len == sizeof(expected_ordered_composite_group_sum_request));
    assert(memcmp(encoded, expected_ordered_composite_group_sum_request,
                  encoded_len) == 0);
    assert(prdb_sdk_sql_parse_select_v1(group_average_sql, sizeof(group_average_sql) - 1,
               &group_average_statement, sizeof(group_average_statement), &error_offset) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_optimizer_encode_group_aggregate_v1(&group_average_statement, schema, sizeof(schema) - 1,
               encoded, sizeof(encoded), &encoded_len) == PRDB_SDK_OPTIMIZER_ENCODE_OK);
    assert(encoded_len == sizeof(expected_group_average_request));
    assert(memcmp(encoded, expected_group_average_request, encoded_len) == 0);
    assert(prdb_sdk_sql_parse_select_v1(
               ordered_group_average_sql,
               sizeof(ordered_group_average_sql) - 1,
               &ordered_group_average_statement,
               sizeof(ordered_group_average_statement), &error_offset) ==
           PRDB_SDK_SQL_OK);
    assert(prdb_sdk_optimizer_encode_ordered_group_aggregate_v1(
               &ordered_group_average_statement, schema, sizeof(schema) - 1,
               encoded, sizeof(encoded), &encoded_len) ==
           PRDB_SDK_OPTIMIZER_ENCODE_OK);
    assert(encoded_len == sizeof(expected_ordered_group_average_request));
    assert(memcmp(encoded, expected_ordered_group_average_request,
                  sizeof(expected_ordered_group_average_request)) == 0);
    assert(prdb_sdk_sql_parse_select_v1(
               composite_group_count_sql,
               sizeof(composite_group_count_sql) - 1,
               &composite_group_count_statement,
               sizeof(composite_group_count_statement), &error_offset) ==
           PRDB_SDK_SQL_OK);
    assert(prdb_sdk_optimizer_encode_composite_group_count_v1(
               &composite_group_count_statement, schema, sizeof(schema) - 1,
               encoded, sizeof(encoded), &encoded_len) ==
           PRDB_SDK_OPTIMIZER_ENCODE_OK);
    assert(encoded_len == sizeof(expected_composite_group_count_request));
    assert(memcmp(encoded, expected_composite_group_count_request,
                  sizeof(expected_composite_group_count_request)) == 0);
    assert(prdb_sdk_sql_parse_select_v1(
               composite_group_sum_sql, sizeof(composite_group_sum_sql) - 1,
               &composite_group_sum_statement,
               sizeof(composite_group_sum_statement), &error_offset) ==
           PRDB_SDK_SQL_OK);
    assert(prdb_sdk_optimizer_encode_composite_group_aggregate_v1(
               &composite_group_sum_statement, schema, sizeof(schema) - 1,
               encoded, sizeof(encoded), &encoded_len) ==
           PRDB_SDK_OPTIMIZER_ENCODE_OK);
    assert(encoded_len == sizeof(expected_composite_group_sum_request));
    assert(memcmp(encoded, expected_composite_group_sum_request,
                  sizeof(expected_composite_group_sum_request)) == 0);
    {
        uint8_t bool_value = 1;
        prdb_sdk_value_v1 parameters[1] = {{
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BOOLEAN,
            &bool_value, 1}};
        assert(prdb_sdk_sql_parse_select_v1(
                   filtered_composite_group_sum_sql,
                   sizeof(filtered_composite_group_sum_sql) - 1,
                   &filtered_composite_group_sum_statement,
                   sizeof(filtered_composite_group_sum_statement),
                   &error_offset) == PRDB_SDK_SQL_OK);
        assert(prdb_sdk_optimizer_encode_filtered_composite_group_aggregate_v1(
                   &filtered_composite_group_sum_statement, parameters, 1,
                   schema, sizeof(schema) - 1, encoded, sizeof(encoded),
                   &encoded_len) == PRDB_SDK_OPTIMIZER_ENCODE_OK);
        assert(encoded_len == sizeof(expected_filtered_composite_group_sum_request));
        assert(memcmp(encoded, expected_filtered_composite_group_sum_request,
                      encoded_len) == 0);
    }
    {
        static const uint8_t bigint_filtered_aggregate_sql[] =
            "SELECT active, id, SUM(amount) FROM items "
            "WHERE amount >= $1 GROUP BY active, id";
        static const uint8_t expected_bigint_filtered_aggregate_request[] = {
            0, 1, 15, 0, 6, 'p', 'u', 'b', 'l', 'i', 'c', 5, 'i', 't', 'e',
            'm', 's', 2, 6, 'a', 'c', 't', 'i', 'v', 'e', 2, 'i', 'd', 4, 6,
            'a', 'm', 'o', 'u', 'n', 't', 0, 0, 0, 19, 0x20, 5, 0x10, 6,
            'a', 'm', 'o', 'u', 'n', 't', 0x02, 0, 0, 0, 0, 0, 0, 0, 9, 0};
        uint8_t bigint_bytes[8] = {0, 0, 0, 0, 0, 0, 0, 9};
        prdb_sdk_value_v1 parameters[1] = {{
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BIGINT,
            bigint_bytes, sizeof(bigint_bytes)}};
        prdb_sdk_select_v1 statement = {0};
        assert(prdb_sdk_sql_parse_select_v1(
                   bigint_filtered_aggregate_sql,
                   sizeof(bigint_filtered_aggregate_sql) - 1, &statement,
                   sizeof(statement), &error_offset) == PRDB_SDK_SQL_OK);
        assert(prdb_sdk_optimizer_encode_filtered_composite_group_aggregate_v1(
                   &statement, parameters, 1, schema, sizeof(schema) - 1,
                   encoded, sizeof(encoded), &encoded_len) ==
               PRDB_SDK_OPTIMIZER_ENCODE_OK);
        assert(encoded_len == sizeof(expected_bigint_filtered_aggregate_request));
        assert(memcmp(encoded, expected_bigint_filtered_aggregate_request,
                      encoded_len) == 0);
    }
    {
        static const uint8_t filtered_count_sql[] =
            "SELECT left_key, right_key, COUNT(bigint_value) "
            "FROM decimal_grouped_records "
            "WHERE bigint_value IS NULL OR bigint_value >= $1 "
            "GROUP BY left_key, right_key "
            "HAVING COUNT(bigint_value) >= 0 "
            "ORDER BY COUNT(bigint_value) DESC NULLS LAST, left_key ASC";
        uint8_t bigint_bytes[8] = {0, 0, 0, 0, 0, 0, 0, 9};
        prdb_sdk_value_v1 parameters[1] = {{
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BIGINT,
            bigint_bytes, sizeof(bigint_bytes)}};
        prdb_sdk_select_v1 parsed = {0};
        prdb_sdk_select_binding_v1 binding = {0};
        static const uint8_t relation[] = "decimal_grouped_records";
        static const prdb_sdk_sql_column_shape_v1 columns[] = {
            {(const uint8_t *)"left_key", 8, 0, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
            {(const uint8_t *)"right_key", 9, 1, PRDB_SDK_VALUE_TYPE_INTEGER, 0, 0},
            {(const uint8_t *)"decimal_value", 13, 2, PRDB_SDK_VALUE_TYPE_DECIMAL, 4, 2},
            {(const uint8_t *)"bigint_value", 12, 3, PRDB_SDK_VALUE_TYPE_BIGINT, 0, 0}};
        uint8_t ordered_encoded[128];
        uint64_t ordered_encoded_len = 0;
        assert(prdb_sdk_sql_parse_select_v1(
                   filtered_count_sql, sizeof(filtered_count_sql) - 1,
                   &parsed, sizeof(parsed), &error_offset) == PRDB_SDK_SQL_OK);
        assert(prdb_sdk_sql_bind_select_v1(
                   &parsed, relation, sizeof(relation) - 1, columns, 4,
                   &binding, sizeof(binding)) == PRDB_SDK_SQL_BIND_OK);
        assert(prdb_sdk_optimizer_encode_ordered_filtered_composite_group_aggregate_v1(
                   &parsed, parameters, 1, schema,
                   sizeof(schema) - 1, ordered_encoded, sizeof(ordered_encoded),
                   &ordered_encoded_len) == PRDB_SDK_OPTIMIZER_ENCODE_OK);
    }
    assert(prdb_sdk_optimizer_submit_group_count_v1(
               &host_v5, encoded, encoded_len, response, sizeof(response),
               &response_len, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_RESULT_OK);

    assert(prdb_sdk_sql_parse_select_v1(
               group_count_sql, sizeof(group_count_sql) - 1,
               &group_count_statement, sizeof(group_count_statement),
               &error_offset) == PRDB_SDK_SQL_OK);
    assert(prdb_sdk_optimizer_encode_group_count_v1(
               &group_count_statement, schema, sizeof(schema) - 1, encoded,
               sizeof(encoded), &encoded_len) == PRDB_SDK_OPTIMIZER_ENCODE_OK);
    assert(encoded_len == sizeof(expected_group_count_request));
    assert(memcmp(encoded, expected_group_count_request,
                  sizeof(expected_group_count_request)) == 0);
    assert(prdb_sdk_sql_parse_select_v1(
               ordered_group_count_sql, sizeof(ordered_group_count_sql) - 1,
               &ordered_group_count_statement,
               sizeof(ordered_group_count_statement), &error_offset) ==
           PRDB_SDK_SQL_OK);
    assert(prdb_sdk_optimizer_encode_ordered_group_count_v1(
               &ordered_group_count_statement, schema, sizeof(schema) - 1,
               encoded, sizeof(encoded), &encoded_len) ==
           PRDB_SDK_OPTIMIZER_ENCODE_OK);
    assert(encoded_len == sizeof(expected_ordered_group_count_request));
    assert(memcmp(encoded, expected_ordered_group_count_request,
                  sizeof(expected_ordered_group_count_request)) == 0);
    assert(prdb_sdk_optimizer_submit_group_count_v1(
               &host_v5, encoded, encoded_len, response, sizeof(response),
               &response_len, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_RESULT_OK);
    assert(decoded.status == PRDB_SDK_OPTIMIZER_STATUS_SUCCESS);
    {
        static const uint8_t filtered_group_sql[] =
            "SELECT category, COUNT(*) FROM items WHERE active = $1 "
            "GROUP BY category";
        prdb_sdk_select_v1 filtered_group_statement = {0};
        assert(prdb_sdk_sql_parse_select_v1(
                   filtered_group_sql, sizeof(filtered_group_sql) - 1,
                   &filtered_group_statement,
                   sizeof(filtered_group_statement), &error_offset) ==
               PRDB_SDK_SQL_OK);
        assert(prdb_sdk_optimizer_encode_group_count_v1(
                   &filtered_group_statement, schema, sizeof(schema) - 1,
                   encoded, sizeof(encoded), &encoded_len) ==
               PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED);
    }

    memset(encoded, 0xa5, sizeof(encoded));
    encoded_len = 0;
    assert(prdb_sdk_optimizer_encode_select_v1(
               &statement, schema, sizeof(schema) - 1, encoded, 1,
               &encoded_len) ==
           PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL);
    assert(encoded_len == sizeof(expected_encoded));
    assert(encoded[0] == 0xa5);

    {
        static const uint8_t star_sql[] = "SELECT * FROM items;";
        prdb_sdk_select_v1 star_statement = {0};
        assert(prdb_sdk_sql_parse_select_v1(
                   star_sql, sizeof(star_sql) - 1, &star_statement,
                   sizeof(star_statement), &error_offset) == PRDB_SDK_SQL_OK);
        assert(prdb_sdk_optimizer_encode_select_v1(
                   &star_statement, schema, sizeof(schema) - 1, encoded,
                   sizeof(encoded), &encoded_len) ==
               PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED);
    }
    {
        static const uint8_t predicate_sql[] =
            "SELECT name FROM items WHERE id = $1;";
        static const uint8_t expected_predicate_request[] = {
            0, 1, 1, 0, 6, 'p', 'u', 'b', 'l', 'i', 'c', 5, 'i', 't', 'e',
            'm', 's', 0, 1, 4, 'n', 'a', 'm', 'e', 0, 0, 0, 15, 0x20, 0,
            0x10, 2, 'i', 'd', 0x02, 0, 0, 0, 0, 0, 0, 0, 7, 0};
        prdb_sdk_select_v1 predicate_statement = {0};
        uint8_t integer_bytes[4] = {0, 0, 0, 7};
        prdb_sdk_value_v1 parameters[1] = {{
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_INTEGER,
            integer_bytes, sizeof(integer_bytes)}};
        assert(prdb_sdk_sql_parse_select_v1(
                   predicate_sql, sizeof(predicate_sql) - 1,
                   &predicate_statement, sizeof(predicate_statement),
                   &error_offset) == PRDB_SDK_SQL_OK);
        assert(prdb_sdk_optimizer_encode_select_v1(
                   &predicate_statement, schema, sizeof(schema) - 1, encoded,
                   sizeof(encoded), &encoded_len) ==
               PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED);
        assert(prdb_sdk_optimizer_encode_select_with_parameters_v1(
                   &predicate_statement, parameters, 1, schema,
                   sizeof(schema) - 1, encoded, sizeof(encoded),
                   &encoded_len) == PRDB_SDK_OPTIMIZER_ENCODE_OK);
        assert(encoded_len == sizeof(expected_predicate_request));
        assert(memcmp(encoded, expected_predicate_request,
                      sizeof(expected_predicate_request)) == 0);
        assert(prdb_sdk_optimizer_encode_select_with_parameters_v1(
                   &predicate_statement, NULL, 0, schema,
                   sizeof(schema) - 1, encoded, sizeof(encoded),
                   &encoded_len) ==
               PRDB_SDK_OPTIMIZER_ENCODE_INVALID_ARGUMENT);
    }
    {
        static const uint8_t date_predicate_sql[] =
            "SELECT name FROM items WHERE created_on = $1;";
        static const uint8_t expected_date_predicate[] = {
            0, 1, 1, 0, 6, 'p', 'u', 'b', 'l', 'i', 'c', 5, 'i', 't', 'e',
            'm', 's', 0, 1, 4, 'n', 'a', 'm', 'e', 0, 0, 0, 19, 0x20, 0,
            0x10, 10, 'c', 'r', 'e', 'a', 't', 'e', 'd', '_', 'o', 'n',
            0x04, 0xff, 0xff, 0xff, 0xd6, 0};
        prdb_sdk_select_v1 date_statement = {0};
        uint8_t date_bytes[4] = {0xff, 0xff, 0xff, 0xd6};
        prdb_sdk_value_v1 date_parameters[1] = {{
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_DATE,
            date_bytes, sizeof(date_bytes)}};
        assert(prdb_sdk_sql_parse_select_v1(
                   date_predicate_sql, sizeof(date_predicate_sql) - 1,
                   &date_statement, sizeof(date_statement),
                   &error_offset) == PRDB_SDK_SQL_OK);
        assert(prdb_sdk_optimizer_encode_select_with_parameters_v1(
                   &date_statement, date_parameters, 1, schema,
                   sizeof(schema) - 1, encoded, sizeof(encoded),
                   &encoded_len) == PRDB_SDK_OPTIMIZER_ENCODE_OK);
        assert(encoded_len == sizeof(expected_date_predicate));
        assert(memcmp(encoded, expected_date_predicate,
                      sizeof(expected_date_predicate)) == 0);
    }
    {
        static const uint8_t uuid_predicate_sql[] =
            "SELECT name FROM items WHERE object_id = $1;";
        static const uint8_t expected_uuid_predicate[] = {
            0, 1, 1, 0, 6, 'p', 'u', 'b', 'l', 'i', 'c', 5, 'i', 't', 'e',
            'm', 's', 0, 1, 4, 'n', 'a', 'm', 'e', 0, 0, 0, 30, 0x20, 0,
            0x10, 9, 'o', 'b', 'j', 'e', 'c', 't', '_', 'i', 'd', 0x05,
            0x12, 0x12, 0x12, 0x12, 0x12, 0x12, 0x12, 0x12,
            0x12, 0x12, 0x12, 0x12, 0x12, 0x12, 0x12, 0x12, 0};
        prdb_sdk_select_v1 uuid_statement = {0};
        uint8_t uuid_bytes[16];
        memset(uuid_bytes, 0x12, sizeof(uuid_bytes));
        prdb_sdk_value_v1 uuid_parameters[1] = {{
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_UUID,
            uuid_bytes, sizeof(uuid_bytes)}};
        assert(prdb_sdk_sql_parse_select_v1(
                   uuid_predicate_sql, sizeof(uuid_predicate_sql) - 1,
                   &uuid_statement, sizeof(uuid_statement),
                   &error_offset) == PRDB_SDK_SQL_OK);
        assert(prdb_sdk_optimizer_encode_select_with_parameters_v1(
                   &uuid_statement, uuid_parameters, 1, schema,
                   sizeof(schema) - 1, encoded, sizeof(encoded),
                   &encoded_len) == PRDB_SDK_OPTIMIZER_ENCODE_OK);
        assert(encoded_len == sizeof(expected_uuid_predicate));
        assert(memcmp(encoded, expected_uuid_predicate,
                      sizeof(expected_uuid_predicate)) == 0);
    }
    {
        static const uint8_t timestamp_predicate_sql[] =
            "SELECT name FROM items WHERE created_at = $1;";
        static const uint8_t expected_timestamp_predicate[] = {
            0, 1, 1, 0, 6, 'p', 'u', 'b', 'l', 'i', 'c', 5, 'i', 't', 'e',
            'm', 's', 0, 1, 4, 'n', 'a', 'm', 'e', 0, 0, 0, 23, 0x20, 0,
            0x10, 10, 'c', 'r', 'e', 'a', 't', 'e', 'd', '_', 'a', 't',
            0x06, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xd6, 0};
        prdb_sdk_select_v1 timestamp_statement = {0};
        uint8_t timestamp_bytes[8] = {0xff, 0xff, 0xff, 0xff,
                                      0xff, 0xff, 0xff, 0xd6};
        prdb_sdk_value_v1 timestamp_parameters[1] = {{
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ,
            timestamp_bytes, sizeof(timestamp_bytes)}};
        assert(prdb_sdk_sql_parse_select_v1(
                   timestamp_predicate_sql,
                   sizeof(timestamp_predicate_sql) - 1,
                   &timestamp_statement, sizeof(timestamp_statement),
                   &error_offset) == PRDB_SDK_SQL_OK);
        assert(prdb_sdk_optimizer_encode_select_with_parameters_v1(
                   &timestamp_statement, timestamp_parameters, 1, schema,
                   sizeof(schema) - 1, encoded, sizeof(encoded),
                   &encoded_len) == PRDB_SDK_OPTIMIZER_ENCODE_OK);
        assert(encoded_len == sizeof(expected_timestamp_predicate));
        assert(memcmp(encoded, expected_timestamp_predicate,
                      sizeof(expected_timestamp_predicate)) == 0);
    }
    {
        static const uint8_t bytea_predicate_sql[] =
            "SELECT name FROM items WHERE payload = $1;";
        static const uint8_t expected_bytea_predicate[] = {
            0, 1, 1, 0, 6, 'p', 'u', 'b', 'l', 'i', 'c', 5, 'i', 't', 'e',
            'm', 's', 0, 1, 4, 'n', 'a', 'm', 'e', 0, 0, 0, 17, 0x20, 0,
            0x10, 7, 'p', 'a', 'y', 'l', 'o', 'a', 'd', 0x07, 0, 3,
            0, 0x80, 0xff, 0};
        prdb_sdk_select_v1 bytea_statement = {0};
        uint8_t bytea_bytes[3] = {0, 0x80, 0xff};
        prdb_sdk_value_v1 bytea_parameters[1] = {{
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_BYTEA,
            bytea_bytes, sizeof(bytea_bytes)}};
        assert(prdb_sdk_sql_parse_select_v1(
                   bytea_predicate_sql, sizeof(bytea_predicate_sql) - 1,
                   &bytea_statement, sizeof(bytea_statement),
                   &error_offset) == PRDB_SDK_SQL_OK);
        assert(prdb_sdk_optimizer_encode_select_with_parameters_v1(
                   &bytea_statement, bytea_parameters, 1, schema,
                   sizeof(schema) - 1, encoded, sizeof(encoded),
                   &encoded_len) == PRDB_SDK_OPTIMIZER_ENCODE_OK);
        assert(encoded_len == sizeof(expected_bytea_predicate));
        assert(memcmp(encoded, expected_bytea_predicate,
                      sizeof(expected_bytea_predicate)) == 0);
    }
    {
        static const uint8_t decimal_predicate_sql[] =
            "SELECT name FROM items WHERE amount = $1;";
        static const uint8_t expected_decimal_predicate[] = {
            0, 1, 1, 0, 6, 'p', 'u', 'b', 'l', 'i', 'c', 5, 'i', 't', 'e',
            'm', 's', 0, 1, 4, 'n', 'a', 'm', 'e', 0, 0, 0, 27, 0x20, 0,
            0x10, 6, 'a', 'm', 'o', 'u', 'n', 't', 0x08,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0xe2, 0x40, 0};
        prdb_sdk_select_v1 decimal_statement = {0};
        uint8_t decimal_bytes[16] = {
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0xe2, 0x40};
        prdb_sdk_value_v1 decimal_parameters[1] = {{
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_VALUE, PRDB_SDK_VALUE_TYPE_DECIMAL,
            decimal_bytes, sizeof(decimal_bytes)}};
        assert(prdb_sdk_sql_parse_select_v1(
                   decimal_predicate_sql, sizeof(decimal_predicate_sql) - 1,
                   &decimal_statement, sizeof(decimal_statement),
                   &error_offset) == PRDB_SDK_SQL_OK);
        assert(prdb_sdk_optimizer_encode_select_with_parameters_v1(
                   &decimal_statement, decimal_parameters, 1, schema,
                   sizeof(schema) - 1, encoded, sizeof(encoded),
                   &encoded_len) == PRDB_SDK_OPTIMIZER_ENCODE_OK);
        assert(encoded_len == sizeof(expected_decimal_predicate));
        assert(memcmp(encoded, expected_decimal_predicate,
                      sizeof(expected_decimal_predicate)) == 0);
    }

    assert(prdb_sdk_optimizer_submit_select_v1(
               &host, request, sizeof(request), response, sizeof(response),
               &response_len, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_RESULT_OK);
    assert(response_len == sizeof(success));
    assert(decoded.status == PRDB_SDK_OPTIMIZER_STATUS_SUCCESS);
    assert(decoded.plan_len == 3);
    assert(decoded.plan == response + 7);
    assert(memcmp(decoded.plan, "\xaa\xbb\xcc", 3) == 0);

    mock.payload = unavailable;
    mock.payload_len = sizeof(unavailable);
    memset(response, 0, sizeof(response));
    assert(prdb_sdk_optimizer_submit_select_v1(
               &host, request, sizeof(request), response, sizeof(response),
               &response_len, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_RESULT_OK);
    assert(decoded.status == PRDB_SDK_OPTIMIZER_STATUS_UNAVAILABLE);
    assert(decoded.plan == NULL && decoded.plan_len == 0);

    mock.payload = success;
    mock.payload_len = sizeof(success);
    assert(prdb_sdk_optimizer_submit_select_v1(
               &host, request, sizeof(request), response, 2, &response_len,
               &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_RESULT_RESPONSE_TOO_SMALL);
    assert(response_len == sizeof(success));

    mock.payload_len = PRDB_QUERY_MAX_IPC_REQUEST_BYTES + 1;
    assert(prdb_sdk_optimizer_submit_select_v1(
               &host, request, sizeof(request), response, 2, &response_len,
               &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE);

    mock.callback_status = PRDB_QUERY_CALLBACK_IO_ERROR;
    assert(prdb_sdk_optimizer_submit_select_v1(
               &host, request, sizeof(request), response, sizeof(response),
               &response_len, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_RESULT_CALLBACK_ERROR);

    assert(prdb_sdk_optimizer_decode_response_v1(
               malformed, sizeof(malformed), &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_RESULT_INVALID_RESPONSE);
    return 0;
}
