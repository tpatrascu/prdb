#include "prdb_query_abi_v6.h"
#include "prdb_sdk_sql_dispatch_v1.h"
#include "prdb_sdk_spill_v1.h"

#include <stddef.h>

const prdb_query_descriptor_v6_record prdb_query_descriptor_v6 = {
    sizeof(prdb_query_descriptor_v6_record), PRDB_QUERY_ABI_V6_VERSION,
    sizeof(prdb_host_api_v6), sizeof(prdb_invocation_v6)};

int32_t prdb_query_main_v6(const prdb_host_api_v6 *host,
                           const prdb_invocation_v6 *invocation) {
    static const uint8_t schema[] = "sql_roundtrip";
    static const uint8_t spill_group_count_sql[] =
        "SELECT left_key, COUNT(*) FROM decimal_grouped_records "
        "GROUP BY left_key";
    static const uint8_t spill_group_limit_sql[] =
        "SELECT left_key, COUNT(*) FROM decimal_grouped_records "
        "GROUP BY left_key LIMIT 2";
    static const uint8_t spill_ordered_rows_sql[] =
        "SELECT left_key FROM decimal_grouped_records "
        "ORDER BY left_key DESC";
    static const uint8_t spill_decimal_rows_sql[] =
        "SELECT decimal_value FROM decimal_grouped_records "
        "ORDER BY decimal_value DESC";
    static const uint8_t spill_join_rows_sql[] =
        "SELECT a.left_key, b.right_key FROM decimal_grouped_records AS a "
        "INNER JOIN decimal_grouped_records AS b ON "
        "a.left_key = b.left_key AND a.right_key = b.right_key";
    if (host == NULL || invocation == NULL ||
        host->v5.v4.v3.struct_size < sizeof(host->v5.v4.v3) ||
        host->v5.v4.v3.abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        invocation->abi_version != PRDB_QUERY_ABI_V6_VERSION ||
        invocation->parameters_len != 1 || invocation->parameters == NULL) {
        return 1;
    }
    if (invocation->parameters[0] == 'X') {
        static const uint8_t spill_bytes[] = "private bytes before worker crash";
        static const uint8_t provisional[] = "spill-crash-provisional";
        prdb_sdk_spill_stream_v1 stream = {0};
        if (prdb_sdk_spill_create_v1(host, &stream) != 0 ||
            prdb_sdk_spill_append_v1(host, &stream, spill_bytes,
                                     sizeof(spill_bytes) - 1) != 0 ||
            host->v5.v4.v3.write_result(host->v5.v4.v3.context, provisional,
                                        sizeof(provisional) - 1) != 0) {
            return 1;
        }
        volatile uint32_t *crash_address = (volatile uint32_t *)(uintptr_t)0;
        *crash_address = 1;
        return 1;
    }
    if (invocation->parameters[0] == 'F') {
        int32_t status = prdb_sdk_sql_run_statement_v2(
            host, schema, sizeof(schema) - 1, spill_ordered_rows_sql,
            sizeof(spill_ordered_rows_sql) - 1, NULL, 0);
        return status == PRDB_SDK_SQL_DISPATCH_OK ? 0 : 1;
    }
    prdb_sdk_spill_stream_v1 pressure_stream = {0};
    if (invocation->parameters[0] == 'P') {
        static const uint8_t pressure_bytes[PRDB_SDK_SPILL_MAX_IO_BYTES];
        static const uint8_t pressure_marker[] = "spill-pressure-started";
        if (prdb_sdk_spill_create_v1(host, &pressure_stream) != 0 ||
            prdb_sdk_spill_append_v1(host, &pressure_stream, pressure_bytes,
                                     sizeof(pressure_bytes) - 16u) != 0 ||
            prdb_sdk_spill_seal_v1(host, &pressure_stream) != 0) {
            return 1;
        }
        if (host->v5.v4.v3.write_result(host->v5.v4.v3.context, pressure_marker,
                                        sizeof(pressure_marker) - 1) != 0) {
            prdb_sdk_spill_destroy_v1(host, &pressure_stream);
            return 1;
        }
    }
    const uint8_t *sql = NULL;
    uint64_t sql_len = 0;
    if (invocation->parameters[0] == 'S') {
        sql = spill_group_count_sql;
        sql_len = sizeof(spill_group_count_sql) - 1;
    } else if (invocation->parameters[0] == 'L') {
        sql = spill_group_limit_sql;
        sql_len = sizeof(spill_group_limit_sql) - 1;
    } else if (invocation->parameters[0] == 'O' ||
               invocation->parameters[0] == 'P') {
        sql = spill_ordered_rows_sql;
        sql_len = sizeof(spill_ordered_rows_sql) - 1;
    } else if (invocation->parameters[0] == 'D' ||
               invocation->parameters[0] == 'E') {
        sql = spill_decimal_rows_sql;
        sql_len = sizeof(spill_decimal_rows_sql) - 1;
    } else if (invocation->parameters[0] == 'J') {
        sql = spill_join_rows_sql;
        sql_len = sizeof(spill_join_rows_sql) - 1;
    } else {
        return 1;
    }
    int32_t status = prdb_sdk_sql_run_statement_v2(
        host, schema, sizeof(schema) - 1, sql, sql_len, NULL, 0);
    if (invocation->parameters[0] == 'P' &&
        prdb_sdk_spill_destroy_v1(host, &pressure_stream) != 0) {
        return 1;
    }
    return status == PRDB_SDK_SQL_DISPATCH_OK ? 0 : 1;
}
