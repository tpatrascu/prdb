/* mixed_insert_and_verify.c
 *
 * Demonstrates mixing the SQL convenience layer with direct low-level
 * storage calls in one program, as specs/client-sdk.md anticipates: "The
 * user program may call SQL, process authorized results, or use
 * lower-level scoped storage APIs." Both calls share the same invocation
 * transaction, so the row this program inserts through SQL is visible to
 * the direct scan that follows it, before anything commits.
 *
 * Schema prerequisite (see README.md for the exact commands):
 *   CREATE TABLE public.widgets (id INTEGER NOT NULL, name TEXT)
 *
 * Submitted parameters: two PSP1-encoded values, (INTEGER id, TEXT name)
 * matching $1/$2 below. Build them with:
 *   prdb-client parameters-encode --input - --output params.psp1 <<'JSON'
 *   {"format":"PDB-SQL-PARAMETERS-1","values":[
 *     {"type":"integer","value":"42"},
 *     {"type":"text","value":"left-handed widget"}
 *   ]}
 *   JSON
 */

#include "prdb_sdk_sql_dispatch_v1.h"
#include "prdb_sdk_executor_v1.h"
#include "prdb_sdk_batch_v1.h"
#include "prdb_sdk_value_v1.h"
#include "prdb_query_abi_v1.h"

#include "common.h"

#include <stdio.h>

const prdb_query_descriptor_v1_record prdb_query_descriptor_v1 = {
    sizeof(prdb_query_descriptor_v1_record), PRDB_QUERY_ABI_VERSION,
    sizeof(prdb_host_api_v1), sizeof(prdb_invocation_v1)};

static int emit_row(const prdb_host_api_v1 *host, int32_t id,
                    const uint8_t *name, uint64_t name_len) {
    char line[320];
    int written = snprintf(line, sizeof(line), "id=%d name=%.*s\n", id,
                           (int)name_len, (const char *)name);
    if (written <= 0 || (size_t)written >= sizeof(line)) {
        return 1;
    }
    return prdb_example_write_text(host, line, (size_t)written);
}

int32_t prdb_query_main_v1(const prdb_host_api_v1 *host,
                           const prdb_invocation_v1 *invocation) {
    static const uint8_t schema[] = "public";
    static const uint8_t relation_name[] = "widgets";
    static const uint8_t insert_sql[] =
        "INSERT INTO widgets (id, name) VALUES ($1, $2)";
    uint8_t resolve_response[4096];
    uint64_t resolve_response_len = 0;
    prdb_sdk_executor_relation_v1 relation = {0};
    prdb_sdk_sql_column_shape_v1 columns[8] = {{0}};
    uint8_t scan_response[4096];
    uint64_t scan_response_len = 0;
    uint8_t cursor[16] = {0};
    uint8_t batch_response[65536];
    uint64_t batch_response_len = 0;
    prdb_sdk_batch_v1 batch = {0};
    prdb_sdk_row_iterator_v1 rows;
    prdb_sdk_row_v1 row;
    int32_t status;

    if (!prdb_example_require_host(host, invocation)) {
        return 1;
    }

    /* Step 1: write through SQL. prdb_sdk_sql_run_statement_v1 parses,
     * binds, validates, and stages the INSERT in one call; a failure here
     * already latches the sticky statement-failure callback. */
    status = prdb_sdk_sql_run_statement_v1(
        host, schema, sizeof(schema) - 1, insert_sql, sizeof(insert_sql) - 1,
        invocation->parameters, invocation->parameters_len);
    if (status != PRDB_SDK_SQL_DISPATCH_OK) {
        return 1;
    }

    /* Step 2: read back through the low-level storage API, in the same
     * invocation transaction, so the private write from step 1 is already
     * visible even though nothing has committed yet. */
    if (prdb_sdk_executor_resolve_relation_v1(
            host, schema, sizeof(schema) - 1, relation_name,
            sizeof(relation_name) - 1, resolve_response,
            sizeof(resolve_response), &resolve_response_len, &relation,
            sizeof(relation), columns, 8) != PRDB_SDK_EXECUTOR_OK) {
        return 1;
    }
    if (prdb_sdk_executor_open_full_scan_v1(
            host, relation.executor_handle, scan_response,
            sizeof(scan_response), &scan_response_len,
            cursor) != PRDB_SDK_EXECUTOR_OK) {
        return 1;
    }

    for (;;) {
        if (prdb_sdk_executor_next_batch_v1(
                host, cursor, PRDB_SDK_BATCH_MAX_ROWS, sizeof(batch_response),
                batch_response, sizeof(batch_response), &batch_response_len,
                &batch, sizeof(batch)) != PRDB_SDK_EXECUTOR_OK ||
            prdb_sdk_batch_rows_begin_v1(&batch, &rows, sizeof(rows)) != PRDB_SDK_BATCH_OK) {
            prdb_sdk_executor_close_cursor_v1(host, cursor, scan_response,
                                              sizeof(scan_response),
                                              &scan_response_len);
            return 1;
        }
        while (prdb_sdk_batch_row_next_v1(&rows, &row, sizeof(row)) == PRDB_SDK_BATCH_OK) {
            prdb_sdk_cell_iterator_v1 cells;
            prdb_sdk_value_v1 id_value = {0};
            prdb_sdk_value_v1 name_value = {0};
            int32_t id = 0;
            const uint8_t *name_bytes = NULL;
            uint64_t name_len = 0;

            if (prdb_sdk_row_cells_begin_v1(&row, &cells, sizeof(cells)) != PRDB_SDK_BATCH_OK ||
                prdb_sdk_cell_next_v1(&cells, &id_value, sizeof(id_value)) != PRDB_SDK_BATCH_OK ||
                prdb_sdk_cell_next_v1(&cells, &name_value, sizeof(name_value)) != PRDB_SDK_BATCH_OK ||
                prdb_sdk_value_get_integer_v1(&id_value, &id) != PRDB_SDK_VALUE_VALID ||
                prdb_sdk_value_get_bytes_v1(&name_value, &name_bytes, &name_len) !=
                    PRDB_SDK_VALUE_VALID) {
                prdb_sdk_executor_close_cursor_v1(host, cursor, scan_response,
                                                  sizeof(scan_response),
                                                  &scan_response_len);
                return 1;
            }
            if (emit_row(host, id, name_bytes, name_len) != 0) {
                prdb_sdk_executor_close_cursor_v1(host, cursor, scan_response,
                                                  sizeof(scan_response),
                                                  &scan_response_len);
                return 1;
            }
        }
        if (batch.finished) {
            break;
        }
    }

    /* Nothing is durable yet: this scan only proves the write is visible
     * inside the invocation. Whether it survives is decided by storage's
     * commit outcome after this function returns, not by anything here. */
    return prdb_sdk_executor_close_cursor_v1(host, cursor, scan_response,
                                             sizeof(scan_response),
                                             &scan_response_len) == PRDB_SDK_EXECUTOR_OK
               ? 0
               : 1;
}
