/* direct_scan.c
 *
 * Reads an authorized table using only the low-level storage SDK -- no SQL
 * parsing, binding, or SQLR encoding anywhere in this file. This is the
 * "scoped lower-level storage API" path mentioned in specs/client-sdk.md:
 * resolve a relation, open a scan, decode batches, and read typed cells
 * directly. Because we format the output ourselves, it is plain text you
 * can read straight off the terminal after `prdb-client submit`.
 *
 * Schema prerequisite (see README.md for the exact commands):
 *   CREATE TABLE public.widgets (id INTEGER NOT NULL, name TEXT)
 *
 * This program takes no parameters; it prints every row.
 */

#include "prdb_sdk_executor_v1.h"
#include "prdb_sdk_batch_v1.h"
#include "prdb_sdk_value_v1.h"
#include "prdb_query_abi_v1.h"

#include "common.h"

#include <stdio.h>
#include <string.h>

const prdb_query_descriptor_v1_record prdb_query_descriptor_v1 = {
    sizeof(prdb_query_descriptor_v1_record), PRDB_QUERY_ABI_VERSION,
    sizeof(prdb_host_api_v1), sizeof(prdb_invocation_v1)};

/* Formats one widgets row as "id=<int> name=<text|NULL>\n" and writes it.
 * Returns 0 on success, matching this file's other helpers. */
static int format_and_emit_row(const prdb_host_api_v1 *host,
                                const prdb_sdk_row_v1 *row) {
    prdb_sdk_cell_iterator_v1 cells;
    prdb_sdk_value_v1 id_value = {0};
    prdb_sdk_value_v1 name_value = {0};
    int32_t id = 0;
    const uint8_t *name_bytes = NULL;
    uint64_t name_len = 0;
    uint32_t name_state = PRDB_SDK_VALUE_STATE_NULL;
    char line[320];
    int written;

    if (prdb_sdk_row_cells_begin_v1(row, &cells, sizeof(cells)) != PRDB_SDK_BATCH_OK ||
        /* Column order matches the resolved relation's declared schema
         * order: id (ordinal 0), name (ordinal 1). */
        prdb_sdk_cell_next_v1(&cells, &id_value, sizeof(id_value)) != PRDB_SDK_BATCH_OK ||
        prdb_sdk_cell_next_v1(&cells, &name_value, sizeof(name_value)) != PRDB_SDK_BATCH_OK ||
        prdb_sdk_value_get_integer_v1(&id_value, &id) != PRDB_SDK_VALUE_VALID ||
        prdb_sdk_value_state_v1(&name_value, &name_state) != PRDB_SDK_VALUE_VALID) {
        return 1;
    }
    if (name_state == PRDB_SDK_VALUE_STATE_VALUE) {
        if (prdb_sdk_value_get_bytes_v1(&name_value, &name_bytes, &name_len) !=
            PRDB_SDK_VALUE_VALID) {
            return 1;
        }
    } else if (name_state == PRDB_SDK_VALUE_STATE_REDACTED) {
        /* A policy hid this cell from the authenticated principal. Treat it
         * like any other application-visible redaction; never guess or
         * substitute a real value. */
        name_bytes = (const uint8_t *)"<redacted>";
        name_len = 10;
    }
    /* name_state == NULL falls through with name_bytes == NULL, printed as
     * an empty field below via the "%.*s" precision-0 case. */

    written = snprintf(line, sizeof(line), "id=%d name=%.*s\n", id,
                       (int)name_len, name_bytes != NULL ? (const char *)name_bytes : "");
    if (written <= 0 || (size_t)written >= sizeof(line)) {
        return 1;
    }
    return prdb_example_write_text(host, line, (size_t)written);
}

int32_t prdb_query_main_v1(const prdb_host_api_v1 *host,
                           const prdb_invocation_v1 *invocation) {
    static const uint8_t schema[] = "public";
    static const uint8_t relation_name[] = "widgets";
    uint8_t resolve_response[4096];
    uint64_t resolve_response_len = 0;
    prdb_sdk_executor_relation_v1 relation = {0};
    prdb_sdk_sql_column_shape_v1 columns[8] = {{0}};
    uint8_t scan_response[4096];
    uint64_t scan_response_len = 0;
    uint8_t cursor[16] = {0};

    if (!prdb_example_require_host(host, invocation)) {
        return 1;
    }

    /* Resolving by name is itself an authorization check: an unauthorized
     * or absent table returns the same bounded error either way. The
     * returned executor_handle is a connection-local capability, not the
     * same handle the (separate) planner channel would hand back. */
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
        /* Every row this host returns has already passed row/cell
         * authorization; there is no separate permission check to apply
         * here. A real operator should also poll cancellation between
         * pages on a long scan -- omitted here only because a tiny demo
         * table scans in one page. */
        uint8_t batch_response[65536];
        uint64_t batch_response_len = 0;
        prdb_sdk_batch_v1 batch = {0};
        prdb_sdk_row_iterator_v1 rows;
        prdb_sdk_row_v1 row;

        if (prdb_sdk_executor_next_batch_v1(
                host, cursor, PRDB_SDK_BATCH_MAX_ROWS, sizeof(batch_response),
                batch_response, sizeof(batch_response), &batch_response_len,
                &batch, sizeof(batch)) != PRDB_SDK_EXECUTOR_OK) {
            prdb_sdk_executor_close_cursor_v1(host, cursor, scan_response,
                                              sizeof(scan_response),
                                              &scan_response_len);
            return 1;
        }

        if (prdb_sdk_batch_rows_begin_v1(&batch, &rows, sizeof(rows)) != PRDB_SDK_BATCH_OK) {
            prdb_sdk_executor_close_cursor_v1(host, cursor, scan_response,
                                              sizeof(scan_response),
                                              &scan_response_len);
            return 1;
        }
        while (prdb_sdk_batch_row_next_v1(&rows, &row, sizeof(row)) == PRDB_SDK_BATCH_OK) {
            if (format_and_emit_row(host, &row) != 0) {
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

    return prdb_sdk_executor_close_cursor_v1(host, cursor, scan_response,
                                             sizeof(scan_response),
                                             &scan_response_len) == PRDB_SDK_EXECUTOR_OK
               ? 0
               : 1;
}
