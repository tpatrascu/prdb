/* hello_sql.c
 *
 * The simplest possible custom query program: call the SQL SDK directly
 * from hand-written C instead of going through `prdb-client interactive-sql`
 * (which submits plain SQL text with no compiled program at all). Write a
 * program like this when you need host-side logic around a query that the
 * plain-SQL-text path can't express -- for example, deciding which of
 * several statements to run based on the invocation's parameters, or
 * mixing SQL with direct storage calls (see mixed_insert_and_verify.c).
 *
 * This program runs one parameterized SELECT and lets the SQL dispatcher
 * emit its result rows directly through the host's write_result callback.
 * That output is the binary SQLR v1 wire format documented in
 * prdb_sdk_sql_result_v1.h and specs/sql-library.md -- not text. A real
 * client decodes SQLR itself, the same way prdb-client's own interactive
 * SQL path used to. If you want output you can read directly at a
 * terminal without writing a decoder, see direct_scan.c instead.
 *
 * Schema prerequisite (see README.md for the exact commands):
 *   CREATE TABLE public.widgets (id INTEGER NOT NULL, name TEXT)
 *
 * Submitted parameters: one PSP1-encoded INTEGER, matching $1 below. Build
 * it with:
 *   prdb-client parameters-encode --input - --output params.psp1 <<'JSON'
 *   {"format":"PDB-SQL-PARAMETERS-1","values":[{"type":"integer","value":"1"}]}
 *   JSON
 */

#include "prdb_sdk_sql_dispatch_v1.h"
#include "prdb_query_abi_v1.h"

#include "common.h"

const prdb_query_descriptor_v1_record prdb_query_descriptor_v1 = {
    sizeof(prdb_query_descriptor_v1_record), PRDB_QUERY_ABI_VERSION,
    sizeof(prdb_host_api_v1), sizeof(prdb_invocation_v1)};

int32_t prdb_query_main_v1(const prdb_host_api_v1 *host,
                           const prdb_invocation_v1 *invocation) {
    static const uint8_t schema[] = "public";
    static const uint8_t sql[] = "SELECT id, name FROM widgets WHERE id = $1";

    if (!prdb_example_require_host(host, invocation)) {
        return 1;
    }

    /* prdb_sdk_sql_run_statement_v1 parses, binds, authorizes, executes, and
     * emits results for one supported SELECT/INSERT/UPDATE/DELETE statement
     * in a single call. It decodes `parameters` as a PSP1 block itself; we
     * never need to touch prdb_sdk_value_v1 directly for this shape. */
    int32_t status = prdb_sdk_sql_run_statement_v1(
        host, schema, sizeof(schema) - 1, sql, sizeof(sql) - 1,
        invocation->parameters, invocation->parameters_len);

    /* A nonzero status means the statement failed, and the dispatcher has
     * already latched that failure through host->statement_failure. The
     * executor aborts the invocation after this function returns regardless
     * of what we return here; the return value only selects the local exit
     * code the supervisor observes. */
    return status == PRDB_SDK_SQL_DISPATCH_OK ? 0 : 1;
}
