#ifndef PRDB_SDK_SQL_BATCH_V1_H
#define PRDB_SDK_SQL_BATCH_V1_H

#include "prdb_query_abi_v1.h"

#define PRDB_SDK_SQL_BATCH_ABI_V1_VERSION UINT32_C(1)
#define PRDB_SDK_SQL_BATCH_MAX_STATEMENTS UINT32_C(64)
#define PRDB_SDK_SQL_BATCH_MAX_STATEMENT_BYTES UINT64_C(1048576)
#define PRDB_SDK_SQL_BATCH_MAX_PARAMETER_BYTES UINT64_C(1048576)
#define PRDB_SDK_SQL_BATCH_MAX_PACKAGE_BYTES UINT64_C(67108864)
#define PRDB_SDK_SQL_BATCH_RESULT_HEADER_BYTES UINT32_C(12)

#define PRDB_SDK_SQL_BATCH_OK INT32_C(0)
#define PRDB_SDK_SQL_BATCH_INVALID_ARGUMENT INT32_C(1)
#define PRDB_SDK_SQL_BATCH_LIMIT_EXCEEDED INT32_C(2)
#define PRDB_SDK_SQL_BATCH_STATEMENT_FAILED INT32_C(3)
#define PRDB_SDK_SQL_BATCH_OUTPUT_FAILED INT32_C(4)

#define PRDB_SDK_SQL_BATCH_EVENT_START UINT8_C(1)
#define PRDB_SDK_SQL_BATCH_EVENT_DATA UINT8_C(2)
#define PRDB_SDK_SQL_BATCH_EVENT_COMPLETE UINT8_C(3)
#define PRDB_SDK_SQL_BATCH_EVENT_FAILED UINT8_C(4)

typedef struct prdb_sdk_sql_batch_statement_v1 {
    const uint8_t *sql;
    uint64_t sql_len;
    const uint8_t *parameters;
    uint64_t parameters_len;
} prdb_sdk_sql_batch_statement_v1;

typedef int32_t (*prdb_sdk_sql_batch_statement_runner_v1_fn)(
    const prdb_host_api_v1 *host, const uint8_t *schema,
    uint32_t schema_len, const prdb_sdk_sql_batch_statement_v1 *statement);

/* Executes a PSQ1 SQL batch encoded in invocation->sql_data sequentially in
 * the same host invocation. The caller must use a SQL lexer/parser to
 * construct PSQ1 statement slices; this function never splits source text on
 * semicolons. Each output callback is framed as SQLB version 1 with event,
 * zero-based statement ordinal, and payload length. Any statement or output
 * failure latches the host failure callback and stops the package immediately. */
int32_t prdb_sdk_sql_run_batch_v1(
    const prdb_host_api_v1 *host, const prdb_invocation_v1 *invocation,
    prdb_sdk_sql_batch_statement_runner_v1_fn run_statement);

#endif /* PRDB_SDK_SQL_BATCH_V1_H */
