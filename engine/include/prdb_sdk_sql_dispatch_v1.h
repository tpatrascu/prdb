#ifndef PRDB_SDK_SQL_DISPATCH_V1_H
#define PRDB_SDK_SQL_DISPATCH_V1_H

#include "prdb_sdk_sql_exec_v2.h"
#include "prdb_sdk_sql_result_v1.h"

#define PRDB_SDK_SQL_DISPATCH_ABI_V1_VERSION UINT32_C(1)
#define PRDB_SDK_SQL_DISPATCH_MAX_PARAMETER_BYTES UINT64_C(1048576)

#define PRDB_SDK_SQL_DISPATCH_OK INT32_C(0)
#define PRDB_SDK_SQL_DISPATCH_INVALID_ARGUMENT INT32_C(1)
#define PRDB_SDK_SQL_DISPATCH_INVALID_PARAMETERS INT32_C(2)
#define PRDB_SDK_SQL_DISPATCH_RESOURCE_ERROR INT32_C(3)
#define PRDB_SDK_SQL_DISPATCH_EXECUTION_ERROR INT32_C(4)

/* Decodes a canonical PSP1 block into borrowed payloads and type metadata.
 * Fixed-width signed payloads retain PSP1's little-endian representation.
 * The statement dispatchers normalize these before calling SDK runners. */
int32_t prdb_sdk_sql_decode_parameters_v1(
    const uint8_t *parameters, uint64_t parameters_len,
    prdb_sdk_value_v1 *values, uint32_t values_capacity,
    prdb_sdk_sql_parameter_metadata_v1 *metadata,
    uint32_t metadata_capacity, uint32_t *parameter_count_out);

/* Runs one row SELECT, supported single-key/composite grouped SELECT, a
 * single-key INNER JOIN, or an alias-aware filtered INNER JOIN through
 * bounded executor APIs. Invalid PSP1/SQL input and execution errors latch
 * failure. */
int32_t prdb_sdk_sql_run_v1(
    const prdb_host_api_v3 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const uint8_t *parameters, uint64_t parameters_len);

/* Runs one supported row/grouped/joined SELECT or DML statement. INSERT v3,
 * UPDATE v1, and DELETE v1 use bounded mutation APIs; SELECT input is
 * delegated to the SELECT entry point. Errors latch statement failure. */
int32_t prdb_sdk_sql_run_statement_v1(
    const prdb_host_api_v3 *host, const uint8_t *schema,
    uint32_t schema_len, const uint8_t *sql, uint64_t sql_len,
    const uint8_t *parameters, uint64_t parameters_len);

#endif /* PRDB_SDK_SQL_DISPATCH_V1_H */
