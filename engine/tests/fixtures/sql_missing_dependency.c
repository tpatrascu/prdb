#include "prdb_sdk_sql_exec_v1.h"

#include <stddef.h>

extern int prdb_missing_dependency(void);

const prdb_query_descriptor_v1_record prdb_query_descriptor_v1 = {
    sizeof(prdb_query_descriptor_v1_record), PRDB_QUERY_ABI_VERSION,
    sizeof(prdb_host_api_v1), sizeof(prdb_invocation_v1)};

int32_t prdb_query_main_v1(const prdb_host_api_v1 *host,
                           const prdb_invocation_v1 *invocation) {
    (void)invocation;
    (void)prdb_sdk_sql_execute_select_v1(
        host, NULL, 0, NULL, 0, NULL, 0, 0, 0, NULL, 0, NULL, 0, NULL, 0,
        NULL, 0, NULL, 0, NULL, 0, NULL, NULL);
    return prdb_missing_dependency();
}
