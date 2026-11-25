#include "prdb_sdk_sql_exec_v1.h"

#include <stddef.h>

extern int prdb_missing_dependency(void);

const prdb_query_descriptor_v3_record prdb_query_descriptor_v3 = {
    sizeof(prdb_query_descriptor_v3_record), PRDB_QUERY_ABI_V3_VERSION,
    sizeof(prdb_host_api_v3), sizeof(prdb_invocation_v3)};

int32_t prdb_query_main_v3(const prdb_host_api_v3 *host,
                           const prdb_invocation_v3 *invocation) {
    (void)invocation;
    (void)prdb_sdk_sql_execute_select_v1(
        host, NULL, 0, NULL, 0, NULL, 0, 0, 0, NULL, 0, NULL, 0, NULL, 0,
        NULL, 0, NULL, 0, NULL, 0, NULL, NULL);
    return prdb_missing_dependency();
}
