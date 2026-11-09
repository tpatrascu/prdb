#ifndef PRDB_QUERY_ABI_V1_H
#define PRDB_QUERY_ABI_V1_H

#include <stdint.h>
#include "prdb_query_abi_constants.h"

#include "prdb_query_abi_records_v1.h"

typedef int32_t (*prdb_query_main_v1_fn)(const prdb_host_api_v1 *host,
                                         const prdb_invocation_v1 *invocation);

extern const prdb_query_descriptor_v1_record prdb_query_descriptor_v1;
int32_t prdb_query_main_v1(const prdb_host_api_v1 *host,
                           const prdb_invocation_v1 *invocation);

#endif /* PRDB_QUERY_ABI_V1_H */
