/* common.h
 *
 * Tiny shared helpers for the example query programs in this directory.
 * Nothing here is part of the SDK; it exists only to avoid repeating the
 * same few lines of boilerplate in every example.
 */
#ifndef PRDB_EXAMPLE_COMMON_H
#define PRDB_EXAMPLE_COMMON_H

#include "prdb_query_abi_v1.h"

#include <stddef.h>
#include <stdint.h>

/* Every query entry point must reject a host it cannot safely call back
 * into before touching any SDK function. A disposable confined executor
 * always supplies a valid host/invocation pair, but a malformed descriptor
 * or an older/newer host would not. */
static inline int prdb_example_require_host(const prdb_host_api_v1 *host,
                                             const prdb_invocation_v1 *invocation) {
    return host != NULL && invocation != NULL &&
           host->struct_size >= sizeof(*host) &&
           host->abi_version == PRDB_QUERY_ABI_VERSION &&
           host->executor_request != NULL && host->planner_request != NULL &&
           host->write_result != NULL && host->statement_failure != NULL;
}

/* Writes one chunk of provisional application output. This can never
 * finalize or abort the invocation; a nonzero SQL/storage status is still
 * what determines the durable outcome. Callers with more than
 * PRDB_QUERY_MAX_RESULT_CHUNK_BYTES to emit at once must split it into
 * several calls; every example here stays well under that limit. */
static inline int prdb_example_write_text(const prdb_host_api_v1 *host,
                                           const char *text, size_t length) {
    return host->write_result(host->context, (const uint8_t *)text,
                               (uint64_t)length) == PRDB_QUERY_CALLBACK_OK
               ? 0
               : 1;
}

#endif /* PRDB_EXAMPLE_COMMON_H */
