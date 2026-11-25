#include "prdb_query_abi_v1.h"
#include "prdb_query_abi_constants.h"
#include "prdb_query_abi_payload_codecs.h"

const prdb_query_descriptor_v1_record prdb_query_descriptor_v1 = {
    sizeof(prdb_query_descriptor_v1_record), PRDB_QUERY_ABI_VERSION,
    sizeof(prdb_host_api_v1), sizeof(prdb_invocation_v1)};

int32_t prdb_query_main_v1(const prdb_host_api_v1 *host,
                           const prdb_invocation_v1 *invocation) {
    if (host == NULL || invocation == NULL ||
        host->struct_size < sizeof(*host) ||
        host->abi_version != PRDB_QUERY_ABI_VERSION ||
        host->executor_request == NULL || host->statement_failure == NULL) {
        return 1;
    }

    uint8_t delete_request[PRDB_EXECUTOR_STAGE_DELETE_REQUEST_HEADER_BYTES] = {0};
    uint8_t update_request[PRDB_EXECUTOR_STAGE_UPDATE_REQUEST_HEADER_BYTES +
                           PRDB_PATCH_ORDINAL_RECORD_HEADER_BYTES +
                           PRDB_MUTATION_CELL_TAG_RECORD_HEADER_BYTES] = {0};
    uint8_t forged_handle[16] = {8};
    uint8_t response[PRDB_EXECUTOR_MUTATION_ACKNOWLEDGEMENT_HEADER_BYTES] = {0};
    uint64_t response_len = 0;
    prdb_encode_executor_stage_delete_request(delete_request, forged_handle);

    /* Skip the SDK and try to mutate through a valid-shaped, never-issued row
     * handle. The connection must reject both operations before staging a
     * mutation. */
    int32_t delete_status = host->executor_request(
        host->context, PRDB_IPC_OP_STAGE_DELETE, delete_request,
        sizeof(delete_request),
        response, sizeof(response), &response_len);

    prdb_encode_executor_stage_update_request(update_request, forged_handle, 1);
    prdb_encode_patch_ordinal_record(
        update_request + PRDB_EXECUTOR_STAGE_UPDATE_REQUEST_HEADER_BYTES, 0);
    prdb_encode_mutation_cell_tag_record(
        update_request + PRDB_EXECUTOR_STAGE_UPDATE_REQUEST_HEADER_BYTES +
            PRDB_PATCH_ORDINAL_RECORD_HEADER_BYTES,
        1);
    response_len = 0;
    int32_t update_status = host->executor_request(
        host->context, PRDB_IPC_OP_STAGE_UPDATE, update_request,
        sizeof(update_request), response, sizeof(response), &response_len);

    if (delete_status == PRDB_QUERY_CALLBACK_OK ||
        update_status == PRDB_QUERY_CALLBACK_OK) {
        return 0;
    }
    if (host->statement_failure(host->context,
                                PRDB_QUERY_STATEMENT_FAILURE_SQL) !=
        PRDB_QUERY_CALLBACK_OK) {
        return 3;
    }
    return 0;
}
