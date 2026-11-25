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

    uint8_t scan_request[PRDB_EXECUTOR_OPEN_SCAN_REQUEST_HEADER_BYTES] = {0};
    uint8_t insert_request[PRDB_EXECUTOR_STAGE_INSERT_REQUEST_HEADER_BYTES +
                           PRDB_MUTATION_CELL_TAG_RECORD_HEADER_BYTES] = {0};
    uint8_t update_request[PRDB_EXECUTOR_STAGE_UPDATE_REQUEST_HEADER_BYTES +
                           PRDB_PATCH_ORDINAL_RECORD_HEADER_BYTES +
                           PRDB_MUTATION_CELL_TAG_RECORD_HEADER_BYTES] = {0};
    uint8_t forged_handle[16] = {6};
    uint8_t response[PRDB_EXECUTOR_OPEN_SCAN_RESPONSE_HEADER_BYTES] = {0};
    uint64_t response_len = 0;
    prdb_encode_executor_open_scan_request(scan_request, forged_handle, 0);

    /* Skip the SDK and try reading and writing through a valid-shaped,
     * never-issued relation handle. The connection must reject all requests. */
    int32_t scan_status = host->executor_request(
        host->context, PRDB_IPC_OP_OPEN_SCAN, scan_request,
        sizeof(scan_request),
        response, sizeof(response), &response_len);

    prdb_encode_executor_stage_insert_request(insert_request, forged_handle,
                                              1);
    prdb_encode_mutation_cell_tag_record(
        insert_request + PRDB_EXECUTOR_STAGE_INSERT_REQUEST_HEADER_BYTES, 1);
    response_len = 0;
    int32_t insert_status = host->executor_request(
        host->context, PRDB_IPC_OP_STAGE_INSERT, insert_request,
        sizeof(insert_request), response, sizeof(response), &response_len);

    prdb_encode_executor_stage_update_request(update_request, forged_handle,
                                              1);
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

    if (scan_status == PRDB_QUERY_CALLBACK_OK ||
        insert_status == PRDB_QUERY_CALLBACK_OK ||
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
