#include "prdb_sdk_sql_exec_v2.h"

#include <fcntl.h>
#include <string.h>
#ifdef PRDB_PROBE_CONSTRUCTOR_NETWORK
#include <netinet/in.h>
#include <stdlib.h>
#include <sys/socket.h>
#endif
#include <unistd.h>

const prdb_query_descriptor_v3_record prdb_query_descriptor_v3 = {
    sizeof(prdb_query_descriptor_v3_record), PRDB_QUERY_ABI_V3_VERSION,
    sizeof(prdb_host_api_v3), sizeof(prdb_invocation_v3)};

static int constructor_write_allowed;
#ifdef PRDB_PROBE_CONSTRUCTOR_NETWORK
static int constructor_connection_allowed;
#endif

__attribute__((constructor)) static void probe_constructor_filesystem_access(
    void) {
#ifdef PRDB_PROBE_CONSTRUCTOR_NETWORK
    static const char marker_path[] =
        "/tmp/prdb-sandbox-network-constructor-probe";
#else
    static const char marker_path[] = "/tmp/prdb-sandbox-constructor-probe";
#endif
    static const char marker_byte = 'x';
    int fd = open(marker_path, O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (fd >= 0) {
        constructor_write_allowed = write(fd, &marker_byte, 1) == 1;
        (void)close(fd);
    }
#ifdef PRDB_PROBE_CONSTRUCTOR_NETWORK
    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd >= 0) {
        static const char probe_port[] = "PRDB_PORT_00000";
        struct sockaddr_in address = {0};
        address.sin_family = AF_INET;
        address.sin_port = htons((uint16_t)strtoul(probe_port + 10, NULL, 10));
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        constructor_connection_allowed =
            connect(fd, (const struct sockaddr *)&address, sizeof(address)) ==
            0;
        (void)close(fd);
    }
#endif
}

static int32_t discard_row(void *context, const prdb_sdk_row_v1 *row,
                           const prdb_sdk_select_binding_v2 *binding) {
    (void)context;
    (void)row;
    (void)binding;
    return 0;
}

int32_t prdb_query_main_v3(const prdb_host_api_v3 *host,
                           const prdb_invocation_v3 *invocation) {
    static const uint8_t partial_output[] = "partial SQL output";
    static const uint8_t constructor_escape[] =
        "constructor filesystem access allowed";
#ifdef PRDB_PROBE_CONSTRUCTOR_NETWORK
    static const uint8_t socket_escape[] =
        "constructor network connection allowed";
    static const uint8_t socket_blocked[] =
        "constructor network connection blocked";
#endif
    static const uint8_t schema[] = "public";
    static const uint8_t invalid_sql[] = "DELETE FROM records";
    uint8_t planner_response[256];
    uint8_t executor_response[256];
    uint8_t batch_response[8];
    prdb_sdk_sql_column_shape_v2 planner_columns[1];
    prdb_sdk_sql_column_shape_v2 executor_columns[1];
    prdb_sdk_value_v1 cell_scratch[1];
    const uint8_t *first_output = partial_output;
    uint64_t first_output_len = sizeof(partial_output) - 1;

    (void)invocation;
    if (host == NULL || host->struct_size < sizeof(*host) ||
        host->abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->write_result == NULL) {
        return 1;
    }
    if (constructor_write_allowed) {
        first_output = constructor_escape;
        first_output_len = sizeof(constructor_escape) - 1;
#ifdef PRDB_PROBE_CONSTRUCTOR_NETWORK
    } else if (constructor_connection_allowed) {
        first_output = socket_escape;
        first_output_len = sizeof(socket_escape) - 1;
    } else {
        first_output = socket_blocked;
        first_output_len = sizeof(socket_blocked) - 1;
#endif
    }
    if (host->write_result(host->context, first_output, first_output_len) != 0) {
        return 1;
    }

    /* Intentionally catch the SQL API error and return success. The v3 host
     * latch must still make the executor process fail after this entrypoint. */
    (void)prdb_sdk_sql_execute_select_v2(
        host, schema, sizeof(schema) - 1, invalid_sql, sizeof(invalid_sql) - 1,
        NULL, 0, 1, sizeof(batch_response), planner_response,
        sizeof(planner_response), executor_response, sizeof(executor_response),
        batch_response, sizeof(batch_response), planner_columns, 1,
        executor_columns, 1, cell_scratch, 1, discard_row, NULL);
    return 0;
}
