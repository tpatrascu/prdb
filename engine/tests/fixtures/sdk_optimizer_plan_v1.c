#include "prdb_sdk_optimizer_plan_v1.h"

#include <assert.h>
#include <stdint.h>
#include <string.h>

static void put_u16(uint8_t *bytes, uint32_t *offset, uint16_t value) {
    bytes[(*offset)++] = (uint8_t)(value >> 8);
    bytes[(*offset)++] = (uint8_t)value;
}

static void put_u32(uint8_t *bytes, uint32_t *offset, uint32_t value) {
    bytes[(*offset)++] = (uint8_t)(value >> 24);
    bytes[(*offset)++] = (uint8_t)(value >> 16);
    bytes[(*offset)++] = (uint8_t)(value >> 8);
    bytes[(*offset)++] = (uint8_t)value;
}

static void put_u64(uint8_t *bytes, uint32_t *offset, uint64_t value) {
    put_u32(bytes, offset, (uint32_t)(value >> 32));
    put_u32(bytes, offset, (uint32_t)value);
}

static uint32_t put_column(uint8_t *bytes, uint32_t *offset) {
    uint32_t id_offset = *offset;
    bytes[(*offset)++] = 3;
    memset(bytes + *offset, 0, 15);
    *offset += 15;
    put_u32(bytes, offset, 3);
    bytes[(*offset)++] = 6; /* TEXT */
    memset(bytes + *offset, 0, 4);
    *offset += 4;
    bytes[(*offset)++] = 1;
    return id_offset;
}

static uint32_t build_plan(uint8_t *bytes, uint32_t *projected_id_offset,
                           uint32_t *project_node_offset) {
    uint32_t offset = 0;
    bytes[offset++] = 0;
    bytes[offset++] = 1; /* version */
    bytes[offset++] = 1; /* SELECT */
    bytes[offset++] = 0; /* reserved */
    bytes[offset++] = 6;
    memcpy(bytes + offset, "public", 6);
    offset += 6;
    bytes[offset++] = 1;
    memset(bytes + offset, 0, 15);
    offset += 15;
    put_u64(bytes, &offset, 1); /* schema version */
    bytes[offset++] = 5;
    memcpy(bytes + offset, "items", 5);
    offset += 5;
    bytes[offset++] = 2;
    memset(bytes + offset, 0, 15);
    offset += 15;
    put_u64(bytes, &offset, 1); /* table epoch */
    bytes[offset++] = 3; /* Scan, Project, Limit */
    bytes[offset++] = 1;
    put_u16(bytes, &offset, 1);
    (void)put_column(bytes, &offset);
    *project_node_offset = offset;
    bytes[offset++] = 3;
    put_u16(bytes, &offset, 1);
    *projected_id_offset = put_column(bytes, &offset);
    bytes[offset++] = 4;
    put_u32(bytes, &offset, 7);
    return offset;
}

int main(void) {
    uint8_t payload[256] = {0};
    uint8_t malformed[sizeof(payload)];
    uint32_t projected_id_offset;
    uint32_t project_node_offset;
    uint32_t payload_len = build_plan(payload, &projected_id_offset,
                                      &project_node_offset);
    static prdb_sdk_optimizer_plan_v1 decoded;

    assert(prdb_sdk_optimizer_decode_plan_v1(
               payload, payload_len, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    assert(decoded.struct_size == sizeof(decoded));
    assert(decoded.abi_version == PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION);
    assert(decoded.schema_name_len == 6);
    assert(memcmp(decoded.schema_name, "public", 6) == 0);
    assert(decoded.relation_name_len == 5);
    assert(memcmp(decoded.relation_name, "items", 5) == 0);
    assert(decoded.schema_version == 1 && decoded.table_epoch == 1);
    assert(decoded.scan_column_count == 1 && decoded.projection_count == 1);
    assert(decoded.scan_columns[0].ordinal == 3);
    assert(decoded.projections[0].type_code == 6);
    assert(decoded.has_limit == 1 && decoded.limit == 7);

    memcpy(malformed, payload, payload_len);
    malformed[projected_id_offset] = 9;
    assert(prdb_sdk_optimizer_decode_plan_v1(
               malformed, payload_len, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    memcpy(malformed, payload, payload_len);
    malformed[project_node_offset] = 2;
    assert(prdb_sdk_optimizer_decode_plan_v1(
               malformed, payload_len, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED);

    memcpy(malformed, payload, payload_len);
    malformed[payload_len] = 0;
    assert(prdb_sdk_optimizer_decode_plan_v1(
               malformed, payload_len + 1, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD);

    assert(prdb_sdk_optimizer_decode_plan_v1(
               payload, payload_len, &decoded, sizeof(decoded) - 1) ==
           PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT);
    assert(prdb_sdk_optimizer_decode_plan_v1(
               payload, payload_len, &decoded, sizeof(decoded)) ==
           PRDB_SDK_OPTIMIZER_PLAN_OK);
    return 0;
}
