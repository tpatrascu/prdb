#include "optimizer_plan_codec_internal.h"

int32_t prdb_sdk_optimizer_validate_executor_group_count_relation_v1(
    const prdb_host_api_v2 *host, const uint8_t *plan_payload,
    uint64_t plan_payload_len, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len) {
    prdb_sdk_optimizer_group_count_plan_v1 plan = {0};
    int32_t status;
    if (plan_payload == NULL) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    status = prdb_sdk_optimizer_decode_group_count_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    return prdb_sdk_optimizer_validate_executor_relation_v1(
        host, plan_payload, plan_payload_len, response, response_capacity,
        response_len);
}

int32_t prdb_sdk_optimizer_validate_executor_composite_group_count_relation_v1(
    const prdb_host_api_v2 *host, const uint8_t *plan_payload,
    uint64_t plan_payload_len, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len) {
    prdb_sdk_optimizer_composite_group_count_plan_v1 plan = {0};
    int32_t status;
    if (plan_payload == NULL) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    status = prdb_sdk_optimizer_decode_composite_group_count_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    return prdb_sdk_optimizer_validate_executor_relation_v1(
        host, plan_payload, plan_payload_len, response, response_capacity,
        response_len);
}

PRDB_SDK_INTERNAL uint16_t prdb_sdk_internal_response_u16(const uint8_t *bytes, uint64_t offset) {
    return (uint16_t)(((uint16_t)bytes[offset] << 8) | bytes[offset + 1]);
}

PRDB_SDK_INTERNAL uint32_t prdb_sdk_internal_response_u32(const uint8_t *bytes, uint64_t offset) {
    return ((uint32_t)bytes[offset] << 24) |
           ((uint32_t)bytes[offset + 1] << 16) |
           ((uint32_t)bytes[offset + 2] << 8) | bytes[offset + 3];
}

static uint64_t response_u64(const uint8_t *bytes, uint64_t offset) {
    return ((uint64_t)prdb_sdk_internal_response_u32(bytes, offset) << 32) |
           prdb_sdk_internal_response_u32(bytes, offset + 4);
}

static int response_identifier(const uint8_t *bytes, uint32_t length) {
    return prdb_sdk_internal_valid_identifier(bytes, length);
}

int32_t prdb_sdk_optimizer_validate_executor_relation_v1(
    const prdb_host_api_v2 *host, const uint8_t *plan_payload,
    uint64_t plan_payload_len, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len) {
    prdb_sdk_optimizer_plan_v1 plan = {0};
    prdb_sdk_optimizer_filter_v1 filter = {0};
    uint8_t request[6 + 2 * 63];
    uint64_t offset;
    uint32_t schema_len;
    uint32_t relation_len;
    uint32_t column_count;
    uint32_t scan_index = 0;
    uint8_t relation_status;
    uint8_t response_schema_id[16];
    uint8_t response_relation_id[16];
    uint64_t response_schema_version;
    uint64_t response_table_epoch;
    int32_t callback_status;
    int32_t plan_status;

    if (host == NULL || host->struct_size < sizeof(*host) ||
        host->abi_version != PRDB_QUERY_ABI_V2_VERSION ||
        host->executor_request == NULL || plan_payload == NULL ||
        response == NULL || response_len == NULL ||
        response_capacity > PRDB_QUERY_MAX_IPC_RESPONSE_BYTES ||
        response_capacity < 3 ||
        plan_payload_len > PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES ||
        prdb_sdk_internal_ranges_overlap(plan_payload, plan_payload_len, response,
                       response_capacity)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    plan_status = prdb_sdk_internal_decode_any_optimizer_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan), &filter,
        sizeof(filter));
    if (plan_status != PRDB_SDK_OPTIMIZER_PLAN_OK) return plan_status;

    schema_len = plan.schema_name_len;
    relation_len = plan.relation_name_len;
    prdb_encode_executor_resolve_relation_request(
        request, (uint16_t)schema_len, (uint16_t)relation_len);
    memcpy(request + 6, plan.schema_name, schema_len);
    memcpy(request + 6 + schema_len, plan.relation_name, relation_len);
    *response_len = 0;
    callback_status = host->executor_request(
        host->context, PRDB_IPC_OP_RESOLVE_PLAN_RELATION, request,
        6 + schema_len + relation_len, response, response_capacity,
        response_len);
    if (callback_status == PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL) {
        return PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL;
    }
    if (callback_status != PRDB_QUERY_CALLBACK_OK ||
        *response_len > response_capacity || *response_len < 3) {
        return PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
    }
    if (prdb_decode_planner_describe_plan_relation_unavailable_response(
            response, (size_t)*response_len, &relation_status) &&
        relation_status == 1) {
        return PRDB_SDK_OPTIMIZER_PLAN_UNAVAILABLE;
    }
    if (!prdb_decode_planner_describe_plan_relation_available_response(
            response, (size_t)*response_len, &relation_status,
            response_schema_id, &response_schema_version,
            &response_table_epoch, response_relation_id) ||
        relation_status != 0 || *response_len < 55) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (memcmp(response_schema_id, plan.schema_id, 16) != 0 ||
        response_schema_version != plan.schema_version ||
        response_table_epoch != plan.table_epoch ||
        memcmp(response_relation_id, plan.relation_id, 16) != 0) {
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    offset = 51;
    if (offset + 2 > *response_len) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    relation_len = prdb_sdk_internal_response_u16(response, offset);
    offset += 2;
    if (relation_len == 0 || relation_len > 63 ||
        offset + relation_len + 2 > *response_len ||
        !response_identifier(response + offset, relation_len)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (relation_len != plan.relation_name_len ||
        memcmp(response + offset, plan.relation_name, relation_len) != 0) {
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    offset += relation_len;
    column_count = prdb_sdk_internal_response_u16(response, offset);
    offset += 2;
    if (column_count == 0 || column_count > 4096) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    for (uint32_t ordinal = 0; ordinal < column_count; ++ordinal) {
        uint32_t column_ordinal;
        uint8_t nullable;
        uint8_t type_code;
        const uint8_t *column_id;
        const uint8_t *parameters;
        uint32_t column_name_len;
        if (offset + 28 > *response_len) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        column_id = response + offset;
        if (prdb_sdk_internal_all_zero(column_id, 16)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        offset += 16;
        column_ordinal = prdb_sdk_internal_response_u32(response, offset);
        offset += 4;
        nullable = response[offset++];
        type_code = response[offset++];
        parameters = response + offset;
        offset += 4;
        if (column_ordinal != ordinal || nullable > 1 ||
            !prdb_sdk_internal_valid_type(type_code, parameters) || offset + 2 > *response_len) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        column_name_len = prdb_sdk_internal_response_u16(response, offset);
        offset += 2;
        if (column_name_len == 0 || column_name_len > 63 ||
            offset + column_name_len > *response_len ||
            !response_identifier(response + offset, column_name_len)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        offset += column_name_len;
        if (scan_index < plan.scan_column_count &&
            plan.scan_columns[scan_index].ordinal < ordinal) {
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        }
        if (scan_index < plan.scan_column_count &&
            plan.scan_columns[scan_index].ordinal == ordinal) {
            const prdb_sdk_optimizer_plan_column_v1 *expected =
                &plan.scan_columns[scan_index++];
            if (memcmp(column_id, expected->column_id, 16) != 0 ||
                type_code != expected->type_code || nullable != expected->nullable ||
                memcmp(parameters, expected->type_parameters, 4) != 0) {
                return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
            }
        }
    }
    if (offset != *response_len) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    if (scan_index != plan.scan_column_count) {
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

static int32_t validate_join_relation_identity(
    const prdb_host_api_v2 *host, const uint8_t *schema_name,
    uint32_t schema_name_len, const uint8_t schema_id[16],
    uint64_t schema_version,
    const prdb_sdk_optimizer_join_relation_v1 *plan_relation,
    uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len) {
    uint8_t request[6 + 2 * 63];
    uint8_t relation_status;
    uint8_t response_schema_id[16];
    uint8_t response_relation_id[16];
    uint64_t response_schema_version;
    uint64_t response_table_epoch;
    uint64_t offset;
    uint32_t column_count;
    uint32_t scan_index = 0;
    int32_t callback_status;

    prdb_encode_executor_resolve_relation_request(
        request, (uint16_t)schema_name_len,
        (uint16_t)plan_relation->relation_name_len);
    memcpy(request + 6, schema_name, schema_name_len);
    memcpy(request + 6 + schema_name_len, plan_relation->relation_name,
           plan_relation->relation_name_len);
    *response_len = 0;
    callback_status = host->executor_request(
        host->context, PRDB_IPC_OP_RESOLVE_PLAN_RELATION, request,
        6 + schema_name_len + plan_relation->relation_name_len, response,
        response_capacity, response_len);
    if (callback_status == PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL)
        return PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL;
    if (callback_status != PRDB_QUERY_CALLBACK_OK ||
        *response_len > response_capacity || *response_len < 3)
        return PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
    if (prdb_decode_planner_describe_plan_relation_unavailable_response(
            response, (size_t)*response_len, &relation_status) &&
        relation_status == 1)
        return PRDB_SDK_OPTIMIZER_PLAN_UNAVAILABLE;
    if (!prdb_decode_planner_describe_plan_relation_available_response(
            response, (size_t)*response_len, &relation_status,
            response_schema_id, &response_schema_version,
            &response_table_epoch, response_relation_id) ||
        relation_status != 0 || *response_len < 55)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (memcmp(response_schema_id, schema_id, 16) != 0 ||
        response_schema_version != schema_version ||
        response_table_epoch != plan_relation->table_epoch ||
        memcmp(response_relation_id, plan_relation->relation_id, 16) != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    offset = 51;
    if (offset + 2 > *response_len)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    uint32_t relation_name_len = prdb_sdk_internal_response_u16(response, offset);
    offset += 2;
    if (relation_name_len == 0 || relation_name_len > 63 ||
        offset + relation_name_len + 2 > *response_len ||
        !response_identifier(response + offset, relation_name_len))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (relation_name_len != plan_relation->relation_name_len ||
        memcmp(response + offset, plan_relation->relation_name,
               relation_name_len) != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    offset += relation_name_len;
    column_count = prdb_sdk_internal_response_u16(response, offset);
    offset += 2;
    if (column_count == 0 || column_count > 4096)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    for (uint32_t ordinal = 0; ordinal < column_count; ++ordinal) {
        uint32_t column_ordinal;
        uint8_t nullable;
        uint8_t type_code;
        const uint8_t *column_id;
        const uint8_t *parameters;
        uint32_t column_name_len;
        if (offset + 28 > *response_len)
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        column_id = response + offset;
        if (prdb_sdk_internal_all_zero(column_id, 16))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        offset += 16;
        column_ordinal = prdb_sdk_internal_response_u32(response, offset);
        offset += 4;
        nullable = response[offset++];
        type_code = response[offset++];
        parameters = response + offset;
        offset += 4;
        if (column_ordinal != ordinal || nullable > 1 ||
            !prdb_sdk_internal_valid_type(type_code, parameters) || offset + 2 > *response_len)
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        column_name_len = prdb_sdk_internal_response_u16(response, offset);
        offset += 2;
        if (column_name_len == 0 || column_name_len > 63 ||
            offset + column_name_len > *response_len ||
            !response_identifier(response + offset, column_name_len))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        offset += column_name_len;
        if (scan_index < plan_relation->scan_column_count &&
            plan_relation->scan_columns[scan_index].ordinal < ordinal)
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        if (scan_index < plan_relation->scan_column_count &&
            plan_relation->scan_columns[scan_index].ordinal == ordinal) {
            const prdb_sdk_optimizer_plan_column_v1 *expected =
                &plan_relation->scan_columns[scan_index++];
            if (memcmp(column_id, expected->column_id, 16) != 0 ||
                type_code != expected->type_code ||
                nullable != expected->nullable ||
                memcmp(parameters, expected->type_parameters, 4) != 0)
                return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        }
    }
    if (offset != *response_len)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (scan_index != plan_relation->scan_column_count)
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_validate_executor_join_relations_v1(
    const prdb_host_api_v2 *host, const uint8_t *plan_payload,
    uint64_t plan_payload_len, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len) {
    prdb_sdk_optimizer_join_plan_v1 plan = {0};
    prdb_sdk_optimizer_index_join_plan_v1 index_plan = {0};
    prdb_sdk_optimizer_index_prefix_join_plan_v1 prefix_plan = {0};
    uint16_t version;
    uint8_t kind;
    uint8_t reserved;
    const prdb_sdk_optimizer_join_plan_v1 *join_plan;
    int32_t status;
    if (host == NULL || host->struct_size < sizeof(*host) ||
        host->abi_version != PRDB_QUERY_ABI_V2_VERSION ||
        host->executor_request == NULL || plan_payload == NULL ||
        response == NULL || response_len == NULL ||
        response_capacity > PRDB_QUERY_MAX_IPC_RESPONSE_BYTES ||
        response_capacity < 3 ||
        plan_payload_len > PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES ||
        prdb_sdk_internal_ranges_overlap(plan_payload, plan_payload_len, response,
                       response_capacity))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    if (!prdb_decode_optimizer_plan_header_record(
            plan_payload, PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES,
            &version, &kind, &reserved) || version != 1 || reserved != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (kind == PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_NESTED_LOOP_JOIN) {
        status = prdb_sdk_optimizer_decode_index_join_plan_v1(
            plan_payload, plan_payload_len, &index_plan, sizeof(index_plan));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        join_plan = &index_plan.join;
    } else if (kind == PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_PREFIX_NESTED_LOOP_JOIN) {
        status = prdb_sdk_optimizer_decode_index_prefix_join_plan_v1(
            plan_payload, plan_payload_len, &prefix_plan, sizeof(prefix_plan));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        if (prefix_plan.has_filter != 0)
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        join_plan = &prefix_plan.join;
    } else {
        status = prdb_sdk_optimizer_decode_join_plan_v1(
            plan_payload, plan_payload_len, &plan, sizeof(plan));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        join_plan = &plan;
    }
    status = validate_join_relation_identity(
        host, join_plan->schema_name, join_plan->schema_name_len,
        join_plan->schema_id, join_plan->schema_version,
        &join_plan->left_relation, response,
        response_capacity, response_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    return validate_join_relation_identity(
        host, join_plan->schema_name, join_plan->schema_name_len,
        join_plan->schema_id, join_plan->schema_version,
        &join_plan->right_relation, response,
        response_capacity, response_len);
}

int32_t prdb_sdk_optimizer_validate_executor_filtered_join_relations_v1(
    const prdb_host_api_v2 *host, const uint8_t *plan_payload,
    uint64_t plan_payload_len, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len) {
    prdb_sdk_optimizer_filtered_join_plan_v1 plan = {0};
    prdb_sdk_optimizer_index_join_plan_v1 index_plan = {0};
    prdb_sdk_optimizer_index_prefix_join_plan_v1 prefix_plan = {0};
    uint16_t version;
    uint8_t kind;
    uint8_t reserved;
    const prdb_sdk_optimizer_join_plan_v1 *join_plan;
    int32_t status;
    if (host == NULL || host->struct_size < sizeof(*host) ||
        host->abi_version != PRDB_QUERY_ABI_V2_VERSION ||
        host->executor_request == NULL || plan_payload == NULL ||
        response == NULL || response_len == NULL ||
        response_capacity > PRDB_QUERY_MAX_IPC_RESPONSE_BYTES ||
        response_capacity < 3 ||
        plan_payload_len > PRDB_SDK_OPTIMIZER_MAX_PLAN_BYTES ||
        prdb_sdk_internal_ranges_overlap(plan_payload, plan_payload_len, response,
                       response_capacity))
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    if (!prdb_decode_optimizer_plan_header_record(
            plan_payload, PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES,
            &version, &kind, &reserved) || version != 1 || reserved != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (kind == PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_NESTED_LOOP_JOIN) {
        status = prdb_sdk_optimizer_decode_index_join_plan_v1(
            plan_payload, plan_payload_len, &index_plan, sizeof(index_plan));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        if (index_plan.has_filter != 1)
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        join_plan = &index_plan.join;
    } else if (kind == PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_PREFIX_NESTED_LOOP_JOIN) {
        status = prdb_sdk_optimizer_decode_index_prefix_join_plan_v1(
            plan_payload, plan_payload_len, &prefix_plan, sizeof(prefix_plan));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        if (prefix_plan.has_filter != 1)
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        join_plan = &prefix_plan.join;
    } else {
        status = prdb_sdk_optimizer_decode_filtered_join_plan_v1(
            plan_payload, plan_payload_len, &plan, sizeof(plan));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        join_plan = &plan.join;
    }
    status = validate_join_relation_identity(
        host, join_plan->schema_name, join_plan->schema_name_len,
        join_plan->schema_id, join_plan->schema_version,
        &join_plan->left_relation, response, response_capacity, response_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    return validate_join_relation_identity(
        host, join_plan->schema_name, join_plan->schema_name_len,
        join_plan->schema_id, join_plan->schema_version,
        &join_plan->right_relation, response, response_capacity, response_len);
}

int32_t prdb_sdk_optimizer_acquire_plan_relation_v1(
    const prdb_host_api_v2 *host, const uint8_t *plan_payload,
    uint64_t plan_payload_len, uint8_t *response,
    uint64_t response_capacity, uint64_t *response_len,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *columns, uint32_t column_capacity) {
    prdb_sdk_optimizer_plan_v1 plan = {0};
    prdb_sdk_optimizer_filter_v1 filter = {0};
    int32_t status;
    uint64_t columns_bytes =
        (uint64_t)column_capacity * sizeof(prdb_sdk_sql_column_shape_v1);

    if (relation_out == NULL || relation_out_capacity < sizeof(*relation_out) ||
        columns == NULL || column_capacity == 0 ||
        prdb_sdk_internal_ranges_overlap(relation_out, sizeof(*relation_out), columns,
                       columns_bytes) ||
        prdb_sdk_internal_ranges_overlap(relation_out, sizeof(*relation_out), response,
                       response_capacity) ||
        prdb_sdk_internal_ranges_overlap(columns, columns_bytes, response, response_capacity) ||
        prdb_sdk_internal_ranges_overlap(relation_out, sizeof(*relation_out), plan_payload,
                       plan_payload_len) ||
        prdb_sdk_internal_ranges_overlap(columns, columns_bytes, plan_payload, plan_payload_len)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    status = prdb_sdk_optimizer_validate_executor_relation_v1(
        host, plan_payload, plan_payload_len, response, response_capacity,
        response_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    status = prdb_sdk_internal_decode_any_optimizer_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan), &filter,
        sizeof(filter));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;

    status = prdb_sdk_executor_resolve_relation_v1(
        host, plan.schema_name, plan.schema_name_len, plan.relation_name,
        plan.relation_name_len, response, response_capacity, response_len,
        relation_out, relation_out_capacity, columns, column_capacity);
    if (status == PRDB_SDK_EXECUTOR_UNAVAILABLE) {
        return PRDB_SDK_OPTIMIZER_PLAN_UNAVAILABLE;
    }
    if (status == PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL) {
        return PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL;
    }
    if (status != PRDB_SDK_EXECUTOR_OK) {
        return PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
    }
    if (memcmp(relation_out->relation_id, plan.relation_id, 16) != 0 ||
        relation_out->name_len != plan.relation_name_len ||
        memcmp(relation_out->name, plan.relation_name,
               plan.relation_name_len) != 0) {
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_open_plan_scan_v1(
    const prdb_host_api_v2 *host, const uint8_t *plan_payload,
    uint64_t plan_payload_len, uint8_t *relation_response,
    uint64_t relation_response_capacity, uint64_t *relation_response_len,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint64_t *scan_response_len, prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *columns, uint32_t column_capacity,
    uint8_t cursor_out[16], uint32_t *scan_opened_out) {
    prdb_sdk_optimizer_plan_v1 plan = {0};
    prdb_sdk_optimizer_filter_v1 filter = {0};
    int32_t status;

    if (relation_response == NULL || scan_response == NULL ||
        relation_response_len == NULL || scan_response_len == NULL ||
        cursor_out == NULL || scan_opened_out == NULL ||
        prdb_sdk_internal_ranges_overlap(relation_response, relation_response_capacity,
                       scan_response, scan_response_capacity) ||
        prdb_sdk_internal_ranges_overlap(cursor_out, 16, relation_response,
                       relation_response_capacity) ||
        prdb_sdk_internal_ranges_overlap(cursor_out, 16, scan_response,
                       scan_response_capacity) ||
        prdb_sdk_internal_ranges_overlap(scan_opened_out, sizeof(*scan_opened_out),
                       relation_response, relation_response_capacity) ||
        prdb_sdk_internal_ranges_overlap(scan_opened_out, sizeof(*scan_opened_out),
                       scan_response, scan_response_capacity)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    if (prdb_sdk_internal_ranges_overlap(cursor_out, 16, relation_out,
                       relation_out_capacity) ||
        prdb_sdk_internal_ranges_overlap(cursor_out, 16, columns,
                       (uint64_t)column_capacity * sizeof(*columns)) ||
        prdb_sdk_internal_ranges_overlap(scan_opened_out, sizeof(*scan_opened_out), cursor_out,
                       16) ||
        prdb_sdk_internal_ranges_overlap(scan_opened_out, sizeof(*scan_opened_out), relation_out,
                       relation_out_capacity) ||
        prdb_sdk_internal_ranges_overlap(scan_opened_out, sizeof(*scan_opened_out), columns,
                       (uint64_t)column_capacity * sizeof(*columns))) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    status = prdb_sdk_internal_decode_any_optimizer_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan), &filter,
        sizeof(filter));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    status = prdb_sdk_optimizer_acquire_plan_relation_v1(
        host, plan_payload, plan_payload_len, relation_response,
        relation_response_capacity, relation_response_len,
        relation_out, relation_out_capacity, columns, column_capacity);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    if (plan.has_limit != 0 && plan.limit == 0) {
        memset(cursor_out, 0, 16);
        *scan_response_len = 0;
        *scan_opened_out = 0;
        return PRDB_SDK_OPTIMIZER_PLAN_OK;
    }
    status = prdb_sdk_executor_open_full_scan_v1(
        host, relation_out->executor_handle, scan_response,
        scan_response_capacity, scan_response_len, cursor_out);
    if (status == PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL) {
        return PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL;
    }
    if (status != PRDB_SDK_EXECUTOR_OK) {
        return PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
    }
    *scan_opened_out = 1;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_scan_state_init_v1(
    const uint8_t *plan_payload, uint64_t plan_payload_len,
    prdb_sdk_optimizer_scan_state_v1 *state_out,
    uint32_t state_out_capacity) {
    prdb_sdk_optimizer_plan_v1 plan = {0};
    prdb_sdk_optimizer_filter_v1 filter = {0};
    prdb_sdk_optimizer_scan_state_v1 state = {0};
    int32_t status;
    if (state_out == NULL || state_out_capacity < sizeof(*state_out) ||
        prdb_sdk_internal_ranges_overlap(state_out, sizeof(*state_out), plan_payload,
                       plan_payload_len)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    status = prdb_sdk_optimizer_decode_plan_with_filter_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan), &filter,
        sizeof(filter));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    state.struct_size = sizeof(state);
    state.abi_version = PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION;
    state.projection_count = plan.projection_count;
    state.has_limit = plan.has_limit;
    state.limit = plan.limit;
    state.done = plan.has_limit != 0 && plan.limit == 0;
    memcpy(state.projections, plan.projections,
           plan.projection_count * sizeof(plan.projections[0]));
    *state_out = state;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

static int32_t consume_scan_batch_internal(
    prdb_sdk_optimizer_scan_state_v1 *state,
    const prdb_sdk_batch_v1 *batch, uint32_t relation_column_count,
    prdb_sdk_value_v1 *source_cells, uint32_t source_cell_capacity,
    prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity,
    const prdb_sdk_optimizer_plan_v1 *plan,
    const prdb_sdk_optimizer_filter_v1 *filter,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out) {
    prdb_sdk_row_iterator_v1 rows = {0};
    uint32_t delivered = 0;
    int32_t status;
    if (state == NULL || state->struct_size < sizeof(*state) ||
        state->abi_version != PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION ||
        state->projection_count == 0 ||
        state->projection_count > PRDB_SDK_OPTIMIZER_PLAN_MAX_PROJECTIONS ||
        state->has_limit > 1 || state->done > 1 ||
        (state->has_limit != 0 && state->delivered_rows > state->limit) ||
        batch == NULL || relation_column_count == 0 || source_cells == NULL ||
        relation_column_count > PRDB_SDK_BATCH_MAX_CELLS_PER_ROW ||
        source_cell_capacity < relation_column_count || projected_values == NULL ||
        projected_value_capacity < state->projection_count || on_row == NULL ||
        delivered_rows_out == NULL) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    *delivered_rows_out = 0;
    if (state->done != 0) return PRDB_SDK_OPTIMIZER_PLAN_OK;
    for (uint32_t index = 0; index < state->projection_count; ++index) {
        const prdb_sdk_optimizer_plan_column_v1 *projection =
            &state->projections[index];
        if (projection->ordinal >= relation_column_count ||
            !prdb_sdk_internal_valid_type(projection->type_code,
                        projection->type_parameters)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
    }
    status = prdb_sdk_batch_rows_begin_v1(batch, &rows, sizeof(rows));
    if (status != PRDB_SDK_BATCH_OK) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    }
    while (rows.rows_remaining != 0) {
        prdb_sdk_row_v1 row = {0};
        prdb_sdk_cell_iterator_v1 cells = {0};
        status = prdb_sdk_batch_row_next_v1(&rows, &row, sizeof(row));
        if (status != PRDB_SDK_BATCH_OK ||
            row.cell_count != relation_column_count ||
            prdb_sdk_row_cells_begin_v1(&row, &cells, sizeof(cells)) !=
                PRDB_SDK_BATCH_OK) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        for (uint32_t ordinal = 0; ordinal < relation_column_count; ++ordinal) {
            if (prdb_sdk_cell_next_v1(&cells, &source_cells[ordinal],
                                      sizeof(source_cells[ordinal])) !=
                PRDB_SDK_BATCH_OK) {
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            }
        }
        if (filter != NULL && filter->present != 0) {
            plan_reader predicate_reader = {filter->prefix,
                                            filter->prefix_len, 0};
            predicate_value predicate = {0};
            uint32_t nodes = 0;
            int32_t eval_status = prdb_sdk_internal_evaluate_predicate_node(
                &predicate_reader, plan, source_cells, &predicate, 1, &nodes);
            if (eval_status == PRDB_SDK_SQL_EVAL_OK &&
                predicate.is_truth == 0) {
                eval_status = prdb_sdk_sql_value_truth_v1(
                    &predicate.value, &predicate.truth);
                predicate.is_truth = eval_status == PRDB_SDK_SQL_EVAL_OK;
            }
            if (eval_status != PRDB_SDK_SQL_EVAL_OK ||
                predicate_reader.offset != predicate_reader.length ||
                predicate.is_truth == 0) {
                return PRDB_SDK_OPTIMIZER_PLAN_FILTER_ERROR;
            }
            if (predicate.truth != PRDB_SDK_SQL_TRUTH_TRUE) continue;
        }
        for (uint32_t index = 0; index < state->projection_count; ++index) {
            const prdb_sdk_optimizer_plan_column_v1 *projection =
                &state->projections[index];
            const prdb_sdk_value_v1 *value =
                &source_cells[projection->ordinal];
            if (value->state == PRDB_SDK_VALUE_STATE_VALUE &&
                value->type_code != projection->type_code) {
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            }
            projected_values[index] = *value;
        }
        if (on_row(row_context, state->projections, projected_values,
                   state->projection_count) != 0) {
            *delivered_rows_out = delivered;
            return PRDB_SDK_OPTIMIZER_PLAN_ROW_CALLBACK_ERROR;
        }
        ++delivered;
        ++state->delivered_rows;
        if (state->has_limit != 0 &&
            state->delivered_rows >= state->limit) {
            state->done = 1;
            break;
        }
    }
    *delivered_rows_out = delivered;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

static int32_t consume_index_ranges_internal(
    const prdb_host_api_v2 *host, prdb_sdk_optimizer_scan_state_v1 *state,
    const prdb_sdk_index_ranges_v1 *ranges, uint32_t relation_column_count,
    uint16_t max_candidates, const prdb_sdk_optimizer_plan_v1 *plan,
    const prdb_sdk_optimizer_filter_v1 *filter,
    prdb_sdk_value_v1 *source_cells, uint32_t source_cell_capacity,
    prdb_sdk_value_v1 *projected_values, uint32_t projected_value_capacity,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out) {
    prdb_sdk_index_ranges_group_iterator_v1 groups = {0};
    uint32_t candidates = 0;
    uint32_t delivered = 0;
    uint32_t examined = 0;
    int32_t status;
    if (host == NULL || state == NULL || ranges == NULL || plan == NULL ||
        source_cells == NULL || source_cell_capacity < relation_column_count ||
        projected_values == NULL || projected_value_capacity < state->projection_count ||
        on_row == NULL || delivered_rows_out == NULL || max_candidates == 0)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    *delivered_rows_out = 0;
    if (prdb_sdk_index_ranges_groups_begin_v1(ranges, &groups, sizeof(groups)) !=
        PRDB_SDK_BATCH_OK) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    /* First pass validates every row and the aggregate candidate ceiling. */
    for (uint16_t group_index = 0; group_index < ranges->group_count; ++group_index) {
        prdb_sdk_index_range_v1 range = {0};
        prdb_sdk_index_range_row_iterator_v1 rows = {0};
        if (prdb_sdk_index_ranges_group_next_v1(&groups, &range, sizeof(range)) !=
                PRDB_SDK_BATCH_OK ||
            prdb_sdk_index_range_rows_begin_v1(&range, &rows, sizeof(rows)) !=
                PRDB_SDK_BATCH_OK)
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        while (rows.rows_remaining != 0) {
            prdb_sdk_index_range_row_v1 row = {0};
            prdb_sdk_cell_iterator_v1 cells = {0};
            if (++candidates > max_candidates ||
                prdb_sdk_index_range_row_next_v1(&rows, &row, sizeof(row)) !=
                    PRDB_SDK_BATCH_OK || row.cell_count != relation_column_count ||
                prdb_sdk_index_range_row_cells_begin_v1(&row, &cells, sizeof(cells)) !=
                    PRDB_SDK_BATCH_OK)
                return candidates > max_candidates
                    ? PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL
                    : PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            if ((candidates % 2048) == 0) {
                int32_t cancelled = 0;
                status = prdb_sdk_executor_check_cancelled_v1(host, &cancelled);
                if (status != PRDB_SDK_EXECUTOR_OK)
                    return PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
                if (cancelled != 0) return PRDB_SDK_OPTIMIZER_PLAN_CANCELLED;
            }
            for (uint32_t i = 0; i < row.cell_count; ++i) {
                prdb_sdk_value_v1 value = {0};
                if (prdb_sdk_cell_next_v1(&cells, &value, sizeof(value)) !=
                        PRDB_SDK_BATCH_OK || value.state > PRDB_SDK_VALUE_STATE_REDACTED)
                    return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            }
        }
    }
    if (prdb_sdk_index_ranges_groups_begin_v1(ranges, &groups, sizeof(groups)) !=
        PRDB_SDK_BATCH_OK) return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    for (uint16_t group_index = 0; group_index < ranges->group_count; ++group_index) {
        prdb_sdk_index_range_v1 range = {0};
        prdb_sdk_index_range_row_iterator_v1 rows = {0};
        if (prdb_sdk_index_ranges_group_next_v1(&groups, &range, sizeof(range)) !=
                PRDB_SDK_BATCH_OK ||
            prdb_sdk_index_range_rows_begin_v1(&range, &rows, sizeof(rows)) !=
                PRDB_SDK_BATCH_OK)
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        while (rows.rows_remaining != 0 && state->done == 0) {
            prdb_sdk_index_range_row_v1 row = {0};
            prdb_sdk_cell_iterator_v1 cells = {0};
            if (prdb_sdk_index_range_row_next_v1(&rows, &row, sizeof(row)) !=
                    PRDB_SDK_BATCH_OK ||
                prdb_sdk_index_range_row_cells_begin_v1(&row, &cells, sizeof(cells)) !=
                    PRDB_SDK_BATCH_OK)
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            for (uint32_t i = 0; i < row.cell_count; ++i) {
                if (prdb_sdk_cell_next_v1(&cells, &source_cells[i],
                                          sizeof(source_cells[i])) != PRDB_SDK_BATCH_OK)
                    return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            }
            if ((++examined % 1024) == 0) {
                int32_t cancelled = 0;
                status = prdb_sdk_executor_check_cancelled_v1(host, &cancelled);
                if (status != PRDB_SDK_EXECUTOR_OK)
                    return PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
                if (cancelled != 0) return PRDB_SDK_OPTIMIZER_PLAN_CANCELLED;
            }
            if (filter != NULL && filter->present != 0) {
                plan_reader predicate_reader = {filter->prefix, filter->prefix_len, 0};
                predicate_value predicate = {0};
                uint32_t nodes = 0;
                int32_t eval_status = prdb_sdk_internal_evaluate_predicate_node(
                    &predicate_reader, plan, source_cells, &predicate, 1, &nodes);
                if (eval_status == PRDB_SDK_SQL_EVAL_OK && predicate.is_truth == 0) {
                    eval_status = prdb_sdk_sql_value_truth_v1(&predicate.value,
                                                               &predicate.truth);
                    predicate.is_truth = eval_status == PRDB_SDK_SQL_EVAL_OK;
                }
                if (eval_status != PRDB_SDK_SQL_EVAL_OK ||
                    predicate_reader.offset != predicate_reader.length ||
                    predicate.is_truth == 0)
                    return PRDB_SDK_OPTIMIZER_PLAN_FILTER_ERROR;
                if (predicate.truth != PRDB_SDK_SQL_TRUTH_TRUE) continue;
            }
            for (uint32_t i = 0; i < state->projection_count; ++i) {
                const prdb_sdk_optimizer_plan_column_v1 *projection = &state->projections[i];
                if (projection->ordinal >= relation_column_count)
                    return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
                projected_values[i] = source_cells[projection->ordinal];
            }
            if (on_row(row_context, state->projections, projected_values,
                       state->projection_count) != 0)
                return PRDB_SDK_OPTIMIZER_PLAN_ROW_CALLBACK_ERROR;
            ++delivered;
            ++state->delivered_rows;
            if (state->has_limit != 0 && state->delivered_rows >= state->limit)
                state->done = 1;
        }
    }
    *delivered_rows_out = delivered;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_consume_scan_batch_v1(
    prdb_sdk_optimizer_scan_state_v1 *state,
    const prdb_sdk_batch_v1 *batch, uint32_t relation_column_count,
    prdb_sdk_value_v1 *source_cells, uint32_t source_cell_capacity,
    prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out) {
    return consume_scan_batch_internal(
        state, batch, relation_column_count, source_cells,
        source_cell_capacity, projected_values, projected_value_capacity,
        NULL, NULL, on_row, row_context, delivered_rows_out);
}

int32_t prdb_sdk_optimizer_consume_plan_batch_v1(
    const uint8_t *plan_payload, uint64_t plan_payload_len,
    prdb_sdk_optimizer_scan_state_v1 *state,
    const prdb_sdk_batch_v1 *batch, uint32_t relation_column_count,
    prdb_sdk_value_v1 *source_cells, uint32_t source_cell_capacity,
    prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out) {
    prdb_sdk_optimizer_plan_v1 plan = {0};
    prdb_sdk_optimizer_filter_v1 filter = {0};
    int32_t status = prdb_sdk_optimizer_decode_plan_with_filter_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan), &filter,
        sizeof(filter));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    if (state == NULL || state->struct_size < sizeof(*state) ||
        state->abi_version != PRDB_SDK_OPTIMIZER_PLAN_ABI_V1_VERSION ||
        state->projection_count != plan.projection_count ||
        state->has_limit != plan.has_limit ||
        (state->has_limit != 0 && state->limit != plan.limit) ||
        memcmp(state->projections, plan.projections,
               plan.projection_count * sizeof(plan.projections[0])) != 0) {
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    return consume_scan_batch_internal(
        state, batch, relation_column_count, source_cells,
        source_cell_capacity, projected_values, projected_value_capacity,
        &plan, &filter, on_row, row_context, delivered_rows_out);
}

PRDB_SDK_INTERNAL int32_t prdb_sdk_internal_execute_unfiltered_plan_internal(
    const prdb_host_api_v2 *host, const uint8_t *plan_payload,
    uint64_t plan_payload_len, uint16_t max_candidates,
    uint16_t max_index_candidates,
    uint32_t max_batch_bytes, uint8_t *relation_response,
    uint64_t relation_response_capacity, uint8_t *scan_response,
    uint64_t scan_response_capacity, uint8_t *batch_response,
    uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns,
    uint32_t relation_column_capacity, prdb_sdk_value_v1 *source_cells,
    uint32_t source_cell_capacity, prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out) {
    prdb_sdk_optimizer_scan_state_v1 state = {0};
    prdb_sdk_optimizer_plan_v1 plan = {0};
    prdb_sdk_optimizer_filter_v1 filter = {0};
    point_probe_hint point_probe = {0};
    range_probe_hint range_probe = {0};
    multi_point_probe_hint multi_point_probe = {0};
    prdb_sdk_batch_v1 batch = {0};
    uint8_t cursor[16] = {0};
    uint64_t relation_response_len = 0;
    uint64_t scan_response_len = 0;
    uint64_t batch_response_len = 0;
    uint32_t total_delivered = 0;
    uint32_t scan_opened = 0;
    uint8_t empty_view_hint = 0;
    int use_full_scan = 1;
    uint32_t max_scan_pages = PRDB_SDK_OPTIMIZER_PLAN_MAX_NEXT_REQUESTS;
    int cursor_open = 0;
    int32_t final_status = PRDB_SDK_OPTIMIZER_PLAN_OK;
    int32_t status;

    if (relation_response == NULL || scan_response == NULL ||
        batch_response == NULL || delivered_rows_out == NULL ||
        on_row == NULL || max_candidates == 0 || max_index_candidates == 0 ||
        max_index_candidates > PRDB_MAX_SQL_INDEX_PROBE_CANDIDATES ||
        max_candidates > PRDB_SDK_BATCH_MAX_ROWS || max_batch_bytes < 8 ||
        max_batch_bytes > PRDB_SDK_BATCH_MAX_BYTES ||
        batch_response_capacity < max_batch_bytes ||
        prdb_sdk_internal_ranges_overlap(relation_response, relation_response_capacity,
                       scan_response, scan_response_capacity) ||
        prdb_sdk_internal_ranges_overlap(relation_response, relation_response_capacity,
                       batch_response, batch_response_capacity) ||
        prdb_sdk_internal_ranges_overlap(scan_response, scan_response_capacity,
                       batch_response, batch_response_capacity)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    *delivered_rows_out = 0;
    status = prdb_sdk_internal_decode_select_plan_with_access_path_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan), &filter,
        sizeof(filter), &point_probe, &range_probe, &multi_point_probe,
        &empty_view_hint);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    status = prdb_sdk_optimizer_scan_state_init_v1(
        plan_payload, plan_payload_len, &state, sizeof(state));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    status = prdb_sdk_optimizer_acquire_plan_relation_v1(
        host, plan_payload, plan_payload_len, relation_response,
        relation_response_capacity, &relation_response_len, relation_out,
        relation_out_capacity, relation_columns, relation_column_capacity);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    if (empty_view_hint != 0) {
        int32_t cancelled = 0;
        status = prdb_sdk_executor_check_cancelled_v1(host, &cancelled);
        if (status != PRDB_SDK_EXECUTOR_OK)
            return PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
        if (cancelled != 0) return PRDB_SDK_OPTIMIZER_PLAN_CANCELLED;
        return PRDB_SDK_OPTIMIZER_PLAN_OK;
    }
    if (plan.has_limit != 0 && plan.limit == 0) return PRDB_SDK_OPTIMIZER_PLAN_OK;

    if (range_probe.present != 0) {
        uint8_t request[38 + 2 * (5 + 4096) +
                        PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS * 16];
        uint8_t column_ids[PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS * 16];
        uint8_t lower_integer_bytes[4];
        uint8_t upper_integer_bytes[4];
        prdb_sdk_value_v1 lower_value = {0};
        prdb_sdk_value_v1 upper_value = {0};
        const prdb_sdk_value_v1 *lower_bound = NULL;
        const prdb_sdk_value_v1 *upper_bound = NULL;
        prdb_sdk_index_range_v1 range = {0};
        uint64_t range_response_capacity;
        uint64_t request_len = 0;
        int32_t cancelled = 0;
        uint32_t delivered = 0;
        int32_t executor_status;

        if (!prdb_sdk_internal_relation_response_column_ids(
                relation_response, relation_response_len,
                relation_out->column_count, column_ids, sizeof(column_ids))) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        if (range_probe.lower_literal != NULL) {
            if (!prdb_sdk_internal_range_literal_to_value(
                    range_probe.lower_literal, range_probe.lower_literal_len,
                    range_probe.key_column.type_code,
                    range_probe.key_column.type_parameters[0],
                    lower_integer_bytes,
                    &lower_value)) {
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            }
            lower_bound = &lower_value;
        }
        if (range_probe.upper_literal != NULL) {
            if (!prdb_sdk_internal_range_literal_to_value(
                    range_probe.upper_literal, range_probe.upper_literal_len,
                    range_probe.key_column.type_code,
                    range_probe.key_column.type_parameters[0],
                    upper_integer_bytes,
                    &upper_value)) {
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            }
            upper_bound = &upper_value;
        }
        range_response_capacity = batch_response_capacity;
        if (range_response_capacity > max_batch_bytes)
            range_response_capacity = max_batch_bytes;
        executor_status = prdb_sdk_executor_scan_index_range_v1(
            host, relation_out->executor_handle, range_probe.index_id,
            lower_bound, range_probe.lower_inclusive, upper_bound,
            range_probe.upper_inclusive, column_ids,
            (uint16_t)relation_out->column_count, request, sizeof(request),
            batch_response, range_response_capacity, &batch_response_len,
            &range, sizeof(range));
        if (executor_status == PRDB_SDK_EXECUTOR_UNAVAILABLE ||
            executor_status == PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL) {
            use_full_scan = 1;
            max_scan_pages--;
        } else if (executor_status != PRDB_SDK_EXECUTOR_OK) {
            return executor_status == PRDB_SDK_EXECUTOR_INVALID_PAYLOAD
                       ? PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD
                       : PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
        } else {
            status = prdb_sdk_executor_check_cancelled_v1(host, &cancelled);
            if (status != PRDB_SDK_EXECUTOR_OK)
                return PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
            if (cancelled != 0) return PRDB_SDK_OPTIMIZER_PLAN_CANCELLED;
            if (!prdb_sdk_internal_range_response_to_internal_batch(
                    batch_response, &batch_response_len,
                    batch_response_capacity, max_batch_bytes,
                    max_candidates)) {
                use_full_scan = 1;
                max_scan_pages--;
            } else {
                status = prdb_sdk_batch_open_v1(
                    &batch, sizeof(batch), batch_response,
                    batch_response_len);
                if (status != PRDB_SDK_BATCH_OK || batch.finished == 0) {
                    return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
                }
                status = consume_scan_batch_internal(
                    &state, &batch, relation_out->column_count,
                    source_cells, source_cell_capacity, projected_values,
                    projected_value_capacity, &plan, &filter, on_row,
                    row_context, &delivered);
                if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
                *delivered_rows_out = delivered;
                return PRDB_SDK_OPTIMIZER_PLAN_OK;
            }
        }
    }

    if (multi_point_probe.present != 0) {
        prdb_sdk_executor_index_probe_v1 probes[64] = {{0}};
        prdb_sdk_value_v1 values[64] = {{0}};
        uint8_t integer_bytes[64][4] = {{0}};
        uint8_t column_ids[PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS * 16];
        uint64_t response_capacity = batch_response_capacity;
        prdb_sdk_index_ranges_v1 ranges = {0};
        uint32_t delivered = 0;
        int32_t executor_status;
        int32_t cancelled = 0;
        if (!prdb_sdk_internal_relation_response_column_ids(
                relation_response, relation_response_len,
                relation_out->column_count, column_ids, sizeof(column_ids)))
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        for (uint16_t i = 0; i < multi_point_probe.count; ++i) {
            if (!prdb_sdk_internal_range_literal_to_value(
                    multi_point_probe.literals[i],
                    multi_point_probe.literal_lengths[i],
                    multi_point_probe.key_column.type_code,
                    multi_point_probe.key_column.type_parameters[0],
                    integer_bytes[i], &values[i]))
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            probes[i].struct_size = sizeof(probes[i]);
            probes[i].abi_version = PRDB_SDK_EXECUTOR_ABI_V1_VERSION;
            probes[i].values = &values[i];
            probes[i].value_count = 1;
        }
        if (response_capacity > max_batch_bytes) response_capacity = max_batch_bytes;
        executor_status = prdb_sdk_executor_scan_index_ranges_v1(
            host, relation_out->executor_handle, multi_point_probe.index_id,
            probes, multi_point_probe.count, column_ids,
            (uint16_t)relation_out->column_count, scan_response,
            scan_response_capacity, batch_response, response_capacity,
            &batch_response_len, &ranges, sizeof(ranges));
        if (executor_status == PRDB_SDK_EXECUTOR_UNAVAILABLE ||
            executor_status == PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL ||
            executor_status == PRDB_SDK_EXECUTOR_INVALID_ARGUMENT) {
            use_full_scan = 1;
            max_scan_pages--;
        } else if (executor_status != PRDB_SDK_EXECUTOR_OK) {
            return executor_status == PRDB_SDK_EXECUTOR_INVALID_PAYLOAD
                ? PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD
                : PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
        } else {
            status = prdb_sdk_executor_check_cancelled_v1(host, &cancelled);
            if (status != PRDB_SDK_EXECUTOR_OK)
                return PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
            if (cancelled != 0) return PRDB_SDK_OPTIMIZER_PLAN_CANCELLED;
            status = consume_index_ranges_internal(
                host, &state, &ranges, relation_out->column_count,
                max_index_candidates, &plan, &filter, source_cells,
                source_cell_capacity, projected_values,
                projected_value_capacity, on_row, row_context, &delivered);
            if (status == PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL) {
                use_full_scan = 1;
                max_scan_pages--;
            } else if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) {
                return status;
            } else {
                *delivered_rows_out = delivered;
                return PRDB_SDK_OPTIMIZER_PLAN_OK;
            }
        }
    }

    if (point_probe.present != 0) {
        uint8_t request[38 + 5 + 4096 +
                        PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS * 16];
        uint64_t request_len = 0;
        uint64_t probe_response_capacity;
        int32_t cancelled = 0;
        int32_t callback_status;
        uint8_t probe_version;
        uint8_t probe_status;
        if (!prdb_sdk_internal_encode_point_probe_request(
                &plan, relation_out, &point_probe, relation_response,
                relation_response_len, request, sizeof(request),
                &request_len)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        probe_response_capacity = max_batch_bytes - 3;
        if (probe_response_capacity > batch_response_capacity) {
            probe_response_capacity = batch_response_capacity;
        }
        callback_status = host->executor_request(
            host->context, PRDB_SDK_EXECUTOR_OP_PROBE_UNIQUE_INDEX, request,
            request_len, batch_response, probe_response_capacity,
            &batch_response_len);
        if (callback_status == PRDB_QUERY_CALLBACK_RESPONSE_TOO_SMALL) {
            use_full_scan = 1;
            max_scan_pages--;
        } else if (callback_status != PRDB_QUERY_CALLBACK_OK ||
                   batch_response_len > probe_response_capacity) {
            return PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
        } else if (!prdb_decode_executor_probe_unique_index_response_match_record(
                       batch_response, (size_t)batch_response_len,
                       &probe_version, &probe_status) ||
                   probe_version != PRDB_PROBE_UNIQUE_INDEX_RESPONSE_VERSION) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        } else if (probe_status == 0 &&
                   prdb_decode_executor_probe_unique_index_response_unavailable_record(
                       batch_response, (size_t)batch_response_len,
                       &probe_version, &probe_status)) {
            use_full_scan = 1;
            max_scan_pages--;
        } else if (probe_status == 1 &&
                   prdb_decode_executor_probe_unique_index_response_no_match_record(
                       batch_response, (size_t)batch_response_len,
                       &probe_version, &probe_status)) {
            status = prdb_sdk_executor_check_cancelled_v1(host, &cancelled);
            if (status != PRDB_SDK_EXECUTOR_OK) {
                return PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
            }
            if (cancelled != 0) return PRDB_SDK_OPTIMIZER_PLAN_CANCELLED;
            return PRDB_SDK_OPTIMIZER_PLAN_OK;
        } else if (probe_status == 2 && batch_response_len >=
                   PRDB_EXECUTOR_PROBE_UNIQUE_INDEX_RESPONSE_MATCH_RECORD_HEADER_BYTES +
                       PRDB_EXECUTOR_BATCH_ROW_RECORD_HEADER_BYTES) {
            uint32_t delivered = 0;
            status = prdb_sdk_executor_check_cancelled_v1(host, &cancelled);
            if (status != PRDB_SDK_EXECUTOR_OK) {
                return PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
            }
            if (cancelled != 0) return PRDB_SDK_OPTIMIZER_PLAN_CANCELLED;
            if (batch_response_len + 3 > max_batch_bytes ||
                batch_response_len + 3 > batch_response_capacity) {
                use_full_scan = 1;
                max_scan_pages--;
            } else {
                memmove(batch_response + 5,
                        batch_response +
                            PRDB_EXECUTOR_PROBE_UNIQUE_INDEX_RESPONSE_MATCH_RECORD_HEADER_BYTES,
                        (size_t)(batch_response_len -
                                 PRDB_EXECUTOR_PROBE_UNIQUE_INDEX_RESPONSE_MATCH_RECORD_HEADER_BYTES));
                batch_response[0] = 0;
                batch_response[1] = 1;
                batch_response[2] = 1;
                batch_response[3] = 0;
                batch_response[4] = 1;
                batch_response_len +=
                    5 - PRDB_EXECUTOR_PROBE_UNIQUE_INDEX_RESPONSE_MATCH_RECORD_HEADER_BYTES;
                status = prdb_sdk_batch_open_v1(
                    &batch, sizeof(batch), batch_response,
                    batch_response_len);
                if (status != PRDB_SDK_BATCH_OK || batch.finished == 0 ||
                    batch.row_count != 1) {
                    return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
                }
                status = consume_scan_batch_internal(
                    &state, &batch, relation_out->column_count,
                    source_cells, source_cell_capacity, projected_values,
                    projected_value_capacity, &plan, &filter, on_row,
                    row_context, &delivered);
                if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
                *delivered_rows_out = delivered;
                return PRDB_SDK_OPTIMIZER_PLAN_OK;
            }
        } else {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
    }
    if (use_full_scan != 0) {
        status = prdb_sdk_executor_open_full_scan_v1(
            host, relation_out->executor_handle, scan_response,
            scan_response_capacity, &scan_response_len, cursor);
        if (status == PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL) {
            return PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL;
        }
        if (status != PRDB_SDK_EXECUTOR_OK) {
            return PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
        }
        scan_opened = 1;
    }
    if (scan_opened == 0) return PRDB_SDK_OPTIMIZER_PLAN_OK;
    cursor_open = 1;

    for (uint32_t page = 0; page < max_scan_pages; ++page) {
        int32_t cancelled = 0;
        uint32_t page_rows = 0;
        status = prdb_sdk_executor_check_cancelled_v1(host, &cancelled);
        if (status != PRDB_SDK_EXECUTOR_OK) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
            break;
        }
        if (cancelled != 0) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_CANCELLED;
            break;
        }
        status = prdb_sdk_executor_next_batch_v1(
            host, cursor, max_candidates, max_batch_bytes, batch_response,
            batch_response_capacity, &batch_response_len, &batch,
            sizeof(batch));
        if (status == PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL;
            break;
        }
        if (status != PRDB_SDK_EXECUTOR_OK) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
            break;
        }
        if (batch.finished != 0) {
            cursor_open = 0;
        } else {
            memcpy(cursor, batch.continuation, sizeof(cursor));
        }
        status = prdb_sdk_optimizer_consume_plan_batch_v1(
            plan_payload, plan_payload_len, &state, &batch,
            relation_out->column_count, source_cells,
            source_cell_capacity, projected_values,
            projected_value_capacity, on_row, row_context, &page_rows);
        if (UINT32_MAX - total_delivered < page_rows) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            break;
        }
        total_delivered += page_rows;
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) {
            final_status = status;
            break;
        }
        if (state.done != 0 || batch.finished != 0) break;
        if (page + 1 == max_scan_pages) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_REQUEST_LIMIT;
        }
    }

    if (cursor_open != 0) {
        status = prdb_sdk_executor_close_cursor_v1(
            host, cursor, scan_response, scan_response_capacity,
            &scan_response_len);
        if (status != PRDB_SDK_EXECUTOR_OK &&
            final_status == PRDB_SDK_OPTIMIZER_PLAN_OK) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
        }
    }
    *delivered_rows_out = total_delivered;
    return final_status;
}
