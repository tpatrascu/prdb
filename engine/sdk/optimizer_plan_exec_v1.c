#include "optimizer_plan_codec_internal.h"

static int32_t latch_optimizer_statement_failure(
    const prdb_host_api_v4 *host, int32_t status) {
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK &&
        status != PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT &&
        host != NULL && host->v3.statement_failure != NULL) {
        (void)host->v3.statement_failure(
            host->v3.context, PRDB_QUERY_STATEMENT_FAILURE_SQL);
    }
    return status;
}

static int identifier_equal_ascii_casefold(const uint8_t *left,
                                           const uint8_t *right,
                                           uint32_t length) {
    for (uint32_t index = 0; index < length; ++index) {
        uint8_t left_byte = left[index];
        uint8_t right_byte = right[index];
        if (left_byte >= 'A' && left_byte <= 'Z')
            left_byte = (uint8_t)(left_byte + ('a' - 'A'));
        if (right_byte >= 'A' && right_byte <= 'Z')
            right_byte = (uint8_t)(right_byte + ('a' - 'A'));
        if (left_byte != right_byte) return 0;
    }
    return 1;
}

static int response_column_name(const uint8_t *response,
                                uint64_t response_len, uint32_t target_ordinal,
                                const uint8_t **name_out,
                                uint32_t *name_len_out) {
    uint64_t offset = 51;
    uint32_t relation_name_len;
    uint32_t column_count;
    if (response == NULL || name_out == NULL || name_len_out == NULL ||
        response_len < offset + 2) return 0;
    relation_name_len = prdb_sdk_internal_response_u16(response, offset);
    offset += 2;
    if (relation_name_len > response_len - offset) return 0;
    offset += relation_name_len;
    if (response_len - offset < 2) return 0;
    column_count = prdb_sdk_internal_response_u16(response, offset);
    offset += 2;
    for (uint32_t ordinal = 0; ordinal < column_count; ++ordinal) {
        uint32_t column_name_len;
        if (response_len - offset < 28) return 0;
        offset += 26;
        column_name_len = prdb_sdk_internal_response_u16(response, offset);
        offset += 2;
        if (column_name_len > response_len - offset) return 0;
        if (ordinal == target_ordinal) {
            *name_out = response + offset;
            *name_len_out = column_name_len;
            return 1;
        }
        offset += column_name_len;
    }
    return 0;
}

static int match_optimizer_parameter(plan_reader *reader,
                                     const prdb_sdk_value_v1 *value) {
    uint8_t tag;
    uint64_t tag_offset = reader->offset;
    const uint8_t *bytes;
    if (value == NULL || prdb_sdk_value_validate_v1(value) !=
                             PRDB_SDK_VALUE_VALID ||
        !prdb_sdk_internal_read_u8(reader, &tag)) return 0;
    if (value->state == PRDB_SDK_VALUE_STATE_NULL)
        return tag == 0x00 && prdb_sdk_internal_read_null_predicate_tag(reader, tag_offset);
    if (value->state != PRDB_SDK_VALUE_STATE_VALUE) return 0;
    switch (value->type_code) {
    case PRDB_SDK_VALUE_TYPE_BOOLEAN:
        {
            uint8_t boolean_value;
            return tag == 0x01 &&
                   prdb_sdk_internal_read_boolean_literal_predicate_record(
                       reader, tag_offset, &boolean_value) &&
                   boolean_value == value->data[0];
        }
    case PRDB_SDK_VALUE_TYPE_INTEGER: {
        int64_t integer_value;
        int32_t expected;
        if (prdb_sdk_value_get_integer_v1(value, &expected) !=
            PRDB_SDK_VALUE_ACCESS_OK) return 0;
        if (tag != 0x02 ||
            !prdb_sdk_internal_read_integer_predicate_record(reader, tag_offset,
                                           &integer_value)) return 0;
        return integer_value == expected;
    }
    case PRDB_SDK_VALUE_TYPE_TEXT: {
        uint16_t length;
        if (tag != 0x03 ||
            !prdb_sdk_internal_read_text_predicate_header(reader, tag_offset, &length) ||
            length != value->data_len || length > 4096 ||
            !prdb_sdk_internal_take(reader, length, &bytes)) return 0;
        return length == 0 || memcmp(bytes, value->data, length) == 0;
    }
    case PRDB_SDK_VALUE_TYPE_DATE: {
        const uint8_t *date_bytes;
        if (tag != 0x04 ||
            !prdb_sdk_internal_read_date_predicate_record(reader, tag_offset, &date_bytes))
            return 0;
        return memcmp(date_bytes, value->data, 4) == 0;
    }
    case PRDB_SDK_VALUE_TYPE_UUID: {
        const uint8_t *uuid_bytes;
        if (tag != 0x05 ||
            !prdb_sdk_internal_read_uuid_predicate_record(reader, tag_offset, &uuid_bytes))
            return 0;
        return memcmp(uuid_bytes, value->data, 16) == 0;
    }
    case PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ: {
        const uint8_t *timestamp_bytes;
        if (tag != 0x06 || !prdb_sdk_internal_read_timestamptz_predicate_record(
                                reader, tag_offset, &timestamp_bytes))
            return 0;
        return memcmp(timestamp_bytes, value->data, 8) == 0;
    }
    case PRDB_SDK_VALUE_TYPE_BYTEA: {
        uint16_t length;
        const uint8_t *bytea_bytes;
        if (tag != 0x07 ||
            !prdb_sdk_internal_read_bytea_predicate_header(reader, tag_offset, &length) ||
            length > 4096 || length != value->data_len ||
            !prdb_sdk_internal_take(reader, length, &bytea_bytes)) return 0;
        return length == 0 || memcmp(bytea_bytes, value->data, length) == 0;
    }
    case PRDB_SDK_VALUE_TYPE_DECIMAL: {
        const uint8_t *coefficient;
        if (tag != 0x08 ||
            !prdb_sdk_internal_read_decimal_predicate_record(reader, tag_offset, &coefficient))
            return 0;
        return memcmp(coefficient, value->data, 16) == 0;
    }
    default:
        return 0;
    }
}

static int match_optimizer_filter_node(
    plan_reader *reader, const prdb_sdk_optimizer_plan_v1 *plan,
    const uint8_t *metadata_response, uint64_t metadata_response_len,
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    uint32_t node_index, uint8_t *seen, uint32_t depth) {
    const prdb_sdk_predicate_node_v1 *node;
    uint8_t tag;
    uint64_t tag_offset = reader->offset;
    if (depth > PRDB_SDK_SQL_MAX_EXPRESSION_DEPTH ||
        node_index >= statement->predicate_count || seen[node_index] != 0 ||
        !prdb_sdk_internal_read_u8(reader, &tag)) return 0;
    seen[node_index] = 1;
    node = &statement->predicates[node_index];
    switch (node->node_kind) {
    case PRDB_SDK_SQL_PREDICATE_COMPARE: {
        uint8_t comparison;
        uint8_t column_tag;
        uint8_t column_id[16];
        const uint8_t *column_name = NULL;
        uint32_t column_name_len = 0;
        uint32_t ordinal = UINT32_MAX;
        if (tag != 0x20 || node->comparison < PRDB_SDK_SQL_COMPARE_EQUAL ||
            node->comparison > PRDB_SDK_SQL_COMPARE_GREATER_EQUAL ||
            node->column.offset > statement->sql_len ||
            node->column.length == 0 ||
            node->column.length > statement->sql_len - node->column.offset ||
            !prdb_sdk_internal_valid_identifier(statement->sql + node->column.offset,
                              node->column.length) ||
            !prdb_sdk_internal_read_compare_predicate_header(reader, tag_offset, &comparison) ||
            comparison != node->comparison - PRDB_SDK_SQL_COMPARE_EQUAL ||
            !prdb_sdk_internal_read_u8(reader, &column_tag) || column_tag != 0x10 ||
            !prdb_sdk_internal_read_column_predicate_record(reader, reader->offset - 1,
                                          column_id))
            return 0;
        for (uint32_t index = 0; index < plan->scan_column_count; ++index) {
            if (memcmp(column_id, plan->scan_columns[index].column_id, 16) == 0) {
                ordinal = plan->scan_columns[index].ordinal;
                break;
            }
        }
        if (ordinal == UINT32_MAX ||
            !response_column_name(metadata_response, metadata_response_len,
                                  ordinal, &column_name, &column_name_len) ||
            column_name_len != node->column.length ||
            !identifier_equal_ascii_casefold(
                column_name, statement->sql + node->column.offset,
                node->column.length) ||
            node->parameter_index == 0 ||
            node->parameter_index > parameter_count) return 0;
        return match_optimizer_parameter(
            reader, &parameters[node->parameter_index - 1]);
    }
    case PRDB_SDK_SQL_PREDICATE_AND:
    case PRDB_SDK_SQL_PREDICATE_OR: {
        uint8_t node_tag;
        uint8_t child_count;
        uint8_t expected_tag = node->node_kind == PRDB_SDK_SQL_PREDICATE_AND
                                   ? 0x30
                                   : 0x31;
        return tag == expected_tag &&
               prdb_sdk_internal_read_boolean_predicate_header(reader, tag_offset, &node_tag,
                                             &child_count) &&
               node_tag == expected_tag &&
               child_count == 2 &&
               match_optimizer_filter_node(
                   reader, plan, metadata_response, metadata_response_len,
                   statement, parameters, parameter_count, node->left_child,
                   seen, depth + 1) &&
               match_optimizer_filter_node(
                   reader, plan, metadata_response, metadata_response_len,
                   statement, parameters, parameter_count, node->right_child,
                   seen, depth + 1);
    }
    case PRDB_SDK_SQL_PREDICATE_NOT:
        return tag == 0x32 && prdb_sdk_internal_read_not_predicate_tag(reader, tag_offset) &&
               match_optimizer_filter_node(
                   reader, plan, metadata_response, metadata_response_len,
                   statement, parameters, parameter_count, node->left_child,
                   seen, depth + 1);
    case PRDB_SDK_SQL_PREDICATE_IS_NULL:
    case PRDB_SDK_SQL_PREDICATE_IS_NOT_NULL: {
        uint8_t column_tag;
        uint8_t column_id[16];
        const uint8_t *column_name = NULL;
        uint32_t column_name_len = 0;
        uint32_t ordinal = UINT32_MAX;
        uint8_t expected_tag =
            node->node_kind == PRDB_SDK_SQL_PREDICATE_IS_NULL ? 0x40 : 0x41;
        if (tag != expected_tag || node->left_child != 0 ||
            node->right_child != 0 || node->parameter_index != 0 ||
            node->comparison != 0 ||
            node->column.offset > statement->sql_len ||
            node->column.length == 0 ||
            node->column.length > statement->sql_len - node->column.offset ||
            !prdb_sdk_internal_valid_identifier(statement->sql + node->column.offset,
                              node->column.length) ||
            !prdb_sdk_internal_read_u8(reader, &column_tag) || column_tag != 0x10 ||
            !prdb_sdk_internal_read_column_predicate_record(reader, reader->offset - 1,
                                          column_id))
            return 0;
        for (uint32_t index = 0; index < plan->scan_column_count; ++index) {
            if (memcmp(column_id, plan->scan_columns[index].column_id, 16) == 0) {
                ordinal = plan->scan_columns[index].ordinal;
                break;
            }
        }
        return ordinal != UINT32_MAX &&
               response_column_name(metadata_response, metadata_response_len,
                                    ordinal, &column_name, &column_name_len) &&
               column_name_len == node->column.length &&
               identifier_equal_ascii_casefold(
                   column_name, statement->sql + node->column.offset,
                   node->column.length);
    }
    default:
        return 0;
    }
}

static int32_t validate_select_plan_internal(
    const prdb_host_api_v2 *host, const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    int allow_filter,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len,
    uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len) {
    prdb_sdk_optimizer_plan_v1 plan = {0};
    prdb_sdk_optimizer_filter_v1 filter = {0};
    uint64_t offset;
    uint32_t relation_name_len;
    uint32_t column_count;
    int32_t status;

    if (statement == NULL || statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->sql == NULL || statement->sql_len == 0 || schema == NULL ||
        parameter_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        (parameter_count != 0 && parameters == NULL) ||
        statement->predicate_count > PRDB_SDK_SQL_MAX_PREDICATE_NODES ||
        !prdb_sdk_internal_valid_identifier(schema, schema_len) || statement->projection_star != 0 ||
        (!allow_filter && statement->predicate_count != 0) ||
        statement->projection_count == 0 ||
        statement->projection_count > PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS ||
        statement->has_limit > 1 ||
        statement->relation.offset > statement->sql_len ||
        statement->relation.length == 0 ||
        statement->relation.length > statement->sql_len - statement->relation.offset ||
        !prdb_sdk_internal_valid_identifier(statement->sql + statement->relation.offset,
                          statement->relation.length)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    if ((statement->predicate_count == 0 && statement->predicate_root != 0) ||
        (statement->predicate_count != 0 &&
         statement->predicate_root >= statement->predicate_count)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    for (uint32_t index = 0; index < statement->projection_count; ++index) {
        prdb_sdk_sql_span_v1 projection = statement->projection[index];
        if (projection.offset > statement->sql_len || projection.length == 0 ||
            projection.length > statement->sql_len - projection.offset ||
            !prdb_sdk_internal_valid_identifier(statement->sql + projection.offset,
                              projection.length)) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
        }
    }
    status = prdb_sdk_optimizer_decode_plan_with_filter_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan), &filter,
        sizeof(filter));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    if ((filter.present != 0) != (statement->predicate_count != 0) ||
        plan.schema_name_len != schema_len ||
        memcmp(plan.schema_name, schema, schema_len) != 0 ||
        plan.relation_name_len != statement->relation.length ||
        !identifier_equal_ascii_casefold(
            plan.relation_name, statement->sql + statement->relation.offset,
            statement->relation.length) ||
        plan.projection_count != statement->projection_count ||
        plan.has_limit != statement->has_limit ||
        (plan.has_limit != 0 && plan.limit != statement->limit)) {
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    status = prdb_sdk_optimizer_validate_executor_relation_v1(
        host, plan_payload, plan_payload_len, response, response_capacity,
        response_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;

    offset = 51;
    relation_name_len = prdb_sdk_internal_response_u16(response, offset);
    offset += 2 + relation_name_len;
    column_count = prdb_sdk_internal_response_u16(response, offset);
    offset += 2;
    for (uint32_t ordinal = 0; ordinal < column_count; ++ordinal) {
        uint32_t column_name_len;
        offset += 26;
        column_name_len = prdb_sdk_internal_response_u16(response, offset);
        offset += 2;
        for (uint32_t projection_index = 0;
             projection_index < plan.projection_count; ++projection_index) {
            const prdb_sdk_optimizer_plan_column_v1 *projection =
                &plan.projections[projection_index];
            prdb_sdk_sql_span_v1 requested =
                statement->projection[projection_index];
            if (projection->ordinal == ordinal &&
                (column_name_len != requested.length ||
                 !identifier_equal_ascii_casefold(
                     response + offset, statement->sql + requested.offset,
                     requested.length))) {
                return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
            }
        }
        offset += column_name_len;
    }
    if (filter.present != 0) {
        plan_reader filter_reader = {filter.prefix, filter.prefix_len, 0};
        uint8_t seen[PRDB_SDK_SQL_MAX_PREDICATE_NODES] = {0};
        if (parameters == NULL ||
            !match_optimizer_filter_node(
                &filter_reader, &plan, response, *response_len, statement,
                parameters, parameter_count, statement->predicate_root, seen,
                1) ||
            filter_reader.offset != filter_reader.length) {
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        }
        for (uint32_t index = 0; index < statement->predicate_count; ++index)
            if (seen[index] == 0)
                return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_execute_unfiltered_plan_v1(
    const prdb_host_api_v2 *host, const uint8_t *plan_payload,
    uint64_t plan_payload_len, uint16_t max_candidates,
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
    return prdb_sdk_internal_execute_unfiltered_plan_internal(
        host, plan_payload, plan_payload_len, max_candidates, max_candidates,
        max_batch_bytes, relation_response, relation_response_capacity,
        scan_response, scan_response_capacity, batch_response,
        batch_response_capacity, relation_out, relation_out_capacity,
        relation_columns, relation_column_capacity, source_cells,
        source_cell_capacity, projected_values, projected_value_capacity,
        on_row, row_context, delivered_rows_out);
}

int32_t prdb_sdk_optimizer_validate_select_plan_v1(
    const prdb_host_api_v2 *host, const prdb_sdk_select_v1 *statement,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len,
    uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len) {
    return validate_select_plan_internal(
        host, statement, NULL, 0, 0, schema, schema_len, plan_payload,
        plan_payload_len, response, response_capacity, response_len);
}

int32_t prdb_sdk_optimizer_validate_select_plan_with_parameters_v1(
    const prdb_host_api_v2 *host, const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len,
    uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len) {
    return validate_select_plan_internal(
        host, statement, parameters, parameter_count, 1, schema, schema_len,
        plan_payload, plan_payload_len, response, response_capacity,
        response_len);
}

int32_t prdb_sdk_optimizer_validate_delete_plan_with_parameters_v1(
    const prdb_host_api_v2 *host, const prdb_sdk_delete_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len,
    uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len) {
    const prdb_sdk_select_v1 *predicate;
    prdb_sdk_optimizer_plan_v1 plan = {0};
    prdb_sdk_optimizer_filter_v1 filter = {0};
    plan_reader filter_reader;
    uint8_t seen[PRDB_SDK_SQL_MAX_PREDICATE_NODES] = {0};
    int32_t status;

    if (statement == NULL || statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_DML_ABI_V1_VERSION ||
        statement->sql == NULL || statement->sql_len == 0 ||
        statement->sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES || schema == NULL ||
        !prdb_sdk_internal_valid_identifier(schema, schema_len) ||
        statement->relation.offset > statement->sql_len ||
        statement->relation.length == 0 ||
        statement->relation.length > statement->sql_len -
                                         statement->relation.offset ||
        !prdb_sdk_internal_valid_identifier(statement->sql + statement->relation.offset,
                          statement->relation.length) ||
        parameter_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        (parameter_count != 0 && parameters == NULL)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    predicate = &statement->predicate_statement;
    if (predicate->struct_size != sizeof(*predicate) ||
        predicate->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        predicate->sql != statement->sql ||
        predicate->sql_len != statement->sql_len ||
        predicate->relation.offset != statement->relation.offset ||
        predicate->relation.length != statement->relation.length ||
        predicate->projection_star != 1 || predicate->projection_count != 0 ||
        predicate->has_limit != 0 || predicate->limit != 0 ||
        predicate->predicate_count == 0 ||
        predicate->predicate_count > PRDB_SDK_SQL_MAX_PREDICATE_NODES ||
        predicate->predicate_root >= predicate->predicate_count) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    status = prdb_sdk_optimizer_decode_delete_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan), &filter,
        sizeof(filter));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    if (filter.present == 0 || plan.schema_name_len != schema_len ||
        memcmp(plan.schema_name, schema, schema_len) != 0 ||
        plan.relation_name_len != statement->relation.length ||
        !identifier_equal_ascii_casefold(
            plan.relation_name,
            statement->sql + statement->relation.offset,
            statement->relation.length)) {
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    status = prdb_sdk_optimizer_validate_executor_relation_v1(
        host, plan_payload, plan_payload_len, response, response_capacity,
        response_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;

    filter_reader = (plan_reader){filter.prefix, filter.prefix_len, 0};
    if (!match_optimizer_filter_node(
            &filter_reader, &plan, response, *response_len, predicate,
            parameters, parameter_count, predicate->predicate_root, seen, 1) ||
        filter_reader.offset != filter_reader.length) {
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    for (uint32_t index = 0; index < predicate->predicate_count; ++index) {
        if (seen[index] == 0)
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_validate_update_plan_with_parameters_v1(
    const prdb_host_api_v2 *host, const prdb_sdk_update_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len,
    uint8_t *response, uint64_t response_capacity,
    uint64_t *response_len) {
    const prdb_sdk_select_v1 *predicate;
    prdb_sdk_optimizer_update_plan_v1 update = {0};
    plan_reader filter_reader;
    uint8_t seen[PRDB_SDK_SQL_MAX_PREDICATE_NODES] = {0};
    int32_t status;

    if (statement == NULL || statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_DML_ABI_V1_VERSION ||
        statement->sql == NULL || statement->sql_len == 0 ||
        statement->sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES || schema == NULL ||
        !prdb_sdk_internal_valid_identifier(schema, schema_len) ||
        statement->relation.offset > statement->sql_len ||
        statement->relation.length == 0 ||
        statement->relation.length > statement->sql_len -
                                         statement->relation.offset ||
        !prdb_sdk_internal_valid_identifier(statement->sql + statement->relation.offset,
                          statement->relation.length) ||
        statement->assignment_count == 0 ||
        statement->assignment_count > PRDB_SDK_SQL_MAX_UPDATE_ASSIGNMENTS ||
        parameter_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        (parameter_count != 0 && parameters == NULL)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    predicate = &statement->predicate_statement;
    if (predicate->struct_size != sizeof(*predicate) ||
        predicate->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        predicate->sql != statement->sql ||
        predicate->sql_len != statement->sql_len ||
        predicate->relation.offset != statement->relation.offset ||
        predicate->relation.length != statement->relation.length ||
        predicate->projection_star != 0 ||
        predicate->projection_count != statement->assignment_count ||
        predicate->has_limit != 0 || predicate->limit != 0 ||
        predicate->predicate_count == 0 ||
        predicate->predicate_count > PRDB_SDK_SQL_MAX_PREDICATE_NODES ||
        predicate->predicate_root >= predicate->predicate_count) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    for (uint32_t index = 0; index < statement->assignment_count; ++index) {
        const prdb_sdk_sql_update_assignment_v1 *assignment =
            &statement->assignments[index];
        if (assignment->column.offset > statement->sql_len ||
            assignment->column.length == 0 ||
            assignment->column.length >
                statement->sql_len - assignment->column.offset ||
            !prdb_sdk_internal_valid_identifier(statement->sql + assignment->column.offset,
                              assignment->column.length) ||
            predicate->projection[index].offset != assignment->column.offset ||
            predicate->projection[index].length != assignment->column.length) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
        }
    }
    status = prdb_sdk_optimizer_decode_update_plan_v1(
        plan_payload, plan_payload_len, &update, sizeof(update));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    if (update.filter.present == 0 ||
        update.assignment_count != statement->assignment_count ||
        update.plan.schema_name_len != schema_len ||
        memcmp(update.plan.schema_name, schema, schema_len) != 0 ||
        update.plan.relation_name_len != statement->relation.length ||
        !identifier_equal_ascii_casefold(
            update.plan.relation_name,
            statement->sql + statement->relation.offset,
            statement->relation.length)) {
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }

    status = prdb_sdk_optimizer_validate_executor_relation_v1(
        host, plan_payload, plan_payload_len, response, response_capacity,
        response_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;

    filter_reader = (plan_reader){update.filter.prefix,
                                  update.filter.prefix_len, 0};
    if (!match_optimizer_filter_node(
            &filter_reader, &update.plan, response, *response_len, predicate,
            parameters, parameter_count, predicate->predicate_root, seen, 1) ||
        filter_reader.offset != filter_reader.length) {
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    for (uint32_t index = 0; index < predicate->predicate_count; ++index) {
        if (seen[index] == 0)
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }

    for (uint32_t index = 0; index < statement->assignment_count; ++index) {
        const prdb_sdk_sql_update_assignment_v1 *requested =
            &statement->assignments[index];
        const prdb_sdk_optimizer_assignment_v1 *planned =
            &update.assignments[index];
        const uint8_t *column_name = NULL;
        uint32_t column_name_len = 0;
        if (!response_column_name(response, *response_len,
                                  planned->column.ordinal, &column_name,
                                  &column_name_len) ||
            column_name_len != requested->column.length ||
            !identifier_equal_ascii_casefold(
                column_name, statement->sql + requested->column.offset,
                requested->column.length)) {
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        }
        if (requested->kind == PRDB_SDK_SQL_UPDATE_PARAMETER) {
            const prdb_sdk_value_v1 *value;
            if (requested->parameter_index == 0 ||
                requested->parameter_index > parameter_count ||
                planned->value_kind !=
                    PRDB_OPTIMIZER_UPDATE_PARAMETER_VALUE ||
                planned->parameter_index != requested->parameter_index) {
                return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
            }
            value = &parameters[requested->parameter_index - 1];
            if (prdb_sdk_value_validate_v1(value) != PRDB_SDK_VALUE_VALID ||
                value->state != PRDB_SDK_VALUE_STATE_VALUE ||
                value->type_code != planned->column.type_code) {
                return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
            }
        } else if (requested->kind == PRDB_SDK_SQL_UPDATE_NULL) {
            if (requested->parameter_index != 0 ||
                planned->value_kind != PRDB_OPTIMIZER_UPDATE_NULL_VALUE ||
                planned->parameter_index != 0 ||
                planned->column.nullable == 0) {
                return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
            }
        } else {
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        }
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_execute_delete_ast_v1(
    const prdb_host_api_v5 *host, const prdb_sdk_delete_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns,
    uint32_t relation_column_capacity,
    prdb_sdk_select_binding_v1 *binding, uint32_t binding_capacity,
    prdb_sdk_value_v1 *cell_scratch, uint32_t cell_scratch_capacity,
    uint8_t *target_handles, uint32_t target_capacity,
    uint32_t *deleted_count_out) {
    prdb_host_api_v2 base_host = {0};
    prdb_sdk_optimizer_response_v1 decoded_response = {0};
    prdb_sdk_batch_v1 batch = {0};
    prdb_sdk_row_iterator_v1 rows = {0};
    prdb_sdk_row_v1 row = {0};
    uint8_t cursor[16] = {0};
    uint64_t request_len = 0;
    uint64_t optimizer_response_len = 0;
    uint64_t relation_response_len = 0;
    uint64_t scan_response_len = 0;
    uint64_t batch_response_len = 0;
    uint32_t target_count = 0;
    uint32_t page_count = 0;
    uint32_t close_count = 0;
    uint32_t scan_opened = 0;
    int cursor_open = 0;
    int32_t final_status = PRDB_SDK_OPTIMIZER_PLAN_OK;
    int32_t status;
    const prdb_sdk_select_v1 *predicate;

    if (host == NULL ||
        host->v4.v3.struct_size < sizeof(prdb_host_api_v3) ||
        host->v4.v3.abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->v4.v3.context == NULL ||
        host->v4.v3.executor_request == NULL ||
        host->v4.v3.statement_failure == NULL ||
        host->v4.optimizer_request == NULL || host->optimizer_request == NULL ||
        request == NULL || optimizer_response == NULL ||
        relation_response == NULL || scan_response == NULL ||
        batch_response == NULL || relation_out == NULL ||
        relation_columns == NULL || binding == NULL || cell_scratch == NULL ||
        target_handles == NULL || deleted_count_out == NULL ||
        relation_out_capacity < sizeof(*relation_out) ||
        binding_capacity < sizeof(*binding) ||
        parameter_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        (parameter_count != 0 && parameters == NULL) || max_candidates == 0 ||
        max_candidates > PRDB_SDK_BATCH_MAX_ROWS || max_batch_bytes < 8 ||
        max_batch_bytes > PRDB_SDK_BATCH_MAX_BYTES ||
        batch_response_capacity < max_batch_bytes || request_capacity == 0 ||
        request_capacity > PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES ||
        optimizer_response_capacity == 0 ||
        optimizer_response_capacity > PRDB_QUERY_MAX_IPC_REQUEST_BYTES ||
        relation_response_capacity == 0 || scan_response_capacity == 0 ||
        relation_column_capacity == 0 || cell_scratch_capacity == 0 ||
        target_capacity == 0 ||
        target_capacity > PRDB_SDK_OPTIMIZER_PLAN_MAX_DELETE_TARGETS ||
        prdb_sdk_internal_ranges_overlap(request, request_capacity, optimizer_response,
                       optimizer_response_capacity) ||
        prdb_sdk_internal_ranges_overlap(request, request_capacity, relation_response,
                       relation_response_capacity) ||
        prdb_sdk_internal_ranges_overlap(optimizer_response, optimizer_response_capacity,
                       relation_response, relation_response_capacity) ||
        prdb_sdk_internal_ranges_overlap(relation_response, relation_response_capacity,
                       scan_response, scan_response_capacity) ||
        prdb_sdk_internal_ranges_overlap(relation_response, relation_response_capacity,
                       batch_response, batch_response_capacity) ||
        prdb_sdk_internal_ranges_overlap(scan_response, scan_response_capacity, batch_response,
                       batch_response_capacity) ||
        prdb_sdk_internal_ranges_overlap(target_handles, (uint64_t)target_capacity * 16,
                       request, request_capacity) ||
        prdb_sdk_internal_ranges_overlap(target_handles, (uint64_t)target_capacity * 16,
                       optimizer_response, optimizer_response_capacity) ||
        prdb_sdk_internal_ranges_overlap(target_handles, (uint64_t)target_capacity * 16,
                       relation_response, relation_response_capacity) ||
        prdb_sdk_internal_ranges_overlap(target_handles, (uint64_t)target_capacity * 16,
                       scan_response, scan_response_capacity) ||
        prdb_sdk_internal_ranges_overlap(target_handles, (uint64_t)target_capacity * 16,
                       batch_response, batch_response_capacity)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    *deleted_count_out = 0;
    base_host.struct_size = sizeof(base_host);
    base_host.abi_version = PRDB_QUERY_ABI_V2_VERSION;
    base_host.context = host->v4.v3.context;
    base_host.executor_request = host->v4.v3.executor_request;
    base_host.planner_request = host->v4.v3.planner_request;
    base_host.write_result = host->v4.v3.write_result;
    predicate = &statement->predicate_statement;

    status = prdb_sdk_optimizer_encode_delete_v1(
        statement, parameters, parameter_count, schema, schema_len, request,
        request_capacity, &request_len);
    if (status == PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED)
        return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
    if (status == PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL ||
        status == PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED)
        return PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL;
    if (status != PRDB_SDK_OPTIMIZER_ENCODE_OK)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;

    status = prdb_sdk_optimizer_submit_delete_v1(
        host, request, request_len, optimizer_response,
        optimizer_response_capacity, &optimizer_response_len,
        &decoded_response, sizeof(decoded_response));
    if (status == PRDB_SDK_OPTIMIZER_RESULT_RESPONSE_TOO_SMALL) {
        status = PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL;
    } else if (status != PRDB_SDK_OPTIMIZER_RESULT_OK) {
        status = status == PRDB_SDK_OPTIMIZER_RESULT_INVALID_ARGUMENT
                     ? PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT
                     : PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
    } else if (decoded_response.status ==
               PRDB_SDK_OPTIMIZER_STATUS_UNAVAILABLE) {
        status = PRDB_SDK_OPTIMIZER_PLAN_UNAVAILABLE;
    } else if (decoded_response.status !=
               PRDB_SDK_OPTIMIZER_STATUS_SUCCESS) {
        status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
    }
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK)
        return latch_optimizer_statement_failure(&host->v4, status);

    status = prdb_sdk_optimizer_validate_delete_plan_with_parameters_v1(
        &base_host, statement, parameters, parameter_count, schema, schema_len,
        decoded_response.plan, decoded_response.plan_len, relation_response,
        relation_response_capacity, &relation_response_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK)
        return latch_optimizer_statement_failure(&host->v4, status);

    status = prdb_sdk_optimizer_open_plan_scan_v1(
        &base_host, decoded_response.plan, decoded_response.plan_len,
        relation_response, relation_response_capacity, &relation_response_len,
        scan_response, scan_response_capacity, &scan_response_len,
        relation_out, relation_out_capacity, relation_columns,
        relation_column_capacity, cursor, &scan_opened);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK)
        return latch_optimizer_statement_failure(&host->v4, status);
    if (relation_out->column_count > PRDB_SDK_SQL_MAX_RELATION_COLUMNS ||
        relation_out->column_count > cell_scratch_capacity) {
        return latch_optimizer_statement_failure(
            &host->v4, PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT);
    }
    status = prdb_sdk_sql_bind_select_v1(
        predicate, relation_out->name, relation_out->name_len,
        relation_columns, relation_out->column_count, binding,
        binding_capacity);
    if (status != PRDB_SDK_SQL_BIND_OK) {
        return latch_optimizer_statement_failure(
            &host->v4, PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    }
    if (scan_opened == 0) return PRDB_SDK_OPTIMIZER_PLAN_OK;
    cursor_open = 1;

    while (page_count < PRDB_SDK_OPTIMIZER_PLAN_MAX_NEXT_REQUESTS) {
        int32_t cancelled = 0;
        status = prdb_sdk_executor_check_cancelled_v1(&base_host, &cancelled);
        if (status != PRDB_SDK_EXECUTOR_OK || cancelled) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_CANCELLED;
            break;
        }
        status = prdb_sdk_executor_next_batch_v1(
            &base_host, cursor, max_candidates, max_batch_bytes, batch_response,
            batch_response_capacity, &batch_response_len, &batch, sizeof(batch));
        if (status != PRDB_SDK_EXECUTOR_OK) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
            break;
        }
        ++page_count;
        if (batch.finished) {
            cursor_open = 0;
        } else {
            memcpy(cursor, batch.continuation, sizeof(cursor));
        }
        status = prdb_sdk_batch_rows_begin_v1(&batch, &rows, sizeof(rows));
        if (status != PRDB_SDK_BATCH_OK) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
            break;
        }
        while (rows.rows_remaining != 0) {
            uint32_t truth = PRDB_SDK_SQL_TRUTH_UNKNOWN;
            status = prdb_sdk_batch_row_next_v1(&rows, &row, sizeof(row));
            if (status != PRDB_SDK_BATCH_OK) {
                final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
                break;
            }
            status = prdb_sdk_sql_eval_bound_select_row_v1(
                predicate, binding, parameters, parameter_count, &row,
                cell_scratch, cell_scratch_capacity, &truth);
            if (status != PRDB_SDK_SQL_EVAL_OK) {
                final_status = PRDB_SDK_OPTIMIZER_PLAN_FILTER_ERROR;
                break;
            }
            if (truth == PRDB_SDK_SQL_TRUTH_TRUE) {
                if (target_count >= target_capacity) {
                    final_status = PRDB_SDK_OPTIMIZER_PLAN_REQUEST_LIMIT;
                    break;
                }
                memcpy(target_handles + (uint64_t)target_count * 16,
                       row.handle, 16);
                ++target_count;
            }
        }
        if (final_status != PRDB_SDK_OPTIMIZER_PLAN_OK || batch.finished)
            break;
    }
    if (cursor_open && final_status == PRDB_SDK_OPTIMIZER_PLAN_OK)
        final_status = PRDB_SDK_OPTIMIZER_PLAN_REQUEST_LIMIT;
    if (cursor_open) {
        status = prdb_sdk_executor_close_cursor_v1(
            &base_host, cursor, scan_response, scan_response_capacity,
            &scan_response_len);
        ++close_count;
        if (status != PRDB_SDK_EXECUTOR_OK &&
            final_status == PRDB_SDK_OPTIMIZER_PLAN_OK) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
        }
    }
    if (final_status != PRDB_SDK_OPTIMIZER_PLAN_OK)
        return latch_optimizer_statement_failure(&host->v4, final_status);
    if (4U + 2U * page_count + close_count + target_count > 256U) {
        return latch_optimizer_statement_failure(
            &host->v4, PRDB_SDK_OPTIMIZER_PLAN_REQUEST_LIMIT);
    }
    for (uint32_t index = 0; index < target_count; ++index) {
        status = prdb_sdk_executor_stage_delete_v1(
            &base_host, target_handles + (uint64_t)index * 16,
            scan_response, scan_response_capacity, &scan_response_len);
        if (status != PRDB_SDK_EXECUTOR_OK) {
            return latch_optimizer_statement_failure(
                &host->v4, PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR);
        }
    }
    *deleted_count_out = target_count;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_execute_update_ast_v1(
    const prdb_host_api_v5 *host, const prdb_sdk_update_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns,
    uint32_t relation_column_capacity,
    prdb_sdk_select_binding_v1 *binding, uint32_t binding_capacity,
    prdb_sdk_value_v1 *cell_scratch, uint32_t cell_scratch_capacity,
    uint8_t *target_handles, uint32_t target_capacity,
    uint32_t *updated_count_out) {
    prdb_host_api_v2 base_host = {0};
    prdb_sdk_optimizer_response_v1 decoded_response = {0};
    prdb_sdk_optimizer_update_plan_v1 update = {0};
    prdb_sdk_batch_v1 batch = {0};
    prdb_sdk_row_iterator_v1 rows = {0};
    prdb_sdk_row_v1 row = {0};
    prdb_sdk_executor_update_patch_v1 patches[
        PRDB_SDK_OPTIMIZER_PLAN_MAX_UPDATE_ASSIGNMENTS] = {{0}};
    uint8_t cursor[16] = {0};
    uint64_t request_len = 0;
    uint64_t optimizer_response_len = 0;
    uint64_t relation_response_len = 0;
    uint64_t scan_response_len = 0;
    uint64_t batch_response_len = 0;
    uint32_t target_count = 0;
    uint32_t page_count = 0;
    uint32_t close_count = 0;
    uint32_t scan_opened = 0;
    int cursor_open = 0;
    int32_t final_status = PRDB_SDK_OPTIMIZER_PLAN_OK;
    int32_t status;
    const prdb_sdk_select_v1 *predicate;

    if (host == NULL ||
        host->v4.v3.struct_size < sizeof(prdb_host_api_v3) ||
        host->v4.v3.abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->v4.v3.context == NULL ||
        host->v4.v3.executor_request == NULL ||
        host->v4.v3.statement_failure == NULL ||
        host->v4.optimizer_request == NULL || host->optimizer_request == NULL ||
        request == NULL || optimizer_response == NULL ||
        relation_response == NULL || scan_response == NULL ||
        batch_response == NULL || relation_out == NULL ||
        relation_columns == NULL || binding == NULL || cell_scratch == NULL ||
        target_handles == NULL || updated_count_out == NULL ||
        relation_out_capacity < sizeof(*relation_out) ||
        binding_capacity < sizeof(*binding) ||
        parameter_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        (parameter_count != 0 && parameters == NULL) || max_candidates == 0 ||
        max_candidates > PRDB_SDK_BATCH_MAX_ROWS || max_batch_bytes < 8 ||
        max_batch_bytes > PRDB_SDK_BATCH_MAX_BYTES ||
        batch_response_capacity < max_batch_bytes || request_capacity == 0 ||
        request_capacity > PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES ||
        optimizer_response_capacity == 0 ||
        optimizer_response_capacity > PRDB_QUERY_MAX_IPC_REQUEST_BYTES ||
        relation_response_capacity == 0 || scan_response_capacity == 0 ||
        relation_column_capacity == 0 || cell_scratch_capacity == 0 ||
        target_capacity == 0 ||
        target_capacity > PRDB_SDK_OPTIMIZER_PLAN_MAX_DELETE_TARGETS ||
        prdb_sdk_internal_ranges_overlap(request, request_capacity, optimizer_response,
                       optimizer_response_capacity) ||
        prdb_sdk_internal_ranges_overlap(request, request_capacity, relation_response,
                       relation_response_capacity) ||
        prdb_sdk_internal_ranges_overlap(optimizer_response, optimizer_response_capacity,
                       relation_response, relation_response_capacity) ||
        prdb_sdk_internal_ranges_overlap(relation_response, relation_response_capacity,
                       scan_response, scan_response_capacity) ||
        prdb_sdk_internal_ranges_overlap(relation_response, relation_response_capacity,
                       batch_response, batch_response_capacity) ||
        prdb_sdk_internal_ranges_overlap(scan_response, scan_response_capacity, batch_response,
                       batch_response_capacity) ||
        prdb_sdk_internal_ranges_overlap(target_handles, (uint64_t)target_capacity * 16,
                       request, request_capacity) ||
        prdb_sdk_internal_ranges_overlap(target_handles, (uint64_t)target_capacity * 16,
                       optimizer_response, optimizer_response_capacity) ||
        prdb_sdk_internal_ranges_overlap(target_handles, (uint64_t)target_capacity * 16,
                       relation_response, relation_response_capacity) ||
        prdb_sdk_internal_ranges_overlap(target_handles, (uint64_t)target_capacity * 16,
                       scan_response, scan_response_capacity) ||
        prdb_sdk_internal_ranges_overlap(target_handles, (uint64_t)target_capacity * 16,
                       batch_response, batch_response_capacity)) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    *updated_count_out = 0;
    base_host.struct_size = sizeof(base_host);
    base_host.abi_version = PRDB_QUERY_ABI_V2_VERSION;
    base_host.context = host->v4.v3.context;
    base_host.executor_request = host->v4.v3.executor_request;
    base_host.planner_request = host->v4.v3.planner_request;
    base_host.write_result = host->v4.v3.write_result;
    predicate = &statement->predicate_statement;

    status = prdb_sdk_optimizer_encode_update_v1(
        statement, parameters, parameter_count, schema, schema_len, request,
        request_capacity, &request_len);
    if (status == PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED)
        return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
    if (status == PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL ||
        status == PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED)
        return PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL;
    if (status != PRDB_SDK_OPTIMIZER_ENCODE_OK)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;

    status = prdb_sdk_optimizer_submit_update_v1(
        host, request, request_len, optimizer_response,
        optimizer_response_capacity, &optimizer_response_len,
        &decoded_response, sizeof(decoded_response));
    if (status == PRDB_SDK_OPTIMIZER_RESULT_RESPONSE_TOO_SMALL) {
        status = PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL;
    } else if (status != PRDB_SDK_OPTIMIZER_RESULT_OK) {
        status = status == PRDB_SDK_OPTIMIZER_RESULT_INVALID_ARGUMENT
                     ? PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT
                     : PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
    } else if (decoded_response.status ==
               PRDB_SDK_OPTIMIZER_STATUS_UNAVAILABLE) {
        status = PRDB_SDK_OPTIMIZER_PLAN_UNAVAILABLE;
    } else if (decoded_response.status !=
               PRDB_SDK_OPTIMIZER_STATUS_SUCCESS) {
        status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
    }
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK)
        return latch_optimizer_statement_failure(&host->v4, status);

    status = prdb_sdk_optimizer_validate_update_plan_with_parameters_v1(
        &base_host, statement, parameters, parameter_count, schema, schema_len,
        decoded_response.plan, decoded_response.plan_len, relation_response,
        relation_response_capacity, &relation_response_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK)
        return latch_optimizer_statement_failure(&host->v4, status);

    status = prdb_sdk_optimizer_open_plan_scan_v1(
        &base_host, decoded_response.plan, decoded_response.plan_len,
        relation_response, relation_response_capacity, &relation_response_len,
        scan_response, scan_response_capacity, &scan_response_len,
        relation_out, relation_out_capacity, relation_columns,
        relation_column_capacity, cursor, &scan_opened);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK)
        return latch_optimizer_statement_failure(&host->v4, status);
    if (relation_out->column_count > PRDB_SDK_SQL_MAX_RELATION_COLUMNS ||
        relation_out->column_count > cell_scratch_capacity) {
        return latch_optimizer_statement_failure(
            &host->v4, PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT);
    }
    status = prdb_sdk_sql_bind_select_v1(
        predicate, relation_out->name, relation_out->name_len,
        relation_columns, relation_out->column_count, binding,
        binding_capacity);
    if (status != PRDB_SDK_SQL_BIND_OK) {
        return latch_optimizer_statement_failure(
            &host->v4, PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    }
    if (scan_opened != 0) {
        cursor_open = 1;
        while (page_count < PRDB_SDK_OPTIMIZER_PLAN_MAX_NEXT_REQUESTS) {
            int32_t cancelled = 0;
            status = prdb_sdk_executor_check_cancelled_v1(&base_host,
                                                          &cancelled);
            if (status != PRDB_SDK_EXECUTOR_OK || cancelled) {
                final_status = PRDB_SDK_OPTIMIZER_PLAN_CANCELLED;
                break;
            }
            status = prdb_sdk_executor_next_batch_v1(
                &base_host, cursor, max_candidates, max_batch_bytes,
                batch_response, batch_response_capacity, &batch_response_len,
                &batch, sizeof(batch));
            if (status != PRDB_SDK_EXECUTOR_OK) {
                final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
                break;
            }
            ++page_count;
            if (batch.finished) {
                cursor_open = 0;
            } else {
                memcpy(cursor, batch.continuation, sizeof(cursor));
            }
            status = prdb_sdk_batch_rows_begin_v1(&batch, &rows, sizeof(rows));
            if (status != PRDB_SDK_BATCH_OK) {
                final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
                break;
            }
            while (rows.rows_remaining != 0) {
                uint32_t truth = PRDB_SDK_SQL_TRUTH_UNKNOWN;
                status = prdb_sdk_batch_row_next_v1(&rows, &row, sizeof(row));
                if (status != PRDB_SDK_BATCH_OK) {
                    final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
                    break;
                }
                status = prdb_sdk_sql_eval_bound_select_row_v1(
                    predicate, binding, parameters, parameter_count, &row,
                    cell_scratch, cell_scratch_capacity, &truth);
                if (status != PRDB_SDK_SQL_EVAL_OK) {
                    final_status = PRDB_SDK_OPTIMIZER_PLAN_FILTER_ERROR;
                    break;
                }
                if (truth == PRDB_SDK_SQL_TRUTH_TRUE) {
                    if (target_count >= target_capacity) {
                        final_status = PRDB_SDK_OPTIMIZER_PLAN_REQUEST_LIMIT;
                        break;
                    }
                    memcpy(target_handles + (uint64_t)target_count * 16,
                           row.handle, 16);
                    ++target_count;
                }
            }
            if (final_status != PRDB_SDK_OPTIMIZER_PLAN_OK || batch.finished)
                break;
        }
    }
    if (cursor_open && final_status == PRDB_SDK_OPTIMIZER_PLAN_OK)
        final_status = PRDB_SDK_OPTIMIZER_PLAN_REQUEST_LIMIT;
    if (cursor_open) {
        status = prdb_sdk_executor_close_cursor_v1(
            &base_host, cursor, scan_response, scan_response_capacity,
            &scan_response_len);
        ++close_count;
        if (status != PRDB_SDK_EXECUTOR_OK &&
            final_status == PRDB_SDK_OPTIMIZER_PLAN_OK) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
        }
    }
    if (final_status != PRDB_SDK_OPTIMIZER_PLAN_OK)
        return latch_optimizer_statement_failure(&host->v4, final_status);
    if (4U + 2U * page_count + close_count + target_count > 256U) {
        return latch_optimizer_statement_failure(
            &host->v4, PRDB_SDK_OPTIMIZER_PLAN_REQUEST_LIMIT);
    }

    status = prdb_sdk_optimizer_decode_update_plan_v1(
        decoded_response.plan, decoded_response.plan_len, &update,
        sizeof(update));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK)
        return latch_optimizer_statement_failure(&host->v4, status);
    for (uint32_t index = 0; index < update.assignment_count; ++index) {
        patches[index].struct_size = sizeof(patches[index]);
        patches[index].abi_version = PRDB_SDK_EXECUTOR_ABI_V1_VERSION;
        patches[index].ordinal = update.assignments[index].column.ordinal;
        if (update.assignments[index].value_kind ==
            PRDB_OPTIMIZER_UPDATE_PARAMETER_VALUE) {
            patches[index].value =
                parameters[update.assignments[index].parameter_index - 1];
        } else {
            patches[index].value = (prdb_sdk_value_v1){
                sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
                PRDB_SDK_VALUE_STATE_NULL, 0, NULL, 0};
        }
    }
    for (uint32_t index = 0; index < target_count; ++index) {
        status = prdb_sdk_executor_stage_update_v1(
            &base_host, target_handles + (uint64_t)index * 16, patches,
            (uint16_t)update.assignment_count, request, request_capacity,
            scan_response, scan_response_capacity, &scan_response_len);
        if (status != PRDB_SDK_EXECUTOR_OK) {
            return latch_optimizer_statement_failure(
                &host->v4, PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR);
        }
    }
    *updated_count_out = target_count;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

static int32_t execute_select_ast_internal(
    const prdb_host_api_v4 *host, const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    int parameterized,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint16_t max_index_candidates,
    uint32_t max_batch_bytes, uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns,
    uint32_t relation_column_capacity, prdb_sdk_value_v1 *source_cells,
    uint32_t source_cell_capacity, prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out) {
    prdb_sdk_optimizer_response_v1 decoded_response = {0};
    prdb_host_api_v2 base_host = {0};
    uint64_t request_len = 0;
    uint64_t optimizer_response_len = 0;
    int32_t status;

    if (host == NULL || host->v3.struct_size < sizeof(host->v3) ||
        host->v3.abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->v3.executor_request == NULL ||
        host->v3.statement_failure == NULL || host->optimizer_request == NULL ||
        delivered_rows_out == NULL) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    *delivered_rows_out = 0;
    status = parameterized != 0
                 ? prdb_sdk_optimizer_encode_select_with_parameters_v1(
                       statement, parameters, parameter_count, schema,
                       schema_len, request, request_capacity, &request_len)
                 : prdb_sdk_optimizer_encode_select_v1(
                       statement, schema, schema_len, request,
                       request_capacity, &request_len);
    if (status == PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED) {
        return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
    }
    if (status == PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL ||
        status == PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED) {
        return PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL;
    }
    if (status != PRDB_SDK_OPTIMIZER_ENCODE_OK) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    status = prdb_sdk_optimizer_submit_select_v1(
        host, request, request_len, optimizer_response,
        optimizer_response_capacity, &optimizer_response_len,
        &decoded_response, sizeof(decoded_response));
    if (status == PRDB_SDK_OPTIMIZER_RESULT_RESPONSE_TOO_SMALL) {
        status = PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL;
    } else if (status != PRDB_SDK_OPTIMIZER_RESULT_OK) {
        status = status == PRDB_SDK_OPTIMIZER_RESULT_INVALID_ARGUMENT
                     ? PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT
                     : PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
    } else if (decoded_response.status ==
               PRDB_SDK_OPTIMIZER_STATUS_UNAVAILABLE) {
        status = PRDB_SDK_OPTIMIZER_PLAN_UNAVAILABLE;
    } else if (decoded_response.status !=
               PRDB_SDK_OPTIMIZER_STATUS_SUCCESS) {
        status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
    }
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) {
        return latch_optimizer_statement_failure(host, status);
    }
    base_host.struct_size = sizeof(base_host);
    base_host.abi_version = PRDB_QUERY_ABI_V2_VERSION;
    base_host.context = host->v3.context;
    base_host.executor_request = host->v3.executor_request;
    base_host.planner_request = host->v3.planner_request;
    base_host.write_result = host->v3.write_result;
    status = parameterized != 0
                 ? prdb_sdk_optimizer_validate_select_plan_with_parameters_v1(
                       &base_host, statement, parameters, parameter_count,
                       schema, schema_len, decoded_response.plan,
                       decoded_response.plan_len, relation_response,
                       relation_response_capacity, &optimizer_response_len)
                 : prdb_sdk_optimizer_validate_select_plan_v1(
                       &base_host, statement, schema, schema_len,
                       decoded_response.plan, decoded_response.plan_len,
                       relation_response, relation_response_capacity,
                       &optimizer_response_len);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) {
        return latch_optimizer_statement_failure(host, status);
    }
    status = prdb_sdk_internal_execute_unfiltered_plan_internal(
        &base_host, decoded_response.plan, decoded_response.plan_len,
        max_candidates, max_index_candidates, max_batch_bytes, relation_response,
        relation_response_capacity, scan_response, scan_response_capacity,
        batch_response, batch_response_capacity, relation_out,
        relation_out_capacity, relation_columns, relation_column_capacity,
        source_cells, source_cell_capacity, projected_values,
        projected_value_capacity, on_row, row_context, delivered_rows_out);
    if (status == PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED) {
        status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
    }
    return latch_optimizer_statement_failure(host, status);
}

int32_t prdb_sdk_optimizer_execute_ordered_select_ast_v1(
    const prdb_host_api_v4 *host, const prdb_sdk_select_v1 *statement,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns,
    uint32_t relation_column_capacity, prdb_sdk_value_v1 *source_cells,
    uint32_t source_cell_capacity, prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity, uint8_t *materialized_rows,
    uint64_t materialized_rows_capacity,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out) {
    prdb_sdk_optimizer_response_v1 response = {0};
    prdb_sdk_optimizer_ordered_select_plan_v1 ordered = {0};
    prdb_sdk_select_binding_v1 binding = {0};
    prdb_host_api_v2 executor_host = {0};
    prdb_sdk_batch_v1 batch = {0};
    prdb_sdk_row_iterator_v1 rows = {0};
    prdb_sdk_row_v1 ordered_rows[PRDB_SDK_SQL_ORDER_MAX_ROWS];
    prdb_sdk_value_v1 order_keys[PRDB_SDK_SQL_ORDER_MAX_ROWS];
    uint32_t order_indices[PRDB_SDK_SQL_ORDER_MAX_ROWS];
    uint8_t cursor[16] = {0};
    uint64_t optimizer_response_len = 0;
    uint64_t relation_response_len = 0;
    uint64_t scan_response_len = 0;
    uint64_t batch_response_len = 0;
    uint64_t materialized_offset = 5;
    uint32_t materialized_count = 0;
    uint32_t page_count = 0;
    uint32_t cursor_open = 0;
    uint32_t delivered = 0;
    int32_t status;
    int32_t final_status = PRDB_SDK_OPTIMIZER_PLAN_OK;
    const void *output_regions[10];
    uint64_t output_region_lengths[10];

    if (host == NULL || host->v3.struct_size < sizeof(host->v3) ||
        host->v3.abi_version != PRDB_QUERY_ABI_V3_VERSION ||
        host->v3.statement_failure == NULL ||
        host->v3.executor_request == NULL || host->optimizer_request == NULL ||
        statement == NULL || statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->sql == NULL || statement->sql_len == 0 ||
        schema == NULL || request == NULL || optimizer_response == NULL ||
        relation_response == NULL || scan_response == NULL ||
        batch_response == NULL || relation_out == NULL ||
        relation_columns == NULL || source_cells == NULL ||
        projected_values == NULL || materialized_rows == NULL ||
        on_row == NULL || delivered_rows_out == NULL ||
        max_candidates == 0 || max_candidates > PRDB_SDK_BATCH_MAX_ROWS ||
        max_batch_bytes < 8 || max_batch_bytes > PRDB_SDK_BATCH_MAX_BYTES ||
        batch_response_capacity < max_batch_bytes ||
        materialized_rows_capacity < 5 ||
        materialized_rows_capacity >
            PRDB_SDK_SQL_EXEC_MAX_MATERIALIZED_BYTES ||
        relation_column_capacity == 0 || source_cell_capacity == 0 ||
        projected_value_capacity == 0 ||
        request_capacity > PRDB_SDK_OPTIMIZER_MAX_REQUEST_BYTES ||
        optimizer_response_capacity == 0 ||
        relation_response_capacity == 0 || scan_response_capacity == 0 ||
        schema_len == 0 || schema_len > 63) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    if (prdb_sdk_internal_ranges_overlap(delivered_rows_out, sizeof(*delivered_rows_out),
                       statement, sizeof(*statement)) ||
        prdb_sdk_internal_ranges_overlap(delivered_rows_out, sizeof(*delivered_rows_out),
                       schema, schema_len) ||
        prdb_sdk_internal_ranges_overlap(delivered_rows_out, sizeof(*delivered_rows_out), host,
                       sizeof(*host))) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    output_regions[0] = request;
    output_region_lengths[0] = request_capacity;
    output_regions[1] = optimizer_response;
    output_region_lengths[1] = optimizer_response_capacity;
    output_regions[2] = relation_response;
    output_region_lengths[2] = relation_response_capacity;
    output_regions[3] = scan_response;
    output_region_lengths[3] = scan_response_capacity;
    output_regions[4] = batch_response;
    output_region_lengths[4] = batch_response_capacity;
    output_regions[5] = materialized_rows;
    output_region_lengths[5] = materialized_rows_capacity;
    output_regions[6] = relation_out;
    output_region_lengths[6] = relation_out_capacity;
    output_regions[7] = relation_columns;
    output_region_lengths[7] =
        (uint64_t)relation_column_capacity * sizeof(*relation_columns);
    output_regions[8] = source_cells;
    output_region_lengths[8] =
        (uint64_t)source_cell_capacity * sizeof(*source_cells);
    output_regions[9] = projected_values;
    output_region_lengths[9] =
        (uint64_t)projected_value_capacity * sizeof(*projected_values);
    for (uint32_t left = 0; left < 10; ++left) {
        if (prdb_sdk_internal_ranges_overlap(output_regions[left], output_region_lengths[left],
                           delivered_rows_out,
                           sizeof(*delivered_rows_out)) ||
            prdb_sdk_internal_ranges_overlap(output_regions[left], output_region_lengths[left],
                           statement, sizeof(*statement)) ||
            prdb_sdk_internal_ranges_overlap(output_regions[left], output_region_lengths[left],
                           statement->sql,
                           statement->sql_len) ||
            prdb_sdk_internal_ranges_overlap(output_regions[left], output_region_lengths[left],
                           schema, schema_len) ||
            prdb_sdk_internal_ranges_overlap(output_regions[left], output_region_lengths[left],
                           host, sizeof(*host))) {
            return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
        }
        for (uint32_t right = left + 1; right < 10; ++right) {
            if (prdb_sdk_internal_ranges_overlap(output_regions[left],
                               output_region_lengths[left],
                               output_regions[right],
                               output_region_lengths[right])) {
                return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
            }
        }
    }
    *delivered_rows_out = 0;
    status = prdb_sdk_optimizer_encode_ordered_select_v1(
        statement, schema, schema_len, request, request_capacity,
        &optimizer_response_len);
    if (status == PRDB_SDK_OPTIMIZER_ENCODE_UNSUPPORTED) {
        return PRDB_SDK_OPTIMIZER_PLAN_UNSUPPORTED;
    }
    if (status == PRDB_SDK_OPTIMIZER_ENCODE_BUFFER_TOO_SMALL ||
        status == PRDB_SDK_OPTIMIZER_ENCODE_LIMIT_EXCEEDED) {
        return latch_optimizer_statement_failure(
            host, PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL);
    }
    if (status != PRDB_SDK_OPTIMIZER_ENCODE_OK) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    status = prdb_sdk_optimizer_submit_select_v1(
        host, request, optimizer_response_len, optimizer_response,
        optimizer_response_capacity, &optimizer_response_len, &response,
        sizeof(response));
    if (status == PRDB_SDK_OPTIMIZER_RESULT_RESPONSE_TOO_SMALL) {
        final_status = PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL;
    } else if (status != PRDB_SDK_OPTIMIZER_RESULT_OK) {
        final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
    } else if (response.status == PRDB_SDK_OPTIMIZER_STATUS_UNAVAILABLE) {
        final_status = PRDB_SDK_OPTIMIZER_PLAN_UNAVAILABLE;
    } else if (response.status != PRDB_SDK_OPTIMIZER_STATUS_SUCCESS) {
        final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
    }
    if (final_status != PRDB_SDK_OPTIMIZER_PLAN_OK) {
        return latch_optimizer_statement_failure(host, final_status);
    }
    status = prdb_sdk_optimizer_decode_ordered_select_plan_v1(
        response.plan, response.plan_len, &ordered, sizeof(ordered));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) {
        return latch_optimizer_statement_failure(host, status);
    }

    executor_host.struct_size = sizeof(executor_host);
    executor_host.abi_version = PRDB_QUERY_ABI_V2_VERSION;
    executor_host.context = host->v3.context;
    executor_host.executor_request = host->v3.executor_request;
    executor_host.planner_request = host->v3.planner_request;
    executor_host.write_result = host->v3.write_result;
    status = prdb_sdk_optimizer_acquire_plan_relation_v1(
        &executor_host, response.plan, response.plan_len, relation_response,
        relation_response_capacity, &relation_response_len, relation_out,
        relation_out_capacity, relation_columns, relation_column_capacity);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) {
        return latch_optimizer_statement_failure(host, status);
    }
    if (source_cell_capacity < relation_out->column_count ||
        ordered.order_key.ordinal >= relation_out->column_count ||
        projected_value_capacity < ordered.plan.projection_count) {
        return latch_optimizer_statement_failure(
            host, PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    }
    status = prdb_sdk_sql_bind_select_v1(
        statement, relation_out->name, relation_out->name_len,
        relation_columns, relation_out->column_count, &binding,
        sizeof(binding));
    if (status != PRDB_SDK_SQL_BIND_OK ||
        prdb_sdk_optimizer_match_ordered_select_plan_v1(
            statement, &binding, schema, schema_len, response.plan,
            response.plan_len) != PRDB_SDK_OPTIMIZER_PLAN_OK) {
        return latch_optimizer_statement_failure(
            host, PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH);
    }
    if (ordered.plan.has_limit != 0 && ordered.plan.limit == 0) {
        return PRDB_SDK_OPTIMIZER_PLAN_OK;
    }
    status = prdb_sdk_executor_open_full_scan_v1(
        &executor_host, relation_out->executor_handle, scan_response,
        scan_response_capacity, &scan_response_len, cursor);
    if (status == PRDB_SDK_EXECUTOR_RESPONSE_TOO_SMALL) {
        final_status = PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL;
    } else if (status != PRDB_SDK_EXECUTOR_OK) {
        final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
    } else {
        cursor_open = 1;
    }
    while (final_status == PRDB_SDK_OPTIMIZER_PLAN_OK &&
           cursor_open != 0) {
        int32_t cancelled = 0;
        if (page_count >= OPTIMIZER_ORDERED_SELECT_MAX_SCAN_PAGES) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_REQUEST_LIMIT;
            break;
        }
        status = prdb_sdk_executor_check_cancelled_v1(
            &executor_host, &cancelled);
        if (status != PRDB_SDK_EXECUTOR_OK || cancelled != 0) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_CANCELLED;
            break;
        }
        status = prdb_sdk_executor_next_batch_v1(
            &executor_host, cursor, max_candidates, max_batch_bytes,
            batch_response, batch_response_capacity, &batch_response_len,
            &batch, sizeof(batch));
        if (status != PRDB_SDK_EXECUTOR_OK) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
            break;
        }
        ++page_count;
        if (batch.finished != 0) {
            cursor_open = 0;
        } else {
            memcpy(cursor, batch.continuation, sizeof(cursor));
        }
        if (prdb_sdk_batch_rows_begin_v1(&batch, &rows, sizeof(rows)) !=
            PRDB_SDK_BATCH_OK) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
            break;
        }
        while (rows.rows_remaining != 0) {
            prdb_sdk_row_v1 row = {0};
            uint64_t row_start;
            uint64_t row_length;
            if (materialized_count >= PRDB_SDK_SQL_ORDER_MAX_ROWS ||
                prdb_sdk_batch_row_next_v1(&rows, &row, sizeof(row)) !=
                    PRDB_SDK_BATCH_OK ||
                row.cells_offset < 18 || rows.offset < row.cells_offset) {
                final_status = PRDB_SDK_OPTIMIZER_PLAN_REQUEST_LIMIT;
                break;
            }
            row_start = row.cells_offset - 18;
            row_length = rows.offset - row_start;
            if (row_length > materialized_rows_capacity -
                                 materialized_offset) {
                final_status = PRDB_SDK_OPTIMIZER_PLAN_RESPONSE_TOO_SMALL;
                break;
            }
            memcpy(materialized_rows + materialized_offset,
                   batch_response + row_start, (size_t)row_length);
            materialized_offset += row_length;
            ++materialized_count;
        }
    }
    if (cursor_open != 0) {
        status = prdb_sdk_executor_close_cursor_v1(
            &executor_host, cursor, scan_response, scan_response_capacity,
            &scan_response_len);
        if (status != PRDB_SDK_EXECUTOR_OK &&
            final_status == PRDB_SDK_OPTIMIZER_PLAN_OK) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_CALLBACK_ERROR;
        }
    }
    if (final_status == PRDB_SDK_OPTIMIZER_PLAN_OK) {
        int32_t cancelled = 0;
        status = prdb_sdk_executor_check_cancelled_v1(
            &executor_host, &cancelled);
        if (status != PRDB_SDK_EXECUTOR_OK || cancelled != 0) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_CANCELLED;
        }
    }
    if (final_status == PRDB_SDK_OPTIMIZER_PLAN_OK) {
        prdb_sdk_batch_v1 materialized_batch = {0};
        prdb_sdk_row_iterator_v1 materialized_iterator = {0};
        materialized_rows[0] = 0;
        materialized_rows[1] = PRDB_SDK_BATCH_WIRE_V1_VERSION;
        materialized_rows[2] = 1;
        materialized_rows[3] = (uint8_t)(materialized_count >> 8);
        materialized_rows[4] = (uint8_t)materialized_count;
        if (prdb_sdk_batch_open_v1(
                &materialized_batch, sizeof(materialized_batch),
                materialized_rows, materialized_offset) != PRDB_SDK_BATCH_OK ||
            prdb_sdk_batch_rows_begin_v1(
                &materialized_batch, &materialized_iterator,
                sizeof(materialized_iterator)) != PRDB_SDK_BATCH_OK) {
            final_status = PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
        }
        for (uint32_t index = 0;
             final_status == PRDB_SDK_OPTIMIZER_PLAN_OK &&
             index < materialized_count; ++index) {
            prdb_sdk_row_v1 *row = &ordered_rows[index];
            prdb_sdk_cell_iterator_v1 cells = {0};
            if (prdb_sdk_batch_row_next_v1(
                    &materialized_iterator, row, sizeof(*row)) !=
                    PRDB_SDK_BATCH_OK ||
                row->cell_count != relation_out->column_count ||
                prdb_sdk_row_cells_begin_v1(row, &cells, sizeof(cells)) !=
                    PRDB_SDK_BATCH_OK) {
                final_status = PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
                break;
            }
            for (uint32_t ordinal = 0;
                 ordinal < relation_out->column_count; ++ordinal) {
                if (prdb_sdk_cell_next_v1(
                        &cells, &source_cells[ordinal],
                        sizeof(source_cells[ordinal])) != PRDB_SDK_BATCH_OK) {
                    final_status = PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
                    break;
                }
            }
            order_keys[index] =
                source_cells[ordered.order_key.ordinal];
        }
        if (final_status == PRDB_SDK_OPTIMIZER_PLAN_OK) {
            prdb_sdk_sql_order_key_v1 sort_key = {
                order_keys,
                ordered.order_key.type_code,
                ordered.order_key.type_parameters[1],
                (uint8_t)(ordered.descending != 0
                              ? PRDB_SDK_SQL_ORDER_DESCENDING
                              : PRDB_SDK_SQL_ORDER_ASCENDING),
                PRDB_SDK_SQL_ORDER_NULLS_FIRST,
                0,
            };
            if (prdb_sdk_sql_order_row_indices_v1(
                    &sort_key, 1, materialized_count, order_indices,
                    PRDB_SDK_SQL_ORDER_MAX_ROWS) != PRDB_SDK_SQL_ORDER_OK) {
                final_status = PRDB_SDK_OPTIMIZER_PLAN_FILTER_ERROR;
            }
        }
        while (final_status == PRDB_SDK_OPTIMIZER_PLAN_OK &&
               delivered < materialized_count &&
               (ordered.plan.has_limit == 0 ||
                delivered < ordered.plan.limit)) {
            if (delivered % OPTIMIZER_ORDERED_SELECT_CALLBACK_POLL_INTERVAL ==
                0) {
                int32_t cancelled = 0;
                status = prdb_sdk_executor_check_cancelled_v1(
                    &executor_host, &cancelled);
                if (status != PRDB_SDK_EXECUTOR_OK || cancelled != 0) {
                    final_status = PRDB_SDK_OPTIMIZER_PLAN_CANCELLED;
                    break;
                }
            }
            const prdb_sdk_row_v1 *row = &ordered_rows[order_indices[delivered]];
            prdb_sdk_cell_iterator_v1 cells = {0};
            if (prdb_sdk_row_cells_begin_v1(row, &cells, sizeof(cells)) !=
                PRDB_SDK_BATCH_OK) {
                final_status = PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
                break;
            }
            for (uint32_t ordinal = 0;
                 ordinal < relation_out->column_count; ++ordinal) {
                if (prdb_sdk_cell_next_v1(
                        &cells, &source_cells[ordinal],
                        sizeof(source_cells[ordinal])) != PRDB_SDK_BATCH_OK) {
                    final_status = PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
                    break;
                }
            }
            if (final_status != PRDB_SDK_OPTIMIZER_PLAN_OK) break;
            for (uint32_t index = 0;
                 index < ordered.plan.projection_count; ++index) {
                const prdb_sdk_optimizer_plan_column_v1 *projection =
                    &ordered.plan.projections[index];
                if (projection->ordinal >= relation_out->column_count) {
                    final_status =
                        PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
                    break;
                }
                projected_values[index] = source_cells[projection->ordinal];
            }
            if (final_status != PRDB_SDK_OPTIMIZER_PLAN_OK) break;
            if (on_row(row_context, ordered.plan.projections,
                       projected_values, ordered.plan.projection_count) != 0) {
                final_status = PRDB_SDK_OPTIMIZER_PLAN_ROW_CALLBACK_ERROR;
                break;
            }
            ++delivered;
        }
    }
    *delivered_rows_out = delivered;
    return latch_optimizer_statement_failure(host, final_status);
}

int32_t prdb_sdk_optimizer_execute_select_ast_v1(
    const prdb_host_api_v4 *host, const prdb_sdk_select_v1 *statement,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns,
    uint32_t relation_column_capacity, prdb_sdk_value_v1 *source_cells,
    uint32_t source_cell_capacity, prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out) {
    return execute_select_ast_internal(
        host, statement, NULL, 0, 0, schema, schema_len, max_candidates,
        max_candidates, max_batch_bytes, request, request_capacity, optimizer_response,
        optimizer_response_capacity, relation_response,
        relation_response_capacity, scan_response, scan_response_capacity,
        batch_response, batch_response_capacity, relation_out,
        relation_out_capacity, relation_columns, relation_column_capacity,
        source_cells, source_cell_capacity, projected_values,
        projected_value_capacity, on_row, row_context, delivered_rows_out);
}

int32_t prdb_sdk_optimizer_execute_select_ast_with_parameters_v1(
    const prdb_host_api_v4 *host, const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns,
    uint32_t relation_column_capacity, prdb_sdk_value_v1 *source_cells,
    uint32_t source_cell_capacity, prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity,
    prdb_sdk_optimizer_projected_row_v1_fn on_row, void *row_context,
    uint32_t *delivered_rows_out) {
    return execute_select_ast_internal(
        host, statement, parameters, parameter_count, 1, schema, schema_len,
        max_candidates, max_candidates, max_batch_bytes, request, request_capacity,
        optimizer_response, optimizer_response_capacity, relation_response,
        relation_response_capacity, scan_response, scan_response_capacity,
        batch_response, batch_response_capacity, relation_out,
        relation_out_capacity, relation_columns, relation_column_capacity,
        source_cells, source_cell_capacity, projected_values,
        projected_value_capacity, on_row, row_context, delivered_rows_out);
}

typedef struct optimizer_result_context_v1 {
    const prdb_host_api_v3 *host;
    const prdb_sdk_sql_column_shape_v1 *columns;
    const prdb_sdk_executor_relation_v1 *relation;
    uint8_t *record_buffer;
    uint64_t record_capacity;
} optimizer_result_context_v1;

static int32_t emit_optimizer_projected_row_v1(
    void *context, const prdb_sdk_optimizer_plan_column_v1 *projections,
    const prdb_sdk_value_v1 *values, uint32_t projection_count) {
    optimizer_result_context_v1 *result = context;
    uint32_t ordinals[PRDB_SDK_OPTIMIZER_PLAN_MAX_PROJECTIONS];
    uint64_t record_len = 0;
    if (result == NULL || projections == NULL || values == NULL ||
        projection_count == 0 ||
        projection_count > PRDB_SDK_OPTIMIZER_PLAN_MAX_PROJECTIONS) {
        return -1;
    }
    for (uint32_t index = 0; index < projection_count; ++index)
        ordinals[index] = projections[index].ordinal;
    return prdb_sdk_sql_write_projected_result_v1(
               result->host, result->columns, result->relation->column_count,
               ordinals, values, projection_count, result->record_buffer,
               result->record_capacity, &record_len) == PRDB_SDK_SQL_RESULT_OK
               ? 0
               : -1;
}

static int32_t execute_select_ast_to_result_internal(
    const prdb_host_api_v4 *host, const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    int parameterized,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint16_t max_index_candidates,
    uint32_t max_batch_bytes, uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns,
    uint32_t relation_column_capacity, prdb_sdk_value_v1 *source_cells,
    uint32_t source_cell_capacity, prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity, uint8_t *result_record_buffer,
    uint64_t result_record_capacity, uint32_t *delivered_rows_out) {
    optimizer_result_context_v1 result = {0};
    if (host == NULL || host->v3.write_result == NULL || relation_out == NULL ||
        relation_columns == NULL || relation_column_capacity == 0 ||
        result_record_buffer == NULL || result_record_capacity == 0) {
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    }
    result.host = &host->v3;
    result.columns = relation_columns;
    result.relation = relation_out;
    result.record_buffer = result_record_buffer;
    result.record_capacity = result_record_capacity;
    if (parameterized != 0)
        return execute_select_ast_internal(
            host, statement, parameters, parameter_count, 1, schema, schema_len,
            max_candidates, max_index_candidates, max_batch_bytes, request,
            request_capacity, optimizer_response, optimizer_response_capacity,
            relation_response, relation_response_capacity, scan_response,
            scan_response_capacity, batch_response, batch_response_capacity,
            relation_out, relation_out_capacity, relation_columns,
            relation_column_capacity, source_cells, source_cell_capacity,
            projected_values, projected_value_capacity,
            emit_optimizer_projected_row_v1, &result, delivered_rows_out);
    return execute_select_ast_internal(
        host, statement, NULL, 0, 0, schema, schema_len, max_candidates,
        max_index_candidates, max_batch_bytes, request, request_capacity,
        optimizer_response, optimizer_response_capacity, relation_response,
        relation_response_capacity, scan_response, scan_response_capacity,
        batch_response, batch_response_capacity, relation_out,
        relation_out_capacity, relation_columns, relation_column_capacity,
        source_cells, source_cell_capacity, projected_values,
        projected_value_capacity, emit_optimizer_projected_row_v1, &result,
        delivered_rows_out);
}

int32_t prdb_sdk_optimizer_execute_select_ast_to_result_v1(
    const prdb_host_api_v4 *host, const prdb_sdk_select_v1 *statement,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns,
    uint32_t relation_column_capacity, prdb_sdk_value_v1 *source_cells,
    uint32_t source_cell_capacity, prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity, uint8_t *result_record_buffer,
    uint64_t result_record_capacity, uint32_t *delivered_rows_out) {
    return execute_select_ast_to_result_internal(
        host, statement, NULL, 0, 0, schema, schema_len, max_candidates,
        max_candidates,
        max_batch_bytes, request, request_capacity, optimizer_response,
        optimizer_response_capacity, relation_response,
        relation_response_capacity, scan_response, scan_response_capacity,
        batch_response, batch_response_capacity, relation_out,
        relation_out_capacity, relation_columns, relation_column_capacity,
        source_cells, source_cell_capacity, projected_values,
        projected_value_capacity, result_record_buffer, result_record_capacity,
        delivered_rows_out);
}

int32_t prdb_sdk_optimizer_execute_select_ast_with_parameters_to_result_v1(
    const prdb_host_api_v4 *host, const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint32_t max_batch_bytes, uint8_t *request, uint64_t request_capacity,
    uint8_t *optimizer_response, uint64_t optimizer_response_capacity,
    uint8_t *relation_response, uint64_t relation_response_capacity,
    uint8_t *scan_response, uint64_t scan_response_capacity,
    uint8_t *batch_response, uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns,
    uint32_t relation_column_capacity, prdb_sdk_value_v1 *source_cells,
    uint32_t source_cell_capacity, prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity, uint8_t *result_record_buffer,
    uint64_t result_record_capacity, uint32_t *delivered_rows_out) {
    return execute_select_ast_to_result_internal(
        host, statement, parameters, parameter_count, 1, schema, schema_len,
        max_candidates, max_candidates, max_batch_bytes, request, request_capacity,
        optimizer_response, optimizer_response_capacity, relation_response,
        relation_response_capacity, scan_response, scan_response_capacity,
        batch_response, batch_response_capacity, relation_out,
        relation_out_capacity, relation_columns, relation_column_capacity,
        source_cells, source_cell_capacity, projected_values,
        projected_value_capacity, result_record_buffer, result_record_capacity,
        delivered_rows_out);
}

int32_t prdb_sdk_optimizer_execute_select_ast_with_parameters_to_result_v2(
    const prdb_host_api_v4 *host, const prdb_sdk_select_v1 *statement,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len, uint16_t max_candidates,
    uint16_t max_index_candidates, uint32_t max_batch_bytes, uint8_t *request,
    uint64_t request_capacity, uint8_t *optimizer_response,
    uint64_t optimizer_response_capacity, uint8_t *relation_response,
    uint64_t relation_response_capacity, uint8_t *scan_response,
    uint64_t scan_response_capacity, uint8_t *batch_response,
    uint64_t batch_response_capacity,
    prdb_sdk_executor_relation_v1 *relation_out,
    uint32_t relation_out_capacity,
    prdb_sdk_sql_column_shape_v1 *relation_columns,
    uint32_t relation_column_capacity, prdb_sdk_value_v1 *source_cells,
    uint32_t source_cell_capacity, prdb_sdk_value_v1 *projected_values,
    uint32_t projected_value_capacity, uint8_t *result_record_buffer,
    uint64_t result_record_capacity, uint32_t *delivered_rows_out) {
    return execute_select_ast_to_result_internal(
        host, statement, parameters, parameter_count, 1, schema, schema_len,
        max_candidates, max_index_candidates, max_batch_bytes, request,
        request_capacity, optimizer_response, optimizer_response_capacity,
        relation_response, relation_response_capacity, scan_response,
        scan_response_capacity, batch_response, batch_response_capacity,
        relation_out, relation_out_capacity, relation_columns,
        relation_column_capacity, source_cells, source_cell_capacity,
        projected_values, projected_value_capacity, result_record_buffer,
        result_record_capacity, delivered_rows_out);
}
