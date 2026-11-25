#include "optimizer_plan_codec_internal.h"

int32_t prdb_sdk_optimizer_match_group_count_plan_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len) {
    prdb_sdk_optimizer_group_count_plan_v1 plan = {0};
    const prdb_sdk_select_v1 *base;
    uint64_t sql_len;
    prdb_sdk_sql_span_v1 relation;
    prdb_sdk_sql_span_v1 key;
    int32_t status;

    if (statement == NULL || binding == NULL || schema == NULL ||
        plan_payload == NULL || schema_len == 0 || schema_len > 63 ||
        statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->sql == NULL ||
        statement->sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        statement->projection_count != 1 ||
        statement->projection_star != 0 ||
        statement->predicate_count != 0 ||
        statement->has_limit > 1 || statement->has_group_by != 1 ||
        statement->aggregate_kind != PRDB_SDK_SQL_AGGREGATE_COUNT_STAR ||
        binding->struct_size != sizeof(*binding) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        binding->struct_size != sizeof((*binding)) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        binding->projection_count != 1 ||
        binding->projection_star != 0 ||
        binding->has_group_by != 1 ||
        binding->aggregate_kind != PRDB_SDK_SQL_AGGREGATE_COUNT_STAR)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;

    status = prdb_sdk_optimizer_decode_group_count_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    base = statement;
    sql_len = base->sql_len;
    relation = base->relation;
    key = statement->group_by_column;
    if (relation.offset > sql_len || relation.length > sql_len - relation.offset ||
        key.offset > sql_len || key.length > sql_len - key.offset ||
        base->projection[0].offset > sql_len ||
        base->projection[0].length > sql_len - base->projection[0].offset ||
        relation.length != plan.plan.relation_name_len ||
        memcmp(base->sql + relation.offset, plan.plan.relation_name,
               relation.length) != 0 ||
        schema_len != plan.plan.schema_name_len ||
        memcmp(schema, plan.plan.schema_name, schema_len) != 0 ||
        base->projection[0].length != key.length ||
        memcmp(base->sql + base->projection[0].offset,
               base->sql + key.offset, key.length) != 0 ||
        plan.group_key.ordinal != binding->group_by_column.ordinal ||
        plan.group_key.type_code != binding->group_by_column.type_code ||
        (plan.group_key.type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
         (plan.group_key.type_parameters[0] !=
              binding->group_by_column.decimal_precision ||
          plan.group_key.type_parameters[1] !=
              binding->group_by_column.decimal_scale)) ||
        binding->projection_ordinals[0] !=
            binding->group_by_column.ordinal ||
        plan.plan.has_limit != base->has_limit ||
        (plan.plan.has_limit && plan.plan.limit != base->limit))
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_match_ordered_group_count_plan_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len) {
    prdb_sdk_optimizer_ordered_group_count_plan_v1 plan = {0};
    const prdb_sdk_select_v1 *statement_v7;
    const prdb_sdk_select_v1 *base;
    const prdb_sdk_select_binding_v1 *binding_v7;
    int32_t status;
    if (statement == NULL || binding == NULL || schema == NULL ||
        plan_payload == NULL || schema_len == 0 || schema_len > 63 ||
        statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->order_count == 0 ||
        statement->order_count > PRDB_SDK_SQL_MAX_ORDER_KEYS ||
        binding->struct_size != sizeof(*binding) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        binding->order_count != statement->order_count)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    statement_v7 = statement;
    base = statement_v7;
    binding_v7 = binding;
    if (statement_v7->struct_size != sizeof(*statement_v7) ||
        statement_v7->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement_v7->has_group_by != 1 ||
        statement_v7->aggregate_kind != PRDB_SDK_SQL_AGGREGATE_COUNT_STAR ||
        statement->has_having != 0 ||
        base->struct_size != sizeof(*base) ||
        base->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        base->sql == NULL || base->projection_count != 1 ||
        base->projection_star != 0 || base->predicate_count != 0 ||
        base->has_limit > 1 || binding_v7->has_group_by != 1 ||
        binding_v7->aggregate_kind != PRDB_SDK_SQL_AGGREGATE_COUNT_STAR ||
        binding_v7->projection_count != 1 ||
        binding_v7->projection_ordinals[0] !=
            binding_v7->group_by_column.ordinal)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    status = prdb_sdk_optimizer_decode_ordered_group_count_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    if (base->relation.offset > base->sql_len ||
        base->relation.length > base->sql_len - base->relation.offset ||
        statement_v7->group_by_column.offset > base->sql_len ||
        statement_v7->group_by_column.length >
            base->sql_len - statement_v7->group_by_column.offset ||
        base->projection[0].offset > base->sql_len ||
        base->projection[0].length > base->sql_len - base->projection[0].offset ||
        base->relation.length != plan.plan.relation_name_len ||
        memcmp(base->sql + base->relation.offset, plan.plan.relation_name,
               base->relation.length) != 0 ||
        schema_len != plan.plan.schema_name_len ||
        memcmp(schema, plan.plan.schema_name, schema_len) != 0 ||
        base->projection[0].length != statement_v7->group_by_column.length ||
        memcmp(base->sql + base->projection[0].offset,
               base->sql + statement_v7->group_by_column.offset,
               base->projection[0].length) != 0 ||
        plan.group_key.ordinal != binding_v7->group_by_column.ordinal ||
        plan.group_key.type_code != binding_v7->group_by_column.type_code ||
        (plan.group_key.type_code == PRDB_SDK_VALUE_TYPE_DECIMAL &&
         (plan.group_key.type_parameters[0] !=
              binding_v7->group_by_column.decimal_precision ||
          plan.group_key.type_parameters[1] !=
              binding_v7->group_by_column.decimal_scale)) ||
        plan.plan.has_limit != base->has_limit ||
        (plan.plan.has_limit && plan.plan.limit != base->limit))
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    if (plan.order_count != statement->order_count)
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    for (uint32_t index = 0; index < statement->order_count; ++index) {
        const prdb_sdk_sql_order_item_v1 *item = &statement->order_items[index];
        const prdb_sdk_sql_bound_order_item_v1 *bound_item =
            &binding->order_items[index];
        uint32_t source;
        uint32_t nulls_first;
        if (item->kind == PRDB_SDK_SQL_ORDER_GROUP_KEY)
            source = 0;
        else if (item->kind == PRDB_SDK_SQL_ORDER_AGGREGATE)
            source = 1;
        else
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        nulls_first = item->null_order != PRDB_SDK_SQL_NULLS_LAST;
        if (plan.order_items[index].source != source ||
            plan.order_items[index].descending !=
                (item->direction == PRDB_SDK_SQL_ORDER_DESCENDING) ||
            plan.order_items[index].nulls_first != nulls_first ||
            bound_item->item.kind != item->kind ||
            bound_item->item.direction != item->direction ||
            bound_item->item.null_order != item->null_order)
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

static int plan_column_matches_bound(
    const prdb_sdk_optimizer_plan_column_v1 *planned,
    const prdb_sdk_sql_bound_column_v1 *bound) {
    return planned->ordinal == bound->ordinal &&
           planned->type_code == bound->type_code &&
           (planned->type_code != PRDB_SDK_VALUE_TYPE_DECIMAL ||
            (planned->type_parameters[0] == bound->decimal_precision &&
             planned->type_parameters[1] == bound->decimal_scale));
}

static uint8_t plan_ascii_fold(uint8_t byte) {
    return byte >= 'A' && byte <= 'Z' ? (uint8_t)(byte + ('a' - 'A')) : byte;
}

static int plan_spans_equal(
    const uint8_t *sql, uint64_t sql_len, prdb_sdk_sql_span_v1 left,
    prdb_sdk_sql_span_v1 right) {
    if (left.length != right.length || left.length == 0 ||
        left.offset > sql_len || left.length > sql_len - left.offset ||
        right.offset > sql_len || right.length > sql_len - right.offset)
        return 0;
    for (uint32_t index = 0; index < left.length; ++index) {
        if (plan_ascii_fold(sql[left.offset + index]) !=
            plan_ascii_fold(sql[right.offset + index]))
            return 0;
    }
    return 1;
}

int32_t prdb_sdk_optimizer_match_ordered_select_plan_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len) {
    prdb_sdk_optimizer_ordered_select_plan_v1 plan = {0};
    const prdb_sdk_select_v1 *base;
    const uint8_t *sql;
    uint64_t sql_len;
    int32_t status;
    if (statement == NULL || binding == NULL || schema == NULL ||
        plan_payload == NULL || schema_len == 0 || schema_len > 63 ||
        statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->sql == NULL ||
        statement->sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        statement->projection_count == 0 ||
        statement->projection_count > PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS ||
        statement->projection_star != 0 ||
        statement->predicate_count != 0 ||
        statement->has_limit > 1 || statement->has_order_by != 1 ||
        statement->order_descending > 1 ||
        binding->struct_size != sizeof(*binding) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        binding->struct_size != sizeof((*binding)) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        binding->projection_count != statement->projection_count ||
        binding->projection_star != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;

    status = prdb_sdk_optimizer_decode_ordered_select_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    base = statement;
    sql = base->sql;
    sql_len = base->sql_len;
    if (base->relation.offset > sql_len ||
        base->relation.length > sql_len - base->relation.offset ||
        plan.plan.relation_name_len != base->relation.length ||
        schema_len != plan.plan.schema_name_len ||
        memcmp(schema, plan.plan.schema_name, schema_len) != 0 ||
        plan.plan.projection_count != base->projection_count ||
        plan.plan.has_limit != base->has_limit ||
        (base->has_limit != 0 && plan.plan.limit != base->limit) ||
        plan.descending != statement->order_descending ||
        binding->has_order_by != 1 ||
        !plan_column_matches_bound(&plan.order_key,
                                   &binding->order_by_column)) {
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    for (uint32_t index = 0; index < base->relation.length; ++index) {
        if (plan_ascii_fold(plan.plan.relation_name[index]) !=
            plan_ascii_fold(sql[base->relation.offset + index])) {
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        }
    }
    for (uint32_t index = 0; index < base->projection_count; ++index) {
        if (binding->projection_ordinals[index] !=
                plan.plan.projections[index].ordinal ||
            base->projection[index].offset > sql_len ||
            base->projection[index].length >
                sql_len - base->projection[index].offset) {
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        }
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_match_composite_group_count_plan_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len) {
    prdb_sdk_optimizer_composite_group_count_plan_v1 plan = {0};
    const prdb_sdk_select_v1 *base;
    const uint8_t *sql;
    uint64_t sql_len;
    int32_t status;

    if (statement == NULL || binding == NULL || schema == NULL ||
        plan_payload == NULL || schema_len == 0 || schema_len > 63 ||
        statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->sql == NULL ||
        statement->sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        statement->group_key_count < 2 || statement->group_key_count > 8 ||
        statement->has_group_by != 1 ||
        statement->aggregate_kind !=
            PRDB_SDK_SQL_AGGREGATE_COUNT_STAR ||
        statement->projection_count != 1 ||
        statement->projection_star != 0 ||
        statement->predicate_count != 0 ||
        statement->has_limit > 1 ||
        binding->struct_size != sizeof(*binding) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        binding->struct_size != sizeof((*binding)) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        binding->struct_size != sizeof((*binding)) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        binding->group_key_count != statement->group_key_count)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;

    status = prdb_sdk_optimizer_decode_composite_group_count_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    base = statement;
    sql = base->sql;
    sql_len = base->sql_len;
    if (base->relation.offset > sql_len ||
        base->relation.length > sql_len - base->relation.offset ||
        base->has_limit != plan.plan.has_limit ||
        (base->has_limit && base->limit != plan.plan.limit) ||
        base->relation.length != plan.plan.relation_name_len ||
        memcmp(sql + base->relation.offset, plan.plan.relation_name,
               base->relation.length) != 0 ||
        schema_len != plan.plan.schema_name_len ||
        memcmp(schema, plan.plan.schema_name, schema_len) != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    for (uint32_t index = 0; index < plan.group_key_count; ++index) {
        if (!plan_spans_equal(sql, sql_len, statement->group_keys[index],
                              statement->projection_keys[index]) ||
            !plan_column_matches_bound(&plan.group_keys[index],
                                       &binding->group_keys[index]))
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        for (uint32_t prior = 0; prior < index; ++prior) {
            if (plan_spans_equal(sql, sql_len, statement->group_keys[index],
                                 statement->group_keys[prior]))
                return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        }
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_match_composite_group_aggregate_plan_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len) {
    prdb_sdk_optimizer_composite_group_aggregate_plan_v1 plan = {0};
    const prdb_sdk_select_v1 *base;
    const uint8_t *sql;
    uint64_t sql_len;
    int32_t status;
    if (statement == NULL || binding == NULL || schema == NULL ||
        plan_payload == NULL || schema_len == 0 || schema_len > 63 ||
        statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->group_key_count < PRDB_SDK_SQL_MIN_GROUP_KEYS ||
        statement->group_key_count > PRDB_SDK_SQL_MAX_GROUP_KEYS ||
        statement->has_group_by != 1 ||
        statement->aggregate_kind < PRDB_SDK_SQL_AGGREGATE_COUNT_STAR ||
        statement->aggregate_kind > PRDB_SDK_SQL_AGGREGATE_COUNT_COLUMN ||
        statement->projection_count != 1 ||
        statement->projection_star != 0 ||
        statement->predicate_count != 0 ||
        statement->has_limit > 1 ||
        binding->struct_size != sizeof(*binding) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        binding->struct_size != sizeof((*binding)) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        binding->struct_size != sizeof((*binding)) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        binding->group_key_count != statement->group_key_count ||
        binding->aggregate_kind != statement->aggregate_kind)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    status = prdb_sdk_optimizer_decode_composite_group_aggregate_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    base = statement;
    sql = base->sql;
    sql_len = base->sql_len;
    if (base->relation.offset > sql_len ||
        base->relation.length > sql_len - base->relation.offset ||
        base->has_limit != plan.plan.has_limit ||
        (base->has_limit && base->limit != plan.plan.limit) ||
        base->relation.length != plan.plan.relation_name_len ||
        memcmp(sql + base->relation.offset, plan.plan.relation_name,
               base->relation.length) != 0 ||
        schema_len != plan.plan.schema_name_len ||
        memcmp(schema, plan.plan.schema_name, schema_len) != 0 ||
        plan.aggregate_kind != statement->aggregate_kind ||
        plan.group_key_count != binding->group_key_count ||
        plan.has_aggregate_column !=
            (statement->aggregate_kind ==
                     PRDB_SDK_SQL_AGGREGATE_COUNT_STAR
                 ? 0
                 : 1))
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    for (uint32_t index = 0; index < plan.group_key_count; ++index) {
        if (!plan_spans_equal(sql, sql_len, statement->group_keys[index],
                              statement->projection_keys[index]) ||
            !plan_column_matches_bound(&plan.group_keys[index],
                                       &binding->group_keys[index]))
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        for (uint32_t prior = 0; prior < index; ++prior) {
            if (plan_spans_equal(sql, sql_len, statement->group_keys[index],
                                 statement->group_keys[prior]))
                return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        }
    }
    if (plan.has_aggregate_column &&
        !plan_column_matches_bound(&plan.aggregate_column,
                                   &binding->aggregate_column))
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

static int append_bytes(uint8_t *output, uint32_t capacity, uint32_t *offset,
                        const uint8_t *bytes, uint32_t length) {
    if (*offset > capacity || length > capacity - *offset) return 0;
    if (length != 0) memcpy(output + *offset, bytes, length);
    *offset += length;
    return 1;
}

static int encode_bound_sql_predicate(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const prdb_sdk_optimizer_plan_column_v1 *scan_columns,
    uint32_t scan_count, uint32_t node_index, uint32_t depth,
    uint8_t *seen, uint8_t *output, uint32_t capacity, uint32_t *offset) {
    const prdb_sdk_predicate_node_v1 *node;
    uint8_t record[PRDB_OPTIMIZER_PLAN_COLUMN_PREDICATE_RECORD_HEADER_BYTES];
    if (depth > PRDB_SDK_SQL_MAX_EXPRESSION_DEPTH ||
        node_index >= statement->predicate_count || seen[node_index]) return 0;
    seen[node_index] = 1;
    node = &statement->predicates[node_index];
    switch (node->node_kind) {
    case PRDB_SDK_SQL_PREDICATE_COMPARE: {
        uint32_t ordinal;
        const prdb_sdk_optimizer_plan_column_v1 *column = NULL;
        const prdb_sdk_value_v1 *value;
        if (node->comparison < PRDB_SDK_SQL_COMPARE_EQUAL ||
            node->comparison > PRDB_SDK_SQL_COMPARE_GREATER_EQUAL ||
            node->parameter_index == 0 || node->parameter_index > parameter_count ||
            parameters == NULL || node_index >= binding->predicate_count)
            return 0;
        ordinal = binding->predicate_columns[node_index].ordinal;
        for (uint32_t index = 0; index < scan_count; ++index)
            if (scan_columns[index].ordinal == ordinal) column = &scan_columns[index];
        if (column == NULL) return 0;
        record[0] = 0x20;
        record[1] = (uint8_t)(node->comparison - PRDB_SDK_SQL_COMPARE_EQUAL);
        if (!append_bytes(output, capacity, offset, record, 2)) return 0;
        prdb_encode_optimizer_plan_column_predicate_record(
            record, 0x10, column->column_id);
        if (!append_bytes(output, capacity, offset, record,
                          PRDB_OPTIMIZER_PLAN_COLUMN_PREDICATE_RECORD_HEADER_BYTES))
            return 0;
        value = &parameters[node->parameter_index - 1];
        if (prdb_sdk_value_validate_v1(value) != PRDB_SDK_VALUE_VALID) return 0;
        if (value->state == PRDB_SDK_VALUE_STATE_NULL) {
            record[0] = 0;
            return append_bytes(output, capacity, offset, record, 1);
        }
        if (value->state != PRDB_SDK_VALUE_STATE_VALUE) return 0;
        if (value->type_code == PRDB_SDK_VALUE_TYPE_BOOLEAN) {
            prdb_encode_optimizer_plan_boolean_literal_predicate_record(
                record, 0x01, value->data[0]);
            return append_bytes(output, capacity, offset, record,
                                PRDB_OPTIMIZER_PLAN_BOOLEAN_LITERAL_PREDICATE_RECORD_HEADER_BYTES);
        }
        if (value->type_code == PRDB_SDK_VALUE_TYPE_INTEGER) {
            uint32_t raw = ((uint32_t)value->data[0] << 24) |
                           ((uint32_t)value->data[1] << 16) |
                           ((uint32_t)value->data[2] << 8) | value->data[3];
            int64_t integer = (int32_t)raw;
            prdb_encode_optimizer_plan_integer_predicate_record(record, 0x02,
                                                                 integer);
            return append_bytes(output, capacity, offset, record,
                                PRDB_OPTIMIZER_PLAN_INTEGER_PREDICATE_RECORD_HEADER_BYTES);
        }
        if (value->type_code == PRDB_SDK_VALUE_TYPE_TEXT &&
            value->data_len <= 4096 && value->data_len <= UINT16_MAX) {
            prdb_encode_optimizer_plan_text_predicate_header_record(
                record, 0x03, (uint16_t)value->data_len);
            return append_bytes(output, capacity, offset, record,
                                PRDB_OPTIMIZER_PLAN_TEXT_PREDICATE_HEADER_RECORD_HEADER_BYTES) &&
                   append_bytes(output, capacity, offset, value->data,
                                (uint32_t)value->data_len);
        }
        return 0;
    }
    case PRDB_SDK_SQL_PREDICATE_AND:
    case PRDB_SDK_SQL_PREDICATE_OR: {
        record[0] = node->node_kind == PRDB_SDK_SQL_PREDICATE_AND ? 0x30 : 0x31;
        record[1] = 2;
        return append_bytes(output, capacity, offset, record, 2) &&
               encode_bound_sql_predicate(statement, binding, parameters,
                   parameter_count, scan_columns, scan_count, node->left_child,
                   depth + 1, seen, output, capacity, offset) &&
               encode_bound_sql_predicate(statement, binding, parameters,
                   parameter_count, scan_columns, scan_count, node->right_child,
                   depth + 1, seen, output, capacity, offset);
    }
    case PRDB_SDK_SQL_PREDICATE_NOT:
        record[0] = 0x32;
        return append_bytes(output, capacity, offset, record, 1) &&
               encode_bound_sql_predicate(statement, binding, parameters,
                   parameter_count, scan_columns, scan_count, node->left_child,
                   depth + 1, seen, output, capacity, offset);
    case PRDB_SDK_SQL_PREDICATE_IS_NULL:
    case PRDB_SDK_SQL_PREDICATE_IS_NOT_NULL: {
        const prdb_sdk_optimizer_plan_column_v1 *column = NULL;
        uint32_t ordinal;
        if (node->left_child != 0 || node->right_child != 0 ||
            node->parameter_index != 0 ||
            node->comparison != 0 ||
            node_index >= binding->predicate_count)
            return 0;
        ordinal = binding->predicate_columns[node_index].ordinal;
        for (uint32_t index = 0; index < scan_count; ++index)
            if (scan_columns[index].ordinal == ordinal)
                column = &scan_columns[index];
        if (column == NULL) return 0;
        record[0] = node->node_kind == PRDB_SDK_SQL_PREDICATE_IS_NULL
                        ? 0x40
                        : 0x41;
        if (!append_bytes(output, capacity, offset, record, 1)) return 0;
        prdb_encode_optimizer_plan_column_predicate_record(
            record, 0x10, column->column_id);
        return append_bytes(output, capacity, offset, record,
                            PRDB_OPTIMIZER_PLAN_COLUMN_PREDICATE_RECORD_HEADER_BYTES);
    }
    default:
        return 0;
    }
}

int32_t prdb_sdk_optimizer_match_filtered_composite_group_aggregate_plan_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len) {
    prdb_sdk_optimizer_filtered_composite_group_aggregate_plan_v1 plan = {0};
    const prdb_sdk_select_v1 *base;
    uint8_t expected_filter[16384];
    uint8_t seen[PRDB_SDK_SQL_MAX_PREDICATE_NODES] = {0};
    uint32_t expected_filter_len = 0;
    int32_t status;
    if (statement == NULL || binding == NULL || schema == NULL || plan_payload == NULL ||
        schema_len == 0 || schema_len > 63 || parameter_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        (parameter_count != 0 && parameters == NULL) ||
        statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->group_key_count < 1 ||
        statement->group_key_count > PRDB_SDK_SQL_MAX_GROUP_KEYS ||
        statement->has_group_by != 1 ||
        statement->aggregate_kind < PRDB_SDK_SQL_AGGREGATE_COUNT_STAR ||
        statement->aggregate_kind > PRDB_SDK_SQL_AGGREGATE_COUNT_COLUMN ||
        statement->projection_count != 1 ||
        statement->projection_star != 0 ||
        statement->predicate_count == 0 ||
        statement->predicate_count > PRDB_SDK_SQL_MAX_PREDICATE_NODES ||
        statement->predicate_root >= statement->predicate_count ||
        statement->has_limit > 1 ||
        binding->struct_size != sizeof(*binding) || binding->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        binding->struct_size != sizeof((*binding)) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        binding->struct_size != sizeof((*binding)) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        binding->group_key_count != statement->group_key_count ||
        binding->aggregate_kind != statement->aggregate_kind ||
        binding->predicate_count != statement->predicate_count)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    status = prdb_sdk_optimizer_decode_filtered_composite_group_aggregate_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    base = statement;
    if (base->sql == NULL || base->relation.offset > base->sql_len ||
        base->relation.length > base->sql_len - base->relation.offset ||
        base->relation.length != plan.plan.relation_name_len ||
        memcmp(base->sql + base->relation.offset, plan.plan.relation_name,
               base->relation.length) != 0 || schema_len != plan.plan.schema_name_len ||
        memcmp(schema, plan.plan.schema_name, schema_len) != 0 ||
        base->has_limit != plan.plan.has_limit ||
        (base->has_limit && base->limit != plan.plan.limit) ||
        plan.aggregate_kind != statement->aggregate_kind ||
        plan.group_key_count != statement->group_key_count ||
        plan.plan.scan_column_count > PRDB_SDK_OPTIMIZER_PLAN_MAX_SCAN_COLUMNS)
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    for (uint32_t index = 0; index < statement->group_key_count; ++index) {
        if (!plan_spans_equal(base->sql, base->sql_len,
                              statement->group_keys[index],
                              statement->projection_keys[index]) ||
            !plan_column_matches_bound(&plan.group_keys[index],
                                       &binding->group_keys[index]))
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        for (uint32_t prior = 0; prior < index; ++prior)
            if (plan_spans_equal(base->sql, base->sql_len,
                                 statement->group_keys[index],
                                 statement->group_keys[prior]))
                return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    if (plan.has_aggregate_column &&
        !plan_column_matches_bound(&plan.aggregate_column,
                                   &binding->aggregate_column))
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    if (!encode_bound_sql_predicate(base, binding, parameters, parameter_count,
                                    plan.plan.scan_columns,
                                    plan.plan.scan_column_count,
                                    base->predicate_root, 1, seen,
                                    expected_filter, sizeof(expected_filter),
                                    &expected_filter_len))
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    for (uint32_t index = 0; index < base->predicate_count; ++index)
        if (!seen[index]) return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    if (expected_filter_len != plan.filter.prefix_len ||
        memcmp(expected_filter, plan.filter.prefix, expected_filter_len) != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_match_ordered_filtered_composite_group_aggregate_plan_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len) {
    prdb_sdk_optimizer_ordered_filtered_composite_group_aggregate_plan_v1 plan = {0};
    const prdb_sdk_select_v1 *base;
    uint8_t expected_filter[16384];
    uint8_t seen[PRDB_SDK_SQL_MAX_PREDICATE_NODES] = {0};
    uint32_t expected_filter_len = 0;
    int32_t status;
    if (statement == NULL || binding == NULL || schema == NULL || plan_payload == NULL ||
        schema_len == 0 || schema_len > 63 || parameter_count > PRDB_SDK_SQL_MAX_PARAMETER_INDEX ||
        (parameter_count != 0 && parameters == NULL) ||
        statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->has_having != 0 || statement->order_count == 0 ||
        statement->order_count > PRDB_SDK_SQL_MAX_ORDER_KEYS ||
        statement->group_key_count < 1 || statement->group_key_count > 8 ||
        statement->aggregate_kind < PRDB_SDK_SQL_AGGREGATE_COUNT_STAR ||
        statement->aggregate_kind > PRDB_SDK_SQL_AGGREGATE_COUNT_COLUMN ||
        statement->projection_count != 1 ||
        statement->projection_star != 0 ||
        statement->predicate_count == 0 ||
        statement->predicate_count > PRDB_SDK_SQL_MAX_PREDICATE_NODES ||
        statement->predicate_root >= statement->predicate_count ||
        statement->has_limit > 1 ||
        binding->struct_size != sizeof(*binding) || binding->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        binding->order_count != statement->order_count || binding->has_having != 0 ||
        binding->group_key_count != statement->group_key_count ||
        binding->aggregate_kind != statement->aggregate_kind ||
        binding->predicate_count != statement->predicate_count)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    status = prdb_sdk_optimizer_decode_ordered_filtered_composite_group_aggregate_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    base = statement;
    if (base->sql == NULL || base->relation.offset > base->sql_len ||
        base->relation.length > base->sql_len - base->relation.offset ||
        base->relation.length != plan.plan.relation_name_len ||
        memcmp(base->sql + base->relation.offset, plan.plan.relation_name, base->relation.length) != 0 ||
        schema_len != plan.plan.schema_name_len || memcmp(schema, plan.plan.schema_name, schema_len) != 0 ||
        base->has_limit != plan.plan.has_limit || (base->has_limit && base->limit != plan.plan.limit) ||
        plan.aggregate_kind != statement->aggregate_kind ||
        plan.group_key_count != statement->group_key_count ||
        plan.order_count != statement->order_count) {
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    for (uint32_t index = 0; index < statement->group_key_count; ++index) {
        if (!plan_spans_equal(base->sql, base->sql_len,
                              statement->group_keys[index],
                              statement->projection_keys[index]) ||
            !plan_column_matches_bound(&plan.group_keys[index],
                                       &binding->group_keys[index]))
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        for (uint32_t prior = 0; prior < index; ++prior)
            if (plan_spans_equal(base->sql, base->sql_len,
                                 statement->group_keys[index],
                                 statement->group_keys[prior]))
                return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    if (plan.has_aggregate_column &&
        !plan_column_matches_bound(&plan.aggregate_column,
                                   &binding->aggregate_column))
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    for (uint32_t index = 0; index < plan.order_count; ++index) {
        const prdb_sdk_sql_order_item_v1 *item = &statement->order_items[index];
        const prdb_sdk_optimizer_composite_order_item_v1 *planned = &plan.order_items[index];
        uint32_t source = item->kind == PRDB_SDK_SQL_ORDER_GROUP_KEY ? 0 : 1;
        uint32_t nulls_first = item->null_order == PRDB_SDK_SQL_NULL_ORDER_DEFAULT ||
                               item->null_order == PRDB_SDK_SQL_NULLS_FIRST;
        if (planned->source != source ||
            planned->descending != (item->direction == PRDB_SDK_SQL_ORDER_DESCENDING) ||
            planned->nulls_first != nulls_first ||
            (source == 0 && planned->group_key_index != item->group_key_index))
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    if (!encode_bound_sql_predicate(base, binding, parameters,
                                    parameter_count, plan.plan.scan_columns,
                                    plan.plan.scan_column_count, base->predicate_root, 1,
                                    seen, expected_filter, sizeof(expected_filter),
                                    &expected_filter_len))
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    for (uint32_t index = 0; index < base->predicate_count; ++index)
        if (!seen[index]) return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    if (expected_filter_len != plan.filter.prefix_len ||
        memcmp(expected_filter, plan.filter.prefix, expected_filter_len) != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_match_ordered_composite_group_count_plan_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len) {
    prdb_sdk_optimizer_ordered_composite_group_count_plan_v1 plan = {0};
    const prdb_sdk_select_v1 *base;
    const uint8_t *sql;
    uint64_t sql_len;
    int32_t status;
    if (statement == NULL || binding == NULL || schema == NULL || plan_payload == NULL || schema_len == 0 || schema_len > 63 ||
        statement->struct_size != sizeof(*statement) || statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) || statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) || statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->group_key_count < 2 || statement->group_key_count > 8 || statement->aggregate_kind != PRDB_SDK_SQL_AGGREGATE_COUNT_STAR ||
        statement->projection_count != 1 || statement->projection_star != 0 ||
        statement->predicate_count != 0 || statement->has_having != 0 || statement->order_count == 0 || statement->order_count > 9 ||
        binding->struct_size != sizeof(*binding) || binding->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION || binding->order_count != statement->order_count ||
        binding->has_having != 0 || binding->group_key_count != statement->group_key_count)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    status = prdb_sdk_optimizer_decode_ordered_composite_group_count_plan_v1(plan_payload, plan_payload_len, &plan, sizeof(plan));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    base = statement;
    sql = base->sql; sql_len = base->sql_len;
    if (base->relation.offset > sql_len || base->relation.length > sql_len - base->relation.offset || base->has_limit != plan.plan.has_limit ||
        (base->has_limit && base->limit != plan.plan.limit) || base->relation.length != plan.plan.relation_name_len ||
        memcmp(sql + base->relation.offset, plan.plan.relation_name, base->relation.length) != 0 || schema_len != plan.plan.schema_name_len || memcmp(schema, plan.plan.schema_name, schema_len) != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    for (uint32_t i = 0; i < plan.group_key_count; ++i) {
        if (!plan_spans_equal(sql, sql_len, statement->group_keys[i], statement->projection_keys[i]) ||
            !plan_column_matches_bound(&plan.group_keys[i], &binding->group_keys[i])) return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        for (uint32_t j = 0; j < i; ++j) if (plan_spans_equal(sql, sql_len, statement->group_keys[i], statement->group_keys[j])) return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    for (uint32_t i = 0; i < plan.order_count; ++i) {
        const prdb_sdk_sql_order_item_v1 *item = &statement->order_items[i];
        const prdb_sdk_optimizer_composite_order_item_v1 *planned = &plan.order_items[i];
        uint32_t source = item->kind == PRDB_SDK_SQL_ORDER_GROUP_KEY ? 0 : 1;
        uint32_t nulls_first = item->null_order == PRDB_SDK_SQL_NULL_ORDER_DEFAULT || item->null_order == PRDB_SDK_SQL_NULLS_FIRST;
        if (planned->source != source || planned->descending != (item->direction == PRDB_SDK_SQL_ORDER_DESCENDING) || planned->nulls_first != nulls_first ||
            (source == 0 && planned->group_key_index != item->group_key_index)) return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_match_ordered_composite_group_aggregate_plan_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len) {
    prdb_sdk_optimizer_ordered_composite_group_aggregate_plan_v1 plan = {0};
    const prdb_sdk_select_v1 *base;
    const uint8_t *sql;
    uint64_t sql_len;
    int32_t status;
    if (statement == NULL || binding == NULL || schema == NULL ||
        plan_payload == NULL || schema_len == 0 || schema_len > 63 ||
        statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->group_key_count < 2 ||
        statement->group_key_count > 8 ||
        statement->aggregate_kind <
            PRDB_SDK_SQL_AGGREGATE_COUNT_STAR ||
        statement->aggregate_kind >
            PRDB_SDK_SQL_AGGREGATE_COUNT_COLUMN ||
        statement->projection_count != 1 ||
        statement->projection_star != 0 ||
        statement->predicate_count != 0 ||
        statement->has_having != 0 || statement->order_count == 0 ||
        statement->order_count > PRDB_SDK_SQL_MAX_ORDER_KEYS ||
        binding->struct_size != sizeof(*binding) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        binding->order_count != statement->order_count ||
        binding->has_having != 0 ||
        binding->group_key_count !=
            statement->group_key_count ||
        binding->aggregate_kind !=
            statement->aggregate_kind)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    status = prdb_sdk_optimizer_decode_ordered_composite_group_aggregate_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    base = statement;
    sql = base->sql;
    sql_len = base->sql_len;
    if (base->relation.offset > sql_len ||
        base->relation.length > sql_len - base->relation.offset ||
        base->has_limit != plan.plan.has_limit ||
        (base->has_limit && base->limit != plan.plan.limit) ||
        base->relation.length != plan.plan.relation_name_len ||
        memcmp(sql + base->relation.offset, plan.plan.relation_name,
               base->relation.length) != 0 ||
        schema_len != plan.plan.schema_name_len ||
        memcmp(schema, plan.plan.schema_name, schema_len) != 0 ||
        plan.aggregate_kind != statement->aggregate_kind ||
        plan.group_key_count != binding->group_key_count ||
        plan.has_aggregate_column !=
            (statement->aggregate_kind ==
                     PRDB_SDK_SQL_AGGREGATE_COUNT_STAR
                 ? 0
                 : 1))
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    for (uint32_t index = 0; index < plan.group_key_count; ++index) {
        if (!plan_spans_equal(sql, sql_len,
                              statement->group_keys[index],
                              statement->projection_keys[index]) ||
            !plan_column_matches_bound(
                &plan.group_keys[index],
                &binding->group_keys[index]))
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        for (uint32_t prior = 0; prior < index; ++prior)
            if (plan_spans_equal(sql, sql_len,
                                 statement->group_keys[index],
                                 statement->group_keys[prior]))
                return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    if (plan.has_aggregate_column &&
        !plan_column_matches_bound(
            &plan.aggregate_column,
            &binding->aggregate_column))
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    if (plan.order_count != statement->order_count)
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    for (uint32_t index = 0; index < plan.order_count; ++index) {
        const prdb_sdk_sql_order_item_v1 *item = &statement->order_items[index];
        const prdb_sdk_optimizer_composite_order_item_v1 *planned =
            &plan.order_items[index];
        uint32_t source =
            item->kind == PRDB_SDK_SQL_ORDER_GROUP_KEY ? 0 : 1;
        uint32_t nulls_first =
            item->null_order == PRDB_SDK_SQL_NULL_ORDER_DEFAULT ||
            item->null_order == PRDB_SDK_SQL_NULLS_FIRST;
        if (planned->source != source ||
            planned->descending !=
                (item->direction == PRDB_SDK_SQL_ORDER_DESCENDING) ||
            planned->nulls_first != nulls_first ||
            (source == 0 &&
             planned->group_key_index != item->group_key_index))
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_match_group_aggregate_plan_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len) {
    prdb_sdk_optimizer_group_aggregate_plan_v1 plan = {0};
    const prdb_sdk_select_v1 *base;
    const uint8_t *sql;
    uint64_t sql_len;
    int32_t status;
    if (statement == NULL || binding == NULL || schema == NULL || plan_payload == NULL || schema_len == 0 || schema_len > 63 ||
        statement->struct_size != sizeof(*statement) || statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->has_group_by != 1 || statement->aggregate_kind < PRDB_SDK_SQL_AGGREGATE_COUNT_STAR || statement->aggregate_kind > PRDB_SDK_SQL_AGGREGATE_COUNT_COLUMN ||
        statement->projection_count != 1 || statement->projection_star != 0 || statement->predicate_count != 0 || statement->has_limit > 1 ||
        binding->struct_size != sizeof(*binding) || binding->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION || binding->has_group_by != 1 || binding->aggregate_kind != statement->aggregate_kind)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    status = prdb_sdk_optimizer_decode_group_aggregate_plan_v1(plan_payload, plan_payload_len, &plan, sizeof(plan));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    base = statement; sql = base->sql; sql_len = base->sql_len;
    if (base->relation.offset > sql_len || base->relation.length > sql_len - base->relation.offset ||
        base->has_limit != plan.plan.has_limit || (base->has_limit && base->limit != plan.plan.limit) ||
        base->relation.length != plan.plan.relation_name_len || memcmp(sql + base->relation.offset, plan.plan.relation_name, base->relation.length) != 0 ||
        schema_len != plan.plan.schema_name_len || memcmp(schema, plan.plan.schema_name, schema_len) != 0 ||
        !plan_spans_equal(sql, sql_len, statement->group_by_column, base->projection[0]) ||
        plan.aggregate_kind != statement->aggregate_kind || !plan_column_matches_bound(&plan.group_key, &binding->group_by_column))
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    if (statement->aggregate_kind == PRDB_SDK_SQL_AGGREGATE_COUNT_STAR) {
        if (plan.has_aggregate_column != 0 || statement->aggregate_column.length != 0) return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    } else if (plan.has_aggregate_column == 0 || statement->aggregate_column.length == 0 ||
               statement->aggregate_column.offset > sql_len || statement->aggregate_column.length > sql_len - statement->aggregate_column.offset ||
               !plan_column_matches_bound(&plan.aggregate_column, &binding->aggregate_column)) {
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_match_ordered_group_aggregate_plan_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len) {
    prdb_sdk_optimizer_ordered_group_aggregate_plan_v1 plan = {0};
    const prdb_sdk_select_v1 *statement_v7;
    const prdb_sdk_select_binding_v1 *binding_v7;
    const prdb_sdk_select_v1 *base;
    const uint8_t *sql;
    uint64_t sql_len;
    int32_t status;
    if (statement == NULL || binding == NULL || schema == NULL ||
        plan_payload == NULL || schema_len == 0 || schema_len > 63 ||
        statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->order_count == 0 ||
        statement->order_count > PRDB_SDK_SQL_MAX_ORDER_KEYS ||
        statement->struct_size != sizeof((*statement)) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size !=
            sizeof((*statement)) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        binding->struct_size != sizeof(*binding) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        binding->order_count != statement->order_count ||
        binding->struct_size != sizeof((*binding)) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        binding->struct_size != sizeof((*binding)) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        binding->struct_size !=
            sizeof((*binding)) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    statement_v7 = statement;
    binding_v7 = binding;
    base = statement_v7;
    if (statement_v7->struct_size != sizeof(*statement_v7) ||
        statement_v7->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement_v7->has_group_by != 1 ||
        statement_v7->aggregate_kind < PRDB_SDK_SQL_AGGREGATE_COUNT_STAR ||
        statement_v7->aggregate_kind > PRDB_SDK_SQL_AGGREGATE_COUNT_COLUMN ||
        statement->has_having != 0 ||
        statement->has_order_by != 1 ||
        base->struct_size != sizeof(*base) ||
        base->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION || base->sql == NULL ||
        base->projection_count != 1 || base->projection_star != 0 ||
        base->predicate_count != 0 || base->has_limit > 1 ||
        binding_v7->struct_size != sizeof(*binding_v7) ||
        binding_v7->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        binding_v7->has_group_by != 1 ||
        binding_v7->aggregate_kind != statement_v7->aggregate_kind ||
        binding_v7->projection_count != 1 ||
        binding_v7->projection_ordinals[0] !=
            binding_v7->group_by_column.ordinal)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    status = prdb_sdk_optimizer_decode_ordered_group_aggregate_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    sql = base->sql;
    sql_len = base->sql_len;
    if (base->relation.offset > sql_len ||
        base->relation.length > sql_len - base->relation.offset ||
        base->projection[0].offset > sql_len ||
        base->projection[0].length > sql_len - base->projection[0].offset ||
        statement_v7->group_by_column.offset > sql_len ||
        statement_v7->group_by_column.length >
            sql_len - statement_v7->group_by_column.offset ||
        base->relation.length != plan.plan.relation_name_len ||
        memcmp(sql + base->relation.offset, plan.plan.relation_name,
               base->relation.length) != 0 ||
        schema_len != plan.plan.schema_name_len ||
        memcmp(schema, plan.plan.schema_name, schema_len) != 0 ||
        !plan_spans_equal(sql, sql_len, base->projection[0],
                          statement_v7->group_by_column) ||
        plan.aggregate_kind != statement_v7->aggregate_kind ||
        !plan_column_matches_bound(&plan.group_key,
                                   &binding_v7->group_by_column) ||
        plan.plan.has_limit != base->has_limit ||
        (plan.plan.has_limit && plan.plan.limit != base->limit))
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    if (statement_v7->aggregate_kind ==
        PRDB_SDK_SQL_AGGREGATE_COUNT_STAR) {
        if (plan.has_aggregate_column != 0 ||
            statement_v7->aggregate_column.length != 0)
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    } else if (plan.has_aggregate_column == 0 ||
               statement_v7->aggregate_column.length == 0 ||
               statement_v7->aggregate_column.offset > sql_len ||
               statement_v7->aggregate_column.length >
                   sql_len - statement_v7->aggregate_column.offset ||
               !plan_column_matches_bound(&plan.aggregate_column,
                                          &binding_v7->aggregate_column)) {
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    if (plan.order_count != statement->order_count)
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    for (uint32_t index = 0; index < statement->order_count; ++index) {
        const prdb_sdk_sql_order_item_v1 *item =
            &statement->order_items[index];
        const prdb_sdk_sql_bound_order_item_v1 *bound_item =
            &binding->order_items[index];
        uint32_t source;
        uint32_t nulls_first;
        if (item->kind == PRDB_SDK_SQL_ORDER_GROUP_KEY)
            source = 0;
        else if (item->kind == PRDB_SDK_SQL_ORDER_AGGREGATE)
            source = 1;
        else
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        nulls_first = item->null_order != PRDB_SDK_SQL_NULLS_LAST;
        if (plan.order_items[index].source != source ||
            plan.order_items[index].descending !=
                (item->direction == PRDB_SDK_SQL_ORDER_DESCENDING) ||
            plan.order_items[index].nulls_first != nulls_first ||
            bound_item->item.kind != item->kind ||
            bound_item->item.direction != item->direction ||
            bound_item->item.null_order != item->null_order)
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

static int32_t match_join_plan_decoded_v19_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const prdb_sdk_optimizer_join_plan_v1 *plan) {
    const uint8_t *sql;
    uint64_t sql_len;
    prdb_sdk_sql_span_v1 left_name;
    prdb_sdk_sql_span_v1 right_name;
    if (statement == NULL || binding == NULL || schema == NULL ||
        plan == NULL || schema_len == 0 || schema_len > 63 ||
        statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->struct_size != sizeof((*statement)) ||
        statement->sql == NULL ||
        statement->sql_len > PRDB_SDK_SQL_MAX_QUERY_BYTES ||
        statement->join_projection_count == 0 ||
        statement->join_projection_count > PRDB_SDK_SQL_MAX_PROJECTION_COLUMNS ||
        statement->join_key_count < 2 ||
        statement->join_key_count > PRDB_SDK_SQL_MAX_JOIN_KEYS ||
        statement->has_limit > 1 ||
        binding->struct_size != sizeof(*binding) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        binding->struct_size != sizeof((*binding)) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    sql = statement->sql;
    sql_len = statement->sql_len;
    left_name = statement->left_relation;
    right_name = statement->right_relation;
    if (sql == NULL || left_name.offset > sql_len ||
        left_name.length > sql_len - left_name.offset ||
        right_name.offset > sql_len ||
        right_name.length > sql_len - right_name.offset ||
        left_name.length != plan->left_relation.relation_name_len ||
        right_name.length != plan->right_relation.relation_name_len ||
        memcmp(sql + left_name.offset, plan->left_relation.relation_name,
               left_name.length) != 0 ||
        memcmp(sql + right_name.offset, plan->right_relation.relation_name,
               right_name.length) != 0 ||
        schema_len != plan->schema_name_len ||
        memcmp(schema, plan->schema_name, schema_len) != 0 ||
        statement->join_projection_count != plan->projection_count ||
        binding->join_projection_count != plan->projection_count ||
        statement->join_key_count != plan->join_key_count ||
        binding->join_key_count != plan->join_key_count ||
        statement->has_limit != plan->has_limit ||
        (plan->has_limit && statement->limit != plan->limit))
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    for (uint32_t index = 0; index < plan->projection_count; ++index) {
        const prdb_sdk_sql_join_projection_binding_v1 *bound =
            &binding->join_projections[index];
        if (plan->projections[index].relation_side != bound->relation_side ||
            !plan_column_matches_bound(&plan->projections[index].column,
                                       &bound->column))
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    for (uint32_t index = 0; index < plan->join_key_count; ++index) {
        const prdb_sdk_sql_join_key_binding_v1 *bound =
            &binding->join_keys[index];
        if (!plan_column_matches_bound(&plan->join_keys[index].left_column,
                                       &bound->left) ||
            !plan_column_matches_bound(&plan->join_keys[index].right_column,
                                       &bound->right))
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}

int32_t prdb_sdk_optimizer_match_join_plan_v19_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len) {
    prdb_sdk_optimizer_join_plan_v1 plan = {0};
    prdb_sdk_optimizer_index_join_plan_v1 index_plan = {0};
    prdb_sdk_optimizer_index_prefix_join_plan_v1 prefix_plan = {0};
    uint16_t version;
    uint8_t kind;
    uint8_t reserved;
    int32_t status;
    if (plan_payload == NULL)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_ARGUMENT;
    if (!prdb_decode_optimizer_plan_header_record(
            plan_payload, PRDB_OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES,
            &version, &kind, &reserved) || version != 1 || reserved != 0)
        return PRDB_SDK_OPTIMIZER_PLAN_INVALID_PAYLOAD;
    if (kind == PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_NESTED_LOOP_JOIN) {
        status = prdb_sdk_optimizer_decode_index_join_plan_v1(
            plan_payload, plan_payload_len, &index_plan, sizeof(index_plan));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        if (index_plan.has_filter != 0)
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        status = match_join_plan_decoded_v19_v1(
            statement, binding, schema, schema_len, &index_plan.join);
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        for (uint32_t index = 0; index < binding->join_key_count; ++index) {
            const prdb_sdk_sql_join_key_binding_v1 *key =
                &binding->join_keys[index];
            const prdb_sdk_sql_bound_column_v1 *inner =
                index_plan.inner_side == 0 ? &key->left : &key->right;
            if (plan_column_matches_bound(&index_plan.key_column, inner))
                return PRDB_SDK_OPTIMIZER_PLAN_OK;
        }
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    if (kind == PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_PREFIX_NESTED_LOOP_JOIN) {
        status = prdb_sdk_optimizer_decode_index_prefix_join_plan_v1(
            plan_payload, plan_payload_len, &prefix_plan, sizeof(prefix_plan));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        if (prefix_plan.has_filter != 0)
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        status = match_join_plan_decoded_v19_v1(
            statement, binding, schema, schema_len, &prefix_plan.join);
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        for (uint32_t prefix = 0; prefix < prefix_plan.prefix_column_count; ++prefix) {
            int found = 0;
            for (uint32_t index = 0; index < binding->join_key_count; ++index) {
                const prdb_sdk_sql_join_key_binding_v1 *key =
                    &binding->join_keys[index];
                const prdb_sdk_sql_bound_column_v1 *inner =
                    prefix_plan.inner_side == 0 ? &key->left : &key->right;
                if (plan_column_matches_bound(&prefix_plan.prefix_columns[prefix], inner))
                    found = 1;
            }
            if (!found) return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        }
        return PRDB_SDK_OPTIMIZER_PLAN_OK;
    }
    status = prdb_sdk_optimizer_decode_join_plan_v1(
        plan_payload, plan_payload_len, &plan, sizeof(plan));
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    return match_join_plan_decoded_v19_v1(statement, binding, schema,
                                          schema_len, &plan);
}

int32_t prdb_sdk_optimizer_match_filtered_join_plan_v20_v1(
    const prdb_sdk_select_v1 *statement,
    const prdb_sdk_select_binding_v1 *binding,
    const prdb_sdk_value_v1 *parameters, uint32_t parameter_count,
    const uint8_t *schema, uint32_t schema_len,
    const uint8_t *plan_payload, uint64_t plan_payload_len) {
    prdb_sdk_optimizer_filtered_join_plan_v1 plan = {0};
    prdb_sdk_optimizer_index_join_plan_v1 index_plan = {0};
    prdb_sdk_optimizer_index_prefix_join_plan_v1 prefix_plan = {0};
    const prdb_sdk_optimizer_join_plan_v1 *join_plan;
    const prdb_sdk_optimizer_join_filter_v1 *filter_plan;
    prdb_sdk_select_binding_v1 base_binding;
    prdb_sdk_value_v1 bound_parameter = {0};
    uint32_t expression_kind;
    uint16_t version;
    uint8_t kind;
    uint8_t reserved;
    int32_t status;
    if (statement == NULL || binding == NULL || plan_payload == NULL ||
        statement->struct_size != sizeof(*statement) ||
        statement->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        statement->has_join_where != 1 ||
        binding->struct_size != sizeof(*binding) ||
        binding->abi_version != PRDB_SDK_SQL_ABI_V1_VERSION ||
        binding->output_projection_count !=
            statement->join_projection_count ||
        binding->join_projection_count !=
            binding->output_projection_count + 1 ||
        binding->where_projection_index != binding->output_projection_count ||
        binding->where_relation_side > 1)
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
        filter_plan = &index_plan.filter;
    } else if (kind == PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_PREFIX_NESTED_LOOP_JOIN) {
        status = prdb_sdk_optimizer_decode_index_prefix_join_plan_v1(
            plan_payload, plan_payload_len, &prefix_plan, sizeof(prefix_plan));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        if (prefix_plan.has_filter != 1)
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        join_plan = &prefix_plan.join;
        filter_plan = &prefix_plan.filter;
    } else {
        status = prdb_sdk_optimizer_decode_filtered_join_plan_v1(
            plan_payload, plan_payload_len, &plan, sizeof(plan));
        if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
        join_plan = &plan.join;
        filter_plan = &plan.filter;
    }
    base_binding = (*binding);
    base_binding.join_projection_count = binding->output_projection_count;
    status = match_join_plan_decoded_v19_v1(
        statement, &base_binding, schema, schema_len, join_plan);
    if (status != PRDB_SDK_OPTIMIZER_PLAN_OK) return status;
    if (statement->join_where_comparison >= PRDB_SDK_SQL_COMPARE_EQUAL &&
        statement->join_where_comparison <= PRDB_SDK_SQL_COMPARE_GREATER_EQUAL) {
        expression_kind = 0;
        if (statement->join_where_parameter_index == 0 ||
            statement->join_where_parameter_index > parameter_count ||
            parameters == NULL || filter_plan->expression_kind != 0 ||
            filter_plan->comparison != statement->join_where_comparison -
                                         PRDB_SDK_SQL_COMPARE_EQUAL ||
            filter_plan->parameter_index != statement->join_where_parameter_index ||
            prdb_sdk_sql_bind_parameter_v1(
                statement->join_where_parameter_index, parameters, parameter_count,
                binding->where_column.type_code,
                binding->where_column.decimal_precision,
                binding->where_column.decimal_scale, &bound_parameter,
                sizeof(bound_parameter)) != PRDB_SDK_SQL_EVAL_OK)
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    } else if (statement->join_where_comparison == PRDB_SDK_SQL_COMPARE_IS_NULL) {
        expression_kind = 1;
    } else if (statement->join_where_comparison == PRDB_SDK_SQL_COMPARE_IS_NOT_NULL) {
        expression_kind = 2;
    } else {
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    }
    if (filter_plan->relation_side != binding->where_relation_side ||
        !plan_column_matches_bound(&filter_plan->column,
                                   &binding->where_column) ||
        filter_plan->expression_kind != expression_kind ||
        (expression_kind != 0 &&
         (statement->join_where_parameter_index != 0 ||
          filter_plan->parameter_index != 0 || filter_plan->comparison != 0)))
        return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    if (kind == PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_NESTED_LOOP_JOIN) {
        int key_found = 0;
        for (uint32_t index = 0; index < binding->join_key_count;
             ++index) {
            const prdb_sdk_sql_join_key_binding_v1 *key =
                &binding->join_keys[index];
            const prdb_sdk_sql_bound_column_v1 *inner =
                index_plan.inner_side == 0 ? &key->left : &key->right;
            if (plan_column_matches_bound(&index_plan.key_column, inner))
                key_found = 1;
        }
        if (!key_found)
            return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
    } else if (kind == PRDB_SDK_OPTIMIZER_PLAN_KIND_INDEX_PREFIX_NESTED_LOOP_JOIN) {
        for (uint32_t prefix = 0; prefix < prefix_plan.prefix_column_count; ++prefix) {
            int found = 0;
            for (uint32_t index = 0; index < binding->join_key_count; ++index) {
                const prdb_sdk_sql_join_key_binding_v1 *key =
                    &binding->join_keys[index];
                const prdb_sdk_sql_bound_column_v1 *inner =
                    prefix_plan.inner_side == 0 ? &key->left : &key->right;
                if (plan_column_matches_bound(&prefix_plan.prefix_columns[prefix], inner))
                    found = 1;
            }
            if (!found) return PRDB_SDK_OPTIMIZER_PLAN_IDENTITY_MISMATCH;
        }
    }
    return PRDB_SDK_OPTIMIZER_PLAN_OK;
}
