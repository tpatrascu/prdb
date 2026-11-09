#include "prdb_sdk_sql_group_v1.h"

#include <stddef.h>
#include <string.h>

static int32_t validate_group_key(const prdb_sdk_value_v1 *key,
                                  uint32_t declared_type,
                                  uint8_t decimal_scale) {
    int32_t status = prdb_sdk_value_validate_v1(key);
    if (status == PRDB_SDK_VALUE_INVALID_ARGUMENT) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (status != PRDB_SDK_VALUE_VALID) {
        return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
    }
    if (key->state == PRDB_SDK_VALUE_STATE_REDACTED) {
        return PRDB_SDK_SQL_EVAL_REDACTED_INPUT;
    }
    if (key->state == PRDB_SDK_VALUE_STATE_NULL) {
        return PRDB_SDK_SQL_EVAL_OK;
    }
    if (key->type_code != declared_type) {
        return PRDB_SDK_SQL_EVAL_TYPE_MISMATCH;
    }
    {
        uint32_t truth;
        status = prdb_sdk_sql_compare_v1(
            key, key, PRDB_SDK_SQL_COMPARE_EQUAL, decimal_scale,
            decimal_scale, &truth);
    }
    return status;
}

int32_t prdb_sdk_sql_group_count_v1(
    const prdb_sdk_value_v1 *keys, uint32_t key_count,
    uint32_t declared_type, uint8_t decimal_scale,
    prdb_sdk_sql_count_group_v1 *groups_out, uint32_t groups_capacity,
    uint32_t *group_count_out) {
    prdb_sdk_sql_count_group_v1 groups[PRDB_SDK_SQL_GROUP_MAX_ROWS];
    uint32_t group_count = 0;
    int32_t status;

    if (group_count_out == NULL ||
        declared_type < PRDB_SDK_VALUE_TYPE_BOOLEAN ||
        declared_type > PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ ||
        (declared_type == PRDB_SDK_VALUE_TYPE_DECIMAL
             ? decimal_scale > 38
             : decimal_scale != 0) ||
        (key_count != 0 && (keys == NULL || groups_out == NULL))) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (key_count > PRDB_SDK_SQL_GROUP_MAX_ROWS) {
        return PRDB_SDK_SQL_GROUP_LIMIT_EXCEEDED;
    }
    for (uint32_t index = 0; index < key_count; ++index) {
        status = validate_group_key(&keys[index], declared_type,
                                   decimal_scale);
        if (status != PRDB_SDK_SQL_EVAL_OK) {
            return status;
        }
    }

    for (uint32_t row_index = 0; row_index < key_count; ++row_index) {
        uint32_t matching_group = group_count;
        for (uint32_t group_index = 0; group_index < group_count;
             ++group_index) {
            const prdb_sdk_value_v1 *representative =
                &keys[groups[group_index].representative_row];
            if (keys[row_index].state == PRDB_SDK_VALUE_STATE_NULL ||
                representative->state == PRDB_SDK_VALUE_STATE_NULL) {
                if (keys[row_index].state == PRDB_SDK_VALUE_STATE_NULL &&
                    representative->state == PRDB_SDK_VALUE_STATE_NULL) {
                    matching_group = group_index;
                    break;
                }
                continue;
            }
            {
                uint32_t truth;
                status = prdb_sdk_sql_compare_v1(
                    &keys[row_index], representative,
                    PRDB_SDK_SQL_COMPARE_EQUAL, decimal_scale,
                    decimal_scale, &truth);
                if (status != PRDB_SDK_SQL_EVAL_OK) {
                    return status;
                }
                if (truth == PRDB_SDK_SQL_TRUTH_TRUE) {
                    matching_group = group_index;
                    break;
                }
            }
        }
        if (matching_group == group_count) {
            groups[group_count].representative_row = row_index;
            groups[group_count].count = 1;
            ++group_count;
        } else {
            ++groups[matching_group].count;
        }
    }
    if (groups_capacity < group_count) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (group_count != 0) {
        memcpy(groups_out, groups,
               (size_t)group_count * sizeof(*groups_out));
    }
    *group_count_out = group_count;
    return PRDB_SDK_SQL_GROUP_OK;
}
