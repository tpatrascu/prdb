#include "prdb_sdk_sql_group_v1.h"

#include <stddef.h>
#include <string.h>

static void decimal_max_coefficient(uint8_t precision, uint8_t out[16]) {
    memset(out, 0, 16);
    for (uint32_t digit = 0; digit < precision; ++digit) {
        uint32_t carry = 9;
        for (int32_t index = 15; index >= 0; --index) {
            uint32_t value = (uint32_t)out[index] * 10 + carry;
            out[index] = (uint8_t)value;
            carry = value >> 8;
        }
    }
}

static void decimal_magnitude(const uint8_t input[16], uint8_t out[16]) {
    memcpy(out, input, 16);
    if ((input[0] & 0x80) != 0) {
        uint32_t carry = 1;
        for (int32_t index = 15; index >= 0; --index) {
            uint32_t value = (uint32_t)(uint8_t)~out[index] + carry;
            out[index] = (uint8_t)value;
            carry = value >> 8;
        }
    }
}

static int decimal_magnitude_fits(const uint8_t input[16],
                                 const uint8_t limit[16]) {
    uint8_t magnitude[16];
    decimal_magnitude(input, magnitude);
    return memcmp(magnitude, limit, sizeof(magnitude)) <= 0;
}

static void add_sign_extended(uint8_t *total, uint32_t total_width,
                              const uint8_t *value, uint32_t value_width) {
    uint8_t extension = (value[0] & 0x80) != 0 ? 0xFF : 0;
    uint32_t carry = 0;
    for (uint32_t offset = 0; offset < total_width; ++offset) {
        uint32_t total_index = total_width - 1 - offset;
        uint8_t value_byte = offset < value_width
                                 ? value[value_width - 1 - offset]
                                 : extension;
        uint32_t sum = (uint32_t)total[total_index] + value_byte + carry;
        total[total_index] = (uint8_t)sum;
        carry = sum >> 8;
    }
}

static int extended_fits_signed_width(const uint8_t *value,
                                     uint32_t extended_width,
                                     uint32_t result_width) {
    uint8_t extension = (value[extended_width - result_width] & 0x80) != 0
                            ? 0xFF
                            : 0;
    for (uint32_t index = 0; index < extended_width - result_width; ++index) {
        if (value[index] != extension) return 0;
    }
    return ((value[extended_width - result_width] & 0x80) != 0) ==
           (extension != 0);
}

static void write_be_u64(uint64_t value, uint8_t out[8]) {
    for (uint32_t index = 0; index < 8; ++index) {
        out[7 - index] = (uint8_t)(value >> (index * 8));
    }
}

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

int32_t prdb_sdk_sql_minmax_v1(
    const prdb_sdk_value_v1 *values, uint32_t value_count,
    uint32_t declared_type, uint8_t decimal_scale, uint32_t operation,
    prdb_sdk_value_v1 *value_out, uint32_t out_capacity) {
    const prdb_sdk_value_v1 *selected = NULL;
    prdb_sdk_value_v1 result;
    int32_t status;

    if (value_out == NULL || out_capacity < sizeof(*value_out) ||
        declared_type < PRDB_SDK_VALUE_TYPE_BOOLEAN ||
        declared_type > PRDB_SDK_VALUE_TYPE_TIMESTAMPTZ ||
        (declared_type == PRDB_SDK_VALUE_TYPE_DECIMAL
             ? decimal_scale > 38
             : decimal_scale != 0) ||
        (operation != PRDB_SDK_SQL_MINMAX_MIN &&
         operation != PRDB_SDK_SQL_MINMAX_MAX) ||
        (value_count != 0 && values == NULL)) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (value_count > PRDB_SDK_SQL_GROUP_MAX_ROWS) {
        return PRDB_SDK_SQL_GROUP_LIMIT_EXCEEDED;
    }
    for (uint32_t index = 0; index < value_count; ++index) {
        uint32_t truth;
        status = prdb_sdk_value_validate_v1(&values[index]);
        if (status == PRDB_SDK_VALUE_INVALID_ARGUMENT) {
            return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
        }
        if (status != PRDB_SDK_VALUE_VALID) {
            return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
        }
        if (values[index].state == PRDB_SDK_VALUE_STATE_REDACTED) {
            return PRDB_SDK_SQL_EVAL_REDACTED_INPUT;
        }
        if (values[index].state == PRDB_SDK_VALUE_STATE_NULL) {
            continue;
        }
        if (values[index].type_code != declared_type) {
            return PRDB_SDK_SQL_EVAL_TYPE_MISMATCH;
        }
        status = prdb_sdk_sql_compare_v1(
            &values[index], &values[index], PRDB_SDK_SQL_COMPARE_EQUAL,
            decimal_scale, decimal_scale, &truth);
        if (status != PRDB_SDK_SQL_EVAL_OK) {
            return status;
        }
        if (selected == NULL) {
            selected = &values[index];
            continue;
        }
        status = prdb_sdk_sql_compare_v1(
            &values[index], selected,
            operation == PRDB_SDK_SQL_MINMAX_MIN
                ? PRDB_SDK_SQL_COMPARE_LESS
                : PRDB_SDK_SQL_COMPARE_GREATER,
            decimal_scale, decimal_scale, &truth);
        if (status != PRDB_SDK_SQL_EVAL_OK) {
            return status;
        }
        if (truth == PRDB_SDK_SQL_TRUTH_TRUE) {
            selected = &values[index];
        }
    }
    if (selected != NULL) {
        result = *selected;
    } else {
        result = (prdb_sdk_value_v1){
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_NULL, 0, NULL, 0};
    }
    *value_out = result;
    return PRDB_SDK_SQL_EVAL_OK;
}

int32_t prdb_sdk_sql_sum_v1(
    const prdb_sdk_value_v1 *values, uint32_t value_count,
    uint32_t declared_type, uint8_t decimal_precision, uint8_t decimal_scale,
    prdb_sdk_value_v1 *value_out, uint32_t out_capacity,
    uint8_t *value_bytes_out, uint64_t value_bytes_capacity) {
    uint32_t result_type;
    uint32_t result_width;
    uint32_t non_null_count = 0;
    uint8_t integer_total[10] = {0};
    uint8_t decimal_total[17] = {0};
    uint8_t input_decimal_limit[16] = {0};
    uint8_t result_decimal_limit[16] = {0};
    uint8_t encoded[16] = {0};
    prdb_sdk_value_v1 result;

    if (value_out == NULL || out_capacity < sizeof(*value_out) ||
        (value_count != 0 && values == NULL) ||
        (declared_type == PRDB_SDK_VALUE_TYPE_DECIMAL
             ? decimal_precision == 0 || decimal_precision > 38 ||
                   decimal_scale > decimal_precision
             : decimal_precision != 0 || decimal_scale != 0)) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (value_count > PRDB_SDK_SQL_GROUP_MAX_ROWS) {
        return PRDB_SDK_SQL_GROUP_LIMIT_EXCEEDED;
    }
    if (declared_type == PRDB_SDK_VALUE_TYPE_SMALLINT ||
        declared_type == PRDB_SDK_VALUE_TYPE_INTEGER ||
        declared_type == PRDB_SDK_VALUE_TYPE_BIGINT) {
        result_type = PRDB_SDK_VALUE_TYPE_BIGINT;
        result_width = 8;
    } else if (declared_type == PRDB_SDK_VALUE_TYPE_DECIMAL) {
        result_type = PRDB_SDK_VALUE_TYPE_DECIMAL;
        result_width = 16;
        decimal_max_coefficient(decimal_precision, input_decimal_limit);
        decimal_max_coefficient(38, result_decimal_limit);
    } else {
        return PRDB_SDK_SQL_EVAL_TYPE_MISMATCH;
    }

    for (uint32_t index = 0; index < value_count; ++index) {
        const prdb_sdk_value_v1 *value = &values[index];
        int32_t status = prdb_sdk_value_validate_v1(value);
        if (status == PRDB_SDK_VALUE_INVALID_ARGUMENT) {
            return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
        }
        if (status != PRDB_SDK_VALUE_VALID) {
            return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
        }
        if (value->state == PRDB_SDK_VALUE_STATE_REDACTED) {
            return PRDB_SDK_SQL_EVAL_REDACTED_INPUT;
        }
        if (value->state == PRDB_SDK_VALUE_STATE_NULL) continue;
        if (value->type_code != declared_type) {
            return PRDB_SDK_SQL_EVAL_TYPE_MISMATCH;
        }
        ++non_null_count;

        if (declared_type == PRDB_SDK_VALUE_TYPE_SMALLINT) {
            int16_t operand;
            if (prdb_sdk_value_get_smallint_v1(value, &operand) !=
                PRDB_SDK_VALUE_ACCESS_OK) {
                return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
            }
            uint8_t operand_bytes[8];
            write_be_u64((uint64_t)(int64_t)operand, operand_bytes);
            add_sign_extended(integer_total, sizeof(integer_total),
                              operand_bytes, sizeof(operand_bytes));
        } else if (declared_type == PRDB_SDK_VALUE_TYPE_INTEGER) {
            int32_t operand;
            if (prdb_sdk_value_get_integer_v1(value, &operand) !=
                PRDB_SDK_VALUE_ACCESS_OK) {
                return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
            }
            uint8_t operand_bytes[8];
            write_be_u64((uint64_t)(int64_t)operand, operand_bytes);
            add_sign_extended(integer_total, sizeof(integer_total),
                              operand_bytes, sizeof(operand_bytes));
        } else if (declared_type == PRDB_SDK_VALUE_TYPE_BIGINT) {
            int64_t operand;
            if (prdb_sdk_value_get_bigint_v1(value, &operand) !=
                PRDB_SDK_VALUE_ACCESS_OK) {
                return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
            }
            uint8_t operand_bytes[8];
            write_be_u64((uint64_t)operand, operand_bytes);
            add_sign_extended(integer_total, sizeof(integer_total),
                              operand_bytes, sizeof(operand_bytes));
        } else {
            if (!decimal_magnitude_fits(value->data,
                                        input_decimal_limit)) {
                return PRDB_SDK_SQL_EVAL_INVALID_VALUE;
            }
            add_sign_extended(decimal_total, sizeof(decimal_total),
                              value->data, 16);
        }
    }

    if (non_null_count == 0) {
        result = (prdb_sdk_value_v1){
            sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
            PRDB_SDK_VALUE_STATE_NULL, 0, NULL, 0};
        *value_out = result;
        return PRDB_SDK_SQL_EVAL_OK;
    }
    if (value_bytes_out == NULL || value_bytes_capacity < result_width) {
        return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
    }
    if (result_type == PRDB_SDK_VALUE_TYPE_BIGINT) {
        if (!extended_fits_signed_width(integer_total, sizeof(integer_total), 8)) {
            return PRDB_SDK_SQL_EVAL_NUMERIC_OVERFLOW;
        }
        memcpy(encoded, integer_total + sizeof(integer_total) - 8, 8);
    } else {
        if ((decimal_total[0] != 0 || (decimal_total[1] & 0x80) != 0) &&
            (decimal_total[0] != 0xFF || (decimal_total[1] & 0x80) == 0)) {
            return PRDB_SDK_SQL_EVAL_NUMERIC_OVERFLOW;
        }
        if (!decimal_magnitude_fits(decimal_total + 1,
                                    result_decimal_limit)) {
            return PRDB_SDK_SQL_EVAL_NUMERIC_OVERFLOW;
        }
        memcpy(encoded, decimal_total + 1, 16);
    }
    memcpy(value_bytes_out, encoded, result_width);
    result = (prdb_sdk_value_v1){
        sizeof(prdb_sdk_value_v1), PRDB_SDK_VALUE_ABI_V1_VERSION,
        PRDB_SDK_VALUE_STATE_VALUE, result_type, value_bytes_out,
        result_width};
    *value_out = result;
    return PRDB_SDK_SQL_EVAL_OK;
}
