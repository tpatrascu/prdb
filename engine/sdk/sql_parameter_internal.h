#ifndef PRDB_SDK_SQL_PARAMETER_INTERNAL_H
#define PRDB_SDK_SQL_PARAMETER_INTERNAL_H

#include "prdb_sdk_sql_v1.h"

#include <stddef.h>

static inline int32_t bind_sql_parameter(
    uint32_t parameter_index, const prdb_sdk_value_v1 *parameters,
    const prdb_sdk_sql_parameter_metadata_v1 *metadata, uint32_t parameter_count,
    uint32_t expected_type, uint8_t expected_precision, uint8_t expected_scale,
    prdb_sdk_value_v1 *out) {
    prdb_sdk_value_v1 bound;
    int32_t status = prdb_sdk_sql_bind_parameter_v1(
        parameter_index, parameters, parameter_count, expected_type,
        expected_precision, expected_scale, &bound, sizeof(bound));
    if (status != PRDB_SDK_SQL_EVAL_OK) return status;
    if (metadata != NULL) {
        const prdb_sdk_sql_parameter_metadata_v1 *declared =
            &metadata[parameter_index - 1];
        if (declared->struct_size < sizeof(*declared) ||
            declared->abi_version != PRDB_SDK_SQL_PARAMETER_METADATA_ABI_V1_VERSION ||
            declared->reserved[0] != 0 || declared->reserved[1] != 0)
            return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
        if (declared->type_code != expected_type ||
            declared->decimal_scale != expected_scale)
            return PRDB_SDK_SQL_EVAL_TYPE_MISMATCH;
        if (expected_type == PRDB_SDK_VALUE_TYPE_DECIMAL) {
            if (declared->decimal_precision == 0 ||
                declared->decimal_precision > 38 ||
                declared->decimal_scale > declared->decimal_precision)
                return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
            if (declared->decimal_precision < expected_precision)
                return prdb_sdk_sql_bind_parameter_v1(
                    parameter_index, parameters, parameter_count, expected_type,
                    declared->decimal_precision, expected_scale, out, sizeof(*out));
        } else if (declared->decimal_precision != 0) {
            return PRDB_SDK_SQL_EVAL_INVALID_ARGUMENT;
        }
    }
    *out = bound;
    return PRDB_SDK_SQL_EVAL_OK;
}

#endif
