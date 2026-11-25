use super::response::*;
use super::*;
use crate::credentials::TOKEN_SECRET_BYTES;
use std::collections::{HashMap, HashSet};
use zeroize::Zeroizing;

pub(super) fn validate_index_request(
    object_id: [u8; 16],
    column_ids: &[[u8; 16]],
) -> Result<(), ClientSubmitError> {
    if object_id == [0; 16]
        || column_ids.is_empty()
        || column_ids.len() > 256
        || column_ids.contains(&[0; 16])
        || column_ids.iter().copied().collect::<HashSet<_>>().len() != column_ids.len()
    {
        return Err(ClientSubmitError::InvalidAdministrativeRequest);
    }
    Ok(())
}

pub(super) fn parse_canonical_positive_u64(text: String) -> Option<u64> {
    let value = text.parse::<u64>().ok()?;
    (value > 0 && value.to_string() == text).then_some(value)
}

pub(super) fn encode_id_array(ids: &[[u8; 16]]) -> String {
    format!(
        "[{}]",
        ids.iter()
            .map(|id| format!("\"{}\"", lower_hex(id)))
            .collect::<Vec<_>>()
            .join(",")
    )
}

pub(super) fn is_normalized_index_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 128
        && (bytes[0].is_ascii_lowercase() || bytes[0] == b'_')
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
}

pub(super) fn validate_admin_name(name: &str) -> Result<(), ClientSubmitError> {
    if is_normalized_index_name(name) {
        Ok(())
    } else {
        Err(ClientSubmitError::InvalidAdministrativeRequest)
    }
}

pub(super) fn validate_admin_id(id: [u8; 16]) -> Result<(), ClientSubmitError> {
    if id == [0; 16] {
        Err(ClientSubmitError::InvalidAdministrativeRequest)
    } else {
        Ok(())
    }
}

pub(super) fn validate_admin_id_list(
    ids: &[[u8; 16]],
    maximum: usize,
) -> Result<(), ClientSubmitError> {
    if ids.is_empty() {
        return Err(ClientSubmitError::InvalidAdministrativeRequest);
    }
    validate_admin_id_list_allow_empty(ids, maximum)
}

pub(super) fn validate_admin_id_list_allow_empty(
    ids: &[[u8; 16]],
    maximum: usize,
) -> Result<(), ClientSubmitError> {
    if ids.len() > maximum
        || ids.contains(&[0; 16])
        || ids.iter().copied().collect::<HashSet<_>>().len() != ids.len()
    {
        Err(ClientSubmitError::InvalidAdministrativeRequest)
    } else {
        Ok(())
    }
}

pub(super) fn encode_admin_table_columns(
    columns: &[AdminColumnDefinition],
) -> Result<String, ClientSubmitError> {
    use crate::tuple::ScalarType;

    if columns.is_empty() || columns.len() > crate::tuple::MAX_COLUMNS {
        return Err(ClientSubmitError::InvalidAdministrativeRequest);
    }
    let mut seen_names = HashSet::with_capacity(columns.len());
    let mut encoded_columns = Vec::with_capacity(columns.len());
    for column in columns {
        validate_admin_name(&column.name)?;
        if !seen_names.insert(column.name.as_str()) {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        if column.generated_uuid_identity
            && (column.scalar_type != ScalarType::Uuid || column.default.is_some())
        {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let scalar_type = encode_admin_scalar_type(column.scalar_type)?;
        let mut encoded = format!(
            "{{\"name\":{},\"type\":{},\"nullable\":{},\"generated_uuid_identity\":{}",
            json_quote(&column.name),
            json_quote(&scalar_type),
            column.nullable,
            column.generated_uuid_identity
        );
        if let Some(default) = &column.default {
            match default {
                AdminColumnDefault::Null if column.nullable => {
                    encoded.push_str(",\"default\":{\"kind\":\"null\"}");
                }
                AdminColumnDefault::Null => {
                    return Err(ClientSubmitError::InvalidAdministrativeRequest);
                }
                AdminColumnDefault::CurrentPrincipalId
                    if column.scalar_type == ScalarType::Uuid =>
                {
                    encoded.push_str(",\"default\":{\"kind\":\"current_principal_id\"}");
                }
                AdminColumnDefault::CurrentPrincipalId => {
                    return Err(ClientSubmitError::InvalidAdministrativeRequest);
                }
                AdminColumnDefault::Literal(value) => {
                    let text = encode_admin_default_literal(column.scalar_type, value)?;
                    encoded.push_str(&format!(
                        ",\"default\":{{\"kind\":\"literal\",\"value\":{}}}",
                        json_quote(&text)
                    ));
                }
            }
        }
        encoded.push('}');
        encoded_columns.push(encoded);
    }
    Ok(format!("[{}]", encoded_columns.join(",")))
}

pub(super) fn encode_admin_scalar_type(
    scalar_type: crate::tuple::ScalarType,
) -> Result<String, ClientSubmitError> {
    use crate::tuple::ScalarType;
    match scalar_type {
        ScalarType::Boolean => Ok("boolean".into()),
        ScalarType::SmallInt => Ok("smallint".into()),
        ScalarType::Integer => Ok("integer".into()),
        ScalarType::BigInt => Ok("bigint".into()),
        ScalarType::Decimal { precision, scale }
            if (1..=38).contains(&precision) && scale <= precision =>
        {
            Ok(format!("decimal({precision},{scale})"))
        }
        ScalarType::Decimal { .. } => Err(ClientSubmitError::InvalidAdministrativeRequest),
        ScalarType::Text => Ok("text".into()),
        ScalarType::Varchar { max_chars } if max_chars > 0 => Ok(format!("varchar({max_chars})")),
        ScalarType::Varchar { .. } => Err(ClientSubmitError::InvalidAdministrativeRequest),
        ScalarType::Bytea => Ok("bytea".into()),
        ScalarType::Uuid => Ok("uuid".into()),
        ScalarType::Date => Ok("date".into()),
        ScalarType::TimestampTz => Ok("timestamptz".into()),
    }
}

pub(super) fn encode_admin_default_json(
    scalar_type: crate::tuple::ScalarType,
    default: AdminColumnDefault,
) -> Result<String, ClientSubmitError> {
    let encoded = encode_admin_table_columns(&[AdminColumnDefinition {
        name: "_default".into(),
        scalar_type,
        nullable: true,
        generated_uuid_identity: false,
        default: Some(default),
    }])?;
    let column = encoded
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .ok_or(ClientSubmitError::InvalidAdministrativeRequest)?;
    let start = column
        .find(",\"default\":")
        .map(|index| index + 1)
        .ok_or(ClientSubmitError::InvalidAdministrativeRequest)?;
    column
        .get(start..column.len().saturating_sub(1))
        .map(str::to_owned)
        .ok_or(ClientSubmitError::InvalidAdministrativeRequest)
}

pub(super) fn encode_admin_default_literal(
    scalar_type: crate::tuple::ScalarType,
    value: &crate::tuple::ScalarValue,
) -> Result<String, ClientSubmitError> {
    use crate::tuple::{ScalarType, ScalarValue};
    let invalid = || ClientSubmitError::InvalidAdministrativeRequest;
    let text = match (scalar_type, value) {
        (ScalarType::Boolean, ScalarValue::Boolean(value)) => value.to_string(),
        (ScalarType::SmallInt, ScalarValue::SmallInt(value)) => value.to_string(),
        (ScalarType::Integer, ScalarValue::Integer(value)) => value.to_string(),
        (ScalarType::BigInt, ScalarValue::BigInt(value)) => value.to_string(),
        (ScalarType::Decimal { precision, scale }, ScalarValue::Decimal(value)) => {
            let limit = 10u128.pow(u32::from(precision));
            let magnitude = value.unsigned_abs();
            if magnitude >= limit {
                return Err(invalid());
            }
            let factor = 10u128.pow(u32::from(scale));
            let sign = if *value < 0 { "-" } else { "" };
            if scale == 0 {
                format!("{sign}{magnitude}")
            } else {
                format!(
                    "{sign}{}.{:0width$}",
                    magnitude / factor,
                    magnitude % factor,
                    width = usize::from(scale)
                )
            }
        }
        (ScalarType::Text, ScalarValue::Text(value)) => value.clone(),
        (ScalarType::Varchar { max_chars }, ScalarValue::Varchar(value))
            if value.chars().count() <= max_chars as usize =>
        {
            value.clone()
        }
        (ScalarType::Bytea, ScalarValue::Bytea(value)) => lower_hex(value),
        (ScalarType::Uuid, ScalarValue::Uuid(value)) => lower_hex(value),
        (ScalarType::Date, ScalarValue::Date(value)) => value.to_string(),
        (ScalarType::TimestampTz, ScalarValue::TimestampTz(value)) => value.to_string(),
        _ => return Err(invalid()),
    };
    Ok(text)
}

pub(super) fn json_quote(text: &str) -> String {
    let mut quoted = String::with_capacity(text.len() + 2);
    quoted.push('"');
    for character in text.chars() {
        match character {
            '"' => quoted.push_str("\\\""),
            '\\' => quoted.push_str("\\\\"),
            '\n' => quoted.push_str("\\n"),
            '\r' => quoted.push_str("\\r"),
            '\t' => quoted.push_str("\\t"),
            character if character <= '\u{1f}' => {
                use std::fmt::Write as _;
                let _ = write!(quoted, "\\u{:04x}", character as u32);
            }
            character => quoted.push(character),
        }
    }
    quoted.push('"');
    quoted
}

pub(super) fn decode_token_secret(text: &str) -> Option<Zeroizing<[u8; TOKEN_SECRET_BYTES]>> {
    if text.len() != 44 || !text.is_ascii() || !text.ends_with('=') {
        return None;
    }
    let mut secret = Zeroizing::new([0; TOKEN_SECRET_BYTES]);
    let mut accumulator = Zeroizing::new(0u32);
    let mut bit_count = 0u8;
    let mut output_index = 0usize;
    for byte in text.bytes().take(43) {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        };
        *accumulator = (*accumulator << 6) | u32::from(value);
        bit_count += 6;
        if bit_count >= 8 {
            bit_count -= 8;
            if output_index >= secret.len() {
                return None;
            }
            secret[output_index] = (*accumulator >> bit_count) as u8;
            output_index += 1;
        }
    }
    if output_index != TOKEN_SECRET_BYTES
        || bit_count != 2
        || (*accumulator & 0b11) != 0
        || encode_base64(secret.as_ref()) != text
    {
        return None;
    }
    Some(secret)
}

pub(super) fn discovery_records(
    records: Vec<ResponseValue>,
) -> Result<Vec<HashMap<String, ResponseValue>>, ClientSubmitError> {
    records
        .into_iter()
        .map(|value| match value {
            ResponseValue::Object(fields) => Ok(fields),
            _ => Err(ClientSubmitError::InvalidResponse),
        })
        .collect()
}

pub(super) fn validate_metadata_page_request(
    page_limit: usize,
    cursor: Option<&str>,
) -> Result<(), ClientSubmitError> {
    if !(1..=crate::client_protocol::CLIENT_METADATA_MAX_PAGE).contains(&page_limit) {
        return Err(ClientSubmitError::InvalidMetadataPage);
    }
    if let Some(cursor) = cursor {
        decode_metadata_cursor(cursor).map_err(|_| ClientSubmitError::InvalidMetadataPage)?;
    }
    Ok(())
}

pub(super) fn metadata_page_arguments(page_limit: usize, cursor: Option<&str>) -> String {
    format!("{{{}}}", metadata_page_arguments_fields(page_limit, cursor))
}

pub(super) fn metadata_page_arguments_fields(page_limit: usize, cursor: Option<&str>) -> String {
    let mut fields = format!("\"limit\":\"{page_limit}\"");
    if let Some(cursor) = cursor {
        fields.push_str(",\"cursor\":\"");
        fields.push_str(cursor);
        fields.push('"');
    }
    fields
}

pub(super) fn decode_metadata_cursor(text: &str) -> Result<[u8; 16], ClientSubmitError> {
    if text.len() != 32 {
        return Err(ClientSubmitError::InvalidResponse);
    }
    let mut cursor = [0; 16];
    for (index, pair) in text.as_bytes().chunks_exact(2).enumerate() {
        let high = lower_hex_digit(pair[0]).ok_or(ClientSubmitError::InvalidResponse)?;
        let low = lower_hex_digit(pair[1]).ok_or(ClientSubmitError::InvalidResponse)?;
        cursor[index] = (high << 4) | low;
    }
    if cursor[0] != 0xd3 {
        return Err(ClientSubmitError::InvalidResponse);
    }
    Ok(cursor)
}

pub(super) fn lower_hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

pub(super) fn discovery_page_value(
    payload: &[u8],
    operation: &str,
) -> Result<(Vec<ResponseValue>, Option<String>), ClientSubmitError> {
    let key = match operation {
        "metadata.schemas.list" => "schemas",
        "metadata.relations.list" => "relations",
        "metadata.columns.list" => "columns",
        "metadata.indexes.list" => "indexes",
        "metadata.constraints.list" => "constraints",
        "metadata.grants.list" => "grants",
        "metadata.policies.list" => "policies",
        "metadata.dependencies.list" => "dependencies",
        _ => return Err(ClientSubmitError::InvalidResponse),
    };
    let mut fields = parse_flat_response(payload)?;
    if !(fields.len() == 1 || fields.len() == 2) {
        return Err(ClientSubmitError::InvalidResponse);
    }
    let records = fields
        .remove(key)
        .and_then(|value| value.as_array().map(<[ResponseValue]>::to_vec))
        .ok_or(ClientSubmitError::InvalidResponse)?;
    let cursor = match fields.remove("next_cursor") {
        None | Some(ResponseValue::Null) => None,
        Some(ResponseValue::String(value)) => {
            decode_metadata_cursor(&value)?;
            Some(value)
        }
        _ => return Err(ClientSubmitError::InvalidResponse),
    };
    if !fields.is_empty() {
        return Err(ClientSubmitError::InvalidResponse);
    }
    Ok((records, cursor))
}

pub(super) fn response_string(
    fields: &mut HashMap<String, ResponseValue>,
    key: &str,
) -> Result<String, ClientSubmitError> {
    match fields.remove(key) {
        Some(ResponseValue::String(value)) => Ok(value),
        _ => Err(ClientSubmitError::InvalidResponse),
    }
}

pub(super) fn response_decimal_u64(
    fields: &HashMap<String, ResponseValue>,
    key: &str,
) -> Result<u64, ClientSubmitError> {
    let value = fields
        .get(key)
        .and_then(ResponseValue::as_string)
        .ok_or(ClientSubmitError::InvalidResponse)?;
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(ClientSubmitError::InvalidResponse);
    }
    value
        .parse::<u64>()
        .map_err(|_| ClientSubmitError::InvalidResponse)
}

pub(super) fn response_id(
    fields: &HashMap<String, ResponseValue>,
    key: &str,
) -> Result<[u8; 16], ClientSubmitError> {
    let id = fields
        .get(key)
        .and_then(ResponseValue::as_string)
        .and_then(decode_hex_array::<16>)
        .filter(|id| *id != [0; 16])
        .ok_or(ClientSubmitError::InvalidResponse)?;
    Ok(id)
}

pub(super) fn response_id_array(
    fields: &mut HashMap<String, ResponseValue>,
    key: &str,
) -> Result<Vec<[u8; 16]>, ClientSubmitError> {
    let values = match fields.remove(key) {
        Some(ResponseValue::Array(values)) => values,
        _ => return Err(ClientSubmitError::InvalidResponse),
    };
    values
        .into_iter()
        .map(|value| match value {
            ResponseValue::String(value) => decode_hex_array::<16>(&value)
                .filter(|id| *id != [0; 16])
                .ok_or(ClientSubmitError::InvalidResponse),
            _ => Err(ClientSubmitError::InvalidResponse),
        })
        .collect()
}

pub(super) fn response_u64_array(
    fields: &mut HashMap<String, ResponseValue>,
    key: &str,
    expected_len: usize,
) -> Result<[u64; 6], ClientSubmitError> {
    let values = match fields.remove(key) {
        Some(ResponseValue::Array(values)) if values.len() == expected_len && expected_len == 6 => {
            values
        }
        _ => return Err(ClientSubmitError::InvalidResponse),
    };
    let mut output = [0; 6];
    for (index, value) in values.into_iter().enumerate() {
        let ResponseValue::Number(value) = value else {
            return Err(ClientSubmitError::InvalidResponse);
        };
        output[index] = value;
    }
    Ok(output)
}

pub(super) fn response_boolean(
    fields: &mut HashMap<String, ResponseValue>,
    key: &str,
) -> Result<bool, ClientSubmitError> {
    match fields.remove(key) {
        Some(ResponseValue::Boolean(value)) => Ok(value),
        _ => Err(ClientSubmitError::InvalidResponse),
    }
}

pub(super) fn strictly_ordered_ids(ids: &[[u8; 16]]) -> bool {
    ids.windows(2).all(|pair| pair[0] < pair[1])
}

pub(super) fn response_u64(
    fields: &mut HashMap<String, ResponseValue>,
    key: &str,
) -> Result<Option<u64>, ClientSubmitError> {
    match fields.remove(key) {
        Some(ResponseValue::Number(value)) => Ok(Some(value)),
        _ => Err(ClientSubmitError::InvalidResponse),
    }
}

pub(super) fn response_optional_u64(
    fields: &mut HashMap<String, ResponseValue>,
    key: &str,
) -> Result<Option<u64>, ClientSubmitError> {
    match fields.remove(key) {
        Some(ResponseValue::Number(value)) => Ok(Some(value)),
        Some(ResponseValue::Null) => Ok(None),
        _ => Err(ClientSubmitError::InvalidResponse),
    }
}

pub(super) fn response_optional_u16(
    fields: &mut HashMap<String, ResponseValue>,
    key: &str,
) -> Result<Option<u16>, ClientSubmitError> {
    response_optional_u64(fields, key)?
        .map(|value| u16::try_from(value).map_err(|_| ClientSubmitError::InvalidResponse))
        .transpose()
}

pub(super) fn decode_admin_runtime_cursor(value: &str) -> Option<[u8; 49]> {
    let bytes = decode_hex_array::<49>(value)?;
    if bytes[0] != 1
        || bytes[1..17] == [0; 16]
        || bytes[17..33] == [0; 16]
        || bytes[33..49] == [0; 16]
    {
        return None;
    }
    Some(bytes)
}

pub(super) fn response_timestamp(
    fields: &mut HashMap<String, ResponseValue>,
    key: &str,
) -> Result<i64, ClientSubmitError> {
    match fields.remove(key) {
        Some(ResponseValue::Number(value)) => {
            i64::try_from(value).map_err(|_| ClientSubmitError::InvalidResponse)
        }
        _ => Err(ClientSubmitError::InvalidResponse),
    }
}
