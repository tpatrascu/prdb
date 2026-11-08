//! Version 1 ordered index-key components.
//!
//! Encoded components are only comparable within the same index definition.

use crate::tuple::{ColumnSchema, ScalarType, ScalarValue, TupleError};
use std::collections::{HashMap, HashSet};

/// Maximum complete SQL secondary-index key accepted by the engine. A row
/// record is capped at 1 MiB; BYTEA escaping can double tuple bytes, each of
/// at most 4096 components can add one marker, and the index and row UUID
/// prefixes add 32 bytes.
pub const MAX_INDEX_KEY_BYTES: usize =
    2 * crate::row_record::MAX_ROW_RECORD_BYTES + crate::tuple::MAX_COLUMNS + 32;

/// Encode a complete SQL tuple using the pinned column types and v1 ordering.
/// `TUP1` validation is reused to enforce nullability, scalar ranges, decimal
/// precision, and VARCHAR character limits before emitting ordered bytes.
pub fn encode_sql_tuple(
    columns: &[ColumnSchema],
    values: &[Option<ScalarValue>],
) -> Result<Vec<u8>, TupleError> {
    crate::tuple::encode_tuple(columns, 1, values)?;
    let mut encoded = Vec::new();
    for (column, value) in columns.iter().zip(values) {
        let Some(value) = value else {
            encoded.push(0xff);
            continue;
        };
        let component = match (column.scalar_type, value) {
            (ScalarType::Boolean, ScalarValue::Boolean(value)) => encode_bool(*value).to_vec(),
            (ScalarType::SmallInt, ScalarValue::SmallInt(value)) => encode_i16(*value).to_vec(),
            (ScalarType::Integer, ScalarValue::Integer(value))
            | (ScalarType::Date, ScalarValue::Date(value)) => encode_i32(*value).to_vec(),
            (ScalarType::BigInt, ScalarValue::BigInt(value))
            | (ScalarType::TimestampTz, ScalarValue::TimestampTz(value)) => {
                encode_i64(*value).to_vec()
            }
            (ScalarType::Decimal { .. }, ScalarValue::Decimal(value)) => {
                encode_i128(*value).to_vec()
            }
            (ScalarType::Text, ScalarValue::Text(value))
            | (ScalarType::Varchar { .. }, ScalarValue::Varchar(value)) => {
                encode_bytes(value.as_bytes())
            }
            (ScalarType::Bytea, ScalarValue::Bytea(value)) => encode_bytes(value),
            (ScalarType::Uuid, ScalarValue::Uuid(value)) => encode_uuid(*value).to_vec(),
            _ => unreachable!("tuple validation checked the scalar type"),
        };
        encoded.extend_from_slice(&component);
    }
    Ok(encoded)
}

/// Compose the persisted v1 secondary-index key from its index ID, encoded SQL
/// tuple, and opaque row UUID suffix.
pub fn compose_key(index_id: [u8; 16], tuple: &[u8], row_id: [u8; 16]) -> Vec<u8> {
    let mut key = Vec::with_capacity(32usize.saturating_add(tuple.len()));
    key.extend_from_slice(&index_id);
    key.extend_from_slice(tuple);
    key.extend_from_slice(&row_id);
    key
}

/// Errors while deriving an index key from a catalog definition and row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeriveIndexKeyError {
    InvalidDefinition,
    Tuple(TupleError),
}

impl std::fmt::Display for DeriveIndexKeyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidDefinition => formatter.write_str("invalid index column definition"),
            Self::Tuple(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for DeriveIndexKeyError {}

/// A complete index key and the NULL fact needed by unique-index validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedIndexKey {
    pub key: Vec<u8>,
    pub has_null: bool,
}

/// Derive a complete v1 index key by projecting a validated row into the
/// index's ordered catalog column IDs.
pub fn derive_index_key(
    index_id: [u8; 16],
    row_id: [u8; 16],
    schema_version: u64,
    schema_column_ids: &[[u8; 16]],
    schema_columns: &[ColumnSchema],
    tuple_payload: &[u8],
    index_column_ids: &[[u8; 16]],
) -> Result<DerivedIndexKey, DeriveIndexKeyError> {
    if schema_column_ids.is_empty()
        || schema_column_ids.len() != schema_columns.len()
        || index_column_ids.is_empty()
    {
        return Err(DeriveIndexKeyError::InvalidDefinition);
    }

    let mut schema_ordinals = HashMap::with_capacity(schema_column_ids.len());
    for (ordinal, &column_id) in schema_column_ids.iter().enumerate() {
        if column_id == [0; 16] || schema_ordinals.insert(column_id, ordinal).is_some() {
            return Err(DeriveIndexKeyError::InvalidDefinition);
        }
    }

    let mut seen_index_columns = HashSet::with_capacity(index_column_ids.len());
    let mut projection_columns = Vec::with_capacity(index_column_ids.len());
    let mut projection_ordinals = Vec::with_capacity(index_column_ids.len());
    for &column_id in index_column_ids {
        if column_id == [0; 16] || !seen_index_columns.insert(column_id) {
            return Err(DeriveIndexKeyError::InvalidDefinition);
        }
        let Some(&ordinal) = schema_ordinals.get(&column_id) else {
            return Err(DeriveIndexKeyError::InvalidDefinition);
        };
        projection_columns.push(schema_columns[ordinal].clone());
        projection_ordinals.push(ordinal);
    }

    let tuple = crate::tuple::decode_tuple(schema_columns, schema_version, tuple_payload)
        .map_err(DeriveIndexKeyError::Tuple)?;
    let projection_values = projection_ordinals
        .iter()
        .map(|&ordinal| tuple.values[ordinal].clone())
        .collect::<Vec<_>>();
    let has_null = projection_values.iter().any(Option::is_none);
    let encoded_tuple = encode_sql_tuple(&projection_columns, &projection_values)
        .map_err(DeriveIndexKeyError::Tuple)?;

    Ok(DerivedIndexKey {
        key: compose_key(index_id, &encoded_tuple, row_id),
        has_null,
    })
}

/// An invalid or incomplete component.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidComponent;

/// Encode a non-null BOOLEAN component.
pub fn encode_bool(value: bool) -> [u8; 2] {
    [1, u8::from(value)]
}

/// Decode a non-null BOOLEAN component.
pub fn decode_bool(input: &[u8]) -> Result<(bool, usize), InvalidComponent> {
    match input.get(..2) {
        Some([1, 0]) => Ok((false, 2)),
        Some([1, 1]) => Ok((true, 2)),
        _ => Err(InvalidComponent),
    }
}

fn fixed_payload<const N: usize>(input: &[u8]) -> Result<[u8; N], InvalidComponent> {
    if input.first() != Some(&1) {
        return Err(InvalidComponent);
    }
    input
        .get(1..1 + N)
        .ok_or(InvalidComponent)?
        .try_into()
        .map_err(|_| InvalidComponent)
}

/// Decode a non-null signed 16-bit component.
pub fn decode_i16(input: &[u8]) -> Result<(i16, usize), InvalidComponent> {
    Ok((
        (u16::from_be_bytes(fixed_payload(input)?) ^ (1 << 15)) as i16,
        3,
    ))
}
/// Decode a non-null signed 32-bit component.
pub fn decode_i32(input: &[u8]) -> Result<(i32, usize), InvalidComponent> {
    Ok((
        (u32::from_be_bytes(fixed_payload(input)?) ^ (1 << 31)) as i32,
        5,
    ))
}
/// Decode a non-null signed 64-bit component.
pub fn decode_i64(input: &[u8]) -> Result<(i64, usize), InvalidComponent> {
    Ok((
        (u64::from_be_bytes(fixed_payload(input)?) ^ (1 << 63)) as i64,
        9,
    ))
}
/// Encode a non-null DECIMAL unscaled coefficient; the caller checks precision and scale.
pub fn encode_i128(value: i128) -> [u8; 17] {
    let mut result = [0; 17];
    result[0] = 1;
    result[1..].copy_from_slice(&((value as u128) ^ (1 << 127)).to_be_bytes());
    result
}
/// Decode a DECIMAL coefficient; the caller checks precision and scale.
pub fn decode_i128(input: &[u8]) -> Result<(i128, usize), InvalidComponent> {
    Ok((
        (u128::from_be_bytes(fixed_payload(input)?) ^ (1 << 127)) as i128,
        17,
    ))
}
/// Encode a non-null UUID component using its opaque bytes.
pub fn encode_uuid(value: [u8; 16]) -> [u8; 17] {
    let mut result = [0; 17];
    result[0] = 1;
    result[1..].copy_from_slice(&value);
    result
}
/// Decode a non-null UUID component using its opaque bytes.
pub fn decode_uuid(input: &[u8]) -> Result<([u8; 16], usize), InvalidComponent> {
    Ok((fixed_payload(input)?, 17))
}

/// Return the exclusive upper bound of a byte-prefix range, if finite.
pub fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let last = prefix.iter().rposition(|&byte| byte != 0xff)?;
    let mut upper = prefix[..=last].to_vec();
    upper[last] += 1;
    Some(upper)
}

/// Encode a non-null BYTEA payload (or already-validated UTF-8 text).
pub fn encode_bytes(value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(value.len().saturating_add(3));
    out.push(1);
    for &byte in value {
        out.push(byte);
        if byte == 0 {
            out.push(0xff);
        }
    }
    out.extend_from_slice(&[0, 0]);
    out
}

/// Decode one non-null variable-width component and return bytes consumed.
/// The caller validates text and applies its own input and allocation limits.
pub fn decode_bytes(input: &[u8]) -> Result<(Vec<u8>, usize), InvalidComponent> {
    if input.first() != Some(&1) {
        return Err(InvalidComponent);
    }
    let mut out = Vec::new();
    let mut pos = 1;
    while let Some(&byte) = input.get(pos) {
        pos += 1;
        if byte != 0 {
            out.push(byte);
            continue;
        }
        match input.get(pos) {
            Some(0) => return Ok((out, pos + 1)),
            Some(0xff) => {
                out.push(0);
                pos += 1;
            }
            _ => return Err(InvalidComponent),
        }
    }
    Err(InvalidComponent)
}

/// Encode a non-null SMALLINT component.
pub fn encode_i16(value: i16) -> [u8; 3] {
    let bytes = ((value as u16) ^ (1 << 15)).to_be_bytes();
    [1, bytes[0], bytes[1]]
}

/// Encode a non-null INTEGER component (also used for DATE day counts).
pub fn encode_i32(value: i32) -> [u8; 5] {
    let bytes = ((value as u32) ^ (1 << 31)).to_be_bytes();
    [1, bytes[0], bytes[1], bytes[2], bytes[3]]
}

/// Encode a non-null BIGINT component (also used for UTC timestamps).
pub fn encode_i64(value: i64) -> [u8; 9] {
    let bytes = ((value as u64) ^ (1 << 63)).to_be_bytes();
    [
        1, bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]
}
