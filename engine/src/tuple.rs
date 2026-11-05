//! Schema-aware typed row payloads and field-preserving patch application.
use crate::row_record::MAX_ROW_RECORD_BYTES;
use std::borrow::Cow;

const MAGIC: &[u8; 4] = b"TUP1";
const FORMAT_VERSION: u16 = 1;
const HEADER_LEN: usize = 20;
pub const MAX_COLUMNS: usize = 4096;
pub const MAX_TUPLE_BYTES: usize = MAX_ROW_RECORD_BYTES - 20;
const MIN_DATE_DAYS: i32 = -719_162;
const MAX_DATE_DAYS: i32 = 2_932_896;
const MIN_TIMESTAMP_MICROS: i64 = -62_135_596_800_000_000;
const MAX_TIMESTAMP_MICROS: i64 = 253_402_300_799_999_999;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScalarType {
    Boolean,
    SmallInt,
    Integer,
    BigInt,
    Decimal { precision: u8, scale: u8 },
    Text,
    Varchar { max_chars: u32 },
    Bytea,
    Uuid,
    Date,
    TimestampTz,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnSchema {
    pub scalar_type: ScalarType,
    pub nullable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScalarValue {
    Boolean(bool),
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    Decimal(i128),
    Text(String),
    Varchar(String),
    Bytea(Vec<u8>),
    Uuid([u8; 16]),
    Date(i32),
    TimestampTz(i64),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Tuple {
    pub schema_version: u64,
    pub values: Vec<Option<ScalarValue>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FieldPatch {
    pub ordinal: usize,
    pub value: Option<ScalarValue>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TupleError {
    InvalidLength,
    TooLarge,
    InvalidMagic,
    UnsupportedVersion,
    NonzeroFlags,
    InvalidSchemaVersion,
    InvalidColumnCount,
    SchemaVersionMismatch,
    InvalidNull,
    InvalidFieldState,
    TypeMismatch,
    InvalidScalarEncoding,
    InvalidScalarRange,
    InvalidDecimalParameters,
    DecimalPrecisionExceeded,
    VarcharLengthExceeded,
    DuplicatePatch,
    PatchOutOfRange,
}

impl std::fmt::Display for TupleError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "invalid tuple payload: {self:?}")
    }
}

impl std::error::Error for TupleError {}

pub fn encode_tuple(
    columns: &[ColumnSchema],
    schema_version: u64,
    values: &[Option<ScalarValue>],
) -> Result<Vec<u8>, TupleError> {
    validate_shape(columns, schema_version)?;
    if values.len() != columns.len() {
        return Err(TupleError::InvalidColumnCount);
    }
    let mut bytes = Vec::with_capacity(HEADER_LEN);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&0u16.to_le_bytes());
    bytes.extend_from_slice(&schema_version.to_le_bytes());
    bytes.extend_from_slice(&(columns.len() as u16).to_le_bytes());
    bytes.extend_from_slice(&0u16.to_le_bytes());
    for (column, value) in columns.iter().zip(values) {
        let payload = match value {
            None if column.nullable => Cow::<[u8]>::Borrowed(&[]),
            None => return Err(TupleError::InvalidNull),
            Some(value) => encode_scalar(column.scalar_type, value)?,
        };
        let length = u32::try_from(payload.len()).map_err(|_| TupleError::TooLarge)?;
        let next_len = bytes
            .len()
            .checked_add(5)
            .and_then(|len| len.checked_add(payload.len()))
            .ok_or(TupleError::TooLarge)?;
        if next_len > MAX_TUPLE_BYTES {
            return Err(TupleError::TooLarge);
        }
        bytes.push(u8::from(value.is_some()));
        bytes.extend_from_slice(&length.to_le_bytes());
        bytes.extend_from_slice(payload.as_ref());
    }
    Ok(bytes)
}

pub fn decode_tuple(
    columns: &[ColumnSchema],
    expected_schema_version: u64,
    bytes: &[u8],
) -> Result<Tuple, TupleError> {
    validate_shape(columns, expected_schema_version)?;
    if bytes.len() < HEADER_LEN {
        return Err(TupleError::InvalidLength);
    }
    if bytes.len() > MAX_TUPLE_BYTES {
        return Err(TupleError::TooLarge);
    }
    if &bytes[..4] != MAGIC {
        return Err(TupleError::InvalidMagic);
    }
    if u16::from_le_bytes(bytes[4..6].try_into().expect("fixed range")) != FORMAT_VERSION {
        return Err(TupleError::UnsupportedVersion);
    }
    if bytes[6..8] != [0; 2] || bytes[18..20] != [0; 2] {
        return Err(TupleError::NonzeroFlags);
    }
    let schema_version = u64::from_le_bytes(bytes[8..16].try_into().expect("fixed range"));
    if schema_version == 0 {
        return Err(TupleError::InvalidSchemaVersion);
    }
    if schema_version != expected_schema_version {
        return Err(TupleError::SchemaVersionMismatch);
    }
    let count = u16::from_le_bytes(bytes[16..18].try_into().expect("fixed range")) as usize;
    if count != columns.len() {
        return Err(TupleError::InvalidColumnCount);
    }
    let mut offset = HEADER_LEN;
    let mut values = Vec::with_capacity(count);
    for column in columns {
        let header_end = offset.checked_add(5).ok_or(TupleError::InvalidLength)?;
        if header_end > bytes.len() {
            return Err(TupleError::InvalidLength);
        }
        let state = bytes[offset];
        let payload_len = u32::from_le_bytes(
            bytes[offset + 1..header_end]
                .try_into()
                .expect("fixed range"),
        ) as usize;
        offset = header_end;
        let payload_end = offset
            .checked_add(payload_len)
            .ok_or(TupleError::InvalidLength)?;
        if payload_end > bytes.len() {
            return Err(TupleError::InvalidLength);
        }
        let payload = &bytes[offset..payload_end];
        offset = payload_end;
        match state {
            0 if payload.is_empty() && column.nullable => values.push(None),
            0 if payload.is_empty() => return Err(TupleError::InvalidNull),
            0 => return Err(TupleError::InvalidLength),
            1 => values.push(Some(decode_scalar(column.scalar_type, payload)?)),
            _ => return Err(TupleError::InvalidFieldState),
        }
    }
    if offset != bytes.len() {
        return Err(TupleError::InvalidLength);
    }
    Ok(Tuple {
        schema_version,
        values,
    })
}

pub fn apply_patch(
    columns: &[ColumnSchema],
    tuple: &mut Tuple,
    patches: &[FieldPatch],
) -> Result<(), TupleError> {
    validate_shape(columns, tuple.schema_version)?;
    if tuple.values.len() != columns.len() {
        return Err(TupleError::InvalidColumnCount);
    }
    for (column, value) in columns.iter().zip(&tuple.values) {
        match value {
            None if column.nullable => {}
            None => return Err(TupleError::InvalidNull),
            Some(value) => {
                encode_scalar(column.scalar_type, value)?;
            }
        }
    }
    let mut seen = vec![false; columns.len()];
    for patch in patches {
        let Some(column) = columns.get(patch.ordinal) else {
            return Err(TupleError::PatchOutOfRange);
        };
        if std::mem::replace(&mut seen[patch.ordinal], true) {
            return Err(TupleError::DuplicatePatch);
        }
        match &patch.value {
            None if column.nullable => {}
            None => return Err(TupleError::InvalidNull),
            Some(value) => {
                encode_scalar(column.scalar_type, value)?;
            }
        }
    }
    for patch in patches {
        tuple.values[patch.ordinal] = patch.value.clone();
    }
    Ok(())
}

fn validate_shape(columns: &[ColumnSchema], schema_version: u64) -> Result<(), TupleError> {
    if schema_version == 0 {
        return Err(TupleError::InvalidSchemaVersion);
    }
    if columns.is_empty() || columns.len() > MAX_COLUMNS || columns.len() > u16::MAX as usize {
        return Err(TupleError::InvalidColumnCount);
    }
    for column in columns {
        if let ScalarType::Decimal { precision, scale } = column.scalar_type
            && (precision == 0 || precision > 38 || scale > precision)
        {
            return Err(TupleError::InvalidDecimalParameters);
        }
        if let ScalarType::Varchar { max_chars: 0 } = column.scalar_type {
            return Err(TupleError::InvalidScalarRange);
        }
    }
    Ok(())
}

fn encode_scalar<'a>(
    kind: ScalarType,
    value: &'a ScalarValue,
) -> Result<Cow<'a, [u8]>, TupleError> {
    let bytes: Cow<'a, [u8]> = match (kind, value) {
        (ScalarType::Boolean, ScalarValue::Boolean(value)) => Cow::Owned(vec![u8::from(*value)]),
        (ScalarType::SmallInt, ScalarValue::SmallInt(value)) => {
            Cow::Owned(value.to_le_bytes().to_vec())
        }
        (ScalarType::Integer, ScalarValue::Integer(value)) => {
            Cow::Owned(value.to_le_bytes().to_vec())
        }
        (ScalarType::BigInt, ScalarValue::BigInt(value)) => {
            Cow::Owned(value.to_le_bytes().to_vec())
        }
        (ScalarType::Decimal { precision, scale }, ScalarValue::Decimal(value)) => {
            validate_decimal(*value, precision, scale)?;
            Cow::Owned(value.to_le_bytes().to_vec())
        }
        (ScalarType::Text, ScalarValue::Text(value)) => Cow::Borrowed(value.as_bytes()),
        (ScalarType::Varchar { max_chars }, ScalarValue::Varchar(value)) => {
            if value.chars().count() > max_chars as usize {
                return Err(TupleError::VarcharLengthExceeded);
            }
            Cow::Borrowed(value.as_bytes())
        }
        (ScalarType::Bytea, ScalarValue::Bytea(value)) => Cow::Borrowed(value.as_slice()),
        (ScalarType::Uuid, ScalarValue::Uuid(value)) => Cow::Borrowed(value),
        (ScalarType::Date, ScalarValue::Date(value)) => {
            if !(MIN_DATE_DAYS..=MAX_DATE_DAYS).contains(value) {
                return Err(TupleError::InvalidScalarRange);
            }
            Cow::Owned(value.to_le_bytes().to_vec())
        }
        (ScalarType::TimestampTz, ScalarValue::TimestampTz(value)) => {
            if !(MIN_TIMESTAMP_MICROS..=MAX_TIMESTAMP_MICROS).contains(value) {
                return Err(TupleError::InvalidScalarRange);
            }
            Cow::Owned(value.to_le_bytes().to_vec())
        }
        _ => return Err(TupleError::TypeMismatch),
    };
    Ok(bytes)
}

fn decode_scalar(kind: ScalarType, bytes: &[u8]) -> Result<ScalarValue, TupleError> {
    let value = match kind {
        ScalarType::Boolean => match bytes {
            [0] => ScalarValue::Boolean(false),
            [1] => ScalarValue::Boolean(true),
            _ => return Err(TupleError::InvalidScalarEncoding),
        },
        ScalarType::SmallInt => ScalarValue::SmallInt(i16::from_le_bytes(fixed(bytes)?)),
        ScalarType::Integer => ScalarValue::Integer(i32::from_le_bytes(fixed(bytes)?)),
        ScalarType::BigInt => ScalarValue::BigInt(i64::from_le_bytes(fixed(bytes)?)),
        ScalarType::Decimal { precision, scale } => {
            let value = i128::from_le_bytes(fixed(bytes)?);
            validate_decimal(value, precision, scale)?;
            ScalarValue::Decimal(value)
        }
        ScalarType::Text => ScalarValue::Text(
            String::from_utf8(bytes.to_vec()).map_err(|_| TupleError::InvalidScalarEncoding)?,
        ),
        ScalarType::Varchar { max_chars } => {
            let value =
                String::from_utf8(bytes.to_vec()).map_err(|_| TupleError::InvalidScalarEncoding)?;
            if value.chars().count() > max_chars as usize {
                return Err(TupleError::VarcharLengthExceeded);
            }
            ScalarValue::Varchar(value)
        }
        ScalarType::Bytea => ScalarValue::Bytea(bytes.to_vec()),
        ScalarType::Uuid => ScalarValue::Uuid(fixed(bytes)?),
        ScalarType::Date => {
            let value = i32::from_le_bytes(fixed(bytes)?);
            if !(MIN_DATE_DAYS..=MAX_DATE_DAYS).contains(&value) {
                return Err(TupleError::InvalidScalarRange);
            }
            ScalarValue::Date(value)
        }
        ScalarType::TimestampTz => {
            let value = i64::from_le_bytes(fixed(bytes)?);
            if !(MIN_TIMESTAMP_MICROS..=MAX_TIMESTAMP_MICROS).contains(&value) {
                return Err(TupleError::InvalidScalarRange);
            }
            ScalarValue::TimestampTz(value)
        }
    };
    Ok(value)
}

fn fixed<const N: usize>(bytes: &[u8]) -> Result<[u8; N], TupleError> {
    bytes
        .try_into()
        .map_err(|_| TupleError::InvalidScalarEncoding)
}

fn validate_decimal(value: i128, precision: u8, scale: u8) -> Result<(), TupleError> {
    if precision == 0 || precision > 38 || scale > precision {
        return Err(TupleError::InvalidDecimalParameters);
    }
    let digits = value.unsigned_abs().to_string().len();
    if digits > precision as usize {
        return Err(TupleError::DecimalPrecisionExceeded);
    }
    Ok(())
}
