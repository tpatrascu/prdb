//! Canonical, bounded EIR1 encoding for trusted catalog expressions.
use crate::authorization::Expr;
use crate::catalog::decode_registered_envelope;
use crate::tuple::{ColumnSchema, ScalarType, ScalarValue, encode_tuple};

const MAGIC: &[u8; 4] = b"EIR1";
const VERSION: u16 = 1;
const HEADER_LEN: usize = 8;
pub const MAX_EXPRESSION_BYTES: usize = 256 * 1024;
pub const MAX_EXPRESSION_NODES: usize = 256;
pub const MAX_EXPRESSION_DEPTH: usize = 32;
pub const MAX_FIELD_ORDINAL: usize = 4095;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExpressionCodecError {
    InvalidEncoding,
    UnsupportedVersion,
    TooLarge,
    TooComplex,
    InvalidScalar,
    InvalidCatalogRecord,
    TypeMismatch,
    InvalidDependencies,
}

impl std::fmt::Display for ExpressionCodecError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "invalid expression IR: {self:?}")
    }
}

impl std::error::Error for ExpressionCodecError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExpressionPurpose {
    Default,
    Check,
    PolicyUsing,
    PolicyCheck,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogExpression {
    pub expression_id: [u8; 16],
    pub purpose: ExpressionPurpose,
    pub result_type_id: [u8; 16],
    pub result_parameters: [u8; 8],
    pub dependencies: Vec<[u8; 16]>,
    pub expression: Expr,
}

/// Validate an expression record against its containing table schema and
/// expected result type. Stable field/role IDs discovered from EIR must match
/// the catalog record's declared dependency set exactly.
pub fn validate_expression_binding(
    expression: &CatalogExpression,
    columns: &[ColumnSchema],
    column_ids: &[[u8; 16]],
    expected_result_type_id: [u8; 16],
    expected_result_type: ScalarType,
) -> Result<(), ExpressionCodecError> {
    if columns.len() != column_ids.len()
        || columns.len() > crate::tuple::MAX_COLUMNS
        || column_ids.contains(&[0; 16])
        || column_ids
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>()
            .len()
            != column_ids.len()
        || expected_result_type_id == [0; 16]
        || expression.result_type_id != expected_result_type_id
        || expression.result_parameters != type_parameters(expected_result_type)
    {
        return Err(ExpressionCodecError::TypeMismatch);
    }
    validate_type(expected_result_type)?;
    for column in columns {
        validate_type(column.scalar_type)?;
    }
    if matches!(
        expression.purpose,
        ExpressionPurpose::Check | ExpressionPurpose::PolicyUsing | ExpressionPurpose::PolicyCheck
    ) && expected_result_type != ScalarType::Boolean
    {
        return Err(ExpressionCodecError::TypeMismatch);
    }
    if expression.purpose == ExpressionPurpose::Default
        && !matches!(
            &expression.expression,
            Expr::Literal(_) | Expr::CurrentPrincipalId
        )
    {
        return Err(ExpressionCodecError::TypeMismatch);
    }

    let mut dependencies = std::collections::HashSet::new();
    let mut remaining = MAX_EXPRESSION_NODES;
    let inferred = infer_type(
        &expression.expression,
        columns,
        column_ids,
        &mut dependencies,
        &mut remaining,
        1,
    )?;
    if !type_compatible(expected_result_type, inferred) {
        return Err(ExpressionCodecError::TypeMismatch);
    }
    let declared = expression
        .dependencies
        .iter()
        .copied()
        .collect::<std::collections::HashSet<_>>();
    if declared.len() != expression.dependencies.len()
        || declared.contains(&[0; 16])
        || declared != dependencies
    {
        return Err(ExpressionCodecError::InvalidDependencies);
    }

    if let Expr::Literal(value) = &expression.expression {
        encode_tuple(
            &[ColumnSchema {
                scalar_type: expected_result_type,
                nullable: true,
            }],
            1,
            std::slice::from_ref(value),
        )
        .map_err(|_| ExpressionCodecError::TypeMismatch)?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum InferredType {
    Null,
    Known(ScalarType),
}

fn infer_type(
    expression: &Expr,
    columns: &[ColumnSchema],
    column_ids: &[[u8; 16]],
    dependencies: &mut std::collections::HashSet<[u8; 16]>,
    remaining: &mut usize,
    depth: usize,
) -> Result<InferredType, ExpressionCodecError> {
    if depth > MAX_EXPRESSION_DEPTH || *remaining == 0 {
        return Err(ExpressionCodecError::TooComplex);
    }
    *remaining -= 1;
    let child = |expression: &Expr,
                 dependencies: &mut std::collections::HashSet<[u8; 16]>,
                 remaining: &mut usize| {
        infer_type(
            expression,
            columns,
            column_ids,
            dependencies,
            remaining,
            depth + 1,
        )
    };
    match expression {
        Expr::Literal(None) => Ok(InferredType::Null),
        Expr::Literal(Some(value)) => {
            validate_scalar(value)?;
            Ok(InferredType::Known(scalar_type(value)))
        }
        Expr::Field(ordinal) => {
            let column = columns
                .get(*ordinal)
                .ok_or(ExpressionCodecError::TypeMismatch)?;
            dependencies.insert(column_ids[*ordinal]);
            Ok(InferredType::Known(column.scalar_type))
        }
        Expr::CurrentPrincipalId => Ok(InferredType::Known(ScalarType::Uuid)),
        Expr::HasRole(role_id) => {
            if *role_id == [0; 16] {
                return Err(ExpressionCodecError::InvalidDependencies);
            }
            dependencies.insert(*role_id);
            Ok(InferredType::Known(ScalarType::Boolean))
        }
        Expr::Equal(left, right)
        | Expr::NotEqual(left, right)
        | Expr::Less(left, right)
        | Expr::LessEqual(left, right)
        | Expr::Greater(left, right)
        | Expr::GreaterEqual(left, right) => {
            let left = child(left, dependencies, remaining)?;
            let right = child(right, dependencies, remaining)?;
            if !types_comparable(left, right) {
                return Err(ExpressionCodecError::TypeMismatch);
            }
            Ok(InferredType::Known(ScalarType::Boolean))
        }
        Expr::And(expressions) | Expr::Or(expressions) => {
            for child_expression in expressions {
                if !type_is_boolean(child(child_expression, dependencies, remaining)?) {
                    return Err(ExpressionCodecError::TypeMismatch);
                }
            }
            Ok(InferredType::Known(ScalarType::Boolean))
        }
        Expr::Not(child_expression) => {
            if !type_is_boolean(child(child_expression, dependencies, remaining)?) {
                return Err(ExpressionCodecError::TypeMismatch);
            }
            Ok(InferredType::Known(ScalarType::Boolean))
        }
        Expr::IsNull(child_expression) => {
            child(child_expression, dependencies, remaining)?;
            Ok(InferredType::Known(ScalarType::Boolean))
        }
    }
}

fn scalar_type(value: &ScalarValue) -> ScalarType {
    match value {
        ScalarValue::Boolean(_) => ScalarType::Boolean,
        ScalarValue::SmallInt(_) => ScalarType::SmallInt,
        ScalarValue::Integer(_) => ScalarType::Integer,
        ScalarValue::BigInt(_) => ScalarType::BigInt,
        ScalarValue::Decimal(_) => ScalarType::Decimal {
            precision: 38,
            scale: 0,
        },
        ScalarValue::Text(_) => ScalarType::Text,
        ScalarValue::Varchar(_) => ScalarType::Varchar {
            max_chars: u32::MAX,
        },
        ScalarValue::Bytea(_) => ScalarType::Bytea,
        ScalarValue::Uuid(_) => ScalarType::Uuid,
        ScalarValue::Date(_) => ScalarType::Date,
        ScalarValue::TimestampTz(_) => ScalarType::TimestampTz,
    }
}

fn type_parameters(scalar_type: ScalarType) -> [u8; 8] {
    let mut parameters = [0; 8];
    match scalar_type {
        ScalarType::Decimal { precision, scale } => {
            parameters[..2].copy_from_slice(&(precision as u16).to_le_bytes());
            parameters[2..4].copy_from_slice(&(scale as u16).to_le_bytes());
        }
        ScalarType::Varchar { max_chars } => {
            parameters[4..].copy_from_slice(&max_chars.to_le_bytes());
        }
        _ => {}
    }
    parameters
}

fn validate_type(scalar_type: ScalarType) -> Result<(), ExpressionCodecError> {
    encode_tuple(
        &[ColumnSchema {
            scalar_type,
            nullable: true,
        }],
        1,
        &[None],
    )
    .map(|_| ())
    .map_err(|_| ExpressionCodecError::TypeMismatch)
}

fn type_is_boolean(value: InferredType) -> bool {
    matches!(
        value,
        InferredType::Null | InferredType::Known(ScalarType::Boolean)
    )
}

fn type_compatible(expected: ScalarType, actual: InferredType) -> bool {
    match actual {
        InferredType::Null => true,
        InferredType::Known(actual) => scalar_types_compatible(expected, actual),
    }
}

fn types_comparable(left: InferredType, right: InferredType) -> bool {
    match (left, right) {
        (InferredType::Null, _) | (_, InferredType::Null) => true,
        (InferredType::Known(left), InferredType::Known(right)) => {
            scalar_types_compatible(left, right)
        }
    }
}

fn scalar_types_compatible(left: ScalarType, right: ScalarType) -> bool {
    match (left, right) {
        (
            ScalarType::Decimal {
                scale: left_scale, ..
            },
            ScalarType::Decimal {
                scale: right_scale, ..
            },
        ) => left_scale == right_scale,
        (ScalarType::Varchar { .. }, ScalarType::Varchar { .. }) => true,
        _ => left == right,
    }
}

/// Decode the family-7 record envelope, metadata, dependency set, and EIR body.
/// This validates structural integrity; callers must schema-bind a record
/// before using it to evaluate defaults, constraints, or policies.
pub fn decode_catalog_expression(bytes: &[u8]) -> Result<CatalogExpression, ExpressionCodecError> {
    let record = decode_registered_envelope(bytes)
        .map_err(|_| ExpressionCodecError::InvalidCatalogRecord)?;
    decode_catalog_expression_record(&record)
}

pub fn decode_catalog_expression_record(
    record: &crate::catalog::CatalogRecord,
) -> Result<CatalogExpression, ExpressionCodecError> {
    if record.family_code != 7 || record.logical_key.len() != 19 {
        return Err(ExpressionCodecError::InvalidCatalogRecord);
    }
    let purpose = match catalog_u16(record, 1)? {
        1 => ExpressionPurpose::Default,
        2 => ExpressionPurpose::Check,
        3 => ExpressionPurpose::PolicyUsing,
        4 => ExpressionPurpose::PolicyCheck,
        _ => return Err(ExpressionCodecError::InvalidCatalogRecord),
    };
    if catalog_u32(record, 2)? != VERSION as u32 {
        return Err(ExpressionCodecError::UnsupportedVersion);
    }
    let result_type_id = catalog_bytes(record, 3)?
        .try_into()
        .map_err(|_| ExpressionCodecError::InvalidCatalogRecord)?;
    let result_parameters = catalog_bytes(record, 4)?
        .try_into()
        .map_err(|_| ExpressionCodecError::InvalidCatalogRecord)?;
    let expression = decode_expression(catalog_bytes(record, 5)?)?;
    let dependency_bytes = catalog_bytes(record, 6)?;
    if dependency_bytes.len() < 4 {
        return Err(ExpressionCodecError::InvalidCatalogRecord);
    }
    let count = u32::from_le_bytes(dependency_bytes[..4].try_into().unwrap()) as usize;
    let expected_dependency_bytes = count
        .checked_mul(16)
        .and_then(|length| length.checked_add(4))
        .ok_or(ExpressionCodecError::InvalidCatalogRecord)?;
    if dependency_bytes.len() != expected_dependency_bytes {
        return Err(ExpressionCodecError::InvalidCatalogRecord);
    }
    let dependencies = dependency_bytes[4..]
        .chunks_exact(16)
        .map(|id| id.try_into().unwrap())
        .collect();
    let expression_id = record.logical_key[3..19].try_into().unwrap();
    Ok(CatalogExpression {
        expression_id,
        purpose,
        result_type_id,
        result_parameters,
        dependencies,
        expression,
    })
}

fn catalog_bytes(
    record: &crate::catalog::CatalogRecord,
    tag: u16,
) -> Result<&[u8], ExpressionCodecError> {
    record
        .fields
        .iter()
        .find(|field| field.tag == tag)
        .map(|field| field.payload.as_slice())
        .ok_or(ExpressionCodecError::InvalidCatalogRecord)
}

fn catalog_u16(
    record: &crate::catalog::CatalogRecord,
    tag: u16,
) -> Result<u16, ExpressionCodecError> {
    Ok(u16::from_le_bytes(
        catalog_bytes(record, tag)?
            .try_into()
            .map_err(|_| ExpressionCodecError::InvalidCatalogRecord)?,
    ))
}

fn catalog_u32(
    record: &crate::catalog::CatalogRecord,
    tag: u16,
) -> Result<u32, ExpressionCodecError> {
    Ok(u32::from_le_bytes(
        catalog_bytes(record, tag)?
            .try_into()
            .map_err(|_| ExpressionCodecError::InvalidCatalogRecord)?,
    ))
}

/// Encode one typed expression as canonical EIR1 prefix nodes.
pub fn encode_expression(expression: &Expr) -> Result<Vec<u8>, ExpressionCodecError> {
    let mut encoder = Encoder {
        bytes: Vec::with_capacity(HEADER_LEN),
        nodes: 0,
    };
    encoder.append(MAGIC)?;
    encoder.u16(VERSION)?;
    encoder.u16(0)?;
    encoder.node(expression, 1)?;
    Ok(encoder.bytes)
}

/// Decode exactly one bounded EIR1 expression; unknown forms fail closed.
pub fn decode_expression(bytes: &[u8]) -> Result<Expr, ExpressionCodecError> {
    if bytes.len() < HEADER_LEN {
        return Err(ExpressionCodecError::InvalidEncoding);
    }
    if bytes.len() > MAX_EXPRESSION_BYTES {
        return Err(ExpressionCodecError::TooLarge);
    }
    if &bytes[..4] != MAGIC {
        return Err(ExpressionCodecError::InvalidEncoding);
    }
    if u16::from_le_bytes(bytes[4..6].try_into().unwrap()) != VERSION {
        return Err(ExpressionCodecError::UnsupportedVersion);
    }
    if bytes[6..8] != [0; 2] {
        return Err(ExpressionCodecError::InvalidEncoding);
    }
    let mut decoder = Decoder {
        bytes,
        offset: HEADER_LEN,
        nodes: 0,
    };
    let expression = decoder.node(1)?;
    if decoder.offset != bytes.len() {
        return Err(ExpressionCodecError::InvalidEncoding);
    }
    Ok(expression)
}

struct Encoder {
    bytes: Vec<u8>,
    nodes: usize,
}

impl Encoder {
    fn append(&mut self, bytes: &[u8]) -> Result<(), ExpressionCodecError> {
        let next = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or(ExpressionCodecError::TooLarge)?;
        if next > MAX_EXPRESSION_BYTES {
            return Err(ExpressionCodecError::TooLarge);
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn byte(&mut self, value: u8) -> Result<(), ExpressionCodecError> {
        self.append(&[value])
    }

    fn u16(&mut self, value: u16) -> Result<(), ExpressionCodecError> {
        self.append(&value.to_le_bytes())
    }

    fn u32(&mut self, value: u32) -> Result<(), ExpressionCodecError> {
        self.append(&value.to_le_bytes())
    }

    fn node(&mut self, expression: &Expr, depth: usize) -> Result<(), ExpressionCodecError> {
        self.enter_node(depth)?;
        match expression {
            Expr::Literal(value) => {
                self.byte(1)?;
                self.literal(value.as_ref())
            }
            Expr::Field(ordinal) => {
                if *ordinal > MAX_FIELD_ORDINAL {
                    return Err(ExpressionCodecError::InvalidEncoding);
                }
                self.byte(2)?;
                self.u16(*ordinal as u16)
            }
            Expr::CurrentPrincipalId => self.byte(3),
            Expr::HasRole(role_id) => {
                if *role_id == [0; 16] {
                    return Err(ExpressionCodecError::InvalidEncoding);
                }
                self.byte(4)?;
                self.append(role_id)
            }
            Expr::Equal(left, right) => self.binary(16, left, right, depth),
            Expr::NotEqual(left, right) => self.binary(17, left, right, depth),
            Expr::Less(left, right) => self.binary(18, left, right, depth),
            Expr::LessEqual(left, right) => self.binary(19, left, right, depth),
            Expr::Greater(left, right) => self.binary(20, left, right, depth),
            Expr::GreaterEqual(left, right) => self.binary(21, left, right, depth),
            Expr::And(expressions) => self.children(22, expressions, depth),
            Expr::Or(expressions) => self.children(23, expressions, depth),
            Expr::Not(child) => {
                self.byte(24)?;
                self.node(child, depth + 1)
            }
            Expr::IsNull(child) => {
                self.byte(25)?;
                self.node(child, depth + 1)
            }
        }
    }

    fn enter_node(&mut self, depth: usize) -> Result<(), ExpressionCodecError> {
        if depth > MAX_EXPRESSION_DEPTH || self.nodes >= MAX_EXPRESSION_NODES {
            return Err(ExpressionCodecError::TooComplex);
        }
        self.nodes += 1;
        Ok(())
    }

    fn binary(
        &mut self,
        opcode: u8,
        left: &Expr,
        right: &Expr,
        depth: usize,
    ) -> Result<(), ExpressionCodecError> {
        self.byte(opcode)?;
        self.node(left, depth + 1)?;
        self.node(right, depth + 1)
    }

    fn children(
        &mut self,
        opcode: u8,
        expressions: &[Expr],
        depth: usize,
    ) -> Result<(), ExpressionCodecError> {
        let count =
            u16::try_from(expressions.len()).map_err(|_| ExpressionCodecError::TooComplex)?;
        self.byte(opcode)?;
        self.u16(count)?;
        for expression in expressions {
            self.node(expression, depth + 1)?;
        }
        Ok(())
    }

    fn literal(&mut self, value: Option<&ScalarValue>) -> Result<(), ExpressionCodecError> {
        let Some(value) = value else {
            return self.byte(0);
        };
        validate_scalar(value)?;
        match value {
            ScalarValue::Boolean(value) => {
                self.byte(1)?;
                self.byte(u8::from(*value))
            }
            ScalarValue::SmallInt(value) => {
                self.byte(2)?;
                self.append(&value.to_le_bytes())
            }
            ScalarValue::Integer(value) => {
                self.byte(3)?;
                self.append(&value.to_le_bytes())
            }
            ScalarValue::BigInt(value) => {
                self.byte(4)?;
                self.append(&value.to_le_bytes())
            }
            ScalarValue::Decimal(value) => {
                self.byte(5)?;
                self.append(&value.to_le_bytes())
            }
            ScalarValue::Text(value) => {
                self.byte(6)?;
                self.bytes(value.as_bytes())
            }
            ScalarValue::Varchar(value) => {
                self.byte(7)?;
                self.bytes(value.as_bytes())
            }
            ScalarValue::Bytea(value) => {
                self.byte(8)?;
                self.bytes(value)
            }
            ScalarValue::Uuid(value) => {
                self.byte(9)?;
                self.append(value)
            }
            ScalarValue::Date(value) => {
                self.byte(10)?;
                self.append(&value.to_le_bytes())
            }
            ScalarValue::TimestampTz(value) => {
                self.byte(11)?;
                self.append(&value.to_le_bytes())
            }
        }
    }

    fn bytes(&mut self, bytes: &[u8]) -> Result<(), ExpressionCodecError> {
        let length = u32::try_from(bytes.len()).map_err(|_| ExpressionCodecError::TooLarge)?;
        self.u32(length)?;
        self.append(bytes)
    }
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
    nodes: usize,
}

impl Decoder<'_> {
    fn take(&mut self, length: usize) -> Result<&[u8], ExpressionCodecError> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|&end| end <= self.bytes.len())
            .ok_or(ExpressionCodecError::InvalidEncoding)?;
        let result = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(result)
    }

    fn byte(&mut self) -> Result<u8, ExpressionCodecError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, ExpressionCodecError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32, ExpressionCodecError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn node(&mut self, depth: usize) -> Result<Expr, ExpressionCodecError> {
        if depth > MAX_EXPRESSION_DEPTH || self.nodes >= MAX_EXPRESSION_NODES {
            return Err(ExpressionCodecError::TooComplex);
        }
        self.nodes += 1;
        match self.byte()? {
            1 => Ok(Expr::Literal(self.literal()?)),
            2 => {
                let ordinal = self.u16()? as usize;
                if ordinal > MAX_FIELD_ORDINAL {
                    return Err(ExpressionCodecError::InvalidEncoding);
                }
                Ok(Expr::Field(ordinal))
            }
            3 => Ok(Expr::CurrentPrincipalId),
            4 => {
                let role_id: [u8; 16] = self.take(16)?.try_into().unwrap();
                if role_id == [0; 16] {
                    return Err(ExpressionCodecError::InvalidEncoding);
                }
                Ok(Expr::HasRole(role_id))
            }
            16 => self.binary(Expr::Equal, depth),
            17 => self.binary(Expr::NotEqual, depth),
            18 => self.binary(Expr::Less, depth),
            19 => self.binary(Expr::LessEqual, depth),
            20 => self.binary(Expr::Greater, depth),
            21 => self.binary(Expr::GreaterEqual, depth),
            22 => self.children(false, depth),
            23 => self.children(true, depth),
            24 => Ok(Expr::Not(Box::new(self.node(depth + 1)?))),
            25 => Ok(Expr::IsNull(Box::new(self.node(depth + 1)?))),
            _ => Err(ExpressionCodecError::InvalidEncoding),
        }
    }

    fn binary(
        &mut self,
        constructor: fn(Box<Expr>, Box<Expr>) -> Expr,
        depth: usize,
    ) -> Result<Expr, ExpressionCodecError> {
        let left = Box::new(self.node(depth + 1)?);
        let right = Box::new(self.node(depth + 1)?);
        Ok(constructor(left, right))
    }

    fn children(&mut self, is_or: bool, depth: usize) -> Result<Expr, ExpressionCodecError> {
        let count = self.u16()? as usize;
        if count > MAX_EXPRESSION_NODES.saturating_sub(self.nodes) {
            return Err(ExpressionCodecError::TooComplex);
        }
        let mut expressions = Vec::with_capacity(count);
        for _ in 0..count {
            expressions.push(self.node(depth + 1)?);
        }
        Ok(if is_or {
            Expr::Or(expressions)
        } else {
            Expr::And(expressions)
        })
    }

    fn literal(&mut self) -> Result<Option<ScalarValue>, ExpressionCodecError> {
        let value = match self.byte()? {
            0 => return Ok(None),
            1 => match self.byte()? {
                0 => ScalarValue::Boolean(false),
                1 => ScalarValue::Boolean(true),
                _ => return Err(ExpressionCodecError::InvalidEncoding),
            },
            2 => ScalarValue::SmallInt(i16::from_le_bytes(self.take(2)?.try_into().unwrap())),
            3 => ScalarValue::Integer(i32::from_le_bytes(self.take(4)?.try_into().unwrap())),
            4 => ScalarValue::BigInt(i64::from_le_bytes(self.take(8)?.try_into().unwrap())),
            5 => ScalarValue::Decimal(i128::from_le_bytes(self.take(16)?.try_into().unwrap())),
            6 => ScalarValue::Text(self.string()?),
            7 => ScalarValue::Varchar(self.string()?),
            8 => ScalarValue::Bytea(self.bytes()?.to_vec()),
            9 => ScalarValue::Uuid(self.take(16)?.try_into().unwrap()),
            10 => ScalarValue::Date(i32::from_le_bytes(self.take(4)?.try_into().unwrap())),
            11 => ScalarValue::TimestampTz(i64::from_le_bytes(self.take(8)?.try_into().unwrap())),
            _ => return Err(ExpressionCodecError::InvalidEncoding),
        };
        validate_scalar(&value)?;
        Ok(Some(value))
    }

    fn string(&mut self) -> Result<String, ExpressionCodecError> {
        String::from_utf8(self.bytes()?.to_vec()).map_err(|_| ExpressionCodecError::InvalidScalar)
    }

    fn bytes(&mut self) -> Result<&[u8], ExpressionCodecError> {
        let length = self.u32()? as usize;
        self.take(length)
    }
}

fn validate_scalar(value: &ScalarValue) -> Result<(), ExpressionCodecError> {
    let scalar_type = match value {
        ScalarValue::Boolean(_) => ScalarType::Boolean,
        ScalarValue::SmallInt(_) => ScalarType::SmallInt,
        ScalarValue::Integer(_) => ScalarType::Integer,
        ScalarValue::BigInt(_) => ScalarType::BigInt,
        ScalarValue::Decimal(_) => ScalarType::Decimal {
            precision: 38,
            scale: 38,
        },
        ScalarValue::Text(_) => ScalarType::Text,
        ScalarValue::Varchar(_) => ScalarType::Varchar {
            max_chars: u32::MAX,
        },
        ScalarValue::Bytea(_) => ScalarType::Bytea,
        ScalarValue::Uuid(_) => ScalarType::Uuid,
        ScalarValue::Date(_) => ScalarType::Date,
        ScalarValue::TimestampTz(_) => ScalarType::TimestampTz,
    };
    encode_tuple(
        &[ColumnSchema {
            scalar_type,
            nullable: false,
        }],
        1,
        &[Some(value.clone())],
    )
    .map(|_| ())
    .map_err(|_| ExpressionCodecError::InvalidScalar)
}
