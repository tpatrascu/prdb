//! Bounded expression AST and binary codec for the unified optimizer IR.

use crate::tuple::ScalarType;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundColumn {
    pub column_id: [u8; 16],
    pub ordinal: u32,
    pub scalar_type: ScalarType,
    pub nullable: bool,
}

pub const MAX_IDENTIFIER_BYTES: usize = 63;
pub const MAX_EXPR_BYTES: usize = 16 * 1024;
pub const MAX_EXPR_NODES: usize = 256;
pub const MAX_EXPR_DEPTH: usize = 32;
pub const MAX_TEXT_LITERAL_BYTES: usize = 4096;
pub const MAX_PARAMETER_INDEX: u32 = 256;
pub const MAX_IN_LIST_ITEMS: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Comparison {
    Equal,
    NotEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
}

impl Comparison {
    pub fn to_byte(self) -> u8 {
        match self {
            Self::Equal => 0,
            Self::NotEqual => 1,
            Self::Less => 2,
            Self::LessOrEqual => 3,
            Self::Greater => 4,
            Self::GreaterOrEqual => 5,
        }
    }

    pub fn from_byte(byte: u8) -> Result<Self, ExprError> {
        match byte {
            0 => Ok(Self::Equal),
            1 => Ok(Self::NotEqual),
            2 => Ok(Self::Less),
            3 => Ok(Self::LessOrEqual),
            4 => Ok(Self::Greater),
            5 => Ok(Self::GreaterOrEqual),
            _ => Err(ExprError::InvalidTag(byte)),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnRef {
    pub relation_index: u16,
    pub column_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundColumnRef {
    pub relation_index: u16,
    pub column: BoundColumn,
}

/// Unbound expression tree carried in optimizer requests.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Expr {
    Null,
    Boolean(bool),
    Integer(i64),
    Text(String),
    Date(i32),
    Uuid([u8; 16]),
    TimestampTz(i64),
    Bytea(Vec<u8>),
    Decimal(i128),
    Column(ColumnRef),
    Parameter(u32),
    Compare {
        op: Comparison,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    And(Vec<Expr>),
    Or(Vec<Expr>),
    Not(Box<Expr>),
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    InList {
        expr: Box<Expr>,
        items: Vec<Expr>,
        negated: bool,
    },
}

/// Bound expression tree carried in validated logical and physical plans.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BoundExpr {
    Null,
    Boolean(bool),
    Integer(i64),
    Text(String),
    Date(i32),
    Uuid([u8; 16]),
    TimestampTz(i64),
    Bytea(Vec<u8>),
    Decimal(i128),
    Column(BoundColumnRef),
    Parameter(u32),
    Compare {
        op: Comparison,
        left: Box<BoundExpr>,
        right: Box<BoundExpr>,
    },
    And(Vec<BoundExpr>),
    Or(Vec<BoundExpr>),
    Not(Box<BoundExpr>),
    IsNull {
        expr: Box<BoundExpr>,
        negated: bool,
    },
    InList {
        expr: Box<BoundExpr>,
        items: Vec<BoundExpr>,
        negated: bool,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExprError {
    Truncated,
    TrailingBytes,
    InvalidTag(u8),
    InvalidIdentifier,
    InvalidValue,
    TooLarge,
    TooManyNodes,
    TooDeep,
    InvalidArity,
}

pub(crate) fn valid_identifier(name: &str) -> bool {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_IDENTIFIER_BYTES {
        return false;
    }
    let first = bytes[0];
    if !(first.is_ascii_alphabetic() || first == b'_') {
        return false;
    }
    bytes[1..]
        .iter()
        .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
}

pub(crate) fn put_identifier(out: &mut Vec<u8>, name: &str) -> Result<(), ExprError> {
    if !valid_identifier(name) {
        return Err(ExprError::InvalidIdentifier);
    }
    out.push(name.len() as u8);
    out.extend_from_slice(name.as_bytes());
    Ok(())
}

pub(crate) fn encode_scalar_type(scalar_type: ScalarType) -> (u8, [u8; 4]) {
    match scalar_type {
        ScalarType::Boolean => (1, [0; 4]),
        ScalarType::SmallInt => (2, [0; 4]),
        ScalarType::Integer => (3, [0; 4]),
        ScalarType::BigInt => (4, [0; 4]),
        ScalarType::Decimal { precision, scale } => (5, [precision, scale, 0, 0]),
        ScalarType::Text => (6, [0; 4]),
        ScalarType::Varchar { max_chars } => (7, max_chars.to_be_bytes()),
        ScalarType::Bytea => (8, [0; 4]),
        ScalarType::Uuid => (9, [0; 4]),
        ScalarType::Date => (10, [0; 4]),
        ScalarType::TimestampTz => (11, [0; 4]),
    }
}

pub(crate) fn decode_scalar_type(type_code: u8, params: [u8; 4]) -> Result<ScalarType, ExprError> {
    match type_code {
        1 if params == [0; 4] => Ok(ScalarType::Boolean),
        2 if params == [0; 4] => Ok(ScalarType::SmallInt),
        3 if params == [0; 4] => Ok(ScalarType::Integer),
        4 if params == [0; 4] => Ok(ScalarType::BigInt),
        5 if params[2..] == [0, 0] && (1..=38).contains(&params[0]) && params[1] <= params[0] => {
            Ok(ScalarType::Decimal {
                precision: params[0],
                scale: params[1],
            })
        }
        6 if params == [0; 4] => Ok(ScalarType::Text),
        7 => {
            let max_chars = u32::from_be_bytes(params);
            if max_chars == 0 {
                return Err(ExprError::InvalidValue);
            }
            Ok(ScalarType::Varchar { max_chars })
        }
        8 if params == [0; 4] => Ok(ScalarType::Bytea),
        9 if params == [0; 4] => Ok(ScalarType::Uuid),
        10 if params == [0; 4] => Ok(ScalarType::Date),
        11 if params == [0; 4] => Ok(ScalarType::TimestampTz),
        _ => Err(ExprError::InvalidValue),
    }
}

pub(crate) fn put_bound_column(out: &mut Vec<u8>, col: &BoundColumn) {
    out.extend_from_slice(&col.column_id);
    out.extend_from_slice(&col.ordinal.to_be_bytes());
    let (code, params) = encode_scalar_type(col.scalar_type);
    out.push(code);
    out.extend_from_slice(&params);
    out.push(u8::from(col.nullable));
}

pub(crate) struct WireReader<'a> {
    pub bytes: &'a [u8],
    pub offset: usize,
}

impl<'a> WireReader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    pub fn take(&mut self, len: usize) -> Result<&'a [u8], ExprError> {
        let end = self.offset.checked_add(len).ok_or(ExprError::Truncated)?;
        if end > self.bytes.len() {
            return Err(ExprError::Truncated);
        }
        let slice = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(slice)
    }

    pub fn u8(&mut self) -> Result<u8, ExprError> {
        Ok(self.take(1)?[0])
    }

    pub fn u16(&mut self) -> Result<u16, ExprError> {
        let b: [u8; 2] = self.take(2)?.try_into().map_err(|_| ExprError::Truncated)?;
        Ok(u16::from_be_bytes(b))
    }

    pub fn u32(&mut self) -> Result<u32, ExprError> {
        let b: [u8; 4] = self.take(4)?.try_into().map_err(|_| ExprError::Truncated)?;
        Ok(u32::from_be_bytes(b))
    }

    pub fn u64(&mut self) -> Result<u64, ExprError> {
        let b: [u8; 8] = self.take(8)?.try_into().map_err(|_| ExprError::Truncated)?;
        Ok(u64::from_be_bytes(b))
    }

    pub fn i32(&mut self) -> Result<i32, ExprError> {
        let b: [u8; 4] = self.take(4)?.try_into().map_err(|_| ExprError::Truncated)?;
        Ok(i32::from_be_bytes(b))
    }

    pub fn i64(&mut self) -> Result<i64, ExprError> {
        let b: [u8; 8] = self.take(8)?.try_into().map_err(|_| ExprError::Truncated)?;
        Ok(i64::from_be_bytes(b))
    }

    pub fn i128(&mut self) -> Result<i128, ExprError> {
        let b: [u8; 16] = self
            .take(16)?
            .try_into()
            .map_err(|_| ExprError::Truncated)?;
        Ok(i128::from_be_bytes(b))
    }

    pub fn identifier(&mut self) -> Result<String, ExprError> {
        let len = usize::from(self.u8()?);
        let raw = self.take(len)?;
        let s = std::str::from_utf8(raw).map_err(|_| ExprError::InvalidIdentifier)?;
        if !valid_identifier(s) {
            return Err(ExprError::InvalidIdentifier);
        }
        Ok(s.to_owned())
    }

    pub fn bound_column(&mut self) -> Result<BoundColumn, ExprError> {
        let column_id: [u8; 16] = self
            .take(16)?
            .try_into()
            .map_err(|_| ExprError::Truncated)?;
        if column_id == [0; 16] {
            return Err(ExprError::InvalidValue);
        }
        let ordinal = self.u32()?;
        let code = self.u8()?;
        let params: [u8; 4] = self.take(4)?.try_into().map_err(|_| ExprError::Truncated)?;
        let scalar_type = decode_scalar_type(code, params)?;
        let nullable = match self.u8()? {
            0 => false,
            1 => true,
            _ => return Err(ExprError::InvalidValue),
        };
        Ok(BoundColumn {
            column_id,
            ordinal,
            scalar_type,
            nullable,
        })
    }
}

pub fn encode_expr(expr: &Expr) -> Result<Vec<u8>, ExprError> {
    let mut out = Vec::with_capacity(64);
    let mut nodes = 0usize;
    encode_expr_inner(expr, &mut out, &mut nodes, 1)?;
    Ok(out)
}

pub(crate) fn encode_expr_inner(
    expr: &Expr,
    out: &mut Vec<u8>,
    nodes: &mut usize,
    depth: usize,
) -> Result<(), ExprError> {
    if depth > MAX_EXPR_DEPTH {
        return Err(ExprError::TooDeep);
    }
    *nodes += 1;
    if *nodes > MAX_EXPR_NODES {
        return Err(ExprError::TooManyNodes);
    }
    match expr {
        Expr::Null => out.push(0x00),
        Expr::Boolean(v) => {
            out.push(0x01);
            out.push(u8::from(*v));
        }
        Expr::Integer(v) => {
            out.push(0x02);
            out.extend_from_slice(&v.to_be_bytes());
        }
        Expr::Text(s) => {
            if s.len() > MAX_TEXT_LITERAL_BYTES {
                return Err(ExprError::TooLarge);
            }
            out.push(0x03);
            out.extend_from_slice(&(s.len() as u16).to_be_bytes());
            out.extend_from_slice(s.as_bytes());
        }
        Expr::Date(d) => {
            if !crate::tuple::date_day_count_is_valid(*d) {
                return Err(ExprError::InvalidValue);
            }
            out.push(0x04);
            out.extend_from_slice(&d.to_be_bytes());
        }
        Expr::Uuid(u) => {
            out.push(0x05);
            out.extend_from_slice(u);
        }
        Expr::TimestampTz(t) => {
            if !crate::tuple::timestamp_micros_is_valid(*t) {
                return Err(ExprError::InvalidValue);
            }
            out.push(0x06);
            out.extend_from_slice(&t.to_be_bytes());
        }
        Expr::Bytea(b) => {
            if b.len() > MAX_TEXT_LITERAL_BYTES {
                return Err(ExprError::TooLarge);
            }
            out.push(0x07);
            out.extend_from_slice(&(b.len() as u16).to_be_bytes());
            out.extend_from_slice(b);
        }
        Expr::Decimal(v) => {
            out.push(0x08);
            out.extend_from_slice(&v.to_be_bytes());
        }
        Expr::Column(col) => {
            out.push(0x10);
            out.extend_from_slice(&col.relation_index.to_be_bytes());
            put_identifier(out, &col.column_name)?;
        }
        Expr::Parameter(idx) => {
            if *idx == 0 || *idx > MAX_PARAMETER_INDEX {
                return Err(ExprError::InvalidValue);
            }
            out.push(0x11);
            out.extend_from_slice(&idx.to_be_bytes());
        }
        Expr::Compare { op, left, right } => {
            out.push(0x20);
            out.push(op.to_byte());
            encode_expr_inner(left, out, nodes, depth + 1)?;
            encode_expr_inner(right, out, nodes, depth + 1)?;
        }
        Expr::And(children) | Expr::Or(children) => {
            if !(2..=16).contains(&children.len()) {
                return Err(ExprError::InvalidArity);
            }
            out.push(if matches!(expr, Expr::And(_)) {
                0x30
            } else {
                0x31
            });
            out.push(children.len() as u8);
            for child in children {
                encode_expr_inner(child, out, nodes, depth + 1)?;
            }
        }
        Expr::Not(inner) => {
            out.push(0x32);
            encode_expr_inner(inner, out, nodes, depth + 1)?;
        }
        Expr::IsNull {
            expr: inner,
            negated,
        } => {
            out.push(if *negated { 0x41 } else { 0x40 });
            encode_expr_inner(inner, out, nodes, depth + 1)?;
        }
        Expr::InList {
            expr: target,
            items,
            negated,
        } => {
            if !(1..=MAX_IN_LIST_ITEMS).contains(&items.len()) {
                return Err(ExprError::InvalidArity);
            }
            out.push(0x42);
            out.push(u8::from(*negated));
            out.push(items.len() as u8);
            encode_expr_inner(target, out, nodes, depth + 1)?;
            for item in items {
                encode_expr_inner(item, out, nodes, depth + 1)?;
            }
        }
    }
    if out.len() > MAX_EXPR_BYTES {
        return Err(ExprError::TooLarge);
    }
    Ok(())
}

pub fn decode_expr(bytes: &[u8]) -> Result<Expr, ExprError> {
    if bytes.len() > MAX_EXPR_BYTES {
        return Err(ExprError::TooLarge);
    }
    let mut reader = WireReader::new(bytes);
    let mut nodes = 0usize;
    let expr = decode_expr_inner(&mut reader, &mut nodes, 1)?;
    if reader.offset != bytes.len() {
        return Err(ExprError::TrailingBytes);
    }
    Ok(expr)
}

pub(crate) fn decode_expr_inner(
    reader: &mut WireReader<'_>,
    nodes: &mut usize,
    depth: usize,
) -> Result<Expr, ExprError> {
    if depth > MAX_EXPR_DEPTH {
        return Err(ExprError::TooDeep);
    }
    *nodes += 1;
    if *nodes > MAX_EXPR_NODES {
        return Err(ExprError::TooManyNodes);
    }
    let tag = reader.u8()?;
    match tag {
        0x00 => Ok(Expr::Null),
        0x01 => match reader.u8()? {
            0 => Ok(Expr::Boolean(false)),
            1 => Ok(Expr::Boolean(true)),
            _ => Err(ExprError::InvalidValue),
        },
        0x02 => Ok(Expr::Integer(reader.i64()?)),
        0x03 => {
            let len = usize::from(reader.u16()?);
            if len > MAX_TEXT_LITERAL_BYTES {
                return Err(ExprError::TooLarge);
            }
            let raw = reader.take(len)?;
            let s = std::str::from_utf8(raw).map_err(|_| ExprError::InvalidValue)?;
            Ok(Expr::Text(s.to_owned()))
        }
        0x04 => {
            let d = reader.i32()?;
            if !crate::tuple::date_day_count_is_valid(d) {
                return Err(ExprError::InvalidValue);
            }
            Ok(Expr::Date(d))
        }
        0x05 => {
            let u: [u8; 16] = reader
                .take(16)?
                .try_into()
                .map_err(|_| ExprError::Truncated)?;
            Ok(Expr::Uuid(u))
        }
        0x06 => {
            let t = reader.i64()?;
            if !crate::tuple::timestamp_micros_is_valid(t) {
                return Err(ExprError::InvalidValue);
            }
            Ok(Expr::TimestampTz(t))
        }
        0x07 => {
            let len = usize::from(reader.u16()?);
            if len > MAX_TEXT_LITERAL_BYTES {
                return Err(ExprError::TooLarge);
            }
            Ok(Expr::Bytea(reader.take(len)?.to_vec()))
        }
        0x08 => Ok(Expr::Decimal(reader.i128()?)),
        0x10 => {
            let relation_index = reader.u16()?;
            let column_name = reader.identifier()?;
            Ok(Expr::Column(ColumnRef {
                relation_index,
                column_name,
            }))
        }
        0x11 => {
            let idx = reader.u32()?;
            if idx == 0 || idx > MAX_PARAMETER_INDEX {
                return Err(ExprError::InvalidValue);
            }
            Ok(Expr::Parameter(idx))
        }
        0x20 => {
            let op = Comparison::from_byte(reader.u8()?)?;
            let left = Box::new(decode_expr_inner(reader, nodes, depth + 1)?);
            let right = Box::new(decode_expr_inner(reader, nodes, depth + 1)?);
            Ok(Expr::Compare { op, left, right })
        }
        0x30 | 0x31 => {
            let count = usize::from(reader.u8()?);
            if !(2..=16).contains(&count) {
                return Err(ExprError::InvalidArity);
            }
            let mut children = Vec::with_capacity(count);
            for _ in 0..count {
                children.push(decode_expr_inner(reader, nodes, depth + 1)?);
            }
            Ok(if tag == 0x30 {
                Expr::And(children)
            } else {
                Expr::Or(children)
            })
        }
        0x32 => Ok(Expr::Not(Box::new(decode_expr_inner(
            reader,
            nodes,
            depth + 1,
        )?))),
        0x40 | 0x41 => Ok(Expr::IsNull {
            expr: Box::new(decode_expr_inner(reader, nodes, depth + 1)?),
            negated: tag == 0x41,
        }),
        0x42 => {
            let negated = match reader.u8()? {
                0 => false,
                1 => true,
                _ => return Err(ExprError::InvalidValue),
            };
            let count = usize::from(reader.u8()?);
            if !(1..=MAX_IN_LIST_ITEMS).contains(&count) {
                return Err(ExprError::InvalidArity);
            }
            let target = Box::new(decode_expr_inner(reader, nodes, depth + 1)?);
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                items.push(decode_expr_inner(reader, nodes, depth + 1)?);
            }
            Ok(Expr::InList {
                expr: target,
                items,
                negated,
            })
        }
        _ => Err(ExprError::InvalidTag(tag)),
    }
}

pub fn encode_bound_expr(expr: &BoundExpr) -> Result<Vec<u8>, ExprError> {
    let mut out = Vec::with_capacity(64);
    let mut nodes = 0usize;
    encode_bound_expr_inner(expr, &mut out, &mut nodes, 1)?;
    Ok(out)
}

pub(crate) fn encode_bound_expr_inner(
    expr: &BoundExpr,
    out: &mut Vec<u8>,
    nodes: &mut usize,
    depth: usize,
) -> Result<(), ExprError> {
    if depth > MAX_EXPR_DEPTH {
        return Err(ExprError::TooDeep);
    }
    *nodes += 1;
    if *nodes > MAX_EXPR_NODES {
        return Err(ExprError::TooManyNodes);
    }
    match expr {
        BoundExpr::Null => out.push(0x00),
        BoundExpr::Boolean(v) => {
            out.push(0x01);
            out.push(u8::from(*v));
        }
        BoundExpr::Integer(v) => {
            out.push(0x02);
            out.extend_from_slice(&v.to_be_bytes());
        }
        BoundExpr::Text(s) => {
            if s.len() > MAX_TEXT_LITERAL_BYTES {
                return Err(ExprError::TooLarge);
            }
            out.push(0x03);
            out.extend_from_slice(&(s.len() as u16).to_be_bytes());
            out.extend_from_slice(s.as_bytes());
        }
        BoundExpr::Date(d) => {
            if !crate::tuple::date_day_count_is_valid(*d) {
                return Err(ExprError::InvalidValue);
            }
            out.push(0x04);
            out.extend_from_slice(&d.to_be_bytes());
        }
        BoundExpr::Uuid(u) => {
            out.push(0x05);
            out.extend_from_slice(u);
        }
        BoundExpr::TimestampTz(t) => {
            if !crate::tuple::timestamp_micros_is_valid(*t) {
                return Err(ExprError::InvalidValue);
            }
            out.push(0x06);
            out.extend_from_slice(&t.to_be_bytes());
        }
        BoundExpr::Bytea(b) => {
            if b.len() > MAX_TEXT_LITERAL_BYTES {
                return Err(ExprError::TooLarge);
            }
            out.push(0x07);
            out.extend_from_slice(&(b.len() as u16).to_be_bytes());
            out.extend_from_slice(b);
        }
        BoundExpr::Decimal(v) => {
            out.push(0x08);
            out.extend_from_slice(&v.to_be_bytes());
        }
        BoundExpr::Column(col) => {
            out.push(0x10);
            out.extend_from_slice(&col.relation_index.to_be_bytes());
            put_bound_column(out, &col.column);
        }
        BoundExpr::Parameter(idx) => {
            if *idx == 0 || *idx > MAX_PARAMETER_INDEX {
                return Err(ExprError::InvalidValue);
            }
            out.push(0x11);
            out.extend_from_slice(&idx.to_be_bytes());
        }
        BoundExpr::Compare { op, left, right } => {
            out.push(0x20);
            out.push(op.to_byte());
            encode_bound_expr_inner(left, out, nodes, depth + 1)?;
            encode_bound_expr_inner(right, out, nodes, depth + 1)?;
        }
        BoundExpr::And(children) | BoundExpr::Or(children) => {
            if !(2..=16).contains(&children.len()) {
                return Err(ExprError::InvalidArity);
            }
            out.push(if matches!(expr, BoundExpr::And(_)) {
                0x30
            } else {
                0x31
            });
            out.push(children.len() as u8);
            for child in children {
                encode_bound_expr_inner(child, out, nodes, depth + 1)?;
            }
        }
        BoundExpr::Not(inner) => {
            out.push(0x32);
            encode_bound_expr_inner(inner, out, nodes, depth + 1)?;
        }
        BoundExpr::IsNull {
            expr: inner,
            negated,
        } => {
            out.push(if *negated { 0x41 } else { 0x40 });
            encode_bound_expr_inner(inner, out, nodes, depth + 1)?;
        }
        BoundExpr::InList {
            expr: target,
            items,
            negated,
        } => {
            if !(1..=MAX_IN_LIST_ITEMS).contains(&items.len()) {
                return Err(ExprError::InvalidArity);
            }
            out.push(0x42);
            out.push(u8::from(*negated));
            out.push(items.len() as u8);
            encode_bound_expr_inner(target, out, nodes, depth + 1)?;
            for item in items {
                encode_bound_expr_inner(item, out, nodes, depth + 1)?;
            }
        }
    }
    if out.len() > MAX_EXPR_BYTES {
        return Err(ExprError::TooLarge);
    }
    Ok(())
}

pub fn decode_bound_expr(bytes: &[u8]) -> Result<BoundExpr, ExprError> {
    if bytes.len() > MAX_EXPR_BYTES {
        return Err(ExprError::TooLarge);
    }
    let mut reader = WireReader::new(bytes);
    let mut nodes = 0usize;
    let expr = decode_bound_expr_inner(&mut reader, &mut nodes, 1)?;
    if reader.offset != bytes.len() {
        return Err(ExprError::TrailingBytes);
    }
    Ok(expr)
}

pub(crate) fn decode_bound_expr_inner(
    reader: &mut WireReader<'_>,
    nodes: &mut usize,
    depth: usize,
) -> Result<BoundExpr, ExprError> {
    if depth > MAX_EXPR_DEPTH {
        return Err(ExprError::TooDeep);
    }
    *nodes += 1;
    if *nodes > MAX_EXPR_NODES {
        return Err(ExprError::TooManyNodes);
    }
    let tag = reader.u8()?;
    match tag {
        0x00 => Ok(BoundExpr::Null),
        0x01 => match reader.u8()? {
            0 => Ok(BoundExpr::Boolean(false)),
            1 => Ok(BoundExpr::Boolean(true)),
            _ => Err(ExprError::InvalidValue),
        },
        0x02 => Ok(BoundExpr::Integer(reader.i64()?)),
        0x03 => {
            let len = usize::from(reader.u16()?);
            if len > MAX_TEXT_LITERAL_BYTES {
                return Err(ExprError::TooLarge);
            }
            let raw = reader.take(len)?;
            let s = std::str::from_utf8(raw).map_err(|_| ExprError::InvalidValue)?;
            Ok(BoundExpr::Text(s.to_owned()))
        }
        0x04 => {
            let d = reader.i32()?;
            if !crate::tuple::date_day_count_is_valid(d) {
                return Err(ExprError::InvalidValue);
            }
            Ok(BoundExpr::Date(d))
        }
        0x05 => {
            let u: [u8; 16] = reader
                .take(16)?
                .try_into()
                .map_err(|_| ExprError::Truncated)?;
            Ok(BoundExpr::Uuid(u))
        }
        0x06 => {
            let t = reader.i64()?;
            if !crate::tuple::timestamp_micros_is_valid(t) {
                return Err(ExprError::InvalidValue);
            }
            Ok(BoundExpr::TimestampTz(t))
        }
        0x07 => {
            let len = usize::from(reader.u16()?);
            if len > MAX_TEXT_LITERAL_BYTES {
                return Err(ExprError::TooLarge);
            }
            Ok(BoundExpr::Bytea(reader.take(len)?.to_vec()))
        }
        0x08 => Ok(BoundExpr::Decimal(reader.i128()?)),
        0x10 => {
            let relation_index = reader.u16()?;
            let column = reader.bound_column()?;
            Ok(BoundExpr::Column(BoundColumnRef {
                relation_index,
                column,
            }))
        }
        0x11 => {
            let idx = reader.u32()?;
            if idx == 0 || idx > MAX_PARAMETER_INDEX {
                return Err(ExprError::InvalidValue);
            }
            Ok(BoundExpr::Parameter(idx))
        }
        0x20 => {
            let op = Comparison::from_byte(reader.u8()?)?;
            let left = Box::new(decode_bound_expr_inner(reader, nodes, depth + 1)?);
            let right = Box::new(decode_bound_expr_inner(reader, nodes, depth + 1)?);
            Ok(BoundExpr::Compare { op, left, right })
        }
        0x30 | 0x31 => {
            let count = usize::from(reader.u8()?);
            if !(2..=16).contains(&count) {
                return Err(ExprError::InvalidArity);
            }
            let mut children = Vec::with_capacity(count);
            for _ in 0..count {
                children.push(decode_bound_expr_inner(reader, nodes, depth + 1)?);
            }
            Ok(if tag == 0x30 {
                BoundExpr::And(children)
            } else {
                BoundExpr::Or(children)
            })
        }
        0x32 => Ok(BoundExpr::Not(Box::new(decode_bound_expr_inner(
            reader,
            nodes,
            depth + 1,
        )?))),
        0x40 | 0x41 => Ok(BoundExpr::IsNull {
            expr: Box::new(decode_bound_expr_inner(reader, nodes, depth + 1)?),
            negated: tag == 0x41,
        }),
        0x42 => {
            let negated = match reader.u8()? {
                0 => false,
                1 => true,
                _ => return Err(ExprError::InvalidValue),
            };
            let count = usize::from(reader.u8()?);
            if !(1..=MAX_IN_LIST_ITEMS).contains(&count) {
                return Err(ExprError::InvalidArity);
            }
            let target = Box::new(decode_bound_expr_inner(reader, nodes, depth + 1)?);
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                items.push(decode_bound_expr_inner(reader, nodes, depth + 1)?);
            }
            Ok(BoundExpr::InList {
                expr: target,
                items,
                negated,
            })
        }
        _ => Err(ExprError::InvalidTag(tag)),
    }
}

/// Decode a `BoundExpr` and verify every `BoundExpr::Column` appears in `columns`.
pub fn decode_bound_expr_for_columns(
    bytes: &[u8],
    columns: &[BoundColumn],
) -> Result<BoundExpr, ExprError> {
    let expr = decode_bound_expr(bytes)?;
    fn check_cols(e: &BoundExpr, cols: &[BoundColumn]) -> Result<(), ExprError> {
        match e {
            BoundExpr::Column(c) => {
                if !cols.contains(&c.column) {
                    return Err(ExprError::InvalidValue);
                }
                Ok(())
            }
            BoundExpr::Compare { left, right, .. } => {
                check_cols(left, cols)?;
                check_cols(right, cols)
            }
            BoundExpr::And(ch) | BoundExpr::Or(ch) => {
                for c in ch {
                    check_cols(c, cols)?;
                }
                Ok(())
            }
            BoundExpr::Not(inner) | BoundExpr::IsNull { expr: inner, .. } => {
                check_cols(inner, cols)
            }
            BoundExpr::InList { expr, items, .. } => {
                check_cols(expr, cols)?;
                for item in items {
                    check_cols(item, cols)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
    check_cols(&expr, columns)?;
    Ok(expr)
}

pub type ExprRangeBounds = (
    BoundColumn,
    Option<(BoundExpr, bool)>,
    Option<(BoundExpr, bool)>,
);

/// Extract `(key_column, lower_bound, upper_bound)` where each bound is
/// `Option<(BoundExpr, bool /* inclusive */)>` from a single comparison or
/// 2-term `AND` range predicate.
pub fn index_range_bounds_from_expr(expr: &BoundExpr) -> Option<ExprRangeBounds> {
    fn is_literal_for_col(col: &BoundColumn, val: &BoundExpr) -> bool {
        matches!(val, BoundExpr::Parameter(_))
            || matches!(
                (col.scalar_type, val),
                (ScalarType::Boolean, BoundExpr::Boolean(_))
                    | (ScalarType::Text, BoundExpr::Text(_))
                    | (ScalarType::Date, BoundExpr::Date(_))
                    | (ScalarType::Uuid, BoundExpr::Uuid(_))
                    | (ScalarType::TimestampTz, BoundExpr::TimestampTz(_))
                    | (ScalarType::Bytea, BoundExpr::Bytea(_))
            )
            || matches!(
                (col.scalar_type, val),
                (ScalarType::Integer, BoundExpr::Integer(v)) if i32::try_from(*v).is_ok()
            )
            || matches!(
                (col.scalar_type, val),
                (ScalarType::Decimal { precision, .. }, BoundExpr::Decimal(v))
                    if crate::tuple::decimal_coefficient_is_valid(*v, precision)
            )
    }

    fn single_cmp(e: &BoundExpr) -> Option<ExprRangeBounds> {
        let BoundExpr::Compare { op, left, right } = e else {
            return None;
        };
        let (col, val, norm_op) = match (left.as_ref(), right.as_ref()) {
            (BoundExpr::Column(c), v) if is_literal_for_col(&c.column, v) => {
                (c.column.clone(), v.clone(), *op)
            }
            (v, BoundExpr::Column(c)) if is_literal_for_col(&c.column, v) => {
                let rev = match op {
                    Comparison::Equal => Comparison::Equal,
                    Comparison::Less => Comparison::Greater,
                    Comparison::LessOrEqual => Comparison::GreaterOrEqual,
                    Comparison::Greater => Comparison::Less,
                    Comparison::GreaterOrEqual => Comparison::LessOrEqual,
                    Comparison::NotEqual => return None,
                };
                (c.column.clone(), v.clone(), rev)
            }
            _ => return None,
        };
        match norm_op {
            Comparison::Equal => Some((col, Some((val.clone(), true)), Some((val, true)))),
            Comparison::Less => Some((col, None, Some((val, false)))),
            Comparison::LessOrEqual => Some((col, None, Some((val, true)))),
            Comparison::Greater => Some((col, Some((val, false)), None)),
            Comparison::GreaterOrEqual => Some((col, Some((val, true)), None)),
            Comparison::NotEqual => None,
        }
    }

    match expr {
        BoundExpr::Compare { .. } => single_cmp(expr),
        BoundExpr::And(children) if children.len() == 2 => {
            let (lc, ll, lu) = single_cmp(&children[0])?;
            let (rc, rl, ru) = single_cmp(&children[1])?;
            if lc != rc || ll.is_some() == rl.is_some() || lu.is_some() == ru.is_some() {
                return None;
            }
            Some((lc, ll.or(rl), lu.or(ru)))
        }
        _ => None,
    }
}
