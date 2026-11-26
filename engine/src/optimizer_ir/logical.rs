//! Unified composable logical request IR and binary codec.

use super::expr::{
    ColumnRef, Expr, ExprError, MAX_EXPR_DEPTH, MAX_PARAMETER_INDEX, WireReader, decode_expr_inner,
    encode_expr_inner, put_identifier,
};
use crate::query_abi::{
    OPTIMIZER_LOGICAL_OP_AGGREGATE, OPTIMIZER_LOGICAL_OP_DELETE, OPTIMIZER_LOGICAL_OP_FILTER,
    OPTIMIZER_LOGICAL_OP_JOIN, OPTIMIZER_LOGICAL_OP_LIMIT, OPTIMIZER_LOGICAL_OP_PROJECT,
    OPTIMIZER_LOGICAL_OP_SCAN, OPTIMIZER_LOGICAL_OP_SORT, OPTIMIZER_LOGICAL_OP_UPDATE,
    OPTIMIZER_REQUEST_VERSION, OPTIMIZER_UNIFIED_REQUEST_KIND,
};

pub const MAX_UNIFIED_REQUEST_BYTES: usize = 64 * 1024;
pub const MAX_LOGICAL_NODES: usize = 64;
pub const MAX_PROJECTIONS: usize = 128;
pub const MAX_JOIN_KEYS: usize = 8;
pub const MAX_GROUP_KEYS: usize = 8;
pub const MAX_AGGREGATES: usize = 16;
pub const MAX_ORDER_ITEMS: usize = 16;
pub const MAX_UPDATE_ASSIGNMENTS: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Semi,
    Anti,
}

impl JoinType {
    pub fn to_byte(self) -> u8 {
        match self {
            Self::Inner => 0,
            Self::Left => 1,
            Self::Semi => 2,
            Self::Anti => 3,
        }
    }

    pub fn from_byte(b: u8) -> Result<Self, RequestError> {
        match b {
            0 => Ok(Self::Inner),
            1 => Ok(Self::Left),
            2 => Ok(Self::Semi),
            3 => Ok(Self::Anti),
            _ => Err(RequestError::InvalidEncoding),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregateKind {
    CountStar,
    Min,
    Max,
    Sum,
    Avg,
    CountCol,
}

impl AggregateKind {
    pub fn to_byte(self) -> u8 {
        match self {
            Self::CountStar => 1,
            Self::Min => 2,
            Self::Max => 3,
            Self::Sum => 4,
            Self::Avg => 5,
            Self::CountCol => 6,
        }
    }

    pub fn from_byte(b: u8) -> Result<Self, RequestError> {
        match b {
            1 => Ok(Self::CountStar),
            2 => Ok(Self::Min),
            3 => Ok(Self::Max),
            4 => Ok(Self::Sum),
            5 => Ok(Self::Avg),
            6 => Ok(Self::CountCol),
            _ => Err(RequestError::InvalidEncoding),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AggregateSpec {
    pub kind: AggregateKind,
    pub arg: Option<ColumnRef>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OrderTarget {
    Column(ColumnRef),
    AggregateIndex(u16),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderItem {
    pub target: OrderTarget,
    pub descending: bool,
    pub nulls_first: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UpdateValue {
    Parameter(u32),
    Null,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateAssignment {
    pub column_name: String,
    pub value: UpdateValue,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalOp {
    Scan {
        relation_index: u16,
        relation_name: String,
    },
    Filter {
        input: Box<LogicalOp>,
        predicate: Expr,
    },
    Project {
        input: Box<LogicalOp>,
        columns: Vec<ColumnRef>,
    },
    Join {
        join_type: JoinType,
        left: Box<LogicalOp>,
        right: Box<LogicalOp>,
        eq_keys: Vec<(ColumnRef, ColumnRef)>,
        residual: Option<Expr>,
    },
    Aggregate {
        input: Box<LogicalOp>,
        group_keys: Vec<ColumnRef>,
        aggregates: Vec<AggregateSpec>,
        having: Option<Expr>,
    },
    Sort {
        input: Box<LogicalOp>,
        order_by: Vec<OrderItem>,
    },
    Limit {
        input: Box<LogicalOp>,
        count: u32,
    },
    Delete {
        relation_index: u16,
        relation_name: String,
        predicate: Expr,
    },
    Update {
        relation_index: u16,
        relation_name: String,
        assignments: Vec<UpdateAssignment>,
        predicate: Expr,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalRequest {
    pub schema_name: String,
    pub root: LogicalOp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RequestError {
    Truncated,
    TrailingBytes,
    UnsupportedVersion(u16),
    InvalidEncoding,
    TooLarge,
    TooManyNodes,
    TooDeep,
    Expr(ExprError),
}

impl From<ExprError> for RequestError {
    fn from(err: ExprError) -> Self {
        match err {
            ExprError::Truncated => Self::Truncated,
            ExprError::TrailingBytes => Self::TrailingBytes,
            ExprError::TooLarge => Self::TooLarge,
            ExprError::TooManyNodes => Self::TooManyNodes,
            ExprError::TooDeep => Self::TooDeep,
            other => Self::Expr(other),
        }
    }
}

/// Collect all `(schema_name, relation_name)` pairs referenced by leaf `Scan`,
/// `Delete`, or `Update` nodes in deterministic pre-order traversal.
pub fn collect_scan_relations(req: &LogicalRequest) -> Vec<(String, String)> {
    fn walk(op: &LogicalOp, schema: &str, out: &mut Vec<(String, String)>) {
        match op {
            LogicalOp::Scan { relation_name, .. }
            | LogicalOp::Delete { relation_name, .. }
            | LogicalOp::Update { relation_name, .. } => {
                out.push((schema.to_owned(), relation_name.clone()));
            }
            LogicalOp::Filter { input, .. }
            | LogicalOp::Project { input, .. }
            | LogicalOp::Aggregate { input, .. }
            | LogicalOp::Sort { input, .. }
            | LogicalOp::Limit { input, .. } => walk(input, schema, out),
            LogicalOp::Join { left, right, .. } => {
                walk(left, schema, out);
                walk(right, schema, out);
            }
        }
    }
    let mut out = Vec::new();
    walk(&req.root, &req.schema_name, &mut out);
    out
}

fn put_col_ref(out: &mut Vec<u8>, col: &ColumnRef) -> Result<(), RequestError> {
    out.extend_from_slice(&col.relation_index.to_be_bytes());
    put_identifier(out, &col.column_name)?;
    Ok(())
}

fn read_col_ref(reader: &mut WireReader<'_>) -> Result<ColumnRef, RequestError> {
    let relation_index = reader.u16()?;
    let column_name = reader.identifier()?;
    Ok(ColumnRef {
        relation_index,
        column_name,
    })
}

pub fn encode_logical_request(req: &LogicalRequest) -> Result<Vec<u8>, RequestError> {
    let mut out = Vec::with_capacity(256);
    out.extend_from_slice(&OPTIMIZER_REQUEST_VERSION.to_be_bytes());
    out.push(OPTIMIZER_UNIFIED_REQUEST_KIND);
    out.push(0); // flags
    put_identifier(&mut out, &req.schema_name)?;
    let mut nodes = 0usize;
    encode_op(&req.root, &mut out, &mut nodes, 1)?;
    if out.len() > MAX_UNIFIED_REQUEST_BYTES {
        return Err(RequestError::TooLarge);
    }
    Ok(out)
}

fn encode_op(
    op: &LogicalOp,
    out: &mut Vec<u8>,
    nodes: &mut usize,
    depth: usize,
) -> Result<(), RequestError> {
    if depth > MAX_EXPR_DEPTH {
        return Err(RequestError::TooDeep);
    }
    *nodes += 1;
    if *nodes > MAX_LOGICAL_NODES {
        return Err(RequestError::TooManyNodes);
    }
    match op {
        LogicalOp::Scan {
            relation_index,
            relation_name,
        } => {
            out.push(OPTIMIZER_LOGICAL_OP_SCAN);
            out.extend_from_slice(&relation_index.to_be_bytes());
            put_identifier(out, relation_name)?;
        }
        LogicalOp::Filter { input, predicate } => {
            out.push(OPTIMIZER_LOGICAL_OP_FILTER);
            let mut expr_nodes = 0usize;
            encode_expr_inner(predicate, out, &mut expr_nodes, 1)?;
            encode_op(input, out, nodes, depth + 1)?;
        }
        LogicalOp::Project { input, columns } => {
            if columns.is_empty() || columns.len() > MAX_PROJECTIONS {
                return Err(RequestError::InvalidEncoding);
            }
            out.push(OPTIMIZER_LOGICAL_OP_PROJECT);
            out.extend_from_slice(&(columns.len() as u16).to_be_bytes());
            for col in columns {
                put_col_ref(out, col)?;
            }
            encode_op(input, out, nodes, depth + 1)?;
        }
        LogicalOp::Join {
            join_type,
            left,
            right,
            eq_keys,
            residual,
        } => {
            if eq_keys.is_empty() || eq_keys.len() > MAX_JOIN_KEYS {
                return Err(RequestError::InvalidEncoding);
            }
            out.push(OPTIMIZER_LOGICAL_OP_JOIN);
            out.push(join_type.to_byte());
            out.push(eq_keys.len() as u8);
            for (l, r) in eq_keys {
                put_col_ref(out, l)?;
                put_col_ref(out, r)?;
            }
            match residual {
                None => out.push(0),
                Some(expr) => {
                    out.push(1);
                    let mut expr_nodes = 0usize;
                    encode_expr_inner(expr, out, &mut expr_nodes, 1)?;
                }
            }
            encode_op(left, out, nodes, depth + 1)?;
            encode_op(right, out, nodes, depth + 1)?;
        }
        LogicalOp::Aggregate {
            input,
            group_keys,
            aggregates,
            having,
        } => {
            if group_keys.len() > MAX_GROUP_KEYS
                || aggregates.len() > MAX_AGGREGATES
                || (group_keys.is_empty() && aggregates.is_empty())
            {
                return Err(RequestError::InvalidEncoding);
            }
            out.push(OPTIMIZER_LOGICAL_OP_AGGREGATE);
            out.push(group_keys.len() as u8);
            for key in group_keys {
                put_col_ref(out, key)?;
            }
            out.push(aggregates.len() as u8);
            for agg in aggregates {
                out.push(agg.kind.to_byte());
                match (&agg.kind, &agg.arg) {
                    (AggregateKind::CountStar, None) => out.push(0),
                    (AggregateKind::CountStar, Some(_)) => {
                        return Err(RequestError::InvalidEncoding);
                    }
                    (_, Some(col)) => {
                        out.push(1);
                        put_col_ref(out, col)?;
                    }
                    (_, None) => return Err(RequestError::InvalidEncoding),
                }
            }
            match having {
                None => out.push(0),
                Some(expr) => {
                    out.push(1);
                    let mut expr_nodes = 0usize;
                    encode_expr_inner(expr, out, &mut expr_nodes, 1)?;
                }
            }
            encode_op(input, out, nodes, depth + 1)?;
        }
        LogicalOp::Sort { input, order_by } => {
            if order_by.is_empty() || order_by.len() > MAX_ORDER_ITEMS {
                return Err(RequestError::InvalidEncoding);
            }
            out.push(OPTIMIZER_LOGICAL_OP_SORT);
            out.push(order_by.len() as u8);
            for item in order_by {
                match &item.target {
                    OrderTarget::Column(col) => {
                        out.push(0);
                        put_col_ref(out, col)?;
                    }
                    OrderTarget::AggregateIndex(idx) => {
                        out.push(1);
                        out.extend_from_slice(&idx.to_be_bytes());
                    }
                }
                out.push(u8::from(item.descending));
                out.push(u8::from(item.nulls_first));
            }
            encode_op(input, out, nodes, depth + 1)?;
        }
        LogicalOp::Limit { input, count } => {
            out.push(OPTIMIZER_LOGICAL_OP_LIMIT);
            out.extend_from_slice(&count.to_be_bytes());
            encode_op(input, out, nodes, depth + 1)?;
        }
        LogicalOp::Delete {
            relation_index,
            relation_name,
            predicate,
        } => {
            out.push(OPTIMIZER_LOGICAL_OP_DELETE);
            out.extend_from_slice(&relation_index.to_be_bytes());
            put_identifier(out, relation_name)?;
            let mut expr_nodes = 0usize;
            encode_expr_inner(predicate, out, &mut expr_nodes, 1)?;
        }
        LogicalOp::Update {
            relation_index,
            relation_name,
            assignments,
            predicate,
        } => {
            if assignments.is_empty() || assignments.len() > MAX_UPDATE_ASSIGNMENTS {
                return Err(RequestError::InvalidEncoding);
            }
            out.push(OPTIMIZER_LOGICAL_OP_UPDATE);
            out.extend_from_slice(&relation_index.to_be_bytes());
            put_identifier(out, relation_name)?;
            out.extend_from_slice(&(assignments.len() as u16).to_be_bytes());
            for assign in assignments {
                put_identifier(out, &assign.column_name)?;
                match assign.value {
                    UpdateValue::Parameter(idx) => {
                        if idx == 0 || idx > MAX_PARAMETER_INDEX {
                            return Err(RequestError::InvalidEncoding);
                        }
                        out.push(0);
                        out.extend_from_slice(&idx.to_be_bytes());
                    }
                    UpdateValue::Null => {
                        out.push(1);
                    }
                }
            }
            let mut expr_nodes = 0usize;
            encode_expr_inner(predicate, out, &mut expr_nodes, 1)?;
        }
    }
    if out.len() > MAX_UNIFIED_REQUEST_BYTES {
        return Err(RequestError::TooLarge);
    }
    Ok(())
}

pub fn decode_logical_request(bytes: &[u8]) -> Result<LogicalRequest, RequestError> {
    if bytes.len() > MAX_UNIFIED_REQUEST_BYTES {
        return Err(RequestError::TooLarge);
    }
    let mut reader = WireReader::new(bytes);
    let version = reader.u16()?;
    if version != OPTIMIZER_REQUEST_VERSION {
        return Err(RequestError::UnsupportedVersion(version));
    }
    let kind = reader.u8()?;
    let flags = reader.u8()?;
    if kind != OPTIMIZER_UNIFIED_REQUEST_KIND || flags != 0 {
        return Err(RequestError::InvalidEncoding);
    }
    let schema_name = reader.identifier()?;
    let mut nodes = 0usize;
    let root = decode_op(&mut reader, &mut nodes, 1)?;
    if reader.offset != bytes.len() {
        return Err(RequestError::TrailingBytes);
    }
    Ok(LogicalRequest { schema_name, root })
}

fn decode_op(
    reader: &mut WireReader<'_>,
    nodes: &mut usize,
    depth: usize,
) -> Result<LogicalOp, RequestError> {
    if depth > MAX_EXPR_DEPTH {
        return Err(RequestError::TooDeep);
    }
    *nodes += 1;
    if *nodes > MAX_LOGICAL_NODES {
        return Err(RequestError::TooManyNodes);
    }
    let tag = reader.u8()?;
    match tag {
        OPTIMIZER_LOGICAL_OP_SCAN => {
            let relation_index = reader.u16()?;
            let relation_name = reader.identifier()?;
            Ok(LogicalOp::Scan {
                relation_index,
                relation_name,
            })
        }
        OPTIMIZER_LOGICAL_OP_FILTER => {
            let mut expr_nodes = 0usize;
            let predicate = decode_expr_inner(reader, &mut expr_nodes, 1)?;
            let input = Box::new(decode_op(reader, nodes, depth + 1)?);
            Ok(LogicalOp::Filter { input, predicate })
        }
        OPTIMIZER_LOGICAL_OP_PROJECT => {
            let count = usize::from(reader.u16()?);
            if count == 0 || count > MAX_PROJECTIONS {
                return Err(RequestError::InvalidEncoding);
            }
            let mut columns = Vec::with_capacity(count);
            for _ in 0..count {
                columns.push(read_col_ref(reader)?);
            }
            let input = Box::new(decode_op(reader, nodes, depth + 1)?);
            Ok(LogicalOp::Project { input, columns })
        }
        OPTIMIZER_LOGICAL_OP_JOIN => {
            let join_type = JoinType::from_byte(reader.u8()?)?;
            let key_count = usize::from(reader.u8()?);
            if key_count == 0 || key_count > MAX_JOIN_KEYS {
                return Err(RequestError::InvalidEncoding);
            }
            let mut eq_keys = Vec::with_capacity(key_count);
            for _ in 0..key_count {
                eq_keys.push((read_col_ref(reader)?, read_col_ref(reader)?));
            }
            let residual = match reader.u8()? {
                0 => None,
                1 => {
                    let mut expr_nodes = 0usize;
                    Some(decode_expr_inner(reader, &mut expr_nodes, 1)?)
                }
                _ => return Err(RequestError::InvalidEncoding),
            };
            let left = Box::new(decode_op(reader, nodes, depth + 1)?);
            let right = Box::new(decode_op(reader, nodes, depth + 1)?);
            Ok(LogicalOp::Join {
                join_type,
                left,
                right,
                eq_keys,
                residual,
            })
        }
        OPTIMIZER_LOGICAL_OP_AGGREGATE => {
            let group_count = usize::from(reader.u8()?);
            if group_count > MAX_GROUP_KEYS {
                return Err(RequestError::InvalidEncoding);
            }
            let mut group_keys = Vec::with_capacity(group_count);
            for _ in 0..group_count {
                group_keys.push(read_col_ref(reader)?);
            }
            let agg_count = usize::from(reader.u8()?);
            if agg_count > MAX_AGGREGATES || (group_count == 0 && agg_count == 0) {
                return Err(RequestError::InvalidEncoding);
            }
            let mut aggregates = Vec::with_capacity(agg_count);
            for _ in 0..agg_count {
                let kind = AggregateKind::from_byte(reader.u8()?)?;
                let has_arg = reader.u8()?;
                let arg = match (kind, has_arg) {
                    (AggregateKind::CountStar, 0) => None,
                    (AggregateKind::CountStar, _) => return Err(RequestError::InvalidEncoding),
                    (_, 1) => Some(read_col_ref(reader)?),
                    _ => return Err(RequestError::InvalidEncoding),
                };
                aggregates.push(AggregateSpec { kind, arg });
            }
            let having = match reader.u8()? {
                0 => None,
                1 => {
                    let mut expr_nodes = 0usize;
                    Some(decode_expr_inner(reader, &mut expr_nodes, 1)?)
                }
                _ => return Err(RequestError::InvalidEncoding),
            };
            let input = Box::new(decode_op(reader, nodes, depth + 1)?);
            Ok(LogicalOp::Aggregate {
                input,
                group_keys,
                aggregates,
                having,
            })
        }
        OPTIMIZER_LOGICAL_OP_SORT => {
            let count = usize::from(reader.u8()?);
            if count == 0 || count > MAX_ORDER_ITEMS {
                return Err(RequestError::InvalidEncoding);
            }
            let mut order_by = Vec::with_capacity(count);
            for _ in 0..count {
                let target = match reader.u8()? {
                    0 => OrderTarget::Column(read_col_ref(reader)?),
                    1 => OrderTarget::AggregateIndex(reader.u16()?),
                    _ => return Err(RequestError::InvalidEncoding),
                };
                let descending = match reader.u8()? {
                    0 => false,
                    1 => true,
                    _ => return Err(RequestError::InvalidEncoding),
                };
                let nulls_first = match reader.u8()? {
                    0 => false,
                    1 => true,
                    _ => return Err(RequestError::InvalidEncoding),
                };
                order_by.push(OrderItem {
                    target,
                    descending,
                    nulls_first,
                });
            }
            let input = Box::new(decode_op(reader, nodes, depth + 1)?);
            Ok(LogicalOp::Sort { input, order_by })
        }
        OPTIMIZER_LOGICAL_OP_LIMIT => {
            let count = reader.u32()?;
            let input = Box::new(decode_op(reader, nodes, depth + 1)?);
            Ok(LogicalOp::Limit { input, count })
        }
        OPTIMIZER_LOGICAL_OP_DELETE => {
            let relation_index = reader.u16()?;
            let relation_name = reader.identifier()?;
            let mut expr_nodes = 0usize;
            let predicate = decode_expr_inner(reader, &mut expr_nodes, 1)?;
            Ok(LogicalOp::Delete {
                relation_index,
                relation_name,
                predicate,
            })
        }
        OPTIMIZER_LOGICAL_OP_UPDATE => {
            let relation_index = reader.u16()?;
            let relation_name = reader.identifier()?;
            let count = usize::from(reader.u16()?);
            if count == 0 || count > MAX_UPDATE_ASSIGNMENTS {
                return Err(RequestError::InvalidEncoding);
            }
            let mut assignments = Vec::with_capacity(count);
            for _ in 0..count {
                let column_name = reader.identifier()?;
                let value = match reader.u8()? {
                    0 => {
                        let idx = reader.u32()?;
                        if idx == 0 || idx > MAX_PARAMETER_INDEX {
                            return Err(RequestError::InvalidEncoding);
                        }
                        UpdateValue::Parameter(idx)
                    }
                    1 => UpdateValue::Null,
                    _ => return Err(RequestError::InvalidEncoding),
                };
                assignments.push(UpdateAssignment { column_name, value });
            }
            let mut expr_nodes = 0usize;
            let predicate = decode_expr_inner(reader, &mut expr_nodes, 1)?;
            Ok(LogicalOp::Update {
                relation_index,
                relation_name,
                assignments,
                predicate,
            })
        }
        _ => Err(RequestError::InvalidEncoding),
    }
}
