//! Unified composable physical plan tree, binary codec, and validator.

use super::bind::{BoundOrderTarget, BoundUpdateValue};
use super::expr::{
    BoundColumn, BoundExpr, ExprError, MAX_EXPR_DEPTH, MAX_PARAMETER_INDEX, WireReader,
    decode_bound_expr_inner, encode_bound_expr_inner, put_bound_column, put_identifier,
    valid_identifier,
};
use super::logical::{
    AggregateKind, JoinType, MAX_AGGREGATES, MAX_GROUP_KEYS, MAX_JOIN_KEYS, MAX_ORDER_ITEMS,
    MAX_PROJECTIONS, MAX_UPDATE_ASSIGNMENTS,
};
use crate::query_abi::{
    OPTIMIZER_PHYSICAL_OP_COVERING_INDEX_SCAN, OPTIMIZER_PHYSICAL_OP_DELETE,
    OPTIMIZER_PHYSICAL_OP_EMPTY_RESULT, OPTIMIZER_PHYSICAL_OP_FILTER,
    OPTIMIZER_PHYSICAL_OP_HASH_AGGREGATE, OPTIMIZER_PHYSICAL_OP_HASH_JOIN,
    OPTIMIZER_PHYSICAL_OP_INDEX_MULTI_POINT_SCAN, OPTIMIZER_PHYSICAL_OP_INDEX_NESTED_LOOP_JOIN,
    OPTIMIZER_PHYSICAL_OP_INDEX_ORDERED_SCAN, OPTIMIZER_PHYSICAL_OP_INDEX_POINT_SCAN,
    OPTIMIZER_PHYSICAL_OP_INDEX_RANGE_SCAN, OPTIMIZER_PHYSICAL_OP_LIMIT,
    OPTIMIZER_PHYSICAL_OP_MERGE_JOIN, OPTIMIZER_PHYSICAL_OP_PROJECT,
    OPTIMIZER_PHYSICAL_OP_SEQ_SCAN, OPTIMIZER_PHYSICAL_OP_SORT,
    OPTIMIZER_PHYSICAL_OP_STREAM_AGGREGATE, OPTIMIZER_PHYSICAL_OP_TOPN_HEAP,
    OPTIMIZER_PHYSICAL_OP_UPDATE, OPTIMIZER_RESPONSE_VERSION, OPTIMIZER_UNIFIED_PLAN_KIND,
};
use std::collections::HashSet;

pub const MAX_UNIFIED_PLAN_BYTES: usize = 64 * 1024;
pub const MAX_PHYSICAL_NODES: usize = 64;
pub const MAX_PLAN_SCAN_COLUMNS: usize = 256;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationReference {
    pub relation_index: u16,
    pub relation_name: String,
    pub relation_id: [u8; 16],
    pub table_epoch: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexBound {
    pub value: BoundExpr,
    pub inclusive: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundAggregateSpec {
    pub kind: AggregateKind,
    pub arg: Option<BoundColumn>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalOrderItem {
    pub target: BoundOrderTarget,
    pub descending: bool,
    pub nulls_first: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalUpdateAssignment {
    pub column: BoundColumn,
    pub value: BoundUpdateValue,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PhysicalOp {
    SeqScan {
        relation: RelationReference,
        scan_columns: Vec<BoundColumn>,
    },
    IndexPointScan {
        relation: RelationReference,
        scan_columns: Vec<BoundColumn>,
        index_id: [u8; 16],
        key_columns: Vec<BoundColumn>,
        key_values: Vec<BoundExpr>,
    },
    IndexMultiPointScan {
        relation: RelationReference,
        scan_columns: Vec<BoundColumn>,
        index_id: [u8; 16],
        key_column: BoundColumn,
        probe_values: Vec<BoundExpr>,
    },
    IndexRangeScan {
        relation: RelationReference,
        scan_columns: Vec<BoundColumn>,
        index_id: [u8; 16],
        prefix_columns: Vec<BoundColumn>,
        prefix_values: Vec<BoundExpr>,
        range_column: Option<BoundColumn>,
        lower: Option<IndexBound>,
        upper: Option<IndexBound>,
    },
    IndexOrderedScan {
        relation: RelationReference,
        scan_columns: Vec<BoundColumn>,
        index_id: [u8; 16],
        order_columns: Vec<BoundColumn>,
        descending: bool,
        limit_hint: Option<u32>,
    },
    CoveringIndexScan {
        relation: RelationReference,
        scan_columns: Vec<BoundColumn>,
        index_id: [u8; 16],
        range_column: Option<BoundColumn>,
        lower: Option<IndexBound>,
        upper: Option<IndexBound>,
    },
    EmptyResult {
        output_columns: Vec<BoundColumn>,
    },
    Filter {
        input: Box<PhysicalOp>,
        predicate: BoundExpr,
    },
    Project {
        input: Box<PhysicalOp>,
        columns: Vec<BoundColumn>,
    },
    Limit {
        input: Box<PhysicalOp>,
        count: u32,
    },
    HashJoin {
        join_type: JoinType,
        left: Box<PhysicalOp>,
        right: Box<PhysicalOp>,
        eq_keys: Vec<(BoundColumn, BoundColumn)>,
        residual: Option<BoundExpr>,
        allow_spill: bool,
    },
    IndexNestedLoopJoin {
        join_type: JoinType,
        outer: Box<PhysicalOp>,
        inner_relation: RelationReference,
        inner_scan_columns: Vec<BoundColumn>,
        index_id: [u8; 16],
        outer_keys: Vec<BoundColumn>,
        inner_prefix_columns: Vec<BoundColumn>,
        residual: Option<BoundExpr>,
    },
    MergeJoin {
        join_type: JoinType,
        left: Box<PhysicalOp>,
        right: Box<PhysicalOp>,
        eq_keys: Vec<(BoundColumn, BoundColumn)>,
        residual: Option<BoundExpr>,
    },
    HashAggregate {
        input: Box<PhysicalOp>,
        group_keys: Vec<BoundColumn>,
        aggregates: Vec<BoundAggregateSpec>,
        having: Option<BoundExpr>,
        allow_spill: bool,
    },
    StreamAggregate {
        input: Box<PhysicalOp>,
        group_keys: Vec<BoundColumn>,
        aggregates: Vec<BoundAggregateSpec>,
        having: Option<BoundExpr>,
    },
    Sort {
        input: Box<PhysicalOp>,
        order_by: Vec<PhysicalOrderItem>,
        allow_spill: bool,
    },
    TopNHeap {
        input: Box<PhysicalOp>,
        order_by: Vec<PhysicalOrderItem>,
        limit: u32,
    },
    Delete {
        relation: RelationReference,
        input: Box<PhysicalOp>,
    },
    Update {
        relation: RelationReference,
        input: Box<PhysicalOp>,
        assignments: Vec<PhysicalUpdateAssignment>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalPlan {
    pub schema_name: String,
    pub schema_id: [u8; 16],
    pub schema_version: u64,
    pub root: PhysicalOp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlanError {
    Truncated,
    TrailingBytes,
    UnsupportedVersion(u16),
    InvalidEncoding,
    InvalidRelationReference,
    InvalidColumnReference,
    InvalidIndexReference,
    InvalidPipeline,
    TooLarge,
    TooManyNodes,
    TooDeep,
    Expr(ExprError),
}

impl From<ExprError> for PlanError {
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

pub fn encode_physical_plan(plan: &PhysicalPlan) -> Result<Vec<u8>, PlanError> {
    validate_physical_plan(plan)?;
    let mut out = Vec::with_capacity(256);
    out.extend_from_slice(&OPTIMIZER_RESPONSE_VERSION.to_be_bytes());
    out.push(OPTIMIZER_UNIFIED_PLAN_KIND);
    out.push(0);
    put_identifier(&mut out, &plan.schema_name)?;
    out.extend_from_slice(&plan.schema_id);
    out.extend_from_slice(&plan.schema_version.to_be_bytes());
    let mut nodes = 0usize;
    encode_physical_op(&plan.root, &mut out, &mut nodes, 1)?;
    if out.len() > MAX_UNIFIED_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    Ok(out)
}

fn put_relation_ref(out: &mut Vec<u8>, rel: &RelationReference) -> Result<(), PlanError> {
    out.extend_from_slice(&rel.relation_index.to_be_bytes());
    put_identifier(out, &rel.relation_name)?;
    out.extend_from_slice(&rel.relation_id);
    out.extend_from_slice(&rel.table_epoch.to_be_bytes());
    Ok(())
}

fn put_columns(out: &mut Vec<u8>, cols: &[BoundColumn]) {
    out.extend_from_slice(&(cols.len() as u16).to_be_bytes());
    for c in cols {
        put_bound_column(out, c);
    }
}

fn put_expr(out: &mut Vec<u8>, expr: &BoundExpr) -> Result<(), PlanError> {
    let mut expr_nodes = 0usize;
    encode_bound_expr_inner(expr, out, &mut expr_nodes, 1)?;
    Ok(())
}

fn put_bound_opt(out: &mut Vec<u8>, bound: &Option<IndexBound>) -> Result<(), PlanError> {
    match bound {
        None => out.push(0),
        Some(b) => {
            out.push(if b.inclusive { 1 } else { 2 });
            put_expr(out, &b.value)?;
        }
    }
    Ok(())
}

fn put_order_items(out: &mut Vec<u8>, items: &[PhysicalOrderItem]) {
    out.push(items.len() as u8);
    for item in items {
        match &item.target {
            BoundOrderTarget::Column(col) => {
                out.push(0);
                out.extend_from_slice(&col.relation_index.to_be_bytes());
                put_bound_column(out, &col.column);
            }
            BoundOrderTarget::AggregateIndex(idx) => {
                out.push(1);
                out.extend_from_slice(&idx.to_be_bytes());
            }
        }
        out.push(u8::from(item.descending));
        out.push(u8::from(item.nulls_first));
    }
}

fn put_agg_specs(
    out: &mut Vec<u8>,
    group_keys: &[BoundColumn],
    aggs: &[BoundAggregateSpec],
    having: &Option<BoundExpr>,
) -> Result<(), PlanError> {
    out.push(group_keys.len() as u8);
    for k in group_keys {
        put_bound_column(out, k);
    }
    out.push(aggs.len() as u8);
    for a in aggs {
        out.push(a.kind.to_byte());
        match &a.arg {
            None => out.push(0),
            Some(c) => {
                out.push(1);
                put_bound_column(out, c);
            }
        }
    }
    match having {
        None => out.push(0),
        Some(h) => {
            out.push(1);
            put_expr(out, h)?;
        }
    }
    Ok(())
}

fn encode_physical_op(
    op: &PhysicalOp,
    out: &mut Vec<u8>,
    nodes: &mut usize,
    depth: usize,
) -> Result<(), PlanError> {
    if depth > MAX_EXPR_DEPTH {
        return Err(PlanError::TooDeep);
    }
    *nodes += 1;
    if *nodes > MAX_PHYSICAL_NODES {
        return Err(PlanError::TooManyNodes);
    }
    match op {
        PhysicalOp::SeqScan {
            relation,
            scan_columns,
        } => {
            out.push(OPTIMIZER_PHYSICAL_OP_SEQ_SCAN);
            put_relation_ref(out, relation)?;
            put_columns(out, scan_columns);
        }
        PhysicalOp::IndexPointScan {
            relation,
            scan_columns,
            index_id,
            key_columns,
            key_values,
        } => {
            out.push(OPTIMIZER_PHYSICAL_OP_INDEX_POINT_SCAN);
            put_relation_ref(out, relation)?;
            put_columns(out, scan_columns);
            out.extend_from_slice(index_id);
            out.push(key_columns.len() as u8);
            for k in key_columns {
                put_bound_column(out, k);
            }
            for v in key_values {
                put_expr(out, v)?;
            }
        }
        PhysicalOp::IndexMultiPointScan {
            relation,
            scan_columns,
            index_id,
            key_column,
            probe_values,
        } => {
            out.push(OPTIMIZER_PHYSICAL_OP_INDEX_MULTI_POINT_SCAN);
            put_relation_ref(out, relation)?;
            put_columns(out, scan_columns);
            out.extend_from_slice(index_id);
            put_bound_column(out, key_column);
            out.extend_from_slice(&(probe_values.len() as u16).to_be_bytes());
            for v in probe_values {
                put_expr(out, v)?;
            }
        }
        PhysicalOp::IndexRangeScan {
            relation,
            scan_columns,
            index_id,
            prefix_columns,
            prefix_values,
            range_column,
            lower,
            upper,
        } => {
            out.push(OPTIMIZER_PHYSICAL_OP_INDEX_RANGE_SCAN);
            put_relation_ref(out, relation)?;
            put_columns(out, scan_columns);
            out.extend_from_slice(index_id);
            out.push(prefix_columns.len() as u8);
            for k in prefix_columns {
                put_bound_column(out, k);
            }
            for v in prefix_values {
                put_expr(out, v)?;
            }
            match range_column {
                None => out.push(0),
                Some(rc) => {
                    out.push(1);
                    put_bound_column(out, rc);
                }
            }
            put_bound_opt(out, lower)?;
            put_bound_opt(out, upper)?;
        }
        PhysicalOp::IndexOrderedScan {
            relation,
            scan_columns,
            index_id,
            order_columns,
            descending,
            limit_hint,
        } => {
            out.push(OPTIMIZER_PHYSICAL_OP_INDEX_ORDERED_SCAN);
            put_relation_ref(out, relation)?;
            put_columns(out, scan_columns);
            out.extend_from_slice(index_id);
            out.push(order_columns.len() as u8);
            for c in order_columns {
                put_bound_column(out, c);
            }
            out.push(u8::from(*descending));
            match limit_hint {
                None => out.push(0),
                Some(l) => {
                    out.push(1);
                    out.extend_from_slice(&l.to_be_bytes());
                }
            }
        }
        PhysicalOp::CoveringIndexScan {
            relation,
            scan_columns,
            index_id,
            range_column,
            lower,
            upper,
        } => {
            out.push(OPTIMIZER_PHYSICAL_OP_COVERING_INDEX_SCAN);
            put_relation_ref(out, relation)?;
            put_columns(out, scan_columns);
            out.extend_from_slice(index_id);
            match range_column {
                None => out.push(0),
                Some(rc) => {
                    out.push(1);
                    put_bound_column(out, rc);
                }
            }
            put_bound_opt(out, lower)?;
            put_bound_opt(out, upper)?;
        }
        PhysicalOp::EmptyResult { output_columns } => {
            out.push(OPTIMIZER_PHYSICAL_OP_EMPTY_RESULT);
            put_columns(out, output_columns);
        }
        PhysicalOp::Filter { input, predicate } => {
            out.push(OPTIMIZER_PHYSICAL_OP_FILTER);
            put_expr(out, predicate)?;
            encode_physical_op(input, out, nodes, depth + 1)?;
        }
        PhysicalOp::Project { input, columns } => {
            out.push(OPTIMIZER_PHYSICAL_OP_PROJECT);
            put_columns(out, columns);
            encode_physical_op(input, out, nodes, depth + 1)?;
        }
        PhysicalOp::Limit { input, count } => {
            out.push(OPTIMIZER_PHYSICAL_OP_LIMIT);
            out.extend_from_slice(&count.to_be_bytes());
            encode_physical_op(input, out, nodes, depth + 1)?;
        }
        PhysicalOp::HashJoin {
            join_type,
            left,
            right,
            eq_keys,
            residual,
            allow_spill,
        } => {
            out.push(OPTIMIZER_PHYSICAL_OP_HASH_JOIN);
            out.push(join_type.to_byte());
            out.push(u8::from(*allow_spill));
            out.push(eq_keys.len() as u8);
            for (l, r) in eq_keys {
                put_bound_column(out, l);
                put_bound_column(out, r);
            }
            match residual {
                None => out.push(0),
                Some(r) => {
                    out.push(1);
                    put_expr(out, r)?;
                }
            }
            encode_physical_op(left, out, nodes, depth + 1)?;
            encode_physical_op(right, out, nodes, depth + 1)?;
        }
        PhysicalOp::IndexNestedLoopJoin {
            join_type,
            outer,
            inner_relation,
            inner_scan_columns,
            index_id,
            outer_keys,
            inner_prefix_columns,
            residual,
        } => {
            out.push(OPTIMIZER_PHYSICAL_OP_INDEX_NESTED_LOOP_JOIN);
            out.push(join_type.to_byte());
            put_relation_ref(out, inner_relation)?;
            put_columns(out, inner_scan_columns);
            out.extend_from_slice(index_id);
            out.push(outer_keys.len() as u8);
            for k in outer_keys {
                put_bound_column(out, k);
            }
            for k in inner_prefix_columns {
                put_bound_column(out, k);
            }
            match residual {
                None => out.push(0),
                Some(r) => {
                    out.push(1);
                    put_expr(out, r)?;
                }
            }
            encode_physical_op(outer, out, nodes, depth + 1)?;
        }
        PhysicalOp::MergeJoin {
            join_type,
            left,
            right,
            eq_keys,
            residual,
        } => {
            out.push(OPTIMIZER_PHYSICAL_OP_MERGE_JOIN);
            out.push(join_type.to_byte());
            out.push(eq_keys.len() as u8);
            for (l, r) in eq_keys {
                put_bound_column(out, l);
                put_bound_column(out, r);
            }
            match residual {
                None => out.push(0),
                Some(r) => {
                    out.push(1);
                    put_expr(out, r)?;
                }
            }
            encode_physical_op(left, out, nodes, depth + 1)?;
            encode_physical_op(right, out, nodes, depth + 1)?;
        }
        PhysicalOp::HashAggregate {
            input,
            group_keys,
            aggregates,
            having,
            allow_spill,
        } => {
            out.push(OPTIMIZER_PHYSICAL_OP_HASH_AGGREGATE);
            out.push(u8::from(*allow_spill));
            put_agg_specs(out, group_keys, aggregates, having)?;
            encode_physical_op(input, out, nodes, depth + 1)?;
        }
        PhysicalOp::StreamAggregate {
            input,
            group_keys,
            aggregates,
            having,
        } => {
            out.push(OPTIMIZER_PHYSICAL_OP_STREAM_AGGREGATE);
            put_agg_specs(out, group_keys, aggregates, having)?;
            encode_physical_op(input, out, nodes, depth + 1)?;
        }
        PhysicalOp::Sort {
            input,
            order_by,
            allow_spill,
        } => {
            out.push(OPTIMIZER_PHYSICAL_OP_SORT);
            out.push(u8::from(*allow_spill));
            put_order_items(out, order_by);
            encode_physical_op(input, out, nodes, depth + 1)?;
        }
        PhysicalOp::TopNHeap {
            input,
            order_by,
            limit,
        } => {
            out.push(OPTIMIZER_PHYSICAL_OP_TOPN_HEAP);
            out.extend_from_slice(&limit.to_be_bytes());
            put_order_items(out, order_by);
            encode_physical_op(input, out, nodes, depth + 1)?;
        }
        PhysicalOp::Delete { relation, input } => {
            out.push(OPTIMIZER_PHYSICAL_OP_DELETE);
            put_relation_ref(out, relation)?;
            encode_physical_op(input, out, nodes, depth + 1)?;
        }
        PhysicalOp::Update {
            relation,
            input,
            assignments,
        } => {
            out.push(OPTIMIZER_PHYSICAL_OP_UPDATE);
            put_relation_ref(out, relation)?;
            out.extend_from_slice(&(assignments.len() as u16).to_be_bytes());
            for a in assignments {
                put_bound_column(out, &a.column);
                match a.value {
                    BoundUpdateValue::Parameter(idx) => {
                        out.push(0);
                        out.extend_from_slice(&idx.to_be_bytes());
                    }
                    BoundUpdateValue::Null => out.push(1),
                }
            }
            encode_physical_op(input, out, nodes, depth + 1)?;
        }
    }
    if out.len() > MAX_UNIFIED_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    Ok(())
}

pub fn decode_physical_plan(bytes: &[u8]) -> Result<PhysicalPlan, PlanError> {
    if bytes.len() > MAX_UNIFIED_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    let mut reader = WireReader::new(bytes);
    let version = reader.u16()?;
    if version != OPTIMIZER_RESPONSE_VERSION {
        return Err(PlanError::UnsupportedVersion(version));
    }
    let kind = reader.u8()?;
    let reserved = reader.u8()?;
    if kind != OPTIMIZER_UNIFIED_PLAN_KIND || reserved != 0 {
        return Err(PlanError::InvalidEncoding);
    }
    let schema_name = reader.identifier()?;
    let schema_id: [u8; 16] = reader
        .take(16)?
        .try_into()
        .map_err(|_| PlanError::Truncated)?;
    let schema_version = reader.u64()?;
    let mut nodes = 0usize;
    let root = decode_physical_op(&mut reader, &mut nodes, 1)?;
    if reader.offset != bytes.len() {
        return Err(PlanError::TrailingBytes);
    }
    let plan = PhysicalPlan {
        schema_name,
        schema_id,
        schema_version,
        root,
    };
    validate_physical_plan(&plan)?;
    Ok(plan)
}

fn read_relation_ref(reader: &mut WireReader<'_>) -> Result<RelationReference, PlanError> {
    let relation_index = reader.u16()?;
    let relation_name = reader.identifier()?;
    let relation_id: [u8; 16] = reader
        .take(16)?
        .try_into()
        .map_err(|_| PlanError::Truncated)?;
    let table_epoch = reader.u64()?;
    Ok(RelationReference {
        relation_index,
        relation_name,
        relation_id,
        table_epoch,
    })
}

fn read_columns(
    reader: &mut WireReader<'_>,
    max_len: usize,
) -> Result<Vec<BoundColumn>, PlanError> {
    let count = usize::from(reader.u16()?);
    if count == 0 || count > max_len {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut cols = Vec::with_capacity(count);
    for _ in 0..count {
        cols.push(reader.bound_column()?);
    }
    Ok(cols)
}

fn read_expr(reader: &mut WireReader<'_>) -> Result<BoundExpr, PlanError> {
    let mut expr_nodes = 0usize;
    Ok(decode_bound_expr_inner(reader, &mut expr_nodes, 1)?)
}

fn read_bound_opt(reader: &mut WireReader<'_>) -> Result<Option<IndexBound>, PlanError> {
    match reader.u8()? {
        0 => Ok(None),
        1 => Ok(Some(IndexBound {
            value: read_expr(reader)?,
            inclusive: true,
        })),
        2 => Ok(Some(IndexBound {
            value: read_expr(reader)?,
            inclusive: false,
        })),
        _ => Err(PlanError::InvalidEncoding),
    }
}

fn read_bool_flag(reader: &mut WireReader<'_>) -> Result<bool, PlanError> {
    match reader.u8()? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(PlanError::InvalidEncoding),
    }
}

fn read_order_items(reader: &mut WireReader<'_>) -> Result<Vec<PhysicalOrderItem>, PlanError> {
    let count = usize::from(reader.u8()?);
    if count == 0 || count > MAX_ORDER_ITEMS {
        return Err(PlanError::InvalidEncoding);
    }
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        let target = match reader.u8()? {
            0 => BoundOrderTarget::Column(super::expr::BoundColumnRef {
                relation_index: reader.u16()?,
                column: reader.bound_column()?,
            }),
            1 => BoundOrderTarget::AggregateIndex(reader.u16()?),
            _ => return Err(PlanError::InvalidEncoding),
        };
        let descending = read_bool_flag(reader)?;
        let nulls_first = read_bool_flag(reader)?;
        items.push(PhysicalOrderItem {
            target,
            descending,
            nulls_first,
        });
    }
    Ok(items)
}

type DecodedAggSpecs = (Vec<BoundColumn>, Vec<BoundAggregateSpec>, Option<BoundExpr>);

fn read_agg_specs(reader: &mut WireReader<'_>) -> Result<DecodedAggSpecs, PlanError> {
    let g_count = usize::from(reader.u8()?);
    if g_count > MAX_GROUP_KEYS {
        return Err(PlanError::InvalidEncoding);
    }
    let mut group_keys = Vec::with_capacity(g_count);
    for _ in 0..g_count {
        group_keys.push(reader.bound_column()?);
    }
    let a_count = usize::from(reader.u8()?);
    if a_count > MAX_AGGREGATES || (g_count == 0 && a_count == 0) {
        return Err(PlanError::InvalidEncoding);
    }
    let mut aggregates = Vec::with_capacity(a_count);
    for _ in 0..a_count {
        let kind =
            AggregateKind::from_byte(reader.u8()?).map_err(|_| PlanError::InvalidEncoding)?;
        let arg = match reader.u8()? {
            0 => None,
            1 => Some(reader.bound_column()?),
            _ => return Err(PlanError::InvalidEncoding),
        };
        aggregates.push(BoundAggregateSpec { kind, arg });
    }
    let having = match reader.u8()? {
        0 => None,
        1 => Some(read_expr(reader)?),
        _ => return Err(PlanError::InvalidEncoding),
    };
    Ok((group_keys, aggregates, having))
}

fn decode_physical_op(
    reader: &mut WireReader<'_>,
    nodes: &mut usize,
    depth: usize,
) -> Result<PhysicalOp, PlanError> {
    if depth > MAX_EXPR_DEPTH {
        return Err(PlanError::TooDeep);
    }
    *nodes += 1;
    if *nodes > MAX_PHYSICAL_NODES {
        return Err(PlanError::TooManyNodes);
    }
    let tag = reader.u8()?;
    match tag {
        OPTIMIZER_PHYSICAL_OP_SEQ_SCAN => Ok(PhysicalOp::SeqScan {
            relation: read_relation_ref(reader)?,
            scan_columns: read_columns(reader, MAX_PLAN_SCAN_COLUMNS)?,
        }),
        OPTIMIZER_PHYSICAL_OP_INDEX_POINT_SCAN => {
            let relation = read_relation_ref(reader)?;
            let scan_columns = read_columns(reader, MAX_PLAN_SCAN_COLUMNS)?;
            let index_id: [u8; 16] = reader
                .take(16)?
                .try_into()
                .map_err(|_| PlanError::Truncated)?;
            let k_count = usize::from(reader.u8()?);
            if k_count == 0 || k_count > MAX_JOIN_KEYS {
                return Err(PlanError::InvalidIndexReference);
            }
            let mut key_columns = Vec::with_capacity(k_count);
            for _ in 0..k_count {
                key_columns.push(reader.bound_column()?);
            }
            let mut key_values = Vec::with_capacity(k_count);
            for _ in 0..k_count {
                key_values.push(read_expr(reader)?);
            }
            Ok(PhysicalOp::IndexPointScan {
                relation,
                scan_columns,
                index_id,
                key_columns,
                key_values,
            })
        }
        OPTIMIZER_PHYSICAL_OP_INDEX_MULTI_POINT_SCAN => {
            let relation = read_relation_ref(reader)?;
            let scan_columns = read_columns(reader, MAX_PLAN_SCAN_COLUMNS)?;
            let index_id: [u8; 16] = reader
                .take(16)?
                .try_into()
                .map_err(|_| PlanError::Truncated)?;
            let key_column = reader.bound_column()?;
            let count = usize::from(reader.u16()?);
            if !(1..=64).contains(&count) {
                return Err(PlanError::InvalidIndexReference);
            }
            let mut probe_values = Vec::with_capacity(count);
            for _ in 0..count {
                probe_values.push(read_expr(reader)?);
            }
            Ok(PhysicalOp::IndexMultiPointScan {
                relation,
                scan_columns,
                index_id,
                key_column,
                probe_values,
            })
        }
        OPTIMIZER_PHYSICAL_OP_INDEX_RANGE_SCAN => {
            let relation = read_relation_ref(reader)?;
            let scan_columns = read_columns(reader, MAX_PLAN_SCAN_COLUMNS)?;
            let index_id: [u8; 16] = reader
                .take(16)?
                .try_into()
                .map_err(|_| PlanError::Truncated)?;
            let p_count = usize::from(reader.u8()?);
            if p_count > MAX_JOIN_KEYS {
                return Err(PlanError::InvalidIndexReference);
            }
            let mut prefix_columns = Vec::with_capacity(p_count);
            for _ in 0..p_count {
                prefix_columns.push(reader.bound_column()?);
            }
            let mut prefix_values = Vec::with_capacity(p_count);
            for _ in 0..p_count {
                prefix_values.push(read_expr(reader)?);
            }
            let range_column = match reader.u8()? {
                0 => None,
                1 => Some(reader.bound_column()?),
                _ => return Err(PlanError::InvalidEncoding),
            };
            let lower = read_bound_opt(reader)?;
            let upper = read_bound_opt(reader)?;
            Ok(PhysicalOp::IndexRangeScan {
                relation,
                scan_columns,
                index_id,
                prefix_columns,
                prefix_values,
                range_column,
                lower,
                upper,
            })
        }
        OPTIMIZER_PHYSICAL_OP_INDEX_ORDERED_SCAN => {
            let relation = read_relation_ref(reader)?;
            let scan_columns = read_columns(reader, MAX_PLAN_SCAN_COLUMNS)?;
            let index_id: [u8; 16] = reader
                .take(16)?
                .try_into()
                .map_err(|_| PlanError::Truncated)?;
            let o_count = usize::from(reader.u8()?);
            if o_count == 0 || o_count > MAX_ORDER_ITEMS {
                return Err(PlanError::InvalidIndexReference);
            }
            let mut order_columns = Vec::with_capacity(o_count);
            for _ in 0..o_count {
                order_columns.push(reader.bound_column()?);
            }
            let descending = read_bool_flag(reader)?;
            let limit_hint = match reader.u8()? {
                0 => None,
                1 => Some(reader.u32()?),
                _ => return Err(PlanError::InvalidEncoding),
            };
            Ok(PhysicalOp::IndexOrderedScan {
                relation,
                scan_columns,
                index_id,
                order_columns,
                descending,
                limit_hint,
            })
        }
        OPTIMIZER_PHYSICAL_OP_COVERING_INDEX_SCAN => {
            let relation = read_relation_ref(reader)?;
            let scan_columns = read_columns(reader, MAX_PLAN_SCAN_COLUMNS)?;
            let index_id: [u8; 16] = reader
                .take(16)?
                .try_into()
                .map_err(|_| PlanError::Truncated)?;
            let range_column = match reader.u8()? {
                0 => None,
                1 => Some(reader.bound_column()?),
                _ => return Err(PlanError::InvalidEncoding),
            };
            let lower = read_bound_opt(reader)?;
            let upper = read_bound_opt(reader)?;
            Ok(PhysicalOp::CoveringIndexScan {
                relation,
                scan_columns,
                index_id,
                range_column,
                lower,
                upper,
            })
        }
        OPTIMIZER_PHYSICAL_OP_EMPTY_RESULT => Ok(PhysicalOp::EmptyResult {
            output_columns: read_columns(reader, MAX_PROJECTIONS)?,
        }),
        OPTIMIZER_PHYSICAL_OP_FILTER => {
            let predicate = read_expr(reader)?;
            let input = Box::new(decode_physical_op(reader, nodes, depth + 1)?);
            Ok(PhysicalOp::Filter { input, predicate })
        }
        OPTIMIZER_PHYSICAL_OP_PROJECT => {
            let columns = read_columns(reader, MAX_PROJECTIONS)?;
            let input = Box::new(decode_physical_op(reader, nodes, depth + 1)?);
            Ok(PhysicalOp::Project { input, columns })
        }
        OPTIMIZER_PHYSICAL_OP_LIMIT => {
            let count = reader.u32()?;
            let input = Box::new(decode_physical_op(reader, nodes, depth + 1)?);
            Ok(PhysicalOp::Limit { input, count })
        }
        OPTIMIZER_PHYSICAL_OP_HASH_JOIN => {
            let join_type =
                JoinType::from_byte(reader.u8()?).map_err(|_| PlanError::InvalidEncoding)?;
            let allow_spill = read_bool_flag(reader)?;
            let k_count = usize::from(reader.u8()?);
            if k_count == 0 || k_count > MAX_JOIN_KEYS {
                return Err(PlanError::InvalidColumnReference);
            }
            let mut eq_keys = Vec::with_capacity(k_count);
            for _ in 0..k_count {
                eq_keys.push((reader.bound_column()?, reader.bound_column()?));
            }
            let residual = match reader.u8()? {
                0 => None,
                1 => Some(read_expr(reader)?),
                _ => return Err(PlanError::InvalidEncoding),
            };
            let left = Box::new(decode_physical_op(reader, nodes, depth + 1)?);
            let right = Box::new(decode_physical_op(reader, nodes, depth + 1)?);
            Ok(PhysicalOp::HashJoin {
                join_type,
                left,
                right,
                eq_keys,
                residual,
                allow_spill,
            })
        }
        OPTIMIZER_PHYSICAL_OP_INDEX_NESTED_LOOP_JOIN => {
            let join_type =
                JoinType::from_byte(reader.u8()?).map_err(|_| PlanError::InvalidEncoding)?;
            let inner_relation = read_relation_ref(reader)?;
            let inner_scan_columns = read_columns(reader, MAX_PLAN_SCAN_COLUMNS)?;
            let index_id: [u8; 16] = reader
                .take(16)?
                .try_into()
                .map_err(|_| PlanError::Truncated)?;
            let k_count = usize::from(reader.u8()?);
            if k_count == 0 || k_count > MAX_JOIN_KEYS {
                return Err(PlanError::InvalidIndexReference);
            }
            let mut outer_keys = Vec::with_capacity(k_count);
            for _ in 0..k_count {
                outer_keys.push(reader.bound_column()?);
            }
            let mut inner_prefix_columns = Vec::with_capacity(k_count);
            for _ in 0..k_count {
                inner_prefix_columns.push(reader.bound_column()?);
            }
            let residual = match reader.u8()? {
                0 => None,
                1 => Some(read_expr(reader)?),
                _ => return Err(PlanError::InvalidEncoding),
            };
            let outer = Box::new(decode_physical_op(reader, nodes, depth + 1)?);
            Ok(PhysicalOp::IndexNestedLoopJoin {
                join_type,
                outer,
                inner_relation,
                inner_scan_columns,
                index_id,
                outer_keys,
                inner_prefix_columns,
                residual,
            })
        }
        OPTIMIZER_PHYSICAL_OP_MERGE_JOIN => {
            let join_type =
                JoinType::from_byte(reader.u8()?).map_err(|_| PlanError::InvalidEncoding)?;
            let k_count = usize::from(reader.u8()?);
            if k_count == 0 || k_count > MAX_JOIN_KEYS {
                return Err(PlanError::InvalidColumnReference);
            }
            let mut eq_keys = Vec::with_capacity(k_count);
            for _ in 0..k_count {
                eq_keys.push((reader.bound_column()?, reader.bound_column()?));
            }
            let residual = match reader.u8()? {
                0 => None,
                1 => Some(read_expr(reader)?),
                _ => return Err(PlanError::InvalidEncoding),
            };
            let left = Box::new(decode_physical_op(reader, nodes, depth + 1)?);
            let right = Box::new(decode_physical_op(reader, nodes, depth + 1)?);
            Ok(PhysicalOp::MergeJoin {
                join_type,
                left,
                right,
                eq_keys,
                residual,
            })
        }
        OPTIMIZER_PHYSICAL_OP_HASH_AGGREGATE => {
            let allow_spill = read_bool_flag(reader)?;
            let (group_keys, aggregates, having) = read_agg_specs(reader)?;
            let input = Box::new(decode_physical_op(reader, nodes, depth + 1)?);
            Ok(PhysicalOp::HashAggregate {
                input,
                group_keys,
                aggregates,
                having,
                allow_spill,
            })
        }
        OPTIMIZER_PHYSICAL_OP_STREAM_AGGREGATE => {
            let (group_keys, aggregates, having) = read_agg_specs(reader)?;
            let input = Box::new(decode_physical_op(reader, nodes, depth + 1)?);
            Ok(PhysicalOp::StreamAggregate {
                input,
                group_keys,
                aggregates,
                having,
            })
        }
        OPTIMIZER_PHYSICAL_OP_SORT => {
            let allow_spill = read_bool_flag(reader)?;
            let order_by = read_order_items(reader)?;
            let input = Box::new(decode_physical_op(reader, nodes, depth + 1)?);
            Ok(PhysicalOp::Sort {
                input,
                order_by,
                allow_spill,
            })
        }
        OPTIMIZER_PHYSICAL_OP_TOPN_HEAP => {
            let limit = reader.u32()?;
            let order_by = read_order_items(reader)?;
            let input = Box::new(decode_physical_op(reader, nodes, depth + 1)?);
            Ok(PhysicalOp::TopNHeap {
                input,
                order_by,
                limit,
            })
        }
        OPTIMIZER_PHYSICAL_OP_DELETE => {
            let relation = read_relation_ref(reader)?;
            let input = Box::new(decode_physical_op(reader, nodes, depth + 1)?);
            Ok(PhysicalOp::Delete { relation, input })
        }
        OPTIMIZER_PHYSICAL_OP_UPDATE => {
            let relation = read_relation_ref(reader)?;
            let count = usize::from(reader.u16()?);
            if count == 0 || count > MAX_UPDATE_ASSIGNMENTS {
                return Err(PlanError::InvalidColumnReference);
            }
            let mut assignments = Vec::with_capacity(count);
            for _ in 0..count {
                let column = reader.bound_column()?;
                let value = match reader.u8()? {
                    0 => {
                        let idx = reader.u32()?;
                        if idx == 0 || idx > MAX_PARAMETER_INDEX {
                            return Err(PlanError::InvalidEncoding);
                        }
                        BoundUpdateValue::Parameter(idx)
                    }
                    1 => BoundUpdateValue::Null,
                    _ => return Err(PlanError::InvalidEncoding),
                };
                assignments.push(PhysicalUpdateAssignment { column, value });
            }
            let input = Box::new(decode_physical_op(reader, nodes, depth + 1)?);
            Ok(PhysicalOp::Update {
                relation,
                input,
                assignments,
            })
        }
        _ => Err(PlanError::InvalidEncoding),
    }
}

pub fn validate_physical_plan(plan: &PhysicalPlan) -> Result<(), PlanError> {
    if !valid_identifier(&plan.schema_name) || plan.schema_id == [0; 16] || plan.schema_version == 0
    {
        return Err(PlanError::InvalidRelationReference);
    }
    let _ = validate_op(&plan.root)?;
    Ok(())
}

fn validate_relation_ref(rel: &RelationReference) -> Result<(), PlanError> {
    if !valid_identifier(&rel.relation_name) || rel.relation_id == [0; 16] || rel.table_epoch == 0 {
        return Err(PlanError::InvalidRelationReference);
    }
    Ok(())
}

fn validate_scan_columns(cols: &[BoundColumn]) -> Result<(), PlanError> {
    if cols.is_empty() || cols.len() > MAX_PLAN_SCAN_COLUMNS {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut seen = HashSet::new();
    for pair in cols.windows(2) {
        if pair[0].ordinal >= pair[1].ordinal {
            return Err(PlanError::InvalidColumnReference);
        }
    }
    for c in cols {
        if c.column_id == [0; 16] || !seen.insert(c.column_id) {
            return Err(PlanError::InvalidColumnReference);
        }
    }
    Ok(())
}

fn validate_expr_columns(expr: &BoundExpr, available: &[BoundColumn]) -> Result<(), PlanError> {
    match expr {
        BoundExpr::Column(c) => {
            if !available.contains(&c.column) {
                return Err(PlanError::InvalidColumnReference);
            }
            Ok(())
        }
        BoundExpr::Compare { left, right, .. } => {
            validate_expr_columns(left, available)?;
            validate_expr_columns(right, available)
        }
        BoundExpr::And(children) | BoundExpr::Or(children) => {
            for ch in children {
                validate_expr_columns(ch, available)?;
            }
            Ok(())
        }
        BoundExpr::Not(inner) | BoundExpr::IsNull { expr: inner, .. } => {
            validate_expr_columns(inner, available)
        }
        BoundExpr::InList { expr, items, .. } => {
            validate_expr_columns(expr, available)?;
            for item in items {
                validate_expr_columns(item, available)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_op(op: &PhysicalOp) -> Result<Vec<BoundColumn>, PlanError> {
    match op {
        PhysicalOp::SeqScan {
            relation,
            scan_columns,
        } => {
            validate_relation_ref(relation)?;
            validate_scan_columns(scan_columns)?;
            Ok(scan_columns.clone())
        }
        PhysicalOp::IndexPointScan {
            relation,
            scan_columns,
            index_id,
            key_columns,
            key_values,
        } => {
            validate_relation_ref(relation)?;
            validate_scan_columns(scan_columns)?;
            if *index_id == [0; 16]
                || key_columns.is_empty()
                || key_columns.len() != key_values.len()
                || key_columns.iter().any(|k| !scan_columns.contains(k))
            {
                return Err(PlanError::InvalidIndexReference);
            }
            Ok(scan_columns.clone())
        }
        PhysicalOp::IndexMultiPointScan {
            relation,
            scan_columns,
            index_id,
            key_column,
            probe_values,
        } => {
            validate_relation_ref(relation)?;
            validate_scan_columns(scan_columns)?;
            if *index_id == [0; 16]
                || probe_values.is_empty()
                || probe_values.len() > 64
                || !scan_columns.contains(key_column)
            {
                return Err(PlanError::InvalidIndexReference);
            }
            Ok(scan_columns.clone())
        }
        PhysicalOp::IndexRangeScan {
            relation,
            scan_columns,
            index_id,
            prefix_columns,
            prefix_values,
            range_column,
            lower,
            upper,
        } => {
            validate_relation_ref(relation)?;
            validate_scan_columns(scan_columns)?;
            if *index_id == [0; 16]
                || prefix_columns.len() != prefix_values.len()
                || prefix_columns.iter().any(|k| !scan_columns.contains(k))
            {
                return Err(PlanError::InvalidIndexReference);
            }
            if let Some(rc) = range_column {
                if !scan_columns.contains(rc) || (lower.is_none() && upper.is_none()) {
                    return Err(PlanError::InvalidIndexReference);
                }
            } else if prefix_columns.is_empty() || lower.is_some() || upper.is_some() {
                return Err(PlanError::InvalidIndexReference);
            }
            Ok(scan_columns.clone())
        }
        PhysicalOp::IndexOrderedScan {
            relation,
            scan_columns,
            index_id,
            order_columns,
            ..
        } => {
            validate_relation_ref(relation)?;
            validate_scan_columns(scan_columns)?;
            if *index_id == [0; 16]
                || order_columns.is_empty()
                || order_columns.iter().any(|c| !scan_columns.contains(c))
            {
                return Err(PlanError::InvalidIndexReference);
            }
            Ok(scan_columns.clone())
        }
        PhysicalOp::CoveringIndexScan {
            relation,
            scan_columns,
            index_id,
            range_column,
            ..
        } => {
            validate_relation_ref(relation)?;
            validate_scan_columns(scan_columns)?;
            if *index_id == [0; 16] {
                return Err(PlanError::InvalidIndexReference);
            }
            if let Some(rc) = range_column
                && !scan_columns.contains(rc)
            {
                return Err(PlanError::InvalidIndexReference);
            }
            Ok(scan_columns.clone())
        }
        PhysicalOp::EmptyResult { output_columns } => {
            if output_columns.is_empty() || output_columns.len() > MAX_PROJECTIONS {
                return Err(PlanError::InvalidColumnReference);
            }
            Ok(output_columns.clone())
        }
        PhysicalOp::Filter { input, predicate } => {
            let cols = validate_op(input)?;
            validate_expr_columns(predicate, &cols)?;
            Ok(cols)
        }
        PhysicalOp::Project { input, columns } => {
            let available = validate_op(input)?;
            if columns.is_empty() || columns.len() > MAX_PROJECTIONS {
                return Err(PlanError::InvalidColumnReference);
            }
            for c in columns {
                if !available.contains(c) {
                    return Err(PlanError::InvalidColumnReference);
                }
            }
            Ok(columns.clone())
        }
        PhysicalOp::Limit { input, .. } => validate_op(input),
        PhysicalOp::HashJoin {
            join_type,
            left,
            right,
            eq_keys,
            residual,
            ..
        }
        | PhysicalOp::MergeJoin {
            join_type,
            left,
            right,
            eq_keys,
            residual,
        } => {
            let l_cols = validate_op(left)?;
            let r_cols = validate_op(right)?;
            if eq_keys.is_empty() || eq_keys.len() > MAX_JOIN_KEYS {
                return Err(PlanError::InvalidColumnReference);
            }
            for (lk, rk) in eq_keys {
                if !l_cols.contains(lk)
                    || !r_cols.contains(rk)
                    || !super::bind::join_key_types_compatible(lk.scalar_type, rk.scalar_type)
                {
                    return Err(PlanError::InvalidColumnReference);
                }
            }
            let mut out = l_cols;
            if !matches!(join_type, JoinType::Semi | JoinType::Anti) {
                out.extend(r_cols);
            }
            if let Some(res) = residual {
                validate_expr_columns(res, &out)?;
            }
            Ok(out)
        }
        PhysicalOp::IndexNestedLoopJoin {
            join_type,
            outer,
            inner_relation,
            inner_scan_columns,
            index_id,
            outer_keys,
            inner_prefix_columns,
            residual,
        } => {
            let o_cols = validate_op(outer)?;
            validate_relation_ref(inner_relation)?;
            validate_scan_columns(inner_scan_columns)?;
            if *index_id == [0; 16]
                || outer_keys.is_empty()
                || outer_keys.len() != inner_prefix_columns.len()
                || outer_keys.len() > MAX_JOIN_KEYS
            {
                return Err(PlanError::InvalidIndexReference);
            }
            for (ok, ik) in outer_keys.iter().zip(inner_prefix_columns.iter()) {
                if !o_cols.contains(ok)
                    || !inner_scan_columns.contains(ik)
                    || !super::bind::join_key_types_compatible(ok.scalar_type, ik.scalar_type)
                {
                    return Err(PlanError::InvalidColumnReference);
                }
            }
            let mut out = o_cols;
            if !matches!(join_type, JoinType::Semi | JoinType::Anti) {
                out.extend(inner_scan_columns.iter().cloned());
            }
            if let Some(res) = residual {
                validate_expr_columns(res, &out)?;
            }
            Ok(out)
        }
        PhysicalOp::HashAggregate {
            input,
            group_keys,
            aggregates,
            having,
            ..
        }
        | PhysicalOp::StreamAggregate {
            input,
            group_keys,
            aggregates,
            having,
        } => {
            let available = validate_op(input)?;
            if group_keys.len() > MAX_GROUP_KEYS
                || aggregates.len() > MAX_AGGREGATES
                || (group_keys.is_empty() && aggregates.is_empty())
            {
                return Err(PlanError::InvalidColumnReference);
            }
            for k in group_keys {
                if !available.contains(k) {
                    return Err(PlanError::InvalidColumnReference);
                }
            }
            for a in aggregates {
                if let Some(arg) = &a.arg
                    && !available.contains(arg)
                {
                    return Err(PlanError::InvalidColumnReference);
                }
            }
            if let Some(h) = having {
                validate_expr_columns(h, &available)?;
            }
            Ok(group_keys.clone())
        }
        PhysicalOp::Sort {
            input, order_by, ..
        }
        | PhysicalOp::TopNHeap {
            input, order_by, ..
        } => {
            let available = validate_op(input)?;
            if order_by.is_empty() || order_by.len() > MAX_ORDER_ITEMS {
                return Err(PlanError::InvalidColumnReference);
            }
            for item in order_by {
                if let BoundOrderTarget::Column(c) = &item.target
                    && !available.contains(&c.column)
                {
                    return Err(PlanError::InvalidColumnReference);
                }
            }
            Ok(available)
        }
        PhysicalOp::Delete { relation, input } => {
            validate_relation_ref(relation)?;
            validate_op(input)
        }
        PhysicalOp::Update {
            relation,
            input,
            assignments,
        } => {
            validate_relation_ref(relation)?;
            let available = validate_op(input)?;
            if assignments.is_empty() || assignments.len() > MAX_UPDATE_ASSIGNMENTS {
                return Err(PlanError::InvalidColumnReference);
            }
            let mut seen = HashSet::new();
            for a in assignments {
                if a.column.column_id == [0; 16] || !seen.insert(a.column.column_id) {
                    return Err(PlanError::InvalidColumnReference);
                }
                if matches!(a.value, BoundUpdateValue::Null) && !a.column.nullable {
                    return Err(PlanError::InvalidColumnReference);
                }
            }
            Ok(available)
        }
    }
}
