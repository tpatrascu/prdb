//! Invocation-bound metadata resolution and type checking for the unified IR.

use super::expr::{BoundColumn, BoundColumnRef, BoundExpr, ColumnRef, Expr};
use super::logical::{
    AggregateKind, JoinType, LogicalOp, LogicalRequest, OrderTarget, RequestError, UpdateValue,
    collect_scan_relations,
};
use crate::metadata_discovery::{
    AuthorizedPlanRelation, AuthorizedRangeAccessPath, AuthorizedUniqueAccessPath,
};
use crate::transaction::{PlannerMetadataConnection, TransactionError};
use crate::tuple::ScalarType;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundRelation {
    pub relation_index: u16,
    pub relation_name: String,
    pub relation_id: [u8; 16],
    pub table_epoch: u64,
    pub columns: Vec<BoundColumn>,
    pub column_names: Vec<String>,
    pub unique_access_paths: Vec<AuthorizedUniqueAccessPath>,
    pub range_access_paths: Vec<AuthorizedRangeAccessPath>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundAggregateSpec {
    pub kind: AggregateKind,
    pub arg: Option<BoundColumnRef>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BoundOrderTarget {
    Column(BoundColumnRef),
    AggregateIndex(u16),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundOrderItem {
    pub target: BoundOrderTarget,
    pub descending: bool,
    pub nulls_first: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BoundUpdateValue {
    Parameter(u32),
    Null,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundUpdateAssignment {
    pub column: BoundColumn,
    pub value: BoundUpdateValue,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BoundLogicalOp {
    Scan {
        relation: BoundRelation,
    },
    Filter {
        input: Box<BoundLogicalOp>,
        predicate: BoundExpr,
    },
    Project {
        input: Box<BoundLogicalOp>,
        columns: Vec<BoundColumnRef>,
    },
    Join {
        join_type: JoinType,
        left: Box<BoundLogicalOp>,
        right: Box<BoundLogicalOp>,
        eq_keys: Vec<(BoundColumnRef, BoundColumnRef)>,
        residual: Option<BoundExpr>,
    },
    Aggregate {
        input: Box<BoundLogicalOp>,
        group_keys: Vec<BoundColumnRef>,
        aggregates: Vec<BoundAggregateSpec>,
        having: Option<BoundExpr>,
    },
    Sort {
        input: Box<BoundLogicalOp>,
        order_by: Vec<BoundOrderItem>,
    },
    Limit {
        input: Box<BoundLogicalOp>,
        count: u32,
    },
    Delete {
        relation: BoundRelation,
        predicate: BoundExpr,
    },
    Update {
        relation: BoundRelation,
        assignments: Vec<BoundUpdateAssignment>,
        predicate: BoundExpr,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundLogicalRequest {
    pub schema_name: String,
    pub schema_id: [u8; 16],
    pub schema_version: u64,
    pub root: BoundLogicalOp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BindError {
    Request(RequestError),
    RelationUnavailable,
    InvalidMetadata,
    InvalidQuery,
}

#[derive(Debug)]
pub enum MetadataBindError {
    Bind(BindError),
    Transaction(TransactionError),
}

impl From<RequestError> for BindError {
    fn from(err: RequestError) -> Self {
        Self::Request(err)
    }
}

impl From<BindError> for MetadataBindError {
    fn from(err: BindError) -> Self {
        Self::Bind(err)
    }
}

impl From<TransactionError> for MetadataBindError {
    fn from(err: TransactionError) -> Self {
        Self::Transaction(err)
    }
}

/// Bind a decoded `LogicalRequest` using `PlannerMetadataConnection`.
pub fn bind_logical_request_via_metadata(
    request_bytes: &[u8],
    connection: &mut PlannerMetadataConnection<'_, '_>,
) -> Result<BoundLogicalRequest, MetadataBindError> {
    let request = super::logical::decode_logical_request(request_bytes).map_err(BindError::from)?;
    let names = collect_scan_relations(&request);
    let mut relations = Vec::with_capacity(names.len());
    for (schema, rel) in names {
        relations.push(connection.describe_relation_for_plan(&schema, &rel)?);
    }
    bind_logical_request(&request, &relations).map_err(MetadataBindError::Bind)
}

/// Bind a `LogicalRequest` against pre-fetched `AuthorizedPlanRelation`s
/// ordered to match `collect_scan_relations(&request)`.
pub fn bind_logical_request(
    request: &LogicalRequest,
    relations: &[Option<AuthorizedPlanRelation>],
) -> Result<BoundLogicalRequest, BindError> {
    let expected = collect_scan_relations(request);
    if expected.len() != relations.len() {
        return Err(BindError::InvalidMetadata);
    }
    let mut cursor = 0usize;
    let mut schema_identity: Option<([u8; 16], u64)> = None;
    let mut seen_rel_indices = HashSet::new();
    let (bound_root, _) = bind_op(
        &request.root,
        &request.schema_name,
        relations,
        &mut cursor,
        &mut schema_identity,
        &mut seen_rel_indices,
    )?;
    let (schema_id, schema_version) = schema_identity.ok_or(BindError::InvalidMetadata)?;
    Ok(BoundLogicalRequest {
        schema_name: request.schema_name.clone(),
        schema_id,
        schema_version,
        root: bound_root,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExprType {
    Scalar(ScalarType),
    Null,
    Parameter,
}

fn bind_relation(
    relation_index: u16,
    relation_name: &str,
    relations: &[Option<AuthorizedPlanRelation>],
    cursor: &mut usize,
    schema_identity: &mut Option<([u8; 16], u64)>,
    seen_rel_indices: &mut HashSet<u16>,
) -> Result<BoundRelation, BindError> {
    if !seen_rel_indices.insert(relation_index) {
        return Err(BindError::InvalidQuery);
    }
    let auth = relations
        .get(*cursor)
        .and_then(|opt| opt.as_ref())
        .ok_or(BindError::RelationUnavailable)?;
    *cursor += 1;

    if auth.schema_id == [0; 16]
        || auth.schema_version == 0
        || auth.relation.relation_id == [0; 16]
        || auth.table_epoch == 0
        || !auth.relation.name.eq_ignore_ascii_case(relation_name)
        || auth.relation.columns.is_empty()
    {
        return Err(BindError::RelationUnavailable);
    }
    match schema_identity {
        None => *schema_identity = Some((auth.schema_id, auth.schema_version)),
        Some((sid, sver)) if *sid != auth.schema_id || *sver != auth.schema_version => {
            return Err(BindError::InvalidMetadata);
        }
        _ => {}
    }

    let mut columns = Vec::with_capacity(auth.relation.columns.len());
    let mut column_names = Vec::with_capacity(auth.relation.columns.len());
    let mut seen_ids = HashSet::new();
    let mut seen_ordinals = HashSet::new();
    for col in &auth.relation.columns {
        if col.column_id == [0; 16]
            || !seen_ids.insert(col.column_id)
            || !seen_ordinals.insert(col.ordinal)
        {
            return Err(BindError::InvalidMetadata);
        }
        columns.push(BoundColumn {
            column_id: col.column_id,
            ordinal: col.ordinal,
            scalar_type: col.scalar_type,
            nullable: col.nullable,
        });
        column_names.push(col.name.clone());
    }

    Ok(BoundRelation {
        relation_index,
        relation_name: auth.relation.name.clone(),
        relation_id: auth.relation.relation_id,
        table_epoch: auth.table_epoch,
        columns,
        column_names,
        unique_access_paths: auth.unique_access_paths.clone(),
        range_access_paths: auth.range_access_paths.clone(),
    })
}

fn lookup_column(
    rel_map: &HashMap<u16, &BoundRelation>,
    col_ref: &ColumnRef,
) -> Result<BoundColumnRef, BindError> {
    let rel = rel_map
        .get(&col_ref.relation_index)
        .ok_or(BindError::InvalidQuery)?;
    let mut matched = None;
    for (idx, name) in rel.column_names.iter().enumerate() {
        if name.eq_ignore_ascii_case(&col_ref.column_name) {
            if matched.is_some() {
                return Err(BindError::InvalidMetadata);
            }
            matched = Some(rel.columns[idx].clone());
        }
    }
    let column = matched.ok_or(BindError::InvalidQuery)?;
    Ok(BoundColumnRef {
        relation_index: col_ref.relation_index,
        column,
    })
}

fn bind_op(
    op: &LogicalOp,
    schema_name: &str,
    relations: &[Option<AuthorizedPlanRelation>],
    cursor: &mut usize,
    schema_identity: &mut Option<([u8; 16], u64)>,
    seen_rel_indices: &mut HashSet<u16>,
) -> Result<(BoundLogicalOp, Vec<BoundRelation>), BindError> {
    let _ = schema_name;
    match op {
        LogicalOp::Scan {
            relation_index,
            relation_name,
        } => {
            let rel = bind_relation(
                *relation_index,
                relation_name,
                relations,
                cursor,
                schema_identity,
                seen_rel_indices,
            )?;
            Ok((
                BoundLogicalOp::Scan {
                    relation: rel.clone(),
                },
                vec![rel],
            ))
        }
        LogicalOp::Filter { input, predicate } => {
            let (bound_input, scope_rels) = bind_op(
                input,
                schema_name,
                relations,
                cursor,
                schema_identity,
                seen_rel_indices,
            )?;
            let rel_map: HashMap<u16, &BoundRelation> =
                scope_rels.iter().map(|r| (r.relation_index, r)).collect();
            let (bound_pred, pred_ty) = bind_expr(predicate, &rel_map)?;
            if !is_boolean_root(pred_ty) {
                return Err(BindError::InvalidQuery);
            }
            Ok((
                BoundLogicalOp::Filter {
                    input: Box::new(bound_input),
                    predicate: bound_pred,
                },
                scope_rels,
            ))
        }
        LogicalOp::Project { input, columns } => {
            let (bound_input, scope_rels) = bind_op(
                input,
                schema_name,
                relations,
                cursor,
                schema_identity,
                seen_rel_indices,
            )?;
            let rel_map: HashMap<u16, &BoundRelation> =
                scope_rels.iter().map(|r| (r.relation_index, r)).collect();
            let mut bound_cols = Vec::with_capacity(columns.len());
            for col in columns {
                bound_cols.push(lookup_column(&rel_map, col)?);
            }
            Ok((
                BoundLogicalOp::Project {
                    input: Box::new(bound_input),
                    columns: bound_cols,
                },
                scope_rels,
            ))
        }
        LogicalOp::Join {
            join_type,
            left,
            right,
            eq_keys,
            residual,
        } => {
            let (bound_left, left_rels) = bind_op(
                left,
                schema_name,
                relations,
                cursor,
                schema_identity,
                seen_rel_indices,
            )?;
            let (bound_right, right_rels) = bind_op(
                right,
                schema_name,
                relations,
                cursor,
                schema_identity,
                seen_rel_indices,
            )?;
            let left_map: HashMap<u16, &BoundRelation> =
                left_rels.iter().map(|r| (r.relation_index, r)).collect();
            let right_map: HashMap<u16, &BoundRelation> =
                right_rels.iter().map(|r| (r.relation_index, r)).collect();
            let mut bound_keys = Vec::with_capacity(eq_keys.len());
            for (l_ref, r_ref) in eq_keys {
                let l_bound = lookup_column(&left_map, l_ref)?;
                let r_bound = lookup_column(&right_map, r_ref)?;
                if !join_key_types_compatible(
                    l_bound.column.scalar_type,
                    r_bound.column.scalar_type,
                ) {
                    return Err(BindError::InvalidQuery);
                }
                bound_keys.push((l_bound, r_bound));
            }
            let mut combined_rels = left_rels;
            if !matches!(join_type, JoinType::Semi | JoinType::Anti) {
                combined_rels.extend(right_rels);
            }
            let mut combined_map: HashMap<u16, &BoundRelation> = HashMap::new();
            for r in &combined_rels {
                combined_map.insert(r.relation_index, r);
            }
            let bound_residual = match residual {
                None => None,
                Some(expr) => {
                    let (b_expr, ty) = bind_expr(expr, &combined_map)?;
                    if !is_boolean_root(ty) {
                        return Err(BindError::InvalidQuery);
                    }
                    Some(b_expr)
                }
            };
            Ok((
                BoundLogicalOp::Join {
                    join_type: *join_type,
                    left: Box::new(bound_left),
                    right: Box::new(bound_right),
                    eq_keys: bound_keys,
                    residual: bound_residual,
                },
                combined_rels,
            ))
        }
        LogicalOp::Aggregate {
            input,
            group_keys,
            aggregates,
            having,
        } => {
            let (bound_input, scope_rels) = bind_op(
                input,
                schema_name,
                relations,
                cursor,
                schema_identity,
                seen_rel_indices,
            )?;
            let rel_map: HashMap<u16, &BoundRelation> =
                scope_rels.iter().map(|r| (r.relation_index, r)).collect();
            let mut seen_key_ids = HashSet::new();
            let mut bound_keys = Vec::with_capacity(group_keys.len());
            for key in group_keys {
                let b_key = lookup_column(&rel_map, key)?;
                if !seen_key_ids.insert((b_key.relation_index, b_key.column.column_id)) {
                    return Err(BindError::InvalidQuery);
                }
                bound_keys.push(b_key);
            }
            let mut bound_aggs = Vec::with_capacity(aggregates.len());
            for agg in aggregates {
                let bound_arg = match (&agg.kind, &agg.arg) {
                    (AggregateKind::CountStar, None) => None,
                    (AggregateKind::CountStar, Some(_)) => return Err(BindError::InvalidQuery),
                    (kind, Some(col_ref)) => {
                        let b_col = lookup_column(&rel_map, col_ref)?;
                        if matches!(kind, AggregateKind::Sum | AggregateKind::Avg)
                            && !is_numeric_scalar(b_col.column.scalar_type)
                        {
                            return Err(BindError::InvalidQuery);
                        }
                        Some(b_col)
                    }
                    (_, None) => return Err(BindError::InvalidQuery),
                };
                bound_aggs.push(BoundAggregateSpec {
                    kind: agg.kind,
                    arg: bound_arg,
                });
            }
            let bound_having = match having {
                None => None,
                Some(expr) => {
                    let (b_expr, ty) = bind_expr(expr, &rel_map)?;
                    if !is_boolean_root(ty) {
                        return Err(BindError::InvalidQuery);
                    }
                    Some(b_expr)
                }
            };
            Ok((
                BoundLogicalOp::Aggregate {
                    input: Box::new(bound_input),
                    group_keys: bound_keys,
                    aggregates: bound_aggs,
                    having: bound_having,
                },
                scope_rels,
            ))
        }
        LogicalOp::Sort { input, order_by } => {
            let (bound_input, scope_rels) = bind_op(
                input,
                schema_name,
                relations,
                cursor,
                schema_identity,
                seen_rel_indices,
            )?;
            let rel_map: HashMap<u16, &BoundRelation> =
                scope_rels.iter().map(|r| (r.relation_index, r)).collect();
            let agg_count = match &bound_input {
                BoundLogicalOp::Aggregate { aggregates, .. } => aggregates.len(),
                _ => 0,
            };
            let mut bound_items = Vec::with_capacity(order_by.len());
            for item in order_by {
                let target = match &item.target {
                    OrderTarget::Column(col_ref) => {
                        BoundOrderTarget::Column(lookup_column(&rel_map, col_ref)?)
                    }
                    OrderTarget::AggregateIndex(idx) => {
                        if usize::from(*idx) >= agg_count {
                            return Err(BindError::InvalidQuery);
                        }
                        BoundOrderTarget::AggregateIndex(*idx)
                    }
                };
                bound_items.push(BoundOrderItem {
                    target,
                    descending: item.descending,
                    nulls_first: item.nulls_first,
                });
            }
            Ok((
                BoundLogicalOp::Sort {
                    input: Box::new(bound_input),
                    order_by: bound_items,
                },
                scope_rels,
            ))
        }
        LogicalOp::Limit { input, count } => {
            let (bound_input, scope_rels) = bind_op(
                input,
                schema_name,
                relations,
                cursor,
                schema_identity,
                seen_rel_indices,
            )?;
            Ok((
                BoundLogicalOp::Limit {
                    input: Box::new(bound_input),
                    count: *count,
                },
                scope_rels,
            ))
        }
        LogicalOp::Delete {
            relation_index,
            relation_name,
            predicate,
        } => {
            let rel = bind_relation(
                *relation_index,
                relation_name,
                relations,
                cursor,
                schema_identity,
                seen_rel_indices,
            )?;
            let mut rel_map = HashMap::new();
            rel_map.insert(rel.relation_index, &rel);
            let (bound_pred, pred_ty) = bind_expr(predicate, &rel_map)?;
            if !is_boolean_root(pred_ty) {
                return Err(BindError::InvalidQuery);
            }
            Ok((
                BoundLogicalOp::Delete {
                    relation: rel.clone(),
                    predicate: bound_pred,
                },
                vec![rel],
            ))
        }
        LogicalOp::Update {
            relation_index,
            relation_name,
            assignments,
            predicate,
        } => {
            let rel = bind_relation(
                *relation_index,
                relation_name,
                relations,
                cursor,
                schema_identity,
                seen_rel_indices,
            )?;
            let mut rel_map = HashMap::new();
            rel_map.insert(rel.relation_index, &rel);
            let mut seen_targets = HashSet::new();
            let mut bound_assigns = Vec::with_capacity(assignments.len());
            for assign in assignments {
                let col_ref = ColumnRef {
                    relation_index: rel.relation_index,
                    column_name: assign.column_name.clone(),
                };
                let b_col = lookup_column(&rel_map, &col_ref)?;
                if !seen_targets.insert(b_col.column.column_id) {
                    return Err(BindError::InvalidQuery);
                }
                let val = match assign.value {
                    UpdateValue::Null => {
                        if !b_col.column.nullable {
                            return Err(BindError::InvalidQuery);
                        }
                        BoundUpdateValue::Null
                    }
                    UpdateValue::Parameter(idx) => BoundUpdateValue::Parameter(idx),
                };
                bound_assigns.push(BoundUpdateAssignment {
                    column: b_col.column,
                    value: val,
                });
            }
            let (bound_pred, pred_ty) = bind_expr(predicate, &rel_map)?;
            if !is_boolean_root(pred_ty) {
                return Err(BindError::InvalidQuery);
            }
            Ok((
                BoundLogicalOp::Update {
                    relation: rel.clone(),
                    assignments: bound_assigns,
                    predicate: bound_pred,
                },
                vec![rel],
            ))
        }
    }
}

fn bind_expr(
    expr: &Expr,
    rel_map: &HashMap<u16, &BoundRelation>,
) -> Result<(BoundExpr, ExprType), BindError> {
    match expr {
        Expr::Null => Ok((BoundExpr::Null, ExprType::Null)),
        Expr::Boolean(b) => Ok((
            BoundExpr::Boolean(*b),
            ExprType::Scalar(ScalarType::Boolean),
        )),
        Expr::Integer(i) => Ok((BoundExpr::Integer(*i), ExprType::Scalar(ScalarType::BigInt))),
        Expr::Text(s) => Ok((
            BoundExpr::Text(s.clone()),
            ExprType::Scalar(ScalarType::Text),
        )),
        Expr::Date(d) => Ok((BoundExpr::Date(*d), ExprType::Scalar(ScalarType::Date))),
        Expr::Uuid(u) => Ok((BoundExpr::Uuid(*u), ExprType::Scalar(ScalarType::Uuid))),
        Expr::TimestampTz(t) => Ok((
            BoundExpr::TimestampTz(*t),
            ExprType::Scalar(ScalarType::TimestampTz),
        )),
        Expr::Bytea(b) => Ok((
            BoundExpr::Bytea(b.clone()),
            ExprType::Scalar(ScalarType::Bytea),
        )),
        Expr::Decimal(d) => Ok((
            BoundExpr::Decimal(*d),
            ExprType::Scalar(ScalarType::Decimal {
                precision: 38,
                scale: 0,
            }),
        )),
        Expr::Column(col_ref) => {
            let b_col = lookup_column(rel_map, col_ref)?;
            let ty = b_col.column.scalar_type;
            Ok((
                BoundExpr::Column(super::expr::BoundColumnRef {
                    relation_index: b_col.relation_index,
                    column: b_col.column,
                }),
                ExprType::Scalar(ty),
            ))
        }
        Expr::Parameter(idx) => Ok((BoundExpr::Parameter(*idx), ExprType::Parameter)),
        Expr::Compare { op, left, right } => {
            let (b_left, l_ty) = bind_expr(left, rel_map)?;
            let (b_right, r_ty) = bind_expr(right, rel_map)?;
            if !expr_types_comparable(l_ty, r_ty, &b_left, &b_right) {
                return Err(BindError::InvalidQuery);
            }
            Ok((
                BoundExpr::Compare {
                    op: *op,
                    left: Box::new(b_left),
                    right: Box::new(b_right),
                },
                ExprType::Scalar(ScalarType::Boolean),
            ))
        }
        Expr::And(children) | Expr::Or(children) => {
            let mut b_children = Vec::with_capacity(children.len());
            for child in children {
                let (b_child, ty) = bind_expr(child, rel_map)?;
                if !is_boolean_root(ty) {
                    return Err(BindError::InvalidQuery);
                }
                b_children.push(b_child);
            }
            Ok((
                if matches!(expr, Expr::And(_)) {
                    BoundExpr::And(b_children)
                } else {
                    BoundExpr::Or(b_children)
                },
                ExprType::Scalar(ScalarType::Boolean),
            ))
        }
        Expr::Not(inner) => {
            let (b_inner, ty) = bind_expr(inner, rel_map)?;
            if !is_boolean_root(ty) {
                return Err(BindError::InvalidQuery);
            }
            Ok((
                BoundExpr::Not(Box::new(b_inner)),
                ExprType::Scalar(ScalarType::Boolean),
            ))
        }
        Expr::IsNull {
            expr: inner,
            negated,
        } => {
            let (b_inner, _) = bind_expr(inner, rel_map)?;
            Ok((
                BoundExpr::IsNull {
                    expr: Box::new(b_inner),
                    negated: *negated,
                },
                ExprType::Scalar(ScalarType::Boolean),
            ))
        }
        Expr::InList {
            expr: target,
            items,
            negated,
        } => {
            let (b_target, t_ty) = bind_expr(target, rel_map)?;
            let mut b_items = Vec::with_capacity(items.len());
            for item in items {
                let (b_item, i_ty) = bind_expr(item, rel_map)?;
                if !expr_types_comparable(t_ty, i_ty, &b_target, &b_item) {
                    return Err(BindError::InvalidQuery);
                }
                b_items.push(b_item);
            }
            Ok((
                BoundExpr::InList {
                    expr: Box::new(b_target),
                    items: b_items,
                    negated: *negated,
                },
                ExprType::Scalar(ScalarType::Boolean),
            ))
        }
    }
}

fn is_boolean_root(ty: ExprType) -> bool {
    matches!(
        ty,
        ExprType::Scalar(ScalarType::Boolean) | ExprType::Null | ExprType::Parameter
    )
}

fn is_numeric_scalar(scalar_type: ScalarType) -> bool {
    matches!(
        scalar_type,
        ScalarType::SmallInt
            | ScalarType::Integer
            | ScalarType::BigInt
            | ScalarType::Decimal { .. }
    )
}

pub(crate) fn join_key_types_compatible(left: ScalarType, right: ScalarType) -> bool {
    match (left, right) {
        (ScalarType::Text, ScalarType::Varchar { .. })
        | (ScalarType::Varchar { .. }, ScalarType::Text)
        | (ScalarType::Varchar { .. }, ScalarType::Varchar { .. }) => true,
        (ScalarType::Decimal { scale: ls, .. }, ScalarType::Decimal { scale: rs, .. }) => ls == rs,
        (
            ScalarType::SmallInt | ScalarType::Integer | ScalarType::BigInt,
            ScalarType::SmallInt | ScalarType::Integer | ScalarType::BigInt,
        ) => true,
        _ => left == right,
    }
}

fn expr_types_comparable(
    left_ty: ExprType,
    right_ty: ExprType,
    left_expr: &BoundExpr,
    right_expr: &BoundExpr,
) -> bool {
    match (left_ty, right_ty) {
        (ExprType::Null, _)
        | (_, ExprType::Null)
        | (ExprType::Parameter, _)
        | (_, ExprType::Parameter) => true,
        (
            ExprType::Scalar(ScalarType::Decimal { precision, .. }),
            ExprType::Scalar(ScalarType::Decimal { .. }),
        ) if matches!(right_expr, BoundExpr::Decimal(_)) => {
            if let BoundExpr::Decimal(val) = right_expr {
                crate::tuple::decimal_coefficient_is_valid(*val, precision)
            } else {
                true
            }
        }
        (
            ExprType::Scalar(ScalarType::Decimal { .. }),
            ExprType::Scalar(ScalarType::Decimal { precision, .. }),
        ) if matches!(left_expr, BoundExpr::Decimal(_)) => {
            if let BoundExpr::Decimal(val) = left_expr {
                crate::tuple::decimal_coefficient_is_valid(*val, precision)
            } else {
                true
            }
        }
        (ExprType::Scalar(l), ExprType::Scalar(r)) => join_key_types_compatible(l, r),
    }
}
