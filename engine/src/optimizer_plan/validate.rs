use super::select_join::*;
use super::types::*;
use crate::optimizer_binding::{BoundColumn, BoundPredicate};
use crate::tuple::ScalarType;
use std::collections::HashSet;

pub(super) fn is_numeric_type(scalar_type: ScalarType) -> bool {
    matches!(
        scalar_type,
        ScalarType::SmallInt
            | ScalarType::Integer
            | ScalarType::BigInt
            | ScalarType::Decimal { .. }
    )
}

pub(super) fn join_key_types_compatible(left: ScalarType, right: ScalarType) -> bool {
    match (left, right) {
        (ScalarType::Decimal { scale: left, .. }, ScalarType::Decimal { scale: right, .. }) => {
            left == right
        }
        (left, right) => left == right,
    }
}

/// Validate the structural invariants an executor must check independently
/// before interpreting a decoded plan.
pub fn validate_plan_v1(plan: &SingleTablePlanV1) -> Result<(), PlanError> {
    validate_relation(&plan.relation)?;
    let hint_count = usize::from(plan.unique_probe.is_some())
        + usize::from(plan.range_scan.is_some())
        + usize::from(plan.multi_point_probe.is_some());
    if hint_count > 1 || (plan.empty_view_hint && hint_count != 0) {
        return Err(PlanError::InvalidPredicate);
    }
    if !(2..=4).contains(&plan.nodes.len()) {
        return Err(PlanError::InvalidPipeline);
    }
    let PlanNode::Scan { columns } = &plan.nodes[0] else {
        return Err(PlanError::InvalidPipeline);
    };
    validate_columns(columns)?;

    let mut index = 1;
    if matches!(plan.nodes.get(index), Some(PlanNode::Filter(_))) {
        index += 1;
    }
    let Some(PlanNode::Project(projections)) = plan.nodes.get(index) else {
        return Err(PlanError::InvalidPipeline);
    };
    if projections.is_empty() {
        return Err(PlanError::EmptyProjection);
    }
    validate_columns_allow_duplicates(projections)?;
    for projection in projections {
        if !columns.iter().any(|scanned| scanned == projection) {
            return Err(PlanError::InvalidColumnReference);
        }
    }
    index += 1;
    if matches!(plan.nodes.get(index), Some(PlanNode::Limit(_))) {
        index += 1;
    }
    if index != plan.nodes.len() {
        return Err(PlanError::InvalidPipeline);
    }
    if let Some(PlanNode::Filter(predicate)) = plan.nodes.get(1) {
        if predicate_type(predicate, columns)? != Some(ScalarType::Boolean) {
            return Err(PlanError::InvalidPredicate);
        }
        let mut refs = Vec::new();
        collect_predicate_columns(predicate, &mut refs)?;
        for reference in refs {
            if !columns.iter().any(|scanned| scanned == &reference) {
                return Err(PlanError::InvalidColumnReference);
            }
        }
    }
    if let Some(probe) = &plan.unique_probe {
        if probe.index_id == [0; 16]
            || !columns.iter().any(|column| column == &probe.key_column)
            || !is_point_probe_literal(&probe.key_column, &probe.value)
        {
            return Err(PlanError::InvalidPredicate);
        }
        let matches_filter = match plan.nodes.get(1) {
            Some(PlanNode::Filter(BoundPredicate::Compare {
                operator: crate::optimizer_request::Comparison::Equal,
                left,
                right,
            })) => {
                matches!(
                    (left.as_ref(), right.as_ref()),
                    (BoundPredicate::Column(column), value)
                        if column == &probe.key_column && value == &probe.value
                ) || matches!(
                    (left.as_ref(), right.as_ref()),
                    (value, BoundPredicate::Column(column))
                        if column == &probe.key_column && value == &probe.value
                )
            }
            _ => false,
        };
        if !matches_filter {
            return Err(PlanError::InvalidPredicate);
        }
    }
    if let Some(range) = &plan.range_scan {
        if range.index_id == [0; 16]
            || !columns.iter().any(|column| column == &range.key_column)
            || !is_supported_range_key(&range.key_column)
        {
            return Err(PlanError::InvalidPredicate);
        }
        let expected = match plan.nodes.get(1) {
            Some(PlanNode::Filter(predicate)) => index_range_bounds(predicate),
            _ => None,
        };
        if expected
            != Some((
                range.key_column.clone(),
                range.lower.clone(),
                range.upper.clone(),
            ))
        {
            return Err(PlanError::InvalidPredicate);
        }
    }
    if let Some(probe) = &plan.multi_point_probe {
        if probe.index_id == [0; 16]
            || !columns.iter().any(|column| column == &probe.key_column)
            || !(1..=64).contains(&probe.values.len())
            || probe.values.iter().enumerate().any(|(index, value)| {
                !is_point_probe_literal(&probe.key_column, value)
                    || probe.values[..index].contains(value)
            })
        {
            return Err(PlanError::InvalidPredicate);
        }
        let expected = match plan.nodes.get(1) {
            Some(PlanNode::Filter(predicate)) => select_multi_point_probe(
                predicate,
                &[crate::metadata_discovery::AuthorizedUniqueAccessPath {
                    index_id: probe.index_id,
                    column_ids: vec![probe.key_column.column_id],
                }],
                &[],
            ),
            _ => None,
        };
        if expected.as_ref() != Some(probe) {
            return Err(PlanError::InvalidPredicate);
        }
    }
    Ok(())
}

pub(super) fn is_supported_range_key(column: &BoundColumn) -> bool {
    matches!(
        column.scalar_type,
        ScalarType::Boolean | ScalarType::Integer | ScalarType::Text
    )
}

pub(super) fn is_supported_index_join_key(column: &BoundColumn) -> bool {
    valid_scalar_type(column.scalar_type)
}

pub(super) fn validate_relation(relation: &RelationReference) -> Result<(), PlanError> {
    if relation.schema_id == [0; 16]
        || relation.relation_id == [0; 16]
        || relation.schema_version == 0
        || relation.table_epoch == 0
        || !valid_identifier(&relation.schema_name)
        || !valid_identifier(&relation.relation_name)
    {
        return Err(PlanError::InvalidRelationReference);
    }
    Ok(())
}

pub(super) fn validate_columns(columns: &[BoundColumn]) -> Result<(), PlanError> {
    if columns.is_empty() || columns.len() > crate::tuple::MAX_COLUMNS {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut ids = HashSet::with_capacity(columns.len());
    let mut ordinals = HashSet::with_capacity(columns.len());
    for column in columns {
        if column.column_id == [0; 16]
            || !ids.insert(column.column_id)
            || !ordinals.insert(column.ordinal)
        {
            return Err(PlanError::InvalidColumnReference);
        }
        if !valid_scalar_type(column.scalar_type) {
            return Err(PlanError::InvalidScalarType);
        }
    }
    if columns
        .windows(2)
        .any(|pair| pair[0].ordinal >= pair[1].ordinal)
    {
        return Err(PlanError::InvalidColumnReference);
    }
    Ok(())
}

pub(super) fn validate_columns_allow_duplicates(columns: &[BoundColumn]) -> Result<(), PlanError> {
    if columns.is_empty()
        || columns.len() > crate::tuple::MAX_COLUMNS
        || columns.iter().any(|column| column.column_id == [0; 16])
    {
        return Err(PlanError::InvalidColumnReference);
    }
    if columns
        .iter()
        .any(|column| !valid_scalar_type(column.scalar_type))
    {
        return Err(PlanError::InvalidScalarType);
    }
    for (index, column) in columns.iter().enumerate() {
        if columns[index + 1..]
            .iter()
            .any(|later| later.ordinal == column.ordinal && later.column_id != column.column_id)
        {
            return Err(PlanError::InvalidColumnReference);
        }
    }
    Ok(())
}

pub(super) fn add_column(
    columns: &mut Vec<BoundColumn>,
    column: &BoundColumn,
) -> Result<(), PlanError> {
    if column.column_id == [0; 16] {
        return Err(PlanError::InvalidColumnReference);
    }
    if let Some(existing) = columns
        .iter()
        .find(|existing| existing.ordinal == column.ordinal)
    {
        return if existing == column {
            Ok(())
        } else {
            Err(PlanError::InvalidColumnReference)
        };
    }
    if columns
        .iter()
        .any(|existing| existing.column_id == column.column_id)
    {
        return Err(PlanError::InvalidColumnReference);
    }
    columns.push(column.clone());
    Ok(())
}

pub(super) fn collect_predicate_columns(
    predicate: &BoundPredicate,
    columns: &mut Vec<BoundColumn>,
) -> Result<(), PlanError> {
    let mut nodes = 0;
    collect_predicate_columns_at_depth(predicate, columns, 1, &mut nodes)
}

pub(super) fn collect_predicate_columns_at_depth(
    predicate: &BoundPredicate,
    columns: &mut Vec<BoundColumn>,
    depth: usize,
    nodes: &mut usize,
) -> Result<(), PlanError> {
    if depth > crate::optimizer_request::MAX_PREDICATE_DEPTH {
        return Err(PlanError::InvalidPredicate);
    }
    *nodes += 1;
    if *nodes > crate::optimizer_request::MAX_PREDICATE_NODES {
        return Err(PlanError::InvalidPredicate);
    }
    match predicate {
        BoundPredicate::Column(column) => add_column(columns, column),
        BoundPredicate::Compare { left, right, .. } => {
            collect_predicate_columns_at_depth(left, columns, depth + 1, nodes)?;
            collect_predicate_columns_at_depth(right, columns, depth + 1, nodes)
        }
        BoundPredicate::And(children) | BoundPredicate::Or(children) => {
            for child in children {
                collect_predicate_columns_at_depth(child, columns, depth + 1, nodes)?;
            }
            Ok(())
        }
        BoundPredicate::Not(child)
        | BoundPredicate::IsNull(child)
        | BoundPredicate::IsNotNull(child) => {
            collect_predicate_columns_at_depth(child, columns, depth + 1, nodes)
        }
        BoundPredicate::Null
        | BoundPredicate::Boolean(_)
        | BoundPredicate::Integer(_)
        | BoundPredicate::Text(_)
        | BoundPredicate::Bytea(_)
        | BoundPredicate::Date(_)
        | BoundPredicate::Uuid(_)
        | BoundPredicate::TimestampTz(_)
        | BoundPredicate::Decimal(_) => Ok(()),
    }
}

pub(super) fn predicate_type(
    predicate: &BoundPredicate,
    scan_columns: &[BoundColumn],
) -> Result<Option<ScalarType>, PlanError> {
    let mut nodes = 0;
    predicate_type_at_depth(predicate, scan_columns, 1, &mut nodes)
}

pub(super) fn predicate_type_at_depth(
    predicate: &BoundPredicate,
    scan_columns: &[BoundColumn],
    depth: usize,
    nodes: &mut usize,
) -> Result<Option<ScalarType>, PlanError> {
    if depth > crate::optimizer_request::MAX_PREDICATE_DEPTH {
        return Err(PlanError::InvalidPredicate);
    }
    *nodes += 1;
    if *nodes > crate::optimizer_request::MAX_PREDICATE_NODES {
        return Err(PlanError::InvalidPredicate);
    }
    match predicate {
        BoundPredicate::Null => Ok(None),
        BoundPredicate::Boolean(_) => Ok(Some(ScalarType::Boolean)),
        BoundPredicate::Integer(_) => Ok(Some(ScalarType::Integer)),
        BoundPredicate::Decimal(_) => Ok(None),
        BoundPredicate::Text(value)
            if value.len() <= crate::optimizer_request::MAX_TEXT_LITERAL_BYTES =>
        {
            Ok(Some(ScalarType::Text))
        }
        BoundPredicate::Text(_) => Err(PlanError::InvalidPredicate),
        BoundPredicate::Date(value) if crate::tuple::date_day_count_is_valid(*value) => {
            Ok(Some(ScalarType::Date))
        }
        BoundPredicate::Date(_) => Err(PlanError::InvalidPredicate),
        BoundPredicate::Uuid(_) => Ok(Some(ScalarType::Uuid)),
        BoundPredicate::Bytea(value)
            if value.len() <= crate::optimizer_request::MAX_TEXT_LITERAL_BYTES =>
        {
            Ok(Some(ScalarType::Bytea))
        }
        BoundPredicate::Bytea(_) => Err(PlanError::InvalidPredicate),
        BoundPredicate::TimestampTz(value) if crate::tuple::timestamp_micros_is_valid(*value) => {
            Ok(Some(ScalarType::TimestampTz))
        }
        BoundPredicate::TimestampTz(_) => Err(PlanError::InvalidPredicate),
        BoundPredicate::Column(column) => {
            if scan_columns.iter().any(|scanned| scanned == column) {
                Ok(Some(column.scalar_type))
            } else {
                Err(PlanError::InvalidColumnReference)
            }
        }
        BoundPredicate::Compare { left, right, .. } => {
            let decimal_context_valid = match (left.as_ref(), right.as_ref()) {
                (BoundPredicate::Decimal(value), BoundPredicate::Column(column))
                | (BoundPredicate::Column(column), BoundPredicate::Decimal(value)) => {
                    matches!(column.scalar_type, ScalarType::Decimal { precision, .. }
                        if crate::tuple::decimal_coefficient_is_valid(*value, precision))
                }
                (BoundPredicate::Decimal(_), BoundPredicate::Null)
                | (BoundPredicate::Null, BoundPredicate::Decimal(_)) => true,
                (BoundPredicate::Decimal(_), _) | (_, BoundPredicate::Decimal(_)) => false,
                _ => true,
            };
            if !decimal_context_valid {
                return Err(PlanError::InvalidPredicate);
            }
            let left = predicate_type_at_depth(left, scan_columns, depth + 1, nodes)?;
            let right = predicate_type_at_depth(right, scan_columns, depth + 1, nodes)?;
            if matches!((left, right), (Some(left), Some(right)) if left != right) {
                return Err(PlanError::InvalidPredicate);
            }
            Ok(Some(ScalarType::Boolean))
        }
        BoundPredicate::And(children) | BoundPredicate::Or(children) => {
            if !(2..=16).contains(&children.len()) {
                return Err(PlanError::InvalidPredicate);
            }
            for child in children {
                if predicate_type_at_depth(child, scan_columns, depth + 1, nodes)?
                    != Some(ScalarType::Boolean)
                {
                    return Err(PlanError::InvalidPredicate);
                }
            }
            Ok(Some(ScalarType::Boolean))
        }
        BoundPredicate::Not(child) => {
            if predicate_type_at_depth(child, scan_columns, depth + 1, nodes)?
                != Some(ScalarType::Boolean)
            {
                return Err(PlanError::InvalidPredicate);
            }
            Ok(Some(ScalarType::Boolean))
        }
        BoundPredicate::IsNull(child) | BoundPredicate::IsNotNull(child) => {
            if !matches!(child.as_ref(), BoundPredicate::Column(_))
                || predicate_type_at_depth(child, scan_columns, depth + 1, nodes)?.is_none()
            {
                return Err(PlanError::InvalidPredicate);
            }
            Ok(Some(ScalarType::Boolean))
        }
    }
}

pub(super) fn valid_identifier(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 63
        && (bytes[0].is_ascii_alphabetic() || bytes[0] == b'_')
        && bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
}

pub(super) fn valid_scalar_type(scalar_type: ScalarType) -> bool {
    match scalar_type {
        ScalarType::Decimal { precision, scale } => {
            (1..=38).contains(&precision) && scale <= precision
        }
        ScalarType::Varchar { max_chars } => max_chars != 0,
        _ => true,
    }
}
