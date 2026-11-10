//! Deterministic single-table SELECT plan construction for optimizer v1.

use crate::optimizer_binding::{BoundColumn, BoundPredicate, BoundSelect};
use crate::tuple::ScalarType;
use std::collections::HashSet;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationReference {
    pub schema_name: String,
    pub schema_id: [u8; 16],
    pub schema_version: u64,
    pub relation_name: String,
    pub relation_id: [u8; 16],
    pub table_epoch: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlanNode {
    Scan { columns: Vec<BoundColumn> },
    Filter(BoundPredicate),
    Project(Vec<BoundColumn>),
    Limit(u32),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SingleTablePlanV1 {
    pub relation: RelationReference,
    /// Nodes form one linear pipeline in execution order.
    pub nodes: Vec<PlanNode>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlanError {
    EmptyProjection,
    InvalidRelationReference,
    InvalidColumnReference,
    InvalidPredicate,
    InvalidPipeline,
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "optimizer plan error: {self:?}")
    }
}

impl std::error::Error for PlanError {}

/// Lower a bound request to a conservative scan/filter/project/limit
/// pipeline. Scan columns are the unique union of projected and predicate
/// columns, ordered by catalog ordinal.
pub fn plan_select_v1(bound: BoundSelect) -> Result<SingleTablePlanV1, PlanError> {
    if bound.projections.is_empty() {
        return Err(PlanError::EmptyProjection);
    }
    let relation = RelationReference {
        schema_name: bound.schema_name,
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        relation_name: bound.relation_name,
        relation_id: bound.relation_id,
        table_epoch: bound.table_epoch,
    };
    validate_relation(&relation)?;

    let mut scan_columns = Vec::with_capacity(bound.projections.len());
    for column in &bound.projections {
        add_column(&mut scan_columns, column)?;
    }
    if let Some(predicate) = &bound.predicate {
        collect_predicate_columns(predicate, &mut scan_columns)?;
    }
    scan_columns.sort_by_key(|column| column.ordinal);

    let mut nodes = Vec::with_capacity(4);
    nodes.push(PlanNode::Scan {
        columns: scan_columns,
    });
    if let Some(predicate) = bound.predicate {
        nodes.push(PlanNode::Filter(predicate));
    }
    nodes.push(PlanNode::Project(bound.projections));
    if let Some(limit) = bound.limit {
        nodes.push(PlanNode::Limit(limit));
    }
    let plan = SingleTablePlanV1 { relation, nodes };
    validate_plan_v1(&plan)?;
    Ok(plan)
}

/// Validate the structural invariants an executor must check independently
/// before interpreting a decoded plan.
pub fn validate_plan_v1(plan: &SingleTablePlanV1) -> Result<(), PlanError> {
    validate_relation(&plan.relation)?;
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
    Ok(())
}

fn validate_relation(relation: &RelationReference) -> Result<(), PlanError> {
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

fn validate_columns(columns: &[BoundColumn]) -> Result<(), PlanError> {
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
    }
    if columns
        .windows(2)
        .any(|pair| pair[0].ordinal >= pair[1].ordinal)
    {
        return Err(PlanError::InvalidColumnReference);
    }
    Ok(())
}

fn validate_columns_allow_duplicates(columns: &[BoundColumn]) -> Result<(), PlanError> {
    if columns.is_empty()
        || columns.len() > crate::tuple::MAX_COLUMNS
        || columns.iter().any(|column| column.column_id == [0; 16])
    {
        return Err(PlanError::InvalidColumnReference);
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

fn add_column(columns: &mut Vec<BoundColumn>, column: &BoundColumn) -> Result<(), PlanError> {
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

fn collect_predicate_columns(
    predicate: &BoundPredicate,
    columns: &mut Vec<BoundColumn>,
) -> Result<(), PlanError> {
    match predicate {
        BoundPredicate::Column(column) => add_column(columns, column),
        BoundPredicate::Compare { left, right, .. } => {
            collect_predicate_columns(left, columns)?;
            collect_predicate_columns(right, columns)
        }
        BoundPredicate::And(children) | BoundPredicate::Or(children) => {
            for child in children {
                collect_predicate_columns(child, columns)?;
            }
            Ok(())
        }
        BoundPredicate::Not(child) => collect_predicate_columns(child, columns),
        BoundPredicate::Null
        | BoundPredicate::Boolean(_)
        | BoundPredicate::Integer(_)
        | BoundPredicate::Text(_) => Ok(()),
    }
}

fn predicate_type(
    predicate: &BoundPredicate,
    scan_columns: &[BoundColumn],
) -> Result<Option<ScalarType>, PlanError> {
    let mut nodes = 0;
    predicate_type_at_depth(predicate, scan_columns, 1, &mut nodes)
}

fn predicate_type_at_depth(
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
        BoundPredicate::Text(value)
            if value.len() <= crate::optimizer_request::MAX_TEXT_LITERAL_BYTES =>
        {
            Ok(Some(ScalarType::Text))
        }
        BoundPredicate::Text(_) => Err(PlanError::InvalidPredicate),
        BoundPredicate::Column(column) => {
            if scan_columns.iter().any(|scanned| scanned == column) {
                Ok(Some(column.scalar_type))
            } else {
                Err(PlanError::InvalidColumnReference)
            }
        }
        BoundPredicate::Compare { left, right, .. } => {
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
    }
}

fn valid_identifier(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 63
        && (bytes[0].is_ascii_alphabetic() || bytes[0] == b'_')
        && bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer_request::Comparison;
    use crate::tuple::ScalarType;

    fn column(id: u8, ordinal: u32, scalar_type: ScalarType) -> BoundColumn {
        BoundColumn {
            column_id: [id; 16],
            ordinal,
            scalar_type,
            nullable: false,
        }
    }

    fn bound_select() -> BoundSelect {
        let output = column(1, 0, ScalarType::Integer);
        let filter = column(2, 1, ScalarType::Boolean);
        BoundSelect {
            schema_name: "sales".to_owned(),
            schema_id: [0x10; 16],
            schema_version: 4,
            table_epoch: 6,
            relation_id: [0x20; 16],
            relation_name: "orders".to_owned(),
            projections: vec![output.clone()],
            predicate: Some(BoundPredicate::Compare {
                operator: Comparison::Equal,
                left: Box::new(BoundPredicate::Column(filter)),
                right: Box::new(BoundPredicate::Boolean(true)),
            }),
            limit: Some(5),
        }
    }

    #[test]
    fn lowers_filter_columns_into_scan_and_preserves_operator_order() {
        let plan = plan_select_v1(bound_select()).unwrap();
        assert_eq!(
            plan.relation,
            RelationReference {
                schema_name: "sales".to_owned(),
                schema_id: [0x10; 16],
                schema_version: 4,
                relation_name: "orders".to_owned(),
                relation_id: [0x20; 16],
                table_epoch: 6,
            }
        );
        assert_eq!(
            plan.nodes,
            [
                PlanNode::Scan {
                    columns: vec![
                        column(1, 0, ScalarType::Integer),
                        column(2, 1, ScalarType::Boolean),
                    ],
                },
                PlanNode::Filter(BoundPredicate::Compare {
                    operator: Comparison::Equal,
                    left: Box::new(BoundPredicate::Column(column(2, 1, ScalarType::Boolean))),
                    right: Box::new(BoundPredicate::Boolean(true)),
                }),
                PlanNode::Project(vec![column(1, 0, ScalarType::Integer)]),
                PlanNode::Limit(5),
            ]
        );
        assert_eq!(validate_plan_v1(&plan), Ok(()));
    }

    #[test]
    fn rejects_noncanonical_operator_order_and_duplicate_limits() {
        let mut plan = plan_select_v1(bound_select()).unwrap();
        plan.nodes.swap(1, 2);
        assert_eq!(validate_plan_v1(&plan), Err(PlanError::InvalidPipeline));

        let mut plan = plan_select_v1(bound_select()).unwrap();
        plan.nodes.pop();
        plan.nodes.push(PlanNode::Limit(5));
        plan.nodes.push(PlanNode::Limit(9));
        assert_eq!(validate_plan_v1(&plan), Err(PlanError::InvalidPipeline));
    }

    #[test]
    fn rejects_non_boolean_filters_and_unscanned_references() {
        let mut plan = plan_select_v1(bound_select()).unwrap();
        plan.nodes[1] = PlanNode::Filter(BoundPredicate::Integer(1));
        assert_eq!(validate_plan_v1(&plan), Err(PlanError::InvalidPredicate));

        let mut plan = plan_select_v1(bound_select()).unwrap();
        plan.nodes[1] = PlanNode::Filter(BoundPredicate::Compare {
            operator: Comparison::Equal,
            left: Box::new(BoundPredicate::Column(column(3, 2, ScalarType::Integer))),
            right: Box::new(BoundPredicate::Integer(1)),
        });
        assert_eq!(
            validate_plan_v1(&plan),
            Err(PlanError::InvalidColumnReference)
        );
    }

    #[test]
    fn plan_validator_reapplies_predicate_depth_budget() {
        let mut predicate = BoundPredicate::Boolean(true);
        for _ in 0..=crate::optimizer_request::MAX_PREDICATE_DEPTH {
            predicate = BoundPredicate::Not(Box::new(predicate));
        }
        let mut plan = plan_select_v1(bound_select()).unwrap();
        plan.nodes[1] = PlanNode::Filter(predicate);
        assert_eq!(validate_plan_v1(&plan), Err(PlanError::InvalidPredicate));
    }

    #[test]
    fn scan_contains_unique_sorted_union_of_output_and_filter_columns() {
        let mut select = bound_select();
        select.predicate = Some(BoundPredicate::Compare {
            operator: Comparison::Equal,
            left: Box::new(BoundPredicate::Column(column(1, 0, ScalarType::Integer))),
            right: Box::new(BoundPredicate::Integer(2)),
        });
        let plan = plan_select_v1(select).unwrap();
        let PlanNode::Scan { columns } = &plan.nodes[0] else {
            panic!("first node must scan")
        };
        assert_eq!(columns, &[column(1, 0, ScalarType::Integer)]);
    }
}
