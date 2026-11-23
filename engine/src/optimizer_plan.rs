//! Deterministic single-table SELECT plan construction for optimizer v1.

use crate::optimizer_binding::{
    BoundColumn, BoundCompositeGroupAggregate, BoundCompositeGroupCount,
    BoundFilteredCompositeGroupAggregate, BoundGroupAggregate, BoundGroupCount, BoundJoinFilter,
    BoundJoinSelect, BoundOrderedCompositeGroupAggregate, BoundOrderedCompositeGroupCount,
    BoundOrderedGroupAggregate, BoundOrderedGroupCount, BoundOrderedSelect, BoundPredicate,
    BoundSelect, BoundUpdateAssignment, BoundUpdateValue,
};
use crate::optimizer_request::JoinRelationSide;
use crate::optimizer_request::{CompositeGroupCountOrderItem, GroupCountOrderItem};
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
    /// Optional unique equality access hint. The executor validates the index
    /// and authorization again; this never grants access by itself.
    pub unique_probe: Option<UniquePointProbeV1>,
    /// Optional bounded leading-key range hint. Storage revalidates the index
    /// and authorization, and the executor retains the full predicate filter.
    pub range_scan: Option<IndexRangeScanV1>,
    /// Storage-proven empty authorized view, revalidated by the executor.
    pub empty_view_hint: bool,
}

/// Prefer a sequential scan over an index path for very small exact
/// authorized populations. The cutoff is conservative and based on a local
/// storage API profile; it is not an end-to-end SQL cost estimate.
pub const SMALL_POPULATION_SCAN_ROWS: u64 = 4;
const SKEWED_JOIN_OUTER_MAX_ROWS: u64 = 8;
const SKEWED_JOIN_INPUT_RATIO: u64 = 16;
/// Maximum row count probed for optimizer join orientation.
pub const JOIN_POPULATION_ESTIMATE_MAX_ROWS: u64 = 128;

pub fn apply_population_scan_heuristic(
    plan: &mut SingleTablePlanV1,
    visible_rows: Option<u64>,
) -> Result<bool, PlanError> {
    if visible_rows.is_some_and(|rows| rows > 0 && rows <= SMALL_POPULATION_SCAN_ROWS)
        && (plan.unique_probe.is_some() || plan.range_scan.is_some())
    {
        plan.unique_probe = None;
        plan.range_scan = None;
        validate_plan_v1(plan)?;
        return Ok(true);
    }
    Ok(false)
}

/// Prefer a sequential scan over a range index when the exact authorized
/// predicate estimate says at least half the visible rows match. This
/// conservative crossover is based on the local 130-row comparison; unique
/// point probes are unaffected.
pub fn apply_range_selectivity_scan_heuristic(
    plan: &mut SingleTablePlanV1,
    visible_rows: Option<u64>,
    matching_rows: Option<u64>,
) -> Result<bool, PlanError> {
    let (Some(visible_rows), Some(matching_rows)) = (visible_rows, matching_rows) else {
        return Ok(false);
    };
    if matching_rows > visible_rows {
        return Err(PlanError::InvalidPredicate);
    }
    let minimum_sequential_scan_matches = visible_rows / 2 + visible_rows % 2;
    if plan.range_scan.is_some()
        && visible_rows > 0
        && matching_rows >= minimum_sequential_scan_matches
    {
        plan.range_scan = None;
        validate_plan_v1(plan)?;
        return Ok(true);
    }
    Ok(false)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UniquePointProbeV1 {
    pub index_id: [u8; 16],
    pub key_column: BoundColumn,
    pub value: BoundPredicate,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexRangeScanV1 {
    pub index_id: [u8; 16],
    pub key_column: BoundColumn,
    pub lower: Option<IndexRangeBoundV1>,
    pub upper: Option<IndexRangeBoundV1>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexRangeBoundV1 {
    pub value: BoundPredicate,
    pub inclusive: bool,
}

/// Bounded DELETE plan carrying only the authorized logical relation, the
/// columns needed to evaluate its predicate, and its predicate tree.
/// Storage must still authorize every staged delete through its opaque row
/// handle and transaction policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeletePlanV1 {
    pub relation: RelationReference,
    pub scan_columns: Vec<BoundColumn>,
    pub predicate: BoundPredicate,
}

/// Bounded UPDATE plan with stable assignment targets and a predicate. The
/// plan carries no row handles, write grant, or executor capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdatePlanV1 {
    pub relation: RelationReference,
    pub scan_columns: Vec<BoundColumn>,
    pub predicate: BoundPredicate,
    pub assignments: Vec<BoundUpdateAssignment>,
}

/// Bounded two-relation equality join plan. Each relation is identified by
/// stable metadata and join/projection columns carry their bound shape.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JoinSelectPlanV1 {
    pub schema_name: String,
    pub schema_id: [u8; 16],
    pub schema_version: u64,
    pub left_relation: RelationReference,
    pub right_relation: RelationReference,
    pub left_scan_columns: Vec<BoundColumn>,
    pub right_scan_columns: Vec<BoundColumn>,
    pub projections: Vec<(JoinRelationSide, BoundColumn)>,
    pub join_keys: Vec<(BoundColumn, BoundColumn)>,
    pub limit: Option<u32>,
    pub filter: Option<BoundJoinFilter>,
    pub index_nested_loop: Option<IndexNestedLoopJoinV1>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexNestedLoopJoinV1 {
    pub outer_side: JoinRelationSide,
    pub inner_side: JoinRelationSide,
    pub index_id: [u8; 16],
    pub key_column: BoundColumn,
}

/// Bounded single-key COUNT(*) grouping plan. The scan carries only the
/// authorized key; COUNT(*) itself reads row cardinality, not another column.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupCountPlanV1 {
    pub relation: RelationReference,
    pub scan_columns: Vec<BoundColumn>,
    pub group_key: BoundColumn,
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupAggregatePlanV1 {
    pub relation: RelationReference,
    pub scan_columns: Vec<BoundColumn>,
    pub group_key: BoundColumn,
    pub aggregate_kind: u8,
    pub aggregate_column: Option<BoundColumn>,
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompositeGroupAggregatePlanV1 {
    pub relation: RelationReference,
    pub scan_columns: Vec<BoundColumn>,
    pub group_keys: Vec<BoundColumn>,
    pub aggregate_kind: u8,
    pub aggregate_column: Option<BoundColumn>,
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FilteredCompositeGroupAggregatePlanV1 {
    pub relation: RelationReference,
    pub scan_columns: Vec<BoundColumn>,
    pub group_keys: Vec<BoundColumn>,
    pub aggregate_kind: u8,
    pub aggregate_column: Option<BoundColumn>,
    pub predicate: BoundPredicate,
    pub order_items: Vec<CompositeGroupCountOrderItem>,
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderedCompositeGroupAggregatePlanV1 {
    pub aggregate_plan: CompositeGroupAggregatePlanV1,
    pub order_items: Vec<CompositeGroupCountOrderItem>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderedGroupAggregatePlanV1 {
    pub aggregate_plan: GroupAggregatePlanV1,
    pub order_items: Vec<GroupCountOrderItem>,
}

/// Bounded composite-key COUNT(*) grouping plan. Scan columns are sorted by
/// relation ordinal; group keys retain their SQL projection order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompositeGroupCountPlanV1 {
    pub relation: RelationReference,
    pub scan_columns: Vec<BoundColumn>,
    pub group_keys: Vec<BoundColumn>,
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderedGroupCountPlanV1 {
    pub relation: RelationReference,
    pub scan_columns: Vec<BoundColumn>,
    pub group_key: BoundColumn,
    pub order_items: Vec<GroupCountOrderItem>,
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderedCompositeGroupCountPlanV1 {
    pub relation: RelationReference,
    pub scan_columns: Vec<BoundColumn>,
    pub group_keys: Vec<BoundColumn>,
    pub order_items: Vec<CompositeGroupCountOrderItem>,
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderedSelectPlanV1 {
    pub relation: RelationReference,
    pub scan_columns: Vec<BoundColumn>,
    pub projections: Vec<BoundColumn>,
    pub order_key: BoundColumn,
    pub descending: bool,
    pub limit: Option<u32>,
}

pub const MAX_PLAN_BYTES: usize = 64 * 1024;
pub const MAX_PLAN_SCAN_COLUMNS: usize = crate::optimizer_request::MAX_PROJECTIONS * 2;
const PLAN_KIND_SELECT: u8 = 1;
const PLAN_KIND_DELETE: u8 = 2;
const PLAN_KIND_UPDATE: u8 = 3;
const PLAN_KIND_JOIN_SELECT: u8 = 4;
const PLAN_KIND_FILTERED_JOIN_SELECT: u8 = 12;
const PLAN_KIND_INDEX_NESTED_LOOP_JOIN: u8 = 17;
const DELETE_SINK_NODE: u8 = 5;
const PLAN_KIND_GROUP_COUNT: u8 = 5;
const PLAN_KIND_COMPOSITE_GROUP_COUNT: u8 = 6;
const PLAN_KIND_ORDERED_SELECT: u8 = 7;
const PLAN_KIND_ORDERED_GROUP_COUNT: u8 = 8;
const PLAN_KIND_ORDERED_COMPOSITE_GROUP_COUNT: u8 = 9;
const PLAN_KIND_GROUP_AGGREGATE: u8 = 10;
const PLAN_KIND_ORDERED_GROUP_AGGREGATE: u8 = 11;
const PLAN_KIND_COMPOSITE_GROUP_AGGREGATE: u8 = 13;
const PLAN_KIND_ORDERED_COMPOSITE_GROUP_AGGREGATE: u8 = 14;
const PLAN_KIND_FILTERED_COMPOSITE_GROUP_AGGREGATE: u8 = 15;
const PLAN_KIND_ORDERED_FILTERED_COMPOSITE_GROUP_AGGREGATE: u8 = 16;
const GROUP_COUNT_NODE: u8 = 7;
const COMPOSITE_GROUP_COUNT_NODE: u8 = 8;
const ORDER_BY_NODE: u8 = 9;
const ORDERED_COMPOSITE_GROUP_COUNT_NODE: u8 = 10;
const GROUP_AGGREGATE_NODE: u8 = 11;
const ORDERED_GROUP_AGGREGATE_NODE: u8 = 12;
const COMPOSITE_GROUP_AGGREGATE_NODE: u8 = 13;
const ORDERED_COMPOSITE_GROUP_AGGREGATE_NODE: u8 = 14;
const FILTERED_COMPOSITE_GROUP_AGGREGATE_NODE: u8 = 15;
const ORDERED_FILTERED_COMPOSITE_GROUP_AGGREGATE_NODE: u8 = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlanError {
    EmptyProjection,
    InvalidRelationReference,
    InvalidColumnReference,
    InvalidScalarType,
    InvalidPredicate,
    InvalidPipeline,
    Truncated,
    TooLarge,
    UnsupportedVersion(u16),
    InvalidEncoding,
    TrailingBytes,
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "optimizer plan error: {self:?}")
    }
}

impl std::error::Error for PlanError {}

/// Encode a validated SELECT plan as canonical version-1 bytes suitable for
/// transfer over a bounded optimizer/executor channel.
pub fn encode_plan_v1(plan: &SingleTablePlanV1) -> Result<Vec<u8>, PlanError> {
    validate_plan_v1(plan)?;
    let mut output = Vec::with_capacity(256);
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_header_record(
        1,
        PLAN_KIND_SELECT,
        0,
    ));
    put_name(&mut output, &plan.relation.schema_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_schema_reference_record(
            plan.relation.schema_id,
            plan.relation.schema_version,
        ),
    );
    put_name(&mut output, &plan.relation.relation_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_relation_reference_record(
            plan.relation.relation_id,
            plan.relation.table_epoch,
        ),
    );
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_node_count_record(
        u8::try_from(plan.nodes.len()).map_err(|_| PlanError::InvalidPipeline)?,
    ));
    for node in &plan.nodes {
        match node {
            PlanNode::Scan { columns } => {
                put_column_list_header(&mut output, 1, columns.len())?;
                for column in columns {
                    put_column(&mut output, column);
                }
            }
            PlanNode::Filter(predicate) => {
                output.extend_from_slice(
                    &crate::query_abi::encode_optimizer_plan_filter_node_record(2),
                );
                put_predicate(&mut output, predicate)?;
            }
            PlanNode::Project(columns) => {
                put_column_list_header(&mut output, 3, columns.len())?;
                for column in columns {
                    put_column(&mut output, column);
                }
            }
            PlanNode::Limit(limit) => {
                output.extend_from_slice(
                    &crate::query_abi::encode_optimizer_plan_limit_node_record(4, *limit),
                );
            }
        }
        if output.len() > MAX_PLAN_BYTES {
            return Err(PlanError::TooLarge);
        }
    }
    if plan.unique_probe.is_some() && plan.range_scan.is_some() {
        return Err(PlanError::InvalidPredicate);
    }
    if let Some(probe) = &plan.unique_probe {
        output.push(0xa1);
        output.extend_from_slice(&probe.index_id);
        put_column(&mut output, &probe.key_column);
        put_predicate(&mut output, &probe.value)?;
    } else if let Some(range) = &plan.range_scan {
        output.push(0xa2);
        output.extend_from_slice(&range.index_id);
        put_column(&mut output, &range.key_column);
        for bound in [&range.lower, &range.upper] {
            match bound {
                None => output.push(0),
                Some(bound) => {
                    output.push(if bound.inclusive { 1 } else { 2 });
                    put_predicate(&mut output, &bound.value)?;
                }
            }
        }
    }
    if plan.empty_view_hint {
        if plan.unique_probe.is_some() || plan.range_scan.is_some() {
            return Err(PlanError::InvalidPredicate);
        }
        output.push(0xa3);
    }
    if output.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    Ok(output)
}

/// Decode and independently validate a canonical optimizer v1 SELECT plan.
/// The executor must still resolve the logical relation through its own
/// invocation-bound storage connection before using it.
pub fn decode_plan_v1(bytes: &[u8]) -> Result<SingleTablePlanV1, PlanError> {
    if bytes.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    let mut reader = PlanReader { bytes, offset: 0 };
    let (version, kind, reserved) =
        crate::query_abi::decode_optimizer_plan_header_record(bytes).ok_or(PlanError::Truncated)?;
    if version != 1 {
        return Err(PlanError::UnsupportedVersion(version));
    }
    reader.offset = crate::query_abi::OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES as usize;
    if kind != PLAN_KIND_SELECT || reserved != 0 {
        return Err(PlanError::InvalidEncoding);
    }
    let schema_name = reader.name()?;
    let schema_reference = reader
        .take(crate::query_abi::OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES as usize)?;
    let (schema_id, schema_version) =
        crate::query_abi::decode_optimizer_plan_schema_reference_record(schema_reference)
            .ok_or(PlanError::Truncated)?;
    let relation_name = reader.name()?;
    let relation_reference = reader
        .take(crate::query_abi::OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES as usize)?;
    let (relation_id, table_epoch) =
        crate::query_abi::decode_optimizer_plan_relation_reference_record(relation_reference)
            .ok_or(PlanError::Truncated)?;
    let relation = RelationReference {
        schema_name,
        schema_id,
        schema_version,
        relation_name,
        relation_id,
        table_epoch,
    };
    let node_count = usize::from(reader.node_count()?);
    if !(2..=4).contains(&node_count) {
        return Err(PlanError::InvalidPipeline);
    }
    let mut nodes = Vec::with_capacity(node_count);
    let mut scan_columns = Vec::new();
    for index in 0..node_count {
        let tag_offset = reader.offset;
        let tag = reader.u8()?;
        let node = match tag {
            1 if index == 0 => {
                let count = reader.column_list_count(tag_offset, 1, MAX_PLAN_SCAN_COLUMNS)?;
                if count == 0 {
                    return Err(PlanError::InvalidColumnReference);
                }
                let mut columns = Vec::with_capacity(count);
                for _ in 0..count {
                    columns.push(reader.column()?);
                }
                scan_columns = columns.clone();
                PlanNode::Scan { columns }
            }
            2 => {
                reader.filter_node_tag(tag_offset)?;
                PlanNode::Filter(reader.predicate(&scan_columns)?)
            }
            3 => {
                let count = reader.column_list_count(
                    tag_offset,
                    3,
                    crate::optimizer_request::MAX_PROJECTIONS,
                )?;
                let mut columns = Vec::with_capacity(count);
                for _ in 0..count {
                    columns.push(reader.column()?);
                }
                PlanNode::Project(columns)
            }
            4 => PlanNode::Limit(reader.limit_node_value(tag_offset)?),
            _ => return Err(PlanError::InvalidPipeline),
        };
        nodes.push(node);
    }
    let mut empty_view_hint = false;
    let (unique_probe, range_scan) =
        if reader.is_empty() || reader.bytes.get(reader.offset) == Some(&0xa3) {
            (None, None)
        } else {
            let hint_tag = reader.u8()?;
            if hint_tag != 0xa1 && hint_tag != 0xa2 {
                return Err(PlanError::TrailingBytes);
            }
            let index_id: [u8; 16] = reader
                .take(16)?
                .try_into()
                .map_err(|_| PlanError::Truncated)?;
            let key_column = reader.column()?;
            match hint_tag {
                0xa1 => {
                    let value = reader.predicate(&scan_columns)?;
                    (
                        Some(UniquePointProbeV1 {
                            index_id,
                            key_column,
                            value,
                        }),
                        None,
                    )
                }
                0xa2 => {
                    let mut decode_bound = || -> Result<Option<IndexRangeBoundV1>, PlanError> {
                        match reader.u8()? {
                            0 => Ok(None),
                            1 | 2 => {
                                let inclusive = reader.bytes[reader.offset - 1] == 1;
                                Ok(Some(IndexRangeBoundV1 {
                                    value: reader.predicate(&scan_columns)?,
                                    inclusive,
                                }))
                            }
                            _ => Err(PlanError::InvalidEncoding),
                        }
                    };
                    (
                        None,
                        Some(IndexRangeScanV1 {
                            index_id,
                            key_column,
                            lower: decode_bound()?,
                            upper: decode_bound()?,
                        }),
                    )
                }
                _ => return Err(PlanError::TrailingBytes),
            }
        };
    if !reader.is_empty() && reader.bytes.get(reader.offset) == Some(&0xa3) {
        if unique_probe.is_some() || range_scan.is_some() {
            return Err(PlanError::InvalidEncoding);
        }
        reader.offset += 1;
        empty_view_hint = true;
    }
    if !reader.is_empty() {
        return Err(PlanError::TrailingBytes);
    }
    let plan = SingleTablePlanV1 {
        relation,
        nodes,
        unique_probe,
        range_scan,
        empty_view_hint,
    };
    validate_plan_v1(&plan)?;
    Ok(plan)
}

/// Encode an independently validated two-relation join plan. Unfiltered joins
/// use kind 4, filtered joins use kind 12, and authorized index nested-loop
/// joins use the additive kind 17.
pub fn encode_join_select_plan_v1(plan: &JoinSelectPlanV1) -> Result<Vec<u8>, PlanError> {
    validate_join_select_plan_v1(plan)?;
    let mut output = Vec::with_capacity(256);
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_header_record(
        1,
        if plan.index_nested_loop.is_some() {
            PLAN_KIND_INDEX_NESTED_LOOP_JOIN
        } else if plan.filter.is_some() {
            PLAN_KIND_FILTERED_JOIN_SELECT
        } else {
            PLAN_KIND_JOIN_SELECT
        },
        0,
    ));
    put_name(&mut output, &plan.schema_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_schema_reference_record(
            plan.schema_id,
            plan.schema_version,
        ),
    );
    for relation in [&plan.left_relation, &plan.right_relation] {
        put_name(&mut output, &relation.relation_name)?;
        output.extend_from_slice(
            &crate::query_abi::encode_optimizer_plan_relation_reference_record(
                relation.relation_id,
                relation.table_epoch,
            ),
        );
    }
    for columns in [&plan.left_scan_columns, &plan.right_scan_columns] {
        output.extend_from_slice(&(columns.len() as u16).to_be_bytes());
        for column in columns {
            put_column(&mut output, column);
        }
    }
    output.extend_from_slice(&(plan.projections.len() as u16).to_be_bytes());
    for (side, column) in &plan.projections {
        output.push(match side {
            JoinRelationSide::Left => 0,
            JoinRelationSide::Right => 1,
        });
        put_column(&mut output, column);
    }
    output.push(plan.join_keys.len() as u8);
    for (left, right) in &plan.join_keys {
        put_column(&mut output, left);
        put_column(&mut output, right);
    }
    match plan.limit {
        None => output.push(0),
        Some(limit) => {
            output.push(1);
            output.extend_from_slice(&limit.to_be_bytes());
        }
    }
    if plan.index_nested_loop.is_some() {
        output.push(u8::from(plan.filter.is_some()));
    }
    if let Some(filter) = &plan.filter {
        output.push(match filter.relation_side {
            JoinRelationSide::Left => 0,
            JoinRelationSide::Right => 1,
        });
        put_column(&mut output, &filter.column);
        match filter.expression {
            crate::optimizer_request::JoinFilterExpression::Parameter { operator, index } => {
                output.push(0);
                output.push(match operator {
                    crate::optimizer_request::Comparison::Equal => 0,
                    crate::optimizer_request::Comparison::NotEqual => 1,
                    crate::optimizer_request::Comparison::Less => 2,
                    crate::optimizer_request::Comparison::LessOrEqual => 3,
                    crate::optimizer_request::Comparison::Greater => 4,
                    crate::optimizer_request::Comparison::GreaterOrEqual => 5,
                });
                output.extend_from_slice(&index.to_be_bytes());
            }
            crate::optimizer_request::JoinFilterExpression::IsNull { negated } => {
                output.push(if negated { 2 } else { 1 });
            }
        }
    }
    if let Some(index_join) = &plan.index_nested_loop {
        output.push(match index_join.outer_side {
            JoinRelationSide::Left => 0,
            JoinRelationSide::Right => 1,
        });
        output.push(match index_join.inner_side {
            JoinRelationSide::Left => 0,
            JoinRelationSide::Right => 1,
        });
        output.extend_from_slice(&index_join.index_id);
        put_column(&mut output, &index_join.key_column);
    }
    if output.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    Ok(output)
}

/// Decode and independently validate canonical plan kind 4.
pub fn decode_join_select_plan_v1(bytes: &[u8]) -> Result<JoinSelectPlanV1, PlanError> {
    if bytes.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    let (version, kind, reserved) =
        crate::query_abi::decode_optimizer_plan_header_record(bytes).ok_or(PlanError::Truncated)?;
    if version != 1 {
        return Err(PlanError::UnsupportedVersion(version));
    }
    if !matches!(
        kind,
        PLAN_KIND_JOIN_SELECT | PLAN_KIND_FILTERED_JOIN_SELECT | PLAN_KIND_INDEX_NESTED_LOOP_JOIN
    ) || reserved != 0
    {
        return Err(PlanError::InvalidEncoding);
    }
    let mut reader = PlanReader {
        bytes,
        offset: crate::query_abi::OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES as usize,
    };
    let schema_name = reader.name()?;
    let schema_reference = reader
        .take(crate::query_abi::OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES as usize)?;
    let (schema_id, schema_version) =
        crate::query_abi::decode_optimizer_plan_schema_reference_record(schema_reference)
            .ok_or(PlanError::Truncated)?;
    let mut relation_references = Vec::with_capacity(2);
    for _ in 0..2 {
        let relation_name = reader.name()?;
        let reference = reader.take(
            crate::query_abi::OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES as usize,
        )?;
        let (relation_id, table_epoch) =
            crate::query_abi::decode_optimizer_plan_relation_reference_record(reference)
                .ok_or(PlanError::Truncated)?;
        relation_references.push(RelationReference {
            schema_name: schema_name.clone(),
            schema_id,
            schema_version,
            relation_name,
            relation_id,
            table_epoch,
        });
    }
    let mut scan_shapes = Vec::with_capacity(2);
    for _ in 0..2 {
        let count = usize::from(u16::from_be_bytes(
            reader
                .take(2)?
                .try_into()
                .map_err(|_| PlanError::Truncated)?,
        ));
        if count == 0 || count > MAX_PLAN_SCAN_COLUMNS {
            return Err(PlanError::InvalidColumnReference);
        }
        let mut columns = Vec::with_capacity(count);
        for _ in 0..count {
            columns.push(reader.column()?);
        }
        scan_shapes.push(columns);
    }
    let projection_count = usize::from(u16::from_be_bytes(
        reader
            .take(2)?
            .try_into()
            .map_err(|_| PlanError::Truncated)?,
    ));
    if !(1..=crate::optimizer_request::MAX_PROJECTIONS).contains(&projection_count) {
        return Err(PlanError::EmptyProjection);
    }
    let mut projections = Vec::with_capacity(projection_count);
    for _ in 0..projection_count {
        let side = match reader.u8()? {
            0 => JoinRelationSide::Left,
            1 => JoinRelationSide::Right,
            _ => return Err(PlanError::InvalidEncoding),
        };
        projections.push((side, reader.column()?));
    }
    let key_count = usize::from(reader.u8()?);
    if !(2..=8).contains(&key_count) {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut join_keys = Vec::with_capacity(key_count);
    for _ in 0..key_count {
        join_keys.push((reader.column()?, reader.column()?));
    }
    let limit = match reader.u8()? {
        0 => None,
        1 => Some(u32::from_be_bytes(
            reader
                .take(4)?
                .try_into()
                .map_err(|_| PlanError::Truncated)?,
        )),
        _ => return Err(PlanError::InvalidEncoding),
    };
    let has_filter = if kind == PLAN_KIND_INDEX_NESTED_LOOP_JOIN {
        match reader.u8()? {
            0 => false,
            1 => true,
            _ => return Err(PlanError::InvalidEncoding),
        }
    } else {
        kind == PLAN_KIND_FILTERED_JOIN_SELECT
    };
    let filter = if has_filter {
        let relation_side = match reader.u8()? {
            0 => JoinRelationSide::Left,
            1 => JoinRelationSide::Right,
            _ => return Err(PlanError::InvalidEncoding),
        };
        let column = reader.column()?;
        let expression = match reader.u8()? {
            0 => {
                let operator = match reader.u8()? {
                    0 => crate::optimizer_request::Comparison::Equal,
                    1 => crate::optimizer_request::Comparison::NotEqual,
                    2 => crate::optimizer_request::Comparison::Less,
                    3 => crate::optimizer_request::Comparison::LessOrEqual,
                    4 => crate::optimizer_request::Comparison::Greater,
                    5 => crate::optimizer_request::Comparison::GreaterOrEqual,
                    _ => return Err(PlanError::InvalidEncoding),
                };
                let index = u32::from_be_bytes(
                    reader
                        .take(4)?
                        .try_into()
                        .map_err(|_| PlanError::Truncated)?,
                );
                if index == 0 || index > crate::optimizer_request::MAX_PARAMETER_INDEX {
                    return Err(PlanError::InvalidEncoding);
                }
                crate::optimizer_request::JoinFilterExpression::Parameter { operator, index }
            }
            1 => crate::optimizer_request::JoinFilterExpression::IsNull { negated: false },
            2 => crate::optimizer_request::JoinFilterExpression::IsNull { negated: true },
            _ => return Err(PlanError::InvalidEncoding),
        };
        Some(BoundJoinFilter {
            relation_side,
            column,
            expression,
        })
    } else {
        None
    };
    let index_nested_loop = if kind == PLAN_KIND_INDEX_NESTED_LOOP_JOIN {
        let outer_side = match reader.u8()? {
            0 => JoinRelationSide::Left,
            1 => JoinRelationSide::Right,
            _ => return Err(PlanError::InvalidEncoding),
        };
        let inner_side = match reader.u8()? {
            0 => JoinRelationSide::Left,
            1 => JoinRelationSide::Right,
            _ => return Err(PlanError::InvalidEncoding),
        };
        let index_id = reader
            .take(16)?
            .try_into()
            .map_err(|_| PlanError::Truncated)?;
        Some(IndexNestedLoopJoinV1 {
            outer_side,
            inner_side,
            index_id,
            key_column: reader.column()?,
        })
    } else {
        None
    };
    if reader.offset != bytes.len() {
        return Err(PlanError::TrailingBytes);
    }
    let plan = JoinSelectPlanV1 {
        schema_name,
        schema_id,
        schema_version,
        left_relation: relation_references.remove(0),
        right_relation: relation_references.remove(0),
        left_scan_columns: scan_shapes.remove(0),
        right_scan_columns: scan_shapes.remove(0),
        projections,
        join_keys,
        limit,
        filter,
        index_nested_loop,
    };
    validate_join_select_plan_v1(&plan)?;
    Ok(plan)
}

/// Check relation identity, key shape/type compatibility, projection
/// membership, and all collection bounds before an executor interprets a
/// decoded join plan.
pub fn validate_join_select_plan_v1(plan: &JoinSelectPlanV1) -> Result<(), PlanError> {
    if plan.schema_id == [0; 16]
        || plan.schema_version == 0
        || !valid_identifier(&plan.schema_name)
        || plan.left_relation.schema_name != plan.schema_name
        || plan.right_relation.schema_name != plan.schema_name
        || plan.left_relation.schema_id != plan.schema_id
        || plan.right_relation.schema_id != plan.schema_id
        || plan.left_relation.schema_version != plan.schema_version
        || plan.right_relation.schema_version != plan.schema_version
    {
        return Err(PlanError::InvalidRelationReference);
    }
    validate_relation(&plan.left_relation)?;
    validate_relation(&plan.right_relation)?;
    if plan.projections.is_empty() {
        return Err(PlanError::EmptyProjection);
    }
    if plan.projections.len() > crate::optimizer_request::MAX_PROJECTIONS
        || !(2..=8).contains(&plan.join_keys.len())
    {
        return Err(PlanError::InvalidColumnReference);
    }
    validate_columns(&plan.left_scan_columns)?;
    validate_columns(&plan.right_scan_columns)?;
    for (side, column) in &plan.projections {
        if !valid_scalar_type(column.scalar_type) || column.column_id == [0; 16] {
            return Err(PlanError::InvalidScalarType);
        }
        let scan_columns = match side {
            JoinRelationSide::Left => &plan.left_scan_columns,
            JoinRelationSide::Right => &plan.right_scan_columns,
        };
        if !scan_columns.contains(column) {
            return Err(PlanError::InvalidColumnReference);
        }
    }
    for (left, right) in &plan.join_keys {
        if left.column_id == [0; 16]
            || right.column_id == [0; 16]
            || !valid_scalar_type(left.scalar_type)
            || !valid_scalar_type(right.scalar_type)
            || !join_key_types_compatible(left.scalar_type, right.scalar_type)
        {
            return Err(PlanError::InvalidColumnReference);
        }
        for (side, key) in [
            (JoinRelationSide::Left, left),
            (JoinRelationSide::Right, right),
        ] {
            let scan_columns = match side {
                JoinRelationSide::Left => &plan.left_scan_columns,
                JoinRelationSide::Right => &plan.right_scan_columns,
            };
            if !scan_columns.contains(key) {
                return Err(PlanError::InvalidColumnReference);
            }
        }
    }
    if let Some(filter) = &plan.filter {
        let scan_columns = match filter.relation_side {
            JoinRelationSide::Left => &plan.left_scan_columns,
            JoinRelationSide::Right => &plan.right_scan_columns,
        };
        if filter.column.column_id == [0; 16]
            || !valid_scalar_type(filter.column.scalar_type)
            || !scan_columns.contains(&filter.column)
        {
            return Err(PlanError::InvalidColumnReference);
        }
        if matches!(
            filter.expression,
            crate::optimizer_request::JoinFilterExpression::Parameter { index, .. }
            if index == 0 || index > crate::optimizer_request::MAX_PARAMETER_INDEX
        ) {
            return Err(PlanError::InvalidEncoding);
        }
    }
    if let Some(index_join) = &plan.index_nested_loop {
        if index_join.outer_side == index_join.inner_side
            || index_join.index_id == [0; 16]
            || !is_supported_index_join_key(&index_join.key_column)
        {
            return Err(PlanError::InvalidColumnReference);
        }
        let (inner_scan, matching_key) = match index_join.inner_side {
            JoinRelationSide::Left => (
                &plan.left_scan_columns,
                plan.join_keys
                    .iter()
                    .find(|(inner, _)| inner == &index_join.key_column),
            ),
            JoinRelationSide::Right => (
                &plan.right_scan_columns,
                plan.join_keys
                    .iter()
                    .find(|(_, inner)| inner == &index_join.key_column),
            ),
        };
        if !inner_scan.contains(&index_join.key_column) || matching_key.is_none() {
            return Err(PlanError::InvalidColumnReference);
        }
        let expected_outer = match index_join.inner_side {
            JoinRelationSide::Left => JoinRelationSide::Right,
            JoinRelationSide::Right => JoinRelationSide::Left,
        };
        if index_join.outer_side != expected_outer {
            return Err(PlanError::InvalidColumnReference);
        }
    }
    Ok(())
}

pub fn encode_group_count_plan_v1(plan: &GroupCountPlanV1) -> Result<Vec<u8>, PlanError> {
    validate_group_count_plan_v1(plan)?;
    let mut output = Vec::with_capacity(160);
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_header_record(
        1,
        PLAN_KIND_GROUP_COUNT,
        0,
    ));
    put_name(&mut output, &plan.relation.schema_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_schema_reference_record(
            plan.relation.schema_id,
            plan.relation.schema_version,
        ),
    );
    put_name(&mut output, &plan.relation.relation_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_relation_reference_record(
            plan.relation.relation_id,
            plan.relation.table_epoch,
        ),
    );
    output.extend_from_slice(&(plan.scan_columns.len() as u16).to_be_bytes());
    for column in &plan.scan_columns {
        put_column(&mut output, column);
    }
    output.push(GROUP_COUNT_NODE);
    put_column(&mut output, &plan.group_key);
    match plan.limit {
        None => output.push(0),
        Some(limit) => {
            output.push(1);
            output.extend_from_slice(&limit.to_be_bytes());
        }
    }
    if output.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    Ok(output)
}

pub fn decode_group_count_plan_v1(bytes: &[u8]) -> Result<GroupCountPlanV1, PlanError> {
    if bytes.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    let (version, kind, reserved) =
        crate::query_abi::decode_optimizer_plan_header_record(bytes).ok_or(PlanError::Truncated)?;
    if version != 1 {
        return Err(PlanError::UnsupportedVersion(version));
    }
    if kind != PLAN_KIND_GROUP_COUNT || reserved != 0 {
        return Err(PlanError::InvalidEncoding);
    }
    let mut reader = PlanReader {
        bytes,
        offset: crate::query_abi::OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES as usize,
    };
    let schema_name = reader.name()?;
    let schema_reference = reader
        .take(crate::query_abi::OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES as usize)?;
    let (schema_id, schema_version) =
        crate::query_abi::decode_optimizer_plan_schema_reference_record(schema_reference)
            .ok_or(PlanError::Truncated)?;
    let relation_name = reader.name()?;
    let relation_reference = reader
        .take(crate::query_abi::OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES as usize)?;
    let (relation_id, table_epoch) =
        crate::query_abi::decode_optimizer_plan_relation_reference_record(relation_reference)
            .ok_or(PlanError::Truncated)?;
    let scan_count = usize::from(u16::from_be_bytes(
        reader
            .take(2)?
            .try_into()
            .map_err(|_| PlanError::Truncated)?,
    ));
    if scan_count == 0 || scan_count > MAX_PLAN_SCAN_COLUMNS {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut scan_columns = Vec::with_capacity(scan_count);
    for _ in 0..scan_count {
        scan_columns.push(reader.column()?);
    }
    if reader.u8()? != GROUP_COUNT_NODE {
        return Err(PlanError::InvalidPipeline);
    }
    let group_key = reader.column()?;
    let limit = match reader.u8()? {
        0 => None,
        1 => Some(u32::from_be_bytes(
            reader
                .take(4)?
                .try_into()
                .map_err(|_| PlanError::Truncated)?,
        )),
        _ => return Err(PlanError::InvalidEncoding),
    };
    if reader.offset != bytes.len() {
        return Err(PlanError::TrailingBytes);
    }
    let plan = GroupCountPlanV1 {
        relation: RelationReference {
            schema_name,
            schema_id,
            schema_version,
            relation_name,
            relation_id,
            table_epoch,
        },
        scan_columns,
        group_key,
        limit,
    };
    validate_group_count_plan_v1(&plan)?;
    Ok(plan)
}

pub fn validate_group_count_plan_v1(plan: &GroupCountPlanV1) -> Result<(), PlanError> {
    validate_relation(&plan.relation)?;
    validate_columns(&plan.scan_columns)?;
    if plan.scan_columns != [plan.group_key.clone()]
        || plan.group_key.column_id == [0; 16]
        || !valid_scalar_type(plan.group_key.scalar_type)
    {
        return Err(PlanError::InvalidColumnReference);
    }
    Ok(())
}

pub fn plan_group_count_v1(bound: BoundGroupCount) -> Result<GroupCountPlanV1, PlanError> {
    let relation = RelationReference {
        schema_name: bound.schema_name,
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        relation_name: bound.relation_name,
        relation_id: bound.relation_id,
        table_epoch: bound.table_epoch,
    };
    let plan = GroupCountPlanV1 {
        relation,
        scan_columns: vec![bound.group_key.clone()],
        group_key: bound.group_key,
        limit: bound.limit,
    };
    validate_group_count_plan_v1(&plan)?;
    Ok(plan)
}

pub fn plan_group_aggregate_v1(
    bound: BoundGroupAggregate,
) -> Result<GroupAggregatePlanV1, PlanError> {
    let relation = RelationReference {
        schema_name: bound.schema_name,
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        relation_name: bound.relation_name,
        relation_id: bound.relation_id,
        table_epoch: bound.table_epoch,
    };
    let mut scan_columns = vec![bound.group_key.clone()];
    if let Some(column) = &bound.aggregate_column
        && !scan_columns
            .iter()
            .any(|existing| existing.ordinal == column.ordinal)
    {
        scan_columns.push(column.clone());
    }
    scan_columns.sort_by_key(|column| column.ordinal);
    let plan = GroupAggregatePlanV1 {
        relation,
        scan_columns,
        group_key: bound.group_key,
        aggregate_kind: bound.aggregate_kind,
        aggregate_column: bound.aggregate_column,
        limit: bound.limit,
    };
    validate_group_aggregate_plan_v1(&plan)?;
    Ok(plan)
}

pub fn encode_group_aggregate_plan_v1(plan: &GroupAggregatePlanV1) -> Result<Vec<u8>, PlanError> {
    validate_group_aggregate_plan_v1(plan)?;
    let mut output = Vec::with_capacity(224);
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_header_record(
        1,
        PLAN_KIND_GROUP_AGGREGATE,
        0,
    ));
    put_name(&mut output, &plan.relation.schema_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_schema_reference_record(
            plan.relation.schema_id,
            plan.relation.schema_version,
        ),
    );
    put_name(&mut output, &plan.relation.relation_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_relation_reference_record(
            plan.relation.relation_id,
            plan.relation.table_epoch,
        ),
    );
    output.extend_from_slice(&(plan.scan_columns.len() as u16).to_be_bytes());
    for column in &plan.scan_columns {
        put_column(&mut output, column);
    }
    output.extend_from_slice(&[GROUP_AGGREGATE_NODE, plan.aggregate_kind]);
    put_column(&mut output, &plan.group_key);
    match &plan.aggregate_column {
        None => output.push(0),
        Some(column) => {
            output.push(1);
            put_column(&mut output, column);
        }
    }
    match plan.limit {
        None => output.push(0),
        Some(limit) => {
            output.push(1);
            output.extend_from_slice(&limit.to_be_bytes());
        }
    }
    if output.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    Ok(output)
}

pub fn decode_group_aggregate_plan_v1(bytes: &[u8]) -> Result<GroupAggregatePlanV1, PlanError> {
    if bytes.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    let (version, kind, reserved) =
        crate::query_abi::decode_optimizer_plan_header_record(bytes).ok_or(PlanError::Truncated)?;
    if version != 1 {
        return Err(PlanError::UnsupportedVersion(version));
    }
    if kind != PLAN_KIND_GROUP_AGGREGATE || reserved != 0 {
        return Err(PlanError::InvalidEncoding);
    }
    let mut reader = PlanReader {
        bytes,
        offset: crate::query_abi::OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES as usize,
    };
    let schema_name = reader.name()?;
    let (schema_id, schema_version) =
        crate::query_abi::decode_optimizer_plan_schema_reference_record(reader.take(
            crate::query_abi::OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES as usize,
        )?)
        .ok_or(PlanError::Truncated)?;
    let relation_name = reader.name()?;
    let (relation_id, table_epoch) =
        crate::query_abi::decode_optimizer_plan_relation_reference_record(reader.take(
            crate::query_abi::OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES as usize,
        )?)
        .ok_or(PlanError::Truncated)?;
    let scan_count = usize::from(u16::from_be_bytes(
        reader
            .take(2)?
            .try_into()
            .map_err(|_| PlanError::Truncated)?,
    ));
    if !(1..=2).contains(&scan_count) {
        return Err(PlanError::InvalidPipeline);
    }
    let mut scan_columns = Vec::with_capacity(scan_count);
    for _ in 0..scan_count {
        scan_columns.push(reader.column()?);
    }
    if reader.u8()? != GROUP_AGGREGATE_NODE {
        return Err(PlanError::InvalidPipeline);
    }
    let aggregate_kind = reader.u8()?;
    let group_key = reader.column()?;
    let aggregate_column = match reader.u8()? {
        0 => None,
        1 => Some(reader.column()?),
        _ => return Err(PlanError::InvalidEncoding),
    };
    let limit = match reader.u8()? {
        0 => None,
        1 => Some(u32::from_be_bytes(
            reader
                .take(4)?
                .try_into()
                .map_err(|_| PlanError::Truncated)?,
        )),
        _ => return Err(PlanError::InvalidEncoding),
    };
    if reader.offset != bytes.len() {
        return Err(PlanError::TrailingBytes);
    }
    let plan = GroupAggregatePlanV1 {
        relation: RelationReference {
            schema_name,
            schema_id,
            schema_version,
            relation_name,
            relation_id,
            table_epoch,
        },
        scan_columns,
        group_key,
        aggregate_kind,
        aggregate_column,
        limit,
    };
    validate_group_aggregate_plan_v1(&plan)?;
    Ok(plan)
}

pub fn validate_group_aggregate_plan_v1(plan: &GroupAggregatePlanV1) -> Result<(), PlanError> {
    validate_relation(&plan.relation)?;
    validate_columns(&plan.scan_columns)?;
    if !(1..=6).contains(&plan.aggregate_kind)
        || plan.group_key.column_id == [0; 16]
        || !valid_scalar_type(plan.group_key.scalar_type)
    {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut expected = vec![plan.group_key.clone()];
    match (plan.aggregate_kind, &plan.aggregate_column) {
        (1, None) => {}
        (2..=6, Some(column))
            if column.column_id != [0; 16] && valid_scalar_type(column.scalar_type) =>
        {
            if (plan.aggregate_kind == 4 || plan.aggregate_kind == 5)
                && !is_numeric_type(column.scalar_type)
            {
                return Err(PlanError::InvalidColumnReference);
            }
            if let Some(existing) = expected.iter().find(|item| item.ordinal == column.ordinal) {
                if existing != column {
                    return Err(PlanError::InvalidColumnReference);
                }
            } else {
                expected.push(column.clone());
            }
        }
        _ => return Err(PlanError::InvalidColumnReference),
    }
    expected.sort_by_key(|column| column.ordinal);
    if expected != plan.scan_columns {
        return Err(PlanError::InvalidColumnReference);
    }
    Ok(())
}

pub fn plan_ordered_group_aggregate_v1(
    bound: BoundOrderedGroupAggregate,
) -> Result<OrderedGroupAggregatePlanV1, PlanError> {
    let aggregate_plan = plan_group_aggregate_v1(BoundGroupAggregate {
        schema_name: bound.schema_name,
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        table_epoch: bound.table_epoch,
        relation_id: bound.relation_id,
        relation_name: bound.relation_name,
        group_key: bound.group_key,
        aggregate_kind: bound.aggregate_kind,
        aggregate_column: bound.aggregate_column,
        limit: bound.limit,
    })?;
    let plan = OrderedGroupAggregatePlanV1 {
        aggregate_plan,
        order_items: bound.order_items,
    };
    validate_ordered_group_aggregate_plan_v1(&plan)?;
    Ok(plan)
}

pub fn validate_ordered_group_aggregate_plan_v1(
    plan: &OrderedGroupAggregatePlanV1,
) -> Result<(), PlanError> {
    validate_group_aggregate_plan_v1(&plan.aggregate_plan)?;
    if !(1..=2).contains(&plan.order_items.len()) {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut seen = 0u8;
    for item in &plan.order_items {
        if item.source > 1 || seen & (1 << item.source) != 0 {
            return Err(PlanError::InvalidColumnReference);
        }
        seen |= 1 << item.source;
    }
    Ok(())
}

pub fn encode_ordered_group_aggregate_plan_v1(
    plan: &OrderedGroupAggregatePlanV1,
) -> Result<Vec<u8>, PlanError> {
    validate_ordered_group_aggregate_plan_v1(plan)?;
    let aggregate = &plan.aggregate_plan;
    let mut output = Vec::with_capacity(240);
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_header_record(
        1,
        PLAN_KIND_ORDERED_GROUP_AGGREGATE,
        0,
    ));
    put_name(&mut output, &aggregate.relation.schema_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_schema_reference_record(
            aggregate.relation.schema_id,
            aggregate.relation.schema_version,
        ),
    );
    put_name(&mut output, &aggregate.relation.relation_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_relation_reference_record(
            aggregate.relation.relation_id,
            aggregate.relation.table_epoch,
        ),
    );
    output.extend_from_slice(&(aggregate.scan_columns.len() as u16).to_be_bytes());
    for column in &aggregate.scan_columns {
        put_column(&mut output, column);
    }
    output.extend_from_slice(&[ORDERED_GROUP_AGGREGATE_NODE, aggregate.aggregate_kind]);
    put_column(&mut output, &aggregate.group_key);
    match &aggregate.aggregate_column {
        None => output.push(0),
        Some(column) => {
            output.push(1);
            put_column(&mut output, column);
        }
    }
    output.push(plan.order_items.len() as u8);
    for item in &plan.order_items {
        output.extend_from_slice(&[
            item.source,
            u8::from(item.descending),
            u8::from(item.nulls_first),
        ]);
    }
    match aggregate.limit {
        None => output.push(0),
        Some(limit) => {
            output.push(1);
            output.extend_from_slice(&limit.to_be_bytes());
        }
    }
    if output.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    Ok(output)
}

pub fn decode_ordered_group_aggregate_plan_v1(
    bytes: &[u8],
) -> Result<OrderedGroupAggregatePlanV1, PlanError> {
    if bytes.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    let (version, kind, reserved) =
        crate::query_abi::decode_optimizer_plan_header_record(bytes).ok_or(PlanError::Truncated)?;
    if version != 1 {
        return Err(PlanError::UnsupportedVersion(version));
    }
    if kind != PLAN_KIND_ORDERED_GROUP_AGGREGATE || reserved != 0 {
        return Err(PlanError::InvalidEncoding);
    }
    let mut reader = PlanReader {
        bytes,
        offset: crate::query_abi::OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES as usize,
    };
    let schema_name = reader.name()?;
    let (schema_id, schema_version) =
        crate::query_abi::decode_optimizer_plan_schema_reference_record(reader.take(
            crate::query_abi::OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES as usize,
        )?)
        .ok_or(PlanError::Truncated)?;
    let relation_name = reader.name()?;
    let (relation_id, table_epoch) =
        crate::query_abi::decode_optimizer_plan_relation_reference_record(reader.take(
            crate::query_abi::OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES as usize,
        )?)
        .ok_or(PlanError::Truncated)?;
    let scan_count = usize::from(u16::from_be_bytes(
        reader
            .take(2)?
            .try_into()
            .map_err(|_| PlanError::Truncated)?,
    ));
    if !(1..=2).contains(&scan_count) {
        return Err(PlanError::InvalidPipeline);
    }
    let mut scan_columns = Vec::with_capacity(scan_count);
    for _ in 0..scan_count {
        scan_columns.push(reader.column()?);
    }
    if reader.u8()? != ORDERED_GROUP_AGGREGATE_NODE {
        return Err(PlanError::InvalidPipeline);
    }
    let aggregate_kind = reader.u8()?;
    let group_key = reader.column()?;
    let aggregate_column = match reader.u8()? {
        0 => None,
        1 => Some(reader.column()?),
        _ => return Err(PlanError::InvalidEncoding),
    };
    let order_count = usize::from(reader.u8()?);
    if !(1..=2).contains(&order_count) {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut order_items = Vec::with_capacity(order_count);
    for _ in 0..order_count {
        let source = reader.u8()?;
        let descending = match reader.u8()? {
            0 => false,
            1 => true,
            _ => return Err(PlanError::InvalidEncoding),
        };
        let nulls_first = match reader.u8()? {
            0 => false,
            1 => true,
            _ => return Err(PlanError::InvalidEncoding),
        };
        order_items.push(GroupCountOrderItem {
            source,
            descending,
            nulls_first,
        });
    }
    let limit = match reader.u8()? {
        0 => None,
        1 => Some(u32::from_be_bytes(
            reader
                .take(4)?
                .try_into()
                .map_err(|_| PlanError::Truncated)?,
        )),
        _ => return Err(PlanError::InvalidEncoding),
    };
    if reader.offset != bytes.len() {
        return Err(PlanError::TrailingBytes);
    }
    let plan = OrderedGroupAggregatePlanV1 {
        aggregate_plan: GroupAggregatePlanV1 {
            relation: RelationReference {
                schema_name,
                schema_id,
                schema_version,
                relation_name,
                relation_id,
                table_epoch,
            },
            scan_columns,
            group_key,
            aggregate_kind,
            aggregate_column,
            limit,
        },
        order_items,
    };
    validate_ordered_group_aggregate_plan_v1(&plan)?;
    Ok(plan)
}

fn is_numeric_type(scalar_type: ScalarType) -> bool {
    matches!(
        scalar_type,
        ScalarType::SmallInt
            | ScalarType::Integer
            | ScalarType::BigInt
            | ScalarType::Decimal { .. }
    )
}

pub fn plan_ordered_group_count_v1(
    bound: BoundOrderedGroupCount,
) -> Result<OrderedGroupCountPlanV1, PlanError> {
    let relation = RelationReference {
        schema_name: bound.schema_name,
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        relation_name: bound.relation_name,
        relation_id: bound.relation_id,
        table_epoch: bound.table_epoch,
    };
    let plan = OrderedGroupCountPlanV1 {
        relation,
        scan_columns: vec![bound.group_key.clone()],
        group_key: bound.group_key,
        order_items: bound.order_items,
        limit: bound.limit,
    };
    validate_ordered_group_count_plan_v1(&plan)?;
    Ok(plan)
}

pub fn encode_ordered_group_count_plan_v1(
    plan: &OrderedGroupCountPlanV1,
) -> Result<Vec<u8>, PlanError> {
    validate_ordered_group_count_plan_v1(plan)?;
    let mut output = Vec::with_capacity(192);
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_header_record(
        1,
        PLAN_KIND_ORDERED_GROUP_COUNT,
        0,
    ));
    put_name(&mut output, &plan.relation.schema_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_schema_reference_record(
            plan.relation.schema_id,
            plan.relation.schema_version,
        ),
    );
    put_name(&mut output, &plan.relation.relation_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_relation_reference_record(
            plan.relation.relation_id,
            plan.relation.table_epoch,
        ),
    );
    output.extend_from_slice(&(plan.scan_columns.len() as u16).to_be_bytes());
    for column in &plan.scan_columns {
        put_column(&mut output, column);
    }
    output.push(GROUP_COUNT_NODE);
    put_column(&mut output, &plan.group_key);
    output.push(plan.order_items.len() as u8);
    for item in &plan.order_items {
        output.extend_from_slice(&[
            item.source,
            u8::from(item.descending),
            u8::from(item.nulls_first),
        ]);
    }
    match plan.limit {
        None => output.push(0),
        Some(limit) => {
            output.push(1);
            output.extend_from_slice(&limit.to_be_bytes());
        }
    }
    if output.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    Ok(output)
}

pub fn decode_ordered_group_count_plan_v1(
    bytes: &[u8],
) -> Result<OrderedGroupCountPlanV1, PlanError> {
    if bytes.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    let (version, kind, reserved) =
        crate::query_abi::decode_optimizer_plan_header_record(bytes).ok_or(PlanError::Truncated)?;
    if version != 1 {
        return Err(PlanError::UnsupportedVersion(version));
    }
    if kind != PLAN_KIND_ORDERED_GROUP_COUNT || reserved != 0 {
        return Err(PlanError::InvalidEncoding);
    }
    let mut reader = PlanReader {
        bytes,
        offset: crate::query_abi::OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES as usize,
    };
    let schema_name = reader.name()?;
    let (schema_id, schema_version) =
        crate::query_abi::decode_optimizer_plan_schema_reference_record(reader.take(
            crate::query_abi::OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES as usize,
        )?)
        .ok_or(PlanError::Truncated)?;
    let relation_name = reader.name()?;
    let (relation_id, table_epoch) =
        crate::query_abi::decode_optimizer_plan_relation_reference_record(reader.take(
            crate::query_abi::OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES as usize,
        )?)
        .ok_or(PlanError::Truncated)?;
    let scan_count = usize::from(u16::from_be_bytes(
        reader
            .take(2)?
            .try_into()
            .map_err(|_| PlanError::Truncated)?,
    ));
    if scan_count != 1 {
        return Err(PlanError::InvalidPipeline);
    }
    let scan_columns = vec![reader.column()?];
    if reader.u8()? != GROUP_COUNT_NODE {
        return Err(PlanError::InvalidPipeline);
    }
    let group_key = reader.column()?;
    let order_count = usize::from(reader.u8()?);
    if !(1..=2).contains(&order_count) {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut order_items = Vec::with_capacity(order_count);
    for _ in 0..order_count {
        let source = reader.u8()?;
        let descending = match reader.u8()? {
            0 => false,
            1 => true,
            _ => return Err(PlanError::InvalidEncoding),
        };
        let nulls_first = match reader.u8()? {
            0 => false,
            1 => true,
            _ => return Err(PlanError::InvalidEncoding),
        };
        order_items.push(GroupCountOrderItem {
            source,
            descending,
            nulls_first,
        });
    }
    let limit = match reader.u8()? {
        0 => None,
        1 => Some(u32::from_be_bytes(
            reader
                .take(4)?
                .try_into()
                .map_err(|_| PlanError::Truncated)?,
        )),
        _ => return Err(PlanError::InvalidEncoding),
    };
    if reader.offset != bytes.len() {
        return Err(PlanError::TrailingBytes);
    }
    let plan = OrderedGroupCountPlanV1 {
        relation: RelationReference {
            schema_name,
            schema_id,
            schema_version,
            relation_name,
            relation_id,
            table_epoch,
        },
        scan_columns,
        group_key,
        order_items,
        limit,
    };
    validate_ordered_group_count_plan_v1(&plan)?;
    Ok(plan)
}

pub fn validate_ordered_group_count_plan_v1(
    plan: &OrderedGroupCountPlanV1,
) -> Result<(), PlanError> {
    validate_relation(&plan.relation)?;
    validate_columns(&plan.scan_columns)?;
    if plan.scan_columns != [plan.group_key.clone()]
        || plan.group_key.column_id == [0; 16]
        || !valid_scalar_type(plan.group_key.scalar_type)
        || !(1..=2).contains(&plan.order_items.len())
    {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut seen = 0u8;
    for item in &plan.order_items {
        if item.source > 1 || seen & (1 << item.source) != 0 {
            return Err(PlanError::InvalidColumnReference);
        }
        seen |= 1 << item.source;
    }
    Ok(())
}

pub fn plan_ordered_composite_group_count_v1(
    bound: BoundOrderedCompositeGroupCount,
) -> Result<OrderedCompositeGroupCountPlanV1, PlanError> {
    let relation = RelationReference {
        schema_name: bound.schema_name,
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        relation_name: bound.relation_name,
        relation_id: bound.relation_id,
        table_epoch: bound.table_epoch,
    };
    let mut scan_columns = bound.group_keys.clone();
    scan_columns.sort_by_key(|column| column.ordinal);
    let plan = OrderedCompositeGroupCountPlanV1 {
        relation,
        scan_columns,
        group_keys: bound.group_keys,
        order_items: bound.order_items,
        limit: bound.limit,
    };
    validate_ordered_composite_group_count_plan_v1(&plan)?;
    Ok(plan)
}

pub fn encode_ordered_composite_group_count_plan_v1(
    plan: &OrderedCompositeGroupCountPlanV1,
) -> Result<Vec<u8>, PlanError> {
    validate_ordered_composite_group_count_plan_v1(plan)?;
    let mut output = Vec::with_capacity(320);
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_header_record(
        1,
        PLAN_KIND_ORDERED_COMPOSITE_GROUP_COUNT,
        0,
    ));
    put_name(&mut output, &plan.relation.schema_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_schema_reference_record(
            plan.relation.schema_id,
            plan.relation.schema_version,
        ),
    );
    put_name(&mut output, &plan.relation.relation_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_relation_reference_record(
            plan.relation.relation_id,
            plan.relation.table_epoch,
        ),
    );
    output.extend_from_slice(&(plan.scan_columns.len() as u16).to_be_bytes());
    for column in &plan.scan_columns {
        put_column(&mut output, column);
    }
    output.push(ORDERED_COMPOSITE_GROUP_COUNT_NODE);
    output.push(plan.group_keys.len() as u8);
    for column in &plan.group_keys {
        put_column(&mut output, column);
    }
    output.push(plan.order_items.len() as u8);
    for item in &plan.order_items {
        output.extend_from_slice(&[
            item.source,
            item.group_key_index.unwrap_or(u8::MAX),
            u8::from(item.descending),
            u8::from(item.nulls_first),
        ]);
    }
    match plan.limit {
        None => output.push(0),
        Some(limit) => {
            output.push(1);
            output.extend_from_slice(&limit.to_be_bytes());
        }
    }
    if output.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    Ok(output)
}

pub fn decode_ordered_composite_group_count_plan_v1(
    bytes: &[u8],
) -> Result<OrderedCompositeGroupCountPlanV1, PlanError> {
    if bytes.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    let (version, kind, reserved) =
        crate::query_abi::decode_optimizer_plan_header_record(bytes).ok_or(PlanError::Truncated)?;
    if version != 1 {
        return Err(PlanError::UnsupportedVersion(version));
    }
    if kind != PLAN_KIND_ORDERED_COMPOSITE_GROUP_COUNT || reserved != 0 {
        return Err(PlanError::InvalidEncoding);
    }
    let mut reader = PlanReader {
        bytes,
        offset: crate::query_abi::OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES as usize,
    };
    let schema_name = reader.name()?;
    let (schema_id, schema_version) =
        crate::query_abi::decode_optimizer_plan_schema_reference_record(reader.take(
            crate::query_abi::OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES as usize,
        )?)
        .ok_or(PlanError::Truncated)?;
    let relation_name = reader.name()?;
    let (relation_id, table_epoch) =
        crate::query_abi::decode_optimizer_plan_relation_reference_record(reader.take(
            crate::query_abi::OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES as usize,
        )?)
        .ok_or(PlanError::Truncated)?;
    let scan_count = usize::from(u16::from_be_bytes(
        reader
            .take(2)?
            .try_into()
            .map_err(|_| PlanError::Truncated)?,
    ));
    if !(2..=8).contains(&scan_count) {
        return Err(PlanError::InvalidPipeline);
    }
    let mut scan_columns = Vec::with_capacity(scan_count);
    for _ in 0..scan_count {
        scan_columns.push(reader.column()?);
    }
    if reader.u8()? != ORDERED_COMPOSITE_GROUP_COUNT_NODE {
        return Err(PlanError::InvalidPipeline);
    }
    let group_count = usize::from(reader.u8()?);
    if !(2..=8).contains(&group_count) {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut group_keys = Vec::with_capacity(group_count);
    for _ in 0..group_count {
        group_keys.push(reader.column()?);
    }
    let order_count = usize::from(reader.u8()?);
    if !(1..=9).contains(&order_count) {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut order_items = Vec::with_capacity(order_count);
    for _ in 0..order_count {
        let source = reader.u8()?;
        let raw_index = reader.u8()?;
        let descending = match reader.u8()? {
            0 => false,
            1 => true,
            _ => return Err(PlanError::InvalidEncoding),
        };
        let nulls_first = match reader.u8()? {
            0 => false,
            1 => true,
            _ => return Err(PlanError::InvalidEncoding),
        };
        order_items.push(CompositeGroupCountOrderItem {
            source,
            group_key_index: (raw_index != u8::MAX).then_some(raw_index),
            descending,
            nulls_first,
        });
    }
    let limit = match reader.u8()? {
        0 => None,
        1 => Some(u32::from_be_bytes(
            reader
                .take(4)?
                .try_into()
                .map_err(|_| PlanError::Truncated)?,
        )),
        _ => return Err(PlanError::InvalidEncoding),
    };
    if reader.offset != bytes.len() {
        return Err(PlanError::TrailingBytes);
    }
    let plan = OrderedCompositeGroupCountPlanV1 {
        relation: RelationReference {
            schema_name,
            schema_id,
            schema_version,
            relation_name,
            relation_id,
            table_epoch,
        },
        scan_columns,
        group_keys,
        order_items,
        limit,
    };
    validate_ordered_composite_group_count_plan_v1(&plan)?;
    Ok(plan)
}

pub fn validate_ordered_composite_group_count_plan_v1(
    plan: &OrderedCompositeGroupCountPlanV1,
) -> Result<(), PlanError> {
    validate_relation(&plan.relation)?;
    validate_columns(&plan.scan_columns)?;
    if !(2..=8).contains(&plan.group_keys.len())
        || plan.scan_columns.len() != plan.group_keys.len()
        || !(1..=9).contains(&plan.order_items.len())
    {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut sorted = plan.group_keys.clone();
    sorted.sort_by_key(|column| column.ordinal);
    if sorted != plan.scan_columns
        || plan
            .group_keys
            .iter()
            .any(|column| column.column_id == [0; 16] || !valid_scalar_type(column.scalar_type))
    {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut seen_keys = HashSet::new();
    let mut count_seen = false;
    for item in &plan.order_items {
        match (item.source, item.group_key_index) {
            (0, Some(index))
                if usize::from(index) < plan.group_keys.len() && seen_keys.insert(index) => {}
            (1, None) if !count_seen => count_seen = true,
            _ => return Err(PlanError::InvalidColumnReference),
        }
    }
    Ok(())
}

pub fn encode_composite_group_count_plan_v1(
    plan: &CompositeGroupCountPlanV1,
) -> Result<Vec<u8>, PlanError> {
    validate_composite_group_count_plan_v1(plan)?;
    let mut output = Vec::with_capacity(256);
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_header_record(
        1,
        PLAN_KIND_COMPOSITE_GROUP_COUNT,
        0,
    ));
    put_name(&mut output, &plan.relation.schema_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_schema_reference_record(
            plan.relation.schema_id,
            plan.relation.schema_version,
        ),
    );
    put_name(&mut output, &plan.relation.relation_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_relation_reference_record(
            plan.relation.relation_id,
            plan.relation.table_epoch,
        ),
    );
    output.extend_from_slice(&(plan.scan_columns.len() as u16).to_be_bytes());
    for column in &plan.scan_columns {
        put_column(&mut output, column);
    }
    output.push(COMPOSITE_GROUP_COUNT_NODE);
    output.push(plan.group_keys.len() as u8);
    for key in &plan.group_keys {
        put_column(&mut output, key);
    }
    match plan.limit {
        None => output.push(0),
        Some(limit) => {
            output.push(1);
            output.extend_from_slice(&limit.to_be_bytes());
        }
    }
    if output.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    Ok(output)
}

pub fn decode_composite_group_count_plan_v1(
    bytes: &[u8],
) -> Result<CompositeGroupCountPlanV1, PlanError> {
    if bytes.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    let (version, kind, reserved) =
        crate::query_abi::decode_optimizer_plan_header_record(bytes).ok_or(PlanError::Truncated)?;
    if version != 1 {
        return Err(PlanError::UnsupportedVersion(version));
    }
    if kind != PLAN_KIND_COMPOSITE_GROUP_COUNT || reserved != 0 {
        return Err(PlanError::InvalidEncoding);
    }
    let mut reader = PlanReader {
        bytes,
        offset: crate::query_abi::OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES as usize,
    };
    let schema_name = reader.name()?;
    let (schema_id, schema_version) =
        crate::query_abi::decode_optimizer_plan_schema_reference_record(reader.take(
            crate::query_abi::OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES as usize,
        )?)
        .ok_or(PlanError::Truncated)?;
    let relation_name = reader.name()?;
    let (relation_id, table_epoch) =
        crate::query_abi::decode_optimizer_plan_relation_reference_record(reader.take(
            crate::query_abi::OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES as usize,
        )?)
        .ok_or(PlanError::Truncated)?;
    let scan_count = usize::from(u16::from_be_bytes(
        reader
            .take(2)?
            .try_into()
            .map_err(|_| PlanError::Truncated)?,
    ));
    if !(crate::optimizer_request::MIN_GROUP_KEYS..=crate::optimizer_request::MAX_GROUP_KEYS)
        .contains(&scan_count)
    {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut scan_columns = Vec::with_capacity(scan_count);
    for _ in 0..scan_count {
        scan_columns.push(reader.column()?);
    }
    if reader.u8()? != COMPOSITE_GROUP_COUNT_NODE {
        return Err(PlanError::InvalidPipeline);
    }
    let key_count = usize::from(reader.u8()?);
    if !(crate::optimizer_request::MIN_GROUP_KEYS..=crate::optimizer_request::MAX_GROUP_KEYS)
        .contains(&key_count)
    {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut group_keys = Vec::with_capacity(key_count);
    for _ in 0..key_count {
        group_keys.push(reader.column()?);
    }
    let limit = match reader.u8()? {
        0 => None,
        1 => Some(u32::from_be_bytes(
            reader
                .take(4)?
                .try_into()
                .map_err(|_| PlanError::Truncated)?,
        )),
        _ => return Err(PlanError::InvalidEncoding),
    };
    if reader.offset != bytes.len() {
        return Err(PlanError::TrailingBytes);
    }
    let plan = CompositeGroupCountPlanV1 {
        relation: RelationReference {
            schema_name,
            schema_id,
            schema_version,
            relation_name,
            relation_id,
            table_epoch,
        },
        scan_columns,
        group_keys,
        limit,
    };
    validate_composite_group_count_plan_v1(&plan)?;
    Ok(plan)
}

pub fn validate_composite_group_count_plan_v1(
    plan: &CompositeGroupCountPlanV1,
) -> Result<(), PlanError> {
    validate_relation(&plan.relation)?;
    validate_columns(&plan.scan_columns)?;
    if !(crate::optimizer_request::MIN_GROUP_KEYS..=crate::optimizer_request::MAX_GROUP_KEYS)
        .contains(&plan.group_keys.len())
        || plan
            .group_keys
            .iter()
            .any(|key| key.column_id == [0; 16] || !valid_scalar_type(key.scalar_type))
    {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut unique = std::collections::HashSet::with_capacity(plan.group_keys.len());
    if plan
        .group_keys
        .iter()
        .any(|key| !unique.insert(key.ordinal) || !plan.scan_columns.iter().any(|scan| scan == key))
        || unique.len() != plan.scan_columns.len()
    {
        return Err(PlanError::InvalidColumnReference);
    }
    Ok(())
}

pub fn plan_composite_group_count_v1(
    bound: BoundCompositeGroupCount,
) -> Result<CompositeGroupCountPlanV1, PlanError> {
    let relation = RelationReference {
        schema_name: bound.schema_name,
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        table_epoch: bound.table_epoch,
        relation_id: bound.relation_id,
        relation_name: bound.relation_name,
    };
    let mut scan_columns = bound.group_keys.clone();
    scan_columns.sort_by_key(|column| column.ordinal);
    let plan = CompositeGroupCountPlanV1 {
        relation,
        scan_columns,
        group_keys: bound.group_keys,
        limit: bound.limit,
    };
    validate_composite_group_count_plan_v1(&plan)?;
    Ok(plan)
}

pub fn plan_composite_group_aggregate_v1(
    bound: BoundCompositeGroupAggregate,
) -> Result<CompositeGroupAggregatePlanV1, PlanError> {
    let relation = RelationReference {
        schema_name: bound.schema_name,
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        table_epoch: bound.table_epoch,
        relation_id: bound.relation_id,
        relation_name: bound.relation_name,
    };
    let mut scan_columns = bound.group_keys.clone();
    if let Some(column) = &bound.aggregate_column
        && !scan_columns.iter().any(|key| key == column)
    {
        scan_columns.push(column.clone());
    }
    scan_columns.sort_by_key(|column| column.ordinal);
    let plan = CompositeGroupAggregatePlanV1 {
        relation,
        scan_columns,
        group_keys: bound.group_keys,
        aggregate_kind: bound.aggregate_kind,
        aggregate_column: bound.aggregate_column,
        limit: bound.limit,
    };
    validate_composite_group_aggregate_plan_v1(&plan)?;
    Ok(plan)
}

pub fn plan_filtered_composite_group_aggregate_v1(
    bound: BoundFilteredCompositeGroupAggregate,
) -> Result<FilteredCompositeGroupAggregatePlanV1, PlanError> {
    let relation = RelationReference {
        schema_name: bound.schema_name,
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        table_epoch: bound.table_epoch,
        relation_id: bound.relation_id,
        relation_name: bound.relation_name,
    };
    let mut scan_columns = bound.group_keys.clone();
    if let Some(column) = &bound.aggregate_column {
        add_column(&mut scan_columns, column)?;
    }
    collect_predicate_columns(&bound.predicate, &mut scan_columns)?;
    scan_columns.sort_by_key(|column| column.ordinal);
    let plan = FilteredCompositeGroupAggregatePlanV1 {
        relation,
        scan_columns,
        group_keys: bound.group_keys,
        aggregate_kind: bound.aggregate_kind,
        aggregate_column: bound.aggregate_column,
        predicate: bound.predicate,
        order_items: bound.order_items,
        limit: bound.limit,
    };
    validate_filtered_composite_group_aggregate_plan_v1(&plan)?;
    Ok(plan)
}

pub fn validate_filtered_composite_group_aggregate_plan_v1(
    plan: &FilteredCompositeGroupAggregatePlanV1,
) -> Result<(), PlanError> {
    validate_relation(&plan.relation)?;
    validate_columns(&plan.scan_columns)?;
    if !(1..=crate::optimizer_request::MAX_GROUP_KEYS).contains(&plan.group_keys.len())
        || !(1..=6).contains(&plan.aggregate_kind)
        || plan
            .group_keys
            .iter()
            .any(|key| key.column_id == [0; 16] || !valid_scalar_type(key.scalar_type))
    {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut expected = plan.group_keys.clone();
    let mut seen = HashSet::with_capacity(plan.group_keys.len());
    if plan.group_keys.iter().any(|key| !seen.insert(key.ordinal)) {
        return Err(PlanError::InvalidColumnReference);
    }
    match (plan.aggregate_kind, &plan.aggregate_column) {
        (1, None) => {}
        (2..=6, Some(column))
            if column.column_id != [0; 16] && valid_scalar_type(column.scalar_type) =>
        {
            if matches!(plan.aggregate_kind, 4 | 5) && !is_numeric_type(column.scalar_type) {
                return Err(PlanError::InvalidColumnReference);
            }
            add_column(&mut expected, column)?;
        }
        _ => return Err(PlanError::InvalidColumnReference),
    }
    collect_predicate_columns(&plan.predicate, &mut expected)?;
    if !plan.order_items.is_empty()
        && !(1..=plan.group_keys.len() + 1).contains(&plan.order_items.len())
    {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut seen_order_keys = HashSet::new();
    let mut aggregate_seen = false;
    for item in &plan.order_items {
        match (item.source, item.group_key_index) {
            (0, Some(index))
                if usize::from(index) < plan.group_keys.len() && seen_order_keys.insert(index) => {}
            (1, None) if !aggregate_seen => aggregate_seen = true,
            _ => return Err(PlanError::InvalidColumnReference),
        }
    }
    expected.sort_by_key(|column| column.ordinal);
    if expected != plan.scan_columns
        || predicate_type(&plan.predicate, &plan.scan_columns)? != Some(ScalarType::Boolean)
    {
        return Err(PlanError::InvalidColumnReference);
    }
    Ok(())
}

pub fn validate_composite_group_aggregate_plan_v1(
    plan: &CompositeGroupAggregatePlanV1,
) -> Result<(), PlanError> {
    validate_relation(&plan.relation)?;
    validate_columns(&plan.scan_columns)?;
    if !(crate::optimizer_request::MIN_GROUP_KEYS..=crate::optimizer_request::MAX_GROUP_KEYS)
        .contains(&plan.group_keys.len())
        || !(1..=6).contains(&plan.aggregate_kind)
        || plan
            .group_keys
            .iter()
            .any(|key| key.column_id == [0; 16] || !valid_scalar_type(key.scalar_type))
    {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut expected = plan.group_keys.clone();
    let mut seen = HashSet::with_capacity(plan.group_keys.len());
    if plan.group_keys.iter().any(|key| !seen.insert(key.ordinal)) {
        return Err(PlanError::InvalidColumnReference);
    }
    match (plan.aggregate_kind, &plan.aggregate_column) {
        (1, None) => {}
        (2..=6, Some(column))
            if column.column_id != [0; 16] && valid_scalar_type(column.scalar_type) =>
        {
            if matches!(plan.aggregate_kind, 4 | 5) && !is_numeric_type(column.scalar_type) {
                return Err(PlanError::InvalidColumnReference);
            }
            if let Some(existing) = expected.iter().find(|item| item.ordinal == column.ordinal) {
                if existing != column {
                    return Err(PlanError::InvalidColumnReference);
                }
            } else {
                expected.push(column.clone());
            }
        }
        _ => return Err(PlanError::InvalidColumnReference),
    }
    expected.sort_by_key(|column| column.ordinal);
    if expected != plan.scan_columns {
        return Err(PlanError::InvalidColumnReference);
    }
    Ok(())
}

pub fn encode_composite_group_aggregate_plan_v1(
    plan: &CompositeGroupAggregatePlanV1,
) -> Result<Vec<u8>, PlanError> {
    validate_composite_group_aggregate_plan_v1(plan)?;
    let mut output = Vec::with_capacity(512);
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_header_record(
        1,
        PLAN_KIND_COMPOSITE_GROUP_AGGREGATE,
        0,
    ));
    put_name(&mut output, &plan.relation.schema_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_schema_reference_record(
            plan.relation.schema_id,
            plan.relation.schema_version,
        ),
    );
    put_name(&mut output, &plan.relation.relation_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_relation_reference_record(
            plan.relation.relation_id,
            plan.relation.table_epoch,
        ),
    );
    output.extend_from_slice(&(plan.scan_columns.len() as u16).to_be_bytes());
    for column in &plan.scan_columns {
        put_column(&mut output, column);
    }
    output.extend_from_slice(&[
        COMPOSITE_GROUP_AGGREGATE_NODE,
        plan.aggregate_kind,
        plan.group_keys.len() as u8,
    ]);
    for key in &plan.group_keys {
        put_column(&mut output, key);
    }
    match &plan.aggregate_column {
        None => output.push(0),
        Some(column) => {
            output.push(1);
            put_column(&mut output, column);
        }
    }
    match plan.limit {
        None => output.push(0),
        Some(limit) => {
            output.push(1);
            output.extend_from_slice(&limit.to_be_bytes());
        }
    }
    if output.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    Ok(output)
}

pub fn decode_composite_group_aggregate_plan_v1(
    bytes: &[u8],
) -> Result<CompositeGroupAggregatePlanV1, PlanError> {
    if bytes.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    let (version, kind, reserved) =
        crate::query_abi::decode_optimizer_plan_header_record(bytes).ok_or(PlanError::Truncated)?;
    if version != 1 {
        return Err(PlanError::UnsupportedVersion(version));
    }
    if kind != PLAN_KIND_COMPOSITE_GROUP_AGGREGATE || reserved != 0 {
        return Err(PlanError::InvalidEncoding);
    }
    let mut reader = PlanReader {
        bytes,
        offset: crate::query_abi::OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES as usize,
    };
    let schema_name = reader.name()?;
    let (schema_id, schema_version) =
        crate::query_abi::decode_optimizer_plan_schema_reference_record(reader.take(
            crate::query_abi::OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES as usize,
        )?)
        .ok_or(PlanError::Truncated)?;
    let relation_name = reader.name()?;
    let (relation_id, table_epoch) =
        crate::query_abi::decode_optimizer_plan_relation_reference_record(reader.take(
            crate::query_abi::OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES as usize,
        )?)
        .ok_or(PlanError::Truncated)?;
    let scan_count = usize::from(u16::from_be_bytes(
        reader
            .take(2)?
            .try_into()
            .map_err(|_| PlanError::Truncated)?,
    ));
    if !(crate::optimizer_request::MIN_GROUP_KEYS..=crate::optimizer_request::MAX_GROUP_KEYS + 1)
        .contains(&scan_count)
    {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut scan_columns = Vec::with_capacity(scan_count);
    for _ in 0..scan_count {
        scan_columns.push(reader.column()?);
    }
    if reader.u8()? != COMPOSITE_GROUP_AGGREGATE_NODE {
        return Err(PlanError::InvalidPipeline);
    }
    let aggregate_kind = reader.u8()?;
    let key_count = usize::from(reader.u8()?);
    if !(crate::optimizer_request::MIN_GROUP_KEYS..=crate::optimizer_request::MAX_GROUP_KEYS)
        .contains(&key_count)
    {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut group_keys = Vec::with_capacity(key_count);
    for _ in 0..key_count {
        group_keys.push(reader.column()?);
    }
    let aggregate_column = match reader.u8()? {
        0 => None,
        1 => Some(reader.column()?),
        _ => return Err(PlanError::InvalidEncoding),
    };
    let limit = match reader.u8()? {
        0 => None,
        1 => Some(u32::from_be_bytes(
            reader
                .take(4)?
                .try_into()
                .map_err(|_| PlanError::Truncated)?,
        )),
        _ => return Err(PlanError::InvalidEncoding),
    };
    if reader.offset != bytes.len() {
        return Err(PlanError::TrailingBytes);
    }
    let plan = CompositeGroupAggregatePlanV1 {
        relation: RelationReference {
            schema_name,
            schema_id,
            schema_version,
            relation_name,
            relation_id,
            table_epoch,
        },
        scan_columns,
        group_keys,
        aggregate_kind,
        aggregate_column,
        limit,
    };
    validate_composite_group_aggregate_plan_v1(&plan)?;
    Ok(plan)
}

pub fn encode_filtered_composite_group_aggregate_plan_v1(
    plan: &FilteredCompositeGroupAggregatePlanV1,
) -> Result<Vec<u8>, PlanError> {
    validate_filtered_composite_group_aggregate_plan_v1(plan)?;
    let mut output = Vec::with_capacity(1024);
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_header_record(
        1,
        if plan.order_items.is_empty() {
            PLAN_KIND_FILTERED_COMPOSITE_GROUP_AGGREGATE
        } else {
            PLAN_KIND_ORDERED_FILTERED_COMPOSITE_GROUP_AGGREGATE
        },
        0,
    ));
    put_name(&mut output, &plan.relation.schema_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_schema_reference_record(
            plan.relation.schema_id,
            plan.relation.schema_version,
        ),
    );
    put_name(&mut output, &plan.relation.relation_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_relation_reference_record(
            plan.relation.relation_id,
            plan.relation.table_epoch,
        ),
    );
    output.extend_from_slice(&(plan.scan_columns.len() as u16).to_be_bytes());
    for column in &plan.scan_columns {
        put_column(&mut output, column);
    }
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_filter_node_record(
        2,
    ));
    put_predicate(&mut output, &plan.predicate)?;
    output.extend_from_slice(&[
        if plan.order_items.is_empty() {
            FILTERED_COMPOSITE_GROUP_AGGREGATE_NODE
        } else {
            ORDERED_FILTERED_COMPOSITE_GROUP_AGGREGATE_NODE
        },
        plan.aggregate_kind,
        plan.group_keys.len() as u8,
    ]);
    for key in &plan.group_keys {
        put_column(&mut output, key);
    }
    match &plan.aggregate_column {
        None => output.push(0),
        Some(column) => {
            output.push(1);
            put_column(&mut output, column);
        }
    }
    if !plan.order_items.is_empty() {
        output.push(plan.order_items.len() as u8);
        for item in &plan.order_items {
            output.push(item.source);
            if let Some(index) = item.group_key_index {
                output.push(index);
            }
            output.extend_from_slice(&[u8::from(item.descending), u8::from(item.nulls_first)]);
        }
    }
    match plan.limit {
        None => output.push(0),
        Some(limit) => {
            output.push(1);
            output.extend_from_slice(&limit.to_be_bytes());
        }
    }
    if output.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    Ok(output)
}

pub fn decode_filtered_composite_group_aggregate_plan_v1(
    bytes: &[u8],
) -> Result<FilteredCompositeGroupAggregatePlanV1, PlanError> {
    if bytes.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    let (version, kind, reserved) =
        crate::query_abi::decode_optimizer_plan_header_record(bytes).ok_or(PlanError::Truncated)?;
    if version != 1 {
        return Err(PlanError::UnsupportedVersion(version));
    }
    if !matches!(
        kind,
        PLAN_KIND_FILTERED_COMPOSITE_GROUP_AGGREGATE
            | PLAN_KIND_ORDERED_FILTERED_COMPOSITE_GROUP_AGGREGATE
    ) || reserved != 0
    {
        return Err(PlanError::InvalidEncoding);
    }
    let mut reader = PlanReader {
        bytes,
        offset: crate::query_abi::OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES as usize,
    };
    let schema_name = reader.name()?;
    let (schema_id, schema_version) =
        crate::query_abi::decode_optimizer_plan_schema_reference_record(reader.take(
            crate::query_abi::OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES as usize,
        )?)
        .ok_or(PlanError::Truncated)?;
    let relation_name = reader.name()?;
    let (relation_id, table_epoch) =
        crate::query_abi::decode_optimizer_plan_relation_reference_record(reader.take(
            crate::query_abi::OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES as usize,
        )?)
        .ok_or(PlanError::Truncated)?;
    let scan_count = usize::from(reader.u16()?);
    if !(1..=MAX_PLAN_SCAN_COLUMNS).contains(&scan_count) {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut scan_columns = Vec::with_capacity(scan_count);
    for _ in 0..scan_count {
        scan_columns.push(reader.column()?);
    }
    let filter_tag_offset = reader.offset;
    reader.filter_node_tag(filter_tag_offset)?;
    let predicate = reader.predicate(&scan_columns)?;
    let expected_node = if kind == PLAN_KIND_FILTERED_COMPOSITE_GROUP_AGGREGATE {
        FILTERED_COMPOSITE_GROUP_AGGREGATE_NODE
    } else {
        ORDERED_FILTERED_COMPOSITE_GROUP_AGGREGATE_NODE
    };
    if reader.u8()? != expected_node {
        return Err(PlanError::InvalidPipeline);
    }
    let aggregate_kind = reader.u8()?;
    let key_count = usize::from(reader.u8()?);
    if !(1..=crate::optimizer_request::MAX_GROUP_KEYS).contains(&key_count) {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut group_keys = Vec::with_capacity(key_count);
    for _ in 0..key_count {
        group_keys.push(reader.column()?);
    }
    let aggregate_column = match reader.u8()? {
        0 => None,
        1 => Some(reader.column()?),
        _ => return Err(PlanError::InvalidEncoding),
    };
    let mut order_items = Vec::new();
    if kind == PLAN_KIND_ORDERED_FILTERED_COMPOSITE_GROUP_AGGREGATE {
        let order_count = usize::from(reader.u8()?);
        if !(1..=key_count + 1).contains(&order_count) {
            return Err(PlanError::InvalidColumnReference);
        }
        let mut seen_keys = HashSet::new();
        let mut seen_aggregate = false;
        for _ in 0..order_count {
            let source = reader.u8()?;
            let group_key_index = match source {
                0 => {
                    let index = reader.u8()?;
                    if usize::from(index) >= key_count || !seen_keys.insert(index) {
                        return Err(PlanError::InvalidColumnReference);
                    }
                    Some(index)
                }
                1 if !seen_aggregate => {
                    seen_aggregate = true;
                    None
                }
                _ => return Err(PlanError::InvalidColumnReference),
            };
            let descending = match reader.u8()? {
                0 => false,
                1 => true,
                _ => return Err(PlanError::InvalidEncoding),
            };
            let nulls_first = match reader.u8()? {
                0 => false,
                1 => true,
                _ => return Err(PlanError::InvalidEncoding),
            };
            order_items.push(CompositeGroupCountOrderItem {
                source,
                group_key_index,
                descending,
                nulls_first,
            });
        }
    }
    let limit = match reader.u8()? {
        0 => None,
        1 => Some(reader.u32()?),
        _ => return Err(PlanError::InvalidEncoding),
    };
    if reader.offset != bytes.len() {
        return Err(PlanError::TrailingBytes);
    }
    let plan = FilteredCompositeGroupAggregatePlanV1 {
        relation: RelationReference {
            schema_name,
            schema_id,
            schema_version,
            relation_name,
            relation_id,
            table_epoch,
        },
        scan_columns,
        group_keys,
        aggregate_kind,
        aggregate_column,
        predicate,
        order_items,
        limit,
    };
    validate_filtered_composite_group_aggregate_plan_v1(&plan)?;
    Ok(plan)
}

pub fn plan_ordered_composite_group_aggregate_v1(
    bound: BoundOrderedCompositeGroupAggregate,
) -> Result<OrderedCompositeGroupAggregatePlanV1, PlanError> {
    let aggregate_plan = plan_composite_group_aggregate_v1(BoundCompositeGroupAggregate {
        schema_name: bound.schema_name,
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        table_epoch: bound.table_epoch,
        relation_id: bound.relation_id,
        relation_name: bound.relation_name,
        group_keys: bound.group_keys,
        aggregate_kind: bound.aggregate_kind,
        aggregate_column: bound.aggregate_column,
        limit: bound.limit,
    })?;
    let plan = OrderedCompositeGroupAggregatePlanV1 {
        aggregate_plan,
        order_items: bound.order_items,
    };
    validate_ordered_composite_group_aggregate_plan_v1(&plan)?;
    Ok(plan)
}

pub fn validate_ordered_composite_group_aggregate_plan_v1(
    plan: &OrderedCompositeGroupAggregatePlanV1,
) -> Result<(), PlanError> {
    validate_composite_group_aggregate_plan_v1(&plan.aggregate_plan)?;
    if !(1..=9).contains(&plan.order_items.len()) {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut seen_keys = HashSet::new();
    let mut aggregate_seen = false;
    for item in &plan.order_items {
        match (item.source, item.group_key_index) {
            (0, Some(index))
                if usize::from(index) < plan.aggregate_plan.group_keys.len()
                    && seen_keys.insert(index) => {}
            (1, None) if !aggregate_seen => aggregate_seen = true,
            _ => return Err(PlanError::InvalidColumnReference),
        }
    }
    Ok(())
}

pub fn encode_ordered_composite_group_aggregate_plan_v1(
    plan: &OrderedCompositeGroupAggregatePlanV1,
) -> Result<Vec<u8>, PlanError> {
    validate_ordered_composite_group_aggregate_plan_v1(plan)?;
    let mut output = encode_composite_group_aggregate_plan_v1(&plan.aggregate_plan)?;
    let mut reader = PlanReader {
        bytes: &output,
        offset: crate::query_abi::OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES as usize,
    };
    reader.name()?;
    reader.take(crate::query_abi::OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES as usize)?;
    reader.name()?;
    reader
        .take(crate::query_abi::OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES as usize)?;
    let scan_count = usize::from(reader.u16()?);
    for _ in 0..scan_count {
        reader.column()?;
    }
    let node_offset = reader.offset;
    reader.u8()?;
    reader.u8()?;
    let key_count = usize::from(reader.u8()?);
    for _ in 0..key_count {
        reader.column()?;
    }
    match reader.u8()? {
        0 => {}
        1 => {
            reader.column()?;
        }
        _ => return Err(PlanError::InvalidEncoding),
    }
    let limit_offset = reader.offset;
    output.truncate(limit_offset);
    output.extend_from_slice(&[
        ORDERED_COMPOSITE_GROUP_AGGREGATE_NODE,
        plan.order_items.len() as u8,
    ]);
    for item in &plan.order_items {
        output.push(item.source);
        if let Some(index) = item.group_key_index {
            output.push(index);
        }
        output.extend_from_slice(&[u8::from(item.descending), u8::from(item.nulls_first)]);
    }
    match plan.aggregate_plan.limit {
        None => output.push(0),
        Some(limit) => {
            output.push(1);
            output.extend_from_slice(&limit.to_be_bytes());
        }
    }
    output[2] = PLAN_KIND_ORDERED_COMPOSITE_GROUP_AGGREGATE;
    output[node_offset] = ORDERED_COMPOSITE_GROUP_AGGREGATE_NODE;
    if output.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    Ok(output)
}

pub fn decode_ordered_composite_group_aggregate_plan_v1(
    bytes: &[u8],
) -> Result<OrderedCompositeGroupAggregatePlanV1, PlanError> {
    if bytes.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    let (version, kind, reserved) =
        crate::query_abi::decode_optimizer_plan_header_record(bytes).ok_or(PlanError::Truncated)?;
    if version != 1 {
        return Err(PlanError::UnsupportedVersion(version));
    }
    if kind != PLAN_KIND_ORDERED_COMPOSITE_GROUP_AGGREGATE || reserved != 0 {
        return Err(PlanError::InvalidEncoding);
    }
    let mut reader = PlanReader {
        bytes,
        offset: crate::query_abi::OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES as usize,
    };
    reader.name()?;
    reader.take(crate::query_abi::OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES as usize)?;
    reader.name()?;
    reader
        .take(crate::query_abi::OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES as usize)?;
    let scan_count = usize::from(reader.u16()?);
    if !(crate::optimizer_request::MIN_GROUP_KEYS..=crate::optimizer_request::MAX_GROUP_KEYS + 1)
        .contains(&scan_count)
    {
        return Err(PlanError::InvalidColumnReference);
    }
    for _ in 0..scan_count {
        reader.column()?;
    }
    let node_offset = reader.offset;
    if reader.u8()? != ORDERED_COMPOSITE_GROUP_AGGREGATE_NODE {
        return Err(PlanError::InvalidPipeline);
    }
    let aggregate_kind = reader.u8()?;
    let key_count = usize::from(reader.u8()?);
    if !(crate::optimizer_request::MIN_GROUP_KEYS..=crate::optimizer_request::MAX_GROUP_KEYS)
        .contains(&key_count)
    {
        return Err(PlanError::InvalidColumnReference);
    }
    for _ in 0..key_count {
        reader.column()?;
    }
    match reader.u8()? {
        0 => {}
        1 => {
            reader.column()?;
        }
        _ => return Err(PlanError::InvalidEncoding),
    }
    let base_prefix_end = reader.offset;
    if reader.u8()? != ORDERED_COMPOSITE_GROUP_AGGREGATE_NODE {
        return Err(PlanError::InvalidPipeline);
    }
    let order_count = usize::from(reader.u8()?);
    if !(1..=9).contains(&order_count) {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut order_items = Vec::with_capacity(order_count);
    for _ in 0..order_count {
        let source = reader.u8()?;
        let group_key_index = match source {
            0 => Some(reader.u8()?),
            1 => None,
            _ => return Err(PlanError::InvalidColumnReference),
        };
        let descending = match reader.u8()? {
            0 => false,
            1 => true,
            _ => return Err(PlanError::InvalidEncoding),
        };
        let nulls_first = match reader.u8()? {
            0 => false,
            1 => true,
            _ => return Err(PlanError::InvalidEncoding),
        };
        order_items.push(CompositeGroupCountOrderItem {
            source,
            group_key_index,
            descending,
            nulls_first,
        });
    }
    let limit_offset = reader.offset;
    let limit = match reader.u8()? {
        0 => None,
        1 => Some(reader.u32()?),
        _ => return Err(PlanError::InvalidEncoding),
    };
    if reader.offset != bytes.len() {
        return Err(PlanError::TrailingBytes);
    }
    let mut base_bytes = bytes[..base_prefix_end].to_vec();
    base_bytes.extend_from_slice(&bytes[limit_offset..]);
    base_bytes[2] = PLAN_KIND_COMPOSITE_GROUP_AGGREGATE;
    base_bytes[node_offset] = COMPOSITE_GROUP_AGGREGATE_NODE;
    let aggregate_plan = decode_composite_group_aggregate_plan_v1(&base_bytes)?;
    if aggregate_plan.aggregate_kind != aggregate_kind || aggregate_plan.limit != limit {
        return Err(PlanError::InvalidColumnReference);
    }
    let plan = OrderedCompositeGroupAggregatePlanV1 {
        aggregate_plan,
        order_items,
    };
    validate_ordered_composite_group_aggregate_plan_v1(&plan)?;
    Ok(plan)
}

pub fn plan_ordered_select_v1(bound: BoundOrderedSelect) -> Result<OrderedSelectPlanV1, PlanError> {
    let relation = RelationReference {
        schema_name: bound.schema_name,
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        relation_name: bound.relation_name,
        relation_id: bound.relation_id,
        table_epoch: bound.table_epoch,
    };
    let mut scan_columns = bound.projections.clone();
    if !scan_columns.contains(&bound.order_key) {
        scan_columns.push(bound.order_key.clone());
    }
    scan_columns.sort_by_key(|column| column.ordinal);
    let plan = OrderedSelectPlanV1 {
        relation,
        scan_columns,
        projections: bound.projections,
        order_key: bound.order_key,
        descending: bound.descending,
        limit: bound.limit,
    };
    validate_ordered_select_plan_v1(&plan)?;
    Ok(plan)
}

pub fn validate_ordered_select_plan_v1(plan: &OrderedSelectPlanV1) -> Result<(), PlanError> {
    validate_relation(&plan.relation)?;
    validate_columns(&plan.scan_columns)?;
    if plan.projections.is_empty()
        || plan.projections.len() > crate::optimizer_request::MAX_PROJECTIONS
        || plan.projections.iter().any(|projection| {
            projection.column_id == [0; 16]
                || !valid_scalar_type(projection.scalar_type)
                || !plan.scan_columns.contains(projection)
        })
        || plan.order_key.column_id == [0; 16]
        || !valid_scalar_type(plan.order_key.scalar_type)
        || !plan.scan_columns.contains(&plan.order_key)
        || plan.scan_columns.len()
            != plan
                .projections
                .iter()
                .chain(std::iter::once(&plan.order_key))
                .map(|column| column.ordinal)
                .collect::<HashSet<_>>()
                .len()
    {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut projected = HashSet::with_capacity(plan.projections.len());
    if plan
        .projections
        .iter()
        .any(|column| !projected.insert(column.ordinal))
    {
        return Err(PlanError::InvalidColumnReference);
    }
    Ok(())
}

pub fn encode_ordered_select_plan_v1(plan: &OrderedSelectPlanV1) -> Result<Vec<u8>, PlanError> {
    validate_ordered_select_plan_v1(plan)?;
    let mut output = Vec::with_capacity(256);
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_header_record(
        1,
        PLAN_KIND_ORDERED_SELECT,
        0,
    ));
    put_name(&mut output, &plan.relation.schema_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_schema_reference_record(
            plan.relation.schema_id,
            plan.relation.schema_version,
        ),
    );
    put_name(&mut output, &plan.relation.relation_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_relation_reference_record(
            plan.relation.relation_id,
            plan.relation.table_epoch,
        ),
    );
    output.extend_from_slice(&(plan.scan_columns.len() as u16).to_be_bytes());
    for column in &plan.scan_columns {
        put_column(&mut output, column);
    }
    output.extend_from_slice(&(plan.projections.len() as u16).to_be_bytes());
    for projection in &plan.projections {
        put_column(&mut output, projection);
    }
    output.push(ORDER_BY_NODE);
    put_column(&mut output, &plan.order_key);
    output.push(u8::from(plan.descending));
    match plan.limit {
        None => output.push(0),
        Some(limit) => {
            output.push(1);
            output.extend_from_slice(&limit.to_be_bytes());
        }
    }
    if output.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    Ok(output)
}

pub fn decode_ordered_select_plan_v1(bytes: &[u8]) -> Result<OrderedSelectPlanV1, PlanError> {
    if bytes.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    let (version, kind, reserved) =
        crate::query_abi::decode_optimizer_plan_header_record(bytes).ok_or(PlanError::Truncated)?;
    if version != 1 {
        return Err(PlanError::UnsupportedVersion(version));
    }
    if kind != PLAN_KIND_ORDERED_SELECT || reserved != 0 {
        return Err(PlanError::InvalidEncoding);
    }
    let mut reader = PlanReader {
        bytes,
        offset: crate::query_abi::OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES as usize,
    };
    let schema_name = reader.name()?;
    let (schema_id, schema_version) =
        crate::query_abi::decode_optimizer_plan_schema_reference_record(reader.take(
            crate::query_abi::OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES as usize,
        )?)
        .ok_or(PlanError::Truncated)?;
    let relation_name = reader.name()?;
    let (relation_id, table_epoch) =
        crate::query_abi::decode_optimizer_plan_relation_reference_record(reader.take(
            crate::query_abi::OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES as usize,
        )?)
        .ok_or(PlanError::Truncated)?;
    let scan_count = usize::from(u16::from_be_bytes(
        reader
            .take(2)?
            .try_into()
            .map_err(|_| PlanError::Truncated)?,
    ));
    if scan_count == 0 || scan_count > MAX_PLAN_SCAN_COLUMNS {
        return Err(PlanError::InvalidColumnReference);
    }
    let mut scan_columns = Vec::with_capacity(scan_count);
    for _ in 0..scan_count {
        scan_columns.push(reader.column()?);
    }
    let projection_count = usize::from(u16::from_be_bytes(
        reader
            .take(2)?
            .try_into()
            .map_err(|_| PlanError::Truncated)?,
    ));
    if projection_count == 0 || projection_count > crate::optimizer_request::MAX_PROJECTIONS {
        return Err(PlanError::EmptyProjection);
    }
    let mut projections = Vec::with_capacity(projection_count);
    for _ in 0..projection_count {
        projections.push(reader.column()?);
    }
    if reader.u8()? != ORDER_BY_NODE {
        return Err(PlanError::InvalidPipeline);
    }
    let order_key = reader.column()?;
    let descending = match reader.u8()? {
        0 => false,
        1 => true,
        _ => return Err(PlanError::InvalidEncoding),
    };
    let limit = match reader.u8()? {
        0 => None,
        1 => Some(u32::from_be_bytes(
            reader
                .take(4)?
                .try_into()
                .map_err(|_| PlanError::Truncated)?,
        )),
        _ => return Err(PlanError::InvalidEncoding),
    };
    if reader.offset != bytes.len() {
        return Err(PlanError::TrailingBytes);
    }
    let plan = OrderedSelectPlanV1 {
        relation: RelationReference {
            schema_name,
            schema_id,
            schema_version,
            relation_name,
            relation_id,
            table_epoch,
        },
        scan_columns,
        projections,
        order_key,
        descending,
        limit,
    };
    validate_ordered_select_plan_v1(&plan)?;
    Ok(plan)
}

fn join_key_types_compatible(left: ScalarType, right: ScalarType) -> bool {
    match (left, right) {
        (ScalarType::Decimal { scale: left, .. }, ScalarType::Decimal { scale: right, .. }) => {
            left == right
        }
        (left, right) => left == right,
    }
}

/// Construct a deterministic DELETE plan from an already bound relation and
/// Boolean predicate. The plan conveys no storage capability or write grant.
pub fn plan_delete_v1(
    relation: RelationReference,
    predicate: BoundPredicate,
) -> Result<DeletePlanV1, PlanError> {
    let mut scan_columns = Vec::new();
    collect_predicate_columns(&predicate, &mut scan_columns)?;
    scan_columns.sort_by_key(|column| column.ordinal);
    let plan = DeletePlanV1 {
        relation,
        scan_columns,
        predicate,
    };
    validate_delete_plan_v1(&plan)?;
    Ok(plan)
}

/// Encode a canonical DELETE plan as a `Scan -> Filter -> Delete` pipeline.
pub fn encode_delete_plan_v1(plan: &DeletePlanV1) -> Result<Vec<u8>, PlanError> {
    validate_delete_plan_v1(plan)?;
    let mut output = Vec::with_capacity(256);
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_header_record(
        1,
        PLAN_KIND_DELETE,
        0,
    ));
    put_name(&mut output, &plan.relation.schema_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_schema_reference_record(
            plan.relation.schema_id,
            plan.relation.schema_version,
        ),
    );
    put_name(&mut output, &plan.relation.relation_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_relation_reference_record(
            plan.relation.relation_id,
            plan.relation.table_epoch,
        ),
    );
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_node_count_record(
        3,
    ));
    put_column_list_header(&mut output, 1, plan.scan_columns.len())?;
    for column in &plan.scan_columns {
        put_column(&mut output, column);
    }
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_filter_node_record(
        2,
    ));
    put_predicate(&mut output, &plan.predicate)?;
    output.push(DELETE_SINK_NODE);
    if output.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    Ok(output)
}

/// Decode and validate a canonical DELETE plan independently from the
/// optimizer. The executor must re-resolve the relation and still use its
/// ordinary scan and mutation IPC operations.
pub fn decode_delete_plan_v1(bytes: &[u8]) -> Result<DeletePlanV1, PlanError> {
    if bytes.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    let (version, kind, reserved) =
        crate::query_abi::decode_optimizer_plan_header_record(bytes).ok_or(PlanError::Truncated)?;
    if version != 1 {
        return Err(PlanError::UnsupportedVersion(version));
    }
    if kind != PLAN_KIND_DELETE || reserved != 0 {
        return Err(PlanError::InvalidEncoding);
    }
    let mut reader = PlanReader {
        bytes,
        offset: crate::query_abi::OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES as usize,
    };
    let schema_name = reader.name()?;
    let schema_reference = reader
        .take(crate::query_abi::OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES as usize)?;
    let (schema_id, schema_version) =
        crate::query_abi::decode_optimizer_plan_schema_reference_record(schema_reference)
            .ok_or(PlanError::Truncated)?;
    let relation_name = reader.name()?;
    let relation_reference = reader
        .take(crate::query_abi::OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES as usize)?;
    let (relation_id, table_epoch) =
        crate::query_abi::decode_optimizer_plan_relation_reference_record(relation_reference)
            .ok_or(PlanError::Truncated)?;
    let relation = RelationReference {
        schema_name,
        schema_id,
        schema_version,
        relation_name,
        relation_id,
        table_epoch,
    };
    if reader.node_count()? != 3 {
        return Err(PlanError::InvalidPipeline);
    }
    let scan_tag_offset = reader.offset;
    if reader.u8()? != 1 {
        return Err(PlanError::InvalidPipeline);
    }
    let column_count = reader.column_list_count(scan_tag_offset, 1, crate::tuple::MAX_COLUMNS)?;
    let mut scan_columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        scan_columns.push(reader.column()?);
    }
    if !scan_columns.is_empty() {
        validate_columns(&scan_columns)?;
    }
    let filter_tag_offset = reader.offset;
    reader.filter_node_tag(filter_tag_offset)?;
    let predicate = reader.predicate(&scan_columns)?;
    if reader.u8()? != DELETE_SINK_NODE || !reader.is_empty() {
        return Err(PlanError::InvalidPipeline);
    }
    let plan = DeletePlanV1 {
        relation,
        scan_columns,
        predicate,
    };
    validate_delete_plan_v1(&plan)?;
    Ok(plan)
}

/// Check a decoded DELETE plan's structural, type, and bounded-scan
/// invariants. Relation authorization and row-level permissions remain owned
/// by invocation-bound storage.
pub fn validate_delete_plan_v1(plan: &DeletePlanV1) -> Result<(), PlanError> {
    validate_relation(&plan.relation)?;
    if plan.scan_columns.len() > crate::tuple::MAX_COLUMNS {
        return Err(PlanError::TooLarge);
    }
    if !plan.scan_columns.is_empty() {
        validate_columns(&plan.scan_columns)?;
    }
    if predicate_type(&plan.predicate, &plan.scan_columns)? != Some(ScalarType::Boolean) {
        return Err(PlanError::InvalidPredicate);
    }
    let mut referenced_columns = Vec::new();
    collect_predicate_columns(&plan.predicate, &mut referenced_columns)?;
    referenced_columns.sort_by_key(|column| column.ordinal);
    if referenced_columns != plan.scan_columns {
        return Err(PlanError::InvalidColumnReference);
    }
    Ok(())
}

/// Construct a deterministic UPDATE plan. The scan carries the unique,
/// ordinal-sorted union of assignment targets and predicate references.
pub fn plan_update_v1(
    relation: RelationReference,
    predicate: BoundPredicate,
    assignments: Vec<BoundUpdateAssignment>,
) -> Result<UpdatePlanV1, PlanError> {
    let mut scan_columns = Vec::new();
    collect_predicate_columns(&predicate, &mut scan_columns)?;
    for assignment in &assignments {
        add_column(&mut scan_columns, &assignment.column)?;
    }
    scan_columns.sort_by_key(|column| column.ordinal);
    let plan = UpdatePlanV1 {
        relation,
        scan_columns,
        predicate,
        assignments,
    };
    validate_update_plan_v1(&plan)?;
    Ok(plan)
}

/// Encode the canonical `Scan -> Filter -> Update` pipeline.
pub fn encode_update_plan_v1(plan: &UpdatePlanV1) -> Result<Vec<u8>, PlanError> {
    validate_update_plan_v1(plan)?;
    let mut output = Vec::with_capacity(256);
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_header_record(
        1,
        PLAN_KIND_UPDATE,
        0,
    ));
    put_name(&mut output, &plan.relation.schema_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_schema_reference_record(
            plan.relation.schema_id,
            plan.relation.schema_version,
        ),
    );
    put_name(&mut output, &plan.relation.relation_name)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_relation_reference_record(
            plan.relation.relation_id,
            plan.relation.table_epoch,
        ),
    );
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_node_count_record(
        3,
    ));
    put_column_list_header(&mut output, 1, plan.scan_columns.len())?;
    for column in &plan.scan_columns {
        put_column(&mut output, column);
    }
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_filter_node_record(
        2,
    ));
    put_predicate(&mut output, &plan.predicate)?;
    let assignment_count =
        u16::try_from(plan.assignments.len()).map_err(|_| PlanError::InvalidPipeline)?;
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_update_node_record(
        crate::query_abi::OPTIMIZER_PLAN_UPDATE_NODE,
        assignment_count,
    ));
    for assignment in &plan.assignments {
        put_column(&mut output, &assignment.column);
        match assignment.value {
            BoundUpdateValue::Parameter(index) => {
                output.extend_from_slice(
                    &crate::query_abi::encode_optimizer_plan_update_assignment_value_record(
                        crate::query_abi::OPTIMIZER_UPDATE_PARAMETER_VALUE,
                        index,
                    ),
                );
            }
            BoundUpdateValue::Null => output.extend_from_slice(
                &crate::query_abi::encode_optimizer_plan_update_assignment_value_record(
                    crate::query_abi::OPTIMIZER_UPDATE_NULL_VALUE,
                    0,
                ),
            ),
        }
    }
    if output.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    Ok(output)
}

/// Decode and validate a canonical UPDATE plan without granting storage
/// access. The executor must re-resolve the relation before execution.
pub fn decode_update_plan_v1(bytes: &[u8]) -> Result<UpdatePlanV1, PlanError> {
    if bytes.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    let (version, kind, reserved) =
        crate::query_abi::decode_optimizer_plan_header_record(bytes).ok_or(PlanError::Truncated)?;
    if version != 1 {
        return Err(PlanError::UnsupportedVersion(version));
    }
    if kind != PLAN_KIND_UPDATE || reserved != 0 {
        return Err(PlanError::InvalidEncoding);
    }
    let mut reader = PlanReader {
        bytes,
        offset: crate::query_abi::OPTIMIZER_PLAN_HEADER_RECORD_HEADER_BYTES as usize,
    };
    let schema_name = reader.name()?;
    let schema_reference = reader
        .take(crate::query_abi::OPTIMIZER_PLAN_SCHEMA_REFERENCE_RECORD_HEADER_BYTES as usize)?;
    let (schema_id, schema_version) =
        crate::query_abi::decode_optimizer_plan_schema_reference_record(schema_reference)
            .ok_or(PlanError::Truncated)?;
    let relation_name = reader.name()?;
    let relation_reference = reader
        .take(crate::query_abi::OPTIMIZER_PLAN_RELATION_REFERENCE_RECORD_HEADER_BYTES as usize)?;
    let (relation_id, table_epoch) =
        crate::query_abi::decode_optimizer_plan_relation_reference_record(relation_reference)
            .ok_or(PlanError::Truncated)?;
    let relation = RelationReference {
        schema_name,
        schema_id,
        schema_version,
        relation_name,
        relation_id,
        table_epoch,
    };
    if reader.node_count()? != 3 {
        return Err(PlanError::InvalidPipeline);
    }
    let scan_tag_offset = reader.offset;
    if reader.u8()? != 1 {
        return Err(PlanError::InvalidPipeline);
    }
    let column_count = reader.column_list_count(scan_tag_offset, 1, crate::tuple::MAX_COLUMNS)?;
    let mut scan_columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        scan_columns.push(reader.column()?);
    }
    if !scan_columns.is_empty() {
        validate_columns(&scan_columns)?;
    }
    let filter_tag_offset = reader.offset;
    reader.filter_node_tag(filter_tag_offset)?;
    let predicate = reader.predicate(&scan_columns)?;
    let update_node =
        reader.take(crate::query_abi::OPTIMIZER_PLAN_UPDATE_NODE_RECORD_HEADER_BYTES as usize)?;
    let (node_tag, assignment_count) =
        crate::query_abi::decode_optimizer_plan_update_node_record(update_node)
            .ok_or(PlanError::Truncated)?;
    if node_tag != crate::query_abi::OPTIMIZER_PLAN_UPDATE_NODE {
        return Err(PlanError::InvalidPipeline);
    }
    let assignment_count = usize::from(assignment_count);
    if assignment_count == 0 || assignment_count > crate::optimizer_request::MAX_UPDATE_ASSIGNMENTS
    {
        return Err(PlanError::InvalidPipeline);
    }
    let mut assignments = Vec::with_capacity(assignment_count);
    for _ in 0..assignment_count {
        let column = reader.column()?;
        let value_record = reader.take(
            crate::query_abi::OPTIMIZER_PLAN_UPDATE_ASSIGNMENT_VALUE_RECORD_HEADER_BYTES as usize,
        )?;
        let (kind, parameter_index) =
            crate::query_abi::decode_optimizer_plan_update_assignment_value_record(value_record)
                .ok_or(PlanError::Truncated)?;
        let value = match kind {
            crate::query_abi::OPTIMIZER_UPDATE_PARAMETER_VALUE => {
                let index = parameter_index;
                if index == 0 || index > crate::optimizer_request::MAX_PARAMETER_INDEX {
                    return Err(PlanError::InvalidEncoding);
                }
                BoundUpdateValue::Parameter(index)
            }
            crate::query_abi::OPTIMIZER_UPDATE_NULL_VALUE
                if parameter_index == 0 && column.nullable =>
            {
                BoundUpdateValue::Null
            }
            crate::query_abi::OPTIMIZER_UPDATE_NULL_VALUE => {
                return Err(PlanError::InvalidEncoding);
            }
            _ => return Err(PlanError::InvalidEncoding),
        };
        assignments.push(BoundUpdateAssignment { column, value });
    }
    if !reader.is_empty() {
        return Err(PlanError::TrailingBytes);
    }
    let plan = UpdatePlanV1 {
        relation,
        scan_columns,
        predicate,
        assignments,
    };
    validate_update_plan_v1(&plan)?;
    Ok(plan)
}

/// Validate relation references, scan shape, assignment uniqueness and
/// nullability, and the Boolean predicate without granting write authority.
pub fn validate_update_plan_v1(plan: &UpdatePlanV1) -> Result<(), PlanError> {
    validate_relation(&plan.relation)?;
    if plan.scan_columns.len() > crate::tuple::MAX_COLUMNS {
        return Err(PlanError::TooLarge);
    }
    if !plan.scan_columns.is_empty() {
        validate_columns(&plan.scan_columns)?;
    }
    if predicate_type(&plan.predicate, &plan.scan_columns)? != Some(ScalarType::Boolean) {
        return Err(PlanError::InvalidPredicate);
    }
    if plan.assignments.is_empty()
        || plan.assignments.len() > crate::optimizer_request::MAX_UPDATE_ASSIGNMENTS
    {
        return Err(PlanError::InvalidPipeline);
    }
    let mut ordinals = HashSet::with_capacity(plan.assignments.len());
    for assignment in &plan.assignments {
        if !plan
            .scan_columns
            .iter()
            .any(|column| column == &assignment.column)
            || !ordinals.insert(assignment.column.ordinal)
        {
            return Err(PlanError::InvalidColumnReference);
        }
        match assignment.value {
            BoundUpdateValue::Parameter(index)
                if index != 0 && index <= crate::optimizer_request::MAX_PARAMETER_INDEX => {}
            BoundUpdateValue::Parameter(_) => return Err(PlanError::InvalidEncoding),
            BoundUpdateValue::Null if assignment.column.nullable => {}
            BoundUpdateValue::Null => return Err(PlanError::InvalidEncoding),
        }
    }
    let mut required_columns = Vec::new();
    collect_predicate_columns(&plan.predicate, &mut required_columns)?;
    for assignment in &plan.assignments {
        add_column(&mut required_columns, &assignment.column)?;
    }
    required_columns.sort_by_key(|column| column.ordinal);
    if required_columns != plan.scan_columns {
        return Err(PlanError::InvalidColumnReference);
    }
    Ok(())
}

fn put_name(output: &mut Vec<u8>, name: &str) -> Result<(), PlanError> {
    if !valid_identifier(name) {
        return Err(PlanError::InvalidRelationReference);
    }
    let byte_length = u8::try_from(name.len()).map_err(|_| PlanError::TooLarge)?;
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_name_header_record(
        byte_length,
    ));
    output.extend_from_slice(name.as_bytes());
    Ok(())
}

fn put_column_list_header(
    output: &mut Vec<u8>,
    node_tag: u8,
    column_count: usize,
) -> Result<(), PlanError> {
    let column_count = u16::try_from(column_count).map_err(|_| PlanError::TooLarge)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_column_list_header_record(node_tag, column_count),
    );
    Ok(())
}

fn put_column(output: &mut Vec<u8>, column: &BoundColumn) {
    let (type_code, parameters) = encode_scalar_type(column.scalar_type);
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_column_record(
        column.column_id,
        column.ordinal,
        type_code,
        parameters,
        u8::from(column.nullable),
    ));
}

fn encode_scalar_type(scalar_type: ScalarType) -> (u8, [u8; 4]) {
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

fn put_predicate(output: &mut Vec<u8>, predicate: &BoundPredicate) -> Result<(), PlanError> {
    match predicate {
        BoundPredicate::Null => output.extend_from_slice(
            &crate::query_abi::encode_optimizer_plan_null_predicate_record(0x00),
        ),
        BoundPredicate::Boolean(value) => output.extend_from_slice(
            &crate::query_abi::encode_optimizer_plan_boolean_literal_predicate_record(
                0x01,
                u8::from(*value),
            ),
        ),
        BoundPredicate::Integer(value) => {
            output.extend_from_slice(
                &crate::query_abi::encode_optimizer_plan_integer_predicate_record(0x02, *value),
            );
        }
        BoundPredicate::Text(value) => {
            let text_length = u16::try_from(value.len()).map_err(|_| PlanError::TooLarge)?;
            output.extend_from_slice(
                &crate::query_abi::encode_optimizer_plan_text_predicate_header_record(
                    0x03,
                    text_length,
                ),
            );
            output.extend_from_slice(value.as_bytes());
        }
        BoundPredicate::Column(column) => {
            output.extend_from_slice(
                &crate::query_abi::encode_optimizer_plan_column_predicate_record(
                    0x10,
                    column.column_id,
                ),
            );
        }
        BoundPredicate::Compare {
            operator,
            left,
            right,
        } => {
            let comparison_operator = match operator {
                crate::optimizer_request::Comparison::Equal => 0,
                crate::optimizer_request::Comparison::NotEqual => 1,
                crate::optimizer_request::Comparison::Less => 2,
                crate::optimizer_request::Comparison::LessOrEqual => 3,
                crate::optimizer_request::Comparison::Greater => 4,
                crate::optimizer_request::Comparison::GreaterOrEqual => 5,
            };
            output.extend_from_slice(
                &crate::query_abi::encode_optimizer_plan_compare_predicate_header_record(
                    0x20,
                    comparison_operator,
                ),
            );
            put_predicate(output, left)?;
            put_predicate(output, right)?;
        }
        BoundPredicate::IsNull(child) | BoundPredicate::IsNotNull(child) => {
            let tag = if matches!(predicate, BoundPredicate::IsNull(_)) {
                0x40
            } else {
                0x41
            };
            if !matches!(child.as_ref(), BoundPredicate::Column(_)) {
                return Err(PlanError::InvalidPredicate);
            }
            output.push(tag);
            put_predicate(output, child)?;
        }
        BoundPredicate::And(children) | BoundPredicate::Or(children) => {
            let is_and = matches!(predicate, BoundPredicate::And(_));
            output.extend_from_slice(
                &crate::query_abi::encode_optimizer_plan_boolean_predicate_header_record(
                    if is_and { 0x30 } else { 0x31 },
                    u8::try_from(children.len()).map_err(|_| PlanError::InvalidPredicate)?,
                ),
            );
            for child in children {
                put_predicate(output, child)?;
            }
        }
        BoundPredicate::Not(child) => {
            output.extend_from_slice(
                &crate::query_abi::encode_optimizer_plan_not_predicate_record(0x32),
            );
            put_predicate(output, child)?;
        }
    }
    if output.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    Ok(())
}

pub(crate) fn encode_bound_predicate_v1(predicate: &BoundPredicate) -> Result<Vec<u8>, PlanError> {
    let mut output = Vec::new();
    put_predicate(&mut output, predicate)?;
    if output.len() > crate::optimizer_request::MAX_PREDICATE_BYTES {
        return Err(PlanError::TooLarge);
    }
    Ok(output)
}

pub(crate) fn decode_bound_predicate_v1(
    bytes: &[u8],
    scan_columns: &[BoundColumn],
) -> Result<BoundPredicate, PlanError> {
    if bytes.is_empty() || bytes.len() > crate::optimizer_request::MAX_PREDICATE_BYTES {
        return Err(PlanError::InvalidPredicate);
    }
    let mut reader = PlanReader { bytes, offset: 0 };
    let predicate = reader.predicate(scan_columns)?;
    if !reader.is_empty() {
        return Err(PlanError::InvalidPredicate);
    }
    Ok(predicate)
}

struct PlanReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl PlanReader<'_> {
    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn take(&mut self, count: usize) -> Result<&[u8], PlanError> {
        let end = self.offset.checked_add(count).ok_or(PlanError::TooLarge)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(PlanError::Truncated)?;
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, PlanError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, PlanError> {
        Ok(u16::from_be_bytes(
            self.take(2)?.try_into().map_err(|_| PlanError::Truncated)?,
        ))
    }

    fn u32(&mut self) -> Result<u32, PlanError> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().map_err(|_| PlanError::Truncated)?,
        ))
    }

    fn name(&mut self) -> Result<String, PlanError> {
        let header =
            self.take(crate::query_abi::OPTIMIZER_PLAN_NAME_HEADER_RECORD_HEADER_BYTES as usize)?;
        let byte_length = crate::query_abi::decode_optimizer_plan_name_header_record(header)
            .ok_or(PlanError::Truncated)?;
        let length = usize::from(byte_length);
        let bytes = self.take(length)?;
        let name = std::str::from_utf8(bytes).map_err(|_| PlanError::InvalidEncoding)?;
        if !valid_identifier(name) {
            return Err(PlanError::InvalidRelationReference);
        }
        Ok(name.to_owned())
    }

    fn node_count(&mut self) -> Result<u8, PlanError> {
        let header =
            self.take(crate::query_abi::OPTIMIZER_PLAN_NODE_COUNT_RECORD_HEADER_BYTES as usize)?;
        crate::query_abi::decode_optimizer_plan_node_count_record(header)
            .ok_or(PlanError::Truncated)
    }

    fn filter_node_tag(&mut self, header_offset: usize) -> Result<(), PlanError> {
        let header_len = crate::query_abi::OPTIMIZER_PLAN_FILTER_NODE_RECORD_HEADER_BYTES as usize;
        let end = header_offset
            .checked_add(header_len)
            .ok_or(PlanError::TooLarge)?;
        let header = self
            .bytes
            .get(header_offset..end)
            .ok_or(PlanError::Truncated)?;
        let node_tag = crate::query_abi::decode_optimizer_plan_filter_node_record(header)
            .ok_or(PlanError::Truncated)?;
        if node_tag != 2 {
            return Err(PlanError::InvalidPipeline);
        }
        self.offset = end;
        Ok(())
    }

    fn column_list_count(
        &mut self,
        header_offset: usize,
        expected_tag: u8,
        maximum: usize,
    ) -> Result<usize, PlanError> {
        let header_len =
            crate::query_abi::OPTIMIZER_PLAN_COLUMN_LIST_HEADER_RECORD_HEADER_BYTES as usize;
        let end = header_offset
            .checked_add(header_len)
            .ok_or(PlanError::TooLarge)?;
        let header = self
            .bytes
            .get(header_offset..end)
            .ok_or(PlanError::Truncated)?;
        let (node_tag, count) =
            crate::query_abi::decode_optimizer_plan_column_list_header_record(header)
                .ok_or(PlanError::Truncated)?;
        if node_tag != expected_tag {
            return Err(PlanError::InvalidPipeline);
        }
        self.offset = end;
        let count = usize::from(count);
        if count > maximum {
            return Err(PlanError::TooLarge);
        }
        Ok(count)
    }

    fn limit_node_value(&mut self, header_offset: usize) -> Result<u32, PlanError> {
        let header_len = crate::query_abi::OPTIMIZER_PLAN_LIMIT_NODE_RECORD_HEADER_BYTES as usize;
        let end = header_offset
            .checked_add(header_len)
            .ok_or(PlanError::TooLarge)?;
        let header = self
            .bytes
            .get(header_offset..end)
            .ok_or(PlanError::Truncated)?;
        let (node_tag, row_limit) =
            crate::query_abi::decode_optimizer_plan_limit_node_record(header)
                .ok_or(PlanError::Truncated)?;
        if node_tag != 4 {
            return Err(PlanError::InvalidPipeline);
        }
        self.offset = end;
        Ok(row_limit)
    }

    fn text_predicate_length(&mut self, header_offset: usize) -> Result<u16, PlanError> {
        let header_len =
            crate::query_abi::OPTIMIZER_PLAN_TEXT_PREDICATE_HEADER_RECORD_HEADER_BYTES as usize;
        let end = header_offset
            .checked_add(header_len)
            .ok_or(PlanError::TooLarge)?;
        let header = self
            .bytes
            .get(header_offset..end)
            .ok_or(PlanError::Truncated)?;
        let (node_tag, text_length) =
            crate::query_abi::decode_optimizer_plan_text_predicate_header_record(header)
                .ok_or(PlanError::Truncated)?;
        if node_tag != 0x03 {
            return Err(PlanError::InvalidEncoding);
        }
        self.offset = end;
        Ok(text_length)
    }

    fn integer_predicate_value(&mut self, header_offset: usize) -> Result<i64, PlanError> {
        let header_len =
            crate::query_abi::OPTIMIZER_PLAN_INTEGER_PREDICATE_RECORD_HEADER_BYTES as usize;
        let end = header_offset
            .checked_add(header_len)
            .ok_or(PlanError::TooLarge)?;
        let header = self
            .bytes
            .get(header_offset..end)
            .ok_or(PlanError::Truncated)?;
        let (node_tag, integer_value) =
            crate::query_abi::decode_optimizer_plan_integer_predicate_record(header)
                .ok_or(PlanError::Truncated)?;
        if node_tag != 0x02 {
            return Err(PlanError::InvalidEncoding);
        }
        self.offset = end;
        Ok(integer_value)
    }

    fn column_predicate_id(&mut self, header_offset: usize) -> Result<[u8; 16], PlanError> {
        let header_len =
            crate::query_abi::OPTIMIZER_PLAN_COLUMN_PREDICATE_RECORD_HEADER_BYTES as usize;
        let end = header_offset
            .checked_add(header_len)
            .ok_or(PlanError::TooLarge)?;
        let header = self
            .bytes
            .get(header_offset..end)
            .ok_or(PlanError::Truncated)?;
        let (node_tag, column_id) =
            crate::query_abi::decode_optimizer_plan_column_predicate_record(header)
                .ok_or(PlanError::Truncated)?;
        if node_tag != 0x10 {
            return Err(PlanError::InvalidEncoding);
        }
        self.offset = end;
        Ok(column_id)
    }

    fn compare_predicate_operator(&mut self, header_offset: usize) -> Result<u8, PlanError> {
        let header_len =
            crate::query_abi::OPTIMIZER_PLAN_COMPARE_PREDICATE_HEADER_RECORD_HEADER_BYTES as usize;
        let end = header_offset
            .checked_add(header_len)
            .ok_or(PlanError::TooLarge)?;
        let header = self
            .bytes
            .get(header_offset..end)
            .ok_or(PlanError::Truncated)?;
        let (node_tag, comparison_operator) =
            crate::query_abi::decode_optimizer_plan_compare_predicate_header_record(header)
                .ok_or(PlanError::Truncated)?;
        if node_tag != 0x20 {
            return Err(PlanError::InvalidEncoding);
        }
        self.offset = end;
        Ok(comparison_operator)
    }

    fn boolean_predicate_header(&mut self, header_offset: usize) -> Result<(u8, u8), PlanError> {
        let header_len =
            crate::query_abi::OPTIMIZER_PLAN_BOOLEAN_PREDICATE_HEADER_RECORD_HEADER_BYTES as usize;
        let end = header_offset
            .checked_add(header_len)
            .ok_or(PlanError::TooLarge)?;
        let header = self
            .bytes
            .get(header_offset..end)
            .ok_or(PlanError::Truncated)?;
        let (node_tag, child_count) =
            crate::query_abi::decode_optimizer_plan_boolean_predicate_header_record(header)
                .ok_or(PlanError::Truncated)?;
        if !matches!(node_tag, 0x30 | 0x31) {
            return Err(PlanError::InvalidEncoding);
        }
        self.offset = end;
        Ok((node_tag, child_count))
    }

    fn not_predicate_tag(&mut self, header_offset: usize) -> Result<(), PlanError> {
        let header_len =
            crate::query_abi::OPTIMIZER_PLAN_NOT_PREDICATE_RECORD_HEADER_BYTES as usize;
        let end = header_offset
            .checked_add(header_len)
            .ok_or(PlanError::TooLarge)?;
        let header = self
            .bytes
            .get(header_offset..end)
            .ok_or(PlanError::Truncated)?;
        let node_tag = crate::query_abi::decode_optimizer_plan_not_predicate_record(header)
            .ok_or(PlanError::Truncated)?;
        if node_tag != 0x32 {
            return Err(PlanError::InvalidEncoding);
        }
        self.offset = end;
        Ok(())
    }

    fn null_predicate_tag(&mut self, header_offset: usize) -> Result<(), PlanError> {
        let header_len =
            crate::query_abi::OPTIMIZER_PLAN_NULL_PREDICATE_RECORD_HEADER_BYTES as usize;
        let end = header_offset
            .checked_add(header_len)
            .ok_or(PlanError::TooLarge)?;
        let header = self
            .bytes
            .get(header_offset..end)
            .ok_or(PlanError::Truncated)?;
        let node_tag = crate::query_abi::decode_optimizer_plan_null_predicate_record(header)
            .ok_or(PlanError::Truncated)?;
        if node_tag != 0x00 {
            return Err(PlanError::InvalidEncoding);
        }
        self.offset = end;
        Ok(())
    }

    fn boolean_literal_value(&mut self, header_offset: usize) -> Result<bool, PlanError> {
        let header_len =
            crate::query_abi::OPTIMIZER_PLAN_BOOLEAN_LITERAL_PREDICATE_RECORD_HEADER_BYTES as usize;
        let end = header_offset
            .checked_add(header_len)
            .ok_or(PlanError::TooLarge)?;
        let header = self
            .bytes
            .get(header_offset..end)
            .ok_or(PlanError::Truncated)?;
        let (node_tag, boolean_value) =
            crate::query_abi::decode_optimizer_plan_boolean_literal_predicate_record(header)
                .ok_or(PlanError::Truncated)?;
        if node_tag != 0x01 {
            return Err(PlanError::InvalidEncoding);
        }
        self.offset = end;
        match boolean_value {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(PlanError::InvalidEncoding),
        }
    }

    fn column(&mut self) -> Result<BoundColumn, PlanError> {
        let bytes =
            self.take(crate::query_abi::OPTIMIZER_PLAN_COLUMN_RECORD_HEADER_BYTES as usize)?;
        let (column_id, ordinal, type_code, parameters, nullable) =
            crate::query_abi::decode_optimizer_plan_column_record(bytes)
                .ok_or(PlanError::Truncated)?;
        let nullable = match nullable {
            0 => false,
            1 => true,
            _ => return Err(PlanError::InvalidEncoding),
        };
        let scalar_type = decode_scalar_type(type_code, parameters)?;
        Ok(BoundColumn {
            column_id,
            ordinal,
            scalar_type,
            nullable,
        })
    }

    fn predicate(&mut self, scan_columns: &[BoundColumn]) -> Result<BoundPredicate, PlanError> {
        let mut nodes = 0;
        self.predicate_at_depth(scan_columns, 1, &mut nodes)
    }

    fn predicate_at_depth(
        &mut self,
        scan_columns: &[BoundColumn],
        depth: usize,
        nodes: &mut usize,
    ) -> Result<BoundPredicate, PlanError> {
        if depth > crate::optimizer_request::MAX_PREDICATE_DEPTH {
            return Err(PlanError::InvalidPredicate);
        }
        *nodes += 1;
        if *nodes > crate::optimizer_request::MAX_PREDICATE_NODES {
            return Err(PlanError::InvalidPredicate);
        }
        let tag_offset = self.offset;
        let tag = self.u8()?;
        match tag {
            0x00 => {
                self.null_predicate_tag(tag_offset)?;
                Ok(BoundPredicate::Null)
            }
            0x01 => Ok(BoundPredicate::Boolean(
                self.boolean_literal_value(tag_offset)?,
            )),
            0x02 => Ok(BoundPredicate::Integer(
                self.integer_predicate_value(tag_offset)?,
            )),
            0x03 => {
                let length = usize::from(self.text_predicate_length(tag_offset)?);
                if length > crate::optimizer_request::MAX_TEXT_LITERAL_BYTES {
                    return Err(PlanError::TooLarge);
                }
                let text = std::str::from_utf8(self.take(length)?)
                    .map_err(|_| PlanError::InvalidEncoding)?
                    .to_owned();
                Ok(BoundPredicate::Text(text))
            }
            0x10 => {
                let column_id = self.column_predicate_id(tag_offset)?;
                let column = scan_columns
                    .iter()
                    .find(|column| column.column_id == column_id)
                    .ok_or(PlanError::InvalidColumnReference)?;
                Ok(BoundPredicate::Column(column.clone()))
            }
            0x20 => {
                let operator = match self.compare_predicate_operator(tag_offset)? {
                    0 => crate::optimizer_request::Comparison::Equal,
                    1 => crate::optimizer_request::Comparison::NotEqual,
                    2 => crate::optimizer_request::Comparison::Less,
                    3 => crate::optimizer_request::Comparison::LessOrEqual,
                    4 => crate::optimizer_request::Comparison::Greater,
                    5 => crate::optimizer_request::Comparison::GreaterOrEqual,
                    _ => return Err(PlanError::InvalidEncoding),
                };
                let left = Box::new(self.predicate_at_depth(scan_columns, depth + 1, nodes)?);
                let right = Box::new(self.predicate_at_depth(scan_columns, depth + 1, nodes)?);
                Ok(BoundPredicate::Compare {
                    operator,
                    left,
                    right,
                })
            }
            0x40 | 0x41 => {
                let child = Box::new(self.predicate_at_depth(scan_columns, depth + 1, nodes)?);
                if !matches!(child.as_ref(), BoundPredicate::Column(_)) {
                    return Err(PlanError::InvalidPredicate);
                }
                if tag == 0x40 {
                    Ok(BoundPredicate::IsNull(child))
                } else {
                    Ok(BoundPredicate::IsNotNull(child))
                }
            }
            0x30 | 0x31 => {
                let (tag, child_count) = self.boolean_predicate_header(tag_offset)?;
                let count = usize::from(child_count);
                if !(2..=16).contains(&count) {
                    return Err(PlanError::InvalidPredicate);
                }
                let mut children = Vec::with_capacity(count);
                for _ in 0..count {
                    children.push(self.predicate_at_depth(scan_columns, depth + 1, nodes)?);
                }
                if tag == 0x30 {
                    Ok(BoundPredicate::And(children))
                } else {
                    Ok(BoundPredicate::Or(children))
                }
            }
            0x32 => {
                self.not_predicate_tag(tag_offset)?;
                Ok(BoundPredicate::Not(Box::new(self.predicate_at_depth(
                    scan_columns,
                    depth + 1,
                    nodes,
                )?)))
            }
            _ => Err(PlanError::InvalidEncoding),
        }
    }
}

fn decode_scalar_type(code: u8, parameters: [u8; 4]) -> Result<ScalarType, PlanError> {
    match code {
        1 if parameters == [0; 4] => Ok(ScalarType::Boolean),
        2 if parameters == [0; 4] => Ok(ScalarType::SmallInt),
        3 if parameters == [0; 4] => Ok(ScalarType::Integer),
        4 if parameters == [0; 4] => Ok(ScalarType::BigInt),
        5 if (1..=38).contains(&parameters[0])
            && parameters[1] <= parameters[0]
            && parameters[2..] == [0; 2] =>
        {
            Ok(ScalarType::Decimal {
                precision: parameters[0],
                scale: parameters[1],
            })
        }
        6 if parameters == [0; 4] => Ok(ScalarType::Text),
        7 if u32::from_be_bytes(parameters) != 0 => Ok(ScalarType::Varchar {
            max_chars: u32::from_be_bytes(parameters),
        }),
        8 if parameters == [0; 4] => Ok(ScalarType::Bytea),
        9 if parameters == [0; 4] => Ok(ScalarType::Uuid),
        10 if parameters == [0; 4] => Ok(ScalarType::Date),
        11 if parameters == [0; 4] => Ok(ScalarType::TimestampTz),
        _ => Err(PlanError::InvalidEncoding),
    }
}

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
    let unique_probe = bound
        .predicate
        .as_ref()
        .and_then(|predicate| select_unique_point_probe(predicate, &bound.unique_access_paths));
    let range_scan = if unique_probe.is_none() {
        bound.predicate.as_ref().and_then(|predicate| {
            select_index_range_scan(
                predicate,
                &bound.unique_access_paths,
                &bound.range_access_paths,
            )
        })
    } else {
        None
    };

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
    let plan = SingleTablePlanV1 {
        relation,
        nodes,
        unique_probe,
        range_scan,
        empty_view_hint: false,
    };
    validate_plan_v1(&plan)?;
    Ok(plan)
}

fn select_index_range_scan(
    predicate: &BoundPredicate,
    unique_paths: &[crate::metadata_discovery::AuthorizedUniqueAccessPath],
    range_paths: &[crate::metadata_discovery::AuthorizedRangeAccessPath],
) -> Option<IndexRangeScanV1> {
    let (key_column, lower, upper) = index_range_bounds(predicate)?;
    let index_id = unique_paths
        .iter()
        .map(|path| (&path.index_id, &path.column_ids))
        .chain(
            range_paths
                .iter()
                .map(|path| (&path.index_id, &path.column_ids)),
        )
        .find(|(index_id, column_ids)| {
            **index_id != [0; 16] && column_ids.first() == Some(&key_column.column_id)
        })
        .map(|(index_id, _)| *index_id)?;
    Some(IndexRangeScanV1 {
        index_id,
        key_column,
        lower,
        upper,
    })
}

fn index_range_bounds(
    predicate: &BoundPredicate,
) -> Option<(
    BoundColumn,
    Option<IndexRangeBoundV1>,
    Option<IndexRangeBoundV1>,
)> {
    match predicate {
        BoundPredicate::Compare { .. } => index_range_comparison(predicate),
        BoundPredicate::And(children) if children.len() == 2 => {
            let (left_column, left_lower, left_upper) = index_range_comparison(&children[0])?;
            let (right_column, right_lower, right_upper) = index_range_comparison(&children[1])?;
            if left_column != right_column
                || left_lower.is_some() == right_lower.is_some()
                || left_upper.is_some() == right_upper.is_some()
            {
                return None;
            }
            Some((
                left_column,
                left_lower.or(right_lower),
                left_upper.or(right_upper),
            ))
        }
        _ => None,
    }
}

fn index_range_comparison(
    predicate: &BoundPredicate,
) -> Option<(
    BoundColumn,
    Option<IndexRangeBoundV1>,
    Option<IndexRangeBoundV1>,
)> {
    let BoundPredicate::Compare {
        operator,
        left,
        right,
    } = predicate
    else {
        return None;
    };
    let (column, value, operator) = match (left.as_ref(), right.as_ref()) {
        (BoundPredicate::Column(column), value) if is_point_probe_literal(column, value) => {
            (column.clone(), value, *operator)
        }
        (value, BoundPredicate::Column(column)) if is_point_probe_literal(column, value) => {
            let reverse = match operator {
                crate::optimizer_request::Comparison::Equal => {
                    crate::optimizer_request::Comparison::Equal
                }
                crate::optimizer_request::Comparison::Less => {
                    crate::optimizer_request::Comparison::Greater
                }
                crate::optimizer_request::Comparison::LessOrEqual => {
                    crate::optimizer_request::Comparison::GreaterOrEqual
                }
                crate::optimizer_request::Comparison::Greater => {
                    crate::optimizer_request::Comparison::Less
                }
                crate::optimizer_request::Comparison::GreaterOrEqual => {
                    crate::optimizer_request::Comparison::LessOrEqual
                }
                _ => return None,
            };
            (column.clone(), value, reverse)
        }
        _ => return None,
    };
    let bound = |inclusive| {
        Some(IndexRangeBoundV1 {
            value: value.clone(),
            inclusive,
        })
    };
    let (lower, upper) = match operator {
        crate::optimizer_request::Comparison::Equal => (bound(true), bound(true)),
        crate::optimizer_request::Comparison::Less => (None, bound(false)),
        crate::optimizer_request::Comparison::LessOrEqual => (None, bound(true)),
        crate::optimizer_request::Comparison::Greater => (bound(false), None),
        crate::optimizer_request::Comparison::GreaterOrEqual => (bound(true), None),
        _ => return None,
    };
    Some((column, lower, upper))
}

fn select_unique_point_probe(
    predicate: &BoundPredicate,
    paths: &[crate::metadata_discovery::AuthorizedUniqueAccessPath],
) -> Option<UniquePointProbeV1> {
    let BoundPredicate::Compare {
        operator: crate::optimizer_request::Comparison::Equal,
        left,
        right,
    } = predicate
    else {
        return None;
    };
    let (column, value) = match (left.as_ref(), right.as_ref()) {
        (BoundPredicate::Column(column), value) if is_point_probe_literal(column, value) => {
            (column, value)
        }
        (value, BoundPredicate::Column(column)) if is_point_probe_literal(column, value) => {
            (column, value)
        }
        _ => return None,
    };
    let path = paths.iter().find(|path| {
        path.index_id != [0; 16] && path.column_ids.as_slice() == [column.column_id]
    })?;
    Some(UniquePointProbeV1 {
        index_id: path.index_id,
        key_column: column.clone(),
        value: value.clone(),
    })
}

fn is_point_probe_literal(column: &BoundColumn, value: &BoundPredicate) -> bool {
    matches!(
        (column.scalar_type, value),
        (ScalarType::Boolean, BoundPredicate::Boolean(_))
            | (ScalarType::Text, BoundPredicate::Text(_))
    ) || matches!(
        (column.scalar_type, value),
        (ScalarType::Integer, BoundPredicate::Integer(value))
            if i32::try_from(*value).is_ok()
    )
}

/// Lower authorized join bindings to a canonical join plan. Each scan reads
/// the ordinal-sorted union of its projected and equality-key columns.
pub fn plan_join_select_v1(bound: BoundJoinSelect) -> Result<JoinSelectPlanV1, PlanError> {
    plan_join_select_v1_with_estimates(bound, None, None)
}

/// Lower a join and use exact authorized populations to choose a physical
/// strategy. A known small outer input may use an eligible index on the other
/// side. When both known inputs exceed the measured small-input region, prefer
/// hash join; unknown estimates retain the deterministic index preference.
pub fn plan_join_select_v1_with_estimates(
    bound: BoundJoinSelect,
    left_visible_rows: Option<u64>,
    right_visible_rows: Option<u64>,
) -> Result<JoinSelectPlanV1, PlanError> {
    let index_nested_loop =
        select_index_nested_loop_join(&bound, left_visible_rows, right_visible_rows);
    let relation = |relation: crate::optimizer_binding::BoundJoinRelation| RelationReference {
        schema_name: bound.schema_name.clone(),
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        relation_name: relation.relation_name,
        relation_id: relation.relation_id,
        table_epoch: relation.table_epoch,
    };
    let left_relation = relation(bound.left_relation);
    let right_relation = relation(bound.right_relation);
    let mut left_scan_columns = Vec::new();
    let mut right_scan_columns = Vec::new();
    for projection in &bound.projections {
        add_column(
            match projection.relation_side {
                JoinRelationSide::Left => &mut left_scan_columns,
                JoinRelationSide::Right => &mut right_scan_columns,
            },
            &projection.column,
        )?;
    }
    for key in &bound.join_keys {
        add_column(&mut left_scan_columns, &key.left_column)?;
        add_column(&mut right_scan_columns, &key.right_column)?;
    }
    if let Some(filter) = &bound.filter {
        add_column(
            match filter.relation_side {
                JoinRelationSide::Left => &mut left_scan_columns,
                JoinRelationSide::Right => &mut right_scan_columns,
            },
            &filter.column,
        )?;
    }
    left_scan_columns.sort_by_key(|column| column.ordinal);
    right_scan_columns.sort_by_key(|column| column.ordinal);
    let plan = JoinSelectPlanV1 {
        schema_name: bound.schema_name,
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        left_relation,
        right_relation,
        left_scan_columns,
        right_scan_columns,
        projections: bound
            .projections
            .into_iter()
            .map(|projection| (projection.relation_side, projection.column))
            .collect(),
        join_keys: bound
            .join_keys
            .into_iter()
            .map(|key| (key.left_column, key.right_column))
            .collect(),
        limit: bound.limit,
        filter: bound.filter,
        index_nested_loop,
    };
    validate_join_select_plan_v1(&plan)?;
    Ok(plan)
}

fn select_index_nested_loop_join(
    bound: &BoundJoinSelect,
    left_visible_rows: Option<u64>,
    right_visible_rows: Option<u64>,
) -> Option<IndexNestedLoopJoinV1> {
    fn eligible(
        paths: impl Iterator<Item = ([u8; 16], Vec<[u8; 16]>)>,
        key_columns: &[BoundColumn],
        side: JoinRelationSide,
    ) -> Option<IndexNestedLoopJoinV1> {
        let mut paths = paths
            .filter(|(index_id, columns)| *index_id != [0; 16] && !columns.is_empty())
            .collect::<Vec<_>>();
        paths.sort_by_key(|(index_id, _)| *index_id);
        for (index_id, column_ids) in paths {
            let Some(key_column) = key_columns
                .iter()
                .find(|column| column.column_id == column_ids[0])
            else {
                continue;
            };
            if is_supported_index_join_key(key_column) {
                return Some(IndexNestedLoopJoinV1 {
                    outer_side: match side {
                        JoinRelationSide::Left => JoinRelationSide::Right,
                        JoinRelationSide::Right => JoinRelationSide::Left,
                    },
                    inner_side: side,
                    index_id,
                    key_column: key_column.clone(),
                });
            }
        }
        None
    }

    let right = eligible(
        bound
            .right_relation
            .unique_access_paths
            .iter()
            .map(|path| (path.index_id, path.column_ids.clone()))
            .chain(
                bound
                    .right_relation
                    .range_access_paths
                    .iter()
                    .map(|path| (path.index_id, path.column_ids.clone())),
            ),
        &bound
            .join_keys
            .iter()
            .map(|key| key.right_column.clone())
            .collect::<Vec<_>>(),
        JoinRelationSide::Right,
    );
    let left = eligible(
        bound
            .left_relation
            .unique_access_paths
            .iter()
            .map(|path| (path.index_id, path.column_ids.clone()))
            .chain(
                bound
                    .left_relation
                    .range_access_paths
                    .iter()
                    .map(|path| (path.index_id, path.column_ids.clone())),
            ),
        &bound
            .join_keys
            .iter()
            .map(|key| key.left_column.clone())
            .collect::<Vec<_>>(),
        JoinRelationSide::Left,
    );

    if left_visible_rows == Some(0) || right_visible_rows == Some(0) {
        return None;
    }
    match (left_visible_rows, right_visible_rows) {
        (Some(left_rows), Some(right_rows))
            if left_rows <= SMALL_POPULATION_SCAN_ROWS
                && right_rows <= SMALL_POPULATION_SCAN_ROWS =>
        {
            return None;
        }
        (Some(left_rows), Some(right_rows))
            if left_rows <= SMALL_POPULATION_SCAN_ROWS
                && right_rows > SMALL_POPULATION_SCAN_ROWS =>
        {
            return right;
        }
        (Some(left_rows), Some(right_rows))
            if right_rows <= SMALL_POPULATION_SCAN_ROWS
                && left_rows > SMALL_POPULATION_SCAN_ROWS =>
        {
            return left;
        }
        (Some(left_rows), None) if (1..=SMALL_POPULATION_SCAN_ROWS).contains(&left_rows) => {
            return right;
        }
        (None, Some(right_rows)) if (1..=SMALL_POPULATION_SCAN_ROWS).contains(&right_rows) => {
            return left;
        }
        (Some(left_rows), Some(right_rows))
            if left_rows > SMALL_POPULATION_SCAN_ROWS
                && left_rows <= SKEWED_JOIN_OUTER_MAX_ROWS
                && right_rows >= left_rows * SKEWED_JOIN_INPUT_RATIO =>
        {
            return right;
        }
        (Some(left_rows), Some(right_rows))
            if right_rows > SMALL_POPULATION_SCAN_ROWS
                && right_rows <= SKEWED_JOIN_OUTER_MAX_ROWS
                && left_rows >= right_rows * SKEWED_JOIN_INPUT_RATIO =>
        {
            return left;
        }
        (Some(_), Some(_)) => return None,
        _ => {}
    }

    right.or(left)
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
    Ok(())
}

fn is_supported_range_key(column: &BoundColumn) -> bool {
    matches!(
        column.scalar_type,
        ScalarType::Boolean | ScalarType::Integer | ScalarType::Text
    )
}

fn is_supported_index_join_key(column: &BoundColumn) -> bool {
    valid_scalar_type(column.scalar_type)
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

fn validate_columns_allow_duplicates(columns: &[BoundColumn]) -> Result<(), PlanError> {
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
    let mut nodes = 0;
    collect_predicate_columns_at_depth(predicate, columns, 1, &mut nodes)
}

fn collect_predicate_columns_at_depth(
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

fn valid_identifier(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 63
        && (bytes[0].is_ascii_alphabetic() || bytes[0] == b'_')
        && bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
}

fn valid_scalar_type(scalar_type: ScalarType) -> bool {
    match scalar_type {
        ScalarType::Decimal { precision, scale } => {
            (1..=38).contains(&precision) && scale <= precision
        }
        ScalarType::Varchar { max_chars } => max_chars != 0,
        _ => true,
    }
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
            unique_access_paths: Vec::new(),
            range_access_paths: Vec::new(),
            limit: Some(5),
        }
    }

    #[test]
    fn ordered_select_roundtrips_hidden_sort_key_and_limit() {
        let output = column(1, 0, ScalarType::Integer);
        let sort_key = column(2, 1, ScalarType::Boolean);
        let bound = BoundOrderedSelect {
            schema_name: "sales".to_owned(),
            schema_id: [0x10; 16],
            schema_version: 4,
            table_epoch: 6,
            relation_id: [0x20; 16],
            relation_name: "orders".to_owned(),
            projections: vec![output.clone()],
            order_key: sort_key.clone(),
            descending: true,
            limit: Some(5),
        };
        let plan = plan_ordered_select_v1(bound).unwrap();
        assert_eq!(plan.scan_columns, [output, sort_key.clone()]);
        assert_eq!(plan.order_key, sort_key);
        assert!(plan.descending);
        assert_eq!(plan.limit, Some(5));

        let bytes = encode_ordered_select_plan_v1(&plan).unwrap();
        assert_eq!(decode_ordered_select_plan_v1(&bytes).unwrap(), plan);
        let mut malformed = plan;
        malformed.scan_columns.pop();
        assert_eq!(
            encode_ordered_select_plan_v1(&malformed),
            Err(PlanError::InvalidColumnReference)
        );
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
        assert!(plan.unique_probe.is_none());
        assert_eq!(validate_plan_v1(&plan), Ok(()));
    }

    #[test]
    fn unique_point_hint_requires_an_approved_exact_equality_path() {
        let key = column(2, 1, ScalarType::Boolean);
        let mut bound = bound_select();
        bound.unique_access_paths = vec![crate::metadata_discovery::AuthorizedUniqueAccessPath {
            index_id: [0x44; 16],
            column_ids: vec![key.column_id],
        }];
        let plan = plan_select_v1(bound.clone()).unwrap();
        assert_eq!(
            plan.unique_probe,
            Some(UniquePointProbeV1 {
                index_id: [0x44; 16],
                key_column: key.clone(),
                value: BoundPredicate::Boolean(true),
            })
        );
        let mut small_population = plan.clone();
        assert!(apply_population_scan_heuristic(&mut small_population, Some(4)).unwrap());
        assert!(small_population.unique_probe.is_none());
        let mut unknown_population = plan.clone();
        assert!(!apply_population_scan_heuristic(&mut unknown_population, None).unwrap());
        assert!(unknown_population.unique_probe.is_some());
        let mut broad_match = plan.clone();
        assert!(
            !apply_range_selectivity_scan_heuristic(&mut broad_match, Some(130), Some(120),)
                .unwrap()
        );
        assert!(broad_match.unique_probe.is_some());
        let encoded = encode_plan_v1(&plan).unwrap();
        assert_eq!(decode_plan_v1(&encoded).unwrap(), plan);

        bound.unique_access_paths.clear();
        assert!(
            plan_select_v1(bound.clone())
                .unwrap()
                .unique_probe
                .is_none()
        );
        bound.range_access_paths = vec![crate::metadata_discovery::AuthorizedRangeAccessPath {
            index_id: [0x46; 16],
            column_ids: vec![key.column_id],
        }];
        assert!(
            plan_select_v1(bound.clone())
                .unwrap()
                .unique_probe
                .is_none()
        );
        bound.unique_access_paths = vec![crate::metadata_discovery::AuthorizedUniqueAccessPath {
            index_id: [0x44; 16],
            column_ids: vec![key.column_id, [0x45; 16]],
        }];
        assert!(plan_select_v1(bound).unwrap().unique_probe.is_none());

        let integer_key = column(2, 1, ScalarType::Integer);
        let mut out_of_range = bound_select();
        out_of_range.predicate = Some(BoundPredicate::Compare {
            operator: crate::optimizer_request::Comparison::Equal,
            left: Box::new(BoundPredicate::Column(integer_key.clone())),
            right: Box::new(BoundPredicate::Integer(i64::MAX)),
        });
        out_of_range.unique_access_paths =
            vec![crate::metadata_discovery::AuthorizedUniqueAccessPath {
                index_id: [0x44; 16],
                column_ids: vec![integer_key.column_id],
            }];
        assert!(plan_select_v1(out_of_range).unwrap().unique_probe.is_none());
    }

    #[test]
    fn bounded_range_hint_roundtrips_only_for_an_approved_leading_key() {
        let key = column(3, 1, ScalarType::Integer);
        let mut bound = bound_select();
        bound.predicate = Some(BoundPredicate::Compare {
            operator: Comparison::GreaterOrEqual,
            left: Box::new(BoundPredicate::Column(key.clone())),
            right: Box::new(BoundPredicate::Integer(42)),
        });
        bound.range_access_paths = vec![crate::metadata_discovery::AuthorizedRangeAccessPath {
            index_id: [0x46; 16],
            column_ids: vec![key.column_id, [0x47; 16]],
        }];
        let plan = plan_select_v1(bound.clone()).unwrap();
        assert_eq!(plan.unique_probe, None);
        assert_eq!(
            plan.range_scan,
            Some(IndexRangeScanV1 {
                index_id: [0x46; 16],
                key_column: key.clone(),
                lower: Some(IndexRangeBoundV1 {
                    value: BoundPredicate::Integer(42),
                    inclusive: true,
                }),
                upper: None,
            })
        );
        let mut small_population = plan.clone();
        assert!(apply_population_scan_heuristic(&mut small_population, Some(4)).unwrap());
        assert!(small_population.range_scan.is_none());
        assert!(!apply_population_scan_heuristic(&mut small_population, Some(4)).unwrap());
        let mut unknown_population = plan.clone();
        assert!(!apply_population_scan_heuristic(&mut unknown_population, None).unwrap());
        assert!(unknown_population.range_scan.is_some());
        let mut larger_population = plan.clone();
        assert!(!apply_population_scan_heuristic(&mut larger_population, Some(5)).unwrap());
        assert!(larger_population.range_scan.is_some());
        assert!(
            !apply_range_selectivity_scan_heuristic(&mut larger_population, Some(130), Some(64),)
                .unwrap()
        );
        assert!(larger_population.range_scan.is_some());
        assert!(
            apply_range_selectivity_scan_heuristic(&mut larger_population, Some(130), Some(65),)
                .unwrap()
        );
        assert!(larger_population.range_scan.is_none());
        assert!(matches!(
            apply_range_selectivity_scan_heuristic(&mut plan.clone(), Some(130), Some(131)),
            Err(PlanError::InvalidPredicate)
        ));
        let encoded = encode_plan_v1(&plan).unwrap();
        assert_eq!(decode_plan_v1(&encoded).unwrap(), plan);

        bound.range_access_paths.clear();
        assert!(plan_select_v1(bound.clone()).unwrap().range_scan.is_none());
        bound.range_access_paths = vec![crate::metadata_discovery::AuthorizedRangeAccessPath {
            index_id: [0x46; 16],
            column_ids: vec![[0x48; 16], key.column_id],
        }];
        assert!(plan_select_v1(bound.clone()).unwrap().range_scan.is_none());

        let mut mismatched = plan;
        let Some(PlanNode::Filter(BoundPredicate::Compare { right, .. })) =
            mismatched.nodes.get_mut(1)
        else {
            panic!("range plan must retain its predicate");
        };
        **right = BoundPredicate::Integer(43);
        assert_eq!(
            validate_plan_v1(&mismatched),
            Err(PlanError::InvalidPredicate)
        );
    }

    #[test]
    fn non_unique_range_hint_uses_equal_inclusive_bounds_for_equality() {
        let key = column(3, 1, ScalarType::Integer);
        let mut bound = bound_select();
        bound.predicate = Some(BoundPredicate::Compare {
            operator: Comparison::Equal,
            left: Box::new(BoundPredicate::Integer(42)),
            right: Box::new(BoundPredicate::Column(key.clone())),
        });
        bound.unique_access_paths.clear();
        bound.range_access_paths = vec![crate::metadata_discovery::AuthorizedRangeAccessPath {
            index_id: [0x46; 16],
            column_ids: vec![key.column_id, [0x47; 16]],
        }];

        let plan = plan_select_v1(bound).unwrap();
        let bound = IndexRangeBoundV1 {
            value: BoundPredicate::Integer(42),
            inclusive: true,
        };
        assert_eq!(
            plan.range_scan,
            Some(IndexRangeScanV1 {
                index_id: [0x46; 16],
                key_column: key,
                lower: Some(bound.clone()),
                upper: Some(bound),
            })
        );
        assert_eq!(
            decode_plan_v1(&encode_plan_v1(&plan).unwrap()).unwrap(),
            plan
        );
    }

    #[test]
    fn bounded_range_hint_accepts_a_two_sided_same_key_conjunction() {
        let key = column(3, 1, ScalarType::Integer);
        let mut bound = bound_select();
        bound.predicate = Some(BoundPredicate::And(vec![
            BoundPredicate::Compare {
                operator: Comparison::LessOrEqual,
                left: Box::new(BoundPredicate::Integer(10)),
                right: Box::new(BoundPredicate::Column(key.clone())),
            },
            BoundPredicate::Compare {
                operator: Comparison::Less,
                left: Box::new(BoundPredicate::Column(key.clone())),
                right: Box::new(BoundPredicate::Integer(20)),
            },
        ]));
        bound.range_access_paths = vec![crate::metadata_discovery::AuthorizedRangeAccessPath {
            index_id: [0x49; 16],
            column_ids: vec![key.column_id, [0x4a; 16]],
        }];

        let plan = plan_select_v1(bound).unwrap();
        assert_eq!(
            plan.range_scan,
            Some(IndexRangeScanV1 {
                index_id: [0x49; 16],
                key_column: key,
                lower: Some(IndexRangeBoundV1 {
                    value: BoundPredicate::Integer(10),
                    inclusive: true,
                }),
                upper: Some(IndexRangeBoundV1 {
                    value: BoundPredicate::Integer(20),
                    inclusive: false,
                }),
            })
        );
        assert_eq!(
            decode_plan_v1(&encode_plan_v1(&plan).unwrap()).unwrap(),
            plan
        );
        let mut mismatched = plan.clone();
        let Some(PlanNode::Filter(BoundPredicate::And(children))) = mismatched.nodes.get_mut(1)
        else {
            panic!("two-sided range plan must retain its conjunction");
        };
        let Some(BoundPredicate::Compare { right, .. }) = children.get_mut(1) else {
            panic!("upper range term must retain its comparison");
        };
        **right = BoundPredicate::Integer(21);
        assert_eq!(
            validate_plan_v1(&mismatched),
            Err(PlanError::InvalidPredicate)
        );
    }

    #[test]
    fn builds_and_roundtrips_a_bounded_delete_plan() {
        let bound = bound_select();
        let relation = RelationReference {
            schema_name: bound.schema_name,
            schema_id: bound.schema_id,
            schema_version: bound.schema_version,
            relation_name: bound.relation_name,
            relation_id: bound.relation_id,
            table_epoch: bound.table_epoch,
        };
        let predicate = BoundPredicate::And(vec![
            BoundPredicate::Compare {
                operator: Comparison::Equal,
                left: Box::new(BoundPredicate::Column(column(2, 1, ScalarType::Boolean))),
                right: Box::new(BoundPredicate::Boolean(true)),
            },
            BoundPredicate::Compare {
                operator: Comparison::Equal,
                left: Box::new(BoundPredicate::Column(column(1, 0, ScalarType::Integer))),
                right: Box::new(BoundPredicate::Integer(7)),
            },
        ]);

        let plan = plan_delete_v1(relation, predicate).unwrap();
        assert_eq!(
            plan.scan_columns,
            vec![
                column(1, 0, ScalarType::Integer),
                column(2, 1, ScalarType::Boolean),
            ]
        );
        assert_eq!(
            validate_delete_plan_v1(&plan),
            Ok(()),
            "the plan is valid before crossing the optimizer boundary"
        );
        let encoded = encode_delete_plan_v1(&plan).unwrap();
        assert_eq!(decode_delete_plan_v1(&encoded).unwrap(), plan);

        let mut malformed = encoded;
        *malformed.last_mut().unwrap() = 4;
        assert_eq!(
            decode_delete_plan_v1(&malformed),
            Err(PlanError::InvalidPipeline)
        );
    }

    #[test]
    fn delete_plan_supports_constant_predicates_with_full_shape_scans() {
        let bound = bound_select();
        let relation = RelationReference {
            schema_name: bound.schema_name,
            schema_id: bound.schema_id,
            schema_version: bound.schema_version,
            relation_name: bound.relation_name,
            relation_id: bound.relation_id,
            table_epoch: bound.table_epoch,
        };
        let plan = plan_delete_v1(relation, BoundPredicate::Boolean(true)).unwrap();
        assert!(plan.scan_columns.is_empty());
        let encoded = encode_delete_plan_v1(&plan).unwrap();
        let mut expected = vec![0x00, 0x01, PLAN_KIND_DELETE, 0x00];
        expected.extend_from_slice(&[5]);
        expected.extend_from_slice(b"sales");
        expected.extend_from_slice(&[0x10; 16]);
        expected.extend_from_slice(&4u64.to_be_bytes());
        expected.extend_from_slice(&[6]);
        expected.extend_from_slice(b"orders");
        expected.extend_from_slice(&[0x20; 16]);
        expected.extend_from_slice(&6u64.to_be_bytes());
        expected.extend_from_slice(&[3, 1, 0, 0, 2, 0x01, 0x01, DELETE_SINK_NODE]);
        assert_eq!(encoded, expected);
        assert_eq!(decode_delete_plan_v1(&encoded).unwrap(), plan);
    }

    #[test]
    fn delete_plan_rejects_non_boolean_and_unbounded_predicates() {
        let bound = bound_select();
        let relation = RelationReference {
            schema_name: bound.schema_name,
            schema_id: bound.schema_id,
            schema_version: bound.schema_version,
            relation_name: bound.relation_name,
            relation_id: bound.relation_id,
            table_epoch: bound.table_epoch,
        };
        assert_eq!(
            plan_delete_v1(relation.clone(), BoundPredicate::Integer(7)),
            Err(PlanError::InvalidPredicate)
        );

        let mut predicate = BoundPredicate::Boolean(true);
        for _ in 0..crate::optimizer_request::MAX_PREDICATE_DEPTH {
            predicate = BoundPredicate::Not(Box::new(predicate));
        }
        assert_eq!(
            plan_delete_v1(relation, predicate),
            Err(PlanError::InvalidPredicate)
        );
    }

    #[test]
    fn builds_and_roundtrips_a_bounded_update_plan() {
        let bound = bound_select();
        let relation = RelationReference {
            schema_name: bound.schema_name,
            schema_id: bound.schema_id,
            schema_version: bound.schema_version,
            relation_name: bound.relation_name,
            relation_id: bound.relation_id,
            table_epoch: bound.table_epoch,
        };
        let active = BoundColumn {
            nullable: true,
            ..column(2, 1, ScalarType::Boolean)
        };
        let predicate = BoundPredicate::Compare {
            operator: Comparison::Greater,
            left: Box::new(BoundPredicate::Column(column(1, 0, ScalarType::Integer))),
            right: Box::new(BoundPredicate::Integer(7)),
        };
        let assignments = vec![
            BoundUpdateAssignment {
                column: active.clone(),
                value: BoundUpdateValue::Parameter(2),
            },
            BoundUpdateAssignment {
                column: column(1, 0, ScalarType::Integer),
                value: BoundUpdateValue::Parameter(1),
            },
        ];
        let plan = plan_update_v1(relation, predicate, assignments).unwrap();
        assert_eq!(
            plan.scan_columns,
            vec![column(1, 0, ScalarType::Integer), active.clone()]
        );
        assert_eq!(validate_update_plan_v1(&plan), Ok(()));
        let encoded = encode_update_plan_v1(&plan).unwrap();
        assert_eq!(decode_update_plan_v1(&encoded).unwrap(), plan);

        let mut trailing = encoded.clone();
        trailing.push(0);
        assert_eq!(
            decode_update_plan_v1(&trailing),
            Err(PlanError::TrailingBytes)
        );
        let mut invalid_assignment_kind = encoded;
        let invalid_kind_offset = invalid_assignment_kind.len() - 5;
        invalid_assignment_kind[invalid_kind_offset] = 9;
        assert_eq!(
            decode_update_plan_v1(&invalid_assignment_kind),
            Err(PlanError::InvalidEncoding)
        );
    }

    #[test]
    fn update_plan_rejects_invalid_assignments_and_scan_shapes() {
        let bound = bound_select();
        let relation = RelationReference {
            schema_name: bound.schema_name,
            schema_id: bound.schema_id,
            schema_version: bound.schema_version,
            relation_name: bound.relation_name,
            relation_id: bound.relation_id,
            table_epoch: bound.table_epoch,
        };
        let predicate = BoundPredicate::Boolean(true);
        let required = column(1, 0, ScalarType::Integer);
        assert_eq!(
            plan_update_v1(
                relation.clone(),
                predicate.clone(),
                vec![BoundUpdateAssignment {
                    column: required.clone(),
                    value: BoundUpdateValue::Null,
                }],
            ),
            Err(PlanError::InvalidEncoding)
        );
        assert_eq!(
            plan_update_v1(
                relation.clone(),
                predicate.clone(),
                vec![BoundUpdateAssignment {
                    column: required.clone(),
                    value: BoundUpdateValue::Parameter(0),
                }],
            ),
            Err(PlanError::InvalidEncoding)
        );
        let valid_assignment = BoundUpdateAssignment {
            column: required.clone(),
            value: BoundUpdateValue::Parameter(1),
        };
        assert_eq!(
            plan_update_v1(
                relation.clone(),
                predicate.clone(),
                vec![valid_assignment.clone(), valid_assignment],
            ),
            Err(PlanError::InvalidColumnReference)
        );

        let mut invalid = plan_update_v1(
            relation,
            predicate,
            vec![BoundUpdateAssignment {
                column: required,
                value: BoundUpdateValue::Parameter(1),
            }],
        )
        .unwrap();
        invalid.scan_columns.clear();
        assert_eq!(
            validate_update_plan_v1(&invalid),
            Err(PlanError::InvalidColumnReference)
        );
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

    #[test]
    fn versioned_plan_encoding_round_trips_canonically() {
        let plan = plan_select_v1(bound_select()).unwrap();
        let bytes = encode_plan_v1(&plan).unwrap();
        assert_eq!(&bytes[..4], &[0, 1, 1, 0]);
        let decoded = decode_plan_v1(&bytes).unwrap();
        assert_eq!(decoded, plan);
        assert_eq!(encode_plan_v1(&decoded).unwrap(), bytes);
    }

    #[test]
    fn plan_decoder_rejects_malformed_and_noncanonical_payloads() {
        let plan = plan_select_v1(bound_select()).unwrap();
        let bytes = encode_plan_v1(&plan).unwrap();

        let mut wrong_version = bytes.clone();
        wrong_version[1] = 2;
        assert_eq!(
            decode_plan_v1(&wrong_version),
            Err(PlanError::UnsupportedVersion(2))
        );

        let mut truncated = bytes.clone();
        truncated.pop();
        assert_eq!(decode_plan_v1(&truncated), Err(PlanError::Truncated));

        let mut trailing = bytes.clone();
        trailing.push(0);
        assert_eq!(decode_plan_v1(&trailing), Err(PlanError::TrailingBytes));

        let mut unsupported_type = bytes;
        let scan_first_column_type_code = 89;
        unsupported_type[scan_first_column_type_code] = 0xff;
        assert_eq!(
            decode_plan_v1(&unsupported_type),
            Err(PlanError::InvalidEncoding)
        );

        let mut invalid_decimal = encode_plan_v1(&plan).unwrap();
        invalid_decimal[scan_first_column_type_code] = 5;
        invalid_decimal[scan_first_column_type_code + 1] = 39;
        invalid_decimal[scan_first_column_type_code + 2] = 0;
        invalid_decimal[scan_first_column_type_code + 3] = 0;
        invalid_decimal[scan_first_column_type_code + 4] = 0;
        assert_eq!(
            decode_plan_v1(&invalid_decimal),
            Err(PlanError::InvalidEncoding)
        );
    }

    #[test]
    fn text_predicate_header_matches_the_generated_wire_record() {
        let predicate = BoundPredicate::Text("hi".to_owned());
        let mut encoded = Vec::new();
        put_predicate(&mut encoded, &predicate).unwrap();
        assert_eq!(encoded, [0x03, 0x00, 0x02, b'h', b'i']);

        let mut reader = PlanReader {
            bytes: &encoded,
            offset: 0,
        };
        assert_eq!(reader.predicate(&[]).unwrap(), predicate);
        assert_eq!(reader.offset, encoded.len());
    }

    #[test]
    fn integer_predicate_record_matches_the_generated_wire_record() {
        let predicate = BoundPredicate::Integer(-2);
        let mut encoded = Vec::new();
        put_predicate(&mut encoded, &predicate).unwrap();
        assert_eq!(
            encoded,
            [0x02, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe]
        );

        let mut reader = PlanReader {
            bytes: &encoded,
            offset: 0,
        };
        assert_eq!(reader.predicate(&[]).unwrap(), predicate);
        assert_eq!(reader.offset, encoded.len());
    }

    #[test]
    fn column_predicate_record_matches_the_generated_wire_record() {
        let source_column = column(0xab, 0, ScalarType::Integer);
        let predicate = BoundPredicate::Column(source_column.clone());
        let mut encoded = Vec::new();
        put_predicate(&mut encoded, &predicate).unwrap();
        assert_eq!(encoded, [&[0x10][..], &[0xabu8; 16][..]].concat());

        let mut reader = PlanReader {
            bytes: &encoded,
            offset: 0,
        };
        assert_eq!(reader.predicate(&[source_column]).unwrap(), predicate);
        assert_eq!(reader.offset, encoded.len());
    }

    #[test]
    fn compare_predicate_header_matches_the_generated_wire_record() {
        let predicate = BoundPredicate::Compare {
            operator: Comparison::GreaterOrEqual,
            left: Box::new(BoundPredicate::Null),
            right: Box::new(BoundPredicate::Null),
        };
        let mut encoded = Vec::new();
        put_predicate(&mut encoded, &predicate).unwrap();
        assert_eq!(encoded, [0x20, 0x05, 0x00, 0x00]);

        let mut reader = PlanReader {
            bytes: &encoded,
            offset: 0,
        };
        assert_eq!(reader.predicate(&[]).unwrap(), predicate);
        assert_eq!(reader.offset, encoded.len());
    }

    #[test]
    fn boolean_predicate_header_matches_the_generated_wire_record() {
        let predicate = BoundPredicate::And(vec![
            BoundPredicate::Boolean(true),
            BoundPredicate::Boolean(false),
        ]);
        let mut encoded = Vec::new();
        put_predicate(&mut encoded, &predicate).unwrap();
        assert_eq!(encoded, [0x30, 0x02, 0x01, 0x01, 0x01, 0x00]);

        let mut reader = PlanReader {
            bytes: &encoded,
            offset: 0,
        };
        assert_eq!(reader.predicate(&[]).unwrap(), predicate);
        assert_eq!(reader.offset, encoded.len());
    }

    #[test]
    fn name_header_matches_the_generated_wire_record() {
        let mut encoded = Vec::new();
        put_name(&mut encoded, "sales").unwrap();
        assert_eq!(encoded, [0x05, b's', b'a', b'l', b'e', b's']);

        let mut reader = PlanReader {
            bytes: &encoded,
            offset: 0,
        };
        assert_eq!(reader.name().unwrap(), "sales");
        assert_eq!(reader.offset, encoded.len());
    }

    #[test]
    fn not_predicate_record_matches_the_generated_wire_record() {
        let predicate = BoundPredicate::Not(Box::new(BoundPredicate::Boolean(true)));
        let mut encoded = Vec::new();
        put_predicate(&mut encoded, &predicate).unwrap();
        assert_eq!(encoded, [0x32, 0x01, 0x01]);

        let mut reader = PlanReader {
            bytes: &encoded,
            offset: 0,
        };
        assert_eq!(reader.predicate(&[]).unwrap(), predicate);
        assert_eq!(reader.offset, encoded.len());
    }

    #[test]
    fn null_and_boolean_predicate_records_match_the_generated_wire_records() {
        for (predicate, expected) in [
            (BoundPredicate::Null, vec![0x00]),
            (BoundPredicate::Boolean(true), vec![0x01, 0x01]),
            (BoundPredicate::Boolean(false), vec![0x01, 0x00]),
        ] {
            let mut encoded = Vec::new();
            put_predicate(&mut encoded, &predicate).unwrap();
            assert_eq!(encoded, expected);

            let mut reader = PlanReader {
                bytes: &encoded,
                offset: 0,
            };
            assert_eq!(reader.predicate(&[]).unwrap(), predicate);
            assert_eq!(reader.offset, encoded.len());
        }
    }

    #[test]
    fn plan_encoder_rejects_invalid_scalar_type_parameters() {
        let mut plan = plan_select_v1(bound_select()).unwrap();
        let PlanNode::Scan { columns } = &mut plan.nodes[0] else {
            panic!("first node must scan")
        };
        columns[0].scalar_type = ScalarType::Decimal {
            precision: 0,
            scale: 0,
        };
        assert_eq!(encode_plan_v1(&plan), Err(PlanError::InvalidScalarType));
    }

    #[test]
    fn group_count_plan_roundtrips_and_rejects_unscanned_group_key() {
        let key = column(4, 1, ScalarType::Text);
        let plan = plan_group_count_v1(BoundGroupCount {
            schema_name: "sales".to_owned(),
            schema_id: [0x10; 16],
            schema_version: 4,
            table_epoch: 6,
            relation_id: [0x20; 16],
            relation_name: "orders".to_owned(),
            group_key: key.clone(),
            limit: Some(7),
        })
        .unwrap();
        let bytes = encode_group_count_plan_v1(&plan).unwrap();
        assert_eq!(decode_group_count_plan_v1(&bytes).unwrap(), plan);

        let mut invalid = plan;
        invalid.scan_columns.clear();
        assert_eq!(
            validate_group_count_plan_v1(&invalid),
            Err(PlanError::InvalidColumnReference)
        );
    }

    #[test]
    fn group_aggregate_plan_roundtrips_and_checks_argument_scan_shape() {
        let plan = plan_group_aggregate_v1(BoundGroupAggregate {
            schema_name: "public".into(),
            schema_id: [1; 16],
            schema_version: 1,
            table_epoch: 1,
            relation_id: [2; 16],
            relation_name: "items".into(),
            group_key: column(1, 0, ScalarType::Integer),
            aggregate_kind: 4,
            aggregate_column: Some(column(2, 1, ScalarType::BigInt)),
            limit: Some(5),
        })
        .unwrap();
        let bytes = encode_group_aggregate_plan_v1(&plan).unwrap();
        assert_eq!(decode_group_aggregate_plan_v1(&bytes).unwrap(), plan);
        let mut invalid = plan;
        invalid.scan_columns.pop();
        assert_eq!(
            validate_group_aggregate_plan_v1(&invalid),
            Err(PlanError::InvalidColumnReference)
        );

        let count_column_plan = plan_group_aggregate_v1(BoundGroupAggregate {
            schema_name: "public".into(),
            schema_id: [1; 16],
            schema_version: 1,
            table_epoch: 1,
            relation_id: [2; 16],
            relation_name: "items".into(),
            group_key: column(1, 0, ScalarType::Integer),
            aggregate_kind: 6,
            aggregate_column: Some(column(3, 2, ScalarType::Boolean)),
            limit: None,
        })
        .unwrap();
        let bytes = encode_group_aggregate_plan_v1(&count_column_plan).unwrap();
        assert_eq!(
            decode_group_aggregate_plan_v1(&bytes).unwrap(),
            count_column_plan
        );
    }

    #[test]
    fn ordered_group_aggregate_plan_roundtrips_and_rejects_bad_order_sources() {
        let mut avg_plan = None;
        for aggregate_kind in 1..=6 {
            let plan = plan_ordered_group_aggregate_v1(BoundOrderedGroupAggregate {
                schema_name: "public".into(),
                schema_id: [1; 16],
                schema_version: 1,
                table_epoch: 1,
                relation_id: [2; 16],
                relation_name: "items".into(),
                group_key: column(1, 0, ScalarType::Integer),
                aggregate_kind,
                aggregate_column: (aggregate_kind != 1).then(|| column(2, 1, ScalarType::BigInt)),
                order_items: vec![
                    GroupCountOrderItem {
                        source: 1,
                        descending: true,
                        nulls_first: false,
                    },
                    GroupCountOrderItem {
                        source: 0,
                        descending: false,
                        nulls_first: true,
                    },
                ],
                limit: Some(5),
            })
            .unwrap();
            let bytes = encode_ordered_group_aggregate_plan_v1(&plan).unwrap();
            assert_eq!(&bytes[..4], &[0, 1, 11, 0]);
            assert_eq!(
                decode_ordered_group_aggregate_plan_v1(&bytes).unwrap(),
                plan
            );
            if aggregate_kind == 5 {
                avg_plan = Some(plan);
            }
        }

        let mut invalid = avg_plan.unwrap();
        invalid.order_items[1].source = 1;
        assert_eq!(
            validate_ordered_group_aggregate_plan_v1(&invalid),
            Err(PlanError::InvalidColumnReference)
        );
    }

    #[test]
    fn ordered_group_count_plan_roundtrips_and_rejects_duplicate_sort_sources() {
        let key = column(4, 1, ScalarType::Text);
        let plan = plan_ordered_group_count_v1(BoundOrderedGroupCount {
            schema_name: "sales".to_owned(),
            schema_id: [0x10; 16],
            schema_version: 4,
            table_epoch: 6,
            relation_id: [0x20; 16],
            relation_name: "orders".to_owned(),
            group_key: key,
            order_items: vec![
                GroupCountOrderItem {
                    source: 1,
                    descending: true,
                    nulls_first: false,
                },
                GroupCountOrderItem {
                    source: 0,
                    descending: false,
                    nulls_first: true,
                },
            ],
            limit: Some(7),
        })
        .unwrap();
        let bytes = encode_ordered_group_count_plan_v1(&plan).unwrap();
        assert_eq!(&bytes[..4], &[0, 1, 8, 0]);
        assert_eq!(decode_ordered_group_count_plan_v1(&bytes).unwrap(), plan);

        let mut invalid = plan;
        invalid.order_items[1].source = 1;
        assert_eq!(
            validate_ordered_group_count_plan_v1(&invalid),
            Err(PlanError::InvalidColumnReference)
        );
    }

    #[test]
    fn ordered_composite_group_count_plan_roundtrips_and_validates_order_sources() {
        use crate::optimizer_request::CompositeGroupCountOrderItem;
        let keys = vec![
            column(2, 1, ScalarType::Boolean),
            column(1, 0, ScalarType::Integer),
        ];
        let plan = plan_ordered_composite_group_count_v1(BoundOrderedCompositeGroupCount {
            schema_name: "public".into(),
            schema_id: [1; 16],
            schema_version: 1,
            table_epoch: 1,
            relation_id: [2; 16],
            relation_name: "items".into(),
            group_keys: keys,
            order_items: vec![
                CompositeGroupCountOrderItem {
                    source: 1,
                    group_key_index: None,
                    descending: true,
                    nulls_first: false,
                },
                CompositeGroupCountOrderItem {
                    source: 0,
                    group_key_index: Some(0),
                    descending: false,
                    nulls_first: true,
                },
            ],
            limit: Some(4),
        })
        .unwrap();
        assert_eq!(
            plan.scan_columns
                .iter()
                .map(|column| column.ordinal)
                .collect::<Vec<_>>(),
            [0, 1]
        );
        let bytes = encode_ordered_composite_group_count_plan_v1(&plan).unwrap();
        assert_eq!(
            decode_ordered_composite_group_count_plan_v1(&bytes).unwrap(),
            plan
        );
        let mut invalid = plan;
        invalid.order_items[1].group_key_index = Some(1);
        invalid.order_items.push(CompositeGroupCountOrderItem {
            source: 0,
            group_key_index: Some(1),
            descending: false,
            nulls_first: false,
        });
        assert_eq!(
            validate_ordered_composite_group_count_plan_v1(&invalid),
            Err(PlanError::InvalidColumnReference)
        );
    }

    #[test]
    fn composite_group_count_plan_preserves_key_order_and_canonical_scan_shape() {
        let first = column(4, 1, ScalarType::Text);
        let second = column(3, 0, ScalarType::Integer);
        let plan = plan_composite_group_count_v1(BoundCompositeGroupCount {
            schema_name: "sales".to_owned(),
            schema_id: [0x10; 16],
            schema_version: 4,
            table_epoch: 6,
            relation_id: [0x20; 16],
            relation_name: "orders".to_owned(),
            group_keys: vec![first.clone(), second.clone()],
            limit: Some(7),
        })
        .unwrap();
        assert_eq!(plan.group_keys, [first.clone(), second.clone()]);
        assert_eq!(plan.scan_columns, [second, first]);
        let bytes = encode_composite_group_count_plan_v1(&plan).unwrap();
        assert_eq!(&bytes[..4], &[0, 1, 6, 0]);
        assert_eq!(decode_composite_group_count_plan_v1(&bytes).unwrap(), plan);

        let mut invalid = plan;
        invalid.group_keys[1] = invalid.group_keys[0].clone();
        assert_eq!(
            validate_composite_group_count_plan_v1(&invalid),
            Err(PlanError::InvalidColumnReference)
        );
    }

    #[test]
    fn composite_group_aggregate_plan_round_trips_and_checks_scan_union() {
        let first = column(4, 1, ScalarType::Text);
        let second = column(3, 0, ScalarType::Integer);
        let amount = column(5, 2, ScalarType::BigInt);
        let plan = plan_composite_group_aggregate_v1(BoundCompositeGroupAggregate {
            schema_name: "sales".to_owned(),
            schema_id: [0x10; 16],
            schema_version: 4,
            table_epoch: 6,
            relation_id: [0x20; 16],
            relation_name: "orders".to_owned(),
            group_keys: vec![first.clone(), second.clone()],
            aggregate_kind: 4,
            aggregate_column: Some(amount.clone()),
            limit: Some(7),
        })
        .unwrap();
        assert_eq!(plan.group_keys, [first.clone(), second.clone()]);
        assert_eq!(plan.scan_columns, [second, first, amount]);
        let bytes = encode_composite_group_aggregate_plan_v1(&plan).unwrap();
        assert_eq!(&bytes[..4], &[0, 1, 13, 0]);
        assert_eq!(
            decode_composite_group_aggregate_plan_v1(&bytes).unwrap(),
            plan
        );

        let mut invalid = plan;
        invalid.scan_columns.pop();
        assert_eq!(
            validate_composite_group_aggregate_plan_v1(&invalid),
            Err(PlanError::InvalidColumnReference)
        );
    }

    #[test]
    fn filtered_composite_group_aggregate_plan_round_trips_filter_only_columns() {
        use crate::optimizer_binding::{BoundColumn, BoundPredicate};
        use crate::optimizer_request::{Comparison, CompositeGroupCountOrderItem};

        let first = column(4, 1, ScalarType::Text);
        let second = column(3, 0, ScalarType::Integer);
        let amount = column(5, 2, ScalarType::BigInt);
        let predicate = BoundPredicate::And(vec![
            BoundPredicate::IsNotNull(Box::new(BoundPredicate::Column(amount.clone()))),
            BoundPredicate::Compare {
                operator: Comparison::Greater,
                left: Box::new(BoundPredicate::Column(BoundColumn {
                    column_id: [0x26; 16],
                    ordinal: 3,
                    scalar_type: ScalarType::Integer,
                    nullable: false,
                })),
                right: Box::new(BoundPredicate::Integer(7)),
            },
        ]);
        let plan =
            plan_filtered_composite_group_aggregate_v1(BoundFilteredCompositeGroupAggregate {
                schema_name: "sales".to_owned(),
                schema_id: [0x10; 16],
                schema_version: 4,
                table_epoch: 6,
                relation_id: [0x20; 16],
                relation_name: "orders".to_owned(),
                group_keys: vec![first.clone(), second.clone()],
                aggregate_kind: 4,
                aggregate_column: Some(amount.clone()),
                predicate,
                order_items: Vec::new(),
                limit: Some(7),
            })
            .unwrap();
        assert_eq!(plan.scan_columns.len(), 4);
        assert_eq!(plan.scan_columns[3].ordinal, 3);
        let bytes = encode_filtered_composite_group_aggregate_plan_v1(&plan).unwrap();
        assert_eq!(&bytes[..4], &[0, 1, 15, 0]);
        assert_eq!(
            decode_filtered_composite_group_aggregate_plan_v1(&bytes).unwrap(),
            plan
        );

        let mut ordered_plan = plan.clone();
        ordered_plan.order_items = vec![
            CompositeGroupCountOrderItem {
                source: 0,
                group_key_index: Some(0),
                descending: true,
                nulls_first: false,
            },
            CompositeGroupCountOrderItem {
                source: 1,
                group_key_index: None,
                descending: false,
                nulls_first: true,
            },
        ];
        let ordered_bytes =
            encode_filtered_composite_group_aggregate_plan_v1(&ordered_plan).unwrap();
        assert_eq!(&ordered_bytes[..4], &[0, 1, 16, 0]);
        assert_eq!(
            decode_filtered_composite_group_aggregate_plan_v1(&ordered_bytes).unwrap(),
            ordered_plan
        );

        let single_key_plan =
            plan_filtered_composite_group_aggregate_v1(BoundFilteredCompositeGroupAggregate {
                schema_name: "sales".to_owned(),
                schema_id: [0x10; 16],
                schema_version: 4,
                table_epoch: 6,
                relation_id: [0x20; 16],
                relation_name: "orders".to_owned(),
                group_keys: vec![second],
                aggregate_kind: 4,
                aggregate_column: Some(amount),
                predicate: BoundPredicate::IsNotNull(Box::new(BoundPredicate::Column(
                    first.clone(),
                ))),
                order_items: Vec::new(),
                limit: Some(7),
            })
            .unwrap();
        assert_eq!(single_key_plan.group_keys.len(), 1);
        assert_eq!(single_key_plan.scan_columns.len(), 3);
        let single_key_bytes =
            encode_filtered_composite_group_aggregate_plan_v1(&single_key_plan).unwrap();
        assert_eq!(
            decode_filtered_composite_group_aggregate_plan_v1(&single_key_bytes).unwrap(),
            single_key_plan
        );

        let mut invalid = plan;
        invalid.scan_columns.pop();
        assert_eq!(
            validate_filtered_composite_group_aggregate_plan_v1(&invalid),
            Err(PlanError::InvalidColumnReference)
        );
    }

    #[test]
    fn ordered_composite_group_aggregate_plan_roundtrips_and_checks_sort_items() {
        let keys = vec![
            column(4, 1, ScalarType::Text),
            column(3, 0, ScalarType::Integer),
        ];
        let amount = column(5, 2, ScalarType::BigInt);
        let plan = plan_ordered_composite_group_aggregate_v1(BoundOrderedCompositeGroupAggregate {
            schema_name: "sales".into(),
            schema_id: [0x10; 16],
            schema_version: 4,
            table_epoch: 6,
            relation_id: [0x20; 16],
            relation_name: "orders".into(),
            group_keys: keys,
            aggregate_kind: 4,
            aggregate_column: Some(amount),
            order_items: vec![
                CompositeGroupCountOrderItem {
                    source: 1,
                    group_key_index: None,
                    descending: true,
                    nulls_first: false,
                },
                CompositeGroupCountOrderItem {
                    source: 0,
                    group_key_index: Some(1),
                    descending: false,
                    nulls_first: true,
                },
            ],
            limit: Some(4),
        })
        .unwrap();
        let bytes = encode_ordered_composite_group_aggregate_plan_v1(&plan).unwrap();
        assert_eq!(&bytes[..4], &[0, 1, 14, 0]);
        assert_eq!(
            decode_ordered_composite_group_aggregate_plan_v1(&bytes).unwrap(),
            plan
        );

        let mut invalid = plan;
        invalid.order_items[1].group_key_index = Some(0);
        invalid.order_items.push(CompositeGroupCountOrderItem {
            source: 0,
            group_key_index: Some(0),
            descending: false,
            nulls_first: false,
        });
        assert_eq!(
            validate_ordered_composite_group_aggregate_plan_v1(&invalid),
            Err(PlanError::InvalidColumnReference)
        );
    }

    #[test]
    fn join_plan_round_trips_and_rejects_unscanned_keys() {
        let left_key = column(1, 0, ScalarType::Integer);
        let left_payload = column(2, 1, ScalarType::Text);
        let left_key2 = column(3, 2, ScalarType::BigInt);
        let right_key = column(4, 0, ScalarType::Integer);
        let right_payload = column(5, 1, ScalarType::Text);
        let right_key2 = column(6, 2, ScalarType::BigInt);
        let plan = JoinSelectPlanV1 {
            schema_name: "sales".to_owned(),
            schema_id: [0x10; 16],
            schema_version: 7,
            left_relation: RelationReference {
                schema_name: "sales".to_owned(),
                schema_id: [0x10; 16],
                schema_version: 7,
                relation_name: "orders".to_owned(),
                relation_id: [0x20; 16],
                table_epoch: 11,
            },
            right_relation: RelationReference {
                schema_name: "sales".to_owned(),
                schema_id: [0x10; 16],
                schema_version: 7,
                relation_name: "customers".to_owned(),
                relation_id: [0x30; 16],
                table_epoch: 12,
            },
            left_scan_columns: vec![left_key.clone(), left_payload.clone(), left_key2.clone()],
            right_scan_columns: vec![right_key.clone(), right_payload.clone(), right_key2.clone()],
            projections: vec![
                (JoinRelationSide::Left, left_payload),
                (JoinRelationSide::Right, right_payload),
            ],
            join_keys: vec![(left_key, right_key), (left_key2, right_key2)],
            limit: Some(9),
            filter: None,
            index_nested_loop: None,
        };
        let bytes = encode_join_select_plan_v1(&plan).unwrap();
        assert_eq!(decode_join_select_plan_v1(&bytes).unwrap(), plan);

        let mut filtered = plan.clone();
        filtered.filter = Some(BoundJoinFilter {
            relation_side: JoinRelationSide::Left,
            column: filtered.left_scan_columns[1].clone(),
            expression: crate::optimizer_request::JoinFilterExpression::Parameter {
                operator: crate::optimizer_request::Comparison::Greater,
                index: 3,
            },
        });
        let filtered_bytes = encode_join_select_plan_v1(&filtered).unwrap();
        assert_eq!(
            crate::query_abi::decode_optimizer_plan_header_record(&filtered_bytes)
                .unwrap()
                .1,
            PLAN_KIND_FILTERED_JOIN_SELECT
        );
        assert_eq!(
            decode_join_select_plan_v1(&filtered_bytes).unwrap(),
            filtered
        );
        let mut invalid_filter = filtered.clone();
        invalid_filter.filter.as_mut().unwrap().column.ordinal = 99;
        assert_eq!(
            validate_join_select_plan_v1(&invalid_filter),
            Err(PlanError::InvalidColumnReference)
        );

        let mut invalid = plan;
        invalid.join_keys[0].0.column_id = [0x7f; 16];
        assert_eq!(
            validate_join_select_plan_v1(&invalid),
            Err(PlanError::InvalidColumnReference)
        );
    }
}
