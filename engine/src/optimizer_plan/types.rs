use super::validate::validate_plan_v1;
use crate::optimizer_binding::{
    BoundColumn, BoundJoinFilter, BoundPredicate, BoundUpdateAssignment,
};
use crate::optimizer_request::{
    CompositeGroupCountOrderItem, GroupCountOrderItem, JoinRelationSide,
};

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
    /// Optional bounded leading-key batch point-probe hint for a complete OR
    /// of equalities. The original predicate remains in the Filter node.
    pub multi_point_probe: Option<MultiPointProbeV1>,
    /// Storage-proven empty authorized view, revalidated by the executor.
    pub empty_view_hint: bool,
}

/// Prefer a sequential scan over an index path for very small exact
/// authorized populations. The cutoff is conservative and based on a local
/// storage API profile; it is not an end-to-end SQL cost estimate.
pub const SMALL_POPULATION_SCAN_ROWS: u64 = 4;
pub(super) const SKEWED_JOIN_OUTER_MAX_ROWS: u64 = 8;
pub(super) const SKEWED_JOIN_INPUT_RATIO: u64 = 16;
// The bounded SQL hash path materializes at most 128 rows from either input.
pub(crate) const BOUNDED_HASH_JOIN_MAX_ROWS_PER_INPUT: u64 = 128;
// These limits match the indexed SDK runner's outer and pair capacities.
pub(super) const INDEXED_JOIN_MAX_OUTER_ROWS: u64 = 128;
pub(super) const INDEXED_JOIN_MAX_OUTPUT_PAIRS: u64 = 16_384;
pub(crate) const FANOUT_AWARE_JOIN_OUTER_MAX_ROWS: u64 = 19;
pub(crate) const FANOUT_AWARE_JOIN_INPUT_RATIO: u64 = 8;
pub(super) const FANOUT_AWARE_JOIN_LARGE_OUTER_INPUT_RATIO: u64 = 7;
pub(super) const FANOUT_AWARE_JOIN_MAX_PAIRS_PER_OUTER_ROW: u64 = 4;
/// Maximum row count probed for optimizer join orientation. This matches the
/// bounded candidate budget available to a planner population estimate.
pub const JOIN_POPULATION_ESTIMATE_MAX_ROWS: u64 = 16_384;

pub(crate) fn fanout_aware_join_inputs_are_eligible(outer_rows: u64, indexed_rows: u64) -> bool {
    let minimum_ratio = if outer_rows >= 17 {
        FANOUT_AWARE_JOIN_LARGE_OUTER_INPUT_RATIO
    } else {
        FANOUT_AWARE_JOIN_INPUT_RATIO
    };
    (9..=FANOUT_AWARE_JOIN_OUTER_MAX_ROWS).contains(&outer_rows)
        && indexed_rows >= outer_rows * minimum_ratio
}

pub fn apply_population_scan_heuristic(
    plan: &mut SingleTablePlanV1,
    visible_rows: Option<u64>,
) -> Result<bool, PlanError> {
    if visible_rows.is_some_and(|rows| rows > 0 && rows <= SMALL_POPULATION_SCAN_ROWS)
        && (plan.unique_probe.is_some()
            || plan.range_scan.is_some()
            || plan.multi_point_probe.is_some())
    {
        plan.unique_probe = None;
        plan.range_scan = None;
        plan.multi_point_probe = None;
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultiPointProbeV1 {
    pub index_id: [u8; 16],
    pub key_column: BoundColumn,
    pub values: Vec<BoundPredicate>,
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
    /// Full contiguous index prefix for a kind-18 probe. Kind 17 leaves it empty.
    pub prefix_columns: Vec<BoundColumn>,
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
pub(super) const PLAN_KIND_SELECT: u8 = 1;
pub(super) const PLAN_KIND_DELETE: u8 = 2;
pub(super) const PLAN_KIND_UPDATE: u8 = 3;
pub(super) const PLAN_KIND_JOIN_SELECT: u8 = 4;
pub(super) const PLAN_KIND_FILTERED_JOIN_SELECT: u8 = 12;
pub(super) const PLAN_KIND_INDEX_NESTED_LOOP_JOIN: u8 = 17;
pub(super) const PLAN_KIND_INDEX_PREFIX_NESTED_LOOP_JOIN: u8 = 18;
pub(super) const DELETE_SINK_NODE: u8 = 5;
pub(super) const PLAN_KIND_GROUP_COUNT: u8 = 5;
pub(super) const PLAN_KIND_COMPOSITE_GROUP_COUNT: u8 = 6;
pub(super) const PLAN_KIND_ORDERED_SELECT: u8 = 7;
pub(super) const PLAN_KIND_ORDERED_GROUP_COUNT: u8 = 8;
pub(super) const PLAN_KIND_ORDERED_COMPOSITE_GROUP_COUNT: u8 = 9;
pub(super) const PLAN_KIND_GROUP_AGGREGATE: u8 = 10;
pub(super) const PLAN_KIND_ORDERED_GROUP_AGGREGATE: u8 = 11;
pub(super) const PLAN_KIND_COMPOSITE_GROUP_AGGREGATE: u8 = 13;
pub(super) const PLAN_KIND_ORDERED_COMPOSITE_GROUP_AGGREGATE: u8 = 14;
pub(super) const PLAN_KIND_FILTERED_COMPOSITE_GROUP_AGGREGATE: u8 = 15;
pub(super) const PLAN_KIND_ORDERED_FILTERED_COMPOSITE_GROUP_AGGREGATE: u8 = 16;
pub(super) const GROUP_COUNT_NODE: u8 = 7;
pub(super) const COMPOSITE_GROUP_COUNT_NODE: u8 = 8;
pub(super) const ORDER_BY_NODE: u8 = 9;
pub(super) const ORDERED_COMPOSITE_GROUP_COUNT_NODE: u8 = 10;
pub(super) const GROUP_AGGREGATE_NODE: u8 = 11;
pub(super) const ORDERED_GROUP_AGGREGATE_NODE: u8 = 12;
pub(super) const COMPOSITE_GROUP_AGGREGATE_NODE: u8 = 13;
pub(super) const ORDERED_COMPOSITE_GROUP_AGGREGATE_NODE: u8 = 14;
pub(super) const FILTERED_COMPOSITE_GROUP_AGGREGATE_NODE: u8 = 15;
pub(super) const ORDERED_FILTERED_COMPOSITE_GROUP_AGGREGATE_NODE: u8 = 16;

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
