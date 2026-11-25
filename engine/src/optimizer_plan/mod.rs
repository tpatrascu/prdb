//! Deterministic single-table SELECT plan construction for optimizer v1.

mod group_aggregate;
mod group_count;
mod ordered_dml;
mod select_join;
#[cfg(test)]
mod tests;
mod types;
mod validate;
mod wire;

pub use group_aggregate::*;
pub use group_count::*;
pub use ordered_dml::*;
pub(crate) use select_join::index_range_bounds;
pub use select_join::{
    decode_join_select_plan_v1, decode_plan_v1, encode_join_select_plan_v1, encode_plan_v1,
    plan_join_select_v1, plan_join_select_v1_with_cardinality_estimate,
    plan_join_select_v1_with_estimates, plan_select_v1, validate_join_select_plan_v1,
};
#[cfg(test)]
pub(crate) use types::BOUNDED_HASH_JOIN_MAX_ROWS_PER_INPUT;
pub(crate) use types::fanout_aware_join_inputs_are_eligible;
pub use types::{
    CompositeGroupAggregatePlanV1, CompositeGroupCountPlanV1, DeletePlanV1,
    FilteredCompositeGroupAggregatePlanV1, GroupAggregatePlanV1, GroupCountPlanV1,
    IndexNestedLoopJoinV1, IndexRangeBoundV1, IndexRangeScanV1, JOIN_POPULATION_ESTIMATE_MAX_ROWS,
    JoinSelectPlanV1, MAX_PLAN_BYTES, MAX_PLAN_SCAN_COLUMNS, MultiPointProbeV1,
    OrderedCompositeGroupAggregatePlanV1, OrderedCompositeGroupCountPlanV1,
    OrderedGroupAggregatePlanV1, OrderedGroupCountPlanV1, OrderedSelectPlanV1, PlanError, PlanNode,
    RelationReference, SMALL_POPULATION_SCAN_ROWS, SingleTablePlanV1, UniquePointProbeV1,
    UpdatePlanV1, apply_population_scan_heuristic, apply_range_selectivity_scan_heuristic,
};
pub use validate::validate_plan_v1;
pub(crate) use wire::{decode_bound_predicate_v1, encode_bound_predicate_v1};
