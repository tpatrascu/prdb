//! Clean-slate unified optimizer IR, binder, and physical plan codec.

pub mod bind;
pub mod expr;
pub mod logical;
pub mod physical;
#[cfg(test)]
mod tests;

pub use bind::{
    BindError, BoundAggregateSpec, BoundLogicalOp, BoundLogicalRequest, BoundOrderItem,
    BoundOrderTarget, BoundRelation, BoundUpdateAssignment, BoundUpdateValue, MetadataBindError,
    bind_logical_request, bind_logical_request_via_metadata,
};
pub use expr::{
    BoundColumn, BoundColumnRef, BoundExpr, Comparison, Expr, ExprError, MAX_EXPR_BYTES,
    MAX_EXPR_DEPTH, MAX_EXPR_NODES, MAX_IDENTIFIER_BYTES, MAX_IN_LIST_ITEMS, MAX_PARAMETER_INDEX,
    MAX_TEXT_LITERAL_BYTES, decode_bound_expr, decode_bound_expr_for_columns, decode_expr,
    encode_bound_expr, encode_expr, index_range_bounds_from_expr,
};
pub use logical::{
    AggregateKind, AggregateSpec, JoinType, LogicalOp, LogicalRequest, MAX_LOGICAL_NODES,
    MAX_PROJECTIONS, MAX_UNIFIED_REQUEST_BYTES, MAX_UPDATE_ASSIGNMENTS, OrderItem, OrderTarget,
    RequestError, UpdateAssignment, UpdateValue, collect_scan_relations, decode_logical_request,
    encode_logical_request,
};
pub use physical::{
    BoundAggregateSpec as PhysicalAggregateSpec, IndexBound, MAX_PHYSICAL_NODES,
    MAX_PLAN_SCAN_COLUMNS, MAX_UNIFIED_PLAN_BYTES, PhysicalOp, PhysicalPlan, PlanError,
    RelationReference, decode_physical_plan, encode_physical_plan, validate_physical_plan,
};
