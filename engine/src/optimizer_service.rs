//! Invocation-bound structured query planning service.

use crate::metadata_discovery::AuthorizedPlanRelation;
use crate::optimizer_binding::{
    MetadataBindError, bind_select_request, bind_select_request_via_metadata,
};
use crate::optimizer_plan::{PlanError, encode_plan_v1, plan_select_v1};
use crate::transaction::PlannerMetadataConnection;
use std::fmt;

/// Failure returned to the caller of the optimizer service. Relation lookup
/// failures intentionally do not expose whether a name exists outside the
/// invocation's authorized metadata view.
#[derive(Debug)]
pub enum OptimizerError {
    Bind(MetadataBindError),
    Plan(PlanError),
}

impl fmt::Display for OptimizerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "optimizer service error: {self:?}")
    }
}

impl std::error::Error for OptimizerError {}

impl From<MetadataBindError> for OptimizerError {
    fn from(error: MetadataBindError) -> Self {
        Self::Bind(error)
    }
}

impl From<PlanError> for OptimizerError {
    fn from(error: PlanError) -> Self {
        Self::Plan(error)
    }
}

/// Resolve, bind, lower, and encode one canonical SELECT request using only
/// the metadata authority already attached to this invocation. The returned
/// bytes are data; the executor must independently resolve and authorize the
/// relation before execution.
pub fn optimize_select_v1(
    request: &[u8],
    connection: &mut PlannerMetadataConnection<'_, '_>,
) -> Result<Vec<u8>, OptimizerError> {
    let bound = bind_select_request_via_metadata(request, connection)?;
    encode_bound_select(bound)
}

/// Optimize a request using an authorized stable relation description
/// obtained through a separate planner IPC connection. The relation record
/// carries no storage capability and is still validated during binding.
pub fn optimize_select_v1_with_relation(
    request: &[u8],
    relation: Option<AuthorizedPlanRelation>,
) -> Result<Vec<u8>, OptimizerError> {
    let bound = bind_select_request(request, relation)
        .map_err(|error| OptimizerError::Bind(MetadataBindError::Bind(error)))?;
    encode_bound_select(bound)
}

fn encode_bound_select(
    bound: crate::optimizer_binding::BoundSelect,
) -> Result<Vec<u8>, OptimizerError> {
    let plan = plan_select_v1(bound)?;
    Ok(encode_plan_v1(&plan)?)
}
