//! Invocation-bound structured query planning service.

use crate::metadata_discovery::AuthorizedPlanRelation;
use crate::optimizer_binding::{
    MetadataBindError, bind_composite_group_aggregate_request,
    bind_composite_group_aggregate_request_via_metadata, bind_composite_group_count_request,
    bind_composite_group_count_request_via_metadata, bind_delete_request,
    bind_delete_request_via_metadata, bind_filtered_composite_group_aggregate_request,
    bind_filtered_composite_group_aggregate_request_via_metadata, bind_group_aggregate_request,
    bind_group_aggregate_request_via_metadata, bind_group_count_request,
    bind_group_count_request_via_metadata, bind_join_select_request,
    bind_join_select_request_via_metadata, bind_ordered_composite_group_aggregate_request,
    bind_ordered_composite_group_aggregate_request_via_metadata,
    bind_ordered_composite_group_count_request,
    bind_ordered_composite_group_count_request_via_metadata, bind_ordered_group_aggregate_request,
    bind_ordered_group_aggregate_request_via_metadata, bind_ordered_group_count_request,
    bind_ordered_group_count_request_via_metadata, bind_ordered_select_request,
    bind_ordered_select_request_via_metadata, bind_select_request,
    bind_select_request_via_metadata, bind_update_request, bind_update_request_via_metadata,
};
use crate::optimizer_plan::{
    PlanError, RelationReference, encode_composite_group_aggregate_plan_v1,
    encode_composite_group_count_plan_v1, encode_delete_plan_v1,
    encode_filtered_composite_group_aggregate_plan_v1, encode_group_aggregate_plan_v1,
    encode_group_count_plan_v1, encode_join_select_plan_v1,
    encode_ordered_composite_group_aggregate_plan_v1, encode_ordered_composite_group_count_plan_v1,
    encode_ordered_group_aggregate_plan_v1, encode_ordered_group_count_plan_v1,
    encode_ordered_select_plan_v1, encode_plan_v1, encode_update_plan_v1,
    plan_composite_group_aggregate_v1, plan_composite_group_count_v1, plan_delete_v1,
    plan_filtered_composite_group_aggregate_v1, plan_group_aggregate_v1, plan_group_count_v1,
    plan_join_select_v1_with_estimates, plan_ordered_composite_group_aggregate_v1,
    plan_ordered_composite_group_count_v1, plan_ordered_group_aggregate_v1,
    plan_ordered_group_count_v1, plan_ordered_select_v1, plan_select_v1, plan_update_v1,
};
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

pub fn optimize_ordered_select_v1(
    request: &[u8],
    connection: &mut PlannerMetadataConnection<'_, '_>,
) -> Result<Vec<u8>, OptimizerError> {
    let bound = bind_ordered_select_request_via_metadata(request, connection)?;
    encode_bound_ordered_select(bound)
}

pub fn optimize_ordered_select_v1_with_relation(
    request: &[u8],
    relation: Option<AuthorizedPlanRelation>,
) -> Result<Vec<u8>, OptimizerError> {
    let bound = bind_ordered_select_request(request, relation)
        .map_err(|error| OptimizerError::Bind(MetadataBindError::Bind(error)))?;
    encode_bound_ordered_select(bound)
}

/// Resolve, bind, lower, and encode a two-relation equality join using only
/// the invocation-bound planner metadata connection.
pub fn optimize_join_select_v1(
    request: &[u8],
    connection: &mut PlannerMetadataConnection<'_, '_>,
) -> Result<Vec<u8>, OptimizerError> {
    let bound = bind_join_select_request_via_metadata(request, connection)?;
    let left_rows = estimate_join_relation_population(
        connection,
        &bound.schema_name,
        &bound.left_relation.relation_name,
    )?;
    let right_rows = estimate_join_relation_population(
        connection,
        &bound.schema_name,
        &bound.right_relation.relation_name,
    )?;
    encode_bound_join_select_with_estimates(bound, left_rows, right_rows)
}

fn estimate_join_relation_population(
    connection: &mut PlannerMetadataConnection<'_, '_>,
    schema_name: &str,
    relation_name: &str,
) -> Result<Option<u64>, OptimizerError> {
    let Some((handle, _)) = connection
        .resolve_relation_for_plan(schema_name, relation_name)
        .map_err(|error| OptimizerError::Bind(MetadataBindError::Transaction(error)))?
    else {
        return Ok(None);
    };
    let estimate = connection.estimate_population_at_most(
        &handle,
        crate::optimizer_plan::JOIN_POPULATION_ESTIMATE_MAX_ROWS,
    );
    let close = connection.close_relation(&handle);
    match (estimate, close) {
        (Err(error), _) => Err(OptimizerError::Bind(MetadataBindError::Transaction(error))),
        (Ok(_), Err(error)) => Err(OptimizerError::Bind(MetadataBindError::Transaction(error))),
        (Ok(estimate), Ok(())) => Ok(estimate),
    }
}

/// Optimize a join request using two independently authorized stable
/// relation descriptions obtained through the planner channel.
pub fn optimize_join_select_v1_with_relations(
    request: &[u8],
    left_relation: Option<AuthorizedPlanRelation>,
    right_relation: Option<AuthorizedPlanRelation>,
) -> Result<Vec<u8>, OptimizerError> {
    optimize_join_select_v1_with_relations_and_estimates(
        request,
        left_relation,
        right_relation,
        None,
        None,
    )
}

/// Optimize a join using bounded, invocation-authorized row-count estimates
/// when available. Unknown estimates retain the deterministic legacy choice.
pub fn optimize_join_select_v1_with_relations_and_estimates(
    request: &[u8],
    left_relation: Option<AuthorizedPlanRelation>,
    right_relation: Option<AuthorizedPlanRelation>,
    left_visible_rows: Option<u64>,
    right_visible_rows: Option<u64>,
) -> Result<Vec<u8>, OptimizerError> {
    let bound = bind_join_select_request(request, left_relation, right_relation)
        .map_err(|error| OptimizerError::Bind(MetadataBindError::Bind(error)))?;
    encode_bound_join_select_with_estimates(bound, left_visible_rows, right_visible_rows)
}

/// Resolve, bind, lower, and encode one canonical DELETE request using only
/// the invocation-bound planner metadata connection. Storage still owns
/// authorization and every eventual mutation.
pub fn optimize_delete_v1(
    request: &[u8],
    connection: &mut PlannerMetadataConnection<'_, '_>,
) -> Result<Vec<u8>, OptimizerError> {
    let bound = bind_delete_request_via_metadata(request, connection)?;
    encode_bound_delete(bound)
}

/// Optimize a DELETE request using the authorized stable relation description
/// from the planner channel. This description grants no executor capability.
pub fn optimize_delete_v1_with_relation(
    request: &[u8],
    relation: Option<AuthorizedPlanRelation>,
) -> Result<Vec<u8>, OptimizerError> {
    let bound = bind_delete_request(request, relation)
        .map_err(|error| OptimizerError::Bind(MetadataBindError::Bind(error)))?;
    encode_bound_delete(bound)
}

/// Resolve, bind, lower, and encode one canonical UPDATE request using only
/// the invocation-bound planner metadata connection. The encoded result has
/// no storage capability; executor storage remains authoritative for writes.
pub fn optimize_update_v1(
    request: &[u8],
    connection: &mut PlannerMetadataConnection<'_, '_>,
) -> Result<Vec<u8>, OptimizerError> {
    let bound = bind_update_request_via_metadata(request, connection)?;
    encode_bound_update(bound)
}

/// Optimize an UPDATE request using an authorized stable relation
/// description obtained through the planner channel.
pub fn optimize_update_v1_with_relation(
    request: &[u8],
    relation: Option<AuthorizedPlanRelation>,
) -> Result<Vec<u8>, OptimizerError> {
    let bound = bind_update_request(request, relation)
        .map_err(|error| OptimizerError::Bind(MetadataBindError::Bind(error)))?;
    encode_bound_update(bound)
}

pub fn optimize_group_count_v1(
    request: &[u8],
    connection: &mut PlannerMetadataConnection<'_, '_>,
) -> Result<Vec<u8>, OptimizerError> {
    let bound = bind_group_count_request_via_metadata(request, connection)?;
    encode_bound_group_count(bound)
}

pub fn optimize_group_count_v1_with_relation(
    request: &[u8],
    relation: Option<AuthorizedPlanRelation>,
) -> Result<Vec<u8>, OptimizerError> {
    let bound = bind_group_count_request(request, relation)
        .map_err(|error| OptimizerError::Bind(MetadataBindError::Bind(error)))?;
    encode_bound_group_count(bound)
}

pub fn optimize_group_aggregate_v1(
    request: &[u8],
    connection: &mut PlannerMetadataConnection<'_, '_>,
) -> Result<Vec<u8>, OptimizerError> {
    let bound = bind_group_aggregate_request_via_metadata(request, connection)?;
    encode_bound_group_aggregate(bound)
}

pub fn optimize_composite_group_aggregate_v1(
    request: &[u8],
    connection: &mut PlannerMetadataConnection<'_, '_>,
) -> Result<Vec<u8>, OptimizerError> {
    let bound = bind_composite_group_aggregate_request_via_metadata(request, connection)?;
    encode_bound_composite_group_aggregate(bound)
}

pub fn optimize_composite_group_aggregate_v1_with_relation(
    request: &[u8],
    relation: Option<AuthorizedPlanRelation>,
) -> Result<Vec<u8>, OptimizerError> {
    let bound = bind_composite_group_aggregate_request(request, relation)
        .map_err(|error| OptimizerError::Bind(MetadataBindError::Bind(error)))?;
    encode_bound_composite_group_aggregate(bound)
}

pub fn optimize_filtered_composite_group_aggregate_v1(
    request: &[u8],
    connection: &mut PlannerMetadataConnection<'_, '_>,
) -> Result<Vec<u8>, OptimizerError> {
    let bound = bind_filtered_composite_group_aggregate_request_via_metadata(request, connection)?;
    encode_bound_filtered_composite_group_aggregate(bound)
}

pub fn optimize_filtered_composite_group_aggregate_v1_with_relation(
    request: &[u8],
    relation: Option<AuthorizedPlanRelation>,
) -> Result<Vec<u8>, OptimizerError> {
    let bound = bind_filtered_composite_group_aggregate_request(request, relation)
        .map_err(|error| OptimizerError::Bind(MetadataBindError::Bind(error)))?;
    encode_bound_filtered_composite_group_aggregate(bound)
}

fn encode_bound_filtered_composite_group_aggregate(
    bound: crate::optimizer_binding::BoundFilteredCompositeGroupAggregate,
) -> Result<Vec<u8>, OptimizerError> {
    let plan = plan_filtered_composite_group_aggregate_v1(bound)?;
    Ok(encode_filtered_composite_group_aggregate_plan_v1(&plan)?)
}

pub fn optimize_group_aggregate_v1_with_relation(
    request: &[u8],
    relation: Option<AuthorizedPlanRelation>,
) -> Result<Vec<u8>, OptimizerError> {
    let bound = bind_group_aggregate_request(request, relation)
        .map_err(|error| OptimizerError::Bind(MetadataBindError::Bind(error)))?;
    encode_bound_group_aggregate(bound)
}

pub fn optimize_ordered_group_aggregate_v1(
    request: &[u8],
    connection: &mut PlannerMetadataConnection<'_, '_>,
) -> Result<Vec<u8>, OptimizerError> {
    let bound = bind_ordered_group_aggregate_request_via_metadata(request, connection)?;
    encode_bound_ordered_group_aggregate(bound)
}

pub fn optimize_ordered_group_aggregate_v1_with_relation(
    request: &[u8],
    relation: Option<AuthorizedPlanRelation>,
) -> Result<Vec<u8>, OptimizerError> {
    let bound = bind_ordered_group_aggregate_request(request, relation)
        .map_err(|error| OptimizerError::Bind(MetadataBindError::Bind(error)))?;
    encode_bound_ordered_group_aggregate(bound)
}

pub fn optimize_ordered_group_count_v1(
    request: &[u8],
    connection: &mut PlannerMetadataConnection<'_, '_>,
) -> Result<Vec<u8>, OptimizerError> {
    let bound = bind_ordered_group_count_request_via_metadata(request, connection)?;
    encode_bound_ordered_group_count(bound)
}

pub fn optimize_ordered_group_count_v1_with_relation(
    request: &[u8],
    relation: Option<AuthorizedPlanRelation>,
) -> Result<Vec<u8>, OptimizerError> {
    let bound = bind_ordered_group_count_request(request, relation)
        .map_err(|error| OptimizerError::Bind(MetadataBindError::Bind(error)))?;
    encode_bound_ordered_group_count(bound)
}

pub fn optimize_composite_group_count_v1(
    request: &[u8],
    connection: &mut PlannerMetadataConnection<'_, '_>,
) -> Result<Vec<u8>, OptimizerError> {
    let bound = bind_composite_group_count_request_via_metadata(request, connection)?;
    encode_bound_composite_group_count(bound)
}

pub fn optimize_composite_group_count_v1_with_relation(
    request: &[u8],
    relation: Option<AuthorizedPlanRelation>,
) -> Result<Vec<u8>, OptimizerError> {
    let bound = bind_composite_group_count_request(request, relation)
        .map_err(|error| OptimizerError::Bind(MetadataBindError::Bind(error)))?;
    encode_bound_composite_group_count(bound)
}

pub fn optimize_ordered_composite_group_count_v1(
    request: &[u8],
    connection: &mut PlannerMetadataConnection<'_, '_>,
) -> Result<Vec<u8>, OptimizerError> {
    let bound = bind_ordered_composite_group_count_request_via_metadata(request, connection)?;
    encode_bound_ordered_composite_group_count(bound)
}

pub fn optimize_ordered_composite_group_count_v1_with_relation(
    request: &[u8],
    relation: Option<AuthorizedPlanRelation>,
) -> Result<Vec<u8>, OptimizerError> {
    let bound = bind_ordered_composite_group_count_request(request, relation)
        .map_err(|error| OptimizerError::Bind(MetadataBindError::Bind(error)))?;
    encode_bound_ordered_composite_group_count(bound)
}

pub fn optimize_ordered_composite_group_aggregate_v1(
    request: &[u8],
    connection: &mut PlannerMetadataConnection<'_, '_>,
) -> Result<Vec<u8>, OptimizerError> {
    let bound = bind_ordered_composite_group_aggregate_request_via_metadata(request, connection)?;
    encode_bound_ordered_composite_group_aggregate(bound)
}

pub fn optimize_ordered_composite_group_aggregate_v1_with_relation(
    request: &[u8],
    relation: Option<AuthorizedPlanRelation>,
) -> Result<Vec<u8>, OptimizerError> {
    let bound = bind_ordered_composite_group_aggregate_request(request, relation)
        .map_err(|error| OptimizerError::Bind(MetadataBindError::Bind(error)))?;
    encode_bound_ordered_composite_group_aggregate(bound)
}

fn encode_bound_ordered_composite_group_aggregate(
    bound: crate::optimizer_binding::BoundOrderedCompositeGroupAggregate,
) -> Result<Vec<u8>, OptimizerError> {
    let plan = plan_ordered_composite_group_aggregate_v1(bound)?;
    Ok(encode_ordered_composite_group_aggregate_plan_v1(&plan)?)
}

fn encode_bound_ordered_composite_group_count(
    bound: crate::optimizer_binding::BoundOrderedCompositeGroupCount,
) -> Result<Vec<u8>, OptimizerError> {
    let plan = plan_ordered_composite_group_count_v1(bound)?;
    Ok(encode_ordered_composite_group_count_plan_v1(&plan)?)
}

fn encode_bound_select(
    bound: crate::optimizer_binding::BoundSelect,
) -> Result<Vec<u8>, OptimizerError> {
    let plan = plan_select_v1(bound)?;
    Ok(encode_plan_v1(&plan)?)
}

fn encode_bound_ordered_select(
    bound: crate::optimizer_binding::BoundOrderedSelect,
) -> Result<Vec<u8>, OptimizerError> {
    let plan = plan_ordered_select_v1(bound)?;
    Ok(encode_ordered_select_plan_v1(&plan)?)
}

fn encode_bound_join_select_with_estimates(
    bound: crate::optimizer_binding::BoundJoinSelect,
    left_visible_rows: Option<u64>,
    right_visible_rows: Option<u64>,
) -> Result<Vec<u8>, OptimizerError> {
    let plan = plan_join_select_v1_with_estimates(bound, left_visible_rows, right_visible_rows)?;
    Ok(encode_join_select_plan_v1(&plan)?)
}

fn encode_bound_group_count(
    bound: crate::optimizer_binding::BoundGroupCount,
) -> Result<Vec<u8>, OptimizerError> {
    let plan = plan_group_count_v1(bound)?;
    Ok(encode_group_count_plan_v1(&plan)?)
}

fn encode_bound_group_aggregate(
    bound: crate::optimizer_binding::BoundGroupAggregate,
) -> Result<Vec<u8>, OptimizerError> {
    let plan = plan_group_aggregate_v1(bound)?;
    Ok(encode_group_aggregate_plan_v1(&plan)?)
}

fn encode_bound_composite_group_aggregate(
    bound: crate::optimizer_binding::BoundCompositeGroupAggregate,
) -> Result<Vec<u8>, OptimizerError> {
    let plan = plan_composite_group_aggregate_v1(bound)?;
    Ok(encode_composite_group_aggregate_plan_v1(&plan)?)
}

fn encode_bound_ordered_group_aggregate(
    bound: crate::optimizer_binding::BoundOrderedGroupAggregate,
) -> Result<Vec<u8>, OptimizerError> {
    let plan = plan_ordered_group_aggregate_v1(bound)?;
    Ok(encode_ordered_group_aggregate_plan_v1(&plan)?)
}

fn encode_bound_ordered_group_count(
    bound: crate::optimizer_binding::BoundOrderedGroupCount,
) -> Result<Vec<u8>, OptimizerError> {
    let plan = plan_ordered_group_count_v1(bound)?;
    Ok(encode_ordered_group_count_plan_v1(&plan)?)
}

fn encode_bound_composite_group_count(
    bound: crate::optimizer_binding::BoundCompositeGroupCount,
) -> Result<Vec<u8>, OptimizerError> {
    let plan = plan_composite_group_count_v1(bound)?;
    Ok(encode_composite_group_count_plan_v1(&plan)?)
}

fn encode_bound_delete(
    bound: crate::optimizer_binding::BoundDelete,
) -> Result<Vec<u8>, OptimizerError> {
    let plan = plan_delete_v1(
        RelationReference {
            schema_name: bound.schema_name,
            schema_id: bound.schema_id,
            schema_version: bound.schema_version,
            relation_name: bound.relation_name,
            relation_id: bound.relation_id,
            table_epoch: bound.table_epoch,
        },
        bound.predicate,
    )?;
    Ok(encode_delete_plan_v1(&plan)?)
}

fn encode_bound_update(
    bound: crate::optimizer_binding::BoundUpdate,
) -> Result<Vec<u8>, OptimizerError> {
    let plan = plan_update_v1(
        RelationReference {
            schema_name: bound.schema_name,
            schema_id: bound.schema_id,
            schema_version: bound.schema_version,
            relation_name: bound.relation_name,
            relation_id: bound.relation_id,
            table_epoch: bound.table_epoch,
        },
        bound.predicate,
        bound.assignments,
    )?;
    Ok(encode_update_plan_v1(&plan)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_discovery::{AuthorizedRelationDescription, VisibleColumn};
    use crate::optimizer_plan::{
        decode_delete_plan_v1, decode_join_select_plan_v1, decode_ordered_group_count_plan_v1,
        decode_update_plan_v1,
    };
    use crate::tuple::ScalarType;

    fn delete_request() -> Vec<u8> {
        let mut bytes = vec![0, 1, crate::query_abi::OPTIMIZER_DELETE_STATEMENT, 0];
        for name in ["public", "items"] {
            bytes.push(name.len() as u8);
            bytes.extend_from_slice(name.as_bytes());
        }
        let predicate = [0x20, 0, 0x10, 2, b'i', b'd', 0x02, 0, 0, 0, 0, 0, 0, 0, 7];
        bytes.extend_from_slice(&(predicate.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&predicate);
        bytes
    }

    fn update_request() -> Vec<u8> {
        let mut bytes = vec![0, 1, crate::query_abi::OPTIMIZER_UPDATE_STATEMENT, 0];
        for name in ["public", "items"] {
            bytes.push(name.len() as u8);
            bytes.extend_from_slice(name.as_bytes());
        }
        bytes.extend_from_slice(&1u16.to_be_bytes());
        bytes.push(6);
        bytes.extend_from_slice(b"active");
        bytes.push(0);
        bytes.extend_from_slice(&1u32.to_be_bytes());
        let predicate = [0x20, 0, 0x10, 2, b'i', b'd', 0x02, 0, 0, 0, 0, 0, 0, 0, 7];
        bytes.extend_from_slice(&(predicate.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&predicate);
        bytes
    }

    fn ordered_group_count_request() -> Vec<u8> {
        let mut bytes = vec![
            0,
            1,
            crate::query_abi::OPTIMIZER_GROUP_COUNT_ORDERED_STATEMENT,
            0,
        ];
        for name in ["public", "items", "active"] {
            bytes.push(name.len() as u8);
            bytes.extend_from_slice(name.as_bytes());
        }
        bytes.extend_from_slice(&[2, 1, 1, 0, 0, 0, 1, 1, 0, 0, 0, 5]);
        bytes
    }

    fn filtered_join_select_request() -> Vec<u8> {
        let mut bytes = vec![
            0,
            1,
            crate::query_abi::OPTIMIZER_FILTERED_JOIN_SELECT_STATEMENT,
            0,
        ];
        for name in ["public", "lefts", "rights"] {
            bytes.push(name.len() as u8);
            bytes.extend_from_slice(name.as_bytes());
        }
        bytes.extend_from_slice(&1u16.to_be_bytes());
        bytes.push(1);
        bytes.push(5);
        bytes.extend_from_slice(b"value");
        bytes.push(2);
        for name in ["id", "id", "active", "active"] {
            bytes.push(name.len() as u8);
            bytes.extend_from_slice(name.as_bytes());
        }
        bytes.extend_from_slice(&[1, 0, 0, 0, 7]);
        bytes.push(1);
        bytes.push(6);
        bytes.extend_from_slice(b"active");
        bytes.extend_from_slice(&[0, 4]);
        bytes.extend_from_slice(&2u32.to_be_bytes());
        bytes
    }

    fn relation() -> AuthorizedPlanRelation {
        AuthorizedPlanRelation {
            schema_id: [0x10; 16],
            schema_version: 3,
            table_epoch: 4,
            relation: AuthorizedRelationDescription {
                relation_id: [0x11; 16],
                name: "items".to_owned(),
                columns: vec![
                    VisibleColumn {
                        column_id: [0x21; 16],
                        name: "id".to_owned(),
                        ordinal: 0,
                        scalar_type: ScalarType::Integer,
                        nullable: false,
                    },
                    VisibleColumn {
                        column_id: [0x22; 16],
                        name: "active".to_owned(),
                        ordinal: 1,
                        scalar_type: ScalarType::Boolean,
                        nullable: true,
                    },
                ],
            },
            unique_access_paths: Vec::new(),
            range_access_paths: Vec::new(),
        }
    }

    fn join_relation(name: &str, relation_id: u8) -> AuthorizedPlanRelation {
        AuthorizedPlanRelation {
            schema_id: [0x10; 16],
            schema_version: 3,
            table_epoch: u64::from(relation_id),
            relation: AuthorizedRelationDescription {
                relation_id: [relation_id; 16],
                name: name.to_owned(),
                columns: vec![
                    VisibleColumn {
                        column_id: [relation_id + 0x10; 16],
                        name: "id".to_owned(),
                        ordinal: 0,
                        scalar_type: ScalarType::Integer,
                        nullable: false,
                    },
                    VisibleColumn {
                        column_id: [relation_id + 0x11; 16],
                        name: "active".to_owned(),
                        ordinal: 1,
                        scalar_type: ScalarType::Boolean,
                        nullable: true,
                    },
                    VisibleColumn {
                        column_id: [relation_id + 0x12; 16],
                        name: "value".to_owned(),
                        ordinal: 2,
                        scalar_type: ScalarType::Text,
                        nullable: true,
                    },
                ],
            },
            unique_access_paths: Vec::new(),
            range_access_paths: Vec::new(),
        }
    }

    #[test]
    fn authorized_delete_request_produces_only_a_bounded_delete_plan() {
        let encoded =
            optimize_delete_v1_with_relation(&delete_request(), Some(relation())).unwrap();
        let plan = decode_delete_plan_v1(&encoded).unwrap();
        assert_eq!(plan.relation.schema_id, [0x10; 16]);
        assert_eq!(plan.relation.relation_id, [0x11; 16]);
        assert_eq!(plan.scan_columns[0].column_id, [0x21; 16]);
        assert_eq!(
            plan.predicate,
            crate::optimizer_binding::BoundPredicate::Compare {
                operator: crate::optimizer_request::Comparison::Equal,
                left: Box::new(crate::optimizer_binding::BoundPredicate::Column(
                    crate::optimizer_binding::BoundColumn {
                        column_id: [0x21; 16],
                        ordinal: 0,
                        scalar_type: ScalarType::Integer,
                        nullable: false,
                    }
                )),
                right: Box::new(crate::optimizer_binding::BoundPredicate::Integer(7)),
            }
        );
    }

    #[test]
    fn filtered_join_plan_carries_filter_shape_and_authorized_scan_column() {
        let encoded = optimize_join_select_v1_with_relations(
            &filtered_join_select_request(),
            Some(join_relation("lefts", 0x31)),
            Some(join_relation("rights", 0x41)),
        )
        .unwrap();
        let plan = decode_join_select_plan_v1(&encoded).unwrap();
        assert_eq!(plan.left_relation.relation_id, [0x31; 16]);
        assert_eq!(plan.right_relation.relation_id, [0x41; 16]);
        assert_eq!(
            plan.right_scan_columns
                .iter()
                .map(|c| c.ordinal)
                .collect::<Vec<_>>(),
            [0, 1, 2]
        );
        let filter = plan.filter.unwrap();
        assert_eq!(
            filter.relation_side,
            crate::optimizer_request::JoinRelationSide::Right
        );
        assert_eq!(filter.column.column_id, [0x52; 16]);
        assert_eq!(filter.column.scalar_type, ScalarType::Boolean);
        assert_eq!(
            filter.expression,
            crate::optimizer_request::JoinFilterExpression::Parameter {
                operator: crate::optimizer_request::Comparison::Greater,
                index: 2,
            }
        );
    }

    #[test]
    fn filtered_join_select_uses_authorized_inner_index_for_nested_loop_plan() {
        let mut indexed_right = join_relation("rights", 0x41);
        indexed_right.range_access_paths =
            vec![crate::metadata_discovery::AuthorizedRangeAccessPath {
                index_id: [0x71; 16],
                column_ids: vec![[0x51; 16]],
            }];
        let encoded = optimize_join_select_v1_with_relations(
            &filtered_join_select_request(),
            Some(join_relation("lefts", 0x31)),
            Some(indexed_right.clone()),
        )
        .unwrap();
        let (_, kind, _) = crate::query_abi::decode_optimizer_plan_header_record(&encoded)
            .expect("optimizer must return a plan header");
        assert_eq!(
            kind, 17,
            "an authorized inner index should select nested loop"
        );
        let plan = decode_join_select_plan_v1(&encoded).unwrap();
        let index_join = plan.index_nested_loop.expect("kind 17 index strategy");
        assert_eq!(
            index_join.outer_side,
            crate::optimizer_request::JoinRelationSide::Left
        );
        assert_eq!(
            index_join.inner_side,
            crate::optimizer_request::JoinRelationSide::Right
        );
        assert_eq!(index_join.index_id, [0x71; 16]);
        assert_eq!(index_join.key_column.column_id, [0x51; 16]);
        assert!(plan.filter.is_some());
    }

    #[test]
    fn filtered_join_select_uses_left_index_as_fallback_inner() {
        let mut indexed_left = join_relation("lefts", 0x31);
        indexed_left.range_access_paths =
            vec![crate::metadata_discovery::AuthorizedRangeAccessPath {
                index_id: [0x72; 16],
                column_ids: vec![[0x41; 16]],
            }];
        let encoded = optimize_join_select_v1_with_relations(
            &filtered_join_select_request(),
            Some(indexed_left),
            Some(join_relation("rights", 0x41)),
        )
        .unwrap();
        let plan = decode_join_select_plan_v1(&encoded).unwrap();
        let index_join = plan.index_nested_loop.expect("kind 17 index strategy");
        assert_eq!(
            index_join.outer_side,
            crate::optimizer_request::JoinRelationSide::Right
        );
        assert_eq!(
            index_join.inner_side,
            crate::optimizer_request::JoinRelationSide::Left
        );
        assert_eq!(index_join.index_id, [0x72; 16]);
        assert_eq!(index_join.key_column.column_id, [0x41; 16]);
    }

    #[test]
    fn medium_join_cardinality_estimates_keep_hash_join() {
        let mut indexed_left = join_relation("lefts", 0x31);
        indexed_left.range_access_paths =
            vec![crate::metadata_discovery::AuthorizedRangeAccessPath {
                index_id: [0x72; 16],
                column_ids: vec![[0x41; 16]],
            }];
        let mut indexed_right = join_relation("rights", 0x41);
        indexed_right.range_access_paths =
            vec![crate::metadata_discovery::AuthorizedRangeAccessPath {
                index_id: [0x71; 16],
                column_ids: vec![[0x51; 16]],
            }];
        let encoded = optimize_join_select_v1_with_relations_and_estimates(
            &filtered_join_select_request(),
            Some(indexed_left),
            Some(indexed_right.clone()),
            Some(64),
            Some(96),
        )
        .unwrap();
        let plan = decode_join_select_plan_v1(&encoded).unwrap();
        assert!(plan.index_nested_loop.is_none());
    }

    #[test]
    fn one_known_small_join_input_orients_to_the_opposite_index() {
        let mut indexed_right = join_relation("rights", 0x41);
        indexed_right.range_access_paths =
            vec![crate::metadata_discovery::AuthorizedRangeAccessPath {
                index_id: [0x71; 16],
                column_ids: vec![[0x51; 16]],
            }];
        let encoded = optimize_join_select_v1_with_relations_and_estimates(
            &filtered_join_select_request(),
            Some(join_relation("lefts", 0x31)),
            Some(indexed_right),
            Some(4),
            Some(96),
        )
        .unwrap();
        let plan = decode_join_select_plan_v1(&encoded).unwrap();
        let index_join = plan
            .index_nested_loop
            .expect("small input should use index");
        assert_eq!(
            index_join.outer_side,
            crate::optimizer_request::JoinRelationSide::Left
        );
        assert_eq!(
            index_join.inner_side,
            crate::optimizer_request::JoinRelationSide::Right
        );
        assert_eq!(index_join.index_id, [0x71; 16]);
    }

    #[test]
    fn join_with_two_known_small_inputs_keeps_hash_join() {
        let mut indexed_left = join_relation("lefts", 0x31);
        indexed_left.range_access_paths =
            vec![crate::metadata_discovery::AuthorizedRangeAccessPath {
                index_id: [0x72; 16],
                column_ids: vec![[0x41; 16]],
            }];
        let mut indexed_right = join_relation("rights", 0x41);
        indexed_right.range_access_paths =
            vec![crate::metadata_discovery::AuthorizedRangeAccessPath {
                index_id: [0x71; 16],
                column_ids: vec![[0x51; 16]],
            }];
        let encoded = optimize_join_select_v1_with_relations_and_estimates(
            &filtered_join_select_request(),
            Some(indexed_left),
            Some(indexed_right.clone()),
            Some(4),
            Some(2),
        )
        .unwrap();
        let plan = decode_join_select_plan_v1(&encoded).unwrap();
        assert!(plan.index_nested_loop.is_none());

        let empty_outer = optimize_join_select_v1_with_relations_and_estimates(
            &filtered_join_select_request(),
            Some(join_relation("lefts", 0x31)),
            Some(indexed_right),
            Some(0),
            None,
        )
        .unwrap();
        let plan = decode_join_select_plan_v1(&empty_outer).unwrap();
        assert!(plan.index_nested_loop.is_none());
    }

    #[test]
    fn ordered_group_count_plan_carries_authorized_sort_items_and_limit() {
        let encoded = optimize_ordered_group_count_v1_with_relation(
            &ordered_group_count_request(),
            Some(relation()),
        )
        .unwrap();
        let plan = decode_ordered_group_count_plan_v1(&encoded).unwrap();
        assert_eq!(plan.relation.relation_id, [0x11; 16]);
        assert_eq!(plan.group_key.column_id, [0x22; 16]);
        assert_eq!(
            plan.scan_columns.as_slice(),
            std::slice::from_ref(&plan.group_key)
        );
        assert_eq!(plan.order_items.len(), 2);
        assert_eq!(plan.order_items[0].source, 1);
        assert!(plan.order_items[0].descending);
        assert_eq!(plan.order_items[1].source, 0);
        assert!(plan.order_items[1].nulls_first);
        assert_eq!(plan.limit, Some(5));
    }

    #[test]
    fn authorized_update_request_composes_binding_and_plan_lowering() {
        let encoded =
            optimize_update_v1_with_relation(&update_request(), Some(relation())).unwrap();
        let plan = decode_update_plan_v1(&encoded).unwrap();
        assert_eq!(plan.relation.schema_id, [0x10; 16]);
        assert_eq!(plan.relation.relation_id, [0x11; 16]);
        assert_eq!(
            plan.scan_columns
                .iter()
                .map(|column| column.ordinal)
                .collect::<Vec<_>>(),
            [0, 1]
        );
        assert_eq!(plan.assignments.len(), 1);
        assert_eq!(plan.assignments[0].column.column_id, [0x22; 16]);
        assert_eq!(
            plan.assignments[0].value,
            crate::optimizer_binding::BoundUpdateValue::Parameter(1)
        );
        assert!(matches!(
            optimize_update_v1_with_relation(&update_request(), None),
            Err(OptimizerError::Bind(MetadataBindError::Bind(
                crate::optimizer_binding::BindError::RelationUnavailable
            )))
        ));
    }
}
