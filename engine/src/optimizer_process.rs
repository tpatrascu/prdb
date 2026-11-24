//! Protocol loop used by the isolated optimizer executable.

use crate::ipc::{DispatchError, IpcFrame, Operation, ServiceRole, authorize_operation};
use crate::optimizer_ipc::{
    MAX_OPTIMIZER_REQUESTS_PER_CONNECTION, OptimizerPayloadError, OptimizerResponse,
    OptimizerStatus, status_for_error,
};
use crate::optimizer_plan::MAX_PLAN_BYTES;
use crate::planner_ipc::{
    DescribePlanRelationRequest, DescribePlanRelationResponse, MAX_PLANNER_REQUESTS_PER_CONNECTION,
    PlannerRequestError,
};
use std::collections::HashSet;
use std::io::{Read, Write};

#[derive(Debug)]
pub enum OptimizerProcessError {
    Frame(crate::ipc::FrameError),
    Dispatch(DispatchError),
    PlannerClosed,
    PlannerResponseMismatch,
    PlannerPayload(PlannerRequestError),
    Response(OptimizerPayloadError),
    RequestLimitExceeded,
    DuplicateRequestId,
}

impl std::fmt::Display for OptimizerProcessError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "optimizer process protocol error: {self:?}")
    }
}

impl std::error::Error for OptimizerProcessError {}

/// Serve structured requests using only two inherited Unix streams: an
/// executor-submission endpoint and an invocation-bound planner endpoint.
/// The optimizer never opens storage files or receives executor handles.
pub fn serve_optimizer_process<S: Read + Write, P: Read + Write>(
    submission: &mut S,
    planner: &mut P,
) -> Result<(), OptimizerProcessError> {
    let mut request_ids = HashSet::new();
    // Planner IPC is shared with direct SDK planner requests. Keep optimizer
    // metadata lookups in a disjoint range and give every relation its own ID.
    let mut next_planner_request_id = MAX_PLANNER_REQUESTS_PER_CONNECTION as u64 + 1;
    loop {
        let request = match crate::ipc::read_frame(submission) {
            Ok(Some(request)) => request,
            Ok(None) => return Ok(()),
            Err(error) => return Err(OptimizerProcessError::Frame(error)),
        };
        if request_ids.len() >= MAX_OPTIMIZER_REQUESTS_PER_CONNECTION {
            return Err(OptimizerProcessError::RequestLimitExceeded);
        }
        if !request_ids.insert(request.request_id) {
            return Err(OptimizerProcessError::DuplicateRequestId);
        }
        let operation =
            Operation::from_code(request.operation).map_err(OptimizerProcessError::Dispatch)?;
        authorize_operation(ServiceRole::OptimizerSubmission, operation)
            .map_err(OptimizerProcessError::Dispatch)?;
        if !matches!(
            operation,
            Operation::OptimizeSelect
                | Operation::OptimizeDelete
                | Operation::OptimizeUpdate
                | Operation::OptimizeJoinSelect
                | Operation::OptimizeGroupCount
        ) {
            return Err(OptimizerProcessError::Dispatch(
                DispatchError::OperationNotAllowed {
                    role: ServiceRole::OptimizerSubmission,
                    operation,
                },
            ));
        }

        let decoded_names = match operation {
            Operation::OptimizeSelect => {
                if request.payload.get(2)
                    == Some(&crate::query_abi::OPTIMIZER_ORDERED_SELECT_STATEMENT)
                {
                    crate::optimizer_request::decode_ordered_select_request(&request.payload)
                        .map(|decoded| vec![(decoded.schema_name, decoded.relation_name)])
                } else {
                    crate::optimizer_request::decode_select_request(&request.payload)
                        .map(|decoded| vec![(decoded.schema_name, decoded.relation_name)])
                }
            }
            Operation::OptimizeDelete => {
                crate::optimizer_request::decode_delete_request(&request.payload)
                    .map(|decoded| vec![(decoded.schema_name, decoded.relation_name)])
            }
            Operation::OptimizeUpdate => {
                crate::optimizer_request::decode_update_request(&request.payload)
                    .map(|decoded| vec![(decoded.schema_name, decoded.relation_name)])
            }
            Operation::OptimizeJoinSelect => crate::optimizer_request::decode_join_select_request(
                &request.payload,
            )
            .map(|decoded| {
                vec![
                    (decoded.schema_name.clone(), decoded.left_relation_name),
                    (decoded.schema_name, decoded.right_relation_name),
                ]
            }),
            Operation::OptimizeGroupCount => {
                if request.payload.get(2).is_some_and(|statement| {
                    *statement == crate::query_abi::OPTIMIZER_FILTERED_COMPOSITE_GROUP_AGGREGATE_STATEMENT
                        || *statement == crate::query_abi::OPTIMIZER_ORDERED_FILTERED_COMPOSITE_GROUP_AGGREGATE_STATEMENT
                })
                {
                    crate::optimizer_request::decode_filtered_composite_group_aggregate_request(
                        &request.payload,
                    )
                    .map(|decoded| vec![(decoded.schema_name, decoded.relation_name)])
                } else if request.payload.get(2)
                    == Some(
                        &crate::query_abi::OPTIMIZER_ORDERED_COMPOSITE_GROUP_AGGREGATE_STATEMENT,
                    )
                {
                    crate::optimizer_request::decode_ordered_composite_group_aggregate_request(
                        &request.payload,
                    )
                    .map(|decoded| vec![(decoded.schema_name, decoded.relation_name)])
                } else if request.payload.get(2)
                    == Some(&crate::query_abi::OPTIMIZER_COMPOSITE_GROUP_AGGREGATE_STATEMENT)
                {
                    crate::optimizer_request::decode_composite_group_aggregate_request(
                        &request.payload,
                    )
                    .map(|decoded| vec![(decoded.schema_name, decoded.relation_name)])
                } else if request.payload.get(2)
                    == Some(&crate::query_abi::OPTIMIZER_ORDERED_GROUP_AGGREGATE_STATEMENT)
                {
                    crate::optimizer_request::decode_ordered_group_aggregate_request(
                        &request.payload,
                    )
                    .map(|decoded| vec![(decoded.schema_name, decoded.relation_name)])
                } else if request.payload.get(2)
                    == Some(&crate::query_abi::OPTIMIZER_GROUP_AGGREGATE_STATEMENT)
                {
                    crate::optimizer_request::decode_group_aggregate_request(&request.payload)
                        .map(|decoded| vec![(decoded.schema_name, decoded.relation_name)])
                } else if request.payload.get(2)
                    == Some(&crate::query_abi::OPTIMIZER_COMPOSITE_GROUP_COUNT_ORDERED_STATEMENT)
                {
                    crate::optimizer_request::decode_ordered_composite_group_count_request(
                        &request.payload,
                    )
                    .map(|decoded| vec![(decoded.schema_name, decoded.relation_name)])
                } else if request.payload.get(2)
                    == Some(&crate::query_abi::OPTIMIZER_COMPOSITE_GROUP_COUNT_STATEMENT)
                {
                    crate::optimizer_request::decode_composite_group_count_request(&request.payload)
                        .map(|decoded| vec![(decoded.schema_name, decoded.relation_name)])
                } else if request.payload.get(2)
                    == Some(&crate::query_abi::OPTIMIZER_GROUP_COUNT_ORDERED_STATEMENT)
                {
                    crate::optimizer_request::decode_ordered_group_count_request(&request.payload)
                        .map(|decoded| vec![(decoded.schema_name, decoded.relation_name)])
                } else {
                    crate::optimizer_request::decode_group_count_request(&request.payload)
                        .map(|decoded| vec![(decoded.schema_name, decoded.relation_name)])
                }
            }
            _ => unreachable!("optimizer operation was checked above"),
        };
        let relation_names = match decoded_names {
            Ok(names) => names,
            Err(_) => {
                write_submission_response(
                    submission,
                    request.operation,
                    request.request_id,
                    OptimizerResponse::failure(OptimizerStatus::InvalidRequest)
                        .map_err(OptimizerProcessError::Response)?,
                )?;
                continue;
            }
        };
        let mut relations = Vec::with_capacity(relation_names.len());
        let mut proven_empty = Vec::with_capacity(relation_names.len());
        let mut visible_rows_estimates = Vec::with_capacity(relation_names.len());
        for (schema_name, relation_name) in relation_names {
            let planner_request_id = next_planner_request_id;
            next_planner_request_id = next_planner_request_id
                .checked_add(1)
                .ok_or(OptimizerProcessError::RequestLimitExceeded)?;
            let include_scan_estimate = operation == Operation::OptimizeSelect
                && request.payload.get(2) == Some(&crate::query_abi::OPTIMIZER_SELECT_STATEMENT);
            let planner_request = IpcFrame {
                operation: Operation::DescribePlanRelation as u16,
                request_id: planner_request_id,
                payload: DescribePlanRelationRequest {
                    schema_name,
                    relation_name,
                    include_scan_estimate,
                    population_estimate_max_rows: (operation == Operation::OptimizeJoinSelect)
                        .then_some(crate::optimizer_plan::JOIN_POPULATION_ESTIMATE_MAX_ROWS),
                }
                .encode()
                .map_err(OptimizerProcessError::PlannerPayload)?,
            };
            authorize_operation(ServiceRole::Planner, Operation::DescribePlanRelation)
                .map_err(OptimizerProcessError::Dispatch)?;
            crate::ipc::write_frame(planner, &planner_request)
                .map_err(OptimizerProcessError::Frame)?;
            let planner_response = crate::ipc::read_frame(planner)
                .map_err(OptimizerProcessError::Frame)?
                .ok_or(OptimizerProcessError::PlannerClosed)?;
            if planner_response.operation != planner_request.operation
                || planner_response.request_id != planner_request.request_id
            {
                return Err(OptimizerProcessError::PlannerResponseMismatch);
            }
            let described = DescribePlanRelationResponse::decode(&planner_response.payload)
                .map_err(OptimizerProcessError::PlannerPayload)?;
            proven_empty.push(described.proven_empty);
            visible_rows_estimates.push(described.visible_rows_estimate);
            relations.push(described.relation);
        }
        let join_cardinality_estimate = if operation == Operation::OptimizeJoinSelect
            && join_fanout_estimate_is_eligible(
                visible_rows_estimates.first().copied().flatten(),
                visible_rows_estimates.get(1).copied().flatten(),
            ) {
            let decoded = crate::optimizer_request::decode_join_select_request(&request.payload)
                .map_err(|_| OptimizerProcessError::PlannerResponseMismatch)?;
            match (
                relations.first().and_then(Option::as_ref),
                relations.get(1).and_then(Option::as_ref),
            ) {
                (Some(left_relation), Some(right_relation)) => {
                    let left_key_ids = decoded
                        .join_keys
                        .iter()
                        .map(|key| {
                            left_relation
                                .relation
                                .columns
                                .iter()
                                .find(|column| {
                                    column.name.eq_ignore_ascii_case(&key.left_column_name)
                                })
                                .map(|column| column.column_id)
                        })
                        .collect::<Option<Vec<_>>>();
                    let right_key_ids = decoded
                        .join_keys
                        .iter()
                        .map(|key| {
                            right_relation
                                .relation
                                .columns
                                .iter()
                                .find(|column| {
                                    column.name.eq_ignore_ascii_case(&key.right_column_name)
                                })
                                .map(|column| column.column_id)
                        })
                        .collect::<Option<Vec<_>>>();
                    match (left_key_ids, right_key_ids) {
                        (Some(left_key_ids), Some(right_key_ids)) => {
                            if has_join_index_path(left_relation, &left_key_ids)
                                || has_join_index_path(right_relation, &right_key_ids)
                            {
                                request_join_cardinality_estimate(
                                    planner,
                                    &mut next_planner_request_id,
                                    decoded.schema_name,
                                    decoded.left_relation_name,
                                    decoded.right_relation_name,
                                    left_key_ids,
                                    right_key_ids,
                                )?
                            } else {
                                None
                            }
                        }
                        _ => None,
                    }
                }
                _ => None,
            }
        } else {
            None
        };
        let optimized = match operation {
            Operation::OptimizeSelect => {
                if request.payload.get(2)
                    == Some(&crate::query_abi::OPTIMIZER_ORDERED_SELECT_STATEMENT)
                {
                    crate::optimizer_service::optimize_ordered_select_v1_with_relation(
                        &request.payload,
                        relations.remove(0),
                    )
                } else {
                    crate::optimizer_service::optimize_select_v1_with_relation(
                        &request.payload,
                        relations.remove(0),
                    )
                }
            }
            Operation::OptimizeDelete => {
                crate::optimizer_service::optimize_delete_v1_with_relation(
                    &request.payload,
                    relations.remove(0),
                )
            }
            Operation::OptimizeUpdate => {
                crate::optimizer_service::optimize_update_v1_with_relation(
                    &request.payload,
                    relations.remove(0),
                )
            }
            Operation::OptimizeJoinSelect => {
                crate::optimizer_service::optimize_join_select_v1_with_relations_and_estimates_and_cardinality(
                    &request.payload,
                    relations.remove(0),
                    relations.remove(0),
                    visible_rows_estimates.first().copied().flatten(),
                    visible_rows_estimates.get(1).copied().flatten(),
                    join_cardinality_estimate,
                )
            }
            Operation::OptimizeGroupCount => {
                if request.payload.get(2).is_some_and(|statement| {
                    *statement == crate::query_abi::OPTIMIZER_FILTERED_COMPOSITE_GROUP_AGGREGATE_STATEMENT
                        || *statement == crate::query_abi::OPTIMIZER_ORDERED_FILTERED_COMPOSITE_GROUP_AGGREGATE_STATEMENT
                })
                {
                    crate::optimizer_service::optimize_filtered_composite_group_aggregate_v1_with_relation(
                        &request.payload,
                        relations.remove(0),
                    )
                } else if request.payload.get(2)
                    == Some(
                        &crate::query_abi::OPTIMIZER_ORDERED_COMPOSITE_GROUP_AGGREGATE_STATEMENT,
                    )
                {
                    crate::optimizer_service::optimize_ordered_composite_group_aggregate_v1_with_relation(
                        &request.payload,
                        relations.remove(0),
                    )
                } else if request.payload.get(2)
                    == Some(&crate::query_abi::OPTIMIZER_COMPOSITE_GROUP_AGGREGATE_STATEMENT)
                {
                    crate::optimizer_service::optimize_composite_group_aggregate_v1_with_relation(
                        &request.payload,
                        relations.remove(0),
                    )
                } else if request.payload.get(2)
                    == Some(&crate::query_abi::OPTIMIZER_ORDERED_GROUP_AGGREGATE_STATEMENT)
                {
                    crate::optimizer_service::optimize_ordered_group_aggregate_v1_with_relation(
                        &request.payload,
                        relations.remove(0),
                    )
                } else if request.payload.get(2)
                    == Some(&crate::query_abi::OPTIMIZER_GROUP_AGGREGATE_STATEMENT)
                {
                    crate::optimizer_service::optimize_group_aggregate_v1_with_relation(
                        &request.payload,
                        relations.remove(0),
                    )
                } else if request.payload.get(2)
                    == Some(&crate::query_abi::OPTIMIZER_COMPOSITE_GROUP_COUNT_ORDERED_STATEMENT)
                {
                    crate::optimizer_service::optimize_ordered_composite_group_count_v1_with_relation(
                        &request.payload, relations.remove(0),
                    )
                } else if request.payload.get(2)
                    == Some(&crate::query_abi::OPTIMIZER_COMPOSITE_GROUP_COUNT_STATEMENT)
                {
                    crate::optimizer_service::optimize_composite_group_count_v1_with_relation(
                        &request.payload,
                        relations.remove(0),
                    )
                } else if request.payload.get(2)
                    == Some(&crate::query_abi::OPTIMIZER_GROUP_COUNT_ORDERED_STATEMENT)
                {
                    crate::optimizer_service::optimize_ordered_group_count_v1_with_relation(
                        &request.payload,
                        relations.remove(0),
                    )
                } else {
                    crate::optimizer_service::optimize_group_count_v1_with_relation(
                        &request.payload,
                        relations.remove(0),
                    )
                }
            }
            _ => unreachable!("optimizer operation was checked above"),
        };
        let response = match optimized {
            Ok(mut plan) if plan.len() <= MAX_PLAN_BYTES => {
                if operation == Operation::OptimizeSelect
                    && request.payload.get(2) == Some(&crate::query_abi::OPTIMIZER_SELECT_STATEMENT)
                    && let Ok(mut decoded) = crate::optimizer_plan::decode_plan_v1(&plan)
                    && let Ok(true) = crate::optimizer_plan::apply_population_scan_heuristic(
                        &mut decoded,
                        visible_rows_estimates.first().copied().flatten(),
                    )
                    && let Ok(encoded) = crate::optimizer_plan::encode_plan_v1(&decoded)
                {
                    plan = encoded;
                }
                let predicate_estimate = if operation == Operation::OptimizeSelect
                    && request.payload.get(2) == Some(&crate::query_abi::OPTIMIZER_SELECT_STATEMENT)
                    && proven_empty.first() != Some(&true)
                {
                    let decoded_request =
                        crate::optimizer_request::decode_select_request(&request.payload).ok();
                    let decoded_plan = crate::optimizer_plan::decode_plan_v1(&plan).ok();
                    match (decoded_request, decoded_plan) {
                        (Some(select), Some(decoded_plan)) if select.predicate.is_some() => {
                            let bound_predicate = decoded_plan.nodes.iter().find_map(|node| {
                                if let crate::optimizer_plan::PlanNode::Filter(predicate) = node {
                                    Some(predicate)
                                } else {
                                    None
                                }
                            });
                            if let Some(predicate) = bound_predicate {
                                Some(request_predicate_estimate(
                                    planner,
                                    &mut next_planner_request_id,
                                    select.schema_name,
                                    select.relation_name,
                                    predicate,
                                )?)
                            } else {
                                None
                            }
                        }
                        _ => None,
                    }
                } else {
                    None
                };
                let predicate_proves_empty = matches!(
                    &predicate_estimate,
                    Some(crate::planner_ipc::EstimatePredicateResponse::Exact {
                        matching_rows: 0,
                        ..
                    })
                );
                let predicate_visible_rows = match &predicate_estimate {
                    Some(crate::planner_ipc::EstimatePredicateResponse::Exact {
                        visible_rows,
                        ..
                    }) => Some(*visible_rows),
                    _ => None,
                };
                let predicate_matching_rows = match &predicate_estimate {
                    Some(crate::planner_ipc::EstimatePredicateResponse::Exact {
                        matching_rows,
                        ..
                    }) => Some(*matching_rows),
                    _ => None,
                };
                if operation == Operation::OptimizeSelect
                    && request.payload.get(2) == Some(&crate::query_abi::OPTIMIZER_SELECT_STATEMENT)
                    && let Ok(mut decoded) = crate::optimizer_plan::decode_plan_v1(&plan)
                    && let Ok(true) = crate::optimizer_plan::apply_population_scan_heuristic(
                        &mut decoded,
                        predicate_visible_rows,
                    )
                    && let Ok(encoded) = crate::optimizer_plan::encode_plan_v1(&decoded)
                {
                    plan = encoded;
                }
                if operation == Operation::OptimizeSelect
                    && request.payload.get(2) == Some(&crate::query_abi::OPTIMIZER_SELECT_STATEMENT)
                    && let Ok(mut decoded) = crate::optimizer_plan::decode_plan_v1(&plan)
                    && let Ok(true) = crate::optimizer_plan::apply_range_selectivity_scan_heuristic(
                        &mut decoded,
                        predicate_visible_rows,
                        predicate_matching_rows,
                    )
                    && let Ok(encoded) = crate::optimizer_plan::encode_plan_v1(&decoded)
                {
                    plan = encoded;
                }
                if operation == Operation::OptimizeSelect
                    && request.payload.get(2) == Some(&crate::query_abi::OPTIMIZER_SELECT_STATEMENT)
                    && (proven_empty.first() == Some(&true) || predicate_proves_empty)
                    && let Ok(mut decoded) = crate::optimizer_plan::decode_plan_v1(&plan)
                {
                    decoded.unique_probe = None;
                    decoded.range_scan = None;
                    decoded.empty_view_hint = true;
                    if let Ok(encoded) = crate::optimizer_plan::encode_plan_v1(&decoded) {
                        plan = encoded;
                    }
                }
                if plan.len() <= MAX_PLAN_BYTES {
                    OptimizerResponse::success(plan)
                } else {
                    OptimizerResponse::failure(OptimizerStatus::Internal)
                }
            }
            Ok(_) => OptimizerResponse::failure(OptimizerStatus::Internal),
            Err(error) => OptimizerResponse::failure(status_for_error(&error)),
        }
        .map_err(OptimizerProcessError::Response)?;
        write_submission_response(submission, request.operation, request.request_id, response)?;
    }
}

fn join_fanout_estimate_is_eligible(left_rows: Option<u64>, right_rows: Option<u64>) -> bool {
    let (Some(left_rows), Some(right_rows)) = (left_rows, right_rows) else {
        return false;
    };
    crate::optimizer_plan::fanout_aware_join_inputs_are_eligible(left_rows, right_rows)
        || crate::optimizer_plan::fanout_aware_join_inputs_are_eligible(right_rows, left_rows)
}

fn has_join_index_path(
    relation: &crate::metadata_discovery::AuthorizedPlanRelation,
    join_key_column_ids: &[[u8; 16]],
) -> bool {
    relation
        .unique_access_paths
        .iter()
        .map(|path| path.column_ids.as_slice())
        .chain(
            relation
                .range_access_paths
                .iter()
                .map(|path| path.column_ids.as_slice()),
        )
        .any(|column_ids| {
            column_ids
                .first()
                .is_some_and(|column_id| join_key_column_ids.contains(column_id))
        })
}

fn request_predicate_estimate<P: Read + Write>(
    planner: &mut P,
    next_request_id: &mut u64,
    schema_name: String,
    relation_name: String,
    predicate: &crate::optimizer_binding::BoundPredicate,
) -> Result<crate::planner_ipc::EstimatePredicateResponse, OptimizerProcessError> {
    let request_id = *next_request_id;
    *next_request_id = next_request_id
        .checked_add(1)
        .ok_or(OptimizerProcessError::RequestLimitExceeded)?;
    let request = IpcFrame {
        operation: Operation::EstimateScan as u16,
        request_id,
        payload: crate::planner_ipc::EstimatePredicateRequest::from_bound_predicate(
            schema_name,
            relation_name,
            predicate,
        )
        .and_then(|request| request.encode())
        .map_err(OptimizerProcessError::PlannerPayload)?,
    };
    authorize_operation(ServiceRole::Planner, Operation::EstimateScan)
        .map_err(OptimizerProcessError::Dispatch)?;
    crate::ipc::write_frame(planner, &request).map_err(OptimizerProcessError::Frame)?;
    let response = crate::ipc::read_frame(planner)
        .map_err(OptimizerProcessError::Frame)?
        .ok_or(OptimizerProcessError::PlannerClosed)?;
    if response.operation != request.operation || response.request_id != request.request_id {
        return Err(OptimizerProcessError::PlannerResponseMismatch);
    }
    crate::planner_ipc::EstimatePredicateResponse::decode(&response.payload)
        .map_err(OptimizerProcessError::PlannerPayload)
}

fn request_join_cardinality_estimate<P: Read + Write>(
    planner: &mut P,
    next_request_id: &mut u64,
    schema_name: String,
    left_relation_name: String,
    right_relation_name: String,
    left_key_column_ids: Vec<[u8; 16]>,
    right_key_column_ids: Vec<[u8; 16]>,
) -> Result<Option<u64>, OptimizerProcessError> {
    let request_id = *next_request_id;
    *next_request_id = next_request_id
        .checked_add(1)
        .ok_or(OptimizerProcessError::RequestLimitExceeded)?;
    let request = IpcFrame {
        operation: Operation::EstimateScan as u16,
        request_id,
        payload: crate::planner_ipc::EstimateJoinRequest {
            schema_name,
            left_relation_name,
            right_relation_name,
            left_key_column_ids,
            right_key_column_ids,
            maximum_rows: crate::statistics::MAX_JOIN_ESTIMATE_ROWS,
        }
        .encode()
        .map_err(OptimizerProcessError::PlannerPayload)?,
    };
    authorize_operation(ServiceRole::Planner, Operation::EstimateScan)
        .map_err(OptimizerProcessError::Dispatch)?;
    crate::ipc::write_frame(planner, &request).map_err(OptimizerProcessError::Frame)?;
    let response = crate::ipc::read_frame(planner)
        .map_err(OptimizerProcessError::Frame)?
        .ok_or(OptimizerProcessError::PlannerClosed)?;
    if response.operation != request.operation || response.request_id != request.request_id {
        return Err(OptimizerProcessError::PlannerResponseMismatch);
    }
    match crate::planner_ipc::EstimateJoinResponse::decode(&response.payload)
        .map_err(OptimizerProcessError::PlannerPayload)?
    {
        crate::planner_ipc::EstimateJoinResponse::Exact { joined_rows } => Ok(Some(joined_rows)),
        crate::planner_ipc::EstimateJoinResponse::Unknown => Ok(None),
    }
}

fn write_submission_response<S: Write>(
    submission: &mut S,
    operation: u16,
    request_id: u64,
    response: OptimizerResponse,
) -> Result<(), OptimizerProcessError> {
    let frame = IpcFrame {
        operation,
        request_id,
        payload: response.encode().map_err(OptimizerProcessError::Response)?,
    };
    crate::ipc::write_frame(submission, &frame).map_err(OptimizerProcessError::Frame)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_discovery::{
        AuthorizedPlanRelation, AuthorizedRelationDescription, VisibleColumn,
    };
    use crate::optimizer_ipc::OptimizerStatus;
    use crate::optimizer_plan::{PlanNode, decode_plan_v1};
    use crate::planner_ipc::DescribePlanRelationResponse;
    use crate::tuple::ScalarType;
    use std::net::Shutdown;
    use std::os::unix::net::UnixStream;
    use std::time::Duration;

    #[test]
    fn join_fanout_estimate_eligibility_covers_the_19_row_outer_limit() {
        assert!(join_fanout_estimate_is_eligible(Some(16), Some(128)));
        assert!(!join_fanout_estimate_is_eligible(Some(16), Some(127)));
        assert!(join_fanout_estimate_is_eligible(Some(17), Some(119)));
        assert!(join_fanout_estimate_is_eligible(Some(119), Some(17)));
        assert!(join_fanout_estimate_is_eligible(Some(18), Some(126)));
        assert!(join_fanout_estimate_is_eligible(Some(126), Some(18)));
        assert!(join_fanout_estimate_is_eligible(Some(19), Some(133)));
        assert!(join_fanout_estimate_is_eligible(Some(133), Some(19)));
        assert!(!join_fanout_estimate_is_eligible(Some(17), Some(118)));
        assert!(!join_fanout_estimate_is_eligible(Some(18), Some(125)));
        assert!(!join_fanout_estimate_is_eligible(Some(19), Some(132)));
        assert!(!join_fanout_estimate_is_eligible(Some(20), Some(140)));
        assert!(!join_fanout_estimate_is_eligible(Some(17), None));
    }

    fn select_request() -> Vec<u8> {
        let mut bytes = vec![0, 1, 1, 0];
        for name in ["public", "items"] {
            bytes.push(name.len() as u8);
            bytes.extend_from_slice(name.as_bytes());
        }
        bytes.extend_from_slice(&1u16.to_be_bytes());
        bytes.push(4);
        bytes.extend_from_slice(b"name");
        bytes.extend_from_slice(&0u32.to_be_bytes());
        bytes.push(0);
        bytes
    }

    fn range_select_request() -> Vec<u8> {
        let mut bytes = select_request();
        let predicate = [0x20, 5, 0x10, 4, b'n', b'a', b'm', b'e', 0x03, 0, 1, b'm'];
        bytes.truncate(bytes.len() - 5);
        bytes.extend_from_slice(&(predicate.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&predicate);
        bytes.push(0);
        bytes
    }

    fn select_plan_with_predicate_estimate(
        estimate_response: crate::planner_ipc::EstimatePredicateResponse,
    ) -> crate::optimizer_plan::SingleTablePlanV1 {
        let (mut submission_client, mut submission_server) = UnixStream::pair().unwrap();
        let (mut planner_client, mut planner_server) = UnixStream::pair().unwrap();
        std::thread::scope(|scope| {
            let planner_thread = scope.spawn(|| {
                let describe = crate::ipc::read_frame(&mut planner_server)
                    .unwrap()
                    .expect("optimizer must resolve relation metadata");
                crate::ipc::write_frame(
                    &mut planner_server,
                    &IpcFrame {
                        operation: describe.operation,
                        request_id: describe.request_id,
                        payload: DescribePlanRelationResponse {
                            relation: Some(AuthorizedPlanRelation {
                                schema_id: [1; 16],
                                schema_version: 3,
                                table_epoch: 7,
                                relation: AuthorizedRelationDescription {
                                    relation_id: [2; 16],
                                    name: "items".to_owned(),
                                    columns: vec![VisibleColumn {
                                        column_id: [3; 16],
                                        name: "name".to_owned(),
                                        ordinal: 0,
                                        scalar_type: ScalarType::Text,
                                        nullable: false,
                                    }],
                                },
                                unique_access_paths: Vec::new(),
                                range_access_paths: vec![
                                    crate::metadata_discovery::AuthorizedRangeAccessPath {
                                        index_id: [4; 16],
                                        column_ids: vec![[3; 16]],
                                    },
                                ],
                            }),
                            proven_empty: false,
                            visible_rows_estimate: None,
                        }
                        .encode()
                        .unwrap(),
                    },
                )
                .unwrap();
                let estimate = crate::ipc::read_frame(&mut planner_server)
                    .unwrap()
                    .expect("filtered SELECT must request a bounded predicate estimate");
                assert_eq!(estimate.operation, Operation::EstimateScan as u16);
                let decoded =
                    crate::planner_ipc::EstimatePredicateRequest::decode(&estimate.payload)
                        .unwrap();
                assert_eq!(decoded.schema_name, "public");
                assert_eq!(decoded.relation_name, "items");
                crate::ipc::write_frame(
                    &mut planner_server,
                    &IpcFrame {
                        operation: estimate.operation,
                        request_id: estimate.request_id,
                        payload: estimate_response.encode().unwrap(),
                    },
                )
                .unwrap();
            });
            let optimizer_thread = scope
                .spawn(|| serve_optimizer_process(&mut submission_server, &mut planner_client));
            crate::ipc::write_frame(
                &mut submission_client,
                &IpcFrame {
                    operation: Operation::OptimizeSelect as u16,
                    request_id: 32,
                    payload: range_select_request(),
                },
            )
            .unwrap();
            let response = crate::ipc::read_frame(&mut submission_client)
                .unwrap()
                .expect("optimizer must return a plan");
            let response = OptimizerResponse::decode(&response.payload).unwrap();
            assert_eq!(response.status, OptimizerStatus::Success);
            let plan = decode_plan_v1(response.plan.as_deref().unwrap()).unwrap();
            submission_client.shutdown(Shutdown::Write).unwrap();
            planner_thread.join().unwrap();
            optimizer_thread.join().unwrap().unwrap();
            plan
        })
    }

    fn ordered_select_request() -> Vec<u8> {
        let mut bytes = vec![
            0,
            crate::query_abi::OPTIMIZER_REQUEST_VERSION as u8,
            crate::query_abi::OPTIMIZER_ORDERED_SELECT_STATEMENT,
            0,
        ];
        for name in ["public", "items"] {
            bytes.push(name.len() as u8);
            bytes.extend_from_slice(name.as_bytes());
        }
        bytes.extend_from_slice(&1u16.to_be_bytes());
        bytes.push(2);
        bytes.extend_from_slice(b"id");
        bytes.push(2);
        bytes.extend_from_slice(b"id");
        bytes.extend_from_slice(&[1, 1]);
        bytes.extend_from_slice(&3u32.to_be_bytes());
        bytes
    }

    fn delete_request() -> Vec<u8> {
        let mut bytes = vec![0, 1, 2, 0];
        for name in ["public", "items"] {
            bytes.push(name.len() as u8);
            bytes.extend_from_slice(name.as_bytes());
        }
        bytes.extend_from_slice(&2u32.to_be_bytes());
        bytes.extend_from_slice(&[0x01, 1]);
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

    fn join_select_request() -> Vec<u8> {
        let mut bytes = vec![0, 1, crate::query_abi::OPTIMIZER_JOIN_SELECT_STATEMENT, 0];
        for name in ["public", "lefts", "rights"] {
            bytes.push(name.len() as u8);
            bytes.extend_from_slice(name.as_bytes());
        }
        bytes.extend_from_slice(&2u16.to_be_bytes());
        for (side, name) in [(0, "value"), (1, "value")] {
            bytes.push(side);
            bytes.push(name.len() as u8);
            bytes.extend_from_slice(name.as_bytes());
        }
        bytes.push(2);
        for name in ["id", "id", "active", "active"] {
            bytes.push(name.len() as u8);
            bytes.extend_from_slice(name.as_bytes());
        }
        bytes.extend_from_slice(&[1, 0, 0, 0, 5]);
        bytes
    }

    fn filtered_join_select_request() -> Vec<u8> {
        let mut bytes = join_select_request();
        bytes[2] = crate::query_abi::OPTIMIZER_FILTERED_JOIN_SELECT_STATEMENT;
        bytes.push(1);
        bytes.push(6);
        bytes.extend_from_slice(b"active");
        bytes.extend_from_slice(&[0, 4]);
        bytes.extend_from_slice(&3u32.to_be_bytes());
        bytes
    }

    fn group_count_request() -> Vec<u8> {
        let mut bytes = vec![0, 1, crate::query_abi::OPTIMIZER_GROUP_COUNT_STATEMENT, 0];
        for name in ["public", "items", "active"] {
            bytes.push(name.len() as u8);
            bytes.extend_from_slice(name.as_bytes());
        }
        bytes.extend_from_slice(&[1, 0, 0, 0, 5]);
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

    fn ordered_group_aggregate_request() -> Vec<u8> {
        let mut bytes = vec![
            0,
            1,
            crate::query_abi::OPTIMIZER_ORDERED_GROUP_AGGREGATE_STATEMENT,
            0,
        ];
        for name in ["public", "items", "id"] {
            bytes.push(name.len() as u8);
            bytes.extend_from_slice(name.as_bytes());
        }
        bytes.push(5);
        bytes.push(6);
        bytes.extend_from_slice(b"amount");
        bytes.extend_from_slice(&[2, 1, 1, 0, 0, 0, 1, 1, 0, 0, 0, 5]);
        bytes
    }

    fn composite_group_count_request() -> Vec<u8> {
        let mut bytes = vec![
            0,
            1,
            crate::query_abi::OPTIMIZER_COMPOSITE_GROUP_COUNT_STATEMENT,
            0,
        ];
        for name in ["public", "items"] {
            bytes.push(name.len() as u8);
            bytes.extend_from_slice(name.as_bytes());
        }
        bytes.push(2);
        for name in ["active", "id"] {
            bytes.push(name.len() as u8);
            bytes.extend_from_slice(name.as_bytes());
        }
        bytes.extend_from_slice(&[1, 0, 0, 0, 5]);
        bytes
    }

    fn composite_group_aggregate_request(kind: u8) -> Vec<u8> {
        let mut bytes = vec![
            0,
            1,
            crate::query_abi::OPTIMIZER_COMPOSITE_GROUP_AGGREGATE_STATEMENT,
            0,
        ];
        for name in ["public", "items"] {
            bytes.push(name.len() as u8);
            bytes.extend_from_slice(name.as_bytes());
        }
        bytes.push(2);
        for name in ["active", "id"] {
            bytes.push(name.len() as u8);
            bytes.extend_from_slice(name.as_bytes());
        }
        bytes.push(kind);
        if kind != 1 {
            bytes.push(6);
            bytes.extend_from_slice(b"amount");
        }
        bytes.extend_from_slice(&[1, 0, 0, 0, 5]);
        bytes
    }

    fn filtered_composite_group_aggregate_request() -> Vec<u8> {
        let mut bytes = vec![
            0,
            1,
            crate::query_abi::OPTIMIZER_FILTERED_COMPOSITE_GROUP_AGGREGATE_STATEMENT,
            0,
        ];
        for name in ["public", "items"] {
            bytes.push(name.len() as u8);
            bytes.extend_from_slice(name.as_bytes());
        }
        bytes.push(2);
        for name in ["active", "id"] {
            bytes.push(name.len() as u8);
            bytes.extend_from_slice(name.as_bytes());
        }
        bytes.push(4);
        bytes.extend_from_slice(&[6]);
        bytes.extend_from_slice(b"amount");
        let predicate = [
            0x20, 0, 0x10, 6, b'a', b'c', b't', b'i', b'v', b'e', 0x01, 1,
        ];
        bytes.extend_from_slice(&(predicate.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&predicate);
        bytes.extend_from_slice(&[1, 0, 0, 0, 5]);
        bytes
    }

    fn filtered_single_group_aggregate_request() -> Vec<u8> {
        let mut bytes = vec![
            0,
            1,
            crate::query_abi::OPTIMIZER_FILTERED_COMPOSITE_GROUP_AGGREGATE_STATEMENT,
            0,
        ];
        for name in ["public", "items"] {
            bytes.push(name.len() as u8);
            bytes.extend_from_slice(name.as_bytes());
        }
        bytes.push(1);
        bytes.push(6);
        bytes.extend_from_slice(b"active");
        bytes.push(4);
        bytes.push(6);
        bytes.extend_from_slice(b"amount");
        let predicate = [0x41, 0x10, 6, b'a', b'c', b't', b'i', b'v', b'e'];
        bytes.extend_from_slice(&(predicate.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&predicate);
        bytes.extend_from_slice(&[1, 0, 0, 0, 5]);
        bytes
    }

    fn ordered_filtered_composite_group_aggregate_request() -> Vec<u8> {
        let mut bytes = filtered_composite_group_aggregate_request();
        bytes[2] = crate::query_abi::OPTIMIZER_ORDERED_FILTERED_COMPOSITE_GROUP_AGGREGATE_STATEMENT;
        bytes.truncate(bytes.len() - 5);
        bytes.extend_from_slice(&[2, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 5]);
        bytes
    }

    fn eight_key_group_aggregate_request(kind: u8) -> Vec<u8> {
        let mut bytes = vec![
            0,
            1,
            crate::query_abi::OPTIMIZER_COMPOSITE_GROUP_AGGREGATE_STATEMENT,
            0,
        ];
        for name in ["public", "items"] {
            bytes.push(name.len() as u8);
            bytes.extend_from_slice(name.as_bytes());
        }
        bytes.push(8);
        for ordinal in 0..8 {
            let name = format!("key_{ordinal}");
            bytes.push(name.len() as u8);
            bytes.extend_from_slice(name.as_bytes());
        }
        bytes.push(kind);
        if kind != 1 {
            bytes.extend_from_slice(&[7]);
            bytes.extend_from_slice(b"measure");
        }
        bytes.extend_from_slice(&[1, 0, 0, 0, 5]);
        bytes
    }

    fn eight_key_ordered_group_aggregate_request() -> Vec<u8> {
        let mut bytes = vec![
            0,
            1,
            crate::query_abi::OPTIMIZER_ORDERED_COMPOSITE_GROUP_AGGREGATE_STATEMENT,
            0,
        ];
        for name in ["public", "items"] {
            bytes.push(name.len() as u8);
            bytes.extend_from_slice(name.as_bytes());
        }
        bytes.push(8);
        for ordinal in 0..8 {
            let name = format!("key_{ordinal}");
            bytes.push(name.len() as u8);
            bytes.extend_from_slice(name.as_bytes());
        }
        bytes.push(4);
        bytes.extend_from_slice(&[7]);
        bytes.extend_from_slice(b"measure");
        bytes.push(9);
        for key_index in 0..8 {
            bytes.extend_from_slice(&[0, key_index, 1, 0]);
        }
        bytes.extend_from_slice(&[1, 1, 0]);
        bytes.extend_from_slice(&[1, 0, 0, 0, 5]);
        bytes
    }

    fn ordered_composite_group_aggregate_request(kind: u8) -> Vec<u8> {
        let mut bytes = vec![
            0,
            1,
            crate::query_abi::OPTIMIZER_ORDERED_COMPOSITE_GROUP_AGGREGATE_STATEMENT,
            0,
        ];
        for name in ["public", "items"] {
            bytes.push(name.len() as u8);
            bytes.extend_from_slice(name.as_bytes());
        }
        bytes.push(2);
        for name in ["active", "id"] {
            bytes.push(name.len() as u8);
            bytes.extend_from_slice(name.as_bytes());
        }
        bytes.push(kind);
        if kind != 1 {
            bytes.push(6);
            bytes.extend_from_slice(b"amount");
        }
        bytes.extend_from_slice(&[
            2, 1, 1, 0, // selected aggregate DESC NULLS LAST
            0, 0, 1, 1, // first group key ASC NULLS FIRST
            1, 0, 0, 0, 5, // LIMIT 5
        ]);
        bytes
    }

    #[test]
    fn process_protocol_uses_planner_metadata_and_returns_a_validated_plan() {
        let (mut submission_client, mut submission_server) = UnixStream::pair().unwrap();
        let (mut planner_client, mut planner_server) = UnixStream::pair().unwrap();
        std::thread::scope(|scope| {
            let planner_thread = scope.spawn(|| {
                let request = crate::ipc::read_frame(&mut planner_server)
                    .unwrap()
                    .expect("optimizer must resolve plan relation metadata");
                assert_eq!(request.operation, Operation::DescribePlanRelation as u16);
                assert_eq!(request.request_id, 257);
                let relation = AuthorizedPlanRelation {
                    schema_id: [1; 16],
                    schema_version: 3,
                    table_epoch: 7,
                    relation: AuthorizedRelationDescription {
                        relation_id: [2; 16],
                        name: "items".to_owned(),
                        columns: vec![VisibleColumn {
                            column_id: [3; 16],
                            name: "name".to_owned(),
                            ordinal: 0,
                            scalar_type: ScalarType::Text,
                            nullable: false,
                        }],
                    },
                    unique_access_paths: Vec::new(),
                    range_access_paths: Vec::new(),
                };
                crate::ipc::write_frame(
                    &mut planner_server,
                    &IpcFrame {
                        operation: request.operation,
                        request_id: request.request_id,
                        payload: DescribePlanRelationResponse {
                            relation: Some(relation),
                            proven_empty: false,
                            visible_rows_estimate: None,
                        }
                        .encode()
                        .unwrap(),
                    },
                )
                .unwrap();
            });
            let optimizer_thread = scope
                .spawn(|| serve_optimizer_process(&mut submission_server, &mut planner_client));

            crate::ipc::write_frame(
                &mut submission_client,
                &IpcFrame {
                    operation: Operation::OptimizeSelect as u16,
                    request_id: 9,
                    payload: select_request(),
                },
            )
            .unwrap();
            let response = crate::ipc::read_frame(&mut submission_client)
                .unwrap()
                .expect("optimizer must return a plan");
            assert_eq!(response.request_id, 9);
            let response = OptimizerResponse::decode(&response.payload).unwrap();
            assert_eq!(response.status, OptimizerStatus::Success);
            let plan = decode_plan_v1(response.plan.as_deref().unwrap()).unwrap();
            assert_eq!(plan.relation.schema_id, [1; 16]);
            assert_eq!(plan.relation.schema_version, 3);
            assert_eq!(plan.relation.table_epoch, 7);
            assert!(
                !plan.empty_view_hint,
                "missing storage proof must keep the bounded scan plan"
            );
            assert!(
                matches!(plan.nodes.first(), Some(PlanNode::Scan { columns }) if columns[0].column_id == [3; 16])
            );
            submission_client.shutdown(Shutdown::Write).unwrap();
            planner_thread.join().unwrap();
            optimizer_thread.join().unwrap().unwrap();
        });
    }

    #[test]
    fn process_protocol_uses_small_cardinality_to_choose_sequential_scan() {
        let (mut submission_client, mut submission_server) = UnixStream::pair().unwrap();
        let (mut planner_client, mut planner_server) = UnixStream::pair().unwrap();
        std::thread::scope(|scope| {
            let planner_thread = scope.spawn(|| {
                let request = crate::ipc::read_frame(&mut planner_server)
                    .unwrap()
                    .expect("optimizer must resolve plan relation metadata");
                let relation = AuthorizedPlanRelation {
                    schema_id: [1; 16],
                    schema_version: 3,
                    table_epoch: 7,
                    relation: AuthorizedRelationDescription {
                        relation_id: [2; 16],
                        name: "items".to_owned(),
                        columns: vec![VisibleColumn {
                            column_id: [3; 16],
                            name: "name".to_owned(),
                            ordinal: 0,
                            scalar_type: ScalarType::Text,
                            nullable: false,
                        }],
                    },
                    unique_access_paths: Vec::new(),
                    range_access_paths: vec![
                        crate::metadata_discovery::AuthorizedRangeAccessPath {
                            index_id: [4; 16],
                            column_ids: vec![[3; 16]],
                        },
                    ],
                };
                crate::ipc::write_frame(
                    &mut planner_server,
                    &IpcFrame {
                        operation: request.operation,
                        request_id: request.request_id,
                        payload: DescribePlanRelationResponse {
                            relation: Some(relation),
                            proven_empty: false,
                            visible_rows_estimate: Some(4),
                        }
                        .encode()
                        .unwrap(),
                    },
                )
                .unwrap();
                let estimate = crate::ipc::read_frame(&mut planner_server)
                    .unwrap()
                    .expect("filtered SELECT must request a bounded predicate estimate");
                assert_eq!(estimate.operation, Operation::EstimateScan as u16);
                crate::ipc::write_frame(
                    &mut planner_server,
                    &IpcFrame {
                        operation: estimate.operation,
                        request_id: estimate.request_id,
                        payload: crate::planner_ipc::EstimatePredicateResponse::Unknown
                            .encode()
                            .unwrap(),
                    },
                )
                .unwrap();
            });
            let optimizer_thread = scope
                .spawn(|| serve_optimizer_process(&mut submission_server, &mut planner_client));

            crate::ipc::write_frame(
                &mut submission_client,
                &IpcFrame {
                    operation: Operation::OptimizeSelect as u16,
                    request_id: 1,
                    payload: range_select_request(),
                },
            )
            .unwrap();
            let response = crate::ipc::read_frame(&mut submission_client)
                .unwrap()
                .expect("optimizer must return a plan");
            let response = OptimizerResponse::decode(&response.payload).unwrap();
            assert_eq!(response.status, OptimizerStatus::Success);
            let plan = decode_plan_v1(response.plan.as_deref().unwrap()).unwrap();
            assert!(plan.range_scan.is_none());
            assert!(
                !plan.empty_view_hint,
                "unknown estimate must preserve normal execution"
            );
            assert!(matches!(plan.nodes.get(1), Some(PlanNode::Filter(_))));
            submission_client.shutdown(Shutdown::Write).unwrap();
            planner_thread.join().unwrap();
            optimizer_thread.join().unwrap().unwrap();
        });
    }

    #[test]
    fn process_protocol_uses_exact_empty_predicate_estimate_as_empty_plan_hint() {
        let plan = select_plan_with_predicate_estimate(
            crate::planner_ipc::EstimatePredicateResponse::Exact {
                visible_rows: 4,
                matching_rows: 0,
            },
        );
        assert!(plan.empty_view_hint);
        assert!(plan.unique_probe.is_none() && plan.range_scan.is_none());
    }

    #[test]
    fn process_protocol_uses_visible_population_for_scan_choice() {
        let plan = select_plan_with_predicate_estimate(
            crate::planner_ipc::EstimatePredicateResponse::Exact {
                visible_rows: 8,
                matching_rows: 2,
            },
        );
        assert!(!plan.empty_view_hint);
        assert!(plan.range_scan.is_some());
        assert!(matches!(plan.nodes.get(1), Some(PlanNode::Filter(_))));
    }

    #[test]
    fn process_protocol_uses_scan_for_broad_range_matches() {
        let plan = select_plan_with_predicate_estimate(
            crate::planner_ipc::EstimatePredicateResponse::Exact {
                visible_rows: 130,
                matching_rows: 80,
            },
        );
        assert!(!plan.empty_view_hint);
        assert!(plan.range_scan.is_none());
        assert!(matches!(plan.nodes.get(1), Some(PlanNode::Filter(_))));
    }

    #[test]
    fn process_protocol_retains_range_just_below_half_selectivity() {
        let plan = select_plan_with_predicate_estimate(
            crate::planner_ipc::EstimatePredicateResponse::Exact {
                visible_rows: 130,
                matching_rows: 64,
            },
        );
        assert!(plan.range_scan.is_some());
        assert!(matches!(plan.nodes.get(1), Some(PlanNode::Filter(_))));
    }

    #[test]
    fn process_protocol_uses_sequential_scan_for_small_visible_population() {
        let plan = select_plan_with_predicate_estimate(
            crate::planner_ipc::EstimatePredicateResponse::Exact {
                visible_rows: 4,
                matching_rows: 2,
            },
        );
        assert!(!plan.empty_view_hint);
        assert!(plan.unique_probe.is_none() && plan.range_scan.is_none());
        assert!(matches!(plan.nodes.get(1), Some(PlanNode::Filter(_))));
    }

    #[test]
    fn process_protocol_keeps_range_plan_when_predicate_estimate_is_unknown() {
        let plan = select_plan_with_predicate_estimate(
            crate::planner_ipc::EstimatePredicateResponse::Unknown,
        );
        assert!(!plan.empty_view_hint);
        assert!(plan.range_scan.is_some());
        assert!(matches!(plan.nodes.get(1), Some(PlanNode::Filter(_))));
    }

    #[test]
    fn process_protocol_terminates_when_predicate_estimate_stream_closes() {
        let (mut submission_client, mut submission_server) = UnixStream::pair().unwrap();
        let (mut planner_client, mut planner_server) = UnixStream::pair().unwrap();
        submission_client
            .set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .unwrap();
        let optimizer_thread = std::thread::spawn(move || {
            serve_optimizer_process(&mut submission_server, &mut planner_client)
        });

        crate::ipc::write_frame(
            &mut submission_client,
            &IpcFrame {
                operation: Operation::OptimizeSelect as u16,
                request_id: 51,
                payload: range_select_request(),
            },
        )
        .unwrap();
        let describe = crate::ipc::read_frame(&mut planner_server)
            .unwrap()
            .expect("optimizer must resolve the relation before estimating");
        crate::ipc::write_frame(
            &mut planner_server,
            &IpcFrame {
                operation: describe.operation,
                request_id: describe.request_id,
                payload: DescribePlanRelationResponse {
                    relation: Some(AuthorizedPlanRelation {
                        schema_id: [1; 16],
                        schema_version: 3,
                        table_epoch: 7,
                        relation: AuthorizedRelationDescription {
                            relation_id: [2; 16],
                            name: "items".to_owned(),
                            columns: vec![VisibleColumn {
                                column_id: [3; 16],
                                name: "name".to_owned(),
                                ordinal: 0,
                                scalar_type: ScalarType::Text,
                                nullable: false,
                            }],
                        },
                        unique_access_paths: Vec::new(),
                        range_access_paths: vec![
                            crate::metadata_discovery::AuthorizedRangeAccessPath {
                                index_id: [4; 16],
                                column_ids: vec![[3; 16]],
                            },
                        ],
                    }),
                    proven_empty: false,
                    visible_rows_estimate: None,
                }
                .encode()
                .unwrap(),
            },
        )
        .unwrap();
        let estimate = crate::ipc::read_frame(&mut planner_server)
            .unwrap()
            .expect("filtered SELECT must request a predicate estimate");
        assert_eq!(estimate.operation, Operation::EstimateScan as u16);
        planner_server.shutdown(std::net::Shutdown::Both).unwrap();

        assert!(
            crate::ipc::read_frame(&mut submission_client)
                .unwrap()
                .is_none(),
            "a cancelled or disconnected estimate must not become an unknown-estimate fallback plan"
        );
        assert!(matches!(
            optimizer_thread.join().unwrap(),
            Err(OptimizerProcessError::PlannerClosed)
        ));
    }

    #[test]
    fn process_protocol_backpressures_submissions_while_metadata_is_pending() {
        use std::io::Read;
        use std::time::Duration;

        let (mut submission_client, mut submission_server) = UnixStream::pair().unwrap();
        let (mut planner_client, mut planner_server) = UnixStream::pair().unwrap();
        let optimizer_thread = std::thread::spawn(move || {
            serve_optimizer_process(&mut submission_server, &mut planner_client)
        });

        for request_id in [9, 10] {
            crate::ipc::write_frame(
                &mut submission_client,
                &IpcFrame {
                    operation: Operation::OptimizeSelect as u16,
                    request_id,
                    payload: select_request(),
                },
            )
            .unwrap();
        }

        let first_planner_request = crate::ipc::read_frame(&mut planner_server)
            .unwrap()
            .expect("first submission must request planner metadata");
        assert_eq!(first_planner_request.request_id, 257);
        planner_server
            .set_read_timeout(Some(Duration::from_millis(25)))
            .unwrap();
        let mut unexpected = [0; 1];
        let error = planner_server.read(&mut unexpected).unwrap_err();
        assert!(
            matches!(
                error.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            ),
            "optimizer issued another planner request before resolving the first: {error}"
        );
        planner_server.set_read_timeout(None).unwrap();

        let relation = AuthorizedPlanRelation {
            schema_id: [1; 16],
            schema_version: 3,
            table_epoch: 7,
            relation: AuthorizedRelationDescription {
                relation_id: [2; 16],
                name: "items".to_owned(),
                columns: vec![VisibleColumn {
                    column_id: [3; 16],
                    name: "name".to_owned(),
                    ordinal: 0,
                    scalar_type: ScalarType::Text,
                    nullable: false,
                }],
            },
            unique_access_paths: Vec::new(),
            range_access_paths: Vec::new(),
        };
        crate::ipc::write_frame(
            &mut planner_server,
            &IpcFrame {
                operation: first_planner_request.operation,
                request_id: first_planner_request.request_id,
                payload: DescribePlanRelationResponse {
                    relation: Some(relation.clone()),
                    proven_empty: false,
                    visible_rows_estimate: None,
                }
                .encode()
                .unwrap(),
            },
        )
        .unwrap();
        let first_response = crate::ipc::read_frame(&mut submission_client)
            .unwrap()
            .expect("first submission must finish after metadata arrives");
        assert_eq!(first_response.request_id, 9);
        assert_eq!(
            OptimizerResponse::decode(&first_response.payload)
                .unwrap()
                .status,
            OptimizerStatus::Success
        );

        let second_planner_request = crate::ipc::read_frame(&mut planner_server)
            .unwrap()
            .expect("second submission must run after the first completes");
        assert_eq!(second_planner_request.request_id, 258);
        crate::ipc::write_frame(
            &mut planner_server,
            &IpcFrame {
                operation: second_planner_request.operation,
                request_id: second_planner_request.request_id,
                payload: DescribePlanRelationResponse {
                    relation: Some(relation),
                    proven_empty: false,
                    visible_rows_estimate: None,
                }
                .encode()
                .unwrap(),
            },
        )
        .unwrap();
        let second_response = crate::ipc::read_frame(&mut submission_client)
            .unwrap()
            .expect("second submission must finish after metadata arrives");
        assert_eq!(second_response.request_id, 10);
        assert_eq!(
            OptimizerResponse::decode(&second_response.payload)
                .unwrap()
                .status,
            OptimizerStatus::Success
        );
        submission_client.shutdown(Shutdown::Write).unwrap();
        optimizer_thread.join().unwrap().unwrap();
    }

    #[test]
    fn process_protocol_dispatches_ordered_select_through_one_authorized_relation() {
        let (mut submission_client, mut submission_server) = UnixStream::pair().unwrap();
        let (mut planner_client, mut planner_server) = UnixStream::pair().unwrap();
        std::thread::scope(|scope| {
            let planner_thread = scope.spawn(|| {
                let request = crate::ipc::read_frame(&mut planner_server)
                    .expect("optimizer must read planner request")
                    .expect("optimizer must resolve ordered SELECT relation");
                assert_eq!(request.request_id, 257);
                crate::ipc::write_frame(
                    &mut planner_server,
                    &IpcFrame {
                        operation: request.operation,
                        request_id: request.request_id,
                        payload: DescribePlanRelationResponse {
                            relation: Some(AuthorizedPlanRelation {
                                schema_id: [1; 16],
                                schema_version: 3,
                                table_epoch: 7,
                                relation: AuthorizedRelationDescription {
                                    relation_id: [2; 16],
                                    name: "items".to_owned(),
                                    columns: vec![VisibleColumn {
                                        column_id: [4; 16],
                                        name: "id".to_owned(),
                                        ordinal: 0,
                                        scalar_type: ScalarType::Integer,
                                        nullable: false,
                                    }],
                                },
                                unique_access_paths: Vec::new(),
                                range_access_paths: Vec::new(),
                            }),
                            proven_empty: false,
                            visible_rows_estimate: None,
                        }
                        .encode()
                        .unwrap(),
                    },
                )
                .unwrap();
            });
            let optimizer_thread = scope
                .spawn(|| serve_optimizer_process(&mut submission_server, &mut planner_client));
            crate::ipc::write_frame(
                &mut submission_client,
                &IpcFrame {
                    operation: Operation::OptimizeSelect as u16,
                    request_id: 25,
                    payload: ordered_select_request(),
                },
            )
            .unwrap();
            let response = crate::ipc::read_frame(&mut submission_client)
                .unwrap()
                .expect("optimizer must return an ordered SELECT plan");
            assert_eq!(response.request_id, 25);
            let response = OptimizerResponse::decode(&response.payload).unwrap();
            assert_eq!(response.status, OptimizerStatus::Success);
            let plan = crate::optimizer_plan::decode_ordered_select_plan_v1(
                response.plan.as_deref().unwrap(),
            )
            .unwrap();
            assert_eq!(plan.scan_columns[0].column_id, [4; 16]);
            assert_eq!(plan.projections[0].ordinal, 0);
            assert_eq!(plan.order_key.ordinal, 0);
            assert!(plan.descending);
            assert_eq!(plan.limit, Some(3));
            submission_client.shutdown(Shutdown::Write).unwrap();
            planner_thread.join().unwrap();
            optimizer_thread.join().unwrap().unwrap();
        });
    }

    #[test]
    fn process_protocol_dispatches_delete_and_returns_a_validated_delete_plan() {
        let (mut submission_client, mut submission_server) = UnixStream::pair().unwrap();
        let (mut planner_client, mut planner_server) = UnixStream::pair().unwrap();
        std::thread::scope(|scope| {
            let planner_thread = scope.spawn(|| {
                let request = crate::ipc::read_frame(&mut planner_server)
                    .unwrap()
                    .expect("optimizer must resolve DELETE relation metadata");
                assert_eq!(request.operation, Operation::DescribePlanRelation as u16);
                assert_eq!(request.request_id, 257);
                let relation = AuthorizedPlanRelation {
                    schema_id: [1; 16],
                    schema_version: 3,
                    table_epoch: 7,
                    relation: AuthorizedRelationDescription {
                        relation_id: [2; 16],
                        name: "items".to_owned(),
                        columns: vec![VisibleColumn {
                            column_id: [3; 16],
                            name: "id".to_owned(),
                            ordinal: 0,
                            scalar_type: ScalarType::Integer,
                            nullable: false,
                        }],
                    },
                    unique_access_paths: Vec::new(),
                    range_access_paths: Vec::new(),
                };
                crate::ipc::write_frame(
                    &mut planner_server,
                    &IpcFrame {
                        operation: request.operation,
                        request_id: request.request_id,
                        payload: DescribePlanRelationResponse {
                            relation: Some(relation),
                            proven_empty: false,
                            visible_rows_estimate: None,
                        }
                        .encode()
                        .unwrap(),
                    },
                )
                .unwrap();
            });
            let optimizer_thread = scope
                .spawn(|| serve_optimizer_process(&mut submission_server, &mut planner_client));

            crate::ipc::write_frame(
                &mut submission_client,
                &IpcFrame {
                    operation: Operation::OptimizeDelete as u16,
                    request_id: 10,
                    payload: delete_request(),
                },
            )
            .unwrap();
            let response = crate::ipc::read_frame(&mut submission_client)
                .unwrap()
                .expect("optimizer must return a DELETE plan");
            assert_eq!(response.operation, Operation::OptimizeDelete as u16);
            assert_eq!(response.request_id, 10);
            let response = OptimizerResponse::decode(&response.payload).unwrap();
            assert_eq!(response.status, OptimizerStatus::Success);
            let plan =
                crate::optimizer_plan::decode_delete_plan_v1(response.plan.as_deref().unwrap())
                    .unwrap();
            assert_eq!(plan.relation.schema_id, [1; 16]);
            assert_eq!(plan.relation.table_epoch, 7);
            assert!(plan.scan_columns.is_empty());
            submission_client.shutdown(Shutdown::Write).unwrap();
            planner_thread.join().unwrap();
            optimizer_thread.join().unwrap().unwrap();
        });
    }

    #[test]
    fn process_protocol_dispatches_update_and_returns_a_validated_update_plan() {
        let (mut submission_client, mut submission_server) = UnixStream::pair().unwrap();
        let (mut planner_client, mut planner_server) = UnixStream::pair().unwrap();
        std::thread::scope(|scope| {
            let planner_thread = scope.spawn(|| {
                let request = crate::ipc::read_frame(&mut planner_server)
                    .unwrap()
                    .expect("optimizer must resolve UPDATE relation metadata");
                assert_eq!(request.operation, Operation::DescribePlanRelation as u16);
                assert_eq!(request.request_id, 257);
                let relation = AuthorizedPlanRelation {
                    schema_id: [1; 16],
                    schema_version: 3,
                    table_epoch: 7,
                    relation: AuthorizedRelationDescription {
                        relation_id: [2; 16],
                        name: "items".to_owned(),
                        columns: vec![
                            VisibleColumn {
                                column_id: [3; 16],
                                name: "id".to_owned(),
                                ordinal: 0,
                                scalar_type: ScalarType::Integer,
                                nullable: false,
                            },
                            VisibleColumn {
                                column_id: [4; 16],
                                name: "active".to_owned(),
                                ordinal: 1,
                                scalar_type: ScalarType::Boolean,
                                nullable: true,
                            },
                        ],
                    },
                    unique_access_paths: Vec::new(),
                    range_access_paths: Vec::new(),
                };
                crate::ipc::write_frame(
                    &mut planner_server,
                    &IpcFrame {
                        operation: request.operation,
                        request_id: request.request_id,
                        payload: DescribePlanRelationResponse {
                            relation: Some(relation),
                            proven_empty: false,
                            visible_rows_estimate: None,
                        }
                        .encode()
                        .unwrap(),
                    },
                )
                .unwrap();
            });
            let optimizer_thread = scope
                .spawn(|| serve_optimizer_process(&mut submission_server, &mut planner_client));

            crate::ipc::write_frame(
                &mut submission_client,
                &IpcFrame {
                    operation: Operation::OptimizeUpdate as u16,
                    request_id: 11,
                    payload: update_request(),
                },
            )
            .unwrap();
            let response = crate::ipc::read_frame(&mut submission_client)
                .unwrap()
                .expect("optimizer must return an UPDATE plan");
            assert_eq!(response.operation, Operation::OptimizeUpdate as u16);
            assert_eq!(response.request_id, 11);
            let response = OptimizerResponse::decode(&response.payload).unwrap();
            assert_eq!(response.status, OptimizerStatus::Success);
            let plan =
                crate::optimizer_plan::decode_update_plan_v1(response.plan.as_deref().unwrap())
                    .unwrap();
            assert_eq!(plan.relation.schema_id, [1; 16]);
            assert_eq!(plan.relation.table_epoch, 7);
            assert_eq!(plan.scan_columns.len(), 2);
            assert_eq!(plan.assignments[0].column.column_id, [4; 16]);
            assert_eq!(
                plan.assignments[0].value,
                crate::optimizer_binding::BoundUpdateValue::Parameter(1)
            );
            submission_client.shutdown(Shutdown::Write).unwrap();
            planner_thread.join().unwrap();
            optimizer_thread.join().unwrap().unwrap();
        });
    }

    #[test]
    fn process_protocol_resolves_both_join_relations_and_returns_a_validated_join_plan() {
        let (mut submission_client, mut submission_server) = UnixStream::pair().unwrap();
        let (mut planner_client, mut planner_server) = UnixStream::pair().unwrap();
        submission_client
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        planner_server
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        std::thread::scope(|scope| {
            let planner_thread = scope.spawn(|| {
                for (request_id, name, relation_id, base_column_id) in [
                    (257, "lefts", 0x31, 0x41),
                    (258, "rights", 0x32, 0x51),
                    (260, "lefts", 0x31, 0x41),
                    (261, "rights", 0x32, 0x51),
                ] {
                    let request = crate::ipc::read_frame(&mut planner_server)
                        .unwrap()
                        .expect("optimizer must resolve each join relation");
                    let decoded = DescribePlanRelationRequest::decode(&request.payload).unwrap();
                    assert_eq!(request.request_id, request_id);
                    assert_eq!(decoded.schema_name, "public");
                    assert_eq!(decoded.relation_name, name);
                    assert!(!decoded.include_scan_estimate);
                    assert_eq!(
                        decoded.population_estimate_max_rows,
                        Some(crate::optimizer_plan::JOIN_POPULATION_ESTIMATE_MAX_ROWS)
                    );
                    let mut range_access_paths = Vec::new();
                    if name == "rights" {
                        range_access_paths.push(
                            crate::metadata_discovery::AuthorizedRangeAccessPath {
                                index_id: [0x71; 16],
                                column_ids: vec![[base_column_id; 16]],
                            },
                        );
                    }
                    let relation = AuthorizedPlanRelation {
                        schema_id: [1; 16],
                        schema_version: 3,
                        table_epoch: u64::from(relation_id),
                        relation: AuthorizedRelationDescription {
                            relation_id: [relation_id; 16],
                            name: name.to_owned(),
                            columns: vec![
                                VisibleColumn {
                                    column_id: [base_column_id; 16],
                                    name: "id".to_owned(),
                                    ordinal: 0,
                                    scalar_type: ScalarType::Integer,
                                    nullable: false,
                                },
                                VisibleColumn {
                                    column_id: [base_column_id + 1; 16],
                                    name: "active".to_owned(),
                                    ordinal: 1,
                                    scalar_type: ScalarType::Boolean,
                                    nullable: false,
                                },
                                VisibleColumn {
                                    column_id: [base_column_id + 2; 16],
                                    name: "value".to_owned(),
                                    ordinal: 2,
                                    scalar_type: ScalarType::Text,
                                    nullable: true,
                                },
                            ],
                        },
                        unique_access_paths: Vec::new(),
                        range_access_paths,
                    };
                    crate::ipc::write_frame(
                        &mut planner_server,
                        &IpcFrame {
                            operation: request.operation,
                            request_id: request.request_id,
                            payload: DescribePlanRelationResponse {
                                relation: Some(relation),
                                proven_empty: false,
                                visible_rows_estimate: Some(if name == "lefts" {
                                    17
                                } else if request_id >= 260 {
                                    4097
                                } else {
                                    2048
                                }),
                            }
                            .encode()
                            .unwrap(),
                        },
                    )
                    .unwrap();
                    if name == "rights" {
                        let estimate = crate::ipc::read_frame(&mut planner_server)
                            .unwrap()
                            .expect("optimizer must request the bounded join fanout estimate");
                        assert_eq!(estimate.operation, Operation::EstimateScan as u16);
                        let estimate_request =
                            crate::planner_ipc::EstimateJoinRequest::decode(&estimate.payload)
                                .unwrap();
                        assert_eq!(estimate_request.schema_name, "public");
                        assert_eq!(estimate_request.left_relation_name, "lefts");
                        assert_eq!(estimate_request.right_relation_name, "rights");
                        assert_eq!(
                            estimate_request.maximum_rows,
                            crate::statistics::MAX_JOIN_ESTIMATE_ROWS
                        );
                        assert_eq!(
                            estimate_request.left_key_column_ids,
                            vec![[0x41; 16], [0x42; 16]]
                        );
                        assert_eq!(
                            estimate_request.right_key_column_ids,
                            vec![[0x51; 16], [0x52; 16]]
                        );
                        let estimate_response = if request_id >= 260 {
                            crate::planner_ipc::EstimateJoinResponse::Unknown
                        } else {
                            crate::planner_ipc::EstimateJoinResponse::Exact { joined_rows: 17 }
                        };
                        crate::ipc::write_frame(
                            &mut planner_server,
                            &IpcFrame {
                                operation: estimate.operation,
                                request_id: estimate.request_id,
                                payload: estimate_response.encode().unwrap(),
                            },
                        )
                        .unwrap();
                    }
                }
            });
            let optimizer_thread = scope
                .spawn(|| serve_optimizer_process(&mut submission_server, &mut planner_client));

            crate::ipc::write_frame(
                &mut submission_client,
                &IpcFrame {
                    operation: Operation::OptimizeJoinSelect as u16,
                    request_id: 12,
                    payload: join_select_request(),
                },
            )
            .unwrap();
            let response = crate::ipc::read_frame(&mut submission_client)
                .unwrap()
                .expect("optimizer must return a join plan");
            assert_eq!(response.operation, Operation::OptimizeJoinSelect as u16);
            assert_eq!(response.request_id, 12);
            let response = OptimizerResponse::decode(&response.payload).unwrap();
            assert_eq!(response.status, OptimizerStatus::Success);
            let plan = crate::optimizer_plan::decode_join_select_plan_v1(
                response.plan.as_deref().unwrap(),
            )
            .unwrap();
            assert_eq!(plan.left_relation.relation_name, "lefts");
            assert_eq!(plan.right_relation.relation_name, "rights");
            assert_eq!(plan.left_scan_columns.len(), 3);
            assert_eq!(plan.right_scan_columns.len(), 3);
            assert_eq!(plan.join_keys.len(), 2);
            assert_eq!(plan.limit, Some(5));
            assert!(
                plan.index_nested_loop.is_some(),
                "low fanout against an exact 2,048-row input should select indexed nested loop"
            );

            crate::ipc::write_frame(
                &mut submission_client,
                &IpcFrame {
                    operation: Operation::OptimizeJoinSelect as u16,
                    request_id: 13,
                    payload: filtered_join_select_request(),
                },
            )
            .unwrap();
            let response = crate::ipc::read_frame(&mut submission_client)
                .unwrap()
                .expect("optimizer must return a filtered join plan");
            assert_eq!(response.operation, Operation::OptimizeJoinSelect as u16);
            assert_eq!(response.request_id, 13);
            let response = OptimizerResponse::decode(&response.payload).unwrap();
            assert_eq!(response.status, OptimizerStatus::Success);
            let plan = crate::optimizer_plan::decode_join_select_plan_v1(
                response.plan.as_deref().unwrap(),
            )
            .unwrap();
            assert_eq!(plan.right_scan_columns.len(), 3);
            assert!(
                plan.index_nested_loop.is_some(),
                "a 17-row outer and 4,097-row indexed input should retain the measured indexed fallback when exact fanout is unavailable"
            );
            let filter = plan.filter.unwrap();
            assert_eq!(
                filter.relation_side,
                crate::optimizer_request::JoinRelationSide::Right
            );
            assert_eq!(filter.column.ordinal, 1);
            assert_eq!(
                filter.expression,
                crate::optimizer_request::JoinFilterExpression::Parameter {
                    operator: crate::optimizer_request::Comparison::Greater,
                    index: 3,
                }
            );
            submission_client.shutdown(Shutdown::Write).unwrap();
            planner_thread.join().unwrap();
            optimizer_thread.join().unwrap().unwrap();
        });
    }

    #[test]
    fn process_protocol_dispatches_group_count_and_returns_a_validated_plan() {
        let (mut submission_client, mut submission_server) = UnixStream::pair().unwrap();
        let (mut planner_client, mut planner_server) = UnixStream::pair().unwrap();
        std::thread::scope(|scope| {
            let planner_thread = scope.spawn(|| {
                let request = crate::ipc::read_frame(&mut planner_server)
                    .unwrap()
                    .expect("optimizer must resolve group relation metadata");
                assert_eq!(request.request_id, 257);
                let relation = AuthorizedPlanRelation {
                    schema_id: [1; 16],
                    schema_version: 3,
                    table_epoch: 7,
                    relation: AuthorizedRelationDescription {
                        relation_id: [2; 16],
                        name: "items".to_owned(),
                        columns: vec![VisibleColumn {
                            column_id: [4; 16],
                            name: "active".to_owned(),
                            ordinal: 0,
                            scalar_type: ScalarType::Boolean,
                            nullable: false,
                        }],
                    },
                    unique_access_paths: Vec::new(),
                    range_access_paths: Vec::new(),
                };
                crate::ipc::write_frame(
                    &mut planner_server,
                    &IpcFrame {
                        operation: request.operation,
                        request_id: request.request_id,
                        payload: DescribePlanRelationResponse {
                            relation: Some(relation),
                            proven_empty: false,
                            visible_rows_estimate: None,
                        }
                        .encode()
                        .unwrap(),
                    },
                )
                .unwrap();
            });
            let optimizer_thread = scope
                .spawn(|| serve_optimizer_process(&mut submission_server, &mut planner_client));
            submission_client
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            crate::ipc::write_frame(
                &mut submission_client,
                &IpcFrame {
                    operation: Operation::OptimizeGroupCount as u16,
                    request_id: 14,
                    payload: group_count_request(),
                },
            )
            .unwrap();
            let response = crate::ipc::read_frame(&mut submission_client);
            submission_client.shutdown(Shutdown::Write).unwrap();
            planner_thread.join().unwrap();
            optimizer_thread.join().unwrap().unwrap();
            let response = response
                .unwrap()
                .expect("optimizer must return a group-count plan");
            assert_eq!(response.request_id, 14);
            let response = OptimizerResponse::decode(&response.payload).unwrap();
            assert_eq!(response.status, OptimizerStatus::Success);
            let plan = crate::optimizer_plan::decode_group_count_plan_v1(
                response.plan.as_deref().unwrap(),
            )
            .unwrap();
            assert_eq!(plan.relation.relation_name, "items");
            assert_eq!(plan.group_key.column_id, [4; 16]);
            assert_eq!(plan.limit, Some(5));
        });
    }

    #[test]
    fn process_protocol_dispatches_ordered_group_count_and_returns_kind_eight() {
        let (mut submission_client, mut submission_server) = UnixStream::pair().unwrap();
        let (mut planner_client, mut planner_server) = UnixStream::pair().unwrap();
        std::thread::scope(|scope| {
            let planner_thread = scope.spawn(|| {
                let request = crate::ipc::read_frame(&mut planner_server)
                    .unwrap()
                    .expect("optimizer must resolve grouped relation metadata");
                let relation = AuthorizedPlanRelation {
                    schema_id: [1; 16],
                    schema_version: 3,
                    table_epoch: 7,
                    relation: AuthorizedRelationDescription {
                        relation_id: [2; 16],
                        name: "items".to_owned(),
                        columns: vec![VisibleColumn {
                            column_id: [4; 16],
                            name: "active".to_owned(),
                            ordinal: 0,
                            scalar_type: ScalarType::Boolean,
                            nullable: false,
                        }],
                    },
                    unique_access_paths: Vec::new(),
                    range_access_paths: Vec::new(),
                };
                crate::ipc::write_frame(
                    &mut planner_server,
                    &IpcFrame {
                        operation: request.operation,
                        request_id: request.request_id,
                        payload: DescribePlanRelationResponse {
                            relation: Some(relation),
                            proven_empty: false,
                            visible_rows_estimate: None,
                        }
                        .encode()
                        .unwrap(),
                    },
                )
                .unwrap();
            });
            let optimizer_thread = scope
                .spawn(|| serve_optimizer_process(&mut submission_server, &mut planner_client));
            crate::ipc::write_frame(
                &mut submission_client,
                &IpcFrame {
                    operation: Operation::OptimizeGroupCount as u16,
                    request_id: 15,
                    payload: ordered_group_count_request(),
                },
            )
            .unwrap();
            let response = crate::ipc::read_frame(&mut submission_client)
                .unwrap()
                .expect("optimizer must return an ordered group-count plan");
            let response = OptimizerResponse::decode(&response.payload).unwrap();
            assert_eq!(response.status, OptimizerStatus::Success);
            let plan = crate::optimizer_plan::decode_ordered_group_count_plan_v1(
                response.plan.as_deref().unwrap(),
            )
            .unwrap();
            assert_eq!(plan.relation.relation_name, "items");
            assert_eq!(plan.group_key.column_id, [4; 16]);
            assert_eq!(plan.order_items.len(), 2);
            assert_eq!(plan.limit, Some(5));
            submission_client.shutdown(Shutdown::Write).unwrap();
            planner_thread.join().unwrap();
            optimizer_thread.join().unwrap().unwrap();
        });
    }

    #[test]
    fn process_protocol_dispatches_ordered_group_aggregate_and_returns_kind_eleven() {
        let (mut submission_client, mut submission_server) = UnixStream::pair().unwrap();
        let (mut planner_client, mut planner_server) = UnixStream::pair().unwrap();
        std::thread::scope(|scope| {
            let planner_thread = scope.spawn(|| {
                let request = crate::ipc::read_frame(&mut planner_server)
                    .unwrap()
                    .expect("optimizer must resolve grouped aggregate metadata");
                let relation = AuthorizedPlanRelation {
                    schema_id: [1; 16],
                    schema_version: 3,
                    table_epoch: 7,
                    relation: AuthorizedRelationDescription {
                        relation_id: [2; 16],
                        name: "items".to_owned(),
                        columns: vec![
                            VisibleColumn {
                                column_id: [4; 16],
                                name: "id".to_owned(),
                                ordinal: 0,
                                scalar_type: ScalarType::Integer,
                                nullable: false,
                            },
                            VisibleColumn {
                                column_id: [5; 16],
                                name: "amount".to_owned(),
                                ordinal: 1,
                                scalar_type: ScalarType::BigInt,
                                nullable: false,
                            },
                        ],
                    },
                    unique_access_paths: Vec::new(),
                    range_access_paths: Vec::new(),
                };
                crate::ipc::write_frame(
                    &mut planner_server,
                    &IpcFrame {
                        operation: request.operation,
                        request_id: request.request_id,
                        payload: DescribePlanRelationResponse {
                            relation: Some(relation),
                            proven_empty: false,
                            visible_rows_estimate: None,
                        }
                        .encode()
                        .unwrap(),
                    },
                )
                .unwrap();
            });
            let optimizer_thread = scope
                .spawn(|| serve_optimizer_process(&mut submission_server, &mut planner_client));
            crate::ipc::write_frame(
                &mut submission_client,
                &IpcFrame {
                    operation: Operation::OptimizeGroupCount as u16,
                    request_id: 18,
                    payload: ordered_group_aggregate_request(),
                },
            )
            .unwrap();
            let response = crate::ipc::read_frame(&mut submission_client)
                .unwrap()
                .expect("optimizer must return a sorted aggregate plan");
            let response = OptimizerResponse::decode(&response.payload).unwrap();
            assert_eq!(response.status, OptimizerStatus::Success);
            let plan = crate::optimizer_plan::decode_ordered_group_aggregate_plan_v1(
                response.plan.as_deref().unwrap(),
            )
            .unwrap();
            assert_eq!(plan.aggregate_plan.relation.relation_name, "items");
            assert_eq!(plan.aggregate_plan.group_key.column_id, [4; 16]);
            assert_eq!(
                plan.aggregate_plan.aggregate_column.unwrap().column_id,
                [5; 16]
            );
            assert_eq!(plan.order_items.len(), 2);
            assert_eq!(plan.aggregate_plan.limit, Some(5));
            submission_client.shutdown(Shutdown::Write).unwrap();
            planner_thread.join().unwrap();
            optimizer_thread.join().unwrap().unwrap();
        });
    }

    #[test]
    fn process_protocol_dispatches_composite_group_count_and_returns_validated_plan() {
        let (mut submission_client, mut submission_server) = UnixStream::pair().unwrap();
        let (mut planner_client, mut planner_server) = UnixStream::pair().unwrap();
        std::thread::scope(|scope| {
            let planner_thread = scope.spawn(|| {
                let request = crate::ipc::read_frame(&mut planner_server)
                    .unwrap()
                    .expect("optimizer must resolve composite group relation");
                assert_eq!(request.request_id, 257);
                let relation = AuthorizedPlanRelation {
                    schema_id: [1; 16],
                    schema_version: 3,
                    table_epoch: 7,
                    relation: AuthorizedRelationDescription {
                        relation_id: [2; 16],
                        name: "items".to_owned(),
                        columns: vec![
                            VisibleColumn {
                                column_id: [4; 16],
                                name: "active".to_owned(),
                                ordinal: 0,
                                scalar_type: ScalarType::Boolean,
                                nullable: false,
                            },
                            VisibleColumn {
                                column_id: [5; 16],
                                name: "id".to_owned(),
                                ordinal: 1,
                                scalar_type: ScalarType::Integer,
                                nullable: false,
                            },
                        ],
                    },
                    unique_access_paths: Vec::new(),
                    range_access_paths: Vec::new(),
                };
                crate::ipc::write_frame(
                    &mut planner_server,
                    &IpcFrame {
                        operation: request.operation,
                        request_id: request.request_id,
                        payload: DescribePlanRelationResponse {
                            relation: Some(relation),
                            proven_empty: false,
                            visible_rows_estimate: None,
                        }
                        .encode()
                        .unwrap(),
                    },
                )
                .unwrap();
            });
            let optimizer_thread = scope
                .spawn(|| serve_optimizer_process(&mut submission_server, &mut planner_client));
            crate::ipc::write_frame(
                &mut submission_client,
                &IpcFrame {
                    operation: Operation::OptimizeGroupCount as u16,
                    request_id: 15,
                    payload: composite_group_count_request(),
                },
            )
            .unwrap();
            let response = crate::ipc::read_frame(&mut submission_client)
                .unwrap()
                .expect("optimizer must return a composite COUNT plan");
            assert_eq!(response.request_id, 15);
            let response = OptimizerResponse::decode(&response.payload).unwrap();
            assert_eq!(response.status, OptimizerStatus::Success);
            let plan = crate::optimizer_plan::decode_composite_group_count_plan_v1(
                response.plan.as_deref().unwrap(),
            )
            .unwrap();
            assert_eq!(plan.group_keys[0].ordinal, 0);
            assert_eq!(plan.group_keys[1].ordinal, 1);
            assert_eq!(plan.scan_columns[0].ordinal, 0);
            assert_eq!(plan.scan_columns[1].ordinal, 1);
            assert_eq!(plan.limit, Some(5));
            submission_client.shutdown(Shutdown::Write).unwrap();
            planner_thread.join().unwrap();
            optimizer_thread.join().unwrap().unwrap();
        });
    }

    #[test]
    fn process_protocol_dispatches_composite_aggregate_families_and_returns_validated_plans() {
        for kind in 1..=6 {
            assert!(
                crate::optimizer_request::decode_ordered_composite_group_aggregate_request(
                    &ordered_composite_group_aggregate_request(kind)
                )
                .is_ok()
            );
        }
        let (mut submission_client, mut submission_server) = UnixStream::pair().unwrap();
        let (mut planner_client, mut planner_server) = UnixStream::pair().unwrap();
        std::thread::scope(|scope| {
            let planner_thread = scope.spawn(|| {
                planner_server
                    .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                for expected_request_id in 257..=271 {
                    let request = crate::ipc::read_frame(&mut planner_server)
                        .unwrap()
                        .expect("optimizer must resolve composite aggregate relation");
                    assert_eq!(request.request_id, expected_request_id);
                    let relation = AuthorizedPlanRelation {
                        schema_id: [1; 16],
                        schema_version: 3,
                        table_epoch: 7,
                        relation: AuthorizedRelationDescription {
                            relation_id: [2; 16],
                            name: "items".to_owned(),
                            columns: vec![
                                VisibleColumn {
                                    column_id: [4; 16],
                                    name: "active".to_owned(),
                                    ordinal: 0,
                                    scalar_type: ScalarType::Boolean,
                                    nullable: false,
                                },
                                VisibleColumn {
                                    column_id: [5; 16],
                                    name: "id".to_owned(),
                                    ordinal: 1,
                                    scalar_type: ScalarType::Integer,
                                    nullable: false,
                                },
                                VisibleColumn {
                                    column_id: [6; 16],
                                    name: "amount".to_owned(),
                                    ordinal: 2,
                                    scalar_type: ScalarType::BigInt,
                                    nullable: true,
                                },
                            ],
                        },
                        unique_access_paths: Vec::new(),
                        range_access_paths: Vec::new(),
                    };
                    crate::ipc::write_frame(
                        &mut planner_server,
                        &IpcFrame {
                            operation: request.operation,
                            request_id: request.request_id,
                            payload: DescribePlanRelationResponse {
                                relation: Some(relation),
                                proven_empty: false,
                                visible_rows_estimate: None,
                            }
                            .encode()
                            .unwrap(),
                        },
                    )
                    .unwrap();
                }
            });
            let optimizer_thread = scope
                .spawn(|| serve_optimizer_process(&mut submission_server, &mut planner_client));
            submission_client
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            for kind in 1..=6 {
                crate::ipc::write_frame(
                    &mut submission_client,
                    &IpcFrame {
                        operation: Operation::OptimizeGroupCount as u16,
                        request_id: 15 + u64::from(kind),
                        payload: composite_group_aggregate_request(kind),
                    },
                )
                .unwrap();
                let response = crate::ipc::read_frame(&mut submission_client)
                    .unwrap()
                    .expect("optimizer must return a composite aggregate plan");
                assert_eq!(response.request_id, 15 + u64::from(kind));
                let response = OptimizerResponse::decode(&response.payload).unwrap();
                assert_eq!(response.status, OptimizerStatus::Success);
                let plan = crate::optimizer_plan::decode_composite_group_aggregate_plan_v1(
                    response.plan.as_deref().unwrap(),
                )
                .unwrap();
                assert_eq!(plan.aggregate_kind, kind);
                assert_eq!(plan.group_keys[0].ordinal, 0);
                assert_eq!(plan.group_keys[1].ordinal, 1);
                assert_eq!(
                    plan.aggregate_column.map(|column| column.ordinal),
                    (kind != 1).then_some(2)
                );
                assert_eq!(
                    plan.scan_columns
                        .iter()
                        .map(|column| column.ordinal)
                        .collect::<Vec<_>>(),
                    if kind == 1 { vec![0, 1] } else { vec![0, 1, 2] }
                );
                assert_eq!(plan.limit, Some(5));
            }
            crate::ipc::write_frame(
                &mut submission_client,
                &IpcFrame {
                    operation: Operation::OptimizeGroupCount as u16,
                    request_id: 36,
                    payload: filtered_composite_group_aggregate_request(),
                },
            )
            .unwrap();
            let response = crate::ipc::read_frame(&mut submission_client)
                .unwrap()
                .expect("optimizer must return a filtered composite aggregate plan");
            assert_eq!(response.request_id, 36);
            let response = OptimizerResponse::decode(&response.payload).unwrap();
            assert_eq!(response.status, OptimizerStatus::Success);
            let plan = crate::optimizer_plan::decode_filtered_composite_group_aggregate_plan_v1(
                response.plan.as_deref().unwrap(),
            )
            .unwrap();
            assert_eq!(plan.aggregate_kind, 4);
            assert_eq!(plan.group_keys[0].ordinal, 0);
            assert_eq!(plan.group_keys[1].ordinal, 1);
            assert_eq!(plan.scan_columns.len(), 3);
            assert_eq!(plan.limit, Some(5));
            crate::ipc::write_frame(
                &mut submission_client,
                &IpcFrame {
                    operation: Operation::OptimizeGroupCount as u16,
                    request_id: 37,
                    payload: filtered_single_group_aggregate_request(),
                },
            )
            .unwrap();
            let response = crate::ipc::read_frame(&mut submission_client)
                .unwrap()
                .expect("optimizer must return a filtered single-key aggregate plan");
            assert_eq!(response.request_id, 37);
            let response = OptimizerResponse::decode(&response.payload).unwrap();
            assert_eq!(response.status, OptimizerStatus::Success);
            let plan = crate::optimizer_plan::decode_filtered_composite_group_aggregate_plan_v1(
                response.plan.as_deref().unwrap(),
            )
            .unwrap();
            assert_eq!(plan.aggregate_kind, 4);
            assert_eq!(plan.group_keys.len(), 1);
            assert_eq!(plan.group_keys[0].ordinal, 0);
            assert_eq!(plan.scan_columns.len(), 2);
            assert_eq!(plan.limit, Some(5));
            crate::ipc::write_frame(
                &mut submission_client,
                &IpcFrame {
                    operation: Operation::OptimizeGroupCount as u16,
                    request_id: 38,
                    payload: ordered_filtered_composite_group_aggregate_request(),
                },
            )
            .unwrap();
            let response = crate::ipc::read_frame(&mut submission_client)
                .unwrap()
                .expect("optimizer must return an ordered filtered composite plan");
            assert_eq!(response.request_id, 38);
            let response = OptimizerResponse::decode(&response.payload).unwrap();
            assert_eq!(response.status, OptimizerStatus::Success);
            let plan = crate::optimizer_plan::decode_filtered_composite_group_aggregate_plan_v1(
                response.plan.as_deref().unwrap(),
            )
            .unwrap();
            assert_eq!(plan.order_items.len(), 2);
            assert_eq!(plan.order_items[0].group_key_index, Some(0));
            assert_eq!(plan.order_items[1].source, 1);
            assert_eq!(plan.limit, Some(5));
            for kind in 1..=6 {
                crate::ipc::write_frame(
                    &mut submission_client,
                    &IpcFrame {
                        operation: Operation::OptimizeGroupCount as u16,
                        request_id: 25 + u64::from(kind),
                        payload: ordered_composite_group_aggregate_request(kind),
                    },
                )
                .unwrap();
                let response = crate::ipc::read_frame(&mut submission_client)
                    .unwrap()
                    .expect("optimizer must return ordered composite aggregate plan");
                assert_eq!(response.request_id, 25 + u64::from(kind));
                let response = OptimizerResponse::decode(&response.payload).unwrap();
                assert_eq!(response.status, OptimizerStatus::Success);
                let plan = crate::optimizer_plan::decode_ordered_composite_group_aggregate_plan_v1(
                    response.plan.as_deref().unwrap(),
                )
                .unwrap();
                assert_eq!(plan.aggregate_plan.aggregate_kind, kind);
                assert_eq!(plan.aggregate_plan.group_keys[0].ordinal, 0);
                assert_eq!(plan.aggregate_plan.group_keys[1].ordinal, 1);
                assert_eq!(plan.order_items.len(), 2);
                assert_eq!(plan.order_items[0].source, 1);
                assert_eq!(plan.order_items[1].group_key_index, Some(0));
                assert_eq!(plan.aggregate_plan.limit, Some(5));
            }
            submission_client.shutdown(Shutdown::Write).unwrap();
            planner_thread.join().unwrap();
            optimizer_thread.join().unwrap().unwrap();
        });
    }

    #[test]
    fn process_protocol_accepts_all_eight_key_composite_aggregate_plans() {
        let (mut submission_client, mut submission_server) = UnixStream::pair().unwrap();
        let (mut planner_client, mut planner_server) = UnixStream::pair().unwrap();
        std::thread::scope(|scope| {
            let planner_thread = scope.spawn(|| {
                planner_server
                    .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                for expected_request_id in 257..=263 {
                    let request = crate::ipc::read_frame(&mut planner_server)
                        .unwrap()
                        .expect("optimizer must resolve eight-key aggregate relation");
                    assert_eq!(request.request_id, expected_request_id);
                    let columns = (0..9)
                        .map(|ordinal| VisibleColumn {
                            column_id: [u8::try_from(ordinal + 4).unwrap(); 16],
                            name: if ordinal == 8 {
                                "measure".to_owned()
                            } else {
                                format!("key_{ordinal}")
                            },
                            ordinal: u32::try_from(ordinal).unwrap(),
                            scalar_type: if ordinal == 8 {
                                ScalarType::BigInt
                            } else {
                                ScalarType::Integer
                            },
                            nullable: ordinal == 8,
                        })
                        .collect();
                    let relation = AuthorizedPlanRelation {
                        schema_id: [1; 16],
                        schema_version: 3,
                        table_epoch: 7,
                        relation: AuthorizedRelationDescription {
                            relation_id: [2; 16],
                            name: "items".to_owned(),
                            columns,
                        },
                        unique_access_paths: Vec::new(),
                        range_access_paths: Vec::new(),
                    };
                    crate::ipc::write_frame(
                        &mut planner_server,
                        &IpcFrame {
                            operation: request.operation,
                            request_id: request.request_id,
                            payload: DescribePlanRelationResponse {
                                relation: Some(relation),
                                proven_empty: false,
                                visible_rows_estimate: None,
                            }
                            .encode()
                            .unwrap(),
                        },
                    )
                    .unwrap();
                }
            });
            let optimizer_thread = scope
                .spawn(|| serve_optimizer_process(&mut submission_server, &mut planner_client));
            submission_client
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            for kind in 1..=6 {
                let request_id = 700 + u64::from(kind);
                crate::ipc::write_frame(
                    &mut submission_client,
                    &IpcFrame {
                        operation: Operation::OptimizeGroupCount as u16,
                        request_id,
                        payload: eight_key_group_aggregate_request(kind),
                    },
                )
                .unwrap();
                let response = crate::ipc::read_frame(&mut submission_client)
                    .unwrap()
                    .expect("optimizer must return an eight-key aggregate plan");
                assert_eq!(response.request_id, request_id);
                let response = OptimizerResponse::decode(&response.payload).unwrap();
                assert_eq!(response.status, OptimizerStatus::Success);
                let plan = crate::optimizer_plan::decode_composite_group_aggregate_plan_v1(
                    response.plan.as_deref().unwrap(),
                )
                .unwrap();
                assert_eq!(plan.aggregate_kind, kind);
                assert_eq!(
                    plan.group_keys
                        .iter()
                        .map(|key| key.ordinal)
                        .collect::<Vec<_>>(),
                    (0..8).collect::<Vec<_>>()
                );
                assert_eq!(
                    plan.scan_columns
                        .iter()
                        .map(|column| column.ordinal)
                        .collect::<Vec<_>>(),
                    if kind == 1 {
                        (0..8).collect::<Vec<_>>()
                    } else {
                        (0..9).collect::<Vec<_>>()
                    }
                );
                assert_eq!(
                    plan.aggregate_column.map(|column| column.ordinal),
                    (kind != 1).then_some(8)
                );
                assert_eq!(plan.limit, Some(5));
            }
            crate::ipc::write_frame(
                &mut submission_client,
                &IpcFrame {
                    operation: Operation::OptimizeGroupCount as u16,
                    request_id: 707,
                    payload: eight_key_ordered_group_aggregate_request(),
                },
            )
            .unwrap();
            let response = crate::ipc::read_frame(&mut submission_client)
                .unwrap()
                .expect("optimizer must return an ordered eight-key aggregate plan");
            assert_eq!(response.request_id, 707);
            let response = OptimizerResponse::decode(&response.payload).unwrap();
            assert_eq!(response.status, OptimizerStatus::Success);
            let plan = crate::optimizer_plan::decode_ordered_composite_group_aggregate_plan_v1(
                response.plan.as_deref().unwrap(),
            )
            .unwrap();
            assert_eq!(plan.aggregate_plan.aggregate_kind, 4);
            assert_eq!(
                plan.aggregate_plan
                    .group_keys
                    .iter()
                    .map(|key| key.ordinal)
                    .collect::<Vec<_>>(),
                (0..8).collect::<Vec<_>>()
            );
            assert_eq!(
                plan.aggregate_plan
                    .scan_columns
                    .iter()
                    .map(|column| column.ordinal)
                    .collect::<Vec<_>>(),
                (0..9).collect::<Vec<_>>()
            );
            assert_eq!(
                plan.aggregate_plan
                    .aggregate_column
                    .map(|column| column.ordinal),
                Some(8)
            );
            assert_eq!(plan.order_items.len(), 9);
            for (index, item) in plan.order_items[..8].iter().enumerate() {
                assert_eq!(item.source, 0);
                assert_eq!(item.group_key_index, Some(index as u8));
                assert!(item.descending);
                assert!(!item.nulls_first);
            }
            assert_eq!(plan.order_items[8].source, 1);
            assert_eq!(plan.order_items[8].group_key_index, None);
            assert!(plan.order_items[8].descending);
            assert!(!plan.order_items[8].nulls_first);
            assert_eq!(plan.aggregate_plan.limit, Some(5));
            submission_client.shutdown(Shutdown::Write).unwrap();
            planner_thread.join().unwrap();
            optimizer_thread.join().unwrap().unwrap();
        });
    }

    #[test]
    fn process_protocol_rejects_malformed_frames() {
        let (mut submission_client, mut submission_server) = UnixStream::pair().unwrap();
        let (mut planner_client, _planner_server) = UnixStream::pair().unwrap();
        let optimizer_thread = std::thread::spawn(move || {
            serve_optimizer_process(&mut submission_server, &mut planner_client)
        });

        submission_client.write_all(&[0; 28]).unwrap();
        submission_client.shutdown(Shutdown::Write).unwrap();
        assert!(matches!(
            optimizer_thread.join().unwrap(),
            Err(OptimizerProcessError::Frame(_))
        ));
    }

    #[test]
    fn process_protocol_rejects_duplicate_request_ids_after_invalid_request() {
        let (mut submission_client, mut submission_server) = UnixStream::pair().unwrap();
        let (mut planner_client, _planner_server) = UnixStream::pair().unwrap();
        let optimizer_thread = std::thread::spawn(move || {
            serve_optimizer_process(&mut submission_server, &mut planner_client)
        });
        let invalid_request = IpcFrame {
            operation: Operation::OptimizeSelect as u16,
            request_id: 41,
            payload: Vec::new(),
        };
        crate::ipc::write_frame(&mut submission_client, &invalid_request).unwrap();
        let invalid_response = crate::ipc::read_frame(&mut submission_client)
            .unwrap()
            .expect("invalid request receives a response");
        assert_eq!(invalid_response.request_id, invalid_request.request_id);
        assert_eq!(
            OptimizerResponse::decode(&invalid_response.payload)
                .unwrap()
                .status,
            OptimizerStatus::InvalidRequest
        );

        crate::ipc::write_frame(&mut submission_client, &invalid_request).unwrap();
        submission_client.shutdown(Shutdown::Write).unwrap();
        assert!(matches!(
            optimizer_thread.join().unwrap(),
            Err(OptimizerProcessError::DuplicateRequestId)
        ));
    }

    #[test]
    fn process_protocol_enforces_connection_request_limit() {
        let (mut submission_client, mut submission_server) = UnixStream::pair().unwrap();
        let (mut planner_client, _planner_server) = UnixStream::pair().unwrap();
        let optimizer_thread = std::thread::spawn(move || {
            serve_optimizer_process(&mut submission_server, &mut planner_client)
        });

        for request_id in 1..=MAX_OPTIMIZER_REQUESTS_PER_CONNECTION as u64 {
            crate::ipc::write_frame(
                &mut submission_client,
                &IpcFrame {
                    operation: Operation::OptimizeSelect as u16,
                    request_id,
                    payload: Vec::new(),
                },
            )
            .unwrap();
            let response = crate::ipc::read_frame(&mut submission_client)
                .unwrap()
                .expect("invalid request remains recoverable within the stream budget");
            assert_eq!(response.request_id, request_id);
            assert_eq!(
                OptimizerResponse::decode(&response.payload).unwrap().status,
                OptimizerStatus::InvalidRequest
            );
        }

        crate::ipc::write_frame(
            &mut submission_client,
            &IpcFrame {
                operation: Operation::OptimizeSelect as u16,
                request_id: MAX_OPTIMIZER_REQUESTS_PER_CONNECTION as u64 + 1,
                payload: Vec::new(),
            },
        )
        .unwrap();
        submission_client.shutdown(Shutdown::Write).unwrap();
        assert!(matches!(
            optimizer_thread.join().unwrap(),
            Err(OptimizerProcessError::RequestLimitExceeded)
        ));
    }
}
