//! Protocol loop used by the isolated optimizer executable.

use crate::ipc::{DispatchError, IpcFrame, Operation, ServiceRole, authorize_operation};
use crate::optimizer_ipc::{
    MAX_OPTIMIZER_REQUESTS_PER_CONNECTION, OptimizerPayloadError, OptimizerResponse,
    OptimizerStatus, status_for_error,
};
use crate::optimizer_ir::physical::MAX_UNIFIED_PLAN_BYTES;
use crate::planner_ipc::{
    DescribePlanRelationsRequest, DescribePlanRelationsResponse, EstimateScanRequest,
    EstimateScanResponse, EstimateScansRequest, EstimateScansResponse,
    MAX_PLANNER_REQUESTS_PER_CONNECTION, PlannerRequestError,
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

        let is_unified_request =
            request.payload.get(2) == Some(&crate::query_abi::OPTIMIZER_UNIFIED_REQUEST_KIND);
        if is_unified_request {
            let decoded_req = match crate::optimizer_ir::decode_logical_request(&request.payload) {
                Ok(req) => req,
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
            let rel_names = crate::optimizer_ir::collect_scan_relations(&decoded_req);
            let planner_request_id = take_planner_request_id(&mut next_planner_request_id)?;
            let planner_request = IpcFrame {
                operation: Operation::DescribePlanRelations as u16,
                request_id: planner_request_id,
                payload: DescribePlanRelationsRequest {
                    relations: rel_names.clone(),
                }
                .encode()
                .map_err(OptimizerProcessError::PlannerPayload)?,
            };
            let planner_response = planner_roundtrip(planner, planner_request)?;
            let described = DescribePlanRelationsResponse::decode(&planner_response.payload)
                .map_err(OptimizerProcessError::PlannerPayload)?;
            if described.entries.len() != rel_names.len() {
                return Err(OptimizerProcessError::PlannerResponseMismatch);
            }
            let mut stats_catalog = crate::optimizer_cost::StatsCatalog::default();
            let mut estimate_entries = Vec::new();
            let mut estimate_positions = Vec::new();
            let mut auth_relations = Vec::with_capacity(described.entries.len());
            for (idx, entry) in described.entries.into_iter().enumerate() {
                let rel = entry.response.relation;
                if rel.is_some() != entry.handle.is_some() {
                    return Err(OptimizerProcessError::PlannerResponseMismatch);
                }
                if let Some(handle) = entry.handle {
                    let col_ids = rel
                        .as_ref()
                        .and_then(|r| r.relation.columns.first().map(|c| vec![c.column_id]))
                        .unwrap_or_default();
                    estimate_positions.push(idx as u16);
                    estimate_entries.push(EstimateScanRequest {
                        handle,
                        column_ids: col_ids,
                    });
                }
                auth_relations.push(rel);
            }
            if !estimate_entries.is_empty() {
                let est_req_id = take_planner_request_id(&mut next_planner_request_id)?;
                let est_req = IpcFrame {
                    operation: Operation::EstimateScans as u16,
                    request_id: est_req_id,
                    payload: EstimateScansRequest {
                        entries: estimate_entries,
                    }
                    .encode()
                    .map_err(OptimizerProcessError::PlannerPayload)?,
                };
                let est_resp = planner_roundtrip(planner, est_req)?;
                let estimates = EstimateScansResponse::decode(&est_resp.payload)
                    .map_err(OptimizerProcessError::PlannerPayload)?;
                if estimates.entries.len() == estimate_positions.len() {
                    for (rel_idx, est) in estimate_positions.into_iter().zip(estimates.entries) {
                        if let EstimateScanResponse::Exact(summary) = est {
                            stats_catalog.relations.insert(
                                rel_idx,
                                crate::statistics::AuthorizedRelationStats {
                                    visible_rows: summary.visible_rows,
                                    is_exact: true,
                                    avg_authorized_row_bytes: 0,
                                    columns: Vec::new(),
                                },
                            );
                        }
                    }
                }
            }
            let response = match crate::optimizer_service::optimize_unified_v1_with_relations(
                &request.payload,
                &auth_relations,
                &stats_catalog,
                &crate::optimizer_cost::CostWeights::default(),
            ) {
                Ok(plan) if plan.len() <= MAX_UNIFIED_PLAN_BYTES => {
                    OptimizerResponse::success(plan)
                }
                Ok(_) => OptimizerResponse::failure(OptimizerStatus::Internal),
                Err(err) => OptimizerResponse::failure(status_for_error(&err)),
            }
            .map_err(OptimizerProcessError::Response)?;
            write_submission_response(submission, request.operation, request.request_id, response)?;
            continue;
        }
        write_submission_response(
            submission,
            request.operation,
            request.request_id,
            OptimizerResponse::failure(OptimizerStatus::InvalidRequest)
                .map_err(OptimizerProcessError::Response)?,
        )?;
    }
}

fn take_planner_request_id(next_request_id: &mut u64) -> Result<u64, OptimizerProcessError> {
    let request_id = *next_request_id;
    *next_request_id = next_request_id
        .checked_add(1)
        .ok_or(OptimizerProcessError::RequestLimitExceeded)?;
    Ok(request_id)
}

fn planner_roundtrip<P: Read + Write>(
    planner: &mut P,
    request: IpcFrame,
) -> Result<IpcFrame, OptimizerProcessError> {
    let operation =
        Operation::from_code(request.operation).map_err(OptimizerProcessError::Dispatch)?;
    authorize_operation(ServiceRole::Planner, operation)
        .map_err(OptimizerProcessError::Dispatch)?;
    crate::ipc::write_frame(planner, &request).map_err(OptimizerProcessError::Frame)?;
    let response = crate::ipc::read_frame(planner)
        .map_err(OptimizerProcessError::Frame)?
        .ok_or(OptimizerProcessError::PlannerClosed)?;
    if response.operation != request.operation || response.request_id != request.request_id {
        return Err(OptimizerProcessError::PlannerResponseMismatch);
    }
    Ok(response)
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
    use std::net::Shutdown;
    use std::os::unix::net::UnixStream;

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

    #[test]
    fn unified_request_roundtrips_through_optimizer_process_with_estimates() {
        use crate::optimizer_ir::{
            Comparison, Expr, LogicalOp, LogicalRequest, PhysicalOp, decode_physical_plan,
            encode_logical_request, expr::ColumnRef,
        };
        use crate::planner_ipc::{
            DescribePlanRelationBatchEntry, DescribePlanRelationResponse,
            DescribePlanRelationsRequest, DescribePlanRelationsResponse, EstimateScanResponse,
            EstimateScansRequest, EstimateScansResponse,
        };
        use crate::statistics::{AuthorizedScanSummary, ColumnPopulation};
        use std::net::Shutdown;
        use std::os::unix::net::UnixStream;

        let (mut submission_client, mut submission_server) = UnixStream::pair().unwrap();
        let (mut planner_client, mut planner_server) = UnixStream::pair().unwrap();

        let optimizer_thread = std::thread::spawn(move || {
            serve_optimizer_process(&mut submission_server, &mut planner_client)
        });

        let req = LogicalRequest {
            schema_name: "app".to_owned(),
            root: LogicalOp::Filter {
                predicate: Expr::Compare {
                    op: Comparison::Equal,
                    left: Box::new(Expr::Column(ColumnRef {
                        relation_index: 0,
                        column_name: "id".to_owned(),
                    })),
                    right: Box::new(Expr::Integer(7)),
                },
                input: Box::new(LogicalOp::Scan {
                    relation_index: 0,
                    relation_name: "users".to_owned(),
                }),
            },
        };
        let payload = encode_logical_request(&req).unwrap();

        crate::ipc::write_frame(
            &mut submission_client,
            &IpcFrame {
                operation: Operation::OptimizeSelect as u16,
                request_id: 1,
                payload,
            },
        )
        .unwrap();

        let desc_frame = crate::ipc::read_frame(&mut planner_server)
            .unwrap()
            .unwrap();
        assert_eq!(
            desc_frame.operation,
            Operation::DescribePlanRelations as u16
        );
        let desc_req = DescribePlanRelationsRequest::decode(&desc_frame.payload).unwrap();
        assert_eq!(desc_req.relations.len(), 1);
        let rel = crate::metadata_discovery::AuthorizedPlanRelation {
            schema_id: [1; 16],
            schema_version: 1,
            table_epoch: 1,
            relation: crate::metadata_discovery::AuthorizedRelationDescription {
                relation_id: [2; 16],
                name: "users".to_owned(),
                columns: vec![crate::metadata_discovery::VisibleColumn {
                    column_id: [3; 16],
                    name: "id".to_owned(),
                    ordinal: 0,
                    scalar_type: crate::tuple::ScalarType::Integer,
                    nullable: false,
                }],
            },
            unique_access_paths: vec![crate::metadata_discovery::AuthorizedUniqueAccessPath {
                index_id: [4; 16],
                column_ids: vec![[3; 16]],
            }],
            range_access_paths: vec![],
        };
        let mut token = [0x55; 16];
        token[0] = crate::metadata_discovery::PLANNING_TABLE_HANDLE_TOKEN_KIND;
        let desc_resp = DescribePlanRelationsResponse {
            entries: vec![DescribePlanRelationBatchEntry {
                handle: Some(crate::metadata_discovery::PlanningTableHandle { token }),
                response: DescribePlanRelationResponse {
                    relation: Some(rel),
                    proven_empty: false,
                    visible_rows_estimate: None,
                },
            }],
        }
        .encode()
        .unwrap();
        crate::ipc::write_frame(
            &mut planner_server,
            &IpcFrame {
                operation: desc_frame.operation,
                request_id: desc_frame.request_id,
                payload: desc_resp,
            },
        )
        .unwrap();

        let est_frame = crate::ipc::read_frame(&mut planner_server)
            .unwrap()
            .unwrap();
        assert_eq!(est_frame.operation, Operation::EstimateScans as u16);
        let _est_req = EstimateScansRequest::decode(&est_frame.payload).unwrap();
        let est_resp = EstimateScansResponse {
            entries: vec![EstimateScanResponse::Exact(AuthorizedScanSummary {
                visible_rows: 1000,
                columns: vec![ColumnPopulation {
                    readable_values: 1000,
                    nulls: 0,
                    redacted: 0,
                }],
            })],
        }
        .encode()
        .unwrap();
        crate::ipc::write_frame(
            &mut planner_server,
            &IpcFrame {
                operation: est_frame.operation,
                request_id: est_frame.request_id,
                payload: est_resp,
            },
        )
        .unwrap();

        let Some(resp_frame) = crate::ipc::read_frame(&mut submission_client).unwrap() else {
            optimizer_thread.join().unwrap().unwrap();
            panic!("optimizer closed without response");
        };
        let resp = OptimizerResponse::decode(&resp_frame.payload).unwrap();
        assert_eq!(resp.status, OptimizerStatus::Success);
        let plan = decode_physical_plan(resp.plan.as_deref().unwrap()).unwrap();
        assert!(matches!(
            plan.root,
            PhysicalOp::IndexPointScan { .. } | PhysicalOp::Filter { .. }
        ));

        submission_client.shutdown(Shutdown::Write).unwrap();
        optimizer_thread.join().unwrap().unwrap();
    }
}
