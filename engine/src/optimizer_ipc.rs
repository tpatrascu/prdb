//! Versioned request/response payloads for executor-to-optimizer submission.

use crate::ipc::{DispatchError, IpcFrame, Operation, ServiceRole, authorize_operation};
use crate::optimizer_binding::{BindError, MetadataBindError};
use crate::optimizer_plan::{MAX_PLAN_BYTES, decode_plan_v1};
use crate::optimizer_request::{RequestError, SelectRequest, decode_select_request};
use crate::optimizer_service::OptimizerError;
use crate::transaction::{InvocationTransaction, TransactionError};
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

pub const OPTIMIZER_RESPONSE_VERSION: u16 = 1;
pub const OPTIMIZER_RESPONSE_HEADER_BYTES: usize = 3;
pub const MAX_OPTIMIZER_REQUESTS_PER_CONNECTION: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum OptimizerStatus {
    Success = 0,
    InvalidRequest = 1,
    Unavailable = 2,
    Rejected = 3,
    Internal = 4,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptimizerResponse {
    pub status: OptimizerStatus,
    pub plan: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OptimizerPayloadError {
    InvalidRequest(RequestError),
    InvalidResponse,
    PlanTooLarge,
    InvalidPlan,
}

#[derive(Debug)]
pub enum OptimizerConnectionError {
    Frame(crate::ipc::FrameError),
    Dispatch(DispatchError),
    Abort(TransactionError),
    Response(OptimizerPayloadError),
    RequestLimitExceeded,
    DuplicateRequestId,
    SharedInvocationUnavailable,
    SharedInvocationPoisoned,
}

impl std::fmt::Display for OptimizerPayloadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "optimizer IPC payload error: {self:?}")
    }
}

impl std::error::Error for OptimizerPayloadError {}

/// Validate and own a submitted request before it reaches planning code.
pub fn decode_optimizer_request(bytes: &[u8]) -> Result<SelectRequest, OptimizerPayloadError> {
    decode_select_request(bytes).map_err(OptimizerPayloadError::InvalidRequest)
}

impl OptimizerResponse {
    pub fn success(plan: Vec<u8>) -> Result<Self, OptimizerPayloadError> {
        if plan.is_empty() || plan.len() > MAX_PLAN_BYTES {
            return Err(OptimizerPayloadError::PlanTooLarge);
        }
        decode_plan_v1(&plan).map_err(|_| OptimizerPayloadError::InvalidPlan)?;
        Ok(Self {
            status: OptimizerStatus::Success,
            plan: Some(plan),
        })
    }

    pub fn failure(status: OptimizerStatus) -> Result<Self, OptimizerPayloadError> {
        if status == OptimizerStatus::Success {
            return Err(OptimizerPayloadError::InvalidResponse);
        }
        Ok(Self { status, plan: None })
    }

    pub fn encode(&self) -> Result<Vec<u8>, OptimizerPayloadError> {
        let mut output = Vec::with_capacity(OPTIMIZER_RESPONSE_HEADER_BYTES + 4 + 256);
        output.extend_from_slice(&OPTIMIZER_RESPONSE_VERSION.to_be_bytes());
        output.push(self.status as u8);
        match (&self.status, &self.plan) {
            (OptimizerStatus::Success, Some(plan)) => {
                if plan.is_empty() || plan.len() > MAX_PLAN_BYTES {
                    return Err(OptimizerPayloadError::PlanTooLarge);
                }
                decode_plan_v1(plan).map_err(|_| OptimizerPayloadError::InvalidPlan)?;
                output.extend_from_slice(
                    &u32::try_from(plan.len())
                        .map_err(|_| OptimizerPayloadError::PlanTooLarge)?
                        .to_be_bytes(),
                );
                output.extend_from_slice(plan);
            }
            (OptimizerStatus::Success, None) | (_, Some(_)) => {
                return Err(OptimizerPayloadError::InvalidResponse);
            }
            (_, None) => {}
        }
        Ok(output)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, OptimizerPayloadError> {
        if bytes.len() < OPTIMIZER_RESPONSE_HEADER_BYTES
            || u16::from_be_bytes([bytes[0], bytes[1]]) != OPTIMIZER_RESPONSE_VERSION
        {
            return Err(OptimizerPayloadError::InvalidResponse);
        }
        let status = match bytes[2] {
            0 => OptimizerStatus::Success,
            1 => OptimizerStatus::InvalidRequest,
            2 => OptimizerStatus::Unavailable,
            3 => OptimizerStatus::Rejected,
            4 => OptimizerStatus::Internal,
            _ => return Err(OptimizerPayloadError::InvalidResponse),
        };
        if status != OptimizerStatus::Success {
            if bytes.len() != OPTIMIZER_RESPONSE_HEADER_BYTES {
                return Err(OptimizerPayloadError::InvalidResponse);
            }
            return Ok(Self { status, plan: None });
        }
        if bytes.len() < OPTIMIZER_RESPONSE_HEADER_BYTES + 4 {
            return Err(OptimizerPayloadError::InvalidResponse);
        }
        let len = u32::from_be_bytes(
            bytes[3..7]
                .try_into()
                .map_err(|_| OptimizerPayloadError::InvalidResponse)?,
        ) as usize;
        if len == 0 || len > MAX_PLAN_BYTES || bytes.len() != 7 + len {
            return Err(OptimizerPayloadError::InvalidResponse);
        }
        let plan = bytes[7..].to_vec();
        decode_plan_v1(&plan).map_err(|_| OptimizerPayloadError::InvalidPlan)?;
        Ok(Self {
            status,
            plan: Some(plan),
        })
    }
}

/// Collapse planner failures to a small non-disclosing wire status set.
pub fn status_for_error(error: &OptimizerError) -> OptimizerStatus {
    match error {
        OptimizerError::Bind(MetadataBindError::Bind(BindError::Request(_))) => {
            OptimizerStatus::InvalidRequest
        }
        OptimizerError::Bind(MetadataBindError::Bind(BindError::RelationUnavailable)) => {
            OptimizerStatus::Unavailable
        }
        OptimizerError::Bind(MetadataBindError::Bind(BindError::InvalidQuery))
        | OptimizerError::Plan(_) => OptimizerStatus::Rejected,
        OptimizerError::Bind(MetadataBindError::Bind(BindError::InvalidMetadata))
        | OptimizerError::Bind(MetadataBindError::Transaction(_)) => OptimizerStatus::Internal,
    }
}

/// Serve executor-to-optimizer submissions against one invocation. Metadata
/// authority comes only from the invocation's planner connection. The
/// optimizer submission role cannot issue planner estimates or storage scans.
pub fn serve_optimizer_shared<S: Read + Write>(
    stream: &mut S,
    invocation: &Arc<Mutex<Option<InvocationTransaction<'_>>>>,
) -> Result<(), OptimizerConnectionError> {
    let mut state = crate::transaction::PlannerMetadataSessionState::default();
    let mut request_ids = std::collections::HashSet::new();
    loop {
        let request = match crate::ipc::read_frame(stream) {
            Ok(Some(request)) => request,
            Ok(None) => return Ok(()),
            Err(error) => {
                abort_shared_invocation(invocation)?;
                return Err(OptimizerConnectionError::Frame(error));
            }
        };
        if request_ids.len() >= MAX_OPTIMIZER_REQUESTS_PER_CONNECTION {
            abort_shared_invocation(invocation)?;
            return Err(OptimizerConnectionError::RequestLimitExceeded);
        }
        if !request_ids.insert(request.request_id) {
            abort_shared_invocation(invocation)?;
            return Err(OptimizerConnectionError::DuplicateRequestId);
        }
        let operation = match Operation::from_code(request.operation) {
            Ok(operation) => operation,
            Err(error) => {
                abort_shared_invocation(invocation)?;
                return Err(OptimizerConnectionError::Dispatch(error));
            }
        };
        if let Err(error) = authorize_operation(ServiceRole::OptimizerSubmission, operation) {
            abort_shared_invocation(invocation)?;
            return Err(OptimizerConnectionError::Dispatch(error));
        }
        let result = {
            let mut invocation = match invocation.lock() {
                Ok(invocation) => invocation,
                Err(poisoned) => {
                    let mut invocation = poisoned.into_inner();
                    if let Some(mut transaction) = invocation.take() {
                        let _ = transaction.abort();
                    }
                    return Err(OptimizerConnectionError::SharedInvocationPoisoned);
                }
            };
            let transaction = invocation
                .as_mut()
                .ok_or(OptimizerConnectionError::SharedInvocationUnavailable)?;
            let mut connection =
                transaction.open_planner_metadata_connection_with_state(std::mem::take(&mut state));
            let result =
                crate::optimizer_service::optimize_select_v1(&request.payload, &mut connection);
            state = connection.detach();
            result
        };
        let response = match result {
            Ok(plan) => OptimizerResponse::success(plan),
            Err(error @ OptimizerError::Bind(MetadataBindError::Transaction(_))) => {
                abort_shared_invocation(invocation)?;
                let OptimizerError::Bind(MetadataBindError::Transaction(transaction_error)) = error
                else {
                    unreachable!()
                };
                return Err(OptimizerConnectionError::Abort(transaction_error));
            }
            Err(error) => OptimizerResponse::failure(status_for_error(&error)),
        }
        .and_then(|response| response.encode())
        .map(|payload| IpcFrame {
            operation: request.operation,
            request_id: request.request_id,
            payload,
        });
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                abort_shared_invocation(invocation)?;
                return Err(OptimizerConnectionError::Response(error));
            }
        };
        if let Err(error) = crate::ipc::write_frame(stream, &response) {
            abort_shared_invocation(invocation)?;
            return Err(OptimizerConnectionError::Frame(error));
        }
    }
}

fn abort_shared_invocation(
    invocation: &Arc<Mutex<Option<InvocationTransaction<'_>>>>,
) -> Result<(), OptimizerConnectionError> {
    let mut invocation = match invocation.lock() {
        Ok(invocation) => invocation,
        Err(poisoned) => {
            let mut invocation = poisoned.into_inner();
            if let Some(mut transaction) = invocation.take() {
                let _ = transaction.abort();
            }
            return Err(OptimizerConnectionError::SharedInvocationPoisoned);
        }
    };
    if let Some(transaction) = invocation.as_mut() {
        transaction
            .abort()
            .map_err(OptimizerConnectionError::Abort)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer_binding::{BoundColumn, BoundSelect};
    use crate::optimizer_plan::{encode_plan_v1, plan_select_v1};
    use crate::tuple::ScalarType;

    fn plan_bytes() -> Vec<u8> {
        let bound = BoundSelect {
            schema_name: "public".to_owned(),
            schema_id: [1; 16],
            schema_version: 1,
            table_epoch: 1,
            relation_id: [2; 16],
            relation_name: "items".to_owned(),
            projections: vec![BoundColumn {
                column_id: [3; 16],
                ordinal: 0,
                scalar_type: ScalarType::Integer,
                nullable: true,
            }],
            predicate: None,
            limit: None,
        };
        encode_plan_v1(&plan_select_v1(bound).unwrap()).unwrap()
    }

    #[test]
    fn optimizer_responses_roundtrip_only_validated_plans() {
        let plan = plan_bytes();
        let response = OptimizerResponse::success(plan.clone()).unwrap();
        assert_eq!(
            OptimizerResponse::decode(&response.encode().unwrap()),
            Ok(response)
        );
        let failure = OptimizerResponse::failure(OptimizerStatus::Unavailable).unwrap();
        assert_eq!(
            OptimizerResponse::decode(&failure.encode().unwrap()),
            Ok(failure)
        );
    }

    #[test]
    fn optimizer_responses_reject_malformed_status_lengths_and_plans() {
        assert_eq!(
            OptimizerResponse::decode(&[0, 1, 0, 0, 0, 0, 1, 0]),
            Err(OptimizerPayloadError::InvalidPlan)
        );
        assert_eq!(
            OptimizerResponse::decode(&[0, 1, 9]),
            Err(OptimizerPayloadError::InvalidResponse)
        );
        assert_eq!(
            OptimizerResponse::decode(&[0, 1, 2, 0]),
            Err(OptimizerPayloadError::InvalidResponse)
        );
        let mut trailing = OptimizerResponse::success(plan_bytes())
            .unwrap()
            .encode()
            .unwrap();
        trailing.push(0);
        assert_eq!(
            OptimizerResponse::decode(&trailing),
            Err(OptimizerPayloadError::InvalidResponse)
        );
    }
}
