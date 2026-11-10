//! Versioned request/response payloads for executor-to-optimizer submission.

use crate::optimizer_binding::{BindError, MetadataBindError};
use crate::optimizer_plan::{MAX_PLAN_BYTES, decode_plan_v1};
use crate::optimizer_request::{RequestError, SelectRequest, decode_select_request};
use crate::optimizer_service::OptimizerError;
pub use crate::query_abi::{
    OPTIMIZER_RESPONSE_FAILURE_HEADER_BYTES, OPTIMIZER_RESPONSE_FAILURE_STATUS_OFFSET,
    OPTIMIZER_RESPONSE_FAILURE_VERSION_OFFSET, OPTIMIZER_RESPONSE_SUCCESS_HEADER_BYTES,
    OPTIMIZER_RESPONSE_SUCCESS_PLAN_LENGTH_OFFSET, OPTIMIZER_RESPONSE_SUCCESS_STATUS_OFFSET,
    OPTIMIZER_RESPONSE_SUCCESS_VERSION_OFFSET, OPTIMIZER_RESPONSE_VERSION,
};

pub const OPTIMIZER_RESPONSE_HEADER_BYTES: usize = OPTIMIZER_RESPONSE_FAILURE_HEADER_BYTES as usize;
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
        match (&self.status, &self.plan) {
            (OptimizerStatus::Success, Some(plan)) => {
                if plan.is_empty() || plan.len() > MAX_PLAN_BYTES {
                    return Err(OptimizerPayloadError::PlanTooLarge);
                }
                decode_plan_v1(plan).map_err(|_| OptimizerPayloadError::InvalidPlan)?;
                let plan_length =
                    u32::try_from(plan.len()).map_err(|_| OptimizerPayloadError::PlanTooLarge)?;
                let mut output = crate::query_abi::encode_optimizer_response_success(
                    OptimizerStatus::Success as u8,
                    plan_length,
                )
                .to_vec();
                output.extend_from_slice(plan);
                Ok(output)
            }
            (OptimizerStatus::Success, None) | (_, Some(_)) => {
                Err(OptimizerPayloadError::InvalidResponse)
            }
            (_, None) => {
                Ok(crate::query_abi::encode_optimizer_response_failure(self.status as u8).to_vec())
            }
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, OptimizerPayloadError> {
        if bytes.len() < OPTIMIZER_RESPONSE_HEADER_BYTES
            || u16::from_be_bytes([
                bytes[OPTIMIZER_RESPONSE_FAILURE_VERSION_OFFSET as usize],
                bytes[OPTIMIZER_RESPONSE_FAILURE_VERSION_OFFSET as usize + 1],
            ]) != OPTIMIZER_RESPONSE_VERSION
        {
            return Err(OptimizerPayloadError::InvalidResponse);
        }
        let status = match bytes[OPTIMIZER_RESPONSE_FAILURE_STATUS_OFFSET as usize] {
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
        if bytes.len() < OPTIMIZER_RESPONSE_SUCCESS_HEADER_BYTES as usize {
            return Err(OptimizerPayloadError::InvalidResponse);
        }
        let (success_status, encoded_plan_length) =
            crate::query_abi::decode_optimizer_response_success(bytes)
                .ok_or(OptimizerPayloadError::InvalidResponse)?;
        if success_status != OptimizerStatus::Success as u8 {
            return Err(OptimizerPayloadError::InvalidResponse);
        }
        let len = usize::try_from(encoded_plan_length)
            .map_err(|_| OptimizerPayloadError::InvalidResponse)?;
        let header_bytes = OPTIMIZER_RESPONSE_SUCCESS_HEADER_BYTES as usize;
        if len == 0 || len > MAX_PLAN_BYTES || bytes.len() != header_bytes + len {
            return Err(OptimizerPayloadError::InvalidResponse);
        }
        let plan = bytes[header_bytes..].to_vec();
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
