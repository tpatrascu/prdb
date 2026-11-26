//! Versioned request/response payloads for executor-to-optimizer submission.

use crate::optimizer_ir::physical::MAX_UNIFIED_PLAN_BYTES;
use crate::optimizer_ir::{BindError, LogicalRequest, RequestError, decode_logical_request};
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

#[derive(Clone, Debug, Eq, PartialEq)]
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
pub fn decode_optimizer_request(bytes: &[u8]) -> Result<LogicalRequest, OptimizerPayloadError> {
    decode_logical_request(bytes).map_err(OptimizerPayloadError::InvalidRequest)
}

fn validate_optimizer_plan(bytes: &[u8]) -> Result<(), OptimizerPayloadError> {
    crate::optimizer_ir::decode_physical_plan(bytes)
        .map(|_| ())
        .map_err(|_| OptimizerPayloadError::InvalidPlan)
}

impl OptimizerResponse {
    pub fn success(plan: Vec<u8>) -> Result<Self, OptimizerPayloadError> {
        if plan.is_empty() || plan.len() > MAX_UNIFIED_PLAN_BYTES {
            return Err(OptimizerPayloadError::PlanTooLarge);
        }
        validate_optimizer_plan(&plan)?;
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
                if plan.is_empty() || plan.len() > MAX_UNIFIED_PLAN_BYTES {
                    return Err(OptimizerPayloadError::PlanTooLarge);
                }
                validate_optimizer_plan(plan)?;
                let plan_length =
                    u32::try_from(plan.len()).map_err(|_| OptimizerPayloadError::PlanTooLarge)?;
                let mut output = crate::query_abi::encode_optimizer_response_success(
                    OptimizerStatus::Success as u8,
                    plan_length,
                )
                .to_vec();
                let encoded_plan = crate::query_abi::encode_optimizer_response_success_plan(plan)
                    .ok_or(OptimizerPayloadError::PlanTooLarge)?;
                output.extend_from_slice(encoded_plan);
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
        if len == 0 || len > MAX_UNIFIED_PLAN_BYTES || bytes.len() != header_bytes + len {
            return Err(OptimizerPayloadError::InvalidResponse);
        }
        let plan = crate::query_abi::decode_optimizer_response_success_plan(
            &bytes[header_bytes..],
            encoded_plan_length,
        )
        .ok_or(OptimizerPayloadError::InvalidResponse)?
        .to_vec();
        validate_optimizer_plan(&plan)?;
        Ok(Self {
            status,
            plan: Some(plan),
        })
    }
}

/// Collapse planner failures to a small non-disclosing wire status set.
pub fn status_for_error(error: &OptimizerError) -> OptimizerStatus {
    match error {
        OptimizerError::Request(_) | OptimizerError::Bind(BindError::Request(_)) => {
            OptimizerStatus::InvalidRequest
        }
        OptimizerError::Bind(BindError::RelationUnavailable) => OptimizerStatus::Unavailable,
        OptimizerError::Bind(BindError::InvalidQuery) | OptimizerError::Plan(_) => {
            OptimizerStatus::Rejected
        }
        OptimizerError::Bind(BindError::InvalidMetadata) => OptimizerStatus::Internal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer_ir::physical::PhysicalUpdateAssignment;
    use crate::optimizer_ir::{
        BoundColumn, BoundExpr, BoundUpdateValue, PhysicalOp, PhysicalPlan, RelationReference,
        encode_physical_plan,
    };
    use crate::tuple::ScalarType;

    fn test_relation() -> RelationReference {
        RelationReference {
            relation_index: 0,
            relation_name: "items".to_owned(),
            relation_id: [2; 16],
            table_epoch: 1,
        }
    }

    fn test_column() -> BoundColumn {
        BoundColumn {
            column_id: [3; 16],
            ordinal: 0,
            scalar_type: ScalarType::Integer,
            nullable: false,
        }
    }

    fn plan_bytes() -> Vec<u8> {
        let plan = PhysicalPlan {
            schema_name: "public".to_owned(),
            schema_id: [1; 16],
            schema_version: 1,
            root: PhysicalOp::Project {
                columns: vec![test_column()],
                input: Box::new(PhysicalOp::SeqScan {
                    relation: test_relation(),
                    scan_columns: vec![test_column()],
                }),
            },
        };
        encode_physical_plan(&plan).unwrap()
    }

    fn delete_plan_bytes() -> Vec<u8> {
        let plan = PhysicalPlan {
            schema_name: "public".to_owned(),
            schema_id: [1; 16],
            schema_version: 1,
            root: PhysicalOp::Delete {
                relation: test_relation(),
                input: Box::new(PhysicalOp::Filter {
                    predicate: BoundExpr::Boolean(true),
                    input: Box::new(PhysicalOp::SeqScan {
                        relation: test_relation(),
                        scan_columns: vec![test_column()],
                    }),
                }),
            },
        };
        encode_physical_plan(&plan).unwrap()
    }

    fn update_plan_bytes() -> Vec<u8> {
        let plan = PhysicalPlan {
            schema_name: "public".to_owned(),
            schema_id: [1; 16],
            schema_version: 1,
            root: PhysicalOp::Update {
                relation: test_relation(),
                assignments: vec![PhysicalUpdateAssignment {
                    column: test_column(),
                    value: BoundUpdateValue::Parameter(1),
                }],
                input: Box::new(PhysicalOp::Filter {
                    predicate: BoundExpr::Boolean(true),
                    input: Box::new(PhysicalOp::SeqScan {
                        relation: test_relation(),
                        scan_columns: vec![test_column()],
                    }),
                }),
            },
        };
        encode_physical_plan(&plan).unwrap()
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

        let delete = OptimizerResponse::success(delete_plan_bytes()).unwrap();
        assert_eq!(
            OptimizerResponse::decode(&delete.encode().unwrap()),
            Ok(delete)
        );

        let update = OptimizerResponse::success(update_plan_bytes()).unwrap();
        assert_eq!(
            OptimizerResponse::decode(&update.encode().unwrap()),
            Ok(update)
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
