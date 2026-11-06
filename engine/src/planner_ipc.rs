//! Scoped planner metadata and estimate operations over trusted storage APIs.

use crate::executor_ipc::{
    ExecutorRequestError, ResolveRelationRequest, decode_name, decode_visible_column, encode_name,
    encode_visible_column, ensure_response_size,
};
use crate::ipc::{DispatchError, IpcFrame, Operation, ServiceRole, authorize_operation};
use crate::metadata_discovery::{AuthorizedRelationDescription, PlanningTableHandle};
use crate::statistics::{AuthorizedScanEstimate, AuthorizedScanSummary, ColumnPopulation};
use crate::transaction::{PlannerMetadataConnection, TransactionError};
use std::io::{Read, Write};

pub const PLANNER_PAYLOAD_VERSION: u16 = 1;
const DESCRIBE_HEADER_BYTES: usize = 3;
const ESTIMATE_HEADER_BYTES: usize = 20;
pub const MAX_PLANNER_REQUESTS_PER_CONNECTION: usize = 256;

#[derive(Debug)]
pub enum PlannerRequestError {
    InvalidPayload,
    ResponseTooLarge,
    Dispatch(DispatchError),
    Transaction(TransactionError),
}

#[derive(Debug)]
pub enum PlannerConnectionError {
    Frame(crate::ipc::FrameError),
    Request(PlannerRequestError),
    Abort(TransactionError),
    RequestLimitExceeded,
    DuplicateRequestId,
}

impl std::fmt::Display for PlannerRequestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "planner request failed: {self:?}")
    }
}

impl std::error::Error for PlannerRequestError {}

impl From<ExecutorRequestError> for PlannerRequestError {
    fn from(error: ExecutorRequestError) -> Self {
        match error {
            ExecutorRequestError::InvalidPayload => Self::InvalidPayload,
            ExecutorRequestError::ResponseTooLarge => Self::ResponseTooLarge,
            ExecutorRequestError::Dispatch(error) => Self::Dispatch(error),
            ExecutorRequestError::Transaction(error) => Self::Transaction(error),
            _ => Self::InvalidPayload,
        }
    }
}

impl From<TransactionError> for PlannerRequestError {
    fn from(error: TransactionError) -> Self {
        Self::Transaction(error)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeRelationResponse {
    pub table: Option<(PlanningTableHandle, AuthorizedRelationDescription)>,
}

impl DescribeRelationResponse {
    pub fn encode(&self) -> Result<Vec<u8>, PlannerRequestError> {
        let mut payload = PLANNER_PAYLOAD_VERSION.to_be_bytes().to_vec();
        match &self.table {
            None => payload.push(1),
            Some((handle, description)) => {
                payload.push(0);
                payload.extend_from_slice(&description.relation_id);
                payload.extend_from_slice(&handle.to_bytes());
                encode_name(&mut payload, &description.name)?;
                let count = u16::try_from(description.columns.len())
                    .map_err(|_| PlannerRequestError::ResponseTooLarge)?;
                payload.extend_from_slice(&count.to_be_bytes());
                for column in &description.columns {
                    encode_visible_column(&mut payload, column)?;
                }
            }
        }
        ensure_response_size(&payload)?;
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, PlannerRequestError> {
        if payload.len() < DESCRIBE_HEADER_BYTES || read_u16(payload, 0)? != PLANNER_PAYLOAD_VERSION
        {
            return Err(PlannerRequestError::InvalidPayload);
        }
        match payload[2] {
            1 if payload.len() == DESCRIBE_HEADER_BYTES => Ok(Self { table: None }),
            0 => {
                if payload.len() < 40 {
                    return Err(PlannerRequestError::InvalidPayload);
                }
                let relation_id = payload[3..19]
                    .try_into()
                    .map_err(|_| PlannerRequestError::InvalidPayload)?;
                let handle = PlanningTableHandle::from_bytes(&payload[19..35])
                    .ok_or(PlannerRequestError::InvalidPayload)?;
                let mut offset = 35;
                let name = decode_name(payload, &mut offset)?;
                let count = read_u16(payload, offset)? as usize;
                offset += 2;
                if count > crate::tuple::MAX_COLUMNS {
                    return Err(PlannerRequestError::InvalidPayload);
                }
                let mut columns = Vec::with_capacity(count);
                for _ in 0..count {
                    columns.push(decode_visible_column(payload, &mut offset)?);
                }
                if offset != payload.len() {
                    return Err(PlannerRequestError::InvalidPayload);
                }
                Ok(Self {
                    table: Some((
                        handle,
                        AuthorizedRelationDescription {
                            relation_id,
                            name,
                            columns,
                        },
                    )),
                })
            }
            _ => Err(PlannerRequestError::InvalidPayload),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EstimateScanRequest {
    pub handle: PlanningTableHandle,
    /// Empty requests estimates for the full declared shape.
    pub column_ids: Vec<[u8; 16]>,
}

impl EstimateScanRequest {
    pub fn encode(&self) -> Result<Vec<u8>, PlannerRequestError> {
        if self.column_ids.len() > crate::tuple::MAX_COLUMNS
            || self.column_ids.contains(&[0; 16])
            || self
                .column_ids
                .iter()
                .copied()
                .collect::<std::collections::HashSet<_>>()
                .len()
                != self.column_ids.len()
        {
            return Err(PlannerRequestError::InvalidPayload);
        }
        let mut payload = PLANNER_PAYLOAD_VERSION.to_be_bytes().to_vec();
        payload.extend_from_slice(&self.handle.to_bytes());
        payload.extend_from_slice(
            &u16::try_from(self.column_ids.len())
                .map_err(|_| PlannerRequestError::InvalidPayload)?
                .to_be_bytes(),
        );
        for column_id in &self.column_ids {
            payload.extend_from_slice(column_id);
        }
        ensure_response_size(&payload)?;
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, PlannerRequestError> {
        if payload.len() < ESTIMATE_HEADER_BYTES || read_u16(payload, 0)? != PLANNER_PAYLOAD_VERSION
        {
            return Err(PlannerRequestError::InvalidPayload);
        }
        let handle = PlanningTableHandle::from_bytes(&payload[2..18])
            .ok_or(PlannerRequestError::InvalidPayload)?;
        let count = read_u16(payload, 18)? as usize;
        let expected = ESTIMATE_HEADER_BYTES
            .checked_add(
                count
                    .checked_mul(16)
                    .ok_or(PlannerRequestError::InvalidPayload)?,
            )
            .ok_or(PlannerRequestError::InvalidPayload)?;
        if count > crate::tuple::MAX_COLUMNS || expected != payload.len() {
            return Err(PlannerRequestError::InvalidPayload);
        }
        let column_ids = payload[ESTIMATE_HEADER_BYTES..]
            .chunks_exact(16)
            .map(|bytes| {
                bytes
                    .try_into()
                    .map_err(|_| PlannerRequestError::InvalidPayload)
            })
            .collect::<Result<Vec<[u8; 16]>, _>>()?;
        if column_ids.contains(&[0; 16])
            || column_ids
                .iter()
                .copied()
                .collect::<std::collections::HashSet<_>>()
                .len()
                != column_ids.len()
        {
            return Err(PlannerRequestError::InvalidPayload);
        }
        Ok(Self { handle, column_ids })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EstimateScanResponse {
    Exact(AuthorizedScanSummary),
    Unknown,
}

impl EstimateScanResponse {
    pub fn from_estimate(estimate: AuthorizedScanEstimate) -> Self {
        match estimate {
            AuthorizedScanEstimate::Exact(summary) => Self::Exact(summary),
            AuthorizedScanEstimate::Unknown => Self::Unknown,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, PlannerRequestError> {
        let mut payload = PLANNER_PAYLOAD_VERSION.to_be_bytes().to_vec();
        match self {
            Self::Unknown => payload.push(1),
            Self::Exact(summary) => {
                payload.push(0);
                payload.extend_from_slice(&summary.visible_rows.to_be_bytes());
                payload.extend_from_slice(
                    &u16::try_from(summary.columns.len())
                        .map_err(|_| PlannerRequestError::ResponseTooLarge)?
                        .to_be_bytes(),
                );
                for column in &summary.columns {
                    payload.extend_from_slice(&column.readable_values.to_be_bytes());
                    payload.extend_from_slice(&column.nulls.to_be_bytes());
                    payload.extend_from_slice(&column.redacted.to_be_bytes());
                }
            }
        }
        ensure_response_size(&payload)?;
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, PlannerRequestError> {
        if payload.len() < 3 || read_u16(payload, 0)? != PLANNER_PAYLOAD_VERSION {
            return Err(PlannerRequestError::InvalidPayload);
        }
        match payload[2] {
            1 if payload.len() == 3 => Ok(Self::Unknown),
            0 if payload.len() >= 13 => {
                let visible_rows = read_u64(payload, 3)?;
                let count = read_u16(payload, 11)? as usize;
                let expected = 13usize
                    .checked_add(
                        count
                            .checked_mul(24)
                            .ok_or(PlannerRequestError::InvalidPayload)?,
                    )
                    .ok_or(PlannerRequestError::InvalidPayload)?;
                if count > crate::tuple::MAX_COLUMNS || expected != payload.len() {
                    return Err(PlannerRequestError::InvalidPayload);
                }
                let mut offset = 13;
                let mut columns = Vec::with_capacity(count);
                for _ in 0..count {
                    columns.push(ColumnPopulation {
                        readable_values: read_u64(payload, offset)?,
                        nulls: read_u64(payload, offset + 8)?,
                        redacted: read_u64(payload, offset + 16)?,
                    });
                    offset += 24;
                }
                Ok(Self::Exact(AuthorizedScanSummary {
                    visible_rows,
                    columns,
                }))
            }
            _ => Err(PlannerRequestError::InvalidPayload),
        }
    }
}

pub fn dispatch_planner_request(
    role: ServiceRole,
    connection: &mut PlannerMetadataConnection<'_, '_>,
    request: &IpcFrame,
) -> Result<IpcFrame, PlannerRequestError> {
    let operation =
        Operation::from_code(request.operation).map_err(PlannerRequestError::Dispatch)?;
    authorize_operation(role, operation).map_err(PlannerRequestError::Dispatch)?;
    let payload = match operation {
        Operation::DescribeRelation => {
            let decoded = ResolveRelationRequest::decode(&request.payload)?;
            DescribeRelationResponse {
                table: connection.resolve_relation(&decoded.schema_name, &decoded.relation_name)?,
            }
            .encode()?
        }
        Operation::EstimateScan => {
            let decoded = EstimateScanRequest::decode(&request.payload)?;
            EstimateScanResponse::from_estimate(
                connection.estimate_scan(&decoded.handle, &decoded.column_ids)?,
            )
            .encode()?
        }
        _ => {
            return Err(PlannerRequestError::Dispatch(
                DispatchError::OperationNotAllowed { role, operation },
            ));
        }
    };
    Ok(IpcFrame {
        operation: request.operation,
        request_id: request.request_id,
        payload,
    })
}

/// Serve one planner stream bound out of band to a single invocation. Clean
/// EOF ends only this planning session so the invocation can continue with
/// execution; malformed frames and protocol violations abort it.
pub fn serve_planner<S: Read + Write>(
    stream: &mut S,
    transaction: &mut crate::transaction::InvocationTransaction<'_>,
) -> Result<(), PlannerConnectionError> {
    let mut connection = transaction.open_planner_metadata_connection();
    let mut request_ids = std::collections::HashSet::new();
    loop {
        let request = match crate::ipc::read_frame(stream) {
            Ok(Some(request)) => request,
            Ok(None) => {
                drop(connection);
                return Ok(());
            }
            Err(error) => {
                drop(connection);
                return match transaction.abort() {
                    Ok(()) => Err(PlannerConnectionError::Frame(error)),
                    Err(abort_error) => Err(PlannerConnectionError::Abort(abort_error)),
                };
            }
        };
        if request_ids.len() >= MAX_PLANNER_REQUESTS_PER_CONNECTION {
            drop(connection);
            return match transaction.abort() {
                Ok(()) => Err(PlannerConnectionError::RequestLimitExceeded),
                Err(error) => Err(PlannerConnectionError::Abort(error)),
            };
        }
        if !request_ids.insert(request.request_id) {
            drop(connection);
            return match transaction.abort() {
                Ok(()) => Err(PlannerConnectionError::DuplicateRequestId),
                Err(error) => Err(PlannerConnectionError::Abort(error)),
            };
        }
        let response =
            match dispatch_planner_request(ServiceRole::Planner, &mut connection, &request) {
                Ok(response) => response,
                Err(error) => {
                    drop(connection);
                    return match transaction.abort() {
                        Ok(()) => Err(PlannerConnectionError::Request(error)),
                        Err(abort_error) => Err(PlannerConnectionError::Abort(abort_error)),
                    };
                }
            };
        if let Err(error) = crate::ipc::write_frame(stream, &response) {
            drop(connection);
            return match transaction.abort() {
                Ok(()) => Err(PlannerConnectionError::Frame(error)),
                Err(abort_error) => Err(PlannerConnectionError::Abort(abort_error)),
            };
        }
    }
}

fn read_u16(payload: &[u8], offset: usize) -> Result<u16, PlannerRequestError> {
    let bytes: [u8; 2] = payload
        .get(offset..offset + 2)
        .ok_or(PlannerRequestError::InvalidPayload)?
        .try_into()
        .map_err(|_| PlannerRequestError::InvalidPayload)?;
    Ok(u16::from_be_bytes(bytes))
}

fn read_u64(payload: &[u8], offset: usize) -> Result<u64, PlannerRequestError> {
    let bytes: [u8; 8] = payload
        .get(offset..offset + 8)
        .ok_or(PlannerRequestError::InvalidPayload)?
        .try_into()
        .map_err(|_| PlannerRequestError::InvalidPayload)?;
    Ok(u64::from_be_bytes(bytes))
}
