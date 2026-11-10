//! Scoped planner metadata and estimate operations over trusted storage APIs.

use crate::executor_ipc::{
    ExecutorRequestError, ResolveRelationRequest, decode_name, decode_visible_column, encode_name,
    encode_visible_column, ensure_response_size,
};
use crate::ipc::{DispatchError, IpcFrame, Operation, ServiceRole, authorize_operation};
use crate::metadata_discovery::{
    AuthorizedPlanRelation, AuthorizedRelationDescription, PlanningTableHandle,
};
pub use crate::query_abi::{
    PLANNER_DESCRIBE_PLAN_RELATION_AVAILABLE_RESPONSE_HEADER_BYTES,
    PLANNER_DESCRIBE_PLAN_RELATION_AVAILABLE_RESPONSE_RELATION_ID_OFFSET,
    PLANNER_DESCRIBE_PLAN_RELATION_AVAILABLE_RESPONSE_SCHEMA_ID_OFFSET,
    PLANNER_DESCRIBE_PLAN_RELATION_AVAILABLE_RESPONSE_SCHEMA_VERSION_OFFSET,
    PLANNER_DESCRIBE_PLAN_RELATION_AVAILABLE_RESPONSE_STATUS_OFFSET,
    PLANNER_DESCRIBE_PLAN_RELATION_AVAILABLE_RESPONSE_TABLE_EPOCH_OFFSET,
    PLANNER_DESCRIBE_PLAN_RELATION_AVAILABLE_RESPONSE_VERSION_OFFSET,
    PLANNER_DESCRIBE_PLAN_RELATION_UNAVAILABLE_RESPONSE_HEADER_BYTES,
    PLANNER_DESCRIBE_PLAN_RELATION_UNAVAILABLE_RESPONSE_STATUS_OFFSET,
    PLANNER_DESCRIBE_PLAN_RELATION_UNAVAILABLE_RESPONSE_VERSION_OFFSET,
    PLANNER_DESCRIBE_RELATION_AVAILABLE_RESPONSE_HEADER_BYTES,
    PLANNER_DESCRIBE_RELATION_AVAILABLE_RESPONSE_PLANNING_HANDLE_OFFSET,
    PLANNER_DESCRIBE_RELATION_AVAILABLE_RESPONSE_RELATION_ID_OFFSET,
    PLANNER_DESCRIBE_RELATION_AVAILABLE_RESPONSE_STATUS_OFFSET,
    PLANNER_DESCRIBE_RELATION_AVAILABLE_RESPONSE_VERSION_OFFSET,
    PLANNER_DESCRIBE_RELATION_UNAVAILABLE_RESPONSE_HEADER_BYTES,
    PLANNER_DESCRIBE_RELATION_UNAVAILABLE_RESPONSE_STATUS_OFFSET,
    PLANNER_DESCRIBE_RELATION_UNAVAILABLE_RESPONSE_VERSION_OFFSET,
    PLANNER_ESTIMATE_SCAN_REQUEST_COLUMN_COUNT_OFFSET, PLANNER_ESTIMATE_SCAN_REQUEST_HEADER_BYTES,
    PLANNER_ESTIMATE_SCAN_REQUEST_PLANNING_HANDLE_OFFSET,
    PLANNER_ESTIMATE_SCAN_REQUEST_VERSION_OFFSET,
    PLANNER_ESTIMATE_SCAN_RESPONSE_COLUMN_COUNT_OFFSET,
    PLANNER_ESTIMATE_SCAN_RESPONSE_HEADER_BYTES,
    PLANNER_ESTIMATE_SCAN_RESPONSE_VISIBLE_ROWS_OFFSET,
};
use crate::statistics::{AuthorizedScanEstimate, AuthorizedScanSummary, ColumnPopulation};
use crate::transaction::{PlannerMetadataConnection, TransactionError};
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

const DESCRIBE_HEADER_BYTES: usize =
    PLANNER_DESCRIBE_RELATION_UNAVAILABLE_RESPONSE_HEADER_BYTES as usize;
const ESTIMATE_HEADER_BYTES: usize = PLANNER_ESTIMATE_SCAN_REQUEST_HEADER_BYTES as usize;
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
    SharedInvocationUnavailable,
    SharedInvocationPoisoned,
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

/// Authorized stable relation identity for optimizer binding. This response
/// contains no connection-local planner token and grants no executor access.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribePlanRelationResponse {
    pub relation: Option<AuthorizedPlanRelation>,
}

impl DescribePlanRelationResponse {
    pub fn encode(&self) -> Result<Vec<u8>, PlannerRequestError> {
        match &self.relation {
            None => {
                let payload =
                    crate::query_abi::encode_planner_describe_plan_relation_unavailable_response(1)
                        .to_vec();
                ensure_response_size(&payload)?;
                Ok(payload)
            }
            Some(relation) => {
                if relation.schema_id == [0; 16]
                    || relation.schema_version == 0
                    || relation.table_epoch == 0
                    || relation.relation.relation_id == [0; 16]
                    || relation.relation.columns.is_empty()
                    || relation.relation.columns.len() > crate::tuple::MAX_COLUMNS
                {
                    return Err(PlannerRequestError::InvalidPayload);
                }
                let mut payload =
                    crate::query_abi::encode_planner_describe_plan_relation_available_response(
                        0,
                        relation.schema_id,
                        relation.schema_version,
                        relation.table_epoch,
                        relation.relation.relation_id,
                    )
                    .to_vec();
                encode_name(&mut payload, &relation.relation.name)?;
                let count = u16::try_from(relation.relation.columns.len())
                    .map_err(|_| PlannerRequestError::ResponseTooLarge)?;
                payload.extend_from_slice(&count.to_be_bytes());
                for column in &relation.relation.columns {
                    encode_visible_column(&mut payload, column)?;
                }
                ensure_response_size(&payload)?;
                Ok(payload)
            }
        }
    }

    pub fn decode(payload: &[u8]) -> Result<Self, PlannerRequestError> {
        if payload.len()
            == PLANNER_DESCRIBE_PLAN_RELATION_UNAVAILABLE_RESPONSE_HEADER_BYTES as usize
        {
            return match crate::query_abi::decode_planner_describe_plan_relation_unavailable_response(payload) {
                Some(1) => Ok(Self { relation: None }),
                _ => Err(PlannerRequestError::InvalidPayload),
            };
        }
        let (status, schema_id, schema_version, table_epoch, relation_id) =
            crate::query_abi::decode_planner_describe_plan_relation_available_response(payload)
                .ok_or(PlannerRequestError::InvalidPayload)?;
        if status != 0
            || payload.len()
                < PLANNER_DESCRIBE_PLAN_RELATION_AVAILABLE_RESPONSE_HEADER_BYTES as usize + 5
        {
            return Err(PlannerRequestError::InvalidPayload);
        }
        {
            if schema_id == [0; 16]
                || schema_version == 0
                || table_epoch == 0
                || relation_id == [0; 16]
            {
                return Err(PlannerRequestError::InvalidPayload);
            }
            let mut offset =
                PLANNER_DESCRIBE_PLAN_RELATION_AVAILABLE_RESPONSE_HEADER_BYTES as usize;
            let name = decode_name(payload, &mut offset)?;
            let count = read_u16(payload, offset)? as usize;
            offset += 2;
            if count == 0 || count > crate::tuple::MAX_COLUMNS {
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
                relation: Some(AuthorizedPlanRelation {
                    schema_id,
                    schema_version,
                    table_epoch,
                    relation: AuthorizedRelationDescription {
                        relation_id,
                        name,
                        columns,
                    },
                }),
            })
        }
    }
}

impl DescribeRelationResponse {
    pub fn encode(&self) -> Result<Vec<u8>, PlannerRequestError> {
        match &self.table {
            None => {
                let payload =
                    crate::query_abi::encode_planner_describe_relation_unavailable_response(1)
                        .to_vec();
                ensure_response_size(&payload)?;
                Ok(payload)
            }
            Some((handle, description)) => {
                let mut payload =
                    crate::query_abi::encode_planner_describe_relation_available_response(
                        0,
                        description.relation_id,
                        handle.to_bytes(),
                    )
                    .to_vec();
                encode_name(&mut payload, &description.name)?;
                let count = u16::try_from(description.columns.len())
                    .map_err(|_| PlannerRequestError::ResponseTooLarge)?;
                payload.extend_from_slice(&count.to_be_bytes());
                for column in &description.columns {
                    encode_visible_column(&mut payload, column)?;
                }
                ensure_response_size(&payload)?;
                Ok(payload)
            }
        }
    }

    pub fn decode(payload: &[u8]) -> Result<Self, PlannerRequestError> {
        if payload.len() == DESCRIBE_HEADER_BYTES {
            return match crate::query_abi::decode_planner_describe_relation_unavailable_response(
                payload,
            ) {
                Some(1) => Ok(Self { table: None }),
                _ => Err(PlannerRequestError::InvalidPayload),
            };
        }
        let (status, relation_id, handle_bytes) =
            crate::query_abi::decode_planner_describe_relation_available_response(payload)
                .ok_or(PlannerRequestError::InvalidPayload)?;
        if status != 0
            || payload.len()
                < PLANNER_DESCRIBE_RELATION_AVAILABLE_RESPONSE_HEADER_BYTES as usize + 5
        {
            return Err(PlannerRequestError::InvalidPayload);
        }
        {
            let handle = PlanningTableHandle::from_bytes(&handle_bytes)
                .ok_or(PlannerRequestError::InvalidPayload)?;
            let mut offset = PLANNER_DESCRIBE_RELATION_AVAILABLE_RESPONSE_HEADER_BYTES as usize;
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
        let column_count = u16::try_from(self.column_ids.len())
            .map_err(|_| PlannerRequestError::InvalidPayload)?;
        let mut payload = crate::query_abi::encode_planner_estimate_scan_request(
            self.handle.to_bytes(),
            column_count,
        )
        .to_vec();
        for column_id in &self.column_ids {
            payload.extend_from_slice(column_id);
        }
        ensure_response_size(&payload)?;
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, PlannerRequestError> {
        let (handle_bytes, encoded_column_count) =
            crate::query_abi::decode_planner_estimate_scan_request(payload)
                .ok_or(PlannerRequestError::InvalidPayload)?;
        let handle = PlanningTableHandle::from_bytes(&handle_bytes)
            .ok_or(PlannerRequestError::InvalidPayload)?;
        let count = usize::from(encoded_column_count);
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
        match self {
            Self::Unknown => {
                let payload =
                    crate::query_abi::encode_planner_estimate_scan_unknown_response(1).to_vec();
                ensure_response_size(&payload)?;
                Ok(payload)
            }
            Self::Exact(summary) => {
                let column_count = u16::try_from(summary.columns.len())
                    .map_err(|_| PlannerRequestError::ResponseTooLarge)?;
                let mut payload = crate::query_abi::encode_planner_estimate_scan_response(
                    0,
                    summary.visible_rows,
                    column_count,
                )
                .to_vec();
                for column in &summary.columns {
                    payload.extend_from_slice(&column.readable_values.to_be_bytes());
                    payload.extend_from_slice(&column.nulls.to_be_bytes());
                    payload.extend_from_slice(&column.redacted.to_be_bytes());
                }
                ensure_response_size(&payload)?;
                Ok(payload)
            }
        }
    }

    pub fn decode(payload: &[u8]) -> Result<Self, PlannerRequestError> {
        if crate::query_abi::decode_planner_estimate_scan_unknown_response(payload) == Some(1) {
            return Ok(Self::Unknown);
        }
        let (status, visible_rows, encoded_column_count) =
            crate::query_abi::decode_planner_estimate_scan_response(payload)
                .ok_or(PlannerRequestError::InvalidPayload)?;
        if status != 0 {
            return Err(PlannerRequestError::InvalidPayload);
        }
        let count = usize::from(encoded_column_count);
        let header_bytes = PLANNER_ESTIMATE_SCAN_RESPONSE_HEADER_BYTES as usize;
        let expected = header_bytes
            .checked_add(
                count
                    .checked_mul(24)
                    .ok_or(PlannerRequestError::InvalidPayload)?,
            )
            .ok_or(PlannerRequestError::InvalidPayload)?;
        if count > crate::tuple::MAX_COLUMNS || expected != payload.len() {
            return Err(PlannerRequestError::InvalidPayload);
        }
        let mut offset = header_bytes;
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
        Operation::DescribePlanRelation => {
            let decoded = ResolveRelationRequest::decode(&request.payload)?;
            DescribePlanRelationResponse {
                relation: connection
                    .describe_relation_for_plan(&decoded.schema_name, &decoded.relation_name)?,
            }
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

/// Serve a planner connection that shares an invocation with executor
/// sessions. The transaction lock is held only while one request is handled.
pub fn serve_planner_shared<S: Read + Write>(
    stream: &mut S,
    invocation: &Arc<Mutex<Option<crate::transaction::InvocationTransaction<'_>>>>,
) -> Result<(), PlannerConnectionError> {
    let mut state = crate::transaction::PlannerMetadataSessionState::default();
    let mut request_ids = std::collections::HashSet::new();
    loop {
        let request = match crate::ipc::read_frame(stream) {
            Ok(Some(request)) => request,
            Ok(None) => return Ok(()),
            Err(error) => {
                abort_shared_planner_invocation(invocation)?;
                return Err(PlannerConnectionError::Frame(error));
            }
        };
        if request_ids.len() >= MAX_PLANNER_REQUESTS_PER_CONNECTION {
            abort_shared_planner_invocation(invocation)?;
            return Err(PlannerConnectionError::RequestLimitExceeded);
        }
        if !request_ids.insert(request.request_id) {
            abort_shared_planner_invocation(invocation)?;
            return Err(PlannerConnectionError::DuplicateRequestId);
        }
        let response = {
            let mut invocation = match invocation.lock() {
                Ok(invocation) => invocation,
                Err(poisoned) => {
                    let mut invocation = poisoned.into_inner();
                    if let Some(mut transaction) = invocation.take() {
                        let _ = transaction.abort();
                    }
                    return Err(PlannerConnectionError::SharedInvocationPoisoned);
                }
            };
            let transaction = invocation
                .as_mut()
                .ok_or(PlannerConnectionError::SharedInvocationUnavailable)?;
            let mut connection =
                transaction.open_planner_metadata_connection_with_state(std::mem::take(&mut state));
            let result = dispatch_planner_request(ServiceRole::Planner, &mut connection, &request);
            state = connection.detach();
            result
        };
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                abort_shared_planner_invocation(invocation)?;
                return Err(PlannerConnectionError::Request(error));
            }
        };
        if let Err(error) = crate::ipc::write_frame(stream, &response) {
            abort_shared_planner_invocation(invocation)?;
            return Err(PlannerConnectionError::Frame(error));
        }
    }
}

fn abort_shared_planner_invocation(
    invocation: &Arc<Mutex<Option<crate::transaction::InvocationTransaction<'_>>>>,
) -> Result<(), PlannerConnectionError> {
    let mut invocation = match invocation.lock() {
        Ok(invocation) => invocation,
        Err(poisoned) => {
            let mut invocation = poisoned.into_inner();
            if let Some(mut transaction) = invocation.take() {
                let _ = transaction.abort();
            }
            return Err(PlannerConnectionError::SharedInvocationPoisoned);
        }
    };
    if let Some(transaction) = invocation.as_mut() {
        transaction.abort().map_err(PlannerConnectionError::Abort)?;
    }
    Ok(())
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
