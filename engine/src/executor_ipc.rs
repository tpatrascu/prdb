//! Executor IPC operations that terminate at trusted storage primitives.

use crate::ipc::{DispatchError, IpcFrame, Operation, ServiceRole, authorize_operation};
use crate::metadata_discovery::ResolvedRelationHandle;
use crate::transaction::{ExecutorMetadataConnection, InvocationTransaction, TransactionError};
use std::io::{Read, Write};

pub const RESOLVE_RELATION_PAYLOAD_VERSION: u16 = 1;
const RESOLVE_REQUEST_HEADER_BYTES: usize = 6;
const RESOLVE_RESPONSE_HEADER_BYTES: usize = 3;
pub const MAX_RESOLVE_REQUESTS_PER_CONNECTION: usize = 256;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolveRelationRequest {
    pub schema_name: String,
    pub relation_name: String,
}

impl ResolveRelationRequest {
    pub fn encode(&self) -> Result<Vec<u8>, ExecutorRequestError> {
        let schema_len = u16::try_from(self.schema_name.len())
            .map_err(|_| ExecutorRequestError::InvalidPayload)?;
        let relation_len = u16::try_from(self.relation_name.len())
            .map_err(|_| ExecutorRequestError::InvalidPayload)?;
        if self.schema_name.is_empty()
            || self.relation_name.is_empty()
            || self.schema_name.len() + self.relation_name.len() + RESOLVE_REQUEST_HEADER_BYTES
                > crate::ipc::MAX_PAYLOAD_BYTES
        {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let mut payload = Vec::with_capacity(
            RESOLVE_REQUEST_HEADER_BYTES + self.schema_name.len() + self.relation_name.len(),
        );
        payload.extend_from_slice(&RESOLVE_RELATION_PAYLOAD_VERSION.to_be_bytes());
        payload.extend_from_slice(&schema_len.to_be_bytes());
        payload.extend_from_slice(&relation_len.to_be_bytes());
        payload.extend_from_slice(self.schema_name.as_bytes());
        payload.extend_from_slice(self.relation_name.as_bytes());
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        if payload.len() < RESOLVE_REQUEST_HEADER_BYTES
            || u16::from_be_bytes([payload[0], payload[1]]) != RESOLVE_RELATION_PAYLOAD_VERSION
        {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let schema_len = usize::from(u16::from_be_bytes([payload[2], payload[3]]));
        let relation_len = usize::from(u16::from_be_bytes([payload[4], payload[5]]));
        let expected_len = RESOLVE_REQUEST_HEADER_BYTES
            .checked_add(schema_len)
            .and_then(|len| len.checked_add(relation_len))
            .ok_or(ExecutorRequestError::InvalidPayload)?;
        if schema_len == 0 || relation_len == 0 || expected_len != payload.len() {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let schema_end = RESOLVE_REQUEST_HEADER_BYTES + schema_len;
        let schema_name = std::str::from_utf8(&payload[RESOLVE_REQUEST_HEADER_BYTES..schema_end])
            .map_err(|_| ExecutorRequestError::InvalidPayload)?
            .to_owned();
        let relation_name = std::str::from_utf8(&payload[schema_end..])
            .map_err(|_| ExecutorRequestError::InvalidPayload)?
            .to_owned();
        Ok(Self {
            schema_name,
            relation_name,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResolveRelationResponse {
    Resolved {
        relation_id: [u8; 16],
        handle: ResolvedRelationHandle,
    },
    Unavailable,
}

impl ResolveRelationResponse {
    pub fn encode(&self) -> Vec<u8> {
        let mut payload = Vec::with_capacity(35);
        payload.extend_from_slice(&RESOLVE_RELATION_PAYLOAD_VERSION.to_be_bytes());
        match self {
            Self::Resolved {
                relation_id,
                handle,
            } => {
                payload.push(0);
                payload.extend_from_slice(relation_id);
                payload.extend_from_slice(&handle.to_bytes());
            }
            Self::Unavailable => payload.push(1),
        }
        payload
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        if payload.len() < RESOLVE_RESPONSE_HEADER_BYTES
            || u16::from_be_bytes([payload[0], payload[1]]) != RESOLVE_RELATION_PAYLOAD_VERSION
        {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        match (payload[2], payload.len()) {
            (0, 35) => {
                let relation_id = payload[3..19]
                    .try_into()
                    .map_err(|_| ExecutorRequestError::InvalidPayload)?;
                let handle = ResolvedRelationHandle::from_bytes(&payload[19..])
                    .ok_or(ExecutorRequestError::InvalidPayload)?;
                Ok(Self::Resolved {
                    relation_id,
                    handle,
                })
            }
            (1, RESOLVE_RESPONSE_HEADER_BYTES) => Ok(Self::Unavailable),
            _ => Err(ExecutorRequestError::InvalidPayload),
        }
    }
}

#[derive(Debug)]
pub enum ExecutorRequestError {
    Dispatch(DispatchError),
    UnsupportedOperation(Operation),
    InvalidPayload,
    Transaction(TransactionError),
}

#[derive(Debug)]
pub enum ExecutorConnectionError {
    Frame(crate::ipc::FrameError),
    Request(ExecutorRequestError),
    DuplicateRequestId,
    RequestLimitExceeded,
    Abort(TransactionError),
}

impl std::fmt::Display for ExecutorRequestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "executor IPC request failed: {self:?}")
    }
}

impl std::error::Error for ExecutorRequestError {}

impl std::fmt::Display for ExecutorConnectionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "executor IPC connection failed: {self:?}")
    }
}

impl std::error::Error for ExecutorConnectionError {}

impl From<TransactionError> for ExecutorRequestError {
    fn from(error: TransactionError) -> Self {
        Self::Transaction(error)
    }
}

pub fn dispatch_executor_resolve_relation(
    role: ServiceRole,
    connection: &mut ExecutorMetadataConnection<'_, '_>,
    request: &IpcFrame,
) -> Result<IpcFrame, ExecutorRequestError> {
    let operation =
        Operation::from_code(request.operation).map_err(ExecutorRequestError::Dispatch)?;
    authorize_operation(role, operation).map_err(ExecutorRequestError::Dispatch)?;
    if operation != Operation::ResolveRelation {
        return Err(ExecutorRequestError::UnsupportedOperation(operation));
    }
    let request_payload = ResolveRelationRequest::decode(&request.payload)?;
    let response = match connection
        .resolve_relation(&request_payload.schema_name, &request_payload.relation_name)?
    {
        Some((handle, description)) => ResolveRelationResponse::Resolved {
            relation_id: description.relation_id,
            handle,
        },
        None => ResolveRelationResponse::Unavailable,
    };
    Ok(IpcFrame {
        operation: request.operation,
        request_id: request.request_id,
        payload: response.encode(),
    })
}

/// Serve the initial executor metadata operation on one provisioned stream.
/// The stream's caller must bind it to this invocation out of band. A clean
/// disconnect aborts the invocation; framing or request errors do the same.
pub fn serve_executor_resolve_relation<S: Read + Write>(
    stream: &mut S,
    transaction: &mut InvocationTransaction<'_>,
) -> Result<(), ExecutorConnectionError> {
    let mut connection = transaction.open_executor_metadata_connection();
    let mut request_ids = std::collections::HashSet::new();
    loop {
        let request = match crate::ipc::read_frame(stream) {
            Ok(Some(request)) => request,
            Ok(None) => {
                drop(connection);
                return transaction.abort().map_err(ExecutorConnectionError::Abort);
            }
            Err(error) => {
                drop(connection);
                return match transaction.abort() {
                    Ok(()) => Err(ExecutorConnectionError::Frame(error)),
                    Err(abort_error) => Err(ExecutorConnectionError::Abort(abort_error)),
                };
            }
        };
        if request_ids.len() >= MAX_RESOLVE_REQUESTS_PER_CONNECTION {
            drop(connection);
            return match transaction.abort() {
                Ok(()) => Err(ExecutorConnectionError::RequestLimitExceeded),
                Err(abort_error) => Err(ExecutorConnectionError::Abort(abort_error)),
            };
        }
        if !request_ids.insert(request.request_id) {
            drop(connection);
            return match transaction.abort() {
                Ok(()) => Err(ExecutorConnectionError::DuplicateRequestId),
                Err(abort_error) => Err(ExecutorConnectionError::Abort(abort_error)),
            };
        }
        let response = match dispatch_executor_resolve_relation(
            ServiceRole::Executor,
            &mut connection,
            &request,
        ) {
            Ok(response) => response,
            Err(error) => {
                drop(connection);
                return match transaction.abort() {
                    Ok(()) => Err(ExecutorConnectionError::Request(error)),
                    Err(abort_error) => Err(ExecutorConnectionError::Abort(abort_error)),
                };
            }
        };
        if let Err(error) = crate::ipc::write_frame(stream, &response) {
            drop(connection);
            return match transaction.abort() {
                Ok(()) => Err(ExecutorConnectionError::Frame(error)),
                Err(abort_error) => Err(ExecutorConnectionError::Abort(abort_error)),
            };
        }
    }
}
