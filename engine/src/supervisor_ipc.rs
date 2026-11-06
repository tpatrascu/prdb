//! Supervisor-only transaction finalization over a provisioned local channel.

use crate::ipc::{DispatchError, IpcFrame, Operation, ServiceRole, authorize_operation};
use crate::transaction::{InvocationTransaction, TransactionError};
use std::io::{Read, Write};

pub const SUPERVISOR_PAYLOAD_VERSION: u16 = 1;
pub const MAX_SUPERVISOR_REQUESTS_PER_CONNECTION: usize = 16;

#[derive(Debug)]
pub enum SupervisorRequestError {
    InvalidPayload,
    Dispatch(DispatchError),
    Transaction(TransactionError),
}

impl std::fmt::Display for SupervisorRequestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "supervisor request failed: {self:?}")
    }
}

impl std::error::Error for SupervisorRequestError {}

impl From<TransactionError> for SupervisorRequestError {
    fn from(error: TransactionError) -> Self {
        Self::Transaction(error)
    }
}

#[derive(Debug)]
pub enum SupervisorConnectionError {
    Frame(crate::ipc::FrameError),
    Request(SupervisorRequestError),
    Abort(TransactionError),
    RequestLimitExceeded,
    DuplicateRequestId,
    NoActiveInvocation,
}

/// Handle one request against the invocation bound out of band to this
/// supervisor channel. Only the trusted Supervisor role may finalize it.
pub fn dispatch_supervisor_request(
    role: ServiceRole,
    invocation: &mut Option<InvocationTransaction<'_>>,
    request: &IpcFrame,
) -> Result<IpcFrame, SupervisorRequestError> {
    let operation =
        Operation::from_code(request.operation).map_err(SupervisorRequestError::Dispatch)?;
    authorize_operation(role, operation).map_err(SupervisorRequestError::Dispatch)?;
    if !matches!(
        operation,
        Operation::FinalizeTransaction | Operation::AbortTransaction
    ) {
        return Err(SupervisorRequestError::Dispatch(
            DispatchError::OperationNotAllowed { role, operation },
        ));
    }
    if request.payload.len() != 2
        || u16::from_be_bytes([request.payload[0], request.payload[1]])
            != SUPERVISOR_PAYLOAD_VERSION
    {
        return Err(SupervisorRequestError::InvalidPayload);
    }
    let transaction = invocation
        .take()
        .ok_or(SupervisorRequestError::InvalidPayload)?;
    let (payload, transaction_error) = match operation {
        Operation::FinalizeTransaction => match transaction.commit() {
            Ok(sequence) => {
                let mut payload = SUPERVISOR_PAYLOAD_VERSION.to_be_bytes().to_vec();
                payload.push(0);
                payload.extend_from_slice(&sequence.to_be_bytes());
                (payload, None)
            }
            Err(error) => (Vec::new(), Some(error)),
        },
        Operation::AbortTransaction => {
            let mut transaction = transaction;
            match transaction.abort() {
                Ok(()) => (SUPERVISOR_PAYLOAD_VERSION.to_be_bytes().to_vec(), None),
                Err(error) => (Vec::new(), Some(error)),
            }
        }
        _ => {
            return Err(SupervisorRequestError::Dispatch(
                DispatchError::OperationNotAllowed { role, operation },
            ));
        }
    };
    if let Some(error) = transaction_error {
        return Err(SupervisorRequestError::Transaction(error));
    }
    Ok(IpcFrame {
        operation: request.operation,
        request_id: request.request_id,
        payload,
    })
}

/// Serve a trusted supervisor's single invocation channel. Losing the
/// supervisor before a final decision aborts the transaction. A successful
/// worker disconnect alone never commits.
pub fn serve_supervisor<S: Read + Write>(
    stream: &mut S,
    invocation: &mut Option<InvocationTransaction<'_>>,
) -> Result<(), SupervisorConnectionError> {
    let mut request_ids = std::collections::HashSet::new();
    loop {
        let request = match crate::ipc::read_frame(stream) {
            Ok(Some(request)) => request,
            Ok(None) => {
                abort_invocation(invocation)?;
                return Ok(());
            }
            Err(error) => {
                abort_invocation(invocation)?;
                return Err(SupervisorConnectionError::Frame(error));
            }
        };
        if request_ids.len() >= MAX_SUPERVISOR_REQUESTS_PER_CONNECTION {
            abort_invocation(invocation)?;
            return Err(SupervisorConnectionError::RequestLimitExceeded);
        }
        if !request_ids.insert(request.request_id) {
            abort_invocation(invocation)?;
            return Err(SupervisorConnectionError::DuplicateRequestId);
        }
        let response =
            match dispatch_supervisor_request(ServiceRole::Supervisor, invocation, &request) {
                Ok(response) => response,
                Err(error) => {
                    abort_invocation(invocation)?;
                    return Err(SupervisorConnectionError::Request(error));
                }
            };
        if let Err(error) = crate::ipc::write_frame(stream, &response) {
            abort_invocation(invocation)?;
            return Err(SupervisorConnectionError::Frame(error));
        }
        if invocation.is_none() {
            return Ok(());
        }
    }
}

fn abort_invocation(
    invocation: &mut Option<InvocationTransaction<'_>>,
) -> Result<(), SupervisorConnectionError> {
    if let Some(mut transaction) = invocation.take() {
        transaction
            .abort()
            .map_err(SupervisorConnectionError::Abort)?;
    }
    Ok(())
}
