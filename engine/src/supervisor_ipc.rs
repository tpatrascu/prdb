//! Supervisor-only transaction finalization over a provisioned local channel.

use crate::audit::{AuditAction, AuditEvent, AuditResult};
use crate::ipc::{DispatchError, IpcFrame, Operation, ServiceRole, authorize_operation};
use crate::outcome::OutcomeStatus;
use crate::storage_manager::{
    AdministrativeCancellationError, AdministrativeCancellationResult, InvocationStatus,
    InvocationStatusError, OwnerCancellationResult, StorageManager,
};
use crate::transaction::{InvocationTransaction, TransactionError};
use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::net::UnixListener;

pub const SUPERVISOR_PAYLOAD_VERSION: u16 = 1;
pub const MAX_SUPERVISOR_REQUESTS_PER_CONNECTION: usize = 16;

#[derive(Debug)]
pub enum SupervisorRequestError {
    InvalidPayload,
    Dispatch(DispatchError),
    Cancellation(AdministrativeCancellationError),
    Status(InvocationStatusError),
    Transaction(TransactionError),
}

#[derive(Debug)]
pub enum GatewayRequestError {
    InvalidPayload,
    Dispatch(DispatchError),
    Client(crate::client_protocol::ClientSessionError),
    Start(crate::client_protocol::ClientStartError),
    Staging(crate::supervisor::StagingError),
}

impl std::fmt::Display for GatewayRequestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "gateway request failed: {self:?}")
    }
}

impl std::error::Error for GatewayRequestError {}

pub enum GatewayClientDispatch<'db> {
    Response(IpcFrame),
    Started {
        response: IpcFrame,
        invocation: crate::storage_manager::SupervisorInvocation<'db>,
        submission: crate::supervisor::SealedSubmission,
    },
}

pub enum GatewayClientServiceEvent<'db> {
    Disconnected,
    Started {
        request_id: u64,
        invocation: crate::storage_manager::SupervisorInvocation<'db>,
        submission: crate::supervisor::SealedSubmission,
    },
}

#[derive(Clone, Copy, Debug)]
pub struct GatewayAdmissionConfig {
    pub retention_deadline_us: i64,
    pub max_rows: usize,
    pub max_bytes: usize,
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug)]
pub struct GatewayServiceListenerConfig {
    pub expected_gateway_uid: u32,
    pub max_connections: usize,
    pub idle_timeout: Duration,
    pub accept_poll_interval: Duration,
}

#[cfg(unix)]
#[derive(Debug)]
pub enum GatewayServiceListenerError {
    InvalidConfig,
    Io(io::Error),
    WorkerPanicked,
}

#[cfg(unix)]
impl std::fmt::Display for GatewayServiceListenerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "gateway service listener failed: {self:?}")
    }
}

#[cfg(unix)]
impl std::error::Error for GatewayServiceListenerError {}

#[cfg(unix)]
pub fn bind_gateway_service_socket(socket_path: impl AsRef<Path>) -> io::Result<UnixListener> {
    use std::os::unix::fs::PermissionsExt;
    let socket_path = socket_path.as_ref();
    let listener = UnixListener::bind(socket_path)?;
    if let Err(error) =
        std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o600))
    {
        drop(listener);
        let _ = std::fs::remove_file(socket_path);
        return Err(error);
    }
    Ok(listener)
}

/// Accept bounded per-client Unix streams from the configured gateway UID.
/// The START callback must hand invocation work to a trusted scheduler and
/// return so the service thread can continue processing cancellation/status.
#[cfg(unix)]
pub fn serve_gateway_service_listener<'db, F>(
    listener: UnixListener,
    manager: &StorageManager<'db>,
    staging: &crate::supervisor::StagingStore,
    admission: GatewayAdmissionConfig,
    config: GatewayServiceListenerConfig,
    shutdown: Arc<AtomicBool>,
    on_started: F,
) -> Result<(), GatewayServiceListenerError>
where
    F: Fn(crate::storage_manager::SupervisorInvocation<'db>, crate::supervisor::SealedSubmission)
        + Sync,
{
    if config.max_connections == 0
        || config.idle_timeout.is_zero()
        || config.accept_poll_interval.is_zero()
    {
        return Err(GatewayServiceListenerError::InvalidConfig);
    }
    listener
        .set_nonblocking(true)
        .map_err(GatewayServiceListenerError::Io)?;
    let limiter = crate::gateway::GatewayConnectionLimiter::new(config.max_connections)
        .map_err(|_| GatewayServiceListenerError::InvalidConfig)?;

    std::thread::scope(|scope| {
        let mut workers = Vec::new();
        let mut accept_error = None;
        while !shutdown.load(Ordering::Acquire) {
            match listener.accept() {
                Ok((mut stream, _peer)) => {
                    let Some(permit) = limiter.try_acquire() else {
                        continue;
                    };
                    let worker_manager = manager;
                    let worker_staging = staging;
                    let worker_started = &on_started;
                    workers.push(scope.spawn(move || {
                        let _permit = permit;
                        if crate::gateway::unix_peer_uid(&stream).ok()
                            != Some(config.expected_gateway_uid)
                        {
                            return;
                        }
                        if stream
                            .set_read_timeout(Some(config.idle_timeout))
                            .and_then(|()| stream.set_write_timeout(Some(config.idle_timeout)))
                            .is_err()
                        {
                            return;
                        }
                        let mut session =
                            crate::client_protocol::ClientConnectionSession::default();
                        loop {
                            match serve_gateway_client_session(
                                &mut stream,
                                worker_manager,
                                worker_staging,
                                &mut session,
                                admission,
                            ) {
                                Ok(GatewayClientServiceEvent::Disconnected) => return,
                                Ok(GatewayClientServiceEvent::Started {
                                    invocation,
                                    submission,
                                    ..
                                }) => worker_started(invocation, submission),
                                Err(_) => return,
                            }
                        }
                    }));
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    std::thread::sleep(config.accept_poll_interval);
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => {
                    accept_error = Some(error);
                    break;
                }
            }
            let mut index = 0;
            while index < workers.len() {
                if workers[index].is_finished() {
                    let worker = workers.swap_remove(index);
                    if worker.join().is_err() {
                        accept_error = Some(io::Error::other("gateway service worker panicked"));
                        break;
                    }
                } else {
                    index += 1;
                }
            }
            if accept_error.is_some() {
                break;
            }
        }
        let mut worker_panicked = false;
        for worker in workers {
            worker_panicked |= worker.join().is_err();
        }
        if worker_panicked {
            Err(GatewayServiceListenerError::WorkerPanicked)
        } else if let Some(error) = accept_error {
            Err(GatewayServiceListenerError::Io(error))
        } else {
            Ok(())
        }
    })
}

/// Route one client operation over a provisioned gateway IPC connection.
/// HELLO establishes the per-client mode on this channel; all identity and
/// staging ownership remain server-derived.
pub fn dispatch_gateway_client_request<'db>(
    role: ServiceRole,
    manager: &StorageManager<'db>,
    staging: &crate::supervisor::StagingStore,
    session: &mut crate::client_protocol::ClientConnectionSession,
    request: &IpcFrame,
    admission: GatewayAdmissionConfig,
) -> Result<GatewayClientDispatch<'db>, GatewayRequestError> {
    let operation =
        Operation::from_code(request.operation).map_err(GatewayRequestError::Dispatch)?;
    authorize_operation(role, operation).map_err(GatewayRequestError::Dispatch)?;
    let frame = |kind, payload| crate::client_protocol::ClientFrame {
        kind,
        request_id: request.request_id,
        response_sequence: 0,
        payload,
    };
    let response = match operation {
        Operation::OpenClientSession => session
            .handle_handshake_request(manager, frame(1, request.payload.clone()))
            .map_err(GatewayRequestError::Client)?,
        Operation::PingClientSession => session
            .handle_handshake_request(manager, frame(11, request.payload.clone()))
            .map_err(GatewayRequestError::Client)?,
        Operation::AuthenticateSession => session
            .handle_handshake_request(manager, frame(2, request.payload.clone()))
            .map_err(GatewayRequestError::Client)?,
        Operation::PrepareSubmission => session
            .handle_prepare_request(manager, staging, frame(3, request.payload.clone()))
            .map_err(GatewayRequestError::Client)?,
        Operation::UploadSubmissionChunk => {
            let (&kind, payload) = request
                .payload
                .split_first()
                .ok_or(GatewayRequestError::InvalidPayload)?;
            if !matches!(kind, 4 | 5) {
                return Err(GatewayRequestError::InvalidPayload);
            }
            session
                .handle_upload_request(manager, staging, frame(u16::from(kind), payload.to_vec()))
                .map_err(GatewayRequestError::Client)?
        }
        Operation::SealSubmission => session
            .handle_seal_request(manager, staging, frame(6, request.payload.clone()))
            .map_err(GatewayRequestError::Client)?,
        Operation::StartSubmission => {
            let invocation_id = crate::client_protocol::decode_client_seal(&request.payload)
                .map_err(|error| {
                    GatewayRequestError::Client(
                        crate::client_protocol::ClientSessionError::InvalidControlPayload(error),
                    )
                })?;
            let (response, invocation) = session
                .handle_start_request(
                    manager,
                    staging,
                    frame(7, request.payload.clone()),
                    admission.retention_deadline_us,
                    admission.max_rows,
                    admission.max_bytes,
                )
                .map_err(GatewayRequestError::Start)?;
            let submission = staging
                .started_submission(
                    session
                        .staging_owner()
                        .map_err(GatewayRequestError::Client)?,
                    invocation_id,
                )
                .map_err(GatewayRequestError::Staging)?;
            return Ok(GatewayClientDispatch::Started {
                response: IpcFrame {
                    operation: request.operation,
                    request_id: response.request_id,
                    payload: response.payload,
                },
                invocation,
                submission,
            });
        }
        Operation::ClientGetInvocationStatus => session
            .handle_status_request(manager, staging, frame(8, request.payload.clone()))
            .map_err(GatewayRequestError::Client)?,
        Operation::ClientCancelInvocation => session
            .handle_cancel_prepared_request(manager, staging, frame(9, request.payload.clone()))
            .map_err(GatewayRequestError::Client)?,
        _ => return Err(GatewayRequestError::InvalidPayload),
    };
    Ok(GatewayClientDispatch::Response(IpcFrame {
        operation: request.operation,
        request_id: response.request_id,
        payload: response.payload,
    }))
}

/// Serve one gateway IPC stream bound to one client session. A clean close or
/// protocol failure discards unstarted staging and requests cancellation of
/// started work. START returns its trusted launch event to the caller after
/// the acknowledgement is written; the caller may resume this function after
/// that invocation reaches a terminal state.
pub fn serve_gateway_client_session<'db, S: Read + Write>(
    stream: &mut S,
    manager: &StorageManager<'db>,
    staging: &crate::supervisor::StagingStore,
    session: &mut crate::client_protocol::ClientConnectionSession,
    admission: GatewayAdmissionConfig,
) -> Result<GatewayClientServiceEvent<'db>, SupervisorConnectionError> {
    loop {
        let request = match crate::ipc::read_frame(stream) {
            Ok(Some(request)) => request,
            Ok(None) => {
                close_gateway_client_session(manager, staging, session)?;
                return Ok(GatewayClientServiceEvent::Disconnected);
            }
            Err(error) => {
                let cleanup = close_gateway_client_session(manager, staging, session);
                cleanup?;
                return Err(SupervisorConnectionError::Frame(error));
            }
        };
        let dispatched = match dispatch_gateway_client_request(
            ServiceRole::Gateway,
            manager,
            staging,
            session,
            &request,
            admission,
        ) {
            Ok(dispatched) => dispatched,
            Err(error) => {
                close_gateway_client_session(manager, staging, session)?;
                return Err(SupervisorConnectionError::Gateway(error));
            }
        };
        match dispatched {
            GatewayClientDispatch::Response(response) => {
                if let Err(error) = crate::ipc::write_frame(stream, &response) {
                    close_gateway_client_session(manager, staging, session)?;
                    return Err(SupervisorConnectionError::Frame(error));
                }
            }
            GatewayClientDispatch::Started {
                response,
                invocation,
                submission,
            } => {
                if let Err(error) = crate::ipc::write_frame(stream, &response) {
                    close_gateway_client_session(manager, staging, session)?;
                    return Err(SupervisorConnectionError::Frame(error));
                }
                return Ok(GatewayClientServiceEvent::Started {
                    request_id: response.request_id,
                    invocation,
                    submission,
                });
            }
        }
    }
}

fn close_gateway_client_session(
    manager: &StorageManager<'_>,
    staging: &crate::supervisor::StagingStore,
    session: &crate::client_protocol::ClientConnectionSession,
) -> Result<(), SupervisorConnectionError> {
    let owner = match session.staging_owner() {
        Ok(owner) => owner,
        Err(crate::client_protocol::ClientSessionError::AuthenticationRequired)
        | Err(crate::client_protocol::ClientSessionError::WrongMode) => return Ok(()),
        Err(error) => {
            return Err(SupervisorConnectionError::Gateway(
                GatewayRequestError::Client(error),
            ));
        }
    };
    let closed = staging
        .close_connection(owner)
        .map_err(SupervisorConnectionError::Staging)?;
    if let crate::supervisor::ConnectionClose::CancelStarted(invocation_ids) = closed {
        let now_us = crate::credentials::now_unix_micros().ok_or(
            SupervisorConnectionError::Transaction(TransactionError::ClockUnavailable),
        )?;
        for invocation_id in invocation_ids {
            manager
                .cancel_owned_invocation(owner.principal_id(), invocation_id, now_us)
                .map_err(SupervisorConnectionError::Status)?;
        }
    }
    Ok(())
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

/// Handle supervisor status or administrative cancellation with an actor
/// bound by trusted channel setup, never read from request payloads.
pub fn dispatch_supervisor_control_request(
    role: ServiceRole,
    manager: &StorageManager<'_>,
    authenticated_principal_id: [u8; 16],
    request: &IpcFrame,
) -> Result<IpcFrame, SupervisorRequestError> {
    let operation =
        Operation::from_code(request.operation).map_err(SupervisorRequestError::Dispatch)?;
    authorize_operation(role, operation).map_err(SupervisorRequestError::Dispatch)?;
    if request.payload.len() < 2
        || u16::from_be_bytes([request.payload[0], request.payload[1]])
            != SUPERVISOR_PAYLOAD_VERSION
    {
        return Err(SupervisorRequestError::InvalidPayload);
    }
    let payload = match operation {
        Operation::SupervisorGetInvocationStatus if request.payload.len() == 18 => {
            let invocation_id: [u8; 16] = request.payload[2..]
                .try_into()
                .map_err(|_| SupervisorRequestError::InvalidPayload)?;
            let now_us = crate::credentials::now_unix_micros().ok_or(
                SupervisorRequestError::Transaction(TransactionError::ClockUnavailable),
            )?;
            let snapshot = manager
                .get_invocation_status(invocation_id, authenticated_principal_id, now_us)
                .map_err(SupervisorRequestError::Status)?;
            encode_supervisor_status(snapshot.as_ref())?
        }
        Operation::SupervisorCancelInvocation if request.payload.len() == 20 => {
            let invocation_id: [u8; 16] = request.payload[2..18]
                .try_into()
                .map_err(|_| SupervisorRequestError::InvalidPayload)?;
            let reason_code = u16::from_be_bytes([request.payload[18], request.payload[19]]);
            let now_us = crate::credentials::now_unix_micros().ok_or(
                SupervisorRequestError::Transaction(TransactionError::ClockUnavailable),
            )?;
            let mut event_id = [0; 16];
            getrandom::fill(&mut event_id).map_err(|_| {
                SupervisorRequestError::Transaction(TransactionError::RandomnessUnavailable)
            })?;
            if event_id == [0; 16] {
                return Err(SupervisorRequestError::Transaction(
                    TransactionError::RandomnessUnavailable,
                ));
            }
            let event = AuditEvent {
                event_id,
                actor_id: Some(authenticated_principal_id),
                correlation_id: invocation_id,
                occurred_at_us: now_us,
                action: AuditAction::AdministrativeCancellation,
                result: AuditResult::Denied,
                target_ids: vec![invocation_id],
            };
            let result = manager
                .cancel_invocation(
                    authenticated_principal_id,
                    invocation_id,
                    reason_code,
                    now_us,
                    event,
                )
                .map_err(SupervisorRequestError::Cancellation)?;
            encode_cancellation_result(result)
        }
        _ => return Err(SupervisorRequestError::InvalidPayload),
    };
    Ok(IpcFrame {
        operation: request.operation,
        request_id: request.request_id,
        payload,
    })
}

/// Dispatch owner-scoped client status/cancel operations on a trusted
/// gateway-service channel. The principal is bound by channel setup and is
/// never accepted from the IPC payload.
pub fn dispatch_client_control_request(
    role: ServiceRole,
    manager: &StorageManager<'_>,
    authenticated_principal_id: [u8; 16],
    request: &IpcFrame,
) -> Result<IpcFrame, SupervisorRequestError> {
    let operation =
        Operation::from_code(request.operation).map_err(SupervisorRequestError::Dispatch)?;
    authorize_operation(role, operation).map_err(SupervisorRequestError::Dispatch)?;
    if !matches!(
        operation,
        Operation::ClientGetInvocationStatus | Operation::ClientCancelInvocation
    ) || request.payload.len() != 18
        || u16::from_be_bytes([request.payload[0], request.payload[1]])
            != SUPERVISOR_PAYLOAD_VERSION
    {
        return Err(SupervisorRequestError::InvalidPayload);
    }
    let invocation_id: [u8; 16] = request.payload[2..]
        .try_into()
        .map_err(|_| SupervisorRequestError::InvalidPayload)?;
    let now_us = crate::credentials::now_unix_micros().ok_or(
        SupervisorRequestError::Transaction(TransactionError::ClockUnavailable),
    )?;
    let payload = match operation {
        Operation::ClientGetInvocationStatus => {
            let snapshot = manager
                .get_invocation_status(invocation_id, authenticated_principal_id, now_us)
                .map_err(SupervisorRequestError::Status)?;
            encode_supervisor_status(snapshot.as_ref())?
        }
        Operation::ClientCancelInvocation => {
            let result = manager
                .cancel_owned_invocation(authenticated_principal_id, invocation_id, now_us)
                .map_err(SupervisorRequestError::Status)?;
            encode_owner_cancellation_result(result)
        }
        _ => unreachable!("operation was checked above"),
    };
    Ok(IpcFrame {
        operation: request.operation,
        request_id: request.request_id,
        payload,
    })
}

fn encode_owner_cancellation_result(result: OwnerCancellationResult) -> Vec<u8> {
    match result {
        OwnerCancellationResult::Accepted => {
            encode_cancellation_result(AdministrativeCancellationResult::Accepted)
        }
        OwnerCancellationResult::TooLate => {
            encode_cancellation_result(AdministrativeCancellationResult::TooLate)
        }
        OwnerCancellationResult::Terminal(outcome) => {
            encode_cancellation_result(AdministrativeCancellationResult::Terminal(outcome))
        }
        OwnerCancellationResult::Unknown => {
            encode_cancellation_result(AdministrativeCancellationResult::Unknown)
        }
    }
}

fn encode_cancellation_result(result: AdministrativeCancellationResult) -> Vec<u8> {
    let mut payload = SUPERVISOR_PAYLOAD_VERSION.to_be_bytes().to_vec();
    match result {
        AdministrativeCancellationResult::Accepted => payload.push(0),
        AdministrativeCancellationResult::TooLate => payload.push(1),
        AdministrativeCancellationResult::Terminal(outcome) => {
            payload.push(2);
            payload.push(match outcome.status {
                OutcomeStatus::Committed => 1,
                OutcomeStatus::Aborted => 2,
            });
            payload.extend_from_slice(&outcome.commit_sequence.to_be_bytes());
            payload.extend_from_slice(&outcome.retention_deadline_us.to_be_bytes());
        }
        AdministrativeCancellationResult::Unknown => payload.push(3),
    }
    payload
}

fn encode_supervisor_status(
    snapshot: Option<&crate::storage_manager::InvocationStatusSnapshot>,
) -> Result<Vec<u8>, SupervisorRequestError> {
    let mut payload = SUPERVISOR_PAYLOAD_VERSION.to_be_bytes().to_vec();
    let Some(snapshot) = snapshot else {
        payload.push(0); // Unknown or not owned by this principal.
        return Ok(payload);
    };
    match &snapshot.status {
        InvocationStatus::Active {
            started_at_us,
            deadline_at_us,
            cancellation_requested,
        } => encode_active_status(
            &mut payload,
            1,
            snapshot.observed_at_us,
            *started_at_us,
            *deadline_at_us,
            *cancellation_requested,
        ),
        InvocationStatus::Committing {
            started_at_us,
            deadline_at_us,
            cancellation_requested,
        } => encode_active_status(
            &mut payload,
            2,
            snapshot.observed_at_us,
            *started_at_us,
            *deadline_at_us,
            *cancellation_requested,
        ),
        InvocationStatus::Terminal(outcome) => {
            payload.push(match outcome.status {
                OutcomeStatus::Committed => 3,
                OutcomeStatus::Aborted => 4,
            });
            payload.extend_from_slice(&snapshot.observed_at_us.to_be_bytes());
            payload.extend_from_slice(&outcome.commit_sequence.to_be_bytes());
            payload.extend_from_slice(&outcome.retention_deadline_us.to_be_bytes());
        }
    }
    Ok(payload)
}

fn encode_active_status(
    payload: &mut Vec<u8>,
    status: u8,
    observed_at_us: i64,
    started_at_us: i64,
    deadline_at_us: Option<i64>,
    cancellation_requested: bool,
) {
    payload.push(status);
    payload.extend_from_slice(&observed_at_us.to_be_bytes());
    payload.extend_from_slice(&started_at_us.to_be_bytes());
    payload.push(u8::from(deadline_at_us.is_some()));
    if let Some(deadline_at_us) = deadline_at_us {
        payload.extend_from_slice(&deadline_at_us.to_be_bytes());
    }
    payload.push(u8::from(cancellation_requested));
}

#[derive(Debug)]
pub enum SupervisorConnectionError {
    Frame(crate::ipc::FrameError),
    Request(SupervisorRequestError),
    Gateway(GatewayRequestError),
    Staging(crate::supervisor::StagingError),
    Status(InvocationStatusError),
    Transaction(TransactionError),
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
                Ok(()) => {
                    let mut payload = SUPERVISOR_PAYLOAD_VERSION.to_be_bytes().to_vec();
                    payload.push(0);
                    (payload, None)
                }
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

/// Serve status and cancellation controls on a supervisor connection whose
/// authenticated actor is supplied by trusted channel setup.
pub fn serve_supervisor_control<S: Read + Write>(
    stream: &mut S,
    manager: &StorageManager<'_>,
    authenticated_principal_id: [u8; 16],
) -> Result<(), SupervisorConnectionError> {
    let mut request_ids = std::collections::HashSet::new();
    loop {
        let request = match crate::ipc::read_frame(stream) {
            Ok(Some(request)) => request,
            Ok(None) => return Ok(()),
            Err(error) => return Err(SupervisorConnectionError::Frame(error)),
        };
        if request_ids.len() >= MAX_SUPERVISOR_REQUESTS_PER_CONNECTION {
            return Err(SupervisorConnectionError::RequestLimitExceeded);
        }
        if !request_ids.insert(request.request_id) {
            return Err(SupervisorConnectionError::DuplicateRequestId);
        }
        let response = dispatch_supervisor_control_request(
            ServiceRole::Supervisor,
            manager,
            authenticated_principal_id,
            &request,
        )
        .map_err(SupervisorConnectionError::Request)?;
        crate::ipc::write_frame(stream, &response).map_err(SupervisorConnectionError::Frame)?;
    }
}

/// Serve the bounded gateway-to-storage status/cancel control channel. Role
/// and authenticated principal are supplied by trusted socket provisioning.
pub fn serve_client_control<S: Read + Write>(
    stream: &mut S,
    manager: &StorageManager<'_>,
    authenticated_principal_id: [u8; 16],
) -> Result<(), SupervisorConnectionError> {
    let mut request_ids = std::collections::HashSet::new();
    loop {
        let request = match crate::ipc::read_frame(stream) {
            Ok(Some(request)) => request,
            Ok(None) => return Ok(()),
            Err(error) => return Err(SupervisorConnectionError::Frame(error)),
        };
        if request_ids.len() >= MAX_SUPERVISOR_REQUESTS_PER_CONNECTION {
            return Err(SupervisorConnectionError::RequestLimitExceeded);
        }
        if !request_ids.insert(request.request_id) {
            return Err(SupervisorConnectionError::DuplicateRequestId);
        }
        let response = dispatch_client_control_request(
            ServiceRole::Gateway,
            manager,
            authenticated_principal_id,
            &request,
        )
        .map_err(SupervisorConnectionError::Request)?;
        crate::ipc::write_frame(stream, &response).map_err(SupervisorConnectionError::Frame)?;
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
