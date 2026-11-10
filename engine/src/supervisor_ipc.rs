//! Supervisor-only transaction finalization over a provisioned local channel.

use crate::audit::{AuditAction, AuditEvent, AuditResult};
use crate::ipc::{DispatchError, IpcFrame, Operation, ServiceRole, authorize_operation};
use crate::outcome::OutcomeStatus;
pub use crate::query_abi::{
    SUPERVISOR_ADMIN_CANCEL_REQUEST_HEADER_BYTES,
    SUPERVISOR_ADMIN_CANCEL_REQUEST_INVOCATION_ID_OFFSET,
    SUPERVISOR_ADMIN_CANCEL_REQUEST_REASON_CODE_OFFSET,
    SUPERVISOR_ADMIN_CANCEL_REQUEST_VERSION_OFFSET,
    SUPERVISOR_INVOCATION_CONTROL_REQUEST_HEADER_BYTES,
    SUPERVISOR_INVOCATION_CONTROL_REQUEST_INVOCATION_ID_OFFSET,
    SUPERVISOR_INVOCATION_CONTROL_REQUEST_VERSION_OFFSET, SUPERVISOR_PAYLOAD_VERSION,
};
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
use zeroize::Zeroize;

#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};

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
    ClockUnavailable,
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
        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        executor: Option<Box<crate::executor_launcher::RunningExecutor>>,
    },
}

pub enum GatewayClientServiceEvent<'db> {
    Disconnected,
    Started {
        request_id: u64,
        invocation: crate::storage_manager::SupervisorInvocation<'db>,
        submission: crate::supervisor::SealedSubmission,
        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        executor: Option<Box<crate::executor_launcher::RunningExecutor>>,
    },
}

#[derive(Clone, Copy, Debug)]
pub struct GatewayAdmissionConfig {
    /// Outcome retention interval from each START or ADMIN request, in
    /// microseconds. `i64::MAX` retains outcomes indefinitely.
    pub retention_period_us: i64,
    pub max_rows: usize,
    pub max_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetentionDeadlineError {
    InvalidPeriod,
    ClockUnavailable,
}

impl GatewayAdmissionConfig {
    pub fn retention_deadline_us(self) -> Result<i64, RetentionDeadlineError> {
        if self.retention_period_us <= 0 {
            return Err(RetentionDeadlineError::InvalidPeriod);
        }
        if self.retention_period_us == i64::MAX {
            return Ok(i64::MAX);
        }
        crate::credentials::now_unix_micros()
            .and_then(|now| now.checked_add(self.retention_period_us))
            .ok_or(RetentionDeadlineError::ClockUnavailable)
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
#[derive(Clone, Debug)]
pub struct GatewayExecutorConfig {
    pub cgroup_parent: std::path::PathBuf,
    pub resource_limits: crate::linux_sandbox::WorkerResourceLimits,
    /// Optional analytical profile. When absent, analytical submissions use
    /// the operational profile for backwards-compatible deployments.
    pub analytical_resource_limits: Option<crate::linux_sandbox::WorkerResourceLimits>,
    pub launch: crate::executor_launcher::ExecutorLaunchConfig,
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
impl GatewayExecutorConfig {
    pub fn resource_limits_for(
        &self,
        workload_class: crate::supervisor::WorkloadClass,
    ) -> crate::linux_sandbox::WorkerResourceLimits {
        match workload_class {
            crate::supervisor::WorkloadClass::Operational => self.resource_limits,
            crate::supervisor::WorkloadClass::Analytical => self
                .analytical_resource_limits
                .unwrap_or(self.resource_limits),
        }
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
#[derive(Clone, Debug)]
pub struct GatewayServiceRuntimeConfig {
    pub admission: GatewayAdmissionConfig,
    pub executor: GatewayExecutorConfig,
    pub listener: GatewayServiceListenerConfig,
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug)]
pub struct GatewayServiceListenerConfig {
    pub expected_gateway_uid: u32,
    pub max_connections: usize,
    pub idle_timeout: Duration,
    pub accept_poll_interval: Duration,
    pub outcome_maintenance_interval: Duration,
    pub outcome_maintenance_batch: usize,
}

#[cfg(unix)]
#[derive(Debug)]
pub enum GatewayServiceListenerError {
    InvalidConfig,
    StorageReopenRequired,
    Staging(crate::supervisor::StagingError),
    OutcomeMaintenance(crate::outcome::OutcomeMaintenanceError),
    Io(io::Error),
    WorkerPanicked,
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
#[derive(Debug)]
pub enum GatewayServiceRecoveryError {
    Bind(io::Error),
    Manager(crate::storage_manager::StorageManagerInitError),
    Listener(GatewayServiceListenerError),
    Bootstrap(crate::bootstrap::BootstrapError),
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
impl std::fmt::Display for GatewayServiceRecoveryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "gateway service recovery failed: {self:?}")
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
impl std::error::Error for GatewayServiceRecoveryError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GatewayAbortReason {
    QueryError,
    WorkerFailure,
    Cancelled,
    Deadline,
    ResourceLimit,
    InternalError,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GatewayTerminalDecision {
    Commit,
    Abort(GatewayAbortReason),
}

#[derive(Debug)]
pub enum GatewayTerminalRelayError {
    Io(io::Error),
    Frame(crate::ipc::FrameError),
    Invocation(crate::storage_manager::SupervisorInvocationError),
    Status(InvocationStatusError),
    InvalidStorageResponse,
    HandlerPanicked,
}

impl std::fmt::Display for GatewayTerminalRelayError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "gateway terminal relay failed: {self:?}")
    }
}

impl std::error::Error for GatewayTerminalRelayError {}

pub fn send_gateway_result_data_event<W: Write>(
    stream: &mut W,
    start_request_id: u64,
    payload: Vec<u8>,
) -> Result<(), crate::ipc::FrameError> {
    if payload.len() > crate::client_protocol::CLIENT_MAX_PAYLOAD_BYTES {
        return Err(crate::ipc::FrameError::PayloadTooLarge);
    }
    crate::ipc::write_frame(
        stream,
        &IpcFrame {
            operation: Operation::GatewayResultData as u16,
            request_id: start_request_id,
            payload,
        },
    )
}

/// Commit or abort through storage's terminal authority, then emit TERMINAL
/// only after its correlated response confirms the durable decision.
pub fn finalize_gateway_invocation_and_send_terminal<'db, W: Write>(
    invocation: &mut crate::storage_manager::SupervisorInvocation<'db>,
    result_stream: &mut W,
    start_request_id: u64,
    decision: GatewayTerminalDecision,
) -> Result<(), GatewayTerminalRelayError> {
    let operation = match decision {
        GatewayTerminalDecision::Commit => Operation::FinalizeTransaction,
        GatewayTerminalDecision::Abort(_) => Operation::AbortTransaction,
    };
    let (mut storage_stream, mut supervisor_stream) =
        UnixStream::pair().map_err(GatewayTerminalRelayError::Io)?;
    let response = std::thread::scope(|scope| {
        let handler = scope.spawn(|| invocation.serve_terminal(&mut storage_stream));
        crate::ipc::write_frame(
            &mut supervisor_stream,
            &IpcFrame {
                operation: operation as u16,
                request_id: 1,
                payload: SUPERVISOR_PAYLOAD_VERSION.to_be_bytes().to_vec(),
            },
        )
        .map_err(GatewayTerminalRelayError::Frame)?;
        let response = crate::ipc::read_frame(&mut supervisor_stream)
            .map_err(GatewayTerminalRelayError::Frame)?
            .ok_or(GatewayTerminalRelayError::InvalidStorageResponse)?;
        handler
            .join()
            .map_err(|_| GatewayTerminalRelayError::HandlerPanicked)?
            .map_err(GatewayTerminalRelayError::Invocation)?;
        Ok::<_, GatewayTerminalRelayError>(response)
    })?;
    let valid = response.operation == operation as u16
        && response.request_id == 1
        && response.payload.len()
            == match decision {
                GatewayTerminalDecision::Commit => 11,
                GatewayTerminalDecision::Abort(_) => 3,
            }
        && u16::from_be_bytes([response.payload[0], response.payload[1]])
            == SUPERVISOR_PAYLOAD_VERSION
        && response.payload[2] == 0;
    if !valid {
        return Err(GatewayTerminalRelayError::InvalidStorageResponse);
    }
    let (outcome, reason_code) = match decision {
        GatewayTerminalDecision::Commit => ("committed", "success"),
        GatewayTerminalDecision::Abort(reason) => (
            "aborted",
            match reason {
                GatewayAbortReason::QueryError => "query_error",
                GatewayAbortReason::WorkerFailure => "worker_failure",
                GatewayAbortReason::Cancelled => "cancelled",
                GatewayAbortReason::Deadline => "deadline",
                GatewayAbortReason::ResourceLimit => "resource_limit",
                GatewayAbortReason::InternalError => "internal_error",
            },
        ),
    };
    let invocation_id = invocation
        .invocation_id()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let payload = format!(
        "{{\"invocation_id\":\"{invocation_id}\",\"outcome\":\"{outcome}\",\"reason_code\":\"{reason_code}\"}}"
    )
    .into_bytes();
    crate::ipc::write_frame(
        result_stream,
        &IpcFrame {
            operation: Operation::GatewayTerminal as u16,
            request_id: start_request_id,
            payload,
        },
    )
    .map_err(GatewayTerminalRelayError::Frame)
}

/// Emit a terminal event only after an owner-scoped storage status confirms a
/// retained durable outcome. This covers worker-failure aborts and ambiguous
/// terminal replies without inventing an outcome from process or socket state.
pub fn send_gateway_terminal_for_confirmed_outcome<'db, W: Write>(
    manager: &StorageManager<'db>,
    invocation: &crate::storage_manager::SupervisorInvocation<'_>,
    result_stream: &mut W,
    start_request_id: u64,
    aborted_reason: GatewayAbortReason,
) -> Result<GatewayTerminalDecision, GatewayTerminalRelayError> {
    let now_us = crate::credentials::now_unix_micros()
        .ok_or(GatewayTerminalRelayError::InvalidStorageResponse)?;
    let snapshot = manager
        .get_invocation_status(
            invocation.invocation_id(),
            invocation.principal_id(),
            now_us,
        )
        .map_err(GatewayTerminalRelayError::Status)?
        .ok_or(GatewayTerminalRelayError::InvalidStorageResponse)?;
    let outcome = match snapshot.status {
        InvocationStatus::Terminal(outcome) => outcome,
        InvocationStatus::Active { .. } | InvocationStatus::Committing { .. } => {
            return Err(GatewayTerminalRelayError::InvalidStorageResponse);
        }
    };
    let decision = match outcome.status {
        OutcomeStatus::Committed => GatewayTerminalDecision::Commit,
        OutcomeStatus::Aborted => GatewayTerminalDecision::Abort(aborted_reason),
    };
    let reason_code = match decision {
        GatewayTerminalDecision::Commit => "success",
        GatewayTerminalDecision::Abort(reason) => gateway_abort_reason_code(reason),
    };
    let invocation_id = invocation
        .invocation_id()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let outcome_name = match decision {
        GatewayTerminalDecision::Commit => "committed",
        GatewayTerminalDecision::Abort(_) => "aborted",
    };
    let payload = format!(
        "{{\"invocation_id\":\"{invocation_id}\",\"outcome\":\"{outcome_name}\",\"reason_code\":\"{reason_code}\"}}"
    )
    .into_bytes();
    crate::ipc::write_frame(
        result_stream,
        &IpcFrame {
            operation: Operation::GatewayTerminal as u16,
            request_id: start_request_id,
            payload,
        },
    )
    .map_err(GatewayTerminalRelayError::Frame)?;
    Ok(decision)
}

/// Run the trusted worker/result/storage sequence and emit exactly the
/// storage-confirmed terminal event. A nonzero worker exit, result relay error,
/// cancellation, or commit rejection is reported as aborted only after a
/// durable owner-scoped status lookup confirms ABORTED.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub fn observe_executor_relay_and_finalize_gateway<'db>(
    manager: &StorageManager<'db>,
    invocation: &mut crate::storage_manager::SupervisorInvocation<'_>,
    executor: &mut crate::executor_launcher::RunningExecutor,
    result_stream: &mut UnixStream,
    start_request_id: u64,
) -> Result<GatewayTerminalDecision, GatewayTerminalRelayError> {
    let observation =
        observe_executor_and_relay_results(executor, invocation, result_stream, start_request_id);
    match observation {
        Ok(status) if status.success() => {
            match finalize_gateway_invocation_and_send_terminal(
                invocation,
                result_stream,
                start_request_id,
                GatewayTerminalDecision::Commit,
            ) {
                Ok(()) => Ok(GatewayTerminalDecision::Commit),
                Err(commit_error) => {
                    match send_gateway_terminal_for_confirmed_outcome(
                        manager,
                        invocation,
                        result_stream,
                        start_request_id,
                        GatewayAbortReason::QueryError,
                    ) {
                        Ok(decision) => Ok(decision),
                        Err(_) => Err(commit_error),
                    }
                }
            }
        }
        Ok(_) => send_gateway_terminal_for_confirmed_outcome(
            manager,
            invocation,
            result_stream,
            start_request_id,
            GatewayAbortReason::WorkerFailure,
        ),
        Err(error) => send_gateway_terminal_for_confirmed_outcome(
            manager,
            invocation,
            result_stream,
            start_request_id,
            gateway_abort_reason_for_observation(&error),
        ),
    }
}

fn gateway_abort_reason_for_observation(
    error: &crate::storage_manager::SupervisorInvocationError,
) -> GatewayAbortReason {
    match error {
        crate::storage_manager::SupervisorInvocationError::WorkerExited(_) => {
            GatewayAbortReason::WorkerFailure
        }
        crate::storage_manager::SupervisorInvocationError::Deadline(_) => {
            GatewayAbortReason::Deadline
        }
        crate::storage_manager::SupervisorInvocationError::Transaction(_) => {
            GatewayAbortReason::QueryError
        }
        crate::storage_manager::SupervisorInvocationError::Executor(_)
        | crate::storage_manager::SupervisorInvocationError::Planner(_) => {
            GatewayAbortReason::QueryError
        }
        crate::storage_manager::SupervisorInvocationError::WorkerChannelIo(error)
            if error.kind() == io::ErrorKind::InvalidData =>
        {
            GatewayAbortReason::ResourceLimit
        }
        crate::storage_manager::SupervisorInvocationError::WorkerCgroupIo(_) => {
            GatewayAbortReason::ResourceLimit
        }
        crate::storage_manager::SupervisorInvocationError::WorkerChannelIo(_)
        | crate::storage_manager::SupervisorInvocationError::WorkerProcessIo(_)
        | crate::storage_manager::SupervisorInvocationError::WorkerHandlerPanicked
        | crate::storage_manager::SupervisorInvocationError::WorkerChannelsActive
        | crate::storage_manager::SupervisorInvocationError::WorkerExitNotObserved
        | crate::storage_manager::SupervisorInvocationError::WorkerExitObservationActive
        | crate::storage_manager::SupervisorInvocationError::Closed
        | crate::storage_manager::SupervisorInvocationError::Supervisor(_) => {
            GatewayAbortReason::InternalError
        }
    }
}

fn gateway_abort_reason_code(reason: GatewayAbortReason) -> &'static str {
    match reason {
        GatewayAbortReason::QueryError => "query_error",
        GatewayAbortReason::WorkerFailure => "worker_failure",
        GatewayAbortReason::Cancelled => "cancelled",
        GatewayAbortReason::Deadline => "deadline",
        GatewayAbortReason::ResourceLimit => "resource_limit",
        GatewayAbortReason::InternalError => "internal_error",
    }
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
/// The START callback owns trusted scheduling and event emission for that
/// invocation and receives the service's current StorageManager for durable
/// outcome confirmation; status/cancel control remains available through other
/// sessions.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub fn serve_gateway_service_listener<'db, F>(
    listener: UnixListener,
    manager: &StorageManager<'db>,
    staging: &crate::supervisor::StagingStore,
    runtime: GatewayServiceRuntimeConfig,
    shutdown: Arc<AtomicBool>,
    on_started: F,
) -> Result<(), GatewayServiceListenerError>
where
    F: Fn(
            &StorageManager<'db>,
            u64,
            &mut UnixStream,
            crate::storage_manager::SupervisorInvocation<'db>,
            crate::supervisor::SealedSubmission,
            crate::executor_launcher::RunningExecutor,
        ) + Sync,
{
    if runtime.listener.max_connections == 0
        || runtime.listener.idle_timeout.is_zero()
        || runtime.listener.accept_poll_interval.is_zero()
        || runtime.listener.outcome_maintenance_interval.is_zero()
        || runtime.listener.outcome_maintenance_batch == 0
        || runtime.listener.outcome_maintenance_batch > crate::outcome::MAX_OUTCOME_PRUNE_BATCH
        || runtime.admission.retention_period_us <= 0
    {
        return Err(GatewayServiceListenerError::InvalidConfig);
    }
    listener
        .set_nonblocking(true)
        .map_err(GatewayServiceListenerError::Io)?;
    let limiter = crate::gateway::GatewayConnectionLimiter::new(runtime.listener.max_connections)
        .map_err(|_| GatewayServiceListenerError::InvalidConfig)?;
    let mut next_outcome_maintenance = std::time::Instant::now();

    std::thread::scope(|scope| {
        let mut workers = Vec::new();
        let mut accept_error = None;
        let mut staging_error = None;
        let mut outcome_maintenance_error = None;
        let mut storage_reopen_required = false;
        while !shutdown.load(Ordering::Acquire) {
            if manager.requires_reopen_after_commit_error() {
                storage_reopen_required = true;
                break;
            }
            if let Err(error) = staging.reap_expired(256) {
                staging_error = Some(error);
                break;
            }
            if std::time::Instant::now() >= next_outcome_maintenance {
                next_outcome_maintenance =
                    std::time::Instant::now() + runtime.listener.outcome_maintenance_interval;
                if let Some(now_us) = crate::credentials::now_unix_micros()
                    && let Err(error) = manager
                        .prune_expired_outcomes(now_us, runtime.listener.outcome_maintenance_batch)
                {
                    outcome_maintenance_error = Some(error);
                    break;
                }
            }
            match listener.accept() {
                Ok((mut stream, _peer)) => {
                    let Some(permit) = limiter.try_acquire() else {
                        continue;
                    };
                    let worker_manager = manager;
                    let worker_staging = staging;
                    let worker_started = &on_started;
                    let worker_runtime = &runtime;
                    workers.push(scope.spawn(move || {
                        let _permit = permit;
                        if crate::gateway::unix_peer_uid(&stream).ok()
                            != Some(worker_runtime.listener.expected_gateway_uid)
                        {
                            return;
                        }
                        if stream
                            .set_read_timeout(Some(worker_runtime.listener.idle_timeout))
                            .and_then(|()| {
                                stream.set_write_timeout(Some(worker_runtime.listener.idle_timeout))
                            })
                            .is_err()
                        {
                            return;
                        }
                        let mut session =
                            crate::client_protocol::ClientConnectionSession::default();
                        loop {
                            match serve_gateway_client_session_with_executor(
                                &mut stream,
                                worker_manager,
                                worker_staging,
                                &mut session,
                                worker_runtime.admission,
                                &worker_runtime.executor,
                            ) {
                                Ok(GatewayClientServiceEvent::Disconnected) => return,
                                Ok(GatewayClientServiceEvent::Started {
                                    request_id,
                                    invocation,
                                    submission,
                                    executor: Some(executor),
                                }) => {
                                    let Ok(owner) = session.staging_owner() else {
                                        return;
                                    };
                                    let invocation_id = submission.invocation_id();
                                    if finish_started_after_callback(
                                        worker_staging,
                                        owner,
                                        invocation_id,
                                        || {
                                            worker_started(
                                                worker_manager,
                                                request_id,
                                                &mut stream,
                                                invocation,
                                                submission,
                                                *executor,
                                            );
                                        },
                                    )
                                    .is_err()
                                    {
                                        return;
                                    }
                                }
                                Ok(GatewayClientServiceEvent::Started { .. }) => return,
                                Err(_) => return,
                            }
                        }
                    }));
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    std::thread::sleep(runtime.listener.accept_poll_interval);
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
        storage_reopen_required |= manager.requires_reopen_after_commit_error();
        let mut worker_panicked = false;
        for worker in workers {
            worker_panicked |= worker.join().is_err();
        }
        if worker_panicked {
            Err(GatewayServiceListenerError::WorkerPanicked)
        } else if storage_reopen_required {
            Err(GatewayServiceListenerError::StorageReopenRequired)
        } else if let Some(error) = staging_error {
            Err(GatewayServiceListenerError::Staging(error))
        } else if let Some(error) = outcome_maintenance_error {
            Err(GatewayServiceListenerError::OutcomeMaintenance(error))
        } else if let Some(error) = accept_error {
            Err(GatewayServiceListenerError::Io(error))
        } else {
            Ok(())
        }
    })
}

fn finish_started_after_callback(
    staging: &crate::supervisor::StagingStore,
    owner: crate::supervisor::StagingOwner,
    invocation_id: [u8; 16],
    callback: impl FnOnce(),
) -> Result<(), crate::supervisor::StagingError> {
    callback();
    staging.finish_started(owner, invocation_id)
}

/// Own the StorageInstance/StorageManager lifecycle for a gateway service.
/// `bind_listener` must return a fresh private listener each time it is called;
/// the previous listener has been dropped before a reopen cycle reaches it.
/// The START callback receives the manager for the currently validated
/// database instance, including after a reopen.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub fn run_gateway_service_with_recovery<B, F>(
    mut instance: crate::bootstrap::StorageInstance,
    storage_config: crate::storage_manager::StorageManagerConfig,
    staging: &crate::supervisor::StagingStore,
    runtime: GatewayServiceRuntimeConfig,
    shutdown: Arc<AtomicBool>,
    mut bind_listener: B,
    on_started: F,
) -> Result<crate::bootstrap::StorageInstance, GatewayServiceRecoveryError>
where
    B: FnMut() -> io::Result<UnixListener>,
    F: for<'db> Fn(
            &StorageManager<'db>,
            u64,
            &mut UnixStream,
            crate::storage_manager::SupervisorInvocation<'db>,
            crate::supervisor::SealedSubmission,
            crate::executor_launcher::RunningExecutor,
        ) + Sync,
{
    loop {
        if shutdown.load(Ordering::Acquire) {
            return Ok(instance);
        }
        let listener = bind_listener().map_err(GatewayServiceRecoveryError::Bind)?;
        let (listener_result, requires_reopen) = {
            let manager = instance
                .storage_manager(storage_config)
                .map_err(GatewayServiceRecoveryError::Manager)?;
            let result = serve_gateway_service_listener(
                listener,
                &manager,
                staging,
                runtime.clone(),
                Arc::clone(&shutdown),
                &on_started,
            );
            (result, manager.requires_reopen_after_commit_error())
        };

        if requires_reopen {
            instance = instance
                .reopen()
                .map_err(GatewayServiceRecoveryError::Bootstrap)?;
            return match listener_result {
                Ok(()) if shutdown.load(Ordering::Acquire) => Ok(instance),
                Ok(()) | Err(GatewayServiceListenerError::StorageReopenRequired) => {
                    continue;
                }
                Err(error) => Err(GatewayServiceRecoveryError::Listener(error)),
            };
        }
        match listener_result {
            Ok(()) => return Ok(instance),
            Err(error) => return Err(GatewayServiceRecoveryError::Listener(error)),
        }
    }
}

/// Observe one executor while relaying its provisional result bytes to the
/// gateway service stream. This emits only data events; the caller must send a
/// terminal event after the storage-owned transaction decision is confirmed.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub fn observe_executor_and_relay_results(
    executor: &mut crate::executor_launcher::RunningExecutor,
    invocation: &crate::storage_manager::SupervisorInvocation<'_>,
    service: &mut UnixStream,
    start_request_id: u64,
) -> Result<std::process::ExitStatus, crate::storage_manager::SupervisorInvocationError> {
    executor.observe_exit_with_results(invocation, |payload| {
        write_gateway_result_data_event(service, start_request_id, payload)
    })
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn write_gateway_result_data_event(
    service: &mut impl Write,
    start_request_id: u64,
    payload: &[u8],
) -> io::Result<()> {
    let frame = IpcFrame {
        operation: Operation::GatewayResultData as u16,
        request_id: start_request_id,
        payload: payload.to_vec(),
    };
    crate::ipc::write_frame(service, &frame)
        .map_err(|error| io::Error::new(io::ErrorKind::BrokenPipe, error.to_string()))
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
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    {
        dispatch_gateway_client_request_inner(
            role, manager, staging, session, request, admission, None,
        )
    }
    #[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
    {
        dispatch_gateway_client_request_inner(role, manager, staging, session, request, admission)
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub fn dispatch_gateway_client_request_with_executor<'db>(
    role: ServiceRole,
    manager: &StorageManager<'db>,
    staging: &crate::supervisor::StagingStore,
    session: &mut crate::client_protocol::ClientConnectionSession,
    request: &IpcFrame,
    admission: GatewayAdmissionConfig,
    executor: &GatewayExecutorConfig,
) -> Result<GatewayClientDispatch<'db>, GatewayRequestError> {
    dispatch_gateway_client_request_inner(
        role,
        manager,
        staging,
        session,
        request,
        admission,
        Some(executor),
    )
}

fn dispatch_gateway_client_request_inner<'db>(
    role: ServiceRole,
    manager: &StorageManager<'db>,
    staging: &crate::supervisor::StagingStore,
    session: &mut crate::client_protocol::ClientConnectionSession,
    request: &IpcFrame,
    admission: GatewayAdmissionConfig,
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))] executor: Option<
        &GatewayExecutorConfig,
    >,
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
            #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
            if let Some(executor) = executor {
                let (response, invocation, worker) = session
                    .handle_start_request_with_executor(
                        manager,
                        staging,
                        frame(7, request.payload.clone()),
                        admission,
                        executor,
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
                    executor: Some(Box::new(worker)),
                });
            }
            let (response, invocation) = session
                .handle_start_request(
                    manager,
                    staging,
                    frame(7, request.payload.clone()),
                    admission
                        .retention_deadline_us()
                        .map_err(|_| GatewayRequestError::ClockUnavailable)?,
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
                #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
                executor: None,
            });
        }
        Operation::ClientGetInvocationStatus => session
            .handle_status_request(manager, staging, frame(8, request.payload.clone()))
            .map_err(GatewayRequestError::Client)?,
        Operation::ClientCancelInvocation => session
            .handle_cancel_prepared_request(manager, staging, frame(9, request.payload.clone()))
            .map_err(GatewayRequestError::Client)?,
        Operation::SubmitAdministrativeOperation => session
            .handle_admin_request(
                manager,
                frame(10, request.payload.clone()),
                admission
                    .retention_deadline_us()
                    .map_err(|_| GatewayRequestError::ClockUnavailable)?,
            )
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
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    {
        serve_gateway_client_session_inner(stream, manager, staging, session, admission, None)
    }
    #[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
    {
        serve_gateway_client_session_inner(stream, manager, staging, session, admission)
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub fn serve_gateway_client_session_with_executor<'db, S: Read + Write>(
    stream: &mut S,
    manager: &StorageManager<'db>,
    staging: &crate::supervisor::StagingStore,
    session: &mut crate::client_protocol::ClientConnectionSession,
    admission: GatewayAdmissionConfig,
    executor: &GatewayExecutorConfig,
) -> Result<GatewayClientServiceEvent<'db>, SupervisorConnectionError> {
    serve_gateway_client_session_inner(stream, manager, staging, session, admission, Some(executor))
}

fn serve_gateway_client_session_inner<'db, S: Read + Write>(
    stream: &mut S,
    manager: &StorageManager<'db>,
    staging: &crate::supervisor::StagingStore,
    session: &mut crate::client_protocol::ClientConnectionSession,
    admission: GatewayAdmissionConfig,
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))] executor: Option<
        &GatewayExecutorConfig,
    >,
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
        let dispatched = match dispatch_gateway_client_request_inner(
            ServiceRole::Gateway,
            manager,
            staging,
            session,
            &request,
            admission,
            #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
            executor,
        ) {
            Ok(dispatched) => dispatched,
            Err(error) => {
                let error_response = IpcFrame {
                    operation: Operation::GatewayClientError as u16,
                    request_id: request.request_id,
                    payload: sanitized_gateway_error_payload(&error),
                };
                let write_result = crate::ipc::write_frame(stream, &error_response);
                close_gateway_client_session(manager, staging, session)?;
                write_result.map_err(SupervisorConnectionError::Frame)?;
                return Err(SupervisorConnectionError::Gateway(error));
            }
        };
        match dispatched {
            GatewayClientDispatch::Response(mut response) => {
                let sensitive =
                    response.operation == Operation::SubmitAdministrativeOperation as u16;
                let write_result = crate::ipc::write_frame(stream, &response);
                if sensitive {
                    response.payload.zeroize();
                }
                if let Err(error) = write_result {
                    close_gateway_client_session(manager, staging, session)?;
                    return Err(SupervisorConnectionError::Frame(error));
                }
            }
            GatewayClientDispatch::Started {
                response,
                invocation,
                submission,
                #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
                executor,
            } => {
                if let Err(error) = crate::ipc::write_frame(stream, &response) {
                    close_gateway_client_session(manager, staging, session)?;
                    return Err(SupervisorConnectionError::Frame(error));
                }
                return Ok(GatewayClientServiceEvent::Started {
                    request_id: response.request_id,
                    invocation,
                    submission,
                    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
                    executor,
                });
            }
        }
    }
}

fn sanitized_gateway_error_payload(error: &GatewayRequestError) -> Vec<u8> {
    use crate::client_protocol::ClientSessionError;
    let code = match error {
        GatewayRequestError::InvalidPayload => {
            crate::gateway::GatewayClientErrorCode::InvalidRequest
        }
        GatewayRequestError::ClockUnavailable => {
            crate::gateway::GatewayClientErrorCode::Unavailable
        }
        GatewayRequestError::Dispatch(DispatchError::OperationNotAllowed { .. }) => {
            crate::gateway::GatewayClientErrorCode::InvalidState
        }
        GatewayRequestError::Dispatch(DispatchError::UnknownOperation(_)) => {
            crate::gateway::GatewayClientErrorCode::InvalidRequest
        }
        GatewayRequestError::Client(ClientSessionError::AuthenticationRequired) => {
            crate::gateway::GatewayClientErrorCode::Unauthenticated
        }
        GatewayRequestError::Client(
            ClientSessionError::WrongMode
            | ClientSessionError::InvalidHello
            | ClientSessionError::HelloAlreadyAccepted
            | ClientSessionError::AuthenticationOutOfOrder
            | ClientSessionError::AlreadyAuthenticated,
        ) => crate::gateway::GatewayClientErrorCode::InvalidState,
        GatewayRequestError::Client(
            ClientSessionError::InvalidControlPayload(_) | ClientSessionError::UnsupportedRequest,
        ) => crate::gateway::GatewayClientErrorCode::InvalidRequest,
        GatewayRequestError::Client(ClientSessionError::ResourceLimit) => {
            crate::gateway::GatewayClientErrorCode::ResourceLimit
        }
        GatewayRequestError::Client(_)
        | GatewayRequestError::Start(_)
        | GatewayRequestError::Staging(_) => crate::gateway::GatewayClientErrorCode::Unavailable,
    };
    code.payload().to_vec()
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
    let invocation_header_bytes = SUPERVISOR_INVOCATION_CONTROL_REQUEST_HEADER_BYTES as usize;
    let version_offset = SUPERVISOR_INVOCATION_CONTROL_REQUEST_VERSION_OFFSET as usize;
    if request.payload.len() < invocation_header_bytes
        || u16::from_be_bytes([
            request.payload[version_offset],
            request.payload[version_offset + 1],
        ]) != SUPERVISOR_PAYLOAD_VERSION
    {
        return Err(SupervisorRequestError::InvalidPayload);
    }
    let payload = match operation {
        Operation::SupervisorGetInvocationStatus
            if request.payload.len() == invocation_header_bytes =>
        {
            let invocation_offset =
                SUPERVISOR_INVOCATION_CONTROL_REQUEST_INVOCATION_ID_OFFSET as usize;
            let invocation_id: [u8; 16] = request.payload
                [invocation_offset..invocation_offset + 16]
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
        Operation::SupervisorCancelInvocation
            if request.payload.len() == SUPERVISOR_ADMIN_CANCEL_REQUEST_HEADER_BYTES as usize =>
        {
            let invocation_offset = SUPERVISOR_ADMIN_CANCEL_REQUEST_INVOCATION_ID_OFFSET as usize;
            let invocation_id: [u8; 16] = request.payload
                [invocation_offset..invocation_offset + 16]
                .try_into()
                .map_err(|_| SupervisorRequestError::InvalidPayload)?;
            let reason_offset = SUPERVISOR_ADMIN_CANCEL_REQUEST_REASON_CODE_OFFSET as usize;
            let reason_code = u16::from_be_bytes([
                request.payload[reason_offset],
                request.payload[reason_offset + 1],
            ]);
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
    let header_bytes = SUPERVISOR_INVOCATION_CONTROL_REQUEST_HEADER_BYTES as usize;
    let version_offset = SUPERVISOR_INVOCATION_CONTROL_REQUEST_VERSION_OFFSET as usize;
    if !matches!(
        operation,
        Operation::ClientGetInvocationStatus | Operation::ClientCancelInvocation
    ) || request.payload.len() != header_bytes
        || u16::from_be_bytes([
            request.payload[version_offset],
            request.payload[version_offset + 1],
        ]) != SUPERVISOR_PAYLOAD_VERSION
    {
        return Err(SupervisorRequestError::InvalidPayload);
    }
    let invocation_offset = SUPERVISOR_INVOCATION_CONTROL_REQUEST_INVOCATION_ID_OFFSET as usize;
    let invocation_id: [u8; 16] = request.payload[invocation_offset..invocation_offset + 16]
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

#[cfg(all(test, target_os = "linux", target_arch = "x86_64"))]
mod worker_result_event_tests {
    use super::{finish_started_after_callback, write_gateway_result_data_event};
    use crate::ipc::{IpcFrame, Operation, read_frame};
    use std::io::Cursor;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    #[test]
    fn workload_class_selects_the_configured_cgroup_profile() {
        let operational = crate::linux_sandbox::WorkerResourceLimits {
            cpu_quota_micros: 100_000,
            cpu_period_micros: 100_000,
            memory_max_bytes: 64 * 1024 * 1024,
            max_processes: 16,
        };
        let analytical = crate::linux_sandbox::WorkerResourceLimits {
            cpu_quota_micros: 200_000,
            cpu_period_micros: 100_000,
            memory_max_bytes: 512 * 1024 * 1024,
            max_processes: 32,
        };
        let config = super::GatewayExecutorConfig {
            cgroup_parent: std::env::temp_dir(),
            resource_limits: operational,
            analytical_resource_limits: Some(analytical),
            launch: crate::executor_launcher::ExecutorLaunchConfig {
                executor_path: std::env::current_exe().unwrap(),
                optimizer_path: std::env::current_exe().unwrap(),
                scratch_parent: std::env::temp_dir(),
                runtime_read_execute: Vec::new(),
                runtime_read_only: Vec::new(),
                cleanup_timeout: Duration::from_millis(10),
            },
        };
        assert_eq!(
            config.resource_limits_for(crate::supervisor::WorkloadClass::Operational),
            operational
        );
        assert_eq!(
            config.resource_limits_for(crate::supervisor::WorkloadClass::Analytical),
            analytical
        );
        assert_eq!(
            super::GatewayExecutorConfig {
                analytical_resource_limits: None,
                ..config
            }
            .resource_limits_for(crate::supervisor::WorkloadClass::Analytical),
            operational
        );
    }

    #[test]
    fn outcome_retention_period_is_applied_from_each_request_time() {
        let before = crate::credentials::now_unix_micros().expect("clock");
        let config = super::GatewayAdmissionConfig {
            retention_period_us: 1_000_000,
            max_rows: 1,
            max_bytes: 1,
        };
        let deadline = config.retention_deadline_us().expect("retention deadline");
        assert!(deadline >= before + 1_000_000);
        assert!(deadline < before + 1_100_000);
        assert_eq!(
            super::GatewayAdmissionConfig {
                retention_period_us: 0,
                ..config
            }
            .retention_deadline_us(),
            Err(super::RetentionDeadlineError::InvalidPeriod)
        );
        assert_eq!(
            super::GatewayAdmissionConfig {
                retention_period_us: i64::MAX,
                ..config
            }
            .retention_deadline_us(),
            Ok(i64::MAX)
        );
    }

    #[test]
    fn started_callback_releases_reservation_before_same_session_resumes() {
        use crate::supervisor::{
            ChunkKind, PrepareSubmission, StagingLimits, StagingOwner, StagingStore,
        };
        use sha2::{Digest, Sha256};

        let staging = StagingStore::new(StagingLimits::default(), Arc::new(AcceptMetadata))
            .expect("staging store");
        let owner = StagingOwner::authenticated([3; 16], [4; 16]);
        let artifact = b"a";
        let parameters: &[u8] = b"";
        let reservation = staging
            .prepare(
                owner,
                PrepareSubmission {
                    manifest_version: 1,
                    manifest: b"manifest".to_vec(),
                    artifact_bytes: artifact.len() as u64,
                    artifact_sha256: Sha256::digest(artifact).into(),
                    scope_version: 1,
                    requested_scope: {
                        let mut scope = b"PSC1\0\x01\x02\x02".to_vec();
                        scope.extend([1; 16]);
                        scope
                    },
                    parameter_bytes: 0,
                    parameter_sha256: Sha256::digest(parameters).into(),
                    deadline_ms: 1000,
                    workload_class: crate::supervisor::WorkloadClass::Operational,
                },
            )
            .expect("prepare");
        staging
            .upload_chunk(
                owner,
                reservation.invocation_id,
                ChunkKind::Artifact,
                0,
                artifact,
            )
            .expect("upload");
        staging
            .seal(owner, reservation.invocation_id)
            .expect("seal");
        staging
            .start_with(owner, reservation.invocation_id, |_| Ok::<_, ()>(()))
            .expect("start");

        finish_started_after_callback(&staging, owner, reservation.invocation_id, || {})
            .expect("finish");
        let next = staging
            .prepare(
                owner,
                PrepareSubmission {
                    manifest_version: 1,
                    manifest: b"manifest".to_vec(),
                    artifact_bytes: artifact.len() as u64,
                    artifact_sha256: Sha256::digest(artifact).into(),
                    scope_version: 1,
                    requested_scope: {
                        let mut scope = b"PSC1\0\x01\x02\x02".to_vec();
                        scope.extend([1; 16]);
                        scope
                    },
                    parameter_bytes: 0,
                    parameter_sha256: Sha256::digest(parameters).into(),
                    deadline_ms: 1000,
                    workload_class: crate::supervisor::WorkloadClass::Operational,
                },
            )
            .expect("connection can prepare another invocation");
        assert_ne!(next.invocation_id, reservation.invocation_id);
    }

    #[test]
    fn result_relay_emits_only_correlated_gateway_data_events() {
        let mut encoded = Cursor::new(Vec::new());
        write_gateway_result_data_event(&mut encoded, 17, b"provisional").unwrap();
        encoded.set_position(0);
        assert_eq!(
            read_frame(&mut encoded).unwrap(),
            Some(IpcFrame {
                operation: Operation::GatewayResultData as u16,
                request_id: 17,
                payload: b"provisional".to_vec(),
            })
        );
        assert_eq!(read_frame(&mut encoded).unwrap(), None);
    }

    struct AcceptMetadata;
    impl crate::supervisor::SubmissionMetadataValidator for AcceptMetadata {
        fn supports_versions(&self, manifest_version: u32, scope_version: u32) -> bool {
            manifest_version == 1 && scope_version == 1
        }

        fn validate(&self, _metadata: &crate::supervisor::SubmissionMetadata) -> bool {
            true
        }
    }

    #[test]
    fn gateway_service_reopens_quarantined_instance_before_rebinding() {
        let fixture_id = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "prdb-service-recovery-{}-{}",
            std::process::id(),
            fixture_id.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&root);
        let mut bootstrap = crate::bootstrap::initialize(&root, "service_admin", i64::MAX)
            .expect("initialize service instance");
        bootstrap.take_secret().expect("take bootstrap secret");
        let instance = bootstrap
            .acknowledge()
            .expect("publish ready service instance");
        crate::storage_manager::mark_database_uncertain(instance.database());

        let socket_path = root.with_extension("gateway.sock");
        let listener_socket_path = socket_path.clone();
        let bind_count = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        let bind_count_for_listener = Arc::clone(&bind_count);
        let shutdown_for_listener = Arc::clone(&shutdown);
        let bind_listener = move || {
            let count = bind_count_for_listener.fetch_add(1, Ordering::SeqCst);
            if listener_socket_path.exists() {
                std::fs::remove_file(&listener_socket_path)?;
            }
            let listener = super::bind_gateway_service_socket(&listener_socket_path)?;
            if count == 1 {
                shutdown_for_listener.store(true, Ordering::Release);
            }
            Ok(listener)
        };
        let staging = crate::supervisor::StagingStore::new(
            crate::supervisor::StagingLimits::default(),
            Arc::new(AcceptMetadata),
        )
        .expect("build staging store");
        let runtime = super::GatewayServiceRuntimeConfig {
            admission: super::GatewayAdmissionConfig {
                retention_period_us: i64::MAX,
                max_rows: 1,
                max_bytes: 64,
            },
            executor: super::GatewayExecutorConfig {
                cgroup_parent: root.clone(),
                resource_limits: crate::linux_sandbox::WorkerResourceLimits {
                    cpu_quota_micros: 100_000,
                    cpu_period_micros: 100_000,
                    memory_max_bytes: 64 * 1024 * 1024,
                    max_processes: 16,
                },
                analytical_resource_limits: None,
                launch: crate::executor_launcher::ExecutorLaunchConfig {
                    executor_path: std::env::current_exe().unwrap(),
                    optimizer_path: std::env::current_exe().unwrap(),
                    scratch_parent: std::env::temp_dir(),
                    runtime_read_execute: Vec::new(),
                    runtime_read_only: Vec::new(),
                    cleanup_timeout: Duration::from_millis(100),
                },
            },
            listener: super::GatewayServiceListenerConfig {
                expected_gateway_uid: unsafe { libc::geteuid() },
                max_connections: 1,
                idle_timeout: Duration::from_secs(1),
                accept_poll_interval: Duration::from_millis(1),
                outcome_maintenance_interval: Duration::from_secs(60),
                outcome_maintenance_batch: crate::outcome::MAX_OUTCOME_PRUNE_BATCH,
            },
        };

        let instance = super::run_gateway_service_with_recovery(
            instance,
            crate::storage_manager::StorageManagerConfig::default(),
            &staging,
            runtime,
            shutdown,
            bind_listener,
            |_manager, _request_id, _stream, _invocation, _submission, _executor| {},
        )
        .expect("reopen and restart gateway listener");
        assert_eq!(bind_count.load(Ordering::SeqCst), 2);
        let manager = instance
            .storage_manager(crate::storage_manager::StorageManagerConfig::default())
            .expect("build manager after recovery");
        assert!(!manager.requires_reopen_after_commit_error());
        drop(manager);
        drop(instance);
        let _ = std::fs::remove_file(&socket_path);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn idle_gateway_service_periodically_reaps_expired_staging() {
        use crate::supervisor::{
            ChunkKind, PrepareSubmission, StagingLimits, StagingOwner, StagingStore, WorkloadClass,
        };
        use sha2::{Digest, Sha256};

        let root = std::env::temp_dir().join(format!("prdb-service-reaper-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let mut bootstrap = crate::bootstrap::initialize(&root, "reaper_admin", i64::MAX)
            .expect("initialize service instance");
        bootstrap.take_secret().expect("take bootstrap secret");
        let instance = bootstrap
            .acknowledge()
            .expect("publish ready service instance");
        let manager = instance
            .storage_manager(crate::storage_manager::StorageManagerConfig::default())
            .expect("build storage manager");
        let mut expired = manager
            .begin_invocation([73; 16], [74; 16], 1, 1, 1)
            .expect("admit expired-outcome fixture");
        expired.abort().expect("persist expired outcome");
        assert!(
            crate::outcome::lookup_outcome(instance.database(), [73; 16], [74; 16], 0)
                .expect("lookup pre-maintenance outcome")
                .is_some()
        );

        let owner = StagingOwner::authenticated([1; 16], [2; 16]);
        let artifact = b"staged artifact";
        let mut requested_scope = b"PSC1\0\x01\x02\x02".to_vec();
        requested_scope.extend([1; 16]);
        let staging = StagingStore::new(
            StagingLimits {
                reservation_ttl: Duration::from_millis(5),
                ..StagingLimits::default()
            },
            Arc::new(AcceptMetadata),
        )
        .expect("build staging store");
        let reservation = staging
            .prepare(
                owner,
                PrepareSubmission {
                    manifest_version: 1,
                    manifest: b"manifest".to_vec(),
                    artifact_bytes: artifact.len() as u64,
                    artifact_sha256: Sha256::digest(artifact).into(),
                    scope_version: 1,
                    requested_scope,
                    parameter_bytes: 0,
                    parameter_sha256: Sha256::digest([]).into(),
                    deadline_ms: 30_000,
                    workload_class: WorkloadClass::Operational,
                },
            )
            .expect("prepare staging reservation");
        staging
            .upload_chunk(
                owner,
                reservation.invocation_id,
                ChunkKind::Artifact,
                0,
                artifact,
            )
            .expect("upload artifact");

        let socket_path = root.with_extension("reaper.sock");
        let listener = super::bind_gateway_service_socket(&socket_path).expect("bind listener");
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_signal = Arc::clone(&shutdown);
        let shutdown_thread = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(40));
            shutdown_signal.store(true, Ordering::Release);
        });
        let runtime = super::GatewayServiceRuntimeConfig {
            admission: super::GatewayAdmissionConfig {
                retention_period_us: i64::MAX,
                max_rows: 1,
                max_bytes: 64,
            },
            executor: super::GatewayExecutorConfig {
                cgroup_parent: root.clone(),
                resource_limits: crate::linux_sandbox::WorkerResourceLimits {
                    cpu_quota_micros: 100_000,
                    cpu_period_micros: 100_000,
                    memory_max_bytes: 64 * 1024 * 1024,
                    max_processes: 16,
                },
                analytical_resource_limits: None,
                launch: crate::executor_launcher::ExecutorLaunchConfig {
                    executor_path: std::env::current_exe().expect("current executable"),
                    optimizer_path: std::env::current_exe().expect("current executable"),
                    scratch_parent: std::env::temp_dir(),
                    runtime_read_execute: Vec::new(),
                    runtime_read_only: Vec::new(),
                    cleanup_timeout: Duration::from_millis(100),
                },
            },
            listener: super::GatewayServiceListenerConfig {
                expected_gateway_uid: unsafe { libc::geteuid() },
                max_connections: 1,
                idle_timeout: Duration::from_secs(1),
                accept_poll_interval: Duration::from_millis(1),
                outcome_maintenance_interval: Duration::from_millis(1),
                outcome_maintenance_batch: crate::outcome::MAX_OUTCOME_PRUNE_BATCH,
            },
        };
        super::serve_gateway_service_listener(
            listener,
            &manager,
            &staging,
            runtime,
            shutdown,
            |_manager, _request_id, _stream, _invocation, _submission, _executor| {},
        )
        .expect("serve idle gateway listener");
        shutdown_thread.join().expect("join shutdown thread");
        assert!(
            crate::outcome::lookup_outcome(instance.database(), [73; 16], [74; 16], 0)
                .expect("lookup after service maintenance")
                .is_none()
        );
        assert_eq!(
            staging
                .close_connection(owner)
                .expect("close expired owner"),
            crate::supervisor::ConnectionClose::Discarded(Vec::new())
        );

        drop(manager);
        drop(instance);
        let _ = std::fs::remove_file(socket_path);
        std::fs::remove_dir_all(root).unwrap();
    }
}
