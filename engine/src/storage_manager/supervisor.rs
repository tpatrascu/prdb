use super::admission::*;
use super::*;
use crate::credentials::TOKEN_SECRET_BYTES;
use crate::metadata_discovery::{
    InformationSchemaColumnsCursor, InformationSchemaColumnsScanPage,
    InformationSchemaSchemataPage, InformationSchemaTablesCursor, InformationSchemaTablesScanPage,
};
use crate::transaction::{InvocationTransaction, TransactionError};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// One token-authenticated invocation owned by the trusted supervisor-facing
/// storage service. The transaction cannot be finalized through executor or
/// planner methods and is aborted if this owner drops it without a decision.
pub struct SupervisorInvocation<'db> {
    pub(super) invocation_id: [u8; 16],
    pub(super) principal_id: [u8; 16],
    pub(super) role_ids: HashSet<[u8; 16]>,
    pub(super) transaction: Arc<Mutex<Option<InvocationTransaction<'db>>>>,
    pub(super) channels: Arc<Mutex<SupervisorChannelState>>,
    pub(super) deadline: Option<std::time::Instant>,
    pub(super) abort_on_drop: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct SupervisorAdmissionRequest {
    pub invocation_id: [u8; 16],
    pub token_id: [u8; 16],
    pub retention_deadline_us: i64,
    pub max_rows: usize,
    pub max_bytes: usize,
    pub deadline: Option<std::time::Instant>,
    pub workload_class: crate::supervisor::WorkloadClass,
}

#[derive(Default)]
pub(super) struct SupervisorChannelState {
    pub(super) active_worker_channels: usize,
    pub(super) terminal_requested: bool,
    pub(super) worker_exit_observing: bool,
    pub(super) worker_exit_succeeded: bool,
}

pub(super) struct SupervisorWorkerChannelLease {
    pub(super) channels: Arc<Mutex<SupervisorChannelState>>,
}

#[derive(Debug)]
pub enum SupervisorInvocationError {
    Closed,
    WorkerChannelsActive,
    WorkerExitNotObserved,
    WorkerExitObservationActive,
    WorkerChannelIo(std::io::Error),
    WorkerProcessIo(std::io::Error),
    WorkerCgroupIo(std::io::Error),
    WorkerExited(std::process::ExitStatus),
    WorkerHandlerPanicked,
    Deadline(TransactionError),
    Transaction(TransactionError),
    Executor(crate::executor_ipc::ExecutorConnectionError),
    Planner(crate::planner_ipc::PlannerConnectionError),
    Supervisor(crate::supervisor_ipc::SupervisorConnectionError),
}

impl std::fmt::Display for SupervisorInvocationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "supervisor invocation failed: {self:?}")
    }
}

impl std::error::Error for SupervisorInvocationError {}

impl Drop for SupervisorWorkerChannelLease {
    fn drop(&mut self) {
        let mut channels = self
            .channels
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        channels.active_worker_channels = channels.active_worker_channels.saturating_sub(1);
    }
}

impl SupervisorInvocation<'_> {
    pub fn invocation_id(&self) -> [u8; 16] {
        self.invocation_id
    }

    pub fn principal_id(&self) -> [u8; 16] {
        self.principal_id
    }

    pub fn role_ids(&self) -> &HashSet<[u8; 16]> {
        &self.role_ids
    }

    pub(super) fn with_owned_transaction<T>(
        &self,
        operation: impl FnOnce(&mut InvocationTransaction<'_>) -> Result<T, TransactionError>,
    ) -> Result<T, SupervisorInvocationError> {
        let mut owner = match self.transaction.lock() {
            Ok(owner) => owner,
            Err(poisoned) => {
                let mut owner = poisoned.into_inner();
                if let Some(mut transaction) = owner.take() {
                    let _ = transaction.abort();
                }
                return Err(SupervisorInvocationError::Closed);
            }
        };
        let transaction = owner.as_mut().ok_or(SupervisorInvocationError::Closed)?;
        operation(transaction).map_err(SupervisorInvocationError::Transaction)
    }

    /// Read one invocation-pinned page of schema metadata through the trusted
    /// storage discovery API. Executor channels remain separate and opaque.
    pub fn scan_information_schema_schemata(
        &self,
        page_limit: usize,
    ) -> Result<InformationSchemaSchemataPage, SupervisorInvocationError> {
        self.with_owned_transaction(|transaction| {
            transaction.scan_information_schema_schemata(page_limit, None)
        })
    }

    /// Read one invocation-pinned page of table metadata through trusted
    /// storage discovery.
    pub fn scan_information_schema_tables(
        &self,
        page_limit: usize,
        continuation: Option<InformationSchemaTablesCursor>,
    ) -> Result<InformationSchemaTablesScanPage, SupervisorInvocationError> {
        self.with_owned_transaction(|transaction| {
            transaction.scan_information_schema_tables(page_limit, continuation)
        })
    }

    /// Read one invocation-pinned page of column metadata through trusted
    /// storage discovery.
    pub fn scan_information_schema_columns(
        &self,
        page_limit: usize,
        continuation: Option<InformationSchemaColumnsCursor>,
    ) -> Result<InformationSchemaColumnsScanPage, SupervisorInvocationError> {
        self.with_owned_transaction(|transaction| {
            transaction.scan_information_schema_columns(page_limit, continuation)
        })
    }

    pub(super) fn enter_worker_channel(
        &self,
    ) -> Result<SupervisorWorkerChannelLease, SupervisorInvocationError> {
        let mut channels = match self.channels.lock() {
            Ok(channels) => channels,
            Err(poisoned) => {
                let mut channels = poisoned.into_inner();
                channels.terminal_requested = true;
                self.abort_owned_invocation();
                return Err(SupervisorInvocationError::Closed);
            }
        };
        if channels.terminal_requested
            || channels.worker_exit_observing
            || channels.worker_exit_succeeded
        {
            return Err(SupervisorInvocationError::Closed);
        }
        channels.active_worker_channels = channels
            .active_worker_channels
            .checked_add(1)
            .ok_or(SupervisorInvocationError::Closed)?;
        Ok(SupervisorWorkerChannelLease {
            channels: Arc::clone(&self.channels),
        })
    }

    pub(super) fn abort_owned_invocation(&self) {
        let mut owner = self
            .transaction
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(mut transaction) = owner.take() {
            let _ = transaction.abort();
        }
    }

    /// Poll cancellation and transaction age while worker channels are idle.
    /// A failed check durably aborts the invocation through its transaction.
    pub fn check_deadline(&self) -> Result<(), SupervisorInvocationError> {
        let mut owner = match self.transaction.lock() {
            Ok(owner) => owner,
            Err(poisoned) => {
                let mut owner = poisoned.into_inner();
                if let Some(mut transaction) = owner.take() {
                    let _ = transaction.abort();
                }
                return Err(SupervisorInvocationError::Closed);
            }
        };
        let transaction = owner.as_mut().ok_or(SupervisorInvocationError::Closed)?;
        transaction
            .check_deadline()
            .map_err(SupervisorInvocationError::Deadline)?;
        if self
            .deadline
            .is_some_and(|deadline| std::time::Instant::now() >= deadline)
        {
            transaction
                .abort()
                .map_err(SupervisorInvocationError::Deadline)?;
            return Err(SupervisorInvocationError::Deadline(
                TransactionError::Expired,
            ));
        }
        Ok(())
    }

    /// Check only hard age limits while a worker is draining cooperative
    /// cancellation. The executor gets a bounded window to poll cancellation
    /// and exit before the launcher forcefully reaps it.
    pub fn check_worker_deadline(&self) -> Result<(), SupervisorInvocationError> {
        let mut owner = match self.transaction.lock() {
            Ok(owner) => owner,
            Err(poisoned) => {
                let mut owner = poisoned.into_inner();
                if let Some(mut transaction) = owner.take() {
                    let _ = transaction.abort();
                }
                return Err(SupervisorInvocationError::Closed);
            }
        };
        let transaction = owner.as_mut().ok_or(SupervisorInvocationError::Closed)?;
        transaction
            .check_worker_deadline()
            .map_err(SupervisorInvocationError::Deadline)?;
        if self
            .deadline
            .is_some_and(|deadline| std::time::Instant::now() >= deadline)
        {
            transaction
                .abort()
                .map_err(SupervisorInvocationError::Deadline)?;
            return Err(SupervisorInvocationError::Deadline(
                TransactionError::Expired,
            ));
        }
        Ok(())
    }

    /// Whether cancellation has been accepted, including after an executor
    /// poll has already aborted and released the storage transaction.
    pub fn cancellation_requested(&self) -> Result<bool, SupervisorInvocationError> {
        let owner = self
            .transaction
            .lock()
            .map_err(|_| SupervisorInvocationError::Closed)?;
        let transaction = owner.as_ref().ok_or(SupervisorInvocationError::Closed)?;
        Ok(transaction.cancellation_requested())
    }

    /// Observe and reap a worker after its planner and executor channels have
    /// drained. A failed exit or process-inspection error durably aborts the
    /// invocation; the caller may request commit only after this returns Ok.
    pub fn observe_worker_exit(
        &self,
        worker: &mut std::process::Child,
    ) -> Result<std::process::ExitStatus, SupervisorInvocationError> {
        self.observe_worker_exit_inner(worker, None)
    }

    /// Observe a worker whose process was placed in `cgroup` before exec.
    /// The cgroup is killed and verified empty before successful exit can
    /// authorize terminal storage decisions.
    pub fn observe_worker_exit_in_cgroup(
        &self,
        worker: &mut std::process::Child,
        cgroup: crate::linux_sandbox::WorkerCgroup,
        cleanup_timeout: Duration,
    ) -> Result<std::process::ExitStatus, SupervisorInvocationError> {
        self.observe_worker_exit_inner(worker, Some((cgroup, cleanup_timeout)))
    }

    pub(super) fn observe_worker_exit_inner(
        &self,
        worker: &mut std::process::Child,
        mut cgroup: Option<(crate::linux_sandbox::WorkerCgroup, Duration)>,
    ) -> Result<std::process::ExitStatus, SupervisorInvocationError> {
        {
            let mut channels = match self.channels.lock() {
                Ok(channels) => channels,
                Err(poisoned) => {
                    let mut channels = poisoned.into_inner();
                    channels.terminal_requested = true;
                    self.abort_owned_invocation();
                    return Err(SupervisorInvocationError::Closed);
                }
            };
            if channels.terminal_requested {
                return Err(SupervisorInvocationError::Closed);
            }
            if channels.worker_exit_succeeded {
                return Err(SupervisorInvocationError::Closed);
            }
            if channels.active_worker_channels != 0 {
                return Err(SupervisorInvocationError::WorkerChannelsActive);
            }
            if channels.worker_exit_observing {
                return Err(SupervisorInvocationError::WorkerExitObservationActive);
            }
            channels.worker_exit_observing = true;
        }

        loop {
            match worker.try_wait() {
                Ok(Some(status)) if status.success() => {
                    if let Err(error) = Self::cleanup_worker_cgroup(&mut cgroup) {
                        self.finish_worker_exit_observation(false)?;
                        self.abort_owned_invocation();
                        return Err(error);
                    }
                    self.finish_worker_exit_observation(true)?;
                    return Ok(status);
                }
                Ok(Some(status)) => {
                    let cleanup_error = Self::cleanup_worker_cgroup(&mut cgroup).err();
                    self.finish_worker_exit_observation(false)?;
                    self.abort_owned_invocation();
                    if let Some(error) = cleanup_error {
                        return Err(error);
                    }
                    return Err(SupervisorInvocationError::WorkerExited(status));
                }
                Ok(None) => {
                    if let Err(error) = self.check_deadline() {
                        let _ = worker.kill();
                        let _ = worker.wait();
                        let cleanup_error = Self::cleanup_worker_cgroup(&mut cgroup).err();
                        self.finish_worker_exit_observation(false)?;
                        if let Some(cleanup_error) = cleanup_error {
                            self.abort_owned_invocation();
                            return Err(cleanup_error);
                        }
                        return Err(error);
                    }
                    std::thread::sleep(SUPERVISOR_IDLE_POLL_INTERVAL);
                }
                Err(error) => {
                    let _ = worker.kill();
                    let _ = worker.wait();
                    let cleanup_error = Self::cleanup_worker_cgroup(&mut cgroup).err();
                    self.finish_worker_exit_observation(false)?;
                    self.abort_owned_invocation();
                    if let Some(cleanup_error) = cleanup_error {
                        return Err(cleanup_error);
                    }
                    return Err(SupervisorInvocationError::WorkerProcessIo(error));
                }
            }
        }
    }

    pub(super) fn cleanup_worker_cgroup(
        cgroup: &mut Option<(crate::linux_sandbox::WorkerCgroup, Duration)>,
    ) -> Result<(), SupervisorInvocationError> {
        if let Some((cgroup, timeout)) = cgroup.take() {
            cgroup
                .cleanup(timeout)
                .map_err(SupervisorInvocationError::WorkerCgroupIo)?;
        }
        Ok(())
    }

    pub(super) fn finish_worker_exit_observation(
        &self,
        succeeded: bool,
    ) -> Result<(), SupervisorInvocationError> {
        let mut channels = match self.channels.lock() {
            Ok(channels) => channels,
            Err(poisoned) => {
                let mut channels = poisoned.into_inner();
                channels.worker_exit_observing = false;
                channels.terminal_requested = true;
                self.abort_owned_invocation();
                return Err(SupervisorInvocationError::Closed);
            }
        };
        channels.worker_exit_observing = false;
        channels.worker_exit_succeeded = succeeded;
        if !succeeded {
            channels.terminal_requested = true;
        }
        Ok(())
    }

    /// Serve executor, executor-planner, and optimizer-planner Unix streams
    /// concurrently against this invocation. Poll deadlines while streams are
    /// idle, stop every peer after handler failure, and drain all handlers.
    pub fn serve_worker_channels(
        &self,
        planner_stream: std::os::unix::net::UnixStream,
        executor_stream: std::os::unix::net::UnixStream,
        optimizer_planner_stream: std::os::unix::net::UnixStream,
    ) -> Result<(), SupervisorInvocationError> {
        self.serve_worker_channels_with_optimizer_planner_observer(
            planner_stream,
            executor_stream,
            optimizer_planner_stream,
            |_, _| {},
        )
    }

    /// Serve the production worker channels while observing optimizer planner
    /// requests immediately before storage dispatch. The callback runs on the
    /// optimizer-planner handler thread without holding the invocation lock.
    #[doc(hidden)]
    pub fn serve_worker_channels_with_optimizer_planner_observer<F>(
        &self,
        planner_stream: std::os::unix::net::UnixStream,
        executor_stream: std::os::unix::net::UnixStream,
        optimizer_planner_stream: std::os::unix::net::UnixStream,
        observer: F,
    ) -> Result<(), SupervisorInvocationError>
    where
        F: Fn(u16, u64) + Send + Sync,
    {
        self.serve_worker_channels_with_optimizer_planner_timing_observer(
            planner_stream,
            executor_stream,
            optimizer_planner_stream,
            observer,
            |_, _, _| {},
            false,
        )
    }

    /// Variant that also reports the duration of each optimizer planner
    /// request's trusted storage dispatch for confined workload profiling.
    #[doc(hidden)]
    pub fn serve_worker_channels_with_optimizer_planner_timing_observer<F, T>(
        &self,
        mut planner_stream: std::os::unix::net::UnixStream,
        mut executor_stream: std::os::unix::net::UnixStream,
        mut optimizer_planner_stream: std::os::unix::net::UnixStream,
        observer: F,
        timing_observer: T,
        measure_dispatch: bool,
    ) -> Result<(), SupervisorInvocationError>
    where
        F: Fn(u16, u64) + Send + Sync,
        T: Fn(u16, u64, Duration) + Send + Sync,
    {
        use std::net::Shutdown;
        let stop_streams = || -> std::io::Result<(
            std::os::unix::net::UnixStream,
            std::os::unix::net::UnixStream,
            std::os::unix::net::UnixStream,
        )> {
            Ok((
                planner_stream.try_clone()?,
                executor_stream.try_clone()?,
                optimizer_planner_stream.try_clone()?,
            ))
        };
        let (stop_planner, stop_executor, stop_optimizer) = match stop_streams() {
            Ok(streams) => streams,
            Err(error) => {
                self.abort_owned_invocation();
                return Err(SupervisorInvocationError::WorkerChannelIo(error));
            }
        };
        let planner_peers = match (stop_executor.try_clone(), stop_optimizer.try_clone()) {
            (Ok(executor), Ok(optimizer)) => (executor, optimizer),
            (Err(error), _) | (_, Err(error)) => {
                self.abort_owned_invocation();
                return Err(SupervisorInvocationError::WorkerChannelIo(error));
            }
        };
        let executor_peers = match (stop_planner.try_clone(), stop_optimizer.try_clone()) {
            (Ok(planner), Ok(optimizer)) => (planner, optimizer),
            (Err(error), _) | (_, Err(error)) => {
                self.abort_owned_invocation();
                return Err(SupervisorInvocationError::WorkerChannelIo(error));
            }
        };
        let optimizer_peers = match (stop_planner.try_clone(), stop_executor.try_clone()) {
            (Ok(planner), Ok(executor)) => (planner, executor),
            (Err(error), _) | (_, Err(error)) => {
                self.abort_owned_invocation();
                return Err(SupervisorInvocationError::WorkerChannelIo(error));
            }
        };

        std::thread::scope(|scope| {
            let (completed_tx, completed_rx) = std::sync::mpsc::channel();
            let planner_completed = completed_tx.clone();
            let planner = scope.spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    self.serve_planner(&mut planner_stream)
                }))
                .unwrap_or(Err(SupervisorInvocationError::WorkerHandlerPanicked));
                if result.is_err() {
                    let _ = planner_peers.0.shutdown(Shutdown::Both);
                    let _ = planner_peers.1.shutdown(Shutdown::Both);
                }
                let _ = planner_completed.send((0u8, result));
            });
            let executor_completed = completed_tx.clone();
            let executor = scope.spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    self.serve_executor(&mut executor_stream)
                }))
                .unwrap_or(Err(SupervisorInvocationError::WorkerHandlerPanicked));
                if result.is_err() {
                    let _ = executor_peers.0.shutdown(Shutdown::Both);
                    let _ = executor_peers.1.shutdown(Shutdown::Both);
                }
                let _ = executor_completed.send((1u8, result));
            });
            let optimizer_completed = completed_tx.clone();
            let optimizer = scope.spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let _lease = self.enter_worker_channel()?;
                    let mut observe_and_check = |operation, request_id| {
                        observer(operation, request_id);
                        self.check_worker_deadline().map_err(|error| match error {
                            SupervisorInvocationError::Deadline(transaction_error) => {
                                crate::planner_ipc::PlannerConnectionError::Request(
                                    crate::planner_ipc::PlannerRequestError::Transaction(
                                        transaction_error,
                                    ),
                                )
                            }
                            _ => crate::planner_ipc::PlannerConnectionError::SharedInvocationUnavailable,
                        })
                    };
                    let mut observe_dispatch = |operation, request_id, duration| {
                        timing_observer(operation, request_id, duration);
                    };
                    if measure_dispatch {
                        crate::planner_ipc::serve_planner_shared_with_timing_observer(
                            &mut optimizer_planner_stream,
                            &self.transaction,
                            &mut observe_and_check,
                            &mut observe_dispatch,
                            true,
                        )
                    } else {
                        crate::planner_ipc::serve_planner_shared_with_observer(
                            &mut optimizer_planner_stream,
                            &self.transaction,
                            &mut observe_and_check,
                        )
                    }
                    .map_err(SupervisorInvocationError::Planner)
                }))
                .unwrap_or(Err(SupervisorInvocationError::WorkerHandlerPanicked));
                if result.is_err() {
                    let _ = optimizer_peers.0.shutdown(Shutdown::Both);
                    let _ = optimizer_peers.1.shutdown(Shutdown::Both);
                }
                let _ = optimizer_completed.send((2u8, result));
            });
            drop(completed_tx);

            let mut results = [None, None, None];
            let mut completed = 0;
            let mut deadline_error = None;
            while completed < 3 {
                match completed_rx.recv_timeout(SUPERVISOR_IDLE_POLL_INTERVAL) {
                    Ok((index, result)) => {
                        completed += 1;
                        results[index as usize] = Some(result);
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        if let Err(error) = self.check_worker_deadline() {
                            deadline_error = Some(error);
                            let _ = stop_planner.shutdown(Shutdown::Both);
                            let _ = stop_executor.shutdown(Shutdown::Both);
                            let _ = stop_optimizer.shutdown(Shutdown::Both);
                            break;
                        }
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
            while completed < 3 {
                let Ok((index, result)) = completed_rx.recv() else {
                    break;
                };
                completed += 1;
                results[index as usize] = Some(result);
            }

            let planner_panicked = planner.join().is_err();
            let executor_panicked = executor.join().is_err();
            let optimizer_panicked = optimizer.join().is_err();
            if planner_panicked || executor_panicked || optimizer_panicked {
                self.abort_owned_invocation();
                return Err(SupervisorInvocationError::WorkerHandlerPanicked);
            }
            if let Some(error) = deadline_error {
                return Err(error);
            }
            if results.iter().flatten().any(|result| {
                matches!(
                    result,
                    Err(SupervisorInvocationError::WorkerHandlerPanicked)
                )
            }) {
                self.abort_owned_invocation();
                return Err(SupervisorInvocationError::WorkerHandlerPanicked);
            }
            for result in results {
                result.unwrap_or(Err(SupervisorInvocationError::Closed))?;
            }
            Ok(())
        })
    }

    pub fn serve_planner<S: std::io::Read + std::io::Write>(
        &self,
        stream: &mut S,
    ) -> Result<(), SupervisorInvocationError> {
        let _lease = self.enter_worker_channel()?;
        crate::planner_ipc::serve_planner_shared(stream, &self.transaction)
            .map_err(SupervisorInvocationError::Planner)
    }

    pub fn serve_executor<S: std::io::Read + std::io::Write>(
        &self,
        stream: &mut S,
    ) -> Result<(), SupervisorInvocationError> {
        let _lease = self.enter_worker_channel()?;
        crate::executor_ipc::serve_executor_shared(stream, &self.transaction)
            .map_err(SupervisorInvocationError::Executor)
    }

    pub fn serve_terminal<S: std::io::Read + std::io::Write>(
        &mut self,
        stream: &mut S,
    ) -> Result<(), SupervisorInvocationError> {
        let mut channels = match self.channels.lock() {
            Ok(channels) => channels,
            Err(poisoned) => {
                let mut channels = poisoned.into_inner();
                channels.terminal_requested = true;
                self.abort_owned_invocation();
                return Err(SupervisorInvocationError::Closed);
            }
        };
        if channels.terminal_requested {
            return Err(SupervisorInvocationError::Closed);
        }
        if channels.active_worker_channels != 0 {
            return Err(SupervisorInvocationError::WorkerChannelsActive);
        }
        if channels.worker_exit_observing {
            return Err(SupervisorInvocationError::WorkerExitObservationActive);
        }
        if !channels.worker_exit_succeeded {
            return Err(SupervisorInvocationError::WorkerExitNotObserved);
        }
        channels.terminal_requested = true;
        drop(channels);
        let mut transaction = match self.transaction.lock() {
            Ok(transaction) => transaction,
            Err(poisoned) => {
                let mut transaction = poisoned.into_inner();
                if let Some(mut invocation) = transaction.take() {
                    let _ = invocation.abort();
                }
                return Err(SupervisorInvocationError::Closed);
            }
        };
        crate::supervisor_ipc::serve_supervisor(stream, &mut transaction)
            .map_err(SupervisorInvocationError::Supervisor)
    }

    pub fn serve_control<S: std::io::Read + std::io::Write>(
        &self,
        manager: &StorageManager<'_>,
        stream: &mut S,
    ) -> Result<(), SupervisorInvocationError> {
        crate::supervisor_ipc::serve_supervisor_control(stream, manager, self.principal_id)
            .map_err(SupervisorInvocationError::Supervisor)
    }
}

impl Drop for SupervisorInvocation<'_> {
    fn drop(&mut self) {
        if !self.abort_on_drop {
            return;
        }
        let mut owner = self
            .transaction
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(mut transaction) = owner.take() {
            let _ = transaction.abort();
        }
    }
}

impl<'db> StorageManager<'db> {
    /// Admit a token-authenticated invocation into the supervisor-owned
    /// storage context used by planner, executor, and terminal IPC handlers.
    pub fn admit_supervisor_invocation(
        &self,
        invocation_id: [u8; 16],
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        retention_deadline_us: i64,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<SupervisorInvocation<'db>, TransactionError> {
        self.admit_supervisor_invocation_inner(
            SupervisorAdmissionRequest {
                invocation_id,
                token_id,
                retention_deadline_us,
                max_rows,
                max_bytes,
                deadline: None,
                workload_class: crate::supervisor::WorkloadClass::Operational,
            },
            token_secret,
            None,
        )
    }

    /// Admit a token-authenticated supervisor invocation with an additional
    /// caller-requested execution deadline bounded by trusted scheduling.
    pub fn admit_supervisor_invocation_with_deadline(
        &self,
        request: SupervisorAdmissionRequest,
        token_secret: &[u8; TOKEN_SECRET_BYTES],
    ) -> Result<SupervisorInvocation<'db>, TransactionError> {
        if request.deadline.is_none() {
            return Err(TransactionError::InvalidContext);
        }
        self.admit_supervisor_invocation_inner(request, token_secret, None)
    }

    /// Admit an invocation and intersect its catalog-derived authority with
    /// the client's decoded requested capability set.
    pub fn admit_supervisor_invocation_with_deadline_and_scope(
        &self,
        request: SupervisorAdmissionRequest,
        requested_scope: crate::submission_scope::RequestedScope,
        token_secret: &[u8; TOKEN_SECRET_BYTES],
    ) -> Result<SupervisorInvocation<'db>, TransactionError> {
        if request.deadline.is_none() {
            return Err(TransactionError::InvalidContext);
        }
        self.admit_supervisor_invocation_inner(request, token_secret, Some(requested_scope))
    }

    pub(super) fn admit_supervisor_invocation_inner(
        &self,
        request: SupervisorAdmissionRequest,
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        requested_scope: Option<crate::submission_scope::RequestedScope>,
    ) -> Result<SupervisorInvocation<'db>, TransactionError> {
        let principal_id = lookup_credential_principal(self.database, request.token_id)?;
        let (mut transaction, role_ids) =
            self.begin_token_authenticated_invocation_in_class(request, token_secret)?;
        if let Some(requested_scope) = requested_scope {
            transaction.restrict_to_requested_scope(requested_scope);
        }
        Ok(SupervisorInvocation {
            invocation_id: request.invocation_id,
            principal_id,
            role_ids,
            transaction: Arc::new(Mutex::new(Some(transaction))),
            channels: Arc::new(Mutex::new(SupervisorChannelState::default())),
            deadline: request.deadline,
            abort_on_drop: true,
        })
    }
}
