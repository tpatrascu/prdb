//! Confined launch of a sealed native query artifact.
//!
//! This module only prepares and starts the executor host. The executor
//! performs ABI validation and loads the artifact after its post-exec checks.

use crate::linux_sandbox::{
    LandlockPolicy, LandlockRuleset, WorkerCgroup, configure_worker_command,
};
use crate::storage_manager::{SupervisorInvocation, SupervisorInvocationError};
use crate::supervisor::SealedSubmission;
use std::collections::{HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

const COOPERATIVE_CANCELLATION_GRACE: Duration = Duration::from_millis(500);

#[derive(Clone, Debug)]
pub struct ExecutorLaunchConfig {
    pub executor_path: PathBuf,
    pub optimizer_path: PathBuf,
    pub scratch_parent: PathBuf,
    /// Runtime files/directories needed by the dynamic loader and executor.
    /// The caller must supply the exact deployment profile, such as the
    /// pinned libc and its loader cache.
    pub runtime_read_execute: Vec<PathBuf>,
    pub runtime_read_only: Vec<PathBuf>,
    /// Shared trusted broker. Absence keeps spill disabled.
    pub spill_broker: Option<crate::spill::SpillBroker>,
    /// Host-level ceiling for per-invocation output budget.
    pub max_total_result_bytes: u64,
    /// Host-level ceiling for per-statement SQL index-probe candidates.
    pub max_sql_index_probe_candidates: u32,
    pub cleanup_timeout: Duration,
}

impl ExecutorLaunchConfig {
    pub fn clamp_profile(
        &self,
        profile: &crate::workload_profile::WorkloadProfileLimits,
    ) -> crate::workload_profile::WorkloadProfileLimits {
        let mut clamped = profile.clone();
        clamped.max_total_result_bytes = clamped
            .max_total_result_bytes
            .min(self.max_total_result_bytes);
        clamped.max_sql_index_probe_candidates = clamped
            .max_sql_index_probe_candidates
            .min(self.max_sql_index_probe_candidates);
        clamped
    }
}

#[derive(Debug)]
pub struct ExecutorLaunchError {
    pub launch_error: io::Error,
    pub cleanup_error: Option<io::Error>,
}

fn wait_for_optimizer_exit(
    optimizer: &mut Child,
    timeout: Duration,
) -> Result<(), SupervisorInvocationError> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match optimizer.try_wait() {
            Ok(Some(status)) if status.success() => return Ok(()),
            Ok(Some(status)) => {
                return Err(SupervisorInvocationError::WorkerProcessIo(
                    io::Error::other(format!("optimizer exited with status {status}")),
                ));
            }
            Ok(None) if std::time::Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Ok(None) => {
                let _ = optimizer.kill();
                let _ = optimizer.wait();
                return Err(SupervisorInvocationError::WorkerProcessIo(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "optimizer did not exit after its channels closed",
                )));
            }
            Err(error) => {
                let _ = optimizer.kill();
                let _ = optimizer.wait();
                return Err(SupervisorInvocationError::WorkerProcessIo(error));
            }
        }
    }
}

impl std::fmt::Display for ExecutorLaunchError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "executor launch failed: {}", self.launch_error)?;
        if let Some(error) = &self.cleanup_error {
            write!(formatter, "; cgroup cleanup failed: {error}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ExecutorLaunchError {}

/// A started executor and its private artifact directory. `observe_exit`
/// serves storage channels and drains provisional result data before exit
/// observation; use `observe_exit_with_results` when those bytes need relay.
pub struct RunningExecutor {
    child: Child,
    optimizer_child: Option<Child>,
    cgroup: Option<WorkerCgroup>,
    artifact_directory: Option<PrivateArtifactDirectory>,
    worker_channels: Option<(UnixStream, UnixStream, UnixStream)>,
    spill_stream: Option<UnixStream>,
    spill_session: Option<Arc<SpillSession>>,
    result_stream: Option<UnixStream>,
    cleanup_timeout: Duration,
    max_total_result_bytes: u64,
}

/// Fully prepared command and staged inputs, ready to spawn after the storage
/// admission callback succeeds.
pub struct PreparedExecutor {
    command: Command,
    optimizer_command: Option<Command>,
    parameters: Option<File>,
    sql_data: Option<File>,
    #[allow(dead_code)]
    profile_file: Option<File>,
    cgroup: Option<WorkerCgroup>,
    artifact_directory: Option<PrivateArtifactDirectory>,
    worker_channels: Option<WorkerChannelPair>,
    spill_session: Option<Arc<SpillSession>>,
    cleanup_timeout: Duration,
    max_total_result_bytes: u64,
}

struct SpillSession {
    broker: crate::spill::SpillBroker,
    package: crate::spill::SpillPackage,
    closed: std::sync::atomic::AtomicBool,
}

impl SpillSession {
    fn close(&self) -> Result<(), crate::spill::SpillError> {
        if self.closed.swap(true, std::sync::atomic::Ordering::AcqRel) {
            return Ok(());
        }
        self.broker.close_package(self.package)
    }
}

impl Drop for SpillSession {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

struct WorkerChannelPair {
    planner_worker: UnixStream,
    planner_supervisor: UnixStream,
    executor_worker: UnixStream,
    executor_supervisor: UnixStream,
    result_worker: UnixStream,
    result_supervisor: UnixStream,
    spill_worker: UnixStream,
    spill_supervisor: UnixStream,
    optimizer_submission_worker: UnixStream,
    optimizer_submission_optimizer: UnixStream,
    optimizer_planner_optimizer: UnixStream,
    optimizer_planner_supervisor: UnixStream,
}

impl WorkerChannelPair {
    fn new() -> io::Result<Self> {
        let (planner_worker, planner_supervisor) = UnixStream::pair()?;
        let (executor_worker, executor_supervisor) = UnixStream::pair()?;
        let (result_worker, result_supervisor) = UnixStream::pair()?;
        let (spill_worker, spill_supervisor) = UnixStream::pair()?;
        let (optimizer_submission_worker, optimizer_submission_optimizer) = UnixStream::pair()?;
        let (optimizer_planner_optimizer, optimizer_planner_supervisor) = UnixStream::pair()?;
        Ok(Self {
            planner_worker,
            planner_supervisor,
            executor_worker,
            executor_supervisor,
            result_worker,
            result_supervisor,
            spill_worker,
            spill_supervisor,
            optimizer_submission_worker,
            optimizer_submission_optimizer,
            optimizer_planner_optimizer,
            optimizer_planner_supervisor,
        })
    }

    fn worker_fds(&self) -> [i32; 5] {
        [
            self.planner_worker.as_raw_fd(),
            self.executor_worker.as_raw_fd(),
            self.optimizer_submission_worker.as_raw_fd(),
            self.result_worker.as_raw_fd(),
            self.spill_worker.as_raw_fd(),
        ]
    }

    fn optimizer_fds(&self) -> [i32; 2] {
        [
            self.optimizer_submission_optimizer.as_raw_fd(),
            self.optimizer_planner_optimizer.as_raw_fd(),
        ]
    }

    fn into_supervisor_streams(
        self,
    ) -> ((UnixStream, UnixStream, UnixStream), UnixStream, UnixStream) {
        (
            (
                self.planner_supervisor,
                self.executor_supervisor,
                self.optimizer_planner_supervisor,
            ),
            self.result_supervisor,
            self.spill_supervisor,
        )
    }
}

#[derive(Debug)]
pub enum ExecutorStartError<E> {
    Admission {
        error: E,
        cleanup_error: Option<io::Error>,
    },
    Launch(ExecutorLaunchError),
}

impl RunningExecutor {
    pub fn id(&self) -> u32 {
        self.child.id()
    }

    pub fn observe_exit(
        &mut self,
        invocation: &SupervisorInvocation<'_>,
    ) -> Result<ExitStatus, SupervisorInvocationError> {
        self.observe_exit_with_results(invocation, |_| Ok(()))
    }

    /// Concurrently serves storage channels and bounded provisional output.
    /// The callback runs on the supervisor thread and must not make a storage
    /// terminal decision based on output bytes.
    pub fn observe_exit_with_results<F>(
        &mut self,
        invocation: &SupervisorInvocation<'_>,
        on_result: F,
    ) -> Result<ExitStatus, SupervisorInvocationError>
    where
        F: FnMut(&[u8]) -> io::Result<()>,
    {
        self.observe_exit_with_results_and_optimizer_planner_observer(
            invocation,
            on_result,
            |_, _| {},
        )
    }

    /// Concurrently serves worker channels and relays bounded provisional
    /// output while observing optimizer planner requests at their storage
    /// dispatch boundary. The request observer runs on the storage handler
    /// thread without holding the invocation lock.
    #[doc(hidden)]
    pub fn observe_exit_with_results_and_optimizer_planner_observer<F, O>(
        &mut self,
        invocation: &SupervisorInvocation<'_>,
        on_result: F,
        optimizer_planner_observer: O,
    ) -> Result<ExitStatus, SupervisorInvocationError>
    where
        F: FnMut(&[u8]) -> io::Result<()>,
        O: Fn(u16, u64) + Send + Sync,
    {
        self.observe_exit_with_results_and_optimizer_planner_timing_observer(
            invocation,
            on_result,
            optimizer_planner_observer,
            |_, _, _| {},
            false,
        )
    }

    /// Variant that also reports optimizer-planner storage dispatch durations
    /// for confined workload profiling.
    #[doc(hidden)]
    pub fn observe_exit_with_results_and_optimizer_planner_timing_observer<F, O, T>(
        &mut self,
        invocation: &SupervisorInvocation<'_>,
        mut on_result: F,
        optimizer_planner_observer: O,
        optimizer_planner_timing_observer: T,
        measure_planner_dispatch: bool,
    ) -> Result<ExitStatus, SupervisorInvocationError>
    where
        F: FnMut(&[u8]) -> io::Result<()>,
        O: Fn(u16, u64) + Send + Sync,
        T: Fn(u16, u64, Duration) + Send + Sync,
    {
        let result_stream = self.result_stream.take();
        let spill_stream = self.spill_stream.take();
        let spill_session = self.spill_session.take();
        let mut primary_error = None;
        if let Some(result_stream) = result_stream {
            let (result_tx, result_rx) = std::sync::mpsc::sync_channel(4);
            std::thread::scope(|scope| {
                let max_total_result_bytes = self.max_total_result_bytes;
                let result_thread = scope.spawn(move || {
                    drain_result_stream(result_stream, result_tx, max_total_result_bytes)
                });
                let (spill_tx, spill_rx) = std::sync::mpsc::channel();
                let spill_task = match (spill_stream, spill_session.as_ref()) {
                    (Some(stream), Some(session)) => {
                        let broker = session.broker.clone();
                        let package = session.package;
                        let session = Arc::clone(session);
                        Some((
                            scope.spawn(move || {
                                let mut result =
                                    broker.serve_channel(stream, package).map_err(|error| {
                                        SupervisorInvocationError::WorkerChannelIo(
                                            io::Error::other(format!(
                                                "spill broker channel failed: {error:?}"
                                            )),
                                        )
                                    });
                                if let Err(error) = session.close() {
                                    result = Err(SupervisorInvocationError::WorkerChannelIo(
                                        io::Error::other(format!(
                                            "spill package cleanup failed: {error:?}"
                                        )),
                                    ));
                                }
                                let _ = spill_tx.send(result);
                            }),
                            spill_rx,
                        ))
                    }
                    (stream, _) => {
                        drop(stream);
                        None
                    }
                };
                let storage_task =
                    self.worker_channels
                        .take()
                        .map(|(planner, executor, optimizer_planner)| {
                            let (storage_tx, storage_rx) = std::sync::mpsc::channel();
                            let optimizer_planner_observer = &optimizer_planner_observer;
                            let optimizer_planner_timing_observer =
                                &optimizer_planner_timing_observer;
                            let storage_thread = scope.spawn(move || {
                                let result = invocation
                                    .serve_worker_channels_with_optimizer_planner_timing_observer(
                                        planner,
                                        executor,
                                        optimizer_planner,
                                        optimizer_planner_observer,
                                        optimizer_planner_timing_observer,
                                        measure_planner_dispatch,
                                    );
                                let _ = storage_tx.send(result);
                            });
                            (storage_thread, storage_rx)
                        });

                let mut result_done = false;
                let mut storage_done = storage_task.is_none();
                let mut spill_done = spill_task.is_none();
                let mut cancellation_started_at = None;
                while !result_done || !storage_done || !spill_done {
                    if primary_error.is_none()
                        && let Err(error) = invocation.check_worker_deadline()
                    {
                        if let Some(session) = spill_session.as_ref() {
                            let _ = session.broker.cancel(session.package);
                        }
                        primary_error = Some(error);
                    }
                    if primary_error.is_none() {
                        match invocation.cancellation_requested() {
                            Ok(true) => {
                                if let Some(session) = spill_session.as_ref() {
                                    let _ = session.broker.cancel(session.package);
                                }
                                let requested_at = cancellation_started_at
                                    .get_or_insert_with(std::time::Instant::now);
                                if requested_at.elapsed() >= COOPERATIVE_CANCELLATION_GRACE {
                                    primary_error = Some(SupervisorInvocationError::Deadline(
                                        crate::transaction::TransactionError::CancellationRequested,
                                    ));
                                }
                            }
                            Ok(false) => {}
                            Err(error) => primary_error = Some(error),
                        }
                    }
                    match result_rx.recv_timeout(Duration::from_millis(50)) {
                        Ok(Ok(chunk)) => {
                            if primary_error.is_none()
                                && let Err(error) = on_result(&chunk)
                            {
                                primary_error =
                                    Some(SupervisorInvocationError::WorkerChannelIo(error));
                            }
                        }
                        Ok(Err(error)) => {
                            primary_error
                                .get_or_insert(SupervisorInvocationError::WorkerChannelIo(error));
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                            result_done = true;
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                    }

                    if let Some((storage_thread, storage_rx)) = &storage_task
                        && !storage_done
                        && storage_thread.is_finished()
                    {
                        storage_done = true;
                        match storage_rx.try_recv() {
                            Ok(Ok(())) => {}
                            Ok(Err(error)) => {
                                primary_error.get_or_insert(error);
                            }
                            Err(_) => {
                                primary_error.get_or_insert(
                                    SupervisorInvocationError::WorkerHandlerPanicked,
                                );
                            }
                        }
                    }

                    if let Some((spill_thread, spill_rx)) = &spill_task
                        && !spill_done
                        && spill_thread.is_finished()
                    {
                        spill_done = true;
                        match spill_rx.try_recv() {
                            Ok(Ok(())) => {}
                            Ok(Err(error)) => {
                                primary_error.get_or_insert(error);
                            }
                            Err(_) => {
                                primary_error.get_or_insert(
                                    SupervisorInvocationError::WorkerHandlerPanicked,
                                );
                            }
                        }
                    }

                    if primary_error.is_some() {
                        if let Some(cgroup) = self.cgroup.as_ref() {
                            let _ = cgroup.kill_all();
                        }
                        let _ = self.child.kill();
                    }
                    if result_done && (!storage_done || !spill_done) {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                }

                match result_thread.join() {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        primary_error
                            .get_or_insert(SupervisorInvocationError::WorkerChannelIo(error));
                    }
                    Err(_) => {
                        primary_error
                            .get_or_insert(SupervisorInvocationError::WorkerHandlerPanicked);
                    }
                }
                if let Some((storage_thread, _)) = storage_task
                    && storage_thread.join().is_err()
                {
                    primary_error.get_or_insert(SupervisorInvocationError::WorkerHandlerPanicked);
                }
                if let Some((spill_thread, _)) = spill_task
                    && spill_thread.join().is_err()
                {
                    primary_error.get_or_insert(SupervisorInvocationError::WorkerHandlerPanicked);
                }
            });
        } else if let Some((planner, executor, optimizer_planner)) = self.worker_channels.take()
            && let Err(error) = invocation
                .serve_worker_channels_with_optimizer_planner_timing_observer(
                    planner,
                    executor,
                    optimizer_planner,
                    optimizer_planner_observer,
                    optimizer_planner_timing_observer,
                    measure_planner_dispatch,
                )
        {
            self.kill_reap_and_cleanup();
            return Err(error);
        } else {
            drop(spill_stream);
        }
        // The optimizer is a child in the same cgroup. Reap it before the
        // cgroup cleanup checks pids.current, otherwise its zombie can keep
        // the group populated until this parent later calls wait().
        let optimizer_result = self
            .optimizer_child
            .take()
            .map(|mut optimizer| wait_for_optimizer_exit(&mut optimizer, self.cleanup_timeout))
            .unwrap_or(Ok(()));
        if optimizer_result.is_err() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        let cgroup = self
            .cgroup
            .take()
            .ok_or(SupervisorInvocationError::Closed)?;
        let result =
            invocation.observe_worker_exit_in_cgroup(&mut self.child, cgroup, self.cleanup_timeout);
        if result.is_err() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        self.artifact_directory.take();
        match (primary_error, result, optimizer_result) {
            (Some(_), Err(error @ SupervisorInvocationError::WorkerCgroupIo(_)), _) => Err(error),
            (Some(_), Err(error @ SupervisorInvocationError::WorkerProcessIo(_)), _) => Err(error),
            (Some(error), _, _) => Err(error),
            (None, Err(error), _) => Err(error),
            (None, Ok(_), Err(error)) => Err(error),
            (None, Ok(status), Ok(())) => Ok(status),
        }
    }

    /// Serve the invocation's planner and executor endpoints concurrently.
    /// Callers that use this directly must also drain results concurrently;
    /// `observe_exit` handles both streams together.
    pub fn serve_worker_channels(
        &mut self,
        invocation: &SupervisorInvocation<'_>,
    ) -> Result<(), SupervisorInvocationError> {
        let (planner, executor, optimizer_planner) = self
            .worker_channels
            .take()
            .ok_or(SupervisorInvocationError::Closed)?;
        invocation.serve_worker_channels(planner, executor, optimizer_planner)
    }

    fn kill_reap_and_cleanup(&mut self) {
        if let Some(cgroup) = self.cgroup.as_ref() {
            let _ = cgroup.kill_all();
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(optimizer) = self.optimizer_child.as_mut() {
            let _ = optimizer.kill();
            let _ = optimizer.wait();
        }
        if let Some(cgroup) = self.cgroup.take() {
            let _ = cgroup.cleanup(self.cleanup_timeout);
        }
        self.artifact_directory.take();
    }
}

impl Drop for RunningExecutor {
    fn drop(&mut self) {
        if let Some(cgroup) = self.cgroup.take() {
            let _ = cgroup.kill_all();
            let _ = self.child.kill();
            let _ = self.child.wait();
            if let Some(optimizer) = self.optimizer_child.as_mut() {
                let _ = optimizer.kill();
                let _ = optimizer.wait();
            }
            let _ = cgroup.cleanup(self.cleanup_timeout);
        } else if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
            if let Some(optimizer) = self.optimizer_child.as_mut() {
                let _ = optimizer.kill();
                let _ = optimizer.wait();
            }
        }
        self.artifact_directory.take();
        self.worker_channels.take();
        self.result_stream.take();
    }
}

fn drain_result_stream(
    mut stream: UnixStream,
    sender: std::sync::mpsc::SyncSender<Result<Vec<u8>, io::Error>>,
    max_total_result_bytes: u64,
) -> io::Result<()> {
    let mut total_bytes = 0u64;
    loop {
        let mut first = [0u8; 1];
        match stream.read(&mut first) {
            Ok(0) => return Ok(()),
            Ok(1) => {}
            Ok(_) => unreachable!("one-byte read returned more than one byte"),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return send_result_error(&sender, error),
        }
        let mut rest = [0u8; 3];
        if let Err(error) = stream.read_exact(&mut rest) {
            return send_result_error(&sender, error);
        }
        let len = u32::from_be_bytes([first[0], rest[0], rest[1], rest[2]]) as u64;
        if len == 0 || len > crate::query_abi::MAX_RESULT_CHUNK_BYTES {
            return send_result_error(
                &sender,
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid worker result chunk size",
                ),
            );
        }
        let Some(next_total) = total_bytes.checked_add(len) else {
            return send_result_error(
                &sender,
                io::Error::new(io::ErrorKind::InvalidData, "worker result size overflow"),
            );
        };
        if next_total > max_total_result_bytes {
            return send_result_error(
                &sender,
                io::Error::new(io::ErrorKind::InvalidData, "worker result quota exceeded"),
            );
        }
        total_bytes = next_total;
        let Ok(len) = usize::try_from(len) else {
            return send_result_error(
                &sender,
                io::Error::new(io::ErrorKind::InvalidData, "worker result size overflow"),
            );
        };
        let mut chunk = Vec::new();
        if let Err(error) = chunk.try_reserve_exact(len) {
            return send_result_error(
                &sender,
                io::Error::other(format!("cannot reserve worker result buffer: {error}")),
            );
        }
        chunk.resize(len, 0);
        if let Err(error) = stream.read_exact(&mut chunk) {
            return send_result_error(&sender, error);
        }
        if sender.send(Ok(chunk)).is_err() {
            return Ok(());
        }
    }
}

fn send_result_error(
    sender: &std::sync::mpsc::SyncSender<Result<Vec<u8>, io::Error>>,
    error: io::Error,
) -> io::Result<()> {
    let _ = sender.send(Err(io::Error::new(error.kind(), error.to_string())));
    Ok(())
}

/// Materialize staged inputs and configure confinement before storage
/// admission; spawn only after `admit` returns successfully. An admission or
/// spawn failure releases the prepared resources, and dropping the admitted
/// value is left to its own rollback semantics.
pub fn start_and_launch_executor<T, E>(
    staging: &crate::supervisor::StagingStore,
    owner: crate::supervisor::StagingOwner,
    invocation_id: [u8; 16],
    cgroup_parent: impl AsRef<Path>,
    limits: crate::linux_sandbox::WorkerResourceLimits,
    config: &ExecutorLaunchConfig,
    admit: impl FnOnce(&SealedSubmission) -> Result<T, E>,
) -> Result<(T, RunningExecutor), crate::supervisor::StartError<ExecutorStartError<E>>> {
    start_and_launch_executor_selected(
        staging,
        owner,
        invocation_id,
        cgroup_parent,
        |_| limits,
        config,
        admit,
    )
}

/// Variant that selects cgroup limits from immutable sealed submission
/// metadata before creating the worker cgroup.
pub fn start_and_launch_executor_selected<T, E>(
    staging: &crate::supervisor::StagingStore,
    owner: crate::supervisor::StagingOwner,
    invocation_id: [u8; 16],
    cgroup_parent: impl AsRef<Path>,
    select_limits: impl FnOnce(
        &crate::supervisor::SubmissionMetadata,
    ) -> crate::linux_sandbox::WorkerResourceLimits,
    config: &ExecutorLaunchConfig,
    admit: impl FnOnce(&SealedSubmission) -> Result<T, E>,
) -> Result<(T, RunningExecutor), crate::supervisor::StartError<ExecutorStartError<E>>> {
    let cgroup_parent = cgroup_parent.as_ref();
    if should_use_prewarmed_pool(config) {
        let pool_key = PrewarmedPoolKey::new(cgroup_parent, config);
        if let Some(worker) = pop_prewarmed_worker(&pool_key, config) {
            return launch_on_prewarmed_worker(
                staging,
                owner,
                invocation_id,
                select_limits,
                config,
                admit,
                worker,
            );
        }
        trigger_pool_replenish(&pool_key, config);
    }

    staging.start_with_worker_cgroup_selected(
        owner,
        invocation_id,
        cgroup_parent,
        select_limits,
        |submission, cgroup| {
            let channels = match WorkerChannelPair::new() {
                Ok(channels) => channels,
                Err(error) => {
                    return Err(ExecutorStartError::Launch(ExecutorLaunchError {
                        launch_error: error,
                        cleanup_error: cgroup.cleanup(config.cleanup_timeout).err(),
                    }));
                }
            };
            let spill_session = match create_spill_session(submission, config) {
                Ok(session) => session,
                Err(error) => {
                    return Err(ExecutorStartError::Launch(ExecutorLaunchError {
                        launch_error: error,
                        cleanup_error: cgroup.cleanup(config.cleanup_timeout).err(),
                    }));
                }
            };
            let prepared = prepare_executor(submission, cgroup, channels, spill_session, config)
                .map_err(ExecutorStartError::Launch)?;
            let admitted = match admit(submission) {
                Ok(admitted) => admitted,
                Err(error) => {
                    return Err(ExecutorStartError::Admission {
                        error,
                        cleanup_error: prepared.cleanup(),
                    });
                }
            };
            let worker = prepared.spawn().map_err(ExecutorStartError::Launch)?;
            Ok((admitted, worker))
        },
    )
}

fn create_spill_session(
    submission: &SealedSubmission,
    config: &ExecutorLaunchConfig,
) -> io::Result<Option<Arc<SpillSession>>> {
    let Some(broker) = config.spill_broker.as_ref() else {
        return Ok(None);
    };
    let metadata = submission.metadata();
    if !metadata.workload_profile.spill_enabled() {
        return Ok(None);
    }
    let (invocation_id, package_id) = match submission.interactive_package() {
        Some(binding) => (binding.transaction_id, binding.package_id),
        None => (submission.invocation_id(), submission.invocation_id()),
    };
    let package = crate::spill::SpillPackage::new(invocation_id, package_id);
    let deadline = std::time::Instant::now()
        .checked_add(Duration::from_millis(metadata.deadline_ms))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "spill deadline overflow"))?;
    let spill_limits = crate::spill::SpillInvocationLimits {
        max_bytes: metadata.workload_profile.spill_max_bytes,
        max_io_bytes: metadata.workload_profile.spill_max_io_bytes,
        max_streams: metadata.workload_profile.spill_max_streams as u64,
        max_operations: metadata.workload_profile.spill_max_operations,
        max_response_bytes: metadata.workload_profile.spill_max_response_bytes,
    };
    broker
        .open_package(package, Some(spill_limits), deadline)
        .map_err(|error| io::Error::other(format!("spill package admission failed: {error:?}")))?;
    Ok(Some(Arc::new(SpillSession {
        broker: broker.clone(),
        package,
        closed: std::sync::atomic::AtomicBool::new(false),
    })))
}

/// Materialize staged bytes privately and install the pre-exec restrictions.
/// The returned host process is not started until `PreparedExecutor::spawn`.
fn prepare_executor(
    submission: &SealedSubmission,
    cgroup: WorkerCgroup,
    channels: WorkerChannelPair,
    spill_session: Option<Arc<SpillSession>>,
    config: &ExecutorLaunchConfig,
) -> Result<PreparedExecutor, ExecutorLaunchError> {
    let effective_profile = config.clamp_profile(&submission.metadata().workload_profile);
    let mut effective_config = config.clone();
    effective_config.max_total_result_bytes = effective_profile.max_total_result_bytes;
    effective_config.max_sql_index_probe_candidates =
        effective_profile.max_sql_index_probe_candidates;
    match build_command(
        submission,
        &cgroup,
        &channels,
        spill_session.is_some(),
        &effective_config,
        &effective_profile,
    ) {
        Ok((
            command,
            optimizer_command,
            parameters,
            sql_data,
            profile_file,
            artifact_directory,
        )) => Ok(PreparedExecutor {
            command,
            optimizer_command: Some(optimizer_command),
            parameters: Some(parameters),
            sql_data: Some(sql_data),
            profile_file: Some(profile_file),
            cgroup: Some(cgroup),
            artifact_directory: Some(artifact_directory),
            worker_channels: Some(channels),
            spill_session,
            cleanup_timeout: config.cleanup_timeout,
            max_total_result_bytes: effective_config.max_total_result_bytes,
        }),
        Err(launch_error) => Err(ExecutorLaunchError {
            launch_error,
            cleanup_error: cgroup.cleanup(config.cleanup_timeout).err(),
        }),
    }
}

impl PreparedExecutor {
    fn cleanup(mut self) -> Option<io::Error> {
        let cleanup_error = self
            .cgroup
            .take()
            .and_then(|cgroup| cgroup.cleanup(self.cleanup_timeout).err());
        self.artifact_directory.take();
        self.parameters.take();
        self.sql_data.take();
        cleanup_error
    }

    pub fn spawn(mut self) -> Result<RunningExecutor, ExecutorLaunchError> {
        let mut optimizer_command = self
            .optimizer_command
            .take()
            .expect("prepared optimizer command");
        let mut optimizer_child = match optimizer_command.spawn() {
            Ok(child) => child,
            Err(launch_error) => {
                return Err(ExecutorLaunchError {
                    launch_error,
                    cleanup_error: self
                        .cgroup
                        .take()
                        .and_then(|cgroup| cgroup.cleanup(self.cleanup_timeout).err()),
                });
            }
        };
        match self.command.spawn() {
            Ok(child) => {
                self.parameters.take();
                self.sql_data.take();
                let channels = self
                    .worker_channels
                    .take()
                    .expect("prepared worker channels");
                let (supervisor_streams, result_stream, spill_stream) =
                    channels.into_supervisor_streams();
                Ok(RunningExecutor {
                    child,
                    optimizer_child: Some(optimizer_child),
                    cgroup: self.cgroup.take(),
                    artifact_directory: self.artifact_directory.take(),
                    worker_channels: Some(supervisor_streams),
                    spill_stream: Some(spill_stream),
                    spill_session: self.spill_session.take(),
                    result_stream: Some(result_stream),
                    cleanup_timeout: self.cleanup_timeout,
                    max_total_result_bytes: self.max_total_result_bytes,
                })
            }
            Err(launch_error) => Err(ExecutorLaunchError {
                cleanup_error: {
                    let _ = optimizer_child.kill();
                    let _ = optimizer_child.wait();
                    self.cgroup
                        .take()
                        .and_then(|cgroup| cgroup.cleanup(self.cleanup_timeout).err())
                },
                launch_error,
            }),
        }
    }
}

impl Drop for PreparedExecutor {
    fn drop(&mut self) {
        if let Some(cgroup) = self.cgroup.take() {
            let _ = cgroup.kill_all();
            let _ = cgroup.cleanup(self.cleanup_timeout);
        }
        self.artifact_directory.take();
        self.parameters.take();
        self.sql_data.take();
        self.worker_channels.take();
        self.spill_session.take();
    }
}

fn build_command(
    submission: &SealedSubmission,
    cgroup: &WorkerCgroup,
    channels: &WorkerChannelPair,
    spill_enabled: bool,
    config: &ExecutorLaunchConfig,
    profile: &crate::workload_profile::WorkloadProfileLimits,
) -> io::Result<(Command, Command, File, File, File, PrivateArtifactDirectory)> {
    let executor_path = config.executor_path.canonicalize()?;
    if !executor_path.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "executor path is not a regular file",
        ));
    }
    let artifact_directory = PrivateArtifactDirectory::create(
        &config.scratch_parent,
        submission.invocation_id(),
        submission.artifact(),
        submission.sdk_library_pin(),
    )?;
    let parameters = create_sealed_memfd(c"prdb-query-parameters", submission.parameters())?;
    let sql_data = create_sealed_memfd(c"prdb-query-sql-data", submission.sql_data())?;
    let profile_bytes = profile.encode();
    let profile_file = create_sealed_memfd(c"prdb-query-profile", &profile_bytes)?;
    let optimizer_path = config.optimizer_path.canonicalize()?;
    if !optimizer_path.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "optimizer path is not a regular file",
        ));
    }

    let mut policy = LandlockPolicy::default();
    policy.allow_read_execute(&executor_path)?;
    policy.allow_read_execute(&artifact_directory.path)?;
    allow_runtime_paths(&mut policy, &config.runtime_read_execute, true)?;
    allow_runtime_paths(&mut policy, &config.runtime_read_only, false)?;
    let ruleset = LandlockRuleset::new(&policy)?;

    let [planner_fd, executor_fd, optimizer_fd, result_fd, spill_fd] = channels.worker_fds();
    let mut command = Command::new(executor_path);
    command
        .arg("--library")
        .arg(&artifact_directory.artifact_path)
        .arg("--parameters-fd")
        .arg(parameters.as_raw_fd().to_string())
        .arg("--sql-data-fd")
        .arg(sql_data.as_raw_fd().to_string())
        .arg("--planner-fd")
        .arg(planner_fd.to_string())
        .arg("--executor-fd")
        .arg(executor_fd.to_string())
        .arg("--optimizer-fd")
        .arg(optimizer_fd.to_string())
        .arg("--result-fd")
        .arg(result_fd.to_string())
        .arg("--spill-fd")
        .arg(if spill_enabled {
            spill_fd.to_string()
        } else {
            "-1".to_owned()
        })
        .arg("--max-total-result-bytes")
        .arg(config.max_total_result_bytes.to_string())
        .arg("--max-sql-index-probe-candidates")
        .arg(config.max_sql_index_probe_candidates.to_string())
        .arg("--profile-fd")
        .arg(profile_file.as_raw_fd().to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut inherited_fds = vec![
        parameters.as_raw_fd(),
        sql_data.as_raw_fd(),
        profile_file.as_raw_fd(),
        planner_fd,
        executor_fd,
        optimizer_fd,
        result_fd,
    ];
    if spill_enabled {
        inherited_fds.push(spill_fd);
    }
    inherited_fds.sort_unstable();
    inherited_fds.dedup();
    configure_worker_command(&mut command, cgroup.path(), &ruleset, &inherited_fds)?;

    let mut optimizer_policy = LandlockPolicy::default();
    optimizer_policy.allow_read_execute(&optimizer_path)?;
    allow_runtime_paths(&mut optimizer_policy, &config.runtime_read_execute, true)?;
    allow_runtime_paths(&mut optimizer_policy, &config.runtime_read_only, false)?;
    let optimizer_ruleset = LandlockRuleset::new(&optimizer_policy)?;
    let [optimizer_submission_fd, optimizer_planner_fd] = channels.optimizer_fds();
    let mut optimizer_command = Command::new(optimizer_path);
    optimizer_command
        .arg("--submission-fd")
        .arg(optimizer_submission_fd.to_string())
        .arg("--planner-fd")
        .arg(optimizer_planner_fd.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    configure_worker_command(
        &mut optimizer_command,
        cgroup.path(),
        &optimizer_ruleset,
        &[optimizer_submission_fd, optimizer_planner_fd],
    )?;
    Ok((
        command,
        optimizer_command,
        parameters,
        sql_data,
        profile_file,
        artifact_directory,
    ))
}

fn allow_runtime_paths(
    policy: &mut LandlockPolicy,
    paths: &[PathBuf],
    executable: bool,
) -> io::Result<()> {
    for requested_path in paths {
        let mut parent = requested_path.parent();
        while let Some(directory) = parent {
            policy.allow_path_traversal(directory).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!("runtime path traversal {}: {error}", directory.display()),
                )
            })?;
            parent = directory.parent();
        }
        let path = requested_path.canonicalize()?;
        if executable {
            policy.allow_read_execute(&path)?;
        } else {
            policy.allow_read(&path)?;
        }
        let mut parent = path.parent();
        while let Some(directory) = parent {
            policy.allow_path_traversal(directory)?;
            parent = directory.parent();
        }
    }
    Ok(())
}

struct PrivateArtifactDirectory {
    path: PathBuf,
    artifact_path: PathBuf,
    _sdk_library_pin: Option<crate::sdk_registry::SdkLibraryPin>,
}

impl PrivateArtifactDirectory {
    fn create_empty(parent: &Path, invocation_id: [u8; 16]) -> io::Result<Self> {
        let parent = parent.canonicalize()?;
        if !parent.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "executor scratch parent is not a directory",
            ));
        }
        let mut name = String::from("prdb-");
        for byte in invocation_id {
            use std::fmt::Write as _;
            write!(&mut name, "{byte:02x}").expect("writing to String cannot fail");
        }
        let path = parent.join(name);
        let mut builder = fs::DirBuilder::new();
        builder.mode(0o700);
        builder.create(&path)?;
        let artifact_path = path.join("query.so");
        Ok(Self {
            path,
            artifact_path,
            _sdk_library_pin: None,
        })
    }

    fn populate(
        &mut self,
        artifact: &[u8],
        sdk_library_pin: Option<&crate::sdk_registry::SdkLibraryPin>,
    ) -> io::Result<()> {
        self._sdk_library_pin = sdk_library_pin.cloned();
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o500)
            .open(&self.artifact_path)?;
        file.write_all(artifact)?;
        file.sync_all()?;
        fs::set_permissions(&self.artifact_path, fs::Permissions::from_mode(0o500))?;
        if let Some(pin) = sdk_library_pin {
            let staged_library_path = self.path.join(pin.soname());
            let mut source = File::open(pin.library_path())?;
            let mut staged_library = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o500)
                .open(&staged_library_path)?;
            io::copy(&mut source, &mut staged_library)?;
            staged_library.sync_all()?;
            fs::set_permissions(&staged_library_path, fs::Permissions::from_mode(0o500))?;
        }
        Ok(())
    }

    fn create(
        parent: &Path,
        invocation_id: [u8; 16],
        artifact: &[u8],
        sdk_library_pin: Option<&crate::sdk_registry::SdkLibraryPin>,
    ) -> io::Result<Self> {
        let mut directory = Self::create_empty(parent, invocation_id)?;
        directory.populate(artifact, sdk_library_pin)?;
        Ok(directory)
    }
}

#[derive(Clone, Eq, PartialEq, Hash)]
struct PrewarmedPoolKey {
    cgroup_parent: PathBuf,
    executor_path: PathBuf,
    optimizer_path: PathBuf,
    scratch_parent: PathBuf,
    runtime_read_execute: Vec<PathBuf>,
    runtime_read_only: Vec<PathBuf>,
}

impl PrewarmedPoolKey {
    fn new(cgroup_parent: &Path, config: &ExecutorLaunchConfig) -> Self {
        Self {
            cgroup_parent: cgroup_parent.to_path_buf(),
            executor_path: config.executor_path.clone(),
            optimizer_path: config.optimizer_path.clone(),
            scratch_parent: config.scratch_parent.clone(),
            runtime_read_execute: config.runtime_read_execute.clone(),
            runtime_read_only: config.runtime_read_only.clone(),
        }
    }
}

struct PrewarmedWorker {
    child: Option<Child>,
    optimizer_child: Option<Child>,
    ctrl_stream: Option<UnixStream>,
    cgroup: Option<WorkerCgroup>,
    artifact_directory: Option<PrivateArtifactDirectory>,
    channels: Option<WorkerChannelPair>,
    cleanup_timeout: Duration,
}

impl Drop for PrewarmedWorker {
    fn drop(&mut self) {
        if let Some(cgroup) = self.cgroup.take() {
            let _ = cgroup.kill_all();
            if let Some(mut child) = self.child.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
            if let Some(mut opt) = self.optimizer_child.take() {
                let _ = opt.kill();
                let _ = opt.wait();
            }
            let _ = cgroup.cleanup(self.cleanup_timeout);
        }
        self.ctrl_stream.take();
        self.artifact_directory.take();
        self.channels.take();
    }
}

struct PoolQueueState {
    workers: VecDeque<PrewarmedWorker>,
    in_flight: usize,
}

static PREWARMED_POOLS: OnceLock<Mutex<HashMap<PrewarmedPoolKey, Arc<Mutex<PoolQueueState>>>>> =
    OnceLock::new();
static POOL_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

fn configured_prewarm_pool_size() -> usize {
    static SIZE: OnceLock<usize> = OnceLock::new();
    *SIZE.get_or_init(|| {
        std::env::var("PRDB_PREWARM_POOL_SIZE")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(4)
            .min(16)
    })
}

fn should_use_prewarmed_pool(config: &ExecutorLaunchConfig) -> bool {
    configured_prewarm_pool_size() > 0
        && config
            .executor_path
            .file_name()
            .is_some_and(|name| name == "prdb-executor")
}

fn pool_queue_for(key: &PrewarmedPoolKey) -> Arc<Mutex<PoolQueueState>> {
    let pools = PREWARMED_POOLS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut map = pools.lock().expect("prewarmed pool mutex");
    map.entry(key.clone())
        .or_insert_with(|| {
            Arc::new(Mutex::new(PoolQueueState {
                workers: VecDeque::new(),
                in_flight: 0,
            }))
        })
        .clone()
}

fn trigger_pool_replenish(key: &PrewarmedPoolKey, config: &ExecutorLaunchConfig) {
    let target = configured_prewarm_pool_size();
    if target == 0 {
        return;
    }
    let queue = pool_queue_for(key);
    let to_spawn = {
        let mut guard = queue.lock().expect("pool queue mutex");
        let current = guard.workers.len() + guard.in_flight;
        if current >= target {
            0
        } else {
            let needed = target - current;
            guard.in_flight += needed;
            needed
        }
    };
    if to_spawn == 0 {
        return;
    }
    let key = key.clone();
    let config = config.clone();
    std::thread::spawn(move || {
        for _ in 0..to_spawn {
            let spawned = spawn_prewarmed_worker(&key, &config);
            let mut guard = queue.lock().expect("pool queue mutex");
            guard.in_flight = guard.in_flight.saturating_sub(1);
            if let Ok(worker) = spawned {
                guard.workers.push_back(worker);
            }
        }
    });
}

fn pop_prewarmed_worker(
    key: &PrewarmedPoolKey,
    config: &ExecutorLaunchConfig,
) -> Option<PrewarmedWorker> {
    let queue = pool_queue_for(key);
    let mut selected = None;
    {
        let mut guard = queue.lock().expect("pool queue mutex");
        while let Some(mut worker) = guard.workers.pop_front() {
            let child_alive = worker
                .child
                .as_mut()
                .and_then(|c| c.try_wait().ok())
                .is_some_and(|s| s.is_none());
            let opt_alive = worker
                .optimizer_child
                .as_mut()
                .and_then(|c| c.try_wait().ok())
                .is_some_and(|s| s.is_none());
            if child_alive && opt_alive {
                selected = Some(worker);
                break;
            }
        }
    }
    trigger_pool_replenish(key, config);
    selected
}

fn spawn_prewarmed_worker(
    key: &PrewarmedPoolKey,
    config: &ExecutorLaunchConfig,
) -> io::Result<PrewarmedWorker> {
    let seq = POOL_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut slot_id = [0u8; 16];
    slot_id[0] = 0x50;
    slot_id[1..9].copy_from_slice(&seq.to_le_bytes());
    slot_id[9..13].copy_from_slice(&std::process::id().to_le_bytes());

    let cgroup = WorkerCgroup::create(
        &key.cgroup_parent,
        slot_id,
        crate::linux_sandbox::WorkerResourceLimits {
            cpu_quota_micros: 100_000,
            cpu_period_micros: 100_000,
            memory_max_bytes: 256 * 1024 * 1024,
            max_processes: 16,
        },
    )?;
    let channels = WorkerChannelPair::new()?;
    let (ctrl_supervisor, ctrl_worker) = UnixStream::pair()?;
    let artifact_directory = PrivateArtifactDirectory::create_empty(&key.scratch_parent, slot_id)?;

    let executor_path = key.executor_path.canonicalize()?;
    let optimizer_path = key.optimizer_path.canonicalize()?;

    let mut policy = LandlockPolicy::default();
    policy.allow_read_execute(&executor_path)?;
    policy.allow_read_execute(&artifact_directory.path)?;
    allow_runtime_paths(&mut policy, &key.runtime_read_execute, true)?;
    allow_runtime_paths(&mut policy, &key.runtime_read_only, false)?;
    let ruleset = LandlockRuleset::new(&policy)?;

    let [planner_fd, executor_fd, optimizer_fd, result_fd, _spill_fd] = channels.worker_fds();
    let ctrl_fd = ctrl_worker.as_raw_fd();
    let mut command = Command::new(&executor_path);
    command
        .arg("--standby-fd")
        .arg(ctrl_fd.to_string())
        .arg("--planner-fd")
        .arg(planner_fd.to_string())
        .arg("--executor-fd")
        .arg(executor_fd.to_string())
        .arg("--optimizer-fd")
        .arg(optimizer_fd.to_string())
        .arg("--result-fd")
        .arg(result_fd.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut inherited_fds = vec![ctrl_fd, planner_fd, executor_fd, optimizer_fd, result_fd];
    inherited_fds.sort_unstable();
    inherited_fds.dedup();
    configure_worker_command(&mut command, cgroup.path(), &ruleset, &inherited_fds)?;

    let mut opt_policy = LandlockPolicy::default();
    opt_policy.allow_read_execute(&optimizer_path)?;
    allow_runtime_paths(&mut opt_policy, &key.runtime_read_execute, true)?;
    allow_runtime_paths(&mut opt_policy, &key.runtime_read_only, false)?;
    let opt_ruleset = LandlockRuleset::new(&opt_policy)?;
    let [opt_sub_fd, opt_plan_fd] = channels.optimizer_fds();
    let mut opt_cmd = Command::new(&optimizer_path);
    opt_cmd
        .arg("--submission-fd")
        .arg(opt_sub_fd.to_string())
        .arg("--planner-fd")
        .arg(opt_plan_fd.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    configure_worker_command(
        &mut opt_cmd,
        cgroup.path(),
        &opt_ruleset,
        &[opt_sub_fd, opt_plan_fd],
    )?;

    let mut optimizer_child = opt_cmd.spawn()?;
    let child = match command.spawn() {
        Ok(c) => c,
        Err(e) => {
            let _ = optimizer_child.kill();
            let _ = optimizer_child.wait();
            let _ = cgroup.cleanup(config.cleanup_timeout);
            return Err(e);
        }
    };
    drop(ctrl_worker);

    Ok(PrewarmedWorker {
        child: Some(child),
        optimizer_child: Some(optimizer_child),
        ctrl_stream: Some(ctrl_supervisor),
        cgroup: Some(cgroup),
        artifact_directory: Some(artifact_directory),
        channels: Some(channels),
        cleanup_timeout: config.cleanup_timeout,
    })
}

fn send_standby_invocation(stream: &UnixStream, payload: &[u8], fds: &[i32]) -> io::Result<()> {
    let mut iov = libc::iovec {
        iov_base: payload.as_ptr() as *mut libc::c_void,
        iov_len: payload.len(),
    };
    let cmsg_len_bytes = std::mem::size_of_val(fds);
    let cmsg_space = unsafe { libc::CMSG_SPACE(cmsg_len_bytes as u32) };
    let mut cmsg_buf = vec![0u8; cmsg_space as usize];
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsg_buf.as_mut_ptr().cast();
    msg.msg_controllen = unsafe { libc::CMSG_LEN(cmsg_len_bytes as u32) } as _;

    let cmsg = unsafe { libc::CMSG_FIRSTHDR(&msg) };
    unsafe {
        (*cmsg).cmsg_level = libc::SOL_SOCKET;
        (*cmsg).cmsg_type = libc::SCM_RIGHTS;
        (*cmsg).cmsg_len = libc::CMSG_LEN(cmsg_len_bytes as u32) as _;
        let data_ptr = libc::CMSG_DATA(cmsg);
        std::ptr::copy_nonoverlapping(fds.as_ptr().cast::<u8>(), data_ptr, cmsg_len_bytes);
    }

    loop {
        let sent = unsafe { libc::sendmsg(stream.as_raw_fd(), &msg, 0) };
        if sent < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }
        return Ok(());
    }
}

fn launch_on_prewarmed_worker<T, E>(
    staging: &crate::supervisor::StagingStore,
    owner: crate::supervisor::StagingOwner,
    invocation_id: [u8; 16],
    select_limits: impl FnOnce(
        &crate::supervisor::SubmissionMetadata,
    ) -> crate::linux_sandbox::WorkerResourceLimits,
    config: &ExecutorLaunchConfig,
    admit: impl FnOnce(&SealedSubmission) -> Result<T, E>,
    mut worker: PrewarmedWorker,
) -> Result<(T, RunningExecutor), crate::supervisor::StartError<ExecutorStartError<E>>> {
    staging.start_with(owner, invocation_id, |submission| {
        let effective_profile = config.clamp_profile(&submission.metadata().workload_profile);
        let limits = select_limits(submission.metadata());
        let cgroup = worker.cgroup.as_ref().expect("worker cgroup");
        cgroup.apply_limits(limits).map_err(|error| {
            ExecutorStartError::Launch(ExecutorLaunchError {
                launch_error: error,
                cleanup_error: None,
            })
        })?;

        let spill_session = create_spill_session(submission, config).map_err(|error| {
            ExecutorStartError::Launch(ExecutorLaunchError {
                launch_error: error,
                cleanup_error: None,
            })
        })?;

        let artifact_dir = worker
            .artifact_directory
            .as_mut()
            .expect("worker artifact dir");
        artifact_dir
            .populate(submission.artifact(), submission.sdk_library_pin())
            .map_err(|error| {
                ExecutorStartError::Launch(ExecutorLaunchError {
                    launch_error: error,
                    cleanup_error: None,
                })
            })?;

        let parameters = create_sealed_memfd(c"prdb-query-parameters", submission.parameters())
            .map_err(|error| {
                ExecutorStartError::Launch(ExecutorLaunchError {
                    launch_error: error,
                    cleanup_error: None,
                })
            })?;
        let sql_data = create_sealed_memfd(c"prdb-query-sql-data", submission.sql_data()).map_err(
            |error| {
                ExecutorStartError::Launch(ExecutorLaunchError {
                    launch_error: error,
                    cleanup_error: None,
                })
            },
        )?;
        let profile_bytes = effective_profile.encode();
        let profile_file =
            create_sealed_memfd(c"prdb-query-profile", &profile_bytes).map_err(|error| {
                ExecutorStartError::Launch(ExecutorLaunchError {
                    launch_error: error,
                    cleanup_error: None,
                })
            })?;

        let admitted = match admit(submission) {
            Ok(admitted) => admitted,
            Err(error) => {
                return Err(ExecutorStartError::Admission {
                    error,
                    cleanup_error: None,
                });
            }
        };

        let channels = worker.channels.take().expect("worker channels");
        let [_, _, _, _, spill_fd] = channels.worker_fds();
        let mut fds = vec![
            parameters.as_raw_fd(),
            sql_data.as_raw_fd(),
            profile_file.as_raw_fd(),
        ];
        if spill_session.is_some() {
            fds.push(spill_fd);
        }

        let path_bytes = artifact_dir.artifact_path.as_os_str().as_bytes();
        let mut payload = Vec::with_capacity(17 + path_bytes.len());
        payload.extend_from_slice(&effective_profile.max_total_result_bytes.to_le_bytes());
        payload.extend_from_slice(
            &effective_profile
                .max_sql_index_probe_candidates
                .to_le_bytes(),
        );
        payload.push(u8::from(spill_session.is_some()));
        payload.extend_from_slice(&(path_bytes.len() as u32).to_le_bytes());
        payload.extend_from_slice(path_bytes);

        let ctrl = worker.ctrl_stream.take().expect("worker ctrl stream");
        send_standby_invocation(&ctrl, &payload, &fds).map_err(|error| {
            ExecutorStartError::Launch(ExecutorLaunchError {
                launch_error: error,
                cleanup_error: None,
            })
        })?;

        let (supervisor_streams, result_stream, spill_stream) = channels.into_supervisor_streams();
        Ok((
            admitted,
            RunningExecutor {
                child: worker.child.take().expect("worker child"),
                optimizer_child: worker.optimizer_child.take(),
                cgroup: worker.cgroup.take(),
                artifact_directory: worker.artifact_directory.take(),
                worker_channels: Some(supervisor_streams),
                spill_stream: Some(spill_stream),
                spill_session,
                result_stream: Some(result_stream),
                cleanup_timeout: config.cleanup_timeout,
                max_total_result_bytes: effective_profile.max_total_result_bytes,
            },
        ))
    })
}

impl Drop for PrivateArtifactDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn create_sealed_memfd(name: &std::ffi::CStr, bytes: &[u8]) -> io::Result<File> {
    // SAFETY: the name is a static NUL-terminated string and flags are a
    // fixed memfd option mask.
    let descriptor = unsafe {
        libc::syscall(
            libc::SYS_memfd_create,
            name.as_ptr(),
            libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
        )
    };
    if descriptor < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful memfd_create returns a new owned descriptor.
    let mut file = unsafe { File::from_raw_fd(descriptor as i32) };
    file.write_all(bytes)?;
    let seals = libc::F_SEAL_WRITE | libc::F_SEAL_GROW | libc::F_SEAL_SHRINK | libc::F_SEAL_SEAL;
    // SAFETY: F_ADD_SEALS consumes the scalar seal mask and a valid memfd.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_ADD_SEALS, seals) } < 0 {
        return Err(io::Error::last_os_error());
    }
    file.seek(SeekFrom::Start(0))?;
    Ok(file)
}

#[cfg(test)]
fn create_sealed_parameters(parameters: &[u8]) -> io::Result<File> {
    create_sealed_memfd(c"prdb-query-parameters", parameters)
}

#[cfg(test)]
mod tests {
    use super::{
        ExecutorLaunchConfig, PrivateArtifactDirectory, WorkerChannelPair,
        create_sealed_parameters, drain_result_stream, wait_for_optimizer_exit,
    };
    use std::io::{self, Read, Seek, SeekFrom, Write};
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::UnixStream;

    #[test]
    fn profile_budgets_are_clamped_by_host_launch_ceilings() {
        let config = ExecutorLaunchConfig {
            executor_path: std::path::PathBuf::new(),
            optimizer_path: std::path::PathBuf::new(),
            scratch_parent: std::path::PathBuf::new(),
            runtime_read_execute: Vec::new(),
            runtime_read_only: Vec::new(),
            spill_broker: None,
            max_total_result_bytes: 100,
            max_sql_index_probe_candidates: 256,
            cleanup_timeout: std::time::Duration::ZERO,
        };
        let mut profile = crate::workload_profile::WorkloadProfileLimits::small_analytical();
        profile.max_total_result_bytes = 200;
        profile.max_sql_index_probe_candidates = 512;
        let clamped = config.clamp_profile(&profile);
        assert_eq!(clamped.max_total_result_bytes, 100);
        assert_eq!(clamped.max_sql_index_probe_candidates, 256);
    }

    #[test]
    fn optimizer_child_is_reaped_before_cgroup_cleanup() {
        let mut successful = std::process::Command::new("sh")
            .args(["-c", "exit 0"])
            .spawn()
            .unwrap();
        assert!(
            wait_for_optimizer_exit(&mut successful, std::time::Duration::from_secs(1)).is_ok()
        );

        let mut stalled = std::process::Command::new("sh")
            .args(["-c", "exec sleep 5"])
            .spawn()
            .unwrap();
        let error = wait_for_optimizer_exit(&mut stalled, std::time::Duration::from_millis(20))
            .unwrap_err();
        assert!(matches!(
            error,
            crate::storage_manager::SupervisorInvocationError::WorkerProcessIo(ref error)
                if error.kind() == io::ErrorKind::TimedOut
        ));
        assert!(stalled.try_wait().unwrap().is_some());
    }

    #[test]
    fn staged_artifact_is_private_read_only_and_removed_on_drop() {
        let parent = std::env::temp_dir().join(format!("prdb-launch-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&parent);
        std::fs::create_dir(&parent).unwrap();
        let directory =
            PrivateArtifactDirectory::create(&parent, [7; 16], b"artifact", None).unwrap();
        assert_eq!(
            std::fs::read(&directory.artifact_path).unwrap(),
            b"artifact"
        );
        assert_eq!(
            std::fs::metadata(&directory.artifact_path)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o500
        );
        let staged_path = directory.path.clone();
        drop(directory);
        assert!(!staged_path.exists());
        std::fs::remove_dir(parent).unwrap();
    }

    #[test]
    fn parameters_are_sealed_and_rewound_for_the_executor() {
        use std::os::fd::AsRawFd;

        let mut parameters = create_sealed_parameters(b"params").unwrap();
        // SAFETY: F_GET_SEALS reads the seal mask from this live memfd.
        let seals = unsafe { libc::fcntl(parameters.as_raw_fd(), libc::F_GET_SEALS) };
        assert_eq!(
            seals,
            libc::F_SEAL_WRITE | libc::F_SEAL_GROW | libc::F_SEAL_SHRINK | libc::F_SEAL_SEAL
        );
        let mut bytes = Vec::new();
        parameters.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"params");
        assert!(parameters.seek(SeekFrom::Start(0)).is_ok());
        assert!(parameters.write_all(b"x").is_err());
    }

    #[test]
    fn planner_and_executor_stream_pairs_are_distinct_and_connected() {
        use std::io::{Read, Write};

        let channels = WorkerChannelPair::new().unwrap();
        let [planner_fd, executor_fd, optimizer_fd, result_fd, spill_fd] = channels.worker_fds();
        let [optimizer_submission_fd, optimizer_planner_fd] = channels.optimizer_fds();
        assert!(planner_fd >= 3);
        assert!(executor_fd >= 3);
        assert!(optimizer_fd >= 3);
        assert!(result_fd >= 3);
        assert!(spill_fd >= 3);
        assert_ne!(planner_fd, executor_fd);
        assert_ne!(planner_fd, result_fd);
        assert_ne!(executor_fd, result_fd);
        assert_ne!(spill_fd, result_fd);
        assert_ne!(optimizer_fd, planner_fd);
        assert_ne!(optimizer_fd, executor_fd);
        assert_ne!(optimizer_fd, result_fd);
        assert_ne!(optimizer_submission_fd, optimizer_planner_fd);

        let mut planner_peer = channels.planner_supervisor.try_clone().unwrap();
        let mut executor_peer = channels.executor_supervisor.try_clone().unwrap();
        let mut result_peer = channels.result_supervisor.try_clone().unwrap();
        let mut optimizer_peer = channels.optimizer_submission_optimizer.try_clone().unwrap();
        let mut optimizer_worker_peer = channels.optimizer_submission_worker.try_clone().unwrap();
        let mut planner_worker = channels.planner_worker;
        let mut executor_worker = channels.executor_worker;
        let mut result_worker = channels.result_worker;
        planner_worker.write_all(b"planner").unwrap();
        executor_worker.write_all(b"executor").unwrap();
        result_worker.write_all(b"result").unwrap();
        optimizer_worker_peer.write_all(b"opt").unwrap();
        let mut planner_request = [0; 7];
        let mut executor_request = [0; 8];
        let mut result = [0; 6];
        let mut optimizer = [0; 3];
        planner_peer.read_exact(&mut planner_request).unwrap();
        executor_peer.read_exact(&mut executor_request).unwrap();
        result_peer.read_exact(&mut result).unwrap();
        optimizer_peer.read_exact(&mut optimizer).unwrap();
        assert_eq!(&planner_request, b"planner");
        assert_eq!(&executor_request, b"executor");
        assert_eq!(&result, b"result");
        assert_eq!(&optimizer, b"opt");
    }

    #[test]
    fn result_stream_enforces_chunk_and_cumulative_bounds() {
        use std::io::Write;

        let (mut worker, supervisor) = UnixStream::pair().unwrap();
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        worker.write_all(&3u32.to_be_bytes()).unwrap();
        worker.write_all(b"abc").unwrap();
        drop(worker);
        drain_result_stream(supervisor, sender, crate::query_abi::MAX_TOTAL_RESULT_BYTES).unwrap();
        assert_eq!(receiver.recv().unwrap().unwrap(), b"abc");
        assert!(receiver.recv().is_err());

        let (mut worker, supervisor) = UnixStream::pair().unwrap();
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        worker.write_all(&3u32.to_be_bytes()).unwrap();
        worker.write_all(b"abc").unwrap();
        drop(worker);
        drain_result_stream(supervisor, sender, 2).unwrap();
        assert_eq!(
            receiver.recv().unwrap().unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );

        let (mut worker, supervisor) = UnixStream::pair().unwrap();
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        worker
            .write_all(&((crate::query_abi::MAX_RESULT_CHUNK_BYTES as u32) + 1).to_be_bytes())
            .unwrap();
        drop(worker);
        drain_result_stream(supervisor, sender, crate::query_abi::MAX_TOTAL_RESULT_BYTES).unwrap();
        assert_eq!(
            receiver.recv().unwrap().unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn result_stream_accepts_elevated_cumulative_budget_with_bounded_memory() {
        use std::io::Write;

        let elevated_limit = 128 * 1024 * 1024u64;
        let chunk_len = crate::query_abi::MAX_RESULT_CHUNK_BYTES as u32;
        let frame_count = elevated_limit / u64::from(chunk_len);
        assert_eq!(elevated_limit % u64::from(chunk_len), 0);

        let (mut worker, supervisor) = UnixStream::pair().unwrap();
        let (sender, receiver) = std::sync::mpsc::sync_channel(2);
        let payload = vec![0x5a; chunk_len as usize];
        let writer = std::thread::spawn(move || -> io::Result<()> {
            for _ in 0..frame_count {
                worker.write_all(&chunk_len.to_be_bytes())?;
                worker.write_all(&payload)?;
            }
            worker.shutdown(std::net::Shutdown::Write)
        });
        let relay =
            std::thread::spawn(move || drain_result_stream(supervisor, sender, elevated_limit));

        let mut received = 0u64;
        while let Ok(chunk) = receiver.recv() {
            received = received.saturating_add(chunk.unwrap().len() as u64);
        }
        writer.join().unwrap().unwrap();
        relay.join().unwrap().unwrap();
        assert_eq!(received, elevated_limit);
    }
}
