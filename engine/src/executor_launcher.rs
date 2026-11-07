//! Confined launch of a sealed native query artifact.
//!
//! This module only prepares and starts the executor host. The executor
//! performs ABI validation and loads the artifact after its post-exec checks.

use crate::linux_sandbox::{
    LandlockPolicy, LandlockRuleset, WorkerCgroup, configure_worker_command,
};
use crate::storage_manager::{SupervisorInvocation, SupervisorInvocationError};
use crate::supervisor::SealedSubmission;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct ExecutorLaunchConfig {
    pub executor_path: PathBuf,
    pub scratch_parent: PathBuf,
    /// Runtime files/directories needed by the dynamic loader and executor.
    /// The caller must supply the exact deployment profile, such as the
    /// pinned libc and its loader cache.
    pub runtime_read_execute: Vec<PathBuf>,
    pub runtime_read_only: Vec<PathBuf>,
    pub cleanup_timeout: Duration,
}

#[derive(Debug)]
pub struct ExecutorLaunchError {
    pub launch_error: io::Error,
    pub cleanup_error: Option<io::Error>,
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
    cgroup: Option<WorkerCgroup>,
    artifact_directory: Option<PrivateArtifactDirectory>,
    worker_channels: Option<(UnixStream, UnixStream)>,
    result_stream: Option<UnixStream>,
    cleanup_timeout: Duration,
}

/// Fully prepared command and staged inputs, ready to spawn after the storage
/// admission callback succeeds.
pub struct PreparedExecutor {
    command: Command,
    parameters: Option<File>,
    cgroup: Option<WorkerCgroup>,
    artifact_directory: Option<PrivateArtifactDirectory>,
    worker_channels: Option<WorkerChannelPair>,
    cleanup_timeout: Duration,
}

struct WorkerChannelPair {
    planner_worker: UnixStream,
    planner_supervisor: UnixStream,
    executor_worker: UnixStream,
    executor_supervisor: UnixStream,
    result_worker: UnixStream,
    result_supervisor: UnixStream,
}

impl WorkerChannelPair {
    fn new() -> io::Result<Self> {
        let (planner_worker, planner_supervisor) = UnixStream::pair()?;
        let (executor_worker, executor_supervisor) = UnixStream::pair()?;
        let (result_worker, result_supervisor) = UnixStream::pair()?;
        Ok(Self {
            planner_worker,
            planner_supervisor,
            executor_worker,
            executor_supervisor,
            result_worker,
            result_supervisor,
        })
    }

    fn worker_fds(&self) -> [i32; 3] {
        [
            self.planner_worker.as_raw_fd(),
            self.executor_worker.as_raw_fd(),
            self.result_worker.as_raw_fd(),
        ]
    }

    fn into_supervisor_streams(self) -> ((UnixStream, UnixStream), UnixStream) {
        (
            (self.planner_supervisor, self.executor_supervisor),
            self.result_supervisor,
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
        mut on_result: F,
    ) -> Result<ExitStatus, SupervisorInvocationError>
    where
        F: FnMut(&[u8]) -> io::Result<()>,
    {
        let result_stream = self.result_stream.take();
        let mut primary_error = None;
        if let Some(result_stream) = result_stream {
            let (result_tx, result_rx) = std::sync::mpsc::sync_channel(4);
            std::thread::scope(|scope| {
                let result_thread =
                    scope.spawn(move || drain_result_stream(result_stream, result_tx));
                let storage_task = self.worker_channels.take().map(|(planner, executor)| {
                    let (storage_tx, storage_rx) = std::sync::mpsc::channel();
                    let storage_thread = scope.spawn(move || {
                        let result = invocation.serve_worker_channels(planner, executor);
                        let _ = storage_tx.send(result);
                    });
                    (storage_thread, storage_rx)
                });

                let mut result_done = false;
                let mut storage_done = storage_task.is_none();
                while !result_done || !storage_done {
                    if primary_error.is_none()
                        && let Err(error) = invocation.check_deadline()
                    {
                        primary_error = Some(error);
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

                    if primary_error.is_some() {
                        if let Some(cgroup) = self.cgroup.as_ref() {
                            let _ = cgroup.kill_all();
                        }
                        let _ = self.child.kill();
                    }
                    if result_done && !storage_done {
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
            });
        } else if self.worker_channels.is_some()
            && let Err(error) = self.serve_worker_channels(invocation)
        {
            self.kill_reap_and_cleanup();
            return Err(error);
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
        match (primary_error, result) {
            (Some(_), Err(error @ SupervisorInvocationError::WorkerCgroupIo(_))) => Err(error),
            (Some(_), Err(error @ SupervisorInvocationError::WorkerProcessIo(_))) => Err(error),
            (Some(error), _) => Err(error),
            (None, result) => result,
        }
    }

    /// Serve the invocation's planner and executor endpoints concurrently.
    /// Callers that use this directly must also drain results concurrently;
    /// `observe_exit` handles both streams together.
    pub fn serve_worker_channels(
        &mut self,
        invocation: &SupervisorInvocation<'_>,
    ) -> Result<(), SupervisorInvocationError> {
        let (planner, executor) = self
            .worker_channels
            .take()
            .ok_or(SupervisorInvocationError::Closed)?;
        invocation.serve_worker_channels(planner, executor)
    }

    fn kill_reap_and_cleanup(&mut self) {
        if let Some(cgroup) = self.cgroup.as_ref() {
            let _ = cgroup.kill_all();
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
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
            let _ = cgroup.cleanup(self.cleanup_timeout);
        } else if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        self.artifact_directory.take();
        self.worker_channels.take();
        self.result_stream.take();
    }
}

fn drain_result_stream(
    mut stream: UnixStream,
    sender: std::sync::mpsc::SyncSender<Result<Vec<u8>, io::Error>>,
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
        if next_total > crate::query_abi::MAX_TOTAL_RESULT_BYTES {
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
    staging.start_with_worker_cgroup(
        owner,
        invocation_id,
        cgroup_parent,
        limits,
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
            let prepared = prepare_executor(submission, cgroup, channels, config)
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

/// Materialize staged bytes privately and install the pre-exec restrictions.
/// The returned host process is not started until `PreparedExecutor::spawn`.
fn prepare_executor(
    submission: &SealedSubmission,
    cgroup: WorkerCgroup,
    channels: WorkerChannelPair,
    config: &ExecutorLaunchConfig,
) -> Result<PreparedExecutor, ExecutorLaunchError> {
    match build_command(submission, &cgroup, &channels, config) {
        Ok((command, parameters, artifact_directory)) => Ok(PreparedExecutor {
            command,
            parameters: Some(parameters),
            cgroup: Some(cgroup),
            artifact_directory: Some(artifact_directory),
            worker_channels: Some(channels),
            cleanup_timeout: config.cleanup_timeout,
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
        cleanup_error
    }

    pub fn spawn(mut self) -> Result<RunningExecutor, ExecutorLaunchError> {
        match self.command.spawn() {
            Ok(child) => {
                self.parameters.take();
                let channels = self
                    .worker_channels
                    .take()
                    .expect("prepared worker channels");
                let (supervisor_streams, result_stream) = channels.into_supervisor_streams();
                Ok(RunningExecutor {
                    child,
                    cgroup: self.cgroup.take(),
                    artifact_directory: self.artifact_directory.take(),
                    worker_channels: Some(supervisor_streams),
                    result_stream: Some(result_stream),
                    cleanup_timeout: self.cleanup_timeout,
                })
            }
            Err(launch_error) => Err(ExecutorLaunchError {
                launch_error,
                cleanup_error: self
                    .cgroup
                    .take()
                    .and_then(|cgroup| cgroup.cleanup(self.cleanup_timeout).err()),
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
        self.worker_channels.take();
    }
}

fn build_command(
    submission: &SealedSubmission,
    cgroup: &WorkerCgroup,
    channels: &WorkerChannelPair,
    config: &ExecutorLaunchConfig,
) -> io::Result<(Command, File, PrivateArtifactDirectory)> {
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
    )?;
    let parameters = create_sealed_parameters(submission.parameters())?;

    let mut policy = LandlockPolicy::default();
    policy.allow_read_execute(&executor_path)?;
    policy.allow_read_execute(&artifact_directory.path)?;
    for path in &config.runtime_read_execute {
        policy.allow_read_execute(path)?;
    }
    for path in &config.runtime_read_only {
        policy.allow_read(path)?;
    }
    let ruleset = LandlockRuleset::new(&policy)?;

    let [planner_fd, executor_fd, result_fd] = channels.worker_fds();
    let mut command = Command::new(executor_path);
    command
        .arg("--library")
        .arg(&artifact_directory.artifact_path)
        .arg("--parameters-fd")
        .arg(parameters.as_raw_fd().to_string())
        .arg("--planner-fd")
        .arg(planner_fd.to_string())
        .arg("--executor-fd")
        .arg(executor_fd.to_string())
        .arg("--result-fd")
        .arg(result_fd.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut inherited_fds = vec![parameters.as_raw_fd(), planner_fd, executor_fd, result_fd];
    inherited_fds.sort_unstable();
    inherited_fds.dedup();
    configure_worker_command(&mut command, cgroup.path(), &ruleset, &inherited_fds)?;
    Ok((command, parameters, artifact_directory))
}

struct PrivateArtifactDirectory {
    path: PathBuf,
    artifact_path: PathBuf,
}

impl PrivateArtifactDirectory {
    fn create(parent: &Path, invocation_id: [u8; 16], artifact: &[u8]) -> io::Result<Self> {
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
        let directory = Self {
            path,
            artifact_path,
        };
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o500)
            .open(&directory.artifact_path)?;
        file.write_all(artifact)?;
        file.sync_all()?;
        fs::set_permissions(&directory.artifact_path, fs::Permissions::from_mode(0o500))?;
        Ok(directory)
    }
}

impl Drop for PrivateArtifactDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn create_sealed_parameters(parameters: &[u8]) -> io::Result<File> {
    let name = c"prdb-query-parameters";
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
    file.write_all(parameters)?;
    let seals = libc::F_SEAL_WRITE | libc::F_SEAL_GROW | libc::F_SEAL_SHRINK | libc::F_SEAL_SEAL;
    // SAFETY: F_ADD_SEALS consumes the scalar seal mask and a valid memfd.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_ADD_SEALS, seals) } < 0 {
        return Err(io::Error::last_os_error());
    }
    file.seek(SeekFrom::Start(0))?;
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::{
        PrivateArtifactDirectory, WorkerChannelPair, create_sealed_parameters, drain_result_stream,
    };
    use std::io::{self, Read, Seek, SeekFrom, Write};
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::UnixStream;

    #[test]
    fn staged_artifact_is_private_read_only_and_removed_on_drop() {
        let parent = std::env::temp_dir().join(format!("prdb-launch-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&parent);
        std::fs::create_dir(&parent).unwrap();
        let directory = PrivateArtifactDirectory::create(&parent, [7; 16], b"artifact").unwrap();
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
        let [planner_fd, executor_fd, result_fd] = channels.worker_fds();
        assert!(planner_fd >= 3);
        assert!(executor_fd >= 3);
        assert!(result_fd >= 3);
        assert_ne!(planner_fd, executor_fd);
        assert_ne!(planner_fd, result_fd);
        assert_ne!(executor_fd, result_fd);

        let mut planner_peer = channels.planner_supervisor.try_clone().unwrap();
        let mut executor_peer = channels.executor_supervisor.try_clone().unwrap();
        let mut result_peer = channels.result_supervisor.try_clone().unwrap();
        let mut planner_worker = channels.planner_worker;
        let mut executor_worker = channels.executor_worker;
        let mut result_worker = channels.result_worker;
        planner_worker.write_all(b"planner").unwrap();
        executor_worker.write_all(b"executor").unwrap();
        result_worker.write_all(b"result").unwrap();
        let mut planner_request = [0; 7];
        let mut executor_request = [0; 8];
        let mut result = [0; 6];
        planner_peer.read_exact(&mut planner_request).unwrap();
        executor_peer.read_exact(&mut executor_request).unwrap();
        result_peer.read_exact(&mut result).unwrap();
        assert_eq!(&planner_request, b"planner");
        assert_eq!(&executor_request, b"executor");
        assert_eq!(&result, b"result");
    }

    #[test]
    fn result_stream_enforces_chunk_and_cumulative_bounds() {
        use std::io::Write;

        let (mut worker, supervisor) = UnixStream::pair().unwrap();
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        worker.write_all(&3u32.to_be_bytes()).unwrap();
        worker.write_all(b"abc").unwrap();
        drop(worker);
        drain_result_stream(supervisor, sender).unwrap();
        assert_eq!(receiver.recv().unwrap().unwrap(), b"abc");
        assert!(receiver.recv().is_err());

        let (mut worker, supervisor) = UnixStream::pair().unwrap();
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        worker
            .write_all(&((crate::query_abi::MAX_RESULT_CHUNK_BYTES as u32) + 1).to_be_bytes())
            .unwrap();
        drop(worker);
        drain_result_stream(supervisor, sender).unwrap();
        assert_eq!(
            receiver.recv().unwrap().unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }
}
