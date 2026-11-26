//! Trusted disk-backed scratch streams for analytical query operators.
//!
//! The worker-facing interface is intentionally not a filesystem interface:
//! callers receive random handles and this module retains every file
//! descriptor. A supervisor must create a session only after authenticating
//! the worker channel and must destroy it on every terminal package path.

use crate::query_abi::MAX_IPC_REQUEST_BYTES;
use getrandom::fill as random_fill;
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::fs::File;
use std::io::{ErrorKind, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use std::{fmt, fmt::Formatter};

const HANDLE_BYTES: usize = 16;
const FILE_NAME_BYTES: usize = 16;
const DIRECTORY_NAME_BYTES: usize = 16;
pub const SPILL_CHANNEL_MAGIC: [u8; 4] = *b"PSPL";
pub const SPILL_CHANNEL_VERSION: u16 = 1;
pub const SPILL_CHANNEL_HEADER_BYTES: usize = 20;
pub const SPILL_OP_CREATE: u16 = 1;
pub const SPILL_OP_APPEND: u16 = 2;
pub const SPILL_OP_SEAL: u16 = 3;
pub const SPILL_OP_READ_AT: u16 = 4;
pub const SPILL_OP_DESTROY: u16 = 5;

#[derive(Clone, Debug)]
pub struct SpillConfig {
    pub parent: PathBuf,
    pub max_bytes_per_invocation: u64,
    pub max_io_bytes_per_invocation: u64,
    pub max_streams_per_invocation: u64,
    pub max_operations_per_invocation: u64,
    pub max_response_bytes: u64,
    pub max_service_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SpillInvocationLimits {
    pub max_bytes: u64,
    pub max_io_bytes: u64,
    pub max_streams: u64,
    pub max_operations: u64,
    pub max_response_bytes: u64,
}

impl SpillConfig {
    pub fn invocation_limits(&self) -> SpillInvocationLimits {
        SpillInvocationLimits {
            max_bytes: self.max_bytes_per_invocation,
            max_io_bytes: self.max_io_bytes_per_invocation,
            max_streams: self.max_streams_per_invocation,
            max_operations: self.max_operations_per_invocation,
            max_response_bytes: self.max_response_bytes,
        }
    }

    pub fn validate(&self) -> Result<(), SpillError> {
        if self.max_bytes_per_invocation == 0
            || self.max_io_bytes_per_invocation == 0
            || self.max_streams_per_invocation == 0
            || self.max_operations_per_invocation == 0
            || self.max_response_bytes == 0
            || self.max_service_bytes == 0
        {
            return Err(SpillError::InvalidConfiguration);
        }
        if self.max_service_bytes < self.max_bytes_per_invocation {
            return Err(SpillError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SpillHandle([u8; HANDLE_BYTES]);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SpillPackage {
    invocation_id: [u8; 16],
    package_id: [u8; 16],
}

impl SpillPackage {
    /// Construct from supervisor-owned admission state, never worker input.
    pub fn new(invocation_id: [u8; 16], package_id: [u8; 16]) -> Self {
        Self {
            invocation_id,
            package_id,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpillError {
    InvalidConfiguration,
    InvalidScratchRoot,
    UnsupportedFilesystem,
    InsufficientScratchCapacity,
    AllocationUnavailable,
    BrokerAlreadyRunning,
    DisabledForWorkload,
    ClosedSession,
    InvalidHandle,
    InvalidRange,
    StreamLimit,
    OperationLimit,
    IoLimit,
    AllocationLimit,
    ResponseLimit,
    Cancelled,
    Io,
    AccountingFailure,
    Protocol,
}

#[derive(Clone)]
pub struct SpillBroker(Arc<BrokerInner>);

impl fmt::Debug for SpillBroker {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SpillBroker")
            .field("enabled", &true)
            .field("max_service_bytes", &self.0.config.max_service_bytes)
            .finish_non_exhaustive()
    }
}

struct BrokerInner {
    config: SpillConfig,
    root: OwnedFd,
    allocation_unit: u64,
    _lock: OwnedFd,
    state: Mutex<BrokerState>,
    cancellation: Mutex<HashMap<SpillPackage, Arc<AtomicBool>>>,
    #[cfg(test)]
    append_io_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

struct BrokerState {
    service_reserved: u64,
    sessions: HashMap<SpillPackage, SessionState>,
    failed_closed: bool,
}

struct SessionState {
    limits: SpillInvocationLimits,
    deadline: Instant,
    cancelled: Arc<AtomicBool>,
    closed: bool,
    allocated: u64,
    io_bytes: u64,
    operations: u64,
    streams: HashMap<SpillHandle, StreamState>,
    directory: Option<OwnedFd>,
    directory_name: CString,
}

struct StreamState {
    file: File,
    length: u64,
    reserved: u64,
    sealed: bool,
    io_lock: Arc<Mutex<()>>,
}

impl SpillBroker {
    /// Open and validate the service-owned, disk-backed scratch directory.
    /// The directory must be owned by the service user and inaccessible to
    /// group/other users. This rejects memory-backed tmpfs scratch.
    pub fn open(config: SpillConfig) -> Result<Self, SpillError> {
        config.validate()?;
        let path = CString::new(config.parent.as_os_str().as_bytes())
            .map_err(|_| SpillError::InvalidScratchRoot)?;
        // SAFETY: path is NUL terminated and flags request a directory fd with
        // symlink traversal disabled for the final component.
        let raw = unsafe {
            libc::open(
                path.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            )
        };
        if raw < 0 {
            return Err(SpillError::InvalidScratchRoot);
        }
        // SAFETY: open returned a new owned descriptor.
        let root = unsafe { OwnedFd::from_raw_fd(raw) };
        let mut root_stat: libc::stat = unsafe { std::mem::zeroed() };
        // SAFETY: fstat writes metadata for the live directory descriptor.
        if unsafe { libc::fstat(root.as_raw_fd(), &mut root_stat) } != 0 {
            return Err(SpillError::Io);
        }
        if root_stat.st_mode & libc::S_IFMT != libc::S_IFDIR
            || root_stat.st_uid != unsafe { libc::geteuid() }
            || root_stat.st_mode & 0o077 != 0
        {
            return Err(SpillError::InvalidScratchRoot);
        }
        let mut stat: libc::statfs = unsafe { std::mem::zeroed() };
        // SAFETY: statfs writes into the initialized output struct for a valid fd.
        if unsafe { libc::fstatfs(root.as_raw_fd(), &mut stat) } != 0 {
            return Err(SpillError::Io);
        }
        if stat.f_type as libc::c_long == libc::TMPFS_MAGIC as libc::c_long
            || stat.f_type as libc::c_long == 0x8584_58f6
        {
            return Err(SpillError::UnsupportedFilesystem);
        }
        let allocation_unit = u64::try_from(stat.f_bsize)
            .ok()
            .filter(|value| *value > 0)
            .ok_or(SpillError::UnsupportedFilesystem)?;
        let mut capacity: libc::statvfs = unsafe { std::mem::zeroed() };
        // SAFETY: fstatvfs writes filesystem capacity for the live directory fd.
        if unsafe { libc::fstatvfs(root.as_raw_fd(), &mut capacity) } != 0 {
            return Err(SpillError::Io);
        }
        let available_bytes = capacity
            .f_bavail
            .checked_mul(capacity.f_frsize)
            .ok_or(SpillError::UnsupportedFilesystem)?;
        if config.max_service_bytes >= available_bytes {
            return Err(SpillError::InsufficientScratchCapacity);
        }
        let lock = lock_broker_root(root.as_raw_fd())?;
        recover_orphaned_entries(root.as_raw_fd())?;
        probe_physical_allocation(root.as_raw_fd(), allocation_unit)?;
        Ok(Self(Arc::new(BrokerInner {
            config,
            root,
            allocation_unit,
            _lock: lock,
            state: Mutex::new(BrokerState {
                service_reserved: 0,
                sessions: HashMap::new(),
                failed_closed: false,
            }),
            cancellation: Mutex::new(HashMap::new()),
            #[cfg(test)]
            append_io_hook: Mutex::new(None),
        })))
    }

    /// Bind a worker channel to trusted invocation/package state.
    pub fn open_package(
        &self,
        package: SpillPackage,
        limits: Option<SpillInvocationLimits>,
        deadline: Instant,
    ) -> Result<(), SpillError> {
        let host_limits = self.0.config.invocation_limits();
        let effective_limits = match limits {
            Some(l) => {
                if l.max_bytes == 0
                    || l.max_io_bytes == 0
                    || l.max_streams == 0
                    || l.max_operations == 0
                    || l.max_response_bytes == 0
                {
                    return Err(SpillError::DisabledForWorkload);
                }
                SpillInvocationLimits {
                    max_bytes: l.max_bytes.min(host_limits.max_bytes),
                    max_io_bytes: l.max_io_bytes.min(host_limits.max_io_bytes),
                    max_streams: l.max_streams.min(host_limits.max_streams),
                    max_operations: l.max_operations.min(host_limits.max_operations),
                    max_response_bytes: l.max_response_bytes.min(host_limits.max_response_bytes),
                }
            }
            None => host_limits,
        };
        let c_name = random_name(DIRECTORY_NAME_BYTES)?;
        let mut state = self
            .0
            .state
            .lock()
            .map_err(|_| SpillError::AccountingFailure)?;
        if state.failed_closed {
            return Err(SpillError::AccountingFailure);
        }
        if state.sessions.contains_key(&package) {
            return Err(SpillError::ClosedSession);
        }
        // SAFETY: root is a validated directory fd and name is generated hex.
        if unsafe { libc::mkdirat(self.0.root.as_raw_fd(), c_name.as_ptr(), 0o700) } != 0 {
            return Err(SpillError::Io);
        }
        // SAFETY: openat is confined to root, with no-follow and directory flags.
        let dir = unsafe {
            libc::openat(
                self.0.root.as_raw_fd(),
                c_name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            )
        };
        if dir < 0 {
            unsafe {
                libc::unlinkat(self.0.root.as_raw_fd(), c_name.as_ptr(), libc::AT_REMOVEDIR);
            }
            return Err(SpillError::Io);
        }
        let directory = unsafe { OwnedFd::from_raw_fd(dir) };
        let cancelled = Arc::new(AtomicBool::new(false));
        self.0
            .cancellation
            .lock()
            .map_err(|_| SpillError::AccountingFailure)?
            .insert(package, Arc::clone(&cancelled));
        state.sessions.insert(
            package,
            SessionState {
                limits: effective_limits,
                deadline,
                cancelled,
                closed: false,
                allocated: 0,
                io_bytes: 0,
                operations: 0,
                streams: HashMap::new(),
                directory: Some(directory),
                directory_name: c_name,
            },
        );
        Ok(())
    }

    pub fn create(&self, package: SpillPackage) -> Result<SpillHandle, SpillError> {
        let mut state = self
            .0
            .state
            .lock()
            .map_err(|_| SpillError::AccountingFailure)?;
        let session = session_mut(&mut state, package)?;
        charge_operation(session)?;
        if session.streams.len() as u64 >= session.limits.max_streams {
            return Err(SpillError::StreamLimit);
        }
        let directory = session
            .directory
            .as_ref()
            .ok_or(SpillError::ClosedSession)?;
        for _ in 0..8 {
            let c_name = random_name(FILE_NAME_BYTES)?;
            // SAFETY: generated basename and private directory fd; O_EXCL and
            // O_NOFOLLOW prevent replacement and symlink following.
            let fd = unsafe {
                libc::openat(
                    directory.as_raw_fd(),
                    c_name.as_ptr(),
                    libc::O_RDWR
                        | libc::O_CREAT
                        | libc::O_EXCL
                        | libc::O_CLOEXEC
                        | libc::O_NOFOLLOW,
                    0o600,
                )
            };
            if fd < 0 {
                if std::io::Error::last_os_error().raw_os_error() == Some(libc::EEXIST) {
                    continue;
                }
                return Err(SpillError::Io);
            }
            let file = unsafe { File::from_raw_fd(fd) };
            if unsafe { libc::unlinkat(directory.as_raw_fd(), c_name.as_ptr(), 0) } != 0 {
                state.failed_closed = true;
                return Err(SpillError::AccountingFailure);
            }
            let mut bytes = [0u8; HANDLE_BYTES];
            random_fill(&mut bytes).map_err(|_| SpillError::Io)?;
            let handle = SpillHandle(bytes);
            if session.streams.contains_key(&handle) {
                continue;
            }
            session.streams.insert(
                handle,
                StreamState {
                    file,
                    length: 0,
                    reserved: 0,
                    sealed: false,
                    io_lock: Arc::new(Mutex::new(())),
                },
            );
            return Ok(handle);
        }
        Err(SpillError::Io)
    }

    /// Append a contiguous payload. Disk blocks are reserved before the write
    /// is acknowledged; sparse growth is never used.
    pub fn append(
        &self,
        package: SpillPackage,
        handle: SpillHandle,
        bytes: &[u8],
    ) -> Result<(), SpillError> {
        if bytes.is_empty() || bytes.len() > MAX_IPC_REQUEST_BYTES as usize - HANDLE_BYTES {
            return Err(SpillError::ResponseLimit);
        }
        let io_lock = self.stream_io_lock(package, handle)?;
        let _io_guard = io_lock.lock().map_err(|_| SpillError::AccountingFailure)?;
        let allocation_unit = self.0.allocation_unit;
        let (file, offset, new_len, allocation_size, delta) = {
            let mut state = self
                .0
                .state
                .lock()
                .map_err(|_| SpillError::AccountingFailure)?;
            if state.failed_closed {
                return Err(SpillError::AccountingFailure);
            }
            let service_cap = self.0.config.max_service_bytes;
            let service_reserved = state.service_reserved;
            let session = session_mut(&mut state, package)?;
            let per_invocation_cap = session.limits.max_bytes;
            charge_operation(session)?;
            charge_io(session, bytes.len() as u64)?;
            let stream = session
                .streams
                .get_mut(&handle)
                .ok_or(SpillError::InvalidHandle)?;
            if !Arc::ptr_eq(&io_lock, &stream.io_lock) {
                return Err(SpillError::InvalidHandle);
            }
            if stream.sealed {
                return Err(SpillError::ClosedSession);
            }
            let offset = stream.length;
            let new_len = offset
                .checked_add(bytes.len() as u64)
                .ok_or(SpillError::AllocationLimit)?;
            let wanted = round_allocation(new_len, allocation_unit)?;
            let allocation_size =
                libc::off_t::try_from(wanted).map_err(|_| SpillError::AllocationLimit)?;
            let delta = wanted.saturating_sub(stream.reserved);
            if session
                .allocated
                .checked_add(delta)
                .ok_or(SpillError::AllocationLimit)?
                > per_invocation_cap
            {
                return Err(SpillError::AllocationLimit);
            }
            if service_reserved
                .checked_add(delta)
                .ok_or(SpillError::AllocationLimit)?
                > service_cap
            {
                return Err(SpillError::AllocationLimit);
            }
            let file = stream.file.try_clone().map_err(|_| SpillError::Io)?;
            stream.reserved = wanted;
            session.allocated += delta;
            state.service_reserved += delta;
            (file, offset, new_len, allocation_size, delta)
        };

        if delta > 0 {
            #[cfg(test)]
            if let Some(hook) = self
                .0
                .append_io_hook
                .lock()
                .map_err(|_| SpillError::AccountingFailure)?
                .clone()
            {
                hook();
            }
            // SAFETY: posix_fallocate operates on the broker-owned regular file.
            let result = unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, allocation_size) };
            if result != 0 {
                let mut state = self
                    .0
                    .state
                    .lock()
                    .map_err(|_| SpillError::AccountingFailure)?;
                let session = state
                    .sessions
                    .get_mut(&package)
                    .ok_or(SpillError::ClosedSession)?;
                let stream = session
                    .streams
                    .get_mut(&handle)
                    .ok_or(SpillError::InvalidHandle)?;
                stream.reserved = stream
                    .reserved
                    .checked_sub(delta)
                    .ok_or(SpillError::AccountingFailure)?;
                session.allocated = session
                    .allocated
                    .checked_sub(delta)
                    .ok_or(SpillError::AccountingFailure)?;
                state.service_reserved = state
                    .service_reserved
                    .checked_sub(delta)
                    .ok_or(SpillError::AccountingFailure)?;
                return Err(SpillError::Io);
            }
        }

        let mut written = 0usize;
        while written < bytes.len() {
            match file.write_at(&bytes[written..], offset + written as u64) {
                Ok(0) => {
                    self.poison_after_spill_io_failure()?;
                    return Err(SpillError::AccountingFailure);
                }
                Ok(count) => written += count,
                Err(error) if error.kind() == ErrorKind::Interrupted => continue,
                Err(_) => {
                    self.poison_after_spill_io_failure()?;
                    return Err(SpillError::AccountingFailure);
                }
            }
        }

        let mut state = self
            .0
            .state
            .lock()
            .map_err(|_| SpillError::AccountingFailure)?;
        let session = state
            .sessions
            .get_mut(&package)
            .ok_or(SpillError::ClosedSession)?;
        let stream = session
            .streams
            .get_mut(&handle)
            .ok_or(SpillError::InvalidHandle)?;
        if !Arc::ptr_eq(&io_lock, &stream.io_lock) || stream.length != offset {
            state.failed_closed = true;
            return Err(SpillError::AccountingFailure);
        }
        stream.length = new_len;
        Ok(())
    }

    fn stream_io_lock(
        &self,
        package: SpillPackage,
        handle: SpillHandle,
    ) -> Result<Arc<Mutex<()>>, SpillError> {
        let mut state = self
            .0
            .state
            .lock()
            .map_err(|_| SpillError::AccountingFailure)?;
        let session = session_mut(&mut state, package)?;
        session
            .streams
            .get(&handle)
            .map(|stream| Arc::clone(&stream.io_lock))
            .ok_or(SpillError::InvalidHandle)
    }

    fn poison_after_spill_io_failure(&self) -> Result<(), SpillError> {
        self.0
            .state
            .lock()
            .map_err(|_| SpillError::AccountingFailure)?
            .failed_closed = true;
        Ok(())
    }

    pub fn seal(&self, package: SpillPackage, handle: SpillHandle) -> Result<(), SpillError> {
        let io_lock = self.stream_io_lock(package, handle)?;
        let _io_guard = io_lock.lock().map_err(|_| SpillError::AccountingFailure)?;
        let file = {
            let mut state = self
                .0
                .state
                .lock()
                .map_err(|_| SpillError::AccountingFailure)?;
            let session = session_mut(&mut state, package)?;
            charge_operation(session)?;
            let stream = session
                .streams
                .get(&handle)
                .ok_or(SpillError::InvalidHandle)?;
            if !Arc::ptr_eq(&io_lock, &stream.io_lock) {
                return Err(SpillError::InvalidHandle);
            }
            stream.file.try_clone().map_err(|_| SpillError::Io)?
        };
        file.sync_data().map_err(|_| SpillError::Io)?;
        let mut state = self
            .0
            .state
            .lock()
            .map_err(|_| SpillError::AccountingFailure)?;
        let session = state
            .sessions
            .get_mut(&package)
            .ok_or(SpillError::ClosedSession)?;
        let stream = session
            .streams
            .get_mut(&handle)
            .ok_or(SpillError::InvalidHandle)?;
        if !Arc::ptr_eq(&io_lock, &stream.io_lock) {
            return Err(SpillError::InvalidHandle);
        }
        stream.sealed = true;
        Ok(())
    }

    pub fn read_at(
        &self,
        package: SpillPackage,
        handle: SpillHandle,
        offset: u64,
        out: &mut [u8],
    ) -> Result<usize, SpillError> {
        let io_lock = self.stream_io_lock(package, handle)?;
        let _io_guard = io_lock.lock().map_err(|_| SpillError::AccountingFailure)?;
        let (file, count) = {
            let mut state = self
                .0
                .state
                .lock()
                .map_err(|_| SpillError::AccountingFailure)?;
            let session = session_mut(&mut state, package)?;
            if out.len() as u64 > session.limits.max_response_bytes {
                return Err(SpillError::ResponseLimit);
            }
            charge_operation(session)?;
            let stream = session
                .streams
                .get(&handle)
                .ok_or(SpillError::InvalidHandle)?;
            if !Arc::ptr_eq(&io_lock, &stream.io_lock) {
                return Err(SpillError::InvalidHandle);
            }
            if !stream.sealed {
                return Err(SpillError::ClosedSession);
            }
            if offset > stream.length {
                return Err(SpillError::InvalidRange);
            }
            let available = stream.length - offset;
            let count = usize::try_from(available.min(out.len() as u64))
                .map_err(|_| SpillError::InvalidRange)?;
            let file = stream.file.try_clone().map_err(|_| SpillError::Io)?;
            charge_io(session, count as u64)?;
            (file, count)
        };
        let mut read = 0usize;
        while read < count {
            match file.read_at(&mut out[read..count], offset + read as u64) {
                Ok(0) => return Err(SpillError::Io),
                Ok(length) => read += length,
                Err(error) if error.kind() == ErrorKind::Interrupted => continue,
                Err(_) => return Err(SpillError::Io),
            }
        }
        Ok(count)
    }

    pub fn destroy(&self, package: SpillPackage, handle: SpillHandle) -> Result<(), SpillError> {
        let io_lock = self.stream_io_lock(package, handle)?;
        let _io_guard = io_lock.lock().map_err(|_| SpillError::AccountingFailure)?;
        let mut state = self
            .0
            .state
            .lock()
            .map_err(|_| SpillError::AccountingFailure)?;
        let session = session_mut(&mut state, package)?;
        charge_operation(session)?;
        if !session
            .streams
            .get(&handle)
            .is_some_and(|stream| Arc::ptr_eq(&io_lock, &stream.io_lock))
        {
            return Err(SpillError::InvalidHandle);
        }
        let stream = session
            .streams
            .remove(&handle)
            .ok_or(SpillError::InvalidHandle)?;
        session.allocated = session
            .allocated
            .checked_sub(stream.reserved)
            .ok_or(SpillError::AccountingFailure)?;
        state.service_reserved = state
            .service_reserved
            .checked_sub(stream.reserved)
            .ok_or(SpillError::AccountingFailure)?;
        drop(stream);
        Ok(())
    }

    pub fn cancel(&self, package: SpillPackage) -> Result<(), SpillError> {
        let cancellation = self
            .0
            .cancellation
            .lock()
            .map_err(|_| SpillError::AccountingFailure)?
            .get(&package)
            .cloned()
            .ok_or(SpillError::ClosedSession)?;
        cancellation.store(true, Ordering::Release);
        Ok(())
    }

    /// Close package namespace and release every unlinked descriptor. Cleanup
    /// inconsistencies poison the broker so later admissions fail closed.
    pub fn close_package(&self, package: SpillPackage) -> Result<(), SpillError> {
        let stream_locks = {
            let mut state = self
                .0
                .state
                .lock()
                .map_err(|_| SpillError::AccountingFailure)?;
            let Some(session) = state.sessions.get_mut(&package) else {
                return Ok(());
            };
            session.closed = true;
            session
                .streams
                .values()
                .map(|stream| Arc::clone(&stream.io_lock))
                .collect::<Vec<_>>()
        };
        // Drain any operation that passed admission before `closed` was set.
        // New operations fail session_mut and cannot enter disk I/O.
        for io_lock in stream_locks {
            drop(io_lock.lock().map_err(|_| SpillError::AccountingFailure)?);
        }

        let mut state = self
            .0
            .state
            .lock()
            .map_err(|_| SpillError::AccountingFailure)?;
        let Some(mut session) = state.sessions.remove(&package) else {
            return Ok(());
        };
        let released = session.allocated;
        session.streams.clear();
        session.directory.take();
        if unsafe {
            libc::unlinkat(
                self.0.root.as_raw_fd(),
                session.directory_name.as_ptr(),
                libc::AT_REMOVEDIR,
            )
        } != 0
        {
            state.failed_closed = true;
            return Err(SpillError::AccountingFailure);
        }
        if released > state.service_reserved {
            state.failed_closed = true;
            return Err(SpillError::AccountingFailure);
        }
        state.service_reserved -= released;
        self.0
            .cancellation
            .lock()
            .map_err(|_| SpillError::AccountingFailure)?
            .remove(&package);
        Ok(())
    }

    /// Serve the versioned, identity-free worker byte-stream protocol. The
    /// connected channel is provisioned by the supervisor for exactly one
    /// package; no request field can select identity, quota, or filesystem
    /// location. One active serve loop per admitted worker bounds concurrency
    /// by the supervisor's analytical admission limit.
    pub fn serve_channel(
        &self,
        mut stream: std::os::unix::net::UnixStream,
        package: SpillPackage,
    ) -> Result<(), SpillError> {
        use std::time::Duration;
        let result = (|| {
            stream
                .set_read_timeout(Some(Duration::from_millis(50)))
                .map_err(|_| SpillError::Io)?;
            stream
                .set_write_timeout(Some(Duration::from_millis(50)))
                .map_err(|_| SpillError::Io)?;
            let mut last_request_id = 0u64;
            loop {
                let mut header = [0u8; SPILL_CHANNEL_HEADER_BYTES];
                if !self.read_cancellable(&mut stream, package, &mut header, true)? {
                    return Ok(());
                }
                if header[..4] != SPILL_CHANNEL_MAGIC
                    || u16::from_be_bytes([header[4], header[5]]) != SPILL_CHANNEL_VERSION
                {
                    return Err(SpillError::Protocol);
                }
                let operation = u16::from_be_bytes([header[6], header[7]]);
                let request_id =
                    u64::from_be_bytes(header[8..16].try_into().map_err(|_| SpillError::Protocol)?);
                let payload_len = u32::from_be_bytes(
                    header[16..20]
                        .try_into()
                        .map_err(|_| SpillError::Protocol)?,
                ) as usize;
                if request_id == 0
                    || request_id <= last_request_id
                    || payload_len > MAX_IPC_REQUEST_BYTES as usize
                {
                    return Err(SpillError::Protocol);
                }
                last_request_id = request_id;
                let mut payload = vec![0u8; payload_len];
                if payload_len > 0
                    && !self.read_cancellable(&mut stream, package, &mut payload, false)?
                {
                    return Err(SpillError::Protocol);
                }
                let (status, response) =
                    match self.dispatch_channel_operation(package, operation, &payload) {
                        Ok(response) => (0u16, response),
                        Err(error) => (spill_status(error), Vec::new()),
                    };
                let response_len =
                    u32::try_from(response.len() + 2).map_err(|_| SpillError::Protocol)?;
                let mut response_header = [0u8; SPILL_CHANNEL_HEADER_BYTES];
                response_header[..4].copy_from_slice(&SPILL_CHANNEL_MAGIC);
                response_header[4..6].copy_from_slice(&SPILL_CHANNEL_VERSION.to_be_bytes());
                response_header[6..8].copy_from_slice(&(operation | 0x8000).to_be_bytes());
                response_header[8..16].copy_from_slice(&request_id.to_be_bytes());
                response_header[16..20].copy_from_slice(&response_len.to_be_bytes());
                self.write_cancellable(&mut stream, package, &response_header)?;
                self.write_cancellable(&mut stream, package, &status.to_be_bytes())?;
                if !response.is_empty() {
                    self.write_cancellable(&mut stream, package, &response)?;
                }
            }
        })();
        let cleanup = self.close_package(package);
        match (result, cleanup) {
            (Err(error), _) => Err(error),
            (Ok(()), result) => result,
        }
    }

    fn dispatch_channel_operation(
        &self,
        package: SpillPackage,
        operation: u16,
        payload: &[u8],
    ) -> Result<Vec<u8>, SpillError> {
        match operation {
            SPILL_OP_CREATE if payload.is_empty() => Ok(self.create(package)?.0.to_vec()),
            SPILL_OP_APPEND if payload.len() > HANDLE_BYTES => {
                let handle = decode_handle(&payload[..HANDLE_BYTES])?;
                self.append(package, handle, &payload[HANDLE_BYTES..])?;
                Ok(Vec::new())
            }
            SPILL_OP_SEAL if payload.len() == HANDLE_BYTES => {
                self.seal(package, decode_handle(payload)?)?;
                Ok(Vec::new())
            }
            SPILL_OP_READ_AT if payload.len() == HANDLE_BYTES + 12 => {
                let handle = decode_handle(&payload[..HANDLE_BYTES])?;
                let offset = u64::from_be_bytes(
                    payload[HANDLE_BYTES..HANDLE_BYTES + 8]
                        .try_into()
                        .map_err(|_| SpillError::Protocol)?,
                );
                let length = u32::from_be_bytes(
                    payload[HANDLE_BYTES + 8..]
                        .try_into()
                        .map_err(|_| SpillError::Protocol)?,
                ) as usize;
                if length as u64 > self.0.config.max_response_bytes {
                    return Err(SpillError::ResponseLimit);
                }
                let mut bytes = vec![0u8; length];
                let count = self.read_at(package, handle, offset, &mut bytes)?;
                bytes.truncate(count);
                Ok(bytes)
            }
            SPILL_OP_DESTROY if payload.len() == HANDLE_BYTES => {
                self.destroy(package, decode_handle(payload)?)?;
                Ok(Vec::new())
            }
            _ => Err(SpillError::Protocol),
        }
    }

    fn read_cancellable(
        &self,
        stream: &mut std::os::unix::net::UnixStream,
        package: SpillPackage,
        output: &mut [u8],
        allow_clean_eof: bool,
    ) -> Result<bool, SpillError> {
        let mut offset = 0usize;
        while offset < output.len() {
            self.ensure_active(package)?;
            match stream.read(&mut output[offset..]) {
                Ok(0) if offset == 0 && allow_clean_eof => return Ok(false),
                Ok(0) => return Err(SpillError::Protocol),
                Ok(count) => offset += count,
                Err(error) if error.kind() == ErrorKind::Interrupted => continue,
                Err(error)
                    if error.kind() == ErrorKind::WouldBlock
                        || error.kind() == ErrorKind::TimedOut =>
                {
                    continue;
                }
                Err(_) => return Err(SpillError::Io),
            }
        }
        Ok(true)
    }

    fn write_cancellable(
        &self,
        stream: &mut std::os::unix::net::UnixStream,
        package: SpillPackage,
        bytes: &[u8],
    ) -> Result<(), SpillError> {
        let mut offset = 0usize;
        while offset < bytes.len() {
            self.ensure_active(package)?;
            match stream.write(&bytes[offset..]) {
                Ok(0) => return Err(SpillError::Io),
                Ok(count) => offset += count,
                Err(error) if error.kind() == ErrorKind::Interrupted => continue,
                Err(error)
                    if error.kind() == ErrorKind::WouldBlock
                        || error.kind() == ErrorKind::TimedOut =>
                {
                    continue;
                }
                Err(_) => return Err(SpillError::Io),
            }
        }
        Ok(())
    }

    fn ensure_active(&self, package: SpillPackage) -> Result<(), SpillError> {
        let mut state = self
            .0
            .state
            .lock()
            .map_err(|_| SpillError::AccountingFailure)?;
        session_mut(&mut state, package).map(|_| ())
    }
}

fn lock_broker_root(root_fd: i32) -> Result<OwnedFd, SpillError> {
    let name = c".prdb-spill.lock";
    // SAFETY: root_fd is the validated broker root; the lock name is constant.
    let raw = unsafe {
        libc::openat(
            root_fd,
            name.as_ptr(),
            libc::O_RDWR | libc::O_CREAT | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0o600,
        )
    };
    if raw < 0 {
        return Err(SpillError::InvalidScratchRoot);
    }
    // SAFETY: openat returned a unique owned descriptor.
    let lock = unsafe { OwnedFd::from_raw_fd(raw) };
    let mut metadata: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstat(lock.as_raw_fd(), &mut metadata) } != 0
        || metadata.st_mode & libc::S_IFMT != libc::S_IFREG
        || metadata.st_uid != unsafe { libc::geteuid() }
        || metadata.st_mode & 0o077 != 0
    {
        return Err(SpillError::InvalidScratchRoot);
    }
    // SAFETY: flock is applied to the broker's live lock file descriptor.
    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err(SpillError::BrokerAlreadyRunning);
    }
    Ok(lock)
}

fn recover_orphaned_entries(root_fd: i32) -> Result<(), SpillError> {
    // SAFETY: duplicate root_fd; fdopendir takes ownership of the duplicate.
    let duplicate = unsafe { libc::fcntl(root_fd, libc::F_DUPFD_CLOEXEC, 3) };
    if duplicate < 0 {
        return Err(SpillError::Io);
    }
    let directory = unsafe { libc::fdopendir(duplicate) };
    if directory.is_null() {
        unsafe {
            libc::close(duplicate);
        }
        return Err(SpillError::Io);
    }
    let mut result = Ok(());
    loop {
        unsafe {
            *libc::__errno_location() = 0;
        }
        let entry = unsafe { libc::readdir(directory) };
        if entry.is_null() {
            if unsafe { *libc::__errno_location() } != 0 {
                result = Err(SpillError::Io);
            }
            break;
        }
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
        if matches!(name.to_bytes(), b"." | b".." | b".prdb-spill.lock") {
            continue;
        }
        if !is_spill_name(name.to_bytes()) {
            result = Err(SpillError::InvalidScratchRoot);
            break;
        }
        let mut metadata: libc::stat = unsafe { std::mem::zeroed() };
        if unsafe {
            libc::fstatat(
                root_fd,
                name.as_ptr(),
                &mut metadata,
                libc::AT_SYMLINK_NOFOLLOW,
            )
        } != 0
        {
            result = Err(SpillError::Io);
            break;
        }
        let kind = metadata.st_mode & libc::S_IFMT;
        if kind != libc::S_IFDIR && kind != libc::S_IFREG && kind != libc::S_IFLNK {
            result = Err(SpillError::InvalidScratchRoot);
            break;
        }
        let cleanup = if kind == libc::S_IFDIR {
            cleanup_orphan_directory(root_fd, name)
        } else {
            unsafe { libc::unlinkat(root_fd, name.as_ptr(), 0) }
        };
        if cleanup != 0 {
            result = Err(SpillError::AccountingFailure);
            break;
        }
    }
    unsafe {
        libc::closedir(directory);
    }
    result
}

fn cleanup_orphan_directory(root_fd: i32, name: &CStr) -> libc::c_int {
    // SAFETY: root_fd is trusted and the name has been checked as a broker
    // generated basename. No symlink following is allowed.
    let directory_fd = unsafe {
        libc::openat(
            root_fd,
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if directory_fd < 0 {
        return -1;
    }
    let duplicate = unsafe { libc::fcntl(directory_fd, libc::F_DUPFD_CLOEXEC, 3) };
    if duplicate < 0 {
        unsafe {
            libc::close(directory_fd);
        }
        return -1;
    }
    let directory = unsafe { libc::fdopendir(duplicate) };
    if directory.is_null() {
        unsafe {
            libc::close(duplicate);
            libc::close(directory_fd);
        }
        return -1;
    }
    let mut result = 0;
    loop {
        unsafe {
            *libc::__errno_location() = 0;
        }
        let entry = unsafe { libc::readdir(directory) };
        if entry.is_null() {
            if unsafe { *libc::__errno_location() } != 0 {
                result = -1;
            }
            break;
        }
        let entry_name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
        if matches!(entry_name.to_bytes(), b"." | b"..") {
            continue;
        }
        let mut metadata: libc::stat = unsafe { std::mem::zeroed() };
        if !is_spill_name(entry_name.to_bytes())
            || unsafe {
                libc::fstatat(
                    directory_fd,
                    entry_name.as_ptr(),
                    &mut metadata,
                    libc::AT_SYMLINK_NOFOLLOW,
                )
            } != 0
            || metadata.st_mode & libc::S_IFMT != libc::S_IFREG
            || unsafe { libc::unlinkat(directory_fd, entry_name.as_ptr(), 0) } != 0
        {
            result = -1;
            break;
        }
    }
    unsafe {
        libc::closedir(directory);
        libc::close(directory_fd);
    }
    if result != 0 {
        return result;
    }
    unsafe { libc::unlinkat(root_fd, name.as_ptr(), libc::AT_REMOVEDIR) }
}

fn is_spill_name(bytes: &[u8]) -> bool {
    bytes.len() == FILE_NAME_BYTES * 2
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn probe_physical_allocation(root_fd: i32, allocation_unit: u64) -> Result<(), SpillError> {
    let name = random_name(FILE_NAME_BYTES)?;
    let length =
        libc::off_t::try_from(allocation_unit).map_err(|_| SpillError::AllocationUnavailable)?;
    // SAFETY: name is a cryptographically random basename beneath the trusted
    // root; exclusive creation prevents replacement and follows no symlinks.
    let fd = unsafe {
        libc::openat(
            root_fd,
            name.as_ptr(),
            libc::O_RDWR | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0o600,
        )
    };
    if fd < 0 {
        return Err(SpillError::Io);
    }
    // SAFETY: openat returned a unique owned file descriptor.
    let file = unsafe { File::from_raw_fd(fd) };
    // SAFETY: posix_fallocate reserves physical blocks for this broker-owned file.
    let allocation_result = unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, length) };
    // Remove the name while the trusted descriptor is established; closing it
    // below releases the startup probe's blocks immediately.
    let unlink_result = unsafe { libc::unlinkat(root_fd, name.as_ptr(), 0) };
    drop(file);
    if unlink_result != 0 {
        return Err(SpillError::AccountingFailure);
    }
    if allocation_result != 0 {
        return Err(SpillError::AllocationUnavailable);
    }
    Ok(())
}

fn decode_handle(bytes: &[u8]) -> Result<SpillHandle, SpillError> {
    let handle: [u8; HANDLE_BYTES] = bytes.try_into().map_err(|_| SpillError::Protocol)?;
    Ok(SpillHandle(handle))
}

fn spill_status(error: SpillError) -> u16 {
    match error {
        SpillError::InvalidHandle => 1,
        SpillError::InvalidRange => 2,
        SpillError::StreamLimit => 3,
        SpillError::OperationLimit => 4,
        SpillError::IoLimit => 5,
        SpillError::AllocationLimit => 6,
        SpillError::ResponseLimit => 7,
        SpillError::Cancelled => 8,
        SpillError::DisabledForWorkload => 9,
        SpillError::ClosedSession => 10,
        _ => 255,
    }
}

fn session_mut(
    state: &mut BrokerState,
    package: SpillPackage,
) -> Result<&mut SessionState, SpillError> {
    if state.failed_closed {
        return Err(SpillError::AccountingFailure);
    }
    let session = state
        .sessions
        .get_mut(&package)
        .ok_or(SpillError::ClosedSession)?;
    if session.closed {
        return Err(SpillError::ClosedSession);
    }
    if session.cancelled.load(Ordering::Acquire) {
        return Err(SpillError::Cancelled);
    }
    if Instant::now() >= session.deadline {
        return Err(SpillError::Cancelled);
    }
    Ok(session)
}

fn charge_operation(session: &mut SessionState) -> Result<(), SpillError> {
    session.operations = session
        .operations
        .checked_add(1)
        .ok_or(SpillError::OperationLimit)?;
    if session.operations > session.limits.max_operations {
        return Err(SpillError::OperationLimit);
    }
    Ok(())
}

fn charge_io(session: &mut SessionState, bytes: u64) -> Result<(), SpillError> {
    let next = session
        .io_bytes
        .checked_add(bytes)
        .ok_or(SpillError::IoLimit)?;
    if next > session.limits.max_io_bytes {
        return Err(SpillError::IoLimit);
    }
    session.io_bytes = next;
    Ok(())
}

fn round_allocation(length: u64, unit: u64) -> Result<u64, SpillError> {
    length
        .checked_add(unit - 1)
        .map(|value| value / unit * unit)
        .ok_or(SpillError::AllocationLimit)
}

fn random_name(bytes: usize) -> Result<CString, SpillError> {
    let mut random = vec![0u8; bytes];
    random_fill(&mut random).map_err(|_| SpillError::Io)?;
    let mut name = Vec::with_capacity(bytes * 2);
    for byte in random {
        name.extend_from_slice(format!("{byte:02x}").as_bytes());
    }
    CString::new(name).map_err(|_| SpillError::Io)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use std::time::Duration;

    struct Scratch(PathBuf);

    impl Scratch {
        fn new() -> Self {
            Self::new_in(&std::env::temp_dir())
        }

        fn new_in(parent: &Path) -> Self {
            let mut suffix = [0u8; 8];
            random_fill(&mut suffix).unwrap();
            let path = parent.join(format!(
                "prdb-spill-test-{}-{}",
                std::process::id(),
                suffix
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            ));
            std::fs::create_dir(&path).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
            Self(path)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn config(parent: &Path) -> SpillConfig {
        SpillConfig {
            parent: parent.to_owned(),
            max_bytes_per_invocation: 8192,
            max_io_bytes_per_invocation: 32768,
            max_streams_per_invocation: 4,
            max_operations_per_invocation: 32,
            max_response_bytes: 4096,
            max_service_bytes: 16384,
        }
    }

    fn package(id: u8) -> SpillPackage {
        SpillPackage::new([id; 16], [id.wrapping_add(1); 16])
    }

    #[test]
    fn streams_are_private_immutable_and_reclaimed_with_the_package() {
        let scratch = Scratch::new();
        let broker = SpillBroker::open(config(&scratch.0)).unwrap();
        let owner = package(1);
        let other = package(2);
        let deadline = Instant::now() + Duration::from_secs(10);
        broker.open_package(owner, None, deadline).unwrap();
        broker.open_package(other, None, deadline).unwrap();
        let stream = broker.create(owner).unwrap();
        broker.append(owner, stream, b"authorized bytes").unwrap();
        broker.seal(owner, stream).unwrap();

        let mut output = [0u8; 32];
        let length = broker.read_at(owner, stream, 0, &mut output).unwrap();
        assert_eq!(&output[..length], b"authorized bytes");
        assert_eq!(
            broker.read_at(other, stream, 0, &mut output),
            Err(SpillError::InvalidHandle)
        );
        assert_eq!(
            broker.append(owner, stream, b"later"),
            Err(SpillError::ClosedSession)
        );
        let visible_entries = std::fs::read_dir(&scratch.0)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name() != ".prdb-spill.lock")
            .count();
        assert_eq!(visible_entries, 2);

        broker.close_package(owner).unwrap();
        broker.close_package(other).unwrap();
        let visible_entries = std::fs::read_dir(&scratch.0)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name() != ".prdb-spill.lock")
            .count();
        assert_eq!(visible_entries, 0);
    }

    #[test]
    fn quotas_and_workload_class_fail_closed() {
        let scratch = Scratch::new();
        let mut limits = config(&scratch.0);
        limits.max_bytes_per_invocation = 4096;
        limits.max_service_bytes = 4096;
        let broker = SpillBroker::open(limits).unwrap();
        let analytical = package(3);
        let operational = package(4);
        let deadline = Instant::now() + Duration::from_secs(10);
        assert_eq!(
            broker.open_package(
                operational,
                Some(SpillInvocationLimits {
                    max_bytes: 0,
                    max_io_bytes: 0,
                    max_streams: 0,
                    max_operations: 0,
                    max_response_bytes: 0,
                }),
                deadline,
            ),
            Err(SpillError::DisabledForWorkload)
        );
        broker.open_package(analytical, None, deadline).unwrap();
        let stream = broker.create(analytical).unwrap();
        assert_eq!(
            broker.append(analytical, stream, &vec![7; 4097]),
            Err(SpillError::AllocationLimit)
        );
        broker.append(analytical, stream, &vec![7; 4096]).unwrap();
        assert_eq!(
            broker.append(analytical, stream, b"x"),
            Err(SpillError::AllocationLimit)
        );
        broker.close_package(analytical).unwrap();
    }

    #[test]
    fn concurrent_packages_cannot_overreserve_service_allocation() {
        let scratch = Scratch::new();
        let mut limits = config(&scratch.0);
        let allocation_unit = u64::try_from(unsafe {
            let mut stat: libc::statfs = std::mem::zeroed();
            let path = CString::new(scratch.0.as_os_str().as_bytes()).unwrap();
            let fd = libc::open(
                path.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
            );
            assert!(fd >= 0);
            let result = libc::fstatfs(fd, &mut stat);
            libc::close(fd);
            assert_eq!(result, 0);
            stat.f_bsize
        })
        .unwrap();
        limits.max_bytes_per_invocation = allocation_unit;
        limits.max_service_bytes = allocation_unit;
        let broker = SpillBroker::open(limits).unwrap();
        let owners = [package(31), package(32)];
        let deadline = Instant::now() + Duration::from_secs(10);
        let handles = owners.map(|owner| {
            broker.open_package(owner, None, deadline).unwrap();
            broker.create(owner).unwrap()
        });
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let results = owners
            .into_iter()
            .zip(handles)
            .map(|(owner, handle)| {
                let broker = broker.clone();
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    broker.append(owner, handle, b"x")
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let results = results
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            results.iter().filter(|result| result.is_ok()).count(),
            1,
            "one allocation unit of service capacity can admit only one package"
        );
        assert_eq!(
            results
                .iter()
                .filter(|result| **result == Err(SpillError::AllocationLimit))
                .count(),
            1
        );

        let admitted = results.iter().position(Result::is_ok).unwrap();
        let waiting = 1 - admitted;
        broker.close_package(owners[admitted]).unwrap();
        assert_eq!(
            broker.append(owners[waiting], handles[waiting], b"x"),
            Ok(())
        );
        broker.close_package(owners[waiting]).unwrap();
    }

    #[test]
    fn cancellation_invalidates_all_handles_for_the_package() {
        let scratch = Scratch::new();
        let broker = SpillBroker::open(config(&scratch.0)).unwrap();
        let owner = package(5);
        broker
            .open_package(owner, None, Instant::now() + Duration::from_secs(10))
            .unwrap();
        let stream = broker.create(owner).unwrap();
        broker.cancel(owner).unwrap();
        assert_eq!(
            broker.append(owner, stream, b"after cancel"),
            Err(SpillError::Cancelled)
        );
        assert_eq!(broker.create(owner), Err(SpillError::Cancelled));
        broker.close_package(owner).unwrap();
    }

    #[test]
    fn cancelled_partial_channel_does_not_block_another_package() {
        let scratch = Scratch::new();
        let mut limits = config(&scratch.0);
        limits.max_bytes_per_invocation = 256 * 1024;
        limits.max_service_bytes = 512 * 1024;
        let broker = SpillBroker::open(limits).unwrap();
        let stalled_package = package(13);
        let active_package = package(14);
        let deadline = Instant::now() + Duration::from_secs(10);
        broker
            .open_package(stalled_package, None, deadline)
            .unwrap();
        broker.open_package(active_package, None, deadline).unwrap();

        let (mut stalled_client, stalled_server) = std::os::unix::net::UnixStream::pair().unwrap();
        let serving_broker = broker.clone();
        let stalled_thread = std::thread::spawn(move || {
            serving_broker.serve_channel(stalled_server, stalled_package)
        });
        let create_response = channel_request(&mut stalled_client, SPILL_OP_CREATE, 1, &[]);
        assert_eq!(&create_response[..2], &0u16.to_be_bytes());
        let stalled_handle: [u8; HANDLE_BYTES] = create_response[2..].try_into().unwrap();

        let mut partial_append = [0u8; SPILL_CHANNEL_HEADER_BYTES];
        partial_append[..4].copy_from_slice(&SPILL_CHANNEL_MAGIC);
        partial_append[4..6].copy_from_slice(&SPILL_CHANNEL_VERSION.to_be_bytes());
        partial_append[6..8].copy_from_slice(&SPILL_OP_APPEND.to_be_bytes());
        partial_append[8..16].copy_from_slice(&2u64.to_be_bytes());
        partial_append[16..20].copy_from_slice(&4096u32.to_be_bytes());
        stalled_client.write_all(&partial_append).unwrap();
        stalled_client.write_all(&stalled_handle).unwrap();

        let (mut active_client, active_server) = std::os::unix::net::UnixStream::pair().unwrap();
        let serving_broker = broker.clone();
        let active_thread =
            std::thread::spawn(move || serving_broker.serve_channel(active_server, active_package));
        let active_create = channel_request(&mut active_client, SPILL_OP_CREATE, 1, &[]);
        assert_eq!(&active_create[..2], &0u16.to_be_bytes());
        let active_handle: [u8; HANDLE_BYTES] = active_create[2..].try_into().unwrap();
        let mut append = active_handle.to_vec();
        append.extend_from_slice(&[0x5a; 2048]);
        assert_eq!(
            channel_request(&mut active_client, SPILL_OP_APPEND, 2, &append),
            0u16.to_be_bytes(),
            "another package must keep making spill progress"
        );

        broker.cancel(stalled_package).unwrap();
        assert_eq!(stalled_thread.join().unwrap(), Err(SpillError::Cancelled));
        drop(stalled_client);
        drop(active_client);
        assert_eq!(active_thread.join().unwrap(), Ok(()));
        broker.close_package(stalled_package).unwrap();
        broker.close_package(active_package).unwrap();
    }

    #[test]
    fn one_package_append_io_does_not_hold_the_service_state_lock() {
        let scratch = Scratch::new();
        let broker = SpillBroker::open(config(&scratch.0)).unwrap();
        let blocked_package = package(15);
        let active_package = package(16);
        let deadline = Instant::now() + Duration::from_secs(10);
        broker
            .open_package(blocked_package, None, deadline)
            .unwrap();
        broker.open_package(active_package, None, deadline).unwrap();
        let blocked_handle = broker.create(blocked_package).unwrap();

        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        *broker.0.append_io_hook.lock().unwrap() = Some(Arc::new(move || {
            entered_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
        }));

        let appending_broker = broker.clone();
        let append_thread = std::thread::spawn(move || {
            appending_broker.append(blocked_package, blocked_handle, b"blocked disk write")
        });
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("append did not reach the controlled I/O point");

        let (progress_tx, progress_rx) = std::sync::mpsc::channel();
        let active_broker = broker.clone();
        let create_thread = std::thread::spawn(move || {
            let result = active_broker.create(active_package);
            let _ = progress_tx.send(result);
        });
        let independent_package_progressed =
            progress_rx.recv_timeout(Duration::from_millis(100)).is_ok();

        release_tx.send(()).unwrap();
        assert_eq!(append_thread.join().unwrap(), Ok(()));
        create_thread.join().unwrap();
        assert!(
            independent_package_progressed,
            "one package's disk I/O must not hold the broker-wide state lock"
        );
        broker.close_package(blocked_package).unwrap();
        broker.close_package(active_package).unwrap();
    }

    #[test]
    fn package_close_waits_for_inflight_append_before_releasing_quota() {
        let scratch = Scratch::new();
        let mut limits = config(&scratch.0);
        limits.max_bytes_per_invocation = 4096;
        limits.max_service_bytes = 4096;
        let broker = SpillBroker::open(limits).unwrap();
        let owner = package(17);
        broker
            .open_package(owner, None, Instant::now() + Duration::from_secs(10))
            .unwrap();
        let handle = broker.create(owner).unwrap();

        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        *broker.0.append_io_hook.lock().unwrap() = Some(Arc::new(move || {
            entered_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
        }));
        let appending_broker = broker.clone();
        let append_thread =
            std::thread::spawn(move || appending_broker.append(owner, handle, &[0xA5; 4096]));
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("append did not reach the controlled I/O point");

        let (close_started_tx, close_started_rx) = std::sync::mpsc::channel();
        let (close_result_tx, close_result_rx) = std::sync::mpsc::channel();
        let closing_broker = broker.clone();
        let close_thread = std::thread::spawn(move || {
            close_started_tx.send(()).unwrap();
            let _ = close_result_tx.send(closing_broker.close_package(owner));
        });
        close_started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("package close thread did not start");
        let close_deadline = Instant::now() + Duration::from_secs(1);
        loop {
            match broker.create(owner) {
                Err(SpillError::ClosedSession) => break,
                Ok(handle) => {
                    let _ = broker.destroy(owner, handle);
                }
                Err(error) => panic!("unexpected response while closing package: {error:?}"),
            }
            assert!(
                Instant::now() < close_deadline,
                "close did not stop new stream admissions"
            );
            std::thread::yield_now();
        }
        assert!(
            close_result_rx
                .recv_timeout(Duration::from_millis(50))
                .is_err(),
            "package close must wait for its active stream I/O"
        );

        release_tx.send(()).unwrap();
        assert_eq!(append_thread.join().unwrap(), Ok(()));
        assert_eq!(
            close_result_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap(),
            Ok(())
        );
        close_thread.join().unwrap();
        *broker.0.append_io_hook.lock().unwrap() = None;

        broker
            .open_package(owner, None, Instant::now() + Duration::from_secs(10))
            .unwrap();
        let replacement = broker.create(owner).unwrap();
        assert_eq!(
            broker.append(owner, replacement, &[0x5A; 4096]),
            Ok(()),
            "closing the prior package must release its physical quota"
        );
        broker.close_package(owner).unwrap();
    }

    #[test]
    fn broker_rejects_memory_backed_scratch() {
        let memory_root = PathBuf::from("/dev/shm");
        if memory_root.exists() {
            let scratch = Scratch::new_in(&memory_root);
            assert!(matches!(
                SpillBroker::open(config(&scratch.0)),
                Err(SpillError::UnsupportedFilesystem)
            ));
        }
    }

    #[test]
    fn optional_configured_filesystem_supports_reserved_spill() {
        let Some(parent) = std::env::var_os("PRDB_TEST_SPILL_FILESYSTEM_ROOT") else {
            return;
        };
        let scratch = Scratch::new_in(Path::new(&parent));
        let mut limits = config(&scratch.0);
        limits.max_bytes_per_invocation = 2 * 1024 * 1024;
        limits.max_io_bytes_per_invocation = 4 * 1024 * 1024;
        limits.max_response_bytes = 16 * 1024;
        limits.max_service_bytes = 4 * 1024 * 1024;
        let broker = SpillBroker::open(limits).unwrap_or_else(|error| {
            panic!("configured spill filesystem could not reserve physical allocation: {error:?}")
        });
        let owner = package(23);
        broker
            .open_package(owner, None, Instant::now() + Duration::from_secs(10))
            .unwrap();
        let handle = broker.create(owner).unwrap();
        let expected = [0xA7; 8192];
        broker.append(owner, handle, &expected).unwrap();
        broker.seal(owner, handle).unwrap();
        let mut actual = vec![0; expected.len()];
        assert_eq!(
            broker.read_at(owner, handle, 0, &mut actual),
            Ok(expected.len())
        );
        assert_eq!(actual, expected);
        broker.close_package(owner).unwrap();
        let remaining = std::fs::read_dir(&scratch.0)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        assert_eq!(
            remaining,
            vec![std::ffi::OsString::from(".prdb-spill.lock")]
        );
        drop(broker);
    }

    #[test]
    fn optional_configured_filesystem_sustains_shared_allocation_pressure() {
        let Some(parent) = std::env::var_os("PRDB_TEST_SPILL_PRESSURE_FILESYSTEM_ROOT") else {
            return;
        };
        let scratch = Scratch::new_in(Path::new(&parent));
        let mut limits = config(&scratch.0);
        limits.max_bytes_per_invocation = 8 * 1024 * 1024;
        limits.max_io_bytes_per_invocation = 16 * 1024 * 1024;
        limits.max_operations_per_invocation = 64;
        limits.max_service_bytes = 16 * 1024 * 1024;
        let broker = SpillBroker::open(limits).unwrap_or_else(|error| {
            panic!("configured spill filesystem cannot sustain allocation pressure: {error:?}")
        });
        let chunk = Arc::new(vec![0x6B; MAX_IPC_REQUEST_BYTES as usize - HANDLE_BYTES]);
        let owners = [package(41), package(42)];
        let started = Instant::now();
        for round in 0..4 {
            let deadline = Instant::now() + Duration::from_secs(60);
            let handles = owners.map(|owner| {
                broker.open_package(owner, None, deadline).unwrap();
                broker.create(owner).unwrap()
            });
            let barrier = Arc::new(std::sync::Barrier::new(3));
            let workers = owners
                .into_iter()
                .zip(handles)
                .map(|(owner, handle)| {
                    let broker = broker.clone();
                    let barrier = Arc::clone(&barrier);
                    let chunk = Arc::clone(&chunk);
                    std::thread::spawn(move || {
                        barrier.wait();
                        for _ in 0..8 {
                            broker.append(owner, handle, &chunk).unwrap();
                        }
                        broker.seal(owner, handle).unwrap();
                    })
                })
                .collect::<Vec<_>>();
            barrier.wait();
            for worker in workers {
                worker.join().unwrap();
            }
            let reserved = broker.0.state.lock().unwrap().service_reserved;
            assert_eq!(reserved, 16 * 1024 * 1024);
            for owner in owners {
                broker.close_package(owner).unwrap();
            }
            assert_eq!(broker.0.state.lock().unwrap().service_reserved, 0);
            eprintln!("spill filesystem pressure round {} complete", round + 1);
        }
        eprintln!(
            "allocated and reclaimed 64 MiB across two concurrent packages in {} ms",
            started.elapsed().as_millis()
        );
        drop(broker);
    }

    #[test]
    fn broker_restart_removes_orphaned_stream_files_and_package_directories() {
        let scratch = Scratch::new();
        let first = SpillBroker::open(config(&scratch.0)).unwrap();
        assert!(matches!(
            SpillBroker::open(config(&scratch.0)),
            Err(SpillError::BrokerAlreadyRunning)
        ));
        drop(first);

        let orphan_file = scratch.0.join("00112233445566778899aabbccddeeff");
        std::fs::write(&orphan_file, b"temporary plaintext").unwrap();
        let orphan_directory = scratch.0.join("ffeeddccbbaa99887766554433221100");
        std::fs::create_dir(&orphan_directory).unwrap();
        std::fs::write(
            orphan_directory.join("0123456789abcdef0123456789abcdef"),
            b"partial creation before unlink",
        )
        .unwrap();

        let restarted = SpillBroker::open(config(&scratch.0)).unwrap();
        assert!(!orphan_file.exists());
        assert!(!orphan_directory.exists());
        let remaining: Vec<_> = std::fs::read_dir(&scratch.0)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(
            remaining,
            vec![std::ffi::OsString::from(".prdb-spill.lock")]
        );
        drop(restarted);
    }

    #[test]
    fn service_cap_must_leave_filesystem_headroom() {
        let scratch = Scratch::new();
        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
        let path = CString::new(scratch.0.as_os_str().as_bytes()).unwrap();
        let fd = unsafe {
            libc::open(
                path.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
            )
        };
        assert!(fd >= 0);
        let result = unsafe { libc::fstatvfs(fd, &mut stat) };
        unsafe {
            libc::close(fd);
        }
        assert_eq!(result, 0);
        let available = (stat.f_bavail as u64).saturating_mul(stat.f_frsize as u64);
        let mut limits = config(&scratch.0);
        limits.max_service_bytes = available.saturating_add(1);
        if limits.max_service_bytes > limits.max_bytes_per_invocation {
            assert!(matches!(
                SpillBroker::open(limits),
                Err(SpillError::InsufficientScratchCapacity)
            ));
        }
    }

    #[test]
    fn dedicated_channel_frames_are_versioned_and_stream_handles_stay_package_scoped() {
        let scratch = Scratch::new();
        let broker = SpillBroker::open(config(&scratch.0)).unwrap();
        let owner = package(9);
        broker
            .open_package(owner, None, Instant::now() + Duration::from_secs(10))
            .unwrap();
        let (mut client, server) = std::os::unix::net::UnixStream::pair().unwrap();
        let serving_broker = broker.clone();
        let server_thread = std::thread::spawn(move || serving_broker.serve_channel(server, owner));

        let create_response = channel_request(&mut client, SPILL_OP_CREATE, 1, &[]);
        assert_eq!(
            u16::from_be_bytes([create_response[0], create_response[1]]),
            0
        );
        let handle: [u8; HANDLE_BYTES] = create_response[2..].try_into().unwrap();

        let mut append = handle.to_vec();
        append.extend_from_slice(b"framed payload");
        assert_eq!(
            channel_request(&mut client, SPILL_OP_APPEND, 2, &append),
            0u16.to_be_bytes()
        );
        assert_eq!(
            channel_request(&mut client, SPILL_OP_SEAL, 3, &handle),
            0u16.to_be_bytes()
        );

        let mut read = handle.to_vec();
        read.extend_from_slice(&0u64.to_be_bytes());
        read.extend_from_slice(&32u32.to_be_bytes());
        let read_response = channel_request(&mut client, SPILL_OP_READ_AT, 4, &read);
        assert_eq!(&read_response[..2], &0u16.to_be_bytes());
        assert_eq!(&read_response[2..], b"framed payload");

        drop(client);
        assert_eq!(server_thread.join().unwrap(), Ok(()));
        broker.close_package(owner).unwrap();

        let (mut client, server) = std::os::unix::net::UnixStream::pair().unwrap();
        let second = package(10);
        broker
            .open_package(second, None, Instant::now() + Duration::from_secs(10))
            .unwrap();
        let serving_broker = broker.clone();
        let server_thread =
            std::thread::spawn(move || serving_broker.serve_channel(server, second));
        let mut foreign_read = handle.to_vec();
        foreign_read.extend_from_slice(&0u64.to_be_bytes());
        foreign_read.extend_from_slice(&1u32.to_be_bytes());
        let foreign_response = channel_request(&mut client, SPILL_OP_READ_AT, 1, &foreign_read);
        assert_eq!(
            u16::from_be_bytes([foreign_response[0], foreign_response[1]]),
            spill_status(SpillError::InvalidHandle)
        );
        drop(client);
        assert_eq!(server_thread.join().unwrap(), Ok(()));
        broker.close_package(second).unwrap();
    }

    #[test]
    fn channel_append_is_bounded_by_request_and_io_limits_not_response_limit() {
        let scratch = Scratch::new();
        let broker = SpillBroker::open(config(&scratch.0)).unwrap();
        let owner = package(12);
        broker
            .open_package(owner, None, Instant::now() + Duration::from_secs(10))
            .unwrap();
        let (mut client, server) = std::os::unix::net::UnixStream::pair().unwrap();
        let serving_broker = broker.clone();
        let server_thread = std::thread::spawn(move || serving_broker.serve_channel(server, owner));

        let create_response = channel_request(&mut client, SPILL_OP_CREATE, 1, &[]);
        assert_eq!(&create_response[..2], &0u16.to_be_bytes());
        let handle: [u8; HANDLE_BYTES] = create_response[2..].try_into().unwrap();
        let mut append = handle.to_vec();
        append.extend(std::iter::repeat_n(b'x', 5000));
        assert_eq!(
            channel_request(&mut client, SPILL_OP_APPEND, 2, &append),
            0u16.to_be_bytes(),
            "a valid append may exceed the configured response cap"
        );

        drop(client);
        assert_eq!(server_thread.join().unwrap(), Ok(()));
        broker.close_package(owner).unwrap();
    }

    #[test]
    fn worker_channel_disconnect_reclaims_spill_before_package_cleanup() {
        let scratch = Scratch::new();
        let broker = SpillBroker::open(config(&scratch.0)).unwrap();
        let owner = package(11);
        broker
            .open_package(owner, None, Instant::now() + Duration::from_secs(10))
            .unwrap();
        let (mut client, server) = std::os::unix::net::UnixStream::pair().unwrap();
        let serving_broker = broker.clone();
        let server_thread = std::thread::spawn(move || serving_broker.serve_channel(server, owner));

        let create_response = channel_request(&mut client, SPILL_OP_CREATE, 1, &[]);
        assert_eq!(&create_response[..2], &0u16.to_be_bytes());
        let handle: [u8; HANDLE_BYTES] = create_response[2..].try_into().unwrap();
        let mut append = handle.to_vec();
        append.extend_from_slice(b"written before worker crash");
        assert_eq!(
            channel_request(&mut client, SPILL_OP_APPEND, 2, &append),
            0u16.to_be_bytes()
        );

        drop(client);
        assert_eq!(server_thread.join().unwrap(), Ok(()));
        let remaining = std::fs::read_dir(&scratch.0)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        assert_eq!(
            remaining,
            vec![std::ffi::OsString::from(".prdb-spill.lock")],
            "the disconnected worker's stream and package directory must be reclaimed"
        );
        broker.close_package(owner).unwrap();
    }

    fn channel_request(
        stream: &mut std::os::unix::net::UnixStream,
        operation: u16,
        request_id: u64,
        payload: &[u8],
    ) -> Vec<u8> {
        let mut header = [0u8; SPILL_CHANNEL_HEADER_BYTES];
        header[..4].copy_from_slice(&SPILL_CHANNEL_MAGIC);
        header[4..6].copy_from_slice(&SPILL_CHANNEL_VERSION.to_be_bytes());
        header[6..8].copy_from_slice(&operation.to_be_bytes());
        header[8..16].copy_from_slice(&request_id.to_be_bytes());
        header[16..20].copy_from_slice(&(payload.len() as u32).to_be_bytes());
        stream.write_all(&header).unwrap();
        stream.write_all(payload).unwrap();

        let mut response_header = [0u8; SPILL_CHANNEL_HEADER_BYTES];
        stream.read_exact(&mut response_header).unwrap();
        assert_eq!(&response_header[..4], &SPILL_CHANNEL_MAGIC);
        assert_eq!(
            u16::from_be_bytes([response_header[6], response_header[7]]),
            operation | 0x8000
        );
        assert_eq!(
            u64::from_be_bytes(response_header[8..16].try_into().unwrap()),
            request_id
        );
        let length = u32::from_be_bytes(response_header[16..20].try_into().unwrap()) as usize;
        let mut response = vec![0; length];
        stream.read_exact(&mut response).unwrap();
        response
    }
}
