//! Trusted disk-backed scratch streams for analytical query operators.
//!
//! The worker-facing interface is intentionally not a filesystem interface:
//! callers receive random handles and this module retains every file
//! descriptor. A supervisor must create a session only after authenticating
//! the worker channel and must destroy it on every terminal package path.

use crate::query_abi::MAX_IPC_REQUEST_BYTES;
use crate::supervisor::WorkloadClass;
use getrandom::fill as random_fill;
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::fs::File;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
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

impl SpillConfig {
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
}

struct BrokerState {
    service_reserved: u64,
    sessions: HashMap<SpillPackage, SessionState>,
    failed_closed: bool,
}

struct SessionState {
    workload: WorkloadClass,
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
        })))
    }

    /// Bind a worker channel to trusted invocation/package state. Operational
    /// invocations are denied regardless of worker requests.
    pub fn open_package(
        &self,
        package: SpillPackage,
        workload: WorkloadClass,
        deadline: Instant,
    ) -> Result<(), SpillError> {
        if workload != WorkloadClass::Analytical {
            return Err(SpillError::DisabledForWorkload);
        }
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
                workload,
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
        charge_operation(session, &self.0.config)?;
        if session.streams.len() as u64 >= self.0.config.max_streams_per_invocation {
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
        let mut state = self
            .0
            .state
            .lock()
            .map_err(|_| SpillError::AccountingFailure)?;
        if state.failed_closed {
            return Err(SpillError::AccountingFailure);
        }
        let service_cap = self.0.config.max_service_bytes;
        let per_invocation_cap = self.0.config.max_bytes_per_invocation;
        let service_reserved = state.service_reserved;
        let allocation_unit = self.0.allocation_unit;
        let mut write_failed = false;
        let mut reserved_delta = 0;
        {
            let session = session_mut(&mut state, package)?;
            charge_operation(session, &self.0.config)?;
            charge_io(session, bytes.len() as u64, &self.0.config)?;
            let stream = session
                .streams
                .get_mut(&handle)
                .ok_or(SpillError::InvalidHandle)?;
            if stream.sealed {
                return Err(SpillError::ClosedSession);
            }
            let new_len = stream
                .length
                .checked_add(bytes.len() as u64)
                .ok_or(SpillError::AllocationLimit)?;
            let wanted = round_allocation(new_len, allocation_unit)?;
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
            if delta > 0 {
                let size =
                    libc::off_t::try_from(wanted).map_err(|_| SpillError::AllocationLimit)?;
                // SAFETY: posix_fallocate operates on the broker-owned regular file.
                let result = unsafe { libc::posix_fallocate(stream.file.as_raw_fd(), 0, size) };
                if result != 0 {
                    return Err(SpillError::Io);
                }
                stream.reserved = wanted;
                session.allocated += delta;
                reserved_delta = delta;
            }
            stream
                .file
                .seek(SeekFrom::Start(stream.length))
                .map_err(|_| SpillError::Io)?;
            if stream.file.write_all(bytes).is_err() {
                write_failed = true;
            } else {
                stream.length = new_len;
            }
        }
        state.service_reserved = service_reserved
            .checked_add(reserved_delta)
            .ok_or(SpillError::AccountingFailure)?;
        if write_failed {
            state.failed_closed = true;
            return Err(SpillError::AccountingFailure);
        }
        Ok(())
    }

    pub fn seal(&self, package: SpillPackage, handle: SpillHandle) -> Result<(), SpillError> {
        let mut state = self
            .0
            .state
            .lock()
            .map_err(|_| SpillError::AccountingFailure)?;
        let session = session_mut(&mut state, package)?;
        charge_operation(session, &self.0.config)?;
        let stream = session
            .streams
            .get_mut(&handle)
            .ok_or(SpillError::InvalidHandle)?;
        stream.file.sync_data().map_err(|_| SpillError::Io)?;
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
        if out.len() as u64 > self.0.config.max_response_bytes {
            return Err(SpillError::ResponseLimit);
        }
        let mut state = self
            .0
            .state
            .lock()
            .map_err(|_| SpillError::AccountingFailure)?;
        let session = session_mut(&mut state, package)?;
        charge_operation(session, &self.0.config)?;
        let count = {
            let stream = session
                .streams
                .get(&handle)
                .ok_or(SpillError::InvalidHandle)?;
            if !stream.sealed {
                return Err(SpillError::ClosedSession);
            }
            if offset > stream.length {
                return Err(SpillError::InvalidRange);
            }
            let available = stream.length - offset;
            usize::try_from(available.min(out.len() as u64))
                .map_err(|_| SpillError::InvalidRange)?
        };
        charge_io(session, count as u64, &self.0.config)?;
        let stream = session
            .streams
            .get_mut(&handle)
            .ok_or(SpillError::InvalidHandle)?;
        stream
            .file
            .seek(SeekFrom::Start(offset))
            .map_err(|_| SpillError::Io)?;
        stream
            .file
            .read_exact(&mut out[..count])
            .map_err(|_| SpillError::Io)?;
        Ok(count)
    }

    pub fn destroy(&self, package: SpillPackage, handle: SpillHandle) -> Result<(), SpillError> {
        let mut state = self
            .0
            .state
            .lock()
            .map_err(|_| SpillError::AccountingFailure)?;
        let session = session_mut(&mut state, package)?;
        charge_operation(session, &self.0.config)?;
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
        let mut state = self
            .0
            .state
            .lock()
            .map_err(|_| SpillError::AccountingFailure)?;
        let Some(mut session) = state.sessions.remove(&package) else {
            return Ok(());
        };
        session.closed = true;
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
    if session.workload != WorkloadClass::Analytical {
        return Err(SpillError::DisabledForWorkload);
    }
    if session.cancelled.load(Ordering::Acquire) {
        return Err(SpillError::Cancelled);
    }
    if Instant::now() >= session.deadline {
        return Err(SpillError::Cancelled);
    }
    Ok(session)
}

fn charge_operation(session: &mut SessionState, config: &SpillConfig) -> Result<(), SpillError> {
    session.operations = session
        .operations
        .checked_add(1)
        .ok_or(SpillError::OperationLimit)?;
    if session.operations > config.max_operations_per_invocation {
        return Err(SpillError::OperationLimit);
    }
    Ok(())
}

fn charge_io(
    session: &mut SessionState,
    bytes: u64,
    config: &SpillConfig,
) -> Result<(), SpillError> {
    let next = session
        .io_bytes
        .checked_add(bytes)
        .ok_or(SpillError::IoLimit)?;
    if next > config.max_io_bytes_per_invocation {
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
        broker
            .open_package(owner, WorkloadClass::Analytical, deadline)
            .unwrap();
        broker
            .open_package(other, WorkloadClass::Analytical, deadline)
            .unwrap();
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
            broker.open_package(operational, WorkloadClass::Operational, deadline),
            Err(SpillError::DisabledForWorkload)
        );
        broker
            .open_package(analytical, WorkloadClass::Analytical, deadline)
            .unwrap();
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
    fn cancellation_invalidates_all_handles_for_the_package() {
        let scratch = Scratch::new();
        let broker = SpillBroker::open(config(&scratch.0)).unwrap();
        let owner = package(5);
        broker
            .open_package(
                owner,
                WorkloadClass::Analytical,
                Instant::now() + Duration::from_secs(10),
            )
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
            .open_package(stalled_package, WorkloadClass::Analytical, deadline)
            .unwrap();
        broker
            .open_package(active_package, WorkloadClass::Analytical, deadline)
            .unwrap();

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
            .open_package(
                owner,
                WorkloadClass::Analytical,
                Instant::now() + Duration::from_secs(10),
            )
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
            .open_package(
                second,
                WorkloadClass::Analytical,
                Instant::now() + Duration::from_secs(10),
            )
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
            .open_package(
                owner,
                WorkloadClass::Analytical,
                Instant::now() + Duration::from_secs(10),
            )
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
            .open_package(
                owner,
                WorkloadClass::Analytical,
                Instant::now() + Duration::from_secs(10),
            )
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
