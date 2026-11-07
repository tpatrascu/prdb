//! TCP listener configuration for the plaintext client gateway.
//!
//! The listener is deliberately separate from storage and does not open
//! database files or accept a storage manager. Connection serving is layered
//! on top of this bind policy.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GatewayListenerConfig {
    pub bind_address: SocketAddr,
    /// Plaintext TCP exposes bearer credentials and results to the network.
    /// Non-loopback binding therefore requires an explicit operator choice.
    pub allow_non_loopback_plaintext: bool,
}

impl Default for GatewayListenerConfig {
    fn default() -> Self {
        Self {
            bind_address: SocketAddr::from(([127, 0, 0, 1], 0)),
            allow_non_loopback_plaintext: false,
        }
    }
}

#[derive(Debug)]
pub enum GatewayListenerError {
    NonLoopbackPlaintextDisabled(SocketAddr),
    InvalidConnectionLimit,
    ConnectionLimitReached,
    Io(io::Error),
}

impl std::fmt::Display for GatewayListenerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NonLoopbackPlaintextDisabled(address) => write!(
                formatter,
                "plaintext gateway binding to {address} requires explicit opt-in"
            ),
            Self::InvalidConnectionLimit => {
                write!(formatter, "gateway connection limit must be positive")
            }
            Self::ConnectionLimitReached => {
                write!(formatter, "gateway connection limit reached")
            }
            Self::Io(error) => write!(formatter, "gateway listener failed: {error}"),
        }
    }
}

impl std::error::Error for GatewayListenerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NonLoopbackPlaintextDisabled(_)
            | Self::InvalidConnectionLimit
            | Self::ConnectionLimitReached => None,
            Self::Io(error) => Some(error),
        }
    }
}

pub struct TcpGatewayListener {
    listener: TcpListener,
}

pub trait GatewayClientStream: Read + Write {
    fn set_gateway_read_timeout(&mut self, timeout: Option<Duration>) -> io::Result<()>;
}

impl GatewayClientStream for TcpStream {
    fn set_gateway_read_timeout(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        self.set_read_timeout(timeout)
    }
}

#[cfg(unix)]
impl GatewayClientStream for std::os::unix::net::UnixStream {
    fn set_gateway_read_timeout(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        self.set_read_timeout(timeout)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct GatewayRuntimeConfig {
    pub worker_count: usize,
    pub queue_capacity: usize,
    pub connections_per_ip_per_minute: usize,
    pub idle_timeout: Duration,
    pub accept_poll_interval: Duration,
}

impl Default for GatewayRuntimeConfig {
    fn default() -> Self {
        Self {
            worker_count: 32,
            queue_capacity: 64,
            connections_per_ip_per_minute: 60,
            idle_timeout: Duration::from_secs(30),
            accept_poll_interval: Duration::from_millis(10),
        }
    }
}

#[derive(Clone)]
pub struct GatewayPeerRateLimiter {
    max_attempts: NonZeroUsize,
    max_sources: NonZeroUsize,
    window: Duration,
    attempts: Arc<std::sync::Mutex<HashMap<IpAddr, VecDeque<std::time::Instant>>>>,
}

impl GatewayPeerRateLimiter {
    pub fn new(
        max_attempts: usize,
        max_sources: usize,
        window: Duration,
    ) -> Result<Self, GatewayListenerError> {
        if window.is_zero() {
            return Err(GatewayListenerError::InvalidConnectionLimit);
        }
        Ok(Self {
            max_attempts: NonZeroUsize::new(max_attempts)
                .ok_or(GatewayListenerError::InvalidConnectionLimit)?,
            max_sources: NonZeroUsize::new(max_sources)
                .ok_or(GatewayListenerError::InvalidConnectionLimit)?,
            window,
            attempts: Arc::new(std::sync::Mutex::new(HashMap::new())),
        })
    }

    pub fn admit(&self, source: IpAddr) -> bool {
        self.admit_at(source, std::time::Instant::now())
    }

    pub fn admit_at(&self, source: IpAddr, now: std::time::Instant) -> bool {
        let mut attempts = self
            .attempts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for timestamps in attempts.values_mut() {
            while timestamps
                .front()
                .is_some_and(|timestamp| now.saturating_duration_since(*timestamp) >= self.window)
            {
                timestamps.pop_front();
            }
        }
        attempts.retain(|_, timestamps| !timestamps.is_empty());
        if !attempts.contains_key(&source) && attempts.len() >= self.max_sources.get() {
            return false;
        }
        let timestamps = attempts.entry(source).or_default();
        if timestamps.len() >= self.max_attempts.get() {
            return false;
        }
        timestamps.push_back(now);
        true
    }
}

#[derive(Debug)]
pub enum GatewayRuntimeError {
    InvalidConfig,
    Io(io::Error),
    WorkerPanicked,
}

impl std::fmt::Display for GatewayRuntimeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "gateway runtime failed: {self:?}")
    }
}

impl std::error::Error for GatewayRuntimeError {}

struct GatewayConnectionLimit {
    maximum: NonZeroUsize,
    active: AtomicUsize,
}

#[derive(Clone)]
pub struct GatewayConnectionLimiter {
    inner: Arc<GatewayConnectionLimit>,
}

pub struct GatewayConnectionPermit {
    inner: Arc<GatewayConnectionLimit>,
}

#[derive(Clone, Debug)]
pub struct UnixGatewayServiceConnector {
    socket_path: PathBuf,
    expected_peer_uid: u32,
}

impl UnixGatewayServiceConnector {
    pub fn new(socket_path: impl AsRef<Path>, expected_peer_uid: u32) -> Self {
        Self {
            socket_path: socket_path.as_ref().to_path_buf(),
            expected_peer_uid,
        }
    }

    pub fn connect(&self) -> io::Result<std::os::unix::net::UnixStream> {
        let stream = std::os::unix::net::UnixStream::connect(&self.socket_path)?;
        let peer_uid = unix_peer_uid(&stream)?;
        if peer_uid != self.expected_peer_uid {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "gateway service peer has an unexpected UID",
            ));
        }
        Ok(stream)
    }
}

impl GatewayConnectionLimiter {
    pub fn new(maximum: usize) -> Result<Self, GatewayListenerError> {
        let maximum =
            NonZeroUsize::new(maximum).ok_or(GatewayListenerError::InvalidConnectionLimit)?;
        Ok(Self {
            inner: Arc::new(GatewayConnectionLimit {
                maximum,
                active: AtomicUsize::new(0),
            }),
        })
    }

    pub fn active_connections(&self) -> usize {
        self.inner.active.load(Ordering::Acquire)
    }

    pub(crate) fn try_acquire(&self) -> Option<GatewayConnectionPermit> {
        let mut active = self.inner.active.load(Ordering::Relaxed);
        loop {
            if active >= self.inner.maximum.get() {
                return None;
            }
            match self.inner.active.compare_exchange_weak(
                active,
                active + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some(GatewayConnectionPermit {
                        inner: Arc::clone(&self.inner),
                    });
                }
                Err(current) => active = current,
            }
        }
    }
}

impl Drop for GatewayConnectionPermit {
    fn drop(&mut self) {
        let previous = self.inner.active.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0);
    }
}

#[derive(Debug)]
pub enum GatewayConnectionError {
    Io(io::Error),
    ClientFrame(crate::client_protocol::ClientFrameError),
    ServiceFrame(crate::ipc::FrameError),
    UnsupportedClientKind(u16),
    ServiceDisconnected,
    MismatchedServiceResponse,
}

impl std::fmt::Display for GatewayConnectionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "gateway connection failed: {self:?}")
    }
}

impl std::error::Error for GatewayConnectionError {}

impl TcpGatewayListener {
    pub fn bind(config: GatewayListenerConfig) -> Result<Self, GatewayListenerError> {
        if !config.bind_address.ip().is_loopback() && !config.allow_non_loopback_plaintext {
            return Err(GatewayListenerError::NonLoopbackPlaintextDisabled(
                config.bind_address,
            ));
        }
        let listener = TcpListener::bind(config.bind_address).map_err(GatewayListenerError::Io)?;
        Ok(Self { listener })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub fn accept(&self) -> io::Result<(std::net::TcpStream, SocketAddr)> {
        self.listener.accept()
    }

    pub fn accept_bounded(
        &self,
        limiter: &GatewayConnectionLimiter,
    ) -> Result<(TcpStream, SocketAddr, GatewayConnectionPermit), GatewayListenerError> {
        let (stream, peer) = self.listener.accept().map_err(GatewayListenerError::Io)?;
        let permit = limiter
            .try_acquire()
            .ok_or(GatewayListenerError::ConnectionLimitReached)?;
        Ok((stream, peer, permit))
    }
}

/// Forward one TCP client connection over its already-provisioned private
/// gateway service stream. Callers must configure read/write deadlines and
/// connection limits before serving untrusted peers.
pub fn serve_tcp_client_connection<C: GatewayClientStream, S: Read + Write>(
    client: &mut C,
    service: &mut S,
) -> Result<(), GatewayConnectionError> {
    serve_tcp_client_connection_inner(client, service, None)
}

fn serve_tcp_client_connection_inner<C: GatewayClientStream, S: Read + Write>(
    client: &mut C,
    service: &mut S,
    frame_timeout: Option<Duration>,
) -> Result<(), GatewayConnectionError> {
    let mut decoder = crate::client_protocol::ClientFrameDecoder::default();
    let mut input = [0u8; 8192];
    loop {
        let Some(request) = read_client_request(client, &mut decoder, &mut input, frame_timeout)?
        else {
            return Ok(());
        };
        let (operation, payload) = client_request_to_ipc(request.kind, request.payload)?;
        let ipc_request = crate::ipc::IpcFrame {
            operation: operation as u16,
            request_id: request.request_id,
            payload,
        };
        crate::ipc::write_frame(service, &ipc_request)
            .map_err(GatewayConnectionError::ServiceFrame)?;
        let response = crate::ipc::read_frame(service)
            .map_err(GatewayConnectionError::ServiceFrame)?
            .ok_or(GatewayConnectionError::ServiceDisconnected)?;
        if response.operation != ipc_request.operation || response.request_id != request.request_id
        {
            return Err(GatewayConnectionError::MismatchedServiceResponse);
        }
        let response = crate::client_protocol::ClientFrame {
            kind: 100,
            request_id: response.request_id,
            response_sequence: 0,
            payload: response.payload,
        }
        .encode()
        .map_err(GatewayConnectionError::ClientFrame)?;
        client
            .write_all(&response)
            .map_err(GatewayConnectionError::Io)?;
    }
}

/// Set idle I/O timeouts on a TCP connection and its private Unix service
/// stream before running the bounded frame bridge.
pub fn serve_tcp_client_connection_with_timeout(
    client: &mut TcpStream,
    service: &mut std::os::unix::net::UnixStream,
    idle_timeout: Duration,
) -> Result<(), GatewayConnectionError> {
    if idle_timeout.is_zero() {
        return Err(GatewayConnectionError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "gateway idle timeout must be positive",
        )));
    }
    client
        .set_read_timeout(Some(idle_timeout))
        .map_err(GatewayConnectionError::Io)?;
    client
        .set_write_timeout(Some(idle_timeout))
        .map_err(GatewayConnectionError::Io)?;
    service
        .set_read_timeout(Some(idle_timeout))
        .map_err(GatewayConnectionError::Io)?;
    service
        .set_write_timeout(Some(idle_timeout))
        .map_err(GatewayConnectionError::Io)?;
    serve_tcp_client_connection_inner(client, service, Some(idle_timeout))
}

/// Run a fixed worker pool with a bounded pending-connection queue. The
/// connector opens exactly one private Unix service stream for each accepted
/// client. Set `shutdown` to drain the queue and join workers.
pub fn run_gateway<F>(
    gateway: TcpGatewayListener,
    config: GatewayRuntimeConfig,
    connect_service: F,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
) -> Result<(), GatewayRuntimeError>
where
    F: Fn() -> io::Result<std::os::unix::net::UnixStream> + Send + Sync + 'static,
{
    if config.worker_count == 0
        || config.queue_capacity == 0
        || config.connections_per_ip_per_minute == 0
        || config.idle_timeout.is_zero()
        || config.accept_poll_interval.is_zero()
    {
        return Err(GatewayRuntimeError::InvalidConfig);
    }
    gateway
        .listener
        .set_nonblocking(true)
        .map_err(GatewayRuntimeError::Io)?;

    let (sender, receiver) = std::sync::mpsc::sync_channel::<TcpStream>(config.queue_capacity);
    let receiver = Arc::new(std::sync::Mutex::new(receiver));
    let connector = Arc::new(connect_service);
    let rate_limiter = GatewayPeerRateLimiter::new(
        config.connections_per_ip_per_minute,
        4096,
        Duration::from_secs(60),
    )
    .map_err(|_| GatewayRuntimeError::InvalidConfig)?;
    let mut workers = Vec::with_capacity(config.worker_count);
    for _ in 0..config.worker_count {
        let receiver = Arc::clone(&receiver);
        let connector = Arc::clone(&connector);
        let idle_timeout = config.idle_timeout;
        workers.push(
            std::thread::Builder::new()
                .name("prdb-gateway-worker".to_owned())
                .spawn(move || {
                    loop {
                        let client = receiver
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .recv();
                        let Ok(mut client) = client else {
                            break;
                        };
                        let Ok(mut service) = connector() else {
                            continue;
                        };
                        let _ = serve_tcp_client_connection_with_timeout(
                            &mut client,
                            &mut service,
                            idle_timeout,
                        );
                    }
                })
                .map_err(GatewayRuntimeError::Io)?,
        );
    }

    let accept_result = loop {
        if shutdown.load(Ordering::Acquire) {
            break Ok(());
        }
        match gateway.listener.accept() {
            Ok((client, peer)) => {
                if !rate_limiter.admit(peer.ip()) {
                    continue;
                }
                match sender.try_send(client) {
                    Ok(()) | Err(std::sync::mpsc::TrySendError::Full(_)) => {}
                    Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                        break Err(GatewayRuntimeError::WorkerPanicked);
                    }
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(config.accept_poll_interval);
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => break Err(GatewayRuntimeError::Io(error)),
        }
    };
    drop(sender);

    let mut worker_panicked = false;
    for worker in workers {
        worker_panicked |= worker.join().is_err();
    }
    if worker_panicked {
        Err(GatewayRuntimeError::WorkerPanicked)
    } else {
        accept_result
    }
}

#[cfg(target_os = "linux")]
pub(crate) fn unix_peer_uid(stream: &std::os::unix::net::UnixStream) -> io::Result<u32> {
    use std::os::fd::AsRawFd;
    let mut credentials = std::mem::MaybeUninit::<libc::ucred>::uninit();
    let mut length = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    // SAFETY: getsockopt writes at most `length` bytes to the valid ucred
    // output buffer and does not retain the pointer.
    let result = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            credentials.as_mut_ptr().cast(),
            &mut length,
        )
    };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    if usize::try_from(length).ok() != Some(std::mem::size_of::<libc::ucred>()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "gateway service peer credentials have an invalid size",
        ));
    }
    // SAFETY: getsockopt succeeded and reported the full ucred size.
    let credentials = unsafe { credentials.assume_init() };
    Ok(credentials.uid)
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn unix_peer_uid(_stream: &std::os::unix::net::UnixStream) -> io::Result<u32> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "Unix gateway service peer verification is supported only on Linux",
    ))
}

fn read_client_request<C: GatewayClientStream>(
    client: &mut C,
    decoder: &mut crate::client_protocol::ClientFrameDecoder,
    input: &mut [u8],
    frame_timeout: Option<Duration>,
) -> Result<Option<crate::client_protocol::ClientFrame>, GatewayConnectionError> {
    let deadline = frame_timeout.and_then(|timeout| std::time::Instant::now().checked_add(timeout));
    loop {
        if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Err(GatewayConnectionError::Io(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "client frame deadline expired",
                )));
            }
            client
                .set_gateway_read_timeout(Some(remaining))
                .map_err(GatewayConnectionError::Io)?;
        }
        let count = match client.read(input) {
            Ok(count) => count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(GatewayConnectionError::Io(error)),
        };
        if count == 0 {
            decoder
                .finish()
                .map_err(GatewayConnectionError::ClientFrame)?;
            return Ok(None);
        }
        let mut frames = decoder
            .push(&input[..count])
            .map_err(GatewayConnectionError::ClientFrame)?;
        if frames.len() > 1 {
            return Err(GatewayConnectionError::ClientFrame(
                crate::client_protocol::ClientFrameError::PipelinedRequests,
            ));
        }
        if let Some(frame) = frames.pop() {
            if decoder.finish().is_err() {
                return Err(GatewayConnectionError::ClientFrame(
                    crate::client_protocol::ClientFrameError::PipelinedRequests,
                ));
            }
            return Ok(Some(frame));
        }
    }
}

fn client_request_to_ipc(
    kind: u16,
    mut payload: Vec<u8>,
) -> Result<(crate::ipc::Operation, Vec<u8>), GatewayConnectionError> {
    use crate::ipc::Operation;
    let operation = match kind {
        1 => Operation::OpenClientSession,
        2 => Operation::AuthenticateSession,
        3 => Operation::PrepareSubmission,
        4 | 5 => {
            payload.insert(0, kind as u8);
            Operation::UploadSubmissionChunk
        }
        6 => Operation::SealSubmission,
        7 => Operation::StartSubmission,
        8 => Operation::ClientGetInvocationStatus,
        9 => Operation::ClientCancelInvocation,
        11 => Operation::PingClientSession,
        _ => return Err(GatewayConnectionError::UnsupportedClientKind(kind)),
    };
    Ok((operation, payload))
}
