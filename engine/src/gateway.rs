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
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use zeroize::Zeroize;
use zeroize::Zeroizing;

static GATEWAY_RESULT_BYTES_SENT_TO_CLIENTS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Result payload bytes completely written to external client streams since
/// this gateway process started. Client frame headers are excluded.
pub fn gateway_result_bytes_sent_to_clients_total() -> u64 {
    GATEWAY_RESULT_BYTES_SENT_TO_CLIENTS_TOTAL.load(Ordering::Relaxed)
}

fn record_gateway_result_bytes_sent_to_clients(bytes: usize) {
    let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
    saturating_add_counter(&GATEWAY_RESULT_BYTES_SENT_TO_CLIENTS_TOTAL, bytes);
}

fn saturating_add_counter(counter: &AtomicU64, amount: u64) {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        let next = current.saturating_add(amount);
        match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(actual) => current = actual,
        }
    }
}

fn append_gateway_result_counter(payload: &[u8], count: u64) -> Option<Vec<u8>> {
    let end = payload
        .iter()
        .rposition(|byte| !byte.is_ascii_whitespace())?
        + 1;
    if payload.get(end - 1) != Some(&b'}') {
        return None;
    }
    if !payload
        .windows(b"\"gateway_result_bytes_relayed_total\":".len())
        .any(|window| window == b"\"gateway_result_bytes_relayed_total\":")
        || !payload
            .windows(b"\"invocations\":".len())
            .any(|window| window == b"\"invocations\":")
    {
        return Some(payload.to_vec());
    }
    let suffix = format!(",\"gateway_result_bytes_sent_to_clients_total\":{count}}}");
    let capacity = end.checked_add(suffix.len())?;
    let mut output = Vec::with_capacity(capacity);
    output.extend_from_slice(&payload[..end - 1]);
    output.extend_from_slice(suffix.as_bytes());
    Some(output)
}

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
    pub control_worker_count: usize,
    pub control_queue_capacity: usize,
    pub connections_per_ip_per_minute: usize,
    pub auth_failures_per_ip_per_minute: usize,
    pub idle_timeout: Duration,
    pub pre_auth_timeout: Duration,
    pub accept_poll_interval: Duration,
    /// Cumulative result bytes accepted per request. Individual protocol
    /// frame limits remain fixed independently.
    pub max_total_result_bytes: u64,
}

impl Default for GatewayRuntimeConfig {
    fn default() -> Self {
        Self {
            worker_count: 32,
            queue_capacity: 64,
            control_worker_count: 2,
            control_queue_capacity: 8,
            connections_per_ip_per_minute: 60,
            auth_failures_per_ip_per_minute: 10,
            idle_timeout: Duration::from_secs(30),
            pre_auth_timeout: Duration::from_secs(10),
            accept_poll_interval: Duration::from_millis(10),
            max_total_result_bytes: crate::query_abi::MAX_TOTAL_RESULT_BYTES,
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

/// Bounds failed AUTH requests by source without retaining credential bytes.
type AuthFailureHistory = HashMap<IpAddr, VecDeque<(std::time::Instant, u64)>>;
type SharedAuthFailureHistory = Arc<std::sync::Mutex<AuthFailureHistory>>;

#[derive(Clone)]
pub struct GatewayAuthFailureLimiter {
    max_failures: NonZeroUsize,
    max_sources: NonZeroUsize,
    window: Duration,
    failures: SharedAuthFailureHistory,
    next_ticket: Arc<std::sync::atomic::AtomicU64>,
}

impl GatewayAuthFailureLimiter {
    pub fn new(
        max_failures: usize,
        max_sources: usize,
        window: Duration,
    ) -> Result<Self, GatewayListenerError> {
        if window.is_zero() {
            return Err(GatewayListenerError::InvalidConnectionLimit);
        }
        Ok(Self {
            max_failures: NonZeroUsize::new(max_failures)
                .ok_or(GatewayListenerError::InvalidConnectionLimit)?,
            max_sources: NonZeroUsize::new(max_sources)
                .ok_or(GatewayListenerError::InvalidConnectionLimit)?,
            window,
            failures: Arc::new(std::sync::Mutex::new(HashMap::new())),
            next_ticket: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        })
    }

    pub fn allows_attempt(&self, source: IpAddr) -> bool {
        self.allows_attempt_at(source, std::time::Instant::now())
    }

    pub fn allows_attempt_at(&self, source: IpAddr, now: std::time::Instant) -> bool {
        let mut failures = self
            .failures
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.prune(&mut failures, now);
        match failures.get(&source) {
            Some(times) => times.len() < self.max_failures.get(),
            None => failures.len() < self.max_sources.get(),
        }
    }

    fn begin_attempt(&self, source: IpAddr) -> Option<GatewayAuthAttempt> {
        self.begin_attempt_at(source, std::time::Instant::now())
    }

    fn begin_attempt_at(
        &self,
        source: IpAddr,
        now: std::time::Instant,
    ) -> Option<GatewayAuthAttempt> {
        let mut failures = self.failures.lock().ok()?;
        self.prune(&mut failures, now);
        if failures
            .get(&source)
            .is_some_and(|times| times.len() >= self.max_failures.get())
            || (!failures.contains_key(&source) && failures.len() >= self.max_sources.get())
        {
            return None;
        }
        let ticket = self
            .next_ticket
            .fetch_update(
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |ticket| ticket.checked_add(1),
            )
            .ok()?;
        failures.entry(source).or_default().push_back((now, ticket));
        Some(GatewayAuthAttempt {
            limiter: self.clone(),
            source,
            ticket,
            keep_failure: false,
        })
    }

    /// Record an unsuccessful AUTH. `false` means a full source map prevented
    /// recording; callers must close the connection and the next attempt from
    /// a new source is denied until an existing history expires.
    pub fn record_failure(&self, source: IpAddr) -> bool {
        self.record_failure_at(source, std::time::Instant::now())
    }

    pub fn record_failure_at(&self, source: IpAddr, now: std::time::Instant) -> bool {
        let Some(attempt) = self.begin_attempt_at(source, now) else {
            return false;
        };
        attempt.retain_failure();
        true
    }

    fn prune(&self, failures: &mut AuthFailureHistory, now: std::time::Instant) {
        for times in failures.values_mut() {
            while times
                .front()
                .is_some_and(|(time, _)| now.saturating_duration_since(*time) >= self.window)
            {
                times.pop_front();
            }
        }
        failures.retain(|_, times| !times.is_empty());
    }
}

struct GatewayAuthAttempt {
    limiter: GatewayAuthFailureLimiter,
    source: IpAddr,
    ticket: u64,
    keep_failure: bool,
}

impl GatewayAuthAttempt {
    fn retain_failure(mut self) {
        self.keep_failure = true;
    }
}

impl Drop for GatewayAuthAttempt {
    fn drop(&mut self) {
        if self.keep_failure {
            return;
        }
        if let Ok(mut failures) = self.limiter.failures.lock()
            && let Some(times) = failures.get_mut(&self.source)
        {
            times.retain(|(_, ticket)| *ticket != self.ticket);
            if times.is_empty() {
                failures.remove(&self.source);
            }
        }
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
    UnexpectedServiceEvent,
    AuthenticationRateLimited,
    ResultLimitExceeded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GatewayClientErrorCode {
    InvalidRequest,
    InvalidState,
    Unauthenticated,
    ResourceLimit,
    Unavailable,
}

impl GatewayClientErrorCode {
    pub(crate) fn payload(self) -> &'static [u8] {
        match self {
            Self::InvalidRequest => crate::query_abi::IPC_GATEWAY_ERROR_INVALID_REQUEST.as_bytes(),
            Self::InvalidState => crate::query_abi::IPC_GATEWAY_ERROR_INVALID_STATE.as_bytes(),
            Self::Unauthenticated => crate::query_abi::IPC_GATEWAY_ERROR_UNAUTHENTICATED.as_bytes(),
            Self::ResourceLimit => crate::query_abi::IPC_GATEWAY_ERROR_RESOURCE_LIMIT.as_bytes(),
            Self::Unavailable => crate::query_abi::IPC_GATEWAY_ERROR_UNAVAILABLE.as_bytes(),
        }
    }

    pub(crate) fn from_payload(payload: &[u8]) -> Option<Self> {
        [
            Self::InvalidRequest,
            Self::InvalidState,
            Self::Unauthenticated,
            Self::ResourceLimit,
            Self::Unavailable,
        ]
        .into_iter()
        .find(|code| code.payload() == payload)
    }
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
    serve_tcp_client_connection_inner(
        client,
        service,
        GatewayConnectionSettings {
            frame_timeout: None,
            pre_auth_timeout: None,
            auth_failure_limiter: None,
            source_ip: None,
            control_only: false,
            max_total_result_bytes: crate::query_abi::MAX_TOTAL_RESULT_BYTES,
        },
    )
}

#[derive(Clone, Copy)]
struct GatewayConnectionSettings<'a> {
    frame_timeout: Option<Duration>,
    pre_auth_timeout: Option<Duration>,
    auth_failure_limiter: Option<&'a GatewayAuthFailureLimiter>,
    source_ip: Option<IpAddr>,
    control_only: bool,
    max_total_result_bytes: u64,
}

fn serve_tcp_client_connection_inner<C: GatewayClientStream, S: Read + Write>(
    client: &mut C,
    service: &mut S,
    settings: GatewayConnectionSettings<'_>,
) -> Result<(), GatewayConnectionError> {
    let GatewayConnectionSettings {
        frame_timeout,
        pre_auth_timeout,
        auth_failure_limiter,
        source_ip,
        control_only,
        max_total_result_bytes,
    } = settings;
    let mut decoder = crate::client_protocol::ClientFrameDecoder::negotiating();
    let mut input = [0u8; 8192];
    let pre_auth_deadline =
        pre_auth_timeout.and_then(|timeout| std::time::Instant::now().checked_add(timeout));
    let mut authenticated = false;
    loop {
        let now = std::time::Instant::now();
        let frame_deadline = frame_timeout.and_then(|timeout| now.checked_add(timeout));
        let deadline = if authenticated {
            frame_deadline
        } else {
            match (frame_deadline, pre_auth_deadline) {
                (Some(frame), Some(pre_auth)) => Some(frame.min(pre_auth)),
                (Some(frame), None) => Some(frame),
                (None, Some(pre_auth)) => Some(pre_auth),
                (None, None) => None,
            }
        };
        let Some(request) = read_client_request(client, &mut decoder, &mut input, deadline)? else {
            return Ok(());
        };
        let protocol_major = decoder
            .protocol_version()
            .map(|version| version.0)
            .ok_or(GatewayConnectionError::MismatchedServiceResponse)?;
        let client_kind = request.kind;
        let runtime_list_v6_or_later = client_kind == 10
            && matches!(
                crate::client_protocol::decode_client_admin_command(&request.payload),
                Ok(
                    crate::client_protocol::ClientAdminCommand::ListRuntimeInvocations {
                        response_version: 6..,
                        ..
                    }
                )
            );
        if control_only
            && !matches!(
                client_kind,
                1 | 2 | 8 | 9 | 10 | 11 | crate::client_protocol::CLIENT_SDK_LIBRARY_CHUNK_KIND
            )
        {
            write_client_error_version(
                client,
                request.request_id,
                "invalid_request",
                protocol_major,
            )?;
            return Ok(());
        }
        let auth_attempt = if client_kind == 2 {
            match auth_failure_limiter
                .zip(source_ip)
                .map(|(limiter, source)| {
                    limiter
                        .begin_attempt(source)
                        .ok_or(GatewayConnectionError::AuthenticationRateLimited)
                })
                .transpose()
            {
                Ok(attempt) => attempt,
                Err(_) => {
                    write_client_error_version(
                        client,
                        request.request_id,
                        "unauthenticated",
                        protocol_major,
                    )?;
                    return Ok(());
                }
            }
        } else {
            None
        };
        let (operation, payload) =
            match client_request_to_ipc(client_kind, request.payload, protocol_major) {
                Ok(request) => request,
                Err(GatewayConnectionError::UnsupportedClientKind(_)) => {
                    write_client_error_version(
                        client,
                        request.request_id,
                        "invalid_request",
                        protocol_major,
                    )?;
                    return Ok(());
                }
                Err(error) => return Err(error),
            };
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
        if response.request_id != request.request_id {
            return Err(GatewayConnectionError::MismatchedServiceResponse);
        }
        if client_kind == 2 {
            let authenticated_response = response
                .payload
                .windows(b"\"authenticated\":true".len())
                .any(|part| part == b"\"authenticated\":true");
            if authenticated_response {
                authenticated = true;
                drop(auth_attempt);
            } else if let Some(attempt) = auth_attempt {
                attempt.retain_failure();
            }
        }
        if response.operation == crate::ipc::Operation::GatewayClientError as u16 {
            if response.payload.len() > crate::client_protocol::CLIENT_MAX_CONTROL_BYTES
                || !is_sanitized_gateway_error_payload(&response.payload)
            {
                return Err(GatewayConnectionError::MismatchedServiceResponse);
            }
            let response_frame = crate::client_protocol::ClientFrame {
                kind: crate::client_protocol::CLIENT_ERROR_KIND,
                request_id: response.request_id,
                response_sequence: 0,
                payload: response.payload,
            };
            let bytes = encode_client_response(&response_frame, protocol_major)?;
            client
                .write_all(&bytes)
                .map_err(GatewayConnectionError::Io)?;
            return Ok(());
        }
        if response.operation != ipc_request.operation {
            return Err(GatewayConnectionError::MismatchedServiceResponse);
        }
        let response_payload = if runtime_list_v6_or_later {
            crate::gateway::append_gateway_result_counter(
                &response.payload,
                gateway_result_bytes_sent_to_clients_total(),
            )
            .ok_or(GatewayConnectionError::MismatchedServiceResponse)?
        } else {
            response.payload
        };
        let mut response_frame = crate::client_protocol::ClientFrame {
            kind: 100,
            request_id: response.request_id,
            response_sequence: 0,
            payload: response_payload,
        };
        let mut response = Zeroizing::new(encode_client_response(&response_frame, protocol_major)?);
        let write_result = client
            .write_all(&response)
            .map_err(GatewayConnectionError::Io);
        response_frame.payload.zeroize();
        response.zeroize();
        write_result?;
        if operation == crate::ipc::Operation::StartSubmission {
            relay_gateway_result_events_with_limit(
                client,
                service,
                request.request_id,
                max_total_result_bytes,
                protocol_major,
            )?;
        }
    }
}

fn is_sanitized_gateway_error_payload(payload: &[u8]) -> bool {
    GatewayClientErrorCode::from_payload(payload).is_some()
}

fn write_client_error_version<C: GatewayClientStream>(
    client: &mut C,
    request_id: u64,
    code: &str,
    protocol_major: u16,
) -> Result<(), GatewayConnectionError> {
    let message = match code {
        "unauthenticated" => "Authentication is required.",
        _ => "Request is invalid.",
    };
    let payload = format!("{{\"code\":\"{code}\",\"message\":\"{message}\"}}").into_bytes();
    let frame = crate::client_protocol::ClientFrame {
        kind: crate::client_protocol::CLIENT_ERROR_KIND,
        request_id,
        response_sequence: 0,
        payload,
    };
    let bytes = match protocol_major {
        1 => frame.encode(),
        2 => frame.encode_v2(),
        _ => return Err(GatewayConnectionError::MismatchedServiceResponse),
    }
    .map_err(GatewayConnectionError::ClientFrame)?;
    client.write_all(&bytes).map_err(GatewayConnectionError::Io)
}

fn relay_gateway_result_events_with_limit<C: GatewayClientStream, S: Read + Write>(
    client: &mut C,
    service: &mut S,
    start_request_id: u64,
    max_total_result_bytes: u64,
    protocol_major: u16,
) -> Result<(), GatewayConnectionError> {
    let mut response_sequence = 0u64;
    let mut total_result_bytes = 0u64;
    loop {
        let event = crate::ipc::read_frame(service)
            .map_err(GatewayConnectionError::ServiceFrame)?
            .ok_or(GatewayConnectionError::ServiceDisconnected)?;
        if event.request_id != start_request_id {
            return Err(GatewayConnectionError::MismatchedServiceResponse);
        }
        let kind = match crate::ipc::Operation::from_code(event.operation)
            .map_err(|_| GatewayConnectionError::UnexpectedServiceEvent)?
        {
            crate::ipc::Operation::GatewayResultData => 102,
            crate::ipc::Operation::GatewayTerminal => 103,
            _ => return Err(GatewayConnectionError::UnexpectedServiceEvent),
        };
        if kind == 102 {
            let event_bytes = u64::try_from(event.payload.len())
                .map_err(|_| GatewayConnectionError::ResultLimitExceeded)?;
            total_result_bytes = total_result_bytes
                .checked_add(event_bytes)
                .filter(|total| *total <= max_total_result_bytes)
                .ok_or(GatewayConnectionError::ResultLimitExceeded)?;
        }
        response_sequence = response_sequence
            .checked_add(1)
            .ok_or(GatewayConnectionError::UnexpectedServiceEvent)?;
        let terminal = kind == 103;
        let result_payload_bytes = (kind == 102).then_some(event.payload.len());
        let response = crate::client_protocol::ClientFrame {
            kind,
            request_id: start_request_id,
            response_sequence,
            payload: event.payload,
        };
        let bytes = encode_client_response(&response, protocol_major)?;
        client
            .write_all(&bytes)
            .map_err(GatewayConnectionError::Io)?;
        if let Some(payload_bytes) = result_payload_bytes {
            record_gateway_result_bytes_sent_to_clients(payload_bytes);
        }
        if terminal {
            return Ok(());
        }
    }
}

/// Set idle I/O timeouts on a TCP connection and its private Unix service
/// stream before running the bounded frame bridge.
pub fn serve_tcp_client_connection_with_timeout(
    client: &mut TcpStream,
    service: &mut std::os::unix::net::UnixStream,
    idle_timeout: Duration,
) -> Result<(), GatewayConnectionError> {
    serve_tcp_client_connection_with_timeouts(client, service, idle_timeout, idle_timeout)
}

pub fn serve_tcp_client_connection_with_timeouts(
    client: &mut TcpStream,
    service: &mut std::os::unix::net::UnixStream,
    idle_timeout: Duration,
    pre_auth_timeout: Duration,
) -> Result<(), GatewayConnectionError> {
    if idle_timeout.is_zero() || pre_auth_timeout.is_zero() {
        return Err(GatewayConnectionError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "gateway idle and pre-auth timeouts must be positive",
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
    serve_tcp_client_connection_inner(
        client,
        service,
        GatewayConnectionSettings {
            frame_timeout: Some(idle_timeout),
            pre_auth_timeout: Some(pre_auth_timeout),
            auth_failure_limiter: None,
            source_ip: None,
            control_only: false,
            max_total_result_bytes: crate::query_abi::MAX_TOTAL_RESULT_BYTES,
        },
    )
}

fn serve_tcp_client_connection_with_limits(
    client: &mut TcpStream,
    service: &mut std::os::unix::net::UnixStream,
    settings: GatewayConnectionSettings<'_>,
) -> Result<(), GatewayConnectionError> {
    let GatewayConnectionSettings {
        frame_timeout: idle_timeout,
        pre_auth_timeout,
        auth_failure_limiter,
        source_ip,
        control_only,
        max_total_result_bytes,
    } = settings;
    let (Some(idle_timeout), Some(pre_auth_timeout), Some(auth_failure_limiter), Some(source_ip)) = (
        idle_timeout,
        pre_auth_timeout,
        auth_failure_limiter,
        source_ip,
    ) else {
        return Err(GatewayConnectionError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "gateway worker connection settings are incomplete",
        )));
    };
    if idle_timeout.is_zero() || pre_auth_timeout.is_zero() {
        return Err(GatewayConnectionError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "gateway idle and pre-auth timeouts must be positive",
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
    serve_tcp_client_connection_inner(
        client,
        service,
        GatewayConnectionSettings {
            frame_timeout: Some(idle_timeout),
            pre_auth_timeout: Some(pre_auth_timeout),
            auth_failure_limiter: Some(auth_failure_limiter),
            source_ip: Some(source_ip),
            control_only,
            max_total_result_bytes,
        },
    )
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
    validate_gateway_runtime_config(&config)?;
    let rate_limiter = new_gateway_rate_limiter(&config)?;
    let auth_failure_limiter = new_gateway_auth_failure_limiter(&config)?;
    run_gateway_worker_pool(
        gateway,
        config,
        false,
        Arc::new(connect_service),
        rate_limiter,
        auth_failure_limiter,
        shutdown,
    )
}

/// Run the ordinary gateway endpoint and an independent bounded endpoint for
/// session setup, status, cancellation, administration, and PING. Result and
/// submission traffic is rejected on the control endpoint. Both pools share
/// peer and failed-auth limits, but cannot consume one another's workers or
/// pending queue slots.
pub fn run_gateway_with_control<F>(
    gateway: TcpGatewayListener,
    control_gateway: TcpGatewayListener,
    config: GatewayRuntimeConfig,
    connect_service: F,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
) -> Result<(), GatewayRuntimeError>
where
    F: Fn() -> io::Result<std::os::unix::net::UnixStream> + Send + Sync + 'static,
{
    validate_gateway_runtime_config(&config)?;
    let rate_limiter = new_gateway_rate_limiter(&config)?;
    let auth_failure_limiter = new_gateway_auth_failure_limiter(&config)?;
    let connector = Arc::new(connect_service);
    let ordinary_shutdown = Arc::clone(&shutdown);
    let ordinary_connector = Arc::clone(&connector);
    let ordinary_rate_limiter = rate_limiter.clone();
    let ordinary_auth_limiter = auth_failure_limiter.clone();
    let ordinary_config = config;
    let ordinary = std::thread::Builder::new()
        .name("prdb-gateway-ordinary-pool".to_owned())
        .spawn(move || {
            let result = run_gateway_worker_pool(
                gateway,
                ordinary_config,
                false,
                ordinary_connector,
                ordinary_rate_limiter,
                ordinary_auth_limiter,
                Arc::clone(&ordinary_shutdown),
            );
            ordinary_shutdown.store(true, Ordering::Release);
            result
        })
        .map_err(GatewayRuntimeError::Io)?;
    let control_shutdown = Arc::clone(&shutdown);
    let control_config = config;
    let control = match std::thread::Builder::new()
        .name("prdb-gateway-control-pool".to_owned())
        .spawn(move || {
            let result = run_gateway_worker_pool(
                control_gateway,
                control_config,
                true,
                connector,
                rate_limiter,
                auth_failure_limiter,
                Arc::clone(&control_shutdown),
            );
            control_shutdown.store(true, Ordering::Release);
            result
        }) {
        Ok(control) => control,
        Err(error) => {
            shutdown.store(true, Ordering::Release);
            let _ = ordinary.join();
            return Err(GatewayRuntimeError::Io(error));
        }
    };
    let ordinary_result = ordinary
        .join()
        .map_err(|_| GatewayRuntimeError::WorkerPanicked)?;
    let control_result = control
        .join()
        .map_err(|_| GatewayRuntimeError::WorkerPanicked)?;
    ordinary_result?;
    control_result
}

fn validate_gateway_runtime_config(
    config: &GatewayRuntimeConfig,
) -> Result<(), GatewayRuntimeError> {
    if config.worker_count == 0
        || config.queue_capacity == 0
        || config.control_worker_count == 0
        || config.control_queue_capacity == 0
        || config.connections_per_ip_per_minute == 0
        || config.auth_failures_per_ip_per_minute == 0
        || config.idle_timeout.is_zero()
        || config.pre_auth_timeout.is_zero()
        || config.accept_poll_interval.is_zero()
        || config.max_total_result_bytes == 0
    {
        return Err(GatewayRuntimeError::InvalidConfig);
    }
    Ok(())
}

fn new_gateway_rate_limiter(
    config: &GatewayRuntimeConfig,
) -> Result<GatewayPeerRateLimiter, GatewayRuntimeError> {
    GatewayPeerRateLimiter::new(
        config.connections_per_ip_per_minute,
        4096,
        Duration::from_secs(60),
    )
    .map_err(|_| GatewayRuntimeError::InvalidConfig)
}

fn new_gateway_auth_failure_limiter(
    config: &GatewayRuntimeConfig,
) -> Result<GatewayAuthFailureLimiter, GatewayRuntimeError> {
    GatewayAuthFailureLimiter::new(
        config.auth_failures_per_ip_per_minute,
        4096,
        Duration::from_secs(60),
    )
    .map_err(|_| GatewayRuntimeError::InvalidConfig)
}

fn run_gateway_worker_pool<F>(
    gateway: TcpGatewayListener,
    config: GatewayRuntimeConfig,
    control_only: bool,
    connector: Arc<F>,
    rate_limiter: GatewayPeerRateLimiter,
    auth_failure_limiter: GatewayAuthFailureLimiter,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
) -> Result<(), GatewayRuntimeError>
where
    F: Fn() -> io::Result<std::os::unix::net::UnixStream> + Send + Sync + 'static,
{
    let (worker_count, queue_capacity) = if control_only {
        (config.control_worker_count, config.control_queue_capacity)
    } else {
        (config.worker_count, config.queue_capacity)
    };
    gateway
        .listener
        .set_nonblocking(true)
        .map_err(GatewayRuntimeError::Io)?;
    let (sender, receiver) = std::sync::mpsc::sync_channel::<(TcpStream, IpAddr)>(queue_capacity);
    let receiver = Arc::new(std::sync::Mutex::new(receiver));
    let mut workers = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let receiver = Arc::clone(&receiver);
        let connector = Arc::clone(&connector);
        let auth_failure_limiter = auth_failure_limiter.clone();
        let idle_timeout = config.idle_timeout;
        let pre_auth_timeout = config.pre_auth_timeout;
        let max_total_result_bytes = config.max_total_result_bytes;
        workers.push(
            std::thread::Builder::new()
                .name(if control_only {
                    "prdb-gateway-control-worker".to_owned()
                } else {
                    "prdb-gateway-worker".to_owned()
                })
                .spawn(move || {
                    loop {
                        let client = receiver
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .recv();
                        let Ok((mut client, source_ip)) = client else {
                            break;
                        };
                        let Ok(mut service) = connector() else {
                            continue;
                        };
                        let _ = serve_tcp_client_connection_with_limits(
                            &mut client,
                            &mut service,
                            GatewayConnectionSettings {
                                frame_timeout: Some(idle_timeout),
                                pre_auth_timeout: Some(pre_auth_timeout),
                                auth_failure_limiter: Some(&auth_failure_limiter),
                                source_ip: Some(source_ip),
                                control_only,
                                max_total_result_bytes,
                            },
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
                match sender.try_send((client, peer.ip())) {
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
    deadline: Option<std::time::Instant>,
) -> Result<Option<crate::client_protocol::ClientFrame>, GatewayConnectionError> {
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
    protocol_major: u16,
) -> Result<(crate::ipc::Operation, Vec<u8>), GatewayConnectionError> {
    use crate::ipc::Operation;
    let operation = match kind {
        1 => {
            payload.insert(0, protocol_major as u8);
            Operation::OpenClientSession
        }
        2 => {
            payload.insert(0, protocol_major as u8);
            Operation::AuthenticateSession
        }
        3 => Operation::PrepareSubmission,
        4 | 5 | crate::client_protocol::CLIENT_SDK_LIBRARY_CHUNK_KIND => {
            payload.insert(0, kind as u8);
            Operation::UploadSubmissionChunk
        }
        6 => Operation::SealSubmission,
        7 => Operation::StartSubmission,
        8 => Operation::ClientGetInvocationStatus,
        9 => Operation::ClientCancelInvocation,
        10 => Operation::SubmitAdministrativeOperation,
        11 => {
            payload.insert(0, protocol_major as u8);
            Operation::PingClientSession
        }
        13..=18 if protocol_major == 2 => {
            payload.insert(0, kind as u8);
            Operation::InteractiveTransaction
        }
        _ => return Err(GatewayConnectionError::UnsupportedClientKind(kind)),
    };
    Ok((operation, payload))
}

fn encode_client_response(
    frame: &crate::client_protocol::ClientFrame,
    protocol_major: u16,
) -> Result<Vec<u8>, GatewayConnectionError> {
    match protocol_major {
        1 => frame.encode(),
        2 => frame.encode_v2(),
        _ => return Err(GatewayConnectionError::MismatchedServiceResponse),
    }
    .map_err(GatewayConnectionError::ClientFrame)
}

#[cfg(test)]
mod gateway_error_payload_tests {
    use super::{GatewayClientErrorCode as ErrorCode, client_request_to_ipc};
    use crate::client_protocol::{ClientFrame, ClientFrameDecoder};

    #[test]
    fn gateway_error_payloads_have_one_canonical_v1_encoding() {
        let cases = [
            (
                ErrorCode::InvalidRequest,
                br#"{"code":"invalid_request","message":"Request is invalid."}"#.as_slice(),
            ),
            (
                ErrorCode::InvalidState,
                br#"{"code":"invalid_state","message":"Request is not valid in the current session state."}"#.as_slice(),
            ),
            (
                ErrorCode::Unauthenticated,
                br#"{"code":"unauthenticated","message":"Authentication is required."}"#.as_slice(),
            ),
            (
                ErrorCode::ResourceLimit,
                br#"{"code":"resource_limit","message":"Request exceeds a configured limit."}"#.as_slice(),
            ),
            (
                ErrorCode::Unavailable,
                br#"{"code":"unavailable","message":"Requested resource is unavailable."}"#.as_slice(),
            ),
        ];
        for (code, expected) in cases {
            assert_eq!(code.payload(), expected);
            assert_eq!(ErrorCode::from_payload(expected), Some(code));
        }
        assert_eq!(
            ErrorCode::from_payload(br#"{"code":"invalid_request"}"#),
            None
        );
        assert_eq!(
            ErrorCode::from_payload(br#"{"code":"invalid_request","message":"secret detail"}"#),
            None
        );
    }

    #[test]
    fn sql_library_chunk_kind_is_rejected_at_the_gateway_boundary() {
        assert!(matches!(
            client_request_to_ipc(12, vec![0xaa, 0xbb], 1),
            Err(super::GatewayConnectionError::UnsupportedClientKind(12))
        ));
    }

    #[test]
    fn v2_transaction_kinds_route_to_gateway_only_ipc_operation() {
        for kind in 13..=18 {
            let (operation, payload) = client_request_to_ipc(kind, vec![0xaa, 0xbb], 2).unwrap();
            assert_eq!(operation, crate::ipc::Operation::InteractiveTransaction);
            assert_eq!(payload, [kind as u8, 0xaa, 0xbb]);
            assert!(
                crate::ipc::authorize_operation(crate::ipc::ServiceRole::Gateway, operation)
                    .is_ok()
            );
            assert!(
                crate::ipc::authorize_operation(crate::ipc::ServiceRole::Executor, operation)
                    .is_err()
            );
            assert!(client_request_to_ipc(kind, vec![0xaa], 1).is_err());
        }
        assert!(
            crate::ipc::authorize_operation(
                crate::ipc::ServiceRole::Executor,
                crate::ipc::Operation::SubmitAdministrativeOperation,
            )
            .is_err(),
            "the confined executor must not access the ADMIN control channel"
        );
        let (operation, payload) =
            client_request_to_ipc(1, b"{\"mode\":\"query\"}".to_vec(), 2).unwrap();
        assert_eq!(operation, crate::ipc::Operation::OpenClientSession);
        assert_eq!(payload[0], 2);
        let response = ClientFrame {
            kind: 100,
            request_id: 3,
            response_sequence: 0,
            payload: b"{}".to_vec(),
        };
        let bytes = super::encode_client_response(&response, 2).unwrap();
        let mut decoder = ClientFrameDecoder::v2();
        assert_eq!(decoder.push(&bytes).unwrap()[0].kind, 100);
    }
}

#[cfg(test)]
mod result_relay_tests {
    use super::{
        GatewayClientErrorCode, GatewayClientStream, GatewayConnectionError,
        relay_gateway_result_events_with_limit, write_client_error_version,
    };
    use crate::client_protocol::ClientFrameDecoder;
    use crate::ipc::{IpcFrame, Operation, write_frame};
    use std::io::{self, Read, Write};
    use std::os::unix::net::UnixStream;
    use std::time::Duration;

    #[derive(Default)]
    struct CapturedClient(Vec<u8>);

    impl Read for CapturedClient {
        fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
            Ok(0)
        }
    }

    impl Write for CapturedClient {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl GatewayClientStream for CapturedClient {
        fn set_gateway_read_timeout(&mut self, _timeout: Option<Duration>) -> io::Result<()> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct CountingClient(u64);

    impl Read for CountingClient {
        fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
            Ok(0)
        }
    }

    impl Write for CountingClient {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 = self.0.saturating_add(bytes.len() as u64);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl GatewayClientStream for CountingClient {
        fn set_gateway_read_timeout(&mut self, _timeout: Option<Duration>) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn v2_gateway_rejection_uses_the_negotiated_frame_version() {
        let mut client = CapturedClient::default();
        write_client_error_version(&mut client, 12, "invalid_request", 2).unwrap();
        let mut decoder = ClientFrameDecoder::v2();
        let frames = decoder.push(&client.0).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].kind, crate::client_protocol::CLIENT_ERROR_KIND);
        assert_eq!(frames[0].request_id, 12);
        assert_eq!(
            frames[0].payload,
            GatewayClientErrorCode::InvalidRequest.payload()
        );
    }

    #[test]
    fn result_relay_rejects_cumulative_output_over_budget() {
        let before = super::gateway_result_bytes_sent_to_clients_total();
        let (mut relay_service, mut supervisor_service) = UnixStream::pair().unwrap();
        for payload in [b"abc".as_slice(), b"d".as_slice()] {
            write_frame(
                &mut supervisor_service,
                &IpcFrame {
                    operation: Operation::GatewayResultData as u16,
                    request_id: 7,
                    payload: payload.to_vec(),
                },
            )
            .unwrap();
        }
        write_frame(
            &mut supervisor_service,
            &IpcFrame {
                operation: Operation::GatewayTerminal as u16,
                request_id: 7,
                payload: b"committed".to_vec(),
            },
        )
        .unwrap();

        let mut client = CapturedClient::default();
        let error =
            relay_gateway_result_events_with_limit(&mut client, &mut relay_service, 7, 3, 1)
                .unwrap_err();
        assert!(matches!(error, GatewayConnectionError::ResultLimitExceeded));

        let mut decoder = ClientFrameDecoder::default();
        let frames = decoder.push(&client.0).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].kind, 102);
        assert!(super::gateway_result_bytes_sent_to_clients_total() >= before.saturating_add(3));
        assert_eq!(frames[0].payload, b"abc");
    }

    #[test]
    fn result_relay_accepts_elevated_cumulative_budget_with_bounded_memory() {
        let elevated_limit = 128 * 1024 * 1024u64;
        let result_chunk = vec![0x5a; 16 * 1024];
        let frame_count = elevated_limit / result_chunk.len() as u64;
        let (mut relay_service, mut supervisor_service) = UnixStream::pair().unwrap();
        let writer = std::thread::spawn(move || {
            for _ in 0..frame_count {
                write_frame(
                    &mut supervisor_service,
                    &IpcFrame {
                        operation: Operation::GatewayResultData as u16,
                        request_id: 7,
                        payload: result_chunk.clone(),
                    },
                )
                .unwrap();
            }
            write_frame(
                &mut supervisor_service,
                &IpcFrame {
                    operation: Operation::GatewayTerminal as u16,
                    request_id: 7,
                    payload: Vec::new(),
                },
            )
            .unwrap();
        });

        let mut client = CountingClient::default();
        relay_gateway_result_events_with_limit(
            &mut client,
            &mut relay_service,
            7,
            elevated_limit,
            1,
        )
        .unwrap();
        writer.join().unwrap();
        assert!(client.0 > elevated_limit);
    }

    #[test]
    fn gateway_runtime_counter_is_added_to_a_version_six_object() {
        assert_eq!(
            super::append_gateway_result_counter(
                br#"{"invocations":[],"gateway_result_bytes_relayed_total":41}"#,
                42
            )
            .unwrap(),
            br#"{"invocations":[],"gateway_result_bytes_relayed_total":41,"gateway_result_bytes_sent_to_clients_total":42}"#
        );
        assert!(super::append_gateway_result_counter(b"[]", 42).is_none());
        assert!(super::append_gateway_result_counter(b"", 42).is_none());
        assert_eq!(
            super::append_gateway_result_counter(br#"{"error":"unauthorized"}"#, 42).unwrap(),
            br#"{"error":"unauthorized"}"#
        );
    }

    #[test]
    fn gateway_result_byte_counter_saturates() {
        let counter = std::sync::atomic::AtomicU64::new(u64::MAX - 1);
        super::saturating_add_counter(&counter, 1);
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), u64::MAX);
        super::saturating_add_counter(&counter, 1);
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), u64::MAX);
    }
}
