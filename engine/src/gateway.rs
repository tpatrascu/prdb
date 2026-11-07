//! TCP listener configuration for the plaintext client gateway.
//!
//! The listener is deliberately separate from storage and does not open
//! database files or accept a storage manager. Connection serving is layered
//! on top of this bind policy.

use std::io;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};

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
    Io(io::Error),
}

impl std::fmt::Display for GatewayListenerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NonLoopbackPlaintextDisabled(address) => write!(
                formatter,
                "plaintext gateway binding to {address} requires explicit opt-in"
            ),
            Self::Io(error) => write!(formatter, "gateway listener failed: {error}"),
        }
    }
}

impl std::error::Error for GatewayListenerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NonLoopbackPlaintextDisabled(_) => None,
            Self::Io(error) => Some(error),
        }
    }
}

pub struct TcpGatewayListener {
    listener: TcpListener,
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
}

/// Forward one TCP client connection over its already-provisioned private
/// gateway service stream. Callers must configure read/write deadlines and
/// connection limits before serving untrusted peers.
pub fn serve_tcp_client_connection<C: Read + Write, S: Read + Write>(
    client: &mut C,
    service: &mut S,
) -> Result<(), GatewayConnectionError> {
    let mut decoder = crate::client_protocol::ClientFrameDecoder::default();
    let mut input = [0u8; 8192];
    loop {
        let Some(request) = read_client_request(client, &mut decoder, &mut input)? else {
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

fn read_client_request<R: Read>(
    client: &mut R,
    decoder: &mut crate::client_protocol::ClientFrameDecoder,
    input: &mut [u8],
) -> Result<Option<crate::client_protocol::ClientFrame>, GatewayConnectionError> {
    loop {
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
