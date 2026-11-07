//! TCP listener configuration for the plaintext client gateway.
//!
//! The listener is deliberately separate from storage and does not open
//! database files or accept a storage manager. Connection serving is layered
//! on top of this bind policy.

use std::io;
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
