//! Bounded framing primitives for client network protocol version 1.0.

use crate::credentials::TOKEN_SECRET_BYTES;
use crate::storage_manager::{AuthenticatedQuerySession, StorageManager};
use crate::supervisor::StagingOwner;
use crate::transaction::TransactionError;
use std::collections::HashMap;
use zeroize::Zeroize;

pub const CLIENT_FRAME_HEADER_BYTES: usize = 32;
pub const CLIENT_MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
pub const CLIENT_MAX_CONTROL_BYTES: usize = 64 * 1024;
pub const CLIENT_MAX_CHUNK_BYTES: usize = 256 * 1024;
const CLIENT_MAGIC: &[u8; 4] = b"PDBN";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientFrame {
    pub kind: u16,
    pub request_id: u64,
    pub response_sequence: u64,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Header {
    kind: u16,
    payload_bytes: usize,
    request_id: u64,
    response_sequence: u64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ClientFrameError {
    InvalidMagic,
    UnsupportedVersion { major: u16, minor: u16 },
    InvalidKind(u16),
    NonzeroFlags,
    PayloadTooLarge,
    InvalidChunkLength,
    InvalidSequence,
    ZeroRequestId,
    AllocationFailed,
    IncompleteFrame,
    RequestIdsNotIncreasing,
    UnexpectedResponseSequence,
}

impl std::fmt::Display for ClientFrameError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "client protocol frame error: {self:?}")
    }
}

impl std::error::Error for ClientFrameError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientMode {
    Query,
    Control,
}

#[derive(Debug)]
pub enum ClientSessionError {
    InvalidHello,
    HelloAlreadyAccepted,
    AuthenticationOutOfOrder,
    AlreadyAuthenticated,
    AuthenticationRequired,
    WrongMode,
    Storage(TransactionError),
}

impl std::fmt::Display for ClientSessionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "client session error: {self:?}")
    }
}

impl std::error::Error for ClientSessionError {}

/// Per-connection authentication and mode state used by the gateway dispatcher.
/// It keeps the bearer secret only for this connection lifetime, zeroizes it
/// on drop, and gives staging authority only after storage AUTH.
#[derive(Default)]
pub struct ClientConnectionSession {
    hello: Option<ClientMode>,
    authenticated: Option<AuthenticatedQuerySession>,
    token_secret: Option<[u8; TOKEN_SECRET_BYTES]>,
}

impl ClientConnectionSession {
    /// Accept the one HELLO for this connection before any AUTH request.
    pub fn accept_hello(
        &mut self,
        mode: ClientMode,
        client_name: &str,
    ) -> Result<(), ClientSessionError> {
        if self.hello.is_some() {
            return Err(ClientSessionError::HelloAlreadyAccepted);
        }
        if client_name.len() > 128 || self.authenticated.is_some() {
            return Err(ClientSessionError::InvalidHello);
        }
        self.hello = Some(mode);
        Ok(())
    }

    /// Authenticate using storage's current token and login/role snapshot.
    /// Callers should map all storage authentication failures to one public
    /// unauthenticated response category.
    pub fn authenticate(
        &mut self,
        storage: &StorageManager<'_>,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
    ) -> Result<&AuthenticatedQuerySession, ClientSessionError> {
        if self.hello.is_none() {
            return Err(ClientSessionError::AuthenticationOutOfOrder);
        }
        if self.authenticated.is_some() {
            return Err(ClientSessionError::AlreadyAuthenticated);
        }
        let session = storage
            .authenticate_query_session(token_id, token_secret)
            .map_err(ClientSessionError::Storage)?;
        self.authenticated = Some(session);
        self.token_secret = Some(*token_secret);
        Ok(self
            .authenticated
            .as_ref()
            .expect("session was just stored"))
    }

    /// Authorize a request kind and refresh token/login/role authority before
    /// each execution, status, cancellation, or administrative operation.
    pub fn authorize_request(
        &mut self,
        storage: &StorageManager<'_>,
        kind: u16,
    ) -> Result<(), ClientSessionError> {
        if !self.permits_request_kind(kind) {
            return if self.authenticated.is_none() {
                Err(ClientSessionError::AuthenticationRequired)
            } else {
                Err(ClientSessionError::WrongMode)
            };
        }
        if (3..=10).contains(&kind) {
            let session = self
                .authenticated
                .as_mut()
                .ok_or(ClientSessionError::AuthenticationRequired)?;
            let secret = self
                .token_secret
                .as_ref()
                .ok_or(ClientSessionError::AuthenticationRequired)?;
            storage
                .revalidate_query_session(session, secret)
                .map_err(ClientSessionError::Storage)?;
        }
        Ok(())
    }

    pub fn session_id(&self) -> Result<[u8; 16], ClientSessionError> {
        self.authenticated
            .as_ref()
            .map(AuthenticatedQuerySession::connection_id)
            .ok_or(ClientSessionError::AuthenticationRequired)
    }

    /// Return the authenticated owner required for query staging.
    pub fn staging_owner(&self) -> Result<StagingOwner, ClientSessionError> {
        if self.hello != Some(ClientMode::Query) {
            return Err(ClientSessionError::WrongMode);
        }
        self.authenticated
            .as_ref()
            .map(AuthenticatedQuerySession::staging_owner)
            .ok_or(ClientSessionError::AuthenticationRequired)
    }

    /// Enforce the v1 mode and pre-authentication operation allowlists.
    pub fn permits_request_kind(&self, kind: u16) -> bool {
        let Some(mode) = self.hello else {
            return kind == 1 || kind == 11;
        };
        let Some(_) = self.authenticated else {
            return kind == 2 || kind == 11;
        };
        match mode {
            ClientMode::Query => (3..=9).contains(&kind) || kind == 11,
            ClientMode::Control => (8..=10).contains(&kind) || kind == 11,
        }
    }
}

impl Drop for ClientConnectionSession {
    fn drop(&mut self) {
        if let Some(secret) = &mut self.token_secret {
            secret.zeroize();
        }
    }
}

#[derive(Default)]
pub struct ClientFrameDecoder {
    header: [u8; CLIENT_FRAME_HEADER_BYTES],
    header_bytes: usize,
    pending_header: Option<Header>,
    pending_payload: Vec<u8>,
}

impl ClientFrameDecoder {
    /// Add any part of the TCP byte stream and return all complete frames.
    /// Header limits are checked before payload storage is reserved.
    pub fn push(&mut self, mut input: &[u8]) -> Result<Vec<ClientFrame>, ClientFrameError> {
        let mut frames = Vec::new();
        while !input.is_empty() {
            if self.pending_header.is_none() {
                let needed = CLIENT_FRAME_HEADER_BYTES - self.header_bytes;
                let copied = needed.min(input.len());
                self.header[self.header_bytes..self.header_bytes + copied]
                    .copy_from_slice(&input[..copied]);
                self.header_bytes += copied;
                input = &input[copied..];
                if self.header_bytes < CLIENT_FRAME_HEADER_BYTES {
                    continue;
                }
                let header = decode_header(&self.header)?;
                self.pending_payload = Vec::new();
                self.pending_payload
                    .try_reserve_exact(header.payload_bytes)
                    .map_err(|_| ClientFrameError::AllocationFailed)?;
                self.pending_header = Some(header);
                if header.payload_bytes == 0 {
                    frames.push(self.take_pending_frame());
                    continue;
                }
            }

            let header = self.pending_header.expect("pending frame header");
            let needed = header.payload_bytes - self.pending_payload.len();
            let copied = needed.min(input.len());
            self.pending_payload.extend_from_slice(&input[..copied]);
            input = &input[copied..];
            if self.pending_payload.len() == header.payload_bytes {
                frames.push(self.take_pending_frame());
            }
        }
        Ok(frames)
    }

    /// Reject an EOF that leaves any partial header or payload buffered.
    pub fn finish(&self) -> Result<(), ClientFrameError> {
        if self.header_bytes != 0 || self.pending_header.is_some() {
            Err(ClientFrameError::IncompleteFrame)
        } else {
            Ok(())
        }
    }

    fn take_pending_frame(&mut self) -> ClientFrame {
        let header = self.pending_header.take().expect("pending frame header");
        let payload = std::mem::take(&mut self.pending_payload);
        self.header_bytes = 0;
        ClientFrame {
            kind: header.kind,
            request_id: header.request_id,
            response_sequence: header.response_sequence,
            payload,
        }
    }
}

impl ClientFrame {
    pub fn encode(&self) -> Result<Vec<u8>, ClientFrameError> {
        let header = Header {
            kind: self.kind,
            payload_bytes: self.payload.len(),
            request_id: self.request_id,
            response_sequence: self.response_sequence,
        };
        validate_header_fields(header)?;
        let payload_bytes =
            u32::try_from(self.payload.len()).map_err(|_| ClientFrameError::PayloadTooLarge)?;
        let total_bytes = CLIENT_FRAME_HEADER_BYTES
            .checked_add(self.payload.len())
            .ok_or(ClientFrameError::PayloadTooLarge)?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(total_bytes)
            .map_err(|_| ClientFrameError::AllocationFailed)?;
        output.extend_from_slice(CLIENT_MAGIC);
        output.extend_from_slice(&1u16.to_le_bytes());
        output.extend_from_slice(&0u16.to_le_bytes());
        output.extend_from_slice(&self.kind.to_le_bytes());
        output.extend_from_slice(&0u16.to_le_bytes());
        output.extend_from_slice(&payload_bytes.to_le_bytes());
        output.extend_from_slice(&self.request_id.to_le_bytes());
        output.extend_from_slice(&self.response_sequence.to_le_bytes());
        output.extend_from_slice(&self.payload);
        Ok(output)
    }
}

fn decode_header(bytes: &[u8; CLIENT_FRAME_HEADER_BYTES]) -> Result<Header, ClientFrameError> {
    if &bytes[..4] != CLIENT_MAGIC {
        return Err(ClientFrameError::InvalidMagic);
    }
    let major = u16::from_le_bytes([bytes[4], bytes[5]]);
    let minor = u16::from_le_bytes([bytes[6], bytes[7]]);
    if major != 1 || minor != 0 {
        return Err(ClientFrameError::UnsupportedVersion { major, minor });
    }
    let kind = u16::from_le_bytes([bytes[8], bytes[9]]);
    let flags = u16::from_le_bytes([bytes[10], bytes[11]]);
    if flags != 0 {
        return Err(ClientFrameError::NonzeroFlags);
    }
    let payload_bytes = u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]) as usize;
    let request_id = u64::from_le_bytes(bytes[16..24].try_into().expect("fixed request ID"));
    let response_sequence =
        u64::from_le_bytes(bytes[24..32].try_into().expect("fixed response sequence"));
    let header = Header {
        kind,
        payload_bytes,
        request_id,
        response_sequence,
    };
    validate_header_fields(header)?;
    Ok(header)
}

fn validate_header_fields(header: Header) -> Result<(), ClientFrameError> {
    let maximum = match header.kind {
        1..=3 | 6..=11 | 100 | 101 | 103 => CLIENT_MAX_CONTROL_BYTES,
        4 | 5 => CLIENT_MAX_CHUNK_BYTES + 24,
        102 => CLIENT_MAX_PAYLOAD_BYTES,
        kind => return Err(ClientFrameError::InvalidKind(kind)),
    };
    if header.payload_bytes > maximum || header.payload_bytes > CLIENT_MAX_PAYLOAD_BYTES {
        return Err(ClientFrameError::PayloadTooLarge);
    }
    if header.request_id == 0 {
        return Err(ClientFrameError::ZeroRequestId);
    }
    match header.kind {
        4 | 5 if header.payload_bytes <= 24 => Err(ClientFrameError::InvalidChunkLength),
        1..=11 if header.response_sequence != 0 => Err(ClientFrameError::InvalidSequence),
        100 | 101 if header.response_sequence != 0 => Err(ClientFrameError::InvalidSequence),
        102 | 103 if header.response_sequence == 0 => Err(ClientFrameError::InvalidSequence),
        _ => Ok(()),
    }
}

#[derive(Default)]
pub struct ClientRequestIds {
    last_request_id: u64,
}

impl ClientRequestIds {
    /// Enforce strictly increasing nonzero IDs on a single client connection.
    pub fn accept(&mut self, frame: &ClientFrame) -> Result<(), ClientFrameError> {
        if !(1..=11).contains(&frame.kind)
            || frame.request_id == 0
            || frame.request_id <= self.last_request_id
        {
            return Err(ClientFrameError::RequestIdsNotIncreasing);
        }
        self.last_request_id = frame.request_id;
        Ok(())
    }
}

#[derive(Default)]
pub struct ClientResponseSequences {
    next_by_request: HashMap<u64, u64>,
}

impl ClientResponseSequences {
    /// Begin streamed responses after the sequence-zero START acceptance.
    pub fn begin_stream(&mut self, request_id: u64) -> Result<(), ClientFrameError> {
        if request_id == 0
            || !self.next_by_request.is_empty()
            || self.next_by_request.contains_key(&request_id)
        {
            return Err(ClientFrameError::UnexpectedResponseSequence);
        }
        self.next_by_request.insert(request_id, 1);
        Ok(())
    }

    /// Validate consecutive RESULT_DATA frames and the terminal sequence.
    pub fn accept_stream_frame(&mut self, frame: &ClientFrame) -> Result<bool, ClientFrameError> {
        let expected = self
            .next_by_request
            .get(&frame.request_id)
            .copied()
            .ok_or(ClientFrameError::UnexpectedResponseSequence)?;
        if !matches!(frame.kind, 102 | 103) || frame.response_sequence != expected {
            return Err(ClientFrameError::UnexpectedResponseSequence);
        }
        if frame.kind == 103 {
            self.next_by_request.remove(&frame.request_id);
            Ok(true)
        } else {
            self.next_by_request.insert(
                frame.request_id,
                expected
                    .checked_add(1)
                    .ok_or(ClientFrameError::UnexpectedResponseSequence)?,
            );
            Ok(false)
        }
    }
}
