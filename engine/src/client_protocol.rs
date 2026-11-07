//! Bounded framing primitives for client network protocol version 1.0.

use crate::credentials::TOKEN_SECRET_BYTES;
use crate::storage_manager::{AuthenticatedQuerySession, StorageManager};
use crate::supervisor::StagingOwner;
use crate::transaction::TransactionError;
use std::collections::HashMap;
use zeroize::{Zeroize, Zeroizing};

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
    UnsupportedRequest,
    InvalidControlPayload(ClientControlError),
    Staging(crate::supervisor::StagingError),
    Storage(TransactionError),
}

#[derive(Debug)]
pub enum ClientStartError {
    Session(ClientSessionError),
    Start(crate::supervisor::StartError<TransactionError>),
}

impl std::fmt::Display for ClientStartError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "client START failed: {self:?}")
    }
}

impl std::error::Error for ClientStartError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientControlError {
    InvalidJson,
    DuplicateKey,
    UnknownKey,
    MissingKey,
    InvalidMode,
    InvalidClientName,
    InvalidTokenId,
    InvalidTokenSecret,
    InvalidChunk,
    InvalidPrepare,
    InvalidInvocationId,
}

impl std::fmt::Display for ClientControlError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "invalid client control payload: {self:?}")
    }
}

impl std::error::Error for ClientControlError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientHello {
    pub mode: ClientMode,
    pub client_name: String,
}

#[derive(Eq, PartialEq)]
pub struct ClientCredentials {
    pub token_id: [u8; 16],
    pub token_secret: [u8; TOKEN_SECRET_BYTES],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientChunk {
    pub invocation_id: [u8; 16],
    pub offset: u64,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientPrepare {
    pub manifest_version: u32,
    pub manifest: Vec<u8>,
    pub artifact_bytes: u64,
    pub artifact_sha256: [u8; 32],
    pub parameter_bytes: u64,
    pub parameter_sha256: [u8; 32],
    pub scope_version: u32,
    pub requested_scope: Vec<u8>,
    pub deadline_ms: u64,
    pub workload_class: crate::supervisor::WorkloadClass,
}

impl Drop for ClientCredentials {
    fn drop(&mut self) {
        self.token_secret.zeroize();
    }
}

/// Decode the exact HELLO v1 JSON object. Keys and values are strictly typed
/// strings; duplicate and unknown keys are rejected.
pub fn decode_client_hello(payload: &[u8]) -> Result<ClientHello, ClientControlError> {
    let mut fields = parse_string_object(payload)?;
    let mode = match take_required(&mut fields, "mode")?.as_str() {
        "query" => ClientMode::Query,
        "control" => ClientMode::Control,
        _ => return Err(ClientControlError::InvalidMode),
    };
    let client_name = take_required(&mut fields, "client_name")?;
    if client_name.len() > 128 {
        return Err(ClientControlError::InvalidClientName);
    }
    reject_unknown_fields(fields)?;
    Ok(ClientHello { mode, client_name })
}

/// Decode the exact AUTH v1 JSON object, requiring a lowercase opaque token
/// ID and canonical padded base64 for the fixed-size bearer secret.
pub fn decode_client_credentials(payload: &[u8]) -> Result<ClientCredentials, ClientControlError> {
    let mut fields = parse_string_object(payload)?;
    let token_id = decode_lower_hex_id(&take_required(&mut fields, "token_id")?)?;
    let secret_bytes = decode_canonical_base64(&take_required(&mut fields, "token_secret")?)?;
    let token_secret = secret_bytes
        .as_slice()
        .try_into()
        .map_err(|_| ClientControlError::InvalidTokenSecret)?;
    reject_unknown_fields(fields)?;
    Ok(ClientCredentials {
        token_id,
        token_secret,
    })
}

/// Decode ARTIFACT_CHUNK/PARAMETER_CHUNK as invocation ID, little-endian
/// contiguous offset, and 1–256 KiB of opaque data.
pub fn decode_client_chunk(payload: &[u8]) -> Result<ClientChunk, ClientControlError> {
    if !(25..=24 + CLIENT_MAX_CHUNK_BYTES).contains(&payload.len()) {
        return Err(ClientControlError::InvalidChunk);
    }
    let invocation_id: [u8; 16] = payload[..16]
        .try_into()
        .map_err(|_| ClientControlError::InvalidChunk)?;
    if invocation_id == [0; 16] {
        return Err(ClientControlError::InvalidChunk);
    }
    let offset = u64::from_le_bytes(
        payload[16..24]
            .try_into()
            .map_err(|_| ClientControlError::InvalidChunk)?,
    );
    let mut data = Vec::new();
    data.try_reserve_exact(payload.len() - 24)
        .map_err(|_| ClientControlError::InvalidChunk)?;
    data.extend_from_slice(&payload[24..]);
    Ok(ClientChunk {
        invocation_id,
        offset,
        data,
    })
}

/// Decode the exact PREPARE JSON schema, validating declared sizes and all
/// canonical representations before allocating decoded metadata buffers.
pub fn decode_client_prepare(payload: &[u8]) -> Result<ClientPrepare, ClientControlError> {
    let mut fields = parse_scalar_object(payload)?;
    let manifest_version = take_number(&mut fields, "manifest_version")?
        .try_into()
        .map_err(|_| ClientControlError::InvalidPrepare)?;
    let manifest_text = take_string(&mut fields, "manifest")?;
    let artifact_bytes = take_decimal_string(&mut fields, "artifact_bytes")?;
    let artifact_sha256 = decode_lower_hex_hash(&take_string(&mut fields, "artifact_sha256")?)?;
    let parameter_bytes = take_decimal_string(&mut fields, "parameter_bytes")?;
    let parameter_sha256 = decode_lower_hex_hash(&take_string(&mut fields, "parameter_sha256")?)?;
    let scope_version = take_number(&mut fields, "scope_version")?
        .try_into()
        .map_err(|_| ClientControlError::InvalidPrepare)?;
    let scope_text = take_string(&mut fields, "requested_scope")?;
    let deadline_ms = take_decimal_string(&mut fields, "deadline_ms")?;
    let workload_class = match take_string(&mut fields, "workload_class")?.as_str() {
        "operational" => crate::supervisor::WorkloadClass::Operational,
        "analytical" => crate::supervisor::WorkloadClass::Analytical,
        _ => return Err(ClientControlError::InvalidPrepare),
    };
    reject_unknown_scalars(fields)?;
    if artifact_bytes == 0
        || artifact_bytes > crate::supervisor::MAX_ARTIFACT_BYTES
        || parameter_bytes > crate::supervisor::MAX_PARAMETER_BYTES
        || deadline_ms == 0
        || manifest_text.len() > crate::supervisor::MAX_MANIFEST_BYTES.div_ceil(3) * 4
        || scope_text.len() > crate::supervisor::MAX_SCOPE_BYTES.div_ceil(3) * 4
    {
        return Err(ClientControlError::InvalidPrepare);
    }
    let manifest = decode_base64_bytes(&manifest_text)
        .filter(|bytes| !bytes.is_empty() && bytes.len() <= crate::supervisor::MAX_MANIFEST_BYTES)
        .ok_or(ClientControlError::InvalidPrepare)?;
    let requested_scope = decode_base64_bytes(&scope_text)
        .filter(|bytes| !bytes.is_empty() && bytes.len() <= crate::supervisor::MAX_SCOPE_BYTES)
        .ok_or(ClientControlError::InvalidPrepare)?;
    Ok(ClientPrepare {
        manifest_version,
        manifest,
        artifact_bytes,
        artifact_sha256,
        parameter_bytes,
        parameter_sha256,
        scope_version,
        requested_scope,
        deadline_ms,
        workload_class,
    })
}

pub fn decode_client_seal(payload: &[u8]) -> Result<[u8; 16], ClientControlError> {
    let mut fields = parse_string_object(payload)?;
    let invocation_id = decode_lower_hex_id(&take_required(&mut fields, "invocation_id")?)
        .map_err(|_| ClientControlError::InvalidInvocationId)?;
    reject_unknown_fields(fields)?;
    Ok(invocation_id)
}

#[derive(Debug)]
enum JsonScalar {
    String(String),
    Number(u64),
}

fn parse_scalar_object(payload: &[u8]) -> Result<HashMap<String, JsonScalar>, ClientControlError> {
    if payload.len() > CLIENT_MAX_CONTROL_BYTES {
        return Err(ClientControlError::InvalidJson);
    }
    let text = std::str::from_utf8(payload).map_err(|_| ClientControlError::InvalidJson)?;
    let mut parser = JsonStringObjectParser {
        text: text.as_bytes(),
        offset: 0,
    };
    parser.skip_whitespace();
    parser.expect(b'{')?;
    let mut fields = HashMap::new();
    parser.skip_whitespace();
    if !parser.consume(b'}') {
        loop {
            parser.skip_whitespace();
            let key = parser.parse_string()?;
            parser.skip_whitespace();
            parser.expect(b':')?;
            parser.skip_whitespace();
            let value = if parser.text.get(parser.offset) == Some(&b'"') {
                JsonScalar::String(parser.parse_string()?)
            } else {
                JsonScalar::Number(parser.parse_u64_number()?)
            };
            if fields.insert(key, value).is_some() {
                return Err(ClientControlError::DuplicateKey);
            }
            parser.skip_whitespace();
            if parser.consume(b'}') {
                break;
            }
            parser.expect(b',')?;
        }
    }
    parser.skip_whitespace();
    if parser.offset != parser.text.len() {
        return Err(ClientControlError::InvalidJson);
    }
    Ok(fields)
}

fn take_string(
    fields: &mut HashMap<String, JsonScalar>,
    key: &str,
) -> Result<String, ClientControlError> {
    match fields.remove(key).ok_or(ClientControlError::MissingKey)? {
        JsonScalar::String(value) => Ok(value),
        JsonScalar::Number(_) => Err(ClientControlError::InvalidPrepare),
    }
}

fn take_number(
    fields: &mut HashMap<String, JsonScalar>,
    key: &str,
) -> Result<u64, ClientControlError> {
    match fields.remove(key).ok_or(ClientControlError::MissingKey)? {
        JsonScalar::Number(value) => Ok(value),
        JsonScalar::String(_) => Err(ClientControlError::InvalidPrepare),
    }
}

fn take_decimal_string(
    fields: &mut HashMap<String, JsonScalar>,
    key: &str,
) -> Result<u64, ClientControlError> {
    let value = take_string(fields, key)?;
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(ClientControlError::InvalidPrepare);
    }
    value
        .parse()
        .map_err(|_| ClientControlError::InvalidPrepare)
}

fn decode_lower_hex_hash(text: &str) -> Result<[u8; 32], ClientControlError> {
    if text.len() != 64 {
        return Err(ClientControlError::InvalidPrepare);
    }
    let mut hash = [0; 32];
    for (index, pair) in text.as_bytes().chunks_exact(2).enumerate() {
        hash[index] = (lower_hex_nibble(pair[0]).ok_or(ClientControlError::InvalidPrepare)? << 4)
            | lower_hex_nibble(pair[1]).ok_or(ClientControlError::InvalidPrepare)?;
    }
    Ok(hash)
}

fn decode_base64_bytes(text: &str) -> Option<Vec<u8>> {
    let input = text.as_bytes();
    if input.is_empty() || !input.len().is_multiple_of(4) {
        return None;
    }
    let mut output = Vec::new();
    output.try_reserve_exact(input.len() / 4 * 3).ok()?;
    for (index, chunk) in input.chunks_exact(4).enumerate() {
        let last = index + 1 == input.len() / 4;
        let a = base64_value(chunk[0])?;
        let b = base64_value(chunk[1])?;
        output.push((a << 2) | (b >> 4));
        match (chunk[2], chunk[3]) {
            (b'=', b'=') if last && b & 0x0f == 0 => (),
            (c, b'=') if last => {
                let c = base64_value(c)?;
                if c & 0x03 != 0 {
                    return None;
                }
                output.push((b << 4) | (c >> 2));
            }
            (c, d) if c != b'=' && d != b'=' => {
                let c = base64_value(c)?;
                let d = base64_value(d)?;
                output.push((b << 4) | (c >> 2));
                output.push((c << 6) | d);
            }
            _ => return None,
        }
    }
    Some(output)
}

fn parse_string_object(payload: &[u8]) -> Result<HashMap<String, String>, ClientControlError> {
    if payload.len() > CLIENT_MAX_CONTROL_BYTES {
        return Err(ClientControlError::InvalidJson);
    }
    let text = std::str::from_utf8(payload).map_err(|_| ClientControlError::InvalidJson)?;
    let mut parser = JsonStringObjectParser {
        text: text.as_bytes(),
        offset: 0,
    };
    parser.skip_whitespace();
    parser.expect(b'{')?;
    parser.skip_whitespace();
    let mut fields = HashMap::new();
    if parser.consume(b'}') {
        parser.skip_whitespace();
        return if parser.offset == parser.text.len() {
            Ok(fields)
        } else {
            Err(ClientControlError::InvalidJson)
        };
    }
    loop {
        parser.skip_whitespace();
        let key = parser.parse_string()?;
        parser.skip_whitespace();
        parser.expect(b':')?;
        parser.skip_whitespace();
        let value = parser.parse_string()?;
        if fields.insert(key, value).is_some() {
            return Err(ClientControlError::DuplicateKey);
        }
        parser.skip_whitespace();
        if parser.consume(b'}') {
            parser.skip_whitespace();
            return if parser.offset == parser.text.len() {
                Ok(fields)
            } else {
                Err(ClientControlError::InvalidJson)
            };
        }
        parser.expect(b',')?;
    }
}

struct JsonStringObjectParser<'a> {
    text: &'a [u8],
    offset: usize,
}

impl JsonStringObjectParser<'_> {
    fn skip_whitespace(&mut self) {
        while self
            .text
            .get(self.offset)
            .is_some_and(|byte| matches!(byte, b' ' | b'\n' | b'\r' | b'\t'))
        {
            self.offset += 1;
        }
    }

    fn consume(&mut self, expected: u8) -> bool {
        if self.text.get(self.offset) == Some(&expected) {
            self.offset += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, expected: u8) -> Result<(), ClientControlError> {
        self.consume(expected)
            .then_some(())
            .ok_or(ClientControlError::InvalidJson)
    }

    fn parse_u64_number(&mut self) -> Result<u64, ClientControlError> {
        let start = self.offset;
        while self.text.get(self.offset).is_some_and(u8::is_ascii_digit) {
            self.offset += 1;
        }
        let digits = self
            .text
            .get(start..self.offset)
            .ok_or(ClientControlError::InvalidJson)?;
        if digits.is_empty() || (digits.len() > 1 && digits[0] == b'0') {
            return Err(ClientControlError::InvalidJson);
        }
        std::str::from_utf8(digits)
            .map_err(|_| ClientControlError::InvalidJson)?
            .parse()
            .map_err(|_| ClientControlError::InvalidJson)
    }

    fn parse_string(&mut self) -> Result<String, ClientControlError> {
        self.expect(b'"')?;
        let mut output = Vec::new();
        loop {
            let byte = *self
                .text
                .get(self.offset)
                .ok_or(ClientControlError::InvalidJson)?;
            self.offset += 1;
            match byte {
                b'"' => {
                    return String::from_utf8(output).map_err(|_| ClientControlError::InvalidJson);
                }
                b'\\' => {
                    let escaped = *self
                        .text
                        .get(self.offset)
                        .ok_or(ClientControlError::InvalidJson)?;
                    self.offset += 1;
                    match escaped {
                        b'"' | b'\\' | b'/' => output.push(escaped),
                        b'b' => output.push(0x08),
                        b'f' => output.push(0x0c),
                        b'n' => output.push(b'\n'),
                        b'r' => output.push(b'\r'),
                        b't' => output.push(b'\t'),
                        b'u' => {
                            let first = self.parse_hex_quad()?;
                            let scalar = if (0xd800..=0xdbff).contains(&first) {
                                self.expect(b'\\')?;
                                self.expect(b'u')?;
                                let second = self.parse_hex_quad()?;
                                if !(0xdc00..=0xdfff).contains(&second) {
                                    return Err(ClientControlError::InvalidJson);
                                }
                                0x1_0000
                                    + (((first as u32 - 0xd800) << 10) | (second as u32 - 0xdc00))
                            } else if (0xdc00..=0xdfff).contains(&first) {
                                return Err(ClientControlError::InvalidJson);
                            } else {
                                first as u32
                            };
                            let character =
                                char::from_u32(scalar).ok_or(ClientControlError::InvalidJson)?;
                            let mut encoded = [0; 4];
                            output
                                .extend_from_slice(character.encode_utf8(&mut encoded).as_bytes());
                        }
                        _ => return Err(ClientControlError::InvalidJson),
                    }
                }
                0x00..=0x1f => return Err(ClientControlError::InvalidJson),
                other => output.push(other),
            }
        }
    }

    fn parse_hex_quad(&mut self) -> Result<u16, ClientControlError> {
        let end = self
            .offset
            .checked_add(4)
            .ok_or(ClientControlError::InvalidJson)?;
        let digits = self
            .text
            .get(self.offset..end)
            .ok_or(ClientControlError::InvalidJson)?;
        let mut value = 0u16;
        for digit in digits {
            value = value
                .checked_mul(16)
                .and_then(|value| value.checked_add(hex_nibble(*digit)? as u16))
                .ok_or(ClientControlError::InvalidJson)?;
        }
        self.offset = end;
        Ok(value)
    }
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn take_required(
    fields: &mut HashMap<String, String>,
    key: &str,
) -> Result<String, ClientControlError> {
    fields.remove(key).ok_or(ClientControlError::MissingKey)
}

fn reject_unknown_fields(fields: HashMap<String, String>) -> Result<(), ClientControlError> {
    if fields.is_empty() {
        Ok(())
    } else {
        Err(ClientControlError::UnknownKey)
    }
}

fn reject_unknown_scalars(fields: HashMap<String, JsonScalar>) -> Result<(), ClientControlError> {
    if fields.is_empty() {
        Ok(())
    } else {
        Err(ClientControlError::UnknownKey)
    }
}

fn decode_lower_hex_id(text: &str) -> Result<[u8; 16], ClientControlError> {
    if text.len() != 32 {
        return Err(ClientControlError::InvalidTokenId);
    }
    let mut id = [0; 16];
    for (index, pair) in text.as_bytes().chunks_exact(2).enumerate() {
        let high = lower_hex_nibble(pair[0]).ok_or(ClientControlError::InvalidTokenId)?;
        let low = lower_hex_nibble(pair[1]).ok_or(ClientControlError::InvalidTokenId)?;
        id[index] = (high << 4) | low;
    }
    if id == [0; 16] {
        return Err(ClientControlError::InvalidTokenId);
    }
    Ok(id)
}

fn lower_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn decode_canonical_base64(text: &str) -> Result<Zeroizing<Vec<u8>>, ClientControlError> {
    let input = text.as_bytes();
    if input.is_empty() || !input.len().is_multiple_of(4) {
        return Err(ClientControlError::InvalidTokenSecret);
    }
    let mut output = Zeroizing::new(Vec::new());
    output
        .try_reserve_exact(input.len() / 4 * 3)
        .map_err(|_| ClientControlError::InvalidTokenSecret)?;
    for (index, chunk) in input.chunks_exact(4).enumerate() {
        let last = index + 1 == input.len() / 4;
        let a = base64_value(chunk[0]).ok_or(ClientControlError::InvalidTokenSecret)?;
        let b = base64_value(chunk[1]).ok_or(ClientControlError::InvalidTokenSecret)?;
        output.push((a << 2) | (b >> 4));
        match (chunk[2], chunk[3]) {
            (b'=', b'=') if last && b & 0x0f == 0 => (),
            (c, b'=') if last => {
                let c = base64_value(c).ok_or(ClientControlError::InvalidTokenSecret)?;
                if c & 0x03 != 0 {
                    return Err(ClientControlError::InvalidTokenSecret);
                }
                output.push((b << 4) | (c >> 2));
            }
            (c, d) if c != b'=' && d != b'=' => {
                let c = base64_value(c).ok_or(ClientControlError::InvalidTokenSecret)?;
                let d = base64_value(d).ok_or(ClientControlError::InvalidTokenSecret)?;
                output.push((b << 4) | (c >> 2));
                output.push((c << 6) | d);
            }
            _ => return Err(ClientControlError::InvalidTokenSecret),
        }
    }
    if output.len() != TOKEN_SECRET_BYTES {
        return Err(ClientControlError::InvalidTokenSecret);
    }
    Ok(output)
}

fn base64_value(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
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
    request_ids: ClientRequestIds,
}

impl ClientConnectionSession {
    /// Process the protocol handshake and PING requests. The TCP adapter must
    /// decode bounded frames first and close the connection on framing errors.
    pub fn handle_handshake_request(
        &mut self,
        storage: &StorageManager<'_>,
        request: ClientFrame,
    ) -> Result<ClientFrame, ClientSessionError> {
        if request.response_sequence != 0 {
            return Err(ClientSessionError::UnsupportedRequest);
        }
        self.request_ids
            .accept(&request)
            .map_err(|_| ClientSessionError::UnsupportedRequest)?;
        if !self.permits_request_kind(request.kind) {
            return Err(if self.hello.is_none() || request.kind == 1 {
                ClientSessionError::UnsupportedRequest
            } else if self.authenticated.is_none() {
                ClientSessionError::AuthenticationRequired
            } else {
                ClientSessionError::WrongMode
            });
        }
        let payload = match request.kind {
            1 => {
                let hello = decode_client_hello(&request.payload)
                    .map_err(ClientSessionError::InvalidControlPayload)?;
                let mode_text = match hello.mode {
                    ClientMode::Query => "query",
                    ClientMode::Control => "control",
                };
                self.accept_hello(hello.mode, &hello.client_name)?;
                format!(
                    "{{\"major\":1,\"minor\":0,\"mode\":\"{mode_text}\",\"limits\":{{\"payload_bytes\":{CLIENT_MAX_PAYLOAD_BYTES},\"control_bytes\":{CLIENT_MAX_CONTROL_BYTES},\"chunk_bytes\":{CLIENT_MAX_CHUNK_BYTES}}},\"transport\":\"plaintext_tcp\"}}"
                )
                .into_bytes()
            }
            2 => {
                let credentials = decode_client_credentials(&request.payload)
                    .map_err(ClientSessionError::InvalidControlPayload)?;
                let session =
                    self.authenticate(storage, credentials.token_id, &credentials.token_secret)?;
                let session_id = encode_lower_hex_id(session.connection_id());
                format!("{{\"session_id\":\"{session_id}\",\"authenticated\":true}}").into_bytes()
            }
            11 if request.payload == b"{}" => b"{\"alive\":true}".to_vec(),
            _ => return Err(ClientSessionError::UnsupportedRequest),
        };
        Ok(ClientFrame {
            kind: 100,
            request_id: request.request_id,
            response_sequence: 0,
            payload,
        })
    }

    /// Route one artifact/parameter chunk frame to the staging store using
    /// this connection's storage-authenticated owner, and acknowledge only
    /// the contiguous next offset.
    pub fn handle_upload_request(
        &mut self,
        staging: &crate::supervisor::StagingStore,
        request: ClientFrame,
    ) -> Result<ClientFrame, ClientSessionError> {
        if request.response_sequence != 0 || !matches!(request.kind, 4 | 5) {
            return Err(ClientSessionError::UnsupportedRequest);
        }
        self.request_ids
            .accept(&request)
            .map_err(|_| ClientSessionError::UnsupportedRequest)?;
        if !self.permits_request_kind(request.kind) {
            return Err(if self.authenticated.is_none() {
                ClientSessionError::AuthenticationRequired
            } else {
                ClientSessionError::WrongMode
            });
        }
        let chunk = decode_client_chunk(&request.payload)
            .map_err(ClientSessionError::InvalidControlPayload)?;
        let kind = if request.kind == 4 {
            crate::supervisor::ChunkKind::Artifact
        } else {
            crate::supervisor::ChunkKind::Parameters
        };
        let next_offset = staging
            .upload_chunk(
                self.staging_owner()?,
                chunk.invocation_id,
                kind,
                chunk.offset,
                &chunk.data,
            )
            .map_err(ClientSessionError::Staging)?;
        Ok(ClientFrame {
            kind: 100,
            request_id: request.request_id,
            response_sequence: 0,
            payload: format!("{{\"next_offset\":\"{next_offset}\"}}").into_bytes(),
        })
    }

    /// Reserve bounded supervisor staging from a fully validated PREPARE
    /// request. No storage snapshot or executor is created by this operation.
    pub fn handle_prepare_request(
        &mut self,
        staging: &crate::supervisor::StagingStore,
        request: ClientFrame,
    ) -> Result<ClientFrame, ClientSessionError> {
        if request.response_sequence != 0 || request.kind != 3 {
            return Err(ClientSessionError::UnsupportedRequest);
        }
        self.request_ids
            .accept(&request)
            .map_err(|_| ClientSessionError::UnsupportedRequest)?;
        if !self.permits_request_kind(request.kind) {
            return Err(if self.authenticated.is_none() {
                ClientSessionError::AuthenticationRequired
            } else {
                ClientSessionError::WrongMode
            });
        }
        let prepare = decode_client_prepare(&request.payload)
            .map_err(ClientSessionError::InvalidControlPayload)?;
        let reservation = staging
            .prepare(
                self.staging_owner()?,
                crate::supervisor::PrepareSubmission {
                    manifest_version: prepare.manifest_version,
                    manifest: prepare.manifest,
                    artifact_bytes: prepare.artifact_bytes,
                    artifact_sha256: prepare.artifact_sha256,
                    scope_version: prepare.scope_version,
                    requested_scope: prepare.requested_scope,
                    parameter_bytes: prepare.parameter_bytes,
                    parameter_sha256: prepare.parameter_sha256,
                    deadline_ms: prepare.deadline_ms,
                    workload_class: prepare.workload_class,
                },
            )
            .map_err(ClientSessionError::Staging)?;
        let invocation_id = encode_lower_hex_id(reservation.invocation_id);
        Ok(ClientFrame {
            kind: 100,
            request_id: request.request_id,
            response_sequence: 0,
            payload: format!(
                "{{\"invocation_id\":\"{invocation_id}\",\"artifact_chunk_bytes\":{CLIENT_MAX_CHUNK_BYTES},\"parameter_chunk_bytes\":{CLIENT_MAX_CHUNK_BYTES}}}"
            )
            .into_bytes(),
        })
    }

    /// Verify uploaded lengths, hashes, and versioned admission metadata,
    /// then make the staged submission immutable.
    pub fn handle_seal_request(
        &mut self,
        staging: &crate::supervisor::StagingStore,
        request: ClientFrame,
    ) -> Result<ClientFrame, ClientSessionError> {
        if request.response_sequence != 0 || request.kind != 6 {
            return Err(ClientSessionError::UnsupportedRequest);
        }
        self.request_ids
            .accept(&request)
            .map_err(|_| ClientSessionError::UnsupportedRequest)?;
        if !self.permits_request_kind(request.kind) {
            return Err(if self.authenticated.is_none() {
                ClientSessionError::AuthenticationRequired
            } else {
                ClientSessionError::WrongMode
            });
        }
        let invocation_id = decode_client_seal(&request.payload)
            .map_err(ClientSessionError::InvalidControlPayload)?;
        staging
            .seal(self.staging_owner()?, invocation_id)
            .map_err(ClientSessionError::Staging)?;
        Ok(ClientFrame {
            kind: 100,
            request_id: request.request_id,
            response_sequence: 0,
            payload: b"{\"state\":\"prepared\"}".to_vec(),
        })
    }

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

    pub fn accept_hello_payload(&mut self, payload: &[u8]) -> Result<(), ClientSessionError> {
        let hello =
            decode_client_hello(payload).map_err(ClientSessionError::InvalidControlPayload)?;
        self.accept_hello(hello.mode, &hello.client_name)
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

    pub fn authenticate_payload(
        &mut self,
        storage: &StorageManager<'_>,
        payload: &[u8],
    ) -> Result<&AuthenticatedQuerySession, ClientSessionError> {
        let credentials = decode_client_credentials(payload)
            .map_err(ClientSessionError::InvalidControlPayload)?;
        self.authenticate(storage, credentials.token_id, &credentials.token_secret)
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

    /// Revalidate authority and open the storage invocation only after a
    /// sealed reservation wins the one-shot START transition.
    pub fn start_submission<'db>(
        &mut self,
        storage: &StorageManager<'db>,
        staging: &crate::supervisor::StagingStore,
        invocation_id: [u8; 16],
        retention_deadline_us: i64,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<crate::storage_manager::SupervisorInvocation<'db>, ClientStartError> {
        let owner = self.staging_owner().map_err(ClientStartError::Session)?;
        let authenticated = self
            .authenticated
            .as_ref()
            .ok_or(ClientStartError::Session(
                ClientSessionError::AuthenticationRequired,
            ))?;
        let token_id = authenticated.token_id();
        let mut token_secret = *self.token_secret.as_ref().ok_or(ClientStartError::Session(
            ClientSessionError::AuthenticationRequired,
        ))?;
        let result = staging.start_with(owner, invocation_id, |submission| {
            let duration = std::time::Duration::from_millis(submission.metadata().deadline_ms);
            let deadline = std::time::Instant::now()
                .checked_add(duration)
                .ok_or(TransactionError::InvalidContext)?;
            storage.admit_supervisor_invocation_with_deadline(
                crate::storage_manager::SupervisorAdmissionRequest {
                    invocation_id: submission.invocation_id(),
                    token_id,
                    retention_deadline_us,
                    max_rows,
                    max_bytes,
                    deadline: Some(deadline),
                },
                &token_secret,
            )
        });
        token_secret.zeroize();
        result.map_err(ClientStartError::Start)
    }

    /// Enforce the v1 mode and pre-authentication operation allowlists.
    pub fn permits_request_kind(&self, kind: u16) -> bool {
        let Some(mode) = self.hello else {
            return kind == 1;
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

fn encode_lower_hex_id(id: [u8; 16]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(32);
    for byte in id {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
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
