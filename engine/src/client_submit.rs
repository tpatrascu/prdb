//! Authenticated TCP upload client for prepared query packages.

use crate::client_protocol::{
    CLIENT_ERROR_KIND, CLIENT_MAX_CHUNK_BYTES, CLIENT_MAX_CONTROL_BYTES, CLIENT_MAX_PAYLOAD_BYTES,
    ClientFrame, ClientFrameDecoder, JsonStringObjectParser,
};
use crate::credentials::TOKEN_SECRET_BYTES;
use crate::submission_manifest::decode_manifest_v1;
use crate::submission_scope::decode_requested_scope_v1;
use crate::supervisor::{
    MAX_ARTIFACT_BYTES, MAX_MANIFEST_BYTES, MAX_PARAMETER_BYTES, MAX_SCOPE_BYTES, WorkloadClass,
};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::time::Duration;
use zeroize::{Zeroize, Zeroizing};

const MAX_PACKAGE_ARCHIVE_BYTES: u64 = MAX_ARTIFACT_BYTES;
const CLIENT_NAME: &str = "prdb-rust-sdk";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub enum ClientSubmitError {
    Io(io::Error),
    Frame(crate::client_protocol::ClientFrameError),
    InvalidCredentials,
    InvalidPackage,
    InvalidResponse,
    ServerRejected(String),
    OutcomeUncertain([u8; 16]),
}

impl std::fmt::Display for ClientSubmitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "client submission failed: {self:?}")
    }
}

impl std::error::Error for ClientSubmitError {}

impl From<io::Error> for ClientSubmitError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<crate::client_protocol::ClientFrameError> for ClientSubmitError {
    fn from(error: crate::client_protocol::ClientFrameError) -> Self {
        Self::Frame(error)
    }
}

/// One authenticated query-mode TCP connection for upload, execution, and status.
pub struct SubmissionClient {
    stream: TcpStream,
    decoder: ClientFrameDecoder,
    pending_frames: VecDeque<ClientFrame>,
    started_invocations: HashSet<[u8; 16]>,
    next_request_id: u64,
    max_payload_bytes: usize,
    max_control_bytes: usize,
    max_chunk_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvocationStatus {
    Prepared,
    Active,
    Committing,
    Committed,
    Aborted,
    Unknown,
    Expired,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TerminalOutcome {
    Committed,
    Aborted,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvocationTerminal {
    pub invocation_id: [u8; 16],
    pub outcome: TerminalOutcome,
    pub reason_code: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientResultEvent {
    Data(Vec<u8>),
    Terminal(InvocationTerminal),
}

pub struct StartedInvocation<'a> {
    client: &'a mut SubmissionClient,
    request_id: u64,
    next_sequence: u64,
    invocation_id: [u8; 16],
    terminal: bool,
}

struct PackageBytes {
    artifact: Vec<u8>,
    manifest: Vec<u8>,
    scope: Vec<u8>,
}

impl SubmissionClient {
    /// Connect, negotiate protocol v1.0, and authenticate. The secret buffer is
    /// zeroized before return, including authentication failures.
    pub fn connect_authenticated(
        address: SocketAddr,
        token_id: [u8; 16],
        token_secret: &mut [u8],
    ) -> Result<Self, ClientSubmitError> {
        Self::connect_authenticated_with_timeout(address, token_id, token_secret, DEFAULT_TIMEOUT)
    }

    pub fn connect_authenticated_with_timeout(
        address: SocketAddr,
        token_id: [u8; 16],
        token_secret: &mut [u8],
        timeout: Duration,
    ) -> Result<Self, ClientSubmitError> {
        let secret = Zeroizing::new(token_secret.to_vec());
        token_secret.zeroize();
        if secret.len() != TOKEN_SECRET_BYTES || timeout.is_zero() {
            return Err(ClientSubmitError::InvalidCredentials);
        }
        let stream = TcpStream::connect_timeout(&address, timeout)?;
        stream.set_read_timeout(Some(timeout))?;
        stream.set_write_timeout(Some(timeout))?;
        let mut client = Self {
            stream,
            decoder: ClientFrameDecoder::default(),
            pending_frames: VecDeque::new(),
            started_invocations: HashSet::new(),
            next_request_id: 1,
            max_payload_bytes: CLIENT_MAX_PAYLOAD_BYTES,
            max_control_bytes: CLIENT_MAX_CONTROL_BYTES,
            max_chunk_bytes: CLIENT_MAX_CHUNK_BYTES,
        };

        let hello = format!("{{\"mode\":\"query\",\"client_name\":\"{CLIENT_NAME}\"}}");
        let response = client.exchange(1, hello.into_bytes())?;
        let hello_fields = parse_flat_response(&response.payload)?;
        let limits = hello_fields
            .get("limits")
            .and_then(ResponseValue::as_object)
            .ok_or(ClientSubmitError::InvalidResponse)?;
        let max_payload_bytes = hello_fields
            .get("limits")
            .and_then(ResponseValue::as_object)
            .and_then(|limits| limits.get("payload_bytes"))
            .and_then(ResponseValue::as_number)
            .and_then(|bytes| usize::try_from(bytes).ok());
        let max_control_bytes = limits
            .get("control_bytes")
            .and_then(ResponseValue::as_number)
            .and_then(|bytes| usize::try_from(bytes).ok());
        let max_chunk_bytes = limits
            .get("chunk_bytes")
            .and_then(ResponseValue::as_number)
            .and_then(|bytes| usize::try_from(bytes).ok());
        if hello_fields.len() != 5
            || hello_fields.get("major") != Some(&ResponseValue::Number(1))
            || hello_fields.get("minor") != Some(&ResponseValue::Number(0))
            || take_string(&hello_fields, "mode").as_deref() != Some("query")
            || take_string(&hello_fields, "transport").as_deref() != Some("plaintext_tcp")
            || limits.len() != 3
            || max_payload_bytes.is_none_or(|bytes| bytes == 0 || bytes > CLIENT_MAX_PAYLOAD_BYTES)
            || max_control_bytes.is_none_or(|bytes| bytes == 0 || bytes > CLIENT_MAX_CONTROL_BYTES)
            || max_chunk_bytes.is_none_or(|bytes| bytes == 0 || bytes > CLIENT_MAX_CHUNK_BYTES)
            || max_payload_bytes.unwrap() < max_control_bytes.unwrap()
            || max_payload_bytes.unwrap() < max_chunk_bytes.unwrap().saturating_add(24)
        {
            return Err(ClientSubmitError::InvalidResponse);
        }
        client.max_payload_bytes = max_payload_bytes.unwrap();
        client.max_control_bytes = max_control_bytes.unwrap();
        client.max_chunk_bytes = max_chunk_bytes.unwrap();

        let mut token_secret_b64 = Zeroizing::new(encode_base64(&secret));
        let token_id_hex = lower_hex(&token_id);
        let mut auth = Zeroizing::new(
            format!(
                "{{\"token_id\":\"{token_id_hex}\",\"token_secret\":\"{}\"}}",
                token_secret_b64.as_str()
            )
            .into_bytes(),
        );
        token_secret_b64.zeroize();
        let response = client.exchange(2, std::mem::take(&mut *auth))?;
        let fields = parse_flat_response(&response.payload)?;
        if fields.len() != 2
            || take_string(&fields, "session_id").is_none_or(|value| !decode_lower_hex(&value, 16))
            || fields.get("authenticated") != Some(&ResponseValue::Boolean(true))
        {
            return Err(ClientSubmitError::InvalidResponse);
        }
        Ok(client)
    }

    /// Upload a private package directory and parameter bytes, then seal it.
    /// The returned ID is reserved but has not started execution.
    pub fn upload_and_seal(
        &mut self,
        package_dir: &std::path::Path,
        parameters: &[u8],
        deadline_ms: u64,
        workload_class: WorkloadClass,
    ) -> Result<[u8; 16], ClientSubmitError> {
        if deadline_ms == 0 || parameters.len() as u64 > MAX_PARAMETER_BYTES {
            return Err(ClientSubmitError::InvalidPackage);
        }
        let package = read_package(package_dir)?;
        let artifact_hash: [u8; 32] = Sha256::digest(&package.artifact).into();
        let parameter_hash: [u8; 32] = Sha256::digest(parameters).into();
        let workload = match workload_class {
            WorkloadClass::Operational => "operational",
            WorkloadClass::Analytical => "analytical",
        };
        let prepare = format!(
            "{{\"manifest_version\":1,\"manifest\":\"{}\",\"artifact_bytes\":\"{}\",\"artifact_sha256\":\"{}\",\"parameter_bytes\":\"{}\",\"parameter_sha256\":\"{}\",\"scope_version\":1,\"requested_scope\":\"{}\",\"deadline_ms\":\"{deadline_ms}\",\"workload_class\":\"{workload}\"}}",
            encode_base64(&package.manifest),
            package.artifact.len(),
            lower_hex(&artifact_hash),
            parameters.len(),
            lower_hex(&parameter_hash),
            encode_base64(&package.scope),
        );
        let response = self.exchange(3, prepare.into_bytes())?;
        let fields = parse_flat_response(&response.payload)?;
        let invocation_id = fields
            .get("invocation_id")
            .and_then(ResponseValue::as_string)
            .and_then(decode_hex_array::<16>)
            .ok_or(ClientSubmitError::InvalidResponse)?;
        let artifact_chunk_bytes = fields
            .get("artifact_chunk_bytes")
            .and_then(ResponseValue::as_number)
            .filter(|bytes| (1..=self.max_chunk_bytes as u64).contains(bytes))
            .ok_or(ClientSubmitError::InvalidResponse)? as usize;
        let parameter_chunk_bytes = fields
            .get("parameter_chunk_bytes")
            .and_then(ResponseValue::as_number)
            .filter(|bytes| (1..=self.max_chunk_bytes as u64).contains(bytes))
            .ok_or(ClientSubmitError::InvalidResponse)?
            as usize;
        if fields.len() != 3 {
            return Err(ClientSubmitError::InvalidResponse);
        }

        self.upload_chunks(4, invocation_id, &package.artifact, artifact_chunk_bytes)?;
        self.upload_chunks(5, invocation_id, parameters, parameter_chunk_bytes)?;
        let seal_payload = format!("{{\"invocation_id\":\"{}\"}}", lower_hex(&invocation_id));
        let sealed = self.exchange(6, seal_payload.into_bytes())?;
        let fields = parse_flat_response(&sealed.payload)?;
        if fields.len() != 1 || take_string(&fields, "state").as_deref() != Some("prepared") {
            return Err(ClientSubmitError::InvalidResponse);
        }
        Ok(invocation_id)
    }

    /// Start a sealed reservation once. If the request may have reached the
    /// gateway but its reply is lost or invalid, callers must reconcile the ID
    /// with `status` over a newly authenticated connection. START is never retried.
    pub fn start(
        &mut self,
        invocation_id: [u8; 16],
        receipt_path: &std::path::Path,
    ) -> Result<StartedInvocation<'_>, ClientSubmitError> {
        if self.started_invocations.contains(&invocation_id) {
            return Err(ClientSubmitError::InvalidResponse);
        }
        write_start_receipt(receipt_path, invocation_id)?;
        self.started_invocations.insert(invocation_id);
        let request_id = self.next_request_id;
        let payload = format!("{{\"invocation_id\":\"{}\"}}", lower_hex(&invocation_id));
        let response = match self.exchange(7, payload.into_bytes()) {
            Ok(response) => response,
            Err(error @ ClientSubmitError::ServerRejected(_)) => return Err(error),
            Err(_) => return Err(ClientSubmitError::OutcomeUncertain(invocation_id)),
        };
        let fields = parse_flat_response(&response.payload)
            .map_err(|_| ClientSubmitError::OutcomeUncertain(invocation_id))?;
        if fields.len() != 1 || take_string(&fields, "state").as_deref() != Some("accepted") {
            return Err(ClientSubmitError::OutcomeUncertain(invocation_id));
        }
        Ok(StartedInvocation {
            client: self,
            request_id,
            next_sequence: 1,
            invocation_id,
            terminal: false,
        })
    }

    /// Query the authenticated owner's current or retained invocation status.
    /// Use a newly authenticated client after an uncertain START response.
    pub fn status(
        &mut self,
        invocation_id: [u8; 16],
    ) -> Result<InvocationStatus, ClientSubmitError> {
        let payload = format!("{{\"invocation_id\":\"{}\"}}", lower_hex(&invocation_id));
        let response = self.exchange(8, payload.into_bytes())?;
        let fields = parse_flat_response(&response.payload)?;
        if fields.len() != 1 {
            return Err(ClientSubmitError::InvalidResponse);
        }
        match take_string(&fields, "state").as_deref() {
            Some("prepared") => Ok(InvocationStatus::Prepared),
            Some("active") => Ok(InvocationStatus::Active),
            Some("committing") => Ok(InvocationStatus::Committing),
            Some("committed") => Ok(InvocationStatus::Committed),
            Some("aborted") => Ok(InvocationStatus::Aborted),
            Some("unknown") => Ok(InvocationStatus::Unknown),
            Some("expired") => Ok(InvocationStatus::Expired),
            _ => Err(ClientSubmitError::InvalidResponse),
        }
    }

    fn upload_chunks(
        &mut self,
        kind: u16,
        invocation_id: [u8; 16],
        bytes: &[u8],
        chunk_bytes: usize,
    ) -> Result<(), ClientSubmitError> {
        let mut offset = 0usize;
        for chunk in bytes.chunks(chunk_bytes) {
            let mut payload = Vec::with_capacity(24 + chunk.len());
            payload.extend_from_slice(&invocation_id);
            payload.extend_from_slice(&(offset as u64).to_le_bytes());
            payload.extend_from_slice(chunk);
            let response = self.exchange(kind, payload)?;
            let fields = parse_flat_response(&response.payload)?;
            let expected_next = offset + chunk.len();
            let next_offset = fields
                .get("next_offset")
                .and_then(ResponseValue::as_decimal_string)
                .and_then(|value| value.parse::<usize>().ok());
            if fields.len() != 1 || next_offset != Some(expected_next) {
                return Err(ClientSubmitError::InvalidResponse);
            }
            offset = expected_next;
        }
        Ok(())
    }

    fn exchange(
        &mut self,
        kind: u16,
        mut payload: Vec<u8>,
    ) -> Result<ClientFrame, ClientSubmitError> {
        let request_id = self.next_request_id;
        self.next_request_id = request_id
            .checked_add(1)
            .ok_or(ClientSubmitError::InvalidResponse)?;
        let mut request = ClientFrame {
            kind,
            request_id,
            response_sequence: 0,
            payload: std::mem::take(&mut payload),
        };
        let request_limit = if matches!(kind, 4 | 5) {
            self.max_chunk_bytes.saturating_add(24)
        } else {
            self.max_control_bytes
        };
        if request.payload.len() > request_limit || request.payload.len() > self.max_payload_bytes {
            request.payload.zeroize();
            return Err(ClientSubmitError::InvalidResponse);
        }
        let mut encoded = match request.encode() {
            Ok(encoded) => encoded,
            Err(error) => {
                request.payload.zeroize();
                return Err(ClientSubmitError::Frame(error));
            }
        };
        let write_result = self.stream.write_all(&encoded);
        encoded.zeroize();
        // Both buffers may contain the bearer secret during AUTH.
        request.payload.zeroize();
        if let Err(error) = write_result {
            return Err(ClientSubmitError::Io(error));
        }
        let response = self.receive_frame()?;
        if response.request_id != request_id || response.response_sequence != 0 {
            return Err(ClientSubmitError::InvalidResponse);
        }
        if kind != 7 && !self.pending_frames.is_empty() {
            return Err(ClientSubmitError::InvalidResponse);
        }
        if response.kind == CLIENT_ERROR_KIND {
            return Err(ClientSubmitError::ServerRejected(
                String::from_utf8_lossy(&response.payload)
                    .chars()
                    .take(512)
                    .collect(),
            ));
        }
        if response.kind != 100 {
            return Err(ClientSubmitError::InvalidResponse);
        }
        Ok(response)
    }

    fn receive_frame(&mut self) -> Result<ClientFrame, ClientSubmitError> {
        if let Some(frame) = self.pending_frames.pop_front() {
            return Ok(frame);
        }
        let mut input = [0u8; 8192];
        loop {
            let count = self.stream.read(&mut input)?;
            if count == 0 {
                self.decoder.finish()?;
                return Err(ClientSubmitError::InvalidResponse);
            }
            let frames = self.decoder.push(&input[..count])?;
            if !frames.is_empty() {
                self.pending_frames.extend(frames);
                return self
                    .pending_frames
                    .pop_front()
                    .ok_or(ClientSubmitError::InvalidResponse);
            }
        }
    }
}

impl StartedInvocation<'_> {
    /// Read one provisional data frame or a supervisor terminal envelope.
    /// Data is successful output only if a committed terminal event follows.
    pub fn next_event(&mut self) -> Result<Option<ClientResultEvent>, ClientSubmitError> {
        if self.terminal {
            return Ok(None);
        }
        let frame = self
            .client
            .receive_frame()
            .map_err(|_| ClientSubmitError::OutcomeUncertain(self.invocation_id))?;
        if frame.request_id != self.request_id || frame.response_sequence != self.next_sequence {
            return Err(ClientSubmitError::OutcomeUncertain(self.invocation_id));
        }
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(ClientSubmitError::OutcomeUncertain(self.invocation_id))?;
        match frame.kind {
            102 => Ok(Some(ClientResultEvent::Data(frame.payload))),
            103 => {
                let fields = parse_flat_response(&frame.payload)
                    .map_err(|_| ClientSubmitError::OutcomeUncertain(self.invocation_id))?;
                if fields.len() != 3 {
                    return Err(ClientSubmitError::OutcomeUncertain(self.invocation_id));
                }
                let response_id = fields
                    .get("invocation_id")
                    .and_then(ResponseValue::as_string)
                    .and_then(decode_hex_array::<16>);
                let outcome = match take_string(&fields, "outcome").as_deref() {
                    Some("committed") => TerminalOutcome::Committed,
                    Some("aborted") => TerminalOutcome::Aborted,
                    Some("unknown") => TerminalOutcome::Unknown,
                    _ => return Err(ClientSubmitError::OutcomeUncertain(self.invocation_id)),
                };
                let reason_code = take_string(&fields, "reason_code")
                    .ok_or(ClientSubmitError::OutcomeUncertain(self.invocation_id))?;
                if response_id != Some(self.invocation_id) || reason_code.len() > 64 {
                    return Err(ClientSubmitError::OutcomeUncertain(self.invocation_id));
                }
                self.terminal = true;
                Ok(Some(ClientResultEvent::Terminal(InvocationTerminal {
                    invocation_id: self.invocation_id,
                    outcome,
                    reason_code,
                })))
            }
            _ => Err(ClientSubmitError::OutcomeUncertain(self.invocation_id)),
        }
    }
}

fn read_package(package_dir: &std::path::Path) -> Result<PackageBytes, ClientSubmitError> {
    let metadata = fs::symlink_metadata(package_dir)?;
    if !metadata.is_dir() || metadata.permissions().mode() & 0o077 != 0 {
        return Err(ClientSubmitError::InvalidPackage);
    }
    let artifact = read_private_file(&package_dir.join("query.so"), MAX_PACKAGE_ARCHIVE_BYTES)?;
    let manifest = read_private_file(
        &package_dir.join("manifest.pmf1"),
        MAX_MANIFEST_BYTES as u64,
    )?;
    let scope = read_private_file(
        &package_dir.join("requested_scope.psc1"),
        MAX_SCOPE_BYTES as u64,
    )?;
    let expected_hash = read_private_file(&package_dir.join("artifact.sha256"), 65)?;
    let calculated_hash: [u8; 32] = Sha256::digest(&artifact).into();
    if artifact.is_empty()
        || decode_manifest_v1(&manifest).is_err()
        || decode_requested_scope_v1(&scope).is_err()
        || expected_hash.len() != 65
        || expected_hash[64] != b'\n'
        || !decode_lower_hex(std::str::from_utf8(&expected_hash[..64]).unwrap_or(""), 32)
        || decode_hex_array::<32>(std::str::from_utf8(&expected_hash[..64]).unwrap_or(""))
            != Some(calculated_hash)
    {
        return Err(ClientSubmitError::InvalidPackage);
    }
    Ok(PackageBytes {
        artifact,
        manifest,
        scope,
    })
}

fn read_private_file(path: &std::path::Path, maximum: u64) -> Result<Vec<u8>, ClientSubmitError> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o077 != 0 || metadata.len() > maximum
    {
        return Err(ClientSubmitError::InvalidPackage);
    }
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    let file = options.open(path)?;
    let opened_metadata = file.metadata()?;
    if !opened_metadata.is_file()
        || opened_metadata.permissions().mode() & 0o077 != 0
        || opened_metadata.len() > maximum
    {
        return Err(ClientSubmitError::InvalidPackage);
    }
    let mut bytes = Vec::with_capacity(opened_metadata.len() as usize);
    file.take(maximum.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != opened_metadata.len() || bytes.len() as u64 > maximum {
        return Err(ClientSubmitError::InvalidPackage);
    }
    Ok(bytes)
}

fn write_start_receipt(
    receipt_path: &std::path::Path,
    invocation_id: [u8; 16],
) -> Result<(), ClientSubmitError> {
    let file_name = receipt_path
        .file_name()
        .ok_or(ClientSubmitError::InvalidPackage)?;
    let parent = receipt_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(std::path::Path::new("."));
    let parent = fs::canonicalize(parent)?;
    if !parent.is_dir() {
        return Err(ClientSubmitError::InvalidPackage);
    }
    let path = parent.join(file_name);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    file.write_all(format!("PDBR1 {}\n", lower_hex(&invocation_id)).as_bytes())?;
    file.sync_all()?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

#[derive(Debug, Eq, PartialEq)]
enum ResponseValue {
    String(String),
    Number(u64),
    Boolean(bool),
    Object(HashMap<String, ResponseValue>),
}

impl ResponseValue {
    fn as_string(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }

    fn as_number(&self) -> Option<u64> {
        match self {
            Self::Number(value) => Some(*value),
            _ => None,
        }
    }

    fn as_decimal_string(&self) -> Option<&str> {
        match self {
            Self::String(value)
                if !value.is_empty()
                    && (value.len() == 1 || !value.starts_with('0'))
                    && value.bytes().all(|byte| byte.is_ascii_digit()) =>
            {
                Some(value)
            }
            _ => None,
        }
    }

    fn as_object(&self) -> Option<&HashMap<String, ResponseValue>> {
        match self {
            Self::Object(value) => Some(value),
            _ => None,
        }
    }
}

fn parse_flat_response(
    payload: &[u8],
) -> Result<HashMap<String, ResponseValue>, ClientSubmitError> {
    if payload.len() > CLIENT_MAX_CONTROL_BYTES {
        return Err(ClientSubmitError::InvalidResponse);
    }
    let text = std::str::from_utf8(payload).map_err(|_| ClientSubmitError::InvalidResponse)?;
    let mut parser = JsonStringObjectParser {
        text: text.as_bytes(),
        offset: 0,
    };
    parser.skip_whitespace();
    let fields = parse_response_object(&mut parser, 0)?;
    parser.skip_whitespace();
    (parser.offset == parser.text.len())
        .then_some(fields)
        .ok_or(ClientSubmitError::InvalidResponse)
}

fn parse_response_object(
    parser: &mut JsonStringObjectParser<'_>,
    depth: usize,
) -> Result<HashMap<String, ResponseValue>, ClientSubmitError> {
    if depth > 16 {
        return Err(ClientSubmitError::InvalidResponse);
    }
    parser.skip_whitespace();
    parser
        .expect(b'{')
        .map_err(|_| ClientSubmitError::InvalidResponse)?;
    let mut fields = HashMap::new();
    parser.skip_whitespace();
    if parser.consume(b'}') {
        return Ok(fields);
    }
    loop {
        parser.skip_whitespace();
        let key = parser
            .parse_string()
            .map_err(|_| ClientSubmitError::InvalidResponse)?;
        parser.skip_whitespace();
        parser
            .expect(b':')
            .map_err(|_| ClientSubmitError::InvalidResponse)?;
        parser.skip_whitespace();
        let value = match parser.text.get(parser.offset) {
            Some(b'"') => ResponseValue::String(
                parser
                    .parse_string()
                    .map_err(|_| ClientSubmitError::InvalidResponse)?,
            ),
            Some(b'{') => ResponseValue::Object(parse_response_object(parser, depth + 1)?),
            Some(b't' | b'f') => ResponseValue::Boolean(
                parser
                    .parse_boolean()
                    .map_err(|_| ClientSubmitError::InvalidResponse)?,
            ),
            Some(byte) if byte.is_ascii_digit() => ResponseValue::Number(
                parser
                    .parse_u64_number()
                    .map_err(|_| ClientSubmitError::InvalidResponse)?,
            ),
            _ => return Err(ClientSubmitError::InvalidResponse),
        };
        if fields.insert(key, value).is_some() {
            return Err(ClientSubmitError::InvalidResponse);
        }
        parser.skip_whitespace();
        if parser.consume(b'}') {
            return Ok(fields);
        }
        parser
            .expect(b',')
            .map_err(|_| ClientSubmitError::InvalidResponse)?;
    }
}

fn take_string(fields: &HashMap<String, ResponseValue>, key: &str) -> Option<String> {
    fields.get(key)?.as_string().map(ToOwned::to_owned)
}

fn encode_base64(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        let second = *chunk.get(1).unwrap_or(&0);
        let third = *chunk.get(2).unwrap_or(&0);
        output.push(TABLE[(first >> 2) as usize] as char);
        output.push(TABLE[(((first & 3) << 4) | (second >> 4)) as usize] as char);
        output.push(if chunk.len() > 1 {
            TABLE[(((second & 15) << 2) | (third >> 6)) as usize] as char
        } else {
            '='
        });
        output.push(if chunk.len() > 2 {
            TABLE[(third & 63) as usize] as char
        } else {
            '='
        });
    }
    output
}

fn lower_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn decode_lower_hex(text: &str, expected_bytes: usize) -> bool {
    text.len() == expected_bytes * 2
        && text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn decode_hex_array<const N: usize>(text: &str) -> Option<[u8; N]> {
    if !decode_lower_hex(text, N) {
        return None;
    }
    let mut bytes = [0u8; N];
    for (index, pair) in text.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Some(bytes)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{ResponseValue, decode_hex_array, parse_flat_response};

    #[test]
    fn response_decoder_rejects_duplicate_unknown_json_shapes_and_noncanonical_numbers() {
        assert!(parse_flat_response(br#"{"state":"prepared"}"#).is_ok());
        assert!(parse_flat_response(br#"{"state":"prepared","state":"prepared"}"#).is_err());
        assert!(parse_flat_response(br#"{"nested":[]}"#).is_err());
        assert!(parse_flat_response(br#"{"nested":{"a":1,"a":2}}"#).is_err());
        assert!(parse_flat_response(br#"{"value":01}"#).is_err());
        assert_eq!(
            parse_flat_response(br#"{"value":"\u0061"}"#).unwrap()["value"],
            ResponseValue::String("a".to_owned())
        );
        assert!(parse_flat_response(br#"{"a":1,"\u0061":2}"#).is_err());
    }

    #[test]
    fn response_decoder_accepts_canonical_invocation_ids_and_typed_fields() {
        let fields = parse_flat_response(
            br#"{"invocation_id":"01010101010101010101010101010101","chunk":262144,"ok":true}"#,
        )
        .unwrap();
        assert_eq!(fields.get("ok"), Some(&ResponseValue::Boolean(true)));
        assert_eq!(fields.get("chunk"), Some(&ResponseValue::Number(262144)));
        assert_eq!(
            decode_hex_array::<16>(fields["invocation_id"].as_string().unwrap()),
            Some([1; 16])
        );
    }
}
