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
    InvalidAdministrativeRequest,
    InvalidMetadataPage,
    InvalidResponse,
    ServerRejected(String),
    OutcomeUncertain([u8; 16]),
    AdministrativeOutcomeUncertain([u8; 16]),
    AdministrativeOperationRejected {
        operation_id: [u8; 16],
        message: String,
    },
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

/// Authenticated control-mode client for permission-filtered metadata reads.
/// The same transport validates protocol limits and zeroizes AUTH payloads as
/// the query submission client.
pub struct MetadataClient {
    transport: SubmissionClient,
}

/// Authenticated control-mode client for typed administrative DDL operations.
/// It is separate from query packages and never exposes admin credentials to
/// native executor code.
pub struct AdminClient {
    transport: SubmissionClient,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminIndexResult {
    pub operation_id: [u8; 16],
    pub index_id: [u8; 16],
    pub commit_sequence: u64,
}

/// Result of creating a user or role through the control channel.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminIdentityResult {
    pub operation_id: [u8; 16],
    pub identity_id: [u8; 16],
    pub commit_sequence: u64,
}

/// Result of an administrative operation that does not create an identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminOperationResult {
    pub operation_id: [u8; 16],
    pub commit_sequence: u64,
}

/// One-time token material returned only by a successful token issuance.
/// The secret is zeroized when this value is dropped and is omitted from Debug.
pub struct AdminTokenResult {
    pub operation_id: [u8; 16],
    pub token_id: [u8; 16],
    token_secret: Zeroizing<[u8; TOKEN_SECRET_BYTES]>,
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminColumnDefinition {
    pub name: String,
    pub scalar_type: crate::tuple::ScalarType,
    pub nullable: bool,
    pub generated_uuid_identity: bool,
    pub default: Option<AdminColumnDefault>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminColumnDefault {
    Null,
    Literal(crate::tuple::ScalarValue),
    CurrentPrincipalId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminTableResult {
    pub operation_id: [u8; 16],
    pub table_id: [u8; 16],
    pub column_ids: Vec<[u8; 16]>,
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminTableColumnResult {
    pub operation_id: [u8; 16],
    pub table_id: [u8; 16],
    pub column_id: [u8; 16],
    pub commit_sequence: u64,
}

/// Database-scoped privileges that an administrator may grant or revoke.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminSystemPrivilege {
    ManageUsers,
    ManageRoles,
    CreateSchema,
}

impl AdminSystemPrivilege {
    fn as_wire_name(self) -> &'static str {
        match self {
            Self::ManageUsers => "manage_users",
            Self::ManageRoles => "manage_roles",
            Self::CreateSchema => "create_schema",
        }
    }
}

impl AdminTokenResult {
    /// Borrow the one-time bearer secret for secure storage or immediate use.
    pub fn token_secret(&self) -> &[u8; TOKEN_SECRET_BYTES] {
        &self.token_secret
    }
}

/// One bounded page from a permission-filtered metadata query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataPage<T> {
    pub records: Vec<T>,
    pub next_cursor: Option<String>,
}

pub const DEFAULT_METADATA_PAGE_SIZE: usize = 64;

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
        Self::connect_authenticated_mode(
            address,
            token_id,
            token_secret,
            timeout,
            crate::client_protocol::ClientMode::Query,
        )
    }

    fn connect_authenticated_mode(
        address: SocketAddr,
        token_id: [u8; 16],
        token_secret: &mut [u8],
        timeout: Duration,
        mode: crate::client_protocol::ClientMode,
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

        let mode_name = match mode {
            crate::client_protocol::ClientMode::Query => "query",
            crate::client_protocol::ClientMode::Control => "control",
        };
        let hello = format!("{{\"mode\":\"{mode_name}\",\"client_name\":\"{CLIENT_NAME}\"}}");
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
            || take_string(&hello_fields, "mode").as_deref() != Some(mode_name)
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
        let mut input = Zeroizing::new([0u8; 8192]);
        loop {
            let count = self.stream.read(&mut input[..])?;
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

impl MetadataClient {
    /// Connect over TCP, negotiate control mode, and authenticate. The caller's
    /// credential buffer is zeroized on every return path.
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
        let transport = SubmissionClient::connect_authenticated_mode(
            address,
            token_id,
            token_secret,
            timeout,
            crate::client_protocol::ClientMode::Control,
        )?;
        Ok(Self { transport })
    }

    /// Return schema records from the authenticated principal's visible set.
    pub fn list_schemas(
        &mut self,
    ) -> Result<Vec<crate::metadata_discovery::VisibleSchema>, ClientSubmitError> {
        let mut schemas = Vec::new();
        let mut cursor = None;
        let mut previous_id = None;
        loop {
            let page = self.list_schemas_page(DEFAULT_METADATA_PAGE_SIZE, cursor.as_deref())?;
            for schema in &page.records {
                if previous_id.is_some_and(|previous| previous >= schema.schema_id) {
                    return Err(ClientSubmitError::InvalidResponse);
                }
                previous_id = Some(schema.schema_id);
            }
            schemas.extend(page.records);
            cursor = page.next_cursor;
            if cursor.is_none() {
                return Ok(schemas);
            }
        }
    }

    /// Return one ordered page of visible schemas. Continuations are opaque,
    /// session-bound cursors issued by the authenticated control connection.
    pub fn list_schemas_page(
        &mut self,
        page_limit: usize,
        cursor: Option<&str>,
    ) -> Result<MetadataPage<crate::metadata_discovery::VisibleSchema>, ClientSubmitError> {
        validate_metadata_page_request(page_limit, cursor)?;
        let arguments = metadata_page_arguments(page_limit, cursor);
        let (records, next_cursor) =
            self.request_metadata_page("metadata.schemas.list", arguments)?;
        let records = discovery_records(records)?;
        let mut schemas = Vec::with_capacity(records.len());
        let mut previous_id = None;
        for mut record in records {
            if record.len() != 2 {
                return Err(ClientSubmitError::InvalidResponse);
            }
            let schema_id = response_id(&record, "schema_id")?;
            let name = response_string(&mut record, "name")?;
            if name.is_empty() || previous_id.is_some_and(|previous| previous >= schema_id) {
                return Err(ClientSubmitError::InvalidResponse);
            }
            previous_id = Some(schema_id);
            schemas.push(crate::metadata_discovery::VisibleSchema { schema_id, name });
        }
        Ok(MetadataPage {
            records: schemas,
            next_cursor,
        })
    }

    /// Return grant-visible relations for one schema. An empty vector means
    /// either an authorized empty schema or an unavailable schema.
    pub fn list_relations(
        &mut self,
        schema_id: [u8; 16],
    ) -> Result<Vec<crate::metadata_discovery::VisibleRelation>, ClientSubmitError> {
        let mut relations = Vec::new();
        let mut cursor = None;
        let mut previous_id = None;
        loop {
            let page =
                self.list_relations_page(schema_id, DEFAULT_METADATA_PAGE_SIZE, cursor.as_deref())?;
            for relation in &page.records {
                if previous_id.is_some_and(|previous| previous >= relation.relation_id) {
                    return Err(ClientSubmitError::InvalidResponse);
                }
                previous_id = Some(relation.relation_id);
            }
            relations.extend(page.records);
            cursor = page.next_cursor;
            if cursor.is_none() {
                return Ok(relations);
            }
        }
    }

    /// Return one grant-filtered relation page for a schema.
    pub fn list_relations_page(
        &mut self,
        schema_id: [u8; 16],
        page_limit: usize,
        cursor: Option<&str>,
    ) -> Result<MetadataPage<crate::metadata_discovery::VisibleRelation>, ClientSubmitError> {
        validate_metadata_page_request(page_limit, cursor)?;
        let arguments = format!(
            "{{\"schema_id\":\"{}\",{}}}",
            lower_hex(&schema_id),
            metadata_page_arguments_fields(page_limit, cursor)
        );
        let (records, next_cursor) =
            self.request_metadata_page("metadata.relations.list", arguments)?;
        let records = discovery_records(records)?;
        let mut relations = Vec::with_capacity(records.len());
        let mut previous_id = None;
        for mut record in records {
            if record.len() != 2 {
                return Err(ClientSubmitError::InvalidResponse);
            }
            let relation_id = response_id(&record, "relation_id")?;
            let name = response_string(&mut record, "name")?;
            if name.is_empty() || previous_id.is_some_and(|previous| previous >= relation_id) {
                return Err(ClientSubmitError::InvalidResponse);
            }
            previous_id = Some(relation_id);
            relations.push(crate::metadata_discovery::VisibleRelation { relation_id, name });
        }
        Ok(MetadataPage {
            records: relations,
            next_cursor,
        })
    }

    /// Return the declared, grant-visible column shape for one relation.
    pub fn list_columns(
        &mut self,
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    ) -> Result<Vec<crate::metadata_discovery::VisibleColumn>, ClientSubmitError> {
        let mut columns = Vec::new();
        let mut cursor = None;
        let mut previous_ordinal = None;
        loop {
            let page = self.list_columns_page(
                schema_id,
                relation_id,
                DEFAULT_METADATA_PAGE_SIZE,
                cursor.as_deref(),
            )?;
            for column in &page.records {
                if previous_ordinal.is_some_and(|previous| previous >= column.ordinal) {
                    return Err(ClientSubmitError::InvalidResponse);
                }
                previous_ordinal = Some(column.ordinal);
            }
            columns.extend(page.records);
            cursor = page.next_cursor;
            if cursor.is_none() {
                return Ok(columns);
            }
        }
    }

    /// Return one ordinal-ordered declared-column page for a relation.
    pub fn list_columns_page(
        &mut self,
        schema_id: [u8; 16],
        relation_id: [u8; 16],
        page_limit: usize,
        cursor: Option<&str>,
    ) -> Result<MetadataPage<crate::metadata_discovery::VisibleColumn>, ClientSubmitError> {
        validate_metadata_page_request(page_limit, cursor)?;
        let arguments = format!(
            "{{\"schema_id\":\"{}\",\"relation_id\":\"{}\",{}}}",
            lower_hex(&schema_id),
            lower_hex(&relation_id),
            metadata_page_arguments_fields(page_limit, cursor)
        );
        let (records, next_cursor) =
            self.request_metadata_page("metadata.columns.list", arguments)?;
        let records = discovery_records(records)?;
        let mut columns = Vec::with_capacity(records.len());
        let mut previous_ordinal = None;
        for mut record in records {
            if record.len() != 5 {
                return Err(ClientSubmitError::InvalidResponse);
            }
            let column_id = response_id(&record, "column_id")?;
            let name = response_string(&mut record, "name")?;
            let ordinal = match record.remove("ordinal") {
                Some(ResponseValue::Number(value)) => {
                    u32::try_from(value).map_err(|_| ClientSubmitError::InvalidResponse)?
                }
                _ => return Err(ClientSubmitError::InvalidResponse),
            };
            let scalar_type_name = response_string(&mut record, "scalar_type")?;
            let scalar_type = crate::client_protocol::decode_table_scalar_type(&scalar_type_name)
                .map_err(|_| ClientSubmitError::InvalidResponse)?;
            let nullable = match record.remove("nullable") {
                Some(ResponseValue::Boolean(value)) => value,
                _ => return Err(ClientSubmitError::InvalidResponse),
            };
            if column_id == [0; 16]
                || name.is_empty()
                || previous_ordinal.is_some_and(|previous| previous >= ordinal)
            {
                return Err(ClientSubmitError::InvalidResponse);
            }
            previous_ordinal = Some(ordinal);
            columns.push(crate::metadata_discovery::VisibleColumn {
                column_id,
                name,
                ordinal,
                scalar_type,
                nullable,
            });
        }
        Ok(MetadataPage {
            records: columns,
            next_cursor,
        })
    }

    fn request_metadata_page(
        &mut self,
        operation: &str,
        arguments: String,
    ) -> Result<(Vec<ResponseValue>, Option<String>), ClientSubmitError> {
        let payload = format!(
            "{{\"phase\":\"read\",\"operation_version\":1,\"operation\":\"{operation}\",\"arguments\":{arguments}}}"
        );
        let response = self.transport.exchange(10, payload.into_bytes())?;
        discovery_page_value(&response.payload, operation)
    }
}

impl AdminClient {
    /// Connect over TCP in control mode and authenticate. The caller's
    /// credential buffer is zeroized on every return path.
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
        let transport = SubmissionClient::connect_authenticated_mode(
            address,
            token_id,
            token_secret,
            timeout,
            crate::client_protocol::ClientMode::Control,
        )?;
        Ok(Self { transport })
    }

    /// Create a login identity. New users have no credential until a token is issued.
    pub fn create_user(&mut self, name: &str) -> Result<AdminIdentityResult, ClientSubmitError> {
        self.create_identity("users.create", name, "user_id")
    }

    /// Create a role identity.
    pub fn create_role(&mut self, name: &str) -> Result<AdminIdentityResult, ClientSubmitError> {
        self.create_identity("roles.create", name, "role_id")
    }

    pub fn rename_user(
        &mut self,
        user_id: [u8; 16],
        name: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        self.rename_identity("users.rename", "user_id", user_id, name)
    }

    pub fn rename_role(
        &mut self,
        role_id: [u8; 16],
        name: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        self.rename_identity("roles.rename", "role_id", role_id, name)
    }

    pub fn drop_user(
        &mut self,
        user_id: [u8; 16],
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        self.drop_identity("users.drop", "user_id", user_id)
    }

    pub fn drop_role(
        &mut self,
        role_id: [u8; 16],
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        self.drop_identity("roles.drop", "role_id", role_id)
    }

    /// Enable or disable login for a user. Disabling login revokes that user's tokens.
    pub fn set_user_login(
        &mut self,
        user_id: [u8; 16],
        enabled: bool,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(user_id)?;
        self.execute_admin_operation(
            "users.login",
            format!(
                "{{\"user_id\":\"{}\",\"enabled\":\"{enabled}\"}}",
                lower_hex(&user_id)
            ),
            None,
        )
        .map(|(result, _)| result)
    }

    pub fn grant_role(
        &mut self,
        member_id: [u8; 16],
        role_id: [u8; 16],
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        self.change_role_membership("roles.grant", member_id, role_id)
    }

    pub fn revoke_role(
        &mut self,
        member_id: [u8; 16],
        role_id: [u8; 16],
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        self.change_role_membership("roles.revoke", member_id, role_id)
    }

    /// Issue a bearer token. The plaintext secret is returned once and zeroized
    /// when the result is dropped; a lost START response cannot be recovered.
    pub fn issue_token(
        &mut self,
        user_id: [u8; 16],
        label: &str,
        expires_at_us: i64,
    ) -> Result<AdminTokenResult, ClientSubmitError> {
        validate_admin_id(user_id)?;
        if label.is_empty() || label.len() > 128 || expires_at_us <= 0 {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let arguments = format!(
            "{{\"user_id\":\"{}\",\"label\":{},\"expires_at_us\":\"{expires_at_us}\"}}",
            lower_hex(&user_id),
            json_quote(label)
        );
        let prepare = format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"tokens.issue\",\"arguments\":{arguments}}}"
        );
        let prepared = self.transport.exchange(10, prepare.into_bytes())?;
        let fields = parse_flat_response(&prepared.payload)?;
        let operation_id = response_id(&fields, "operation_id")?;
        if fields.len() != 2 || take_string(&fields, "state").as_deref() != Some("prepared") {
            return Err(ClientSubmitError::InvalidResponse);
        }
        let start = format!(
            "{{\"phase\":\"start\",\"operation_id\":\"{}\"}}",
            lower_hex(&operation_id)
        );
        let mut started = match self.transport.exchange(10, start.into_bytes()) {
            Ok(response) => response,
            Err(ClientSubmitError::ServerRejected(message)) => {
                return Err(ClientSubmitError::AdministrativeOperationRejected {
                    operation_id,
                    message,
                });
            }
            Err(_) => {
                return Err(ClientSubmitError::AdministrativeOutcomeUncertain(
                    operation_id,
                ));
            }
        };
        let parsed = parse_flat_response(&started.payload);
        started.payload.zeroize();
        let mut fields =
            parsed.map_err(|_| ClientSubmitError::AdministrativeOutcomeUncertain(operation_id))?;
        let secret_text = match fields.remove("token_secret") {
            Some(ResponseValue::String(value)) => Zeroizing::new(value),
            _ => {
                return Err(ClientSubmitError::AdministrativeOutcomeUncertain(
                    operation_id,
                ));
            }
        };
        let token_id = response_id(&fields, "token_id").ok();
        let response_operation_id = response_id(&fields, "operation_id").ok();
        let outcome = fields.get("outcome").and_then(ResponseValue::as_string);
        let sequence = fields
            .get("commit_sequence")
            .and_then(ResponseValue::as_string)
            .and_then(|text| parse_canonical_positive_u64(text.to_owned()));
        let token_secret = decode_token_secret(&secret_text);
        if fields.len() != 4
            || token_id.is_none()
            || response_operation_id != Some(operation_id)
            || outcome != Some("committed")
            || sequence.is_none()
            || token_secret.is_none()
        {
            return Err(ClientSubmitError::AdministrativeOutcomeUncertain(
                operation_id,
            ));
        }
        Ok(AdminTokenResult {
            operation_id,
            token_id: token_id.ok_or(ClientSubmitError::AdministrativeOutcomeUncertain(
                operation_id,
            ))?,
            token_secret: token_secret.ok_or(ClientSubmitError::AdministrativeOutcomeUncertain(
                operation_id,
            ))?,
            commit_sequence: sequence.ok_or(ClientSubmitError::AdministrativeOutcomeUncertain(
                operation_id,
            ))?,
        })
    }

    pub fn revoke_token(
        &mut self,
        token_id: [u8; 16],
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(token_id)?;
        self.execute_admin_operation(
            "tokens.revoke",
            format!("{{\"token_id\":\"{}\"}}", lower_hex(&token_id)),
            None,
        )
        .map(|(result, _)| result)
    }

    pub fn grant_system_privilege(
        &mut self,
        grantee_id: [u8; 16],
        privilege: AdminSystemPrivilege,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        self.change_system_privilege("privileges.grant", grantee_id, privilege)
    }

    pub fn revoke_system_privilege(
        &mut self,
        grantee_id: [u8; 16],
        privilege: AdminSystemPrivilege,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        self.change_system_privilege("privileges.revoke", grantee_id, privilege)
    }

    fn change_system_privilege(
        &mut self,
        operation: &str,
        grantee_id: [u8; 16],
        privilege: AdminSystemPrivilege,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(grantee_id)?;
        self.execute_admin_operation(
            operation,
            format!(
                "{{\"grantee_id\":\"{}\",\"privilege\":\"{}\"}}",
                lower_hex(&grantee_id),
                privilege.as_wire_name()
            ),
            None,
        )
        .map(|(result, _)| result)
    }

    pub fn create_schema(
        &mut self,
        name: &str,
        owner_id: [u8; 16],
    ) -> Result<AdminIdentityResult, ClientSubmitError> {
        validate_admin_name(name)?;
        validate_admin_id(owner_id)?;
        let (result, schema_id) = self.execute_admin_operation(
            "schemas.create",
            format!(
                "{{\"name\":\"{name}\",\"owner_id\":\"{}\"}}",
                lower_hex(&owner_id)
            ),
            Some("schema_id"),
        )?;
        Ok(AdminIdentityResult {
            operation_id: result.operation_id,
            identity_id: schema_id.ok_or(ClientSubmitError::InvalidResponse)?,
            commit_sequence: result.commit_sequence,
        })
    }

    pub fn drop_schema(
        &mut self,
        schema_id: [u8; 16],
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(schema_id)?;
        self.execute_admin_operation(
            "schemas.drop",
            format!("{{\"schema_id\":\"{}\"}}", lower_hex(&schema_id)),
            Some("schema_id"),
        )
        .map(|(result, _)| result)
    }

    pub fn rename_schema(
        &mut self,
        schema_id: [u8; 16],
        name: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(schema_id)?;
        validate_admin_name(name)?;
        self.execute_admin_operation_with_name_echo(
            "schemas.rename",
            format!(
                "{{\"schema_id\":\"{}\",\"name\":\"{name}\"}}",
                lower_hex(&schema_id)
            ),
            Some("schema_id"),
            Some(("name", name)),
        )
        .map(|(result, _)| result)
    }

    pub fn rename_table(
        &mut self,
        table_id: [u8; 16],
        name: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(table_id)?;
        validate_admin_name(name)?;
        self.execute_admin_operation(
            "tables.rename",
            format!(
                "{{\"table_id\":\"{}\",\"name\":\"{name}\"}}",
                lower_hex(&table_id)
            ),
            Some("table_id"),
        )
        .map(|(result, _)| result)
    }

    pub fn drop_table(
        &mut self,
        table_id: [u8; 16],
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(table_id)?;
        self.execute_admin_operation(
            "tables.drop",
            format!("{{\"table_id\":\"{}\"}}", lower_hex(&table_id)),
            Some("table_id"),
        )
        .map(|(result, _)| result)
    }

    /// Create a table with typed columns. The result preserves the server's
    /// column order and returns each generated column ID in that same order.
    pub fn create_table(
        &mut self,
        schema_id: [u8; 16],
        name: &str,
        columns: &[AdminColumnDefinition],
    ) -> Result<AdminTableResult, ClientSubmitError> {
        validate_admin_id(schema_id)?;
        validate_admin_name(name)?;
        let encoded_columns = encode_admin_table_columns(columns)?;
        let prepare = format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"tables.create\",\"arguments\":{{\"schema_id\":\"{}\",\"name\":\"{name}\",\"columns\":{encoded_columns}}}}}",
            lower_hex(&schema_id)
        );
        let prepared = self.transport.exchange(10, prepare.into_bytes())?;
        let fields = parse_flat_response(&prepared.payload)?;
        let operation_id = response_id(&fields, "operation_id")?;
        if fields.len() != 2 || take_string(&fields, "state").as_deref() != Some("prepared") {
            return Err(ClientSubmitError::InvalidResponse);
        }
        let start = format!(
            "{{\"phase\":\"start\",\"operation_id\":\"{}\"}}",
            lower_hex(&operation_id)
        );
        let started = match self.transport.exchange(10, start.into_bytes()) {
            Ok(response) => response,
            Err(ClientSubmitError::ServerRejected(message)) => {
                return Err(ClientSubmitError::AdministrativeOperationRejected {
                    operation_id,
                    message,
                });
            }
            Err(_) => {
                return Err(ClientSubmitError::AdministrativeOutcomeUncertain(
                    operation_id,
                ));
            }
        };
        let fields = parse_flat_response(&started.payload)
            .map_err(|_| ClientSubmitError::AdministrativeOutcomeUncertain(operation_id))?;
        let response_operation_id = response_id(&fields, "operation_id").ok();
        let table_id = response_id(&fields, "table_id").ok();
        let outcome = fields.get("outcome").and_then(ResponseValue::as_string);
        let sequence = fields
            .get("commit_sequence")
            .and_then(ResponseValue::as_string)
            .and_then(|text| parse_canonical_positive_u64(text.to_owned()));
        let column_ids = fields.get("column_ids").and_then(|value| {
            let values = value.as_array()?;
            values
                .iter()
                .map(|value| {
                    value
                        .as_string()
                        .and_then(decode_hex_array::<16>)
                        .filter(|id| *id != [0; 16])
                })
                .collect::<Option<Vec<_>>>()
        });
        let valid_columns = column_ids.as_ref().is_some_and(|ids| {
            ids.len() == columns.len()
                && ids.iter().copied().collect::<HashSet<_>>().len() == ids.len()
        });
        if fields.len() != 5
            || response_operation_id != Some(operation_id)
            || table_id.is_none()
            || outcome != Some("committed")
            || sequence.is_none()
            || !valid_columns
        {
            return Err(ClientSubmitError::AdministrativeOutcomeUncertain(
                operation_id,
            ));
        }
        Ok(AdminTableResult {
            operation_id,
            table_id: table_id.ok_or(ClientSubmitError::AdministrativeOutcomeUncertain(
                operation_id,
            ))?,
            column_ids: column_ids.ok_or(ClientSubmitError::AdministrativeOutcomeUncertain(
                operation_id,
            ))?,
            commit_sequence: sequence.ok_or(ClientSubmitError::AdministrativeOutcomeUncertain(
                operation_id,
            ))?,
        })
    }

    pub fn add_column(
        &mut self,
        table_id: [u8; 16],
        column: &AdminColumnDefinition,
    ) -> Result<AdminTableColumnResult, ClientSubmitError> {
        validate_admin_id(table_id)?;
        if column.generated_uuid_identity
            || matches!(column.default, Some(AdminColumnDefault::CurrentPrincipalId))
            || (!column.nullable && column.default.is_none())
        {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let encoded = encode_admin_table_columns(std::slice::from_ref(column))?;
        let column_json = &encoded[1..encoded.len() - 1];
        let arguments = format!(
            "{{\"table_id\":\"{}\",\"column\":{column_json}}}",
            lower_hex(&table_id)
        );
        let (result, column_id) = self.execute_admin_operation_with_fields(
            "tables.add_column",
            arguments,
            Some("column_id"),
            &[
                AdminExpectedResponseField::Id("table_id", table_id),
                AdminExpectedResponseField::PositiveInteger("schema_version"),
            ],
        )?;
        Ok(AdminTableColumnResult {
            operation_id: result.operation_id,
            table_id,
            column_id: column_id.ok_or(ClientSubmitError::InvalidResponse)?,
            commit_sequence: result.commit_sequence,
        })
    }

    pub fn drop_column(
        &mut self,
        table_id: [u8; 16],
        column_id: [u8; 16],
    ) -> Result<AdminTableColumnResult, ClientSubmitError> {
        validate_admin_id(table_id)?;
        validate_admin_id(column_id)?;
        let arguments = format!(
            "{{\"table_id\":\"{}\",\"column_id\":\"{}\"}}",
            lower_hex(&table_id),
            lower_hex(&column_id)
        );
        let (result, response_column_id) = self.execute_admin_operation_with_fields(
            "tables.drop_column",
            arguments,
            Some("column_id"),
            &[AdminExpectedResponseField::Id("table_id", table_id)],
        )?;
        if response_column_id != Some(column_id) {
            return Err(ClientSubmitError::AdministrativeOutcomeUncertain(
                result.operation_id,
            ));
        }
        Ok(AdminTableColumnResult {
            operation_id: result.operation_id,
            table_id,
            column_id,
            commit_sequence: result.commit_sequence,
        })
    }

    pub fn rename_column(
        &mut self,
        table_id: [u8; 16],
        column_id: [u8; 16],
        name: &str,
    ) -> Result<AdminTableColumnResult, ClientSubmitError> {
        validate_admin_id(table_id)?;
        validate_admin_id(column_id)?;
        validate_admin_name(name)?;
        let arguments = format!(
            "{{\"table_id\":\"{}\",\"column_id\":\"{}\",\"name\":\"{name}\"}}",
            lower_hex(&table_id),
            lower_hex(&column_id)
        );
        let (result, response_column_id) = self.execute_admin_operation_with_fields(
            "tables.rename_column",
            arguments,
            Some("column_id"),
            &[
                AdminExpectedResponseField::Id("table_id", table_id),
                AdminExpectedResponseField::String("name", name),
            ],
        )?;
        if response_column_id != Some(column_id) {
            return Err(ClientSubmitError::AdministrativeOutcomeUncertain(
                result.operation_id,
            ));
        }
        Ok(AdminTableColumnResult {
            operation_id: result.operation_id,
            table_id,
            column_id,
            commit_sequence: result.commit_sequence,
        })
    }

    pub fn set_table_owner(
        &mut self,
        table_id: [u8; 16],
        owner_id: [u8; 16],
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(table_id)?;
        validate_admin_id(owner_id)?;
        self.execute_admin_operation_with_fields(
            "tables.set_owner",
            format!(
                "{{\"table_id\":\"{}\",\"owner_id\":\"{}\"}}",
                lower_hex(&table_id),
                lower_hex(&owner_id)
            ),
            None,
            &[
                AdminExpectedResponseField::Id("table_id", table_id),
                AdminExpectedResponseField::Id("owner_id", owner_id),
            ],
        )
        .map(|(result, _)| result)
    }

    pub fn alter_column_nullability(
        &mut self,
        table_id: [u8; 16],
        column_id: [u8; 16],
        nullable: bool,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(table_id)?;
        validate_admin_id(column_id)?;
        self.execute_admin_operation_with_fields(
            "tables.alter_column_nullability",
            format!(
                "{{\"table_id\":\"{}\",\"column_id\":\"{}\",\"nullable\":\"{nullable}\"}}",
                lower_hex(&table_id),
                lower_hex(&column_id)
            ),
            None,
            &[
                AdminExpectedResponseField::Id("table_id", table_id),
                AdminExpectedResponseField::Id("column_id", column_id),
                AdminExpectedResponseField::Boolean("nullable", nullable),
            ],
        )
        .map(|(result, _)| result)
    }

    pub fn set_column_default(
        &mut self,
        table_id: [u8; 16],
        column_id: [u8; 16],
        scalar_type: crate::tuple::ScalarType,
        default: AdminColumnDefault,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        self.alter_column_default(table_id, column_id, scalar_type, Some(default))
    }

    pub fn drop_column_default(
        &mut self,
        table_id: [u8; 16],
        column_id: [u8; 16],
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        self.alter_column_default(table_id, column_id, crate::tuple::ScalarType::Boolean, None)
    }

    fn alter_column_default(
        &mut self,
        table_id: [u8; 16],
        column_id: [u8; 16],
        scalar_type: crate::tuple::ScalarType,
        default: Option<AdminColumnDefault>,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(table_id)?;
        validate_admin_id(column_id)?;
        let (operation, arguments) = if let Some(default) = default {
            let scalar_type_name = encode_admin_scalar_type(scalar_type)?;
            let encoded_default = encode_admin_default_json(scalar_type, default)?;
            (
                "tables.set_column_default",
                format!(
                    "{{\"table_id\":\"{}\",\"column_id\":\"{}\",\"type\":{},{} }}",
                    lower_hex(&table_id),
                    lower_hex(&column_id),
                    json_quote(&scalar_type_name),
                    encoded_default
                ),
            )
        } else {
            (
                "tables.drop_column_default",
                format!(
                    "{{\"table_id\":\"{}\",\"column_id\":\"{}\"}}",
                    lower_hex(&table_id),
                    lower_hex(&column_id)
                ),
            )
        };
        self.execute_admin_operation_with_fields(
            operation,
            arguments,
            None,
            &[
                AdminExpectedResponseField::Id("table_id", table_id),
                AdminExpectedResponseField::Id("column_id", column_id),
            ],
        )
        .map(|(result, _)| result)
    }

    fn create_identity(
        &mut self,
        operation: &str,
        name: &str,
        id_field: &str,
    ) -> Result<AdminIdentityResult, ClientSubmitError> {
        validate_admin_name(name)?;
        let (result, identity_id) = self.execute_admin_operation(
            operation,
            format!("{{\"name\":\"{name}\"}}"),
            Some(id_field),
        )?;
        Ok(AdminIdentityResult {
            operation_id: result.operation_id,
            identity_id: identity_id.ok_or(ClientSubmitError::InvalidResponse)?,
            commit_sequence: result.commit_sequence,
        })
    }

    fn rename_identity(
        &mut self,
        operation: &str,
        id_field: &str,
        identity_id: [u8; 16],
        name: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(identity_id)?;
        validate_admin_name(name)?;
        self.execute_admin_operation(
            operation,
            format!(
                "{{\"{id_field}\":\"{}\",\"name\":\"{name}\"}}",
                lower_hex(&identity_id)
            ),
            None,
        )
        .map(|(result, _)| result)
    }

    fn drop_identity(
        &mut self,
        operation: &str,
        id_field: &str,
        identity_id: [u8; 16],
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(identity_id)?;
        self.execute_admin_operation(
            operation,
            format!("{{\"{id_field}\":\"{}\"}}", lower_hex(&identity_id)),
            None,
        )
        .map(|(result, _)| result)
    }

    fn change_role_membership(
        &mut self,
        operation: &str,
        member_id: [u8; 16],
        role_id: [u8; 16],
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(member_id)?;
        validate_admin_id(role_id)?;
        if member_id == role_id {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        self.execute_admin_operation(
            operation,
            format!(
                "{{\"member_id\":\"{}\",\"role_id\":\"{}\"}}",
                lower_hex(&member_id),
                lower_hex(&role_id)
            ),
            None,
        )
        .map(|(result, _)| result)
    }

    fn execute_admin_operation(
        &mut self,
        operation: &str,
        arguments: String,
        created_id_field: Option<&str>,
    ) -> Result<(AdminOperationResult, Option<[u8; 16]>), ClientSubmitError> {
        self.execute_admin_operation_with_fields(operation, arguments, created_id_field, &[])
    }

    fn execute_admin_operation_with_name_echo(
        &mut self,
        operation: &str,
        arguments: String,
        created_id_field: Option<&str>,
        expected_name: Option<(&str, &str)>,
    ) -> Result<(AdminOperationResult, Option<[u8; 16]>), ClientSubmitError> {
        let expected_fields = expected_name
            .map(|(key, value)| vec![AdminExpectedResponseField::String(key, value)])
            .unwrap_or_default();
        self.execute_admin_operation_with_fields(
            operation,
            arguments,
            created_id_field,
            &expected_fields,
        )
    }

    fn execute_admin_operation_with_fields(
        &mut self,
        operation: &str,
        arguments: String,
        created_id_field: Option<&str>,
        expected_response_fields: &[AdminExpectedResponseField<'_>],
    ) -> Result<(AdminOperationResult, Option<[u8; 16]>), ClientSubmitError> {
        let prepare = format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"{operation}\",\"arguments\":{arguments}}}"
        );
        let prepared = self.transport.exchange(10, prepare.into_bytes())?;
        let fields = parse_flat_response(&prepared.payload)?;
        let operation_id = response_id(&fields, "operation_id")?;
        if fields.len() != 2 || take_string(&fields, "state").as_deref() != Some("prepared") {
            return Err(ClientSubmitError::InvalidResponse);
        }
        let start = format!(
            "{{\"phase\":\"start\",\"operation_id\":\"{}\"}}",
            lower_hex(&operation_id)
        );
        let started = match self.transport.exchange(10, start.into_bytes()) {
            Ok(response) => response,
            Err(ClientSubmitError::ServerRejected(message)) => {
                return Err(ClientSubmitError::AdministrativeOperationRejected {
                    operation_id,
                    message,
                });
            }
            Err(_) => {
                return Err(ClientSubmitError::AdministrativeOutcomeUncertain(
                    operation_id,
                ));
            }
        };
        let fields = parse_flat_response(&started.payload)
            .map_err(|_| ClientSubmitError::AdministrativeOutcomeUncertain(operation_id))?;
        let response_operation_id = response_id(&fields, "operation_id").ok();
        let outcome = fields.get("outcome").and_then(ResponseValue::as_string);
        let sequence = fields
            .get("commit_sequence")
            .and_then(ResponseValue::as_string)
            .and_then(|text| parse_canonical_positive_u64(text.to_owned()));
        let created_id = created_id_field.and_then(|key| response_id(&fields, key).ok());
        let expected_field_count =
            3 + usize::from(created_id_field.is_some()) + expected_response_fields.len();
        if fields.len() != expected_field_count
            || response_operation_id != Some(operation_id)
            || outcome != Some("committed")
            || sequence.is_none()
            || (created_id_field.is_some() && created_id.is_none())
            || expected_response_fields
                .iter()
                .any(|expectation| !expectation.matches(&fields))
        {
            return Err(ClientSubmitError::AdministrativeOutcomeUncertain(
                operation_id,
            ));
        }
        Ok((
            AdminOperationResult {
                operation_id,
                commit_sequence: sequence.unwrap(),
            },
            created_id,
        ))
    }

    /// Create an ordinary index using the authenticated control connection.
    pub fn create_index(
        &mut self,
        table_id: [u8; 16],
        name: &str,
        column_ids: &[[u8; 16]],
        unique: bool,
    ) -> Result<AdminIndexResult, ClientSubmitError> {
        validate_index_request(table_id, column_ids)?;
        if !is_normalized_index_name(name) {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let columns = encode_id_array(column_ids);
        let arguments = format!(
            "{{\"table_id\":\"{}\",\"name\":\"{name}\",\"column_ids\":{columns},\"unique\":{unique}}}",
            lower_hex(&table_id)
        );
        self.execute_index_ddl("indexes.create", arguments, None)
    }

    /// Rebuild an index definition and its memberships through the control
    /// channel. An uncertain START result includes the server-issued
    /// operation ID for status reconciliation; START is never retried.
    pub fn rebuild_index(
        &mut self,
        index_id: [u8; 16],
        column_ids: &[[u8; 16]],
        unique: bool,
    ) -> Result<AdminIndexResult, ClientSubmitError> {
        validate_index_request(index_id, column_ids)?;
        let arguments = format!(
            "{{\"index_id\":\"{}\",\"column_ids\":{},\"unique\":{unique}}}",
            lower_hex(&index_id),
            encode_id_array(column_ids)
        );
        self.execute_index_ddl("indexes.rebuild", arguments, Some(index_id))
    }

    /// Drop an ordinary index through the authenticated control channel.
    pub fn drop_index(
        &mut self,
        index_id: [u8; 16],
    ) -> Result<AdminIndexResult, ClientSubmitError> {
        if index_id == [0; 16] {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        self.execute_index_ddl(
            "indexes.drop",
            format!("{{\"index_id\":\"{}\"}}", lower_hex(&index_id)),
            Some(index_id),
        )
    }

    /// Reconcile a prior administrative operation by operation ID. Use this
    /// after reconnecting if START returned `AdministrativeOutcomeUncertain`.
    pub fn status(
        &mut self,
        operation_id: [u8; 16],
    ) -> Result<InvocationStatus, ClientSubmitError> {
        if operation_id == [0; 16] {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let payload = format!("{{\"invocation_id\":\"{}\"}}", lower_hex(&operation_id));
        let response = self.transport.exchange(8, payload.into_bytes())?;
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

    fn execute_index_ddl(
        &mut self,
        operation: &str,
        arguments: String,
        expected_index_id: Option<[u8; 16]>,
    ) -> Result<AdminIndexResult, ClientSubmitError> {
        let prepare = format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"{operation}\",\"arguments\":{arguments}}}"
        );
        let prepared = self.transport.exchange(10, prepare.into_bytes())?;
        let fields = parse_flat_response(&prepared.payload)?;
        let operation_id = response_id(&fields, "operation_id")?;
        if fields.len() != 2 || take_string(&fields, "state").as_deref() != Some("prepared") {
            return Err(ClientSubmitError::InvalidResponse);
        }

        let start = format!(
            "{{\"phase\":\"start\",\"operation_id\":\"{}\"}}",
            lower_hex(&operation_id)
        );
        let started = match self.transport.exchange(10, start.into_bytes()) {
            Ok(response) => response,
            Err(ClientSubmitError::ServerRejected(message)) => {
                return Err(ClientSubmitError::AdministrativeOperationRejected {
                    operation_id,
                    message,
                });
            }
            Err(_) => {
                return Err(ClientSubmitError::AdministrativeOutcomeUncertain(
                    operation_id,
                ));
            }
        };
        let fields = parse_flat_response(&started.payload)
            .map_err(|_| ClientSubmitError::AdministrativeOutcomeUncertain(operation_id))?;
        let response_operation_id = response_id(&fields, "operation_id").ok();
        let index_id = response_id(&fields, "index_id").ok();
        let outcome = take_string(&fields, "outcome");
        let sequence =
            take_string(&fields, "commit_sequence").and_then(parse_canonical_positive_u64);
        if fields.len() != 4
            || response_operation_id != Some(operation_id)
            || outcome.as_deref() != Some("committed")
            || index_id.is_none()
            || expected_index_id.is_some_and(|expected| index_id != Some(expected))
            || sequence.is_none()
        {
            return Err(ClientSubmitError::AdministrativeOutcomeUncertain(
                operation_id,
            ));
        }
        Ok(AdminIndexResult {
            operation_id,
            index_id: index_id.ok_or(ClientSubmitError::InvalidResponse)?,
            commit_sequence: sequence.ok_or(ClientSubmitError::InvalidResponse)?,
        })
    }
}

fn validate_index_request(
    object_id: [u8; 16],
    column_ids: &[[u8; 16]],
) -> Result<(), ClientSubmitError> {
    if object_id == [0; 16]
        || column_ids.is_empty()
        || column_ids.len() > 256
        || column_ids.contains(&[0; 16])
        || column_ids.iter().copied().collect::<HashSet<_>>().len() != column_ids.len()
    {
        return Err(ClientSubmitError::InvalidAdministrativeRequest);
    }
    Ok(())
}

fn parse_canonical_positive_u64(text: String) -> Option<u64> {
    let value = text.parse::<u64>().ok()?;
    (value > 0 && value.to_string() == text).then_some(value)
}

fn encode_id_array(ids: &[[u8; 16]]) -> String {
    format!(
        "[{}]",
        ids.iter()
            .map(|id| format!("\"{}\"", lower_hex(id)))
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn is_normalized_index_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 128
        && (bytes[0].is_ascii_lowercase() || bytes[0] == b'_')
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
}

fn validate_admin_name(name: &str) -> Result<(), ClientSubmitError> {
    if is_normalized_index_name(name) {
        Ok(())
    } else {
        Err(ClientSubmitError::InvalidAdministrativeRequest)
    }
}

fn validate_admin_id(id: [u8; 16]) -> Result<(), ClientSubmitError> {
    if id == [0; 16] {
        Err(ClientSubmitError::InvalidAdministrativeRequest)
    } else {
        Ok(())
    }
}

fn encode_admin_table_columns(
    columns: &[AdminColumnDefinition],
) -> Result<String, ClientSubmitError> {
    use crate::tuple::ScalarType;

    if columns.is_empty() || columns.len() > crate::tuple::MAX_COLUMNS {
        return Err(ClientSubmitError::InvalidAdministrativeRequest);
    }
    let mut seen_names = HashSet::with_capacity(columns.len());
    let mut encoded_columns = Vec::with_capacity(columns.len());
    for column in columns {
        validate_admin_name(&column.name)?;
        if !seen_names.insert(column.name.as_str()) {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        if column.generated_uuid_identity
            && (column.scalar_type != ScalarType::Uuid || column.default.is_some())
        {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let scalar_type = encode_admin_scalar_type(column.scalar_type)?;
        let mut encoded = format!(
            "{{\"name\":{},\"type\":{},\"nullable\":{},\"generated_uuid_identity\":{}",
            json_quote(&column.name),
            json_quote(&scalar_type),
            column.nullable,
            column.generated_uuid_identity
        );
        if let Some(default) = &column.default {
            match default {
                AdminColumnDefault::Null if column.nullable => {
                    encoded.push_str(",\"default\":{\"kind\":\"null\"}");
                }
                AdminColumnDefault::Null => {
                    return Err(ClientSubmitError::InvalidAdministrativeRequest);
                }
                AdminColumnDefault::CurrentPrincipalId
                    if column.scalar_type == ScalarType::Uuid =>
                {
                    encoded.push_str(",\"default\":{\"kind\":\"current_principal_id\"}");
                }
                AdminColumnDefault::CurrentPrincipalId => {
                    return Err(ClientSubmitError::InvalidAdministrativeRequest);
                }
                AdminColumnDefault::Literal(value) => {
                    let text = encode_admin_default_literal(column.scalar_type, value)?;
                    encoded.push_str(&format!(
                        ",\"default\":{{\"kind\":\"literal\",\"value\":{}}}",
                        json_quote(&text)
                    ));
                }
            }
        }
        encoded.push('}');
        encoded_columns.push(encoded);
    }
    Ok(format!("[{}]", encoded_columns.join(",")))
}

fn encode_admin_scalar_type(
    scalar_type: crate::tuple::ScalarType,
) -> Result<String, ClientSubmitError> {
    use crate::tuple::ScalarType;
    match scalar_type {
        ScalarType::Boolean => Ok("boolean".into()),
        ScalarType::SmallInt => Ok("smallint".into()),
        ScalarType::Integer => Ok("integer".into()),
        ScalarType::BigInt => Ok("bigint".into()),
        ScalarType::Decimal { precision, scale }
            if (1..=38).contains(&precision) && scale <= precision =>
        {
            Ok(format!("decimal({precision},{scale})"))
        }
        ScalarType::Decimal { .. } => Err(ClientSubmitError::InvalidAdministrativeRequest),
        ScalarType::Text => Ok("text".into()),
        ScalarType::Varchar { max_chars } if max_chars > 0 => Ok(format!("varchar({max_chars})")),
        ScalarType::Varchar { .. } => Err(ClientSubmitError::InvalidAdministrativeRequest),
        ScalarType::Bytea => Ok("bytea".into()),
        ScalarType::Uuid => Ok("uuid".into()),
        ScalarType::Date => Ok("date".into()),
        ScalarType::TimestampTz => Ok("timestamptz".into()),
    }
}

fn encode_admin_default_json(
    scalar_type: crate::tuple::ScalarType,
    default: AdminColumnDefault,
) -> Result<String, ClientSubmitError> {
    let encoded = encode_admin_table_columns(&[AdminColumnDefinition {
        name: "_default".into(),
        scalar_type,
        nullable: true,
        generated_uuid_identity: false,
        default: Some(default),
    }])?;
    let column = encoded
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .ok_or(ClientSubmitError::InvalidAdministrativeRequest)?;
    let start = column
        .find(",\"default\":")
        .map(|index| index + 1)
        .ok_or(ClientSubmitError::InvalidAdministrativeRequest)?;
    column
        .get(start..column.len().saturating_sub(1))
        .map(str::to_owned)
        .ok_or(ClientSubmitError::InvalidAdministrativeRequest)
}

fn encode_admin_default_literal(
    scalar_type: crate::tuple::ScalarType,
    value: &crate::tuple::ScalarValue,
) -> Result<String, ClientSubmitError> {
    use crate::tuple::{ScalarType, ScalarValue};
    let invalid = || ClientSubmitError::InvalidAdministrativeRequest;
    let text = match (scalar_type, value) {
        (ScalarType::Boolean, ScalarValue::Boolean(value)) => value.to_string(),
        (ScalarType::SmallInt, ScalarValue::SmallInt(value)) => value.to_string(),
        (ScalarType::Integer, ScalarValue::Integer(value)) => value.to_string(),
        (ScalarType::BigInt, ScalarValue::BigInt(value)) => value.to_string(),
        (ScalarType::Decimal { precision, scale }, ScalarValue::Decimal(value)) => {
            let limit = 10u128.pow(u32::from(precision));
            let magnitude = value.unsigned_abs();
            if magnitude >= limit {
                return Err(invalid());
            }
            let factor = 10u128.pow(u32::from(scale));
            let sign = if *value < 0 { "-" } else { "" };
            if scale == 0 {
                format!("{sign}{magnitude}")
            } else {
                format!(
                    "{sign}{}.{:0width$}",
                    magnitude / factor,
                    magnitude % factor,
                    width = usize::from(scale)
                )
            }
        }
        (ScalarType::Text, ScalarValue::Text(value)) => value.clone(),
        (ScalarType::Varchar { max_chars }, ScalarValue::Varchar(value))
            if value.chars().count() <= max_chars as usize =>
        {
            value.clone()
        }
        (ScalarType::Bytea, ScalarValue::Bytea(value)) => lower_hex(value),
        (ScalarType::Uuid, ScalarValue::Uuid(value)) => lower_hex(value),
        (ScalarType::Date, ScalarValue::Date(value)) => value.to_string(),
        (ScalarType::TimestampTz, ScalarValue::TimestampTz(value)) => value.to_string(),
        _ => return Err(invalid()),
    };
    Ok(text)
}

fn json_quote(text: &str) -> String {
    let mut quoted = String::with_capacity(text.len() + 2);
    quoted.push('"');
    for character in text.chars() {
        match character {
            '"' => quoted.push_str("\\\""),
            '\\' => quoted.push_str("\\\\"),
            '\n' => quoted.push_str("\\n"),
            '\r' => quoted.push_str("\\r"),
            '\t' => quoted.push_str("\\t"),
            character if character <= '\u{1f}' => {
                use std::fmt::Write as _;
                let _ = write!(quoted, "\\u{:04x}", character as u32);
            }
            character => quoted.push(character),
        }
    }
    quoted.push('"');
    quoted
}

fn decode_token_secret(text: &str) -> Option<Zeroizing<[u8; TOKEN_SECRET_BYTES]>> {
    if text.len() != 44 || !text.is_ascii() || !text.ends_with('=') {
        return None;
    }
    let mut secret = Zeroizing::new([0; TOKEN_SECRET_BYTES]);
    let mut accumulator = Zeroizing::new(0u32);
    let mut bit_count = 0u8;
    let mut output_index = 0usize;
    for byte in text.bytes().take(43) {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        };
        *accumulator = (*accumulator << 6) | u32::from(value);
        bit_count += 6;
        if bit_count >= 8 {
            bit_count -= 8;
            if output_index >= secret.len() {
                return None;
            }
            secret[output_index] = (*accumulator >> bit_count) as u8;
            output_index += 1;
        }
    }
    if output_index != TOKEN_SECRET_BYTES
        || bit_count != 2
        || (*accumulator & 0b11) != 0
        || encode_base64(secret.as_ref()) != text
    {
        return None;
    }
    Some(secret)
}

fn discovery_records(
    records: Vec<ResponseValue>,
) -> Result<Vec<HashMap<String, ResponseValue>>, ClientSubmitError> {
    records
        .into_iter()
        .map(|value| match value {
            ResponseValue::Object(fields) => Ok(fields),
            _ => Err(ClientSubmitError::InvalidResponse),
        })
        .collect()
}

fn validate_metadata_page_request(
    page_limit: usize,
    cursor: Option<&str>,
) -> Result<(), ClientSubmitError> {
    if !(1..=crate::client_protocol::CLIENT_METADATA_MAX_PAGE).contains(&page_limit) {
        return Err(ClientSubmitError::InvalidMetadataPage);
    }
    if let Some(cursor) = cursor {
        decode_metadata_cursor(cursor).map_err(|_| ClientSubmitError::InvalidMetadataPage)?;
    }
    Ok(())
}

fn metadata_page_arguments(page_limit: usize, cursor: Option<&str>) -> String {
    format!("{{{}}}", metadata_page_arguments_fields(page_limit, cursor))
}

fn metadata_page_arguments_fields(page_limit: usize, cursor: Option<&str>) -> String {
    let mut fields = format!("\"limit\":\"{page_limit}\"");
    if let Some(cursor) = cursor {
        fields.push_str(",\"cursor\":\"");
        fields.push_str(cursor);
        fields.push('"');
    }
    fields
}

fn decode_metadata_cursor(text: &str) -> Result<[u8; 16], ClientSubmitError> {
    if text.len() != 32 {
        return Err(ClientSubmitError::InvalidResponse);
    }
    let mut cursor = [0; 16];
    for (index, pair) in text.as_bytes().chunks_exact(2).enumerate() {
        let high = lower_hex_digit(pair[0]).ok_or(ClientSubmitError::InvalidResponse)?;
        let low = lower_hex_digit(pair[1]).ok_or(ClientSubmitError::InvalidResponse)?;
        cursor[index] = (high << 4) | low;
    }
    if cursor[0] != 0xd3 {
        return Err(ClientSubmitError::InvalidResponse);
    }
    Ok(cursor)
}

fn lower_hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn discovery_page_value(
    payload: &[u8],
    operation: &str,
) -> Result<(Vec<ResponseValue>, Option<String>), ClientSubmitError> {
    let key = match operation {
        "metadata.schemas.list" => "schemas",
        "metadata.relations.list" => "relations",
        "metadata.columns.list" => "columns",
        _ => return Err(ClientSubmitError::InvalidResponse),
    };
    let mut fields = parse_flat_response(payload)?;
    if !(fields.len() == 1 || fields.len() == 2) {
        return Err(ClientSubmitError::InvalidResponse);
    }
    let records = fields
        .remove(key)
        .and_then(|value| value.as_array().map(<[ResponseValue]>::to_vec))
        .ok_or(ClientSubmitError::InvalidResponse)?;
    let cursor = match fields.remove("next_cursor") {
        None | Some(ResponseValue::Null) => None,
        Some(ResponseValue::String(value)) => {
            decode_metadata_cursor(&value)?;
            Some(value)
        }
        _ => return Err(ClientSubmitError::InvalidResponse),
    };
    if !fields.is_empty() {
        return Err(ClientSubmitError::InvalidResponse);
    }
    Ok((records, cursor))
}

fn response_string(
    fields: &mut HashMap<String, ResponseValue>,
    key: &str,
) -> Result<String, ClientSubmitError> {
    match fields.remove(key) {
        Some(ResponseValue::String(value)) => Ok(value),
        _ => Err(ClientSubmitError::InvalidResponse),
    }
}

fn response_id(
    fields: &HashMap<String, ResponseValue>,
    key: &str,
) -> Result<[u8; 16], ClientSubmitError> {
    let id = fields
        .get(key)
        .and_then(ResponseValue::as_string)
        .and_then(decode_hex_array::<16>)
        .filter(|id| *id != [0; 16])
        .ok_or(ClientSubmitError::InvalidResponse)?;
    Ok(id)
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

#[derive(Clone, Debug, Eq, PartialEq)]
enum ResponseValue {
    String(String),
    Number(u64),
    Boolean(bool),
    Object(HashMap<String, ResponseValue>),
    Array(Vec<ResponseValue>),
    Null,
}

#[derive(Clone, Copy)]
enum AdminExpectedResponseField<'a> {
    Id(&'a str, [u8; 16]),
    String(&'a str, &'a str),
    Boolean(&'a str, bool),
    PositiveInteger(&'a str),
}

impl AdminExpectedResponseField<'_> {
    fn matches(self, fields: &HashMap<String, ResponseValue>) -> bool {
        match self {
            Self::Id(key, expected) => response_id(fields, key).ok() == Some(expected),
            Self::String(key, expected) => {
                fields.get(key).and_then(ResponseValue::as_string) == Some(expected)
            }
            Self::Boolean(key, expected) => {
                fields.get(key) == Some(&ResponseValue::Boolean(expected))
            }
            Self::PositiveInteger(key) => fields
                .get(key)
                .and_then(ResponseValue::as_string)
                .and_then(|text| parse_canonical_positive_u64(text.to_owned()))
                .is_some(),
        }
    }
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

    fn as_array(&self) -> Option<&[ResponseValue]> {
        match self {
            Self::Array(value) => Some(value),
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
        let value = parse_response_value(parser, depth + 1)?;
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

fn parse_response_value(
    parser: &mut JsonStringObjectParser<'_>,
    depth: usize,
) -> Result<ResponseValue, ClientSubmitError> {
    if depth > 16 {
        return Err(ClientSubmitError::InvalidResponse);
    }
    parser.skip_whitespace();
    match parser.text.get(parser.offset) {
        Some(b'"') => Ok(ResponseValue::String(
            parser
                .parse_string()
                .map_err(|_| ClientSubmitError::InvalidResponse)?,
        )),
        Some(b'{') => Ok(ResponseValue::Object(parse_response_object(parser, depth)?)),
        Some(b'[') => {
            parser
                .expect(b'[')
                .map_err(|_| ClientSubmitError::InvalidResponse)?;
            let mut values = Vec::new();
            parser.skip_whitespace();
            if parser.consume(b']') {
                return Ok(ResponseValue::Array(values));
            }
            loop {
                values.push(parse_response_value(parser, depth + 1)?);
                parser.skip_whitespace();
                if parser.consume(b']') {
                    return Ok(ResponseValue::Array(values));
                }
                parser
                    .expect(b',')
                    .map_err(|_| ClientSubmitError::InvalidResponse)?;
            }
        }
        Some(b't' | b'f') => Ok(ResponseValue::Boolean(
            parser
                .parse_boolean()
                .map_err(|_| ClientSubmitError::InvalidResponse)?,
        )),
        Some(byte) if byte.is_ascii_digit() => Ok(ResponseValue::Number(
            parser
                .parse_u64_number()
                .map_err(|_| ClientSubmitError::InvalidResponse)?,
        )),
        Some(b'n')
            if parser
                .text
                .get(parser.offset..parser.offset.saturating_add(4))
                == Some(b"null") =>
        {
            parser.offset += 4;
            Ok(ResponseValue::Null)
        }
        _ => Err(ClientSubmitError::InvalidResponse),
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
    use super::{
        ClientSubmitError, ResponseValue, decode_hex_array, decode_token_secret,
        encode_admin_table_columns, is_normalized_index_name, parse_canonical_positive_u64,
        parse_flat_response, validate_admin_id, validate_admin_name, validate_index_request,
    };

    #[test]
    fn administrative_index_request_validation_rejects_invalid_ids_names_and_columns() {
        assert!(matches!(
            validate_index_request([0; 16], &[[1; 16]]),
            Err(ClientSubmitError::InvalidAdministrativeRequest)
        ));
        assert!(matches!(
            validate_index_request([1; 16], &[]),
            Err(ClientSubmitError::InvalidAdministrativeRequest)
        ));
        assert!(matches!(
            validate_index_request([1; 16], &[[2; 16], [2; 16]]),
            Err(ClientSubmitError::InvalidAdministrativeRequest)
        ));
        assert!(matches!(
            validate_index_request([1; 16], &[[0; 16]]),
            Err(ClientSubmitError::InvalidAdministrativeRequest)
        ));
        assert!(is_normalized_index_name("_user_idx9"));
        assert!(!is_normalized_index_name("UserIdx"));
        assert!(!is_normalized_index_name("user-index"));
        assert_eq!(parse_canonical_positive_u64("42".to_owned()), Some(42));
        assert_eq!(parse_canonical_positive_u64("0".to_owned()), None);
        assert_eq!(parse_canonical_positive_u64("042".to_owned()), None);
    }

    #[test]
    fn administrative_identity_validation_matches_canonical_names_and_nonzero_ids() {
        assert!(validate_admin_name("alice_9").is_ok());
        assert!(matches!(
            validate_admin_name("Alice"),
            Err(ClientSubmitError::InvalidAdministrativeRequest)
        ));
        assert!(matches!(
            validate_admin_name("has-dash"),
            Err(ClientSubmitError::InvalidAdministrativeRequest)
        ));
        assert!(validate_admin_id([1; 16]).is_ok());
        assert!(matches!(
            validate_admin_id([0; 16]),
            Err(ClientSubmitError::InvalidAdministrativeRequest)
        ));
    }

    #[test]
    fn one_time_token_secret_decoder_requires_canonical_fixed_width_base64() {
        assert_eq!(
            decode_token_secret("paWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaU=").as_deref(),
            Some(&[0xa5; 32])
        );
        assert!(decode_token_secret("paWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaU").is_none());
        assert!(decode_token_secret("p=WlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaU=").is_none());
        assert!(decode_token_secret("paWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaV=").is_none());
    }

    #[test]
    fn admin_table_column_encoder_rejects_invalid_shapes_and_defaults() {
        use super::{AdminColumnDefault, AdminColumnDefinition};
        use crate::tuple::{ScalarType, ScalarValue};

        assert!(encode_admin_table_columns(&[]).is_err());
        let duplicate_names = [
            AdminColumnDefinition {
                name: "value".into(),
                scalar_type: ScalarType::Text,
                nullable: true,
                generated_uuid_identity: false,
                default: None,
            },
            AdminColumnDefinition {
                name: "value".into(),
                scalar_type: ScalarType::Integer,
                nullable: true,
                generated_uuid_identity: false,
                default: None,
            },
        ];
        assert!(encode_admin_table_columns(&duplicate_names).is_err());
        let generated_non_uuid = [AdminColumnDefinition {
            name: "id".into(),
            scalar_type: ScalarType::Text,
            nullable: false,
            generated_uuid_identity: true,
            default: None,
        }];
        assert!(encode_admin_table_columns(&generated_non_uuid).is_err());
        let null_not_null = [AdminColumnDefinition {
            name: "required".into(),
            scalar_type: ScalarType::Integer,
            nullable: false,
            generated_uuid_identity: false,
            default: Some(AdminColumnDefault::Null),
        }];
        assert!(encode_admin_table_columns(&null_not_null).is_err());
        let mismatched_literal = [AdminColumnDefinition {
            name: "count".into(),
            scalar_type: ScalarType::Integer,
            nullable: false,
            generated_uuid_identity: false,
            default: Some(AdminColumnDefault::Literal(ScalarValue::Text("1".into()))),
        }];
        assert!(encode_admin_table_columns(&mismatched_literal).is_err());
    }

    #[test]
    fn response_decoder_rejects_duplicate_unknown_json_shapes_and_noncanonical_numbers() {
        assert!(parse_flat_response(br#"{"state":"prepared"}"#).is_ok());
        assert!(parse_flat_response(br#"{"state":"prepared","state":"prepared"}"#).is_err());
        assert_eq!(
            parse_flat_response(br#"{"nested":[]}"#).unwrap()["nested"],
            ResponseValue::Array(vec![])
        );
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
