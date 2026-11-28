use super::response::*;
use super::types::*;
use super::*;
use crate::client_protocol::{
    CLIENT_ERROR_KIND, CLIENT_MAX_CHUNK_BYTES, CLIENT_MAX_CONTROL_BYTES, CLIENT_MAX_PAYLOAD_BYTES,
    ClientFrame, ClientFrameDecoder,
};
use crate::credentials::TOKEN_SECRET_BYTES;
use crate::submission_manifest::decode_manifest_v1;
use crate::submission_scope::decode_requested_scope_v1;
use crate::supervisor::{
    MAX_MANIFEST_BYTES, MAX_PARAMETER_BYTES, MAX_SCOPE_BYTES, MAX_SQL_DATA_BYTES,
};
use sha2::{Digest, Sha256};
use std::collections::{HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::time::Duration;
use zeroize::{Zeroize, Zeroizing};

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
            1,
            true,
        )
    }

    /// Connect and authenticate using the interactive SQL v2 protocol.
    /// V2 never falls back to v1 when the peer negotiates a different version.
    pub fn connect_interactive_authenticated(
        address: SocketAddr,
        token_id: [u8; 16],
        token_secret: &mut [u8],
    ) -> Result<Self, ClientSubmitError> {
        Self::connect_authenticated_mode(
            address,
            token_id,
            token_secret,
            DEFAULT_TIMEOUT,
            crate::client_protocol::ClientMode::Query,
            2,
            true,
        )
    }

    /// Connect only when the peer advertises the repository-backed SDK
    /// profile. The client never retries this handshake as a legacy upload.
    pub fn connect_repository_authenticated_with_timeout(
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
            1,
            true,
        )
    }

    /// Interactive v2 counterpart that requires repository SDK negotiation
    /// and retains the transaction binding behavior of the v2 protocol.
    pub fn connect_repository_interactive_authenticated_with_timeout(
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
            2,
            true,
        )
    }

    pub(super) fn connect_authenticated_mode(
        address: SocketAddr,
        token_id: [u8; 16],
        token_secret: &mut [u8],
        timeout: Duration,
        mode: crate::client_protocol::ClientMode,
        protocol_major: u16,
        repository_sdk_library: bool,
    ) -> Result<Self, ClientSubmitError> {
        let secret = Zeroizing::new(token_secret.to_vec());
        token_secret.zeroize();
        if secret.len() != TOKEN_SECRET_BYTES || timeout.is_zero() {
            return Err(ClientSubmitError::InvalidCredentials);
        }
        if protocol_major == 2 && !address.ip().is_loopback() {
            return Err(ClientSubmitError::InteractiveV2LoopbackOnly);
        }
        if protocol_major == 2 && !address.ip().is_loopback() {
            return Err(ClientSubmitError::InteractiveV2LoopbackOnly);
        }
        let stream = TcpStream::connect_timeout(&address, timeout)?;
        stream.set_read_timeout(Some(timeout))?;
        stream.set_write_timeout(Some(timeout))?;
        let mut client = Self {
            stream,
            decoder: if protocol_major == 2 {
                ClientFrameDecoder::negotiating()
            } else {
                ClientFrameDecoder::default()
            },
            pending_frames: VecDeque::new(),
            started_invocations: HashSet::new(),
            next_request_id: 1,
            max_payload_bytes: CLIENT_MAX_PAYLOAD_BYTES,
            max_control_bytes: CLIENT_MAX_CONTROL_BYTES,
            max_chunk_bytes: CLIENT_MAX_CHUNK_BYTES,
            protocol_major,
        };

        let mode_name = match mode {
            crate::client_protocol::ClientMode::Query => "query",
            crate::client_protocol::ClientMode::Control => "control",
        };
        let hello = if repository_sdk_library {
            format!(
                "{{\"mode\":\"{mode_name}\",\"client_name\":\"{CLIENT_NAME}\",\"sdk_library_mode\":\"repository\"}}"
            )
        } else {
            format!("{{\"mode\":\"{mode_name}\",\"client_name\":\"{CLIENT_NAME}\"}}")
        };
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
        if hello_fields.len() != if repository_sdk_library { 6 } else { 5 }
            || hello_fields.get("major") != Some(&ResponseValue::Number(protocol_major as u64))
            || hello_fields.get("minor") != Some(&ResponseValue::Number(0))
            || take_string(&hello_fields, "mode").as_deref() != Some(mode_name)
            || take_string(&hello_fields, "transport").as_deref() != Some("plaintext_tcp")
            || repository_sdk_library
                && hello_fields
                    .get("sdk_library_mode")
                    .and_then(ResponseValue::as_string)
                    != Some("repository")
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

    /// Begin an explicit interactive transaction. If the reply is lost, the
    /// server may retain an unknown transaction until its idle lease expires.
    pub fn interactive_transaction_begin(
        &mut self,
        begin_request_id: [u8; 16],
    ) -> Result<crate::client_protocol::ClientV2Response, ClientSubmitError> {
        self.require_v2()?;
        self.interactive_transaction_request(crate::client_protocol::ClientV2Request::TxBegin {
            begin_request_id,
        })
        .map_err(|error| match error {
            ClientSubmitError::ServerRejected(_) => error,
            _ => ClientSubmitError::InteractiveTransactionBeginOutcomeUncertain,
        })
    }

    pub fn interactive_transaction_attach(
        &mut self,
        transaction_id: [u8; 16],
    ) -> Result<crate::client_protocol::ClientV2Response, ClientSubmitError> {
        self.interactive_transaction_request(crate::client_protocol::ClientV2Request::TxAttach {
            transaction_id,
        })
        .map_err(|error| uncertain_transaction_error(error, transaction_id))
    }

    pub fn interactive_transaction_commit(
        &mut self,
        transaction_id: [u8; 16],
    ) -> Result<crate::client_protocol::ClientV2Response, ClientSubmitError> {
        self.interactive_transaction_request(crate::client_protocol::ClientV2Request::TxCommit {
            transaction_id,
        })
        .map_err(|error| uncertain_transaction_error(error, transaction_id))
    }

    pub fn interactive_transaction_rollback(
        &mut self,
        transaction_id: [u8; 16],
    ) -> Result<crate::client_protocol::ClientV2Response, ClientSubmitError> {
        self.interactive_transaction_request(crate::client_protocol::ClientV2Request::TxRollback {
            transaction_id,
        })
        .map_err(|error| uncertain_transaction_error(error, transaction_id))
    }

    pub fn interactive_transaction_status(
        &mut self,
        transaction_id: [u8; 16],
    ) -> Result<crate::client_protocol::ClientV2Response, ClientSubmitError> {
        self.interactive_transaction_request(crate::client_protocol::ClientV2Request::TxStatus {
            transaction_id,
        })
        .map_err(|error| uncertain_transaction_error(error, transaction_id))
    }

    pub fn interactive_package_status(
        &mut self,
        transaction_id: [u8; 16],
        package_id: [u8; 16],
    ) -> Result<crate::client_protocol::ClientV2Response, ClientSubmitError> {
        self.interactive_transaction_request(
            crate::client_protocol::ClientV2Request::PackageStatus {
                transaction_id,
                package_id,
            },
        )
        .map_err(|error| match error {
            ClientSubmitError::ServerRejected(_) => error,
            _ => ClientSubmitError::InteractivePackageOutcomeUncertain {
                transaction_id,
                package_id,
            },
        })
    }

    pub(super) fn interactive_transaction_request(
        &mut self,
        request: crate::client_protocol::ClientV2Request,
    ) -> Result<crate::client_protocol::ClientV2Response, ClientSubmitError> {
        self.require_v2()?;
        let (kind, payload) = request
            .encode()
            .map_err(|_| ClientSubmitError::InvalidResponse)?;
        let response = self.exchange(kind, payload)?;
        crate::client_protocol::ClientV2Response::decode(kind, &response.payload)
            .map_err(|_| ClientSubmitError::InvalidResponse)
    }

    pub(super) fn require_v2(&self) -> Result<(), ClientSubmitError> {
        if self.protocol_major == 2 {
            Ok(())
        } else {
            Err(ClientSubmitError::InvalidResponse)
        }
    }

    /// Upload a private package directory and parameter bytes, then seal it.
    /// The returned ID is reserved but has not started execution.
    pub fn upload_and_seal(
        &mut self,
        package_dir: &std::path::Path,
        parameters: &[u8],
        deadline_ms: u64,
    ) -> Result<[u8; 16], ClientSubmitError> {
        self.upload_and_seal_bound(package_dir, parameters, None, deadline_ms, None)
    }

    /// Upload a private package directory with explicit SQL data and parameter
    /// bytes, then seal it.
    pub fn upload_and_seal_with_sql_data(
        &mut self,
        package_dir: &std::path::Path,
        parameters: &[u8],
        sql_data: &[u8],
        deadline_ms: u64,
    ) -> Result<[u8; 16], ClientSubmitError> {
        self.upload_and_seal_bound(package_dir, parameters, Some(sql_data), deadline_ms, None)
    }

    /// Upload and seal a package bound to a retained v2 transaction. The
    /// caller supplies a fresh package ID and must never reuse it after an
    /// uncertain START result.
    pub fn upload_interactive_package_and_seal(
        &mut self,
        package_dir: &std::path::Path,
        parameters: &[u8],
        deadline_ms: u64,
        transaction_id: [u8; 16],
        package_id: [u8; 16],
    ) -> Result<[u8; 16], ClientSubmitError> {
        self.upload_and_seal_bound(
            package_dir,
            parameters,
            None,
            deadline_ms,
            Some((transaction_id, package_id)),
        )
    }

    /// Upload and seal a package with explicit SQL data bound to a retained v2
    /// transaction.
    #[allow(clippy::too_many_arguments)]
    pub fn upload_interactive_package_and_seal_with_sql_data(
        &mut self,
        package_dir: &std::path::Path,
        parameters: &[u8],
        sql_data: &[u8],
        deadline_ms: u64,
        transaction_id: [u8; 16],
        package_id: [u8; 16],
    ) -> Result<[u8; 16], ClientSubmitError> {
        self.upload_and_seal_bound(
            package_dir,
            parameters,
            Some(sql_data),
            deadline_ms,
            Some((transaction_id, package_id)),
        )
    }

    /// Upload a plain SQL-text submission (no compiled artifact) and seal it.
    /// The server's own SQL engine parses and executes `sql_data` directly;
    /// there is no `query.so` to build or validate.
    pub fn upload_sql_text_and_seal(
        &mut self,
        requested_scope: &[u8],
        parameters: &[u8],
        sql_data: &[u8],
        deadline_ms: u64,
    ) -> Result<[u8; 16], ClientSubmitError> {
        self.upload_sql_text_and_seal_bound(
            requested_scope,
            parameters,
            sql_data,
            deadline_ms,
            None,
        )
    }

    /// Upload a plain SQL-text submission bound to a retained v2 transaction.
    /// The caller supplies a fresh package ID and must never reuse it after an
    /// uncertain START result.
    pub fn upload_interactive_sql_text_and_seal(
        &mut self,
        requested_scope: &[u8],
        parameters: &[u8],
        sql_data: &[u8],
        deadline_ms: u64,
        transaction_id: [u8; 16],
        package_id: [u8; 16],
    ) -> Result<[u8; 16], ClientSubmitError> {
        self.upload_sql_text_and_seal_bound(
            requested_scope,
            parameters,
            sql_data,
            deadline_ms,
            Some((transaction_id, package_id)),
        )
    }

    fn upload_sql_text_and_seal_bound(
        &mut self,
        requested_scope: &[u8],
        parameters: &[u8],
        sql_data: &[u8],
        deadline_ms: u64,
        transaction_binding: Option<([u8; 16], [u8; 16])>,
    ) -> Result<[u8; 16], ClientSubmitError> {
        if deadline_ms == 0
            || parameters.len() as u64 > MAX_PARAMETER_BYTES
            || sql_data.len() as u64 > MAX_SQL_DATA_BYTES
            || requested_scope.is_empty()
            || requested_scope.len() > MAX_SCOPE_BYTES
        {
            return Err(ClientSubmitError::InvalidPackage);
        }
        if transaction_binding.is_some_and(|(transaction_id, package_id)| {
            self.protocol_major != 2 || transaction_id == [0; 16] || package_id == [0; 16]
        }) {
            return Err(ClientSubmitError::InvalidPackage);
        }
        let empty_sha256: [u8; 32] = Sha256::digest([]).into();
        let parameter_hash: [u8; 32] = Sha256::digest(parameters).into();
        let sql_data_hash: [u8; 32] = Sha256::digest(sql_data).into();
        let mut prepare = format!(
            "{{\"submission_kind\":\"sql_text\",\"manifest_version\":0,\"manifest\":\"\",\"artifact_bytes\":\"0\",\"artifact_sha256\":\"{}\",\"parameter_bytes\":\"{}\",\"parameter_sha256\":\"{}\",\"sql_data_bytes\":\"{}\",\"sql_data_sha256\":\"{}\",\"scope_version\":1,\"requested_scope\":\"{}\",\"deadline_ms\":\"{deadline_ms}\"}}",
            lower_hex(&empty_sha256),
            parameters.len(),
            lower_hex(&parameter_hash),
            sql_data.len(),
            lower_hex(&sql_data_hash),
            encode_base64(requested_scope),
        );
        if let Some((transaction_id, package_id)) = transaction_binding {
            prepare.pop();
            prepare.push_str(&format!(
                ",\"transaction_id\":\"{}\",\"package_id\":\"{}\"}}",
                lower_hex(&transaction_id),
                lower_hex(&package_id),
            ));
        }
        let response = self.exchange(3, prepare.into_bytes())?;
        let fields = parse_flat_response(&response.payload)?;
        let invocation_id = fields
            .get("invocation_id")
            .and_then(ResponseValue::as_string)
            .and_then(decode_hex_array::<16>)
            .ok_or(ClientSubmitError::InvalidResponse)?;
        let parameter_chunk_bytes = fields
            .get("parameter_chunk_bytes")
            .and_then(ResponseValue::as_number)
            .filter(|bytes| (1..=self.max_chunk_bytes as u64).contains(bytes))
            .ok_or(ClientSubmitError::InvalidResponse)?
            as usize;
        let sql_data_chunk_bytes = fields
            .get("sql_data_chunk_bytes")
            .and_then(ResponseValue::as_number)
            .filter(|bytes| (1..=self.max_chunk_bytes as u64).contains(bytes))
            .ok_or(ClientSubmitError::InvalidResponse)? as usize;
        // artifact_chunk_bytes is still advertised for the shared PREPARE
        // response shape, but no artifact chunk is ever uploaded here.
        if fields.len() != 4 || !fields.contains_key("artifact_chunk_bytes") {
            return Err(ClientSubmitError::InvalidResponse);
        }

        self.upload_chunks(5, invocation_id, parameters, parameter_chunk_bytes)?;
        self.upload_chunks(
            crate::client_protocol::CLIENT_SQL_DATA_CHUNK_KIND,
            invocation_id,
            sql_data,
            sql_data_chunk_bytes,
        )?;
        let seal_payload = format!("{{\"invocation_id\":\"{}\"}}", lower_hex(&invocation_id));
        let sealed = self.exchange(6, seal_payload.into_bytes())?;
        let fields = parse_flat_response(&sealed.payload)?;
        if fields.len() != 1 || take_string(&fields, "state").as_deref() != Some("prepared") {
            return Err(ClientSubmitError::InvalidResponse);
        }
        Ok(invocation_id)
    }

    pub(super) fn upload_and_seal_bound(
        &mut self,
        package_dir: &std::path::Path,
        parameters: &[u8],
        sql_data_override: Option<&[u8]>,
        deadline_ms: u64,
        transaction_binding: Option<([u8; 16], [u8; 16])>,
    ) -> Result<[u8; 16], ClientSubmitError> {
        if deadline_ms == 0 || parameters.len() as u64 > MAX_PARAMETER_BYTES {
            return Err(ClientSubmitError::InvalidPackage);
        }
        if transaction_binding.is_some_and(|(transaction_id, package_id)| {
            self.protocol_major != 2 || transaction_id == [0; 16] || package_id == [0; 16]
        }) {
            return Err(ClientSubmitError::InvalidPackage);
        }
        let package = read_package(package_dir)?;
        let sql_data = sql_data_override.unwrap_or(&package.sql_data);
        if sql_data.len() as u64 > MAX_SQL_DATA_BYTES {
            return Err(ClientSubmitError::InvalidPackage);
        }
        let artifact_hash: [u8; 32] = Sha256::digest(&package.artifact).into();
        let parameter_hash: [u8; 32] = Sha256::digest(parameters).into();
        let sql_data_hash: [u8; 32] = Sha256::digest(sql_data).into();
        let mut prepare = format!(
            "{{\"manifest_version\":1,\"manifest\":\"{}\",\"artifact_bytes\":\"{}\",\"artifact_sha256\":\"{}\",\"parameter_bytes\":\"{}\",\"parameter_sha256\":\"{}\",\"sql_data_bytes\":\"{}\",\"sql_data_sha256\":\"{}\",\"scope_version\":1,\"requested_scope\":\"{}\",\"deadline_ms\":\"{deadline_ms}\",\"sdk_library_mode\":\"repository\"}}",
            encode_base64(&package.manifest),
            package.artifact.len(),
            lower_hex(&artifact_hash),
            parameters.len(),
            lower_hex(&parameter_hash),
            sql_data.len(),
            lower_hex(&sql_data_hash),
            encode_base64(&package.scope),
        );
        if let Some((transaction_id, package_id)) = transaction_binding {
            prepare.pop();
            prepare.push_str(&format!(
                ",\"transaction_id\":\"{}\",\"package_id\":\"{}\"}}",
                lower_hex(&transaction_id),
                lower_hex(&package_id),
            ));
        }
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
        let sql_data_chunk_bytes = fields
            .get("sql_data_chunk_bytes")
            .and_then(ResponseValue::as_number)
            .filter(|bytes| (1..=self.max_chunk_bytes as u64).contains(bytes))
            .ok_or(ClientSubmitError::InvalidResponse)? as usize;
        if fields.len() != 4 {
            return Err(ClientSubmitError::InvalidResponse);
        }

        self.upload_chunks(4, invocation_id, &package.artifact, artifact_chunk_bytes)?;
        self.upload_chunks(5, invocation_id, parameters, parameter_chunk_bytes)?;
        self.upload_chunks(
            crate::client_protocol::CLIENT_SQL_DATA_CHUNK_KIND,
            invocation_id,
            sql_data,
            sql_data_chunk_bytes,
        )?;
        let seal_payload = format!("{{\"invocation_id\":\"{}\"}}", lower_hex(&invocation_id));
        let sealed = self.exchange(6, seal_payload.into_bytes())?;
        let fields = parse_flat_response(&sealed.payload)?;
        if fields.len() != 1 || take_string(&fields, "state").as_deref() != Some("prepared") {
            return Err(ClientSubmitError::InvalidResponse);
        }
        Ok(invocation_id)
    }

    /// Start a sealed package in its transaction. `actor` carries an
    /// independently authenticated RUNAS identity; its secret is zeroized on
    /// return. The transaction controller remains the connection identity.
    pub fn start_interactive_package(
        &mut self,
        invocation_id: [u8; 16],
        transaction_id: [u8; 16],
        package_id: [u8; 16],
        actor: Option<crate::client_protocol::ClientStartActorCredentials>,
        receipt_path: &std::path::Path,
    ) -> Result<StartedInvocation<'_>, ClientSubmitError> {
        self.require_v2()?;
        if transaction_id == [0; 16]
            || package_id == [0; 16]
            || invocation_id == [0; 16]
            || self.started_invocations.contains(&invocation_id)
        {
            return Err(ClientSubmitError::InvalidResponse);
        }
        if actor
            .as_ref()
            .is_some_and(|actor| !is_normalized_interactive_username(&actor.expected_name))
        {
            return Err(ClientSubmitError::InvalidCredentials);
        }
        if actor
            .as_ref()
            .is_some_and(|actor| !is_normalized_interactive_username(&actor.expected_name))
        {
            return Err(ClientSubmitError::InvalidCredentials);
        }
        write_start_receipt(receipt_path, invocation_id)?;
        self.started_invocations.insert(invocation_id);
        let request_id = self.next_request_id;
        let mut payload = Zeroizing::new(format!(
            "{{\"invocation_id\":\"{}\",\"transaction_id\":\"{}\",\"package_id\":\"{}\"",
            lower_hex(&invocation_id),
            lower_hex(&transaction_id),
            lower_hex(&package_id),
        ));
        if let Some(actor) = actor {
            let mut encoded_secret = Zeroizing::new(encode_base64(&actor.token_secret[..]));
            use std::fmt::Write as _;
            write!(
                &mut *payload,
                ",\"expected_actor_name\":\"{}\",\"actor_token_id\":\"{}\",\"actor_token_secret\":\"{}\"",
                actor.expected_name,
                lower_hex(&actor.token_id),
                encoded_secret.as_str(),
            )
            .map_err(|_| ClientSubmitError::InvalidResponse)?;
            encoded_secret.zeroize();
        }
        payload.push('}');
        let response = match self.exchange(7, std::mem::take(&mut *payload).into_bytes()) {
            Ok(response) => response,
            Err(error @ ClientSubmitError::ServerRejected(_)) => return Err(error),
            Err(_) => {
                return Err(ClientSubmitError::InteractivePackageOutcomeUncertain {
                    transaction_id,
                    package_id,
                });
            }
        };
        let fields = parse_flat_response(&response.payload).map_err(|_| {
            ClientSubmitError::InteractivePackageOutcomeUncertain {
                transaction_id,
                package_id,
            }
        })?;
        if fields.len() != 1 || take_string(&fields, "state").as_deref() != Some("accepted") {
            return Err(ClientSubmitError::InteractivePackageOutcomeUncertain {
                transaction_id,
                package_id,
            });
        }
        Ok(StartedInvocation {
            client: self,
            request_id,
            next_sequence: 1,
            invocation_id,
            interactive_package: Some((transaction_id, package_id)),
            terminal: false,
        })
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
            interactive_package: None,
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

    /// Request cancellation of this authenticated owner's prepared or active
    /// invocation. Acknowledgement does not itself prove a terminal outcome.
    pub fn cancel(
        &mut self,
        invocation_id: [u8; 16],
    ) -> Result<CancellationResult, ClientSubmitError> {
        if invocation_id == [0; 16] {
            return Err(ClientSubmitError::InvalidInvocationId);
        }
        let payload = format!("{{\"invocation_id\":\"{}\"}}", lower_hex(&invocation_id));
        let response = self.exchange(9, payload.into_bytes())?;
        let fields = parse_flat_response(&response.payload)?;
        if fields.len() != 1 {
            return Err(ClientSubmitError::InvalidResponse);
        }
        match take_string(&fields, "state").as_deref() {
            Some("accepted") => Ok(CancellationResult::Accepted),
            Some("too_late") => Ok(CancellationResult::TooLate),
            Some("committed") => Ok(CancellationResult::Terminal(TerminalOutcome::Committed)),
            Some("aborted") => Ok(CancellationResult::Terminal(TerminalOutcome::Aborted)),
            Some("unknown") => Ok(CancellationResult::Unknown),
            _ => Err(ClientSubmitError::InvalidResponse),
        }
    }

    pub(super) fn upload_chunks(
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

    pub(super) fn exchange(
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
        let request_limit = if matches!(
            kind,
            4 | 5
                | 12
                | crate::client_protocol::CLIENT_SDK_LIBRARY_CHUNK_KIND
                | crate::client_protocol::CLIENT_SQL_DATA_CHUNK_KIND
        ) {
            self.max_chunk_bytes.saturating_add(24)
        } else {
            self.max_control_bytes
        };
        if request.payload.len() > request_limit || request.payload.len() > self.max_payload_bytes {
            request.payload.zeroize();
            return Err(ClientSubmitError::InvalidResponse);
        }
        let encoded_request = if self.protocol_major == 2 {
            request.encode_v2()
        } else {
            request.encode()
        };
        let mut encoded = match encoded_request {
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

    pub(super) fn receive_frame(&mut self) -> Result<ClientFrame, ClientSubmitError> {
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
            .map_err(|_| self.uncertain_error())?;
        if frame.request_id != self.request_id || frame.response_sequence != self.next_sequence {
            return Err(self.uncertain_error());
        }
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| self.uncertain_error())?;
        match frame.kind {
            102 => Ok(Some(ClientResultEvent::Data(frame.payload))),
            103 => {
                if let Some((transaction_id, package_id)) = self.interactive_package {
                    let response = crate::client_protocol::ClientV2Response::decode(
                        crate::client_protocol::CLIENT_V2_PACKAGE_STATUS_KIND,
                        &frame.payload,
                    )
                    .map_err(|_| self.uncertain_error())?;
                    let crate::client_protocol::ClientV2Response::Package {
                        transaction_id: response_transaction_id,
                        package_id: response_package_id,
                        state,
                        result_lost,
                    } = response
                    else {
                        return Err(self.uncertain_error());
                    };
                    if response_transaction_id != transaction_id
                        || response_package_id != package_id
                    {
                        return Err(self.uncertain_error());
                    }
                    self.terminal = true;
                    return Ok(Some(ClientResultEvent::PackageTerminal {
                        transaction_id,
                        package_id,
                        state,
                        result_lost,
                    }));
                }
                let fields =
                    parse_flat_response(&frame.payload).map_err(|_| self.uncertain_error())?;
                if fields.len() != 3 {
                    return Err(self.uncertain_error());
                }
                let response_id = fields
                    .get("invocation_id")
                    .and_then(ResponseValue::as_string)
                    .and_then(decode_hex_array::<16>);
                let outcome = match take_string(&fields, "outcome").as_deref() {
                    Some("committed") => TerminalOutcome::Committed,
                    Some("aborted") => TerminalOutcome::Aborted,
                    Some("unknown") => TerminalOutcome::Unknown,
                    _ => return Err(self.uncertain_error()),
                };
                let reason_code =
                    take_string(&fields, "reason_code").ok_or_else(|| self.uncertain_error())?;
                if response_id != Some(self.invocation_id) || reason_code.len() > 64 {
                    return Err(self.uncertain_error());
                }
                self.terminal = true;
                Ok(Some(ClientResultEvent::Terminal(InvocationTerminal {
                    invocation_id: self.invocation_id,
                    outcome,
                    reason_code,
                })))
            }
            _ => Err(self.uncertain_error()),
        }
    }

    pub(super) fn uncertain_error(&self) -> ClientSubmitError {
        self.interactive_package
            .map(|(transaction_id, package_id)| {
                ClientSubmitError::InteractivePackageOutcomeUncertain {
                    transaction_id,
                    package_id,
                }
            })
            .unwrap_or(ClientSubmitError::OutcomeUncertain(self.invocation_id))
    }
}

pub fn read_package(package_dir: &std::path::Path) -> Result<PackageBytes, ClientSubmitError> {
    let metadata = fs::symlink_metadata(package_dir)?;
    if !metadata.is_dir() || metadata.permissions().mode() & 0o077 != 0 {
        return Err(ClientSubmitError::InvalidPackage);
    }
    let artifact = read_private_file(&package_dir.join("query.so"), MAX_PACKAGE_ARCHIVE_BYTES)?;
    if std::fs::symlink_metadata(package_dir.join("sql.so")).is_ok()
        || std::fs::symlink_metadata(package_dir.join("sql-library.sha256")).is_ok()
    {
        return Err(ClientSubmitError::InvalidPackage);
    }
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
    let has_sql_data = std::fs::symlink_metadata(package_dir.join("query.data")).is_ok();
    let has_sql_data_hash = std::fs::symlink_metadata(package_dir.join("sql_data.sha256")).is_ok();
    if has_sql_data != has_sql_data_hash {
        return Err(ClientSubmitError::InvalidPackage);
    }
    let sql_data = if has_sql_data {
        let bytes = read_private_file(&package_dir.join("query.data"), MAX_SQL_DATA_BYTES)?;
        let expected_sql_hash = read_private_file(&package_dir.join("sql_data.sha256"), 65)?;
        let calculated_sql_hash: [u8; 32] = Sha256::digest(&bytes).into();
        if expected_sql_hash.len() != 65
            || expected_sql_hash[64] != b'\n'
            || !decode_lower_hex(
                std::str::from_utf8(&expected_sql_hash[..64]).unwrap_or(""),
                32,
            )
            || decode_hex_array::<32>(std::str::from_utf8(&expected_sql_hash[..64]).unwrap_or(""))
                != Some(calculated_sql_hash)
        {
            return Err(ClientSubmitError::InvalidPackage);
        }
        bytes
    } else {
        Vec::new()
    };
    Ok(PackageBytes {
        artifact,
        manifest,
        scope,
        sql_data,
    })
}

pub(super) fn read_private_file(
    path: &std::path::Path,
    maximum: u64,
) -> Result<Vec<u8>, ClientSubmitError> {
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

pub fn write_start_receipt(
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
