use super::control_decode::*;
use super::frame::*;
use super::json::*;
use super::types::*;
use super::*;
use crate::credentials::TOKEN_SECRET_BYTES;
use crate::storage_manager::{AuthenticatedQuerySession, StorageManager};
use crate::supervisor::StagingOwner;
use crate::transaction::TransactionError;
use zeroize::{Zeroize, Zeroizing};

/// Per-connection authentication and mode state used by the gateway dispatcher.
/// It keeps the bearer secret only for this connection lifetime, zeroizes it
/// on drop, and gives staging authority only after storage AUTH.
pub struct ClientConnectionSession {
    pub(super) protocol_major: u16,
    pub(super) repository_sdk_library: bool,
    pub(super) hello: Option<ClientMode>,
    pub(super) authenticated: Option<AuthenticatedQuerySession>,
    pub(super) token_secret: Option<[u8; TOKEN_SECRET_BYTES]>,
    pub(super) request_ids: ClientRequestIds,
    pub(super) pending_admin_operation: Option<PendingAdminOperation>,
    pub(super) metadata_page_snapshot: Option<ActiveMetadataPageSnapshot>,
    pub(super) sdk_library_page_snapshot: Option<ActiveSdkLibraryPageSnapshot>,
}

impl Default for ClientConnectionSession {
    fn default() -> Self {
        Self {
            protocol_major: 1,
            repository_sdk_library: false,
            hello: None,
            authenticated: None,
            token_secret: None,
            request_ids: ClientRequestIds::default(),
            pending_admin_operation: None,
            metadata_page_snapshot: None,
            sdk_library_page_snapshot: None,
        }
    }
}

impl ClientConnectionSession {
    /// Select the negotiated client framing version before processing HELLO.
    pub fn set_protocol_major(&mut self, major: u16) -> Result<(), ClientSessionError> {
        if !matches!(major, 1 | 2) || self.hello.is_some() {
            return Err(ClientSessionError::UnsupportedRequest);
        }
        self.protocol_major = major;
        Ok(())
    }

    pub(crate) fn protocol_major(&self) -> u16 {
        self.protocol_major
    }

    /// Validate a transaction-bound START request and return zeroizing
    /// controller credentials for trusted service orchestration.
    pub(crate) fn validate_interactive_start_request(
        &mut self,
        storage: &StorageManager<'_>,
        request: &ClientFrame,
        expected_binding: crate::supervisor::InteractivePackageBinding,
    ) -> Result<ValidatedInteractiveStartCredentials, ClientSessionError> {
        if self.protocol_major != 2 || request.kind != 7 || request.response_sequence != 0 {
            return Err(ClientSessionError::UnsupportedRequest);
        }
        self.request_ids
            .accept(request)
            .map_err(|_| ClientSessionError::UnsupportedRequest)?;
        if !self.permits_request_kind(request.kind) {
            return Err(if self.authenticated.is_none() {
                ClientSessionError::AuthenticationRequired
            } else {
                ClientSessionError::WrongMode
            });
        }
        self.revalidate_storage(storage)?;
        let mut start = decode_client_start_for_protocol(&request.payload, self.protocol_major)
            .map_err(ClientSessionError::InvalidControlPayload)?;
        if start.transaction_id != Some(expected_binding.transaction_id)
            || start.package_id != Some(expected_binding.package_id)
        {
            return Err(ClientSessionError::InvalidControlPayload(
                ClientControlError::InvalidPrepare,
            ));
        }
        let token_id = self
            .authenticated
            .as_ref()
            .ok_or(ClientSessionError::AuthenticationRequired)?
            .token_id();
        let token_secret = self
            .token_secret
            .as_ref()
            .ok_or(ClientSessionError::AuthenticationRequired)?;
        Ok(ValidatedInteractiveStartCredentials {
            controller_token_id: token_id,
            controller_token_secret: Zeroizing::new(*token_secret),
            actor: start.actor.take(),
        })
    }

    /// Dispatch one authenticated v2 transaction-control request. The caller
    /// must have negotiated v2 framing before invoking this method.
    pub fn handle_v2_transaction_request(
        &mut self,
        storage: &StorageManager<'_>,
        request: ClientFrame,
        outcome_retention_deadline_us: i64,
    ) -> Result<ClientFrame, ClientSessionError> {
        if request.response_sequence != 0
            || !(CLIENT_V2_TX_BEGIN_KIND..=CLIENT_V2_PACKAGE_STATUS_KIND).contains(&request.kind)
        {
            return Err(ClientSessionError::UnsupportedRequest);
        }
        self.request_ids
            .accept_v2(&request)
            .map_err(|_| ClientSessionError::UnsupportedRequest)?;
        if self.hello != Some(ClientMode::Query) {
            return Err(if self.authenticated.is_none() {
                ClientSessionError::AuthenticationRequired
            } else {
                ClientSessionError::WrongMode
            });
        }
        self.revalidate_storage(storage)?;
        let controller = self
            .authenticated
            .as_ref()
            .ok_or(ClientSessionError::AuthenticationRequired)?;
        let token_id = controller.token_id();
        let token_secret = self
            .token_secret
            .as_ref()
            .ok_or(ClientSessionError::AuthenticationRequired)?;
        let decoded = ClientV2Request::decode(request.kind, &request.payload)
            .map_err(ClientSessionError::InvalidControlPayload)?;
        let response = match decoded {
            ClientV2Request::TxBegin { begin_request_id } => {
                let begun = storage
                    .begin_interactive_transaction_with_request_id(
                        token_id,
                        token_secret,
                        outcome_retention_deadline_us,
                        begin_request_id,
                    )
                    .map_err(ClientSessionError::InteractiveTransaction)?;
                ClientV2Response::TransactionBegun {
                    transaction_id: begun.transaction_id,
                    state: client_v2_transaction_state(begun.state),
                    idle_timeout_ms: begun.idle_timeout_ms,
                    lifetime_ms: begun.lifetime_ms,
                    max_package_bytes: begun.max_package_bytes,
                    max_packages: begun.max_packages,
                }
            }
            ClientV2Request::TxAttach { transaction_id } => {
                let state = storage
                    .attach_interactive_transaction(token_id, token_secret, transaction_id)
                    .map_err(ClientSessionError::InteractiveTransaction)?;
                ClientV2Response::Transaction {
                    transaction_id,
                    state: client_v2_transaction_state(state),
                }
            }
            ClientV2Request::TxCommit { transaction_id } => {
                let result = storage
                    .commit_interactive_transaction(token_id, token_secret, transaction_id)
                    .map_err(ClientSessionError::InteractiveTransaction)?;
                let _commit_sequence = result;
                ClientV2Response::Transaction {
                    transaction_id,
                    state: crate::client_protocol::ClientV2TransactionState::Committed,
                }
            }
            ClientV2Request::TxRollback { transaction_id } => {
                storage
                    .rollback_interactive_transaction(token_id, token_secret, transaction_id)
                    .map_err(ClientSessionError::InteractiveTransaction)?;
                ClientV2Response::Transaction {
                    transaction_id,
                    state: crate::client_protocol::ClientV2TransactionState::Aborted,
                }
            }
            ClientV2Request::TxStatus { transaction_id } => {
                let state = storage
                    .interactive_transaction_status(token_id, token_secret, transaction_id)
                    .map_err(ClientSessionError::InteractiveTransaction)?;
                ClientV2Response::Transaction {
                    transaction_id,
                    state: client_v2_transaction_state(state),
                }
            }
            ClientV2Request::PackageStatus {
                transaction_id,
                package_id,
            } => {
                let status = storage
                    .interactive_package_status(token_id, token_secret, transaction_id, package_id)
                    .map_err(ClientSessionError::InteractiveTransaction)?;
                ClientV2Response::Package {
                    transaction_id,
                    package_id,
                    state: match status.state {
                        crate::storage_manager::InteractivePackageState::Running => {
                            ClientV2PackageState::Running
                        }
                        crate::storage_manager::InteractivePackageState::Completed => {
                            ClientV2PackageState::Completed
                        }
                        crate::storage_manager::InteractivePackageState::Failed => {
                            ClientV2PackageState::Failed
                        }
                        crate::storage_manager::InteractivePackageState::Unknown => {
                            ClientV2PackageState::Unknown
                        }
                    },
                    result_lost: status.result_lost,
                }
            }
        };
        self.json_response(request.request_id, response.encode())
    }

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
                self.accept_hello_with_sdk_library_mode(
                    hello.mode,
                    &hello.client_name,
                    hello.repository_sdk_library,
                )?;
                if hello.repository_sdk_library {
                    format!(
                        "{{\"major\":{},\"minor\":0,\"mode\":\"{mode_text}\",\"sdk_library_mode\":\"repository\",\"limits\":{{\"payload_bytes\":{CLIENT_MAX_PAYLOAD_BYTES},\"control_bytes\":{CLIENT_MAX_CONTROL_BYTES},\"chunk_bytes\":{CLIENT_MAX_CHUNK_BYTES}}},\"transport\":\"plaintext_tcp\"}}",
                        self.protocol_major
                    )
                    .into_bytes()
                } else {
                    format!(
                        "{{\"major\":{},\"minor\":0,\"mode\":\"{mode_text}\",\"limits\":{{\"payload_bytes\":{CLIENT_MAX_PAYLOAD_BYTES},\"control_bytes\":{CLIENT_MAX_CONTROL_BYTES},\"chunk_bytes\":{CLIENT_MAX_CHUNK_BYTES}}},\"transport\":\"plaintext_tcp\"}}",
                        self.protocol_major
                    )
                    .into_bytes()
                }
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

    /// Route one artifact/parameter/SQL-library chunk using
    /// this connection's storage-authenticated owner, and acknowledge only
    /// the contiguous next offset.
    pub fn handle_upload_request(
        &mut self,
        storage: &StorageManager<'_>,
        staging: &crate::supervisor::StagingStore,
        request: ClientFrame,
    ) -> Result<ClientFrame, ClientSessionError> {
        if request.response_sequence != 0
            || !matches!(request.kind, 4 | 5 | 12 | CLIENT_SDK_LIBRARY_CHUNK_KIND)
        {
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
        self.revalidate_storage(storage)?;
        let chunk = decode_client_chunk(&request.payload)
            .map_err(ClientSessionError::InvalidControlPayload)?;
        if request.kind == CLIENT_SDK_LIBRARY_CHUNK_KIND {
            let actor_id = self
                .authenticated
                .as_ref()
                .ok_or(ClientSessionError::AuthenticationRequired)?
                .principal_id();
            if !storage
                .is_database_admin(actor_id)
                .map_err(ClientSessionError::TokenInventory)?
            {
                return Err(ClientSessionError::SdkLibraryRepositoryUnauthorized);
            }
            let repository = staging
                .sdk_repository()
                .ok_or(ClientSessionError::SdkLibraryRepositoryUnavailable)?;
            let next_offset = repository
                .upload_install_chunk(chunk.invocation_id, actor_id, chunk.offset, &chunk.data)
                .map_err(ClientSessionError::SdkLibraryRepository)?;
            return Ok(ClientFrame {
                kind: 100,
                request_id: request.request_id,
                response_sequence: 0,
                payload: format!("{{\"next_offset\":\"{next_offset}\"}}").into_bytes(),
            });
        }
        let kind = match request.kind {
            4 => crate::supervisor::ChunkKind::Artifact,
            5 => crate::supervisor::ChunkKind::Parameters,
            _ => unreachable!("chunk kind was checked"),
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
        storage: &StorageManager<'_>,
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
        self.revalidate_storage(storage)?;
        let prepare = decode_client_prepare_for_protocol(&request.payload, self.protocol_major)
            .map_err(ClientSessionError::InvalidControlPayload)?;
        if prepare.repository_sdk_library != self.repository_sdk_library {
            return Err(ClientSessionError::InvalidControlPayload(
                ClientControlError::InvalidPrepare,
            ));
        }
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
        if let (Some(transaction_id), Some(package_id)) =
            (prepare.transaction_id, prepare.package_id)
        {
            staging
                .bind_interactive_package(
                    self.staging_owner()?,
                    reservation.invocation_id,
                    transaction_id,
                    package_id,
                )
                .map_err(ClientSessionError::Staging)?;
        }
        let invocation_id = encode_lower_hex_id(reservation.invocation_id);
        let payload = format!(
            "{{\"invocation_id\":\"{invocation_id}\",\"artifact_chunk_bytes\":{CLIENT_MAX_CHUNK_BYTES},\"parameter_chunk_bytes\":{CLIENT_MAX_CHUNK_BYTES}}}"
        );
        Ok(ClientFrame {
            kind: 100,
            request_id: request.request_id,
            response_sequence: 0,
            payload: payload.into_bytes(),
        })
    }

    /// Verify uploaded lengths, hashes, and versioned admission metadata,
    /// then make the staged submission immutable.
    pub fn handle_seal_request(
        &mut self,
        storage: &StorageManager<'_>,
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
        self.revalidate_storage(storage)?;
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

    /// Accept one sealed invocation and return both the sequence-zero wire
    /// acknowledgement and the storage admission that the supervisor owns.
    pub fn handle_start_request<'db>(
        &mut self,
        storage: &StorageManager<'db>,
        staging: &crate::supervisor::StagingStore,
        request: ClientFrame,
        retention_deadline_us: i64,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<
        (
            ClientFrame,
            crate::storage_manager::SupervisorInvocation<'db>,
        ),
        ClientStartError,
    > {
        if request.response_sequence != 0 || request.kind != 7 {
            return Err(ClientStartError::Session(
                ClientSessionError::UnsupportedRequest,
            ));
        }
        self.request_ids
            .accept(&request)
            .map_err(|_| ClientStartError::Session(ClientSessionError::UnsupportedRequest))?;
        if !self.permits_request_kind(request.kind) {
            return Err(ClientStartError::Session(if self.authenticated.is_none() {
                ClientSessionError::AuthenticationRequired
            } else {
                ClientSessionError::WrongMode
            }));
        }
        let invocation_id = decode_client_start_for_protocol(&request.payload, self.protocol_major)
            .map(|start| start.invocation_id)
            .map_err(|error| {
                ClientStartError::Session(ClientSessionError::InvalidControlPayload(error))
            })?;
        let invocation = self.start_submission(
            storage,
            staging,
            invocation_id,
            retention_deadline_us,
            max_rows,
            max_bytes,
        )?;
        let response = ClientFrame {
            kind: 100,
            request_id: request.request_id,
            response_sequence: 0,
            payload: b"{\"state\":\"accepted\"}".to_vec(),
        };
        Ok((response, invocation))
    }

    /// Accept START through the configured executor launch path. The host
    /// profile and cgroup are established before storage admission; the
    /// executor is spawned only after its artifact and confinement command
    /// have been prepared.
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    pub fn handle_start_request_with_executor<'db>(
        &mut self,
        storage: &StorageManager<'db>,
        staging: &crate::supervisor::StagingStore,
        request: ClientFrame,
        admission: crate::supervisor_ipc::GatewayAdmissionConfig,
        executor: &crate::supervisor_ipc::GatewayExecutorConfig,
    ) -> Result<
        (
            ClientFrame,
            crate::storage_manager::SupervisorInvocation<'db>,
            crate::executor_launcher::RunningExecutor,
        ),
        ClientStartError,
    > {
        if request.response_sequence != 0 || request.kind != 7 {
            return Err(ClientStartError::Session(
                ClientSessionError::UnsupportedRequest,
            ));
        }
        self.request_ids
            .accept(&request)
            .map_err(|_| ClientStartError::Session(ClientSessionError::UnsupportedRequest))?;
        if !self.permits_request_kind(request.kind) {
            return Err(ClientStartError::Session(if self.authenticated.is_none() {
                ClientSessionError::AuthenticationRequired
            } else {
                ClientSessionError::WrongMode
            }));
        }
        let invocation_id = decode_client_start_for_protocol(&request.payload, self.protocol_major)
            .map(|start| start.invocation_id)
            .map_err(|error| {
                ClientStartError::Session(ClientSessionError::InvalidControlPayload(error))
            })?;
        let (invocation, worker) = self.start_submission_with_executor(
            storage,
            staging,
            invocation_id,
            admission,
            executor,
        )?;
        let response = ClientFrame {
            kind: 100,
            request_id: request.request_id,
            response_sequence: 0,
            payload: b"{\"state\":\"accepted\"}".to_vec(),
        };
        Ok((response, invocation, worker))
    }

    /// Report the caller-owned staging state or current storage outcome after
    /// revalidating the connection's credential and role closure.
    pub fn handle_status_request(
        &mut self,
        storage: &StorageManager<'_>,
        staging: &crate::supervisor::StagingStore,
        request: ClientFrame,
    ) -> Result<ClientFrame, ClientSessionError> {
        let invocation_id = self.validate_control_request(storage, &request, 8, false)?;
        let session = self
            .authenticated
            .as_ref()
            .ok_or(ClientSessionError::AuthenticationRequired)?;
        let now_us = current_unix_micros().ok_or(ClientSessionError::UnsupportedRequest)?;
        if self.hello == Some(ClientMode::Control) {
            let snapshot = storage
                .get_invocation_status(invocation_id, session.principal_id(), now_us)
                .map_err(ClientSessionError::Status)?;
            let snapshot = match snapshot {
                Some(snapshot) => Some(snapshot),
                None => storage
                    .get_administrative_invocation_status(
                        session.principal_id(),
                        invocation_id,
                        now_us,
                    )
                    .map_err(ClientSessionError::AdministrativeStatus)?,
            };
            let payload = match snapshot.map(|snapshot| snapshot.status) {
                Some(crate::storage_manager::InvocationStatus::Active { .. }) => {
                    b"{\"state\":\"active\"}".to_vec()
                }
                Some(crate::storage_manager::InvocationStatus::Committing { .. }) => {
                    b"{\"state\":\"committing\"}".to_vec()
                }
                Some(crate::storage_manager::InvocationStatus::Terminal(outcome)) => {
                    match outcome.status {
                        crate::outcome::OutcomeStatus::Committed => {
                            b"{\"state\":\"committed\"}".to_vec()
                        }
                        crate::outcome::OutcomeStatus::Aborted => {
                            b"{\"state\":\"aborted\"}".to_vec()
                        }
                    }
                }
                None => b"{\"state\":\"unknown\"}".to_vec(),
            };
            return self.json_response(request.request_id, payload);
        }
        match staging.status(session.staging_owner(), invocation_id) {
            Ok(
                crate::supervisor::SubmissionState::Uploading
                | crate::supervisor::SubmissionState::Sealing
                | crate::supervisor::SubmissionState::Prepared
                | crate::supervisor::SubmissionState::Starting,
            ) => self.json_response(request.request_id, b"{\"state\":\"prepared\"}".to_vec()),
            Ok(crate::supervisor::SubmissionState::Started)
            | Err(crate::supervisor::StagingError::Unavailable) => {
                let snapshot = storage
                    .get_invocation_status(invocation_id, session.principal_id(), now_us)
                    .map_err(ClientSessionError::Status)?;
                let payload = match snapshot.map(|snapshot| snapshot.status) {
                    Some(crate::storage_manager::InvocationStatus::Active { .. }) => {
                        b"{\"state\":\"active\"}".to_vec()
                    }
                    Some(crate::storage_manager::InvocationStatus::Committing { .. }) => {
                        b"{\"state\":\"committing\"}".to_vec()
                    }
                    Some(crate::storage_manager::InvocationStatus::Terminal(outcome)) => {
                        match outcome.status {
                            crate::outcome::OutcomeStatus::Committed => {
                                b"{\"state\":\"committed\"}".to_vec()
                            }
                            crate::outcome::OutcomeStatus::Aborted => {
                                b"{\"state\":\"aborted\"}".to_vec()
                            }
                        }
                    }
                    None => b"{\"state\":\"unknown\"}".to_vec(),
                };
                self.json_response(request.request_id, payload)
            }
            Err(error) => Err(ClientSessionError::Staging(error)),
        }
    }

    pub fn handle_cancel_prepared_request(
        &mut self,
        storage: &StorageManager<'_>,
        staging: &crate::supervisor::StagingStore,
        request: ClientFrame,
    ) -> Result<ClientFrame, ClientSessionError> {
        let invocation_id = self.validate_control_request(storage, &request, 9, false)?;
        let session = self
            .authenticated
            .as_ref()
            .ok_or(ClientSessionError::AuthenticationRequired)?;
        if self.hello == Some(ClientMode::Control) {
            let now_us = current_unix_micros().ok_or(ClientSessionError::UnsupportedRequest)?;
            let mut event_id = [0; 16];
            getrandom::fill(&mut event_id).map_err(|_| {
                ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
            })?;
            if event_id == [0; 16] {
                return Err(ClientSessionError::Storage(
                    TransactionError::RandomnessUnavailable,
                ));
            }
            let result = storage
                .cancel_invocation(
                    session.principal_id(),
                    invocation_id,
                    1,
                    now_us,
                    crate::audit::AuditEvent {
                        event_id,
                        actor_id: Some(session.principal_id()),
                        correlation_id: invocation_id,
                        occurred_at_us: now_us,
                        action: crate::audit::AuditAction::AdministrativeCancellation,
                        result: crate::audit::AuditResult::Accepted,
                        target_ids: vec![invocation_id],
                    },
                )
                .map_err(ClientSessionError::AdministrativeCancellation)?;
            let payload = match result {
                crate::storage_manager::AdministrativeCancellationResult::Accepted => {
                    b"{\"state\":\"accepted\"}".to_vec()
                }
                crate::storage_manager::AdministrativeCancellationResult::TooLate => {
                    b"{\"state\":\"too_late\"}".to_vec()
                }
                crate::storage_manager::AdministrativeCancellationResult::Terminal(outcome) => {
                    match outcome.status {
                        crate::outcome::OutcomeStatus::Committed => {
                            b"{\"state\":\"committed\"}".to_vec()
                        }
                        crate::outcome::OutcomeStatus::Aborted => {
                            b"{\"state\":\"aborted\"}".to_vec()
                        }
                    }
                }
                crate::storage_manager::AdministrativeCancellationResult::Unknown => {
                    b"{\"state\":\"unknown\"}".to_vec()
                }
            };
            return self.json_response(request.request_id, payload);
        }
        match staging.cancel_prepared(session.staging_owner(), invocation_id) {
            Ok(()) => self.json_response(request.request_id, b"{\"state\":\"accepted\"}".to_vec()),
            Err(
                crate::supervisor::StagingError::InvalidState
                | crate::supervisor::StagingError::Unavailable,
            ) => {
                let now_us = current_unix_micros().ok_or(ClientSessionError::UnsupportedRequest)?;
                let result = storage
                    .cancel_owned_invocation(session.principal_id(), invocation_id, now_us)
                    .map_err(ClientSessionError::Status)?;
                let payload = match result {
                    crate::storage_manager::OwnerCancellationResult::Accepted => {
                        b"{\"state\":\"accepted\"}".to_vec()
                    }
                    crate::storage_manager::OwnerCancellationResult::TooLate => {
                        b"{\"state\":\"too_late\"}".to_vec()
                    }
                    crate::storage_manager::OwnerCancellationResult::Terminal(outcome) => {
                        match outcome.status {
                            crate::outcome::OutcomeStatus::Committed => {
                                b"{\"state\":\"committed\"}".to_vec()
                            }
                            crate::outcome::OutcomeStatus::Aborted => {
                                b"{\"state\":\"aborted\"}".to_vec()
                            }
                        }
                    }
                    crate::storage_manager::OwnerCancellationResult::Unknown => {
                        b"{\"state\":\"unknown\"}".to_vec()
                    }
                };
                self.json_response(request.request_id, payload)
            }
            Err(error) => Err(ClientSessionError::Staging(error)),
        }
    }

    pub(super) fn validate_control_request(
        &mut self,
        storage: &StorageManager<'_>,
        request: &ClientFrame,
        kind: u16,
        query_mode_required: bool,
    ) -> Result<[u8; 16], ClientSessionError> {
        if request.response_sequence != 0 || request.kind != kind {
            return Err(ClientSessionError::UnsupportedRequest);
        }
        self.request_ids
            .accept(request)
            .map_err(|_| ClientSessionError::UnsupportedRequest)?;
        if !self.permits_request_kind(kind) {
            return Err(if self.authenticated.is_none() {
                ClientSessionError::AuthenticationRequired
            } else {
                ClientSessionError::WrongMode
            });
        }
        let invocation_id = decode_client_seal(&request.payload)
            .map_err(ClientSessionError::InvalidControlPayload)?;
        if query_mode_required && self.hello != Some(ClientMode::Query) {
            return Err(ClientSessionError::WrongMode);
        }
        self.revalidate_storage(storage)?;
        Ok(invocation_id)
    }

    pub(super) fn revalidate_storage(
        &mut self,
        storage: &StorageManager<'_>,
    ) -> Result<(), ClientSessionError> {
        let session = self
            .authenticated
            .as_mut()
            .ok_or(ClientSessionError::AuthenticationRequired)?;
        let mut secret = *self
            .token_secret
            .as_ref()
            .ok_or(ClientSessionError::AuthenticationRequired)?;
        let result = storage.revalidate_query_session(session, &secret);
        secret.zeroize();
        result.map_err(ClientSessionError::Storage)
    }

    pub(super) fn json_response(
        &self,
        request_id: u64,
        payload: Vec<u8>,
    ) -> Result<ClientFrame, ClientSessionError> {
        Ok(ClientFrame {
            kind: 100,
            request_id,
            response_sequence: 0,
            payload,
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

    pub fn accept_hello_with_sdk_library_mode(
        &mut self,
        mode: ClientMode,
        client_name: &str,
        repository_sdk_library: bool,
    ) -> Result<(), ClientSessionError> {
        if mode == ClientMode::Query && !repository_sdk_library
            || repository_sdk_library && mode != ClientMode::Query
        {
            return Err(ClientSessionError::InvalidHello);
        }
        self.accept_hello(mode, client_name)?;
        self.repository_sdk_library = repository_sdk_library;
        Ok(())
    }

    pub fn accept_hello_payload(&mut self, payload: &[u8]) -> Result<(), ClientSessionError> {
        let hello =
            decode_client_hello(payload).map_err(ClientSessionError::InvalidControlPayload)?;
        self.accept_hello_with_sdk_library_mode(
            hello.mode,
            &hello.client_name,
            hello.repository_sdk_library,
        )
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
            let requested_scope = crate::submission_scope::decode_requested_scope_v1(
                &submission.metadata().requested_scope,
            )
            .map_err(|_| TransactionError::InvalidContext)?;
            storage.admit_supervisor_invocation_with_deadline_and_scope(
                crate::storage_manager::SupervisorAdmissionRequest {
                    invocation_id: submission.invocation_id(),
                    token_id,
                    retention_deadline_us,
                    max_rows,
                    max_bytes,
                    deadline: Some(deadline),
                    workload_class: submission.metadata().workload_class,
                },
                requested_scope,
                &token_secret,
            )
        });
        token_secret.zeroize();
        result.map_err(ClientStartError::Start)
    }

    /// Revalidate token authority and run the one-shot storage plus executor
    /// launch sequence for a sealed query.
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    pub fn start_submission_with_executor<'db>(
        &mut self,
        storage: &StorageManager<'db>,
        staging: &crate::supervisor::StagingStore,
        invocation_id: [u8; 16],
        admission: crate::supervisor_ipc::GatewayAdmissionConfig,
        executor: &crate::supervisor_ipc::GatewayExecutorConfig,
    ) -> Result<
        (
            crate::storage_manager::SupervisorInvocation<'db>,
            crate::executor_launcher::RunningExecutor,
        ),
        ClientStartError,
    > {
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
        let result = crate::executor_launcher::start_and_launch_executor_selected(
            staging,
            owner,
            invocation_id,
            &executor.cgroup_parent,
            |metadata| executor.resource_limits_for(metadata.workload_class),
            &executor.launch,
            |submission| {
                let duration = std::time::Duration::from_millis(submission.metadata().deadline_ms);
                let deadline = std::time::Instant::now()
                    .checked_add(duration)
                    .ok_or(TransactionError::InvalidContext)?;
                let requested_scope = crate::submission_scope::decode_requested_scope_v1(
                    &submission.metadata().requested_scope,
                )
                .map_err(|_| TransactionError::InvalidContext)?;
                storage.admit_supervisor_invocation_with_deadline_and_scope(
                    crate::storage_manager::SupervisorAdmissionRequest {
                        invocation_id: submission.invocation_id(),
                        token_id,
                        retention_deadline_us: admission
                            .retention_deadline_us()
                            .map_err(|_| TransactionError::InvalidContext)?,
                        max_rows: admission.max_rows,
                        max_bytes: admission.max_bytes,
                        deadline: Some(deadline),
                        workload_class: submission.metadata().workload_class,
                    },
                    requested_scope,
                    &token_secret,
                )
            },
        );
        token_secret.zeroize();
        result.map_err(ClientStartError::ExecutorStart)
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
            ClientMode::Control => {
                (8..=10).contains(&kind) || matches!(kind, 11 | CLIENT_SDK_LIBRARY_CHUNK_KIND)
            }
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
