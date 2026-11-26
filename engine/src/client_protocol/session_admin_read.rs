use super::json::*;
use super::types::*;
use super::*;
use crate::storage_manager::StorageManager;
use crate::transaction::TransactionError;
use zeroize::Zeroizing;

impl ClientConnectionSession {
    /// Execute supported ADMIN reads and connection-owned prepare/start operations.
    pub fn handle_admin_request(
        &mut self,
        storage: &StorageManager<'_>,
        request: ClientFrame,
        retention_deadline_us: i64,
    ) -> Result<ClientFrame, ClientSessionError> {
        self.handle_admin_request_with_staging(storage, None, request, retention_deadline_us)
    }

    /// Execute ADMIN requests with the supervisor-local queue projection
    /// available to version-three runtime-list responses.
    pub fn handle_admin_request_with_staging(
        &mut self,
        storage: &StorageManager<'_>,
        staging: Option<&crate::supervisor::StagingStore>,
        request: ClientFrame,
        retention_deadline_us: i64,
    ) -> Result<ClientFrame, ClientSessionError> {
        if request.response_sequence != 0 || request.kind != 10 {
            return Err(ClientSessionError::UnsupportedRequest);
        }
        self.request_ids
            .accept(&request)
            .map_err(|_| ClientSessionError::UnsupportedRequest)?;
        if !self.permits_request_kind(10) {
            return Err(if self.authenticated.is_none() {
                ClientSessionError::AuthenticationRequired
            } else {
                ClientSessionError::WrongMode
            });
        }
        self.revalidate_storage(storage)?;
        let command = decode_client_admin_command(&request.payload)
            .map_err(ClientSessionError::InvalidControlPayload)?;
        let actor_id = self
            .authenticated
            .as_ref()
            .ok_or(ClientSessionError::AuthenticationRequired)?
            .principal_id();
        if matches!(
            &command,
            ClientAdminCommand::ListSchemas
                | ClientAdminCommand::ListRelations { .. }
                | ClientAdminCommand::ListColumns { .. }
        ) {
            self.metadata_page_snapshot = None;
        }
        match command {
            ClientAdminCommand::ResolveIdentityName {
                name,
                kind,
                policy_scope,
            } => {
                let token_id = self
                    .authenticated
                    .as_ref()
                    .ok_or(ClientSessionError::AuthenticationRequired)?
                    .token_id();
                let token_secret = self
                    .token_secret
                    .as_ref()
                    .ok_or(ClientSessionError::AuthenticationRequired)?;
                let identity_id = match policy_scope {
                    Some((schema_id, table_id)) => storage.resolve_token_policy_identity_name(
                        token_id,
                        token_secret,
                        schema_id,
                        table_id,
                        &name,
                        kind,
                    ),
                    None => storage.resolve_token_administrative_identity_name(
                        token_id,
                        token_secret,
                        &name,
                        kind,
                    ),
                }
                .map_err(ClientSessionError::Storage)?;
                let payload = match identity_id {
                    Some(identity_id) => format!(
                        "{{\"identity_id\":\"{}\"}}",
                        encode_lower_hex_id(identity_id)
                    ),
                    None => "{\"identity_id\":null}".to_owned(),
                };
                self.json_response(request.request_id, payload.into_bytes())
            }
            ClientAdminCommand::ResolveGrantIdentityName {
                schema_name,
                table_name,
                name,
                kind,
            } => {
                let token_id = self
                    .authenticated
                    .as_ref()
                    .ok_or(ClientSessionError::AuthenticationRequired)?
                    .token_id();
                let token_secret = self
                    .token_secret
                    .as_ref()
                    .ok_or(ClientSessionError::AuthenticationRequired)?;
                let resolved = storage
                    .resolve_token_grant_target_identity_name(
                        token_id,
                        token_secret,
                        &schema_name,
                        table_name.as_deref(),
                        &name,
                        kind,
                    )
                    .map_err(ClientSessionError::Storage)?;
                let payload = match resolved {
                    Some(resolution) => format!(
                        "{{\"schema_id\":\"{}\",\"object_id\":\"{}\",\"identity_id\":\"{}\"}}",
                        encode_lower_hex_id(resolution.schema_id),
                        encode_lower_hex_id(resolution.object_id),
                        encode_lower_hex_id(resolution.identity_id)
                    ),
                    None => {
                        "{\"schema_id\":null,\"object_id\":null,\"identity_id\":null}".to_owned()
                    }
                };
                self.json_response(request.request_id, payload.into_bytes())
            }
            ClientAdminCommand::ResolveAlterIdentityName {
                object_id,
                name,
                kind,
            } => {
                let token_id = self
                    .authenticated
                    .as_ref()
                    .ok_or(ClientSessionError::AuthenticationRequired)?
                    .token_id();
                let token_secret = self
                    .token_secret
                    .as_ref()
                    .ok_or(ClientSessionError::AuthenticationRequired)?;
                let identity_id = storage
                    .resolve_token_alter_identity_name(
                        token_id,
                        token_secret,
                        object_id,
                        &name,
                        kind,
                    )
                    .map_err(ClientSessionError::Storage)?;
                let payload = match identity_id {
                    Some(identity_id) => format!(
                        "{{\"identity_id\":\"{}\"}}",
                        encode_lower_hex_id(identity_id)
                    ),
                    None => "{\"identity_id\":null}".to_owned(),
                };
                self.json_response(request.request_id, payload.into_bytes())
            }
            ClientAdminCommand::ResolvePolicyName {
                schema_id,
                table_id,
                name,
                is_cell,
            } => {
                let token_id = self
                    .authenticated
                    .as_ref()
                    .ok_or(ClientSessionError::AuthenticationRequired)?
                    .token_id();
                let token_secret = self
                    .token_secret
                    .as_ref()
                    .ok_or(ClientSessionError::AuthenticationRequired)?;
                let policy_id = storage
                    .resolve_token_policy_name(
                        token_id,
                        token_secret,
                        schema_id,
                        table_id,
                        &name,
                        is_cell,
                    )
                    .map_err(ClientSessionError::Storage)?;
                let payload = match policy_id {
                    Some(policy_id) => {
                        format!("{{\"policy_id\":\"{}\"}}", encode_lower_hex_id(policy_id))
                    }
                    None => "{\"policy_id\":null}".to_owned(),
                };
                self.json_response(request.request_id, payload.into_bytes())
            }
            ClientAdminCommand::ListSdkLibrariesPage { page_limit, cursor } => {
                self.handle_sdk_library_page(staging, request.request_id, page_limit, cursor)
            }
            ClientAdminCommand::RemoveSdkLibrary {
                operation_id,
                sdk_release,
            } => {
                if !storage
                    .is_database_admin(actor_id)
                    .map_err(ClientSessionError::TokenInventory)?
                {
                    return Err(ClientSessionError::SdkLibraryRepositoryUnauthorized);
                }
                let repository = staging
                    .and_then(crate::supervisor::StagingStore::sdk_repository)
                    .ok_or(ClientSessionError::SdkLibraryRepositoryUnavailable)?;
                let removed_bytes = repository
                    .remove_release_with_operation(operation_id, actor_id, sdk_release)
                    .map_err(ClientSessionError::SdkLibraryRepository)?;
                let payload = format!(
                    "{{\"operation_id\":\"{}\",\"release\":{sdk_release},\"removed_bytes\":{removed_bytes}}}",
                    encode_lower_hex_id(operation_id)
                );
                self.json_response(request.request_id, payload.into_bytes())
            }
            ClientAdminCommand::BeginSdkLibraryInstall {
                operation_id,
                metadata_bytes,
                library_bytes,
                library_sha256,
            } => {
                if !storage
                    .is_database_admin(actor_id)
                    .map_err(ClientSessionError::TokenInventory)?
                {
                    return Err(ClientSessionError::SdkLibraryRepositoryUnauthorized);
                }
                let repository = staging
                    .and_then(crate::supervisor::StagingStore::sdk_repository)
                    .ok_or(ClientSessionError::SdkLibraryRepositoryUnavailable)?;
                let next_offset = repository
                    .begin_install_upload(
                        operation_id,
                        actor_id,
                        metadata_bytes,
                        library_bytes,
                        library_sha256,
                    )
                    .map_err(ClientSessionError::SdkLibraryRepository)?;
                let payload = format!(
                    "{{\"operation_id\":\"{}\",\"next_offset\":\"{next_offset}\"}}",
                    encode_lower_hex_id(operation_id)
                );
                self.json_response(request.request_id, payload.into_bytes())
            }
            ClientAdminCommand::SdkLibraryOperationStatus { operation_id } => {
                let repository = staging
                    .and_then(crate::supervisor::StagingStore::sdk_repository)
                    .ok_or(ClientSessionError::SdkLibraryRepositoryUnavailable)?;
                let payload = match repository
                    .operation_outcome(operation_id)
                    .map_err(ClientSessionError::SdkLibraryRepository)?
                {
                    None => format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"unknown\"}}",
                        encode_lower_hex_id(operation_id)
                    ),
                    Some(crate::sdk_registry::SdkLibraryOperationOutcome::Installed {
                        owner_id,
                        sdk_release,
                        sha256,
                        library_bytes,
                    }) if owner_id == actor_id => format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"succeeded\",\"operation\":\"install\",\"release\":{sdk_release},\"sha256\":\"{}\",\"library_bytes\":{library_bytes}}}",
                        encode_lower_hex_id(operation_id),
                        encode_lower_hex_bytes(&sha256)
                    ),
                    Some(crate::sdk_registry::SdkLibraryOperationOutcome::Removed {
                        owner_id,
                        sdk_release,
                        removed_bytes,
                    }) if owner_id == actor_id => format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"succeeded\",\"operation\":\"remove\",\"release\":{sdk_release},\"removed_bytes\":{removed_bytes}}}",
                        encode_lower_hex_id(operation_id)
                    ),
                    Some(_) => {
                        return Err(ClientSessionError::SdkLibraryRepositoryUnauthorized);
                    }
                };
                self.json_response(request.request_id, payload.into_bytes())
            }
            ClientAdminCommand::FinishSdkLibraryInstall { operation_id } => {
                if !storage
                    .is_database_admin(actor_id)
                    .map_err(ClientSessionError::TokenInventory)?
                {
                    return Err(ClientSessionError::SdkLibraryRepositoryUnauthorized);
                }
                let repository = staging
                    .and_then(crate::supervisor::StagingStore::sdk_repository)
                    .ok_or(ClientSessionError::SdkLibraryRepositoryUnavailable)?;
                let release = repository
                    .finish_install_upload(operation_id, actor_id)
                    .map_err(ClientSessionError::SdkLibraryRepository)?;
                let payload = format!(
                    "{{\"operation_id\":\"{}\",\"release\":{},\"sha256\":\"{}\",\"library_bytes\":{}}}",
                    encode_lower_hex_id(operation_id),
                    release.sdk_release,
                    encode_lower_hex_bytes(&release.sha256),
                    release.library_bytes
                );
                self.json_response(request.request_id, payload.into_bytes())
            }
            ClientAdminCommand::ListSdkLibraries => {
                let repository = staging
                    .and_then(crate::supervisor::StagingStore::sdk_repository)
                    .ok_or(ClientSessionError::SdkLibraryRepositoryUnavailable)?;
                let releases = repository
                    .list()
                    .map_err(ClientSessionError::SdkLibraryRepository)?;
                let mut payload = String::from("{\"sdk_libraries\":[");
                for (index, release) in releases.iter().enumerate() {
                    if index != 0 {
                        payload.push(',');
                    }
                    payload.push_str("{\"release\":");
                    payload.push_str(&release.sdk_release.to_string());
                    payload.push_str(",\"native_abis\":[");
                    for (abi_index, abi) in release.native_abis.iter().enumerate() {
                        if abi_index != 0 {
                            payload.push(',');
                        }
                        payload.push_str(&abi.to_string());
                    }
                    payload.push_str("],\"target\":\"");
                    push_json_string(&mut payload, &release.target);
                    payload.push_str("\",\"cpu_baseline\":\"");
                    push_json_string(&mut payload, &release.cpu_baseline);
                    payload.push_str("\",\"runtime_profile\":\"");
                    push_json_string(&mut payload, &release.runtime_profile);
                    payload.push_str("\",\"soname\":\"");
                    push_json_string(&mut payload, &release.soname);
                    payload.push_str("\",\"sha256\":\"");
                    payload.push_str(&encode_lower_hex_bytes(&release.sha256));
                    payload.push_str("\",\"needed\":[");
                    for (dependency_index, dependency) in release.needed.iter().enumerate() {
                        if dependency_index != 0 {
                            payload.push(',');
                        }
                        payload.push('"');
                        push_json_string(&mut payload, dependency);
                        payload.push('"');
                    }
                    payload.push_str("],\"library_bytes\":");
                    payload.push_str(&release.library_bytes.to_string());
                    payload.push('}');
                    if payload.len() > CLIENT_MAX_CONTROL_BYTES {
                        return Err(ClientSessionError::ResourceLimit);
                    }
                }
                payload.push_str("]}");
                if payload.len() > CLIENT_MAX_CONTROL_BYTES {
                    return Err(ClientSessionError::ResourceLimit);
                }
                self.json_response(request.request_id, payload.into_bytes())
            }
            ClientAdminCommand::ListSchemas => {
                let token_id = self
                    .authenticated
                    .as_ref()
                    .ok_or(ClientSessionError::AuthenticationRequired)?
                    .token_id();
                let token_secret = self
                    .token_secret
                    .as_ref()
                    .ok_or(ClientSessionError::AuthenticationRequired)?;
                let schemas = storage
                    .discover_token_authorized_schemas(token_id, token_secret)
                    .map_err(ClientSessionError::Storage)?;
                let mut payload = String::from("{\"schemas\":[");
                for (index, schema) in schemas.iter().enumerate() {
                    if index != 0 {
                        payload.push(',');
                    }
                    payload.push_str("{\"schema_id\":\"");
                    payload.push_str(&encode_lower_hex_id(schema.schema_id));
                    payload.push_str("\",\"name\":\"");
                    push_json_string(&mut payload, &schema.name);
                    payload.push_str("\"}");
                }
                payload.push_str("]}");
                if payload.len() > CLIENT_MAX_CONTROL_BYTES {
                    return Err(ClientSessionError::ResourceLimit);
                }
                self.json_response(request.request_id, payload.into_bytes())
            }
            ClientAdminCommand::ListSchemasPage { page_limit, cursor } => self
                .handle_metadata_discovery_page(
                    storage,
                    request.request_id,
                    MetadataPageQuery::Schemas,
                    page_limit,
                    cursor,
                ),
            ClientAdminCommand::ListRelations { schema_id } => {
                let token_id = self
                    .authenticated
                    .as_ref()
                    .ok_or(ClientSessionError::AuthenticationRequired)?
                    .token_id();
                let token_secret = self
                    .token_secret
                    .as_ref()
                    .ok_or(ClientSessionError::AuthenticationRequired)?;
                let relations = storage
                    .discover_token_authorized_relations(token_id, token_secret, schema_id)
                    .map_err(ClientSessionError::Storage)?;
                let mut payload = String::from("{\"relations\":[");
                if let Some(relations) = relations {
                    for (index, relation) in relations.iter().enumerate() {
                        if index != 0 {
                            payload.push(',');
                        }
                        payload.push_str("{\"relation_id\":\"");
                        payload.push_str(&encode_lower_hex_id(relation.relation_id));
                        payload.push_str("\",\"name\":\"");
                        push_json_string(&mut payload, &relation.name);
                        payload.push_str("\"}");
                    }
                }
                payload.push_str("]}");
                if payload.len() > CLIENT_MAX_CONTROL_BYTES {
                    return Err(ClientSessionError::ResourceLimit);
                }
                self.json_response(request.request_id, payload.into_bytes())
            }
            ClientAdminCommand::ListRelationsPage {
                schema_id,
                page_limit,
                cursor,
            } => self.handle_metadata_discovery_page(
                storage,
                request.request_id,
                MetadataPageQuery::Relations(schema_id),
                page_limit,
                cursor,
            ),
            ClientAdminCommand::ListColumns {
                schema_id,
                relation_id,
            } => {
                let token_id = self
                    .authenticated
                    .as_ref()
                    .ok_or(ClientSessionError::AuthenticationRequired)?
                    .token_id();
                let token_secret = self
                    .token_secret
                    .as_ref()
                    .ok_or(ClientSessionError::AuthenticationRequired)?;
                let columns = storage
                    .discover_token_authorized_columns(
                        token_id,
                        token_secret,
                        schema_id,
                        relation_id,
                    )
                    .map_err(ClientSessionError::Storage)?;
                let mut payload = String::from("{\"columns\":[");
                if let Some(columns) = columns {
                    for (index, column) in columns.iter().enumerate() {
                        if index != 0 {
                            payload.push(',');
                        }
                        payload.push_str("{\"column_id\":\"");
                        payload.push_str(&encode_lower_hex_id(column.column_id));
                        payload.push_str("\",\"name\":\"");
                        push_json_string(&mut payload, &column.name);
                        payload.push_str("\",\"ordinal\":");
                        payload.push_str(&column.ordinal.to_string());
                        payload.push_str(" ,\"scalar_type\":\"");
                        push_json_string(
                            &mut payload,
                            &crate::metadata_discovery::scalar_type_name(column.scalar_type),
                        );
                        payload.push_str("\",\"nullable\":");
                        payload.push_str(if column.nullable { "true" } else { "false" });
                        payload.push('}');
                    }
                }
                payload.push_str("]}");
                if payload.len() > CLIENT_MAX_CONTROL_BYTES {
                    return Err(ClientSessionError::ResourceLimit);
                }
                self.json_response(request.request_id, payload.into_bytes())
            }
            ClientAdminCommand::ListColumnsPage {
                schema_id,
                relation_id,
                page_limit,
                cursor,
            } => self.handle_metadata_discovery_page(
                storage,
                request.request_id,
                MetadataPageQuery::Columns {
                    schema_id,
                    relation_id,
                },
                page_limit,
                cursor,
            ),
            ClientAdminCommand::ListIndexesPage {
                schema_id,
                relation_id,
                page_limit,
                cursor,
            } => self.handle_metadata_discovery_page(
                storage,
                request.request_id,
                MetadataPageQuery::Indexes {
                    schema_id,
                    relation_id,
                },
                page_limit,
                cursor,
            ),
            ClientAdminCommand::ListConstraintsPage {
                schema_id,
                relation_id,
                page_limit,
                cursor,
            } => self.handle_metadata_discovery_page(
                storage,
                request.request_id,
                MetadataPageQuery::Constraints {
                    schema_id,
                    relation_id,
                },
                page_limit,
                cursor,
            ),
            ClientAdminCommand::ListGrantsPage {
                schema_id,
                relation_id,
                page_limit,
                cursor,
            } => self.handle_metadata_discovery_page(
                storage,
                request.request_id,
                MetadataPageQuery::Grants {
                    schema_id,
                    relation_id,
                },
                page_limit,
                cursor,
            ),
            ClientAdminCommand::ListPoliciesPage {
                schema_id,
                relation_id,
                page_limit,
                cursor,
            } => self.handle_metadata_discovery_page(
                storage,
                request.request_id,
                MetadataPageQuery::Policies {
                    schema_id,
                    relation_id,
                },
                page_limit,
                cursor,
            ),
            ClientAdminCommand::ListDependenciesPage {
                object_id,
                page_limit,
                cursor,
            } => self.handle_metadata_discovery_page(
                storage,
                request.request_id,
                MetadataPageQuery::Dependencies(object_id),
                page_limit,
                cursor,
            ),
            ClientAdminCommand::ListTokens {
                user_id,
                after_token_id,
            } => {
                let page = storage
                    .list_tokens_page(actor_id, user_id, after_token_id)
                    .map_err(ClientSessionError::TokenInventory)?;
                let mut payload = String::from("{\"tokens\":[");
                for (index, token) in page.tokens.iter().enumerate() {
                    if index != 0 {
                        payload.push(',');
                    }
                    payload.push_str("{\"token_id\":\"");
                    payload.push_str(&encode_lower_hex_id(token.token_id));
                    payload.push_str("\",\"label\":\"");
                    push_json_string(&mut payload, &token.label);
                    payload.push_str("\",\"issued_at_us\":");
                    payload.push_str(&token.issued_at_us.to_string());
                    payload.push_str(" ,\"expires_at_us\":");
                    payload.push_str(&token.expires_at_us.to_string());
                    payload.push_str(" ,\"revoked\":");
                    payload.push_str(if token.revoked { "true" } else { "false" });
                    payload.push_str(" ,\"revoked_at_us\":");
                    if let Some(revoked_at) = token.revoked_at_us {
                        payload.push_str(&revoked_at.to_string());
                    } else {
                        payload.push_str("null");
                    }
                    payload.push('}');
                }
                payload.push_str("],\"next_after_token_id\":");
                if let Some(cursor) = page.next_after_token_id {
                    payload.push('"');
                    payload.push_str(&encode_lower_hex_id(cursor));
                    payload.push('"');
                } else {
                    payload.push_str("null");
                }
                payload.push('}');
                if payload.len() > CLIENT_MAX_CONTROL_BYTES {
                    return Err(ClientSessionError::ResourceLimit);
                }
                self.json_response(request.request_id, payload.into_bytes())
            }
            ClientAdminCommand::ListAuditEvents { page_limit, cursor } => {
                let page = storage
                    .list_audit_events(actor_id, page_limit, cursor.as_ref())
                    .map_err(ClientSessionError::AdministrativeAuditList)?;
                let mut payload = String::from("{\"events\":[");
                for (index, event) in page.events.iter().enumerate() {
                    if index != 0 {
                        payload.push(',');
                    }
                    payload.push_str("{\"event_id\":\"");
                    payload.push_str(&encode_lower_hex_id(event.event_id));
                    payload.push_str("\",\"actor_id\":");
                    if let Some(actor_id) = event.actor_id {
                        payload.push('"');
                        payload.push_str(&encode_lower_hex_id(actor_id));
                        payload.push('"');
                    } else {
                        payload.push_str("null");
                    }
                    payload.push_str(" ,\"correlation_id\":\"");
                    payload.push_str(&encode_lower_hex_id(event.correlation_id));
                    payload.push_str("\",\"occurred_at_us\":");
                    payload.push_str(&event.occurred_at_us.to_string());
                    payload.push_str(" ,\"action\":");
                    payload.push_str(&(event.action as u16).to_string());
                    payload.push_str(" ,\"result\":");
                    payload.push_str(&(event.result as u16).to_string());
                    payload.push_str(" ,\"target_ids\":[");
                    for (target_index, target_id) in event.target_ids.iter().enumerate() {
                        if target_index != 0 {
                            payload.push(',');
                        }
                        payload.push('"');
                        payload.push_str(&encode_lower_hex_id(*target_id));
                        payload.push('"');
                    }
                    payload.push_str("]}");
                }
                payload.push_str("],\"next_cursor\":");
                if let Some(cursor) = page.continuation {
                    payload.push('"');
                    payload.push_str(&cursor.to_wire());
                    payload.push('"');
                } else {
                    payload.push_str("null");
                }
                payload.push('}');
                if payload.len() > CLIENT_MAX_CONTROL_BYTES {
                    return Err(ClientSessionError::ResourceLimit);
                }
                self.json_response(request.request_id, payload.into_bytes())
            }
            ClientAdminCommand::ListOwnInvocations {
                response_version,
                page_limit,
                cursor,
            } => {
                let now_us = current_unix_micros().ok_or(ClientSessionError::UnsupportedRequest)?;
                let page = storage
                    .list_own_invocations(actor_id, now_us, page_limit, cursor.as_ref())
                    .map_err(ClientSessionError::Status)?;
                let mut payload = format!(
                    "{{\"observed_at_us\":{},\"consistency\":\"best_effort\",\"invocations\":[",
                    page.observed_at_us
                );
                for (index, invocation) in page.invocations.iter().enumerate() {
                    if index != 0 {
                        payload.push(',');
                    }
                    let (state, started_at_us, deadline_at_us, cancellation_requested) =
                        match invocation.status {
                            crate::storage_manager::InvocationStatus::Active {
                                started_at_us,
                                deadline_at_us,
                                cancellation_requested,
                            } => (
                                "active",
                                started_at_us,
                                deadline_at_us,
                                cancellation_requested,
                            ),
                            crate::storage_manager::InvocationStatus::Committing {
                                started_at_us,
                                deadline_at_us,
                                cancellation_requested,
                            } => (
                                "committing",
                                started_at_us,
                                deadline_at_us,
                                cancellation_requested,
                            ),
                            _ => return Err(ClientSessionError::UnsupportedRequest),
                        };
                    payload.push_str("{\"invocation_id\":\"");
                    payload.push_str(&encode_lower_hex_id(invocation.invocation_id));
                    payload.push_str("\",\"state\":\"");
                    payload.push_str(state);
                    payload.push_str("\",\"started_at_us\":");
                    payload.push_str(&started_at_us.to_string());
                    payload.push_str(",\"deadline_at_us\":");
                    push_json_optional_i64(&mut payload, deadline_at_us);
                    payload.push_str(",\"cancellation_requested\":");
                    payload.push_str(if cancellation_requested {
                        "true"
                    } else {
                        "false"
                    });
                    if response_version >= 2 {
                        payload.push_str(",\"staged_rows\":");
                        payload.push_str(&invocation.staged_rows.to_string());
                        payload.push_str(",\"staged_value_bytes\":");
                        payload.push_str(&invocation.staged_value_bytes.to_string());
                    }
                    payload.push('}');
                }
                payload.push_str("],\"next_cursor\":");
                if let Some(cursor) = page.continuation {
                    payload.push('"');
                    payload.push_str(&cursor.to_wire());
                    payload.push('"');
                } else {
                    payload.push_str("null");
                }
                payload.push('}');
                if payload.len() > CLIENT_MAX_CONTROL_BYTES {
                    return Err(ClientSessionError::ResourceLimit);
                }
                self.json_response(request.request_id, payload.into_bytes())
            }
            ClientAdminCommand::ListRuntimeInvocations {
                response_version,
                page_limit,
                cursor,
            } => {
                let now_us = current_unix_micros().ok_or(ClientSessionError::UnsupportedRequest)?;
                let page = storage
                    .list_runtime_invocations(actor_id, now_us, page_limit, cursor.as_ref())
                    .map_err(ClientSessionError::AdministrativeRuntimeList)?;
                let mut payload = format!(
                    "{{\"observed_at_us\":{},\"consistency\":\"best_effort\",\"invocations\":[",
                    page.observed_at_us
                );
                for (index, invocation) in page.invocations.iter().enumerate() {
                    if index != 0 {
                        payload.push(',');
                    }
                    let (state, started_at_us, deadline_at_us, cancellation_requested) =
                        match &invocation.status {
                            crate::storage_manager::InvocationStatus::Active {
                                started_at_us,
                                deadline_at_us,
                                cancellation_requested,
                            } => (
                                "active",
                                Some(*started_at_us),
                                *deadline_at_us,
                                *cancellation_requested,
                            ),
                            crate::storage_manager::InvocationStatus::Committing {
                                started_at_us,
                                deadline_at_us,
                                cancellation_requested,
                            } => (
                                "committing",
                                Some(*started_at_us),
                                *deadline_at_us,
                                *cancellation_requested,
                            ),
                            crate::storage_manager::InvocationStatus::Terminal(outcome) => (
                                match outcome.status {
                                    crate::outcome::OutcomeStatus::Committed => "committed",
                                    crate::outcome::OutcomeStatus::Aborted => "aborted",
                                },
                                None,
                                None,
                                false,
                            ),
                        };
                    payload.push_str("{\"invocation_id\":\"");
                    payload.push_str(&encode_lower_hex_id(invocation.invocation_id));
                    payload.push_str("\",\"transaction_id\":\"");
                    payload.push_str(&encode_lower_hex_id(invocation.transaction_id));
                    payload.push_str("\",\"principal_id\":\"");
                    payload.push_str(&encode_lower_hex_id(invocation.principal_id));
                    payload.push_str("\",\"state\":\"");
                    payload.push_str(state);
                    payload.push_str("\",\"started_at_us\":");
                    push_json_optional_i64(&mut payload, started_at_us);
                    payload.push_str(" ,\"age_us\":");
                    payload.push_str(&invocation.age_us.to_string());
                    payload.push_str(" ,\"deadline_at_us\":");
                    push_json_optional_i64(&mut payload, deadline_at_us);
                    payload.push_str(" ,\"staged_rows\":");
                    payload.push_str(&invocation.staged_rows.to_string());
                    payload.push_str(" ,\"staged_value_bytes\":");
                    payload.push_str(&invocation.staged_value_bytes.to_string());
                    payload.push_str(" ,\"snapshot_pin_count\":");
                    payload.push_str(&invocation.snapshot_pin_count.to_string());
                    payload.push_str(" ,\"cancellation_requested\":");
                    payload.push_str(if cancellation_requested {
                        "true"
                    } else {
                        "false"
                    });
                    payload.push_str(" ,\"cancellation_reason_code\":");
                    if let Some(reason) = invocation.cancellation_reason_code {
                        payload.push_str(&reason.to_string());
                    } else {
                        payload.push_str("null");
                    }
                    payload.push('}');
                }
                encode_fence_waits(
                    &mut payload,
                    RuntimeInvocationSummary {
                        pinned_snapshot_count: page.pinned_snapshot_count,
                        oldest_snapshot_pin_age_us: page.oldest_snapshot_pin_age_us,
                        invocations_admitted_total: page.invocations_admitted_total,
                        invocation_conflicts_total: page.invocation_conflicts_total,
                        admin_cancellations_accepted_total: page.admin_cancellations_accepted_total,
                        admin_cancellations_denied_total: page.admin_cancellations_denied_total,
                        invocation_policy_denials_total: page.invocation_policy_denials_total,
                        invocation_staged_value_bytes_total: page
                            .invocation_staged_value_bytes_total,
                        invocation_commit_latency_buckets: page.invocation_commit_latency_buckets,
                        invocation_redb_write_acquisition_buckets: page
                            .invocation_redb_write_acquisition_buckets,
                        include_write_lock_wait: response_version >= 7,
                        include_outcome_maintenance: response_version >= 2,
                        outcome_maintenance_passes_total: page.outcome_maintenance_passes_total,
                        outcome_maintenance_failures_total: page.outcome_maintenance_failures_total,
                        outcomes_pruned_total: page.outcomes_pruned_total,
                        waits: &page.fence_waits,
                        truncated: page.fence_waits_truncated,
                    },
                );
                if response_version >= 5 {
                    payload.push_str(",\"gateway_result_bytes_relayed_total\":");
                    payload.push_str(
                        &crate::supervisor_ipc::gateway_result_bytes_relayed_total().to_string(),
                    );
                }
                if response_version >= 3 {
                    let usage = staging
                        .map(crate::supervisor::StagingStore::runtime_usage_summary)
                        .transpose()
                        .map_err(ClientSessionError::Staging)?
                        .unwrap_or_default();
                    payload.push_str(",\"supervisor_queue\":{\"pending_submissions\":");
                    payload.push_str(&usage.pending_submissions.to_string());
                    payload.push_str(",\"starting_submissions\":");
                    payload.push_str(&usage.starting_submissions.to_string());
                    payload.push_str(",\"started_submissions\":");
                    payload.push_str(&usage.started_submissions.to_string());
                    payload.push_str(",\"queued_starts\":");
                    payload.push_str(&usage.queued_starts.to_string());
                    if response_version >= 4 {
                        payload.push_str(",\"oldest_queued_start_wait_us\":");
                        payload.push_str(&usage.oldest_queued_start_wait_us.to_string());
                    }
                    if response_version >= 8 {
                        payload.push_str(",\"start_wait_buckets\":[");
                        for (index, bucket) in usage.start_wait_buckets.iter().enumerate() {
                            if index != 0 {
                                payload.push(',');
                            }
                            payload.push_str(&bucket.to_string());
                        }
                        payload.push(']');
                    }
                    payload.push_str(",\"reserved_submission_bytes\":");
                    payload.push_str(&usage.reserved_submission_bytes.to_string());
                    payload.push('}');
                }
                payload.push_str(" ,\"next_cursor\":");
                if let Some(cursor) = page.continuation {
                    payload.push('"');
                    payload.push_str(&cursor.to_wire());
                    payload.push('"');
                } else {
                    payload.push_str("null");
                }
                payload.push('}');
                if payload.len() > CLIENT_MAX_CONTROL_BYTES {
                    return Err(ClientSessionError::ResourceLimit);
                }
                self.json_response(request.request_id, payload.into_bytes())
            }
            ClientAdminCommand::Start { operation_id } => self.handle_admin_start(
                storage,
                &request,
                actor_id,
                operation_id,
                retention_deadline_us,
            ),
            other => self.handle_admin_prepare(&request, other),
        }
    }

    /// Cancel an unstarted reservation without creating a storage outcome.
    /// Started invocations require the supervisor's cancellation service.
    pub(super) fn handle_sdk_library_page(
        &mut self,
        staging: Option<&crate::supervisor::StagingStore>,
        request_id: u64,
        page_limit: usize,
        cursor: Option<[u8; 16]>,
    ) -> Result<ClientFrame, ClientSessionError> {
        if !(1..=CLIENT_METADATA_MAX_PAGE).contains(&page_limit) {
            return Err(ClientSessionError::InvalidControlPayload(
                ClientControlError::InvalidMetadataPageLimit,
            ));
        }
        let repository = staging
            .and_then(crate::supervisor::StagingStore::sdk_repository)
            .ok_or(ClientSessionError::SdkLibraryRepositoryUnavailable)?;
        let session = self
            .authenticated
            .as_ref()
            .ok_or(ClientSessionError::AuthenticationRequired)?;
        let principal_id = session.principal_id();
        let role_ids = session.role_ids().clone();

        let mut snapshot = if let Some(cursor) = cursor {
            let current = self.sdk_library_page_snapshot.as_ref();
            let invalid = current.is_none_or(|snapshot| {
                snapshot.expires_at <= std::time::Instant::now()
                    || snapshot.principal_id != principal_id
                    || snapshot.role_ids != role_ids
                    || snapshot.current_cursor != Some(cursor)
                    || snapshot.page_limit != page_limit
            });
            if invalid {
                self.sdk_library_page_snapshot = None;
                return Err(ClientSessionError::InvalidControlPayload(
                    ClientControlError::InvalidMetadataCursor,
                ));
            }
            let current_releases = repository
                .list()
                .map_err(ClientSessionError::SdkLibraryRepository)?;
            if !current.is_some_and(|snapshot| snapshot.releases == current_releases) {
                self.sdk_library_page_snapshot = None;
                return Err(ClientSessionError::InvalidControlPayload(
                    ClientControlError::InvalidMetadataCursor,
                ));
            }
            self.sdk_library_page_snapshot.take().ok_or(
                ClientSessionError::InvalidControlPayload(
                    ClientControlError::InvalidMetadataCursor,
                ),
            )?
        } else {
            self.sdk_library_page_snapshot = None;
            ActiveSdkLibraryPageSnapshot {
                releases: repository
                    .list()
                    .map_err(ClientSessionError::SdkLibraryRepository)?,
                principal_id,
                role_ids,
                page_limit,
                next_index: 0,
                current_cursor: None,
                expires_at: std::time::Instant::now() + std::time::Duration::from_secs(60),
            }
        };

        let start = snapshot.next_index;
        let end = start
            .saturating_add(snapshot.page_limit)
            .min(snapshot.releases.len());
        let next_cursor = if end < snapshot.releases.len() {
            let mut token = [0; 16];
            loop {
                getrandom::fill(&mut token).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                token[0] = CLIENT_METADATA_CURSOR_KIND;
                if Some(token) != cursor {
                    break;
                }
            }
            Some(token)
        } else {
            None
        };
        let payload = encode_sdk_library_page(&snapshot.releases[start..end], next_cursor);
        if payload.len() > CLIENT_MAX_CONTROL_BYTES {
            return Err(ClientSessionError::ResourceLimit);
        }
        if let Some(next_cursor) = next_cursor {
            snapshot.next_index = end;
            snapshot.current_cursor = Some(next_cursor);
            snapshot.expires_at = std::time::Instant::now() + std::time::Duration::from_secs(60);
            self.sdk_library_page_snapshot = Some(snapshot);
        } else {
            self.sdk_library_page_snapshot = None;
        }
        self.json_response(request_id, payload.into_bytes())
    }

    pub(super) fn handle_metadata_discovery_page(
        &mut self,
        storage: &StorageManager<'_>,
        request_id: u64,
        query: MetadataPageQuery,
        page_limit: usize,
        cursor: Option<[u8; 16]>,
    ) -> Result<ClientFrame, ClientSessionError> {
        if !(1..=CLIENT_METADATA_MAX_PAGE).contains(&page_limit) {
            return Err(ClientSessionError::InvalidControlPayload(
                ClientControlError::InvalidMetadataPageLimit,
            ));
        }
        let session = self
            .authenticated
            .as_ref()
            .ok_or(ClientSessionError::AuthenticationRequired)?;
        let token_id = session.token_id();
        let principal_id = session.principal_id();
        let role_ids = session.role_ids().clone();
        let secret = Zeroizing::new(
            *self
                .token_secret
                .as_ref()
                .ok_or(ClientSessionError::AuthenticationRequired)?,
        );

        let mut snapshot = if let Some(cursor) = cursor {
            let current_snapshot = self.metadata_page_snapshot.as_ref();
            let expired = current_snapshot
                .is_some_and(|snapshot| snapshot.expires_at <= std::time::Instant::now());
            let identity_changed = current_snapshot.is_some_and(|snapshot| {
                snapshot.principal_id != principal_id || snapshot.role_ids != role_ids
            });
            if expired || identity_changed {
                self.metadata_page_snapshot = None;
                return Err(ClientSessionError::InvalidControlPayload(
                    ClientControlError::InvalidMetadataCursor,
                ));
            }
            let valid_cursor = current_snapshot.is_some_and(|snapshot| {
                snapshot.current_cursor == Some(cursor)
                    && snapshot.query == query
                    && snapshot.page_limit == page_limit
            });
            if !valid_cursor {
                return Err(ClientSessionError::InvalidControlPayload(
                    ClientControlError::InvalidMetadataCursor,
                ));
            }
            let current_rows = match &query {
                MetadataPageQuery::Schemas => MetadataPageRows::Schemas(
                    storage
                        .discover_token_authorized_schemas(token_id, &secret)
                        .map_err(ClientSessionError::Storage)?,
                ),
                MetadataPageQuery::Relations(schema_id) => MetadataPageRows::Relations(
                    storage
                        .discover_token_authorized_relations(token_id, &secret, *schema_id)
                        .map_err(ClientSessionError::Storage)?
                        .unwrap_or_default(),
                ),
                MetadataPageQuery::Columns {
                    schema_id,
                    relation_id,
                } => MetadataPageRows::Columns(
                    storage
                        .discover_token_authorized_columns(
                            token_id,
                            &secret,
                            *schema_id,
                            *relation_id,
                        )
                        .map_err(ClientSessionError::Storage)?
                        .unwrap_or_default(),
                ),
                MetadataPageQuery::Indexes {
                    schema_id,
                    relation_id,
                } => MetadataPageRows::Indexes(
                    storage
                        .discover_token_administrative_indexes(
                            token_id,
                            &secret,
                            *schema_id,
                            *relation_id,
                        )
                        .map_err(ClientSessionError::Storage)?
                        .unwrap_or_default(),
                ),
                MetadataPageQuery::Constraints {
                    schema_id,
                    relation_id,
                } => MetadataPageRows::Constraints(
                    storage
                        .discover_token_administrative_constraints(
                            token_id,
                            &secret,
                            *schema_id,
                            *relation_id,
                        )
                        .map_err(ClientSessionError::Storage)?
                        .unwrap_or_default(),
                ),
                MetadataPageQuery::Grants {
                    schema_id,
                    relation_id,
                } => MetadataPageRows::Grants(
                    storage
                        .discover_token_administrative_grants(
                            token_id,
                            &secret,
                            *schema_id,
                            *relation_id,
                        )
                        .map_err(ClientSessionError::Storage)?
                        .unwrap_or_default(),
                ),
                MetadataPageQuery::Policies {
                    schema_id,
                    relation_id,
                } => MetadataPageRows::Policies(
                    storage
                        .discover_token_administrative_policies(
                            token_id,
                            &secret,
                            *schema_id,
                            *relation_id,
                        )
                        .map_err(ClientSessionError::Storage)?
                        .unwrap_or_default(),
                ),
                MetadataPageQuery::Dependencies(object_id) => MetadataPageRows::Dependencies(
                    storage
                        .discover_token_administrative_dependencies(token_id, &secret, *object_id)
                        .map_err(ClientSessionError::Storage)?
                        .unwrap_or_default(),
                ),
            };
            if !current_snapshot
                .is_some_and(|snapshot| snapshot.rows.has_same_object_ids(&current_rows))
            {
                self.metadata_page_snapshot = None;
                return Err(ClientSessionError::InvalidControlPayload(
                    ClientControlError::InvalidMetadataCursor,
                ));
            }
            self.metadata_page_snapshot
                .take()
                .ok_or(ClientSessionError::InvalidControlPayload(
                    ClientControlError::InvalidMetadataCursor,
                ))?
        } else {
            self.metadata_page_snapshot = None;
            let rows = match &query {
                MetadataPageQuery::Schemas => MetadataPageRows::Schemas(
                    storage
                        .discover_token_authorized_schemas(token_id, &secret)
                        .map_err(ClientSessionError::Storage)?,
                ),
                MetadataPageQuery::Relations(schema_id) => MetadataPageRows::Relations(
                    storage
                        .discover_token_authorized_relations(token_id, &secret, *schema_id)
                        .map_err(ClientSessionError::Storage)?
                        .unwrap_or_default(),
                ),
                MetadataPageQuery::Columns {
                    schema_id,
                    relation_id,
                } => MetadataPageRows::Columns(
                    storage
                        .discover_token_authorized_columns(
                            token_id,
                            &secret,
                            *schema_id,
                            *relation_id,
                        )
                        .map_err(ClientSessionError::Storage)?
                        .unwrap_or_default(),
                ),
                MetadataPageQuery::Indexes {
                    schema_id,
                    relation_id,
                } => MetadataPageRows::Indexes(
                    storage
                        .discover_token_administrative_indexes(
                            token_id,
                            &secret,
                            *schema_id,
                            *relation_id,
                        )
                        .map_err(ClientSessionError::Storage)?
                        .unwrap_or_default(),
                ),
                MetadataPageQuery::Constraints {
                    schema_id,
                    relation_id,
                } => MetadataPageRows::Constraints(
                    storage
                        .discover_token_administrative_constraints(
                            token_id,
                            &secret,
                            *schema_id,
                            *relation_id,
                        )
                        .map_err(ClientSessionError::Storage)?
                        .unwrap_or_default(),
                ),
                MetadataPageQuery::Grants {
                    schema_id,
                    relation_id,
                } => MetadataPageRows::Grants(
                    storage
                        .discover_token_administrative_grants(
                            token_id,
                            &secret,
                            *schema_id,
                            *relation_id,
                        )
                        .map_err(ClientSessionError::Storage)?
                        .unwrap_or_default(),
                ),
                MetadataPageQuery::Policies {
                    schema_id,
                    relation_id,
                } => MetadataPageRows::Policies(
                    storage
                        .discover_token_administrative_policies(
                            token_id,
                            &secret,
                            *schema_id,
                            *relation_id,
                        )
                        .map_err(ClientSessionError::Storage)?
                        .unwrap_or_default(),
                ),
                MetadataPageQuery::Dependencies(object_id) => MetadataPageRows::Dependencies(
                    storage
                        .discover_token_administrative_dependencies(token_id, &secret, *object_id)
                        .map_err(ClientSessionError::Storage)?
                        .unwrap_or_default(),
                ),
            };
            ActiveMetadataPageSnapshot {
                query,
                rows,
                principal_id,
                role_ids,
                page_limit,
                next_index: 0,
                current_cursor: None,
                expires_at: std::time::Instant::now() + std::time::Duration::from_secs(60),
            }
        };

        let start = snapshot.next_index;
        let end = start
            .saturating_add(snapshot.page_limit)
            .min(snapshot.rows.len());
        let next_cursor = if end < snapshot.rows.len() {
            let mut token = [0; 16];
            loop {
                getrandom::fill(&mut token).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                token[0] = CLIENT_METADATA_CURSOR_KIND;
                if Some(token) != cursor {
                    break;
                }
            }
            Some(token)
        } else {
            None
        };
        let payload = encode_metadata_page(&snapshot, start, end, next_cursor)?;
        if payload.len() > CLIENT_MAX_CONTROL_BYTES {
            return Err(ClientSessionError::ResourceLimit);
        }

        if let Some(next_cursor) = next_cursor {
            snapshot.next_index = end;
            snapshot.current_cursor = Some(next_cursor);
            snapshot.expires_at = std::time::Instant::now() + std::time::Duration::from_secs(60);
            self.metadata_page_snapshot = Some(snapshot);
        } else {
            self.metadata_page_snapshot = None;
        }
        self.json_response(request_id, payload)
    }
}
