use super::json::*;
use super::types::*;
use super::*;
use crate::storage_manager::StorageManager;
use crate::transaction::TransactionError;
use zeroize::{Zeroize, Zeroizing};

impl ClientConnectionSession {
    pub(super) fn handle_admin_start(
        &mut self,
        storage: &StorageManager<'_>,
        request: &ClientFrame,
        actor_id: [u8; 16],
        operation_id: [u8; 16],
        retention_deadline_us: i64,
    ) -> Result<ClientFrame, ClientSessionError> {
        let Some(pending) = self.pending_admin_operation.as_ref() else {
            return Err(ClientSessionError::WrongMode);
        };
        if pending.operation_id != operation_id {
            return Err(ClientSessionError::WrongMode);
        }
        let pending = self
            .pending_admin_operation
            .take()
            .ok_or(ClientSessionError::WrongMode)?;
        let now_us = current_unix_micros().ok_or(ClientSessionError::UnsupportedRequest)?;
        let mut event_id = [0; 16];
        getrandom::fill(&mut event_id)
            .map_err(|_| ClientSessionError::Storage(TransactionError::RandomnessUnavailable))?;
        if event_id == [0; 16] {
            return Err(ClientSessionError::Storage(
                TransactionError::RandomnessUnavailable,
            ));
        }
        match pending.action {
            PendingAdminAction::ChangeRoleMembership {
                member_id,
                role_id,
                grant,
            } => {
                let sequence = storage
                    .set_role_membership(
                        operation_id,
                        actor_id,
                        crate::storage_manager::RoleMembershipChange {
                            member_id,
                            role_id,
                            grant,
                        },
                        retention_deadline_us,
                        crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::AuthorityChange,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    )
                    .map_err(ClientSessionError::MembershipLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"commit_sequence\":\"{sequence}\"}}",
                                encode_lower_hex_id(operation_id)
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::RenameIdentity { identity_id, name } => {
                let sequence = storage
                    .rename_identity(
                        operation_id,
                        actor_id,
                        identity_id,
                        &name,
                        retention_deadline_us,
                        crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::Ddl,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    )
                    .map_err(ClientSessionError::IdentityLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"commit_sequence\":\"{sequence}\"}}",
                                encode_lower_hex_id(operation_id)
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::DropIdentity { identity_id } => {
                let sequence = storage
                    .drop_identity(
                        operation_id,
                        actor_id,
                        identity_id,
                        retention_deadline_us,
                        crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::Ddl,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    )
                    .map_err(ClientSessionError::IdentityLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"commit_sequence\":\"{sequence}\"}}",
                                encode_lower_hex_id(operation_id)
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::SetUserLogin { user_id, enabled } => {
                let sequence = storage
                    .set_user_login_enabled(
                        operation_id,
                        actor_id,
                        user_id,
                        enabled,
                        retention_deadline_us,
                        crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::Ddl,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    )
                    .map_err(ClientSessionError::IdentityLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"commit_sequence\":\"{sequence}\"}}",
                                encode_lower_hex_id(operation_id)
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::ChangeSystemPrivilege {
                grantee_id,
                privilege,
                grant,
            } => {
                let sequence = storage
                    .set_system_privilege(
                        operation_id,
                        actor_id,
                        crate::storage_manager::SystemPrivilegeChange {
                            grantee_id,
                            privilege,
                            grant,
                        },
                        retention_deadline_us,
                        crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::AuthorityChange,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    )
                    .map_err(ClientSessionError::SystemPrivilegeLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"commit_sequence\":\"{sequence}\"}}",
                                encode_lower_hex_id(operation_id)
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::ChangeObjectGrant {
                grantee_id,
                object_id,
                privileges,
                target,
                entries,
                grant,
            } => {
                let request_audit_event = crate::audit::AuditEvent {
                    event_id,
                    actor_id: Some(actor_id),
                    correlation_id: operation_id,
                    occurred_at_us: now_us,
                    action: crate::audit::AuditAction::AuthorityChange,
                    result: crate::audit::AuditResult::Succeeded,
                    target_ids: Vec::new(),
                };
                let sequence = if let Some(entries) = entries {
                    storage.set_object_grant_entries_batch(
                        crate::storage_manager::SetObjectGrantEntriesBatchRequest {
                            invocation_id: operation_id,
                            authenticated_actor_id: actor_id,
                            grantee_id,
                            object_id,
                            entries,
                            grant,
                            retention_deadline_us,
                            audit_event: request_audit_event,
                        },
                    )
                } else {
                    storage.set_object_grants_batch(
                        crate::storage_manager::SetObjectGrantBatchRequest {
                            invocation_id: operation_id,
                            authenticated_actor_id: actor_id,
                            grantee_id,
                            object_id,
                            privileges,
                            target,
                            grant,
                            retention_deadline_us,
                            audit_event: request_audit_event,
                        },
                    )
                }
                .map_err(ClientSessionError::ObjectGrantLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"commit_sequence\":\"{sequence}\"}}",
                                encode_lower_hex_id(operation_id)
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::CreateSchema { name, owner_id } => {
                let owner_id = owner_id.unwrap_or(actor_id);
                let created = storage
                    .create_schema(
                        operation_id,
                        actor_id,
                        &name,
                        owner_id,
                        retention_deadline_us,
                        crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::Ddl,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    )
                    .map_err(ClientSessionError::SchemaLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"schema_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(created.schema_id),
                                created.commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::DropSchema { schema_id } => {
                let dropped = storage
                    .drop_schema(
                        operation_id,
                        actor_id,
                        schema_id,
                        retention_deadline_us,
                        crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::Ddl,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    )
                    .map_err(ClientSessionError::SchemaLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"schema_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(dropped.schema_id),
                                dropped.commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::SetSchemaOwner {
                schema_id,
                owner_id,
            } => {
                let commit_sequence = storage
                    .set_schema_owner(
                        operation_id,
                        actor_id,
                        schema_id,
                        owner_id,
                        retention_deadline_us,
                        crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::Ddl,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    )
                    .map_err(ClientSessionError::SchemaLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"schema_id\":\"{}\",\"owner_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(schema_id),
                                encode_lower_hex_id(owner_id),
                                commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::RenameSchema { schema_id, name } => {
                let commit_sequence = storage
                    .rename_schema(
                        operation_id,
                        actor_id,
                        schema_id,
                        &name,
                        retention_deadline_us,
                        crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::Ddl,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    )
                    .map_err(ClientSessionError::SchemaLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"schema_id\":\"{}\",\"name\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(schema_id),
                                name,
                                commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::DropTable { table_id } => {
                let dropped = storage
                    .drop_table(
                        operation_id,
                        actor_id,
                        table_id,
                        retention_deadline_us,
                        crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::Ddl,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    )
                    .map_err(ClientSessionError::TableLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"table_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(dropped.table_id),
                                dropped.commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::TruncateTable { table_id } => {
                let truncated = storage
                    .truncate_table(
                        operation_id,
                        actor_id,
                        table_id,
                        retention_deadline_us,
                        crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::Ddl,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    )
                    .map_err(ClientSessionError::TableLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"table_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(truncated.table_id),
                                truncated.commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::DropForeignKey { foreign_key_id } => {
                let commit_sequence = storage
                    .drop_foreign_key(
                        operation_id,
                        actor_id,
                        foreign_key_id,
                        retention_deadline_us,
                        crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::Ddl,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    )
                    .map_err(ClientSessionError::ForeignKeyLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"foreign_key_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(foreign_key_id),
                                commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::DropCheckConstraint { constraint_id } => {
                let commit_sequence = storage
                    .drop_check_constraint(
                        operation_id,
                        actor_id,
                        constraint_id,
                        retention_deadline_us,
                        crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::Ddl,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    )
                    .map_err(ClientSessionError::ConstraintLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"constraint_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(constraint_id),
                                commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::DropPrimaryKey { constraint_id } => {
                let commit_sequence = storage
                    .drop_primary_key(
                        operation_id,
                        actor_id,
                        constraint_id,
                        retention_deadline_us,
                        crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::Ddl,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    )
                    .map_err(ClientSessionError::ConstraintLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"constraint_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(constraint_id),
                                commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::DropUniqueConstraint { constraint_id } => {
                let commit_sequence = storage
                    .drop_unique_constraint(
                        operation_id,
                        actor_id,
                        constraint_id,
                        retention_deadline_us,
                        crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::Ddl,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    )
                    .map_err(ClientSessionError::ConstraintLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"constraint_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(constraint_id),
                                commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::CreateForeignKey {
                source_table_id,
                source_column_ids,
                target_constraint_id,
                name,
            } => {
                let create_request = crate::storage_manager::CreateForeignKeyRequest {
                    invocation_id: operation_id,
                    authenticated_actor_id: actor_id,
                    source_table_id,
                    source_column_ids,
                    target_constraint_id,
                    retention_deadline_us,
                    audit_event: crate::audit::AuditEvent {
                        event_id,
                        actor_id: Some(actor_id),
                        correlation_id: operation_id,
                        occurred_at_us: now_us,
                        action: crate::audit::AuditAction::Ddl,
                        result: crate::audit::AuditResult::Succeeded,
                        target_ids: Vec::new(),
                    },
                };
                let created = match name {
                    Some(name) => storage.create_named_foreign_key(create_request, name),
                    None => storage.create_foreign_key(create_request),
                }
                .map_err(ClientSessionError::ForeignKeyCreateLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"foreign_key_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(created.foreign_key_id),
                                created.commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::CreateUniqueConstraint {
                table_id,
                column_ids,
                name,
            } => {
                let create_request = crate::storage_manager::CreateUniqueConstraintRequest {
                    invocation_id: operation_id,
                    authenticated_actor_id: actor_id,
                    table_id,
                    column_ids,
                    retention_deadline_us,
                    audit_event: crate::audit::AuditEvent {
                        event_id,
                        actor_id: Some(actor_id),
                        correlation_id: operation_id,
                        occurred_at_us: now_us,
                        action: crate::audit::AuditAction::Ddl,
                        result: crate::audit::AuditResult::Succeeded,
                        target_ids: Vec::new(),
                    },
                };
                let created = match name {
                    Some(name) => storage.create_named_unique_constraint(create_request, name),
                    None => storage.create_unique_constraint(create_request),
                }
                .map_err(ClientSessionError::ConstraintLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"constraint_id\":\"{}\",\"index_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(created.constraint_id),
                                encode_lower_hex_id(created.index_id),
                                created.commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::CreateIndex {
                table_id,
                name,
                column_ids,
                unique,
            } => {
                let created = storage
                    .create_index(crate::storage_manager::CreateIndexRequest {
                        invocation_id: operation_id,
                        authenticated_actor_id: actor_id,
                        table_id,
                        name,
                        column_ids,
                        unique,
                        retention_deadline_us,
                        audit_event: crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::Ddl,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    })
                    .map_err(ClientSessionError::IndexLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"index_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(created.index_id),
                                created.commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::DropIndex { index_id } => {
                let dropped = storage
                    .drop_index(crate::storage_manager::DropIndexRequest {
                        invocation_id: operation_id,
                        authenticated_actor_id: actor_id,
                        index_id,
                        retention_deadline_us,
                        audit_event: crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::Ddl,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    })
                    .map_err(ClientSessionError::IndexLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"index_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(dropped.index_id),
                                dropped.commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::RebuildIndex {
                index_id,
                column_ids,
                unique,
            } => {
                let rebuilt = storage
                    .rebuild_index(crate::storage_manager::RebuildIndexRequest {
                        invocation_id: operation_id,
                        authenticated_actor_id: actor_id,
                        index_id,
                        column_ids,
                        unique,
                        retention_deadline_us,
                        audit_event: crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::Ddl,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    })
                    .map_err(ClientSessionError::IndexLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"index_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(rebuilt.index_id),
                                rebuilt.commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::RenameIndex { index_id, name } => {
                let renamed = storage
                    .rename_index(crate::storage_manager::RenameIndexRequest {
                        invocation_id: operation_id,
                        authenticated_actor_id: actor_id,
                        index_id,
                        name,
                        retention_deadline_us,
                        audit_event: crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::Ddl,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    })
                    .map_err(ClientSessionError::IndexLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"index_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(renamed.index_id),
                                renamed.commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::CreatePrimaryKey {
                table_id,
                column_ids,
                name,
            } => {
                let create_request = crate::storage_manager::CreatePrimaryKeyRequest {
                    invocation_id: operation_id,
                    authenticated_actor_id: actor_id,
                    table_id,
                    column_ids,
                    retention_deadline_us,
                    audit_event: crate::audit::AuditEvent {
                        event_id,
                        actor_id: Some(actor_id),
                        correlation_id: operation_id,
                        occurred_at_us: now_us,
                        action: crate::audit::AuditAction::Ddl,
                        result: crate::audit::AuditResult::Succeeded,
                        target_ids: Vec::new(),
                    },
                };
                let created = match name {
                    Some(name) => storage.create_named_primary_key(create_request, name),
                    None => storage.create_primary_key(create_request),
                }
                .map_err(ClientSessionError::ConstraintLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"constraint_id\":\"{}\",\"index_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(created.constraint_id),
                                encode_lower_hex_id(created.index_id),
                                created.commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::CreateCheckConstraint {
                table_id,
                name,
                expression,
                dependency_column_ids,
            } => {
                let created = storage
                    .create_check_constraint(crate::storage_manager::CreateCheckConstraintRequest {
                        invocation_id: operation_id,
                        authenticated_actor_id: actor_id,
                        table_id,
                        name,
                        expression,
                        dependency_column_ids,
                        retention_deadline_us,
                        audit_event: crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::Ddl,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    })
                    .map_err(ClientSessionError::ConstraintLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"constraint_id\":\"{}\",\"expression_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(created.constraint_id),
                                encode_lower_hex_id(created.expression_id),
                                created.commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::CreatePolicy {
                replaced_policy_id,
                table_id,
                name,
                kind,
                operation,
                effect,
                recipients,
                target_column_ids,
                using_expression,
                with_check_expression,
            } => {
                let policy_request = crate::storage_manager::CreatePolicyRequest {
                    invocation_id: operation_id,
                    authenticated_actor_id: actor_id,
                    table_id,
                    name,
                    kind,
                    operation,
                    effect,
                    recipients,
                    target_column_ids,
                    using_expression,
                    with_check_expression,
                    retention_deadline_us,
                    audit_event: crate::audit::AuditEvent {
                        event_id,
                        actor_id: Some(actor_id),
                        correlation_id: operation_id,
                        occurred_at_us: now_us,
                        action: crate::audit::AuditAction::Ddl,
                        result: crate::audit::AuditResult::Succeeded,
                        target_ids: Vec::new(),
                    },
                };
                let created = match replaced_policy_id {
                    Some(policy_id) => storage.replace_policy(policy_request, policy_id),
                    None => storage.create_policy(policy_request),
                }
                .map_err(ClientSessionError::PolicyLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"policy_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(created.policy_id),
                                created.commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::DropPolicy { policy_id } => {
                let commit_sequence = storage
                    .drop_policy(crate::storage_manager::DropPolicyRequest {
                        invocation_id: operation_id,
                        authenticated_actor_id: actor_id,
                        policy_id,
                        retention_deadline_us,
                        audit_event: crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::Ddl,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    })
                    .map_err(ClientSessionError::PolicyLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"policy_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(policy_id),
                                commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::RenameTable { table_id, name } => {
                let commit_sequence = storage
                    .rename_table(
                        operation_id,
                        actor_id,
                        table_id,
                        &name,
                        retention_deadline_us,
                        crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::Ddl,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    )
                    .map_err(ClientSessionError::TableLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"table_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(table_id),
                                commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::SetTableOwner { table_id, owner_id } => {
                let commit_sequence = storage
                    .set_table_owner(
                        operation_id,
                        actor_id,
                        table_id,
                        owner_id,
                        retention_deadline_us,
                        crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::Ddl,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    )
                    .map_err(ClientSessionError::TableLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"table_id\":\"{}\",\"owner_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(table_id),
                                encode_lower_hex_id(owner_id),
                                commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::RenameColumn {
                table_id,
                column_id,
                name,
            } => {
                let commit_sequence = storage
                    .rename_column(
                        operation_id,
                        actor_id,
                        table_id,
                        column_id,
                        &name,
                        retention_deadline_us,
                        crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::Ddl,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    )
                    .map_err(ClientSessionError::TableLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"table_id\":\"{}\",\"column_id\":\"{}\",\"name\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(table_id),
                                encode_lower_hex_id(column_id),
                                name,
                                commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::SetColumnNullable {
                table_id,
                column_id,
                nullable,
            } => {
                let commit_sequence = storage
                    .set_column_nullable(
                        operation_id,
                        actor_id,
                        table_id,
                        column_id,
                        nullable,
                        retention_deadline_us,
                        crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::Ddl,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    )
                    .map_err(ClientSessionError::TableLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"table_id\":\"{}\",\"column_id\":\"{}\",\"nullable\":{},\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(table_id),
                                encode_lower_hex_id(column_id),
                                nullable,
                                commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::WidenVarcharColumn {
                table_id,
                column_id,
                max_chars,
            } => {
                let commit_sequence = storage
                    .widen_varchar_column(
                        operation_id,
                        actor_id,
                        table_id,
                        column_id,
                        max_chars,
                        retention_deadline_us,
                        crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::Ddl,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    )
                    .map_err(ClientSessionError::TableLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"table_id\":\"{}\",\"column_id\":\"{}\",\"max_chars\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(table_id),
                                encode_lower_hex_id(column_id),
                                max_chars,
                                commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::AlterColumnDefault {
                table_id,
                column_id,
                default,
            } => {
                let commit_sequence = storage
                    .alter_column_default(crate::storage_manager::AlterColumnDefaultRequest {
                        invocation_id: operation_id,
                        authenticated_actor_id: actor_id,
                        table_id,
                        column_id,
                        default,
                        retention_deadline_us,
                        audit_event: crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::Ddl,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    })
                    .map_err(ClientSessionError::TableLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"table_id\":\"{}\",\"column_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(table_id),
                                encode_lower_hex_id(column_id),
                                commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::AddColumn { table_id, column } => {
                let added = storage
                    .add_table_column(crate::storage_manager::AddTableColumnRequest {
                        invocation_id: operation_id,
                        authenticated_actor_id: actor_id,
                        table_id,
                        column,
                        retention_deadline_us,
                        audit_event: crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::Ddl,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    })
                    .map_err(ClientSessionError::TableLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"table_id\":\"{}\",\"column_id\":\"{}\",\"schema_version\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(added.table_id),
                                encode_lower_hex_id(added.column_id),
                                added.schema_version,
                                added.commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::DropColumn {
                table_id,
                column_id,
            } => {
                let dropped = storage
                    .drop_column(
                        operation_id,
                        actor_id,
                        table_id,
                        column_id,
                        retention_deadline_us,
                        crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::Ddl,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    )
                    .map_err(ClientSessionError::TableLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"table_id\":\"{}\",\"column_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(dropped.table_id),
                                encode_lower_hex_id(dropped.column_id),
                                dropped.commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::CreateTable {
                schema_id,
                name,
                columns,
                primary_key_columns,
                primary_key_name,
                unique_constraints,
                unique_constraint_names,
                check_expressions,
                check_constraint_names,
                foreign_keys,
                foreign_key_names,
            } => {
                let create_request = crate::storage_manager::CreateTableRequest {
                    invocation_id: operation_id,
                    authenticated_actor_id: actor_id,
                    schema_id,
                    name,
                    columns,
                    retention_deadline_us,
                    audit_event: crate::audit::AuditEvent {
                        event_id,
                        actor_id: Some(actor_id),
                        correlation_id: operation_id,
                        occurred_at_us: now_us,
                        action: crate::audit::AuditAction::Ddl,
                        result: crate::audit::AuditResult::Succeeded,
                        target_ids: Vec::new(),
                    },
                };
                let created = storage
                    .create_table_with_named_constraints_and_foreign_keys(
                        create_request,
                        primary_key_columns.as_deref(),
                        primary_key_name,
                        &unique_constraints,
                        &unique_constraint_names,
                        &check_expressions,
                        &check_constraint_names,
                        &foreign_keys,
                        &foreign_key_names,
                    )
                    .map_err(ClientSessionError::TableLifecycle)?;
                let mut payload = format!(
                    "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"table_id\":\"{}\",\"column_ids\":[",
                    encode_lower_hex_id(operation_id),
                    encode_lower_hex_id(created.table_id),
                );
                for (index, column_id) in created.column_ids.iter().enumerate() {
                    if index != 0 {
                        payload.push(',');
                    }
                    payload.push('"');
                    payload.push_str(&encode_lower_hex_id(*column_id));
                    payload.push('"');
                }
                payload.push_str(&format!(
                    "],\"commit_sequence\":\"{}\"}}",
                    created.commit_sequence
                ));
                if payload.len() > CLIENT_MAX_CONTROL_BYTES {
                    return Err(ClientSessionError::ResourceLimit);
                }
                self.json_response(request.request_id, payload.into_bytes())
            }
            PendingAdminAction::CreateRole { name } => {
                let created = storage
                    .create_role(
                        operation_id,
                        actor_id,
                        &name,
                        retention_deadline_us,
                        crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::Ddl,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    )
                    .map_err(ClientSessionError::IdentityLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"role_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(created.role_id),
                                created.commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::CreateUser { name } => {
                let created = storage
                    .create_user(
                        operation_id,
                        actor_id,
                        &name,
                        retention_deadline_us,
                        crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::Ddl,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    )
                    .map_err(ClientSessionError::IdentityLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"user_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(created.user_id),
                                created.commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::CreateUserNoLogin { name } => {
                let created = storage
                    .create_user_no_login(
                        operation_id,
                        actor_id,
                        &name,
                        retention_deadline_us,
                        crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::Ddl,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: Vec::new(),
                        },
                    )
                    .map_err(ClientSessionError::IdentityLifecycle)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"user_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(created.user_id),
                                created.commit_sequence,
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::Revoke { token_id } => {
                let sequence = storage
                    .revoke_token(
                        operation_id,
                        actor_id,
                        token_id,
                        retention_deadline_us,
                        crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::TokenLifecycle,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: vec![token_id],
                        },
                    )
                    .map_err(ClientSessionError::TokenInventory)?;
                self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"commit_sequence\":\"{sequence}\"}}",
                                encode_lower_hex_id(operation_id)
                            )
                            .into_bytes(),
                        )
            }
            PendingAdminAction::Issue {
                user_id,
                label,
                expires_at_us,
            } => {
                let mut issued = storage
                    .issue_token(crate::storage_manager::TokenIssueRequest {
                        invocation_id: operation_id,
                        authenticated_actor_id: actor_id,
                        user_id,
                        label: &label,
                        expires_at_us,
                        retention_deadline_us,
                        audit_event: crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::TokenLifecycle,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: vec![user_id],
                        },
                    })
                    .map_err(ClientSessionError::TokenInventory)?;
                let secret = issued
                    .take_secret()
                    .ok_or(ClientSessionError::UnsupportedRequest)?;
                let mut secret = Zeroizing::new(secret);
                let secret_base64 = Zeroizing::new(encode_base64(secret.as_ref()));
                secret.zeroize();
                let mut payload = Zeroizing::new(format!(
                    "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"token_id\":\"{}\",\"token_secret\":\"{}\",\"commit_sequence\":\"{}\"}}",
                    encode_lower_hex_id(operation_id),
                    encode_lower_hex_id(issued.token_id),
                    secret_base64.as_str(),
                    issued.commit_sequence,
                ));
                let response_payload = payload.as_bytes().to_vec();
                payload.zeroize();
                self.json_response(request.request_id, response_payload)
            }
            PendingAdminAction::Rotate {
                old_token_id,
                label,
                expires_at_us,
            } => {
                let mut issued = storage
                    .rotate_token(crate::storage_manager::TokenRotationRequest {
                        invocation_id: operation_id,
                        authenticated_actor_id: actor_id,
                        old_token_id,
                        label: &label,
                        expires_at_us,
                        retention_deadline_us,
                        audit_event: crate::audit::AuditEvent {
                            event_id,
                            actor_id: Some(actor_id),
                            correlation_id: operation_id,
                            occurred_at_us: now_us,
                            action: crate::audit::AuditAction::TokenLifecycle,
                            result: crate::audit::AuditResult::Succeeded,
                            target_ids: vec![old_token_id],
                        },
                    })
                    .map_err(ClientSessionError::TokenInventory)?;
                let secret = issued
                    .take_secret()
                    .ok_or(ClientSessionError::UnsupportedRequest)?;
                let mut secret = Zeroizing::new(secret);
                let secret_base64 = Zeroizing::new(encode_base64(secret.as_ref()));
                secret.zeroize();
                let mut payload = Zeroizing::new(format!(
                    "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"token_id\":\"{}\",\"token_secret\":\"{}\",\"commit_sequence\":\"{}\"}}",
                    encode_lower_hex_id(operation_id),
                    encode_lower_hex_id(issued.token_id),
                    secret_base64.as_str(),
                    issued.commit_sequence
                ));
                let response_payload = payload.as_bytes().to_vec();
                payload.zeroize();
                self.json_response(request.request_id, response_payload)
            }
        }
    }
}
