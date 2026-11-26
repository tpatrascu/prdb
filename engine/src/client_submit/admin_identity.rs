use super::codec::*;
use super::response::*;
use super::*;
use std::collections::{HashMap, HashSet};
use zeroize::{Zeroize, Zeroizing};

impl AdminClient {
    /// Create a login identity. New users have no credential until a token is issued.
    pub fn create_user(&mut self, name: &str) -> Result<AdminIdentityResult, ClientSubmitError> {
        self.create_identity("users.create", name, "user_id")
    }

    /// Create a login identity with an explicit workload profile.
    pub fn create_user_with_profile(
        &mut self,
        name: &str,
        workload_profile: &str,
    ) -> Result<AdminIdentityResult, ClientSubmitError> {
        validate_admin_name(name)?;
        validate_admin_name(workload_profile)?;
        let (result, identity_id) = self.execute_admin_operation(
            "users.create",
            format!("{{\"name\":\"{name}\",\"workload_profile\":\"{workload_profile}\"}}"),
            Some("user_id"),
        )?;
        Ok(AdminIdentityResult {
            operation_id: result.operation_id,
            identity_id: identity_id.ok_or(ClientSubmitError::InvalidResponse)?,
            commit_sequence: result.commit_sequence,
        })
    }

    /// Create a user that cannot authenticate until a later LOGIN change.
    pub fn create_user_no_login(
        &mut self,
        name: &str,
    ) -> Result<AdminIdentityResult, ClientSubmitError> {
        self.create_identity("users.create_no_login", name, "user_id")
    }

    /// Create a NOLOGIN user with an explicit workload profile.
    pub fn create_user_no_login_with_profile(
        &mut self,
        name: &str,
        workload_profile: &str,
    ) -> Result<AdminIdentityResult, ClientSubmitError> {
        validate_admin_name(name)?;
        validate_admin_name(workload_profile)?;
        let (result, identity_id) = self.execute_admin_operation(
            "users.create_no_login",
            format!("{{\"name\":\"{name}\",\"workload_profile\":\"{workload_profile}\"}}"),
            Some("user_id"),
        )?;
        Ok(AdminIdentityResult {
            operation_id: result.operation_id,
            identity_id: identity_id.ok_or(ClientSubmitError::InvalidResponse)?,
            commit_sequence: result.commit_sequence,
        })
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

    /// Atomically revoke an active token and issue a replacement for the same
    /// user. The replacement bearer secret is returned once.
    pub fn rotate_token(
        &mut self,
        old_token_id: [u8; 16],
        label: &str,
        expires_at_us: i64,
    ) -> Result<AdminTokenResult, ClientSubmitError> {
        validate_admin_id(old_token_id)?;
        if label.is_empty() || label.len() > 128 || expires_at_us <= 0 {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let arguments = format!(
            "{{\"old_token_id\":\"{}\",\"label\":{},\"expires_at_us\":\"{expires_at_us}\"}}",
            lower_hex(&old_token_id),
            json_quote(label)
        );
        let prepare = format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"tokens.rotate\",\"arguments\":{arguments}}}"
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

    /// List non-secret metadata for one user's tokens. The server enforces
    /// self-inventory or current MANAGE_USERS/database-administrator authority.
    pub fn list_tokens_page(
        &mut self,
        user_id: [u8; 16],
        after_token_id: Option<[u8; 16]>,
    ) -> Result<AdminTokenPage, ClientSubmitError> {
        validate_admin_id(user_id)?;
        if after_token_id == Some([0; 16]) {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let after = after_token_id
            .map(|id| format!(",\"after_token_id\":\"{}\"", lower_hex(&id)))
            .unwrap_or_default();
        let request = format!(
            "{{\"phase\":\"read\",\"operation_version\":1,\"operation\":\"tokens.list\",\"arguments\":{{\"user_id\":\"{}\"{after}}}}}",
            lower_hex(&user_id)
        );
        let response = self.transport.exchange(10, request.into_bytes())?;
        let mut fields = parse_flat_response(&response.payload)?;
        if fields.len() != 2 {
            return Err(ClientSubmitError::InvalidResponse);
        }
        let records = fields
            .remove("tokens")
            .and_then(|value| value.as_array().map(<[ResponseValue]>::to_vec))
            .ok_or(ClientSubmitError::InvalidResponse)?;
        let next_after_token_id = match fields.remove("next_after_token_id") {
            Some(ResponseValue::Null) => None,
            Some(ResponseValue::String(value)) => {
                let mut wrapper = HashMap::new();
                wrapper.insert("token_id".to_owned(), ResponseValue::String(value));
                Some(response_id(&wrapper, "token_id")?)
            }
            _ => return Err(ClientSubmitError::InvalidResponse),
        };
        let mut tokens = Vec::with_capacity(records.len());
        let mut previous = after_token_id;
        for record in records {
            let ResponseValue::Object(mut record) = record else {
                return Err(ClientSubmitError::InvalidResponse);
            };
            if record.len() != 6 {
                return Err(ClientSubmitError::InvalidResponse);
            }
            let token_id = response_id(&record, "token_id")?;
            record.remove("token_id");
            let label = response_string(&mut record, "label")?;
            let issued_at_us = response_timestamp(&mut record, "issued_at_us")?;
            let expires_at_us = response_timestamp(&mut record, "expires_at_us")?;
            let revoked = match record.remove("revoked") {
                Some(ResponseValue::Boolean(value)) => value,
                _ => return Err(ClientSubmitError::InvalidResponse),
            };
            let revoked_at_us = match record.remove("revoked_at_us") {
                Some(ResponseValue::Null) => None,
                Some(ResponseValue::Number(value)) => {
                    Some(i64::try_from(value).map_err(|_| ClientSubmitError::InvalidResponse)?)
                }
                _ => return Err(ClientSubmitError::InvalidResponse),
            };
            if !record.is_empty()
                || token_id == [0; 16]
                || label.is_empty()
                || previous.is_some_and(|id| id >= token_id)
                || expires_at_us <= issued_at_us
                || revoked != revoked_at_us.is_some()
            {
                return Err(ClientSubmitError::InvalidResponse);
            }
            previous = Some(token_id);
            tokens.push(AdminTokenMetadata {
                token_id,
                label,
                issued_at_us,
                expires_at_us,
                revoked,
                revoked_at_us,
            });
        }
        if tokens.len() > 64
            || (next_after_token_id.is_some()
                && (tokens.len() != 64
                    || next_after_token_id != tokens.last().map(|token| token.token_id)))
        {
            return Err(ClientSubmitError::InvalidResponse);
        }
        Ok(AdminTokenPage {
            tokens,
            next_after_token_id,
        })
    }

    /// Read a bounded, administrator-bound page of non-secret AUD1 metadata.
    pub fn list_audit_events_page(
        &mut self,
        page_limit: usize,
        cursor: Option<&str>,
    ) -> Result<AdminAuditPage, ClientSubmitError> {
        if !(1..=crate::storage_manager::MAX_ADMIN_AUDIT_PAGE).contains(&page_limit)
            || cursor.is_some_and(|value| {
                crate::storage_manager::AdministrativeAuditCursor::from_wire(value).is_none()
            })
        {
            return Err(ClientSubmitError::InvalidMetadataPage);
        }
        let cursor_argument = cursor
            .map(|value| format!(",\"cursor\":\"{value}\""))
            .unwrap_or_default();
        let request = format!(
            "{{\"phase\":\"read\",\"operation_version\":1,\"operation\":\"audit.events.list\",\"arguments\":{{\"limit\":\"{page_limit}\"{cursor_argument}}}}}"
        );
        let response = self.transport.exchange(10, request.into_bytes())?;
        let mut fields = parse_flat_response(&response.payload)?;
        if fields.len() != 2 {
            return Err(ClientSubmitError::InvalidResponse);
        }
        let rows = fields
            .remove("events")
            .and_then(|value| value.as_array().map(<[ResponseValue]>::to_vec))
            .ok_or(ClientSubmitError::InvalidResponse)?;
        if rows.len() > page_limit {
            return Err(ClientSubmitError::InvalidResponse);
        }
        let mut events = Vec::with_capacity(rows.len());
        let mut previous_id: Option<[u8; 16]> = None;
        for row in rows {
            let ResponseValue::Object(mut row) = row else {
                return Err(ClientSubmitError::InvalidResponse);
            };
            let event_id = response_id(&row, "event_id")?;
            row.remove("event_id");
            if previous_id.is_some_and(|previous| previous >= event_id) {
                return Err(ClientSubmitError::InvalidResponse);
            }
            previous_id = Some(event_id);
            let actor_id = match row.remove("actor_id") {
                Some(ResponseValue::Null) => None,
                Some(ResponseValue::String(value)) => {
                    let mut wrapper = HashMap::new();
                    wrapper.insert("actor_id".to_owned(), ResponseValue::String(value));
                    Some(response_id(&wrapper, "actor_id")?)
                }
                _ => return Err(ClientSubmitError::InvalidResponse),
            };
            let correlation_id = response_id(&row, "correlation_id")?;
            row.remove("correlation_id");
            let occurred_at_us = response_u64(&mut row, "occurred_at_us")?
                .and_then(|value| i64::try_from(value).ok())
                .filter(|value| *value > 0)
                .ok_or(ClientSubmitError::InvalidResponse)?;
            let action = response_u64(&mut row, "action")?
                .and_then(|value| u16::try_from(value).ok())
                .filter(|value| (1..=5).contains(value))
                .ok_or(ClientSubmitError::InvalidResponse)?;
            let result = response_u64(&mut row, "result")?
                .and_then(|value| u16::try_from(value).ok())
                .filter(|value| (1..=4).contains(value))
                .ok_or(ClientSubmitError::InvalidResponse)?;
            let target_ids = response_id_array(&mut row, "target_ids")?;
            if target_ids.windows(2).any(|pair| pair[0] >= pair[1]) || !row.is_empty() {
                return Err(ClientSubmitError::InvalidResponse);
            }
            events.push(AdminAuditEvent {
                event_id,
                actor_id,
                correlation_id,
                occurred_at_us,
                action,
                result,
                target_ids,
            });
        }
        let next_cursor = match fields.remove("next_cursor") {
            Some(ResponseValue::Null) => None,
            Some(ResponseValue::String(value))
                if crate::storage_manager::AdministrativeAuditCursor::from_wire(&value)
                    .is_some() =>
            {
                Some(value)
            }
            _ => return Err(ClientSubmitError::InvalidResponse),
        };
        if next_cursor.is_some() && events.is_empty() {
            return Err(ClientSubmitError::InvalidResponse);
        }
        Ok(AdminAuditPage {
            events,
            next_cursor,
        })
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

    pub fn grant_object_privilege(
        &mut self,
        grantee_id: [u8; 16],
        object_id: [u8; 16],
        privilege: AdminObjectPrivilege,
        target: AdminObjectGrantTarget,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        self.change_object_privilege("objects.grant", grantee_id, object_id, privilege, target)
    }

    pub fn revoke_object_privilege(
        &mut self,
        grantee_id: [u8; 16],
        object_id: [u8; 16],
        privilege: AdminObjectPrivilege,
        target: AdminObjectGrantTarget,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        self.change_object_privilege("objects.revoke", grantee_id, object_id, privilege, target)
    }

    pub(super) fn change_object_privilege(
        &mut self,
        operation: &str,
        grantee_id: [u8; 16],
        object_id: [u8; 16],
        privilege: AdminObjectPrivilege,
        target: AdminObjectGrantTarget,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        self.change_object_privileges(operation, grantee_id, object_id, vec![privilege], target)
    }

    pub(super) fn change_object_privileges(
        &mut self,
        operation: &str,
        grantee_id: [u8; 16],
        object_id: [u8; 16],
        privileges: Vec<AdminObjectPrivilege>,
        target: AdminObjectGrantTarget,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(grantee_id)?;
        validate_admin_id(object_id)?;
        if privileges.is_empty()
            || privileges.len() > 11
            || privileges
                .iter()
                .enumerate()
                .any(|(i, value)| privileges[..i].contains(value))
        {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let privilege_json = privileges
            .iter()
            .map(|privilege| format!("\"{}\"", privilege.as_wire_name()))
            .collect::<Vec<_>>()
            .join(",");
        let (operation, privilege_field) = if privileges.len() == 1 {
            (
                operation,
                format!("\"privilege\":\"{}\"", privileges[0].as_wire_name()),
            )
        } else {
            let batch_operation = match operation {
                "objects.grant" => "objects.grant_batch",
                "objects.revoke" => "objects.revoke_batch",
                _ => return Err(ClientSubmitError::InvalidAdministrativeRequest),
            };
            (
                batch_operation,
                format!("\"privileges\":[{privilege_json}]"),
            )
        };
        let arguments = match target {
            AdminObjectGrantTarget::Object => format!(
                "{{\"grantee_id\":\"{}\",\"object_id\":\"{}\",{},\"target_scope\":\"{}\",\"target_id\":\"{}\"}}",
                lower_hex(&grantee_id),
                lower_hex(&object_id),
                privilege_field,
                "object",
                lower_hex(&object_id)
            ),
            AdminObjectGrantTarget::Column(column_id) => {
                validate_admin_id(column_id)?;
                format!(
                    "{{\"grantee_id\":\"{}\",\"object_id\":\"{}\",{},\"target_scope\":\"column\",\"target_id\":\"{}\"}}",
                    lower_hex(&grantee_id),
                    lower_hex(&object_id),
                    privilege_field,
                    lower_hex(&column_id)
                )
            }
            AdminObjectGrantTarget::Columns(column_ids) => {
                if !(2..=64).contains(&column_ids.len()) {
                    return Err(ClientSubmitError::InvalidAdministrativeRequest);
                }
                let mut unique = HashSet::new();
                let mut encoded_ids = Vec::with_capacity(column_ids.len());
                for column_id in column_ids {
                    validate_admin_id(column_id)?;
                    if !unique.insert(column_id) {
                        return Err(ClientSubmitError::InvalidAdministrativeRequest);
                    }
                    encoded_ids.push(lower_hex(&column_id));
                }
                let encoded_ids = encoded_ids
                    .iter()
                    .map(|column_id| format!("\"{column_id}\""))
                    .collect::<Vec<_>>()
                    .join(",");
                format!(
                    "{{\"grantee_id\":\"{}\",\"object_id\":\"{}\",{},\"target_scope\":\"columns\",\"target_ids\":[{}]}}",
                    lower_hex(&grantee_id),
                    lower_hex(&object_id),
                    privilege_field,
                    encoded_ids
                )
            }
            AdminObjectGrantTarget::AllColumns => format!(
                "{{\"grantee_id\":\"{}\",\"object_id\":\"{}\",{},\"target_scope\":\"all_columns\"}}",
                lower_hex(&grantee_id),
                lower_hex(&object_id),
                privilege_field
            ),
        };
        self.execute_admin_operation(operation, arguments, None)
            .map(|(result, _)| result)
    }

    /// Atomically change privileges that can each have a different target scope.
    pub fn change_object_grant_entries(
        &mut self,
        operation: &str,
        grantee_id: [u8; 16],
        object_id: [u8; 16],
        entries: Vec<AdminObjectGrantEntry>,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(grantee_id)?;
        validate_admin_id(object_id)?;
        if !matches!(operation, "objects.grant" | "objects.revoke")
            || entries.is_empty()
            || entries.len() > 64
        {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let mut encoded = Vec::with_capacity(entries.len());
        for (index, entry) in entries.iter().enumerate() {
            if entries[..index].iter().any(|previous| {
                previous.privilege == entry.privilege && previous.target == entry.target
            }) {
                return Err(ClientSubmitError::InvalidAdministrativeRequest);
            }
            let target = match &entry.target {
                AdminObjectGrantTarget::Object => format!("object:{}", lower_hex(&object_id)),
                AdminObjectGrantTarget::Column(id) => {
                    validate_admin_id(*id)?;
                    format!("column:{}", lower_hex(id))
                }
                AdminObjectGrantTarget::Columns(ids) => {
                    if !(2..=64).contains(&ids.len()) {
                        return Err(ClientSubmitError::InvalidAdministrativeRequest);
                    }
                    let mut unique = HashSet::new();
                    for id in ids {
                        validate_admin_id(*id)?;
                        if !unique.insert(*id) {
                            return Err(ClientSubmitError::InvalidAdministrativeRequest);
                        }
                    }
                    format!(
                        "columns:{}",
                        ids.iter()
                            .map(|id| lower_hex(id))
                            .collect::<Vec<_>>()
                            .join("+")
                    )
                }
                AdminObjectGrantTarget::AllColumns => "all_columns:".to_owned(),
            };
            encoded.push(format!("{}@{target}", entry.privilege.as_wire_name()));
        }
        let operation = if operation == "objects.grant" {
            "objects.grant_entries"
        } else {
            "objects.revoke_entries"
        };
        self.execute_admin_operation(
            operation,
            format!(
                "{{\"grantee_id\":\"{}\",\"object_id\":\"{}\",\"entries\":\"{}\"}}",
                lower_hex(&grantee_id),
                lower_hex(&object_id),
                encoded.join(";")
            ),
            None,
        )
        .map(|(result, _)| result)
    }

    pub(super) fn change_system_privilege(
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
}
