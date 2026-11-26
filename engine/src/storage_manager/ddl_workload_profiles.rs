use super::StorageManager;
use super::catalog_helpers::{
    dependency_catalog_key, id_catalog_key, identity_object_catalog_key,
    is_normalized_principal_name, principal_catalog_key, retired_object_id_catalog_key,
};
use super::ddl_identity_grants::IdentityLifecycleError;
use crate::audit::{AuditAction, AuditEvent, AuditResult};
use crate::catalog::{
    CatalogField, WireType, decode_registered_envelope, encode_envelope, family_schema,
};
use crate::commit::{CatalogWrite, CommitError};
use crate::schema::CATALOG;
use crate::workload_profile::{
    WORKLOAD_PROFILE_LIMITS_VERSION, WorkloadProfile, WorkloadProfileLimits,
};
use redb::ReadableDatabase;

fn workload_profile_catalog_key(profile_id: [u8; 16]) -> Vec<u8> {
    id_catalog_key(18, profile_id)
}

fn decode_workload_profile_record(
    profile_id: [u8; 16],
    bytes: &[u8],
) -> Result<WorkloadProfile, IdentityLifecycleError> {
    let record = decode_registered_envelope(bytes).map_err(|_| IdentityLifecycleError::Storage)?;
    if record.family_code != 18 {
        return Err(IdentityLifecycleError::Storage);
    }
    let name = record
        .fields
        .iter()
        .find(|f| f.tag == 1 && f.wire_type == WireType::Text)
        .and_then(|f| std::str::from_utf8(&f.payload).ok())
        .ok_or(IdentityLifecycleError::Storage)?
        .to_owned();
    let definition_epoch = record
        .fields
        .iter()
        .find(|f| f.tag == 2 && f.wire_type == WireType::U64)
        .and_then(|f| <[u8; 8]>::try_from(f.payload.as_slice()).ok())
        .map(u64::from_le_bytes)
        .filter(|e| *e != 0)
        .ok_or(IdentityLifecycleError::Storage)?;
    let is_builtin = record
        .fields
        .iter()
        .find(|f| f.tag == 3 && f.wire_type == WireType::Bool)
        .and_then(|f| f.payload.first().copied())
        .map(|b| b != 0)
        .ok_or(IdentityLifecycleError::Storage)?;
    let limits_version = record
        .fields
        .iter()
        .find(|f| f.tag == 4 && f.wire_type == WireType::U32)
        .and_then(|f| <[u8; 4]>::try_from(f.payload.as_slice()).ok())
        .map(u32::from_le_bytes)
        .ok_or(IdentityLifecycleError::Storage)?;
    if limits_version != WORKLOAD_PROFILE_LIMITS_VERSION {
        return Err(IdentityLifecycleError::Storage);
    }
    let payload = record
        .fields
        .iter()
        .find(|f| f.tag == 5 && f.wire_type == WireType::Bytes)
        .ok_or(IdentityLifecycleError::Storage)?;
    let limits = WorkloadProfileLimits::decode(&payload.payload)
        .map_err(|_| IdentityLifecycleError::Storage)?;
    Ok(WorkloadProfile {
        profile_id,
        name,
        definition_epoch,
        is_builtin,
        limits,
    })
}

fn encode_workload_profile_payload(
    key: &[u8],
    profile: &WorkloadProfile,
) -> Result<Vec<u8>, IdentityLifecycleError> {
    profile
        .limits
        .validate()
        .map_err(|_| IdentityLifecycleError::InvalidRequest)?;
    encode_envelope(
        &family_schema(18).ok_or(IdentityLifecycleError::Storage)?,
        key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Text,
                payload: profile.name.as_bytes().to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U64,
                payload: profile.definition_epoch.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Bool,
                payload: vec![u8::from(profile.is_builtin)],
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::U32,
                payload: WORKLOAD_PROFILE_LIMITS_VERSION.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::Bytes,
                payload: profile.limits.encode(),
            },
        ],
    )
    .map_err(|_| IdentityLifecycleError::InvalidRequest)
}

impl<'db> StorageManager<'db> {
    pub fn load_workload_profile(
        &self,
        profile_id: [u8; 16],
    ) -> Result<WorkloadProfile, IdentityLifecycleError> {
        let key = workload_profile_catalog_key(profile_id);
        let read = self
            .database
            .begin_read()
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let value = catalog
            .get(key.as_slice())
            .map_err(|_| IdentityLifecycleError::Storage)?
            .ok_or(IdentityLifecycleError::InvalidRequest)?;
        decode_workload_profile_record(profile_id, value.value())
    }

    pub fn resolve_workload_profile_by_name(
        &self,
        name: &str,
    ) -> Result<WorkloadProfile, IdentityLifecycleError> {
        if !is_normalized_principal_name(name) {
            return Err(IdentityLifecycleError::InvalidRequest);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let prefix = 18u16.to_be_bytes();
        for entry in catalog
            .range(prefix.as_slice()..)
            .map_err(|_| IdentityLifecycleError::Storage)?
        {
            let (key, value) = entry.map_err(|_| IdentityLifecycleError::Storage)?;
            let key_bytes = key.value();
            if !key_bytes.starts_with(&prefix) {
                break;
            }
            if key_bytes.len() != 19 {
                continue;
            }
            let profile_id: [u8; 16] = key_bytes[3..19]
                .try_into()
                .map_err(|_| IdentityLifecycleError::Storage)?;
            let profile = decode_workload_profile_record(profile_id, value.value())?;
            if profile.name == name {
                return Ok(profile);
            }
        }
        Err(IdentityLifecycleError::InvalidRequest)
    }

    pub fn list_workload_profiles(&self) -> Result<Vec<WorkloadProfile>, IdentityLifecycleError> {
        let read = self
            .database
            .begin_read()
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let prefix = 18u16.to_be_bytes();
        let mut profiles = Vec::new();
        for entry in catalog
            .range(prefix.as_slice()..)
            .map_err(|_| IdentityLifecycleError::Storage)?
        {
            let (key, value) = entry.map_err(|_| IdentityLifecycleError::Storage)?;
            let key_bytes = key.value();
            if !key_bytes.starts_with(&prefix) {
                break;
            }
            if key_bytes.len() != 19 {
                continue;
            }
            let profile_id: [u8; 16] = key_bytes[3..19]
                .try_into()
                .map_err(|_| IdentityLifecycleError::Storage)?;
            profiles.push(decode_workload_profile_record(profile_id, value.value())?);
        }
        Ok(profiles)
    }

    pub fn create_workload_profile(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        name: &str,
        limits: WorkloadProfileLimits,
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<([u8; 16], u64), IdentityLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || retention_deadline_us <= 0
            || !is_normalized_principal_name(name)
        {
            return Err(IdentityLifecycleError::InvalidRequest);
        }
        limits
            .validate()
            .map_err(|_| IdentityLifecycleError::InvalidRequest)?;
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(IdentityLifecycleError::InvalidAudit);
        }

        let profile_id = self.generate_identity_id()?;
        let profile_key = workload_profile_catalog_key(profile_id);
        let object_key = identity_object_catalog_key(profile_id);
        let profile = WorkloadProfile {
            profile_id,
            name: name.to_owned(),
            definition_epoch: 1,
            is_builtin: false,
            limits,
        };
        let profile_payload = encode_workload_profile_payload(&profile_key, &profile)?;
        let object_payload = encode_envelope(
            &family_schema(2).ok_or(IdentityLifecycleError::Storage)?,
            &object_key,
            &[
                CatalogField {
                    tag: 1,
                    wire_type: WireType::U16,
                    payload: 11u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::Text,
                    payload: name.as_bytes().to_vec(),
                },
                CatalogField {
                    tag: 4,
                    wire_type: WireType::Id,
                    payload: crate::bootstrap::database_admin_role_id().to_vec(),
                },
                CatalogField {
                    tag: 5,
                    wire_type: WireType::U64,
                    payload: 1u64.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 6,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
            ],
        )
        .map_err(|_| IdentityLifecycleError::InvalidRequest)?;
        audit_event.target_ids = vec![profile_id];
        let writes = [
            CatalogWrite {
                key: profile_key,
                expected_value: None,
                payload: Some(profile_payload),
            },
            CatalogWrite {
                key: object_key,
                expected_value: None,
                payload: Some(object_payload),
            },
        ];
        let commit_sequence = self
            .commit_authorized_identity_ddl(
                invocation_id,
                authenticated_actor_id,
                retention_deadline_us,
                &writes,
                &[audit_event],
            )
            .map_err(|error| match error {
                CommitError::DdlAuthorizationDenied => IdentityLifecycleError::Unauthorized,
                error => IdentityLifecycleError::Commit(error),
            })?;
        Ok((profile_id, commit_sequence))
    }

    pub fn alter_workload_profile(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        profile_id: [u8; 16],
        settings: &[(String, u64)],
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, IdentityLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || profile_id == [0; 16]
            || settings.is_empty()
            || retention_deadline_us <= 0
        {
            return Err(IdentityLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(IdentityLifecycleError::InvalidAudit);
        }

        let profile_key = workload_profile_catalog_key(profile_id);
        let read = self
            .database
            .begin_read()
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let current_bytes = catalog
            .get(profile_key.as_slice())
            .map_err(|_| IdentityLifecycleError::Storage)?
            .map(|v| v.value().to_vec())
            .ok_or(IdentityLifecycleError::InvalidRequest)?;
        drop(catalog);
        drop(read);

        let mut profile = decode_workload_profile_record(profile_id, &current_bytes)?;
        for (k, v) in settings {
            profile
                .limits
                .set_field(k, *v)
                .map_err(|_| IdentityLifecycleError::InvalidRequest)?;
        }
        profile
            .limits
            .validate()
            .map_err(|_| IdentityLifecycleError::InvalidRequest)?;
        profile.definition_epoch = profile
            .definition_epoch
            .checked_add(1)
            .ok_or(IdentityLifecycleError::InvalidRequest)?;
        let next_payload = encode_workload_profile_payload(&profile_key, &profile)?;
        audit_event.target_ids = vec![profile_id];
        let writes = [CatalogWrite {
            key: profile_key,
            expected_value: Some(current_bytes),
            payload: Some(next_payload),
        }];
        self.commit_authorized_identity_ddl(
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &writes,
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => IdentityLifecycleError::Unauthorized,
            error => IdentityLifecycleError::Commit(error),
        })
    }

    pub fn rename_workload_profile(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        profile_id: [u8; 16],
        new_name: &str,
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, IdentityLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || profile_id == [0; 16]
            || retention_deadline_us <= 0
            || !is_normalized_principal_name(new_name)
        {
            return Err(IdentityLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(IdentityLifecycleError::InvalidAudit);
        }
        let profile_key = workload_profile_catalog_key(profile_id);
        let object_key = identity_object_catalog_key(profile_id);
        let read = self
            .database
            .begin_read()
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let current_profile = catalog
            .get(profile_key.as_slice())
            .map_err(|_| IdentityLifecycleError::Storage)?
            .map(|v| v.value().to_vec())
            .ok_or(IdentityLifecycleError::InvalidRequest)?;
        let current_object = catalog
            .get(object_key.as_slice())
            .map_err(|_| IdentityLifecycleError::Storage)?
            .map(|v| v.value().to_vec())
            .ok_or(IdentityLifecycleError::Storage)?;
        drop(catalog);
        drop(read);

        let mut profile = decode_workload_profile_record(profile_id, &current_profile)?;
        if profile.is_builtin || profile.name == new_name {
            return Err(IdentityLifecycleError::InvalidRequest);
        }
        profile.name = new_name.to_owned();
        profile.definition_epoch = profile
            .definition_epoch
            .checked_add(1)
            .ok_or(IdentityLifecycleError::InvalidRequest)?;
        let next_profile = encode_workload_profile_payload(&profile_key, &profile)?;

        let mut object = decode_registered_envelope(&current_object)
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let obj_name = object
            .fields
            .iter_mut()
            .find(|f| f.tag == 3 && f.wire_type == WireType::Text)
            .ok_or(IdentityLifecycleError::Storage)?;
        obj_name.payload = new_name.as_bytes().to_vec();
        let obj_epoch = object
            .fields
            .iter_mut()
            .find(|f| f.tag == 5 && f.wire_type == WireType::U64)
            .ok_or(IdentityLifecycleError::Storage)?;
        let cur_obj_epoch = u64::from_le_bytes(
            obj_epoch
                .payload
                .as_slice()
                .try_into()
                .map_err(|_| IdentityLifecycleError::Storage)?,
        );
        obj_epoch.payload = cur_obj_epoch
            .checked_add(1)
            .ok_or(IdentityLifecycleError::InvalidRequest)?
            .to_le_bytes()
            .to_vec();
        let next_object = encode_envelope(
            &family_schema(2).ok_or(IdentityLifecycleError::Storage)?,
            &object_key,
            &object.fields,
        )
        .map_err(|_| IdentityLifecycleError::Storage)?;
        audit_event.target_ids = vec![profile_id];
        let writes = [
            CatalogWrite {
                key: profile_key,
                expected_value: Some(current_profile),
                payload: Some(next_profile),
            },
            CatalogWrite {
                key: object_key,
                expected_value: Some(current_object),
                payload: Some(next_object),
            },
        ];
        self.commit_authorized_identity_ddl(
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &writes,
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => IdentityLifecycleError::Unauthorized,
            error => IdentityLifecycleError::Commit(error),
        })
    }

    pub fn drop_workload_profile(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        profile_id: [u8; 16],
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, IdentityLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || profile_id == [0; 16]
            || retention_deadline_us <= 0
        {
            return Err(IdentityLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(IdentityLifecycleError::InvalidAudit);
        }
        let profile_key = workload_profile_catalog_key(profile_id);
        let object_key = identity_object_catalog_key(profile_id);
        let tombstone_key = retired_object_id_catalog_key(profile_id);

        let read = self
            .database
            .begin_read()
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let current_profile = catalog
            .get(profile_key.as_slice())
            .map_err(|_| IdentityLifecycleError::Storage)?
            .map(|v| v.value().to_vec())
            .ok_or(IdentityLifecycleError::InvalidRequest)?;
        let current_object = catalog
            .get(object_key.as_slice())
            .map_err(|_| IdentityLifecycleError::Storage)?
            .map(|v| v.value().to_vec())
            .ok_or(IdentityLifecycleError::Storage)?;
        let profile = decode_workload_profile_record(profile_id, &current_profile)?;
        if profile.is_builtin {
            return Err(IdentityLifecycleError::InvalidRequest);
        }
        // RESTRICT: reject if any dependency edge references this profile.
        let dep_prefix = 15u16.to_be_bytes();
        for entry in catalog
            .range(dep_prefix.as_slice()..)
            .map_err(|_| IdentityLifecycleError::Storage)?
        {
            let (k, _) = entry.map_err(|_| IdentityLifecycleError::Storage)?;
            let kb = k.value();
            if !kb.starts_with(&dep_prefix) {
                break;
            }
            if kb.len() == 39 && kb[20..36] == profile_id {
                return Err(IdentityLifecycleError::InvalidRequest);
            }
        }
        drop(catalog);
        drop(read);

        audit_event.target_ids = vec![profile_id];
        let writes = [
            CatalogWrite {
                key: profile_key,
                expected_value: Some(current_profile),
                payload: None,
            },
            CatalogWrite {
                key: object_key,
                expected_value: Some(current_object),
                payload: None,
            },
            CatalogWrite {
                key: tombstone_key.clone(),
                expected_value: None,
                payload: Some(
                    encode_envelope(
                        &family_schema(17).ok_or(IdentityLifecycleError::Storage)?,
                        &tombstone_key,
                        &[],
                    )
                    .map_err(|_| IdentityLifecycleError::Storage)?,
                ),
            },
        ];
        self.commit_authorized_identity_ddl(
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &writes,
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => IdentityLifecycleError::Unauthorized,
            error => IdentityLifecycleError::Commit(error),
        })
    }

    pub fn set_user_workload_profile(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        user_id: [u8; 16],
        profile_id: [u8; 16],
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, IdentityLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || user_id == [0; 16]
            || profile_id == [0; 16]
            || retention_deadline_us <= 0
        {
            return Err(IdentityLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(IdentityLifecycleError::InvalidAudit);
        }
        let _ = self.load_workload_profile(profile_id)?;
        let principal_key = principal_catalog_key(user_id);
        let read = self
            .database
            .begin_read()
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let current_principal = catalog
            .get(principal_key.as_slice())
            .map_err(|_| IdentityLifecycleError::Storage)?
            .map(|v| v.value().to_vec())
            .ok_or(IdentityLifecycleError::InvalidRequest)?;
        let mut principal = decode_registered_envelope(&current_principal)
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let kind = principal
            .fields
            .iter()
            .find(|f| f.tag == 1 && f.wire_type == WireType::U16)
            .and_then(|f| <[u8; 2]>::try_from(f.payload.as_slice()).ok())
            .map(u16::from_le_bytes)
            .ok_or(IdentityLifecycleError::Storage)?;
        if kind != 1 {
            return Err(IdentityLifecycleError::InvalidRequest);
        }
        let old_profile_id = principal
            .fields
            .iter()
            .find(|f| f.tag == 5 && f.wire_type == WireType::Id)
            .and_then(|f| <[u8; 16]>::try_from(f.payload.as_slice()).ok());
        if old_profile_id == Some(profile_id) {
            return Err(IdentityLifecycleError::InvalidRequest);
        }
        let mut writes = Vec::new();
        if let Some(old_id) = old_profile_id {
            let old_dep_key = dependency_catalog_key(user_id, old_id, 6);
            if let Some(old_dep_val) = catalog
                .get(old_dep_key.as_slice())
                .map_err(|_| IdentityLifecycleError::Storage)?
                .map(|v| v.value().to_vec())
            {
                writes.push(CatalogWrite {
                    key: old_dep_key,
                    expected_value: Some(old_dep_val),
                    payload: None,
                });
            }
        }
        drop(catalog);
        drop(read);

        if let Some(tag5) = principal.fields.iter_mut().find(|f| f.tag == 5) {
            tag5.payload = profile_id.to_vec();
        } else {
            principal.fields.push(CatalogField {
                tag: 5,
                wire_type: WireType::Id,
                payload: profile_id.to_vec(),
            });
        }
        let epoch = principal
            .fields
            .iter_mut()
            .find(|f| f.tag == 4 && f.wire_type == WireType::U64)
            .ok_or(IdentityLifecycleError::Storage)?;
        let current_epoch = u64::from_le_bytes(
            epoch
                .payload
                .as_slice()
                .try_into()
                .map_err(|_| IdentityLifecycleError::Storage)?,
        );
        epoch.payload = current_epoch
            .checked_add(1)
            .ok_or(IdentityLifecycleError::InvalidRequest)?
            .to_le_bytes()
            .to_vec();
        let next_principal = encode_envelope(
            &family_schema(10).ok_or(IdentityLifecycleError::Storage)?,
            &principal_key,
            &principal.fields,
        )
        .map_err(|_| IdentityLifecycleError::Storage)?;
        writes.push(CatalogWrite {
            key: principal_key,
            expected_value: Some(current_principal),
            payload: Some(next_principal),
        });
        let new_dep_key = dependency_catalog_key(user_id, profile_id, 6);
        let new_dep_payload = encode_envelope(
            &family_schema(15).ok_or(IdentityLifecycleError::Storage)?,
            &new_dep_key,
            &[],
        )
        .map_err(|_| IdentityLifecycleError::Storage)?;
        writes.push(CatalogWrite {
            key: new_dep_key,
            expected_value: None,
            payload: Some(new_dep_payload),
        });
        audit_event.target_ids = if user_id < profile_id {
            vec![user_id, profile_id]
        } else if user_id > profile_id {
            vec![profile_id, user_id]
        } else {
            vec![user_id]
        };
        self.commit_authorized_identity_ddl(
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &writes,
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => IdentityLifecycleError::Unauthorized,
            error => IdentityLifecycleError::Commit(error),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::audit::{AuditAction, AuditEvent, AuditResult};
    use crate::bootstrap::initialize;
    use crate::storage_manager::StorageManager;
    use crate::workload_profile::{
        LARGE_OPERATIONAL_PROFILE_ID, SMALL_OPERATIONAL_PROFILE_ID, WorkloadProfileLimits,
    };
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn test_db_dir(label: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "prdb-wp-test-{label}-{}-{nanos}",
            std::process::id()
        ))
    }

    fn ddl_audit(event_id: u8, inv_id: [u8; 16], actor: [u8; 16]) -> AuditEvent {
        AuditEvent {
            event_id: [event_id; 16],
            occurred_at_us: 1_000,
            actor_id: Some(actor),
            correlation_id: inv_id,
            action: AuditAction::Ddl,
            target_ids: Vec::new(),
            result: AuditResult::Succeeded,
        }
    }

    #[test]
    fn workload_profile_ddl_lifecycle_and_user_binding() {
        let dir = test_db_dir("lifecycle");
        let now_us = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros(),
        )
        .unwrap();
        let deadline_us = now_us + 3_600_000_000;
        let mut session = initialize(&dir, "admin", deadline_us).unwrap();
        let admin = session.admin_user_id();
        let _ = session.take_secret();
        let instance = session.acknowledge().unwrap();
        let storage = StorageManager::new(instance.database(), Duration::from_millis(100)).unwrap();

        // Builtin profiles are seeded
        let profiles = storage.list_workload_profiles().unwrap();
        assert_eq!(profiles.len(), 4);
        let small_op = storage
            .resolve_workload_profile_by_name("small_operational")
            .unwrap();
        assert_eq!(small_op.profile_id, SMALL_OPERATIONAL_PROFILE_ID);

        // Create a custom profile
        let mut limits = WorkloadProfileLimits::small_operational();
        limits.memory_max_bytes = 512 * 1024 * 1024;
        let (custom_id, _) = storage
            .create_workload_profile(
                [0x11; 16],
                admin,
                "custom_erp",
                limits,
                9_999,
                ddl_audit(1, [0x11; 16], admin),
            )
            .unwrap();

        // Create user with custom profile
        let created_user = storage
            .create_user_with_profile(
                [0x12; 16],
                admin,
                "erp_user",
                custom_id,
                9_999,
                ddl_audit(2, [0x12; 16], admin),
            )
            .unwrap();
        let user_id = created_user.user_id;

        // Dropping an in-use profile fails (RESTRICT)
        assert!(
            storage
                .drop_workload_profile(
                    [0x13; 16],
                    admin,
                    custom_id,
                    9_999,
                    ddl_audit(3, [0x13; 16], admin),
                )
                .is_err()
        );

        // Alter profile increments epoch
        storage
            .alter_workload_profile(
                [0x14; 16],
                admin,
                custom_id,
                &[("max_processes".to_owned(), 64)],
                9_999,
                ddl_audit(4, [0x14; 16], admin),
            )
            .unwrap();
        let updated = storage.load_workload_profile(custom_id).unwrap();
        assert_eq!(updated.definition_epoch, 2);
        assert_eq!(updated.limits.max_processes, 64);

        // Rename profile
        storage
            .rename_workload_profile(
                [0x15; 16],
                admin,
                custom_id,
                "custom_erp_v2",
                9_999,
                ddl_audit(5, [0x15; 16], admin),
            )
            .unwrap();
        assert!(
            storage
                .resolve_workload_profile_by_name("custom_erp")
                .is_err()
        );
        assert_eq!(
            storage
                .resolve_workload_profile_by_name("custom_erp_v2")
                .unwrap()
                .profile_id,
            custom_id
        );

        // Reassign user to builtin large_operational, then drop custom profile
        storage
            .set_user_workload_profile(
                [0x16; 16],
                admin,
                user_id,
                LARGE_OPERATIONAL_PROFILE_ID,
                9_999,
                ddl_audit(6, [0x16; 16], admin),
            )
            .unwrap();
        storage
            .drop_workload_profile(
                [0x17; 16],
                admin,
                custom_id,
                9_999,
                ddl_audit(7, [0x17; 16], admin),
            )
            .unwrap();
        assert!(storage.load_workload_profile(custom_id).is_err());
        drop(storage);
        drop(instance);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
