use super::catalog_helpers::*;
use super::*;
use crate::audit::{AuditAction, AuditEvent, AuditResult};
use crate::catalog::{
    CatalogField, WireType, decode_registered_envelope, encode_envelope, family_schema,
};
use crate::commit::{
    CatalogWrite, CommitError, commit_authorized_identity_ddl_with_fences,
    commit_authorized_membership_ddl_with_fences, commit_authorized_object_grant_ddl_with_fences,
    commit_authorized_system_privilege_ddl_with_fences,
};
use crate::credentials::{decode_verifier_record, encode_revoked_verifier_record, now_unix_micros};
use crate::metadata::{EngineMetadata, Lifecycle};
use crate::schema::{CATALOG, ENGINE_META};
use crate::snapshot::MAX_CATALOG_FAMILY_SCAN;
use redb::ReadableDatabase;
use std::collections::HashSet;
use zeroize::Zeroize;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IdentityLifecycleError {
    InvalidRequest,
    InvalidAudit,
    RandomnessUnavailable,
    IdentityIdGenerationFailed,
    Unauthorized,
    Storage,
    Commit(CommitError),
}

impl std::fmt::Display for IdentityLifecycleError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "identity lifecycle operation failed: {self:?}")
    }
}

impl std::error::Error for IdentityLifecycleError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MembershipLifecycleError {
    InvalidRequest,
    InvalidAudit,
    Unauthorized,
    Storage,
    Commit(CommitError),
}

impl std::fmt::Display for MembershipLifecycleError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "membership lifecycle operation failed: {self:?}")
    }
}

impl std::error::Error for MembershipLifecycleError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdministrativeSystemPrivilege {
    ManageUsers,
    ManageRoles,
    CreateSchema,
}

impl AdministrativeSystemPrivilege {
    pub(super) fn catalog_code(self) -> u16 {
        match self {
            Self::ManageUsers => 12,
            Self::ManageRoles => 13,
            Self::CreateSchema => 14,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SystemPrivilegeChange {
    pub grantee_id: [u8; 16],
    pub privilege: AdministrativeSystemPrivilege,
    pub grant: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SystemPrivilegeLifecycleError {
    InvalidRequest,
    InvalidAudit,
    Unauthorized,
    Storage,
    Commit(CommitError),
}

impl std::fmt::Display for SystemPrivilegeLifecycleError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "system privilege operation failed: {self:?}")
    }
}

impl std::error::Error for SystemPrivilegeLifecycleError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedUser {
    pub user_id: [u8; 16],
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedRole {
    pub role_id: [u8; 16],
    pub commit_sequence: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectGrantPrivilege {
    Usage,
    Select,
    Insert,
    Update,
    Delete,
    References,
    Create,
    Alter,
    Index,
    ManagePolicies,
    ManageGrants,
}

impl ObjectGrantPrivilege {
    pub(super) fn code(self) -> u16 {
        match self {
            Self::Usage => 1,
            Self::Select => 2,
            Self::Insert => 3,
            Self::Update => 4,
            Self::Delete => 5,
            Self::References => 6,
            Self::Create => 7,
            Self::Alter => 8,
            Self::Index => 9,
            Self::ManagePolicies => 10,
            Self::ManageGrants => 11,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObjectGrantTarget {
    Object,
    Column([u8; 16]),
    Columns(Vec<[u8; 16]>),
    AllColumns,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SetObjectGrantRequest {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub grantee_id: [u8; 16],
    pub object_id: [u8; 16],
    pub privilege: ObjectGrantPrivilege,
    pub target: ObjectGrantTarget,
    pub grant: bool,
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SetObjectGrantBatchRequest {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub grantee_id: [u8; 16],
    pub object_id: [u8; 16],
    pub privileges: Vec<ObjectGrantPrivilege>,
    pub target: ObjectGrantTarget,
    pub grant: bool,
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectGrantEntry {
    pub privilege: ObjectGrantPrivilege,
    pub target: ObjectGrantTarget,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SetObjectGrantEntriesBatchRequest {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub grantee_id: [u8; 16],
    pub object_id: [u8; 16],
    pub entries: Vec<ObjectGrantEntry>,
    pub grant: bool,
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoleMembershipChange {
    pub member_id: [u8; 16],
    pub role_id: [u8; 16],
    pub grant: bool,
}

#[derive(Clone, Copy)]
pub(super) enum NewPrincipalKind {
    User,
    UserNoLogin,
    Role,
}

impl<'db> StorageManager<'db> {
    /// Create a LOGIN user through the authorized identity DDL path.
    pub fn create_user(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        name: &str,
        retention_deadline_us: i64,
        audit_event: AuditEvent,
    ) -> Result<CreatedUser, IdentityLifecycleError> {
        self.create_user_with_profile(
            invocation_id,
            authenticated_actor_id,
            name,
            crate::workload_profile::SMALL_OPERATIONAL_PROFILE_ID,
            retention_deadline_us,
            audit_event,
        )
    }

    /// Create a LOGIN user with an explicit workload profile.
    pub fn create_user_with_profile(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        name: &str,
        workload_profile_id: [u8; 16],
        retention_deadline_us: i64,
        audit_event: AuditEvent,
    ) -> Result<CreatedUser, IdentityLifecycleError> {
        let (user_id, commit_sequence) = self.create_identity(
            invocation_id,
            authenticated_actor_id,
            name,
            NewPrincipalKind::User,
            Some(workload_profile_id),
            retention_deadline_us,
            audit_event,
        )?;
        Ok(CreatedUser {
            user_id,
            commit_sequence,
        })
    }

    /// Create a NOLOGIN user through the authorized identity DDL path.
    pub fn create_user_no_login(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        name: &str,
        retention_deadline_us: i64,
        audit_event: AuditEvent,
    ) -> Result<CreatedUser, IdentityLifecycleError> {
        self.create_user_no_login_with_profile(
            invocation_id,
            authenticated_actor_id,
            name,
            crate::workload_profile::SMALL_OPERATIONAL_PROFILE_ID,
            retention_deadline_us,
            audit_event,
        )
    }

    /// Create a NOLOGIN user with an explicit workload profile.
    pub fn create_user_no_login_with_profile(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        name: &str,
        workload_profile_id: [u8; 16],
        retention_deadline_us: i64,
        audit_event: AuditEvent,
    ) -> Result<CreatedUser, IdentityLifecycleError> {
        let (user_id, commit_sequence) = self.create_identity(
            invocation_id,
            authenticated_actor_id,
            name,
            NewPrincipalKind::UserNoLogin,
            Some(workload_profile_id),
            retention_deadline_us,
            audit_event,
        )?;
        Ok(CreatedUser {
            user_id,
            commit_sequence,
        })
    }

    /// Create a non-login role through the authorized identity DDL path.
    pub fn create_role(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        name: &str,
        retention_deadline_us: i64,
        audit_event: AuditEvent,
    ) -> Result<CreatedRole, IdentityLifecycleError> {
        let (role_id, commit_sequence) = self.create_identity(
            invocation_id,
            authenticated_actor_id,
            name,
            NewPrincipalKind::Role,
            None,
            retention_deadline_us,
            audit_event,
        )?;
        Ok(CreatedRole {
            role_id,
            commit_sequence,
        })
    }

    /// Rename a user or role through the authorized identity DDL path.
    pub fn rename_identity(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        identity_id: [u8; 16],
        name: &str,
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, IdentityLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || identity_id == [0; 16]
            || retention_deadline_us <= 0
            || !is_normalized_principal_name(name)
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
        let principal_key = principal_catalog_key(identity_id);
        let object_key = identity_object_catalog_key(identity_id);
        let read = self
            .database
            .begin_read()
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let principal_current = catalog
            .get(principal_key.as_slice())
            .map_err(|_| IdentityLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(IdentityLifecycleError::InvalidRequest)?;
        let object_current = catalog
            .get(object_key.as_slice())
            .map_err(|_| IdentityLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(IdentityLifecycleError::Storage)?;
        drop(catalog);
        drop(read);

        let mut principal = decode_registered_envelope(&principal_current)
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let kind = principal
            .fields
            .iter()
            .find(|field| field.tag == 1 && field.wire_type == WireType::U16)
            .and_then(|field| <[u8; 2]>::try_from(field.payload.as_slice()).ok())
            .map(u16::from_le_bytes)
            .filter(|kind| matches!(kind, 1 | 2))
            .ok_or(IdentityLifecycleError::Storage)?;
        let principal_name = principal
            .fields
            .iter_mut()
            .find(|field| field.tag == 2 && field.wire_type == WireType::Text)
            .ok_or(IdentityLifecycleError::Storage)?;
        if principal_name.payload == name.as_bytes() {
            return Err(IdentityLifecycleError::InvalidRequest);
        }
        principal_name.payload = name.as_bytes().to_vec();
        let epoch = principal
            .fields
            .iter_mut()
            .find(|field| field.tag == 4 && field.wire_type == WireType::U64)
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
            .filter(|next| *next != 0)
            .ok_or(IdentityLifecycleError::InvalidRequest)?
            .to_le_bytes()
            .to_vec();
        let principal_payload = encode_envelope(
            &family_schema(10).ok_or(IdentityLifecycleError::Storage)?,
            &principal_key,
            &principal.fields,
        )
        .map_err(|_| IdentityLifecycleError::Storage)?;

        let mut object = decode_registered_envelope(&object_current)
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let expected_object_kind = if kind == 1 { 8 } else { 9 };
        let object_kind = object
            .fields
            .iter()
            .find(|field| field.tag == 1 && field.wire_type == WireType::U16)
            .and_then(|field| <[u8; 2]>::try_from(field.payload.as_slice()).ok())
            .map(u16::from_le_bytes);
        if object_kind != Some(expected_object_kind) {
            return Err(IdentityLifecycleError::Storage);
        }
        let object_name = object
            .fields
            .iter_mut()
            .find(|field| field.tag == 3 && field.wire_type == WireType::Text)
            .ok_or(IdentityLifecycleError::Storage)?;
        object_name.payload = name.as_bytes().to_vec();
        let version = object
            .fields
            .iter_mut()
            .find(|field| field.tag == 5 && field.wire_type == WireType::U64)
            .ok_or(IdentityLifecycleError::Storage)?;
        let current_version = u64::from_le_bytes(
            version
                .payload
                .as_slice()
                .try_into()
                .map_err(|_| IdentityLifecycleError::Storage)?,
        );
        version.payload = current_version
            .checked_add(1)
            .filter(|next| *next != 0)
            .ok_or(IdentityLifecycleError::InvalidRequest)?
            .to_le_bytes()
            .to_vec();
        let object_payload = encode_envelope(
            &family_schema(2).ok_or(IdentityLifecycleError::Storage)?,
            &object_key,
            &object.fields,
        )
        .map_err(|_| IdentityLifecycleError::Storage)?;
        audit_event.target_ids = vec![identity_id];
        let writes = [
            CatalogWrite {
                key: principal_key,
                expected_value: Some(principal_current),
                payload: Some(principal_payload),
            },
            CatalogWrite {
                key: object_key,
                expected_value: Some(object_current),
                payload: Some(object_payload),
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

    /// Change a user's LOGIN state through the authorized identity DDL path.
    /// Disabling LOGIN revokes every outstanding token in the same commit.
    pub fn set_user_login_enabled(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        user_id: [u8; 16],
        enabled: bool,
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, IdentityLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || user_id == [0; 16]
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
        let principal_key = principal_catalog_key(user_id);
        let read = self
            .database
            .begin_read()
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let principal_current = catalog
            .get(principal_key.as_slice())
            .map_err(|_| IdentityLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(IdentityLifecycleError::InvalidRequest)?;
        let mut principal = decode_registered_envelope(&principal_current)
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let kind = principal
            .fields
            .iter()
            .find(|field| field.tag == 1 && field.wire_type == WireType::U16)
            .and_then(|field| <[u8; 2]>::try_from(field.payload.as_slice()).ok())
            .map(u16::from_le_bytes);
        if kind != Some(1) {
            return Err(IdentityLifecycleError::InvalidRequest);
        }
        let login_field = principal
            .fields
            .iter_mut()
            .find(|field| field.tag == 3 && field.wire_type == WireType::Bool)
            .ok_or(IdentityLifecycleError::Storage)?;
        let current_enabled = match login_field.payload.as_slice() {
            [0] => false,
            [1] => true,
            _ => return Err(IdentityLifecycleError::Storage),
        };
        if current_enabled == enabled {
            return Err(IdentityLifecycleError::InvalidRequest);
        }
        login_field.payload = vec![u8::from(enabled)];
        let epoch_field = principal
            .fields
            .iter_mut()
            .find(|field| field.tag == 4 && field.wire_type == WireType::U64)
            .ok_or(IdentityLifecycleError::Storage)?;
        let current_epoch = u64::from_le_bytes(
            epoch_field
                .payload
                .as_slice()
                .try_into()
                .map_err(|_| IdentityLifecycleError::Storage)?,
        );
        epoch_field.payload = current_epoch
            .checked_add(1)
            .filter(|next| *next != 0)
            .ok_or(IdentityLifecycleError::InvalidRequest)?
            .to_le_bytes()
            .to_vec();
        let principal_payload = encode_envelope(
            &family_schema(10).ok_or(IdentityLifecycleError::Storage)?,
            &principal_key,
            &principal.fields,
        )
        .map_err(|_| IdentityLifecycleError::Storage)?;
        let mut writes = vec![CatalogWrite {
            key: principal_key,
            expected_value: Some(principal_current),
            payload: Some(principal_payload),
        }];
        if !enabled {
            let revoked_at_us = now_unix_micros().ok_or(IdentityLifecycleError::InvalidRequest)?;
            let prefix = 16u16.to_be_bytes();
            let mut count = 0usize;
            for entry in catalog
                .range(prefix.as_slice()..)
                .map_err(|_| IdentityLifecycleError::Storage)?
            {
                let (key, value) = entry.map_err(|_| IdentityLifecycleError::Storage)?;
                let key = key.value().to_vec();
                if !key.starts_with(&prefix) {
                    break;
                }
                count += 1;
                if count > MAX_CATALOG_FAMILY_SCAN || key.len() != 19 || key[2] != 1 {
                    return Err(IdentityLifecycleError::Storage);
                }
                let current = value.value().to_vec();
                let record = decode_registered_envelope(&current)
                    .map_err(|_| IdentityLifecycleError::Storage)?;
                let verifier =
                    decode_verifier_record(&record).map_err(|_| IdentityLifecycleError::Storage)?;
                if verifier.user_id == user_id && !verifier.revoked {
                    let payload = encode_revoked_verifier_record(
                        &record,
                        revoked_at_us.max(verifier.issued_at_us),
                    )
                    .map_err(|_| IdentityLifecycleError::Storage)?;
                    writes.push(CatalogWrite {
                        key,
                        expected_value: Some(current),
                        payload: Some(payload),
                    });
                }
            }
        }
        drop(catalog);
        drop(read);
        audit_event.target_ids = vec![user_id];
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

    /// Drop a user or role only when the authorized identity DDL path finds no
    /// remaining references. User credentials are revoked in the same commit.
    pub fn drop_identity(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        identity_id: [u8; 16],
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, IdentityLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || identity_id == [0; 16]
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
        let principal_key = principal_catalog_key(identity_id);
        let object_key = identity_object_catalog_key(identity_id);
        let tombstone_key = retired_object_id_catalog_key(identity_id);
        let read = self
            .database
            .begin_read()
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let principal_current = catalog
            .get(principal_key.as_slice())
            .map_err(|_| IdentityLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(IdentityLifecycleError::InvalidRequest)?;
        let object_current = catalog
            .get(object_key.as_slice())
            .map_err(|_| IdentityLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(IdentityLifecycleError::Storage)?;
        if catalog
            .get(tombstone_key.as_slice())
            .map_err(|_| IdentityLifecycleError::Storage)?
            .is_some()
        {
            return Err(IdentityLifecycleError::InvalidRequest);
        }
        let principal = decode_registered_envelope(&principal_current)
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let kind = principal
            .fields
            .iter()
            .find(|field| field.tag == 1 && field.wire_type == WireType::U16)
            .and_then(|field| <[u8; 2]>::try_from(field.payload.as_slice()).ok())
            .map(u16::from_le_bytes)
            .filter(|kind| matches!(kind, 1 | 2))
            .ok_or(IdentityLifecycleError::Storage)?;
        let expected_object_kind = if kind == 1 { 8 } else { 9 };
        let object = decode_registered_envelope(&object_current)
            .map_err(|_| IdentityLifecycleError::Storage)?;
        if object
            .fields
            .iter()
            .find(|field| field.tag == 1 && field.wire_type == WireType::U16)
            .and_then(|field| <[u8; 2]>::try_from(field.payload.as_slice()).ok())
            .map(u16::from_le_bytes)
            != Some(expected_object_kind)
        {
            return Err(IdentityLifecycleError::Storage);
        }
        let mut writes = vec![
            CatalogWrite {
                key: principal_key,
                expected_value: Some(principal_current),
                payload: None,
            },
            CatalogWrite {
                key: object_key,
                expected_value: Some(object_current),
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
        if kind == 1 {
            if let Some(profile_field) = principal
                .fields
                .iter()
                .find(|field| field.tag == 5 && field.wire_type == WireType::Id)
                && let Ok(profile_id) = <[u8; 16]>::try_from(profile_field.payload.as_slice())
            {
                let dep_key = dependency_catalog_key(identity_id, profile_id, 6);
                if let Some(dep_current) = catalog
                    .get(dep_key.as_slice())
                    .map_err(|_| IdentityLifecycleError::Storage)?
                    .map(|value| value.value().to_vec())
                {
                    writes.push(CatalogWrite {
                        key: dep_key,
                        expected_value: Some(dep_current),
                        payload: None,
                    });
                }
            }
            let revoked_at_us = now_unix_micros().ok_or(IdentityLifecycleError::InvalidRequest)?;
            let prefix = 16u16.to_be_bytes();
            let mut count = 0usize;
            for entry in catalog
                .range(prefix.as_slice()..)
                .map_err(|_| IdentityLifecycleError::Storage)?
            {
                let (key, value) = entry.map_err(|_| IdentityLifecycleError::Storage)?;
                let key = key.value().to_vec();
                if !key.starts_with(&prefix) {
                    break;
                }
                count += 1;
                if count > MAX_CATALOG_FAMILY_SCAN || key.len() != 19 || key[2] != 1 {
                    return Err(IdentityLifecycleError::Storage);
                }
                let current = value.value().to_vec();
                let record = decode_registered_envelope(&current)
                    .map_err(|_| IdentityLifecycleError::Storage)?;
                let verifier =
                    decode_verifier_record(&record).map_err(|_| IdentityLifecycleError::Storage)?;
                if verifier.user_id == identity_id && !verifier.revoked {
                    let payload = encode_revoked_verifier_record(
                        &record,
                        revoked_at_us.max(verifier.issued_at_us),
                    )
                    .map_err(|_| IdentityLifecycleError::Storage)?;
                    writes.push(CatalogWrite {
                        key,
                        expected_value: Some(current),
                        payload: Some(payload),
                    });
                }
            }
        }
        drop(catalog);
        drop(read);
        audit_event.target_ids = vec![identity_id];
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

    #[allow(clippy::too_many_arguments)]
    pub(super) fn create_identity(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        name: &str,
        kind: NewPrincipalKind,
        workload_profile_id: Option<[u8; 16]>,
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<([u8; 16], u64), IdentityLifecycleError> {
        let (principal_kind, login_enabled, object_kind) = match kind {
            NewPrincipalKind::User => (1u16, true, 8u16),
            NewPrincipalKind::UserNoLogin => (1u16, false, 8u16),
            NewPrincipalKind::Role => (2u16, false, 9u16),
        };
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || retention_deadline_us <= 0
            || !is_normalized_principal_name(name)
        {
            return Err(IdentityLifecycleError::InvalidRequest);
        }
        if let Some(profile_id) = workload_profile_id
            && (profile_id == [0; 16] || self.load_workload_profile(profile_id).is_err())
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

        let user_id = self.generate_identity_id()?;
        let principal_key = principal_catalog_key(user_id);
        let object_key = identity_object_catalog_key(user_id);
        let mut principal_fields = vec![
            CatalogField {
                tag: 1,
                wire_type: WireType::U16,
                payload: principal_kind.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::Text,
                payload: name.as_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Bool,
                payload: vec![u8::from(login_enabled)],
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::U64,
                payload: 1u64.to_le_bytes().to_vec(),
            },
        ];
        if let Some(profile_id) = workload_profile_id {
            principal_fields.push(CatalogField {
                tag: 5,
                wire_type: WireType::Id,
                payload: profile_id.to_vec(),
            });
        }
        let principal_payload = encode_envelope(
            &family_schema(10).ok_or(IdentityLifecycleError::Storage)?,
            &principal_key,
            &principal_fields,
        )
        .map_err(|_| IdentityLifecycleError::InvalidRequest)?;
        let object_payload = encode_envelope(
            &family_schema(2).ok_or(IdentityLifecycleError::Storage)?,
            &object_key,
            &[
                CatalogField {
                    tag: 1,
                    wire_type: WireType::U16,
                    payload: object_kind.to_le_bytes().to_vec(),
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
        audit_event.target_ids = vec![user_id];
        let mut writes = vec![
            CatalogWrite {
                key: principal_key,
                expected_value: None,
                payload: Some(principal_payload),
            },
            CatalogWrite {
                key: object_key,
                expected_value: None,
                payload: Some(object_payload),
            },
        ];
        if let Some(profile_id) = workload_profile_id {
            let dep_key = dependency_catalog_key(user_id, profile_id, 6);
            let dep_payload = encode_envelope(
                &family_schema(15).ok_or(IdentityLifecycleError::Storage)?,
                &dep_key,
                &[],
            )
            .map_err(|_| IdentityLifecycleError::InvalidRequest)?;
            writes.push(CatalogWrite {
                key: dep_key,
                expected_value: None,
                payload: Some(dep_payload),
            });
        }
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
        Ok((user_id, commit_sequence))
    }

    /// Grant or revoke one role membership through the audited membership DDL path.
    pub fn set_role_membership(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        change: RoleMembershipChange,
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, MembershipLifecycleError> {
        let RoleMembershipChange {
            member_id,
            role_id,
            grant,
        } = change;
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || member_id == [0; 16]
            || role_id == [0; 16]
            || member_id == role_id
            || retention_deadline_us <= 0
        {
            return Err(MembershipLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::AuthorityChange
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(MembershipLifecycleError::InvalidAudit);
        }
        let membership_key = membership_catalog_key(member_id, role_id);
        let dependency_key = dependency_catalog_key(member_id, role_id, 5);
        let member_key = principal_catalog_key(member_id);
        let read = self
            .database
            .begin_read()
            .map_err(|_| MembershipLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| MembershipLifecycleError::Storage)?;
        let membership_value = catalog
            .get(membership_key.as_slice())
            .map_err(|_| MembershipLifecycleError::Storage)?
            .map(|value| value.value().to_vec());
        let dependency_value = catalog
            .get(dependency_key.as_slice())
            .map_err(|_| MembershipLifecycleError::Storage)?
            .map(|value| value.value().to_vec());
        let member_value = catalog
            .get(member_key.as_slice())
            .map_err(|_| MembershipLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(MembershipLifecycleError::InvalidRequest)?;
        drop(catalog);
        drop(read);
        if membership_value.is_some() != dependency_value.is_some() {
            return Err(MembershipLifecycleError::Storage);
        }
        let (membership_payload, dependency_payload) = if grant {
            if membership_value.is_some() {
                return Err(MembershipLifecycleError::InvalidRequest);
            }
            let membership_payload = encode_envelope(
                &family_schema(11).ok_or(MembershipLifecycleError::Storage)?,
                &membership_key,
                &[
                    CatalogField {
                        tag: 1,
                        wire_type: WireType::Id,
                        payload: authenticated_actor_id.to_vec(),
                    },
                    CatalogField {
                        tag: 2,
                        wire_type: WireType::U64,
                        payload: 1u64.to_le_bytes().to_vec(),
                    },
                ],
            )
            .map_err(|_| MembershipLifecycleError::InvalidRequest)?;
            let dependency_payload = encode_envelope(
                &family_schema(15).ok_or(MembershipLifecycleError::Storage)?,
                &dependency_key,
                &[],
            )
            .map_err(|_| MembershipLifecycleError::InvalidRequest)?;
            (Some(membership_payload), Some(dependency_payload))
        } else {
            if membership_value.is_none() {
                return Err(MembershipLifecycleError::InvalidRequest);
            }
            (None, None)
        };
        let mut member_record = decode_registered_envelope(&member_value)
            .map_err(|_| MembershipLifecycleError::Storage)?;
        let epoch_field = member_record
            .fields
            .iter_mut()
            .find(|field| field.tag == 4 && field.wire_type == WireType::U64)
            .ok_or(MembershipLifecycleError::Storage)?;
        let current_epoch = u64::from_le_bytes(
            epoch_field
                .payload
                .as_slice()
                .try_into()
                .map_err(|_| MembershipLifecycleError::Storage)?,
        );
        let next_epoch = current_epoch
            .checked_add(1)
            .filter(|epoch| *epoch != 0)
            .ok_or(MembershipLifecycleError::InvalidRequest)?;
        epoch_field.payload = next_epoch.to_le_bytes().to_vec();
        let member_payload = encode_envelope(
            &family_schema(10).ok_or(MembershipLifecycleError::Storage)?,
            &member_key,
            &member_record.fields,
        )
        .map_err(|_| MembershipLifecycleError::Storage)?;
        let mut target_ids = vec![member_id, role_id];
        target_ids.sort_unstable();
        target_ids.dedup();
        audit_event.target_ids = target_ids;
        let writes = [
            CatalogWrite {
                key: membership_key,
                expected_value: membership_value,
                payload: membership_payload,
            },
            CatalogWrite {
                key: dependency_key,
                expected_value: dependency_value,
                payload: dependency_payload,
            },
            CatalogWrite {
                key: member_key,
                expected_value: Some(member_value),
                payload: Some(member_payload),
            },
        ];
        self.commit_authorized_membership_ddl(
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &writes,
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => MembershipLifecycleError::Unauthorized,
            error => MembershipLifecycleError::Commit(error),
        })
    }

    /// Grant or revoke one database-scoped system privilege as an administrator.
    pub fn set_system_privilege(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        change: SystemPrivilegeChange,
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, SystemPrivilegeLifecycleError> {
        let SystemPrivilegeChange {
            grantee_id,
            privilege,
            grant,
        } = change;
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || grantee_id == [0; 16]
            || retention_deadline_us <= 0
        {
            return Err(SystemPrivilegeLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::AuthorityChange
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(SystemPrivilegeLifecycleError::InvalidAudit);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| SystemPrivilegeLifecycleError::Storage)?;
        let metadata_bytes = read
            .open_table(ENGINE_META)
            .map_err(|_| SystemPrivilegeLifecycleError::Storage)?
            .get(&b"format"[..])
            .map_err(|_| SystemPrivilegeLifecycleError::Storage)?
            .ok_or(SystemPrivilegeLifecycleError::Storage)?
            .value()
            .to_vec();
        let metadata = EngineMetadata::decode(&metadata_bytes)
            .map_err(|_| SystemPrivilegeLifecycleError::Storage)?;
        if metadata.lifecycle != Lifecycle::Ready || !metadata.supports_current_formats() {
            return Err(SystemPrivilegeLifecycleError::Storage);
        }
        let database_id = metadata.database_id;
        let key = system_privilege_catalog_key(grantee_id, database_id, privilege.catalog_code());
        let dependency_key = dependency_catalog_key(grantee_id, database_id, 5);
        let grantee_key = principal_catalog_key(grantee_id);
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| SystemPrivilegeLifecycleError::Storage)?;
        let current = catalog
            .get(key.as_slice())
            .map_err(|_| SystemPrivilegeLifecycleError::Storage)?
            .map(|value| value.value().to_vec());
        let dependency_current = catalog
            .get(dependency_key.as_slice())
            .map_err(|_| SystemPrivilegeLifecycleError::Storage)?
            .map(|value| value.value().to_vec());
        let mut other_privilege_exists = false;
        for other_privilege in [12u16, 13, 14] {
            if other_privilege == privilege.catalog_code() {
                continue;
            }
            let other_key = system_privilege_catalog_key(grantee_id, database_id, other_privilege);
            if catalog
                .get(other_key.as_slice())
                .map_err(|_| SystemPrivilegeLifecycleError::Storage)?
                .is_some()
            {
                other_privilege_exists = true;
            }
        }
        let grantee_current = catalog
            .get(grantee_key.as_slice())
            .map_err(|_| SystemPrivilegeLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(SystemPrivilegeLifecycleError::InvalidRequest)?;
        drop(catalog);
        drop(read);
        if (current.is_some() || other_privilege_exists) != dependency_current.is_some() {
            return Err(SystemPrivilegeLifecycleError::Storage);
        }
        let (payload, dependency_update) = if grant {
            if current.is_some() {
                return Err(SystemPrivilegeLifecycleError::InvalidRequest);
            }
            let payload = Some(
                encode_envelope(
                    &family_schema(12).ok_or(SystemPrivilegeLifecycleError::Storage)?,
                    &key,
                    &[
                        CatalogField {
                            tag: 1,
                            wire_type: WireType::Id,
                            payload: authenticated_actor_id.to_vec(),
                        },
                        CatalogField {
                            tag: 2,
                            wire_type: WireType::U64,
                            payload: 1u64.to_le_bytes().to_vec(),
                        },
                    ],
                )
                .map_err(|_| SystemPrivilegeLifecycleError::InvalidRequest)?,
            );
            let dependency_update = if dependency_current.is_none() {
                Some(Some(
                    encode_envelope(
                        &family_schema(15).ok_or(SystemPrivilegeLifecycleError::Storage)?,
                        &dependency_key,
                        &[],
                    )
                    .map_err(|_| SystemPrivilegeLifecycleError::InvalidRequest)?,
                ))
            } else {
                None
            };
            (payload, dependency_update)
        } else {
            if current.is_none() {
                return Err(SystemPrivilegeLifecycleError::InvalidRequest);
            }
            let dependency_update = (!other_privilege_exists).then_some(None);
            (None, dependency_update)
        };
        let mut grantee_record = decode_registered_envelope(&grantee_current)
            .map_err(|_| SystemPrivilegeLifecycleError::Storage)?;
        let epoch_field = grantee_record
            .fields
            .iter_mut()
            .find(|field| field.tag == 4 && field.wire_type == WireType::U64)
            .ok_or(SystemPrivilegeLifecycleError::Storage)?;
        let current_epoch = u64::from_le_bytes(
            epoch_field
                .payload
                .as_slice()
                .try_into()
                .map_err(|_| SystemPrivilegeLifecycleError::Storage)?,
        );
        let next_epoch = current_epoch
            .checked_add(1)
            .filter(|epoch| *epoch != 0)
            .ok_or(SystemPrivilegeLifecycleError::InvalidRequest)?;
        epoch_field.payload = next_epoch.to_le_bytes().to_vec();
        let grantee_payload = encode_envelope(
            &family_schema(10).ok_or(SystemPrivilegeLifecycleError::Storage)?,
            &grantee_key,
            &grantee_record.fields,
        )
        .map_err(|_| SystemPrivilegeLifecycleError::Storage)?;
        audit_event.target_ids = vec![database_id, grantee_id];
        audit_event.target_ids.sort_unstable();
        audit_event.target_ids.dedup();
        let mut writes = vec![CatalogWrite {
            key,
            expected_value: current,
            payload,
        }];
        if let Some(dependency_payload) = dependency_update {
            writes.push(CatalogWrite {
                key: dependency_key,
                expected_value: dependency_current,
                payload: dependency_payload,
            });
        }
        writes.push(CatalogWrite {
            key: grantee_key,
            expected_value: Some(grantee_current),
            payload: Some(grantee_payload),
        });
        self.commit_authorized_system_privilege_ddl(
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &writes,
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => SystemPrivilegeLifecycleError::Unauthorized,
            error => SystemPrivilegeLifecycleError::Commit(error),
        })
    }

    pub(super) fn generate_identity_id(&self) -> Result<[u8; 16], IdentityLifecycleError> {
        for _ in 0..8 {
            let mut user_id = [0; 16];
            if getrandom::fill(&mut user_id).is_err() {
                user_id.zeroize();
                return Err(IdentityLifecycleError::RandomnessUnavailable);
            }
            if user_id == [0; 16] {
                user_id.zeroize();
                continue;
            }
            let principal_key = principal_catalog_key(user_id);
            let object_key = identity_object_catalog_key(user_id);
            let tombstone_key = retired_object_id_catalog_key(user_id);
            let read = self
                .database
                .begin_read()
                .map_err(|_| IdentityLifecycleError::Storage)?;
            let catalog = read
                .open_table(CATALOG)
                .map_err(|_| IdentityLifecycleError::Storage)?;
            let conflict = catalog
                .get(principal_key.as_slice())
                .map_err(|_| IdentityLifecycleError::Storage)?
                .is_some()
                || catalog
                    .get(object_key.as_slice())
                    .map_err(|_| IdentityLifecycleError::Storage)?
                    .is_some()
                || catalog
                    .get(tombstone_key.as_slice())
                    .map_err(|_| IdentityLifecycleError::Storage)?
                    .is_some();
            if !conflict {
                return Ok(user_id);
            }
            user_id.zeroize();
        }
        Err(IdentityLifecycleError::IdentityIdGenerationFailed)
    }

    /// Authorize user/role creation, rename, login-state changes, and
    /// `DROP ... RESTRICT` when no dependencies reference the identity. The
    /// actor is authenticated by the trusted administrative channel. User
    /// lifecycle changes require `MANAGE_USERS`, role changes require
    /// `MANAGE_ROLES`; a dropped user's active tokens are revoked in the same
    /// batch as its tombstone.
    pub fn commit_authorized_identity_ddl(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        retention_deadline_us: i64,
        catalog_writes: &[CatalogWrite],
        audit_events: &[AuditEvent],
    ) -> Result<u64, CommitError> {
        commit_authorized_identity_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        )
    }

    /// Authorize and commit role membership changes for an actor authenticated
    /// by the trusted administrative channel. Requires `MANAGE_ROLES`, except
    /// changes to the reserved database-administrator role, which require an
    /// existing administrator.
    pub fn commit_authorized_membership_ddl(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        retention_deadline_us: i64,
        catalog_writes: &[CatalogWrite],
        audit_events: &[AuditEvent],
    ) -> Result<u64, CommitError> {
        commit_authorized_membership_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        )
    }

    /// Authorize system privilege grant/revoke operations for an actor
    /// authenticated by the trusted administrative channel. Only current
    /// database administrators may use this route.
    pub fn commit_authorized_system_privilege_ddl(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        retention_deadline_us: i64,
        catalog_writes: &[CatalogWrite],
        audit_events: &[AuditEvent],
    ) -> Result<u64, CommitError> {
        commit_authorized_system_privilege_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        )
    }

    /// Authorize and commit object grant/revoke changes for an actor
    /// authenticated by the trusted administrative channel. The actor must
    /// own each target, hold `MANAGE_GRANTS` on it, or be a database admin.
    pub fn commit_authorized_object_grant_ddl(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        retention_deadline_us: i64,
        catalog_writes: &[CatalogWrite],
        audit_events: &[AuditEvent],
    ) -> Result<u64, CommitError> {
        commit_authorized_object_grant_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        )
    }

    /// Add or remove one direct schema/table/column object grant using the
    /// authenticated ADMIN path. Authority and catalog dependencies are
    /// revalidated by the serialized commit validator.
    pub fn set_object_grant(&self, request: SetObjectGrantRequest) -> Result<u64, CommitError> {
        let SetObjectGrantRequest {
            invocation_id,
            authenticated_actor_id,
            grantee_id,
            object_id,
            privilege,
            target,
            grant,
            retention_deadline_us,
            audit_event,
        } = request;
        self.set_object_grants_batch(SetObjectGrantBatchRequest {
            invocation_id,
            authenticated_actor_id,
            grantee_id,
            object_id,
            privileges: vec![privilege],
            target,
            grant,
            retention_deadline_us,
            audit_event,
        })
    }

    /// Add or remove several direct grants in one authenticated catalog
    /// publication. Every privilege shares one grantee, object, target, and
    /// direction so authorization and provenance change atomically.
    pub fn set_object_grants_batch(
        &self,
        request: SetObjectGrantBatchRequest,
    ) -> Result<u64, CommitError> {
        let SetObjectGrantBatchRequest {
            invocation_id,
            authenticated_actor_id,
            grantee_id,
            object_id,
            privileges,
            target,
            grant,
            retention_deadline_us,
            audit_event,
        } = request;
        let entries = privileges
            .into_iter()
            .map(|privilege| ObjectGrantEntry {
                privilege,
                target: target.clone(),
            })
            .collect();
        self.set_object_grant_entries_batch(SetObjectGrantEntriesBatchRequest {
            invocation_id,
            authenticated_actor_id,
            grantee_id,
            object_id,
            entries,
            grant,
            retention_deadline_us,
            audit_event,
        })
    }

    /// Add or remove several direct grants with independently scoped targets
    /// in one authenticated catalog publication.
    pub fn set_object_grant_entries_batch(
        &self,
        mut request: SetObjectGrantEntriesBatchRequest,
    ) -> Result<u64, CommitError> {
        let SetObjectGrantEntriesBatchRequest {
            invocation_id,
            authenticated_actor_id,
            grantee_id,
            object_id,
            entries,
            grant,
            retention_deadline_us,
            ref mut audit_event,
        } = request;
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || grantee_id == [0; 16]
            || object_id == [0; 16]
            || entries.is_empty()
            || entries.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
            || entries.iter().enumerate().any(|(index, entry)| {
                entries[..index].iter().any(|previous| {
                    previous.privilege == entry.privilege && previous.target == entry.target
                })
            })
            || retention_deadline_us <= 0
            || audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::AuthorityChange
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(
                if audit_event.action != AuditAction::AuthorityChange
                    || audit_event.result != AuditResult::Succeeded
                    || audit_event.actor_id != Some(authenticated_actor_id)
                    || audit_event.correlation_id != invocation_id
                {
                    CommitError::InvalidAudit
                } else {
                    CommitError::InvalidWrite
                },
            );
        }

        let read = self
            .database
            .begin_read()
            .map_err(|error| CommitError::Storage(error.to_string()))?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|error| CommitError::Storage(error.to_string()))?;
        let principal_key = id_catalog_key(10, grantee_id);
        let principal_bytes = catalog
            .get(principal_key.as_slice())
            .map_err(|error| CommitError::Storage(error.to_string()))?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        let principal_kind = read_catalog_u16(&principal_key, &principal_bytes, 10, 1)
            .map_err(table_lifecycle_commit_error)?;
        if !matches!(principal_kind, 1 | 2) {
            return Err(CommitError::InvalidCatalog);
        }
        let principal_epoch = read_catalog_u64(&principal_key, &principal_bytes, 10, 4)
            .map_err(table_lifecycle_commit_error)?;
        let next_principal_epoch = principal_epoch
            .checked_add(1)
            .ok_or(CommitError::InvalidCatalog)?;

        let object_key = id_catalog_key(2, object_id);
        let object_bytes = catalog
            .get(object_key.as_slice())
            .map_err(|error| CommitError::Storage(error.to_string()))?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        let object_kind = read_catalog_u16(&object_key, &object_bytes, 2, 1)
            .map_err(table_lifecycle_commit_error)?;
        let mut grant_specs = Vec::<(u16, u16, [u8; 16], Vec<u8>)>::new();
        let mut all_target_ids = HashSet::new();
        let mut unique_grants = HashSet::new();
        for entry in &entries {
            let code = entry.privilege.code();
            let (scope, target_ids) = match &entry.target {
                ObjectGrantTarget::Object => (1u16, vec![object_id]),
                ObjectGrantTarget::AllColumns => {
                    if object_kind != 3 || !matches!(code, 2 | 3 | 4 | 6) {
                        return Err(CommitError::InvalidCatalog);
                    }
                    let prefix = 6u16.to_be_bytes();
                    let mut column_ids = Vec::new();
                    let mut scanned = 0usize;
                    for entry in catalog
                        .range(prefix.as_slice()..)
                        .map_err(|error| CommitError::Storage(error.to_string()))?
                    {
                        let (key, value) =
                            entry.map_err(|error| CommitError::Storage(error.to_string()))?;
                        let key = key.value();
                        if !key.starts_with(&prefix) {
                            break;
                        }
                        scanned += 1;
                        if scanned > crate::snapshot::MAX_CATALOG_FAMILY_SCAN
                            || key.len() != 19
                            || key[2] != 1
                        {
                            return Err(CommitError::InvalidCatalog);
                        }
                        let column_id: [u8; 16] = key[3..19]
                            .try_into()
                            .map_err(|_| CommitError::InvalidCatalog)?;
                        if read_catalog_id(key, value.value(), 6, 1)
                            .map_err(table_lifecycle_commit_error)?
                            == object_id
                        {
                            column_ids.push(column_id);
                            if column_ids.len() > crate::tuple::MAX_COLUMNS {
                                return Err(CommitError::InvalidCatalog);
                            }
                        }
                    }
                    if column_ids.is_empty() {
                        return Err(CommitError::InvalidCatalog);
                    }
                    (2, column_ids)
                }
                ObjectGrantTarget::Column(column_id) => {
                    if object_kind != 3 || *column_id == [0; 16] {
                        return Err(CommitError::InvalidCatalog);
                    }
                    let column_key = id_catalog_key(6, *column_id);
                    let column_bytes = catalog
                        .get(column_key.as_slice())
                        .map_err(|error| CommitError::Storage(error.to_string()))?
                        .map(|value| value.value().to_vec())
                        .ok_or(CommitError::InvalidCatalog)?;
                    if read_catalog_id(&column_key, &column_bytes, 6, 1)
                        .map_err(table_lifecycle_commit_error)?
                        != object_id
                    {
                        return Err(CommitError::InvalidCatalog);
                    }
                    (2, vec![*column_id])
                }
                ObjectGrantTarget::Columns(column_ids) => {
                    if object_kind != 3
                        || !(2..=64).contains(&column_ids.len())
                        || column_ids.contains(&[0; 16])
                        || column_ids.iter().copied().collect::<HashSet<_>>().len()
                            != column_ids.len()
                    {
                        return Err(CommitError::InvalidCatalog);
                    }
                    for column_id in column_ids {
                        let column_key = id_catalog_key(6, *column_id);
                        let column_bytes = catalog
                            .get(column_key.as_slice())
                            .map_err(|error| CommitError::Storage(error.to_string()))?
                            .map(|value| value.value().to_vec())
                            .ok_or(CommitError::InvalidCatalog)?;
                        if read_catalog_id(&column_key, &column_bytes, 6, 1)
                            .map_err(table_lifecycle_commit_error)?
                            != object_id
                        {
                            return Err(CommitError::InvalidCatalog);
                        }
                    }
                    (2, column_ids.clone())
                }
            };
            let valid_scope = matches!(
                (object_kind, code, scope),
                (1, 1 | 7 | 8 | 10 | 11, 1) | (3, 2..=4 | 6, 1 | 2) | (3, 5 | 8 | 9 | 10 | 11, 1)
            );
            if !valid_scope {
                return Err(CommitError::InvalidCatalog);
            }
            for target_id in target_ids {
                if !unique_grants.insert((code, scope, target_id)) {
                    return Err(CommitError::InvalidWrite);
                }
                all_target_ids.insert(target_id);
                grant_specs.push((
                    code,
                    scope,
                    target_id,
                    object_grant_catalog_key(grantee_id, object_id, code, scope, target_id),
                ));
            }
        }
        let mut target_ids = all_target_ids.into_iter().collect::<Vec<_>>();
        target_ids.sort_unstable();
        if grant_specs
            .len()
            .saturating_add(target_ids.len())
            .saturating_add(1)
            > crate::authorization::MAX_AUTHORIZATION_INPUTS
        {
            return Err(CommitError::InvalidWrite);
        }

        let grant_keys = grant_specs
            .iter()
            .map(|(_, _, _, grant_key)| grant_key.clone())
            .collect::<Vec<_>>();
        let mut current_grants = Vec::with_capacity(grant_specs.len());
        for (_, _, _, grant_key) in &grant_specs {
            let current = catalog
                .get(grant_key.as_slice())
                .map_err(|error| CommitError::Storage(error.to_string()))?
                .map(|value| value.value().to_vec());
            if grant == current.is_some() {
                return Err(CommitError::InvalidWrite);
            }
            current_grants.push(current);
        }
        let other_grant_targets = if grant {
            HashSet::new()
        } else {
            let grant_prefix = 12u16.to_be_bytes();
            let target_id_set = target_ids.iter().copied().collect::<HashSet<_>>();
            let mut remaining_targets = HashSet::new();
            for entry in catalog
                .range(grant_prefix.as_slice()..)
                .map_err(|error| CommitError::Storage(error.to_string()))?
            {
                let (key, _) = entry.map_err(|error| CommitError::Storage(error.to_string()))?;
                let key = key.value();
                if !key.starts_with(&grant_prefix) {
                    break;
                }
                if key.len() == 59
                    && key[2] == 1
                    && key[3..19] == grantee_id
                    && !grant_keys
                        .iter()
                        .any(|grant_key| grant_key.as_slice() == key)
                {
                    let target_id: [u8; 16] = key[43..59]
                        .try_into()
                        .map_err(|_| CommitError::InvalidCatalog)?;
                    if target_id_set.contains(&target_id) {
                        remaining_targets.insert(target_id);
                    }
                }
            }
            remaining_targets
        };
        let mut writes = Vec::with_capacity(grant_specs.len() * 2 + 1);
        for ((_, _, _, grant_key), current_grant) in grant_specs.iter().zip(current_grants) {
            let payload = if grant {
                Some(
                    encode_envelope(
                        &family_schema(12).ok_or(CommitError::InvalidCatalog)?,
                        grant_key,
                        &[
                            CatalogField {
                                tag: 1,
                                wire_type: WireType::Id,
                                payload: authenticated_actor_id.to_vec(),
                            },
                            CatalogField {
                                tag: 2,
                                wire_type: WireType::U64,
                                payload: 1u64.to_le_bytes().to_vec(),
                            },
                        ],
                    )
                    .map_err(|_| CommitError::InvalidCatalog)?,
                )
            } else {
                None
            };
            writes.push(CatalogWrite {
                key: grant_key.clone(),
                expected_value: current_grant,
                payload,
            });
        }
        for target_id in &target_ids {
            let dependency_key = dependency_catalog_key(grantee_id, *target_id, 5);
            let current_dependency = catalog
                .get(dependency_key.as_slice())
                .map_err(|error| CommitError::Storage(error.to_string()))?
                .map(|value| value.value().to_vec());
            let needs_dependency = if grant {
                current_dependency.is_none()
            } else {
                current_dependency.is_some() && !other_grant_targets.contains(target_id)
            };
            if needs_dependency {
                let edge = new_dependency_write(grantee_id, *target_id, 5)
                    .map_err(table_lifecycle_commit_error)?;
                writes.push(CatalogWrite {
                    expected_value: current_dependency,
                    payload: if grant { edge.payload } else { None },
                    ..edge
                });
            }
        }
        let updated_principal = rewrite_catalog_u64_field(
            &principal_key,
            &principal_bytes,
            10,
            4,
            next_principal_epoch,
        )
        .map_err(table_lifecycle_commit_error)?;
        writes.push(CatalogWrite {
            key: principal_key,
            expected_value: Some(principal_bytes),
            payload: Some(updated_principal),
        });
        drop(catalog);
        drop(read);

        audit_event.target_ids = vec![grantee_id, object_id];
        if target_ids.len() + audit_event.target_ids.len() <= crate::audit::MAX_TARGETS {
            audit_event.target_ids.extend(target_ids);
        }
        audit_event.target_ids.sort_unstable();
        audit_event.target_ids.dedup();
        self.commit_authorized_object_grant_ddl(
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &writes,
            std::slice::from_ref(audit_event),
        )
    }
}
