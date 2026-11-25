use super::admission::*;
use super::catalog_helpers::*;
use super::*;
use crate::audit::{AuditAction, AuditEvent, AuditResult};
use crate::catalog::{
    CatalogField, WireType, decode_builtin_scalar_type, decode_policy_target,
    decode_registered_envelope, encode_envelope, family_schema,
};
use crate::commit::{
    CatalogWrite, CommitError, IndexWrite, RowWrite, TableTruncateCommitRequest,
    commit_authorized_column_drop_ddl_with_fences, commit_authorized_column_rename_ddl_with_fences,
    commit_authorized_schema_create_ddl_with_fences, commit_authorized_schema_drop_ddl_with_fences,
    commit_authorized_schema_owner_ddl_with_fences,
    commit_authorized_schema_rename_ddl_with_fences, commit_authorized_table_drop_ddl_with_fences,
    commit_authorized_table_owner_ddl_with_fences, commit_authorized_table_rename_ddl_with_fences,
    commit_authorized_table_truncate_ddl_with_fences, record_aborted_outcome,
    validate_table_truncate_ddl_authority,
};
use crate::index_locator::IndexLocator;
use crate::row_record::RowRecord;
use crate::schema::{CATALOG, INDEXES, ROW_INDEXES, ROWS};
use crate::snapshot::MAX_CATALOG_FAMILY_SCAN;
use crate::tuple::{ColumnSchema, ScalarType, encode_tuple};
use redb::ReadableDatabase;
use std::collections::{HashMap, HashSet};
use zeroize::Zeroize;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchemaLifecycleError {
    InvalidRequest,
    InvalidAudit,
    RandomnessUnavailable,
    SchemaIdGenerationFailed,
    Unauthorized,
    Storage,
    Commit(CommitError),
}

impl std::fmt::Display for SchemaLifecycleError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "schema lifecycle operation failed: {self:?}")
    }
}

impl std::error::Error for SchemaLifecycleError {}

pub(super) fn schema_lifecycle_from_table_error(
    error: TableLifecycleError,
) -> SchemaLifecycleError {
    match error {
        TableLifecycleError::Commit(error) => SchemaLifecycleError::Commit(error),
        TableLifecycleError::Storage => SchemaLifecycleError::Storage,
        _ => SchemaLifecycleError::Commit(CommitError::InvalidCatalog),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedSchema {
    pub schema_id: [u8; 16],
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DroppedSchema {
    pub schema_id: [u8; 16],
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DroppedTable {
    pub table_id: [u8; 16],
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TruncatedTable {
    pub table_id: [u8; 16],
    pub deleted_rows: usize,
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DroppedColumn {
    pub table_id: [u8; 16],
    pub column_id: [u8; 16],
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddedTableColumn {
    pub table_id: [u8; 16],
    pub column_id: [u8; 16],
    pub schema_version: u64,
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TableLifecycleError {
    InvalidRequest,
    InvalidAudit,
    RandomnessUnavailable,
    ObjectIdGenerationFailed,
    Unauthorized,
    Storage,
    Commit(CommitError),
}

impl std::fmt::Display for TableLifecycleError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "table lifecycle operation failed: {self:?}")
    }
}

impl std::error::Error for TableLifecycleError {}

pub(super) fn table_lifecycle_commit_error(error: TableLifecycleError) -> CommitError {
    match error {
        TableLifecycleError::InvalidAudit => CommitError::InvalidAudit,
        TableLifecycleError::Unauthorized => CommitError::DdlAuthorizationDenied,
        TableLifecycleError::Storage => {
            CommitError::Storage("storage manager catalog read failed".to_owned())
        }
        TableLifecycleError::Commit(error) => error,
        TableLifecycleError::InvalidRequest
        | TableLifecycleError::RandomnessUnavailable
        | TableLifecycleError::ObjectIdGenerationFailed => CommitError::InvalidWrite,
    }
}

impl<'db> StorageManager<'db> {
    /// Create a schema and assign its initial owner atomically. The actor must
    /// hold CREATE_SCHEMA or database-administrator membership.
    pub fn create_schema(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        name: &str,
        owner_id: [u8; 16],
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<CreatedSchema, SchemaLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || owner_id == [0; 16]
            || retention_deadline_us <= 0
            || !is_normalized_principal_name(name)
        {
            return Err(SchemaLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(SchemaLifecycleError::InvalidAudit);
        }
        let mut schema_id = None;
        for _ in 0..8 {
            let mut candidate = [0; 16];
            if getrandom::fill(&mut candidate).is_err() {
                candidate.zeroize();
                return Err(SchemaLifecycleError::RandomnessUnavailable);
            }
            if candidate == [0; 16] {
                candidate.zeroize();
                continue;
            }
            let mut conflict = false;
            for family in [2, 3, 17] {
                let key = id_catalog_key(family, candidate);
                let read = self
                    .database
                    .begin_read()
                    .map_err(|_| SchemaLifecycleError::Storage)?;
                let catalog = read
                    .open_table(CATALOG)
                    .map_err(|_| SchemaLifecycleError::Storage)?;
                conflict |= catalog
                    .get(key.as_slice())
                    .map_err(|_| SchemaLifecycleError::Storage)?
                    .is_some();
            }
            if !conflict {
                schema_id = Some(candidate);
                break;
            }
            candidate.zeroize();
        }
        let schema_id = schema_id.ok_or(SchemaLifecycleError::SchemaIdGenerationFailed)?;
        let object_key = id_catalog_key(2, schema_id);
        let schema_key = id_catalog_key(3, schema_id);
        let object_payload = encode_envelope(
            &family_schema(2).ok_or(SchemaLifecycleError::Storage)?,
            &object_key,
            &[
                CatalogField {
                    tag: 1,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::Text,
                    payload: name.as_bytes().to_vec(),
                },
                CatalogField {
                    tag: 4,
                    wire_type: WireType::Id,
                    payload: owner_id.to_vec(),
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
        .map_err(|_| SchemaLifecycleError::InvalidRequest)?;
        let schema_payload = encode_envelope(
            &family_schema(3).ok_or(SchemaLifecycleError::Storage)?,
            &schema_key,
            &[CatalogField {
                tag: 1,
                wire_type: WireType::Ids,
                payload: 0u32.to_le_bytes().to_vec(),
            }],
        )
        .map_err(|_| SchemaLifecycleError::InvalidRequest)?;
        audit_event.target_ids = vec![schema_id];
        let writes = [
            CatalogWrite {
                key: object_key,
                expected_value: None,
                payload: Some(object_payload),
            },
            CatalogWrite {
                key: schema_key,
                expected_value: None,
                payload: Some(schema_payload),
            },
        ];
        let commit_sequence = commit_authorized_schema_create_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &writes,
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => SchemaLifecycleError::Unauthorized,
            error => SchemaLifecycleError::Commit(error),
        })?;
        Ok(CreatedSchema {
            schema_id,
            commit_sequence,
        })
    }

    /// Drop an empty schema and its object record atomically. The schema owner
    /// or a database administrator must authorize the operation.
    pub fn drop_schema(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        schema_id: [u8; 16],
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<DroppedSchema, SchemaLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || schema_id == [0; 16]
            || retention_deadline_us <= 0
        {
            return Err(SchemaLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(SchemaLifecycleError::InvalidAudit);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| SchemaLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| SchemaLifecycleError::Storage)?;
        let object_key = id_catalog_key(2, schema_id);
        let schema_key = id_catalog_key(3, schema_id);
        let reservation_key = retired_object_id_catalog_key(schema_id);
        let object_bytes = catalog
            .get(object_key.as_slice())
            .map_err(|_| SchemaLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(SchemaLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let schema_bytes = catalog
            .get(schema_key.as_slice())
            .map_err(|_| SchemaLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(SchemaLifecycleError::Commit(CommitError::InvalidCatalog))?;
        decode_registered_envelope(&object_bytes)
            .map_err(|_| SchemaLifecycleError::Commit(CommitError::InvalidCatalog))?;
        decode_registered_envelope(&schema_bytes)
            .map_err(|_| SchemaLifecycleError::Commit(CommitError::InvalidCatalog))?;
        if catalog
            .get(reservation_key.as_slice())
            .map_err(|_| SchemaLifecycleError::Storage)?
            .is_some()
        {
            return Err(SchemaLifecycleError::Commit(CommitError::InvalidCatalog));
        }
        let reservation_payload = encode_envelope(
            &family_schema(17).ok_or(SchemaLifecycleError::Storage)?,
            &reservation_key,
            &[],
        )
        .map_err(|_| SchemaLifecycleError::Commit(CommitError::InvalidCatalog))?;
        audit_event.target_ids = vec![schema_id];
        let writes = [
            CatalogWrite {
                key: object_key,
                expected_value: Some(object_bytes),
                payload: None,
            },
            CatalogWrite {
                key: schema_key,
                expected_value: Some(schema_bytes),
                payload: None,
            },
            CatalogWrite {
                key: reservation_key,
                expected_value: None,
                payload: Some(reservation_payload),
            },
        ];
        let commit_sequence = commit_authorized_schema_drop_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &writes,
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => SchemaLifecycleError::Unauthorized,
            error => SchemaLifecycleError::Commit(error),
        })?;
        Ok(DroppedSchema {
            schema_id,
            commit_sequence,
        })
    }

    /// Transfer a schema's ownership without adding table data privileges.
    pub fn set_schema_owner(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        schema_id: [u8; 16],
        new_owner_id: [u8; 16],
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, SchemaLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || schema_id == [0; 16]
            || new_owner_id == [0; 16]
            || retention_deadline_us <= 0
        {
            return Err(SchemaLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(SchemaLifecycleError::InvalidAudit);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| SchemaLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| SchemaLifecycleError::Storage)?;
        let object_key = id_catalog_key(2, schema_id);
        let schema_key = id_catalog_key(3, schema_id);
        let owner_key = id_catalog_key(10, new_owner_id);
        let object_current = catalog
            .get(object_key.as_slice())
            .map_err(|_| SchemaLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(SchemaLifecycleError::InvalidRequest)?;
        let _schema_current = catalog
            .get(schema_key.as_slice())
            .map_err(|_| SchemaLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(SchemaLifecycleError::InvalidRequest)?;
        let owner_current = catalog
            .get(owner_key.as_slice())
            .map_err(|_| SchemaLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(SchemaLifecycleError::InvalidRequest)?;
        if read_catalog_u16(&object_key, &object_current, 2, 1)
            .map_err(schema_lifecycle_from_table_error)?
            != 1
            || read_catalog_id(&object_key, &object_current, 2, 4)
                .map_err(schema_lifecycle_from_table_error)?
                == new_owner_id
        {
            return Err(SchemaLifecycleError::InvalidRequest);
        }
        let owner_record = decode_registered_envelope(&owner_current)
            .map_err(|_| SchemaLifecycleError::InvalidRequest)?;
        let owner_kind = owner_record
            .fields
            .iter()
            .find(|field| field.tag == 1 && field.wire_type == WireType::U16)
            .and_then(|field| <[u8; 2]>::try_from(field.payload.as_slice()).ok())
            .map(u16::from_le_bytes);
        let owner_epoch = owner_record
            .fields
            .iter()
            .find(|field| field.tag == 4 && field.wire_type == WireType::U64)
            .and_then(|field| <[u8; 8]>::try_from(field.payload.as_slice()).ok())
            .map(u64::from_le_bytes)
            .unwrap_or_default();
        if !matches!(owner_kind, Some(1 | 2)) || owner_epoch == 0 {
            return Err(SchemaLifecycleError::InvalidRequest);
        }
        let next_object_version = read_catalog_u64(&object_key, &object_current, 2, 5)
            .map_err(schema_lifecycle_from_table_error)?
            .checked_add(1)
            .ok_or(SchemaLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let object_payload = rewrite_catalog_fields(
            &object_key,
            &object_current,
            2,
            &[
                (4, WireType::Id, new_owner_id.to_vec()),
                (5, WireType::U64, next_object_version.to_le_bytes().to_vec()),
            ],
        )
        .map_err(schema_lifecycle_from_table_error)?;
        drop(catalog);
        drop(read);
        audit_event.target_ids = vec![schema_id];
        commit_authorized_schema_owner_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &[CatalogWrite {
                key: object_key,
                expected_value: Some(object_current),
                payload: Some(object_payload),
            }],
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => SchemaLifecycleError::Unauthorized,
            error => SchemaLifecycleError::Commit(error),
        })
    }

    /// Rename a schema while preserving its stable ID and contents.
    #[allow(clippy::too_many_arguments)]
    pub fn rename_schema(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        schema_id: [u8; 16],
        name: &str,
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, SchemaLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || schema_id == [0; 16]
            || retention_deadline_us <= 0
            || !is_normalized_principal_name(name)
        {
            return Err(SchemaLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(SchemaLifecycleError::InvalidAudit);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| SchemaLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| SchemaLifecycleError::Storage)?;
        let object_key = id_catalog_key(2, schema_id);
        let schema_key = id_catalog_key(3, schema_id);
        let object_current = catalog
            .get(object_key.as_slice())
            .map_err(|_| SchemaLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(SchemaLifecycleError::InvalidRequest)?;
        let _schema_current = catalog
            .get(schema_key.as_slice())
            .map_err(|_| SchemaLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(SchemaLifecycleError::InvalidRequest)?;
        if read_catalog_u16(&object_key, &object_current, 2, 1)
            .map_err(schema_lifecycle_from_table_error)?
            != 1
            || read_catalog_field(&object_key, &object_current, 2, 3)
                .map_err(schema_lifecycle_from_table_error)?
                == name.as_bytes()
        {
            return Err(SchemaLifecycleError::InvalidRequest);
        }
        let next_object_version = read_catalog_u64(&object_key, &object_current, 2, 5)
            .map_err(schema_lifecycle_from_table_error)?
            .checked_add(1)
            .ok_or(SchemaLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let object_payload = rewrite_catalog_fields(
            &object_key,
            &object_current,
            2,
            &[
                (3, WireType::Text, name.as_bytes().to_vec()),
                (5, WireType::U64, next_object_version.to_le_bytes().to_vec()),
            ],
        )
        .map_err(schema_lifecycle_from_table_error)?;
        drop(catalog);
        drop(read);
        audit_event.target_ids = vec![schema_id];
        commit_authorized_schema_rename_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &[CatalogWrite {
                key: object_key,
                expected_value: Some(object_current),
                payload: Some(object_payload),
            }],
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => SchemaLifecycleError::Unauthorized,
            error => SchemaLifecycleError::Commit(error),
        })
    }

    /// Remove every row and physical index membership from a table atomically,
    /// retaining its catalog definitions. Incoming foreign keys RESTRICT the
    /// operation. The table's exclusive DDL fence covers the complete scan.
    pub fn truncate_table(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        table_id: [u8; 16],
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<TruncatedTable, TableLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || table_id == [0; 16]
            || retention_deadline_us <= 0
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(TableLifecycleError::InvalidAudit);
        }

        let mut commit_started = false;
        let result =
            (|| {
                let _fence_guard = self
                    .ddl_fences
                    .acquire_exclusive(&[table_id])
                    .map_err(|_| TableLifecycleError::Commit(CommitError::DdlFenceUnavailable))?;
                let read = self
                    .database
                    .begin_read()
                    .map_err(|_| TableLifecycleError::Storage)?;
                let catalog = read
                    .open_table(CATALOG)
                    .map_err(|_| TableLifecycleError::Storage)?;
                let table_catalog_key = id_catalog_key(5, table_id);
                let table_catalog_bytes = catalog
                    .get(table_catalog_key.as_slice())
                    .map_err(|_| TableLifecycleError::Storage)?
                    .map(|value| value.value().to_vec())
                    .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                let object_catalog_key = id_catalog_key(2, table_id);
                let object_catalog_bytes = catalog
                    .get(object_catalog_key.as_slice())
                    .map_err(|_| TableLifecycleError::Storage)?
                    .map(|value| value.value().to_vec())
                    .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                let next_epoch = read_catalog_u64(&table_catalog_key, &table_catalog_bytes, 5, 3)?
                    .checked_add(1)
                    .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                let next_object_version =
                    read_catalog_u64(&object_catalog_key, &object_catalog_bytes, 2, 5)?
                        .checked_add(1)
                        .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;

                let updated_table = rewrite_catalog_u64_field(
                    &table_catalog_key,
                    &table_catalog_bytes,
                    5,
                    3,
                    next_epoch,
                )?;
                let updated_object = rewrite_catalog_u64_field(
                    &object_catalog_key,
                    &object_catalog_bytes,
                    2,
                    5,
                    next_object_version,
                )?;
                let catalog_writes = [
                    CatalogWrite {
                        key: table_catalog_key,
                        expected_value: Some(table_catalog_bytes),
                        payload: Some(updated_table),
                    },
                    CatalogWrite {
                        key: object_catalog_key,
                        expected_value: Some(object_catalog_bytes),
                        payload: Some(updated_object),
                    },
                ];
                audit_event.target_ids = vec![table_id];
                let audit_events = [audit_event];
                validate_table_truncate_ddl_authority(
                    self.database,
                    authenticated_actor_id,
                    table_id,
                    &catalog_writes,
                    &audit_events,
                )
                .map_err(|error| match error {
                    CommitError::DdlAuthorizationDenied => TableLifecycleError::Unauthorized,
                    error => TableLifecycleError::Commit(error),
                })?;

                // TRUNCATE RESTRICT follows the relation dependency graph, even if
                // a referencing table currently has no rows.
                let constraint_prefix = 8u16.to_be_bytes();
                let mut scanned_constraints = 0usize;
                for entry in catalog
                    .range(constraint_prefix.as_slice()..)
                    .map_err(|_| TableLifecycleError::Storage)?
                {
                    let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
                    let key = key.value();
                    if !key.starts_with(&constraint_prefix) {
                        break;
                    }
                    scanned_constraints += 1;
                    if scanned_constraints > MAX_CATALOG_FAMILY_SCAN {
                        return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
                    }
                    let key = key.to_vec();
                    let bytes = value.value().to_vec();
                    if read_catalog_u16(&key, &bytes, 8, 2)? != 5 {
                        continue;
                    }
                    let source_table_id = read_catalog_id(&key, &bytes, 8, 1)?;
                    let referenced_constraint_id = read_catalog_id(&key, &bytes, 8, 5)?;
                    let referenced_key = id_catalog_key(8, referenced_constraint_id);
                    let referenced_bytes = catalog
                        .get(referenced_key.as_slice())
                        .map_err(|_| TableLifecycleError::Storage)?
                        .map(|value| value.value().to_vec())
                        .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                    if source_table_id != table_id
                        && read_catalog_id(&referenced_key, &referenced_bytes, 8, 1)? == table_id
                    {
                        return Err(TableLifecycleError::Commit(
                            CommitError::TableHasDependencies,
                        ));
                    }
                }

                let rows = read
                    .open_table(ROWS)
                    .map_err(|_| TableLifecycleError::Storage)?;
                let indexes = read
                    .open_table(INDEXES)
                    .map_err(|_| TableLifecycleError::Storage)?;
                let reverse_indexes = read
                    .open_table(ROW_INDEXES)
                    .map_err(|_| TableLifecycleError::Storage)?;
                let mut row_writes = Vec::new();
                let mut index_writes = Vec::new();
                let mut row_keys = HashSet::<[u8; 32]>::new();
                let mut row_versions = HashMap::<[u8; 32], u64>::new();
                let mut total_bytes = 0usize;
                for entry in rows
                    .range(table_id.as_slice()..)
                    .map_err(|_| TableLifecycleError::Storage)?
                {
                    let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
                    let key = key.value();
                    if !key.starts_with(&table_id) {
                        break;
                    }
                    if key.len() != 32 {
                        return Err(TableLifecycleError::Commit(CommitError::InvalidRow));
                    }
                    if row_writes.len() >= MAX_TABLE_TRUNCATE_ROWS {
                        return Err(TableLifecycleError::Commit(
                            CommitError::TableTruncateLimitExceeded,
                        ));
                    }
                    let row_key: [u8; 32] = key
                        .try_into()
                        .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidRow))?;
                    if row_key[16..] == [0; 16] {
                        return Err(TableLifecycleError::Commit(CommitError::InvalidRow));
                    }
                    let row_bytes = value.value().to_vec();
                    total_bytes = total_bytes.checked_add(row_bytes.len()).ok_or(
                        TableLifecycleError::Commit(CommitError::TableTruncateLimitExceeded),
                    )?;
                    if total_bytes > MAX_TABLE_TRUNCATE_BYTES {
                        return Err(TableLifecycleError::Commit(
                            CommitError::TableTruncateLimitExceeded,
                        ));
                    }
                    let row = RowRecord::decode(&row_bytes)
                        .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidRow))?;
                    row_keys.insert(row_key);
                    row_versions.insert(row_key, row.row_version);
                    row_writes.push(RowWrite {
                        key: row_key,
                        expected_version: Some(row.row_version),
                        payload: None,
                    });

                    for reverse_entry in reverse_indexes
                        .range(row_key.as_slice()..)
                        .map_err(|_| TableLifecycleError::Storage)?
                    {
                        let (reverse_key, marker) =
                            reverse_entry.map_err(|_| TableLifecycleError::Storage)?;
                        let reverse_key = reverse_key.value();
                        if !reverse_key.starts_with(&row_key) {
                            break;
                        }
                        if reverse_key.len() <= row_key.len() || !marker.value().is_empty() {
                            return Err(TableLifecycleError::Commit(CommitError::InvalidIndex));
                        }
                        let mut index_key = reverse_key[row_key.len()..].to_vec();
                        index_key.extend_from_slice(&row_key[16..]);
                        let locator_bytes = indexes
                            .get(index_key.as_slice())
                            .map_err(|_| TableLifecycleError::Storage)?
                            .map(|value| value.value().to_vec())
                            .ok_or(TableLifecycleError::Commit(CommitError::InvalidIndex))?;
                        let locator = IndexLocator::decode_for_key(&index_key, &locator_bytes)
                            .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidIndex))?;
                        if locator.row_id != row_key[16..] || locator.row_version != row.row_version
                        {
                            return Err(TableLifecycleError::Commit(CommitError::InvalidIndex));
                        }
                        if index_writes.len() >= MAX_TABLE_TRUNCATE_INDEX_ENTRIES {
                            return Err(TableLifecycleError::Commit(
                                CommitError::TableTruncateLimitExceeded,
                            ));
                        }
                        total_bytes = total_bytes
                            .checked_add(index_key.len())
                            .and_then(|bytes| bytes.checked_add(locator_bytes.len()))
                            .ok_or(TableLifecycleError::Commit(
                                CommitError::TableTruncateLimitExceeded,
                            ))?;
                        if total_bytes > MAX_TABLE_TRUNCATE_BYTES {
                            return Err(TableLifecycleError::Commit(
                                CommitError::TableTruncateLimitExceeded,
                            ));
                        }
                        index_writes.push(IndexWrite {
                            key: index_key,
                            row_key,
                            expected_version: Some(locator.row_version),
                            present: false,
                            unique: false,
                            null_exempt: false,
                        });
                    }
                }
                let mut index_ids = HashSet::<[u8; 16]>::new();
                let index_catalog_prefix = 9u16.to_be_bytes();
                let mut scanned_index_definitions = 0usize;
                for entry in catalog
                    .range(index_catalog_prefix.as_slice()..)
                    .map_err(|_| TableLifecycleError::Storage)?
                {
                    let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
                    let key = key.value();
                    if !key.starts_with(&index_catalog_prefix) {
                        break;
                    }
                    scanned_index_definitions += 1;
                    if scanned_index_definitions > MAX_CATALOG_FAMILY_SCAN {
                        return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
                    }
                    let key = key.to_vec();
                    let bytes = value.value().to_vec();
                    if read_catalog_id(&key, &bytes, 9, 1)? == table_id {
                        let index_id: [u8; 16] = key[3..19].try_into().map_err(|_| {
                            TableLifecycleError::Commit(CommitError::InvalidCatalog)
                        })?;
                        index_ids.insert(index_id);
                    }
                }
                let expected_index_keys = index_writes
                    .iter()
                    .map(|write| write.key.clone())
                    .collect::<HashSet<_>>();
                let mut physical_index_keys = HashSet::<Vec<u8>>::new();
                let mut scanned_physical_index_entries = 0usize;
                for index_id in index_ids {
                    for entry in indexes
                        .range(index_id.as_slice()..)
                        .map_err(|_| TableLifecycleError::Storage)?
                    {
                        let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
                        let key = key.value();
                        if !key.starts_with(&index_id) {
                            break;
                        }
                        scanned_physical_index_entries += 1;
                        if scanned_physical_index_entries > MAX_TABLE_TRUNCATE_INDEX_ENTRIES {
                            return Err(TableLifecycleError::Commit(
                                CommitError::TableTruncateLimitExceeded,
                            ));
                        }
                        let index_key = key.to_vec();
                        let locator = IndexLocator::decode_for_key(&index_key, value.value())
                            .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidIndex))?;
                        let mut row_key = [0; 32];
                        row_key[..16].copy_from_slice(&table_id);
                        row_key[16..].copy_from_slice(&locator.row_id);
                        let row_version = row_versions
                            .get(&row_key)
                            .ok_or(TableLifecycleError::Commit(CommitError::InvalidIndex))?;
                        if *row_version != locator.row_version {
                            return Err(TableLifecycleError::Commit(CommitError::InvalidIndex));
                        }
                        let mut reverse_key = row_key.to_vec();
                        reverse_key.extend_from_slice(&index_key[..index_key.len() - 16]);
                        let reverse_marker = reverse_indexes
                            .get(reverse_key.as_slice())
                            .map_err(|_| TableLifecycleError::Storage)?
                            .ok_or(TableLifecycleError::Commit(CommitError::InvalidIndex))?;
                        if !reverse_marker.value().is_empty() {
                            return Err(TableLifecycleError::Commit(CommitError::InvalidIndex));
                        }
                        physical_index_keys.insert(index_key);
                    }
                }
                if physical_index_keys != expected_index_keys {
                    return Err(TableLifecycleError::Commit(CommitError::InvalidIndex));
                }
                // Reject orphan reverse entries instead of leaving them behind in
                // a supposedly empty table's physical index state.
                let mut scanned_reverse_entries = 0usize;
                for entry in reverse_indexes
                    .range(table_id.as_slice()..)
                    .map_err(|_| TableLifecycleError::Storage)?
                {
                    let (key, marker) = entry.map_err(|_| TableLifecycleError::Storage)?;
                    let key = key.value();
                    if !key.starts_with(&table_id) {
                        break;
                    }
                    scanned_reverse_entries += 1;
                    if scanned_reverse_entries > MAX_TABLE_TRUNCATE_INDEX_ENTRIES {
                        return Err(TableLifecycleError::Commit(
                            CommitError::TableTruncateLimitExceeded,
                        ));
                    }
                    if key.len() <= 32 || !marker.value().is_empty() {
                        return Err(TableLifecycleError::Commit(CommitError::InvalidIndex));
                    }
                    let row_key: [u8; 32] = key[..32]
                        .try_into()
                        .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidIndex))?;
                    if !row_keys.contains(&row_key) {
                        return Err(TableLifecycleError::Commit(CommitError::InvalidIndex));
                    }
                }
                drop(reverse_indexes);
                drop(indexes);
                drop(rows);
                drop(catalog);
                drop(read);

                commit_started = true;
                let commit_sequence = commit_authorized_table_truncate_ddl_with_fences(
                    self.database,
                    TableTruncateCommitRequest {
                        invocation_id,
                        principal_id: authenticated_actor_id,
                        table_id,
                        retention_deadline_us,
                        rows: &row_writes,
                        indexes: &index_writes,
                        catalog_writes: &catalog_writes,
                        audit_events: &audit_events,
                    },
                )
                .map_err(|error| match error {
                    CommitError::DdlAuthorizationDenied => TableLifecycleError::Unauthorized,
                    error => TableLifecycleError::Commit(error),
                })?;
                Ok(TruncatedTable {
                    table_id,
                    deleted_rows: row_writes.len(),
                    commit_sequence,
                })
            })();
        if result.is_err() && !commit_started {
            let original_is_storage = matches!(
                &result,
                Err(TableLifecycleError::Storage)
                    | Err(TableLifecycleError::Commit(CommitError::Storage(_)))
            );
            if !original_is_storage {
                record_aborted_outcome(
                    self.database,
                    invocation_id,
                    authenticated_actor_id,
                    retention_deadline_us,
                )
                .map_err(TableLifecycleError::Commit)?;
            }
        }
        result
    }

    /// Drop a table with RESTRICT semantics, removing owned definitions and rows atomically.
    pub fn drop_table(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        table_id: [u8; 16],
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<DroppedTable, TableLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || table_id == [0; 16]
            || retention_deadline_us <= 0
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(TableLifecycleError::InvalidAudit);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| TableLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| TableLifecycleError::Storage)?;
        let mut deletes = HashMap::<Vec<u8>, Vec<u8>>::new();
        let mut owned_ids = HashSet::from([table_id]);
        let mut typed_records = Vec::<(Vec<u8>, Vec<u8>)>::new();
        for family in [5u16, 6, 8, 9, 13] {
            let prefix = family.to_be_bytes();
            let mut scanned = 0usize;
            for entry in catalog
                .range(prefix.as_slice()..)
                .map_err(|_| TableLifecycleError::Storage)?
            {
                let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
                let key = key.value().to_vec();
                if !key.starts_with(&prefix) {
                    break;
                }
                scanned += 1;
                if scanned > MAX_CATALOG_FAMILY_SCAN {
                    return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
                }
                let bytes = value.value().to_vec();
                let record = decode_registered_envelope(&bytes)
                    .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                let belongs = family == 5 && key == id_catalog_key(5, table_id)
                    || family != 5
                        && record
                            .fields
                            .iter()
                            .any(|field| field.tag == 1 && field.payload.as_slice() == table_id);
                if belongs {
                    if key.len() != 19 {
                        return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
                    }
                    owned_ids.insert(
                        key[3..19].try_into().map_err(|_| {
                            TableLifecycleError::Commit(CommitError::InvalidCatalog)
                        })?,
                    );
                    typed_records.push((key, bytes));
                }
            }
        }
        if typed_records
            .iter()
            .all(|(key, _)| key != &id_catalog_key(5, table_id))
        {
            return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
        }
        let mut expression_ids = HashSet::<[u8; 16]>::new();
        let mut policy_ids = HashSet::<[u8; 16]>::new();
        for (key, bytes) in &typed_records {
            let family = u16::from_be_bytes([key[0], key[1]]);
            let record = decode_registered_envelope(bytes)
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            for field in &record.fields {
                if ((family == 6 && matches!(field.tag, 7 | 10))
                    || (family == 8 && field.tag == 4)
                    || (family == 13 && matches!(field.tag, 5 | 6)))
                    && field.payload.len() == 16
                {
                    let id: [u8; 16] = field.payload.as_slice().try_into().unwrap();
                    expression_ids.insert(id);
                    owned_ids.insert(id);
                }
            }
            if family == 13 {
                policy_ids.insert(
                    key[3..19]
                        .try_into()
                        .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?,
                );
            }
        }
        // Include object records for the table's typed children and their defaults.
        let object_prefix = 2u16.to_be_bytes();
        let mut changed = true;
        while changed {
            changed = false;
            let mut scanned = 0usize;
            for entry in catalog
                .range(object_prefix.as_slice()..)
                .map_err(|_| TableLifecycleError::Storage)?
            {
                let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
                let key = key.value().to_vec();
                if !key.starts_with(&object_prefix) {
                    break;
                }
                scanned += 1;
                if scanned > MAX_CATALOG_FAMILY_SCAN {
                    return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
                }
                if key.len() != 19 {
                    return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
                }
                let id: [u8; 16] = key[3..19].try_into().unwrap();
                let bytes = value.value().to_vec();
                let record = decode_registered_envelope(&bytes)
                    .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                let parent = record
                    .fields
                    .iter()
                    .find(|field| field.tag == 2)
                    .and_then(|field| <[u8; 16]>::try_from(field.payload.as_slice()).ok());
                if owned_ids.contains(&id)
                    || parent.is_some_and(|parent| owned_ids.contains(&parent))
                {
                    if owned_ids.insert(id) {
                        changed = true;
                    }
                    deletes.insert(key, bytes);
                }
            }
        }
        for (key, bytes) in typed_records {
            deletes.insert(key, bytes);
        }
        // Removing an owned foreign key changes the referenced table's
        // constraint graph too, so advance that table's write epoch in the
        // same transaction. The table being dropped is already removed.
        let mut referenced_tables = HashSet::new();
        for (key, bytes) in &deletes {
            if key.starts_with(&8u16.to_be_bytes()) && read_catalog_u16(key, bytes, 8, 2)? == 5 {
                let target_constraint_id = read_catalog_id(key, bytes, 8, 5)?;
                let target_constraint_key = id_catalog_key(8, target_constraint_id);
                let target_constraint_bytes = catalog
                    .get(target_constraint_key.as_slice())
                    .map_err(|_| TableLifecycleError::Storage)?
                    .map(|value| value.value().to_vec())
                    .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                let target_table_id =
                    read_catalog_id(&target_constraint_key, &target_constraint_bytes, 8, 1)?;
                if target_table_id != table_id {
                    referenced_tables.insert(target_table_id);
                }
            }
        }
        let mut table_epoch_updates = Vec::with_capacity(referenced_tables.len());
        for referenced_table_id in referenced_tables {
            let key = id_catalog_key(5, referenced_table_id);
            let bytes = catalog
                .get(key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let next_epoch = read_catalog_u64(&key, &bytes, 5, 3)?
                .checked_add(1)
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            table_epoch_updates.push(CatalogWrite {
                key: key.clone(),
                expected_value: Some(bytes.clone()),
                payload: Some(rewrite_catalog_u64_field(&key, &bytes, 5, 3, next_epoch)?),
            });
        }
        for id in &expression_ids {
            let key = id_catalog_key(7, *id);
            if let Some(value) = catalog
                .get(key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
            {
                deletes.insert(key, value.value().to_vec());
            }
        }
        let policy_target_prefix = 14u16.to_be_bytes();
        let mut scanned = 0usize;
        for entry in catalog
            .range(policy_target_prefix.as_slice()..)
            .map_err(|_| TableLifecycleError::Storage)?
        {
            let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
            let key = key.value().to_vec();
            if !key.starts_with(&policy_target_prefix) {
                break;
            }
            scanned += 1;
            if scanned > MAX_CATALOG_FAMILY_SCAN {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            let bytes = value.value().to_vec();
            let target = decode_policy_target(&bytes)
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            if policy_ids.contains(&target.policy_id) {
                deletes.insert(key, bytes);
            }
        }
        // Remove grants on owned objects and their matching authority dependency edges.
        let grants_prefix = 12u16.to_be_bytes();
        let mut scanned = 0usize;
        let mut grant_pairs = Vec::new();
        let mut changed_grantees = HashSet::<[u8; 16]>::new();
        for entry in catalog
            .range(grants_prefix.as_slice()..)
            .map_err(|_| TableLifecycleError::Storage)?
        {
            let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
            let key = key.value().to_vec();
            if !key.starts_with(&grants_prefix) {
                break;
            }
            scanned += 1;
            if scanned > MAX_CATALOG_FAMILY_SCAN {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            let object_id = key
                .get(20..36)
                .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok());
            if key.len() == 59 && object_id.is_some_and(|object_id| owned_ids.contains(&object_id))
            {
                let grantee: [u8; 16] = key[3..19].try_into().unwrap();
                let target: [u8; 16] = key[43..59].try_into().unwrap();
                grant_pairs.push((grantee, target));
                changed_grantees.insert(grantee);
                deletes.insert(key, value.value().to_vec());
            }
        }
        for (grantee, target) in grant_pairs {
            let key = dependency_catalog_key(grantee, target, 5);
            if let Some(value) = catalog
                .get(key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
            {
                deletes.insert(key, value.value().to_vec());
            }
        }
        let mut principal_updates = Vec::with_capacity(changed_grantees.len());
        for grantee_id in changed_grantees {
            let principal_key = id_catalog_key(10, grantee_id);
            let principal_bytes = catalog
                .get(principal_key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?
                .value()
                .to_vec();
            let mut principal = decode_registered_envelope(&principal_bytes)
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let epoch = principal
                .fields
                .iter_mut()
                .find(|field| field.tag == 4 && field.wire_type == WireType::U64)
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let current_epoch = u64::from_le_bytes(
                epoch
                    .payload
                    .as_slice()
                    .try_into()
                    .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?,
            );
            epoch.payload = current_epoch
                .checked_add(1)
                .filter(|next| *next != 0)
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?
                .to_le_bytes()
                .to_vec();
            let payload = encode_envelope(
                &family_schema(10)
                    .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?,
                &principal_key,
                &principal.fields,
            )
            .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            principal_updates.push(CatalogWrite {
                key: principal_key,
                expected_value: Some(principal_bytes),
                payload: Some(payload),
            });
        }
        let dependency_prefix = 15u16.to_be_bytes();
        let mut scanned = 0usize;
        for entry in catalog
            .range(dependency_prefix.as_slice()..)
            .map_err(|_| TableLifecycleError::Storage)?
        {
            let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
            let key = key.value().to_vec();
            if !key.starts_with(&dependency_prefix) {
                break;
            }
            scanned += 1;
            if scanned > MAX_CATALOG_FAMILY_SCAN {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            let dependent: [u8; 16] = key
                .get(3..19)
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?
                .try_into()
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            if owned_ids.contains(&dependent) {
                deletes.insert(key, value.value().to_vec());
            }
        }
        let retired_ids = deletes
            .iter()
            .filter_map(|(key, bytes)| {
                (key.starts_with(&2u16.to_be_bytes())
                    && matches!(read_catalog_u16(key, bytes, 2, 1).ok(), Some(3..=7 | 10)))
                .then(|| <[u8; 16]>::try_from(&key[3..19]).ok())
                .flatten()
            })
            .collect::<HashSet<_>>();
        let mut writes = principal_updates
            .into_iter()
            .chain(table_epoch_updates)
            .chain(
                deletes
                    .into_iter()
                    .map(|(key, expected_value)| CatalogWrite {
                        key,
                        expected_value: Some(expected_value),
                        payload: None,
                    }),
            )
            .collect::<Vec<_>>();
        for retired_id in retired_ids {
            let reservation_key = retired_object_id_catalog_key(retired_id);
            let reservation_payload = encode_envelope(
                &family_schema(17)
                    .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?,
                &reservation_key,
                &[],
            )
            .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            writes.push(CatalogWrite {
                key: reservation_key,
                expected_value: None,
                payload: Some(reservation_payload),
            });
        }
        audit_event.target_ids = vec![table_id];
        let commit_sequence = commit_authorized_table_drop_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &writes,
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => TableLifecycleError::Unauthorized,
            error => TableLifecycleError::Commit(error),
        })?;
        Ok(DroppedTable {
            table_id,
            commit_sequence,
        })
    }

    /// Add one column and backfill every existing row from its immutable default.
    pub fn add_table_column(
        &self,
        request: AddTableColumnRequest,
    ) -> Result<AddedTableColumn, TableLifecycleError> {
        let AddTableColumnRequest {
            invocation_id,
            authenticated_actor_id,
            table_id,
            column,
            retention_deadline_us,
            mut audit_event,
        } = request;
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || table_id == [0; 16]
            || retention_deadline_us <= 0
            || !is_normalized_principal_name(&column.name)
            || column.generated_uuid_identity
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(TableLifecycleError::InvalidAudit);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| TableLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| TableLifecycleError::Storage)?;
        let table_key = id_catalog_key(5, table_id);
        let table_bytes = catalog
            .get(table_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        let old_schema_version = read_catalog_u64(&table_key, &table_bytes, 5, 2)?;
        let old_write_epoch = read_catalog_u64(&table_key, &table_bytes, 5, 3)?;
        let next_schema_version = old_schema_version
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let next_write_epoch = old_write_epoch
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let column_prefix = 6u16.to_be_bytes();
        let mut ordinal = 0usize;
        let mut scanned = 0usize;
        for entry in catalog
            .range(column_prefix.as_slice()..)
            .map_err(|_| TableLifecycleError::Storage)?
        {
            let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
            let key = key.value().to_vec();
            if !key.starts_with(&column_prefix) {
                break;
            }
            scanned += 1;
            if scanned > MAX_CATALOG_FAMILY_SCAN {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            let bytes = value.value().to_vec();
            if read_catalog_id(&key, &bytes, 6, 1)? != table_id {
                continue;
            }
            if read_catalog_field(&key, &bytes, 6, 3)? == column.name.as_bytes() {
                return Err(TableLifecycleError::InvalidRequest);
            }
            ordinal = ordinal
                .checked_add(1)
                .ok_or(TableLifecycleError::InvalidRequest)?;
        }
        if ordinal == 0 || ordinal >= crate::tuple::MAX_COLUMNS {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let type_definitions = load_builtin_type_definitions(self.database)?;
        let type_kind = scalar_type_kind(column.scalar_type);
        let (type_id, type_record) = type_definitions
            .get(&type_kind)
            .ok_or(TableLifecycleError::InvalidRequest)?;
        let type_parameters = scalar_type_parameters(column.scalar_type);
        decode_builtin_scalar_type(type_record, &type_parameters)
            .map_err(|_| TableLifecycleError::InvalidRequest)?;
        let default = match column.default.as_ref() {
            Some(TableColumnDefault::Literal(value)) => {
                let default_mask = u32::from_le_bytes(
                    type_record
                        .fields
                        .iter()
                        .find(|field| field.tag == 5)
                        .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?
                        .payload
                        .as_slice()
                        .try_into()
                        .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?,
                );
                if default_mask & 1 == 0 {
                    return Err(TableLifecycleError::InvalidRequest);
                }
                encode_tuple(
                    &[ColumnSchema {
                        scalar_type: column.scalar_type,
                        nullable: column.nullable,
                    }],
                    1,
                    std::slice::from_ref(value),
                )
                .map_err(|_| TableLifecycleError::InvalidRequest)?;
                value.clone()
            }
            Some(TableColumnDefault::CurrentPrincipalId) => {
                return Err(TableLifecycleError::InvalidRequest);
            }
            None if column.nullable => None,
            None => return Err(TableLifecycleError::InvalidRequest),
        };
        let mut column_id = self.generate_table_object_id()?;
        let expression_id = if column.default.is_some() {
            let candidate = self.generate_table_object_id()?;
            if candidate == column_id {
                column_id.zeroize();
                return Err(TableLifecycleError::ObjectIdGenerationFailed);
            }
            Some(candidate)
        } else {
            None
        };
        let column_key = id_catalog_key(6, column_id);
        let object_key = id_catalog_key(2, column_id);
        let mut writes = vec![
            CatalogWrite {
                key: table_key.clone(),
                expected_value: Some(table_bytes.clone()),
                payload: Some(rewrite_catalog_u64_fields(
                    &table_key,
                    &table_bytes,
                    5,
                    &[(2, next_schema_version), (3, next_write_epoch)],
                )?),
            },
            CatalogWrite {
                key: object_key.clone(),
                expected_value: None,
                payload: Some(
                    encode_envelope(
                        &family_schema(2).ok_or(TableLifecycleError::Storage)?,
                        &object_key,
                        &[
                            CatalogField {
                                tag: 1,
                                wire_type: WireType::U16,
                                payload: 4u16.to_le_bytes().to_vec(),
                            },
                            CatalogField {
                                tag: 2,
                                wire_type: WireType::Id,
                                payload: table_id.to_vec(),
                            },
                            CatalogField {
                                tag: 3,
                                wire_type: WireType::Text,
                                payload: column.name.as_bytes().to_vec(),
                            },
                            CatalogField {
                                tag: 4,
                                wire_type: WireType::Id,
                                payload: authenticated_actor_id.to_vec(),
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
                    .map_err(|_| TableLifecycleError::InvalidRequest)?,
                ),
            },
        ];
        let mut fields = vec![
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: table_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U32,
                payload: (ordinal as u32).to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Text,
                payload: column.name.as_bytes().to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::Id,
                payload: type_id.to_vec(),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::Bytes,
                payload: type_parameters.to_vec(),
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::Bool,
                payload: vec![u8::from(column.nullable)],
            },
        ];
        if let Some(expression_id) = expression_id {
            fields.push(CatalogField {
                tag: 7,
                wire_type: WireType::Id,
                payload: expression_id.to_vec(),
            });
        }
        fields.extend([
            CatalogField {
                tag: 8,
                wire_type: WireType::U16,
                payload: 0u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 9,
                wire_type: WireType::U64,
                payload: next_schema_version.to_le_bytes().to_vec(),
            },
        ]);
        if let Some(expression_id) = expression_id {
            fields.push(CatalogField {
                tag: 10,
                wire_type: WireType::Id,
                payload: expression_id.to_vec(),
            });
        }
        writes.push(CatalogWrite {
            key: column_key.clone(),
            expected_value: None,
            payload: Some(
                encode_envelope(
                    &family_schema(6).ok_or(TableLifecycleError::Storage)?,
                    &column_key,
                    &fields,
                )
                .map_err(|_| TableLifecycleError::InvalidRequest)?,
            ),
        });
        writes.push(new_dependency_write(column_id, table_id, 1)?);
        if let Some(expression_id) = expression_id {
            writes.push(new_default_expression_object_write(
                expression_id,
                ordinal,
                authenticated_actor_id,
            )?);
            writes.push(new_default_expression_write(
                expression_id,
                *type_id,
                type_parameters,
                crate::authorization::Expr::Literal(default),
            )?);
            writes.push(new_dependency_write(column_id, expression_id, 2)?);
        }
        let index_prefix = 9u16.to_be_bytes();
        let mut index_scanned = 0usize;
        for entry in catalog
            .range(index_prefix.as_slice()..)
            .map_err(|_| TableLifecycleError::Storage)?
        {
            let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
            let key = key.value().to_vec();
            if !key.starts_with(&index_prefix) {
                break;
            }
            index_scanned += 1;
            if index_scanned > MAX_CATALOG_FAMILY_SCAN {
                column_id.zeroize();
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            let bytes = value.value().to_vec();
            if read_catalog_id(&key, &bytes, 9, 1)? == table_id {
                writes.push(CatalogWrite {
                    key: key.clone(),
                    expected_value: Some(bytes.clone()),
                    payload: Some(rewrite_catalog_u64_field(
                        &key,
                        &bytes,
                        9,
                        10,
                        next_schema_version,
                    )?),
                });
            }
        }
        drop(catalog);
        drop(read);
        audit_event.target_ids = vec![table_id, column_id];
        audit_event.target_ids.sort_unstable();
        let commit_sequence = self
            .commit_authorized_constraint_ddl(
                invocation_id,
                authenticated_actor_id,
                retention_deadline_us,
                &writes,
                &[audit_event],
            )
            .map_err(|error| match error {
                CommitError::DdlAuthorizationDenied => TableLifecycleError::Unauthorized,
                error => TableLifecycleError::Commit(error),
            })?;
        Ok(AddedTableColumn {
            table_id,
            column_id,
            schema_version: next_schema_version,
            commit_sequence,
        })
    }

    /// Drop one unreferenced table column and rewrite stored tuples atomically.
    pub fn drop_column(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        table_id: [u8; 16],
        column_id: [u8; 16],
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<DroppedColumn, TableLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || table_id == [0; 16]
            || column_id == [0; 16]
            || retention_deadline_us <= 0
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(TableLifecycleError::InvalidAudit);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| TableLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| TableLifecycleError::Storage)?;
        let table_key = id_catalog_key(5, table_id);
        let table_bytes = catalog
            .get(table_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let column_key = id_catalog_key(6, column_id);
        let column_bytes = catalog
            .get(column_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let column_record = decode_registered_envelope(&column_bytes)
            .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let actual_table_id: [u8; 16] = column_record
            .fields
            .iter()
            .find(|field| field.tag == 1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?
            .payload
            .as_slice()
            .try_into()
            .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        if actual_table_id != table_id {
            return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
        }
        let schema_version = read_catalog_u64(&table_key, &table_bytes, 5, 2)?;
        let write_epoch = read_catalog_u64(&table_key, &table_bytes, 5, 3)?;
        let next_schema_version = schema_version
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let next_write_epoch = write_epoch
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let mut writes = Vec::new();
        writes.push(CatalogWrite {
            key: table_key.clone(),
            expected_value: Some(table_bytes.clone()),
            payload: Some(rewrite_catalog_u64_fields(
                &table_key,
                &table_bytes,
                5,
                &[(2, next_schema_version), (3, next_write_epoch)],
            )?),
        });
        let old_ordinal = read_catalog_u32(&column_key, &column_bytes, 6, 2)?;
        let object_key = id_catalog_key(2, column_id);
        let object_bytes = catalog
            .get(object_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        writes.push(CatalogWrite {
            key: column_key.clone(),
            expected_value: Some(column_bytes.clone()),
            payload: None,
        });
        writes.push(CatalogWrite {
            key: object_key,
            expected_value: Some(object_bytes),
            payload: None,
        });
        let mut expression_ids = HashSet::<[u8; 16]>::new();
        for tag in [7u16, 10] {
            if let Some(field) = column_record.fields.iter().find(|field| field.tag == tag) {
                let expression_id: [u8; 16] = field
                    .payload
                    .as_slice()
                    .try_into()
                    .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                expression_ids.insert(expression_id);
            }
        }
        for expression_id in &expression_ids {
            for family in [2u16, 7] {
                let key = id_catalog_key(family, *expression_id);
                if let Some(value) = catalog
                    .get(key.as_slice())
                    .map_err(|_| TableLifecycleError::Storage)?
                {
                    writes.push(CatalogWrite {
                        key,
                        expected_value: Some(value.value().to_vec()),
                        payload: None,
                    });
                }
            }
        }
        let prefix = 6u16.to_be_bytes();
        let mut scanned = 0usize;
        for entry in catalog
            .range(prefix.as_slice()..)
            .map_err(|_| TableLifecycleError::Storage)?
        {
            let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
            let key = key.value().to_vec();
            if !key.starts_with(&prefix) {
                break;
            }
            scanned += 1;
            if scanned > MAX_CATALOG_FAMILY_SCAN {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            if key == column_key {
                continue;
            }
            let bytes = value.value().to_vec();
            if read_catalog_id(&key, &bytes, 6, 1)? != table_id {
                continue;
            }
            let ordinal = read_catalog_u32(&key, &bytes, 6, 2)?;
            if ordinal > old_ordinal {
                writes.push(CatalogWrite {
                    key: key.clone(),
                    expected_value: Some(bytes.clone()),
                    payload: Some(rewrite_catalog_u32_field(&key, &bytes, 6, 2, ordinal - 1)?),
                });
            }
        }
        let index_prefix = 9u16.to_be_bytes();
        let mut scanned = 0usize;
        for entry in catalog
            .range(index_prefix.as_slice()..)
            .map_err(|_| TableLifecycleError::Storage)?
        {
            let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
            let key = key.value().to_vec();
            if !key.starts_with(&index_prefix) {
                break;
            }
            scanned += 1;
            if scanned > MAX_CATALOG_FAMILY_SCAN {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            let bytes = value.value().to_vec();
            if read_catalog_id(&key, &bytes, 9, 1)? != table_id {
                continue;
            }
            writes.push(CatalogWrite {
                key: key.clone(),
                expected_value: Some(bytes.clone()),
                payload: Some(rewrite_catalog_u64_field(
                    &key,
                    &bytes,
                    9,
                    10,
                    next_schema_version,
                )?),
            });
        }
        let grants_prefix = 12u16.to_be_bytes();
        let mut scanned = 0usize;
        let mut grant_pairs = Vec::new();
        for entry in catalog
            .range(grants_prefix.as_slice()..)
            .map_err(|_| TableLifecycleError::Storage)?
        {
            let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
            let key = key.value().to_vec();
            if !key.starts_with(&grants_prefix) {
                break;
            }
            scanned += 1;
            if scanned > MAX_CATALOG_FAMILY_SCAN {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            if key.len() == 59
                && key[20..36] == table_id
                && key[40..42] == 2u16.to_be_bytes()
                && key[43..59] == column_id
            {
                let grantee: [u8; 16] = key[3..19]
                    .try_into()
                    .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                grant_pairs.push((grantee, column_id));
                writes.push(CatalogWrite {
                    key,
                    expected_value: Some(value.value().to_vec()),
                    payload: None,
                });
            }
        }
        let mut dependent_ids = expression_ids.clone();
        dependent_ids.insert(column_id);
        let dependency_prefix = 15u16.to_be_bytes();
        let mut scanned = 0usize;
        for entry in catalog
            .range(dependency_prefix.as_slice()..)
            .map_err(|_| TableLifecycleError::Storage)?
        {
            let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
            let key = key.value().to_vec();
            if !key.starts_with(&dependency_prefix) {
                break;
            }
            scanned += 1;
            if scanned > MAX_CATALOG_FAMILY_SCAN {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            let dependent: [u8; 16] = key
                .get(3..19)
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?
                .try_into()
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let referenced: [u8; 16] = key
                .get(20..36)
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?
                .try_into()
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let kind = u16::from_be_bytes(
                key.get(37..39)
                    .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?
                    .try_into()
                    .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?,
            );
            let remove_grant_edge = kind == 5 && grant_pairs.contains(&(dependent, referenced));
            if dependent_ids.contains(&dependent) || remove_grant_edge {
                writes.push(CatalogWrite {
                    key,
                    expected_value: Some(value.value().to_vec()),
                    payload: None,
                });
            }
        }
        let retired_ids = writes
            .iter()
            .filter(|write| write.key.starts_with(&2u16.to_be_bytes()) && write.payload.is_none())
            .filter_map(|write| write.key.get(3..19))
            .filter_map(|bytes| <[u8; 16]>::try_from(bytes).ok())
            .collect::<HashSet<_>>();
        for retired_id in retired_ids {
            let reservation_key = retired_object_id_catalog_key(retired_id);
            let reservation_payload = encode_envelope(
                &family_schema(17)
                    .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?,
                &reservation_key,
                &[],
            )
            .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            writes.push(CatalogWrite {
                key: reservation_key,
                expected_value: None,
                payload: Some(reservation_payload),
            });
        }
        audit_event.target_ids = vec![table_id, column_id];
        audit_event.target_ids.sort_unstable();
        let commit_sequence = commit_authorized_column_drop_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &writes,
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => TableLifecycleError::Unauthorized,
            error => TableLifecycleError::Commit(error),
        })?;
        Ok(DroppedColumn {
            table_id,
            column_id,
            commit_sequence,
        })
    }

    /// Rename a table while preserving its stable object ID.
    pub fn rename_table(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        table_id: [u8; 16],
        name: &str,
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, TableLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || table_id == [0; 16]
            || retention_deadline_us <= 0
            || !is_normalized_principal_name(name)
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(TableLifecycleError::InvalidAudit);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| TableLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| TableLifecycleError::Storage)?;
        let table_key = id_catalog_key(5, table_id);
        let object_key = id_catalog_key(2, table_id);
        let table_current = catalog
            .get(table_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        let object_current = catalog
            .get(object_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        if read_catalog_u16(&object_key, &object_current, 2, 1)? != 3
            || read_catalog_id(&object_key, &object_current, 2, 2)?
                != read_catalog_id(&table_key, &table_current, 5, 1)?
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let old_name = read_catalog_field(&object_key, &object_current, 2, 3)?;
        if old_name == name.as_bytes() {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let table_epoch = read_catalog_u64(&table_key, &table_current, 5, 3)?
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let object_version = read_catalog_u64(&object_key, &object_current, 2, 5)?
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let table_payload =
            rewrite_catalog_u64_field(&table_key, &table_current, 5, 3, table_epoch)?;
        let object_payload = rewrite_catalog_fields(
            &object_key,
            &object_current,
            2,
            &[
                (3, WireType::Text, name.as_bytes().to_vec()),
                (5, WireType::U64, object_version.to_le_bytes().to_vec()),
            ],
        )?;
        drop(catalog);
        drop(read);
        audit_event.target_ids = vec![table_id];
        commit_authorized_table_rename_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &[
                CatalogWrite {
                    key: table_key,
                    expected_value: Some(table_current),
                    payload: Some(table_payload),
                },
                CatalogWrite {
                    key: object_key,
                    expected_value: Some(object_current),
                    payload: Some(object_payload),
                },
            ],
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => TableLifecycleError::Unauthorized,
            error => TableLifecycleError::Commit(error),
        })
    }

    /// Transfer a table's DDL ownership without granting data privileges.
    #[allow(clippy::too_many_arguments)]
    pub fn set_table_owner(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        table_id: [u8; 16],
        new_owner_id: [u8; 16],
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, TableLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || table_id == [0; 16]
            || new_owner_id == [0; 16]
            || retention_deadline_us <= 0
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(TableLifecycleError::InvalidAudit);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| TableLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| TableLifecycleError::Storage)?;
        let table_key = id_catalog_key(5, table_id);
        let object_key = id_catalog_key(2, table_id);
        let owner_key = id_catalog_key(10, new_owner_id);
        let table_current = catalog
            .get(table_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        let object_current = catalog
            .get(object_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        let owner_current = catalog
            .get(owner_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        let table_schema_id = read_catalog_id(&table_key, &table_current, 5, 1)?;
        if read_catalog_u16(&object_key, &object_current, 2, 1)? != 3
            || read_catalog_id(&object_key, &object_current, 2, 2)? != table_schema_id
            || read_catalog_id(&object_key, &object_current, 2, 4)? == new_owner_id
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let owner_record = decode_registered_envelope(&owner_current)
            .map_err(|_| TableLifecycleError::InvalidRequest)?;
        let owner_kind = owner_record
            .fields
            .iter()
            .find(|field| field.tag == 1 && field.wire_type == WireType::U16)
            .and_then(|field| <[u8; 2]>::try_from(field.payload.as_slice()).ok())
            .map(u16::from_le_bytes);
        let owner_epoch = owner_record
            .fields
            .iter()
            .find(|field| field.tag == 4 && field.wire_type == WireType::U64)
            .and_then(|field| <[u8; 8]>::try_from(field.payload.as_slice()).ok())
            .map(u64::from_le_bytes)
            .unwrap_or_default();
        if !matches!(owner_kind, Some(1 | 2)) || owner_epoch == 0 {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let next_epoch = read_catalog_u64(&table_key, &table_current, 5, 3)?
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let next_object_version = read_catalog_u64(&object_key, &object_current, 2, 5)?
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let table_payload =
            rewrite_catalog_u64_field(&table_key, &table_current, 5, 3, next_epoch)?;
        let object_payload = rewrite_catalog_fields(
            &object_key,
            &object_current,
            2,
            &[
                (4, WireType::Id, new_owner_id.to_vec()),
                (5, WireType::U64, next_object_version.to_le_bytes().to_vec()),
            ],
        )?;
        drop(catalog);
        drop(read);
        audit_event.target_ids = vec![table_id];
        commit_authorized_table_owner_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &[
                CatalogWrite {
                    key: table_key,
                    expected_value: Some(table_current),
                    payload: Some(table_payload),
                },
                CatalogWrite {
                    key: object_key,
                    expected_value: Some(object_current),
                    payload: Some(object_payload),
                },
            ],
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => TableLifecycleError::Unauthorized,
            error => TableLifecycleError::Commit(error),
        })
    }

    /// Rename a column while preserving its stable ID and tuple position.
    #[allow(clippy::too_many_arguments)]
    pub fn rename_column(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        table_id: [u8; 16],
        column_id: [u8; 16],
        name: &str,
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, TableLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || table_id == [0; 16]
            || column_id == [0; 16]
            || retention_deadline_us <= 0
            || !is_normalized_principal_name(name)
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(TableLifecycleError::InvalidAudit);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| TableLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| TableLifecycleError::Storage)?;
        let table_key = id_catalog_key(5, table_id);
        let column_key = id_catalog_key(6, column_id);
        let object_key = id_catalog_key(2, column_id);
        let table_current = catalog
            .get(table_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        let column_current = catalog
            .get(column_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        let object_current = catalog
            .get(object_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        if read_catalog_id(&column_key, &column_current, 6, 1)? != table_id
            || read_catalog_u16(&object_key, &object_current, 2, 1)? != 4
            || read_catalog_id(&object_key, &object_current, 2, 2)? != table_id
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let old_name = read_catalog_field(&column_key, &column_current, 6, 3)?;
        if old_name == name.as_bytes()
            || read_catalog_field(&object_key, &object_current, 2, 3)? != old_name
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let table_epoch = read_catalog_u64(&table_key, &table_current, 5, 3)?
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let object_version = read_catalog_u64(&object_key, &object_current, 2, 5)?
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let table_payload =
            rewrite_catalog_u64_field(&table_key, &table_current, 5, 3, table_epoch)?;
        let column_payload = rewrite_catalog_fields(
            &column_key,
            &column_current,
            6,
            &[(3, WireType::Text, name.as_bytes().to_vec())],
        )?;
        let object_payload = rewrite_catalog_fields(
            &object_key,
            &object_current,
            2,
            &[
                (3, WireType::Text, name.as_bytes().to_vec()),
                (5, WireType::U64, object_version.to_le_bytes().to_vec()),
            ],
        )?;
        drop(catalog);
        drop(read);
        audit_event.target_ids = vec![table_id, column_id];
        audit_event.target_ids.sort_unstable();
        commit_authorized_column_rename_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &[
                CatalogWrite {
                    key: table_key,
                    expected_value: Some(table_current),
                    payload: Some(table_payload),
                },
                CatalogWrite {
                    key: column_key,
                    expected_value: Some(column_current),
                    payload: Some(column_payload),
                },
                CatalogWrite {
                    key: object_key,
                    expected_value: Some(object_current),
                    payload: Some(object_payload),
                },
            ],
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => TableLifecycleError::Unauthorized,
            error => TableLifecycleError::Commit(error),
        })
    }

    /// Widen a VARCHAR column without rewriting values or index key bytes.
    #[allow(clippy::too_many_arguments)]
    pub fn widen_varchar_column(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        table_id: [u8; 16],
        column_id: [u8; 16],
        max_chars: u32,
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, TableLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || table_id == [0; 16]
            || column_id == [0; 16]
            || max_chars == 0
            || retention_deadline_us <= 0
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(TableLifecycleError::InvalidAudit);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| TableLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| TableLifecycleError::Storage)?;
        let table_key = id_catalog_key(5, table_id);
        let column_key = id_catalog_key(6, column_id);
        let table_bytes = catalog
            .get(table_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        let column_bytes = catalog
            .get(column_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        if read_catalog_id(&column_key, &column_bytes, 6, 1)? != table_id {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let type_id = read_catalog_id(&column_key, &column_bytes, 6, 4)?;
        let old_parameters: [u8; 8] = read_catalog_field(&column_key, &column_bytes, 6, 5)?
            .as_slice()
            .try_into()
            .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let type_key = id_catalog_key(4, type_id);
        let type_bytes = catalog
            .get(type_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let type_record = decode_registered_envelope(&type_bytes)
            .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let old_type = decode_builtin_scalar_type(&type_record, &old_parameters)
            .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let old_max_chars = match old_type {
            ScalarType::Varchar {
                max_chars: current_max_chars,
            } => current_max_chars,
            _ => return Err(TableLifecycleError::InvalidRequest),
        };
        if max_chars <= old_max_chars {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let new_parameters = scalar_type_parameters(ScalarType::Varchar { max_chars });
        let next_write_epoch = read_catalog_u64(&table_key, &table_bytes, 5, 3)?
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let writes = vec![
            CatalogWrite {
                key: table_key.clone(),
                expected_value: Some(table_bytes.clone()),
                payload: Some(rewrite_catalog_u64_field(
                    &table_key,
                    &table_bytes,
                    5,
                    3,
                    next_write_epoch,
                )?),
            },
            CatalogWrite {
                key: column_key.clone(),
                expected_value: Some(column_bytes.clone()),
                payload: Some(rewrite_catalog_fields(
                    &column_key,
                    &column_bytes,
                    6,
                    &[(5, WireType::Bytes, new_parameters.to_vec())],
                )?),
            },
        ];
        drop(catalog);
        drop(read);
        audit_event.target_ids = vec![table_id, column_id];
        audit_event.target_ids.sort_unstable();
        self.commit_authorized_constraint_ddl(
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &writes,
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => TableLifecycleError::Unauthorized,
            error => TableLifecycleError::Commit(error),
        })
    }

    /// Change an existing column's nullability through authorized constraint DDL.
    #[allow(clippy::too_many_arguments)]
    pub fn set_column_nullable(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        table_id: [u8; 16],
        column_id: [u8; 16],
        nullable: bool,
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, TableLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || table_id == [0; 16]
            || column_id == [0; 16]
            || retention_deadline_us <= 0
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(TableLifecycleError::InvalidAudit);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| TableLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| TableLifecycleError::Storage)?;
        let table_key = id_catalog_key(5, table_id);
        let column_key = id_catalog_key(6, column_id);
        let table_current = catalog
            .get(table_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        let column_current = catalog
            .get(column_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        if read_catalog_id(&column_key, &column_current, 6, 1)? != table_id {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let current_nullable = read_catalog_field(&column_key, &column_current, 6, 6)?;
        let current_nullable = match current_nullable.as_slice() {
            [0] => false,
            [1] => true,
            _ => return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog)),
        };
        if current_nullable == nullable {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let constraint_prefix = 8u16.to_be_bytes();
        let mut existing_not_null = Vec::new();
        let mut scanned = 0usize;
        for entry in catalog
            .range(constraint_prefix.as_slice()..)
            .map_err(|_| TableLifecycleError::Storage)?
        {
            let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
            let key = key.value().to_vec();
            if !key.starts_with(&constraint_prefix) {
                break;
            }
            scanned += 1;
            if scanned > MAX_CATALOG_FAMILY_SCAN {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            let bytes = value.value().to_vec();
            if read_catalog_u16(&key, &bytes, 8, 2)? != 1
                || read_catalog_id(&key, &bytes, 8, 1)? != table_id
            {
                continue;
            }
            let record = read_catalog_record(&key, &bytes, 8)?;
            let field = record
                .fields
                .iter()
                .find(|field| field.tag == 3 && field.wire_type == WireType::Ids)
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let payload = field.payload.as_slice();
            if payload.len() < 4 {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            let count = u32::from_le_bytes(payload[..4].try_into().unwrap()) as usize;
            if count > crate::tuple::MAX_COLUMNS || payload.len() != 4 + count * 16 {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            if payload[4..]
                .chunks_exact(16)
                .any(|source| source == column_id)
            {
                existing_not_null.push((key, bytes));
            }
        }
        if existing_not_null.len() > 1
            || (nullable && current_nullable && !existing_not_null.is_empty())
        {
            return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
        }
        let mut writes = Vec::new();
        let next_epoch = read_catalog_u64(&table_key, &table_current, 5, 3)?
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        writes.push(CatalogWrite {
            key: table_key.clone(),
            expected_value: Some(table_current.clone()),
            payload: Some(rewrite_catalog_u64_field(
                &table_key,
                &table_current,
                5,
                3,
                next_epoch,
            )?),
        });
        writes.push(CatalogWrite {
            key: column_key.clone(),
            expected_value: Some(column_current.clone()),
            payload: Some(rewrite_catalog_fields(
                &column_key,
                &column_current,
                6,
                &[(6, WireType::Bool, vec![u8::from(nullable)])],
            )?),
        });
        if nullable {
            if let Some((constraint_key, constraint_bytes)) = existing_not_null.first() {
                let constraint_id: [u8; 16] = constraint_key[3..19]
                    .try_into()
                    .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                let object_key = id_catalog_key(2, constraint_id);
                let object_bytes = catalog
                    .get(object_key.as_slice())
                    .map_err(|_| TableLifecycleError::Storage)?
                    .map(|value| value.value().to_vec())
                    .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                writes.push(CatalogWrite {
                    key: constraint_key.clone(),
                    expected_value: Some(constraint_bytes.clone()),
                    payload: None,
                });
                writes.push(CatalogWrite {
                    key: object_key,
                    expected_value: Some(object_bytes),
                    payload: None,
                });
            }
        } else {
            if !existing_not_null.is_empty() {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            let constraint_id = self.generate_table_object_id()?;
            let constraint_key = id_catalog_key(8, constraint_id);
            let mut source_columns = 1u32.to_le_bytes().to_vec();
            source_columns.extend_from_slice(&column_id);
            let constraint_payload = encode_envelope(
                &family_schema(8).ok_or(TableLifecycleError::Storage)?,
                &constraint_key,
                &[
                    CatalogField {
                        tag: 1,
                        wire_type: WireType::Id,
                        payload: table_id.to_vec(),
                    },
                    CatalogField {
                        tag: 2,
                        wire_type: WireType::U16,
                        payload: 1u16.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 3,
                        wire_type: WireType::Ids,
                        payload: source_columns,
                    },
                    CatalogField {
                        tag: 6,
                        wire_type: WireType::Ids,
                        payload: 0u32.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 7,
                        wire_type: WireType::U16,
                        payload: 0u16.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 8,
                        wire_type: WireType::U16,
                        payload: 0u16.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 9,
                        wire_type: WireType::U16,
                        payload: 0u16.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 10,
                        wire_type: WireType::U16,
                        payload: 1u16.to_le_bytes().to_vec(),
                    },
                ],
            )
            .map_err(|_| TableLifecycleError::InvalidRequest)?;
            let object_key = id_catalog_key(2, constraint_id);
            let name = format!(
                "not_null_{}",
                constraint_id
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            );
            let object_payload = encode_envelope(
                &family_schema(2).ok_or(TableLifecycleError::Storage)?,
                &object_key,
                &[
                    CatalogField {
                        tag: 1,
                        wire_type: WireType::U16,
                        payload: 6u16.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 3,
                        wire_type: WireType::Text,
                        payload: name.into_bytes(),
                    },
                    CatalogField {
                        tag: 4,
                        wire_type: WireType::Id,
                        payload: authenticated_actor_id.to_vec(),
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
            .map_err(|_| TableLifecycleError::InvalidRequest)?;
            writes.push(CatalogWrite {
                key: constraint_key,
                expected_value: None,
                payload: Some(constraint_payload),
            });
            writes.push(CatalogWrite {
                key: object_key,
                expected_value: None,
                payload: Some(object_payload),
            });
        }
        drop(catalog);
        drop(read);
        audit_event.target_ids = vec![table_id, column_id];
        audit_event.target_ids.sort_unstable();
        self.commit_authorized_constraint_ddl(
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &writes,
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => TableLifecycleError::Unauthorized,
            error => TableLifecycleError::Commit(error),
        })
    }

    /// Set or drop a column's current insert default.
    pub fn alter_column_default(
        &self,
        request: AlterColumnDefaultRequest,
    ) -> Result<u64, TableLifecycleError> {
        let AlterColumnDefaultRequest {
            invocation_id,
            authenticated_actor_id,
            table_id,
            column_id,
            default,
            retention_deadline_us,
            mut audit_event,
        } = request;
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || table_id == [0; 16]
            || column_id == [0; 16]
            || retention_deadline_us <= 0
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(TableLifecycleError::InvalidAudit);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| TableLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| TableLifecycleError::Storage)?;
        let table_key = id_catalog_key(5, table_id);
        let column_key = id_catalog_key(6, column_id);
        let table_current = catalog
            .get(table_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        let column_current = catalog
            .get(column_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        if read_catalog_id(&column_key, &column_current, 6, 1)? != table_id
            || read_catalog_u16(&column_key, &column_current, 6, 8)? != 0
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let type_id = read_catalog_id(&column_key, &column_current, 6, 4)?;
        let type_parameters: [u8; 8] = read_catalog_field(&column_key, &column_current, 6, 5)?
            .as_slice()
            .try_into()
            .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let type_key = id_catalog_key(4, type_id);
        let type_bytes = catalog
            .get(type_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let type_record = read_catalog_record(&type_key, &type_bytes, 4)?;
        let scalar_type = decode_builtin_scalar_type(&type_record, &type_parameters)
            .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        if let Some(default) = &default {
            let expression = match default {
                TableColumnDefault::Literal(value) => {
                    crate::authorization::Expr::Literal(value.clone())
                }
                TableColumnDefault::CurrentPrincipalId => {
                    crate::authorization::Expr::CurrentPrincipalId
                }
            };
            let default_mask = read_catalog_u32(&type_key, &type_bytes, 4, 5)?;
            let permitted = match &expression {
                crate::authorization::Expr::Literal(_) => default_mask & 1 != 0,
                crate::authorization::Expr::CurrentPrincipalId => {
                    default_mask & 4 != 0 && scalar_type == ScalarType::Uuid
                }
                _ => false,
            };
            if !permitted {
                return Err(TableLifecycleError::InvalidRequest);
            }
            if let crate::authorization::Expr::Literal(value) = &expression
                && crate::tuple::encode_tuple(
                    &[ColumnSchema {
                        scalar_type,
                        nullable: true,
                    }],
                    1,
                    std::slice::from_ref(value),
                )
                .is_err()
            {
                return Err(TableLifecycleError::InvalidRequest);
            }
        }
        let column_record = read_catalog_record(&column_key, &column_current, 6)?;
        let old_default_id = column_record
            .fields
            .iter()
            .find(|field| field.tag == 7)
            .map(|field| {
                field
                    .payload
                    .as_slice()
                    .try_into()
                    .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))
            })
            .transpose()?;
        let ordinal = read_catalog_u32(&column_key, &column_current, 6, 2)? as usize;
        let expression = default.as_ref().map(|default| match default {
            TableColumnDefault::Literal(value) => {
                crate::authorization::Expr::Literal(value.clone())
            }
            TableColumnDefault::CurrentPrincipalId => {
                crate::authorization::Expr::CurrentPrincipalId
            }
        });
        if let (Some(old_id), Some(next_expression)) = (old_default_id, expression.as_ref()) {
            let old_key = id_catalog_key(7, old_id);
            let old_bytes = catalog
                .get(old_key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let old = crate::expression::decode_catalog_expression_record(&read_catalog_record(
                &old_key, &old_bytes, 7,
            )?)
            .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            if old.expression == *next_expression {
                return Err(TableLifecycleError::InvalidRequest);
            }
        } else if old_default_id.is_none() && default.is_none() {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let next_default_id = if expression.is_some() {
            Some(self.generate_table_object_id()?)
        } else {
            None
        };
        let mut writes = Vec::new();
        let next_epoch = read_catalog_u64(&table_key, &table_current, 5, 3)?
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        writes.push(CatalogWrite {
            key: table_key.clone(),
            expected_value: Some(table_current.clone()),
            payload: Some(rewrite_catalog_u64_field(
                &table_key,
                &table_current,
                5,
                3,
                next_epoch,
            )?),
        });
        writes.push(CatalogWrite {
            key: column_key.clone(),
            expected_value: Some(column_current.clone()),
            payload: Some(rewrite_optional_catalog_id_field(
                &column_key,
                &column_current,
                6,
                7,
                next_default_id,
            )?),
        });
        if let Some(old_id) = old_default_id {
            let expression_key = id_catalog_key(7, old_id);
            let expression_bytes = catalog
                .get(expression_key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let object_key = id_catalog_key(2, old_id);
            let object_bytes = catalog
                .get(object_key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let dependency_key = dependency_catalog_key(column_id, old_id, 2);
            let dependency_bytes = catalog
                .get(dependency_key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            writes.extend([
                CatalogWrite {
                    key: expression_key,
                    expected_value: Some(expression_bytes),
                    payload: None,
                },
                CatalogWrite {
                    key: object_key,
                    expected_value: Some(object_bytes),
                    payload: None,
                },
                CatalogWrite {
                    key: dependency_key,
                    expected_value: Some(dependency_bytes),
                    payload: None,
                },
            ]);
        }
        if let (Some(expression_id), Some(expression)) = (next_default_id, expression) {
            writes.push(new_default_expression_write(
                expression_id,
                type_id,
                type_parameters,
                expression,
            )?);
            writes.push(new_default_expression_object_write(
                expression_id,
                ordinal,
                authenticated_actor_id,
            )?);
            writes.push(new_dependency_write(column_id, expression_id, 2)?);
        }
        drop(catalog);
        drop(read);
        audit_event.target_ids = vec![table_id, column_id];
        audit_event.target_ids.sort_unstable();
        self.commit_authorized_constraint_ddl(
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &writes,
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => TableLifecycleError::Unauthorized,
            error => TableLifecycleError::Commit(error),
        })
    }
}
