use super::catalog_helpers::*;
use super::*;
use crate::audit::{AuditAction, AuditEvent, AuditResult};
use crate::catalog::{CatalogField, WireType, encode_envelope, family_schema};
use crate::commit::{
    CatalogWrite, CommitError, commit_authorized_index_ddl_with_fences,
    commit_catalog_and_audit_with_fences,
};
use crate::schema::CATALOG;
use redb::ReadableDatabase;
use std::collections::HashSet;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateIndexRequest {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub table_id: [u8; 16],
    pub name: String,
    pub column_ids: Vec<[u8; 16]>,
    pub unique: bool,
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RenameIndexRequest {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub index_id: [u8; 16],
    pub name: String,
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterColumnDefaultRequest {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub table_id: [u8; 16],
    pub column_id: [u8; 16],
    pub default: Option<TableColumnDefault>,
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddTableColumnRequest {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub table_id: [u8; 16],
    pub column: TableColumnDefinition,
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedIndex {
    pub index_id: [u8; 16],
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DropIndexRequest {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub index_id: [u8; 16],
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DroppedIndex {
    pub index_id: [u8; 16],
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebuildIndexRequest {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub index_id: [u8; 16],
    pub column_ids: Vec<[u8; 16]>,
    pub unique: bool,
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebuiltIndex {
    pub index_id: [u8; 16],
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RenamedIndex {
    pub index_id: [u8; 16],
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IndexLifecycleError {
    InvalidRequest,
    InvalidAudit,
    RandomnessUnavailable,
    ObjectIdGenerationFailed,
    Storage,
    Commit(CommitError),
}

impl std::fmt::Display for IndexLifecycleError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "index lifecycle operation failed: {self:?}")
    }
}

impl std::error::Error for IndexLifecycleError {}

impl From<TableLifecycleError> for IndexLifecycleError {
    fn from(error: TableLifecycleError) -> Self {
        match error {
            TableLifecycleError::InvalidRequest => Self::InvalidRequest,
            TableLifecycleError::InvalidAudit => Self::InvalidAudit,
            TableLifecycleError::RandomnessUnavailable => Self::RandomnessUnavailable,
            TableLifecycleError::ObjectIdGenerationFailed => Self::ObjectIdGenerationFailed,
            TableLifecycleError::Unauthorized => Self::Commit(CommitError::DdlAuthorizationDenied),
            TableLifecycleError::Storage => Self::Storage,
            TableLifecycleError::Commit(error) => Self::Commit(error),
        }
    }
}

impl<'db> StorageManager<'db> {
    /// Trusted coordinated catalog publication with no actor authorization.
    /// Use only from an internal caller that has already applied the relevant
    /// action-specific administrative checks.
    pub fn commit_catalog_and_audit(
        &self,
        invocation_id: [u8; 16],
        principal_id: [u8; 16],
        retention_deadline_us: i64,
        catalog_writes: &[CatalogWrite],
        audit_events: &[AuditEvent],
    ) -> Result<u64, CommitError> {
        commit_catalog_and_audit_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        )
    }

    /// Authorize ordinary index DDL for a principal authenticated by the
    /// trusted administrative channel, then commit it under the shared DDL
    /// fences. Supports family-9 index definitions, matching family-2 index
    /// objects, and validated logical index-name changes. Other catalog DDL
    /// remains on the trusted internal entry point until its action-specific
    /// authority rules are implemented.
    pub fn commit_authorized_index_ddl(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        retention_deadline_us: i64,
        catalog_writes: &[CatalogWrite],
        audit_events: &[AuditEvent],
    ) -> Result<u64, CommitError> {
        commit_authorized_index_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        )
    }

    /// Rename an ordinary index without rebuilding its physical memberships.
    /// The index object and audit event publish through the same authorized
    /// index DDL path used by create, drop, and rebuild.
    pub fn rename_index(
        &self,
        request: RenameIndexRequest,
    ) -> Result<RenamedIndex, IndexLifecycleError> {
        let RenameIndexRequest {
            invocation_id,
            authenticated_actor_id,
            index_id,
            name,
            retention_deadline_us,
            mut audit_event,
        } = request;
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || index_id == [0; 16]
            || !is_normalized_principal_name(&name)
            || retention_deadline_us <= 0
        {
            return Err(IndexLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(IndexLifecycleError::InvalidAudit);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| IndexLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| IndexLifecycleError::Storage)?;
        let index_key = id_catalog_key(9, index_id);
        let index_bytes = catalog
            .get(index_key.as_slice())
            .map_err(|_| IndexLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(IndexLifecycleError::Commit(CommitError::InvalidCatalog))?;
        read_catalog_record(&index_key, &index_bytes, 9).map_err(IndexLifecycleError::from)?;
        let table_id =
            read_catalog_id(&index_key, &index_bytes, 9, 1).map_err(IndexLifecycleError::from)?;
        let table_key = id_catalog_key(5, table_id);
        let table_bytes = catalog
            .get(table_key.as_slice())
            .map_err(|_| IndexLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(IndexLifecycleError::Commit(CommitError::InvalidCatalog))?;
        read_catalog_record(&table_key, &table_bytes, 5).map_err(IndexLifecycleError::from)?;
        let schema_id =
            read_catalog_id(&table_key, &table_bytes, 5, 1).map_err(IndexLifecycleError::from)?;
        let object_key = id_catalog_key(2, index_id);
        let object_bytes = catalog
            .get(object_key.as_slice())
            .map_err(|_| IndexLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(IndexLifecycleError::Commit(CommitError::InvalidCatalog))?;
        read_catalog_record(&object_key, &object_bytes, 2).map_err(IndexLifecycleError::from)?;
        if read_catalog_u16(&object_key, &object_bytes, 2, 1).map_err(IndexLifecycleError::from)?
            != 7
            || read_catalog_id(&object_key, &object_bytes, 2, 2)
                .map_err(IndexLifecycleError::from)?
                != schema_id
            || read_catalog_field(&object_key, &object_bytes, 2, 3)
                .map_err(IndexLifecycleError::from)?
                == name.as_bytes()
        {
            return Err(IndexLifecycleError::InvalidRequest);
        }
        let next_object_version = read_catalog_u64(&object_key, &object_bytes, 2, 5)
            .map_err(IndexLifecycleError::from)?
            .checked_add(1)
            .ok_or(IndexLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let object_payload = rewrite_catalog_fields(
            &object_key,
            &object_bytes,
            2,
            &[
                (3, WireType::Text, name.into_bytes()),
                (5, WireType::U64, next_object_version.to_le_bytes().to_vec()),
            ],
        )
        .map_err(IndexLifecycleError::from)?;
        drop(catalog);
        drop(read);
        audit_event.target_ids = vec![index_id];
        let commit_sequence = self
            .commit_authorized_index_ddl(
                invocation_id,
                authenticated_actor_id,
                retention_deadline_us,
                &[CatalogWrite {
                    key: object_key,
                    expected_value: Some(object_bytes),
                    payload: Some(object_payload),
                }],
                &[audit_event],
            )
            .map_err(IndexLifecycleError::Commit)?;
        Ok(RenamedIndex {
            index_id,
            commit_sequence,
        })
    }

    /// Create and backfill one ordinary index through the authorized index DDL
    /// commit path. The index ID is generated by storage; the caller supplies
    /// only the target table, ordered columns, name, and uniqueness profile.
    pub fn create_index(
        &self,
        request: CreateIndexRequest,
    ) -> Result<CreatedIndex, IndexLifecycleError> {
        let CreateIndexRequest {
            invocation_id,
            authenticated_actor_id,
            table_id,
            name,
            column_ids,
            unique,
            retention_deadline_us,
            audit_event,
        } = request;
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || table_id == [0; 16]
            || !is_normalized_principal_name(&name)
            || column_ids.is_empty()
            || column_ids.len() > 256
            || column_ids.contains(&[0; 16])
            || column_ids.iter().copied().collect::<HashSet<_>>().len() != column_ids.len()
            || retention_deadline_us <= 0
        {
            return Err(IndexLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(IndexLifecycleError::InvalidAudit);
        }

        let index_id = self
            .generate_table_object_id()
            .map_err(IndexLifecycleError::from)?;
        let read = self
            .database
            .begin_read()
            .map_err(|_| IndexLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| IndexLifecycleError::Storage)?;
        let table_key = id_catalog_key(5, table_id);
        let table_bytes = catalog
            .get(table_key.as_slice())
            .map_err(|_| IndexLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(IndexLifecycleError::Commit(CommitError::InvalidCatalog))?;
        read_catalog_record(&table_key, &table_bytes, 5).map_err(IndexLifecycleError::from)?;
        let schema_id =
            read_catalog_id(&table_key, &table_bytes, 5, 1).map_err(IndexLifecycleError::from)?;
        let schema_version =
            read_catalog_u64(&table_key, &table_bytes, 5, 2).map_err(IndexLifecycleError::from)?;
        drop(catalog);
        drop(read);

        let encode_ids = |ids: &[[u8; 16]]| {
            let mut payload = (ids.len() as u32).to_le_bytes().to_vec();
            payload.extend(ids.iter().flatten());
            payload
        };
        let index_key = id_catalog_key(9, index_id);
        let index_fields = [
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: table_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::Ids,
                payload: encode_ids(&column_ids),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::Bool,
                payload: vec![u8::from(unique)],
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 7,
                wire_type: WireType::Id,
                payload: index_id.to_vec(),
            },
            CatalogField {
                tag: 8,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 9,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 10,
                wire_type: WireType::U64,
                payload: schema_version.to_le_bytes().to_vec(),
            },
        ];
        let index_payload = encode_envelope(
            &family_schema(9).ok_or(IndexLifecycleError::Storage)?,
            &index_key,
            &index_fields,
        )
        .map_err(|_| IndexLifecycleError::InvalidRequest)?;
        let object_key = id_catalog_key(2, index_id);
        let object_payload = encode_envelope(
            &family_schema(2).ok_or(IndexLifecycleError::Storage)?,
            &object_key,
            &[
                CatalogField {
                    tag: 1,
                    wire_type: WireType::U16,
                    payload: 7u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 2,
                    wire_type: WireType::Id,
                    payload: schema_id.to_vec(),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::Text,
                    payload: name.into_bytes(),
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
        .map_err(|_| IndexLifecycleError::InvalidRequest)?;
        let ownership_edge =
            new_dependency_write(index_id, schema_id, 1).map_err(IndexLifecycleError::from)?;
        let writes = [
            CatalogWrite {
                key: index_key,
                expected_value: None,
                payload: Some(index_payload),
            },
            CatalogWrite {
                key: object_key,
                expected_value: None,
                payload: Some(object_payload),
            },
            ownership_edge,
        ];
        let commit_sequence = self
            .commit_authorized_index_ddl(
                invocation_id,
                authenticated_actor_id,
                retention_deadline_us,
                &writes,
                &[audit_event],
            )
            .map_err(IndexLifecycleError::Commit)?;
        Ok(CreatedIndex {
            index_id,
            commit_sequence,
        })
    }

    /// Remove an ordinary index and its schema ownership edge through the
    /// authorized index DDL commit path. Physical memberships are removed by
    /// that commit atomically with the catalog records.
    pub fn drop_index(
        &self,
        request: DropIndexRequest,
    ) -> Result<DroppedIndex, IndexLifecycleError> {
        let DropIndexRequest {
            invocation_id,
            authenticated_actor_id,
            index_id,
            retention_deadline_us,
            audit_event,
        } = request;
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || index_id == [0; 16]
            || retention_deadline_us <= 0
        {
            return Err(IndexLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(IndexLifecycleError::InvalidAudit);
        }

        let read = self
            .database
            .begin_read()
            .map_err(|_| IndexLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| IndexLifecycleError::Storage)?;
        let index_key = id_catalog_key(9, index_id);
        let index_bytes = catalog
            .get(index_key.as_slice())
            .map_err(|_| IndexLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(IndexLifecycleError::Commit(CommitError::InvalidCatalog))?;
        read_catalog_record(&index_key, &index_bytes, 9).map_err(IndexLifecycleError::from)?;
        let object_key = id_catalog_key(2, index_id);
        let object_bytes = catalog
            .get(object_key.as_slice())
            .map_err(|_| IndexLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(IndexLifecycleError::Commit(CommitError::InvalidCatalog))?;
        read_catalog_record(&object_key, &object_bytes, 2).map_err(IndexLifecycleError::from)?;
        if read_catalog_u16(&object_key, &object_bytes, 2, 1).map_err(IndexLifecycleError::from)?
            != 7
        {
            return Err(IndexLifecycleError::Commit(CommitError::InvalidCatalog));
        }
        let schema_id =
            read_catalog_id(&object_key, &object_bytes, 2, 2).map_err(IndexLifecycleError::from)?;
        let edge_key = dependency_catalog_key(index_id, schema_id, 1);
        let edge_bytes = catalog
            .get(edge_key.as_slice())
            .map_err(|_| IndexLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(IndexLifecycleError::Commit(CommitError::InvalidCatalog))?;
        drop(catalog);
        drop(read);

        let reservation_key = retired_object_id_catalog_key(index_id);
        let reservation_payload = encode_envelope(
            &family_schema(17).ok_or(IndexLifecycleError::Commit(CommitError::InvalidCatalog))?,
            &reservation_key,
            &[],
        )
        .map_err(|_| IndexLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let writes = [
            CatalogWrite {
                key: index_key,
                expected_value: Some(index_bytes),
                payload: None,
            },
            CatalogWrite {
                key: object_key,
                expected_value: Some(object_bytes),
                payload: None,
            },
            CatalogWrite {
                key: edge_key,
                expected_value: Some(edge_bytes),
                payload: None,
            },
            CatalogWrite {
                key: reservation_key,
                expected_value: None,
                payload: Some(reservation_payload),
            },
        ];
        let commit_sequence = self
            .commit_authorized_index_ddl(
                invocation_id,
                authenticated_actor_id,
                retention_deadline_us,
                &writes,
                &[audit_event],
            )
            .map_err(IndexLifecycleError::Commit)?;
        Ok(DroppedIndex {
            index_id,
            commit_sequence,
        })
    }

    /// Rebuild an index using a new ordered column set and uniqueness profile.
    /// The authorized index DDL commit validates the old memberships, builds
    /// the replacement from current rows, and publishes it under the table
    /// DDL fence.
    pub fn rebuild_index(
        &self,
        request: RebuildIndexRequest,
    ) -> Result<RebuiltIndex, IndexLifecycleError> {
        let RebuildIndexRequest {
            invocation_id,
            authenticated_actor_id,
            index_id,
            column_ids,
            unique,
            retention_deadline_us,
            audit_event,
        } = request;
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || index_id == [0; 16]
            || column_ids.is_empty()
            || column_ids.len() > 256
            || column_ids.contains(&[0; 16])
            || column_ids.iter().copied().collect::<HashSet<_>>().len() != column_ids.len()
            || retention_deadline_us <= 0
        {
            return Err(IndexLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(IndexLifecycleError::InvalidAudit);
        }

        let read = self
            .database
            .begin_read()
            .map_err(|_| IndexLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| IndexLifecycleError::Storage)?;
        let index_key = id_catalog_key(9, index_id);
        let index_bytes = catalog
            .get(index_key.as_slice())
            .map_err(|_| IndexLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(IndexLifecycleError::Commit(CommitError::InvalidCatalog))?;
        read_catalog_record(&index_key, &index_bytes, 9).map_err(IndexLifecycleError::from)?;
        let table_id =
            read_catalog_id(&index_key, &index_bytes, 9, 1).map_err(IndexLifecycleError::from)?;
        let table_key = id_catalog_key(5, table_id);
        let table_bytes = catalog
            .get(table_key.as_slice())
            .map_err(|_| IndexLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(IndexLifecycleError::Commit(CommitError::InvalidCatalog))?;
        read_catalog_record(&table_key, &table_bytes, 5).map_err(IndexLifecycleError::from)?;
        let schema_version =
            read_catalog_u64(&table_key, &table_bytes, 5, 2).map_err(IndexLifecycleError::from)?;
        drop(catalog);
        drop(read);

        let mut encoded_ids = (column_ids.len() as u32).to_le_bytes().to_vec();
        encoded_ids.extend(column_ids.iter().flatten());
        let updated = rewrite_catalog_fields(
            &index_key,
            &index_bytes,
            9,
            &[
                (2, WireType::Ids, encoded_ids),
                (4, WireType::Bool, vec![u8::from(unique)]),
                (10, WireType::U64, schema_version.to_le_bytes().to_vec()),
            ],
        )
        .map_err(IndexLifecycleError::from)?;
        let writes = [CatalogWrite {
            key: index_key,
            expected_value: Some(index_bytes),
            payload: Some(updated),
        }];
        let commit_sequence = self
            .commit_authorized_index_ddl(
                invocation_id,
                authenticated_actor_id,
                retention_deadline_us,
                &writes,
                &[audit_event],
            )
            .map_err(IndexLifecycleError::Commit)?;
        Ok(RebuiltIndex {
            index_id,
            commit_sequence,
        })
    }
}
