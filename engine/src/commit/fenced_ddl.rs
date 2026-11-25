use super::constraint_validation::build_constraint_validation_rows;
use super::execution::*;
use super::guards_constraints::*;
use super::guards_foreign_keys::*;
use super::guards_identity::*;
use super::guards_index_grant_policy::*;
use super::guards_schema_table::*;
use super::guards_table_column::*;
use super::index_ddl::*;
use super::*;
use crate::audit::AuditEvent;
use crate::catalog::{WireType, decode_registered_envelope, encode_envelope, family_schema};
use crate::ddl_fence::DdlFenceCoordinator;
use crate::schema::CATALOG;
use redb::{Database, ReadOnlyTable, ReadableDatabase};
use std::collections::{HashMap, HashSet};

/// Atomically publish catalog changes and required audit events. Membership
/// graphs, dependency endpoints, and changed constraint references are checked
/// at commit. This helper neither authorizes the actor nor acquires DDL fences;
/// it is trusted-only and must not be exposed directly to control clients.
pub fn commit_catalog_and_audit(
    database: &Database,
    invocation_id: [u8; 16],
    principal_id: [u8; 16],
    retention_deadline_us: i64,
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<u64, CommitError> {
    commit_storage_changes(
        database,
        CommitRequest {
            invocation_id,
            principal_id,
            retention_deadline_us,
            rows: &[],
            indexes: &[],
            foreign_keys: &[],
            catalog_guards: &[],
            catalog: catalog_writes,
            audit: audit_events,
        },
    )
}

pub(super) fn commit_catalog_and_audit_with_index_ddl(
    database: &Database,
    request: CatalogCommitRequest<'_>,
    catalog_guards: &[CatalogGuard],
    backfill: &IndexDdlPlan,
    require_usable_admin_credential: bool,
    check_table_name_uniqueness: bool,
    check_column_name_uniqueness: bool,
) -> Result<u64, CommitError> {
    let CatalogCommitRequest {
        invocation_id,
        principal_id,
        retention_deadline_us,
        catalog_writes,
        audit_events,
    } = request;
    if backfill.index_tables.is_empty()
        && backfill.expected_rows.is_empty()
        && backfill.writes.is_empty()
        && backfill.row_changes.is_empty()
        && catalog_guards.is_empty()
        && !require_usable_admin_credential
        && !check_table_name_uniqueness
        && !check_column_name_uniqueness
        && backfill.schema_drop_id.is_none()
        && backfill.table_drop_id.is_none()
    {
        return commit_catalog_and_audit(
            database,
            invocation_id,
            principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        );
    }
    let catalog_writes = add_index_ddl_table_epoch_writes(database, catalog_writes, backfill)?;
    commit_storage_changes_with_index_ddl(
        database,
        CommitRequest {
            invocation_id,
            principal_id,
            retention_deadline_us,
            rows: &backfill.row_changes,
            indexes: &backfill.writes,
            foreign_keys: &[],
            catalog_guards,
            catalog: &catalog_writes,
            audit: audit_events,
        },
        backfill,
        require_usable_admin_credential,
        check_table_name_uniqueness,
        check_column_name_uniqueness,
    )
}

pub(super) fn add_index_ddl_table_epoch_writes(
    database: &Database,
    catalog_writes: &[CatalogWrite],
    backfill: &IndexDdlPlan,
) -> Result<Vec<CatalogWrite>, CommitError> {
    let table_ids = backfill
        .index_tables
        .values()
        .copied()
        .collect::<HashSet<_>>();
    if table_ids.is_empty() {
        return Ok(catalog_writes.to_vec());
    }
    let read = database.begin_read().map_err(storage_error)?;
    let catalog = read.open_table(CATALOG).map_err(storage_error)?;
    let mut result = Vec::with_capacity(catalog_writes.len() + table_ids.len());
    result.extend_from_slice(catalog_writes);
    for table_id in table_ids {
        let table_key = id_catalog_key(5, table_id);
        if catalog_writes.iter().any(|write| write.key == table_key) {
            continue;
        }
        let current = catalog
            .get(table_key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        let next_epoch = table_write_epoch(&table_key, &current)?
            .checked_add(1)
            .ok_or(CommitError::InvalidCatalog)?;
        let mut record = decode_catalog_record(&table_key, &current)?;
        let epoch_field = record
            .fields
            .iter_mut()
            .find(|field| field.tag == 3)
            .ok_or(CommitError::InvalidCatalog)?;
        if epoch_field.wire_type != WireType::U64 || epoch_field.payload.len() != 8 {
            return Err(CommitError::InvalidCatalog);
        }
        epoch_field.payload = next_epoch.to_le_bytes().to_vec();
        let payload = encode_envelope(
            &family_schema(5).ok_or(CommitError::InvalidCatalog)?,
            &table_key,
            &record.fields,
        )
        .map_err(|_| CommitError::InvalidCatalog)?;
        result.push(CatalogWrite {
            key: table_key,
            expected_value: Some(current),
            payload: Some(payload),
        });
    }
    result.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(result)
}

/// Trusted coordinated catalog publication that does not authorize the actor.
/// Use [`commit_authorized_index_ddl_with_fences`] for ordinary index DDL.
/// This path exclusively fences every affected table.
/// The same `DdlFenceCoordinator` must be passed to invocation commits through
/// `InvocationTransaction::begin_with_fences`.
pub fn commit_catalog_and_audit_with_fences(
    database: &Database,
    ddl_fences: &DdlFenceCoordinator,
    invocation_id: [u8; 16],
    principal_id: [u8; 16],
    retention_deadline_us: i64,
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<u64, CommitError> {
    commit_catalog_and_audit_with_fences_inner(
        database,
        ddl_fences,
        CatalogCommitRequest {
            invocation_id,
            principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        },
        CatalogAuthorizationMode::Trusted,
    )
}

/// Authorize and publish ordinary index DDL for a principal that has already
/// been authenticated by the trusted administrative channel. This entry point
/// accepts index-definition and matching index-object records only. Other
/// catalog changes still require their own authorization rules.
pub fn commit_authorized_index_ddl_with_fences(
    database: &Database,
    ddl_fences: &DdlFenceCoordinator,
    invocation_id: [u8; 16],
    authenticated_principal_id: [u8; 16],
    retention_deadline_us: i64,
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<u64, CommitError> {
    commit_catalog_and_audit_with_fences_inner(
        database,
        ddl_fences,
        CatalogCommitRequest {
            invocation_id,
            principal_id: authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        },
        CatalogAuthorizationMode::IndexDdl,
    )
}

/// Authorize and publish user/role creation, rename, login-state changes, and
/// `DROP ... RESTRICT` after checking all references. The actor is authenticated
/// by the trusted administrative channel. Users require `MANAGE_USERS`; roles
/// require `MANAGE_ROLES`.
pub fn commit_authorized_identity_ddl_with_fences(
    database: &Database,
    ddl_fences: &DdlFenceCoordinator,
    invocation_id: [u8; 16],
    authenticated_principal_id: [u8; 16],
    retention_deadline_us: i64,
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<u64, CommitError> {
    commit_catalog_and_audit_with_fences_inner(
        database,
        ddl_fences,
        CatalogCommitRequest {
            invocation_id,
            principal_id: authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        },
        CatalogAuthorizationMode::IdentityDdl,
    )
}

/// Authorize and publish row/cell policy DDL for an actor authenticated by
/// the trusted administrative channel. The actor must own each affected table
/// or hold inherited `MANAGE_POLICIES` on it.
pub fn commit_authorized_policy_ddl_with_fences(
    database: &Database,
    ddl_fences: &DdlFenceCoordinator,
    invocation_id: [u8; 16],
    authenticated_principal_id: [u8; 16],
    retention_deadline_us: i64,
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<u64, CommitError> {
    commit_catalog_and_audit_with_fences_inner(
        database,
        ddl_fences,
        CatalogCommitRequest {
            invocation_id,
            principal_id: authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        },
        CatalogAuthorizationMode::PolicyDdl,
    )
}

/// Authorize and publish foreign-key additions/removals on existing tables.
/// Creation requires source-table `ALTER` plus `REFERENCES` on each target
/// key column; removal requires source-table `ALTER`.
pub fn commit_authorized_foreign_key_ddl_with_fences(
    database: &Database,
    ddl_fences: &DdlFenceCoordinator,
    invocation_id: [u8; 16],
    authenticated_principal_id: [u8; 16],
    retention_deadline_us: i64,
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<u64, CommitError> {
    commit_catalog_and_audit_with_fences_inner(
        database,
        ddl_fences,
        CatalogCommitRequest {
            invocation_id,
            principal_id: authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        },
        CatalogAuthorizationMode::ForeignKeyDdl,
    )
}

/// Authorize add/drop operations for NOT NULL, PRIMARY KEY, UNIQUE, and CHECK
/// constraints on existing tables. Key constraints atomically create or remove
/// their owned unique backing indexes. CHECK and key additions validate stored
/// rows before publication. The actor must own the source table or hold
/// inherited ALTER on its table or schema.
pub fn commit_authorized_constraint_ddl_with_fences(
    database: &Database,
    ddl_fences: &DdlFenceCoordinator,
    invocation_id: [u8; 16],
    authenticated_principal_id: [u8; 16],
    retention_deadline_us: i64,
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<u64, CommitError> {
    commit_catalog_and_audit_with_fences_inner(
        database,
        ddl_fences,
        CatalogCommitRequest {
            invocation_id,
            principal_id: authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        },
        CatalogAuthorizationMode::ConstraintDdl,
    )
}

/// Authorize and publish role-membership changes for a principal already
/// authenticated by the trusted administrative channel. `MANAGE_ROLES` is
/// sufficient except when the reserved database-administrator role is a
/// membership target, which requires an existing database administrator.
pub fn commit_authorized_membership_ddl_with_fences(
    database: &Database,
    ddl_fences: &DdlFenceCoordinator,
    invocation_id: [u8; 16],
    authenticated_principal_id: [u8; 16],
    retention_deadline_us: i64,
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<u64, CommitError> {
    commit_catalog_and_audit_with_fences_inner(
        database,
        ddl_fences,
        CatalogCommitRequest {
            invocation_id,
            principal_id: authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        },
        CatalogAuthorizationMode::MembershipDdl,
    )
}

/// Authorize and publish database-scoped system privilege grant or revoke
/// operations. Only a current database administrator may use this route.
pub fn commit_authorized_system_privilege_ddl_with_fences(
    database: &Database,
    ddl_fences: &DdlFenceCoordinator,
    invocation_id: [u8; 16],
    authenticated_principal_id: [u8; 16],
    retention_deadline_us: i64,
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<u64, CommitError> {
    commit_catalog_and_audit_with_fences_inner(
        database,
        ddl_fences,
        CatalogCommitRequest {
            invocation_id,
            principal_id: authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        },
        CatalogAuthorizationMode::SystemPrivilegeDdl,
    )
}

/// Authorize and publish one schema creation. The actor must hold the
/// database-scoped CREATE_SCHEMA privilege or be a database administrator.
pub fn commit_authorized_schema_create_ddl_with_fences(
    database: &Database,
    ddl_fences: &DdlFenceCoordinator,
    invocation_id: [u8; 16],
    authenticated_principal_id: [u8; 16],
    retention_deadline_us: i64,
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<u64, CommitError> {
    commit_catalog_and_audit_with_fences_inner(
        database,
        ddl_fences,
        CatalogCommitRequest {
            invocation_id,
            principal_id: authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        },
        CatalogAuthorizationMode::SchemaCreateDdl,
    )
}

/// Authorize and publish removal of an empty schema. The owner or a database
/// administrator must authorize the operation; emptiness is rechecked inside
/// the serialized write transaction.
pub fn commit_authorized_schema_drop_ddl_with_fences(
    database: &Database,
    ddl_fences: &DdlFenceCoordinator,
    invocation_id: [u8; 16],
    authenticated_principal_id: [u8; 16],
    retention_deadline_us: i64,
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<u64, CommitError> {
    commit_catalog_and_audit_with_fences_inner(
        database,
        ddl_fences,
        CatalogCommitRequest {
            invocation_id,
            principal_id: authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        },
        CatalogAuthorizationMode::SchemaDropDdl,
    )
}

/// Authorize and publish transfer of a non-system schema to a live principal.
pub fn commit_authorized_schema_owner_ddl_with_fences(
    database: &Database,
    ddl_fences: &DdlFenceCoordinator,
    invocation_id: [u8; 16],
    authenticated_principal_id: [u8; 16],
    retention_deadline_us: i64,
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<u64, CommitError> {
    commit_catalog_and_audit_with_fences_inner(
        database,
        ddl_fences,
        CatalogCommitRequest {
            invocation_id,
            principal_id: authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        },
        CatalogAuthorizationMode::SchemaOwnerTransferDdl,
    )
}

/// Authorize and publish a non-system schema rename while preserving its ID.
pub fn commit_authorized_schema_rename_ddl_with_fences(
    database: &Database,
    ddl_fences: &DdlFenceCoordinator,
    invocation_id: [u8; 16],
    authenticated_principal_id: [u8; 16],
    retention_deadline_us: i64,
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<u64, CommitError> {
    commit_catalog_and_audit_with_fences_inner(
        database,
        ddl_fences,
        CatalogCommitRequest {
            invocation_id,
            principal_id: authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        },
        CatalogAuthorizationMode::SchemaRenameDdl,
    )
}

/// Authorize and atomically remove a table and its owned catalog records.
pub fn commit_authorized_table_drop_ddl_with_fences(
    database: &Database,
    ddl_fences: &DdlFenceCoordinator,
    invocation_id: [u8; 16],
    authenticated_principal_id: [u8; 16],
    retention_deadline_us: i64,
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<u64, CommitError> {
    commit_catalog_and_audit_with_fences_inner(
        database,
        ddl_fences,
        CatalogCommitRequest {
            invocation_id,
            principal_id: authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        },
        CatalogAuthorizationMode::TableDropDdl,
    )
}

/// Authorize a stable-ID table rename and validate the final schema-local name set.
pub fn commit_authorized_table_rename_ddl_with_fences(
    database: &Database,
    ddl_fences: &DdlFenceCoordinator,
    invocation_id: [u8; 16],
    authenticated_principal_id: [u8; 16],
    retention_deadline_us: i64,
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<u64, CommitError> {
    commit_catalog_and_audit_with_fences_inner(
        database,
        ddl_fences,
        CatalogCommitRequest {
            invocation_id,
            principal_id: authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        },
        CatalogAuthorizationMode::TableRenameDdl,
    )
}

/// Authorize a table ownership transfer while preserving its stable ID.
pub fn commit_authorized_table_owner_ddl_with_fences(
    database: &Database,
    ddl_fences: &DdlFenceCoordinator,
    invocation_id: [u8; 16],
    authenticated_principal_id: [u8; 16],
    retention_deadline_us: i64,
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<u64, CommitError> {
    commit_catalog_and_audit_with_fences_inner(
        database,
        ddl_fences,
        CatalogCommitRequest {
            invocation_id,
            principal_id: authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        },
        CatalogAuthorizationMode::TableOwnerTransferDdl,
    )
}

/// Atomically delete a table's rows and index memberships while retaining its
/// definitions. The storage manager must hold the table's exclusive DDL fence
/// across its bounded snapshot scan and this commit.
pub(crate) fn commit_authorized_table_truncate_ddl_with_fences(
    database: &Database,
    request: TableTruncateCommitRequest<'_>,
) -> Result<u64, CommitError> {
    let TableTruncateCommitRequest {
        invocation_id,
        principal_id,
        table_id,
        retention_deadline_us,
        rows,
        indexes,
        catalog_writes,
        audit_events,
    } = request;
    if invocation_id == [0; 16]
        || principal_id == [0; 16]
        || table_id == [0; 16]
        || !catalog_writes
            .iter()
            .any(|write| write.key == id_catalog_key(5, table_id))
        || rows.iter().any(|write| {
            write.key[..16] != table_id
                || write.key[16..] == [0; 16]
                || write.expected_version.is_none()
                || write.payload.is_some()
        })
        || indexes.iter().any(|write| {
            write.row_key[..16] != table_id || write.expected_version.is_none() || write.present
        })
    {
        return Err(CommitError::InvalidWrite);
    }
    let catalog_guards = match table_rename_ddl_authority_guards(
        database,
        principal_id,
        catalog_writes,
        audit_events,
        TableObjectMutationKind::Truncate,
    ) {
        Ok(guards) => guards,
        Err(error) => {
            if !matches!(error, CommitError::Storage(_)) {
                record_aborted_outcome(
                    database,
                    invocation_id,
                    principal_id,
                    retention_deadline_us,
                )?;
            }
            return Err(error);
        }
    };
    commit_storage_changes(
        database,
        CommitRequest {
            invocation_id,
            principal_id,
            retention_deadline_us,
            rows,
            indexes,
            foreign_keys: &[],
            catalog_guards: &catalog_guards,
            catalog: catalog_writes,
            audit: audit_events,
        },
    )
}

/// Authorize a stable-ID column rename and validate final table-local names.
pub fn commit_authorized_column_rename_ddl_with_fences(
    database: &Database,
    ddl_fences: &DdlFenceCoordinator,
    invocation_id: [u8; 16],
    authenticated_principal_id: [u8; 16],
    retention_deadline_us: i64,
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<u64, CommitError> {
    commit_catalog_and_audit_with_fences_inner(
        database,
        ddl_fences,
        CatalogCommitRequest {
            invocation_id,
            principal_id: authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        },
        CatalogAuthorizationMode::ColumnRenameDdl,
    )
}

/// Authorize and atomically remove a column, rewrite rows, and rebuild surviving indexes.
pub fn commit_authorized_column_drop_ddl_with_fences(
    database: &Database,
    ddl_fences: &DdlFenceCoordinator,
    invocation_id: [u8; 16],
    authenticated_principal_id: [u8; 16],
    retention_deadline_us: i64,
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<u64, CommitError> {
    commit_catalog_and_audit_with_fences_inner(
        database,
        ddl_fences,
        CatalogCommitRequest {
            invocation_id,
            principal_id: authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        },
        CatalogAuthorizationMode::ColumnDropDdl,
    )
}

/// Authorize and publish a table with its initial ordered columns. The actor
/// must hold CREATE on the containing schema or be a database administrator.
pub fn commit_authorized_table_create_ddl_with_fences(
    database: &Database,
    ddl_fences: &DdlFenceCoordinator,
    invocation_id: [u8; 16],
    authenticated_principal_id: [u8; 16],
    retention_deadline_us: i64,
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<u64, CommitError> {
    commit_catalog_and_audit_with_fences_inner(
        database,
        ddl_fences,
        CatalogCommitRequest {
            invocation_id,
            principal_id: authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        },
        CatalogAuthorizationMode::TableCreateDdl,
    )
}

/// Authorize and publish object-scoped grant/revoke records. The actor must
/// own each target object, hold `MANAGE_GRANTS` on it, or be a database
/// administrator. System privileges use the separate admin-only route.
pub fn commit_authorized_object_grant_ddl_with_fences(
    database: &Database,
    ddl_fences: &DdlFenceCoordinator,
    invocation_id: [u8; 16],
    authenticated_principal_id: [u8; 16],
    retention_deadline_us: i64,
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<u64, CommitError> {
    commit_catalog_and_audit_with_fences_inner(
        database,
        ddl_fences,
        CatalogCommitRequest {
            invocation_id,
            principal_id: authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        },
        CatalogAuthorizationMode::ObjectGrantDdl,
    )
}

pub(super) fn commit_catalog_and_audit_with_fences_inner(
    database: &Database,
    ddl_fences: &DdlFenceCoordinator,
    request: CatalogCommitRequest<'_>,
    authorization_mode: CatalogAuthorizationMode,
) -> Result<u64, CommitError> {
    let CatalogCommitRequest {
        invocation_id,
        principal_id,
        retention_deadline_us,
        catalog_writes,
        audit_events,
        ..
    } = request;
    let affected_tables = match affected_table_ids_for_catalog_writes(database, catalog_writes) {
        Ok(tables) => tables,
        Err(error) => {
            if !matches!(error, CommitError::Storage(_)) {
                record_aborted_outcome(
                    database,
                    invocation_id,
                    principal_id,
                    retention_deadline_us,
                )?;
            }
            return Err(error);
        }
    };
    let _fence_guard = match ddl_fences.acquire_exclusive(&affected_tables) {
        Ok(guard) => guard,
        Err(_) => {
            record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
            return Err(CommitError::DdlFenceUnavailable);
        }
    };
    // DDL authority validators inspect current catalog relationships to
    // authorize the proposed final shape. First reject a stale read set so a
    // concurrent DDL publication is reported as a conflict rather than as a
    // malformed-catalog error from validating stale payload bytes.
    if matches!(
        authorization_mode,
        CatalogAuthorizationMode::TableRenameDdl | CatalogAuthorizationMode::ConstraintDdl
    ) {
        let catalog_conflict = (|| {
            let read = database.begin_read().map_err(storage_error)?;
            let catalog = read.open_table(CATALOG).map_err(storage_error)?;
            for write in catalog_writes {
                let current = catalog
                    .get(write.key.as_slice())
                    .map_err(storage_error)?
                    .map(|value| value.value().to_vec());
                if current != write.expected_value {
                    return Ok(true);
                }
            }
            Ok(false)
        })();
        match catalog_conflict {
            Ok(true) => {
                record_aborted_outcome(
                    database,
                    invocation_id,
                    principal_id,
                    retention_deadline_us,
                )?;
                return Err(CommitError::Conflict);
            }
            Ok(false) => {}
            Err(error) => {
                if !matches!(error, CommitError::Storage(_)) {
                    record_aborted_outcome(
                        database,
                        invocation_id,
                        principal_id,
                        retention_deadline_us,
                    )?;
                }
                return Err(error);
            }
        }
    }
    let catalog_guards = match authorization_mode {
        CatalogAuthorizationMode::Trusted => Vec::new(),
        CatalogAuthorizationMode::IndexDdl => {
            match index_ddl_authority_guards(database, principal_id, catalog_writes) {
                Ok(guards) => guards,
                Err(error) => {
                    if !matches!(error, CommitError::Storage(_)) {
                        record_aborted_outcome(
                            database,
                            invocation_id,
                            principal_id,
                            retention_deadline_us,
                        )?;
                    }
                    return Err(error);
                }
            }
        }
        CatalogAuthorizationMode::IdentityDdl => {
            match identity_ddl_authority_guards(
                database,
                principal_id,
                catalog_writes,
                audit_events,
            ) {
                Ok(guards) => guards,
                Err(error) => {
                    if !matches!(error, CommitError::Storage(_)) {
                        record_aborted_outcome(
                            database,
                            invocation_id,
                            principal_id,
                            retention_deadline_us,
                        )?;
                    }
                    return Err(error);
                }
            }
        }
        CatalogAuthorizationMode::PolicyDdl => {
            match policy_ddl_authority_guards(database, principal_id, catalog_writes, audit_events)
            {
                Ok(guards) => guards,
                Err(error) => {
                    if !matches!(error, CommitError::Storage(_)) {
                        record_aborted_outcome(
                            database,
                            invocation_id,
                            principal_id,
                            retention_deadline_us,
                        )?;
                    }
                    return Err(error);
                }
            }
        }
        CatalogAuthorizationMode::ConstraintDdl => {
            match constraint_ddl_authority_guards(
                database,
                principal_id,
                catalog_writes,
                audit_events,
            ) {
                Ok(guards) => guards,
                Err(error) => {
                    if !matches!(error, CommitError::Storage(_)) {
                        record_aborted_outcome(
                            database,
                            invocation_id,
                            principal_id,
                            retention_deadline_us,
                        )?;
                    }
                    return Err(error);
                }
            }
        }
        CatalogAuthorizationMode::ForeignKeyDdl => {
            match foreign_key_ddl_authority_guards(
                database,
                principal_id,
                catalog_writes,
                audit_events,
            ) {
                Ok(guards) => guards,
                Err(error) => {
                    if !matches!(error, CommitError::Storage(_)) {
                        record_aborted_outcome(
                            database,
                            invocation_id,
                            principal_id,
                            retention_deadline_us,
                        )?;
                    }
                    return Err(error);
                }
            }
        }
        CatalogAuthorizationMode::MembershipDdl => {
            match membership_ddl_authority_guards(
                database,
                principal_id,
                catalog_writes,
                audit_events,
            ) {
                Ok(guards) => guards,
                Err(error) => {
                    if !matches!(error, CommitError::Storage(_)) {
                        record_aborted_outcome(
                            database,
                            invocation_id,
                            principal_id,
                            retention_deadline_us,
                        )?;
                    }
                    return Err(error);
                }
            }
        }
        CatalogAuthorizationMode::SystemPrivilegeDdl => {
            match system_privilege_ddl_authority_guards(
                database,
                principal_id,
                catalog_writes,
                audit_events,
            ) {
                Ok(guards) => guards,
                Err(error) => {
                    if !matches!(error, CommitError::Storage(_)) {
                        record_aborted_outcome(
                            database,
                            invocation_id,
                            principal_id,
                            retention_deadline_us,
                        )?;
                    }
                    return Err(error);
                }
            }
        }
        CatalogAuthorizationMode::SchemaCreateDdl => {
            match schema_create_ddl_authority_guards(
                database,
                principal_id,
                catalog_writes,
                audit_events,
            ) {
                Ok(guards) => guards,
                Err(error) => {
                    if !matches!(error, CommitError::Storage(_)) {
                        record_aborted_outcome(
                            database,
                            invocation_id,
                            principal_id,
                            retention_deadline_us,
                        )?;
                    }
                    return Err(error);
                }
            }
        }
        CatalogAuthorizationMode::SchemaDropDdl => {
            match schema_drop_ddl_authority_guards(
                database,
                principal_id,
                catalog_writes,
                audit_events,
            ) {
                Ok(guards) => guards,
                Err(error) => {
                    if !matches!(error, CommitError::Storage(_)) {
                        record_aborted_outcome(
                            database,
                            invocation_id,
                            principal_id,
                            retention_deadline_us,
                        )?;
                    }
                    return Err(error);
                }
            }
        }
        CatalogAuthorizationMode::SchemaOwnerTransferDdl => {
            match schema_metadata_ddl_authority_guards(
                database,
                principal_id,
                catalog_writes,
                audit_events,
                true,
            ) {
                Ok(guards) => guards,
                Err(error) => {
                    if !matches!(error, CommitError::Storage(_)) {
                        record_aborted_outcome(
                            database,
                            invocation_id,
                            principal_id,
                            retention_deadline_us,
                        )?;
                    }
                    return Err(error);
                }
            }
        }
        CatalogAuthorizationMode::SchemaRenameDdl => {
            match schema_metadata_ddl_authority_guards(
                database,
                principal_id,
                catalog_writes,
                audit_events,
                false,
            ) {
                Ok(guards) => guards,
                Err(error) => {
                    if !matches!(error, CommitError::Storage(_)) {
                        record_aborted_outcome(
                            database,
                            invocation_id,
                            principal_id,
                            retention_deadline_us,
                        )?;
                    }
                    return Err(error);
                }
            }
        }
        CatalogAuthorizationMode::TableDropDdl => {
            match table_drop_ddl_authority_guards(
                database,
                principal_id,
                catalog_writes,
                audit_events,
            ) {
                Ok(guards) => guards,
                Err(error) => {
                    if !matches!(error, CommitError::Storage(_)) {
                        record_aborted_outcome(
                            database,
                            invocation_id,
                            principal_id,
                            retention_deadline_us,
                        )?;
                    }
                    return Err(error);
                }
            }
        }
        CatalogAuthorizationMode::TableRenameDdl => {
            match table_rename_ddl_authority_guards(
                database,
                principal_id,
                catalog_writes,
                audit_events,
                TableObjectMutationKind::Rename,
            ) {
                Ok(guards) => guards,
                Err(error) => {
                    if !matches!(error, CommitError::Storage(_)) {
                        record_aborted_outcome(
                            database,
                            invocation_id,
                            principal_id,
                            retention_deadline_us,
                        )?;
                    }
                    return Err(error);
                }
            }
        }
        CatalogAuthorizationMode::TableOwnerTransferDdl => {
            match table_rename_ddl_authority_guards(
                database,
                principal_id,
                catalog_writes,
                audit_events,
                TableObjectMutationKind::OwnerTransfer,
            ) {
                Ok(guards) => guards,
                Err(error) => {
                    if !matches!(error, CommitError::Storage(_)) {
                        record_aborted_outcome(
                            database,
                            invocation_id,
                            principal_id,
                            retention_deadline_us,
                        )?;
                    }
                    return Err(error);
                }
            }
        }
        CatalogAuthorizationMode::ColumnRenameDdl => {
            match column_rename_ddl_authority_guards(
                database,
                principal_id,
                catalog_writes,
                audit_events,
            ) {
                Ok(guards) => guards,
                Err(error) => {
                    if !matches!(error, CommitError::Storage(_)) {
                        record_aborted_outcome(
                            database,
                            invocation_id,
                            principal_id,
                            retention_deadline_us,
                        )?;
                    }
                    return Err(error);
                }
            }
        }
        CatalogAuthorizationMode::ColumnDropDdl => {
            match column_drop_ddl_authority_guards(
                database,
                principal_id,
                catalog_writes,
                audit_events,
            ) {
                Ok(guards) => guards,
                Err(error) => {
                    if !matches!(error, CommitError::Storage(_)) {
                        record_aborted_outcome(
                            database,
                            invocation_id,
                            principal_id,
                            retention_deadline_us,
                        )?;
                    }
                    return Err(error);
                }
            }
        }
        CatalogAuthorizationMode::TableCreateDdl => {
            match table_create_ddl_authority_guards(
                database,
                principal_id,
                catalog_writes,
                audit_events,
            ) {
                Ok(guards) => guards,
                Err(error) => {
                    if !matches!(error, CommitError::Storage(_)) {
                        record_aborted_outcome(
                            database,
                            invocation_id,
                            principal_id,
                            retention_deadline_us,
                        )?;
                    }
                    return Err(error);
                }
            }
        }
        CatalogAuthorizationMode::ObjectGrantDdl => {
            match object_grant_ddl_authority_guards(
                database,
                principal_id,
                catalog_writes,
                audit_events,
            ) {
                Ok(guards) => guards,
                Err(error) => {
                    if !matches!(error, CommitError::Storage(_)) {
                        record_aborted_outcome(
                            database,
                            invocation_id,
                            principal_id,
                            retention_deadline_us,
                        )?;
                    }
                    return Err(error);
                }
            }
        }
    };
    let require_usable_admin_credential = match catalog_changes_may_remove_last_admin_credential(
        authorization_mode,
        catalog_writes,
    ) {
        Ok(required) => required,
        Err(error) => {
            if !matches!(error, CommitError::Storage(_)) {
                record_aborted_outcome(
                    database,
                    invocation_id,
                    principal_id,
                    retention_deadline_us,
                )?;
            }
            return Err(error);
        }
    };
    let schema_drop_id = if matches!(authorization_mode, CatalogAuthorizationMode::SchemaDropDdl) {
        Some(
            catalog_writes
                .iter()
                .find(|write| write.key.starts_with(&3u16.to_be_bytes()))
                .and_then(|write| write.key.get(3..19))
                .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok())
                .ok_or(CommitError::InvalidCatalog)?,
        )
    } else {
        None
    };
    let table_drop_id = if matches!(authorization_mode, CatalogAuthorizationMode::TableDropDdl) {
        Some(
            catalog_writes
                .iter()
                .find(|write| write.key.starts_with(&5u16.to_be_bytes()) && write.payload.is_none())
                .and_then(|write| write.key.get(3..19))
                .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok())
                .ok_or(CommitError::InvalidCatalog)?,
        )
    } else {
        None
    };
    let backfill = match build_index_ddl_plan(database, catalog_writes) {
        Ok(mut plan) => {
            plan.schema_drop_id = schema_drop_id;
            plan.table_drop_id = table_drop_id;
            match build_constraint_validation_rows(database, catalog_writes, &[]) {
                Ok(rows) => {
                    for (table_id, expected_rows) in rows {
                        if plan
                            .expected_rows
                            .insert(table_id, expected_rows.clone())
                            .is_some_and(|previous| previous != expected_rows)
                        {
                            record_aborted_outcome(
                                database,
                                invocation_id,
                                principal_id,
                                retention_deadline_us,
                            )?;
                            return Err(CommitError::Conflict);
                        }
                    }
                }
                Err(error) => {
                    if !matches!(error, CommitError::Storage(_)) {
                        record_aborted_outcome(
                            database,
                            invocation_id,
                            principal_id,
                            retention_deadline_us,
                        )?;
                    }
                    return Err(error);
                }
            }
            plan
        }
        Err(error) => {
            if !matches!(error, CommitError::Storage(_)) {
                record_aborted_outcome(
                    database,
                    invocation_id,
                    principal_id,
                    retention_deadline_us,
                )?;
            }
            return Err(error);
        }
    };
    commit_catalog_and_audit_with_index_ddl(
        database,
        request,
        &catalog_guards,
        &backfill,
        require_usable_admin_credential,
        matches!(
            authorization_mode,
            CatalogAuthorizationMode::TableCreateDdl
                | CatalogAuthorizationMode::TableRenameDdl
                | CatalogAuthorizationMode::TableOwnerTransferDdl
                | CatalogAuthorizationMode::ColumnRenameDdl
        ),
        matches!(
            authorization_mode,
            CatalogAuthorizationMode::ColumnRenameDdl
        ),
    )
}

pub(super) fn add_catalog_guard(
    guards: &mut HashMap<Vec<u8>, Option<Vec<u8>>>,
    key: Vec<u8>,
    expected_value: Option<Vec<u8>>,
) {
    guards.entry(key).or_insert(expected_value);
}

pub(super) fn affected_table_ids_for_catalog_writes(
    database: &Database,
    writes: &[CatalogWrite],
) -> Result<Vec<[u8; 16]>, CommitError> {
    let read = database.begin_read().map_err(storage_error)?;
    let catalog = read.open_table(CATALOG).map_err(storage_error)?;
    let mut affected = HashSet::new();
    let mut changed_expressions = HashSet::new();
    for write in writes {
        let family = u16::from_be_bytes(
            write
                .key
                .get(..2)
                .ok_or(CommitError::InvalidCatalog)?
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?,
        );
        let current = catalog
            .get(write.key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        if current == write.payload && family != 9 {
            continue;
        }
        match family {
            5 => {
                if write.key.len() != 19 {
                    return Err(CommitError::InvalidCatalog);
                }
                affected.insert(
                    write.key[3..19]
                        .try_into()
                        .map_err(|_| CommitError::InvalidCatalog)?,
                );
            }
            6 => {
                for bytes in [current.as_deref(), write.payload.as_deref()]
                    .into_iter()
                    .flatten()
                {
                    affected.insert(column_table_id(&write.key, bytes)?);
                }
            }
            8 => {
                for bytes in [current.as_deref(), write.payload.as_deref()]
                    .into_iter()
                    .flatten()
                {
                    affected.insert(constraint_table_id(&write.key, bytes)?);
                    if u16::from_le_bytes(
                        catalog_field_bytes(&write.key, bytes, 8, 2)?
                            .try_into()
                            .map_err(|_| CommitError::InvalidCatalog)?,
                    ) == 5
                    {
                        let target_id: [u8; 16] = catalog_field_bytes(&write.key, bytes, 8, 5)?
                            .try_into()
                            .map_err(|_| CommitError::InvalidCatalog)?;
                        let target_key = id_catalog_key(8, target_id);
                        if let Some(target_bytes) =
                            read_final_catalog_value(&catalog, writes, &target_key)?
                        {
                            affected.insert(constraint_table_id(&target_key, &target_bytes)?);
                        }
                    }
                }
            }
            9 => {
                for bytes in [current.as_deref(), write.payload.as_deref()]
                    .into_iter()
                    .flatten()
                {
                    affected.insert(decode_index_definition(&write.key, bytes)?.1);
                }
            }
            13 => {
                for bytes in [current.as_deref(), write.payload.as_deref()]
                    .into_iter()
                    .flatten()
                {
                    affected.insert(policy_table_id(&write.key, bytes)?);
                }
            }
            14 => {
                if write.key.len() != 39 {
                    return Err(CommitError::InvalidCatalog);
                }
                let policy_id: [u8; 16] = write.key[3..19]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                let policy_key = id_catalog_key(13, policy_id);
                if let Some(policy_bytes) = read_final_catalog_value(&catalog, writes, &policy_key)?
                {
                    affected.insert(policy_table_id(&policy_key, &policy_bytes)?);
                }
            }
            7 => {
                if write.key.len() != 19 {
                    return Err(CommitError::InvalidCatalog);
                }
                changed_expressions.insert(
                    write.key[3..19]
                        .try_into()
                        .map_err(|_| CommitError::InvalidCatalog)?,
                );
            }
            _ => {}
        }
    }
    if !changed_expressions.is_empty() {
        add_expression_dependent_tables(&catalog, writes, &changed_expressions, &mut affected)?;
    }
    let mut affected = affected.into_iter().collect::<Vec<_>>();
    affected.sort_unstable();
    Ok(affected)
}

pub(super) fn read_final_catalog_value(
    catalog: &ReadOnlyTable<&[u8], &[u8]>,
    writes: &[CatalogWrite],
    key: &[u8],
) -> Result<Option<Vec<u8>>, CommitError> {
    if let Some(write) = writes.iter().find(|write| write.key == key) {
        return Ok(write.payload.clone());
    }
    Ok(catalog
        .get(key)
        .map_err(storage_error)?
        .map(|value| value.value().to_vec()))
}

pub(super) fn add_expression_dependent_tables(
    catalog: &ReadOnlyTable<&[u8], &[u8]>,
    writes: &[CatalogWrite],
    expression_ids: &HashSet<[u8; 16]>,
    affected: &mut HashSet<[u8; 16]>,
) -> Result<(), CommitError> {
    for family in [6u16, 8, 13] {
        let prefix = family.to_be_bytes();
        let tags: &[u16] = match family {
            6 => &[7, 10],
            8 => &[4],
            13 => &[5, 6],
            _ => unreachable!(),
        };
        let mut count = 0usize;
        for entry in catalog.range(prefix.as_slice()..).map_err(storage_error)? {
            let (key, value) = entry.map_err(storage_error)?;
            let key = key.value();
            if !key.starts_with(&prefix) {
                break;
            }
            if writes.iter().any(|write| write.key == key) {
                continue;
            }
            count += 1;
            if count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
                return Err(CommitError::InvalidCatalog);
            }
            if catalog_record_references_expression(
                key,
                value.value(),
                family,
                tags,
                expression_ids,
            )? {
                affected.insert(match family {
                    6 => column_table_id(key, value.value())?,
                    8 => constraint_table_id(key, value.value())?,
                    13 => policy_table_id(key, value.value())?,
                    _ => unreachable!(),
                });
            }
        }
        for write in writes.iter().filter(|write| write.key.starts_with(&prefix)) {
            let Some(bytes) = write.payload.as_deref() else {
                continue;
            };
            count += 1;
            if count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
                return Err(CommitError::InvalidCatalog);
            }
            if catalog_record_references_expression(
                &write.key,
                bytes,
                family,
                tags,
                expression_ids,
            )? {
                affected.insert(match family {
                    6 => column_table_id(&write.key, bytes)?,
                    8 => constraint_table_id(&write.key, bytes)?,
                    13 => policy_table_id(&write.key, bytes)?,
                    _ => unreachable!(),
                });
            }
        }
    }
    Ok(())
}

pub(super) fn catalog_record_references_expression(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tags: &[u16],
    expression_ids: &HashSet<[u8; 16]>,
) -> Result<bool, CommitError> {
    let record = decode_registered_envelope(bytes).map_err(|_| CommitError::InvalidCatalog)?;
    if record.family_code != family || record.logical_key != key {
        return Err(CommitError::InvalidCatalog);
    }
    for field in record
        .fields
        .iter()
        .filter(|field| tags.contains(&field.tag))
    {
        let id: [u8; 16] = field
            .payload
            .as_slice()
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        if expression_ids.contains(&id) {
            return Ok(true);
        }
    }
    Ok(false)
}
