use super::catalog_graph_validation::*;
use super::catalog_invariant_validation::*;
use super::constraint_validation::*;
use super::index_ddl::*;
use super::*;
use crate::index_locator::IndexLocator;
use crate::metadata::{EngineMetadata, Lifecycle};
use crate::outcome::{InvocationOutcome, OutcomeStatus};
use crate::row_record::RowRecord;
use crate::schema::{AUDIT, CATALOG, ENGINE_META, INDEXES, OUTCOMES, ROW_INDEXES, ROWS, STATS};
use redb::{Database, ReadableTable, ReadableTableMetadata};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

/// Atomically publish a row write set and commit sequence without index deltas.
/// This low-level helper does not derive index changes from typed row values or
/// enforce authorization; it is not an executor-facing write path.
pub fn commit_rows(
    database: &Database,
    invocation_id: [u8; 16],
    principal_id: [u8; 16],
    retention_deadline_us: i64,
    writes: &[RowWrite],
) -> Result<u64, CommitError> {
    commit_storage_changes(
        database,
        CommitRequest {
            invocation_id,
            principal_id,
            retention_deadline_us,
            rows: writes,
            indexes: &[],
            foreign_keys: &[],
            catalog_guards: &[],
            catalog: &[],
            audit: &[],
        },
    )
}

/// Atomically publish rows and explicit logical index-key mutations. The writer
/// validates index/table mapping and uniqueness, maintains reverse membership,
/// and refreshes retained locator versions. It does not derive a complete index
/// delta from typed row values or enforce authorization, so it is not an
/// executor-facing write path.
pub fn commit_rows_and_indexes(
    database: &Database,
    invocation_id: [u8; 16],
    principal_id: [u8; 16],
    retention_deadline_us: i64,
    writes: &[RowWrite],
    index_writes: &[IndexWrite],
) -> Result<u64, CommitError> {
    commit_storage_changes(
        database,
        CommitRequest {
            invocation_id,
            principal_id,
            retention_deadline_us,
            rows: writes,
            indexes: index_writes,
            foreign_keys: &[],
            catalog_guards: &[],
            catalog: &[],
            audit: &[],
        },
    )
}

/// Publish every record family participating in one operation through one
/// Immediate/two-phase redb transaction. This low-level function does not
/// acquire DDL fences; coordinated runtime paths hold the affected shared or
/// exclusive guards before calling it.
pub fn commit_storage_changes(
    database: &Database,
    request: CommitRequest<'_>,
) -> Result<u64, CommitError> {
    commit_storage_changes_with_gate(database, request, || true, |_| {})
}

/// Publish storage changes only if `before_durable_commit` grants the
/// irreversible commit transition. The callback runs after all validations
/// and writes have been staged, immediately before redb commits them.
pub(crate) fn commit_storage_changes_with_gate(
    database: &Database,
    request: CommitRequest<'_>,
    before_durable_commit: impl FnOnce() -> bool,
    on_write_acquisition: impl FnOnce(Duration),
) -> Result<u64, CommitError> {
    let expected_rows =
        match build_constraint_validation_rows(database, request.catalog, request.rows) {
            Ok(rows) => rows,
            Err(error) => {
                if matches!(error, CommitError::Storage(_)) {
                    crate::storage_manager::mark_database_uncertain(database);
                }
                if !matches!(error, CommitError::Storage(_)) {
                    record_aborted_outcome(
                        database,
                        request.invocation_id,
                        request.principal_id,
                        request.retention_deadline_us,
                    )?;
                }
                return Err(error);
            }
        };
    if expected_rows.is_empty() {
        return commit_storage_changes_inner_with_admin_credential_check(
            database,
            request,
            None,
            false,
            false,
            false,
            (before_durable_commit, on_write_acquisition),
        );
    }
    let plan = IndexDdlPlan {
        expected_rows,
        ..IndexDdlPlan::default()
    };
    commit_storage_changes_inner_with_admin_credential_check(
        database,
        request,
        Some(&plan),
        false,
        false,
        false,
        (before_durable_commit, on_write_acquisition),
    )
}

pub(super) fn commit_storage_changes_with_index_ddl(
    database: &Database,
    request: CommitRequest<'_>,
    backfill: &IndexDdlPlan,
    require_usable_admin_credential: bool,
    check_table_name_uniqueness: bool,
    check_column_name_uniqueness: bool,
) -> Result<u64, CommitError> {
    commit_storage_changes_inner_with_admin_credential_check(
        database,
        request,
        Some(backfill),
        require_usable_admin_credential,
        check_table_name_uniqueness,
        check_column_name_uniqueness,
        (|| true, |_| {}),
    )
}

pub(super) fn commit_storage_changes_inner_with_admin_credential_check(
    database: &Database,
    request: CommitRequest<'_>,
    index_ddl_plan: Option<&IndexDdlPlan>,
    require_usable_admin_credential: bool,
    check_table_name_uniqueness: bool,
    check_column_name_uniqueness: bool,
    commit_callbacks: (impl FnOnce() -> bool, impl FnOnce(Duration)),
) -> Result<u64, CommitError> {
    let result = commit_storage_changes_inner_unchecked(
        database,
        request,
        index_ddl_plan,
        require_usable_admin_credential,
        check_table_name_uniqueness,
        check_column_name_uniqueness,
        commit_callbacks,
    );
    if matches!(&result, Err(CommitError::Storage(_))) {
        crate::storage_manager::mark_database_uncertain(database);
    }
    result
}

pub(super) fn commit_storage_changes_inner_unchecked(
    database: &Database,
    request: CommitRequest<'_>,
    index_ddl_plan: Option<&IndexDdlPlan>,
    require_usable_admin_credential: bool,
    check_table_name_uniqueness: bool,
    check_column_name_uniqueness: bool,
    commit_callbacks: (impl FnOnce() -> bool, impl FnOnce(Duration)),
) -> Result<u64, CommitError> {
    let (before_durable_commit, on_write_acquisition) = commit_callbacks;
    let CommitRequest {
        invocation_id,
        principal_id,
        retention_deadline_us,
        rows: writes,
        indexes: index_writes,
        foreign_keys,
        catalog_guards,
        catalog: catalog_writes,
        audit: audit_events,
    } = request;
    let mut unique = HashSet::with_capacity(writes.len());
    for write in writes {
        if write.key[..16] == [0; 16] {
            return Err(CommitError::InvalidWrite);
        }
        if !unique.insert(write.key) {
            return Err(CommitError::DuplicateKey);
        }
        if write.expected_version.is_none() && write.payload.is_none() {
            return Err(CommitError::InvalidWrite);
        }
    }
    let rows_by_key: std::collections::HashMap<_, _> =
        writes.iter().map(|write| (write.key, write)).collect();
    let mut unique_indexes = HashSet::with_capacity(index_writes.len());
    for index_write in index_writes {
        let row_write = rows_by_key.get(&index_write.row_key);
        let is_index_ddl_write = index_ddl_plan.is_some_and(|plan| {
            let index_id: Option<[u8; 16]> =
                index_write.key.get(..16).and_then(|id| id.try_into().ok());
            index_id
                .and_then(|id| plan.index_tables.get(&id).copied().map(|table| (id, table)))
                .is_some_and(|(index_id, table_id)| {
                    table_id == index_write.row_key[..16]
                        && (if index_write.present {
                            (index_write.expected_version.is_none()
                                && plan.created_indexes.contains(&index_id))
                                || plan.rebuilt_indexes.contains(&index_id)
                        } else {
                            index_write.expected_version.is_some()
                                && (plan.dropped_indexes.contains(&index_id)
                                    || plan.rebuilt_indexes.contains(&index_id))
                        })
                        && plan.writes.contains(index_write)
                })
        });
        if index_write.key.len() < 33
            || index_write.key.len() > crate::index_key::MAX_INDEX_KEY_BYTES
            || index_write.key[index_write.key.len() - 16..] != index_write.row_key[16..]
            || (row_write.is_none() && !is_index_ddl_write)
            || !unique_indexes.insert(index_write.key.as_slice())
            || (index_write.present && row_write.is_some_and(|write| write.payload.is_none()))
            || (!index_write.present && index_write.expected_version.is_none())
        {
            return Err(CommitError::InvalidIndex);
        }
        if let Some(expected_version) = index_write.expected_version
            && expected_version == 0
        {
            return Err(CommitError::InvalidIndex);
        }
    }
    let mut unique_catalog = HashSet::with_capacity(catalog_writes.len());
    for write in catalog_writes {
        if !unique_catalog.insert(write.key.as_slice())
            || (write.expected_value.is_none() && write.payload.is_none())
        {
            return Err(CommitError::InvalidCatalog);
        }
        for bytes in [write.expected_value.as_ref(), write.payload.as_ref()]
            .into_iter()
            .flatten()
        {
            decode_catalog_record(&write.key, bytes)?;
        }
    }
    let dropped_table_ids = catalog_writes
        .iter()
        .filter(|write| {
            write.payload.is_none()
                && write.key.len() == 19
                && write.key[..2] == 5u16.to_be_bytes()
                && write.key[2] == 1
        })
        .map(|write| {
            write.key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)
        })
        .collect::<Result<Vec<[u8; 16]>, _>>()?;
    let mut unique_guards = HashSet::with_capacity(catalog_guards.len());
    for guard in catalog_guards {
        if !unique_guards.insert(guard.key.as_slice()) {
            return Err(CommitError::InvalidCatalog);
        }
        if let Some(expected) = &guard.expected_value {
            decode_catalog_record(&guard.key, expected)?;
        }
    }
    let mut unique_audit = HashSet::with_capacity(audit_events.len());
    let mut encoded_audit = Vec::with_capacity(audit_events.len());
    for event in audit_events {
        let bytes = event.encode().map_err(|_| CommitError::InvalidAudit)?;
        if !unique_audit.insert(event.event_id) {
            return Err(CommitError::InvalidAudit);
        }
        encoded_audit.push((event.event_id, bytes));
    }
    if (!catalog_writes.is_empty() || !audit_events.is_empty()) && encoded_audit.is_empty() {
        return Err(CommitError::InvalidAudit);
    }
    if !catalog_writes.is_empty()
        && !audit_events.iter().any(|event| {
            matches!(
                event.action,
                crate::audit::AuditAction::Ddl
                    | crate::audit::AuditAction::AuthorityChange
                    | crate::audit::AuditAction::TokenLifecycle
            ) && event.result == crate::audit::AuditResult::Succeeded
                && event.actor_id == Some(principal_id)
        })
    {
        return Err(CommitError::InvalidAudit);
    }

    let lock_wait_started = Instant::now();
    let write_transaction = database.begin_write();
    on_write_acquisition(lock_wait_started.elapsed());
    let mut transaction = write_transaction.map_err(storage_error)?;
    transaction
        .set_durability(redb::Durability::Immediate)
        .map_err(storage_error)?;
    transaction.set_two_phase_commit(true);

    let current_metadata = {
        let table = transaction.open_table(ENGINE_META).map_err(storage_error)?;
        let value = table
            .get(&b"format"[..])
            .map_err(storage_error)?
            .ok_or(CommitError::InvalidMetadata)?
            .value()
            .to_vec();
        EngineMetadata::decode(&value).map_err(|_| CommitError::InvalidMetadata)?
    };
    if !current_metadata.supports_current_formats() {
        return Err(CommitError::UnsupportedFormat);
    }
    if current_metadata.lifecycle != Lifecycle::Ready {
        return Err(CommitError::NotReady);
    }
    let next_sequence = current_metadata
        .commit_sequence
        .checked_add(1)
        .ok_or(CommitError::SequenceExhausted)?;
    let outcome = InvocationOutcome {
        status: OutcomeStatus::Committed,
        invocation_id,
        principal_id,
        commit_sequence: next_sequence,
        retention_deadline_us,
    }
    .encode()
    .map_err(|_| CommitError::InvalidWrite)?;
    {
        let table = transaction.open_table(OUTCOMES).map_err(storage_error)?;
        if table
            .get(&invocation_id[..])
            .map_err(storage_error)?
            .is_some()
        {
            return Err(CommitError::OutcomeAlreadyExists);
        }
    }

    let mut conflict = false;
    {
        let table = transaction.open_table(ROWS).map_err(storage_error)?;
        for write in writes {
            let current = table
                .get(&write.key[..])
                .map_err(storage_error)?
                .map(|value| RowRecord::decode(value.value()).map(|row| row.row_version))
                .transpose()
                .map_err(|_| CommitError::InvalidRow)?;
            if current != write.expected_version {
                conflict = true;
                break;
            }
        }
    }
    if conflict {
        transaction.abort().map_err(storage_error)?;
        record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
        return Err(CommitError::Conflict);
    }

    if let Some(plan) = index_ddl_plan {
        match validate_index_ddl_snapshot(&transaction, plan) {
            Ok(()) => {}
            Err(CommitError::Conflict) => {
                transaction.abort().map_err(storage_error)?;
                record_aborted_outcome(
                    database,
                    invocation_id,
                    principal_id,
                    retention_deadline_us,
                )?;
                return Err(CommitError::Conflict);
            }
            Err(
                error @ (CommitError::InvalidRow
                | CommitError::InvalidIndex
                | CommitError::InvalidCatalog
                | CommitError::Storage(_)),
            ) => {
                transaction.abort().map_err(storage_error)?;
                record_aborted_outcome(
                    database,
                    invocation_id,
                    principal_id,
                    retention_deadline_us,
                )?;
                return Err(error);
            }
            Err(error) => return Err(error),
        }
    }

    let mut index_conflict = false;
    {
        let table = transaction.open_table(INDEXES).map_err(storage_error)?;
        let reverse_table = transaction.open_table(ROW_INDEXES).map_err(storage_error)?;
        for write in index_writes {
            let current = table
                .get(&write.key[..])
                .map_err(storage_error)?
                .map(|value| {
                    IndexLocator::decode_for_key(&write.key, value.value())
                        .map(|locator| locator.row_version)
                })
                .transpose()
                .map_err(|_| CommitError::InvalidIndex)?;
            if current != write.expected_version {
                index_conflict = true;
                break;
            }
            let reverse_key = reverse_index_key(&write.row_key, &write.key);
            let reverse_exists = reverse_table
                .get(&reverse_key[..])
                .map_err(storage_error)?
                .is_some();
            if current.is_some() != reverse_exists {
                return Err(CommitError::InvalidIndex);
            }
        }
    }
    if index_conflict {
        transaction.abort().map_err(storage_error)?;
        record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
        return Err(CommitError::Conflict);
    }

    let mut unique_violation = false;
    {
        let table = transaction.open_table(INDEXES).map_err(storage_error)?;
        let mut unique_prefixes = HashMap::<Vec<u8>, ([u8; 16], HashSet<[u8; 16]>)>::new();
        for addition in index_writes
            .iter()
            .filter(|write| write.present && write.unique && !write.null_exempt)
        {
            let prefix = &addition.key[..addition.key.len() - 16];
            let (table_id, row_ids) = unique_prefixes
                .entry(prefix.to_vec())
                .or_insert_with(|| (addition.row_key[..16].try_into().unwrap(), HashSet::new()));
            if *table_id != addition.row_key[..16]
                || !row_ids.insert(addition.row_key[16..].try_into().unwrap())
                || row_ids.len() > 1
            {
                unique_violation = true;
                break;
            }
        }
        if !unique_violation && !unique_prefixes.is_empty() {
            let removals = index_writes
                .iter()
                .filter(|write| !write.present)
                .map(|write| write.key.as_slice())
                .collect::<HashSet<_>>();
            'prefixes: for (prefix, (table_id, row_ids)) in &unique_prefixes {
                for entry in table.range(prefix.as_slice()..).map_err(storage_error)? {
                    let (stored_key, stored_value) = entry.map_err(storage_error)?;
                    let stored_key = stored_key.value().to_vec();
                    if !stored_key.starts_with(prefix) {
                        break;
                    }
                    if stored_key.len() < 33 {
                        return Err(CommitError::InvalidIndex);
                    }
                    let row_id: [u8; 16] = stored_key[stored_key.len() - 16..]
                        .try_into()
                        .map_err(|_| CommitError::InvalidIndex)?;
                    if row_ids.contains(&row_id) || removals.contains(stored_key.as_slice()) {
                        continue;
                    }
                    let mut candidate_row_key = [0; 32];
                    candidate_row_key[..16].copy_from_slice(table_id);
                    candidate_row_key[16..].copy_from_slice(&row_id);
                    if rows_by_key
                        .get(&candidate_row_key)
                        .is_some_and(|write| write.payload.is_none())
                    {
                        continue;
                    }
                    IndexLocator::decode_for_key(&stored_key, stored_value.value())
                        .map_err(|_| CommitError::InvalidIndex)?;
                    unique_violation = true;
                    break 'prefixes;
                }
            }
        }
    }
    if unique_violation {
        transaction.abort().map_err(storage_error)?;
        record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
        return Err(CommitError::UniqueViolation);
    }

    let mut catalog_conflict = false;
    {
        let table = transaction.open_table(CATALOG).map_err(storage_error)?;
        for guard in catalog_guards {
            let current = table
                .get(&guard.key[..])
                .map_err(storage_error)?
                .map(|value| value.value().to_vec());
            if current.as_deref() != guard.expected_value.as_deref() {
                catalog_conflict = true;
                break;
            }
        }
        for write in catalog_writes {
            if catalog_conflict {
                break;
            }
            let current = table
                .get(&write.key[..])
                .map_err(storage_error)?
                .map(|value| value.value().to_vec());
            if let Some(bytes) = &current {
                decode_catalog_record(&write.key, bytes)?;
            }
            if current != write.expected_value {
                catalog_conflict = true;
                break;
            }
        }
    }
    if catalog_conflict {
        transaction.abort().map_err(storage_error)?;
        record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
        return Err(CommitError::Conflict);
    }
    if let Some(schema_id) = index_ddl_plan.and_then(|plan| plan.schema_drop_id)
        && let Err(error) = validate_schema_drop_empty(&transaction, schema_id)
    {
        transaction.abort().map_err(storage_error)?;
        if !matches!(error, CommitError::Storage(_)) {
            record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
        }
        return Err(error);
    }
    match validate_foreign_key_constraints(&transaction, writes, foreign_keys, false) {
        Ok(()) => {}
        Err(
            error @ (CommitError::InvalidCatalog
            | CommitError::InvalidRow
            | CommitError::ForeignKeyViolation
            | CommitError::ForeignKeyValidationLimitExceeded),
        ) => {
            transaction.abort().map_err(storage_error)?;
            record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
            return Err(error);
        }
        Err(error) => return Err(error),
    }
    match validate_generated_identity_collisions(&transaction, writes, catalog_writes) {
        Ok(()) => {}
        Err(
            error @ (CommitError::InvalidCatalog
            | CommitError::InvalidRow
            | CommitError::UniqueViolation
            | CommitError::ForeignKeyValidationLimitExceeded),
        ) => {
            transaction.abort().map_err(storage_error)?;
            record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
            return Err(error);
        }
        Err(error) => return Err(error),
    }
    match validate_final_membership_graph(&transaction, catalog_writes) {
        Ok(()) => {}
        Err(CommitError::InvalidCatalog) => {
            transaction.abort().map_err(storage_error)?;
            record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
            return Err(CommitError::InvalidCatalog);
        }
        Err(error) => return Err(error),
    }
    match validate_final_dependency_edges(
        &transaction,
        catalog_writes,
        current_metadata.database_id,
    ) {
        Ok(()) => {}
        Err(CommitError::InvalidCatalog) => {
            transaction.abort().map_err(storage_error)?;
            record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
            return Err(CommitError::InvalidCatalog);
        }
        Err(error) => return Err(error),
    }
    match validate_final_object_owners(&transaction, catalog_writes) {
        Ok(()) => {}
        Err(CommitError::InvalidCatalog) => {
            transaction.abort().map_err(storage_error)?;
            record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
            return Err(CommitError::InvalidCatalog);
        }
        Err(error) => return Err(error),
    }
    match validate_final_principal_names(&transaction, catalog_writes) {
        Ok(()) => {}
        Err(CommitError::InvalidCatalog) => {
            transaction.abort().map_err(storage_error)?;
            record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
            return Err(CommitError::InvalidCatalog);
        }
        Err(error) => return Err(error),
    }
    match validate_final_schema_names(&transaction, catalog_writes) {
        Ok(()) => {}
        Err(CommitError::InvalidCatalog) => {
            transaction.abort().map_err(storage_error)?;
            record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
            return Err(CommitError::InvalidCatalog);
        }
        Err(error) => return Err(error),
    }
    match validate_final_schema_search_paths(&transaction, catalog_writes) {
        Ok(()) => {}
        Err(CommitError::InvalidCatalog) => {
            transaction.abort().map_err(storage_error)?;
            record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
            return Err(CommitError::InvalidCatalog);
        }
        Err(error) => return Err(error),
    }
    if check_table_name_uniqueness {
        match validate_final_table_names(&transaction, catalog_writes) {
            Ok(()) => {}
            Err(CommitError::InvalidCatalog) => {
                transaction.abort().map_err(storage_error)?;
                record_aborted_outcome(
                    database,
                    invocation_id,
                    principal_id,
                    retention_deadline_us,
                )?;
                return Err(CommitError::InvalidCatalog);
            }
            Err(error) => return Err(error),
        }
    }
    if check_column_name_uniqueness {
        match validate_final_column_names(&transaction, catalog_writes) {
            Ok(()) => {}
            Err(CommitError::InvalidCatalog) => {
                transaction.abort().map_err(storage_error)?;
                record_aborted_outcome(
                    database,
                    invocation_id,
                    principal_id,
                    retention_deadline_us,
                )?;
                return Err(CommitError::InvalidCatalog);
            }
            Err(error) => return Err(error),
        }
    }
    match validate_final_identity_tombstones(&transaction, catalog_writes) {
        Ok(()) => {}
        Err(CommitError::InvalidCatalog) => {
            transaction.abort().map_err(storage_error)?;
            record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
            return Err(CommitError::InvalidCatalog);
        }
        Err(error) => return Err(error),
    }
    match validate_final_credential_verifiers(&transaction, catalog_writes) {
        Ok(()) => {}
        Err(CommitError::InvalidCatalog) => {
            transaction.abort().map_err(storage_error)?;
            record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
            return Err(CommitError::InvalidCatalog);
        }
        Err(error) => return Err(error),
    }
    match validate_final_typed_object_mappings(&transaction, catalog_writes) {
        Ok(()) => {}
        Err(CommitError::InvalidCatalog) => {
            transaction.abort().map_err(storage_error)?;
            record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
            return Err(CommitError::InvalidCatalog);
        }
        Err(error) => return Err(error),
    }
    match validate_final_bootstrap_builtin_targets(
        &transaction,
        catalog_writes,
        current_metadata.database_id,
    ) {
        Ok(()) => {}
        Err(CommitError::InvalidCatalog) => {
            transaction.abort().map_err(storage_error)?;
            record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
            return Err(CommitError::InvalidCatalog);
        }
        Err(error) => return Err(error),
    }
    let (current_index_definitions, final_index_definitions) =
        load_index_definitions(&transaction, catalog_writes)?;
    match validate_final_catalog_index_definitions(&transaction, catalog_writes) {
        Ok(()) => {}
        Err(error @ (CommitError::InvalidCatalog | CommitError::InvalidRow)) => {
            transaction.abort().map_err(storage_error)?;
            record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
            return Err(error);
        }
        Err(error) => return Err(error),
    }
    match validate_index_definition_changes(&transaction, catalog_writes, writes, index_ddl_plan) {
        Ok(()) => {}
        Err(error @ (CommitError::InvalidCatalog | CommitError::InvalidRow)) => {
            transaction.abort().map_err(storage_error)?;
            record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
            return Err(error);
        }
        Err(error) => return Err(error),
    }
    let new_foreign_keys = {
        let catalog = transaction.open_table(CATALOG).map_err(storage_error)?;
        load_new_foreign_key_checks(&catalog, catalog_writes)
    };
    let new_foreign_keys = match new_foreign_keys {
        Ok(checks) => checks,
        Err(CommitError::InvalidCatalog) => {
            transaction.abort().map_err(storage_error)?;
            record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
            return Err(CommitError::InvalidCatalog);
        }
        Err(error) => return Err(error),
    };
    match validate_foreign_key_constraints(&transaction, writes, &new_foreign_keys, true) {
        Ok(()) => {}
        Err(
            error @ (CommitError::InvalidCatalog
            | CommitError::InvalidRow
            | CommitError::ForeignKeyViolation
            | CommitError::ForeignKeyValidationLimitExceeded),
        ) => {
            transaction.abort().map_err(storage_error)?;
            record_aborted_outcome(database, invocation_id, principal_id, retention_deadline_us)?;
            return Err(error);
        }
        Err(error) => return Err(error),
    }
    for write in index_writes {
        let index_id: [u8; 16] = write.key[..16]
            .try_into()
            .map_err(|_| CommitError::InvalidIndex)?;
        let definitions = if write.present {
            &final_index_definitions
        } else {
            &current_index_definitions
        };
        let table_id: [u8; 16] = write.row_key[..16]
            .try_into()
            .map_err(|_| CommitError::InvalidIndex)?;
        if definitions.get(&index_id) != Some(&table_id) {
            return Err(CommitError::InvalidIndex);
        }
    }
    let retained_index_mutations = retained_index_mutations(
        &transaction,
        writes,
        index_writes,
        &current_index_definitions,
        &final_index_definitions,
        next_sequence,
    )?;
    {
        let table = transaction.open_table(AUDIT).map_err(storage_error)?;
        let current_events = usize::try_from(table.len().map_err(storage_error)?)
            .map_err(|_| CommitError::AuditCapacityExceeded)?;
        if !encoded_audit.is_empty()
            && !crate::audit::has_audit_capacity(current_events, encoded_audit.len())
        {
            return Err(CommitError::AuditCapacityExceeded);
        }
        for (event_id, _) in &encoded_audit {
            if table.get(&event_id[..]).map_err(storage_error)?.is_some() {
                return Err(CommitError::AuditAlreadyExists);
            }
        }
    }

    if require_usable_admin_credential {
        match validate_final_admin_credential(&transaction, catalog_writes) {
            Ok(()) => {}
            Err(error @ (CommitError::DdlAuthorizationDenied | CommitError::InvalidCatalog)) => {
                transaction.abort().map_err(storage_error)?;
                record_aborted_outcome(
                    database,
                    invocation_id,
                    principal_id,
                    retention_deadline_us,
                )?;
                return Err(error);
            }
            Err(error) => return Err(error),
        }
    }

    {
        let mut table = transaction.open_table(ROWS).map_err(storage_error)?;
        for write in writes {
            match &write.payload {
                Some(payload) => {
                    let record = RowRecord {
                        row_version: next_sequence,
                        payload: payload.clone(),
                    }
                    .encode()
                    .map_err(|_| CommitError::InvalidWrite)?;
                    table
                        .insert(&write.key[..], &record[..])
                        .map_err(storage_error)?;
                }
                None => {
                    table.remove(&write.key[..]).map_err(storage_error)?;
                }
            }
        }
    }
    {
        let mut table = transaction.open_table(INDEXES).map_err(storage_error)?;
        let mut reverse_table = transaction.open_table(ROW_INDEXES).map_err(storage_error)?;
        for mutation in &retained_index_mutations {
            match &mutation.locator {
                Some(locator) => {
                    table
                        .insert(&mutation.index_key[..], &locator[..])
                        .map_err(storage_error)?;
                }
                None => {
                    table
                        .remove(&mutation.index_key[..])
                        .map_err(storage_error)?;
                    reverse_table
                        .remove(&mutation.row_index_key[..])
                        .map_err(storage_error)?;
                }
            }
        }
        for write in index_writes {
            if write.present {
                let row_version = if rows_by_key.contains_key(&write.row_key) {
                    next_sequence
                } else {
                    index_ddl_plan
                        .and_then(|plan| plan.row_versions.get(&write.row_key).copied())
                        .ok_or(CommitError::InvalidIndex)?
                };
                let locator = IndexLocator {
                    row_id: write.row_key[16..].try_into().unwrap(),
                    row_version,
                }
                .encode();
                table
                    .insert(&write.key[..], &locator[..])
                    .map_err(storage_error)?;
                let reverse_key = reverse_index_key(&write.row_key, &write.key);
                reverse_table
                    .insert(&reverse_key[..], &b""[..])
                    .map_err(storage_error)?;
            } else {
                table.remove(&write.key[..]).map_err(storage_error)?;
                let reverse_key = reverse_index_key(&write.row_key, &write.key);
                reverse_table
                    .remove(&reverse_key[..])
                    .map_err(storage_error)?;
            }
        }
    }
    {
        let mut table = transaction.open_table(CATALOG).map_err(storage_error)?;
        let mut principal_names = transaction
            .open_table(crate::schema::PRINCIPAL_NAMES)
            .map_err(storage_error)?;
        let mut schema_names = transaction
            .open_table(crate::schema::SCHEMA_NAMES)
            .map_err(storage_error)?;
        let mut relation_names = transaction
            .open_table(crate::schema::RELATION_NAMES)
            .map_err(storage_error)?;
        let mut memberships = transaction
            .open_table(crate::schema::MEMBERSHIPS)
            .map_err(storage_error)?;
        let mut grants = transaction
            .open_table(crate::schema::GRANTS)
            .map_err(storage_error)?;
        let mut policies = transaction
            .open_table(crate::schema::POLICIES)
            .map_err(storage_error)?;
        let mut dependencies = transaction
            .open_table(crate::schema::DEPENDENCIES)
            .map_err(storage_error)?;
        let mut column_lookups = transaction
            .open_table(crate::schema::COLUMN_LOOKUPS)
            .map_err(storage_error)?;
        let mut index_names = transaction
            .open_table(crate::schema::INDEX_NAMES)
            .map_err(storage_error)?;
        let mut table_indexes = transaction
            .open_table(crate::schema::TABLE_INDEXES)
            .map_err(storage_error)?;
        let mut table_constraints = transaction
            .open_table(crate::schema::TABLE_CONSTRAINTS)
            .map_err(storage_error)?;
        let mut object_owners = transaction
            .open_table(crate::schema::OBJECT_OWNERS)
            .map_err(storage_error)?;
        let principal_prefix = 10u16.to_be_bytes();
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&principal_prefix))
        {
            if let Some(current) = table.get(write.key.as_slice()).map_err(storage_error)? {
                let current_name = catalog_field_bytes(&write.key, current.value(), 10, 2)?;
                principal_names
                    .remove(current_name.as_slice())
                    .map_err(storage_error)?;
            }
        }
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&principal_prefix))
        {
            if let Some(payload) = &write.payload {
                let name = catalog_field_bytes(&write.key, payload, 10, 2)?;
                let principal_id = write.key.get(3..19).ok_or(CommitError::InvalidCatalog)?;
                if principal_names
                    .insert(name.as_slice(), principal_id)
                    .map_err(storage_error)?
                    .is_some()
                {
                    return Err(CommitError::InvalidCatalog);
                }
            }
        }
        let schema_prefix = 2u16.to_be_bytes();
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&schema_prefix))
        {
            if let Some(current) = table.get(write.key.as_slice()).map_err(storage_error)?
                && catalog_field_u16(&write.key, current.value(), 2, 1)? == 1
            {
                let current_name = catalog_field_bytes(&write.key, current.value(), 2, 3)?;
                schema_names
                    .remove(current_name.as_slice())
                    .map_err(storage_error)?;
            }
        }
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&schema_prefix))
        {
            if let Some(payload) = &write.payload
                && catalog_field_u16(&write.key, payload, 2, 1)? == 1
            {
                let name = catalog_field_bytes(&write.key, payload, 2, 3)?;
                let schema_id = write.key.get(3..19).ok_or(CommitError::InvalidCatalog)?;
                if schema_names
                    .insert(name.as_slice(), schema_id)
                    .map_err(storage_error)?
                    .is_some()
                {
                    return Err(CommitError::InvalidCatalog);
                }
            }
        }
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&schema_prefix))
        {
            if write.key.len() != 19 || write.key[2] != 1 {
                return Err(CommitError::InvalidCatalog);
            }
            if let Some(current) = table.get(write.key.as_slice()).map_err(storage_error)?
                && catalog_field_u16(&write.key, current.value(), 2, 1)? == 7
            {
                let Some(schema_id) =
                    catalog_optional_field_bytes(&write.key, current.value(), 2, 2)?
                else {
                    continue;
                };
                let name = catalog_field_bytes(&write.key, current.value(), 2, 3)?;
                let mut index_key = schema_id;
                index_key.extend_from_slice(&name);
                index_names
                    .remove(index_key.as_slice())
                    .map_err(storage_error)?;
            }
        }
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&schema_prefix))
        {
            if let Some(payload) = &write.payload
                && catalog_field_u16(&write.key, payload, 2, 1)? == 7
            {
                let Some(schema_id) = catalog_optional_field_bytes(&write.key, payload, 2, 2)?
                else {
                    continue;
                };
                let name = catalog_field_bytes(&write.key, payload, 2, 3)?;
                if write.key.len() != 19 || write.key[2] != 1 || name.is_empty() {
                    return Err(CommitError::InvalidCatalog);
                }
                let index_id = write.key.get(3..19).ok_or(CommitError::InvalidCatalog)?;
                let mut index_key = schema_id;
                index_key.extend_from_slice(&name);
                if index_names
                    .insert(index_key.as_slice(), index_id)
                    .map_err(storage_error)?
                    .is_some()
                {
                    return Err(CommitError::InvalidCatalog);
                }
            }
        }
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&schema_prefix))
        {
            if let Some(current) = table.get(write.key.as_slice()).map_err(storage_error)? {
                if write.key.len() != 19 || write.key[2] != 1 {
                    return Err(CommitError::InvalidCatalog);
                }
                let mut owner_key = catalog_field_bytes(&write.key, current.value(), 2, 4)?;
                owner_key.extend_from_slice(&write.key[3..19]);
                object_owners
                    .remove(owner_key.as_slice())
                    .map_err(storage_error)?;
            }
        }
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&schema_prefix))
        {
            if let Some(payload) = &write.payload {
                if write.key.len() != 19 || write.key[2] != 1 {
                    return Err(CommitError::InvalidCatalog);
                }
                let mut owner_key = catalog_field_bytes(&write.key, payload, 2, 4)?;
                owner_key.extend_from_slice(&write.key[3..19]);
                if object_owners
                    .insert(owner_key.as_slice(), &b""[..])
                    .map_err(storage_error)?
                    .is_some()
                {
                    return Err(CommitError::InvalidCatalog);
                }
            }
        }
        let index_definition_prefix = 9u16.to_be_bytes();
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&index_definition_prefix))
        {
            if let Some(current) = table.get(write.key.as_slice()).map_err(storage_error)? {
                if write.key.len() != 19 || write.key[2] != 1 {
                    return Err(CommitError::InvalidCatalog);
                }
                let mut index_key = catalog_field_bytes(&write.key, current.value(), 9, 1)?;
                index_key.extend_from_slice(&write.key[3..19]);
                table_indexes
                    .remove(index_key.as_slice())
                    .map_err(storage_error)?;
            }
        }
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&index_definition_prefix))
        {
            if let Some(payload) = &write.payload {
                if write.key.len() != 19 || write.key[2] != 1 {
                    return Err(CommitError::InvalidCatalog);
                }
                let mut index_key = catalog_field_bytes(&write.key, payload, 9, 1)?;
                index_key.extend_from_slice(&write.key[3..19]);
                if table_indexes
                    .insert(index_key.as_slice(), &b""[..])
                    .map_err(storage_error)?
                    .is_some()
                {
                    return Err(CommitError::InvalidCatalog);
                }
            }
        }
        let constraint_prefix = 8u16.to_be_bytes();
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&constraint_prefix))
        {
            if let Some(current) = table.get(write.key.as_slice()).map_err(storage_error)? {
                if write.key.len() != 19 || write.key[2] != 1 {
                    return Err(CommitError::InvalidCatalog);
                }
                let mut index_key = catalog_field_bytes(&write.key, current.value(), 8, 1)?;
                index_key.extend_from_slice(&write.key[3..19]);
                table_constraints
                    .remove(index_key.as_slice())
                    .map_err(storage_error)?;
            }
        }
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&constraint_prefix))
        {
            if let Some(payload) = &write.payload {
                if write.key.len() != 19 || write.key[2] != 1 {
                    return Err(CommitError::InvalidCatalog);
                }
                let mut index_key = catalog_field_bytes(&write.key, payload, 8, 1)?;
                index_key.extend_from_slice(&write.key[3..19]);
                if table_constraints
                    .insert(index_key.as_slice(), &b""[..])
                    .map_err(storage_error)?
                    .is_some()
                {
                    return Err(CommitError::InvalidCatalog);
                }
            }
        }
        let mut relation_ids = HashSet::<[u8; 16]>::new();
        for write in catalog_writes {
            if write.key.starts_with(&5u16.to_be_bytes()) && write.key.len() == 19 {
                relation_ids.insert(write.key[3..19].try_into().unwrap());
            } else if write.key.starts_with(&schema_prefix) && write.key.len() == 19 {
                let existing_is_relation = table
                    .get(write.key.as_slice())
                    .map_err(storage_error)?
                    .is_some_and(|value| {
                        catalog_field_u16(&write.key, value.value(), 2, 1).ok() == Some(3)
                    });
                let replacement_is_relation = write.payload.as_deref().is_some_and(|payload| {
                    catalog_field_u16(&write.key, payload, 2, 1).ok() == Some(3)
                });
                if existing_is_relation || replacement_is_relation {
                    relation_ids.insert(write.key[3..19].try_into().unwrap());
                }
            }
        }
        for relation_id in &relation_ids {
            let table_key = catalog_id_key(5, *relation_id);
            let object_key = catalog_id_key(2, *relation_id);
            let Some(table_record) = table.get(table_key.as_slice()).map_err(storage_error)? else {
                continue;
            };
            let Some(object_record) = table.get(object_key.as_slice()).map_err(storage_error)?
            else {
                continue;
            };
            if catalog_field_u16(&object_key, object_record.value(), 2, 1)? != 3 {
                continue;
            }
            let schema_id = catalog_field_bytes(&table_key, table_record.value(), 5, 1)?;
            let name = catalog_field_bytes(&object_key, object_record.value(), 2, 3)?;
            let mut index_key = schema_id;
            index_key.extend_from_slice(&name);
            relation_names
                .remove(index_key.as_slice())
                .map_err(storage_error)?;
        }
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&11u16.to_be_bytes()))
        {
            if write.key.len() != 36 || write.key[2] != 1 || write.key[19] != 1 {
                return Err(CommitError::InvalidCatalog);
            }
            let member_id = &write.key[3..19];
            let role_id = &write.key[20..36];
            for (direction, first, second) in [(1u8, member_id, role_id), (2u8, role_id, member_id)]
            {
                let mut index_key = Vec::with_capacity(33);
                index_key.push(direction);
                index_key.extend_from_slice(first);
                index_key.extend_from_slice(second);
                memberships
                    .remove(index_key.as_slice())
                    .map_err(storage_error)?;
            }
        }
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&12u16.to_be_bytes()))
        {
            if write.key.len() != 59
                || write.key[2] != 1
                || write.key[19] != 1
                || write.key[36] != 1
                || write.key[39] != 1
                || write.key[42] != 1
            {
                return Err(CommitError::InvalidCatalog);
            }
            for (direction, indexed_id) in [(1u8, &write.key[3..19]), (2u8, &write.key[20..36])] {
                let mut index_key = Vec::with_capacity(76);
                index_key.push(direction);
                index_key.extend_from_slice(indexed_id);
                index_key.extend_from_slice(&write.key);
                grants.remove(index_key.as_slice()).map_err(storage_error)?;
            }
        }
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&13u16.to_be_bytes()))
        {
            if write.key.len() != 19 || write.key[2] != 1 {
                return Err(CommitError::InvalidCatalog);
            }
            if let Some(current) = table.get(write.key.as_slice()).map_err(storage_error)? {
                let table_id = catalog_field_bytes(&write.key, current.value(), 13, 1)?;
                let operation = catalog_field_u16(&write.key, current.value(), 13, 3)?;
                if !matches!(operation, 1..=4) {
                    return Err(CommitError::InvalidCatalog);
                }
                let mut index_key = table_id;
                index_key.extend_from_slice(&operation.to_be_bytes());
                index_key.extend_from_slice(&write.key[3..19]);
                policies
                    .remove(index_key.as_slice())
                    .map_err(storage_error)?;
            }
        }
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&15u16.to_be_bytes()))
        {
            if write.key.len() != 39
                || write.key[2] != 1
                || write.key[19] != 1
                || write.key[36] != 1
                || !matches!(u16::from_be_bytes([write.key[37], write.key[38]]), 1..=5)
            {
                return Err(CommitError::InvalidCatalog);
            }
            let dependent_id = &write.key[3..19];
            let referenced_id = &write.key[20..36];
            if dependent_id.iter().all(|byte| *byte == 0)
                || referenced_id.iter().all(|byte| *byte == 0)
            {
                return Err(CommitError::InvalidCatalog);
            }
            for (direction, first, second) in [
                (1u8, dependent_id, referenced_id),
                (2u8, referenced_id, dependent_id),
            ] {
                let mut index_key = Vec::with_capacity(35);
                index_key.push(direction);
                index_key.extend_from_slice(first);
                index_key.extend_from_slice(second);
                index_key.extend_from_slice(&write.key[37..39]);
                dependencies
                    .remove(index_key.as_slice())
                    .map_err(storage_error)?;
            }
        }
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&6u16.to_be_bytes()))
        {
            if write.key.len() != 19 || write.key[2] != 1 {
                return Err(CommitError::InvalidCatalog);
            }
            if let Some(current) = table.get(write.key.as_slice()).map_err(storage_error)? {
                let table_id = catalog_field_bytes(&write.key, current.value(), 6, 1)?;
                let ordinal = catalog_field_u32(&write.key, current.value(), 6, 2)?;
                let name = catalog_field_bytes(&write.key, current.value(), 6, 3)?;
                let mut by_name = vec![1];
                by_name.extend_from_slice(&table_id);
                by_name.extend_from_slice(&name);
                let mut by_ordinal = vec![2];
                by_ordinal.extend_from_slice(&table_id);
                by_ordinal.extend_from_slice(&ordinal.to_be_bytes());
                column_lookups
                    .remove(by_name.as_slice())
                    .map_err(storage_error)?;
                column_lookups
                    .remove(by_ordinal.as_slice())
                    .map_err(storage_error)?;
            }
        }
        for write in catalog_writes {
            match &write.payload {
                Some(payload) => {
                    table
                        .insert(&write.key[..], &payload[..])
                        .map_err(storage_error)?;
                }
                None => {
                    table.remove(&write.key[..]).map_err(storage_error)?;
                }
            }
        }
        for relation_id in relation_ids {
            let table_key = catalog_id_key(5, relation_id);
            let object_key = catalog_id_key(2, relation_id);
            let Some(table_record) = table.get(table_key.as_slice()).map_err(storage_error)? else {
                continue;
            };
            let Some(object_record) = table.get(object_key.as_slice()).map_err(storage_error)?
            else {
                continue;
            };
            if catalog_field_u16(&object_key, object_record.value(), 2, 1)? != 3 {
                continue;
            }
            let schema_id = catalog_field_bytes(&table_key, table_record.value(), 5, 1)?;
            let name = catalog_field_bytes(&object_key, object_record.value(), 2, 3)?;
            let mut index_key = schema_id;
            index_key.extend_from_slice(&name);
            if relation_names
                .insert(index_key.as_slice(), &relation_id[..])
                .map_err(storage_error)?
                .is_some()
            {
                return Err(CommitError::InvalidCatalog);
            }
        }
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&11u16.to_be_bytes()))
        {
            if write.payload.is_none() {
                continue;
            }
            let member_id = &write.key[3..19];
            let role_id = &write.key[20..36];
            for (direction, first, second) in [(1u8, member_id, role_id), (2u8, role_id, member_id)]
            {
                let mut index_key = Vec::with_capacity(33);
                index_key.push(direction);
                index_key.extend_from_slice(first);
                index_key.extend_from_slice(second);
                if memberships
                    .insert(index_key.as_slice(), &b""[..])
                    .map_err(storage_error)?
                    .is_some()
                {
                    return Err(CommitError::InvalidCatalog);
                }
            }
        }
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&12u16.to_be_bytes()))
        {
            if write.payload.is_none() {
                continue;
            }
            for (direction, indexed_id) in [(1u8, &write.key[3..19]), (2u8, &write.key[20..36])] {
                let mut index_key = Vec::with_capacity(76);
                index_key.push(direction);
                index_key.extend_from_slice(indexed_id);
                index_key.extend_from_slice(&write.key);
                if grants
                    .insert(index_key.as_slice(), &b""[..])
                    .map_err(storage_error)?
                    .is_some()
                {
                    return Err(CommitError::InvalidCatalog);
                }
            }
        }
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&13u16.to_be_bytes()))
        {
            let Some(payload) = &write.payload else {
                continue;
            };
            let table_id = catalog_field_bytes(&write.key, payload, 13, 1)?;
            let operation = catalog_field_u16(&write.key, payload, 13, 3)?;
            if !matches!(operation, 1..=4) {
                return Err(CommitError::InvalidCatalog);
            }
            let mut index_key = table_id;
            index_key.extend_from_slice(&operation.to_be_bytes());
            index_key.extend_from_slice(&write.key[3..19]);
            if policies
                .insert(index_key.as_slice(), &b""[..])
                .map_err(storage_error)?
                .is_some()
            {
                return Err(CommitError::InvalidCatalog);
            }
        }
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&15u16.to_be_bytes()))
        {
            if write.payload.is_none() {
                continue;
            }
            let dependent_id = &write.key[3..19];
            let referenced_id = &write.key[20..36];
            for (direction, first, second) in [
                (1u8, dependent_id, referenced_id),
                (2u8, referenced_id, dependent_id),
            ] {
                let mut index_key = Vec::with_capacity(35);
                index_key.push(direction);
                index_key.extend_from_slice(first);
                index_key.extend_from_slice(second);
                index_key.extend_from_slice(&write.key[37..39]);
                if dependencies
                    .insert(index_key.as_slice(), &b""[..])
                    .map_err(storage_error)?
                    .is_some()
                {
                    return Err(CommitError::InvalidCatalog);
                }
            }
        }
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&6u16.to_be_bytes()))
        {
            let Some(payload) = &write.payload else {
                continue;
            };
            if write.key.len() != 19 || write.key[2] != 1 {
                return Err(CommitError::InvalidCatalog);
            }
            let table_id = catalog_field_bytes(&write.key, payload, 6, 1)?;
            let ordinal = catalog_field_u32(&write.key, payload, 6, 2)?;
            let name = catalog_field_bytes(&write.key, payload, 6, 3)?;
            if table_id.iter().all(|byte| *byte == 0) || name.is_empty() || name.len() > 4096 {
                return Err(CommitError::InvalidCatalog);
            }
            let column_id = &write.key[3..19];
            let mut by_name = vec![1];
            by_name.extend_from_slice(&table_id);
            by_name.extend_from_slice(&name);
            let mut by_ordinal = vec![2];
            by_ordinal.extend_from_slice(&table_id);
            by_ordinal.extend_from_slice(&ordinal.to_be_bytes());
            for key in [by_name, by_ordinal] {
                if column_lookups
                    .insert(key.as_slice(), column_id)
                    .map_err(storage_error)?
                    .is_some()
                {
                    return Err(CommitError::InvalidCatalog);
                }
            }
        }
        for entry in index_names.iter().map_err(storage_error)? {
            let (name_key, _) = entry.map_err(storage_error)?;
            if relation_names
                .get(name_key.value())
                .map_err(storage_error)?
                .is_some()
            {
                return Err(CommitError::InvalidCatalog);
            }
        }
    }
    {
        let mut table = transaction.open_table(AUDIT).map_err(storage_error)?;
        for (event_id, payload) in &encoded_audit {
            table
                .insert(&event_id[..], &payload[..])
                .map_err(storage_error)?;
        }
    }
    if !dropped_table_ids.is_empty() {
        let mut table = transaction.open_table(STATS).map_err(storage_error)?;
        for table_id in &dropped_table_ids {
            let mut key = Vec::with_capacity(
                crate::statistics::COMMITTED_POPULATION_KEY_PREFIX.len() + table_id.len(),
            );
            key.extend_from_slice(crate::statistics::COMMITTED_POPULATION_KEY_PREFIX);
            key.extend_from_slice(table_id);
            table.remove(key.as_slice()).map_err(storage_error)?;
        }
    }

    let mut next_metadata = current_metadata;
    next_metadata.commit_sequence = next_sequence;
    let encoded_metadata = next_metadata
        .encode()
        .map_err(|_| CommitError::InvalidMetadata)?;
    {
        let mut table = transaction.open_table(ENGINE_META).map_err(storage_error)?;
        table
            .insert(&b"format"[..], &encoded_metadata[..])
            .map_err(storage_error)?;
    }
    {
        let mut table = transaction.open_table(OUTCOMES).map_err(storage_error)?;
        table
            .insert(&invocation_id[..], &outcome[..])
            .map_err(storage_error)?;
    }
    if !before_durable_commit() {
        transaction.abort().map_err(storage_error)?;
        return Err(CommitError::InvocationCancelled);
    }
    if let Err(error) = transaction.commit() {
        return Err(storage_error(error));
    }
    Ok(next_sequence)
}

pub(crate) fn record_aborted_outcome(
    database: &Database,
    invocation_id: [u8; 16],
    principal_id: [u8; 16],
    retention_deadline_us: i64,
) -> Result<(), CommitError> {
    let result = record_aborted_outcome_unchecked(
        database,
        invocation_id,
        principal_id,
        retention_deadline_us,
    );
    if matches!(&result, Err(CommitError::Storage(_))) {
        crate::storage_manager::mark_database_uncertain(database);
    }
    result
}

pub(super) fn record_aborted_outcome_unchecked(
    database: &Database,
    invocation_id: [u8; 16],
    principal_id: [u8; 16],
    retention_deadline_us: i64,
) -> Result<(), CommitError> {
    let outcome = InvocationOutcome {
        status: OutcomeStatus::Aborted,
        invocation_id,
        principal_id,
        commit_sequence: 0,
        retention_deadline_us,
    }
    .encode()
    .map_err(|_| CommitError::InvalidWrite)?;
    let mut transaction = database.begin_write().map_err(storage_error)?;
    transaction
        .set_durability(redb::Durability::Immediate)
        .map_err(storage_error)?;
    transaction.set_two_phase_commit(true);
    {
        let table = transaction.open_table(ENGINE_META).map_err(storage_error)?;
        let metadata = table
            .get(&b"format"[..])
            .map_err(storage_error)?
            .ok_or(CommitError::InvalidMetadata)?;
        let metadata =
            EngineMetadata::decode(metadata.value()).map_err(|_| CommitError::InvalidMetadata)?;
        if !metadata.supports_current_formats() {
            return Err(CommitError::UnsupportedFormat);
        }
        if metadata.lifecycle != Lifecycle::Ready {
            return Err(CommitError::NotReady);
        }
    }
    {
        let table = transaction.open_table(OUTCOMES).map_err(storage_error)?;
        if table
            .get(&invocation_id[..])
            .map_err(storage_error)?
            .is_some()
        {
            return Err(CommitError::OutcomeAlreadyExists);
        }
    }
    {
        let mut table = transaction.open_table(OUTCOMES).map_err(storage_error)?;
        table
            .insert(&invocation_id[..], &outcome[..])
            .map_err(storage_error)?;
    }
    match transaction.commit() {
        Ok(()) => Ok(()),
        Err(error) => Err(storage_error(error)),
    }
}
