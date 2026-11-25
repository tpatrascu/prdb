//! One invocation's pinned snapshot and private row overlay.

mod admin_discovery;
mod catalog_loader;
mod discovery;
mod estimates;
mod executor_session;
mod information_schema;
#[cfg(test)]
mod information_schema_column_type_tests;
#[cfg(test)]
mod planner_estimate_budget_tests;
#[cfg(test)]
mod planner_population_estimate_cache_tests;
mod planner_session;
#[cfg(test)]
mod predicate_estimate_scan_budget_tests;
#[cfg(test)]
mod requested_scope_tests;
mod scans_and_indexes;
mod staging;
mod types;

pub use executor_session::ExecutorMetadataConnection;
pub(crate) use executor_session::ExecutorMetadataSessionState;
pub use planner_session::PlannerMetadataConnection;
pub(crate) use planner_session::PlannerMetadataSessionState;
pub use types::*;

pub(super) const MAX_INDEX_CHANGES_PER_ROW: usize = 256;
pub const MAX_INVOCATION_SCAN_WORK: usize = 65_536;
/// Maximum number of estimate entries encoded in one planner IPC request.
pub const MAX_PLANNER_ESTIMATE_BATCH_ENTRIES: usize = 16;
/// Keep the original name as an alias for callers that use the per-request
/// envelope; this is distinct from the cumulative invocation allowance.
pub const MAX_PLANNER_ESTIMATE_REQUESTS_PER_CONNECTION: usize = MAX_PLANNER_ESTIMATE_BATCH_ENTRIES;
pub const DEFAULT_PLANNER_ESTIMATE_REQUESTS: usize = 16;
pub const MAX_PLANNER_ESTIMATE_REQUESTS: usize = 4_096;
pub(super) const INFORMATION_SCHEMA_ID: [u8; 16] =
    crate::metadata_discovery::INFORMATION_SCHEMA_SCHEMA_ID;
pub(super) const INFORMATION_SCHEMA_COLUMN_EXTRA_BYTES: usize =
    "TIMESTAMP WITH TIME ZONE".len() + "YES".len() + 4 + 3 * 5 + 16;

use std::cell::Cell;

use crate::authorization::{
    AccessTarget, AuthorityDependency, AuthorizationContext, AuthorizationError, Grant, Operation,
};
use crate::catalog::decode_registered_envelope;
use crate::commit::{
    CatalogGuard, CommitError, CommitRequest, RowWrite, commit_storage_changes_with_gate,
    record_aborted_outcome,
};
use crate::credentials::{TOKEN_SECRET_BYTES, decode_verifier_record};
use crate::ddl_fence::DdlFenceCoordinator;
use crate::metadata::{EngineMetadata, Lifecycle};
use crate::metadata_discovery::{
    AuthorizedPlanRelation, AuthorizedRangeAccessPath, AuthorizedRelationDescription,
    AuthorizedUniqueAccessPath,
};
use crate::outcome::{InvocationOutcome, OutcomeStatus};
use crate::overlay::{Overlay, RowId};
use crate::row_record::RowRecord;
use crate::schema::{ENGINE_META, OUTCOMES};
use crate::snapshot::{RowSnapshot, SnapshotError};
use catalog_loader::*;
use discovery::*;
use estimates::*;
use information_schema::*;
use redb::{Database, ReadableDatabase};
use scans_and_indexes::*;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

impl<'db> InvocationTransaction<'db> {
    /// Principal identities that authored the transaction's final staged rows.
    /// Interactive commit uses this set to revalidate only actors whose
    /// writes will be published.
    pub(crate) fn staged_write_actor_ids(&self) -> Result<HashSet<[u8; 16]>, TransactionError> {
        Ok(self.overlay.staged_actor_ids()?)
    }

    pub(crate) fn restrict_to_requested_scope(
        &mut self,
        requested_scope: HashSet<(Operation, AccessTarget)>,
    ) {
        self.requested_scope = Some(requested_scope);
    }

    pub(crate) fn set_package_requested_scope(
        &mut self,
        requested_scope: Option<HashSet<(Operation, AccessTarget)>>,
    ) {
        self.requested_scope = requested_scope;
    }

    /// Switch the effective actor for one interactive package while keeping
    /// the transaction controller separately pinned for commit ownership.
    pub(crate) fn set_package_principal(&mut self, principal_id: Option<[u8; 16]>) {
        self.principal_id = principal_id.unwrap_or(self.controller_principal_id);
    }

    pub(super) fn scope_from_grants(&self, grants: &[Grant]) -> HashSet<(Operation, AccessTarget)> {
        grants
            .iter()
            .map(|grant| (grant.operation, grant.target))
            .filter(|capability| {
                self.requested_scope
                    .as_ref()
                    .is_none_or(|requested| requested.contains(capability))
            })
            .collect()
    }

    /// Begin an uncoordinated invocation for trusted low-level use. Runtime
    /// callers should use `StorageManager::begin_invocation` so commits share
    /// table fences with DDL.
    pub fn begin(
        database: &'db Database,
        invocation_id: [u8; 16],
        principal_id: [u8; 16],
        retention_deadline_us: i64,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Self, TransactionError> {
        Self::begin_inner(
            database,
            InvocationCoordination::uncoordinated(),
            invocation_id,
            principal_id,
            retention_deadline_us,
            max_rows,
            max_bytes,
        )
    }

    /// Begin an invocation that acquires shared table fences for its staged
    /// write set while publishing the final commit.
    pub fn begin_with_fences(
        database: &'db Database,
        ddl_fences: DdlFenceCoordinator,
        invocation_id: [u8; 16],
        principal_id: [u8; 16],
        retention_deadline_us: i64,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Self, TransactionError> {
        Self::begin_inner(
            database,
            InvocationCoordination::fenced(ddl_fences),
            invocation_id,
            principal_id,
            retention_deadline_us,
            max_rows,
            max_bytes,
        )
    }

    pub(crate) fn begin_inner(
        database: &'db Database,
        coordination: InvocationCoordination,
        invocation_id: [u8; 16],
        principal_id: [u8; 16],
        retention_deadline_us: i64,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Self, TransactionError> {
        let transaction = database
            .begin_read()
            .map_err(|error| TransactionError::Storage(error.to_string()))?;
        coordination.update_snapshot_pin_count(1);
        let metadata_bytes = transaction
            .open_table(ENGINE_META)
            .map_err(|error| TransactionError::Storage(error.to_string()))?
            .get(&b"format"[..])
            .map_err(|error| TransactionError::Storage(error.to_string()))?
            .ok_or(TransactionError::NotReady)?
            .value()
            .to_vec();
        let metadata = EngineMetadata::decode(&metadata_bytes)
            .map_err(|error| TransactionError::Storage(error.to_string()))?;
        if !metadata.supports_current_formats() {
            return Err(TransactionError::UnsupportedFormat);
        }
        if metadata.lifecycle != Lifecycle::Ready {
            return Err(TransactionError::NotReady);
        }
        InvocationOutcome {
            status: OutcomeStatus::Aborted,
            invocation_id,
            principal_id,
            commit_sequence: 0,
            retention_deadline_us,
        }
        .encode()
        .map_err(|_| TransactionError::InvalidContext)?;
        if transaction
            .open_table(OUTCOMES)
            .map_err(|error| TransactionError::Storage(error.to_string()))?
            .get(&invocation_id[..])
            .map_err(|error| TransactionError::Storage(error.to_string()))?
            .is_some()
        {
            return Err(TransactionError::DuplicateInvocation);
        }
        Ok(Self {
            database,
            snapshot: Some(RowSnapshot::new(transaction)),
            overlay: Overlay::new(max_rows, max_bytes),
            base_versions: HashMap::new(),
            index_writes: HashMap::new(),
            loaded_schemas: HashMap::new(),
            loaded_indexes: HashMap::new(),
            loaded_foreign_keys: HashMap::new(),
            catalog_guards: HashMap::new(),
            max_index_changes: max_rows.saturating_mul(MAX_INDEX_CHANGES_PER_ROW),
            max_index_bytes: max_bytes,
            index_bytes: 0,
            invocation_id,
            principal_id,
            controller_principal_id: principal_id,
            requested_scope: None,
            retention_deadline_us,
            max_scan_work: coordination.max_scan_work,
            scanned_work: 0,
            max_authorized_index_range_candidates: coordination
                .authorized_index_range_budget
                .candidates,
            max_authorized_index_range_bytes: coordination.authorized_index_range_budget.bytes,
            coordination,
            aborted: false,
            cancellation_triggered: false,
            outcome_recorded: false,
            active_scan: None,
            active_relation_scan: None,
            active_schema_scan: None,
            active_column_scan: None,
            active_builtin_relation_scan: None,
            active_builtin_column_scan: None,
            active_information_schema_tables_scan: None,
            active_information_schema_columns_scan: None,
        })
    }

    /// Open an executor metadata session whose relation handles are private to
    /// that connection and expire when the returned value is dropped.
    pub fn open_executor_metadata_connection(&mut self) -> ExecutorMetadataConnection<'_, 'db> {
        self.open_executor_metadata_connection_with_state(ExecutorMetadataSessionState::default())
    }

    pub(crate) fn open_executor_metadata_connection_with_state(
        &mut self,
        state: ExecutorMetadataSessionState,
    ) -> ExecutorMetadataConnection<'_, 'db> {
        ExecutorMetadataConnection {
            transaction: self,
            handles: state.handles,
            used_tokens: state.used_tokens,
            scans: state.scans,
            used_scan_tokens: state.used_scan_tokens,
            row_handles: state.row_handles,
            used_row_tokens: state.used_row_tokens,
            detached: false,
        }
    }

    /// Open a planner session with planning handles isolated from executor
    /// relation and row handles.
    pub fn open_planner_metadata_connection(&mut self) -> PlannerMetadataConnection<'_, 'db> {
        self.open_planner_metadata_connection_with_state(PlannerMetadataSessionState::default())
    }

    pub(crate) fn open_planner_metadata_connection_with_state(
        &mut self,
        state: PlannerMetadataSessionState,
    ) -> PlannerMetadataConnection<'_, 'db> {
        PlannerMetadataConnection {
            transaction: self,
            handles: state.handles,
            used_tokens: state.used_tokens,
            estimate_requests: state.estimate_requests,
            predicate_estimates: state.predicate_estimates,
            population_estimates: state.population_estimates,
            join_estimates: state.join_estimates,
            summary_estimates: state.summary_estimates,
        }
    }

    /// Check whether the managed invocation is still active and within its age
    /// limit. Runtime services should poll this while an executor is idle.
    pub fn check_deadline(&mut self) -> Result<(), TransactionError> {
        if self.aborted {
            return Err(TransactionError::Aborted);
        }
        if self.coordination.is_expired() {
            self.abort()?;
            return Err(TransactionError::Expired);
        }
        if !self.coordination.may_continue() {
            self.cancellation_triggered = true;
            self.abort()?;
            return Err(TransactionError::CancellationRequested);
        }
        Ok(())
    }

    /// Check hard age limits without consuming a pending cooperative
    /// cancellation. The supervisor uses this while waiting for the worker's
    /// next cancellation poll or grace-period expiry.
    pub(crate) fn check_worker_deadline(&mut self) -> Result<(), TransactionError> {
        if self.coordination.is_expired() {
            if !self.aborted {
                self.abort()?;
            }
            return Err(TransactionError::Expired);
        }
        if self.aborted {
            // The worker may abort its transaction while handling a query
            // error (for example, a denied write). That is a normal terminal
            // path; the worker-channel handler will report the cause and
            // close its peers. The supervisor's monotonic deadline still
            // bounds a worker that fails to exit after the abort.
            return Ok(());
        }
        if self.coordination.cancellation_requested() {
            return Ok(());
        }
        Ok(())
    }

    pub(crate) fn cancellation_requested(&self) -> bool {
        self.cancellation_triggered || self.coordination.cancellation_requested()
    }

    pub(super) fn pinned_snapshot(&mut self) -> Result<&RowSnapshot, TransactionError> {
        if self.coordination.is_expired() {
            self.abort()?;
            return Err(TransactionError::Expired);
        }
        if !self.coordination.may_continue() {
            self.abort()?;
            return Err(TransactionError::CancellationRequested);
        }
        self.snapshot.as_ref().ok_or(TransactionError::Aborted)
    }

    pub(super) fn charge_scan_work(&mut self, amount: usize) -> Result<(), TransactionError> {
        self.scanned_work = self
            .scanned_work
            .checked_add(amount)
            .filter(|work| *work <= self.max_scan_work)
            .ok_or(TransactionError::ScanBudgetExceeded)?;
        Ok(())
    }

    /// Verify one database-issued bearer token against this invocation's
    /// pinned catalog snapshot, then return the pinned role closure. The token
    /// record and every identity/membership record used for admission remain
    /// guarded through commit, so revocation or an authority change conflicts.
    pub fn authenticate_credential_token(
        &mut self,
        token_id: [u8; 16],
        secret: &[u8; TOKEN_SECRET_BYTES],
        now_us: i64,
    ) -> Result<HashSet<[u8; 16]>, TransactionError> {
        let result = (|| {
            if token_id == [0; 16] {
                return Err(TransactionError::AuthenticationFailed);
            }
            let record = match self.read_catalog_record(&catalog_id_key(16, token_id)) {
                Ok(Some(record)) => record,
                Ok(None) => return Err(TransactionError::AuthenticationFailed),
                Err(TransactionError::CorruptCatalog) => {
                    return Err(TransactionError::AuthenticationFailed);
                }
                Err(error) => return Err(error),
            };
            let credential = decode_verifier_record(&record)
                .map_err(|_| TransactionError::AuthenticationFailed)?;
            if credential.user_id != self.principal_id
                || !credential.is_valid_at(now_us)
                || !credential.matches_secret(secret)
            {
                return Err(TransactionError::AuthenticationFailed);
            }
            match self.load_catalog_role_closure(self.principal_id) {
                Ok(roles) => Ok(roles),
                Err(TransactionError::Authorization(AuthorizationError::Denied)) => {
                    Err(TransactionError::AuthenticationFailed)
                }
                Err(error) => Err(error),
            }
        })();
        match result {
            Ok(roles) => Ok(roles),
            Err(error) => {
                if !self.aborted {
                    self.discard_unstarted_admission();
                }
                Err(error)
            }
        }
    }

    /// Reject an admission attempt before execution without publishing an
    /// ABORTED outcome. A client that failed START has no transaction result.
    pub(crate) fn discard_unstarted_admission(&mut self) {
        self.overlay.abort();
        self.coordination.update_write_set_usage(0, 0);
        self.base_versions.clear();
        self.index_writes.clear();
        self.catalog_guards.clear();
        self.loaded_schemas.clear();
        self.loaded_indexes.clear();
        self.loaded_foreign_keys.clear();
        self.snapshot.take();
        self.coordination.update_snapshot_pin_count(0);
        self.aborted = true;
        self.outcome_recorded = true;
        self.coordination._admission_permit.take();
    }

    /// Roll back all staged work and make this invocation terminal.
    pub fn abort(&mut self) -> Result<(), TransactionError> {
        self.overlay.abort();
        self.coordination.update_write_set_usage(0, 0);
        self.base_versions.clear();
        self.index_writes.clear();
        self.loaded_schemas.clear();
        self.loaded_indexes.clear();
        self.loaded_foreign_keys.clear();
        self.index_bytes = 0;
        self.catalog_guards.clear();
        self.active_scan = None;
        self.active_relation_scan = None;
        self.active_schema_scan = None;
        self.active_column_scan = None;
        self.active_builtin_relation_scan = None;
        self.active_builtin_column_scan = None;
        self.active_information_schema_tables_scan = None;
        self.active_information_schema_columns_scan = None;
        self.aborted = true;
        self.snapshot.take();
        self.coordination.update_snapshot_pin_count(0);
        if !self.outcome_recorded {
            record_aborted_outcome(
                self.database,
                self.invocation_id,
                self.principal_id,
                self.retention_deadline_us,
            )?;
            self.outcome_recorded = true;
        }
        self.coordination._admission_permit.take();
        Ok(())
    }

    /// Commit rows, engine sequence, and durable COMMITTED outcome in one redb write transaction.
    pub fn commit(self) -> Result<u64, TransactionError> {
        let started_at = Instant::now();
        let metrics = self.coordination.admission_metrics.clone();
        let result = self.commit_inner();
        if let Some(metrics) = metrics {
            metrics.record_commit_latency(started_at.elapsed());
        }
        result
    }

    pub(super) fn commit_inner(mut self) -> Result<u64, TransactionError> {
        if self.aborted {
            if !self.outcome_recorded {
                record_aborted_outcome(
                    self.database,
                    self.invocation_id,
                    self.principal_id,
                    self.retention_deadline_us,
                )?;
            }
            return Err(TransactionError::Aborted);
        }
        if self.active_scan.is_some() {
            self.abort()?;
            return Err(TransactionError::ScanStillActive);
        }
        self.check_deadline()?;
        let ddl_fences = self.coordination.ddl_fences.clone();
        let write_lock_wait_metrics = self.coordination.admission_metrics.clone();
        let entries = self.overlay.into_entries()?;
        let mut writes = Vec::with_capacity(entries.len());
        for (id, payload) in entries {
            let expected_version = self.base_versions.get(&id).copied().unwrap_or(None);
            writes.push(RowWrite {
                key: row_key(id),
                expected_version,
                payload,
            });
        }
        let (index_validation, index_writes) = match validate_catalog_index_deltas(
            self.snapshot.as_ref().ok_or(TransactionError::Aborted)?,
            &self.loaded_schemas,
            &self.loaded_indexes,
            &self.index_writes,
            &writes,
            self.max_index_changes,
            self.max_index_bytes,
        ) {
            Ok(index_writes) => (Ok(()), index_writes),
            Err(error) => (Err(error), Vec::new()),
        };
        let catalog_guards = self
            .catalog_guards
            .into_iter()
            .filter(|(key, _)| {
                // A rename changes the family-2 display-name object and the
                // family-5 table epoch while preserving the pinned schema.
                // Those guards must not prevent a read-only outcome from
                // completing; other catalog guards still revalidate.
                !writes.is_empty()
                    || !index_writes.is_empty()
                    || key
                        .get(..2)
                        .map(|prefix| u16::from_be_bytes([prefix[0], prefix[1]]))
                        .is_some_and(|family| !matches!(family, 2 | 5))
            })
            .map(|(key, expected_value)| CatalogGuard {
                key,
                expected_value,
            })
            .collect::<Vec<_>>();
        let foreign_keys = self.loaded_foreign_keys.into_values().collect::<Vec<_>>();
        let table_ids = writes
            .iter()
            .map(|write| {
                write.key[..16]
                    .try_into()
                    .expect("row keys have fixed table ID width")
            })
            .collect::<Vec<[u8; 16]>>();
        let commit_started = Cell::new(false);
        let result = index_validation.and_then(|()| {
            if self.coordination.is_expired() {
                return Err(CommitError::InvocationExpired);
            }
            let _fence_guard = if let Some(ddl_fences) = &ddl_fences {
                Some(
                    ddl_fences
                        .acquire_shared(&table_ids)
                        .map_err(|_| CommitError::DdlFenceUnavailable)?,
                )
            } else {
                None
            };
            if self.coordination.is_expired() {
                return Err(CommitError::InvocationExpired);
            }
            commit_storage_changes_with_gate(
                self.database,
                CommitRequest {
                    invocation_id: self.invocation_id,
                    principal_id: self.principal_id,
                    retention_deadline_us: self.retention_deadline_us,
                    rows: &writes,
                    indexes: &index_writes,
                    foreign_keys: &foreign_keys,
                    catalog_guards: &catalog_guards,
                    catalog: &[],
                    audit: &[],
                },
                || {
                    let started = self.coordination.mark_committing();
                    if started {
                        commit_started.set(true);
                    }
                    started
                },
                move |elapsed| {
                    if let Some(metrics) = write_lock_wait_metrics {
                        metrics.record_write_acquisition(elapsed);
                    }
                },
            )
        });
        self.snapshot.take();
        self.coordination.update_snapshot_pin_count(0);
        match result {
            Ok(sequence) => Ok(sequence),
            Err(
                error @ (CommitError::Storage(_)
                | CommitError::OutcomeAlreadyExists
                | CommitError::UnsupportedFormat),
            ) => {
                if matches!(error, CommitError::Storage(_)) && commit_started.get() {
                    self.coordination.mark_commit_uncertain();
                }
                Err(error.into())
            }
            Err(error) => {
                if matches!(&error, CommitError::Conflict) {
                    self.coordination.record_conflict();
                }
                // The commit transaction has been discarded. Some validation paths
                // already recorded the abort; preserve that existing decision.
                match record_aborted_outcome(
                    self.database,
                    self.invocation_id,
                    self.principal_id,
                    self.retention_deadline_us,
                ) {
                    Ok(()) | Err(CommitError::OutcomeAlreadyExists) => Err(error.into()),
                    Err(record_error) => Err(record_error.into()),
                }
            }
        }
    }

    pub(super) fn capture_base_version(
        &mut self,
        id: RowId,
    ) -> Result<Option<u64>, TransactionError> {
        self.check_deadline()?;
        if let Some(version) = self.base_versions.get(&id) {
            return Ok(*version);
        }
        let version = self
            .pinned_snapshot()?
            .get(&row_key(id))
            .map_err(|error| TransactionError::Storage(error.to_string()))?
            .map(|bytes| RowRecord::decode(&bytes).map(|record| record.row_version))
            .transpose()
            .map_err(TransactionError::CorruptRow)?;
        self.base_versions.insert(id, version);
        Ok(version)
    }

    pub(super) fn capture_authority_dependencies(
        &mut self,
        context: &AuthorizationContext,
        dependencies: &[AuthorityDependency],
    ) -> Result<(), TransactionError> {
        if context.principal_id != self.principal_id
            || dependencies.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
            || context.role_ids.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
            || context.scope.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
            || context.role_ids.contains(&[0; 16])
        {
            return Err(TransactionError::InvalidContext);
        }
        for dependency in dependencies {
            let snapshot_value = self
                .pinned_snapshot()?
                .get_catalog_value(&dependency.key)
                .map_err(|error| TransactionError::Storage(error.to_string()))?;
            if snapshot_value != dependency.expected_value {
                return Err(TransactionError::InvalidContext);
            }
            if let Some(bytes) = &dependency.expected_value {
                let record = decode_registered_envelope(bytes)
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                if record.logical_key != dependency.key {
                    return Err(TransactionError::CorruptCatalog);
                }
            }
            if self
                .catalog_guards
                .get(&dependency.key)
                .is_some_and(|expected| expected != &dependency.expected_value)
            {
                return Err(TransactionError::InvalidContext);
            }
            self.catalog_guards
                .insert(dependency.key.clone(), dependency.expected_value.clone());
        }
        Ok(())
    }
}

pub(super) fn attach_plan_identity(
    transaction: &mut InvocationTransaction<'_>,
    schema_name: &str,
    relation: AuthorizedRelationDescription,
) -> Result<AuthorizedPlanRelation, TransactionError> {
    if schema_name == "information_schema" {
        return Ok(AuthorizedPlanRelation {
            schema_id: crate::metadata_discovery::INFORMATION_SCHEMA_SCHEMA_ID,
            schema_version: 1,
            table_epoch: 1,
            relation,
            unique_access_paths: Vec::new(),
            range_access_paths: Vec::new(),
        });
    }
    let projection = transaction.load_catalog_estimate_projection(relation.relation_id)?;
    let schema = &projection.schema;
    if schema.schema_id == [0; 16]
        || schema.schema_version == 0
        || schema.write_epoch == 0
        || schema.column_ids.len() != relation.columns.len()
        || schema.columns.len() != relation.columns.len()
        || relation
            .columns
            .iter()
            .enumerate()
            .any(|(ordinal, column)| {
                column.ordinal as usize != ordinal
                    || column.column_id != schema.column_ids[ordinal]
                    || column.scalar_type != schema.columns[ordinal].scalar_type
                    || column.nullable != schema.columns[ordinal].nullable
            })
    {
        return Err(TransactionError::CorruptCatalog);
    }
    let indexes = transaction.load_catalog_indexes(&projection.schema)?;
    let projection_view = projection.as_projection();
    let mut unique_access_paths = Vec::new();
    let mut range_access_paths = Vec::new();
    for index in indexes {
        if index.column_ids.is_empty()
            || !index.column_ids.iter().all(|column_id| {
                index_component_unconditionally_readable(
                    &projection_view,
                    relation.relation_id,
                    *column_id,
                )
            })
        {
            continue;
        }
        if index.unique {
            unique_access_paths.push(AuthorizedUniqueAccessPath {
                index_id: index.index_id,
                column_ids: index.column_ids,
            });
        } else {
            range_access_paths.push(AuthorizedRangeAccessPath {
                index_id: index.index_id,
                column_ids: index.column_ids,
            });
        }
    }
    Ok(AuthorizedPlanRelation {
        schema_id: schema.schema_id,
        schema_version: schema.schema_version,
        table_epoch: schema.write_epoch,
        relation,
        unique_access_paths,
        range_access_paths,
    })
}

pub(super) fn map_snapshot_scan_error(error: SnapshotError) -> TransactionError {
    match error {
        SnapshotError::InvalidTableId
        | SnapshotError::InvalidCursor
        | SnapshotError::InvalidScanLimit
        | SnapshotError::InvalidIndexPrefix
        | SnapshotError::InvalidIndexCursor => TransactionError::InvalidScanRequest,
        SnapshotError::InvalidRowKey
        | SnapshotError::InvalidIndexKey
        | SnapshotError::InvalidIndexLocator => TransactionError::CorruptRowKey,
        SnapshotError::InvalidCatalogRecord | SnapshotError::RecordLimitExceeded => {
            TransactionError::CorruptCatalog
        }
        SnapshotError::Storage(error) => TransactionError::Storage(error),
    }
}
