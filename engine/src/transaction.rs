//! One invocation's pinned snapshot and private row overlay.
use crate::authorization::{
    AccessTarget, AuthorityDependency, AuthorizationContext, AuthorizationError,
    AuthorizedProjection, Effect, Expr, Grant, Membership, Operation, Policy, SanitizedCell,
    Subject, authorize, check_constraint_passes, has_permission, materialize_default, project_row,
    resolve_role_closure, row_visible,
};
use crate::catalog::{CatalogRecord, decode_builtin_scalar_type, decode_registered_envelope};
use crate::commit::{
    CatalogGuard, CommitError, CommitRequest, ForeignKeyCheck, IndexWrite, RowWrite,
    commit_storage_changes, record_aborted_outcome,
};
use crate::credentials::{TOKEN_SECRET_BYTES, decode_verifier_record};
use crate::ddl_fence::DdlFenceCoordinator;
use crate::expression::{
    CatalogExpression, ExpressionPurpose, decode_catalog_expression_record,
    is_row_local_expression, validate_expression_binding,
};
use crate::index_key::{DeriveIndexKeyError, derive_index_key};
use crate::metadata::{EngineMetadata, Lifecycle};
use crate::metadata_discovery::{
    AuthorizedRelationDescription, ColumnScanCursor, MAX_COLUMNS_PER_PAGE, MAX_METADATA_PAGE_BYTES,
    MAX_RELATIONS_PER_PAGE, MAX_SCHEMAS_PER_PAGE, RelationScanCursor, SchemaScanCursor,
    VisibleColumn, VisibleColumnPage, VisibleRelation, VisibleRelationPage, VisibleRelationSchema,
    VisibleSchema, VisibleSchemaPage,
};
use crate::outcome::{InvocationOutcome, OutcomeStatus};
use crate::overlay::{Overlay, OverlayError, ResolvedRow, RowId, StagedRow};
use crate::row_record::{RowRecord, RowRecordError};
use crate::schema::{ENGINE_META, OUTCOMES};
use crate::snapshot::{MAX_CATALOG_FAMILY_SCAN, MAX_ROW_SCAN_PAGE, RowSnapshot, SnapshotError};
use crate::statistics::{
    AuthorizedScanEstimate, AuthorizedScanSummary, ColumnPopulation, MAX_ESTIMATE_CANDIDATES,
};
use crate::storage_manager::InvocationAdmissionPermit;
use crate::tuple::{
    ColumnSchema, FieldPatch, ScalarType, ScalarValue, TupleError, apply_patch, decode_tuple,
    encode_tuple,
};
use redb::{Database, ReadableDatabase};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

const MAX_INDEX_CHANGES_PER_ROW: usize = 256;

struct StagedIndexChange {
    row_id: RowId,
    expected_version: Option<u64>,
    present: bool,
    unique: bool,
    null_exempt: bool,
}

#[derive(Debug)]
pub enum TransactionError {
    InvalidContext,
    ClockUnavailable,
    NotReady,
    UnsupportedFormat,
    DuplicateInvocation,
    AuthenticationFailed,
    AdmissionLimitExceeded,
    Expired,
    CancellationRequested,
    RandomnessUnavailable,
    IndexRowNotStaged,
    RowAlreadyExists,
    GeneratedIdentityUnsupported,
    ConstraintViolation,
    IndexOverlayLimitExceeded,
    InvalidScanRequest,
    CorruptRowKey,
    ScanStillActive,
    CorruptCatalog,
    InvalidMetadataRequest,
    MetadataResultTooLarge,
    Authorization(AuthorizationError),
    Aborted,
    NotFound,
    Overlay(OverlayError),
    CorruptRow(RowRecordError),
    Tuple(TupleError),
    Storage(String),
    Commit(CommitError),
}

/// One supplied INSERT field or a request to materialize its catalog default.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InsertInput {
    Value(Option<crate::tuple::ScalarValue>),
    Default,
}

/// Schema and stable column/type identities resolved from one pinned catalog
/// snapshot. The containing table and every referenced column/type record are
/// added to the invocation's final-commit catalog guards by the loader.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogTableSchema {
    pub table_id: [u8; 16],
    pub schema_id: [u8; 16],
    pub schema_version: u64,
    pub write_epoch: u64,
    pub storage_object_id: [u8; 16],
    pub columns: Vec<ColumnSchema>,
    pub column_ids: Vec<[u8; 16]>,
    pub column_type_ids: Vec<[u8; 16]>,
    pub column_default_masks: Vec<u32>,
    pub default_expression_ids: Vec<Option<[u8; 16]>>,
    pub generation_modes: Vec<u16>,
}

/// A valid index definition resolved against one pinned table schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogIndexDefinition {
    pub index_id: [u8; 16],
    pub table_id: [u8; 16],
    pub column_ids: Vec<[u8; 16]>,
    pub unique: bool,
}

/// Opaque continuation for a row scan, scoped to the transaction that produced it.
#[derive(Clone)]
pub struct RowScanCursor {
    invocation_id: [u8; 16],
    table_id: [u8; 16],
    after_row_id: [u8; 16],
}

/// One bounded authorized row-scan page.
#[derive(Clone)]
pub struct AuthorizedRowScanPage {
    pub rows: Vec<(RowId, Vec<SanitizedCell>)>,
    pub continuation: Option<RowScanCursor>,
}

#[derive(Clone)]
struct ActiveRowScan {
    table_id: [u8; 16],
    after_row_id: [u8; 16],
    authority: ScanAuthority,
}

#[derive(Clone)]
struct ScanAuthority {
    columns: Vec<ColumnSchema>,
    column_ids: Vec<[u8; 16]>,
    schema_version: u64,
    schema_id: [u8; 16],
    context: AuthorizationContext,
    grants: Vec<Grant>,
    policies: Vec<Policy>,
    checks: Vec<Expr>,
    policy_driving_columns: Vec<[u8; 16]>,
    dependencies: Vec<AuthorityDependency>,
}

impl ScanAuthority {
    fn capture(projection: &AuthorizedProjection<'_>) -> Self {
        Self {
            columns: projection.columns.to_vec(),
            column_ids: projection.column_ids.to_vec(),
            schema_version: projection.schema_version,
            schema_id: projection.schema_id,
            context: projection.context.clone(),
            grants: projection.grants.to_vec(),
            policies: projection.policies.to_vec(),
            checks: projection.checks.to_vec(),
            policy_driving_columns: projection.policy_driving_columns.to_vec(),
            dependencies: projection.dependencies.to_vec(),
        }
    }

    fn matches(&self, projection: &AuthorizedProjection<'_>) -> bool {
        self.columns == projection.columns
            && self.column_ids == projection.column_ids
            && self.schema_version == projection.schema_version
            && self.schema_id == projection.schema_id
            && self.context.principal_id == projection.context.principal_id
            && self.context.role_ids == projection.context.role_ids
            && self.context.scope == projection.context.scope
            && self.grants == projection.grants
            && self.policies == projection.policies
            && self.checks == projection.checks
            && self.policy_driving_columns == projection.policy_driving_columns
            && self.dependencies == projection.dependencies
    }
}

impl std::fmt::Display for TransactionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "invocation transaction failed: {self:?}")
    }
}

impl std::error::Error for TransactionError {}

impl From<OverlayError> for TransactionError {
    fn from(error: OverlayError) -> Self {
        Self::Overlay(error)
    }
}

impl From<CommitError> for TransactionError {
    fn from(error: CommitError) -> Self {
        match error {
            CommitError::IndexOverlayLimitExceeded => Self::IndexOverlayLimitExceeded,
            CommitError::InvocationExpired => Self::Expired,
            CommitError::InvocationCancelled => Self::CancellationRequested,
            error => Self::Commit(error),
        }
    }
}

pub struct InvocationTransaction<'db> {
    database: &'db Database,
    snapshot: Option<RowSnapshot>,
    overlay: Overlay,
    base_versions: HashMap<RowId, Option<u64>>,
    index_writes: HashMap<Vec<u8>, StagedIndexChange>,
    loaded_schemas: HashMap<[u8; 16], CatalogTableSchema>,
    loaded_indexes: HashMap<[u8; 16], Vec<CatalogIndexDefinition>>,
    loaded_foreign_keys: HashMap<[u8; 16], ForeignKeyCheck>,
    catalog_guards: HashMap<Vec<u8>, Option<Vec<u8>>>,
    max_index_changes: usize,
    max_index_bytes: usize,
    index_bytes: usize,
    invocation_id: [u8; 16],
    principal_id: [u8; 16],
    retention_deadline_us: i64,
    coordination: InvocationCoordination,
    aborted: bool,
    outcome_recorded: bool,
    active_scan: Option<ActiveRowScan>,
    active_relation_scan: Option<ActiveRelationScan>,
    active_schema_scan: Option<ActiveSchemaScan>,
    active_column_scan: Option<ActiveColumnScan>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActiveRelationScan {
    schema_id: [u8; 16],
    principal_id: [u8; 16],
    role_ids: HashSet<[u8; 16]>,
    scope: HashSet<(Operation, AccessTarget)>,
    grants: Vec<Grant>,
    dependencies: Vec<AuthorityDependency>,
    continuation: Option<([u8; 16], [u8; 16])>,
    used_tokens: HashSet<[u8; 16]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActiveSchemaScan {
    principal_id: [u8; 16],
    role_ids: HashSet<[u8; 16]>,
    scope: HashSet<(Operation, AccessTarget)>,
    grants: Vec<Grant>,
    dependencies: Vec<AuthorityDependency>,
    continuation: Option<([u8; 16], [u8; 16])>,
    used_tokens: HashSet<[u8; 16]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActiveColumnScan {
    relation_id: [u8; 16],
    principal_id: [u8; 16],
    role_ids: HashSet<[u8; 16]>,
    scope: HashSet<(Operation, AccessTarget)>,
    grants: Vec<Grant>,
    dependencies: Vec<AuthorityDependency>,
    continuation: Option<([u8; 16], u32)>,
    used_tokens: HashSet<[u8; 16]>,
}

pub(crate) struct InvocationCoordination {
    ddl_fences: Option<DdlFenceCoordinator>,
    _admission_permit: Option<InvocationAdmissionPermit>,
    max_age: Option<(Instant, Duration)>,
}

impl InvocationCoordination {
    fn uncoordinated() -> Self {
        Self {
            ddl_fences: None,
            _admission_permit: None,
            max_age: None,
        }
    }

    fn fenced(ddl_fences: DdlFenceCoordinator) -> Self {
        Self {
            ddl_fences: Some(ddl_fences),
            _admission_permit: None,
            max_age: None,
        }
    }

    pub(crate) fn managed(
        ddl_fences: DdlFenceCoordinator,
        admission_permit: InvocationAdmissionPermit,
        max_age: Duration,
    ) -> Self {
        Self {
            ddl_fences: Some(ddl_fences),
            _admission_permit: Some(admission_permit),
            max_age: Some((Instant::now(), max_age)),
        }
    }

    fn is_expired(&self) -> bool {
        self.max_age
            .is_some_and(|(admitted_at, max_age)| admitted_at.elapsed() >= max_age)
    }

    fn may_continue(&self) -> bool {
        self._admission_permit
            .as_ref()
            .is_none_or(InvocationAdmissionPermit::may_continue)
    }

    fn mark_committing(&self) -> bool {
        self._admission_permit
            .as_ref()
            .is_none_or(|permit| permit.try_mark_committing())
    }
}

impl<'db> InvocationTransaction<'db> {
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
            retention_deadline_us,
            coordination,
            aborted: false,
            outcome_recorded: false,
            active_scan: None,
            active_relation_scan: None,
            active_schema_scan: None,
            active_column_scan: None,
        })
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
            self.abort()?;
            return Err(TransactionError::CancellationRequested);
        }
        Ok(())
    }

    fn pinned_snapshot(&mut self) -> Result<&RowSnapshot, TransactionError> {
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

    pub fn read(&mut self, id: RowId) -> Result<Option<Vec<u8>>, TransactionError> {
        match self.read_inner(id) {
            Ok(value) => Ok(value),
            Err(error) => {
                self.abort()?;
                Err(error)
            }
        }
    }

    /// Read the snapshot/private row, decode it with the pinned schema, and
    /// return only its authorized sanitized projection. Predicate/runtime
    /// errors abort the invocation; denied rows disappear and denied cells are
    /// represented without their value bytes or lengths.
    pub fn read_authorized(
        &mut self,
        id: RowId,
        projection: &AuthorizedProjection<'_>,
    ) -> Result<Option<Vec<SanitizedCell>>, TransactionError> {
        let result = (|| {
            self.capture_authority_dependencies(projection.context, projection.dependencies)?;
            let Some(payload) = self.read_inner(id)? else {
                return Ok(None);
            };
            let tuple = decode_tuple(projection.columns, projection.schema_version, &payload)
                .map_err(TransactionError::Tuple)?;
            project_row(
                projection.context,
                projection.grants,
                projection.policies,
                projection.schema_id,
                id.table,
                projection.column_ids,
                &tuple.values,
            )
            .map_err(TransactionError::Authorization)
        })();
        match result {
            Ok(projected) => Ok(projected),
            Err(error) => {
                self.abort()?;
                Err(error)
            }
        }
    }

    /// Scan a bounded page from the invocation snapshot plus its private
    /// overlay, then apply the pinned SELECT policy and cell projection to
    /// each resolved row. Full pages require a follow-up call to confirm EOF.
    pub fn scan_authorized(
        &mut self,
        table_id: [u8; 16],
        cursor: Option<&RowScanCursor>,
        max_scanned: usize,
        projection: &AuthorizedProjection<'_>,
    ) -> Result<AuthorizedRowScanPage, TransactionError> {
        let result = (|| {
            if table_id == [0; 16]
                || max_scanned == 0
                || max_scanned > MAX_ROW_SCAN_PAGE
                || projection.columns.is_empty()
                || projection.columns.len() != projection.column_ids.len()
                || projection.columns.len() > crate::tuple::MAX_COLUMNS
                || projection.schema_version == 0
                || projection.schema_id == [0; 16]
                || projection.column_ids.contains(&[0; 16])
                || projection.grants.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
                || projection.policies.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
                || projection.checks.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
                || projection.policy_driving_columns.len()
                    > crate::authorization::MAX_AUTHORIZATION_INPUTS
                || projection.dependencies.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
            {
                return Err(TransactionError::InvalidScanRequest);
            }
            let after_row_id = cursor.map(|cursor| cursor.after_row_id);
            match self.active_scan.as_ref() {
                Some(active)
                    if active.table_id == table_id
                        && Some(active.after_row_id) == after_row_id
                        && cursor.is_some_and(|cursor| {
                            cursor.invocation_id == self.invocation_id
                                && cursor.table_id == table_id
                        })
                        && active.authority.matches(projection) => {}
                Some(_) => return Err(TransactionError::InvalidScanRequest),
                None if after_row_id.is_some() => {
                    return Err(TransactionError::InvalidScanRequest);
                }
                None => {}
            }
            self.capture_authority_dependencies(projection.context, projection.dependencies)?;
            let (snapshot_ids, snapshot_complete) = {
                self.pinned_snapshot()?
                    .scan_row_ids_after(table_id, after_row_id, max_scanned)
                    .map_err(map_snapshot_scan_error)?
            };
            let (overlay_ids, overlay_complete) =
                self.overlay
                    .row_ids_after(table_id, after_row_id, max_scanned)?;

            let mut row_ids = snapshot_ids;
            row_ids.extend(overlay_ids);
            row_ids.sort_unstable();
            row_ids.dedup();
            row_ids.truncate(max_scanned);
            let complete = snapshot_complete && overlay_complete && row_ids.len() < max_scanned;
            let after_row_id = row_ids.last().copied();
            let mut rows = Vec::with_capacity(row_ids.len());
            for row in &row_ids {
                let id = RowId {
                    table: table_id,
                    row: *row,
                };
                if let Some(cells) = self.read_authorized(id, projection)? {
                    rows.push((id, cells));
                }
            }
            self.active_scan = if complete {
                None
            } else {
                after_row_id.map(|after_row_id| ActiveRowScan {
                    table_id,
                    after_row_id,
                    authority: ScanAuthority::capture(projection),
                })
            };
            Ok(AuthorizedRowScanPage {
                rows,
                continuation: if complete {
                    None
                } else {
                    after_row_id.map(|after_row_id| RowScanCursor {
                        invocation_id: self.invocation_id,
                        table_id,
                        after_row_id,
                    })
                },
            })
        })();
        match result {
            Ok(page) => Ok(page),
            Err(error) => {
                self.abort()?;
                Err(error)
            }
        }
    }

    /// Return summary counts for the trusted planner without returning row
    /// values. This entry point is intended for a provisioned planner channel.
    pub fn estimate_authorized_scan(
        &mut self,
        table_id: [u8; 16],
        projection: &AuthorizedProjection<'_>,
    ) -> Result<AuthorizedScanEstimate, TransactionError> {
        self.check_deadline()?;
        if !valid_estimate_projection(table_id, projection) {
            return self.finish_estimate(AuthorizedScanEstimate::Unknown);
        }
        if let Err(error) =
            self.capture_authority_dependencies(projection.context, projection.dependencies)
        {
            return if estimate_error_is_terminal(&error) {
                Err(error)
            } else {
                self.finish_estimate(AuthorizedScanEstimate::Unknown)
            };
        }
        if !estimate_population_is_unfiltered(table_id, projection) {
            return self.finish_estimate(AuthorizedScanEstimate::Unknown);
        }
        let overlay_row_ids = match self
            .overlay
            .bounded_row_ids_for_table(table_id, MAX_ESTIMATE_CANDIDATES)
        {
            Ok(Some(row_ids)) => row_ids,
            Ok(None) => return self.finish_estimate(AuthorizedScanEstimate::Unknown),
            Err(OverlayError::Aborted) => return Err(TransactionError::Aborted),
            Err(_) => return self.finish_estimate(AuthorizedScanEstimate::Unknown),
        };

        let mut populations = vec![ColumnPopulation::default(); projection.columns.len()];
        let mut visible_rows = 0u64;
        let mut scanned = 0usize;
        let mut after_row_id = None;
        loop {
            let remaining = MAX_ESTIMATE_CANDIDATES.saturating_sub(scanned);
            if remaining == 0 {
                return self.finish_estimate(AuthorizedScanEstimate::Unknown);
            }
            let limit = remaining.min(MAX_ROW_SCAN_PAGE);
            let (mut snapshot_ids, snapshot_complete) = match self
                .pinned_snapshot()?
                .scan_row_ids_after(table_id, after_row_id, limit)
            {
                Ok(page) => page,
                Err(_) => return self.finish_estimate(AuthorizedScanEstimate::Unknown),
            };
            let overlay_start = after_row_id.map_or(0, |after| {
                overlay_row_ids.partition_point(|row_id| *row_id <= after)
            });
            let overlay_end = overlay_start
                .saturating_add(limit)
                .min(overlay_row_ids.len());
            let overlay_complete = overlay_end == overlay_row_ids.len();
            snapshot_ids.extend_from_slice(&overlay_row_ids[overlay_start..overlay_end]);
            snapshot_ids.sort_unstable();
            snapshot_ids.dedup();
            snapshot_ids.truncate(limit);
            let page_complete = snapshot_complete && overlay_complete && snapshot_ids.len() < limit;
            let Some(next_after_row_id) = snapshot_ids.last().copied() else {
                if page_complete {
                    return self.finish_estimate(AuthorizedScanEstimate::Exact(
                        AuthorizedScanSummary {
                            visible_rows,
                            columns: populations,
                        },
                    ));
                }
                return self.finish_estimate(AuthorizedScanEstimate::Unknown);
            };

            for row_id in &snapshot_ids {
                let id = RowId {
                    table: table_id,
                    row: *row_id,
                };
                let cells = match self.project_row_for_estimate(id, projection) {
                    Ok(Some(cells)) => cells,
                    Ok(None) => continue,
                    Err(error) if estimate_error_is_terminal(&error) => return Err(error),
                    Err(_) => return self.finish_estimate(AuthorizedScanEstimate::Unknown),
                };
                if cells.len() != populations.len() {
                    return self.finish_estimate(AuthorizedScanEstimate::Unknown);
                }
                visible_rows += 1;
                for (population, cell) in populations.iter_mut().zip(cells) {
                    match cell {
                        SanitizedCell::Value(_) => population.readable_values += 1,
                        SanitizedCell::Null => population.nulls += 1,
                        SanitizedCell::Redacted => population.redacted += 1,
                    }
                }
            }
            scanned += snapshot_ids.len();
            if page_complete {
                return self.finish_estimate(AuthorizedScanEstimate::Exact(
                    AuthorizedScanSummary {
                        visible_rows,
                        columns: populations,
                    },
                ));
            }
            if scanned >= MAX_ESTIMATE_CANDIDATES {
                return self.finish_estimate(AuthorizedScanEstimate::Unknown);
            }
            after_row_id = Some(next_after_row_id);
        }
    }

    fn finish_estimate(
        &mut self,
        estimate: AuthorizedScanEstimate,
    ) -> Result<AuthorizedScanEstimate, TransactionError> {
        self.check_deadline()?;
        Ok(estimate)
    }

    fn project_row_for_estimate(
        &mut self,
        id: RowId,
        projection: &AuthorizedProjection<'_>,
    ) -> Result<Option<Vec<SanitizedCell>>, TransactionError> {
        let Some(payload) = self.read_inner(id)? else {
            return Ok(None);
        };
        let tuple = decode_tuple(projection.columns, projection.schema_version, &payload)
            .map_err(TransactionError::Tuple)?;
        project_row(
            projection.context,
            projection.grants,
            projection.policies,
            projection.schema_id,
            id.table,
            projection.column_ids,
            &tuple.values,
        )
        .map_err(TransactionError::Authorization)
    }

    /// Read one catalog definition from the pinned snapshot and retain its exact
    /// bytes for schema/authority revalidation at final commit.
    pub fn read_catalog_record(
        &mut self,
        key: &[u8],
    ) -> Result<Option<CatalogRecord>, TransactionError> {
        if self.aborted {
            return Err(TransactionError::Aborted);
        }
        let bytes = match self.pinned_snapshot()?.get_catalog_value(key) {
            Ok(bytes) => bytes,
            Err(error) => {
                self.abort()?;
                return Err(TransactionError::Storage(error.to_string()));
            }
        };
        let Some(bytes) = bytes else {
            self.catalog_guards.insert(key.to_vec(), None);
            return Ok(None);
        };
        let record = match decode_registered_envelope(&bytes) {
            Ok(record) if record.logical_key == key => record,
            _ => {
                self.abort()?;
                return Err(TransactionError::CorruptCatalog);
            }
        };
        if record.family_code == 7 && decode_catalog_expression_record(&record).is_err() {
            self.abort()?;
            return Err(TransactionError::CorruptCatalog);
        }
        self.catalog_guards.insert(key.to_vec(), Some(bytes));
        Ok(Some(record))
    }

    /// List visible relations using grants derived from the admitted
    /// principal's pinned catalog. The caller supplies only the schema and
    /// page continuation.
    pub fn list_catalog_authorized_relations(
        &mut self,
        schema_id: [u8; 16],
        page_limit: usize,
        continuation: Option<RelationScanCursor>,
    ) -> Result<Option<VisibleRelationPage>, TransactionError> {
        let result = (|| {
            if schema_id == [0; 16] || page_limit == 0 || page_limit > MAX_RELATIONS_PER_PAGE {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let principal_id = self.principal_id;
            let (role_ids, grants) = self.load_catalog_relation_discovery_grants(schema_id)?;
            let scope = grants
                .iter()
                .map(|grant| (grant.operation, grant.target))
                .collect();
            let context = AuthorizationContext {
                principal_id,
                role_ids,
                scope,
            };
            self.list_authorized_relations(
                schema_id,
                page_limit,
                continuation,
                &context,
                &grants,
                &[],
            )
        })();
        match result {
            Ok(page) => Ok(page),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Resolve a qualified schema and relation name using authority derived
    /// from the admitted invocation's pinned catalog.
    pub fn resolve_catalog_authorized_relation(
        &mut self,
        schema_name: &str,
        relation_name: &str,
    ) -> Result<Option<AuthorizedRelationDescription>, TransactionError> {
        let result = (|| {
            if schema_name.is_empty()
                || schema_name.len() > MAX_METADATA_PAGE_BYTES
                || relation_name.is_empty()
                || relation_name.len() > MAX_METADATA_PAGE_BYTES
            {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let principal_id = self.principal_id;
            let (schema_roles, schema_grants) =
                self.load_catalog_schema_usage_grants(principal_id)?;
            let schema_scope = schema_grants
                .iter()
                .map(|grant| (grant.operation, grant.target))
                .collect();
            let schema_context = AuthorizationContext {
                principal_id,
                role_ids: schema_roles,
                scope: schema_scope,
            };
            let Some(schema) =
                self.resolve_authorized_schema(schema_name, &schema_context, &schema_grants, &[])?
            else {
                return Ok(None);
            };

            let (relation_roles, relation_grants) =
                self.load_catalog_relation_discovery_grants(schema.schema_id)?;
            let relation_scope = relation_grants
                .iter()
                .map(|grant| (grant.operation, grant.target))
                .collect();
            let relation_context = AuthorizationContext {
                principal_id,
                role_ids: relation_roles,
                scope: relation_scope,
            };
            self.resolve_authorized_relation(
                schema.schema_id,
                relation_name,
                &relation_context,
                &relation_grants,
                &[],
            )
        })();
        match result {
            Ok(description) => Ok(description),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// List relations in one schema only when the invocation has schema usage
    /// and at least one table or column data grant. The role closure and each
    /// supplied grant are checked against this transaction's pinned catalog.
    pub fn list_authorized_relations(
        &mut self,
        schema_id: [u8; 16],
        page_limit: usize,
        continuation: Option<RelationScanCursor>,
        context: &AuthorizationContext,
        grants: &[Grant],
        dependencies: &[AuthorityDependency],
    ) -> Result<Option<VisibleRelationPage>, TransactionError> {
        let result = (|| {
            if schema_id == [0; 16]
                || page_limit == 0
                || page_limit > MAX_RELATIONS_PER_PAGE
                || grants.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
            {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            self.check_deadline()?;
            self.capture_authority_dependencies(context, dependencies)?;
            let pinned_roles = self.load_catalog_role_closure(context.principal_id)?;
            if pinned_roles != context.role_ids {
                return Err(TransactionError::InvalidContext);
            }
            for grant in grants {
                self.validate_metadata_grant(context, *grant)?;
            }

            if !has_permission(
                context,
                grants,
                Operation::Usage,
                AccessTarget::Schema(schema_id),
            ) {
                return Ok(None);
            }

            let mut active_scan = match continuation.as_ref() {
                Some(cursor) => {
                    let Some(active) = self.active_relation_scan.as_ref() else {
                        return Err(TransactionError::InvalidMetadataRequest);
                    };
                    if active.schema_id != schema_id
                        || active.principal_id != context.principal_id
                        || active.role_ids != context.role_ids
                        || active.scope != context.scope
                        || active.grants != grants
                        || active.dependencies != dependencies
                        || active
                            .continuation
                            .is_none_or(|(token, _)| token != cursor.token)
                    {
                        return Err(TransactionError::InvalidMetadataRequest);
                    }
                    active.clone()
                }
                None => ActiveRelationScan {
                    schema_id,
                    principal_id: context.principal_id,
                    role_ids: context.role_ids.clone(),
                    scope: context.scope.clone(),
                    grants: grants.to_vec(),
                    dependencies: dependencies.to_vec(),
                    continuation: None,
                    used_tokens: HashSet::new(),
                },
            };

            let schema_object_key = catalog_id_key(2, schema_id);
            let Some(schema_object) = self.read_catalog_record(&schema_object_key)? else {
                return Ok(None);
            };
            if schema_object.family_code != 2
                || catalog_u16(&schema_object, 1)? != 1
                || catalog_optional_id(&schema_object, 2)?.is_some()
                || catalog_id(&schema_object, 4)? == [0; 16]
                || catalog_u64(&schema_object, 5)? == 0
                || catalog_u16(&schema_object, 6)? != 1
            {
                return Err(TransactionError::CorruptCatalog);
            }
            let schema_key = catalog_id_key(3, schema_id);
            let schema = self
                .read_catalog_record(&schema_key)?
                .ok_or(TransactionError::CorruptCatalog)?;
            if schema.family_code != 3 {
                return Err(TransactionError::CorruptCatalog);
            }
            let mut table_ids = catalog_id_list(&schema, 1)?;
            if table_ids.contains(&[0; 16]) {
                return Err(TransactionError::CorruptCatalog);
            }
            table_ids.sort_unstable();
            if table_ids.windows(2).any(|pair| pair[0] == pair[1]) {
                return Err(TransactionError::CorruptCatalog);
            }

            let mut table_grants = HashSet::new();
            let mut column_grants = HashMap::<[u8; 16], HashSet<[u8; 16]>>::new();
            for grant in grants {
                let subject_matches = match grant.subject {
                    Subject::Principal(id) => id == context.principal_id,
                    Subject::Role(id) => context.role_ids.contains(&id),
                };
                if !subject_matches || !context.scope.contains(&(grant.operation, grant.target)) {
                    continue;
                }
                match (grant.operation, grant.target) {
                    (
                        Operation::Select
                        | Operation::Insert
                        | Operation::Update
                        | Operation::Delete,
                        AccessTarget::Table(table_id),
                    ) => {
                        table_grants.insert(table_id);
                    }
                    (
                        Operation::Select | Operation::Insert | Operation::Update,
                        AccessTarget::Column {
                            table_id,
                            column_id,
                        },
                    ) => {
                        column_grants.entry(table_id).or_default().insert(column_id);
                    }
                    _ => {}
                }
            }

            let after_id = if let Some(cursor) = continuation.as_ref() {
                let (token, after_id) = active_scan
                    .continuation
                    .take()
                    .ok_or(TransactionError::InvalidMetadataRequest)?;
                if token != cursor.token {
                    return Err(TransactionError::InvalidMetadataRequest);
                }
                Some(after_id)
            } else {
                None
            };
            let mut relations = Vec::with_capacity(page_limit);
            let mut page_bytes = 0usize;
            let mut has_more = false;
            for table_id in table_ids {
                if after_id.is_some_and(|after| table_id <= after) {
                    continue;
                }
                let mut discoverable = table_grants.contains(&table_id);
                if !discoverable && let Some(column_ids) = column_grants.get(&table_id) {
                    for column_id in column_ids {
                        self.validate_discovery_column(table_id, *column_id)?;
                    }
                    discoverable = !column_ids.is_empty();
                }
                if !discoverable {
                    continue;
                }
                let relation = self.read_discovery_relation(schema_id, table_id)?;
                let encoded_size = relation
                    .name
                    .len()
                    .checked_add(24)
                    .ok_or(TransactionError::MetadataResultTooLarge)?;
                if relations.len() == page_limit
                    || page_bytes.saturating_add(encoded_size) > MAX_METADATA_PAGE_BYTES
                {
                    if relations.is_empty() {
                        return Err(TransactionError::MetadataResultTooLarge);
                    }
                    has_more = true;
                    break;
                }
                page_bytes += encoded_size;
                relations.push(relation);
            }
            let continuation = if has_more {
                let after_id = relations
                    .last()
                    .map(|relation| relation.relation_id)
                    .ok_or(TransactionError::InvalidMetadataRequest)?;
                let token = new_metadata_cursor_token(&mut active_scan.used_tokens)?;
                active_scan.continuation = Some((token, after_id));
                Some(RelationScanCursor { token })
            } else {
                active_scan.continuation = None;
                None
            };
            self.active_relation_scan = Some(active_scan);
            Ok(Some(VisibleRelationPage {
                relations,
                continuation,
            }))
        })();
        match result {
            Ok(page) => Ok(page),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Resolve an exact relation name from filtered catalog pages, then
    /// independently recheck authority before returning its declared shape.
    pub fn resolve_authorized_relation(
        &mut self,
        schema_id: [u8; 16],
        relation_name: &str,
        context: &AuthorizationContext,
        grants: &[Grant],
        dependencies: &[AuthorityDependency],
    ) -> Result<Option<AuthorizedRelationDescription>, TransactionError> {
        let result = (|| {
            if relation_name.is_empty() || relation_name.len() > MAX_METADATA_PAGE_BYTES {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let mut continuation = None;
            loop {
                let Some(page) = self.list_authorized_relations(
                    schema_id,
                    MAX_RELATIONS_PER_PAGE,
                    continuation,
                    context,
                    grants,
                    dependencies,
                )?
                else {
                    return Ok(None);
                };
                let relation = page
                    .relations
                    .into_iter()
                    .find(|relation| relation.name == relation_name);
                if let Some(relation) = relation {
                    let Some(description) = self.describe_authorized_columns(
                        relation.relation_id,
                        context,
                        grants,
                        dependencies,
                    )?
                    else {
                        return Ok(None);
                    };
                    let initial_response_bytes = relation
                        .name
                        .len()
                        .checked_add(24)
                        .ok_or(TransactionError::MetadataResultTooLarge)?;
                    let response_bytes = description
                        .columns
                        .iter()
                        .try_fold(initial_response_bytes, |bytes, column| {
                            bytes
                                .checked_add(column.name.len())
                                .and_then(|total| total.checked_add(32))
                        })
                        .ok_or(TransactionError::MetadataResultTooLarge)?;
                    if response_bytes > MAX_METADATA_PAGE_BYTES {
                        return Err(TransactionError::MetadataResultTooLarge);
                    }
                    return Ok(Some(AuthorizedRelationDescription {
                        relation_id: relation.relation_id,
                        name: relation.name,
                        columns: description.columns,
                    }));
                }
                continuation = page.continuation;
                if continuation.is_none() {
                    return Ok(None);
                }
            }
        })();
        match result {
            Ok(description) => Ok(description),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Describe declared column shape after checking relation discovery authority.
    pub fn describe_authorized_columns(
        &mut self,
        table_id: [u8; 16],
        context: &AuthorizationContext,
        grants: &[Grant],
        dependencies: &[AuthorityDependency],
    ) -> Result<Option<VisibleRelationSchema>, TransactionError> {
        let result = (|| {
            let Some(trusted_schema) =
                self.load_authorized_discovery_schema(table_id, context, grants, dependencies)?
            else {
                return Ok(None);
            };
            let mut columns = Vec::with_capacity(trusted_schema.column_ids.len());
            let mut response_bytes = 0usize;
            for ordinal in 0..trusted_schema.column_ids.len() {
                let column = self.read_visible_column(table_id, &trusted_schema, ordinal)?;
                response_bytes = response_bytes
                    .checked_add(column.name.len())
                    .and_then(|bytes| bytes.checked_add(32))
                    .ok_or(TransactionError::MetadataResultTooLarge)?;
                if response_bytes > MAX_METADATA_PAGE_BYTES {
                    return Err(TransactionError::MetadataResultTooLarge);
                }
                columns.push(column);
            }
            Ok(Some(VisibleRelationSchema {
                relation_id: table_id,
                columns,
            }))
        })();
        match result {
            Ok(description) => Ok(description),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Return one bounded page of a relation's declared columns.
    pub fn describe_authorized_columns_page(
        &mut self,
        table_id: [u8; 16],
        page_limit: usize,
        continuation: Option<ColumnScanCursor>,
        context: &AuthorizationContext,
        grants: &[Grant],
        dependencies: &[AuthorityDependency],
    ) -> Result<Option<VisibleColumnPage>, TransactionError> {
        let result = (|| {
            if page_limit == 0 || page_limit > MAX_COLUMNS_PER_PAGE {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let Some(trusted_schema) =
                self.load_authorized_discovery_schema(table_id, context, grants, dependencies)?
            else {
                return Ok(None);
            };
            let mut active_scan = match continuation.as_ref() {
                Some(cursor) => {
                    let Some(active) = self.active_column_scan.as_ref() else {
                        return Err(TransactionError::InvalidMetadataRequest);
                    };
                    if active.relation_id != table_id
                        || active.principal_id != context.principal_id
                        || active.role_ids != context.role_ids
                        || active.scope != context.scope
                        || active.grants != grants
                        || active.dependencies != dependencies
                        || active
                            .continuation
                            .is_none_or(|(token, _)| token != cursor.token)
                    {
                        return Err(TransactionError::InvalidMetadataRequest);
                    }
                    active.clone()
                }
                None => ActiveColumnScan {
                    relation_id: table_id,
                    principal_id: context.principal_id,
                    role_ids: context.role_ids.clone(),
                    scope: context.scope.clone(),
                    grants: grants.to_vec(),
                    dependencies: dependencies.to_vec(),
                    continuation: None,
                    used_tokens: HashSet::new(),
                },
            };
            let start_ordinal = if let Some(cursor) = continuation.as_ref() {
                let (token, after_ordinal) = active_scan
                    .continuation
                    .take()
                    .ok_or(TransactionError::InvalidMetadataRequest)?;
                if token != cursor.token {
                    return Err(TransactionError::InvalidMetadataRequest);
                }
                usize::try_from(after_ordinal)
                    .ok()
                    .and_then(|ordinal| ordinal.checked_add(1))
                    .ok_or(TransactionError::InvalidMetadataRequest)?
            } else {
                0
            };
            if start_ordinal > trusted_schema.column_ids.len() {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let mut columns = Vec::with_capacity(page_limit);
            let mut response_bytes = 0usize;
            let mut has_more = false;
            for ordinal in start_ordinal..trusted_schema.column_ids.len() {
                if columns.len() == page_limit {
                    has_more = true;
                    break;
                }
                let column = self.read_visible_column(table_id, &trusted_schema, ordinal)?;
                let encoded_size = column
                    .name
                    .len()
                    .checked_add(32)
                    .ok_or(TransactionError::MetadataResultTooLarge)?;
                if response_bytes.saturating_add(encoded_size) > MAX_METADATA_PAGE_BYTES {
                    if columns.is_empty() {
                        return Err(TransactionError::MetadataResultTooLarge);
                    }
                    has_more = true;
                    break;
                }
                response_bytes += encoded_size;
                columns.push(column);
            }
            let continuation = if has_more {
                let after_ordinal = columns
                    .last()
                    .map(|column| column.ordinal)
                    .ok_or(TransactionError::InvalidMetadataRequest)?;
                let token = new_metadata_cursor_token(&mut active_scan.used_tokens)?;
                active_scan.continuation = Some((token, after_ordinal));
                Some(ColumnScanCursor { token })
            } else {
                active_scan.continuation = None;
                None
            };
            self.active_column_scan = Some(active_scan);
            Ok(Some(VisibleColumnPage {
                relation_id: table_id,
                columns,
                continuation,
            }))
        })();
        match result {
            Ok(page) => Ok(page),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// List schemas for which the invocation has schema USAGE.
    pub fn list_catalog_authorized_schemas(
        &mut self,
        page_limit: usize,
        continuation: Option<SchemaScanCursor>,
    ) -> Result<VisibleSchemaPage, TransactionError> {
        let result = (|| {
            if page_limit == 0 || page_limit > MAX_SCHEMAS_PER_PAGE {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let principal_id = self.principal_id;
            let (role_ids, grants) = self.load_catalog_schema_usage_grants(principal_id)?;
            let scope = grants
                .iter()
                .map(|grant| (grant.operation, grant.target))
                .collect();
            let context = AuthorizationContext {
                principal_id,
                role_ids,
                scope,
            };
            self.list_authorized_schemas(page_limit, continuation, &context, &grants, &[])
        })();
        match result {
            Ok(page) => Ok(page),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// List schemas for which the invocation has schema USAGE.
    pub fn list_authorized_schemas(
        &mut self,
        page_limit: usize,
        continuation: Option<SchemaScanCursor>,
        context: &AuthorizationContext,
        grants: &[Grant],
        dependencies: &[AuthorityDependency],
    ) -> Result<VisibleSchemaPage, TransactionError> {
        let result = (|| {
            if page_limit == 0
                || page_limit > MAX_SCHEMAS_PER_PAGE
                || grants.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
            {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            self.check_deadline()?;
            self.capture_authority_dependencies(context, dependencies)?;
            let pinned_roles = self.load_catalog_role_closure(context.principal_id)?;
            if pinned_roles != context.role_ids {
                return Err(TransactionError::InvalidContext);
            }
            for grant in grants {
                self.validate_metadata_grant(context, *grant)?;
            }

            let mut active_scan = match continuation.as_ref() {
                Some(cursor) => {
                    let Some(active) = self.active_schema_scan.as_ref() else {
                        return Err(TransactionError::InvalidMetadataRequest);
                    };
                    if active.principal_id != context.principal_id
                        || active.role_ids != context.role_ids
                        || active.scope != context.scope
                        || active.grants != grants
                        || active.dependencies != dependencies
                        || active
                            .continuation
                            .is_none_or(|(token, _)| token != cursor.token)
                    {
                        return Err(TransactionError::InvalidMetadataRequest);
                    }
                    active.clone()
                }
                None => ActiveSchemaScan {
                    principal_id: context.principal_id,
                    role_ids: context.role_ids.clone(),
                    scope: context.scope.clone(),
                    grants: grants.to_vec(),
                    dependencies: dependencies.to_vec(),
                    continuation: None,
                    used_tokens: HashSet::new(),
                },
            };

            let mut visible_schema_ids = grants
                .iter()
                .filter_map(|grant| {
                    let AccessTarget::Schema(schema_id) = grant.target else {
                        return None;
                    };
                    let subject_matches = match grant.subject {
                        Subject::Principal(id) => id == context.principal_id,
                        Subject::Role(id) => context.role_ids.contains(&id),
                    };
                    (grant.operation == Operation::Usage
                        && subject_matches
                        && context
                            .scope
                            .contains(&(Operation::Usage, AccessTarget::Schema(schema_id))))
                    .then_some(schema_id)
                })
                .collect::<Vec<_>>();
            visible_schema_ids.sort_unstable();
            visible_schema_ids.dedup();
            let after_id = if let Some(cursor) = continuation.as_ref() {
                let (token, after_id) = active_scan
                    .continuation
                    .take()
                    .ok_or(TransactionError::InvalidMetadataRequest)?;
                if token != cursor.token {
                    return Err(TransactionError::InvalidMetadataRequest);
                }
                Some(after_id)
            } else {
                None
            };
            let mut schemas = Vec::with_capacity(page_limit);
            let mut page_bytes = 0usize;
            let mut has_more = false;
            for schema_id in visible_schema_ids {
                if after_id.is_some_and(|after| schema_id <= after) {
                    continue;
                }
                let schema = self.read_discovery_schema(schema_id)?;
                let encoded_size = schema
                    .name
                    .len()
                    .checked_add(24)
                    .ok_or(TransactionError::MetadataResultTooLarge)?;
                if schemas.len() == page_limit
                    || page_bytes.saturating_add(encoded_size) > MAX_METADATA_PAGE_BYTES
                {
                    if schemas.is_empty() {
                        return Err(TransactionError::MetadataResultTooLarge);
                    }
                    has_more = true;
                    break;
                }
                page_bytes += encoded_size;
                schemas.push(schema);
            }
            let continuation = if has_more {
                let after_id = schemas
                    .last()
                    .map(|schema| schema.schema_id)
                    .ok_or(TransactionError::InvalidMetadataRequest)?;
                let token = new_metadata_cursor_token(&mut active_scan.used_tokens)?;
                active_scan.continuation = Some((token, after_id));
                Some(SchemaScanCursor { token })
            } else {
                active_scan.continuation = None;
                None
            };
            self.active_schema_scan = Some(active_scan);
            Ok(VisibleSchemaPage {
                schemas,
                continuation,
            })
        })();
        match result {
            Ok(page) => Ok(page),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Resolve an exact schema name from the invocation's visible schema pages.
    pub fn resolve_authorized_schema(
        &mut self,
        schema_name: &str,
        context: &AuthorizationContext,
        grants: &[Grant],
        dependencies: &[AuthorityDependency],
    ) -> Result<Option<VisibleSchema>, TransactionError> {
        let result = (|| {
            if schema_name.is_empty() || schema_name.len() > MAX_METADATA_PAGE_BYTES {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let mut continuation = None;
            loop {
                let page = self.list_authorized_schemas(
                    MAX_SCHEMAS_PER_PAGE,
                    continuation,
                    context,
                    grants,
                    dependencies,
                )?;
                let schema = page
                    .schemas
                    .into_iter()
                    .find(|schema| schema.name == schema_name);
                if schema.is_some() {
                    return Ok(schema);
                }
                continuation = page.continuation;
                if continuation.is_none() {
                    return Ok(None);
                }
            }
        })();
        match result {
            Ok(schema) => Ok(schema),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    fn validate_discovery_column(
        &mut self,
        table_id: [u8; 16],
        column_id: [u8; 16],
    ) -> Result<(), TransactionError> {
        let column_key = catalog_id_key(6, column_id);
        let column = self
            .read_catalog_record(&column_key)?
            .ok_or(TransactionError::CorruptCatalog)?;
        if column.family_code != 6 || catalog_id(&column, 1)? != table_id {
            return Err(TransactionError::CorruptCatalog);
        }
        let column_object_key = catalog_id_key(2, column_id);
        let column_object = self
            .read_catalog_record(&column_object_key)?
            .ok_or(TransactionError::CorruptCatalog)?;
        if column_object.family_code != 2
            || catalog_u16(&column_object, 1)? != 4
            || catalog_optional_id(&column_object, 2)? != Some(table_id)
            || catalog_id(&column_object, 4)? == [0; 16]
            || catalog_u64(&column_object, 5)? == 0
            || catalog_u16(&column_object, 6)? != 1
            || catalog_field(&column_object, 3)? != catalog_field(&column, 3)?
        {
            return Err(TransactionError::CorruptCatalog);
        }
        Ok(())
    }

    fn load_authorized_discovery_schema(
        &mut self,
        table_id: [u8; 16],
        context: &AuthorizationContext,
        grants: &[Grant],
        dependencies: &[AuthorityDependency],
    ) -> Result<Option<CatalogTableSchema>, TransactionError> {
        if table_id == [0; 16] || grants.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        self.check_deadline()?;
        self.capture_authority_dependencies(context, dependencies)?;
        let pinned_roles = self.load_catalog_role_closure(context.principal_id)?;
        if pinned_roles != context.role_ids {
            return Err(TransactionError::InvalidContext);
        }
        for grant in grants {
            self.validate_metadata_grant(context, *grant)?;
        }

        let table_key = catalog_id_key(5, table_id);
        let Some(table) = self.read_catalog_record(&table_key)? else {
            return Ok(None);
        };
        if table.family_code != 5 {
            return Err(TransactionError::CorruptCatalog);
        }
        let schema_id = catalog_id(&table, 1)?;
        if !has_permission(
            context,
            grants,
            Operation::Usage,
            AccessTarget::Schema(schema_id),
        ) {
            return Ok(None);
        }

        let table_target = AccessTarget::Table(table_id);
        let table_grant = [
            Operation::Select,
            Operation::Insert,
            Operation::Update,
            Operation::Delete,
        ]
        .into_iter()
        .any(|operation| has_permission(context, grants, operation, table_target));
        let column_grants = grants
            .iter()
            .filter_map(|grant| {
                let AccessTarget::Column {
                    table_id: granted_table_id,
                    column_id,
                } = grant.target
                else {
                    return None;
                };
                let operation_allowed = matches!(
                    grant.operation,
                    Operation::Select | Operation::Insert | Operation::Update
                );
                let subject_matches = match grant.subject {
                    Subject::Principal(id) => id == context.principal_id,
                    Subject::Role(id) => context.role_ids.contains(&id),
                };
                (granted_table_id == table_id
                    && operation_allowed
                    && subject_matches
                    && context.scope.contains(&(grant.operation, grant.target)))
                .then_some(column_id)
            })
            .collect::<HashSet<_>>();
        if !table_grant && column_grants.is_empty() {
            return Ok(None);
        }
        if !table_grant {
            for column_id in &column_grants {
                self.validate_discovery_column(table_id, *column_id)?;
            }
        }

        self.read_discovery_relation(schema_id, table_id)?;
        self.load_catalog_table_schema(table_id).map(Some)
    }

    fn read_visible_column(
        &mut self,
        table_id: [u8; 16],
        schema: &CatalogTableSchema,
        ordinal: usize,
    ) -> Result<VisibleColumn, TransactionError> {
        let column_id = *schema
            .column_ids
            .get(ordinal)
            .ok_or(TransactionError::CorruptCatalog)?;
        self.validate_discovery_column(table_id, column_id)?;
        let record = self
            .read_catalog_record(&catalog_id_key(6, column_id))?
            .ok_or(TransactionError::CorruptCatalog)?;
        let name = String::from_utf8(catalog_field(&record, 3)?.to_vec())
            .map_err(|_| TransactionError::CorruptCatalog)?;
        let schema_column = schema
            .columns
            .get(ordinal)
            .ok_or(TransactionError::CorruptCatalog)?;
        Ok(VisibleColumn {
            column_id,
            name,
            ordinal: ordinal as u32,
            scalar_type: schema_column.scalar_type,
            nullable: schema_column.nullable,
        })
    }

    fn validate_metadata_grant(
        &mut self,
        context: &AuthorizationContext,
        grant: Grant,
    ) -> Result<(), TransactionError> {
        let grantee_id = match grant.subject {
            Subject::Principal(id) if id == context.principal_id => id,
            Subject::Role(id) if context.role_ids.contains(&id) => id,
            _ => return Err(TransactionError::InvalidContext),
        };
        let (object_id, privilege, target_scope, target_id) = match (grant.operation, grant.target)
        {
            (Operation::Usage, AccessTarget::Schema(schema_id)) => (schema_id, 1, 1, schema_id),
            (Operation::Select, AccessTarget::Table(table_id)) => (table_id, 2, 1, table_id),
            (Operation::Insert, AccessTarget::Table(table_id)) => (table_id, 3, 1, table_id),
            (Operation::Update, AccessTarget::Table(table_id)) => (table_id, 4, 1, table_id),
            (Operation::Delete, AccessTarget::Table(table_id)) => (table_id, 5, 1, table_id),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id,
                    column_id,
                },
            ) => (table_id, 2, 2, column_id),
            (
                Operation::Insert,
                AccessTarget::Column {
                    table_id,
                    column_id,
                },
            ) => (table_id, 3, 2, column_id),
            (
                Operation::Update,
                AccessTarget::Column {
                    table_id,
                    column_id,
                },
            ) => (table_id, 4, 2, column_id),
            _ => return Err(TransactionError::InvalidContext),
        };
        if grantee_id == [0; 16] || object_id == [0; 16] || target_id == [0; 16] {
            return Err(TransactionError::InvalidContext);
        }
        let key = catalog_grant_key(grantee_id, object_id, privilege, target_scope, target_id);
        let record = self
            .read_catalog_record(&key)?
            .ok_or(TransactionError::InvalidContext)?;
        let grantor_id = catalog_id(&record, 1)?;
        if record.family_code != 12 || grantor_id == [0; 16] || catalog_u64(&record, 2)? == 0 {
            return Err(TransactionError::CorruptCatalog);
        }
        let grantor = self
            .read_catalog_record(&catalog_id_key(10, grantor_id))?
            .ok_or(TransactionError::CorruptCatalog)?;
        let grantor_kind = catalog_u16(&grantor, 1)?;
        let grantor_login = catalog_field(&grantor, 3)?;
        if grantor.family_code != 10
            || !matches!(grantor_kind, 1 | 2)
            || catalog_u64(&grantor, 4)? == 0
            || !matches!(grantor_login, [0] | [1])
            || (grantor_kind == 2 && grantor_login != [0])
        {
            return Err(TransactionError::CorruptCatalog);
        }
        Ok(())
    }

    fn read_discovery_relation(
        &mut self,
        schema_id: [u8; 16],
        table_id: [u8; 16],
    ) -> Result<VisibleRelation, TransactionError> {
        let table_key = catalog_id_key(5, table_id);
        let table = self
            .read_catalog_record(&table_key)?
            .ok_or(TransactionError::CorruptCatalog)?;
        if table.family_code != 5
            || catalog_id(&table, 1)? != schema_id
            || catalog_u64(&table, 2)? == 0
            || catalog_u64(&table, 3)? == 0
            || catalog_id(&table, 4)? == [0; 16]
            || catalog_u16(&table, 5)? != 1
        {
            return Err(TransactionError::CorruptCatalog);
        }
        let object_key = catalog_id_key(2, table_id);
        let object = self
            .read_catalog_record(&object_key)?
            .ok_or(TransactionError::CorruptCatalog)?;
        if object.family_code != 2
            || catalog_u16(&object, 1)? != 3
            || catalog_optional_id(&object, 2)? != Some(schema_id)
            || catalog_id(&object, 4)? == [0; 16]
            || catalog_u64(&object, 5)? == 0
            || catalog_u16(&object, 6)? != 1
        {
            return Err(TransactionError::CorruptCatalog);
        }
        let name = String::from_utf8(catalog_field(&object, 3)?.to_vec())
            .map_err(|_| TransactionError::CorruptCatalog)?;
        Ok(VisibleRelation {
            relation_id: table_id,
            name,
        })
    }

    fn read_discovery_schema(
        &mut self,
        schema_id: [u8; 16],
    ) -> Result<VisibleSchema, TransactionError> {
        let object_key = catalog_id_key(2, schema_id);
        let object = self
            .read_catalog_record(&object_key)?
            .ok_or(TransactionError::CorruptCatalog)?;
        if object.family_code != 2
            || catalog_u16(&object, 1)? != 1
            || catalog_optional_id(&object, 2)?.is_some()
            || catalog_id(&object, 4)? == [0; 16]
            || catalog_u64(&object, 5)? == 0
            || catalog_u16(&object, 6)? != 1
        {
            return Err(TransactionError::CorruptCatalog);
        }
        let schema_key = catalog_id_key(3, schema_id);
        let schema = self
            .read_catalog_record(&schema_key)?
            .ok_or(TransactionError::CorruptCatalog)?;
        if schema.family_code != 3 {
            return Err(TransactionError::CorruptCatalog);
        }
        let table_ids = catalog_id_list(&schema, 1)?;
        if table_ids.contains(&[0; 16])
            || table_ids.iter().collect::<HashSet<_>>().len() != table_ids.len()
        {
            return Err(TransactionError::CorruptCatalog);
        }
        let name = String::from_utf8(catalog_field(&object, 3)?.to_vec())
            .map_err(|_| TransactionError::CorruptCatalog)?;
        Ok(VisibleSchema { schema_id, name })
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
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Resolve a table's current schema from its pinned catalog snapshot.
    /// Column records are selected by their table ID, sorted by ordinal, and
    /// checked for contiguous ordinals. Every table, column, and type record
    /// is retained as an exact-byte guard for final commit validation.
    pub fn load_catalog_table_schema(
        &mut self,
        table_id: [u8; 16],
    ) -> Result<CatalogTableSchema, TransactionError> {
        let result = (|| {
            if table_id == [0; 16] {
                return Err(TransactionError::InvalidContext);
            }
            let table_key = catalog_id_key(5, table_id);
            let table = self
                .read_catalog_record(&table_key)?
                .ok_or(TransactionError::CorruptCatalog)?;
            if table.family_code != 5 {
                return Err(TransactionError::CorruptCatalog);
            }
            let schema_id = catalog_id(&table, 1)?;
            let schema_version = catalog_u64(&table, 2)?;
            let write_epoch = catalog_u64(&table, 3)?;
            let storage_object_id = catalog_id(&table, 4)?;
            if schema_version == 0 || write_epoch == 0 || catalog_u16(&table, 5)? != 1 {
                return Err(TransactionError::CorruptCatalog);
            }

            let records = self
                .pinned_snapshot()?
                .scan_catalog_family(6, MAX_CATALOG_FAMILY_SCAN)
                .map_err(|error| TransactionError::Storage(error.to_string()))?;
            let mut columns = Vec::new();
            for record in records.into_iter().filter(|record| {
                record.family_code == 6
                    && record
                        .fields
                        .iter()
                        .find(|field| field.tag == 1)
                        .is_some_and(|field| field.payload == table_id)
            }) {
                let key = record.logical_key.clone();
                let guarded = self
                    .read_catalog_record(&key)?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if guarded != record || key.len() != 19 {
                    return Err(TransactionError::CorruptCatalog);
                }
                columns.push((
                    catalog_u32(&record, 2)?,
                    key[3..19].try_into().unwrap(),
                    record,
                ));
                if columns.len() > crate::tuple::MAX_COLUMNS {
                    return Err(TransactionError::CorruptCatalog);
                }
            }
            columns.sort_by_key(|(ordinal, _, _)| *ordinal);
            if columns.is_empty() {
                return Err(TransactionError::CorruptCatalog);
            }

            let mut column_schemas = Vec::with_capacity(columns.len());
            let mut column_ids = Vec::with_capacity(columns.len());
            let mut column_type_ids = Vec::with_capacity(columns.len());
            let mut column_default_masks = Vec::with_capacity(columns.len());
            let mut default_expression_ids = Vec::with_capacity(columns.len());
            let mut generation_modes = Vec::with_capacity(columns.len());
            let mut type_records = HashMap::<[u8; 16], CatalogRecord>::new();
            for (expected_ordinal, (ordinal, column_id, record)) in columns.into_iter().enumerate()
            {
                if ordinal as usize != expected_ordinal
                    || catalog_id(&record, 1)? != table_id
                    || catalog_u64(&record, 9)? == 0
                    || catalog_u64(&record, 9)? > schema_version
                {
                    return Err(TransactionError::CorruptCatalog);
                }
                let type_id = catalog_id(&record, 4)?;
                if let std::collections::hash_map::Entry::Vacant(entry) =
                    type_records.entry(type_id)
                {
                    entry.insert(
                        self.read_catalog_record(&catalog_id_key(4, type_id))?
                            .ok_or(TransactionError::CorruptCatalog)?,
                    );
                }
                let type_record = &type_records[&type_id];
                let scalar_type = decode_catalog_scalar_type(type_record, &record)?;
                let default_mask = catalog_u32(type_record, 5)?;
                let nullable = match catalog_field(&record, 6)? {
                    [0] => false,
                    [1] => true,
                    _ => return Err(TransactionError::CorruptCatalog),
                };
                let generation_mode = catalog_u16(&record, 8)?;
                if generation_mode > 1
                    || (generation_mode == 1 && scalar_type != ScalarType::Uuid)
                    || (generation_mode == 1 && record.fields.iter().any(|field| field.tag == 7))
                {
                    return Err(TransactionError::CorruptCatalog);
                }
                encode_tuple(
                    &[ColumnSchema {
                        scalar_type,
                        nullable: true,
                    }],
                    1,
                    &[None],
                )
                .map_err(|_| TransactionError::CorruptCatalog)?;
                column_schemas.push(ColumnSchema {
                    scalar_type,
                    nullable,
                });
                column_ids.push(column_id);
                column_type_ids.push(type_id);
                column_default_masks.push(default_mask);
                default_expression_ids.push(
                    record
                        .fields
                        .iter()
                        .find(|field| field.tag == 7)
                        .map(|field| field.payload.as_slice().try_into().unwrap()),
                );
                generation_modes.push(generation_mode);
            }
            Ok(CatalogTableSchema {
                table_id,
                schema_id,
                schema_version,
                write_epoch,
                storage_object_id,
                columns: column_schemas,
                column_ids,
                column_type_ids,
                column_default_masks,
                default_expression_ids,
                generation_modes,
            })
        })();
        match result {
            Ok(schema) => {
                self.loaded_schemas.insert(schema.table_id, schema.clone());
                Ok(schema)
            }
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Load the table's ordered index definitions from the pinned catalog.
    /// Each definition is checked against the catalog-derived schema, and its
    /// exact bytes are guarded for final commit validation.
    pub fn load_catalog_indexes(
        &mut self,
        schema: &CatalogTableSchema,
    ) -> Result<Vec<CatalogIndexDefinition>, TransactionError> {
        let result = (|| {
            let trusted_schema = self.load_catalog_table_schema(schema.table_id)?;
            if trusted_schema != *schema {
                return Err(TransactionError::InvalidContext);
            }
            let records = self
                .pinned_snapshot()?
                .scan_catalog_family(9, MAX_CATALOG_FAMILY_SCAN)
                .map_err(|error| TransactionError::Storage(error.to_string()))?;
            let mut indexes = Vec::new();
            let mut seen_index_ids = HashSet::new();
            for record in records {
                if record.logical_key.len() != 19 || record.family_code != 9 {
                    return Err(TransactionError::CorruptCatalog);
                }
                let table_id = catalog_id(&record, 1)?;
                if table_id != schema.table_id {
                    continue;
                }
                let guarded = self
                    .read_catalog_record(&record.logical_key)?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if guarded != record {
                    return Err(TransactionError::CorruptCatalog);
                }
                let index_id: [u8; 16] = record.logical_key[3..19]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                let column_ids = catalog_id_list(&record, 2)?;
                let mut seen_columns = HashSet::new();
                let unique = match catalog_field(&record, 4)? {
                    [0] => false,
                    [1] => true,
                    _ => return Err(TransactionError::CorruptCatalog),
                };
                if index_id == [0; 16]
                    || !seen_index_ids.insert(index_id)
                    || column_ids.is_empty()
                    || column_ids.len() > 256
                    || column_ids.iter().any(|column_id| {
                        *column_id == [0; 16]
                            || !seen_columns.insert(*column_id)
                            || !schema.column_ids.contains(column_id)
                    })
                    || catalog_u16(&record, 3)? != 1
                    || catalog_u16(&record, 5)? != 1
                    || catalog_optional_id(&record, 6)?.is_some_and(|id| id == [0; 16])
                    || catalog_id(&record, 7)? == [0; 16]
                    || catalog_u16(&record, 8)? != 1
                    || catalog_u32(&record, 9)? != 1
                    || catalog_u64(&record, 10)? != schema.schema_version
                {
                    return Err(TransactionError::CorruptCatalog);
                }
                indexes.push(CatalogIndexDefinition {
                    index_id,
                    table_id,
                    column_ids,
                    unique,
                });
            }
            indexes.sort_by_key(|index| index.index_id);
            self.load_catalog_foreign_keys_for_table(schema)?;
            Ok(indexes)
        })();
        match result {
            Ok(indexes) => {
                self.loaded_schemas.insert(schema.table_id, schema.clone());
                self.loaded_indexes.insert(schema.table_id, indexes.clone());
                Ok(indexes)
            }
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    fn load_catalog_foreign_keys_for_table(
        &mut self,
        schema: &CatalogTableSchema,
    ) -> Result<(), TransactionError> {
        let records = self
            .pinned_snapshot()?
            .scan_catalog_family(8, MAX_CATALOG_FAMILY_SCAN)
            .map_err(|error| TransactionError::Storage(error.to_string()))?;
        for record in records {
            if record.family_code != 8 || record.logical_key.len() != 19 {
                return Err(TransactionError::CorruptCatalog);
            }
            if catalog_u16(&record, 2)? != 5 {
                continue;
            }
            let constraint_id: [u8; 16] = record.logical_key[3..19]
                .try_into()
                .map_err(|_| TransactionError::CorruptCatalog)?;
            let source_table_id = catalog_id(&record, 1)?;
            let target_constraint_id =
                catalog_optional_id(&record, 5)?.ok_or(TransactionError::CorruptCatalog)?;
            if constraint_id == [0; 16] || target_constraint_id == [0; 16] {
                return Err(TransactionError::CorruptCatalog);
            }
            let target_constraint_key = catalog_id_key(8, target_constraint_id);
            let target_snapshot_record = self
                .pinned_snapshot()?
                .get_catalog_record(&target_constraint_key)
                .map_err(|error| TransactionError::Storage(error.to_string()))?
                .ok_or(TransactionError::CorruptCatalog)?;
            if target_snapshot_record.family_code != 8 {
                return Err(TransactionError::CorruptCatalog);
            }
            let target_table_id = catalog_id(&target_snapshot_record, 1)?;
            if source_table_id != schema.table_id && target_table_id != schema.table_id {
                continue;
            }

            let guarded_foreign_key = self
                .read_catalog_record(&record.logical_key)?
                .ok_or(TransactionError::CorruptCatalog)?;
            let guarded_target = self
                .read_catalog_record(&target_constraint_key)?
                .ok_or(TransactionError::CorruptCatalog)?;
            if guarded_foreign_key != record || guarded_target != target_snapshot_record {
                return Err(TransactionError::CorruptCatalog);
            }
            let target_kind = catalog_u16(&guarded_target, 2)?;
            let source_column_ids = catalog_id_list(&guarded_foreign_key, 3)?;
            let target_column_ids = catalog_id_list(&guarded_target, 3)?;
            let referenced_column_ids = catalog_id_list(&guarded_foreign_key, 6)?;
            let target_index_id = catalog_optional_id(&guarded_target, 11)?
                .ok_or(TransactionError::CorruptCatalog)?;
            if !matches!(target_kind, 2 | 3)
                || catalog_id(&guarded_target, 1)? != target_table_id
                || source_column_ids.is_empty()
                || source_column_ids.len() != referenced_column_ids.len()
                || referenced_column_ids != target_column_ids
                || source_column_ids.contains(&[0; 16])
                || referenced_column_ids.contains(&[0; 16])
                || source_column_ids.iter().collect::<HashSet<_>>().len() != source_column_ids.len()
                || referenced_column_ids.iter().collect::<HashSet<_>>().len()
                    != referenced_column_ids.len()
                || catalog_optional_id(&guarded_foreign_key, 4)?.is_some()
                || catalog_optional_id(&guarded_foreign_key, 11)?.is_some()
                || catalog_u16(&guarded_foreign_key, 7)? != 1
                || catalog_u16(&guarded_foreign_key, 8)? != 1
                || catalog_u16(&guarded_foreign_key, 9)? != 1
                || catalog_u16(&guarded_foreign_key, 10)? != 1
                || catalog_optional_id(&guarded_target, 5)?.is_some()
                || !catalog_id_list(&guarded_target, 6)?.is_empty()
                || catalog_u16(&guarded_target, 7)? != 0
                || catalog_u16(&guarded_target, 8)? != 0
                || catalog_u16(&guarded_target, 9)? != 0
                || catalog_u16(&guarded_target, 10)? != 1
                || target_index_id == [0; 16]
            {
                return Err(TransactionError::CorruptCatalog);
            }

            let source_schema = if source_table_id == schema.table_id {
                schema.clone()
            } else {
                self.load_catalog_table_schema(source_table_id)?
            };
            let target_schema = if target_table_id == schema.table_id {
                schema.clone()
            } else {
                self.load_catalog_table_schema(target_table_id)?
            };
            let source_ordinals = source_column_ids
                .iter()
                .map(|id| {
                    source_schema
                        .column_ids
                        .iter()
                        .position(|column_id| column_id == id)
                        .ok_or(TransactionError::CorruptCatalog)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let target_ordinals = referenced_column_ids
                .iter()
                .map(|id| {
                    target_schema
                        .column_ids
                        .iter()
                        .position(|column_id| column_id == id)
                        .ok_or(TransactionError::CorruptCatalog)
                })
                .collect::<Result<Vec<_>, _>>()?;
            if source_ordinals
                .iter()
                .zip(&target_ordinals)
                .any(|(source, target)| {
                    source_schema.columns[*source].scalar_type
                        != target_schema.columns[*target].scalar_type
                })
            {
                return Err(TransactionError::CorruptCatalog);
            }
            if target_kind == 2
                && target_ordinals
                    .iter()
                    .any(|ordinal| target_schema.columns[*ordinal].nullable)
            {
                return Err(TransactionError::CorruptCatalog);
            }

            let target_index = self
                .read_catalog_record(&catalog_id_key(9, target_index_id))?
                .ok_or(TransactionError::CorruptCatalog)?;
            if target_index.family_code != 9
                || catalog_id(&target_index, 1)? != target_table_id
                || catalog_id_list(&target_index, 2)? != target_column_ids
                || catalog_u16(&target_index, 3)? != 1
                || catalog_field(&target_index, 4)? != [1]
                || catalog_u16(&target_index, 5)? != 1
                || catalog_optional_id(&target_index, 6)? != Some(target_constraint_id)
                || catalog_id(&target_index, 7)? == [0; 16]
                || catalog_u16(&target_index, 8)? != 1
                || catalog_u32(&target_index, 9)? != 1
                || catalog_u64(&target_index, 10)? != target_schema.schema_version
            {
                return Err(TransactionError::CorruptCatalog);
            }

            let check = ForeignKeyCheck {
                source_table_id,
                source_schema_version: source_schema.schema_version,
                source_schema: source_schema.columns,
                source_ordinals,
                target_table_id,
                target_schema_version: target_schema.schema_version,
                target_schema: target_schema.columns,
                target_ordinals,
            };
            if let Some(existing) = self.loaded_foreign_keys.get(&constraint_id) {
                if existing != &check {
                    return Err(TransactionError::CorruptCatalog);
                }
            } else {
                self.loaded_foreign_keys.insert(constraint_id, check);
            }
            if self.loaded_foreign_keys.len() > 256 {
                return Err(TransactionError::CorruptCatalog);
            }
        }
        Ok(())
    }

    /// Load the pinned membership graph, validate every live endpoint, and
    /// return the transitive role closure for one principal. Membership and
    /// principal records are guarded so authority changes conflict at commit.
    pub fn load_catalog_role_closure(
        &mut self,
        principal_id: [u8; 16],
    ) -> Result<HashSet<[u8; 16]>, TransactionError> {
        let result = (|| {
            if principal_id == [0; 16] {
                return Err(TransactionError::InvalidContext);
            }
            let records = self
                .pinned_snapshot()?
                .scan_catalog_family(11, MAX_CATALOG_FAMILY_SCAN)
                .map_err(|error| TransactionError::Storage(error.to_string()))?;
            if records.len() > crate::authorization::MAX_MEMBERSHIP_EDGES {
                return Err(TransactionError::CorruptCatalog);
            }
            let mut memberships = Vec::with_capacity(records.len());
            let mut principal_ids = HashSet::new();
            for record in records {
                let key = record.logical_key.clone();
                if key.len() != 36 || key[2] != 1 || key[19] != 1 {
                    return Err(TransactionError::CorruptCatalog);
                }
                let guarded = self
                    .read_catalog_record(&key)?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if guarded != record
                    || catalog_id(&record, 1)? == [0; 16]
                    || catalog_u64(&record, 2)? == 0
                {
                    return Err(TransactionError::CorruptCatalog);
                }
                let member_id: [u8; 16] = key[3..19]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                let role_id: [u8; 16] = key[20..36]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                memberships.push(Membership { member_id, role_id });
                principal_ids.insert(member_id);
                principal_ids.insert(role_id);
            }
            let principal_is_membership_endpoint = principal_ids.contains(&principal_id);
            principal_ids.insert(principal_id);
            for id in principal_ids {
                let record = self
                    .read_catalog_record(&catalog_id_key(10, id))?
                    .ok_or_else(|| {
                        if id == self.principal_id && !principal_is_membership_endpoint {
                            TransactionError::Authorization(AuthorizationError::Denied)
                        } else {
                            TransactionError::CorruptCatalog
                        }
                    })?;
                let kind = catalog_u16(&record, 1)?;
                let login_enabled = catalog_field(&record, 3)?;
                if !matches!(kind, 1 | 2)
                    || catalog_u64(&record, 4)? == 0
                    || !matches!(login_enabled, [0] | [1])
                    || (kind == 2 && login_enabled != [0])
                {
                    return Err(TransactionError::CorruptCatalog);
                }
                if id == self.principal_id && (kind != 1 || login_enabled != [1]) {
                    return Err(TransactionError::Authorization(AuthorizationError::Denied));
                }
                if memberships.iter().any(|edge| edge.role_id == id) && kind != 2 {
                    return Err(TransactionError::CorruptCatalog);
                }
            }
            resolve_role_closure(principal_id, &memberships)
                .map_err(|_| TransactionError::CorruptCatalog)
        })();
        match result {
            Ok(roles) => Ok(roles),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Load data-access grants for the admitted principal and its pinned role
    /// closure, scoped to one catalog-derived table schema. Matching grant,
    /// grantor, and target column records are retained as commit guards.
    pub fn load_catalog_grants(
        &mut self,
        principal_id: [u8; 16],
        schema: &CatalogTableSchema,
    ) -> Result<(HashSet<[u8; 16]>, Vec<Grant>), TransactionError> {
        let result = (|| {
            if principal_id != self.principal_id || schema.table_id == [0; 16] {
                return Err(TransactionError::InvalidContext);
            }
            let roles = self.load_catalog_role_closure(principal_id)?;
            let mut grantees = roles.clone();
            grantees.insert(principal_id);
            let records = self
                .pinned_snapshot()?
                .scan_catalog_family(12, MAX_CATALOG_FAMILY_SCAN)
                .map_err(|error| TransactionError::Storage(error.to_string()))?;
            let mut grants = Vec::new();
            let mut guarded_principals = HashSet::new();
            for record in records {
                let key = &record.logical_key;
                if key.len() != 59
                    || key[2] != 1
                    || key[19] != 1
                    || key[36] != 1
                    || key[39] != 1
                    || key[42] != 1
                {
                    return Err(TransactionError::CorruptCatalog);
                }
                let grantee_id: [u8; 16] = key[3..19]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                if !grantees.contains(&grantee_id) {
                    continue;
                }
                let object_id: [u8; 16] = key[20..36]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                let privilege = u16::from_be_bytes([key[37], key[38]]);
                let scope = u16::from_be_bytes([key[40], key[41]]);
                let target_id: [u8; 16] = key[43..59]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                let target = match scope {
                    1 if object_id == schema.schema_id && target_id == object_id => {
                        AccessTarget::Schema(object_id)
                    }
                    1 if object_id == schema.table_id && target_id == object_id => {
                        AccessTarget::Table(object_id)
                    }
                    2 if object_id == schema.table_id && schema.column_ids.contains(&target_id) => {
                        let column_record = self
                            .read_catalog_record(&catalog_id_key(6, target_id))?
                            .ok_or(TransactionError::CorruptCatalog)?;
                        if catalog_id(&column_record, 1)? != schema.table_id {
                            return Err(TransactionError::CorruptCatalog);
                        }
                        AccessTarget::Column {
                            table_id: schema.table_id,
                            column_id: target_id,
                        }
                    }
                    _ => continue,
                };
                let operation = match (privilege, target) {
                    (1, AccessTarget::Schema(_)) => Operation::Usage,
                    (2, AccessTarget::Table(_) | AccessTarget::Column { .. }) => Operation::Select,
                    (3, AccessTarget::Table(_) | AccessTarget::Column { .. }) => Operation::Insert,
                    (4, AccessTarget::Table(_) | AccessTarget::Column { .. }) => Operation::Update,
                    (5, AccessTarget::Table(_)) => Operation::Delete,
                    (1..=5, _) => return Err(TransactionError::CorruptCatalog),
                    _ => continue,
                };
                let guarded = self
                    .read_catalog_record(key)?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if guarded != record || catalog_u64(&record, 2)? == 0 {
                    return Err(TransactionError::CorruptCatalog);
                }
                let grantor_id = catalog_id(&record, 1)?;
                if guarded_principals.insert(grantor_id) {
                    let grantor = self
                        .read_catalog_record(&catalog_id_key(10, grantor_id))?
                        .ok_or(TransactionError::CorruptCatalog)?;
                    let grantor_kind = catalog_u16(&grantor, 1)?;
                    let grantor_login = catalog_field(&grantor, 3)?;
                    if !matches!(grantor_kind, 1 | 2)
                        || catalog_u64(&grantor, 4)? == 0
                        || !matches!(grantor_login, [0] | [1])
                        || (grantor_kind == 2 && grantor_login != [0])
                    {
                        return Err(TransactionError::CorruptCatalog);
                    }
                }
                let subject = if grantee_id == principal_id {
                    Subject::Principal(principal_id)
                } else {
                    Subject::Role(grantee_id)
                };
                grants.push(Grant {
                    subject,
                    operation,
                    target,
                });
                if grants.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS {
                    return Err(TransactionError::CorruptCatalog);
                }
            }
            Ok((roles, grants))
        })();
        match result {
            Ok(authorization) => Ok(authorization),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Load schema USAGE grants for the admitted principal and its pinned role
    /// closure. Matching grants, grantors, and schema records become commit
    /// guards on this invocation.
    pub fn load_catalog_schema_usage_grants(
        &mut self,
        principal_id: [u8; 16],
    ) -> Result<(HashSet<[u8; 16]>, Vec<Grant>), TransactionError> {
        let result = (|| {
            if principal_id != self.principal_id || principal_id == [0; 16] {
                return Err(TransactionError::InvalidContext);
            }
            let roles = self.load_catalog_role_closure(principal_id)?;
            let mut grantees = roles.clone();
            grantees.insert(principal_id);
            let records = self
                .pinned_snapshot()?
                .scan_catalog_family(12, MAX_CATALOG_FAMILY_SCAN)
                .map_err(|error| TransactionError::Storage(error.to_string()))?;
            let mut grants = Vec::new();
            let mut guarded_principals = HashSet::new();
            for record in records {
                let key = &record.logical_key;
                if key.len() != 59
                    || key[2] != 1
                    || key[19] != 1
                    || key[36] != 1
                    || key[39] != 1
                    || key[42] != 1
                {
                    return Err(TransactionError::CorruptCatalog);
                }
                let grantee_id: [u8; 16] = key[3..19]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                if !grantees.contains(&grantee_id) {
                    continue;
                }
                let schema_id: [u8; 16] = key[20..36]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                let privilege = u16::from_be_bytes([key[37], key[38]]);
                let target_scope = u16::from_be_bytes([key[40], key[41]]);
                let target_id: [u8; 16] = key[43..59]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                if privilege != 1 {
                    continue;
                }
                if target_scope != 1 || schema_id == [0; 16] || target_id != schema_id {
                    return Err(TransactionError::CorruptCatalog);
                }
                let guarded = self
                    .read_catalog_record(key)?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if guarded != record || guarded.family_code != 12 || catalog_u64(&record, 2)? == 0 {
                    return Err(TransactionError::CorruptCatalog);
                }
                self.read_discovery_schema(schema_id)?;
                let grantor_id = catalog_id(&record, 1)?;
                if grantor_id == [0; 16] {
                    return Err(TransactionError::CorruptCatalog);
                }
                if guarded_principals.insert(grantor_id) {
                    let grantor = self
                        .read_catalog_record(&catalog_id_key(10, grantor_id))?
                        .ok_or(TransactionError::CorruptCatalog)?;
                    let grantor_kind = catalog_u16(&grantor, 1)?;
                    let grantor_login = catalog_field(&grantor, 3)?;
                    if grantor.family_code != 10
                        || !matches!(grantor_kind, 1 | 2)
                        || catalog_u64(&grantor, 4)? == 0
                        || !matches!(grantor_login, [0] | [1])
                        || (grantor_kind == 2 && grantor_login != [0])
                    {
                        return Err(TransactionError::CorruptCatalog);
                    }
                }
                grants.push(Grant {
                    subject: if grantee_id == principal_id {
                        Subject::Principal(principal_id)
                    } else {
                        Subject::Role(grantee_id)
                    },
                    operation: Operation::Usage,
                    target: AccessTarget::Schema(schema_id),
                });
                if grants.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS {
                    return Err(TransactionError::CorruptCatalog);
                }
            }
            Ok((roles, grants))
        })();
        match result {
            Ok(authorization) => Ok(authorization),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Load the admitted principal's schema USAGE and relation data grants for
    /// one schema from the pinned catalog and role closure.
    pub fn load_catalog_relation_discovery_grants(
        &mut self,
        schema_id: [u8; 16],
    ) -> Result<(HashSet<[u8; 16]>, Vec<Grant>), TransactionError> {
        let result = (|| {
            if schema_id == [0; 16] {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let principal_id = self.principal_id;
            let roles = self.load_catalog_role_closure(principal_id)?;
            let mut grantees = roles.clone();
            grantees.insert(principal_id);
            let records = self
                .pinned_snapshot()?
                .scan_catalog_family(12, MAX_CATALOG_FAMILY_SCAN)
                .map_err(|error| TransactionError::Storage(error.to_string()))?;

            let mut has_schema_usage = false;
            for record in &records {
                let key = &record.logical_key;
                if key.len() != 59
                    || key[2] != 1
                    || key[19] != 1
                    || key[36] != 1
                    || key[39] != 1
                    || key[42] != 1
                {
                    return Err(TransactionError::CorruptCatalog);
                }
                let grantee_id: [u8; 16] = key[3..19]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                if !grantees.contains(&grantee_id) {
                    continue;
                }
                let object_id: [u8; 16] = key[20..36]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                let privilege = u16::from_be_bytes([key[37], key[38]]);
                if object_id == schema_id && privilege == 1 {
                    let target_scope = u16::from_be_bytes([key[40], key[41]]);
                    let target_id: [u8; 16] = key[43..59]
                        .try_into()
                        .map_err(|_| TransactionError::CorruptCatalog)?;
                    if target_scope != 1 || target_id != schema_id {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    has_schema_usage = true;
                }
            }
            if !has_schema_usage {
                return Ok((roles, Vec::new()));
            }

            self.read_discovery_schema(schema_id)?;
            let schema_record = self
                .read_catalog_record(&catalog_id_key(3, schema_id))?
                .ok_or(TransactionError::CorruptCatalog)?;
            let mut table_ids = catalog_id_list(&schema_record, 1)?;
            table_ids.sort_unstable();
            if table_ids.contains(&[0; 16]) || table_ids.windows(2).any(|pair| pair[0] == pair[1]) {
                return Err(TransactionError::CorruptCatalog);
            }

            let mut grants = Vec::new();
            let mut guarded_principals = HashSet::new();
            for record in records {
                let key = &record.logical_key;
                let grantee_id: [u8; 16] = key[3..19]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                if !grantees.contains(&grantee_id) {
                    continue;
                }
                let object_id: [u8; 16] = key[20..36]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                let privilege = u16::from_be_bytes([key[37], key[38]]);
                let target_scope = u16::from_be_bytes([key[40], key[41]]);
                let target_id: [u8; 16] = key[43..59]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                let target = if object_id == schema_id {
                    if privilege != 1 || target_scope != 1 || target_id != schema_id {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    Some((Operation::Usage, AccessTarget::Schema(schema_id)))
                } else if table_ids.binary_search(&object_id).is_ok() {
                    match target_scope {
                        1 if target_id == object_id => {
                            let operation = match privilege {
                                2 => Operation::Select,
                                3 => Operation::Insert,
                                4 => Operation::Update,
                                5 => Operation::Delete,
                                _ => return Err(TransactionError::CorruptCatalog),
                            };
                            Some((operation, AccessTarget::Table(object_id)))
                        }
                        2 if matches!(privilege, 2..=4) => {
                            self.validate_discovery_column(object_id, target_id)?;
                            let operation = match privilege {
                                2 => Operation::Select,
                                3 => Operation::Insert,
                                4 => Operation::Update,
                                _ => return Err(TransactionError::CorruptCatalog),
                            };
                            Some((
                                operation,
                                AccessTarget::Column {
                                    table_id: object_id,
                                    column_id: target_id,
                                },
                            ))
                        }
                        _ => return Err(TransactionError::CorruptCatalog),
                    }
                } else {
                    None
                };
                let Some((operation, target)) = target else {
                    continue;
                };
                let guarded = self
                    .read_catalog_record(key)?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if guarded != record || guarded.family_code != 12 || catalog_u64(&record, 2)? == 0 {
                    return Err(TransactionError::CorruptCatalog);
                }
                let grantor_id = catalog_id(&record, 1)?;
                if grantor_id == [0; 16] {
                    return Err(TransactionError::CorruptCatalog);
                }
                if guarded_principals.insert(grantor_id) {
                    let grantor = self
                        .read_catalog_record(&catalog_id_key(10, grantor_id))?
                        .ok_or(TransactionError::CorruptCatalog)?;
                    let grantor_kind = catalog_u16(&grantor, 1)?;
                    let grantor_login = catalog_field(&grantor, 3)?;
                    if grantor.family_code != 10
                        || !matches!(grantor_kind, 1 | 2)
                        || catalog_u64(&grantor, 4)? == 0
                        || !matches!(grantor_login, [0] | [1])
                        || (grantor_kind == 2 && grantor_login != [0])
                    {
                        return Err(TransactionError::CorruptCatalog);
                    }
                }
                grants.push(Grant {
                    subject: if grantee_id == principal_id {
                        Subject::Principal(principal_id)
                    } else {
                        Subject::Role(grantee_id)
                    },
                    operation,
                    target,
                });
                if grants.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS {
                    return Err(TransactionError::CorruptCatalog);
                }
            }
            Ok((roles, grants))
        })();
        match result {
            Ok(authorization) => Ok(authorization),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Resolve one unique built-in type ID from the pinned type catalog and
    /// retain the type definition as a commit guard.
    pub fn load_catalog_builtin_type_id(
        &mut self,
        builtin_kind: u16,
    ) -> Result<[u8; 16], TransactionError> {
        let result = (|| {
            if !(1..=11).contains(&builtin_kind) {
                return Err(TransactionError::InvalidContext);
            }
            let records = self
                .pinned_snapshot()?
                .scan_catalog_family(4, MAX_CATALOG_FAMILY_SCAN)
                .map_err(|error| TransactionError::Storage(error.to_string()))?;
            let mut matched = None;
            for record in records {
                if catalog_u16(&record, 1)? != builtin_kind {
                    continue;
                }
                if matched.is_some() || record.logical_key.len() != 19 {
                    return Err(TransactionError::CorruptCatalog);
                }
                let guarded = self
                    .read_catalog_record(&record.logical_key)?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if guarded != record {
                    return Err(TransactionError::CorruptCatalog);
                }
                let scalar_type = decode_catalog_scalar_type_parameters(&record, &[0; 8])?;
                if !matches!(
                    (builtin_kind, scalar_type),
                    (1, ScalarType::Boolean)
                        | (2, ScalarType::SmallInt)
                        | (3, ScalarType::Integer)
                        | (4, ScalarType::BigInt)
                        | (5, ScalarType::Decimal { .. })
                        | (6, ScalarType::Text)
                        | (7, ScalarType::Varchar { .. })
                        | (8, ScalarType::Bytea)
                        | (9, ScalarType::Uuid)
                        | (10, ScalarType::Date)
                        | (11, ScalarType::TimestampTz)
                ) {
                    return Err(TransactionError::CorruptCatalog);
                }
                matched = Some(record.logical_key[3..19].try_into().unwrap());
            }
            matched.ok_or(TransactionError::CorruptCatalog)
        })();
        match result {
            Ok(type_id) => Ok(type_id),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Load applicable row and cell policies for the admitted principal from
    /// the pinned catalog. Definitions, target rows, recipient principals,
    /// and referenced schema-bound expressions are retained as commit guards.
    pub fn load_catalog_policies(
        &mut self,
        principal_id: [u8; 16],
        schema: &CatalogTableSchema,
    ) -> Result<Vec<Policy>, TransactionError> {
        let result = (|| {
            if principal_id != self.principal_id || schema.table_id == [0; 16] {
                return Err(TransactionError::InvalidContext);
            }
            let roles = self.load_catalog_role_closure(principal_id)?;
            let policies = self
                .pinned_snapshot()?
                .scan_catalog_family(13, MAX_CATALOG_FAMILY_SCAN)
                .map_err(|error| TransactionError::Storage(error.to_string()))?;
            let targets = self
                .pinned_snapshot()?
                .scan_catalog_family(14, MAX_CATALOG_FAMILY_SCAN)
                .map_err(|error| TransactionError::Storage(error.to_string()))?;
            if policies.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS {
                return Err(TransactionError::CorruptCatalog);
            }
            let boolean_type_id = self.load_catalog_builtin_type_id(1)?;
            let mut loaded = Vec::new();
            for record in policies {
                if catalog_id(&record, 1)? != schema.table_id {
                    continue;
                }
                let policy_key = record.logical_key.clone();
                if policy_key.len() != 19 {
                    return Err(TransactionError::CorruptCatalog);
                }
                let guarded = self
                    .read_catalog_record(&policy_key)?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if guarded != record || catalog_u64(&record, 7)? == 0 {
                    return Err(TransactionError::CorruptCatalog);
                }
                let policy_id: [u8; 16] = policy_key[3..19]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                let kind = catalog_u16(&record, 2)?;
                let operation = match catalog_u16(&record, 3)? {
                    1 => Operation::Select,
                    2 => Operation::Insert,
                    3 => Operation::Update,
                    4 => Operation::Delete,
                    _ => return Err(TransactionError::CorruptCatalog),
                };
                let effect = match catalog_u16(&record, 4)? {
                    1 => Effect::Allow,
                    2 => Effect::Restrict,
                    _ => return Err(TransactionError::CorruptCatalog),
                };
                if kind == 2 && operation == Operation::Delete {
                    return Err(TransactionError::CorruptCatalog);
                }
                let using_id = catalog_optional_id(&record, 5)?;
                let check_id = catalog_optional_id(&record, 6)?;
                match operation {
                    Operation::Select | Operation::Delete
                        if using_id.is_some() && check_id.is_none() => {}
                    Operation::Insert if using_id.is_none() && check_id.is_some() => {}
                    Operation::Update if using_id.is_some() && check_id.is_some() => {}
                    _ => return Err(TransactionError::CorruptCatalog),
                }

                let mut subjects = Vec::new();
                let mut policy_columns = Vec::new();
                let mut has_recipient = false;
                for target_record in targets.iter().filter(|target| {
                    target.logical_key.len() == 39 && target.logical_key[3..19] == policy_id
                }) {
                    let target_key = target_record.logical_key.clone();
                    let target_guard = self
                        .read_catalog_record(&target_key)?
                        .ok_or(TransactionError::CorruptCatalog)?;
                    if target_guard != *target_record {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    let target_kind = u16::from_be_bytes([target_key[20], target_key[21]]);
                    let target_id: [u8; 16] = target_key[23..39]
                        .try_into()
                        .map_err(|_| TransactionError::CorruptCatalog)?;
                    match target_kind {
                        1 | 2 => {
                            has_recipient = true;
                            let principal_record = self
                                .read_catalog_record(&catalog_id_key(10, target_id))?
                                .ok_or(TransactionError::CorruptCatalog)?;
                            let expected_kind = if target_kind == 1 { 1 } else { 2 };
                            let login_enabled = catalog_field(&principal_record, 3)?;
                            if catalog_u16(&principal_record, 1)? != expected_kind
                                || catalog_u64(&principal_record, 4)? == 0
                                || !matches!(login_enabled, [0] | [1])
                                || (expected_kind == 2 && login_enabled != [0])
                            {
                                return Err(TransactionError::CorruptCatalog);
                            }
                            if target_kind == 1 && target_id == principal_id {
                                subjects.push(Subject::Principal(principal_id));
                            } else if target_kind == 2 && roles.contains(&target_id) {
                                subjects.push(Subject::Role(target_id));
                            }
                        }
                        3 => {
                            if kind != 2
                                || operation == Operation::Delete
                                || !schema.column_ids.contains(&target_id)
                            {
                                return Err(TransactionError::CorruptCatalog);
                            }
                            let column_record = self
                                .read_catalog_record(&catalog_id_key(6, target_id))?
                                .ok_or(TransactionError::CorruptCatalog)?;
                            if catalog_id(&column_record, 1)? != schema.table_id {
                                return Err(TransactionError::CorruptCatalog);
                            }
                            policy_columns.push(target_id);
                        }
                        _ => return Err(TransactionError::CorruptCatalog),
                    }
                }
                if !has_recipient
                    || (kind == 1 && !policy_columns.is_empty())
                    || (kind == 2 && policy_columns.is_empty())
                    || !matches!(kind, 1 | 2)
                {
                    return Err(TransactionError::CorruptCatalog);
                }
                if subjects.is_empty() {
                    continue;
                }
                if subjects.len() > crate::authorization::MAX_POLICY_SUBJECTS {
                    return Err(TransactionError::CorruptCatalog);
                }
                let using = using_id
                    .map(|expression_id| {
                        self.read_bound_policy_expression(
                            expression_id,
                            ExpressionPurpose::PolicyUsing,
                            schema,
                            boolean_type_id,
                        )
                    })
                    .transpose()?;
                let with_check = check_id
                    .map(|expression_id| {
                        self.read_bound_policy_expression(
                            expression_id,
                            ExpressionPurpose::PolicyCheck,
                            schema,
                            boolean_type_id,
                        )
                    })
                    .transpose()?;
                let targets = if kind == 1 {
                    vec![AccessTarget::Table(schema.table_id)]
                } else {
                    policy_columns
                        .into_iter()
                        .map(|column_id| AccessTarget::Column {
                            table_id: schema.table_id,
                            column_id,
                        })
                        .collect()
                };
                for target in targets {
                    loaded.push(Policy {
                        subjects: subjects.clone(),
                        operation,
                        target,
                        effect,
                        using: using.clone(),
                        with_check: with_check.clone(),
                    });
                    if loaded.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS {
                        return Err(TransactionError::CorruptCatalog);
                    }
                }
            }
            Ok(loaded)
        })();
        match result {
            Ok(policies) => Ok(policies),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Load the table's pinned row constraints. CHECK expressions are bound to
    /// the catalog BOOLEAN type, and PRIMARY KEY/UNIQUE constraints are checked
    /// against their backing unique index. Unsupported kinds fail closed.
    pub fn load_catalog_check_constraints(
        &mut self,
        schema: &CatalogTableSchema,
    ) -> Result<Vec<Expr>, TransactionError> {
        let result = (|| {
            self.load_catalog_foreign_keys_for_table(schema)?;
            let records = self
                .pinned_snapshot()?
                .scan_catalog_family(8, MAX_CATALOG_FAMILY_SCAN)
                .map_err(|error| TransactionError::Storage(error.to_string()))?;
            let mut checks = Vec::new();
            for record in records {
                if catalog_id(&record, 1)? != schema.table_id {
                    continue;
                }
                let key = record.logical_key.clone();
                if key.len() != 19 {
                    return Err(TransactionError::CorruptCatalog);
                }
                let guarded = self
                    .read_catalog_record(&key)?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if guarded != record {
                    return Err(TransactionError::CorruptCatalog);
                }
                let kind = catalog_u16(&record, 2)?;
                let source_columns = catalog_id_list(&record, 3)?;
                let referenced_columns = catalog_id_list(&record, 6)?;
                let expression_id = catalog_optional_id(&record, 4)?;
                let constraint_id: [u8; 16] = key[3..19]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                if constraint_id == [0; 16] {
                    return Err(TransactionError::CorruptCatalog);
                }
                if kind == 1 {
                    if source_columns.len() != 1
                        || expression_id.is_some()
                        || catalog_optional_id(&record, 5)?.is_some()
                        || !referenced_columns.is_empty()
                        || catalog_u16(&record, 7)? != 0
                        || catalog_u16(&record, 8)? != 0
                        || catalog_u16(&record, 9)? != 0
                        || catalog_u16(&record, 10)? != 1
                        || catalog_optional_id(&record, 11)?.is_some()
                    {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    let Some(ordinal) = schema
                        .column_ids
                        .iter()
                        .position(|column_id| *column_id == source_columns[0])
                    else {
                        return Err(TransactionError::CorruptCatalog);
                    };
                    if schema.columns[ordinal].nullable {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    continue;
                }
                if matches!(kind, 2 | 3) {
                    let backing_index_id = catalog_optional_id(&record, 11)?
                        .ok_or(TransactionError::CorruptCatalog)?;
                    let mut seen_columns = HashSet::new();
                    if source_columns.is_empty()
                        || source_columns.len() > crate::tuple::MAX_COLUMNS
                        || source_columns.iter().any(|column_id| {
                            *column_id == [0; 16]
                                || !seen_columns.insert(*column_id)
                                || !schema.column_ids.contains(column_id)
                        })
                        || expression_id.is_some()
                        || catalog_optional_id(&record, 5)?.is_some()
                        || !referenced_columns.is_empty()
                        || catalog_u16(&record, 7)? != 0
                        || catalog_u16(&record, 8)? != 0
                        || catalog_u16(&record, 9)? != 0
                        || catalog_u16(&record, 10)? != 1
                        || backing_index_id == [0; 16]
                    {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    if kind == 2
                        && source_columns.iter().any(|column_id| {
                            let ordinal = schema
                                .column_ids
                                .iter()
                                .position(|schema_column_id| schema_column_id == column_id);
                            ordinal.is_none_or(|ordinal| schema.columns[ordinal].nullable)
                        })
                    {
                        return Err(TransactionError::CorruptCatalog);
                    }

                    let backing_index = self
                        .read_catalog_record(&catalog_id_key(9, backing_index_id))?
                        .ok_or(TransactionError::CorruptCatalog)?;
                    let index_columns = catalog_id_list(&backing_index, 2)?;
                    if backing_index.family_code != 9
                        || catalog_id(&backing_index, 1)? != schema.table_id
                        || index_columns != source_columns
                        || catalog_u16(&backing_index, 3)? != 1
                        || catalog_field(&backing_index, 4)? != [1]
                        || catalog_u16(&backing_index, 5)? != 1
                        || catalog_optional_id(&backing_index, 6)? != Some(constraint_id)
                        || catalog_id(&backing_index, 7)? == [0; 16]
                        || catalog_u16(&backing_index, 8)? != 1
                        || catalog_u32(&backing_index, 9)? != 1
                        || catalog_u64(&backing_index, 10)? != schema.schema_version
                    {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    continue;
                }
                if kind != 4 {
                    if kind == 5 {
                        continue;
                    }
                    return Err(TransactionError::CorruptCatalog);
                }
                let expression_id = expression_id.ok_or(TransactionError::CorruptCatalog)?;
                if !source_columns.is_empty()
                    || !referenced_columns.is_empty()
                    || catalog_optional_id(&record, 5)?.is_some()
                    || catalog_u16(&record, 7)? != 0
                    || catalog_u16(&record, 8)? != 0
                    || catalog_u16(&record, 9)? != 0
                    || catalog_u16(&record, 10)? != 1
                    || catalog_optional_id(&record, 11)?.is_some()
                {
                    return Err(TransactionError::CorruptCatalog);
                }
                let boolean_type_id = self.load_catalog_builtin_type_id(1)?;
                checks.push(self.read_bound_policy_expression(
                    expression_id,
                    ExpressionPurpose::Check,
                    schema,
                    boolean_type_id,
                )?);
                if checks.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS {
                    return Err(TransactionError::CorruptCatalog);
                }
            }
            Ok(checks)
        })();
        match result {
            Ok(checks) => Ok(checks),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Read and decode one schema-bound expression from the pinned catalog,
    /// retaining its exact bytes as a final-commit dependency guard.
    pub fn read_catalog_expression(
        &mut self,
        expression_id: [u8; 16],
    ) -> Result<Option<CatalogExpression>, TransactionError> {
        let mut key = 7u16.to_be_bytes().to_vec();
        key.push(1);
        key.extend_from_slice(&expression_id);
        let Some(record) = self.read_catalog_record(&key)? else {
            return Ok(None);
        };
        let expression = decode_catalog_expression_record(&record)
            .map_err(|_| TransactionError::CorruptCatalog)?;
        if expression.expression_id != expression_id {
            self.abort()?;
            return Err(TransactionError::CorruptCatalog);
        }
        Ok(Some(expression))
    }

    fn read_bound_policy_expression(
        &mut self,
        expression_id: [u8; 16],
        purpose: ExpressionPurpose,
        schema: &CatalogTableSchema,
        boolean_type_id: [u8; 16],
    ) -> Result<Expr, TransactionError> {
        let expression = self
            .read_catalog_expression(expression_id)?
            .ok_or(TransactionError::CorruptCatalog)?;
        if expression.purpose != purpose
            || (purpose == ExpressionPurpose::Check
                && !is_row_local_expression(&expression.expression))
        {
            return Err(TransactionError::CorruptCatalog);
        }
        validate_expression_binding(
            &expression,
            &schema.columns,
            &schema.column_ids,
            boolean_type_id,
            ScalarType::Boolean,
        )
        .map_err(|_| TransactionError::CorruptCatalog)?;
        Ok(expression.expression)
    }

    /// Load and schema-bind a bounded set of expressions from the pinned
    /// catalog. Each record must match its purpose, result type, and stable
    /// dependencies before its executable form is returned.
    pub fn read_catalog_bound_expressions(
        &mut self,
        expression_ids: &[[u8; 16]],
        expected_purpose: ExpressionPurpose,
        columns: &[ColumnSchema],
        column_ids: &[[u8; 16]],
        expected_result_type_ids: &[[u8; 16]],
        expected_result_types: &[ScalarType],
    ) -> Result<Vec<Expr>, TransactionError> {
        let result = (|| {
            if expression_ids.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS {
                return Err(TransactionError::InvalidContext);
            }
            if expression_ids.len() != expected_result_type_ids.len()
                || expression_ids.len() != expected_result_types.len()
            {
                return Err(TransactionError::InvalidContext);
            }
            let mut expressions = Vec::with_capacity(expression_ids.len());
            for (ordinal, expression_id) in expression_ids.iter().enumerate() {
                let expression = self
                    .read_catalog_expression(*expression_id)?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if expression.purpose != expected_purpose
                    || (expected_purpose == ExpressionPurpose::Check
                        && !is_row_local_expression(&expression.expression))
                {
                    return Err(TransactionError::CorruptCatalog);
                }
                validate_expression_binding(
                    &expression,
                    columns,
                    column_ids,
                    expected_result_type_ids[ordinal],
                    expected_result_types[ordinal],
                )
                .map_err(|_| TransactionError::CorruptCatalog)?;
                expressions.push(expression.expression);
            }
            Ok(expressions)
        })();
        match result {
            Ok(expressions) => Ok(expressions),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Materialize defaults by expression ID from the pinned catalog. Each
    /// expression must be a valid family-7 DEFAULT record; reading it also
    /// adds its exact bytes to the final-commit catalog guard set. Generated
    /// identities fail closed until storage can allocate and collision-check them.
    pub fn stage_insert_authorized_from_catalog_defaults(
        &mut self,
        id: RowId,
        projection: &AuthorizedProjection<'_>,
        inputs: &[InsertInput],
        schema: &CatalogTableSchema,
    ) -> Result<(), TransactionError> {
        let result = (|| {
            if schema.table_id != id.table
                || schema.schema_id != projection.schema_id
                || schema.schema_version != projection.schema_version
                || schema.columns != projection.columns
                || schema.column_ids != projection.column_ids
                || inputs.len() != schema.columns.len()
                || schema.default_expression_ids.len() != schema.columns.len()
                || schema.column_type_ids.len() != schema.columns.len()
                || schema.column_default_masks.len() != schema.columns.len()
                || schema.generation_modes.len() != schema.columns.len()
                || inputs.len() > crate::tuple::MAX_COLUMNS
            {
                return Err(TransactionError::InvalidContext);
            }
            if schema.generation_modes.iter().any(|&mode| mode != 0) {
                return Err(TransactionError::GeneratedIdentityUnsupported);
            }
            let mut defaults = Vec::with_capacity(inputs.len());
            for (ordinal, expression_id) in schema.default_expression_ids.iter().enumerate() {
                let Some(expression_id) = expression_id else {
                    defaults.push(None);
                    continue;
                };
                let expression = self
                    .read_catalog_expression(*expression_id)?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if expression.purpose != ExpressionPurpose::Default {
                    return Err(TransactionError::CorruptCatalog);
                }
                validate_expression_binding(
                    &expression,
                    &schema.columns,
                    &schema.column_ids,
                    schema.column_type_ids[ordinal],
                    schema.columns[ordinal].scalar_type,
                )
                .map_err(|_| TransactionError::CorruptCatalog)?;
                let allowed_default = match &expression.expression {
                    Expr::Literal(_) => schema.column_default_masks[ordinal] & 1 != 0,
                    Expr::CurrentPrincipalId => schema.column_default_masks[ordinal] & 4 != 0,
                    _ => false,
                };
                if !allowed_default {
                    return Err(TransactionError::CorruptCatalog);
                }
                defaults.push(Some(expression.expression));
            }
            self.stage_insert_authorized_with_defaults(id, projection, inputs, &defaults, &[])
        })();
        match result {
            Ok(()) => Ok(()),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    fn read_inner(&mut self, id: RowId) -> Result<Option<Vec<u8>>, TransactionError> {
        if self.aborted {
            return Err(TransactionError::Aborted);
        }
        let snapshot_row = self
            .pinned_snapshot()?
            .get(&row_key(id))
            .map_err(|error| TransactionError::Storage(error.to_string()))?
            .map(|bytes| RowRecord::decode(&bytes))
            .transpose()
            .map_err(TransactionError::CorruptRow)?;
        match self.overlay.read(&id, |_| {
            snapshot_row
                .as_ref()
                .map(|record| record.payload.as_slice())
        })? {
            Some(ResolvedRow::OverlayValue(bytes) | ResolvedRow::SnapshotValue(bytes)) => {
                Ok(Some(bytes.to_vec()))
            }
            None => Ok(None),
        }
    }

    pub fn stage_put(&mut self, id: RowId, payload: &[u8]) -> Result<(), TransactionError> {
        match self.stage_put_inner(id, payload) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.abort()?;
                Err(error)
            }
        }
    }

    /// Insert a fully materialized row after row INSERT policy validation and
    /// per-field INSERT checks. Trusted generated fields are exempt from cell
    /// grants/policies but remain subject to the row creation policy.
    pub fn stage_insert_authorized(
        &mut self,
        id: RowId,
        projection: &AuthorizedProjection<'_>,
        values: &[Option<crate::tuple::ScalarValue>],
        generated_columns: &[[u8; 16]],
    ) -> Result<(), TransactionError> {
        let result = self.stage_insert_authorized_inner(id, projection, values, generated_columns);
        match result {
            Ok(()) => Ok(()),
            Err(error) => {
                self.abort()?;
                Err(error)
            }
        }
    }

    /// Materialize explicit DEFAULT markers from trusted schema-bound
    /// expressions before applying the usual authorization and CHECK path.
    /// A missing expression means SQL NULL; tuple encoding enforces nullability.
    pub fn stage_insert_authorized_with_defaults(
        &mut self,
        id: RowId,
        projection: &AuthorizedProjection<'_>,
        inputs: &[InsertInput],
        defaults: &[Option<Expr>],
        generated_columns: &[[u8; 16]],
    ) -> Result<(), TransactionError> {
        let result = (|| {
            if inputs.len() != projection.columns.len()
                || defaults.len() != projection.columns.len()
                || inputs.len() > crate::tuple::MAX_COLUMNS
            {
                return Err(TransactionError::InvalidContext);
            }
            let mut values = Vec::with_capacity(inputs.len());
            for (input, default) in inputs.iter().zip(defaults) {
                values.push(match input {
                    InsertInput::Value(value) => value.clone(),
                    InsertInput::Default => match default {
                        Some(expression) => materialize_default(expression, projection.context)
                            .map_err(TransactionError::Authorization)?,
                        None => None,
                    },
                });
            }
            self.stage_insert_authorized(id, projection, &values, generated_columns)
        })();
        match result {
            Ok(()) => Ok(()),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    fn stage_insert_authorized_inner(
        &mut self,
        id: RowId,
        projection: &AuthorizedProjection<'_>,
        values: &[Option<crate::tuple::ScalarValue>],
        generated_columns: &[[u8; 16]],
    ) -> Result<(), TransactionError> {
        if self.aborted {
            return Err(TransactionError::Aborted);
        }
        self.capture_authority_dependencies(projection.context, projection.dependencies)?;
        let generated = generated_columns.iter().copied().collect::<HashSet<_>>();
        if projection.columns.len() != projection.column_ids.len()
            || projection.columns.len() != values.len()
            || generated.len() != generated_columns.len()
            || generated
                .iter()
                .any(|column_id| !projection.column_ids.contains(column_id))
        {
            return Err(TransactionError::InvalidContext);
        }
        let table = AccessTarget::Table(id.table);
        let schema = AccessTarget::Schema(projection.schema_id);
        if !has_permission(
            projection.context,
            projection.grants,
            Operation::Usage,
            schema,
        ) {
            return Err(TransactionError::Authorization(AuthorizationError::Denied));
        }
        if !has_permission(
            projection.context,
            projection.grants,
            Operation::Insert,
            table,
        ) {
            return Err(TransactionError::Authorization(AuthorizationError::Denied));
        }
        for &column_id in projection.column_ids {
            if generated.contains(&column_id) {
                continue;
            }
            let target = AccessTarget::Column {
                table_id: id.table,
                column_id,
            };
            if !has_permission(
                projection.context,
                projection.grants,
                Operation::Insert,
                target,
            ) {
                return Err(TransactionError::Authorization(AuthorizationError::Denied));
            }
        }
        let payload = encode_tuple(projection.columns, projection.schema_version, values)
            .map_err(TransactionError::Tuple)?;
        authorize(
            projection.context,
            projection.grants,
            projection.policies,
            Operation::Insert,
            table,
            None,
            Some(values),
        )
        .map_err(TransactionError::Authorization)?;
        validate_check_constraints(projection, values)?;
        for &column_id in projection.column_ids {
            if generated.contains(&column_id) {
                continue;
            }
            let target = AccessTarget::Column {
                table_id: id.table,
                column_id,
            };
            authorize(
                projection.context,
                projection.grants,
                projection.policies,
                Operation::Insert,
                target,
                None,
                Some(values),
            )
            .map_err(TransactionError::Authorization)?;
        }
        if self.read_inner(id)?.is_some() || self.capture_base_version(id)?.is_some() {
            return Err(TransactionError::RowAlreadyExists);
        }
        self.stage_put_inner(id, &payload)
    }

    fn stage_put_inner(&mut self, id: RowId, payload: &[u8]) -> Result<(), TransactionError> {
        if self.aborted {
            return Err(TransactionError::Aborted);
        }
        if self.active_scan.is_some() {
            return Err(TransactionError::ScanStillActive);
        }
        self.capture_base_version(id)?;
        self.overlay.put(id, payload)?;
        Ok(())
    }

    pub fn stage_delete(&mut self, id: RowId) -> Result<(), TransactionError> {
        match self.stage_delete_inner(id) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.abort()?;
                Err(error)
            }
        }
    }

    /// Delete a row only when it is visible and the pinned table DELETE policy
    /// and explicit DELETE grant pass. Cell readability is not required.
    pub fn stage_delete_authorized(
        &mut self,
        id: RowId,
        projection: &AuthorizedProjection<'_>,
    ) -> Result<(), TransactionError> {
        let result = self.stage_delete_authorized_inner(id, projection);
        match result {
            Ok(()) => Ok(()),
            Err(error) => {
                self.abort()?;
                Err(error)
            }
        }
    }

    fn stage_delete_authorized_inner(
        &mut self,
        id: RowId,
        projection: &AuthorizedProjection<'_>,
    ) -> Result<(), TransactionError> {
        if self.aborted {
            return Err(TransactionError::Aborted);
        }
        self.capture_authority_dependencies(projection.context, projection.dependencies)?;
        let table = AccessTarget::Table(id.table);
        if !has_permission(
            projection.context,
            projection.grants,
            Operation::Delete,
            table,
        ) {
            return Err(TransactionError::Authorization(AuthorizationError::Denied));
        }
        let payload = self.read_inner(id)?.ok_or(TransactionError::NotFound)?;
        let tuple = decode_tuple(projection.columns, projection.schema_version, &payload)
            .map_err(TransactionError::Tuple)?;
        if !row_visible(
            projection.context,
            projection.grants,
            projection.policies,
            projection.schema_id,
            id.table,
            projection.column_ids,
            &tuple.values,
        )
        .map_err(TransactionError::Authorization)?
        {
            return Err(TransactionError::NotFound);
        }
        authorize(
            projection.context,
            projection.grants,
            projection.policies,
            Operation::Delete,
            table,
            Some(&tuple.values),
            None,
        )
        .map_err(TransactionError::Authorization)?;
        self.stage_delete_inner(id)
    }

    /// Stage a schema-derived index membership change for an already staged row.
    /// The caller must provide the complete key; trusted storage still owns
    /// catalog validation, uniqueness, and index-delta completeness.
    pub fn stage_index_membership(
        &mut self,
        id: RowId,
        key: Vec<u8>,
        present: bool,
    ) -> Result<(), TransactionError> {
        self.stage_index_membership_with_semantics(id, key, present, false, false)
    }

    /// Stage membership with catalog-derived uniqueness and NULL semantics.
    pub fn stage_index_membership_with_semantics(
        &mut self,
        id: RowId,
        key: Vec<u8>,
        present: bool,
        unique: bool,
        null_exempt: bool,
    ) -> Result<(), TransactionError> {
        match self.stage_index_membership_inner(id, key, present, unique, null_exempt) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.abort()?;
                Err(error)
            }
        }
    }

    fn stage_index_membership_inner(
        &mut self,
        id: RowId,
        key: Vec<u8>,
        present: bool,
        unique: bool,
        null_exempt: bool,
    ) -> Result<(), TransactionError> {
        if self.aborted {
            return Err(TransactionError::Aborted);
        }
        if self.active_scan.is_some() {
            return Err(TransactionError::ScanStillActive);
        }
        match self.overlay.get(&id)? {
            Some(StagedRow::Value(_)) => {}
            Some(StagedRow::Delete) if !present => {}
            _ => return Err(TransactionError::IndexRowNotStaged),
        }
        if key.len() < 32 || (unique && key.len() == 32) || key[key.len() - 16..] != id.row {
            return Err(TransactionError::Commit(CommitError::InvalidIndex));
        }
        if self
            .index_writes
            .get(&key)
            .is_some_and(|write| write.row_id != id)
        {
            return Err(TransactionError::Commit(CommitError::InvalidIndex));
        }
        let expected_version = self
            .pinned_snapshot()?
            .get_index_version(&key)
            .map_err(|error| TransactionError::Storage(error.to_string()))?;
        if !present && expected_version.is_none() {
            if self
                .index_writes
                .get(&key)
                .is_some_and(|write| write.present)
            {
                self.index_writes.remove(&key);
                self.index_bytes -= key.len();
                return Ok(());
            }
            return Err(TransactionError::Commit(CommitError::InvalidIndex));
        }
        let old_len = self.index_writes.get(&key).map_or(0, |_| key.len());
        let next_bytes = self
            .index_bytes
            .checked_sub(old_len)
            .and_then(|bytes| bytes.checked_add(key.len()))
            .filter(|&bytes| bytes <= self.max_index_bytes)
            .ok_or(TransactionError::IndexOverlayLimitExceeded)?;
        if !self.index_writes.contains_key(&key)
            && self.index_writes.len() >= self.max_index_changes
        {
            return Err(TransactionError::IndexOverlayLimitExceeded);
        }
        self.index_writes.insert(
            key,
            StagedIndexChange {
                row_id: id,
                expected_version,
                present,
                unique,
                null_exempt,
            },
        );
        self.index_bytes = next_bytes;
        Ok(())
    }

    /// Merge validated field changes into the pinned row while retaining untouched fields.
    /// Caller-side authorization must restrict which ordinals may be patched before this is
    /// exposed to any executor request path.
    pub fn stage_patch(
        &mut self,
        id: RowId,
        columns: &[ColumnSchema],
        schema_version: u64,
        patches: &[FieldPatch],
    ) -> Result<(), TransactionError> {
        match self.stage_patch_inner(id, columns, schema_version, patches) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.abort()?;
                Err(error)
            }
        }
    }

    /// Apply a cell patch only after checking the pinned row and cell policies
    /// against both old and complete proposed rows. A dependency-driving field
    /// is immutable to query updates.
    pub fn stage_patch_authorized(
        &mut self,
        id: RowId,
        projection: &AuthorizedProjection<'_>,
        patches: &[FieldPatch],
    ) -> Result<(), TransactionError> {
        let result = self.stage_patch_authorized_inner(id, projection, patches);
        match result {
            Ok(()) => Ok(()),
            Err(error) => {
                self.abort()?;
                Err(error)
            }
        }
    }

    fn stage_patch_authorized_inner(
        &mut self,
        id: RowId,
        projection: &AuthorizedProjection<'_>,
        patches: &[FieldPatch],
    ) -> Result<(), TransactionError> {
        if self.aborted {
            return Err(TransactionError::Aborted);
        }
        self.capture_authority_dependencies(projection.context, projection.dependencies)?;
        if projection.columns.len() != projection.column_ids.len()
            || patches.iter().any(|patch| {
                projection
                    .column_ids
                    .get(patch.ordinal)
                    .is_none_or(|column_id| projection.policy_driving_columns.contains(column_id))
            })
        {
            return Err(TransactionError::Authorization(AuthorizationError::Denied));
        }
        let table = AccessTarget::Table(id.table);
        if !has_permission(
            projection.context,
            projection.grants,
            Operation::Update,
            table,
        ) {
            return Err(TransactionError::Authorization(AuthorizationError::Denied));
        }
        let old_payload = self.read_inner(id)?.ok_or(TransactionError::NotFound)?;
        let old_tuple = decode_tuple(projection.columns, projection.schema_version, &old_payload)
            .map_err(TransactionError::Tuple)?;
        if !row_visible(
            projection.context,
            projection.grants,
            projection.policies,
            projection.schema_id,
            id.table,
            projection.column_ids,
            &old_tuple.values,
        )
        .map_err(TransactionError::Authorization)?
        {
            return Err(TransactionError::NotFound);
        }
        for patch in patches {
            let target = AccessTarget::Column {
                table_id: id.table,
                column_id: projection.column_ids[patch.ordinal],
            };
            if !has_permission(
                projection.context,
                projection.grants,
                Operation::Select,
                target,
            ) || !has_permission(
                projection.context,
                projection.grants,
                Operation::Update,
                target,
            ) {
                return Err(TransactionError::Authorization(AuthorizationError::Denied));
            }
            authorize(
                projection.context,
                projection.grants,
                projection.policies,
                Operation::Select,
                target,
                Some(&old_tuple.values),
                None,
            )
            .map_err(TransactionError::Authorization)?;
        }
        let mut new_tuple = old_tuple.clone();
        apply_patch(projection.columns, &mut new_tuple, patches)
            .map_err(TransactionError::Tuple)?;
        validate_check_constraints(projection, &new_tuple.values)?;
        let new_payload = encode_tuple(
            projection.columns,
            projection.schema_version,
            &new_tuple.values,
        )
        .map_err(TransactionError::Tuple)?;
        authorize(
            projection.context,
            projection.grants,
            projection.policies,
            Operation::Update,
            table,
            Some(&old_tuple.values),
            Some(&new_tuple.values),
        )
        .map_err(TransactionError::Authorization)?;
        for patch in patches {
            let target = AccessTarget::Column {
                table_id: id.table,
                column_id: projection.column_ids[patch.ordinal],
            };
            authorize(
                projection.context,
                projection.grants,
                projection.policies,
                Operation::Select,
                target,
                Some(&old_tuple.values),
                None,
            )
            .map_err(TransactionError::Authorization)?;
            authorize(
                projection.context,
                projection.grants,
                projection.policies,
                Operation::Update,
                target,
                Some(&old_tuple.values),
                Some(&new_tuple.values),
            )
            .map_err(TransactionError::Authorization)?;
        }
        self.stage_put_inner(id, &new_payload)?;
        Ok(())
    }

    fn stage_patch_inner(
        &mut self,
        id: RowId,
        columns: &[ColumnSchema],
        schema_version: u64,
        patches: &[FieldPatch],
    ) -> Result<(), TransactionError> {
        if self.aborted {
            return Err(TransactionError::Aborted);
        }
        if self.active_scan.is_some() {
            return Err(TransactionError::ScanStillActive);
        }
        let payload = self.read_inner(id)?.ok_or(TransactionError::NotFound)?;
        let mut tuple =
            decode_tuple(columns, schema_version, &payload).map_err(TransactionError::Tuple)?;
        apply_patch(columns, &mut tuple, patches).map_err(TransactionError::Tuple)?;
        let merged = encode_tuple(columns, schema_version, &tuple.values)
            .map_err(TransactionError::Tuple)?;
        self.stage_put_inner(id, &merged)
    }

    fn stage_delete_inner(&mut self, id: RowId) -> Result<(), TransactionError> {
        if self.aborted {
            return Err(TransactionError::Aborted);
        }
        if self.active_scan.is_some() {
            return Err(TransactionError::ScanStillActive);
        }
        let base_version = self.capture_base_version(id)?;
        let current = self.overlay.get(&id)?;
        if matches!(current, Some(StagedRow::Delete)) {
            return Err(TransactionError::NotFound);
        }
        if base_version.is_none() {
            if current.is_some() {
                self.overlay.remove(&id)?;
                self.base_versions.remove(&id);
                self.index_writes.retain(|_, write| write.row_id != id);
                self.index_bytes = self.index_writes.keys().map(Vec::len).sum();
                return Ok(());
            }
            return Err(TransactionError::NotFound);
        }
        self.overlay.delete(id)?;
        Ok(())
    }

    /// Roll back all staged work and make this invocation terminal.
    pub fn abort(&mut self) -> Result<(), TransactionError> {
        self.overlay.abort();
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
        self.aborted = true;
        self.snapshot.take();
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
    pub fn commit(mut self) -> Result<u64, TransactionError> {
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
            if !self.coordination.mark_committing() {
                return Err(CommitError::InvocationCancelled);
            }
            commit_storage_changes(
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
            )
        });
        match result {
            Ok(sequence) => Ok(sequence),
            Err(
                error @ (CommitError::Storage(_)
                | CommitError::OutcomeAlreadyExists
                | CommitError::UnsupportedFormat),
            ) => Err(error.into()),
            Err(error) => {
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

    fn capture_base_version(&mut self, id: RowId) -> Result<Option<u64>, TransactionError> {
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

    fn capture_authority_dependencies(
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

fn valid_estimate_projection(table_id: [u8; 16], projection: &AuthorizedProjection<'_>) -> bool {
    table_id != [0; 16]
        && !projection.columns.is_empty()
        && projection.columns.len() == projection.column_ids.len()
        && projection.columns.len() <= crate::tuple::MAX_COLUMNS
        && projection.schema_version != 0
        && projection.schema_id != [0; 16]
        && projection.column_ids.iter().all(|id| id != &[0; 16])
        && projection
            .column_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>()
            .len()
            == projection.column_ids.len()
        && projection.grants.len() <= crate::authorization::MAX_AUTHORIZATION_INPUTS
        && projection.policies.len() <= crate::authorization::MAX_AUTHORIZATION_INPUTS
        && projection.checks.len() <= crate::authorization::MAX_AUTHORIZATION_INPUTS
        && projection.policy_driving_columns.len() <= crate::authorization::MAX_AUTHORIZATION_INPUTS
        && projection.dependencies.len() <= crate::authorization::MAX_AUTHORIZATION_INPUTS
        && projection.context.principal_id != [0; 16]
        && projection.context.role_ids.len() <= crate::authorization::MAX_AUTHORIZATION_INPUTS
        && projection.context.scope.len() <= crate::authorization::MAX_AUTHORIZATION_INPUTS
        && projection
            .context
            .role_ids
            .iter()
            .all(|role_id| role_id != &[0; 16])
        && projection
            .policies
            .iter()
            .all(|policy| policy.subjects.len() <= crate::authorization::MAX_POLICY_SUBJECTS)
}

/// Exact counts are released only when row inclusion is independent of row
/// values. Otherwise a work-limit result could reveal the number of invisible
/// candidates through whether the scan completed.
fn estimate_population_is_unfiltered(
    table_id: [u8; 16],
    projection: &AuthorizedProjection<'_>,
) -> bool {
    let context = projection.context;
    if !has_permission(
        context,
        projection.grants,
        Operation::Usage,
        AccessTarget::Schema(projection.schema_id),
    ) || !context
        .scope
        .contains(&(Operation::Select, AccessTarget::Table(table_id)))
        || !projection.column_ids.iter().any(|&column_id| {
            has_permission(
                context,
                projection.grants,
                Operation::Select,
                AccessTarget::Column {
                    table_id,
                    column_id,
                },
            )
        })
    {
        return false;
    }

    let mut has_allow = false;
    let mut has_applicable_policy = false;
    for policy in projection.policies.iter().filter(|policy| {
        policy.operation == Operation::Select
            && policy.target == AccessTarget::Table(table_id)
            && policy.subjects.iter().any(|subject| match subject {
                Subject::Principal(principal_id) => *principal_id == context.principal_id,
                Subject::Role(role_id) => context.role_ids.contains(role_id),
            })
    }) {
        has_applicable_policy = true;
        if !matches!(
            policy.using.as_ref(),
            Some(Expr::Literal(Some(ScalarValue::Boolean(true))))
        ) || policy.with_check.is_some()
        {
            return false;
        }
        has_allow |= policy.effect == Effect::Allow;
    }
    has_applicable_policy && has_allow
}

fn estimate_error_is_terminal(error: &TransactionError) -> bool {
    matches!(
        error,
        TransactionError::Expired
            | TransactionError::CancellationRequested
            | TransactionError::Aborted
            | TransactionError::Commit(CommitError::InvocationCancelled)
    )
}

fn derive_catalog_index_key(
    schema: &CatalogTableSchema,
    index: &CatalogIndexDefinition,
    row_id: [u8; 16],
    payload: &[u8],
) -> Result<crate::index_key::DerivedIndexKey, CommitError> {
    derive_index_key(
        index.index_id,
        row_id,
        schema.schema_version,
        &schema.column_ids,
        &schema.columns,
        payload,
        &index.column_ids,
    )
    .map_err(|error| match error {
        DeriveIndexKeyError::InvalidDefinition => CommitError::InvalidCatalog,
        DeriveIndexKeyError::Tuple(_) => CommitError::InvalidRow,
    })
}

fn validate_catalog_index_deltas(
    snapshot: &RowSnapshot,
    loaded_schemas: &HashMap<[u8; 16], CatalogTableSchema>,
    loaded_indexes: &HashMap<[u8; 16], Vec<CatalogIndexDefinition>>,
    staged_indexes: &HashMap<Vec<u8>, StagedIndexChange>,
    row_writes: &[RowWrite],
    max_index_changes: usize,
    max_index_bytes: usize,
) -> Result<Vec<IndexWrite>, CommitError> {
    for row_write in row_writes {
        let table_id: [u8; 16] = row_write.key[..16]
            .try_into()
            .map_err(|_| CommitError::InvalidWrite)?;
        if loaded_schemas.contains_key(&table_id) && !loaded_indexes.contains_key(&table_id) {
            return Err(CommitError::InvalidIndex);
        }
        if let Some(schema) = loaded_schemas.get(&table_id) {
            if schema.table_id != table_id {
                return Err(CommitError::InvalidCatalog);
            }
            if let Some(payload) = row_write.payload.as_deref() {
                decode_tuple(&schema.columns, schema.schema_version, payload)
                    .map_err(|_| CommitError::InvalidRow)?;
            }
        }
    }
    let tracked_tables = loaded_indexes.keys().copied().collect::<HashSet<_>>();
    let mut expected = HashMap::<Vec<u8>, IndexWrite>::new();

    for row_write in row_writes {
        let table_id: [u8; 16] = row_write.key[..16]
            .try_into()
            .map_err(|_| CommitError::InvalidWrite)?;
        let Some(indexes) = loaded_indexes.get(&table_id) else {
            continue;
        };
        let schema = loaded_schemas
            .get(&table_id)
            .ok_or(CommitError::InvalidCatalog)?;
        let row_id: [u8; 16] = row_write.key[16..]
            .try_into()
            .map_err(|_| CommitError::InvalidWrite)?;
        let previous = snapshot
            .get(&row_write.key)
            .map_err(|error| CommitError::Storage(error.to_string()))?
            .map(|bytes| RowRecord::decode(&bytes).map_err(|_| CommitError::InvalidRow))
            .transpose()?;
        if previous.as_ref().map(|record| record.row_version) != row_write.expected_version {
            return Err(CommitError::Conflict);
        }
        if let Some(record) = &previous {
            decode_tuple(&schema.columns, schema.schema_version, &record.payload)
                .map_err(|_| CommitError::InvalidRow)?;
        }

        for index in indexes {
            if index.table_id != table_id {
                return Err(CommitError::InvalidCatalog);
            }
            let old = previous
                .as_ref()
                .map(|record| derive_catalog_index_key(schema, index, row_id, &record.payload))
                .transpose()?;
            let new = row_write
                .payload
                .as_deref()
                .map(|payload| derive_catalog_index_key(schema, index, row_id, payload))
                .transpose()?;
            if old.as_ref().map(|derived| &derived.key) == new.as_ref().map(|derived| &derived.key)
            {
                if let Some(record) = &previous {
                    let version = snapshot
                        .get_index_version(&old.as_ref().ok_or(CommitError::InvalidIndex)?.key)
                        .map_err(|error| CommitError::Storage(error.to_string()))?;
                    if version != Some(record.row_version) {
                        return Err(CommitError::InvalidIndex);
                    }
                }
                continue;
            }

            if let (Some(record), Some(derived)) = (&previous, &old) {
                let version = snapshot
                    .get_index_version(&derived.key)
                    .map_err(|error| CommitError::Storage(error.to_string()))?;
                if version != Some(record.row_version) {
                    return Err(CommitError::InvalidIndex);
                }
                let write = IndexWrite {
                    key: derived.key.clone(),
                    row_key: row_write.key,
                    expected_version: version,
                    present: false,
                    unique: index.unique,
                    null_exempt: index.unique && derived.has_null,
                };
                if expected.insert(write.key.clone(), write).is_some() {
                    return Err(CommitError::InvalidIndex);
                }
            }
            if let Some(derived) = &new {
                let version = snapshot
                    .get_index_version(&derived.key)
                    .map_err(|error| CommitError::Storage(error.to_string()))?;
                if version.is_some() {
                    return Err(CommitError::InvalidIndex);
                }
                let write = IndexWrite {
                    key: derived.key.clone(),
                    row_key: row_write.key,
                    expected_version: None,
                    present: true,
                    unique: index.unique,
                    null_exempt: index.unique && derived.has_null,
                };
                if expected.insert(write.key.clone(), write).is_some() {
                    return Err(CommitError::InvalidIndex);
                }
            }
        }
    }

    let mut untracked_writes = Vec::new();
    for (key, staged) in staged_indexes {
        if !tracked_tables.contains(&staged.row_id.table) {
            untracked_writes.push(IndexWrite {
                key: key.clone(),
                row_key: row_key(staged.row_id),
                expected_version: staged.expected_version,
                present: staged.present,
                unique: staged.unique,
                null_exempt: staged.null_exempt,
            });
            continue;
        }
        let write = IndexWrite {
            key: key.clone(),
            row_key: row_key(staged.row_id),
            expected_version: staged.expected_version,
            present: staged.present,
            unique: staged.unique,
            null_exempt: staged.null_exempt,
        };
        if expected.get(key) != Some(&write) {
            return Err(CommitError::InvalidIndex);
        }
    }
    let mut writes = expected.into_values().collect::<Vec<_>>();
    writes.extend(untracked_writes);
    let total_bytes = writes
        .iter()
        .try_fold(0usize, |total, write| total.checked_add(write.key.len()));
    if writes.len() > max_index_changes || total_bytes.is_none_or(|bytes| bytes > max_index_bytes) {
        return Err(CommitError::IndexOverlayLimitExceeded);
    }
    Ok(writes)
}

fn catalog_field(record: &CatalogRecord, tag: u16) -> Result<&[u8], TransactionError> {
    record
        .fields
        .iter()
        .find(|field| field.tag == tag)
        .map(|field| field.payload.as_slice())
        .ok_or(TransactionError::CorruptCatalog)
}

fn catalog_u16(record: &CatalogRecord, tag: u16) -> Result<u16, TransactionError> {
    Ok(u16::from_le_bytes(
        catalog_field(record, tag)?
            .try_into()
            .map_err(|_| TransactionError::CorruptCatalog)?,
    ))
}

fn catalog_u32(record: &CatalogRecord, tag: u16) -> Result<u32, TransactionError> {
    Ok(u32::from_le_bytes(
        catalog_field(record, tag)?
            .try_into()
            .map_err(|_| TransactionError::CorruptCatalog)?,
    ))
}

fn catalog_u64(record: &CatalogRecord, tag: u16) -> Result<u64, TransactionError> {
    Ok(u64::from_le_bytes(
        catalog_field(record, tag)?
            .try_into()
            .map_err(|_| TransactionError::CorruptCatalog)?,
    ))
}

fn catalog_id(record: &CatalogRecord, tag: u16) -> Result<[u8; 16], TransactionError> {
    catalog_field(record, tag)?
        .try_into()
        .map_err(|_| TransactionError::CorruptCatalog)
}

fn catalog_optional_id(
    record: &CatalogRecord,
    tag: u16,
) -> Result<Option<[u8; 16]>, TransactionError> {
    record
        .fields
        .iter()
        .find(|field| field.tag == tag)
        .map(|field| {
            field
                .payload
                .as_slice()
                .try_into()
                .map_err(|_| TransactionError::CorruptCatalog)
        })
        .transpose()
}

fn catalog_id_list(record: &CatalogRecord, tag: u16) -> Result<Vec<[u8; 16]>, TransactionError> {
    let bytes = catalog_field(record, tag)?;
    if bytes.len() < 4 {
        return Err(TransactionError::CorruptCatalog);
    }
    let count = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
    if count > crate::authorization::MAX_AUTHORIZATION_INPUTS || bytes.len() != 4 + count * 16 {
        return Err(TransactionError::CorruptCatalog);
    }
    bytes[4..]
        .chunks_exact(16)
        .map(|id| id.try_into().map_err(|_| TransactionError::CorruptCatalog))
        .collect()
}

fn new_metadata_cursor_token(
    used_tokens: &mut HashSet<[u8; 16]>,
) -> Result<[u8; 16], TransactionError> {
    for _ in 0..8 {
        let mut token = [0; 16];
        getrandom::fill(&mut token).map_err(|_| TransactionError::RandomnessUnavailable)?;
        if token != [0; 16] && used_tokens.insert(token) {
            return Ok(token);
        }
    }
    Err(TransactionError::RandomnessUnavailable)
}

fn catalog_id_key(family: u16, id: [u8; 16]) -> Vec<u8> {
    let mut key = family.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&id);
    key
}

fn catalog_grant_key(
    grantee_id: [u8; 16],
    object_id: [u8; 16],
    privilege: u16,
    target_scope: u16,
    target_id: [u8; 16],
) -> Vec<u8> {
    let mut key = 12u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&grantee_id);
    key.push(1);
    key.extend_from_slice(&object_id);
    key.push(1);
    key.extend_from_slice(&privilege.to_be_bytes());
    key.push(1);
    key.extend_from_slice(&target_scope.to_be_bytes());
    key.push(1);
    key.extend_from_slice(&target_id);
    key
}

fn decode_catalog_scalar_type(
    type_record: &CatalogRecord,
    column_record: &CatalogRecord,
) -> Result<ScalarType, TransactionError> {
    decode_catalog_scalar_type_parameters(type_record, catalog_field(column_record, 5)?)
}

fn decode_catalog_scalar_type_parameters(
    type_record: &CatalogRecord,
    parameters: &[u8],
) -> Result<ScalarType, TransactionError> {
    decode_builtin_scalar_type(type_record, parameters)
        .map_err(|_| TransactionError::CorruptCatalog)
}

fn validate_check_constraints(
    projection: &AuthorizedProjection<'_>,
    values: &[Option<crate::tuple::ScalarValue>],
) -> Result<(), TransactionError> {
    if projection.checks.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS {
        return Err(TransactionError::InvalidContext);
    }
    for expression in projection.checks {
        if !check_constraint_passes(expression, values, projection.context)
            .map_err(TransactionError::Authorization)?
        {
            return Err(TransactionError::ConstraintViolation);
        }
    }
    Ok(())
}

fn row_key(id: RowId) -> [u8; 32] {
    let mut key = [0; 32];
    key[..16].copy_from_slice(&id.table);
    key[16..].copy_from_slice(&id.row);
    key
}

fn map_snapshot_scan_error(error: SnapshotError) -> TransactionError {
    match error {
        SnapshotError::InvalidTableId
        | SnapshotError::InvalidCursor
        | SnapshotError::InvalidScanLimit => TransactionError::InvalidScanRequest,
        SnapshotError::InvalidRowKey => TransactionError::CorruptRowKey,
        SnapshotError::Storage(error) => TransactionError::Storage(error),
    }
}
