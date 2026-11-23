//! One invocation's pinned snapshot and private row overlay.
use std::cell::Cell;

use crate::authorization::{
    AccessTarget, AuthorityDependency, AuthorizationContext, AuthorizationError,
    AuthorizedProjection, Effect, Expr, Grant, Membership, Operation, Policy, SanitizedCell,
    Subject, authorize, check_constraint_passes, has_permission, materialize_default, project_row,
    resolve_role_closure, row_visible,
};
use crate::catalog::{CatalogRecord, decode_builtin_scalar_type, decode_registered_envelope};
use crate::commit::{
    CatalogGuard, CommitError, CommitRequest, ForeignKeyCheck, IndexWrite, RowWrite,
    commit_storage_changes_with_gate, record_aborted_outcome,
};
use crate::credentials::{TOKEN_SECRET_BYTES, decode_verifier_record};
use crate::ddl_fence::DdlFenceCoordinator;
use crate::expression::{
    CatalogExpression, ExpressionPurpose, decode_catalog_expression_record,
    is_row_local_expression, validate_expression_binding,
};
use crate::index_key::{DeriveIndexKeyError, derive_index_key, encode_sql_tuple};
use crate::metadata::{EngineMetadata, Lifecycle};
use crate::metadata_discovery::{
    AuthorizedPlanRelation, AuthorizedRangeAccessPath, AuthorizedRelationDescription,
    AuthorizedUniqueAccessPath, COLUMN_CURSOR_TOKEN_KIND, ColumnScanCursor,
    EXECUTOR_ROW_HANDLE_TOKEN_KIND, EXECUTOR_SCAN_CURSOR_TOKEN_KIND, ExecutorRowHandle,
    ExecutorScanCursor, INFORMATION_SCHEMA_COLUMNS_CURSOR_TOKEN_KIND,
    INFORMATION_SCHEMA_TABLES_CURSOR_TOKEN_KIND, InformationSchemaColumnRow,
    InformationSchemaColumnsCursor, InformationSchemaColumnsPage, InformationSchemaColumnsScanPage,
    InformationSchemaSchemataPage, InformationSchemaSchemataRow, InformationSchemaTableRow,
    InformationSchemaTablesCursor, InformationSchemaTablesPage, InformationSchemaTablesScanPage,
    MAX_COLUMNS_PER_PAGE, MAX_EXECUTOR_BATCH_CANDIDATES, MAX_EXECUTOR_ROW_HANDLES,
    MAX_METADATA_PAGE_BYTES, MAX_RELATION_HANDLES, MAX_RELATIONS_PER_PAGE, MAX_SCHEMAS_PER_PAGE,
    PLANNING_TABLE_HANDLE_TOKEN_KIND, PlanningTableHandle, RELATION_CURSOR_TOKEN_KIND,
    RELATION_HANDLE_TOKEN_KIND, RelationScanCursor, ResolvedRelationHandle,
    SCHEMA_CURSOR_TOKEN_KIND, SchemaScanCursor, VisibleColumn, VisibleColumnPage, VisibleRelation,
    VisibleRelationPage, VisibleRelationSchema, VisibleSchema, VisibleSchemaPage,
};
use crate::outcome::{InvocationOutcome, OutcomeStatus};
use crate::overlay::{Overlay, OverlayError, ResolvedRow, RowId, StagedRow};
use crate::row_record::{RowRecord, RowRecordError};
use crate::schema::{ENGINE_META, OUTCOMES, PRINCIPAL_NAMES};
use crate::snapshot::{MAX_CATALOG_FAMILY_SCAN, MAX_ROW_SCAN_PAGE, RowSnapshot, SnapshotError};
use crate::statistics::{
    AuthorizedScanEstimate, AuthorizedScanSummary, ColumnPopulation, MAX_ESTIMATE_CANDIDATES,
    MAX_PREDICATE_ESTIMATE_ROWS,
};
use crate::storage_manager::{InvocationAdmissionMetrics, InvocationAdmissionPermit};
use crate::tuple::{
    ColumnSchema, FieldPatch, ScalarType, ScalarValue, TupleError, apply_patch, decode_tuple,
    encode_tuple,
};
use redb::{Database, ReadableDatabase};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

const MAX_INDEX_CHANGES_PER_ROW: usize = 256;
pub const MAX_INVOCATION_SCAN_WORK: usize = 65_536;
/// Authorized estimate requests admitted on one planner connection before the
/// session switches to fixed optimizer heuristics.
pub const MAX_PLANNER_ESTIMATE_REQUESTS_PER_CONNECTION: usize = 16;
const INFORMATION_SCHEMA_ID: [u8; 16] = crate::metadata_discovery::INFORMATION_SCHEMA_SCHEMA_ID;
const INFORMATION_SCHEMA_COLUMN_EXTRA_BYTES: usize =
    "TIMESTAMP WITH TIME ZONE".len() + "YES".len() + 4 + 3 * 5 + 16;

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
    AuditCapacityExceeded,
    AdmissionLimitExceeded,
    Expired,
    CancellationRequested,
    RandomnessUnavailable,
    IndexRowNotStaged,
    RowAlreadyExists,
    ConstraintViolation,
    IndexOverlayLimitExceeded,
    InvalidScanRequest,
    ScanBudgetExceeded,
    CorruptRowKey,
    ScanStillActive,
    CorruptCatalog,
    InvalidMetadataRequest,
    RelationHandleLimitExceeded,
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
/// snapshot. The containing table and referenced column/type records are
/// retained as commit guards when the invocation publishes row or index writes.
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

/// Result of a catalog-validated unique equality probe. `Unavailable` means
/// the caller must use the authorized reference scan because a safe index
/// rewrite could not be proven from the pinned permissions and policies.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AuthorizedUniqueIndexProbe {
    Unavailable,
    NoMatch,
    Match {
        id: RowId,
        cells: Vec<SanitizedCell>,
    },
}

/// One typed lower or upper bound for a catalog index range scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorizedIndexRangeBound {
    pub value: ScalarValue,
    pub inclusive: bool,
}

/// Result of a catalog-validated, bounded range scan. `Unavailable` requires
/// the caller to use the authorized reference scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AuthorizedIndexRangeScan {
    Unavailable,
    Rows(Vec<(RowId, Vec<SanitizedCell>)>),
}

/// A bounded index range result exposed through one executor connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutorIndexRangeScan {
    Unavailable,
    Rows(Vec<Vec<SanitizedCell>>),
}

const MAX_AUTHORIZED_INDEX_RANGE_CANDIDATES: usize = 1_024;
const MAX_AUTHORIZED_INDEX_RANGE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutorAuthorizedRowScanPage {
    pub rows: Vec<(ExecutorRowHandle, Vec<SanitizedCell>)>,
    pub continuation: Option<ExecutorScanCursor>,
}

/// One authorized unique-index probe result with an opaque executor row handle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutorUniqueIndexProbe {
    Unavailable,
    NoMatch,
    Match {
        handle: ExecutorRowHandle,
        cells: Vec<SanitizedCell>,
    },
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
    controller_principal_id: [u8; 16],
    requested_scope: Option<HashSet<(Operation, AccessTarget)>>,
    retention_deadline_us: i64,
    coordination: InvocationCoordination,
    aborted: bool,
    cancellation_triggered: bool,
    outcome_recorded: bool,
    active_scan: Option<ActiveRowScan>,
    max_scan_work: usize,
    scanned_work: usize,
    active_relation_scan: Option<ActiveRelationScan>,
    active_schema_scan: Option<ActiveSchemaScan>,
    active_column_scan: Option<ActiveColumnScan>,
    active_builtin_relation_scan: Option<ActiveBuiltinRelationScan>,
    active_builtin_column_scan: Option<ActiveBuiltinColumnScan>,
    active_information_schema_tables_scan: Option<ActiveInformationSchemaTablesScan>,
    active_information_schema_columns_scan: Option<ActiveInformationSchemaColumnsScan>,
}

/// Executor metadata session whose opaque relation handles expire when the
/// session is dropped. Handle resolution and revalidation always consult the
/// transaction's pinned catalog and never grant row access by themselves.
pub struct ExecutorMetadataConnection<'connection, 'db> {
    transaction: &'connection mut InvocationTransaction<'db>,
    handles: HashMap<[u8; 16], ResolvedRelationHandleState>,
    used_tokens: HashSet<[u8; 16]>,
    scans: HashMap<[u8; 16], ActiveExecutorScan>,
    used_scan_tokens: HashSet<[u8; 16]>,
    row_handles: HashMap<[u8; 16], RowId>,
    used_row_tokens: HashSet<[u8; 16]>,
    detached: bool,
}

#[derive(Default)]
pub(crate) struct ExecutorMetadataSessionState {
    handles: HashMap<[u8; 16], ResolvedRelationHandleState>,
    used_tokens: HashSet<[u8; 16]>,
    scans: HashMap<[u8; 16], ActiveExecutorScan>,
    used_scan_tokens: HashSet<[u8; 16]>,
    row_handles: HashMap<[u8; 16], RowId>,
    used_row_tokens: HashSet<[u8; 16]>,
}

/// Planner metadata session with opaque, connection-local table handles.
/// Estimates are rebuilt from the invocation's pinned catalog on each request.
pub struct PlannerMetadataConnection<'connection, 'db> {
    transaction: &'connection mut InvocationTransaction<'db>,
    handles: HashMap<[u8; 16], PlannerTableHandleState>,
    used_tokens: HashSet<[u8; 16]>,
    estimate_requests: usize,
}

#[derive(Default)]
pub(crate) struct PlannerMetadataSessionState {
    handles: HashMap<[u8; 16], PlannerTableHandleState>,
    used_tokens: HashSet<[u8; 16]>,
    estimate_requests: usize,
}

#[derive(Clone)]
struct PlannerTableHandleState {
    schema_name: String,
    relation_name: String,
    relation_id: [u8; 16],
}

#[derive(Clone)]
struct ResolvedRelationHandleState {
    schema_name: String,
    relation_name: String,
    relation_id: [u8; 16],
}

#[derive(Clone)]
struct ActiveExecutorScan {
    relation_handle: [u8; 16],
    selected_column_ordinals: Vec<usize>,
    source: ExecutorScanSource,
}

#[derive(Clone)]
enum ExecutorScanSource {
    Physical {
        row_cursor: Option<RowScanCursor>,
        pending_row: Option<(RowId, Vec<SanitizedCell>)>,
    },
    InformationSchema {
        relation: BuiltinInformationSchemaRelation,
        continuation: BuiltinInformationSchemaCursor,
    },
}

impl ExecutorScanSource {
    fn is_information_schema(&self) -> bool {
        matches!(self, Self::InformationSchema { .. })
    }

    fn information_schema_relation(&self) -> Option<BuiltinInformationSchemaRelation> {
        match self {
            Self::InformationSchema { relation, .. } => Some(*relation),
            Self::Physical { .. } => None,
        }
    }
}

fn information_schema_optional_integer(
    value: Option<u32>,
) -> Result<SanitizedCell, TransactionError> {
    match value {
        Some(value) => i32::try_from(value)
            .map(|value| SanitizedCell::Value(ScalarValue::Integer(value)))
            .map_err(|_| TransactionError::InvalidMetadataRequest),
        None => Ok(SanitizedCell::Null),
    }
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
enum BuiltinInformationSchemaRelation {
    Schemata,
    Tables,
    Columns,
}

#[derive(Clone)]
enum BuiltinInformationSchemaCursor {
    Schemata(Option<SchemaScanCursor>),
    Tables(Option<InformationSchemaTablesCursor>),
    Columns(Option<InformationSchemaColumnsCursor>),
}

struct CatalogTableProjection {
    schema: CatalogTableSchema,
    context: AuthorizationContext,
    grants: Vec<Grant>,
    policies: Vec<Policy>,
    checks: Vec<Expr>,
    policy_driving_columns: Vec<[u8; 16]>,
}

impl CatalogTableProjection {
    fn as_projection(&self) -> AuthorizedProjection<'_> {
        AuthorizedProjection {
            columns: &self.schema.columns,
            column_ids: &self.schema.column_ids,
            schema_version: self.schema.schema_version,
            schema_id: self.schema.schema_id,
            context: &self.context,
            grants: &self.grants,
            policies: &self.policies,
            checks: &self.checks,
            policy_driving_columns: &self.policy_driving_columns,
            dependencies: &[],
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActiveRelationScan {
    schema_id: [u8; 16],
    extra_bytes_per_relation: usize,
    principal_id: [u8; 16],
    role_ids: HashSet<[u8; 16]>,
    scope: HashSet<(Operation, AccessTarget)>,
    grants: Vec<Grant>,
    dependencies: Vec<AuthorityDependency>,
    continuation: Option<([u8; 16], [u8; 16])>,
    used_tokens: HashSet<[u8; 16]>,
}

struct MetadataDiscoveryAccess<'a> {
    context: &'a AuthorizationContext,
    grants: &'a [Grant],
    dependencies: &'a [AuthorityDependency],
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
    extra_bytes_per_column: usize,
    principal_id: [u8; 16],
    role_ids: HashSet<[u8; 16]>,
    scope: HashSet<(Operation, AccessTarget)>,
    grants: Vec<Grant>,
    dependencies: Vec<AuthorityDependency>,
    continuation: Option<([u8; 16], u32)>,
    used_tokens: HashSet<[u8; 16]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BuiltinRelationScanPurpose {
    Tables,
    Columns,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActiveBuiltinRelationScan {
    purpose: BuiltinRelationScanPurpose,
    extra_bytes_per_relation: usize,
    principal_id: [u8; 16],
    continuation: Option<([u8; 16], usize)>,
    used_tokens: HashSet<[u8; 16]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActiveBuiltinColumnScan {
    relation_id: [u8; 16],
    extra_bytes_per_column: usize,
    principal_id: [u8; 16],
    continuation: Option<([u8; 16], u32)>,
    used_tokens: HashSet<[u8; 16]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActiveInformationSchemaTablesScan {
    principal_id: [u8; 16],
    current_schema: Option<VisibleSchema>,
    schema_continuation: Option<SchemaScanCursor>,
    relation_continuation: Option<RelationScanCursor>,
    continuation: Option<[u8; 16]>,
    used_tokens: HashSet<[u8; 16]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActiveInformationSchemaColumnsScan {
    principal_id: [u8; 16],
    schema_scan_started: bool,
    current_schema: Option<VisibleSchema>,
    schema_continuation: Option<SchemaScanCursor>,
    relation_scan_started: bool,
    relation_scan_exhausted: bool,
    current_relation: Option<VisibleRelation>,
    relation_continuation: Option<RelationScanCursor>,
    current_relation_columns_started: bool,
    column_continuation: Option<ColumnScanCursor>,
    continuation: Option<[u8; 16]>,
    used_tokens: HashSet<[u8; 16]>,
}

pub(crate) struct InvocationCoordination {
    ddl_fences: Option<DdlFenceCoordinator>,
    _admission_permit: Option<InvocationAdmissionPermit>,
    admission_metrics: Option<InvocationAdmissionMetrics>,
    max_age: Option<(Instant, Duration)>,
    max_scan_work: usize,
}

impl InvocationCoordination {
    pub(crate) fn uncoordinated() -> Self {
        Self {
            ddl_fences: None,
            _admission_permit: None,
            admission_metrics: None,
            max_age: None,
            max_scan_work: MAX_INVOCATION_SCAN_WORK,
        }
    }

    fn fenced(ddl_fences: DdlFenceCoordinator) -> Self {
        Self {
            ddl_fences: Some(ddl_fences),
            _admission_permit: None,
            admission_metrics: None,
            max_age: None,
            max_scan_work: MAX_INVOCATION_SCAN_WORK,
        }
    }

    pub(crate) fn managed(
        ddl_fences: DdlFenceCoordinator,
        admission_permit: InvocationAdmissionPermit,
        max_age: Duration,
        max_scan_work: usize,
    ) -> Self {
        let admission_metrics = admission_permit.metrics();
        Self {
            ddl_fences: Some(ddl_fences),
            _admission_permit: Some(admission_permit),
            admission_metrics: Some(admission_metrics),
            max_age: Some((Instant::now(), max_age)),
            max_scan_work,
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

    fn cancellation_requested(&self) -> bool {
        self._admission_permit
            .as_ref()
            .is_some_and(InvocationAdmissionPermit::cancellation_requested)
    }

    fn update_write_set_usage(&self, staged_rows: usize, staged_value_bytes: usize) {
        if let Some(permit) = &self._admission_permit {
            permit.update_write_set_usage(staged_rows, staged_value_bytes);
        }
    }

    fn record_staged_value_bytes(&self, bytes: usize) {
        if let Some(permit) = &self._admission_permit {
            permit.record_staged_value_bytes(bytes);
        }
    }

    fn update_snapshot_pin_count(&self, snapshot_pin_count: u8) {
        if let Some(permit) = &self._admission_permit {
            permit.update_snapshot_pin_count(snapshot_pin_count);
        }
    }

    fn record_conflict(&self) {
        if let Some(permit) = &self._admission_permit {
            permit.record_conflict();
        }
    }

    fn record_policy_denial(&self) {
        if let Some(metrics) = &self.admission_metrics {
            metrics.record_policy_denial();
        }
    }

    fn mark_committing(&self) -> bool {
        self._admission_permit
            .as_ref()
            .is_none_or(|permit| permit.try_mark_committing())
    }

    fn mark_commit_uncertain(&self) {
        if let Some(permit) = &self._admission_permit {
            permit.mark_commit_uncertain();
        }
    }
}

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

    fn scope_from_grants(&self, grants: &[Grant]) -> HashSet<(Operation, AccessTarget)> {
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

    fn charge_scan_work(&mut self, amount: usize) -> Result<(), TransactionError> {
        self.scanned_work = self
            .scanned_work
            .checked_add(amount)
            .filter(|work| *work <= self.max_scan_work)
            .ok_or(TransactionError::ScanBudgetExceeded)?;
        Ok(())
    }

    fn scan_catalog_family(
        &mut self,
        family_code: u16,
    ) -> Result<Vec<CatalogRecord>, TransactionError> {
        let result = (|| {
            let remaining = self.max_scan_work.saturating_sub(self.scanned_work);
            if remaining == 0 {
                return Err(TransactionError::ScanBudgetExceeded);
            }
            let family_limit = MAX_CATALOG_FAMILY_SCAN;
            let records =
                self.pinned_snapshot()?
                    .scan_catalog_family_limited(family_code, remaining.min(family_limit))
                    .map_err(|error| match error {
                        SnapshotError::RecordLimitExceeded if remaining < family_limit => {
                            TransactionError::ScanBudgetExceeded
                        }
                        SnapshotError::RecordLimitExceeded
                        | SnapshotError::InvalidCatalogRecord => TransactionError::CorruptCatalog,
                        SnapshotError::Storage(error) => TransactionError::Storage(error),
                        _ => TransactionError::CorruptCatalog,
                    })?;
            self.charge_scan_work(records.len())?;
            Ok(records)
        })();
        if matches!(&result, Err(TransactionError::ScanBudgetExceeded)) && !self.aborted {
            self.abort()?;
        }
        result
    }

    fn scan_policy_ids_for_table(
        &mut self,
        table_id: [u8; 16],
    ) -> Result<Vec<[u8; 16]>, TransactionError> {
        let result = (|| {
            let remaining = self.max_scan_work.saturating_sub(self.scanned_work);
            if remaining == 0 {
                return Err(TransactionError::ScanBudgetExceeded);
            }
            let policy_limit = crate::authorization::MAX_AUTHORIZATION_INPUTS;
            let limit = remaining.min(policy_limit);
            let ids =
                self.pinned_snapshot()?
                    .scan_policy_ids_for_table_limited(table_id, limit)
                    .map_err(|error| match error {
                        SnapshotError::RecordLimitExceeded if remaining <= policy_limit => {
                            TransactionError::ScanBudgetExceeded
                        }
                        SnapshotError::RecordLimitExceeded
                        | SnapshotError::InvalidCatalogRecord => TransactionError::CorruptCatalog,
                        SnapshotError::Storage(error) => TransactionError::Storage(error),
                        _ => TransactionError::CorruptCatalog,
                    })?;
            self.charge_scan_work(ids.len())?;
            Ok(ids)
        })();
        if matches!(&result, Err(TransactionError::ScanBudgetExceeded)) && !self.aborted {
            self.abort()?;
        }
        result
    }

    fn scan_column_ids_for_table(
        &mut self,
        table_id: [u8; 16],
    ) -> Result<Vec<[u8; 16]>, TransactionError> {
        let result = (|| {
            let remaining = self.max_scan_work.saturating_sub(self.scanned_work);
            if remaining == 0 {
                return Err(TransactionError::ScanBudgetExceeded);
            }
            let column_limit = crate::tuple::MAX_COLUMNS;
            let limit = remaining.min(column_limit);
            let ids =
                self.pinned_snapshot()?
                    .scan_column_ids_for_table(table_id, limit)
                    .map_err(|error| match error {
                        SnapshotError::RecordLimitExceeded if remaining <= column_limit => {
                            TransactionError::ScanBudgetExceeded
                        }
                        SnapshotError::RecordLimitExceeded
                        | SnapshotError::InvalidCatalogRecord => TransactionError::CorruptCatalog,
                        SnapshotError::Storage(error) => TransactionError::Storage(error),
                        _ => TransactionError::CorruptCatalog,
                    })?;
            self.charge_scan_work(ids.len())?;
            Ok(ids)
        })();
        if matches!(&result, Err(TransactionError::ScanBudgetExceeded)) && !self.aborted {
            self.abort()?;
        }
        result
    }

    fn scan_index_ids_for_table(
        &mut self,
        table_id: [u8; 16],
    ) -> Result<Vec<[u8; 16]>, TransactionError> {
        let result = (|| {
            let remaining = self.max_scan_work.saturating_sub(self.scanned_work);
            if remaining == 0 {
                return Err(TransactionError::ScanBudgetExceeded);
            }
            let limit = remaining.min(MAX_CATALOG_FAMILY_SCAN);
            let ids = self
                .pinned_snapshot()?
                .scan_index_ids_for_table(table_id, limit)
                .map_err(|error| match error {
                    SnapshotError::RecordLimitExceeded if remaining <= MAX_CATALOG_FAMILY_SCAN => {
                        TransactionError::ScanBudgetExceeded
                    }
                    SnapshotError::RecordLimitExceeded | SnapshotError::InvalidCatalogRecord => {
                        TransactionError::CorruptCatalog
                    }
                    SnapshotError::Storage(error) => TransactionError::Storage(error),
                    _ => TransactionError::CorruptCatalog,
                })?;
            self.charge_scan_work(ids.len())?;
            Ok(ids)
        })();
        if matches!(&result, Err(TransactionError::ScanBudgetExceeded)) && !self.aborted {
            self.abort()?;
        }
        result
    }

    fn scan_constraint_ids_for_table(
        &mut self,
        table_id: [u8; 16],
    ) -> Result<Vec<[u8; 16]>, TransactionError> {
        let result = (|| {
            let remaining = self.max_scan_work.saturating_sub(self.scanned_work);
            if remaining == 0 {
                return Err(TransactionError::ScanBudgetExceeded);
            }
            let limit = remaining.min(MAX_CATALOG_FAMILY_SCAN);
            let ids = self
                .pinned_snapshot()?
                .scan_constraint_ids_for_table(table_id, limit)
                .map_err(|error| match error {
                    SnapshotError::RecordLimitExceeded if remaining <= MAX_CATALOG_FAMILY_SCAN => {
                        TransactionError::ScanBudgetExceeded
                    }
                    SnapshotError::RecordLimitExceeded | SnapshotError::InvalidCatalogRecord => {
                        TransactionError::CorruptCatalog
                    }
                    SnapshotError::Storage(error) => TransactionError::Storage(error),
                    _ => TransactionError::CorruptCatalog,
                })?;
            self.charge_scan_work(ids.len())?;
            Ok(ids)
        })();
        if matches!(&result, Err(TransactionError::ScanBudgetExceeded)) && !self.aborted {
            self.abort()?;
        }
        result
    }

    fn scan_relation_ids_for_schema(
        &mut self,
        schema_id: [u8; 16],
    ) -> Result<Vec<[u8; 16]>, TransactionError> {
        let result = (|| {
            let remaining = self.max_scan_work.saturating_sub(self.scanned_work);
            if remaining == 0 {
                return Err(TransactionError::ScanBudgetExceeded);
            }
            let limit = remaining.min(MAX_CATALOG_FAMILY_SCAN);
            let ids = self
                .pinned_snapshot()?
                .scan_relation_ids_for_schema(schema_id, limit)
                .map_err(|error| match error {
                    SnapshotError::RecordLimitExceeded if remaining <= MAX_CATALOG_FAMILY_SCAN => {
                        TransactionError::ScanBudgetExceeded
                    }
                    SnapshotError::RecordLimitExceeded | SnapshotError::InvalidCatalogRecord => {
                        TransactionError::CorruptCatalog
                    }
                    SnapshotError::Storage(error) => TransactionError::Storage(error),
                    _ => TransactionError::CorruptCatalog,
                })?;
            self.charge_scan_work(ids.len())?;
            Ok(ids)
        })();
        if matches!(&result, Err(TransactionError::ScanBudgetExceeded)) && !self.aborted {
            self.abort()?;
        }
        result
    }

    fn scan_object_ids_for_owner(
        &mut self,
        owner_id: [u8; 16],
    ) -> Result<Vec<[u8; 16]>, TransactionError> {
        let result = (|| {
            let remaining = self.max_scan_work.saturating_sub(self.scanned_work);
            if remaining == 0 {
                return Err(TransactionError::ScanBudgetExceeded);
            }
            let limit = remaining.min(MAX_CATALOG_FAMILY_SCAN);
            let ids = self
                .pinned_snapshot()?
                .scan_object_ids_for_owner(owner_id, limit)
                .map_err(|error| match error {
                    SnapshotError::RecordLimitExceeded if remaining <= MAX_CATALOG_FAMILY_SCAN => {
                        TransactionError::ScanBudgetExceeded
                    }
                    SnapshotError::RecordLimitExceeded | SnapshotError::InvalidCatalogRecord => {
                        TransactionError::CorruptCatalog
                    }
                    SnapshotError::Storage(error) => TransactionError::Storage(error),
                    _ => TransactionError::CorruptCatalog,
                })?;
            self.charge_scan_work(ids.len())?;
            Ok(ids)
        })();
        if matches!(&result, Err(TransactionError::ScanBudgetExceeded)) && !self.aborted {
            self.abort()?;
        }
        result
    }

    fn lookup_catalog_dependents_for_referenced(
        &mut self,
        referenced_id: [u8; 16],
    ) -> Result<Vec<CatalogRecord>, TransactionError> {
        let result = (|| {
            let remaining = self.max_scan_work.saturating_sub(self.scanned_work);
            if remaining == 0 {
                return Err(TransactionError::ScanBudgetExceeded);
            }
            let limit = remaining.min(MAX_CATALOG_FAMILY_SCAN);
            let records = self
                .pinned_snapshot()?
                .lookup_dependents_for_referenced(referenced_id, limit)
                .map_err(|error| match error {
                    SnapshotError::RecordLimitExceeded if remaining <= MAX_CATALOG_FAMILY_SCAN => {
                        TransactionError::ScanBudgetExceeded
                    }
                    SnapshotError::RecordLimitExceeded | SnapshotError::InvalidCatalogRecord => {
                        TransactionError::CorruptCatalog
                    }
                    SnapshotError::Storage(error) => TransactionError::Storage(error),
                    _ => TransactionError::CorruptCatalog,
                })?;
            self.charge_scan_work(records.len())?;
            Ok(records)
        })();
        if matches!(&result, Err(TransactionError::ScanBudgetExceeded)) && !self.aborted {
            self.abort()?;
        }
        result
    }

    fn scan_policy_targets_for_policy(
        &mut self,
        policy_id: [u8; 16],
    ) -> Result<Vec<CatalogRecord>, TransactionError> {
        let result = (|| {
            let remaining = self.max_scan_work.saturating_sub(self.scanned_work);
            if remaining == 0 {
                return Err(TransactionError::ScanBudgetExceeded);
            }
            let policy_limit = crate::authorization::MAX_AUTHORIZATION_INPUTS;
            let limit = remaining.min(policy_limit);
            let records =
                self.pinned_snapshot()?
                    .scan_policy_targets_for_policy_limited(policy_id, limit)
                    .map_err(|error| match error {
                        SnapshotError::RecordLimitExceeded if remaining <= policy_limit => {
                            TransactionError::ScanBudgetExceeded
                        }
                        SnapshotError::RecordLimitExceeded
                        | SnapshotError::InvalidCatalogRecord => TransactionError::CorruptCatalog,
                        SnapshotError::Storage(error) => TransactionError::Storage(error),
                        _ => TransactionError::CorruptCatalog,
                    })?;
            self.charge_scan_work(records.len())?;
            Ok(records)
        })();
        if matches!(&result, Err(TransactionError::ScanBudgetExceeded)) && !self.aborted {
            self.abort()?;
        }
        result
    }

    fn lookup_catalog_grants_for_grantees(
        &mut self,
        grantees: &HashSet<[u8; 16]>,
    ) -> Result<Vec<CatalogRecord>, TransactionError> {
        let mut grantee_ids = grantees.iter().copied().collect::<Vec<_>>();
        grantee_ids.sort_unstable();
        let mut records = Vec::new();
        for grantee_id in grantee_ids {
            let indexed = self
                .pinned_snapshot()?
                .lookup_grants_for_grantee(grantee_id)
                .map_err(|error| match error {
                    SnapshotError::Storage(error) => TransactionError::Storage(error),
                    SnapshotError::InvalidCatalogRecord => TransactionError::CorruptCatalog,
                    _ => TransactionError::CorruptCatalog,
                })?;
            self.charge_scan_work(indexed.len())?;
            records.extend(indexed);
        }
        Ok(records)
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
            self.charge_scan_work(row_ids.len())?;
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

    /// Read one authorized row using schema, grants, and policies loaded from
    /// this invocation's pinned catalog. Callers cannot substitute authorization
    /// contexts or grant records for this read path.
    pub fn read_catalog_authorized_row(
        &mut self,
        table_id: [u8; 16],
        row_id: [u8; 16],
    ) -> Result<Option<Vec<SanitizedCell>>, TransactionError> {
        let result = (|| {
            if table_id == [0; 16] || row_id == [0; 16] {
                return Err(TransactionError::InvalidContext);
            }
            let schema = self.load_catalog_table_schema(table_id)?;
            let principal_id = self.principal_id;
            let (role_ids, grants) = self.load_catalog_grants(principal_id, &schema)?;
            let policies = self.load_catalog_policies(principal_id, &schema)?;
            let scope = self.scope_from_grants(&grants);
            let context = AuthorizationContext {
                principal_id,
                role_ids,
                scope,
            };
            let projection = AuthorizedProjection {
                columns: &schema.columns,
                column_ids: &schema.column_ids,
                schema_version: schema.schema_version,
                schema_id: schema.schema_id,
                context: &context,
                grants: &grants,
                policies: &policies,
                checks: &[],
                policy_driving_columns: &[],
                dependencies: &[],
            };
            self.read_authorized(
                RowId {
                    table: table_id,
                    row: row_id,
                },
                &projection,
            )
        })();
        match result {
            Ok(row) => Ok(row),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Scan authorized rows using schema, grants, and policies loaded from this
    /// invocation's pinned catalog. Callers cannot substitute authorization
    /// contexts or grant records for this read path.
    pub fn scan_catalog_authorized_rows(
        &mut self,
        table_id: [u8; 16],
        cursor: Option<&RowScanCursor>,
        max_scanned: usize,
    ) -> Result<AuthorizedRowScanPage, TransactionError> {
        let result = (|| {
            let schema = self.load_catalog_table_schema(table_id)?;
            let principal_id = self.principal_id;
            let (role_ids, grants) = self.load_catalog_grants(principal_id, &schema)?;
            let policies = self.load_catalog_policies(principal_id, &schema)?;
            let scope = self.scope_from_grants(&grants);
            let context = AuthorizationContext {
                principal_id,
                role_ids,
                scope,
            };
            let projection = AuthorizedProjection {
                columns: &schema.columns,
                column_ids: &schema.column_ids,
                schema_version: schema.schema_version,
                schema_id: schema.schema_id,
                context: &context,
                grants: &grants,
                policies: &policies,
                checks: &[],
                policy_driving_columns: &[],
                dependencies: &[],
            };
            self.scan_authorized(table_id, cursor, max_scanned, &projection)
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

    /// Probe a catalog-loaded unique index for one non-NULL equality tuple.
    /// The path is available only when each key column is readable without a
    /// cell policy; otherwise the caller must retain the reference scan so
    /// redacted predicate behavior stays unchanged. Snapshot candidates are
    /// checked against their row version, then reconciled with staged writes
    /// and projected through the same pinned row/cell authorization path.
    pub fn probe_catalog_unique_index(
        &mut self,
        table_id: [u8; 16],
        index_id: [u8; 16],
        values: &[Option<ScalarValue>],
        projection: &AuthorizedProjection<'_>,
    ) -> Result<AuthorizedUniqueIndexProbe, TransactionError> {
        let result = (|| {
            if table_id == [0; 16]
                || index_id == [0; 16]
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
            if self.active_scan.is_some() {
                return Err(TransactionError::ScanStillActive);
            }
            self.capture_authority_dependencies(projection.context, projection.dependencies)?;
            let schema = self.load_catalog_table_schema(table_id)?;
            if schema.schema_id != projection.schema_id
                || schema.schema_version != projection.schema_version
                || schema.column_ids != projection.column_ids
                || schema.columns != projection.columns
            {
                return Err(TransactionError::InvalidContext);
            }
            let indexes = if let Some(indexes) = self.loaded_indexes.get(&table_id) {
                indexes.clone()
            } else {
                self.load_catalog_indexes(&schema)?
            };
            let Some(index) = indexes
                .into_iter()
                .find(|index| index.index_id == index_id && index.table_id == table_id)
            else {
                return Ok(AuthorizedUniqueIndexProbe::Unavailable);
            };
            if !index.unique || index.column_ids.len() != values.len() {
                return Ok(AuthorizedUniqueIndexProbe::Unavailable);
            }
            let ordinals = index
                .column_ids
                .iter()
                .map(|column_id| {
                    schema
                        .column_ids
                        .iter()
                        .position(|candidate| candidate == column_id)
                        .ok_or(TransactionError::CorruptCatalog)
                })
                .collect::<Result<Vec<_>, _>>()?;
            if ordinals.iter().any(|ordinal| {
                !index_component_unconditionally_readable(
                    projection,
                    table_id,
                    schema.column_ids[*ordinal],
                )
            }) {
                return Ok(AuthorizedUniqueIndexProbe::Unavailable);
            }
            if values.iter().any(Option::is_none) {
                return Ok(AuthorizedUniqueIndexProbe::NoMatch);
            }
            let key_columns = ordinals
                .iter()
                .map(|ordinal| schema.columns[*ordinal].clone())
                .collect::<Vec<_>>();
            let encoded_tuple =
                encode_sql_tuple(&key_columns, values).map_err(TransactionError::Tuple)?;
            let mut prefix = Vec::with_capacity(16 + encoded_tuple.len());
            prefix.extend_from_slice(&index_id);
            prefix.extend_from_slice(&encoded_tuple);

            let snapshot_page = self
                .pinned_snapshot()?
                .scan_index_prefix(&prefix, None, 2)
                .map_err(|error| TransactionError::Storage(error.to_string()))?;
            if snapshot_page.candidates.len() > 1 || snapshot_page.continuation.is_some() {
                return Err(TransactionError::CorruptRowKey);
            }
            let remaining_work = self.max_scan_work.saturating_sub(self.scanned_work);
            let overlay_scan_work = self.overlay.staged_row_count()?;
            let Some(overlay_row_ids) = self
                .overlay
                .bounded_row_ids_for_table(table_id, remaining_work)?
            else {
                return Err(TransactionError::ScanBudgetExceeded);
            };
            let candidate_work = overlay_scan_work
                .checked_add(snapshot_page.candidates.len())
                .ok_or(TransactionError::ScanBudgetExceeded)?;
            self.charge_scan_work(candidate_work)?;

            let mut candidate_ids = HashSet::new();
            for candidate in &snapshot_page.candidates {
                let row_id = RowId {
                    table: table_id,
                    row: candidate.locator.row_id,
                };
                let stored = self
                    .pinned_snapshot()?
                    .get(&row_key(row_id))
                    .map_err(|error| TransactionError::Storage(error.to_string()))?
                    .ok_or(TransactionError::CorruptRowKey)?;
                let record = RowRecord::decode(&stored).map_err(TransactionError::CorruptRow)?;
                if record.row_version != candidate.locator.row_version {
                    return Err(TransactionError::CorruptRowKey);
                }
                candidate_ids.insert(row_id);
            }
            for row_id in overlay_row_ids {
                let id = RowId {
                    table: table_id,
                    row: row_id,
                };
                let Some(StagedRow::Value(payload)) =
                    self.overlay.get_for_actor(&id, self.principal_id)?
                else {
                    continue;
                };
                let derived = derive_index_key(
                    index_id,
                    row_id,
                    schema.schema_version,
                    &schema.column_ids,
                    &schema.columns,
                    payload,
                    &index.column_ids,
                )
                .map_err(|error| match error {
                    DeriveIndexKeyError::InvalidDefinition => TransactionError::CorruptCatalog,
                    DeriveIndexKeyError::Tuple(error) => TransactionError::Tuple(error),
                })?;
                if derived.key.starts_with(&prefix) {
                    candidate_ids.insert(id);
                }
            }
            if candidate_ids.len() > 1 {
                // A staged duplicate violates the unique-index assumption for
                // this access path; use the reference scan until finalization.
                return Ok(AuthorizedUniqueIndexProbe::Unavailable);
            }
            let mut candidate_ids = candidate_ids.into_iter().collect::<Vec<_>>();
            candidate_ids.sort_unstable_by_key(|id| id.row);
            let Some(candidate_id) = candidate_ids.into_iter().next() else {
                return Ok(AuthorizedUniqueIndexProbe::NoMatch);
            };
            let Some(cells) = self.read_authorized(candidate_id, projection)? else {
                return Ok(AuthorizedUniqueIndexProbe::NoMatch);
            };
            for (ordinal, expected) in ordinals.iter().zip(values) {
                match (&cells[*ordinal], expected) {
                    (SanitizedCell::Value(actual), Some(expected)) if actual == expected => {}
                    (SanitizedCell::Redacted, _) => {
                        return Ok(AuthorizedUniqueIndexProbe::Unavailable);
                    }
                    _ => return Ok(AuthorizedUniqueIndexProbe::NoMatch),
                }
            }
            Ok(AuthorizedUniqueIndexProbe::Match {
                id: candidate_id,
                cells,
            })
        })();
        match result {
            Ok(probe) => Ok(probe),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Read a bounded leading-key range from one catalog-loaded index. Storage
    /// validates the index against the pinned schema, requires every index
    /// component to be unconditionally readable, reconciles snapshot entries
    /// with private writes, and returns only sanitized authorized rows. Wide
    /// ranges fall back to the reference scan instead of returning a prefix.
    pub fn scan_catalog_index_range(
        &mut self,
        table_id: [u8; 16],
        index_id: [u8; 16],
        lower: Option<AuthorizedIndexRangeBound>,
        upper: Option<AuthorizedIndexRangeBound>,
        projection: &AuthorizedProjection<'_>,
    ) -> Result<AuthorizedIndexRangeScan, TransactionError> {
        let result = (|| {
            if table_id == [0; 16]
                || index_id == [0; 16]
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
            if self.active_scan.is_some() {
                return Err(TransactionError::ScanStillActive);
            }
            self.capture_authority_dependencies(projection.context, projection.dependencies)?;
            let schema = self.load_catalog_table_schema(table_id)?;
            if schema.schema_id != projection.schema_id
                || schema.schema_version != projection.schema_version
                || schema.column_ids != projection.column_ids
                || schema.columns != projection.columns
            {
                return Err(TransactionError::InvalidContext);
            }
            let indexes = if let Some(indexes) = self.loaded_indexes.get(&table_id) {
                indexes.clone()
            } else {
                self.load_catalog_indexes(&schema)?
            };
            let Some(index) = indexes
                .into_iter()
                .find(|index| index.index_id == index_id && index.table_id == table_id)
            else {
                return Ok(AuthorizedIndexRangeScan::Unavailable);
            };
            let Some(first_column_id) = index.column_ids.first().copied() else {
                return Err(TransactionError::CorruptCatalog);
            };
            let key_ordinal = schema
                .column_ids
                .iter()
                .position(|column_id| *column_id == first_column_id)
                .ok_or(TransactionError::CorruptCatalog)?;
            if index.column_ids.iter().any(|column_id| {
                let Some(ordinal) = schema
                    .column_ids
                    .iter()
                    .position(|candidate| candidate == column_id)
                else {
                    return true;
                };
                !index_component_unconditionally_readable(
                    projection,
                    table_id,
                    schema.column_ids[ordinal],
                )
            }) {
                return Ok(AuthorizedIndexRangeScan::Unavailable);
            }

            let encode_bound = |bound: &AuthorizedIndexRangeBound| {
                encode_sql_tuple(
                    std::slice::from_ref(&schema.columns[key_ordinal]),
                    &[Some(bound.value.clone())],
                )
                .map_err(TransactionError::Tuple)
            };
            let lower_tuple = lower.as_ref().map(encode_bound).transpose()?;
            let upper_tuple = upper.as_ref().map(encode_bound).transpose()?;
            let snapshot_lower =
                lower_tuple
                    .as_ref()
                    .map(|tuple| crate::snapshot::IndexRangeBound {
                        tuple: tuple.clone(),
                        inclusive: lower.as_ref().is_some_and(|bound| bound.inclusive),
                    });
            let snapshot_upper =
                upper_tuple
                    .as_ref()
                    .map(|tuple| crate::snapshot::IndexRangeBound {
                        tuple: tuple.clone(),
                        inclusive: upper.as_ref().is_some_and(|bound| bound.inclusive),
                    });

            let lower_start = match &lower_tuple {
                Some(tuple) => {
                    let mut prefix = Vec::with_capacity(16 + tuple.len());
                    prefix.extend_from_slice(&index_id);
                    prefix.extend_from_slice(tuple);
                    if lower.as_ref().is_some_and(|bound| bound.inclusive) {
                        prefix
                    } else if let Some(successor) = crate::index_key::prefix_successor(&prefix) {
                        successor
                    } else {
                        return Ok(AuthorizedIndexRangeScan::Rows(Vec::new()));
                    }
                }
                None => index_id.to_vec(),
            };
            let upper_stop = match &upper_tuple {
                Some(tuple) => {
                    let mut prefix = Vec::with_capacity(16 + tuple.len());
                    prefix.extend_from_slice(&index_id);
                    prefix.extend_from_slice(tuple);
                    if upper.as_ref().is_some_and(|bound| bound.inclusive) {
                        crate::index_key::prefix_successor(&prefix)
                    } else {
                        Some(prefix)
                    }
                }
                None => crate::index_key::prefix_successor(&index_id),
            };
            if upper_stop.as_ref().is_some_and(|stop| lower_start >= *stop) {
                return Ok(AuthorizedIndexRangeScan::Rows(Vec::new()));
            }

            let mut snapshot_candidates = Vec::new();
            let mut snapshot_bytes = 0usize;
            let mut after_key: Option<Vec<u8>> = None;
            loop {
                let page = self
                    .pinned_snapshot()?
                    .scan_index_range(
                        &index_id,
                        snapshot_lower.clone(),
                        snapshot_upper.clone(),
                        after_key.as_deref(),
                        crate::snapshot::MAX_ROW_SCAN_PAGE,
                    )
                    .map_err(|error| TransactionError::Storage(error.to_string()))?;
                snapshot_bytes = snapshot_bytes.saturating_add(
                    page.candidates
                        .iter()
                        .map(|candidate| candidate.key.len())
                        .sum::<usize>(),
                );
                if snapshot_candidates
                    .len()
                    .saturating_add(page.candidates.len())
                    > MAX_AUTHORIZED_INDEX_RANGE_CANDIDATES
                    || snapshot_bytes > MAX_AUTHORIZED_INDEX_RANGE_BYTES
                {
                    self.charge_scan_work(
                        snapshot_candidates
                            .len()
                            .saturating_add(page.candidates.len()),
                    )?;
                    return Ok(AuthorizedIndexRangeScan::Unavailable);
                }
                snapshot_candidates.extend(page.candidates);
                let Some(continuation) = page.continuation else {
                    break;
                };
                after_key = Some(continuation);
            }

            let remaining_work = self.max_scan_work.saturating_sub(self.scanned_work);
            let overlay_scan_work = self.overlay.staged_row_count()?;
            let Some(overlay_row_ids) = self
                .overlay
                .bounded_row_ids_for_table(table_id, remaining_work)?
            else {
                return Err(TransactionError::ScanBudgetExceeded);
            };
            let candidate_work = overlay_scan_work
                .checked_add(snapshot_candidates.len())
                .ok_or(TransactionError::ScanBudgetExceeded)?;
            self.charge_scan_work(candidate_work)?;

            let in_range = |key: &[u8]| {
                key.len() >= 16
                    && key[..16] == index_id
                    && key >= lower_start.as_slice()
                    && upper_stop.as_ref().is_none_or(|stop| key < stop.as_slice())
            };
            let mut candidate_ids = HashSet::new();
            let mut candidate_row_bytes = 0usize;
            for candidate in &snapshot_candidates {
                let row_id = RowId {
                    table: table_id,
                    row: candidate.locator.row_id,
                };
                let stored = self
                    .pinned_snapshot()?
                    .get(&row_key(row_id))
                    .map_err(|error| TransactionError::Storage(error.to_string()))?
                    .ok_or(TransactionError::CorruptRowKey)?;
                let record = RowRecord::decode(&stored).map_err(TransactionError::CorruptRow)?;
                if record.row_version != candidate.locator.row_version {
                    return Err(TransactionError::CorruptRowKey);
                }
                let derived = derive_index_key(
                    index_id,
                    row_id.row,
                    schema.schema_version,
                    &schema.column_ids,
                    &schema.columns,
                    &record.payload,
                    &index.column_ids,
                )
                .map_err(|error| match error {
                    DeriveIndexKeyError::InvalidDefinition => TransactionError::CorruptCatalog,
                    DeriveIndexKeyError::Tuple(error) => TransactionError::Tuple(error),
                })?;
                if derived.key != candidate.key {
                    return Err(TransactionError::CorruptRowKey);
                }
                let tuple = decode_tuple(&schema.columns, schema.schema_version, &record.payload)
                    .map_err(TransactionError::Tuple)?;
                if tuple.values[key_ordinal].is_some() && in_range(&candidate.key) {
                    candidate_row_bytes = candidate_row_bytes.saturating_add(record.payload.len());
                    if candidate_row_bytes > MAX_AUTHORIZED_INDEX_RANGE_BYTES {
                        return Ok(AuthorizedIndexRangeScan::Unavailable);
                    }
                    candidate_ids.insert(row_id);
                }
            }
            for row_bytes in overlay_row_ids {
                let row_id = RowId {
                    table: table_id,
                    row: row_bytes,
                };
                match self.overlay.get_for_actor(&row_id, self.principal_id)? {
                    Some(StagedRow::Delete) => {
                        candidate_ids.remove(&row_id);
                    }
                    Some(StagedRow::Value(payload)) => {
                        let derived = derive_index_key(
                            index_id,
                            row_id.row,
                            schema.schema_version,
                            &schema.column_ids,
                            &schema.columns,
                            payload,
                            &index.column_ids,
                        )
                        .map_err(|error| match error {
                            DeriveIndexKeyError::InvalidDefinition => {
                                TransactionError::CorruptCatalog
                            }
                            DeriveIndexKeyError::Tuple(error) => TransactionError::Tuple(error),
                        })?;
                        let tuple = decode_tuple(&schema.columns, schema.schema_version, payload)
                            .map_err(TransactionError::Tuple)?;
                        if tuple.values[key_ordinal].is_some() && in_range(&derived.key) {
                            candidate_row_bytes = candidate_row_bytes.saturating_add(payload.len());
                            if candidate_row_bytes > MAX_AUTHORIZED_INDEX_RANGE_BYTES {
                                return Ok(AuthorizedIndexRangeScan::Unavailable);
                            }
                            candidate_ids.insert(row_id);
                        } else {
                            candidate_ids.remove(&row_id);
                        }
                    }
                    None => {}
                }
            }
            if candidate_ids.len() > MAX_AUTHORIZED_INDEX_RANGE_CANDIDATES {
                return Ok(AuthorizedIndexRangeScan::Unavailable);
            }
            let mut candidate_ids = candidate_ids.into_iter().collect::<Vec<_>>();
            candidate_ids.sort_unstable_by_key(|id| id.row);
            let mut rows = Vec::with_capacity(candidate_ids.len());
            for candidate_id in candidate_ids {
                if let Some(cells) = self.read_authorized(candidate_id, projection)? {
                    rows.push((candidate_id, cells));
                }
            }
            Ok(AuthorizedIndexRangeScan::Rows(rows))
        })();
        match result {
            Ok(scan) => Ok(scan),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Catalog-derived entry point for a unique equality probe. Schema,
    /// principal/role grants, and row/cell policies all come from this
    /// invocation's pinned catalog before the projection-based trusted helper
    /// is called.
    pub fn probe_catalog_unique_index_for_principal(
        &mut self,
        table_id: [u8; 16],
        index_id: [u8; 16],
        values: &[Option<ScalarValue>],
    ) -> Result<AuthorizedUniqueIndexProbe, TransactionError> {
        let result = (|| {
            let projection = self.load_catalog_estimate_projection(table_id)?;
            self.load_catalog_indexes(&projection.schema)?;
            self.probe_catalog_unique_index(table_id, index_id, values, &projection.as_projection())
        })();
        match result {
            Ok(probe) => Ok(probe),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Catalog-derived entry point for a bounded leading-key range scan.
    pub fn scan_catalog_index_range_for_principal(
        &mut self,
        table_id: [u8; 16],
        index_id: [u8; 16],
        lower: Option<AuthorizedIndexRangeBound>,
        upper: Option<AuthorizedIndexRangeBound>,
    ) -> Result<AuthorizedIndexRangeScan, TransactionError> {
        let result = (|| {
            let catalog_projection = self.load_catalog_estimate_projection(table_id)?;
            self.load_catalog_indexes(&catalog_projection.schema)?;
            self.scan_catalog_index_range(
                table_id,
                index_id,
                lower,
                upper,
                &catalog_projection.as_projection(),
            )
        })();
        match result {
            Ok(scan) => Ok(scan),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    fn load_catalog_executor_write_projection(
        &mut self,
        table_id: [u8; 16],
    ) -> Result<CatalogTableProjection, TransactionError> {
        let schema = self.load_catalog_table_schema(table_id)?;
        let principal_id = self.principal_id;
        let (role_ids, grants) = self.load_catalog_grants(principal_id, &schema)?;
        let (policies, policy_driving_columns) =
            self.load_catalog_policies_with_policy_inputs(principal_id, &schema, true)?;
        let checks = self.load_catalog_check_constraints(&schema)?;
        self.load_catalog_indexes(&schema)?;
        let scope = self.scope_from_grants(&grants);
        let context = AuthorizationContext {
            principal_id,
            role_ids,
            scope,
        };
        Ok(CatalogTableProjection {
            schema,
            context,
            grants,
            policies,
            checks,
            policy_driving_columns,
        })
    }

    fn load_catalog_estimate_projection(
        &mut self,
        table_id: [u8; 16],
    ) -> Result<CatalogTableProjection, TransactionError> {
        let schema = self.load_catalog_table_schema(table_id)?;
        let principal_id = self.principal_id;
        let (role_ids, grants) = self.load_catalog_grants(principal_id, &schema)?;
        let policies = self.load_catalog_policies(principal_id, &schema)?;
        let scope = self.scope_from_grants(&grants);
        let context = AuthorizationContext {
            principal_id,
            role_ids,
            scope,
        };
        let mut referenced_ordinals = HashSet::new();
        for policy in &policies {
            if let Some(expression) = &policy.using {
                collect_expression_fields(expression, &mut referenced_ordinals);
            }
            if let Some(expression) = &policy.with_check {
                collect_expression_fields(expression, &mut referenced_ordinals);
            }
        }
        let mut policy_driving_columns = referenced_ordinals
            .into_iter()
            .map(|ordinal| {
                schema
                    .column_ids
                    .get(ordinal)
                    .copied()
                    .ok_or(TransactionError::CorruptCatalog)
            })
            .collect::<Result<Vec<_>, _>>()?;
        policy_driving_columns.sort_unstable();
        Ok(CatalogTableProjection {
            schema,
            context,
            grants,
            policies,
            checks: Vec::new(),
            policy_driving_columns,
        })
    }

    fn discard_active_scan_for_table(&mut self, table_id: [u8; 16]) {
        if self
            .active_scan
            .as_ref()
            .is_some_and(|active| active.table_id == table_id)
        {
            self.active_scan = None;
        }
    }

    /// Estimate whether the authorized population fits under a fixed bound.
    /// It reads at most `maximum_rows + 1` snapshot keys in bounded pages and
    /// does not decode row values, so larger tables fall back without a full
    /// table scan.
    pub fn estimate_authorized_population_at_most(
        &mut self,
        table_id: [u8; 16],
        projection: &AuthorizedProjection<'_>,
        maximum_rows: u64,
    ) -> Result<Option<u64>, TransactionError> {
        self.check_deadline()?;
        if maximum_rows > MAX_ESTIMATE_CANDIDATES as u64
            || !valid_estimate_projection(table_id, projection)
        {
            return Ok(None);
        }
        if let Err(error) =
            self.capture_authority_dependencies(projection.context, projection.dependencies)
        {
            return if estimate_error_is_terminal(&error) {
                Err(error)
            } else {
                Ok(None)
            };
        }
        match estimate_population(table_id, projection) {
            Some(EstimatePopulation::NoRows) => return self.finish_population_probe(Some(0)),
            Some(EstimatePopulation::AllRows) => {}
            None => return self.finish_population_probe(None),
        }
        let Some(candidate_limit) = usize::try_from(maximum_rows)
            .ok()
            .and_then(|rows| rows.checked_add(1))
        else {
            return self.finish_population_probe(None);
        };
        let mut snapshot_ids = Vec::with_capacity(candidate_limit.min(MAX_ROW_SCAN_PAGE));
        let mut after_row_id = None;
        loop {
            let limit = candidate_limit
                .saturating_sub(snapshot_ids.len())
                .min(MAX_ROW_SCAN_PAGE);
            if limit == 0 {
                return self.finish_population_probe(None);
            }
            let (page, complete) =
                match self
                    .pinned_snapshot()?
                    .scan_row_ids_after(table_id, after_row_id, limit)
                {
                    Ok(page) => page,
                    Err(_) => return self.finish_population_probe(None),
                };
            if page.is_empty() {
                break;
            }
            after_row_id = page.last().copied();
            snapshot_ids.extend(page);
            if complete {
                break;
            }
        }
        if snapshot_ids.len() == candidate_limit {
            return self.finish_population_probe(None);
        }
        let mut visible_rows = snapshot_ids.into_iter().collect::<HashSet<_>>();
        let overlay_limit = match self.overlay.staged_row_count() {
            Ok(rows) if rows <= maximum_rows as usize => rows,
            Ok(_) => return self.finish_population_probe(None),
            Err(OverlayError::Aborted) => return Err(TransactionError::Aborted),
            Err(_) => return self.finish_population_probe(None),
        };
        let overlay_row_ids = match self
            .overlay
            .bounded_row_ids_for_table(table_id, overlay_limit)
        {
            Ok(Some(row_ids)) => row_ids,
            Ok(None) => return self.finish_population_probe(None),
            Err(OverlayError::Aborted) => return Err(TransactionError::Aborted),
            Err(_) => return self.finish_population_probe(None),
        };
        for row_id in overlay_row_ids {
            let staged = match self.overlay.get_for_actor(
                &RowId {
                    table: table_id,
                    row: row_id,
                },
                self.principal_id,
            ) {
                Ok(Some(staged)) => staged,
                Ok(None) => return self.finish_population_probe(None),
                Err(OverlayError::Aborted) => return Err(TransactionError::Aborted),
                Err(_) => return self.finish_population_probe(None),
            };
            match staged {
                StagedRow::Value(_) => {
                    visible_rows.insert(row_id);
                }
                StagedRow::Delete => {
                    visible_rows.remove(&row_id);
                }
            }
            if visible_rows.len() > maximum_rows as usize {
                return self.finish_population_probe(None);
            }
        }
        self.finish_population_probe(Some(visible_rows.len() as u64))
    }

    /// Estimate exact matches for a bounded predicate over an authorized
    /// population. This intentionally returns unknown unless every referenced
    /// cell is readable for every candidate row.
    pub fn estimate_authorized_predicate_at_most(
        &mut self,
        table_id: [u8; 16],
        projection: &AuthorizedProjection<'_>,
        predicate: &crate::optimizer_binding::BoundPredicate,
        maximum_rows: u64,
    ) -> Result<Option<(u64, u64)>, TransactionError> {
        self.check_deadline()?;
        if maximum_rows > MAX_PREDICATE_ESTIMATE_ROWS
            || !valid_estimate_projection(table_id, projection)
            || !valid_estimate_predicate(predicate, projection, 32, 256)
        {
            return Ok(None);
        }
        let Some(population) =
            self.estimate_authorized_population_at_most(table_id, projection, maximum_rows)?
        else {
            return Ok(None);
        };
        if population == 0 {
            return self.finish_predicate_estimate(Some((0, 0)));
        }
        let Ok(candidate_limit) = usize::try_from(maximum_rows + 1) else {
            return self.finish_predicate_estimate(None);
        };
        let mut row_ids = Vec::with_capacity(candidate_limit.min(MAX_ROW_SCAN_PAGE));
        let mut after_row_id = None;
        loop {
            let limit = candidate_limit
                .saturating_sub(row_ids.len())
                .min(MAX_ROW_SCAN_PAGE);
            if limit == 0 {
                return self.finish_predicate_estimate(None);
            }
            let (page, complete) =
                match self
                    .pinned_snapshot()?
                    .scan_row_ids_after(table_id, after_row_id, limit)
                {
                    Ok(page) => page,
                    Err(_) => return self.finish_predicate_estimate(None),
                };
            if page.is_empty() {
                if complete {
                    break;
                }
                return self.finish_predicate_estimate(None);
            }
            if page.len() < limit && !complete {
                return self.finish_predicate_estimate(None);
            }
            after_row_id = page.last().copied();
            row_ids.extend(page);
            if complete {
                break;
            }
        }
        if row_ids.len() == candidate_limit {
            return self.finish_predicate_estimate(None);
        }
        let overlay_limit = usize::try_from(maximum_rows).unwrap_or(0);
        let overlay_ids = match self
            .overlay
            .bounded_row_ids_for_table(table_id, overlay_limit)
        {
            Ok(Some(ids)) => ids,
            Ok(None) => return self.finish_predicate_estimate(None),
            Err(OverlayError::Aborted) => return Err(TransactionError::Aborted),
            Err(_) => return self.finish_predicate_estimate(None),
        };
        row_ids.extend(overlay_ids);
        row_ids.sort_unstable();
        row_ids.dedup();
        if row_ids.len() as u64 > maximum_rows {
            return self.finish_predicate_estimate(None);
        }

        let mut visible_rows = 0u64;
        let mut matching_rows = 0u64;
        for row_id in row_ids {
            self.check_deadline()?;
            let id = RowId {
                table: table_id,
                row: row_id,
            };
            match self.overlay.get_for_actor(&id, self.principal_id) {
                Ok(Some(StagedRow::Delete)) => continue,
                Ok(Some(StagedRow::Value(_))) | Ok(None) => {}
                Err(OverlayError::Aborted) => return Err(TransactionError::Aborted),
                Err(_) => return self.finish_predicate_estimate(None),
            }
            let cells = match self.project_row_for_estimate(id, projection) {
                Ok(Some(cells)) => cells,
                Ok(None) => return self.finish_predicate_estimate(None),
                Err(error) if estimate_error_is_terminal(&error) => return Err(error),
                Err(_) => return self.finish_predicate_estimate(None),
            };
            let Some(matches) = evaluate_estimate_predicate(predicate, &cells, projection, 0, 256)
            else {
                return self.finish_predicate_estimate(None);
            };
            visible_rows += 1;
            if matches == EstimateTruth::True {
                matching_rows += 1;
            }
        }
        if visible_rows != population {
            return self.finish_predicate_estimate(None);
        }
        self.finish_predicate_estimate(Some((visible_rows, matching_rows)))
    }

    fn finish_predicate_estimate(
        &mut self,
        estimate: Option<(u64, u64)>,
    ) -> Result<Option<(u64, u64)>, TransactionError> {
        self.check_deadline()?;
        Ok(estimate)
    }

    fn finish_population_probe(
        &mut self,
        visible_rows: Option<u64>,
    ) -> Result<Option<u64>, TransactionError> {
        self.check_deadline()?;
        Ok(visible_rows)
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
        match estimate_population(table_id, projection) {
            Some(EstimatePopulation::AllRows) => {}
            Some(EstimatePopulation::NoRows) => {
                return self.finish_estimate(AuthorizedScanEstimate::Exact(
                    AuthorizedScanSummary {
                        visible_rows: 0,
                        columns: vec![ColumnPopulation::default(); projection.columns.len()],
                    },
                ));
            }
            None => return self.finish_estimate(AuthorizedScanEstimate::Unknown),
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
        self.list_catalog_authorized_relations_with_extra_bytes(
            schema_id,
            page_limit,
            continuation,
            0,
        )
    }

    /// Describe a relation's declared columns using grants derived from the
    /// pinned catalog. Both schema and relation IDs are references only.
    pub fn describe_catalog_authorized_columns_page(
        &mut self,
        schema_id: [u8; 16],
        table_id: [u8; 16],
        page_limit: usize,
        continuation: Option<ColumnScanCursor>,
    ) -> Result<Option<VisibleColumnPage>, TransactionError> {
        let result = (|| {
            if page_limit == 0 || page_limit > MAX_COLUMNS_PER_PAGE {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            if schema_id == INFORMATION_SCHEMA_ID {
                if continuation.is_some() {
                    return Err(TransactionError::InvalidMetadataRequest);
                }
                let Some(description) = builtin_information_schema_relation_schema(table_id) else {
                    return Ok(None);
                };
                if description.columns.len() > page_limit {
                    return Err(TransactionError::InvalidMetadataRequest);
                }
                return Ok(Some(VisibleColumnPage {
                    relation_id: table_id,
                    columns: description.columns,
                    continuation: None,
                }));
            }
            if schema_id == [0; 16] {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let principal_id = self.principal_id;
            let (role_ids, grants) = self.load_catalog_relation_discovery_grants(schema_id)?;
            let scope = self.scope_from_grants(&grants);
            let context = AuthorizationContext {
                principal_id,
                role_ids,
                scope,
            };
            self.describe_authorized_columns_page(
                table_id,
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

    fn list_catalog_authorized_relations_with_extra_bytes(
        &mut self,
        schema_id: [u8; 16],
        page_limit: usize,
        continuation: Option<RelationScanCursor>,
        extra_bytes_per_relation: usize,
    ) -> Result<Option<VisibleRelationPage>, TransactionError> {
        let result = (|| {
            if schema_id == [0; 16] || page_limit == 0 || page_limit > MAX_RELATIONS_PER_PAGE {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let principal_id = self.principal_id;
            let (role_ids, grants) = self.load_catalog_relation_discovery_grants(schema_id)?;
            let scope = self.scope_from_grants(&grants);
            let context = AuthorizationContext {
                principal_id,
                role_ids,
                scope,
            };
            self.list_authorized_relations_with_extra_bytes(
                schema_id,
                page_limit,
                continuation,
                extra_bytes_per_relation,
                MetadataDiscoveryAccess {
                    context: &context,
                    grants: &grants,
                    dependencies: &[],
                },
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
            let schema_scope = self.scope_from_grants(&schema_grants);
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

            if schema.schema_id == INFORMATION_SCHEMA_ID {
                return self.resolve_authorized_relation(
                    schema.schema_id,
                    relation_name,
                    &schema_context,
                    &schema_grants,
                    &[],
                );
            }

            let (relation_roles, relation_grants) =
                self.load_catalog_relation_discovery_grants(schema.schema_id)?;
            let relation_scope = self.scope_from_grants(&relation_grants);
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
        self.list_authorized_relations_with_extra_bytes(
            schema_id,
            page_limit,
            continuation,
            0,
            MetadataDiscoveryAccess {
                context,
                grants,
                dependencies,
            },
        )
    }

    fn list_authorized_relations_with_extra_bytes(
        &mut self,
        schema_id: [u8; 16],
        page_limit: usize,
        continuation: Option<RelationScanCursor>,
        extra_bytes_per_relation: usize,
        access: MetadataDiscoveryAccess<'_>,
    ) -> Result<Option<VisibleRelationPage>, TransactionError> {
        let result = (|| {
            let MetadataDiscoveryAccess {
                context,
                grants,
                dependencies,
            } = access;
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
                        || active.extra_bytes_per_relation != extra_bytes_per_relation
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
                    extra_bytes_per_relation,
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
            let mut table_ids = self.scan_relation_ids_for_schema(schema_id)?;
            if table_ids.is_empty() {
                // Retain bounded support for snapshots written before the
                // schema-scoped relation-name index was maintained.
                for table_record in self.scan_catalog_family(5)? {
                    if table_record.family_code != 5 {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    if catalog_id(&table_record, 1)? != schema_id {
                        continue;
                    }
                    let table_id = table_record
                        .logical_key
                        .get(3..19)
                        .ok_or(TransactionError::CorruptCatalog)?
                        .try_into()
                        .map_err(|_| TransactionError::CorruptCatalog)?;
                    if table_id == [0; 16] {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    table_ids.push(table_id);
                }
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
                    .and_then(|size| size.checked_add(extra_bytes_per_relation))
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
                let token = new_metadata_cursor_token(
                    &mut active_scan.used_tokens,
                    RELATION_CURSOR_TOKEN_KIND,
                )?;
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
    /// The reserved information_schema relations have fixed public shapes.
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
            if schema_id == INFORMATION_SCHEMA_ID {
                self.check_deadline()?;
                let Some(relation) = builtin_information_schema_relation(relation_name) else {
                    return Ok(None);
                };
                let schema = builtin_information_schema_relation_schema(relation.relation_id)
                    .ok_or(TransactionError::CorruptCatalog)?;
                let response_bytes = relation
                    .name
                    .len()
                    .checked_add(24)
                    .and_then(|bytes| {
                        schema.columns.iter().try_fold(bytes, |total, column| {
                            total
                                .checked_add(column.name.len())
                                .and_then(|sum| sum.checked_add(32))
                        })
                    })
                    .ok_or(TransactionError::MetadataResultTooLarge)?;
                if response_bytes > MAX_METADATA_PAGE_BYTES {
                    return Err(TransactionError::MetadataResultTooLarge);
                }
                return Ok(Some(AuthorizedRelationDescription {
                    relation_id: relation.relation_id,
                    name: relation.name,
                    columns: schema.columns,
                }));
            }
            let indexed = self
                .pinned_snapshot()?
                .get_relation_name_index_value(schema_id, relation_name.as_bytes())
                .map_err(|error| TransactionError::Storage(error.to_string()))?;
            if let Some(indexed) = indexed {
                let relation_id: [u8; 16] = indexed
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                if relation_id == [0; 16] {
                    return Err(TransactionError::CorruptCatalog);
                }
                let Some(description) =
                    self.describe_authorized_columns(relation_id, context, grants, dependencies)?
                else {
                    return Ok(None);
                };
                let relation = self.read_discovery_relation(schema_id, relation_id)?;
                if relation.name != relation_name {
                    return Err(TransactionError::CorruptCatalog);
                }
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
                    relation_id,
                    name: relation.name,
                    columns: description.columns,
                }));
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
        self.describe_authorized_columns_page_with_extra_bytes(
            table_id,
            page_limit,
            continuation,
            0,
            MetadataDiscoveryAccess {
                context,
                grants,
                dependencies,
            },
        )
    }

    fn describe_authorized_columns_page_with_extra_bytes(
        &mut self,
        table_id: [u8; 16],
        page_limit: usize,
        continuation: Option<ColumnScanCursor>,
        extra_bytes_per_column: usize,
        access: MetadataDiscoveryAccess<'_>,
    ) -> Result<Option<VisibleColumnPage>, TransactionError> {
        let result = (|| {
            let MetadataDiscoveryAccess {
                context,
                grants,
                dependencies,
            } = access;
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
                        || active.extra_bytes_per_column != extra_bytes_per_column
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
                    extra_bytes_per_column,
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
                    .and_then(|size| size.checked_add(extra_bytes_per_column))
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
                let token = new_metadata_cursor_token(
                    &mut active_scan.used_tokens,
                    COLUMN_CURSOR_TOKEN_KIND,
                )?;
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

    /// List schemas for which the invocation has schema USAGE, plus the public
    /// reserved information_schema namespace.
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
            let scope = self.scope_from_grants(&grants);
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

    /// Resolve one normalized principal name only for a caller with the
    /// corresponding identity-management privilege or database-admin role.
    /// The lookup is non-enumerating and remains within this pinned snapshot.
    pub(crate) fn resolve_catalog_administrative_identity_name(
        &mut self,
        name: &str,
        kind: crate::metadata_discovery::AdministrativeIdentityKind,
    ) -> Result<Option<[u8; 16]>, TransactionError> {
        let bytes = name.as_bytes();
        if bytes.is_empty()
            || bytes.len() > 128
            || !(bytes[0].is_ascii_lowercase() || bytes[0] == b'_')
            || !bytes
                .iter()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
        {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        let roles = self.load_catalog_role_closure(self.principal_id)?;
        let is_database_admin = roles.contains(&crate::bootstrap::database_admin_role_id());
        let metadata_bytes = self
            .pinned_snapshot()?
            .get_engine_metadata_value()
            .map_err(|error| TransactionError::Storage(error.to_string()))?
            .ok_or(TransactionError::NotReady)?;
        let metadata = EngineMetadata::decode(&metadata_bytes)
            .map_err(|error| TransactionError::Storage(error.to_string()))?;
        let mut subjects = roles.clone();
        subjects.insert(self.principal_id);
        let manages_identity_kind = |this: &mut Self, privilege_code: u16| {
            if is_database_admin {
                return Ok(true);
            }
            for subject in &subjects {
                let mut key = 12u16.to_be_bytes().to_vec();
                key.extend_from_slice(&[1]);
                key.extend_from_slice(subject);
                key.extend_from_slice(&[1]);
                key.extend_from_slice(&metadata.database_id);
                key.extend_from_slice(&[1]);
                key.extend_from_slice(&privilege_code.to_be_bytes());
                key.extend_from_slice(&[1]);
                key.extend_from_slice(&1u16.to_be_bytes());
                key.extend_from_slice(&[1]);
                key.extend_from_slice(&metadata.database_id);
                if let Some(record) = this.read_catalog_record(&key)? {
                    if record.family_code != 12
                        || catalog_id(&record, 1)? == [0; 16]
                        || catalog_u64(&record, 2)? == 0
                    {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    return Ok(true);
                }
            }
            Ok(false)
        };
        let manages_users = manages_identity_kind(self, 12)?;
        let manages_roles = manages_identity_kind(self, 13)?;
        let can_lookup_any = match kind {
            crate::metadata_discovery::AdministrativeIdentityKind::User => manages_users,
            crate::metadata_discovery::AdministrativeIdentityKind::Role => manages_roles,
            crate::metadata_discovery::AdministrativeIdentityKind::Either => {
                manages_users || manages_roles
            }
        };
        if !can_lookup_any {
            return Ok(None);
        }
        let snapshot = self.pinned_snapshot()?;
        let name_index = snapshot
            .read_transaction()
            .open_table(PRINCIPAL_NAMES)
            .map_err(|error| TransactionError::Storage(error.to_string()))?;
        let Some(identity_id) = name_index
            .get(bytes)
            .map_err(|error| TransactionError::Storage(error.to_string()))?
        else {
            return Ok(None);
        };
        let identity_id: [u8; 16] = identity_id
            .value()
            .try_into()
            .map_err(|_| TransactionError::CorruptCatalog)?;
        if identity_id == [0; 16] {
            return Err(TransactionError::CorruptCatalog);
        }
        let principal = self
            .read_catalog_record(&catalog_id_key(10, identity_id))?
            .ok_or(TransactionError::CorruptCatalog)?;
        if principal.family_code != 10 {
            return Err(TransactionError::CorruptCatalog);
        }
        let principal_kind = catalog_u16(&principal, 1)?;
        if catalog_field(&principal, 2)? != bytes {
            return Err(TransactionError::CorruptCatalog);
        }
        let can_inspect_kind = match (kind, principal_kind) {
            (crate::metadata_discovery::AdministrativeIdentityKind::User, 1) => manages_users,
            (crate::metadata_discovery::AdministrativeIdentityKind::Role, 2) => manages_roles,
            (crate::metadata_discovery::AdministrativeIdentityKind::Either, 1) => manages_users,
            (crate::metadata_discovery::AdministrativeIdentityKind::Either, 2) => manages_roles,
            (_, 1 | 2) => false,
            _ => return Err(TransactionError::CorruptCatalog),
        };
        if !can_inspect_kind {
            return Ok(None);
        }
        Ok(Some(identity_id))
    }

    /// Resolve a policy recipient only when the authenticated principal can
    /// administer policies on this table. This keeps name lookup scoped to
    /// the object whose policy is being created.
    pub(crate) fn resolve_catalog_policy_identity_name(
        &mut self,
        schema_id: [u8; 16],
        table_id: [u8; 16],
        name: &str,
        kind: crate::metadata_discovery::AdministrativeIdentityKind,
    ) -> Result<Option<[u8; 16]>, TransactionError> {
        let bytes = name.as_bytes();
        if bytes.is_empty()
            || bytes.len() > 128
            || !(bytes[0].is_ascii_lowercase() || bytes[0] == b'_')
            || !bytes
                .iter()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
            || schema_id == [0; 16]
            || table_id == [0; 16]
        {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        let roles = self.load_catalog_role_closure(self.principal_id)?;
        let mut subjects = roles.clone();
        subjects.insert(self.principal_id);
        let is_database_admin = roles.contains(&crate::bootstrap::database_admin_role_id());
        let schema_key = catalog_id_key(2, schema_id);
        let table_key = catalog_id_key(5, table_id);
        let Some(schema) = self.read_catalog_record(&schema_key)? else {
            return Ok(None);
        };
        let Some(table) = self.read_catalog_record(&table_key)? else {
            return Ok(None);
        };
        if schema.family_code != 2
            || catalog_u16(&schema, 1)? != 1
            || table.family_code != 5
            || catalog_id(&table, 1)? != schema_id
        {
            return Ok(None);
        }
        let schema_object = self
            .read_catalog_record(&catalog_id_key(2, schema_id))?
            .ok_or(TransactionError::CorruptCatalog)?;
        let table_object = self
            .read_catalog_record(&catalog_id_key(2, table_id))?
            .ok_or(TransactionError::CorruptCatalog)?;
        if schema_object.family_code != 2
            || catalog_u16(&schema_object, 1)? != 1
            || table_object.family_code != 2
            || catalog_u16(&table_object, 1)? != 3
        {
            return Err(TransactionError::CorruptCatalog);
        }
        let mut has_policy_authority = is_database_admin
            || subjects.contains(&catalog_id(&schema_object, 4)?)
            || subjects.contains(&catalog_id(&table_object, 4)?);
        for object_id in [schema_id, table_id] {
            if has_policy_authority {
                break;
            }
            let grants = self
                .pinned_snapshot()?
                .lookup_grants_for_object(object_id)
                .map_err(|error| match error {
                    SnapshotError::Storage(error) => TransactionError::Storage(error),
                    _ => TransactionError::CorruptCatalog,
                })?;
            self.charge_scan_work(grants.len())?;
            for grant in grants {
                let key = &grant.logical_key;
                if grant.family_code != 12
                    || key.len() != 59
                    || key[0..2] != 12u16.to_be_bytes()
                    || key[2] != 1
                    || key[19] != 1
                    || key[20..36] != object_id
                    || key[36] != 1
                    || key[37..39] != 10u16.to_be_bytes()
                    || key[39] != 1
                    || key[40..42] != 1u16.to_be_bytes()
                    || key[42] != 1
                    || key[43..59] != object_id
                {
                    continue;
                }
                if catalog_u64(&grant, 2)? == 0 {
                    return Err(TransactionError::CorruptCatalog);
                }
                let grantee: [u8; 16] = key[3..19]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                if grantee == [0; 16] {
                    return Err(TransactionError::CorruptCatalog);
                }
                if subjects.contains(&grantee) {
                    has_policy_authority = true;
                    break;
                }
            }
        }
        if !has_policy_authority {
            return Ok(None);
        }
        let snapshot = self.pinned_snapshot()?;
        let name_index = snapshot
            .read_transaction()
            .open_table(PRINCIPAL_NAMES)
            .map_err(|error| TransactionError::Storage(error.to_string()))?;
        let Some(identity_id) = name_index
            .get(bytes)
            .map_err(|error| TransactionError::Storage(error.to_string()))?
        else {
            return Ok(None);
        };
        let identity_id: [u8; 16] = identity_id
            .value()
            .try_into()
            .map_err(|_| TransactionError::CorruptCatalog)?;
        if identity_id == [0; 16] {
            return Err(TransactionError::CorruptCatalog);
        }
        let principal = self
            .read_catalog_record(&catalog_id_key(10, identity_id))?
            .ok_or(TransactionError::CorruptCatalog)?;
        if principal.family_code != 10 {
            return Err(TransactionError::CorruptCatalog);
        }
        let expected_kind = match kind {
            crate::metadata_discovery::AdministrativeIdentityKind::User => 1,
            crate::metadata_discovery::AdministrativeIdentityKind::Role => 2,
            crate::metadata_discovery::AdministrativeIdentityKind::Either => {
                return Err(TransactionError::InvalidMetadataRequest);
            }
        };
        if catalog_u16(&principal, 1)? != expected_kind || catalog_field(&principal, 2)? != bytes {
            return Err(TransactionError::CorruptCatalog);
        }
        Ok(Some(identity_id))
    }

    /// Resolve a grant target and recipient only for an actor authorized to
    /// administer grants on that schema or table. Mutation rechecks authority.
    pub(crate) fn resolve_catalog_grant_target_identity_name(
        &mut self,
        schema_name: &str,
        table_name: Option<&str>,
        name: &str,
        kind: crate::metadata_discovery::AdministrativeIdentityKind,
    ) -> Result<
        Option<crate::metadata_discovery::AdministrativeGrantIdentityResolution>,
        TransactionError,
    > {
        let valid_name = |value: &str| {
            !value.is_empty()
                && value.len() <= 128
                && (value.as_bytes()[0].is_ascii_lowercase() || value.as_bytes()[0] == b'_')
                && value
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        };
        if !valid_name(schema_name) || table_name.is_some_and(|name| !valid_name(name)) {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        let indexed_schema_id = {
            self.pinned_snapshot()?
                .get_schema_name_index_value(schema_name.as_bytes())
                .map_err(|error| TransactionError::Storage(error.to_string()))?
        };
        let Some(schema_id) = indexed_schema_id else {
            return Ok(None);
        };
        let schema_id: [u8; 16] = schema_id
            .try_into()
            .map_err(|_| TransactionError::CorruptCatalog)?;
        if schema_id == [0; 16] {
            return Err(TransactionError::CorruptCatalog);
        }
        let schema_object = self
            .read_catalog_record(&catalog_id_key(2, schema_id))?
            .ok_or(TransactionError::CorruptCatalog)?;
        if schema_object.family_code != 2
            || catalog_u16(&schema_object, 1)? != 1
            || catalog_field(&schema_object, 3)? != schema_name.as_bytes()
        {
            return Err(TransactionError::CorruptCatalog);
        }
        let object_id = if let Some(table_name) = table_name {
            let indexed_table_id = {
                self.pinned_snapshot()?
                    .get_relation_name_index_value(schema_id, table_name.as_bytes())
                    .map_err(|error| TransactionError::Storage(error.to_string()))?
            };
            let Some(table_id) = indexed_table_id else {
                return Ok(None);
            };
            let table_id: [u8; 16] = table_id
                .try_into()
                .map_err(|_| TransactionError::CorruptCatalog)?;
            if table_id == [0; 16] {
                return Err(TransactionError::CorruptCatalog);
            }
            let table_object = self
                .read_catalog_record(&catalog_id_key(2, table_id))?
                .ok_or(TransactionError::CorruptCatalog)?;
            if table_object.family_code != 2
                || catalog_u16(&table_object, 1)? != 3
                || catalog_optional_id(&table_object, 2)? != Some(schema_id)
                || catalog_field(&table_object, 3)? != table_name.as_bytes()
            {
                return Err(TransactionError::CorruptCatalog);
            }
            table_id
        } else {
            schema_id
        };
        Ok(self
            .resolve_catalog_grant_identity_name(object_id, name, kind)?
            .map(
                |identity_id| crate::metadata_discovery::AdministrativeGrantIdentityResolution {
                    schema_id,
                    object_id,
                    identity_id,
                },
            ))
    }

    /// Resolve a grant recipient only for an actor authorized to administer
    /// grants on the target schema or table. Mutation rechecks authority.
    pub(crate) fn resolve_catalog_grant_identity_name(
        &mut self,
        object_id: [u8; 16],
        name: &str,
        kind: crate::metadata_discovery::AdministrativeIdentityKind,
    ) -> Result<Option<[u8; 16]>, TransactionError> {
        self.resolve_catalog_object_ddl_identity_name(object_id, name, kind, 11)
    }

    /// Resolve an owner name only for an actor with ALTER authority over the
    /// target schema or table. The owner-transfer operation rechecks authority.
    pub(crate) fn resolve_catalog_alter_identity_name(
        &mut self,
        object_id: [u8; 16],
        name: &str,
        kind: crate::metadata_discovery::AdministrativeIdentityKind,
    ) -> Result<Option<[u8; 16]>, TransactionError> {
        self.resolve_catalog_object_ddl_identity_name(object_id, name, kind, 8)
    }

    fn resolve_catalog_object_ddl_identity_name(
        &mut self,
        object_id: [u8; 16],
        name: &str,
        kind: crate::metadata_discovery::AdministrativeIdentityKind,
        privilege_code: u16,
    ) -> Result<Option<[u8; 16]>, TransactionError> {
        if name.is_empty()
            || name.len() > 128
            || !(name.as_bytes()[0].is_ascii_lowercase() || name.as_bytes()[0] == b'_')
            || !name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
            || object_id == [0; 16]
            || !matches!(privilege_code, 8 | 9 | 11)
        {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        let Some(object) = self.read_catalog_record(&catalog_id_key(2, object_id))? else {
            return Ok(None);
        };
        if object.family_code != 2 {
            return Ok(None);
        }
        let object_kind = catalog_u16(&object, 1)?;
        let (schema_id, target_kind) = match object_kind {
            1 => (object_id, 1),
            3 => {
                let Some(schema_id) = catalog_optional_id(&object, 2)? else {
                    return Err(TransactionError::CorruptCatalog);
                };
                (schema_id, 3)
            }
            _ => return Ok(None),
        };
        let roles = self.load_catalog_role_closure(self.principal_id)?;
        let mut subjects = roles.clone();
        subjects.insert(self.principal_id);
        let mut authorized = roles.contains(&crate::bootstrap::database_admin_role_id());
        if target_kind == 1 {
            authorized |= subjects.contains(&catalog_id(&object, 4)?);
        } else {
            let schema = self
                .read_catalog_record(&catalog_id_key(2, schema_id))?
                .ok_or(TransactionError::CorruptCatalog)?;
            let table = self
                .read_catalog_record(&catalog_id_key(5, object_id))?
                .ok_or(TransactionError::CorruptCatalog)?;
            if schema.family_code != 2 || catalog_u16(&schema, 1)? != 1 {
                return Err(TransactionError::CorruptCatalog);
            }
            if table.family_code != 5 || catalog_id(&table, 1)? != schema_id {
                return Err(TransactionError::CorruptCatalog);
            }
            authorized |= subjects.contains(&catalog_id(&schema, 4)?)
                || subjects.contains(&catalog_id(&object, 4)?);
        }
        let targets: &[([u8; 16], bool)] = if target_kind == 1 {
            &[(schema_id, true)]
        } else {
            &[(schema_id, true), (object_id, false)]
        };
        for (target, schema_scope) in targets {
            if authorized {
                break;
            }
            let grants = self
                .pinned_snapshot()?
                .lookup_grants_for_object(*target)
                .map_err(|error| match error {
                    SnapshotError::Storage(error) => TransactionError::Storage(error),
                    _ => TransactionError::CorruptCatalog,
                })?;
            self.charge_scan_work(grants.len())?;
            for grant in grants {
                let key = &grant.logical_key;
                if grant.family_code != 12
                    || key.len() != 59
                    || key[0..2] != 12u16.to_be_bytes()
                    || key[2] != 1
                    || key[19] != 1
                    || key[20..36] != *target
                    || key[36] != 1
                    || key[37..39] != privilege_code.to_be_bytes()
                    || key[39] != 1
                    || key[40..42] != 1u16.to_be_bytes()
                    || key[42] != 1
                    || key[43..59] != if *schema_scope { *target } else { object_id }
                {
                    continue;
                }
                if catalog_u64(&grant, 2)? == 0 {
                    return Err(TransactionError::CorruptCatalog);
                }
                let grantee: [u8; 16] = key[3..19]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                if grantee == [0; 16] {
                    return Err(TransactionError::CorruptCatalog);
                }
                if subjects.contains(&grantee) {
                    authorized = true;
                    break;
                }
            }
        }
        if !authorized {
            return Ok(None);
        }
        let snapshot = self.pinned_snapshot()?;
        let name_index = snapshot
            .read_transaction()
            .open_table(PRINCIPAL_NAMES)
            .map_err(|error| TransactionError::Storage(error.to_string()))?;
        let Some(identity_id) = name_index
            .get(name.as_bytes())
            .map_err(|error| TransactionError::Storage(error.to_string()))?
        else {
            return Ok(None);
        };
        let identity_id: [u8; 16] = identity_id
            .value()
            .try_into()
            .map_err(|_| TransactionError::CorruptCatalog)?;
        if identity_id == [0; 16] {
            return Err(TransactionError::CorruptCatalog);
        }
        let principal = self
            .read_catalog_record(&catalog_id_key(10, identity_id))?
            .ok_or(TransactionError::CorruptCatalog)?;
        if principal.family_code != 10 {
            return Err(TransactionError::CorruptCatalog);
        }
        let expected_kind = match kind {
            crate::metadata_discovery::AdministrativeIdentityKind::User => Some(1),
            crate::metadata_discovery::AdministrativeIdentityKind::Role => Some(2),
            crate::metadata_discovery::AdministrativeIdentityKind::Either => None,
        };
        if let Some(expected_kind) = expected_kind
            && catalog_u16(&principal, 1)? != expected_kind
        {
            return Err(TransactionError::CorruptCatalog);
        }
        if catalog_field(&principal, 2)? != name.as_bytes() {
            return Err(TransactionError::CorruptCatalog);
        }
        Ok(Some(identity_id))
    }

    /// Resolve a policy name only inside a table for which this actor has
    /// policy DDL authority. The mutating drop operation rechecks authority.
    pub(crate) fn resolve_catalog_policy_name(
        &mut self,
        schema_id: [u8; 16],
        table_id: [u8; 16],
        name: &str,
        is_cell: Option<bool>,
    ) -> Result<Option<[u8; 16]>, TransactionError> {
        if name.is_empty()
            || name.len() > 128
            || !name.as_bytes()[0].is_ascii_lowercase() && name.as_bytes()[0] != b'_'
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
            || schema_id == [0; 16]
            || table_id == [0; 16]
        {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        let roles = self.load_catalog_role_closure(self.principal_id)?;
        let mut subjects = roles.clone();
        subjects.insert(self.principal_id);
        let is_database_admin = roles.contains(&crate::bootstrap::database_admin_role_id());
        let Some(schema) = self.read_catalog_record(&catalog_id_key(2, schema_id))? else {
            return Ok(None);
        };
        let Some(table) = self.read_catalog_record(&catalog_id_key(5, table_id))? else {
            return Ok(None);
        };
        let Some(schema_object) = self.read_catalog_record(&catalog_id_key(2, schema_id))? else {
            return Err(TransactionError::CorruptCatalog);
        };
        let Some(table_object) = self.read_catalog_record(&catalog_id_key(2, table_id))? else {
            return Err(TransactionError::CorruptCatalog);
        };
        if schema.family_code != 2
            || catalog_u16(&schema, 1)? != 1
            || table.family_code != 5
            || catalog_id(&table, 1)? != schema_id
            || schema_object.family_code != 2
            || catalog_u16(&schema_object, 1)? != 1
            || table_object.family_code != 2
            || catalog_u16(&table_object, 1)? != 3
        {
            return Ok(None);
        }
        let mut authorized = is_database_admin
            || subjects.contains(&catalog_id(&schema_object, 4)?)
            || subjects.contains(&catalog_id(&table_object, 4)?);
        for object_id in [schema_id, table_id] {
            if authorized {
                break;
            }
            let grants = self
                .pinned_snapshot()?
                .lookup_grants_for_object(object_id)
                .map_err(|error| match error {
                    SnapshotError::Storage(error) => TransactionError::Storage(error),
                    _ => TransactionError::CorruptCatalog,
                })?;
            self.charge_scan_work(grants.len())?;
            for grant in grants {
                let key = &grant.logical_key;
                if grant.family_code != 12
                    || key.len() != 59
                    || key[0..2] != 12u16.to_be_bytes()
                    || key[2] != 1
                    || key[19] != 1
                    || key[20..36] != object_id
                    || key[36] != 1
                    || key[37..39] != 10u16.to_be_bytes()
                    || key[39] != 1
                    || key[40..42] != 1u16.to_be_bytes()
                    || key[42] != 1
                    || key[43..59] != object_id
                    || catalog_u64(&grant, 2)? == 0
                {
                    continue;
                }
                let grantee: [u8; 16] = key[3..19]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                if grantee == [0; 16] {
                    return Err(TransactionError::CorruptCatalog);
                }
                if subjects.contains(&grantee) {
                    authorized = true;
                    break;
                }
            }
        }
        if !authorized {
            return Ok(None);
        }
        for policy_id in self.scan_policy_ids_for_table(table_id)? {
            let policy = self
                .read_catalog_record(&catalog_id_key(13, policy_id))?
                .ok_or(TransactionError::CorruptCatalog)?;
            if policy.family_code != 13 || catalog_id(&policy, 1)? != table_id {
                return Err(TransactionError::CorruptCatalog);
            }
            let policy_kind = catalog_u16(&policy, 2)?;
            if !matches!(policy_kind, 1 | 2) {
                return Err(TransactionError::CorruptCatalog);
            }
            if let Some(is_cell) = is_cell {
                let expected_kind = if is_cell { 2 } else { 1 };
                if policy_kind != expected_kind {
                    continue;
                }
            }
            let object = self
                .read_catalog_record(&catalog_id_key(2, policy_id))?
                .ok_or(TransactionError::CorruptCatalog)?;
            if object.family_code != 2 || catalog_u16(&object, 1)? != 10 {
                return Err(TransactionError::CorruptCatalog);
            }
            if catalog_field(&object, 3)? == name.as_bytes() {
                return Ok(Some(policy_id));
            }
        }
        Ok(None)
    }

    /// List schemas visible for authenticated control operations. This entry
    /// point is separate from invocation discovery so DDL name binding never
    /// widens executor or SQL query grants.
    pub(crate) fn list_catalog_administrative_schemas(
        &mut self,
    ) -> Result<Vec<VisibleSchema>, TransactionError> {
        let roles = self.load_catalog_role_closure(self.principal_id)?;
        let database_admin = roles.contains(&crate::bootstrap::database_admin_role_id());
        let records = if database_admin {
            self.scan_catalog_family(2)?
        } else {
            let mut owner_ids = roles.clone();
            owner_ids.insert(self.principal_id);
            let mut owner_ids = owner_ids.into_iter().collect::<Vec<_>>();
            owner_ids.sort_unstable();
            let mut object_ids = Vec::new();
            for owner_id in owner_ids {
                object_ids.extend(self.scan_object_ids_for_owner(owner_id)?);
            }
            object_ids.sort_unstable();
            if object_ids.windows(2).any(|pair| pair[0] == pair[1]) {
                return Err(TransactionError::CorruptCatalog);
            }
            if object_ids.is_empty() {
                // Older low-level snapshots may contain object rows without
                // the derived owner index; retain the bounded compatibility
                // path when no indexed owner entry is found.
                self.scan_catalog_family(2)?
            } else {
                object_ids
                    .into_iter()
                    .map(|object_id| {
                        self.read_catalog_record(&catalog_id_key(2, object_id))?
                            .ok_or(TransactionError::CorruptCatalog)
                    })
                    .collect::<Result<Vec<_>, _>>()?
            }
        };
        let mut schemas = Vec::new();
        let mut encoded_bytes = 0usize;
        for record in records {
            if record.family_code != 2 {
                continue;
            }
            if record.logical_key.len() != 19
                || record.logical_key[2] != 1
                || record.logical_key[..2] != 2u16.to_be_bytes()
            {
                return Err(TransactionError::CorruptCatalog);
            }
            let object_id: [u8; 16] = record.logical_key[3..19]
                .try_into()
                .map_err(|_| TransactionError::CorruptCatalog)?;
            let object_kind = catalog_u16(&record, 1)?;
            let owner_id = catalog_id(&record, 4)?;
            if !database_admin && owner_id != self.principal_id && !roles.contains(&owner_id) {
                continue;
            }
            let schema_id = match object_kind {
                1 => object_id,
                3 if !database_admin => {
                    let Some(schema_id) = catalog_optional_id(&record, 2)? else {
                        return Err(TransactionError::CorruptCatalog);
                    };
                    let schema_object = self
                        .read_catalog_record(&catalog_id_key(2, schema_id))?
                        .ok_or(TransactionError::CorruptCatalog)?;
                    if schema_object.family_code != 2 || catalog_u16(&schema_object, 1)? != 1 {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    schema_id
                }
                _ => continue,
            };
            let schema = self.read_discovery_schema(schema_id)?;
            encoded_bytes = encoded_bytes
                .saturating_add(16)
                .saturating_add(schema.name.len());
            if schemas.len() >= 4096 || encoded_bytes > 1024 * 1024 {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            schemas.push(schema);
        }
        if !database_admin {
            let mut authority_subjects = roles.clone();
            authority_subjects.insert(self.principal_id);
            let mut discovered_schema_ids = schemas
                .iter()
                .map(|schema: &VisibleSchema| schema.schema_id)
                .collect::<HashSet<_>>();
            for grant in self.scan_catalog_family(12)? {
                if grant.logical_key.len() != 59
                    || grant.logical_key[2] != 1
                    || grant.logical_key[19] != 1
                    || grant.logical_key[36] != 1
                    || grant.logical_key[39] != 1
                    || grant.logical_key[42] != 1
                {
                    return Err(TransactionError::CorruptCatalog);
                }
                let privilege_code =
                    u16::from_be_bytes([grant.logical_key[37], grant.logical_key[38]]);
                if !matches!(privilege_code, 7..=9)
                    || grant.logical_key[40..42] != 1u16.to_be_bytes()
                {
                    continue;
                }
                let grantee_id: [u8; 16] = grant.logical_key[3..19]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                if !authority_subjects.contains(&grantee_id) {
                    continue;
                }
                let object_id: [u8; 16] = grant.logical_key[20..36]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                let target_id: [u8; 16] = grant.logical_key[43..59]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                if object_id == [0; 16]
                    || object_id != target_id
                    || grant.logical_key[36] != 1
                    || catalog_u64(&grant, 2)? == 0
                    || catalog_id(&grant, 1)? == [0; 16]
                {
                    return Err(TransactionError::CorruptCatalog);
                }
                let object = self
                    .read_catalog_record(&catalog_id_key(2, object_id))?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if object.family_code != 2 {
                    return Err(TransactionError::CorruptCatalog);
                }
                let schema_id = match (privilege_code, catalog_u16(&object, 1)?) {
                    (7, 1) => object_id,
                    (8, 1) => object_id,
                    (8 | 9, 3) => {
                        let Some(schema_id) = catalog_optional_id(&object, 2)? else {
                            return Err(TransactionError::CorruptCatalog);
                        };
                        schema_id
                    }
                    _ => continue,
                };
                let schema = self.read_discovery_schema(schema_id)?;
                if !discovered_schema_ids.insert(schema_id) {
                    continue;
                }
                encoded_bytes = encoded_bytes
                    .saturating_add(16)
                    .saturating_add(schema.name.len());
                if schemas.len() >= 4096 || encoded_bytes > 1024 * 1024 {
                    return Err(TransactionError::InvalidMetadataRequest);
                }
                schemas.push(schema);
            }
        }
        schemas.push(VisibleSchema {
            schema_id: INFORMATION_SCHEMA_ID,
            name: "information_schema".to_owned(),
        });
        schemas.sort_unstable_by_key(|schema| schema.schema_id);
        schemas.dedup_by_key(|schema| schema.schema_id);
        Ok(schemas)
    }

    /// List relations in a schema visible to this control principal through
    /// ordinary discovery, schema ownership, policy DDL, or scoped table DDL.
    /// `None` means no relation is visible under any of those authorities.
    pub(crate) fn list_catalog_administrative_relations(
        &mut self,
        schema_id: [u8; 16],
    ) -> Result<Option<Vec<VisibleRelation>>, TransactionError> {
        let roles = self.load_catalog_role_closure(self.principal_id)?;
        let schema_admin = self.has_schema_metadata_authority(schema_id, &roles)?;
        let has_schema_usage = if !schema_admin {
            let (_, schema_grants) = self.load_catalog_schema_usage_grants(self.principal_id)?;
            schema_grants.iter().any(|grant| {
                grant.operation == Operation::Usage
                    && grant.target == AccessTarget::Schema(schema_id)
            })
        } else {
            true
        };
        let mut relation_ids = self.scan_relation_ids_for_schema(schema_id)?;
        if relation_ids.is_empty() {
            for record in self.scan_catalog_family(5)? {
                if record.family_code != 5 {
                    return Err(TransactionError::CorruptCatalog);
                }
                if catalog_id(&record, 1)? == schema_id {
                    let relation_id = record
                        .logical_key
                        .get(3..19)
                        .ok_or(TransactionError::CorruptCatalog)?
                        .try_into()
                        .map_err(|_| TransactionError::CorruptCatalog)?;
                    relation_ids.push(relation_id);
                }
            }
        }
        relation_ids.sort_unstable();
        if relation_ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(TransactionError::CorruptCatalog);
        }
        let mut visible_relation_ids = if schema_admin {
            relation_ids.iter().copied().collect::<HashSet<_>>()
        } else if !has_schema_usage {
            HashSet::new()
        } else {
            let mut visible = HashSet::new();
            let mut continuation = None;
            loop {
                let Some(page) = self.list_catalog_authorized_relations(
                    schema_id,
                    crate::metadata_discovery::MAX_RELATIONS_PER_PAGE,
                    continuation,
                )?
                else {
                    return Ok(None);
                };
                visible.extend(
                    page.relations
                        .into_iter()
                        .map(|relation| relation.relation_id),
                );
                continuation = page.continuation;
                if continuation.is_none() {
                    break;
                }
            }
            visible
        };
        if !schema_admin {
            for relation_id in &relation_ids {
                let policy_authority = has_schema_usage
                    && self.has_catalog_policy_ddl_authority(schema_id, *relation_id, &roles)?;
                if policy_authority
                    || self.has_catalog_table_owner_authority(*relation_id, &roles)?
                    || self.current_principal_has_index_ddl_authority(*relation_id)?
                    || self.current_principal_has_alter_ddl_authority(*relation_id)?
                {
                    visible_relation_ids.insert(*relation_id);
                }
            }
        }
        if !schema_admin && !has_schema_usage && visible_relation_ids.is_empty() {
            return Ok(None);
        }
        let mut relations = Vec::new();
        let mut encoded_bytes = 0usize;
        for relation_id in relation_ids {
            if !visible_relation_ids.contains(&relation_id) {
                continue;
            }
            let relation = self.read_discovery_relation(schema_id, relation_id)?;
            encoded_bytes = encoded_bytes
                .saturating_add(16)
                .saturating_add(relation.name.len());
            if relations.len() >= 4096 || encoded_bytes > 1024 * 1024 {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            relations.push(relation);
        }
        Ok(Some(relations))
    }

    /// Describe a relation's declared shape for ordinary discovery or an
    /// authorized DDL target, including MANAGE_GRANTS, INDEX, and ALTER
    /// authorities.
    pub(crate) fn describe_catalog_administrative_columns(
        &mut self,
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    ) -> Result<Option<Vec<VisibleColumn>>, TransactionError> {
        let roles = self.load_catalog_role_closure(self.principal_id)?;
        let schema_admin = self.has_schema_metadata_authority(schema_id, &roles)?;
        let table_key = catalog_id_key(5, relation_id);
        let Some(table) = self.read_catalog_record(&table_key)? else {
            return Ok(None);
        };
        if table.family_code != 5 {
            return Err(TransactionError::CorruptCatalog);
        }
        if catalog_id(&table, 1)? != schema_id {
            return Ok(None);
        }
        if !schema_admin {
            let (_, schema_grants) = self.load_catalog_schema_usage_grants(self.principal_id)?;
            let has_usage = schema_grants.iter().any(|grant| {
                grant.operation == Operation::Usage
                    && grant.target == AccessTarget::Schema(schema_id)
            });
            let has_policy_metadata = has_usage
                && self.has_catalog_policy_ddl_authority(schema_id, relation_id, &roles)?;
            if !has_policy_metadata
                && !self.has_catalog_table_owner_authority(relation_id, &roles)?
                && !self.current_principal_has_grant_ddl_authority(relation_id)?
                && !self.current_principal_has_index_ddl_authority(relation_id)?
                && !self.current_principal_has_alter_ddl_authority(relation_id)?
            {
                return Ok(None);
            }
        }
        self.read_discovery_relation(schema_id, relation_id)?;
        let schema = self.load_catalog_table_schema(relation_id)?;
        let mut columns = Vec::with_capacity(schema.columns.len());
        let mut encoded_bytes = 0usize;
        for ordinal in 0..schema.columns.len() {
            let column = self.read_visible_column(relation_id, &schema, ordinal)?;
            encoded_bytes = encoded_bytes
                .saturating_add(24)
                .saturating_add(column.name.len())
                .saturating_add(
                    crate::metadata_discovery::scalar_type_name(column.scalar_type).len(),
                );
            if columns.len() >= 4096 || encoded_bytes > 1024 * 1024 {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            columns.push(column);
        }
        Ok(Some(columns))
    }

    /// List logical indexes for a relation when this control principal owns
    /// its schema or has scoped index DDL authority. Physical storage
    /// identifiers and membership records are never returned.
    pub(crate) fn list_catalog_administrative_indexes(
        &mut self,
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    ) -> Result<Option<Vec<crate::metadata_discovery::AdministrativeIndex>>, TransactionError> {
        let roles = self.load_catalog_role_closure(self.principal_id)?;
        let schema_admin = self.has_schema_metadata_authority(schema_id, &roles)?;
        let table_key = catalog_id_key(5, relation_id);
        let Some(table) = self.read_catalog_record(&table_key)? else {
            return Ok(None);
        };
        if table.family_code != 5 || catalog_id(&table, 1)? != schema_id {
            return Ok(None);
        }
        if !schema_admin && !self.current_principal_has_index_ddl_authority(relation_id)? {
            return Ok(None);
        }
        self.read_discovery_relation(schema_id, relation_id)?;
        let schema = self.load_catalog_table_schema(relation_id)?;
        let indexes = self.load_catalog_indexes(&schema)?;
        let mut descriptions = Vec::with_capacity(indexes.len());
        let mut encoded_bytes = 0usize;
        for index in indexes {
            let object = self
                .read_catalog_record(&catalog_id_key(2, index.index_id))?
                .ok_or(TransactionError::CorruptCatalog)?;
            if object.family_code != 2
                || catalog_u16(&object, 1)? != 7
                || catalog_optional_id(&object, 2)?
                    .is_some_and(|owner_schema| owner_schema != schema_id)
            {
                return Err(TransactionError::CorruptCatalog);
            }
            let name = String::from_utf8(catalog_field(&object, 3)?.to_vec())
                .map_err(|_| TransactionError::CorruptCatalog)?;
            encoded_bytes = encoded_bytes
                .saturating_add(17)
                .saturating_add(index.column_ids.len().saturating_mul(16))
                .saturating_add(name.len());
            if descriptions.len() >= 4096 || encoded_bytes > 1024 * 1024 {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            descriptions.push(crate::metadata_discovery::AdministrativeIndex {
                index_id: index.index_id,
                name,
                column_ids: index.column_ids,
                unique: index.unique,
            });
        }
        descriptions.sort_unstable_by_key(|index| index.index_id);
        Ok(Some(descriptions))
    }

    /// Describe a table's logical constraints to its schema/table owner, a
    /// table-scoped ALTER grantee, or a database administrator. CHECK
    /// expression bodies are validated but omitted. Foreign-key endpoints are
    /// returned only when the target schema is also administratively visible.
    pub(crate) fn list_catalog_administrative_constraints(
        &mut self,
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    ) -> Result<Option<Vec<crate::metadata_discovery::AdministrativeConstraint>>, TransactionError>
    {
        let roles = self.load_catalog_role_closure(self.principal_id)?;
        let table_key = catalog_id_key(5, relation_id);
        let Some(table) = self.read_catalog_record(&table_key)? else {
            return Ok(None);
        };
        if table.family_code != 5 || catalog_id(&table, 1)? != schema_id {
            return Ok(None);
        }
        let mut subjects = roles.clone();
        subjects.insert(self.principal_id);
        let schema_authority = self.has_schema_metadata_authority(schema_id, &roles)?;
        let table_object = self
            .read_catalog_record(&catalog_id_key(2, relation_id))?
            .ok_or(TransactionError::CorruptCatalog)?;
        if table_object.family_code != 2 || catalog_u16(&table_object, 1)? != 3 {
            return Err(TransactionError::CorruptCatalog);
        }
        let owner_authority = subjects.contains(&catalog_id(&table_object, 4)?);
        let alter_authority = self.current_principal_has_object_ddl_authority(relation_id, 8)?;
        if !schema_authority && !owner_authority && !alter_authority {
            return Ok(None);
        }
        self.read_discovery_relation(schema_id, relation_id)?;
        let schema = self.load_catalog_table_schema(relation_id)?;
        // Validate the complete logical definitions against the same pinned
        // catalog before projecting the secret-free administrative shape.
        self.load_catalog_indexes(&schema)?;
        self.load_catalog_check_constraints(&schema)?;

        let constraint_ids = self.scan_constraint_ids_for_table(relation_id)?;
        let records = if constraint_ids.is_empty() {
            self.scan_catalog_family(8)?
        } else {
            constraint_ids
                .into_iter()
                .map(|constraint_id| {
                    self.read_catalog_record(&catalog_id_key(8, constraint_id))?
                        .ok_or(TransactionError::CorruptCatalog)
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        let mut descriptions = Vec::new();
        let mut encoded_bytes = 0usize;
        for record in records {
            if record.family_code != 8 || record.logical_key.len() != 19 {
                return Err(TransactionError::CorruptCatalog);
            }
            if catalog_id(&record, 1)? != relation_id {
                continue;
            }
            let constraint_id: [u8; 16] = record.logical_key[3..19]
                .try_into()
                .map_err(|_| TransactionError::CorruptCatalog)?;
            let object = self
                .read_catalog_record(&catalog_id_key(2, constraint_id))?
                .ok_or(TransactionError::CorruptCatalog)?;
            let name = std::str::from_utf8(catalog_field(&object, 3)?)
                .map_err(|_| TransactionError::CorruptCatalog)?
                .to_owned();
            let kind = match catalog_u16(&record, 2)? {
                1 => crate::metadata_discovery::AdministrativeConstraintKind::NotNull,
                2 => crate::metadata_discovery::AdministrativeConstraintKind::PrimaryKey,
                3 => crate::metadata_discovery::AdministrativeConstraintKind::Unique,
                4 => crate::metadata_discovery::AdministrativeConstraintKind::Check,
                5 => crate::metadata_discovery::AdministrativeConstraintKind::ForeignKey,
                _ => return Err(TransactionError::CorruptCatalog),
            };
            let column_ids = catalog_id_list(&record, 3)?;
            let mut referenced_table_id = None;
            let mut referenced_column_ids = Vec::new();
            if kind == crate::metadata_discovery::AdministrativeConstraintKind::ForeignKey {
                let referenced_constraint_id =
                    catalog_optional_id(&record, 5)?.ok_or(TransactionError::CorruptCatalog)?;
                let referenced_constraint = self
                    .read_catalog_record(&catalog_id_key(8, referenced_constraint_id))?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if referenced_constraint.family_code != 8 {
                    return Err(TransactionError::CorruptCatalog);
                }
                let target_table_id = catalog_id(&referenced_constraint, 1)?;
                let target_table = self
                    .read_catalog_record(&catalog_id_key(5, target_table_id))?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if target_table.family_code != 5 {
                    return Err(TransactionError::CorruptCatalog);
                }
                let target_schema_id = catalog_id(&target_table, 1)?;
                if self.has_schema_metadata_authority(target_schema_id, &roles)? {
                    referenced_table_id = Some(target_table_id);
                    referenced_column_ids = catalog_id_list(&record, 6)?;
                }
            }
            encoded_bytes = encoded_bytes
                .saturating_add(18)
                .saturating_add(name.len())
                .saturating_add(kind.as_wire_name().len())
                .saturating_add(column_ids.len().saturating_mul(16))
                .saturating_add(1)
                .saturating_add(usize::from(referenced_table_id.is_some()).saturating_mul(16))
                .saturating_add(referenced_column_ids.len().saturating_mul(16));
            if descriptions.len() >= 4096 || encoded_bytes > 1024 * 1024 {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            descriptions.push(crate::metadata_discovery::AdministrativeConstraint {
                constraint_id,
                name,
                kind,
                column_ids,
                referenced_table_id,
                referenced_column_ids,
            });
        }
        descriptions.sort_unstable_by_key(|constraint| constraint.constraint_id);
        Ok(Some(descriptions))
    }

    /// Describe direct table and column grants to a current schema owner or
    /// database administrator. Principal names and inherited grants are not
    /// part of this direct-provenance view.
    pub(crate) fn list_catalog_administrative_grants(
        &mut self,
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    ) -> Result<Option<Vec<crate::metadata_discovery::AdministrativeGrant>>, TransactionError> {
        let roles = self.load_catalog_role_closure(self.principal_id)?;
        if !self.has_schema_metadata_authority(schema_id, &roles)? {
            return Ok(None);
        }
        let table_key = catalog_id_key(5, relation_id);
        let Some(table) = self.read_catalog_record(&table_key)? else {
            return Ok(None);
        };
        if table.family_code != 5 || catalog_id(&table, 1)? != schema_id {
            return Ok(None);
        }
        self.read_discovery_relation(schema_id, relation_id)?;
        let schema = self.load_catalog_table_schema(relation_id)?;
        let columns = schema.column_ids.iter().copied().collect::<HashSet<_>>();
        let records = self
            .pinned_snapshot()?
            .lookup_grants_for_object(relation_id)
            .map_err(|error| match error {
                SnapshotError::Storage(error) => TransactionError::Storage(error),
                _ => TransactionError::CorruptCatalog,
            })?;
        self.charge_scan_work(records.len())?;
        let mut descriptions = Vec::with_capacity(records.len());
        let mut encoded_bytes = 0usize;
        for record in records {
            let key = &record.logical_key;
            if record.family_code != 12
                || key.len() != 59
                || key[0..2] != 12u16.to_be_bytes()
                || key[2] != 1
                || key[19] != 1
                || key[20..36] != relation_id
                || key[36] != 1
                || key[39] != 1
                || key[42] != 1
            {
                return Err(TransactionError::CorruptCatalog);
            }
            let grantee_id: [u8; 16] = key[3..19]
                .try_into()
                .map_err(|_| TransactionError::CorruptCatalog)?;
            let privilege_code = u16::from_be_bytes([key[37], key[38]]);
            let scope_code = u16::from_be_bytes([key[40], key[41]]);
            let target_id: [u8; 16] = key[43..59]
                .try_into()
                .map_err(|_| TransactionError::CorruptCatalog)?;
            if grantee_id == [0; 16] || target_id == [0; 16] {
                return Err(TransactionError::CorruptCatalog);
            }
            let grantor_id = catalog_id(&record, 1)?;
            if grantor_id == [0; 16] || catalog_u64(&record, 2)? == 0 {
                return Err(TransactionError::CorruptCatalog);
            }
            for principal_id in [grantee_id, grantor_id] {
                let principal = self
                    .read_catalog_record(&catalog_id_key(10, principal_id))?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if principal.family_code != 10 {
                    return Err(TransactionError::CorruptCatalog);
                }
            }
            let privilege = match privilege_code {
                2 => crate::metadata_discovery::AdministrativeGrantPrivilege::Select,
                3 => crate::metadata_discovery::AdministrativeGrantPrivilege::Insert,
                4 => crate::metadata_discovery::AdministrativeGrantPrivilege::Update,
                5 => crate::metadata_discovery::AdministrativeGrantPrivilege::Delete,
                6 => crate::metadata_discovery::AdministrativeGrantPrivilege::References,
                8 => crate::metadata_discovery::AdministrativeGrantPrivilege::Alter,
                9 => crate::metadata_discovery::AdministrativeGrantPrivilege::Index,
                10 => crate::metadata_discovery::AdministrativeGrantPrivilege::ManagePolicies,
                11 => crate::metadata_discovery::AdministrativeGrantPrivilege::ManageGrants,
                _ => return Err(TransactionError::CorruptCatalog),
            };
            let target_scope = match scope_code {
                1 if target_id == relation_id => {
                    crate::metadata_discovery::AdministrativeGrantTargetScope::Object
                }
                2 if matches!(privilege_code, 2 | 3 | 4 | 6) && columns.contains(&target_id) => {
                    crate::metadata_discovery::AdministrativeGrantTargetScope::Column
                }
                _ => return Err(TransactionError::CorruptCatalog),
            };
            encoded_bytes = encoded_bytes
                .saturating_add(81)
                .saturating_add(privilege.as_wire_name().len())
                .saturating_add(target_scope.as_wire_name().len());
            if descriptions.len() >= 4096 || encoded_bytes > 1024 * 1024 {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            descriptions.push(crate::metadata_discovery::AdministrativeGrant {
                grantee_id,
                grantor_id,
                privilege,
                target_scope,
                target_id,
            });
        }
        descriptions.sort_unstable_by_key(|grant| {
            let privilege = match grant.privilege {
                crate::metadata_discovery::AdministrativeGrantPrivilege::Select => 2,
                crate::metadata_discovery::AdministrativeGrantPrivilege::Insert => 3,
                crate::metadata_discovery::AdministrativeGrantPrivilege::Update => 4,
                crate::metadata_discovery::AdministrativeGrantPrivilege::Delete => 5,
                crate::metadata_discovery::AdministrativeGrantPrivilege::References => 6,
                crate::metadata_discovery::AdministrativeGrantPrivilege::Alter => 8,
                crate::metadata_discovery::AdministrativeGrantPrivilege::Index => 9,
                crate::metadata_discovery::AdministrativeGrantPrivilege::ManagePolicies => 10,
                crate::metadata_discovery::AdministrativeGrantPrivilege::ManageGrants => 11,
            };
            let scope = match grant.target_scope {
                crate::metadata_discovery::AdministrativeGrantTargetScope::Object => 1,
                crate::metadata_discovery::AdministrativeGrantTargetScope::Column => 2,
            };
            (grant.grantee_id, privilege, scope, grant.target_id)
        });
        Ok(Some(descriptions))
    }

    /// Return secret-free policy summaries for a table under current schema
    /// owner/database-administrator authority. Only database administrators
    /// receive policy recipient IDs; expression bodies and IDs are omitted.
    pub(crate) fn list_catalog_administrative_policies(
        &mut self,
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    ) -> Result<Option<Vec<crate::metadata_discovery::AdministrativePolicy>>, TransactionError>
    {
        let roles = self.load_catalog_role_closure(self.principal_id)?;
        if !self.has_schema_metadata_authority(schema_id, &roles)? {
            return Ok(None);
        }
        let table_key = catalog_id_key(5, relation_id);
        let Some(table) = self.read_catalog_record(&table_key)? else {
            return Ok(None);
        };
        if table.family_code != 5 || catalog_id(&table, 1)? != schema_id {
            return Ok(None);
        }
        self.read_discovery_relation(schema_id, relation_id)?;
        let schema = self.load_catalog_table_schema(relation_id)?;
        let is_database_admin = roles.contains(&crate::bootstrap::database_admin_role_id());
        let policy_ids = self.scan_policy_ids_for_table(relation_id)?;
        let records = if policy_ids.is_empty() {
            self.scan_catalog_family(13)?
        } else {
            policy_ids
                .into_iter()
                .map(|policy_id| {
                    self.read_catalog_record(&catalog_id_key(13, policy_id))?
                        .ok_or(TransactionError::CorruptCatalog)
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        let boolean_type_id = self.load_catalog_builtin_type_id(1)?;
        let mut descriptions = Vec::new();
        let mut encoded_bytes = 0usize;
        for record in records {
            if record.family_code != 13 || record.logical_key.len() != 19 {
                return Err(TransactionError::CorruptCatalog);
            }
            if catalog_id(&record, 1)? != relation_id {
                continue;
            }
            let policy_id: [u8; 16] = record.logical_key[3..19]
                .try_into()
                .map_err(|_| TransactionError::CorruptCatalog)?;
            if policy_id == [0; 16] || catalog_u64(&record, 7)? == 0 {
                return Err(TransactionError::CorruptCatalog);
            }
            let kind = match catalog_u16(&record, 2)? {
                1 => crate::metadata_discovery::AdministrativePolicyKind::Row,
                2 => crate::metadata_discovery::AdministrativePolicyKind::Cell,
                _ => return Err(TransactionError::CorruptCatalog),
            };
            let operation = match catalog_u16(&record, 3)? {
                1 => crate::metadata_discovery::AdministrativePolicyOperation::Select,
                2 => crate::metadata_discovery::AdministrativePolicyOperation::Insert,
                3 => crate::metadata_discovery::AdministrativePolicyOperation::Update,
                4 => crate::metadata_discovery::AdministrativePolicyOperation::Delete,
                _ => return Err(TransactionError::CorruptCatalog),
            };
            let effect = match catalog_u16(&record, 4)? {
                1 => crate::metadata_discovery::AdministrativePolicyEffect::Allow,
                2 => crate::metadata_discovery::AdministrativePolicyEffect::Restrict,
                _ => return Err(TransactionError::CorruptCatalog),
            };
            if kind == crate::metadata_discovery::AdministrativePolicyKind::Cell
                && operation == crate::metadata_discovery::AdministrativePolicyOperation::Delete
            {
                return Err(TransactionError::CorruptCatalog);
            }
            let using_id = catalog_optional_id(&record, 5)?;
            let check_id = catalog_optional_id(&record, 6)?;
            match operation {
                crate::metadata_discovery::AdministrativePolicyOperation::Select
                | crate::metadata_discovery::AdministrativePolicyOperation::Delete
                    if using_id.is_some() && check_id.is_none() => {}
                crate::metadata_discovery::AdministrativePolicyOperation::Insert
                    if using_id.is_none() && check_id.is_some() => {}
                crate::metadata_discovery::AdministrativePolicyOperation::Update
                    if using_id.is_some() && check_id.is_some() => {}
                _ => return Err(TransactionError::CorruptCatalog),
            }
            if let Some(expression_id) = using_id {
                self.read_bound_policy_expression(
                    expression_id,
                    ExpressionPurpose::PolicyUsing,
                    &schema,
                    boolean_type_id,
                )?;
            }
            if let Some(expression_id) = check_id {
                self.read_bound_policy_expression(
                    expression_id,
                    ExpressionPurpose::PolicyCheck,
                    &schema,
                    boolean_type_id,
                )?;
            }

            let target_records = self.scan_policy_targets_for_policy(policy_id)?;
            let mut recipients = Vec::new();
            let mut column_ids = Vec::new();
            let mut has_recipient = false;
            for target_record in target_records {
                let key = &target_record.logical_key;
                if target_record.family_code != 14
                    || key.len() != 39
                    || key[0..2] != 14u16.to_be_bytes()
                    || key[2] != 1
                    || key[3..19] != policy_id
                    || key[19] != 1
                    || key[22] != 1
                {
                    return Err(TransactionError::CorruptCatalog);
                }
                let target_kind = u16::from_be_bytes([key[20], key[21]]);
                let target_id: [u8; 16] = key[23..39]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                if target_id == [0; 16] {
                    return Err(TransactionError::CorruptCatalog);
                }
                match target_kind {
                    1 | 2 => {
                        has_recipient = true;
                        let principal = self
                            .read_catalog_record(&catalog_id_key(10, target_id))?
                            .ok_or(TransactionError::CorruptCatalog)?;
                        let expected_kind = if target_kind == 1 { 1 } else { 2 };
                        let login_enabled = catalog_field(&principal, 3)?;
                        if principal.family_code != 10
                            || catalog_u16(&principal, 1)? != expected_kind
                            || catalog_u64(&principal, 4)? == 0
                            || !matches!(login_enabled, [0] | [1])
                            || (expected_kind == 2 && login_enabled != [0])
                        {
                            return Err(TransactionError::CorruptCatalog);
                        }
                        if is_database_admin {
                            recipients.push(
                                crate::metadata_discovery::AdministrativePolicyRecipient {
                                    kind: if target_kind == 1 {
                                        crate::metadata_discovery::AdministrativePolicyRecipientKind::User
                                    } else {
                                        crate::metadata_discovery::AdministrativePolicyRecipientKind::Role
                                    },
                                    principal_id: target_id,
                                },
                            );
                        }
                    }
                    3 => {
                        if kind != crate::metadata_discovery::AdministrativePolicyKind::Cell
                            || operation
                                == crate::metadata_discovery::AdministrativePolicyOperation::Delete
                            || !schema.column_ids.contains(&target_id)
                        {
                            return Err(TransactionError::CorruptCatalog);
                        }
                        let column = self
                            .read_catalog_record(&catalog_id_key(6, target_id))?
                            .ok_or(TransactionError::CorruptCatalog)?;
                        if catalog_id(&column, 1)? != relation_id {
                            return Err(TransactionError::CorruptCatalog);
                        }
                        column_ids.push(target_id);
                    }
                    _ => return Err(TransactionError::CorruptCatalog),
                }
            }
            if !has_recipient
                || recipients.is_empty() && is_database_admin
                || column_ids.is_empty()
                    && kind == crate::metadata_discovery::AdministrativePolicyKind::Cell
                || !column_ids.is_empty()
                    && kind == crate::metadata_discovery::AdministrativePolicyKind::Row
            {
                return Err(TransactionError::CorruptCatalog);
            }
            recipients
                .sort_unstable_by_key(|recipient| (recipient.kind as u8, recipient.principal_id));
            column_ids.sort_unstable();
            encoded_bytes = encoded_bytes
                .saturating_add(33)
                .saturating_add(column_ids.len().saturating_mul(16))
                .saturating_add(if is_database_admin {
                    recipients.len().saturating_mul(21)
                } else {
                    1
                });
            if descriptions.len() >= 4096 || encoded_bytes > 1024 * 1024 {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            descriptions.push(crate::metadata_discovery::AdministrativePolicy {
                policy_id,
                kind,
                operation,
                effect,
                column_ids,
                recipients: is_database_admin.then_some(recipients),
            });
        }
        descriptions.sort_unstable_by_key(|policy| policy.policy_id);
        Ok(Some(descriptions))
    }

    fn administrative_dependency_endpoint_visible(
        &mut self,
        endpoint_id: [u8; 16],
        roles: &HashSet<[u8; 16]>,
        is_database_admin: bool,
    ) -> Result<bool, TransactionError> {
        if endpoint_id == [0; 16] {
            return Ok(false);
        }
        let Some(mut object) = self.read_catalog_record(&catalog_id_key(2, endpoint_id))? else {
            if !is_database_admin {
                return Ok(false);
            }
            return Ok(self
                .read_catalog_record(&catalog_id_key(1, endpoint_id))?
                .is_some_and(|record| record.family_code == 1));
        };
        if object.family_code != 2 {
            return Err(TransactionError::CorruptCatalog);
        }
        let object_kind = catalog_u16(&object, 1)?;
        if matches!(object_kind, 8 | 9) {
            return Ok(is_database_admin);
        }
        let owning_table_id = match object_kind {
            4 | 6 | 7 | 10 => {
                let family = match object_kind {
                    4 => 6,
                    6 => 8,
                    7 => 9,
                    10 => 13,
                    _ => unreachable!(),
                };
                let typed = self
                    .read_catalog_record(&catalog_id_key(family, endpoint_id))?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if typed.family_code != family {
                    return Err(TransactionError::CorruptCatalog);
                }
                Some(catalog_id(&typed, 1)?)
            }
            _ => None,
        };
        if let Some(table_id) = owning_table_id {
            let table = self
                .read_catalog_record(&catalog_id_key(5, table_id))?
                .ok_or(TransactionError::CorruptCatalog)?;
            if table.family_code != 5 {
                return Err(TransactionError::CorruptCatalog);
            }
            return self.has_schema_metadata_authority(catalog_id(&table, 1)?, roles);
        }
        let mut current_id = endpoint_id;
        let mut seen = HashSet::new();
        for _ in 0..32 {
            if !seen.insert(current_id) {
                return Err(TransactionError::CorruptCatalog);
            }
            let kind = catalog_u16(&object, 1)?;
            if kind == 1 {
                return self.has_schema_metadata_authority(current_id, roles);
            }
            if kind == 3 {
                let table = self
                    .read_catalog_record(&catalog_id_key(5, current_id))?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if table.family_code != 5 {
                    return Err(TransactionError::CorruptCatalog);
                }
                return self.has_schema_metadata_authority(catalog_id(&table, 1)?, roles);
            }
            let Some(parent_id) = catalog_optional_id(&object, 2)? else {
                return Ok(false);
            };
            let Some(parent) = self.read_catalog_record(&catalog_id_key(2, parent_id))? else {
                return Err(TransactionError::CorruptCatalog);
            };
            if parent.family_code != 2 {
                return Err(TransactionError::CorruptCatalog);
            }
            current_id = parent_id;
            object = parent;
        }
        Err(TransactionError::CorruptCatalog)
    }

    /// Return bounded direct dependency edges touching a visible object. Edges
    /// with either endpoint outside the caller's administrative visibility are
    /// omitted, without returning hidden counts or names.
    pub(crate) fn list_catalog_administrative_dependencies(
        &mut self,
        object_id: [u8; 16],
    ) -> Result<Option<Vec<crate::metadata_discovery::AdministrativeDependency>>, TransactionError>
    {
        let roles = self.load_catalog_role_closure(self.principal_id)?;
        let is_database_admin = roles.contains(&crate::bootstrap::database_admin_role_id());
        if !self.administrative_dependency_endpoint_visible(object_id, &roles, is_database_admin)? {
            return Ok(None);
        }
        const EDGE_LIMIT: usize = 4097;
        let snapshot = self.pinned_snapshot()?;
        let outgoing = snapshot
            .lookup_dependencies_for_dependent(object_id, EDGE_LIMIT)
            .map_err(|error| match error {
                SnapshotError::Storage(error) => TransactionError::Storage(error),
                SnapshotError::RecordLimitExceeded => TransactionError::InvalidMetadataRequest,
                _ => TransactionError::CorruptCatalog,
            })?;
        let incoming = snapshot
            .lookup_dependents_for_referenced(object_id, EDGE_LIMIT)
            .map_err(|error| match error {
                SnapshotError::Storage(error) => TransactionError::Storage(error),
                SnapshotError::RecordLimitExceeded => TransactionError::InvalidMetadataRequest,
                _ => TransactionError::CorruptCatalog,
            })?;
        self.charge_scan_work(outgoing.len().saturating_add(incoming.len()))?;
        let mut edges = HashSet::new();
        for record in outgoing.into_iter().chain(incoming) {
            let key = &record.logical_key;
            if record.family_code != 15
                || key.len() != 39
                || key[0..2] != 15u16.to_be_bytes()
                || key[2] != 1
                || key[19] != 1
                || key[36] != 1
            {
                return Err(TransactionError::CorruptCatalog);
            }
            let dependent_id: [u8; 16] = key[3..19]
                .try_into()
                .map_err(|_| TransactionError::CorruptCatalog)?;
            let referenced_id: [u8; 16] = key[20..36]
                .try_into()
                .map_err(|_| TransactionError::CorruptCatalog)?;
            let kind = u16::from_be_bytes([key[37], key[38]]);
            if dependent_id == [0; 16] || referenced_id == [0; 16] || !matches!(kind, 1..=5) {
                return Err(TransactionError::CorruptCatalog);
            }
            if self.administrative_dependency_endpoint_visible(
                dependent_id,
                &roles,
                is_database_admin,
            )? && self.administrative_dependency_endpoint_visible(
                referenced_id,
                &roles,
                is_database_admin,
            )? {
                edges.insert((dependent_id, referenced_id, kind));
            }
        }
        if edges.len() > 4096 {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        let mut descriptions = edges
            .into_iter()
            .map(|(dependent_id, referenced_id, kind)| {
                let kind = match kind {
                    1 => crate::metadata_discovery::AdministrativeDependencyKind::OwnedChild,
                    2 => {
                        crate::metadata_discovery::AdministrativeDependencyKind::ExpressionReference
                    }
                    3 => crate::metadata_discovery::AdministrativeDependencyKind::ForeignKeyTarget,
                    4 => crate::metadata_discovery::AdministrativeDependencyKind::BackingIndex,
                    5 => crate::metadata_discovery::AdministrativeDependencyKind::Authority,
                    _ => unreachable!("dependency kind checked above"),
                };
                crate::metadata_discovery::AdministrativeDependency {
                    dependent_id,
                    referenced_id,
                    kind,
                }
            })
            .collect::<Vec<_>>();
        descriptions
            .sort_unstable_by_key(|edge| (edge.dependent_id, edge.referenced_id, edge.kind as u8));
        let encoded_bytes = descriptions.iter().fold(0usize, |total, edge| {
            total.saturating_add(35 + edge.kind.as_wire_name().len())
        });
        if encoded_bytes > 1024 * 1024 {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        Ok(Some(descriptions))
    }

    fn has_schema_metadata_authority(
        &mut self,
        schema_id: [u8; 16],
        roles: &HashSet<[u8; 16]>,
    ) -> Result<bool, TransactionError> {
        let Some(object) = self.read_catalog_record(&catalog_id_key(2, schema_id))? else {
            return Ok(false);
        };
        if object.family_code != 2 || catalog_u16(&object, 1)? != 1 {
            return Err(TransactionError::CorruptCatalog);
        }
        if roles.contains(&crate::bootstrap::database_admin_role_id()) {
            self.read_discovery_schema(schema_id)?;
            return Ok(true);
        }
        self.read_discovery_schema(schema_id)?;
        let owner_id = catalog_id(&object, 4)?;
        Ok(owner_id == self.principal_id || roles.contains(&owner_id))
    }

    fn has_catalog_table_owner_authority(
        &mut self,
        table_id: [u8; 16],
        roles: &HashSet<[u8; 16]>,
    ) -> Result<bool, TransactionError> {
        let Some(object) = self.read_catalog_record(&catalog_id_key(2, table_id))? else {
            return Ok(false);
        };
        if object.family_code != 2 || catalog_u16(&object, 1)? != 3 {
            return Ok(false);
        }
        let owner_id = catalog_id(&object, 4)?;
        Ok(owner_id == self.principal_id || roles.contains(&owner_id))
    }

    fn current_principal_has_grant_ddl_authority(
        &mut self,
        object_id: [u8; 16],
    ) -> Result<bool, TransactionError> {
        self.current_principal_has_object_ddl_authority(object_id, 11)
    }

    fn current_principal_has_index_ddl_authority(
        &mut self,
        object_id: [u8; 16],
    ) -> Result<bool, TransactionError> {
        self.current_principal_has_object_ddl_authority(object_id, 9)
    }

    fn current_principal_has_alter_ddl_authority(
        &mut self,
        object_id: [u8; 16],
    ) -> Result<bool, TransactionError> {
        self.current_principal_has_object_ddl_authority(object_id, 8)
    }

    fn current_principal_has_object_ddl_authority(
        &mut self,
        object_id: [u8; 16],
        privilege_code: u16,
    ) -> Result<bool, TransactionError> {
        let principal = self
            .read_catalog_record(&catalog_id_key(10, self.principal_id))?
            .ok_or(TransactionError::CorruptCatalog)?;
        if principal.family_code != 10 || catalog_u16(&principal, 1)? != 1 {
            return Err(TransactionError::CorruptCatalog);
        }
        let name = std::str::from_utf8(catalog_field(&principal, 2)?)
            .map_err(|_| TransactionError::CorruptCatalog)?;
        let Some(identity_id) = self.resolve_catalog_object_ddl_identity_name(
            object_id,
            name,
            crate::metadata_discovery::AdministrativeIdentityKind::User,
            privilege_code,
        )?
        else {
            return Ok(false);
        };
        if identity_id != self.principal_id {
            return Err(TransactionError::CorruptCatalog);
        }
        Ok(true)
    }

    fn has_catalog_policy_ddl_authority(
        &mut self,
        schema_id: [u8; 16],
        table_id: [u8; 16],
        roles: &HashSet<[u8; 16]>,
    ) -> Result<bool, TransactionError> {
        let Some(schema_object) = self.read_catalog_record(&catalog_id_key(2, schema_id))? else {
            return Ok(false);
        };
        let Some(table_record) = self.read_catalog_record(&catalog_id_key(5, table_id))? else {
            return Ok(false);
        };
        let Some(table_object) = self.read_catalog_record(&catalog_id_key(2, table_id))? else {
            return Err(TransactionError::CorruptCatalog);
        };
        if schema_object.family_code != 2
            || catalog_u16(&schema_object, 1)? != 1
            || table_record.family_code != 5
            || catalog_id(&table_record, 1)? != schema_id
            || table_object.family_code != 2
            || catalog_u16(&table_object, 1)? != 3
        {
            return Ok(false);
        }
        let mut subjects = roles.clone();
        subjects.insert(self.principal_id);
        if roles.contains(&crate::bootstrap::database_admin_role_id()) {
            return Ok(true);
        }
        if subjects.contains(&catalog_id(&schema_object, 4)?)
            || subjects.contains(&catalog_id(&table_object, 4)?)
        {
            return Ok(true);
        }
        for object_id in [schema_id, table_id] {
            let grants = self
                .pinned_snapshot()?
                .lookup_grants_for_object(object_id)
                .map_err(|error| match error {
                    SnapshotError::Storage(error) => TransactionError::Storage(error),
                    _ => TransactionError::CorruptCatalog,
                })?;
            self.charge_scan_work(grants.len())?;
            for grant in grants {
                let key = &grant.logical_key;
                if grant.family_code != 12
                    || key.len() != 59
                    || key[0..2] != 12u16.to_be_bytes()
                    || key[2] != 1
                    || key[19] != 1
                    || key[20..36] != object_id
                    || key[36] != 1
                    || key[37..39] != 10u16.to_be_bytes()
                    || key[39] != 1
                    || key[40..42] != 1u16.to_be_bytes()
                    || key[42] != 1
                    || key[43..59] != object_id
                    || catalog_u64(&grant, 2)? == 0
                {
                    continue;
                }
                let grantee: [u8; 16] = key[3..19]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                if grantee == [0; 16] {
                    return Err(TransactionError::CorruptCatalog);
                }
                if subjects.contains(&grantee) {
                    if catalog_id(&grant, 1)? == [0; 16] {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Return the authorized rows exposed by `information_schema.schemata`.
    pub fn scan_information_schema_schemata(
        &mut self,
        page_limit: usize,
        continuation: Option<SchemaScanCursor>,
    ) -> Result<InformationSchemaSchemataPage, TransactionError> {
        let page = self.list_catalog_authorized_schemas(page_limit, continuation)?;
        Ok(InformationSchemaSchemataPage {
            rows: page
                .schemas
                .into_iter()
                .map(|schema| InformationSchemaSchemataRow {
                    schema_name: schema.name,
                })
                .collect(),
            continuation: page.continuation,
        })
    }

    /// Return the authorized `information_schema.tables` rows for one schema.
    pub fn scan_information_schema_tables_in_schema(
        &mut self,
        schema_name: &str,
        page_limit: usize,
        continuation: Option<RelationScanCursor>,
    ) -> Result<Option<InformationSchemaTablesPage>, TransactionError> {
        let result = (|| {
            if schema_name.is_empty()
                || schema_name.len() > MAX_METADATA_PAGE_BYTES
                || page_limit == 0
                || page_limit > MAX_RELATIONS_PER_PAGE
            {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let principal_id = self.principal_id;
            let (role_ids, schema_grants) = self.load_catalog_schema_usage_grants(principal_id)?;
            let scope = self.scope_from_grants(&schema_grants);
            let schema_context = AuthorizationContext {
                principal_id,
                role_ids,
                scope,
            };
            let Some(schema) =
                self.resolve_authorized_schema(schema_name, &schema_context, &schema_grants, &[])?
            else {
                return Ok(None);
            };
            self.scan_information_schema_tables_for_visible_schema(
                &schema,
                page_limit,
                continuation,
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

    fn scan_information_schema_tables_for_visible_schema(
        &mut self,
        schema: &VisibleSchema,
        page_limit: usize,
        continuation: Option<RelationScanCursor>,
    ) -> Result<Option<InformationSchemaTablesPage>, TransactionError> {
        if schema.schema_id == INFORMATION_SCHEMA_ID {
            let extra_bytes_per_relation = schema
                .name
                .len()
                .checked_add("VIEW".len())
                .ok_or(TransactionError::MetadataResultTooLarge)?;
            let page = self.list_builtin_information_schema_relations(
                page_limit,
                continuation,
                BuiltinRelationScanPurpose::Tables,
                extra_bytes_per_relation,
            )?;
            return Ok(Some(InformationSchemaTablesPage {
                rows: page
                    .relations
                    .into_iter()
                    .map(|relation| InformationSchemaTableRow {
                        table_schema: schema.name.clone(),
                        table_name: relation.name,
                        table_type: "VIEW".to_owned(),
                    })
                    .collect(),
                continuation: page.continuation,
            }));
        }
        let extra_bytes_per_relation = schema
            .name
            .len()
            .checked_add("BASE TABLE".len())
            .ok_or(TransactionError::MetadataResultTooLarge)?;
        let Some(page) = self.list_catalog_authorized_relations_with_extra_bytes(
            schema.schema_id,
            page_limit,
            continuation,
            extra_bytes_per_relation,
        )?
        else {
            return Ok(None);
        };
        Ok(Some(InformationSchemaTablesPage {
            rows: page
                .relations
                .into_iter()
                .map(|relation| InformationSchemaTableRow {
                    table_schema: schema.name.clone(),
                    table_name: relation.name,
                    table_type: "BASE TABLE".to_owned(),
                })
                .collect(),
            continuation: page.continuation,
        }))
    }

    fn list_builtin_information_schema_relations(
        &mut self,
        page_limit: usize,
        continuation: Option<RelationScanCursor>,
        purpose: BuiltinRelationScanPurpose,
        extra_bytes_per_relation: usize,
    ) -> Result<VisibleRelationPage, TransactionError> {
        let result = (|| {
            if page_limit == 0 || page_limit > MAX_RELATIONS_PER_PAGE {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            self.check_deadline()?;
            let principal_id = self.principal_id;
            let mut active_scan = match continuation.as_ref() {
                Some(cursor) => {
                    let Some(active) = self.active_builtin_relation_scan.as_ref() else {
                        return Err(TransactionError::InvalidMetadataRequest);
                    };
                    if active.principal_id != principal_id
                        || active.purpose != purpose
                        || active.extra_bytes_per_relation != extra_bytes_per_relation
                        || active
                            .continuation
                            .is_none_or(|(token, _)| token != cursor.token)
                    {
                        return Err(TransactionError::InvalidMetadataRequest);
                    }
                    active.clone()
                }
                None => ActiveBuiltinRelationScan {
                    purpose,
                    extra_bytes_per_relation,
                    principal_id,
                    continuation: None,
                    used_tokens: HashSet::new(),
                },
            };
            let start_index = if let Some(cursor) = continuation.as_ref() {
                let (token, after_index) = active_scan
                    .continuation
                    .take()
                    .ok_or(TransactionError::InvalidMetadataRequest)?;
                if token != cursor.token {
                    return Err(TransactionError::InvalidMetadataRequest);
                }
                after_index
            } else {
                0
            };
            let available = builtin_information_schema_relations();
            let mut relations = Vec::with_capacity(page_limit);
            let mut page_bytes = 0usize;
            let mut next_index = None;
            for (index, relation) in available.into_iter().enumerate().skip(start_index) {
                if relations.len() == page_limit {
                    next_index = Some(index);
                    break;
                }
                let encoded_size = relation
                    .name
                    .len()
                    .checked_add(24)
                    .and_then(|size| size.checked_add(extra_bytes_per_relation))
                    .ok_or(TransactionError::MetadataResultTooLarge)?;
                if page_bytes.saturating_add(encoded_size) > MAX_METADATA_PAGE_BYTES {
                    if relations.is_empty() {
                        return Err(TransactionError::MetadataResultTooLarge);
                    }
                    next_index = Some(index);
                    break;
                }
                page_bytes += encoded_size;
                relations.push(relation);
            }
            let continuation = if let Some(after_index) = next_index {
                let token = new_metadata_cursor_token(
                    &mut active_scan.used_tokens,
                    RELATION_CURSOR_TOKEN_KIND,
                )?;
                active_scan.continuation = Some((token, after_index));
                Some(RelationScanCursor { token })
            } else {
                active_scan.continuation = None;
                None
            };
            self.active_builtin_relation_scan = continuation.as_ref().map(|_| active_scan);
            Ok(VisibleRelationPage {
                relations,
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

    fn describe_builtin_information_schema_columns_page(
        &mut self,
        relation_id: [u8; 16],
        page_limit: usize,
        continuation: Option<ColumnScanCursor>,
        extra_bytes_per_column: usize,
    ) -> Result<Option<VisibleColumnPage>, TransactionError> {
        let result = (|| {
            if page_limit == 0 || page_limit > MAX_COLUMNS_PER_PAGE {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let Some(schema) = builtin_information_schema_relation_schema(relation_id) else {
                return Ok(None);
            };
            self.check_deadline()?;
            let principal_id = self.principal_id;
            let mut active_scan = match continuation.as_ref() {
                Some(cursor) => {
                    let Some(active) = self.active_builtin_column_scan.as_ref() else {
                        return Err(TransactionError::InvalidMetadataRequest);
                    };
                    if active.relation_id != relation_id
                        || active.extra_bytes_per_column != extra_bytes_per_column
                        || active.principal_id != principal_id
                        || active
                            .continuation
                            .is_none_or(|(token, _)| token != cursor.token)
                    {
                        return Err(TransactionError::InvalidMetadataRequest);
                    }
                    active.clone()
                }
                None => ActiveBuiltinColumnScan {
                    relation_id,
                    extra_bytes_per_column,
                    principal_id,
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
            if start_ordinal > schema.columns.len() {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let mut columns = Vec::with_capacity(page_limit);
            let mut page_bytes = 0usize;
            let mut has_more = false;
            for ordinal in start_ordinal..schema.columns.len() {
                if columns.len() == page_limit {
                    has_more = true;
                    break;
                }
                let column = schema.columns[ordinal].clone();
                let encoded_size = column
                    .name
                    .len()
                    .checked_add(32)
                    .and_then(|size| size.checked_add(extra_bytes_per_column))
                    .ok_or(TransactionError::MetadataResultTooLarge)?;
                if page_bytes.saturating_add(encoded_size) > MAX_METADATA_PAGE_BYTES {
                    if columns.is_empty() {
                        return Err(TransactionError::MetadataResultTooLarge);
                    }
                    has_more = true;
                    break;
                }
                page_bytes += encoded_size;
                columns.push(column);
            }
            let continuation = if has_more {
                let after_ordinal = columns
                    .last()
                    .map(|column| column.ordinal)
                    .ok_or(TransactionError::InvalidMetadataRequest)?;
                let token = new_metadata_cursor_token(
                    &mut active_scan.used_tokens,
                    COLUMN_CURSOR_TOKEN_KIND,
                )?;
                active_scan.continuation = Some((token, after_ordinal));
                Some(ColumnScanCursor { token })
            } else {
                active_scan.continuation = None;
                None
            };
            self.active_builtin_column_scan = continuation.as_ref().map(|_| active_scan);
            Ok(Some(VisibleColumnPage {
                relation_id,
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

    /// Return all schema-authorized table rows in bounded metadata pages.
    pub fn scan_information_schema_tables(
        &mut self,
        page_limit: usize,
        continuation: Option<InformationSchemaTablesCursor>,
    ) -> Result<InformationSchemaTablesScanPage, TransactionError> {
        let result = (|| {
            if page_limit == 0 || page_limit > MAX_RELATIONS_PER_PAGE {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let principal_id = self.principal_id;
            let mut active_scan = match continuation.as_ref() {
                Some(cursor) => {
                    let Some(active) = self.active_information_schema_tables_scan.as_ref() else {
                        return Err(TransactionError::InvalidMetadataRequest);
                    };
                    if active.principal_id != principal_id
                        || active
                            .continuation
                            .is_none_or(|token| token != cursor.token)
                    {
                        return Err(TransactionError::InvalidMetadataRequest);
                    }
                    active.clone()
                }
                None => ActiveInformationSchemaTablesScan {
                    principal_id,
                    current_schema: None,
                    schema_continuation: None,
                    relation_continuation: None,
                    continuation: None,
                    used_tokens: HashSet::new(),
                },
            };
            if let Some(cursor) = continuation.as_ref() {
                let token = active_scan
                    .continuation
                    .take()
                    .ok_or(TransactionError::InvalidMetadataRequest)?;
                if token != cursor.token {
                    return Err(TransactionError::InvalidMetadataRequest);
                }
            }
            if active_scan.current_schema.is_none() || active_scan.relation_continuation.is_none() {
                let schema_page = self
                    .list_catalog_authorized_schemas(1, active_scan.schema_continuation.take())?;
                active_scan.current_schema = schema_page.schemas.into_iter().next();
                active_scan.schema_continuation = schema_page.continuation;
                active_scan.relation_continuation = None;
            }
            let Some(schema) = active_scan.current_schema.clone() else {
                self.active_information_schema_tables_scan = None;
                return Ok(InformationSchemaTablesScanPage {
                    rows: Vec::new(),
                    continuation: None,
                });
            };
            let page = self
                .scan_information_schema_tables_for_visible_schema(
                    &schema,
                    page_limit,
                    active_scan.relation_continuation.take(),
                )?
                .ok_or(TransactionError::InvalidContext)?;
            active_scan.relation_continuation = page.continuation;
            let continuation = if active_scan.relation_continuation.is_some()
                || active_scan.schema_continuation.is_some()
            {
                let token = new_metadata_cursor_token(
                    &mut active_scan.used_tokens,
                    INFORMATION_SCHEMA_TABLES_CURSOR_TOKEN_KIND,
                )?;
                active_scan.continuation = Some(token);
                Some(InformationSchemaTablesCursor { token })
            } else {
                active_scan.continuation = None;
                None
            };
            if continuation.is_some() {
                self.active_information_schema_tables_scan = Some(active_scan);
            } else {
                self.active_information_schema_tables_scan = None;
            }
            Ok(InformationSchemaTablesScanPage {
                rows: page.rows,
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

    /// Return the authorized `information_schema.columns` rows for one relation.
    pub fn scan_information_schema_columns_for_relation(
        &mut self,
        schema_name: &str,
        relation_name: &str,
        page_limit: usize,
        continuation: Option<ColumnScanCursor>,
    ) -> Result<Option<InformationSchemaColumnsPage>, TransactionError> {
        let result = (|| {
            if schema_name.is_empty()
                || schema_name.len() > MAX_METADATA_PAGE_BYTES
                || relation_name.is_empty()
                || relation_name.len() > MAX_METADATA_PAGE_BYTES
                || page_limit == 0
                || page_limit > MAX_COLUMNS_PER_PAGE
            {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let principal_id = self.principal_id;
            let (schema_roles, schema_grants) =
                self.load_catalog_schema_usage_grants(principal_id)?;
            let schema_scope = self.scope_from_grants(&schema_grants);
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

            if schema.schema_id == INFORMATION_SCHEMA_ID {
                let Some(relation) = builtin_information_schema_relation(relation_name) else {
                    return Ok(None);
                };
                return self.scan_information_schema_columns_for_visible_relation(
                    &schema,
                    &relation,
                    page_limit,
                    continuation,
                );
            }

            let mut relation_continuation = None;
            let relation = loop {
                let Some(page) = self.list_catalog_authorized_relations(
                    schema.schema_id,
                    MAX_RELATIONS_PER_PAGE,
                    relation_continuation,
                )?
                else {
                    return Ok(None);
                };
                if let Some(relation) = page
                    .relations
                    .into_iter()
                    .find(|relation| relation.name == relation_name)
                {
                    break relation;
                }
                relation_continuation = page.continuation;
                if relation_continuation.is_none() {
                    return Ok(None);
                }
            };

            self.scan_information_schema_columns_for_visible_relation(
                &schema,
                &relation,
                page_limit,
                continuation,
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

    fn scan_information_schema_columns_for_visible_relation(
        &mut self,
        schema: &VisibleSchema,
        relation: &VisibleRelation,
        page_limit: usize,
        continuation: Option<ColumnScanCursor>,
    ) -> Result<Option<InformationSchemaColumnsPage>, TransactionError> {
        let extra_bytes_per_column = schema
            .name
            .len()
            .checked_add(relation.name.len())
            .and_then(|size| size.checked_add(INFORMATION_SCHEMA_COLUMN_EXTRA_BYTES))
            .ok_or(TransactionError::MetadataResultTooLarge)?;
        let Some(page) = (if schema.schema_id == INFORMATION_SCHEMA_ID {
            self.describe_builtin_information_schema_columns_page(
                relation.relation_id,
                page_limit,
                continuation,
                extra_bytes_per_column,
            )?
        } else {
            let principal_id = self.principal_id;
            let (relation_roles, relation_grants) =
                self.load_catalog_relation_discovery_grants(schema.schema_id)?;
            let relation_scope = self.scope_from_grants(&relation_grants);
            let relation_context = AuthorizationContext {
                principal_id,
                role_ids: relation_roles,
                scope: relation_scope,
            };
            self.describe_authorized_columns_page_with_extra_bytes(
                relation.relation_id,
                page_limit,
                continuation,
                extra_bytes_per_column,
                MetadataDiscoveryAccess {
                    context: &relation_context,
                    grants: &relation_grants,
                    dependencies: &[],
                },
            )?
        }) else {
            return Ok(None);
        };
        let rows = page
            .columns
            .into_iter()
            .map(|column| {
                let ordinal_position = column
                    .ordinal
                    .checked_add(1)
                    .ok_or(TransactionError::CorruptCatalog)?;
                let (data_type, character_maximum_length, numeric_precision, numeric_scale) =
                    information_schema_column_type(column.scalar_type);
                Ok(InformationSchemaColumnRow {
                    table_schema: schema.name.clone(),
                    table_name: relation.name.clone(),
                    column_name: column.name,
                    ordinal_position,
                    data_type: data_type.to_owned(),
                    is_nullable: if column.nullable { "YES" } else { "NO" }.to_owned(),
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale,
                })
            })
            .collect::<Result<Vec<_>, TransactionError>>()?;
        Ok(Some(InformationSchemaColumnsPage {
            rows,
            continuation: page.continuation,
        }))
    }

    /// Return declared columns for every relation admitted to the invocation.
    pub fn scan_information_schema_columns(
        &mut self,
        page_limit: usize,
        continuation: Option<InformationSchemaColumnsCursor>,
    ) -> Result<InformationSchemaColumnsScanPage, TransactionError> {
        let result = (|| {
            if page_limit == 0 || page_limit > MAX_COLUMNS_PER_PAGE {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let principal_id = self.principal_id;
            let mut active_scan = match continuation.as_ref() {
                Some(cursor) => {
                    let Some(active) = self.active_information_schema_columns_scan.as_ref() else {
                        return Err(TransactionError::InvalidMetadataRequest);
                    };
                    if active.principal_id != principal_id
                        || active
                            .continuation
                            .is_none_or(|token| token != cursor.token)
                    {
                        return Err(TransactionError::InvalidMetadataRequest);
                    }
                    active.clone()
                }
                None => ActiveInformationSchemaColumnsScan {
                    principal_id,
                    schema_scan_started: false,
                    current_schema: None,
                    schema_continuation: None,
                    relation_scan_started: false,
                    relation_scan_exhausted: false,
                    current_relation: None,
                    relation_continuation: None,
                    current_relation_columns_started: false,
                    column_continuation: None,
                    continuation: None,
                    used_tokens: HashSet::new(),
                },
            };
            if let Some(cursor) = continuation.as_ref() {
                let token = active_scan
                    .continuation
                    .take()
                    .ok_or(TransactionError::InvalidMetadataRequest)?;
                if token != cursor.token {
                    return Err(TransactionError::InvalidMetadataRequest);
                }
            }
            if active_scan.current_relation_columns_started
                && active_scan.column_continuation.is_none()
            {
                active_scan.current_relation = None;
                active_scan.current_relation_columns_started = false;
            }
            let mut empty_schema_pages = 0usize;
            while active_scan.current_relation.is_none() {
                if active_scan.current_schema.is_none() {
                    if active_scan.schema_scan_started && active_scan.schema_continuation.is_none()
                    {
                        break;
                    }
                    let schema_page = self.list_catalog_authorized_schemas(
                        1,
                        active_scan.schema_continuation.take(),
                    )?;
                    active_scan.schema_scan_started = true;
                    active_scan.current_schema = schema_page.schemas.into_iter().next();
                    active_scan.schema_continuation = schema_page.continuation;
                    active_scan.relation_scan_started = false;
                    active_scan.relation_scan_exhausted = false;
                    active_scan.relation_continuation = None;
                    if active_scan.current_schema.is_none() {
                        break;
                    }
                }
                if active_scan.relation_scan_started && active_scan.relation_scan_exhausted {
                    active_scan.current_schema = None;
                    active_scan.relation_scan_started = false;
                    active_scan.relation_scan_exhausted = false;
                    active_scan.relation_continuation = None;
                    empty_schema_pages += 1;
                    if empty_schema_pages >= MAX_SCHEMAS_PER_PAGE {
                        break;
                    }
                    continue;
                }
                let schema = active_scan
                    .current_schema
                    .as_ref()
                    .ok_or(TransactionError::InvalidContext)?
                    .clone();
                let relation_page = if schema.schema_id == INFORMATION_SCHEMA_ID {
                    self.list_builtin_information_schema_relations(
                        1,
                        active_scan.relation_continuation.take(),
                        BuiltinRelationScanPurpose::Columns,
                        0,
                    )?
                } else {
                    self.list_catalog_authorized_relations(
                        schema.schema_id,
                        1,
                        active_scan.relation_continuation.take(),
                    )?
                    .ok_or(TransactionError::InvalidContext)?
                };
                active_scan.relation_continuation = relation_page.continuation;
                active_scan.relation_scan_started = true;
                active_scan.relation_scan_exhausted = active_scan.relation_continuation.is_none();
                if let Some(relation) = relation_page.relations.into_iter().next() {
                    active_scan.current_relation = Some(relation);
                    break;
                }
                if active_scan.relation_continuation.is_some() {
                    continue;
                }
                active_scan.current_schema = None;
                active_scan.relation_scan_started = false;
                active_scan.relation_scan_exhausted = false;
                empty_schema_pages += 1;
                if empty_schema_pages >= MAX_SCHEMAS_PER_PAGE {
                    break;
                }
            }
            let projected_page = match (
                active_scan.current_schema.as_ref(),
                active_scan.current_relation.as_ref(),
            ) {
                (Some(schema), Some(relation)) => {
                    let schema = schema.clone();
                    let relation = relation.clone();
                    let column_continuation = active_scan.column_continuation.take();
                    Some(
                        self.scan_information_schema_columns_for_visible_relation(
                            &schema,
                            &relation,
                            page_limit,
                            column_continuation,
                        )?
                        .ok_or(TransactionError::InvalidContext)?,
                    )
                }
                _ => None,
            };
            if let Some(page) = projected_page {
                active_scan.column_continuation = page.continuation;
                active_scan.current_relation_columns_started = true;
                let more = active_scan.column_continuation.is_some()
                    || active_scan.relation_continuation.is_some()
                    || active_scan.schema_continuation.is_some();
                let continuation = if more {
                    let token = new_metadata_cursor_token(
                        &mut active_scan.used_tokens,
                        INFORMATION_SCHEMA_COLUMNS_CURSOR_TOKEN_KIND,
                    )?;
                    active_scan.continuation = Some(token);
                    Some(InformationSchemaColumnsCursor { token })
                } else {
                    active_scan.continuation = None;
                    None
                };
                self.active_information_schema_columns_scan =
                    continuation.as_ref().map(|_| active_scan);
                Ok(InformationSchemaColumnsScanPage {
                    rows: page.rows,
                    continuation,
                })
            } else {
                let continuation = if active_scan.relation_continuation.is_some()
                    || active_scan.schema_continuation.is_some()
                {
                    let token = new_metadata_cursor_token(
                        &mut active_scan.used_tokens,
                        INFORMATION_SCHEMA_COLUMNS_CURSOR_TOKEN_KIND,
                    )?;
                    active_scan.continuation = Some(token);
                    Some(InformationSchemaColumnsCursor { token })
                } else {
                    None
                };
                self.active_information_schema_columns_scan =
                    continuation.as_ref().map(|_| active_scan);
                Ok(InformationSchemaColumnsScanPage {
                    rows: Vec::new(),
                    continuation,
                })
            }
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

    /// List schemas for which the invocation has schema USAGE, plus the public
    /// reserved information_schema namespace.
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
            // The reserved information_schema namespace is public even though
            // bootstrap does not represent its virtual views as stored tables.
            visible_schema_ids.push(INFORMATION_SCHEMA_ID);
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
                let schema = if schema_id == INFORMATION_SCHEMA_ID {
                    VisibleSchema {
                        schema_id,
                        name: "information_schema".to_owned(),
                    }
                } else {
                    self.read_discovery_schema(schema_id)?
                };
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
                let token = new_metadata_cursor_token(
                    &mut active_scan.used_tokens,
                    SCHEMA_CURSOR_TOKEN_KIND,
                )?;
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

    /// Resolve an exact schema name through the pinned schema-name index after
    /// applying the same schema-usage visibility checks as schema listing.
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
            self.check_deadline()?;
            self.capture_authority_dependencies(context, dependencies)?;
            if self.load_catalog_role_closure(context.principal_id)? != context.role_ids {
                return Err(TransactionError::InvalidContext);
            }
            for grant in grants {
                self.validate_metadata_grant(context, *grant)?;
            }
            let visible = grants.iter().any(|grant| {
                let AccessTarget::Schema(schema_id) = grant.target else {
                    return false;
                };
                let subject_matches = match grant.subject {
                    Subject::Principal(id) => id == context.principal_id,
                    Subject::Role(id) => context.role_ids.contains(&id),
                };
                grant.operation == Operation::Usage
                    && subject_matches
                    && context
                        .scope
                        .contains(&(Operation::Usage, AccessTarget::Schema(schema_id)))
            });
            if schema_name == "information_schema" {
                return Ok(Some(VisibleSchema {
                    schema_id: INFORMATION_SCHEMA_ID,
                    name: schema_name.to_owned(),
                }));
            }
            if !visible {
                return Ok(None);
            }
            let indexed = self
                .pinned_snapshot()?
                .get_schema_name_index_value(schema_name.as_bytes())
                .map_err(|error| TransactionError::Storage(error.to_string()))?;
            let Some(indexed) = indexed else {
                // Startup normally guarantees a complete derived index. Some
                // in-process callers can hold a catalog snapshot created
                // before startup validation, so preserve listing semantics on
                // an index miss while those snapshots remain supported.
                let mut continuation = None;
                loop {
                    let page = self.list_authorized_schemas(
                        MAX_SCHEMAS_PER_PAGE,
                        continuation,
                        context,
                        grants,
                        dependencies,
                    )?;
                    if let Some(schema) = page
                        .schemas
                        .into_iter()
                        .find(|schema| schema.name == schema_name)
                    {
                        return Ok(Some(schema));
                    }
                    continuation = page.continuation;
                    if continuation.is_none() {
                        return Ok(None);
                    }
                }
            };
            let schema_id: [u8; 16] = indexed
                .try_into()
                .map_err(|_| TransactionError::CorruptCatalog)?;
            if schema_id == [0; 16] {
                return Err(TransactionError::CorruptCatalog);
            }
            let authorized = grants.iter().any(|grant| {
                grant.target == AccessTarget::Schema(schema_id)
                    && grant.operation == Operation::Usage
                    && match grant.subject {
                        Subject::Principal(id) => id == context.principal_id,
                        Subject::Role(id) => context.role_ids.contains(&id),
                    }
                    && context
                        .scope
                        .contains(&(Operation::Usage, AccessTarget::Schema(schema_id)))
            });
            if !authorized {
                return Ok(None);
            }
            let schema = self.read_discovery_schema(schema_id)?;
            if schema.name != schema_name {
                return Err(TransactionError::CorruptCatalog);
            }
            Ok(Some(schema))
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

            let column_ids = self.scan_column_ids_for_table(table_id)?;
            let records = if column_ids.is_empty() {
                // Preserve the low-level path for snapshots written before
                // catalog-derived column indexes were maintained.
                self.scan_catalog_family(6)?
            } else {
                column_ids
                    .into_iter()
                    .map(|column_id| {
                        self.read_catalog_record(&catalog_id_key(6, column_id))?
                            .ok_or(TransactionError::CorruptCatalog)
                    })
                    .collect::<Result<Vec<_>, _>>()?
            };
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
                if guarded != record || key.len() != 19 || catalog_id(&record, 1)? != table_id {
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
            let index_ids = self.scan_index_ids_for_table(schema.table_id)?;
            let records = if index_ids.is_empty() {
                // Preserve the low-level path for snapshots whose family-9
                // records predate derived table-index maintenance.
                self.scan_catalog_family(9)?
            } else {
                index_ids
                    .into_iter()
                    .map(|index_id| {
                        self.read_catalog_record(&catalog_id_key(9, index_id))?
                            .ok_or(TransactionError::CorruptCatalog)
                    })
                    .collect::<Result<Vec<_>, _>>()?
            };
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
        let constraint_ids = self.scan_constraint_ids_for_table(schema.table_id)?;
        let records = if constraint_ids.is_empty() {
            // Preserve raw low-level snapshots created before derived indexes
            // were maintained atomically with family-8 writes.
            self.scan_catalog_family(8)?
        } else {
            let mut candidate_ids = constraint_ids.iter().copied().collect::<HashSet<_>>();
            let own_records = constraint_ids
                .into_iter()
                .map(|constraint_id| {
                    self.read_catalog_record(&catalog_id_key(8, constraint_id))?
                        .ok_or(TransactionError::CorruptCatalog)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let mut referenced_constraint_ids = Vec::new();
            for record in &own_records {
                if matches!(catalog_u16(record, 2)?, 2 | 3) {
                    let constraint_id = record
                        .logical_key
                        .get(3..19)
                        .and_then(|bytes| bytes.try_into().ok())
                        .ok_or(TransactionError::CorruptCatalog)?;
                    referenced_constraint_ids.push(constraint_id);
                }
            }
            referenced_constraint_ids.sort_unstable();
            referenced_constraint_ids.dedup();
            for referenced_id in referenced_constraint_ids {
                for edge in self.lookup_catalog_dependents_for_referenced(referenced_id)? {
                    let edge_kind = edge
                        .logical_key
                        .get(37..39)
                        .and_then(|bytes| bytes.try_into().ok())
                        .map(u16::from_be_bytes)
                        .ok_or(TransactionError::CorruptCatalog)?;
                    if edge.logical_key.len() != 39 {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    if edge_kind == 3 {
                        let foreign_key_id = edge
                            .logical_key
                            .get(3..19)
                            .and_then(|bytes| bytes.try_into().ok())
                            .ok_or(TransactionError::CorruptCatalog)?;
                        if foreign_key_id == [0; 16] {
                            return Err(TransactionError::CorruptCatalog);
                        }
                        candidate_ids.insert(foreign_key_id);
                    }
                }
            }
            let mut candidate_ids = candidate_ids.into_iter().collect::<Vec<_>>();
            candidate_ids.sort_unstable();
            candidate_ids
                .into_iter()
                .map(|constraint_id| {
                    self.read_catalog_record(&catalog_id_key(8, constraint_id))?
                        .ok_or(TransactionError::CorruptCatalog)
                })
                .collect::<Result<Vec<_>, _>>()?
        };
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
            let mut memberships = Vec::new();
            let mut principal_ids = HashSet::from([principal_id]);
            let mut pending = vec![principal_id];
            while let Some(member_id) = pending.pop() {
                let role_ids = self
                    .pinned_snapshot()?
                    .lookup_roles_for_member(member_id)
                    .map_err(|error| match error {
                        SnapshotError::Storage(error) => TransactionError::Storage(error),
                        SnapshotError::InvalidCatalogRecord => TransactionError::CorruptCatalog,
                        _ => TransactionError::CorruptCatalog,
                    })?;
                if memberships.len().saturating_add(role_ids.len())
                    > crate::authorization::MAX_MEMBERSHIP_EDGES
                {
                    return Err(TransactionError::CorruptCatalog);
                }
                for role_id in role_ids {
                    let mut key = 11u16.to_be_bytes().to_vec();
                    key.push(1);
                    key.extend_from_slice(&member_id);
                    key.push(1);
                    key.extend_from_slice(&role_id);
                    let record = self
                        .read_catalog_record(&key)?
                        .ok_or(TransactionError::CorruptCatalog)?;
                    if record.family_code != 11
                        || record.logical_key != key
                        || catalog_id(&record, 1)? == [0; 16]
                        || catalog_u64(&record, 2)? == 0
                    {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    memberships.push(Membership { member_id, role_id });
                    if principal_ids.insert(role_id) {
                        pending.push(role_id);
                    }
                }
            }
            self.charge_scan_work(memberships.len())?;
            let principal_is_membership_endpoint = memberships
                .iter()
                .any(|edge| edge.member_id == principal_id || edge.role_id == principal_id);
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
            let records = self.lookup_catalog_grants_for_grantees(&grantees)?;
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
            let records = self.lookup_catalog_grants_for_grantees(&grantees)?;
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
            let records = self.lookup_catalog_grants_for_grantees(&grantees)?;

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
            let mut table_ids = self.scan_relation_ids_for_schema(schema_id)?;
            if table_ids.is_empty() {
                for table_record in self.scan_catalog_family(5)? {
                    if table_record.family_code != 5 {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    if catalog_id(&table_record, 1)? == schema_id {
                        let table_id = table_record
                            .logical_key
                            .get(3..19)
                            .ok_or(TransactionError::CorruptCatalog)?
                            .try_into()
                            .map_err(|_| TransactionError::CorruptCatalog)?;
                        if table_id == [0; 16] {
                            return Err(TransactionError::CorruptCatalog);
                        }
                        table_ids.push(table_id);
                    }
                }
            }
            table_ids.sort_unstable();
            if table_ids.windows(2).any(|pair| pair[0] == pair[1]) {
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
                    if target_scope != 1 || target_id != schema_id {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    match privilege {
                        1 => Some((Operation::Usage, AccessTarget::Schema(schema_id))),
                        7 | 8 | 10 | 11 => None,
                        _ => return Err(TransactionError::CorruptCatalog),
                    }
                } else if table_ids.binary_search(&object_id).is_ok() {
                    match target_scope {
                        1 if target_id == object_id => {
                            let operation = match privilege {
                                2 => Some(Operation::Select),
                                3 => Some(Operation::Insert),
                                4 => Some(Operation::Update),
                                5 => Some(Operation::Delete),
                                6 | 8 | 9 | 10 | 11 => None,
                                _ => return Err(TransactionError::CorruptCatalog),
                            };
                            operation.map(|operation| (operation, AccessTarget::Table(object_id)))
                        }
                        2 if matches!(privilege, 2..=4 | 6) => {
                            self.validate_discovery_column(object_id, target_id)?;
                            let operation = match privilege {
                                2 => Some(Operation::Select),
                                3 => Some(Operation::Insert),
                                4 => Some(Operation::Update),
                                6 => None,
                                _ => unreachable!(),
                            };
                            operation.map(|operation| {
                                (
                                    operation,
                                    AccessTarget::Column {
                                        table_id: object_id,
                                        column_id: target_id,
                                    },
                                )
                            })
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
            let records = self.scan_catalog_family(4)?;
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
        self.load_catalog_policies_with_policy_inputs(principal_id, schema, false)
            .map(|(policies, _)| policies)
    }

    fn load_catalog_policies_with_policy_inputs(
        &mut self,
        principal_id: [u8; 16],
        schema: &CatalogTableSchema,
        collect_all_policy_inputs: bool,
    ) -> Result<(Vec<Policy>, Vec<[u8; 16]>), TransactionError> {
        let result = (|| {
            if principal_id != self.principal_id || schema.table_id == [0; 16] {
                return Err(TransactionError::InvalidContext);
            }
            let roles = self.load_catalog_role_closure(principal_id)?;
            let policy_ids = self.scan_policy_ids_for_table(schema.table_id)?;
            let policies = if policy_ids.is_empty() {
                // Preserve semantics for in-process snapshots created before
                // startup has reconstructed derived catalog indexes.
                self.scan_catalog_family(13)?
            } else {
                policy_ids
                    .into_iter()
                    .map(|policy_id| {
                        self.read_catalog_record(&catalog_id_key(13, policy_id))?
                            .ok_or(TransactionError::CorruptCatalog)
                    })
                    .collect::<Result<Vec<_>, _>>()?
            };
            if policies.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS {
                return Err(TransactionError::CorruptCatalog);
            }
            let boolean_type_id = self.load_catalog_builtin_type_id(1)?;
            let mut loaded = Vec::new();
            let mut referenced_ordinals = HashSet::new();
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
                let targets = self.scan_policy_targets_for_policy(policy_id)?;
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
                for target_record in &targets {
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
                if subjects.is_empty() && !collect_all_policy_inputs {
                    continue;
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
                if let Some(expression) = &using {
                    collect_expression_fields(expression, &mut referenced_ordinals);
                }
                if let Some(expression) = &with_check {
                    collect_expression_fields(expression, &mut referenced_ordinals);
                }
                if subjects.is_empty() {
                    continue;
                }
                if subjects.len() > crate::authorization::MAX_POLICY_SUBJECTS {
                    return Err(TransactionError::CorruptCatalog);
                }
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
            let mut policy_driving_columns = referenced_ordinals
                .into_iter()
                .map(|ordinal| {
                    schema
                        .column_ids
                        .get(ordinal)
                        .copied()
                        .ok_or(TransactionError::CorruptCatalog)
                })
                .collect::<Result<Vec<_>, _>>()?;
            policy_driving_columns.sort_unstable();
            Ok((loaded, policy_driving_columns))
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
            let constraint_ids = self.scan_constraint_ids_for_table(schema.table_id)?;
            let records = if constraint_ids.is_empty() {
                // Retain the bounded low-level path for snapshots whose
                // family-8 rows predate derived table-constraint indexes.
                self.scan_catalog_family(8)?
            } else {
                constraint_ids
                    .into_iter()
                    .map(|constraint_id| {
                        self.read_catalog_record(&catalog_id_key(8, constraint_id))?
                            .ok_or(TransactionError::CorruptCatalog)
                    })
                    .collect::<Result<Vec<_>, _>>()?
            };
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

    /// Materialize defaults and generated UUID identities from the pinned
    /// catalog. Reading each DEFAULT expression adds its exact bytes to the
    /// final-commit catalog guard set; generated values are collision-checked
    /// against the serialized final table state during commit.
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
            let generated_columns = schema
                .generation_modes
                .iter()
                .enumerate()
                .filter_map(|(ordinal, mode)| (*mode == 1).then_some(schema.column_ids[ordinal]))
                .collect::<Vec<_>>();
            let mut materialized_inputs = inputs.to_vec();
            for (ordinal, mode) in schema.generation_modes.iter().enumerate() {
                if *mode == 1 {
                    if inputs[ordinal] != InsertInput::Default {
                        return Err(TransactionError::InvalidContext);
                    }
                    let mut uuid = [0; 16];
                    getrandom::fill(&mut uuid)
                        .map_err(|_| TransactionError::RandomnessUnavailable)?;
                    if uuid == [0; 16] {
                        return Err(TransactionError::RandomnessUnavailable);
                    }
                    materialized_inputs[ordinal] =
                        InsertInput::Value(Some(crate::tuple::ScalarValue::Uuid(uuid)));
                }
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
            self.stage_insert_authorized_with_defaults(
                id,
                projection,
                &materialized_inputs,
                &defaults,
                &generated_columns,
            )
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
        match self.overlay.read_for_actor(&id, self.principal_id, |_| {
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
        if let Err(error) = self.overlay.put_as(id, payload, self.principal_id) {
            self.coordination.update_write_set_usage(0, 0);
            return Err(error.into());
        }
        self.coordination.record_staged_value_bytes(payload.len());
        self.update_write_set_usage()?;
        Ok(())
    }

    fn update_write_set_usage(&self) -> Result<(), TransactionError> {
        self.coordination.update_write_set_usage(
            self.overlay.staged_row_count()?,
            self.overlay.staged_value_bytes()?,
        );
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
        match self.overlay.get_for_actor(&id, self.principal_id)? {
            Some(StagedRow::Value(_)) => {}
            Some(StagedRow::Delete) if !present => {}
            _ => return Err(TransactionError::IndexRowNotStaged),
        }
        if key.len() < 32
            || key.len() > crate::index_key::MAX_INDEX_KEY_BYTES
            || (unique && key.len() == 32)
            || key[key.len() - 16..] != id.row
        {
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
        let current = self.overlay.get_for_actor(&id, self.principal_id)?;
        if matches!(current, Some(StagedRow::Delete)) {
            return Err(TransactionError::NotFound);
        }
        if base_version.is_none() {
            if current.is_some() {
                self.overlay.remove(&id)?;
                self.update_write_set_usage()?;
                self.base_versions.remove(&id);
                self.index_writes.retain(|_, write| write.row_id != id);
                self.index_bytes = self.index_writes.keys().map(Vec::len).sum();
                return Ok(());
            }
            return Err(TransactionError::NotFound);
        }
        if let Err(error) = self.overlay.delete_as(id, self.principal_id) {
            self.coordination.update_write_set_usage(0, 0);
            return Err(error.into());
        }
        self.update_write_set_usage()?;
        Ok(())
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

    fn commit_inner(mut self) -> Result<u64, TransactionError> {
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

fn attach_plan_identity(
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

impl ExecutorMetadataConnection<'_, '_> {
    pub(crate) fn record_policy_denial(&mut self) {
        self.transaction.coordination.record_policy_denial();
    }

    /// Poll the invocation's cancellation and deadline state. Expired or
    /// cancelled invocations are aborted before returning `true`.
    pub fn check_cancellation(&mut self) -> Result<bool, TransactionError> {
        match self.transaction.check_deadline() {
            Ok(()) => Ok(false),
            Err(TransactionError::CancellationRequested | TransactionError::Expired) => Ok(true),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn detach(mut self) -> ExecutorMetadataSessionState {
        self.detached = true;
        ExecutorMetadataSessionState {
            handles: std::mem::take(&mut self.handles),
            used_tokens: std::mem::take(&mut self.used_tokens),
            scans: std::mem::take(&mut self.scans),
            used_scan_tokens: std::mem::take(&mut self.used_scan_tokens),
            row_handles: std::mem::take(&mut self.row_handles),
            used_row_tokens: std::mem::take(&mut self.used_row_tokens),
        }
    }

    /// Resolve a qualified name through the invocation's pinned catalog and
    /// issue an opaque handle owned by this executor metadata session.
    pub fn resolve_relation(
        &mut self,
        schema_name: &str,
        relation_name: &str,
    ) -> Result<Option<(ResolvedRelationHandle, AuthorizedRelationDescription)>, TransactionError>
    {
        let Some(description) = self
            .transaction
            .resolve_catalog_authorized_relation(schema_name, relation_name)?
        else {
            return Ok(None);
        };
        if self.used_tokens.len() >= MAX_RELATION_HANDLES {
            return Err(TransactionError::RelationHandleLimitExceeded);
        }
        let token = new_metadata_cursor_token(&mut self.used_tokens, RELATION_HANDLE_TOKEN_KIND)?;
        let handle = ResolvedRelationHandle { token };
        self.handles.insert(
            token,
            ResolvedRelationHandleState {
                schema_name: schema_name.to_owned(),
                relation_name: relation_name.to_owned(),
                relation_id: description.relation_id,
            },
        );
        Ok(Some((handle, description)))
    }

    /// Re-resolve a plan's qualified relation name against this invocation's
    /// pinned authorized catalog and return its stable logical identity. This
    /// creates no executor handle or row-access capability.
    pub fn describe_relation_for_plan(
        &mut self,
        schema_name: &str,
        relation_name: &str,
    ) -> Result<Option<AuthorizedPlanRelation>, TransactionError> {
        let Some(relation) = self
            .transaction
            .resolve_catalog_authorized_relation(schema_name, relation_name)?
        else {
            return Ok(None);
        };
        attach_plan_identity(self.transaction, schema_name, relation).map(Some)
    }

    /// Re-resolve the saved qualified name against this connection's pinned
    /// catalog before returning its description. The handle is not authority.
    pub fn revalidate_relation_handle(
        &mut self,
        handle: &ResolvedRelationHandle,
    ) -> Result<AuthorizedRelationDescription, TransactionError> {
        let state = self
            .handles
            .get(&handle.token)
            .cloned()
            .ok_or(TransactionError::InvalidMetadataRequest)?;
        let description = self
            .transaction
            .resolve_catalog_authorized_relation(&state.schema_name, &state.relation_name)?
            .ok_or(TransactionError::InvalidMetadataRequest)?;
        if description.relation_id != state.relation_id || description.name != state.relation_name {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        Ok(description)
    }

    fn revalidate_scan_relation(
        &mut self,
        relation_handle: [u8; 16],
    ) -> Result<AuthorizedRelationDescription, TransactionError> {
        let state = self
            .handles
            .get(&relation_handle)
            .cloned()
            .ok_or(TransactionError::InvalidMetadataRequest)?;
        if let Some(schema) = builtin_information_schema_relation_schema(state.relation_id) {
            let relation = builtin_information_schema_relations()
                .into_iter()
                .find(|relation| relation.relation_id == state.relation_id)
                .ok_or(TransactionError::InvalidMetadataRequest)?;
            if state.schema_name != "information_schema" || relation.name != state.relation_name {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            return Ok(AuthorizedRelationDescription {
                relation_id: schema.relation_id,
                name: relation.name,
                columns: schema.columns,
            });
        }
        self.revalidate_relation_handle(&ResolvedRelationHandle {
            token: relation_handle,
        })
    }

    fn clear_information_schema_scan(&mut self, relation: BuiltinInformationSchemaRelation) {
        match relation {
            BuiltinInformationSchemaRelation::Schemata => {
                self.transaction.active_schema_scan = None;
            }
            BuiltinInformationSchemaRelation::Tables => {
                self.transaction.active_information_schema_tables_scan = None;
            }
            BuiltinInformationSchemaRelation::Columns => {
                self.transaction.active_information_schema_columns_scan = None;
            }
        }
    }

    /// Invalidate a relation handle before the connection closes.
    pub fn close_relation_handle(
        &mut self,
        handle: &ResolvedRelationHandle,
    ) -> Result<(), TransactionError> {
        let state = self
            .handles
            .remove(&handle.token)
            .ok_or(TransactionError::InvalidMetadataRequest)?;
        self.row_handles
            .retain(|_, row_id| row_id.table != state.relation_id);
        if let Some((token, scan)) = self
            .scans
            .iter()
            .find(|(_, scan)| scan.relation_handle == handle.token)
            .map(|(token, scan)| (*token, scan.clone()))
        {
            self.scans.remove(&token);
            if let Some(relation) = scan.source.information_schema_relation() {
                self.clear_information_schema_scan(relation);
            } else if let ExecutorScanSource::Physical {
                row_cursor: Some(cursor),
                ..
            } = scan.source
            {
                self.transaction
                    .discard_active_scan_for_table(cursor.table_id);
            } else if let Some(active) = self.transaction.active_scan.as_ref() {
                self.transaction
                    .discard_active_scan_for_table(active.table_id);
            }
        }
        Ok(())
    }

    /// Open one connection-local scan for a resolved relation. The returned
    /// opaque cursor does not expose row IDs or own authorization.
    pub fn open_relation_scan(
        &mut self,
        handle: &ResolvedRelationHandle,
        requested_column_ids: &[[u8; 16]],
    ) -> Result<ExecutorScanCursor, TransactionError> {
        let description = self.revalidate_relation_handle(handle)?;
        let is_information_schema_relation =
            builtin_information_schema_relation_schema(description.relation_id).is_some();
        if is_information_schema_relation
            && !matches!(description.name.as_str(), "schemata" | "tables" | "columns")
        {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        if requested_column_ids.len() > crate::tuple::MAX_COLUMNS
            || requested_column_ids.contains(&[0; 16])
            || requested_column_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>()
                .len()
                != requested_column_ids.len()
        {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        if !self.scans.is_empty() {
            return Err(TransactionError::ScanStillActive);
        }
        if self.used_scan_tokens.len() >= MAX_RELATION_HANDLES {
            return Err(TransactionError::RelationHandleLimitExceeded);
        }
        let token =
            new_metadata_cursor_token(&mut self.used_scan_tokens, EXECUTOR_SCAN_CURSOR_TOKEN_KIND)?;
        let selected_column_ordinals = if requested_column_ids.is_empty() {
            (0..description.columns.len()).collect()
        } else {
            let selected = description
                .columns
                .iter()
                .enumerate()
                .filter_map(|(index, column)| {
                    requested_column_ids
                        .contains(&column.column_id)
                        .then_some(index)
                })
                .collect::<Vec<_>>();
            if selected.len() != requested_column_ids.len() {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            selected
        };
        self.scans.insert(
            token,
            ActiveExecutorScan {
                relation_handle: handle.token,
                selected_column_ordinals,
                source: if is_information_schema_relation {
                    let relation = match description.name.as_str() {
                        "schemata" => BuiltinInformationSchemaRelation::Schemata,
                        "tables" => BuiltinInformationSchemaRelation::Tables,
                        "columns" => BuiltinInformationSchemaRelation::Columns,
                        _ => return Err(TransactionError::InvalidMetadataRequest),
                    };
                    ExecutorScanSource::InformationSchema {
                        relation,
                        continuation: match relation {
                            BuiltinInformationSchemaRelation::Schemata => {
                                BuiltinInformationSchemaCursor::Schemata(None)
                            }
                            BuiltinInformationSchemaRelation::Tables => {
                                BuiltinInformationSchemaCursor::Tables(None)
                            }
                            BuiltinInformationSchemaRelation::Columns => {
                                BuiltinInformationSchemaCursor::Columns(None)
                            }
                        },
                    }
                } else {
                    ExecutorScanSource::Physical {
                        row_cursor: None,
                        pending_row: None,
                    }
                },
            },
        );
        Ok(ExecutorScanCursor { token })
    }

    /// Probe one catalog-loaded unique index through the invocation's pinned
    /// authorization projection. The raw storage row ID is retained only in
    /// this connection's opaque handle map.
    pub fn probe_relation_unique_index(
        &mut self,
        handle: &ResolvedRelationHandle,
        index_id: [u8; 16],
        values: &[Option<ScalarValue>],
        requested_column_ids: &[[u8; 16]],
    ) -> Result<ExecutorUniqueIndexProbe, TransactionError> {
        let description = self.revalidate_relation_handle(handle)?;
        if values.is_empty()
            || values.len() > crate::tuple::MAX_COLUMNS
            || requested_column_ids.len() > crate::tuple::MAX_COLUMNS
            || requested_column_ids.contains(&[0; 16])
            || requested_column_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>()
                .len()
                != requested_column_ids.len()
        {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        let selected_ordinals = if requested_column_ids.is_empty() {
            (0..description.columns.len()).collect::<Vec<_>>()
        } else {
            let selected = description
                .columns
                .iter()
                .enumerate()
                .filter_map(|(ordinal, column)| {
                    requested_column_ids
                        .contains(&column.column_id)
                        .then_some(ordinal)
                })
                .collect::<Vec<_>>();
            if selected.len() != requested_column_ids.len() {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            selected
        };
        match self.transaction.probe_catalog_unique_index_for_principal(
            description.relation_id,
            index_id,
            values,
        )? {
            AuthorizedUniqueIndexProbe::Unavailable => Ok(ExecutorUniqueIndexProbe::Unavailable),
            AuthorizedUniqueIndexProbe::NoMatch => Ok(ExecutorUniqueIndexProbe::NoMatch),
            AuthorizedUniqueIndexProbe::Match { id, cells } => {
                if self.used_row_tokens.len() >= MAX_EXECUTOR_ROW_HANDLES {
                    self.transaction.abort()?;
                    return Err(TransactionError::RelationHandleLimitExceeded);
                }
                let selected_cells = selected_ordinals
                    .iter()
                    .map(|ordinal| {
                        cells
                            .get(*ordinal)
                            .cloned()
                            .ok_or(TransactionError::InvalidMetadataRequest)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let token = match new_metadata_cursor_token(
                    &mut self.used_row_tokens,
                    EXECUTOR_ROW_HANDLE_TOKEN_KIND,
                ) {
                    Ok(token) => token,
                    Err(error) => {
                        self.transaction.abort()?;
                        return Err(error);
                    }
                };
                self.row_handles.insert(token, id);
                Ok(ExecutorUniqueIndexProbe::Match {
                    handle: ExecutorRowHandle { token },
                    cells: selected_cells,
                })
            }
        }
    }

    /// Scan a catalog-validated leading-key range and return only authorized
    /// projected cells. Storage row identifiers never cross this boundary.
    pub fn scan_relation_index_range(
        &mut self,
        handle: &ResolvedRelationHandle,
        index_id: [u8; 16],
        lower: Option<AuthorizedIndexRangeBound>,
        upper: Option<AuthorizedIndexRangeBound>,
        requested_column_ids: &[[u8; 16]],
    ) -> Result<ExecutorIndexRangeScan, TransactionError> {
        let description = self.revalidate_relation_handle(handle)?;
        if builtin_information_schema_relation_schema(description.relation_id).is_some() {
            return Ok(ExecutorIndexRangeScan::Unavailable);
        }
        if index_id == [0; 16]
            || (lower.is_none() && upper.is_none())
            || requested_column_ids.len() > crate::tuple::MAX_COLUMNS
            || requested_column_ids.contains(&[0; 16])
            || requested_column_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>()
                .len()
                != requested_column_ids.len()
        {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        let selected_ordinals = if requested_column_ids.is_empty() {
            (0..description.columns.len()).collect::<Vec<_>>()
        } else {
            let selected = description
                .columns
                .iter()
                .enumerate()
                .filter_map(|(ordinal, column)| {
                    requested_column_ids
                        .contains(&column.column_id)
                        .then_some(ordinal)
                })
                .collect::<Vec<_>>();
            if selected.len() != requested_column_ids.len() {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            selected
        };
        match self.transaction.scan_catalog_index_range_for_principal(
            description.relation_id,
            index_id,
            lower,
            upper,
        )? {
            AuthorizedIndexRangeScan::Unavailable => Ok(ExecutorIndexRangeScan::Unavailable),
            AuthorizedIndexRangeScan::Rows(rows) => rows
                .into_iter()
                .map(|(_, cells)| {
                    selected_ordinals
                        .iter()
                        .map(|ordinal| {
                            cells
                                .get(*ordinal)
                                .cloned()
                                .ok_or(TransactionError::InvalidMetadataRequest)
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
                .collect::<Result<Vec<_>, _>>()
                .map(ExecutorIndexRangeScan::Rows),
        }
    }

    fn next_information_schema_page(
        &mut self,
        scan: ActiveExecutorScan,
        max_scanned: usize,
    ) -> Result<ExecutorAuthorizedRowScanPage, TransactionError> {
        let ExecutorScanSource::InformationSchema {
            relation,
            continuation,
        } = scan.source
        else {
            return Err(TransactionError::InvalidMetadataRequest);
        };
        let (full_rows, next_cursor) = match (relation, continuation) {
            (
                BuiltinInformationSchemaRelation::Schemata,
                BuiltinInformationSchemaCursor::Schemata(cursor),
            ) => {
                let page = self
                    .transaction
                    .scan_information_schema_schemata(max_scanned, cursor)?;
                (
                    page.rows
                        .into_iter()
                        .map(|row| vec![SanitizedCell::Value(ScalarValue::Text(row.schema_name))])
                        .collect::<Vec<_>>(),
                    page.continuation
                        .map(|cursor| BuiltinInformationSchemaCursor::Schemata(Some(cursor))),
                )
            }
            (
                BuiltinInformationSchemaRelation::Tables,
                BuiltinInformationSchemaCursor::Tables(cursor),
            ) => {
                let page = self
                    .transaction
                    .scan_information_schema_tables(max_scanned, cursor)?;
                (
                    page.rows
                        .into_iter()
                        .map(|row| {
                            Ok(vec![
                                SanitizedCell::Value(ScalarValue::Text(row.table_schema)),
                                SanitizedCell::Value(ScalarValue::Text(row.table_name)),
                                SanitizedCell::Value(ScalarValue::Text(row.table_type)),
                            ])
                        })
                        .collect::<Result<Vec<_>, TransactionError>>()?,
                    page.continuation
                        .map(|cursor| BuiltinInformationSchemaCursor::Tables(Some(cursor))),
                )
            }
            (
                BuiltinInformationSchemaRelation::Columns,
                BuiltinInformationSchemaCursor::Columns(cursor),
            ) => {
                let page = self
                    .transaction
                    .scan_information_schema_columns(max_scanned, cursor)?;
                (
                    page.rows
                        .into_iter()
                        .map(|row| {
                            Ok(vec![
                                SanitizedCell::Value(ScalarValue::Text(row.table_schema)),
                                SanitizedCell::Value(ScalarValue::Text(row.table_name)),
                                SanitizedCell::Value(ScalarValue::Text(row.column_name)),
                                SanitizedCell::Value(ScalarValue::Integer(
                                    i32::try_from(row.ordinal_position)
                                        .map_err(|_| TransactionError::InvalidMetadataRequest)?,
                                )),
                                SanitizedCell::Value(ScalarValue::Text(row.data_type)),
                                SanitizedCell::Value(ScalarValue::Text(row.is_nullable)),
                                information_schema_optional_integer(row.character_maximum_length)?,
                                information_schema_optional_integer(row.numeric_precision)?,
                                information_schema_optional_integer(row.numeric_scale)?,
                            ])
                        })
                        .collect::<Result<Vec<_>, TransactionError>>()?,
                    page.continuation
                        .map(|cursor| BuiltinInformationSchemaCursor::Columns(Some(cursor))),
                )
            }
            _ => return Err(TransactionError::InvalidMetadataRequest),
        };
        if self.used_row_tokens.len().saturating_add(full_rows.len()) > MAX_EXECUTOR_ROW_HANDLES {
            self.transaction.abort()?;
            return Err(TransactionError::RelationHandleLimitExceeded);
        }
        let mut rows = Vec::with_capacity(full_rows.len());
        for full_shape in full_rows {
            let cells = scan
                .selected_column_ordinals
                .iter()
                .map(|ordinal| {
                    full_shape
                        .get(*ordinal)
                        .cloned()
                        .ok_or(TransactionError::InvalidMetadataRequest)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let token = match new_metadata_cursor_token(
                &mut self.used_row_tokens,
                EXECUTOR_ROW_HANDLE_TOKEN_KIND,
            ) {
                Ok(token) => token,
                Err(error) => {
                    self.transaction.abort()?;
                    return Err(error);
                }
            };
            // Virtual metadata rows can be selected but never staged for update
            // or deletion; their opaque tokens are deliberately not row-map keys.
            rows.push((ExecutorRowHandle { token }, cells));
        }
        let continuation = if let Some(continuation) = next_cursor {
            if self.used_scan_tokens.len() >= MAX_RELATION_HANDLES {
                self.transaction.abort()?;
                return Err(TransactionError::RelationHandleLimitExceeded);
            }
            let token = match new_metadata_cursor_token(
                &mut self.used_scan_tokens,
                EXECUTOR_SCAN_CURSOR_TOKEN_KIND,
            ) {
                Ok(token) => token,
                Err(error) => {
                    self.transaction.abort()?;
                    return Err(error);
                }
            };
            self.scans.insert(
                token,
                ActiveExecutorScan {
                    relation_handle: scan.relation_handle,
                    selected_column_ordinals: scan.selected_column_ordinals,
                    source: ExecutorScanSource::InformationSchema {
                        relation,
                        continuation,
                    },
                },
            );
            Some(ExecutorScanCursor { token })
        } else {
            None
        };
        Ok(ExecutorAuthorizedRowScanPage { rows, continuation })
    }

    /// Consume one cursor and return the next authorized row page. A live
    /// continuation receives a fresh token; completed and old tokens cannot
    /// be replayed.
    pub fn next_relation_scan_page(
        &mut self,
        cursor: &ExecutorScanCursor,
        max_scanned: usize,
    ) -> Result<ExecutorAuthorizedRowScanPage, TransactionError> {
        let scan = self
            .scans
            .remove(&cursor.token)
            .ok_or(TransactionError::InvalidMetadataRequest)?;
        let description = self.revalidate_scan_relation(scan.relation_handle)?;
        if scan.source.is_information_schema() {
            return self.next_information_schema_page(scan, max_scanned);
        }
        let ExecutorScanSource::Physical { row_cursor, .. } = scan.source else {
            return Err(TransactionError::InvalidMetadataRequest);
        };
        let page = self.transaction.scan_catalog_authorized_rows(
            description.relation_id,
            row_cursor.as_ref(),
            max_scanned,
        )?;
        if self.used_row_tokens.len().saturating_add(page.rows.len()) > MAX_EXECUTOR_ROW_HANDLES {
            self.transaction.abort()?;
            return Err(TransactionError::RelationHandleLimitExceeded);
        }
        let mut rows = Vec::with_capacity(page.rows.len());
        for (row_id, cells) in page.rows {
            if row_id.table != description.relation_id {
                self.transaction.abort()?;
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let selected_cells = scan
                .selected_column_ordinals
                .iter()
                .map(|ordinal| {
                    cells
                        .get(*ordinal)
                        .cloned()
                        .ok_or(TransactionError::InvalidMetadataRequest)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let token = match new_metadata_cursor_token(
                &mut self.used_row_tokens,
                EXECUTOR_ROW_HANDLE_TOKEN_KIND,
            ) {
                Ok(token) => token,
                Err(error) => {
                    self.transaction.abort()?;
                    return Err(error);
                }
            };
            self.row_handles.insert(token, row_id);
            rows.push((ExecutorRowHandle { token }, selected_cells));
        }
        let continuation = if let Some(row_cursor) = page.continuation {
            if self.used_scan_tokens.len() >= MAX_RELATION_HANDLES {
                self.transaction.abort()?;
                return Err(TransactionError::RelationHandleLimitExceeded);
            }
            let token = match new_metadata_cursor_token(
                &mut self.used_scan_tokens,
                EXECUTOR_SCAN_CURSOR_TOKEN_KIND,
            ) {
                Ok(token) => token,
                Err(error) => {
                    self.transaction.abort()?;
                    return Err(error);
                }
            };
            self.scans.insert(
                token,
                ActiveExecutorScan {
                    relation_handle: scan.relation_handle,
                    selected_column_ordinals: scan.selected_column_ordinals,
                    source: ExecutorScanSource::Physical {
                        row_cursor: Some(row_cursor),
                        pending_row: None,
                    },
                },
            );
            Some(ExecutorScanCursor { token })
        } else {
            None
        };
        Ok(ExecutorAuthorizedRowScanPage { rows, continuation })
    }

    /// Build an executor batch while bounding both candidate work and encoded
    /// response size. One authorized row that does not fit is retained for the
    /// next request; an individually oversized row aborts the invocation.
    pub fn next_relation_scan_batch(
        &mut self,
        cursor: &ExecutorScanCursor,
        max_scanned: usize,
        max_bytes: usize,
    ) -> Result<ExecutorAuthorizedRowScanPage, TransactionError> {
        if max_scanned == 0
            || max_scanned > MAX_EXECUTOR_BATCH_CANDIDATES
            || max_bytes > crate::ipc::MAX_PAYLOAD_BYTES
        {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        let scan = self
            .scans
            .remove(&cursor.token)
            .ok_or(TransactionError::InvalidMetadataRequest)?;
        let description = self.revalidate_scan_relation(scan.relation_handle)?;
        if scan.source.is_information_schema() {
            let page = self.next_information_schema_page(scan, max_scanned)?;
            let encoded_bytes = page.rows.iter().try_fold(21usize, |bytes, (_, cells)| {
                bytes
                    .checked_add(executor_batch_row_encoded_size(cells)?)
                    .ok_or(TransactionError::MetadataResultTooLarge)
            })?;
            if encoded_bytes > max_bytes {
                self.transaction.abort()?;
                return Err(TransactionError::MetadataResultTooLarge);
            }
            return Ok(page);
        }
        let ExecutorScanSource::Physical {
            row_cursor,
            pending_row,
        } = scan.source
        else {
            return Err(TransactionError::InvalidMetadataRequest);
        };
        // Reserve the larger header, which includes a continuation token.
        let mut encoded_bytes = 21usize;
        let mut scanned = 0usize;
        let mut rows = Vec::new();
        let mut row_cursor = row_cursor;
        let mut pending_row = pending_row;

        while rows.len() < max_scanned {
            let candidate = if let Some(candidate) = pending_row.take() {
                Some(candidate)
            } else if scanned < max_scanned {
                let page = self.transaction.scan_catalog_authorized_rows(
                    description.relation_id,
                    row_cursor.as_ref(),
                    1,
                )?;
                scanned += 1;
                row_cursor = page.continuation;
                page.rows.into_iter().next()
            } else {
                None
            };
            let Some((row_id, cells)) = candidate else {
                if row_cursor.is_none() {
                    break;
                }
                continue;
            };
            if row_id.table != description.relation_id {
                self.transaction.abort()?;
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let selected_cells = scan
                .selected_column_ordinals
                .iter()
                .map(|ordinal| {
                    cells
                        .get(*ordinal)
                        .cloned()
                        .ok_or(TransactionError::InvalidMetadataRequest)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let row_bytes = executor_batch_row_encoded_size(&selected_cells)?;
            if encoded_bytes.saturating_add(row_bytes) > max_bytes {
                if rows.is_empty() {
                    self.transaction.abort()?;
                    return Err(TransactionError::MetadataResultTooLarge);
                }
                pending_row = Some((row_id, selected_cells));
                break;
            }
            if self.used_row_tokens.len() >= MAX_EXECUTOR_ROW_HANDLES {
                self.transaction.abort()?;
                return Err(TransactionError::RelationHandleLimitExceeded);
            }
            let token = match new_metadata_cursor_token(
                &mut self.used_row_tokens,
                EXECUTOR_ROW_HANDLE_TOKEN_KIND,
            ) {
                Ok(token) => token,
                Err(error) => {
                    self.transaction.abort()?;
                    return Err(error);
                }
            };
            self.row_handles.insert(token, row_id);
            rows.push((ExecutorRowHandle { token }, selected_cells));
            encoded_bytes += row_bytes;
        }

        let continuation = if pending_row.is_some() || row_cursor.is_some() {
            if self.used_scan_tokens.len() >= MAX_RELATION_HANDLES {
                self.transaction.abort()?;
                return Err(TransactionError::RelationHandleLimitExceeded);
            }
            let token = match new_metadata_cursor_token(
                &mut self.used_scan_tokens,
                EXECUTOR_SCAN_CURSOR_TOKEN_KIND,
            ) {
                Ok(token) => token,
                Err(error) => {
                    self.transaction.abort()?;
                    return Err(error);
                }
            };
            self.scans.insert(
                token,
                ActiveExecutorScan {
                    relation_handle: scan.relation_handle,
                    selected_column_ordinals: scan.selected_column_ordinals,
                    source: ExecutorScanSource::Physical {
                        row_cursor,
                        pending_row,
                    },
                },
            );
            Some(ExecutorScanCursor { token })
        } else {
            None
        };
        Ok(ExecutorAuthorizedRowScanPage { rows, continuation })
    }

    /// Resolve a connection-local visible-row handle to its internal row ID.
    /// The caller must still perform operation-specific authorization.
    pub fn revalidate_row_handle(
        &self,
        handle: &ExecutorRowHandle,
    ) -> Result<RowId, TransactionError> {
        self.row_handles
            .get(&handle.token)
            .copied()
            .ok_or(TransactionError::InvalidMetadataRequest)
    }

    /// Stage an insert from a complete declared-shape value vector. The new
    /// row receives an opaque connection-local handle. DEFAULT markers and
    /// generated UUID identities are materialized from the pinned catalog.
    pub fn stage_insert(
        &mut self,
        relation: &ResolvedRelationHandle,
        values: &[InsertInput],
    ) -> Result<ExecutorRowHandle, TransactionError> {
        let description = self.revalidate_relation_handle(relation)?;
        if builtin_information_schema_relation_schema(description.relation_id).is_some() {
            self.transaction.abort()?;
            return Err(TransactionError::InvalidContext);
        }
        if self.used_row_tokens.len() >= MAX_EXECUTOR_ROW_HANDLES {
            self.transaction.abort()?;
            return Err(TransactionError::RelationHandleLimitExceeded);
        }
        let projection_state = self
            .transaction
            .load_catalog_executor_write_projection(description.relation_id)?;
        let mut row_id_bytes = [0; 16];
        getrandom::fill(&mut row_id_bytes).map_err(|_| TransactionError::RandomnessUnavailable)?;
        if row_id_bytes == [0; 16] {
            self.transaction.abort()?;
            return Err(TransactionError::RandomnessUnavailable);
        }
        let id = RowId {
            table: description.relation_id,
            row: row_id_bytes,
        };
        let handle_token =
            new_metadata_cursor_token(&mut self.used_row_tokens, EXECUTOR_ROW_HANDLE_TOKEN_KIND)?;
        let projection = projection_state.as_projection();
        self.transaction
            .stage_insert_authorized_from_catalog_defaults(
                id,
                &projection,
                values,
                &projection_state.schema,
            )?;
        self.row_handles.insert(handle_token, id);
        Ok(ExecutorRowHandle {
            token: handle_token,
        })
    }

    /// Apply a catalog-authorized field patch to a visible opaque row handle.
    /// All previously issued handles for that row expire after the update.
    pub fn stage_update(
        &mut self,
        handle: &ExecutorRowHandle,
        patches: &[FieldPatch],
    ) -> Result<(), TransactionError> {
        let id = self.revalidate_row_handle(handle)?;
        let projection_state = self
            .transaction
            .load_catalog_executor_write_projection(id.table)?;
        if patches.iter().any(|patch| {
            projection_state
                .schema
                .generation_modes
                .get(patch.ordinal)
                .is_none_or(|mode| *mode != 0)
        }) {
            self.transaction.abort()?;
            return Err(TransactionError::InvalidContext);
        }
        let projection = projection_state.as_projection();
        self.transaction
            .stage_patch_authorized(id, &projection, patches)?;
        self.row_handles.retain(|_, row_id| *row_id != id);
        Ok(())
    }

    /// Delete a visible opaque row handle after pinned-catalog authorization.
    /// All previously issued handles for that row expire after deletion.
    pub fn stage_delete(&mut self, handle: &ExecutorRowHandle) -> Result<(), TransactionError> {
        let id = self.revalidate_row_handle(handle)?;
        let projection_state = self
            .transaction
            .load_catalog_executor_write_projection(id.table)?;
        let projection = projection_state.as_projection();
        self.transaction.stage_delete_authorized(id, &projection)?;
        self.row_handles.retain(|_, row_id| *row_id != id);
        Ok(())
    }

    /// Close a live cursor and release the invocation's active row-scan slot.
    pub fn close_relation_scan(
        &mut self,
        cursor: &ExecutorScanCursor,
    ) -> Result<(), TransactionError> {
        let scan = self
            .scans
            .remove(&cursor.token)
            .ok_or(TransactionError::InvalidMetadataRequest)?;
        let table_id = self
            .handles
            .get(&scan.relation_handle)
            .map(|handle| handle.relation_id)
            .ok_or(TransactionError::InvalidMetadataRequest)?;
        if let Some(relation) = scan.source.information_schema_relation() {
            self.clear_information_schema_scan(relation);
        } else {
            self.transaction.discard_active_scan_for_table(table_id);
        }
        Ok(())
    }
}

impl Drop for ExecutorMetadataConnection<'_, '_> {
    fn drop(&mut self) {
        if self.detached {
            return;
        }
        let information_schema_scans = self
            .scans
            .values()
            .filter_map(|scan| scan.source.information_schema_relation())
            .collect::<HashSet<_>>();
        for relation in information_schema_scans {
            self.clear_information_schema_scan(relation);
        }
        let table_ids = self
            .handles
            .values()
            .map(|handle| handle.relation_id)
            .collect::<HashSet<_>>();
        for table_id in table_ids {
            self.transaction.discard_active_scan_for_table(table_id);
        }
    }
}

impl PlannerMetadataConnection<'_, '_> {
    pub(crate) fn detach(self) -> PlannerMetadataSessionState {
        PlannerMetadataSessionState {
            handles: self.handles,
            used_tokens: self.used_tokens,
            estimate_requests: self.estimate_requests,
        }
    }

    /// Resolve an authorized relation name and issue a planner-only handle.
    pub fn resolve_relation(
        &mut self,
        schema_name: &str,
        relation_name: &str,
    ) -> Result<Option<(PlanningTableHandle, AuthorizedRelationDescription)>, TransactionError>
    {
        let Some(description) = self
            .transaction
            .resolve_catalog_authorized_relation(schema_name, relation_name)?
        else {
            return Ok(None);
        };
        if self.used_tokens.len() >= MAX_RELATION_HANDLES {
            return Err(TransactionError::RelationHandleLimitExceeded);
        }
        let token =
            new_metadata_cursor_token(&mut self.used_tokens, PLANNING_TABLE_HANDLE_TOKEN_KIND)?;
        self.handles.insert(
            token,
            PlannerTableHandleState {
                schema_name: schema_name.to_owned(),
                relation_name: relation_name.to_owned(),
                relation_id: description.relation_id,
            },
        );
        Ok(Some((PlanningTableHandle { token }, description)))
    }

    /// Resolve an authorized planner relation together with pinned schema and
    /// table identity needed to bind a stable logical plan.
    pub fn resolve_relation_for_plan(
        &mut self,
        schema_name: &str,
        relation_name: &str,
    ) -> Result<Option<(PlanningTableHandle, AuthorizedPlanRelation)>, TransactionError> {
        let Some((handle, relation)) = self.resolve_relation(schema_name, relation_name)? else {
            return Ok(None);
        };
        let identity = attach_plan_identity(self.transaction, schema_name, relation)?;
        Ok(Some((handle, identity)))
    }

    /// Resolve an authorized relation and stable plan identity without
    /// creating a connection-local planning handle. This is the metadata
    /// surface used by a separately isolated optimizer process.
    pub fn describe_relation_for_plan(
        &mut self,
        schema_name: &str,
        relation_name: &str,
    ) -> Result<Option<AuthorizedPlanRelation>, TransactionError> {
        let Some(relation) = self
            .transaction
            .resolve_catalog_authorized_relation(schema_name, relation_name)?
        else {
            return Ok(None);
        };
        attach_plan_identity(self.transaction, schema_name, relation).map(Some)
    }

    /// Prove that a resolved relation has no rows in this invocation's
    /// authorized view using only constant SELECT policies. The proof does
    /// not inspect table rows and shares the planner's estimate allowance.
    pub fn prove_empty_population_for_plan(
        &mut self,
        table_id: [u8; 16],
        visible_column_ids: &[[u8; 16]],
    ) -> Result<bool, TransactionError> {
        self.transaction.check_deadline()?;
        if table_id == [0; 16]
            || visible_column_ids.is_empty()
            || visible_column_ids.len() > crate::tuple::MAX_COLUMNS
            || visible_column_ids.contains(&[0; 16])
            || visible_column_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>()
                .len()
                != visible_column_ids.len()
        {
            return Ok(false);
        }
        if !consume_planner_estimate_budget(&mut self.estimate_requests) {
            return Ok(false);
        }
        let state = match self.transaction.load_catalog_estimate_projection(table_id) {
            Ok(state) => state,
            Err(error) if estimate_error_is_terminal(&error) => return Err(error),
            Err(_) => return Ok(false),
        };
        let ordinals = match visible_column_ids
            .iter()
            .map(|column_id| {
                state
                    .schema
                    .column_ids
                    .iter()
                    .position(|candidate| candidate == column_id)
                    .ok_or(TransactionError::InvalidMetadataRequest)
            })
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(ordinals) => ordinals,
            Err(_) => return Ok(false),
        };
        let columns = ordinals
            .iter()
            .map(|ordinal| state.schema.columns[*ordinal].clone())
            .collect::<Vec<_>>();
        let selected_column_ids = ordinals
            .iter()
            .map(|ordinal| state.schema.column_ids[*ordinal])
            .collect::<Vec<_>>();
        let projection = AuthorizedProjection {
            columns: &columns,
            column_ids: &selected_column_ids,
            schema_version: state.schema.schema_version,
            schema_id: state.schema.schema_id,
            context: &state.context,
            grants: &state.grants,
            policies: &state.policies,
            checks: &state.checks,
            policy_driving_columns: &state.policy_driving_columns,
            dependencies: &[],
        };
        Ok(matches!(
            estimate_population(table_id, &projection),
            Some(EstimatePopulation::NoRows)
        ))
    }

    /// Revalidate the qualified name before returning its authorized schema.
    pub fn describe_relation(
        &mut self,
        handle: &PlanningTableHandle,
    ) -> Result<AuthorizedRelationDescription, TransactionError> {
        let state = self
            .handles
            .get(&handle.token)
            .cloned()
            .ok_or(TransactionError::InvalidMetadataRequest)?;
        let description = self
            .transaction
            .resolve_catalog_authorized_relation(&state.schema_name, &state.relation_name)?
            .ok_or(TransactionError::InvalidMetadataRequest)?;
        if description.relation_id != state.relation_id || description.name != state.relation_name {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        Ok(description)
    }

    /// Produce an authorized-view summary for selected stable column IDs.
    /// Empty selection requests every declared column. The planner receives
    /// only counts and cannot use this handle for executor row access.
    pub fn estimate_scan(
        &mut self,
        handle: &PlanningTableHandle,
        column_ids: &[[u8; 16]],
    ) -> Result<AuthorizedScanEstimate, TransactionError> {
        let description = self.describe_relation(handle)?;
        if self
            .handles
            .get(&handle.token)
            .is_some_and(|state| state.schema_name == "information_schema")
        {
            return Ok(AuthorizedScanEstimate::Unknown);
        }
        if !consume_planner_estimate_budget(&mut self.estimate_requests) {
            return Ok(AuthorizedScanEstimate::Unknown);
        }
        let state = self
            .transaction
            .load_catalog_estimate_projection(description.relation_id)?;
        let ordinals = if column_ids.is_empty() {
            (0..state.schema.columns.len()).collect::<Vec<_>>()
        } else {
            if column_ids.iter().copied().collect::<HashSet<_>>().len() != column_ids.len() {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            column_ids
                .iter()
                .map(|column_id| {
                    state
                        .schema
                        .column_ids
                        .iter()
                        .position(|candidate| candidate == column_id)
                        .ok_or(TransactionError::InvalidMetadataRequest)
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        if ordinals.is_empty() {
            return Ok(AuthorizedScanEstimate::Unknown);
        }
        let columns = ordinals
            .iter()
            .map(|ordinal| state.schema.columns[*ordinal].clone())
            .collect::<Vec<_>>();
        let selected_column_ids = ordinals
            .iter()
            .map(|ordinal| state.schema.column_ids[*ordinal])
            .collect::<Vec<_>>();
        let projection = AuthorizedProjection {
            columns: &columns,
            column_ids: &selected_column_ids,
            schema_version: state.schema.schema_version,
            schema_id: state.schema.schema_id,
            context: &state.context,
            grants: &state.grants,
            policies: &state.policies,
            checks: &state.checks,
            policy_driving_columns: &state.policy_driving_columns,
            dependencies: &[],
        };
        self.transaction
            .estimate_authorized_scan(description.relation_id, &projection)
    }

    /// Probe whether a table's authorized view is at most a small row bound.
    /// Unlike a full column summary, this stops after the bound plus one row.
    pub fn estimate_population_at_most(
        &mut self,
        handle: &PlanningTableHandle,
        maximum_rows: u64,
    ) -> Result<Option<u64>, TransactionError> {
        let description = self.describe_relation(handle)?;
        if self
            .handles
            .get(&handle.token)
            .is_some_and(|state| state.schema_name == "information_schema")
        {
            return Ok(None);
        }
        if !consume_planner_estimate_budget(&mut self.estimate_requests) {
            return Ok(None);
        }
        let state = self
            .transaction
            .load_catalog_estimate_projection(description.relation_id)?;
        let projection = AuthorizedProjection {
            columns: &state.schema.columns,
            column_ids: &state.schema.column_ids,
            schema_version: state.schema.schema_version,
            schema_id: state.schema.schema_id,
            context: &state.context,
            grants: &state.grants,
            policies: &state.policies,
            checks: &state.checks,
            policy_driving_columns: &state.policy_driving_columns,
            dependencies: &[],
        };
        self.transaction.estimate_authorized_population_at_most(
            description.relation_id,
            &projection,
            maximum_rows,
        )
    }

    /// Estimate a bound planner predicate after re-resolving its relation in
    /// the invocation snapshot. Predicate column references are decoded
    /// against the pinned table shape and convey no storage capability.
    pub fn estimate_predicate_for_plan(
        &mut self,
        schema_name: &str,
        relation_name: &str,
        predicate_bytes: &[u8],
    ) -> Result<Option<(u64, u64)>, TransactionError> {
        self.transaction.check_deadline()?;
        if predicate_bytes.is_empty()
            || predicate_bytes.len() > crate::optimizer_request::MAX_PREDICATE_BYTES
        {
            return Ok(None);
        }
        let Some(relation) = self.describe_relation_for_plan(schema_name, relation_name)? else {
            return Ok(None);
        };
        if schema_name == "information_schema"
            || !consume_planner_estimate_budget(&mut self.estimate_requests)
        {
            return Ok(None);
        }
        let state = match self
            .transaction
            .load_catalog_estimate_projection(relation.relation.relation_id)
        {
            Ok(state) => state,
            Err(error) if estimate_error_is_terminal(&error) => return Err(error),
            Err(_) => return Ok(None),
        };
        let scan_columns = state
            .schema
            .columns
            .iter()
            .zip(&state.schema.column_ids)
            .enumerate()
            .map(
                |(ordinal, (column, &column_id))| crate::optimizer_binding::BoundColumn {
                    column_id,
                    ordinal: ordinal as u32,
                    scalar_type: column.scalar_type,
                    nullable: column.nullable,
                },
            )
            .collect::<Vec<_>>();
        let predicate =
            crate::optimizer_plan::decode_bound_predicate_v1(predicate_bytes, &scan_columns)
                .map_err(|_| TransactionError::InvalidMetadataRequest)?;
        let projection = AuthorizedProjection {
            columns: &state.schema.columns,
            column_ids: &state.schema.column_ids,
            schema_version: state.schema.schema_version,
            schema_id: state.schema.schema_id,
            context: &state.context,
            grants: &state.grants,
            policies: &state.policies,
            checks: &state.checks,
            policy_driving_columns: &state.policy_driving_columns,
            dependencies: &[],
        };
        self.transaction.estimate_authorized_predicate_at_most(
            relation.relation.relation_id,
            &projection,
            &predicate,
            MAX_PREDICATE_ESTIMATE_ROWS,
        )
    }

    pub fn close_relation(&mut self, handle: &PlanningTableHandle) -> Result<(), TransactionError> {
        self.handles
            .remove(&handle.token)
            .map(|_| ())
            .ok_or(TransactionError::InvalidMetadataRequest)
    }
}

fn consume_planner_estimate_budget(requests: &mut usize) -> bool {
    if *requests >= MAX_PLANNER_ESTIMATE_REQUESTS_PER_CONNECTION {
        return false;
    }
    *requests += 1;
    true
}

fn index_component_unconditionally_readable(
    projection: &AuthorizedProjection<'_>,
    table_id: [u8; 16],
    column_id: [u8; 16],
) -> bool {
    let target = AccessTarget::Column {
        table_id,
        column_id,
    };
    if !has_permission(
        projection.context,
        projection.grants,
        Operation::Select,
        target,
    ) {
        return false;
    }
    let mut has_allow = false;
    for policy in projection.policies.iter().filter(|policy| {
        policy.operation == Operation::Select
            && policy.target == target
            && policy.subjects.iter().any(|subject| match subject {
                Subject::Principal(principal_id) => {
                    *principal_id == projection.context.principal_id
                }
                Subject::Role(role_id) => projection.context.role_ids.contains(role_id),
            })
    }) {
        if !matches!(
            policy.using,
            Some(Expr::Literal(Some(ScalarValue::Boolean(true))))
        ) || policy.with_check.is_some()
        {
            return false;
        }
        has_allow |= policy.effect == Effect::Allow;
    }
    has_allow
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EstimateTruth {
    True,
    False,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum EstimateValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Text(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EstimateValueKind {
    Null,
    Boolean,
    Integer,
    Text,
}

fn valid_estimate_predicate(
    predicate: &crate::optimizer_binding::BoundPredicate,
    projection: &AuthorizedProjection<'_>,
    maximum_depth: usize,
    maximum_nodes: usize,
) -> bool {
    use crate::optimizer_binding::BoundPredicate;

    fn visit(
        predicate: &BoundPredicate,
        projection: &AuthorizedProjection<'_>,
        depth: usize,
        remaining_nodes: &mut usize,
        maximum_depth: usize,
    ) -> bool {
        if depth > maximum_depth || *remaining_nodes == 0 {
            return false;
        }
        *remaining_nodes -= 1;
        match predicate {
            BoundPredicate::Null | BoundPredicate::Boolean(_) | BoundPredicate::Integer(_) => true,
            BoundPredicate::Text(value) => value.len() <= 4096,
            BoundPredicate::Column(column) => usize::try_from(column.ordinal)
                .ok()
                .and_then(|ordinal| {
                    projection.column_ids.get(ordinal).map(|column_id| {
                        *column_id == column.column_id
                            && projection.columns[ordinal].scalar_type == column.scalar_type
                            && projection.columns[ordinal].nullable == column.nullable
                    })
                })
                .unwrap_or(false),
            BoundPredicate::Compare { left, right, .. } => {
                visit(left, projection, depth + 1, remaining_nodes, maximum_depth)
                    && visit(right, projection, depth + 1, remaining_nodes, maximum_depth)
            }
            BoundPredicate::And(children) | BoundPredicate::Or(children) => {
                !children.is_empty()
                    && children.len() <= 16
                    && children.iter().all(|child| {
                        visit(child, projection, depth + 1, remaining_nodes, maximum_depth)
                    })
            }
            BoundPredicate::Not(child) => {
                visit(child, projection, depth + 1, remaining_nodes, maximum_depth)
            }
            BoundPredicate::IsNull(child) | BoundPredicate::IsNotNull(child) => {
                matches!(child.as_ref(), BoundPredicate::Column(_))
                    && visit(child, projection, depth + 1, remaining_nodes, maximum_depth)
            }
        }
    }

    let mut remaining_nodes = maximum_nodes;
    visit(
        predicate,
        projection,
        0,
        &mut remaining_nodes,
        maximum_depth,
    ) && estimate_predicate_kind(predicate, projection)
        .is_some_and(|kind| matches!(kind, EstimateValueKind::Boolean | EstimateValueKind::Null))
}

fn estimate_predicate_kind(
    predicate: &crate::optimizer_binding::BoundPredicate,
    projection: &AuthorizedProjection<'_>,
) -> Option<EstimateValueKind> {
    use crate::optimizer_binding::BoundPredicate;
    use crate::tuple::ScalarType;
    match predicate {
        BoundPredicate::Null => Some(EstimateValueKind::Null),
        BoundPredicate::Boolean(_) => Some(EstimateValueKind::Boolean),
        BoundPredicate::Integer(_) => Some(EstimateValueKind::Integer),
        BoundPredicate::Text(_) => Some(EstimateValueKind::Text),
        BoundPredicate::Column(column) => {
            let ordinal = usize::try_from(column.ordinal).ok()?;
            let column = &projection.columns[ordinal];
            match column.scalar_type {
                ScalarType::Boolean => Some(EstimateValueKind::Boolean),
                ScalarType::SmallInt | ScalarType::Integer | ScalarType::BigInt => {
                    Some(EstimateValueKind::Integer)
                }
                ScalarType::Text | ScalarType::Varchar { .. } => Some(EstimateValueKind::Text),
                ScalarType::Decimal { .. }
                | ScalarType::Bytea
                | ScalarType::Uuid
                | ScalarType::Date
                | ScalarType::TimestampTz => None,
            }
        }
        BoundPredicate::Compare { left, right, .. } => {
            let left = estimate_predicate_kind(left, projection)?;
            let right = estimate_predicate_kind(right, projection)?;
            (left == right || left == EstimateValueKind::Null || right == EstimateValueKind::Null)
                .then_some(EstimateValueKind::Boolean)
        }
        BoundPredicate::And(children) | BoundPredicate::Or(children) => children
            .iter()
            .all(|child| {
                estimate_predicate_kind(child, projection).is_some_and(|kind| {
                    matches!(kind, EstimateValueKind::Boolean | EstimateValueKind::Null)
                })
            })
            .then_some(EstimateValueKind::Boolean),
        BoundPredicate::Not(child) => estimate_predicate_kind(child, projection)
            .is_some_and(|kind| {
                matches!(kind, EstimateValueKind::Boolean | EstimateValueKind::Null)
            })
            .then_some(EstimateValueKind::Boolean),
        BoundPredicate::IsNull(child) | BoundPredicate::IsNotNull(child) => {
            matches!(child.as_ref(), BoundPredicate::Column(_))
                .then_some(EstimateValueKind::Boolean)
        }
    }
}

fn evaluate_estimate_predicate(
    predicate: &crate::optimizer_binding::BoundPredicate,
    cells: &[SanitizedCell],
    projection: &AuthorizedProjection<'_>,
    depth: usize,
    remaining_nodes: usize,
) -> Option<EstimateTruth> {
    use crate::optimizer_binding::BoundPredicate;

    fn value(
        predicate: &BoundPredicate,
        cells: &[SanitizedCell],
        projection: &AuthorizedProjection<'_>,
        depth: usize,
        remaining_nodes: usize,
    ) -> Option<EstimateValue> {
        if depth > 32 || remaining_nodes == 0 {
            return None;
        }
        let next_budget = remaining_nodes - 1;
        match predicate {
            BoundPredicate::Null => Some(EstimateValue::Null),
            BoundPredicate::Boolean(value) => Some(EstimateValue::Boolean(*value)),
            BoundPredicate::Integer(value) => Some(EstimateValue::Integer(*value)),
            BoundPredicate::Text(value) if value.len() <= 4096 => {
                Some(EstimateValue::Text(value.clone()))
            }
            BoundPredicate::Text(_) => None,
            BoundPredicate::Column(column) => {
                let ordinal = usize::try_from(column.ordinal).ok()?;
                if projection.column_ids.get(ordinal) != Some(&column.column_id)
                    || projection.columns.get(ordinal)?.scalar_type != column.scalar_type
                    || projection.columns.get(ordinal)?.nullable != column.nullable
                {
                    return None;
                }
                match cells.get(ordinal)? {
                    SanitizedCell::Value(value) => scalar_estimate_value(value),
                    SanitizedCell::Null => Some(EstimateValue::Null),
                    SanitizedCell::Redacted => None,
                }
            }
            BoundPredicate::Compare {
                operator,
                left,
                right,
            } => {
                let left = value(left, cells, projection, depth + 1, next_budget)?;
                let right = value(right, cells, projection, depth + 1, next_budget)?;
                match compare_estimate_values(&left, &right, *operator)? {
                    Some(value) => Some(EstimateValue::Boolean(value)),
                    None => Some(EstimateValue::Null),
                }
            }
            BoundPredicate::And(children) | BoundPredicate::Or(children) => {
                if children.is_empty() || children.len() > 16 {
                    return None;
                }
                let is_and = matches!(predicate, BoundPredicate::And(_));
                let mut truth = if is_and {
                    EstimateTruth::True
                } else {
                    EstimateTruth::False
                };
                let mut budget = next_budget;
                for child in children {
                    budget = budget.checked_sub(1)?;
                    let child_value = value(child, cells, projection, depth + 1, budget)?;
                    let child_truth = match child_value {
                        EstimateValue::Boolean(true) => EstimateTruth::True,
                        EstimateValue::Boolean(false) => EstimateTruth::False,
                        EstimateValue::Null => EstimateTruth::Unknown,
                        EstimateValue::Integer(_) | EstimateValue::Text(_) => return None,
                    };
                    truth = if is_and {
                        estimate_and(truth, child_truth)
                    } else {
                        estimate_or(truth, child_truth)
                    };
                }
                match truth {
                    EstimateTruth::True => Some(EstimateValue::Boolean(true)),
                    EstimateTruth::False => Some(EstimateValue::Boolean(false)),
                    EstimateTruth::Unknown => Some(EstimateValue::Null),
                }
            }
            BoundPredicate::Not(child) => {
                let child = value(child, cells, projection, depth + 1, next_budget)?;
                match child {
                    EstimateValue::Boolean(value) => Some(EstimateValue::Boolean(!value)),
                    EstimateValue::Null => Some(EstimateValue::Null),
                    EstimateValue::Integer(_) | EstimateValue::Text(_) => None,
                }
            }
            BoundPredicate::IsNull(child) | BoundPredicate::IsNotNull(child) => {
                let BoundPredicate::Column(column) = child.as_ref() else {
                    return None;
                };
                let ordinal = usize::try_from(column.ordinal).ok()?;
                if projection.column_ids.get(ordinal) != Some(&column.column_id)
                    || projection.columns.get(ordinal)?.scalar_type != column.scalar_type
                    || projection.columns.get(ordinal)?.nullable != column.nullable
                {
                    return None;
                }
                match cells.get(ordinal)? {
                    SanitizedCell::Value(_) => Some(EstimateValue::Boolean(matches!(
                        predicate,
                        BoundPredicate::IsNotNull(_)
                    ))),
                    SanitizedCell::Null => Some(EstimateValue::Boolean(matches!(
                        predicate,
                        BoundPredicate::IsNull(_)
                    ))),
                    SanitizedCell::Redacted => None,
                }
            }
        }
    }

    if remaining_nodes == 0 || depth != 0 {
        return None;
    }
    match value(predicate, cells, projection, depth, remaining_nodes)? {
        EstimateValue::Boolean(true) => Some(EstimateTruth::True),
        EstimateValue::Boolean(false) => Some(EstimateTruth::False),
        EstimateValue::Null => Some(EstimateTruth::Unknown),
        EstimateValue::Integer(_) | EstimateValue::Text(_) => None,
    }
}

fn scalar_estimate_value(value: &crate::tuple::ScalarValue) -> Option<EstimateValue> {
    use crate::tuple::ScalarValue;
    match value {
        ScalarValue::Boolean(value) => Some(EstimateValue::Boolean(*value)),
        ScalarValue::SmallInt(value) => Some(EstimateValue::Integer(i64::from(*value))),
        ScalarValue::Integer(value) => Some(EstimateValue::Integer(i64::from(*value))),
        ScalarValue::BigInt(value) => Some(EstimateValue::Integer(*value)),
        ScalarValue::Text(value) | ScalarValue::Varchar(value) if value.len() <= 4096 => {
            Some(EstimateValue::Text(value.clone()))
        }
        ScalarValue::Decimal(_)
        | ScalarValue::Text(_)
        | ScalarValue::Varchar(_)
        | ScalarValue::Bytea(_)
        | ScalarValue::Uuid(_)
        | ScalarValue::Date(_)
        | ScalarValue::TimestampTz(_) => None,
    }
}

fn compare_estimate_values(
    left: &EstimateValue,
    right: &EstimateValue,
    operator: crate::optimizer_request::Comparison,
) -> Option<Option<bool>> {
    use crate::optimizer_request::Comparison;
    let ordering = match (left, right) {
        (EstimateValue::Null, _) | (_, EstimateValue::Null) => return Some(None),
        (EstimateValue::Boolean(left), EstimateValue::Boolean(right)) => left.cmp(right),
        (EstimateValue::Integer(left), EstimateValue::Integer(right)) => left.cmp(right),
        (EstimateValue::Text(left), EstimateValue::Text(right)) => left.cmp(right),
        _ => return None,
    };
    let result = match operator {
        Comparison::Equal => ordering.is_eq(),
        Comparison::NotEqual => !ordering.is_eq(),
        Comparison::Less => ordering.is_lt(),
        Comparison::LessOrEqual => !ordering.is_gt(),
        Comparison::Greater => ordering.is_gt(),
        Comparison::GreaterOrEqual => !ordering.is_lt(),
    };
    Some(Some(result))
}

fn estimate_and(left: EstimateTruth, right: EstimateTruth) -> EstimateTruth {
    match (left, right) {
        (EstimateTruth::False, _) | (_, EstimateTruth::False) => EstimateTruth::False,
        (EstimateTruth::True, EstimateTruth::True) => EstimateTruth::True,
        _ => EstimateTruth::Unknown,
    }
}

fn estimate_or(left: EstimateTruth, right: EstimateTruth) -> EstimateTruth {
    match (left, right) {
        (EstimateTruth::True, _) | (_, EstimateTruth::True) => EstimateTruth::True,
        (EstimateTruth::False, EstimateTruth::False) => EstimateTruth::False,
        _ => EstimateTruth::Unknown,
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

fn executor_batch_row_encoded_size(cells: &[SanitizedCell]) -> Result<usize, TransactionError> {
    let mut size = 18usize; // Opaque row handle and cell count.
    for cell in cells {
        size = size
            .checked_add(match cell {
                SanitizedCell::Null | SanitizedCell::Redacted => 1,
                SanitizedCell::Value(value) => {
                    let value_bytes = match value {
                        ScalarValue::Boolean(_) => 1,
                        ScalarValue::SmallInt(_) => 2,
                        ScalarValue::Integer(_) | ScalarValue::Date(_) => 4,
                        ScalarValue::BigInt(_) | ScalarValue::TimestampTz(_) => 8,
                        ScalarValue::Decimal(_) | ScalarValue::Uuid(_) => 16,
                        ScalarValue::Text(value) | ScalarValue::Varchar(value) => value.len(),
                        ScalarValue::Bytea(value) => value.len(),
                    };
                    6usize
                        .checked_add(value_bytes)
                        .ok_or(TransactionError::MetadataResultTooLarge)?
                }
            })
            .ok_or(TransactionError::MetadataResultTooLarge)?;
    }
    Ok(size)
}

fn collect_expression_fields(expression: &Expr, ordinals: &mut HashSet<usize>) {
    match expression {
        Expr::Field(ordinal) => {
            ordinals.insert(*ordinal);
        }
        Expr::Equal(left, right)
        | Expr::NotEqual(left, right)
        | Expr::Less(left, right)
        | Expr::LessEqual(left, right)
        | Expr::Greater(left, right)
        | Expr::GreaterEqual(left, right)
        | Expr::Add(left, right)
        | Expr::Subtract(left, right)
        | Expr::Multiply(left, right)
        | Expr::Modulo(left, right)
        | Expr::Divide(left, right) => {
            collect_expression_fields(left, ordinals);
            collect_expression_fields(right, ordinals);
        }
        Expr::And(expressions) | Expr::Or(expressions) => {
            for expression in expressions {
                collect_expression_fields(expression, ordinals);
            }
        }
        Expr::Not(expression) | Expr::IsNull(expression) | Expr::Negate(expression) => {
            collect_expression_fields(expression, ordinals);
        }
        Expr::Literal(_) | Expr::CurrentPrincipalId | Expr::HasRole(_) => {}
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EstimatePopulation {
    AllRows,
    NoRows,
}

/// Exact counts are released only when row inclusion is independent of row
/// values. Constant policy outcomes can prove either the full or empty view;
/// value-dependent policies remain unknown so a work-limit result cannot
/// reveal how many candidates failed visibility.
fn estimate_population(
    table_id: [u8; 16],
    projection: &AuthorizedProjection<'_>,
) -> Option<EstimatePopulation> {
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
        return None;
    }

    let mut has_allow = false;
    let mut has_applicable_policy = false;
    let mut denied_all_rows = false;
    for policy in projection.policies.iter().filter(|policy| {
        policy.operation == Operation::Select
            && policy.target == AccessTarget::Table(table_id)
            && policy.subjects.iter().any(|subject| match subject {
                Subject::Principal(principal_id) => *principal_id == context.principal_id,
                Subject::Role(role_id) => context.role_ids.contains(role_id),
            })
    }) {
        has_applicable_policy = true;
        if policy.with_check.is_some() {
            return None;
        }
        match policy.using.as_ref() {
            Some(Expr::Literal(Some(ScalarValue::Boolean(true)))) => {
                has_allow |= policy.effect == Effect::Allow;
            }
            Some(Expr::Literal(Some(ScalarValue::Boolean(false)))) => {
                if policy.effect == Effect::Restrict {
                    denied_all_rows = true;
                }
            }
            _ => return None,
        }
    }
    if !has_applicable_policy {
        return None;
    }
    if denied_all_rows {
        Some(EstimatePopulation::NoRows)
    } else if has_allow {
        Some(EstimatePopulation::AllRows)
    } else {
        Some(EstimatePopulation::NoRows)
    }
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
    token_kind: u8,
) -> Result<[u8; 16], TransactionError> {
    for _ in 0..8 {
        let mut token = [0; 16];
        token[0] = token_kind;
        getrandom::fill(&mut token[1..]).map_err(|_| TransactionError::RandomnessUnavailable)?;
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

fn builtin_information_schema_relations() -> [VisibleRelation; 3] {
    crate::metadata_discovery::information_schema_relations()
}

fn builtin_information_schema_relation(relation_name: &str) -> Option<VisibleRelation> {
    builtin_information_schema_relations()
        .into_iter()
        .find(|relation| relation.name == relation_name)
}

fn builtin_information_schema_relation_schema(
    relation_id: [u8; 16],
) -> Option<VisibleRelationSchema> {
    let relation_code = match relation_id {
        id if id == builtin_information_schema_relation_id(1) => 1,
        id if id == builtin_information_schema_relation_id(2) => 2,
        id if id == builtin_information_schema_relation_id(3) => 3,
        _ => return None,
    };
    let columns = match relation_code {
        1 => vec![
            ("table_schema", ScalarType::Text, false),
            ("table_name", ScalarType::Text, false),
            ("column_name", ScalarType::Text, false),
            ("ordinal_position", ScalarType::Integer, false),
            ("data_type", ScalarType::Text, false),
            ("is_nullable", ScalarType::Text, false),
            ("character_maximum_length", ScalarType::Integer, true),
            ("numeric_precision", ScalarType::Integer, true),
            ("numeric_scale", ScalarType::Integer, true),
        ],
        2 => vec![("schema_name", ScalarType::Text, false)],
        3 => vec![
            ("table_schema", ScalarType::Text, false),
            ("table_name", ScalarType::Text, false),
            ("table_type", ScalarType::Text, false),
        ],
        _ => return None,
    };
    Some(VisibleRelationSchema {
        relation_id,
        columns: columns
            .into_iter()
            .enumerate()
            .map(|(ordinal, (name, scalar_type, nullable))| VisibleColumn {
                column_id: builtin_information_schema_column_id(
                    relation_code * 16 + ordinal as u8 + 1,
                ),
                name: name.to_owned(),
                ordinal: ordinal as u32,
                scalar_type,
                nullable,
            })
            .collect(),
    })
}

const fn builtin_information_schema_relation_id(code: u8) -> [u8; 16] {
    let mut id = [0x81; 16];
    id[15] = code;
    id
}

const fn builtin_information_schema_column_id(code: u8) -> [u8; 16] {
    let mut id = [0x82; 16];
    id[15] = code;
    id
}

fn information_schema_column_type(
    scalar_type: ScalarType,
) -> (&'static str, Option<u32>, Option<u32>, Option<u32>) {
    match scalar_type {
        ScalarType::Boolean => ("BOOLEAN", None, None, None),
        ScalarType::SmallInt => ("SMALLINT", None, Some(5), Some(0)),
        ScalarType::Integer => ("INTEGER", None, Some(10), Some(0)),
        ScalarType::BigInt => ("BIGINT", None, Some(19), Some(0)),
        ScalarType::Decimal { precision, scale } => (
            "DECIMAL",
            None,
            Some(u32::from(precision)),
            Some(u32::from(scale)),
        ),
        ScalarType::Text => ("TEXT", None, None, None),
        ScalarType::Varchar { max_chars } => ("CHARACTER VARYING", Some(max_chars), None, None),
        ScalarType::Bytea => ("BYTEA", None, None, None),
        ScalarType::Uuid => ("UUID", None, None, None),
        ScalarType::Date => ("DATE", None, None, None),
        ScalarType::TimestampTz => ("TIMESTAMP WITH TIME ZONE", None, None, None),
    }
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

#[cfg(test)]
mod information_schema_column_type_tests {
    use super::{ScalarType, information_schema_column_type};

    #[test]
    fn reports_declared_type_names_and_applicable_parameters() {
        assert_eq!(
            information_schema_column_type(ScalarType::Varchar { max_chars: 37 }),
            ("CHARACTER VARYING", Some(37), None, None)
        );
        assert_eq!(
            information_schema_column_type(ScalarType::Decimal {
                precision: 12,
                scale: 3,
            }),
            ("DECIMAL", None, Some(12), Some(3))
        );
        assert_eq!(
            information_schema_column_type(ScalarType::SmallInt),
            ("SMALLINT", None, Some(5), Some(0))
        );
        assert_eq!(
            information_schema_column_type(ScalarType::Integer),
            ("INTEGER", None, Some(10), Some(0))
        );
        assert_eq!(
            information_schema_column_type(ScalarType::BigInt),
            ("BIGINT", None, Some(19), Some(0))
        );
        assert_eq!(
            information_schema_column_type(ScalarType::TimestampTz),
            ("TIMESTAMP WITH TIME ZONE", None, None, None)
        );
    }
}

#[cfg(test)]
mod requested_scope_tests {
    use super::{AccessTarget, Grant, InvocationTransaction, Operation, Subject};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn invocation_scope_intersects_requested_capabilities_with_catalog_grants() {
        static NEXT_FIXTURE: AtomicUsize = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "prdb-requested-scope-{}-{}",
            std::process::id(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&root);
        let mut bootstrap = crate::bootstrap::initialize(&root, "scope_admin", i64::MAX)
            .expect("initialize scope fixture");
        bootstrap.take_secret().expect("take bootstrap secret");
        let instance = bootstrap.acknowledge().expect("publish scope fixture");
        let mut invocation =
            InvocationTransaction::begin(instance.database(), [7; 16], [8; 16], i64::MAX, 1, 64)
                .expect("begin invocation");
        let selected_table = AccessTarget::Table([1; 16]);
        let other_table = AccessTarget::Table([2; 16]);
        let grants = [
            Grant {
                subject: Subject::Principal([8; 16]),
                operation: Operation::Select,
                target: selected_table,
            },
            Grant {
                subject: Subject::Principal([8; 16]),
                operation: Operation::Delete,
                target: other_table,
            },
        ];
        invocation.restrict_to_requested_scope(
            [(Operation::Select, selected_table)].into_iter().collect(),
        );
        assert_eq!(
            invocation.scope_from_grants(&grants),
            [(Operation::Select, selected_table)].into_iter().collect()
        );
        drop(invocation);
        drop(instance);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(test)]
mod planner_estimate_budget_tests {
    use super::{MAX_PLANNER_ESTIMATE_REQUESTS_PER_CONNECTION, consume_planner_estimate_budget};

    #[test]
    fn estimate_budget_exhaustion_falls_back_without_ending_the_planner_session() {
        let mut requests = 0;
        for _ in 0..MAX_PLANNER_ESTIMATE_REQUESTS_PER_CONNECTION {
            assert!(consume_planner_estimate_budget(&mut requests));
        }
        assert_eq!(requests, MAX_PLANNER_ESTIMATE_REQUESTS_PER_CONNECTION);
        assert!(!consume_planner_estimate_budget(&mut requests));
        assert_eq!(requests, MAX_PLANNER_ESTIMATE_REQUESTS_PER_CONNECTION);
    }
}
