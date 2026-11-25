use super::*;
use crate::authorization::{
    AccessTarget, AuthorityDependency, AuthorizationContext, AuthorizationError,
    AuthorizedProjection, Expr, Grant, Operation, Policy, SanitizedCell,
};
use crate::commit::{CommitError, ForeignKeyCheck};
use crate::ddl_fence::DdlFenceCoordinator;
use crate::metadata_discovery::{ExecutorRowHandle, ExecutorScanCursor};
use crate::overlay::{Overlay, OverlayError, RowId};
use crate::row_record::RowRecordError;
use crate::snapshot::RowSnapshot;
use crate::statistics::DEFAULT_ESTIMATE_CANDIDATES;
use crate::storage_manager::{InvocationAdmissionMetrics, InvocationAdmissionPermit};
use crate::tuple::{ColumnSchema, ScalarValue, TupleError};
use redb::Database;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

pub(super) struct StagedIndexChange {
    pub(super) row_id: RowId,
    pub(super) expected_version: Option<u64>,
    pub(super) present: bool,
    pub(super) unique: bool,
    pub(super) null_exempt: bool,
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
    pub(super) invocation_id: [u8; 16],
    pub(super) table_id: [u8; 16],
    pub(super) after_row_id: [u8; 16],
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

pub(crate) const MAX_AUTHORIZED_INDEX_RANGE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy)]
pub(crate) struct AuthorizedIndexRangeBudget {
    pub candidates: usize,
    pub bytes: usize,
}

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
pub(super) struct ActiveRowScan {
    pub(super) table_id: [u8; 16],
    pub(super) after_row_id: [u8; 16],
    pub(super) authority: ScanAuthority,
}

#[derive(Clone)]
pub(super) struct ScanAuthority {
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
    pub(super) fn capture(projection: &AuthorizedProjection<'_>) -> Self {
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

    pub(super) fn matches(&self, projection: &AuthorizedProjection<'_>) -> bool {
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
    pub(super) database: &'db Database,
    pub(super) snapshot: Option<RowSnapshot>,
    pub(super) overlay: Overlay,
    pub(super) base_versions: HashMap<RowId, Option<u64>>,
    pub(super) index_writes: HashMap<Vec<u8>, StagedIndexChange>,
    pub(super) loaded_schemas: HashMap<[u8; 16], CatalogTableSchema>,
    pub(super) loaded_indexes: HashMap<[u8; 16], Vec<CatalogIndexDefinition>>,
    pub(super) loaded_foreign_keys: HashMap<[u8; 16], ForeignKeyCheck>,
    pub(super) catalog_guards: HashMap<Vec<u8>, Option<Vec<u8>>>,
    pub(super) max_index_changes: usize,
    pub(super) max_index_bytes: usize,
    pub(super) index_bytes: usize,
    pub(super) invocation_id: [u8; 16],
    pub(super) principal_id: [u8; 16],
    pub(super) controller_principal_id: [u8; 16],
    pub(super) requested_scope: Option<HashSet<(Operation, AccessTarget)>>,
    pub(super) retention_deadline_us: i64,
    pub(super) coordination: InvocationCoordination,
    pub(super) aborted: bool,
    pub(super) cancellation_triggered: bool,
    pub(super) outcome_recorded: bool,
    pub(super) active_scan: Option<ActiveRowScan>,
    pub(super) max_scan_work: usize,
    pub(super) scanned_work: usize,
    pub(super) max_authorized_index_range_candidates: usize,
    pub(super) max_authorized_index_range_bytes: usize,
    pub(super) active_relation_scan: Option<ActiveRelationScan>,
    pub(super) active_schema_scan: Option<ActiveSchemaScan>,
    pub(super) active_column_scan: Option<ActiveColumnScan>,
    pub(super) active_builtin_relation_scan: Option<ActiveBuiltinRelationScan>,
    pub(super) active_builtin_column_scan: Option<ActiveBuiltinColumnScan>,
    pub(super) active_information_schema_tables_scan: Option<ActiveInformationSchemaTablesScan>,
    pub(super) active_information_schema_columns_scan: Option<ActiveInformationSchemaColumnsScan>,
}

pub(crate) struct InvocationCoordination {
    pub(super) ddl_fences: Option<DdlFenceCoordinator>,
    pub(super) _admission_permit: Option<InvocationAdmissionPermit>,
    pub(super) admission_metrics: Option<InvocationAdmissionMetrics>,
    pub(super) max_age: Option<(Instant, Duration)>,
    pub(super) max_scan_work: usize,
    pub(super) authorized_index_range_budget: AuthorizedIndexRangeBudget,
    pub(super) max_planner_estimate_requests: usize,
    pub(super) max_planner_estimate_candidates: usize,
}

impl InvocationCoordination {
    pub(crate) fn uncoordinated() -> Self {
        Self {
            ddl_fences: None,
            _admission_permit: None,
            admission_metrics: None,
            max_age: None,
            max_scan_work: MAX_INVOCATION_SCAN_WORK,
            authorized_index_range_budget: AuthorizedIndexRangeBudget {
                candidates: crate::query_abi::DEFAULT_SQL_INDEX_PROBE_CANDIDATES as usize,
                bytes: MAX_AUTHORIZED_INDEX_RANGE_BYTES,
            },
            max_planner_estimate_requests: DEFAULT_PLANNER_ESTIMATE_REQUESTS,
            max_planner_estimate_candidates: DEFAULT_ESTIMATE_CANDIDATES,
        }
    }

    pub(super) fn fenced(ddl_fences: DdlFenceCoordinator) -> Self {
        Self {
            ddl_fences: Some(ddl_fences),
            _admission_permit: None,
            admission_metrics: None,
            max_age: None,
            max_scan_work: MAX_INVOCATION_SCAN_WORK,
            authorized_index_range_budget: AuthorizedIndexRangeBudget {
                candidates: crate::query_abi::DEFAULT_SQL_INDEX_PROBE_CANDIDATES as usize,
                bytes: MAX_AUTHORIZED_INDEX_RANGE_BYTES,
            },
            max_planner_estimate_requests: DEFAULT_PLANNER_ESTIMATE_REQUESTS,
            max_planner_estimate_candidates: DEFAULT_ESTIMATE_CANDIDATES,
        }
    }

    pub(crate) fn managed(
        ddl_fences: DdlFenceCoordinator,
        admission_permit: InvocationAdmissionPermit,
        max_age: Duration,
        max_scan_work: usize,
        authorized_index_range_budget: AuthorizedIndexRangeBudget,
        max_planner_estimate_requests: usize,
        max_planner_estimate_candidates: usize,
    ) -> Self {
        let admission_metrics = admission_permit.metrics();
        Self {
            ddl_fences: Some(ddl_fences),
            _admission_permit: Some(admission_permit),
            admission_metrics: Some(admission_metrics),
            max_age: Some((Instant::now(), max_age)),
            max_scan_work,
            authorized_index_range_budget,
            max_planner_estimate_requests,
            max_planner_estimate_candidates,
        }
    }

    pub(super) fn is_expired(&self) -> bool {
        self.max_age
            .is_some_and(|(admitted_at, max_age)| admitted_at.elapsed() >= max_age)
    }

    pub(super) fn may_continue(&self) -> bool {
        self._admission_permit
            .as_ref()
            .is_none_or(InvocationAdmissionPermit::may_continue)
    }

    pub(super) fn cancellation_requested(&self) -> bool {
        self._admission_permit
            .as_ref()
            .is_some_and(InvocationAdmissionPermit::cancellation_requested)
    }

    pub(super) fn update_write_set_usage(&self, staged_rows: usize, staged_value_bytes: usize) {
        if let Some(permit) = &self._admission_permit {
            permit.update_write_set_usage(staged_rows, staged_value_bytes);
        }
    }

    pub(super) fn record_staged_value_bytes(&self, bytes: usize) {
        if let Some(permit) = &self._admission_permit {
            permit.record_staged_value_bytes(bytes);
        }
    }

    pub(super) fn update_snapshot_pin_count(&self, snapshot_pin_count: u8) {
        if let Some(permit) = &self._admission_permit {
            permit.update_snapshot_pin_count(snapshot_pin_count);
        }
    }

    pub(super) fn record_conflict(&self) {
        if let Some(permit) = &self._admission_permit {
            permit.record_conflict();
        }
    }

    pub(super) fn record_policy_denial(&self) {
        if let Some(metrics) = &self.admission_metrics {
            metrics.record_policy_denial();
        }
    }

    pub(super) fn mark_committing(&self) -> bool {
        self._admission_permit
            .as_ref()
            .is_none_or(|permit| permit.try_mark_committing())
    }

    pub(super) fn mark_commit_uncertain(&self) {
        if let Some(permit) = &self._admission_permit {
            permit.mark_commit_uncertain();
        }
    }
}
