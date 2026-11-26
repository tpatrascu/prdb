//! Coordinated entry points for invocation commits and administrative DDL.

mod admission;
mod catalog_helpers;
#[cfg(test)]
mod commit_cancellation_boundary_tests;
mod ddl_constraints;
mod ddl_create_table;
mod ddl_identity_grants;
mod ddl_index;
mod ddl_policies;
mod ddl_schema_table;
mod ddl_workload_profiles;
mod discovery;
mod interactive_txn;
mod invocation_status;
mod session_auth;
#[cfg(test)]
mod statistics_collection_tests;
mod statistics_maintenance;
mod supervisor;
#[cfg(test)]
mod worker_storage_lifetime_tests;

pub(crate) use admission::{
    InvocationAdmissionMetrics, InvocationAdmissionPermit, clear_uncertain_database_key,
    mark_database_uncertain,
};
pub use admission::{MAX_ADMIN_AUDIT_PAGE, MAX_ADMIN_INVOCATION_PAGE, MAX_OWN_INVOCATION_PAGE};
pub use ddl_constraints::*;
pub use ddl_create_table::*;
pub use ddl_identity_grants::*;
pub use ddl_index::*;
pub use ddl_policies::*;
pub use ddl_schema_table::*;
pub use interactive_txn::*;
pub use invocation_status::*;
pub use session_auth::*;
pub use supervisor::*;

use crate::ddl_fence::{DdlFenceCoordinator, DdlFenceManager};
use crate::snapshot::RowSnapshot;
use crate::startup::{StartupError, validate_persisted_records};
use crate::statistics::{
    DEFAULT_ESTIMATE_CANDIDATES, MAX_COMMITTED_POPULATION_ROWS,
    MAX_CONFIGURABLE_COMMITTED_POPULATION_ROWS, MAX_CONFIGURABLE_ESTIMATE_CANDIDATES,
};
use crate::transaction::{DEFAULT_PLANNER_ESTIMATE_REQUESTS, MAX_PLANNER_ESTIMATE_REQUESTS};
use admission::*;
use redb::{Database, ReadableDatabase};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Admission and coordination limits for one storage-manager handle.
///
/// Defaults allow at most 128 simultaneous invocation snapshots, a 15-minute
/// lifetime, and 65,536 row-scan candidates per managed invocation, plus 30
/// token-lifecycle and administrative cancellation requests per actor per
/// rolling minute.
#[derive(Clone, Copy, Debug)]
pub struct StorageManagerConfig {
    pub ddl_fence_timeout: Duration,
    pub max_active_invocations: usize,
    pub max_invocation_age: Duration,
    /// Host-wide maximum catalog records and row candidates scanned in one invocation.
    pub max_invocation_scan_work: usize,
    /// Host-wide maximum authorized candidates returned by one index-range scan.
    pub max_authorized_index_range_candidates: usize,
    /// Host-wide maximum cumulative encoded-key and authorized-row bytes for one index-range lookup.
    pub max_authorized_index_range_bytes: usize,
    /// Host-wide cumulative storage-estimate allowance for planner sessions.
    pub max_planner_estimate_requests: usize,
    /// Host-wide maximum row candidates inspected by each planner estimate.
    pub max_planner_estimate_candidates: usize,
    /// Maximum committed row keys inspected by one statistics maintenance pass.
    pub max_statistics_collection_rows: usize,
    pub max_token_lifecycle_requests_per_minute: usize,
    pub max_admin_cancellation_requests_per_minute: usize,
}

impl StorageManagerConfig {
    pub fn max_invocation_scan_work_for_profile(
        &self,
        profile: &crate::workload_profile::WorkloadProfileLimits,
    ) -> usize {
        usize::try_from(profile.max_invocation_scan_work)
            .unwrap_or(usize::MAX)
            .min(self.max_invocation_scan_work)
    }

    pub fn max_authorized_index_range_candidates_for_profile(
        &self,
        profile: &crate::workload_profile::WorkloadProfileLimits,
    ) -> usize {
        (profile.max_sql_index_probe_candidates as usize)
            .min(self.max_authorized_index_range_candidates)
    }

    pub fn max_authorized_index_range_bytes_for_profile(
        &self,
        profile: &crate::workload_profile::WorkloadProfileLimits,
    ) -> usize {
        usize::try_from(profile.max_authorized_index_range_bytes)
            .unwrap_or(usize::MAX)
            .min(self.max_authorized_index_range_bytes)
    }

    pub fn max_planner_estimate_requests_for_profile(
        &self,
        profile: &crate::workload_profile::WorkloadProfileLimits,
    ) -> usize {
        (profile.max_planner_estimate_requests as usize).min(self.max_planner_estimate_requests)
    }

    pub fn max_planner_estimate_candidates_for_profile(
        &self,
        profile: &crate::workload_profile::WorkloadProfileLimits,
    ) -> usize {
        (profile.max_planner_estimate_candidates as usize).min(self.max_planner_estimate_candidates)
    }
}

impl Default for StorageManagerConfig {
    fn default() -> Self {
        Self {
            ddl_fence_timeout: Duration::from_secs(5),
            max_active_invocations: 128,
            max_invocation_age: Duration::from_secs(15 * 60),
            max_invocation_scan_work: crate::transaction::MAX_INVOCATION_SCAN_WORK,
            max_authorized_index_range_candidates:
                crate::query_abi::DEFAULT_SQL_INDEX_PROBE_CANDIDATES as usize,
            max_authorized_index_range_bytes: crate::transaction::MAX_AUTHORIZED_INDEX_RANGE_BYTES,
            max_planner_estimate_requests: DEFAULT_PLANNER_ESTIMATE_REQUESTS,
            max_planner_estimate_candidates: DEFAULT_ESTIMATE_CANDIDATES,
            max_statistics_collection_rows: MAX_COMMITTED_POPULATION_ROWS as usize,
            max_token_lifecycle_requests_per_minute: 30,
            max_admin_cancellation_requests_per_minute: 30,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageManagerConfigError {
    ZeroActiveInvocationLimit,
    ZeroInvocationAge,
    ZeroInvocationScanWorkLimit,
    ZeroAuthorizedIndexRangeCandidateLimit,
    AuthorizedIndexRangeCandidateLimitTooLarge,
    ZeroAuthorizedIndexRangeByteLimit,
    ZeroPlannerEstimateRequestLimit,
    PlannerEstimateRequestLimitTooLarge,
    ZeroPlannerEstimateCandidateLimit,
    PlannerEstimateCandidateLimitTooLarge,
    ZeroStatisticsCollectionRowLimit,
    StatisticsCollectionRowLimitTooLarge,
    ZeroTokenLifecycleRequestLimit,
    ZeroAdminCancellationRequestLimit,
}

impl std::fmt::Display for StorageManagerConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "invalid storage manager configuration: {self:?}")
    }
}

impl std::error::Error for StorageManagerConfigError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageManagerInitError {
    Config(StorageManagerConfigError),
    Startup(StartupError),
}

impl std::fmt::Display for StorageManagerInitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "storage manager initialization failed: {self:?}")
    }
}

impl std::error::Error for StorageManagerInitError {}

/// Shares one per-table DDL fence manager across row and catalog operations for
/// a database handle.
#[derive(Clone)]
pub struct StorageManager<'db> {
    pub(in crate::storage_manager) database: &'db Database,
    pub(in crate::storage_manager) ddl_fences: DdlFenceCoordinator,
    pub(in crate::storage_manager) max_active_invocations: usize,
    pub(in crate::storage_manager) admissions: Arc<InvocationAdmissionState>,
    pub(in crate::storage_manager) token_requests: Arc<TokenRequestLimiter>,
    pub(in crate::storage_manager) admin_cancellation_requests: Arc<TokenRequestLimiter>,
    pub(in crate::storage_manager) authentication_failure_audits: Arc<TokenRequestLimiter>,
    pub(in crate::storage_manager) max_invocation_age: Duration,
    pub(in crate::storage_manager) max_invocation_scan_work: usize,
    pub(in crate::storage_manager) max_authorized_index_range_candidates: usize,
    pub(in crate::storage_manager) max_authorized_index_range_bytes: usize,
    pub(in crate::storage_manager) max_planner_estimate_requests: usize,
    pub(in crate::storage_manager) max_planner_estimate_candidates: usize,
    pub(in crate::storage_manager) interactive_transactions: InteractiveTransactionMap<'db>,
    pub(in crate::storage_manager) interactive_begin_requests: InteractiveBeginRequestMap,
}

impl<'db> StorageManager<'db> {
    /// Return the caller's current database-administrator authority from a
    /// fresh catalog snapshot. Callers must revalidate the authenticated
    /// session before relying on this result.
    pub fn is_database_admin(&self, actor_id: [u8; 16]) -> Result<bool, TokenLifecycleError> {
        if actor_id == [0; 16] {
            return Err(TokenLifecycleError::TokenUnavailable);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?;
        let snapshot = RowSnapshot::new(read);
        database_admin_authority(&snapshot, actor_id)
    }

    pub(crate) fn database(&self) -> &'db Database {
        self.database
    }

    /// Validate the recovered database, then use the default snapshot limit
    /// and the supplied DDL-fence timeout.
    pub fn new(
        database: &'db Database,
        ddl_fence_timeout: Duration,
    ) -> Result<Self, StorageManagerInitError> {
        Self::with_config(
            database,
            StorageManagerConfig {
                ddl_fence_timeout,
                ..StorageManagerConfig::default()
            },
        )
    }

    pub fn with_config(
        database: &'db Database,
        config: StorageManagerConfig,
    ) -> Result<Self, StorageManagerInitError> {
        Self::build(database, config, true)
    }

    pub(crate) fn from_validated_database(
        database: &'db Database,
        config: StorageManagerConfig,
    ) -> Result<Self, StorageManagerInitError> {
        Self::build(database, config, false)
    }

    pub(super) fn build(
        database: &'db Database,
        config: StorageManagerConfig,
        validate_startup: bool,
    ) -> Result<Self, StorageManagerInitError> {
        if config.max_active_invocations == 0 {
            return Err(StorageManagerInitError::Config(
                StorageManagerConfigError::ZeroActiveInvocationLimit,
            ));
        }
        if config.max_invocation_age.is_zero() {
            return Err(StorageManagerInitError::Config(
                StorageManagerConfigError::ZeroInvocationAge,
            ));
        }
        if config.max_invocation_scan_work == 0 {
            return Err(StorageManagerInitError::Config(
                StorageManagerConfigError::ZeroInvocationScanWorkLimit,
            ));
        }
        if config.max_authorized_index_range_candidates == 0 {
            return Err(StorageManagerInitError::Config(
                StorageManagerConfigError::ZeroAuthorizedIndexRangeCandidateLimit,
            ));
        }
        if config.max_authorized_index_range_candidates
            > crate::query_abi::MAX_SQL_INDEX_PROBE_CANDIDATES as usize
        {
            return Err(StorageManagerInitError::Config(
                StorageManagerConfigError::AuthorizedIndexRangeCandidateLimitTooLarge,
            ));
        }
        if config.max_authorized_index_range_bytes == 0 {
            return Err(StorageManagerInitError::Config(
                StorageManagerConfigError::ZeroAuthorizedIndexRangeByteLimit,
            ));
        }
        if config.max_planner_estimate_requests == 0 {
            return Err(StorageManagerInitError::Config(
                StorageManagerConfigError::ZeroPlannerEstimateRequestLimit,
            ));
        }
        if config.max_planner_estimate_requests > MAX_PLANNER_ESTIMATE_REQUESTS {
            return Err(StorageManagerInitError::Config(
                StorageManagerConfigError::PlannerEstimateRequestLimitTooLarge,
            ));
        }
        if config.max_planner_estimate_candidates == 0 {
            return Err(StorageManagerInitError::Config(
                StorageManagerConfigError::ZeroPlannerEstimateCandidateLimit,
            ));
        }
        if config.max_planner_estimate_candidates > MAX_CONFIGURABLE_ESTIMATE_CANDIDATES {
            return Err(StorageManagerInitError::Config(
                StorageManagerConfigError::PlannerEstimateCandidateLimitTooLarge,
            ));
        }
        if config.max_statistics_collection_rows == 0 {
            return Err(StorageManagerInitError::Config(
                StorageManagerConfigError::ZeroStatisticsCollectionRowLimit,
            ));
        }
        if config.max_statistics_collection_rows as u64 > MAX_CONFIGURABLE_COMMITTED_POPULATION_ROWS
        {
            return Err(StorageManagerInitError::Config(
                StorageManagerConfigError::StatisticsCollectionRowLimitTooLarge,
            ));
        }
        if config.max_token_lifecycle_requests_per_minute == 0 {
            return Err(StorageManagerInitError::Config(
                StorageManagerConfigError::ZeroTokenLifecycleRequestLimit,
            ));
        }
        if config.max_admin_cancellation_requests_per_minute == 0 {
            return Err(StorageManagerInitError::Config(
                StorageManagerConfigError::ZeroAdminCancellationRequestLimit,
            ));
        }
        let recover_interactive_transactions = !has_live_invocation_registry(database);
        if validate_startup {
            validate_persisted_records(database).map_err(StorageManagerInitError::Startup)?;
        }
        if recover_interactive_transactions {
            recover_interactive_transaction_markers(database)
                .map_err(|_| StorageManagerInitError::Startup(StartupError::Storage))?;
        }
        let token_requests =
            shared_token_request_limiter(database, config.max_token_lifecycle_requests_per_minute);
        let admin_cancellation_requests = shared_admin_cancellation_limiter(
            database,
            config.max_admin_cancellation_requests_per_minute,
        );
        let authentication_failure_audits = shared_auth_failure_audit_limiter(database);
        let admissions = shared_invocation_registry(
            database,
            config.max_active_invocations,
            config.max_statistics_collection_rows,
        );
        Ok(Self {
            database,
            ddl_fences: DdlFenceCoordinator::new(DdlFenceManager::new(), config.ddl_fence_timeout),
            max_active_invocations: config.max_active_invocations,
            admissions,
            token_requests,
            admin_cancellation_requests,
            authentication_failure_audits,
            max_invocation_age: config.max_invocation_age,
            max_invocation_scan_work: config.max_invocation_scan_work,
            max_authorized_index_range_candidates: config.max_authorized_index_range_candidates,
            max_authorized_index_range_bytes: config.max_authorized_index_range_bytes,
            max_planner_estimate_requests: config.max_planner_estimate_requests,
            max_planner_estimate_candidates: config.max_planner_estimate_candidates,
            interactive_transactions: Arc::new(Mutex::new(HashMap::new())),
            interactive_begin_requests: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// The effective hard cap shared by live manager handles for this database.
    pub fn max_active_invocations(&self) -> usize {
        self.max_active_invocations
            .min(self.admissions.limit.load(Ordering::Acquire))
    }

    pub fn max_invocation_age(&self) -> Duration {
        self.max_invocation_age
    }

    pub fn max_invocation_scan_work(&self) -> usize {
        self.max_invocation_scan_work
    }

    pub fn max_invocation_scan_work_for_profile(
        &self,
        profile: &crate::workload_profile::WorkloadProfileLimits,
    ) -> usize {
        usize::try_from(profile.max_invocation_scan_work)
            .unwrap_or(usize::MAX)
            .min(self.max_invocation_scan_work)
    }

    pub fn max_authorized_index_range_candidates_for_profile(
        &self,
        profile: &crate::workload_profile::WorkloadProfileLimits,
    ) -> usize {
        (profile.max_sql_index_probe_candidates as usize)
            .min(self.max_authorized_index_range_candidates)
    }

    pub fn max_authorized_index_range_bytes_for_profile(
        &self,
        profile: &crate::workload_profile::WorkloadProfileLimits,
    ) -> usize {
        usize::try_from(profile.max_authorized_index_range_bytes)
            .unwrap_or(usize::MAX)
            .min(self.max_authorized_index_range_bytes)
    }

    pub fn max_planner_estimate_requests_for_profile(
        &self,
        profile: &crate::workload_profile::WorkloadProfileLimits,
    ) -> usize {
        (profile.max_planner_estimate_requests as usize).min(self.max_planner_estimate_requests)
    }

    pub fn max_planner_estimate_candidates_for_profile(
        &self,
        profile: &crate::workload_profile::WorkloadProfileLimits,
    ) -> usize {
        (profile.max_planner_estimate_candidates as usize).min(self.max_planner_estimate_candidates)
    }

    /// Whether a managed invocation hit an ambiguous redb commit error and
    /// this database handle must be closed and reopened before more work.
    pub fn requires_reopen_after_commit_error(&self) -> bool {
        self.admissions.database_uncertain.load(Ordering::Acquire)
    }
}
